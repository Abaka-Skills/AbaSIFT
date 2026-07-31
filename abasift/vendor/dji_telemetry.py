"""Read the IMU out of a DJI MP4's embedded telemetry track.

The egoverse vendor ships DJI Osmo Action footage; there is no sidecar IMU file — the
inertial data lives inside the MP4 as an extra ``data`` track. Two such tracks exist:

===============  =============  ====================================================
handler          codec tag      content
===============  =============  ====================================================
``DJI meta``     ``djmd``       per-video-frame telemetry, ``dvtm_*.proto`` payloads
``DJI dbgi``     ``dbgi``       camera debug/AE statistics, ``dbginfo_*.proto``, ~3 kB
                                per frame — not inertial, ignored here
===============  =============  ====================================================

The payloads are bare protobuf with no schema shipped, so this module walks the wire
format and reads the fields by number. Field map, established by decoding the stream and
checking the numbers against physics (see doc/components/dji-telemetry.md):

    3          per-frame record          (field 1/2 carry the file header, first packet only)
    3.1.2      capture timestamp, µs     Δ = 16683 µs == 1/59.94 s, matches the video rate
    3.2.9      orientation quaternion    4 × float32 w,x,y,z; ‖q‖ = 1.00000 ± 0.00000
    3.2.10     accelerometer, g          3 × float32 x,y,z (field 1 absent = proto3 zero);
                                         median ‖a‖ ≈ 1.02 g at rest, range 0.58-1.48 g

Because the mapping is inferred rather than specified, :func:`read_dji_imu` validates it
before returning: a track whose quaternions are not unit-norm or whose median
acceleration is not near 1 g is rejected as an unrecognised layout (``DecodeError`` ->
``status: error``) instead of being silently mis-measured.
"""

from __future__ import annotations

import struct
from typing import Iterator

import numpy as np

from ..errors import DecodeError, MissingStream
from ..payloads import ImuTrack

IMU_HANDLER = "DJI meta"

_F_RECORD = 3
_F_TIME = 1
_F_TIME_US = 2
_F_PAYLOAD = 2
_F_QUAT = 9
_F_ACCEL = 10
_F_HEADER = 1
_F_HEADER_INFO = 1
_F_PROTO_NAME = 1
_F_DEVICE = 10


# -- minimal protobuf wire reader (schema-less) ---------------------------


def _varint(buf: bytes, i: int) -> tuple[int, int]:
    result = shift = 0
    while True:
        byte = buf[i]
        i += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, i
        shift += 7
        if shift > 63:
            raise DecodeError("protobuf varint overflow")


def _walk(buf: bytes) -> Iterator[tuple[int, int, object]]:
    """Yield ``(field_number, wire_type, value)`` for one message. Bytes stay bytes."""
    i, n = 0, len(buf)
    while i < n:
        key, i = _varint(buf, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, i = _varint(buf, i)
        elif wire == 1:
            value, i = buf[i : i + 8], i + 8
        elif wire == 2:
            length, i = _varint(buf, i)
            value, i = buf[i : i + length], i + length
        elif wire == 5:
            value, i = buf[i : i + 4], i + 4
        else:
            raise DecodeError(f"unsupported protobuf wire type {wire}")
        yield field, wire, value


def _msg(buf: bytes) -> dict[int, object]:
    """Message as ``{field_number: value}``; later repeats win (none of ours repeat)."""
    return {f: v for f, _w, v in _walk(buf)}


def _floats(buf: bytes, fields: tuple[int, ...]) -> list[float] | None:
    """Read fixed32 floats out of a sub-message by field number; absent field -> 0.0."""
    m = _msg(buf)
    out = []
    for f in fields:
        v = m.get(f)
        if v is None:
            out.append(0.0)
        elif isinstance(v, bytes) and len(v) == 4:
            out.append(struct.unpack("<f", v)[0])
        else:
            return None
    return out


# -- public ---------------------------------------------------------------


def parse_frame(payload: bytes) -> tuple[int, list[float], list[float]] | None:
    """One ``djmd`` packet -> ``(timestamp_us, quat_wxyz, accel_xyz)``, or None."""
    top = _msg(payload)
    record = top.get(_F_RECORD)
    if not isinstance(record, bytes):
        return None
    rec = _msg(record)
    time_msg, body = rec.get(_F_TIME), rec.get(_F_PAYLOAD)
    if not isinstance(time_msg, bytes) or not isinstance(body, bytes):
        return None
    ts = _msg(time_msg).get(_F_TIME_US)
    inner = _msg(body)
    quat_buf, accel_buf = inner.get(_F_QUAT), inner.get(_F_ACCEL)
    if not isinstance(ts, int) or not isinstance(quat_buf, bytes) or not isinstance(accel_buf, bytes):
        return None
    quat = _floats(quat_buf, (1, 2, 3, 4))
    accel = _floats(accel_buf, (2, 3, 4))
    if quat is None or accel is None:
        return None
    return ts, quat, accel


def parse_header(payload: bytes) -> dict[str, str]:
    """Proto name and device model out of the first packet, for provenance."""
    top = _msg(payload)
    header = top.get(_F_HEADER)
    if not isinstance(header, bytes):
        return {}
    info = _msg(header).get(_F_HEADER_INFO)
    if not isinstance(info, bytes):
        return {}
    m = _msg(info)
    out = {}
    for key, field in (("layout", _F_PROTO_NAME), ("device", _F_DEVICE)):
        v = m.get(field)
        if isinstance(v, bytes):
            try:
                out[key] = v.decode("utf-8", "replace")
            except Exception:
                pass
    return out


def read_dji_imu(path: str, handler: str = IMU_HANDLER) -> ImuTrack:
    """Demux the telemetry track of a local DJI MP4 into an :class:`ImuTrack`.

    Takes a **local path** on purpose: the telemetry packets are interleaved with the
    video, so reading them touches ~100% of the file's byte range (measured: 177.9 MB of
    a 177.9 MB file, in 4406 ranged reads if done over the network). Materialising the
    object to the disk cache once and demuxing locally is strictly cheaper.
    """
    import av  # local import: PyAV is only needed by video/IMU decoders

    try:
        container = av.open(path)
    except Exception as e:
        raise DecodeError(f"cannot open container {path}: {e}") from e

    with container:
        tracks = [s for s in container.streams if s.metadata.get("handler_name") == handler]
        if not tracks:
            present = sorted({s.metadata.get("handler_name") or s.type for s in container.streams})
            raise MissingStream(f"no {handler!r} track in {path}; tracks present: {present}")

        info: dict[str, str] = {}
        header_tried = False  # not `if not info`: a header we cannot read must not be
        # retried on all ~2000 packets of the file
        ts_us: list[int] = []
        quats: list[list[float]] = []
        accels: list[list[float]] = []
        try:
            for packet in container.demux(tracks[0]):
                payload = bytes(packet)
                if not payload:
                    continue
                if not header_tried:
                    info, header_tried = parse_header(payload), True
                frame = parse_frame(payload)
                if frame is None:
                    continue
                t, q, a = frame
                ts_us.append(t)
                quats.append(q)
                accels.append(a)
        except Exception as e:
            raise DecodeError(f"failed demuxing {handler!r} from {path}: {e}") from e

    if len(ts_us) < 2:
        raise DecodeError(f"{handler!r} track in {path} yielded {len(ts_us)} usable records")

    t = (np.asarray(ts_us, dtype=np.float64) - ts_us[0]) / 1e6
    accel = np.asarray(accels, dtype=np.float64)
    quat = np.asarray(quats, dtype=np.float64)
    _validate_layout(t, accel, quat, path)
    return ImuTrack(
        t=t, accel_g=accel, quat=quat, device=info.get("device", ""), layout=info.get("layout", "")
    )


def _validate_layout(t, accel, quat, path: str) -> None:
    """Guard the schema-less field mapping against a firmware that moved the fields."""
    if not np.all(np.diff(t) > 0):
        raise DecodeError(f"{path}: telemetry timestamps are not monotonic")
    qn = float(np.median(np.linalg.norm(quat, axis=1)))
    if not 0.98 <= qn <= 1.02:
        raise DecodeError(
            f"{path}: field 3.2.9 is not a unit quaternion (median norm {qn:.4f}) — "
            "unrecognised DJI telemetry layout"
        )
    an = float(np.median(np.linalg.norm(accel, axis=1)))
    if not 0.5 <= an <= 2.0:
        raise DecodeError(
            f"{path}: field 3.2.10 is not acceleration in g (median norm {an:.4f} g) — "
            "unrecognised DJI telemetry layout"
        )
