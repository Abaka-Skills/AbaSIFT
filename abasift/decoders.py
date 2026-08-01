"""The decoder registry: how a ``LazyRaw`` becomes something a kernel can measure.

Each decoder also encodes an **access-mode decision** — remote-seek vs materialise —
which is where the real I/O cost of a pipeline is decided:

=================  ==============  ==========================================
decoder            access mode     cost on a 178 MB DJI MP4
=================  ==============  ==========================================
``video_meta``     ``open()``      0.45 MB, 11 ranged reads, ~1 s
``video_file``     ``local_path()``  one sequential GET into the disk cache
``video_frames``   ``local_path()``  memory-mapped raw stack — no heap copy at all
``dji_imu``        ``local_path()``  same; remote demux would read 100% anyway
``json``           ``read_bytes()``  sidecars, capped at 16 MiB
``bytes``          ``read_bytes()``  capped; not for video
=================  ==============  ==========================================
"""

from __future__ import annotations

import json as _json

from .errors import DecodeError
from .lazy import LazyRaw, register_decoder
from .payloads import VideoFrames, VideoMeta
from .vendor.dji_telemetry import IMU_HANDLER, read_dji_imu


@register_decoder("bytes")
def decode_bytes(raw: LazyRaw) -> bytes:
    """Whole object in memory. Refuses large files unless ``max_bytes`` is raised."""
    return raw.read_bytes(raw.opts.get("max_bytes", 64 * 2**20))


@register_decoder("json")
def decode_json(raw: LazyRaw):
    data = raw.read_bytes(raw.opts.get("max_bytes", 16 * 2**20))
    try:
        return _json.loads(data)
    except ValueError as e:
        raise DecodeError(f"{raw.uri} is not valid JSON: {e}") from e


@register_decoder("video_file")
def decode_video_file(raw: LazyRaw) -> str:
    """Materialise to the worker disk cache and hand back a path.

    For kernels that need a real file: frame-by-frame passes, external CLI tools,
    GPU decoders. Bytes stream to disk in chunks; nothing is buffered whole in RAM.
    """
    return raw.local_path()


@register_decoder("video_meta")
def decode_video_meta(raw: LazyRaw) -> VideoMeta:
    """Container facts from the header only — no frames, no download.

    PyAV reads through a seekable fsspec file object, so only the ranges holding
    ``ftyp``/``moov`` are fetched. This is what makes a duration probe over a 15 GB
    delivery cost megabytes instead of gigabytes.
    """
    import av

    handle = raw.open(block_size=raw.opts.get("block_size", 1 << 20))
    try:
        container = av.open(handle)
    except Exception as e:
        handle.close()
        raise DecodeError(f"cannot open {raw.uri} as a media container: {e}") from e

    try:
        with container:
            video = next((s for s in container.streams if s.type == "video"), None)
            audio = next((s for s in container.streams if s.type == "audio"), None)
            data_tracks = tuple(
                s.metadata.get("handler_name") or f"{s.type}#{s.index}"
                for s in container.streams
                if s.type == "data"
            )
            duration = _duration_s(container, video)
            if duration is None:
                raise DecodeError(f"{raw.uri}: container reports no duration")
            return VideoMeta(
                duration_s=duration,
                container=container.format.name,
                n_streams=len(container.streams),
                video=_video_info(video),
                audio=_audio_info(audio),
                data_tracks=data_tracks,
                source=raw.uri,
            )
    except DecodeError:
        raise
    except Exception as e:
        raise DecodeError(f"cannot read metadata of {raw.uri}: {e}") from e
    finally:
        handle.close()


#: The frame-stack contract, in one place: the decoder's registered name, the pixel depth,
#: and the two functions that write and read a handle's shape. The producer
#: (``VideoFrameKernel``), the consumer here and any reduce over the handles would otherwise
#: each spell out ``(n, height, width, 3)`` and ``n*w*h*3`` for themselves.
VIDEO_FRAMES = "video_frames"
CHANNELS = 3


def frames_handle(uri: str, *, n: int, width: int, height: int, fps: float, source: str = "") -> LazyRaw:
    """A handle on a raw rgb24 stack. The file has no header, so the shape rides here."""
    return LazyRaw(
        uri, VIDEO_FRAMES, n=int(n), width=int(width), height=int(height),
        fps=round(float(fps), 6), source=source,
    )


def frames_shape(opts) -> tuple[int, int, int, int]:
    try:
        return int(opts["n"]), int(opts["height"]), int(opts["width"]), CHANNELS
    except (KeyError, TypeError, ValueError) as e:
        raise DecodeError(f"{VIDEO_FRAMES} handle is missing its shape: {e}") from e


def frames_nbytes(opts) -> int:
    """What the stack occupies on disk — derived from the shape, never re-multiplied."""
    n, height, width, channels = frames_shape(opts)
    return n * height * width * channels


@register_decoder(VIDEO_FRAMES)
def decode_video_frames(raw: LazyRaw) -> VideoFrames:
    """Memory-map a raw rgb24 stack written by ``VideoFrameKernel``.

    The file has no header — its shape rides in the handle's ``opts`` — so this costs one
    ``mmap`` and no copy, whatever the stack's size. A stack written out to ``s3://`` and read
    back lands in the disk cache first, exactly like any other payload.
    """
    import numpy as np

    shape = frames_shape(raw.opts)
    try:
        data = np.memmap(raw.local_path(), dtype=np.uint8, mode="r", shape=shape)
    except Exception as e:
        raise DecodeError(f"cannot map {raw.uri} as {shape} rgb24: {e}") from e
    return VideoFrames(
        data=data, fps=float(raw.opts["fps"]), source=str(raw.opts.get("source") or raw.uri)
    )


@register_decoder("dji_imu")
def decode_dji_imu(raw: LazyRaw):
    """IMU from a DJI MP4's embedded telemetry track. See :mod:`abasift.vendor.dji_telemetry`."""
    return read_dji_imu(raw.local_path(), handler=raw.opts.get("handler", IMU_HANDLER))


# -- helpers --------------------------------------------------------------


def _duration_s(container, video) -> float | None:
    import av

    if container.duration is not None:
        return round(container.duration / av.time_base, 6)
    if video is not None and video.duration is not None and video.time_base:
        return round(float(video.duration * video.time_base), 6)
    return None


def _video_info(stream) -> dict | None:
    if stream is None:
        return None
    fps = stream.average_rate or stream.guessed_rate
    return {
        "codec": stream.codec_context.name if stream.codec_context else None,
        "width": stream.codec_context.width if stream.codec_context else None,
        "height": stream.codec_context.height if stream.codec_context else None,
        "fps": round(float(fps), 4) if fps else None,
        "nb_frames": int(stream.frames) or None,
        "duration_s": (
            round(float(stream.duration * stream.time_base), 6)
            if stream.duration and stream.time_base
            else None
        ),
    }


def _audio_info(stream) -> dict | None:
    if stream is None:
        return None
    return {
        "codec": stream.codec_context.name if stream.codec_context else None,
        "sample_rate": stream.codec_context.sample_rate if stream.codec_context else None,
        "channels": getattr(stream.codec_context, "channels", None),
    }
