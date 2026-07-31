"""Demo 2 offline: spike detection maths, kernel verdicts, and the DJI wire parser.

The one thing that cannot be faked offline is a real MP4 carrying a ``DJI meta`` track;
that path is covered by ``test_integration_s3.py``. Everything else — the statistics, the
thresholds, the protobuf field mapping, the missing-track failsafe — is exercised here
with no network.
"""

from __future__ import annotations

import struct

import kernels_for_test as kft
import numpy as np
import pytest

from abasift import Executor, MissingStream, Pipeline, Sample
from abasift.kernels.imu_spike import ImuSpikeKernel, spike_scan
from abasift.payloads import ImuTrack
from abasift.vendor import dji_telemetry as dji


# -- the statistics -------------------------------------------------------


def test_clean_track_has_no_spikes():
    n_spikes, max_z, times = spike_scan(kft.synth_imu(), z_thresh=8.0)
    assert n_spikes == 0
    assert 0 < max_z < 8.0
    assert len(times) == 0


def test_single_impulse_is_found_at_the_right_time():
    track = kft.synth_imu(n=1200, rate_hz=60.0, spikes=((600, 3.0),))
    n_spikes, max_z, times = spike_scan(track, z_thresh=8.0)
    # One impulse perturbs two consecutive first-differences (in and out of the spike).
    assert n_spikes == 2
    assert max_z > 50
    assert times[0] == pytest.approx(600 / 60.0, abs=1 / 60.0)


def test_robust_scale_is_not_fooled_by_many_spikes():
    """A mean/std z-score would hide these; median/MAD must not."""
    track = kft.synth_imu(n=1200, spikes=tuple((100 * i, 2.5) for i in range(1, 11)))
    n_spikes, _max_z, _times = spike_scan(track, z_thresh=8.0)
    assert n_spikes == 20  # 10 impulses, two differences each


def test_flat_track_yields_no_spurious_spikes():
    """A frozen sensor has zero jerk; the scale floor must not turn 0/0 into a spike."""
    n = 500
    flat = ImuTrack(t=np.arange(n) / 50.0, accel_g=np.tile([0.0, 0.0, -1.0], (n, 1)))
    assert spike_scan(flat, z_thresh=8.0)[0] == 0


# -- kernel verdicts come from YAML params --------------------------------


def _check(track, **params):
    kernel = ImuSpikeKernel(**params)
    sample = Sample("s1", {"imu/main": kft.put_track("t1", track)})
    checks, artifacts = kernel.sift(sample, None)
    return checks["imu_spike"], artifacts


def test_thresholds_decide_the_verdict():
    dirty = kft.synth_imu(spikes=((300, 3.0),))
    assert _check(dirty, max_spikes=0)[0].status == "fail"
    assert _check(dirty, max_spikes=5)[0].status == "pass"
    assert _check(dirty, max_spikes=5, warn_spikes=1)[0].status == "warn"
    # A looser outlier definition stops calling it a spike at all.
    assert _check(dirty, z_thresh=1e6, max_spikes=0)[0].status == "pass"


def test_measurement_and_details_are_reported():
    check, artifacts = _check(kft.synth_imu(n=600, rate_hz=50.0, spikes=((100, 4.0),)), max_spikes=0)
    assert check.measurement == 2
    assert check.details["rate_hz"] == pytest.approx(50.0, abs=0.01)
    assert check.details["n_samples"] == 600
    assert check.details["accel_g"]["median_magnitude"] == pytest.approx(1.0, abs=0.2)
    assert artifacts["n_spikes/s1"] == 2


def test_track_too_short_is_an_error_not_a_verdict():
    check, artifacts = _check(kft.synth_imu(n=10), min_samples=64)
    assert check.status == "error" and check.measurement is None
    assert "need 64" in check.details["reason"]
    assert artifacts == {}


def test_finalize_reduces_over_the_dataset():
    nodes = [
        {"name": "load", "kernel": "kernels_for_test.SyntheticLoader",
         "params": {"n": 3, "batch_size": 2, "decoder": "test_imu_stream"}, "inputs": []},
        {"name": "imu", "kernel": "abasift.kernels.ImuSpikeKernel",
         "params": {"stream": "blob/main", "max_spikes": 0}, "inputs": ["load"]},
    ]
    report = Executor(Pipeline.from_dict({"name": "imu", "nodes": nodes})).run()
    summary = report.summary["imu"]
    assert summary["n_tracks"] == 3
    assert summary["tracks_with_spikes"] == 1  # only sample s1 gets an injected impulse
    assert summary["worst_sample"] == "s1"
    assert report.counts() == {"pass": 2, "warn": 0, "fail": 1, "error": 0}


# -- the DJI wire format --------------------------------------------------


def _tag(field: int, wire: int) -> bytes:
    return _varint((field << 3) | wire)


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def _sub(field: int, payload: bytes) -> bytes:
    return _tag(field, 2) + _varint(len(payload)) + payload


def _f32s(fields: tuple[int, ...], values) -> bytes:
    return b"".join(_tag(f, 5) + struct.pack("<f", v) for f, v in zip(fields, values))


def build_djmd_packet(ts_us: int, quat, accel) -> bytes:
    """Re-encode a packet in the layout documented in abasift.vendor.dji_telemetry."""
    payload = _sub(9, _f32s((1, 2, 3, 4), quat)) + _sub(10, _f32s((2, 3, 4), accel))
    record = _sub(1, _tag(2, 0) + _varint(ts_us)) + _sub(2, payload)
    return _sub(3, record)


def build_djmd_header() -> bytes:
    info = _sub(1, b"dvtm_ac204.proto") + _sub(10, b"DJI OsmoAction5 Pro")
    return _sub(1, _sub(1, info))


def test_frame_parser_round_trips():
    quat = (0.898413, -0.00987188, -0.437986, -0.0304068)
    accel = (-0.673736, 0.0115874, -0.631113)
    ts, got_quat, got_accel = dji.parse_frame(build_djmd_packet(11255136001, quat, accel))
    assert ts == 11255136001
    assert got_quat == pytest.approx(quat, abs=1e-6)
    assert got_accel == pytest.approx(accel, abs=1e-6)


def test_header_parser_extracts_provenance():
    assert dji.parse_header(build_djmd_header()) == {
        "layout": "dvtm_ac204.proto",
        "device": "DJI OsmoAction5 Pro",
    }


def test_unrelated_payload_is_not_mistaken_for_telemetry():
    assert dji.parse_frame(build_djmd_header()) is None
    assert dji.parse_frame(_sub(7, b"\x08\x01")) is None


def test_layout_validation_rejects_a_moved_field_mapping():
    """The field numbers are reverse-engineered, so a firmware change must be caught."""
    n = 300
    t = np.arange(n) / 60.0
    good_quat = np.tile([1.0, 0.0, 0.0, 0.0], (n, 1))
    good_accel = np.tile([0.0, 0.0, -1.0], (n, 1))

    dji._validate_layout(t, good_accel, good_quat, "ok.mp4")  # no raise

    with pytest.raises(Exception, match="unit quaternion"):
        dji._validate_layout(t, good_accel, good_quat * 7.0, "x.mp4")
    with pytest.raises(Exception, match="acceleration in g"):
        dji._validate_layout(t, good_accel * 900.0, good_quat, "x.mp4")
    with pytest.raises(Exception, match="monotonic"):
        dji._validate_layout(t[::-1], good_accel, good_quat, "x.mp4")


def test_missing_telemetry_track_raises_missing_stream(videos):
    """A plain video with no data track: the failsafe path, tested without any vendor file."""
    with pytest.raises(MissingStream, match="DJI meta"):
        dji.read_dji_imu(str(videos / "clip_2s.mp4"))
