"""Integration tests against the real vendor bucket. Marked ``s3``; skipped without creds.

Deliberately tiny by default — ``max_samples`` is 3 (duration) / 2 (IMU), and the loaders
take the cheapest samples first (``order: size``). To run wide::

    ABASIFT_TEST_MAX_SAMPLES=50 pytest -m s3 -s

    pytest -m 's3 and not slow'      # header probes only, downloads nothing
    pytest -m 'not s3'               # the offline suite

Credentials are read from ``test/s3.json`` by the ``s3_env`` fixture and put on the
standard AWS chain. The framework itself never looks at that file.
"""

from __future__ import annotations

import os

import pytest

from abasift import Executor, Pipeline
from abasift.cache import disk_cache

pytestmark = pytest.mark.s3

FLAT_DATASET = "20260730_test"  # iPhone/Core Media footage, flat directory, no telemetry
DJI_DATASET = "general_324h"  # DJI Osmo Action, one md5 dir per sample, IMU inside the MP4


def _n(default: int) -> int:
    return int(os.environ.get("ABASIFT_TEST_MAX_SAMPLES", default))


def _run(nodes, name, tmp_path):
    nodes = list(nodes) + [
        {
            "name": "dump_report",
            "kernel": "abasift.kernels.DataDumper",
            "params": {"keys": ["__report__"], "target": str(tmp_path / "out")},
            "inputs": [nodes[-1]["name"]],
        }
    ]
    ex = Executor(Pipeline.from_dict({"name": name, "job_id": "itest", "nodes": nodes}))
    report = ex.run()
    assert (tmp_path / "out" / "itest" / "dump_report" / "report.json").exists()
    return report, ex.artifacts


# -- demo 1: duration probe over the flat delivery ------------------------


def test_duration_probe_reads_headers_only(s3_env, tmp_path):
    n = _n(3)
    report, artifacts = _run(
        [
            {
                "name": "load",
                "kernel": "abasift.loaders.FlatDirLoader",
                "params": {
                    "root": f"{s3_env['root']}/{FLAT_DATASET}",
                    "batch_size": 2,
                    "order": "size",
                    "max_samples": n,
                },
                "inputs": [],
            },
            {
                "name": "duration",
                "kernel": "abasift.kernels.VideoDurationKernel",
                "params": {"min_s": 1.0, "max_s": 1800.0},
                "inputs": ["load"],
            },
        ],
        "s3_duration_probe",
        tmp_path,
    )

    assert report.job["n_samples"] == n
    assert report.counts()["error"] == 0, "these files are readable; any error is our bug"
    for sample_id, entry in report.samples.items():
        check = entry["checks"]["duration/video_length"]
        assert check.measurement > 0, sample_id
        assert check.details["video"]["codec"] and check.details["container"]
        assert artifacts[f"duration/duration_s/{sample_id}"] == check.measurement
    assert report.summary["duration"]["n_videos"] == n

    # The design claim, asserted rather than asserted-in-prose: a header probe over
    # multi-hundred-MB objects downloads nothing to disk.
    assert not list(disk_cache().root.iterdir()), "video_meta must not materialise the payload"


# -- demo 2: IMU spike check over the DJI delivery ------------------------


@pytest.mark.slow
def test_imu_spike_over_embedded_dji_telemetry(s3_env, tmp_path):
    """Two branches over one loader: duration (header-only) and IMU (materialised)."""
    n = _n(2)
    report, artifacts = _run(
        [
            {
                "name": "load",
                "kernel": "abasift.loaders.EgoverseDjiLoader",
                "params": {
                    "root": f"{s3_env['root']}/{DJI_DATASET}",
                    "batch_size": 2,
                    "order": "size",
                    "max_samples": n,
                },
                "inputs": [],
            },
            {
                "name": "duration",
                "kernel": "abasift.kernels.VideoDurationKernel",
                "params": {"min_s": 1.0, "max_s": 1800.0},
                "inputs": ["load"],
            },
            {
                "name": "imu",
                "kernel": "abasift.kernels.ImuSpikeKernel",
                "params": {"z_thresh": 12.0, "warn_spikes": 0, "max_spikes": 5},
                "inputs": ["load"],
            },
        ],
        "s3_dji_imu",
        tmp_path,
    )

    assert report.job["n_samples"] == n
    # Both branches ran on every sample: the join unioned two independent edges.
    for entry in report.samples.values():
        assert "duration/video_length" in entry["checks"]
        assert "imu/imu_spike" in entry["checks"]

    measured = [
        c
        for e in report.samples.values()
        if (c := e["checks"]["imu/imu_spike"]).measurement is not None
    ]
    assert measured, "no sample yielded a usable IMU track"
    for check in measured:
        details = check.details
        assert check.status in ("pass", "warn", "fail")
        assert isinstance(check.measurement, int)
        assert details["rate_hz"] > 1.0
        assert details["n_samples"] > 1
        # Physics sanity: the reverse-engineered field really is acceleration in g.
        assert 0.5 < details["accel_g"]["median_magnitude"] < 2.0
        assert "DJI" in details["device"]
        assert details["layout"].startswith("dvtm_")

    assert report.summary["imu"]["n_tracks"] == len(measured)
    assert list(disk_cache().root.iterdir()), "dji_imu must materialise to the disk cache"
    assert any(k.startswith("imu/n_spikes/") for k in artifacts.keys())


@pytest.mark.slow
def test_the_same_video_is_downloaded_once_for_two_streams(s3_env, tmp_path):
    """``video/main`` and ``imu/main`` share a URI, so the disk cache holds one copy."""
    _run(
        [
            {
                "name": "load",
                "kernel": "abasift.loaders.EgoverseDjiLoader",
                "params": {
                    "root": f"{s3_env['root']}/{DJI_DATASET}",
                    "batch_size": 1,
                    "order": "size",
                    "max_samples": 1,
                },
                "inputs": [],
            },
            {
                "name": "imu",
                "kernel": "abasift.kernels.ImuSpikeKernel",
                "params": {"max_spikes": 5},
                "inputs": ["load"],
            },
        ],
        "s3_cache_once",
        tmp_path,
    )
    assert len(list(disk_cache().root.iterdir())) == 1


def test_a_sample_without_telemetry_errors_without_killing_the_job(s3_env, tmp_path):
    """Some deliveries mix in phone-recorded clips with no ``DJI meta`` track."""
    report, _ = _run(
        [
            {
                "name": "load",
                "kernel": "abasift.loaders.EgoverseDjiLoader",
                "params": {
                    "root": f"{s3_env['root']}/{DJI_DATASET}",
                    "batch_size": 4,
                    "order": "size",
                    "max_samples": _n(4),
                },
                "inputs": [],
            },
            {
                "name": "imu",
                "kernel": "abasift.kernels.ImuSpikeKernel",
                "params": {"max_spikes": 5},
                "inputs": ["load"],
            },
        ],
        "s3_dji_failsafe",
        tmp_path,
    )
    assert report.job["finished_at"], "the job must always complete and report"
    errors = [
        e["checks"]["imu/imu_spike"]
        for e in report.samples.values()
        if e["checks"]["imu/imu_spike"].status == "error"
    ]
    for check in errors:
        details = check.details
        assert "exception" in details or "reason" in details
