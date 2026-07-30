"""Acceptance test for demo 1, exactly as specified in doc/design.md §6.

ffmpeg-synthesized clips of 2/5/10 s plus one deliberately corrupt file:
the three real clips pass with duration error < 0.1 s, the corrupt file is ``error``,
the job completes normally, and the summary numbers are right.
"""

from __future__ import annotations

import json

import pytest
from conftest import CLIP_SECONDS

from abasift import Executor, Pipeline
from abasift.cli import main

DURATION_TOLERANCE_S = 0.1


def _pipeline(videos, target=None, max_s=None):
    nodes = [
        {
            "name": "load",
            "kernel": "abasift.loaders.FlatDirLoader",
            "params": {"root": str(videos), "batch_size": 2},
            "inputs": [],
        },
        {
            "name": "duration",
            "kernel": "abasift.kernels.VideoDurationKernel",
            "params": {"min_s": 1.0, **({"max_s": max_s} if max_s else {})},
            "inputs": ["load"],
        },
    ]
    if target:
        nodes.append(
            {
                "name": "dump_report",
                "kernel": "abasift.kernels.DataDumper",
                "params": {"keys": ["__report__"], "target": str(target)},
                "inputs": ["duration"],
            }
        )
    return Pipeline.from_dict({"name": "basic_video_qc", "job_id": "demo", "nodes": nodes})


@pytest.fixture(scope="module")
def demo(videos):
    ex = Executor(_pipeline(videos))
    return ex.run(), ex.artifacts


def test_every_real_clip_passes_with_the_right_duration(demo):
    report, _ = demo
    for secs in CLIP_SECONDS:
        entry = report.samples[f"clip_{secs}s"]
        check = entry["checks"]["duration/video_length"]
        assert entry["status"] == "pass"
        assert abs(check.measurement - secs) < DURATION_TOLERANCE_S


def test_corrupt_file_is_an_error_and_the_job_still_completes(demo):
    report, _ = demo
    assert report.samples["broken"]["status"] == "error"
    check = report.samples["broken"]["checks"]["duration/video_length"]
    assert "DecodeError" in check.details["exception"]
    assert report.counts() == {"pass": 3, "warn": 0, "fail": 0, "error": 1}
    assert report.job["n_samples"] == 4 and report.job["n_batches"] == 2


def test_summary_reduces_over_all_batches(demo):
    report, _ = demo
    summary = report.summary["duration"]
    assert summary["n_videos"] == 3  # the corrupt file contributes no measurement
    assert summary["total_s"] == pytest.approx(sum(CLIP_SECONDS), abs=DURATION_TOLERANCE_S)
    assert summary["mean_s"] == pytest.approx(sum(CLIP_SECONDS) / 3, abs=DURATION_TOLERANCE_S)
    assert summary["shortest"] == "clip_2s" and summary["longest"] == "clip_10s"


def test_per_sample_artifacts_are_keyed_per_sample(demo):
    _report, artifacts = demo
    assert artifacts["duration/duration_s/clip_5s"] == pytest.approx(5.0, abs=DURATION_TOLERANCE_S)
    assert "load/batch" not in artifacts, "the live Batch must not leak into the job union"


def test_thresholds_live_in_yaml_not_code(videos):
    """Same kernel, stricter YAML: the 10 s clip becomes a failure."""
    report = Executor(_pipeline(videos, max_s=8.0)).run()
    assert report.samples["clip_10s"]["status"] == "fail"
    assert report.samples["clip_10s"]["checks"]["duration/video_length"].details["verdict"] == "too_long"
    assert report.samples["clip_5s"]["status"] == "pass"


def test_cli_run_writes_a_report(videos, tmp_path, capsys):
    yaml_path = tmp_path / "pipeline.yaml"
    out = tmp_path / "report.json"
    yaml_path.write_text(_as_yaml(_pipeline(videos, target=tmp_path / "dumped")))

    assert main(["run", str(yaml_path), "-o", str(out)]) == 0
    doc = json.loads(out.read_text())
    assert doc["job"]["counts"] == {"pass": 3, "warn": 0, "fail": 0, "error": 1}
    assert (tmp_path / "dumped" / "demo" / "dump_report" / "report.json").exists()
    assert "summary[duration]" in capsys.readouterr().out


def test_cli_validate_rejects_a_broken_yaml(tmp_path, capsys):
    bad = tmp_path / "bad.yaml"
    bad.write_text("pipeline:\n  name: x\n  nodes:\n    - name: a\n      kernel: nope.Nope\n")
    assert main(["validate", str(bad)]) == 2
    assert "error:" in capsys.readouterr().err


def _as_yaml(pipeline: Pipeline) -> str:
    import yaml

    return yaml.safe_dump({"pipeline": pipeline.to_dict()}, sort_keys=False)
