"""Execution semantics: branch parallelism, decode sharing, the failsafe layers, dumping."""

from __future__ import annotations

import json

import kernels_for_test as kft
import pytest

from abasift import Executor, Pipeline

SYNTH = "kernels_for_test.SyntheticLoader"
TOUCH = "kernels_for_test.TouchKernel"
RECORD = "kernels_for_test.RecordingKernel"


def _run(nodes, name="t", job_id="jid"):
    pipeline = Pipeline.from_dict({"name": name, "job_id": job_id, "nodes": nodes})
    ex = Executor(pipeline)
    return ex.run(), ex.artifacts


@pytest.fixture(autouse=True)
def reset_globals():
    kft.DECODE_COUNTS.clear()
    kft.SEEN.clear()


def test_parallel_branches_share_one_decode_then_release():
    report, _ = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 6, "batch_size": 3}, "inputs": []},
            {"name": "a", "kernel": TOUCH, "inputs": ["load"]},
            {"name": "b", "kernel": TOUCH, "inputs": ["load"]},
        ]
    )
    assert report.counts()["pass"] == 6
    # Two branches over the same sample: decoded once, not twice.
    assert set(kft.DECODE_COUNTS.values()) == {1}
    assert len(kft.DECODE_COUNTS) == 6
    assert all("a/touched" in e["checks"] and "b/touched" in e["checks"] for e in report.samples.values())


def test_join_unions_both_branches():
    _report, artifacts = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 2, "batch_size": 2}, "inputs": []},
            {"name": "a", "kernel": TOUCH, "inputs": ["load"]},
            {"name": "b", "kernel": TOUCH, "inputs": ["load"]},
            {"name": "sink", "kernel": RECORD, "inputs": ["a", "b"]},
        ]
    )
    assert kft.SEEN["sink"] == ["s0", "s1"]
    assert artifacts["sink/seen/0"] == ["s0", "s1"]


def test_failed_sample_is_dropped_downstream_but_batchmates_continue():
    report, _ = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 4, "batch_size": 4}, "inputs": []},
            {"name": "ok", "kernel": TOUCH, "inputs": ["load"]},
            {"name": "boom", "kernel": "kernels_for_test.ExplodingKernel", "inputs": ["load"]},
            {"name": "sink", "kernel": RECORD, "inputs": ["ok", "boom"]},
        ]
    )
    assert report.counts()["error"] == 4
    assert kft.SEEN["sink"] == []  # every sample errored upstream -> nothing reaches sink
    # ...but the healthy node still ran on all of them, and the job completed.
    assert all("ok/touched" in e["checks"] for e in report.samples.values())
    assert all(e["checks"]["boom/boom"].status == "error" for e in report.samples.values())


def test_whole_node_failure_errors_live_samples_only():
    report, _ = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 3, "batch_size": 3}, "inputs": []},
            {"name": "dead", "kernel": "kernels_for_test.WholeNodeExplodingKernel", "inputs": ["load"]},
        ]
    )
    assert report.counts()["error"] == 3
    assert all("dead/error" in e["checks"] for e in report.samples.values())


def test_source_failure_is_recorded_and_the_job_still_reports(monkeypatch):
    def explode(self):
        yield from ()
        raise RuntimeError("listing blew up")

    monkeypatch.setattr(kft.SyntheticLoader, "iter_batches", explode)
    report, _ = _run([{"name": "load", "kernel": SYNTH, "inputs": []}])
    assert "listing blew up" in report.summary["load"]["source_error"]
    assert report.job["n_samples"] == 0


def test_dumper_writes_the_finished_report(tmp_path):
    target = tmp_path / "out"
    report, artifacts = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 2, "batch_size": 1}, "inputs": []},
            {"name": "a", "kernel": TOUCH, "inputs": ["load"]},
            {
                "name": "dump",
                "kernel": "abasift.kernels.DataDumper",
                "params": {"keys": ["__report__"], "target": str(target)},
                "inputs": ["a"],
            },
        ],
        job_id="deterministic",
    )
    path = target / "deterministic" / "dump" / "report.json"
    assert path.exists(), "dump path must be f(job_id, node, key) with no timestamps"
    dumped = json.loads(path.read_text())
    # Dumped after the finalize pass: the job block is complete, not half-written.
    assert dumped["job"]["counts"] == report.counts()
    assert dumped["job"]["n_samples"] == 2
    assert artifacts["dump/report_uri"].endswith("report.json")


def test_dumper_free_mode_drops_the_key(tmp_path):
    _report, artifacts = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 2, "batch_size": 2}, "inputs": []},
            {"name": "a", "kernel": TOUCH, "inputs": ["load"]},
            {"name": "sink", "kernel": RECORD, "inputs": ["a"]},
            {
                "name": "free",
                "kernel": "abasift.kernels.DataDumper",
                "params": {"keys": ["sink/seen/*"], "target": ""},
                "inputs": ["sink"],
            },
        ]
    )
    assert "sink/seen/0" not in artifacts


def test_rerunning_the_same_yaml_overwrites_the_same_paths(tmp_path):
    nodes = [
        {"name": "load", "kernel": SYNTH, "params": {"n": 1, "batch_size": 1}, "inputs": []},
        {
            "name": "dump",
            "kernel": "abasift.kernels.DataDumper",
            "params": {"keys": ["__report__"], "target": str(tmp_path / "out")},
            "inputs": ["load"],
        },
    ]
    _run(nodes, job_id="same")
    before = sorted(p.relative_to(tmp_path) for p in (tmp_path / "out").rglob("*") if p.is_file())
    kft.DECODE_COUNTS.clear()
    _run(nodes, job_id="same")
    after = sorted(p.relative_to(tmp_path) for p in (tmp_path / "out").rglob("*") if p.is_file())
    assert before == after  # idempotent retries, no timestamped duplicates
