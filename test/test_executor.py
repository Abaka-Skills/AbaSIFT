"""Execution semantics: branch parallelism, decode sharing, the failsafe layers, archiving."""

from __future__ import annotations

import json
import time

import kernels_for_test as kft
import pytest

from abasift import Executor, Pipeline

SYNTH = "kernels_for_test.SyntheticLoader"
TOUCH = "kernels_for_test.TouchKernel"
RECORD = "kernels_for_test.RecordingKernel"


def _run(nodes, name="t", job_id="jid"):
    pipeline = Pipeline.from_dict({"job_id": job_id, "nodes": nodes})
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
                "kernel": "abasift.kernels.DataArchiver",
                "params": {"keys": ["__report__"], "target": str(target)},
                "inputs": ["a"],
            },
        ],
        job_id="deterministic",
    )
    job_dir = f"deterministic_{report.job['pipeline_hash']}"
    path = target / job_dir / "dump" / "report.json"
    assert path.exists(), "dump path must be f(job_id, hash, node, key) with no timestamps"
    dumped = json.loads(path.read_text())
    # Dumped after the digest pass: the job block is complete, not half-written.
    assert dumped["job"]["counts"] == report.counts()
    assert dumped["job"]["n_samples"] == 2
    assert artifacts["dump/report_uri"].endswith("report.json")


def test_dump_target_defaults_to_a_job_tree_with_a_run_per_timestamp(tmp_path, monkeypatch):
    """No `target:` -> ./dump/<job_id>_<hash>/<unix ts>/<node>/, never an absolute path.

    Job first, run second: every run of a shard is then listable in one directory.
    """
    monkeypatch.chdir(tmp_path)  # the default is relative to the working directory
    before = int(time.time())
    report, _artifacts = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 1, "batch_size": 1}, "inputs": []},
            {"name": "dump", "kernel": "abasift.kernels.DataArchiver", "inputs": ["load"]},
        ],
        job_id="stamped",
    )
    stamp = report.job["started_unix"]
    assert isinstance(stamp, int) and before <= stamp <= int(time.time())
    job_root = tmp_path / "dump" / f"stamped_{report.job['pipeline_hash']}"
    assert (job_root / str(stamp) / "dump" / "report.json").exists()
    assert (job_root / str(stamp) / "dump" / "pipeline.json").exists()
    # The config sits beside the runs, not inside one: they all share it.
    assert (job_root / "pipeline.yaml").exists()


def test_an_explicit_target_is_used_verbatim_and_undated(tmp_path):
    """The no-timestamps rule still holds where it matters: a configured target."""
    target = tmp_path / "out"
    report, _ = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 1, "batch_size": 1}, "inputs": []},
            {
                "name": "dump",
                "kernel": "abasift.kernels.DataArchiver",
                "params": {"target": str(target)},
                "inputs": ["load"],
            },
        ],
        job_id="pinned",
    )
    assert (target / f"pinned_{report.job['pipeline_hash']}" / "dump" / "report.json").exists()
    assert not list(target.glob("[0-9]" * 8)), "an explicit target must not gain a stamp segment"


def test_dumper_free_mode_drops_the_key(tmp_path):
    _report, artifacts = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 2, "batch_size": 2}, "inputs": []},
            {"name": "a", "kernel": TOUCH, "inputs": ["load"]},
            {"name": "sink", "kernel": RECORD, "inputs": ["a"]},
            {
                "name": "free",
                "kernel": "abasift.kernels.DataArchiver",
                "params": {"keys": ["sink/seen/*"], "target": ""},
                "inputs": ["sink"],
            },
        ]
    )
    assert "sink/seen/0" not in artifacts


def test_archive_mode_replaces_the_key_the_job_union_carries(tmp_path):
    """A replacement must win over the pre-mutation copy its own ancestor still holds.

    Only the *leaf* unions are merged per batch, so the dumped node's stale value never
    gets a vote. Merging every node instead is what used to make this an ExecutorError.
    """
    target = tmp_path / "out"
    _report, artifacts = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 2, "batch_size": 2}, "inputs": []},
            {"name": "sink", "kernel": RECORD, "inputs": ["load"]},
            {
                "name": "dump",
                "kernel": "abasift.kernels.DataArchiver",
                "params": {"keys": ["sink/seen/*"], "target": str(target)},
                "inputs": ["sink"],
            },
        ]
    )
    handle = artifacts["sink/seen/0"]
    assert handle.decoder == "json"  # no longer the list the kernel returned
    assert handle.decode() == ["s0", "s1"]  # ...but the same information, on disk


def test_a_delete_in_one_branch_is_not_resurrected_by_its_sibling(tmp_path):
    """Merging leaves is not enough on its own: sibling leaves disagree, and delete wins.

    `free` removed the key; `reader` is a *sibling* leaf that never saw the removal and
    still carries it. The union's sticky `deleted` set is what settles it.
    """
    _report, artifacts = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 2, "batch_size": 2}, "inputs": []},
            {"name": "mid", "kernel": RECORD, "inputs": ["load"]},
            {"name": "reader", "kernel": TOUCH, "inputs": ["mid"]},
            {
                "name": "free",
                "kernel": "abasift.kernels.DataArchiver",
                "params": {"keys": ["mid/seen/*"], "target": ""},
                "inputs": ["mid"],
            },
        ]
    )
    assert "mid/seen/0" not in artifacts


def test_a_mid_graph_node_still_reaches_the_job_union(tmp_path):
    """Merging leaves must not lose the middle of the graph — leaves already contain it."""
    report, artifacts = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 2, "batch_size": 2}, "inputs": []},
            {"name": "mid", "kernel": RECORD, "inputs": ["load"]},
            {"name": "leaf", "kernel": RECORD, "inputs": ["mid"]},
            {"name": "other", "kernel": RECORD, "inputs": ["load"]},
        ]
    )
    assert artifacts["mid/seen/0"] == ["s0", "s1"]  # mid is not a leaf, and is still here
    assert artifacts["leaf/seen/0"] == ["s0", "s1"]
    assert artifacts["other/seen/0"] == ["s0", "s1"]  # a second leaf still contributes
    assert not report.samples  # these kernels judge nothing at all; findings tested below


def test_a_mid_graph_nodes_findings_still_reach_the_job_report():
    """The report is merged over leaves too — and a leaf carries its ancestors' checks."""
    report, _artifacts = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 2, "batch_size": 2}, "inputs": []},
            {"name": "mid", "kernel": TOUCH, "inputs": ["load"]},
            {"name": "leaf", "kernel": TOUCH, "inputs": ["mid"]},
        ]
    )
    for entry in report.samples.values():
        assert set(entry["checks"]) == {"mid/touched", "leaf/touched"}


def test_the_two_documents_split_per_sample_from_per_pipeline(tmp_path):
    """`report.json` says what happened to each sample; `pipeline.json` what was run."""
    target = tmp_path / "out"
    report, _ = _run(
        [
            {"name": "load", "kernel": SYNTH, "params": {"n": 2, "batch_size": 2}, "inputs": []},
            {"name": "a", "kernel": TOUCH, "inputs": ["load"]},
            {
                "name": "dump",
                "kernel": "abasift.kernels.DataArchiver",
                "params": {"target": str(target)},  # default keys = both documents
                "inputs": ["a"],
            },
        ],
        job_id="split",
    )
    job_root = target / f"split_{report.job['pipeline_hash']}"
    samples = json.loads((job_root / "dump" / "report.json").read_text())
    pipeline = json.loads((job_root / "dump" / "pipeline.json").read_text())

    assert set(samples) == {"schema_version", "job", "samples"}
    assert len(samples["samples"]) == 2
    assert "threshold" not in json.dumps(samples), "identical across samples -> not per sample"

    assert set(pipeline) == {"schema_version", "job", "pipeline", "nodes"}
    assert "samples" not in pipeline
    (node,) = [n for n in pipeline["nodes"] if n["name"] == "a"]
    assert node["kernel"] == TOUCH  # what the node is...
    assert node["checks"]["touched"]["counts"] == {"pass": 2}  # ...and what it did
    assert [n["name"] for n in pipeline["nodes"]] == ["load", "a", "dump"], "each node once"
    assert pipeline["job"] == samples["job"], "same job block, so either file reads alone"
    # The config that produced them, beside the runs rather than inside one.
    assert (job_root / "pipeline.yaml").exists()


def test_an_edited_yaml_dumps_beside_the_old_run_rather_than_over_it(tmp_path):
    """`{job_id}_{hash}`: a retry overwrites, an *edit* lands somewhere new.

    Same shard id, different thresholds — overwriting would destroy the artifacts behind
    a verdict someone may already have read.
    """
    target = tmp_path / "out"

    def run_with(n):
        return _run(
            [
                {"name": "load", "kernel": SYNTH, "params": {"n": n, "batch_size": 1}, "inputs": []},
                {
                    "name": "dump",
                    "kernel": "abasift.kernels.DataArchiver",
                    "params": {"keys": ["__report__"], "target": str(target)},
                    "inputs": ["load"],
                },
            ],
            job_id="shard_7",
        )[0]

    first, second = run_with(1), run_with(2)
    dirs = sorted(p.name for p in target.iterdir())
    assert dirs == sorted({f"shard_7_{first.job['pipeline_hash']}", f"shard_7_{second.job['pipeline_hash']}"})
    assert len(dirs) == 2, "the same id under a different definition must not clobber"


def test_rerunning_the_same_yaml_overwrites_the_same_paths(tmp_path):
    nodes = [
        {"name": "load", "kernel": SYNTH, "params": {"n": 1, "batch_size": 1}, "inputs": []},
        {
            "name": "dump",
            "kernel": "abasift.kernels.DataArchiver",
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
