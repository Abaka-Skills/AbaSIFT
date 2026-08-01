"""Core contracts: ArtifactUnion, Report aggregation, Pipeline validation, LazyRaw."""

from __future__ import annotations

import pytest

from abasift import (
    ArtifactUnion,
    Batch,
    Check,
    ExecutorError,
    LazyRaw,
    Pipeline,
    PipelineError,
    Report,
    ReportExt,
    Sample,
)
from abasift.report import worst


# -- ArtifactUnion --------------------------------------------------------


def test_union_is_extend_only():
    u = ArtifactUnion().extended("a", {"x": 1})
    assert u["a/x"] == 1
    with pytest.raises(ExecutorError, match="overwrite"):
        u.extended("a", {"x": 2})


def test_diamond_duplicates_merge_but_conflicts_raise():
    base = ArtifactUnion().extended("load", {"n": 3})
    left = base.extended("l", {"v": 1})
    right = base.extended("r", {"v": 1})
    merged = left.union(right)
    assert merged["load/n"] == 3 and merged["l/v"] == 1 and merged["r/v"] == 1

    other = ArtifactUnion().extended("load", {"n": 4})
    with pytest.raises(ExecutorError, match="two different values"):
        base.union(other)


def test_lazyraw_duplicates_are_equal_by_value():
    a = ArtifactUnion().extended("x", {"v": LazyRaw("s3://b/k", "json")})
    b = ArtifactUnion().extended("x", {"v": LazyRaw("s3://b/k", "json")})
    assert a.union(b)["x/v"].uri == "s3://b/k"


def test_deletion_survives_a_later_join():
    """A freed key must not be resurrected by a branch that still carries it."""
    base = ArtifactUnion().extended("load", {"heavy": b"payload"})
    freed = base.with_mutations(delete=frozenset({"load/heavy"}))
    assert "load/heavy" not in freed
    assert "load/heavy" not in freed.union(base)


def test_replace_requires_an_existing_key():
    u = ArtifactUnion().extended("a", {"x": 1})
    assert u.with_mutations(replace={"a/x": 2})["a/x"] == 2
    with pytest.raises(ExecutorError, match="absent"):
        u.with_mutations(replace={"a/nope": 2})


def test_batch_lookup_and_transient_removal():
    batch = Batch((_sample("s1"),), 0)
    u = ArtifactUnion().extended("load", {"batch": batch, "n": 1})
    assert u.batch() is batch
    stripped = u.without_transients()
    assert "load/batch" not in stripped and stripped["load/n"] == 1
    with pytest.raises(ExecutorError, match="no Batch"):
        stripped.batch()


def test_under_returns_one_namespace():
    u = ArtifactUnion().extended("a", {"x": 1, "y": 2}).extended("b", {"x": 3})
    assert u.under("a") == {"x": 1, "y": 2}


def test_find_lazy_matches_by_decoder_and_sample_not_by_node():
    """A consumer names the payload it can read; the producing node's name is the YAML's."""
    stack = LazyRaw("file:///tmp/a", "video_frames", n=1)
    other = LazyRaw("file:///tmp/b", "video_file")
    u = (
        ArtifactUnion()
        .extended("slow", {"frames/dir/clip": stack})  # a sample id may contain slashes
        .extended("fast", {"frames/dir/clip": LazyRaw("file:///tmp/c", "video_frames")})
        .extended("raw", {"file/dir/clip": other, "frames/dir/other": stack})
    )
    assert set(u.find_lazy("dir/clip", "video_frames")) == {
        "slow/frames/dir/clip",
        "fast/frames/dir/clip",
    }
    assert u.find_lazy("dir/clip", "video_frames", node="slow") == {"slow/frames/dir/clip": stack}
    assert u.find_lazy("dir/clip", "dji_imu") == {}
    assert u.find_lazy("nobody", "video_frames") == {}


# -- Report ---------------------------------------------------------------


def test_sample_status_is_worst_of_checks():
    assert worst([]) == "pass"
    assert worst(["pass", "warn"]) == "warn"
    assert worst(["warn", "fail"]) == "fail"
    assert worst(["fail", "error"]) == "error"

    r = Report()
    ext = ReportExt()
    ext.add("s1", "a", Check("pass", 1))
    ext.add("s1", "b", Check("fail", 2))
    r.apply("node", ext)
    assert r.samples["s1"]["status"] == "fail"
    assert set(r.samples["s1"]["checks"]) == {"node/a", "node/b"}


def test_batch_reports_merge_by_dict_union():
    a, b = Report(), Report()
    a.apply("n", ReportExt(checks={"s1": {"c": Check("pass")}}))
    b.apply("n", ReportExt(checks={"s2": {"c": Check("error")}}))
    a.merge(b)
    assert a.counts() == {"pass": 1, "warn": 0, "fail": 0, "error": 1}


def test_merge_does_not_alias_the_other_report():
    a, b = Report(), Report()
    b.apply("n", ReportExt(checks={"s1": {"c": Check("pass")}}))
    a.merge(b)
    a.apply("m", ReportExt(checks={"s1": {"c": Check("fail")}}))
    assert b.samples["s1"]["status"] == "pass"  # untouched
    assert a.samples["s1"]["status"] == "fail"


def test_report_json_is_per_sample_only():
    """Anything identical across samples belongs in the pipeline document, not here."""
    r = Report({"pipeline": "p"})
    r.apply("n", ReportExt(checks={"s1": {"c": Check("warn", 1.5, {"max": 1.0}, {"why": "x"})}}))
    doc = r.to_json()
    assert doc["schema_version"] == 2
    assert set(doc) == {"schema_version", "job", "samples"}
    check = doc["samples"]["s1"]["checks"]["n/c"]
    assert check == {"status": "warn", "measurement": 1.5, "details": {"why": "x"}}


def test_pipeline_json_is_per_pipeline_only():
    """The other half: the definition, the thresholds behind the verdicts, the tallies."""
    definition = {
        "job_id": "j",
        "nodes": [{"name": "n", "kernel": "pkg.K", "params": {"max": 1.0}, "inputs": ["load"]}],
    }
    r = Report({"pipeline": "p"}, definition=definition)
    r.apply("n", ReportExt(checks={"s1": {"c": Check("warn", 1.5, {"max": 1.0})}}))
    r.apply("n", ReportExt(checks={"s2": {"c": Check("pass", 0.5, {"max": 1.0})}}))
    r.apply("n", ReportExt(summary={"mean": 1.0}))

    doc = r.pipeline_json()
    assert set(doc) == {"schema_version", "job", "pipeline", "nodes"}
    assert doc["pipeline"] == {"job_id": "j"}, "the node list is not repeated here"

    # One entry per node, carrying what it is *and* what it did — never listed twice.
    (node,) = doc["nodes"]
    assert node["name"] == "n" and node["kernel"] == "pkg.K" and node["params"] == {"max": 1.0}
    assert node["summary"] == {"mean": 1.0}
    assert node["counts"] == {"warn": 1, "pass": 1}
    assert node["checks"] == {"c": {"counts": {"warn": 1, "pass": 1}, "threshold": {"max": 1.0}}}
    assert "samples" not in doc


def test_a_nodes_counts_are_worst_of_its_own_checks_per_sample():
    """Two checks on one node must not count one sample twice."""
    definition = {"job_id": "j", "nodes": [{"name": "n", "kernel": "pkg.K", "inputs": []}]}
    r = Report({}, definition=definition)
    r.apply("n", ReportExt(checks={"s1": {"a": Check("pass"), "b": Check("fail")}}))
    r.apply("n", ReportExt(checks={"s2": {"a": Check("pass"), "b": Check("pass")}}))

    (node,) = r.pipeline_json()["nodes"]
    assert node["counts"] == {"fail": 1, "pass": 1}, "two samples, judged once each"
    assert sum(node["counts"].values()) == 2
    # The per-check tallies still say what each check decided, and sum to 4.
    assert node["checks"]["a"]["counts"] == {"pass": 2}
    assert node["checks"]["b"]["counts"] == {"fail": 1, "pass": 1}


def test_a_node_that_judged_nothing_says_nothing():
    """A loader or a writer has no verdicts; empty `checks: {}` would just be noise."""
    definition = {"job_id": "p", "nodes": [{"name": "load", "kernel": "pkg.L", "inputs": []}]}
    doc = Report({}, definition=definition).pipeline_json()
    assert doc["nodes"] == [{"name": "load", "kernel": "pkg.L", "inputs": []}]


# -- Pipeline -------------------------------------------------------------

_LOAD = {"name": "load", "kernel": "abasift.loaders.FlatDirLoader", "params": {"root": "."}, "inputs": []}
_DUR = {"name": "d", "kernel": "abasift.kernels.VideoDurationKernel", "inputs": ["load"]}


def test_valid_pipeline_topo_order_and_hash_stability():
    p = Pipeline.from_dict({"job_id": "p", "nodes": [_LOAD, _DUR]})
    assert p.topo_order() == ["load", "d"]
    assert p.source == "load"
    assert p.hash() == Pipeline.from_dict({"job_id": "p", "nodes": [_LOAD, _DUR]}).hash()


@pytest.mark.parametrize(
    "nodes, match",
    [
        ([_LOAD, _LOAD], "duplicate node names"),
        ([_LOAD, {**_DUR, "inputs": ["nope"]}], "unknown inputs"),
        ([_LOAD, _DUR, {**_LOAD, "name": "load2"}], "exactly one source"),
        ([{**_DUR, "inputs": []}], "not a SourceKernel"),
        ([_LOAD, {"name": "x", "kernel": "no.such.module.K", "inputs": ["load"]}], "cannot import"),
        ([_LOAD, {"name": "x", "kernel": "abasift.Report", "inputs": ["load"]}], "not a Kernel"),
    ],
)
def test_invalid_pipelines_fail_fast(nodes, match):
    with pytest.raises(PipelineError, match=match):
        Pipeline.from_dict({"job_id": "p", "nodes": nodes})


def test_cycle_is_rejected():
    nodes = [
        _LOAD,
        {"name": "a", "kernel": "abasift.kernels.VideoDurationKernel", "inputs": ["b"]},
        {"name": "b", "kernel": "abasift.kernels.VideoDurationKernel", "inputs": ["a"]},
    ]
    with pytest.raises(PipelineError, match="cycle"):
        Pipeline.from_dict({"job_id": "p", "nodes": nodes})


def _two_archivers(a_keys, b_keys, b_inputs, b_target="/tmp/out"):
    return {
        "job_id": "p",
        "nodes": [
            _LOAD,
            {**_DUR, "name": "dur", "inputs": ["load"]},
            {
                "name": "dA",
                "kernel": "abasift.kernels.DataArchiver",
                "params": {"keys": a_keys, "target": "/tmp/out"},
                "inputs": ["dur"],
            },
            {
                "name": "dB",
                "kernel": "abasift.kernels.DataArchiver",
                "params": {"keys": b_keys, "target": b_target},
                "inputs": b_inputs,
            },
        ],
    }


@pytest.mark.parametrize(
    "spec",
    [
        _two_archivers(["dur/duration_s/*"], ["dur/duration_s/*"], ["dur"]),  # same glob
        _two_archivers(["dur/*"], ["dur/duration_s/*"], ["dur"]),  # one subsumes the other
    ],
    ids=["same-glob", "overlapping-globs"],
)
def test_two_archivers_replacing_one_key_on_parallel_branches_is_a_load_error(spec):
    """One key, two values, no honest winner — so it fails before the job starts."""
    with pytest.raises(PipelineError, match="parallel branches"):
        Pipeline.from_dict(spec)


@pytest.mark.parametrize(
    "spec",
    [
        _two_archivers(["dur/duration_s/*"], ["load/batch"], ["dur"]),  # disjoint keys
        _two_archivers(["dur/duration_s/*"], ["dur/duration_s/*"], ["dA"]),  # ordered, not parallel
        _two_archivers(["dur/duration_s/*"], ["dur/duration_s/*"], ["dur"], b_target=""),  # free
        _two_archivers(["__report__"], ["__report__"], ["dur"]),  # pseudo-keys never in the union
    ],
    ids=["disjoint", "ordered", "free-mode", "report-only"],
)
def test_archivers_that_cannot_collide_are_allowed(spec):
    assert Pipeline.from_dict(spec).source == "load"


def test_bad_params_are_caught_at_instantiation():
    p = Pipeline.from_dict(
        {"job_id": "p", "nodes": [_LOAD, {**_DUR, "params": {"nonsense_param": 1}}]}
    )
    with pytest.raises(PipelineError, match="bad params"):
        p.instantiate()


# -- LazyRaw / Sample -----------------------------------------------------


def test_lazyraw_is_serializable_and_value_typed():
    raw = LazyRaw("s3://b/k.mp4", "video_meta", block_size=1024)
    assert LazyRaw.from_json(raw.to_json()) == raw
    assert raw != LazyRaw("s3://b/k.mp4", "dji_imu")
    assert len({raw, LazyRaw("s3://b/k.mp4", "video_meta", block_size=1024)}) == 1


def test_decode_is_memoized_then_released(tmp_path):
    path = tmp_path / "a.json"
    path.write_text('{"v": 1}')
    raw = LazyRaw(str(path), "json")
    first = raw.decode()
    assert raw.decode() is first and raw.is_decoded
    raw.release()
    assert not raw.is_decoded
    assert raw.decode() == {"v": 1}


def test_read_bytes_refuses_oversized_payloads(tmp_path):
    path = tmp_path / "big.bin"
    path.write_bytes(b"x" * 4096)
    with pytest.raises(Exception, match="max_bytes"):
        LazyRaw(str(path), "bytes").read_bytes(max_bytes=1024)


def test_stream_names_must_be_canonical():
    with pytest.raises(PipelineError, match="kind/name"):
        Sample("s1", {"video": LazyRaw("x", "bytes")})
    with pytest.raises(PipelineError, match="kind/name"):
        Sample("s1", {"telemetry/main": LazyRaw("x", "bytes")})
    Sample("s1", {"imu/main": LazyRaw("x", "bytes")})  # ok


def _sample(sid: str) -> Sample:
    return Sample(sid, {"video/main": LazyRaw(f"mem://{sid}", "bytes")})
