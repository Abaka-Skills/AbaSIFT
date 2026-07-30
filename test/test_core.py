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


def test_report_json_shape():
    r = Report({"pipeline": "p"})
    r.apply("n", ReportExt(checks={"s1": {"c": Check("warn", 1.5, {"max": 1.0}, {"why": "x"})}}))
    doc = r.to_json()
    assert doc["schema_version"] == 1
    check = doc["samples"]["s1"]["checks"]["n/c"]
    assert check == {"status": "warn", "measurement": 1.5, "threshold": {"max": 1.0}, "details": {"why": "x"}}


# -- Pipeline -------------------------------------------------------------

_LOAD = {"name": "load", "kernel": "abasift.loaders.FlatDirLoader", "params": {"root": "."}, "inputs": []}
_DUR = {"name": "d", "kernel": "abasift.kernels.VideoDurationKernel", "inputs": ["load"]}


def test_valid_pipeline_topo_order_and_hash_stability():
    p = Pipeline.from_dict({"name": "p", "nodes": [_LOAD, _DUR]})
    assert p.topo_order() == ["load", "d"]
    assert p.source == "load"
    assert p.hash() == Pipeline.from_dict({"name": "p", "nodes": [_LOAD, _DUR]}).hash()


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
        Pipeline.from_dict({"name": "p", "nodes": nodes})


def test_cycle_is_rejected():
    nodes = [
        _LOAD,
        {"name": "a", "kernel": "abasift.kernels.VideoDurationKernel", "inputs": ["b"]},
        {"name": "b", "kernel": "abasift.kernels.VideoDurationKernel", "inputs": ["a"]},
    ]
    with pytest.raises(PipelineError, match="cycle"):
        Pipeline.from_dict({"name": "p", "nodes": nodes})


def test_bad_params_are_caught_at_instantiation():
    p = Pipeline.from_dict(
        {"name": "p", "nodes": [_LOAD, {**_DUR, "params": {"nonsense_param": 1}}]}
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
