"""Loader contracts: enumeration findings, ordering, and the shared batching helper.

Enumeration is metadata-only, so none of this needs a real media file — which is the
point: a loader that had to decode to enumerate would be doing the wrong thing.
"""

from __future__ import annotations

import pytest

from abasift import Check, Sample
from abasift.kernel import batch_stream
from abasift.loaders import EgoverseDjiLoader, FlatDirLoader
from abasift.loaders._fs import check_order


def _sample(sid: str) -> Sample:
    from abasift import LazyRaw

    return Sample(sid, {"video/main": LazyRaw(f"mem://{sid}", "bytes")})


# -- batch_stream: the batching rule every loader used to re-derive ------


def test_batch_stream_groups_and_flushes_the_tail():
    batches = list(batch_stream([_sample(f"s{i}") for i in range(5)], batch_size=2))
    assert [b["batch"].ids for b, _ in batches] == [("s0", "s1"), ("s2", "s3"), ("s4",)]
    assert [b["batch"].index for b, _ in batches] == [0, 1, 2]


def test_enumeration_findings_ride_along_with_their_batch():
    items = [_sample("s0"), ("bad", Check("error")), _sample("s1")]
    batches = list(batch_stream(items, batch_size=2))
    assert len(batches) == 1
    ext, rext = batches[0]
    assert ext["batch"].ids == ("s0", "s1")
    assert rext.checks["bad"]["enumerate"].status == "error"


def test_a_trailing_batch_of_only_findings_is_still_emitted():
    """The edge case a hand-rolled loader loses: findings after the last full batch."""
    items = [_sample("s0"), _sample("s1"), ("bad", Check("error"))]
    batches = list(batch_stream(items, batch_size=2))
    assert len(batches) == 2
    assert batches[1][0]["batch"].ids == ()
    assert "bad" in batches[1][1].checks


def test_empty_source_yields_nothing():
    assert list(batch_stream([], batch_size=4)) == []


# -- FlatDirLoader --------------------------------------------------------


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "a.mp4").write_bytes(b"x" * 10)
    (tmp_path / "empty.mp4").write_bytes(b"")
    (tmp_path / "notes.txt").write_bytes(b"hello")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.mov").write_bytes(b"x" * 20)
    return tmp_path


def _ids(loader):
    return [sid for ext, _ in loader.iter_batches() for sid in ext["batch"].ids]


def test_flat_loader_is_shallow_by_default(tree):
    assert _ids(FlatDirLoader(root=str(tree))) == ["a"]  # not sub/b, not notes.txt


def test_recursive_descends_into_subdirectories(tree):
    assert _ids(FlatDirLoader(root=str(tree), recursive=True)) == ["a", "sub/b"]


def test_patterns_select_which_files_are_samples(tree):
    assert _ids(FlatDirLoader(root=str(tree), patterns=("*.txt",))) == ["notes"]


def test_zero_size_file_is_an_enumeration_finding_not_a_sample(tree):
    findings = {
        sid: checks
        for _ext, rext in FlatDirLoader(root=str(tree)).iter_batches()
        for sid, checks in rext.checks.items()
    }
    assert findings["empty"]["enumerate"].status == "error"
    assert "zero-size" in findings["empty"]["enumerate"].details["reason"]


def test_order_by_size_is_ascending_and_deterministic(tree):
    loader = FlatDirLoader(root=str(tree), recursive=True, order="size")
    assert _ids(loader) == ["a", "sub/b"]  # 10 bytes then 20


def test_max_samples_caps_enumeration(tree):
    assert _ids(FlatDirLoader(root=str(tree), recursive=True, max_samples=1)) == ["a"]


# -- EgoverseDjiLoader ----------------------------------------------------


@pytest.fixture
def delivery(tmp_path):
    for md5, video in (("aaa", "DJI_1.MP4"), ("bbb", "DJI_2.MP4")):
        d = tmp_path / md5
        d.mkdir()
        (d / video).write_bytes(b"x" * (10 if md5 == "aaa" else 20))
        (d / video.replace(".MP4", ".json")).write_text("{}")
    (tmp_path / "ccc").mkdir()
    (tmp_path / "ccc" / "only.json").write_text("{}")  # no video
    return tmp_path


def test_dji_loader_normalises_a_sample_directory(delivery):
    batches = list(EgoverseDjiLoader(root=str(delivery)).iter_batches())
    samples = {s.sample_id: s for ext, _ in batches for s in ext["batch"]}
    assert set(samples) == {"aaa", "bbb"}
    aaa = samples["aaa"]
    assert set(aaa.streams) == {"video/main", "imu/main", "annotation/task"}
    # The IMU is inside the MP4: same URI, different decoder.
    assert aaa.stream("imu/main").uri == aaa.stream("video/main").uri
    assert aaa.stream("imu/main").decoder == "dji_imu"


def test_directory_without_a_video_is_an_enumeration_finding(delivery):
    findings = {
        sid: checks
        for _ext, rext in EgoverseDjiLoader(root=str(delivery)).iter_batches()
        for sid, checks in rext.checks.items()
    }
    assert findings["ccc"]["enumerate"].status == "error"


def test_dji_loader_orders_by_size(delivery):
    loader = EgoverseDjiLoader(root=str(delivery), order="size")
    assert [s.sample_id for ext, _ in loader.iter_batches() for s in ext["batch"]] == ["aaa", "bbb"]


def test_size_order_ranks_video_less_directories_first(delivery):
    """Known wart, recorded rather than hidden: a directory with no video has size 0, so
    ``order: size`` sorts it ahead of every real sample and ``max_samples`` spends its
    budget on the broken ones first."""
    loader = EgoverseDjiLoader(root=str(delivery), order="size", max_samples=1)
    ext, rext = next(iter(loader.iter_batches()))
    assert ext["batch"].ids == ()
    assert "ccc" in rext.checks


# -- shared param validation ---------------------------------------------


def test_bad_order_is_rejected_at_construction():
    for loader in (FlatDirLoader, EgoverseDjiLoader):
        with pytest.raises(ValueError, match="order must be one of"):
            loader(root=".", order="random")
    assert check_order("size") == "size"
