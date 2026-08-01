"""``VideoFrameKernel``: the sampling contract, and the memory contract.

The sampling half is arithmetic on synthesized clips (10 fps, 160x120, 2/5/10 s). The
memory half is the reason the kernel exists in this shape: what lands in the union is a
handle, and what a reader gets is a memory map — never an array copied into the job union.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from conftest import CLIP_FPS, CLIP_SECONDS as CLIP_LENGTHS, CLIP_SIZE, load_node, run_nodes

from abasift import LazyRaw, Pipeline
from abasift.payloads import VideoFrames

#: The clip these tests do arithmetic on. The others (2 s, 10 s) are asserted as a set.
CLIP_SECONDS = 5
CLIP = f"clip_{CLIP_SECONDS}s"
CLIP_FRAMES = CLIP_SECONDS * CLIP_FPS  # what a default (full-rate) run yields for it


def _nodes(videos, **params):
    return [
        load_node(videos),
        {
            "name": "frames",
            "kernel": "abasift.kernels.VideoFrameKernel",
            "params": params,
            "inputs": ["load"],
        },
    ]


def _pipeline(videos, **params):
    return Pipeline.from_dict({"job_id": "frames_test", "nodes": _nodes(videos, **params)})


def _run(videos, **params):
    return run_nodes(_nodes(videos, **params), job_id="frames_test")


@pytest.fixture(scope="module")
def default_run(videos):
    return _run(videos)


def test_default_keeps_every_frame_at_the_source_resolution(default_run):
    """Silence means a full decode: subsampling can only ever lose a defect."""
    _report, art = default_run
    frames = art[f"frames/frames/{CLIP}"].decode()
    assert isinstance(frames, VideoFrames)
    assert len(frames) == CLIP_FRAMES
    assert frames.data.shape == (CLIP_FRAMES, CLIP_SIZE[1], CLIP_SIZE[0], 3)
    assert frames.data.dtype == np.uint8
    assert frames.size == CLIP_SIZE
    assert frames.fps == CLIP_FPS
    assert frames.t.tolist()[:3] == [0.0, 0.1, 0.2]


def test_the_union_carries_a_handle_not_an_array(default_run):
    """The memory contract: an ndarray here would be held for the whole job."""
    _report, art = default_run
    handle = art[f"frames/frames/{CLIP}"]
    assert isinstance(handle, LazyRaw)
    assert handle.decoder == "video_frames"
    assert handle.to_json()["opts"]["n"] == CLIP_FRAMES


def test_decoded_frames_are_memory_mapped(default_run):
    _report, art = default_run
    frames = art[f"frames/frames/{CLIP}"].decode()
    assert isinstance(frames.data, np.memmap)


def test_every_clip_reports_pass_with_its_frame_count(default_run):
    report, _art = default_run
    for secs in CLIP_LENGTHS:
        check = report.samples[f"clip_{secs}s"]["checks"]["frames/frames"]
        assert check.status == "pass"
        assert check.measurement == secs * CLIP_FPS
        assert check.details["source"]["fps"] == CLIP_FPS


def test_an_explicit_fps_subsamples_by_presentation_time(videos):
    _report, art = _run(videos, fps=1)
    frames = art[f"frames/frames/{CLIP}"].decode()
    assert len(frames) == CLIP_SECONDS  # 0s, 1s, ... 4s
    assert frames.fps == 1.0
    assert frames.t.tolist() == [0.0, 1.0, 2.0, 3.0, 4.0]


def test_fps_above_the_source_rate_cannot_invent_more_frames_than_it_asks_for(videos):
    _report, art = _run(videos, fps=5)
    frames = art[f"frames/frames/{CLIP}"].decode()
    assert len(frames) == CLIP_SECONDS * 5


def test_width_alone_preserves_the_aspect_ratio(videos):
    _report, art = _run(videos, width=80)
    frames = art[f"frames/frames/{CLIP}"].decode()
    assert frames.size == (80, 60)


def test_both_dimensions_are_honoured_verbatim(videos):
    _report, art = _run(videos, width=64, height=64)
    frames = art[f"frames/frames/{CLIP}"].decode()
    assert frames.size == (64, 64)


def test_the_stack_is_cached_by_what_it_is_so_a_rerun_reuses_it(videos):
    """Deterministic key -> a retried job re-decodes nothing."""
    _r1, art1 = _run(videos, width=80)
    handle = art1[f"frames/frames/{CLIP}"]
    written = Path(handle.local_path()).read_bytes()

    _r2, art2 = _run(videos, width=80)
    assert art2[f"frames/frames/{CLIP}"].uri == handle.uri
    assert Path(handle.local_path()).read_bytes() == written

    # A different request is a different file, not an overwrite.
    _r3, art3 = _run(videos, width=64)
    assert art3[f"frames/frames/{CLIP}"].uri != handle.uri


def test_a_cache_directory_with_a_space_still_yields_a_readable_handle(videos, tmp_path):
    """The handle's URI and the cache's path have to be the same spelling, unencoded."""
    from abasift.cache import DiskCache, set_disk_cache

    set_disk_cache(DiskCache(tmp_path / "cache dir"))
    _report, art = _run(videos)
    assert " " in art[f"frames/frames/{CLIP}"].uri
    assert len(art[f"frames/frames/{CLIP}"].decode()) == CLIP_FRAMES


def test_a_corrupt_file_is_an_error_sample_and_the_job_completes(default_run):
    report, art = default_run
    assert report.samples["broken"]["checks"]["frames/frames"].status == "error"
    assert "frames/frames/broken" not in art
    assert report.job["counts"]["error"] == 1


def test_digest_sums_what_the_disk_cache_now_holds(default_run):
    report, _art = default_run
    summary = report.summary["frames"]
    assert summary["n_videos"] == 3
    frames = sum(CLIP_LENGTHS) * CLIP_FPS             # default: every frame of every clip
    assert summary["n_frames"] == frames
    assert summary["cache_bytes"] == frames * CLIP_SIZE[0] * CLIP_SIZE[1] * 3


def _with_archiver(videos, target):
    return run_nodes(
        _nodes(videos)
        + [
            {
                "name": "dump",
                "kernel": "abasift.kernels.DataArchiver",
                "params": {"keys": ["frames/frames/*"], "target": target},
                "inputs": ["frames"],
            }
        ],
        job_id="frames_test",
    )


def test_free_mode_deletes_the_stack_from_the_disk_cache(videos):
    """A stack is an intermediate: nothing downstream reads it, so the archiver frees it."""
    _report, art = _run(videos)
    path = Path(art[f"frames/frames/{CLIP}"].local_path())
    assert path.exists()

    _report, art = _with_archiver(videos, "")
    assert f"frames/frames/{CLIP}" not in art
    assert not path.exists()


def test_a_dumped_stack_still_decodes(videos, tmp_path):
    """Archive mode over a real artifact key: the handle in the union points at the target."""
    _report, art = _with_archiver(videos, str(tmp_path / "out"))
    handle = art[f"frames/frames/{CLIP}"]
    assert handle.uri.startswith(str(tmp_path / "out"))
    frames = handle.decode()
    assert len(frames) == CLIP_FRAMES
    assert frames.data.shape == (CLIP_FRAMES, CLIP_SIZE[1], CLIP_SIZE[0], 3)


@pytest.mark.parametrize("params", [{"fps": 0}, {"fps": -1}, {"width": 0}, {"height": -4}])
def test_nonsense_parameters_fail_at_load_time(videos, params):
    from abasift.errors import PipelineError

    with pytest.raises(PipelineError):
        _pipeline(videos, **params).instantiate()
