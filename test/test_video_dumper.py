"""``VideoDumper``: what it writes, where it writes it, and what it refuses to guess.

Every assertion here reads the *written file back* rather than trusting the check it
reported — an exhibit a vendor will be shown is only worth as much as it is playable.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from conftest import CLIP_SIZE, load_node, run_nodes

from abasift import LazyRaw, Pipeline
from abasift.errors import PipelineError

CLIP_SECONDS = 5
CLIP = f"clip_{CLIP_SECONDS}s"
STACK_FPS = 2  # frames sampled per second by the upstream VideoFrameKernel
STACK_WIDTH = 64
#: What the aspect-preserving resize of the source clip produces.
STACK_SIZE = (STACK_WIDTH, round(CLIP_SIZE[1] * STACK_WIDTH / CLIP_SIZE[0]))

JOB_ID = "video_test"


def _frames_node(**params):
    return {
        "name": "frames",
        "kernel": "abasift.kernels.VideoFrameKernel",
        "params": {"fps": STACK_FPS, "width": STACK_WIDTH, **params},
        "inputs": ["load"],
    }


def _video_node(target, inputs=("frames",), **params):
    return {
        "name": "vid",
        "kernel": "abasift.kernels.VideoDumper",
        "params": {"target": str(target), **params},
        "inputs": list(inputs),
    }


def _nodes(videos, target, **video_params):
    return [load_node(videos), _frames_node(), _video_node(target, **video_params)]


def _pipeline(videos, target, **video_params):
    return Pipeline.from_dict({"job_id": JOB_ID, "nodes": _nodes(videos, target, **video_params)})


def _run(videos, target, **video_params):
    return run_nodes(_nodes(videos, target, **video_params), job_id=JOB_ID)


def _decoded(uri):
    """The written file, read back through the framework's own header decoder."""
    return LazyRaw(uri, "video_meta").decode()


def _first_frame(path):
    import av

    with av.open(str(path)) as container:
        frame = next(container.decode(container.streams.video[0]))
        return frame.to_ndarray(format="rgb24")


@pytest.fixture(scope="module")
def written(videos, tmp_path_factory):
    return _run(videos, tmp_path_factory.mktemp("out"), fps=STACK_FPS)


def test_it_writes_a_playable_video_of_the_stack(written):
    _report, art = written
    meta = _decoded(art[f"vid/video/{CLIP}"].uri)
    assert meta.video["nb_frames"] == CLIP_SECONDS * STACK_FPS
    assert (meta.video["width"], meta.video["height"]) == STACK_SIZE
    assert meta.video["fps"] == STACK_FPS
    assert meta.duration_s == pytest.approx(CLIP_SECONDS, abs=0.05)


def test_the_path_is_the_dumper_scheme_keyed_on_the_job(written):
    report, art = written
    uri = art[f"vid/video/{CLIP}"].uri
    assert uri.endswith(f"/{JOB_ID}_{report.job['pipeline_hash']}/vid/{CLIP}.mp4")
    assert Path(uri).is_file()


def test_the_artifact_is_a_handle_to_what_was_written(written):
    _report, art = written
    handle = art[f"vid/video/{CLIP}"]
    assert isinstance(handle, LazyRaw)
    assert handle.decoder == "video_meta"


def test_the_check_reports_the_exhibit_it_produced(written):
    report, _art = written
    check = report.samples[CLIP]["checks"]["vid/video"]
    assert check.status == "pass"
    assert check.measurement == CLIP_SECONDS * STACK_FPS
    assert check.details["duration_s"] == CLIP_SECONDS
    assert check.details["codec"] == "libx264"
    assert check.details["bytes"] > 0


def test_fps_is_playback_rate_and_is_independent_of_the_sampling_rate(videos, tmp_path):
    """Same stack, five times the playback rate: a fifth of the duration, same frames."""
    _report, art = _run(videos, tmp_path, fps=STACK_FPS * 5)
    meta = _decoded(art[f"vid/video/{CLIP}"].uri)
    assert meta.video["nb_frames"] == CLIP_SECONDS * STACK_FPS
    assert meta.video["fps"] == STACK_FPS * 5
    assert meta.duration_s == pytest.approx(CLIP_SECONDS / 5, abs=0.05)


def test_fps_defaults_to_the_rate_the_stack_was_sampled_at(videos, tmp_path):
    _report, art = _run(videos, tmp_path)
    meta = _decoded(art[f"vid/video/{CLIP}"].uri)
    assert meta.video["fps"] == STACK_FPS
    assert meta.duration_s == pytest.approx(CLIP_SECONDS, abs=0.05)


def test_a_fractional_rate_survives_the_container(videos, tmp_path):
    """29.97 is 30000/1001 — a float would land as 29.969999 or worse."""
    _report, art = _run(videos, tmp_path, fps=29.97)
    assert _decoded(art[f"vid/video/{CLIP}"].uri).video["fps"] == pytest.approx(29.97, abs=1e-4)


def test_the_written_frames_are_the_stack_frames(written):
    """Round-trip the pixels: right content, right order, not a black or shuffled video."""
    _report, art = written
    stack = art[f"frames/frames/{CLIP}"].decode()
    written_first = _first_frame(art[f"vid/video/{CLIP}"].uri)
    assert written_first.shape == stack.data[0].shape
    # H.264 at default quality: close, not identical.
    assert np.abs(written_first.astype(int) - stack.data[0].astype(int)).mean() < 12
    # ...and it is *this* frame, not a later one: testsrc's counter keeps moving.
    later = np.abs(written_first.astype(int) - stack.data[3].astype(int)).mean()
    assert later > 12


def test_a_rerun_to_the_same_target_overwrites_rather_than_duplicates(videos, tmp_path):
    _report, art = _run(videos, tmp_path, fps=STACK_FPS)
    uri = art[f"vid/video/{CLIP}"].uri
    first = Path(uri).read_bytes()

    _report, art = _run(videos, tmp_path, fps=STACK_FPS)
    assert art[f"vid/video/{CLIP}"].uri == uri
    assert Path(uri).read_bytes() == first
    assert len(list(Path(uri).parent.iterdir())) == 3  # one per real clip, none doubled


def test_a_sample_that_failed_upstream_gets_no_video(written):
    report, art = written
    assert "vid/video" not in report.samples["broken"]["checks"]
    assert "vid/video/broken" not in art
    assert report.job["counts"]["error"] == 1


def test_digest_reports_where_the_exhibits_went(written):
    report, art = written
    summary = report.summary["vid"]
    assert summary["n_videos"] == 3
    # The run root, so a reader can find every node's output from one line.
    assert art[f"vid/video/{CLIP}"].uri.startswith(summary["target"] + "/")


def test_without_a_frames_node_upstream_it_is_an_error_not_a_crash(videos, tmp_path):
    """The DAG is the author's job; a missing stack is a per-sample finding."""
    report, _art = run_nodes(
        [load_node(videos), _video_node(tmp_path, inputs=["load"])], job_id="no_frames"
    )
    check = report.samples[CLIP]["checks"]["vid/video"]
    assert check.status == "error"
    assert "VideoFrameKernel" in check.details["exception"]
    assert report.job["counts"]["error"] == 4  # every sample, and the job still completed


def _two_stacks(videos, target, **video_params):
    """Two stacks of the same samples at different rates — a real configuration."""
    slow = dict(_frames_node(fps=1), name="slow")
    fast = dict(_frames_node(fps=4), name="fast")
    return [
        load_node(videos),
        slow,
        fast,
        _video_node(target, inputs=["slow", "fast"], **video_params),
    ]


def test_two_stacks_in_one_dag_is_an_error_with_the_fix_in_the_message(videos, tmp_path):
    report, _art = run_nodes(_two_stacks(videos, tmp_path), job_id="two_stacks")
    check = report.samples[CLIP]["checks"]["vid/video"]
    assert check.status == "error"
    assert "frames_node" in check.details["exception"]


def test_frames_node_says_which_stack_to_write(videos, tmp_path):
    report, art = run_nodes(_two_stacks(videos, tmp_path, frames_node="slow"), job_id="two_stacks")
    assert report.samples[CLIP]["checks"]["vid/video"].status == "pass"
    meta = _decoded(art[f"vid/video/{CLIP}"].uri)
    assert meta.video["nb_frames"] == CLIP_SECONDS * 1  # the slow stack, not the fast one


@pytest.mark.parametrize("params", [{"fps": 0}, {"fps": -2}, {"target": ""}])
def test_nonsense_parameters_fail_at_load_time(videos, params):
    params = dict(params)
    target = params.pop("target", "out")
    with pytest.raises(PipelineError):
        _pipeline(videos, target, **params).instantiate()
