"""What a job prints around the work, and the YAML-configured infrastructure behind it.

These exist to answer, from a worker log alone: where is this writing, how much disk may
it take, what is it about to run — and afterwards, which node produced the failures. So
the tests check the banner reports the cache the job *resolved*, not the one the YAML
asked for (those differ whenever the YAML is silent), and that the closing tallies are the
pipeline document's own numbers rather than a second reckoning that could drift from it.
"""

from __future__ import annotations

import logging
import re

import pytest

from abasift import Executor, Pipeline
from abasift.cache import DiskCache, disk_cache, set_disk_cache
from abasift.cli import ColorFormatter, color_enabled, main, run_banner, run_results
from abasift.errors import PipelineError

SYNTH = "kernels_for_test.SyntheticLoader"
TOUCH = "kernels_for_test.TouchKernel"

DIAMOND = {
    "job_id": "bd",
    "nodes": [
        {"name": "load", "kernel": SYNTH, "params": {"n": 2, "batch_size": 2}, "inputs": []},
        {"name": "a", "kernel": TOUCH, "inputs": ["load"]},
        {"name": "b", "kernel": TOUCH, "inputs": ["load"]},
        {"name": "sink", "kernel": "kernels_for_test.RecordingKernel", "inputs": ["a", "b"]},
    ],
}


def banner_for(spec, cache=None, workers=4, yaml_path="p.yaml", color=False):
    pipeline = Pipeline.from_dict(spec)
    return run_banner(pipeline, cache or disk_cache(), workers, yaml_path, color=color)


RAILS = "●│├┤╭╮╰╯┬┴┼─ "  # everything the edge gutter is drawn from


def _inside(banner: str) -> list[str]:
    """The framed DAG's rows, with the frame itself peeled off."""
    return [ln.strip()[1:-1] for ln in banner.splitlines() if ln.strip().startswith("│")]


def graph_lines(banner: str) -> list[str]:
    """What the labelled rows say, without the rail gutter — link rows drop out."""
    return [row for row in (r.lstrip(RAILS).rstrip() for r in _inside(banner)) if row]


def rail_lines(banner: str) -> list[str]:
    """The gutter alone: how the edges are drawn, one string per row."""
    return [r[: len(r) - len(r.lstrip(RAILS))].strip() for r in _inside(banner)]


# -- cache configuration ------------------------------------------------------


def test_cache_settings_are_read_from_the_yaml(tmp_path):
    p = Pipeline.from_dict(DIAMOND | {"cache": {"dir": str(tmp_path / "c"), "size_gb": 4}})
    assert p.cache == {"dir": str(tmp_path / "c"), "size_gb": 4.0}


def test_a_yaml_cache_is_installed_before_any_io(tmp_path):
    """The executor resolves it at construction, so nothing can read data first."""
    set_disk_cache(None)
    root = tmp_path / "scratch"
    ex = Executor(Pipeline.from_dict(DIAMOND | {"cache": {"dir": str(root), "size_gb": 2}}))
    assert ex.cache.root == root and ex.cache.capacity_bytes == 2 * 2**30
    assert disk_cache() is ex.cache, "the worker-global cache is the one the job configured"
    assert root.is_dir()
    set_disk_cache(None)


def test_overriding_the_job_id_keeps_the_cache_settings(tmp_path, capsys, monkeypatch):
    """`--job-id` rebuilds the Pipeline; every other field has to survive that."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "p.yaml").write_text(
        "pipeline:\n  job_id: keeps\n"
        f"  cache: {{dir: {tmp_path / 'scratch'}, size_gb: 5}}\n"
        f"  nodes:\n    - {{name: load, kernel: {SYNTH}, inputs: []}}\n"
    )
    assert main(["run", "p.yaml", "--job-id", "override"]) == 0
    out = capsys.readouterr().out
    assert "5.0 GiB cap  (yaml)" in out and str(tmp_path / "scratch") in out
    set_disk_cache(None)


def test_a_pipeline_that_says_nothing_leaves_the_cache_alone(tmp_path):
    """Silence must not trample a cache the surrounding process installed on purpose."""
    mine = DiskCache(tmp_path / "mine")
    set_disk_cache(mine)
    ex = Executor(Pipeline.from_dict(DIAMOND))
    assert ex.cache is mine
    set_disk_cache(None)


@pytest.mark.parametrize(
    "cache, message",
    [
        ({"size_g": 4}, "unknown keys"),
        ({"size_gb": "big"}, "must be a number"),
        ({"size_gb": 0}, "must be positive"),
        ("/scratch", "must be a mapping"),
    ],
)
def test_a_bad_cache_block_fails_at_load_time(cache, message):
    """Strict like kernel params: a typo is a load error, not a silent default."""
    with pytest.raises(PipelineError, match=message):
        Pipeline.from_dict(DIAMOND | {"cache": cache})


def test_the_cache_block_is_part_of_the_definition(tmp_path):
    plain = Pipeline.from_dict(DIAMOND)
    configured = Pipeline.from_dict(DIAMOND | {"cache": {"dir": str(tmp_path)}})
    assert "cache" not in plain.to_dict(), "silence must hash as it did before the feature"
    assert configured.to_dict()["cache"] == {"dir": str(tmp_path)}
    assert plain.hash() != configured.hash()


# -- the banner ---------------------------------------------------------------


def test_the_banner_reports_the_infrastructure(tmp_path):
    text = banner_for(DIAMOND, cache=DiskCache(tmp_path / "c", capacity_bytes=3 * 2**30), workers=6)
    assert "bd" in text
    assert str(tmp_path / "c") in text and "3.0 GiB cap" in text
    assert "6 threads" in text
    assert "p.yaml" in text


def test_the_banner_says_where_the_cache_setting_came_from(tmp_path):
    cache = DiskCache(tmp_path / "c")
    assert "(default)" in banner_for(DIAMOND, cache)
    assert "(yaml)" in banner_for(DIAMOND | {"cache": {"size_gb": 8}}, cache)


def test_the_banner_draws_the_dag_top_to_bottom():
    banner = banner_for(DIAMOND)
    graph = [" ".join(ln.split()) for ln in graph_lines(banner)]

    # One node per line, `name[KernelClass] ← inputs`, in dependency order.
    assert graph[0] == "load[SyntheticLoader]"  # the source has no inputs to name
    assert [ln.split("[")[0] for ln in graph] == ["load", "a", "b", "sink"]
    assert "a[TouchKernel] ← load" in graph
    # The rails draw the join; naming it too is what makes a wide gutter still readable.
    assert "sink[RecordingKernel] ← a, b" in graph


def test_the_dag_draws_its_edges():
    """The rails are the picture: a fork out of `load`, a join back into `sink`."""
    assert rail_lines(banner_for(DIAMOND)) == ["●", "├─╮", "│ ●", "│ │", "● │", "├─╯", "●"]


def test_a_lane_is_reused_once_its_edge_is_spent():
    """Width follows edges in flight, not node count — a chain stays in one rail."""
    chain = DIAMOND | {"nodes": [
        DIAMOND["nodes"][0],
        {"name": "a", "kernel": TOUCH, "inputs": ["load"]},
        {"name": "b", "kernel": TOUCH, "inputs": ["a"]},
    ]}
    assert rail_lines(banner_for(chain)) == ["●", "│", "●", "│", "●"]


def test_the_dag_sits_in_a_square_frame():
    box = [ln for ln in banner_for(DIAMOND).splitlines() if ln.strip().startswith(("┌", "│", "└"))]
    assert box[0].strip().startswith("┌─ dag ") and box[0].strip().endswith("┐")
    assert box[-1].strip().startswith("└") and box[-1].strip().endswith("┘")
    assert len({len(ln) for ln in box}) == 1, "every frame line is the same width"


def test_any_shape_of_dag_prints():
    """A node reading across the graph is fine: the lane just stays open longer."""
    spec = DIAMOND | {"nodes": DIAMOND["nodes"][:3] + [{**DIAMOND["nodes"][3], "inputs": ["load", "b"]}]}
    banner = banner_for(spec)
    assert " ".join(graph_lines(banner)[-1].split()) == "sink[RecordingKernel] ← load, b"
    assert rail_lines(banner) == ["●", "├─╮", "│ ●", "├─╮", "│ ●", "├─╯", "●"]


# -- colour -------------------------------------------------------------------


def test_colour_is_off_unless_someone_is_watching(monkeypatch):
    """Escape codes in a log a machine will grep are worse than no colour at all."""
    import io

    monkeypatch.delenv("NO_COLOR", raising=False)
    assert not color_enabled(io.StringIO())  # a pipe, a file, a CI capture

    class Terminal(io.StringIO):
        def isatty(self):
            return True

    assert color_enabled(Terminal())
    monkeypatch.setenv("NO_COLOR", "1")
    assert not color_enabled(Terminal()), "https://no-color.org"


def test_the_banner_colours_only_when_asked():
    assert "\033[" not in banner_for(DIAMOND, color=False)
    assert "\033[1m" in banner_for(DIAMOND, color=True)


def test_the_kernel_class_is_the_coloured_part():
    """The class is what you scan a DAG for; the node name is the anchor holding it."""
    text = banner_for(DIAMOND, color=True)
    assert "\033[36mSyntheticLoader\033[0m" in text, "the class is tinted"
    assert "\033[1mload\033[0m" in text, "the node name carries the weight, not the colour"
    assert "\033[2m[\033[0m" in text, "the brackets are structural, so they stay dim"


# -- the closing results ------------------------------------------------------

DOC = {
    "job": {"n_samples": 3, "n_batches": 2, "elapsed_s": 0.04,
            "counts": {"pass": 2, "warn": 0, "fail": 1, "error": 0}},
    "nodes": [
        {"name": "load", "kernel": "abasift.loaders.FlatDirLoader"},
        {"name": "duration", "kernel": "abasift.kernels.VideoDurationKernel",
         "counts": {"fail": 1, "pass": 2},
         "checks": {"video_length": {"counts": {"fail": 1, "pass": 2},
                                     "threshold": {"min_s": 1.0}}},
         "summary": {"n_videos": 3}},
    ],
}


def test_the_results_say_which_node_produced_the_failures():
    """The job line says *how many* failed; this is the part that says where."""
    lines = run_results(DOC, color=False).splitlines()
    assert lines[0] == "duration[VideoDurationKernel]"
    assert lines[1] == "  video_length  pass=2 fail=1"
    assert lines[-1] == "3 samples in 2 batches, 0.04s — pass=2 warn=0 fail=1 error=0"
    assert '"n_videos": 3' in "\n".join(lines), "the kernel's own reduce, indented under it"


def test_the_verdicts_are_tallied_once_and_the_thresholds_not_at_all():
    """A single-check node would otherwise say its tally twice; `min_s` is configuration,
    and configuration is what the YAML and the pipeline document are for."""
    text = run_results(DOC, color=False)
    assert text.count("pass=2 fail=1") == 1
    assert "min_s" not in text


def test_a_node_that_judges_nothing_says_nothing():
    """A loader has no verdicts and no reduce, so it earns no line at all."""
    assert not any(ln.startswith("load[") for ln in run_results(DOC, color=False).splitlines())


def test_a_status_that_never_happened_is_left_out_per_node():
    """Per node the zeros are noise; on the job line they are a fact worth stating."""
    text = run_results(DOC, color=False)
    assert "warn=" not in text.splitlines()[1], "a check names only what occurred"
    assert "warn=0 fail=1 error=0" in text.splitlines()[-1]


def test_only_a_count_that_happened_is_coloured():
    """`error=0` in alarm red is a lie told in colour, so a zero stays dim."""
    job = run_results(DOC, color=True).splitlines()[-1]
    assert "\033[31mfail=1\033[0m" in job, "a real failure is red"
    assert "\033[2merror=0\033[0m" in job, "a zero is dim whatever its status"
    assert "\033[32mpass=2\033[0m" in job


def test_the_results_stay_plain_when_nobody_is_watching():
    assert "\033[" not in run_results(DOC, color=False)


def test_log_records_keep_their_alignment_and_their_message():
    record = logging.LogRecord("abasift.executor", logging.INFO, __file__, 1, "batch 3 done", None, None)
    fmt = "%(levelname)s %(name)s: %(message)s"
    plain = ColorFormatter(fmt, enabled=False).format(record)
    coloured = ColorFormatter(fmt, enabled=True).format(record)

    assert plain == "INFO    abasift.executor: batch 3 done"  # level padded to 7
    # Padding happens *before* painting: strip the colour and the line is byte-identical,
    # which would not hold if `%(levelname)-7s` had counted the escape codes.
    assert re.sub(r"\033\[[0-9;]*m", "", coloured) == plain
    assert "\033[36m" in coloured  # INFO is cyan
    assert record.levelname == "INFO", "formatting must not mutate the record"


def test_the_banner_prints_before_the_job_runs(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "p.yaml").write_text(
        "pipeline:\n  job_id: cli_banner\n  nodes:\n"
        f"    - {{name: load, kernel: {SYNTH}, inputs: []}}\n"
    )
    assert main(["run", "p.yaml"]) == 0
    out = capsys.readouterr().out
    assert out.index("abasift · cli_banner") < out.index("samples in")
