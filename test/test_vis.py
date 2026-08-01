"""The runtime pipeline view: what it reads off the classes, and what it draws.

The point of these tests is that the picture is *derived*. Every assertion here would
break if the visualiser started transcribing instead of introspecting — which is exactly
the failure mode of the hand-drawn diagrams it complements.
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from abasift import Executor, Pipeline
from abasift.vis import LiveJob, PipelineView, RunView, describe, make_server, render_pipeline

SYNTH = "kernels_for_test.SyntheticLoader"
TOUCH = "kernels_for_test.TouchKernel"
ROOT = Path(__file__).resolve().parents[1]

#: load -> {dur, touch} -> dump. A diamond, so layering and joins are both exercised.
DIAMOND = {
    "job_id": "vis_demo",
    "nodes": [
        {"name": "load", "kernel": SYNTH, "params": {"n": 4, "batch_size": 2}, "inputs": []},
        {
            "name": "dur",
            "kernel": "abasift.kernels.VideoDurationKernel",
            "params": {"min_s": 1.0},
            "inputs": ["load"],
        },
        {"name": "touch", "kernel": TOUCH, "inputs": ["load"]},
        {
            "name": "dump",
            "kernel": "abasift.kernels.DataArchiver",
            "params": {"keys": ["__report__"]},
            "inputs": ["dur", "touch"],
        },
    ],
}


#: Just the loader and a kernel that passes: for asserting on verdicts without the
#: duration kernel erroring on synthetic samples that carry no video.
TOUCH_ONLY = DIAMOND | {"nodes": [DIAMOND["nodes"][0], DIAMOND["nodes"][2]]}


@pytest.fixture(autouse=True)
def scratch_cwd(tmp_path, monkeypatch):
    """Run every test in this file from a throwaway directory.

    ``DIAMOND``'s dumper sets no ``target:``, so it writes to the *relative* default
    ``dump/<unix ts>/<job_id>_<hash>/<node>/`` — which is the repo root unless we move. A suite
    must not leave artifacts in the working tree, gitignored or not.
    """
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def model():
    return describe(Pipeline.from_dict(DIAMOND))


def node(model, name):
    return next(n for n in model["nodes"] if n["name"] == name)


def method(node, name):
    return next(m for m in node["methods"] if m["name"] == name)


def test_roles_and_columns_come_from_the_dag(model):
    assert {n["name"]: n["role"] for n in model["nodes"]} == {
        "load": "source",
        "dur": "sample",
        "touch": "sample",
        "dump": "mutating",
    }
    # Longest path from the source: the join sits one column right of both branches.
    assert {n["name"]: n["layer"] for n in model["nodes"]} == {"load": 0, "dur": 1, "touch": 1, "dump": 2}
    assert model["n_layers"] == 3
    assert len(model["edges"]) == 4


def test_the_source_kernel_shows_its_own_interface(model):
    """Node 0 is not a Kernel: it must be described by ``iter_batches``, not ``run``."""
    load = node(model, "load")
    assert [m["name"] for m in load["methods"]] == ["iter_batches", "digest"]
    assert method(load, "iter_batches")["own"] is True
    assert method(load, "digest")["owner"] == "SourceKernel"
    assert load["is_source"] and load["interface"] == "SourceKernel"


def test_a_check_kernel_shows_what_it_wrote_and_what_it_inherited(model):
    dur = node(model, "dur")
    sift, run = method(dur, "sift"), method(dur, "run")
    assert sift["own"] and sift["owner"] == "VideoDurationKernel"
    assert sift["signature"].startswith("(sample: Sample, art: ArtifactUnion)")
    # The batch loop, the alive-filter and the per-sample failsafe are the base's.
    assert not run["own"] and run["owner"] == "SampleKernel"
    assert run["signature"] == (
        "(art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]"
    )


def test_a_mutating_kernel_is_described_by_its_mutating_interface(model):
    dump = node(model, "dump")
    assert [m["name"] for m in dump["methods"]] == ["run_mutating", "commit"]
    assert all(m["own"] for m in dump["methods"])


def test_params_separate_what_the_yaml_set_from_the_kernel_default(model):
    params = {p["name"]: p for p in node(model, "dur")["params"]}
    assert params["min_s"]["given"] and params["min_s"]["value"] == "1.0"
    # Untouched knobs still show, with the default the kernel would fall back to.
    assert not params["max_s"]["given"] and params["max_s"]["default"] == "None"
    assert params["stream"]["default"] == "'video/main'"
    assert params["stream"]["annotation"] == "str"
    # Kernels declare every parameter explicitly, so this table is exhaustive.
    assert set(params) == {"min_s", "max_s", "stream", "check_name"}


def test_a_renamed_parameter_would_change_the_picture():
    """Guards the whole premise: the table is read from the class, not from the YAML."""
    from abasift.kernels import VideoDurationKernel

    declared = {p["name"] for p in node(describe(Pipeline.from_dict(DIAMOND)), "dur")["params"]}
    assert declared == set(VideoDurationKernel.__init__.__annotations__) - {"return"}


@pytest.mark.parametrize("path", sorted((ROOT / "pipelines").glob("*.yaml")), ids=lambda p: p.name)
def test_every_shipped_pipeline_renders(path):
    html = render_pipeline(Pipeline.from_yaml(path), yaml_path=str(path))
    assert "<article class='node" in html


def test_the_page_is_self_contained_and_names_every_node():
    html = render_pipeline(Pipeline.from_dict(DIAMOND))
    assert html.startswith("<!doctype html>")
    # No network: a page pulled off a worker must render with nothing else beside it.
    # (The one URL in the file is the SVG namespace, which is an identifier, not a fetch.)
    assert not re.search(r'(?:src|href)="(?!#)', html)
    assert not re.findall(r"https?://(?!www\.w3\.org/2000/svg)\S+", html)
    for name in ("load", "dur", "touch", "dump"):
        assert f'data-node="{name}"' in html
    assert json.loads(re.search(r'<script id="edges"[^>]*>(.*?)</script>', html, re.S).group(1))


def test_the_card_is_light_and_the_detail_waits_behind_a_click():
    """Scanning the DAG should be cheap for the eye; the rest is one click away."""
    html = render_pipeline(Pipeline.from_dict(DIAMOND))
    article = re.search(r"<article[^>]*data-node=\"dur\"[^>]*>(.*?)</article>", html, re.S).group(1)
    card, _, detail = article.partition("<template class='detail'>")

    assert "VideoDurationKernel" in card and "4 params" in card  # enough to read the graph
    for heavy in ("min_s", "video_length", "abasift.kernels.", "sift("):
        assert heavy not in card, f"{heavy!r} belongs in the sheet, not on the card"

    assert "min_s" in detail and "video/main" in detail  # params
    assert "sift(sample: Sample" in detail  # signatures
    assert "abasift.kernels.VideoDurationKernel" in detail  # where it comes from
    # A <template> is inert: none of it is in the document until the node is clicked.
    assert '<dialog id="sheet">' in html


def test_describing_a_pipeline_says_nothing_about_any_run(model):
    """`abasift vis` is structure only: verdicts belong to `run --vis`, not to this."""
    assert "job" not in model
    assert all("run" not in n for n in model["nodes"])
    assert "this run" not in render_pipeline(Pipeline.from_dict(DIAMOND))


# -- hosting: the page is a window on the files, not a copy of them -----------


@pytest.fixture
def hosted(tmp_path):
    """A running server over a YAML we can edit, plus a `get(route)` helper."""
    yaml_path = tmp_path / "p.yaml"
    yaml_path.write_text(yaml.safe_dump({"pipeline": DIAMOND}))
    server = make_server(PipelineView(yaml_path), port=0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address[:2]

    def get(route):
        with urllib.request.urlopen(f"http://{host}:{port}{route}") as r:
            return r.read().decode()

    try:
        yield SimpleNamespace(get=get, yaml=yaml_path, dir=tmp_path)
    finally:
        server.shutdown()
        server.server_close()


def rewrite(path: Path, text: str) -> None:
    """Write and push the mtime forward, so the change is visible however coarse the clock."""
    path.write_text(text)
    stamp = time.time() + 2
    os.utime(path, (stamp, stamp))


def test_the_server_hosts_the_page_the_body_and_a_state_digest(hosted):
    page = hosted.get("/")
    assert page.startswith("<!doctype html>") and 'data-node="dump"' in page
    assert "ABASIFT_LIVE" in page and "pill-live" in page, "a hosted page must poll"
    # /body is the same content without the document around it: what a swap replaces.
    assert 'data-node="dump"' in hosted.get("/body")
    assert "<!doctype" not in hosted.get("/body")
    assert hosted.get("/state") == hosted.get("/state") != ""
    with pytest.raises(urllib.error.HTTPError, match="404"):
        hosted.get("/elsewhere")


def test_editing_the_yaml_moves_the_state_and_the_page_follows(hosted):
    before = hosted.get("/state")
    assert "1.0" in hosted.get("/body")

    edited = json.loads(json.dumps(DIAMOND))  # deep copy
    edited["nodes"][1]["params"]["min_s"] = 42.0
    rewrite(hosted.yaml, yaml.safe_dump({"pipeline": edited}))

    assert hosted.get("/state") != before, "the digest is what tells the page to re-fetch"
    assert "42.0" in hosted.get("/body")


def test_a_broken_yaml_is_shown_not_fatal(hosted):
    rewrite(hosted.yaml, "pipeline: {job_id: broken, nodes: []}")
    shown = hosted.get("/body")
    assert "PipelineError" in shown and "nodes is empty" in shown

    rewrite(hosted.yaml, yaml.safe_dump({"pipeline": DIAMOND}))
    assert 'data-node="dump"' in hosted.get("/body"), "it must recover by itself"


#: A kernel whose one parameter's default we can move around, to watch the page follow.
PROBE_SOURCE = """
from abasift import Check, SampleKernel

class Probe(SampleKernel):
    def __init__(self, limit: float = %s):
        self.limit = limit

    def sift(self, sample, art):
        return {"probe": Check("pass")}
"""


def probe_pipeline(tmp_path, dotted: str) -> Path:
    yaml_path = tmp_path / "p.yaml"
    yaml_path.write_text(
        yaml.safe_dump(
            {
                "pipeline": {
                    "job_id": "hot",
                    "nodes": [
                        {"name": "load", "kernel": SYNTH, "inputs": []},
                        {"name": "probe", "kernel": dotted, "inputs": ["load"]},
                    ],
                }
            }
        )
    )
    return yaml_path


def test_editing_a_kernel_reloads_it(tmp_path, monkeypatch):
    """The signatures are the payload, so they have to follow the source file too."""
    source = tmp_path / "live_kernel.py"
    source.write_text(PROBE_SOURCE % "1.0")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "live_kernel", raising=False)

    view = PipelineView(probe_pipeline(tmp_path, "live_kernel.Probe"))
    limit = lambda: {p["name"]: p["default"] for p in node(view.model(), "probe")["params"]}
    assert limit() == {"limit": "1.0"}

    rewrite(source, PROBE_SOURCE % "99.0")
    assert limit() == {"limit": "99.0"}, "an edited kernel must be re-imported"
    assert "99.0" in view.html()
    sys.modules.pop("live_kernel", None)


def test_reloading_reaches_through_a_re_exporting_package(tmp_path, monkeypatch):
    """The real shape: YAML says `abasift.kernels.X`, the class lives in a submodule.

    Reloading only the submodule is not enough — the package's `from .x import X` still
    holds the old class object, which is what the dotted path resolves to.
    """
    pkg = tmp_path / "livepkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("from .probe import Probe\n")
    (pkg / "probe.py").write_text(PROBE_SOURCE % "1.0")
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in ("livepkg", "livepkg.probe"):
        monkeypatch.delitem(sys.modules, name, raising=False)

    view = PipelineView(probe_pipeline(tmp_path, "livepkg.Probe"))
    limit = lambda: {p["name"]: p["default"] for p in node(view.model(), "probe")["params"]}
    assert limit() == {"limit": "1.0"}

    rewrite(pkg / "probe.py", PROBE_SOURCE % "7.5")
    assert limit() == {"limit": "7.5"}
    for name in ("livepkg", "livepkg.probe"):
        sys.modules.pop(name, None)


# -- watching a run: `abasift run --vis` --------------------------------------


def test_the_executor_reports_its_progress_to_an_observer():
    """The executor's whole contribution to the live view: five events, no vis import."""
    seen = []
    Executor(Pipeline.from_dict(DIAMOND), observer=lambda e, **kw: seen.append((e, kw))).run()
    events = [e for e, _ in seen]
    assert events[0] == "job_started" and events[-1] == "job_finished"
    assert events.count("batch_started") == events.count("batch_merged") == 2  # n=4, batch_size=2
    nodes_run = {kw["node"] for e, kw in seen if e == "node_started"}
    assert nodes_run == {"dur", "touch", "dump"}  # every downstream node, per batch
    assert events.count("node_started") == events.count("node_finished")


def test_an_observer_that_raises_cannot_take_the_job_down():
    """Telemetry is never load-bearing: the job must complete and report regardless."""

    def hostile(event, **kw):
        raise RuntimeError("watcher exploded")

    report = Executor(Pipeline.from_dict(TOUCH_ONLY), observer=hostile).run()
    assert report.counts()["pass"] == 4


def test_a_live_job_accumulates_verdicts_batch_by_batch():
    live = LiveJob()

    assert live.snapshot()["nodes"] == {} and not live.snapshot()["done"]
    Executor(Pipeline.from_dict(TOUCH_ONLY), observer=live).run()

    snap = live.snapshot()
    assert snap["done"] and snap["running"] == []
    assert snap["batches"] == 2 and snap["job"]["counts"] == {"pass": 4, "warn": 0, "fail": 0, "error": 0}
    assert snap["nodes"]["touch"] == {"checks": {"touched": {"pass": 4}}, "n_samples": 4, "summary": None}


def test_the_run_view_shows_the_graph_filling_in(tmp_path):
    """RunView = the same static description plus whatever the job has done so far."""
    yaml_path = tmp_path / "p.yaml"
    yaml_path.write_text(yaml.safe_dump({"pipeline": DIAMOND}))
    live = LiveJob()
    view = RunView(Pipeline.from_yaml(yaml_path), live, yaml_path)

    before = view.state()
    assert "this run" not in view.html(), "nothing has happened yet"

    Executor(Pipeline.from_dict(DIAMOND), observer=live).run()

    assert view.state() != before, "progress must move the token the page polls"
    after = view.html()
    assert "4 pass" in after and "pill-done" in after
    assert 'data-node="dur"' in after  # still the same graph, just filled in


def test_the_node_being_executed_is_marked_on_the_page():
    """Driven by hand rather than by racing a real job: the events are the contract."""
    live = LiveJob()
    view = RunView(Pipeline.from_dict(DIAMOND), live)
    live("job_started", job={"job_id": "watch", "counts": {}})
    live("batch_started", index=1, n_samples=2)
    live("node_started", node="dur", batch=1)

    shown = view.html(full=False)  # the fragment, so the stylesheet can't match
    assert "pill-run" in shown
    card = lambda html, name: re.search(rf"<article[^>]*data-node=\"{name}\"[^>]*>", html).group(0)
    assert "is-running" in card(shown, "dur")
    assert "is-running" not in card(shown, "touch")

    live("node_finished", node="dur", batch=1)
    assert "is-running" not in view.html(full=False)


def test_the_run_view_does_not_re_read_the_yaml_mid_run(tmp_path):
    """The job is executing the classes it loaded; showing anything else would be a lie."""
    yaml_path = tmp_path / "p.yaml"
    yaml_path.write_text(yaml.safe_dump({"pipeline": DIAMOND}))
    view = RunView(Pipeline.from_yaml(yaml_path), LiveJob(), yaml_path)

    rewrite(yaml_path, yaml.safe_dump({"pipeline": {"job_id": "swapped", "nodes": []}}))
    assert 'data-node="dump"' in view.html()
