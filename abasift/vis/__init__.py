"""Hosted views of a pipeline. Two commands, two questions, nothing written to disk.

**What is this pipeline?** — ``abasift vis <yaml>`` hosts the DAG the YAML builds. It is

* **per-pipeline** — *your* YAML, with the parameters that YAML sets (unlike the
  hand-drawn framework diagrams in ``doc/uml/``); and
* **live against the code** — the server re-reads the YAML, re-imports any kernel whose
  source changed, and re-describes on every request, so an open page follows your edits.
  Node 0's ``iter_batches``, a check kernel's ``sift``, which methods are the kernel's own
  and which come from the base, the default a parameter falls back to: all read off the
  classes with ``inspect``, never transcribed.

**What is this job doing?** — ``abasift run <yaml> --vis`` hosts the same graph while the
job runs, with the executing node lit up and each box's verdicts filling in batch by
batch. The executor pushes progress to a :class:`~abasift.vis.live.LiveJob`; it knows
nothing about the page.

Describing a pipeline imports the kernels it names — that is where the signatures come
from — but never instantiates them and never touches the data.
"""

from __future__ import annotations

from ..pipeline import Pipeline
from .live import LiveJob, overlay
from .model import describe
from .render import body, render
from .server import PipelineView, RunView, make_server, serve, url_of

__all__ = [
    "LiveJob",
    "PipelineView",
    "RunView",
    "body",
    "describe",
    "make_server",
    "overlay",
    "render",
    "render_pipeline",
    "serve",
    "url_of",
]


def render_pipeline(pipeline: Pipeline, yaml_path: str | None = None) -> str:
    """Describe a pipeline and draw it, in one call. The server does this per request."""
    return render(describe(pipeline, yaml_path))
