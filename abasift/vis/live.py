"""The runtime half of ``vis``: watching a job while it runs (``abasift run --vis``).

``abasift vis`` shows what a pipeline *is*. This shows what a run of it is *doing*: which
node is executing right now, how many samples each has judged, and the verdicts landing on
the boxes batch by batch.

The executor knows nothing about any of this. It calls ``observer(event, **payload)`` at
five points and never looks at the result (see ``Executor._emit``), so the watcher is
strictly additive — remove it and the job runs identically.

Counting is **incremental**: each ``batch_merged`` carries only that batch's report
fragment, and per-``node/check`` keys are disjoint across batches, so folding is a
counter bump rather than a rescan of every sample seen so far.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from typing import Any

from ..report import Report


class LiveJob:
    """Thread-safe progress of one running job. Written by the executor's driver thread,
    read by the web server's threads — hence the lock around every touch of the state."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started = time.time()
        self._job: dict[str, Any] = {}
        self._summary: dict[str, Any] = {}
        #: node -> check -> status -> count
        self._checks: dict[str, dict[str, Counter]] = {}
        self._samples: dict[str, set[str]] = {}  # node -> sample ids it has judged
        self._running: set[str] = set()
        self._batches = 0
        self._batch_samples = 0
        self._done = False
        self._version = 0  # bumped on every change; the page polls this

    # -- the observer protocol the executor calls ------------------------

    def __call__(self, event: str, **payload) -> None:
        handler = getattr(self, f"_on_{event}", None)
        if handler:
            handler(**payload)

    def _on_job_started(self, job: dict) -> None:
        with self._lock:
            self._job = dict(job)
            self._touch()

    def _on_batch_started(self, index: int, n_samples: int) -> None:
        with self._lock:
            self._batches = index
            self._batch_samples = n_samples
            self._touch()

    def _on_node_started(self, node: str, batch: int) -> None:
        with self._lock:
            self._running.add(node)
            self._touch()

    def _on_node_finished(self, node: str, batch: int) -> None:
        with self._lock:
            self._running.discard(node)
            self._touch()

    def _on_batch_merged(self, index: int, report: Report) -> None:
        """Fold one batch's verdicts in. Keys are disjoint across batches, so this is a
        pure accumulation — the same reason the executor can merge batches with a dict
        union and no logic."""
        with self._lock:
            for sample_id, entry in report.samples.items():
                for key, check in entry["checks"].items():
                    node, _, name = key.partition("/")
                    self._checks.setdefault(node, {}).setdefault(name, Counter())[check.status] += 1
                    self._samples.setdefault(node, set()).add(sample_id)
            self._touch()

    def _on_job_finished(self, job: dict, summary: dict) -> None:
        with self._lock:
            self._job = dict(job)
            self._summary = {k: dict(v) for k, v in summary.items()}
            self._running.clear()
            self._done = True
            self._touch()

    def _touch(self) -> None:
        self._version += 1  # caller holds the lock

    # -- what the page reads --------------------------------------------

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def snapshot(self) -> dict:
        """A consistent picture of the job right now."""
        with self._lock:
            return {
                "job": dict(self._job),
                "done": self._done,
                "batches": self._batches,
                "batch_samples": self._batch_samples,
                "elapsed_s": round(time.time() - self._started, 1),
                "running": sorted(self._running),
                "nodes": {
                    node: {
                        "checks": {name: dict(c) for name, c in sorted(checks.items())},
                        "n_samples": len(self._samples.get(node, ())),
                        "summary": self._summary.get(node),
                    }
                    for node, checks in self._checks.items()
                },
            }


def overlay(model: dict, snapshot: dict) -> dict:
    """Paint a job snapshot onto a described pipeline. The renderer needs no new code.

    ``model`` is the static description; this adds the two optional keys the renderer
    already knows how to draw — ``model["job"]`` and each node's ``run`` — so the live
    view and the static view are the same page with more filled in.
    """
    job = dict(snapshot["job"])
    job.setdefault("n_batches", snapshot["batches"])
    job.setdefault("elapsed_s", snapshot["elapsed_s"])
    model["job"] = job
    model["running"] = snapshot["running"]
    model["done"] = snapshot["done"]
    for node in model["nodes"]:
        node["run"] = snapshot["nodes"].get(node["name"])
        node["running"] = node["name"] in snapshot["running"]
    return model
