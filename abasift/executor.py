"""Single-machine executor: per-batch streaming through the DAG + terminal finalize.

    for each batch yielded by the source:
        run the downstream DAG on it
          - independent branches run concurrently on threads
          - a node sees exactly the union (and the report) of its ancestors
          - branches over one sample share one decoded copy (LazyRaw memo)
        merge the batch's per-sample fragments into the job union/report
        release the batch's decoded payloads
    then: finalize() per kernel in topo order  -> summaries
          finalize_mutating() for DataDumpers  -> dump the finished report

Two properties are worth naming because they are easy to lose:

*Determinism.* A node's inputs are the union of its **ancestors'** unions and reports —
never the global "whatever finished first" state. So which samples a node considers
alive does not depend on thread scheduling.

*The job always completes.* A kernel that raises on one sample marks that sample
``error``; a kernel that raises outright marks every live sample in the batch ``error``
at that node; a source that blows up mid-enumeration is recorded in the summary. Nothing
propagates out of ``run()``.
"""

from __future__ import annotations

import logging
import os
import platform
import socket
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from .data import ArtifactUnion
from .errors import ExecutorError
from .kernel import MutatingKernel, SourceKernel
from .pipeline import Pipeline
from .report import Check, Report, ReportExt, ReportView

log = logging.getLogger("abasift.executor")


class Executor:
    def __init__(
        self,
        pipeline: Pipeline,
        max_workers: int | None = None,
        observer: "Callable[..., None] | None" = None,
    ):
        self.pipeline = pipeline
        self.max_workers = max_workers or min(8, max(1, len(pipeline.nodes)))
        self.artifacts = ArtifactUnion()
        #: Optional ``observer(event, **payload)`` — progress telemetry for whoever is
        #: watching (``abasift run --vis``). The executor knows nothing about the watcher,
        #: and an observer that raises can never take the job down: see :meth:`_emit`.
        self.observer = observer

    # -- public ----------------------------------------------------------

    def run(self) -> Report:
        p = self.pipeline
        started = time.time()
        report = Report(
            {
                "pipeline": p.name,
                "pipeline_hash": p.hash(),
                "job_id": p.job_id,
                "worker": f"{socket.gethostname()}:{os.getpid()}",
                "started_at": _utc(started),
                # One clock read per job, so every dumper in the DAG agrees on the stamp;
                # recorded here so the tree a default dump went to is reproducible.
                "started_unix": int(started),
                "python": platform.python_version(),
            }
        )
        kernels = p.instantiate()
        for name, k in kernels.items():
            k._bind(name, report.job)
        source = kernels[p.source]
        assert isinstance(source, SourceKernel)

        order = p.topo_order()
        downstream = [n for n in order if n != p.source]
        inputs = {n.name: tuple(n.inputs) for n in p.nodes}
        self._emit("job_started", job=report.job)

        n_batches = 0
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="abasift") as pool:
            for batch_ext, batch_rext in self._iter_source(source, report):
                n_batches += 1
                src_art = ArtifactUnion().extended(p.source, batch_ext)
                batch = src_art.find_batch()
                log.info(
                    "batch %d: %d samples (%s)",
                    n_batches,
                    len(batch) if batch else 0,
                    ", ".join(batch.ids[:3]) + ("..." if batch and len(batch) > 3 else "") if batch else "-",
                )
                src_rep = Report()
                src_rep.apply(p.source, batch_rext)

                self._emit("batch_started", index=n_batches, n_samples=len(batch) if batch else 0)
                arts, reps = self._run_dag(
                    kernels, downstream, inputs, src_art, src_rep, pool, n_batches
                )

                batch_art = ArtifactUnion()
                for a in arts.values():
                    batch_art = batch_art.union(a)
                batch_report = Report()
                for r in reps.values():
                    batch_report.merge(r)

                self.artifacts = self.artifacts.union(batch_art.without_transients())
                report.merge(batch_report)
                # The batch's own fragment, not the job report: a watcher folds it in
                # incrementally instead of re-counting every sample seen so far.
                self._emit("batch_merged", index=n_batches, report=batch_report)
                if batch is not None:
                    batch.release()  # tier-2 cache: decoded payloads die with the batch

        # dataset-level reduces, then the dump pass (so dumped reports are complete)
        view = ReportView(report)
        for name in order:
            k = kernels[name]
            out = k.finalize(self.artifacts, view)
            if out is not None:
                ext, rext = out
                self.artifacts = self.artifacts.extended(name, ext)
                report.apply(name, rext)

        report.job["n_batches"] = n_batches
        report.job["n_samples"] = len(report.samples)
        report.job["counts"] = report.counts()
        report.job["finished_at"] = _utc(time.time())
        report.job["elapsed_s"] = round(time.time() - started, 3)

        for name in order:
            k = kernels[name]
            if isinstance(k, MutatingKernel):
                m = k.finalize_mutating(self.artifacts, ReportView(report))
                if m is not None:
                    self.artifacts = self.artifacts.with_mutations(m.replace, m.delete).extended(
                        name, m.ext
                    )
                    report.apply(name, m.report_ext)

        log.info("job done: %s", report.job["counts"])
        self._emit("job_finished", job=report.job, summary=report.summary)
        return report

    # -- internals -------------------------------------------------------

    def _emit(self, event: str, **payload) -> None:
        """Tell the observer, if there is one. Telemetry must never fail a job."""
        if self.observer is None:
            return
        try:
            self.observer(event, **payload)
        except Exception:
            log.debug("observer failed on %s", event, exc_info=True)

    def _iter_source(
        self, source: SourceKernel, report: Report
    ) -> Iterator[tuple[dict[str, Any], ReportExt]]:
        """Source iteration with a failsafe: enumeration blowing up must not kill the job."""
        it = source.iter_batches()
        while True:
            try:
                yield next(it)
            except StopIteration:
                return
            except Exception as e:
                log.exception("source %r failed during enumeration", source.node_name)
                report.summary.setdefault(source.node_name, {})["source_error"] = (
                    f"{type(e).__name__}: {e}"
                )
                return

    def _run_dag(self, kernels, downstream, inputs, src_art, src_rep, pool, batch_index=0):
        """Run every downstream node once, respecting edges, branches in parallel."""
        arts: dict[str, ArtifactUnion] = {self.pipeline.source: src_art}
        reps: dict[str, Report] = {self.pipeline.source: src_rep}
        pending = list(downstream)
        inflight: dict[Future, str] = {}

        while pending or inflight:
            ready = [n for n in pending if all(d in arts for d in inputs[n])]
            for name in ready:
                pending.remove(name)
                art_in = _union_of(arts[d] for d in inputs[name])
                rep_in = _report_of(reps[d] for d in inputs[name])
                self._emit("node_started", node=name, batch=batch_index)
                inflight[pool.submit(self._run_node, name, kernels[name], art_in, rep_in)] = name
            if not inflight:
                raise ExecutorError(f"nodes {pending} are unreachable (validation should have caught this)")
            done, _ = wait(list(inflight), return_when=FIRST_COMPLETED)
            for f in done:
                name = inflight.pop(f)
                arts[name], reps[name] = f.result()
                self._emit("node_finished", node=name, batch=batch_index)
        return arts, reps

    def _run_node(self, name, kernel, art_in: ArtifactUnion, rep_in: Report):
        view = ReportView(rep_in)
        t0 = time.time()
        try:
            if isinstance(kernel, MutatingKernel):
                m = kernel.run_mutating(art_in, view)
                art_out = art_in.with_mutations(m.replace, m.delete).extended(name, m.ext)
                rext = m.report_ext
            else:
                ext, rext = kernel.run(art_in, view)
                art_out = art_in.extended(name, ext)
        except Exception as e:
            # Whole-node failure: every sample still alive is an error *at this node*.
            log.exception("node %r failed on this batch", name)
            art_out = art_in
            rext = ReportExt()
            detail = {"exception": f"{type(e).__name__}: {e}"}
            for sid in _live_ids(art_in, view):
                rext.add(sid, "error", Check("error", details=detail))
        rep_out = Report()
        rep_out.merge(rep_in)
        rep_out.apply(name, rext)
        log.debug("node %r: %.2fs", name, time.time() - t0)
        return art_out, rep_out


def _union_of(unions) -> ArtifactUnion:
    out = ArtifactUnion()
    for u in unions:
        out = out.union(u)
    return out


def _report_of(reports) -> Report:
    out = Report()
    for r in reports:
        out.merge(r)
    return out


def _live_ids(art: ArtifactUnion, view: ReportView) -> list[str]:
    batch = art.find_batch()
    return [s.sample_id for s in batch if view.is_alive(s.sample_id)] if batch else []


def _utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")


def run_pipeline(path: str, max_workers: int | None = None) -> tuple[Report, ArtifactUnion]:
    """Convenience: YAML path -> (report, artifacts)."""
    ex = Executor(Pipeline.from_yaml(path), max_workers=max_workers)
    return ex.run(), ex.artifacts
