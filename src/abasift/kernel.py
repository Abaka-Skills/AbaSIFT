"""Kernel interfaces. Inputs are read-only; kernels return only extensions.

Four classes, three of which a QC author might subclass:

* ``SourceKernel`` — node 0. Enumerates a vendor layout into batches of samples.
* ``Kernel`` — anything else. One ``run`` per batch, optional ``finalize`` at the end.
* ``SampleKernel`` — the base most check kernels want: it loops the batch, skips samples
  that already failed upstream, and turns a per-sample exception into ``status: error``
  so one bad sample never takes down its batchmates.
* ``MutatingKernel`` — framework-internal. Only ``DataDumper`` may replace or delete
  existing union keys.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping

from .data import ArtifactUnion, Sample
from .report import Check, ReportExt, ReportView

#: What a kernel adds to the union: ``name -> value``, namespaced by the executor.
ArtifactExt = dict[str, Any]


class _Bound:
    """Framework-injected identity. The executor binds this before the first call.

    Note what is *not* here: a ``**params`` catch-all. Kernels declare their parameters
    explicitly so that a mistyped threshold in YAML is a load-time ``PipelineError``
    instead of a silently-defaulted check.
    """

    node_name: str = "?"
    job: Mapping[str, Any] = {}
    params: Mapping[str, Any] = {}

    def _bind(self, node_name: str, job: Mapping[str, Any]) -> None:
        self.node_name = node_name
        self.job = job


class Kernel(_Bound):
    def run(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]:
        """Called once per batch. ``art`` and ``report`` are read-only."""
        raise NotImplementedError

    def finalize(
        self, art: ArtifactUnion, report: ReportView
    ) -> tuple[ArtifactExt, ReportExt] | None:
        """Optional dataset-level reduce over the merged union, after all batches."""
        return None


class SourceKernel(_Bound):
    def iter_batches(self) -> Iterator[tuple[ArtifactExt, ReportExt]]:
        """Yield one ``({"batch": Batch}, ReportExt)`` per batch.

        The report fragment carries enumeration findings — e.g. a sample directory with
        no video becomes an ``error`` sample right here, before any decoding.
        """
        raise NotImplementedError

    def finalize(
        self, art: ArtifactUnion, report: ReportView
    ) -> tuple[ArtifactExt, ReportExt] | None:
        return None


class SampleKernel(Kernel):
    """Per-sample check kernel. Subclasses implement :meth:`check`.

    This is where the per-sample failsafe lives, so every check kernel gets it for free.
    """

    #: Fallback check name when the kernel blows up on a sample. Subclasses that define
    #: ``check_name`` report failures under it instead, so one node always produces one
    #: check key per sample whether it succeeded or not.
    error_check_name = "error"

    def check(self, sample: Sample, art: ArtifactUnion):
        """Judge one sample.

        Return either ``{check_name: Check}`` or ``({check_name: Check}, {artifact: value})``.
        Artifact names should be per-sample (e.g. ``f"duration_s/{sample.sample_id}"``) so
        that keys stay disjoint across batches and ``finalize`` can reduce over them.
        """
        raise NotImplementedError

    def run(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]:
        ext: ArtifactExt = {}
        rext = ReportExt()
        for sample in art.batch():
            if not report.is_alive(sample.sample_id):
                continue  # failed upstream: dropped from this node
            try:
                result = self.check(sample, art)
                checks, artifacts = result if isinstance(result, tuple) else (result, {})
            except Exception as e:  # per-sample finding, never a job crash
                name = getattr(self, "check_name", None) or self.error_check_name
                checks, artifacts = {
                    name: Check("error", details={"exception": f"{type(e).__name__}: {e}"})
                }, {}
            rext.checks[sample.sample_id] = dict(checks)
            ext.update(artifacts)
        return ext, rext


@dataclass
class Mutation:
    """A ``MutatingKernel``'s result: extensions plus in-place changes to the union."""

    ext: ArtifactExt = field(default_factory=dict)
    report_ext: ReportExt = field(default_factory=ReportExt)
    replace: dict[str, Any] = field(default_factory=dict)
    delete: frozenset[str] = frozenset()


class MutatingKernel(Kernel):
    """Framework-internal: may rewrite existing union keys. ``DataDumper`` only."""

    def run_mutating(self, art: ArtifactUnion, report: ReportView) -> Mutation:
        raise NotImplementedError

    def finalize_mutating(self, art: ArtifactUnion, report: ReportView) -> Mutation | None:
        return None
