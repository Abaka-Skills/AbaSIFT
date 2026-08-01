"""Kernel interfaces. Inputs are read-only; kernels return only extensions.

Four classes, three of which a QC author might subclass:

* ``SourceKernel`` — node 0. Enumerates a vendor layout into batches of samples.
* ``Kernel`` — anything else. One ``run`` per batch, optional ``digest`` (the
  dataset-level reduce) once every batch has been through the DAG.
* ``SampleKernel`` — the base most check kernels want: it loops the batch calling ``sift``,
  skips samples that already failed upstream, and turns a per-sample exception into
  ``status: error`` so one bad sample never takes down its batchmates.
* ``MutatingKernel`` — framework-internal. Only ``DataArchiver`` may replace or delete
  existing union keys; a kernel that merely *writes* something out is not one of these.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping

from .data import ArtifactUnion, Batch, Sample
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

    def digest(
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

    def digest(
        self, art: ArtifactUnion, report: ReportView
    ) -> tuple[ArtifactExt, ReportExt] | None:
        return None


def batch_stream(
    items: Iterable["Sample | tuple[str, Check]"], batch_size: int
) -> Iterator[tuple[ArtifactExt, ReportExt]]:
    """Group a loader's normalised output into batches — framework policy, not a vendor's.

    Accepts either a ``Sample`` or an enumeration finding ``(sample_id, Check)``. Findings
    ride along with the batch under construction, and a trailing batch carrying nothing but
    findings is still emitted — the edge case every loader would otherwise re-derive.

    A ``SourceKernel`` therefore only has to normalise one vendor layout into samples::

        def iter_batches(self):
            return batch_stream(self._enumerate(), self.batch_size)
    """
    pending: list[Sample] = []
    rext = ReportExt()
    index = 0
    for item in items:
        if isinstance(item, Sample):
            pending.append(item)
        else:
            sample_id, check = item
            rext.add(sample_id, "enumerate", check)
        if len(pending) >= batch_size:
            yield {"batch": Batch(tuple(pending), index)}, rext
            pending, rext, index = [], ReportExt(), index + 1
    if pending or rext.checks:
        yield {"batch": Batch(tuple(pending), index)}, rext


class SampleKernel(Kernel):
    """Per-sample check kernel. Subclasses implement :meth:`sift`.

    This is where the per-sample failsafe lives, so every check kernel gets it for free.
    """

    #: The check name this kernel reports under — including when it fails, so one node
    #: always produces exactly one check key per sample whether it succeeded or not.
    #: Subclasses set it (as a class attribute or in ``__init__``).
    check_name: str = "check"

    def sift(self, sample: Sample, art: ArtifactUnion):
        """Measure, process and judge one sample.

        Return either ``{check_name: Check}`` or ``({check_name: Check}, {artifact: value})``.
        Artifact names should be per-sample (e.g. ``f"duration_s/{sample.sample_id}"``) so
        that keys stay disjoint across batches and ``digest`` can reduce over them.
        """
        raise NotImplementedError

    def run(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]:
        ext: ArtifactExt = {}
        rext = ReportExt()
        for sample in art.batch():
            if not report.is_alive(sample.sample_id):
                continue  # failed upstream: dropped from this node
            try:
                result = self.sift(sample, art)
                checks, artifacts = result if isinstance(result, tuple) else (result, {})
            except Exception as e:  # per-sample finding, never a job crash
                checks, artifacts = {
                    self.check_name: Check("error", details={"exception": f"{type(e).__name__}: {e}"})
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
    """Framework-internal: may rewrite existing union keys. ``DataArchiver`` only."""

    def replaced_key_patterns(self) -> tuple[str, ...]:
        """Union-key globs whose *value* this node will replace, for the validator.

        Two nodes replacing one key on parallel branches produce one key with two values
        and no honest winner, so the pipeline is rejected at load rather than raising
        mid-job. Declaring the patterns keeps that check in the validator, which is the
        only place that can see the graph, without the validator knowing what a
        ``DataArchiver`` is. Deletions are exempt — deleting twice is deleting once.
        """
        return ()

    def run_mutating(self, art: ArtifactUnion, report: ReportView) -> Mutation:
        raise NotImplementedError

    def commit(self, art: ArtifactUnion, report: ReportView) -> Mutation | None:
        """The terminal write, after every ``digest()`` and after the job stats are stamped.

        Not a reduce — nothing is left to compute by the time this runs. It is where a
        mutating kernel *persists* the finished job and rewrites the union to point at what
        it persisted, which is why it is the last thing to happen and why it is the only
        pass that may still change the union.
        """
        return None
