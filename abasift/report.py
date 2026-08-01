"""The report: enforced skeleton, free-form leaves.

Division of labour, and it is the whole point of the schema:

* the **kernel** judges (it alone knows what its measurement means),
* the **YAML** holds thresholds (same kernel, per-vendor strictness, no code change),
* the **framework** only aggregates — sample status is the worst of its checks.

``error`` is a first-class status: unreadable data is a quality defect, not a crash.
"""

from __future__ import annotations

import copy
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

#: 2: per-sample checks lost their `threshold`, and `summary` moved to the pipeline
#: document — see ``doc/design.md`` §5.
SCHEMA_VERSION = 2

PASS, WARN, FAIL, ERROR = "pass", "warn", "fail", "error"
_ORDER = {PASS: 0, WARN: 1, FAIL: 2, ERROR: 3}


def worst(statuses: Iterable[str]) -> str:
    """``error > fail > warn > pass``; no checks at all means ``pass``."""
    best = PASS
    for s in statuses:
        if s not in _ORDER:
            raise ValueError(f"unknown status {s!r}; expected one of {sorted(_ORDER)}")
        if _ORDER[s] > _ORDER[best]:
            best = s
    return best


@dataclass
class Check:
    """One kernel verdict about one sample."""

    status: str
    measurement: Any = None
    threshold: Mapping[str, Any] | None = None
    details: Mapping[str, Any] | None = None

    def __post_init__(self):
        if self.status not in _ORDER:
            raise ValueError(f"unknown status {self.status!r}")

    def to_json(self) -> dict:
        """What this check found. **Not** its thresholds: those are the same for every
        sample the node judged, so they belong once in the pipeline document, not
        repeated a hundred thousand times in the per-sample one."""
        out: dict[str, Any] = {"status": self.status}
        if self.measurement is not None:
            out["measurement"] = self.measurement
        if self.details:
            out["details"] = dict(self.details)
        return out


@dataclass
class ReportExt:
    """What a kernel returns: per-sample checks, and (from ``digest``) a summary.

    ``checks`` is ``{sample_id: {check_name: Check}}``. The executor prefixes check names
    with the node name, so a kernel never needs to know where it sits in the graph.
    """

    checks: dict[str, dict[str, Check]] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)

    def add(self, sample_id: str, name: str, check: Check) -> "ReportExt":
        self.checks.setdefault(sample_id, {})[name] = check
        return self


class Report:
    """The job report. Mutated only by the executor's driver thread."""

    def __init__(
        self,
        job: Mapping[str, Any] | None = None,
        definition: Mapping[str, Any] | None = None,
        yaml_path: str = "",
    ):
        self.schema_version = SCHEMA_VERSION
        self.job: dict[str, Any] = dict(job or {})
        #: ``Pipeline.to_dict()`` — a plain dict, so this module still imports nothing.
        self.definition: dict[str, Any] = dict(definition or {})
        #: The YAML this came from, when it came from a file: the archiver copies it.
        self.yaml_path = yaml_path
        self.samples: dict[str, dict[str, Any]] = {}
        self.summary: dict[str, Any] = {}
        #: ``"node/check" -> threshold`` — recorded once, not per sample.
        self.thresholds: dict[str, dict[str, Any]] = {}

    # -- write -----------------------------------------------------------

    def apply(self, node: str, ext: ReportExt) -> None:
        """Fold one node's fragment in, namespacing check names and re-aggregating."""
        for sample_id, checks in ext.checks.items():
            entry = self.samples.setdefault(sample_id, {"status": PASS, "checks": {}})
            for name, check in checks.items():
                entry["checks"][f"{node}/{name}"] = check
                if check.threshold:
                    # Identical across samples by construction — the kernel reads it from
                    # its params. Kept once here so the per-sample file need not carry it.
                    self.thresholds.setdefault(f"{node}/{name}", dict(check.threshold))
            entry["status"] = worst(c.status for c in entry["checks"].values())
        if ext.summary:
            self.summary.setdefault(node, {}).update(ext.summary)

    def merge(self, other: "Report") -> None:
        """Fold a finished batch report into the job report.

        Per-sample keys are disjoint across batches, so this is a dict union with no
        logic — the reason per-batch streaming works at all.
        """
        for sample_id, entry in other.samples.items():
            if sample_id in self.samples:
                mine = self.samples[sample_id]
                mine["checks"].update(entry["checks"])
                mine["status"] = worst(c.status for c in mine["checks"].values())
            else:  # copy: the other report's entry must not be aliased into ours
                self.samples[sample_id] = {
                    "status": entry["status"],
                    "checks": dict(entry["checks"]),
                }
        for node, s in other.summary.items():
            self.summary.setdefault(node, {}).update(s)
        for key, threshold in other.thresholds.items():
            self.thresholds.setdefault(key, threshold)

    # -- read ------------------------------------------------------------

    def status_of(self, sample_id: str) -> str | None:
        entry = self.samples.get(sample_id)
        return entry["status"] if entry else None

    def counts(self) -> dict[str, int]:
        out = {PASS: 0, WARN: 0, FAIL: 0, ERROR: 0}
        for entry in self.samples.values():
            out[entry["status"]] += 1
        return out

    def to_json(self) -> dict:
        """The **per-sample** document: one entry per data sample, and nothing that is
        the same for all of them. Thresholds, params, the DAG and the dataset-level
        summaries live in :meth:`pipeline_json` instead."""
        return {
            "schema_version": self.schema_version,
            "job": copy.deepcopy(self.job),
            "samples": {
                sid: {
                    "status": e["status"],
                    "checks": {k: c.to_json() for k, c in e["checks"].items()},
                }
                for sid, e in self.samples.items()
            },
        }

    def pipeline_json(self) -> dict:
        """The **per-pipeline** document: what was run, how it was configured, how it did.

        The counterpart to :meth:`to_json`. Everything here is a statement about the job
        or one of its nodes; nothing is per sample. Both files carry the same ``job``
        block so either one can be read on its own.
        """
        per_check: dict[str, Counter] = {}
        per_node: dict[str, Counter] = {}
        for entry in self.samples.values():
            # A node's verdict on a sample is the worst of *its own* checks — the same
            # rule the sample status uses, scoped to one node. Summing the per-check
            # tallies instead would count a sample twice whenever a node has two checks.
            node_worst: dict[str, str] = {}
            for key, check in entry["checks"].items():
                per_check.setdefault(key, Counter())[check.status] += 1
                node = key.partition("/")[0]
                node_worst[node] = worst([node_worst.get(node, PASS), check.status])
            for node, status in node_worst.items():
                per_node.setdefault(node, Counter())[status] += 1

        # One entry per node, carrying both what it *is* and what it *did*. Listing the
        # nodes twice — once as the definition, once as results keyed by name — would
        # leave the reader joining them by hand, which is the duplication this document
        # exists to avoid.
        nodes = []
        for spec in self.definition.get("nodes", []):
            name = spec["name"]
            prefix = f"{name}/"
            entry = dict(copy.deepcopy(spec))
            checks = {
                key[len(prefix) :]: {
                    "counts": dict(counts),
                    **({"threshold": self.thresholds[key]} if key in self.thresholds else {}),
                }
                for key, counts in sorted(per_check.items())
                if key.startswith(prefix)
            }
            if checks:  # a loader or a writer judges nothing; say nothing rather than {}
                entry["counts"] = dict(per_node.get(name, {}))
                entry["checks"] = checks
            if self.summary.get(name):
                entry["summary"] = copy.deepcopy(self.summary[name])
            nodes.append(entry)

        pipeline = {k: v for k, v in copy.deepcopy(self.definition).items() if k != "nodes"}
        return {
            "schema_version": self.schema_version,
            "job": copy.deepcopy(self.job),
            "pipeline": pipeline,
            "nodes": nodes,
        }


class ReportView:
    """Read-only report handed to kernels.

    Its real job is telling a kernel which samples are still alive: a sample whose
    status is already ``error`` failed upstream and must be skipped, not re-decoded.
    """

    __slots__ = ("_r",)

    def __init__(self, report: Report):
        self._r = report

    def status_of(self, sample_id: str) -> str | None:
        return self._r.status_of(sample_id)

    def is_alive(self, sample_id: str) -> bool:
        return self.status_of(sample_id) != ERROR

    def counts(self) -> dict[str, int]:
        return self._r.counts()

    def to_json(self) -> dict:
        return self._r.to_json()

    def pipeline_json(self) -> dict:
        return self._r.pipeline_json()

    @property
    def yaml_path(self) -> str:
        """Where the pipeline was loaded from, if it came from a file."""
        return self._r.yaml_path

    @property
    def definition(self) -> dict:
        """``Pipeline.to_dict()`` — for the archiver writing the pipeline document."""
        return self._r.definition
