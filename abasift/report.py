"""The report: enforced skeleton, free-form leaves.

Division of labour, and it is the whole point of the schema:

* the **kernel** judges (it alone knows what its measurement means),
* the **YAML** holds thresholds (same kernel, per-vendor strictness, no code change),
* the **framework** only aggregates — sample status is the worst of its checks.

``error`` is a first-class status: unreadable data is a quality defect, not a crash.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

SCHEMA_VERSION = 1

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
        out: dict[str, Any] = {"status": self.status}
        if self.measurement is not None:
            out["measurement"] = self.measurement
        if self.threshold:
            out["threshold"] = dict(self.threshold)
        if self.details:
            out["details"] = dict(self.details)
        return out


@dataclass
class ReportExt:
    """What a kernel returns: per-sample checks, and (from ``finalize``) a summary.

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

    def __init__(self, job: Mapping[str, Any] | None = None):
        self.schema_version = SCHEMA_VERSION
        self.job: dict[str, Any] = dict(job or {})
        self.samples: dict[str, dict[str, Any]] = {}
        self.summary: dict[str, Any] = {}

    # -- write -----------------------------------------------------------

    def apply(self, node: str, ext: ReportExt) -> None:
        """Fold one node's fragment in, namespacing check names and re-aggregating."""
        for sample_id, checks in ext.checks.items():
            entry = self.samples.setdefault(sample_id, {"status": PASS, "checks": {}})
            for name, check in checks.items():
                entry["checks"][f"{node}/{name}"] = check
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
            "summary": copy.deepcopy(self.summary),
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
