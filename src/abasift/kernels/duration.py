"""``VideoDurationKernel`` — the reference check kernel.

Small on purpose: it shows the whole contract in 40 lines. Measure one thing per sample,
judge it against YAML thresholds, emit a per-sample artifact, then reduce those artifacts
into a dataset summary in ``finalize()``.

Cost note: it decodes ``video_meta``, which reads the container header over a ranged GET
(0.25% of a 178 MB file). Probing a 15 GB delivery costs megabytes.
"""

from __future__ import annotations

from typing import Any

from ..data import ArtifactUnion, Sample
from ..kernel import ArtifactExt, SampleKernel
from ..report import Check, ReportExt, ReportView


class VideoDurationKernel(SampleKernel):
    """Params: ``min_s`` (default 0), ``max_s`` (default none), ``stream``, ``check_name``."""

    def __init__(
        self,
        min_s: float = 0.0,
        max_s: float | None = None,
        stream: str = "video/main",
        check_name: str = "video_length",
    ):
        self.min_s = float(min_s)
        self.max_s = None if max_s is None else float(max_s)
        self.stream = stream
        self.check_name = check_name

    def check(self, sample: Sample, art: ArtifactUnion):
        meta = sample.stream(self.stream).decode()
        duration = round(float(meta.duration_s), 3)
        too_short = duration < self.min_s
        too_long = self.max_s is not None and duration > self.max_s
        check = Check(
            status="fail" if (too_short or too_long) else "pass",
            measurement=duration,
            threshold={"min_s": self.min_s, "max_s": self.max_s},
            details={
                "container": meta.container,
                "video": meta.video,
                "data_tracks": list(meta.data_tracks),
                "verdict": "too_short" if too_short else "too_long" if too_long else "in_range",
            },
        )
        # Per-sample artifact key: disjoint across batches, so finalize() can reduce it.
        return {self.check_name: check}, {f"duration_s/{sample.sample_id}": duration}

    def finalize(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]:
        durations = {
            key[len("duration_s/") :]: value
            for key, value in art.under(self.node_name).items()
            if key.startswith("duration_s/")
        }
        if not durations:
            return {}, ReportExt(summary={"n_videos": 0})
        values = sorted(durations.values())
        total = sum(values)
        summary = {
            "n_videos": len(values),
            "total_s": round(total, 3),
            "mean_s": round(total / len(values), 3),
            "min_s": values[0],
            "max_s": values[-1],
            "shortest": min(durations, key=durations.get),
            "longest": max(durations, key=durations.get),
        }
        return {}, ReportExt(summary=summary)
