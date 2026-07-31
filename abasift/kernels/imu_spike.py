"""``ImuSpikeKernel`` — sensor-corruption check: impulsive discontinuities in the IMU.

What counts as a spike. Real egocentric motion is band-limited: between two samples at
~60 Hz the acceleration changes by a small, *typical* amount. A corrupt sample (dropped
packet, bit flip, sensor glitch) shows up as a one-sample jump far outside that
distribution. So we look at the **first difference per axis** and score it against a
**robust** spread of the same signal:

    jerk[i]  = a[i+1] - a[i]                                (per axis, in g)
    scale    = 1.4826 * MAD(jerk)                           (per axis; ~sigma if Gaussian)
    z[i]     = max_axis |jerk[i] - median(jerk)| / scale
    spike    <=> z[i] > z_thresh

Median/MAD rather than mean/std on purpose: a few large spikes inflate a standard
deviation enough to hide themselves, which is exactly the failure mode of a naive
z-score. The 1.4826 factor makes MAD a consistent estimator of sigma for Gaussian noise,
so ``z_thresh`` reads in familiar sigma units.

Verdicts come from YAML: ``z_thresh`` sets what "outlier" means, ``max_spikes`` /
``warn_spikes`` set how many the vendor is allowed. Frozen or dead-flat sensors are a
*different* defect (they produce no spikes at all) and are out of scope here.
"""

from __future__ import annotations

import numpy as np

from ..data import ArtifactUnion, Sample
from ..kernel import ArtifactExt, SampleKernel
from ..payloads import ImuTrack
from ..report import Check, ReportExt, ReportView

#: Floor on the robust scale, in g. Guards 0/0 on a perfectly flat (frozen) track.
_SCALE_FLOOR_G = 1e-6


class ImuSpikeKernel(SampleKernel):
    """Params:

    ``stream``       canonical IMU stream, default ``imu/main``
    ``z_thresh``     robust z above which a sample counts as a spike, default 8.0
    ``max_spikes``   more than this many spikes -> ``fail``, default 0
    ``warn_spikes``  more than this many -> ``warn`` (set below ``max_spikes``), default none
    ``min_samples``  shorter tracks are unmeasurable -> ``error``, default 64
    ``check_name``   default ``imu_spike``
    """

    def __init__(
        self,
        stream: str = "imu/main",
        z_thresh: float = 8.0,
        max_spikes: int = 0,
        warn_spikes: int | None = None,
        min_samples: int = 64,
        check_name: str = "imu_spike",
    ):
        self.stream = stream
        self.z_thresh = float(z_thresh)
        self.max_spikes = int(max_spikes)
        self.warn_spikes = None if warn_spikes is None else int(warn_spikes)
        self.min_samples = int(min_samples)
        self.check_name = check_name

    def sift(self, sample: Sample, art: ArtifactUnion):
        imu: ImuTrack = sample.stream(self.stream).decode()
        threshold = {
            "z_thresh": self.z_thresh,
            "max_spikes": self.max_spikes,
            "warn_spikes": self.warn_spikes,
        }
        if len(imu) < self.min_samples:
            check = Check(
                "error",
                measurement=None,
                threshold=threshold,
                details={"reason": f"track has {len(imu)} samples, need {self.min_samples}"},
            )
            return {self.check_name: check}, {}

        n_spikes, max_z, spike_t = spike_scan(imu, self.z_thresh)
        if n_spikes > self.max_spikes:
            status = "fail"
        elif self.warn_spikes is not None and n_spikes > self.warn_spikes:
            status = "warn"
        else:
            status = "pass"

        magnitude = imu.magnitude_g()
        check = Check(
            status=status,
            measurement=n_spikes,
            threshold=threshold,
            details={
                "max_z": round(max_z, 2),
                "spike_times_s": [round(float(t), 3) for t in spike_t[:10]],
                "n_samples": len(imu),
                "rate_hz": round(imu.rate_hz, 3),
                "duration_s": round(imu.duration_s, 3),
                "accel_g": {
                    "median_magnitude": round(float(np.median(magnitude)), 4),
                    "min_magnitude": round(float(magnitude.min()), 4),
                    "max_magnitude": round(float(magnitude.max()), 4),
                },
                "device": imu.device,
                "layout": imu.layout,
            },
        )
        return (
            {self.check_name: check},
            {
                f"n_spikes/{sample.sample_id}": n_spikes,
                f"max_z/{sample.sample_id}": round(max_z, 3),
                f"rate_hz/{sample.sample_id}": round(imu.rate_hz, 3),
            },
        )

    def finalize(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]:
        spikes = art.per_sample(self.node_name, "n_spikes")
        zs = art.per_sample(self.node_name, "max_z")
        if not spikes:
            return {}, ReportExt(summary={"n_tracks": 0})
        summary = {
            "n_tracks": len(spikes),
            "total_spikes": int(sum(spikes.values())),
            "tracks_with_spikes": sum(1 for v in spikes.values() if v),
            "worst_sample": max(zs, key=zs.get) if zs else None,
            "worst_max_z": max(zs.values()) if zs else None,
        }
        return {}, ReportExt(summary=summary)


def spike_scan(imu: ImuTrack, z_thresh: float) -> tuple[int, float, np.ndarray]:
    """``(n_spikes, max_z, spike_times_s)`` for one track. Pure function, easy to test."""
    accel = imu.accel_g
    jerk = np.diff(accel, axis=0)
    centre = np.median(jerk, axis=0)
    deviation = np.abs(jerk - centre)
    mad = np.median(deviation, axis=0)
    scale = np.maximum(1.4826 * mad, _SCALE_FLOOR_G)
    z = np.max(deviation / scale, axis=1)
    hits = z > z_thresh
    # jerk[i] sits between samples i and i+1; attribute the spike to i+1.
    return int(hits.sum()), float(z.max(initial=0.0)), imu.t[1:][hits]
