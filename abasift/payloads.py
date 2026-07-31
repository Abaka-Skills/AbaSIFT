"""Decoded payload types — what ``LazyRaw.decode()`` hands a kernel.

These are the typed contract between a vendor decoder and a vendor-agnostic kernel:
``ImuSpikeKernel`` works on any vendor whose loader can produce an :class:`ImuTrack`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np


@dataclass(frozen=True)
class VideoMeta:
    """Container-level facts, obtained from the header alone (no frame decoding)."""

    duration_s: float
    container: str
    n_streams: int
    video: dict[str, Any] | None = None
    audio: dict[str, Any] | None = None
    data_tracks: tuple[str, ...] = ()
    source: str = ""

    def to_json(self) -> dict:
        return {
            "duration_s": self.duration_s,
            "container": self.container,
            "n_streams": self.n_streams,
            "video": self.video,
            "audio": self.audio,
            "data_tracks": list(self.data_tracks),
        }


@dataclass(frozen=True)
class ImuTrack:
    """A uniformly sampled inertial track.

    ``t`` seconds from track start; ``accel_g`` (N,3) in g; ``quat`` (N,4) as w,x,y,z
    (empty array when the vendor gives no orientation).
    """

    t: np.ndarray
    accel_g: np.ndarray
    quat: np.ndarray = field(default_factory=lambda: np.empty((0, 4)))
    device: str = ""
    layout: str = ""

    def __post_init__(self):
        if self.accel_g.ndim != 2 or self.accel_g.shape[1] != 3:
            raise ValueError(f"accel_g must be (N,3), got {self.accel_g.shape}")
        if len(self.t) != len(self.accel_g):
            raise ValueError(f"t/accel_g length mismatch: {len(self.t)} vs {len(self.accel_g)}")

    def __len__(self) -> int:
        return len(self.t)

    @property
    def duration_s(self) -> float:
        return float(self.t[-1] - self.t[0]) if len(self.t) > 1 else 0.0

    @property
    def rate_hz(self) -> float:
        """Mean sample rate. 0.0 for a degenerate track."""
        if len(self.t) < 2:
            return 0.0
        dt = float(np.mean(np.diff(self.t)))
        return 1.0 / dt if dt > 0 else 0.0

    def magnitude_g(self) -> np.ndarray:
        return np.linalg.norm(self.accel_g, axis=1)

    def to_json(self) -> dict:
        """Summary only — never the samples (a 5-minute track is ~18k rows)."""
        return {
            "n_samples": len(self),
            "rate_hz": round(self.rate_hz, 3),
            "duration_s": round(self.duration_s, 3),
            "device": self.device,
            "layout": self.layout,
        }
