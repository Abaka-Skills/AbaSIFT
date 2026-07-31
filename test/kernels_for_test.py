"""Test-only kernels, referenced from pipeline YAML by dotted path."""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterator

import numpy as np

from abasift import (
    ArtifactUnion,
    Check,
    Kernel,
    LazyRaw,
    ReportExt,
    ReportView,
    Sample,
    SampleKernel,
    SourceKernel,
)
from abasift.kernel import batch_stream
from abasift.lazy import register_decoder
from abasift.payloads import ImuTrack

#: How many times each URI was decoded. Proves parallel branches share one decode.
DECODE_COUNTS: Counter[str] = Counter()


@register_decoder("counting")
def _decode_counting(raw: LazyRaw) -> str:
    DECODE_COUNTS[raw.uri] += 1
    return raw.uri


class SyntheticLoader(SourceKernel):
    """Source over made-up samples: exercises the executor without any real I/O."""

    def __init__(self, n: int = 6, batch_size: int = 3, decoder: str = "counting"):
        self.n = int(n)
        self.batch_size = int(batch_size)
        self.decoder = decoder

    def iter_batches(self) -> Iterator[tuple[dict[str, Any], ReportExt]]:
        return batch_stream(
            (
                Sample(f"s{i}", {"blob/main": LazyRaw(f"mem://sample/{i}", self.decoder)})
                for i in range(self.n)
            ),
            self.batch_size,
        )


class TouchKernel(SampleKernel):
    """Decodes ``blob/main`` and passes. Two of these in parallel must decode once."""

    check_name = "touched"

    def sift(self, sample, art):
        value = sample.stream("blob/main").decode()
        return {self.check_name: Check("pass", measurement=value)}

#: Sample ids each RecordingKernel instance was handed. Used to prove that a sample which
#: errored upstream is dropped from downstream nodes.
SEEN: dict[str, list[str]] = {}


class RecordingKernel(Kernel):
    """Records which samples reached it, and how many were skipped as already-failed."""

    def run(self, art: ArtifactUnion, report: ReportView):
        batch = art.batch()
        seen = [s.sample_id for s in batch if report.is_alive(s.sample_id)]
        SEEN.setdefault(self.node_name, []).extend(seen)
        return {f"seen/{batch.index}": sorted(seen)}, ReportExt()


class ExplodingKernel(SampleKernel):
    """Raises on every sample: exercises the per-sample failsafe."""

    check_name = "boom"

    def sift(self, sample, art):
        raise RuntimeError("deliberate kernel failure")


class WholeNodeExplodingKernel(Kernel):
    """Raises before touching any sample: exercises the whole-node failsafe."""

    def run(self, art, report):
        raise RuntimeError("deliberate node failure")


# -- synthetic IMU, so IMU tests need no network and no MP4 ---------------


def synth_imu(
    n: int = 1200,
    rate_hz: float = 60.0,
    spikes: tuple[tuple[int, float], ...] = (),
    seed: int = 0,
) -> ImuTrack:
    """Band-limited walking-like acceleration in g, with optional one-sample impulses."""
    rng = np.random.default_rng(seed)
    t = np.arange(n) / rate_hz
    accel = np.stack(
        [
            0.15 * np.sin(2 * np.pi * 1.8 * t) + rng.normal(0, 0.01, n),
            0.10 * np.cos(2 * np.pi * 1.1 * t) + rng.normal(0, 0.01, n),
            -1.0 + 0.08 * np.sin(2 * np.pi * 2.4 * t) + rng.normal(0, 0.01, n),
        ],
        axis=1,
    )
    for index, amplitude in spikes:
        accel[index, 0] += amplitude
    quat = np.tile(np.array([1.0, 0.0, 0.0, 0.0]), (n, 1))
    return ImuTrack(t=t, accel_g=accel, quat=quat, device="synthetic", layout="test")


_TRACKS: dict[str, ImuTrack] = {}


def put_track(name: str, track: ImuTrack) -> LazyRaw:
    """Register a synthetic track and return a LazyRaw that decodes to it."""
    _TRACKS[name] = track
    return LazyRaw(f"memory://imu/{name}", "test_imu")


@register_decoder("test_imu")
def _decode_test_imu(raw: LazyRaw) -> ImuTrack:
    return _TRACKS[raw.uri.rsplit("/", 1)[-1]]


@register_decoder("test_imu_stream")
def _decode_imu_stream(raw: LazyRaw) -> ImuTrack:
    """Synthetic IMU for SyntheticLoader's samples; only ``s1`` carries an impulse."""
    index = int(raw.uri.rsplit("/", 1)[-1])
    return synth_imu(n=600, rate_hz=50.0, spikes=((100, 4.0),) if index == 1 else (), seed=index)
