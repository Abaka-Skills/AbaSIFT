# Kernels: interfaces and the two demo checks

Modules: `src/abasift/kernel.py`, `src/abasift/kernels/`.

## The interfaces

| class | for | contract |
|-------|-----|----------|
| `SourceKernel` | node 0 | `iter_batches() -> Iterator[(ArtifactExt, ReportExt)]` |
| `Kernel` | everything else | `run(art, report) -> (ArtifactExt, ReportExt)`, optional `finalize(...)` |
| `SampleKernel` | most check kernels | implement `check(sample, art)`; the base class loops the batch |
| `MutatingKernel` | framework-internal | `DataDumper` only, see [dumper.md](dumper.md) |

Inputs are read-only. Kernels return only extensions; the executor namespaces and merges.
Kernels must never mutate the union or report in place — branches run concurrently.

`SampleKernel` is where the **per-sample failsafe** lives, so every check kernel inherits it:
it skips samples already `error` upstream (`report.is_alive`), and turns an exception on one
sample into `status: error` for that sample under the kernel's own `check_name`. `check()`
returns either `{check_name: Check}` or `({check_name: Check}, {artifact_name: value})`.

Artifact names should be **per-sample** (`f"duration_s/{sample.sample_id}"`) so keys stay
disjoint across batches and `finalize()` can reduce over them. That is the whole mechanism
behind dataset-level summaries: `run()` writes per-sample facts, `finalize()` reads its own
namespace out of the merged job union.

Parameters are declared explicitly — no `**kwargs` catch-all — so a typo in YAML is a
load-time error instead of a silently defaulted threshold.

## Demo 1 — `VideoDurationKernel`

Params: `min_s` (0), `max_s` (none), `stream` (`video/main`), `check_name` (`video_length`).

Decodes `video_meta` (header-only, ~0.45 MB per file), emits
`Check(status, measurement=duration_s, threshold, details)` where details carry container,
codec/resolution/fps and the data-track handler names, plus a per-sample artifact
`duration_s/<sample_id>`. `finalize()` reduces those into
`{n_videos, total_s, mean_s, min_s, max_s, shortest, longest}`.

Acceptance (design §6, `test_duration_demo.py`): 2/5/10 s synthesized clips pass within
0.1 s; a deliberately corrupt file is `error` while the job completes; summary numbers
correct.

Real run over `20260730_test` (53 files, 15.7 GB): **21.6 s**, `pass=50 fail=3 error=0`,
total 31118 s of footage, mean 587 s. The three failures are takes longer than the
`max_s: 1800` the YAML allows (longest 3449 s) — the vendor never split those recordings.

## Demo 2 — `ImuSpikeKernel`

Params: `stream` (`imu/main`), `z_thresh` (8.0), `max_spikes` (0), `warn_spikes` (none),
`min_samples` (64), `check_name` (`imu_spike`).

Real egocentric motion is band-limited: between two samples the acceleration changes by a
small, *typical* amount. A corrupt sample (dropped packet, bit flip, sensor glitch) is a
one-sample jump far outside that distribution. So:

```
jerk[i] = a[i+1] - a[i]                        per axis, in g
scale   = 1.4826 * MAD(jerk)                   per axis
z[i]    = max_axis |jerk[i] - median(jerk)| / scale
spike  <=> z[i] > z_thresh
```

**Median/MAD, not mean/std** — deliberately. A handful of large spikes inflates a standard
deviation enough to hide themselves, which is the classic failure of a naive z-score;
`test_robust_scale_is_not_fooled_by_many_spikes` pins that down with ten injected impulses.
The 1.4826 factor makes MAD a consistent estimator of σ for Gaussian noise, so `z_thresh`
reads in familiar sigma units. A single impulse perturbs two consecutive differences, so it
counts as 2 — documented rather than smoothed over.

Verdict: `> max_spikes` → `fail`, else `> warn_spikes` → `warn`, else `pass`. A track shorter
than `min_samples` is `error` (unmeasurable), not `pass`. Details report `max_z`,
`spike_times_s`, `n_samples`, `rate_hz`, accel magnitude stats, and the device/layout that
produced the track. `finalize()` reduces to
`{n_tracks, total_spikes, tracks_with_spikes, worst_sample, worst_max_z}`.

A frozen sensor produces *no* jerk at all and so no spikes; the scale floor keeps 0/0 from
manufacturing false positives. Frozen-value detection is a different defect class and would
be a sibling kernel.

Real run over `general_324h` (4 smallest samples, `z_thresh: 12`): 2 pass with 0 spikes
(max_z 5.2 and 7.2, median ‖a‖ 0.996 and 1.006 g at 29.97/25.0 Hz), and 2 legitimate errors —
one file is phone-recorded with no `DJI meta` track (`MissingStream`), one is a 1.6 s clip
with ~48 records, below `min_samples`. The job completed and reported all four.

## Writing a check kernel

```python
class MyCheck(SampleKernel):
    check_name = "my_check"

    def __init__(self, max_thing: float = 1.0, stream: str = "video/main"):
        self.max_thing = float(max_thing)
        self.stream = stream

    def check(self, sample, art):
        value = measure(sample.stream(self.stream).decode())
        status = "fail" if value > self.max_thing else "pass"
        check = Check(status, measurement=value, threshold={"max_thing": self.max_thing})
        return {self.check_name: check}, {f"value/{sample.sample_id}": value}
```

Reference it from YAML by dotted import path. Don't catch decode errors — the base class
turns them into findings. Don't judge with hardcoded numbers — take them as params.
