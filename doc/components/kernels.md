# Kernels: interfaces and the two demo checks

Modules: `abasift/kernel.py`, `abasift/kernels/`.

## The interfaces

| class | for | contract |
|-------|-----|----------|
| `SourceKernel` | node 0 | `iter_batches() -> Iterator[(ArtifactExt, ReportExt)]` |
| `Kernel` | everything else | `run(art, report) -> (ArtifactExt, ReportExt)`, optional `digest(...)` |
| `SampleKernel` | most check kernels | implement `sift(sample, art)`; the base class loops the batch |
| `MutatingKernel` | framework-internal | `DataArchiver` only, see [archiver.md](archiver.md) |

Inputs are read-only. Kernels return only extensions; the executor namespaces and merges.
Kernels must never mutate the union or report in place — branches run concurrently.

`SampleKernel` is where the **per-sample failsafe** lives, so every check kernel inherits it:
it skips samples already `error` upstream (`report.is_alive`), and turns an exception on one
sample into `status: error` for that sample under the kernel's own `check_name` — a declared
class attribute, so one node always contributes exactly one check key per sample. `sift()`
returns either `{check_name: Check}` or `({check_name: Check}, {artifact_name: value})` — it
both processes the sample and judges it, which is why it is not called `check()`.

`SourceKernel` gets the mirror-image helper: `batch_stream(items, batch_size)` groups a
loader's normalised output into batches, so batching policy lives in the framework rather
than being copy-pasted into every vendor loader.

Artifact names should be **per-sample** (`f"duration_s/{sample.sample_id}"`) so keys stay
disjoint across batches and `digest()` can reduce over them. That is the whole mechanism
behind dataset-level summaries: `run()` writes per-sample facts, and `digest()` reads them
back with `art.per_sample(self.node_name, "duration_s")` — the accessor that owns the naming
convention, so the write-name and read-name can't drift apart.

Parameters are declared explicitly — no `**kwargs` catch-all — so a typo in YAML is a
load-time error instead of a silently defaulted threshold.

## Demo 1 — `VideoDurationKernel`

Params: `min_s` (0), `max_s` (none), `stream` (`video/main`), `check_name` (`video_length`).

Decodes `video_meta` (header-only, ~0.45 MB per file), emits
`Check(status, measurement=duration_s, threshold, details)` where details carry container,
codec/resolution/fps and the data-track handler names, plus a per-sample artifact
`duration_s/<sample_id>`. `digest()` reduces those into
`{n_videos, total_s, mean_s, min_s, max_s, shortest, longest}`.

Acceptance (design §6, `test_duration_demo.py`): 2/5/10 s synthesized clips pass within
0.1 s; a deliberately corrupt file is `error` while the job completes; summary numbers
correct.

Real run over `1_test_20260730` (53 files, 15.7 GB): **21.6 s**, `pass=50 fail=3 error=0`,
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
produced the track. `digest()` reduces to
`{n_tracks, total_spikes, tracks_with_spikes, worst_sample, worst_max_z}`.

A frozen sensor produces *no* jerk at all and so no spikes; the scale floor keeps 0/0 from
manufacturing false positives. Frozen-value detection is a different defect class and would
be a sibling kernel.

Real run over `0_egoverse_20260730` (4 smallest samples, `z_thresh: 12`): 2 pass with 0 spikes
(max_z 5.2 and 7.2, median ‖a‖ 0.996 and 1.006 g at 29.97/25.0 Hz), and 2 legitimate errors —
one file is phone-recorded with no `DJI meta` track (`MissingStream`), one is a 1.6 s clip
with ~48 records, below `min_samples`. The job completed and reported all four.

## The shared decode — `VideoFrameKernel`

Params: `fps` (unset — keep every frame), `width` (source width), `height` (source
height, or the aspect-preserving value when only `width` is given), `stream`
(`video/main`), `check_name` (`frames`).

**The default is a full-rate decode**, and deliberately: a subsampled stack cannot show a
defect that fell between its frames, and a check reading one has no way to know that is why
it saw nothing. Subsampling is a cost decision, so a pipeline states it (`fps: 1.0`) once a
check says it can afford one — silence gets the lossless answer, not the cheap one.

Not a check — the one node that decodes *for other kernels*, so N pixel-level checks over
one video decode it once instead of N times. Emits `frames/<sample_id>` ->
`LazyRaw(video_frames)`; a reader calls `.decode()` and gets a
[`VideoFrames`](../../abasift/payloads.py): `data` as `(N, H, W, 3)` uint8 RGB, plus `fps`,
`size`, `t` (timestamps) and `source`. The check itself is `pass` with
`measurement = n_frames` (or `error`, from the `SampleKernel` failsafe, when the file will
not decode); `digest()` reports `{n_videos, n_frames, cache_bytes}` — the last being what
the stacks now occupy in the worker disk cache, i.e. the number that says whether
`pipeline.cache.size_gb` is big enough.

**Why the union carries a handle and not the array.** The executor merges every batch's
artifacts into the job union and `without_transients()` drops only the `Batch`, so an
ndarray parked there would live for the whole job — a few hundred batches of it is the end
of the worker. So frames are written to the disk cache and the union carries the same kind
of `LazyRaw` the `DataArchiver` swaps in. Consequences, all of them wanted: a reader gets a
`np.memmap` rather than a heap copy; a stack outlives its batch, so a later node can read
it; a `DataArchiver` in free mode deletes it (`keys: ["frames/frames/*"], target: ""`).

**Sampling.** Frames are selected by presentation time — emit when `t >= next_t`, then
advance `next_t` by `1/fps` — so a variable-rate source still yields a uniform grid and a
gap in the source does not shift it. This is the rule ffmpeg's `fps` filter uses. Scaling
is swscale via `frame.reformat`, so the output matches `-vf fps=F,scale=W:H` frame for frame.

**Cost.** The stack is raw rgb24 and is routinely *larger than the source* — which is why
the default costs what it costs: the 13 clips in
`video_quality_defects/` are 159 MB of H.264 and 381 MB of frames **at 1 fps** — of which the
five 3840x1200 base clips alone are 5 frames × 13.2 MB each. Peak RSS while decoding is
libav's frame-thread pool (~340 MB on a 3840x1200 H.264 with 36 cores, vs ~140 MB
single-threaded), not the stack: the stack is written a frame at a time and read back
memory-mapped. Deriving from a memmap copies, so `frames.data.astype("float32")` on that
same clip costs its 264 MB like any other array — the memmap saves you from holding what
you don't touch, not from what you do.

The cache key is `f(source uri, fps, size)`, so two kernels asking for the same stack share
one file and a retried job re-uses it instead of re-decoding — the same determinism rule the
archiver's paths follow.

## The exhibit writer — `VideoDumper`

Params: `fps` (the stack's own rate), `target` (same semantics as `DataArchiver`, minus free
mode), `codec` (`libx264`), `frames_node` (auto), `check_name` (`video`).

The counterpart to `VideoFrameKernel`: that node turns a video into frames for kernels to
measure, this one turns frames back into a video for a *person* to look at — a proxy of the
take that failed, small enough to attach to the finding. 13 clips, 159 MB of source → 199
frames at 2 fps → **1.9 MB of exhibits** in 5.3 s.

```yaml
- name: proxy
  kernel: abasift.kernels.VideoDumper
  params: {fps: 8.0, target: /data/exhibits}   # 4x timelapse of a 2 fps stack
  inputs: [frames]
```

`fps` is the **playback** rate and is deliberately independent of the rate the stack was
sampled at: a 1 fps stack written at 1 fps is a real-time proxy, the same stack at 25 fps is
a timelapse. Neither is more correct, so the pipeline says which it wants. It is stored as a
`Fraction`, so 29.97 lands as 30000/1001 rather than as a float that rounds.

It writes to `target` under the shared `DumpTarget` scheme (`_dump.py`), so retries overwrite
— but it is **not** a `MutatingKernel`: it adds `video/<sample_id>` -> `LazyRaw(video_meta)`
and rewrites nothing, so the "only `DataArchiver` may rewrite the union" rule is untouched. The
encode goes into the disk cache first and is copied out from there, so a retried job re-uses
the expensive half and still writes the target.

Which stack to write is found **by decoder**, not by a configured node name — `art.find_lazy`,
the same rule as `art.batch()`. Two stacks in one DAG is a real configuration, so ambiguity
is an error naming both and telling you to set `frames_node`, never a guess. A sample with no
stack (this node placed off a `VideoFrameKernel` branch) is a per-sample `error` like any
other finding; the job completes.

`yuv420p` needs even dimensions, so an odd-sized stack is rejected with that in the message
rather than by libx264 three frames later.

## Writing a check kernel

```python
class MyCheck(SampleKernel):
    check_name = "my_check"

    def __init__(self, max_thing: float = 1.0, stream: str = "video/main"):
        self.max_thing = float(max_thing)
        self.stream = stream

    def sift(self, sample, art):
        value = measure(sample.stream(self.stream).decode())
        status = "fail" if value > self.max_thing else "pass"
        check = Check(status, measurement=value, threshold={"max_thing": self.max_thing})
        return {self.check_name: check}, {f"value/{sample.sample_id}": value}
```

Reference it from YAML by dotted import path. Don't catch decode errors — the base class
turns them into findings. Don't judge with hardcoded numbers — take them as params.
