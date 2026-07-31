# Ego_QA — Distributed Egocentric Dataset QC Framework: Design

Status: **implemented** (2026-07-30). This document is the decision log and contract spec.
Implementation must not deviate without updating this file. Per-component detail and
progress live in [progress.md](progress.md) and [components/](components/).

Python package name is **`abasift`** (CLI: `abasift run …`); this document predates that
naming and used `ego_qa` in examples — see decision log #15.

## 0. Scope

We build a **framework/SDK** (scope 1), not a platform:

- The abstractions: `LazyRaw`, `Sample`/`Batch`, `ArtifactUnion`, `Report`,
  `Kernel`/`SourceKernel`, pipeline YAML, single-machine `Executor`, `DataDumper`, CLI.
- Two **demo pipelines**, both running against the real vendor bucket:
  video duration probe, and an IMU spike check over telemetry embedded in the MP4.
- **Out of scope:** work distribution. An external system generates one job YAML per
  machine (e.g. by splitting S3 prefixes); our contract is *"given one YAML, one
  machine runs it to completion and emits the report + artifacts for that YAML's
  scope."* Cross-machine report merging is also external.
- QC kernels (blur, lighting, sync, sensor corruption, …) are implemented by other
  teams against the `Kernel` interface.

Language: Python ≥ 3.11. Parallelism: **threads only** (heavy libs — PyAV, numpy,
torch, I/O — release the GIL; design transfers unchanged to free-threaded CPython later).

## 1. Data model

### 1.1 LazyRaw — the lazy handle

```python
LazyRaw(uri: str, decoder: DecoderRef)   # fully serializable: uri + decoder name
lazyraw.decode() -> Any                  # bytes / ndarray / av container / ...
```

- Nothing is downloaded at load time. A pipeline that only checks IMU never touches
  video bytes.
- **Three access modes, chosen per decoder** (added at implementation time, decision #16):
  `open()` = remote seekable file object, ranged GETs only; `local_path()` = stream the
  object into the disk cache once and work on a file; `read_bytes(max_bytes)` = whole
  object in memory, *refusing* oversize payloads. Measured on a 177.9 MB DJI MP4: a
  container-header probe reads 0.45 MB (0.25%), while demuxing the interleaved telemetry
  track over the network reads 177.90 MB in 4406 ranged GETs — so headers stay remote and
  interleaved tracks materialise. See [components/lazyraw-cache.md](components/lazyraw-cache.md).
- **Two-tier cache:** raw bytes cached on a worker-global disk scratch dir keyed by
  URI (size-capped LRU) — the S3→disk bridge; decoded object memoized in-memory on
  the instance, **released when its batch's DAG run completes** — the disk→memory
  bridge. Internal lock prevents concurrent double-decode from parallel DAG branches.
- I/O goes through **fsspec**: `s3://…` and local paths behave identically.
  Credentials come from the standard AWS chain (env, `~/.aws/credentials`) —
  **never from YAML or checked-in files**.
- A `decode()` failure inside a kernel is a **QC finding** (`status: error` for that
  sample at that node), not a worker crash.

### 1.2 Sample and Batch

- **Sample** = metadata header + dict of **canonical named streams**
  (`video/cam_front`, `imu/left`, `captions`, …) whose values are `LazyRaw`s.
  The per-vendor dataloader's whole job is normalizing vendor directory layout into
  these canonical names + URIs + decoders. Stream naming/typing is a framework-owned
  registry (every stream type declares required fields, e.g. a common time base —
  sync kernels depend on this contract).
- **Batch** = list of Samples. Batch is an **efficiency unit only** (GPU
  vectorization, prefetch); report and artifacts stay keyed per-sample. A failed
  sample fails alone; its batchmates continue.

### 1.3 ArtifactUnion

- Flat map `"{node_name}/{artifact_name}" -> value`, per job. Values are
  serializable primitives or `LazyRaw`/file references — never live Python objects.
- **Extend-only (write-once):** a node adds keys under its own namespace, never
  overwrites. Collisions are impossible by construction; runs are deterministic
  regardless of branch scheduling. Every intermediate survives to the end, so the
  dumper can select any node's output.
- **Union at joins is executor-owned:** key-wise dict union. Duplicate keys arriving
  via two ingress edges (diamond topology) are identical by construction and merge
  silently; a genuine value conflict is an executor error, never a kernel concern.
- **The loaded Batch itself travels inside the ArtifactUnion** under the loader
  node's namespace (e.g. `load/batch`). One uniform kernel signature; no
  special-cased batch parameter.

## 2. Pipeline definition

One YAML fully describes a job (no separate loader/job config):

```yaml
pipeline:
  name: basic_video_qc
  nodes:
    - name: load                                # unique node name = artifact namespace
      kernel: abasift.loaders.FlatDirLoader     # dotted import path, fails fast at load
      params: { root: s3://bucket/vendor_a, batch_size: 8 }
      inputs: []                                # [] + SourceKernel => source node
    - name: duration
      kernel: abasift.kernels.VideoDurationKernel
      params: { max_s: 1800 }
      inputs: [load]
    - name: dump_report
      kernel: abasift.kernels.DataDumper
      params: { keys: ["__report__"], target: /data/out/vendor_a }
      inputs: [duration]
```

- Edges = `inputs:` lists. Executor topo-sorts and validates (acyclic, unique names,
  importable kernels, exactly one source) at load time.
- Kernel **params are strict** — kernels declare parameters explicitly, no `**kwargs`
  catch-all, so a mistyped threshold is a load-time `PipelineError` rather than a job that
  silently reports against defaults (decision #20).
- Kernel resolution = **dotted import path** (no registry; registered aliases can be
  layered on later without breaking anything).
- Thresholds/severity live in `params` — same kernel, per-vendor strictness, no code
  changes.

## 3. Kernel interfaces

Exactly two interfaces; inputs are **read-only**, kernels return only extensions,
the executor merges and namespaces.

```python
class Kernel:
    def run(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]:
        """Called once per batch."""
    def finalize(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt] | None:
        """Optional. Called once after all batches, on the merged union/report.
        This is where dataset-level reduces live (means, failure rates)."""

class SourceKernel:
    def iter_batches(self) -> Iterator[tuple[ArtifactExt, ReportExt]]:
        """Each yield = one batch manifest (+ initial report fragment: sample ids,
        enumeration errors as error-status samples)."""

class SampleKernel(Kernel):
    """What most check kernels subclass. Implements run() as a per-sample loop that skips
    samples already errored upstream and converts a per-sample exception into
    status: error — the failsafe lives here, once, for everyone."""
    def check(self, sample: Sample, art: ArtifactUnion) -> dict[str, Check] | tuple[dict, dict]: ...
```

`ArtifactExt` is `{name: value}` (executor namespaces it). `ReportExt` is a typed object
carrying `checks: {sample_id: {check_name: Check}}` and `summary: dict` — one return type
serves both `run()` and `finalize()` (decision #17). `report` is a read-only `ReportView`
whose load-bearing method is `is_alive(sample_id)`.

### DataDumper — the one sanctioned mutation

Ordinary kernels are extend-only. The framework-owned `DataDumper` may additionally:

- **Dump:** for each key matching its configured globs, write the binary to the
  target (local or `s3://`), then **replace the value in-place with a `LazyRaw`**
  pointing at the dumped file. Downstream kernels are oblivious (`.decode()` works
  either way). Memory freed, information preserved.
- **Delete (empty/null target):** remove the key and its local backing file — for
  intermediates nobody downstream reads.
- **Deterministic dump paths:** `uri = f(job_id, node, key)` — no timestamps —
  so retries overwrite idempotently.

Dump/free points are explicit YAML nodes, visible in the graph. Pipeline authors
must insert them after heavy stages — a documented pattern, not an optimization.

## 4. Execution semantics

**Per-batch streaming + terminal finalize** (stage-wise model was considered and
rolled back — see Decision log #13/#14):

```
for each batch yielded by source:
    run the downstream DAG on it
      - independent branches run CONCURRENTLY on a thread pool
      - joins: executor unions inputs; nodes run in topo order within each path
      - branches share one decode cache for this batch's samples
    merge the batch's per-sample fragments into the job union/report
      - per-sample keys are DISJOINT across batches => dumb dict union, zero logic
    release the batch's decode memoization
after all batches:
    call each kernel's finalize() once, in topo order, on the merged union/report
    -> job-level summary
```

- Few batches in flight per worker (1–2): parallel branches over the *same* sample
  share one decoded copy; parallel samples would multiply decoded memory.
- **Failsafe layers:** loader enumeration failure → error sample in the initial
  fragment; kernel exception / decode failure on a sample → executor writes
  `status: error` for that sample at that node and drops it from downstream nodes;
  the job always completes and reports.

## 5. Report schema

Minimal enforced skeleton, free-form leaves:

```json
{
  "schema_version": 1,
  "job": { "pipeline": "...", "pipeline_hash": "...", "worker": "...", "started_at": "..." },
  "samples": {
    "<sample_id>": {
      "status": "pass | warn | fail | error",
      "checks": {
        "<node>/<check>": {
          "status": "pass | warn | fail | error",
          "measurement": 312.4,
          "threshold": { "max_s": 1800 },
          "details": { }
        }
      }
    }
  },
  "summary": { }
}
```

- **Verdicts belong to kernels, thresholds to YAML params.** The framework never
  judges; it only aggregates: sample status = worst of its checks
  (`error > fail > warn > pass`).
- `error` is a first-class status — unreadable data is itself a quality defect.
- `summary` is produced by `finalize()` implementations.

## 6. Demo pipelines (the reference implementations)

Both live in `pipelines/` and run against `s3://egocentric-data-delivery/`.
The two vendor deliveries there have different layouts and different capabilities, which
is why there are two loaders — see [components/loaders.md](components/loaders.md).

### 6.1 Duration probe — `pipelines/duration_egoverse_flat.yaml`

- `FlatDirLoader` (SourceKernel): one media file = one sample, stream `video/main`.
  Same code on a local path or an `s3://` prefix (decision #15).
- `VideoDurationKernel`: `video_meta` (header-only) duration per sample → check
  `duration/video_length`; per-sample artifact `duration_s/<id>`; `finalize()` → summary.
- `DataDumper` writes the finished report to the target.
- CLI: `abasift run pipeline.yaml` → runs, prints summary.

**Acceptance (pytest, self-verifiable):** ffmpeg-synthesize 3 videos of known durations
(2s/5s/10s) + 1 deliberately corrupted file → 3 samples `pass` with duration error < 0.1s;
corrupted file is `error` while the job completes normally; summary numbers correct.
Green (`test_duration_demo.py`).

**Real run:** 53 files / 15.7 GB in **21.6 s**, `pass=50 fail=3 error=0` — the three
failures are takes longer than the YAML's `max_s: 1800` (longest 3449 s). Header-only
access means ~0.5 MB read per file instead of the whole object; the integration test
asserts the disk cache stays *empty* for this pipeline.

### 6.2 IMU spike check — `pipelines/imu_spike_egoverse_dji.yaml`

The vendor ships no IMU sidecar: the inertial data is a protobuf `DJI meta` data track
**inside the MP4**. So this demo needed a vendor-specific loader plus a container decoder.

- `EgoverseDjiLoader`: one md5 directory = one sample; `video/main` (`video_meta`),
  `imu/main` (`dji_imu`, *same URI*, materialises to the disk cache), `annotation/task`.
- `ImuSpikeKernel`: robust median/MAD z-score over the per-axis first difference of the
  accelerometer → check `imu/imu_spike`; thresholds `z_thresh` / `max_spikes` /
  `warn_spikes` in YAML.
- Two branches (duration, imu) over one loader, joined at the dumper — the executor
  unions both, and both share one decode per sample.

**Real run** (4 smallest samples): 2 `pass` with 0 spikes (median ‖a‖ 0.996 / 1.006 g at
29.97 / 25.0 Hz), 2 `error` — one file is phone-recorded with no telemetry track, one is a
1.6 s clip too short to measure. The job completed and reported all four.

The reverse-engineering evidence, the validation guard against a moved field mapping, and
the frame-synchronous-rate limitation are documented in
[components/dji-telemetry.md](components/dji-telemetry.md).

## 7. Security note

`test/s3.json` contains plaintext AWS credentials. It must never be committed
(gitignore before `git init`), never referenced by framework code, and the key
should be rotated if it was ever shared. The framework reads credentials from the
standard AWS chain only.

## Appendix: Decision log

| # | Decision | Alternatives rejected |
|---|----------|----------------------|
| 1 | Scope 1: framework + demo pipeline | Building orchestration or full platform |
| 2 | Job = YAML config (loader over S3 dirs); atom flowing through DAG = **sample** | Job-per-file (kills cross-modality checks), time-window atoms |
| 3 | Batch = efficiency unit; outputs per-sample | Batch as semantic unit |
| 4 | Fully **lazy** loading via `LazyRaw` (uri + decoder) | Staged-download hybrid; eager materialization |
| 5 | Two-tier cache: disk (raw, worker-global LRU) + in-memory decode memo (per batch run) | No cache; memory-only |
| 6 | Parallelism **within** the DAG: concurrent independent branches, threads | Sequential topo walk + many samples in flight (multiplies decoded memory) |
| 7 | **Threads only**; no proc/cmd node runtimes | Per-node proc/bash escalation (serialization tax; deferred) |
| 8 | ArtifactUnion: flat node-namespaced map, **extend-only**, executor-owned union at joins | Nested trees; kernel-side merging; overwrite rights |
| 9 | Loader = **node 0** (SourceKernel); batch travels inside the ArtifactUnion | Separate job YAML for loader; special batch parameter |
| 10 | `DataDumper` = the one sanctioned mutator: dump→LazyRaw swap, empty target→delete, deterministic paths | Letting any kernel mutate; timestamped dump paths |
| 11 | **Per-batch streaming through the DAG + terminal `finalize()`** | Stage-wise (all-batches-then-propagate): chosen at first for report-merge fears, rolled back once per-sample keys proved disjoint across batches (dumb union); streaming restores decode sharing and bounded memory |
| 12 | Report: minimal enforced skeleton; kernel verdicts, YAML thresholds; `error` first-class | Free-form reports; heavyweight per-check schemas |
| 13 | Distribution is **external** (per-machine YAMLs generated by an outside system) | In-framework sharding + merge tool; central batch queue |
| 14 | fsspec for all I/O; AWS credential chain; kernel resolution by dotted import path | boto3-only; credential params in YAML; string registry |

### Decisions taken during implementation (2026-07-30)

| # | Decision | Why / alternatives rejected |
|---|----------|----------------------------|
| 15 | Package **`abasift`**, CLI `abasift`; loader named `FlatDirLoader`, not `LocalDirLoader` | Project name settled as abasift. "LocalDir" would lie: the same loader serves `s3://` prefixes unchanged, which is the point of fsspec |
| 16 | `LazyRaw` exposes **three access modes** (`open` / `local_path` / `read_bytes`), decoder picks | Measured: header probe 0.45 MB vs interleaved-track demux 177.9 MB on the same file. One mode cannot serve both. `read_bytes` refuses oversize payloads so nobody pulls a 2 GB video into RAM by accident |
| 17 | `ReportExt` is a typed object (`checks` + `summary`) instead of a bare dict | One return type for `run()` and `finalize()`; kernels can't accidentally write the enforced skeleton |
| 18 | `Batch`-valued keys are dropped when merging a batch union into the job union | Union values are meant to be primitives/`LazyRaw`; `load/batch` would otherwise arrive with a different value per batch and trip the conflict check, and would pin decoded memory |
| 19 | A node's view = the union **and report** of its *ancestors*, not global state | With a shared report, whether branch B skipped a sample branch A had just failed would depend on thread scheduling. Ancestor-scoped views make reports scheduling-invariant |
| 20 | Kernel params are strict (explicit signatures, no `**kwargs`) | A mistyped threshold silently defaulting is worse than a crash for a QC framework |
| 21 | Mutation is a separate interface: `MutatingKernel.run_mutating -> Mutation`; deletions recorded in the union so joins can't resurrect them | "Who may rewrite an artifact" becomes a type-level answer, not a convention; and in-place mutation of a union shared by concurrent branches is unsafe |
| 22 | Two-phase terminal pass: all `finalize()`, then job stats, then `finalize_mutating()` | Otherwise a dumped `report.json` is missing the job block that is written after it |
| 23 | `SampleKernel` base class owns the per-sample failsafe; failures report under the kernel's own `check_name` | Every check kernel gets drop-on-upstream-error and exception→`error` for free, and a node always contributes exactly one check key per sample |
| 24 | **Stream registry reduced** to a validated `kind/name` prefix set (`video image audio imu annotation blob`) — §1.2's "every stream type declares required fields (e.g. a common time base)" is **not** implemented | The prefix check catches the actual mistake seen in practice (a loader inventing `telemetry/main`). Declared per-kind required fields would only pay off once a sync kernel exists to consume them. **This is the one place the implementation under-delivers against the agreed design** — flagged, not hidden |
| 25 | `art.batch()` finds the `Batch` by type, not by a configured key name | A kernel never hardcodes the loader's node name; raises on missing/ambiguous instead of silently reading the wrong thing |
| 26 | Concrete dump layout `{target}/{job_id}/{node}/{name}` (design only said `f(job_id, node, key)`) | Job-scoped directory keeps a re-run's outputs together and makes an accidental cross-job overwrite impossible |
| 27 | CLI exit codes: `0` = the job completed and reported (whatever the verdicts), `2` = the YAML is broken | QC failures are data findings, not process failures — an orchestrator must not retry a job because a vendor's video was corrupt |
| 28 | Loaders take `max_samples` / `order` (`name`\|`size`) / `batch_size` | Cheap probes and cheap tests over a huge delivery. Explicitly **not** sharding: distribution stays external (#13) |
| 29 | `EgoverseDjiLoader` declares `imu/main` for every sample, even though some vendor files have no telemetry track | Knowing would require opening each object at enumeration time. "Declared but unreadable" already has a right answer (`MissingStream` → `error`), and *which files lack IMU* is a QC result, not a loader precondition |
| 30 | The reverse-engineered DJI field mapping is guarded by physics (`‖q‖≈1`, median `‖a‖∈[0.5,2] g`, monotonic timestamps) and rejects unrecognised layouts | The mapping is inferred, not specified; a firmware change that moved the fields would otherwise be reported as plausible-looking nonsense. Failing loudly beats mis-measuring |
| 31 | Spike statistics: median/MAD (not mean/std), scale floor, one impulse counts as **2** spikes, track shorter than `min_samples` → `error` not `pass` | A few large spikes inflate a standard deviation enough to hide themselves. The 2-per-impulse artefact of first differencing is documented rather than smoothed away, so the measurement stays explainable |
| 32 | Executor scheduling is a driver loop (submit-ready → wait `FIRST_COMPLETED` → fold) rather than level barriers or self-blocking pool tasks | Level barriers waste wall-clock on uneven branches; tasks that block waiting on dependencies can starve the pool. The driver loop has neither failure mode |
| 33 | Flat package layout (`abasift/` at the repo root, no `src/`), `pyflakes` as a tracked dev dependency | Requested; the linter is tracked so "imports are clean" is checkable rather than asserted |
