# AbaSift — Distributed Egocentric Dataset QC Framework: Design

Status: **implemented**. This document is the contract spec: what the framework guarantees,
and why each guarantee looks the way it does. The implementation must not deviate from it
without the change being made here first. Per-component detail lives in
[components/](components/), the test map in [test.md](test.md), the diagrams in
[uml/index.html](uml/index.html). [log.md](log.md) is the historical decision log — how the
contract got here, including the entries later revisited; this document is what holds now.

| | |
|---|---|
| [0. Scope](#0-scope) | what is and is not being built |
| [Principles](#design-principles) | the load-bearing choices, in one table |
| [1. Data model](#1-data-model) | `LazyRaw`, `Sample`/`Batch`, `ArtifactUnion` |
| [2. Pipeline definition](#2-pipeline-definition) | the one YAML that describes a job |
| [3. Kernel interfaces](#3-kernel-interfaces) | the four classes, and the one mutator |
| [4. Execution semantics](#4-execution-semantics) | per-batch streaming, digest, commit |
| [5. Report schema](#5-report-schema--two-documents) | the two documents a job emits |
| [6. Demo pipelines](#6-demo-pipelines-the-reference-implementations) | what runs against the real bucket |
| [7. Security note](#7-security-note) | credentials, and where they may not be |
| [8. Known limitations](#8-known-limitations) | gaps, and which are deliberate |

## 0. Scope

**The problem.** Multiple vendors deliver egocentric datasets to our S3 bucket, each with
its own directory organisation and a mix of modalities in heterogeneous formats: video,
images, IMU, binary blobs, JSON, text, per-frame captions. They have to be quality-
controlled at scale — hundreds of workers across CPU/GPU slots and AWS instances, working
decentralised, with no central scheduler inside the framework itself.

We build a **framework/SDK**, not a platform:

- The abstractions: `LazyRaw`, `Sample`/`Batch`, `ArtifactUnion`, `Report`,
  `Kernel`/`SourceKernel`, pipeline YAML, single-machine `Executor`, `DataArchiver`, CLI,
  and hosted views of a pipeline and of a running job.
- Reference kernels that prove the contract rather than cover the checks: two checks
  (`VideoDurationKernel`, `ImuSpikeKernel`), one shared decode (`VideoFrameKernel`)
  and one writer (`VideoDumper`).
- Two **demo pipelines**, both running against the real vendor bucket:
  video duration probe, and an IMU spike check over telemetry embedded in the MP4.
- **Out of scope:** work distribution. An external system generates one job YAML per
  machine (e.g. by splitting S3 prefixes); our contract is *"given one YAML, one
  machine runs it to completion and emits the report + artifacts for that YAML's
  scope."* Cross-machine report merging is also external.
- **Out of scope:** the checks themselves. Other teams write them against the `Kernel`
  interface; the framework must be able to carry, at minimum, visual artifacts
  (compression, banding, corruption), motion blur obscuring hands/tools/workpiece,
  excessive camera shake, poor lighting (heavy shadow or blow-out over the work surface),
  synchronisation failures across camera/IMU/VIO streams, and sensor corruption (dead
  pixels, IMU spikes or frozen values, broken streams). `ImuSpikeKernel` is the worked
  example of the last one.

Language: Python ≥ 3.11. Parallelism: **threads only** (heavy libs — PyAV, numpy,
torch, I/O — release the GIL; design transfers unchanged to free-threaded CPython later).

## Design principles

The choices everything else follows from, each against the alternative it displaced.

| Principle | Instead of | Because |
|---|---|---|
| The atom flowing through the DAG is a **sample**; a job is one YAML | job-per-file; time-window atoms | cross-modality checks need every stream of one take in one place |
| **Batch is an efficiency unit only** | batch as a semantic unit | a failed sample must fail alone, and reports stay keyed per sample |
| Loading is **lazy** — `LazyRaw` is uri + decoder | staged download; eager materialization | a pipeline that only checks IMU must never pay for video bytes |
| **Two-tier cache**: worker-global disk LRU + per-run in-memory decode memo | no cache; memory-only | bytes are shared across batches, decoded objects must not outlive one |
| Parallelism **inside** the DAG, threads only | sequential walk with many samples in flight | branches over one sample share one decoded copy; parallel samples multiply memory |
| `ArtifactUnion` is flat, node-namespaced, **extend-only** | nested trees; kernel-side merging; overwrite rights | collisions become impossible and runs deterministic regardless of scheduling |
| Loader is **node 0**; the batch travels inside the union | a separate loader config; a special batch parameter | one uniform kernel signature, and the DAG shape is the whole job |
| Exactly **one sanctioned mutator** (`DataArchiver`) | any kernel may rewrite an artifact | "who may rewrite this" must be answerable by `isinstance`, not by convention |
| **Per-batch streaming** through the DAG, then a terminal reduce | stage-wise, all batches per node | keeps memory bounded and preserves decode sharing; per-sample keys are disjoint across batches, so the merge stays trivial |
| Kernels judge, **YAML params hold thresholds**, the framework only aggregates | verdict logic in the framework; free-form reports | same kernel, per-vendor strictness, no code change |
| `error` is a **first-class status**; a job always completes and reports | exceptions escaping to the worker | unreadable data is itself a quality defect, and a crashed shard reports nothing |
| Distribution is **external** | in-framework sharding and merge | one machine, one YAML, one report is a contract the rest of the system can build on |
| **fsspec** everywhere, credentials from the AWS chain, kernels by dotted import path | boto3-only; credentials in YAML; a string registry | local and `s3://` behave identically, and nothing secret has a place to hide |

## 1. Data model

### 1.1 LazyRaw — the lazy handle

```python
LazyRaw(uri: str, decoder: DecoderRef)   # fully serializable: uri + decoder name
lazyraw.decode() -> Any                  # bytes / ndarray / av container / ...
```

- Nothing is downloaded at load time. A pipeline that only checks IMU never touches
  video bytes.
- **Three access modes, chosen per decoder:** `open()` = remote seekable file object,
  ranged GETs only; `local_path()` = stream the object into the disk cache once and work on
  a file; `read_bytes(max_bytes)` = whole object in memory, *refusing* oversize payloads.
  One mode cannot serve both ends. Measured on a 177.9 MB DJI MP4: a container-header probe
  reads 0.45 MB (0.25%), while demuxing the interleaved telemetry track over the network
  reads 177.90 MB in 4406 ranged GETs — so headers stay remote and interleaved tracks
  materialise. See [components/lazyraw-cache.md](components/lazyraw-cache.md).
- **Two-tier cache:** raw bytes cached on a worker-global disk scratch dir keyed by
  URI (size-capped LRU) — the S3→disk bridge; decoded object memoized in-memory on
  the instance, **released when its batch's DAG run completes** — the disk→memory
  bridge. An internal lock prevents concurrent double-decode from parallel DAG branches.
- I/O goes through **fsspec**: `s3://…` and local paths behave identically.
  Credentials come from the standard AWS chain (env, `~/.aws/credentials`) —
  **never from YAML or checked-in files**.
- A `decode()` failure inside a kernel is a **QC finding** (`status: error` for that
  sample at that node), not a worker crash.

### 1.2 Sample and Batch

- **Sample** = metadata header + dict of **canonical named streams**
  (`video/cam_front`, `imu/left`, `captions`, …) whose values are `LazyRaw`s.
  The per-vendor dataloader's whole job is normalizing vendor directory layout into
  these canonical names + URIs + decoders. Stream naming is framework-owned: a validated
  `kind/name` prefix set, which catches the mistake actually seen in practice — a loader
  inventing `telemetry/main`. Per-*kind* declared fields (e.g. a common time base for sync
  kernels) are **not** implemented; see §8.
- **Batch** = list of Samples. Batch is an **efficiency unit only** (GPU
  vectorization, prefetch); report and artifacts stay keyed per-sample. A failed
  sample fails alone; its batchmates continue.

### 1.3 ArtifactUnion

- Flat map `"{node_name}/{artifact_name}" -> value`, per job. Values are
  serializable primitives or `LazyRaw`/file references — never live Python objects.
- **Extend-only (write-once):** a node adds keys under its own namespace, never
  overwrites. Collisions are impossible by construction; runs are deterministic
  regardless of branch scheduling. Every intermediate survives to the end, so the
  archiver can select any node's output.
- **Union at joins is executor-owned:** key-wise dict union. Duplicate keys arriving
  via two ingress edges (diamond topology) are identical by construction and merge
  silently; a genuine value conflict is an executor error, never a kernel concern.
- **The loaded Batch itself travels inside the ArtifactUnion** under the loader
  node's namespace (e.g. `load/batch`). One uniform kernel signature; no
  special-cased batch parameter. `Batch`-valued keys are dropped again before the batch's
  union folds into the job's: the union carries facts that outlive a batch, and a live
  batch would otherwise arrive with a different value each time *and* pin its decoded
  payloads for the rest of the job.
- Two lookups keep kernels off the key convention: `art.batch()` finds the batch **by
  type**, and `art.find_lazy(sample_id, decoder)` finds another node's per-sample payload
  **by decoder**. Neither hardcodes a node name the YAML chose; both raise on missing or
  ambiguous rather than silently reading the wrong thing.
- A deletion by the archiver is **sticky**: recorded in the union so a join with a branch
  that never saw it cannot resurrect the key.

## 2. Pipeline definition

One YAML fully describes a job (no separate loader/job config):

```yaml
pipeline:
  job_id: basic_video_qc                          # the one name a job has
  cache: { dir: /scratch/abasift, size_gb: 200 }   # optional worker scratch
  nodes:
    - name: load                                # unique node name = artifact namespace
      kernel: abasift.loaders.FlatDirLoader     # dotted import path, fails fast at load
      params: { root: s3://bucket/vendor_a, batch_size: 8 }
      inputs: []                                # [] + SourceKernel => source node
    - name: duration
      kernel: abasift.kernels.VideoDurationKernel
      params: { max_s: 1800 }
      inputs: [load]
    - name: archive_report
      kernel: abasift.kernels.DataArchiver
      params: { keys: ["__report__", "__pipeline__"], target: /data/out/vendor_a }
      inputs: [duration]
```

- Edges = `inputs:` lists. Executor topo-sorts and validates (acyclic, unique names,
  importable kernels, exactly one source) at load time.
- Kernel **params are strict** — kernels declare parameters explicitly, no `**kwargs`
  catch-all. For a QC framework a mistyped threshold silently defaulting is worse than a
  crash, so a bad param is a load-time `PipelineError`.
- Kernel resolution = **dotted import path** (no registry; registered aliases can be
  layered on later without breaking anything).
- Thresholds/severity live in `params` — same kernel, per-vendor strictness, no code
  changes.
- `job_id` is the **one name a job has**: dump paths use it, and the definition itself is
  identified by its hash. Two free-text labels for one thing drift.
- `cache:` is the only **infrastructure** a YAML may set (scratch `dir` and `size_gb`,
  both optional and strict). It fits the "one YAML fully describes a job" rule: the
  splitter that knows a machine's disk can say so, while the environment does not always.
  YAML wins per setting, so `size_gb` alone still honours `ABASIFT_CACHE_DIR`, and a YAML
  that says nothing installs *nothing* — the cache is process-global, and overwriting it
  would trample one a test fixture or an embedding program installed on purpose.
  Credentials remain out of bounds (§7).

## 3. Kernel interfaces

Four classes, three of which a QC author might subclass. Inputs are **read-only**; kernels
return only extensions, and the executor merges and namespaces them.

```python
class Kernel:
    def run(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]:
        """Called once per batch."""
    def digest(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt] | None:
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
    def sift(self, sample: Sample, art: ArtifactUnion) -> dict[str, Check] | tuple[dict, dict]: ...

class MutatingKernel(Kernel):
    """Framework-internal, and there is exactly one (DataArchiver). The only class that
    may replace or delete an existing union key."""
    def run_mutating(self, art, report) -> Mutation:          # per batch
    def commit(self, art, report) -> Mutation | None:         # once, after the job stats
    def replaced_key_patterns(self) -> tuple[str, ...]:       # for the validator
```

`ArtifactExt` is `{name: value}` (executor namespaces it). `ReportExt` is a typed object
carrying `checks: {sample_id: {check_name: Check}}` and `summary: dict` — one return type
serves both `run()` and `digest()`, and unlike a bare dict it gives a kernel no way to write
into the enforced report skeleton by accident. `report` is a read-only `ReportView` whose
load-bearing method is `is_alive(sample_id)`.

`SampleKernel` exists so that drop-on-upstream-error and exception→`error` are implemented
once rather than in every check: a node contributes exactly one check key per sample whether
its kernel succeeded or not. Its hook is named `sift` rather than `check` because it decodes,
measures, derives artifacts and *then* judges.

The two terminal hooks are **not** the same kind of thing, which is why they have unrelated
names: `digest()` reduces a job's per-sample facts into dataset-level summaries and touches
nothing; `commit()` writes the finished job out, after its counts are already stamped, and is
the last chance anything has to change the union. Neither *merges* batches — the executor has
already done that.

### 3.1 DataArchiver — the one sanctioned mutation

Ordinary kernels are extend-only. The framework-owned `DataArchiver` may additionally:

- **Archive:** for each key matching its configured globs, write the binary to the
  target (local or `s3://`), then **replace the value in-place with a `LazyRaw`**
  pointing at the written file. Downstream *readers* are oblivious (`.decode()` works
  either way). Memory freed, information preserved. A **reduce** is not a reader: archive
  intermediates, never a key some node reduces in `digest()`.
- **Delete (empty/null target):** remove the key and its local backing file — for
  intermediates nobody downstream reads. Free mode requires an explicit empty `target`, so
  the destructive branch is never the default.
- **Deterministic dump paths:** an explicitly configured `target` gives
  `uri = f(job_id, pipeline_hash, node, key)` — no timestamps — so a retried shard, which
  reuses its id by design, overwrites idempotently. The hash is in the path because an
  *edited* YAML retried under the same id would otherwise overwrite artifacts produced by
  different thresholds, destroying the evidence behind a verdict someone has already read.
  When `target` is omitted the default is the *relative*, per-run
  `dump/{job_id}_{hash}/<unix ts>/{node}/`: an interactive run must not collide with its own
  earlier output, paid for in directories that accumulate until someone clears them. The
  stamp sits *inside* the job directory so a shard's history lists without globbing, and it
  is read once from the job start time so every writer in a DAG agrees. Anything generated by
  the external splitter sets `target` and keeps the idempotency guarantee.
- **Writing is not archiving.** A kernel that writes something out without touching the
  union is an ordinary `SampleKernel` — a *dumper*. Archive = write the value out *and* take
  the object out of the working set; dump = write something out and leave the union as found.
  So an archiver is a `MutatingKernel` and there is exactly one; a dumper is a `SampleKernel`
  and there can be many. What they genuinely share — where a write lands — lives in
  `kernels/_dump.py`. The output directory keeps the name `dump/`: it is a place both write
  to, not a claim about the union.

Archive/free points are explicit YAML nodes, visible in the graph. Pipeline authors must
place them downstream of everything that reads the keys they take away — a documented
pattern, not an optimization, and not something the framework guesses. Two nodes replacing
one key on parallel branches has no honest winner, so it is rejected at **load** time: the
mistake is visible in the YAML, and mid-job is after both files have already been written.
The validator asks each mutating kernel for its `replaced_key_patterns()` rather than knowing
what a `DataArchiver` is, and tests glob overlap conservatively — general glob intersection is
not decidable by inspection, and a validator that guessed would be worse than one that states
what it checked.

## 4. Execution semantics

**Per-batch streaming, then digest, then commit:**

```
for each batch yielded by the source:
    run the downstream DAG on it
      - independent branches run CONCURRENTLY on a thread pool
      - a node's view = the union and report of its ANCESTORS, never global state
      - branches over one sample share one decoded copy (LazyRaw memo)
    merge the LEAF nodes' unions and reports into the job's
      - a leaf already holds every ancestor's, and holds it post-mutation
      - per-sample keys are DISJOINT across batches => dumb dict union, zero logic
    release the batch's decode memoization
after all batches:
    digest() per kernel, in topo order, over the merged union/report -> summaries
    stamp the job block (counts, n_samples, elapsed)
    commit() for the archivers -> the finished documents are written last
```

- One batch in flight per worker: parallel branches over the *same* sample share one
  decoded copy; parallel samples would multiply decoded memory instead. Batch size is
  therefore a memory dial, not a throughput one.
- Scheduling is a driver loop (submit every ready node → wait `FIRST_COMPLETED` → fold),
  so no worker thread ever blocks on another node. Level barriers would waste wall-clock on
  uneven branches; pool tasks that block waiting on dependencies can starve the pool.
- **A node's view is its ancestors', not global state.** Kernels skip samples that already
  failed upstream; against a shared, concurrently updated report, whether branch B skipped a
  sample branch A had just failed would depend on thread scheduling, and two runs of one job
  could disagree. Ancestor-scoped views make the report scheduling-invariant.
- **Only the leaves fold back.** Every node's union already contains its ancestors', so
  folding the whole graph in is redundant — and wrong once an archiver has *replaced* a
  value, because the ancestor still holds the pre-mutation copy and the merge then sees one
  key with two values. That happens on a linear DAG, so "forbid mutation on branches" was
  never the fix. Sibling leaves can still disagree, which is what the sticky `deleted` set
  settles.
- **The terminal pass is two-phase** because an archived `report.json` has to contain the
  job block — counts, elapsed — which is only known once every `digest()` has run.
- **Failsafe layers**, outermost first: the source blowing up mid-enumeration is recorded
  in the summary and the job still reports; a node raising outright marks every live sample
  `error` *at that node*; a kernel raising on one sample marks that sample and lets its
  batchmates continue. Nothing propagates out of `Executor.run()` — **except** an exception
  from `digest()`, which is the one remaining gap (§8).
- The executor takes an optional `observer(event, **payload)` and emits six events, which is
  how the live view gets progress the report cannot give it (the report only exists once the
  job ends). The dependency points the right way — the executor imports nothing from `vis`
  and does not know anything is watching — and `_emit` swallows whatever an observer raises,
  because telemetry must never be load-bearing.

## 5. Report schema — two documents

A job emits **two** JSON documents, split by what a fact is *about*. Thresholds and params
are identical for every sample a node judged, so repeating them per sample would be most of
the file on a 100k-sample delivery. Both carry the same `job` block, so either can be read
alone; neither contains the exact blob the hash covers, which is acceptable because the
verbatim YAML sits at the job root and `job.pipeline_hash` is in both.

**`report.json` — one entry per data sample.** Nothing here is the same for every sample:

```json
{
  "schema_version": 2,
  "job": { "pipeline_hash": "...", "job_id": "...", "worker": "...",
           "started_at": "...", "counts": { }, "n_samples": 0 },
  "samples": {
    "<sample_id>": {
      "status": "pass | warn | fail | error",
      "checks": {
        "<node>/<check>": {
          "status": "pass | warn | fail | error",
          "measurement": 312.4,
          "details": { }
        }
      }
    }
  }
}
```

**`pipeline.json` — one entry per node.** The definition that ran, and how it did:

```json
{
  "schema_version": 2,
  "job": { },
  "pipeline": { "job_id": "...", "cache": { } },
  "nodes": [
    { "name": "duration", "kernel": "...", "params": { }, "inputs": ["load"],
      "counts": { "pass": 50, "fail": 3 },
      "checks": { "<check>": { "counts": { "pass": 50, "fail": 3 },
                               "threshold": { "max_s": 1800 } } },
      "summary": { } }
  ]
}
```

- **Verdicts belong to kernels, thresholds to YAML params.** The framework never
  judges; it only aggregates: sample status = worst of its checks
  (`error > fail > warn > pass`).
- `error` is a first-class status — unreadable data is itself a quality defect.
- `summary` is produced by `digest()` implementations, and is dataset-level, so it
  sits in the pipeline document rather than the per-sample one.
- A `Check` still *declares* its `threshold` — that is part of the judging contract — but
  it is serialised once per node instead of once per sample.

## 6. Demo pipelines (the reference implementations)

Both live in `pipelines/` and run against `s3://egocentric-data-delivery/`.
The two vendor deliveries there have different layouts and different capabilities, which
is why there are two loaders — see [components/loaders.md](components/loaders.md). Loaders
take `max_samples` / `order` (`name` | `size`) / `batch_size` so a probe or a test can stay
cheap over a huge delivery; that is explicitly **not** sharding, which stays external.

### 6.1 Duration probe — `pipelines/duration_egoverse_flat.yaml`

- `FlatDirLoader` (SourceKernel): one media file = one sample, stream `video/main`.
  Same code on a local path or an `s3://` prefix — which is why it is not called
  `LocalDirLoader`.
- `VideoDurationKernel`: `video_meta` (header-only) duration per sample → check
  `duration/video_length`; per-sample artifact `duration_s/<id>`; `digest()` → summary.
- `DataArchiver` writes the finished report to the target.
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
  It declares `imu/main` for every sample, telemetry or not: knowing which files carry it
  would mean opening every object at enumeration time, "declared but unreadable" already has
  a right answer (`MissingStream` → `error`), and *which* files lack IMU is a QC result, not
  a loader precondition.
- `ImuSpikeKernel`: robust median/MAD z-score over the per-axis first difference of the
  accelerometer → check `imu/imu_spike`; thresholds `z_thresh` / `max_spikes` /
  `warn_spikes` in YAML. Mean/std would hide what it is meant to find — a handful of large
  spikes inflates σ enough to conceal themselves. First differencing makes one impulse
  perturb two consecutive differences, so it counts as 2: documented rather than smoothed
  away, so the measurement stays explainable. A track shorter than `min_samples` is `error`,
  not `pass` — unmeasurable is not clean.
- Two branches (duration, imu) over one loader, joined at the archiver — the executor
  unions both, and both share one decode per sample.

**Real run** (4 smallest samples): 2 `pass` with 0 spikes (median ‖a‖ 0.996 / 1.006 g at
29.97 / 25.0 Hz), 2 `error` — one file is phone-recorded with no telemetry track, one is a
1.6 s clip too short to measure. The job completed and reported all four.

The DJI field mapping is reverse-engineered, not specified, so it is guarded by physics
(`‖q‖≈1`, median `‖a‖∈[0.5,2] g`, monotonic timestamps) and unrecognised layouts are
rejected: a firmware change that moved the fields fails loudly instead of reporting
plausible-looking nonsense. Evidence and the frame-synchronous-rate limitation are in
[components/dji-telemetry.md](components/dji-telemetry.md).

### 6.3 Shared decode and exhibits

Two reference kernels exist to show what belongs in the DAG rather than inside a check.
`VideoFrameKernel` is a shared frame-decode **node**: four pixel-level checks over one video
would otherwise decode it four times, and every check author would re-derive "frames at
1 fps". A node rather than a decoder, because the sampling policy belongs in the YAML beside
the thresholds and the result is then an artifact the DAG can archive, free or reuse. It
yields a **handle**, not an ndarray, because the job union accumulates every batch and only
`Batch` values are dropped — an array parked there would live for the whole job. It keeps
**every frame unless the YAML says otherwise**: a subsampled stack cannot show a defect that
fell between its frames, and a check reading one cannot tell that is why it saw nothing, so
the lossy answer is the one a pipeline has to ask for. The cost moves to disk and is real —
raw rgb24 even at 1 fps is larger than the compressed source (159 MB of clips → 381 MB of
frames) — so it is bounded by `pipeline.cache.size_gb` and reported as `cache_bytes`. Its
cache key is `f(source, fps, size)` — the same determinism rule as dump paths.

`VideoDumper` writes a frame stack back out as a video, because a verdict a vendor will
dispute is worth an exhibit beside the report that explains it. Playback `fps` is separate
from sampling rate since both readings are legitimate — a 1 fps stack played at 1 fps is a
real-time proxy, the same stack at 25 fps is a timelapse — and it is a `Fraction`, so 29.97
stays 30000/1001. It stays an ordinary `SampleKernel`: it adds an artifact and rewrites
nothing, and it finds its input stack by *decoder*, not by node name.

### 6.4 What the CLI guarantees

- **Exit codes:** 0 = the job reported, 2 = the YAML is broken. QC failures are data
  findings, not process failures — an orchestrator must not retry a job because a vendor's
  video was corrupt, so any set of verdicts still exits 0.
- **A run opens with a banner:** the resolved cache root and cap, the thread count, and the
  DAG. A worker log should answer "where is this writing, how much disk may it eat, what is
  it about to run" without anyone opening the YAML, and it is read off the objects the job
  will actually use, so it cannot promise one thing and do another. Edges are both named
  (`← inputs`) and drawn as rails down a left gutter: *layout* of an arbitrary DAG has no
  honest ASCII form, but with nodes fixed one per row in dependency order, *routing* is a
  lane allocation and is exact. Both come from the same `inputs` in one pass, so there is no
  second copy to keep in step.
- **A run closes with the verdicts, per node.** The job line says *how many* samples failed;
  the block above it says *where*, naming each node's checks and how each one went, then
  whatever its `digest()` reduced. It is rendered from the pipeline document the job is
  about to write, not recomputed, so the terminal and the JSON cannot disagree. Verdicts
  only — thresholds are configuration and are already in the YAML and that document — and a
  verdict is tallied once, since a node's own tally is the worst-of across its checks and
  would merely repeat the single-check case. A count of zero stays dim: `error=0` in alarm
  red is a lie told in colour.
- **Colour only on a TTY, never under `NO_COLOR`, never on the message.** These logs are
  grepped by a fleet as often as they are read by a person. Levels are padded *before*
  painting, or the escape bytes count toward the width and knock every coloured line out of
  its column.
- **The two views host, never generate a file.** A generated page is a snapshot that is
  wrong the moment the YAML changes with nothing to say so; hosting re-reads the YAML and
  re-imports edited kernels on every request. The boxes are derived by `inspect`ing the
  classes the YAML names rather than transcribed, so a renamed parameter changes the picture
  or the YAML visibly fails to load. Hot reload is scoped to the kernels a YAML names — never
  the core contracts, since a second `Kernel` class would break every `issubclass` check in
  the validator.

## 7. Security note

`test/s3.json` contains plaintext AWS credentials. It must never be committed
(gitignore before `git init`), never referenced by framework code, and the key
should be rotated if it was ever shared. The framework reads credentials from the
standard AWS chain only.

## 8. Known limitations

What the framework does not do today, and whether that is a gap or a decision.

- **The stream registry is only a `kind/name` prefix check.** The agreed design had every
  stream kind declaring required fields — e.g. a common time base, for sync kernels. Declared
  per-kind fields pay off only once a sync kernel exists to consume them, so this is the one
  place the implementation under-delivers against the design: flagged, not hidden.
- The DJI `djmd` telemetry is **frame-synchronous** (one record per video frame, so
  25-60 Hz depending on the take). Spikes shorter than one frame interval are invisible.
  Higher-rate inertial data may live in the undocumented `dbgi` track; not parsed. See
  [components/dji-telemetry.md](components/dji-telemetry.md).
- Frozen/dead-sensor detection is a separate defect class from spikes and is not
  implemented (it would be a sibling kernel, not a change to `ImuSpikeKernel`).
- `digest()` runs in a single thread. Dataset-level reduces over ~10^5 samples are fine
  (dict arithmetic), but a heavy reduce would want its own parallelism.
- **Archiving a key some node reduces in `digest()` raises**, and deliberately so: the
  reduce gets a `LazyRaw` and `sum()` over handles is a category error, not something the
  framework decodes around. Archive intermediates.
- `digest()` is not wrapped in the per-node failsafe, so a kernel that raises there kills
  the job instead of reporting. Deliberate for now — the reduce-over-handles case above is
  the only known trigger and it should be loud — but it is the one gap left in "a job always
  completes and reports" (§4).
