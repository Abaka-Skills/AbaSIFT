# AbaSift (abaka.ai data sift framework)

## Mandate

Vendors deliver egocentric datasets to `s3://egocentric-data-delivery/`, each with its own
directory layout and a mix of modalities (video, images, IMU, JSON, blobs, captions). We
build the **framework/SDK** that quality-controls them at scale across hundreds of
independent workers: the data structures, kernel interfaces, YAML pipeline format,
single-machine executor, archiver, dumper and CLI, plus reference kernels that prove the contract.

Out of scope, deliberately: work distribution (an external splitter emits one YAML per
machine) and the QC checks themselves (blur, lighting, camera shake, sync failures, sensor
corruption) — those are other teams', written against the `Kernel` interface.

**Read [doc/design.md](doc/design.md) before changing anything** — it is the agreed
contract spec and the reasoning behind it; update it when a decision changes.
[doc/components/](doc/components/) has per-component detail, [doc/test.md](doc/test.md) maps
the test suite, [doc/uml/](doc/uml/) has the framework diagrams (open `index.html`), and
[doc/log.md](doc/log.md) keeps the decision history — read it for *how* something got here,
never as the current contract.

Python package is `abasift/` at the repo root; CLI entry point is `abasift`.

## The model in five lines

- A **job** = one pipeline YAML run on one machine. Work distribution is external
  (an outside system generates per-machine YAMLs) — never build scheduling here.
- The loader is **node 0** of the DAG (`SourceKernel`); it yields Batches of
  Samples whose streams are **`LazyRaw`** handles (uri + decoder, nothing
  downloaded until `.decode()`). The batch travels inside the ArtifactUnion.
- **ArtifactUnion** is a flat `"node/name" -> value` map, **extend-only**; the
  executor owns unioning at DAG joins. The only sanctioned mutator is
  `DataArchiver` (archive → swap value for a LazyRaw; empty target → free).
- Execution: **per-batch streaming** through the DAG (independent branches run on
  threads, sharing one decode cache), dumb per-sample merge across batches, then
  each kernel's optional `digest()` once for dataset-level reduces.
- **Two documents**: `report.json` is per *sample* (`samples.<id>.checks.<node/check>`),
  `pipeline.json` is per *node* (definition, params, thresholds, tallies, summaries), and
  the source YAML is copied beside them. Kernels judge, YAML `params` hold thresholds,
  framework aggregates worst-of. `error` is first-class — a job always completes and reports.

## Hard rules

- Kernels treat inputs as **read-only** and return only extensions; never mutate
  the union or report in place (branches run concurrently).
- Failures are per-sample findings, never job crashes: decode/kernel errors →
  `status: error`, sample dropped downstream, batchmates continue.
- Dump paths are deterministic (`f(job_id, pipeline_hash, node, key)`, no timestamps) so retries
  are idempotent — whenever `target` is set. Omit `target` and each run lands under
  `dump/<job_id>_<hash>/<unix ts>/`; never hardcode an absolute path in a YAML.
- All I/O via fsspec; AWS credentials from the standard chain only — **never in
  YAML, never committed**. `test/s3.json` holds real keys: keep it gitignored.
- Parallelism is threads only. Python ≥ 3.11.

## Access modes (the thing that costs money if ignored)

Videos here are 70 MB - 2 GB. Never hold a payload in memory: `LazyRaw.open()` for
container headers (ranged GETs, ~0.5 MB per file), `LazyRaw.local_path()` when the work
genuinely spans the file (materializes once into the worker disk cache, kernel then does
plain file I/O), `read_bytes()` only for small sidecars — it refuses oversize payloads.

The scratch disk they materialize into is `$TMPDIR/abasift-cache`, 32 GiB, unless a YAML
says otherwise (`pipeline.cache: {dir, size_gb}`) or the env does (`ABASIFT_CACHE_DIR` /
`ABASIFT_CACHE_GB`) — YAML wins, per setting. A pipeline that says nothing installs
nothing. Detail: [doc/components/lazyraw-cache.md](doc/components/lazyraw-cache.md).

## Commands

- Set up / update the env: `bash setup.sh` then `conda activate abasift`.
  Dependencies live in `requirements.txt` (referenced by `environment.yml`) —
  add new ones there, not ad hoc.
- Run a pipeline: `abasift run pipelines/duration_egoverse_flat.yaml -o report.json`
  (`abasift validate <yaml>` checks the DAG without running it). A run opens with a banner:
  resolved cache root + cap, thread count, and a framed DAG — one `name[KernelClass] ←
  inputs` per row, with the edges also drawn as rails down a left gutter.
- See a pipeline: `abasift vis <yaml>` **hosts** that YAML's DAG on localhost — structure
  only, and it re-reads the YAML and re-imports edited kernels on every request, so an open
  page follows your edits. Watch a job instead with `abasift run <yaml> --vis`. Both host;
  neither writes a file — never generate a page into the repo
  ([doc/components/vis.md](doc/components/vis.md)).
- Tests (acceptance = green): `pytest` — 174 tests. `pytest -m 'not s3'` for the
  offline suite (synthesizes videos with ffmpeg), `pytest -m s3` for the vendor-bucket
  integration tests, `ABASIFT_TEST_MAX_SAMPLES=N pytest -m s3` to run them wide.
  What each file pins down: [doc/test.md](doc/test.md).

## Keeping the repo consistent

This file stays high-level; the details belong in `doc/`. When something changes:

- a design decision → the section of `doc/design.md` it belongs to (rationale goes *with*
  the rule it explains), and this file only if it changes the model or a hard rule; a new
  gap or caveat → `doc/design.md` §8. Append an entry to `doc/log.md` only when the *how it
  got here* matters — a reversal, or a choice whose rejected alternative is not obvious.
  Log entries are append-only and never renumbered;
- a component → its `doc/components/*.md`;
- a CLI flag or dependency → the Commands section here and `README.md`;
- a test file → the map in `doc/test.md`; the test count appears there and in `README.md`
  — keep both in step with what `pytest` actually reports.
