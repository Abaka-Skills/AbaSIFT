# AbaSift (abaka.ai data sift framework)

## Mandate

Vendors deliver egocentric datasets to `s3://egocentric-data-delivery/`, each with its own
directory layout and a mix of modalities (video, images, IMU, JSON, blobs, captions). We
build the **framework/SDK** that quality-controls them at scale across hundreds of
independent workers: the data structures, kernel interfaces, YAML pipeline format,
single-machine executor, dumper and CLI, plus reference kernels that prove the contract.

Out of scope, deliberately: work distribution (an external splitter emits one YAML per
machine) and the QC checks themselves (blur, lighting, camera shake, sync failures, sensor
corruption) — those are other teams', written against the `Kernel` interface.

**Read [doc/design.md](doc/design.md) before changing anything** — it is the agreed
contract spec + decision log; update it when a decision changes.
[doc/progress.md](doc/progress.md) tracks component state,
[doc/components/](doc/components/) has per-component detail, [doc/uml/](doc/uml/) has the
framework diagrams (open `index.html`).

Python package is `abasift/` at the repo root; CLI entry point is `abasift`.

## The model in five lines

- A **job** = one pipeline YAML run on one machine. Work distribution is external
  (an outside system generates per-machine YAMLs) — never build scheduling here.
- The loader is **node 0** of the DAG (`SourceKernel`); it yields Batches of
  Samples whose streams are **`LazyRaw`** handles (uri + decoder, nothing
  downloaded until `.decode()`). The batch travels inside the ArtifactUnion.
- **ArtifactUnion** is a flat `"node/name" -> value` map, **extend-only**; the
  executor owns unioning at DAG joins. The only sanctioned mutator is
  `DataDumper` (dump → swap value for a LazyRaw; empty target → delete).
- Execution: **per-batch streaming** through the DAG (independent branches run on
  threads, sharing one decode cache), dumb per-sample merge across batches, then
  each kernel's optional `finalize()` once for dataset-level reduces.
- **Report**: enforced skeleton only (`samples.<id>.checks.<node/check>.status`);
  kernels judge, YAML `params` hold thresholds, framework aggregates worst-of.
  `error` is a first-class status — a job must always complete and report.

## Hard rules

- Kernels treat inputs as **read-only** and return only extensions; never mutate
  the union or report in place (branches run concurrently).
- Failures are per-sample findings, never job crashes: decode/kernel errors →
  `status: error`, sample dropped downstream, batchmates continue.
- Dump paths are deterministic (`f(job_id, node, key)`, no timestamps) so retries
  are idempotent — whenever `target` is set. Omit `target` and you get the relative
  per-run default `dump/<unix ts>/`; never hardcode an absolute path in a YAML.
- All I/O via fsspec; AWS credentials from the standard chain only — **never in
  YAML, never committed**. `test/s3.json` holds real keys: keep it gitignored.
- Parallelism is threads only. Python ≥ 3.11.

## Access modes (the thing that costs money if ignored)

Videos here are 70 MB - 2 GB. Never hold a payload in memory: `LazyRaw.open()` for
container headers (ranged GETs, ~0.5 MB per file), `LazyRaw.local_path()` when the work
genuinely spans the file (materializes once into the worker disk cache, kernel then does
plain file I/O), `read_bytes()` only for small sidecars — it refuses oversize payloads.
Detail: [doc/components/lazyraw-cache.md](doc/components/lazyraw-cache.md).

## Commands

- Set up / update the env: `bash setup.sh` then `conda activate abasift`.
  Dependencies live in `requirements.txt` (referenced by `environment.yml`) —
  add new ones there, not ad hoc.
- Run a pipeline: `abasift run pipelines/duration_egoverse_flat.yaml -o report.json`
  (`abasift validate <yaml>` checks the DAG without running it).
- See a pipeline: `abasift vis <yaml>` **hosts** that YAML's DAG on localhost — structure
  only, and it re-reads the YAML and re-imports edited kernels on every request, so an open
  page follows your edits. Watch a job instead with `abasift run <yaml> --vis`. Both host;
  neither writes a file — never generate a page into the repo
  ([doc/components/vis.md](doc/components/vis.md)).
- Tests (acceptance = green): `pytest` — 95 tests. `pytest -m 'not s3'` for the
  offline suite (synthesizes videos with ffmpeg), `pytest -m s3` for the vendor-bucket
  integration tests, `ABASIFT_TEST_MAX_SAMPLES=N pytest -m s3` to run them wide.

## Keeping the repo consistent

This file stays high-level; the details belong in `doc/`. When something changes:

- a design decision → the decision log in `doc/design.md`, and this file only if it
  changes the model or a hard rule;
- a component → its `doc/components/*.md` plus the status table in `doc/progress.md`;
- a CLI flag or dependency → the Commands section here, `README.md`, and `doc/progress.md`;
- the test count appears in `README.md` and `doc/progress.md` — keep both in step with
  what `pytest` actually reports.
