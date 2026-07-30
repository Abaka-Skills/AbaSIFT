# AbaSift (abaka.ai data sift framework)

Distributed QC framework for egocentric vendor datasets on S3. **Read
[doc/design.md](doc/design.md) before changing anything** — it is the agreed
contract spec + decision log; update it when a decision changes.
[doc/uml.md](doc/uml.md) has the diagrams, [doc/progress.md](doc/progress.md)
tracks component state, [doc/components/](doc/components/) has per-component detail.

Python package is `abasift` under `src/`; CLI entry point is `abasift`.

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
  are idempotent.
- All I/O via fsspec; AWS credentials from the standard chain only — **never in
  YAML, never committed**. `test/s3.json` holds real keys: keep it gitignored.
- Parallelism is threads only. Python ≥ 3.11.

## Commands

- Set up / update the env: `bash setup.sh` then `conda activate abasift`.
  Dependencies live in `requirements.txt` (referenced by `environment.yml`) —
  add new ones there, not ad hoc.
- Run a pipeline: `abasift run pipelines/duration_egoverse_flat.yaml -o report.json`
  (`abasift validate <yaml>` checks the DAG without running it)
- Tests (acceptance = green): `pytest` — 56 tests. `pytest -m 'not s3'` for the
  offline suite (synthesizes videos with ffmpeg), `pytest -m s3` for the vendor-bucket
  integration tests, `ABASIFT_TEST_MAX_SAMPLES=N pytest -m s3` to run them wide.

## Access-mode rule (the thing that costs money if ignored)

Videos here are 70 MB - 2 GB. Never hold a payload in memory: `LazyRaw.open()` for
container headers (ranged GETs, ~0.5 MB per file), `LazyRaw.local_path()` when the work
genuinely spans the file (materializes once into the worker disk cache, kernel then does
plain file I/O), `read_bytes()` only for small sidecars — it refuses oversize payloads.

## others
- update the CLAUDE.md if there is any major design update.