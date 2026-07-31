# Implementation progress

Status of every component, with a pointer to its own doc. Update this file when a
component's state changes.

Verification: `pytest` (95 tests: 91 offline + 4 against the real bucket) and
`python -m pyflakes abasift test` (clean — no unused or shadowed imports).

## Repo layout

```
abasift/            the package, flat at the repo root (no src/ indirection)
  ├─ lazy.py cache.py decoders.py payloads.py     data access
  ├─ data.py report.py kernel.py errors.py        core contracts
  ├─ pipeline.py executor.py cli.py               orchestration
  ├─ kernels/       duration.py imu_spike.py dumper.py
  ├─ loaders/       flat_dir.py egoverse.py _fs.py (shared listing helpers)
  ├─ vendor/        dji_telemetry.py — the only vendor-format-specific module
  └─ vis/           server.py model.py live.py render.py assets/ — hosts a view of a YAML,
                    and of a job while it runs (`run --vis`)
pipelines/          runnable demo YAMLs
test/               offline suite + s3-marked integration suite
doc/                design.md, progress.md, components/, uml/ (index.html + mermaid)
```

Import discipline: modules import *inwards* only — `kernels/` and `loaders/` depend on the
core (`data`, `report`, `kernel`, `lazy`), never on each other or on the executor. Vendor
format knowledge is confined to `vendor/`, reached only through a registered decoder, so
adding a vendor touches `loaders/` + `vendor/` and nothing else. `vis/` hangs off the side:
it imports `pipeline.py` to `inspect` what a YAML names, and nothing imports `vis/`.

| # | Component | Module | Doc | State |
|---|-----------|--------|-----|-------|
| 1 | Environment / deps | `environment.yml`, `requirements.txt`, `setup.sh` | this file, below | done |
| 2 | `LazyRaw` + two-tier cache | `lazy.py`, `cache.py`, `decoders.py` | [lazyraw-cache.md](components/lazyraw-cache.md) | done |
| 3 | `Sample` / `Batch` / `ArtifactUnion` | `data.py` | [artifact-union.md](components/artifact-union.md) | done |
| 4 | Report + status algebra | `report.py` | [report.md](components/report.md) | done |
| 5 | Kernel interfaces | `kernel.py` | [kernels.md](components/kernels.md) | done |
| 6 | Pipeline YAML + validation | `pipeline.py` | [executor.md](components/executor.md) | done |
| 7 | Executor | `executor.py` | [executor.md](components/executor.md) | done |
| 8 | `DataDumper` | `kernels/dumper.py` | [dumper.md](components/dumper.md) | done |
| 9 | CLI | `cli.py` | this file, below | done |
| 10 | Loaders (2 vendors) | `loaders/` | [loaders.md](components/loaders.md) | done |
| 11 | DJI telemetry reader | `vendor/dji_telemetry.py` | [dji-telemetry.md](components/dji-telemetry.md) | done |
| 12 | Demo 1 — duration probe | `kernels/duration.py` | [kernels.md](components/kernels.md) | done, runs on S3 |
| 13 | Demo 2 — IMU spike | `kernels/imu_spike.py` | [kernels.md](components/kernels.md) | done, runs on S3 |
| 14 | Pipeline visualiser | `vis/` | [vis.md](components/vis.md) | done |
| 15 | Tests | `test/` | this file, below | 95 passing |

## 1. Environment

Conda, because PyAV and ffmpeg are easier to pin as conda packages than as wheels:

```bash
bash setup.sh            # creates/updates env `abasift`, installs the package editable
conda activate abasift
pytest
```

`requirements.txt` is the single source of truth for pip-installable deps and is
referenced from `environment.yml`, so the two can't drift. Runtime deps: `fsspec`,
`s3fs`, `av`, `numpy`, `PyYAML`. Test dep: `pytest`. Conda additionally provides `ffmpeg`
(the offline tests synthesize clips with it).

## 9. CLI

```bash
abasift validate pipelines/duration_egoverse_flat.yaml     # load + validate, run nothing
abasift run <pipeline.yaml> [-o report.json] [--job-id X] [--max-workers N] [-v]
abasift vis <pipeline.yaml> [--host H] [--port N]                    # what the pipeline is
abasift run <pipeline.yaml> --vis [--vis-port N]                     # what the job is doing
```

Both host (default `http://127.0.0.1:8765`) rather than writing a file. `vis` holds no
rendered output at all — it re-reads the YAML, re-imports edited kernels and re-describes
on every request, so an open page follows the working tree. `run --vis` hands the executor
a progress observer and serves the graph filling in as the job runs —
[components/vis.md](components/vis.md).

Exit code 0 means *the job completed and reported* — QC verdicts inside the report are
not process failures. Exit 2 means the YAML itself is broken.

## 15. Tests

```bash
pytest                       # everything (needs test/s3.json for the 4 s3 tests)
pytest -m 'not s3'           # offline only, no credentials needed
pytest -m s3                 # integration only
pytest -m 's3 and not slow'  # header probes only — downloads nothing
ABASIFT_TEST_MAX_SAMPLES=50 pytest -m s3 -s     # run wide over the real delivery
```

| file | what it pins down |
|------|-------------------|
| `test_core.py` | union extend-only/diamond/delete semantics, report aggregation, pipeline validation, `LazyRaw` value semantics + memo |
| `test_executor.py` | decode sharing across parallel branches, joins, the three failsafe layers, deterministic dump paths |
| `test_duration_demo.py` | the acceptance test from design §6 (2/5/10 s + corrupt), thresholds-live-in-YAML, CLI |
| `test_loaders.py` | enumeration findings, ordering, `recursive`/`patterns`, and the shared `batch_stream` grouping rule |
| `test_imu_spike.py` | spike statistics, verdict thresholds, DJI wire-format round-trip, layout validation, missing-track failsafe |
| `test_integration_s3.py` | both demos end to end on the vendor bucket; asserts *no* download for header probes and exactly one download for a shared URI |
| `test_vis.py` | that the pipeline view is *derived* (roles and columns from the DAG, signatures and defaults from the live classes) and *hosted* (editing the YAML or a kernel moves the state token and changes the served page; a broken YAML is shown, not fatal); and that `run --vis` works off executor events — an observer that raises cannot fail a job, and `RunView` never re-reads the YAML mid-run |

The integration tests default to 3 (duration) / 2 (IMU) samples and take the smallest
files first (`order: size`), so a full run costs a few seconds. Raise
`ABASIFT_TEST_MAX_SAMPLES` to sweep the delivery.

The only path with no offline coverage is demuxing a *real* MP4 telemetry track — it
needs a real DJI file, so it lives in the `s3` suite. The wire-format parser itself is
covered offline by re-encoding a packet and round-tripping it.

## Known limitations

- The DJI `djmd` telemetry is **frame-synchronous** (one record per video frame, so
  25-60 Hz depending on the take). Spikes shorter than one frame interval are invisible.
  Higher-rate inertial data may live in the undocumented `dbgi` track; not parsed. See
  [dji-telemetry.md](components/dji-telemetry.md).
- Frozen/dead-sensor detection is a separate defect class from spikes and is not
  implemented (it would be a sibling kernel, not a change to `ImuSpikeKernel`).
- `finalize()` runs in a single thread. Dataset-level reduces over ~10^5 samples are fine
  (dict arithmetic), but a heavy reduce would want its own parallelism.
