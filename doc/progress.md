# Implementation progress

Status of every component, with a pointer to its own doc. Update this file when a
component's state changes.

Verification command: `pytest` (56 tests: 52 offline + 4 against the real bucket).

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
| 14 | Tests | `test/` | this file, below | 56 passing |

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
```

Exit code 0 means *the job completed and reported* — QC verdicts inside the report are
not process failures. Exit 2 means the YAML itself is broken.

## 14. Tests

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
| `test_imu_spike.py` | spike statistics, verdict thresholds, DJI wire-format round-trip, layout validation, missing-track failsafe |
| `test_integration_s3.py` | both demos end to end on the vendor bucket; asserts *no* download for header probes and exactly one download for a shared URI |

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
