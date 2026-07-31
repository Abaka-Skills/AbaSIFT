# AbaSift

Distributed quality-control framework for egocentric vendor datasets on S3.

A **job** is one pipeline YAML run on one machine. Work distribution is external: an
outside system generates one YAML per machine; this framework's contract is *"given one
YAML, one machine runs it to completion and emits the report + artifacts for that YAML's
scope."*

```bash
bash setup.sh && conda activate abasift
pytest                                                   # 56 tests
abasift run pipelines/duration_egoverse_flat.yaml -o report.json
```

## What it does

A QC pipeline is a DAG of kernels defined in YAML. Node 0 is a per-vendor loader that
normalises the vendor's directory layout into canonical named streams (`video/main`,
`imu/main`, …) of **lazy handles** — nothing is downloaded until a kernel asks. Each
downstream kernel takes a read-only `ArtifactUnion` + report and returns *extensions*; the
executor owns merging at joins, runs independent branches on threads, and aggregates a
JSON report whose skeleton is enforced and whose leaves are free-form.

Kernels judge, YAML holds thresholds, the framework only aggregates (`error > fail > warn >
pass`). A job always completes and always reports: a corrupt file is a finding, not a crash.

## Two demos, both on the real vendor bucket

| demo | pipeline | result |
|------|----------|--------|
| duration probe | `pipelines/duration_egoverse_flat.yaml` | 53 files / 15.7 GB in 21.6 s, reading ~0.5 MB per file (container header only, no download) |
| IMU spike check | `pipelines/imu_spike_egoverse_dji.yaml` | IMU extracted from a protobuf telemetry track *inside* the MP4; robust median/MAD spike scan; missing-telemetry files reported as `error` while the job completes |

## Docs

- [doc/design.md](doc/design.md) — contract spec + decision log (read this first)
- [doc/uml/index.html](doc/uml/index.html) — rendered architecture diagrams (self-contained, open in a browser); [mermaid source](doc/uml/README.md)
- [doc/progress.md](doc/progress.md) — component status, test map, known limitations
- [doc/components/](doc/components/) — per-component detail:
  [lazyraw + cache](doc/components/lazyraw-cache.md) ·
  [artifact union](doc/components/artifact-union.md) ·
  [report](doc/components/report.md) ·
  [pipeline + executor](doc/components/executor.md) ·
  [kernels](doc/components/kernels.md) ·
  [loaders](doc/components/loaders.md) ·
  [dumper](doc/components/dumper.md) ·
  [DJI telemetry](doc/components/dji-telemetry.md)

## Credentials

All I/O goes through fsspec; `s3://` and local paths behave identically. Credentials come
from the standard AWS chain only — never from YAML, never committed. `test/s3.json` holds
real vendor keys, is gitignored, and is read by exactly one thing in this repo: the
`s3_env` pytest fixture.
