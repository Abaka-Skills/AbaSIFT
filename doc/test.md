# Test map

What each test file pins down, so a change that breaks one tells you which part of the
contract you moved. Acceptance is green: `pytest` — 174 tests (170 offline + 4 against the
real bucket) — plus `python -m pyflakes abasift test` clean (no unused or shadowed imports).

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
| `test_executor.py` | decode sharing across parallel branches, joins, the three failsafe layers, deterministic dump paths, and the leaf-only batch merge (a replacement beating its own stale ancestor, a delete surviving a sibling, the middle of the graph still reaching the job union and the job report) |
| `test_duration_demo.py` | the acceptance test from design §6 (2/5/10 s + corrupt), thresholds-live-in-YAML, CLI |
| `test_loaders.py` | enumeration findings, ordering, `recursive`/`patterns`, and the shared `batch_stream` grouping rule |
| `test_imu_spike.py` | spike statistics, verdict thresholds, DJI wire-format round-trip, layout validation, missing-track failsafe |
| `test_frames.py` | frame sampling (default 1 fps, `fps: null`, aspect-preserving resize), the memory contract (a handle in the union, a `np.memmap` for the reader), cache reuse across runs, and the free-mode archiver deleting a stack |
| `test_video_dumper.py` | what `VideoDumper` writes (every assertion decodes the written file back), `fps` as playback rate vs sampling rate, the shared path scheme, and the two things it refuses to guess: no stack upstream, or two of them |
| `test_integration_s3.py` | both demos end to end on the vendor bucket; asserts *no* download for header probes and exactly one download for a shared URI |
| `test_banner.py` | YAML cache config (strict keys, installed before any I/O, silence leaves the process cache alone), the banner reporting the *resolved* infrastructure, and the DAG: rails that draw a fork, a join, a reused lane and an edge spanning the graph, one `node[Kernel] ← inputs` per row in a square frame; and the closing tallies, which name the node behind each failure, count each verdict once, and colour only what actually happened — all of it plain the moment no TTY is watching |
| `test_vis.py` | that the pipeline view is *derived* (roles and columns from the DAG, signatures and defaults from the live classes) and *hosted* (editing the YAML or a kernel moves the state token and changes the served page; a broken YAML is shown, not fatal); and that `run --vis` works off executor events — an observer that raises cannot fail a job, and `RunView` never re-reads the YAML mid-run |

The integration tests default to 3 (duration) / 2 (IMU) samples and take the smallest
files first (`order: size`), so a full run costs a few seconds. Raise
`ABASIFT_TEST_MAX_SAMPLES` to sweep the delivery.

The only path with no offline coverage is demuxing a *real* MP4 telemetry track — it
needs a real DJI file, so it lives in the `s3` suite. The wire-format parser itself is
covered offline by re-encoding a packet and round-tripping it. The rest of what is *not*
covered is design, not omission: see [design.md §8](design.md#8-known-limitations).
