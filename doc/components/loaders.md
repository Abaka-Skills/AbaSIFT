# Loaders (node 0)

Module: `abasift/loaders/`.

A loader's entire job is normalising one vendor's directory layout into canonical named
streams carrying `LazyRaw` handles. Enumeration is metadata-only — listing 1000 sample
directories does not read a single media byte.

## What the vendor bucket actually contains

`s3://egocentric-data-delivery/` holds two deliveries with *different* layouts and
different capabilities, which is why two loaders exist:

| dataset | layout | streams available | notes |
|---------|--------|-------------------|-------|
| `1_test_20260730` | flat directory of `VID_*.mp4` / `*.mov` | video + audio | iPhone / Core Media footage. 176 files, 208 GB. **No telemetry track.** Durations 28 s - 57 min. |
| `0_egoverse_20260730` | one md5-named dir per sample: `DJI_*.MP4` + `*.json` | video + audio + **IMU inside the MP4** + task annotation | DJI Osmo Action 5 Pro. 1000 samples, ~640 MB each. A few samples are phone-recorded (`dji_mimo_*.mp4`) and carry no telemetry. |

## `FlatDirLoader`

One media file = one sample, stream `video/main`. Works unchanged on a local path or an
`s3://` prefix — same code serves the offline acceptance test and the flat vendor delivery.

Params: `root`, `batch_size` (8), `patterns` (video extensions), `recursive` (False),
`order` (`name` | `size`), `max_samples`, `stream`, `decoder`.

`sample_id` is the path relative to `root` without its extension: stable, readable, and
deterministic. A zero-size file becomes an `error` sample at enumeration time — we know
it's broken before opening it.

## `EgoverseDjiLoader`

One md5 directory = one sample, `sample_id` = the md5 folder name.

| stream | decoder | cost |
|--------|---------|------|
| `video/main` | `video_meta` | header only, ~0.45 MB, no download |
| `imu/main` | `dji_imu` | **same URI**, materialises to the disk cache |
| `annotation/task` | `json` | sidecar, a few hundred bytes |

Two streams over one URI with different decoders is the point of splitting `LazyRaw` into
uri + decoder: a duration pipeline pays kilobytes, an IMU pipeline downloads, and a
pipeline doing both downloads **once** (the disk cache is keyed by URI — asserted by
`test_the_same_video_is_downloaded_once_for_two_streams`).

The whole tree is listed once (`fs.find`, 2000 keys) and grouped by parent directory — not
1000 round trips. Within a directory the largest media file wins, so a stray thumbnail
can't be mistaken for the take. A directory with no video becomes an `error` sample with
`details.reason`.

`imu/main` is declared for **every** sample, even though some files have no telemetry
track. That is deliberate: the loader would have to open each object to know, and the
framework already has the right answer for "declared but unreadable" — `MissingStream`
inside the decoder becomes `status: error` for that sample while its batchmates continue.
Discovering which vendor files lack IMU is a QC result, not a loader precondition.

Params: `root`, `batch_size` (4 — each sample can materialise a whole video),
`order` (`name` | `size`), `max_samples`.

**Known wart:** under `order: size` a directory with no video has size 0 and therefore sorts
*first*, so `max_samples` spends its budget on broken samples before real ones. Harmless on a
complete delivery, visible while one is still uploading. Recorded by
`test_size_order_ranks_video_less_directories_first`.

## Scoping a job

Work distribution is external: an outside system generates one YAML per machine. The
`max_samples` / `batch_size` / `order` params are for *probes* and tests, not sharding —
`order: size` ascending makes a cheap smoke run over the smallest files.

## Writing a loader for a new vendor

Subclass `SourceKernel` and normalise the vendor layout into a stream of samples; the
framework does the batching:

```python
def iter_batches(self):
    return batch_stream(self._enumerate(), self.batch_size)

def _enumerate(self):          # yields Sample | (sample_id, Check)
    fs, base = fsspec.core.url_to_fs(self.root)
    for entry in list_files(fs, base):
        ...
```

`batch_stream` (in `kernel.py`) owns the grouping rule, including the edge case a hand-rolled
loader loses: a trailing batch that carries only enumeration findings must still be emitted.
`loaders/_fs.py` has `list_files` (one listing call, name + size, no per-file stat) and
`check_order`. Rules that matter:

1. Emit `LazyRaw`s, never bytes. Nothing should be downloaded during enumeration.
2. Use canonical `kind/name` stream names (validated) so vendor-agnostic kernels can find
   them.
3. Yield enumeration failures as `(sample_id, Check("error", ...))` rather than raising — a
   raise aborts the rest of the enumeration.
4. Keep `sample_id` deterministic; dump paths and idempotent retries depend on it.
