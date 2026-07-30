# LazyRaw + the two-tier cache

Modules: `src/abasift/lazy.py`, `src/abasift/cache.py`, `src/abasift/decoders.py`.

## The question this component answers

Vendor videos in this delivery run **70 MB - 2 GB**; one dataset is 15.7 GB across 53
files, another is ~1000 files of similar size. So: do we hold payloads in memory, or put
them on disk and let kernels work on files?

The answer is **both, chosen per decoder**, because the two demo checks sit at opposite
ends. Measured on `general_324h/0008ac4e.../DJI_20260319133058_0116_D.MP4` (177.9 MB),
reading through a seeking fsspec file object:

| what | bytes pulled | read calls | wall clock |
|------|--------------|-----------|-----------|
| container header only (duration) | **0.45 MB (0.25%)** | 11 | 1.0 s |
| demux the interleaved `djmd` telemetry track | **177.90 MB (99.99%)** | 4406 | 6.3 s |

A duration probe must never download the file. An IMU read touches essentially every byte
*anyway*, and doing it over the network costs thousands of tiny ranged GETs — so it should
be one sequential download to disk, then local file work.

## Three access modes

`LazyRaw` is `uri + decoder name + opts`, fully serializable, nothing fetched on
construction. It exposes:

| method | mechanism | use for |
|--------|-----------|---------|
| `open()` | fsspec seekable file object; ranged GETs only | container headers/metadata |
| `local_path()` | stream whole object into the worker disk cache, return a path | interleaved tracks, external CLI tools, GPU decoders, frame sweeps |
| `read_bytes(max_bytes)` | whole object in memory, **refuses** oversize | JSON sidecars, small blobs |

`read_bytes` defaults to a 64 MiB ceiling and raises `DecodeError` above it. That guard is
deliberate: the easiest way to blow up a worker is a kernel author reaching for "just give
me the bytes" on a 2 GB video.

Nothing ever holds a whole video in memory: `local_path()` downloads via
`fs.get_file` (chunked), and `DataDumper` copies with `shutil.copyfileobj` in 8 MB chunks.

## Tier 1 — worker-global disk cache (`cache.py`)

- Keyed by URI (`sha256[:32]` + original suffix, so ffmpeg can still sniff the format).
- Size-capped LRU, default 32 GB; `ABASIFT_CACHE_DIR` / `ABASIFT_CACHE_GB` override.
  mtime is the LRU clock (atime is unreliable under `relatime`), touched on every hit.
- Download goes to a `.part-<pid>-<tid>` sibling and is atomically `os.replace`d, so a
  crash can never expose a truncated file and a retried job is idempotent.
- Per-URI locks: N threads asking for the same video download it **once**. The S3
  integration suite asserts this (`test_the_same_video_is_downloaded_once_for_two_streams`).
- Eviction runs on insert. Evicting a file another thread has open is safe on POSIX — the
  descriptor stays valid after `unlink`.

## Tier 2 — in-memory decode memo (`lazy.py`)

`decode()` memoizes the decoded object on the instance under a lock. Two things follow:

- **Parallel DAG branches over one sample decode once.** The duration branch and the IMU
  branch of demo 2 share whatever they both touch. Verified offline by
  `test_parallel_branches_share_one_decode_then_release` (counting decoder, asserts every
  URI decoded exactly once).
- **The memo dies with the batch.** The executor calls `Batch.release()` after each
  batch's DAG run, so decoded memory is bounded by (batch size × decoded size), not by
  dataset size. This is why the design keeps only 1-2 batches in flight.

## Decoders and their access mode

| decoder | returns | mode |
|---------|---------|------|
| `video_meta` | `VideoMeta` (duration, codec, wxh, fps, data-track handler names) | `open()` |
| `video_file` | local path `str` | `local_path()` |
| `dji_imu` | `ImuTrack` | `local_path()` |
| `json` | parsed JSON | `read_bytes(16 MiB)` |
| `bytes` | `bytes` | `read_bytes(64 MiB)` |

Registration is a decorator (`@register_decoder("name")`), so a vendor team can add a
decoder without touching the framework. The name is what gets serialized — a `LazyRaw`
stays a plain dict of strings.

Any exception inside a decoder is wrapped as `DecodeError`, which the per-sample failsafe
turns into `status: error` for that sample. Unreadable data is a QC finding, not a crash.

## Credentials

All I/O goes through fsspec; `s3://` and local paths behave identically. Credentials come
from the standard AWS chain only. `test/s3.json` is read by exactly one thing in this
repo — the `s3_env` pytest fixture, which puts the keys in environment variables. The
framework has no code path that reads it, and it is gitignored.
