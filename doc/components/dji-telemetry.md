# DJI embedded telemetry — reverse-engineering log

Module: `src/abasift/vendor/dji_telemetry.py`. Consumed via the `dji_imu` decoder.

## The problem

The `general_324h` delivery has no IMU sidecar. The inertial data is **inside the MP4**, so
"write a suitable dataloader for that vendor" means understanding the container.

## What is in the container

`ffprobe` on `general_324h/0008ac4e.../DJI_20260319133058_0116_D.MP4` — six tracks:

| idx | type | tag | handler | rate | content |
|-----|------|-----|---------|------|---------|
| 0 | video | `hvc1` | VideoHandler | 59.94 fps, 1920x1080 | HEVC |
| 1 | audio | `mp4a` | SoundHandler | 48 kHz | AAC |
| 2 | data | `djmd` | **DJI meta** | 1/frame, ~64 kbit/s | telemetry — what we want |
| 3 | data | `dbgi` | DJI dbgi | 1/frame, ~1.45 Mbit/s | camera debug/AE stats |
| 4 | data | `tmcd` | TimeCodeHandler | 1 | timecode |
| 5 | video | mjpeg | — | — | thumbnail |

Both data tracks carry **bare protobuf** with no schema shipped. The first `djmd` packet
contains a header naming the schema:

```
dvtm_ac204.proto / 02.01.01 / 2.0.1 / 82JXN7F00E5W40 / DJI OsmoAction5 Pro
```

`dbginfo_ac204.proto` for the other one. `ac204` is the model code (Osmo Action 5 Pro).

## Finding the fields

No schema, so the payloads were walked with a generic protobuf wire reader (field number +
wire type) and the candidates checked against physics. Per-frame record layout:

```
3                per-frame record          (fields 1/2 hold the file header, first packet only)
3.1.2  varint    capture timestamp, µs
3.2.9  msg       4 × float32 (fields 1-4)
3.2.10 msg       3 × float32 (fields 2,3,4 — field 1 absent, i.e. proto3 zero)
```

Evidence that fixed the interpretation, over the full 2208-packet track:

- **3.1.2 is a µs timestamp.** Δ between consecutive packets = 16683 µs = 1/59.94 s,
  exactly the video frame rate. Track span 36.818 s vs container duration 36.864 s.
- **3.2.9 is an orientation quaternion.** ‖q‖ mean 1.00000, std 0.00000 over the whole
  track. (Rotating world-down by it gives a vector close to 3.2.10, so 3.2.10 is
  gravity-dominated — consistent with an accelerometer.)
- **3.2.10 is acceleration in g.** Per-axis means `[-0.948, -0.167, -0.252]`, ‖a‖ mean
  1.0205, std 0.0625, min 0.5796, max 1.4767. A stationary-ish handheld camera reads 1 g;
  nothing else in the message has that signature.

The `dbgi` track (~3 kB/frame) was inspected and left alone: its fields look like AE/exposure
statistics and histograms, and guessing at them isn't warranted.

## Guarding an inferred mapping

The field numbers are reverse-engineered, so a firmware or model change could move them and
we would silently report nonsense. `_validate_layout` therefore rejects a track whose

- timestamps are not monotonic, or
- median ‖quaternion‖ is outside [0.98, 1.02], or
- median ‖accel‖ is outside [0.5, 2.0] g,

as an *unrecognised layout* → `DecodeError` → `status: error` for that sample. Failing loudly
beats mis-measuring. Round-trip and rejection are both tested offline
(`test_frame_parser_round_trips`, `test_layout_validation_rejects_a_moved_field_mapping`).

The parsed header's proto name and device string land in `ImuTrack.layout` / `.device` and
in the report details, so every IMU verdict records which layout produced it.

## Why `local_path()` and not `open()`

Measured on the same 177.9 MB file: demuxing this track over a seeking remote file object
read **177.90 MB in 4406 ranged GETs (6.3 s)** — the packets are interleaved with video, so
"just the telemetry" touches the whole byte range anyway. One sequential download into the
worker disk cache, then local demuxing, is strictly cheaper. `read_dji_imu` therefore takes
a local path by design.

## Limitation: the IMU is frame-synchronous

There is one telemetry record per **video frame**, so the effective IMU rate equals the frame
rate — observed 59.94, 29.97 and 25.0 Hz across samples. Consequences:

- Spike detection bandwidth is limited to that rate; a sub-frame-duration glitch is
  invisible, and `ImuSpikeKernel` reports `rate_hz` so a reader can see what was possible.
- A 1.6 s clip yields ~48 records, below `min_samples: 64`, and is reported as `error`
  ("unmeasurable") rather than `pass`.

Higher-rate raw inertial data may exist in `dbgi`. Extracting it is future work and would
be a new decoder, not a change to any kernel.
