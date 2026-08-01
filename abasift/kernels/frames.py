"""``VideoFrameKernel`` — subsample a video into a frame stack the rest of the DAG shares.

This is the one node that decodes *for other kernels*. It exists because four pixel-level
checks over one video would otherwise decode that video four times, and because a check
author should not have to re-derive "how do I get frames at 1 fps" in every kernel.

Two decisions worth knowing before reading the code:

**The stack is never an array in the artifact union.** Frames go to the worker disk cache
and the union carries a ``LazyRaw`` — the same shape ``DataArchiver`` uses when it swaps a
archived artifact for a handle. It has to be this way: the executor merges every batch's
artifacts into the job union, and ``without_transients()`` only drops the ``Batch``, so an
ndarray put there would be held for the whole job and a 400-batch job would die of it.

**The stack never becomes a heap array.** Each frame is scaled, written and dropped, and a
reader gets a ``np.memmap`` — so stack size is a disk-cache question, not a RAM one, and the
cap in ``pipeline.cache`` matters more once this node is in a DAG: raw rgb24 is several
times larger than the compressed source it came from, and the default keeps *every* frame
(subsampling is what a pipeline opts into, never what it gets by silence, because a
skipped frame is a defect nobody can find later). What a decode *does* cost in RAM is
libav's frame-thread pool (measured ~340 MB on a 3840x1200 h264, 36 cores) — a function of
the codec and the machine, not of how long the video is.
"""

from __future__ import annotations

from ..cache import disk_cache
from ..data import ArtifactUnion, Sample
from ..decoders import CHANNELS, frames_handle, frames_nbytes
from ..errors import DecodeError, PipelineError
from ..kernel import ArtifactExt, SampleKernel
from ..lazy import LazyRaw
from ..payloads import VideoMeta
from ..report import Check, ReportExt, ReportView


class VideoFrameKernel(SampleKernel):
    """Params:

    ``fps``         frames per second to keep; the default keeps **every** frame, which is
                    the only sampling that cannot lose the defect a check is looking for.
                    Set it to subsample (and to bound the stack) once a check says it can
    ``width``       output width in pixels, default the source width
    ``height``      output height, default the source height — or, when only one of the two
                    is given, the aspect-preserving value for the other
    ``stream``      stream to read, default ``video/main``
    ``check_name``  default ``frames``

    Emits one per-sample artifact, ``frames/<sample_id>`` -> ``LazyRaw(video_frames)``.
    A downstream kernel calls ``.decode()`` on it and gets a :class:`~abasift.payloads.VideoFrames`.
    """

    def __init__(
        self,
        fps: float | None = None,
        width: int | None = None,
        height: int | None = None,
        stream: str = "video/main",
        check_name: str = "frames",
    ):
        self.fps = None if fps is None else float(fps)
        if self.fps is not None and self.fps <= 0:
            raise PipelineError(f"fps must be > 0 or null (every frame), got {fps!r}")
        self.width = None if width is None else int(width)
        self.height = None if height is None else int(height)
        for name, value in (("width", self.width), ("height", self.height)):
            if value is not None and value < 1:
                raise PipelineError(f"{name} must be >= 1, got {value}")
        self.stream = stream
        self.check_name = check_name

    def sift(self, sample: Sample, art: ArtifactUnion):
        src = sample.stream(self.stream)
        meta = _meta(src)
        width, height = self._target_size(meta)

        # Keyed by what the frames *are*, not by when they were made: two kernels asking
        # for the same stack share one file, and a retried job re-uses it instead of
        # re-decoding. Same reasoning as the archiver's deterministic paths.
        key = f"{src.uri}#frames:fps={self.fps if self.fps else 'all'},size={width}x{height}"
        path = disk_cache().materialize(
            key, lambda dst: _render(src.local_path(), dst, self.fps, width, height)
        )

        frame_bytes = width * height * CHANNELS
        n, remainder = divmod(path.stat().st_size, frame_bytes)
        if remainder:
            raise DecodeError(f"{path} is not a whole number of {width}x{height} rgb24 frames")
        if not n:
            raise DecodeError(f"{src.uri}: decoded no frames at fps={self.fps}")

        source_fps = (meta.video or {}).get("fps")
        rate = float(self.fps or source_fps or (n / meta.duration_s if meta.duration_s else 0.0))
        handle = frames_handle(
            file_uri(path), n=n, width=width, height=height, fps=rate, source=src.uri
        )
        check = Check(
            status="pass",
            measurement=int(n),
            details={
                "fps": round(rate, 4),
                "width": width,
                "height": height,
                "nbytes": frames_nbytes(handle.opts),
                "source": {
                    "fps": source_fps,
                    "width": (meta.video or {}).get("width"),
                    "height": (meta.video or {}).get("height"),
                    "duration_s": meta.duration_s,
                },
            },
        )
        return {self.check_name: check}, {f"frames/{sample.sample_id}": handle}

    def digest(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]:
        handles = art.per_sample(self.node_name, "frames")
        if not handles:
            return {}, ReportExt(summary={"n_videos": 0})
        frames = sum(int(h.opts["n"]) for h in handles.values())
        cache_bytes = sum(frames_nbytes(h.opts) for h in handles.values())
        # Named for what it is: bytes now sitting in the worker disk cache, which is the
        # number that decides whether the pipeline's `cache.size_gb` is big enough.
        summary = {"n_videos": len(handles), "n_frames": frames, "cache_bytes": cache_bytes}
        return {}, ReportExt(summary=summary)

    # -- internals ---------------------------------------------------------

    def _target_size(self, meta: VideoMeta) -> tuple[int, int]:
        """Resolve the output size, preserving the aspect ratio when only one side is given."""
        if self.width and self.height:
            return self.width, self.height
        info = meta.video or {}
        source_w, source_h = info.get("width"), info.get("height")
        if not source_w or not source_h:
            raise DecodeError(f"{meta.source}: container reports no frame size")
        if self.width:
            return self.width, max(1, round(source_h * self.width / source_w))
        if self.height:
            return max(1, round(source_w * self.height / source_h)), self.height
        return int(source_w), int(source_h)


def file_uri(path) -> str:
    """``file://`` + the path, *unencoded*.

    ``Path.as_uri()`` percent-encodes, and fsspec's local filesystem does not decode — so a
    cache directory with a space in it would produce a handle nothing can open.
    """
    return f"file://{path}"


def _meta(raw: LazyRaw) -> VideoMeta:
    """The container header — reusing the stream's own memo when it is already ``video_meta``."""
    if raw.decoder == "video_meta":
        return raw.decode()
    return LazyRaw(raw.uri, "video_meta").decode()


def _render(src_path: str, dst: str, fps: float | None, width: int, height: int) -> None:
    """Decode ``src_path`` once, writing the kept frames to ``dst`` as raw rgb24.

    Exactly one frame is in memory at a time: each is scaled, written and dropped. Frames
    are selected by presentation time, so a variable-rate source still yields a uniformly
    spaced stack (the same rule ffmpeg's ``fps`` filter applies).
    """
    import av
    from av.video.reformatter import VideoReformatter

    # One scaler for the whole video. `frame.reformat()` builds an sws context per frame
    # object, and building it costs an order of magnitude more than the decode: 1167 frames
    # of 1280x400 -> 640x200 take 5.5 s that way and 0.6 s with the context reused.
    scaler = VideoReformatter()
    with av.open(src_path) as container:
        stream = container.streams.video[0]
        stream.thread_type = "AUTO"
        with open(dst, "wb") as out:
            for frame in _kept(container, stream, fps):
                out.write(scaler.reformat(frame, width=width, height=height, format="rgb24").to_ndarray())


def _kept(container, stream, fps: float | None):
    """The frames to keep: everything, or one per ``1/fps`` of presentation time.

    Selecting by time rather than by index is what makes a variable-rate source yield a
    uniformly spaced stack, and advancing the grid past a gap is what keeps that spacing
    when the source drops frames. Same rule as ffmpeg's ``fps`` filter.
    """
    if fps is None:
        yield from container.decode(stream)
        return
    step = 1.0 / fps
    fallback_rate = float(stream.average_rate or stream.guessed_rate or 0) or 1.0
    next_t = 0.0
    for i, frame in enumerate(container.decode(stream)):
        t = float(frame.time) if frame.time is not None else i / fallback_rate
        if t + 1e-9 < next_t:
            continue
        while next_t <= t + 1e-9:
            next_t += step
        yield frame
