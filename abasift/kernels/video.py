"""``VideoDumper`` — write a sample's frame stack back out as a video.

The counterpart to ``VideoFrameKernel``: that node turns a video into frames for kernels to
measure, this one turns frames into a video for a *person* to look at. A QC verdict a vendor
will dispute is worth an exhibit — a 2 fps proxy of the take that failed, small enough to
attach to the finding, next to the report that explains it.

``fps`` is the playback rate of what gets written, and it is deliberately independent of the
rate the stack was sampled at: a 1 fps stack written at 1 fps is a real-time proxy, and the
same stack written at 25 fps is a timelapse of the same footage. Neither is more correct, so
the pipeline says which it wants.

It is a **dumper**, not an archiver: it writes to ``target`` under the shared path rule
(``_dump.py``) and leaves the union as it found it, adding ``video/<sample_id>`` and
rewriting nothing. So it stays an ordinary ``SampleKernel`` — taking an object *out* of the
working set is what makes a kernel a ``MutatingKernel``, and ``DataArchiver`` remains the
only one.
"""

from __future__ import annotations

from fractions import Fraction

from ..cache import disk_cache
from ..data import ArtifactUnion, Sample
from ..decoders import VIDEO_FRAMES, frames_shape
from ..errors import DecodeError, PipelineError
from ..kernel import ArtifactExt, SampleKernel
from ..lazy import LazyRaw
from ..payloads import VideoFrames
from ..report import Check, ReportExt, ReportView
from ._dump import DumpTarget, copy_file, flat_name

PIX_FMT = "yuv420p"


class VideoDumper(DumpTarget, SampleKernel):
    """Params:

    ``fps``          playback rate of the written video; default: the stack's own rate
    ``target``       destination prefix (local or ``s3://``). Omit it and each run lands in
                     ``dump/<job>_<hash>/<unix ts>/<node>/``
    ``codec``        encoder, default ``libx264``
    ``frames_node``  which upstream node's stack to write, when a DAG has more than one
    ``check_name``   default ``video``

    Emits ``video/<sample_id>`` -> ``LazyRaw(video_meta)`` pointing at what was written.
    """

    def __init__(
        self,
        fps: float | None = None,
        target: str | None = None,
        codec: str = "libx264",
        frames_node: str | None = None,
        check_name: str = "video",
    ):
        self.fps = None if fps is None else float(fps)
        if self.fps is not None and self.fps <= 0:
            raise PipelineError(f"fps must be > 0, got {fps!r}")
        self.target = self.clean_target(target)
        if self.target == "":
            # The archiver's empty target means "free"; a dumper has nothing to free, and
            # here it would mean "write the video to the working directory" — nobody's intent.
            raise PipelineError("target must be a prefix or omitted; '' is not free mode here")
        self.codec = str(codec)
        self.frames_node = frames_node
        self.check_name = check_name

    def sift(self, sample: Sample, art: ArtifactUnion):
        # Everything the encode needs to be *described* is in the handle, so a cache hit
        # never maps the stack: the decode happens inside the fetch, on a miss only.
        handle = self._find_stack(art, sample.sample_id)
        n, height, width, _ = frames_shape(handle.opts)
        rate = _rate(self.fps if self.fps is not None else float(handle.opts["fps"]))
        if width % 2 or height % 2:
            raise DecodeError(f"{PIX_FMT} needs even dimensions, got {width}x{height}")

        # Encode into the disk cache, then copy out: a retried job re-uses the encode (the
        # expensive half) and still writes the target, and the same stack asked for twice
        # at the same rate is encoded once.
        codec = self.codec
        key = f"{handle.uri}#video:fps={rate},codec={codec}"
        local = disk_cache().materialize(
            key, lambda dst: _encode(handle.decode(), dst, rate, codec)
        )
        uri = copy_file(str(local), self.path_for(f"{flat_name(sample.sample_id)}.mp4"))

        n_bytes = local.stat().st_size
        check = Check(
            status="pass",
            measurement=n,
            details={
                "uri": uri,
                "fps": float(rate),
                "width": width,
                "height": height,
                "duration_s": round(n / float(rate), 3),
                "codec": self.codec,
                "bytes": int(n_bytes),
            },
        )
        return {self.check_name: check}, {f"video/{sample.sample_id}": LazyRaw(uri, "video_meta")}

    def digest(self, art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]:
        videos = art.per_sample(self.node_name, "video")
        return {}, ReportExt(summary={"n_videos": len(videos), "target": self.run_root})

    # -- internals ---------------------------------------------------------

    def _find_stack(self, art: ArtifactUnion, sample_id: str) -> LazyRaw:
        """This sample's frame stack, found by decoder rather than by a configured key.

        The lookup itself is ``ArtifactUnion.find_lazy`` — a kernel should not hardcode the
        name another node was given in the YAML, nor re-derive the key convention. What
        stays here is the judgement: two stacks in one DAG is a real configuration, so
        ambiguity is an error with the fix in the message, never a guess.
        """
        hits = art.find_lazy(sample_id, VIDEO_FRAMES, node=self.frames_node)
        if not hits:
            where = "" if self.frames_node is None else f" under node {self.frames_node!r}"
            raise DecodeError(
                f"no {VIDEO_FRAMES} artifact for sample {sample_id!r}{where}; "
                "is this node downstream of a VideoFrameKernel?"
            )
        if len(hits) > 1:
            raise DecodeError(
                f"sample {sample_id!r} has {len(hits)} frame stacks ({sorted(hits)}); "
                "set frames_node to say which one to write"
            )
        return next(iter(hits.values()))


def _rate(fps: float) -> Fraction:
    """A frame rate a container can store exactly (29.97 is 30000/1001, not a float)."""
    return Fraction(fps).limit_denominator(10000)


def _encode(frames: VideoFrames, dst: str, rate: Fraction, codec: str) -> None:
    """Encode the stack to ``dst``, one frame in flight at a time.

    ``dst`` is a cache ``.part`` file with no useful suffix, so the container format is
    stated rather than inferred.
    """
    import av

    width, height = frames.size
    with av.open(dst, "w", format="mp4") as out:
        stream = out.add_stream(codec, rate=rate)
        stream.width, stream.height = width, height
        stream.pix_fmt = PIX_FMT
        for i in range(len(frames)):
            # One row of the memmap at a time: the stack itself is never made resident.
            frame = av.VideoFrame.from_ndarray(frames.data[i], format="rgb24")
            frame.pts = i
            for packet in stream.encode(frame):
                out.mux(packet)
        for packet in stream.encode():  # flush the encoder's lookahead
            out.mux(packet)
