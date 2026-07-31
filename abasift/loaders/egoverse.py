"""``EgoverseDjiLoader`` — the egoverse vendor's ``0_egoverse_20260730`` delivery.

Layout, one sample per md5-named directory::

    <root>/<md5>/DJI_20260319133058_0116_D.MP4     # video + embedded DJI telemetry
    <root>/<md5>/DJI_20260319133058_0116_D.json    # task + planning annotation

Normalised to three canonical streams:

===================  ================  ==================================================
``video/main``       ``video_meta``    header-only probe, remote-seek, no download
``imu/main``         ``dji_imu``       *same URI*, different decoder: demuxes the embedded
                                       ``DJI meta`` track from a disk-cached copy
``annotation/task``  ``json``          the sidecar
===================  ================  ==================================================

Two streams pointing at one URI with different decoders is the whole point of splitting
``LazyRaw`` into uri + decoder: a duration pipeline pays 0.45 MB per file, an IMU
pipeline materialises it, and a pipeline doing both downloads it exactly once (the disk
cache is keyed by URI).
"""

from __future__ import annotations

import posixpath
from typing import Iterator

import fsspec

from ..data import Sample
from ..kernel import ArtifactExt, SourceKernel, batch_stream
from ..lazy import LazyRaw
from ..report import Check, ReportExt
from ._fs import check_order, list_files

VIDEO_EXT = (".mp4", ".mov", ".mkv")


class EgoverseDjiLoader(SourceKernel):
    """Params:

    ``root``          prefix holding the md5 directories (local or ``s3://``)
    ``batch_size``    default 4 (IMU work materialises whole videos — keep batches small)
    ``order``         ``name`` (default) or ``size``; ``size`` ascending makes probes cheap
    ``max_samples``   stop after N samples
    """

    def __init__(
        self,
        root: str,
        batch_size: int = 4,
        order: str = "name",
        max_samples: int | None = None,
    ):
        self.root = root
        self.batch_size = int(batch_size)
        self.order = check_order(order)
        self.max_samples = max_samples

    def iter_batches(self) -> Iterator[tuple[ArtifactExt, ReportExt]]:
        return batch_stream(self._enumerate(), self.batch_size)

    def _enumerate(self) -> Iterator[Sample | tuple[str, Check]]:
        """One md5 directory -> one sample; the framework does the batching."""
        fs, base = fsspec.core.url_to_fs(self.root)
        groups = _group_by_directory(list_files(fs, base), base)
        ordered = sorted(
            groups.items(),
            key=(lambda kv: kv[1]["video_size"]) if self.order == "size" else (lambda kv: kv[1]["dir"]),
        )
        if self.max_samples is not None:
            ordered = ordered[: self.max_samples]

        for sample_id, files in ordered:
            if not files["video"]:
                yield sample_id, Check(
                    "error", details={"reason": "no video in sample directory", "dir": files["dir"]}
                )
                continue
            video_uri = fs.unstrip_protocol(files["video"])
            streams = {
                "video/main": LazyRaw(video_uri, "video_meta"),
                # Same URI, different decoder: the IMU lives inside the MP4.
                "imu/main": LazyRaw(video_uri, "dji_imu"),
            }
            if files["json"]:
                streams["annotation/task"] = LazyRaw(fs.unstrip_protocol(files["json"]), "json")
            yield Sample(
                sample_id=sample_id,
                streams=streams,
                meta={
                    "uri": video_uri,
                    "size_bytes": files["video_size"],
                    "md5_folder": sample_id,
                    "has_annotation": bool(files["json"]),
                },
            )


def _group_by_directory(entries: list[dict], base: str) -> dict[str, dict]:
    """One listing of the whole tree -> ``{md5: {dir, video, video_size, json}}``.

    1000 sample directories is 2000 keys: one recursive listing, not 1000 round trips.
    """
    groups: dict[str, dict] = {}
    for e in entries:
        path = e["name"]
        directory = posixpath.dirname(path)
        sample_id = posixpath.basename(directory)
        if directory.rstrip("/") == base.rstrip("/"):
            continue  # stray file at the top level, not a sample
        g = groups.setdefault(
            sample_id, {"dir": directory, "video": None, "video_size": 0, "json": None}
        )
        name = posixpath.basename(path).lower()
        size = e["size"]
        if name.endswith(VIDEO_EXT) and size > g["video_size"]:
            g["video"], g["video_size"] = path, size  # largest media file wins
        elif name.endswith(".json"):
            g["json"] = path
    return groups
