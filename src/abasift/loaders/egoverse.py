"""``EgoverseDjiLoader`` — the egoverse vendor's ``general_324h`` delivery.

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
from typing import Any, Iterator

import fsspec

from ..data import Batch, Sample
from ..kernel import SourceKernel
from ..lazy import LazyRaw
from ..report import Check, ReportExt

VIDEO_EXT = (".mp4", ".mov", ".mkv")


class EgoverseDjiLoader(SourceKernel):
    """Params:

    ``root``          prefix holding the md5 directories (local or ``s3://``)
    ``batch_size``    default 4 (IMU work materialises whole videos — keep batches small)
    ``order``         ``name`` (default) or ``size``; ``size`` ascending makes probes cheap
    ``max_samples``   stop after N samples
    ``with_imu``      declare ``imu/main``, default True
    ``with_annotation``  declare ``annotation/task``, default True
    """

    def __init__(
        self,
        root: str,
        batch_size: int = 4,
        order: str = "name",
        max_samples: int | None = None,
        with_imu: bool = True,
        with_annotation: bool = True,
    ):
        self.root = root
        self.batch_size = int(batch_size)
        self.order = order
        self.max_samples = max_samples
        self.with_imu = bool(with_imu)
        self.with_annotation = bool(with_annotation)
        if order not in ("name", "size"):
            raise ValueError(f"order must be 'name' or 'size', got {order!r}")

    def iter_batches(self) -> Iterator[tuple[dict[str, Any], ReportExt]]:
        fs, base = fsspec.core.url_to_fs(self.root)
        groups = _group_by_directory(fs, base)

        def sort_key(item):
            _sid, files = item
            return files["video_size"] if self.order == "size" else files["dir"]

        ordered = sorted(groups.items(), key=sort_key)
        if self.max_samples is not None:
            ordered = ordered[: self.max_samples]

        pending: list[Sample] = []
        rext = ReportExt()
        index = 0
        for sample_id, files in ordered:
            if not files["video"]:
                rext.add(
                    sample_id,
                    "enumerate",
                    Check("error", details={"reason": "no video in sample directory", "dir": files["dir"]}),
                )
                continue
            video_uri = fs.unstrip_protocol(files["video"])
            streams = {"video/main": LazyRaw(video_uri, "video_meta")}
            if self.with_imu:
                streams["imu/main"] = LazyRaw(video_uri, "dji_imu")
            if self.with_annotation and files["json"]:
                streams["annotation/task"] = LazyRaw(fs.unstrip_protocol(files["json"]), "json")
            pending.append(
                Sample(
                    sample_id=sample_id,
                    streams=streams,
                    meta={
                        "uri": video_uri,
                        "size_bytes": files["video_size"],
                        "md5_folder": sample_id,
                        "has_annotation": bool(files["json"]),
                    },
                )
            )
            if len(pending) >= self.batch_size:
                yield {"batch": Batch(tuple(pending), index)}, rext
                pending, rext, index = [], ReportExt(), index + 1
        if pending or rext.checks:
            yield {"batch": Batch(tuple(pending), index)}, rext


def _group_by_directory(fs, base: str) -> dict[str, dict]:
    """One listing of the whole tree -> ``{md5: {dir, video, video_size, json}}``.

    1000 sample directories is 2000 keys: one recursive listing, not 1000 round trips.
    """
    detail = fs.find(base, detail=True)
    entries = detail.values() if isinstance(detail, dict) else detail
    groups: dict[str, dict] = {}
    for e in entries:
        if e.get("type", "file") != "file":
            continue
        path = e["name"]
        directory = posixpath.dirname(path)
        sample_id = posixpath.basename(directory)
        if directory.rstrip("/") == base.rstrip("/"):
            continue  # stray file at the top level, not a sample
        g = groups.setdefault(
            sample_id, {"dir": directory, "video": None, "video_size": 0, "json": None}
        )
        name = posixpath.basename(path).lower()
        size = int(e.get("size") or 0)
        if name.endswith(VIDEO_EXT) and size > g["video_size"]:
            g["video"], g["video_size"] = path, size  # largest media file wins
        elif name.endswith(".json"):
            g["json"] = path
    return groups
