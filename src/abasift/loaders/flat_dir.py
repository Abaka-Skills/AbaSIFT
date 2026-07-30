"""``FlatDirLoader`` — one media file per sample, from a directory.

Works unchanged on a local path or an ``s3://`` prefix (fsspec), which is why it serves
both the unit tests and the ``egoverse_test/20260730_test`` vendor delivery: a flat
directory of ``VID_*.mp4`` / ``*.mov`` with no sidecars and no telemetry track.
"""

from __future__ import annotations

import fnmatch
import posixpath
from typing import Any, Iterator

import fsspec

from ..data import Batch, Sample
from ..kernel import SourceKernel
from ..lazy import LazyRaw
from ..report import Check, ReportExt

DEFAULT_PATTERNS = ("*.mp4", "*.MP4", "*.mov", "*.MOV", "*.mkv", "*.avi")


class FlatDirLoader(SourceKernel):
    """Params:

    ``root``          directory or prefix (local path or ``s3://bucket/prefix``)
    ``batch_size``    samples per batch (efficiency unit only), default 8
    ``patterns``      basename globs, default video extensions
    ``recursive``     descend into subdirectories, default False
    ``order``         ``name`` (default) or ``size`` — ascending, always deterministic
    ``max_samples``   stop after N samples (job slicing is external; this is for probes)
    ``stream``        canonical stream name, default ``video/main``
    ``decoder``       decoder for that stream, default ``video_meta``
    """

    def __init__(
        self,
        root: str,
        batch_size: int = 8,
        patterns: tuple[str, ...] = DEFAULT_PATTERNS,
        recursive: bool = False,
        order: str = "name",
        max_samples: int | None = None,
        stream: str = "video/main",
        decoder: str = "video_meta",
    ):
        self.root = root
        self.batch_size = int(batch_size)
        self.patterns = tuple(patterns)
        self.recursive = bool(recursive)
        self.order = order
        self.max_samples = max_samples
        self.stream = stream
        self.decoder = decoder
        if order not in ("name", "size"):
            raise ValueError(f"order must be 'name' or 'size', got {order!r}")

    def iter_batches(self) -> Iterator[tuple[dict[str, Any], ReportExt]]:
        fs, base = fsspec.core.url_to_fs(self.root)
        entries = _list(fs, base, self.recursive)
        media = [e for e in entries if _matches(e["name"], self.patterns)]
        media.sort(key=(lambda e: e["size"]) if self.order == "size" else (lambda e: e["name"]))
        if self.max_samples is not None:
            media = media[: self.max_samples]

        pending: list[Sample] = []
        rext = ReportExt()
        index = 0
        for entry in media:
            sample_id = _sample_id(base, entry["name"])
            if not entry["size"]:
                # Enumeration finding: nothing to decode, and we knew before opening it.
                rext.add(sample_id, "enumerate", Check("error", details={"reason": "zero-size file"}))
                continue
            pending.append(
                Sample(
                    sample_id=sample_id,
                    streams={self.stream: LazyRaw(fs.unstrip_protocol(entry["name"]), self.decoder)},
                    meta={"uri": fs.unstrip_protocol(entry["name"]), "size_bytes": entry["size"]},
                )
            )
            if len(pending) >= self.batch_size:
                yield {"batch": Batch(tuple(pending), index)}, rext
                pending, rext, index = [], ReportExt(), index + 1
        if pending or rext.checks:
            yield {"batch": Batch(tuple(pending), index)}, rext


def _list(fs, base: str, recursive: bool) -> list[dict]:
    detail = fs.find(base, detail=True) if recursive else fs.ls(base, detail=True)
    entries = detail.values() if isinstance(detail, dict) else detail
    return [
        {"name": e["name"], "size": int(e.get("size") or 0)}
        for e in entries
        if e.get("type", "file") == "file"
    ]


def _matches(path: str, patterns: tuple[str, ...]) -> bool:
    name = posixpath.basename(path)
    return any(fnmatch.fnmatchcase(name, p) for p in patterns)


def _sample_id(base: str, path: str) -> str:
    rel = posixpath.relpath(path, base)
    stem, _, _ext = rel.rpartition(".")
    return stem or rel
