"""``FlatDirLoader`` — one media file per sample, from a directory.

Works unchanged on a local path or an ``s3://`` prefix (fsspec), which is why it serves
both the unit tests and the ``1_test_20260730`` vendor delivery: a flat
directory of ``VID_*.mp4`` / ``*.mov`` with no sidecars and no telemetry track.
"""

from __future__ import annotations

import fnmatch
import posixpath
from typing import Iterator

import fsspec

from ..data import Sample
from ..kernel import ArtifactExt, SourceKernel, batch_stream
from ..lazy import LazyRaw
from ..report import Check, ReportExt
from ._fs import check_order, list_files

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
        self.order = check_order(order)
        self.max_samples = max_samples
        self.stream = stream
        self.decoder = decoder

    def iter_batches(self) -> Iterator[tuple[ArtifactExt, ReportExt]]:
        return batch_stream(self._enumerate(), self.batch_size)

    def _enumerate(self) -> Iterator[Sample | tuple[str, Check]]:
        """One media file -> one sample; the framework does the batching."""
        fs, base = fsspec.core.url_to_fs(self.root)
        media = [e for e in list_files(fs, base, self.recursive) if _matches(e["name"], self.patterns)]
        media.sort(key=(lambda e: e["size"]) if self.order == "size" else (lambda e: e["name"]))
        if self.max_samples is not None:
            media = media[: self.max_samples]

        for entry in media:
            sample_id = _sample_id(base, entry["name"])
            if not entry["size"]:
                # Enumeration finding: nothing to decode, and we knew before opening it.
                yield sample_id, Check("error", details={"reason": "zero-size file"})
                continue
            uri = fs.unstrip_protocol(entry["name"])
            yield Sample(
                sample_id=sample_id,
                streams={self.stream: LazyRaw(uri, self.decoder)},
                meta={"uri": uri, "size_bytes": entry["size"]},
            )


def _matches(path: str, patterns: tuple[str, ...]) -> bool:
    name = posixpath.basename(path)
    return any(fnmatch.fnmatchcase(name, p) for p in patterns)


def _sample_id(base: str, path: str) -> str:
    rel = posixpath.relpath(path, base)
    stem, _, _ext = rel.rpartition(".")
    return stem or rel
