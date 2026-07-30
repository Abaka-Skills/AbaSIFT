"""``DataDumper`` — the one kernel allowed to change existing artifacts.

Two modes, chosen by whether ``target`` is set:

**dump** (``target: s3://... | /path``) — write each matching artifact out, then swap the
in-union value for a ``LazyRaw`` pointing at what was written. Downstream kernels are
oblivious: ``.decode()`` works either way. Memory freed, information preserved.

**free** (``target:`` empty) — drop the matching keys, and delete their backing file if it
lives in our own disk cache. For intermediates nobody downstream reads.

Paths are ``f(job_id, node, key)`` with no timestamps, so a re-run of the same YAML
overwrites the same objects and a retried job is idempotent.

Placement is the pipeline author's job: a dumper is an explicit node, and it must sit
*downstream* of everything that reads the keys it frees — a dumper running concurrently
with a reader of the same key is an authoring error, not something the framework guesses.
"""

from __future__ import annotations

import fnmatch
import json
import shutil
from typing import Any

import fsspec

from ..data import ArtifactUnion, _jsonable
from ..kernel import Mutation, MutatingKernel
from ..lazy import LazyRaw
from ..report import ReportView

#: Pseudo-key: the finished job report. Dumped in the finalize pass, never per batch.
REPORT_KEY = "__report__"

_COPY_CHUNK = 8 * 2**20


class DataDumper(MutatingKernel):
    """Params:

    ``keys``    globs over union keys, plus the pseudo-key ``__report__``
    ``target``  destination prefix (local or ``s3://``); empty means *free*
    """

    def __init__(self, keys: list[str] | tuple[str, ...] = (REPORT_KEY,), target: str = ""):
        self.keys = tuple(keys)
        self.target = (target or "").rstrip("/")

    # -- per batch: artifacts -------------------------------------------

    def run_mutating(self, art: ArtifactUnion, report: ReportView) -> Mutation:
        matched = self._match(art)
        if not matched:
            return Mutation()
        if not self.target:
            freed = {k: art[k] for k in matched}
            for value in freed.values():
                _free_cached_file(value)
            return Mutation(delete=frozenset(matched))
        replace = {key: self._dump(key, art[key]) for key in matched}
        return Mutation(replace=replace)

    # -- after finalize: the report -------------------------------------

    def finalize_mutating(self, art: ArtifactUnion, report: ReportView) -> Mutation | None:
        if REPORT_KEY not in self.keys or not self.target:
            return None
        uri = self._write_json(self.path_for("report.json"), report.to_json())
        return Mutation(ext={"report_uri": uri})

    # -- paths ----------------------------------------------------------

    def path_for(self, name: str) -> str:
        """``{target}/{job_id}/{node}/{name}`` — deterministic, no timestamps."""
        job_id = self.job.get("job_id", "job")
        return f"{self.target}/{job_id}/{self.node_name}/{name}"

    # -- internals ------------------------------------------------------

    def _match(self, art: ArtifactUnion) -> list[str]:
        return sorted(
            key
            for key in art.keys()
            if any(fnmatch.fnmatchcase(key, pattern) for pattern in self.keys)
        )

    def _dump(self, key: str, value: Any) -> Any:
        name = key.replace("/", "__")
        if isinstance(value, LazyRaw):
            suffix = value.uri.rsplit("/", 1)[-1]
            uri = self._copy_stream(value, self.path_for(f"{name}__{suffix}"))
            return LazyRaw(uri, value.decoder, **value.opts)
        if isinstance(value, (bytes, bytearray)):
            uri = self._write_bytes(self.path_for(f"{name}.bin"), bytes(value))
            return LazyRaw(uri, "bytes")
        uri = self._write_json(self.path_for(f"{name}.json"), _jsonable(value))
        return LazyRaw(uri, "json")

    def _copy_stream(self, raw: LazyRaw, uri: str) -> str:
        """Stream source -> destination in chunks; a 2 GB video never lands in RAM."""
        fs, path = fsspec.core.url_to_fs(uri)
        _makedirs(fs, path)
        with raw.open() as src, fs.open(path, "wb") as dst:
            shutil.copyfileobj(src, dst, _COPY_CHUNK)
        return uri

    def _write_bytes(self, uri: str, data: bytes) -> str:
        fs, path = fsspec.core.url_to_fs(uri)
        _makedirs(fs, path)
        with fs.open(path, "wb") as f:
            f.write(data)
        return uri

    def _write_json(self, uri: str, obj: Any) -> str:
        return self._write_bytes(uri, json.dumps(obj, indent=2, sort_keys=False).encode())


def _makedirs(fs, path: str) -> None:
    parent = path.rsplit("/", 1)[0]
    try:
        fs.makedirs(parent, exist_ok=True)
    except Exception:
        pass  # object stores have no directories


def _free_cached_file(value: Any) -> None:
    """Delete a freed artifact's backing file, but only inside our own disk cache."""
    if not isinstance(value, LazyRaw):
        return
    from ..cache import disk_cache

    cache = disk_cache()
    root = cache.root.resolve()
    candidate = root / cache.key_for(value.uri)
    try:
        if candidate.resolve().is_relative_to(root) and candidate.exists():
            candidate.unlink()
    except OSError:
        pass
