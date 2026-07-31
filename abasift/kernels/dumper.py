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
import posixpath
import shutil
from typing import Any

import fsspec

from ..cache import disk_cache
from ..data import ArtifactUnion, jsonable
from ..kernel import Mutation, MutatingKernel
from ..lazy import LazyRaw
from ..report import ReportView

#: Pseudo-key: the finished job report. Dumped in the finalize pass, never per batch.
REPORT_KEY = "__report__"

#: Where dumps land when a pipeline doesn't say. Relative to the working directory —
#: never an absolute path, so a YAML is portable between machines.
DEFAULT_TARGET_ROOT = "dump"

_COPY_CHUNK = 8 * 2**20


class DataDumper(MutatingKernel):
    """Params:

    ``keys``    globs over union keys, plus the pseudo-key ``__report__``
    ``target``  destination prefix (local or ``s3://``). Omit it for the dated default
                ``dump/<mmddyyyy>/``; set it to ``""`` for *free* mode.
    """

    def __init__(self, keys: list[str] | tuple[str, ...] = (REPORT_KEY,), target: str | None = None):
        self.keys = tuple(keys)
        #: ``None`` = use the dated default; ``""`` = free mode; anything else = verbatim.
        self.target = None if target is None else target.rstrip("/")

    @property
    def target_root(self) -> str:
        """An explicit ``target`` is used verbatim; the default is ``dump/<mmddyyyy>``.

        The date lives in the *default* only, deliberately. Dump paths must be
        deterministic so a retried job overwrites rather than duplicates, and a date is a
        timestamp — so a pipeline that cares (anything an external splitter generates)
        sets ``target`` explicitly and keeps `f(job_id, node, key)`. Interactive runs get
        a tidy dated tree instead, and pay for it only if they are retried across midnight.
        """
        if self.target is not None:
            return self.target
        return f"{DEFAULT_TARGET_ROOT}/{self.job.get('date') or 'undated'}"

    @property
    def freeing(self) -> bool:
        """Free mode is opt-in: destructive behaviour never happens by default."""
        return self.target == ""

    # -- per batch: artifacts -------------------------------------------

    def run_mutating(self, art: ArtifactUnion, report: ReportView) -> Mutation:
        matched = self._match(art)
        if not matched:
            return Mutation()
        if self.freeing:
            for key in matched:
                value = art[key]
                if isinstance(value, LazyRaw):
                    disk_cache().forget(value.uri)
            return Mutation(delete=frozenset(matched))
        replace = {key: self._dump(key, art[key]) for key in matched}
        return Mutation(replace=replace)

    # -- after finalize: the report -------------------------------------

    def finalize_mutating(self, art: ArtifactUnion, report: ReportView) -> Mutation | None:
        if REPORT_KEY not in self.keys or self.freeing:
            return None
        uri = self._write_json(self.path_for("report.json"), report.to_json())
        return Mutation(ext={"report_uri": uri})

    # -- paths ----------------------------------------------------------

    def path_for(self, name: str) -> str:
        """``{target_root}/{job_id}/{node}/{name}`` — relative unless ``target`` is absolute."""
        parts = (self.target_root, self.job.get("job_id", "job"), self.node_name, name)
        return "/".join(p for p in parts if p)

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
        uri = self._write_json(self.path_for(f"{name}.json"), jsonable(value))
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
    try:
        fs.makedirs(posixpath.dirname(path), exist_ok=True)
    except Exception:
        pass  # object stores have no directories
