"""``DataArchiver`` — the one kernel allowed to change existing artifacts.

**Archiving is what distinguishes it from a dumper.** A dumper (``VideoDumper``) writes
something to ``target`` and leaves the union exactly as it found it. An archiver takes an
object *out* of the job's working set: it writes the value out and swaps it for a handle,
or drops it outright. Writing is the part they share; removing the in-memory object is what
makes this one a ``MutatingKernel``, and the reason it is the only one.

Two modes, chosen by whether ``target`` is set:

**archive** (``target: s3://... | /path``) — write each matching artifact out, then swap the
in-union value for a ``LazyRaw`` pointing at what was written. Memory freed, information
preserved: a downstream *reader* calls ``.decode()`` and gets the value back either way.

A reduce is not a reader. **Archive intermediates; never archive a key some node reduces in
``digest()``.** ``sum()`` over handles raises, and it should: once a key is archived the
object is out of the job's working set, so arithmetic over it is a category error rather
than something the framework should paper over by decoding behind your back.

**free** (``target:`` empty) — drop the matching keys, and delete their backing file if it
lives in our own disk cache. Nothing is saved: this is for intermediates nobody downstream
reads. Destructive, so it is never the default — the empty string has to be written out.

Under an explicit ``target`` paths are ``f(job_id, pipeline_hash, node, key)`` with no
timestamps, so a re-run of the same YAML overwrites the same objects and a retried job is
idempotent — while an *edited* YAML lands beside it rather than over it. The *default*
target instead stamps the job's start time (``dump/<unix ts>/``), giving every run its own
tree — see :mod:`abasift.kernels._dump`, which both writers share.

Placement is the pipeline author's job: an archiver is an explicit node, and it must sit
*downstream* of everything that reads the keys it takes away — an archiver running
concurrently with a reader of the same key is an authoring error, not something the
framework guesses.
"""

from __future__ import annotations

import fnmatch
import json
from pathlib import Path
from typing import Any

import yaml

from ..cache import disk_cache
from ..data import ArtifactUnion, jsonable
from ..kernel import Mutation, MutatingKernel
from ..lazy import LazyRaw
from ..report import ReportView
from ._dump import DumpTarget, flat_name, write_bytes, write_stream

#: Pseudo-keys, both written in the commit pass and never per batch: the per-sample
#: document, and the per-pipeline one (which brings the config file along with it).
REPORT_KEY = "__report__"
PIPELINE_KEY = "__pipeline__"

__all__ = ["DataArchiver", "REPORT_KEY", "PIPELINE_KEY"]


class DataArchiver(DumpTarget, MutatingKernel):
    """Params:

    ``keys``    globs over union keys, plus the pseudo-keys ``__report__`` (per sample)
                and ``__pipeline__`` (the job + its config)
    ``target``  destination prefix (local or ``s3://``). Omit it and each run lands in
                ``dump/<job>_<hash>/<unix ts>/``; set it to ``""`` for *free* mode.
    """

    def __init__(
        self,
        keys: list[str] | tuple[str, ...] = (REPORT_KEY, PIPELINE_KEY),
        target: str | None = None,
    ):
        self.keys = tuple(keys)
        #: ``None`` = use the dated default; ``""`` = free mode; anything else = verbatim.
        self.target = self.clean_target(target)

    @property
    def freeing(self) -> bool:
        """Free mode is opt-in: destructive behaviour never happens by default."""
        return self.target == ""

    def replaced_key_patterns(self) -> tuple[str, ...]:
        """What this node will swap for a handle: everything but the pseudo-keys.

        ``__report__`` / ``__pipeline__`` never enter the union — they are written in the
        commit pass — so two nodes archiving the report on parallel branches is fine.
        """
        if self.freeing:
            return ()
        return tuple(k for k in self.keys if k not in (REPORT_KEY, PIPELINE_KEY))

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
        replace = {key: self._archive(key, art[key]) for key in matched}
        return Mutation(replace=replace)

    # -- commit: the finished job documents ------------------------------

    def commit(self, art: ArtifactUnion, report: ReportView) -> Mutation | None:
        """Write the two job documents, and the config that produced them.

        Both are held until the commit pass so what lands is the *complete*
        picture: `job.counts` stamped, every `digest()` summary folded in.
        """
        if self.freeing:
            return None
        ext = {}
        if REPORT_KEY in self.keys:
            ext["report_uri"] = self._write_json(self.path_for("report.json"), report.to_json())
        if PIPELINE_KEY in self.keys:
            ext["pipeline_uri"] = self._write_json(
                self.path_for("pipeline.json"), report.pipeline_json()
            )
            # The config itself, beside the runs rather than inside one: every run under
            # this directory shares it — that is what the hash in the name asserts.
            ext["config_uri"] = self._write_config(report)
        return Mutation(ext=ext) if ext else None

    def _write_config(self, report: ReportView) -> str:
        """Copy the source YAML to ``{job_root}/pipeline.yaml``, comments and all.

        A pipeline built in code has no file to copy, so its definition is serialised
        instead — the archived config is never absent, only sometimes reconstructed.
        """
        uri = f"{self.job_root}/pipeline.yaml"
        if report.yaml_path and Path(report.yaml_path).is_file():
            return write_bytes(uri, Path(report.yaml_path).read_bytes())
        return write_bytes(uri, yaml.safe_dump({"pipeline": report.definition}, sort_keys=False).encode())

    # -- internals ------------------------------------------------------

    def _match(self, art: ArtifactUnion) -> list[str]:
        return sorted(
            key
            for key in art.keys()
            if any(fnmatch.fnmatchcase(key, pattern) for pattern in self.keys)
        )

    def _archive(self, key: str, value: Any) -> Any:
        name = flat_name(key)
        if isinstance(value, LazyRaw):
            suffix = value.uri.rsplit("/", 1)[-1]
            with value.open() as src:
                uri = write_stream(self.path_for(f"{name}__{suffix}"), src)
            return LazyRaw(uri, value.decoder, **value.opts)
        if isinstance(value, (bytes, bytearray)):
            uri = write_bytes(self.path_for(f"{name}.bin"), bytes(value))
            return LazyRaw(uri, "bytes")
        uri = self._write_json(self.path_for(f"{name}.json"), jsonable(value))
        return LazyRaw(uri, "json")

    def _write_json(self, uri: str, obj: Any) -> str:
        return write_bytes(uri, json.dumps(obj, indent=2, sort_keys=False).encode())
