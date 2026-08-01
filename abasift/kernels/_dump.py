"""Where a kernel that *writes* puts what it wrote — shared by the archiver and the dumpers.

One scheme for both, because it is a hard rule rather than a kernel's opinion: with an
explicit ``target`` every path is ``f(job_id, pipeline_hash, node, key)`` and a retried job
overwrites exactly what it wrote before; without one, each run gets its own dated subtree.
Two writing kernels drifting apart on that would be a silent correctness bug, so the rule
lives here rather than in each of them. It says nothing about the union — writing where and
taking something out of the working set are separate concerns, which is exactly the line
between ``VideoDumper`` and ``DataArchiver``.

Same reasoning as ``loaders/_fs.py``: a private helper module keeps the sibling kernels
from importing each other.
"""

from __future__ import annotations

import posixpath
import shutil
from io import BytesIO

import fsspec

#: Where dumps land when a pipeline doesn't say. Relative to the working directory —
#: never an absolute path, so a YAML is portable between machines.
DEFAULT_TARGET_ROOT = "dump"

COPY_CHUNK = 8 * 2**20


class DumpTarget:
    """The ``target`` parameter's semantics and the paths that follow from it.

    A mixin over the framework-bound attributes (``node_name``, ``job``), so it is only
    ever used on a kernel the executor has bound.
    """

    #: ``None`` = the dated default; anything else = used verbatim. ``DataArchiver`` gives
    #: ``""`` a third meaning (free mode) that is its own, not this scheme's.
    target: str | None = None

    @staticmethod
    def clean_target(target: str | None) -> str | None:
        return None if target is None else target.rstrip("/")

    @property
    def job_dir(self) -> str:
        """``{job_id}_{pipeline_hash}``: which work unit, and which definition produced it.

        The id alone is not enough to name a directory. A shard is retried under the same
        id by design (that is what makes retries idempotent), but a YAML that was *edited*
        between attempts is a different job in every sense that matters — different
        thresholds, possibly different kernels — and overwriting the earlier run's
        artifacts with it would destroy the evidence for a verdict someone has already
        read. Appending the definition's hash keeps retries overwriting and edits landing
        somewhere new, without a timestamp anywhere near the path.
        """
        job_id = self.job.get("job_id", "job")
        pipeline_hash = self.job.get("pipeline_hash")
        return f"{job_id}_{pipeline_hash}" if pipeline_hash else job_id

    @property
    def job_root(self) -> str:
        """``{target or 'dump'}/{job_id}_{pipeline_hash}`` — everything this job wrote.

        The job comes *first* in the path so that every run of a job lives under one
        directory: with the timestamp outermost, the runs of one shard were scattered
        across sibling trees and you could not list them.
        """
        base = DEFAULT_TARGET_ROOT if self.target is None else self.target
        return "/".join(p for p in (base, self.job_dir) if p)

    @property
    def run_root(self) -> str:
        """Where *this run's* artifacts go: ``{job_root}/{unix ts}`` by default.

        The timestamp lives in the default only, deliberately. Dump paths must be
        deterministic so a retried job overwrites rather than duplicates — so a pipeline
        that cares (anything an external splitter generates) sets ``target`` explicitly
        and keeps `f(job_id, pipeline_hash, node, key)`. A default run gets a subtree per
        run instead: nothing it writes can collide with an earlier one, at the price of
        directories accumulating until a human clears them.
        """
        if self.target is not None:
            return self.job_root
        return f"{self.job_root}/{self.job.get('started_unix') or 'unstamped'}"

    def path_for(self, name: str) -> str:
        """``{job_root}/[{unix ts}/]{node}/{name}`` — relative unless ``target`` is not."""
        return "/".join(p for p in (self.run_root, self.node_name, name) if p)


def flat_name(key: str) -> str:
    """One path segment out of a slashed union key or sample id — also a writer convention."""
    return key.replace("/", "__")


def write_stream(uri: str, src) -> str:
    """Copy an open file object to ``uri`` in chunks — a 2 GB video never lands in RAM.

    The one place the destination is opened, so ``makedirs``, the chunk size and the
    object-stores-have-no-directories quirk are decided once for every writer.
    """
    fs, path = fsspec.core.url_to_fs(uri)
    try:
        fs.makedirs(posixpath.dirname(path), exist_ok=True)
    except Exception:
        pass  # object stores have no directories
    with fs.open(path, "wb") as dst:
        shutil.copyfileobj(src, dst, COPY_CHUNK)
    return uri


def write_bytes(uri: str, data: bytes) -> str:
    return write_stream(uri, BytesIO(data))


def copy_file(local_path: str, uri: str) -> str:
    with open(local_path, "rb") as src:
        return write_stream(uri, src)
