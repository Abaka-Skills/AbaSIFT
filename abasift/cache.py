"""Worker-global disk scratch: the S3 -> local-disk bridge (tier 1 of the two-tier cache).

Why disk at all: vendor videos here run 70 MB - 2 GB. Holding raw bytes in memory is
never an option, and some work genuinely needs a *file* (PyAV/ffmpeg seeking, external
CLI tools). So a kernel that needs the payload asks ``LazyRaw.local_path()``, which
streams the object down once (chunked, never fully in RAM) and hands back a path.

Keyed by URI, size-capped LRU, shared by every thread in the worker, so N kernels
reading the same video download it once. Eviction is by least-recently-used and
happens on insert; on POSIX an already-open file stays readable after unlink, so
evicting a file a kernel is mid-read on is safe.

Config: ``ABASIFT_CACHE_DIR`` (default ``$TMPDIR/abasift-cache``), ``ABASIFT_CACHE_GB``
(default 32).
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import threading
from pathlib import Path, PurePosixPath
from typing import Callable

_PART = ".part-"


class DiskCache:
    def __init__(self, root: str | os.PathLike | None = None, capacity_bytes: int | None = None):
        self.root = Path(
            root
            or os.environ.get("ABASIFT_CACHE_DIR")
            or Path(tempfile.gettempdir()) / "abasift-cache"
        )
        if capacity_bytes is None:
            capacity_bytes = int(float(os.environ.get("ABASIFT_CACHE_GB", "32")) * 2**30)
        self.capacity_bytes = capacity_bytes
        self.root.mkdir(parents=True, exist_ok=True)
        self._guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}

    # -- keys -------------------------------------------------------------

    @staticmethod
    def key_for(uri: str) -> str:
        """Deterministic filename for a URI; suffix kept so ffmpeg can sniff the format."""
        stem = hashlib.sha256(uri.encode()).hexdigest()[:32]
        suffix = PurePosixPath(uri.split("?", 1)[0]).suffix
        if len(suffix) > 8 or not suffix.isascii():
            suffix = ""
        return stem + suffix

    def _lock_for(self, key: str) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(key, threading.Lock())

    # -- the one operation ------------------------------------------------

    def materialize(self, uri: str, fetch: Callable[[str], None]) -> Path:
        """Return a local path holding ``uri``, downloading via ``fetch(dst_path)`` on miss.

        ``fetch`` must write the whole object to the path it is given. Download goes to
        a ``.part-*`` sibling and is atomically renamed, so a crashed or concurrent
        worker can never expose a truncated file and retries are idempotent.
        """
        key = self.key_for(uri)
        path = self.root / key
        if path.exists():
            _touch(path)
            return path
        with self._lock_for(key):
            if path.exists():  # another thread won the race
                _touch(path)
                return path
            part = self.root / f"{key}{_PART}{os.getpid()}-{threading.get_ident()}"
            try:
                fetch(str(part))
                os.replace(part, path)
            finally:
                if part.exists():
                    part.unlink(missing_ok=True)
        self.evict()
        return path

    def evict(self) -> int:
        """Drop least-recently-used entries until under capacity. Returns bytes freed."""
        entries = []
        total = 0
        for p in self.root.iterdir():
            if _PART in p.name or not p.is_file():
                continue
            try:
                st = p.stat()
            except OSError:
                continue
            entries.append((st.st_mtime, st.st_size, p))
            total += st.st_size
        freed = 0
        if total <= self.capacity_bytes:
            return 0
        for _mtime, size, p in sorted(entries):
            if total - freed <= self.capacity_bytes:
                break
            try:
                p.unlink()
                freed += size
            except OSError:
                pass
        return freed

    def forget(self, uri: str) -> bool:
        """Drop this URI's cached copy; ``True`` if a file was removed.

        Freeing an artifact goes through here rather than deriving the path at the call
        site, so the key scheme and the "never delete outside the cache root" guarantee
        stay in one place. Two kinds of URI arrive: one that was *materialized* here (keyed
        by hash), and one that already names an entry — a kernel that writes its own entry
        (``VideoFrameKernel``) hands back the ``file://`` path of the entry itself.
        """
        local = _as_local_path(uri)
        path = local if local is not None else self.root / self.key_for(uri)
        try:
            if path.parent.resolve() != self.root.resolve():
                return False
            path.unlink()
            return True
        except OSError:
            return False


def _as_local_path(uri: str) -> Path | None:
    """The path a ``file://`` URI names, or ``None`` for anything remote.

    Plain slicing, deliberately: fsspec does not percent-decode either, so a URI built by
    encoding (``Path.as_uri()``) would be a path fsspec cannot open. One spelling on both
    sides — see :func:`abasift.kernels.frames.file_uri`.
    """
    if uri.startswith("file://"):
        return Path(uri[len("file://") :])
    return None


def _touch(path: Path) -> None:
    """Mark as recently used. mtime is the LRU clock (atime is unreliable under relatime)."""
    try:
        os.utime(path, None)
    except OSError:
        pass


_default: DiskCache | None = None
_default_guard = threading.Lock()


def disk_cache() -> DiskCache:
    """The worker-global instance. One per process; threads share it."""
    global _default
    if _default is None:
        with _default_guard:
            if _default is None:
                _default = DiskCache()
    return _default


def set_disk_cache(cache: DiskCache | None) -> None:
    """Override the global cache (tests, and :func:`configure_cache`)."""
    global _default
    with _default_guard:
        _default = cache


def configure_cache(dir: str | None = None, size_gb: float | None = None) -> DiskCache:
    """Install the worker cache a job asked for, and return it.

    Precedence is **YAML > environment > built-in default**: an explicit setting in the
    pipeline wins because it is the more specific statement of intent, and whatever it
    leaves out falls through to ``ABASIFT_CACHE_DIR`` / ``ABASIFT_CACHE_GB`` and then to
    ``$TMPDIR/abasift-cache`` at 32 GiB — which is exactly what ``DiskCache()`` already
    resolves for a ``None``.

    A pipeline that says nothing about the cache must not call this at all: replacing the
    global would trample a cache someone else (an embedding process, a test fixture)
    deliberately installed.
    """
    cache = DiskCache(root=dir, capacity_bytes=None if size_gb is None else int(size_gb * 2**30))
    set_disk_cache(cache)
    return cache
