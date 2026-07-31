"""``LazyRaw`` — the lazy stream handle, and the decoder registry.

A ``LazyRaw`` is *just* ``uri + decoder name + options``: fully serializable, cheap to
enumerate by the million, and nothing is fetched until a kernel asks. A pipeline that
only checks IMU never reads a video frame.

Three ways to get at the payload; a decoder picks the one that fits, and picking right
is the difference between 0.5 MB and 2 GB of traffic:

======================  ============================================================
``open()``              remote random-access file object (fsspec). Ranged GETs only.
                        Right for container headers/metadata: a duration probe on a
                        178 MB DJI file reads 0.45 MB (0.25%).
``local_path()``        stream the whole object into the worker disk cache once and
                        return a path. Right when the payload is genuinely needed
                        throughout the file (demuxing an interleaved telemetry track
                        touches ~100% of the bytes anyway) or when the tool wants a
                        real file. Bytes never pass through RAM in one piece.
``read_bytes()``        the whole object in memory. Small sidecars only; refuses
                        anything over ``max_bytes`` so nobody accidentally pulls 2 GB.
======================  ============================================================

Tier 2 of the cache is here: ``decode()`` memoizes the decoded object on the instance
under a lock (parallel DAG branches over one sample decode once), and the executor
calls ``release()`` when the batch's DAG run finishes.
"""

from __future__ import annotations

import threading
from typing import Any, Callable

import fsspec

from .errors import DecodeError

_MISSING = object()

#: Decoder name -> callable. Populated by :mod:`abasift.decoders` (and by vendor code).
DECODERS: dict[str, Callable[["LazyRaw"], Any]] = {}

DEFAULT_MAX_BYTES = 64 * 2**20


def register_decoder(name: str) -> Callable[[Callable[["LazyRaw"], Any]], Callable]:
    """Decorator: register a decoder under a serializable name."""

    def deco(fn):
        if name in DECODERS and DECODERS[name] is not fn:
            raise ValueError(f"decoder {name!r} already registered")
        DECODERS[name] = fn
        return fn

    return deco


def get_decoder(name: str) -> Callable[["LazyRaw"], Any]:
    try:
        return DECODERS[name]
    except KeyError:
        raise DecodeError(
            f"unknown decoder {name!r}; registered: {sorted(DECODERS)}"
        ) from None


class LazyRaw:
    """Handle on one remote-or-local byte stream plus how to interpret it."""

    __slots__ = ("uri", "decoder", "opts", "_lock", "_memo")

    def __init__(self, uri: str, decoder: str, **opts: Any):
        self.uri = str(uri)
        self.decoder = decoder
        self.opts = dict(opts)
        self._lock = threading.Lock()
        self._memo: Any = _MISSING

    # -- identity: value semantics, so diamond joins merge silently ------

    def _ident(self):
        return (self.uri, self.decoder, tuple(sorted(self.opts.items())))

    def __eq__(self, other):
        return isinstance(other, LazyRaw) and self._ident() == other._ident()

    def __hash__(self):
        return hash(self._ident())

    def __repr__(self):
        extra = f", {self.opts}" if self.opts else ""
        return f"LazyRaw({self.uri!r}, {self.decoder!r}{extra})"

    # -- serialization ---------------------------------------------------

    def to_json(self) -> dict:
        return {"__lazyraw__": 1, "uri": self.uri, "decoder": self.decoder, "opts": self.opts}

    @classmethod
    def from_json(cls, d: dict) -> "LazyRaw":
        return cls(d["uri"], d["decoder"], **d.get("opts", {}))

    # -- access paths ----------------------------------------------------

    def filesystem(self):
        """``(fs, path)`` for this URI. Credentials come from the standard AWS chain."""
        return fsspec.core.url_to_fs(self.uri)

    def open(self, block_size: int | None = None):
        """Seekable remote file object. Reads only the ranges actually touched."""
        fs, path = self.filesystem()
        try:
            return fs.open(path, "rb", block_size=block_size) if block_size else fs.open(path, "rb")
        except Exception as e:
            raise DecodeError(f"cannot open {self.uri}: {e}") from e

    def size(self) -> int:
        fs, path = self.filesystem()
        try:
            return int(fs.size(path))
        except Exception as e:
            raise DecodeError(f"cannot stat {self.uri}: {e}") from e

    def local_path(self) -> str:
        """Path to a local copy, downloading into the worker disk cache on first use.

        Local URIs are used in place — no pointless copy.
        """
        from .cache import disk_cache  # local import: cache is a heavier module

        fs, path = self.filesystem()
        if getattr(fs, "protocol", None) in ("file", ("file", "local")) or fs.__class__.__name__ == "LocalFileSystem":
            return path
        try:
            return str(disk_cache().materialize(self.uri, lambda dst: fs.get_file(path, dst)))
        except Exception as e:
            raise DecodeError(f"cannot materialize {self.uri}: {e}") from e

    def read_bytes(self, max_bytes: int | None = DEFAULT_MAX_BYTES) -> bytes:
        """Whole object in memory. Guarded: refuses anything larger than ``max_bytes``."""
        fs, path = self.filesystem()
        try:
            if max_bytes is not None:
                size = int(fs.size(path))
                if size > max_bytes:
                    raise DecodeError(
                        f"{self.uri} is {size / 2**20:.1f} MiB > max_bytes "
                        f"{max_bytes / 2**20:.1f} MiB; use local_path() or open() instead"
                    )
            return fs.cat_file(path)
        except DecodeError:
            raise
        except Exception as e:
            raise DecodeError(f"cannot read {self.uri}: {e}") from e

    # -- decode ----------------------------------------------------------

    def decode(self) -> Any:
        """Decoded payload, memoized for this batch run. Raises ``DecodeError``."""
        with self._lock:
            if self._memo is _MISSING:
                fn = get_decoder(self.decoder)
                try:
                    self._memo = fn(self)
                except DecodeError:
                    raise
                except Exception as e:
                    raise DecodeError(f"{self.decoder} failed on {self.uri}: {e}") from e
            return self._memo

    def release(self) -> None:
        """Drop the in-memory decoded object. Called by the executor per batch run."""
        with self._lock:
            self._memo = _MISSING

    @property
    def is_decoded(self) -> bool:
        return self._memo is not _MISSING
