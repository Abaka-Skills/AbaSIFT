"""``Sample``, ``Batch``, ``ArtifactUnion`` — what flows through the DAG."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping

from .errors import ExecutorError, PipelineError
from .lazy import LazyRaw

#: Canonical stream-name prefixes. A stream is named ``"{kind}/{name}"`` — the whole job
#: of a per-vendor loader is mapping that vendor's directory layout onto these names, so
#: a kernel can ask for ``imu/main`` without knowing anything about the vendor.
STREAM_KINDS = frozenset({"video", "image", "audio", "imu", "annotation", "blob"})


@dataclass(frozen=True)
class Sample:
    """One QC atom: an id, a bag of named streams, and vendor metadata."""

    sample_id: str
    streams: Mapping[str, LazyRaw]
    meta: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.sample_id:
            raise PipelineError("sample_id must be non-empty")
        for name in self.streams:
            kind, _, rest = name.partition("/")
            if kind not in STREAM_KINDS or not rest:
                raise PipelineError(
                    f"stream {name!r} on sample {self.sample_id!r} is not "
                    f"'kind/name' with kind in {sorted(STREAM_KINDS)}"
                )

    def stream(self, name: str) -> LazyRaw:
        """The named stream, or a ``KeyError`` that says what the sample actually has."""
        try:
            return self.streams[name]
        except KeyError:
            raise KeyError(
                f"sample {self.sample_id!r} has no stream {name!r}; has {sorted(self.streams)}"
            ) from None

    def release(self) -> None:
        for s in self.streams.values():
            s.release()


@dataclass(frozen=True)
class Batch:
    """A list of samples. Efficiency unit only — report and artifacts stay per-sample."""

    samples: tuple[Sample, ...]
    index: int = 0

    def __post_init__(self):
        object.__setattr__(self, "samples", tuple(self.samples))

    def __iter__(self) -> Iterator[Sample]:
        return iter(self.samples)

    def __len__(self) -> int:
        return len(self.samples)

    @property
    def ids(self) -> tuple[str, ...]:
        return tuple(s.sample_id for s in self.samples)

    def release(self) -> None:
        """Drop every decoded payload in this batch (end of its DAG run)."""
        for s in self.samples:
            s.release()


class ArtifactUnion:
    """Flat ``"{node}/{name}" -> value`` map. Extend-only; the executor owns merging.

    Immutable by construction: ``extended`` / ``union`` / ``with_mutations`` all return a
    new instance, so concurrent DAG branches can never see a half-written union.

    ``deleted`` records keys a ``DataDumper`` removed, so a delete is not resurrected by
    a later join with a branch that still carries the key.
    """

    __slots__ = ("_data", "_deleted")

    def __init__(self, data: Mapping[str, Any] | None = None, deleted: frozenset[str] = frozenset()):
        self._data: dict[str, Any] = dict(data or {})
        self._deleted = frozenset(deleted)

    # -- read ------------------------------------------------------------

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"ArtifactUnion({sorted(self._data)})"

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def keys(self):
        return self._data.keys()

    def items(self):
        return self._data.items()

    @property
    def deleted(self) -> frozenset[str]:
        return self._deleted

    def under(self, node: str) -> dict[str, Any]:
        """Every artifact in one node's namespace, keyed without the ``"{node}/"`` prefix."""
        p = node + "/"
        return {k[len(p):]: v for k, v in self._data.items() if k.startswith(p)}

    def per_sample(self, node: str, name: str) -> dict[str, Any]:
        """A node's per-sample artifacts (``"{name}/{sample_id}"``), keyed by sample id.

        The mirror of the convention :meth:`SampleKernel.check` writes, so a ``finalize()``
        reduce reads its own artifacts back without re-deriving the prefix surgery.
        """
        prefix = f"{name}/"
        return {k[len(prefix):]: v for k, v in self.under(node).items() if k.startswith(prefix)}

    def find_batch(self) -> Batch | None:
        """The ``Batch`` travelling in this union, or ``None`` if there is none."""
        found = [(k, v) for k, v in self._data.items() if isinstance(v, Batch)]
        if len(found) > 1:
            raise ExecutorError(f"ambiguous batch: {sorted(k for k, _ in found)}")
        return found[0][1] if found else None

    def batch(self) -> Batch:
        """The ``Batch`` travelling in this union (node 0's artifact). Raises if absent."""
        batch = self.find_batch()
        if batch is None:
            raise ExecutorError(
                "no Batch in the artifact union — is this node downstream of the source node?"
            )
        return batch

    # -- write (each returns a new union) --------------------------------

    def extended(self, node: str, ext: Mapping[str, Any]) -> "ArtifactUnion":
        """Add one node's artifacts under its namespace. Overwriting is an error."""
        if not ext:
            return self
        data = dict(self._data)
        for name, value in ext.items():
            key = f"{node}/{name}"
            if key in data:
                raise ExecutorError(f"node {node!r} would overwrite existing artifact {key!r}")
            data[key] = value
        return ArtifactUnion(data, self._deleted)

    def union(self, other: "ArtifactUnion") -> "ArtifactUnion":
        """Key-wise union of two unions. Duplicate keys must agree (diamond topology)."""
        data = dict(self._data)
        for key, value in other._data.items():
            if key in data and not _same(data[key], value):
                raise ExecutorError(
                    f"artifact key {key!r} arrived with two different values; "
                    "per-batch aggregates belong in finalize(), not run()"
                )
            data[key] = value
        deleted = self._deleted | other._deleted
        for key in deleted:
            data.pop(key, None)
        return ArtifactUnion(data, deleted)

    def with_mutations(
        self, replace: Mapping[str, Any] = (), delete: frozenset[str] = frozenset()
    ) -> "ArtifactUnion":
        """Sanctioned mutation — ``DataDumper`` only (dump->LazyRaw swap, or free).

        Ordinary kernels must never call this; they return extensions.
        """
        data = dict(self._data)
        for key, value in dict(replace).items():
            if key not in data:
                raise ExecutorError(f"cannot replace absent artifact {key!r}")
            data[key] = value
        for key in delete:
            data.pop(key, None)
        return ArtifactUnion(data, self._deleted | frozenset(delete))

    def without_transients(self) -> "ArtifactUnion":
        """Drop live-object artifacts (the ``Batch``) before merging into the job union.

        Union values are meant to be primitives or ``LazyRaw``. The batch is the one
        live object, and it is per-batch-run scoped: keeping it would both pin decoded
        memory and collide across batches on the same key.
        """
        return ArtifactUnion(
            {k: v for k, v in self._data.items() if not isinstance(v, Batch)}, self._deleted
        )

    def to_json(self) -> dict:
        return {k: jsonable(v) for k, v in self._data.items()}


def _same(a: Any, b: Any) -> bool:
    if a is b:
        return True
    try:
        return bool(a == b)
    except Exception:  # numpy arrays and friends: only identity counts
        return False


def jsonable(v: Any) -> Any:
    """Best-effort JSON projection of an artifact value. Used by the union and the dumper."""
    if isinstance(v, LazyRaw):
        return v.to_json()
    if isinstance(v, Batch):
        return {"__batch__": v.index, "sample_ids": list(v.ids)}
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, Mapping):
        return {str(k): jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    to_json = getattr(v, "to_json", None)
    if callable(to_json):
        return to_json()
    return repr(v)
