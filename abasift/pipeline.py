"""Pipeline YAML -> validated DAG. One YAML fully describes one job.

Everything here fails fast: bad edges, cycles, unimportable kernels and missing source
nodes are all caught before a single byte of vendor data is read.
"""

from __future__ import annotations

import fnmatch
import hashlib
import importlib
import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping

import yaml

from .errors import PipelineError
from .kernel import Kernel, MutatingKernel, SourceKernel


@dataclass(frozen=True)
class NodeSpec:
    name: str
    kernel: str  # dotted import path
    params: Mapping[str, Any] = field(default_factory=dict)
    inputs: tuple[str, ...] = ()


#: Everything a ``pipeline:`` block may contain. Strict, like kernel params: a typo — or
#: a leftover key from an older format — is a load-time error, not a silent no-op.
PIPELINE_KEYS = {"job_id", "nodes", "cache"}

#: Worker infrastructure a YAML may set. Strict, like kernel params: a typo is a
#: load-time error, not a job that silently runs against the defaults.
CACHE_KEYS = {"dir", "size_gb"}


@dataclass(frozen=True)
class Pipeline:
    #: The one name a job has. It names the *work unit*, so it is what dump paths are
    #: keyed on; the definition is identified by :meth:`hash`, not by a label.
    job_id: str
    nodes: tuple[NodeSpec, ...]
    source: str = ""  # name of node 0, filled by validate()
    #: ``{}`` means "leave the worker's cache alone" — env, then the built-in default.
    cache: Mapping[str, Any] = field(default_factory=dict)
    #: Where this was loaded from, when it came from a file. Machine-specific, so it is
    #: deliberately outside ``to_dict()`` and cannot affect the hash.
    path: str = ""

    # -- construction ----------------------------------------------------

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Pipeline":
        text = Path(path).read_text()
        try:
            doc = yaml.safe_load(text)
        except yaml.YAMLError as e:
            raise PipelineError(f"{path}: invalid YAML: {e}") from e
        if not isinstance(doc, Mapping) or "pipeline" not in doc:
            raise PipelineError(f"{path}: expected a top-level 'pipeline:' mapping")
        return replace(cls.from_dict(doc["pipeline"]), path=str(path))

    @classmethod
    def from_dict(cls, spec: Mapping[str, Any]) -> "Pipeline":
        stray = set(spec) - PIPELINE_KEYS
        if stray:
            raise PipelineError(f"pipeline has unknown keys {sorted(stray)}; expected {sorted(PIPELINE_KEYS)}")
        job_id = spec.get("job_id")
        if not job_id:
            raise PipelineError("pipeline.job_id is required")
        raw_nodes = spec.get("nodes") or []
        if not raw_nodes:
            raise PipelineError("pipeline.nodes is empty")
        nodes = []
        for i, n in enumerate(raw_nodes):
            if not isinstance(n, Mapping):
                raise PipelineError(f"node #{i} is not a mapping")
            missing = [k for k in ("name", "kernel") if not n.get(k)]
            if missing:
                raise PipelineError(f"node #{i} is missing {missing}")
            unknown = set(n) - {"name", "kernel", "params", "inputs"}
            if unknown:
                raise PipelineError(f"node {n['name']!r} has unknown keys {sorted(unknown)}")
            nodes.append(
                NodeSpec(
                    name=str(n["name"]),
                    kernel=str(n["kernel"]),
                    params=dict(n.get("params") or {}),
                    inputs=tuple(n.get("inputs") or ()),
                )
            )
        p = cls(job_id=str(job_id), nodes=tuple(nodes), cache=_cache_spec(spec.get("cache")))
        return p.validated()

    # -- validation ------------------------------------------------------

    def validated(self) -> "Pipeline":
        names = [n.name for n in self.nodes]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise PipelineError(f"duplicate node names: {sorted(dupes)}")
        known = set(names)
        for n in self.nodes:
            bad = [i for i in n.inputs if i not in known]
            if bad:
                raise PipelineError(f"node {n.name!r} has unknown inputs {bad}")
            if n.name in n.inputs:
                raise PipelineError(f"node {n.name!r} takes itself as input")

        sources = [n for n in self.nodes if not n.inputs]
        if len(sources) != 1:
            raise PipelineError(
                f"expected exactly one source node (inputs: []), found {[n.name for n in sources]}"
            )
        source = sources[0]

        for n in self.nodes:
            cls = import_kernel(n.kernel)
            is_source = issubclass(cls, SourceKernel)
            if n is source and not is_source:
                raise PipelineError(f"source node {n.name!r}: {n.kernel} is not a SourceKernel")
            if n is not source and not issubclass(cls, Kernel):
                raise PipelineError(f"node {n.name!r}: {n.kernel} is not a Kernel")
            if n is not source and is_source:
                raise PipelineError(f"node {n.name!r} is a SourceKernel but has inputs")

        # `replace`, not a positional rebuild: every field added to Pipeline has to
        # survive validation, and listing them here is how they get quietly dropped.
        p = replace(self, source=source.name)
        p.topo_order()  # raises on cycles
        p._check_replacement_conflicts()
        return p

    def _check_replacement_conflicts(self) -> None:
        """Reject two nodes replacing one key on branches that never meet in order.

        A replacement only reaches the job union because the node that made it is
        downstream of the node whose value it replaced. Two of them side by side hand the
        merge one key with two values and no rule for choosing — an `ExecutorError`
        halfway through a job, for a mistake that is visible in the YAML. So it is a load
        error, and the message says both fixes.
        """
        mutators = []
        for n in self.nodes:
            kernel = import_kernel(n.kernel)
            if not issubclass(kernel, MutatingKernel):
                continue
            patterns = _instantiate(n).replaced_key_patterns()
            if patterns:
                mutators.append((n.name, patterns))

        for i, (a, a_pats) in enumerate(mutators):
            for b, b_pats in mutators[i + 1 :]:
                if b in self.ancestors(a) or a in self.ancestors(b):
                    continue  # ordered: the later one replaces what the earlier one wrote
                clash = sorted({p for p in a_pats for q in b_pats if _globs_overlap(p, q)})
                if clash:
                    raise PipelineError(
                        f"nodes {a!r} and {b!r} both archive {clash} on parallel branches; "
                        "put one downstream of the other, or give them disjoint keys"
                    )

    def ancestors(self, name: str) -> set[str]:
        """Every node ``name`` transitively depends on."""
        deps = {n.name: tuple(n.inputs) for n in self.nodes}
        seen: set[str] = set()
        stack = list(deps.get(name, ()))
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            stack.extend(deps.get(current, ()))
        return seen

    def topo_order(self) -> list[str]:
        """Node names in dependency order; YAML order breaks ties, so runs are stable."""
        deps = {n.name: set(n.inputs) for n in self.nodes}
        order: list[str] = []
        remaining = [n.name for n in self.nodes]
        while remaining:
            ready = [n for n in remaining if deps[n] <= set(order)]
            if not ready:
                raise PipelineError(f"cycle among nodes {sorted(remaining)}")
            order.extend(ready)
            remaining = [n for n in remaining if n not in set(order)]
        return order

    # -- accessors -------------------------------------------------------

    def to_dict(self) -> dict:
        out = {
            "job_id": self.job_id,
            "nodes": [
                {"name": n.name, "kernel": n.kernel, "params": dict(n.params), "inputs": list(n.inputs)}
                for n in self.nodes
            ],
        }
        if self.cache:  # absent when unset, so a YAML that says nothing hashes as before
            out["cache"] = dict(self.cache)
        return out

    def hash(self) -> str:
        """Stable hash of the definition — ``job.pipeline_hash``, for traceability."""
        canonical = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    def instantiate(self) -> dict[str, Kernel | SourceKernel]:
        return {n.name: _instantiate(n) for n in self.nodes}


def _instantiate(node: NodeSpec) -> Kernel | SourceKernel:
    """One node's kernel, with its YAML params. A bad param is a load error, not a crash."""
    cls = import_kernel(node.kernel)
    try:
        kernel = cls(**dict(node.params))
    except TypeError as e:
        raise PipelineError(f"node {node.name!r}: bad params for {node.kernel}: {e}") from e
    kernel.params = dict(node.params)
    return kernel


def _globs_overlap(a: str, b: str) -> bool:
    """Whether two key globs can match a common key — conservatively, and cheaply.

    Exact equality, or one pattern matching the other as a literal. That covers what
    authors actually write (`dur/*` beside `dur/duration_s/*`); glob intersection in
    general is not decidable by inspection, and a validator that guessed would be worse
    than one that says what it checked.
    """
    return a == b or fnmatch.fnmatchcase(a, b) or fnmatch.fnmatchcase(b, a)


def _cache_spec(raw: Any) -> dict[str, Any]:
    """Validate ``pipeline.cache``. Absent is fine and means "don't touch the worker's".

    Only the *size and location* of the scratch disk are configurable here — this is
    infrastructure, not credentials, which never appear in a YAML (design §7).
    """
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise PipelineError("pipeline.cache must be a mapping, e.g. {dir: /scratch, size_gb: 64}")
    unknown = set(raw) - CACHE_KEYS
    if unknown:
        raise PipelineError(f"pipeline.cache has unknown keys {sorted(unknown)}; expected {sorted(CACHE_KEYS)}")
    out: dict[str, Any] = {}
    if raw.get("dir"):
        out["dir"] = str(raw["dir"])
    if raw.get("size_gb") is not None:
        try:
            out["size_gb"] = float(raw["size_gb"])
        except (TypeError, ValueError):
            raise PipelineError(f"pipeline.cache.size_gb must be a number, got {raw['size_gb']!r}") from None
        if out["size_gb"] <= 0:
            raise PipelineError(f"pipeline.cache.size_gb must be positive, got {out['size_gb']}")
    return out


def import_kernel(path: str):
    """Resolve a dotted import path to a class. No registry, no magic."""
    if "." not in path:
        raise PipelineError(f"kernel {path!r} must be a dotted import path")
    module_path, _, attr = path.rpartition(".")
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise PipelineError(f"cannot import module {module_path!r} for kernel {path!r}: {e}") from e
    try:
        cls = getattr(module, attr)
    except AttributeError:
        raise PipelineError(f"{module_path!r} has no attribute {attr!r}") from None
    if not isinstance(cls, type) or not issubclass(cls, (Kernel, SourceKernel)):
        raise PipelineError(f"{path!r} is not a Kernel or SourceKernel subclass")
    return cls
