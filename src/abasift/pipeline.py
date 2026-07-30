"""Pipeline YAML -> validated DAG. One YAML fully describes one job.

Everything here fails fast: bad edges, cycles, unimportable kernels and missing source
nodes are all caught before a single byte of vendor data is read.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

from .errors import PipelineError
from .kernel import Kernel, SourceKernel


@dataclass(frozen=True)
class NodeSpec:
    name: str
    kernel: str  # dotted import path
    params: Mapping[str, Any] = field(default_factory=dict)
    inputs: tuple[str, ...] = ()


@dataclass(frozen=True)
class Pipeline:
    name: str
    nodes: tuple[NodeSpec, ...]
    job_id: str = ""
    source: str = ""  # name of node 0, filled by validate()

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
        return cls.from_dict(doc["pipeline"])

    @classmethod
    def from_dict(cls, spec: Mapping[str, Any]) -> "Pipeline":
        name = spec.get("name")
        if not name:
            raise PipelineError("pipeline.name is required")
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
        p = cls(name=str(name), nodes=tuple(nodes), job_id=str(spec.get("job_id") or name))
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

        p = Pipeline(self.name, self.nodes, self.job_id, source.name)
        p.topo_order()  # raises on cycles
        return p

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

    def node(self, name: str) -> NodeSpec:
        for n in self.nodes:
            if n.name == name:
                return n
        raise KeyError(name)

    def consumers(self, name: str) -> tuple[str, ...]:
        return tuple(n.name for n in self.nodes if name in n.inputs)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "job_id": self.job_id,
            "nodes": [
                {"name": n.name, "kernel": n.kernel, "params": dict(n.params), "inputs": list(n.inputs)}
                for n in self.nodes
            ],
        }

    def hash(self) -> str:
        """Stable digest of the definition — recorded in the report for traceability."""
        canonical = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    def instantiate(self) -> dict[str, Kernel | SourceKernel]:
        out: dict[str, Kernel | SourceKernel] = {}
        for n in self.nodes:
            cls = import_kernel(n.kernel)
            try:
                kernel = cls(**dict(n.params))
            except TypeError as e:
                raise PipelineError(f"node {n.name!r}: bad params for {n.kernel}: {e}") from e
            kernel.params = dict(n.params)
            out[n.name] = kernel
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
