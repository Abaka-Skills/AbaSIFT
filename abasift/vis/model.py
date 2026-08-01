"""Pipeline YAML -> a plain description of the DAG, read off the *live* classes.

Nothing here is hand-maintained. The node boxes, their parameters and their method
signatures all come from ``inspect`` on the very classes the YAML imports, so this
picture cannot drift from the code the way a drawn diagram does: rename a kernel's
parameter and the next render shows the new name, or the pipeline fails to load.

The output is a plain dict (JSON-safe), so the renderer stays a pure function of it and
the same description can be dumped for a diff.
"""

from __future__ import annotations

import inspect
import os
from typing import Any, Mapping

from ..kernel import Kernel, MutatingKernel, SampleKernel, SourceKernel
from ..pipeline import NodeSpec, Pipeline, import_kernel

#: Interface class -> (role, the methods that define it). Most specific first: every
#: kernel role is a ``Kernel`` subclass, so the first match wins.
#:
#: This is the one table in the package that a new *interface* would have to touch. A new
#: kernel, loader or vendor needs no change here at all.
ROLES: tuple[tuple[type, str, tuple[str, ...]], ...] = (
    (SourceKernel, "source", ("iter_batches", "digest")),
    (MutatingKernel, "mutating", ("run_mutating", "commit")),
    (SampleKernel, "sample", ("sift", "run", "digest")),
    (Kernel, "kernel", ("run", "digest")),
)

_MAX_VALUE_CHARS = 72


def describe(pipeline: Pipeline, yaml_path: str | None = None) -> dict:
    """Describe one pipeline: what it is, never what a run of it did."""
    layers = _layers(pipeline)
    nodes = [
        _describe_node(spec, layers[spec.name], is_source=spec.name == pipeline.source)
        for spec in pipeline.nodes
    ]
    return {
        "pipeline": {
            "job_id": pipeline.job_id,
            "hash": pipeline.hash(),
            "source": pipeline.source,
            "yaml": yaml_path,
            "n_nodes": len(pipeline.nodes),
            "order": pipeline.topo_order(),
        },
        "nodes": nodes,
        "edges": [{"from": i, "to": n.name} for n in pipeline.nodes for i in n.inputs],
        "n_layers": max(layers.values()) + 1 if layers else 0,
    }


def _layers(pipeline: Pipeline) -> dict[str, int]:
    """Longest path from the source: a node sits one column right of its deepest input."""
    layer: dict[str, int] = {}
    specs = {n.name: n for n in pipeline.nodes}
    for name in pipeline.topo_order():  # already dependency-ordered, so one pass suffices
        layer[name] = 1 + max((layer[i] for i in specs[name].inputs), default=-1)
    return layer


def _describe_node(spec: NodeSpec, layer: int, is_source: bool) -> dict:
    cls = import_kernel(spec.kernel)
    interface, role, methods = _role_of(cls)
    return {
        "name": spec.name,
        "kernel": spec.kernel,
        "class": cls.__name__,
        "module": cls.__module__,  # where the class is defined; the server watches it
        "role": role,
        "interface": interface.__name__,
        "is_source": is_source,
        "bases": [c.__name__ for c in cls.__mro__[1:] if issubclass(c, (Kernel, SourceKernel))],
        "source_file": _source_file(cls),
        "doc": _summary(cls),
        "inputs": list(spec.inputs),
        "layer": layer,
        "params": _params(cls, spec.params),
        "methods": [m for m in (_method(cls, name) for name in methods) if m],
    }


def _role_of(cls: type) -> tuple[type, str, tuple[str, ...]]:
    for interface, role, methods in ROLES:
        if issubclass(cls, interface):
            return interface, role, methods
    raise TypeError(f"{cls.__name__} is neither a Kernel nor a SourceKernel")


# -- signatures ----------------------------------------------------------------


def _method(cls: type, name: str) -> dict | None:
    """One row of the node's interface, with the class the implementation came from.

    Whether a method is *this kernel's own* or inherited from the base is the single most
    useful thing on the box: it says at a glance that ``ImuSpikeKernel`` only writes
    ``sift`` and gets the batch loop, the alive-filter and the per-sample failsafe from
    ``SampleKernel``.
    """
    fn = getattr(cls, name, None)
    if fn is None:
        return None
    owner = next((c for c in cls.__mro__ if name in vars(c)), cls)
    return {
        "name": name,
        "signature": _signature(fn),
        "owner": owner.__name__,
        "own": owner is cls,
        "doc": _summary(fn),
    }


def _signature(fn) -> str:
    """``(art: ArtifactUnion, report: ReportView) -> tuple[ArtifactExt, ReportExt]``.

    Built by hand rather than ``str(inspect.signature(...))`` because these modules use
    ``from __future__ import annotations``, which would render every annotation as a
    quoted string.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):  # C-implemented or otherwise unintrospectable
        return "(…)"
    parts = []
    for p in sig.parameters.values():
        if p.name == "self":
            continue
        text = {p.VAR_POSITIONAL: "*", p.VAR_KEYWORD: "**"}.get(p.kind, "") + p.name
        if p.annotation is not p.empty:
            text += f": {_annotation(p.annotation)}"
        if p.default is not p.empty:
            text += f" = {_value(p.default)}"
        parts.append(text)
    out = f"({', '.join(parts)})"
    if sig.return_annotation is not sig.empty:
        out += f" -> {_annotation(sig.return_annotation)}"
    return out


def _annotation(a: Any) -> str:
    if isinstance(a, str):
        return a  # PEP 563: already the source text
    return getattr(a, "__name__", None) or str(a).replace("typing.", "")


def _value(v: Any) -> str:
    text = repr(v)
    return text if len(text) <= _MAX_VALUE_CHARS else text[: _MAX_VALUE_CHARS - 1] + "…"


# -- parameters ----------------------------------------------------------------


def _params(cls: type, given: Mapping[str, Any]) -> list[dict]:
    """The constructor's declared parameters, marked with what this YAML supplied.

    Kernels declare every parameter explicitly (no ``**kwargs``), which is what makes this
    table exhaustive: what you see is every knob the node has.
    """
    try:
        sig = inspect.signature(cls.__init__)
    except (TypeError, ValueError):
        return []
    out = []
    for p in sig.parameters.values():
        if p.name == "self" or p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            continue
        out.append(
            {
                "name": p.name,
                "annotation": _annotation(p.annotation) if p.annotation is not p.empty else "",
                "default": _value(p.default) if p.default is not p.empty else "",
                "required": p.default is p.empty,
                "given": p.name in given,
                "value": _value(given[p.name]) if p.name in given else "",
            }
        )
    return out


# -- provenance ----------------------------------------------------------------


def _source_file(cls: type) -> str:
    try:
        path = inspect.getsourcefile(cls) or ""
        line = inspect.getsourcelines(cls)[1]
    except (OSError, TypeError):
        return ""
    return f"{os.path.relpath(path)}:{line}" if path else ""


def _summary(obj: Any) -> str:
    """First line of the docstring — the module's for a class, since that is the prose.

    A kernel class docstring is its ``Params:`` block (already rendered as the parameter
    table), while the module docstring opens with the one-line statement of what the
    kernel is for. That is the line worth putting on the box.
    """
    doc = ""
    if isinstance(obj, type):
        module = inspect.getmodule(obj)
        doc = (module.__doc__ or "") if module else ""
    if not doc.strip():
        doc = obj.__doc__ or ""
    first = doc.strip().split("\n\n")[0].strip()
    return " ".join(first.split()).replace("``", "")
