"""What a job prints around the work: the infrastructure and DAG before, the verdicts after.

Two questions a worker log should answer without anyone opening the YAML — *where is this
writing and how much disk may it eat*, and *what is it about to run*. Both are read off
the objects the job will actually use (the resolved `DiskCache`, the validated
`Pipeline`), never re-derived, so the banner cannot promise one thing and do another. The
closing tallies are read the same way, off the pipeline document the job is about to
write, so the terminal and the JSON cannot disagree either.

The graph is one node per line in dependency order, ``name[KernelClass] ← its inputs``,
with the same edges drawn as rails down a left-hand gutter. Lanes are allocated the way a
commit graph does it — a lane lives from a node's row to its last child's row and is then
reused — so the gutter is as wide as the edges *in flight*, never as wide as the pipeline.
The rails are the shape at a glance and the ``←`` names are the detail; both are generated
from the same inputs, so they cannot disagree.
"""

from __future__ import annotations

import json
import sys

from ..cache import DiskCache
from ..pipeline import Pipeline
from .term import color_enabled, paint

_LABEL = 14  # width of the left-hand key column

#: Status -> style. Same four everywhere: a verdict reads the same in a tally as in a log.
_STATUS_STYLE = {"pass": ("green",), "warn": ("yellow",), "fail": ("red",), "error": ("red", "bold")}
_STATUS_ORDER = ("pass", "warn", "fail", "error")


def run_banner(
    pipeline: Pipeline, cache: DiskCache, max_workers: int, yaml_path: str = "", color: bool | None = None
) -> str:
    on = color_enabled(sys.stdout) if color is None else color
    return "\n".join(
        [
            "",
            paint(f"abasift · {pipeline.job_id}", "bold", enabled=on),
            *_infra(pipeline, cache, max_workers, yaml_path, on),
            "",
            *_framed(_graph(pipeline), "dag", on),
            "",
        ]
    )


def run_results(document: dict, color: bool | None = None) -> str:
    """What the job did, per node, from the pipeline document it is about to write.

    The job line already says how many samples failed; this says *where*. Each node that
    judged anything names its checks and how each one went, then whatever its ``digest()``
    reduced. Verdicts only — the thresholds behind them are configuration, and live in the
    YAML and in this same document.
    """
    on = color_enabled(sys.stdout) if color is None else color
    out: list[str] = []
    for node in document.get("nodes", []):
        counts, checks, summary = node.get("counts"), node.get("checks", {}), node.get("summary")
        if not counts and not summary:
            continue  # a loader or a writer judges nothing and reduces nothing
        head = paint(node["name"], "bold", enabled=on) + _dim("[", on)
        head += paint(_short(node["kernel"]), "cyan", enabled=on) + _dim("]", on)
        out.append(head)
        # The node's own tally is the worst-of across its checks, which on the common
        # single-check node is the check's tally said twice. The thresholds it judged
        # against are in the YAML and in the pipeline document; the terminal wants the
        # verdicts, not the configuration.
        for name, check in checks.items():
            out.append(f"  {name}  {_tally(check['counts'], on)}")
        if summary:
            body = json.dumps(summary, indent=2, sort_keys=True).replace("\n", "\n  ")
            out.append(f"  {_dim('summary:', on)} {body}")

    job = document.get("job", {})
    if job:
        # The headline keeps its zeros: worker logs are grepped for `fail=0` as often as
        # they are read, and an absent key is not the same claim as a zero one.
        out.append(
            f"{job['n_samples']} samples in {job['n_batches']} batches, {job['elapsed_s']}s "
            f"— {_tally(job['counts'], on, zeros=True)}"
        )
    return "\n".join(out)


def _tally(counts: dict[str, int], on: bool, zeros: bool = False) -> str:
    """``pass=2 fail=1`` — per node, a status that never happened is noise; on the job
    line it is a fact worth stating, so ``zeros`` keeps it.

    A zero is dim whatever its status: `error=0` in alarm red is a lie told in colour.
    """
    out = []
    for status in _STATUS_ORDER:
        tally = counts.get(status, 0)
        if not tally and not zeros:
            continue
        out.append(paint(f"{status}={tally}", *(_STATUS_STYLE[status] if tally else ("dim",)), enabled=on))
    return " ".join(out)


def _dim(text: str, on: bool) -> str:
    return paint(text, "dim", enabled=on)


def _infra(pipeline: Pipeline, cache: DiskCache, max_workers: int, yaml_path: str, on: bool) -> list[str]:
    source = "yaml" if pipeline.cache else "default"  # env still shows through DiskCache
    rows = [
        ("hash", pipeline.hash()),
        ("yaml", yaml_path),
        ("workers", f"{max_workers} threads"),
        ("disk cache", f"{cache.root}  ·  {cache.capacity_bytes / 2**30:.1f} GiB cap  ({source})"),
    ]
    return [f"  {paint(key.ljust(_LABEL), 'dim', enabled=on)}{value}" for key, value in rows if value]


def _lanes(pipeline: Pipeline) -> tuple[list[str], dict[str, int], dict[str, int], dict[str, int], int]:
    """Place every node on a row and a rail lane.

    A lane is busy from its node's row down to its *last child's* row; past that the edge
    is spent and the lane is free again, so a wide graph reuses columns instead of growing
    one per node. The last child may take its parent's lane — that is the straight-down
    edge, and it keeps a linear pipeline in a single rail.
    """
    order = pipeline.topo_order()
    specs = {n.name: n for n in pipeline.nodes}
    row = {name: i for i, name in enumerate(order)}
    end = dict(row)
    for name in order:
        for parent in specs[name].inputs:
            end[parent] = max(end[parent], row[name])

    lane: dict[str, int] = {}
    held: list[str] = []  # lane -> the node currently owning it
    for name in order:
        parents = set(specs[name].inputs)
        for l, owner in enumerate(held):
            if end[owner] < row[name] or (end[owner] == row[name] and owner in parents):
                lane[name] = l
                break
        else:
            lane[name] = len(held)
            held.append(name)
        held[lane[name]] = name
    return order, row, lane, end, 2 * len(held) - 1


def _rail(node: str, row: dict[str, int], lane: dict[str, int], end: dict[str, int], width: int) -> str:
    """A node's own row: the node's marker, plus every rail passing it by."""
    cells = [" "] * width
    for other, l in lane.items():
        if row[other] < row[node] < end[other]:
            cells[2 * l] = "│"
    cells[2 * lane[node]] = "●"
    return "".join(cells)


def _link(node, inputs, row, lane, end, width: int) -> str:
    """The row above a node: where its incoming edges turn out of their lanes into it."""
    here, mine = row[node], lane[node]
    cells = [" "] * width
    for other, l in lane.items():  # rails spanning the gap between the two node rows
        if row[other] < here <= end[other]:
            cells[2 * l] = "│"

    for parent in inputs:  # each edge leaves its lane with a corner, or tees off a rail
        there = lane[parent]
        if there == mine:
            continue
        turning = "├" if there < mine else "┤"
        corner = "╰" if there < mine else "╯"
        cells[2 * there] = turning if end[parent] > here else corner
        for col in range(min(there, mine) * 2 + 1, max(there, mine) * 2):
            cells[col] = {" ": "─", "│": "┼", "╰": "┴", "╯": "┴", "├": "┼", "┤": "┼"}.get(cells[col], cells[col])

    sides = {lane[p] < mine for p in inputs if lane[p] != mine} | {
        2 for p in inputs if lane[p] == mine
    }  # True: from the left · False: from the right · 2: straight down its own lane
    cells[2 * mine] = {
        frozenset({2}): "│", frozenset({True}): "╮", frozenset({False}): "╭",
        frozenset({True, False}): "┬", frozenset({2, True}): "┤",
        frozenset({2, False}): "├", frozenset({2, True, False}): "┼",
    }.get(frozenset(sides), " ")
    return "".join(cells)


def _graph(pipeline: Pipeline) -> list[tuple[str, str, str, str]]:
    """``[(rail, node, kernel class, edge label)]`` top to bottom — plain text, for
    measuring. A row with no node name is a link row: rails only."""
    specs = {n.name: n for n in pipeline.nodes}
    order, row, lane, end, width = _lanes(pipeline)
    out = []
    for name in order:
        inputs = specs[name].inputs
        if row[name]:
            out.append((_link(name, inputs, row, lane, end, width), "", "", ""))
        edge = f"← {', '.join(inputs)}" if inputs else ""
        out.append((_rail(name, row, lane, end, width), name, _short(specs[name].kernel), edge))
    return out


def _framed(rows: list[tuple[str, str, str, str]], title: str, on: bool) -> list[str]:
    """Box the graph. Widths are measured on the plain text, then colour goes on top —
    the other order counts escape codes as characters and skews the frame."""
    def dim(text: str) -> str:
        return _dim(text, on)

    gutter = max(len(rail) for rail, *_ in rows)
    labels = [f"{name}[{cls}]" if name else "" for _, name, cls, _ in rows]
    width = max(len(label) for label in labels)
    plain = [
        f"{rail.ljust(gutter)}  {label.ljust(width)}" + (f"  {edge}" if edge else "")
        for (rail, _, _, edge), label in zip(rows, labels)
    ]
    inner = max(len(p) for p in plain) + 2  # one space of padding either side

    head = f"─ {title} "
    out = [dim(f"  ┌{head}{'─' * (inner - len(head))}┐")]
    for (rail, node, cls, edge), label, text in zip(rows, labels, plain):
        # The class is what you scan for, so it gets the colour; the node name is the
        # anchor, so it gets the weight. Rails and edge names stay dim — structure.
        named = paint(node, "bold", enabled=on) + dim("[") + paint(cls, "cyan", enabled=on) + dim("]")
        body = dim(rail.ljust(gutter)) + "  " + (named if node else "") + " " * (width - len(label))
        body += f"  {dim(edge)}" if edge else ""
        out.append(f"  {dim('│')} {body}{' ' * (inner - 2 - len(text))} {dim('│')}")
    out.append(dim(f"  └{'─' * inner}┘"))
    return out


def _short(dotted: str) -> str:
    """``abasift.kernels.ImuSpikeKernel`` -> ``ImuSpikeKernel``: the class is the point."""
    return dotted.rpartition(".")[2] or dotted
