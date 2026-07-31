# `abasift.vis` — the hosted pipeline views

Modules: `abasift/vis/server.py` (host), `model.py` (describe), `live.py` (watch a run),
`render.py` (draw), `assets/` (inlined CSS+JS).

Two commands, because there are two questions. Both host a page; neither writes a file.

```bash
abasift vis pipelines/imu_spike_egoverse_dji.yaml            # what is this pipeline?
abasift run pipelines/imu_spike_egoverse_dji.yaml --vis      # what is this job doing?
#  -> http://127.0.0.1:8765   (ctrl-c to stop)
```

| | `abasift vis <yaml>` | `abasift run <yaml> --vis` |
|---|---|---|
| shows | the DAG: nodes, wiring, params, kernel signatures | the same graph, filling in as the job runs |
| view class | `PipelineView` | `RunView` |
| tracks | the YAML and the kernels' source files | the executor's progress events |
| re-reads files | yes, every request | **no** — see below |

## It is a window, not an artifact

There is no "generate the HTML" step, deliberately. A generated file is a snapshot that is
wrong the moment you edit the YAML, and nothing tells you it went wrong. The server holds
no rendered output at all — `PipelineView` re-reads the YAML and re-imports any kernel
whose source file moved, then re-describes, on every request.

The browser polls `/state` — a token that changes when anything the page depends on does —
roughly once a second, and when it moves it pulls a fresh `/body` and swaps its own
`<main>`. So an open tab follows your editor: change `z_thresh` in the YAML and the box
changes; add a parameter to a kernel and the row appears. No refresh, no rebuild, no stale
copy in the repo.

Three routes, all GET: `/` (page), `/body` (just the `<main>` contents), `/state` (token).
Binds loopback by default — it is a dev tool, not a service.

## Watching a run

`--vis` starts the same server on a daemon thread over a `RunView`, and hands the executor
a `LiveJob` as its **observer**. The executor calls `observer(event, **payload)` at five
points — `job_started`, `batch_started`, `node_started`, `node_finished`, `batch_merged`,
`job_finished` — and never looks at the result, so the watcher is strictly additive:

- the executor imports nothing from `vis` and needs no knowledge of it;
- an observer that raises cannot fail a job (`Executor._emit` swallows it), which is the
  same rule as everywhere else here: telemetry is never load-bearing;
- with no `--vis` there is no observer and the calls are a null check.

The node currently executing is outlined and pulsing; each box's verdict counts accumulate
batch by batch. Counting is **incremental** — `batch_merged` carries only that batch's
report fragment, and per-`node/check` keys are disjoint across batches, so folding is a
counter bump rather than a rescan of every sample so far.

`RunView` deliberately does **not** re-read the YAML or reload kernels: the running job is
executing the classes it loaded at startup, so showing anything else would be a lie. The
structure is described once; only the progress moves. When the job ends the server keeps
serving the finished picture until you stop it.

**A file that won't parse is a state to show, not a crash.** Mid-edit YAML gives a
"pipeline won't load" card with the `PipelineError`, and the next poll that parses cleanly
puts the graph back.

**Hot reload is scoped to the kernels the YAML names.** Their defining module and the
package that re-exports them are reloaded (deepest first, so the package picks up the new
class); the core contracts never are. Reloading `abasift.kernel` would mint a second
`Kernel` class and every `issubclass` check in the pipeline validator would start failing.

## Why not a diagram

[doc/uml/](../uml/) is hand-drawn architecture: it explains the *framework*, and it is
maintained by a human who can forget. This is the opposite thing on both axes.

| | `doc/uml/` | `abasift vis` |
|---|---|---|
| subject | the framework | **your YAML**, with the parameters it sets |
| source | written by hand | `inspect` on the classes the YAML imports |
| when it lies | when someone forgets to redraw | it can't — it re-reads the code every render |

## Card, then sheet

The graph has to be readable at a glance, so a card carries only what you need to *read
the DAG*: node name, role badge, kernel class, and how much there is behind it
(`6 params · 3 methods`), plus verdict chips while a job runs.

Everything else lives in an inert `<template>` inside the card and is cloned into a
`<dialog>` when you click (or tab to and press enter on) the node — dotted import path,
`file:line`, the doc line, the full parameter table, the signatures, and that node's run
detail. Nothing is fetched to open it: the detail is already in the page, just not in the
document.

The open sheet is remembered **by node name** across a live swap, so during `run --vis`
you can leave one node's detail open and watch its verdicts land.

## What the sheet shows

**Role.** Node 0 is a `SourceKernel`, so it is described by `iter_batches()` — it has no
`run`. A check kernel is a `SampleKernel` and is described by `check`. A dumper is a
`MutatingKernel` and is described by `run_mutating` / `finalize_mutating`. Colour and
badge follow the interface, so the one node allowed to mutate the union is visibly
different from the ones that aren't.

**Own vs inherited.** Each signature says which class actually implements it. This is the
most useful line on the page: `ImuSpikeKernel` writes `check` and `finalize` and inherits
`run` from `SampleKernel` — i.e. the batch loop, the skip-if-already-failed filter and the
per-sample failsafe are the framework's, not the kernel author's. The division of labour
is visible instead of documented.

**Params.** Every constructor parameter, with what this YAML set (bold) and what it would
otherwise default to (grey). The table is exhaustive because kernels declare their
parameters explicitly — no `**kwargs` catch-all — which is the same property that turns a
mistyped threshold into a load-time `PipelineError`.

**This run** *(only under `run --vis`)*. Verdict counts per check and the node's
`finalize` summary. `describe()` never produces this: the static view has no opinion about
any run, and `live.overlay()` is what adds it.

## Layout

Python decides one thing: which **column** a node sits in — the longest path from the
source, so a join sits one column right of both its branches. Everything else is ordinary
flow content, and `assets/vis.js` draws the wires between whatever geometry the browser
produced, re-routing on resize and after every live swap. Cards can therefore grow (a long
parameter list, verdicts arriving mid-run) with nothing to re-tune, which is why the page
has no baked-in coordinates the way a hand-authored SVG does.

The document still inlines its CSS and JS and fetches nothing external — the only URL in
it is the SVG namespace, and the only requests it makes are back to its own server.

## Notes

- Describing a pipeline **imports** the kernels a YAML names (that is where the signatures
  come from) but never instantiates them and does no I/O: no bucket is listed, no
  credential is needed, nothing is decoded.
- Streams (`video/main`, `imu/main`, …) are not shown, because they are not knowable
  statically — a loader decides them per sample while enumerating. Reporting them from the
  batches the watcher sees would be a natural extension.
- Progress is per node **per batch**, not per sample: the executor emits when a node starts
  and finishes on a batch, so a long-running node shows as busy but without a bar inside
  it. Per-sample progress would need a kernel-level callback, which no kernel has.
