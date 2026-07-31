"""Description dict -> HTML. Called per request by the server; nothing is written to disk.

The document inlines its CSS and JS and fetches nothing external — the only requests the
page makes are back to its own server, to poll for a change. The only layout decision
taken here is *which column* a node sits in; where the wires go is settled in the browser
(``assets/vis.js``).
"""

from __future__ import annotations

import json
from datetime import datetime
from html import escape
from itertools import groupby
from pathlib import Path

_ASSETS = Path(__file__).parent / "assets"

#: Status order for the verdict chips — the report's own worst-of ordering.
_STATUSES = ("pass", "warn", "fail", "error")

_ROLE_LABEL = {
    "source": "node 0 · SourceKernel",
    "sample": "SampleKernel",
    "kernel": "Kernel",
    "mutating": "MutatingKernel",
}


def render(model: dict) -> str:
    """The whole page for one described pipeline."""
    return page(model["pipeline"]["name"], body(model), model.get("live"))


def page(name: str, inner: str, live: str | None = None) -> str:
    """Wrap a body fragment in the document. CSS and JS are inlined; nothing is fetched.

    ``live`` is the server's current state digest. When it is present the page polls for a
    new one and swaps its own ``<main>`` — see ``server.py``.
    """
    title = escape(f"abasift · {name}")
    css = (_ASSETS / "vis.css").read_text()
    js = (_ASSETS / "vis.js").read_text()
    live_js = f"window.ABASIFT_LIVE = {json.dumps({'state': live})};" if live else ""
    # An f-string, not str.format: the CSS and JS are full of braces.
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
{css}
</style>
</head>
<body>
<main id="main">
{inner}
</main>
<script>{live_js}</script>
<script>
{js}
</script>
</body>
</html>
"""


def body(model: dict) -> str:
    """Everything inside ``<main>``. The live server re-serves just this on a change.

    The edge list rides along inside it, because a reload that changed the DAG must
    replace the wiring too. It is inert JSON, so it needs no re-execution after the swap.
    """
    return f"""{_header(model)}
{_job_strip(model)}
<div class="graph" id="graph">
  <svg class="wires" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7"
              orient="auto-start-reverse" markerUnits="userSpaceOnUse">
        <path d="M0,1 L9,5 L0,9 z" fill="var(--edge)" stroke="none"/>
      </marker>
    </defs>
    <g id="wire-group"></g>
  </svg>
  <div class="layers">
{_layers(model["nodes"])}
  </div>
</div>
<dialog id="sheet">
  <button class="sheet-close" type="button" aria-label="close">esc</button>
  <div class="sheet-body"></div>
</dialog>
{_legend(model["nodes"])}
{_footer(model)}
<script id="edges" type="application/json">{json.dumps(model["edges"])}</script>"""


def error_body(title: str, detail: str, source: str = "") -> str:
    """What the live server shows while a YAML or a kernel is mid-edit and won't load.

    A broken file is a state to display, not a reason to stop serving: the next poll that
    parses cleanly replaces this with the graph again.
    """
    where = f"<p class='path'>{escape(source)}</p>" if source else ""
    return (
        "<div class='top'><h1>pipeline won't load</h1>"
        "<button id='theme' type='button'>theme</button></div>"
        "<p class='meta'><span>the page will recover by itself once the file parses</span></p>"
        f"<article class='node r-error'><div class='head'><h3>{escape(title)}</h3></div>{where}"
        f"<p class='summary'>{escape(detail)}</p></article>"
        '<script id="edges" type="application/json">[]</script>'
    )


# -- page pieces ---------------------------------------------------------------


def _header(model: dict) -> str:
    p = model["pipeline"]
    bits = [
        f"<span class='mono'>{escape(p['job_id'])}</span>",
        f"{p['n_nodes']} nodes in {model['n_layers']} stages",
        f"source <span class='mono'>{escape(p['source'])}</span>",
        f"hash <span class='mono'>{escape(p['hash'])}</span>",
    ]
    if p.get("yaml"):
        bits.insert(0, f"<span class='mono'>{escape(p['yaml'])}</span>")
    # Served live, the page re-reads the YAML and the kernels on every change.
    live = "<span class='pill-live'>live</span>" if model.get("live") else ""
    return (
        "<div class='top'>"
        f"<h1>{escape(p['name'])}{live}</h1>"
        "<button id='theme' type='button'>theme</button>"
        "</div>"
        f"<p class='meta'>{''.join(f'<span>{b}</span>' for b in bits)}</p>"
    )


def _job_strip(model: dict) -> str:
    """Only present while (or after) watching a run; `abasift vis` alone has no job."""
    job = model.get("job")
    if not job:
        return ""
    counts = job.get("counts") or {}
    chips = "".join(_chip(s, counts.get(s, 0)) for s in _STATUSES if s in counts)
    done = model.get("done")
    running = model.get("running") or []
    facts = [
        ("job", job.get("job_id") or ""),
        ("started", job.get("started_at") or job.get("started_unix") or ""),
        ("batches", job.get("n_batches")),
        ("samples", job.get("n_samples")),
        ("elapsed", f"{job.get('elapsed_s')}s" if job.get("elapsed_s") is not None else None),
    ]
    text = "".join(
        f"<span class='k'>{k}</span> <span class='mono'>{escape(str(v))}</span>"
        for k, v in facts
        if v not in (None, "")
    )
    if done:
        status = "<span class='pill-done'>done</span>"
    else:
        where = f" · {escape(', '.join(running))}" if running else ""
        status = f"<span class='pill-run'>running{where}</span>"
    return f"<div class='job-strip'>{status}{text}{chips}</div>"


def _chip(status: str, n: int) -> str:
    cls = status if n else "zero"
    return f"<span class='chip {cls}'>{n} {status}</span>"


def _layers(nodes: list[dict]) -> str:
    ordered = sorted(nodes, key=lambda n: n["layer"])
    out = []
    for layer, group in groupby(ordered, key=lambda n: n["layer"]):
        cards = "".join(_card(n) for n in group)
        label = "source" if layer == 0 else f"stage {layer}"
        out.append(f"<div class='layer'><p class='layer-label'>{label}</p>{cards}</div>")
    return "".join(out)


def _card(n: dict) -> str:
    """The box in the graph: what you need to read the DAG at a glance, and no more.

    Name, role, class, and how much there is to see. Everything else — the params, the
    signatures, where the code lives — sits in the ``<template>`` and is only built into
    the DOM when the node is clicked, so scanning the graph stays cheap for the eye.
    """
    counts = [
        f"{len(n['params'])} params" if n["params"] else "",
        f"{len(n['methods'])} methods" if n["methods"] else "",
    ]
    parts = [
        "<div class='head'>"
        f"<h3>{escape(n['name'])}</h3>"
        f"<span class='badge'>{escape(_ROLE_LABEL.get(n['role'], n['role']))}</span>"
        "</div>",
        f"<p class='cls'>{escape(n['class'])}</p>",
        f"<p class='more'>{escape(' · '.join(c for c in counts if c))}</p>",
    ]
    if n.get("run"):  # only a watched run fills this in
        parts.append(f"<div class='card-run'>{_verdicts(n['run'])}</div>")
    running = " is-running" if n.get("running") else ""
    return (
        f"<article class='node r-{escape(n['role'])}{running}' tabindex='0' role='button'"
        f" data-node=\"{escape(n['name'], quote=True)}\">"
        + "".join(parts)
        + f"<template class='detail'>{_detail(n)}</template>"
        + "</article>"
    )


def _detail(n: dict) -> str:
    """The full node, shown in a sheet on click. Inert until then: it lives in a template."""
    parts = [
        "<div class='head'>"
        f"<h3>{escape(n['name'])}</h3>"
        f"<span class='badge'>{escape(_ROLE_LABEL.get(n['role'], n['role']))}</span>"
        "</div>",
        f"<p class='path'>{escape(n['kernel'])}</p>",
    ]
    if n["doc"]:
        parts.append(f"<p class='doc'>{escape(n['doc'])}</p>")
    if n["source_file"]:
        parts.append(f"<p class='file'>{escape(n['source_file'])}</p>")
    if n["inputs"]:
        parts.append(f"<p class='file'>inputs: {escape(', '.join(n['inputs']))}</p>")
    if n["params"]:
        parts.append(f"<section><h4>params</h4>{_params(n['params'])}</section>")
    if n["methods"]:
        parts.append(f"<section><h4>interface</h4>{_methods(n['methods'])}</section>")
    if n.get("run"):
        parts.append(f"<section><h4>this run</h4>{_run(n['run'])}</section>")
    return "".join(parts)


def _params(params: list[dict]) -> str:
    rows = []
    for p in params:
        if p["given"]:
            value, cls = escape(p["value"]), "set"
        elif p["required"]:
            value, cls = "", "req"
        else:
            value, cls = f"<span class='dim'>{escape(p['default'])}</span>", ""
        name = escape(p["name"])
        if p["annotation"]:
            name += f"<span class='dim'>: {escape(p['annotation'])}</span>"
        rows.append(f"<tr class='{cls}'><td class='n'>{name}</td><td class='v'>{value}</td></tr>")
    return f"<table class='params'>{''.join(rows)}</table>"


def _methods(methods: list[dict]) -> str:
    items = []
    for m in methods:
        cls = "own" if m["own"] else "inh"
        via = "" if m["own"] else f" <span class='via'>inherited from {escape(m['owner'])}</span>"
        doc = f"<p class='mdoc'>{escape(m['doc'])}</p>" if m["doc"] else ""
        items.append(
            f"<li><code class='sig {cls}'>{escape(m['name'])}{escape(m['signature'])}</code>{via}{doc}</li>"
        )
    return f"<ul class='sigs'>{''.join(items)}</ul>"


def _verdicts(run: dict) -> str:
    """Just the tallies — small enough to sit on a card while a job runs."""
    chips = []
    for counts in run["checks"].values():
        chips += [_chip(s, counts[s]) for s in _STATUSES if counts.get(s)]
    return f"<div class='verdicts'>{''.join(chips)}</div>" if chips else ""


def _run(run: dict) -> str:
    rows = []
    for name, counts in run["checks"].items():
        chips = "".join(_chip(s, counts[s]) for s in _STATUSES if counts.get(s))
        rows.append(f"<div class='verdicts'><span class='cname'>{escape(name)}</span>{chips}</div>")
    if run.get("summary"):
        rows.append(f"<p class='summary'>{escape(json.dumps(run['summary'], sort_keys=True))}</p>")
    return "".join(rows) or "<p class='summary'>no checks recorded</p>"


def _legend(nodes: list[dict]) -> str:
    seen = {n["role"] for n in nodes}
    swatch = {
        "source": ("--vend", "--vend-b"),
        "sample": ("--kern", "--kern-b"),
        "kernel": ("--core", "--core-b"),
        "mutating": ("--exec", "--exec-b"),
    }
    out = []
    for role, label in _ROLE_LABEL.items():
        if role not in seen:
            continue
        fill, line = swatch[role]
        out.append(
            f"<span><i class='swatch' style='background:var({fill});border-color:var({line})'></i>"
            f"{escape(label)}</span>"
        )
    out.append("<span>click a node for its parameters and signatures</span>")
    return f"<div class='legend'>{''.join(out)}</div>"


def _footer(model: dict) -> str:
    from .. import __version__

    when = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    return (
        f"<footer>Rendered by <code>abasift.vis</code> {escape(__version__)} at {escape(when)} "
        "by introspecting the kernel classes this pipeline imports — signatures, parameters and "
        "defaults are read off the live code, not transcribed.</footer>"
    )
