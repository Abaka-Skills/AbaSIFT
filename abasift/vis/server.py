"""Host a pipeline view. Nothing is written to disk; the page is a window, not an artifact.

Two views, one server, because there are two questions:

* :class:`PipelineView` — *what is this pipeline?* (``abasift vis <yaml>``). A window onto
  the YAML and the kernel classes it names, both re-read on every request.
* :class:`RunView` — *what is this job doing?* (``abasift run <yaml> --vis``). A window
  onto a :class:`~abasift.vis.live.LiveJob` being filled in by the executor as it runs.

Either way the browser polls a cheap state token and pulls a new ``<main>`` when it moves,
so an open page keeps up by itself: with your editor in the first case, with the job in
the second. No refresh, no rebuild, no stale file.
"""

from __future__ import annotations

import copy
import hashlib
import importlib
import logging
import os
import sys
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from ..errors import AbaSiftError
from ..pipeline import Pipeline
from .live import LiveJob, overlay
from .model import describe
from .render import body, error_body, page

log = logging.getLogger("abasift.vis")

#: How the client asks "has anything changed?" — see ``assets/vis.js``.
ROUTES = ("/", "/body", "/state")


class PipelineView:
    """What the pipeline *is*: re-read, re-import and re-describe on demand.

    Holding no rendered output at all is the point. There is nothing here that can be
    stale, only files that are read again the next time someone asks.
    """

    def __init__(self, yaml_path: str | Path):
        self.yaml_path = Path(yaml_path)
        #: module name -> source mtime when we last looked. Filled in from each describe,
        #: so we watch exactly the kernels this YAML actually names — nothing else.
        self._watched: dict[str, float] = {}

    # -- what the page depends on ---------------------------------------

    def state(self) -> str:
        """Digest of every input. Cheap enough to poll: a handful of ``stat`` calls."""
        h = hashlib.sha256()
        h.update(_stamp(self.yaml_path).encode())
        for name in sorted(self._watched):
            h.update(f"{name}@{_module_mtime(name)}".encode())
        return h.hexdigest()[:16]

    def model(self) -> dict:
        """Describe the pipeline as it is *right now*."""
        self._reload_changed()
        model = describe(Pipeline.from_yaml(self.yaml_path), str(self.yaml_path))
        self._watch(model)
        model["live"] = self.state()
        return model

    # -- keeping the imported classes current ---------------------------

    def _watch(self, model: dict) -> None:
        names = set()
        for node in model["nodes"]:
            names.add(node["module"])  # where the class is defined
            names.add(node["kernel"].rpartition(".")[0])  # what the YAML imports it from
        self._watched = {n: _module_mtime(n) for n in names if n in sys.modules}

    def _reload_changed(self) -> None:
        """Re-import kernel modules whose source changed, so signatures stay honest.

        Only the modules this YAML names are ever reloaded — never the core contracts.
        Reloading ``abasift.kernel`` would mint a second ``Kernel`` class and every
        ``issubclass`` check in the pipeline validator would start failing.
        """
        changed = {n for n, seen in self._watched.items() if _module_mtime(n) != seen}
        if not changed:
            return
        # A package that re-exports a changed submodule holds the old class object, so it
        # has to be reloaded too — deepest first, so the parent picks up the new child.
        targets = set(changed)
        for name in changed:
            targets |= {w for w in self._watched if name.startswith(f"{w}.")}
        for name in sorted(targets, key=lambda n: -n.count(".")):
            module = sys.modules.get(name)
            if module is None:
                continue
            try:
                importlib.reload(module)
                log.info("reloaded %s", name)
            except Exception as e:  # mid-edit / broken import: keep the last good class
                log.warning("could not reload %s: %s", name, e)
        self._watched = {n: _module_mtime(n) for n in self._watched}

    # -- what the server hands back -------------------------------------

    def html(self, full: bool = True) -> str:
        """The page (``full``) or just its ``<main>`` contents.

        A YAML that won't parse is a state to *show*, not a reason to stop serving: the
        next poll that loads cleanly replaces the message with the graph again.
        """
        try:
            model = self.model()
            inner, name, live = body(model), model["pipeline"]["job_id"], model["live"]
        except AbaSiftError as e:
            inner, name, live = error_body(type(e).__name__, str(e), str(self.yaml_path)), "error", self.state()
        except Exception as e:
            detail = traceback.format_exc() if not isinstance(e, OSError) else str(e)
            inner, name, live = error_body(type(e).__name__, detail, str(self.yaml_path)), "error", self.state()
        return page(name, inner, live) if full else inner


class RunView:
    """What a job is *doing*: the pipeline as loaded, plus the executor's live progress.

    Nothing is re-read here, deliberately. The running job is using the classes that were
    imported when it started, so re-importing them mid-run would show a graph the job is
    not executing. The structure is described once; only the progress moves.
    """

    def __init__(self, pipeline: Pipeline, live: LiveJob, yaml_path: str | Path | None = None):
        self.pipeline = pipeline
        self.live = live
        self.yaml_path = Path(yaml_path) if yaml_path else None
        self._static = describe(pipeline, str(yaml_path) if yaml_path else None)

    def state(self) -> str:
        return f"v{self.live.version}"  # bumped by the executor's every step

    def model(self) -> dict:
        model = copy.deepcopy(self._static)
        model = overlay(model, self.live.snapshot())
        model["live"] = self.state()
        return model

    def html(self, full: bool = True) -> str:
        model = self.model()
        inner = body(model)
        return page(model["pipeline"]["job_id"], inner, model["live"]) if full else inner


def _stamp(path: Path | None) -> str:
    """``mtime:size`` for a file, or a marker for one that is not there (yet)."""
    if path is None:
        return "-"
    try:
        st = path.stat()
        return f"{st.st_mtime_ns}:{st.st_size}"
    except OSError:
        return "missing"


def _module_mtime(name: str) -> float:
    module = sys.modules.get(name)
    file = getattr(module, "__file__", None)
    try:
        return os.path.getmtime(file) if file else 0.0
    except OSError:
        return 0.0


class Handler(BaseHTTPRequestHandler):
    """Three routes, all GET. ``view`` is bound per server by :func:`make_server`.

    A view is anything with ``state()`` and ``html(full)`` — :class:`PipelineView` or
    :class:`RunView`; the server does not care which it is holding.
    """

    view: "PipelineView | RunView"
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 — http.server's spelling
        route = urlparse(self.path).path.rstrip("/") or "/"
        if route == "/":
            self._send(200, "text/html; charset=utf-8", self.view.html(full=True))
        elif route == "/body":
            self._send(200, "text/html; charset=utf-8", self.view.html(full=False))
        elif route == "/state":
            self._send(200, "text/plain; charset=utf-8", self.view.state())
        else:
            self._send(404, "text/plain; charset=utf-8", f"no such route; try {' '.join(ROUTES)}")

    def _send(self, status: int, content_type: str, text: str) -> None:
        payload = text.encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")  # the whole point is freshness
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt: str, *args) -> None:
        log.debug("%s - %s", self.address_string(), fmt % args)  # polling must not spam


def make_server(
    view: "PipelineView | RunView", host: str = "127.0.0.1", port: int = 8765
) -> ThreadingHTTPServer:
    """A configured, not-yet-running server. ``port=0`` picks a free one (tests use this)."""
    handler = type("BoundHandler", (Handler,), {"view": view})
    return ThreadingHTTPServer((host, port), handler)


def url_of(server: ThreadingHTTPServer) -> str:
    host, port = server.server_address[:2]
    return f"http://{host}:{port}"


def serve(yaml_path: str | Path, host: str = "127.0.0.1", port: int = 8765) -> int:
    """Host a pipeline view until interrupted. Loopback by default — this is a dev tool."""
    server = make_server(PipelineView(yaml_path), host, port)
    print(f"abasift vis: {url_of(server)}  ({yaml_path})  — ctrl-c to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print()
    finally:
        server.server_close()
    return 0
