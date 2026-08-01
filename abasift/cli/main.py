"""``abasift`` CLI. One YAML, one machine, one report."""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path

from ..errors import AbaSiftError
from ..executor import Executor
from ..pipeline import Pipeline
from .banner import run_banner, run_results
from .term import setup_logging


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="abasift", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run a pipeline YAML to completion")
    run.add_argument("pipeline", help="path to the pipeline YAML")
    run.add_argument("--job-id", help="override pipeline.job_id (dump paths depend on it)")
    run.add_argument("-o", "--out", help="also write the report JSON here")
    run.add_argument("--max-workers", type=int, default=None, help="thread pool size for DAG branches")
    run.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    run.add_argument("--vis", action="store_true", help="watch the job in a browser while it runs")
    run.add_argument("--vis-port", type=int, default=8765, help="0 picks a free port")

    check = sub.add_parser("validate", help="load and validate a pipeline YAML, run nothing")
    check.add_argument("pipeline")

    vis = sub.add_parser("vis", help="host a live view of the pipeline DAG on localhost")
    vis.add_argument("pipeline")
    vis.add_argument("--host", default="127.0.0.1", help="bind address (default: loopback)")
    vis.add_argument("--port", type=int, default=8765, help="0 picks a free port")

    args = parser.parse_args(argv)
    setup_logging(getattr(args, "verbose", False))

    try:
        pipeline = Pipeline.from_yaml(args.pipeline)
    except AbaSiftError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.command == "validate":
        print(f"ok: {pipeline.job_id} ({pipeline.hash()}) — {' -> '.join(pipeline.topo_order())}")
        return 0

    if args.command == "vis":
        from ..vis import serve  # only the vis path pays for it

        # Nothing is rendered here: the server re-describes the YAML on every request.
        return serve(args.pipeline, host=args.host, port=args.port)

    if args.job_id:
        pipeline = replace(pipeline, job_id=args.job_id)

    watcher, server = _watch(pipeline, args.pipeline, args.vis_port) if args.vis else (None, None)
    executor = Executor(pipeline, max_workers=args.max_workers, observer=watcher)
    # Constructing the executor resolved the cache, so the banner reports what will
    # actually be used rather than what the YAML asked for.
    # flush: logging goes to stderr, and a block-buffered stdout would print this last.
    print(run_banner(pipeline, executor.cache, executor.max_workers, args.pipeline), flush=True)
    report = executor.run()
    payload = report.to_json()

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(payload, indent=2))
        print(f"report: {args.out}")
    # Tallies and summaries are per node, not per sample, so they come off the pipeline
    # document — the same one the archiver writes, rather than a second reckoning here.
    print(run_results(report.pipeline_json()))
    if server is not None:
        _hold(server)
    # A job that completes and reports is a success, whatever the verdicts inside it.
    return 0


def _watch(pipeline, yaml_path: str, port: int):
    """Start the run watcher and its server. Returns ``(observer, server)``.

    The server runs on a daemon thread so it can never hold the process open on its own —
    a job with ``--vis`` still exits when the job is done and you stop watching.
    """
    from ..vis import LiveJob, RunView, make_server, url_of

    live = LiveJob()
    server = make_server(RunView(pipeline, live, yaml_path), port=port)
    threading.Thread(target=server.serve_forever, daemon=True, name="abasift-vis").start()
    print(f"abasift vis: {url_of(server)} — watching this run")
    return live, server


def _hold(server) -> None:
    """Keep serving the finished job until the user is done looking at it."""
    from ..vis import url_of

    print(f"abasift vis: job finished; still serving {url_of(server)} — ctrl-c to stop")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print()
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
