"""``abasift`` CLI. One YAML, one machine, one report."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from pathlib import Path

from .errors import AbaSiftError
from .executor import Executor
from .pipeline import Pipeline


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
    logging.basicConfig(
        level=logging.DEBUG if getattr(args, "verbose", False) else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    try:
        pipeline = Pipeline.from_yaml(args.pipeline)
    except AbaSiftError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.command == "validate":
        print(f"ok: {pipeline.name} ({pipeline.hash()}) — {' -> '.join(pipeline.topo_order())}")
        return 0

    if args.command == "vis":
        from .vis import serve  # only the vis path pays for it

        # Nothing is rendered here: the server re-describes the YAML on every request.
        return serve(args.pipeline, host=args.host, port=args.port)

    if args.job_id:
        pipeline = Pipeline(pipeline.name, pipeline.nodes, args.job_id, pipeline.source)

    watcher, server = _watch(pipeline, args.pipeline, args.vis_port) if args.vis else (None, None)
    executor = Executor(pipeline, max_workers=args.max_workers, observer=watcher)
    report = executor.run()
    payload = report.to_json()

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(payload, indent=2))
        print(f"report: {args.out}")
    for node, summary in payload["summary"].items():
        print(f"summary[{node}]: {json.dumps(summary, sort_keys=True)}")
    counts = payload["job"]["counts"]
    print(
        f"{payload['job']['n_samples']} samples in {payload['job']['n_batches']} batches, "
        f"{payload['job']['elapsed_s']}s — "
        + " ".join(f"{k}={v}" for k, v in counts.items())
    )
    if server is not None:
        _hold(server)
    # A job that completes and reports is a success, whatever the verdicts inside it.
    return 0


def _watch(pipeline, yaml_path: str, port: int):
    """Start the run watcher and its server. Returns ``(observer, server)``.

    The server runs on a daemon thread so it can never hold the process open on its own —
    a job with ``--vis`` still exits when the job is done and you stop watching.
    """
    from .vis import LiveJob, RunView, make_server, url_of

    live = LiveJob()
    server = make_server(RunView(pipeline, live, yaml_path), port=port)
    threading.Thread(target=server.serve_forever, daemon=True, name="abasift-vis").start()
    print(f"abasift vis: {url_of(server)} — watching this run")
    return live, server


def _hold(server) -> None:
    """Keep serving the finished job until the user is done looking at it."""
    from .vis import url_of

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
