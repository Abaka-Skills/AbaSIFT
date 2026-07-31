"""``abasift`` CLI. One YAML, one machine, one report."""

from __future__ import annotations

import argparse
import json
import logging
import sys
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

    check = sub.add_parser("validate", help="load and validate a pipeline YAML, run nothing")
    check.add_argument("pipeline")

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

    if args.job_id:
        pipeline = Pipeline(pipeline.name, pipeline.nodes, args.job_id, pipeline.source)

    executor = Executor(pipeline, max_workers=args.max_workers)
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
    # A job that completes and reports is a success, whatever the verdicts inside it.
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
