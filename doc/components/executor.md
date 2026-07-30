# Pipeline + Executor

Modules: `src/abasift/pipeline.py`, `src/abasift/executor.py`.

## Pipeline: everything fails fast

`Pipeline.from_yaml` → `from_dict` → `validated()`. Before any vendor byte is read we
check: unique node names, every `inputs` entry exists, no self-edge, **exactly one** source
node (`inputs: []`), the source is a `SourceKernel`, every other node is a `Kernel` and not
a source, all dotted import paths import, and the graph is acyclic (`topo_order()` raises
on a cycle). Unknown YAML keys on a node are rejected too.

Kernel params are **strict**: kernels declare their parameters explicitly and there is no
`**kwargs` catch-all, so `z_tresh: 12` in YAML is a load-time `PipelineError` rather than a
job that silently reports against default thresholds. `Pipeline.instantiate()` converts a
`TypeError` from the constructor into a `PipelineError` naming the node.

`Pipeline.hash()` is a stable 16-hex digest of the canonical definition, recorded in
`report.job.pipeline_hash` so a report can be traced back to the exact YAML.

## Execution model

```
for each batch yielded by the source:
    run the downstream DAG on it
      - independent branches run concurrently on a thread pool
      - joins: the executor unions the inputs
      - branches over one sample share one decode (LazyRaw memo)
    merge the batch's per-sample fragments into the job union/report
    release the batch's decoded payloads
after all batches:
    finalize() per kernel in topo order            -> summaries
    finalize_mutating() for DataDumpers            -> dump the finished report
```

Scheduling is a driver loop in the main thread: it submits every node whose inputs are
done, waits for `FIRST_COMPLETED`, folds the result in, repeats. No worker thread ever
blocks waiting on another node, so the pool cannot deadlock on itself.

### Determinism: a node sees its ancestors, not "whatever finished first"

Each node result carries **two** cumulative values: the union of its ancestors' unions,
and the report of its ancestors' findings. A node's view is built from its own `inputs`,
never from global state. This matters because kernels skip samples that are already
`error` — if that view came from a shared, concurrently-updated report, whether branch B
skipped a sample that branch A had just failed would depend on thread scheduling, and two
runs of the same job could produce different reports. With ancestor-scoped views, parallel
branches judge every sample independently and the report is scheduling-invariant.

### Failsafe layers

| failure | handling | test |
|---------|----------|------|
| loader can't enumerate an item | source emits an `error` sample in its report fragment | `FlatDirLoader` zero-size path |
| source raises mid-iteration | recorded in `summary.<source>.source_error`; job completes with what it got | `test_source_failure_is_recorded_and_the_job_still_reports` |
| decode fails / kernel raises on one sample | `status: error` for that sample at that node; sample dropped from downstream nodes; batchmates continue | `test_failed_sample_is_dropped_downstream_but_batchmates_continue` |
| kernel raises before touching samples | every still-live sample in the batch gets `error` at that node | `test_whole_node_failure_errors_live_samples_only` |
| worker dies | external system re-runs the YAML; dump paths are `f(job_id, node, key)` with no timestamps, so retries overwrite idempotently | `test_rerunning_the_same_yaml_overwrites_the_same_paths` |

Nothing propagates out of `Executor.run()`. A job always completes and always reports.

### Two-phase finalize

`finalize()` runs for every kernel in topo order (dataset-level reduces), *then* the
executor stamps `job.counts` / `n_samples` / `n_batches` / `elapsed_s`, *then*
`finalize_mutating()` runs for dumpers. Without that ordering a dumped `report.json`
would be missing the job block written after it — the test asserts the dumped file's
counts equal the in-memory report's.

### Threads, not processes

Threads only: PyAV, numpy and I/O all release the GIL, and the design transfers unchanged
to free-threaded CPython. Pool size defaults to `min(8, n_nodes)`, overridable with
`--max-workers`. Concurrency is *within* the DAG (branches over one batch), not across
many batches in flight — parallel samples would multiply decoded memory, parallel branches
share it.
