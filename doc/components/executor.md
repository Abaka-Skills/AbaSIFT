# Pipeline + Executor

Modules: `abasift/pipeline.py`, `abasift/executor.py`.

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
    merge the *leaf* nodes' unions and reports into the job's
    release the batch's decoded payloads
after all batches:
    digest() per kernel in topo order            -> summaries
    commit() for DataArchivers            -> archive the finished report
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

### Why the batch merge only looks at leaves

`art_out = art_in.extended(node, ext)`, and `art_in` is the union of that node's inputs — so
**a leaf already contains every ancestor's artifacts**. Folding the middle of the graph back
in adds nothing: `U0 ∪ U1 ∪ U2 ∪ U3 = U3` when `U3` descends from all of them.

It is not merely redundant. A `MutatingKernel` that *replaces* a value leaves its ancestors
holding the pre-mutation copy, so merging every node meant one key arriving with two values
— the mutation losing an argument with a stale copy of itself, and `ArtifactUnion.union`
rightly refusing to pick a winner. That was an `ExecutorError` that killed the job, and it
fired on a **linear** three-node DAG, which is why "don't mutate on a branch" was never the
fix (`test_archive_mode_replaces_the_key_the_job_union_carries`).

Leaves alone are still not enough when leaves are *siblings*: `free` deletes a key while its
sibling leaf still carries it. That is what the union's sticky `deleted` set settles, and why
it stays (`test_a_delete_in_one_branch_is_not_resurrected_by_its_sibling`). Two mutating
nodes *replacing* the same key in parallel branches has no such rule and never will — there
is no honest winner — so `Pipeline.validated()` rejects that shape at load instead, via
`MutatingKernel.replaced_key_patterns()`.

The report is merged the same way, for the same reason. A leaf's report is its ancestors'
findings plus its own (`_run_node` folds `rep_in` into `rep_out`), so leaves carry
everything — `test_a_mid_graph_nodes_findings_still_reach_the_job_report`. Report merging
is additive, so folding in the middle of the graph could never *disagree* the way the
union can; it was simply saying the same thing several times. One rule for both is worth
more than a special case that happens to be harmless.

### Failsafe layers

| failure | handling | test |
|---------|----------|------|
| loader can't enumerate an item | source emits an `error` sample in its report fragment | `FlatDirLoader` zero-size path |
| source raises mid-iteration | recorded in `summary.<source>.source_error`; job completes with what it got | `test_source_failure_is_recorded_and_the_job_still_reports` |
| decode fails / kernel raises on one sample | `status: error` for that sample at that node; sample dropped from downstream nodes; batchmates continue | `test_failed_sample_is_dropped_downstream_but_batchmates_continue` |
| kernel raises before touching samples | every still-live sample in the batch gets `error` at that node | `test_whole_node_failure_errors_live_samples_only` |
| worker dies | external system re-runs the YAML; dump paths are `f(job_id, pipeline_hash, node, key)` with no timestamps, so retries overwrite idempotently | `test_rerunning_the_same_yaml_overwrites_the_same_paths` |

Nothing propagates out of `Executor.run()`. A job always completes and always reports.

### Digest, then commit

`digest()` runs for every kernel in topo order (dataset-level reduces), *then* the
executor stamps `job.counts` / `n_samples` / `n_batches` / `elapsed_s`, *then*
`commit()` runs for archivers. Without that ordering an archived `report.json`
would be missing the job block written after it — the test asserts the written file's
counts equal the in-memory report's.

### Threads, not processes

Threads only: PyAV, numpy and I/O all release the GIL, and the design transfers unchanged
to free-threaded CPython. Pool size defaults to `min(8, n_nodes)`, overridable with
`--max-workers`. Concurrency is *within* the DAG (branches over one batch), not across
many batches in flight — parallel samples would multiply decoded memory, parallel branches
share it.
