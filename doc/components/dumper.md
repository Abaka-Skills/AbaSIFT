# DataDumper — the one sanctioned mutator

Module: `abasift/kernels/dumper.py`.

Ordinary kernels are extend-only. `DataDumper` is the single exception, and it reaches that
privilege through a distinct interface (`MutatingKernel.run_mutating` → `Mutation`) rather
than by convention, so "who may rewrite an artifact" is a type-level answer.

## Two modes

**dump** (`target` set) — write each matching artifact out, then replace its in-union value
with a `LazyRaw` pointing at what was written. Downstream kernels are oblivious: `.decode()`
works either way. Memory freed, information preserved.

- `LazyRaw` values are copied with `shutil.copyfileobj` in 8 MB chunks — a 2 GB video never
  lands in RAM — and the replacement keeps the original decoder and opts.
- `bytes` values are written as `.bin`, anything else as `.json`.

**free** (`target` empty) — drop the matching keys and call `DiskCache.forget(uri)` on each,
which deletes the backing file only if it lives inside the cache root. The dumper does not
derive that path itself: the key scheme and the "never delete outside the cache" guarantee
stay in `cache.py`, where they belong. For intermediates nobody downstream reads.

## Paths

```
{target_root}/{job_id}/{node}/{key with '/' -> '__'}
{target_root}/{job_id}/{node}/report.json      # the __report__ pseudo-key
```

`target_root` depends on how `target` was given, and the difference is deliberate:

| `target` in YAML | `target_root` | why |
|---|---|---|
| omitted | `dump/<unix ts>` | Interactive runs get their own tree per run under the working directory. **Relative, never absolute**, so a YAML is portable between machines. |
| set (`/data/out`, `s3://…`) | used verbatim, **unstamped** | `f(job_id, node, key)` with no timestamps, so a retried job overwrites rather than duplicates (`test_rerunning_the_same_yaml_overwrites_the_same_paths`). |
| `""` (explicit) | — | *free* mode. |

So the no-timestamps rule still holds wherever it matters: anything an external splitter
generates sets `target` explicitly, and stays idempotent. What the default trades away is
that idempotency — **every run gets its own tree**, so nothing a default run writes can
collide with an earlier one, and the directories accumulate until someone deletes them.
That is the right trade for an interactive run and the wrong one for a production job,
which is exactly the split.

The stamp is `job.started_unix`, read from the job's start time **once per job**, so every
dumper in a DAG agrees. It is in the report, so the tree a run went to can always be
reproduced with `target: dump/<that stamp>`.

Free mode is **opt-in**: `target` must be explicitly `""`. Omitting it gives you the
stamped dump, never a silent delete — the destructive mode is the one you have to ask for.

## `__report__` is dumped after finalize

`run_mutating` handles artifact keys per batch. The `__report__` pseudo-key is handled in
`finalize_mutating`, which the executor runs *after* the finalize pass and after stamping
`job.counts` — so the dumped file is the complete report, not a half-written one. The test
asserts the dumped counts equal the in-memory report's.

## Placement is the author's job

A dumper is an explicit YAML node, visible in the graph. It must sit **downstream** of
everything that reads the keys it frees; a dumper running concurrently with a reader of the
same key is an authoring error, not something the framework second-guesses. Freeing is a
documented pattern to apply after heavy stages, not a hidden optimisation.
