# DataDumper — the one sanctioned mutator

Module: `src/abasift/kernels/dumper.py`.

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

**free** (`target` empty) — drop the matching keys, and delete the backing file *only* if it
lives inside our own disk cache (path-checked against the cache root, so vendor data is
never at risk). For intermediates nobody downstream reads.

## Paths are deterministic

```
{target}/{job_id}/{node}/{key with '/' -> '__'}
{target}/{job_id}/{node}/report.json      # the __report__ pseudo-key
```

`f(job_id, node, key)`, no timestamps — a re-run of the same YAML overwrites the same
objects, so a retried job after a worker death is idempotent
(`test_rerunning_the_same_yaml_overwrites_the_same_paths`).

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
