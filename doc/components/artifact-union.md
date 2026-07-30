# Sample, Batch, ArtifactUnion

Module: `src/abasift/data.py`.

## Sample and stream naming

A `Sample` is `sample_id + {stream_name: LazyRaw} + meta`. Stream names are
`"{kind}/{name}"` with `kind` from a framework-owned set: `video`, `image`, `audio`,
`imu`, `annotation`, `blob`. Validated in `__post_init__` — a loader that emits
`telemetry/main` fails loudly instead of silently producing samples no kernel can find.

That registry is what lets a check kernel be vendor-agnostic: `ImuSpikeKernel` asks for
`imu/main` and never learns that this vendor hides the IMU inside an MP4.

`Batch` is a tuple of samples plus an index. It is an **efficiency unit only** — every
report entry and every artifact key stays per-sample, so a failing sample fails alone.

## ArtifactUnion

Flat map `"{node}/{name}" -> value`. Four properties, each load-bearing:

**Extend-only.** `extended(node, ext)` namespaces the keys and raises `ExecutorError` on
collision. A node cannot overwrite anything, so runs are deterministic regardless of
branch scheduling, and every intermediate survives to the end for the dumper to select.

**Immutable.** `extended` / `union` / `with_mutations` all return a *new* instance. Branches
run concurrently; nobody can observe a half-written union, and no lock is needed.

**Executor-owned joins.** `union(other)` is a key-wise dict union. Duplicate keys arriving
via two ingress edges of a diamond are identical by construction and merge silently; a
genuine value conflict is an `ExecutorError` with a message pointing at the actual mistake
(a kernel computing a per-batch aggregate in `run()` instead of `finalize()`). `LazyRaw`
has value semantics (`__eq__`/`__hash__` over uri+decoder+opts) precisely so that diamond
duplicates compare equal.

**Deletions are sticky.** `with_mutations(delete=...)` records the removed keys in a
`deleted` frozenset that survives `union`. Without this, freeing an artifact in one branch
would be undone by joining with a branch that still carries it. Covered by
`test_deletion_survives_a_later_join`.

### The batch travels inside the union

The loaded `Batch` lands under the source node's namespace (`load/batch`). One uniform
kernel signature, no special-cased batch parameter. `art.batch()` finds it by type and
raises if it is missing or ambiguous, so a kernel never hardcodes the loader's node name.

`without_transients()` drops `Batch`-valued keys before merging a batch's union into the
job union. Two reasons: union values are supposed to be primitives or `LazyRaw`, never
live Python objects; and `load/batch` would otherwise arrive with a *different* value on
every batch and trip the conflict check. This is a framework decision, not something a
kernel author sees — see decision log #18.

### Mutation is a separate interface

Ordinary kernels return extensions and cannot mutate. `with_mutations` exists for
`DataDumper` only, and the executor reaches it through a distinct interface
(`MutatingKernel.run_mutating` returning a `Mutation`), so "who may rewrite an artifact"
is answered by the type system rather than by convention. See
[dumper.md](dumper.md).
