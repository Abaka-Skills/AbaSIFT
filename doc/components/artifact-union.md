# Sample, Batch, ArtifactUnion

Module: `abasift/data.py`.

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
branch scheduling, and every intermediate survives to the end for an archiver to select.

**Immutable.** `extended` / `union` / `with_mutations` all return a *new* instance. Branches
run concurrently; nobody can observe a half-written union, and no lock is needed.

**Executor-owned joins.** `union(other)` is a key-wise dict union. Duplicate keys arriving
via two ingress edges of a diamond are identical by construction and merge silently; a
genuine value conflict is an `ExecutorError` with a message pointing at the actual mistake
(a kernel computing a per-batch aggregate in `run()` instead of `digest()`). `LazyRaw`
has value semantics (`__eq__`/`__hash__` over uri+decoder+opts) precisely so that diamond
duplicates compare equal.

**Deletions are sticky.** `with_mutations(delete=...)` records the removed keys in a
`deleted` frozenset that survives `union`. Without this, freeing an artifact in one branch
would be undone by joining with a branch that still carries it. Covered by
`test_deletion_survives_a_later_join`.

### Reading your own artifacts back

`under(node)` returns one node's namespace. `per_sample(node, name)` is the specialization
every `digest()` wants: it undoes the `f"{name}/{sample_id}"` convention that
`SampleKernel.check` writes, returning `{sample_id: value}`. Kernels use it instead of
hand-slicing prefixes, so the write-name and the read-name are tied together in one place
rather than in a string literal repeated across two methods.

### Reading *another* node's artifacts

`find_lazy(sample_id, decoder, node=None)` is the consumer's counterpart: one sample's
`LazyRaw` artifacts of a given decoder, keyed by full union key. A kernel that eats what
another node produced — `VideoDumper` over a `VideoFrameKernel`'s stack — names the
**decoder** it can read rather than the node name some YAML happened to choose, the same
reasoning as `art.batch()`. The 0/1/many decision is deliberately left to the caller: only
it knows whether two matches mean "ambiguous, say `frames_node`" or something else.

Matching is on the key's *suffix*, not its last segment: a sample id may itself contain
slashes (`FlatDirLoader` with `recursive: true` ids a sample by its relative path).

### The batch travels inside the union

The loaded `Batch` lands under the source node's namespace (`load/batch`). One uniform
kernel signature, no special-cased batch parameter. `art.batch()` finds it by type and
raises if it is missing or ambiguous, so a kernel never hardcodes the loader's node name.
`find_batch()` is the same scan returning `None` instead of raising — the executor uses it
on paths that must not abort the job (logging, marking live samples after a node failure).

`without_transients()` drops `Batch`-valued keys before merging a batch's union into the
job union. Two reasons: union values are supposed to be primitives or `LazyRaw`, never
live Python objects; and `load/batch` would otherwise arrive with a *different* value on
every batch and trip the conflict check. This is a framework decision, not something a
kernel author sees — see [design.md](../design.md) §1.3.

### Mutation is a separate interface

Ordinary kernels return extensions and cannot mutate. `with_mutations` exists for
`DataArchiver` only, and the executor reaches it through a distinct interface
(`MutatingKernel.run_mutating` returning a `Mutation`), so "who may rewrite an artifact"
is answered by the type system rather than by convention. See
[archiver.md](archiver.md).
