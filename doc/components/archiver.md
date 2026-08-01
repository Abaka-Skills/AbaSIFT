# DataArchiver — the one sanctioned mutator

Module: `abasift/kernels/archiver.py`.

**Archiver vs dumper.** A *dumper* (`VideoDumper`) writes something to `target` and leaves
the union as it found it. An *archiver* takes an object out of the job's working set —
writing the value out and swapping it for a handle, or dropping it outright. Writing is the
part they share (both use the `DumpTarget` scheme in `_dump.py`); removing the in-memory
object is what makes this one a `MutatingKernel`, and why it is the only one.

Ordinary kernels are extend-only. `DataArchiver` is the single exception, and it reaches that
privilege through a distinct interface (`MutatingKernel.run_mutating` → `Mutation`) rather
than by convention, so "who may rewrite an artifact" is a type-level answer.

## Two modes

**archive** (`target` set) — write each matching artifact out, then replace its in-union value
with a `LazyRaw` pointing at what was written. Memory freed, information preserved: a
downstream *reader* calls `.decode()` and gets the value back either way.

**A reduce is not a reader.** `sum()` over handles raises, and it is right to raise: once a
key is archived, the object is out of the job's working set and arithmetic over it is a
category error, not an inconvenience. So the rule is a rule, not a caveat —

> **Archive intermediates. Never archive a key some node reduces in `digest()`.**

`duration/duration_s/<id>` is a per-sample fact its own node reads back; a frame stack is an
intermediate nobody reduces. The framework does not police this and will not decode on your
behalf; it fails loudly at the reduce.

- `LazyRaw` values are copied with `shutil.copyfileobj` in 8 MB chunks — a 2 GB video never
  lands in RAM — and the replacement keeps the original decoder and opts.
- `bytes` values are written as `.bin`, anything else as `.json`.
- The replacement reaches the job union because only **leaf** unions are merged per batch —
  the archived node's own pre-mutation copy never gets a vote. See
  [executor.md](executor.md#why-the-batch-merge-only-looks-at-leaves).

**Two archivers may not replace the same key in parallel branches** — one key, two values,
no honest winner. Deletes have the sticky `deleted` set for exactly this; replaces have no
equivalent, so the pipeline is rejected at **load time** rather than raising mid-job:

```
PipelineError: nodes 'dA' and 'dB' both archive 'dur/duration_s/*' on parallel branches;
put one downstream of the other, or give them disjoint keys
```

Free mode is exempt (deleting twice is deleting once), and so are the `__report__` /
`__pipeline__` pseudo-keys, which never enter the union.

**free** (`target` empty) — drop the matching keys and call `DiskCache.forget(uri)` on each,
which deletes the backing file only if it lives inside the cache root. The archiver does not
derive that path itself: the key scheme and the "never delete outside the cache" guarantee
stay in `cache.py`, where they belong. For intermediates nobody downstream reads.

## Paths

The scheme below lives in `kernels/_dump.py` (`DumpTarget`), not in this class: `VideoDumper`
writes to a `target` too, and two writing kernels drifting apart on where a retry lands would
be a silent correctness bug. Same reasoning as `loaders/_fs.py` — a private shared helper, so
sibling kernels never import each other.

```
{job_root}/pipeline.yaml                        # the config, shared by every run
{job_root}/[{unix ts}/]{node}/{key with '/' -> '__'}
{job_root}/[{unix ts}/]{node}/report.json       # __report__   — one entry per sample
{job_root}/[{unix ts}/]{node}/pipeline.json     # __pipeline__ — one entry per node

job_root = {target or "dump"}/{job_id}_{pipeline_hash}
```

`run_root` depends on how `target` was given, and the difference is deliberate:

| `target` in YAML | `run_root` | why |
|---|---|---|
| omitted | `dump/<job>/<unix ts>` | Interactive runs get a subtree per run *inside* the job's directory, so a job's history is one `ls`. **Relative, never absolute**, so a YAML is portable between machines. |
| set (`/data/out`, `s3://…`) | `{target}/<job>`, **unstamped** | `f(job_id, pipeline_hash, node, key)` with no timestamps, so a retried job overwrites rather than duplicates (`test_rerunning_the_same_yaml_overwrites_the_same_paths`). |
| `""` (explicit) | — | *free* mode. |

So the no-timestamps rule still holds wherever it matters: anything an external splitter
generates sets `target` explicitly, and stays idempotent. What the default trades away is
that idempotency — **every run gets its own subtree**, so nothing a default run writes can
collide with an earlier one, and the subtrees accumulate until someone deletes them.
That is the right trade for an interactive run and the wrong one for a production job,
which is exactly the split.

### Why the directory is `{job_id}_{pipeline_hash}` and not just the id

A shard is retried under the *same* id — that is what makes retries idempotent. But a YAML
that was **edited** between attempts is a different job in every sense that matters:
different thresholds, possibly different kernels. Writing it over the first run's artifacts
would destroy the evidence behind a verdict someone may already have read, and leave a
directory whose contents no longer match any definition you can point at.

Appending the definition digest splits those two cases without putting a timestamp
anywhere near the path: same definition → same directory → retry overwrites; changed
definition → new directory → nothing is lost. `test_an_edited_yaml_dumps_beside_the_old_run_rather_than_over_it`
pins it.

The stamp is `job.started_unix`, read from the job's start time **once per job**, so every
writer in a DAG agrees, and it is in both documents' `job` block. Job first, run second, so
that one job's history is a single `ls` — the first version had the timestamp outermost and
scattered a shard's runs across sibling trees.

Free mode is **opt-in**: `target` must be explicitly `""`. Omitting it gives you the
stamped write, never a silent delete — the destructive mode is the one you have to ask for.

## Two documents, split by what a fact is about

`report.json` is **per sample**: status, measurement, details. `pipeline.json` is **per
node**: the kernel, its params, the thresholds behind its verdicts, the tally of what it
decided, and its `digest()` summary. Both carry the same `job` block, so either reads
alone.

The split removes real duplication: a threshold is identical for every sample a node
judged, so on a 100k-sample delivery repeating it per sample is most of the file — and it
buries the part anyone actually reads. `__pipeline__` also copies the source YAML to
`{job_root}/pipeline.yaml` (verbatim, comments and all; a pipeline built in code has its
definition serialised instead). It sits beside the runs rather than inside one because
every run under that directory shares it — which is what the hash in the name asserts.

## Both documents are written after digest

`run_mutating` handles artifact keys per batch. The two pseudo-keys are handled in
`commit`, which the executor runs *after* the digest pass and after stamping
`job.counts` — so what lands is the complete picture, not a half-written one: every
`digest()` summary folded in, every count final. The test asserts the written counts equal
the in-memory report's.

## Placement is the author's job

An archiver is an explicit YAML node, visible in the graph. It must sit **downstream** of
everything that reads the keys it takes away; an archiver running concurrently with a reader of the
same key is an authoring error, not something the framework second-guesses. Freeing is a
documented pattern to apply after heavy stages, not a hidden optimisation.
