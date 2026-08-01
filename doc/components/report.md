# Report

Module: `abasift/report.py`.

## Division of labour

- **Kernels judge.** Only the kernel knows what its measurement means.
- **YAML holds thresholds.** Same kernel, per-vendor strictness, no code change. Asserted
  by `test_thresholds_live_in_yaml_not_code`: the same duration kernel turns a 10 s clip
  from `pass` into `fail` purely by changing `max_s`.
- **The framework only aggregates.** Sample status is the worst of its checks:
  `error > fail > warn > pass`.

`error` is a first-class status. Unreadable data is itself a quality defect, so a corrupt
file produces a finding, not a stack trace.

## Two documents, split by what a fact is about

A job emits both. Each has one job, and neither repeats the other (schema version **2**).

**`report.json` — per data sample.**

```json
{
  "schema_version": 2,
  "job": {"pipeline_hash": "...", "job_id": "...", "worker": "host:pid",
          "started_at": "...", "finished_at": "...", "elapsed_s": 21.6,
          "n_batches": 14, "n_samples": 53, "counts": {"pass": 50, "fail": 3, ...}},
  "samples": {
    "<sample_id>": {
      "status": "pass|warn|fail|error",
      "checks": {
        "<node>/<check>": {"status": "...", "measurement": 312.4, "details": {}}
      }
    }
  }
}
```

**`pipeline.json` — per node.** Same `job` block, so either file reads alone.

```json
{
  "schema_version": 2,
  "job": { ... as above ... },
  "pipeline": {"job_id": "...", "cache": {}},
  "nodes": [
    {"name": "duration", "kernel": "abasift.kernels.VideoDurationKernel",
     "params": {"max_s": 1800}, "inputs": ["load"],
     "counts": {"pass": 50, "fail": 3},
     "checks": {"<check>": {"counts": {"pass": 50, "fail": 3},
                            "threshold": {"max_s": 1800}}},
     "summary": { ... from that kernel's digest() ... }}
  ]
}
```

**Why the split.** A threshold, a param and a `digest()` summary are the same for every
sample the node judged. Repeating a threshold per sample is most of the bytes on a
100k-sample delivery, and it buries the part anyone reads. So a `Check` still *declares*
its threshold — that is part of the judging contract, and `ReportView` sees it — but it is
serialised once per node, in `pipeline.json`, alongside the tally of what that check
decided. `report.json` keeps only what differs sample to sample.

Each node appears **once**, carrying what it is (kernel, params, inputs — verbatim from the
YAML) and what it did (checks, summary) in the same entry. Listing them twice — a
definition array plus a results map keyed by name — would leave the reader joining the two
by hand, which is the duplication this document exists to avoid. `checks` and `summary` are
omitted entirely for a node that has none, so a loader or a writer is three lines.

A node's `counts` is the worst of *its own* checks per sample — the sample-status rule,
scoped to one node. Summing the per-check tallies instead would count a sample twice
whenever a node runs two checks, so the two numbers differ on purpose: `counts` says how
many **samples** this node judged each way, `checks[c].counts` says what each **check**
decided.

`threshold` is the one thing that looks like a copy of `params` and is not: it is what the
kernel actually judged against, per *check* rather than per node, and a kernel may derive
or default it (`ImuSpikeKernel` records `warn_spikes` whether or not the YAML set one).

The skeleton is enforced; leaves are free-form. A kernel returns a `ReportExt`
(`{sample_id: {check_name: Check}}` plus an optional `summary` dict) and never learns its
own position in the graph — the executor prefixes check names with the node name.

## Two mechanisms worth knowing

**Merging across batches is a dict union with no logic.** Per-sample keys are disjoint
across batches, which is what makes per-batch streaming viable at all
([design.md](../design.md) §4). `Report.merge` copies entries rather than aliasing them, so a batch report and the
job report can't corrupt each other.

**`ReportView` is how "dropped downstream" works.** Kernels get a read-only view whose
real job is `is_alive(sample_id)`: a sample whose status is already `error` failed upstream
and must be skipped, not re-decoded. No separate "dead sample" channel exists — the report
*is* the channel, which is only possible because `error` is a real status.

## One node, one check key

When a `SampleKernel` subclass defines `check_name`, a failure is reported under that same
name with `status: error` and `details.exception`. So a node always contributes exactly one
check key per sample whether it succeeded or not, and `summary` arithmetic over
`imu/imu_spike` doesn't have to look for a stray `imu/error` key as well.
