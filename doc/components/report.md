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

## Shape

```json
{
  "schema_version": 1,
  "job": {"pipeline": "...", "pipeline_hash": "...", "job_id": "...", "worker": "host:pid",
          "started_at": "...", "finished_at": "...", "elapsed_s": 21.6,
          "n_batches": 14, "n_samples": 53, "counts": {"pass": 50, "fail": 3, ...}},
  "samples": {
    "<sample_id>": {
      "status": "pass|warn|fail|error",
      "checks": {
        "<node>/<check>": {"status": "...", "measurement": 312.4,
                           "threshold": {"max_s": 1800}, "details": {}}
      }
    }
  },
  "summary": {"<node>": { ... from that kernel's finalize() ... }}
}
```

The skeleton is enforced; leaves are free-form. A kernel returns a `ReportExt`
(`{sample_id: {check_name: Check}}` plus an optional `summary` dict) and never learns its
own position in the graph — the executor prefixes check names with the node name.

## Two mechanisms worth knowing

**Merging across batches is a dict union with no logic.** Per-sample keys are disjoint
across batches, which is what makes per-batch streaming viable at all (design decision
#11). `Report.merge` copies entries rather than aliasing them, so a batch report and the
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
