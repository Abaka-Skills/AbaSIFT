# Target: Distributed Quality-Control System for Vendor Datasets

## Problem

Multiple vendors deliver egocentric datasets to our Amazon S3 bucket. Each vendor
uses its own directory and subdirectory organization, and datasets mix modalities
in heterogeneous formats — including but not limited to: video, images, IMU,
binary blobs, JSON, text, and per-frame captions.

We need an elastic, distributed quality-control system that checks this data at
scale: hundreds of workers spread across CPU/GPU slots and AWS instances, working
in a decentralized style (no central scheduler in the framework itself).

## System model

- **Dataloader (per vendor).** Each vendor gets a dataloader that normalizes its
  layout into a unified data structure (binary-serializable, loadable in batches).
  The loader — together with the datadumper — bridges remote S3, local storage,
  and local memory.
- **QC pipeline = a DAG of kernels, defined per-job in YAML.** Workers execute the
  pipeline. Each node is a processing kernel that takes an `artifact_union` and a
  base report, and returns an *extended* `artifact_union` and an *extended*
  report. Nodes may have multiple input edges; merging the incoming unions/reports
  at a join is the pipeline framework's responsibility, not the kernel's.
- **Output per job:** a quality report as a JSON object, plus the
  `artifact_union` — the union of artifacts produced by all nodes of the DAG.
- **Datadumper.** Saves a selected subset of the `artifact_union` back to S3 (or
  local disk); one implementation for now, more may follow.

For a complicated QC campaign this means: one YAML per (vendor, dataset, pipeline)
combination, generated per machine by an external splitter, with hundreds of
workers each running their YAML to completion independently.

## QC scope (kernels, implemented by other teams)

The framework ships the abstractions and a demo pipeline; check kernels are
written by others against the kernel interface. At minimum the system must
support checks for:

- **Visual artifacts** — compression artifacts, banding, corruption
- **Motion blur** that obscures hands, tools, or the workpiece
- **Excessive camera shake**
- **Poor lighting** — hands, tools, and the work surface must be free of heavy
  shadow or blow-out; footage whose dark regions are indiscernible is a defect
- **Synchronization failures** across cameras / IMU / VIO streams
- **Sensor corruption** — dead pixels, IMU spikes or frozen values, broken streams

## Deliverable

The framework/SDK (data structures, kernel interfaces, YAML pipeline format,
single-machine executor, datadumper, CLI) plus one demo pipeline that reports
each video's duration. The agreed design and decision log live in
[doc/design.md](doc/design.md); diagrams in [doc/uml.md](doc/uml.md).
