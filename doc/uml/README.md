# AbaSift — architecture diagrams (mermaid source)

Rendered visual version: **[index.html](index.html)** — open it in a browser (styles and page
script are local files under [assets/](assets/); no CDN, no build step). This file is the text
source for the same architecture; keep the two in step when the design changes. Mermaid renders in VS Code (Markdown Preview Mermaid) or GitHub.

## 1. Module layers — imports point one way

```mermaid
flowchart TB
    CLI["cli.py"] --> PIPE["pipeline.py<br/>YAML → validated DAG"]
    CLI --> EXEC["executor.py<br/>per-batch streaming, threads"]
    EXEC --> PIPE
    PIPE --> CORE
    EXEC --> CORE
    KERN["kernels/<br/>duration · imu_spike · dumper"] --> CORE
    LOAD["loaders/<br/>flat_dir · egoverse"] --> CORE
    CORE["core contracts<br/>data · report · kernel · lazy · payloads · errors · cache · decoders"]
    CORE -->|"decoders.py only"| VEND["vendor/dji_telemetry.py"]
```

Kernels and loaders depend on the core and nothing else — never on the executor, never on each
other. Vendor format knowledge is a leaf, reached only through a registered decoder.

## 2. Data plane — what flows through the DAG

```mermaid
classDiagram
    direction LR

    class LazyRaw {
        +uri: str
        +decoder: str
        +opts: dict
        +open() seekable
        +local_path() str
        +read_bytes(max_bytes) bytes
        +decode() Any
        +release()
    }
    note for LazyRaw "serializable (uri + decoder)\nthe decoder picks the access mode"

    class DiskCache {
        +root, capacity_bytes
        +materialize(uri, fetch) Path
        +evict()
    }
    note for DiskCache "tier 1: worker-global LRU\natomic .part rename, per-URI lock"

    class Sample {
        +sample_id: str
        +streams: dict~str, LazyRaw~
        +meta: dict
    }
    note for Sample "stream name = 'kind/name', validated:\nvideo image audio imu annotation blob"

    class Batch {
        +samples: tuple~Sample~
        +index: int
        +release()
    }

    class ArtifactUnion {
        +deleted: frozenset
        +extended(node, ext) ArtifactUnion
        +union(other) ArtifactUnion
        +with_mutations(replace, delete) ArtifactUnion
        +batch() Batch
        +find_batch() Batch
        +under(node) dict
        +per_sample(node, name) dict
        +without_transients() ArtifactUnion
    }
    note for ArtifactUnion "flat 'node/name' → value\nextend-only, immutable:\nevery write returns a new instance"

    class VideoMeta { +duration_s +container +video +audio +data_tracks }
    class ImuTrack { +t +accel_g +quat +rate_hz +device +layout }

    Sample "1" o-- "n" LazyRaw : streams
    Batch "1" o-- "n" Sample
    ArtifactUnion o-- Batch : load/batch
    LazyRaw ..> DiskCache : local_path()
    LazyRaw ..> VideoMeta : decode() video_meta
    LazyRaw ..> ImuTrack : decode() dji_imu
```

## 3. Control plane — kernels, pipeline, report

```mermaid
classDiagram
    direction LR

    class SourceKernel {
        <<interface>>
        +iter_batches() Iterator
    }
    note for SourceKernel "batch_stream(items, batch_size) groups a loader's\noutput — batching is framework policy, not a vendor's"
    class Kernel {
        <<interface>>
        +run(art, report) (ArtifactExt, ReportExt)
        +finalize(art, report) (ArtifactExt, ReportExt)
    }
    class SampleKernel {
        +check_name: str
        +check(sample, art)
    }
    note for SampleKernel "owns the per-sample failsafe:\nskips samples already error upstream,\nturns an exception into status: error"

    class MutatingKernel {
        +run_mutating(art, report) Mutation
        +finalize_mutating(art, report) Mutation
    }
    note for MutatingKernel "framework-internal — DataDumper only.\ndump → LazyRaw swap, empty target → free,\npaths f(job_id, node, key), no timestamps"

    class Pipeline {
        +name, job_id, source
        +nodes: tuple~NodeSpec~
        +from_yaml(path) Pipeline
        +validated() Pipeline
        +topo_order() list
        +hash() str
    }
    class NodeSpec { +name +kernel +params +inputs }
    note for NodeSpec "params are strict — kernels declare them\nexplicitly, so a typo fails at load time"

    class Executor {
        +run() Report
        +artifacts: ArtifactUnion
    }
    note for Executor "owns joins, status aggregation,\nand every failsafe layer"

    class Report {
        +schema_version: int
        +job, samples, summary
        +apply(node, ext)
        +merge(other)
        +counts()
    }
    class Check { +status +measurement +threshold +details }
    class ReportExt { +checks +summary }
    class ReportView { +is_alive(sid) +status_of(sid) }

    SampleKernel --|> Kernel
    MutatingKernel --|> Kernel
    Pipeline o-- NodeSpec
    Executor --> Pipeline : executes
    Executor --> Report : aggregates
    Kernel ..> ReportExt : returns
    Kernel ..> ReportView : reads
    ReportExt o-- Check
    Report o-- Check
```

## 4. Execution flow — per-batch streaming + terminal finalize

```mermaid
flowchart TB
    subgraph JOB["one job = one YAML on one machine"]
        SRC["SourceKernel.iter_batches()<br/>enumerate vendor layout → Samples of LazyRaws"]
        SRC -->|"yield batch k"| DAG

        subgraph DAG["DAG run for batch k (threads)"]
            direction TB
            L["load/batch in the ArtifactUnion"]
            L --> A["duration kernel<br/>(branch 1)"]
            L --> B["imu kernel<br/>(branch 2)"]
            A --> J["join: executor unions inputs<br/>(diamond dups merge silently)"]
            B --> J
            J --> D["DataDumper<br/>dump heavy artifacts / free"]
        end

        DAG -->|"per-sample fragments<br/>(disjoint keys across batches)"| MERGE["dumb dict union<br/>into job union + report"]
        MERGE -->|"batch.release(): decoded payloads freed"| SRC
        MERGE --> FIN["all batches done:<br/>finalize() per kernel, topo order → summary"]
        FIN --> STAMP["stamp job.counts / n_samples / elapsed_s"]
        STAMP --> DUMP["finalize_mutating(): dumpers write<br/>the COMPLETE report"]
        DUMP --> OUT["report.json + dumped artifacts<br/>(deterministic paths)"]
    end

    EXT["external splitter<br/>(generates per-machine YAMLs)"] -.-> JOB
    EXT -.-> JOB2["job YAML on machine 2 ..."]
```

A node's inputs are the union **and report** of its *ancestors*, never global state — so which
samples a node considers alive does not depend on thread scheduling.

## 5. Sequence — one batch through the executor

```mermaid
sequenceDiagram
    participant S as SourceKernel
    participant E as Executor (driver loop)
    participant K1 as duration (thread)
    participant K2 as imu (thread)
    participant LR as LazyRaw

    S->>E: yield ({"batch": Batch}, ReportExt)
    E->>E: seed union + report for this run
    par branch 1
        E->>K1: run(ancestor union, ancestor ReportView)
        K1->>LR: decode() video_meta → open(), ranged GETs
        LR-->>K1: VideoMeta (memoized, locked)
        K1-->>E: (ArtifactExt, ReportExt)
    and branch 2
        E->>K2: run(ancestor union, ancestor ReportView)
        K2->>LR: decode() dji_imu → local_path(), disk cache
        LR-->>K2: ImuTrack (memoized, locked)
        K2-->>E: (ArtifactExt, ReportExt)
    end
    E->>E: union at join (namespaced keys, no conflicts)
    Note over E: kernel exception on a sample →<br/>status=error, dropped downstream,<br/>batchmates continue
    E->>E: merge per-sample fragments into the job report
    E->>LR: release() — decode memo dropped
```

## 6. Failsafe layers

```mermaid
flowchart LR
    F1["vendor item unreadable<br/>at enumeration"] -->|"error sample in<br/>initial fragment"| R
    F2["source raises<br/>mid-iteration"] -->|"summary.source_error;<br/>job completes with what it got"| R
    F3["LazyRaw.decode() or kernel<br/>fails on one sample"] -->|"status=error at that node,<br/>sample dropped downstream"| R
    F4["kernel raises before<br/>touching any sample"] -->|"every live sample in the batch<br/>gets error at that node"| R
    F5["worker dies"] -->|"external system re-runs the YAML;<br/>timestamp-free dump paths ⇒ idempotent"| R
    R["Report<br/>(job always completes)"]
```
