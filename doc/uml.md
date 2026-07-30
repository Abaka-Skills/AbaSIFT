# Ego_QA — Architecture Diagrams

Mermaid sources; render in VS Code (Markdown Preview Mermaid extension) or GitHub.

## 1. Class diagram — core abstractions

```mermaid
classDiagram
    direction LR

    class LazyRaw {
        +uri: str
        +decoder: DecoderRef
        +decode() Any
        -_lock
        -_memo
    }
    note for LazyRaw "serializable (uri + decoder name)\ndisk cache: worker-global LRU\nmemo: released per batch run"

    class Sample {
        +sample_id: str
        +meta: dict
        +streams: dict~str, LazyRaw~
    }
    note for Sample "canonical stream names:\nvideo/cam_front, imu/left, captions ..."

    class Batch {
        +samples: list~Sample~
    }

    class ArtifactUnion {
        +get(key) Any
        +extend(node, ext)
        +union(other)
    }
    note for ArtifactUnion "flat map 'node/name' -> value\nextend-only; executor-owned union\nvalues: primitives | LazyRaw"

    class Report {
        +schema_version: int
        +job: dict
        +samples: dict~str, SampleReport~
        +summary: dict
    }

    class Kernel {
        <<interface>>
        +__init__(**params)
        +run(art, report) (ArtifactExt, ReportExt)
        +finalize(art, report) (ArtifactExt, ReportExt)
    }

    class SourceKernel {
        <<interface>>
        +__init__(**params)
        +iter_batches() Iterator
    }

    class DataDumper {
        +params: keys globs, target
        +run(art, report)
    }
    note for DataDumper "the ONE sanctioned mutator:\ndump -> swap value for LazyRaw\nempty target -> delete key\ndeterministic paths f(job,node,key)"

    class Pipeline {
        +name: str
        +nodes: list~NodeSpec~
        +from_yaml(path) Pipeline
        +validate()
    }

    class Executor {
        +run(pipeline) Report
        -thread_pool
        -merge_fragments()
    }

    Sample "1" o-- "n" LazyRaw : streams
    Batch "1" o-- "n" Sample
    ArtifactUnion o-- LazyRaw : values may be
    ArtifactUnion o-- Batch : load/batch
    DataDumper ..|> Kernel
    Pipeline o-- SourceKernel : node 0
    Pipeline o-- Kernel : nodes 1..n
    Executor --> Pipeline : executes
    Executor --> ArtifactUnion : owns merging
    Executor --> Report : aggregates status
```

## 2. Execution flow — per-batch streaming + terminal finalize

```mermaid
flowchart TB
    subgraph JOB["one job = one YAML on one machine"]
        SRC["SourceKernel.iter_batches()\n(enumerate vendor layout -> Samples of LazyRaws)"]
        SRC -->|"yield batch k"| DAG

        subgraph DAG["DAG run for batch k (threads)"]
            direction TB
            L["load/batch in ArtifactUnion"]
            L --> A["blur kernel\n(branch 1)"]
            L --> B["imu kernel\n(branch 2)"]
            A --> J["join: executor unions\n(diamond dups merge silently)"]
            B --> J
            J --> D["DataDumper\n(dump heavy artifacts / free)"]
        end

        DAG -->|"per-sample fragments\n(disjoint keys across batches)"| MERGE["dumb dict union\ninto job union + report"]
        MERGE -->|"release batch decode memo"| SRC
        MERGE --> FIN["all batches done:\nfinalize() per kernel, topo order\n-> summary"]
        FIN --> OUT["report.json + dumped artifacts\n(deterministic S3/local paths)"]
    end

    EXT["external splitter\n(generates per-machine YAMLs)"] -.-> JOB
    EXT -.-> JOB2["job YAML on machine 2 ..."]
```

## 3. Sequence — one batch through the executor

```mermaid
sequenceDiagram
    participant S as SourceKernel
    participant E as Executor
    participant K1 as Kernel blur (thread)
    participant K2 as Kernel imu (thread)
    participant LR as LazyRaw

    S->>E: yield (art_ext: load/batch, report_ext)
    E->>E: seed ArtifactUnion for this run
    par branch 1
        E->>K1: run(art, report)  [read-only]
        K1->>LR: decode()  (video)
        LR-->>K1: frames (memoized, locked)
        K1-->>E: (art_ext, report_ext)
    and branch 2
        E->>K2: run(art, report)  [read-only]
        K2->>LR: decode()  (imu)
        LR-->>K2: arrays
        K2-->>E: (art_ext, report_ext)
    end
    E->>E: union at join (namespaced keys, no conflicts)
    Note over E: kernel exception on a sample ->\nstatus=error, sample dropped downstream,\nbatchmates continue
    E->>E: merge per-sample fragments into job report
    E->>LR: release decode memo for batch
```

## 4. Failsafe layers

```mermaid
flowchart LR
    F1["vendor file unreadable\nat enumeration"] -->|"error sample in\ninitial fragment"| R["Report\n(job always completes)"]
    F2["LazyRaw.decode() fails\ninside a kernel"] -->|"status=error at that node,\nsample dropped downstream"| R
    F3["kernel raises\non one sample"] -->|"same; batchmates continue"| R
    F4["worker dies"] -->|"external system re-runs YAML;\ndeterministic dump paths => idempotent"| R
```
