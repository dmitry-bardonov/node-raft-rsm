# Architecture

`raft-core` is a deterministic transition engine. It owns roles, terms, votes, log matching, peer progress, and commitment, but imports no timers, network, filesystem, or database. `raft-node` owns lifecycle and serializes external events. `raft-storage-memory` and `raft-transport-memory` are deterministic adapters. `raft-testing` provides seeded randomness and virtual time. The KV example is application code, not part of consensus.

```mermaid
flowchart TB
  API[Typed proposal API] --> RT[raft-node runtime]
  NET[Transport] --> RT
  RT --> CORE[Pure raft-core]
  CORE --> READY[Ready batch]
  READY --> STORE[RaftStorage]
  STORE --> SEND[Transport sends]
  SEND --> APPLY[Serialized state-machine apply]
  APPLY --> ADV[advance]
  ADV --> CORE
```

Every Ready batch is processed conservatively: persist hard state, truncation, entries, and snapshot metadata atomically; await durability; send messages; apply committed entries in index order; persist the applied position; advance the core. Vote grants and successful append acknowledgments share that barrier.

```mermaid
sequenceDiagram
  participant Client
  participant Runtime
  participant Core
  participant Disk
  participant Peers
  participant App
  Client->>Runtime: propose(command, commandId)
  Runtime->>Core: proposal event
  Core-->>Runtime: Ready(entry, AppendEntries)
  Runtime->>Disk: atomic persist
  Disk-->>Runtime: durable
  Runtime->>Peers: AppendEntries
  Peers-->>Runtime: quorum acknowledgments
  Runtime->>Core: response events
  Core-->>Runtime: Ready(commit)
  Runtime->>Disk: persist commit index
  Runtime->>App: apply in index order
  App-->>Runtime: typed result
  Runtime-->>Client: resolve
```

Recovery loads hard state, snapshot metadata/data, log entries, and applied position before constructing the core. Snapshot restoration precedes later replay. Full replay and durable deduplication are not implemented yet and remain release blockers.
