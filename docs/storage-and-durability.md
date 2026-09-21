# Storage and durability

Persistent state is the current term, vote, committed log, commit position, applied position, and snapshot. Leader peer progress is volatile. `commitIndex` and `appliedIndex` are distinct: a committed command may still be waiting for application.

The current repository includes only atomic memory storage for deterministic tests. The planned SQLite adapter will use a dedicated Worker Thread, one connection, WAL mode, `synchronous=FULL`, explicit transactions, and 64-bit integer bindings without conversion through JavaScript `number`. Proposed tables are metadata, log entries, and snapshot metadata; large snapshot bytes belong in immutable files.

Crash ordering is conservative: no vote grant or append success is sent before its term/vote/log transaction commits. If the process dies after persistence but before send, recovery may retry safely. If it dies after quorum commit but before responding to a client, the outcome is ambiguous and the client retries the same command ID. Disk-full or transaction failure makes the node unhealthy; it must not acknowledge success.

The current state-machine contract reconstructs application memory by restoring the latest snapshot and replaying every committed command after the snapshot boundary. Persisted `appliedIndex` records Raft progress and diagnostics; it is not evidence that a fresh in-memory state-machine instance still contains those effects. A future durable application-state adapter will need an explicit alternative recovery contract before replay can be skipped safely.

Snapshot file ordering will be temporary write → file sync → checksum validation → atomic rename → metadata transaction → log compaction. A Kubernetes member owns one ReadWriteOnce volume; replacing its PVC is replacement/reprovisioning, not restart.
