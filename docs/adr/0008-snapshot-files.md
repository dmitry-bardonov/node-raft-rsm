# ADR 0008: Immutable snapshot files

Status: planned, not implemented.

Large snapshots will be immutable files installed by temp-write, sync, checksum, atomic rename, metadata commit, then compaction. Until the interruption/recovery tests pass, protocol snapshot types are not a claim of complete snapshot support.
