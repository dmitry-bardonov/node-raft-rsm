# Snapshots

The internal snapshot metadata model records format version, cluster ID, member set, included index/term, checksum, and data. The KV state machine already supports deterministic snapshot bytes and complete restore. Snapshot transport messages are intentionally absent from the public protocol until the consensus snapshot pipeline is complete.

The release design requires immutable snapshot files, SHA-256 verification, a conservative size limit or chunked transfer, atomic metadata installation, and compaction only after durable validation. Installation may never move the included index backward. Recovery restores the snapshot first and replays only entries after its boundary. Interrupted transfers retain the prior usable snapshot. Format compatibility must be maintained through rolling upgrades.
