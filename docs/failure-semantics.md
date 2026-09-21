# Failure semantics

- Before local append: no command exists; a retry is ordinary.
- After local persistence but before quorum: the entry may later be overwritten if uncommitted.
- After quorum commit but before response: outcome is ambiguous; retry identical bytes with the same command ID.
- Follower crash: quorum can continue; recovery reloads durable term/vote/log.
- Minority partition: it cannot elect or commit. A former leader may temporarily believe it leads but cannot complete proposals.
- Majority partition: it can elect and commit; the old leader steps down when it sees the higher term.
- Storage failure: the runtime becomes unhealthy before sending a persistence-dependent acknowledgment.
- State-machine exception: application stops at that index; skipping it would diverge replicas.
- Transport failure: messages may drop; Raft retries. A single send failure is not proof of no quorum.

In-process command results are retained in a bounded cache (default 10,000). Deduplication is not yet durable across restart, so applications must not treat the current implementation as exactly-once.
