# Testing strategy

Current tests cover term/vote transitions, stale-log vote rejection, leadership/no-op creation, higher-term step-down, conflict repair, quorum commit, ordered apply, persistence failure atomicity, persist-before-send orchestration, deterministic testing primitives, a three-node in-memory election/replication, business rejection, and deterministic KV snapshots.

The target simulator owns virtual monotonic time, seeded randomness, durable memory across process restarts, and controllable message delivery/drop/delay/duplication/reordering/partitions. Each transition must assert monotonic terms/commit/applied positions, one vote per term, log matching, committed-entry immutability, current-term commit counting, and state-hash convergence. Every failure prints its seed.

Still required: split votes, crash points around persistence/send/apply, minority/majority partitions, conflicting leaders, storage faults, restart reconstruction, snapshot interruption/catch-up, property-generated schedules, and three real processes with SQLite and a real transport. No safety test should depend on sleep.

`test:integration` currently passes with no matching tests so the workspace command and CI lane exist without mislabeling memory tests as real-process integration. Removing `--passWithNoTests` is part of the Phase 5 acceptance criteria.
