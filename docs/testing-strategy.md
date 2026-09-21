# Testing strategy

Current tests cover term/vote transitions, stale-log vote rejection, leadership/no-op creation, higher-term step-down, conflict repair, quorum commit, ordered apply, persistence failure atomicity, persist-before-send orchestration, deterministic testing primitives, business rejection, and deterministic KV snapshots.

The seeded `TestCluster` now exercises three-node election/replication, split-vote recovery, isolated-minority elections, isolated-leader non-commit, former-leader step-down, leader replacement, restart reconstruction, divergent uncommitted suffix repair, duplicate/reordered delivery, local storage failure, state-hash convergence, and repeated failover/restart across ten seeds. Restartable durable-memory handles preserve term, vote, log, commit, applied, and snapshot metadata while application memory is recreated.

The target simulator owns virtual monotonic time, seeded randomness, durable memory across process restarts, and controllable message delivery/drop/delay/duplication/reordering/partitions. Each transition must assert monotonic terms/commit/applied positions, one vote per term, log matching, committed-entry immutability, current-term commit counting, and state-hash convergence. Every failure prints its seed.

Still required: explicit crash points between persistence/send/apply/client response, virtual timer-driven heartbeats and election schedules, paused processes, snapshot interruption/catch-up, longer property-generated event sequences with shrinking, and three real processes with SQLite and a real transport. No safety test should depend on sleep.

`test:integration` currently passes with no matching tests so the workspace command and CI lane exist without mislabeling memory tests as real-process integration. Removing `--passWithNoTests` is part of the Phase 5 acceptance criteria.
