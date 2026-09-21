# Replicated KV example

This package contains a deterministic state machine, its validated command boundary, and a runnable three-node in-memory cluster. From the repository root:

```sh
nvm use
pnpm install
pnpm example:kv
```

The demo deterministically elects `kv-0`, commits a put and compare-and-set, demonstrates a normal business conflict, and verifies matching values and state hashes across all three replicas. It exits non-zero if election, replication, or convergence fails.

The HTTP, durable SQLite, Docker Compose, and multi-process layers are deliberately deferred until those adapters have their own crash and protocol tests; see the root roadmap. Reads against `KvStateMachine#get` are local and may be stale.

The important retry rule is unchanged: callers generate a command ID before proposal and reuse it after an ambiguous timeout. A command ID must never be reused with different command bytes.
