# State-machine contract

Applications implement `ReplicatedStateMachine<Command, Result>` with `apply`, `createSnapshot`, and `restoreSnapshot`, plus a validated `CommandCodec`. Identical snapshot bytes plus identical ordered commands must produce identical state and results on every member.

`apply` must not consult wall-clock time, randomness, local files, environment-specific values, or remote services to decide state. Generate IDs and timestamps before proposal and include them in the command. A normal business rejection is result data (the KV compare-and-set example returns `conflict`); throwing means the node cannot safely advance and becomes unhealthy.

Bad:

```ts
apply(command) { return { ...command, createdAt: Date.now() }; }
```

Good:

```ts
const command = { type: 'create', id: crypto.randomUUID(), createdAt: Date.now() };
await node.propose(command, { commandId: requestId });
```

External effects are not exactly-once. Use an idempotent consumer, fencing token, or replicated outbox. Rolling upgrades must decode every in-flight command and snapshot version. Snapshot serialization must be complete and deterministic; the KV example sorts keys before encoding.
