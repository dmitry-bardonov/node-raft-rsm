# Getting started: embed Raft in an application

This guide shows how to turn application mutations into deterministic commands and run them through `node-raft-rsm`. The current adapters are intended for development and deterministic testing. Do not deploy this version for production data until the durable SQLite adapter, real authenticated transport, snapshot pipeline, and multi-process fault suite are complete.

## 1. Confirm that embedded Raft is appropriate

Use this SDK when every process owns a local copy of application state and the processes must agree on one ordered mutation stream. A shared-database CRUD API normally does not need embedded Raft: keep the API stateless and let the database provide consistency.

Start with three members. Three members tolerate one unavailable member while preserving quorum. Every member needs:

- a permanent node ID;
- the same permanent cluster ID and member list;
- independent durable storage;
- a transport address reachable by every other member;
- identical command, codec, and state-machine versions.

Membership is fixed in the current release. Do not add or remove voters dynamically.

## 2. Add the workspace packages

The packages are not published to npm yet. Inside this repository they are consumed through pnpm workspace dependencies:

```json
{
  "dependencies": {
    "@node-raft-rsm/core": "workspace:*",
    "@node-raft-rsm/node": "workspace:*",
    "@node-raft-rsm/storage-memory": "workspace:*",
    "@node-raft-rsm/transport-memory": "workspace:*"
  }
}
```

Run the included example before integrating:

```sh
nvm use
pnpm install
pnpm example:kv
```

The example elects a leader, commits commands, and verifies equal state hashes across three replicas.

## 3. Define commands and results

Commands describe deterministic state transitions. Put nondeterministic inputs—timestamps, generated IDs, user identity, or externally obtained values—into the command before proposal.

```ts
export type AccountCommand =
  | {
      readonly type: 'open-account';
      readonly accountId: string;
      readonly openedAt: string;
    }
  | {
      readonly type: 'credit';
      readonly accountId: string;
      readonly amountCents: bigint;
      readonly transferId: string;
    };

export type AccountResult =
  | { readonly status: 'applied'; readonly balanceCents: bigint }
  | { readonly status: 'rejected'; readonly reason: 'exists' | 'missing' };
```

Business rejection is normal result data. It must not throw. The rejected command still occupies a committed log position, ensuring every replica makes the same decision.

## 4. Implement and validate a command codec

The codec boundary receives untrusted bytes. TypeScript types do not validate runtime data. `JsonCommandCodec` therefore requires a validator:

```ts
import { JsonCommandCodec } from '@node-raft-rsm/node';
import type { AccountCommand } from './commands.js';

export const accountCodec = new JsonCommandCodec<AccountCommand>((value) => {
  if (typeof value !== 'object' || value === null || !('type' in value)) {
    throw new Error('invalid account command');
  }

  // Validate every field and bound string/collection sizes here.
  // Return a newly constructed AccountCommand rather than trusting a cast.
  return validateAccountCommand(value);
});
```

JSON does not directly represent `bigint`; encode such fields as decimal strings at the JSON boundary or use a binary codec. The codec must always produce identical bytes for the same logical command.

## 5. Implement the replicated state machine

```ts
import type {
  ApplyContext,
  ReplicatedStateMachine,
  RestoreContext,
  SnapshotContext,
} from '@node-raft-rsm/core';
import type { AccountCommand, AccountResult } from './commands.js';

export class AccountStateMachine implements ReplicatedStateMachine<AccountCommand, AccountResult> {
  readonly #balances = new Map<string, bigint>();

  apply(command: Readonly<AccountCommand>, _context: ApplyContext): AccountResult {
    switch (command.type) {
      case 'open-account':
        if (this.#balances.has(command.accountId)) {
          return { status: 'rejected', reason: 'exists' };
        }
        this.#balances.set(command.accountId, 0n);
        return { status: 'applied', balanceCents: 0n };

      case 'credit': {
        const balance = this.#balances.get(command.accountId);
        if (balance === undefined) return { status: 'rejected', reason: 'missing' };
        const next = balance + command.amountCents;
        this.#balances.set(command.accountId, next);
        return { status: 'applied', balanceCents: next };
      }
    }
  }

  createSnapshot(_context: SnapshotContext): Uint8Array {
    const entries = [...this.#balances.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([id, balance]) => [id, balance.toString()]);
    return new TextEncoder().encode(JSON.stringify({ version: 1, entries }));
  }

  restoreSnapshot(bytes: Uint8Array, _context: RestoreContext): void {
    const snapshot = validateAccountSnapshot(bytes);
    this.#balances.clear();
    for (const [id, balance] of snapshot.entries) this.#balances.set(id, BigInt(balance));
  }

  getBalance(accountId: string): bigint | null {
    return this.#balances.get(accountId) ?? null;
  }
}
```

`apply` must not use `Date.now()`, random values, environment variables, local files, or remote calls to decide state. It is serialized by the runtime and must either apply index `N` successfully or stop the node; the SDK cannot skip a failed committed entry.

## 6. Create a node

The following uses the in-memory adapters. They are useful for local integration and tests, not durable deployment:

```ts
import { clusterId, nodeId } from '@node-raft-rsm/core';
import { RaftNode } from '@node-raft-rsm/node';
import { MemoryRaftStorage } from '@node-raft-rsm/storage-memory';
import { MemoryNetwork } from '@node-raft-rsm/transport-memory';
import { accountCodec } from './codec.js';
import { AccountStateMachine } from './state-machine.js';

const members = [nodeId('accounts-0'), nodeId('accounts-1'), nodeId('accounts-2')] as const;
const network = new MemoryNetwork();
const localId = members[0];
const stateMachine = new AccountStateMachine();

const node = await RaftNode.create({
  nodeId: localId,
  clusterId: clusterId('accounts-production'),
  members,
  heartbeatInterval: 100,
  electionTimeoutMinMs: 500,
  electionTimeoutMaxMs: 900,
  storage: new MemoryRaftStorage(),
  transport: network.endpoint(localId),
  stateMachine,
  codec: accountCodec,
  onEvent: (event) => structuredLog(event),
});

await node.start();
```

For a real deployment, replace both memory adapters. Each process creates only its own node and transport endpoint. IDs, member order, cluster ID, and peer addresses come from validated static configuration. Do not generate a new identity on restart.

Timeouts are operational settings, not universal constants. Election timeout must exceed heartbeat interval and should comfortably exceed normal network, disk, event-loop, and GC latency.

## 7. Route mutations through `propose`

Never mutate replicated state directly in an HTTP handler or background job. Create the complete command, then propose it:

```ts
import { randomUUID } from 'node:crypto';
import { NotLeaderError, ProposalTimeoutError } from '@node-raft-rsm/core';

const commandId = request.headers['idempotency-key'] ?? randomUUID();
const command = {
  type: 'open-account' as const,
  accountId: request.body.accountId,
  openedAt: new Date().toISOString(),
};

try {
  const result = await node.propose(command, { commandId, timeoutMs: 5_000 });
  response.send(result);
} catch (error) {
  if (error instanceof NotLeaderError) {
    response.status(503).send({ code: 'NOT_LEADER', leaderHint: error.leaderHint });
  } else if (error instanceof ProposalTimeoutError) {
    response.status(504).send({
      code: 'OUTCOME_UNKNOWN',
      commandId,
      retry: 'Retry identical command bytes with the same command ID',
    });
  } else {
    throw error;
  }
}
```

`propose()` resolves only after the entry is committed and applied locally. A timeout is ambiguous: the command might already be committed. Retrying with a new ID could apply it twice. Reuse the same ID and identical bytes.

Followers return `NotLeaderError`; automatic forwarding is not implemented. Your API gateway or client may retry the leader hint, but must preserve the command ID and bytes.

## 8. Make read consistency explicit

Only local reads are currently implemented:

```ts
const balance = await node.read(() => stateMachine.getBalance(accountId), {
  consistency: 'local',
});
```

A local read may be stale, including on a former leader in a partition. Do not expose it as linearizable. Strong reads require a future ReadIndex/quorum-confirmation implementation.

## 9. Integrate lifecycle and health

Start Raft after configuration and storage recovery, before accepting mutations. On shutdown, stop accepting requests and close the node:

```ts
async function shutdown(): Promise<void> {
  server.close();
  await node.stop();
}

process.once('SIGTERM', () => void shutdown());
process.once('SIGINT', () => void shutdown());
```

Readiness should mean storage recovery completed and the node is sufficiently caught up for your policy. Loss of quorum must make writes unavailable; it is not a reason to bypass Raft. Log status and lifecycle events, but never log opaque command payloads by default.

## 10. Test the integration

At minimum, verify:

1. three members elect one leader;
2. committed commands reach identical state on all caught-up members;
3. a minority partition cannot complete writes;
4. the majority replaces a failed leader;
5. restart reconstructs state from snapshot plus committed log;
6. retrying the same command ID returns one logical result;
7. storage failure cannot produce a success response;
8. an application rejection is replicated result data;
9. a thrown `apply` error makes the node unhealthy;
10. local reads are labeled as potentially stale.

Use the deterministic adapters to control delivery rather than adding sleeps. See the [testing strategy](./testing-strategy.md), [failure semantics](./failure-semantics.md), and runnable [KV example](../examples/replicated-kv/README.md).

## Production-readiness checklist

The current repository does **not** yet satisfy this checklist. Before using real data, require:

- Worker-owned SQLite storage with transaction/crash tests;
- a bounded authenticated and encrypted real transport;
- durable snapshot installation and compaction;
- durable command-ID retention semantics;
- deterministic partition/crash/property tests;
- three-process failover and restart tests;
- stable package names, a license, and a supported release policy.
