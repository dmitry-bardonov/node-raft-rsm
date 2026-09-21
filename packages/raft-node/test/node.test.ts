import { describe, expect, it } from 'vitest';
import {
  CommandIdConflictError,
  StorageError,
  clusterId,
  nodeId,
  type RaftMessage,
  type ReplicatedStateMachine,
} from '@node-raft-rsm/core';
import { MemoryRaftStorage } from '@node-raft-rsm/storage-memory';
import { MemoryNetwork } from '@node-raft-rsm/transport-memory';
import { JsonCommandCodec, RaftNode } from '../src/index.js';

interface Command {
  readonly delta: number;
}

class Counter implements ReplicatedStateMachine<Command, number> {
  value = 0;
  public apply(command: Readonly<Command>): number {
    this.value += command.delta;
    return this.value;
  }
  public createSnapshot(): Uint8Array {
    return new TextEncoder().encode(String(this.value));
  }
  public restoreSnapshot(snapshot: Uint8Array): void {
    this.value = Number(new TextDecoder().decode(snapshot));
  }
}

const id = nodeId('solo');

describe('RaftNode', () => {
  it('persists before sending and resolves a proposal only after apply', async () => {
    const order: string[] = [];
    const storage = new MemoryRaftStorage();
    const originalPersist = storage.persist.bind(storage);
    storage.persist = async (batch) => {
      order.push('persist');
      await originalPersist(batch);
    };
    const transport = {
      start(_handler: (message: RaftMessage) => Promise<void>): Promise<void> {
        return Promise.resolve();
      },
      send(): Promise<void> {
        order.push('send');
        return Promise.resolve();
      },
      stop(): Promise<void> {
        return Promise.resolve();
      },
    };
    const machine = new Counter();
    const node = await RaftNode.create({
      nodeId: id,
      clusterId: clusterId('single'),
      members: [id],
      heartbeatInterval: 2,
      electionTimeoutMinMs: 10,
      electionTimeoutMaxMs: 20,
      automaticTimers: false,
      storage,
      transport,
      stateMachine: machine,
      codec: new JsonCommandCodec(validateCommand),
    });
    await node.start();
    await node.campaign();
    const result = await node.propose({ delta: 3 }, { commandId: 'one' });
    expect(result).toBe(3);
    expect(machine.value).toBe(3);
    expect(order[0]).toBe('persist');
    await node.stop();
  });

  it('elects a leader and replicates a command in a manually driven three-node cluster', async () => {
    const network = new MemoryNetwork();
    const ids = [nodeId('a'), nodeId('b'), nodeId('c')] as const;
    const machines = ids.map(() => new Counter());
    const nodes = await Promise.all(
      ids.map(async (member, index) => {
        const machine = machines[index];
        if (machine === undefined) throw new Error('missing test state machine');
        const node = await RaftNode.create({
          nodeId: member,
          clusterId: clusterId('three'),
          members: ids,
          heartbeatInterval: 2,
          electionTimeoutMinMs: 10,
          electionTimeoutMaxMs: 20,
          automaticTimers: false,
          storage: new MemoryRaftStorage(),
          transport: network.endpoint(member),
          stateMachine: machine,
          codec: new JsonCommandCodec(validateCommand),
        });
        await node.start();
        return node;
      }),
    );
    await nodes[0].campaign();
    await network.drain();
    expect(nodes[0].status.role).toBe('leader');
    const resultPromise = nodes[0].propose({ delta: 5 }, { commandId: 'five' });
    await network.waitForPending();
    await network.drain();
    expect(await resultPromise).toBe(5);
    await nodes[0].heartbeat();
    await network.drain();
    expect(machines.map((machine) => machine.value)).toEqual([5, 5, 5]);
    await Promise.all(nodes.map(async (node) => node.stop()));
  });

  it('replays committed but unapplied entries during recovery', async () => {
    const storage = new MemoryRaftStorage();
    const codec = new JsonCommandCodec(validateCommand);
    await storage.persist({
      hardState: { currentTerm: 3n, votedFor: null, commitIndex: 2n },
      entries: [
        {
          index: 1n,
          term: 2n,
          type: 'command',
          payload: codec.encode({ delta: 4 }),
          commandId: 'already-applied-before-crash',
        },
        {
          index: 2n,
          term: 3n,
          type: 'command',
          payload: codec.encode({ delta: 9 }),
          commandId: 'recovered',
        },
      ],
    });
    await storage.persist({ entries: [], appliedIndex: 1n });
    const machine = new Counter();
    const network = new MemoryNetwork();
    const node = await RaftNode.create({
      nodeId: id,
      clusterId: clusterId('recover'),
      members: [id],
      heartbeatInterval: 2,
      electionTimeoutMinMs: 10,
      electionTimeoutMaxMs: 20,
      automaticTimers: false,
      storage,
      transport: network.endpoint(id),
      stateMachine: machine,
      codec,
    });
    expect(machine.value).toBe(13);
    expect((await storage.load()).appliedIndex).toBe(2n);
    await node.start();
    await node.stop();
  });

  it('does not send a persistence-dependent message after storage failure', async () => {
    const storage = new MemoryRaftStorage();
    let sends = 0;
    const node = await RaftNode.create({
      nodeId: id,
      clusterId: clusterId('storage-failure'),
      members: [id],
      heartbeatInterval: 2,
      electionTimeoutMinMs: 10,
      electionTimeoutMaxMs: 20,
      automaticTimers: false,
      storage,
      transport: {
        start: () => Promise.resolve(),
        send: () => {
          sends += 1;
          return Promise.resolve();
        },
        stop: () => Promise.resolve(),
      },
      stateMachine: new Counter(),
      codec: new JsonCommandCodec(validateCommand),
    });
    await node.start();
    storage.failNext();
    await expect(node.campaign()).rejects.toBeInstanceOf(StorageError);
    expect(sends).toBe(0);
    await node.stop();
  });

  it('rejects reuse of a completed command ID with different bytes', async () => {
    const storage = new MemoryRaftStorage();
    const network = new MemoryNetwork();
    const node = await RaftNode.create({
      nodeId: id,
      clusterId: clusterId('dedup'),
      members: [id],
      heartbeatInterval: 2,
      electionTimeoutMinMs: 10,
      electionTimeoutMaxMs: 20,
      automaticTimers: false,
      storage,
      transport: network.endpoint(id),
      stateMachine: new Counter(),
      codec: new JsonCommandCodec(validateCommand),
    });
    await node.start();
    await node.campaign();
    await expect(node.propose({ delta: 1 }, { commandId: 'same' })).resolves.toBe(1);
    await expect(node.propose({ delta: 2 }, { commandId: 'same' })).rejects.toBeInstanceOf(
      CommandIdConflictError,
    );
    await node.stop();
  });
});

function validateCommand(value: unknown): Command {
  if (
    typeof value !== 'object' ||
    value === null ||
    !('delta' in value) ||
    typeof value.delta !== 'number'
  )
    throw new Error('invalid command');
  return { delta: value.delta };
}
