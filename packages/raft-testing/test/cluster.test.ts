import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { NodeStoppedError, StorageError, type ReplicatedStateMachine } from '@node-raft-rsm/core';
import { JsonCommandCodec } from '@node-raft-rsm/node';
import { TestCluster } from '../src/index.js';

interface CounterCommand {
  readonly delta: number;
}

class CounterMachine implements ReplicatedStateMachine<CounterCommand, number> {
  value = 0;

  public apply(command: Readonly<CounterCommand>): number {
    this.value += command.delta;
    return this.value;
  }

  public createSnapshot(): Uint8Array {
    return new TextEncoder().encode(JSON.stringify({ version: 1, value: this.value }));
  }

  public restoreSnapshot(snapshot: Uint8Array): void {
    const value: unknown = JSON.parse(new TextDecoder().decode(snapshot));
    if (
      typeof value !== 'object' ||
      value === null ||
      !('version' in value) ||
      value.version !== 1 ||
      !('value' in value) ||
      typeof value.value !== 'number'
    )
      throw new Error('invalid counter snapshot');
    this.value = value.value;
  }

  public hash(): string {
    return createHash('sha256').update(this.createSnapshot()).digest('hex');
  }
}

describe('TestCluster failure scenarios', () => {
  it('elects one leader and converges after a committed proposal', async () => {
    const cluster = await createCluster(101);
    try {
      const [leader] = cluster.members;
      await cluster.campaign(leader);
      await cluster.drain();
      expect(cluster.status(leader).role).toBe('leader');
      await expect(cluster.propose(leader, { delta: 5 }, 'first')).resolves.toBe(5);
      await cluster.heartbeat(leader);
      await cluster.drain();
      expect(cluster.members.map((member) => cluster.machine(member).value)).toEqual([5, 5, 5]);
      cluster.assertInvariants();
    } finally {
      await cluster.stop();
    }
  });

  it('prevents an isolated minority member from becoming leader', async () => {
    const cluster = await createCluster(202);
    try {
      const [leader, follower, isolated] = cluster.members;
      await cluster.campaign(leader);
      await cluster.drain();
      cluster.partition([isolated], [leader, follower]);
      await cluster.campaign(isolated);
      await cluster.drain();
      expect(cluster.status(isolated).role).toBe('candidate');
      expect(cluster.status(isolated).currentTerm).toBeGreaterThan(
        cluster.status(leader).currentTerm,
      );
      await expect(cluster.propose(leader, { delta: 2 }, 'majority-write')).resolves.toBe(2);
      expect(cluster.machine(leader).value).toBe(2);
      expect(cluster.machine(follower).value).toBe(2);
      expect(cluster.machine(isolated).value).toBe(0);
      cluster.assertInvariants();
    } finally {
      await cluster.stop();
    }
  });

  it('elects a replacement leader after the original leader crashes', async () => {
    const cluster = await createCluster(303);
    try {
      const [first, replacement] = cluster.members;
      await cluster.campaign(first);
      await cluster.drain();
      await cluster.propose(first, { delta: 1 }, 'before-crash');
      await cluster.crash(first);
      await cluster.campaign(replacement);
      await cluster.drain();
      expect(cluster.status(replacement).role).toBe('leader');
      await expect(cluster.propose(replacement, { delta: 2 }, 'after-crash')).resolves.toBe(3);
      cluster.assertInvariants();
    } finally {
      await cluster.stop();
    }
  });

  it('reconstructs application state and catches up after restart', async () => {
    const cluster = await createCluster(404);
    try {
      const [leader, , restarted] = cluster.members;
      await cluster.campaign(leader);
      await cluster.drain();
      await cluster.propose(leader, { delta: 4 }, 'before-restart');
      await cluster.heartbeat(leader);
      await cluster.drain();
      await cluster.crash(restarted);
      await cluster.propose(leader, { delta: 6 }, 'while-down');
      await cluster.restart(restarted);
      expect(cluster.machine(restarted).value).toBe(4);
      await cluster.heartbeat(leader);
      await cluster.drain();
      expect(cluster.machine(restarted).value).toBe(10);
      cluster.assertInvariants();
    } finally {
      await cluster.stop();
    }
  });

  it('recovers from a split vote in a later term', async () => {
    const cluster = await createCluster(505);
    try {
      const [first, second, third] = cluster.members;
      cluster.partition([third], [first, second]);
      await cluster.campaign(first);
      await cluster.campaign(second);
      await cluster.drain();
      expect(cluster.status(first).role).toBe('candidate');
      expect(cluster.status(second).role).toBe('candidate');
      cluster.heal();
      await cluster.campaign(first);
      await cluster.drain();
      expect(cluster.status(first).role).toBe('leader');
      expect(cluster.status(first).currentTerm).toBe(2n);
      cluster.assertInvariants();
    } finally {
      await cluster.stop();
    }
  });

  it('does not commit a proposal on an isolated leader', async () => {
    const cluster = await createCluster(606);
    try {
      const [leader, second, third] = cluster.members;
      await cluster.campaign(leader);
      await cluster.drain();
      const committedBefore = cluster.status(leader).commitIndex;
      cluster.partition([leader], [second, third]);
      const outcome = cluster
        .node(leader)
        .propose({ delta: 9 }, { commandId: 'isolated-write', timeoutMs: 60_000 })
        .then(
          (result) => result,
          (error: unknown) => error,
        );
      await cluster.settleLocalWork();
      expect(cluster.status(leader).lastLogIndex).toBe(committedBefore + 1n);
      expect(cluster.status(leader).commitIndex).toBe(committedBefore);
      expect(cluster.machine(leader).value).toBe(0);
      await cluster.crash(leader);
      await expect(outcome).resolves.toBeInstanceOf(NodeStoppedError);
      cluster.assertInvariants();
    } finally {
      await cluster.stop();
    }
  });

  it('steps down an isolated former leader after it observes the higher term', async () => {
    const cluster = await createCluster(707);
    try {
      const [oldLeader, newLeader, third] = cluster.members;
      await cluster.campaign(oldLeader);
      await cluster.drain();
      cluster.partition([oldLeader], [newLeader, third]);
      await cluster.campaign(newLeader);
      await cluster.drain();
      expect(cluster.status(newLeader).role).toBe('leader');
      expect(cluster.status(oldLeader).role).toBe('leader');
      cluster.heal();
      await cluster.heartbeat(newLeader);
      await cluster.drain();
      expect(cluster.status(oldLeader).role).toBe('follower');
      expect(cluster.status(oldLeader).currentTerm).toBe(cluster.status(newLeader).currentTerm);
      cluster.assertInvariants();
    } finally {
      await cluster.stop();
    }
  });

  it('repairs a conflicting uncommitted suffix after leader replacement', async () => {
    const cluster = await createCluster(808);
    try {
      const [oldLeader, newLeader, third] = cluster.members;
      await cluster.campaign(oldLeader);
      await cluster.drain();
      cluster.partition([oldLeader], [newLeader, third]);
      const abandoned = cluster
        .node(oldLeader)
        .propose({ delta: 100 }, { commandId: 'abandoned', timeoutMs: 60_000 })
        .then(
          (result) => result,
          (error: unknown) => error,
        );
      await cluster.settleLocalWork();
      expect(cluster.durableState(oldLeader).inspect().entries.at(-1)).toMatchObject({
        index: 2n,
        term: 1n,
        commandId: 'abandoned',
      });
      await cluster.crash(oldLeader);
      await expect(abandoned).resolves.toBeInstanceOf(NodeStoppedError);
      await cluster.campaign(newLeader);
      await cluster.drain();
      expect(cluster.status(newLeader).role).toBe('leader');
      cluster.heal();
      await cluster.restart(oldLeader);
      await cluster.heartbeat(newLeader);
      await cluster.drain();
      expect(cluster.durableState(oldLeader).inspect().entries.at(-1)).toMatchObject({
        index: 2n,
        term: 2n,
        type: 'noop',
      });
      expect(cluster.machine(oldLeader).value).toBe(0);
      cluster.assertInvariants();
    } finally {
      await cluster.stop();
    }
  });

  it('repeats election, replication, failover, and restart across deterministic seeds', async () => {
    for (let seed = 1; seed <= 10; seed += 1) {
      const cluster = await createCluster(seed);
      try {
        const [first, replacement] = cluster.members;
        await cluster.campaign(first);
        await cluster.drain();
        await cluster.propose(first, { delta: seed }, `seed-${seed.toString()}-first`);
        await cluster.crash(first);
        await cluster.campaign(replacement);
        await cluster.drain();
        await cluster.propose(replacement, { delta: 1 }, `seed-${seed.toString()}-second`);
        await cluster.restart(first);
        await cluster.heartbeat(replacement);
        await cluster.drain();
        expect(cluster.machine(first).value).toBe(seed + 1);
        cluster.assertInvariants();
      } finally {
        await cluster.stop();
      }
    }
  });

  it('remains safe when replication messages are duplicated and reordered', async () => {
    const cluster = await createCluster(909);
    try {
      const [leader] = cluster.members;
      await cluster.campaign(leader);
      await cluster.drain();
      const result = cluster.node(leader).propose({ delta: 3 }, { commandId: 'duplicated' });
      await cluster.network.waitForPending();
      cluster.duplicatePending();
      cluster.reorderPending();
      await cluster.drain();
      await expect(result).resolves.toBe(3);
      expect(cluster.members.map((member) => cluster.machine(member).value)).toEqual([3, 3, 3]);
      cluster.assertInvariants();
    } finally {
      await cluster.stop();
    }
  });

  it('does not send or apply a proposal whose local persistence fails', async () => {
    const cluster = await createCluster(1_010);
    try {
      const [leader, firstFollower, secondFollower] = cluster.members;
      await cluster.campaign(leader);
      await cluster.drain();
      const commitBefore = cluster.status(leader).commitIndex;
      cluster.durableState(leader).failNext();
      await expect(
        cluster.node(leader).propose({ delta: 7 }, { commandId: 'storage-failure' }),
      ).rejects.toBeInstanceOf(StorageError);
      await cluster.settleLocalWork();
      expect(cluster.network.pending).toBe(0);
      expect(cluster.status(leader).commitIndex).toBe(commitBefore);
      expect(cluster.machine(leader).value).toBe(0);
      expect(cluster.machine(firstFollower).value).toBe(0);
      expect(cluster.machine(secondFollower).value).toBe(0);
    } finally {
      await cluster.stop();
    }
  });
});

async function createCluster(
  seed: number,
): Promise<TestCluster<CounterCommand, number, CounterMachine>> {
  return TestCluster.create({
    seed,
    codec: new JsonCommandCodec(validateCounterCommand),
    createStateMachine: () => new CounterMachine(),
    stateHash: (machine) => machine.hash(),
  });
}

function validateCounterCommand(value: unknown): CounterCommand {
  if (
    typeof value !== 'object' ||
    value === null ||
    !('delta' in value) ||
    typeof value.delta !== 'number'
  )
    throw new Error('invalid counter command');
  return { delta: value.delta };
}
