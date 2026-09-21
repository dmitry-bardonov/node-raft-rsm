import { createHash } from 'node:crypto';
import {
  clusterId,
  nodeId,
  type CommandCodec,
  type NodeId,
  type RaftStatus,
  type ReplicatedStateMachine,
} from '@node-raft-rsm/core';
import { RaftNode } from '@node-raft-rsm/node';
import { MemoryNetwork } from '@node-raft-rsm/transport-memory';
import { DurableMemoryStorageState } from './restartable-storage.js';
import { SeededRandom } from './primitives.js';

export interface TestClusterOptions<
  Command,
  Result,
  Machine extends ReplicatedStateMachine<Command, Result>,
> {
  readonly codec: CommandCodec<Command>;
  readonly createStateMachine: (nodeId: NodeId) => Machine;
  readonly stateHash?: (machine: Machine) => string;
  readonly seed?: number;
  readonly nodeIds?: readonly [string, string, string];
}

interface LiveMember<Command, Result, Machine extends ReplicatedStateMachine<Command, Result>> {
  readonly node: RaftNode<Command, Result>;
  readonly machine: Machine;
}

export class TestCluster<Command, Result, Machine extends ReplicatedStateMachine<Command, Result>> {
  readonly network = new MemoryNetwork();
  readonly members: readonly [NodeId, NodeId, NodeId];
  readonly #clusterId = clusterId('deterministic-test-cluster');
  readonly #random: SeededRandom;
  readonly #storage = new Map<NodeId, DurableMemoryStorageState>();
  readonly #live = new Map<NodeId, LiveMember<Command, Result, Machine>>();
  readonly #lastTerms = new Map<NodeId, bigint>();
  readonly #lastCommits = new Map<NodeId, bigint>();
  readonly #lastApplied = new Map<NodeId, bigint>();

  private constructor(private readonly options: TestClusterOptions<Command, Result, Machine>) {
    const ids = options.nodeIds ?? ['node-0', 'node-1', 'node-2'];
    this.members = [nodeId(ids[0]), nodeId(ids[1]), nodeId(ids[2])];
    this.#random = new SeededRandom(options.seed ?? 1);
    for (const member of this.members) this.#storage.set(member, new DurableMemoryStorageState());
  }

  public static async create<
    Command,
    Result,
    Machine extends ReplicatedStateMachine<Command, Result>,
  >(
    options: TestClusterOptions<Command, Result, Machine>,
  ): Promise<TestCluster<Command, Result, Machine>> {
    const cluster = new TestCluster(options);
    for (const member of cluster.members) await cluster.#startMember(member);
    cluster.assertInvariants();
    return cluster;
  }

  public get seed(): number {
    return this.#random.seed;
  }

  public node(member: NodeId): RaftNode<Command, Result> {
    return this.#getLive(member).node;
  }

  public machine(member: NodeId): Machine {
    return this.#getLive(member).machine;
  }

  public status(member: NodeId): RaftStatus {
    return this.node(member).status;
  }

  public durableState(member: NodeId): DurableMemoryStorageState {
    const storage = this.#storage.get(member);
    if (storage === undefined) throw new Error(`unknown member ${member}`);
    return storage;
  }

  public async campaign(member: NodeId): Promise<void> {
    await this.node(member).campaign(this.#random.integer(10, 21));
    this.assertInvariants();
  }

  public async heartbeat(member: NodeId): Promise<void> {
    await this.node(member).heartbeat();
    this.assertInvariants();
  }

  public async drain(): Promise<void> {
    let delivered = 0;
    for (;;) {
      while (this.network.pending > 0) {
        if (++delivered > 10_000)
          throw new Error(`network drain limit exceeded; seed=${this.seed.toString()}`);
        const index = this.#random.integer(0, this.network.pending);
        await this.network.deliver(index);
        this.assertInvariants();
      }
      await new Promise<void>((resolve) => setImmediate(resolve));
      if (this.network.pending === 0) return;
    }
  }

  public async settleLocalWork(): Promise<void> {
    await new Promise<void>((resolve) => setImmediate(resolve));
    this.assertInvariants();
  }

  public async propose(member: NodeId, command: Command, commandId: string): Promise<Result> {
    const result = this.node(member).propose(command, { commandId, timeoutMs: 5_000 });
    await this.network.waitForPending();
    await this.drain();
    const applied = await result;
    this.assertInvariants();
    return applied;
  }

  public partition(left: readonly NodeId[], right: readonly NodeId[]): void {
    this.network.partition(left, right);
  }

  public heal(): void {
    this.network.heal();
  }

  public duplicatePending(index = 0): void {
    this.network.duplicate(index);
  }

  public reorderPending(): void {
    this.network.reorder();
  }

  public async crash(member: NodeId): Promise<void> {
    const live = this.#getLive(member);
    await live.node.stop();
    this.#live.delete(member);
    this.assertInvariants();
  }

  public async restart(member: NodeId): Promise<void> {
    if (this.#live.has(member)) throw new Error(`member ${member} is already running`);
    await this.#startMember(member);
    this.assertInvariants();
  }

  public async stop(): Promise<void> {
    await Promise.all([...this.#live.values()].map(async ({ node }) => node.stop()));
    this.#live.clear();
  }

  public assertInvariants(): void {
    const leadersByTerm = new Map<bigint, NodeId>();
    for (const [member, live] of this.#live) {
      const status = live.node.status;
      const previousTerm = this.#lastTerms.get(member) ?? 0n;
      const previousCommit = this.#lastCommits.get(member) ?? 0n;
      const previousApplied = this.#lastApplied.get(member) ?? 0n;
      if (status.currentTerm < previousTerm) this.#violation('current term decreased', member);
      if (status.commitIndex < previousCommit) this.#violation('commit index decreased', member);
      if (status.appliedIndex < previousApplied) this.#violation('applied index decreased', member);
      if (status.appliedIndex > status.commitIndex)
        this.#violation('applied index exceeds commit index', member);
      if (status.commitIndex > status.lastLogIndex)
        this.#violation('commit index exceeds last log index', member);
      this.#lastTerms.set(member, status.currentTerm);
      this.#lastCommits.set(member, status.commitIndex);
      this.#lastApplied.set(member, status.appliedIndex);
      if (status.role === 'leader') {
        const existing = leadersByTerm.get(status.currentTerm);
        if (existing !== undefined && existing !== member)
          this.#violation(`two leaders in term ${status.currentTerm.toString()}`, member);
        leadersByTerm.set(status.currentTerm, member);
      }
    }

    const committed = new Map<bigint, string>();
    for (const member of this.members) {
      const state = this.durableState(member).inspect();
      const boundary = state.snapshot?.lastIncludedIndex ?? 0n;
      let expected = boundary + 1n;
      for (const entry of state.entries) {
        if (entry.index !== expected) this.#violation('durable log is not contiguous', member);
        expected += 1n;
        if (entry.index <= state.hardState.commitIndex) {
          const fingerprint = entryFingerprint(entry);
          const known = committed.get(entry.index);
          if (known !== undefined && known !== fingerprint)
            this.#violation(`committed entry ${entry.index.toString()} differs`, member);
          committed.set(entry.index, fingerprint);
        }
      }
    }

    const stateHash = this.options.stateHash;
    if (stateHash !== undefined) {
      const caughtUp = [...this.#live.values()].filter(
        ({ node }) => node.status.appliedIndex === node.status.commitIndex,
      );
      const highestApplied = caughtUp.reduce(
        (highest, { node }) =>
          node.status.appliedIndex > highest ? node.status.appliedIndex : highest,
        0n,
      );
      const hashes = caughtUp
        .filter(({ node }) => node.status.appliedIndex === highestApplied)
        .map(({ machine }) => stateHash(machine));
      if (hashes.some((hash) => hash !== hashes[0]))
        this.#violation('caught-up state-machine hashes differ');
    }
  }

  async #startMember(member: NodeId): Promise<void> {
    const storage = this.durableState(member).open();
    const machine = this.options.createStateMachine(member);
    const node = await RaftNode.create({
      nodeId: member,
      clusterId: this.#clusterId,
      members: this.members,
      heartbeatInterval: 2,
      electionTimeoutMinMs: 10,
      electionTimeoutMaxMs: 20,
      automaticTimers: false,
      randomInteger: (minimum, maximumExclusive) => this.#random.integer(minimum, maximumExclusive),
      storage,
      transport: this.network.endpoint(member),
      stateMachine: machine,
      codec: this.options.codec,
    });
    await node.start();
    this.#live.set(member, { node, machine });
  }

  #getLive(member: NodeId): LiveMember<Command, Result, Machine> {
    const live = this.#live.get(member);
    if (live === undefined) throw new Error(`member ${member} is not running`);
    return live;
  }

  #violation(message: string, member?: NodeId): never {
    const location = member === undefined ? '' : ` on ${member}`;
    throw new Error(
      `Raft invariant violation${location}: ${message}; seed=${this.seed.toString()}`,
    );
  }
}

function entryFingerprint(entry: {
  readonly term: bigint;
  readonly type: string;
  readonly commandId?: string;
  readonly payload: Uint8Array;
}): string {
  return createHash('sha256')
    .update(entry.term.toString())
    .update('\0')
    .update(entry.type)
    .update('\0')
    .update(entry.commandId ?? '')
    .update('\0')
    .update(entry.payload)
    .digest('hex');
}
