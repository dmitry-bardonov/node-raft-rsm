import {
  clusterId,
  nodeId,
  type NodeId,
  type RaftMessage,
  type RaftStatus,
} from '@node-raft-rsm/core';
import { JsonCommandCodec, RaftNode } from '@node-raft-rsm/node';
import { DurableMemoryStorageState, SeededRandom } from '@node-raft-rsm/testing';
import { MemoryNetwork } from '@node-raft-rsm/transport-memory';
import type {
  ClusterSnapshot,
  MessageSnapshot,
  SimulationAction,
  TimelineEvent,
} from '../shared/protocol.js';
import {
  KvStateMachine,
  validateKvCommand,
  type KvCommand,
  type KvResult,
} from './kv-state-machine.js';

interface Replica {
  readonly node: RaftNode<KvCommand, KvResult>;
  readonly machine: KvStateMachine;
  online: boolean;
}

type SnapshotListener = (snapshot: ClusterSnapshot) => void;

export class SimulationController {
  readonly network = new MemoryNetwork();
  readonly #members: readonly NodeId[];
  readonly #storage = new Map<NodeId, DurableMemoryStorageState>();
  readonly #replicas = new Map<NodeId, Replica>();
  readonly #listeners = new Set<SnapshotListener>();
  readonly #events: TimelineEvent[] = [];
  readonly #random: SeededRandom;
  readonly #codec = new JsonCommandCodec<KvCommand>(validateKvCommand);
  #revision = 0;
  #eventId = 0;
  #commandId = 0;

  private constructor(
    public readonly seed: number,
    nodeCount: number,
  ) {
    validateNodeCount(nodeCount);
    this.#random = new SeededRandom(seed);
    this.#members = Array.from({ length: nodeCount }, (_, index) =>
      nodeId(`node-${(index + 1).toString()}`),
    );
    for (const member of this.#members) this.#storage.set(member, new DurableMemoryStorageState());
  }

  public static async create(nodeCount = 3, seed = 1): Promise<SimulationController> {
    const simulation = new SimulationController(seed, nodeCount);
    for (const member of simulation.#members) await simulation.#start(member);
    simulation.#record(
      'system',
      `Started ${nodeCount.toString()}-node cluster with seed ${seed.toString()}`,
    );
    simulation.#publish();
    return simulation;
  }

  public subscribe(listener: SnapshotListener): () => void {
    this.#listeners.add(listener);
    listener(this.snapshot());
    return () => this.#listeners.delete(listener);
  }

  public snapshot(): ClusterSnapshot {
    return {
      revision: this.#revision,
      seed: this.seed,
      nodes: this.#members.map((member) => this.#nodeSnapshot(member)),
      messages: this.network
        .pendingMessages()
        .map(({ id, message }) => messageSnapshot(id, message)),
      blockedEdges: this.network.blockedEdges(),
      events: [...this.#events],
    };
  }

  public async execute(action: SimulationAction): Promise<void> {
    switch (action.type) {
      case 'reset':
        throw new Error('reset is handled by the simulation server');
      case 'campaign': {
        const member = this.#member(action.nodeId);
        await this.#online(member).node.campaign(this.#random.integer(10, 21));
        this.#record(
          'election',
          `Election started by ${member} · term ${this.#online(member).node.status.currentTerm.toString()}`,
        );
        break;
      }
      case 'heartbeat': {
        const member = this.#member(action.nodeId);
        await this.#online(member).node.heartbeat();
        break;
      }
      case 'propose':
        this.#propose(action);
        break;
      case 'partition': {
        const left = action.left.map((member) => this.#member(member));
        const selected = new Set(left);
        const right = this.#members.filter((member) => !selected.has(member));
        if (left.length === 0 || right.length === 0)
          throw new Error('a partition must split the cluster into two non-empty groups');
        this.network.heal();
        this.network.partition(left, right);
        this.#record('network', `Partitioned [${left.join(', ')}] from [${right.join(', ')}]`);
        break;
      }
      case 'heal':
        this.network.heal();
        this.#record('network', 'Healed all network partitions');
        break;
      case 'disable':
        await this.#disable(this.#member(action.nodeId));
        break;
      case 'restart':
        await this.#restart(this.#member(action.nodeId));
        break;
      case 'deliver': {
        const message = this.network.pendingMessages().find(({ id }) => id === action.messageId);
        const delivered = await this.network.deliverById(action.messageId);
        if (message === undefined) throw new Error('message is no longer pending');
        this.#record(
          'network',
          `${delivered ? 'Delivered' : 'Discarded'} ${message.message.type} ${message.message.from} → ${message.message.to}`,
        );
        break;
      }
      case 'drop':
        if (!this.network.dropById(action.messageId))
          throw new Error('message is no longer pending');
        this.#record('network', `Dropped message #${action.messageId.toString()}`);
        break;
      case 'duplicate':
        if (!this.network.duplicateById(action.messageId))
          throw new Error('message is no longer pending');
        this.#record('network', `Duplicated message #${action.messageId.toString()}`);
        break;
      case 'reorder':
        this.network.reorder();
        this.#record('network', 'Reversed the pending message queue');
        break;
      case 'step':
        await this.#step();
        break;
      case 'drain':
        await this.#drain();
        break;
    }
    await settleLocalWork();
    this.#publish();
  }

  public async stop(): Promise<void> {
    await Promise.all(
      [...this.#replicas.values()]
        .filter(({ online }) => online)
        .map(async ({ node }) => node.stop()),
    );
  }

  async #start(member: NodeId): Promise<void> {
    const durable = this.#storage.get(member);
    if (durable === undefined) throw new Error(`missing storage for ${member}`);
    const machine = new KvStateMachine();
    const node = await RaftNode.create({
      nodeId: member,
      clusterId: clusterId('raft-visualizer'),
      members: this.#members,
      heartbeatInterval: 500,
      electionTimeoutMinMs: 1_500,
      electionTimeoutMaxMs: 3_000,
      automaticTimers: false,
      randomInteger: (minimum, maximumExclusive) => this.#random.integer(minimum, maximumExclusive),
      storage: durable.open(),
      transport: this.network.endpoint(member),
      stateMachine: machine,
      codec: this.#codec,
      onEvent: (event) => {
        if (event.type === 'role-changed') {
          if (event.status.role !== 'candidate')
            this.#record(
              'election',
              event.status.role === 'leader'
                ? `${member} elected leader · term ${event.status.currentTerm.toString()}`
                : `${member} became follower · term ${event.status.currentTerm.toString()}`,
            );
        } else this.#record('error', `${member}: ${event.error.message}`);
      },
    });
    await node.start();
    this.#replicas.set(member, { node, machine, online: true });
  }

  #propose(action: Extract<SimulationAction, { readonly type: 'propose' }>): void {
    const member = this.#member(action.nodeId);
    const replica = this.#online(member);
    if (replica.node.status.role !== 'leader')
      throw new Error(`${member} is not the leader; campaign a node or select the current leader`);
    if (action.key.length === 0 || action.key.length > 128)
      throw new Error('key must be 1–128 characters');
    const command: KvCommand =
      action.command === 'put'
        ? { type: 'put', key: action.key, value: action.value ?? '' }
        : { type: 'delete', key: action.key };
    const commandId = `visualizer-${(++this.#commandId).toString()}`;
    const description =
      action.command === 'put'
        ? `PUT ${action.key} = ${action.value ?? ''}`
        : `DELETE ${action.key}`;
    this.#record('command', `${description} proposed to ${member}`);
    void replica.node
      .propose(command, { commandId, timeoutMs: 60_000 })
      .then(() => {
        this.#record(
          'command',
          `${description} committed and applied · index ${replica.node.status.appliedIndex.toString()}`,
        );
        this.#publish();
      })
      .catch((error: unknown) => {
        this.#record('error', `${commandId} failed: ${asError(error).message}`);
        this.#publish();
      });
  }

  async #disable(member: NodeId): Promise<void> {
    const replica = this.#online(member);
    await replica.node.stop();
    replica.online = false;
    this.#record('node', `Disabled ${member}; durable state was retained`);
  }

  async #restart(member: NodeId): Promise<void> {
    const replica = this.#replicas.get(member);
    if (replica === undefined) throw new Error(`unknown member ${member}`);
    if (replica.online) throw new Error(`${member} is already online`);
    await this.#start(member);
    this.#record('node', `Restarted ${member} from durable state`);
  }

  async #step(): Promise<void> {
    const [pending] = this.network.pendingMessages();
    if (pending === undefined) return;
    await this.network.deliverById(pending.id);
  }

  async #drain(): Promise<void> {
    let delivered = 0;
    while (this.network.pending > 0) {
      if (++delivered > 10_000) throw new Error('network drain limit exceeded');
      await this.network.deliver();
      await settleLocalWork();
    }
  }

  #nodeSnapshot(member: NodeId): ClusterSnapshot['nodes'][number] {
    const replica = this.#replicas.get(member);
    if (replica === undefined) throw new Error(`unknown member ${member}`);
    if (replica.online) return statusSnapshot(replica.node.status, replica.machine.inspect());
    const durable = this.#storage.get(member)?.inspect();
    if (durable === undefined) throw new Error(`missing storage for ${member}`);
    const lastEntry = durable.entries.at(-1);
    return {
      id: member,
      online: false,
      role: 'offline',
      term: durable.hardState.currentTerm.toString(),
      leaderId: null,
      lastLogIndex: (lastEntry?.index ?? durable.snapshot?.lastIncludedIndex ?? 0n).toString(),
      commitIndex: durable.hardState.commitIndex.toString(),
      appliedIndex: durable.appliedIndex.toString(),
      values: replica.machine.inspect(),
    };
  }

  #member(value: string): NodeId {
    const member = nodeId(value);
    if (!this.#members.includes(member)) throw new Error(`unknown member ${value}`);
    return member;
  }

  #online(member: NodeId): Replica {
    const replica = this.#replicas.get(member);
    if (!replica?.online) throw new Error(`${member} is offline`);
    return replica;
  }

  #record(kind: TimelineEvent['kind'], message: string): void {
    this.#events.push({ id: ++this.#eventId, at: new Date().toISOString(), kind, message });
    if (this.#events.length > 200) this.#events.splice(0, this.#events.length - 200);
  }

  #publish(): void {
    this.#revision += 1;
    const snapshot = this.snapshot();
    for (const listener of this.#listeners) listener(snapshot);
  }
}

function statusSnapshot(status: RaftStatus, values: Readonly<Record<string, string>>) {
  return {
    id: status.nodeId,
    online: true,
    role: status.role,
    term: status.currentTerm.toString(),
    leaderId: status.leaderId,
    lastLogIndex: status.lastLogIndex.toString(),
    commitIndex: status.commitIndex.toString(),
    appliedIndex: status.appliedIndex.toString(),
    values,
  } as const;
}

function messageSnapshot(id: number, message: RaftMessage): MessageSnapshot {
  let detail: string;
  switch (message.type) {
    case 'request-vote-request':
      detail = `last log ${message.lastLogIndex.toString()}/${message.lastLogTerm.toString()}`;
      break;
    case 'request-vote-response':
      detail = message.voteGranted ? 'vote granted' : 'vote rejected';
      break;
    case 'append-entries-request':
      detail = `${message.entries.length.toString()} entries, commit ${message.leaderCommit.toString()}`;
      break;
    case 'append-entries-response':
      detail = message.success
        ? `matched ${message.matchIndex.toString()}`
        : `rejected at ${message.rejectHint.toString()}`;
      break;
  }
  return {
    id,
    type: message.type,
    from: message.from,
    to: message.to,
    term: message.term.toString(),
    detail,
  };
}

function validateNodeCount(nodeCount: number): void {
  if (!Number.isInteger(nodeCount) || nodeCount < 1 || nodeCount > 9)
    throw new Error('node count must be an integer from 1 to 9');
}

async function settleLocalWork(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
