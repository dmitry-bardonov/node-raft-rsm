import { ProtocolError } from './errors.js';
import {
  PROTOCOL_VERSION,
  assertNever,
  type AppendEntriesRequest,
  type AppendEntriesResponse,
  type HardState,
  type LogEntry,
  type NodeId,
  type RaftCoreOptions,
  type RaftEvent,
  type RaftMessage,
  type RaftReady,
  type RaftStatus,
  type RequestVoteRequest,
  type RequestVoteResponse,
  type Role,
  type Snapshot,
} from './types.js';

interface PeerProgress {
  nextIndex: bigint;
  matchIndex: bigint;
}

interface Effects {
  hardState?: HardState;
  truncateFrom?: bigint;
  entries: LogEntry[];
  snapshot?: Snapshot;
  messages: RaftMessage[];
}

export class RaftCore {
  readonly #nodeId: NodeId;
  readonly #clusterId: RaftCoreOptions['clusterId'];
  readonly #members: readonly NodeId[];
  readonly #memberSet: ReadonlySet<NodeId>;
  readonly #maxMessageBytes: number;
  #role: Role = 'follower';
  #leaderId: NodeId | null = null;
  #currentTerm = 0n;
  #votedFor: NodeId | null = null;
  #commitIndex = 0n;
  #appliedIndex = 0n;
  #snapshot: Snapshot | undefined;
  #entries: LogEntry[] = [];
  #votes = new Set<NodeId>();
  #progress = new Map<NodeId, PeerProgress>();
  #readySequence = 0n;
  #outstanding: RaftReady | undefined;

  public constructor(options: RaftCoreOptions) {
    if (![1, 3, 5].includes(options.members.length))
      throw new Error('cluster size must be 1, 3, or 5');
    if (!options.members.includes(options.nodeId))
      throw new Error('members must contain local node');
    if (new Set(options.members).size !== options.members.length)
      throw new Error('members must be unique');
    if (options.electionTimeout <= options.heartbeatInterval)
      throw new Error('election timeout must exceed heartbeat interval');
    this.#nodeId = options.nodeId;
    this.#clusterId = options.clusterId;
    this.#members = [...options.members];
    this.#memberSet = new Set(options.members);
    this.#maxMessageBytes = options.maxMessageBytes ?? 4 * 1024 * 1024;
    if (options.persisted !== undefined) {
      this.#currentTerm = options.persisted.hardState.currentTerm;
      this.#votedFor = options.persisted.hardState.votedFor;
      this.#commitIndex = options.persisted.hardState.commitIndex;
      this.#appliedIndex = options.persisted.appliedIndex;
      this.#snapshot = options.persisted.snapshot;
      this.#entries = options.persisted.entries.map(cloneEntry);
    }
    this.#assertInvariants();
  }

  public get status(): RaftStatus {
    return {
      nodeId: this.#nodeId,
      clusterId: this.#clusterId,
      role: this.#role,
      currentTerm: this.#currentTerm,
      leaderId: this.#leaderId,
      lastLogIndex: this.#lastLogIndex(),
      commitIndex: this.#commitIndex,
      appliedIndex: this.#appliedIndex,
      snapshotIndex: this.#snapshot?.lastIncludedIndex ?? 0n,
    };
  }

  public step(event: RaftEvent): RaftReady | undefined {
    if (this.#outstanding !== undefined)
      throw new Error('advance the outstanding Ready before stepping again');
    const effects: Effects = { entries: [], messages: [] };
    switch (event.type) {
      case 'election-timeout':
        if (this.#role !== 'leader') this.#startElection(effects);
        break;
      case 'heartbeat-timeout':
        if (this.#role === 'leader') this.#broadcastAppend(effects);
        break;
      case 'message':
        this.#handleMessage(event.message, effects);
        break;
      case 'propose':
        if (this.#role !== 'leader') throw new Error('not leader');
        if (event.commandId.length === 0 || event.commandId.length > 256)
          throw new ProtocolError('command ID length is invalid');
        if (event.payload.byteLength > this.#maxMessageBytes)
          throw new ProtocolError('command payload exceeds configured maximum');
        this.#appendLocal(
          { type: 'command', payload: new Uint8Array(event.payload), commandId: event.commandId },
          effects,
        );
        this.#broadcastAppend(effects);
        this.#advanceLeaderCommit(effects);
        break;
      default:
        assertNever(event);
    }
    this.#assertInvariants();
    return this.#makeReady(effects);
  }

  public advance(readyId: bigint): void {
    const outstanding = this.#outstanding;
    if (outstanding?.id !== readyId) throw new Error(`unknown Ready id ${readyId.toString()}`);
    const committed = outstanding.committedEntries;
    const lastCommitted = committed.at(-1);
    if (lastCommitted !== undefined) this.#appliedIndex = lastCommitted.index;
    this.#outstanding = undefined;
    this.#assertInvariants();
  }

  #startElection(effects: Effects): void {
    this.#role = 'candidate';
    this.#leaderId = null;
    this.#currentTerm += 1n;
    this.#votedFor = this.#nodeId;
    this.#votes = new Set([this.#nodeId]);
    effects.hardState = this.#hardState();
    const lastLogIndex = this.#lastLogIndex();
    const lastLogTerm = this.#termAt(lastLogIndex) ?? 0n;
    for (const peer of this.#peers()) {
      effects.messages.push({
        type: 'request-vote-request',
        protocolVersion: PROTOCOL_VERSION,
        clusterId: this.#clusterId,
        from: this.#nodeId,
        to: peer,
        term: this.#currentTerm,
        lastLogIndex,
        lastLogTerm,
      });
    }
    if (this.#hasQuorum(this.#votes.size)) this.#becomeLeader(effects);
  }

  #becomeLeader(effects: Effects): void {
    this.#role = 'leader';
    this.#leaderId = this.#nodeId;
    this.#progress.clear();
    const nextIndex = this.#lastLogIndex() + 1n;
    for (const peer of this.#peers()) this.#progress.set(peer, { nextIndex, matchIndex: 0n });
    this.#appendLocal({ type: 'noop', payload: new Uint8Array() }, effects);
    this.#broadcastAppend(effects);
    this.#advanceLeaderCommit(effects);
  }

  #becomeFollower(term: bigint, leaderId: NodeId | null, effects: Effects): void {
    const termChanged = term > this.#currentTerm;
    if (termChanged) {
      this.#currentTerm = term;
      this.#votedFor = null;
      effects.hardState = this.#hardState();
    }
    this.#role = 'follower';
    this.#leaderId = leaderId;
    this.#votes.clear();
    this.#progress.clear();
  }

  #handleMessage(message: RaftMessage, effects: Effects): void {
    if (message.protocolVersion !== PROTOCOL_VERSION)
      throw new ProtocolError('unsupported protocol version');
    if (message.clusterId !== this.#clusterId)
      throw new ProtocolError('cross-cluster message rejected');
    if (message.to !== this.#nodeId || !this.#memberSet.has(message.from))
      throw new ProtocolError('message peer identity rejected');
    if (message.term > this.#currentTerm) this.#becomeFollower(message.term, null, effects);
    switch (message.type) {
      case 'request-vote-request':
        this.#handleVoteRequest(message, effects);
        return;
      case 'request-vote-response':
        this.#handleVoteResponse(message, effects);
        return;
      case 'append-entries-request':
        this.#handleAppendRequest(message, effects);
        return;
      case 'append-entries-response':
        this.#handleAppendResponse(message, effects);
        return;
      default:
        assertNever(message);
    }
  }

  #handleVoteRequest(message: RequestVoteRequest, effects: Effects): void {
    let voteGranted = false;
    if (message.term === this.#currentTerm) {
      const localLastIndex = this.#lastLogIndex();
      const localLastTerm = this.#termAt(localLastIndex) ?? 0n;
      const upToDate =
        message.lastLogTerm > localLastTerm ||
        (message.lastLogTerm === localLastTerm && message.lastLogIndex >= localLastIndex);
      if ((this.#votedFor === null || this.#votedFor === message.from) && upToDate) {
        this.#role = 'follower';
        this.#leaderId = null;
        this.#votedFor = message.from;
        effects.hardState = this.#hardState();
        voteGranted = true;
      }
    }
    const response: RequestVoteResponse = {
      type: 'request-vote-response',
      protocolVersion: PROTOCOL_VERSION,
      clusterId: this.#clusterId,
      from: this.#nodeId,
      to: message.from,
      term: this.#currentTerm,
      voteGranted,
    };
    effects.messages.push(response);
  }

  #handleVoteResponse(message: RequestVoteResponse, effects: Effects): void {
    if (message.term < this.#currentTerm || this.#role !== 'candidate') return;
    if (message.term === this.#currentTerm && message.voteGranted) {
      this.#votes.add(message.from);
      if (this.#hasQuorum(this.#votes.size)) this.#becomeLeader(effects);
    }
  }

  #handleAppendRequest(message: AppendEntriesRequest, effects: Effects): void {
    if (message.term < this.#currentTerm) {
      effects.messages.push(
        this.#appendResponse(message.from, false, 0n, this.#lastLogIndex() + 1n),
      );
      return;
    }
    this.#becomeFollower(message.term, message.from, effects);
    if (this.#termAt(message.prevLogIndex) !== message.prevLogTerm) {
      const hint =
        message.prevLogIndex > this.#lastLogIndex()
          ? this.#lastLogIndex() + 1n
          : message.prevLogIndex;
      effects.messages.push(this.#appendResponse(message.from, false, 0n, hint));
      return;
    }
    let truncateFrom: bigint | undefined;
    const appended: LogEntry[] = [];
    for (const incoming of message.entries) {
      const existing = this.#entryAt(incoming.index);
      if (existing !== undefined && existing.term !== incoming.term) {
        if (incoming.index <= this.#commitIndex)
          throw new Error('invariant: attempted to overwrite committed entry');
        truncateFrom = incoming.index;
        this.#entries = this.#entries.filter((entry) => entry.index < incoming.index);
      }
      if (this.#entryAt(incoming.index) === undefined) {
        const expected = this.#lastLogIndex() + 1n;
        if (incoming.index !== expected)
          throw new ProtocolError('non-contiguous AppendEntries payload');
        const clone = cloneEntry(incoming);
        this.#entries.push(clone);
        appended.push(clone);
      }
    }
    if (truncateFrom !== undefined) effects.truncateFrom = truncateFrom;
    effects.entries.push(...appended);
    const oldCommit = this.#commitIndex;
    if (message.leaderCommit > this.#commitIndex)
      this.#commitIndex = minBigInt(message.leaderCommit, this.#lastLogIndex());
    if (this.#commitIndex !== oldCommit) effects.hardState = this.#hardState();
    const matchIndex = message.entries.at(-1)?.index ?? message.prevLogIndex;
    effects.messages.push(this.#appendResponse(message.from, true, matchIndex, matchIndex + 1n));
  }

  #handleAppendResponse(message: AppendEntriesResponse, effects: Effects): void {
    if (message.term < this.#currentTerm || this.#role !== 'leader') return;
    const progress = this.#progress.get(message.from);
    if (progress === undefined) return;
    if (message.success) {
      if (message.matchIndex > progress.matchIndex) progress.matchIndex = message.matchIndex;
      progress.nextIndex = progress.matchIndex + 1n;
      const commitAdvanced = this.#advanceLeaderCommit(effects);
      if (commitAdvanced) this.#broadcastAppend(effects);
      if (progress.nextIndex <= this.#lastLogIndex())
        effects.messages.push(this.#appendFor(message.from));
    } else {
      progress.nextIndex = maxBigInt(1n, minBigInt(progress.nextIndex - 1n, message.rejectHint));
      effects.messages.push(this.#appendFor(message.from));
    }
  }

  #appendLocal(
    entry: Pick<LogEntry, 'type' | 'payload'> & Partial<Pick<LogEntry, 'commandId'>>,
    effects: Effects,
  ): void {
    const full: LogEntry = {
      index: this.#lastLogIndex() + 1n,
      term: this.#currentTerm,
      type: entry.type,
      payload: new Uint8Array(entry.payload),
      ...(entry.commandId === undefined ? {} : { commandId: entry.commandId }),
    };
    this.#entries.push(full);
    effects.entries.push(full);
  }

  #broadcastAppend(effects: Effects): void {
    for (const peer of this.#peers()) effects.messages.push(this.#appendFor(peer));
  }

  #appendFor(peer: NodeId): AppendEntriesRequest {
    const progress = this.#progress.get(peer);
    if (progress === undefined) throw new Error('missing leader progress');
    const prevLogIndex = progress.nextIndex - 1n;
    const entries = this.#entries
      .filter((entry) => entry.index >= progress.nextIndex)
      .map(cloneEntry);
    return {
      type: 'append-entries-request',
      protocolVersion: PROTOCOL_VERSION,
      clusterId: this.#clusterId,
      from: this.#nodeId,
      to: peer,
      term: this.#currentTerm,
      prevLogIndex,
      prevLogTerm: this.#termAt(prevLogIndex) ?? 0n,
      entries,
      leaderCommit: this.#commitIndex,
    };
  }

  #appendResponse(
    to: NodeId,
    success: boolean,
    matchIndex: bigint,
    rejectHint: bigint,
  ): AppendEntriesResponse {
    return {
      type: 'append-entries-response',
      protocolVersion: PROTOCOL_VERSION,
      clusterId: this.#clusterId,
      from: this.#nodeId,
      to,
      term: this.#currentTerm,
      success,
      matchIndex,
      rejectHint,
    };
  }

  #advanceLeaderCommit(effects: Effects): boolean {
    if (this.#role !== 'leader') return false;
    for (let index = this.#lastLogIndex(); index > this.#commitIndex; index -= 1n) {
      if (this.#termAt(index) !== this.#currentTerm) continue;
      let replicated = 1;
      for (const progress of this.#progress.values())
        if (progress.matchIndex >= index) replicated += 1;
      if (this.#hasQuorum(replicated)) {
        this.#commitIndex = index;
        effects.hardState = this.#hardState();
        return true;
      }
    }
    return false;
  }

  #makeReady(effects: Effects): RaftReady | undefined {
    const committedEntries = this.#entries
      .filter((entry) => entry.index > this.#appliedIndex && entry.index <= this.#commitIndex)
      .map(cloneEntry);
    if (
      effects.hardState === undefined &&
      effects.truncateFrom === undefined &&
      effects.entries.length === 0 &&
      effects.snapshot === undefined &&
      effects.messages.length === 0 &&
      committedEntries.length === 0
    )
      return undefined;
    const ready: RaftReady = {
      id: ++this.#readySequence,
      ...(effects.hardState === undefined ? {} : { hardState: effects.hardState }),
      ...(effects.truncateFrom === undefined ? {} : { truncateFrom: effects.truncateFrom }),
      unstableEntries: effects.entries.map(cloneEntry),
      ...(effects.snapshot === undefined ? {} : { snapshot: effects.snapshot }),
      outboundMessages: effects.messages,
      committedEntries,
    };
    this.#outstanding = ready;
    return ready;
  }

  #entryAt(index: bigint): LogEntry | undefined {
    return this.#entries.find((entry) => entry.index === index);
  }

  #termAt(index: bigint): bigint | undefined {
    if (index === 0n) return 0n;
    if (this.#snapshot?.lastIncludedIndex === index) return this.#snapshot.lastIncludedTerm;
    return this.#entryAt(index)?.term;
  }

  #lastLogIndex(): bigint {
    return this.#entries.at(-1)?.index ?? this.#snapshot?.lastIncludedIndex ?? 0n;
  }

  #hardState(): HardState {
    return {
      currentTerm: this.#currentTerm,
      votedFor: this.#votedFor,
      commitIndex: this.#commitIndex,
    };
  }

  #peers(): NodeId[] {
    return this.#members.filter((member) => member !== this.#nodeId);
  }

  #hasQuorum(count: number): boolean {
    return count >= Math.floor(this.#members.length / 2) + 1;
  }

  #assertInvariants(): void {
    const boundary = this.#snapshot?.lastIncludedIndex ?? 0n;
    let expected = boundary + 1n;
    for (const entry of this.#entries) {
      if (entry.index !== expected) throw new Error('invariant: log indexes must be contiguous');
      expected += 1n;
    }
    if (this.#commitIndex < boundary || this.#commitIndex > this.#lastLogIndex())
      throw new Error('invariant: commit index outside the log');
    if (this.#appliedIndex > this.#commitIndex)
      throw new Error('invariant: applied index exceeds commit index');
    if (this.#currentTerm < 0n) throw new Error('invariant: current term is negative');
  }
}

function cloneEntry(entry: LogEntry): LogEntry {
  return { ...entry, payload: new Uint8Array(entry.payload) };
}

function minBigInt(left: bigint, right: bigint): bigint {
  return left < right ? left : right;
}

function maxBigInt(left: bigint, right: bigint): bigint {
  return left > right ? left : right;
}
