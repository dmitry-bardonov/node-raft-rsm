import type {
  HardState,
  LogEntry,
  PersistBatch,
  PersistedRaftState,
  RaftStorage,
  Snapshot,
} from '@node-raft-rsm/core';

export class DurableMemoryStorageState {
  #hardState: HardState = { currentTerm: 0n, votedFor: null, commitIndex: 0n };
  #entries: LogEntry[] = [];
  #snapshot: Snapshot | undefined;
  #appliedIndex = 0n;
  #failure: Error | undefined;

  public open(): RaftStorage {
    return new RestartableMemoryRaftStorage(this);
  }

  public failNext(error = new Error('injected durable-memory failure')): void {
    this.#failure = error;
  }

  public inspect(): PersistedRaftState {
    return {
      hardState: { ...this.#hardState },
      entries: this.#entries.map(cloneEntry),
      ...(this.#snapshot === undefined ? {} : { snapshot: cloneSnapshot(this.#snapshot) }),
      appliedIndex: this.#appliedIndex,
    };
  }

  public persist(batch: PersistBatch): void {
    if (this.#failure !== undefined) {
      const error = this.#failure;
      this.#failure = undefined;
      throw error;
    }

    const nextEntries = this.#entries.map(cloneEntry);
    if (batch.truncateFrom !== undefined) {
      if (batch.truncateFrom <= this.#hardState.commitIndex)
        throw new Error('invariant: cannot truncate a committed entry');
      while ((nextEntries.at(-1)?.index ?? -1n) >= batch.truncateFrom) nextEntries.pop();
    }
    for (const entry of batch.entries) {
      const expected = (nextEntries.at(-1)?.index ?? this.#snapshot?.lastIncludedIndex ?? 0n) + 1n;
      if (entry.index !== expected) throw new Error('invariant: persisted log is not contiguous');
      nextEntries.push(cloneEntry(entry));
    }

    const nextHardState = batch.hardState ?? this.#hardState;
    if (nextHardState.currentTerm < this.#hardState.currentTerm)
      throw new Error('invariant: term decreased');
    if (
      nextHardState.currentTerm === this.#hardState.currentTerm &&
      this.#hardState.votedFor !== null &&
      nextHardState.votedFor !== this.#hardState.votedFor
    )
      throw new Error('invariant: node voted twice in one term');
    if (nextHardState.commitIndex < this.#hardState.commitIndex)
      throw new Error('invariant: commit index decreased');
    const activeSnapshot = batch.snapshot ?? this.#snapshot;
    const lastIndex = nextEntries.at(-1)?.index ?? activeSnapshot?.lastIncludedIndex ?? 0n;
    if (nextHardState.commitIndex > lastIndex)
      throw new Error('invariant: commit index exceeds durable log');

    const nextAppliedIndex = batch.appliedIndex ?? this.#appliedIndex;
    if (nextAppliedIndex < this.#appliedIndex)
      throw new Error('invariant: applied index decreased');
    if (nextAppliedIndex > nextHardState.commitIndex)
      throw new Error('invariant: applied index exceeds commit index');

    this.#entries = nextEntries;
    this.#hardState = { ...nextHardState };
    this.#appliedIndex = nextAppliedIndex;
    if (batch.snapshot !== undefined) this.#snapshot = cloneSnapshot(batch.snapshot);
  }

  public installSnapshot(snapshot: Snapshot): void {
    if (snapshot.lastIncludedIndex < (this.#snapshot?.lastIncludedIndex ?? 0n))
      throw new Error('invariant: snapshot moved backward');
    this.#snapshot = cloneSnapshot(snapshot);
    this.#entries = this.#entries.filter((entry) => entry.index > snapshot.lastIncludedIndex);
    this.#hardState = {
      ...this.#hardState,
      commitIndex: maxBigInt(this.#hardState.commitIndex, snapshot.lastIncludedIndex),
    };
    this.#appliedIndex = maxBigInt(this.#appliedIndex, snapshot.lastIncludedIndex);
  }

  public compact(throughIndex: bigint): void {
    if (this.#snapshot === undefined || throughIndex > this.#snapshot.lastIncludedIndex)
      throw new Error('cannot compact beyond a durable snapshot');
    this.#entries = this.#entries.filter((entry) => entry.index > throughIndex);
  }
}

class RestartableMemoryRaftStorage implements RaftStorage {
  #closed = false;

  public constructor(private readonly state: DurableMemoryStorageState) {}

  public async load(): Promise<PersistedRaftState> {
    this.#assertOpen();
    await Promise.resolve();
    return this.state.inspect();
  }

  public async persist(batch: PersistBatch): Promise<void> {
    this.#assertOpen();
    await Promise.resolve();
    this.state.persist(batch);
  }

  public async getTerm(index: bigint): Promise<bigint | undefined> {
    this.#assertOpen();
    await Promise.resolve();
    const persisted = this.state.inspect();
    if (persisted.snapshot?.lastIncludedIndex === index) return persisted.snapshot.lastIncludedTerm;
    return persisted.entries.find((entry) => entry.index === index)?.term;
  }

  public async getEntry(index: bigint): Promise<LogEntry | undefined> {
    this.#assertOpen();
    await Promise.resolve();
    const entry = this.state.inspect().entries.find((candidate) => candidate.index === index);
    return entry === undefined ? undefined : cloneEntry(entry);
  }

  public async getEntries(
    fromInclusive: bigint,
    toExclusive: bigint,
    maxBytes?: number,
  ): Promise<readonly LogEntry[]> {
    this.#assertOpen();
    await Promise.resolve();
    const result: LogEntry[] = [];
    let bytes = 0;
    for (const entry of this.state.inspect().entries) {
      if (entry.index < fromInclusive || entry.index >= toExclusive) continue;
      if (
        maxBytes !== undefined &&
        result.length > 0 &&
        bytes + entry.payload.byteLength > maxBytes
      )
        break;
      result.push(cloneEntry(entry));
      bytes += entry.payload.byteLength;
    }
    return result;
  }

  public async installSnapshot(snapshot: Snapshot): Promise<void> {
    this.#assertOpen();
    await Promise.resolve();
    this.state.installSnapshot(snapshot);
  }

  public async compact(throughIndex: bigint): Promise<void> {
    this.#assertOpen();
    await Promise.resolve();
    this.state.compact(throughIndex);
  }

  public async close(): Promise<void> {
    await Promise.resolve();
    this.#closed = true;
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error('storage handle is closed');
  }
}

function cloneEntry(entry: LogEntry): LogEntry {
  return { ...entry, payload: new Uint8Array(entry.payload) };
}

function cloneSnapshot(snapshot: Snapshot): Snapshot {
  return { ...snapshot, members: [...snapshot.members], data: new Uint8Array(snapshot.data) };
}

function maxBigInt(left: bigint, right: bigint): bigint {
  return left > right ? left : right;
}
