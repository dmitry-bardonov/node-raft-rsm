import type {
  HardState,
  LogEntry,
  PersistBatch,
  PersistedRaftState,
  RaftStorage,
  Snapshot,
} from '@node-raft-rsm/core';

export class MemoryRaftStorage implements RaftStorage {
  #hardState: HardState = { currentTerm: 0n, votedFor: null, commitIndex: 0n };
  #entries: LogEntry[] = [];
  #snapshot: Snapshot | undefined;
  #appliedIndex = 0n;
  #closed = false;
  #failure: Error | undefined;
  readonly history: PersistBatch[] = [];

  public failNext(error = new Error('injected storage failure')): void {
    this.#failure = error;
  }

  public async load(): Promise<PersistedRaftState> {
    this.#assertOpen();
    await Promise.resolve();
    return {
      hardState: { ...this.#hardState },
      entries: this.#entries.map(cloneEntry),
      ...(this.#snapshot === undefined ? {} : { snapshot: cloneSnapshot(this.#snapshot) }),
      appliedIndex: this.#appliedIndex,
    };
  }

  public async persist(batch: PersistBatch): Promise<void> {
    this.#assertOpen();
    await Promise.resolve();
    if (this.#failure !== undefined) {
      const error = this.#failure;
      this.#failure = undefined;
      throw error;
    }
    const nextEntries = this.#entries.map(cloneEntry);
    if (batch.truncateFrom !== undefined) {
      if (batch.truncateFrom <= this.#hardState.commitIndex)
        throw new Error('cannot truncate a committed entry');
      let lastEntry = nextEntries.at(-1);
      while (lastEntry !== undefined && lastEntry.index >= batch.truncateFrom) {
        nextEntries.pop();
        lastEntry = nextEntries.at(-1);
      }
    }
    for (const entry of batch.entries) {
      const expected = (nextEntries.at(-1)?.index ?? this.#snapshot?.lastIncludedIndex ?? 0n) + 1n;
      if (entry.index !== expected) throw new Error('persisted entries must be contiguous');
      nextEntries.push(cloneEntry(entry));
    }
    if (batch.hardState !== undefined) {
      if (batch.hardState.currentTerm < this.#hardState.currentTerm)
        throw new Error('term cannot decrease');
      if (batch.hardState.commitIndex < this.#hardState.commitIndex)
        throw new Error('commit index cannot decrease');
      this.#hardState = { ...batch.hardState };
    }
    if (batch.appliedIndex !== undefined) {
      if (
        batch.appliedIndex < this.#appliedIndex ||
        batch.appliedIndex > this.#hardState.commitIndex
      )
        throw new Error('invalid applied index');
      this.#appliedIndex = batch.appliedIndex;
    }
    this.#entries = nextEntries;
    if (batch.snapshot !== undefined) this.#snapshot = cloneSnapshot(batch.snapshot);
    this.history.push(cloneBatch(batch));
  }

  public async getTerm(index: bigint): Promise<bigint | undefined> {
    this.#assertOpen();
    await Promise.resolve();
    if (this.#snapshot?.lastIncludedIndex === index) return this.#snapshot.lastIncludedTerm;
    return this.#entries.find((entry) => entry.index === index)?.term;
  }

  public async getEntry(index: bigint): Promise<LogEntry | undefined> {
    this.#assertOpen();
    await Promise.resolve();
    const entry = this.#entries.find((candidate) => candidate.index === index);
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
    for (const entry of this.#entries) {
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
    await this.persist({ entries: [], snapshot });
    this.#entries = this.#entries.filter((entry) => entry.index > snapshot.lastIncludedIndex);
    this.#hardState = {
      ...this.#hardState,
      commitIndex: maxBigInt(this.#hardState.commitIndex, snapshot.lastIncludedIndex),
    };
    this.#appliedIndex = maxBigInt(this.#appliedIndex, snapshot.lastIncludedIndex);
  }

  public async compact(throughIndex: bigint): Promise<void> {
    this.#assertOpen();
    await Promise.resolve();
    if (this.#snapshot === undefined || throughIndex > this.#snapshot.lastIncludedIndex)
      throw new Error('cannot compact beyond a durable snapshot');
    this.#entries = this.#entries.filter((entry) => entry.index > throughIndex);
  }

  public async close(): Promise<void> {
    await Promise.resolve();
    this.#closed = true;
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error('storage is closed');
  }
}

function cloneEntry(entry: LogEntry): LogEntry {
  return { ...entry, payload: new Uint8Array(entry.payload) };
}
function cloneSnapshot(snapshot: Snapshot): Snapshot {
  return { ...snapshot, members: [...snapshot.members], data: new Uint8Array(snapshot.data) };
}
function cloneBatch(batch: PersistBatch): PersistBatch {
  return {
    ...(batch.hardState === undefined ? {} : { hardState: { ...batch.hardState } }),
    ...(batch.truncateFrom === undefined ? {} : { truncateFrom: batch.truncateFrom }),
    entries: batch.entries.map(cloneEntry),
    ...(batch.snapshot === undefined ? {} : { snapshot: cloneSnapshot(batch.snapshot) }),
    ...(batch.appliedIndex === undefined ? {} : { appliedIndex: batch.appliedIndex }),
  };
}
function maxBigInt(left: bigint, right: bigint): bigint {
  return left > right ? left : right;
}
