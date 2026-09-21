import type {
  HardState,
  LogEntry,
  NodeId,
  PersistedRaftState,
  RaftMessage,
  Snapshot,
} from './types.js';

export interface PersistBatch {
  readonly hardState?: HardState;
  readonly truncateFrom?: bigint;
  readonly entries: readonly LogEntry[];
  readonly snapshot?: Snapshot;
  readonly appliedIndex?: bigint;
}

export interface RaftStorage {
  load(): Promise<PersistedRaftState>;
  persist(batch: PersistBatch): Promise<void>;
  getTerm(index: bigint): Promise<bigint | undefined>;
  getEntry(index: bigint): Promise<LogEntry | undefined>;
  getEntries(
    fromInclusive: bigint,
    toExclusive: bigint,
    maxBytes?: number,
  ): Promise<readonly LogEntry[]>;
  installSnapshot(snapshot: Snapshot): Promise<void>;
  compact(throughIndex: bigint): Promise<void>;
  close(): Promise<void>;
}

export interface RaftTransport {
  start(handler: (message: RaftMessage) => Promise<void>): Promise<void>;
  send(peerId: NodeId, message: RaftMessage): Promise<void>;
  stop(): Promise<void>;
}

export interface ApplyContext {
  readonly index: bigint;
  readonly term: bigint;
  readonly commandId: string;
}

export interface SnapshotContext {
  readonly lastAppliedIndex: bigint;
  readonly lastAppliedTerm: bigint;
}

export interface RestoreContext {
  readonly lastIncludedIndex: bigint;
  readonly lastIncludedTerm: bigint;
}

export interface ReplicatedStateMachine<Command, Result> {
  apply(command: Readonly<Command>, context: ApplyContext): Result | Promise<Result>;
  createSnapshot(context: SnapshotContext): Uint8Array | Promise<Uint8Array>;
  restoreSnapshot(snapshot: Uint8Array, context: RestoreContext): void | Promise<void>;
}

export interface CommandCodec<Command> {
  encode(command: Command): Uint8Array;
  decode(bytes: Uint8Array): Command;
}
