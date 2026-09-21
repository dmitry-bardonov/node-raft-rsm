export const PROTOCOL_VERSION = 1 as const;

declare const nodeIdBrand: unique symbol;
declare const clusterIdBrand: unique symbol;
export type NodeId = string & { readonly [nodeIdBrand]: true };
export type ClusterId = string & { readonly [clusterIdBrand]: true };

export function nodeId(value: string): NodeId {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(value)) throw new Error('invalid node ID');
  return value as NodeId;
}

export function clusterId(value: string): ClusterId {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(value)) throw new Error('invalid cluster ID');
  return value as ClusterId;
}

export type Role = 'follower' | 'candidate' | 'leader';
export type EntryType = 'command' | 'configuration' | 'noop';

export interface HardState {
  readonly currentTerm: bigint;
  readonly votedFor: NodeId | null;
  readonly commitIndex: bigint;
}

export interface LogEntry {
  readonly index: bigint;
  readonly term: bigint;
  readonly type: EntryType;
  readonly payload: Uint8Array;
  readonly commandId?: string;
}

export interface Snapshot {
  readonly formatVersion: 1;
  readonly clusterId: ClusterId;
  readonly lastIncludedIndex: bigint;
  readonly lastIncludedTerm: bigint;
  readonly members: readonly NodeId[];
  readonly data: Uint8Array;
  readonly checksum: string;
}

interface MessageBase {
  readonly protocolVersion: number;
  readonly clusterId: ClusterId;
  readonly from: NodeId;
  readonly to: NodeId;
  readonly term: bigint;
}

export interface RequestVoteRequest extends MessageBase {
  readonly type: 'request-vote-request';
  readonly lastLogIndex: bigint;
  readonly lastLogTerm: bigint;
}

export interface RequestVoteResponse extends MessageBase {
  readonly type: 'request-vote-response';
  readonly voteGranted: boolean;
}

export interface AppendEntriesRequest extends MessageBase {
  readonly type: 'append-entries-request';
  readonly prevLogIndex: bigint;
  readonly prevLogTerm: bigint;
  readonly entries: readonly LogEntry[];
  readonly leaderCommit: bigint;
}

export interface AppendEntriesResponse extends MessageBase {
  readonly type: 'append-entries-response';
  readonly success: boolean;
  readonly matchIndex: bigint;
  readonly rejectHint: bigint;
}

export type RaftMessage =
  RequestVoteRequest | RequestVoteResponse | AppendEntriesRequest | AppendEntriesResponse;

export type RaftEvent =
  | { readonly type: 'election-timeout'; readonly electionTimeout: number }
  | { readonly type: 'heartbeat-timeout' }
  | { readonly type: 'message'; readonly message: RaftMessage }
  | { readonly type: 'propose'; readonly commandId: string; readonly payload: Uint8Array };

export interface RaftReady {
  readonly id: bigint;
  readonly hardState?: HardState;
  readonly truncateFrom?: bigint;
  readonly unstableEntries: readonly LogEntry[];
  readonly snapshot?: Snapshot;
  readonly outboundMessages: readonly RaftMessage[];
  readonly committedEntries: readonly LogEntry[];
}

export interface RaftStatus {
  readonly nodeId: NodeId;
  readonly clusterId: ClusterId;
  readonly role: Role;
  readonly currentTerm: bigint;
  readonly leaderId: NodeId | null;
  readonly lastLogIndex: bigint;
  readonly commitIndex: bigint;
  readonly appliedIndex: bigint;
  readonly snapshotIndex: bigint;
}

export interface PersistedRaftState {
  readonly hardState: HardState;
  readonly entries: readonly LogEntry[];
  readonly snapshot?: Snapshot;
  readonly appliedIndex: bigint;
}

export interface RaftCoreOptions {
  readonly nodeId: NodeId;
  readonly clusterId: ClusterId;
  readonly members: readonly NodeId[];
  readonly electionTimeout: number;
  readonly heartbeatInterval: number;
  readonly maxMessageBytes?: number;
  readonly persisted?: PersistedRaftState;
}

export function assertNever(value: never): never {
  throw new Error(`unhandled variant: ${JSON.stringify(value)}`);
}
