export type NodeRole = 'follower' | 'candidate' | 'leader' | 'offline';

export interface NodeSnapshot {
  readonly id: string;
  readonly online: boolean;
  readonly role: NodeRole;
  readonly term: string;
  readonly leaderId: string | null;
  readonly lastLogIndex: string;
  readonly commitIndex: string;
  readonly appliedIndex: string;
  readonly values: Readonly<Record<string, string>>;
}

export interface MessageSnapshot {
  readonly id: number;
  readonly type: string;
  readonly from: string;
  readonly to: string;
  readonly term: string;
  readonly detail: string;
}

export interface EdgeSnapshot {
  readonly from: string;
  readonly to: string;
}

export interface TimelineEvent {
  readonly id: number;
  readonly at: string;
  readonly kind: 'system' | 'election' | 'command' | 'network' | 'node' | 'error';
  readonly message: string;
}

export interface ClusterSnapshot {
  readonly revision: number;
  readonly seed: number;
  readonly nodes: readonly NodeSnapshot[];
  readonly messages: readonly MessageSnapshot[];
  readonly blockedEdges: readonly EdgeSnapshot[];
  readonly events: readonly TimelineEvent[];
}

export type SimulationAction =
  | { readonly type: 'reset'; readonly nodeCount: number; readonly seed: number }
  | { readonly type: 'campaign'; readonly nodeId: string }
  | { readonly type: 'heartbeat'; readonly nodeId: string }
  | {
      readonly type: 'propose';
      readonly nodeId: string;
      readonly command: 'put' | 'delete';
      readonly key: string;
      readonly value?: string;
    }
  | { readonly type: 'partition'; readonly left: readonly string[] }
  | { readonly type: 'heal' }
  | { readonly type: 'disable'; readonly nodeId: string }
  | { readonly type: 'restart'; readonly nodeId: string }
  | { readonly type: 'deliver'; readonly messageId: number }
  | { readonly type: 'drop'; readonly messageId: number }
  | { readonly type: 'duplicate'; readonly messageId: number }
  | { readonly type: 'reorder' }
  | { readonly type: 'step' }
  | { readonly type: 'drain' };
