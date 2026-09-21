import { describe, expect, it } from 'vitest';
import {
  PROTOCOL_VERSION,
  RaftCore,
  clusterId,
  nodeId,
  type AppendEntriesRequest,
  type RaftReady,
  type RequestVoteResponse,
} from '../src/index.js';

const cluster = clusterId('test-cluster');
const a = nodeId('a');
const b = nodeId('b');
const c = nodeId('c');

function makeCore(id = a): RaftCore {
  return new RaftCore({
    nodeId: id,
    clusterId: cluster,
    members: [a, b, c],
    electionTimeout: 10,
    heartbeatInterval: 2,
  });
}

function advance(core: RaftCore, ready: RaftReady | undefined): RaftReady {
  if (ready === undefined) throw new Error('expected a Ready batch');
  core.advance(ready.id);
  return ready;
}

function elect(core: RaftCore): RaftReady {
  advance(core, core.step({ type: 'election-timeout', electionTimeout: 10 }));
  const vote: RequestVoteResponse = {
    type: 'request-vote-response',
    protocolVersion: PROTOCOL_VERSION,
    clusterId: cluster,
    from: b,
    to: a,
    term: 1n,
    voteGranted: true,
  };
  return advance(core, core.step({ type: 'message', message: vote }));
}

describe('RaftCore elections', () => {
  it('increments the term, persists its self-vote, and requests votes', () => {
    const core = makeCore();
    const ready = advance(core, core.step({ type: 'election-timeout', electionTimeout: 13 }));
    expect(core.status).toMatchObject({ role: 'candidate', currentTerm: 1n, leaderId: null });
    expect(ready.hardState).toEqual({ currentTerm: 1n, votedFor: a, commitIndex: 0n });
    expect(ready.outboundMessages).toHaveLength(2);
  });

  it('becomes leader with a quorum and appends a no-op in its term', () => {
    const core = makeCore();
    const ready = elect(core);
    expect(core.status.role).toBe('leader');
    expect(ready.unstableEntries).toMatchObject([{ index: 1n, term: 1n, type: 'noop' }]);
    expect(
      ready.outboundMessages.filter((message) => message.type === 'append-entries-request'),
    ).toHaveLength(2);
  });

  it('rejects a stale candidate log', () => {
    const core = new RaftCore({
      nodeId: a,
      clusterId: cluster,
      members: [a, b, c],
      electionTimeout: 10,
      heartbeatInterval: 2,
      persisted: {
        hardState: { currentTerm: 2n, votedFor: null, commitIndex: 0n },
        entries: [{ index: 1n, term: 2n, type: 'noop', payload: new Uint8Array() }],
        appliedIndex: 0n,
      },
    });
    const ready = advance(
      core,
      core.step({
        type: 'message',
        message: {
          type: 'request-vote-request',
          protocolVersion: PROTOCOL_VERSION,
          clusterId: cluster,
          from: b,
          to: a,
          term: 3n,
          lastLogIndex: 0n,
          lastLogTerm: 0n,
        },
      }),
    );
    expect(ready.outboundMessages[0]).toMatchObject({ voteGranted: false, term: 3n });
  });

  it('grants at most one vote in a term', () => {
    const core = makeCore();
    const request = {
      type: 'request-vote-request' as const,
      protocolVersion: PROTOCOL_VERSION,
      clusterId: cluster,
      from: b,
      to: a,
      term: 1n,
      lastLogIndex: 0n,
      lastLogTerm: 0n,
    };
    const first = advance(core, core.step({ type: 'message', message: request }));
    expect(first.outboundMessages[0]).toMatchObject({ voteGranted: true });
    const second = advance(core, core.step({ type: 'message', message: { ...request, from: c } }));
    expect(second.outboundMessages[0]).toMatchObject({ voteGranted: false });
  });
});

describe('RaftCore replication', () => {
  it('repairs an uncommitted conflicting suffix', () => {
    const core = new RaftCore({
      nodeId: b,
      clusterId: cluster,
      members: [a, b, c],
      electionTimeout: 10,
      heartbeatInterval: 2,
      persisted: {
        hardState: { currentTerm: 2n, votedFor: null, commitIndex: 1n },
        entries: [
          { index: 1n, term: 1n, type: 'noop', payload: new Uint8Array() },
          { index: 2n, term: 2n, type: 'noop', payload: new Uint8Array() },
        ],
        appliedIndex: 1n,
      },
    });
    const request: AppendEntriesRequest = {
      type: 'append-entries-request',
      protocolVersion: PROTOCOL_VERSION,
      clusterId: cluster,
      from: a,
      to: b,
      term: 3n,
      prevLogIndex: 1n,
      prevLogTerm: 1n,
      entries: [{ index: 2n, term: 3n, type: 'noop', payload: new Uint8Array() }],
      leaderCommit: 1n,
    };
    const ready = advance(core, core.step({ type: 'message', message: request }));
    expect(ready.truncateFrom).toBe(2n);
    expect(ready.unstableEntries[0]).toMatchObject({ index: 2n, term: 3n });
    expect(ready.outboundMessages[0]).toMatchObject({ success: true, matchIndex: 2n });
  });

  it('commits a current-term entry after quorum replication and delivers in order', () => {
    const core = makeCore();
    elect(core);
    const ackNoop = {
      type: 'append-entries-response' as const,
      protocolVersion: PROTOCOL_VERSION,
      clusterId: cluster,
      from: b,
      to: a,
      term: 1n,
      success: true,
      matchIndex: 1n,
      rejectHint: 2n,
    };
    const commitReady = advance(core, core.step({ type: 'message', message: ackNoop }));
    expect(commitReady.committedEntries.map((entry) => entry.index)).toEqual([1n]);
    const proposal = advance(
      core,
      core.step({ type: 'propose', commandId: 'cmd-1', payload: new Uint8Array([7]) }),
    );
    expect(proposal.committedEntries).toHaveLength(0);
    const committed = advance(
      core,
      core.step({ type: 'message', message: { ...ackNoop, matchIndex: 2n, rejectHint: 3n } }),
    );
    expect(committed.hardState?.commitIndex).toBe(2n);
    expect(committed.committedEntries.map((entry) => entry.commandId)).toEqual(['cmd-1']);
  });

  it('steps down immediately on a higher term', () => {
    const core = makeCore();
    elect(core);
    advance(
      core,
      core.step({
        type: 'message',
        message: {
          type: 'append-entries-request',
          protocolVersion: PROTOCOL_VERSION,
          clusterId: cluster,
          from: b,
          to: a,
          term: 4n,
          prevLogIndex: 0n,
          prevLogTerm: 0n,
          entries: [],
          leaderCommit: 0n,
        },
      }),
    );
    expect(core.status).toMatchObject({ role: 'follower', currentTerm: 4n, leaderId: b });
  });

  it('does not commit an old-term entry by quorum counting alone', () => {
    const core = new RaftCore({
      nodeId: a,
      clusterId: cluster,
      members: [a, b, c],
      electionTimeout: 10,
      heartbeatInterval: 2,
      persisted: {
        hardState: { currentTerm: 1n, votedFor: null, commitIndex: 0n },
        entries: [{ index: 1n, term: 1n, type: 'noop', payload: new Uint8Array() }],
        appliedIndex: 0n,
      },
    });
    advance(core, core.step({ type: 'election-timeout', electionTimeout: 10 }));
    advance(
      core,
      core.step({
        type: 'message',
        message: {
          type: 'request-vote-response',
          protocolVersion: PROTOCOL_VERSION,
          clusterId: cluster,
          from: b,
          to: a,
          term: 2n,
          voteGranted: true,
        },
      }),
    );
    const oldOnly = advance(
      core,
      core.step({
        type: 'message',
        message: {
          type: 'append-entries-response',
          protocolVersion: PROTOCOL_VERSION,
          clusterId: cluster,
          from: b,
          to: a,
          term: 2n,
          success: true,
          matchIndex: 1n,
          rejectHint: 2n,
        },
      }),
    );
    expect(oldOnly.committedEntries).toHaveLength(0);
    expect(core.status.commitIndex).toBe(0n);
  });
});
