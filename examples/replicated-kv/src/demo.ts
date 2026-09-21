import { clusterId, nodeId, type RaftStatus } from '@node-raft-rsm/core';
import { JsonCommandCodec, RaftNode } from '@node-raft-rsm/node';
import { MemoryRaftStorage } from '@node-raft-rsm/storage-memory';
import { MemoryNetwork } from '@node-raft-rsm/transport-memory';
import {
  KvStateMachine,
  validateKvCommand,
  type KvCommand,
  type KvResult,
} from './state-machine.js';

const members = [nodeId('kv-0'), nodeId('kv-1'), nodeId('kv-2')] as const;
const demoClusterId = clusterId('kv-demo');
const network = new MemoryNetwork();
const codec = new JsonCommandCodec<KvCommand>(validateKvCommand);

const replicas = await Promise.all(
  members.map(async (member) => {
    const machine = new KvStateMachine();
    const node = await RaftNode.create({
      nodeId: member,
      clusterId: demoClusterId,
      members,
      heartbeatInterval: 2,
      electionTimeoutMinMs: 10,
      electionTimeoutMaxMs: 20,
      automaticTimers: false,
      storage: new MemoryRaftStorage(),
      transport: network.endpoint(member),
      stateMachine: machine,
      codec,
    });
    await node.start();
    return { node, machine };
  }),
);

const leader = replicas[0];
if (leader === undefined) throw new Error('demo cluster has no leader candidate');

try {
  console.log('Starting deterministic three-node Raft KV demo...');
  await leader.node.campaign();
  await network.drain();
  if (leader.node.status.role !== 'leader') throw new Error('kv-0 did not win the election');
  console.log(
    `Leader elected: ${leader.node.status.nodeId}, term ${leader.node.status.currentTerm.toString()}`,
  );

  console.log(
    'put counter=1:',
    await proposeAndDrain(leader.node, { type: 'put', key: 'counter', value: '1' }, 'demo-put-1'),
  );
  console.log(
    'compare-and-set counter 1→2:',
    await proposeAndDrain(
      leader.node,
      { type: 'compare-and-set', key: 'counter', expected: '1', value: '2' },
      'demo-cas-2',
    ),
  );
  console.log(
    'expected business conflict:',
    await proposeAndDrain(
      leader.node,
      { type: 'compare-and-set', key: 'counter', expected: '1', value: '3' },
      'demo-cas-conflict',
    ),
  );

  await leader.node.heartbeat();
  await network.drain();

  const values = replicas.map(({ machine }) => machine.get('counter'));
  const hashes = replicas.map(({ machine }) => machine.stateHash());
  const status = replicas.map(({ node }) => printableStatus(node.status));
  console.log('Replica values:', values);
  console.log('State hashes:', hashes);
  console.log('Node status:', status);

  if (!values.every((value) => value === '2')) throw new Error('replica values diverged');
  if (!hashes.every((hash) => hash === hashes[0])) throw new Error('replica hashes diverged');
  console.log('PASS: all three replicas committed and applied identical state.');
} finally {
  await Promise.all(replicas.map(async ({ node }) => node.stop()));
}

async function proposeAndDrain(
  leaderNode: RaftNode<KvCommand, KvResult>,
  command: KvCommand,
  commandId: string,
): Promise<KvResult> {
  const result = leaderNode.propose(command, { commandId, timeoutMs: 5_000 });
  await network.waitForPending();
  await network.drain();
  return result;
}

function printableStatus(status: RaftStatus): Record<string, string> {
  return {
    nodeId: status.nodeId,
    role: status.role,
    term: status.currentTerm.toString(),
    leaderId: status.leaderId ?? 'unknown',
    lastLogIndex: status.lastLogIndex.toString(),
    commitIndex: status.commitIndex.toString(),
    appliedIndex: status.appliedIndex.toString(),
  };
}
