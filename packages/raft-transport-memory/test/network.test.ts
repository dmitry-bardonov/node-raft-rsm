import { clusterId, nodeId, PROTOCOL_VERSION, type RaftMessage } from '@node-raft-rsm/core';
import { describe, expect, it, vi } from 'vitest';
import { MemoryNetwork } from '../src/index.js';

describe('MemoryNetwork inspection controls', () => {
  it('assigns stable IDs and supports duplicate, drop, and ID-based delivery', async () => {
    const network = new MemoryNetwork();
    const from = nodeId('node-1');
    const to = nodeId('node-2');
    const receive = vi.fn<(message: RaftMessage) => Promise<void>>(() => Promise.resolve());
    await network.endpoint(to).start(receive);
    network.enqueue({
      type: 'request-vote-request',
      protocolVersion: PROTOCOL_VERSION,
      clusterId: clusterId('network-test'),
      from,
      to,
      term: 1n,
      lastLogIndex: 0n,
      lastLogTerm: 0n,
    });

    const [original] = network.pendingMessages();
    expect(original).toBeDefined();
    if (original === undefined) return;
    expect(network.duplicateById(original.id)).toBe(true);
    const [duplicate, stillOriginal] = network.pendingMessages();
    expect(duplicate?.id).not.toBe(stillOriginal?.id);
    expect(network.dropById(duplicate?.id ?? -1)).toBe(true);
    await expect(network.deliverById(original.id)).resolves.toBe(true);
    expect(receive).toHaveBeenCalledOnce();
    expect(network.pendingMessages()).toEqual([]);
  });

  it('reports both directed edges of a partition', () => {
    const network = new MemoryNetwork();
    const first = nodeId('node-1');
    const second = nodeId('node-2');
    network.partition([first], [second]);
    expect(network.blockedEdges()).toEqual([
      { from: first, to: second },
      { from: second, to: first },
    ]);
    network.heal();
    expect(network.blockedEdges()).toEqual([]);
  });
});
