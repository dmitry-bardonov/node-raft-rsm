import { describe, expect, it } from 'vitest';
import { SimulationController } from '../src/server/simulation-controller.js';

describe('SimulationController', () => {
  it('runs an election, commits a command, and recovers a disabled node', async () => {
    const simulation = await SimulationController.create(3, 42);
    try {
      await simulation.execute({ type: 'campaign', nodeId: 'node-1' });
      expect(simulation.snapshot().messages.length).toBeGreaterThan(0);
      await simulation.execute({ type: 'drain' });
      expect(simulation.snapshot().nodes.find(({ id }) => id === 'node-1')?.role).toBe('leader');

      await simulation.execute({
        type: 'propose',
        nodeId: 'node-1',
        command: 'put',
        key: 'counter',
        value: '1',
      });
      await simulation.execute({ type: 'drain' });
      await simulation.execute({ type: 'heartbeat', nodeId: 'node-1' });
      await simulation.execute({ type: 'drain' });
      expect(simulation.snapshot().nodes.map(({ values }) => values.counter)).toEqual([
        '1',
        '1',
        '1',
      ]);

      await simulation.execute({ type: 'partition', left: ['node-3'] });
      expect(simulation.snapshot().blockedEdges).toHaveLength(4);
      await simulation.execute({ type: 'heal' });
      await simulation.execute({ type: 'disable', nodeId: 'node-3' });
      expect(simulation.snapshot().nodes.find(({ id }) => id === 'node-3')?.online).toBe(false);
      await simulation.execute({ type: 'restart', nodeId: 'node-3' });
      expect(simulation.snapshot().nodes.find(({ id }) => id === 'node-3')?.values.counter).toBe(
        '1',
      );
    } finally {
      await simulation.stop();
    }
  });

  it('supports a configurable cluster size', async () => {
    const simulation = await SimulationController.create(5, 7);
    try {
      expect(simulation.snapshot().nodes.map(({ id }) => id)).toEqual([
        'node-1',
        'node-2',
        'node-3',
        'node-4',
        'node-5',
      ]);
    } finally {
      await simulation.stop();
    }
  });
});
