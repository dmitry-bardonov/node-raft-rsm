import { describe, expect, it } from 'vitest';
import { KvStateMachine } from '../src/index.js';

describe('KvStateMachine', () => {
  it('returns business conflicts as data', () => {
    const machine = new KvStateMachine();
    expect(
      machine.apply({ type: 'compare-and-set', key: 'x', expected: 'old', value: 'new' }),
    ).toEqual({
      status: 'conflict',
      currentValue: null,
    });
  });

  it('creates deterministic sorted snapshots and restores the complete state', () => {
    const first = new KvStateMachine();
    first.apply({ type: 'put', key: 'z', value: 'last' });
    first.apply({ type: 'put', key: 'a', value: 'first' });
    const second = new KvStateMachine();
    second.apply({ type: 'put', key: 'a', value: 'first' });
    second.apply({ type: 'put', key: 'z', value: 'last' });
    expect(first.createSnapshot()).toEqual(second.createSnapshot());
    const restored = new KvStateMachine();
    restored.restoreSnapshot(first.createSnapshot());
    expect(restored.stateHash()).toBe(first.stateHash());
  });
});
