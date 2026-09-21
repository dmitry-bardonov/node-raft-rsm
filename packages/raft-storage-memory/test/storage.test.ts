import { describe, expect, it } from 'vitest';
import { MemoryRaftStorage } from '../src/index.js';

describe('MemoryRaftStorage', () => {
  it('atomically persists hard state and entries and survives logical restart', async () => {
    const storage = new MemoryRaftStorage();
    await storage.persist({
      hardState: { currentTerm: 2n, votedFor: null, commitIndex: 1n },
      entries: [
        { index: 1n, term: 2n, type: 'command', payload: new Uint8Array([1]), commandId: 'x' },
      ],
    });
    const loaded = await storage.load();
    expect(loaded.hardState.currentTerm).toBe(2n);
    expect(loaded.entries[0]?.payload).toEqual(new Uint8Array([1]));
  });

  it('does not mutate state when an injected persistence failure occurs', async () => {
    const storage = new MemoryRaftStorage();
    storage.failNext();
    await expect(
      storage.persist({
        hardState: { currentTerm: 1n, votedFor: null, commitIndex: 0n },
        entries: [],
      }),
    ).rejects.toThrow('injected');
    expect((await storage.load()).hardState.currentTerm).toBe(0n);
  });
});
