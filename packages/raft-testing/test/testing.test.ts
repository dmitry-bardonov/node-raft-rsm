import { describe, expect, it } from 'vitest';
import { SeededRandom, VirtualClock } from '../src/index.js';

describe('deterministic testing primitives', () => {
  it('repeats a pseudo-random sequence from its seed', () => {
    const first = new SeededRandom(42);
    const second = new SeededRandom(42);
    expect([first.next(), first.next(), first.next()]).toEqual([
      second.next(),
      second.next(),
      second.next(),
    ]);
  });

  it('executes virtual tasks in timestamp order', () => {
    const clock = new VirtualClock();
    const events: number[] = [];
    clock.schedule(20, () => events.push(20));
    clock.schedule(5, () => events.push(5));
    clock.advance(20);
    expect(events).toEqual([5, 20]);
  });
});
