export class SeededRandom {
  #state: number;

  public constructor(public readonly seed: number) {
    this.#state = seed >>> 0 || 0x6d2b79f5;
  }

  public next(): number {
    let value = (this.#state += 0x6d2b79f5);
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  }

  public integer(minimum: number, maximumExclusive: number): number {
    if (
      !Number.isSafeInteger(minimum) ||
      !Number.isSafeInteger(maximumExclusive) ||
      maximumExclusive <= minimum
    )
      throw new Error('invalid integer range');
    return minimum + Math.floor(this.next() * (maximumExclusive - minimum));
  }
}

export class VirtualClock {
  #now = 0;
  readonly #tasks: { at: number; run: () => void }[] = [];

  public get now(): number {
    return this.#now;
  }

  public schedule(delay: number, run: () => void): void {
    if (delay < 0) throw new Error('delay must not be negative');
    this.#tasks.push({ at: this.#now + delay, run });
    this.#tasks.sort((left, right) => left.at - right.at);
  }

  public advance(milliseconds: number): void {
    const target = this.#now + milliseconds;
    while ((this.#tasks[0]?.at ?? Number.POSITIVE_INFINITY) <= target) {
      const task = this.#tasks.shift();
      if (task === undefined) throw new Error('virtual clock task queue changed unexpectedly');
      this.#now = task.at;
      task.run();
    }
    this.#now = target;
  }
}
