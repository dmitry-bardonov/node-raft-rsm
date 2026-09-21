import type { NodeId, RaftMessage, RaftTransport } from '@node-raft-rsm/core';

type Handler = (message: RaftMessage) => Promise<void>;

export class MemoryNetwork {
  readonly #handlers = new Map<NodeId, Handler>();
  readonly #queue: RaftMessage[] = [];
  readonly #blocked = new Set<string>();
  readonly #pendingWaiters = new Set<() => void>();
  #dropAll = false;

  public endpoint(nodeId: NodeId): MemoryRaftTransport {
    return new MemoryRaftTransport(nodeId, this);
  }

  public register(nodeId: NodeId, handler: Handler): void {
    if (this.#handlers.has(nodeId)) throw new Error(`transport already started for ${nodeId}`);
    this.#handlers.set(nodeId, handler);
  }

  public unregister(nodeId: NodeId): void {
    this.#handlers.delete(nodeId);
  }

  public enqueue(message: RaftMessage): void {
    if (!this.#dropAll && !this.#blocked.has(edge(message.from, message.to))) {
      this.#queue.push(message);
      for (const notify of this.#pendingWaiters) notify();
      this.#pendingWaiters.clear();
    }
  }

  public waitForPending(timeoutMs = 1_000): Promise<void> {
    if (this.#queue.length > 0) return Promise.resolve();
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0)
      return Promise.reject(new Error('pending-message timeout must be a positive integer'));
    return new Promise((resolve, reject) => {
      const notify = (): void => {
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(() => {
        this.#pendingWaiters.delete(notify);
        reject(new Error(`no network message became pending within ${timeoutMs.toString()}ms`));
      }, timeoutMs);
      this.#pendingWaiters.add(notify);
    });
  }

  public partition(left: readonly NodeId[], right: readonly NodeId[]): void {
    for (const from of left)
      for (const to of right) {
        this.#blocked.add(edge(from, to));
        this.#blocked.add(edge(to, from));
      }
  }

  public heal(): void {
    this.#blocked.clear();
    this.#dropAll = false;
  }

  public setDropAll(drop: boolean): void {
    this.#dropAll = drop;
  }

  public duplicate(index = 0): void {
    const message = this.#queue[index];
    if (message !== undefined) this.#queue.splice(index, 0, message);
  }

  public reorder(): void {
    this.#queue.reverse();
  }

  public get pending(): number {
    return this.#queue.length;
  }

  public async deliver(index = 0): Promise<boolean> {
    const [message] = this.#queue.splice(index, 1);
    if (message === undefined) return false;
    const handler = this.#handlers.get(message.to);
    if (handler === undefined || this.#blocked.has(edge(message.from, message.to))) return false;
    await handler(message);
    return true;
  }

  public async drain(limit = 10_000): Promise<void> {
    let delivered = 0;
    while (this.#queue.length > 0) {
      if (++delivered > limit) throw new Error('network drain limit exceeded');
      await this.deliver();
    }
  }
}

export class MemoryRaftTransport implements RaftTransport {
  #started = false;
  public constructor(
    private readonly nodeId: NodeId,
    private readonly network: MemoryNetwork,
  ) {}

  public async start(handler: Handler): Promise<void> {
    await Promise.resolve();
    this.network.register(this.nodeId, handler);
    this.#started = true;
  }

  public async send(peerId: NodeId, message: RaftMessage): Promise<void> {
    await Promise.resolve();
    if (!this.#started) throw new Error('transport is not started');
    if (message.from !== this.nodeId || message.to !== peerId)
      throw new Error('message envelope does not match transport');
    this.network.enqueue(message);
  }

  public async stop(): Promise<void> {
    await Promise.resolve();
    this.network.unregister(this.nodeId);
    this.#started = false;
  }
}

function edge(from: NodeId, to: NodeId): string {
  return `${from}\0${to}`;
}
