import type { NodeId, RaftMessage, RaftTransport } from '@node-raft-rsm/core';

type Handler = (message: RaftMessage) => Promise<void>;

export interface PendingNetworkMessage {
  readonly id: number;
  readonly message: RaftMessage;
}

export class MemoryNetwork {
  readonly #handlers = new Map<NodeId, Handler>();
  readonly #queue: PendingNetworkMessage[] = [];
  readonly #blocked = new Set<string>();
  readonly #pendingWaiters = new Set<() => void>();
  #dropAll = false;
  #nextMessageId = 1;

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
      this.#queue.push({ id: this.#nextMessageId++, message });
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
    const pending = this.#queue[index];
    if (pending !== undefined)
      this.#queue.splice(index, 0, { id: this.#nextMessageId++, message: pending.message });
  }

  public reorder(): void {
    this.#queue.reverse();
  }

  public get pending(): number {
    return this.#queue.length;
  }

  public pendingMessages(): readonly PendingNetworkMessage[] {
    return this.#queue.map(({ id, message }) => ({ id, message }));
  }

  public blockedEdges(): readonly { readonly from: NodeId; readonly to: NodeId }[] {
    return [...this.#blocked].map((blocked) => {
      const separator = blocked.indexOf('\0');
      return {
        from: blocked.slice(0, separator) as NodeId,
        to: blocked.slice(separator + 1) as NodeId,
      };
    });
  }

  public drop(index = 0): boolean {
    return this.#queue.splice(index, 1).length === 1;
  }

  public async deliverById(id: number): Promise<boolean> {
    const index = this.#queue.findIndex((pending) => pending.id === id);
    if (index === -1) return false;
    return this.deliver(index);
  }

  public dropById(id: number): boolean {
    const index = this.#queue.findIndex((pending) => pending.id === id);
    return index !== -1 && this.drop(index);
  }

  public duplicateById(id: number): boolean {
    const index = this.#queue.findIndex((pending) => pending.id === id);
    if (index === -1) return false;
    this.duplicate(index);
    return true;
  }

  public async deliver(index = 0): Promise<boolean> {
    const [pending] = this.#queue.splice(index, 1);
    if (pending === undefined) return false;
    const { message } = pending;
    const handler = this.#handlers.get(message.to);
    if (handler === undefined || this.#blocked.has(edge(message.from, message.to))) return false;
    await handler(message);
    return true;
  }

  public async drain(limit = 10_000): Promise<void> {
    let delivered = 0;
    for (;;) {
      while (this.#queue.length > 0) {
        if (++delivered > limit) throw new Error('network drain limit exceeded');
        await this.deliver();
      }
      await new Promise<void>((resolve) => setImmediate(resolve));
      if (this.#queue.length === 0) return;
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
