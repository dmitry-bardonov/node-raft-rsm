import { createHash, randomInt } from 'node:crypto';
import {
  CommandIdConflictError,
  NodeStoppedError,
  NotLeaderError,
  ProposalTimeoutError,
  RaftCore,
  StateMachineError,
  StorageError,
  TransportError,
  type CommandCodec,
  type LogEntry,
  type RaftCoreOptions,
  type RaftEvent,
  type RaftReady,
  type RaftStatus,
  type RaftStorage,
  type RaftTransport,
  type ReplicatedStateMachine,
} from '@node-raft-rsm/core';

export interface RaftNodeOptions<Command, Result> extends Omit<
  RaftCoreOptions,
  'persisted' | 'electionTimeout'
> {
  readonly electionTimeoutMinMs: number;
  readonly electionTimeoutMaxMs: number;
  readonly storage: RaftStorage;
  readonly transport: RaftTransport;
  readonly stateMachine: ReplicatedStateMachine<Command, Result>;
  readonly codec: CommandCodec<Command>;
  readonly commandRetention?: number;
  readonly randomInteger?: (minimum: number, maximumExclusive: number) => number;
  readonly automaticTimers?: boolean;
  readonly onEvent?: (event: RaftNodeEvent) => void;
}

export type RaftNodeEvent =
  | { readonly type: 'role-changed'; readonly status: RaftStatus }
  | { readonly type: 'storage-error'; readonly error: Error }
  | { readonly type: 'transport-error'; readonly error: Error }
  | { readonly type: 'apply-error'; readonly index: bigint; readonly error: Error };

export interface ProposeOptions {
  readonly commandId: string;
  readonly timeoutMs?: number;
}

interface Pending<Result> {
  readonly hash: string;
  readonly promise: Promise<Result>;
  readonly resolve: (result: Result) => void;
  readonly reject: (error: Error) => void;
}

interface Completed<Result> {
  readonly hash: string;
  readonly result: Result;
}

export class RaftNode<Command, Result> {
  readonly #core: RaftCore;
  readonly #storage: RaftStorage;
  readonly #transport: RaftTransport;
  readonly #stateMachine: ReplicatedStateMachine<Command, Result>;
  readonly #codec: CommandCodec<Command>;
  readonly #retention: number;
  readonly #randomInteger: (minimum: number, maximumExclusive: number) => number;
  readonly #options: RaftNodeOptions<Command, Result>;
  readonly #pending = new Map<string, Pending<Result>>();
  readonly #completed = new Map<string, Completed<Result>>();
  #queue: Promise<void> = Promise.resolve();
  #started = false;
  #healthy = true;
  #electionTimer: NodeJS.Timeout | undefined;
  #heartbeatTimer: NodeJS.Timeout | undefined;
  #lastRole: RaftStatus['role'];

  private constructor(options: RaftNodeOptions<Command, Result>, core: RaftCore) {
    this.#options = options;
    this.#core = core;
    this.#storage = options.storage;
    this.#transport = options.transport;
    this.#stateMachine = options.stateMachine;
    this.#codec = options.codec;
    this.#retention = options.commandRetention ?? 10_000;
    this.#randomInteger = options.randomInteger ?? randomInt;
    this.#lastRole = core.status.role;
  }

  public static async create<Command, Result>(
    options: RaftNodeOptions<Command, Result>,
  ): Promise<RaftNode<Command, Result>> {
    validateOptions(options);
    let persisted = await options.storage.load();
    if (persisted.snapshot !== undefined) {
      await options.stateMachine.restoreSnapshot(persisted.snapshot.data, {
        lastIncludedIndex: persisted.snapshot.lastIncludedIndex,
        lastIncludedTerm: persisted.snapshot.lastIncludedTerm,
      });
    }
    const replayAfter = persisted.snapshot?.lastIncludedIndex ?? 0n;
    for (const entry of persisted.entries) {
      if (entry.index <= replayAfter || entry.index > persisted.hardState.commitIndex) continue;
      if (entry.type === 'command') {
        if (entry.commandId === undefined) throw new Error('recovered command has no command ID');
        const command = options.codec.decode(entry.payload);
        await options.stateMachine.apply(command, {
          index: entry.index,
          term: entry.term,
          commandId: entry.commandId,
        });
      }
    }
    if (persisted.appliedIndex < persisted.hardState.commitIndex) {
      await options.storage.persist({
        entries: [],
        appliedIndex: persisted.hardState.commitIndex,
      });
      persisted = await options.storage.load();
    }
    const core = new RaftCore({
      nodeId: options.nodeId,
      clusterId: options.clusterId,
      members: options.members,
      heartbeatInterval: options.heartbeatInterval,
      electionTimeout: options.electionTimeoutMinMs,
      ...(options.maxMessageBytes === undefined
        ? {}
        : { maxMessageBytes: options.maxMessageBytes }),
      persisted,
    });
    return new RaftNode(options, core);
  }

  public get status(): RaftStatus {
    return this.#core.status;
  }

  public async start(): Promise<void> {
    if (this.#started) return;
    if (!this.#healthy) throw new NodeStoppedError('node is unhealthy');
    await this.#transport.start(async (message) => {
      await this.#enqueue(async () => {
        await this.#process({ type: 'message', message });
        if (message.type === 'append-entries-request') this.#resetElectionTimer();
      });
    });
    this.#started = true;
    if (this.#options.automaticTimers !== false) {
      this.#resetElectionTimer();
      this.#heartbeatTimer = setInterval(() => {
        void this.heartbeat().catch((error: unknown) => {
          this.#fail(error);
        });
      }, this.#options.heartbeatInterval);
      this.#heartbeatTimer.unref();
    }
  }

  public async stop(): Promise<void> {
    if (!this.#started) return;
    this.#started = false;
    if (this.#electionTimer !== undefined) clearTimeout(this.#electionTimer);
    if (this.#heartbeatTimer !== undefined) clearInterval(this.#heartbeatTimer);
    for (const pending of this.#pending.values())
      pending.reject(new NodeStoppedError('node stopped'));
    this.#pending.clear();
    await this.#queue;
    await this.#transport.stop();
    await this.#storage.close();
  }

  public async campaign(electionTimeout = this.#options.electionTimeoutMinMs): Promise<void> {
    this.#assertRunning();
    await this.#enqueue(() => this.#process({ type: 'election-timeout', electionTimeout }));
  }

  public async heartbeat(): Promise<void> {
    this.#assertRunning();
    await this.#enqueue(() => this.#process({ type: 'heartbeat-timeout' }));
  }

  public async propose(command: Command, options: ProposeOptions): Promise<Result> {
    this.#assertRunning();
    if (this.#core.status.role !== 'leader') throw new NotLeaderError(this.#core.status.leaderId);
    const payload = this.#codec.encode(command);
    const hash = createHash('sha256').update(payload).digest('hex');
    const completed = this.#completed.get(options.commandId);
    if (completed !== undefined) {
      if (completed.hash !== hash)
        throw new CommandIdConflictError('command ID was already used with different bytes');
      return completed.result;
    }
    const existing = this.#pending.get(options.commandId);
    if (existing !== undefined) {
      if (existing.hash !== hash)
        throw new CommandIdConflictError('command ID is pending with different bytes');
      return withTimeout(existing.promise, options.timeoutMs);
    }
    let resolve!: (result: Result) => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<Result>((resolvePromise, rejectPromise) => {
      resolve = resolvePromise;
      reject = rejectPromise;
    });
    this.#pending.set(options.commandId, { hash, promise, resolve, reject });
    try {
      await this.#enqueue(() =>
        this.#process({ type: 'propose', commandId: options.commandId, payload }),
      );
    } catch (error) {
      this.#pending.delete(options.commandId);
      reject(asError(error));
    }
    return withTimeout(promise, options.timeoutMs);
  }

  public async read<T>(
    reader: () => T | Promise<T>,
    _options: { readonly consistency: 'local' },
  ): Promise<T> {
    this.#assertRunning();
    await this.#queue;
    return reader();
  }

  #enqueue(operation: () => Promise<void>): Promise<void> {
    const result = this.#queue.then(operation);
    this.#queue = result.catch(() => undefined);
    return result;
  }

  async #process(event: RaftEvent): Promise<void> {
    const oldRole = this.#core.status.role;
    const ready = this.#core.step(event);
    if (ready !== undefined) await this.#processReady(ready);
    if (oldRole !== this.#core.status.role || this.#lastRole !== this.#core.status.role) {
      this.#lastRole = this.#core.status.role;
      this.#options.onEvent?.({ type: 'role-changed', status: this.#core.status });
    }
  }

  async #processReady(ready: RaftReady): Promise<void> {
    try {
      await this.#storage.persist({
        ...(ready.hardState === undefined ? {} : { hardState: ready.hardState }),
        ...(ready.truncateFrom === undefined ? {} : { truncateFrom: ready.truncateFrom }),
        entries: ready.unstableEntries,
        ...(ready.snapshot === undefined ? {} : { snapshot: ready.snapshot }),
      });
    } catch (error) {
      const wrapped = new StorageError('failed to persist Raft Ready batch', { cause: error });
      this.#options.onEvent?.({ type: 'storage-error', error: wrapped });
      this.#fail(wrapped);
      throw wrapped;
    }
    for (const message of ready.outboundMessages) {
      try {
        await this.#transport.send(message.to, message);
      } catch (error) {
        const wrapped = new TransportError(`failed to send ${message.type} to ${message.to}`, {
          cause: error,
        });
        this.#options.onEvent?.({ type: 'transport-error', error: wrapped });
      }
    }
    for (const entry of ready.committedEntries) await this.#apply(entry);
    this.#core.advance(ready.id);
  }

  async #apply(entry: LogEntry): Promise<void> {
    if (entry.type !== 'command') {
      await this.#storage.persist({ entries: [], appliedIndex: entry.index });
      return;
    }
    if (entry.commandId === undefined) throw new Error('command entry has no command ID');
    try {
      const command = this.#codec.decode(entry.payload);
      const result = await this.#stateMachine.apply(command, {
        index: entry.index,
        term: entry.term,
        commandId: entry.commandId,
      });
      await this.#storage.persist({ entries: [], appliedIndex: entry.index });
      const pending = this.#pending.get(entry.commandId);
      const hash = createHash('sha256').update(entry.payload).digest('hex');
      if (pending !== undefined) {
        pending.resolve(result);
        this.#pending.delete(entry.commandId);
      }
      this.#completed.set(entry.commandId, { hash, result });
      while (this.#completed.size > this.#retention) {
        const oldest = this.#completed.keys().next().value;
        if (oldest !== undefined) this.#completed.delete(oldest);
      }
    } catch (error) {
      const wrapped = new StateMachineError(
        `state machine failed at index ${entry.index.toString()}`,
        { cause: error },
      );
      this.#options.onEvent?.({ type: 'apply-error', index: entry.index, error: wrapped });
      this.#fail(wrapped);
      throw wrapped;
    }
  }

  #resetElectionTimer(): void {
    if (!this.#started || this.#options.automaticTimers === false) return;
    if (this.#electionTimer !== undefined) clearTimeout(this.#electionTimer);
    const delay = this.#randomInteger(
      this.#options.electionTimeoutMinMs,
      this.#options.electionTimeoutMaxMs + 1,
    );
    this.#electionTimer = setTimeout(() => {
      void this.campaign(delay)
        .then(() => {
          this.#resetElectionTimer();
        })
        .catch((error: unknown) => {
          this.#fail(error);
        });
    }, delay);
    this.#electionTimer.unref();
  }

  #assertRunning(): void {
    if (!this.#started || !this.#healthy)
      throw new NodeStoppedError('node is stopped or unhealthy');
  }

  #fail(error: unknown): void {
    this.#healthy = false;
    const failure = asError(error);
    for (const pending of this.#pending.values()) pending.reject(failure);
    this.#pending.clear();
  }
}

function validateOptions<Command, Result>(options: RaftNodeOptions<Command, Result>): void {
  if (options.electionTimeoutMinMs <= options.heartbeatInterval)
    throw new Error('minimum election timeout must exceed heartbeat interval');
  if (options.electionTimeoutMaxMs < options.electionTimeoutMinMs)
    throw new Error('maximum election timeout must not be less than minimum');
}

function withTimeout<Result>(
  promise: Promise<Result>,
  timeoutMs: number | undefined,
): Promise<Result> {
  if (timeoutMs === undefined) return promise;
  if (timeoutMs <= 0)
    return Promise.reject(new ProposalTimeoutError('proposal timeout must be positive'));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(
        new ProposalTimeoutError('proposal outcome is ambiguous; retry with the same command ID'),
      );
    }, timeoutMs);
    timer.unref();
    void promise.then(
      (result) => {
        clearTimeout(timer);
        resolve(result);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(asError(error));
      },
    );
  });
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
