import type { ReplicatedStateMachine } from '@node-raft-rsm/core';

export type KvCommand =
  | { readonly type: 'put'; readonly key: string; readonly value: string }
  | { readonly type: 'delete'; readonly key: string };

export interface KvResult {
  readonly previousValue: string | null;
  readonly value: string | null;
}

export class KvStateMachine implements ReplicatedStateMachine<KvCommand, KvResult> {
  readonly #values = new Map<string, string>();

  public apply(command: Readonly<KvCommand>): KvResult {
    const previousValue = this.#values.get(command.key) ?? null;
    if (command.type === 'put') this.#values.set(command.key, command.value);
    else this.#values.delete(command.key);
    return { previousValue, value: this.#values.get(command.key) ?? null };
  }

  public createSnapshot(): Uint8Array {
    return new TextEncoder().encode(JSON.stringify([...this.#values.entries()]));
  }

  public restoreSnapshot(bytes: Uint8Array): void {
    const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (!Array.isArray(parsed)) throw new Error('invalid visualizer KV snapshot');
    this.#values.clear();
    for (const entry of parsed) {
      if (
        !Array.isArray(entry) ||
        entry.length !== 2 ||
        typeof entry[0] !== 'string' ||
        typeof entry[1] !== 'string'
      )
        throw new Error('invalid visualizer KV entry');
      this.#values.set(entry[0], entry[1]);
    }
  }

  public inspect(): Readonly<Record<string, string>> {
    return Object.fromEntries(
      [...this.#values.entries()].sort(([left], [right]) => left.localeCompare(right)),
    );
  }
}

export function validateKvCommand(value: unknown): KvCommand {
  if (typeof value !== 'object' || value === null || !('type' in value) || !('key' in value))
    throw new Error('invalid KV command');
  const command = value as {
    readonly type: unknown;
    readonly key: unknown;
    readonly value?: unknown;
  };
  if (typeof command.key !== 'string') throw new Error('invalid KV key');
  if (command.type === 'delete') return { type: 'delete', key: command.key };
  if (command.type === 'put' && typeof command.value === 'string')
    return { type: 'put', key: command.key, value: command.value };
  throw new Error('invalid KV command');
}
