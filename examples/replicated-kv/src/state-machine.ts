import { createHash } from 'node:crypto';
import type { ReplicatedStateMachine } from '@node-raft-rsm/core';

export type KvCommand =
  | { readonly type: 'put'; readonly key: string; readonly value: string }
  | { readonly type: 'delete'; readonly key: string }
  | {
      readonly type: 'compare-and-set';
      readonly key: string;
      readonly expected: string | null;
      readonly value: string;
    };

export type KvResult =
  | { readonly status: 'applied'; readonly value: string | null }
  | { readonly status: 'conflict'; readonly currentValue: string | null };

interface SnapshotV1 {
  readonly version: 1;
  readonly entries: readonly (readonly [string, string])[];
}

export class KvStateMachine implements ReplicatedStateMachine<KvCommand, KvResult> {
  readonly #values = new Map<string, string>();

  public apply(command: Readonly<KvCommand>): KvResult {
    switch (command.type) {
      case 'put':
        this.#values.set(command.key, command.value);
        return { status: 'applied', value: command.value };
      case 'delete': {
        const previous = this.#values.get(command.key) ?? null;
        this.#values.delete(command.key);
        return { status: 'applied', value: previous };
      }
      case 'compare-and-set': {
        const currentValue = this.#values.get(command.key) ?? null;
        if (currentValue !== command.expected) return { status: 'conflict', currentValue };
        this.#values.set(command.key, command.value);
        return { status: 'applied', value: command.value };
      }
    }
  }

  public createSnapshot(): Uint8Array {
    const snapshot: SnapshotV1 = {
      version: 1,
      entries: [...this.#values.entries()].sort(([left], [right]) => left.localeCompare(right)),
    };
    return new TextEncoder().encode(JSON.stringify(snapshot));
  }

  public restoreSnapshot(bytes: Uint8Array): void {
    const snapshot = validateSnapshot(
      JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)),
    );
    this.#values.clear();
    for (const [key, value] of snapshot.entries) this.#values.set(key, value);
  }

  public get(key: string): string | null {
    return this.#values.get(key) ?? null;
  }

  public stateHash(): string {
    return createHash('sha256').update(this.createSnapshot()).digest('hex');
  }
}

export function validateKvCommand(value: unknown): KvCommand {
  if (!isRecord(value) || typeof value.type !== 'string' || typeof value.key !== 'string')
    throw new Error('invalid KV command');
  if (value.key.length === 0 || value.key.length > 1024) throw new Error('invalid KV key length');
  switch (value.type) {
    case 'put':
      if (typeof value.value !== 'string') throw new Error('put value must be a string');
      return { type: 'put', key: value.key, value: value.value };
    case 'delete':
      return { type: 'delete', key: value.key };
    case 'compare-and-set':
      if (
        typeof value.value !== 'string' ||
        (value.expected !== null && typeof value.expected !== 'string')
      )
        throw new Error('invalid compare-and-set value');
      return {
        type: 'compare-and-set',
        key: value.key,
        expected: value.expected,
        value: value.value,
      };
    default:
      throw new Error('unknown KV command type');
  }
}

function validateSnapshot(value: unknown): SnapshotV1 {
  if (!isRecord(value) || value.version !== 1 || !Array.isArray(value.entries))
    throw new Error('unsupported KV snapshot');
  const entries = value.entries.map((entry): readonly [string, string] => {
    if (
      !Array.isArray(entry) ||
      entry.length !== 2 ||
      typeof entry[0] !== 'string' ||
      typeof entry[1] !== 'string'
    )
      throw new Error('invalid KV snapshot entry');
    return [entry[0], entry[1]];
  });
  return { version: 1, entries };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
