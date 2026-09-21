import type { CommandCodec } from '@node-raft-rsm/core';

export class JsonCommandCodec<Command> implements CommandCodec<Command> {
  readonly #encoder = new TextEncoder();
  readonly #decoder = new TextDecoder('utf-8', { fatal: true });

  public constructor(private readonly validate: (value: unknown) => Command) {}

  public encode(command: Command): Uint8Array {
    return this.#encoder.encode(JSON.stringify(command));
  }

  public decode(bytes: Uint8Array): Command {
    const parsed: unknown = JSON.parse(this.#decoder.decode(bytes));
    return this.validate(parsed);
  }
}
