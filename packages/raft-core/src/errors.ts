export class RaftError extends Error {
  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

export class NotLeaderError extends RaftError {
  public constructor(public readonly leaderHint: string | null) {
    super(
      leaderHint === null
        ? 'node is not the leader'
        : `node is not the leader; leader is ${leaderHint}`,
    );
  }
}
export class NoKnownLeaderError extends RaftError {}
export class NoQuorumError extends RaftError {}
export class ProposalTimeoutError extends RaftError {}
export class CommandIdConflictError extends RaftError {}
export class NodeStoppedError extends RaftError {}
export class StorageError extends RaftError {}
export class TransportError extends RaftError {}
export class StateMachineError extends RaftError {}
export class ProtocolError extends RaftError {}
