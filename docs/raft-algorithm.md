# Raft algorithm mapping

Followers accept valid leader replication and grant at most one vote per term. An election timeout makes a follower/candidate increment its term, persist a self-vote, and request votes. A quorum makes it leader; leadership appends a no-op in the new term. Any higher term causes immediate step-down and clears the vote durably.

`RequestVoteRequest` carries the candidate's last index and term. A receiver compares last term first, then index. `AppendEntriesRequest` carries `prevLogIndex`, `prevLogTerm`, zero or more entries, and `leaderCommit`. Followers reject a missing/mismatched prefix, truncate only an uncommitted conflict, append the missing suffix, and bound commit by their local last index.

Leaders track `nextIndex` and `matchIndex`. Rejection moves `nextIndex` backward and retries. A leader advances its commit index by quorum counting only when the candidate entry belongs to its current term; older entries become committed indirectly. Code terms map directly to the extended Raft paper: `HardState.currentTerm`, `HardState.votedFor`, `LogEntry`, and volatile `RaftStatus`/leader peer progress.

Dynamic membership, learners, pre-vote, leadership transfer, ReadIndex, leases, and conflict-term acceleration are deliberately omitted.
