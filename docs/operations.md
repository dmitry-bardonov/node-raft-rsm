# Operations

Election timeouts must be comfortably above heartbeat interval and expected event-loop/disk/network tails; copied LAN defaults are unsafe for WANs. Operators need role/term/leader, commit/applied/snapshot positions, proposal latency, pending proposals, peer lag, rejection counts, storage latency/queue depth, event-loop delay, snapshot outcomes, and transport errors. Payload bytes must not be logged.

Back up a stopped member or a validated immutable snapshot plus matching metadata; copying a live database and snapshot independently can create an unusable pair. Disk-full and state-machine errors require intervention, not automatic skipping. Shutdown stops new proposals, drains the serialized Ready queue, then closes transport/storage, but safety never depends on graceful exit.

Upgrades must retain command, wire, and snapshot compatibility. Diagnose incidents with term/role transitions, quorum connectivity, per-peer match positions, disk latency/capacity, and apply lag before restarting members. Never restart a majority simultaneously.
