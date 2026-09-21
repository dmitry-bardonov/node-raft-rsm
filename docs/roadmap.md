# Roadmap

Correctness milestones come first: complete crash/restart simulation and safety properties; add Worker-owned SQLite with crash-point tests; add a bounded authenticated transport with three-process failover; finish snapshot files, installation, compaction, and interruption testing; then perform a public-API and compatibility audit.

Only after that evidence: ReadIndex linearizable reads, joint-consensus membership, learner nodes, leadership transfer, TLS provisioning helpers, more storage adapters, richer Kubernetes tooling, multi-Raft/sharding, and performance optimization. Benchmarks must never silently weaken durability.
