# ADR 0009: Command-ID retention

Status: accepted with release blocker.

The runtime retains 10,000 applied IDs/results by default and rejects the same ID with different bytes. This cache is bounded and currently volatile. Durable snapshot-aware retention is required before exactly-once-like retry behavior can be advertised across restart; external effects remain outside that guarantee regardless.
