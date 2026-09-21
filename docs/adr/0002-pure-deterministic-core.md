# ADR 0002: Pure deterministic core

Status: accepted.

Consensus is a synchronous event-to-state/effects transition. Time, random timeout selection, persistence, networking, and application calls remain in runtime/adapters. This makes schedules reproducible and prevents hidden I/O from violating durability ordering.
