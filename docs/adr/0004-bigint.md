# ADR 0004: Bigint counters

Status: accepted.

Terms, indexes, and positions are `bigint` end-to-end. Wire/storage adapters must encode decimal strings or bind 64-bit integers directly and must never round-trip through JavaScript `number`.
