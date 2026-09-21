# ADR 0005: SQLite in a Worker Thread

Status: planned, not implemented.

Node 24 is the supported LTS. Its built-in SQLite API is release-candidate stability rather than stable, so the first durable adapter will use maintained `better-sqlite3` in a dedicated Worker, `WAL`, `synchronous=FULL`, and explicit transactions. Native build requirements will be documented.
