# ADR 0003: Ready/advance lifecycle

Status: accepted.

The core emits one outstanding `RaftReady`; the runtime must persist, send, apply, persist applied position, and then call `advance(id)`. A conservative batch barrier is preferred to an initially faster but difficult-to-audit dependency graph.
