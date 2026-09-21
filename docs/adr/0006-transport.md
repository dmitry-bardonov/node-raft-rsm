# ADR 0006: Real transport selection

Status: deferred.

Memory transport is implemented for deterministic work. The real adapter will be selected only with explicit framing, deadlines, size/backpressure bounds, peer/cluster identity, reconnect behavior, and TLS hooks. gRPC with a maintained pure-JavaScript stack is preferred; no placeholder public package is exposed meanwhile.
