# Security model

Raft tolerates crashes and network faults, not malicious members. Members are trusted. Protocol handling rejects unknown peers, wrong cluster IDs, unsupported versions, oversized command bytes, and structurally invalid application values. Further fuzzing and total message-size accounting are required.

Transport authentication/encryption and certificate lifecycle are deployment responsibilities until a real adapter ships. Bind example management APIs to loopback by default and do not expose them unauthenticated. Logs and snapshots may contain secrets; restrict filesystem permissions, encrypt volumes/backups as needed, and never log opaque payloads by default. Bound message, snapshot, pending proposal, and queue sizes to limit denial of service. Pin and audit dependencies and review Worker/codec boundaries as untrusted inputs.
