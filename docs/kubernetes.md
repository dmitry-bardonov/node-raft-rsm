# Kubernetes

Use a three- or five-replica `StatefulSet`, a headless Service, stable identities such as `kv-0`, one ReadWriteOnce PVC per member, topology spread/anti-affinity, and a PodDisruptionBudget. Startup waits for storage recovery; readiness should require recovery and an operator-defined catch-up threshold. Rolling updates must be conservative and preserve quorum.

HPA must not add or remove voters. Three pods on one physical host are one failure domain. Loss of quorum stops writes by design. Membership changes are not implemented. Replacing a PVC is not equivalent to restarting the same member. Graceful termination helps availability but is not a safety assumption. Example manifests remain deferred until the durable/real-transport integration test exists.
