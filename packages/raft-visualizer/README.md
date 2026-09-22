# Raft visualizer

An interactive local laboratory backed by the real `@node-raft-rsm/node` implementation and the
deterministic in-memory transport.

From the repository root, run:

```bash
pnpm visualizer
```

Then open <http://127.0.0.1:3000>.

## Try a complete scenario

1. Leave the cluster at three nodes and select **New cluster**.
2. Select `node-1`, click **Start election**, then **Drain**. The node becomes leader.
3. Enter a key and value, click **Put**, then **Drain**. Send a **Heartbeat** and drain again to
   show the applied value on every replica.
4. Select `node-1` as partition group A and click **Partition**. Dashed red links show blocked
   routes.
5. Campaign a node in the two-node side and drain the queue to observe a replacement election.
6. Use **Disable** and **Restart** to take a replica offline while retaining its durable Raft state.
7. Use the message queue to deliver, duplicate, drop, or reverse messages one at a time.

Click a node card to select it as the control target. Press and hold a card for about 400 ms, then
move the pointer to reposition the node; its custom position is preserved as simulation updates
arrive.

The seed makes queue behavior reproducible. Node count accepts integers from 1 through 9; odd
clusters are normally preferable because an even-sized cluster does not improve failure tolerance
over the preceding odd size.
