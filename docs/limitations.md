# Limitations

This project is experimental and has limited adoption and correctness evidence. It currently lacks durable SQLite, a real authenticated transport, comprehensive restart/property testing, a complete snapshot lifecycle, dynamic membership, durable command deduplication, linearizable reads, and multi-process operational evidence.

Node.js event-loop stalls and GC pauses can trigger elections and reduce availability. Fixed membership complicates replacement. Raft does not make external side effects exactly-once and cannot tolerate Byzantine members. Operating local replicated storage is materially harder than running stateless services over a managed database. Use the code for evaluation and continued development, not production data.
