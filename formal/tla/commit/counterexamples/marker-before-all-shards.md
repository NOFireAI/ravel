# marker-before-all-shards

Negative control. `MarkerAfterFirstShard = TRUE` writes the idempotency
marker as soon as any one shard's commit is durable, rather than after every
shard of the request.

Violated invariant: `MarkerImpliesAllShardsDurable` (safety, TLC exit 12).

Trace, in prose: a two-shard request commits on the first shard and the
marker lands. The second shard has nothing durable. A retry of the same
request now finds the marker, replays the stored token and writes nothing,
so the second shard's records are lost while the client is told the request
already succeeded.

The correct model gates the marker on every shard being durable, which is
what the logs and traces handlers do: the marker PUT runs only after the
router returns tokens for the whole request.
