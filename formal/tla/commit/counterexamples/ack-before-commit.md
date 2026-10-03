# ack-before-commit

Negative control. `AckAtEnqueue = TRUE` issues the STRICT acknowledgement at
pin time, before any object is durable. Buffered mode's own early
acknowledgement is a separate and correct action, and is not what this switch
models.

Violated invariant: `StrictAckImpliesDurable` (safety, TLC exit 12).

Trace, in prose: a writer pins a flush and the client is told the write
succeeded in strict mode. Neither the data object nor the commit record
exists yet, so a reader that trusts the acknowledgement and asks for the
named commit finds nothing.

The correct model acknowledges only from the `committed` phase, matching the
order in `crates/ravel-ingest/src/shard.rs::ack_waiters` and the router's
token collection.
