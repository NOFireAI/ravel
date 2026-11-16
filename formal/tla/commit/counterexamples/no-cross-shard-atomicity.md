# no-cross-shard-atomicity

Obligation, not a bug. Ravel offers no cross-shard atomicity: a state with
one shard's commit durable and another shard's not durable must be
reachable, or every property that assumes independent per-shard commit is
checking a model that cannot exhibit the behaviour Ravel actually has.

Violated invariant: `NoCrossShardAtomicityUnreachable` (safety, TLC exit 12).

Trace, in prose: writer `w1` flushes content across shards `s1` and `s2`.
One shard's commit record becomes durable while the other shard's flush is
still in progress, with no coordination forcing the two to advance
together. That mixed-durability state is exactly what
`NoCrossShardAtomicityUnreachable` claims cannot exist, so TLC reports it
violated at depth 5 (186 states generated, 140 distinct).

The obligation is correct exactly because TLC reports it violated: a model
that made cross-shard commit atomic would leave this green, which would
mean the model no longer matched the per-shard commit path in
`crates/ravel-commit`.
