# transient-failure-reachable

Obligation, not a bug. `TransientFailureUnreachable` is a predicate that
must fail once `MaxRetries > 0`, or the coverage `smoke.cfg` exists to add
for transient failure and retry is vacuous.

Violated invariant: `TransientFailureUnreachable` (safety, TLC exit 12).

Trace, in prose: writer `w1` pins a flush on shard `s1` and attempts a PUT;
the object store reports a transient failure with no durable effect. The
invariant is a post-state check on `faultFired`, the execution witness
`TransientFailure` alone sets, so TLC reports it violated only once that
transition actually fires, at depth 3 (PinFlush, then TransientFailure; 13
states generated, 13 distinct).

The obligation is correct exactly because TLC reports it violated: a model
where a transient failure could never occur would leave this green, and the
retry-after-transient-failure path this config's `smoke.cfg` counterpart
covers would be untested.
