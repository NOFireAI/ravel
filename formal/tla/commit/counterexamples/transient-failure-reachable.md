# transient-failure-reachable

Obligation, not a bug. `TransientFailureUnreachable` is a predicate that
must fail once `MaxRetries > 0`, or the coverage `smoke.cfg` exists to add
for transient failure and retry is vacuous.

Violated invariant: `TransientFailureUnreachable` (safety, TLC exit 12).

Trace, in prose: writer `w1` attempts a PUT for its flush on shard `s1` and
the object store reports a transient failure with no durable effect. That
is the `TransientFailure` state the invariant says must not exist, and TLC
finds it in two steps (2 states generated, 2 distinct, depth 2).

The obligation is correct exactly because TLC reports it violated: a model
where a transient failure could never occur would leave this green, and the
retry-after-transient-failure path this config's `smoke.cfg` counterpart
covers would be untested.
