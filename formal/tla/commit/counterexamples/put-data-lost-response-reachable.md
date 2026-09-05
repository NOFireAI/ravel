# put-data-lost-response-reachable

Obligation, not a bug. `PutDataLostResponseUnreachable` is a predicate that
must fail once `MaxRetries > 0`, or the retry coverage `smoke.cfg` exists to
add is vacuous: nothing would ever exercise a writer retrying after losing
the data PUT's response.

Violated invariant: `PutDataLostResponseUnreachable` (safety, TLC exit 12).

Trace, in prose: writer `w1` issues the data PUT for its flush on shard
`s1`; the object store durably writes the object but the response back to
the writer is lost. The writer, having no acknowledgement, retries. That is
the `PutDataLostResponse` state the invariant says must not exist, and TLC
finds it in two steps (2 states generated, 2 distinct, depth 2).

The obligation is correct exactly because TLC reports it violated: a model
where a lost data-PUT response could never happen would leave this green,
and the retry-after-lost-response path this config's `smoke.cfg` counterpart
covers would be untested.
