# put-commit-lost-response-reachable

Obligation, not a bug. `PutCommitLostResponseUnreachable` is a predicate
that must fail once `MaxRetries > 0`, or the retry coverage `smoke.cfg`
exists to add is vacuous: nothing would ever exercise a writer retrying
after losing the commit PUT's response.

Violated invariant: `PutCommitLostResponseUnreachable` (safety, TLC exit
12).

Trace, in prose: writer `w1` issues the commit PUT for its flush on shard
`s1`; the object store durably writes the record but the response back to
the writer is lost. The writer, having no acknowledgement, retries. That is
the `PutCommitLostResponse` state the invariant says must not exist, and TLC
finds it at depth 5 (21 states generated, 20 distinct).

The obligation is correct exactly because TLC reports it violated: a model
where a lost commit-PUT response could never happen would leave this green,
and the retry-after-lost-response path this config's `smoke.cfg` counterpart
covers would be untested.
