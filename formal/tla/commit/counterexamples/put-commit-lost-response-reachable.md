# put-commit-lost-response-reachable

Obligation, not a bug. `PutCommitLostResponseUnreachable` is a predicate
that must fail once `MaxRetries > 0`, or the retry coverage that
`smoke.cfg` adds is vacuous: nothing would ever exercise a writer retrying
after losing the commit PUT's response.

Violated invariant: `PutCommitLostResponseUnreachable` (safety, TLC exit
12).

Trace, in prose: writer `w1` pins a flush, puts its data, then issues the
commit PUT on shard `s1`; the object store durably writes the record but
the response back to the writer is lost. The writer, having no
acknowledgement, retries. The invariant is a post-state check on
`faultFired`, the execution witness `PutCommitLostResponse` alone sets, so
TLC reports it violated only once that transition actually fires, at depth
4 (PinFlush, PutData, PutCommitLostResponse; 70 states generated, 63
distinct).

The obligation is correct exactly because TLC reports it violated: a model
where a lost commit-PUT response could never happen would leave this green,
and the retry-after-lost-response path this config's `smoke.cfg` counterpart
covers would be untested.
