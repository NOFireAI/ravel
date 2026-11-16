# put-data-lost-response-reachable

Obligation, not a bug. `PutDataLostResponseUnreachable` is a predicate that
must fail once `MaxRetries > 0`, or the retry coverage that `smoke.cfg`
adds is vacuous: nothing would ever exercise a writer retrying after losing
the data PUT's response.

Violated invariant: `PutDataLostResponseUnreachable` (safety, TLC exit 12).

Trace, in prose: writer `w1` pins a flush, issues the data PUT for it on
shard `s1`; the object store durably writes the object but the response
back to the writer is lost. The writer, having no acknowledgement, retries.
The invariant is a post-state check on `faultFired`, the execution witness
`PutDataLostResponse` alone sets, so TLC reports it violated only once that
transition actually fires, at depth 3 (PinFlush, PutDataLostResponse; 11
states generated, 11 distinct).

The obligation is correct exactly because TLC reports it violated: a model
where a lost data-PUT response could never happen would leave this green,
and the retry-after-lost-response path this config's `smoke.cfg` counterpart
covers would be untested.
