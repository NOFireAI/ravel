# Mutant: the counting consumer silently deduplicates

What was broken: in a scratch copy of `MCRavelObjectStore.tla`,
`DeliverGhosts(k)` was changed to set `deliveryCount` to the cardinality
of the deduplicated set instead of incrementing it on every delivery.
This is the shape of a listing consumer that believes each key arrives
once and therefore counts distinct keys, which is wrong under the
contract's "a key may appear more than once" clause.

Which invariant catches it: `ListingConsumersConsistent`, which requires
the counting consumer to equal the total number of deliveries (the size
of the per-key delivery bag in `listState.delivered`) and the
deduplicating consumer to equal the delivered support. The earlier form
of this invariant only required the count to be at least the distinct
cardinality, which the mutant satisfies with a state space byte-identical
to the correct model's, so no band or count could have caught it.

Trace, in prose: a listing begins with `k1` in its snapshot. `k1` is
delivered once, then delivered again (the contract permits a repeated
key). The bag holds two deliveries of `k1`; the mutant's counting
consumer reports 1.

Observed with the harness's TLC command line on `smoke.cfg` against the
mutated copy:

```
Error: Invariant ListingConsumersConsistent is violated.
```

TLC exit 12 (invariant failure). The correct module is unchanged by this
experiment; the copy was discarded.
