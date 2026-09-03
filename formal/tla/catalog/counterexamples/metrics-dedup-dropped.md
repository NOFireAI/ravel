# metrics-dedup-dropped

Negative control. Switch `DropMetricsDedup = TRUE` stops the metrics query from
collapsing duplicate sources by identity, while the signal is still metrics
(`DedupBySignal = TRUE`). `CompIds = {g1, g2}` so a two-source conflict is
reachable.

Violated invariant: `SignalDedupContract` (safety, TLC exit 12).

## Trace shape

1. Ingest publishes an L0 commit for a record `r` in an hour, and the clock
   advances past the maintenance seal.
2. Two compaction records `g1` and `g2` are both published over that hour with
   `r` in each output set (the resolve-time input-set conflict that
   docs/catalog-and-mvcc.md tie-breaks). `SourcesServing(H, r) = 2`.
3. With `DropMetricsDedup` on, `MetricsMult(H, r)` returns the raw count `2`
   rather than collapsing it to `1`.

At that state `MetricsMult(H, r) = 2 > 1` while `DedupBySignal` holds, so
`SignalDedupContract` is false.

## Why it is the right control

For metrics, query-time dedup by identity
(`crates/ravel-query/src/engine.rs::is_greater`) collapses a same-identity
conflict to one served sample, which is what keeps a transiently double-named
record from being counted twice. The switch turning that collapse off is what
makes the contract falsifiable rather than tautological: the two-source state
is genuinely reachable, so the dedup does real work.
