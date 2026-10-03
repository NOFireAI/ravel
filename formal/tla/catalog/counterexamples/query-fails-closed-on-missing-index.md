# query-fails-closed-on-missing-index

Negative control. Switch `QueryFailsClosedOnMissingIndex = TRUE` makes
`QueryServedView` serve an empty result whenever `~IndexReadable`, instead of
falling back to the direct store listing.

Violated invariant: `MissingIndexDegradesToListing` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. State 1 (Init): HEAD starts absent (`head.status = "absent"`), before any
   fold or compaction has ever published. `IndexReadable == head.status =
   "valid"` is false from the first reachable state, with no HEAD-corrupting
   action needed to reach it.
2. State 2: `DoCommit` publishes `rA` into `l0[1]`. HEAD is still absent.
3. State 3: `DoQueryResolve` resolves with `headStatusAtResolve = "absent"`.
   The store listing already contains the committed record
   (`resolvedView = {<<"l0", 1, rA>>}`), but with the switch on, the query
   serves nothing: `qy.pinned = {}`.

At State 3, `qy.pinned = {}` while `qy.resolvedView = {<<"l0", 1, rA>>}`, so
`MissingIndexDegradesToListing` is false. The trace is depth 4, the shallowest
of the negative lane: no snapshot part ever needs to be lost or corrupted,
because HEAD-absent is already the initial condition and a live L0 record is
enough to make the two views diverge.

## Why it is the right control, and the reconnaissance correction

The real client, `crates/ravel-query/src/engine.rs::QueryEngine::resolve_snapshot_with_retry`,
falls back to a direct store listing whenever it cannot resolve a valid HEAD,
rather than returning nothing: a reader waiting on the very first fold or
compaction to publish must still see records already committed to L0. This
control forces the fail-closed (empty-result) behavior instead to show the
listing fallback is load-bearing rather than vacuous.

A prior version of this control's header comment described the trigger as a
`DoLoseIndexPart` action taking a HEAD-named snapshot part out of `snapParts`
while HEAD stayed valid. No such action exists in `CatalogMVCC.tla`; every
`IndexReadable` reference reduces to `head.status = "valid"` (lines 310, 316,
322), and the actual recorded trace never touches `snapParts` at all — it
reaches `~IndexReadable` for free from the initial HEAD-absent state. The
header comment above corrects that.
