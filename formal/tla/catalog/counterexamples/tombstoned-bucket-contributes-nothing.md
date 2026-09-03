# tombstoned-bucket-contributes-nothing (mutant note)

`TombstonedBucketContributesNothing` has no dedicated switch; its non-vacuity is
argued here.

Invariant: for every tombstoned hour `H`, no `FoldEntriesFor(w)` at any
watermark `w` names an entry in `H`.

## Antecedent is reachable

`DoTombstone(H)` sets `tomb[H] = TRUE` once the hour is maintenance-sealed and
past the retention horizon, and the runs reach such states with an hour that
also holds committed L0 or L1 records. The quantifier body is therefore checked
against a tombstoned hour that would otherwise contribute entries.

## Mutation that would falsify it

Drop the `~tomb[e[2]]` conjunct from `FoldEntriesFor` (and from `ListingView`).
A fold over a tombstoned hour would then re-include its records, so a retired
bucket would reappear in a snapshot after retention had excluded it. The
tombstone marker is what a later fold consults to contribute nothing for the
hour (`crates/ravel-maintain/src/retention.rs::write_tombstone`), which this
invariant pins.
