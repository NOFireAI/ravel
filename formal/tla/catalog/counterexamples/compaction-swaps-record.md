# compaction-swaps-record

Negative control. Switch `CompactionSwapsRecord = TRUE` makes a compaction
output swap one input identity for a spare while keeping the record count, the
change the counts-only conservation gate cannot see.

Violated invariant: `CompactionPreservesMultiset` (safety, TLC exit 12).

## Trace shape

1. Ingest publishes at least one L0 commit into an hour, and the clock advances
   past the maintenance seal so the hour is compactable within the lag bound.
2. `DoCompact` runs. With the switch on and a spare record available, it builds
   `out = (in \ {one input}) \cup {one spare}`: the cardinality is unchanged,
   so `conserve_exact` (counts only) would still pass at runtime.
3. The published record has `out # in`.

At that state `CompactionPreservesMultiset` is false for the compacted hour.

## Why it is the right control

The runtime gate is counts-only by design
(`crates/ravel-maintain/src/publish.rs::conserve_exact`); the multiset-level
equality is an assumption discharged offline by
`crates/ravel-query/tests/differential_compaction.rs::mixed_l0_l1_snapshot_end_to_end`.
This control shows the model's `CompactionPreservesMultiset` is the exact-
identity property that offline oracle must protect, and that the counts gate
alone does not imply it.
