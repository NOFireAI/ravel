# fold-includes-tombstoned-entries

Negative control. Switch `FoldIncludesTombstonedEntries = TRUE` drops the
`~tomb[e[2]]` filter from `FoldEntriesFor`, letting a fold's fresh scan stage
an entry from an hour retention already tombstoned.

Violated invariant: `TombstonedBucketContributesNothing` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. States 2-3: ingest publishes an L0 commit into both hours (`rA` lands in
   hour 0, then hour 1).
2. States 4-6: the clock advances to 3.
3. State 7: `DoTombstone` retires hour 0 (`tomb[0]' = TRUE`).
4. State 8: `DoFoldStart` runs a fresh scan (`baseAbsent = TRUE`,
   `reconcileLo = -1`). With the filter dropped, it stages hour 0's entry
   anyway: `foldStage.entries = {<<"l0", 0, rA>>, <<"l0", 1, rA>>}` and
   `tombAtStage[0] = TRUE`.
5. State 9: `DoFoldPutPart` writes the snapshot part carrying both entries.
6. State 10: `DoFoldCas` publishes HEAD: `lastHead.kind = "fold"`,
   `lastHead.entries` includes `<<"l0", 0, rA>>`, `lastHead.tombAtWrite[0] =
   TRUE`, `lastHead.reconcileLo = -1`.

At State 10, hour 0's entry satisfies `e[2] >= lastHead.reconcileLo` (`0 >=
-1`), so the invariant requires `~lastHead.tombAtWrite[0]`, which is false.

## Why it is the right control, and the reconnaissance correction

The real fold's tombstone check,
`crates/ravel-maintain/src/retention.rs::write_tombstone` (enforced at scan
time in `crates/ravel-catalog/src/fold.rs::reconcile_one_bucket`), never lets
a retired bucket contribute an entry to a fresh or reconciled scan. This
control drops that filter to show the check is load-bearing rather than
vacuous: without it, a tombstoned hour's superseded-looking L0 record would
resurface in the very next fold's published HEAD.
