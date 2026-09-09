# fold-names-entry-above-watermark

Negative control. Switch `FoldNamesEntryAboveWatermark = TRUE` drops the
`e[2] <= w` bound from `FoldEntriesFor`, letting a fold's scan admit an entry
from an hour it has not sealed through.

Violated invariant: `SnapshotEntriesBelowWatermark` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. State 2: ingest publishes an L0 commit into hour 1 (`rA` lands in
   `l0[1]`); hour 0 stays empty.
2. States 3-4: the clock advances to 2, sealing only hour 0
   (`FoldSealDelay = 1`).
3. State 5: `DoFoldStart` computes watermark `w = 0`. With the bound
   dropped, it stages hour 1's entry anyway:
   `foldStage.entries = {<<"l0", 1, rA>>}`.
4. State 6: `DoFoldPutPart` writes the snapshot part carrying that entry.
5. State 7: `DoFoldCas` publishes HEAD: `head.status = "valid"`, `head.wm =
   0`, `head.entries = {<<"l0", 1, rA>>}`.

At State 7, `head.status = "valid"` and the entry's hour (`1`) exceeds
`head.wm` (`0`), so `SnapshotEntriesBelowWatermark` is false.

## Why it is the right control, and the reconnaissance correction

The real fold's watermark bound,
`crates/ravel-catalog/src/fold.rs::sealed_watermark_hour`, never lets a scan
admit an hour past what it has sealed through: a reader trusting the
published watermark would otherwise treat an unsealed, still-mutable hour as
part of a stable snapshot. This control drops that bound to show it is
load-bearing rather than vacuous.
