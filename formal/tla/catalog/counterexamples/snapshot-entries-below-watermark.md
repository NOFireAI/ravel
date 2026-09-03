# snapshot-entries-below-watermark (mutant note)

`SnapshotEntriesBelowWatermark` has no dedicated switch, so its non-vacuity is
argued here rather than by a negative control.

Invariant: `head.status = "valid" => \A e \in head.entries : e[2] <= head.wm`.

## Antecedent is reachable

The smoke and exhaustive runs both reach states with `head.status = "valid"`
and a non-empty `head.entries` (every committed fold produces one, for example
the CAS state in the sweep-superseded trace). The implication is therefore not
vacuously true: it is checked against real HEADs that name entries.

## Mutation that would falsify it

Drop the `e[2] <= w` conjunct from `FoldEntriesFor(w)`. A fold at watermark `w`
would then be free to name a commit in an hour above `w` (one still open to
ingest), and its CAS would publish a HEAD with `e[2] > head.wm`, falsifying the
invariant. The clause is present precisely so a fold freezes a prefix at or
below its sealed watermark, which is what lets a reader trust the watermark as
the visibility boundary
(`crates/ravel-catalog/src/fold.rs::sealed_watermark_hour`).
