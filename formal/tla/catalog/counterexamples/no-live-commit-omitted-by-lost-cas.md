# no-live-commit-omitted-by-lost-cas (mutant note)

`NoLiveCommitOmittedByLostCas` has no dedicated switch; its non-vacuity is
argued here.

Invariant: a valid HEAD names every present, non-superseded, non-tombstoned L0
record at or below its watermark.

## Antecedent is reachable

The runs reach valid HEADs whose watermark covers an hour that still holds a
live, non-superseded L0 commit (any first fold over a committed hour). The
universally quantified body is therefore exercised against non-empty live sets,
not vacuously satisfied.

## Mutation that would falsify it

Two independent mutations break it:

- Have `FoldEntriesFor(w)` omit a live L0 record it should include (for
  example, filter on `SupersededInputs` with the wrong hour). The winning CAS
  would then publish a HEAD missing a live commit below its watermark.
- Let a losing CAS still overwrite HEAD (drop the version-match in `DoFoldCas`
  so a stale folder wins). A folder that read an older HEAD would clobber a
  newer one, dropping commits the newer fold had already named.

The version-matched CAS
(`crates/ravel-catalog/src/fold.rs::get_head::MAX_HEAD_CAS_ATTEMPTS`) and the
faithful classify in `FoldEntriesFor` are what make the invariant hold, so
either mutation is a real omission bug the property catches.
