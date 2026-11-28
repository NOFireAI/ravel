# Catalog MVCC model

Machine-checked model of the catalog plane: fold, snapshots, compaction, and
MVCC (ADR-1113 decision D3, task T3, issue #1121). One `(tenant, signal)`
catalog over a single shard, a small set of ingest hours, and a small pool of
abstract commit-record and compaction-record identities.

## Files

```
CatalogMVCC.tla     the contract module (operators, actions, invariants)
MCCatalogMVCC.tla   model-check entry: folder symmetry, switch defaults
smoke.cfg           fast safety config (symmetry-reduced)
exhaustive.cfg      full safety + liveness config
carryforward.cfg    three-hour safety config that exercises the incremental fold
                    carry-forward branch (paired with its non-vacuity probe)
overlap.cfg         three-record, two-hour safety config that exercises
                    overlapping L1 outputs (paired with its non-vacuity probe)
positive/           targeted safety configs the positive lane runs, each
                    inside its bands.tsv row
  plan-leaves-late-record-out.cfg
                    the late-commit fence at three publishes, where a
                    compaction meets a late record (paired with its
                    non-vacuity probe)
bands.tsv           per-config distinct-state and depth bands
traceability.md     TLA+ action or property -> Rust source table
results.md          recorded figures and the bands they must stay in
counterexamples/    one prose note per negative control, plus the recorded
                    liveness shrink for the one temporal property that is
                    defined but not checked
negative/           negative-control configs, one broken switch each
  <name>.cfg        a config that flips exactly one switch
  <name>.expect     the exit code and property that config must violate
```

## What the model drives

The HEAD register is a `RavelObjectStore` instance, so its create, CAS, and
lost-response semantics are exactly the shared object-store contract rather than
a re-modeled copy. The immutable planes (L0 commits, L1 compaction records,
snapshot parts, tombstones) are append-only model variables. Actors: ingest
publishes L0 records into not-yet-fold-sealed hours; the compactor publishes an
L1 record over a maintenance-sealed hour behind a counts-only conservation
gate; the folder reads HEAD, stages a snapshot part, publishes it, then
CAS-swaps HEAD, and rebases if it outran the protection horizon; the sweeper
runs the HEAD-gated superseded-input sweep (ADR-0020 delete blocker) and the
HEAD-gated catalog-object sweep, both fail-closed on an unreadable HEAD;
retention tombstones a bucket; and a query resolves and pins a snapshot or
degrades to a listing.

Two seal predicates are modeled distinctly: the fold watermark adds
`FoldSealDelay` (the fold safety margin) on top of the maintenance seal, so a
bucket is compactable strictly before it is foldable. A commit that lands
between the two is late (ADR-4561): the store stamped it at or after the
late-commit cutoff, which is the maintenance seal. A compaction plans only
the on-time records of its hour and leaves the late ones where they are.

## Named safety invariants

`HeadNamesOnlyCompleteParts`, `CompactionPreservesMultiset`,
`CompactionRecordImmutable`, `ReconcileOnlyOnWatermarkAdvance`,
`SnapshotEntriesBelowWatermark`, `PinnedSnapshotStableWithinAttempt`,
`NoLiveCommitOmittedByLostCas`, `MissingIndexDegradesToListing`,
`CorruptHeadFailsClosedOnDeletePaths`, `HeadNamedObjectNeverDeleted`,
`TombstonedBucketContributesNothing`, `SignalDedupContract`,
`DedupPreservesCoverage`, and `PlanLeavesLateRecordsOut`. Temporal
(exhaustive only): `QueryTerminates`.
`LateSupersessionEventuallyReflected` is defined but not checked: it is a
recorded shrink (finite-model liveness limitation; see
`counterexamples/late-supersession-shrink.md`).

Thirteen of the fourteen named invariants above are shown non-vacuous by a
negative control that flips one switch and makes them falsifiable (see
`negative/` and `counterexamples/`); none of those thirteen is left to a
mutant note. `DedupPreservesCoverage` is STORE-derived rather than
switch-gated, so it has no `negative/*.cfg` counterpart; it is shown
non-vacuous by a historical before/after demonstration instead (see
`counterexamples/dedup-starvation-fixed.md`). Five configs are non-vacuity
probes rather than switch mutants, each checking a refuted reachability
property instead of flipping a switch:

- `negative/carryforward-nonvacuity.cfg` checks `NoCarryForward` over
  three-hour bounds where a watermark-advancing fold is reachable, so TLC
  reporting it violated proves the bounded incremental fold's carry-forward
  branch does real work (see `carryforward.cfg` and
  `counterexamples/carryforward-nonvacuity.md`).
- `negative/frontier-reconcile-nonvacuity.cfg` checks `NoFrontierReconcile`,
  so TLC reporting it violated proves the ADR-0020 retention-frontier
  reconcile added to `DoFoldStart` / `DoRivalFoldWin` is exercised, not dead
  code, and that `TombstonedBucketContributesNothing`'s
  `frontierReconciled` disjunct is not vacuous.
- `negative/compaction-loser-diverged-nonvacuity.cfg` checks
  `NoCompactionLoserDivergence`, so TLC reporting it violated proves the
  `"diverged"` member of `lastCompact.outcome`'s widened alphabet (finding
  6) is reachable, not a value the model can define but never hit (see
  `counterexamples/compaction-loser-diverged-nonvacuity.md`).
- `negative/overlap-nonvacuity.cfg` checks `NoOverlappingL1Output` over
  three-record, two-hour bounds where the same record identity can be
  committed into two different hours and separately compacted in each, so
  TLC reporting it violated proves two published L1 outputs whose `out`
  sets overlap are reachable (finding 7), not a case `smoke.cfg` or
  `exhaustive.cfg`'s bounds ever reach (see `overlap.cfg` and
  `counterexamples/overlap-nonvacuity.md`).
- `negative/late-record-left-out-nonvacuity.cfg` checks
  `NoLateRecordLeftOut` at the bounds of
  `positive/plan-leaves-late-record-out.cfg`, so TLC reporting it violated
  proves a compaction publishes while its hour holds a late record the plan
  left out, and the positive pass checks `PlanLeavesLateRecordsOut` where the
  fence does work. `smoke.cfg` and `exhaustive.cfg` have too few publishes
  to reach that state (see
  `counterexamples/late-record-left-out-nonvacuity.md`).

## Running

```sh
scripts/check-tla.sh smoke        -a catalog   # fast safety (symmetry-reduced)
scripts/check-tla.sh positive     -a catalog   # targeted safety configs, banded
scripts/check-tla.sh negative     -a catalog   # every negative control must fail
scripts/check-tla.sh traceability -a catalog   # every source ref resolves
scripts/check-tla.sh exhaustive   -a catalog   # full safety + liveness
```

The harness resolves Java from `RAVEL_TLA_JAVA` if set, else `java` on `PATH`.
See `../README.md` for the toolchain and jar-pinning details.
