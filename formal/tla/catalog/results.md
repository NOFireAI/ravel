# Results: catalog MVCC model

Entry module: `MCCatalogMVCC.tla` over `CatalogMVCC.tla`. The per-run figures
are written to `.cache/tla/last-run.tsv`; the enforced bands live in
`bands.tsv`, and `scripts/check-tla.sh` fails a PASS run whose distinct-state
count or depth falls outside them.

Toolchain: TLC2 2.19 (tla2tools 1.7.4, sha256 verified by the harness),
Temurin OpenJDK 21.0.12 on x86_64 Linux, `-workers auto` on a 4-core host.

## Recorded runs

| Config | Spec | Distinct states | Depth | Result | Run |
|---|---|---|---|---|---|
| smoke.cfg | Spec (safety, symmetry-reduced) | 1866396 | 31 | PASS | 20260903T135234Z-a4e9817c24f8fbe59e5dec198aa33c47032f8a47 |
| carryforward.cfg | Spec (safety, three-hour carry-forward) | 4481272 | 37 | PASS | tlc-carryforward-1845293 |
| negative/head-names-unwritten-part.cfg | Spec | first counterexample | n/a | HeadNamesOnlyCompleteParts violated, exit 12 | negative lane |
| negative/compaction-swaps-record.cfg | Spec | first counterexample | n/a | CompactionPreservesMultiset violated, exit 12 | negative lane |
| negative/compaction-loser-overwrites.cfg | Spec | first counterexample | n/a | CompactionRecordImmutable violated, exit 12 | negative lane |
| negative/reconcile-on-tick.cfg | Spec | first counterexample | n/a | ReconcileOnlyOnWatermarkAdvance violated, exit 12 | negative lane |
| negative/snapshot-changes-mid-attempt.cfg | Spec | first counterexample | n/a | PinnedSnapshotStableWithinAttempt violated, exit 12 | negative lane |
| negative/lost-cas-proceeds.cfg | Spec | first counterexample | n/a | NoLiveCommitOmittedByLostCas violated, exit 12 | negative lane |
| negative/metrics-dedup-dropped.cfg | Spec | first counterexample | n/a | SignalDedupContract violated, exit 12 | negative lane |
| negative/sweep-superseded-no-head-gate.cfg | Spec | first counterexample | n/a | HeadNamedObjectNeverDeleted violated, exit 12 | negative lane |
| negative/carryforward-nonvacuity.cfg | Spec | first counterexample | n/a | NoCarryForward violated (probe), exit 12 | tlc-carryforward-nonvacuity-1852394 |
| negative/query-fails-closed-on-missing-index.cfg | Spec | first counterexample | n/a | MissingIndexDegradesToListing violated, exit 12 | negative lane |
| negative/corrupt-head-ignores-delete-gate.cfg | Spec | first counterexample | n/a | CorruptHeadFailsClosedOnDeletePaths violated, exit 12 | negative lane |
| negative/fold-names-entry-above-watermark.cfg | Spec | first counterexample | n/a | SnapshotEntriesBelowWatermark violated, exit 12 | negative lane |
| negative/fold-includes-tombstoned-entries.cfg | Spec | first counterexample | n/a | TombstonedBucketContributesNothing violated, exit 12 | negative lane |
| negative/frontier-reconcile-nonvacuity.cfg | Spec | first counterexample | n/a | NoFrontierReconcile violated (probe), exit 12 | 20260903T132807Z-111d93079ca50bad141da866930552ec362dc447 |
| negative/compaction-loser-diverged-nonvacuity.cfg | Spec | first counterexample | n/a | NoCompactionLoserDivergence violated (probe), exit 12 | negative lane |

The fifteen `negative/` configs each flip exactly one switch (or, for
`carryforward-nonvacuity.cfg`, `frontier-reconcile-nonvacuity.cfg`, and
`compaction-loser-diverged-nonvacuity.cfg`, check a refuted probe) and must
exit 12 reporting exactly the named property; each
`.expect` pins that exit code and property, and the negative lane fails if any
config passes or reports a different property.

`counterexamples/late-supersession-shrink.cfg` is a recorded temporal shrink,
not a gate: run under `FairSpec` with `PROPERTY LateSupersessionEventuallyReflected`
it exits 13 (temporal property violated), reproducing the finite-model liveness
limitation documented in `counterexamples/late-supersession-shrink.md`. It is
not run by any harness lane.

## Bands

A run outside these is a regression to investigate, not to widen; see
`bands.tsv`.

- smoke distinct states in [1861000, 1872000], depth in [31, 31].

The safety model runs to a fixed complete state graph, so its distinct-state
count and depth are deterministic; the band carries a small margin only to
absorb a future toolchain change. This round's `MissingIndexDegradesToListing`
and `SignalDedupContract` fixes (issue #1121 round two) changed `QueryServedView`
and its resolve-time witnesses, which shrank the smoke state graph from the
prior round's 3165708 distinct states to 2910784 (same depth, 32): a query
now dedups the entries it actually serves before recording `dupServed` rather
than carrying a separately-toggled duplicate-count field, collapsing some
previously-distinct `qy` states.

Finding 5 (ADR-0020 retention-frontier reconcile) then grew the smoke graph
again, from 2910784 to 8408098 distinct states (depth unchanged, 32):
`DoFoldStart` and `DoRivalFoldWin` now each existentially choose a frontier
hour from `Hours \cup {-1}`, a three-way branch (no pick, pick hour 0, pick
hour 1) at smoke's two-hour bounds, at every fold-start and rival-fold-win
transition in the graph. This is a real, surfaced cost, not an incidental
shift: a full `SUBSET Hours` frontier choice would have been `2^|Hours|`
branches per fold-start instead of `|Hours| + 1`; the model takes the
single-hour-per-fold abstraction instead (documented beside
`IncrementalFoldEntries` in `CatalogMVCC.tla`) on the argument that each
hour's tombstone filtering is independent of every other hour's, so what a
multi-hour batch reconciles in one fold, a run of single-hour folds
reconciles the same way across several folds -- sound for the safety
invariants this model checks, not a claim about reconcile throughput.

Finding 6 (the `resolve_already_exists` compaction-loser outcome alphabet)
then grew the smoke graph again to 34 million-plus states in progress at the
300-second smoke wall-clock ceiling (`scripts/check-tla.sh` hardcodes this
budget; it is out of `formal/tla/catalog`'s edit scope), before finishing at
all: `lastCompact.outcome` splitting `"converged"` from `"diverged"` is
STORE-derived from `l0[H]` versus `crec[H][g].in`, so it un-collapses many
`l0[H]` subsets that the old boolean `mutated` witness folded into one state,
and that finer state persists in `lastCompact` for the rest of the run
instead of being forgotten. Unlike finding 5's change this added no new
per-transition branching factor (`DoCompactLoser` still has exactly one
successor per firing); the cost is purely from carrying more real
information forward. Rather than widen the smoke band past what the fixed
300-second budget can run to completion, `Records` shrank from `{rA, rB}` to
`{rA}` in `smoke.cfg` (see Smoke constants below): this halves the reachable
`l0[H]` subset space per hour, which is exactly the space
`"converged"`/`"diverged"` classification depends on, and brought the run
back to 1866396 distinct states at depth 31 (one step shallower than before)
in 60 seconds. `CompactionSwapsRecord` is FALSE throughout smoke, so its own
swap mutation (which needs spare capacity in `Records \ inputs`) was never
exercised by smoke's PASS invariants either before or after this change; it
keeps its own non-vacuity proof in `negative/compaction-swaps-record.cfg`
with its own, unaffected bounds. The band above reflects a fresh measurement
of this round's smoke.cfg run, at its current bounds, including both
findings' costs. `exhaustive.cfg`'s graph will grow by a comparable or
larger factor from finding 5's branching the next time an exhaustive run is
in scope (see below); finding 6 adds no branching factor but will still cost
some un-collapsed states there too. That run should re-measure rather than
assume proportional growth, since exhaustive's larger `Hours` and `Records`
sets make both costs larger.

`exhaustive.cfg` carried a band (1185000-1198000 distinct states, depth 31)
from the prior round's measurement, but this round's model changes shifted the
exhaustive graph the same way they shifted smoke's, and this task's rules
forbid an exhaustive TLC run (it risks the idle-timeout kill). Re-measuring
that band is out of scope for this round; the stale band has been removed
from `bands.tsv` rather than left in place unmeasured against the current
model. Re-measure and restore it the next time an exhaustive run is in scope.

`carryforward.cfg` gets no band row: it is a targeted three-hour carry-forward
pass, not the banded gate config, and its full graph (4,481,272 distinct
states, depth 37, recorded here from the prior round) was not re-run this
round; it is not one of this task's required gates. The negative configs are
NOT deterministic: they stop at the first counterexample TLC finds, and under
`-workers auto` which state that is varies between runs, so a negative gets no
band. Each negative is pinned instead by its `.expect` file.

## Invariant derivation audit

Every invariant observes the modelled STORE (objects present, their content, the
HEAD register, an L0 commit or L1 compaction record) or an effect WITNESS that
records what an action actually did, never a compliance flag the model set for
itself. The derivation comment beside each invariant in `CatalogMVCC.tla` is the
authority; the table restates it.

| Invariant | Reads |
|---|---|
| TypeOK | structural: all state variables against their declared domains |
| HeadNamesOnlyCompleteParts | STORE: the HEAD register and the set of present snapshot parts |
| CompactionPreservesMultiset | STORE: the L1 compaction-record plane (`crec`) |
| CompactionRecordImmutable | WITNESS: `lastCompact.outcome` (`"converged"`, `"diverged"`, or `"overwrite"`), set from comparing the loser's recomputed input set to the winner's stored one; only `"overwrite"` changes the stored record |
| ReconcileOnlyOnWatermarkAdvance | WITNESS: `lastHead` wm delta and `entriesChanged` of the last HEAD write |
| SnapshotEntriesBelowWatermark | STORE: the HEAD register |
| PinnedSnapshotStableWithinAttempt | WITNESS: `qy.pinnedAtAttempt` (resolve-time view) versus `qy.pinned` (served now) |
| NoLiveCommitOmittedByLostCas | STORE/WITNESS: HEAD register, the L0 plane, and `maxValidWm` (highest watermark ever on a valid HEAD) |
| MissingIndexDegradesToListing | WITNESS: `qy.indexReadableAtResolve` (`IndexReadable` as observed at resolve), `qy.pinned` (what was served), `qy.resolvedView` (what the store listing said should be served, computed without the `QueryFailsClosedOnMissingIndex` switch) |
| CorruptHeadFailsClosedOnDeletePaths | WITNESS: `lastDelete.headStatus`, the HEAD status at the last real object removal |
| HeadNamedObjectNeverDeleted | STORE: the HEAD register, the L0 plane, and the L1 compaction-record plane |
| TombstonedBucketContributesNothing | WITNESS: `lastHead.entries`, `lastHead.tombAtWrite`, `lastHead.reconcileLo`, and `lastHead.frontierReconciled` of the last fold |
| SignalDedupContract | WITNESS: `qy.dupServed`, recomputed (`RawDupIdentities`) on the entry set the query actually served after `Dedup` ran on it, not on the switch that gates `Dedup` |

## Non-vacuity: behaviour mutant per invariant

Each named safety invariant is shown load-bearing by a behaviour mutant. All
twelve have a dedicated switch and a negative-control config that flips it and
drives TLC to exit 12 on that invariant (the recorded lines are in the run
table above).

| Invariant | Mutant | TLC evidence |
|---|---|---|
| HeadNamesOnlyCompleteParts | switch `HeadNamesUnwrittenPart` | negative/head-names-unwritten-part.cfg, exit 12 |
| CompactionPreservesMultiset | switch `CompactionSwapsRecord` | negative/compaction-swaps-record.cfg, exit 12 |
| CompactionRecordImmutable | switch `CompactionLoserOverwrites` | negative/compaction-loser-overwrites.cfg, exit 12 |
| ReconcileOnlyOnWatermarkAdvance | switch `ReconcileOnTick` | negative/reconcile-on-tick.cfg, exit 12 |
| PinnedSnapshotStableWithinAttempt | switch `SnapshotChangesMidAttempt` | negative/snapshot-changes-mid-attempt.cfg, exit 12 |
| NoLiveCommitOmittedByLostCas | switch `LostCasProceedsOnStaleRead` | negative/lost-cas-proceeds.cfg, exit 12 |
| SignalDedupContract | switch `DropMetricsDedup` | negative/metrics-dedup-dropped.cfg, exit 12 |
| HeadNamedObjectNeverDeleted | switch `SweepSupersededNoHeadGate` | negative/sweep-superseded-no-head-gate.cfg, exit 12 |
| MissingIndexDegradesToListing | switch `QueryFailsClosedOnMissingIndex` | negative/query-fails-closed-on-missing-index.cfg, exit 12 |
| CorruptHeadFailsClosedOnDeletePaths | switch `DeletePathIgnoresUnreadableHead` | negative/corrupt-head-ignores-delete-gate.cfg, exit 12 |
| SnapshotEntriesBelowWatermark | switch `FoldNamesEntryAboveWatermark` | negative/fold-names-entry-above-watermark.cfg, exit 12 |
| TombstonedBucketContributesNothing | switch `FoldIncludesTombstonedEntries` | negative/fold-includes-tombstoned-entries.cfg, exit 12 |

The bounded incremental fold's carry-forward branch is shown non-vacuous
separately by `negative/carryforward-nonvacuity.cfg`, which checks the refuted
probe `NoCarryForward` and exits 12: a watermark-advancing fold is reachable at
those bounds and carries a below-floor hour forward, so the paired
`carryforward.cfg` safety pass is not vacuous.

`TombstonedBucketContributesNothing`'s `frontierReconciled` disjunct (added
for finding 5, ADR-0020) is shown non-vacuous the same way, by
`negative/frontier-reconcile-nonvacuity.cfg`, which checks the refuted probe
`NoFrontierReconcile` and exits 12: a second, watermark-advancing fold
reachable at those bounds picks an already-tombstoned, below-floor hour into
its frontier set, so the frontier-filtering branch of `FrontierAdmits` is
exercised and not dead code. See
`counterexamples/frontier-reconcile-nonvacuity.md` for the recorded trace.

`lastCompact.outcome` (finding 6) now carries the real `resolve_already_exists`
alphabet as far as this model's store state supports: `"converged"` (the
loser's recomputed input set matches the winner's, STORE-derived by comparing
`l0[H]` against `crec[H][g].in`) and `"diverged"` (it does not, the real
`InputSetHashDivergence` path) both leave the record untouched; only
`"overwrite"`, gated by the `CompactionLoserOverwrites` switch, mutates it,
and `negative/compaction-loser-overwrites.cfg` continues to show
`CompactionRecordImmutable` non-vacuous against that switch (unchanged by this
widening: the mutant still flips `"overwrite"` regardless of which of the two
real outcomes would otherwise have applied). The `"diverged"` branch itself is
shown non-vacuous by `negative/compaction-loser-diverged-nonvacuity.cfg`,
which checks the refuted probe `NoCompactionLoserDivergence` and exits 12: at
two-record bounds, a second commit lands in `l0[H]` after the winner already
published, so the loser's retry reads a diverged input set (state 6, depth 6,
1063 states generated, 595 distinct states found). See
`counterexamples/compaction-loser-diverged-nonvacuity.md` for the recorded
trace. One real outcome is intentionally NOT modeled: `ConvergedWinnerPartMissing`,
the fail-closed case where the input sets converge but a referenced L1 part is
missing and unrepairable. This model has no per-part presence in the object
store (`crec` is an abstract record with no tracked part objects), so there is
no store state to ground a third fail-closed branch honestly; adding one would
be a compliance flag standard (a) forbids, not a real gap-closing fix. This is
a disclosed modeling limitation, not a silently dropped finding.

## Fairness

`FairSpec` adds only per-action weak fairness: `WF_vars(DoTick)`,
`WF_vars(FoldProgress)`, and `WF_vars(QueryProgress)`. There is no `WF` or `SF`
over the whole `Next`, and no safety invariant depends on fairness; fairness is
present only so `QueryTerminates` can hold under the bounded clock.

## Smoke constants

`Keys = {hk}`, `Content = {hd, nc}` (`NoContent = nc`), `Clients = {f1}`,
`Hours = {0, 1}`, `Records = {rA}`, `CompIds = {g1}`, `MaxClock = 3`,
`MaxOps = 3`, `FoldSealDelay = 1`, `MaintSealDelay = 0`, `ProtectionHorizon = 1`,
`RetentionHorizon = 2`, `LagBound = 1`, `DedupBySignal = TRUE`, all twelve
mutation switches FALSE, `SYMMETRY Symmetry`. `Records` shrank from `{rA, rB}`
to `{rA}` this round (finding 6, see Bands above) to keep the smoke run
inside the fixed 300-second wall-clock budget.

## Exhaustive constants

`Keys = {hk}`, `Content = {hd, nc}` (`NoContent = nc`), `Clients = {f1}`,
`Hours = {0, 1}`, `Records = {rA, rB}`, `CompIds = {g1, g2}`, `MaxClock = 3`,
`MaxOps = 2`, `FoldSealDelay = 1`, `MaintSealDelay = 0`, `ProtectionHorizon = 1`,
`RetentionHorizon = 2`, `LagBound = 1`, `DedupBySignal = TRUE`, all twelve
mutation switches FALSE, `FairSpec` (no symmetry, so liveness is checked
soundly). Two records and a second compaction identity keep the multiset and
dedup conflicts non-vacuous. The version-matched HEAD CAS is exercised without a
second modeled folder because `DoRivalFoldWin` advances HEAD and bumps its
object version under an in-flight fold, so that fold reaches its CAS with a stale
base version and takes the losing branch.

## Carry-forward constants

`Keys = {hk}`, `Content = {hd, nc}` (`NoContent = nc`), `Clients = {f1}`,
`Hours = {0, 1, 2}`, `Records = {rA}`, `CompIds = {g1}`, `MaxClock = 3`,
`MaxOps = 2`, `FoldSealDelay = 0`, `MaintSealDelay = 0`, `ProtectionHorizon = 1`,
`RetentionHorizon = 2`, `LagBound = 2`, `DedupBySignal = TRUE`, all twelve
mutation switches FALSE, `SYMMETRY Symmetry`. Three hours let a valid HEAD fold
at successive watermarks; both seal delays are zero so all three hours seal
within `MaxClock = 3`. The compact-strictly-before-fold gap (`FoldSealDelay = 1`)
is orthogonal to carry-forward and is covered by the smoke and exhaustive
configs. Under this config the full safety invariant set holds while the
incremental fold carries a below-floor hour forward verbatim.
