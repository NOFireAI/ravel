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
| smoke.cfg | Spec (safety, symmetry-reduced) | 3463504 | 33 | PASS | 20260904T084623Z-fe770728243fd8e009a9c765ac56521babffadb7 |
| exhaustive.cfg | FairSpec (safety + QueryTerminates liveness) | 3422524 | 31 | PASS | 20260904T084623Z-fe770728243fd8e009a9c765ac56521babffadb7 |
| carryforward.cfg | Spec (safety, three-hour carry-forward) | 4481272 | 37 | PASS | tlc-carryforward-1845293 |
| negative/head-names-unwritten-part.cfg | Spec | first counterexample | n/a | HeadNamesOnlyCompleteParts violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/compaction-swaps-record.cfg | Spec | first counterexample | n/a | CompactionPreservesMultiset violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/compaction-loser-overwrites.cfg | Spec | first counterexample | n/a | CompactionRecordImmutable violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/reconcile-on-tick.cfg | Spec | first counterexample | n/a | ReconcileOnlyOnWatermarkAdvance violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/snapshot-changes-mid-attempt.cfg | Spec | first counterexample | n/a | PinnedSnapshotStableWithinAttempt violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/lost-cas-proceeds.cfg | Spec | first counterexample | n/a | NoLiveCommitOmittedByLostCas violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/metrics-dedup-dropped.cfg | Spec | first counterexample | n/a | SignalDedupContract violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/sweep-superseded-no-head-gate.cfg | Spec | first counterexample | n/a | HeadNamedObjectNeverDeleted violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/carryforward-nonvacuity.cfg | Spec | first counterexample | n/a | NoCarryForward violated (probe), exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/query-fails-closed-on-missing-index.cfg | Spec | first counterexample | n/a | MissingIndexDegradesToListing violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/corrupt-head-ignores-delete-gate.cfg | Spec | first counterexample | n/a | CorruptHeadFailsClosedOnDeletePaths violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/part-corruption-ignores-delete-gate.cfg | Spec | first counterexample | n/a | CorruptHeadFailsClosedOnDeletePaths violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/entry-corruption-ignores-delete-gate.cfg | Spec | first counterexample | n/a | CorruptHeadFailsClosedOnDeletePaths violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/fold-names-entry-above-watermark.cfg | Spec | first counterexample | n/a | SnapshotEntriesBelowWatermark violated, exit 12 | 20260903T213944Z-09c75c8132677327d991b82ba18d8bcb0c836fd1 |
| negative/fold-includes-tombstoned-entries.cfg | Spec | first counterexample | n/a | TombstonedBucketContributesNothing violated, exit 12 | 20260904T084623Z-fe770728243fd8e009a9c765ac56521babffadb7 |
| negative/frontier-reconcile-nonvacuity.cfg | Spec | first counterexample | n/a | NoFrontierReconcile violated (probe), exit 12 | 20260904T084623Z-fe770728243fd8e009a9c765ac56521babffadb7 |
| negative/compaction-loser-diverged-nonvacuity.cfg | Spec | first counterexample | n/a | NoCompactionLoserDivergence violated (probe), exit 12 | 20260904T084623Z-fe770728243fd8e009a9c765ac56521babffadb7 |
| negative/head-corruption-nonvacuity.cfg | Spec | first counterexample | n/a | NoHeadCorrupted violated (probe), exit 12 | 20260904T084623Z-fe770728243fd8e009a9c765ac56521babffadb7 |
| negative/part-unreadable-nonvacuity.cfg | Spec | first counterexample | n/a | NoPartUnreadable violated (probe), exit 12 | 20260904T084623Z-fe770728243fd8e009a9c765ac56521babffadb7 |
| negative/entry-undecodable-nonvacuity.cfg | Spec | first counterexample | n/a | NoEntryUndecodable violated (probe), exit 12 | 20260904T084623Z-fe770728243fd8e009a9c765ac56521babffadb7 |

The twenty `negative/` configs each flip exactly one switch (or, for
`carryforward-nonvacuity.cfg`, `frontier-reconcile-nonvacuity.cfg`,
`compaction-loser-diverged-nonvacuity.cfg`, `head-corruption-nonvacuity.cfg`,
`part-unreadable-nonvacuity.cfg`, and `entry-undecodable-nonvacuity.cfg`,
check a refuted probe) and must exit 12 reporting exactly the named
property; each `.expect` pins that exit code and property, and the negative
lane fails if any config passes or reports a different property. All twenty
were last run together with smoke and exhaustive as run id
`20260904T084623Z-fe770728243fd8e009a9c765ac56521babffadb7`
(`.cache/tla/last-run.tsv`); the Run column above and below cites that id
uniformly.

`counterexamples/late-supersession-shrink.cfg` is a recorded temporal shrink,
not a gate: run under `FairSpec` with `PROPERTY LateSupersessionEventuallyReflected`
it exits 13 (temporal property violated), reproducing the finite-model liveness
limitation documented in `counterexamples/late-supersession-shrink.md`. It is
not run by any harness lane.

## Bands

A run outside these is a regression to investigate, not to widen; see
`bands.tsv`.

- smoke distinct states in [3455000, 3470000], depth in [33, 33].

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
with its own, unaffected bounds.

Issue #1121 round three (finding 1: widening `CorruptHeadFailsClosedOnDeletePaths`
to three fail-closed triggers) then grew the smoke graph again, from 1866396
to 3463504 distinct states, depth 31 to 33: the new one-shot `DoCorruptPart`
and `DoPoisonEntry` actions each add a transition reachable from any state
where a part or entry exists to corrupt, and the widened invariant's extra
`lastDelete` witness fields (`partUnreadable`, `entryUndecodable`) un-collapse
states that the single-trigger version folded together. Round three's finding
2 (dropping `MissingIndexDegradesToListing`'s inert `indexReadableAtResolve`
antecedent, and the matching `qy` field) was re-measured against this new
baseline and confirmed to change nothing: 3463504 distinct states, depth 33,
unchanged, because the removed field never influenced reachability, only the
served/resolved comparison. The band above reflects a fresh measurement of
this round's smoke.cfg run at its current bounds, including all three
rounds' costs. `exhaustive.cfg`'s graph grows by a comparable factor from the
same two new actions; see the Exhaustive band below for the fresh
measurement this round took.

Issue #1121 round four then found that round three's growth, measured
against `exhaustive.cfg`'s larger bounds rather than smoke's, blew the
60-minute per-configuration ceiling (`EXHAUSTIVE_BUDGET=3600` in
`scripts/check-tla.sh`): `13690096` distinct states, depth `33`, `21204`
seconds (353 minutes), against the round-two baseline of `3422524` distinct
states, depth `31`, `1115` seconds -- a 4x growth in states and 19x in wall
clock from finding 1's two new one-shot actions (`DoCorruptPart`,
`DoPoisonEntry`), each reachable from nearly every state once a part or
entry exists to corrupt. It was recorded PASS only because the host running
that measurement lacked `timeout`/`gtimeout` to enforce the ceiling; under
CI's enforcement it would be killed and reported TIMEOUT.

The fix (round four) is a new constant, `EnableDeletePathCorruption`, that
gates `DoCorruptPart` and `DoPoisonEntry` out of `Next` entirely. This is
lever 2 of the three suggested in issue #1121 (gate the new triggers behind
a constant on only in configs checking the invariant against them), chosen
over lever 1 (bound to one occurrence per behaviour) because that bound was
already in place before round four -- both actions were already one-shot
(`partCorruptionUsed`, `entryCorruptionUsed`) and fixed to a single
canonical hour, the same shape as the pre-existing `DoCorruptHead`; the cost
was from being unconditionally enabled at nearly every reachable state, not
from repetition. Lever 3 (shrinking constants) was not needed once lever 2
applied, so no coverage was traded away by narrowing `Hours`, `Records`, or
similar bounds.

`exhaustive.cfg` sets `EnableDeletePathCorruption = FALSE`: it now checks
`CorruptHeadFailsClosedOnDeletePaths` against the HEAD-status trigger only
(`DoCorruptHead`, which stays ungated and unconditional in every
configuration). `smoke.cfg` sets `EnableDeletePathCorruption = TRUE` and is
the configuration that still checks the invariant against all three
triggers together (HEAD-status, covering-part, entry-identity);
`carryforward.cfg` sets it `FALSE`, matching exhaustive.

With the gate off, `exhaustive.cfg` reverts to exactly the round-two graph:
`3422524` distinct states, depth `31`, `PASS` in 362 seconds (well inside
the 3600-second ceiling), run id
`20260904T084623Z-fe770728243fd8e009a9c765ac56521babffadb7`. Band recorded
above and in `bands.tsv`: `[3415000, 3430000]` distinct states, depth
`[31, 31]`, mirroring smoke's existing margin proportion. The round-three
band (`[13680000, 13700000]` distinct states, depth `[33, 33]`) is retired:
it described a graph exhaustive.cfg no longer generates.

Gating the two triggers out of `Next` does not by itself prove they remain
reachable wherever a config still enables them. Three new non-vacuity
probes cover that: `HeadCorruptedExercised`/`NoHeadCorrupted`,
`PartUnreadableExercised`/`NoPartUnreadable`, and
`EntryUndecodableExercised`/`NoEntryUndecodable`, each checked as the sole
`INVARIANT` in its own `negative/*-nonvacuity.cfg` (see Non-vacuity below).
All three ran VIOLATED (exit 12) as expected, proving each fail-closed
trigger is independently reachable, not dead code behind the new gate.

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
| MissingIndexDegradesToListing | WITNESS: `qy.pinned` (what was served) versus `qy.resolvedView` (what the store listing said should be served, computed without the `QueryFailsClosedOnMissingIndex` switch); the two coincide by construction whenever the index was readable, so no separate readable/unreadable antecedent is needed |
| CorruptHeadFailsClosedOnDeletePaths | WITNESS: `lastDelete.headStatus`, the HEAD status at the last real object removal |
| HeadNamedObjectNeverDeleted | STORE: the HEAD register, the L0 plane, and the L1 compaction-record plane |
| TombstonedBucketContributesNothing | WITNESS: `lastHead.entries`, `lastHead.tombAtWrite`, `lastHead.reconcileLo`, and `lastHead.frontierReconciled` of the last fold |
| SignalDedupContract | WITNESS: `qy.dupServed`, recomputed (`RawDupIdentities`) on the entry set the query actually served after `Dedup` ran on it, not on the switch that gates `Dedup` |

## Non-vacuity: behaviour mutant per invariant

Each named safety invariant is shown load-bearing by a behaviour mutant. All
twelve have a dedicated switch and a negative-control config that flips it and
drives TLC to exit 12 on that invariant (the recorded lines are in the run
table above). `CorruptHeadFailsClosedOnDeletePaths` has three: one per
fail-closed trigger (unreadable HEAD, unreadable part, undecodable entry) on
the shared delete-gate guard.

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
| CorruptHeadFailsClosedOnDeletePaths | switch `DeletePathIgnoresUnreadablePart` | negative/part-corruption-ignores-delete-gate.cfg, exit 12 |
| CorruptHeadFailsClosedOnDeletePaths | switch `DeletePathIgnoresUndecodableEntry` | negative/entry-corruption-ignores-delete-gate.cfg, exit 12 |
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

Issue #1121 round four gated `DoCorruptPart` and `DoPoisonEntry` behind
`EnableDeletePathCorruption` to fit exhaustive.cfg's 60-minute ceiling (see
Bands above), which sets that constant `FALSE`. Each of
`CorruptHeadFailsClosedOnDeletePaths`'s three fail-closed triggers is shown
independently reachable, wherever the config that enables it, by its own
non-vacuity probe: `negative/head-corruption-nonvacuity.cfg` checks the
refuted probe `NoHeadCorrupted` (`EnableDeletePathCorruption = FALSE`, since
`DoCorruptHead` stays ungated in every config); `negative/part-unreadable-nonvacuity.cfg`
checks `NoPartUnreadable` and `negative/entry-undecodable-nonvacuity.cfg`
checks `NoEntryUndecodable` (both `EnableDeletePathCorruption = TRUE`, since
`DoCorruptPart`/`DoPoisonEntry` cannot fire otherwise). All three exit 12 as
expected: `NoHeadCorrupted` at depth 2 (12174 states generated, 6488
distinct), `NoPartUnreadable` in one step from Init (42 states, 37 distinct),
`NoEntryUndecodable` in one step from Init (51 states, 46 distinct). This
proves the gate removes only branching, not the underlying behaviour: the
same triggers `negative/corrupt-head-ignores-delete-gate.cfg`,
`negative/part-corruption-ignores-delete-gate.cfg`, and
`negative/entry-corruption-ignores-delete-gate.cfg` mutate against remain
reachable everywhere the invariant is checked, including in exhaustive.cfg's
own HEAD-status-only coverage.

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
mutation switches FALSE, `EnableDeletePathCorruption = TRUE`,
`SYMMETRY Symmetry`. `Records` shrank from `{rA, rB}` to `{rA}` this round
(finding 6, see Bands above) to keep the smoke run inside the fixed
300-second wall-clock budget. `EnableDeletePathCorruption = TRUE` (issue
#1121 round four) makes smoke.cfg the configuration that checks
`CorruptHeadFailsClosedOnDeletePaths` against all three fail-closed triggers
together; see Bands above.

## Exhaustive constants

`Keys = {hk}`, `Content = {hd, nc}` (`NoContent = nc`), `Clients = {f1}`,
`Hours = {0, 1}`, `Records = {rA, rB}`, `CompIds = {g1, g2}`, `MaxClock = 3`,
`MaxOps = 2`, `FoldSealDelay = 1`, `MaintSealDelay = 0`, `ProtectionHorizon = 1`,
`RetentionHorizon = 2`, `LagBound = 1`, `DedupBySignal = TRUE`, all twelve
mutation switches FALSE, `EnableDeletePathCorruption = FALSE`, `FairSpec`
(no symmetry, so liveness is checked soundly). Two records and a second
compaction identity keep the multiset and dedup conflicts non-vacuous. The
version-matched HEAD CAS is exercised without a second modeled folder because
`DoRivalFoldWin` advances HEAD and bumps its object version under an
in-flight fold, so that fold reaches its CAS with a stale base version and
takes the losing branch. `EnableDeletePathCorruption = FALSE` (issue #1121
round four) drops `DoCorruptPart`/`DoPoisonEntry` from this config's `Next`
to fit the 60-minute ceiling; exhaustive.cfg now checks
`CorruptHeadFailsClosedOnDeletePaths` against the HEAD-status trigger only.
See Bands above.

## Carry-forward constants

`Keys = {hk}`, `Content = {hd, nc}` (`NoContent = nc`), `Clients = {f1}`,
`Hours = {0, 1, 2}`, `Records = {rA}`, `CompIds = {g1}`, `MaxClock = 3`,
`MaxOps = 2`, `FoldSealDelay = 0`, `MaintSealDelay = 0`, `ProtectionHorizon = 1`,
`RetentionHorizon = 2`, `LagBound = 2`, `DedupBySignal = TRUE`, all twelve
mutation switches FALSE, `EnableDeletePathCorruption = FALSE`,
`SYMMETRY Symmetry`. Three hours let a valid HEAD fold
at successive watermarks; both seal delays are zero so all three hours seal
within `MaxClock = 3`. The compact-strictly-before-fold gap (`FoldSealDelay = 1`)
is orthogonal to carry-forward and is covered by the smoke and exhaustive
configs. Under this config the full safety invariant set holds while the
incremental fold carries a below-floor hour forward verbatim.
