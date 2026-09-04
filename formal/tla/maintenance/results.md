# Maintenance results

TLC checked these finite models under the bounds and assumptions recorded here.
This verifies the protocol designs; implementation conformance is argued in
`traceability.md` and asserted by the named Rust tests, not proved.

- Tool: `tla2tools` 1.7.4 (TLC2 version 2.19), Java 21 (Temurin, installed under
  `/tmp`; this host's `$HOME` is a 256 MB tmpfs with no room for a JRE).
- Host: fleet executor (8 cores, 8 GB), `-workers auto` resolves to 8 TLC
  workers.
- Smoke, negative, and traceability run id:
  `20260904T172930Z-3feef83d9bd216e8ee2b57bcf1227ee0ea33e0da` prefix run
  (`scripts/check-tla.sh smoke|negative|traceability -a maintenance`).
  Exhaustive run id: `20260904T165818Z-3feef83d9bd216e8ee2b57bcf1227ee0ea33e0da`
  (`scripts/check-tla.sh exhaustive -a maintenance`) -- each subcommand
  invocation gets its own timestamped run id, so the ids differ even though
  no `.tla` source changed between the two runs; both ran after the F1/F3
  latch and write-counter changes and the F4/F6 ASSUME/dead-code changes had
  all landed. Figures below are copied
  verbatim from each subcommand's `.cache/tla/last-run.tsv` output (the file
  is truncated per subcommand, so figures were captured immediately after
  each call) or from the command's own PASS/VIOLATED summary line. Both
  exhaustive configs were run this session and completed; neither is a
  carried-over or unmeasured figure. `MCMaintenanceOwnership.exhaustive.cfg`'s
  distinct-state count grew roughly thirteenfold over the prior recorded
  figure (1,038,446 to 13,183,990) because F3's bounded second-write counters
  (`hbWriteCount`, `memoWriteCount`) make every heartbeat/memo key reachable
  in both its absent-write and present-write forms, not because of any
  unbounded growth; the run still completes in about thirty minutes, well
  inside the sixty-minute per-configuration budget. `bands.tsv` was
  regenerated from this run rather than adjusted to fit the old figures.

## Configurations

| cfg | TLC | states | distinct | depth | seconds | host | result |
|---|---|---|---|---|---|---|---|
| MCMaintenanceOwnership.smoke.cfg | 1.7.4 | 47377233 | 2773760 | 21 | 90 | fleet executor | PASS |
| MCMaintenanceOwnership.exhaustive.cfg | 1.7.4 | 136617032 | 13183990 | 20 | 1769 | fleet executor | PASS |
| MCCompactionClaims.smoke.cfg | 1.7.4 | 65454526 | 11155721 | 17 | 161 | fleet executor | PASS |
| MCCompactionClaims.exhaustive.cfg | 1.7.4 | 1972 | 543 | 11 | 2 | fleet executor | PASS |
| negative/ownership-as-publication-authority.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |
| negative/heartbeat-memo-cas.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |
| negative/memo-overstamp.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |
| negative/mo-diverge-overwrites-record.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |
| negative/mo-missing-part-reports-converged.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |
| negative/zero-ownership-phantom.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 13) |
| negative/claim-completion-without-cas.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |
| negative/claim-delete-unconditional.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |
| negative/claim-as-publication-authority.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |
| negative/guarded-publish-ignores-claim.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |
| negative/diverge-overwrites-record.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |
| negative/missing-part-reports-converged.cfg | 1.7.4 | - | - | - | - | fleet executor | VIOLATED (exit 12) |

The two smoke configurations and the two exhaustive configurations are banded in
`bands.tsv`; every row was re-measured this run and every band comes from a run
that completed (see "Exhaustive coverage is split by worker count" below for how
`MCMaintenanceOwnership.exhaustive.cfg` gets there). The negative controls stop
at the first counterexample and carry no band. Every smoke and exhaustive config
completes under its budget. The `states` and `distinct` counts for a PASS
(exhaustive-search) row are deterministic; the reported search `depth` varies by
a step or two between runs under TLC's parallel BFS (`-workers auto`), so the
depth bands are a small range around the observed value rather than an exact
figure. A VIOLATED row's states/distinct count is not reproducible: TLC's
parallel BFS (8 workers on this host) stops at the first violating state it
reaches, and that count depends on worker count and scheduling, not on the
model. Only the invariant name and the exit code are stable facts for a VIOLATED
row (exit 12: an `INVARIANT` failed; exit 13: a `PROPERTY` failed), so that is
all this table records for one; see "Per-invariant audit" below for which
invariant each negative control targets.

## Constants chosen for exhaustive

- Ownership: `Workers = {1}` (see "Exhaustive coverage is split by worker
  count" in README.md), `Units = {1}`, `Variants = {iA, iB}`, `H = 1`,
  `Factor = 1` (window 1), `MaxT = 2`, `Phantom = FALSE`, `AllowCrash = FALSE`
  (the stable-membership environment the liveness property names). Safety is
  view-independent, so the two-worker duplicate-ownership race is checked
  exhaustively (up to the sound `MCView` abstraction) in smoke instead, at the
  same `MaxT = 2` with `AllowCrash = FALSE` and `Workers = {1, 2}`. At
  `MaxT = 2` a worker that stops writing heartbeats has its stamp fall outside
  the staleness window and is excluded from the live set, so the
  safety-relevant crash behaviour (a silent worker being dropped) is reachable
  in smoke without the crashed flag. See the staleness reachability and
  worker-count notes below.
- Claims: `Workers = {1, 2}`, `Units = {1}`, `Variants = {iA, iB}`,
  `DeclaredLease = 1`, `MaxObservedLease = 2`, `MaxV = 6`, `MaxTime = 2`,
  `LivenessMode = TRUE` (the paused-holder / fair-thief lifecycle that is the
  named environment for the steal-liveness property). Safety is checked in
  smoke with `LivenessMode = FALSE` and the full lifecycle (renew, complete,
  corrupt, guarded and ungated publish).

Both areas use a single unit. A single `(tenant, signal, shard)` is enough for
every checked property: duplicate publication, the phantom-owner limitation, and
the claim CAS races all manifest on one unit with two workers and two variants.
This is a bounded model check, not a proof for all sizes.

## Staleness reachability, exhaustive termination, and crash coverage

- Staleness reachable in smoke (F5). The staleness gate `Stale(s)` compares
  `now - hbStamp[s]` against the window `Factor * H = 1`. At `MaxT = 1` the
  clock and any heartbeat stamp are at most one apart, so the gate never fires
  and any invariant guarded by it is vacuous. A scratch reachability invariant
  `NoWorkerEverStale` (never committed) run over the correct smoke model
  confirms this: `MaxT = 1` completes with no violation (408320 distinct), and
  `MaxT = 2` is violated at 112 states (a sibling stamped at clock 0 is stale
  once `now = 2`). The smoke config runs `MaxT = 2` for this reason.
- Exhaustive termination requires a finite `versionCounter`, but that alone
  does not make the two-worker graph tractable (F2). The exhaustive config runs
  with no VIEW, so the raw `versionCounter` (bumped by every successful store
  write, an unbounded `Nat`) must stay finite for the search to terminate at
  all. The only action that could re-mint versions without limit is a part
  vanish followed by a re-PUT of the identical content-addressed bytes.
  `VanishPart(u)` is bounded by a `vanishedOnce[u][variant]` latch, so each part
  vanishes at most once and the vanish/re-PUT loop cannot drive `versionCounter`
  forever; the model is finite regardless of `Workers`. That finiteness is
  necessary but not sufficient: at `Workers = {1, 2}` the no-VIEW exhaustive
  graph is finite but still passed 4,600,000 distinct states at depth 13 of an
  eventual 20 without converging in any practical budget. The blowup tracks
  `Workers`, not `versionCounter` or `MaxT` -- the same no-VIEW configuration at
  `Workers = {1}` completes at 13,183,990 distinct states, depth 20, in about
  thirty minutes (see the Configurations table; this grew from 1,038,446
  distinct states after the bounded second-write counters from F3 widened the
  reachable heartbeat/memo write history at every depth). `versionCounter` was
  ruled out as a bounding lever for this: it is already bounded by the
  one-shot vanish latch above, so constraining it further would not shrink the
  two-worker graph, only mask the same blowup behind a different variable.
  `MCMaintenanceOwnership.exhaustive.cfg` therefore runs at `Workers = {1}`;
  the two-worker race is
  covered exhaustively for safety only, via `MCView`, by smoke.cfg. See
  README.md, "Exhaustive coverage is split by worker count", for the full
  rationale and exactly which properties are covered at which worker count.
- Crash coverage (residual). With `AllowCrash = FALSE` in every current config
  (smoke, exhaustive, and all negatives) the `crashed` flag is never set, so the
  `Crash` and `Revive` actions are not exercised by any checked model. Their
  safety-relevant effect (a worker that stops heartbeating is dropped from the
  live set) is subsumed by time staleness at `MaxT = 2`, which is exercised. The
  crashed-flag machinery remains in the spec as the explicit-fault switch; a
  dedicated `AllowCrash = TRUE` config is not run because at `MaxT = 2` it
  exceeds the 300 s smoke budget (over six million distinct states, still
  growing at the cap). This is a coverage gap in the crashed flag itself, not in
  the staleness behaviour it stands for.

## Counterexamples and their classification

No correct-form configuration produced a counterexample. Every counterexample
recorded is a negative control (a single flipped constant), classified here.

- `ownership-as-publication-authority` (safety): an owner publishing the record
  with Overwrite mutates it away from the CreateIfAbsent winner. Classification:
  a **design flaw the design already avoids** -- the invariant proves ownership
  must not be publication authority, which the shipped code respects.
  Prose walk in `negative/counterexamples/`.
- `heartbeat-memo-cas` and `memo-overstamp` (safety): put-mode and the seed
  clamp are load-bearing. Classification: **model demonstration of a required
  clause**; the shipped code writes Overwrite and clamps.
- `zero-ownership-phantom` (liveness): a phantom live member (a lingering
  heartbeat of a departed or restarted worker, within the window, outranking
  every live worker) leaves a unit with no owner in anyone's view. Classification:
  a **documented liveness limitation** (see README), not a defect: the ADR-0065
  membership design accepts that asymmetric views can leave a unit
  transiently unattended, and correctness never depends on it.
- `claim-completion-without-cas`, `claim-delete-unconditional`,
  `claim-as-publication-authority`, `guarded-publish-ignores-claim` (safety):
  the ADR-1029 hazards the design forbids. Classification: **design
  demonstration**; the proposed design uses CasVersion, never deletes, treats no
  claim as publication authority, and abandons the guarded path on a lost claim.
- `diverge-overwrites-record` / `mo-diverge-overwrites-record` and
  `missing-part-reports-converged` / `mo-missing-part-reports-converged`
  (safety, F6/F7): fail-closed convergence (ADR-1113 D3), one pair per model. The
  first flips `DivergeOverwritesRecord = TRUE` so a loser with a divergent
  input-set hash overwrites the record; `DivergentInputSetNeverMutates` catches
  it via the store version delta. The second flips
  `MissingPartReportsConverged = TRUE` so a loser reports `Converged` while the
  winning part is tombstoned; `MergeAttemptsConverge` catches it via the
  store-derived `winnerPartPresent` witness. Classification: **model
  demonstration of a required clause**; the shipped resolution reports
  `ConvergedWinnerPartMissing` or `InputSetHashDivergence` and mutates nothing.
  Prose walks in `negative/counterexamples/`; the scratch-mutant walks that
  confirm the witnesses are store-derived, not self-reported, are in
  `counterexamples/`.

## Non-vacuity of the named invariants

Each specification has at least one invariant shown non-vacuous by a negative
control that flips exactly one constant:

- Ownership: `QueryVisibleDataCorrectUnderDuplicateOwnership` is violated by
  flipping `OwnerPublishOverwrite = TRUE`
  (`ownership-as-publication-authority`). Additionally
  `HeartbeatAndMemoNeverCas` is violated by `HeartbeatMemoUsesCas = TRUE`,
  `MemoNeverExtendsFreshnessPastSnapshot` by `MemoOverstamp = TRUE`,
  `DivergentInputSetNeverMutates` by `DivergeOverwritesRecord = TRUE`
  (`mo-diverge-overwrites-record`), and `MergeAttemptsConverge` by
  `MissingPartReportsConverged = TRUE` (`mo-missing-part-reports-converged`).
- Claims: `StaleOwnerCannotOverwriteNewerClaim` is violated by flipping
  `CompletionOverwrite = TRUE` (`claim-completion-without-cas`). Additionally
  `NoUnconditionalClaimDelete` is violated by `AllowClaimDelete = TRUE`,
  `ClaimGrantsNoPublicationAuthority` by `ClaimIsPublicationAuthority = TRUE`,
  `DivergentInputSetNeverMutates` by `DivergeOverwritesRecord = TRUE`
  (`diverge-overwrites-record`), and `MergeAttemptsConverge` by
  `MissingPartReportsConverged = TRUE` (`missing-part-reports-converged`).

## Per-invariant audit

One row per named invariant or property across both models. Basis is
store-derived (the invariant reads `store`/`ContentOf` directly) or
witness-derived (the invariant reads a variable the model set as a witness to
an action, such as `lastMaint`, `seedFresh`, `lastPub`, `cliCorrect`, or
`stolen`). The TLC line names the run and its exit code; every cited action
and variable below was grepped against the current `.tla` sources before
being carried forward. A VIOLATED run's state/distinct count is not a stable
fact (TLC's parallel BFS stops at the first violating state, and the count
depends on worker count and scheduling; see the Configurations table note
above), so it is not carried into this table -- the invariant name and the
exit code are what a violated row asserts, and both are reproducible.

| Invariant | Model | Basis | Observes | Non-vacuity mutant | TLC line |
|---|---|---|---|---|---|
| OTypeOK | Ownership | store-derived | state well-formedness (every variable's shape and range) | none; a type invariant holds by construction, not by a design choice a mutant can break | checked every run in the Configurations table above; no dedicated mutant |
| CTypeOK | Claims | store-derived | state well-formedness (every variable's shape and range) | none; a type invariant holds by construction, not by a design choice a mutant can break | checked every run in the Configurations table above; no dedicated mutant |
| QueryVisibleDataCorrectUnderDuplicateOwnership | Ownership | store-derived | `ContentOf(RecordKey(u))` equals the first-writer content and every present part key carries its content-addressed bytes | `OwnerPublishOverwrite = TRUE` | `ownership-as-publication-authority`: VIOLATED, exit 12 |
| HeartbeatAndMemoNeverCas | Ownership | witness-derived (`lastMaint.verBefore`/`verAfter`, read from the store at the write, not self-reported) | the last heartbeat or memo write strictly advanced its key's stored version | `HeartbeatMemoUsesCas = TRUE` | `heartbeat-memo-cas`: VIOLATED, exit 12 |
| MemoNeverExtendsFreshnessPastSnapshot | Ownership | witness-derived (`seedFresh`, the value the seed helper actually stored) | the stored clamped freshness never exceeds the source snapshot's own time | `MemoOverstamp = TRUE` | `memo-overstamp`: VIOLATED, exit 12 |
| MergeAttemptsConverge | Ownership | store-derived (`lastPub.winnerPartPresent`, read from the store) | a loser reports `Converged` only when the winner part is observed present | `MissingPartReportsConverged = TRUE` | `mo-missing-part-reports-converged`: VIOLATED, exit 12 |
| DivergentInputSetNeverMutates | Ownership | store-derived (record version delta) | a loser with a divergent input-set hash never advances the record's stored version | `DivergeOverwritesRecord = TRUE` | `mo-diverge-overwrites-record`: VIOLATED, exit 12 |
| EveryEligibleUnitEventuallyAttempted | Ownership | witness-derived (`attemptedByOwner`) | under stable membership every unit is eventually attempted by an in-view owner | `Phantom = TRUE` (a lingering live member outranks every real worker) | `zero-ownership-phantom`: VIOLATED, exit 13 (documented liveness limitation, not a defect) |
| OwnershipIsNotPublicationAuthority | Ownership | witness-derived (`cliCorrect`, an eventuality witness) | under fairness a non-owner (the CLI path) eventually publishes and the data stays correct | none; this is a reachability witness, not a hazard a mutant demonstrates | `MCMaintenanceOwnership.exhaustive.cfg` (`Workers = {1}`): PASS, 13183990 distinct, depth 20; see Configurations table and README.md's worker-count split for why this `PROPERTY` is checked at one worker, not two |
| ClaimGrantsNoPublicationAuthority | Claims | store-derived | same shape as `QueryVisibleDataCorrectUnderDuplicateOwnership`, over the claims model's store | `ClaimIsPublicationAuthority = TRUE` | `claim-as-publication-authority`: VIOLATED, exit 12 |
| StaleOwnerCannotOverwriteNewerClaim | Claims | witness-derived (`lastClaimOp.beforeVer`/`afterVer`/`beforeContent`/`afterContent`, read from the store) | a claim CAS is `Ok` only against the current version; a non-`Ok` CAS changes nothing | `CompletionOverwrite = TRUE` | `claim-completion-without-cas`: VIOLATED, exit 12 |
| NoUnconditionalClaimDelete | Claims | store-derived (absence of a delete on the claim prefix) | no path removes a claim key outside the modeled CAS operations | `AllowClaimDelete = TRUE` | `claim-delete-unconditional`: VIOLATED, exit 12 |
| LostClaimNeverPublishesThroughGuardedPath | Claims | witness-derived (`lastGuarded.fired`/`held`) | the guarded (checkpoint) publish path never fires while the claim is lost | `GuardIgnoresClaim = TRUE` | `guarded-publish-ignores-claim`: VIOLATED, exit 12 |
| MergeAttemptsConverge | Claims | store-derived (`lastPub.winnerPartPresent`, read from the store) | a loser reports `Converged` only when the winner part is observed present | `MissingPartReportsConverged = TRUE` | `missing-part-reports-converged`: VIOLATED, exit 12 |
| DivergentInputSetNeverMutates | Claims | store-derived (record version delta) | a loser with a divergent input-set hash never advances the record's stored version | `DivergeOverwritesRecord = TRUE` | `diverge-overwrites-record`: VIOLATED, exit 12 |
| ExpiredClaimEventuallyStolen | Claims | witness-derived (`stolen`) | an expired claim is eventually stolen under a fair thief and a fair store | none; this is a reachability witness, not a hazard a mutant demonstrates | `MCCompactionClaims.exhaustive.cfg`: PASS, 543 distinct, depth 11; see Configurations table |

## Where the model and the code differ

- The rendezvous weight is a monotone injected table standing in for
  `blake3(unit_key || process_id)`; the model follows the code's determinism
  and totality, not its exact ordering, which no checked property reads.
- The compaction publication plane is abstracted to a present/absent part and
  one terminal record; the conservation count arithmetic and the RLOG merge
  memory bound are out of scope. The model follows the code's CreateIfAbsent
  and content-addressing, which is what the correctness invariants need.
- No disagreement between the normative docs (ADR-0065, ADR-1029, ADR-0048,
  ADR-0979) and the code was found within this task's scope.
