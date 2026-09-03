# Maintenance results

TLC checked these finite models under the bounds and assumptions recorded here.
This verifies the protocol designs; implementation conformance is argued in
`traceability.md` and asserted by the named Rust tests, not proved.

- Tool: `tla2tools` 1.7.4 (TLC2 version 2.19), Java 21 (Temurin).
- Host: fleet executor (4 cores, 8 GB).
- Run id: `20260903T091233Z-e23694e3abbf2bdfc8391906ca52a7ffad7e9b66`
  (`scripts/check-tla.sh ci -a maintenance`, one coherent run: smoke, negative,
  traceability).
- Smoke and negative figures copied verbatim from `.cache/tla/last-run.tsv` for
  that run id. The exhaustive configs were not run by this executor; their rows
  carry no re-measured figures.

## Configurations

| cfg | TLC | states | distinct | depth | seconds | host | result |
|---|---|---|---|---|---|---|---|
| MCMaintenanceOwnership.smoke.cfg | 1.7.4 | 44219729 | 2773760 | 20 | 91 | fleet executor | PASS |
| MCMaintenanceOwnership.exhaustive.cfg | 1.7.4 | - | - | - | - | - | not run by executor |
| MCCompactionClaims.smoke.cfg | 1.7.4 | 57428832 | 9370767 | 17 | 184 | fleet executor | PASS |
| MCCompactionClaims.exhaustive.cfg | 1.7.4 | - | - | - | - | - | not run by executor |
| negative/ownership-as-publication-authority.cfg | 1.7.4 | 7550 | 2435 | - | 1 | fleet executor | VIOLATED |
| negative/heartbeat-memo-cas.cfg | 1.7.4 | 116 | 91 | - | 1 | fleet executor | VIOLATED |
| negative/memo-overstamp.cfg | 1.7.4 | 1167 | 488 | - | 1 | fleet executor | VIOLATED |
| negative/mo-diverge-overwrites-record.cfg | 1.7.4 | 2865 | 1074 | - | 1 | fleet executor | VIOLATED |
| negative/mo-missing-part-reports-converged.cfg | 1.7.4 | 8096 | 2405 | - | 2 | fleet executor | VIOLATED |
| negative/zero-ownership-phantom.cfg | 1.7.4 | 6350395 | 1317024 | - | 65 | fleet executor | VIOLATED |
| negative/claim-completion-without-cas.cfg | 1.7.4 | 20850 | 9254 | - | 1 | fleet executor | VIOLATED |
| negative/claim-delete-unconditional.cfg | 1.7.4 | 124 | 83 | - | 1 | fleet executor | VIOLATED |
| negative/claim-as-publication-authority.cfg | 1.7.4 | 11577 | 5906 | - | 2 | fleet executor | VIOLATED |
| negative/guarded-publish-ignores-claim.cfg | 1.7.4 | 267 | 180 | - | 1 | fleet executor | VIOLATED |
| negative/diverge-overwrites-record.cfg | 1.7.4 | 1810 | 973 | - | 2 | fleet executor | VIOLATED |
| negative/missing-part-reports-converged.cfg | 1.7.4 | 5952 | 3004 | - | 2 | fleet executor | VIOLATED |

The two smoke configurations and the two exhaustive configurations are banded in
`bands.tsv`; the smoke bands were re-measured this run, the exhaustive bands are
carried from the prior full run and were not re-run here. The negative controls
stop at the first counterexample and carry no band. Every smoke config completes
under the 300 s smoke budget. The `distinct` counts are deterministic; the
reported search `depth` varies by a step or two between runs under TLC's parallel
BFS (`-workers auto`), so the depth bands are a small range around the observed
value rather than an exact figure (the figures in the table above are from the
named run id).

## Constants chosen for exhaustive

- Ownership: `Workers = {1, 2}`, `Units = {1}`, `Variants = {iA, iB}`,
  `H = 1`, `Factor = 1` (window 1), `MaxT = 2`, `Phantom = FALSE`,
  `AllowCrash = FALSE` (the stable-membership environment the liveness
  property names). Safety is view-independent, so it is checked in smoke at the
  same `MaxT = 2` with `AllowCrash = FALSE`. At `MaxT = 2` a worker that stops
  writing heartbeats has its stamp fall outside the staleness window and is
  excluded from the live set, so the safety-relevant crash behaviour (a silent
  worker being dropped) is reachable in smoke without the crashed flag. See the
  staleness reachability and crash-coverage notes below.
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
- Exhaustive termination via bounding (F2). The exhaustive config runs with no
  VIEW, so the raw `versionCounter` (bumped by every successful store write, an
  unbounded `Nat`) must stay finite for the search to terminate. The only action
  that could re-mint versions without limit is a part vanish followed by a
  re-PUT of the identical content-addressed bytes. `VanishPart(u)` is bounded by
  a `vanishedOnce[u][variant]` latch, so each part vanishes at most once and the
  vanish/re-PUT loop cannot drive `versionCounter` forever. A minimal scratch
  model (never committed) with the auxiliary invariant `VCUnderCap ==
  versionCounter =< 12` demonstrates the bound: with the latch guard the search
  completes (263268 distinct, depth 16) and `VCUnderCap` holds; with the guard
  removed the search violates the cap (exit 12) and the reported depth grows
  without settling. The full exhaustive run is not executed by this executor
  (its budget exceeds the streaming idle timeout); the probe shows the bounding
  change is what makes it finite.
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
