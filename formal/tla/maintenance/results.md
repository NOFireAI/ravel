# Maintenance results

TLC checked these finite models under the bounds and assumptions recorded here.
This verifies the protocol designs; implementation conformance is argued in
`traceability.md` and asserted by the named Rust tests, not proved.

- Tool: `tla2tools` 1.7.4 (TLC2 version 2.19), Java 21 (Temurin).
- Host: fleet executor (4 cores, 8 GB).
- Run id: `20260903T055341Z-c5e6a6bb3a38baef0617a8b6f3cbd3ae4b5f9ff5`
  (`scripts/check-tla.sh ci -a maintenance`, one coherent run: smoke, negative,
  traceability).
- Smoke and negative figures copied verbatim from `.cache/tla/last-run.tsv` for
  that run id. The exhaustive configs were not run by this executor; their rows
  carry no re-measured figures.

## Configurations

| cfg | TLC | states | distinct | depth | seconds | host | result |
|---|---|---|---|---|---|---|---|
| MCMaintenanceOwnership.smoke.cfg | 1.7.4 | 23945345 | 1837440 | 21 | 41 | fleet executor | PASS |
| MCMaintenanceOwnership.exhaustive.cfg | 1.7.4 | - | - | - | - | - | not run by executor |
| MCCompactionClaims.smoke.cfg | 1.7.4 | 58416976 | 9574631 | 17 | 140 | fleet executor | PASS |
| MCCompactionClaims.exhaustive.cfg | 1.7.4 | - | - | - | - | - | not run by executor |
| negative/ownership-as-publication-authority.cfg | 1.7.4 | 9362 | 2998 | - | 2 | fleet executor | VIOLATED |
| negative/heartbeat-memo-cas.cfg | 1.7.4 | 115 | 100 | - | 1 | fleet executor | VIOLATED |
| negative/memo-overstamp.cfg | 1.7.4 | 1510 | 560 | - | 1 | fleet executor | VIOLATED |
| negative/mo-diverge-overwrites-record.cfg | 1.7.4 | 1097 | 531 | - | 1 | fleet executor | VIOLATED |
| negative/mo-missing-part-reports-converged.cfg | 1.7.4 | 13362 | 4200 | - | 1 | fleet executor | VIOLATED |
| negative/zero-ownership-phantom.cfg | 1.7.4 | 8433521 | 1607346 | - | 64 | fleet executor | VIOLATED |
| negative/claim-completion-without-cas.cfg | 1.7.4 | 26776 | 11849 | - | 2 | fleet executor | VIOLATED |
| negative/claim-delete-unconditional.cfg | 1.7.4 | 109 | 75 | - | 1 | fleet executor | VIOLATED |
| negative/claim-as-publication-authority.cfg | 1.7.4 | 9601 | 4802 | - | 2 | fleet executor | VIOLATED |
| negative/steal-ignores-cas.cfg | 1.7.4 | 18817 | 8767 | - | 2 | fleet executor | VIOLATED |
| negative/guarded-publish-ignores-claim.cfg | 1.7.4 | 233 | 148 | - | 1 | fleet executor | VIOLATED |
| negative/diverge-overwrites-record.cfg | 1.7.4 | 999 | 565 | - | 1 | fleet executor | VIOLATED |
| negative/missing-part-reports-converged.cfg | 1.7.4 | 8354 | 4204 | - | 2 | fleet executor | VIOLATED |

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
  property names). Safety is view-independent, so it is checked in smoke with
  `AllowCrash = TRUE` and richer membership churn; liveness needs the stable
  environment.
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
  `claim-as-publication-authority`, `steal-ignores-cas`,
  `guarded-publish-ignores-claim` (safety): the ADR-1029 hazards the design
  forbids. Classification: **design demonstration**; the proposed design uses
  CasVersion, never deletes, treats no claim as publication authority, consumes a
  version token at most once, and abandons the guarded path on a lost claim.
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
