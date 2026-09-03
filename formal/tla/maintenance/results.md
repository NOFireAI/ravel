# Maintenance results

TLC checked these finite models under the bounds and assumptions recorded here.
This verifies the protocol designs; implementation conformance is argued in
`traceability.md` and asserted by the named Rust tests, not proved.

- Tool: `tla2tools` 1.7.4 (TLC2 version 2.19), Java 21 (Temurin).
- Host: `rp2` (4 cores).
- Run id: `20260903T023102Z-0d8d5d780b5aa9d3bb58f5a1f283d28cfba3ae44`
  (`scripts/check-tla.sh all -a maintenance`, one coherent run).
- Figures copied verbatim from `.cache/tla/last-run.tsv` for that run id.

## Configurations

| cfg | TLC | states | distinct | depth | seconds | host | result |
|---|---|---|---|---|---|---|---|
| MCMaintenanceOwnership.smoke.cfg | 1.7.4 | 15955521 | 1247232 | 20 | 36 | rp2 | PASS |
| MCMaintenanceOwnership.exhaustive.cfg | 1.7.4 | 3649907 | 231072 | 17 | 77 | rp2 | PASS |
| MCCompactionClaims.smoke.cfg | 1.7.4 | 1696352 | 393257 | 13 | 10 | rp2 | PASS |
| MCCompactionClaims.exhaustive.cfg | 1.7.4 | 1972 | 543 | 10 | 1 | rp2 | PASS |
| negative/ownership-as-publication-authority.cfg | 1.7.4 | 5948 | 1582 | - | 2 | rp2 | VIOLATED |
| negative/heartbeat-memo-cas.cfg | 1.7.4 | 82 | 55 | - | 1 | rp2 | VIOLATED |
| negative/memo-overstamp.cfg | 1.7.4 | 530 | 226 | - | 1 | rp2 | VIOLATED |
| negative/zero-ownership-phantom.cfg | 1.7.4 | 121747 | 21573 | - | 4 | rp2 | VIOLATED |
| negative/claim-completion-without-cas.cfg | 1.7.4 | 14612 | 6044 | - | 2 | rp2 | VIOLATED |
| negative/claim-delete-unconditional.cfg | 1.7.4 | 55 | 39 | - | 2 | rp2 | VIOLATED |
| negative/claim-as-publication-authority.cfg | 1.7.4 | 7700 | 3453 | - | 1 | rp2 | VIOLATED |

The four PASS configurations are banded in `bands.tsv`; the negative controls
stop at the first counterexample and carry no band. Every smoke config completes
well under the 120 s ceiling and every exhaustive config well under 30 minutes.

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
  `claim-as-publication-authority` (safety): the three ADR-1029 hazards the
  design forbids. Classification: **design demonstration**; the proposed design
  uses CasVersion, never deletes, and never treats a claim as publication
  authority.

## Non-vacuity of the named invariants

Each specification has at least one invariant shown non-vacuous by a negative
control that flips exactly one constant:

- Ownership: `QueryVisibleDataCorrectUnderDuplicateOwnership` is violated by
  flipping `OwnerPublishOverwrite = TRUE`
  (`ownership-as-publication-authority`). Additionally
  `HeartbeatAndMemoNeverCas` is violated by `HeartbeatMemoUsesCas = TRUE`
  and `MemoNeverExtendsFreshnessPastSnapshot` by `MemoOverstamp = TRUE`.
- Claims: `StaleOwnerCannotOverwriteNewerClaim` is violated by flipping
  `CompletionOverwrite = TRUE` (`claim-completion-without-cas`). Additionally
  `NoUnconditionalClaimDelete` is violated by `AllowClaimDelete = TRUE` and
  `ClaimGrantsNoPublicationAuthority` by `ClaimIsPublicationAuthority = TRUE`.

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
