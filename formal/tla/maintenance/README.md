# Maintenance TLA+ models

Two specifications over the shared object-store contract
(`../common/RavelObjectStore.tla`, ADR-1113 D2):

- **MaintenanceOwnership** models the shipped maintenance ownership protocol
  (ADR-0065 decisions 1 to 3, ADR-0048 ownership and coverage).
- **CompactionClaims** models a **proposed design** (ADR-1029) layered on a
  landed CreateIfAbsent/CasVersion claim primitive that nothing in the
  repository calls yet; its first sentence, its module header, and every claim
  in this file describe a proposal, not shipped behaviour.

TLC checked these finite models under the bounds and assumptions recorded in
`results.md`. This model verifies the protocol design; implementation
conformance is argued in the traceability table (`traceability.md`) and asserted
by the named Rust tests, not proved. Safety and liveness are stated separately
below, with the fairness assumptions listed next to every liveness result. The
segment/part encoder, the blake3 hash and work-id, the merge's multiset
preservation, and the object store's own conformance to its contract are
assumptions, stated as such. "Ravel is formally verified" is not a claim this
suite makes.

## Running

```sh
scripts/check-tla.sh smoke -a maintenance         # both smoke configs
scripts/check-tla.sh negative -a maintenance      # every negative control violates
scripts/check-tla.sh traceability -a maintenance  # every Rust ref resolves
scripts/check-tla.sh ci -a maintenance            # smoke + negative + traceability
scripts/check-tla.sh all -a maintenance           # ci, then exhaustive
```

Each `MC*.tla` is an entry module with per-module cfgs
(`MC<Spec>.smoke.cfg`, `MC<Spec>.exhaustive.cfg`). Negative controls live under
`negative/`, each a single flipped constant with a two-line `.expect`.

## MaintenanceOwnership

### What it establishes

TLC checked this finite model under the bounds in `results.md`. Safety
(checked against `MCSpec`, no fairness):

- **QueryVisibleDataCorrectUnderDuplicateOwnership**: whoever publishes a unit's
  record -- an in-view owner, a duplicate owner during a membership transition,
  the ungated CLI, or a paused stale worker -- the terminal record is the single
  CreateIfAbsent winner and every part key carries its content-addressed bytes.
- **HeartbeatAndMemoNeverCas**: heartbeat and memo writes are Overwrite on
  self-owned keys, never CAS.
- **MemoNeverExtendsFreshnessPastSnapshot**: seeding clamps each entry to its
  source snapshot's time, so an in-memory entry never reads fresher than the
  snapshot it came from.
- **MergeAttemptsConverge**: fail-closed convergence (ADR-1113 D3). The publish
  outcome witness is drawn from the alphabet that mirrors
  `crates/ravel-maintain/src/publish.rs::resolve_already_exists`
  (`Published`, `Converged`, `Abandoned`, `ConvergedWinnerPartMissing`,
  `InputSetHashDivergence`). A loser reports `Converged` only when it observes
  the winner's part present in the store (or re-PUTs the identical
  content-addressed part and then observes it present); when the winning part has
  vanished and cannot be re-PUT it reports `ConvergedWinnerPartMissing`, never a
  bare `Converged`. The invariant reads the store-derived witness
  (`lastPub.winnerPartPresent`, set from whether the part key is present after
  the publish attempt), not a self-reported success flag.
- **DivergentInputSetNeverMutates**: when a loser observes a record whose
  input-set hash differs from its own (`Variants`, a divergent listing yielding a
  different `input_set_hash`), it fails closed with outcome
  `InputSetHashDivergence` and never deletes or overwrites the existing record.
  The invariant reads a store-derived witness (`lastPub.recOverwritten`, set from
  whether the record object's version changed across the attempt), not the
  action's own claim.

Liveness (checked against `MCFairSpec`; fairness: weak fairness on each worker's
live-set recompute, on the part PUT, on some in-view owner's record attempt per
unit, and on the CLI publish path):

- **EveryEligibleUnitEventuallyAttempted**: under stable membership (no crash, no
  phantom) every unit is eventually attempted by its in-view owner.
- **OwnershipIsNotPublicationAuthority**: encoded as an eventuality witness --
  under fairness on the ungated CLI path a non-owner eventually executes the
  publication path, and because the data-correctness invariant is conjoined the
  witnessed state has correct data. This is how the model shows a non-owner
  publish is reachable and stays correct.

### The double-ownership window

The model does **not** assert continuous single ownership, and none is claimed.
During a membership transition two workers may both believe they own a unit for
up to `3H + H + cycle`: `3H` for a stale sibling to age out of the liveness
window (`Factor * H`, `Factor` = `DEFAULT_LIVENESS_FACTOR` = 3), one more
heartbeat `H` for the survivor to notice, and the length of a discovery cycle
(unbounded: the cycle snapshots the live set once and threads it through every
owned unit). Correctness holds across this window because publication is
CreateIfAbsent over content-addressed parts and never reads ownership --
`QueryVisibleDataCorrectUnderDuplicateOwnership` is exactly this claim.

### Fail-closed convergence and the vanishing part

The publish model carries the store actions that let a winning part disappear
between the record write and a loser's convergence check: `VanishPart` deletes a
part object, and `TombstonePart` marks a part key non-re-PUTtable (the
content-addressed key was tombstoned, so a `CreateIfAbsent` re-PUT is refused).
`DoPublish` branches over the observed store state, not over any intent:

- record absent and the winning part present: `Published`.
- record present with a matching input-set hash and the winning part present:
  `Converged`.
- record present, winning part absent but re-PUTtable: the loser re-PUTs the
  identical part and converges (`Converged`, `winnerPartPresent` observed true).
- record present, winning part absent and tombstoned (not re-PUTtable):
  `ConvergedWinnerPartMissing`. The earlier `MergeAttemptsConverge` held only
  because no part ever vanished; with `VanishPart`/`TombstonePart` present the
  invariant now states the real contract, that a vanished, non-re-PUTtable
  winning part yields `ConvergedWinnerPartMissing` and never a bare `Converged`.
  This mirrors
  `crates/ravel-maintain/tests/tombstone_race.rs::rerun_with_revanished_part_fails_typed_not_converged`.
- record present with a divergent input-set hash: `InputSetHashDivergence`, store
  unchanged.

Coverage note: with two in-view owners publishing different input sets for the
same unit, `QueryVisibleDataCorrectUnderDuplicateOwnership` still holds. The
record is the single `CreateIfAbsent` winner regardless of publisher, and the
loser's divergent-hash observation fails closed rather than overwriting, so the
terminal record and its content-addressed parts stay consistent.

### The live-set fail-open lives in the caller

The `ComputeLive` step models the once-per-cycle live-set recompute. The
fail-open on a read error (a failed refresh keeps the previous set) is **not** in
this computation: it is the caller's, in
`services/ravel-server/src/maintain.rs::run_loop`, whose discovery arm reuses the
last set the heartbeat watch channel published
(`live_rx.borrow().clone()`). The model captures that as the *absence* of a
`ComputeLive` step: the worker keeps its frozen `cachedLive`. The `scrub.rs`
read-fault fallback is a separate caller and is out of scope for this model.

### The zero-ownership limitation

`EveryEligibleUnitEventuallyAttempted` is intentionally **false** when a phantom
member is present (the `zero-ownership-phantom` negative, `Phantom = TRUE`). The
phantom stands for a lingering heartbeat key of a departed or restarted worker
(a fresh process id leaves the old key in place, and it is never deleted): if
that key is within the liveness window and its rendezvous weight outranks every
live worker, every live worker defers to an owner no process embodies, and the
unit is never attempted by discovery. This is the ADR-0065 asymmetric-view
limitation: membership is process-level liveness, zero ownership under
asymmetric views is possible and undetected, and correctness never depends on
it (the ungated CLI path still publishes correctly). No invariant was weakened
to make a run pass; the limitation is recorded, not hidden.

### Modeled state versus Ravel

| Model | Ravel |
|---|---|
| `store[<<"rec", u>>]` | the terminal compaction record object for unit `u` |
| `store[<<"part", u, v>>]` | a content-addressed part object of variant `v` |
| `now`, `hbStamp[w]` | the shared logical clock and each worker's last `heartbeat_unix_ns` |
| `crashed[w]` | a worker process that has stopped heartbeating |
| `cachedLive[w]` | the live set snapshotted once at the head of a discovery cycle |
| `Owner(u, S)` | `owner(unit_key, live_set)`, the rendezvous argmax |
| `memoSnap[w]` | `sys/maintain/memo/<id>`, the per-worker maintain snapshot |
| `firstRecord[u]` | witness: the content of the CreateIfAbsent winner (immutable) |
| `Variants` | a divergent listing seeing a different input set / `input_set_hash` |

Time is a single bounded logical clock; clock skew between workers is not
modeled here, because the checked safety properties are view-independent and the
checked liveness property assumes stable membership. The one adversarial view
the protocol permits, a phantom owner, is the `Phantom` switch, not a skew
parameter.

## CompactionClaims

CompactionClaims is a proposed design (ADR-1029) over a landed primitive: the
claim key and its CreateIfAbsent/CasVersion operations exist in
`crates/ravel-fleet/src/claim.rs`, but no caller wires them into the compaction
pipeline, so this model checks the design, not shipped behaviour.

### What it establishes

TLC checked this finite model under the bounds in `results.md`. Safety (against
`MCSpec`):

- **ClaimGrantsNoPublicationAuthority**: publication correctness never reads the
  claim; the record stays the single CreateIfAbsent winner under every claim
  state (held, lost, expired early, corrupt, duplicated).
- **StaleOwnerCannotOverwriteNewerClaim**: a claim CAS succeeds only against the
  current version; a stale-version write is a no-op.
- **NoUnconditionalClaimDelete**: no path deletes a claim key.
- **AtMostOneThiefWinsAVersion**: the version token is consumed by the first
  steal, so a second steal on the same observed version fails.
- **LostClaimNeverPublishesThroughGuardedPath**: the cancellation-checkpoint
  path abandons once the claim is lost; the ungated path may still publish and
  the data stays correct.
- **MergeAttemptsConverge**: fail-closed convergence (ADR-1113 D3). The outcome
  witness is the alphabet mirroring
  `crates/ravel-maintain/src/publish.rs::resolve_already_exists`. A loser reports
  `Converged` only when it observes the winner's part present (or re-PUTs the
  identical content-addressed part and observes it present); a vanished,
  non-re-PUTtable winning part yields `ConvergedWinnerPartMissing`, never a bare
  `Converged`. The `VanishPart`/`TombstonePart` actions make the vanishing case
  reachable; the invariant reads the store-derived `winnerPartPresent` witness,
  mirroring
  `crates/ravel-maintain/tests/tombstone_race.rs::rerun_with_revanished_part_fails_typed_not_converged`.
- **DivergentInputSetNeverMutates**: a loser observing a record whose input-set
  hash differs from its own fails closed with outcome `InputSetHashDivergence`
  and never deletes or overwrites the record; the invariant reads the
  store-derived `recOverwritten` witness, not the action's own claim.

Liveness (against `MCFairSpec`; fairness: weak fairness on time passing, on the
holder's acquire, and on the thief's observe and steal; environment: a paused
holder, a fair thief, a fair store, and advancing time):

- **ExpiredClaimEventuallyStolen**: an expired claim is eventually stolen.

### The advisory expiry clock

Expiry is read from the store's advisory `last_modified`
(`ClaimExpiryReadLastModifiedAdvisory`) plus the holder's declared lease, capped
at `MAX_OBSERVED_LEASE`. Time is modeled as the store's own monotonic version
domain -- the same source `last_modified` lives in -- advanced by a `TimePass`
action. This makes the ADR-1029 property explicit that node clocks never enter
the correctness decision: every safety invariant here is independent of the
expiry clock, which only gates *when* a steal is permitted. Early or late
stealing is advisory and safe, merely wasteful.

### Modeled state versus Ravel

| Model | Ravel |
|---|---|
| `store[<<"claim", u>>]` | `sys/maintain/claims/compaction/<work_id>` |
| claim content `<<"c", owner, state>>` | the claim payload `{owner_process_id, state, ...}` |
| `heldVer[w][u]` | the `Version` token from a worker's last successful claim PUT |
| `obsVer[w][u]` | the version a worker read via one GET plus one HEAD |
| `Corrupt` | an unreadable claim payload (never stolen) |
| `versionCounter` (via `TimePass`) | logical time in the `last_modified` domain |
| guarded / ungated publish | the ClaimGuard checkpoint path / the `--no-claim` CLI path |

## Assumptions and out of scope

Assumed (stated, not proved): blake3 as a deterministic total order and the
work-id identity; the part encoder's content addressing (a key determines its
bytes); the merge's multiset preservation; and the object store's own
conformance to its contract. Out of scope: the RLOG k-way merge memory bound
(ADR-0065 decision 4, ADR-0979), the orphan breaker and conservation-count
arithmetic (ADR-0048 decisions 4 and 5), the cost gate (`claim_min_input_bytes`),
renewal cadence timing, and jitter scheduling. UUID churn on restart is modeled
only through its one load-bearing consequence, the phantom member.
