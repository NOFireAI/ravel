# Maintenance traceability

One row per TLA+ action and per named property, across both specifications.
The third column is the Rust transition it pins; the meaning column records
whether Rust performs the transition atomically, or the model assumes a helper
or the backend does. Symbols only, never line numbers.

## MaintenanceOwnership (shipped)

| TLA+ action or property | meaning | Rust path and symbol | existing test | new test needed |
|---|---|---|---|---|
| WriteHeartbeat | one self-owned Overwrite PUT stamped with the writer clock; atomic in Rust | `crates/ravel-fleet/src/worker_set.rs::WorkerSet::write_heartbeat` | `crates/ravel-fleet/src/worker_set.rs::live_sets_converge_to_include_both_workers` | none |
| ComputeLive | the once-per-cycle live-set recompute; Rust does a LIST plus one GET per sibling, not atomic, a helper the model collapses to one step. The fail-open freeze (a failed refresh keeps the last set) is the caller's, in `services/ravel-server/src/maintain.rs::run_loop` via `live_rx.borrow().clone()`, modeled as the absence of this step | `crates/ravel-fleet/src/worker_set.rs::live_set` | `crates/ravel-fleet/src/worker_set.rs::stale_sibling_is_excluded_at_three_h` | none |
| Tick | logical clock advance; no atomic Rust transition, the backend clock is_stale compares against | `crates/ravel-fleet/src/worker_set.rs::is_stale` | `crates/ravel-fleet/src/worker_set.rs::future_dated_sibling_never_wins_ownership` | none |
| Crash | a worker stops heartbeating and its stamp goes stale; no atomic Rust transition | `crates/ravel-fleet/src/worker_set.rs::is_stale` | `crates/ravel-fleet/src/worker_set.rs::dropped_worker_units_move_to_survivor` | none |
| Revive | a fresh incarnation re-heartbeats; the old key lingers (abstracted); Rust builds a new set | `crates/ravel-fleet/src/worker_set.rs::WorkerSet::new` | none | none |
| PutPart | one content-addressed CreateIfAbsent part PUT; atomic in Rust | `crates/ravel-maintain/src/build.rs::put_part` | `crates/ravel-maintain/tests/determinism.rs::same_inputs_same_bytes_and_keys` | none |
| WorkerRecord | an in-view owner attempts a unit; ownership gates the attempt, publish is a separate CreateIfAbsent; not atomic across the two | `services/ravel-server/src/maintain.rs::run_tick_with_clock` | `services/ravel-server/src/maintain.rs::two_replicas_partition_units_without_double_pay` | none |
| CliRecord | the ungated CLI compaction with no heartbeat and no ownership; atomic per publish | `services/ravel-cli/src/maintain.rs::compact_bucket` | none | a CLI-publishes-without-ownership test |
| WriteMemo | one self-owned Overwrite of the memo snapshot; atomic in Rust | `crates/ravel-maintain/src/memo_snapshot.rs::write_memo_snapshot` | `services/ravel-server/src/maintain.rs::warm_start_seeds_successor_from_predecessor_snapshot_through_store` | none |
| FutureEntry | a snapshot entry stamped after its own snapshot time; no atomic Rust transition, the case the seed clamp defends | `crates/ravel-maintain/src/scan.rs::seed_from_snapshot` | none | none |
| CorruptMemo | a snapshot decode failure treated as absent; the backend produces it | `crates/ravel-maintain/src/scan.rs::MemoSnapshotError` | none | none |
| SeedMemo | seed in-memory freshness from all valid snapshots with the per-entry clamp; a helper over many GETs | `crates/ravel-maintain/src/scan.rs::MaintainMemo::seed_from_snapshot` | `services/ravel-server/src/maintain.rs::warm_start_seeds_successor_from_predecessor_snapshot_through_store` | none |
| QueryVisibleDataCorrectUnderDuplicateOwnership | the record is the single CreateIfAbsent winner and parts are content addressed, regardless of publisher | `crates/ravel-maintain/src/publish.rs::publish_record_with_conservation` | `crates/ravel-maintain/tests/tombstone_race.rs::rerun_after_vanished_part_converges_by_presence` | none |
| HeartbeatAndMemoNeverCas | heartbeat and memo are Overwrite, never CAS; two self-owned single PUTs (memo path in `crates/ravel-maintain/src/memo_snapshot.rs::write_memo_snapshot`) | `crates/ravel-fleet/src/worker_set.rs::WorkerSet::write_heartbeat` | none | a test asserting both PUTs use PutMode::Overwrite, not a CAS put-mode |
| MemoNeverExtendsFreshnessPastSnapshot | the seed clamp caps each entry at its snapshot time; local to the seed helper | `crates/ravel-maintain/src/scan.rs::seed_from_snapshot` | none | none |
| EveryEligibleUnitEventuallyAttempted | under stable membership every unit gets an in-view owner that attempts it; the discovery cycle threads one live set | `services/ravel-server/src/maintain.rs::run_discovery_cycle` | `services/ravel-server/src/maintain.rs::two_replicas_partition_units_without_double_pay` | none |
| OwnershipIsNotPublicationAuthority | the publish path never reads ownership; a non-owner publish stays correct | `crates/ravel-maintain/src/publish.rs::publish_record_with_conservation` | `crates/ravel-maintain/tests/tombstone_race.rs::rerun_after_vanished_part_converges_by_presence` | none |

## CompactionClaims (proposed design over a landed primitive)

| TLA+ action or property | meaning | Rust path and symbol | existing test | new test needed |
|---|---|---|---|---|
| Acquire | claim CreateIfAbsent returning a version token; atomic single PUT | `crates/ravel-fleet/src/claim.rs::acquire` | `crates/ravel-fleet/src/claim.rs::uncontended_acquire_costs_one_put_and_no_reads` | none |
| Observe | one GET plus one HEAD recording the observed version; a helper, not atomic | `crates/ravel-fleet/src/claim.rs::observe` | `crates/ravel-fleet/src/claim.rs::a_corrupt_claim_payload_is_observed_and_never_stolen` | none |
| Renew | CasVersion on the held token; PreconditionFailed is ClaimLost; atomic | `crates/ravel-fleet/src/claim.rs::renew` | `crates/ravel-fleet/src/claim.rs::renew_after_steal_fails_precondition` | none |
| Steal | CasVersion on the observed version, gated on advisory expiry; atomic | `crates/ravel-fleet/src/claim.rs::steal` | `crates/ravel-fleet/src/claim.rs::steal_requires_matching_version` | none |
| MarkCompleted | CasVersion setting state done; PreconditionFailed is NotOwner; atomic | `crates/ravel-fleet/src/claim.rs::mark_completed` | `crates/ravel-fleet/src/claim.rs::completed_mark_is_cas_guarded` | none |
| CorruptClaim | an unreadable payload; the backend produces it, never stolen after | `crates/ravel-fleet/src/claim.rs::observe` | `crates/ravel-fleet/src/claim.rs::a_corrupt_claim_payload_is_observed_and_never_stolen` | none |
| TimePass | advisory expiry clock advance; no atomic Rust transition, the store last_modified domain the observer compares against | `crates/ravel-fleet/src/claim.rs::MAX_OBSERVED_LEASE_MS` | none | none |
| PutPart | one content-addressed CreateIfAbsent part PUT; atomic | `crates/ravel-maintain/src/build.rs::put_part` | `crates/ravel-maintain/tests/determinism.rs::same_inputs_same_bytes_and_keys` | none |
| GuardedPublish | the cancellation-checkpoint publish that abandons on a lost claim; the ClaimGuard is proposed, no shipped caller | `crates/ravel-fleet/src/claim.rs::renew` | none | a ClaimGuard-abandons-on-lost-claim test |
| UngatedPublish | the --no-claim CLI path and a paused stale worker that ignores checkpoints; atomic per publish | `services/ravel-cli/src/maintain.rs::compact_bucket` | none | a claim-participation CLI test |
| ClaimGrantsNoPublicationAuthority | publication correctness never reads the claim | `crates/ravel-maintain/src/publish.rs::publish_record_with_conservation` | `crates/ravel-maintain/tests/tombstone_race.rs::rerun_after_vanished_part_converges_by_presence` | none |
| StaleOwnerCannotOverwriteNewerClaim | a claim CAS is Ok only against the current version, a non-Ok CAS changes nothing | `crates/ravel-fleet/src/claim.rs::renew` | `crates/ravel-fleet/src/claim.rs::renew_after_steal_fails_precondition` | none |
| NoUnconditionalClaimDelete | no delete operation exists on the claim prefix | `crates/ravel-fleet/src/claim.rs::COMPACTION_CLAIMS_PREFIX` | none | none |
| AtMostOneThiefWinsAVersion | the version token is consumed by the first steal, so a second steal on the same observed version fails | `crates/ravel-fleet/src/claim.rs::steal` | `crates/ravel-fleet/src/claim.rs::steal_requires_matching_version` | none |
| LostClaimNeverPublishesThroughGuardedPath | the checkpoint path abandons once renewal fails; the guard is proposed | `crates/ravel-fleet/src/claim.rs::renew` | none | a ClaimGuard-abandons-on-lost-claim test |
| MergeAttemptsConverge | a present record's winning part is present, so a later attempt finds or re-PUTs identical parts | `crates/ravel-maintain/src/publish.rs::resolve_already_exists` | `crates/ravel-maintain/tests/tombstone_race.rs::rerun_with_revanished_part_fails_typed_not_converged` | none |
| ExpiredClaimEventuallyStolen | an expired claim is eventually stolen under a fair thief and a fair store | `crates/ravel-fleet/src/claim.rs::steal` | `crates/ravel-fleet/src/claim.rs::steal_before_expiry_is_refused` | none |
