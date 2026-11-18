//! The compatibility matrix (ADR-4233 decision 9 and decision 5's rollback
//! table): which test pins each row.
//!
//! The tests in this file run the library write paths against a `MemoryStore`
//! with two-key listing pages. Rows another crate already pins are cited by
//! `file::test`, not duplicated. `every_cited_test_exists` below fails when a
//! cited name is not a test function in the named file.
//!
//! Status words: `pinned` means the cited tests assert the row's outcome;
//! `partial` means they assert part of it and the rest has no test; `deferred`
//! means no test in the tree asserts the row. The reader window is fixed at
//! compile time (`SUPPORTED_VERSIONS`), and this crate builds the segment crates
//! with `format-drill`, so an N-only reader cannot run here: its refusal is
//! cited from the default-feature `ravel-segment` tests.
//!
//! Row-to-test map:
//!
//! - mixed nodes N-only and N+1, before activation; rollback "B+1 installed, F
//!   not activated, roll back to B" [pinned]
//!   - crates/ravel-compat-tests/tests/matrix.rs::mixed_nodes_before_activation_both_write_n
//!   - crates/ravel-segment/src/format.rs::todays_window_accepts_only_the_current_version
//!   - crates/ravel-segment/tests/reader_version_window.rs::a_version_inside_the_window_reads_normally
//! - mixed nodes, after activation; rollback "F activated and N+1 objects exist,
//!   roll back to B" [pinned]
//!   - crates/ravel-compat-tests/tests/matrix.rs::n_plus_one_node_writes_active_after_activation
//!   - crates/ravel-compat-tests/tests/matrix.rs::n_only_node_refuses_the_family_after_activation
//!   - crates/ravel-maintain/tests/compat_write_policy.rs::compaction_refuses_an_unwritable_active_version_before_any_claim_or_put
//!   - crates/ravel-ingest/tests/compat_policy.rs::a_log_write_under_an_rlog_version_this_build_cannot_write_is_refused_at_admission
//!   - crates/ravel-segment/tests/reader_version_window.rs::a_version_outside_the_window_fails_with_unsupported_version
//! - reader window and fail-closed out-of-window read (decision 4) [pinned]
//!   - crates/ravel-segment/tests/reader_version_window.rs::a_version_inside_the_window_reads_normally
//!   - crates/ravel-segment/tests/reader_version_window.rs::a_version_outside_the_window_fails_with_unsupported_version
//!   - crates/ravel-segment/tests/reader_version_window.rs::the_trailer_gate_and_the_validator_admit_the_same_versions
//!   - crates/ravel-segment/src/format.rs::drill_window_is_the_drill_version_and_v7
//!   - crates/ravel-logseg/src/footer.rs::rejects_newer_trailer_as_unsupported_version
//!   - crates/ravel-rspan/src/footer.rs::rejects_newer_trailer_as_unsupported_version
//! - stale `sys/compat` view: the stale writer stops past H [pinned]
//!   - crates/ravel-compat-tests/tests/matrix.rs::a_stale_view_refuses_to_publish_past_the_horizon
//! - `sys/compat` deleted after activation: publishes refused, init refuses
//!   [pinned]
//!   - crates/ravel-compat-tests/tests/matrix.rs::absent_compat_with_a_fenced_bucket_refuses_record_missing
//!   - crates/ravel-compat-tests/tests/matrix.rs::init_refuses_a_fenced_store_missing_compat
//! - a newer peer's `sys/compat` is refused, never overwritten [pinned]
//!   - crates/ravel-catalog/src/compat.rs::newer_compat_record_is_typed_and_never_overwritten
//! - activation preconditions: init first, one version up, attestation; the
//!   fence walk completes [pinned]
//!   - crates/ravel-compat-tests/tests/matrix.rs::activate_refuses_without_init
//!   - crates/ravel-compat-tests/tests/matrix.rs::activate_refuses_a_target_not_above_active
//!   - crates/ravel-compat-tests/tests/matrix.rs::activate_refuses_a_version_jump
//!   - crates/ravel-compat-tests/tests/matrix.rs::activate_refuses_without_the_attestation
//!   - crates/ravel-compat-tests/tests/matrix.rs::activate_completes_the_fence_walk
//! - `compat init` over a clean store, idempotent, incomplete listing [pinned]
//!   - crates/ravel-compat-tests/tests/matrix.rs::init_adopts_a_clean_store
//!   - crates/ravel-compat-tests/tests/matrix.rs::init_is_idempotent
//!   - crates/ravel-compat-tests/tests/matrix.rs::init_refuses_on_an_incomplete_listing
//! - `compat init` over an object outside the read set [pinned]
//!   - crates/ravel-maintain/src/compat_init.rs::init_refuses_an_out_of_window_object
//!   - crates/ravel-maintain/src/compat_init.rs::init_refuses_an_out_of_window_commit_record_stamp
//!   - crates/ravel-maintain/src/compat_init/census_tests.rs::every_census_kind_adopts_in_window_and_refuses_out_of_window
//! - provisioning fence: born fenced, v4 and v3 records through reshard and
//!   floor raise, a record above 4 refused and left unchanged [pinned]
//!   - crates/ravel-compat-tests/tests/matrix.rs::a_tenant_onboarded_after_activation_is_born_fenced
//!   - crates/ravel-compat-tests/tests/matrix.rs::a_fenced_v4_record_survives_a_reshard
//!   - crates/ravel-compat-tests/tests/matrix.rs::an_unfenced_v3_record_reshards_at_v3
//!   - crates/ravel-compat-tests/tests/matrix.rs::a_provisioning_record_above_v4_is_refused
//!   - crates/ravel-catalog/src/provisioning.rs::reshard_and_floor_raise_on_a_fenced_record_keep_the_fields
//!   - crates/ravel-catalog/src/provisioning.rs::an_unfenced_record_is_written_at_3_by_every_path
//!   - crates/ravel-catalog/src/provisioning.rs::a_version_4_record_without_fields_is_never_restamped_3
//! - a bucket provisioned at v3 after the fence walk [pinned]
//!   - crates/ravel-maintain/tests/compat_write_policy.rs::the_first_activated_publish_raises_the_fence_before_the_part_put
//!   - crates/ravel-ingest/src/compat_signal_tests.rs::a_log_flush_above_the_baseline_fences_the_bucket_before_its_first_put
//!   - crates/ravel-maintain/src/compat_certify.rs::a_bucket_provisioned_unfenced_between_runs_refuses
//!   - crates/ravel-maintain/src/compat_certify.rs::the_per_bucket_g_pre_recheck_refuses_a_recently_fenced_bucket
//! - `certify` preconditions: class A family, target at or below active,
//!   `sys/compat` present, time gate [pinned]
//!   - crates/ravel-compat-tests/tests/matrix.rs::certify_refuses_a_non_class_a_family
//!   - crates/ravel-compat-tests/tests/matrix.rs::certify_refuses_a_target_above_active
//!   - crates/ravel-compat-tests/tests/matrix.rs::certify_refuses_without_a_compat_record
//!   - crates/ravel-compat-tests/tests/e2e.rs::compat_init_activate_migrate_certify_verify
//!   - crates/ravel-compat-tests/tests/e2e.rs::with_only_born_fenced_buckets_the_activation_term_sets_the_time_gate
//! - migrate and certify on an incomplete listing [pinned]
//!   - crates/ravel-maintain/src/compat_certify.rs::an_incomplete_listing_blocks
//!   - crates/ravel-maintain/src/compat_certify.rs::a_faulted_listing_blocks_certification
//!   - crates/ravel-maintain/src/migrate.rs::unaddressable_key_fails_the_census
//!   - crates/ravel-maintain/src/migrate.rs::a_failed_census_page_propagates
//! - two late-write passes out of order, and re-certify at the same version
//!   [pinned]
//!   - crates/ravel-maintain/src/compat_certify.rs::the_watermark_never_moves_backward
//!   - crates/ravel-maintain/src/compat_certify.rs::a_certificate_is_replaced_only_upward_or_after_a_late_write
//!   - crates/ravel-maintain/src/compat_certify.rs::a_new_certificate_sets_the_watermark_to_its_s
//! - a late record between two resumed certify runs [pinned]
//!   - crates/ravel-maintain/src/compat_certify.rs::late_write_between_resumed_runs_is_found_by_the_late_write_pass
//! - teardown flush and late write after the certificate [pinned]
//!   - crates/ravel-compat-tests/tests/certify_flags_teardown_pair.rs::certify_does_not_certify_over_a_teardown_pair
//!   - crates/ravel-compat-tests/tests/certify_flags_teardown_pair.rs::certify_does_not_certify_over_an_unrecorded_teardown_object
//!   - crates/ravel-compat-tests/tests/certify_flags_teardown_pair.rs::certify_certifies_the_bucket_without_the_pair
//!   - crates/ravel-compat-tests/tests/certify_flags_teardown_pair.rs::a_teardown_pair_after_the_certificate_sets_contradicted
//! - corpus at N, activation, migrate, certify, verify [pinned]
//!   - crates/ravel-compat-tests/tests/e2e.rs::compat_init_activate_migrate_certify_verify
//!   - crates/ravel-maintain/tests/format_drill.rs::a_run_writing_8_compacts_an_input_recorded_at_8
//!   - crates/ravel-maintain/tests/format_drill.rs::a_run_writing_7_refuses_an_input_recorded_at_8
//!   - crates/ravel-maintain/tests/format_drill.rs::force2_lifts_parts_to_the_runs_version
//! - written by release R, read by HEAD [pinned]
//!   - crates/ravel-compat-tests/tests/corpus.rs::reads_every_frozen_corpus
//! - lease parking: absent, live, stale view, incapable holder [partial: no
//!   test shows a displaced holder stop writing]
//!   - crates/ravel-alerting/src/lease.rs::park_lease_creates_an_absent_lease
//!   - crates/ravel-alerting/src/lease.rs::park_lease_replaces_a_live_lease_whoever_holds_it
//!   - services/ravel-server/src/alerting.rs::a_stale_view_meeting_a_parked_lease_refreshes_and_keeps_it_parked
//!   - services/ravel-server/src/alerting.rs::an_incapable_holder_reparks_its_lease_and_writes_nothing
//!   - services/ravel-server/src/alerting.rs::an_incapable_evaluator_takes_no_lease
//!   - crates/ravel-maintain/src/compat_activate.rs::activation_walk_fences_every_bucket_parks_absent_leases_then_sets_fence_complete
//! - hazards H1, H3, H5 to H11, one or more per hazard, in order [partial: no
//!   test shows an erasure re-encode refuse an unknown section (H6); every
//!   other hazard is pinned]
//!   - crates/ravel-catalog/src/fold.rs::newer_previous_part_fails_cycle_without_rebuild
//!   - services/ravel-server/src/alerting.rs::a_newer_version_object_refuses_the_tick_and_writes_nothing
//!   - services/ravel-server/tests/alerting_newer_version_history.rs::a_newer_version_alert_object_refuses_the_tick
//!   - crates/ravel-maintain/src/compat_certify.rs::an_unreadable_postings_version_is_held
//!   - crates/ravel-maintain/src/retention.rs::held_out_of_window_holds_an_unreadable_rlog_section
//!   - crates/ravel-logseg/src/postings.rs::newer_postings_version_is_typed_unsupported
//!   - crates/ravel-maintain/src/compat_certify.rs::an_unreadable_rspan_key_idx_version_is_held
//!   - crates/ravel-maintain/tests/compat_write_policy.rs::an_unknown_section_kind_holds_the_bucket_in_compaction
//!   - crates/ravel-maintain/tests/compat_write_policy.rs::an_unknown_section_kind_holds_the_bucket_in_migrate
//!   - crates/ravel-maintain/tests/migrate_force2.rs::an_unknown_section_in_a_compacted_part_holds_the_bucket
//!   - crates/ravel-maintain/tests/unknown_section_versions.rs::an_rlog_hold_names_the_inputs_trailer_version
//!   - crates/ravel-maintain/src/audit_retention.rs::unreadable_version_audit_object_is_held
//!   - crates/ravel-maintain/src/alert_retention.rs::unreadable_version_alert_object_is_held
//!   - crates/ravel-fleet/src/claim.rs::claim_above_known_version_or_zero_is_never_stolen
//!   - crates/ravel-maintain/src/claim_guard.rs::a_version_protected_claim_is_never_stolen
//!   - crates/ravel-maintain/src/migrate.rs::unknown_cursor_tag_is_left_in_place
//!   - crates/ravel-maintain/tests/scan.rs::unknown_scan_cursor_tag_is_left_in_place
//!   - crates/ravel-ingest/src/idempotency.rs::newer_marker_refuses_retryably_and_ingests_nothing
//!   - crates/ravel-catalog/src/fold.rs::newer_tenant_config_fails_cycle
//! - retired-reader startup with no, stale, contradicted or current
//!   certificate [pinned]
//!   - services/ravel-server/src/compat_gate.rs::retired_reader_refuses_without_current_certificate
//! - crash after each publish step [pinned]
//!   - crates/ravel-compat-tests/tests/crash_after_each_write.rs::a_crash_at_every_write_of_every_step_resumes_to_the_uninterrupted_end_state
//!   - crates/ravel-maintain/src/compat_activate.rs::crash_mid_walk_resumes_from_the_cursor_and_only_then_sets_fence_complete
//!   - crates/ravel-maintain/src/compat_activate.rs::lost_cursor_resume_rewalks_everything_idempotently
//!   - crates/ravel-maintain/src/compat_certify.rs::a_cursor_left_by_a_failed_delete_is_not_resumed
//!   - crates/ravel-maintain/tests/compat_write_policy.rs::a_failed_fence_cas_leaves_nothing_written
//! - concurrent migrate, ingest, compaction and a late write inside H
//!   [pinned]
//!   - crates/ravel-compat-tests/tests/concurrent_interleaving.rs::seeded_interleavings_of_migrate_ingest_compaction_and_a_late_write
//!   - crates/ravel-maintain/src/migrate.rs::a_straggler_landing_after_the_walk_refuses_the_floor_raise
//!   - crates/ravel-maintain/src/migrate.rs::floor_raise_refuses_when_a_fresh_straggler_survives_the_walk
//!   - crates/ravel-maintain/src/compat_certify.rs::a_lower_version_stamp_yields_contradicted
//! - stale floor [pinned]
//!   - crates/ravel-compat-tests/tests/stale_floor.rs::a_stale_floor_does_not_certify_while_superseded_n_inputs_remain
//!   - services/ravel-cli/src/maintain.rs::audit_versions_reports_a_stale_floor_for_a_record_at_the_floor
//!   - crates/ravel-maintain/src/compat_certify.rs::a_lower_version_stamp_yields_contradicted
//! - a publish straddling activation; a permit granted after activated + H
//!   encodes N+1 [pinned]
//!   - crates/ravel-compat-tests/tests/publish_straddling_activation.rs::a_straddling_publish_lands_at_n_and_a_grant_past_h_encodes_n_plus_one
//!   - crates/ravel-ingest/src/shard.rs::version_chosen_at_permit_grant_and_first_activated_publish_fences_the_bucket
//!   - crates/ravel-maintain/tests/compat_write_policy.rs::the_version_is_chosen_once_per_run_not_per_put
//!   - crates/ravel-maintain/src/compat_certify.rs::certify_refuses_before_the_time_gate
//! - certify cursor lost mid-run [pinned]
//!   - crates/ravel-compat-tests/tests/certify_cursor_lost.rs::a_lost_cursor_restarts_the_census_and_rescans_the_censused_unit
//!   - crates/ravel-compat-tests/tests/certify_cursor_lost.rs::a_lost_cursor_never_certifies_a_partial_census
//!   - crates/ravel-maintain/src/compat_certify.rs::a_cursor_for_another_target_starts_a_fresh_census
//!   - crates/ravel-maintain/src/compat_certify.rs::a_cursor_from_another_epoch_starts_a_fresh_census
//! - Flight SQL scan of an out-of-window object answers a typed error
//!   [partial: HTTP only; no Flight SQL test]
//!   - crates/ravel-query/tests/e2e.rs::an_out_of_window_rseg_answers_422
//!   - services/ravel-server/tests/sql_endpoint.rs::an_out_of_window_rlog_answers_422_not_500
//! - inventory completeness [partial: the sys-proto classification only;
//!   `docs/compatibility.md` does not exist]
//!   - crates/ravel-catalog/tests/sys_proto_format_version_classification.rs::classification_is_complete
//! - retention, erasure, legal hold and a pinned query during migrate
//!   [partial: erasure and a pinned query run during migrate; retention and
//!   legal hold are tested alone, none during a migrate run]
//!   - crates/ravel-compat-tests/tests/erasure_during_migrate.rs::an_erasure_and_a_pinned_query_during_migrate_complete_at_n_plus_1
//!   - crates/ravel-maintain/tests/retention_version_window.rs::an_out_of_window_object_holds_the_bucket
//!   - crates/ravel-maintain/tests/erasure_compaction_fence.rs::an_erasure_rewrite_that_publishes_mid_migration_cancels_the_migration
//!   - crates/ravel-maintain/tests/compat_write_policy.rs::erasure_refuses_an_unwritable_active_version_and_leaves_the_request_pending
//! - rollback "F certified, B+2 installed, roll back to B+1" [pinned]
//!   - crates/ravel-compat-tests/tests/rollback_after_certificate.rs::b_plus_1_after_a_rollback_from_b_plus_2_reads_and_writes_n_plus_1

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use prost::Message;
use ravel_catalog::compat::{
    COMPAT_FORMAT_VERSION, CompatClock, CompatKeys, CompatView, FAMILY_RSEG, activate_family,
    create_compat,
};
use ravel_catalog::{
    AbsentPolicy, CompatRefusal, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS,
    FloorBasis, ProvisioningError, provisioning_key, raise_compat_fence, raise_format_floor,
    read_floors, validate_or_adopt, validate_or_adopt_fenced,
};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::{
    ActivateError, ActivateRequest, Bucket, CertifyRefusal, CertifyReport, CertifyTimings,
    CompactionOutcome, CompactorConfig, CompatInitError, CompatRunRefusal, FamilyVersions,
    FixedClock, InitConfig, InitOutcome, MaintainCompatPolicy, MaintainError, WritableVersions,
    activate, certify, compact_bucket, init_compat,
};
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_proto::sys::v1 as sysproto;
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7,
    open_from_full,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

use common::DecodedSeries;

const SECOND_NS: i64 = 1_000_000_000;
const HOUR_NS: i64 = 3_600 * SECOND_NS;
/// A fixed base instant well inside the representable range.
const T0: i64 = 1_800_000_000 * SECOND_NS;
/// RSEG's baseline version; the drill version is one above it.
const N: u32 = 7;
const N_PLUS_1: u32 = 8;
/// The ingest hour of the seeded L0 inputs; sealed under [`sealed_now_ns`].
const HOUR: u32 = 495_000;
const SHARD: u32 = 0;

#[derive(Debug)]
struct ManualClock(AtomicI64);

impl ManualClock {
    fn new(ns: i64) -> Self {
        ManualClock(AtomicI64::new(ns))
    }
}

impl CompatClock for ManualClock {
    fn now_unix_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn keys() -> CompatKeys {
    CompatKeys::from_file_keys(&[[4; 32]]).expect("one key")
}

fn tenant() -> TenantId {
    TenantId::new("acme")
}

fn hash() -> TenantHash {
    tenant().hash()
}

/// A two-key listing page, so every census and walk pages.
fn store() -> Arc<MemoryStore> {
    Arc::new(MemoryStore::with_page_size(2))
}

/// `sys/compat` present with RSEG at `active`, nothing else activated.
async fn write_compat(store: &dyn ObjectStoreBackend, active: u32) {
    create_compat(
        store,
        &keys(),
        sysproto::CompatRecord {
            format_version: COMPAT_FORMAT_VERSION,
            baseline_release_major: 0,
            baseline_release_minor: 34,
            families: vec![sysproto::CompatFamily {
                family: FAMILY_RSEG.to_string(),
                active_write_version: active,
                ..Default::default()
            }],
            max_publish_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            ..Default::default()
        },
    )
    .await
    .expect("create sys/compat");
}

/// A view over `store`, refreshed once at `T0`.
async fn fresh_view(store: Arc<dyn ObjectStoreBackend>) -> CompatView {
    let view = CompatView::new(store, keys(), Arc::new(ManualClock::new(T0)));
    view.refresh().await.expect("refresh");
    view
}

async fn provision_v3(store: &dyn ObjectStoreBackend) {
    validate_or_adopt(
        store,
        &hash(),
        Signal::Metrics,
        1,
        T0 - HOUR_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
}

async fn read_prov(store: &dyn ObjectStoreBackend) -> sysproto::ProvisioningRecord {
    let bytes = store
        .get(&provisioning_key(&hash(), Signal::Metrics), GetRange::Full)
        .await
        .expect("get prov")
        .data;
    sysproto::ProvisioningRecord::decode(bytes.as_ref()).expect("decode prov")
}

fn init_config() -> InitConfig {
    InitConfig {
        baseline_release_major: 0,
        baseline_release_minor: 34,
        max_publish_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
    }
}

fn activate_request(version: u32, writable_attested: bool) -> ActivateRequest {
    ActivateRequest {
        family: FAMILY_RSEG.to_string(),
        version,
        activated_by: "matrix test".to_string(),
        writer_roles_can_read_compat: writable_attested,
        alert_rule_tenants: Vec::new(),
        clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
        resume_from: None,
    }
}

fn writable_n_and_n1() -> WritableVersions {
    WritableVersions::current().with_family(FAMILY_RSEG, vec![N, N_PLUS_1])
}

fn hour_start_ns() -> i64 {
    i64::from(HOUR) * HOUR_NS
}

/// A compaction clock past the seal margin for [`HOUR`].
fn sealed_now_ns() -> i64 {
    hour_start_ns() + 4 * HOUR_NS
}

fn bucket() -> Bucket {
    Bucket::new(hash(), Signal::Metrics, SHARD, HOUR)
}

fn gauge(name: &str, value: f64, ts_ns: i64) -> SeriesInput {
    let labels = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: name.to_string(),
    }])
    .expect("labels");
    SeriesInput {
        series_id: SeriesId::compute(&tenant(), name, &labels).expect("series id"),
        labels,
        samples: vec![Sample { ts_ns, value }],
    }
}

/// The two L0 inputs a compaction of [`bucket`] merges.
fn l0_inputs() -> Vec<(u64, SeriesInput)> {
    vec![
        (1, gauge("matrix_a", 1.5, hour_start_ns() + 1_000)),
        (2, gauge("matrix_b", -0.0, hour_start_ns() + 2_000)),
    ]
}

/// The bucket's provisioning record and its two RSEG v7 L0 inputs, written as
/// an ingest flush writes them: the data object, then its commit record.
async fn seed_l0_bucket(store: &dyn ObjectStoreBackend) {
    validate_or_adopt(
        store,
        &hash(),
        Signal::Metrics,
        SHARD + 1,
        hour_start_ns(),
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    for (seq, input) in l0_inputs() {
        let writer_id = Uuid::from_u128(0x3A7_0000 + u128::from(seq));
        let ingest_ns = hour_start_ns() + i64::try_from(seq).unwrap() * SECOND_NS;
        let written = SegmentWriter::write(
            vec![input],
            SegmentIdentity {
                tenant_hash: hash().0,
                shard: SHARD,
                writer_id: writer_id.to_string(),
                writer_epoch: 1,
                writer_seq: seq,
            },
            IngestBounds {
                min_ingest_ts_ns: ingest_ns,
                max_ingest_ts_ns: ingest_ns,
            },
        )
        .expect("write RSEG v7");
        let data_key = keys::data_key(
            &hash(),
            Signal::Metrics,
            SHARD,
            writer_id,
            1,
            seq,
            &written.summary.blake3,
        )
        .expect("data key");
        let commit = record::build(NewCommitRecord {
            tenant_hash: hash(),
            signal: Signal::Metrics,
            shard: SHARD,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: ingest_ns,
            max_ingest_ts_ns: ingest_ns,
            segment_format_version: u32::from(VERSION_V7),
            created_unix_ns: ingest_ns,
            ingest_hour_bucket: HOUR,
        })
        .expect("build commit record");
        store
            .put(&data_key, written.bytes.clone(), PutOptions::default())
            .await
            .expect("put data object");
        store
            .put(
                &keys::commit_key_for_record(&commit).expect("commit key"),
                record::encode(&commit),
                PutOptions::default(),
            )
            .await
            .expect("put commit record");
    }
}

/// A compactor config whose compat policy reads `store` and whose RSEG table is
/// `writable` with baseline N: `[N]` is an N-only build, `[N, N+1]` a capable
/// one.
async fn compactor_config(store: Arc<dyn ObjectStoreBackend>, writable: &[u32]) -> CompactorConfig {
    let clock = Arc::new(ManualClock::new(T0));
    let view = CompatView::new(store, keys(), clock.clone());
    view.refresh().await.expect("refresh");
    let policy = MaintainCompatPolicy::new(view, clock).with_family(
        FAMILY_RSEG,
        FamilyVersions {
            writable: writable.iter().map(|v| *v as u16).collect(),
            baseline: N as u16,
        },
    );
    CompactorConfig {
        compat_policy: Some(policy),
        ..CompactorConfig::default()
    }
}

/// Compact [`bucket`] under the RSEG table `writable`.
async fn compact_under(
    store: Arc<MemoryStore>,
    writable: &[u32],
) -> ravel_maintain::Result<CompactionOutcome> {
    let config = compactor_config(store.clone(), writable).await;
    compact_bucket(
        store.as_ref(),
        &FixedClock::new(sealed_now_ns()),
        &config,
        &bucket(),
    )
    .await
}

/// The RSEG L1 parts on the store: key, trailer version and decoded series.
async fn l1_parts(store: &dyn ObjectStoreBackend) -> Vec<(String, u16, Vec<DecodedSeries>)> {
    let mut parts = Vec::new();
    for key in common::data_keys(store).await {
        if !key.contains("/l1/") {
            continue;
        }
        let bytes = store.get(&key, GetRange::Full).await.expect("get").data;
        let version = open_from_full(&bytes, ReaderLimits::default())
            .expect("open the part")
            .version;
        let series = common::decode_rseg(&bytes).expect("decode the part");
        parts.push((key, version, series));
    }
    parts
}

/// The series every L1 part over [`l0_inputs`] must decode to, in `SeriesId`
/// order with exact value bits.
fn merged_model() -> Vec<DecodedSeries> {
    let mut out: Vec<DecodedSeries> = l0_inputs()
        .into_iter()
        .map(|(_, s)| DecodedSeries {
            labels: s
                .labels
                .iter()
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect(),
            series_id: s.series_id.0,
            samples: s
                .samples
                .iter()
                .map(|x| (x.ts_ns, x.value.to_bits()))
                .collect(),
        })
        .collect();
    out.sort_by_key(|s| s.series_id);
    out
}

// === Version resolution: mixed nodes and rollback (decision 5; matrix rows
// "mixed nodes before/after activation", "rollback before activation") ========

/// Before activation, both an N+1-capable build and an N-only build resolve to
/// N for the family, and a real compaction under each writes one L1 part whose
/// trailer is N and which decodes to the inputs' exact samples, without raising
/// the bucket's fence (matrix row "mixed nodes N-only and N+1, before
/// activation", and decision 5 row "B+1 installed, F not activated, roll back
/// to B"). This crate's reader admits {N, N+1}; that an N-only reader admits N
/// is `todays_window_accepts_only_the_current_version`, cited in the header.
#[tokio::test]
async fn mixed_nodes_before_activation_both_write_n() {
    let view_store = store();
    write_compat(view_store.as_ref(), N).await;
    let view = fresh_view(view_store).await;

    let capable = view
        .write_policy(
            T0,
            FAMILY_RSEG,
            &[N as u16, N_PLUS_1 as u16],
            N as u16,
            None,
        )
        .expect("capable build writes");
    let n_only = view
        .write_policy(T0, FAMILY_RSEG, &[N as u16], N as u16, None)
        .expect("N-only build writes");

    assert_eq!(capable, N as u16, "capable build emits N before activation");
    assert_eq!(n_only, N as u16, "N-only build emits N before activation");

    for writable in [&[N, N_PLUS_1][..], &[N][..]] {
        let store = store();
        seed_l0_bucket(store.as_ref()).await;
        write_compat(store.as_ref(), N).await;

        let outcome = compact_under(store.clone(), writable)
            .await
            .unwrap_or_else(|e| panic!("compaction under {writable:?}: {e:?}"));
        assert!(
            matches!(outcome, CompactionOutcome::Compacted { .. }),
            "{writable:?}: {outcome:?}"
        );

        let parts = l1_parts(store.as_ref()).await;
        assert_eq!(parts.len(), 1, "{writable:?}: one L1 part: {parts:?}");
        let (key, version, series) = &parts[0];
        assert_eq!(*version, VERSION_V7, "{writable:?}: {key} trailer is N");
        assert_eq!(series, &merged_model(), "{writable:?}: {key} decodes");
        let prov = read_prov(store.as_ref()).await;
        assert_eq!(
            (prov.format_version, prov.compat_epoch),
            (3, 0),
            "{writable:?}: no fence before activation: {prov:?}"
        );
    }
}

/// After activation, the N+1-capable build resolves to the active version N+1.
#[tokio::test]
async fn n_plus_one_node_writes_active_after_activation() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    activate_family(store.as_ref(), &keys(), FAMILY_RSEG, N_PLUS_1, "t", T0)
        .await
        .expect("activate");
    let view = fresh_view(store.clone()).await;

    let v = view
        .write_policy(
            T0,
            FAMILY_RSEG,
            &[N as u16, N_PLUS_1 as u16],
            N as u16,
            None,
        )
        .expect("capable build writes the active version");
    assert_eq!(v, N_PLUS_1 as u16);
}

/// After activation, an N-only build refuses the family's publishes with the
/// typed `ActiveVersionNotWritable`: a real compaction under its RSEG table
/// returns that refusal and leaves every key and byte on the store as it was
/// (matrix row "mixed nodes, after activation"; decision 5 row "F activated
/// and N+1 objects exist, roll back to B"). That such a build's reader refuses
/// an N+1 object with `UnsupportedVersion` is
/// `a_version_outside_the_window_fails_with_unsupported_version`, cited in the
/// header: the window is compile-time and this crate builds with N+1 in it.
#[tokio::test]
async fn n_only_node_refuses_the_family_after_activation() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    activate_family(store.as_ref(), &keys(), FAMILY_RSEG, N_PLUS_1, "t", T0)
        .await
        .expect("activate");
    let view = fresh_view(store.clone()).await;

    let refusal = view
        .write_policy(T0, FAMILY_RSEG, &[N as u16], N as u16, None)
        .expect_err("N-only build refuses");
    assert!(
        matches!(
            refusal,
            CompatRefusal::ActiveVersionNotWritable { ref family, active: 8, .. }
                if family == FAMILY_RSEG
        ),
        "{refusal:?}"
    );

    seed_l0_bucket(store.as_ref()).await;
    let before = common::snapshot(store.as_ref()).await;
    let err = compact_under(store.clone(), &[N])
        .await
        .expect_err("the N-only compaction refuses");
    assert!(
        matches!(
            &err,
            MaintainError::CompatRefused {
                family,
                refusal: CompatRunRefusal::Compat(CompatRefusal::ActiveVersionNotWritable {
                    active: 8,
                    ..
                }),
                ..
            } if family == FAMILY_RSEG
        ),
        "{err:?}"
    );
    assert_eq!(
        common::snapshot(store.as_ref()).await,
        before,
        "the refused compaction writes nothing"
    );
}

/// A writer whose last successful read is older than the hard horizon refuses
/// to publish with `ViewStale`: the stale writer stops past `H` (matrix row
/// "stale sys/compat view ... stale writer emits only the older version and
/// stops past H").
#[tokio::test]
async fn a_stale_view_refuses_to_publish_past_the_horizon() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let view = fresh_view(store.clone()).await;

    // Fresh at T0, but a day later with no further refresh the view is stale.
    let refusal = view
        .write_policy(
            T0 + 24 * HOUR_NS,
            FAMILY_RSEG,
            &[N as u16, N_PLUS_1 as u16],
            N as u16,
            None,
        )
        .expect_err("stale view refuses");
    assert!(
        matches!(refusal, CompatRefusal::ViewStale { .. }),
        "{refusal:?}"
    );
}

/// A process that finds `sys/compat` absent while a bucket's provisioning
/// record carries a `compat_epoch` refuses that bucket's publishes with
/// `CompatRecordMissing`: a deleted or rolled-back `sys/compat` is never
/// treated as "baseline" (matrix row "sys/compat deleted after activation:
/// publishes refused with CompatRecordMissing").
#[tokio::test]
async fn absent_compat_with_a_fenced_bucket_refuses_record_missing() {
    let store = store();
    // No sys/compat is ever written. The view refreshes against the absence.
    let view = fresh_view(store.clone()).await;
    let fenced_bucket = sysproto::ProvisioningRecord {
        format_version: 4,
        compat_epoch: 1,
        ..Default::default()
    };

    let refusal = view
        .write_policy(
            T0,
            FAMILY_RSEG,
            &[N as u16, N_PLUS_1 as u16],
            N as u16,
            Some(&fenced_bucket),
        )
        .expect_err("absent compat over a fenced bucket refuses");
    assert!(
        matches!(refusal, CompatRefusal::CompatRecordMissing { .. }),
        "{refusal:?}"
    );
}

// === Activation is durable and monotone (decision 3) =========================

/// `activate` refuses unless `sys/compat` exists: it is run only after `init`
/// (decision 3, "it refuses unless sys/compat exists").
#[tokio::test]
async fn activate_refuses_without_init() {
    let store = store();
    let clock = ManualClock::new(T0);
    let err = activate(
        store.as_ref(),
        Some(&keys()),
        &clock,
        &writable_n_and_n1(),
        &activate_request(N_PLUS_1, true),
    )
    .await
    .expect_err("no sys/compat");
    assert!(matches!(err, ActivateError::RecordAbsent), "{err:?}");
}

/// `activate` never lowers or re-sets a version: a request at the current
/// active version is refused `NotAboveActive`.
#[tokio::test]
async fn activate_refuses_a_target_not_above_active() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let clock = ManualClock::new(T0);
    let err = activate(
        store.as_ref(),
        Some(&keys()),
        &clock,
        &writable_n_and_n1(),
        &activate_request(N, true),
    )
    .await
    .expect_err("not above active");
    assert!(
        matches!(
            err,
            ActivateError::NotAboveActive {
                active: 7,
                requested: 7,
                ..
            }
        ),
        "{err:?}"
    );
}

/// `activate` moves exactly one version: a jump of two is refused `VersionJump`.
#[tokio::test]
async fn activate_refuses_a_version_jump() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let clock = ManualClock::new(T0);
    let writable = WritableVersions::current().with_family(FAMILY_RSEG, vec![N, N_PLUS_1, 9]);
    let err = activate(
        store.as_ref(),
        Some(&keys()),
        &clock,
        &writable,
        &activate_request(9, true),
    )
    .await
    .expect_err("version jump");
    assert!(
        matches!(
            err,
            ActivateError::VersionJump {
                active: 7,
                requested: 9,
                ..
            }
        ),
        "{err:?}"
    );
}

/// The external prerequisite is attested at activation: without the operator's
/// attestation (the flag the ADR amendment names
/// `--pre-baseline-processes-terminated`; it ships today as the
/// `writer_roles_can_read_compat` field) `activate` refuses (decision 3,
/// Consequences "compat activate must refuse without the operator's
/// attestation").
#[tokio::test]
async fn activate_refuses_without_the_attestation() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let clock = ManualClock::new(T0);
    let err = activate(
        store.as_ref(),
        Some(&keys()),
        &clock,
        &writable_n_and_n1(),
        &activate_request(N_PLUS_1, false),
    )
    .await
    .expect_err("no attestation");
    assert!(
        matches!(err, ActivateError::WriterRolesCannotReadCompat),
        "{err:?}"
    );
}

/// A successful activation raises the version and completes the fence walk,
/// raising the provisioned bucket's fence (decision 3/4: `compat activate`
/// fences every existing provisioned bucket and records
/// `fence_complete_unix_ns`).
#[tokio::test]
async fn activate_completes_the_fence_walk() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    provision_v3(store.as_ref()).await;
    let clock = ManualClock::new(T0);

    let report = activate(
        store.as_ref(),
        Some(&keys()),
        &clock,
        &writable_n_and_n1(),
        &activate_request(N_PLUS_1, true),
    )
    .await
    .expect("activate");

    assert_eq!(report.active_write_version, N_PLUS_1);
    assert!(report.walk.complete, "{:?}", report.walk);
    assert!(
        report.walk.fence_complete_unix_ns.is_some(),
        "{:?}",
        report.walk
    );
    assert_eq!(report.walk.buckets_raised, 1, "{:?}", report.walk);
    // The bucket's provisioning record is now fenced at version 4.
    let prov = read_prov(store.as_ref()).await;
    assert_eq!(prov.format_version, 4, "{prov:?}");
    assert!(prov.compat_epoch >= 1, "{prov:?}");
}

// === init adopts a clean store (decision 1; matrix rows for `compat init`) ====

/// `init` over a clean store creates `sys/compat` at the baseline versions.
#[tokio::test]
async fn init_adopts_a_clean_store() {
    let store = store();
    let clock = ManualClock::new(T0);
    let report = init_compat(store.as_ref(), Some(&keys()), &clock, &init_config())
        .await
        .expect("init");
    assert_eq!(report.outcome, InitOutcome::Created);
    assert!(report.census.is_some(), "a census ran");
    // The RSEG family is adopted at its baseline.
    let rseg = report
        .record
        .families
        .iter()
        .find(|f| f.family == FAMILY_RSEG)
        .expect("rseg entry");
    assert_eq!(rseg.active_write_version, N);
}

/// `init` is idempotent on the record it would write: a second run finds the
/// record already present and writes nothing.
#[tokio::test]
async fn init_is_idempotent() {
    let store = store();
    let clock = ManualClock::new(T0);
    init_compat(store.as_ref(), Some(&keys()), &clock, &init_config())
        .await
        .expect("first init");
    let again = init_compat(store.as_ref(), Some(&keys()), &clock, &init_config())
        .await
        .expect("second init");
    assert_eq!(again.outcome, InitOutcome::AlreadyInitialized);
    assert!(again.census.is_none(), "no census on the second run");
}

/// `init` refuses a store whose provisioning record carries a `compat_epoch`
/// but has no `sys/compat`: the record was deleted or rolled back and must be
/// restored from bucket versioning, never re-created at the baseline (matrix
/// row "sys/compat deleted after activation: compat init refuses").
#[tokio::test]
async fn init_refuses_a_fenced_store_missing_compat() {
    let store = store();
    provision_v3(store.as_ref()).await;
    raise_compat_fence(store.as_ref(), &hash(), Signal::Metrics, 1, T0)
        .await
        .expect("fence the bucket");
    let clock = ManualClock::new(T0);
    let err = init_compat(store.as_ref(), Some(&keys()), &clock, &init_config())
        .await
        .expect_err("fenced store refuses init");
    assert!(
        matches!(
            err,
            CompatInitError::ProvisioningFenced {
                compat_epoch: 1,
                ..
            }
        ),
        "{err:?}"
    );
}

/// `init` fails closed on an incomplete listing rather than shortening the
/// population (matrix row "incomplete listing: certify/migrate refuse"; here at
/// the `init` census, which lists strictly).
#[tokio::test]
async fn init_refuses_on_an_incomplete_listing() {
    let inner = store();
    let store = FaultStore::new(
        inner,
        FaultPlan::empty().with_rule(Rule::new(
            Op::List,
            ScriptedFault::Transient("list down".into()),
        )),
    );
    let clock = ManualClock::new(T0);
    let err = init_compat(&store, Some(&keys()), &clock, &init_config())
        .await
        .expect_err("listing failure refuses");
    assert!(matches!(err, CompatInitError::List(_)), "{err:?}");
}

// === Provisioning-record fencing (decision 4; matrix rows for fenced/unfenced
// provisioning records and the born-fenced tenant) ===========================

/// A tenant onboarded after activation is born fenced: its record is written
/// directly at version 4 with the current epoch and `compat_fenced_unix_ns = 0`
/// (no pre-baseline process ever saw it at version 3), so the `certify` wait is
/// not restarted (matrix row "a tenant onboarded after activation").
#[tokio::test]
async fn a_tenant_onboarded_after_activation_is_born_fenced() {
    let store = store();
    validate_or_adopt_fenced(
        store.as_ref(),
        &hash(),
        Signal::Metrics,
        1,
        T0,
        AbsentPolicy::CreateFromConfig,
        Some(std::num::NonZeroU64::new(3).unwrap()),
    )
    .await
    .expect("born-fenced provision");

    let prov = read_prov(store.as_ref()).await;
    assert_eq!(prov.format_version, 4, "born at version 4: {prov:?}");
    assert_eq!(prov.compat_epoch, 3, "{prov:?}");
    assert_eq!(
        prov.compat_fenced_unix_ns, 0,
        "born fenced stamps no time: {prov:?}"
    );
}

/// A fenced (version 4) provisioning record keeps fields 8 and 9 through a
/// reshard and a floor raise by the baseline (matrix row "fenced (version 4)
/// provisioning record: reshard and floor raise by the baseline").
#[tokio::test]
async fn a_fenced_v4_record_survives_a_reshard() {
    let store = store();
    provision_v3(store.as_ref()).await;
    raise_compat_fence(store.as_ref(), &hash(), Signal::Metrics, 2, T0)
        .await
        .expect("fence");
    let fenced = read_prov(store.as_ref()).await;
    assert_eq!(fenced.format_version, 4);
    assert_eq!(fenced.compat_epoch, 2);
    let fenced_at = fenced.compat_fenced_unix_ns;
    assert_eq!(fenced_at, T0, "first fence stamps the time");

    // Reshard: append one generation with a different count, activation strictly
    // in the future.
    let now_hour = u32::try_from((T0 + HOUR_NS).div_euclid(HOUR_NS)).unwrap();
    ravel_catalog::append_generation(
        store.as_ref(),
        &hash(),
        Signal::Metrics,
        2,
        now_hour + 10,
        T0 + HOUR_NS,
    )
    .await
    .expect("reshard");

    let after = read_prov(store.as_ref()).await;
    assert_eq!(
        after.format_version, 4,
        "reshard keeps version 4: {after:?}"
    );
    assert_eq!(after.compat_epoch, 2, "keeps the epoch: {after:?}");
    assert_eq!(
        after.compat_fenced_unix_ns, fenced_at,
        "keeps the fence time: {after:?}"
    );

    raise_rseg_floor(store.as_ref(), T0 + 2 * HOUR_NS).await;
    let floored = read_prov(store.as_ref()).await;
    assert_eq!(
        (
            floored.format_version,
            floored.compat_epoch,
            floored.compat_fenced_unix_ns
        ),
        (4, 2, fenced_at),
        "a floor raise keeps version 4 and both fence fields: {floored:?}"
    );
    assert_eq!(rseg_floor(&floored), Some(N), "{floored:?}");
}

const FLOOR_BASIS: FloorBasis = FloorBasis {
    observed_entries: 1,
    observed_newest_created_unix_ns: T0,
    observed_shards: 1,
};

/// Raise the bucket's RSEG floor to N, as `migrate` does after its census.
async fn raise_rseg_floor(store: &dyn ObjectStoreBackend, now_ns: i64) {
    raise_format_floor(
        store,
        &hash(),
        Signal::Metrics,
        FAMILY_RSEG,
        N,
        FLOOR_BASIS,
        "matrix test",
        now_ns,
    )
    .await
    .expect("raise the floor");
}

/// The record's current RSEG floor.
fn rseg_floor(rec: &sysproto::ProvisioningRecord) -> Option<u32> {
    read_floors(rec, &provisioning_key(&hash(), Signal::Metrics))
        .expect("floors")
        .into_iter()
        .rev()
        .find(|f| f.family == FAMILY_RSEG)
        .map(|f| f.floor_version)
}

/// An unfenced (version 3) provisioning record reshards and stays at version 3,
/// adding neither compat field, so the pre-baseline fence never fires on an
/// unfenced bucket (matrix row "unfenced (version 3) provisioning record").
#[tokio::test]
async fn an_unfenced_v3_record_reshards_at_v3() {
    let store = store();
    provision_v3(store.as_ref()).await;
    let before = read_prov(store.as_ref()).await;
    assert_eq!(before.format_version, 3);
    assert_eq!(before.compat_epoch, 0);

    let now_hour = u32::try_from((T0 + HOUR_NS).div_euclid(HOUR_NS)).unwrap();
    ravel_catalog::append_generation(
        store.as_ref(),
        &hash(),
        Signal::Metrics,
        2,
        now_hour + 10,
        T0 + HOUR_NS,
    )
    .await
    .expect("reshard");

    let after = read_prov(store.as_ref()).await;
    assert_eq!(after.format_version, 3, "stays version 3: {after:?}");
    assert_eq!(after.compat_epoch, 0, "no epoch: {after:?}");
    assert_eq!(after.compat_fenced_unix_ns, 0, "no fence time: {after:?}");

    raise_rseg_floor(store.as_ref(), T0 + 2 * HOUR_NS).await;
    let floored = read_prov(store.as_ref()).await;
    assert_eq!(
        (
            floored.format_version,
            floored.compat_epoch,
            floored.compat_fenced_unix_ns
        ),
        (3, 0, 0),
        "a floor raise keeps version 3 and adds no fence field: {floored:?}"
    );
    assert_eq!(rseg_floor(&floored), Some(N), "{floored:?}");
}

/// A provisioning record above the rewrite ceiling (version 4) is refused
/// rather than rewritten through an older field set (matrix row tail "a record
/// above 4 is refused").
#[tokio::test]
async fn a_provisioning_record_above_v4_is_refused() {
    let store = store();
    provision_v3(store.as_ref()).await;
    raise_compat_fence(store.as_ref(), &hash(), Signal::Metrics, 1, T0)
        .await
        .expect("fence to v4");
    // Forge a version-5 record in place, reusing the real record's shape.
    let mut rec = read_prov(store.as_ref()).await;
    rec.format_version = 5;
    store
        .put(
            &provisioning_key(&hash(), Signal::Metrics),
            rec.encode_to_vec().into(),
            ravel_object_store::PutOptions::default(),
        )
        .await
        .expect("put forged v5");
    let before = common::snapshot(store.as_ref()).await;

    let err = raise_compat_fence(store.as_ref(), &hash(), Signal::Metrics, 2, T0 + HOUR_NS)
        .await
        .expect_err("v5 record is refused");
    assert!(
        matches!(
            err,
            ProvisioningError::RefusingToRewriteNewerRecord { got: 5, .. }
        ),
        "{err:?}"
    );
    assert_eq!(
        common::snapshot(store.as_ref()).await,
        before,
        "the refused fence raise leaves the version-5 record's bytes unchanged"
    );
}

// === certify preconditions (decision 7) ======================================

/// `certify` applies only to the class A families and refuses any other with
/// `NotClassAFamily` (decision 7, "it applies to the class A families only, and
/// refuses any other").
#[tokio::test]
async fn certify_refuses_a_non_class_a_family() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let report = certify(store.as_ref(), &keys(), T0, "csnap", 1, timings(), None)
        .await
        .expect("certify runs");
    assert!(
        matches!(
            report,
            CertifyReport::Refused(CertifyRefusal::NotClassAFamily { .. })
        ),
        "{report:?}"
    );
}

/// `certify` refuses a target above the family's active version with
/// `TargetAboveActive` (decision 7 preconditions, "F's active version is at
/// least N+1").
#[tokio::test]
async fn certify_refuses_a_target_above_active() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let report = certify(
        store.as_ref(),
        &keys(),
        T0,
        FAMILY_RSEG,
        N_PLUS_1,
        timings(),
        None,
    )
    .await
    .expect("certify runs");
    assert!(
        matches!(
            report,
            CertifyReport::Refused(CertifyRefusal::TargetAboveActive {
                target: 8,
                active: 7
            })
        ),
        "{report:?}"
    );
}

/// `certify` refuses when `sys/compat` is absent with `CompatRecordAbsent`.
#[tokio::test]
async fn certify_refuses_without_a_compat_record() {
    let store = store();
    let report = certify(
        store.as_ref(),
        &keys(),
        T0,
        FAMILY_RSEG,
        N_PLUS_1,
        timings(),
        None,
    )
    .await
    .expect("certify runs");
    assert!(
        matches!(
            report,
            CertifyReport::Refused(CertifyRefusal::CompatRecordAbsent)
        ),
        "{report:?}"
    );
}

fn timings() -> CertifyTimings {
    CertifyTimings {
        t_req_ns: 60 * SECOND_NS,
        max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
        clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    }
}

// === The row-to-test map is checked against the tree ==========================

/// Citation lines in this file's header map. A changed count fails until the
/// constant is updated, so a parse that silently finds fewer lines fails too.
const CITED_TESTS: usize = 116;

/// The `path::name` citations in this file's header map.
fn header_citations(source: &str) -> Vec<(String, String)> {
    source
        .lines()
        .take_while(|l| l.starts_with("//!"))
        .filter_map(|l| l.strip_prefix("//!   - "))
        .map(|cite| {
            let (file, name) = cite
                .rsplit_once("::")
                .unwrap_or_else(|| panic!("a citation without `::`: {cite}"));
            (file.to_string(), name.to_string())
        })
        .collect()
}

/// Whether `source` defines a function `name` carrying a `#[test]` or
/// `#[tokio::test]` attribute, and no `#[ignore`, among the attribute and doc
/// lines above it.
fn defines_test(source: &str, name: &str) -> bool {
    let lines: Vec<&str> = source.lines().collect();
    let sig = format!("fn {name}(");
    lines.iter().enumerate().any(|(i, line)| {
        if !line.contains(&sig) {
            return false;
        }
        let block: Vec<&str> = lines[..i]
            .iter()
            .rev()
            .map(|l| l.trim())
            .take_while(|l| l.starts_with("#[") || l.starts_with("//"))
            .collect();
        block
            .iter()
            .any(|l| *l == "#[test]" || l.starts_with("#[tokio::test"))
            && !block.iter().any(|l| l.starts_with("#[ignore"))
    })
}

/// Every test the header map cites is a test function in the file it names.
#[test]
fn every_cited_test_exists() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest.join("../..");
    let own = std::fs::read_to_string(manifest.join("tests/matrix.rs")).expect("read matrix.rs");
    let cited = header_citations(&own);
    assert_eq!(cited.len(), CITED_TESTS, "{cited:?}");

    let missing: Vec<String> = cited
        .iter()
        .filter(|(file, name)| {
            std::fs::read_to_string(root.join(file))
                .map(|source| !defines_test(&source, name))
                .unwrap_or(true)
        })
        .map(|(file, name)| format!("{file}::{name}"))
        .collect();
    assert!(missing.is_empty(), "cited but not a test: {missing:#?}");
}

/// The check refuses a name that is only a prefix of a test, a helper that is
/// not a test, an ignored test, and a test in another file.
#[test]
fn the_citation_check_refuses_near_misses() {
    let source = "#[tokio::test]\nasync fn real_test() {}\n\nfn helper() {}\n\n\
                  #[test]\n#[ignore = \"slow\"]\nfn ignored_test() {}\n\n\
                  #[ignore]\n#[tokio::test]\nasync fn ignored_first() {}\n";
    assert!(defines_test(source, "real_test"));
    assert!(!defines_test(source, "real"), "a prefix");
    assert!(!defines_test(source, "helper"), "not a test");
    assert!(
        !defines_test(source, "ignored_test"),
        "ignored after #[test]"
    );
    assert!(
        !defines_test(source, "ignored_first"),
        "ignored before the test"
    );
    assert!(!defines_test("", "real_test"), "another file");
    let cites = header_citations("//! map\n//!   - a/b.rs::c\n//!   - d.rs::e\nfn x() {}\n");
    assert_eq!(
        cites,
        vec![
            ("a/b.rs".to_string(), "c".to_string()),
            ("d.rs".to_string(), "e".to_string())
        ]
    );
}
