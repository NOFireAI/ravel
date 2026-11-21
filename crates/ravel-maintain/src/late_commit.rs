//! The late-commit fence (ADR-4561): maintenance plans a bucket only from the
//! commit records the store stamped before the bucket's cutoff, and leaves
//! every later one where it is, served and kept.
//!
//! A sealed bucket stops gaining commit PUTs from writers, but a PUT a writer
//! issued before its bound can land after it. A plan that took such a record
//! and a plan that did not would name different input sets for one bucket,
//! which is how a compaction record and an erasure rewrite record end up
//! covering it only in part. Classifying by the store's stamp against a fixed
//! cutoff gives every plan of a bucket, at any time, the same L0 set, provided
//! every maintainer of the store derives the cutoff from the same seal margin.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use ravel_object_store::ObjectStoreBackend;
use ravel_types::Signal;

use crate::bucket::Bucket;
use crate::config::{CompactorConfig, LateCommitFence};
use crate::read::BucketListing;

#[derive(Default)]
struct Counts {
    late_commit_records: HashMap<Signal, u64>,
    store_clock_unobserved: HashMap<Signal, u64>,
}

static COUNTS: LazyLock<Mutex<Counts>> = LazyLock::new(|| Mutex::new(Counts::default()));

fn counts() -> MutexGuard<'static, Counts> {
    COUNTS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Late commit records of `signal` that a plan in this process left out since
/// it started (ADR-4561 R8), counted once per plan that left each one out.
pub fn late_commit_records_total(signal: Signal) -> u64 {
    counts()
        .late_commit_records
        .get(&signal)
        .copied()
        .unwrap_or(0)
}

/// Plans of `signal` buckets in this process that found no store-clock
/// observation and so planned on this process's clock alone (ADR-4561 R3).
pub fn store_clock_unobserved_total(signal: Signal) -> u64 {
    counts()
        .store_clock_unobserved
        .get(&signal)
        .copied()
        .unwrap_or(0)
}

/// Whether a pass may LIST `bucket` to plan from it at `now_ns`, by its own
/// clock alone. With the fence off this is [`Bucket::is_sealed`].
pub(crate) fn may_list_to_plan(bucket: &Bucket, now_ns: i64, config: &CompactorConfig) -> bool {
    match config.late_commit_fence {
        LateCommitFence::Off => bucket.is_sealed(now_ns, config),
        LateCommitFence::Enforce => bucket.is_plannable(now_ns, None, config),
    }
}

/// Whether the erasure completion gate may judge `bucket` at `now_ns`
/// ([`crate::bucket_erasure_completion`]). Under [`LateCommitFence::Enforce`]
/// this is the test a raw-L0 erasure rewrite plans under,
/// [`Bucket::is_plannable`] with the store's observed clock, so completion
/// never judges a bucket before the first rewrite of it could plan: an on-time
/// record stamped just before the cutoff may not be visible to a LIST until
/// then. With the fence off it is [`Bucket::is_sealed`].
pub fn may_judge_completion(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    now_ns: i64,
    config: &CompactorConfig,
) -> bool {
    match config.late_commit_fence {
        LateCommitFence::Off => bucket.is_sealed(now_ns, config),
        LateCommitFence::Enforce => {
            bucket.is_plannable(now_ns, store.observed_store_time_ns(), config)
        }
    }
}

/// Apply the fence to `listing`, the LIST a pass will plan from, taken at
/// `now_ns` by the pass's clock. Returns `false` when the store's clock, read
/// now that the LIST has returned, is not yet past the cutoff plus the settle
/// allowance: the pass then reports the bucket not sealed (R3). Otherwise
/// classifies the listing (R2) and reports every late record left out of the
/// plan (R8). With the fence off the listing is left unclassified and this
/// returns `true`.
pub(crate) fn fence_planning_listing(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    now_ns: i64,
    config: &CompactorConfig,
    listing: &mut BucketListing,
    pass: &'static str,
) -> bool {
    if config.late_commit_fence == LateCommitFence::Off {
        return true;
    }
    let observed = store.observed_store_time_ns();
    if observed.is_none() {
        *counts()
            .store_clock_unobserved
            .entry(bucket.signal)
            .or_insert(0) += 1;
    }
    if !bucket.is_plannable(now_ns, observed, config) {
        tracing::debug!(
            signal = ?bucket.signal,
            shard = bucket.shard,
            ingest_hour_bucket = bucket.ingest_hour_bucket,
            pass,
            observed_store_ns = observed,
            "store clock not yet past the bucket's late-commit cutoff; not planning (ADR-4561)"
        );
        return false;
    }
    let cutoff_ns = bucket.late_commit_cutoff_ns(config);
    listing.classify_late(cutoff_ns);
    report_left_out(bucket, listing, pass);
    true
}

/// Classify `relisted`, a re-list of a bucket planned from `planned`, under
/// the cutoff `planned` was classified under, so the pre-publish comparison
/// sees the same split (ADR-4561 R5).
pub(crate) fn classify_like(planned: &BucketListing, relisted: &mut BucketListing) {
    if let Some(cutoff_ns) = planned.late_commit_cutoff_ns {
        relisted.classify_late(cutoff_ns);
    }
}

fn report_left_out(bucket: &Bucket, listing: &BucketListing, pass: &'static str) {
    let late = &listing.late_commit_keys;
    ravel_invariant::inv_sometimes!(
        !late.is_empty(),
        "maintain: a plan left a late commit record out",
        std::collections::BTreeMap::from([
            ("pass", pass.to_string()),
            ("late_commit_records", late.len().to_string()),
        ])
    );
    if late.is_empty() {
        return;
    }
    *counts()
        .late_commit_records
        .entry(bucket.signal)
        .or_insert(0) += late.len() as u64;
    for key in late {
        tracing::warn!(
            signal = ?bucket.signal,
            shard = bucket.shard,
            ingest_hour_bucket = bucket.ingest_hour_bucket,
            pass,
            commit_key = key.as_str(),
            cutoff_ns = listing.late_commit_cutoff_ns,
            "commit record stamped at or after its bucket's late-commit cutoff; left out of \
             this plan and still served (ADR-4561)"
        );
    }
}

/// The publish-side check (ADR-4561, test 10): a record built from `planned`
/// names none of its late commit records. `inputs` are the commit keys the
/// record names.
pub(crate) fn assert_names_no_late_record(
    planned: &BucketListing,
    inputs: &[&str],
    record: &'static str,
) {
    // Without the invariant features the assertion below compiles to nothing.
    let _ = (planned, inputs, record);
    ravel_invariant::inv_always!(
        first_late(planned, inputs).is_none(),
        "maintain: a published record names no late commit record",
        std::collections::BTreeMap::from([
            ("record", record.to_string()),
            (
                "late_commit_key",
                first_late(planned, inputs).unwrap_or_default().to_string()
            ),
        ])
    );
}

// Called only from inside the invariant macros, which may compile to nothing.
#[allow(dead_code)]
fn first_late<'a>(planned: &BucketListing, inputs: &[&'a str]) -> Option<&'a str> {
    inputs.iter().copied().find(|key| {
        planned
            .late_commit_keys
            .binary_search_by(|late| late.as_str().cmp(key))
            .is_ok()
    })
}
