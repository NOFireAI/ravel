//! Hazard H6 (ADR-4233 decision 4) on RLOG and RSPAN: compaction holds a
//! bucket whose input carries a section kind this build does not know, and
//! the hold names the version in that input's trailer, the version the object
//! was written at. The commit record of each spliced input names a different
//! version, so a hold that reports the record's version would fail here too.
//!
//! Its own binary: the held counter is process-wide and
//! `compat_write_policy.rs` asserts its exact increment.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_maintain::{CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use uuid::Uuid;

/// Every key in the store.
async fn keys_of(store: &dyn ObjectStoreBackend) -> Vec<String> {
    list_all(store, "")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect()
}

async fn compact(
    store: &dyn ObjectStoreBackend,
    bucket: &ravel_maintain::Bucket,
) -> CompactionOutcome {
    compact_bucket(
        store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        bucket,
    )
    .await
    .expect("a hold is an outcome")
}

/// The record of the spliced RLOG input says one below its trailer.
///
/// Passing 0 to `refuse_unknown_sections` in `rlog.rs` instead of
/// `footer::trailer_version` fails here.
#[tokio::test]
async fn an_rlog_hold_names_the_inputs_trailer_version() {
    let store = MemoryStore::new();
    seed_rlog_input(
        &store,
        Uuid::from_u128(1),
        10,
        1,
        &[log_record(0, 10, "alpha")],
    )
    .await;
    let trailer = ravel_logseg::footer::VERSION;
    let commit_key = seed_rlog_input_at_hour(
        &store,
        Uuid::from_u128(2),
        10,
        2,
        HOUR,
        &[log_record(1, 15, "bravo")],
        false,
        u32::from(trailer) - 1,
    )
    .await;
    splice_input(&store, &commit_key, rlog_with_unknown_section).await;
    let before = keys_of(&store).await;

    let outcome = compact(&store, &logs_bucket()).await;

    assert_eq!(
        outcome,
        CompactionOutcome::HeldUnknownSection {
            version: trailer,
            kinds: vec![UNKNOWN_KIND],
        }
    );
    assert_eq!(keys_of(&store).await, before, "nothing is written");
}

/// The common RSPAN seed records version 1 while its trailer carries the
/// current version.
///
/// Passing 0 to `refuse_unknown_sections` in `rspan_codec.rs` instead of
/// `rspan_trailer_version` fails here.
#[tokio::test]
async fn an_rspan_hold_names_the_inputs_trailer_version() {
    let store = MemoryStore::new();
    seed_rspan_input(
        &store,
        Uuid::from_u128(1),
        10,
        1,
        &[span_record(0, 0, 10, 20)],
    )
    .await;
    let commit_key = seed_rspan_input(
        &store,
        Uuid::from_u128(2),
        10,
        2,
        &[span_record(1, 0, 5, 9)],
    )
    .await;
    splice_input(&store, &commit_key, rspan_with_unknown_section).await;
    let trailer = ravel_rspan::footer::WriteVersion::CURRENT.number();
    assert_ne!(trailer, 1, "the record and the trailer differ");
    let before = keys_of(&store).await;

    let outcome = compact(&store, &spans_bucket()).await;

    assert_eq!(
        outcome,
        CompactionOutcome::HeldUnknownSection {
            version: trailer,
            kinds: vec![UNKNOWN_KIND],
        }
    );
    assert_eq!(keys_of(&store).await, before, "nothing is written");
}
