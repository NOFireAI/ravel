//! `ravel-cli catalog fold --key-index-leaves` (ADR-2707 decision 4): the
//! flag reaches the catalog the fold runs with, and without it the fold reads
//! no KEY_IDX section and writes no leaf.
//!
//! Driven in-process against one `MemoryStore` per case, for the reason
//! `tests/catalog_signal.rs` gives.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanRecord, StatusCode};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

const TENANT: &str = "cli-fold-key-index-leaves";

/// Old enough that the default seal margin seals its hour whatever minute the
/// test runs in (the same constant as `tests/catalog_signal.rs`).
const SEALED_AGE_NS: i64 = 3 * NS_PER_HOUR;

/// Publishes one RSPAN object, which always carries a KEY_IDX section over
/// `trace_id`, with one span per trace id.
async fn publish_spans(store: &MemoryStore, now_ns: i64) {
    publish_trace_spans(store, 1, now_ns - SEALED_AGE_NS, &[1, 2, 3]).await;
}

/// Publishes RSPAN object `writer_seq` at `base_ts` with one span per id
/// `i` in `ids`, trace id `[i; 16]`, so its key8 is `[i; 8]`.
async fn publish_trace_spans(store: &MemoryStore, writer_seq: u64, base_ts: i64, ids: &[u8]) {
    let tenant_hash = TenantId::new(TENANT).hash();
    let writer_id = Uuid::from_u128(u128::from(writer_seq));
    let mut writer = RspanWriter::new(
        RspanConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq,
        },
    );
    for (n, &i) in ids.iter().enumerate() {
        writer.push(SpanRecord {
            trace_id: [i; 16],
            span_id: [i; 8],
            parent_span_id: None,
            name: "op".to_string(),
            start_ts_ns: base_ts + n as i64,
            end_ts_ns: base_ts + n as i64 + 10,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: Vec::new(),
        });
    }
    let bytes = Bytes::from(writer.finish().expect("finish"));
    let commit = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: ids.len() as u64,
        series_count: 1,
        min_event_ts_ns: base_ts,
        max_event_ts_ns: base_ts + 100,
        min_ingest_ts_ns: base_ts,
        max_ingest_ts_ns: base_ts + 100,
        segment_format_version: 1,
        created_unix_ns: base_ts,
        ingest_hour_bucket: u32::try_from(base_ts.div_euclid(NS_PER_HOUR)).expect("fits u32"),
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&commit).expect("data key");
    publish::put_data_object(store, &data_key, bytes)
        .await
        .expect("put data object");
    publish::publish(store, &commit, &RetryPolicy::default())
        .await
        .expect("publish");
}

async fn kidx_objects(store: &MemoryStore) -> usize {
    let prefix = format!(
        "t/{}/catalog/{}/",
        TenantId::new(TENANT).hash().to_hex(),
        Signal::Spans.key_prefix()
    );
    list_all(store, &prefix)
        .await
        .expect("list the catalog prefix")
        .iter()
        .filter(|meta| meta.key.ends_with(".kidx"))
        .count()
}

async fn fold_spans(key_index_leaves: bool) -> (ravel_catalog::FoldReport, usize) {
    let now = ravel_cli::now_ns().expect("system clock readable");
    let store = Arc::new(MemoryStore::new());
    publish_spans(&store, now).await;
    let (report, _printed) = catalog::fold_with_writers_stopped(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        TENANT,
        1,
        SignalArg::Spans,
        None,
        false,
        key_index_leaves,
        now,
        true,
    )
    .await
    .expect("fold succeeds");
    assert_eq!(report.entry_count, 1, "the spans object is folded");
    (report, kidx_objects(&store).await)
}

/// Without the flag a spans fold, whose objects always carry a KEY_IDX
/// section, issues no section read and writes no leaf; with it, the same
/// fold reads the section and publishes one leaf.
#[tokio::test]
async fn the_key_index_leaves_flag_reaches_the_fold() {
    let (off, off_leaves) = fold_spans(false).await;
    assert_eq!(off.key_index_ranged_gets, 0, "off: no KEY_IDX read");
    assert_eq!(off.key_index_leaves_written, 0, "off: no leaf");
    assert_eq!(off_leaves, 0, "off: no .kidx object");
    assert_eq!(
        (
            off.key_index_sections_read,
            off.key_index_entries_carried,
            off.key_index_build_passes,
            off.key_index_leaf_gets,
        ),
        (0, 0, 0, 0),
        "off: no build figure moves"
    );

    let (on, on_leaves) = fold_spans(true).await;
    assert!(on.key_index_ranged_gets > 0, "on: the section is read");
    assert_eq!(on.key_index_leaves_written, 1, "on: one leaf");
    assert_eq!(on_leaves, 1, "on: one .kidx object");
}

async fn fold_spans_with_budget(max_pairs: u64) -> (ravel_catalog::FoldReport, String) {
    let now = ravel_cli::now_ns().expect("system clock readable");
    let store = Arc::new(MemoryStore::new());
    publish_spans(&store, now).await;
    catalog::fold_with_key_index_budget(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        TENANT,
        1,
        SignalArg::Spans,
        None,
        false,
        true,
        max_pairs,
        None,
        now,
        false,
    )
    .await
    .expect("fold succeeds")
}

/// `--key-index-build-max-pairs` (ADR-3692 decision 5) reaches the fold: the
/// one entry's three trace ids are three (entry, key8) pairs, so a budget of
/// 1 builds the field in ceil(3 / 1) = 3 passes, each reading the entry's
/// section and closing its own leaf, where the default budget builds one
/// leaf in one pass. The text report prints each of the four build figures.
#[tokio::test]
async fn the_pair_budget_reaches_the_fold_and_the_report_prints_the_build_figures() {
    let (one_pass, one_pass_printed) =
        fold_spans_with_budget(ravel_catalog::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS).await;
    assert_eq!(one_pass.key_index_build_passes, 1, "default: one pass");
    assert_eq!(one_pass.key_index_sections_read, 1, "default: one read");
    assert_eq!(one_pass.key_index_leaves_written, 1, "default: one leaf");

    let (budgeted, printed) = fold_spans_with_budget(1).await;
    assert_eq!(budgeted.key_index_build_passes, 3, "budget 1: three passes");
    assert_eq!(budgeted.key_index_sections_read, 3, "one read per pass");
    assert_eq!(
        budgeted.key_index_entries_carried, 0,
        "a first fold carries none"
    );
    assert_eq!(budgeted.key_index_leaf_gets, 0, "no previous tail leaf");
    assert_eq!(budgeted.key_index_leaves_written, 3, "one leaf per pass");

    for line in [
        "key_index_sections_read: 3\n",
        "key_index_entries_carried: 0\n",
        "key_index_build_passes: 3\n",
        "key_index_leaf_gets: 0\n",
    ] {
        assert_eq!(printed.matches(line).count(), 1, "printed once: {line:?}");
    }
    assert_eq!(
        one_pass_printed
            .matches("key_index_build_passes: 1\n")
            .count(),
        1,
        "the default run prints its own pass count"
    );
}

/// An advancing fold's four build figures, each different: three spans
/// objects of two trace ids each are folded two hours ago, which leaves one
/// tail part with one leaf; two later objects of two trace ids each are then
/// folded now under a budget of 5 pairs. That fold re-encodes the tail, GETs
/// its one previous leaf, carries the three old entries, and holds 10 pairs
/// in two passes of 5, reading the two new sections in each and closing one
/// leaf each: 4 sections, 3 entries carried, 2 passes and 1 leaf GET, each
/// printed once, and 2 leaves.
#[tokio::test]
async fn an_advancing_fold_reports_four_different_build_figures() {
    let now = ravel_cli::now_ns().expect("system clock readable");
    let store = Arc::new(MemoryStore::new());
    for (seq, ids) in [(1u64, [1u8, 2]), (2, [3, 4]), (3, [5, 6])] {
        publish_trace_spans(&store, seq, now - 6 * NS_PER_HOUR, &ids).await;
    }
    let fold = |now_ns: i64, max_pairs: u64| {
        catalog::fold_with_key_index_budget(
            Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
            MEMORY,
            TENANT,
            1,
            SignalArg::Spans,
            None,
            false,
            true,
            max_pairs,
            None,
            now_ns,
            false,
        )
    };
    let (first, _) = fold(
        now - 2 * NS_PER_HOUR,
        ravel_catalog::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS,
    )
    .await
    .expect("first fold");
    assert_eq!(first.entry_count, 3, "{first:?}");
    assert_eq!(first.key_index_leaves_written, 1, "{first:?}");
    for (seq, ids) in [(4u64, [7u8, 8]), (5, [9, 10])] {
        publish_trace_spans(&store, seq, now - 3 * NS_PER_HOUR, &ids).await;
    }
    let (second, printed) = fold(now, 5).await.expect("second fold");
    assert_eq!(second.entry_count, 5, "{second:?}");
    assert_eq!(second.key_index_sections_read, 4, "{second:?}");
    assert_eq!(second.key_index_entries_carried, 3, "{second:?}");
    assert_eq!(second.key_index_build_passes, 2, "{second:?}");
    assert_eq!(second.key_index_leaf_gets, 1, "{second:?}");
    assert_eq!(second.key_index_leaves_written, 2, "{second:?}");
    for line in [
        "key_index_sections_read: 4\n",
        "key_index_entries_carried: 3\n",
        "key_index_build_passes: 2\n",
        "key_index_leaf_gets: 1\n",
    ] {
        assert_eq!(printed.matches(line).count(), 1, "printed once: {line:?}");
    }
}
