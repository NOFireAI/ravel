//! `ravel-cli catalog fold` prints the exact dictionary-drop count when the
//! degrade loop fires (#1598 deliverable 1). Replicates
//! `crates/ravel-catalog/src/fold.rs`'s own
//! `fold_drops_the_largest_dictionaries_until_a_part_fits_the_ceiling`
//! fixture shape and ceiling-derivation technique, driven through
//! `ravel-cli`'s own entry point (`catalog::fold_inner`) rather than the
//! crate-internal `Catalog::fold`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_catalog::{
    ColumnStatsLimits, DeclaredColumnType, DeclaredTypedColumn, TenantConfig, TenantLifecycleState,
    column_stats_segments_concat, decode_column_stats, decode_head, set_tenant_config,
};
use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::logstream::AttrValue;
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

use prost::Message as _;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const SEALED_AGE_NS: i64 = 3 * NS_PER_HOUR;
const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

fn now_ns() -> i64 {
    ravel_cli::now_ns().expect("system clock readable")
}

fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

/// Declares `col_a`/`col_b`/`col_c` as typed I64 columns, matching
/// `fold.rs`'s `set_three_column_config` fixture.
async fn set_three_column_config(store: &dyn ObjectStoreBackend, tenant: &TenantHash) {
    let cfg = TenantConfig {
        typed_attr_columns: Some(vec![
            DeclaredTypedColumn {
                key: "col_a".to_string(),
                ty: DeclaredColumnType::I64,
            },
            DeclaredTypedColumn {
                key: "col_b".to_string(),
                ty: DeclaredColumnType::I64,
            },
            DeclaredTypedColumn {
                key: "col_c".to_string(),
                ty: DeclaredColumnType::I64,
            },
        ]),
        ..TenantConfig::new(TenantLifecycleState::Active)
    };
    set_tenant_config(store, tenant, &cfg, 1)
        .await
        .expect("write tenant config");
}

/// Publishes one L0 logs segment of `rows` entries, each carrying
/// `col_a`/`col_b`/`col_c` with deliberately distinct cardinality (`col_a`
/// all-distinct, `col_b` 30 distinct, `col_c` 3 distinct) so their
/// per-column dictionaries encode to distinct sizes, matching `fold.rs`'s
/// `publish_logs_segment_wide` fixture.
async fn publish_logs_segment_wide(
    store: &MemoryStore,
    tenant: &TenantHash,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    rows: usize,
) {
    let writer_id = Uuid::new_v4();
    let base_ts = i64::from(ingest_hour_bucket) * NS_PER_HOUR + 60_000_000_000;
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let resource = [(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);
    let mut w = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq,
        },
    );
    for i in 0..rows {
        let ts = base_ts + i as i64;
        min_ts = min_ts.min(ts);
        max_ts = max_ts.max(ts);
        w.push(LogRecord {
            stream_id: ravel_logseg::LogStreamId([0u8; 16]),
            stream_attrs: stream_attrs.clone(),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: format!("row {i}"),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![
                ("col_a".to_string(), AttrValue::I64(i as i64)),
                ("col_b".to_string(), AttrValue::I64((i % 30) as i64)),
                ("col_c".to_string(), AttrValue::I64((i % 3) as i64)),
            ],
        })
        .expect("push");
    }
    let bytes = w.finish().expect("finish");
    let content_hash = *blake3::hash(&bytes).as_bytes();
    let record = record::build(NewCommitRecord {
        tenant_hash: *tenant,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: rows as u64,
        series_count: 1,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: max_ts,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(bytes))
        .await
        .expect("put data object");
    publish::publish(store, &record, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Deliverable 1 (#1598): `column_stats_dictionaries_dropped` is printed and
/// exact, mirroring `crates/ravel-catalog/src/fold.rs`'s
/// `fold_drops_the_largest_dictionaries_until_a_part_fits_the_ceiling`: a
/// reference fold measures each column's dictionary size, a ceiling is
/// derived (via the crate-root-exported `column_stats_segments_concat`) that
/// only the two largest dictionaries fit under, and a fresh fold against
/// that injected ceiling drops exactly those two.
///
/// Prove-the-test: delete the `column_stats_dictionaries_dropped` `push_str`
/// line from `render_fold_report` (`services/ravel-cli/src/catalog.rs`) and
/// the final assertion on `printed` below fails.
#[tokio::test]
async fn fold_prints_the_exact_dictionaries_dropped_count() {
    let rows = 300;
    let tenant_name = "cli-cstat-degrade";
    let tenant_hash = TenantId::new(tenant_name).hash();
    let created = now_ns() - SEALED_AGE_NS;
    let ingest_hour_bucket = u32::try_from(created / NS_PER_HOUR).expect("fits u32");

    // Reference fold at the real (unbounded-for-this-fixture) ceiling: every
    // dictionary intact.
    let reference_store = Arc::new(MemoryStore::new());
    set_three_column_config(reference_store.as_ref(), &tenant_hash).await;
    publish_logs_segment_wide(
        reference_store.as_ref(),
        &tenant_hash,
        0,
        ingest_hour_bucket,
        rows,
    )
    .await;
    let (reference_report, _) = catalog::fold(
        reference_store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant_name,
        1,
        SignalArg::Logs,
        None,
        now_ns(),
        false,
    )
    .await
    .expect("reference fold at the real ceiling never degrades");
    assert_eq!(
        reference_report.column_stats_dictionaries_dropped, 0,
        "the real ceiling admits this small fixture with every dictionary intact"
    );

    let head_bytes = reference_store
        .get(&head_key(&tenant_hash, Signal::Logs), GetRange::Full)
        .await
        .expect("head present")
        .data;
    let reference_head = decode_head(&head_bytes).expect("head decodes");
    let reference_ref = reference_head.parts[0]
        .column_stats
        .clone()
        .expect("v3 ref");
    let reference_got = reference_store
        .get(&reference_ref.key, GetRange::Full)
        .await
        .expect("v3 object present");
    let reference_decoded = decode_column_stats(&reference_got.data, &ColumnStatsLimits::default())
        .expect("v3 decodes");
    let reference_columns = reference_decoded.segments[0].columns.clone();

    let mut sizes: Vec<(usize, usize)> = reference_columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let size: usize = c.dictionary.iter().map(|e| e.encoded_len()).sum();
            (i, size)
        })
        .collect();
    sizes.sort_by_key(|(_, size)| std::cmp::Reverse(*size));
    let largest_idx = sizes[0].0;
    let second_idx = sizes[1].0;
    assert_eq!(reference_columns[largest_idx].name, "col_a");
    assert_eq!(reference_columns[second_idx].name, "col_b");

    let mut post_drop_segments = reference_decoded.segments.clone();
    post_drop_segments[0].columns[largest_idx].dictionary_present = false;
    post_drop_segments[0].columns[largest_idx]
        .dictionary
        .clear();
    post_drop_segments[0].columns[second_idx].dictionary_present = false;
    post_drop_segments[0].columns[second_idx].dictionary.clear();
    let ceiling = column_stats_segments_concat(&post_drop_segments).len() as u64;

    // Fresh store, byte-identical fixture, ceiling injected via
    // `fold_inner`'s test-only seam.
    let store = Arc::new(MemoryStore::new());
    set_three_column_config(store.as_ref(), &tenant_hash).await;
    publish_logs_segment_wide(store.as_ref(), &tenant_hash, 0, ingest_hour_bucket, rows).await;
    let (report, printed) = catalog::fold_inner(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant_name,
        1,
        SignalArg::Logs,
        None,
        now_ns(),
        false,
        Some(ceiling),
    )
    .await
    .expect("an over-ceiling part degrades rather than refusing");

    assert_eq!(
        report.column_stats_dictionaries_dropped, 2,
        "exactly the two largest dictionaries are dropped"
    );
    assert!(
        printed.contains("column_stats_dictionaries_dropped: 2\n"),
        "the printed report must carry the exact drop count:\n{printed}"
    );
}
