//! Census fixtures over a store a real fold wrote: every catalog object kind,
//! RLOG and RSPAN data objects, the tenant config, and a compaction, rewrite
//! and tombstone record. Each kind has one in-window object the census adopts
//! and one out-of-window object it refuses with a single finding naming it.

use super::tests::{T0, TestClock, config, provision, restamp_trailer, tenant, test_keys};
use super::*;
use bytes::Bytes;
use ravel_catalog::{
    Catalog, CatalogConfig, CompatView, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS, DeclaredColumnType,
    DeclaredTypedColumn, StorageLayoutWrite, TENANT_CONFIG_MAX_READ_VERSION, TenantConfig,
    TenantLifecycleState, restamp_key_index_leaf_version, set_tenant_config,
};
use ravel_commit::keys;
use ravel_commit::record::NewCommitRecord;
use ravel_logseg::key_index::KeyType;
use ravel_logseg::writer::ObjectIdentity as LogObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{PutOptions, list_all};
use ravel_proto::commit::v1 as commitproto;
use ravel_rspan::writer::ObjectIdentity as SpanObjectIdentity;
use ravel_rspan::{RspanConfig, RspanWriter, SpanRecord, StatusCode};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};
use std::sync::Arc;
use uuid::Uuid;

use crate::config::NS_PER_HOUR;

const HOUR: u32 = 470_000;
const USER_ID: &str = "user_id";

/// `now_ns` at which [`HOUR`] has sealed under the default margins.
fn now_sealed() -> i64 {
    (i64::from(HOUR) + 1) * NS_PER_HOUR
        + DEFAULT_MAX_FLUSH_LIFETIME_NS
        + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
        + DEFAULT_FOLD_SAFETY_MARGIN_NS
}

fn created() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR + 60_000_000_000
}

/// Every key-index switch on, one shard, and a metrics label leaf cap far
/// above the one-sample fixture's data bytes.
fn kidx_config() -> CatalogConfig {
    CatalogConfig {
        shard_count: 1,
        build_key_index_leaves: true,
        metrics_label_index: true,
        metrics_label_index_max_ratio_ppm: 1_000_000_000,
        ..Default::default()
    }
}

async fn put(store: &dyn ObjectStoreBackend, key: &str, bytes: impl Into<Bytes>) {
    store
        .put(key, bytes.into(), PutOptions::default())
        .await
        .expect("put");
}

async fn get_bytes(store: &dyn ObjectStoreBackend, key: &str) -> Bytes {
    store.get(key, GetRange::Full).await.expect("get").data
}

/// Store `data` and the commit record naming it.
async fn publish(store: &dyn ObjectStoreBackend, signal: Signal, data: Bytes, stamp: u32) {
    let content_hash = *blake3::hash(&data).as_bytes();
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant(1),
        signal,
        shard: 0,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: data.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: created(),
        max_event_ts_ns: created() + 10,
        min_ingest_ts_ns: created(),
        max_ingest_ts_ns: created(),
        segment_format_version: stamp,
        created_unix_ns: created(),
        ingest_hour_bucket: HOUR,
    })
    .expect("build commit record");
    put(store, &rec.object_key, data).await;
    let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
    put(store, &commit_key, record::encode(&rec)).await;
}

async fn seed_metrics(store: &dyn ObjectStoreBackend) {
    provision(store, tenant(1), Signal::Metrics).await;
    let labels = LabelSet::new(vec![
        Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "cpu_seconds".to_string(),
        },
        Label {
            name: "region".to_string(),
            value: "east".to_string(),
        },
    ])
    .expect("labels");
    let series_id =
        SeriesId::compute(&TenantId::new("acme"), "cpu_seconds", &labels).expect("series id");
    let written = SegmentWriter::write_histograms_with_exemplars(
        vec![SeriesInputV3 {
            series_id,
            labels,
            values: SeriesValues::Scalar(vec![Sample {
                ts_ns: created() + 1,
                value: 1.0,
            }]),
        }],
        SegmentIdentity {
            tenant_hash: tenant(1).0,
            shard: 0,
            writer_id: Uuid::from_u128(1).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        },
        IngestBounds {
            min_ingest_ts_ns: created(),
            max_ingest_ts_ns: created(),
        },
        Vec::new(),
    )
    .expect("write rseg");
    let stamp = u32::from(ravel_segment::SegmentVersion::CURRENT.number());
    publish(store, Signal::Metrics, written.bytes, stamp).await;
}

/// One RLOG object whose KEY_IDX section indexes `user_id`, and the tenant
/// config declaring `user_id` as a key-index column, so the fold builds a
/// logs leaf.
async fn seed_logs(store: &dyn ObjectStoreBackend) {
    provision(store, tenant(1), Signal::Logs).await;
    let mut cfg = TenantConfig {
        typed_attr_columns: Some(vec![DeclaredTypedColumn {
            key: USER_ID.to_string(),
            ty: DeclaredColumnType::I64,
        }]),
        ..TenantConfig::new(TenantLifecycleState::Active)
    };
    cfg.set_key_index_columns(
        vec![USER_ID.to_string()],
        StorageLayoutWrite::ReadersRolledOut,
    )
    .expect("declare the key index");
    set_tenant_config(store, &tenant(1), &cfg, 1)
        .await
        .expect("tenant config");

    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let mut w = RlogWriter::new(
        RlogConfig::default(),
        LogObjectIdentity {
            tenant_hash: tenant(1).0,
            shard: 0,
            writer_id: Uuid::from_u128(1).into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    )
    .with_key_index_fields(vec![(USER_ID.to_string(), KeyType::I64)]);
    for (i, user_id) in [10i64, 20].into_iter().enumerate() {
        w.push(LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1", &[]),
            stream_attrs: ravel_logseg::stream_attrs_bytes(&resource, "scope", "1", &[]),
            ts_ns: created() + i as i64,
            observed_ts_ns: created() + i as i64,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: format!("row {i}"),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![(USER_ID.to_string(), AttrValue::I64(user_id))],
        })
        .expect("push log record");
    }
    let bytes = Bytes::from(w.finish().expect("finish rlog"));
    let stamp = u32::from(ravel_logseg::footer::WriteVersion::CURRENT.number());
    publish(store, Signal::Logs, bytes, stamp).await;
}

async fn seed_spans(store: &dyn ObjectStoreBackend) {
    provision(store, tenant(1), Signal::Spans).await;
    let mut w = RspanWriter::new(
        RspanConfig::default(),
        SpanObjectIdentity {
            tenant_hash: tenant(1).0,
            shard: 0,
            writer_id: Uuid::from_u128(1).into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    w.push(SpanRecord {
        trace_id: [0x11; 16],
        span_id: [1; 8],
        parent_span_id: None,
        name: "op".into(),
        start_ts_ns: created(),
        end_ts_ns: created() + 10,
        status_code: StatusCode::Ok,
        status_message: None,
        attrs: Vec::new(),
    });
    let bytes = Bytes::from(w.finish().expect("finish rspan"));
    let stamp = u32::from(ravel_rspan::WriteVersion::CURRENT.number());
    publish(store, Signal::Spans, bytes, stamp).await;
}

async fn seed_all(store: &dyn ObjectStoreBackend) {
    seed_metrics(store).await;
    seed_logs(store).await;
    seed_spans(store).await;
}

/// A compaction, a rewrite and a retention tombstone record in a metrics
/// bucket the fold never saw. Returns their keys in that order.
async fn put_bucket_records(store: &dyn ObjectStoreBackend) -> [String; 3] {
    let hour = HOUR + 10;
    let compaction = commitproto::CompactionRecord {
        format_version: record::COMPACTION_FORMAT_VERSION,
        tenant_hash: tenant(1).0.to_vec(),
        signal: 1,
        shard: 0,
        ingest_hour_bucket: hour,
        level: 1,
        input_set_hash: vec![0x33; 32],
        ..Default::default()
    };
    let compaction_key = keys::compaction_record_key_for(&compaction).expect("key");
    put(store, &compaction_key, record::encode_compaction(&compaction)).await;

    let inputs = vec![commitproto::CompactionInputIdentity {
        writer_id: Uuid::from_u128(9).to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    }];
    let drops = vec![commitproto::RewriteDrop {
        request_id: Uuid::from_u128(1).to_string(),
        dropped_count: 1,
    }];
    let request_ids = vec![drops[0].request_id.clone()];
    let rewrite = commitproto::RewriteRecord {
        format_version: erasure::FORMAT_VERSION,
        tenant_hash: tenant(1).0.to_vec(),
        signal: 1,
        shard: 0,
        ingest_hour_bucket: hour,
        input_set_hash: erasure::compute_rewrite_input_set_hash(&inputs, None, &request_ids)
            .to_vec(),
        inputs,
        drops,
        ..Default::default()
    };
    let rewrite_key = keys::rewrite_record_key_for(&rewrite).expect("key");
    put(store, &rewrite_key, erasure::encode_rewrite(&rewrite)).await;

    let tombstone = commitproto::RetentionTombstone {
        format_version: record::TOMBSTONE_FORMAT_VERSION,
        tenant_hash: tenant(1).0.to_vec(),
        signal: 1,
        shard: 0,
        ingest_hour_bucket: hour,
        ..Default::default()
    };
    let tombstone_key = keys::retention_tombstone_key_for(&tombstone).expect("key");
    put(store, &tombstone_key, record::encode_tombstone(&tombstone)).await;
    [compaction_key, rewrite_key, tombstone_key]
}

/// Metrics, logs and spans folded with every key-index switch on, plus the
/// three bucket records.
async fn folded_store() -> (Arc<MemoryStore>, [String; 3]) {
    let mem = Arc::new(MemoryStore::with_page_size(2));
    seed_all(mem.as_ref()).await;
    let catalog = Catalog::new(mem.clone(), kidx_config()).expect("catalog");
    for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
        let report = catalog
            .fold(
                &tenant(1),
                signal,
                Uuid::from_u128(0xf01d),
                now_sealed(),
                &[],
                None,
            )
            .await
            .expect("fold");
        assert_eq!(report.key_index_leaves_written, 1, "{signal:?}: {report:?}");
    }
    let records = put_bucket_records(mem.as_ref()).await;
    (mem, records)
}

async fn keys_where(store: &dyn ObjectStoreBackend, pred: impl Fn(&str) -> bool) -> Vec<String> {
    list_all(store, &format!("t/{}/", tenant(1).to_hex()))
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.key)
        .filter(|key| pred(key))
        .collect()
}

/// The single key under `catalog/<s>/` ending in `suffix`.
async fn catalog_key(store: &dyn ObjectStoreBackend, signal: Signal, suffix: &str) -> String {
    let dir = format!("/catalog/{}/", signal.key_prefix());
    let found = keys_where(store, |k| k.contains(&dir) && k.ends_with(suffix)).await;
    assert_eq!(found.len(), 1, "{signal:?} {suffix}: {found:?}");
    found[0].clone()
}

async fn data_key(store: &dyn ObjectStoreBackend, signal: Signal) -> String {
    let dir = format!("/{}/l0/", signal.key_prefix());
    let found = keys_where(store, |k| k.contains(&dir)).await;
    assert_eq!(found.len(), 1, "{signal:?}: {found:?}");
    found[0].clone()
}

/// Set a catalog envelope's version byte and recompute its header crc, the
/// last four bytes, over magic..header.
fn restamp_envelope(bytes: &[u8], version: u8) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out[4] = version;
    let header_len = u32::from_le_bytes([out[8], out[9], out[10], out[11]]) as usize;
    let crc = crc32c::crc32c(&out[..12 + header_len]);
    let at = out.len() - 4;
    out[at..].copy_from_slice(&crc.to_le_bytes());
    out
}

/// The census refuses with exactly one finding, for `key`.
async fn assert_single_finding(
    store: &dyn ObjectStoreBackend,
    signal: Option<Signal>,
    family: &str,
    version: u32,
    key: &str,
) {
    let err = run_census(store).await.expect_err("out of window refuses");
    let CompatInitError::OutsideReadSet { findings } = err else {
        panic!("{family}: expected OutsideReadSet, got {err:?}");
    };
    assert_eq!(
        findings,
        vec![OutsideReadSet {
            tenant: tenant(1),
            signal,
            family: family.to_string(),
            version,
            count: 1,
            sample_key: key.to_string(),
        }],
        "{family}"
    );
}

/// Replace `key` with `outside`, check the census refuses it alone, and put
/// the original back.
async fn refuses_alone(
    store: &dyn ObjectStoreBackend,
    key: &str,
    outside: impl Into<Bytes>,
    signal: Option<Signal>,
    family: &str,
    version: u32,
) {
    let original = get_bytes(store, key).await;
    put(store, key, outside).await;
    assert_single_finding(store, signal, family, version, key).await;
    put(store, key, original).await;
}

/// Every kind the census reads, from a real fold: the clean store is adopted
/// whole, at the exact histogram below, and each object restamped out of
/// its window refuses alone, naming its key.
#[tokio::test]
async fn every_census_kind_adopts_in_window_and_refuses_out_of_window() {
    let (mem, [compaction_key, rewrite_key, tombstone_key]) = folded_store().await;
    let store = mem.as_ref();

    let census = run_census(store).await.expect("the folded store is in window");
    let rseg = u32::from(ravel_segment::SegmentVersion::CURRENT.number());
    let rlog = u32::from(ravel_logseg::footer::WriteVersion::CURRENT.number());
    let rspan = u32::from(ravel_rspan::WriteVersion::CURRENT.number());
    let adopted: BTreeMap<(String, u32), u64> = [
        ((FAMILY_RSEG, rseg), 1),
        ((FAMILY_RLOG, rlog), 1),
        ((FAMILY_RSPAN, rspan), 1),
        ((FAMILY_CATALOG_HEAD, ravel_catalog::HEAD_FORMAT_VERSION_1), 3),
        ((FAMILY_CSNAP, u32::from(ravel_catalog::VERSION)), 3),
        // Postings are built for metrics only.
        ((FAMILY_NPOST, 1), 1),
        ((FAMILY_CSTAT, 3), 3),
        ((FAMILY_KIDX, KEY_INDEX_LEAF_FORMAT_VERSION), 3),
        ((FAMILY_COMMIT_RECORD, record::FORMAT_VERSION), 3),
        ((FAMILY_COMPACTION_RECORD, record::COMPACTION_FORMAT_VERSION), 1),
        ((FAMILY_REWRITE_RECORD, erasure::FORMAT_VERSION), 1),
        ((FAMILY_RETENTION_TOMBSTONE, record::TOMBSTONE_FORMAT_VERSION), 1),
    ]
    .into_iter()
    .map(|((family, version), count)| ((family.to_string(), version), count))
    .collect();
    assert_eq!(census.adopted, adopted);

    // RLOG and RSPAN: the trailer version.
    for (signal, family) in [(Signal::Logs, FAMILY_RLOG), (Signal::Spans, FAMILY_RSPAN)] {
        let key = data_key(store, signal).await;
        restamp_trailer(store, &key, u16::MAX).await;
        assert_single_finding(store, Some(signal), family, u32::from(u16::MAX), &key).await;
        let current = u16::try_from(if signal == Signal::Logs { rlog } else { rspan })
            .expect("u16 version");
        restamp_trailer(store, &key, current).await;
    }

    // Catalog HEAD and each part kind, per signal.
    for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
        let head_key = catalog_key(store, signal, "/HEAD").await;
        let mut head =
            ravel_proto::catalog::v1::SnapshotHead::decode(get_bytes(store, &head_key).await)
                .expect("head");
        head.format_version = 99;
        let s = Some(signal);
        refuses_alone(
            store,
            &head_key,
            head.encode_to_vec(),
            s,
            FAMILY_CATALOG_HEAD,
            99,
        )
        .await;

        let csnap = catalog_key(store, signal, ".csnap").await;
        let next = ravel_catalog::VERSION + 1;
        let bytes = restamp_envelope(&get_bytes(store, &csnap).await, next);
        refuses_alone(store, &csnap, bytes, None, FAMILY_CSNAP, u32::from(next)).await;

        if signal == Signal::Metrics {
            let npost = catalog_key(store, signal, ".npost").await;
            let bytes = restamp_envelope(&get_bytes(store, &npost).await, 2);
            refuses_alone(store, &npost, bytes, s, FAMILY_NPOST, 2).await;
        }

        let cstat = catalog_key(store, signal, ".cstat").await;
        let bytes = restamp_envelope(&get_bytes(store, &cstat).await, 4);
        refuses_alone(store, &cstat, bytes, None, FAMILY_CSTAT, 4).await;

        let kidx = catalog_key(store, signal, ".kidx").await;
        let leaf = restamp_key_index_leaf_version(&get_bytes(store, &kidx).await, 4)
            .expect("restamp leaf");
        refuses_alone(store, &kidx, leaf.bytes, s, FAMILY_KIDX, 4).await;
    }

    // The tenant config.
    let config_key = ravel_catalog::config_key(&tenant(1));
    let mut rec = sysproto::TenantConfigRecord::decode(get_bytes(store, &config_key).await)
        .expect("tenant config");
    let next = TENANT_CONFIG_MAX_READ_VERSION + 1;
    rec.format_version = next;
    refuses_alone(
        store,
        &config_key,
        rec.encode_to_vec(),
        None,
        WHAT_TENANT_CONFIG,
        next,
    )
    .await;

    // The compaction, rewrite and tombstone records.
    let m = Some(Signal::Metrics);
    let mut rec = commitproto::CompactionRecord::decode(get_bytes(store, &compaction_key).await)
        .expect("compaction");
    rec.format_version = 99;
    let bytes = rec.encode_to_vec();
    refuses_alone(store, &compaction_key, bytes, m, FAMILY_COMPACTION_RECORD, 99).await;
    let mut rec = commitproto::RewriteRecord::decode(get_bytes(store, &rewrite_key).await)
        .expect("rewrite");
    rec.format_version = 99;
    let bytes = rec.encode_to_vec();
    refuses_alone(store, &rewrite_key, bytes, m, FAMILY_REWRITE_RECORD, 99).await;
    let mut rec = commitproto::RetentionTombstone::decode(get_bytes(store, &tombstone_key).await)
        .expect("tombstone");
    rec.format_version = 99;
    let bytes = rec.encode_to_vec();
    refuses_alone(store, &tombstone_key, bytes, m, FAMILY_RETENTION_TOMBSTONE, 99).await;

    // Every object is back in window.
    assert_eq!(run_census(store).await.expect("restored").adopted, adopted);
}

/// An in-window RLOG or RSPAN object is checked through its footer: the
/// trailer-sized suffix GET names a footer range, and the census issues that
/// second GET. Failing exactly the second GET on the key fails the census.
#[tokio::test]
async fn an_in_window_rlog_and_rspan_footer_is_read_through_a_range() {
    let (mem, _) = folded_store().await;
    for signal in [Signal::Logs, Signal::Spans] {
        let key = data_key(mem.as_ref(), signal).await;
        let store = FaultStore::new(
            mem.clone(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Get, ScriptedFault::Permanent("footer range".into()))
                    .with_key_contains(key.clone())
                    .with_occurrence(Occurrence::Nth(2)),
            ),
        );
        let err = run_census(&store).await.expect_err("the range GET fails");
        assert!(
            matches!(&err, CompatInitError::Get { key: k, .. } if *k == key),
            "{signal:?}: {err:?}"
        );
        assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
    }
}

/// The kidx read set is what the decoder parses for the signal: a logs
/// version-1 leaf (decoded, its part treated as uncovered) and a metrics
/// version-3 leaf are adopted; a spans version-3 leaf is outside.
#[tokio::test]
async fn kidx_census_reads_every_version_the_signal_decodes() {
    let (mem, _) = folded_store().await;
    let store = mem.as_ref();
    let restamp = |key: String, version: u32| async move {
        let leaf = restamp_key_index_leaf_version(&get_bytes(store, &key).await, version)
            .expect("restamp leaf");
        put(store, &key, leaf.bytes).await;
        key
    };
    restamp(catalog_key(store, Signal::Logs, ".kidx").await, 1).await;
    restamp(catalog_key(store, Signal::Metrics, ".kidx").await, 3).await;
    let census = run_census(store).await.expect("logs v1 and metrics v3 pass");
    let kidx: Vec<(u32, u64)> = census
        .adopted
        .iter()
        .filter(|((family, _), _)| family == FAMILY_KIDX)
        .map(|((_, version), count)| (*version, *count))
        .collect();
    assert_eq!(kidx, vec![(1, 1), (2, 1), (3, 1)]);

    let spans = restamp(catalog_key(store, Signal::Spans, ".kidx").await, 3).await;
    assert_single_finding(store, Some(Signal::Spans), FAMILY_KIDX, 3, &spans).await;
}

/// Init on a seeded, never-folded store, then a fold of each signal through
/// a policy read from the record init wrote: the record names versions the
/// fold writes, so no signal is refused and each writes its leaf.
///
/// The one tolerated refusal is metrics at kidx while this build's
/// `fold_write_version(kidx, metrics)` names a version above the leaf
/// constant, which the production fold does not write (fixed by #4311); the
/// test becomes strict once that table names the constant.
#[tokio::test]
async fn init_then_fold_writes_every_signal_under_the_baseline_record() {
    let mem = Arc::new(MemoryStore::with_page_size(2));
    seed_all(mem.as_ref()).await;
    let report = init_compat(mem.as_ref(), Some(&test_keys()), &TestClock(T0), &config())
        .await
        .expect("init");
    assert_eq!(report.outcome, InitOutcome::Created);

    let now = now_sealed();
    let view = CompatView::new(mem.clone(), test_keys(), Arc::new(TestClock(now)));
    view.refresh().await.expect("refresh");
    let metrics_kidx_tolerated = fold_write_version(FAMILY_KIDX, Signal::Metrics)
        != u16::try_from(KEY_INDEX_LEAF_FORMAT_VERSION).ok();
    for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
        let catalog = Catalog::new(mem.clone(), kidx_config())
            .expect("catalog")
            .with_compat_policy(Some(view.clone()));
        let gate = catalog.fold_compat_versions(signal, now);
        let folded = catalog
            .fold(&tenant(1), signal, Uuid::from_u128(0xf01d), now, &[], None)
            .await;
        if signal == Signal::Metrics && metrics_kidx_tolerated {
            let refusal = gate.expect_err("metrics kidx is above the record");
            assert_eq!(refusal.family, FAMILY_KIDX, "{refusal}");
            assert!(folded.is_err(), "{folded:?}");
            continue;
        }
        gate.unwrap_or_else(|refusal| panic!("{signal:?}: {refusal}"));
        let folded = folded.unwrap_or_else(|err| panic!("{signal:?}: {err}"));
        assert_eq!(folded.key_index_leaves_written, 1, "{signal:?}: {folded:?}");
        let kidx = catalog_key(mem.as_ref(), signal, ".kidx").await;
        assert_eq!(
            key_index_leaf_format_version(&get_bytes(mem.as_ref(), &kidx).await),
            Ok(KEY_INDEX_LEAF_FORMAT_VERSION),
            "{signal:?}"
        );
    }
}
