//! The log flush writes each RLOG object with the tenant's clustering key and
//! bloom scope (ADR-2135 decisions 1 and 5, issue #2142). Every test drives
//! `LogIngestRouter::write` through a real flush, follows the commit record to
//! the data object, and reads the object's footer, row order, and BLOOM
//! section back.
//!
//! The storage layout lives in config record fields 13 and 14, which only a
//! format-version-3 record carries, so each test writes the raw record itself.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use ravel_catalog::config_key;
use ravel_commit::keys;
use ravel_commit::record;
use ravel_ingest::{IngestConfig, LogIngestRouter, TenantCount, WriteMode};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{
    LogFooter, SortBucketWidth, SortDescriptor, SortKeyColumn, SortKeyType, kind, open,
};
use ravel_logseg::record::{COL_BODY, COL_SEVERITY_TEXT};
use ravel_logseg::rlog_bloom::RlogBloomSection;
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::{
    FieldType, LogRecord, ObjectIdentity, Predicate, RlogConfig, RlogReader, RlogWriter,
    read_section,
};
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence, SequenceStep};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_proto::sys::v1::{
    BloomScope, ClusteringBucketWidth, ClusteringKeyConfig, TenantConfigRecord,
    TenantLifecycleState, TypedAttrColumn, TypedAttrColumnConfig, TypedAttrColumnType,
};
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{CommitToken, Signal, TenantHash, TenantId};

mod common;
use common::TestClock;

const BASE_NS: i64 = 1_700_000_000_000_000_000;
const HOUR: i64 = 3_600_000_000_000;
const SIX_HOURS: i64 = 6 * HOUR;
/// The start of the six-hour bucket holding `BASE_NS`.
const T0: i64 = (BASE_NS / SIX_HOURS) * SIX_HOURS;
/// Past the overlay's 60 s refresh horizon.
const PAST_HORIZON_NS: i64 = 61_000_000_000;

fn tenant() -> TenantId {
    TenantId::new("clustered")
}
fn tenant_hash() -> TenantHash {
    tenant().hash()
}

/// Flushes on the first write and never on age, so each strict write drives
/// exactly one complete flush inline.
fn flush_on_first() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

/// One record on a fixed stream; `body` labels it in the scanned row order.
fn rec(ts_ns: i64, body: &str, attrs: Vec<(&str, AttrValue)>) -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &scope_attrs),
        stream_attrs: ravel_logseg::stream_attrs_bytes(&res, "scope", "", &scope_attrs),
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
    }
}

fn s(v: &str) -> AttrValue {
    AttrValue::Str(v.to_string())
}

/// A format-version-3 config record declaring `typed` and carrying `key` and
/// `scope` as fields 13 and 14.
fn layout_record(
    typed: &[(&str, TypedAttrColumnType)],
    key: Option<ClusteringKeyConfig>,
    scope: BloomScope,
) -> TenantConfigRecord {
    TenantConfigRecord {
        format_version: 3,
        tenant_hash: tenant_hash().0.to_vec(),
        lifecycle_state: TenantLifecycleState::Active as i32,
        typed_attr_columns: (!typed.is_empty()).then(|| TypedAttrColumnConfig {
            columns: typed
                .iter()
                .map(|(k, ty)| TypedAttrColumn {
                    key: k.to_string(),
                    r#type: *ty as i32,
                })
                .collect(),
        }),
        clustering_key: key,
        bloom_scope: scope as i32,
        created_unix_ns: 1,
        updated_unix_ns: 1,
        ..Default::default()
    }
}

fn key(columns: &[&str], width: ClusteringBucketWidth, generation: u64) -> ClusteringKeyConfig {
    ClusteringKeyConfig {
        columns: columns.iter().map(|c| c.to_string()).collect(),
        bucket_width: width as i32,
        generation,
    }
}

async fn put_record(store: &dyn ObjectStoreBackend, rec: &TenantConfigRecord) {
    store
        .put(
            &config_key(&tenant_hash()),
            rec.encode_to_vec().into(),
            PutOptions::default(),
        )
        .await
        .expect("put config record");
}

/// One strict write of `records`, returning its single commit token.
async fn write(router: &LogIngestRouter, records: Vec<NormalizedLogRecord>) -> CommitToken {
    let receipt = router
        .write(tenant(), records, WriteMode::Strict, Duration::from_secs(5))
        .await
        .expect("strict write flushes");
    assert_eq!(receipt.tokens.len(), 1, "one object for one flush");
    receipt.tokens[0].clone()
}

/// The data object the commit record for `token` names.
async fn object_for(store: &dyn ObjectStoreBackend, token: &CommitToken) -> Vec<u8> {
    let commit_key =
        keys::commit_key_for_token(&tenant_hash(), Signal::Logs, token).expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let commit = record::decode(&commit_bytes).expect("decode commit record");
    store
        .get(&commit.object_key, GetRange::Full)
        .await
        .expect("get data object")
        .data
        .to_vec()
}

/// A single flush of `records` under `config`, returning the object.
async fn flush_one(
    config: Option<&TenantConfigRecord>,
    records: Vec<NormalizedLogRecord>,
) -> Vec<u8> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    if let Some(config) = config {
        put_record(store.as_ref(), config).await;
    }
    let router = LogIngestRouter::new(
        flush_on_first(),
        Arc::clone(&store),
        TestClock::new(BASE_NS),
    );
    let token = write(&router, records).await;
    router.shutdown().await;
    object_for(store.as_ref(), &token).await
}

fn footer(object: &[u8]) -> LogFooter {
    open(object).expect("open footer")
}

fn bodies(object: &[u8]) -> Vec<String> {
    let reader = RlogReader::new(object, &RlogConfig::default()).expect("reader");
    let (records, _) = reader.scan(&Predicate::And(vec![])).expect("scan");
    records.into_iter().map(|r| r.body).collect()
}

/// The object's FIELD_DIR and raw BLOOM section.
fn dir_and_bloom(object: &[u8]) -> (FieldDir, Vec<u8>) {
    let ftr = footer(object);
    let cfg = RlogConfig::default();
    let dir_raw = read_section(
        object,
        ftr.section(kind::FIELD_DIR).expect("FIELD_DIR"),
        &cfg,
    )
    .expect("read FIELD_DIR");
    let dir = FieldDir::decode(&dir_raw, u64::MAX).expect("decode FIELD_DIR");
    let bloom =
        read_section(object, ftr.section(kind::BLOOM).expect("BLOOM"), &cfg).expect("read BLOOM");
    (dir, bloom)
}

fn str_col(dir: &FieldDir, name: &str) -> u32 {
    dir.column(name, FieldType::Str).expect(name).column_id
}

/// Every bloom key the writer derives from `text` for one column.
fn keys_of(text: &str) -> Vec<Vec<u8>> {
    let mut keys = tokens(text);
    keys.push(text.as_bytes().to_vec());
    keys
}

/// Six records on one stream, pushed as a, d, c, e, f, b. Unkeyed they store
/// in ts order b, a, c, f, e, d. Under a `[region: str, user: i64]` key with
/// six-hour buckets they store as f, b, e, c, a, d, and under a `[user: i64]`
/// key with one-day buckets as f, b, e, a, c, d.
fn clustering_records() -> Vec<NormalizedLogRecord> {
    let r = |ts, body, region: Option<&str>, user| {
        let mut attrs = vec![("user", AttrValue::I64(user))];
        if let Some(region) = region {
            attrs.push(("region", s(region)));
        }
        rec(ts, body, attrs)
    };
    vec![
        r(T0 + 2 * HOUR, "a", Some("west"), 10),
        r(T0 + 6 * HOUR + 1, "d", Some("a"), 1),
        r(T0 + 3 * HOUR, "c", Some("east"), 10),
        r(T0 + 5 * HOUR, "e", Some("east"), 9),
        r(T0 + 4 * HOUR, "f", None, 5),
        r(T0 + HOUR, "b", Some("east"), 9),
    ]
}

const REGION_USER: &[(&str, TypedAttrColumnType)] = &[
    ("region", TypedAttrColumnType::Str),
    ("user", TypedAttrColumnType::I64),
];

#[tokio::test]
async fn flush_records_the_tenant_clustering_key_and_generation() {
    let config = layout_record(
        REGION_USER,
        Some(key(&["region", "user"], ClusteringBucketWidth::SixHours, 5)),
        BloomScope::All,
    );
    let object = flush_one(Some(&config), clustering_records()).await;

    let ftr = footer(&object);
    assert_eq!(
        ftr.sort_descriptor,
        Some(SortDescriptor {
            bucket_width: SortBucketWidth::SixHours,
            key_columns: vec![
                SortKeyColumn {
                    name: "region".to_string(),
                    ty: SortKeyType::Str,
                },
                SortKeyColumn {
                    name: "user".to_string(),
                    ty: SortKeyType::I64,
                },
            ],
        })
    );
    assert_eq!(ftr.clustering_generation, 5);
    assert_eq!(bodies(&object), ["f", "b", "e", "c", "a", "d"]);
}

#[tokio::test]
async fn cleared_key_writes_no_descriptor_and_the_generation() {
    let cleared = ClusteringKeyConfig {
        columns: vec![],
        bucket_width: ClusteringBucketWidth::Unspecified as i32,
        generation: 3,
    };
    let config = layout_record(REGION_USER, Some(cleared), BloomScope::All);
    let object = flush_one(Some(&config), clustering_records()).await;

    let ftr = footer(&object);
    assert_eq!(ftr.sort_descriptor, None);
    assert_eq!(ftr.clustering_generation, 3);
    // No descriptor: the unkeyed (stream, ts) order.
    assert_eq!(bodies(&object), ["b", "a", "c", "f", "e", "d"]);
}

/// Four records whose `note` and `region` string columns each carry words
/// found nowhere else in the object.
fn bloom_records() -> Vec<NormalizedLogRecord> {
    (0..4)
        .map(|i| {
            rec(
                BASE_NS + i,
                &format!("request alpha{i}"),
                vec![
                    ("note", s(&format!("needle{i} haystack"))),
                    ("region", s("westcoast")),
                ],
            )
        })
        .collect()
}

fn notes() -> Vec<String> {
    (0..4).map(|i| format!("needle{i} haystack")).collect()
}

/// Asserts no BLOOM entry holds any key of `values` for column `cid`, and
/// returns how many probes that took.
fn assert_absent(section: &RlogBloomSection, cid: u32, values: &[String]) -> usize {
    let mut probed = 0;
    for v in values {
        for key in keys_of(v) {
            for i in 0..section.len() {
                assert!(
                    !section.entry(i).expect("entry").may_contain(cid, &key),
                    "entry {i} holds uncovered column {cid} key {:?}",
                    String::from_utf8_lossy(&key)
                );
                probed += 1;
            }
        }
    }
    probed
}

fn assert_present(section: &RlogBloomSection, cid: u32, values: &[String]) {
    for v in values {
        for key in keys_of(v) {
            assert!(
                (0..section.len()).any(|i| section.entry(i).expect("entry").may_contain(cid, &key)),
                "covered column {cid} key {:?}",
                String::from_utf8_lossy(&key)
            );
        }
    }
}

#[tokio::test]
async fn bloom_scope_reaches_the_object() {
    let bodies_seen: Vec<String> = (0..4).map(|i| format!("request alpha{i}")).collect();

    // Text: body and severity_text only.
    let text = layout_record(&[], None, BloomScope::Text);
    let object = flush_one(Some(&text), bloom_records()).await;
    let (dir, raw) = dir_and_bloom(&object);
    let section = RlogBloomSection::parse(&raw, &dir).expect("parse BLOOM");
    let (note, region) = (str_col(&dir, "note"), str_col(&dir, "region"));
    let mut covered = vec![COL_SEVERITY_TEXT, COL_BODY];
    covered.sort_unstable();
    assert_eq!(section.covered(), covered.as_slice(), "text coverage list");
    assert_present(&section, COL_BODY, &bodies_seen);
    let probed = assert_absent(&section, note, &notes())
        + assert_absent(&section, region, &["westcoast".to_string()]);
    // 4 notes of 3 keys each plus 1 region value of 2 keys, over one entry.
    assert_eq!(probed, 14);

    // Undeclared with `note` declared: `region` stays covered, `note` is
    // neither covered nor held by any entry.
    let undeclared = layout_record(
        &[("note", TypedAttrColumnType::Str)],
        None,
        BloomScope::Undeclared,
    );
    let object = flush_one(Some(&undeclared), bloom_records()).await;
    let (dir, raw) = dir_and_bloom(&object);
    let section = RlogBloomSection::parse(&raw, &dir).expect("parse BLOOM");
    let (note, region) = (str_col(&dir, "note"), str_col(&dir, "region"));
    let mut covered = vec![COL_SEVERITY_TEXT, COL_BODY, region];
    covered.sort_unstable();
    assert_eq!(
        section.covered(),
        covered.as_slice(),
        "undeclared coverage list"
    );
    assert!(!section.covers(note));
    assert_present(&section, region, &["westcoast".to_string()]);
    assert_eq!(assert_absent(&section, note, &notes()), 12);

    // The same declaration under the default scope covers and holds `note`,
    // so its absence above is the scope's doing.
    let all = layout_record(&[("note", TypedAttrColumnType::Str)], None, BloomScope::All);
    let object = flush_one(Some(&all), bloom_records()).await;
    let (dir, raw) = dir_and_bloom(&object);
    let section = RlogBloomSection::parse(&raw, &dir).expect("parse BLOOM");
    let note = str_col(&dir, "note");
    assert!(section.covers(note));
    assert_present(&section, note, &notes());
}

/// `to_logseg_record`'s mapping, for the direct-writer reference object.
fn to_log_record(rec: NormalizedLogRecord) -> LogRecord {
    LogRecord {
        stream_id: rec.stream_id,
        stream_attrs: rec.stream_attrs,
        ts_ns: rec.ts_ns,
        observed_ts_ns: rec.observed_ts_ns,
        severity_num: rec.severity_num,
        severity_text: rec.severity_text,
        body: rec.body,
        trace_id: rec.trace_id,
        span_id: rec.span_id,
        flags: rec.flags,
        attrs: rec.attrs,
    }
}

/// `records` written by a writer with no builder calls under `ftr`'s identity.
fn direct_object(ftr: &LogFooter, records: Vec<NormalizedLogRecord>) -> Vec<u8> {
    let identity = ObjectIdentity {
        tenant_hash: ftr.tenant_hash,
        shard: ftr.shard,
        writer_id: ftr.writer_id,
        writer_epoch: ftr.writer_epoch,
        writer_seq: ftr.writer_seq,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for r in records {
        writer.push(to_log_record(r)).expect("push");
    }
    writer.finish().expect("finish")
}

#[tokio::test]
async fn no_key_leaves_the_object_unchanged() {
    // Each router draws a random writer id, so each object is compared with
    // the direct writer under its own identity. The seeded two-router
    // comparison in `log_router.rs` pins the two objects to each other.
    let no_record = flush_one(None, clustering_records()).await;
    let neither_field = layout_record(&[], None, BloomScope::All);
    let v3 = flush_one(Some(&neither_field), clustering_records()).await;

    let (ftr_none, ftr_v3) = (footer(&no_record), footer(&v3));
    assert_eq!(ftr_none.sort_descriptor, None);
    assert_eq!(ftr_none.clustering_generation, 0);
    assert_eq!(ftr_v3.sort_descriptor, None);
    assert_eq!(ftr_v3.clustering_generation, 0);

    assert!(
        no_record == direct_object(&ftr_none, clustering_records()),
        "no config record: the object differs from a writer with no builder calls"
    );
    assert!(
        v3 == direct_object(&ftr_v3, clustering_records()),
        "a v3 record without fields 13 and 14: the object differs from a writer \
         with no builder calls"
    );
}

#[tokio::test]
async fn unresolved_key_column_writes_without_a_descriptor_and_counts() {
    // `missing` is not a declared typed column, and the Text scope rides on
    // the same unresolved layout.
    let config = layout_record(
        &[("region", TypedAttrColumnType::Str)],
        Some(key(&["missing"], ClusteringBucketWidth::OneHour, 4)),
        BloomScope::Text,
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    put_record(store.as_ref(), &config).await;
    let router = LogIngestRouter::new(
        flush_on_first(),
        Arc::clone(&store),
        TestClock::new(BASE_NS),
    );

    let first = write(&router, bloom_records()).await;
    assert_eq!(
        router.metrics().clustering_key_unresolved_by_tenant(),
        exact_count(1)
    );
    // A second flush inside the refresh horizon counts again.
    let second = write(&router, bloom_records()).await;
    assert_eq!(
        router.metrics().clustering_key_unresolved_by_tenant(),
        exact_count(2)
    );
    router.shutdown().await;

    for token in [first, second] {
        assert_unkeyed_full_coverage(&object_for(store.as_ref(), &token).await);
    }
}

/// This tenant's unresolved-layout count as the metrics report it, `n` flushes
/// with no overestimate.
fn exact_count(n: u64) -> Vec<TenantCount> {
    vec![TenantCount {
        tenant: tenant_hash(),
        count: n,
        error: 0,
    }]
}

/// No descriptor, generation 0, and every string column of
/// [`bloom_records`] covered.
fn assert_unkeyed_full_coverage(object: &[u8]) {
    let ftr = footer(object);
    assert_eq!(ftr.sort_descriptor, None);
    assert_eq!(ftr.clustering_generation, 0);
    let (dir, raw) = dir_and_bloom(object);
    let section = RlogBloomSection::parse(&raw, &dir).expect("parse BLOOM");
    let mut covered = vec![
        COL_SEVERITY_TEXT,
        COL_BODY,
        str_col(&dir, "note"),
        str_col(&dir, "region"),
    ];
    covered.sort_unstable();
    assert_eq!(section.covered(), covered.as_slice());
}

#[tokio::test]
async fn a_key_the_writer_refuses_writes_without_a_descriptor_and_counts() {
    // A raw record can declare a typed column named "" and key on it. The
    // config accessor accepts that key; the writer refuses an empty key column
    // name, so handing it over would abandon every flush for the tenant.
    let config = layout_record(
        &[
            ("", TypedAttrColumnType::Str),
            ("region", TypedAttrColumnType::Str),
        ],
        Some(key(&[""], ClusteringBucketWidth::OneHour, 6)),
        BloomScope::Text,
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    put_record(store.as_ref(), &config).await;
    let router = LogIngestRouter::new(
        flush_on_first(),
        Arc::clone(&store),
        TestClock::new(BASE_NS),
    );

    let token = write(&router, bloom_records()).await;
    assert_eq!(
        router.metrics().clustering_key_unresolved_by_tenant(),
        exact_count(1)
    );
    assert_eq!(router.metrics().snapshot().abandoned_input_rejected, 0);
    router.shutdown().await;
    assert_unkeyed_full_coverage(&object_for(store.as_ref(), &token).await);
}

#[tokio::test]
async fn deleting_a_keyed_record_resets_the_flush_after_a_refresh() {
    let config = layout_record(
        REGION_USER,
        Some(key(&["region"], ClusteringBucketWidth::OneHour, 8)),
        BloomScope::All,
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    put_record(store.as_ref(), &config).await;
    let clock = TestClock::new(BASE_NS);
    let router = LogIngestRouter::new(flush_on_first(), Arc::clone(&store), clock.clone());

    let keyed = write(&router, clustering_records()).await;
    store
        .delete(&config_key(&tenant_hash()))
        .await
        .expect("delete config record");
    // Inside the horizon the cached key still applies.
    let cached = write(&router, clustering_records()).await;
    clock.advance_ns(PAST_HORIZON_NS);
    let reset = write(&router, clustering_records()).await;
    assert_eq!(
        router.metrics().clustering_key_unresolved_by_tenant(),
        Vec::<TenantCount>::new(),
        "a deleted record is the no-record default, not an unresolved layout"
    );
    router.shutdown().await;

    for (token, generation) in [(keyed, 8), (cached, 8)] {
        let ftr = footer(&object_for(store.as_ref(), &token).await);
        assert!(ftr.sort_descriptor.is_some());
        assert_eq!(ftr.clustering_generation, generation);
    }
    let reset_object = object_for(store.as_ref(), &reset).await;
    let reset = footer(&reset_object);
    assert_eq!(reset.sort_descriptor, None);
    assert_eq!(reset.clustering_generation, 0);
    assert_eq!(bodies(&reset_object), ["b", "a", "c", "f", "e", "d"]);
    assert_eq!(
        reset_object,
        direct_object(&reset, clustering_records()),
        "no record after the refresh: the object differs from a writer with no builder calls"
    );
}

#[tokio::test]
async fn one_config_get_serves_the_fields_and_the_layout() {
    let config = layout_record(
        REGION_USER,
        Some(key(&["region", "user"], ClusteringBucketWidth::SixHours, 5)),
        BloomScope::All,
    );
    // Every GET of the config key passes through one counted sequence step;
    // more steps than any correct run takes, so an extra GET is counted too.
    let steps = 8;
    let plan = FaultPlan::empty().with_sequence(
        Sequence::new(Op::Get)
            .with_key_contains(config_key(&tenant_hash()))
            .with_steps(vec![SequenceStep::Passthrough; steps]),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    put_record(store.as_ref(), &config).await;
    let router = LogIngestRouter::new(
        flush_on_first(),
        Arc::clone(&store),
        TestClock::new(BASE_NS),
    );

    // One flush on a cold cache: one refresh, then the flush itself.
    let token = write(&router, clustering_records()).await;
    router.shutdown().await;
    assert_eq!(
        fault.sequence_progress(0),
        1,
        "one refresh and one flush read the config key once"
    );
    let ftr = footer(&object_for(store.as_ref(), &token).await);
    assert_eq!(
        ftr.clustering_generation, 5,
        "the layout came from that read"
    );
    assert!(ftr.sort_descriptor.is_some());
}

#[tokio::test]
async fn key_change_is_picked_up_after_a_refresh() {
    let key_a = layout_record(
        REGION_USER,
        Some(key(&["region"], ClusteringBucketWidth::OneHour, 1)),
        BloomScope::All,
    );
    let key_b = layout_record(
        REGION_USER,
        Some(key(&["user"], ClusteringBucketWidth::OneDay, 2)),
        BloomScope::All,
    );
    let desc_a = SortDescriptor {
        bucket_width: SortBucketWidth::OneHour,
        key_columns: vec![SortKeyColumn {
            name: "region".to_string(),
            ty: SortKeyType::Str,
        }],
    };
    let desc_b = SortDescriptor {
        bucket_width: SortBucketWidth::OneDay,
        key_columns: vec![SortKeyColumn {
            name: "user".to_string(),
            ty: SortKeyType::I64,
        }],
    };

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    put_record(store.as_ref(), &key_a).await;
    let clock = TestClock::new(BASE_NS);
    let router = LogIngestRouter::new(flush_on_first(), Arc::clone(&store), clock.clone());

    let first = write(&router, clustering_records()).await;
    put_record(store.as_ref(), &key_b).await;
    // Inside the horizon the cached key still applies.
    let cached = write(&router, clustering_records()).await;
    clock.advance_ns(PAST_HORIZON_NS);
    let refreshed = write(&router, clustering_records()).await;
    router.shutdown().await;

    let first = footer(&object_for(store.as_ref(), &first).await);
    assert_eq!(first.sort_descriptor, Some(desc_a.clone()));
    assert_eq!(first.clustering_generation, 1);
    let cached = footer(&object_for(store.as_ref(), &cached).await);
    assert_eq!(cached.sort_descriptor, Some(desc_a));
    assert_eq!(cached.clustering_generation, 1);
    let refreshed_object = object_for(store.as_ref(), &refreshed).await;
    let refreshed = footer(&refreshed_object);
    assert_eq!(refreshed.sort_descriptor, Some(desc_b));
    assert_eq!(refreshed.clustering_generation, 2);
    // `d` alone sits in the next day bucket; the rest order by user then ts.
    assert_eq!(bodies(&refreshed_object), ["f", "b", "e", "a", "c", "d"]);
}
