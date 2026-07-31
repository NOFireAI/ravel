//! Integration tests for [`ravel_query::LogSegmentFetcher`] (ADR-0033, issue
//! #238). Two small RLOG objects with disjoint ts ranges and distinct stream
//! identities exercise: ts-range pruning before any GET, stream-attribute
//! resolution against STREAM_DIR, and an end-to-end word + ts-range scan whose
//! records are checked against a hand-computed expected set. Objects are
//! written with a deliberately tiny `block_target_records` (see
//! [`small_blocks`]) so that block-level pruning is observable rather than
//! trivially satisfied by a single-block object.
//!
//! `matching_streams_over_approximates_nested_map_values` pins the documented
//! false-positive behavior of stream-attribute matching, which is a byte
//! containment search rather than an exact structured match.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, FieldSel, LogRecord, LogStreamId, Predicate, RlogConfig, RlogWriter,
    stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{LogQuery, LogSegmentFetcher, StreamAttrEquals};
use uuid::Uuid;

/// An [`ObjectStoreBackend`] wrapper that counts `get` calls, so a test can
/// prove the ts-range pre-check skipped an object without touching storage.
/// Every other method delegates unchanged.
struct CountingStore {
    inner: Arc<MemoryStore>,
    gets: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        CountingStore {
            inner,
            gets: AtomicU64::new(0),
        }
    }

    fn get_count(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits (issue #298).
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [1u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Resource attributes and the derived [`LogStreamId`] for service `name`.
fn stream(name: &str) -> (LogStreamId, Vec<u8>) {
    let resource = vec![("service.name".to_string(), AttrValue::Str(name.to_string()))];
    let id = ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]);
    let blob = stream_attrs_bytes(&resource, "scope", "1.0", &[]);
    (id, blob)
}

/// A record on the stream identified by an arbitrary resource attribute set
/// (scope `("scope", "1.0")`, no scope attributes), for stream-identity cases
/// the simple `service.name`-only helper cannot express.
fn record_with_resource(resource: &[(String, AttrValue)], ts: i64, body: &str) -> LogRecord {
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn record(name: &str, ts: i64, body: &str) -> LogRecord {
    let (stream_id, stream_attrs) = stream(name);
    LogRecord {
        stream_id,
        stream_attrs,
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// A writer config that cuts a block every 3 records, so the small test
/// objects here still have several blocks and block-level pruning has something
/// real to prove. With the default `block_target_records` of 8192 every test
/// object would be a single block and any "pruning happened" assertion would be
/// vacuous.
fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

/// Writes one RLOG object from `records`, puts it at `key`, and returns a
/// matching L0 [`SegmentRef`] with the object's true ts span.
async fn write_object(store: &MemoryStore, key: &str, records: &[LogRecord]) -> SegmentRef {
    let mut w = RlogWriter::new(small_blocks(), identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");

    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// Object A: stream "api", ts 100..=110. Object B: stream "worker",
/// ts 1000..=1010. Disjoint ts ranges and distinct stream identities.
async fn two_objects(store: &MemoryStore) -> (SegmentRef, SegmentRef) {
    let a: Vec<LogRecord> = (100..=110)
        .map(|ts| {
            record(
                "api",
                ts,
                if ts == 105 {
                    "connection timeout"
                } else {
                    "ok"
                },
            )
        })
        .collect();
    let b: Vec<LogRecord> = (1000..=1010)
        .map(|ts| {
            record(
                "worker",
                ts,
                if ts == 1005 {
                    "connection timeout"
                } else {
                    "ok"
                },
            )
        })
        .collect();
    let ref_a = write_object(store, "logs/a.rlog", &a).await;
    let ref_b = write_object(store, "logs/b.rlog", &b).await;
    (ref_a, ref_b)
}

#[tokio::test]
async fn ts_range_pre_check_skips_object_without_get() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = two_objects(&mem).await;
    let counting = Arc::new(CountingStore::new(mem));
    let fetcher = LogSegmentFetcher::new(counting.clone() as Arc<dyn ObjectStoreBackend>);

    // A ts range covering only object A's span [100, 110].
    let query = LogQuery::new(90, 120);

    let out_a = fetcher.fetch(&ref_a, &query).await.expect("fetch a");
    let out_b = fetcher.fetch(&ref_b, &query).await.expect("fetch b");

    // A is relevant and was fetched; B's span [1000, 1010] is disjoint, so it
    // is pruned to None with no GET at all.
    assert!(out_a.is_some(), "object A overlaps the range");
    assert!(out_b.is_none(), "object B is pruned by ts range");
    assert_eq!(
        counting.get_count(),
        1,
        "exactly one GET: only the relevant object was fetched"
    );
}

#[tokio::test]
async fn stream_attr_equality_returns_only_matching_object_records() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = two_objects(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);

    // A range that spans both objects, filtered to stream "worker" only.
    let query = LogQuery::new(0, 5000).with_stream_attr(StreamAttrEquals::new(
        "service.name",
        AttrValue::Str("worker".into()),
    ));

    let out_a = fetcher
        .fetch(&ref_a, &query)
        .await
        .expect("fetch a")
        .expect("a in range");
    let out_b = fetcher
        .fetch(&ref_b, &query)
        .await
        .expect("fetch b")
        .expect("b in range");

    // Object A holds only "api" records: the "worker" filter resolves to an
    // empty StreamIn there, so no records come back.
    assert!(
        out_a.records.is_empty(),
        "object A has no 'worker' stream records"
    );
    // Object B holds only "worker" records: all of them match.
    assert_eq!(out_b.records.len(), 11);
    let (worker_id, _) = stream("worker");
    assert!(
        out_b.records.iter().all(|r| r.stream_id == worker_id),
        "every returned record belongs to the worker stream"
    );
}

/// The acceptance test named in the epic decomposition: one fetch call prunes
/// by ts range (object B skipped, no GET) and by stream attrs (object A's
/// non-matching stream yields nothing), combined with a word predicate, and
/// the surviving records match a hand-computed expected set exactly.
#[tokio::test]
async fn fetch_prunes_by_ts_range_and_stream_attrs_without_full_object_scan() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = two_objects(&mem).await;
    let counting = Arc::new(CountingStore::new(mem));
    let fetcher = LogSegmentFetcher::new(counting.clone() as Arc<dyn ObjectStoreBackend>);

    // Range [90, 120] overlaps only object A. Filter to stream "api" and require
    // the body word "timeout". In object A exactly the ts=105 record
    // ("connection timeout") matches; ts 100..104 and 106..110 say "ok".
    let query = LogQuery::new(90, 120)
        .with_stream_attr(StreamAttrEquals::new(
            "service.name",
            AttrValue::Str("api".into()),
        ))
        .with_content(Predicate::HasWord {
            field: FieldSel::Body,
            word: "timeout".into(),
        });

    // Object B is pruned by ts before any GET.
    let out_b = fetcher.fetch(&ref_b, &query).await.expect("fetch b");
    assert!(out_b.is_none(), "object B pruned by ts range, no scan");
    assert_eq!(counting.get_count(), 0, "no GET for the pruned object");

    // Object A is fetched and scanned; exactly one record survives.
    let out_a = fetcher
        .fetch(&ref_a, &query)
        .await
        .expect("fetch a")
        .expect("a in range");
    assert_eq!(counting.get_count(), 1, "one GET for the relevant object");

    let (api_id, _) = stream("api");
    assert_eq!(out_a.records.len(), 1, "only the ts=105 timeout record");
    let hit = &out_a.records[0];
    assert_eq!(hit.ts_ns, 105);
    assert_eq!(hit.stream_id, api_id);
    assert_eq!(hit.body, "connection timeout");

    // The scan actually pruned blocks rather than reading the whole object.
    // Object A is written with `small_blocks`, so it really has several blocks;
    // the "timeout" word predicate only survives bloom in the one block holding
    // ts=105. Guard the multi-block precondition first, otherwise the strict
    // inequality below could pass for the wrong reason.
    assert!(
        out_a.stats.blocks_total > 1,
        "object A must be multi-block for block pruning to be observable, got {}",
        out_a.stats.blocks_total
    );
    assert!(
        out_a.stats.blocks_scanned < out_a.stats.blocks_total,
        "bloom pruning dropped blocks before decoding: scanned {} of {}",
        out_a.stats.blocks_scanned,
        out_a.stats.blocks_total
    );
}

/// Skip-index ts pruning is observable on its own, with no content predicate:
/// a range covering only the first few records of a multi-block object leaves
/// strictly fewer candidate blocks than the object has.
#[tokio::test]
async fn ts_range_prunes_blocks_within_the_object() {
    let mem = MemoryStore::new();
    let records: Vec<LogRecord> = (100..=130).map(|ts| record("api", ts, "ok")).collect();
    let seg_ref = write_object(&mem, "logs/many.rlog", &records).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(mem);
    let fetcher = LogSegmentFetcher::new(store);

    let out = fetcher
        .fetch(&seg_ref, &LogQuery::new(100, 102))
        .await
        .expect("fetch")
        .expect("in range");

    assert!(
        out.stats.blocks_total > 1,
        "31 records at 3 per block must span several blocks, got {}",
        out.stats.blocks_total
    );
    assert!(
        out.stats.blocks_after_skip < out.stats.blocks_total,
        "skip index dropped blocks outside [100, 102]: {} of {} survived",
        out.stats.blocks_after_skip,
        out.stats.blocks_total
    );
    // The surviving records are exactly the three in range.
    let got: Vec<i64> = out.records.iter().map(|r| r.ts_ns).collect();
    assert_eq!(got, vec![100, 101, 102]);
}

/// Locks in the documented over-approximation of
/// [`LogSegmentFetcher::matching_streams`]: byte containment cannot distinguish
/// a `(key, value)` pair nested inside a map attribute value from the same pair
/// as a genuine top-level resource attribute, because both are written with the
/// identical byte grammar. Two distinct streams, only one of which carries a
/// top-level `service.name`, both match the `service.name = "api"` filter.
///
/// This test asserts the current, now-documented behavior rather than the ideal
/// behavior. Issue #239 owns the decision to make this exact; if it does, this
/// test should be changed to assert the exact result (only `plain_id`), never
/// deleted, so the change is deliberate and visible.
#[tokio::test]
async fn matching_streams_over_approximates_nested_map_values() {
    let mem = MemoryStore::new();
    // Stream (a): a genuine top-level `service.name = "api"`.
    let plain = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    // Stream (b): no top-level `service.name` at all. The pair only appears
    // nested inside the value of an unrelated `k8s.labels` map attribute.
    let nested = vec![(
        "k8s.labels".to_string(),
        AttrValue::Map(vec![(
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        )]),
    )];
    let plain_id = ravel_types::logstream::log_stream_id(&plain, "scope", "1.0", &[]);
    let nested_id = ravel_types::logstream::log_stream_id(&nested, "scope", "1.0", &[]);
    assert_ne!(plain_id, nested_id, "these are two distinct streams");

    let records = vec![
        record_with_resource(&plain, 1, "x"),
        record_with_resource(&nested, 2, "y"),
    ];
    let _ = write_object(&mem, "logs/nested.rlog", &records).await;
    let bytes = mem
        .get("logs/nested.rlog", GetRange::Full)
        .await
        .expect("get")
        .data;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let fetcher = LogSegmentFetcher::new(store);
    let mut matched = fetcher
        .matching_streams(
            &bytes,
            &[StreamAttrEquals::new(
                "service.name",
                AttrValue::Str("api".into()),
            )],
        )
        .expect("resolve");
    matched.sort();

    let mut both = vec![plain_id, nested_id];
    both.sort();
    assert_eq!(
        matched, both,
        "documented over-approximation: the nested-map stream is a false positive"
    );
    // The no-false-negative direction: the stream that genuinely carries the
    // attribute is always in the result.
    assert!(matched.contains(&plain_id));

    // The over-approximation is confined to nesting-vs-top-level ambiguity. A
    // value that appears nowhere in the object, at any depth, still resolves to
    // the empty set.
    let none = fetcher
        .matching_streams(
            &bytes,
            &[StreamAttrEquals::new(
                "service.name",
                AttrValue::Str("absent".into()),
            )],
        )
        .expect("resolve");
    assert!(none.is_empty(), "no stream carries service.name=absent");
}

#[tokio::test]
async fn matching_streams_resolves_ids_from_stream_dir() {
    let mem = MemoryStore::new();
    let a: Vec<LogRecord> = vec![record("api", 1, "x"), record("worker", 2, "y")];
    let _ = write_object(&mem, "logs/mix.rlog", &a).await;
    let bytes = mem
        .get("logs/mix.rlog", GetRange::Full)
        .await
        .expect("get")
        .data;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let fetcher = LogSegmentFetcher::new(store);

    let (api_id, _) = stream("api");
    let matched = fetcher
        .matching_streams(
            &bytes,
            &[StreamAttrEquals::new(
                "service.name",
                AttrValue::Str("api".into()),
            )],
        )
        .expect("resolve");
    assert_eq!(matched, vec![api_id], "only the api stream matches");

    // A value present in no stream resolves to the empty set.
    let none = fetcher
        .matching_streams(
            &bytes,
            &[StreamAttrEquals::new(
                "service.name",
                AttrValue::Str("absent".into()),
            )],
        )
        .expect("resolve");
    assert!(none.is_empty(), "no stream carries service.name=absent");
}
