//! In-process coverage for `ravel-cli commit reconstruct` (ADR-0058 decision
//! 2). Driven in-process against a single shared `MemoryStore`,
//! the same pattern `tests/catalog.rs` uses and for the same reason: a
//! subprocess-per-invocation of the binary gets its own empty in-memory store,
//! so a seed -> reconstruct -> resolve scenario cannot be built that way
//! without a persistent S3/MinIO backend, unavailable here. `ravel_cli`'s lib
//! target exists for exactly this.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use ravel_cli::maintain::SignalArg;
use ravel_cli::reconstruct::reconstruct;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{
    LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::logstream::AttrValue;
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const MS_PER_HOUR: u64 = 3_600_000;

/// These fixtures build their own store, which is the explicit
/// `--store memory` case (issue #1024): the report header reads
/// `store: memory` and an empty shard stays a success.
const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

/// Event/ingest timestamp 30 minutes into ingest hour 495_734.
const INGEST_HOUR: u32 = 495_734;
fn ts_in_ingest_hour() -> i64 {
    i64::from(INGEST_HOUR) * NS_PER_HOUR + 30 * 60_000_000_000
}

/// End-to-end reachability: a shard whose data object is present but whose
/// commit record was never written (the loss ADR-0058 recovers from). After
/// `commit reconstruct`, the rebuilt record must be readable via the normal
/// catalog resolve path and match the data object's identity fields exactly.
#[tokio::test]
async fn reconstruct_makes_a_record_less_segment_resolvable_again() {
    let store = Arc::new(MemoryStore::new());
    // Objects written after this get last_modified in ingest hour + 1, so the
    // reconstructed created_unix_ns lands past the ingest-hour bucket (the
    // validate() cross-check ingest_hour_bucket <= floor(created/NS_PER_HOUR)).
    store.set_clock_ms((u64::from(INGEST_HOUR) + 1) * MS_PER_HOUR);

    let tenant = "acme";
    let tenant_hash = TenantId::new(tenant).hash();
    let shard = 0u32;
    let writer_id = Uuid::new_v4();
    let event_ts = ts_in_ingest_hour();

    // Build a real RSEG object so `reconstruct` has a genuine footer to read.
    let series = vec![SeriesInput {
        series_id: SeriesId([1u8; 16]),
        labels: LabelSet::new(vec![Label {
            name: "__name__".into(),
            value: "up".into(),
        }])
        .expect("labelset"),
        samples: vec![Sample {
            ts_ns: event_ts,
            value: 1.0,
        }],
    }];
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: event_ts,
        max_ingest_ts_ns: event_ts,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write RSEG");

    // Seed the data object at its content-addressed key. No commit record is
    // ever written: this is exactly the "record-less L0 object" state.
    let data_key = keys::data_key(
        &tenant_hash,
        Signal::Metrics,
        shard,
        writer_id,
        1,
        1,
        &written.summary.blake3,
    )
    .expect("data key");
    store
        .put(&data_key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("seed data object");

    // Before reconstruction: the resolve path sees no segment (no record).
    let range = TimeRange {
        start_ns: 0,
        end_ns: (i64::from(INGEST_HOUR) + 2) * NS_PER_HOUR,
    };
    let now = (i64::from(INGEST_HOUR) + 2) * NS_PER_HOUR;
    let before = resolve_metrics(store.clone(), &tenant_hash, range, now).await;
    assert!(
        !before.iter().any(|s| s == &data_key),
        "the record-less segment must be invisible before reconstruction"
    );

    reconstruct(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SignalArg::Metrics,
        shard,
    )
    .await
    .expect("reconstruct succeeds");

    // After reconstruction: the segment resolves, and its identity fields match
    // the data object's own footer/content exactly.
    let catalog = ravel_catalog::Catalog::new(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        ravel_catalog::CatalogConfig {
            shard_count: 4,
            ..ravel_catalog::CatalogConfig::default()
        },
    )
    .expect("catalog");
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Metrics, range, &[], now)
        .await
        .expect("resolve after reconstruct");
    let seg = snapshot
        .segments
        .iter()
        .find(|s| s.data_object_key == data_key)
        .expect("reconstructed segment must be resolvable via the normal read path");

    assert_eq!(seg.shard, shard);
    assert_eq!(seg.writer_id, writer_id);
    assert_eq!(seg.writer_epoch, 1);
    assert_eq!(seg.writer_seq, 1);
    assert_eq!(seg.sample_count, written.summary.sample_count);
    assert_eq!(seg.series_count, written.summary.series_count);
    assert_eq!(seg.min_event_ts_ns, written.summary.min_event_ts_ns);
    assert_eq!(seg.max_event_ts_ns, written.summary.max_event_ts_ns);
    assert_eq!(seg.content_hash, written.summary.blake3);
    assert_eq!(
        seg.ingest_hour_bucket, INGEST_HOUR,
        "RSEG ingest_hour_bucket is read directly from the footer"
    );
    // created_unix_ns is the data object's own last_modified, not now: it is
    // the historical write time (ingest hour + 1), strictly before `now`.
    assert_eq!(
        seg.created_unix_ns,
        (i64::from(INGEST_HOUR) + 1) * NS_PER_HOUR
    );
}

/// A store wrapper that hides one key from every LIST result while leaving it
/// fully present for GET/PUT, and counts PUT attempts against it. This models
/// the exact race `CreateIfAbsent` guards: a commit record that exists at a
/// candidate's derived key but was not seen by the discovery LIST (a concurrent
/// partial reconstruction, or the record was never actually lost). It lets the
/// data object be picked as a candidate, so the reconstruction genuinely
/// attempts the write and must be refused, not merely filtered out at discovery.
struct HideFromListStore {
    inner: Arc<dyn ObjectStoreBackend>,
    hidden_key: String,
    put_attempts_on_hidden: Arc<AtomicUsize>,
}

#[async_trait]
impl ObjectStoreBackend for HideFromListStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        if key == self.hidden_key {
            self.put_attempts_on_hidden.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        let mut listed = self.inner.list(prefix, page).await?;
        listed.objects.retain(|m| m.key != self.hidden_key);
        Ok(listed)
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let mut listed = self.inner.list_delimited(prefix).await?;
        listed.objects.retain(|m| m.key != self.hidden_key);
        Ok(listed)
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// `CreateIfAbsent` never clobbers: with a commit record already present at a
/// candidate's derived key (hidden from discovery's LIST so the data object is
/// still picked as a candidate), reconstruction attempts the write, is refused
/// with `AlreadyExists`, reports the conflict, and leaves the existing record
/// byte-for-byte intact (ADR-0058 decision 2 step 4).
#[tokio::test]
async fn create_if_absent_never_clobbers_an_existing_record() {
    let inner = Arc::new(MemoryStore::new());
    inner.set_clock_ms((u64::from(INGEST_HOUR) + 1) * MS_PER_HOUR);

    let tenant = "acme";
    let tenant_hash = TenantId::new(tenant).hash();
    let shard = 2u32;
    let writer_id_bytes = [0x42u8; 16];
    let writer_id = Uuid::from_bytes(writer_id_bytes);
    let (epoch, seq) = (7u64, 9u64);
    let observed_ts = ts_in_ingest_hour();

    // Build a real RLOG object.
    let stream_attrs = stream_attrs_bytes(
        &[("service.name".into(), AttrValue::Str("svc".into()))],
        "scope",
        "1.0",
        &[],
    );
    let record = LogRecord {
        stream_id: LogStreamId([3u8; 16]),
        stream_attrs,
        ts_ns: observed_ts - 1_000,
        observed_ts_ns: observed_ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: "hello".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    };
    let identity = ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id_bytes,
        writer_epoch: epoch,
        writer_seq: seq,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    writer.push(record).expect("push log record");
    let object = writer.finish().expect("finish rlog");
    let object = Bytes::from(object);
    let content_hash = *blake3::hash(&object).as_bytes();

    let data_key = keys::data_key(
        &tenant_hash,
        Signal::Logs,
        shard,
        writer_id,
        epoch,
        seq,
        &content_hash,
    )
    .expect("data key");
    inner
        .put(&data_key, object.clone(), PutOptions::default())
        .await
        .expect("seed data object");

    // Pre-seed a *different* commit record at the exact key reconstruction will
    // derive (same identity, RLOG-derived ingest_hour_bucket): a distinct
    // sample_count so any clobber is detectable. RLOG derives
    // ingest_hour_bucket = floor(min_observed_ts_ns / NS_PER_HOUR) = INGEST_HOUR.
    let created = (i64::from(INGEST_HOUR) + 1) * NS_PER_HOUR;
    let existing = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard,
        writer_id,
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: 123,
        content_hash: [0xAAu8; 32],
        sample_count: 999,
        series_count: 7,
        min_event_ts_ns: observed_ts - 1_000,
        max_event_ts_ns: observed_ts,
        min_ingest_ts_ns: observed_ts,
        max_ingest_ts_ns: observed_ts,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: created,
        ingest_hour_bucket: INGEST_HOUR,
    })
    .expect("build existing record");
    let commit_key = keys::commit_key_for_record(&existing).expect("commit key");
    let existing_bytes = record::encode(&existing);
    inner
        .put(
            &commit_key,
            existing_bytes.clone(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("seed existing commit record");

    let put_attempts = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(HideFromListStore {
        inner: inner.clone() as Arc<dyn ObjectStoreBackend>,
        hidden_key: commit_key.clone(),
        put_attempts_on_hidden: put_attempts.clone(),
    });

    // Reconstruction sees the data object as a candidate (the record is hidden
    // from LIST), so it genuinely attempts the write; it must not fail the run.
    reconstruct(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SignalArg::Logs,
        shard,
    )
    .await
    .expect("reconstruct treats an existing record as a skipped conflict, not a failure");

    assert!(
        put_attempts.load(Ordering::SeqCst) >= 1,
        "reconstruction must have attempted a CreateIfAbsent write against the existing key"
    );

    // The existing record is byte-for-byte intact: never clobbered.
    let got = inner
        .get(&commit_key, GetRange::Full)
        .await
        .expect("existing record still present");
    assert_eq!(
        got.data, existing_bytes,
        "the pre-existing commit record must be byte-for-byte unchanged"
    );
    let decoded = record::decode(&got.data).expect("decode");
    assert_eq!(
        decoded.sample_count, 999,
        "the existing record's own sample_count must survive, proving no overwrite"
    );
}

/// Resolve the metrics segments for a tenant via the normal (non-enforcing)
/// catalog path and return their data-object keys.
async fn resolve_metrics(
    store: Arc<MemoryStore>,
    tenant_hash: &ravel_types::TenantHash,
    range: TimeRange,
    now: i64,
) -> Vec<String> {
    let catalog = ravel_catalog::Catalog::new(
        store as Arc<dyn ObjectStoreBackend>,
        ravel_catalog::CatalogConfig {
            shard_count: 4,
            ..ravel_catalog::CatalogConfig::default()
        },
    )
    .expect("catalog");
    let snapshot = catalog
        .resolve(tenant_hash, Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect()
}
