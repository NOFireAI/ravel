//! In-flight fetch cancellation: whether the concurrent segment fetches run
//! inside the stream's poll (cancel on drop) or via detached `tokio::spawn`
//! (leak after a deadline).
//!
//! In ravel-query, `QueryEngine::{instant,range,resolve_series}` wrap their
//! inner future in `tokio::time::timeout(deadline, ..)` (engine.rs:76-82).
//! The fetch fan-out is `stream::iter(..).buffer_unordered(..).collect()`
//! (engine.rs:255-267, 357-371) polled *inside* that future; there is no
//! `tokio::spawn`/`JoinHandle` anywhere in the crate. So when the deadline
//! fires, `timeout` drops the inner future, which drops the
//! `buffer_unordered` stream, which drops every in-flight `fetch_soa` future
//! (each awaiting `store.get`). Cancellation propagates by drop; nothing is
//! detached, so a timed-out query leaks no fetch.
//!
//! This test proves that with an object store whose data-object `get` never
//! completes and carries an RAII drop guard: after the deadline fires the
//! query returns `DeadlineExceeded`, the stalled `get` was entered, and its
//! guard was dropped (i.e. the future was cancelled, not left running). A
//! detached `tokio::spawn`ed fetch would keep the guard alive past the
//! timeout and fail the `dropped == started` assertion.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{EngineConfig, QueryEngine, QueryError};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};
use ravel_types::{CommitToken, Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

const NS: i64 = 1_000_000_000;
const SAMPLE_TS_NS: i64 = 1_000 * NS;

/// Increments `dropped` when it is dropped: the RAII witness that a stalled
/// `get` future was cancelled (dropped mid-await) rather than left running.
struct CancelGuard {
    dropped: Arc<AtomicUsize>,
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// Wraps a `MemoryStore` and stalls forever on `get` for exactly one key (the
/// segment data object), passing every other operation (including the catalog
/// commit-record reads that resolve the snapshot) straight through. The stall
/// records entry (`started`) and, via `CancelGuard`, cancellation (`dropped`).
struct StallingSegmentStore {
    inner: MemoryStore,
    stall_key: String,
    started: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

#[async_trait]
impl ObjectStoreBackend for StallingSegmentStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if key == self.stall_key {
            self.started.fetch_add(1, Ordering::SeqCst);
            // Held across the await: if the surrounding future is dropped
            // (deadline cancellation), this guard drops and records it. If a
            // detached task kept polling, the guard would outlive the timeout.
            let _guard = CancelGuard {
                dropped: Arc::clone(&self.dropped),
            };
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves");
        }
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
        // double inherits.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// Writes one real RSEG segment for `tenant_hash` and publishes its commit
/// record onto `store`, returning the read-your-write token and the segment
/// data-object key. Mirrors the e2e helper; `ingest_hour_bucket` 0 keeps the
/// segment outside any real-time listing window so only the token makes it
/// visible (so catalog resolution reads the commit record, never the data
/// object, before the fetch stalls).
async fn publish_one_segment(
    store: &MemoryStore,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
) -> (CommitToken, String) {
    let writer_id = Uuid::from_u128(7);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: "stalled_metric".to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, "stalled_metric", &label_set).expect("series id");
    let input = SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample {
            ts_ns: SAMPLE_TS_NS,
            value: 1.0,
        }],
    };
    let written: WrittenSegment =
        SegmentWriter::write(vec![input], identity, bounds).expect("write segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 42,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    let token = publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    (token, data_key)
}

/// The deadline fires while a segment fetch is stalled in `store.get`. The
/// query must surface `DeadlineExceeded`, and the stalled fetch future must
/// have been *dropped* (cancelled), proving it ran inside the timed-out
/// future's poll and was not detached onto a task that outlives the timeout.
///
/// The deadline is a short real duration: the stalled fetch never completes,
/// so `timeout` is guaranteed to be what ends the call (the outcome does not
/// depend on how long the deadline is, only that it fires while the fetch is
/// pending), making the test deterministic without a paused clock.
#[tokio::test]
async fn deadline_fire_cancels_the_in_flight_segment_fetch() {
    let tenant_id = TenantId::new("tenant-a".to_string());
    let tenant_hash = tenant_id.hash();

    // Publish onto a plain MemoryStore, then move it inside the stalling
    // wrapper so the commit record (already written) still reads back but the
    // data-object GET stalls.
    let inner = MemoryStore::new();
    let (token, data_key) = publish_one_segment(&inner, &tenant_id, tenant_hash).await;

    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(StallingSegmentStore {
        inner,
        stall_key: data_key,
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    });
    let backend: Arc<dyn ObjectStoreBackend> = store;

    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(catalog, backend, EngineConfig::default());

    let t_ms = SAMPLE_TS_NS / 1_000_000;
    let result = engine
        .instant(
            tenant_hash,
            "stalled_metric",
            t_ms,
            &[token],
            // now_ns is only used for the listing window; the token forces
            // visibility regardless, so any value works.
            SAMPLE_TS_NS,
            Duration::from_millis(100),
        )
        .await;

    assert!(
        matches!(result, Err(QueryError::DeadlineExceeded { .. })),
        "expected DeadlineExceeded, got {result:?}"
    );
    assert_eq!(
        started.load(Ordering::SeqCst),
        1,
        "the segment data-object fetch must actually have been in flight"
    );
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        started.load(Ordering::SeqCst),
        "every stalled fetch future must be dropped (cancelled) when the \
         deadline fires; a detached tokio::spawn task would keep it alive"
    );
}
