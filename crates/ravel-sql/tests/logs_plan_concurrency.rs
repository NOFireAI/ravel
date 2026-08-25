//! The logs scan's plan phase (`compute_plan_counts`, ADR-0102) prunes every
//! segment before any partition drains. Issue #691: that pass ran one prune at
//! a time, so a query over N objects paid N sequential object-store round
//! trips before scanning anything (about 20 minutes on 8424 objects). This
//! pins the fix: the prunes run `target_partitions` at a time.
//!
//! The evidence is a `FaultStore` gate that holds every GET: how many GETs are
//! held at once is exactly how many prunes are in flight. A sequential plan
//! phase never holds more than one, so the "at least two held" wait below
//! times out against it; the bound is pinned by the held count settling at
//! `target_partitions` with more segments than that still unplanned.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::collect;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::LogSegmentFetcher;
use ravel_sql::LogsTableProvider;
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn record(ts: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("event {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

async fn write_object(store: &MemoryStore, key: &str, records: &[LogRecord]) -> SegmentRef {
    let mut w = RlogWriter::new(
        RlogConfig {
            block_target_records: 3,
            ..RlogConfig::default()
        },
        identity(),
    );
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

/// Six relevant segments, a scan planned at four partitions, every GET held:
/// the plan phase parks exactly four prunes at once (the partition count, not
/// one, and not all six).
#[tokio::test]
async fn plan_phase_prunes_target_partitions_segments_at_a_time() {
    const SEGMENTS: usize = 6;
    const TARGET_PARTITIONS: usize = 4;

    let plain = MemoryStore::new();
    let mut segments = Vec::with_capacity(SEGMENTS);
    for i in 0..SEGMENTS {
        let base = (i as i64) * 100;
        let records: Vec<LogRecord> = (base..base + 6).map(record).collect();
        segments.push(write_object(&plain, &format!("logs/{i}.rlog"), &records).await);
    }

    // Wrap after the writes: the gate holds GETs only, but keeping the fixture
    // writes on the plain store makes the ordering obvious.
    let faulty = Arc::new(FaultStore::new(plain, FaultPlan::empty()));
    let gate = faulty.hold(Op::Get, None, Occurrence::Always);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;

    let provider = LogsTableProvider::new(
        Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TENANT,
        LogSegmentFetcher::new(store),
        QueryAccounting::new(),
    );
    let plan = provider.plan(TARGET_PARTITIONS).expect("build plan");
    let query = tokio::spawn(collect(plan, Arc::new(TaskContext::default())));

    // A sequential plan phase holds one GET and never a second one, so this
    // wait is the red line against the pre-#691 code.
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_held(2))
        .await
        .expect("at least two segment prunes must be in flight at once");

    // The in-flight count settles at the partition count: not one, and not all
    // six segments.
    tokio::time::timeout(
        Duration::from_secs(5),
        gate.wait_until_held(TARGET_PARTITIONS),
    )
    .await
    .expect("the plan phase reaches target_partitions prunes in flight");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        gate.held_count(),
        TARGET_PARTITIONS,
        "prunes in flight are bounded by target_partitions"
    );

    // The query can never finish while every GET is held; it is the gate that
    // was under test, not the result.
    query.abort();
}
