//! ADR-1702 task 8: a `logs` scan block decode on the read gate leaves the
//! runtime's worker free.
//!
//! The park sits inside the decode itself: the fetcher enters its `decode`
//! span around every block decode, and this binary's global subscriber parks
//! the entry that lands in the last block's gate job. The binary holds this
//! one test because `tracing` caches a callsite's interest process-wide, so a
//! test in the same process reaching the `decode` callsite with no subscriber
//! could leave the span disabled and the probe blind.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::ThreadId;
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_cpu_gate::{CpuGateConfig, InstantClock, ReadGate, ReadSite};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{LOG_COL_TS, LogsScanExec, logs_schema_with_declared};
use ravel_test_support::{ParkOnFirstArmedCall, run_with_watchdog};
use ravel_types::TenantHash;
use ravel_types::logstream::log_stream_id;
use tracing::span;
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
/// Records in the fixture's one object. The writer cuts a block per record
/// (`block_target_records: 1`), so this is also its block count, and with no
/// predicate and a window over every record each block survives pruning and
/// is decoded exactly once.
const BLOCKS: u64 = 4;
const KEY: &str = "t/gate0.rlog";
const WATCHDOG: Duration = Duration::from_secs(30);

/// One object of [`BLOCKS`] one-record blocks, each carrying one attribute,
/// so no block has an `attrs_raw` page and the columnar drain never falls
/// back to the row path.
fn object() -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records: 1,
        ..RlogConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: [5u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
    let mut writer = RlogWriter::new(cfg, identity);
    for ts in 0..BLOCKS as i64 {
        writer
            .push(LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("row {ts} {}", "payload ".repeat(16)),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![("a".to_string(), AttrValue::Str(format!("a{ts}")))],
            })
            .expect("push");
    }
    writer.finish().expect("finish")
}

async fn fixture() -> (Arc<dyn ObjectStoreBackend>, Vec<SegmentRef>) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let obj = object();
    store
        .put(KEY, bytes::Bytes::from(obj.clone()), PutOptions::default())
        .await
        .expect("put");
    let segment = SegmentRef {
        data_object_key: KEY.to_string(),
        object_size: obj.len() as u64,
        min_event_ts_ns: 0,
        max_event_ts_ns: BLOCKS as i64 - 1,
        ingest_hour_bucket: 0,
        sample_count: BLOCKS,
        series_count: 0,
        shard: 0,
        content_hash: [3u8; 32],
        writer_id: Uuid::from_u128(5),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    };
    (store, vec![segment])
}

/// A read gate whose byte floor is 0, so every block is a gate job.
fn floor_zero_gate() -> Arc<ReadGate> {
    Arc::new(ReadGate::new(
        CpuGateConfig {
            permits: 2,
            inline_floor_bytes: 0,
            eval_floor_samples: 0,
        },
        Arc::new(InstantClock::new()),
    ))
}

fn log_block_counts(gate: &ReadGate) -> (u64, u64) {
    gate.snapshot()
        .sites
        .iter()
        .find(|counts| counts.site == ReadSite::LogBlock)
        .map_or((0, 0), |counts| (counts.jobs, counts.inline))
}

/// A one-partition `ts` scan (the columnar fast path) over the whole fixture
/// window, with `gate` on the fetcher when given.
fn exec(
    store: &Arc<dyn ObjectStoreBackend>,
    segments: &[SegmentRef],
    gate: Option<Arc<ReadGate>>,
) -> LogsScanExec {
    let fetcher = LogSegmentFetcher::new(Arc::clone(store));
    let fetcher = match gate {
        Some(gate) => fetcher.with_read_gate(gate),
        None => fetcher,
    };
    LogsScanExec::new(
        TENANT,
        fetcher,
        segments,
        1,
        0,
        BLOCKS as i64,
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        Some(&vec![LOG_COL_TS]),
        PhaseAccounting::new(),
        logs_schema_with_declared(&[]),
        Arc::new(Vec::new()),
    )
    .expect("scan")
}

/// Parks the block decode the `log_block` job numbered `target` runs: the
/// first `decode` span entry made off the runtime thread once the gate has
/// dispatched `target` block jobs is inside that job, mid-decode. An entry on
/// the runtime thread is a decode that never left the worker; it is counted,
/// never parked.
struct DecodeProbe {
    park: ParkOnFirstArmedCall,
    gate: Arc<ReadGate>,
    runtime_thread: ThreadId,
    target: u64,
    on_runtime_thread: Arc<AtomicU64>,
}

impl<S> tracing_subscriber::Layer<S> for DecodeProbe
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_enter(&self, id: &span::Id, ctx: LayerContext<'_, S>) {
        if ctx.span(id).is_none_or(|s| s.name() != "decode") {
            return;
        }
        if std::thread::current().id() == self.runtime_thread {
            self.on_runtime_thread.fetch_add(1, Ordering::SeqCst);
        } else if log_block_counts(&self.gate).0 == self.target {
            self.park.arm();
            self.park.now_ns();
        }
    }
}

/// With the last block's decode parked inside its gate job, a concurrent task
/// on the `current_thread` runtime completes; released, the scan's output
/// equals, batch for batch, the same scan run with no gate.
///
/// Fails against a wrap placed after the decode (the block decoded inline and
/// its result moved into the `gate.run` closure: every decode is entered on
/// the runtime thread, no job decodes, and the scan finishes unparked), and
/// against offloading only a part's first block (the `log_block` job count
/// never reaches [`BLOCKS`], so nothing parks).
#[test]
fn log_scan_yields_while_a_block_decodes() {
    run_with_watchdog(
        WATCHDOG,
        || format!("test hung for {WATCHDOG:?}: the parked block decode held the runtime"),
        || {
            let gate = floor_zero_gate();
            let (announce_tx, mut announce_rx) = tokio::sync::mpsc::unbounded_channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let on_runtime_thread = Arc::new(AtomicU64::new(0));
            let probe = DecodeProbe {
                park: ParkOnFirstArmedCall::new(
                    move || {
                        let _ = announce_tx.send(());
                    },
                    release_rx,
                ),
                gate: Arc::clone(&gate),
                runtime_thread: std::thread::current().id(),
                target: BLOCKS,
                on_runtime_thread: Arc::clone(&on_runtime_thread),
            };
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(probe))
                .expect("this binary's only subscriber");
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async {
                let (store, segments) = fixture().await;
                let gated = exec(&store, &segments, Some(Arc::clone(&gate)));
                let stream = gated
                    .execute(0, Arc::new(TaskContext::default()))
                    .expect("execute");
                let mut scan = tokio::spawn(async move {
                    stream
                        .map(|b| b.expect("batch"))
                        .collect::<Vec<RecordBatch>>()
                        .await
                });
                match futures::future::select(Box::pin(announce_rx.recv()), &mut scan).await {
                    futures::future::Either::Left((announced, _)) => {
                        assert_eq!(announced, Some(()), "the decode probe announced");
                    }
                    futures::future::Either::Right(_) => panic!(
                        "the scan finished with no block decode parked in a gate job; \
                         {} decode(s) ran on the runtime thread, {:?} log_block jobs ran",
                        on_runtime_thread.load(Ordering::SeqCst),
                        log_block_counts(&gate),
                    ),
                }
                assert_eq!(gate.running(), 1, "the parked decode holds its permit");
                let concurrent = tokio::spawn(async { 7u32 }).await.expect("task");
                assert_eq!(
                    concurrent, 7,
                    "a concurrent task completes while the decode is parked"
                );

                release_tx.send(()).expect("release the parked decode");
                let got = scan.await.expect("scan task");
                assert_eq!(
                    on_runtime_thread.load(Ordering::SeqCst),
                    0,
                    "no decode ran on the runtime thread"
                );
                assert_eq!(log_block_counts(&gate), (BLOCKS, 0));
                let inline = exec(&store, &segments, None);
                let want: Vec<RecordBatch> = inline
                    .execute(0, Arc::new(TaskContext::default()))
                    .expect("execute")
                    .map(|b| b.expect("batch"))
                    .collect()
                    .await;
                assert!(!want.is_empty(), "the baseline returned rows");
                assert_eq!(got, want, "released, the output equals the inline baseline");
            });
        },
    );
}
