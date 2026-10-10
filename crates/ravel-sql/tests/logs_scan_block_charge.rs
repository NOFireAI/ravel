//! ADR-1702 decision 6 on the gated `logs` columnar scan: the decoded block
//! the reader still holds stays charged to the query's pool while the next
//! block's job waits for a permit, and until that job's decode frees it.
//!
//! The park sits at the entry of the fetcher's `decode` span inside the
//! second block's gate job: after the job holds its permit and before the
//! reader frees the first block. The binary holds this one test because
//! `tracing` caches a callsite's interest process-wide, so a test in the same
//! process reaching the `decode` callsite with no subscriber could leave the
//! span disabled and the probe blind.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::thread::ThreadId;
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_cpu_gate::{CpuGateConfig, InstantClock, JobSize, ReadGate, ReadSite};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, ColumnSelection, LogRecord, Predicate, RlogConfig, RlogReader, RlogWriter,
    stream_attrs_bytes,
};
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
/// (`block_target_records: 1`), so this is also its block count.
const BLOCKS: u64 = 4;
const KEY: &str = "t/charge0.rlog";
const WATCHDOG: Duration = Duration::from_secs(30);
/// The decoded heap footprint of the fixture's first block (one record)
/// under a `ts`-only scan, whose column selection is
/// `ColumnSelection::fixed_only()`. [`first_block_decoded_bytes`] derives it
/// by decoding that block straight off the fixture object.
const BLOCK_DECODED_BYTES: usize = 128;

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

/// The first block's decoded footprint, decoded straight off the fixture
/// object with the scan's column selection rather than read off the pool.
fn first_block_decoded_bytes(obj: &[u8]) -> usize {
    let reader = RlogReader::new(obj, &RlogConfig::default()).expect("reader");
    let mut scan = reader
        .scan_blocks(
            &Predicate::TsRange {
                min_ns: i64::MIN,
                max_ns: i64::MAX,
            },
            &[],
            &ColumnSelection::fixed_only(),
        )
        .expect("scan");
    let view = scan
        .next_block_columnar(obj)
        .expect("decode")
        .expect("a first block");
    view.decoded_bytes()
}

async fn fixture() -> (Arc<dyn ObjectStoreBackend>, Vec<SegmentRef>, Vec<u8>) {
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
    (store, vec![segment], obj)
}

/// One permit and a byte floor of 0, so every block is a gate job and a
/// second job waits while anything else holds the permit.
fn one_permit_gate() -> Arc<ReadGate> {
    Arc::new(ReadGate::new(
        CpuGateConfig {
            permits: 1,
            inline_floor_bytes: 0,
            eval_floor_samples: 0,
        },
        Arc::new(InstantClock::new()),
    ))
}

fn log_block_jobs(gate: &ReadGate) -> u64 {
    gate.snapshot()
        .sites
        .iter()
        .find(|counts| counts.site == ReadSite::LogBlock)
        .map_or(0, |counts| counts.jobs)
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

fn task_ctx(pool: &Arc<UnboundedMemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool) as Arc<dyn MemoryPool>)
        .build_arc()
        .expect("runtime env");
    Arc::new(TaskContext::default().with_runtime(runtime))
}

/// Parks the block decode the `log_block` job numbered `target` runs, at the
/// entry of its `decode` span: the first such entry off the runtime thread
/// once the gate has dispatched `target` block jobs is inside that job, with
/// its permit held and the reader not yet moved off the previous block.
struct DecodeProbe {
    park: ParkOnFirstArmedCall,
    gate: Arc<ReadGate>,
    runtime_thread: ThreadId,
    target: u64,
}

impl<S> tracing_subscriber::Layer<S> for DecodeProbe
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_enter(&self, id: &span::Id, ctx: LayerContext<'_, S>) {
        if ctx.span(id).is_none_or(|s| s.name() != "decode") {
            return;
        }
        if std::thread::current().id() != self.runtime_thread
            && log_block_jobs(&self.gate) == self.target
        {
            self.park.arm();
            self.park.now_ns();
        }
    }
}

/// With block 1 drained and block 2's job first queued behind a held permit,
/// then holding the permit and parked before its decode, the pool still
/// carries block 1's decoded bytes on top of the batch handed out last:
/// exactly `batch 1 + BLOCK_DECODED_BYTES` both times. Released, the scan's
/// output equals the ungated run's batch for batch, at the end of the stream
/// only the last emitted batch is charged, and dropping the stream returns
/// the pool to 0.
///
/// Fails against releasing the block's charge when its batches drain, before
/// the next job is submitted (the queued check reads the batch alone, 104
/// bytes of 232), and against releasing it once the job holds its permit but
/// before the decode runs (the queued check passes, the parked one reads 104).
#[test]
fn columnar_block_charge_is_held_while_the_next_decode_waits_for_a_permit() {
    run_with_watchdog(
        WATCHDOG,
        || format!("test hung for {WATCHDOG:?}"),
        || {
            let gate = one_permit_gate();
            let (announce_tx, mut announce_rx) = tokio::sync::mpsc::unbounded_channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let probe = DecodeProbe {
                park: ParkOnFirstArmedCall::new(
                    move || {
                        let _ = announce_tx.send(());
                    },
                    release_rx,
                ),
                gate: Arc::clone(&gate),
                runtime_thread: std::thread::current().id(),
                target: 2,
            };
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(probe))
                .expect("this binary's only subscriber");
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async {
                let (store, segments, obj) = fixture().await;
                let block_bytes = first_block_decoded_bytes(&obj);
                assert_eq!(block_bytes, BLOCK_DECODED_BYTES, "the fixture's block size");

                let pool = Arc::new(UnboundedMemoryPool::default());
                let gated = exec(&store, &segments, Some(Arc::clone(&gate)));
                let mut stream = gated.execute(0, task_ctx(&pool)).expect("execute");
                let first = stream
                    .next()
                    .await
                    .expect("a first batch")
                    .expect("first batch");
                assert_eq!(log_block_jobs(&gate), 1, "block 1 decoded in its job");
                let batch_bytes = first.get_array_memory_size();
                assert_eq!(
                    pool.reserved(),
                    batch_bytes + block_bytes,
                    "block 1 drained: its batch and its decoded block"
                );

                // Something else holds the only permit.
                let (blocker_started_tx, blocker_started_rx) = tokio::sync::oneshot::channel();
                let (unblock_tx, unblock_rx) = std::sync::mpsc::channel::<()>();
                let blocker_gate = Arc::clone(&gate);
                let blocker = tokio::spawn(async move {
                    blocker_gate
                        .run(ReadSite::CatalogPart, JobSize::Bytes(u64::MAX), move || {
                            let _ = blocker_started_tx.send(());
                            let _ = unblock_rx.recv();
                        })
                        .await
                });
                blocker_started_rx
                    .await
                    .expect("the blocker holds the permit");

                let next = tokio::spawn(async move {
                    let batch = stream.next().await;
                    (stream, batch)
                });
                while gate.queued() == 0 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(log_block_jobs(&gate), 1, "block 2's job has no permit yet");
                assert_eq!(
                    pool.reserved(),
                    batch_bytes + block_bytes,
                    "block 2's job queued for a permit: block 1 is still charged"
                );

                unblock_tx.send(()).expect("release the blocker");
                blocker.await.expect("blocker task").expect("blocker job");
                assert_eq!(
                    announce_rx.recv().await,
                    Some(()),
                    "block 2's decode parked"
                );
                assert_eq!(gate.running(), 1, "the parked job holds the permit");
                assert_eq!(
                    pool.reserved(),
                    batch_bytes + block_bytes,
                    "block 2's job holds its permit, decode not run: block 1 is still charged"
                );

                release_tx.send(()).expect("release the parked decode");
                let (mut stream, second) = next.await.expect("next batch task");
                let mut got = vec![first, second.expect("a second batch").expect("batch")];
                while let Some(batch) = stream.next().await {
                    got.push(batch.expect("batch"));
                }
                let last = got.last().expect("batches").get_array_memory_size();
                assert_eq!(
                    pool.reserved(),
                    last,
                    "at the end of the stream only the last emitted batch is charged"
                );
                drop(stream);
                assert_eq!(pool.reserved(), 0, "the dropped stream released everything");
                assert_eq!(log_block_jobs(&gate), BLOCKS);

                let want: Vec<RecordBatch> = exec(&store, &segments, None)
                    .execute(0, Arc::new(TaskContext::default()))
                    .expect("execute")
                    .map(|b| b.expect("batch"))
                    .collect()
                    .await;
                assert!(!want.is_empty(), "the baseline returned rows");
                assert_eq!(got, want, "released, the output equals the ungated run");
            });
        },
    );
}
