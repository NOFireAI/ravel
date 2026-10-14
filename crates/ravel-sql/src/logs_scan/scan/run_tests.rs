//! Issue #2795: with `blocks_per_job` above 1 the columnar logs scan decodes
//! up to that many consecutive blocks of a segment in one `log_block` read
//! gate job ([`ColumnarRunJob`]).
//!
//! # The fixture
//!
//! Two segments of three streams of twelve records, four records per block,
//! so nine blocks per segment in one row group (the writer's default group is
//! 32 blocks). The writer orders rows by stream id, so each segment drains one
//! stream's three blocks, then the next stream's. Record `i` of stream `j` has
//! `k = j + 3i` and `ts = 2k + s`, so block `b` of stream `j` starts at
//! `ts = 2(j + 12b) + s`: block minima are not monotone in drain order, and
//! every stream's last block (drain positions 2, 5 and 8) starts at 48 or
//! above.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use datafusion::physical_expr::expressions::lit;
use ravel_catalog::SegmentLevel;
use ravel_cpu_gate::{CpuGateConfig, InstantClock};
use ravel_logseg::record::stream_attrs_bytes;
use ravel_logseg::{ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const SEGMENTS: usize = 2;
const STREAMS: usize = 3;
const PER_STREAM: usize = 12;
const RECORDS_PER_BLOCK: usize = 4;
const BLOCKS_PER_SEGMENT: usize = STREAMS * PER_STREAM / RECORDS_PER_BLOCK;
const TOTAL_BLOCKS: usize = SEGMENTS * BLOCKS_PER_SEGMENT;
/// A fixed TopK threshold over the fixture: in each segment the last block
/// of every stream (minima 48, 50, 52, plus `s`) is above it and the rest are
/// not.
const CEILING: i64 = 30;

fn object(segment: usize, group_blocks: Option<usize>) -> Vec<u8> {
    let mut cfg = RlogConfig {
        block_target_records: RECORDS_PER_BLOCK,
        ..RlogConfig::default()
    };
    if let Some(blocks) = group_blocks {
        cfg.group_target_blocks = blocks;
    }
    let identity = ObjectIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: [5u8; 16],
        writer_epoch: 1,
        writer_seq: segment as u64 + 1,
    };
    let mut writer = RlogWriter::new(cfg, identity);
    for j in 0..STREAMS {
        let resource = vec![("service.name".to_string(), AttrValue::Str(format!("s{j}")))];
        for i in 0..PER_STREAM {
            let k = j + STREAMS * i;
            let ts = (2 * k + segment) as i64;
            writer
                .push(ravel_logseg::LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("s{segment} k{k} {}", "payload ".repeat(8)),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: vec![("a".to_string(), AttrValue::Str(format!("a{k}")))],
                })
                .expect("push");
        }
    }
    writer.finish().expect("finish")
}

/// The two-segment fixture, each object under a key naming `tag`, so a test
/// arming [`RUN_POINTS`] for its keys reads only its own runs.
async fn fixture(
    tag: &str,
    group_blocks: Option<usize>,
) -> (Arc<dyn ObjectStoreBackend>, Vec<SegmentRef>) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for s in 0..SEGMENTS {
        let key = format!("t/{tag}-{s}.rlog");
        let obj = object(s, group_blocks);
        store
            .put(&key, bytes::Bytes::from(obj.clone()), PutOptions::default())
            .await
            .expect("put");
        segments.push(SegmentRef {
            data_object_key: key,
            object_size: obj.len() as u64,
            min_event_ts_ns: s as i64,
            max_event_ts_ns: (2 * (STREAMS * PER_STREAM - 1) + s) as i64,
            ingest_hour_bucket: 0,
            sample_count: (STREAMS * PER_STREAM) as u64,
            series_count: STREAMS as u64,
            shard: 0,
            content_hash: [3u8; 32],
            writer_id: Uuid::from_u128(5),
            writer_epoch: 1,
            writer_seq: s as u64 + 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        });
    }
    (store, segments)
}

/// A read gate whose byte floor is 0, so every block decode is a gate job.
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

fn log_block_jobs(gate: &ReadGate) -> (u64, u64) {
    gate.snapshot()
        .sites
        .iter()
        .find(|counts| counts.site == ReadSite::LogBlock)
        .map_or((0, 0), |counts| (counts.jobs, counts.inline))
}

/// A confirmed TopK threshold fixed at `ts < ceiling`.
fn fixed_threshold(ceiling: i64) -> TopKThreshold {
    let ts: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));
    let bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&ts),
        Operator::Lt,
        lit(ScalarValue::TimestampNanosecond(Some(ceiling), None)),
    ));
    TopKThreshold {
        filter: Arc::new(DynamicFilterPhysicalExpr::new(vec![ts], bound)),
        confirmed: true,
    }
}

/// A one-partition `ts, body` scan (the columnar path) over the fixture on
/// a floor-zero gate, decoding `blocks` blocks per job.
fn exec(
    store: &Arc<dyn ObjectStoreBackend>,
    segments: &[SegmentRef],
    gate: &Arc<ReadGate>,
    blocks: u32,
) -> LogsScanExec {
    LogsScanExec::new(
        TENANT,
        LogSegmentFetcher::new(Arc::clone(store)).with_read_gate(Arc::clone(gate)),
        segments,
        1,
        0,
        i64::MAX,
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        Some(&vec![
            crate::logs_schema::LOG_COL_TS,
            crate::logs_schema::LOG_COL_BODY,
        ]),
        PhaseAccounting::new(),
        crate::logs_schema::logs_schema_with_declared(&[]),
        Arc::new(Vec::new()),
    )
    .expect("scan")
    .with_blocks_per_job(NonZeroU32::new(blocks).expect("nonzero"))
}

struct Ran {
    batches: Vec<RecordBatch>,
    jobs: u64,
    blocks_scanned: usize,
    blocks_skipped: usize,
    columnar_batches: usize,
}

fn metric_total(plan: &LogsScanExec, name: &str) -> usize {
    plan.metrics()
        .expect("metrics")
        .iter()
        .filter(|m| m.value().name() == name && m.labels().is_empty())
        .map(|m| m.value().as_usize())
        .sum()
}

async fn run(plan: LogsScanExec, gate: &ReadGate) -> Ran {
    let batches: Vec<RecordBatch> = plan
        .execute(0, Arc::new(TaskContext::default()))
        .expect("execute")
        .map(|b| b.expect("batch"))
        .collect()
        .await;
    let (jobs, inline) = log_block_jobs(gate);
    assert_eq!(inline, 0, "a floor-zero gate runs nothing inline");
    Ran {
        batches,
        jobs,
        blocks_scanned: metric_total(&plan, "blocks_scanned"),
        blocks_skipped: metric_total(&plan, "blocks_skipped_by_threshold"),
        columnar_batches: metric_total(&plan, "columnar_batches"),
    }
}

/// Runs the fixture under `tag` with `blocks` per job, shaped by `shape`.
async fn scan_with(
    tag: &str,
    group_blocks: Option<usize>,
    blocks: u32,
    shape: impl Fn(LogsScanExec) -> LogsScanExec,
) -> Ran {
    let (store, segments) = fixture(tag, group_blocks).await;
    let gate = floor_zero_gate();
    run(shape(exec(&store, &segments, &gate, blocks)), &gate).await
}

/// Runs the plain fixture with `blocks` per job.
async fn scan(tag: &str, blocks: u32) -> Ran {
    scan_with(tag, None, blocks, |plan| plan).await
}

/// For 1, 2, 4 and 8 blocks per job the scan returns the same batches, and
/// each segment's nine blocks take `ceil(9 / n)` `log_block` jobs: 9, 5, 3
/// and 2 per segment.
///
/// Fails with the run's loop cut to one block (the counts read 18 for every
/// `n`).
#[tokio::test]
async fn runs_return_the_same_batches_in_ceil_n_jobs_per_segment() {
    let one = scan("eq1", 1).await;
    assert_eq!(one.jobs, TOTAL_BLOCKS as u64, "one job per block at 1");
    assert_eq!(one.blocks_scanned, TOTAL_BLOCKS);
    assert!(one.columnar_batches > 0, "the scan took the columnar path");
    for (n, per_segment) in [(2, 5), (4, 3), (8, 2)] {
        let ran = scan(&format!("eq{n}"), n).await;
        assert_eq!(ran.batches, one.batches, "the same batches at {n}");
        assert_eq!(ran.blocks_scanned, TOTAL_BLOCKS, "every block at {n}");
        assert_eq!(
            ran.jobs,
            (SEGMENTS * per_segment) as u64,
            "ceil(9 / {n}) jobs per segment"
        );
    }
}

/// A run stops at a row-group boundary: with two-block row groups each
/// segment's groups are blocks `0-1, 2-3, 4-5, 6-7, 8`, so four blocks per
/// job take five jobs per segment rather than three, with the same batches.
///
/// Fails with the row-group check removed from the run's stop condition
/// (three jobs per segment).
#[tokio::test]
async fn a_row_group_boundary_ends_a_run() {
    let one = scan_with("rg1", Some(2), 1, |plan| plan).await;
    let four = scan_with("rg4", Some(2), 4, |plan| plan).await;
    assert_eq!(four.batches, one.batches);
    assert_eq!(one.jobs, TOTAL_BLOCKS as u64);
    assert_eq!(four.jobs, (SEGMENTS * 5) as u64);
}

/// A run stops once its charged batch bytes reach the byte cap: at a cap of
/// one byte, every run is its first block alone, so four blocks per job take
/// one job per block, with the same batches.
///
/// Fails with the byte-cap check removed from the run's stop condition (three
/// jobs per segment).
#[tokio::test]
async fn the_byte_cap_ends_a_run() {
    let one = scan("cap1", 1).await;
    let four = scan_with("cap4", None, 4, |plan| plan.with_run_byte_cap(1)).await;
    assert_eq!(four.batches, one.batches);
    assert_eq!(four.jobs, TOTAL_BLOCKS as u64);
}

/// Under a fixed TopK threshold, four blocks per job skip and decode exactly
/// the blocks one per job does: in each segment the threshold skips blocks 2,
/// 5 and 8, which a run stops before rather than decodes, so both counts and
/// the batches agree, and each segment takes three jobs (`0-1`, `3-4`, `6-7`)
/// rather than six.
///
/// Fails with the run's threshold check removed: runs `0-3` and `4-7` then
/// decode blocks 2 and 5, so four blocks per job skip 2 and scan 16 against
/// one per job's 6 and 12.
#[tokio::test]
async fn a_run_decides_each_block_against_the_threshold() {
    let threshold = |plan: LogsScanExec| plan.with_topk(Some(fixed_threshold(CEILING)));
    let one = scan_with("th1", None, 1, threshold).await;
    let four = scan_with("th4", None, 4, threshold).await;
    assert_eq!(
        one.blocks_skipped,
        3 * SEGMENTS,
        "the oracle skips 3 per segment"
    );
    assert_eq!(one.blocks_scanned, 6 * SEGMENTS);
    assert_eq!(
        (four.blocks_skipped, four.blocks_scanned),
        (one.blocks_skipped, one.blocks_scanned),
        "skipped and scanned blocks equal one block per job's"
    );
    assert_eq!(four.batches, one.batches);
    assert_eq!(one.jobs, (6 * SEGMENTS) as u64);
    assert_eq!(four.jobs, (3 * SEGMENTS) as u64);
}

/// The memory bounds of a run, read from the charges the run and the stream
/// record ([`RunPoint`]):
/// - after each block, `resident` is that block's decoded bytes alone: the
///   reader frees a block's columns when it decodes the next, and the run
///   never charges two decoded blocks at once;
/// - after each block, the run's batch charge is at least the Arrow bytes of
///   the batches it holds;
/// - as the job returns, `resident` is the last block's decoded bytes, the
///   one block the reader still holds, as after a one-block job;
/// - as the stream takes the batches back, before emitting any, the charge
///   still covers them.
///
/// Fails with `resident` grown per block rather than resized (the second
/// block's point reads two blocks' bytes); with the batch charge taken once
/// as the run ends instead of per block (a block point reads 0 charged); and
/// with the charge released as the stream takes the batches back (the
/// resumed point reads 0 charged).
#[tokio::test]
async fn a_run_charges_one_decoded_block_and_every_held_batch() {
    let keys: Vec<String> = (0..SEGMENTS).map(|s| format!("t/mem-{s}.rlog")).collect();
    RUN_POINTS
        .lock()
        .expect("run points")
        .extend(keys.iter().map(|k| (k.clone(), Vec::new())));
    let ran = scan("mem", 4).await;
    let points: Vec<RunPoint> = {
        let mut armed = RUN_POINTS.lock().expect("run points");
        let mut points = Vec::new();
        armed.retain(|(k, recorded)| {
            if keys.contains(k) {
                points.extend(recorded.iter().copied());
                false
            } else {
                true
            }
        });
        points
    };
    assert_eq!(ran.jobs, (SEGMENTS * 3) as u64);
    let (mut blocks, mut returns, mut resumed) = (0, 0, 0);
    for point in &points {
        match *point {
            RunPoint::Block {
                resident,
                decoded,
                charged,
                held,
            } => {
                blocks += 1;
                assert!(decoded > 0, "a block decoded: {point:?}");
                assert_eq!(resident, decoded, "one decoded block charged: {point:?}");
                assert!(held > 0, "the run holds batches: {point:?}");
                assert!(charged >= held, "held batches charged: {point:?}");
            }
            RunPoint::JobReturn { resident, decoded } => {
                returns += 1;
                assert!(decoded > 0);
                assert_eq!(resident, decoded, "the reader's one block: {point:?}");
            }
            RunPoint::Resumed { charged, held } => {
                resumed += 1;
                assert!(held > 0);
                assert!(charged >= held, "still charged when taken back: {point:?}");
            }
        }
    }
    assert_eq!(blocks, TOTAL_BLOCKS, "a point per run block");
    assert_eq!(returns, SEGMENTS * 3, "a point per job");
    assert_eq!(resumed, SEGMENTS * 3, "a point per job taken back");
}

/// A run's job size is one block's job size per block it may decode.
#[test]
fn a_run_is_sized_per_block() {
    assert_eq!(
        run_job_size(JobSize::Bytes(1_000), 4),
        JobSize::Bytes(4_000)
    );
    assert_eq!(
        run_job_size(JobSize::Bytes(1_000), 1),
        JobSize::Bytes(1_000)
    );
    assert_eq!(
        run_job_size(JobSize::Bytes(u64::MAX), 2),
        JobSize::Bytes(u64::MAX)
    );
}
