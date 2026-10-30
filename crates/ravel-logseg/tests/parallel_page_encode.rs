//! The parallel column-page encode (ADR-3492 decision 7): an object written
//! through `RlogWriter::finish_on` with a multi-threaded executor that returns
//! results out of order is byte-identical to `RlogWriter::finish`, on both
//! writer paths and under both page choices.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_logseg::encoding::Enc;
use ravel_logseg::footer::{kind, open};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{
    AttrValue, ColumnarLogBatch, LogRecord, LogSegError, LogStreamId, ObjectIdentity,
    PAGE_JOB_FLOOR, PageChoice, PageEncodeDone, PageEncodeJob, RlogConfig, RlogWriter,
    read_section, stream_attrs_bytes,
};

#[path = "../benches/common/ingest_shape.rs"]
mod ingest_shape;
#[path = "../benches/common/page_pool.rs"]
mod page_pool;

use ingest_shape::{PUSH_BATCH, ingest_records};
use page_pool::ScopedPool;

const THREADS: usize = 4;
const CHOICES: [PageChoice; 2] = [PageChoice::Exhaustive, PageChoice::Estimated];

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0x3A; 16],
        shard: 3,
        writer_id: [0xA3; 16],
        writer_epoch: 3,
        writer_seq: 30,
    }
}

/// A writer holding `records`, through the row path or in columnar batches of
/// `batch` records.
fn writer(cfg: RlogConfig, records: &[LogRecord], columnar: bool, batch: usize) -> RlogWriter {
    let mut w = RlogWriter::new(cfg, identity());
    if columnar {
        for chunk in records.chunks(batch.max(1)) {
            let b = ColumnarLogBatch::try_from_records(chunk).expect("batch");
            w.push_columnar(b).expect("push_columnar");
        }
    } else {
        for r in records {
            w.push(r.clone()).expect("push");
        }
    }
    w
}

/// The serial object and the one `pool` encoded, for the same input.
fn both(
    cfg: RlogConfig,
    records: &[LogRecord],
    columnar: bool,
    batch: usize,
    pool: &mut ScopedPool,
) -> (Vec<u8>, Vec<u8>) {
    let serial = writer(cfg, records, columnar, batch)
        .finish()
        .expect("finish");
    let parallel = writer(cfg, records, columnar, batch)
        .finish_on(pool)
        .expect("finish_on");
    (serial, parallel)
}

fn page_dir(object: &[u8]) -> PageDir {
    let cfg = RlogConfig::default();
    let ftr = open(object).expect("open");
    let raw = read_section(object, ftr.section(kind::PAGE_DIR).expect("PAGE_DIR"), &cfg)
        .expect("read PAGE_DIR");
    PageDir::decode(&raw).expect("decode PAGE_DIR")
}

/// Row group dictionary pages (tag 12) the object carries.
fn dict_pages(object: &[u8]) -> usize {
    page_dir(object)
        .groups
        .iter()
        .flat_map(|g| &g.chunks)
        .flat_map(|c| &c.pages)
        .filter(|p| p.enc == Enc::DictPage)
        .count()
}

/// The ingest flush shape the benchmark encodes, one 7.5k-record block.
#[test]
fn ingest_shape_flush_is_byte_identical() {
    let records = ingest_records(7_500);
    for choice in CHOICES {
        for columnar in [false, true] {
            let cfg = RlogConfig {
                page_choice: choice,
                ..RlogConfig::default()
            };
            let mut pool = ScopedPool::new(THREADS);
            let (serial, parallel) = both(cfg, &records, columnar, PUSH_BATCH, &mut pool);
            assert!(
                serial == parallel,
                "{choice:?} columnar={columnar}: parallel encode differs from finish"
            );
            assert_eq!(pool.split_calls, 1, "one block, split into column jobs");
            assert_eq!(
                pool.out_of_order_calls, 1,
                "the pool returned the block's results out of job order"
            );
        }
    }
}

/// The same records in 1000-record blocks, four to a row group, so the row
/// group dictionary decision runs over blocks whose pages were sealed by
/// different jobs, and wins for some columns.
#[test]
fn multi_block_row_groups_are_byte_identical() {
    let records = ingest_records(7_500);
    for choice in CHOICES {
        for columnar in [false, true] {
            let cfg = RlogConfig {
                page_choice: choice,
                block_target_records: 1_000,
                group_target_blocks: 4,
                ..RlogConfig::default()
            };
            let mut pool = ScopedPool::new(THREADS);
            let (serial, parallel) = both(cfg, &records, columnar, PUSH_BATCH, &mut pool);
            assert!(dict_pages(&serial) > 0, "{choice:?}: no row group dictionary");
            assert!(
                serial == parallel,
                "{choice:?} columnar={columnar}: parallel encode differs from finish"
            );
            assert_eq!(pool.split_calls, 8, "eight blocks, each split");
            assert!(pool.out_of_order_calls > 0, "no block came back out of order");
        }
    }
}

const WORDS: &[&str] = &["alpha", "beta", "gamma", "delta", "", "epsilon"];
const NAMES: &[&str] = &["svc", "env", "code", "dur", "flag", "blob", "host", "path"];

fn stream_id(n: u8) -> LogStreamId {
    LogStreamId([n; 16])
}

fn arb_value() -> impl Strategy<Value = AttrValue> {
    prop_oneof![
        3 => proptest::sample::select(WORDS).prop_map(|w| AttrValue::Str(w.to_string())),
        1 => "[a-z]{0,12}".prop_map(AttrValue::Str),
        2 => (-4i64..4).prop_map(AttrValue::I64),
        1 => any::<i64>().prop_map(AttrValue::I64),
        1 => any::<f64>().prop_map(AttrValue::F64),
        1 => any::<bool>().prop_map(AttrValue::Bool),
        1 => proptest::collection::vec(any::<u8>(), 0..6).prop_map(AttrValue::Bytes),
    ]
}

fn arb_record() -> impl Strategy<Value = LogRecord> {
    (
        0u8..3,
        0i64..1_000,
        0i64..3,
        proptest::collection::vec(proptest::sample::select(WORDS), 0..5),
        0u8..24,
        proptest::collection::vec((proptest::sample::select(NAMES), arb_value()), 0..8),
        proptest::option::of(any::<[u8; 16]>()),
        proptest::option::of(any::<[u8; 8]>()),
    )
        .prop_map(|(stream, ts, lag, words, sev, attrs, trace, span)| {
            let mut seen = std::collections::HashSet::new();
            LogRecord {
                stream_id: stream_id(stream),
                stream_attrs: stream_attrs_bytes(
                    &[("service.name".to_string(), AttrValue::Str(format!("s{stream}")))],
                    "scope",
                    "1",
                    &[],
                ),
                ts_ns: ts,
                observed_ts_ns: ts + lag,
                severity_num: sev,
                severity_text: ["INFO", "WARN", "ERROR"][usize::from(sev % 3)].to_string(),
                body: words.join(" "),
                trace_id: trace,
                span_id: span,
                flags: u32::from(sev % 2),
                attrs: attrs
                    .into_iter()
                    .filter(|(name, _)| seen.insert(*name))
                    .map(|(name, v)| (name.to_string(), v))
                    .collect(),
            }
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn generated_records_are_byte_identical(
        records in proptest::collection::vec(arb_record(), 1..200),
        block in 4usize..48,
        group in 1usize..5,
        batch in 1usize..80,
        estimated in any::<bool>(),
        columnar in any::<bool>(),
    ) {
        let cfg = RlogConfig {
            page_choice: if estimated { PageChoice::Estimated } else { PageChoice::Exhaustive },
            block_target_records: block,
            group_target_blocks: group,
            ..RlogConfig::default()
        };
        let mut pool = ScopedPool::new(THREADS);
        let (serial, parallel) = both(cfg, &records, columnar, batch, &mut pool);
        prop_assert!(serial == parallel, "parallel encode differs from finish");
    }
}

/// `PageEncodeJob` and `PageEncodeDone` cross threads and borrow nothing.
#[test]
fn jobs_are_send_and_static() {
    fn send_static<T: Send + 'static>() {}
    send_static::<PageEncodeJob>();
    send_static::<PageEncodeDone>();
}

/// One record with `dynamic` string attributes and no trace, span or overflow.
fn narrow_record(dynamic: usize) -> LogRecord {
    LogRecord {
        stream_id: stream_id(1),
        stream_attrs: stream_attrs_bytes(&[], "scope", "1", &[]),
        ts_ns: 10,
        observed_ts_ns: 10,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "hello".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: (0..dynamic)
            .map(|i| (format!("a{i}"), AttrValue::Str(format!("v{i}"))))
            .collect(),
    }
}

/// `(jobs, pages)` per executor call for one object of `record`.
fn job_shape(record: LogRecord) -> Vec<(usize, usize)> {
    let mut calls = Vec::new();
    let mut exec = |jobs: Vec<PageEncodeJob>| -> Result<Vec<PageEncodeDone>, LogSegError> {
        calls.push((
            jobs.len(),
            jobs.iter().map(PageEncodeJob::page_count).sum::<usize>(),
        ));
        Ok(jobs.into_iter().map(PageEncodeJob::run).collect())
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity());
    w.push(record).expect("push");
    w.finish_on(&mut exec).expect("finish_on");
    calls
}

#[test]
fn a_block_below_the_page_floor_is_one_job() {
    // Seven fixed value pages; observed_ts equals ts, so its page is a
    // reference to ts, still one page.
    assert_eq!(PAGE_JOB_FLOOR, 8);
    assert_eq!(job_shape(narrow_record(0)), vec![(1, 7)]);
}

#[test]
fn a_block_at_the_page_floor_is_one_job_per_column() {
    assert_eq!(job_shape(narrow_record(1)), vec![(8, 8)]);
    assert_eq!(job_shape(narrow_record(5)), vec![(12, 12)]);
}

#[test]
fn an_executor_that_drops_a_result_is_refused() {
    let mut exec = |jobs: Vec<PageEncodeJob>| -> Result<Vec<PageEncodeDone>, LogSegError> {
        Ok(jobs.into_iter().skip(1).map(PageEncodeJob::run).collect())
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity());
    w.push(narrow_record(3)).expect("push");
    let err = w.finish_on(&mut exec).expect_err("a missing result must fail");
    assert!(matches!(err, LogSegError::PageEncode(_)), "{err:?}");
}

#[test]
fn an_executor_error_propagates() {
    let mut exec = |_: Vec<PageEncodeJob>| -> Result<Vec<PageEncodeDone>, LogSegError> {
        Err(LogSegError::PageEncode("pool closed".into()))
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity());
    w.push(narrow_record(3)).expect("push");
    let err = w.finish_on(&mut exec).expect_err("executor error");
    assert!(matches!(&err, LogSegError::PageEncode(m) if m == "pool closed"), "{err:?}");
}
