//! The logs plan phase's `page_fetch` phase span (ADR-0044 decision 5, #782).
//!
//! `LogSegmentFetcher::plan_segment` has three branches, and each of them reads
//! object bytes: the predicate-free fast branch reads the footer through the
//! suffix probe, the skip-decidable branch (#761) reads footer + SKIP_IDX +
//! FIELD_DIR, and the fallback reads the object. Two of the three wrapped that
//! read in a `page_fetch` span and recorded `s3_requests`/`s3_bytes` on it; the
//! skip-decidable branch discarded the `BlockRangeStats` it already had and
//! opened no span, so a trace over a selective statement showed its plan-phase
//! requests nowhere. This asserts both branches now record the same two fields.
//!
//! The collector is installed as this binary's global subscriber and this is the
//! binary's only test, so every captured span is this test's: the `page_fetch`
//! span carries no `tenant_hash` field, so there is no field to isolate a
//! sibling test's spans by.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, FieldSel, FieldType, LogRecord, Predicate, RlogConfig, RlogWriter,
    stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{BlockRangeFetcher, LogQuery, LogSegmentFetcher};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const CONTENT_HASH: [u8; 32] = [9u8; 32];
const KEY: &str = "logs/plan-span.rlog";

const BLOCK_RECORDS: usize = 2;
const GROUP_BLOCKS: usize = 2;
const BLOCKS: usize = 6;
const RECORDS: usize = BLOCKS * BLOCK_RECORDS;

/// The declared numeric attribute the prune-only `NumRange` arm resolves
/// against, so the query is skip-decidable (#761).
const CODE_COL: &str = "code";

// ---- fixture -------------------------------------------------------------

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn record(i: usize) -> LogRecord {
    let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: i as i64,
        observed_ts_ns: i as i64 + 1,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("line {i}"),
        trace_id: None,
        span_id: None,
        flags: 1,
        attrs: vec![(
            CODE_COL.to_string(),
            AttrValue::I64((i / BLOCK_RECORDS) as i64),
        )],
    }
}

fn build_object(records: &[LogRecord]) -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records: BLOCK_RECORDS,
        group_target_blocks: GROUP_BLOCKS,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

fn seg_ref(size: u64, records: &[LogRecord]) -> SegmentRef {
    SegmentRef {
        data_object_key: KEY.to_string(),
        object_size: size,
        min_event_ts_ns: records.iter().map(|r| r.ts_ns).min().expect("nonempty"),
        max_event_ts_ns: records.iter().map(|r| r.ts_ns).max().expect("nonempty"),
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: CONTENT_HASH,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// Bytes after the BLOCKS section: a probe of this length carries the footer,
/// SKIP_IDX and PAGE_DIR, so the only GET beyond it is FIELD_DIR at the front.
fn tail_len(bytes: &[u8]) -> u64 {
    let f = ravel_logseg::footer::open(bytes).expect("footer");
    let b = f
        .section(ravel_logseg::footer::kind::BLOCKS)
        .expect("BLOCKS");
    bytes.len() as u64 - (b.offset + b.len)
}

// ---- span capture --------------------------------------------------------

/// One `page_fetch` span's recorded counts.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
struct CapturedSpan {
    name: String,
    signal: Option<String>,
    s3_requests: Option<u64>,
    s3_bytes: Option<u64>,
}

struct Visitor<'a>(&'a mut CapturedSpan);

impl tracing::field::Visit for Visitor<'_> {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "s3_requests" => self.0.s3_requests = Some(value),
            "s3_bytes" => self.0.s3_bytes = Some(value),
            _ => {}
        }
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "signal" {
            self.0.signal = Some(value.to_string());
        }
    }
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

/// Captures each span's fields while it is open and moves it to `completed` on
/// close, so a span id reused after a close cannot overwrite finished counts.
#[derive(Clone, Default)]
struct SpanCollector {
    live: Arc<std::sync::Mutex<HashMap<u64, CapturedSpan>>>,
    completed: Arc<std::sync::Mutex<Vec<CapturedSpan>>>,
}

impl SpanCollector {
    /// Every completed `page_fetch` span, in close order.
    fn page_fetches(&self) -> Vec<CapturedSpan> {
        self.completed
            .lock()
            .expect("completed")
            .iter()
            .filter(|s| s.name == "page_fetch")
            .cloned()
            .collect()
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpanCollector {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut captured = CapturedSpan {
            name: attrs.metadata().name().to_string(),
            ..CapturedSpan::default()
        };
        attrs.record(&mut Visitor(&mut captured));
        self.live
            .lock()
            .expect("live")
            .insert(id.into_u64(), captured);
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if let Ok(mut live) = self.live.lock()
            && let Some(captured) = live.get_mut(&id.into_u64())
        {
            values.record(&mut Visitor(captured));
        }
    }

    fn on_close(&self, id: tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let taken = self
            .live
            .lock()
            .ok()
            .and_then(|mut live| live.remove(&id.into_u64()));
        if let (Some(captured), Ok(mut completed)) = (taken, self.completed.lock()) {
            completed.push(captured);
        }
    }
}

/// Installs the collector as the process-global default. A thread-local
/// `set_default` is not enough: with no global subscriber the debug-level
/// callsite gate is closed and the phase spans never fire.
fn install() -> SpanCollector {
    use tracing_subscriber::layer::SubscriberExt;
    let collector = SpanCollector::default();
    let subscriber = tracing_subscriber::registry().with(collector.clone());
    tracing::subscriber::set_global_default(subscriber).expect("no other global subscriber");
    collector
}

// ---- the test ------------------------------------------------------------

/// Both plan branches that read through [`BlockRangeFetcher`] open one
/// `page_fetch` span and record `s3_requests` and `s3_bytes` on it, from the
/// `BlockRangeStats` their read returned.
///
/// The two branches are exercised in one test, in one current-thread runtime,
/// so the collector's span order is the call order.
///
/// Non-vacuity: dropping the span wrapper from `plan_segment`'s skip-decidable
/// branch (back to `let (footer, skip, field_dir, _stats) = self.block_range
/// .fetch_plan_sections(..).await?;`) leaves that branch with no `page_fetch`
/// span at all, so the second phase captures zero spans and its count
/// assertion fails.
#[tokio::test]
async fn both_ranged_plan_branches_record_their_read_on_a_page_fetch_span() {
    let collector = install();

    let recs: Vec<LogRecord> = (0..RECORDS).map(record).collect();
    let bytes = build_object(&recs);
    let mem = Arc::new(MemoryStore::new());
    mem.put(KEY, bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let store: Arc<dyn ObjectStoreBackend> = mem;

    // Forced onto the ranged path on this small fixture, with the probe sized
    // to the object tail so the request counts below are exact.
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(0)
        .with_block_range(
            BlockRangeFetcher::new(store)
                .with_whole_object_threshold(0)
                .with_suffix_len(tail_len(&bytes)),
        );
    let seg = seg_ref(bytes.len() as u64, &recs);

    // 1. The predicate-free fast branch: the probe alone.
    let acc = QueryAccounting::new();
    let query = LogQuery::new(i64::MIN, i64::MAX);
    let (survivors, _stats, footer) = fetcher
        .plan_segment(&seg, TENANT, &query, &acc)
        .await
        .expect("plan")
        .expect("relevant segment");
    assert_eq!(
        survivors, BLOCKS,
        "every block survives a predicate-free plan"
    );
    assert!(
        footer.is_some(),
        "the fast branch carries its footer forward"
    );

    let fast = collector.page_fetches();
    assert_eq!(fast.len(), 1, "one page_fetch span: {fast:?}");
    assert_eq!(
        fast[0],
        CapturedSpan {
            name: "page_fetch".to_string(),
            signal: Some("logs".to_string()),
            // The suffix probe, and no block byte.
            s3_requests: Some(1),
            s3_bytes: Some(0),
        }
    );

    // 2. The skip-decidable branch (#761): probe + FIELD_DIR, still no block
    // byte. A fresh fetcher and store, so nothing is served from a warm extent
    // and the request count is the branch's own.
    let mem = Arc::new(MemoryStore::new());
    mem.put(KEY, bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(0)
        .with_block_range(
            BlockRangeFetcher::new(store)
                .with_whole_object_threshold(0)
                .with_suffix_len(tail_len(&bytes)),
        );
    let query = LogQuery::new(i64::MIN, i64::MAX).with_prune(Predicate::NumRange {
        field: FieldSel::Attr(CODE_COL.to_string()),
        ty: FieldType::I64,
        min: Some(0),
        max: Some(0),
    });
    let (survivors, stats, footer) = fetcher
        .plan_segment(&seg, TENANT, &query, &QueryAccounting::new())
        .await
        .expect("plan")
        .expect("relevant segment");
    assert_eq!(survivors, 1, "the numeric arm keeps one block");
    assert_eq!(stats.blocks_scanned, 0, "no block decoded");
    assert!(
        footer.is_some(),
        "the skip-decidable branch carries its footer forward"
    );

    let both = collector.page_fetches();
    assert_eq!(
        both.len(),
        2,
        "the skip-decidable branch opens a page_fetch span of its own: {both:?}"
    );
    assert_eq!(
        both[1],
        CapturedSpan {
            name: "page_fetch".to_string(),
            signal: Some("logs".to_string()),
            // The probe covers footer + SKIP_IDX + PAGE_DIR; FIELD_DIR sits at
            // the object front, where no suffix reaches, so it is the second.
            s3_requests: Some(2),
            s3_bytes: Some(0),
        },
        "the same two fields the other branches record"
    );
}
