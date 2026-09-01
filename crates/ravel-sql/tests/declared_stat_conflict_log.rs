//! The ADR-0873 decision 4 conflict LOG LINE: when the `SegmentRef` stamp and
//! the ADR-0850 `.cstat` entry disagree about one segment and one column, the
//! disagreement is not only counted but recorded with the segment's content
//! hash and both claimed triples.
//!
//! The counter alone cannot be acted on: it says how often a conflict was
//! observed, never which object or which two claims, and neither is
//! recoverable from a number. This file pins the line that carries them.
//!
//! ONE test, in its own integration binary, and both are load-bearing.
//! `tracing` caches a callsite's interest process-wide: a sibling test that
//! reaches the same `warn!` with no subscriber installed on ITS thread can have
//! the callsite cached as uninteresting, after which the line is never emitted
//! for anyone and this assertion fails for a reason that has nothing to do with
//! the code under test. `tests/declared_stat_carrier_conflicts.rs` holds the
//! counter assertions for exactly that reason; adding a second test here would
//! reintroduce the race.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use datafusion::common::stats::Precision;
use ravel_catalog::{
    DeclaredColumnStats, EntryIdentity, LoadedColumnStats, SegmentLevel, SegmentRef, Snapshot,
};
use ravel_commit::declared_stats::{encode as encode_stamp, read_commit_record};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue};
use ravel_proto::commit::v1::CommitRecord;
use ravel_query::LogSegmentFetcher;
use ravel_sql::{DeclaredColumn, DeclaredType, LogsTableProvider, declared_stat_carrier_conflicts};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const COL: &str = "EventDate";
/// Rows in the fabricated segment, and the figure the stamp's NULL count is
/// reconciled against.
const SAMPLE_COUNT: u64 = 4;
/// The conflicting segment's content hash, distinct from the all-zero default
/// so the assertion cannot pass on an unrelated field.
const CONTENT_HASH: [u8; 32] = [0xab; 32];

/// A `tracing` writer that appends every emitted byte to a shared buffer so the
/// test can assert what was logged.
#[derive(Clone)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLog {
    type Writer = CapturedLog;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// One segment carrying `CONTENT_HASH`, stamped with `(min, max, null_count)`
/// through the carrier read that binds the row-count clauses (the only route to
/// a non-empty [`DeclaredColumnStats`]).
fn stamped_segment(min: i64, max: i64, null_count: u64) -> SegmentRef {
    let stat = DeclaredColumnStat::new(
        COL,
        DeclaredStatType::I64,
        Some(DeclaredStatValue::I64(min)),
        Some(DeclaredStatValue::I64(max)),
        null_count,
    )
    .expect("valid stamp");
    let record = CommitRecord {
        sample_count: SAMPLE_COUNT,
        declared_column_stats: vec![encode_stamp(&stat)],
        ..CommitRecord::default()
    };
    let seg = SegmentRef {
        data_object_key: "logs/seg-1.rlog".to_string(),
        object_size: 1,
        min_event_ts_ns: 0,
        max_event_ts_ns: 1_000,
        ingest_hour_bucket: 0,
        sample_count: SAMPLE_COUNT,
        series_count: 0,
        shard: 0,
        content_hash: CONTENT_HASH,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: DeclaredColumnStats::from_validated(&read_commit_record(&record)),
    };
    assert_eq!(seg.declared_column_stats.len(), 1, "fixture stamp is valid");
    seg
}

fn identity_of(seg: &SegmentRef) -> EntryIdentity {
    (
        seg.ingest_hour_bucket,
        seg.shard,
        *seg.writer_id.as_bytes(),
        seg.writer_epoch,
        seg.writer_seq,
    )
}

/// The `.cstat` entry for the same segment and column, with no value dictionary
/// (this path reads neither).
fn loaded_cstat(seg: &SegmentRef, min: i64, max: i64, null_count: u64) -> Arc<LoadedColumnStats> {
    let stat = ColumnStat {
        name: COL.to_string(),
        declared_type: 2, // ravel.sys.v1.TypedAttrColumnType::I64
        non_null_count: SAMPLE_COUNT - null_count,
        null_count,
        min: Some(ColumnValue {
            kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(min)),
        }),
        max: Some(ColumnValue {
            kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(max)),
        }),
        dictionary_present: false,
        dictionary: Vec::new(),
        sum: None,
    };
    let mut segments = HashMap::new();
    segments.insert(
        identity_of(seg),
        ColumnStatsSegment {
            ingest_hour_bucket: seg.ingest_hour_bucket,
            shard: seg.shard,
            writer_id: seg.writer_id.as_bytes().to_vec(),
            writer_epoch: seg.writer_epoch,
            writer_seq: seg.writer_seq,
            columns: vec![stat],
        },
    );
    Arc::new(LoadedColumnStats {
        segments,
        part_blake3: Vec::new(),
    })
}

/// A stamp and a `.cstat` entry that disagree on every field of the triple, so
/// the line has three pairs to distinguish and a partial line cannot pass:
/// the column is `Absent`, the tally moves once, and the log line names the
/// segment by content hash plus both triples.
///
/// Prove-the-test: delete the `tracing::warn!` call in
/// `record_carrier_conflict` (crates/ravel-sql/src/logs_scan.rs) and every
/// `contains` assertion fails while the `Absent` and tally assertions still
/// pass, which is the state before this line existed. Drop the
/// `segment_content_hash` field alone and only the hash assertion fails; log
/// the stamp triple only and the three `cstat_*` assertions fail.
#[test]
fn a_conflict_logs_the_segment_content_hash_and_both_triples() {
    let seg = stamped_segment(200, 500, 1);
    let stats = loaded_cstat(&seg, -1, 499, 2);
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let provider = LogsTableProvider::new(
        Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TENANT,
        LogSegmentFetcher::new(backend),
        QueryAccounting::new(),
    )
    .with_declared_columns(vec![DeclaredColumn::new(COL, DeclaredType::I64)])
    .with_column_stats(Some(stats));
    let plan = provider.plan_filters(4, &[]).expect("plan_filters");

    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(CapturedLog(Arc::clone(&buffer)))
        .with_max_level(Level::WARN)
        .with_ansi(false)
        .finish();
    let before = declared_stat_carrier_conflicts();
    let resolved = tracing::subscriber::with_default(subscriber, || {
        plan.partition_statistics(None)
            .expect("partition_statistics")
    });
    let delta = declared_stat_carrier_conflicts() - before;

    assert_eq!(
        resolved.column_statistics[ravel_sql::FIRST_DECLARED_COL].min_value,
        Precision::Absent,
        "conflicting carriers grant no coverage"
    );
    assert_eq!(delta, 1, "one conflicted segment, observed once");

    let logged = String::from_utf8(buffer.lock().expect("log buffer lock").clone())
        .expect("utf8 log output");
    // Field NAMES bound to their exact figures are the contract; the
    // Option/ScalarValue wrapper text is DataFusion's Debug representation
    // and may change across upgrades without changing conflict behaviour. For
    // each field, the FIRST numeric token after `name=` must equal the
    // expected value exactly -- independent substring checks would accept
    // swapped extrema or a superstring like -10 for -1.
    for needle in [COL, hex::encode(CONTENT_HASH).as_str()] {
        assert!(
            logged.contains(needle),
            "the conflict log line must carry {needle:?}; logged:\n{logged}"
        );
    }
    // The field's whitespace-delimited token (e.g. `stamp_min=Some(Int64(200))`
    // or a future plain `stamp_min=200`) carries the value as its LAST numeric
    // run -- the first can be the wrapper's own `64`.
    let value_of = |field: &str| -> String {
        let at = logged
            .find(field)
            .unwrap_or_else(|| panic!("field {field:?} absent; logged:\n{logged}"))
            + field.len();
        let token: &str = logged[at..].split_whitespace().next().unwrap_or("");
        let mut runs: Vec<String> = Vec::new();
        let mut cur = String::new();
        for c in token.chars() {
            if c.is_ascii_digit() || (c == '-' && cur.is_empty()) {
                cur.push(c);
            } else if !cur.is_empty() {
                runs.push(std::mem::take(&mut cur));
            }
        }
        if !cur.is_empty() {
            runs.push(cur);
        }
        runs.pop()
            .unwrap_or_else(|| panic!("no numeric token in {token:?} after {field:?}"))
    };
    for (field, expected) in [
        ("stamp_min=", "200"),
        ("stamp_max=", "500"),
        ("stamp_null_count=", "1"),
        ("cstat_min=", "-1"),
        ("cstat_max=", "499"),
        ("cstat_null_count=", "2"),
    ] {
        assert_eq!(
            value_of(field),
            expected,
            "field {field:?} must bind exactly to {expected:?}; logged:\n{logged}"
        );
    }
}
