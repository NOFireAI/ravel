//! The heap bytes a cached string column's values memo costs beyond the
//! column's own buffers, which `VALUES_MEMO_BYTES` charges per memo-capable
//! entry (ADR-3326, zero-copy values amendment; issue #3326).
//!
//! Each case decodes one block through a scan with a decoded-column cache
//! attached, so the column is admitted and its memo slot is empty, then fills
//! the slot once under a `stats_alloc` `Region`. The fill is the build
//! `memo_array` runs in `src/logs_scan/batch.rs`: `build_str_memo` over the
//! column's shared buffers, each wrapped as a `MemoOwner` in `Bytes` and an
//! Arrow `Buffer`, the `StringArray` over them, and the `Arc<StrMemo>` the slot
//! holds as `Arc<dyn ValuesMemo>`. Those functions are private to the crate, so
//! [`build_str_memo`] below is a copy of them, allocation for allocation; a
//! change to the memo's layout there changes the copy here in the same commit.
//!
//! The cases are a plain `Text` column and a block-local dictionary of each
//! region kind that gets a memo: `Text` (every entry UTF-8, no validity
//! buffer) and `Mixed` (one entry not UTF-8, so the array carries a null
//! buffer over the dictionary's validity words).
//!
//! This file contains EXACTLY ONE test on purpose: the instrumented global
//! allocator counts every allocation in the process, so a second test running
//! concurrently would pollute the count. The harness's own thread can still
//! allocate while a region is open, so each case is measured in [`WINDOWS`]
//! windows, each over a freshly admitted column, and the window with the
//! fewest allocations is the figure.
//!
//! Run by name with `--nocapture` to see the figures.

#![allow(clippy::expect_used)]

use std::alloc::System;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::ToByteSlice;
use ravel_logseg::block::{ColumnPlan, write_block};
use ravel_logseg::decoded_cache::{
    DecodedCacheAttach, DecodedCacheCounters, DecodedColumnCache, ReservationCell, StrBuffers,
    VALUES_MEMO_BYTES, ValuesMemo,
};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{
    COMP_NONE, LogFooter, SectionDesc, kind, open as open_footer, write_footer_and_trailer,
};
use ravel_logseg::reader::{RlogReader, read_section};
use ravel_logseg::record::{ColumnValue, ResolvedRow};
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::writer::{BlocksBuilder, ObjectIdentity};
use ravel_logseg::{
    AttrValue, ColumnSelection, FieldType, LogRecord, Predicate, RlogConfig, RlogWriter, Validity,
    stream_attrs_bytes,
};
use ravel_types::accounting::AccessIntent;
use ravel_types::logstream::log_stream_id;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const KEY: &str = "k";
/// Rows in the one block each case writes.
const ROWS: usize = 4096;
/// Separately counted windows per case, each over a fresh admission.
const WINDOWS: usize = 5;

// --- the memo build, as `src/logs_scan/batch.rs` runs it ---------------------

struct StrMemo {
    array: Option<StringArray>,
}

impl ValuesMemo for StrMemo {
    fn held_outside(&self) -> bool {
        let Some(array) = &self.array else {
            return false;
        };
        array.values().strong_count() > 1
            || array.offsets().inner().inner().strong_count() > 1
            || array.nulls().is_some_and(|n| n.buffer().strong_count() > 1)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct MemoOwner<T> {
    buf: Arc<T>,
    _cell: Arc<ReservationCell>,
}

impl AsRef<[u8]> for MemoOwner<String> {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_bytes()
    }
}

impl AsRef<[u8]> for MemoOwner<Vec<u32>> {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_slice().to_byte_slice()
    }
}

impl AsRef<[u8]> for MemoOwner<Vec<i32>> {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_slice().to_byte_slice()
    }
}

impl AsRef<[u8]> for MemoOwner<Validity> {
    fn as_ref(&self) -> &[u8] {
        self.buf.words().to_byte_slice()
    }
}

fn memo_buffer<T>(buf: Arc<T>, cell: &Arc<ReservationCell>) -> Buffer
where
    MemoOwner<T>: AsRef<[u8]>,
    T: Send + Sync + 'static,
{
    Buffer::from(bytes::Bytes::from_owner(MemoOwner {
        buf,
        _cell: Arc::clone(cell),
    }))
}

fn memo_offsets<T: Copy + Into<i64>>(
    offsets: &[T],
    values_len: usize,
    buffer: Buffer,
) -> Option<OffsetBuffer<i32>> {
    let first: i64 = (*offsets.first()?).into();
    let last: i64 = (*offsets.last()?).into();
    let ordered = offsets.windows(2).all(|w| w[0].into() <= w[1].into());
    if first != 0
        || !ordered
        || last > i64::from(i32::MAX)
        || usize::try_from(last).ok()? > values_len
    {
        return None;
    }
    Some(OffsetBuffer::new(ScalarBuffer::new(
        buffer,
        0,
        offsets.len(),
    )))
}

fn build_str_memo(buffers: StrBuffers, cell: Arc<ReservationCell>) -> Option<StringArray> {
    let (values, offsets, valid) = match buffers {
        StrBuffers::Plain { values, offsets } => {
            let len = values.len();
            let o = memo_offsets(&offsets, len, memo_buffer(Arc::clone(&offsets), &cell))?;
            (values, o, None)
        }
        StrBuffers::Dict {
            values,
            offsets,
            valid,
        } => {
            let len = values.len();
            let o = memo_offsets(&offsets, len, memo_buffer(Arc::clone(&offsets), &cell))?;
            (values, o, valid)
        }
    };
    let entries = offsets.len() - 1;
    let nulls = match valid {
        Some(valid) if valid.len() == entries => Some(NullBuffer::new(BooleanBuffer::new(
            memo_buffer(valid, &cell),
            0,
            entries,
        ))),
        Some(_) => return None,
        None => None,
    };
    StringArray::try_new(offsets, memo_buffer(values, &cell), nulls).ok()
}

// --- fixtures ----------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Case {
    /// A distinct UTF-8 value per row: a plain page.
    PlainText,
    /// [`ENTRIES`] UTF-8 values: a block-local dictionary, `Text` region.
    DictText,
    /// The same with one entry that is not UTF-8: a `Mixed` region.
    DictMixed,
}

/// Distinct entries of the dictionary cases.
const ENTRIES: usize = 16;

impl Case {
    fn cell(self, i: usize) -> Vec<u8> {
        // Rows pick entries in a scrambled order, so the plain candidate does
        // not compress below the dictionary page the writer should choose.
        let e = (i.wrapping_mul(0x9e37_79b9) >> 7) & (ENTRIES - 1);
        let entry = || format!("dictionary-entry-number-{e:02}").into_bytes();
        match self {
            Case::PlainText => format!("value-{i:06}").into_bytes(),
            Case::DictText => entry(),
            Case::DictMixed if e == ENTRIES - 1 => {
                let mut b = entry();
                b[0] = 0xff;
                b
            }
            Case::DictMixed => entry(),
        }
    }
}

fn cfg() -> RlogConfig {
    RlogConfig {
        block_target_records: ROWS,
        group_target_blocks: 1,
        ..RlogConfig::default()
    }
}

/// One single-block object whose `Str` column `KEY` holds `case`'s cells. The
/// writer lays out every section from UTF-8 seed records; the block is then
/// rewritten through `write_block` with the case's cells verbatim, since the
/// writer refuses a `Str` cell that is not UTF-8.
fn object(case: Case) -> (Vec<u8>, u32) {
    let cfg = cfg();
    let resource = vec![("svc".to_string(), AttrValue::Str("s".into()))];
    let records: Vec<LogRecord> = (0..ROWS)
        .map(|i| LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1", &[]),
            ts_ns: i as i64,
            observed_ts_ns: i as i64,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "b".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![(KEY.to_string(), AttrValue::Str("seed".into()))],
        })
        .collect();
    let mut w = RlogWriter::new(
        cfg,
        ObjectIdentity {
            tenant_hash: [0; 16],
            shard: 0,
            writer_id: [0; 16],
            writer_epoch: 0,
            writer_seq: 0,
        },
    );
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let obj = w.finish().expect("finish");

    let footer = open_footer(&obj).expect("open the writer's footer");
    let desc = *footer.section(kind::FIELD_DIR).expect("FIELD_DIR");
    let dir = FieldDir::decode(&read_section(&obj, &desc, &cfg).expect("read"), u64::MAX)
        .expect("decode FIELD_DIR");
    let column_id = dir
        .column(KEY, FieldType::Str)
        .expect("a Str column")
        .column_id;

    let rows: Vec<ResolvedRow> = records
        .iter()
        .enumerate()
        .map(|(i, r)| ResolvedRow {
            stream_ref: 0,
            ts_ns: r.ts_ns,
            observed_ts_ns: r.observed_ts_ns,
            severity_num: r.severity_num,
            severity_text: r.severity_text.clone(),
            body: r.body.clone(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs_raw: None,
            columns: vec![(column_id, ColumnValue::Str(case.cell(i)))],
            indexed_terms: Vec::new(),
            stat_winners: Vec::new(),
        })
        .collect();
    let plans = vec![ColumnPlan {
        column_id,
        ty: FieldType::Str,
    }];
    let block = write_block(&rows, &plans, cfg.zstd_level).expect("write one block");
    let mut layout = BlocksBuilder::version_4(cfg.group_target_blocks);
    layout.push(block);
    let (blocks_bytes, l0, page_dir) = layout.finish();
    let skip = SkipIndex::build(l0).encode();
    let page_dir_bytes = page_dir.encode();

    let mut out: Vec<u8> = Vec::new();
    let mut sections: Vec<SectionDesc> = Vec::new();
    for d in &footer.sections {
        let (bytes, comp, uncomp_len) = match d.kind {
            kind::BLOCKS => (blocks_bytes.clone(), COMP_NONE, blocks_bytes.len() as u64),
            kind::SKIP_IDX => (skip.clone(), COMP_NONE, skip.len() as u64),
            kind::PAGE_DIR => (
                page_dir_bytes.clone(),
                COMP_NONE,
                page_dir_bytes.len() as u64,
            ),
            _ => {
                let start = usize::try_from(d.offset).expect("offset fits");
                let end = start + usize::try_from(d.len).expect("len fits");
                (obj[start..end].to_vec(), d.comp, d.uncomp_len)
            }
        };
        sections.push(SectionDesc {
            kind: d.kind,
            offset: out.len() as u64,
            len: bytes.len() as u64,
            crc32c: crc32c::crc32c(&bytes),
            comp,
            uncomp_len,
            prefix_len: d.prefix_len,
        });
        out.extend_from_slice(&bytes);
    }
    let patched = LogFooter { sections, ..footer };
    write_footer_and_trailer(&mut out, &patched);
    (out, column_id)
}

fn every_row() -> Predicate {
    Predicate::TsRange {
        min_ns: i64::MIN,
        max_ns: i64::MAX,
    }
}

fn attach(cache: &Arc<DecodedColumnCache>) -> DecodedCacheAttach {
    DecodedCacheAttach {
        cache: Arc::clone(cache),
        tenant: [7; 16],
        object: [9; 32],
        intent: AccessIntent::Interactive,
        counters: Arc::new(DecodedCacheCounters::default()),
        serve_deferred: false,
    }
}

/// The bytes a fresh cache holds after one scan of `obj` under `columns`.
fn resident_after_scan(obj: &[u8], columns: &ColumnSelection) -> u64 {
    let cache = Arc::new(DecodedColumnCache::new(1 << 30, None));
    let reader = RlogReader::new(obj, &cfg()).expect("open");
    let mut scan = reader
        .scan_blocks(&every_row(), &[], columns)
        .expect("scan");
    scan.attach_decoded_cache(attach(&cache));
    scan.next_block_columnar(obj)
        .expect("columnar exit")
        .expect("one block");
    cache.resident_bytes()
}

// --- the measurement ---------------------------------------------------------

/// One case's quietest window, and the memo's array for shape checks.
struct Measured {
    stats: Stats,
    per_window: Vec<usize>,
    array_len: usize,
    array_nulls: usize,
}

/// Bytes still held when the region closed: allocated, plus net growth by
/// reallocation, less deallocated.
fn live(stats: &Stats) -> i64 {
    stats.bytes_allocated as i64 + stats.bytes_reallocated as i64 - stats.bytes_deallocated as i64
}

/// Fills `case`'s memo slot once per window, each over a column freshly
/// admitted to its own cache, with only the fill inside the region.
fn measure(case: Case, obj: &[u8], column_id: u32, columns: &ColumnSelection) -> Measured {
    let mut counts = Vec::with_capacity(WINDOWS);
    let mut shape = None;
    for _ in 0..WINDOWS {
        let cache = Arc::new(DecodedColumnCache::new(1 << 30, None));
        let reader = RlogReader::new(obj, &cfg()).expect("open");
        let mut scan = reader
            .scan_blocks(&every_row(), &[], columns)
            .expect("scan");
        scan.attach_decoded_cache(attach(&cache));
        let view = scan
            .next_block_columnar(obj)
            .expect("columnar exit")
            .expect("one block");
        let is_dict = view.str_dict(column_id).is_some();
        assert_eq!(
            is_dict,
            !matches!(case, Case::PlainText),
            "{case:?}: page kind"
        );
        let source = view
            .str_values_memo(column_id)
            .expect("an admitted entry holds a memo-capable column");
        assert!(source.get().is_none(), "{case:?}: the slot starts empty");

        let region = Region::new(GLOBAL);
        let memo = source.get_or_build(|buffers, cell| {
            Arc::new(StrMemo {
                array: build_str_memo(buffers, cell),
            })
        });
        counts.push(region.change());

        let array = memo
            .as_any()
            .downcast_ref::<StrMemo>()
            .and_then(|m| m.array.as_ref())
            .expect("the buffers form a valid array");
        shape = Some((array.len(), array.null_count()));
    }
    let (array_len, array_nulls) = shape.expect("at least one window");
    let stats = counts
        .iter()
        .copied()
        .min_by_key(|s| s.allocations + s.reallocations)
        .expect("at least one window");
    Measured {
        stats,
        per_window: counts.iter().map(|s| s.allocations).collect(),
        array_len,
        array_nulls,
    }
}

#[test]
fn values_memo_charge_covers_the_measured_memo_and_no_more() {
    let mut largest = 0i64;
    for case in [Case::PlainText, Case::DictText, Case::DictMixed] {
        let (obj, column_id) = object(case);
        let fixed = ColumnSelection::fixed_only();
        let with_key = ColumnSelection::fixed_only().with_attr(KEY);
        // The column's own charge: what admitting it adds to the cache, less
        // the memo constant `offer` adds on top.
        let heap_bytes = (resident_after_scan(&obj, &with_key) - resident_after_scan(&obj, &fixed))
            .checked_sub(VALUES_MEMO_BYTES)
            .expect("the entry carries the memo charge");

        let m = measure(case, &obj, column_id, &with_key);
        let live = live(&m.stats);
        eprintln!(
            "memo_overhead_alloc_pin: {case:?}: live {live} bytes, total {} bytes in {} \
             allocations (allocations per window {:?}); column heap_bytes {heap_bytes}",
            m.stats.bytes_allocated, m.stats.allocations, m.per_window,
        );

        let (want_len, want_nulls) = match case {
            Case::PlainText => (ROWS, 0),
            Case::DictText => (ENTRIES, 0),
            Case::DictMixed => (ENTRIES, 1),
        };
        assert_eq!(m.array_len, want_len, "{case:?}: array length");
        assert_eq!(
            m.array_nulls, want_nulls,
            "{case:?}: one NULL per non-UTF-8 entry"
        );
        assert_eq!(
            m.stats.reallocations, 0,
            "{case:?}: the fill reallocates nothing"
        );
        assert!(live > 0, "{case:?}: the memo holds heap bytes");
        // The memo shares the column's buffers: a figure that counted them
        // would be the size of the column.
        assert!(
            (live as u64) * 16 < heap_bytes,
            "{case:?}: live {live} is not far below the column's {heap_bytes} heap bytes, \
             so it counts the column's own buffers"
        );
        largest = largest.max(live);

        assert!(
            VALUES_MEMO_BYTES >= live as u64,
            "{case:?}: VALUES_MEMO_BYTES {VALUES_MEMO_BYTES} is below the measured {live}"
        );
    }
    assert!(
        VALUES_MEMO_BYTES <= largest as u64 + 16,
        "VALUES_MEMO_BYTES {VALUES_MEMO_BYTES} exceeds the largest measured memo, \
         {largest} bytes, by more than 16"
    );
}
