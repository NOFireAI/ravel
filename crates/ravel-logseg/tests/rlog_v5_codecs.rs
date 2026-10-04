//! RLOG v5 page codecs and encoding choice (ADR-2135 decisions 3 and 4,
//! docs/log-segment-format.md "Encodings (tag registry)" and "Integer codec
//! layouts"): tag 10 (GCD i64), tag 11 (the `observed_ts` reference to `ts`),
//! and the writer's choice of each page's encoding by its stored size.
//!
//! Round trips go through `RlogWriter` and `RlogReader`, the entry points
//! ingest and the SQL scan use. The hand-built corrupt pages enter at
//! `block::read_block_pages`, the function the reader hands every block's
//! checksum-verified pages to: building a whole object around one mutated page
//! would exercise the page and block checksums, not the codec.
#![allow(clippy::expect_used)]

use bytes::Bytes;
use proptest::prelude::*;
use ravel_logseg::block::{ColumnPlan, DecodedBlock, PageCounters, read_block_pages};
use ravel_logseg::encoding::{Enc, encode_bitmap};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{kind, open};
use ravel_logseg::page::{COMP_NONE, COMP_ZSTD, PageDesc, SealedPage, smallest_stored};
use ravel_logseg::page_dir::{PageDir, PageEntry};
use ravel_logseg::record::{COL_OBSERVED_TS, COL_STREAM_REF, COL_TS};
use ravel_logseg::rlog_codec::{
    decode_gcd_i64, encode_column_ref, encode_gcd_i64, i64_candidates, string_candidates,
};
use ravel_logseg::varint::{put_ivarint, put_uvarint};
use ravel_logseg::{
    AttrValue, ColumnSelection, ColumnarLogBatch, FieldType, LogRecord, LogSegError, LogStreamId,
    ObjectIdentity, Predicate, RlogConfig, RlogReader, RlogWriter, SparseObject, read_section,
    stream_attrs_bytes,
};

const ZSTD_LEVEL: i32 = 3;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0x7C; 16],
        shard: 3,
        writer_id: [0xC7; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn record(ts: i64, observed_ts: i64, attrs: Vec<(String, AttrValue)>) -> LogRecord {
    LogRecord {
        stream_id: LogStreamId([0x44; 16]),
        stream_attrs: stream_attrs_bytes(
            &[("service.name".to_string(), AttrValue::Str("svc".into()))],
            "scope",
            "1.0",
            &[],
        ),
        ts_ns: ts,
        observed_ts_ns: observed_ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "ok".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn write_rows(cfg: &RlogConfig, records: &[LogRecord]) -> Vec<u8> {
    let mut w = RlogWriter::new(*cfg, identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

/// The reference row builder ([`RlogWriter::finish_row_reference`]), not
/// `push` + `finish`: `finish` now routes row input through the columnar
/// builder too (ADR-2467 decision 1), so a test that compares row-pushed
/// output against [`write_columnar`]'s output needs this to actually reach
/// `build_object` rather than compare the columnar builder with itself.
fn write_rows_reference(cfg: &RlogConfig, records: &[LogRecord]) -> Vec<u8> {
    let mut w = RlogWriter::new(*cfg, identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish_row_reference().expect("finish")
}

fn write_columnar(cfg: &RlogConfig, batch: ColumnarLogBatch) -> Vec<u8> {
    let mut w = RlogWriter::new(*cfg, identity());
    w.push_columnar(batch).expect("push columnar");
    w.finish().expect("finish")
}

fn page_dir(object: &[u8]) -> PageDir {
    let cfg = RlogConfig::default();
    let ftr = open(object).expect("open");
    let raw = read_section(object, ftr.section(kind::PAGE_DIR).expect("PAGE_DIR"), &cfg)
        .expect("read PAGE_DIR");
    PageDir::decode(&raw).expect("decode PAGE_DIR")
}

/// Every value page `column_id` has, in block order. A presence bitmap page
/// is skipped: it precedes the value page of the same block.
fn value_pages(object: &[u8], column_id: u32) -> Vec<PageEntry> {
    let mut out = Vec::new();
    for g in page_dir(object).groups {
        for c in g.chunks.iter().filter(|c| c.column_id == column_id) {
            for (i, p) in c.pages.iter().enumerate() {
                let next_same_block = c.pages.get(i + 1).is_some_and(|q| q.block == p.block);
                if !next_same_block {
                    out.push(*p);
                }
            }
        }
    }
    out
}

/// The single value page of `column_id` in a one-block object.
fn only_value_page(object: &[u8], column_id: u32) -> PageEntry {
    let pages = value_pages(object, column_id);
    assert_eq!(pages.len(), 1, "one block, one value page for {column_id}");
    pages[0]
}

fn dyn_column(object: &[u8], name: &str, ty: FieldType) -> u32 {
    let cfg = RlogConfig::default();
    let ftr = open(object).expect("open");
    let raw = read_section(
        object,
        ftr.section(kind::FIELD_DIR).expect("FIELD_DIR"),
        &cfg,
    )
    .expect("read FIELD_DIR");
    FieldDir::decode(&raw, u64::MAX)
        .expect("decode FIELD_DIR")
        .column(name, ty)
        .expect("column")
        .column_id
}

fn scan_all(object: &[u8]) -> Vec<LogRecord> {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(object, &cfg).expect("reader");
    reader.scan(&Predicate::And(vec![])).expect("scan").0
}

/// `n` timestamps in whole seconds, stored in nanoseconds: gaps of 0 to 200
/// seconds from a fixed linear congruential sequence, so the page is
/// deterministic and has no regular stride for delta coding to collapse.
fn whole_second_ts(n: usize) -> Vec<i64> {
    let mut s = 1_700_000_000i64;
    let mut x = 12_345u64;
    (0..n)
        .map(|_| {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            s += ((x >> 33) % 201) as i64;
            s * 1_000_000_000
        })
        .collect()
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

fn stored(candidates: Vec<(Enc, Vec<u8>)>) -> SealedPage {
    smallest_stored(candidates, ZSTD_LEVEL).expect("at least one candidate")
}

fn is_corrupted<T>(r: &Result<T, LogSegError>) -> bool {
    matches!(r, Err(LogSegError::Corrupted(_)))
}

// --- hand-built pages through the reader's block decode ---------------------

fn plain_i64(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for &v in values {
        put_ivarint(&mut out, v);
    }
    out
}

/// One page as the reader hands it to `read_block_pages`: raw (never
/// compressed), so the descriptor's lengths are the payload's own length.
fn raw_page(column_id: u32, enc: Enc, bytes: Vec<u8>) -> (PageDesc, Vec<u8>) {
    let len = bytes.len() as u64;
    (
        PageDesc {
            column_id,
            enc,
            comp: COMP_NONE,
            len,
            uncomp_len: len,
        },
        bytes,
    )
}

/// A named hand-built block: its pages and the plans its dynamic columns need.
type Case<'a> = (&'a str, Vec<(PageDesc, Vec<u8>)>, &'a [ColumnPlan]);

fn decode_pages(
    record_count: usize,
    pages: Vec<(PageDesc, Vec<u8>)>,
    plans: &[ColumnPlan],
) -> Result<DecodedBlock, LogSegError> {
    let descs: Vec<PageDesc> = pages.iter().map(|(d, _)| *d).collect();
    let bytes: Vec<Option<Vec<u8>>> = pages.into_iter().map(|(_, b)| Some(b)).collect();
    read_block_pages(record_count, &descs, &bytes, plans, PageCounters::default())
}

// --- acceptance tests -------------------------------------------------------

/// A ts page of whole seconds in nanoseconds is stored as tag 10 and decodes
/// exactly, and the GCD candidate saves at least half a byte per row over the
/// best page the other six candidates can store. A second page shifts every
/// value by 123 ns, so the values share no divisor but their offsets from the
/// page minimum still do: it must still be tag 10.
///
/// Wrong implementations this rules out, each shown failing: no GCD candidate
/// (the tag assertion fails); a GCD taken over the values rather than over the
/// offsets from the minimum, as RSEG's TS_GCD_I64 does (the shifted page is not
/// tag 10).
#[test]
fn gcd_page_round_trips_second_resolution_ts() {
    let n = 2_000usize;
    let cfg = RlogConfig::default();
    // (shift, pinned stored bytes of the best page without the GCD candidate).
    for (shift, without_len) in [(0i64, 4_031usize), (123, 4_032)] {
        let ts: Vec<i64> = whole_second_ts(n).iter().map(|t| t + shift).collect();
        // observed_ts differs from ts in most rows, so it is an ordinary page.
        let records: Vec<LogRecord> = ts
            .iter()
            .enumerate()
            .map(|(i, &t)| record(t, t + (i % 7) as i64, Vec::new()))
            .collect();
        let object = write_rows(&cfg, &records);

        let page = only_value_page(&object, COL_TS);
        assert_eq!(page.enc, Enc::GcdI64, "shift {shift}");

        let with = stored(i64_candidates(&ts));
        let without = stored(
            i64_candidates(&ts)
                .into_iter()
                .filter(|(e, _)| *e != Enc::GcdI64)
                .collect(),
        );
        assert_eq!(page.len, with.stored.len() as u64, "shift {shift}");
        assert_eq!(page.comp, COMP_ZSTD);
        assert_eq!(without.enc, Enc::DeltaZigzag);
        // Pinned for zstd level 3 against the locked zstd build.
        assert_eq!(
            (page.len, without.stored.len()),
            (2_488, without_len),
            "shift {shift}"
        );
        // The saving, stated per row: at least n / 2 bytes.
        assert!(
            without.stored.len() as u64 - page.len >= (n / 2) as u64,
            "shift {shift}: with {} without {}",
            page.len,
            without.stored.len()
        );

        let got: Vec<i64> = scan_all(&object).iter().map(|r| r.ts_ns).collect();
        assert_eq!(got, ts, "shift {shift}");
    }
    // The shifted page's values share no divisor of 2 or more.
    let shifted_gcd = whole_second_ts(n)
        .iter()
        .fold(0u64, |g, &t| gcd(g, (t + 123).unsigned_abs()));
    assert_eq!(shifted_gcd, 1);
}

/// A hand-built tag-10 page whose quotient times gcd overflows u64 is
/// `Corrupted`; one whose product fits u64 but not i64 decodes, by the wrapping
/// add. Through the writer, a page spanning `i64::MIN` and a large positive
/// value is stored as tag 10 and round-trips, because the offsets from the
/// minimum are taken in wrapping u64.
///
/// Wrong implementations this rules out, each shown failing: a wrapping
/// multiply (the overflowing page decodes to a value instead of failing); a
/// checked i64 multiply (the u64-only product and the `i64::MIN` page both fail
/// to decode).
#[test]
fn gcd_refuses_full_range_overflow_on_decode() {
    let tag10 = |g: u64, base: i64, quotient: i64| {
        let mut b = Vec::new();
        put_uvarint(&mut b, g);
        put_ivarint(&mut b, base);
        b.push(Enc::Plain.to_u8());
        put_ivarint(&mut b, quotient);
        b
    };
    // 4 * 2^62 = 2^64.
    let overflow = decode_pages(
        1,
        vec![raw_page(COL_TS, Enc::GcdI64, tag10(1 << 62, 0, 4))],
        &[],
    );
    match overflow {
        Err(LogSegError::Corrupted(m)) => assert!(m.contains("overflows u64"), "{m}"),
        other => panic!("expected Corrupted, got {:?}", other.map(|_| ())),
    }
    // 3 * 2^62 fits u64; added to base i64::MIN it wraps to 2^62.
    let fits = decode_pages(
        1,
        vec![raw_page(COL_TS, Enc::GcdI64, tag10(1 << 62, i64::MIN, 3))],
        &[],
    )
    .expect("a u64 product decodes");
    assert_eq!(fits.i64_col(COL_TS).expect("ts"), &[Some(1i64 << 62)]);

    // Through the writer: offsets 0, G, 2G and 3G from i64::MIN, where 3G is
    // above i64::MAX and below u64::MAX.
    let g: u64 = 6_000_000_000_000_000_000;
    let pattern = [0u64, 3, 1, 2, 3, 0, 2, 1];
    let values: Vec<i64> = (0..64)
        .map(|i| (i64::MIN as u64).wrapping_add(pattern[i % 8] * g) as i64)
        .collect();
    assert_eq!(
        *values.iter().max().expect("max"),
        8_776_627_963_145_224_192
    );
    let records: Vec<LogRecord> = values
        .iter()
        .enumerate()
        .map(|(i, &v)| {
            let t = 1_000 + i as i64;
            record(t, t, vec![("v".to_string(), AttrValue::I64(v))])
        })
        .collect();
    let object = write_rows(&RlogConfig::default(), &records);
    let cid = dyn_column(&object, "v", FieldType::I64);
    assert_eq!(only_value_page(&object, cid).enc, Enc::GcdI64);
    let got: Vec<i64> = scan_all(&object)
        .iter()
        .map(|r| match r.attrs.iter().find(|(k, _)| k == "v") {
            Some((_, AttrValue::I64(v))) => *v,
            other => panic!("v missing: {other:?}"),
        })
        .collect();
    assert_eq!(got, values);
}

/// An `observed_ts` equal to `ts` row for row is stored as a tag-11 page of
/// under 8 bytes and decodes to `ts`, on the row path and the columnar view.
/// An `observed_ts` that differs from `ts` in one middle row is not a
/// reference and still decodes exactly.
///
/// Wrong implementations this rules out, each shown failing: equality judged
/// on the first, last, minimum and maximum values only (the one-row variant
/// becomes a reference and decodes wrong); no reference at all (the tag
/// assertion fails).
#[test]
fn observed_ts_equal_to_ts_is_stored_as_reference() {
    let n = 300usize;
    let ts = whole_second_ts(n);
    let cfg = RlogConfig::default();

    let equal: Vec<LogRecord> = ts.iter().map(|&t| record(t, t, Vec::new())).collect();
    let object = write_rows(&cfg, &equal);
    let page = only_value_page(&object, COL_OBSERVED_TS);
    assert_eq!(page.enc, Enc::ColumnRef);
    assert_eq!((page.comp, page.len), (COMP_NONE, 1));
    assert!(page.len < 8);
    let rows = scan_all(&object);
    assert_eq!(rows.iter().map(|r| r.ts_ns).collect::<Vec<_>>(), ts);
    assert_eq!(
        rows.iter().map(|r| r.observed_ts_ns).collect::<Vec<_>>(),
        ts
    );

    let reader = RlogReader::new(&object, &cfg).expect("reader");
    let mut scan = reader
        .scan_blocks(
            &Predicate::And(vec![]),
            &[],
            &ColumnSelection::fixed_only().with_observed_ts(),
        )
        .expect("scan");
    let view = scan
        .next_block_columnar(&object)
        .expect("block")
        .expect("one");
    assert_eq!(view.surviving_count(), n);
    for (i, &t) in ts.iter().enumerate() {
        assert_eq!(
            (view.ts(i), view.observed_ts(i)),
            (Some(t), Some(t)),
            "row {i}"
        );
    }

    let mut differ = equal.clone();
    differ[n / 2].observed_ts_ns += 1;
    let obs: Vec<i64> = differ.iter().map(|r| r.observed_ts_ns).collect();
    // The differing row moves none of the first, last, minimum or maximum.
    assert_eq!(
        (obs[0], obs[n - 1], obs.iter().min(), obs.iter().max()),
        (ts[0], ts[n - 1], ts.iter().min(), ts.iter().max())
    );
    let object = write_rows(&cfg, &differ);
    let page = only_value_page(&object, COL_OBSERVED_TS);
    assert_ne!(page.enc, Enc::ColumnRef);
    let rows = scan_all(&object);
    assert_eq!(
        rows.iter().map(|r| r.observed_ts_ns).collect::<Vec<_>>(),
        obs
    );
}

/// A projection naming `observed_ts` through a ranged fetch: only the
/// directories and the `ts`, `observed_ts` and `stream_ref` column chunks are
/// placed, and the reference decodes from the `ts` values the projection always
/// carries. With the `ts` chunk left unplaced the read is `Unplaced`, never a
/// wrong value.
#[test]
fn projected_observed_ts_reads_ts_through_a_ranged_fetch() {
    let ts = whole_second_ts(500);
    let records: Vec<LogRecord> = ts.iter().map(|&t| record(t, t, Vec::new())).collect();
    let cfg = RlogConfig {
        block_target_records: 128,
        ..RlogConfig::default()
    };
    let object = write_rows(&cfg, &records);
    assert_eq!(value_pages(&object, COL_OBSERVED_TS).len(), 4);
    assert!(
        value_pages(&object, COL_OBSERVED_TS)
            .iter()
            .all(|p| p.enc == Enc::ColumnRef)
    );

    let place = |columns: &[u32]| {
        let footer = open(&object).expect("footer");
        let mut sparse = SparseObject::new(object.len() as u64);
        let region = |s: u64, l: u64| Bytes::copy_from_slice(&object[s as usize..(s + l) as usize]);
        let mut tail = 0u64;
        for s in &footer.sections {
            tail = tail.max(s.offset + s.len);
            if s.kind != kind::BLOCKS {
                sparse
                    .place(s.offset, region(s.offset, s.len))
                    .expect("place");
            }
        }
        sparse
            .place(tail, region(tail, object.len() as u64 - tail))
            .expect("place");
        let reader = RlogReader::new(&object, &RlogConfig::default()).expect("reader");
        for &c in columns {
            let (start, len) = reader.column_chunk_range(0, c).expect("chunk");
            sparse.place(start, region(start, len)).expect("place");
        }
        sparse
    };
    let read = |sparse: &SparseObject| -> Result<Vec<(i64, i64)>, LogSegError> {
        let reader = RlogReader::from_source(sparse, &RlogConfig::default())?;
        let mut scan = reader.scan_blocks(
            &Predicate::And(vec![]),
            &[],
            &ColumnSelection::fixed_only().with_observed_ts(),
        )?;
        let mut out = Vec::new();
        while let Some(view) = scan.next_block_columnar(sparse)? {
            for i in 0..view.surviving_count() {
                out.push((
                    view.ts(i).unwrap_or(i64::MIN),
                    view.observed_ts(i).unwrap_or(i64::MIN),
                ));
            }
        }
        Ok(out)
    };

    let sparse = place(&[COL_TS, COL_OBSERVED_TS, COL_STREAM_REF]);
    let got = read(&sparse).expect("projected ranged read");
    assert_eq!(got, ts.iter().map(|&t| (t, t)).collect::<Vec<_>>());

    let without_ts = place(&[COL_OBSERVED_TS, COL_STREAM_REF]);
    assert!(matches!(
        read(&without_ts),
        Err(LogSegError::Unplaced { .. })
    ));
}

/// Tag 11 is refused as `Corrupted` on any column but `observed_ts`, and on
/// `observed_ts` naming any column but `ts`; the permitted pair decodes to a
/// copy of `ts`. A reference whose target was not decoded, or whose presence
/// differs from the target's, is `Corrupted` as well.
///
/// Wrong implementations this rules out, each shown failing: a reader that
/// copies whatever column the reference names (the stream_ref target decodes);
/// a reader that accepts tag 11 on any column (the stream_ref and dynamic
/// column cases decode); a string decoder that takes tag 11 and an f64 decoder
/// that takes tag 10 (the typed refusals are not reached).
#[test]
fn column_ref_refuses_other_targets() {
    let ts = [10i64, 20, 30];
    let ts_page = || raw_page(COL_TS, Enc::Plain, plain_i64(&ts));
    let sref_page = || raw_page(COL_STREAM_REF, Enc::Plain, plain_i64(&[0, 0, 0]));
    let plan = [ColumnPlan {
        column_id: 10,
        ty: FieldType::I64,
    }];

    let ok = decode_pages(
        3,
        vec![
            ts_page(),
            raw_page(COL_OBSERVED_TS, Enc::ColumnRef, encode_column_ref(COL_TS)),
        ],
        &[],
    )
    .expect("observed_ts referring to ts decodes");
    assert_eq!(
        ok.i64_col(COL_OBSERVED_TS).expect("observed_ts"),
        &[Some(10), Some(20), Some(30)]
    );

    let cases: Vec<Case<'_>> = vec![
        (
            // stream_ref is placed ahead of observed_ts so it is already
            // decoded: only the target rule can refuse this page.
            "observed_ts naming stream_ref",
            vec![
                ts_page(),
                sref_page(),
                raw_page(
                    COL_OBSERVED_TS,
                    Enc::ColumnRef,
                    encode_column_ref(COL_STREAM_REF),
                ),
            ],
            &[],
        ),
        (
            "observed_ts naming itself",
            vec![
                ts_page(),
                raw_page(
                    COL_OBSERVED_TS,
                    Enc::ColumnRef,
                    encode_column_ref(COL_OBSERVED_TS),
                ),
            ],
            &[],
        ),
        (
            "tag 11 on stream_ref",
            vec![
                ts_page(),
                raw_page(COL_STREAM_REF, Enc::ColumnRef, encode_column_ref(COL_TS)),
            ],
            &[],
        ),
        (
            "tag 11 on a dynamic i64 column",
            vec![
                ts_page(),
                raw_page(10, Enc::ColumnRef, encode_column_ref(COL_TS)),
            ],
            &plan,
        ),
        (
            "tag 11 on ts itself",
            vec![raw_page(COL_TS, Enc::ColumnRef, encode_column_ref(COL_TS))],
            &[],
        ),
        (
            "target not decoded",
            vec![raw_page(
                COL_OBSERVED_TS,
                Enc::ColumnRef,
                encode_column_ref(COL_TS),
            )],
            &[],
        ),
        (
            "presence differs from the target",
            vec![
                ts_page(),
                raw_page(
                    COL_OBSERVED_TS,
                    Enc::Bitmap,
                    encode_bitmap(&[true, false, true]),
                ),
                raw_page(COL_OBSERVED_TS, Enc::ColumnRef, encode_column_ref(COL_TS)),
            ],
            &[],
        ),
        (
            "trailing byte",
            vec![
                ts_page(),
                raw_page(COL_OBSERVED_TS, Enc::ColumnRef, vec![0, 0]),
            ],
            &[],
        ),
    ];
    for (name, pages, plans) in cases {
        let got = decode_pages(3, pages, plans);
        assert!(is_corrupted(&got), "{name}: {:?}", got.map(|_| ()));
    }

    // Each integer codec on a column of another type: tag 11 on a string
    // column, tag 10 on an f64 column.
    let str_plan = [ColumnPlan {
        column_id: 11,
        ty: FieldType::Str,
    }];
    let f64_plan = [ColumnPlan {
        column_id: 12,
        ty: FieldType::F64,
    }];
    type TypedCase<'a> = (&'a str, Vec<(PageDesc, Vec<u8>)>, &'a [ColumnPlan], &'a str);
    let typed: Vec<TypedCase> = vec![
        (
            "tag 11 on a dynamic str column",
            vec![
                ts_page(),
                raw_page(11, Enc::ColumnRef, encode_column_ref(COL_TS)),
            ],
            &str_plan,
            "enc ColumnRef is not a string codec",
        ),
        (
            "tag 10 on a dynamic f64 column",
            vec![
                ts_page(),
                raw_page(12, Enc::GcdI64, encode_gcd_i64(&ts).expect("gcd page")),
            ],
            &f64_plan,
            "enc GcdI64 is not an f64 codec",
        ),
    ];
    for (name, pages, plans, want) in typed {
        match decode_pages(3, pages, plans) {
            Err(LogSegError::Corrupted(m)) => assert_eq!(m, want, "{name}"),
            other => panic!("{name}: {:?}", other.map(|_| ())),
        }
    }
}

/// The writer keeps the candidate with the smallest stored size. On the i64
/// page FOR bit-pack is the smallest before zstd (402 bytes, under the 512-byte
/// floor, so stored raw) while plain compresses to 25; on the string page the
/// dictionary is the smallest before zstd and under the floor while plain
/// compresses below it. Both pages must carry plain.
///
/// Wrong implementations this rules out, each shown failing: choosing by the
/// size before zstd (FOR and dictionary win); choosing by the size after zstd
/// but compressing every candidate regardless of the floor (FOR and dictionary
/// win, because their compressed forms are smaller still).
#[test]
fn encoding_choice_uses_stored_size() {
    let n = 800usize;
    let pattern = [3i64, 9, 1, 14, 6, 11, 0, 15];
    let ints: Vec<i64> = (0..n).map(|i| pattern[i % 8]).collect();
    let words = ["alpha-bravo-charlie-delta", "echo-foxtrot-golf-hotel-india"];
    let strs: Vec<&[u8]> = (0..n).map(|i| words[i % 2].as_bytes()).collect();

    // The page shapes are what make this test distinguishing: check them.
    let pre_zstd = |c: Vec<(Enc, Vec<u8>)>| {
        c.into_iter()
            .fold(None::<(Enc, usize)>, |best, (e, b)| match best {
                Some((_, l)) if b.len() >= l => best,
                _ => Some((e, b.len())),
            })
            .expect("candidate")
    };
    let no_floor = |c: Vec<(Enc, Vec<u8>)>| {
        c.into_iter()
            .map(|(e, b)| (e, zstd::bulk::compress(&b, ZSTD_LEVEL).expect("zstd").len()))
            .fold(None::<(Enc, usize)>, |best, (e, l)| match best {
                Some((_, bl)) if l >= bl => best,
                _ => Some((e, l)),
            })
            .expect("candidate")
    };
    assert_eq!(pre_zstd(i64_candidates(&ints)), (Enc::ForBitpack, 402));
    assert_eq!(no_floor(i64_candidates(&ints)).0, Enc::ForBitpack);
    assert_eq!(pre_zstd(string_candidates(&strs)).0, Enc::Dict);
    assert!(pre_zstd(string_candidates(&strs)).1 < 512);
    assert_eq!(no_floor(string_candidates(&strs)).0, Enc::Dict);

    let records: Vec<LogRecord> = (0..n)
        .map(|i| {
            let t = 1_000 + i as i64;
            record(
                t,
                t,
                vec![
                    ("v".to_string(), AttrValue::I64(ints[i])),
                    ("s".to_string(), AttrValue::Str(words[i % 2].to_string())),
                ],
            )
        })
        .collect();
    let object = write_rows(&RlogConfig::default(), &records);

    let v = only_value_page(&object, dyn_column(&object, "v", FieldType::I64));
    assert_eq!((v.enc, v.comp, v.len), (Enc::Plain, COMP_ZSTD, 25));
    let s = only_value_page(&object, dyn_column(&object, "s", FieldType::Str));
    let s_expected = stored(string_candidates(&strs));
    assert_eq!(s_expected.enc, Enc::Plain);
    assert_eq!((s.enc, s.comp), (Enc::Plain, COMP_ZSTD));
    assert_eq!(s.len, s_expected.stored.len() as u64);
    assert!(s.len < pre_zstd(string_candidates(&strs)).1 as u64);

    let rows = scan_all(&object);
    assert_eq!(rows.len(), n);
    for (i, r) in rows.iter().enumerate() {
        let mut attrs = r.attrs.clone();
        attrs.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            attrs,
            vec![
                ("s".to_string(), AttrValue::Str(words[i % 2].to_string())),
                ("v".to_string(), AttrValue::I64(ints[i])),
            ],
            "row {i}"
        );
    }
}

/// The same one-block object written at zstd levels 1 and 19 stores different
/// BLOCKS bytes, its body page zstd-compressed at both, and reads back the same
/// records: the configured level reaches the block pages.
///
/// Wrong implementations this rules out, each shown failing: a block writer
/// that seals pages at a fixed level; a page sealer that ignores the level it
/// is given.
#[test]
fn zstd_level_reaches_the_block_pages() {
    let mut x = 7u64;
    let records: Vec<LogRecord> = (0..200)
        .map(|i| {
            let words: Vec<String> = (0..12)
                .map(|_| {
                    x = x
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    format!("w{}", (x >> 40) % 97)
                })
                .collect();
            let mut r = record(1_000 + i, 1_000 + i, Vec::new());
            r.body = words.join(" ");
            r
        })
        .collect();
    let at = |zstd_level| {
        let cfg = RlogConfig {
            zstd_level,
            ..RlogConfig::default()
        };
        let object = write_rows(&cfg, &records);
        assert_eq!(page_dir(&object).block_count(), 1);
        let body = only_value_page(&object, ravel_logseg::record::COL_BODY);
        assert_eq!(body.comp, COMP_ZSTD, "level {zstd_level}");
        let blocks = *open(&object)
            .expect("open")
            .section(kind::BLOCKS)
            .expect("BLOCKS");
        let stored = object[blocks.offset as usize..(blocks.offset + blocks.len) as usize].to_vec();
        assert_eq!(scan_all(&object), records, "level {zstd_level}");
        stored
    };
    let (fast, slow) = (at(1), at(19));
    assert_ne!(fast, slow, "levels 1 and 19 stored the same BLOCKS bytes");
}

/// The row path and both columnar paths (plain cells and dictionary-shaped
/// string columns) write the same bytes over a corpus that exercises tag 10,
/// tag 11, and the stored-size choice between dictionary and plain strings,
/// across several blocks, one of which carries an `observed_ts` that is not a
/// reference.
///
/// Wrong implementations this rules out, each shown failing: a columnar path
/// that never emits the reference; a dictionary-shaped path that keeps the
/// dictionary heuristic instead of comparing stored sizes.
#[test]
fn row_and_columnar_paths_identical_with_the_new_codecs() {
    let n = 1_200usize;
    let ts = whole_second_ts(n);
    let words = ["alpha-bravo-charlie-delta", "echo-foxtrot-golf-hotel-india"];
    // Four hosts in an order with no period, drawn from the gaps in `ts`: the
    // dictionary stores smaller than plain here, where `s`'s strict
    // alternation lets plain compress below it.
    let hosts = [
        "host-7f3a9c21e5d04b68",
        "host-0b8e2d47c1a95f36",
        "host-e64c1f0a9b27d853",
        "host-31d9a8e5f07c2b4e",
    ];
    let records: Vec<LogRecord> = ts
        .iter()
        .enumerate()
        .map(|(i, &t)| {
            let observed = if (600..700).contains(&i) { t + 1 } else { t };
            let host = hosts[(t / 1_000_000_000 % 4) as usize];
            record(
                t,
                observed,
                vec![
                    (
                        "code".to_string(),
                        AttrValue::I64(200 + 100 * (i as i64 % 3)),
                    ),
                    ("s".to_string(), AttrValue::Str(words[i % 2].to_string())),
                    ("u".to_string(), AttrValue::Str(host.to_string())),
                ],
            )
        })
        .collect();
    // One block per row group, so the per-block string codecs are what this
    // pins; `rlog_v5_rowgroup_dict.rs` covers the row-group dictionary.
    let cfg = RlogConfig {
        block_target_records: 256,
        group_target_blocks: 1,
        ..RlogConfig::default()
    };

    let rows = write_rows_reference(&cfg, &records);
    let columnar = write_columnar(&cfg, ColumnarLogBatch::from_records(&records));
    let dict = write_columnar(
        &cfg,
        ColumnarLogBatch::from_records(&records).with_dictionaries(),
    );
    assert!(rows == columnar, "row and columnar paths differ");
    assert!(
        rows == dict,
        "row and dictionary-shaped columnar paths differ"
    );

    // The corpus reaches every new path.
    let obs = value_pages(&rows, COL_OBSERVED_TS);
    assert_eq!(obs.len(), 5);
    assert_eq!(obs.iter().filter(|p| p.enc == Enc::ColumnRef).count(), 4);
    assert!(
        value_pages(&rows, COL_TS)
            .iter()
            .all(|p| p.enc == Enc::GcdI64)
    );
    let s = value_pages(&rows, dyn_column(&rows, "s", FieldType::Str));
    assert!(s.iter().all(|p| p.enc == Enc::Plain), "{s:?}");
    let u = value_pages(&rows, dyn_column(&rows, "u", FieldType::Str));
    // Four hosts in each 256-row block: a dictionary stores smaller than plain.
    // A one-block row group keeps the tag 7 page, because splitting it into a
    // tag 12 page and a tag 13 page saves the one-byte id width and costs a
    // whole PAGE_DIR entry.
    assert!(u.iter().all(|p| p.enc == Enc::Dict), "{u:?}");
    assert_eq!(u.len(), 5, "one tag 7 page per one-block row group");

    let got = scan_all(&rows);
    assert_eq!(got.len(), n);
    for (r, want) in got.iter().zip(&records) {
        assert_eq!(
            (r.ts_ns, r.observed_ts_ns),
            (want.ts_ns, want.observed_ts_ns)
        );
    }
}

// --- property tests ---------------------------------------------------------

fn arb_values() -> impl Strategy<Value = Vec<i64>> {
    prop_oneof![
        proptest::collection::vec(any::<i64>(), 1..64),
        (
            any::<i64>(),
            proptest::collection::vec(0u64..64, 1..200),
            prop::sample::select(vec![
                2u64,
                10,
                1_000_000_000,
                1 << 40,
                1 << 61,
                u64::MAX / 3
            ]),
        )
            .prop_map(|(base, steps, scale)| {
                steps
                    .iter()
                    .map(|s| (base as u64).wrapping_add(s.wrapping_mul(scale)) as i64)
                    .collect()
            }),
    ]
}

/// One corruption of a valid page: flip one bit, truncate, or splice bytes in.
fn mutate(bytes: &[u8], kind: u8, at: usize, bit: u8, splice: &[u8]) -> Vec<u8> {
    let mut m = bytes.to_vec();
    match kind % 3 {
        0 if !m.is_empty() => {
            let i = at % m.len();
            m[i] ^= 1 << (bit % 8);
        }
        1 => m.truncate(at % (m.len() + 1)),
        _ => {
            let i = at % (m.len() + 1);
            m.splice(i..i, splice.iter().copied());
        }
    }
    m
}

proptest! {
    /// Tag 10 round-trips every page it applies to, through the codec and
    /// through the reader's block decode; tag 11 decodes to a copy of `ts`. Any
    /// flip, truncation or splice of either page decodes to exactly `count`
    /// values or fails as `Corrupted`, never panics and never returns another
    /// error kind.
    #[test]
    fn gcd_and_column_ref_pages_round_trip_and_corruption_is_typed(
        values in arb_values(),
        kind in any::<u8>(),
        at in any::<usize>(),
        bit in any::<u8>(),
        splice in proptest::collection::vec(any::<u8>(), 1..6),
    ) {
        let n = values.len();
        if let Some(page) = encode_gcd_i64(&values) {
            prop_assert_eq!(&decode_gcd_i64(&page, n).expect("decodes"), &values);
            let block = decode_pages(n, vec![raw_page(COL_TS, Enc::GcdI64, page.clone())], &[])
                .expect("block decodes");
            let want: Vec<Option<i64>> = values.iter().copied().map(Some).collect();
            prop_assert_eq!(block.i64_col(COL_TS).expect("ts"), want.as_slice());

            let bad = mutate(&page, kind, at, bit, &splice);
            match decode_gcd_i64(&bad, n) {
                Ok(v) => prop_assert_eq!(v.len(), n),
                Err(LogSegError::Corrupted(_)) => {}
                Err(other) => prop_assert!(false, "untyped error {other:?}"),
            }
            let got = decode_pages(n, vec![raw_page(COL_TS, Enc::GcdI64, bad)], &[]);
            prop_assert!(got.is_ok() || is_corrupted(&got));
        }

        let ts_page = raw_page(COL_TS, Enc::Plain, plain_i64(&values));
        let reference = encode_column_ref(COL_TS);
        let block = decode_pages(
            n,
            vec![ts_page.clone(), raw_page(COL_OBSERVED_TS, Enc::ColumnRef, reference.clone())],
            &[],
        )
        .expect("reference decodes");
        prop_assert_eq!(block.i64_col(COL_OBSERVED_TS), block.i64_col(COL_TS));

        let bad = mutate(&reference, kind, at, bit, &splice);
        let got = decode_pages(
            n,
            vec![ts_page, raw_page(COL_OBSERVED_TS, Enc::ColumnRef, bad.clone())],
            &[],
        );
        if bad == reference {
            prop_assert!(got.is_ok());
        } else {
            prop_assert!(is_corrupted(&got), "{:?} -> {:?}", bad, got.map(|_| ()));
        }
    }
}
