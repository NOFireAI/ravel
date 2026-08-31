//! `StreamBlockRows`: the columnar-held, row-materializing block view
//! (ADR-0979's read-side primitive), against the eager
//! `decode_block_in_group` path it will replace in the compaction merge.
//!
//! The property both directions of this file assert is one thing: swapping the
//! eager per-block decode for the lazy view changes memory residency and
//! nothing else. Same records, same order, same typed error on corrupt input.
//! It follows the shape of the placement differential in
//! `rlog_v4_row_groups.rs` (the same record generators, the same
//! `format!("{r:?}")` key so an f64 attribute is compared by bits and NaN
//! payloads are not conflated) and extends it with the lazy path.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_logseg::footer::{kind, open};
use ravel_logseg::{
    AttrValue, LogRecord, LogSegError, LogStreamId, ObjectIdentity, RlogConfig, RlogRangeReader,
    RlogWriter, read_section, stream_attrs_bytes,
};

// --- fixtures ---------------------------------------------------------------

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

fn stream_blob(n: u8) -> Vec<u8> {
    stream_attrs_bytes(
        &[("service.name".to_string(), AttrValue::Str(format!("s{n}")))],
        "scope",
        "1",
        &[],
    )
}

fn write_object(cfg: RlogConfig, recs: &[LogRecord]) -> Vec<u8> {
    let identity = ObjectIdentity {
        tenant_hash: [3u8; 16],
        shard: 0,
        writer_id: [4u8; 16],
        writer_epoch: 1,
        writer_seq: 2,
    };
    let mut w = RlogWriter::new(cfg, identity);
    for r in recs {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

/// A ranged reader over `object`'s directories, built the way a compaction
/// input is built: each whole-read section fetched and decoded on its own.
fn range_reader(object: &[u8]) -> RlogRangeReader {
    let cfg = RlogConfig::default();
    let ftr = open(object).expect("open footer");
    let section = |k: u32| {
        read_section(object, ftr.section(k).expect("section present"), &cfg).expect("section")
    };
    let page_dir = ftr
        .section(kind::PAGE_DIR)
        .map(|d| read_section(object, d, &cfg).expect("PAGE_DIR"));
    RlogRangeReader::from_sections_with_page_dir(
        &ftr,
        &section(kind::STREAM_DIR),
        &section(kind::FIELD_DIR),
        &section(kind::SKIP_IDX),
        page_dir.as_deref(),
    )
    .expect("range reader")
}

/// A record key that distinguishes f64 by bits, so -0.0 and NaN payloads are
/// not conflated. Records are compared in yield order, not as a set: the two
/// paths must agree on order too.
fn key(r: &LogRecord) -> String {
    format!("{r:?}")
}

fn keys(recs: &[LogRecord]) -> Vec<String> {
    recs.iter().map(key).collect()
}

// --- the two paths ----------------------------------------------------------

/// Every record of `stream`, decoded the eager way: one row group fetched at a
/// time, each of its candidate blocks decoded into a `Vec<LogRecord>`.
fn eager_stream(
    reader: &RlogRangeReader,
    object: &[u8],
    stream: &LogStreamId,
) -> Result<Vec<LogRecord>, LogSegError> {
    let Some(locs) = reader.stream_blocks(stream)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for loc in &locs {
        let bytes = &object[loc.start() as usize..loc.end() as usize];
        for &b in loc.block_indices() {
            out.extend(reader.decode_block_in_group(loc, b, bytes)?);
        }
    }
    Ok(out)
}

/// Every record of `stream`, decoded the lazy way: the same fetch and the same
/// per-block decode, but the block stays columnar and each record is
/// materialized on demand.
fn lazy_stream(
    reader: &RlogRangeReader,
    object: &[u8],
    stream: &LogStreamId,
) -> Result<Vec<LogRecord>, LogSegError> {
    let Some(locs) = reader.stream_blocks(stream)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for loc in &locs {
        let bytes = &object[loc.start() as usize..loc.end() as usize];
        for &b in loc.block_indices() {
            let rows = reader.block_rows_in_group(loc, b, bytes)?;
            for rec in rows {
                out.push(rec?);
            }
        }
    }
    Ok(out)
}

/// The error's variant, for comparing how the two paths fail without pinning a
/// message. A block that is corrupt in more than one way at once can have the
/// two paths name a different column first: the lazy path validates the whole
/// `ts` column when the view is built, the eager path reaches each row's `ts`
/// as it rebuilds that row. Which column is named is not the contract; the
/// variant, and never returning a record instead, is.
fn variant(e: &LogSegError) -> &'static str {
    match e {
        LogSegError::Corrupted(_) => "Corrupted",
        LogSegError::UnsupportedVersion(_) => "UnsupportedVersion",
        LogSegError::LimitExceeded(_) => "LimitExceeded",
        LogSegError::InconsistentStreamAttrs(_) => "InconsistentStreamAttrs",
        LogSegError::Io(_) => "Io",
    }
}

// --- generators (the placement differential's, unchanged) -------------------

fn arb_attr_value() -> impl Strategy<Value = AttrValue> {
    let leaf = prop_oneof![
        any::<i64>().prop_map(AttrValue::I64),
        any::<f64>().prop_map(AttrValue::F64),
        Just(AttrValue::F64(-0.0)),
        Just(AttrValue::F64(f64::from_bits(f64::NAN.to_bits() | 0x7))),
        any::<bool>().prop_map(AttrValue::Bool),
        "[a-z]{0,4}".prop_map(AttrValue::Str),
        proptest::collection::vec(any::<u8>(), 0..4).prop_map(AttrValue::Bytes),
    ];
    leaf.prop_recursive(2, 6, 3, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..3).prop_map(AttrValue::List),
            proptest::collection::vec(("[a-z]{1,3}", inner), 0..3).prop_map(AttrValue::Map),
        ]
    })
}

fn arb_name() -> impl Strategy<Value = String> {
    prop::sample::select(vec!["a", "b", "c", "dup", "http.status", "svc.k"]).prop_map(String::from)
}

fn arb_record() -> impl Strategy<Value = LogRecord> {
    (
        0u8..3,
        0i64..40,
        0u8..30,
        prop::sample::select(vec!["", "INFO", "ERROR"]),
        prop::sample::select(vec!["", "hello", "a b c"]),
        prop_oneof![Just(None), any::<[u8; 16]>().prop_map(Some)],
        prop_oneof![Just(None), any::<[u8; 8]>().prop_map(Some)],
        any::<u32>(),
        proptest::collection::vec((arb_name(), arb_attr_value()), 0..6),
    )
        .prop_map(
            |(s, ts, sev, sevt, body, trace, span, flags, attrs)| LogRecord {
                stream_id: sid(s),
                stream_attrs: stream_blob(s),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: sev,
                severity_text: sevt.into(),
                body: body.into(),
                trace_id: trace,
                span_id: span,
                flags,
                attrs,
            },
        )
}

/// The records of one stream in the order the writer stores them: sorted by ts,
/// stably, so records sharing a ts keep their push order. The independent
/// oracle for both paths at once.
fn expected_stream(records: &[LogRecord], stream: &LogStreamId) -> Vec<LogRecord> {
    let mut mine: Vec<LogRecord> = records
        .iter()
        .filter(|r| r.stream_id == *stream)
        .cloned()
        .collect();
    mine.sort_by_key(|r| r.ts_ns);
    mine
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// Lazy versus eager, over the same generators the placement differential
    /// uses and over several block and row-group sizes, so blocks split inside a
    /// stream, row groups hold several blocks, and boundary blocks hold more
    /// than one stream. Draining `StreamBlockRows` must yield exactly the
    /// records `decode_block_in_group` returns, in the same order, and both must
    /// equal the written records of that stream in stored order.
    #[test]
    fn lazy_drain_equals_eager_decode_for_any_corpus_and_placement(
        records in proptest::collection::vec(arb_record(), 1..25),
        max_dyn in 1usize..8,
        block in 1usize..6,
        group in 1usize..4,
    ) {
        let cfg = RlogConfig {
            max_dynamic_columns: max_dyn,
            block_target_records: block,
            block_max_bytes: 8192,
            group_target_blocks: group,
            ..RlogConfig::default()
        };
        let object = write_object(cfg, &records);
        let reader = range_reader(&object);

        for s in 0..3u8 {
            let stream = sid(s);
            let eager = eager_stream(&reader, &object, &stream).expect("eager decode");
            let lazy = lazy_stream(&reader, &object, &stream).expect("lazy decode");
            prop_assert_eq!(keys(&lazy), keys(&eager), "stream {}", s);
            // Not compared against the input records here: a `List`/`Map`
            // attribute is stored in the canonical overflow blob and comes back
            // as its encoded `Bytes`, which is the writer's contract and not
            // what this differential is about. The deterministic case below
            // uses a scalar-only corpus and does pin the written records.
        }
    }

    /// Corrupt input, both paths. One byte inside a fetched row group is
    /// flipped, or the buffer is truncated; the lazy path must then either fail
    /// with the same error variant the eager path fails with, or succeed with
    /// exactly the records the eager path returns (a flip that landed on bytes
    /// no read verifies). Never a panic, and never different data.
    #[test]
    fn corrupt_group_bytes_fail_the_same_way_through_both_paths(
        at in any::<usize>(),
        xor in any::<u8>(),
        truncate in any::<bool>(),
        records in proptest::collection::vec(arb_record(), 1..25),
        block in 1usize..6,
        group in 1usize..4,
    ) {
        let cfg = RlogConfig {
            block_target_records: block,
            block_max_bytes: 8192,
            group_target_blocks: group,
            ..RlogConfig::default()
        };
        let object = write_object(cfg, &records);
        let reader = range_reader(&object);

        for s in 0..3u8 {
            let stream = sid(s);
            let Some(locs) = reader.stream_blocks(&stream).expect("stream_blocks") else {
                continue;
            };
            for loc in &locs {
                let clean = &object[loc.start() as usize..loc.end() as usize];
                if clean.is_empty() {
                    continue;
                }
                let damaged: Vec<u8> = if truncate {
                    clean[..at % clean.len()].to_vec()
                } else {
                    let mut m = clean.to_vec();
                    let i = at % m.len();
                    m[i] ^= xor | 1;
                    m
                };
                for &b in loc.block_indices() {
                    let eager = reader.decode_block_in_group(loc, b, &damaged);
                    let lazy = reader
                        .block_rows_in_group(loc, b, &damaged)
                        .and_then(|rows| rows.collect::<Result<Vec<LogRecord>, LogSegError>>());
                    match (eager, lazy) {
                        (Ok(e), Ok(l)) => prop_assert_eq!(keys(&l), keys(&e)),
                        (Err(e), Err(l)) => prop_assert_eq!(variant(&l), variant(&e)),
                        (Ok(e), Err(l)) => prop_assert!(
                            false,
                            "eager decoded {} records where lazy failed: {}",
                            e.len(),
                            l
                        ),
                        (Err(e), Ok(l)) => prop_assert!(
                            false,
                            "lazy decoded {} records where eager failed: {}",
                            l.len(),
                            e
                        ),
                    }
                }
            }
        }
    }
}

/// The block-boundary case named explicitly rather than left to the generators:
/// a stream whose records span many blocks over many row groups, drained one
/// record at a time, compared to the eager path by full struct equality.
#[test]
fn lazy_drain_equals_eager_decode_across_block_and_group_boundaries() {
    const BLOCK: usize = 4;
    const GROUP: usize = 2;
    const PER_STREAM: i64 = 40;

    let mut records = Vec::new();
    for s in 0..3u8 {
        for i in 0..PER_STREAM {
            records.push(LogRecord {
                stream_id: sid(s),
                stream_attrs: stream_blob(s),
                ts_ns: i,
                observed_ts_ns: i + 1,
                severity_num: (i % 24) as u8,
                severity_text: if i % 2 == 0 { "INFO" } else { "WARN" }.to_string(),
                body: format!("stream {s} record {i}"),
                trace_id: if i % 3 == 0 {
                    Some([(i % 251) as u8; 16])
                } else {
                    None
                },
                span_id: if i % 4 == 0 {
                    Some([(i % 251) as u8; 8])
                } else {
                    None
                },
                flags: (i as u32) & 7,
                attrs: vec![
                    ("k".to_string(), AttrValue::I64(i % 11)),
                    ("t".to_string(), AttrValue::Str(format!("v{}", i % 5))),
                ],
            });
        }
    }
    let cfg = RlogConfig {
        block_target_records: BLOCK,
        group_target_blocks: GROUP,
        ..RlogConfig::default()
    };
    let object = write_object(cfg, &records);
    let reader = range_reader(&object);

    for s in 0..3u8 {
        let stream = sid(s);
        let locs = reader
            .stream_blocks(&stream)
            .expect("stream_blocks")
            .expect("stream present");
        // The fixture must really cross boundaries, or this compares one block
        // against itself.
        assert!(
            locs.len() > 1,
            "stream {s} must span several row groups, got {}",
            locs.len()
        );
        assert!(
            locs.iter().any(|l| l.block_indices().len() > 1),
            "a row group must hold more than one of stream {s}'s blocks"
        );

        let eager = eager_stream(&reader, &object, &stream).expect("eager");
        let lazy = lazy_stream(&reader, &object, &stream).expect("lazy");
        let expected = expected_stream(&records, &stream);
        assert_eq!(lazy.len() as i64, PER_STREAM, "exact record count");
        // Full struct equality: this fixture carries no NaN, so `==` is exact
        // and a dropped optional column (a `span_id`, a `trace_id`, an
        // attribute) fails here while counts and timestamps still match.
        assert_eq!(lazy, eager, "stream {s}: lazy drain == eager decode");
        assert_eq!(
            lazy, expected,
            "stream {s}: and both == the written records"
        );
    }
}

/// The envelope `stream_ts_bounds` reports contains every timestamp the lazy
/// path actually yields, on the same multi-stream fixture. The envelope is
/// resident-metadata-only, so this is the end-to-end statement of the bound T4
/// admits inputs with.
#[test]
fn stream_ts_bounds_contains_every_ts_the_lazy_path_yields() {
    let records: Vec<LogRecord> = (0..60i64)
        .map(|i| {
            let s = (i % 3) as u8;
            LogRecord {
                stream_id: sid(s),
                stream_attrs: stream_blob(s),
                ts_ns: i * 7 % 53,
                observed_ts_ns: i,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: format!("body {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![("k".to_string(), AttrValue::I64(i))],
            }
        })
        .collect();
    let cfg = RlogConfig {
        block_target_records: 5,
        group_target_blocks: 2,
        ..RlogConfig::default()
    };
    let object = write_object(cfg, &records);
    let reader = range_reader(&object);

    for s in 0..3u8 {
        let stream = sid(s);
        let (min, max) = reader.stream_ts_bounds(&stream).expect("bounds");
        let yielded = lazy_stream(&reader, &object, &stream).expect("lazy");
        assert!(!yielded.is_empty(), "stream {s} must have records");
        for r in &yielded {
            assert!(
                min <= r.ts_ns && r.ts_ns <= max,
                "stream {s}: yielded ts {} outside envelope ({min}, {max})",
                r.ts_ns
            );
        }
    }
}
