//! Clustered row order and BLOOM scope for RLOG v5 (ADR-2135 decisions 1 and
//! 5, docs/log-segment-format.md "BLOCKS" and "BLOOM"):
//! [`RlogWriter::with_sort_descriptor`] and [`RlogWriter::with_bloom_scope`]
//! on both the row-major and the columnar write paths.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{
    SortBucketWidth, SortDescriptor, SortKeyColumn, SortKeyType, kind, open,
};
use ravel_logseg::record::{COL_BODY, COL_SEVERITY_TEXT};
use ravel_logseg::rlog_bloom::RlogBloomSection;
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::{
    AttrValue, BloomScope, ColumnarLogBatch, FieldType, LogRecord, LogSegError, LogStreamId,
    ObjectIdentity, Predicate, RlogConfig, RlogReader, RlogWriter, read_section,
    stream_attrs_bytes,
};

const HOUR: i64 = 3_600_000_000_000;
/// The start of an hour bucket: `T0.div_euclid(HOUR)` is 472_222.
const T0: i64 = 472_222 * HOUR;

const STREAM_A: LogStreamId = LogStreamId([0x11; 16]);
const STREAM_B: LogStreamId = LogStreamId([0x22; 16]);

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0xC3; 16],
        shard: 3,
        writer_id: [0x3C; 16],
        writer_epoch: 3,
        writer_seq: 30,
    }
}

fn stream_attrs() -> Vec<u8> {
    stream_attrs_bytes(
        &[(
            "service.name".to_string(),
            AttrValue::Str("svc".to_string()),
        )],
        "scope",
        "1.0",
        &[],
    )
}

/// A record whose body is `label`, so a scan's body sequence is its row order.
fn rec(
    stream_id: LogStreamId,
    ts_ns: i64,
    label: &str,
    attrs: Vec<(&str, AttrValue)>,
) -> LogRecord {
    LogRecord {
        stream_id,
        stream_attrs: stream_attrs(),
        ts_ns,
        observed_ts_ns: ts_ns + 1,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: label.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
    }
}

fn s(v: &str) -> AttrValue {
    AttrValue::Str(v.to_string())
}

fn descriptor(keys: &[(&str, SortKeyType)]) -> SortDescriptor {
    SortDescriptor {
        bucket_width: SortBucketWidth::OneHour,
        key_columns: keys
            .iter()
            .map(|(name, ty)| SortKeyColumn {
                name: name.to_string(),
                ty: *ty,
            })
            .collect(),
    }
}

fn writer(cfg: &RlogConfig, d: Option<&SortDescriptor>, generation: u64) -> RlogWriter {
    RlogWriter::new(*cfg, identity()).with_sort_descriptor(d.cloned(), generation)
}

fn write_rows(
    cfg: &RlogConfig,
    d: Option<&SortDescriptor>,
    generation: u64,
    records: &[LogRecord],
) -> Result<Vec<u8>, LogSegError> {
    let mut w = writer(cfg, d, generation);
    for r in records {
        w.push(r.clone())?;
    }
    w.finish()
}

/// One columnar batch of `records`, dictionary-shaped when `dict` is set, so
/// the writer's per-distinct dictionary path runs instead of its per-row one.
fn batch(records: &[LogRecord], dict: bool) -> ColumnarLogBatch {
    let b = ColumnarLogBatch::from_records(records);
    if dict { b.with_dictionaries() } else { b }
}

/// The columnar path, fed as two batches split at `split` so the global
/// dictionaries merge across batches.
fn write_columnar(
    cfg: &RlogConfig,
    d: Option<&SortDescriptor>,
    generation: u64,
    records: &[LogRecord],
    split: usize,
) -> Result<Vec<u8>, LogSegError> {
    write_columnar_shaped(cfg, d, generation, records, split, false)
}

fn write_columnar_shaped(
    cfg: &RlogConfig,
    d: Option<&SortDescriptor>,
    generation: u64,
    records: &[LogRecord],
    split: usize,
    dict: bool,
) -> Result<Vec<u8>, LogSegError> {
    let mut w = writer(cfg, d, generation);
    let (first, second) = records.split_at(split);
    for part in [first, second] {
        if !part.is_empty() {
            w.push_columnar(batch(part, dict))?;
        }
    }
    w.finish()
}

fn scan(object: &[u8]) -> Vec<LogRecord> {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(object, &cfg).expect("reader");
    reader.scan(&Predicate::And(vec![])).expect("scan").0
}

fn bodies(object: &[u8]) -> Vec<String> {
    scan(object).into_iter().map(|r| r.body).collect()
}

fn skip_index(object: &[u8]) -> SkipIndex {
    let ftr = open(object).expect("open");
    let raw = read_section(
        object,
        ftr.section(kind::SKIP_IDX).expect("SKIP_IDX"),
        &RlogConfig::default(),
    )
    .expect("read SKIP_IDX");
    SkipIndex::decode(&raw, u64::MAX).expect("decode SKIP_IDX")
}

/// Both write paths under `d`, the columnar one fed plain and dictionary-shaped
/// batches, asserted byte-identical, and the row order the object stores.
fn order_both_paths(d: &SortDescriptor, records: &[LogRecord]) -> Vec<String> {
    let cfg = RlogConfig::default();
    let rows = write_rows(&cfg, Some(d), 1, records).expect("row path");
    for dict in [false, true] {
        let cols = write_columnar_shaped(&cfg, Some(d), 1, records, records.len() / 2, dict)
            .expect("columnar");
        assert!(rows == cols, "dict {dict}: row and columnar objects differ");
    }
    bodies(&rows)
}

#[test]
fn clustered_object_sorts_by_bucket_then_key() {
    let k = |v: &str| vec![("tenant", s(v))];
    let records = vec![
        rec(STREAM_A, T0 + 300, "r0", k("b")),
        rec(STREAM_A, T0 + 100, "r1", k("c")),
        rec(STREAM_A, T0 + HOUR + 5, "r2", k("a")),
        rec(STREAM_A, T0 + 200, "r3", k("a")),
        rec(STREAM_A, T0 + HOUR + 1, "r4", k("b")),
        rec(STREAM_A, T0 + 400, "r5", k("b")),
        // An earlier bucket on a later stream still sorts after every row of
        // the earlier stream.
        rec(STREAM_B, T0 - HOUR, "b0", k("z")),
        rec(STREAM_B, T0 - HOUR + 9, "b1", k("a")),
    ];
    let d = descriptor(&[("tenant", SortKeyType::Str)]);
    let cfg = RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    };
    let expected = ["r3", "r0", "r5", "r1", "r2", "r4", "b1", "b0"];

    for (path, object) in [
        (
            "row",
            write_rows(&cfg, Some(&d), 7, &records).expect("row path"),
        ),
        (
            "columnar",
            write_columnar(&cfg, Some(&d), 7, &records, 4).expect("columnar path"),
        ),
    ] {
        let scanned = scan(&object);
        let order: Vec<&str> = scanned.iter().map(|r| r.body.as_str()).collect();
        assert_eq!(order, expected, "{path}: (stream, bucket, key, ts) order");

        let ftr = open(&object).expect("open");
        assert_eq!(ftr.sort_descriptor.as_ref(), Some(&d), "{path}");
        assert_eq!(ftr.clustering_generation, 7, "{path}");
        assert_eq!(ftr.min_ts_ns, T0 - HOUR, "{path}: object min_ts is a fold");
        assert_eq!(
            ftr.max_ts_ns,
            T0 + HOUR + 5,
            "{path}: object max_ts is a fold"
        );

        // Every block's min/max ts is a fold over its own rows, not its first
        // and last row: the clustered order puts neither extreme at an edge.
        let skip = skip_index(&object);
        let mut at = 0usize;
        let mut checked_non_edge = false;
        for e in &skip.l0 {
            let block = &scanned[at..at + e.record_count as usize];
            at += e.record_count as usize;
            let min = block.iter().map(|r| r.ts_ns).min().expect("nonempty");
            let max = block.iter().map(|r| r.ts_ns).max().expect("nonempty");
            assert_eq!((e.min_ts, e.max_ts), (min, max), "{path}: block ts fold");
            checked_non_edge |= block[0].ts_ns != min || block[block.len() - 1].ts_ns != max;
        }
        assert_eq!(at, records.len(), "{path}: blocks cover every row");
        assert!(
            checked_non_edge,
            "{path}: some block's edges are not its extremes"
        );
    }

    // Each width buckets by `ts.div_euclid(width)`: -1 and -width share
    // bucket -1, 1 and width - 1 share bucket 0, and width opens bucket 1.
    // Truncating division would put -1 in bucket 0, and another width would
    // split or merge these buckets.
    for (bucket_width, width) in [
        (SortBucketWidth::OneHour, HOUR),
        (SortBucketWidth::SixHours, 6 * HOUR),
        (SortBucketWidth::OneDay, 24 * HOUR),
    ] {
        let d = SortDescriptor {
            bucket_width,
            key_columns: vec![SortKeyColumn {
                name: "tenant".to_string(),
                ty: SortKeyType::Str,
            }],
        };
        let records = vec![
            rec(STREAM_A, width - 1, "p0", k("b")),
            rec(STREAM_A, 1, "p1", k("c")),
            rec(STREAM_A, width, "p2", k("a")),
            rec(STREAM_A, -1, "p3", k("z")),
            rec(STREAM_A, -width, "p4", k("y")),
        ];
        assert_eq!(
            order_both_paths(&d, &records),
            ["p4", "p3", "p0", "p1", "p2"],
            "{bucket_width:?}: euclidean buckets of {width} ns"
        );
    }
}

#[test]
fn row_and_columnar_paths_identical_under_a_key() {
    // First-seen order of `region` is zeta, alpha, mid, so its global
    // dictionary ids order differently from its values bytewise. The second
    // `region` of r3 is a later occurrence and must not be its key.
    let records = vec![
        rec(
            STREAM_A,
            T0 + 1,
            "r0",
            vec![("region", s("zeta")), ("code", AttrValue::I64(2))],
        ),
        rec(
            STREAM_B,
            T0 + 2,
            "r1",
            vec![("region", s("zeta")), ("code", AttrValue::I64(1))],
        ),
        rec(
            STREAM_A,
            T0 + 3,
            "r2",
            vec![("region", s("alpha")), ("code", AttrValue::I64(9))],
        ),
        rec(
            STREAM_A,
            T0 + 4,
            "r3",
            vec![
                ("code", AttrValue::I64(-4)),
                ("region", s("mid")),
                ("region", s("aaa")),
            ],
        ),
        rec(
            STREAM_B,
            T0 + 5,
            "r4",
            vec![("region", s("alpha")), ("code", AttrValue::I64(3))],
        ),
        rec(
            STREAM_A,
            T0 + 6,
            "r5",
            vec![("region", s("zeta")), ("code", AttrValue::I64(-7))],
        ),
        rec(STREAM_A, T0 + 7, "r6", vec![("code", AttrValue::I64(5))]),
        rec(
            STREAM_B,
            T0 + 8,
            "r7",
            vec![("region", s("mid")), ("code", AttrValue::I64(0))],
        ),
    ];
    let d = descriptor(&[("region", SortKeyType::Str), ("code", SortKeyType::I64)]);
    let cfg = RlogConfig::default();
    let rows = write_rows(&cfg, Some(&d), 3, &records).expect("row path");
    for split in [0, 1, 4, records.len()] {
        for dict in [false, true] {
            let cols = write_columnar_shaped(&cfg, Some(&d), 3, &records, split, dict)
                .expect("columnar path");
            assert!(
                rows == cols,
                "split {split}, dict {dict}: columnar object differs from row object"
            );
        }
    }
    let expected = ["r6", "r2", "r3", "r5", "r0", "r4", "r7", "r1"];
    assert_eq!(
        bodies(&rows),
        expected,
        "values order bytewise, not by first-seen dictionary id"
    );

    // With no dynamic-column budget both keys overflow into `attrs_raw` and
    // still order the rows.
    let no_columns = RlogConfig {
        max_dynamic_columns: 0,
        ..RlogConfig::default()
    };
    let mut w = writer(&no_columns, Some(&d), 3);
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let (overflowed, stats) = w.finish_with_stats().expect("row path, no budget");
    assert_eq!(stats.dynamic_columns_used, 0);
    let cols = write_columnar(&no_columns, Some(&d), 3, &records, 4).expect("columnar, no budget");
    assert!(
        overflowed == cols,
        "no budget: columnar object differs from row object"
    );
    assert_eq!(
        bodies(&overflowed),
        expected,
        "overflowed keys still order rows"
    );
}

#[test]
fn no_descriptor_output_is_unchanged() {
    let fixture: &[u8] = include_bytes!("fixtures/golden_rlog_v5.bin");
    let records = golden_records();

    let plain = {
        let mut w = RlogWriter::new(RlogConfig::default(), golden_identity());
        for r in &records {
            w.push(r.clone()).expect("push");
        }
        w.finish().expect("finish")
    };
    let explicit_defaults = {
        let mut w = RlogWriter::new(RlogConfig::default(), golden_identity())
            .with_sort_descriptor(None, 0)
            .with_bloom_scope(BloomScope::All);
        for r in &records {
            w.push(r.clone()).expect("push");
        }
        w.finish().expect("finish")
    };
    assert!(plain.as_slice() == fixture, "no builder: golden bytes");
    assert!(
        explicit_defaults.as_slice() == fixture,
        "default-valued builders: golden bytes"
    );
    let ftr = open(fixture).expect("open fixture");
    assert_eq!(ftr.sort_descriptor, None);
    assert_eq!(ftr.clustering_generation, 0);
}

#[test]
fn absent_key_values_sort_first_and_types_order_as_specified() {
    let one = |ty: SortKeyType| descriptor(&[("k", ty)]);
    let recs = |values: Vec<Option<AttrValue>>| -> Vec<LogRecord> {
        values
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                let attrs = v.map(|v| vec![("k", v)]).unwrap_or_default();
                rec(STREAM_A, T0 + 10 * i as i64, &format!("r{i}"), attrs)
            })
            .collect()
    };

    let i64s = recs(vec![
        Some(AttrValue::I64(10)),
        Some(AttrValue::I64(-5)),
        None,
        Some(AttrValue::I64(3)),
        Some(AttrValue::I64(i64::MIN)),
        // Another type under the key's name carries no value for it.
        Some(s("7")),
    ]);
    assert_eq!(
        order_both_paths(&one(SortKeyType::I64), &i64s),
        ["r2", "r5", "r4", "r1", "r3", "r0"],
        "I64: absent first, then numeric"
    );

    let strs = recs(vec![
        Some(s("b")),
        Some(s("")),
        Some(s("\u{e9}")),
        Some(s("a")),
        None,
        Some(s("B")),
    ]);
    assert_eq!(
        order_both_paths(&one(SortKeyType::Str), &strs),
        ["r4", "r1", "r5", "r3", "r0", "r2"],
        "Str: absent before the empty string, then bytewise"
    );

    let bools = recs(vec![
        Some(AttrValue::Bool(true)),
        Some(AttrValue::Bool(false)),
        None,
        Some(AttrValue::Bool(true)),
    ]);
    assert_eq!(
        order_both_paths(&one(SortKeyType::Bool), &bools),
        ["r2", "r1", "r0", "r3"],
        "Bool: absent, false, true"
    );

    let bytes = recs(vec![
        Some(AttrValue::Bytes(vec![0xFF])),
        Some(AttrValue::Bytes(vec![0x00, 0x01])),
        None,
        Some(AttrValue::Bytes(vec![0x00])),
        Some(AttrValue::Bytes(vec![])),
    ]);
    assert_eq!(
        order_both_paths(&one(SortKeyType::Bytes), &bytes),
        ["r2", "r4", "r3", "r1", "r0"],
        "Bytes: absent before the empty value, then bytewise"
    );

    // Equal keys at an equal ts keep push order.
    let ties = vec![
        rec(STREAM_A, T0 + 5, "t0", vec![("k", AttrValue::I64(1))]),
        rec(STREAM_A, T0 + 5, "t1", vec![]),
        rec(STREAM_A, T0 + 5, "t2", vec![("k", AttrValue::I64(1))]),
        rec(STREAM_A, T0 + 5, "t3", vec![]),
        rec(STREAM_A, T0 + 5, "t4", vec![("k", AttrValue::I64(1))]),
    ];
    assert_eq!(
        order_both_paths(&one(SortKeyType::I64), &ties),
        ["t1", "t3", "t0", "t2", "t4"],
        "ties keep push order"
    );
}

/// Records whose `note` and `region` string columns each carry a word found
/// nowhere else in the object, plus an I64 column no scope affects.
fn bloom_records() -> Vec<LogRecord> {
    (0..4)
        .map(|i| {
            rec(
                STREAM_A,
                T0 + i,
                &format!("request alpha{i}"),
                vec![
                    ("note", s(&format!("needle{i} haystack"))),
                    ("region", s("westcoast")),
                    ("code", AttrValue::I64(i)),
                ],
            )
        })
        .collect()
}

/// The object `scope` gives [`bloom_records`] on the row path, after asserting
/// the columnar path, fed plain and dictionary-shaped batches, writes the same
/// bytes.
fn write_scoped(scope: &BloomScope) -> Vec<u8> {
    let writer =
        || RlogWriter::new(RlogConfig::default(), identity()).with_bloom_scope(scope.clone());
    let mut w = writer();
    for r in bloom_records() {
        w.push(r).expect("push");
    }
    let rows = w.finish().expect("finish");
    for dict in [false, true] {
        let mut w = writer();
        w.push_columnar(batch(&bloom_records(), dict))
            .expect("push_columnar");
        let cols = w.finish().expect("finish");
        assert!(
            rows == cols,
            "{scope:?}, dict {dict}: row and columnar objects differ"
        );
    }
    rows
}

/// The object's FIELD_DIR and raw BLOOM section.
fn dir_and_bloom(object: &[u8]) -> (FieldDir, Vec<u8>) {
    let ftr = open(object).expect("open");
    let cfg = RlogConfig::default();
    let dir_raw = read_section(
        object,
        ftr.section(kind::FIELD_DIR).expect("FIELD_DIR"),
        &cfg,
    )
    .expect("read FIELD_DIR");
    let dir = FieldDir::decode(&dir_raw, u64::MAX).expect("decode FIELD_DIR");
    let bloom =
        read_section(object, ftr.section(kind::BLOOM).expect("BLOOM"), &cfg).expect("read BLOOM");
    (dir, bloom)
}

fn str_col(dir: &FieldDir, name: &str) -> u32 {
    dir.column(name, FieldType::Str).expect(name).column_id
}

/// Every key the writer derives from `text` for one column: its tokens and,
/// when short, its exact bytes.
fn keys_of(text: &str) -> Vec<Vec<u8>> {
    let mut keys = tokens(text);
    keys.push(text.as_bytes().to_vec());
    keys
}

/// Asserts the BLOOM coverage list is exactly `covered` and that no entry
/// holds a key of any column in `absent` for its values, while every covered
/// column's keys are present. Returns the number of keys probed absent.
fn assert_bloom(
    object: &[u8],
    covered: &[u32],
    absent: &[(u32, Vec<String>)],
    present: &[(u32, Vec<String>)],
) -> usize {
    let (dir, raw) = dir_and_bloom(object);
    let section = RlogBloomSection::parse(&raw, &dir).expect("parse BLOOM");
    assert_eq!(section.covered(), covered, "coverage list");
    assert!(!section.is_empty());
    let mut probed = 0;
    for (cid, values) in absent {
        assert!(!section.covers(*cid), "column {cid} is uncovered");
        for v in values {
            for key in keys_of(v) {
                for i in 0..section.len() {
                    let view = section.entry(i).expect("entry");
                    assert!(
                        !view.may_contain(*cid, &key),
                        "entry {i} holds uncovered column {cid} key {:?}",
                        String::from_utf8_lossy(&key)
                    );
                    probed += 1;
                }
            }
        }
    }
    for (cid, values) in present {
        for v in values {
            for key in keys_of(v) {
                let hit = (0..section.len())
                    .any(|i| section.entry(i).expect("entry").may_contain(*cid, &key));
                assert!(
                    hit,
                    "covered column {cid} key {:?}",
                    String::from_utf8_lossy(&key)
                );
            }
        }
    }
    probed
}

fn notes() -> Vec<String> {
    (0..4).map(|i| format!("needle{i} haystack")).collect()
}

fn text_values() -> Vec<(u32, Vec<String>)> {
    vec![
        (
            COL_BODY,
            (0..4).map(|i| format!("request alpha{i}")).collect(),
        ),
        (COL_SEVERITY_TEXT, vec!["INFO".to_string()]),
    ]
}

#[test]
fn bloom_scope_text_covers_only_body_and_severity() {
    let rows = write_scoped(&BloomScope::Text);
    let (dir, _) = dir_and_bloom(&rows);
    let (note, region) = (str_col(&dir, "note"), str_col(&dir, "region"));
    let mut covered = vec![COL_SEVERITY_TEXT, COL_BODY];
    covered.sort_unstable();
    let probed = assert_bloom(
        &rows,
        &covered,
        &[(note, notes()), (region, vec!["westcoast".to_string()])],
        &text_values(),
    );
    // 4 notes of 3 keys each plus 1 region value of 2 keys, over one entry.
    assert_eq!(probed, 14);
}

#[test]
fn bloom_scope_undeclared_excludes_declared_columns() {
    // `code` is an I64 column and `absent` names no column: neither changes
    // what is covered.
    let scope = BloomScope::Undeclared {
        declared: vec!["note".to_string(), "code".to_string(), "absent".to_string()],
    };
    let rows = write_scoped(&scope);
    let (dir, _) = dir_and_bloom(&rows);
    let (note, region) = (str_col(&dir, "note"), str_col(&dir, "region"));
    let mut covered = vec![COL_SEVERITY_TEXT, COL_BODY, region];
    covered.sort_unstable();
    let mut present = text_values();
    present.push((region, vec!["westcoast".to_string()]));
    let probed = assert_bloom(&rows, &covered, &[(note, notes())], &present);
    assert_eq!(probed, 12);

    // The same records under the default scope cover and hold `note`, so the
    // absence above is the scope's doing.
    let all = write_scoped(&BloomScope::All);
    let mut all_covered = vec![COL_SEVERITY_TEXT, COL_BODY, note, region];
    all_covered.sort_unstable();
    present.push((note, notes()));
    assert_bloom(&all, &all_covered, &[], &present);
}

#[test]
fn descriptor_naming_an_absent_or_stream_level_column_is_refused() {
    let records = vec![
        rec(
            STREAM_A,
            T0,
            "r0",
            vec![("code", AttrValue::I64(1)), ("region", s("west"))],
        ),
        rec(STREAM_B, T0 + 1, "r1", vec![("code", AttrValue::I64(2))]),
    ];
    let refused = |d: &SortDescriptor, generation: u64| -> Vec<String> {
        let cfg = RlogConfig::default();
        [
            write_rows(&cfg, Some(d), generation, &records),
            write_columnar(&cfg, Some(d), generation, &records, 1),
        ]
        .into_iter()
        .map(|r| match r {
            Err(LogSegError::InvalidSortDescriptor(why)) => why,
            other => panic!(
                "expected InvalidSortDescriptor, got {:?}",
                other.map(|object| format!("an object of {} bytes", object.len()))
            ),
        })
        .collect()
    };

    let why = refused(&descriptor(&[("missing", SortKeyType::Str)]), 1);
    assert_eq!(
        why,
        vec![
            "key column \"missing\" of type Str is not a per-record attribute of any record"
                .to_string();
            2
        ]
    );
    let why = refused(&descriptor(&[("service.name", SortKeyType::Str)]), 1);
    assert_eq!(
        why,
        vec![
            "key column \"service.name\" of type Str is not a per-record attribute of any \
             record (it is a stream-level attribute only)"
                .to_string();
            2
        ]
    );
    // `code` is carried, but as I64, not Str.
    let why = refused(&descriptor(&[("code", SortKeyType::Str)]), 1);
    assert_eq!(
        why,
        vec![
            "key column \"code\" of type Str is not a per-record attribute of any record"
                .to_string();
            2
        ]
    );
    // One carried key does not excuse an uncarried one after it.
    refused(
        &descriptor(&[("code", SortKeyType::I64), ("missing", SortKeyType::Bool)]),
        1,
    );
    // Shapes the footer decoder would refuse.
    refused(&descriptor(&[("code", SortKeyType::I64)]), 0);
    refused(&descriptor(&[]), 1);
    refused(
        &descriptor(&[
            ("a", SortKeyType::I64),
            ("b", SortKeyType::I64),
            ("c", SortKeyType::I64),
            ("d", SortKeyType::I64),
            ("e", SortKeyType::I64),
        ]),
        1,
    );
    refused(
        &descriptor(&[("code", SortKeyType::I64), ("code", SortKeyType::I64)]),
        1,
    );
    refused(&descriptor(&[("", SortKeyType::I64)]), 1);

    // A key only some records carry is accepted, and so is a cleared key.
    let cfg = RlogConfig::default();
    let d = descriptor(&[("region", SortKeyType::Str), ("code", SortKeyType::I64)]);
    write_rows(&cfg, Some(&d), 1, &records).expect("partially carried key");
    // A name the stream layer also carries is accepted once one record
    // carries it per-record, and the stream-level value is no value for the
    // key: m1 has none and sorts first, although its stream's "svc" sorts
    // after m0's "aaa" and m1 is the later row.
    let mixed = vec![
        rec(STREAM_A, T0, "m0", vec![("service.name", s("aaa"))]),
        rec(STREAM_A, T0 + 1, "m1", vec![]),
    ];
    assert_eq!(
        order_both_paths(&descriptor(&[("service.name", SortKeyType::Str)]), &mixed),
        ["m1", "m0"],
        "stream-level values are not key values"
    );
    let cleared = write_rows(&cfg, None, 4, &records).expect("cleared key");
    let ftr = open(&cleared).expect("open");
    assert_eq!((ftr.sort_descriptor, ftr.clustering_generation), (None, 4));
}

/// The `seq` attribute of every row the object stores, in stored order.
fn seqs(object: &[u8]) -> Vec<i64> {
    scan(object)
        .into_iter()
        .map(|r| {
            r.attrs
                .iter()
                .find_map(|(k, v)| match (k.as_str(), v) {
                    ("seq", AttrValue::I64(n)) => Some(*n),
                    _ => None,
                })
                .expect("seq attribute")
        })
        .collect()
}

#[test]
fn ties_keep_push_order_past_the_small_sort_cutoff() {
    // 192 rows at one ts on one stream, keyed 1, 2, 0, 1, 2, 0, ...: 64 rows
    // per key value, each group tied on the whole sort tuple. Push order is
    // not a sorted run, so the sort has to move rows, and at this length a
    // non-stable sort reorders rows it considers equal.
    let key_of = [1, 2, 0];
    let records: Vec<LogRecord> = (0..192i64)
        .map(|i| {
            rec(
                STREAM_A,
                T0 + 7,
                &format!("t{i}"),
                vec![
                    ("k", AttrValue::I64(key_of[i as usize % 3])),
                    ("seq", AttrValue::I64(i)),
                ],
            )
        })
        .collect();
    let d = descriptor(&[("k", SortKeyType::I64)]);
    let expected_seq: Vec<i64> = [2i64, 0, 1]
        .iter()
        .flat_map(|first| (0..64).map(move |j| first + 3 * j))
        .collect();
    assert_eq!(expected_seq.len(), 192);
    let expected_bodies: Vec<String> = expected_seq.iter().map(|i| format!("t{i}")).collect();

    assert_eq!(
        order_both_paths(&d, &records),
        expected_bodies,
        "each key group in push order"
    );
    let cfg = RlogConfig::default();
    let rows = write_rows(&cfg, Some(&d), 1, &records).expect("row path");
    let cols = write_columnar(&cfg, Some(&d), 1, &records, 96).expect("columnar path");
    assert_eq!(seqs(&rows), expected_seq, "row path");
    assert_eq!(seqs(&cols), expected_seq, "columnar path");
}

#[test]
fn equal_keys_order_by_ts_whatever_the_push_order() {
    // Inside one (stream, bucket, key) group the rows arrive in descending ts
    // and are stored ascending. Key "a" rows interleave with key "b" rows in
    // ts, so ordering by ts ahead of the key would interleave the groups.
    let k = |v: &str| vec![("tenant", s(v))];
    let records = vec![
        rec(STREAM_A, T0 + 50, "a50", k("a")),
        rec(STREAM_A, T0 + 45, "b45", k("b")),
        rec(STREAM_A, T0 + 40, "a40", k("a")),
        rec(STREAM_A, T0 + 35, "b35", k("b")),
        rec(STREAM_A, T0 + 30, "a30", k("a")),
        // The next bucket, also pushed descending.
        rec(STREAM_A, T0 + HOUR + 2, "c2", k("a")),
        rec(STREAM_A, T0 + HOUR + 1, "c1", k("a")),
    ];
    let d = descriptor(&[("tenant", SortKeyType::Str)]);
    let expected = ["a30", "a40", "a50", "b35", "b45", "c1", "c2"];
    assert_eq!(order_both_paths(&d, &records), expected, "row path");
    let cols =
        write_columnar(&RlogConfig::default(), Some(&d), 1, &records, 3).expect("columnar path");
    assert_eq!(bodies(&cols), expected, "columnar path");
    let ts: Vec<i64> = scan(&cols).iter().map(|r| r.ts_ns - T0).collect();
    assert_eq!(ts, [30, 40, 50, 35, 45, HOUR + 1, HOUR + 2]);
}

#[test]
fn compacted_objects_carry_and_validate_the_descriptor() {
    let k = |v: &str| vec![("tenant", s(v))];
    let records = vec![
        rec(STREAM_A, T0 + 3, "r0", k("b")),
        rec(STREAM_A, T0 + 1, "r1", k("c")),
        rec(STREAM_A, T0 + 2, "r2", k("a")),
        rec(STREAM_B, T0, "r3", k("a")),
    ];
    let hash = vec![0xAA, 0xBB, 0xCC];
    let cfg = RlogConfig::default();
    let row_writer = |d: &SortDescriptor| {
        let mut w = writer(&cfg, Some(d), 9);
        for r in &records {
            w.push(r.clone()).expect("push");
        }
        w
    };
    let columnar_writer = |d: &SortDescriptor| {
        let mut w = writer(&cfg, Some(d), 9);
        w.push_columnar(batch(&records[..2], false)).expect("push");
        w.push_columnar(batch(&records[2..], true)).expect("push");
        w
    };

    let d = descriptor(&[("tenant", SortKeyType::Str)]);
    let rows = row_writer(&d)
        .finish_compacted(1, hash.clone(), 2)
        .expect("row path");
    let (rows_stats, _) = row_writer(&d)
        .finish_compacted_with_stats(1, hash.clone(), 2)
        .expect("row path, with stats");
    let cols = columnar_writer(&d)
        .finish_compacted(1, hash.clone(), 2)
        .expect("columnar path");
    let (cols_stats, _) = columnar_writer(&d)
        .finish_compacted_with_stats(1, hash.clone(), 2)
        .expect("columnar path, with stats");
    assert!(rows == rows_stats, "finish_compacted_with_stats bytes");
    assert!(rows == cols, "row and columnar compacted objects differ");
    assert!(
        rows == cols_stats,
        "columnar finish_compacted_with_stats bytes"
    );
    assert_eq!(bodies(&rows), ["r2", "r0", "r1", "r3"]);
    let ftr = open(&rows).expect("open");
    assert_eq!(ftr.sort_descriptor.as_ref(), Some(&d));
    assert_eq!(ftr.clustering_generation, 9);
    assert_eq!(
        (ftr.level, ftr.input_set_hash.as_slice(), ftr.part_index),
        (1, hash.as_slice(), 2)
    );

    let missing = descriptor(&[("missing", SortKeyType::Str)]);
    let refusals = [
        row_writer(&missing).finish_compacted(1, hash.clone(), 2),
        row_writer(&missing)
            .finish_compacted_with_stats(1, hash.clone(), 2)
            .map(|(o, _)| o),
        columnar_writer(&missing).finish_compacted(1, hash.clone(), 2),
        columnar_writer(&missing)
            .finish_compacted_with_stats(1, hash.clone(), 2)
            .map(|(o, _)| o),
    ];
    for (i, r) in refusals.into_iter().enumerate() {
        match r {
            Err(LogSegError::InvalidSortDescriptor(why)) => assert_eq!(
                why,
                "key column \"missing\" of type Str is not a per-record attribute of any record",
                "refusal {i}"
            ),
            other => panic!(
                "refusal {i}: expected InvalidSortDescriptor, got {:?}",
                other.map(|object| format!("an object of {} bytes", object.len()))
            ),
        }
    }
}

#[test]
fn cleared_key_on_the_columnar_path_records_only_the_generation() {
    // Pushed out of ts order within a stream, so the (stream_ref, ts) order a
    // cleared key keeps is visible.
    let records = vec![
        rec(STREAM_B, T0 + 1, "b1", vec![("tenant", s("a"))]),
        rec(STREAM_A, T0 + 5, "a5", vec![("tenant", s("a"))]),
        rec(STREAM_A, T0 + 2, "a2", vec![("tenant", s("z"))]),
    ];
    let cfg = RlogConfig::default();
    let rows = write_rows(&cfg, None, 6, &records).expect("row path");
    for split in [0, 1, 2] {
        let cols = write_columnar(&cfg, None, 6, &records, split).expect("columnar path");
        let ftr = open(&cols).expect("open");
        assert_eq!(
            (ftr.sort_descriptor, ftr.clustering_generation),
            (None, 6),
            "split {split}"
        );
        assert!(cols == rows, "split {split}: columnar object differs");
        assert_eq!(bodies(&cols), ["a2", "a5", "b1"], "split {split}");
    }
}

#[test]
fn list_and_map_values_under_a_bytes_key_sort_by_their_encoding() {
    use ravel_logseg::record::canonical_value_bytes;
    let values = [
        AttrValue::Map(vec![("k".to_string(), AttrValue::Bool(true))]),
        AttrValue::List(vec![AttrValue::I64(2)]),
        AttrValue::Bytes(vec![0xFF]),
        AttrValue::List(vec![AttrValue::Str("a".to_string())]),
        AttrValue::List(vec![AttrValue::I64(1), AttrValue::I64(0)]),
        AttrValue::Bytes(vec![]),
        AttrValue::Map(vec![]),
    ];
    let records: Vec<LogRecord> = values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            rec(
                STREAM_A,
                T0 + i as i64,
                &format!("r{i}"),
                vec![("k", v.clone())],
            )
        })
        .collect();

    // The order bytewise comparison of each value's stored bytes gives.
    let stored = |v: &AttrValue| match v {
        AttrValue::Bytes(b) => b.clone(),
        other => canonical_value_bytes(other),
    };
    let mut by_encoding: Vec<usize> = (0..values.len()).collect();
    by_encoding.sort_by(|&a, &b| stored(&values[a]).cmp(&stored(&values[b])));
    let by_encoding: Vec<String> = by_encoding.iter().map(|i| format!("r{i}")).collect();

    let d = descriptor(&[("k", SortKeyType::Bytes)]);
    let order = order_both_paths(&d, &records);
    assert_eq!(order, by_encoding, "bytewise over the stored encoding");
    assert_eq!(order, ["r5", "r3", "r1", "r4", "r6", "r0", "r2"]);
}

#[test]
fn bloom_scope_undeclared_with_nothing_declared_equals_all() {
    let all = write_scoped(&BloomScope::All);
    let none_declared = write_scoped(&BloomScope::Undeclared { declared: vec![] });
    assert!(all == none_declared, "object bytes differ");
    // `code` has no Str column and `absent` no column at all.
    let no_str_declared = write_scoped(&BloomScope::Undeclared {
        declared: vec!["code".to_string(), "absent".to_string()],
    });
    assert!(all == no_str_declared, "non-string declared names");
    let (dir, _) = dir_and_bloom(&none_declared);
    let mut covered = vec![
        COL_SEVERITY_TEXT,
        COL_BODY,
        str_col(&dir, "note"),
        str_col(&dir, "region"),
    ];
    covered.sort_unstable();
    let mut present = text_values();
    present.push((str_col(&dir, "note"), notes()));
    present.push((str_col(&dir, "region"), vec!["westcoast".to_string()]));
    assert_bloom(&none_declared, &covered, &[], &present);
}

#[test]
fn bloom_scope_undeclared_matches_declared_names_across_types() {
    // `dual` is both an I64 column and a Str column. Declaring the name (for
    // its I64 column) uncovers the Str column too; a scan for its word still
    // returns every row carrying it.
    let records: Vec<LogRecord> = (0..4)
        .map(|i| {
            rec(
                STREAM_A,
                T0 + i,
                &format!("request alpha{i}"),
                vec![("dual", AttrValue::I64(i)), ("dual", s("dualword"))],
            )
        })
        .collect();
    let write = |scope: BloomScope| {
        let mut w = RlogWriter::new(RlogConfig::default(), identity()).with_bloom_scope(scope);
        for r in &records {
            w.push(r.clone()).expect("push");
        }
        w.finish().expect("finish")
    };
    let declared = write(BloomScope::Undeclared {
        declared: vec!["dual".to_string()],
    });
    let all = write(BloomScope::All);

    let (dir, _) = dir_and_bloom(&declared);
    let dual = str_col(&dir, "dual");
    assert!(dir.column("dual", FieldType::I64).is_some(), "I64 column");
    let mut text_only = vec![COL_SEVERITY_TEXT, COL_BODY];
    text_only.sort_unstable();
    let probed = assert_bloom(
        &declared,
        &text_only,
        &[(dual, vec!["dualword".to_string()])],
        &text_values(),
    );
    // 1 value of 2 keys over one entry.
    assert_eq!(probed, 2);

    let mut with_dual = vec![COL_SEVERITY_TEXT, COL_BODY, dual];
    with_dual.sort_unstable();
    let mut present = text_values();
    present.push((dual, vec!["dualword".to_string()]));
    assert_bloom(&all, &with_dual, &[], &present);

    let word = Predicate::HasWord {
        field: ravel_logseg::FieldSel::Attr("dual".to_string()),
        word: "dualword".to_string(),
    };
    for (scope, object) in [("undeclared", &declared), ("all", &all)] {
        let reader = RlogReader::new(object, &RlogConfig::default()).expect("reader");
        let (hits, _) = reader.scan(&word).expect("scan");
        assert_eq!(hits.len(), 4, "{scope}: every row carrying the word");
    }
}

fn golden_identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0xB2; 16],
        shard: 2,
        writer_id: [0x2B; 16],
        writer_epoch: 2,
        writer_seq: 20,
    }
}

/// The corpus `tests/golden_bytes_v5.rs` builds `golden_rlog_v5.bin` from.
fn golden_records() -> Vec<LogRecord> {
    let streams: [(LogStreamId, &str); 2] = [
        (LogStreamId([0x11; 16]), "checkout"),
        (LogStreamId([0x22; 16]), "payments"),
    ];
    let mut out = Vec::new();
    for (stream_id, service) in streams {
        let stream_attrs = stream_attrs_bytes(
            &[(
                "service.name".to_string(),
                AttrValue::Str(service.to_string()),
            )],
            "scope",
            "1.0",
            &[],
        );
        for i in 0..4i64 {
            out.push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: 1_650_000_000_000_000_000 + i * 1000,
                observed_ts_ns: 1_650_000_000_000_000_000 + i * 1000 + 5,
                severity_num: 9 + (i as u8 % 3),
                severity_text: "INFO".to_string(),
                body: format!("{service} handled request {i}"),
                trace_id: Some([i as u8; 16]),
                span_id: Some([i as u8; 8]),
                flags: 1,
                attrs: vec![
                    ("http.status".to_string(), AttrValue::I64(200 + i)),
                    ("http.method".to_string(), AttrValue::Str("GET".to_string())),
                    ("cache.hit".to_string(), AttrValue::Bool(i % 2 == 0)),
                ]
                .into_iter()
                .chain(if i % 2 == 1 {
                    vec![
                        (
                            "http.status".to_string(),
                            AttrValue::Bytes(vec![0xAB, 0xCD]),
                        ),
                        ("cache.hit".to_string(), AttrValue::Bool(i % 2 != 0)),
                    ]
                } else {
                    Vec::new()
                })
                .collect(),
            });
        }
    }
    out
}
