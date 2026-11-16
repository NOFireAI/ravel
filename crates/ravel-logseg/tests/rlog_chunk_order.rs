//! `RlogConfig::chunk_order` (ADR-3217 decision 8): the listed attribute names
//! take the lowest dynamic column ids in the listed order, so their chunks sit
//! first and adjacent in every row group, while FIELD_DIR stays sorted by
//! `(name bytes, type)` and an empty order is the alphabetical assignment.
#![allow(clippy::expect_used)]

use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{kind, open};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::record::FIRST_DYNAMIC_COL;
use ravel_logseg::{
    AttrValue, ColumnarLogBatch, FieldType, LogRecord, LogStreamId, ObjectIdentity, Predicate,
    RlogConfig, RlogReader, RlogWriter, read_section, stream_attrs_bytes,
};

const MAX_FIELD_DIR_ENTRIES: u64 = 1 << 16;

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [5u8; 16],
        shard: 0,
        writer_id: [6u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Six attribute names on every record, so every name has a chunk in every
/// row group. Their `(name bytes, type)` order is the order of this list.
fn corpus(records: usize) -> Vec<LogRecord> {
    (0..records)
        .map(|i| {
            let s = (i % 2) as u8;
            LogRecord {
                stream_id: sid(s),
                stream_attrs: stream_attrs_bytes(
                    &[("service.name".to_string(), AttrValue::Str(format!("s{s}")))],
                    "scope",
                    "1",
                    &[],
                ),
                ts_ns: i as i64,
                observed_ts_ns: i as i64,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: format!("record {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![
                    ("Age".to_string(), AttrValue::I64(i as i64 % 90)),
                    ("CounterID".to_string(), AttrValue::I64(i as i64 % 7)),
                    ("EventDate".to_string(), AttrValue::I64(19_000 + i as i64)),
                    (
                        "SearchPhrase".to_string(),
                        AttrValue::Str(format!("phrase {}", i % 13)),
                    ),
                    ("URL".to_string(), AttrValue::Str(format!("/p/{}", i % 17))),
                    ("UserID".to_string(), AttrValue::I64(1_000 + i as i64 % 31)),
                ],
            }
        })
        .collect()
}

fn cfg(chunk_order: &'static [&'static str]) -> RlogConfig {
    RlogConfig {
        block_target_records: 16,
        group_target_blocks: 4,
        chunk_order,
        ..RlogConfig::default()
    }
}

fn write_rows(cfg: RlogConfig, recs: &[LogRecord]) -> Vec<u8> {
    let mut w = RlogWriter::new(cfg, identity());
    for r in recs {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

fn write_columnar(cfg: RlogConfig, recs: &[LogRecord]) -> Vec<u8> {
    let mut w = RlogWriter::new(cfg, identity());
    w.push_columnar(ColumnarLogBatch::from_records(recs))
        .expect("push_columnar");
    w.finish().expect("finish")
}

fn field_dir_of(object: &[u8]) -> FieldDir {
    let ftr = open(object).expect("open footer");
    let raw = read_section(
        object,
        ftr.section(kind::FIELD_DIR).expect("FIELD_DIR present"),
        &RlogConfig::default(),
    )
    .expect("read FIELD_DIR");
    // `decode` refuses entries not ascending by `(name bytes, type)`, so a
    // successful decode is the sort-order check.
    FieldDir::decode(&raw, MAX_FIELD_DIR_ENTRIES).expect("decode FIELD_DIR")
}

fn page_dir_of(object: &[u8]) -> PageDir {
    let ftr = open(object).expect("open footer");
    let raw = read_section(
        object,
        ftr.section(kind::PAGE_DIR).expect("PAGE_DIR present"),
        &RlogConfig::default(),
    )
    .expect("read PAGE_DIR");
    PageDir::decode(&raw).expect("decode PAGE_DIR")
}

/// `(name, column_id)` in FIELD_DIR entry order.
fn ids(object: &[u8]) -> Vec<(String, u32)> {
    field_dir_of(object)
        .entries()
        .iter()
        .map(|e| (e.name.clone(), e.column_id))
        .collect()
}

fn id_of(object: &[u8], name: &str) -> u32 {
    ids(object)
        .into_iter()
        .find(|(n, _)| n == name)
        .map(|(_, id)| id)
        .expect("name has a column")
}

fn scan_all(object: &[u8]) -> Vec<LogRecord> {
    let reader = RlogReader::new(object, &RlogConfig::default()).expect("open");
    reader.scan(&Predicate::And(Vec::new())).expect("scan").0
}

fn named(pairs: &[(&str, u32)]) -> Vec<(String, u32)> {
    pairs.iter().map(|(n, id)| (n.to_string(), *id)).collect()
}

/// The alphabetical assignment every object written before decision 8 has.
fn alphabetical() -> Vec<(String, u32)> {
    named(&[
        ("Age", 10),
        ("CounterID", 11),
        ("EventDate", 12),
        ("SearchPhrase", 13),
        ("URL", 14),
        ("UserID", 15),
    ])
}

#[test]
fn declared_names_take_the_lowest_ids_in_the_given_order() {
    let recs = corpus(200);
    for (path, object) in [
        ("rows", write_rows(cfg(&["UserID", "SearchPhrase"]), &recs)),
        (
            "columnar",
            write_columnar(cfg(&["UserID", "SearchPhrase"]), &recs),
        ),
    ] {
        assert_eq!(FIRST_DYNAMIC_COL, 10);
        // UserID at 10, not SearchPhrase: the given order, not the names'
        // alphabetical order among themselves.
        assert_eq!(
            ids(&object),
            named(&[
                ("Age", 12),
                ("CounterID", 13),
                ("EventDate", 14),
                ("SearchPhrase", 11),
                ("URL", 15),
                ("UserID", 10),
            ]),
            "{path}"
        );
        assert_eq!(scan_all(&object), scan_all(&write_rows(cfg(&[]), &recs)));
    }
}

#[test]
fn declared_chunks_come_first_and_adjacent_in_every_row_group() {
    let recs = corpus(200);
    let object = write_rows(cfg(&["UserID", "SearchPhrase"]), &recs);
    let page_dir = page_dir_of(&object);
    assert!(
        page_dir.groups.len() > 1,
        "fixture spans several row groups"
    );
    for (g, group) in page_dir.groups.iter().enumerate() {
        let dynamic: Vec<_> = group
            .chunks
            .iter()
            .filter(|c| c.column_id >= FIRST_DYNAMIC_COL)
            .collect();
        let order: Vec<u32> = dynamic.iter().map(|c| c.column_id).collect();
        assert_eq!(order, vec![10, 11, 12, 13, 14, 15], "group {g}");
        let (user_off, user_len) = dynamic[0].extent().expect("UserID extent");
        let (phrase_off, _) = dynamic[1].extent().expect("SearchPhrase extent");
        assert_eq!(user_off + user_len, phrase_off, "group {g} adjacency");
        // Every dynamic chunk follows every fixed one in the group's bytes.
        let last_fixed_end = group
            .chunks
            .iter()
            .filter(|c| c.column_id < FIRST_DYNAMIC_COL)
            .filter_map(|c| c.extent())
            .map(|(off, len)| off + len)
            .max()
            .expect("fixed chunks");
        assert_eq!(last_fixed_end, user_off, "group {g}: UserID chunk first");
    }
}

#[test]
fn default_order_is_the_alphabetical_assignment() {
    let recs = corpus(200);
    let default_cfg = RlogConfig {
        block_target_records: 16,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    assert!(default_cfg.chunk_order.is_empty());
    let rows = write_rows(default_cfg, &recs);
    assert_eq!(ids(&rows), alphabetical());
    assert_eq!(ids(&write_columnar(default_cfg, &recs)), alphabetical());
}

#[test]
fn an_unknown_name_changes_nothing() {
    let recs = corpus(200);
    let default_object = write_rows(cfg(&[]), &recs);
    assert_eq!(write_rows(cfg(&["NoSuchColumn"]), &recs), default_object);
    assert_eq!(
        write_columnar(cfg(&["NoSuchColumn"]), &recs),
        write_columnar(cfg(&[]), &recs)
    );
    // An unknown name ahead of a known one does not take an id either.
    let object = write_rows(cfg(&["NoSuchColumn", "UserID"]), &recs);
    assert_eq!(id_of(&object, "UserID"), 10);
    assert_eq!(id_of(&object, "Age"), 11);
}

#[test]
fn one_name_with_two_types_keeps_type_byte_order() {
    let mut recs = corpus(64);
    for (i, r) in recs.iter_mut().enumerate() {
        if i % 2 == 0 {
            for (k, v) in &mut r.attrs {
                if k == "UserID" {
                    *v = AttrValue::Str(format!("u{i}"));
                }
            }
        }
    }
    let object = write_rows(cfg(&["UserID"]), &recs);
    let dir = field_dir_of(&object);
    let int = dir.column("UserID", FieldType::I64).expect("I64 column");
    let text = dir.column("UserID", FieldType::Str).expect("Str column");
    let (first, second) = if FieldType::I64.to_u8() < FieldType::Str.to_u8() {
        (int, text)
    } else {
        (text, int)
    };
    assert_eq!((first.column_id, second.column_id), (10, 11));
    assert_eq!(id_of(&object, "Age"), 12);
}

#[test]
fn max_dynamic_columns_truncates_the_declared_order() {
    let recs = corpus(64);
    let capped = |chunk_order| RlogConfig {
        max_dynamic_columns: 2,
        ..cfg(chunk_order)
    };
    // Alphabetically the two columns go to Age and CounterID; declaring UserID
    // gives it one of them and pushes CounterID into attrs_raw.
    assert_eq!(
        ids(&write_rows(capped(&[]), &recs)),
        named(&[("Age", 10), ("CounterID", 11)])
    );
    let object = write_rows(capped(&["UserID"]), &recs);
    assert_eq!(ids(&object), named(&[("Age", 11), ("UserID", 10)]));
    // The overflowed attributes still read back, through attrs_raw. A record
    // lists its column attributes before its attrs_raw ones, so which names
    // overflowed changes the list's order and nothing else.
    let sorted = |object: &[u8]| {
        let mut recs = scan_all(object);
        for r in &mut recs {
            r.attrs.sort_by(|a, b| a.0.cmp(&b.0));
        }
        recs.sort_by_key(|r| (r.stream_id.0, r.ts_ns));
        recs
    };
    let mut want = recs.clone();
    for r in &mut want {
        r.attrs.sort_by(|a, b| a.0.cmp(&b.0));
    }
    want.sort_by_key(|r| (r.stream_id.0, r.ts_ns));
    assert_eq!(sorted(&object), want);
    assert_eq!(sorted(&write_rows(capped(&[]), &recs)), want);
}
