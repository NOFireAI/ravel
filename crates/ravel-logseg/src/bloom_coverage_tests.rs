//! Reader behaviour over BLOOM coverage (docs/log-segment-format.md "BLOOM"):
//! a column the covered-column list omits never prunes, whichever predicate
//! arm names it, and a covered list that fails its crc prunes nothing.
#![allow(clippy::expect_used)]

use crate::bloom::BloomBuilder;
use crate::field_dir::FieldDir;
use crate::footer::{kind, open, write_footer_and_trailer};
use crate::record::{COL_BODY, COL_SEVERITY_TEXT};
use crate::rlog_bloom::{RlogBloomSection, encode_rlog_bloom_section};
use crate::{
    AttrValue, FieldSel, FieldType, LogRecord, LogSegError, LogStreamId, ObjectIdentity, Predicate,
    RlogConfig, RlogReader, RlogWriter, read_section, stream_attrs_bytes,
};

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0x5A; 16],
        shard: 1,
        writer_id: [0xA5; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn record(i: i64, body: &str, note: &str) -> LogRecord {
    LogRecord {
        stream_id: LogStreamId([0x33; 16]),
        stream_attrs: stream_attrs_bytes(
            &[("service.name".to_string(), AttrValue::Str("svc".into()))],
            "scope",
            "1.0",
            &[],
        ),
        ts_ns: 1_700_000_000_000_000_000 + i,
        observed_ts_ns: 1_700_000_000_000_000_000 + i,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            ("note".to_string(), AttrValue::Str(note.to_string())),
            ("region".to_string(), AttrValue::Str("west".to_string())),
        ],
    }
}

fn records() -> Vec<LogRecord> {
    vec![
        record(0, "request alpha", "needle in here"),
        record(1, "request beta", "only hay here"),
    ]
}

/// One block holding `records()`, with `note` left out of BLOOM entirely.
fn object_without_note_coverage() -> Vec<u8> {
    let mut w = RlogWriter::new(RlogConfig::default(), identity()).with_bloom_scope(
        crate::writer::BloomScope::Undeclared {
            declared: vec!["note".to_string()],
        },
    );
    for r in records() {
        w.push(r).expect("push");
    }
    w.finish().expect("finish")
}

fn field_dir_of(object: &[u8]) -> FieldDir {
    let ftr = open(object).expect("open");
    let desc = ftr.section(kind::FIELD_DIR).expect("FIELD_DIR");
    let raw = read_section(object, desc, &RlogConfig::default()).expect("read FIELD_DIR");
    FieldDir::decode(&raw, u64::MAX).expect("decode FIELD_DIR")
}

fn str_column(dir: &FieldDir, name: &str) -> u32 {
    dir.column(name, FieldType::Str).expect(name).column_id
}

fn bloom_raw_of(object: &[u8]) -> Vec<u8> {
    let ftr = open(object).expect("open");
    let desc = ftr.section(kind::BLOOM).expect("BLOOM");
    read_section(object, desc, &RlogConfig::default()).expect("read BLOOM")
}

/// `object` with its BLOOM section replaced by `bloom`, appended after the
/// last section and pointed at by a rewritten footer. Readers locate sections
/// only through the footer, so the old BLOOM bytes become unread padding.
fn with_bloom(object: &[u8], bloom: Vec<u8>) -> Vec<u8> {
    let n = object.len();
    let footer_len = u32::from_le_bytes(object[n - 16..n - 12].try_into().expect("4 bytes"));
    let mut out = object[..n - 16 - footer_len as usize].to_vec();
    let mut ftr = open(object).expect("open");
    let desc = ftr
        .sections
        .iter_mut()
        .find(|s| s.kind == kind::BLOOM)
        .expect("BLOOM");
    desc.offset = out.len() as u64;
    desc.len = bloom.len() as u64;
    desc.uncomp_len = bloom.len() as u64;
    desc.crc32c = crc32c::crc32c(&bloom);
    out.extend_from_slice(&bloom);
    write_footer_and_trailer(&mut out, &ftr);
    out
}

fn has_word(field: FieldSel, word: &str) -> Predicate {
    Predicate::HasWord {
        field,
        word: word.to_string(),
    }
}

fn equals(field: FieldSel, value: &str) -> Predicate {
    Predicate::Equals {
        field,
        value: AttrValue::Str(value.to_string()),
    }
}

fn bodies(rows: &[LogRecord]) -> Vec<&str> {
    rows.iter().map(|r| r.body.as_str()).collect()
}

/// Scans `object` for `pred` and asserts the single block reached the exact
/// scan with a parsed, undegraded BLOOM section.
fn scan_unpruned(object: &[u8], pred: &Predicate) -> Vec<LogRecord> {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(object, &cfg).expect("reader");
    let (rows, stats) = reader.scan(pred).expect("scan");
    assert_eq!(stats.blocks_total, 1, "{pred:?}");
    assert!(!stats.bloom_degraded, "{pred:?}");
    assert_eq!(stats.blocks_after_postings, 1, "{pred:?}");
    assert_eq!(
        stats.blocks_after_bloom, 1,
        "{pred:?}: an uncovered column must not prune the block"
    );
    assert_eq!(stats.blocks_scanned, 1, "{pred:?}");
    rows
}

/// Asserts that `pred`, on a covered column and absent from the block, prunes
/// it: the object's bloom is live, so a survival elsewhere is about coverage.
fn assert_pruned(object: &[u8], pred: &Predicate) {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(object, &cfg).expect("reader");
    let (rows, stats) = reader.scan(pred).expect("scan");
    assert!(rows.is_empty(), "{pred:?}");
    assert!(!stats.bloom_degraded, "{pred:?}");
    assert_eq!(stats.blocks_after_postings, 1, "{pred:?}");
    assert_eq!(stats.blocks_after_bloom, 0, "{pred:?}");
}

/// A word predicate on a column BLOOM does not cover builds no bloom arm: the
/// block survives bloom pruning untouched and the exact scan decides row by
/// row. The block holds a matching and a non-matching row, so pruning on the
/// uncovered column (its filter holds no token for it) loses the match, and
/// treating an uncovered column as matching without the exact scan returns the
/// other row too.
#[test]
fn bloom_arm_skips_uncovered_column() {
    let object = object_without_note_coverage();
    let dir = field_dir_of(&object);
    let bloom_raw = bloom_raw_of(&object);
    let section = RlogBloomSection::parse(&bloom_raw, &dir).expect("parse BLOOM");
    assert_eq!(
        section.covered(),
        [COL_SEVERITY_TEXT, COL_BODY, str_column(&dir, "region")].as_slice()
    );
    assert!(!section.covers(str_column(&dir, "note")));

    let rows = scan_unpruned(&object, &has_word(FieldSel::Attr("note".into()), "needle"));
    assert_eq!(bodies(&rows), ["request alpha"]);
    assert_pruned(&object, &has_word(FieldSel::Attr("region".into()), "east"));
}

/// The Equals arm honours coverage the same way HasWord does. The value is a
/// short string on an uncovered `Str` attribute, the one shape the Equals arm
/// would otherwise probe the filter for, and no filter holds it.
#[test]
fn bloom_equals_arm_skips_uncovered_column() {
    let object = object_without_note_coverage();
    let value = "needle in here";
    assert!(value.len() <= 64);

    let rows = scan_unpruned(&object, &equals(FieldSel::Attr("note".into()), value));
    assert_eq!(bodies(&rows), ["request alpha"]);
    assert_pruned(&object, &equals(FieldSel::Attr("region".into()), "east"));
}

/// `body` and `severity_text` are covered because the writer lists them, not
/// because they are fixed columns. A section that lists only `region`, with a
/// filter holding only `region`'s keys, must not prune on either of them.
#[test]
fn body_and_severity_text_are_covered_only_when_listed() {
    let written = object_without_note_coverage();
    let region = str_column(&field_dir_of(&written), "region");
    let mut builder = BloomBuilder::new(RlogConfig::default().bloom_seed);
    builder.insert(region, b"west");
    let object = with_bloom(
        &written,
        encode_rlog_bloom_section(&[region], &[builder.finish_exact()]),
    );
    let section = bloom_raw_of(&object);
    let parsed = RlogBloomSection::parse(&section, &field_dir_of(&object)).expect("parse");
    assert_eq!(parsed.covered(), [region].as_slice());

    let rows = scan_unpruned(&object, &has_word(FieldSel::Body, "alpha"));
    assert_eq!(bodies(&rows), ["request alpha"]);
    let rows = scan_unpruned(&object, &equals(FieldSel::Body, "request beta"));
    assert_eq!(bodies(&rows), ["request beta"]);
    let rows = scan_unpruned(&object, &has_word(FieldSel::SeverityText, "INFO"));
    assert_eq!(bodies(&rows), ["request alpha", "request beta"]);
    let rows = scan_unpruned(&object, &equals(FieldSel::SeverityText, "INFO"));
    assert_eq!(bodies(&rows), ["request alpha", "request beta"]);
    assert_pruned(&object, &has_word(FieldSel::Attr("region".into()), "east"));
}

/// One flipped byte in the covered list, with every entry left intact, turns
/// `region` into `note`: an ascending list of real columns that every
/// structural check accepts. The list crc refuses it as `Corrupted`, so the
/// scan degrades to no bloom pruning and returns the match. Accepting the list
/// would probe `note` in a filter that holds no key for it and drop the row.
#[test]
fn flipped_coverage_list_is_refused_and_prunes_nothing() {
    let mut object = object_without_note_coverage();
    let dir = field_dir_of(&object);
    let (note, region) = (str_column(&dir, "note"), str_column(&dir, "region"));
    let original = bloom_raw_of(&object);
    let intact = RlogBloomSection::parse(&original, &dir).expect("parse");
    assert_eq!(
        intact.covered(),
        [COL_SEVERITY_TEXT, COL_BODY, region].as_slice()
    );
    intact.entry(0).expect("the entry set is valid");

    // BLOOM is stored uncompressed: `covered_count` (4 bytes), then one varint
    // byte per id below 128, so `region` is the section's seventh byte.
    let bloom = open(&object)
        .expect("open")
        .section(kind::BLOOM)
        .copied()
        .expect("BLOOM");
    let at = bloom.offset as usize + 4 + 2;
    assert!(note < 128 && region < 128);
    assert_eq!(object[at], region as u8);
    object[at] = note as u8;

    let cfg = RlogConfig::default();
    let reader = RlogReader::new(&object, &cfg).expect("reader");
    let (rows, stats) = reader
        .scan(&has_word(FieldSel::Attr("note".into()), "needle"))
        .expect("scan");
    assert_eq!(bodies(&rows), ["request alpha"]);
    assert!(stats.bloom_degraded, "the flipped list must be refused");
    assert_eq!(stats.blocks_after_postings, 1);
    assert_eq!(stats.blocks_after_bloom, 1);

    // A covered word absent from the block prunes nothing either: the whole
    // section is refused, not only the flipped id.
    let (rows, stats) = reader
        .scan(&has_word(FieldSel::Attr("region".into()), "east"))
        .expect("scan");
    assert!(rows.is_empty());
    assert!(stats.bloom_degraded);
    assert_eq!(stats.blocks_after_bloom, 1);

    // The stored bytes, sliced as the scan path does: `read_section` would
    // check the footer's whole-section crc, which the scan never consults.
    let stored = &object[bloom.offset as usize..(bloom.offset + bloom.len) as usize];
    match RlogBloomSection::parse(stored, &dir) {
        Err(LogSegError::Corrupted(m)) => {
            assert_eq!(m, "bloom covered column list crc mismatch");
        }
        Err(other) => panic!("expected Corrupted, got {other:?}"),
        Ok(s) => panic!("flipped list parsed as {:?}", s.covered()),
    }
}
