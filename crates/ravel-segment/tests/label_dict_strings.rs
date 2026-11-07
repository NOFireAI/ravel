//! `label_dict_strings` (issue #3863): the LABEL_DICT section alone yields
//! exactly the label names and values the writer was given, on both catalog
//! layouts, and every corrupt or truncated input is a typed error.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;

use proptest::prelude::*;
use ravel_proto::segment::v1::Footer;
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentError, SegmentIdentity, SegmentWriter, SeriesInput,
    V5_SPARSE_THRESHOLD, label_dict_strings, open_from_full,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};

const LABEL_DICT: u32 = 1;
const SERIES_META_CHUNKS: u32 = 9;
const COMPRESSION_NONE: i32 = 0;

fn build(labels: &[Vec<(String, String)>]) -> Vec<u8> {
    let tenant = TenantId::new("label-dict-strings".to_string());
    let base = 1_700_000_000_000_000_000i64;
    let inputs = labels
        .iter()
        .enumerate()
        .map(|(i, pairs)| {
            let ls = LabelSet::new(
                pairs
                    .iter()
                    .map(|(name, value)| Label {
                        name: name.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            )
            .expect("valid labels");
            let metric = ls.get(METRIC_NAME_LABEL).expect("metric name").to_string();
            SeriesInput {
                series_id: SeriesId::compute(&tenant, &metric, &ls).expect("series id"),
                labels: ls,
                samples: vec![Sample {
                    ts_ns: base + i as i64,
                    value: i as f64,
                }],
            }
        })
        .collect();
    let identity = SegmentIdentity {
        tenant_hash: [0x47; 16],
        shard: 0,
        writer_id: "label-dict-strings-test".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    SegmentWriter::write(inputs, identity, bounds)
        .expect("write segment")
        .bytes
        .to_vec()
}

/// The footer and the LABEL_DICT section's stored bytes, owned so a test can
/// corrupt them.
fn dict_of(obj: &[u8]) -> (Footer, Vec<u8>) {
    let footer = open_from_full(obj, ReaderLimits::default())
        .expect("open")
        .footer;
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == LABEL_DICT)
        .expect("LABEL_DICT present");
    let stored = obj[s.offset as usize..(s.offset + s.len) as usize].to_vec();
    (footer, stored)
}

fn given_strings(labels: &[Vec<(String, String)>]) -> BTreeSet<String> {
    labels
        .iter()
        .flatten()
        .flat_map(|(name, value)| [name.clone(), value.clone()])
        .collect()
}

/// Label sets for `n` series: a shared metric name, a unique `inst`, and
/// `zone` on even series only.
fn small_fixture(n: usize) -> Vec<Vec<(String, String)>> {
    (0..n)
        .map(|i| {
            let mut pairs = vec![
                (METRIC_NAME_LABEL.to_string(), "http_requests".to_string()),
                ("inst".to_string(), format!("i{i}")),
            ];
            if i % 2 == 0 {
                pairs.push(("zone".to_string(), "eu-west".to_string()));
            }
            pairs
        })
        .collect()
}

fn dict_section(footer: &mut Footer) -> &mut ravel_proto::segment::v1::Section {
    footer
        .sections
        .iter_mut()
        .find(|s| s.kind == LABEL_DICT)
        .expect("LABEL_DICT present")
}

/// Replaces the section with `raw` stored uncompressed, crc and lengths
/// matching, so the dictionary grammar itself is what the decode meets.
fn with_raw_dict(footer: &Footer, raw: &[u8]) -> (Footer, Vec<u8>) {
    let mut footer = footer.clone();
    let s = dict_section(&mut footer);
    s.comp = COMPRESSION_NONE;
    s.len = raw.len() as u64;
    s.uncompressed_len = raw.len() as u64;
    s.crc32c = crc32c::crc32c(raw);
    (footer, raw.to_vec())
}

/// A raw LABEL_DICT payload: u32le count, then uvarint length and bytes per
/// string (every length here is below 128, one varint byte).
fn raw_dict(count: u32, strings: &[&[u8]]) -> Vec<u8> {
    let mut out = count.to_le_bytes().to_vec();
    for s in strings {
        out.push(s.len() as u8);
        out.extend_from_slice(s);
    }
    out
}

#[test]
fn below_threshold_object_returns_every_name_and_value() {
    let labels = small_fixture(5);
    let obj = build(&labels);
    let (footer, stored) = dict_of(&obj);
    assert!(
        !footer.sections.iter().any(|s| s.kind == SERIES_META_CHUNKS),
        "the fixture must take the below-threshold layout"
    );
    let got: BTreeSet<String> = label_dict_strings(&footer, &stored, ReaderLimits::default())
        .expect("decode")
        .into_iter()
        .collect();
    assert_eq!(got, given_strings(&labels));
}

#[test]
fn chunked_object_returns_every_name_and_value() {
    let labels = small_fixture(V5_SPARSE_THRESHOLD as usize);
    let obj = build(&labels);
    let (footer, stored) = dict_of(&obj);
    assert!(
        footer.sections.iter().any(|s| s.kind == SERIES_META_CHUNKS),
        "the fixture must take the chunked catalog layout"
    );
    let got = label_dict_strings(&footer, &stored, ReaderLimits::default()).expect("decode");
    let unique: BTreeSet<String> = got.iter().cloned().collect();
    assert_eq!(unique.len(), got.len(), "no string is returned twice");
    assert_eq!(unique, given_strings(&labels));
}

#[test]
fn corrupt_and_truncated_inputs_are_typed_errors() {
    let obj = build(&small_fixture(5));
    let (footer, stored) = dict_of(&obj);
    let decode = |footer: &Footer, stored: &[u8]| {
        label_dict_strings(footer, stored, ReaderLimits::default())
    };

    // A flipped byte fails the section crc.
    let mut flipped = stored.clone();
    flipped[0] ^= 0x01;
    assert_eq!(
        decode(&footer, &flipped).err(),
        Some(SegmentError::SectionCrcMismatch)
    );

    // Stored bytes shorter than the footer's section length.
    assert_eq!(
        decode(&footer, &stored[..stored.len() - 1]).err(),
        Some(SegmentError::SectionOutOfBounds)
    );

    // Compressed bytes that do not inflate, crc fixed to match.
    let mut garbage_footer = footer.clone();
    let garbage = vec![0xFFu8; stored.len()];
    dict_section(&mut garbage_footer).crc32c = crc32c::crc32c(&garbage);
    if dict_section(&mut garbage_footer).comp != COMPRESSION_NONE {
        let got = decode(&garbage_footer, &garbage);
        assert!(
            matches!(
                got,
                Err(SegmentError::Decompress(_))
                    | Err(SegmentError::DecompressedLenMismatch { .. })
                    | Err(SegmentError::SectionTooLarge { .. })
                    | Err(SegmentError::Truncated)
            ),
            "{got:?}"
        );
    }

    // A decompressed size above the reader's bound is refused before inflating.
    let limits = ReaderLimits {
        max_section_uncompressed_bytes: 4,
        ..ReaderLimits::default()
    };
    assert!(matches!(
        label_dict_strings(&footer, &stored, limits),
        Err(SegmentError::SectionTooLarge { cap: 4, .. })
    ));

    // The dictionary grammar: a count naming more strings than are stored, a
    // string past the end, a non-UTF-8 string, a repeated string, and bytes
    // after the last string.
    let cases: [(Vec<u8>, SegmentError); 6] = [
        (vec![0x01, 0x00], SegmentError::Truncated),
        (raw_dict(2, &[b"a"]), SegmentError::Truncated),
        (
            {
                let mut d = raw_dict(1, &[]);
                d.extend_from_slice(&[5, b'a']);
                d
            },
            SegmentError::Truncated,
        ),
        (raw_dict(1, &[&[0xFF, 0xFE]]), SegmentError::BadUtf8),
        (
            raw_dict(2, &[b"a", b"a"]),
            SegmentError::DuplicateDictString,
        ),
        (
            {
                let mut d = raw_dict(1, &[b"a"]);
                d.push(0);
                d
            },
            SegmentError::TrailingBytes,
        ),
    ];
    for (raw, want) in cases {
        let (f, s) = with_raw_dict(&footer, &raw);
        assert_eq!(decode(&f, &s).err(), Some(want), "raw {raw:?}");
    }

    // The same crafted path decodes a well-formed payload, so the cases above
    // fail on their defect and not on the crafting.
    let (f, s) = with_raw_dict(&footer, &raw_dict(2, &[b"a", "\u{e9}".as_bytes()]));
    assert_eq!(
        decode(&f, &s).expect("well-formed raw dictionary"),
        vec!["a".to_string(), "\u{e9}".to_string()]
    );

    // No LABEL_DICT section in the footer is an error, not an empty list.
    let mut missing = footer.clone();
    missing.sections.retain(|s| s.kind != LABEL_DICT);
    assert_eq!(
        decode(&missing, &stored).err(),
        Some(SegmentError::MissingSection("LABEL_DICT"))
    );
}

fn labels_strategy() -> impl Strategy<Value = Vec<Vec<(String, String)>>> {
    let pair = ("[a-z][a-z0-9_]{0,5}", "\\PC{1,6}");
    prop::collection::vec(
        ("[a-z_][a-z0-9_]{0,5}", prop::collection::vec(pair, 0..4)),
        1..12,
    )
    .prop_map(|series| {
        series
            .into_iter()
            .enumerate()
            .map(|(i, (metric, extra))| {
                let mut pairs = vec![
                    (METRIC_NAME_LABEL.to_string(), metric),
                    ("inst".to_string(), format!("i{i}")),
                ];
                for (name, value) in extra {
                    if !pairs.iter().any(|(n, _)| *n == name) {
                        pairs.push((name, value));
                    }
                }
                pairs
            })
            .collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The returned strings are exactly the names and values given to the
    /// writer, each once.
    #[test]
    fn returns_exactly_the_written_names_and_values(labels in labels_strategy()) {
        let obj = build(&labels);
        let (footer, stored) = dict_of(&obj);
        let got = label_dict_strings(&footer, &stored, ReaderLimits::default())
            .expect("decode");
        let unique: BTreeSet<String> = got.iter().cloned().collect();
        prop_assert_eq!(unique.len(), got.len());
        prop_assert_eq!(unique, given_strings(&labels));
    }
}
