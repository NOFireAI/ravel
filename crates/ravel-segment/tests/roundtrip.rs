//! Integration tests for RSEG v1 write/read identity, corruption handling,
//! the suffix-slice reader protocol, and cross-series page binding.
#![allow(clippy::expect_used)]

use std::collections::HashMap;

use proptest::array::uniform16;
use proptest::prelude::*;

use ravel_segment::{
    FooterOutcome, IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesEntry,
    SeriesInput,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

/// A series' samples reduced to exact bit-pattern comparison keys.
type NormalizedSamples = Vec<(i64, u64)>;
/// One normalized decoded series: raw id bytes, labels, bit-exact samples.
type NormalizedSeries = ([u8; 16], LabelSet, NormalizedSamples);
/// Expected per-series outcome keyed by raw series_id bytes, for comparing
/// against decoded output in the proptest below.
type ExpectedBySeriesId = HashMap<[u8; 16], (LabelSet, NormalizedSamples)>;

/// Slices `bytes` at an absolute `(offset, len)` range.
fn slice_range(bytes: &[u8], range: (u64, u64)) -> &[u8] {
    let start = range.0 as usize;
    let end = start + range.1 as usize;
    &bytes[start..end]
}

/// Locates a section's stored bytes by kind (1=LABEL_DICT, 2=SERIES_TABLE,
/// 3=TS_PAGES, 4=VAL_PAGES) from the full object bytes and footer.
fn section_bytes<'a>(bytes: &'a [u8], footer: &ravel_segment::Footer, kind: u32) -> &'a [u8] {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    slice_range(bytes, (section.offset, section.len))
}

/// Runs the full reader pipeline (footer -> catalog -> plan -> decode) over
/// bytes already known to be a complete object, returning every series'
/// identity, labels, and samples in on-disk order.
fn full_decode(
    bytes: &[u8],
) -> Result<Vec<(SeriesId, LabelSet, Vec<Sample>)>, ravel_segment::SegmentError> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits)?;
    let label_dict_bytes = section_bytes(bytes, &loc.footer, 1);
    let series_table_bytes = section_bytes(bytes, &loc.footer, 2);
    let entries =
        ravel_segment::decode_catalog(&loc.footer, label_dict_bytes, series_table_bytes, limits)?;

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges(&loc.footer, &selected)?;

    let mut out = Vec::with_capacity(entries.len());
    for (entry, range) in entries.iter().zip(ranges.iter()) {
        let ts_bytes = slice_range(bytes, range.ts_range);
        let val_bytes = slice_range(bytes, range.val_range);
        let samples = ravel_segment::decode_pages(entry, ts_bytes, val_bytes, limits)?;
        out.push((entry.series_id, entry.labels.clone(), samples));
    }
    Ok(out)
}

/// Normalizes decoded output for exact (including NaN payload and -0.0)
/// comparison: values compare by bit pattern, entries sorted by series_id.
fn normalize(entries: &[(SeriesId, LabelSet, Vec<Sample>)]) -> Vec<NormalizedSeries> {
    let mut out: Vec<_> = entries
        .iter()
        .map(|(id, labels, samples)| {
            (
                id.0,
                labels.clone(),
                samples
                    .iter()
                    .map(|s| (s.ts_ns, s.value.to_bits()))
                    .collect(),
            )
        })
        .collect();
    out.sort_by_key(|(id, _, _)| *id);
    out
}

fn labels(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::new(
        pairs
            .iter()
            .map(|(n, v)| Label {
                name: (*n).to_string(),
                value: (*v).to_string(),
            })
            .collect(),
    )
    .expect("valid labels")
}

/// A tiny, hand-picked 2-series segment used by the corruption, suffix, and
/// cross-series tests: irregular/duplicate timestamps and NaN/-0.0/Inf
/// values, small enough to byte-flip exhaustively.
fn build_small_segment() -> bytes::Bytes {
    let series = vec![
        SeriesInput {
            series_id: SeriesId([1u8; 16]),
            labels: labels(&[
                (METRIC_NAME_LABEL, "http_requests_total"),
                ("instance", "a"),
            ]),
            samples: vec![
                Sample {
                    ts_ns: 100,
                    value: 1.0,
                },
                Sample {
                    ts_ns: 50,
                    value: 2.0,
                },
                Sample {
                    ts_ns: 50,
                    value: 3.0,
                },
            ],
        },
        SeriesInput {
            series_id: SeriesId([2u8; 16]),
            labels: labels(&[
                (METRIC_NAME_LABEL, "http_requests_total"),
                ("instance", "b"),
            ]),
            samples: vec![
                Sample {
                    ts_ns: -10,
                    value: f64::NAN,
                },
                Sample {
                    ts_ns: 0,
                    value: -0.0,
                },
                Sample {
                    ts_ns: 20,
                    value: f64::INFINITY,
                },
            ],
        },
    ];
    let identity = SegmentIdentity {
        tenant_hash: [9u8; 16],
        shard: 1,
        writer_id: "writer-x".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 1000,
    };
    SegmentWriter::write(series, identity, bounds)
        .expect("writes")
        .bytes
}

#[test]
fn small_segment_roundtrips() {
    let bytes = build_small_segment();
    let decoded = full_decode(&bytes).expect("decodes");
    assert_eq!(decoded.len(), 2);
    let normalized = normalize(&decoded);
    assert_eq!(normalized[0].0, [1u8; 16]);
    assert_eq!(
        normalized[0].2,
        vec![
            (50, 2.0f64.to_bits()),
            (50, 3.0f64.to_bits()),
            (100, 1.0f64.to_bits())
        ]
    );
    assert_eq!(normalized[1].0, [2u8; 16]);
    assert_eq!(
        normalized[1].2,
        vec![
            (-10, f64::NAN.to_bits()),
            (0, (-0.0f64).to_bits()),
            (20, f64::INFINITY.to_bits())
        ]
    );
}

#[test]
fn corruption_every_byte_flip_errors_or_matches() {
    let bytes = build_small_segment();
    let original = normalize(&full_decode(&bytes).expect("baseline decodes"));

    let mut unflagged_matches = 0usize;
    for offset in 0..bytes.len() {
        let mut flipped = bytes.to_vec();
        flipped[offset] ^= 0xFF;
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| full_decode(&flipped)));
        match result {
            Err(_) => panic!("byte flip at offset {offset} caused a panic"),
            Ok(Err(_)) => {} // corruption correctly detected: fail closed
            Ok(Ok(decoded)) => {
                assert_eq!(
                    normalize(&decoded),
                    original,
                    "byte flip at offset {offset} produced different output without erroring"
                );
                unflagged_matches += 1;
            }
        }
    }
    // Every byte in this layout is either checksum-covered (page crc,
    // section crc32c, or footer_crc32c) or is itself a checksum field, so
    // every single-byte flip is expected to error; an unflagged match would
    // mean an uncovered byte exists and deserves scrutiny, not silent
    // acceptance. Verified empirically: 0 of len(bytes) flips pass silently.
    assert_eq!(
        unflagged_matches, 0,
        "expected every byte flip to be caught by a checksum; found an uncovered byte"
    );
}

#[test]
fn suffix_slice_path_and_range_planning() {
    let bytes = build_small_segment();
    let total_size = bytes.len() as u64;
    let limits = ReaderLimits::default();

    let full_loc = ravel_segment::open_from_full(&bytes, limits).expect("full parses");

    // A real 64 KiB suffix GET: our tiny test segment is fully contained.
    let suffix_len = (64 * 1024).min(bytes.len());
    let suffix = &bytes[bytes.len() - suffix_len..];
    match ravel_segment::open_from_suffix(suffix, total_size, limits).expect("suffix parses") {
        FooterOutcome::Ready(loc) => assert_eq!(loc.footer, full_loc.footer),
        FooterOutcome::NeedRange { .. } => {
            panic!("64 KiB suffix should already contain the footer")
        }
    }

    // Force the NeedRange path with a deliberately undersized suffix
    // (smaller than the 16-byte trailer) and chase the exact byte ranges it
    // names to a successful parse.
    let tiny = &bytes[bytes.len() - 8..];
    let (offset1, len1) =
        match ravel_segment::open_from_suffix(tiny, total_size, limits).expect("no error") {
            FooterOutcome::NeedRange { offset, len } => (offset, len),
            FooterOutcome::Ready(_) => panic!("8 bytes cannot contain the 16-byte trailer"),
        };
    assert_eq!(offset1, total_size - 16);
    assert_eq!(len1, 16);

    let fetched1 = &bytes[offset1 as usize..(offset1 + len1) as usize];
    let (offset2, len2) =
        match ravel_segment::open_from_suffix(fetched1, total_size, limits).expect("no error") {
            FooterOutcome::NeedRange { offset, len } => (offset, len),
            FooterOutcome::Ready(_) => panic!("trailer alone cannot contain a non-empty footer"),
        };
    assert_eq!(offset2, full_loc.footer_offset);
    assert_eq!(offset2 + len2, total_size);

    let fetched2 = &bytes[offset2 as usize..(offset2 + len2) as usize];
    match ravel_segment::open_from_suffix(fetched2, total_size, limits).expect("no error") {
        FooterOutcome::Ready(loc) => assert_eq!(loc.footer, full_loc.footer),
        FooterOutcome::NeedRange { .. } => panic!("footer+trailer range must be sufficient"),
    }
}

#[test]
fn decode_pages_rejects_page_series_id_mismatch() {
    let bytes = build_small_segment();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&bytes, limits).expect("opens");
    let label_dict_bytes = section_bytes(&bytes, &loc.footer, 1);
    let series_table_bytes = section_bytes(&bytes, &loc.footer, 2);
    let entries =
        ravel_segment::decode_catalog(&loc.footer, label_dict_bytes, series_table_bytes, limits)
            .expect("catalog");
    assert_eq!(entries.len(), 2);
    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges(&loc.footer, &selected).expect("ranges");

    let ts_a = slice_range(&bytes, ranges[0].ts_range);
    let val_a = slice_range(&bytes, ranges[0].val_range);
    let ts_b = slice_range(&bytes, ranges[1].ts_range);
    let val_b = slice_range(&bytes, ranges[1].val_range);

    // The page crc is seeded with the owning series_id (ADR-0010 §4): pages
    // decoded against the wrong entry must fail closed, not silently
    // misdecode.
    assert!(matches!(
        ravel_segment::decode_pages(&entries[0], ts_b, val_a, limits),
        Err(ravel_segment::SegmentError::PageCrcMismatch)
    ));
    assert!(matches!(
        ravel_segment::decode_pages(&entries[0], ts_a, val_b, limits),
        Err(ravel_segment::SegmentError::PageCrcMismatch)
    ));
    // Sanity: the correctly paired pages still decode.
    assert!(ravel_segment::decode_pages(&entries[0], ts_a, val_a, limits).is_ok());
    assert!(ravel_segment::decode_pages(&entries[1], ts_b, val_b, limits).is_ok());
}

#[test]
fn select_filters_by_equality_and_predicate_then_plans_a_subset() {
    let bytes = build_small_segment();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&bytes, limits).expect("opens");
    let label_dict_bytes = section_bytes(&bytes, &loc.footer, 1);
    let series_table_bytes = section_bytes(&bytes, &loc.footer, 2);
    let entries =
        ravel_segment::decode_catalog(&loc.footer, label_dict_bytes, series_table_bytes, limits)
            .expect("catalog");

    let by_equals = ravel_segment::select(&entries, &[("instance", "a")], None);
    assert_eq!(by_equals.len(), 1);
    assert_eq!(by_equals[0].series_id, SeriesId([1u8; 16]));

    let no_match = ravel_segment::select(&entries, &[("instance", "does-not-exist")], None);
    assert!(no_match.is_empty());

    let by_predicate = ravel_segment::select(
        &entries,
        &[],
        Some(&|ls: &LabelSet| ls.get("instance") == Some("b")),
    );
    assert_eq!(by_predicate.len(), 1);
    assert_eq!(by_predicate[0].series_id, SeriesId([2u8; 16]));

    let both = ravel_segment::select(
        &entries,
        &[(METRIC_NAME_LABEL, "http_requests_total")],
        Some(&|ls: &LabelSet| ls.get("instance") == Some("b")),
    );
    assert_eq!(both.len(), 1);
    assert_eq!(both[0].series_id, SeriesId([2u8; 16]));

    // Wire a genuine single-series (non-first) subset through plan_ranges and
    // decode_pages: proves plan_ranges works on a real subset, not just "all
    // entries, in order" (which a zip-based bug could pass trivially).
    let ts_section = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == 3)
        .expect("ts section present");
    let val_section = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == 4)
        .expect("val section present");
    let entry_b = by_predicate[0];
    let ranges = ravel_segment::plan_ranges(&loc.footer, &by_predicate).expect("ranges");
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].series_id, entry_b.series_id);
    assert_eq!(
        ranges[0].ts_range,
        (ts_section.offset + entry_b.ts_page.0, entry_b.ts_page.1)
    );
    assert_eq!(
        ranges[0].val_range,
        (val_section.offset + entry_b.val_page.0, entry_b.val_page.1)
    );

    let ts_bytes = slice_range(&bytes, ranges[0].ts_range);
    let val_bytes = slice_range(&bytes, ranges[0].val_range);
    let samples =
        ravel_segment::decode_pages(entry_b, ts_bytes, val_bytes, limits).expect("decodes");
    let got: Vec<(i64, u64)> = samples
        .iter()
        .map(|s| (s.ts_ns, s.value.to_bits()))
        .collect();
    assert_eq!(
        got,
        vec![
            (-10, f64::NAN.to_bits()),
            (0, (-0.0f64).to_bits()),
            (20, f64::INFINITY.to_bits()),
        ]
    );
}

#[test]
fn identity_check_against_real_written_footer() {
    let tenant_hash = [7u8; 16];
    let shard = 3u32;
    let writer_id = "writer-real".to_string();
    let writer_epoch = 42u64;
    let writer_seq = 9u64;

    let series = vec![SeriesInput {
        series_id: SeriesId([1u8; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "m")]),
        samples: vec![Sample {
            ts_ns: 1,
            value: 1.0,
        }],
    }];
    let identity = SegmentIdentity {
        tenant_hash,
        shard,
        writer_id: writer_id.clone(),
        writer_epoch,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 1,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("writes");

    let loc =
        ravel_segment::open_from_full(&written.bytes, ReaderLimits::default()).expect("opens");

    let expected = ravel_segment::ExpectedIdentity {
        tenant_hash,
        shard,
        writer_id: writer_id.clone(),
        writer_epoch,
        writer_seq,
    };
    assert_eq!(
        ravel_segment::check_identity(&loc.footer, &expected),
        Ok(())
    );

    let mismatched = ravel_segment::ExpectedIdentity {
        tenant_hash,
        shard: shard + 1,
        writer_id,
        writer_epoch,
        writer_seq,
    };
    assert_eq!(
        ravel_segment::check_identity(&loc.footer, &mismatched),
        Err(ravel_segment::SegmentError::IdentityMismatch("shard"))
    );
}

// --- proptest: write/read identity over arbitrary inputs ---

fn label_name_strategy() -> impl Strategy<Value = String> {
    "[a-z]{1,6}"
}

fn label_value_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_]{0,8}"
}

fn metric_name_strategy() -> impl Strategy<Value = String> {
    "[a-z_]{1,10}"
}

fn labelset_strategy() -> impl Strategy<Value = LabelSet> {
    (
        metric_name_strategy(),
        prop::collection::vec((label_name_strategy(), label_value_strategy()), 0..5),
    )
        .prop_map(|(metric_name, extra)| {
            let mut ls = vec![Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: metric_name,
            }];
            let mut seen = std::collections::HashSet::new();
            seen.insert(METRIC_NAME_LABEL.to_string());
            for (name, value) in extra {
                if seen.insert(name.clone()) {
                    ls.push(Label { name, value });
                }
            }
            LabelSet::new(ls).expect("no duplicate names by construction")
        })
}

fn ts_strategy() -> impl Strategy<Value = i64> {
    prop_oneof![
        3 => 0i64..300,
        1 => -1_000_000_000_000i64..=1_000_000_000_000i64,
    ]
}

fn sample_value_strategy() -> impl Strategy<Value = f64> {
    prop_oneof![
        10 => any::<f64>(),
        1 => Just(f64::NAN),
        1 => Just(-f64::NAN),
        1 => Just(f64::INFINITY),
        1 => Just(f64::NEG_INFINITY),
        1 => Just(0.0f64),
        1 => Just(-0.0f64),
        1 => Just(f64::MIN_POSITIVE),
        1 => Just(5e-324f64),
        1 => Just(-5e-324f64),
    ]
}

fn samples_strategy() -> impl Strategy<Value = Vec<(i64, f64)>> {
    prop::collection::vec((ts_strategy(), sample_value_strategy()), 1..12)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn write_read_roundtrip_is_identity(
        series_data in prop::collection::vec((labelset_strategy(), samples_strategy()), 0..6),
        tenant_hash in uniform16(any::<u8>()),
        shard in any::<u32>(),
        writer_id in "[a-zA-Z0-9-]{1,20}",
        epoch in any::<u64>(),
        seq in any::<u64>(),
        min_ingest in -1_000_000_000_000i64..=1_000_000_000_000i64,
        ingest_span in 0i64..=1_000_000_000i64,
    ) {
        let max_ingest = min_ingest.saturating_add(ingest_span);

        let mut series_inputs = Vec::new();
        let mut expected: ExpectedBySeriesId = HashMap::new();
        let mut expected_sample_count: u64 = 0;
        let mut expected_min_event_ts = i64::MAX;
        let mut expected_max_event_ts = i64::MIN;

        for (idx, (series_labels, samples)) in series_data.into_iter().enumerate() {
            let mut id_bytes = [0u8; 16];
            id_bytes[..8].copy_from_slice(&(idx as u64).to_be_bytes());

            // Mirror the writer's contract: a stable sort by ts_ns keeps
            // ties in original insertion order, equivalent to sorting the
            // pair (ts, original_index).
            let mut indexed: Vec<(usize, (i64, f64))> =
                samples.iter().copied().enumerate().collect();
            indexed.sort_by_key(|(i, (ts, _))| (*ts, *i));
            let expected_samples: Vec<(i64, u64)> = indexed
                .into_iter()
                .map(|(_, (ts, v))| (ts, v.to_bits()))
                .collect();

            if !samples.is_empty() {
                expected_sample_count += expected_samples.len() as u64;
                if let Some(&(min_ts, _)) = expected_samples.first() {
                    expected_min_event_ts = expected_min_event_ts.min(min_ts);
                }
                if let Some(&(max_ts, _)) = expected_samples.last() {
                    expected_max_event_ts = expected_max_event_ts.max(max_ts);
                }
                expected.insert(id_bytes, (series_labels.clone(), expected_samples));
            }

            series_inputs.push(SeriesInput {
                series_id: SeriesId(id_bytes),
                labels: series_labels,
                samples: samples
                    .into_iter()
                    .map(|(ts_ns, value)| Sample { ts_ns, value })
                    .collect(),
            });
        }
        if expected.is_empty() {
            expected_min_event_ts = 0;
            expected_max_event_ts = 0;
        }

        let identity = SegmentIdentity {
            tenant_hash,
            shard,
            writer_id,
            writer_epoch: epoch,
            writer_seq: seq,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: min_ingest,
            max_ingest_ts_ns: max_ingest,
        };

        let expected_series_count = expected.len();
        let written = SegmentWriter::write(series_inputs, identity, bounds).expect("write succeeds");
        prop_assert_eq!(written.summary.series_count as usize, expected_series_count);
        prop_assert_eq!(written.summary.sample_count, expected_sample_count);
        prop_assert_eq!(written.summary.min_event_ts_ns, expected_min_event_ts);
        prop_assert_eq!(written.summary.max_event_ts_ns, expected_max_event_ts);
        prop_assert_eq!(
            written.summary.blake3,
            *blake3::hash(&written.bytes).as_bytes()
        );

        // The footer itself (sfixed64 fields, protobuf round-trip) must
        // agree with the summary, including negative event timestamps.
        let loc = ravel_segment::open_from_full(&written.bytes, ReaderLimits::default())
            .expect("footer parses");
        prop_assert_eq!(loc.footer.min_event_ts_ns, expected_min_event_ts);
        prop_assert_eq!(loc.footer.max_event_ts_ns, expected_max_event_ts);
        prop_assert_eq!(loc.footer.sample_count, expected_sample_count);
        prop_assert_eq!(loc.footer.series_count as usize, expected_series_count);

        let decoded = full_decode(&written.bytes).expect("decode succeeds");
        prop_assert_eq!(decoded.len(), expected_series_count);

        for (series_id, series_labels, samples) in &decoded {
            let (exp_labels, exp_samples) = expected
                .get(&series_id.0)
                .expect("decoded series must be one we wrote");
            prop_assert_eq!(series_labels, exp_labels);
            let got: Vec<(i64, u64)> =
                samples.iter().map(|s| (s.ts_ns, s.value.to_bits())).collect();
            prop_assert_eq!(&got, exp_samples);
        }
    }
}
