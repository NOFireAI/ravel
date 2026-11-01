//! `decode_catalog_v5_chunked_matching` (issue #3511): the positive-equality
//! chunked catalog decode returns exactly what the unpruned
//! `decode_catalog_v5_chunked` returns after filtering by the same pairs, and
//! a pair the LABEL_DICT lacks returns before any chunk frame is touched.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use proptest::prelude::*;
use ravel_proto::segment::v1::Footer;
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentError, SegmentIdentity, SegmentWriter, SeriesEntryV4,
    SeriesInput, V5_SPARSE_THRESHOLD, V5_STRIDE, decode_catalog_v5_chunked,
    decode_catalog_v5_chunked_matching, open_from_full,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};

const LABEL_DICT: u32 = 1;
const SERIES_IDS: u32 = 5;
const SERIES_IDX: u32 = 8;
const SERIES_META_CHUNKS: u32 = 9;

/// Optional label names and their value pools. "zone" takes values that are
/// also label names, so a name ordinal and a value ordinal can coincide.
const OPTIONAL: [(&str, &[&str]); 5] = [
    ("a", &["v0", "v1", "v2"]),
    ("b", &["v0", "v1", "v2"]),
    ("c", &["v0", "v1", "v2"]),
    ("job", &["v0", "v1", "v2"]),
    ("zone", &["a", "b", "v1"]),
];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn label_set(pairs: Vec<(String, String)>) -> LabelSet {
    LabelSet::new(
        pairs
            .into_iter()
            .map(|(name, value)| Label { name, value })
            .collect(),
    )
    .expect("valid labels")
}

/// Label sets for `n` series. Series `i` always carries `__name__` and a unique
/// `inst`; each optional name is present at random, except that "a" is
/// present exactly when `i % 3 != 0`, so "b" sits at two different schema
/// positions, and the first six series carry `b="v0"`.
fn series_labels(n: usize, rng: &mut Rng) -> Vec<Vec<(String, String)>> {
    (0..n)
        .map(|i| {
            let mut pairs = vec![
                (METRIC_NAME_LABEL.to_string(), format!("m{}", rng.below(4))),
                ("inst".to_string(), format!("i{i}")),
            ];
            for (name, values) in OPTIONAL {
                let present = match name {
                    "a" => i % 3 != 0,
                    "b" => i < 6 || rng.below(10) < 7,
                    _ => rng.below(10) < 6,
                };
                if present {
                    let value = if name == "b" && i < 6 {
                        "v0"
                    } else {
                        values[rng.below(values.len())]
                    };
                    pairs.push((name.to_string(), value.to_string()));
                }
            }
            pairs
        })
        .collect()
}

fn build_sparse(labels: &[Vec<(String, String)>]) -> Vec<u8> {
    let tenant = TenantId::new("chunked-matching".to_string());
    let base = 1_700_000_000_000_000_000i64;
    let inputs = labels
        .iter()
        .enumerate()
        .map(|(i, pairs)| {
            let ls = label_set(pairs.clone());
            let metric = ls.get(METRIC_NAME_LABEL).expect("metric name").to_string();
            SeriesInput {
                series_id: SeriesId::compute(&tenant, &metric, &ls).expect("series id"),
                labels: ls,
                samples: vec![
                    Sample {
                        ts_ns: base + i as i64,
                        value: i as f64,
                    },
                    Sample {
                        ts_ns: base + i as i64 + 1_000,
                        value: i as f64 + 0.5,
                    },
                ],
            }
        })
        .collect();
    let identity = SegmentIdentity {
        tenant_hash: [0x31; 16],
        shard: 0,
        writer_id: "chunked-matching-test".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    SegmentWriter::write(inputs, identity, bounds)
        .expect("write sparse segment")
        .bytes
        .to_vec()
}

fn section<'a>(obj: &'a [u8], footer: &Footer, kind: u32) -> &'a [u8] {
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .unwrap_or_else(|| panic!("section kind {kind} present"));
    &obj[s.offset as usize..(s.offset + s.len) as usize]
}

/// The four catalog sections' stored bytes, owned so a test can corrupt them.
struct Catalog {
    footer: Footer,
    dict: Vec<u8>,
    ids: Vec<u8>,
    idx: Vec<u8>,
    chunks: Vec<u8>,
}

impl Catalog {
    fn of(obj: &[u8]) -> Self {
        let footer = open_from_full(obj, ReaderLimits::default())
            .expect("open")
            .footer;
        assert!(
            footer.sections.iter().any(|s| s.kind == SERIES_META_CHUNKS),
            "the fixture must take the chunked catalog layout"
        );
        Catalog {
            dict: section(obj, &footer, LABEL_DICT).to_vec(),
            ids: section(obj, &footer, SERIES_IDS).to_vec(),
            idx: section(obj, &footer, SERIES_IDX).to_vec(),
            chunks: section(obj, &footer, SERIES_META_CHUNKS).to_vec(),
            footer,
        }
    }

    fn unpruned(&self) -> Result<Vec<SeriesEntryV4>, SegmentError> {
        decode_catalog_v5_chunked(
            &self.footer,
            &self.dict,
            &self.ids,
            &self.idx,
            &self.chunks,
            ReaderLimits::default(),
        )
    }

    fn matching(&self, equals: &[(&str, &str)]) -> Result<Vec<SeriesEntryV4>, SegmentError> {
        decode_catalog_v5_chunked_matching(
            &self.footer,
            &self.dict,
            &self.ids,
            &self.idx,
            &self.chunks,
            equals,
            ReaderLimits::default(),
        )
    }

    /// Rewrites `kind`'s footer crc32c to match its (edited) stored bytes.
    fn refix_section_crc(&mut self, kind: u32) {
        let crc = crc32c::crc32c(match kind {
            SERIES_IDX => &self.idx,
            SERIES_META_CHUNKS => &self.chunks,
            _ => panic!("unsupported kind {kind}"),
        });
        let s = self
            .footer
            .sections
            .iter_mut()
            .find(|s| s.kind == kind)
            .expect("section present");
        assert_eq!(s.comp, 0, "kind {kind} must be stored uncompressed");
        s.crc32c = crc;
    }
}

fn filtered(entries: &[SeriesEntryV4], equals: &[(&str, &str)]) -> Vec<SeriesEntryV4> {
    entries
        .iter()
        .filter(|e| {
            equals
                .iter()
                .all(|(n, v)| e.entry.labels.get(n) == Some(*v))
        })
        .cloned()
        .collect()
}

/// Matcher sets for one segment: two fixed ones, one series' whole label set,
/// and random sets mixing present pairs, absent values, absent names, and
/// pairs whose name and value are both in the dictionary but never together.
fn matcher_sets(labels: &[Vec<(String, String)>], rng: &mut Rng) -> Vec<Vec<(String, String)>> {
    let pair = |n: &str, v: &str| (n.to_string(), v.to_string());
    let mut sets = vec![
        vec![pair("b", "v0")],
        vec![pair("b", "v0"), pair("job", "v1")],
        labels[rng.below(labels.len())].clone(),
    ];
    for _ in 0..8 {
        let count = 1 + rng.below(3);
        let set = (0..count)
            .map(|_| {
                let series = &labels[rng.below(labels.len())];
                match rng.below(10) {
                    0..=4 => series[rng.below(series.len())].clone(),
                    5 | 6 => pair(&series[rng.below(series.len())].0, "zz_absent"),
                    7 => pair("nope_absent", &series[rng.below(series.len())].1),
                    _ => {
                        let other = &labels[rng.below(labels.len())];
                        pair(
                            &series[rng.below(series.len())].0,
                            &other[rng.below(other.len())].1,
                        )
                    }
                }
            })
            .collect();
        sets.push(set);
    }
    sets
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    /// The matching decode returns exactly the unpruned decode's entries
    /// filtered by the pairs: same ids, labels, runs and order. Every segment
    /// carries `b` at two schema positions and series matching only one of a
    /// two-pair set, both asserted, so a decode that keys on one schema's
    /// position or accepts a partial match fails here.
    #[test]
    fn matching_decode_equals_filtered_unpruned(
        seed in any::<u64>(),
        extra in 0usize..200,
    ) {
        let n = V5_SPARSE_THRESHOLD as usize + extra;
        let mut rng = Rng(seed);
        let labels = series_labels(n, &mut rng);
        let catalog = Catalog::of(&build_sparse(&labels));
        let all = catalog.unpruned().expect("unpruned decode");
        prop_assert_eq!(all.len(), n);

        let sets = matcher_sets(&labels, &mut rng);
        for set in &sets {
            let equals: Vec<(&str, &str)> =
                set.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
            let got = catalog.matching(&equals).expect("matching decode");
            prop_assert_eq!(&got, &filtered(&all, &equals), "pairs {:?}", equals);
        }

        // The fixed sets are not vacuous: `b="v0"` matches series where `b` is
        // the second label and series where it is the third, and the two-pair
        // set excludes series that match `b="v0"` alone.
        let b_only = catalog.matching(&[("b", "v0")]).expect("b only");
        let positions: std::collections::BTreeSet<usize> = b_only
            .iter()
            .map(|e| e.entry.labels.iter().position(|l| l.name == "b").expect("b"))
            .collect();
        prop_assert!(positions.len() >= 2, "b positions {:?}", positions);
        let both = catalog.matching(&[("b", "v0"), ("job", "v1")]).expect("both");
        prop_assert!(both.len() < b_only.len());
        let whole = catalog.matching(&equals_of(&sets[2])).expect("whole label set");
        prop_assert_eq!(whole.len(), 1);
    }
}

fn equals_of(set: &[(String, String)]) -> Vec<(&str, &str)> {
    set.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect()
}

fn fixture() -> Catalog {
    let mut rng = Rng(7);
    let labels = series_labels(V5_SPARSE_THRESHOLD as usize + 10, &mut rng);
    Catalog::of(&build_sparse(&labels))
}

/// Overwrites the zstd magic of the first chunk frame, then repairs the frame
/// crc32c in SERIES_IDX's chunk directory and both sections' footer crc32c,
/// so every crc check passes and the frame's decompression is what fails.
fn corrupt_first_frame(catalog: &mut Catalog) {
    let series_count = catalog.footer.series_count as u32;
    let sparse_count = series_count.div_ceil(V5_STRIDE) as usize;
    // 16-byte header, 36-byte sparse entries, chunk_stride + chunk_count, then
    // 36-byte chunk entries whose crc32c is the last 4 bytes.
    let entry = 16 + sparse_count * 36 + 8;
    let frame_offset =
        u64::from_le_bytes(catalog.idx[entry..entry + 8].try_into().unwrap()) as usize;
    let frame_len =
        u64::from_le_bytes(catalog.idx[entry + 8..entry + 16].try_into().unwrap()) as usize;
    catalog.chunks[frame_offset..frame_offset + 4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let crc = crc32c::crc32c(&catalog.chunks[frame_offset..frame_offset + frame_len]);
    catalog.idx[entry + 32..entry + 36].copy_from_slice(&crc.to_le_bytes());
    catalog.refix_section_crc(SERIES_IDX);
    catalog.refix_section_crc(SERIES_META_CHUNKS);
}

/// A value the dictionary lacks returns `Ok(empty)` from the matching decode
/// on a catalog whose first chunk frame cannot be decompressed, while the
/// unpruned decode of the same bytes returns the typed decompression error.
/// So the matching decode returned before decompressing any chunk.
#[test]
fn absent_value_returns_empty_before_any_chunk_is_decompressed() {
    let mut catalog = fixture();
    corrupt_first_frame(&mut catalog);

    let unpruned = catalog.unpruned();
    assert!(
        matches!(unpruned, Err(SegmentError::Decompress(_))),
        "the unpruned decode reaches the corrupt frame: {unpruned:?}"
    );
    assert_eq!(catalog.matching(&[("job", "zz_absent")]), Ok(Vec::new()));
    assert_eq!(catalog.matching(&[("nope_absent", "v0")]), Ok(Vec::new()));
    assert_eq!(
        catalog.matching(&[("b", "v0"), ("job", "zz_absent")]),
        Ok(Vec::new())
    );
}

/// A pair the dictionary holds decodes the chunks, so a corrupt frame is the
/// same typed error the unpruned decode returns; a section crc mismatch is too.
#[test]
fn present_value_surfaces_a_corrupt_chunk_as_the_unpruned_error() {
    let mut catalog = fixture();
    corrupt_first_frame(&mut catalog);
    let unpruned = catalog.unpruned();
    assert!(matches!(unpruned, Err(SegmentError::Decompress(_))));
    assert_eq!(catalog.matching(&[("b", "v0")]), unpruned);
    // A pair whose name and value are both in the dictionary but which no
    // series holds still reads the chunks.
    assert_eq!(catalog.matching(&[("zone", "v0")]), unpruned);

    let mut catalog = fixture();
    let last = catalog.chunks.len() - 1;
    catalog.chunks[last] ^= 0xFF;
    assert_eq!(catalog.unpruned(), Err(SegmentError::SectionCrcMismatch));
    assert_eq!(
        catalog.matching(&[("b", "v0")]),
        Err(SegmentError::SectionCrcMismatch)
    );
}
