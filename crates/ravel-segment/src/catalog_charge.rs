//! The memory a catalog decode materializes beyond the section bytes it
//! decompresses, charged before the decode allocates it.

use crate::reader::SampleProvenance;

/// The most bytes the four decoded `encode_i64` provenance columns (created
/// delta, writer epoch, writer seq, in-page index) hold per flagged sample:
/// twice each value's 8 bytes, since an RLE column's buffer grows by
/// doubling from a capped initial capacity (`ravel_codec::encoding::decode_i64`).
pub const PROVENANCE_COLUMN_BYTES_PER_SAMPLE: u64 = 4 * 2 * std::mem::size_of::<i64>() as u64;

/// Bytes one decoded [`SampleProvenance`] holds.
pub const SAMPLE_PROVENANCE_BYTES: u64 = std::mem::size_of::<SampleProvenance>() as u64;

/// A ledger for what a catalog decode allocates beyond its decompressed
/// sections: the per-sample provenance extension (ADR-0092 decision 1). Its
/// columns store a run-wide constant in a few bytes and decode to
/// [`PROVENANCE_COLUMN_BYTES_PER_SAMPLE`] plus [`SAMPLE_PROVENANCE_BYTES`] per
/// flagged sample, so a section-sized charge does not cover them.
///
/// The `_charged` catalog decoders call [`admit`](Self::admit) with each
/// figure before allocating it, and [`release`](Self::release) with the same
/// figure once those bytes are freed. What stays admitted when a decode
/// returns is the provenance of the entries it returned. A decode that fails
/// releases nothing: the caller drops whatever its ledger admitted.
pub trait CatalogCharge {
    /// Called before the decode allocates `bytes`. `false` stops the decode
    /// with [`SegmentError::CatalogChargeRefused`](crate::SegmentError::CatalogChargeRefused)
    /// before it allocates them.
    fn admit(&mut self, bytes: u64) -> bool;

    /// Called once the decode has freed `bytes` that one earlier
    /// [`admit`](Self::admit) call of exactly that figure admitted.
    fn release(&mut self, bytes: u64);

    /// Whether the decode materializes the per-sample provenance extension.
    /// `false` passes over its four sample-major columns by their block
    /// prefixes without decoding them, so it neither allocates nor admits
    /// them, and every returned entry's `per_sample_provenance` is empty, the
    /// shape of an object without the extension. Only a caller that reads
    /// series ids and labels, never samples, may return `false`: the runs it
    /// gets back no longer carry the per-sample dedup keys their samples need.
    fn decodes_provenance(&self) -> bool {
        true
    }
}

/// A [`CatalogCharge`] that admits everything: the plain catalog decoders.
#[derive(Debug, Clone, Copy, Default)]
pub struct Uncharged;

impl CatalogCharge for Uncharged {
    fn admit(&mut self, _bytes: u64) -> bool {
        true
    }

    fn release(&mut self, _bytes: u64) {}
}

/// The decoded [`SampleProvenance`] bytes `columns` hold.
pub(crate) fn provenance_bytes(columns: &[Option<Vec<SampleProvenance>>]) -> u64 {
    columns
        .iter()
        .flatten()
        .map(|column| (column.len() as u64).saturating_mul(SAMPLE_PROVENANCE_BYTES))
        .fold(0, u64::saturating_add)
}

/// A catalog decode whose charge does not decode provenance returns what the
/// decode that does returns, minus the provenance, over both SERIES_META
/// forms and through the whole-catalog and the matching decoders.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod without_provenance_tests {
    use super::CatalogCharge;
    use crate::format::{ReaderLimits, V5_SPARSE_THRESHOLD, section_kind};
    use crate::reader::{SampleProvenance, SeriesEntryV4, decode_counter, find_section};
    use crate::writer::{
        CompactionMetaV4, IngestBounds, RunInputV7, SegmentIdentity, SegmentWriter, SeriesInputV7,
        encode_run_v4,
    };
    use crate::{
        SegmentError, decode_catalog_matching_v4_charged, decode_catalog_v5_charged,
        decode_catalog_v5_chunked_matching_charged, open_from_full,
    };
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

    /// Sums what the decode admits and releases.
    struct Ledger {
        decodes_provenance: bool,
        admitted: u64,
        released: u64,
    }

    impl Ledger {
        fn new(decodes_provenance: bool) -> Self {
            Self {
                decodes_provenance,
                admitted: 0,
                released: 0,
            }
        }
    }

    impl CatalogCharge for Ledger {
        fn admit(&mut self, bytes: u64) -> bool {
            self.admitted += bytes;
            true
        }

        fn release(&mut self, bytes: u64) {
            self.released += bytes;
        }

        fn decodes_provenance(&self) -> bool {
            self.decodes_provenance
        }
    }

    /// Samples in each flagged run.
    const FLAGGED_RUN: usize = 3;

    /// `n` series named `m`, each with an `idx` label; the series at each of
    /// `flagged` carry one merged run of [`FLAGGED_RUN`] samples with
    /// per-sample provenance, the rest one sample with none.
    fn object(n: usize, flagged: &[usize]) -> Vec<u8> {
        let series = (0..n)
            .map(|i| {
                let mut raw = [0u8; 16];
                raw[8..].copy_from_slice(&(i as u64).to_be_bytes());
                let id = SeriesId(raw);
                let labels = LabelSet::new(vec![
                    Label {
                        name: METRIC_NAME_LABEL.to_string(),
                        value: "m".to_string(),
                    },
                    Label {
                        name: "idx".to_string(),
                        value: i.to_string(),
                    },
                ])
                .expect("labels");
                let is_flagged = flagged.contains(&i);
                let len = if is_flagged { FLAGGED_RUN } else { 1 };
                let samples: Vec<Sample> = (0..len)
                    .map(|k| Sample {
                        ts_ns: 100 + k as i64,
                        value: (i + k) as f64,
                    })
                    .collect();
                let run = encode_run_v4(&id, 40, 1, 1, &crate::SeriesValues::Scalar(samples))
                    .expect("run");
                let provenance = is_flagged.then(|| {
                    (0..len)
                        .map(|k| SampleProvenance {
                            created_unix_ns: 40 + k as i64,
                            writer_epoch: 1,
                            writer_seq: k as u64,
                            in_page_index: 0,
                        })
                        .collect()
                });
                SeriesInputV7 {
                    series_id: id,
                    labels,
                    runs: vec![RunInputV7 { run, provenance }],
                }
            })
            .collect();
        SegmentWriter::write_v7_with_provenance(
            series,
            SegmentIdentity {
                tenant_hash: [0x2A; 16],
                shard: 0,
                writer_id: "listing".to_string(),
                writer_epoch: 1,
                writer_seq: 1,
            },
            IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 100,
            },
            CompactionMetaV4 {
                ingest_hour_bucket: 1,
                input_set_hash: [0x33; 32],
                part_index: 0,
                level: 1,
            },
            Vec::new(),
        )
        .expect("write")
        .bytes
        .to_vec()
    }

    type Decode<'a> =
        dyn Fn(&mut dyn CatalogCharge) -> Result<Vec<SeriesEntryV4>, SegmentError> + 'a;

    /// One decode's entries, `encode_i64` blocks decoded and ledger.
    fn run(decode: &Decode<'_>, decodes_provenance: bool) -> (Vec<SeriesEntryV4>, u64, Ledger) {
        decode_counter::reset();
        let mut ledger = Ledger::new(decodes_provenance);
        let entries = decode(&mut ledger).expect("decode");
        (entries, decode_counter::i64_block_decodes(), ledger)
    }

    /// The decode without provenance returns every entry the decode with it
    /// returns, field for field apart from the provenance, decodes one
    /// presence block per extension and no column, and admits nothing. The
    /// decode with provenance is pinned alongside, so the fixture provably
    /// carries `extensions` extensions over `flagged` samples.
    ///
    /// FLIPS: the skip branch in `parse_series_meta_tail_v4` or
    /// `decode_chunk_frame` calling the full parser with an uncharged ledger
    /// reads `5 * extensions` blocks instead of `extensions`; dropping the
    /// entries whose provenance it skipped fails the series count.
    fn assert_skips(decode: &Decode<'_>, series: usize, extensions: u64, flagged: u64, what: &str) {
        let (with, with_blocks, with_ledger) = run(decode, true);
        let (without, without_blocks, without_ledger) = run(decode, false);
        assert_eq!(with.len(), series, "{what}: every series decoded");
        assert_eq!(without.len(), series, "{what}: every series decoded");
        for (a, b) in with.iter().zip(&without) {
            assert_eq!(a.entry, b.entry, "{what}");
            assert_eq!(a.runs, b.runs, "{what}");
            assert!(b.per_sample_provenance.is_empty(), "{what}");
        }
        let kept: usize = with
            .iter()
            .flat_map(|e| &e.per_sample_provenance)
            .flatten()
            .map(Vec::len)
            .sum();
        assert_eq!(kept as u64, flagged, "{what}: the fixture's provenance");
        assert_eq!(with_blocks, 5 * extensions, "{what}");
        assert_eq!(without_blocks, extensions, "{what}");
        // Columns and structs admitted, then the kept structs re-admitted.
        let per_sample =
            super::PROVENANCE_COLUMN_BYTES_PER_SAMPLE + 2 * super::SAMPLE_PROVENANCE_BYTES;
        assert_eq!(with_ledger.admitted, flagged * per_sample, "{what}");
        assert_eq!(
            (without_ledger.admitted, without_ledger.released),
            (0, 0),
            "{what}"
        );
    }

    fn section<'a>(bytes: &'a [u8], footer: &crate::Footer, kind: u32) -> &'a [u8] {
        let s = find_section(footer, kind).expect("section");
        &bytes[s.offset as usize..(s.offset + s.len) as usize]
    }

    #[test]
    fn a_whole_series_meta_decode_without_provenance_returns_the_same_series() {
        let flagged = [0, 2, 4];
        let bytes = object(6, &flagged);
        let footer = open_from_full(&bytes, ReaderLimits::default())
            .expect("open")
            .footer;
        assert!(find_section(&footer, section_kind::SERIES_META).is_some());
        let samples = (flagged.len() * FLAGGED_RUN) as u64;
        let whole = |charge: &mut dyn CatalogCharge| {
            decode_catalog_v5_charged(&footer, &bytes, ReaderLimits::default(), charge)
        };
        assert_skips(&whole, 6, 1, samples, "whole SERIES_META");
        let matching = |charge: &mut dyn CatalogCharge| {
            decode_catalog_matching_v4_charged(
                &footer,
                section(&bytes, &footer, section_kind::LABEL_DICT),
                section(&bytes, &footer, section_kind::SERIES_IDS),
                section(&bytes, &footer, section_kind::SERIES_META),
                &[(METRIC_NAME_LABEL, "m")],
                ReaderLimits::default(),
                charge,
            )
        };
        assert_skips(&matching, 6, 1, samples, "matching SERIES_META");
    }

    #[test]
    fn a_chunked_decode_without_provenance_returns_the_same_series() {
        let n = V5_SPARSE_THRESHOLD as usize + 3;
        // Three flagged series in three different 512-series chunks.
        let flagged = [1, 600, n - 1];
        let bytes = object(n, &flagged);
        let footer = open_from_full(&bytes, ReaderLimits::default())
            .expect("open")
            .footer;
        assert!(find_section(&footer, section_kind::SERIES_META_CHUNKS).is_some());
        let samples = (flagged.len() * FLAGGED_RUN) as u64;
        let whole = |charge: &mut dyn CatalogCharge| {
            decode_catalog_v5_charged(&footer, &bytes, ReaderLimits::default(), charge)
        };
        assert_skips(&whole, n, 3, samples, "chunked");
        let matching = |charge: &mut dyn CatalogCharge| {
            decode_catalog_v5_chunked_matching_charged(
                &footer,
                section(&bytes, &footer, section_kind::LABEL_DICT),
                section(&bytes, &footer, section_kind::SERIES_IDS),
                section(&bytes, &footer, section_kind::SERIES_IDX),
                section(&bytes, &footer, section_kind::SERIES_META_CHUNKS),
                &[(METRIC_NAME_LABEL, "m")],
                ReaderLimits::default(),
                charge,
            )
        };
        assert_skips(&matching, n, 3, samples, "matching chunked");
    }
}
