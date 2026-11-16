//! Encoding tags 10-13 are reserved for the codecs of ADR-2135 (decisions 3
//! and 6). RSEG never writes them. A TS_GCD_I64 or ALP page whose inner
//! integer stream carries one is refused with a typed `ValuePageCodec` error,
//! and a SERIES_META provenance column block carrying one with a typed
//! `ProvenanceColumnCodec` error, rather than decoded under a guessed codec.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use prost::Message;
use ravel_segment::{
    CompactionMetaV4, IngestBounds, ReaderLimits, RunEntry, RunInputV7, SampleProvenance,
    SegmentError, SegmentIdentity, SegmentWriter, SeriesInput, SeriesInputV7, SeriesValues,
    decode_catalog_v5, decode_run_pages_soa, encode_run_v4, open_from_full,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

const SERIES_META: u32 = 6;
const TS_PAGES: u32 = 3;
const VAL_PAGES: u32 = 4;
const SECTION_COMP_NONE: i32 = 0;
const TS_GCD_I64: u8 = 2;
const VAL_ALP: u8 = 18;
const COMP_NONE: u8 = 0;

const START: i64 = 1_700_000_000_000_000_000;
const MS: i64 = 1_000_000;
const STEP_MS: i64 = 15_000_000_000;
const SERIES: SeriesId = SeriesId([0x07; 16]);

/// A stored page: `enc`, `comp`, crc32c over `series_id || enc || comp ||
/// payload`, then the payload (docs/segment-format.md).
fn page(enc: u8, payload: &[u8]) -> Vec<u8> {
    let mut crc = crc32c::crc32c(&SERIES.0);
    crc = crc32c::crc32c_append(crc, &[enc, COMP_NONE]);
    crc = crc32c::crc32c_append(crc, payload);
    let mut out = vec![enc, COMP_NONE];
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn uvarint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Writes a one-series object and returns its run with the stored TS and VAL
/// pages.
fn written_run() -> (RunEntry, Vec<u8>, Vec<u8>) {
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: "reserved".to_string(),
    }])
    .expect("labels");
    let samples = (0..64)
        .map(|i| Sample {
            ts_ns: START + i as i64 * STEP_MS,
            value: (i % 7) as f64 / 4.0,
        })
        .collect();
    let written = SegmentWriter::write(
        vec![SeriesInput {
            series_id: SERIES,
            labels,
            samples,
        }],
        SegmentIdentity {
            tenant_hash: [0x2A; 16],
            shard: 1,
            writer_id: "reserved-enc-tags".to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        },
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 100_000,
        },
    )
    .expect("write object");
    let obj = written.bytes.as_ref();
    let loc = open_from_full(obj, ReaderLimits::default()).expect("open object");
    let footer = &loc.footer;
    let entries = decode_catalog_v5(footer, obj, ReaderLimits::default()).expect("catalog");
    let run = entries[0].runs[0];
    let slice = |kind: u32, (off, len): (u64, u64)| {
        let s = footer.sections.iter().find(|s| s.kind == kind).unwrap();
        let at = (s.offset + off) as usize;
        obj[at..at + len as usize].to_vec()
    };
    (
        run,
        slice(TS_PAGES, run.ts_page),
        slice(VAL_PAGES, run.val_page),
    )
}

fn decode(run: &RunEntry, ts_page: &[u8], val_page: &[u8]) -> Result<(), SegmentError> {
    let (mut scratch, mut ts_out, mut vals_out) = (Vec::new(), Vec::new(), Vec::new());
    decode_run_pages_soa(
        &SERIES,
        run,
        ts_page,
        val_page,
        ReaderLimits::default(),
        &mut scratch,
        &mut ts_out,
        &mut vals_out,
    )
    .map(|_| ())
}

fn assert_refused(r: Result<(), SegmentError>, tag: u8, what: &str) {
    match r {
        Err(SegmentError::ValuePageCodec(m)) => {
            assert!(m.contains("not an integer codec"), "{what} tag {tag}: {m}")
        }
        Err(other) => panic!("{what} tag {tag}: expected ValuePageCodec, got {other:?}"),
        Ok(()) => panic!("{what} tag {tag}: a reserved tag decoded"),
    }
}

#[test]
fn reserved_enc_tags_are_refused_in_ts_gcd_and_alp_pages() {
    let (run, ts_page, val_page) = written_run();
    decode(&run, &ts_page, &val_page).expect("the written pages decode");

    for tag in 10u8..=13 {
        // TS_GCD_I64: uvarint divisor, inner integer tag, integer stream.
        let mut ts_payload = Vec::new();
        uvarint(MS as u64, &mut ts_payload);
        ts_payload.push(tag);
        ts_payload.extend_from_slice(&[0u8; 16]);
        assert_refused(
            decode(&run, &page(TS_GCD_I64, &ts_payload), &val_page),
            tag,
            "TS_GCD_I64",
        );

        // ALP: decimal exponent, inner integer tag, uvarint stream length,
        // the stream, uvarint exception count.
        let mut alp_payload = vec![2u8, tag];
        uvarint(16, &mut alp_payload);
        alp_payload.extend_from_slice(&[0u8; 16]);
        uvarint(0, &mut alp_payload);
        assert_refused(
            decode(&run, &ts_page, &page(VAL_ALP, &alp_payload)),
            tag,
            "ALP",
        );
    }
}

/// One `encode_i64` column block as SERIES_META stores it: `block_len`
/// varint, the `Enc` tag byte, then the codec payload.
fn encoded_block(values: &[i64]) -> Vec<u8> {
    let (enc, bytes) = ravel_codec::encoding::encode_i64(values);
    let mut out = Vec::new();
    uvarint((1 + bytes.len()) as u64, &mut out);
    out.push(enc.to_u8());
    out.extend_from_slice(&bytes);
    out
}

/// A one-run object whose run carries per-sample provenance, so SERIES_META
/// ends with the five-block provenance extension.
fn object_with_provenance(column: &[SampleProvenance]) -> Vec<u8> {
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: "reserved".to_string(),
    }])
    .expect("labels");
    let samples = SeriesValues::Scalar(
        (0..column.len())
            .map(|i| Sample {
                ts_ns: START + i as i64 * STEP_MS,
                value: i as f64,
            })
            .collect(),
    );
    let run = encode_run_v4(&SERIES, 50, 1, 1, &samples).expect("frame run");
    SegmentWriter::write_v7_with_provenance(
        vec![SeriesInputV7 {
            series_id: SERIES,
            labels,
            runs: vec![RunInputV7 {
                run,
                provenance: Some(column.to_vec()),
            }],
        }],
        SegmentIdentity {
            tenant_hash: [0x2A; 16],
            shard: 1,
            writer_id: "reserved-enc-tags".to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        },
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 100_000,
        },
        CompactionMetaV4 {
            ingest_hour_bucket: 5,
            input_set_hash: [0x33; 32],
            part_index: 0,
            level: 1,
        },
        Vec::new(),
    )
    .expect("write object")
    .bytes
    .to_vec()
}

/// `obj` with its SERIES_META replaced by `raw`, stored uncompressed after the
/// last section and pointed at by a re-encoded footer and a re-sealed trailer.
/// Readers locate sections only through footer offsets, and the old section's
/// bytes are zeroed, the only permitted value between sections.
fn with_series_meta(obj: &[u8], raw: &[u8]) -> Vec<u8> {
    let loc = open_from_full(obj, ReaderLimits::default()).expect("open");
    let mut footer = loc.footer.clone();
    let mut out = obj[..loc.footer_offset as usize].to_vec();
    let meta = footer
        .sections
        .iter_mut()
        .find(|s| s.kind == SERIES_META)
        .expect("SERIES_META");
    out[meta.offset as usize..(meta.offset + meta.len) as usize].fill(0);
    meta.offset = out.len() as u64;
    meta.len = raw.len() as u64;
    meta.uncompressed_len = raw.len() as u64;
    meta.crc32c = crc32c::crc32c(raw);
    meta.comp = SECTION_COMP_NONE;
    out.extend_from_slice(raw);

    let footer_bytes = footer.encode_to_vec();
    let footer_len = footer_bytes.len() as u32;
    out.extend_from_slice(&footer_bytes);
    let trailer = &obj[loc.trailer_offset as usize..];
    let (version, tail) = (&trailer[8..10], &trailer[10..16]);
    let mut crc = crc32c::crc32c(&footer_bytes);
    crc = crc32c::crc32c_append(crc, &footer_len.to_le_bytes());
    crc = crc32c::crc32c_append(crc, version);
    crc = crc32c::crc32c_append(crc, tail);
    out.extend_from_slice(&footer_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(version);
    out.extend_from_slice(tail);
    out
}

/// SERIES_META's provenance column blocks are decoded through
/// `Enc::from_u8` and `decode_i64` (the reader's `take_encoded_i64_block`).
/// The presence block of a real object is re-tagged with each reserved tag,
/// keeping its payload, and the catalog decode must refuse it with a typed
/// `ProvenanceColumnCodec` error. The untouched raw section, relocated the
/// same way, decodes to the written column, so the refusal is the tag's.
#[test]
fn reserved_enc_tags_are_refused_in_series_meta_provenance_blocks() {
    let column = [
        SampleProvenance {
            created_unix_ns: 100,
            writer_epoch: 3,
            writer_seq: 7,
            in_page_index: 0,
        },
        SampleProvenance {
            created_unix_ns: 104,
            writer_epoch: 3,
            writer_seq: 8,
            in_page_index: 1,
        },
    ];
    let obj = object_with_provenance(&column);
    let loc = open_from_full(&obj, ReaderLimits::default()).expect("open");
    let meta = *loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_META)
        .expect("SERIES_META");
    let stored = &obj[meta.offset as usize..(meta.offset + meta.len) as usize];
    let raw = zstd::bulk::decompress(stored, meta.uncompressed_len as usize).expect("unzstd");

    // The extension is SERIES_META's suffix: presence, then created delta
    // from the footer's base, epoch, seq and in-page index.
    let base = loc.footer.base_created_unix_ns;
    let presence = encoded_block(&[1]);
    let mut extension = presence.clone();
    extension.extend(encoded_block(&column.map(|p| p.created_unix_ns - base)));
    extension.extend(encoded_block(&column.map(|p| p.writer_epoch as i64)));
    extension.extend(encoded_block(&column.map(|p| p.writer_seq as i64)));
    extension.extend(encoded_block(&column.map(|p| i64::from(p.in_page_index))));
    assert!(
        raw.ends_with(&extension),
        "SERIES_META ends with the extension"
    );
    assert!(presence.len() < 0x80, "one-byte block_len");
    let tag_at = raw.len() - extension.len() + 1;

    let relocated = with_series_meta(&obj, &raw);
    let loc = open_from_full(&relocated, ReaderLimits::default()).expect("open relocated");
    let entries =
        decode_catalog_v5(&loc.footer, &relocated, ReaderLimits::default()).expect("catalog");
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].per_sample_provenance,
        vec![Some(column.to_vec())]
    );

    for tag in 10u8..=13 {
        let mut tampered = raw.clone();
        tampered[tag_at] = tag;
        let obj = with_series_meta(&obj, &tampered);
        let loc = open_from_full(&obj, ReaderLimits::default()).expect("open tampered");
        match decode_catalog_v5(&loc.footer, &obj, ReaderLimits::default()) {
            Err(SegmentError::ProvenanceColumnCodec(m)) => {
                assert!(m.contains("not an integer codec"), "tag {tag}: {m}")
            }
            Err(other) => panic!("tag {tag}: expected ProvenanceColumnCodec, got {other:?}"),
            Ok(_) => panic!("tag {tag}: a reserved tag decoded"),
        }
    }
}
