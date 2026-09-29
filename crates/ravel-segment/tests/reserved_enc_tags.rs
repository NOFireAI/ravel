//! Encoding tags 10-13 are reserved for the codecs of ADR-2135 (decisions 3
//! and 6). RSEG never writes them, and a TS_GCD_I64 or ALP page whose inner
//! integer stream carries one is refused with a typed `ValuePageCodec` error
//! rather than decoded under a guessed codec.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_segment::{
    IngestBounds, ReaderLimits, RunEntry, SegmentError, SegmentIdentity, SegmentWriter,
    SeriesInput, decode_catalog_v5, decode_run_pages_soa, open_from_full,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

const TS_PAGES: u32 = 3;
const VAL_PAGES: u32 = 4;
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
