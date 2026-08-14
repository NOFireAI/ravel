//! The reader's `comp = NONE` page path enforces the per-page uncompressed
//! cap before materializing the payload, matching the LZ4 path.
//!
//! An oversized uncompressed page is rejected with the same typed
//! `PageTooLarge` error the LZ4 branch returns, rather than decoding
//! successfully.
#![allow(clippy::expect_used)]

use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesEntryV4, SeriesInput,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

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

fn identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [3u8; 16],
        shard: 1,
        writer_id: "audit-a1".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 1000,
    }
}

/// One series, one sample: the raw-fallback rule stores its VAL page as
/// VAL_RAW_F64 with `comp = 0` (none) and an 8-byte payload.
fn one_sample_object() -> bytes::Bytes {
    let series = vec![SeriesInput {
        series_id: SeriesId([0x7F; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "lonely_metric")]),
        samples: vec![Sample {
            ts_ns: 123,
            value: 9.75,
        }],
    }];
    SegmentWriter::write(series, identity(), bounds())
        .expect("writes")
        .bytes
}

fn slice(bytes: &[u8], range: (u64, u64)) -> &[u8] {
    &bytes[range.0 as usize..(range.0 + range.1) as usize]
}

// --- a1-F01 -----------------------------------------------------------------
//
// `decompress_page_payload_into`'s `comp = NONE` branch copied the payload
// with no size check, while the LZ4 branch rejects `prefix >
// max_page_uncompressed_bytes`. So the documented per-page 64 MiB cap was not
// enforced for uncompressed pages (which is every page the writer emits
// today). This test builds a real 8-byte VAL_RAW_F64 page and decodes it
// under a 4-byte page cap: the correct behavior is `PageTooLarge`. It failed
// before the fix (the page decoded) and passes now that the NONE branch
// enforces the cap.
#[test]
fn none_page_enforces_max_page_uncompressed_cap() {
    let object = one_sample_object();
    let catalog_limits = ReaderLimits::default();

    let loc = ravel_segment::open_from_full(&object, catalog_limits).expect("opens");
    let entries =
        ravel_segment::decode_catalog_v5(&loc.footer, &object, catalog_limits).expect("catalog");
    assert_eq!(entries.len(), 1);

    let selected: Vec<&SeriesEntryV4> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v4(&loc.footer, &selected).expect("ranges");
    let ts_bytes = slice(&object, ranges[0].ts_range);
    let val_bytes = slice(&object, ranges[0].val_range);

    // The VAL_RAW_F64 payload is 8 bytes (6-byte header + one f64); the TS
    // payload is a couple of varint bytes. A 4-byte per-page cap must reject
    // the VAL page before it is materialized.
    let tight = ReaderLimits {
        max_section_uncompressed_bytes: catalog_limits.max_section_uncompressed_bytes,
        max_page_uncompressed_bytes: 4,
    };

    let run = &entries[0].runs[0];
    let mut scratch = Vec::new();
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    let result = ravel_segment::decode_run_pages_soa(
        &entries[0].entry.series_id,
        run,
        ts_bytes,
        val_bytes,
        tight,
        &mut scratch,
        &mut timestamps,
        &mut values,
    );
    assert!(
        matches!(
            result,
            Err(ravel_segment::SegmentError::PageTooLarge { .. })
        ),
        "expected PageTooLarge for an 8-byte uncompressed page under a 4-byte \
         cap; got {result:?} -- the comp=NONE path must check \
         max_page_uncompressed_bytes just as the LZ4 path does"
    );
}
