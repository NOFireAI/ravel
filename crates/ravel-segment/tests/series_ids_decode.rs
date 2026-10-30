//! `decode_series_ids` reads an object's SERIES_IDS section on its own, the
//! read a series listing makes before deciding whether to decode the catalog.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentError, SegmentIdentity, SegmentWriter, SeriesInput,
    decode_catalog_v5, decode_series_ids, open_from_full,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

const SERIES_IDS: u32 = 5;

fn series(idx: u8) -> SeriesInput {
    let labels = LabelSet::new(vec![
        Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "m".to_string(),
        },
        Label {
            name: "id".to_string(),
            value: idx.to_string(),
        },
    ])
    .expect("labels");
    SeriesInput {
        series_id: SeriesId([idx; 16]),
        labels,
        samples: vec![Sample {
            ts_ns: 1_700_000_000_000_000_000,
            value: f64::from(idx),
        }],
    }
}

fn written() -> Vec<u8> {
    let identity = SegmentIdentity {
        tenant_hash: [0x2A; 16],
        shard: 0,
        writer_id: "series-ids-decode-test".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 100_000,
    };
    SegmentWriter::write(vec![series(1), series(2), series(3)], identity, bounds)
        .expect("write object")
        .bytes
        .to_vec()
}

#[test]
fn series_ids_decode_alone_to_the_catalogs_ids() {
    let obj = written();
    let loc = open_from_full(&obj, ReaderLimits::default()).expect("open object");
    let section = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_IDS)
        .expect("SERIES_IDS section");
    let start = usize::try_from(section.offset).unwrap();
    let end = start + usize::try_from(section.len).unwrap();

    let ids = decode_series_ids(&loc.footer, &obj[start..end], ReaderLimits::default())
        .expect("decode SERIES_IDS");
    assert_eq!(
        ids,
        vec![SeriesId([1; 16]), SeriesId([2; 16]), SeriesId([3; 16])]
    );
    let catalog: Vec<SeriesId> = decode_catalog_v5(&loc.footer, &obj, ReaderLimits::default())
        .expect("decode catalog")
        .iter()
        .map(|e| e.entry.series_id)
        .collect();
    assert_eq!(ids, catalog);

    let mut flipped = obj[start..end].to_vec();
    let last = flipped.len() - 1;
    flipped[last] ^= 0xFF;
    assert!(matches!(
        decode_series_ids(&loc.footer, &flipped, ReaderLimits::default()),
        Err(SegmentError::SectionCrcMismatch)
    ));
    assert!(matches!(
        decode_series_ids(&loc.footer, &obj[start..end - 1], ReaderLimits::default()),
        Err(SegmentError::SectionOutOfBounds)
    ));
}
