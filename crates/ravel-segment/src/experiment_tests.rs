//! Tests for the within-segment selective-read experiment (issue #167).
//! These exercise both prototype variants: SERIES_IDX-only (additive, still a
//! valid v2 object) and SERIES_IDX + chunked meta (non-additive prototype).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::format::section_kind;
use crate::reader::{SeriesEntry, decode_catalog_v2, open_from_full};
use crate::writer::{SegmentWriter, SeriesInput};
use ravel_proto::segment::v1::Footer;
use ravel_types::{Label, LabelSet, Sample, SeriesId};

fn identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [7; 16],
        shard: 1,
        writer_id: "exp".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    }
}

/// Deterministic batch of `n` series with distinct ids (not in sorted order,
/// so the writer's sort and the reader's binary search are both exercised),
/// a small shared-name schema, and two samples each.
fn make_series(n: usize) -> Vec<SeriesInput> {
    (0..n)
        .map(|i| {
            let mut id = [0u8; 16];
            id[0] = (i % 7) as u8;
            id[4] = (i % 251) as u8;
            id[8..16].copy_from_slice(&(i as u64).to_be_bytes());
            let labels = LabelSet::new(vec![
                Label {
                    name: "job".to_string(),
                    value: format!("j{}", i % 5),
                },
                Label {
                    name: "series_idx".to_string(),
                    value: i.to_string(),
                },
            ])
            .unwrap();
            let base = 1_700_000_000_000_000_000i64;
            let samples = vec![
                Sample {
                    ts_ns: base + i as i64,
                    value: i as f64,
                },
                Sample {
                    ts_ns: base + i as i64 + 1000,
                    value: i as f64 + 0.5,
                },
            ];
            SeriesInput {
                series_id: SeriesId(id),
                labels,
                samples,
            }
        })
        .collect()
}

fn stored<'a>(obj: &'a [u8], footer: &Footer, kind: u32) -> &'a [u8] {
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .unwrap_or_else(|| panic!("section kind {kind} present"));
    &obj[s.offset as usize..(s.offset + s.len) as usize]
}

fn section_len(footer: &Footer, kind: u32) -> u64 {
    footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .map(|s| s.len)
        .unwrap_or(0)
}

/// Catalog of the plain v2 object, the ground truth every variant is checked
/// against (variants copy the TS/VAL pages and re-derive meta from this).
fn v2_catalog(series: &[SeriesInput]) -> (Vec<u8>, Footer, Vec<SeriesEntry>) {
    let seg = SegmentWriter::write_v2(clone_series(series), identity(), bounds()).unwrap();
    let obj = seg.bytes.to_vec();
    let loc = open_from_full(&obj, ReaderLimits::default()).unwrap();
    let footer = loc.footer;
    let entries = decode_catalog_v2(
        &footer,
        stored(&obj, &footer, section_kind::LABEL_DICT),
        stored(&obj, &footer, section_kind::SERIES_IDS),
        stored(&obj, &footer, section_kind::SERIES_META),
        ReaderLimits::default(),
    )
    .unwrap();
    (obj, footer, entries)
}

fn clone_series(series: &[SeriesInput]) -> Vec<SeriesInput> {
    series
        .iter()
        .map(|s| SeriesInput {
            series_id: s.series_id,
            labels: s.labels.clone(),
            samples: s.samples.clone(),
        })
        .collect()
}

#[test]
fn experiment_flag_off_is_byte_identical_to_write_v2() {
    for n in [0usize, 1, 7, 500, 1300] {
        let plain = SegmentWriter::write_v2(make_series(n), identity(), bounds()).unwrap();
        let exp = write_v2_experimental(
            make_series(n),
            identity(),
            bounds(),
            ExperimentFlags::none(),
        )
        .unwrap();
        assert_eq!(
            plain.bytes.as_ref(),
            exp.bytes.as_ref(),
            "flag-off output must be byte-identical to write_v2 at n={n}"
        );
        assert_eq!(plain.summary, exp.summary, "summary identical at n={n}");
    }
}

#[test]
fn series_idx_only_is_readable_by_v2_reader() {
    // A SERIES_IDX-only object carries the unchanged v2 catalog plus one
    // unknown (kind 8) section, which a stock v2 reader skips. So
    // open_from_full succeeds and decode_catalog_v2 returns exactly the plain
    // v2 catalog.
    let series = make_series(1300);
    let (_plain_obj, _plain_footer, plain_entries) = v2_catalog(&series);

    let exp = write_v2_experimental(
        clone_series(&series),
        identity(),
        bounds(),
        ExperimentFlags::series_idx_only(),
    )
    .unwrap();
    let obj = exp.bytes.to_vec();

    // Stock reader validates and reads it as a normal v2 object.
    let loc = open_from_full(&obj, ReaderLimits::default()).expect("valid v2 object");
    assert!(
        find_experimental_section(&loc.footer, SECTION_KIND_SERIES_IDX).is_some(),
        "SERIES_IDX section present"
    );
    let entries = decode_catalog_v2(
        &loc.footer,
        stored(&obj, &loc.footer, section_kind::LABEL_DICT),
        stored(&obj, &loc.footer, section_kind::SERIES_IDS),
        stored(&obj, &loc.footer, section_kind::SERIES_META),
        ReaderLimits::default(),
    )
    .expect("v2 decode of the SERIES_IDX-augmented object");
    assert_eq!(
        entries, plain_entries,
        "SERIES_IDX must not disturb the v2 catalog a stock reader decodes"
    );
}

#[test]
fn decode_page_locations_matches_catalog() {
    let series = make_series(1300);
    let (obj, footer, entries) = v2_catalog(&series);
    let locs = decode_page_locations_v2(
        &footer,
        stored(&obj, &footer, section_kind::SERIES_META),
        section_len(&footer, section_kind::TS_PAGES),
        section_len(&footer, section_kind::VAL_PAGES),
        ReaderLimits::default(),
    )
    .unwrap();
    assert_eq!(locs.len(), entries.len());
    for (loc, e) in locs.iter().zip(&entries) {
        assert_eq!(loc.sample_count, e.sample_count);
        assert_eq!(loc.min_ts_ns, e.min_ts_ns);
        assert_eq!(loc.max_ts_ns, e.max_ts_ns);
        assert_eq!(loc.ts_page, e.ts_page);
        assert_eq!(loc.val_page, e.val_page);
    }
}

#[test]
fn sparse_index_locates_every_series() {
    let series = make_series(1300);
    let (_obj, _footer, entries) = v2_catalog(&series);

    let exp = write_v2_experimental(
        clone_series(&series),
        identity(),
        bounds(),
        ExperimentFlags::series_idx_only(),
    )
    .unwrap();
    let obj = exp.bytes.to_vec();
    let footer = parse_footer_unvalidated(&obj).unwrap();
    let idx = parse_series_idx(stored(&obj, &footer, SECTION_KIND_SERIES_IDX)).unwrap();
    assert!(!idx.has_chunks());
    let ids = stored(&obj, &footer, section_kind::SERIES_IDS);

    for (abs, e) in entries.iter().enumerate() {
        let target = e.series_id.0;
        let window = idx.locate(&target).expect("id is >= smallest, so a window");
        let start = window.section_offset as usize;
        let win_bytes = &ids[start..start + window.len as usize];
        let found = find_index_in_window(win_bytes, window.first_index, &target)
            .unwrap()
            .expect("present id must be found");
        assert_eq!(found, abs as u64, "located index matches sorted position");
    }

    // An id below the smallest indexed id cannot be present.
    let mut below = entries[0].series_id.0;
    if below != [0u8; 16] {
        below = [0u8; 16];
        assert!(
            idx.locate(&below).is_none() || {
                let w = idx.locate(&below).unwrap();
                let start = w.section_offset as usize;
                find_index_in_window(&ids[start..start + w.len as usize], w.first_index, &below)
                    .unwrap()
                    .is_none()
            }
        );
    }
}

#[test]
fn chunked_meta_point_lookup_matches_catalog() {
    let series = make_series(1300);
    let (_obj, _footer, entries) = v2_catalog(&series);

    let exp = write_v2_experimental(
        clone_series(&series),
        identity(),
        bounds(),
        ExperimentFlags::series_idx_and_chunked_meta(),
    )
    .unwrap();
    let obj = exp.bytes.to_vec();
    let footer = parse_footer_unvalidated(&obj).unwrap();

    // A chunked-meta object drops SERIES_META, so a stock reader fails closed.
    assert!(
        find_experimental_section(&footer, section_kind::SERIES_META).is_none(),
        "chunked variant removes whole SERIES_META"
    );
    assert!(open_from_full(&obj, ReaderLimits::default()).is_err());

    let idx = parse_series_idx(stored(&obj, &footer, SECTION_KIND_SERIES_IDX)).unwrap();
    assert!(idx.has_chunks());
    let ids = stored(&obj, &footer, section_kind::SERIES_IDS);
    let chunks = stored(&obj, &footer, SECTION_KIND_SERIES_META_CHUNKS);

    for (abs, e) in entries.iter().enumerate() {
        let target = e.series_id.0;
        let window = idx.locate(&target).unwrap();
        let start = window.section_offset as usize;
        let found = find_index_in_window(
            &ids[start..start + window.len as usize],
            window.first_index,
            &target,
        )
        .unwrap()
        .expect("present id");
        assert_eq!(found, abs as u64);

        let cl = idx.chunk_for(found).expect("chunk for index");
        let fo = cl.frame_offset as usize;
        let frame_stored = &chunks[fo..fo + cl.frame_stored_len as usize];
        let frame = decompress_chunk_frame(
            frame_stored,
            cl.frame_uncompressed_len,
            ReaderLimits::default(),
        )
        .unwrap();
        let loc =
            decode_chunk_page_location(&frame, cl.row_in_chunk, footer.min_event_ts_ns).unwrap();
        assert_eq!(
            loc.sample_count, e.sample_count,
            "sample_count for series {abs}"
        );
        assert_eq!(loc.min_ts_ns, e.min_ts_ns, "min_ts for series {abs}");
        assert_eq!(loc.max_ts_ns, e.max_ts_ns, "max_ts for series {abs}");
        assert_eq!(loc.ts_page, e.ts_page, "ts_page for series {abs}");
        assert_eq!(loc.val_page, e.val_page, "val_page for series {abs}");
    }
}
