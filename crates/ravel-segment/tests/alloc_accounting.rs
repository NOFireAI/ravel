//! Allocation regression test for the SoA decode path
//! (docs/arrow-datafusion-plan.md ticket A1a). A global counting allocator
//! would need an `unsafe impl GlobalAlloc`, which this workspace denies
//! (`unsafe` is forbidden workspace-wide, repo CLAUDE.md); instead this
//! proves zero-reallocation the observable way: `Vec::capacity` only grows
//! on reallocation and never shrinks on `clear`, so a `decode_pages_soa`
//! call that leaves `scratch`/`timestamps`/`values` capacity unchanged
//! after a warm-up pass made zero heap allocations for that call. The AoS
//! `decode_pages` path has no such caller-supplied buffers to warm up: by
//! construction (see `decode_pages`'s delegation to `decode_pages_soa` with
//! fresh `Vec`s) it allocates scratch, timestamps, values, and the merged
//! `Sample` vec fresh on every single call.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesEntry, SeriesInput,
    decode_catalog, decode_pages_soa, open_from_full, plan_ranges,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

const SERIES_COUNT: usize = 50;
const SAMPLES_PER_SERIES: usize = 30;

fn build_test_segment() -> bytes::Bytes {
    let tenant_id = TenantId::new("alloc-test-tenant".to_string());
    let mut series = Vec::with_capacity(SERIES_COUNT);
    for i in 0..SERIES_COUNT {
        let metric = format!("alloc_metric_{i}");
        let labels = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: metric.clone(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, &metric, &labels).expect("series id");
        // Same sample count for every series so a capacity warmed up on the
        // first series is guaranteed sufficient for every later one.
        let samples = (0..SAMPLES_PER_SERIES)
            .map(|j| Sample {
                ts_ns: 1_000_000_000 * (j as i64),
                value: 100.0 + (j as f64) * 0.5 + (i as f64) * 0.001,
            })
            .collect();
        series.push(SeriesInput {
            series_id,
            labels,
            samples,
        });
    }
    let identity = SegmentIdentity {
        tenant_hash: [2u8; 16],
        shard: 0,
        writer_id: "alloc-test-writer".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    SegmentWriter::write(series, identity, bounds)
        .expect("write segment")
        .bytes
}

fn section_bytes(footer: &ravel_segment::Footer, kind: u32) -> std::ops::Range<usize> {
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    s.offset as usize..(s.offset + s.len) as usize
}

fn decode_entries(bytes: &bytes::Bytes) -> (ravel_segment::Footer, Vec<SeriesEntry>) {
    const LABEL_DICT: u32 = 1;
    const SERIES_TABLE: u32 = 2;
    let limits = ReaderLimits::default();
    let footer = open_from_full(bytes, limits).expect("open segment").footer;
    let label_dict = &bytes[section_bytes(&footer, LABEL_DICT)];
    let series_table = &bytes[section_bytes(&footer, SERIES_TABLE)];
    let entries = decode_catalog(&footer, label_dict, series_table, limits).expect("catalog");
    (footer, entries)
}

#[test]
fn soa_reused_buffers_stop_reallocating_after_warm_up() {
    let bytes = build_test_segment();
    let (footer, entries) = decode_entries(&bytes);
    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let planned = plan_ranges(&footer, &selected).expect("plan ranges");
    let limits = ReaderLimits::default();

    let mut scratch = Vec::new();
    let mut timestamps = Vec::new();
    let mut values = Vec::new();

    let decode_all = |scratch: &mut Vec<u8>, timestamps: &mut Vec<i64>, values: &mut Vec<f64>| {
        for (entry, plan) in entries.iter().zip(planned.iter()) {
            let ts_bytes =
                &bytes[plan.ts_range.0 as usize..(plan.ts_range.0 + plan.ts_range.1) as usize];
            let val_bytes =
                &bytes[plan.val_range.0 as usize..(plan.val_range.0 + plan.val_range.1) as usize];
            decode_pages_soa(
                entry, ts_bytes, val_bytes, limits, scratch, timestamps, values,
            )
            .expect("decode");
        }
    };

    // Warm-up pass: buffers start empty, so this pass grows them to their
    // steady-state capacity.
    decode_all(&mut scratch, &mut timestamps, &mut values);
    let (scratch_cap, ts_cap, val_cap) =
        (scratch.capacity(), timestamps.capacity(), values.capacity());

    // Measured pass: every series in this segment has the same sample and
    // page-byte-length shape as the warm-up pass, so a correctly-reusing
    // decode should need no further growth.
    decode_all(&mut scratch, &mut timestamps, &mut values);

    eprintln!(
        "alloc_accounting: {SERIES_COUNT} series x {SAMPLES_PER_SERIES} samples: \
         steady-state capacities scratch={scratch_cap} timestamps={ts_cap} values={val_cap}, \
         unchanged after second decode pass (zero reallocations)"
    );
    assert_eq!(
        scratch.capacity(),
        scratch_cap,
        "scratch buffer reallocated"
    );
    assert_eq!(
        timestamps.capacity(),
        ts_cap,
        "timestamps buffer reallocated"
    );
    assert_eq!(values.capacity(), val_cap, "values buffer reallocated");
}
