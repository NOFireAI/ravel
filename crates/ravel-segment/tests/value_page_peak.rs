//! An ALP or GCD-FOR value page decodes within the 8 bytes per sample the
//! query fetcher charges for its values (issue #4198). Both codecs model each
//! value as an integer; the decode writes those integers straight into the
//! output vector's tail and converts them in place, so it allocates the
//! output's growth and nothing else.
//!
//! The figure is jemalloc's per-thread cumulative allocated-bytes counter
//! across one `decode_run_pages_soa` call whose timestamp vector and scratch
//! buffer are already sized, so the only allocation the call needs is the
//! value vector. A cumulative count bounds the peak from above: an
//! intermediate buffer the decode frees again still counts.
#![cfg(not(target_env = "msvc"))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInput, ValPageKind,
    decode_catalog_v5, decode_run_pages_soa, open_from_full, page_payload_len,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};
use tikv_jemalloc_ctl::thread;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const TS_PAGES: u32 = 3;
const VAL_PAGES: u32 = 4;
const START: i64 = 1_700_000_000_000_000_000;
const STEP: i64 = 15_000_000_000;

/// 4096 samples: their 32 KiB of values is a jemalloc size class of its own,
/// so the allocator rounds nothing up and the bound is the exact charge.
const SAMPLES: usize = 4096;

/// What the fetcher charges per decoded value (`decoded_units_len`).
const CHARGED_VALUE_BYTES: u64 = 8;

fn series(idx: u8, values: &[f64]) -> SeriesInput {
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: format!("m{idx}"),
    }])
    .expect("labels");
    SeriesInput {
        series_id: SeriesId([idx; 16]),
        labels,
        samples: values
            .iter()
            .enumerate()
            .map(|(i, &value)| Sample {
                ts_ns: START + i as i64 * STEP,
                value,
            })
            .collect(),
    }
}

fn xorshift(mut s: u64) -> impl FnMut() -> u64 {
    move || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    }
}

/// Decimal gauge values plus one NaN: no single exponent fits every value,
/// so GCD-FOR falls back to raw and ALP, which stores the NaN as its one
/// exception, is the smallest.
fn alp_values() -> Vec<f64> {
    let mut rng = xorshift(0x9e37_79b9_7f4a_7c15);
    let mut values: Vec<f64> = (0..SAMPLES)
        .map(|_| (rng() % 1_000_000) as f64 / 100.0)
        .collect();
    values[17] = f64::from_bits(0x7ff8_0000_0000_00ab);
    values
}

/// A counter rising in random multiples of 10: its deltas share the GCD 1000
/// at exponent 2, so GCD-FOR packs them in 4 bits each.
fn gcd_values() -> Vec<f64> {
    let mut rng = xorshift(0x2545_f491_4f6c_dd1d);
    let mut acc = 5_000_000i64;
    (0..SAMPLES)
        .map(|_| {
            acc += (rng() % 16) as i64 * 1000;
            acc as f64 / 100.0
        })
        .collect()
}

fn thread_allocated() -> u64 {
    thread::allocatedp::read().expect("thread.allocatedp").get()
}

#[test]
fn integer_model_value_pages_decode_within_their_charge() {
    let alp = alp_values();
    let gcd = gcd_values();
    let written = SegmentWriter::write(
        vec![series(1, &alp), series(2, &gcd)],
        SegmentIdentity {
            tenant_hash: [0x2A; 16],
            shard: 2,
            writer_id: "value-page-peak".to_string(),
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
    let limits = ReaderLimits::default();
    let footer = open_from_full(obj, limits).expect("open object").footer;
    let entries = decode_catalog_v5(&footer, obj, limits).expect("decode catalog");
    let section = |kind| footer.sections.iter().find(|s| s.kind == kind).unwrap();
    let (ts_section, val_section) = (section(TS_PAGES), section(VAL_PAGES));

    let mut seen = Vec::new();
    for entry in &entries {
        assert_eq!(entry.runs.len(), 1, "one run per series");
        let run = &entry.runs[0];
        assert_eq!(run.sample_count as usize, SAMPLES);
        let page = |offset: u64, (off, len): (u64, u64)| {
            let start = (offset + off) as usize;
            &obj[start..start + len as usize]
        };
        let ts_page = page(ts_section.offset, run.ts_page);
        let val_page = page(val_section.offset, run.val_page);

        let payload = |page| page_payload_len(page).expect("page header") as usize;
        let mut scratch = Vec::with_capacity(payload(ts_page).max(payload(val_page)));
        let mut timestamps = Vec::with_capacity(SAMPLES);
        let mut values = Vec::new();
        let before = thread_allocated();
        let kind = decode_run_pages_soa(
            &entry.entry.series_id,
            run,
            ts_page,
            val_page,
            limits,
            &mut scratch,
            &mut timestamps,
            &mut values,
        )
        .expect("decode run");
        let allocated = thread_allocated() - before;
        let charged = CHARGED_VALUE_BYTES * SAMPLES as u64;
        assert!(
            allocated <= charged,
            "{kind:?} page: the decode allocated {allocated} bytes against a charge of {charged}"
        );
        let expected = if kind == ValPageKind::Alp { &alp } else { &gcd };
        assert_eq!(
            values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            expected.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            "{kind:?} values round-trip bit-exactly"
        );
        seen.push((entry.entry.series_id.0[0], kind));
    }
    seen.sort_by_key(|(idx, _)| *idx);
    assert_eq!(
        seen,
        vec![(1, ValPageKind::Alp), (2, ValPageKind::GcdDeltaFor)],
        "the fixture stores one page of each integer-model codec"
    );
}
