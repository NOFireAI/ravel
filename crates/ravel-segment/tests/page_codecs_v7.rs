//! End-to-end reachability for the RSEG v7 page codecs (ADR-0092 decision 6,
//! issue #312). Every ingest flush writes value and timestamp pages through
//! `SegmentWriter::write`, so the per-page selection runs on every write. This
//! test drives that whole path -- write a real object, open it, decode its
//! catalog, slice each run's TS/VAL page out of its section, and decode it back
//! -- and asserts that an integer-model VAL codec is actually selected and
//! stored, that a noisy series falls back, and that every value round-trips
//! bit-exactly (by `f64::to_bits`, so NaN payloads, -0.0, denormals, and both
//! infinities are covered).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInput, ValPageKind,
    decode_catalog_v5, decode_run_pages_soa, open_from_full,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

const TS_PAGES: u32 = 3;
const VAL_PAGES: u32 = 4;
const TS_DELTA_VARINT: u8 = 1;
const TS_GCD_I64: u8 = 2;
const VAL_GORILLA: u8 = 16;
const VAL_RAW_F64: u8 = 17;
const VAL_ALP: u8 = 18;
const VAL_GCD_DELTA_FOR: u8 = 19;

const START: i64 = 1_700_000_000_000_000_000;
const MS: i64 = 1_000_000;
const STEP_MS: i64 = 15_000_000_000;

fn identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0x2A; 16],
        shard: 2,
        writer_id: "page-codecs-v7-test".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 100_000,
    }
}

fn series(idx: u8, metric: &str, ts: &[i64], values: &[f64]) -> SeriesInput {
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("labels");
    let samples: Vec<Sample> = ts
        .iter()
        .zip(values)
        .map(|(&ts_ns, &value)| Sample { ts_ns, value })
        .collect();
    SeriesInput {
        series_id: SeriesId([idx; 16]),
        labels,
        samples,
    }
}

fn bits(v: &[f64]) -> Vec<u64> {
    v.iter().map(|x| x.to_bits()).collect()
}

#[test]
fn write_path_selects_and_round_trips_the_v7_page_codecs() {
    let n = 240usize;
    let ts: Vec<i64> = (0..n).map(|i| START + i as i64 * STEP_MS).collect();

    // A pseudo-random ms jitter (deterministic xorshift) for the GCD-stack case.
    let mut s: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut rng = || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    };
    let ts_jitter: Vec<i64> = (0..n)
        .map(|i| START + i as i64 * STEP_MS + ((rng() % 401) as i64 - 200) * MS)
        .collect();

    let constant: Vec<f64> = vec![42.5; n];
    let decimal: Vec<f64> = (0..n).map(|i| (30_000 + (i % 47)) as f64 / 100.0).collect();
    // Large-magnitude random floats (> 1e19) that no decimal exponent within
    // the i64 domain can model, plus a NaN payload / -0.0 / infinities / a
    // denormal: the integer-model codecs must fall back to the incumbent, and
    // every bit must still survive.
    let mut noisy: Vec<f64> = (0..n)
        .map(|_| (rng() % 1_000_000) as f64 * 1e18 + (rng() % 1000) as f64 + 0.5)
        .collect();
    noisy[0] = f64::from_bits(0x7ff8_0000_0000_00ab);
    noisy[1] = -0.0;
    noisy[2] = f64::INFINITY;
    noisy[3] = f64::NEG_INFINITY;
    noisy[4] = f64::from_bits(1);

    // series 1: regular ts + constant values -> TS_DELTA_VARINT + integer model
    // series 2: jittered ts + decimal values -> TS_GCD_I64 + integer model
    // series 3: regular ts + noisy values     -> fallback VAL codec, exact
    let batch = vec![
        series(0x01, "const", &ts, &constant),
        series(0x02, "decim", &ts_jitter, &decimal),
        series(0x03, "noisy", &ts, &noisy),
    ];
    let written = SegmentWriter::write(batch, identity(), bounds()).expect("write object");
    let obj = written.bytes.as_ref();

    let loc = open_from_full(obj, ReaderLimits::default()).expect("open object");
    let footer = &loc.footer;
    let entries = decode_catalog_v5(footer, obj, ReaderLimits::default()).expect("decode catalog");

    let ts_section = footer.sections.iter().find(|s| s.kind == TS_PAGES).unwrap();
    let val_section = footer
        .sections
        .iter()
        .find(|s| s.kind == VAL_PAGES)
        .unwrap();

    let mut seen = std::collections::HashMap::new();
    for e in &entries {
        let run = &e.runs[0];
        let (ts_off, ts_len) = run.ts_page;
        let ts_abs = (ts_section.offset + ts_off) as usize;
        let ts_page = &obj[ts_abs..ts_abs + ts_len as usize];
        let (v_off, v_len) = run.val_page;
        let v_abs = (val_section.offset + v_off) as usize;
        let val_page = &obj[v_abs..v_abs + v_len as usize];

        let mut scratch = Vec::new();
        let mut ts_out = Vec::new();
        let mut vals_out = Vec::new();
        let kind = decode_run_pages_soa(
            &e.entry.series_id,
            run,
            ts_page,
            val_page,
            ReaderLimits::default(),
            &mut scratch,
            &mut ts_out,
            &mut vals_out,
        )
        .expect("decode run");
        seen.insert(
            e.entry.series_id.0[0],
            (ts_page[0], val_page[0], kind, ts_out, vals_out),
        );
    }

    // series 1: exactly-regular ts keeps the incumbent; constant values take an
    // adopted integer-model codec.
    let (ts_enc, val_enc, kind, ts_out, vals_out) = &seen[&0x01];
    assert_eq!(*ts_enc, TS_DELTA_VARINT, "regular ts keeps TS_DELTA_VARINT");
    assert!(
        *val_enc == VAL_ALP || *val_enc == VAL_GCD_DELTA_FOR,
        "constant series must select an integer-model VAL codec, got {val_enc}"
    );
    assert!(matches!(kind, ValPageKind::Alp | ValPageKind::GcdDeltaFor));
    assert_eq!(ts_out, &ts);
    assert_eq!(bits(vals_out), bits(&constant));

    // series 2: jittered ms ts selects the GCD stack; decimal values take an
    // adopted codec.
    let (ts_enc, val_enc, _, ts_out, vals_out) = &seen[&0x02];
    assert_eq!(*ts_enc, TS_GCD_I64, "jittered ms ts must select TS_GCD_I64");
    assert!(
        *val_enc == VAL_ALP || *val_enc == VAL_GCD_DELTA_FOR,
        "decimal gauge must select an integer-model VAL codec, got {val_enc}"
    );
    assert_eq!(ts_out, &ts_jitter);
    assert_eq!(bits(vals_out), bits(&decimal));

    // series 3: noisy values fall back to the incumbent VAL codec but still
    // decode bit-exactly, including the NaN payload / -0.0 / infinities.
    let (_, val_enc, kind, _, vals_out) = &seen[&0x03];
    assert!(
        *val_enc == VAL_GORILLA || *val_enc == VAL_RAW_F64,
        "noisy floats must keep the incumbent VAL codec, got {val_enc}"
    );
    assert!(matches!(kind, ValPageKind::Gorilla | ValPageKind::RawF64));
    assert_eq!(bits(vals_out), bits(&noisy));
}
