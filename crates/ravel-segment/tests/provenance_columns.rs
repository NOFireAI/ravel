//! Integration tests for the optional per-sample dedup provenance columns
//! (ADR-0092 decision 1): the SERIES_META extension blocks that a run-merged
//! run carries, storable through the real writer and readable through the real
//! reader.
//!
//! The central properties:
//! - Per-sample provenance survives a write-then-read round trip bit-exactly,
//!   over runs with and without the columns in the same object
//!   (`provenance_round_trips`).
//! - A run without the columns costs zero extra bytes: an all-`None`
//!   provenance write is byte-identical to the ordinary `write_v5`
//!   (`absent_columns_are_free`).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use proptest::prelude::*;
use ravel_segment::{
    CompactionMetaV4, IngestBounds, ReaderLimits, RunInputV7, SampleProvenance, SegmentIdentity,
    SegmentWriter, SeriesInputV4, SeriesInputV7, SeriesValues, V5_SPARSE_THRESHOLD,
    decode_catalog_v5, encode_run_v4, open_from_full,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

fn identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0x2A; 16],
        shard: 2,
        writer_id: "provenance-test".to_string(),
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

fn meta() -> CompactionMetaV4 {
    CompactionMetaV4 {
        ingest_hour_bucket: 5,
        input_set_hash: [0x33; 32],
        part_index: 0,
        level: 1,
    }
}

fn labels(name: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: name.to_string(),
    }])
    .expect("valid labels")
}

fn scalar_samples(vals: &[(i64, f64)]) -> SeriesValues {
    SeriesValues::Scalar(
        vals.iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    )
}

/// One run's plain page bytes plus its optional per-sample provenance.
fn run_with_prov(
    id: &SeriesId,
    created: i64,
    epoch: u64,
    seq: u64,
    vals: &[(i64, f64)],
    prov: Option<Vec<SampleProvenance>>,
) -> RunInputV7 {
    let run = encode_run_v4(id, created, epoch, seq, &scalar_samples(vals)).expect("frame run");
    RunInputV7 {
        run,
        provenance: prov,
    }
}

/// A run without columns is free: the all-`None` provenance write emits exactly
/// the bytes `write_v5` would for the same runs, so the optional columns add
/// nothing when unused (measured against the ordinary writer, which is the base
/// commit's behavior unchanged).
#[test]
fn absent_columns_are_free() {
    let id0 = SeriesId([1u8; 16]);
    let id1 = SeriesId([2u8; 16]);

    let plain = vec![
        SeriesInputV4 {
            series_id: id0,
            labels: labels("a"),
            runs: vec![
                encode_run_v4(&id0, 10, 1, 1, &scalar_samples(&[(100, 1.0), (200, 2.0)]))
                    .expect("run"),
            ],
        },
        SeriesInputV4 {
            series_id: id1,
            labels: labels("b"),
            runs: vec![encode_run_v4(&id1, 20, 1, 2, &scalar_samples(&[(150, 3.0)])).expect("run")],
        },
    ];
    let baseline = SegmentWriter::write_v5(plain, identity(), bounds(), meta()).expect("write_v5");

    let with_prov = vec![
        SeriesInputV7 {
            series_id: id0,
            labels: labels("a"),
            runs: vec![run_with_prov(
                &id0,
                10,
                1,
                1,
                &[(100, 1.0), (200, 2.0)],
                None,
            )],
        },
        SeriesInputV7 {
            series_id: id1,
            labels: labels("b"),
            runs: vec![run_with_prov(&id1, 20, 1, 2, &[(150, 3.0)], None)],
        },
    ];
    let via_v7 = SegmentWriter::write_v7_with_provenance(
        with_prov,
        identity(),
        bounds(),
        meta(),
        Vec::new(),
    )
    .expect("write_v7");

    assert_eq!(
        baseline.bytes, via_v7.bytes,
        "an all-None provenance write must be byte-identical to write_v5, so absent columns are free"
    );
}

/// A `SampleProvenance` strategy over small non-negative ranges (kept away from
/// i64 extremes so the base-relative delta is unambiguous).
fn prov_strategy() -> impl Strategy<Value = SampleProvenance> {
    (0i64..10_000, 0u64..1_000, 0u64..1_000, 0u32..64).prop_map(
        |(created_unix_ns, writer_epoch, writer_seq, in_page_index)| SampleProvenance {
            created_unix_ns,
            writer_epoch,
            writer_seq,
            in_page_index,
        },
    )
}

/// One run: 1..=5 samples (ascending ts) and an optional provenance column of
/// matching length.
fn run_strategy() -> impl Strategy<Value = (Vec<(i64, f64)>, Option<Vec<SampleProvenance>>)> {
    (1usize..=5)
        .prop_flat_map(|n| {
            let samples =
                prop::collection::vec((0i64..5_000, any::<u64>().prop_map(f64::from_bits)), n);
            let prov = prop::option::of(prop::collection::vec(prov_strategy(), n));
            (samples, prov)
        })
        .prop_map(|(mut samples, prov)| {
            // Ascending ts, stable, so the writer's page order is deterministic.
            samples.sort_by_key(|(ts, _)| *ts);
            (samples, prov)
        })
}

/// A merged L1 object over a large tenant is the sparse case (>= 4096 series,
/// chunked SERIES_META). This proves the per-sample provenance columns survive
/// a write-then-read round trip through that form too, not just the whole-
/// section form: without the sparse-form support (ADR-0092 decision 1, issue
/// #315) `write_v7_with_provenance` failed closed here, so L1 compaction broke
/// for every tenant above the threshold.
#[test]
fn provenance_round_trips_through_sparse_chunked_form() {
    const N: usize = V5_SPARSE_THRESHOLD as usize + 37;

    // A handful of series carry a merged (multi-sample) run with a per-sample
    // provenance column; the rest are single-sample runs with no column. The
    // flagged indices are spread across several 512-wide chunks (and include the
    // last series) so the chunked rebuild is exercised end to end.
    let flagged: [usize; 5] = [3, 511, 512, 2000, N - 1];
    let column = vec![
        SampleProvenance {
            created_unix_ns: 40,
            writer_epoch: 7,
            writer_seq: 11,
            in_page_index: 0,
        },
        SampleProvenance {
            created_unix_ns: 55,
            writer_epoch: 7,
            writer_seq: 12,
            in_page_index: 2,
        },
        SampleProvenance {
            created_unix_ns: 55,
            writer_epoch: 8,
            writer_seq: 1,
            in_page_index: 1,
        },
    ];

    let mut series_v7 = Vec::with_capacity(N);
    for i in 0..N {
        // Big-endian index ids so the writer's ascending-id sort matches input
        // order, keeping the flagged indices' chunk placement predictable.
        let mut raw = [0u8; 16];
        raw[8..].copy_from_slice(&(i as u64).to_be_bytes());
        let id = SeriesId(raw);
        let name = format!("m{i:05}");
        if flagged.contains(&i) {
            series_v7.push(SeriesInputV7 {
                series_id: id,
                labels: labels(&name),
                runs: vec![run_with_prov(
                    &id,
                    40,
                    1,
                    1,
                    &[(100, 1.0), (200, 2.0), (300, 3.0)],
                    Some(column.clone()),
                )],
            });
        } else {
            series_v7.push(SeriesInputV7 {
                series_id: id,
                labels: labels(&name),
                runs: vec![run_with_prov(&id, 40, 1, 1, &[(100 + i as i64, 1.0)], None)],
            });
        }
    }

    let written =
        SegmentWriter::write_v7_with_provenance(series_v7, identity(), bounds(), meta(), Vec::new())
            .expect("write_v7_with_provenance over a sparse tenant");

    let loc = open_from_full(&written.bytes, ReaderLimits::default()).expect("open");
    // Sanity: this really is the sparse (chunked) form, not the whole-section
    // one, so the assertion below exercises the chunk rebuild.
    assert!(loc.footer.series_count >= V5_SPARSE_THRESHOLD);
    let entries = decode_catalog_v5(&loc.footer, &written.bytes, ReaderLimits::default())
        .expect("decode sparse catalog");
    assert_eq!(entries.len(), N);

    for i in 0..N {
        let mut raw = [0u8; 16];
        raw[8..].copy_from_slice(&(i as u64).to_be_bytes());
        let entry = entries
            .iter()
            .find(|e| e.entry.series_id.0 == raw)
            .expect("series present");
        assert_eq!(entry.runs.len(), 1, "one run per series");
        if flagged.contains(&i) {
            assert_eq!(entry.per_sample_provenance.len(), 1);
            let got = entry.per_sample_provenance[0]
                .as_ref()
                .expect("flagged run keeps its per-sample column");
            assert_eq!(got, &column, "sparse-form provenance must round-trip exact");
        } else {
            // An unflagged run reads back as run-wide: either no column in its
            // chunk (empty vector) or an explicit `None` when its chunk also
            // carried a flagged run. Both mean the same dedup key.
            assert!(entry.per_sample_provenance.iter().all(|p| p.is_none()));
        }
    }
}

proptest! {
    /// Per-sample provenance survives write-then-read bit-exactly, across runs
    /// that carry it and runs that do not, in the same object. Values are
    /// compared by f64 bit pattern so a NaN payload never masks a difference.
    #[test]
    fn provenance_round_trips(
        runs in prop::collection::vec(run_strategy(), 1..=4),
    ) {
        // One series per run, ascending ids and ascending run-wide created so
        // the writer's series-by-id and runs-by-key sorts preserve input order,
        // making decoded[i] correspond to input run i.
        let mut series_v7 = Vec::new();
        let mut expected: Vec<(SeriesId, Option<Vec<SampleProvenance>>, usize)> = Vec::new();
        for (i, (samples, prov)) in runs.iter().enumerate() {
            let id = SeriesId([(i as u8) + 1; 16]);
            let created = 10 + i as i64;
            series_v7.push(SeriesInputV7 {
                series_id: id,
                labels: labels(&format!("m{i}")),
                runs: vec![run_with_prov(&id, created, 1, 1, samples, prov.clone())],
            });
            expected.push((id, prov.clone(), samples.len()));
        }

        let written = SegmentWriter::write_v7_with_provenance(
            series_v7,
            identity(),
            bounds(),
            meta(),
            Vec::new(),
        )
        .expect("write_v7_with_provenance");

        let loc = open_from_full(&written.bytes, ReaderLimits::default()).expect("open");
        let entries = decode_catalog_v5(&loc.footer, &written.bytes, ReaderLimits::default())
            .expect("decode catalog");

        let any_prov = expected.iter().any(|(_, p, _)| p.is_some());
        for (id, exp_prov, n) in &expected {
            let entry = entries
                .iter()
                .find(|e| e.entry.series_id == *id)
                .expect("series present");
            prop_assert_eq!(entry.runs.len(), 1);
            if !any_prov {
                // No run in the whole object carries columns: the extension is
                // absent, so the decoded per-run vector is empty.
                prop_assert!(entry.per_sample_provenance.is_empty());
                continue;
            }
            prop_assert_eq!(entry.per_sample_provenance.len(), 1);
            match exp_prov {
                None => prop_assert!(entry.per_sample_provenance[0].is_none()),
                Some(col) => {
                    let got = entry.per_sample_provenance[0]
                        .as_ref()
                        .expect("column present");
                    prop_assert_eq!(got.len(), *n);
                    prop_assert_eq!(got, col);
                }
            }
        }
    }
}
