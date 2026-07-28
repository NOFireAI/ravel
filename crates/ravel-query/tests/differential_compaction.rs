//! Differential proof for P5 of docs/compaction-retention-plan.md (issue
//! #112): query-over-inputs must equal query-over-L1 bit-for-bit, and also
//! equal query-over-(L1 + any input subset). Drives P4's real compactor
//! (ravel-maintain) to produce `write_v4` L1 parts from adversarial L0
//! populations, then compares the deduped sample streams the fetcher +
//! engine order would produce.
//!
//! The comparison uses an in-test dedup oracle that is a deterministic
//! function of the per-(series, timestamp) candidate multiset under the
//! ADR-0010 §5 total order `(created_unix_ns, writer_epoch, writer_seq,
//! in-page index)` then value bits (exactly `engine::is_greater`). Because
//! it is a pure function of that multiset, equal fetched candidate sets
//! yield equal outputs and unequal ones do not: it proves the fetcher's
//! per-run v4 emission and the resolver's include-parts/exclude-inputs
//! rules reproduce the pre-compaction candidate set exactly.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::*;
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef};
use ravel_commit::keys::commit_key_for_record;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::{
    BuildIdentity, Clock, CompactionInput, MaintainConfig, PublishInput, PublishOutcome,
    input_set_hash, merge_and_build, publish, read_input, sort_inputs_canonically,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{FetchedSeriesSoa, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, SeriesInputV3};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const HOUR: u32 = 500_000;
const SHARD: u32 = 0;

fn tenant_hash() -> TenantHash {
    TenantHash([0x7c; 16])
}

fn hour_start() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn now_ns() -> i64 {
    hour_start() + 30 * 60_000_000_000
}

fn full_range() -> TimeRange {
    TimeRange {
        start_ns: hour_start(),
        end_ns: hour_start() + NS_PER_HOUR,
    }
}

fn config() -> CatalogConfig {
    CatalogConfig {
        shard_count: 1,
        ..Default::default()
    }
}

/// Fixed injected clock: publish's abandonment check compares `now` to the
/// run's `started_ns`, so a constant clock with `started_ns == now` is never
/// abandoned.
struct FixedClock(i64);
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

fn labels_for(idx: u8) -> (String, LabelSet) {
    let metric = format!("m{idx}");
    let labels = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.clone(),
    }])
    .expect("labels");
    (metric, labels)
}

fn series_input(idx: u8, samples: Vec<Sample>) -> SeriesInput {
    let (metric, labels) = labels_for(idx);
    let series_id = SeriesId::compute(&TenantId::new("t"), &metric, &labels).expect("series id");
    SeriesInput {
        series_id,
        labels,
        samples,
    }
}

/// One L0 flush: a writer id, its provenance, and its `(series, ts, bits)`
/// samples. `ts` and `created_ns` are offsets within `HOUR`.
#[derive(Debug, Clone)]
struct WriterSpec {
    created_offset_ns: i64,
    seq: u64,
    samples: Vec<(u8, i64, u64)>,
}

async fn build_l0(store: &dyn ObjectStoreBackend, spec: &WriterSpec) -> CompactionInput {
    let writer_id = Uuid::new_v4();
    let mut by_series: BTreeMap<u8, Vec<Sample>> = BTreeMap::new();
    for &(idx, ts_off, bits) in &spec.samples {
        by_series.entry(idx).or_default().push(Sample {
            ts_ns: hour_start() + ts_off,
            value: f64::from_bits(bits),
        });
    }
    let series: Vec<SeriesInput> = by_series
        .into_iter()
        .map(|(idx, samples)| series_input(idx, samples))
        .collect();

    let identity = SegmentIdentity {
        tenant_hash: tenant_hash().0,
        shard: SHARD,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: spec.seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 10,
    };
    let written = SegmentWriter::write_v2(series, identity, bounds).expect("write v2");

    let record = record::build(NewCommitRecord {
        tenant_hash: tenant_hash(),
        signal: Signal::Metrics,
        shard: SHARD,
        writer_id,
        writer_epoch: 1,
        writer_seq: spec.seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 10,
        segment_format_version: 2,
        created_unix_ns: hour_start() + spec.created_offset_ns,
        ingest_hour_bucket: HOUR,
    })
    .expect("commit record");

    store
        .put(
            &record.object_key,
            written.bytes,
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put data object");
    let key = commit_key_for_record(&record).expect("commit key");
    store
        .put(
            &key,
            record::encode(&record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put commit record");
    CompactionInput { key, record }
}

/// Run P4's compactor over `inputs` on `store`, publishing the record and
/// L1 parts. Returns the published record's key for later inspection.
async fn compact(store: &dyn ObjectStoreBackend, inputs: Vec<CompactionInput>) {
    let sorted = sort_inputs_canonically(inputs).expect("sort");
    let hash = input_set_hash(&sorted).expect("hash");
    let mut decoded = Vec::new();
    for input in &sorted {
        decoded.push(read_input(store, input).await.expect("read input"));
    }
    let parts = merge_and_build(
        &decoded,
        &BuildIdentity {
            tenant_hash: tenant_hash().0,
            shard: SHARD,
        },
        HOUR,
        hash,
        256 << 20,
    )
    .expect("merge");
    let clock = FixedClock(now_ns());
    let cfg = MaintainConfig::default();
    let outcome = publish(
        store,
        &clock,
        &cfg,
        &PublishInput {
            tenant_hash: tenant_hash(),
            signal: Signal::Metrics,
            shard: SHARD,
            ingest_hour_bucket: HOUR,
            inputs: &sorted,
            input_set_hash: hash,
            parts: &parts,
            started_ns: now_ns(),
        },
    )
    .await
    .expect("publish");
    assert_eq!(outcome, PublishOutcome::Published);
}

/// Resolve and fetch every segment (L0 or L1) in the snapshot, returning the
/// per-run SoA the engine would merge.
async fn fetch_all(store: Arc<MemoryStore>) -> Vec<FetchedSeriesSoa> {
    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let snapshot = catalog
        .resolve(&tenant_hash(), Signal::Metrics, full_range(), &[], now_ns())
        .await
        .expect("resolve");
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(backend);
    let mut out = Vec::new();
    for seg in &snapshot.segments {
        let (runs, _stats) = fetcher
            .fetch_soa(tenant_hash(), seg, &[])
            .await
            .expect("fetch_soa");
        out.extend(runs);
    }
    out
}

/// Deterministic dedup oracle: for each (series, ts), pick the candidate that
/// is greatest under `(created_unix_ns, writer_epoch, writer_seq, in-page
/// index)` then value bits. A pure function of the candidate multiset.
fn dedup(runs: &[FetchedSeriesSoa]) -> BTreeMap<[u8; 16], Vec<(i64, u64)>> {
    type Priority = (i64, u64, u64, u32);
    let mut candidates: BTreeMap<[u8; 16], BTreeMap<i64, (Priority, u64)>> = BTreeMap::new();
    for run in runs {
        let series = candidates.entry(run.series_id.0).or_default();
        for (i, (ts, val)) in run.timestamps.iter().zip(run.values.iter()).enumerate() {
            let priority = (
                run.created_unix_ns,
                run.writer_epoch,
                run.writer_seq,
                i as u32,
            );
            let key = (priority, val.to_bits());
            series
                .entry(*ts)
                .and_modify(|best| {
                    if key > *best {
                        *best = key;
                    }
                })
                .or_insert(key);
        }
    }
    candidates
        .into_iter()
        .map(|(sid, by_ts)| {
            (
                sid,
                by_ts
                    .into_iter()
                    .map(|(ts, (_p, bits))| (ts, bits))
                    .collect(),
            )
        })
        .collect()
}

/// Populate a fresh store with `specs`' L0 flushes and return it plus the
/// inputs.
async fn seed_store(specs: &[WriterSpec]) -> (Arc<MemoryStore>, Vec<CompactionInput>) {
    let store = Arc::new(MemoryStore::new());
    let mut inputs = Vec::new();
    for spec in specs {
        inputs.push(build_l0(store.as_ref(), spec).await);
    }
    (store, inputs)
}

/// The core differential property: for any adversarial L0 population,
/// query-over-inputs == query-over-L1 == query-over-(L1 + inputs still
/// present, i.e. a partially-swept bucket), bit-for-bit.
async fn assert_differential(specs: &[WriterSpec]) {
    // query-over-inputs: L0 only.
    let (l0_store, _) = seed_store(specs).await;
    let over_inputs = dedup(&fetch_all(l0_store).await);
    assert!(!over_inputs.is_empty(), "test population must be non-empty");

    // query-over-(L1 + all inputs): compact in place, inputs still visible.
    let (mixed_store, mixed_inputs) = seed_store(specs).await;
    compact(mixed_store.as_ref(), mixed_inputs).await;
    let over_l1_plus_inputs = dedup(&fetch_all(mixed_store).await);

    // query-over-L1: compact, then sweep every input (records + data objects).
    let (l1_store, l1_inputs) = seed_store(specs).await;
    let sweep_keys: Vec<(String, String)> = l1_inputs
        .iter()
        .map(|i| (i.key.clone(), i.record.object_key.clone()))
        .collect();
    compact(l1_store.as_ref(), l1_inputs).await;
    for (commit_key, data_key) in &sweep_keys {
        l1_store.delete(commit_key).await.expect("delete commit");
        l1_store.delete(data_key).await.expect("delete data");
    }
    let over_l1 = dedup(&fetch_all(l1_store).await);

    assert_eq!(
        over_inputs, over_l1,
        "query-over-inputs must equal query-over-L1 bit-for-bit"
    );
    assert_eq!(
        over_inputs, over_l1_plus_inputs,
        "overlap harmlessness: query-over-(L1 + inputs) must equal query-over-inputs"
    );
}

#[tokio::test]
async fn differential_adversarial_fixed() {
    // Three writers whose provenance fully ties (same created_unix_ns, epoch,
    // seq), so cross-writer duplicate timestamps resolve on the value-bits
    // final tiebreak. Covers NaN, -0.0, and the maximal bit pattern.
    let nan = f64::NAN.to_bits();
    let neg_zero = (-0.0f64).to_bits();
    let max_bits = u64::MAX;
    let one = 1.0f64.to_bits();
    let zero = 0.0f64.to_bits();
    let two = 2.0f64.to_bits();

    let specs = vec![
        WriterSpec {
            created_offset_ns: 0,
            seq: 1,
            samples: vec![(0, 10, one), (0, 20, nan), (1, 5, neg_zero)],
        },
        WriterSpec {
            created_offset_ns: 0,
            seq: 1,
            samples: vec![(0, 10, neg_zero), (0, 20, zero)],
        },
        WriterSpec {
            created_offset_ns: 0,
            seq: 1,
            samples: vec![(0, 10, max_bits), (0, 30, two)],
        },
    ];
    assert_differential(&specs).await;
}

#[tokio::test]
async fn differential_distinct_provenance() {
    // Distinct created_unix_ns per writer: the later flush wins duplicate
    // timestamps regardless of value.
    let specs = vec![
        WriterSpec {
            created_offset_ns: 0,
            seq: 1,
            samples: vec![(0, 10, 2.0f64.to_bits()), (0, 20, (-0.0f64).to_bits())],
        },
        WriterSpec {
            created_offset_ns: 60_000_000_000,
            seq: 2,
            samples: vec![(0, 10, f64::NAN.to_bits()), (0, 20, 1.0f64.to_bits())],
        },
    ];
    assert_differential(&specs).await;
}

fn value_bits() -> impl Strategy<Value = u64> {
    prop::sample::select(vec![
        1.0f64.to_bits(),
        0.0f64.to_bits(),
        (-0.0f64).to_bits(),
        f64::NAN.to_bits(),
        u64::MAX,
        2.0f64.to_bits(),
        (-1.5f64).to_bits(),
    ])
}

fn writer_strategy() -> impl Strategy<Value = WriterSpec> {
    let samples = prop::collection::vec((0u8..3u8, 0i64..6i64, value_bits()), 1..6).prop_map(
        |raw: Vec<(u8, i64, u64)>| {
            // Dedup (series, ts) within one writer: write_v2 requires strictly
            // ordered timestamps within a series, so keep the first bits seen
            // for each (series, ts).
            let mut seen: BTreeMap<(u8, i64), u64> = BTreeMap::new();
            for (idx, ts, bits) in raw {
                seen.entry((idx, ts)).or_insert(bits);
            }
            seen.into_iter()
                .map(|((idx, ts), bits)| (idx, ts, bits))
                .collect::<Vec<_>>()
        },
    );
    // Created offsets and seqs drawn from small sets so provenance ties and
    // distinct orderings both occur.
    (
        prop::sample::select(vec![0i64, 0, 60_000_000_000]),
        prop::sample::select(vec![1u64, 1, 2]),
        samples,
    )
        .prop_map(|(created_offset_ns, seq, samples)| WriterSpec {
            created_offset_ns,
            seq,
            samples,
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Random adversarial L0 populations: 2..=4 writers over 3 series with
    /// cross-writer duplicate timestamps, provenance ties, NaN, and -0.0.
    #[test]
    fn differential_random(specs in prop::collection::vec(writer_strategy(), 2..5)) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(assert_differential(&specs));
    }
}

// --- Mixed v1/v2/v3/v4 snapshot end-to-end ---

async fn build_l0_versioned(
    store: &dyn ObjectStoreBackend,
    version: u8,
    seq: u64,
    idx: u8,
    ts_off: i64,
    value: f64,
) -> CompactionInput {
    let writer_id = Uuid::new_v4();
    let (metric, labels) = labels_for(idx);
    let series_id = SeriesId::compute(&TenantId::new("t"), &metric, &labels).expect("series id");
    let samples = vec![Sample {
        ts_ns: hour_start() + ts_off,
        value,
    }];
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash().0,
        shard: SHARD,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 10,
    };
    let written = match version {
        1 => SegmentWriter::write(
            vec![SeriesInput {
                series_id,
                labels,
                samples,
            }],
            identity,
            bounds,
        ),
        2 => SegmentWriter::write_v2(
            vec![SeriesInput {
                series_id,
                labels,
                samples,
            }],
            identity,
            bounds,
        ),
        3 => SegmentWriter::write_v3(
            vec![SeriesInputV3 {
                series_id,
                labels,
                values: ravel_segment::SeriesValues::Scalar(samples),
            }],
            identity,
            bounds,
        ),
        _ => unreachable!("only v1/v2/v3 L0 here"),
    }
    .expect("write segment");

    let record = record::build(NewCommitRecord {
        tenant_hash: tenant_hash(),
        signal: Signal::Metrics,
        shard: SHARD,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 10,
        segment_format_version: u32::from(version),
        created_unix_ns: hour_start() + 100,
        ingest_hour_bucket: HOUR,
    })
    .expect("commit record");
    store
        .put(
            &record.object_key,
            written.bytes,
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put data");
    let key = commit_key_for_record(&record).expect("commit key");
    store
        .put(
            &key,
            record::encode(&record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put commit record");
    CompactionInput { key, record }
}

#[tokio::test]
async fn mixed_v1_v2_v3_v4_snapshot_end_to_end() {
    let store = Arc::new(MemoryStore::new());

    // v1, v2, v3 L0 segments left uncompacted (distinct series so all survive
    // into the snapshot).
    build_l0_versioned(store.as_ref(), 1, 1, 0, 10, 1.0).await;
    build_l0_versioned(store.as_ref(), 2, 2, 1, 10, -0.0).await;
    build_l0_versioned(store.as_ref(), 3, 3, 2, 10, f64::NAN).await;

    // Two more L0s over a fourth series, compacted into a v4 part.
    let c1 = build_l0_versioned(store.as_ref(), 2, 4, 3, 10, 5.0).await;
    let c2 = build_l0_versioned(store.as_ref(), 2, 5, 3, 20, 6.0).await;
    compact(store.as_ref(), vec![c1, c2]).await;

    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let snapshot = catalog
        .resolve(&tenant_hash(), Signal::Metrics, full_range(), &[], now_ns())
        .await
        .expect("resolve");
    // Three L0 (v1/v2/v3) + one L1 (v4) part; the two compacted L0s are gone.
    assert_eq!(snapshot.segments.len(), 4);
    let l1_count = snapshot
        .segments
        .iter()
        .filter(|s| matches!(s.level, SegmentLevel::L1 { .. }))
        .count();
    assert_eq!(l1_count, 1);

    let merged = dedup(&fetch_all(store).await);
    // Four series present, each decoded from its own trailer version.
    assert_eq!(merged.len(), 4);
    // The v3 NaN series round-trips its exact bit pattern.
    let (_m, nan_labels) = labels_for(2);
    let nan_sid = SeriesId::compute(&TenantId::new("t"), "m2", &nan_labels).unwrap();
    let nan_samples = &merged[&nan_sid.0];
    assert_eq!(nan_samples.len(), 1);
    assert_eq!(nan_samples[0].1, f64::NAN.to_bits());
    // The v4 series carries both compacted runs' samples (ts 10 and 20).
    let (_m3, s3_labels) = labels_for(3);
    let s3_sid = SeriesId::compute(&TenantId::new("t"), "m3", &s3_labels).unwrap();
    let s3_samples: Vec<i64> = merged[&s3_sid.0].iter().map(|(ts, _)| *ts).collect();
    assert_eq!(s3_samples, vec![hour_start() + 10, hour_start() + 20]);
}

// --- Footer identity verification against the compaction record ---

#[tokio::test]
async fn l1_footer_identity_verified_against_record() {
    let store = Arc::new(MemoryStore::new());
    let c1 = build_l0_versioned(store.as_ref(), 2, 1, 0, 10, 1.0).await;
    let c2 = build_l0_versioned(store.as_ref(), 2, 2, 0, 20, 2.0).await;
    compact(store.as_ref(), vec![c1, c2]).await;

    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let snapshot = catalog
        .resolve(&tenant_hash(), Signal::Metrics, full_range(), &[], now_ns())
        .await
        .expect("resolve");
    let part = snapshot
        .segments
        .iter()
        .find(|s| matches!(s.level, SegmentLevel::L1 { .. }))
        .cloned()
        .expect("an L1 part");

    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(backend);

    // The honest ref fetches.
    fetcher
        .fetch_soa(tenant_hash(), &part, &[])
        .await
        .expect("honest L1 part fetches");

    // Tampering the input_set_hash the ref claims (without touching the object)
    // is caught by footer identity verification: the object's v4 footer no
    // longer matches the record identity the ref carries.
    let SegmentLevel::L1 {
        input_set_hash,
        part_index,
    } = part.level.clone()
    else {
        unreachable!()
    };
    let mut bad_hash = input_set_hash;
    bad_hash[0] ^= 0xff;
    let tampered = SegmentRef {
        level: SegmentLevel::L1 {
            input_set_hash: bad_hash,
            part_index,
        },
        ..part.clone()
    };
    let err = fetcher.fetch_soa(tenant_hash(), &tampered, &[]).await;
    assert!(
        err.is_err(),
        "wrong input_set_hash must fail footer identity"
    );

    // Likewise a wrong part_index.
    let tampered_idx = SegmentRef {
        level: SegmentLevel::L1 {
            input_set_hash,
            part_index: part_index + 1,
        },
        ..part.clone()
    };
    assert!(
        fetcher
            .fetch_soa(tenant_hash(), &tampered_idx, &[])
            .await
            .is_err(),
        "wrong part_index must fail footer identity"
    );
}
