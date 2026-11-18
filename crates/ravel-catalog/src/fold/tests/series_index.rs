//! ADR-4146 W1: the metrics series index the fold builds behind
//! `metrics_series_index`.

use super::*;
use crate::snapshot_format::{
    METRICS_CLASS_OBJECTS_FIELD, METRICS_SERIES_CLASS_FIELD, METRICS_SERIES_LABEL_FIELD,
};

/// The fixture's second metric name.
const HTTP_REQUESTS: &str = "http_requests";

/// One series of the fixture: its metric name and labels besides
/// `__name__`.
type SeriesSpec = (&'static str, &'static [(&'static str, &'static str)]);

const A: SeriesSpec = (CPU_SECONDS, &[("instance", "i0"), ("job", "api")]);
const B: SeriesSpec = (CPU_SECONDS, &[("instance", "i1"), ("job", "api")]);
const C: SeriesSpec = (HTTP_REQUESTS, &[("instance", "i0"), ("job", "db")]);
const D: SeriesSpec = (HTTP_REQUESTS, &[("instance", "i2"), ("job", "db")]);
const E: SeriesSpec = (CPU_SECONDS, &[("instance", "i3"), ("job", "api")]);
const F: SeriesSpec = (CPU_SECONDS, &[("instance", "i1"), ("job", "web")]);

/// The fixture's objects, `(writer_seq, shard, series)`, all in
/// [`KIDX_HOUR`]: shard 0 holds A, B, E and F, shard 1 holds C and D. B and
/// F are held by the same two objects, so they share a class.
const OBJECTS: [(u64, u32, &[SeriesSpec]); 6] = [
    (1, 0, &[A, B, F]),
    (2, 0, &[A, B, F]),
    (3, 1, &[C, D]),
    (4, 1, &[C, D]),
    (5, 1, &[C]),
    (6, 0, &[A, E]),
];

/// Samples per series: enough data bytes that the fixture's series leaves
/// stay within the default 1 % cap.
const SAMPLES: usize = 2_000;

fn series_tenant_id() -> TenantId {
    TenantId::new("metrics-series-index")
}

fn series_id_of(spec: &SeriesSpec) -> SeriesId {
    SeriesId::compute(&series_tenant_id(), spec.0, &label_set_of(spec)).expect("series id")
}

fn label_set_of(spec: &SeriesSpec) -> LabelSet {
    let mut all = vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: spec.0.to_string(),
    }];
    all.extend(spec.1.iter().map(|(name, value)| Label {
        name: (*name).to_string(),
        value: (*value).to_string(),
    }));
    LabelSet::new(all).expect("valid labels")
}

/// Publishes one real RSEG object into `hour` on `shard` holding `series`,
/// each with `samples` pseudo-random samples.
async fn publish_series_object(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    writer_seq: u64,
    hour: u32,
    series: &[SeriesSpec],
    samples: usize,
) -> CommitRecord {
    let base_ts = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
    let mut state = writer_seq.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut inputs = Vec::with_capacity(series.len());
    for spec in series {
        let mut points = Vec::with_capacity(samples);
        for i in 0..samples {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            points.push(Sample {
                ts_ns: base_ts + i as i64 * 1_000_000,
                value: (state >> 11) as f64 / (1u64 << 53) as f64,
            });
        }
        inputs.push(SeriesInput {
            series_id: series_id_of(spec),
            labels: label_set_of(spec),
            samples: points,
        });
    }
    let writer_id = Uuid::from_u128(4_146_000 + u128::from(writer_seq));
    let min_ingest_ts_ns = base_ts;
    let max_ingest_ts_ns = base_ts + samples as i64 * 1_000_000;
    let written = SegmentWriter::write(
        inputs,
        SegmentIdentity {
            tenant_hash: tenant().0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq,
        },
        IngestBounds {
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        },
    )
    .expect("write segment");
    let bytes = written.bytes.to_vec();
    let record = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns,
        max_ingest_ts_ns,
        segment_format_version: 1,
        created_unix_ns: base_ts,
        ingest_hour_bucket: hour,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(bytes))
        .await
        .expect("put data object");
    publish::publish(store, &record, &RetryPolicy::default())
        .await
        .expect("publish");
    record
}

async fn publish_series_fixture(store: &dyn ObjectStoreBackend) -> Vec<CommitRecord> {
    let mut records = Vec::new();
    for (seq, shard, series) in OBJECTS {
        records.push(publish_series_object(store, shard, seq, KIDX_HOUR, series, SAMPLES).await);
    }
    records
}

/// Two shards, the leaf build and the label index on, the series index as
/// `series_index` says.
fn series_config(series_index: bool) -> CatalogConfig {
    CatalogConfig {
        build_key_index_leaves: true,
        metrics_label_index: true,
        metrics_series_index: series_index,
        ..config(2)
    }
}

/// The series key of `spec` by the convention the router reads: the first 8
/// bytes of its `SeriesId`, big-endian.
fn skey(spec: &SeriesSpec) -> u64 {
    let id = series_id_of(spec).0;
    u64::from_be_bytes([id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7]])
}

/// A part's three series fields as stored, decoded.
#[derive(Debug, Default, PartialEq, Eq)]
struct SeriesLeaves {
    /// Pair key8 to its posting of series keys.
    label: BTreeMap<[u8; 8], Vec<u64>>,
    /// Series key to its posting of classes.
    class: BTreeMap<u64, Vec<u64>>,
    /// Class to the entry ordinals holding it, each at blocks `[0]`.
    objects: BTreeMap<u64, Vec<u64>>,
    /// Field to the `format_version` of each of its leaves.
    versions: BTreeMap<String, Vec<u32>>,
    /// Field to its leaves' `uncovered_entry_ordinals`.
    uncovered: BTreeMap<String, Vec<u64>>,
}

async fn series_leaves(store: &dyn ObjectStoreBackend, part: &SnapshotPartRef) -> SeriesLeaves {
    let mut out = SeriesLeaves::default();
    for leaf in &part.key_index {
        let bytes = store
            .get(&leaf.key, GetRange::Full)
            .await
            .expect("the leaf HEAD names is stored")
            .data;
        let summary = verify_key_index_leaf(&bytes, &KeyIndexLeafLimits::default())
            .expect("the leaf verifies");
        out.versions
            .entry(leaf.field.clone())
            .or_default()
            .push(summary.header.format_version);
        out.uncovered.insert(
            leaf.field.clone(),
            summary.header.uncovered_entry_ordinals.clone(),
        );
        match leaf.field.as_str() {
            METRICS_SERIES_LABEL_FIELD => {
                for posting in summary.postings {
                    out.label.insert(posting.key8, posting.values);
                }
            }
            METRICS_SERIES_CLASS_FIELD => {
                for posting in summary.postings {
                    out.class
                        .insert(u64::from_be_bytes(posting.key8), posting.values);
                }
            }
            METRICS_CLASS_OBJECTS_FIELD => {
                for entry in summary.entries {
                    assert_eq!(entry.blocks, vec![0], "block 0 is the whole object");
                    let class = u64::from_be_bytes(entry.key8) ^ (1u64 << 63);
                    out.objects
                        .entry(class)
                        .or_default()
                        .push(entry.entry_ordinal);
                }
            }
            _ => {}
        }
    }
    out
}

/// The pair key8 of `(name, value)`.
fn pair(name: &str, value: &str) -> [u8; 8] {
    snapshot_format::series_label_pair_key8(name, value).expect("pair key8")
}

/// The ascending series keys of `specs`.
fn skeys(specs: &[SeriesSpec]) -> Vec<u64> {
    let mut keys: Vec<u64> = specs.iter().map(skey).collect();
    keys.sort_unstable();
    keys
}

/// The fixture's `writer_seq` to part entry ordinal: shard 0's writers 1, 2
/// and 6, then shard 1's 3, 4 and 5.
fn fixture_ordinals() -> HashMap<u64, u64> {
    HashMap::from([(1, 0), (2, 1), (6, 2), (3, 3), (4, 4), (5, 5)])
}

fn leaf_fields(part: &SnapshotPartRef) -> Vec<&str> {
    part.key_index
        .iter()
        .map(|leaf| leaf.field.as_str())
        .collect()
}

/// The stored bytes of `part`'s three series fields, in ref order.
async fn stored_series_leaves(
    store: &dyn ObjectStoreBackend,
    part: &SnapshotPartRef,
) -> Vec<Vec<u8>> {
    let mut leaves = Vec::new();
    for leaf in &part.key_index {
        if is_series_index_field(Signal::Metrics, &leaf.field) {
            let got = store
                .get(&leaf.key, GetRange::Full)
                .await
                .expect("the leaf HEAD names is stored");
            leaves.push(got.data.to_vec());
        }
    }
    leaves
}

/// Values a fold of the fixture with the series switch off wrote on the
/// commit before the switch existed (716f5b1): the part's blake3, and the
/// `label_value` leaf's blake3 and size, its one ref.
const GOLDEN_PART_BLAKE3: &str = "f384f1a8c0eb13bf98ea08a5b3e388eb7a79c65d46fb2a66a8537139014e3317";
const GOLDEN_LABEL_VALUE_BLAKE3: &str =
    "a51f2036c53e36637aa0502dc55bcfa813350a109c6ded4bf52e1f73d849e773";
const GOLDEN_LABEL_VALUE_SIZE: u64 = 530;
/// The fixture's summed object sizes.
const FIXTURE_DATA_BYTES: u64 = 191_800;
/// The fixture part's three series leaves, together.
const FIXTURE_SERIES_LEAF_BYTES: u64 = 1_026;

/// With `metrics_series_index` off the fold writes the leaves and refs it
/// wrote before the switch existed, byte for byte: the part and its one
/// `label_value` leaf hash to the values a fold of the same fixture wrote
/// on the commit before the switch (a fold with the field absent). No series
/// build runs and no series counter moves.
#[tokio::test]
async fn with_the_switch_off_the_fold_writes_the_leaves_and_refs_it_wrote_before() {
    let store = recording_store();
    let records = publish_series_fixture(store.as_ref()).await;
    assert_eq!(
        records.iter().map(|r| r.object_size).sum::<u64>(),
        FIXTURE_DATA_BYTES
    );
    assert!(!CatalogConfig::default().metrics_series_index);
    let catalog = Catalog::new(store.clone(), series_config(false)).expect("catalog");
    take_series_build_log_for_test();
    let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
    assert_eq!(take_series_build_log_for_test(), Vec::new());
    assert_eq!(report.metrics_series_index_parts_over_cap, 0);
    assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
    assert_eq!(
        report.key_index_field_pairs.keys().collect::<Vec<_>>(),
        vec![METRICS_LABEL_VALUE_FIELD]
    );
    let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
    assert_eq!(head.parts.len(), 1);
    let part = &head.parts[0];
    assert_eq!(hex_encode(&part.blake3), GOLDEN_PART_BLAKE3);
    assert_eq!(leaf_fields(part), vec![METRICS_LABEL_VALUE_FIELD]);
    assert_eq!(
        hex_encode(&part.key_index[0].blake3),
        GOLDEN_LABEL_VALUE_BLAKE3
    );
    assert_eq!(part.key_index[0].size, GOLDEN_LABEL_VALUE_SIZE);
    assert_eq!(kidx_objects(store.as_ref(), Signal::Metrics).await.len(), 1);
}

/// The switch takes effect only where `metrics_label_index` and the leaf
/// build are both on: `declared_key_index_fields` lists the three series
/// fields after `label_value` exactly then, and a fold with either off
/// writes no series leaf.
#[tokio::test]
async fn the_series_switch_needs_the_label_index_and_the_leaf_build() {
    let declared = |config: &CatalogConfig| {
        declared_key_index_fields(Signal::Metrics, None, &tenant(), config)
            .map(|fields| fields.into_iter().map(|(name, _)| name).collect::<Vec<_>>())
    };
    assert_eq!(
        declared(&series_config(true)),
        Some(vec![
            METRICS_LABEL_VALUE_FIELD.to_string(),
            METRICS_SERIES_LABEL_FIELD.to_string(),
            METRICS_SERIES_CLASS_FIELD.to_string(),
            METRICS_CLASS_OBJECTS_FIELD.to_string(),
        ])
    );
    assert_eq!(
        declared_key_index_fields(Signal::Metrics, None, &tenant(), &series_config(true))
            .expect("metrics fields")
            .into_iter()
            .map(|(_, key_type)| key_type)
            .collect::<Vec<_>>(),
        vec![KeyType::Str, KeyType::Bytes, KeyType::Id16, KeyType::I64]
    );
    assert_eq!(
        declared(&series_config(false)),
        Some(vec![METRICS_LABEL_VALUE_FIELD.to_string()])
    );
    let no_label_index = CatalogConfig {
        metrics_label_index: false,
        ..series_config(true)
    };
    assert_eq!(declared(&no_label_index), None);
    assert_eq!(
        declared_key_index_fields(Signal::Spans, None, &tenant(), &series_config(true))
            .map(|fields| fields.len()),
        Some(1),
        "spans keep trace_id alone"
    );

    for config in [
        no_label_index,
        CatalogConfig {
            build_key_index_leaves: false,
            ..series_config(true)
        },
    ] {
        let store = recording_store();
        publish_series_fixture(store.as_ref()).await;
        let catalog = Catalog::new(store.clone(), config).expect("catalog");
        take_series_build_log_for_test();
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        assert_eq!(take_series_build_log_for_test(), Vec::new());
        assert_eq!(report.key_index_leaves_written, 0, "{report:?}");
        assert_eq!(kidx_objects(store.as_ref(), Signal::Metrics).await.len(), 0);
    }
}

/// ADR-4146 decisions 1 and 2 over a part of six entries on two shards:
/// every field's decoded entries are pinned. `series_label` maps each of the
/// nine pairs to the ascending keys of the series carrying it;
/// `series_class` gives each of the six series its class; `class_objects`
/// lists each class's entries. B and F share the entry set {1, 2} and so a
/// class; the five classes are numbered in order of their smallest member
/// key: F (0x5bba...), C (0x619b...), D (0x973d...), A (0xcf50...), E
/// (0xf290...). The three series fields are at leaf version 3 and
/// `label_value` stays at 2. Every entry is read from the postings build's
/// bytes, with no ranged GET.
#[tokio::test]
async fn the_series_fields_hold_exactly_the_fixture_series() {
    let store = recording_store();
    publish_series_fixture(store.as_ref()).await;
    let catalog = Catalog::new(store.clone(), series_config(true)).expect("catalog");
    take_series_build_log_for_test();
    store.gets.lock().expect("lock").clear();
    let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
    assert_eq!(
        take_series_build_log_for_test(),
        vec![SeriesBuildRecord {
            carried: vec![],
            held: vec![0, 1, 2, 3, 4, 5],
            ranged: vec![],
        }]
    );
    assert_eq!(
        data_object_ranged_gets(&store),
        Vec::<(String, GetRange)>::new()
    );
    assert_eq!(report.key_index_ranged_gets, 0, "{report:?}");
    assert_eq!(report.metrics_series_index_parts_over_cap, 0);
    assert_eq!(report.key_index_leaves_written, 4, "{report:?}");
    assert_eq!(report.key_index_uncovered_entries, 0, "{report:?}");
    assert_eq!(
        report.key_index_field_pairs,
        BTreeMap::from([
            (METRICS_CLASS_OBJECTS_FIELD.to_string(), 11),
            (METRICS_LABEL_VALUE_FIELD.to_string(), 43),
            (METRICS_SERIES_CLASS_FIELD.to_string(), 6),
            (METRICS_SERIES_LABEL_FIELD.to_string(), 18),
        ])
    );

    let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
    assert_eq!(head.parts.len(), 1);
    let part = &head.parts[0];
    assert_eq!(hex_encode(&part.blake3), GOLDEN_PART_BLAKE3);
    assert_eq!(part_ordinals(store.as_ref(), part).await, fixture_ordinals());
    assert_eq!(
        leaf_fields(part),
        vec![
            METRICS_CLASS_OBJECTS_FIELD,
            METRICS_LABEL_VALUE_FIELD,
            METRICS_SERIES_CLASS_FIELD,
            METRICS_SERIES_LABEL_FIELD,
        ]
    );
    let label_value = &part.key_index[1];
    assert_eq!(hex_encode(&label_value.blake3), GOLDEN_LABEL_VALUE_BLAKE3);
    let series_bytes: u64 = part
        .key_index
        .iter()
        .filter(|leaf| is_series_index_field(Signal::Metrics, &leaf.field))
        .map(|leaf| leaf.size)
        .sum();
    assert_eq!(series_bytes, FIXTURE_SERIES_LEAF_BYTES);
    assert_eq!(
        report.key_index_leaf_bytes_written,
        FIXTURE_SERIES_LEAF_BYTES + GOLDEN_LABEL_VALUE_SIZE
    );

    // The key convention the router reads, pinned on one series.
    assert_eq!(skey(&A), 0xcf50_f3a7_5e72_bdfa);
    let leaves = series_leaves(store.as_ref(), part).await;
    assert_eq!(
        leaves.class,
        BTreeMap::from([
            (skey(&F), vec![0]),
            (skey(&C), vec![1]),
            (skey(&D), vec![2]),
            (skey(&B), vec![0]),
            (skey(&A), vec![3]),
            (skey(&E), vec![4]),
        ])
    );
    assert_eq!(
        leaves.objects,
        BTreeMap::from([
            (0, vec![0, 1]),
            (1, vec![3, 4, 5]),
            (2, vec![3, 4]),
            (3, vec![0, 1, 2]),
            (4, vec![2]),
        ])
    );
    assert_eq!(
        leaves.label,
        BTreeMap::from([
            (
                pair(METRIC_NAME_LABEL, CPU_SECONDS),
                skeys(&[A, B, E, F])
            ),
            (pair(METRIC_NAME_LABEL, HTTP_REQUESTS), skeys(&[C, D])),
            (pair("instance", "i0"), skeys(&[A, C])),
            (pair("instance", "i1"), skeys(&[B, F])),
            (pair("instance", "i2"), skeys(&[D])),
            (pair("instance", "i3"), skeys(&[E])),
            (pair("job", "api"), skeys(&[A, B, E])),
            (pair("job", "db"), skeys(&[C, D])),
            (pair("job", "web"), skeys(&[F])),
        ])
    );
    assert_eq!(
        leaves.versions,
        BTreeMap::from([
            (METRICS_CLASS_OBJECTS_FIELD.to_string(), vec![3]),
            (METRICS_LABEL_VALUE_FIELD.to_string(), vec![2]),
            (METRICS_SERIES_CLASS_FIELD.to_string(), vec![3]),
            (METRICS_SERIES_LABEL_FIELD.to_string(), vec![3]),
        ])
    );
    assert!(leaves.uncovered.values().all(Vec::is_empty), "{leaves:?}");
}

/// Folds the fixture with the series switch on at `ppm`.
async fn fold_fixture_at_ppm(ppm: u32) -> (Arc<RangeRecordingStore>, FoldReport, SnapshotHead) {
    let store = recording_store();
    publish_series_fixture(store.as_ref()).await;
    let catalog = Catalog::new(
        store.clone(),
        CatalogConfig {
            metrics_series_index_max_ratio_ppm: ppm,
            ..series_config(true)
        },
    )
    .expect("catalog");
    let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
    let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
    (store, report, head)
}

/// ADR-4146 decision 2's cap: the three fields together, as a ratio of the
/// part's 191,800 data bytes. Their leaves total 1,026 bytes, so at 5,349
/// ppm (a cap of 1,025 bytes) the part gets no series ref, no series leaf is
/// PUT and the fold counts the part over the cap, while `label_value` is
/// written as with the switch off. At 5,350 ppm the cap is exactly 1,026
/// bytes and the part gets all three.
#[tokio::test]
async fn a_part_over_the_series_cap_gets_no_series_refs_and_one_at_it_does() {
    assert_eq!(
        metrics_label_index_leaf_bytes_cap(FIXTURE_DATA_BYTES, 5_349),
        FIXTURE_SERIES_LEAF_BYTES - 1
    );
    assert_eq!(
        metrics_label_index_leaf_bytes_cap(FIXTURE_DATA_BYTES, 5_350),
        FIXTURE_SERIES_LEAF_BYTES
    );

    let (store, report, head) = fold_fixture_at_ppm(5_349).await;
    assert_eq!(report.metrics_series_index_parts_over_cap, 1, "{report:?}");
    assert_eq!(report.metrics_label_index_parts_over_cap, 0, "{report:?}");
    assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
    assert_eq!(report.key_index_uncovered_entries, 18, "{report:?}");
    let part = &head.parts[0];
    assert_eq!(leaf_fields(part), vec![METRICS_LABEL_VALUE_FIELD]);
    assert_eq!(
        hex_encode(&part.key_index[0].blake3),
        GOLDEN_LABEL_VALUE_BLAKE3
    );
    assert_eq!(kidx_objects(store.as_ref(), Signal::Metrics).await.len(), 1);

    let (_, report, head) = fold_fixture_at_ppm(5_350).await;
    assert_eq!(report.metrics_series_index_parts_over_cap, 0, "{report:?}");
    assert_eq!(report.key_index_leaves_written, 4, "{report:?}");
    assert_eq!(leaf_fields(&head.parts[0]).len(), 4);
}

/// One key whose posting exceeds the leaf body ceiling is `BodyOverCeiling`
/// in the splitter, with the cap's outcome: no series ref, counted over the
/// cap. The largest series key is the `__name__="cpu_seconds"` posting of
/// four series; the ceiling is set one byte under it.
#[tokio::test]
async fn a_posting_over_the_body_ceiling_leaves_the_part_without_series_refs() {
    let largest = snapshot_format::encoded_posting_len(&snapshot_format::KeyIndexPostingEntry {
        key8: pair(METRIC_NAME_LABEL, CPU_SECONDS),
        values: skeys(&[A, B, E, F]),
    });
    assert_eq!(largest, 45);
    set_key_index_leaf_body_ceiling_for_test(Some(largest - 1));
    let (_, report, head) = fold_fixture_at_ppm(1_000_000).await;
    set_key_index_leaf_body_ceiling_for_test(None);
    assert_eq!(report.metrics_series_index_parts_over_cap, 1, "{report:?}");
    assert!(
        head.parts[0]
            .key_index
            .iter()
            .all(|leaf| !is_series_index_field(Signal::Metrics, &leaf.field)),
        "{:?}",
        head.parts[0].key_index
    );
}

/// ADR-2707 decision 6, as for `label_value`: with a metrics erasure request
/// pending the fold builds no series leaf and runs no series build.
#[tokio::test]
async fn a_pending_erasure_request_builds_no_series_leaf() {
    let store = recording_store();
    publish_series_fixture(store.as_ref()).await;
    put_metrics_erasure_request(store.as_ref()).await;
    let catalog = Catalog::new(store.clone(), series_config(true)).expect("catalog");
    take_series_build_log_for_test();
    let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
    assert_eq!(take_series_build_log_for_test(), Vec::new());
    assert_eq!(report.metrics_label_index_parts_erasure_gated, 1);
    assert_eq!(report.key_index_leaves_written, 0, "{report:?}");
    let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
    assert!(head.parts.iter().all(|p| p.key_index.is_empty()));
    assert_eq!(kidx_objects(store.as_ref(), Signal::Metrics).await.len(), 0);
}

/// The first switch-on fold over a tail folded with the switch off: the
/// previous part has no series leaf to carry, so the six old entries are read
/// by range (a footer suffix GET and one GET of the catalog sections each)
/// and the new one from the postings build's bytes. Writer 3's catalog GET
/// fails (`FaultStore`, its counter asserted), so its entry is uncovered in
/// all three fields and C and D lose it from their entry sets, while its
/// `label_value` coverage, carried, is untouched.
#[tokio::test]
async fn a_failed_catalog_read_leaves_the_entry_uncovered_in_all_three_fields() {
    let inner = recording_store();
    let records = publish_series_fixture(inner.as_ref()).await;
    let writer_3 = keys::reconstruct_data_key(&records[2]).expect("data key");
    let off = Catalog::new(inner.clone(), series_config(false)).expect("catalog");
    fold_through(&off, Signal::Metrics, KIDX_HOUR).await;
    publish_series_object(inner.as_ref(), 0, 7, KIDX_HOUR + 1, &[A, B], SAMPLES).await;

    // Through the fault store, which the first fold did not use, writer 3's
    // object sees this fold's footer suffix GET, then its catalog GET.
    let store = Arc::new(FaultStore::new(
        Arc::clone(&inner),
        FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("catalog GET refused".into()),
            )
            .with_key_contains(writer_3.as_str())
            .with_occurrence(Occurrence::Nth(2)),
        ),
    ));
    let on = Catalog::new(store.clone(), series_config(true)).expect("catalog");
    take_series_build_log_for_test();
    inner.gets.lock().expect("lock").clear();
    let report = fold_through(&on, Signal::Metrics, KIDX_HOUR + 1).await;
    assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
    let head = read_signal_head(inner.as_ref(), Signal::Metrics).await;
    assert_eq!(head.parts.len(), 1);
    let part = &head.parts[0];
    let ordinal = part_ordinals(inner.as_ref(), part).await;
    assert_eq!(
        ordinal,
        HashMap::from([(1, 0), (2, 1), (6, 2), (3, 3), (4, 4), (5, 5), (7, 6)])
    );
    assert_eq!(
        take_series_build_log_for_test(),
        vec![SeriesBuildRecord {
            carried: vec![],
            held: vec![6],
            ranged: vec![0, 1, 2, 3, 4, 5],
        }]
    );
    // Two GETs issued for each old entry, writer 3's failed one included.
    assert_eq!(report.key_index_ranged_gets, 12, "{report:?}");
    assert_eq!(report.metrics_series_index_parts_over_cap, 0);

    let leaves = series_leaves(inner.as_ref(), part).await;
    let w3 = ordinal[&3];
    for field in [
        METRICS_SERIES_LABEL_FIELD,
        METRICS_SERIES_CLASS_FIELD,
        METRICS_CLASS_OBJECTS_FIELD,
    ] {
        assert_eq!(leaves.uncovered[field], vec![w3], "{field}");
    }
    assert_eq!(leaves.uncovered[METRICS_LABEL_VALUE_FIELD], Vec::<u64>::new());
    // F, C, D, B, A, E by key. Writer 7 holds A and B, so B no longer
    // shares F's entry set and each series has a class of its own.
    assert_eq!(
        leaves.class,
        BTreeMap::from([
            (skey(&F), vec![0]),
            (skey(&C), vec![1]),
            (skey(&D), vec![2]),
            (skey(&B), vec![3]),
            (skey(&A), vec![4]),
            (skey(&E), vec![5]),
        ])
    );
    assert_eq!(
        leaves.objects,
        BTreeMap::from([
            (0, vec![0, 1]),
            (1, vec![4, 5]),
            (2, vec![4]),
            (3, vec![0, 1, 6]),
            (4, vec![0, 1, 2, 6]),
            (5, vec![2]),
        ])
    );
}

/// A series no published entry holds any more: hour `KIDX_HOUR + 1` of
/// shard 0 holds writer 9, the only object of series G, and is retired by a
/// retention tombstone before the tail is re-encoded with writer 10. The
/// carry drops G from all three fields: no class, no posting naming it, its
/// two own pairs gone, and no class of writer 9's entry. The six carried
/// entries are not read and writer 10 is read from held bytes; the leaves
/// equal those of a fold of the same entries on a store that never held
/// writer 9.
#[tokio::test]
async fn a_retired_hour_drops_its_only_series_from_all_three_fields() {
    const G: SeriesSpec = (CPU_SECONDS, &[("instance", "i9"), ("job", "batch")]);
    let store = recording_store();
    publish_series_fixture(store.as_ref()).await;
    publish_series_object(store.as_ref(), 0, 9, KIDX_HOUR + 1, &[G], SAMPLES).await;
    let catalog = Catalog::new(store.clone(), series_config(true)).expect("catalog");
    fold_through(&catalog, Signal::Metrics, KIDX_HOUR + 1).await;
    let first = read_signal_head(store.as_ref(), Signal::Metrics).await;
    let before = series_leaves(store.as_ref(), &first.parts[0]).await;
    assert!(before.class.contains_key(&skey(&G)), "{before:?}");
    assert_eq!(before.label[&pair("job", "batch")], vec![skey(&G)]);

    publish_tombstone(&store.inner, 0, KIDX_HOUR + 1).await;
    publish_series_object(store.as_ref(), 1, 10, KIDX_HOUR + 2, &[C], SAMPLES).await;
    take_series_build_log_for_test();
    let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR + 2).await;
    let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
    assert_eq!(head.parts.len(), 1);
    let part = &head.parts[0];
    let ordinal = part_ordinals(store.as_ref(), part).await;
    assert!(!ordinal.contains_key(&9), "{ordinal:?}");
    let w10 = ordinal[&10];
    assert_eq!(
        take_series_build_log_for_test(),
        vec![SeriesBuildRecord {
            carried: (0..7).filter(|&o| o != w10).collect(),
            held: vec![w10],
            ranged: vec![],
        }]
    );
    assert_eq!(report.key_index_ranged_gets, 0, "{report:?}");

    let after = series_leaves(store.as_ref(), part).await;
    assert!(!after.class.contains_key(&skey(&G)), "{after:?}");
    assert!(
        after.label.values().all(|skeys| !skeys.contains(&skey(&G))),
        "{after:?}"
    );
    assert!(!after.label.contains_key(&pair("job", "batch")));
    assert!(!after.label.contains_key(&pair("instance", "i9")));
    assert_eq!((before.class.len(), before.objects.len()), (7, 6));
    assert_eq!((after.class.len(), after.objects.len()), (6, 5));

    let fresh = recording_store();
    publish_series_fixture(fresh.as_ref()).await;
    publish_series_object(fresh.as_ref(), 1, 10, KIDX_HOUR + 2, &[C], SAMPLES).await;
    let fresh_catalog = Catalog::new(fresh.clone(), series_config(true)).expect("catalog");
    fold_through(&fresh_catalog, Signal::Metrics, KIDX_HOUR + 2).await;
    let fresh_head = read_signal_head(fresh.as_ref(), Signal::Metrics).await;
    assert_eq!(
        series_leaves(fresh.as_ref(), &fresh_head.parts[0]).await,
        after
    );
}

/// Publishes the advancing series fixture's second hour, `KIDX_HOUR + 1`:
/// writer 7 on shard 0 with A and B, and writer 8 on shard 1 with D and a
/// new series.
async fn publish_series_second_hour(store: &dyn ObjectStoreBackend) {
    const H: SeriesSpec = (HTTP_REQUESTS, &[("instance", "i4"), ("job", "db")]);
    publish_series_object(store, 0, 7, KIDX_HOUR + 1, &[A, B], SAMPLES).await;
    publish_series_object(store, 1, 8, KIDX_HOUR + 1, &[D, H], SAMPLES).await;
}

/// The series fields of [`the_leaves_are_the_same_carried_from_scratch_and_after_a_restart`]:
/// a tail of the fixture's hour advanced by [`publish_series_second_hour`]
/// stores the same series leaves, byte for byte, built three ways. Carried:
/// the six old entries from the previous part's series leaves and the two new
/// ones from held bytes. From scratch: the previous `series_label` leaf's
/// carry GET finds no object (its 2nd GET, after the pre-PUT version peek),
/// so the six old entries are read by range. In one fold: all eight entries
/// folded at once, from held bytes.
pub(super) async fn assert_series_leaves_carried_equal_from_scratch() {
    let advance = |fault_carry: bool| async move {
        let inner = recording_store();
        publish_series_fixture(inner.as_ref()).await;
        let first = Catalog::new(inner.clone(), series_config(true)).expect("catalog");
        fold_through(&first, Signal::Metrics, KIDX_HOUR).await;
        let previous = read_signal_head(inner.as_ref(), Signal::Metrics).await;
        let series_label = previous.parts[0]
            .key_index
            .iter()
            .find(|leaf| leaf.field == METRICS_SERIES_LABEL_FIELD)
            .expect("a series_label leaf")
            .key
            .clone();
        publish_series_second_hour(inner.as_ref()).await;
        let plan = if fault_carry {
            FaultPlan::empty().with_rule(
                Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
                    .with_key_contains(series_label)
                    .with_occurrence(Occurrence::Nth(2)),
            )
        } else {
            FaultPlan::empty()
        };
        let store = Arc::new(FaultStore::new(Arc::clone(&inner), plan));
        let second = Catalog::new(store.clone(), series_config(true)).expect("catalog");
        take_series_build_log_for_test();
        let report = fold_through(&second, Signal::Metrics, KIDX_HOUR + 1).await;
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::NotFoundBlip),
            u64::from(fault_carry)
        );
        let log = take_series_build_log_for_test();
        let part = read_signal_head(inner.as_ref(), Signal::Metrics)
            .await
            .parts
            .remove(0);
        let leaves = stored_series_leaves(inner.as_ref(), &part).await;
        (part, report, log, leaves)
    };

    // Entries sort by hour first: the fixture's six, then writers 7 and 8.
    let (carried_part, carried_report, carried_log, carried) = advance(false).await;
    let ordinal_of_new: Vec<u64> = vec![6, 7];
    assert_eq!(
        carried_log,
        vec![SeriesBuildRecord {
            carried: vec![0, 1, 2, 3, 4, 5],
            held: ordinal_of_new.clone(),
            ranged: vec![],
        }]
    );
    // Three previous series leaves plus the generic carry's `label_value`.
    assert_eq!(carried_report.key_index_leaf_gets, 4, "{carried_report:?}");
    assert_eq!(carried_report.key_index_ranged_gets, 0);
    assert_eq!(carried.len(), 3);

    let (scratch_part, scratch_report, scratch_log, scratch) = advance(true).await;
    assert_eq!(
        scratch_log,
        vec![SeriesBuildRecord {
            carried: vec![],
            held: ordinal_of_new,
            ranged: vec![0, 1, 2, 3, 4, 5],
        }]
    );
    assert_eq!(scratch_report.key_index_ranged_gets, 12, "{scratch_report:?}");
    assert_eq!(scratch_part.blake3, carried_part.blake3);
    assert_eq!(scratch, carried, "from scratch");

    let once = recording_store();
    publish_series_fixture(once.as_ref()).await;
    publish_series_second_hour(once.as_ref()).await;
    let catalog = Catalog::new(once.clone(), series_config(true)).expect("catalog");
    take_series_build_log_for_test();
    fold_through(&catalog, Signal::Metrics, KIDX_HOUR + 1).await;
    assert_eq!(
        take_series_build_log_for_test(),
        vec![SeriesBuildRecord {
            carried: vec![],
            held: (0..8).collect(),
            ranged: vec![],
        }]
    );
    let once_part = read_signal_head(once.as_ref(), Signal::Metrics)
        .await
        .parts
        .remove(0);
    assert_eq!(once_part.blake3, carried_part.blake3);
    assert_eq!(
        stored_series_leaves(once.as_ref(), &once_part).await,
        carried,
        "in one fold"
    );
}

/// `--rebuild-key-index` with the switch on attaches the three series fields
/// to a part a switch-off fold wrote, reading each entry by range (a footer
/// suffix GET and one catalog GET each), and the leaves are byte-identical to
/// those a switch-on fold of the same part writes.
#[tokio::test]
async fn rebuild_key_index_attaches_the_three_series_fields() {
    let store = recording_store();
    publish_series_fixture(store.as_ref()).await;
    let off = Catalog::new(store.clone(), series_config(false)).expect("catalog");
    fold_through(&off, Signal::Metrics, KIDX_HOUR).await;

    let on = Catalog::new(store.clone(), series_config(true)).expect("catalog");
    take_series_build_log_for_test();
    let report = on
        .rebuild_key_index(
            &tenant(),
            Signal::Metrics,
            &crate::KeyIndexRebuildOptions::default(),
        )
        .await
        .expect("rebuild");
    assert_eq!(
        report.fields,
        vec![
            METRICS_LABEL_VALUE_FIELD.to_string(),
            METRICS_SERIES_LABEL_FIELD.to_string(),
            METRICS_SERIES_CLASS_FIELD.to_string(),
            METRICS_CLASS_OBJECTS_FIELD.to_string(),
        ]
    );
    assert_eq!(report.parts_unindexed, 1, "{report:?}");
    assert_eq!(report.parts_rebuilt, 1, "{report:?}");
    assert_eq!(report.series_index_parts_over_cap, 0, "{report:?}");
    assert_eq!(report.ranged_gets, 12, "{report:?}");
    assert_eq!(report.footer_phase_ranged_gets, 6, "{report:?}");
    assert_eq!(report.section_phase_ranged_gets, 6, "{report:?}");
    assert_eq!(report.leaves_written, 3, "{report:?}");
    assert_eq!(
        take_series_build_log_for_test(),
        vec![SeriesBuildRecord {
            carried: vec![],
            held: vec![],
            ranged: vec![0, 1, 2, 3, 4, 5],
        }]
    );
    let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
    let part = &head.parts[0];
    assert_eq!(hex_encode(&part.blake3), GOLDEN_PART_BLAKE3);
    assert_eq!(leaf_fields(part).len(), 4, "{:?}", part.key_index);

    let (folded_store, _, folded_head) = fold_fixture_at_ppm(10_000).await;
    assert_eq!(
        stored_series_leaves(store.as_ref(), part).await,
        stored_series_leaves(folded_store.as_ref(), &folded_head.parts[0]).await
    );
}
