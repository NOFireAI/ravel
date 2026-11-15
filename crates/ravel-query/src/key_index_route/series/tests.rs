use std::sync::Arc;

use proptest::prelude::*;
use ravel_catalog::{
    Catalog, CatalogConfig, KeyIndexLeafEntry, KeyIndexLeafInput, KeyIndexPostingEntry,
    KeyIndexPostingLeafInput, METRICS_LABEL_VALUE_FIELD, SegmentLevel, SegmentPartOrigin,
    SegmentRef, encode_key_index_leaf_with_bucket_bits,
    encode_key_index_posting_leaf_with_bucket_bits,
};
use ravel_codec::key_idx::key8_str;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_types::{Signal, TenantHash};

use super::*;
use crate::key_index_route::{
    BudgetRefused, LeafDecodeBudget, LeafDecodeGuard, LeafRangeReader, StoreLeafReader,
    route_values,
};
use crate::phase_accounting::PhaseAccounting;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const OTHER_TENANT: TenantHash = TenantHash([8u8; 16]);
const SIGNAL: Signal = Signal::Metrics;

/// One series: its label pairs, `__name__` included, and its class.
#[derive(Debug, Clone)]
struct Series {
    labels: Vec<(String, String)>,
    class: u64,
}

fn series(labels: &[(&str, &str)], class: u64) -> Series {
    Series {
        labels: labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
        class,
    }
}

/// Series `s`'s key: spread over the key space, never `u64::MAX`.
fn skey(s: usize) -> u64 {
    let digest = blake3::hash(format!("series-{s}").as_bytes());
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_be_bytes(head) >> 1
}

/// Which series leaves a part carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeriesLeaves {
    All,
    /// No series field: a part folded before the series index.
    None,
    /// Every series field but this pass's.
    Missing(SeriesPass),
    /// This pass's leaf binds another part's hash.
    StalePart(SeriesPass),
    /// This pass's leaf binds another tenant.
    OtherTenant(SeriesPass),
}

#[derive(Debug, Clone)]
struct PartSpec {
    /// Per entry ordinal, the series its segment holds.
    entries: Vec<Vec<usize>>,
    uncovered: Vec<bool>,
    /// Whether each entry's segment is in the snapshot.
    present: Vec<bool>,
    series_leaves: SeriesLeaves,
    label_value: bool,
    bucket_bits: u8,
    /// Slices per series field.
    slices: usize,
}

impl PartSpec {
    fn simple(entries: Vec<Vec<usize>>) -> Self {
        let n = entries.len();
        PartSpec {
            entries,
            uncovered: vec![false; n],
            present: vec![true; n],
            series_leaves: SeriesLeaves::All,
            label_value: true,
            bucket_bits: 6,
            slices: 1,
        }
    }

    fn without_series(&self) -> Self {
        PartSpec {
            series_leaves: SeriesLeaves::None,
            ..self.clone()
        }
    }
}

struct Fx {
    catalog: Catalog,
    store: Arc<MemoryStore>,
    snapshot: Snapshot,
    parts: SnapshotParts,
    universe: Vec<Series>,
    /// Parallel to the snapshot's segments: the series each holds.
    seg_series: Vec<Vec<usize>>,
    /// Parallel to the snapshot's segments: the segment is unfolded or its
    /// entry uncovered, so every plan reads it.
    always_read: Vec<bool>,
    /// Parallel to the snapshot's segments: the part it comes from.
    seg_part: Vec<Option<usize>>,
}

fn segment(i: usize) -> SegmentRef {
    SegmentRef {
        data_object_key: format!("seg/{i}"),
        object_size: 1,
        min_event_ts_ns: 0,
        max_event_ts_ns: 0,
        ingest_hour_bucket: 0,
        sample_count: 1,
        series_count: 1,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: uuid::Uuid::nil(),
        writer_epoch: 1,
        writer_seq: i as u64,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: 1,
        declared_column_stats: Default::default(),
    }
}

fn slice_bounds(slices: usize) -> Vec<(u64, u64)> {
    let count = slices.max(1) as u64;
    let width = u64::MAX / count;
    (0..count)
        .map(|i| {
            let hi = if i + 1 == count {
                u64::MAX
            } else {
                (i + 1) * width - 1
            };
            (i * width, hi)
        })
        .collect()
}

/// The header fields every leaf of one part's field shares.
struct LeafHeader<'a> {
    tenant_hash: TenantHash,
    part_blake3: [u8; 32],
    entry_count: u64,
    uncovered: &'a [u64],
    bucket_bits: u8,
}

async fn put_leaf(
    store: &MemoryStore,
    key: String,
    encoded: ravel_catalog::EncodedKeyIndexLeaf,
) -> SnapshotKeyIndexLeafRef {
    let mut leaf_ref = encoded.leaf_ref;
    leaf_ref.key = key;
    store
        .put(
            &leaf_ref.key,
            Bytes::from(encoded.bytes),
            PutOptions::default(),
        )
        .await
        .expect("put leaf");
    leaf_ref
}

#[allow(clippy::too_many_arguments)]
async fn put_entry_leaves(
    store: &MemoryStore,
    refs: &mut Vec<SnapshotKeyIndexLeafRef>,
    p: usize,
    field: &str,
    key_type: KeyType,
    header: &LeafHeader<'_>,
    slices: usize,
    entries: &[KeyIndexLeafEntry],
) {
    for (s, (lo, hi)) in slice_bounds(slices).into_iter().enumerate() {
        let slice: Vec<KeyIndexLeafEntry> = entries
            .iter()
            .filter(|e| (lo..=hi).contains(&u64::from_be_bytes(e.key8)))
            .cloned()
            .collect();
        let encoded = encode_key_index_leaf_with_bucket_bits(
            &KeyIndexLeafInput {
                tenant_hash: header.tenant_hash,
                signal: SIGNAL,
                part_blake3: header.part_blake3,
                part_entry_count: header.entry_count,
                field,
                key_type,
                key8_lo: lo,
                key8_hi: hi,
                uncovered_entry_ordinals: header.uncovered,
                entries: &slice,
            },
            header.bucket_bits,
            &KeyIndexLeafLimits::default(),
        )
        .expect("encode leaf");
        let key = format!("t/{}/kidx/{p}/{field}/{s}", TENANT.to_hex());
        refs.push(put_leaf(store, key, encoded).await);
    }
}

#[allow(clippy::too_many_arguments)]
async fn put_posting_leaves(
    store: &MemoryStore,
    refs: &mut Vec<SnapshotKeyIndexLeafRef>,
    p: usize,
    field: &str,
    key_type: KeyType,
    header: &LeafHeader<'_>,
    slices: usize,
    postings: &[KeyIndexPostingEntry],
) {
    for (s, (lo, hi)) in slice_bounds(slices).into_iter().enumerate() {
        let slice: Vec<KeyIndexPostingEntry> = postings
            .iter()
            .filter(|e| (lo..=hi).contains(&u64::from_be_bytes(e.key8)))
            .cloned()
            .collect();
        let encoded = encode_key_index_posting_leaf_with_bucket_bits(
            &KeyIndexPostingLeafInput {
                tenant_hash: header.tenant_hash,
                signal: SIGNAL,
                part_blake3: header.part_blake3,
                part_entry_count: header.entry_count,
                field,
                key_type,
                key8_lo: lo,
                key8_hi: hi,
                uncovered_entry_ordinals: header.uncovered,
                postings: &slice,
            },
            header.bucket_bits,
            &KeyIndexLeafLimits::default(),
        )
        .expect("encode posting leaf");
        let key = format!("t/{}/kidx/{p}/{field}/{s}", TENANT.to_hex());
        refs.push(put_leaf(store, key, encoded).await);
    }
}

/// Builds every part's leaves as a fold would from `specs`: the label-value
/// leaf over each covered entry's values, and the three series fields over
/// the series of covered entries.
async fn fixture(universe: &[Series], specs: &[PartSpec], recents: &[Vec<usize>]) -> Fx {
    let store = Arc::new(MemoryStore::new());
    let mut handles = Vec::new();
    let mut segments = Vec::new();
    let mut origin = Vec::new();
    let mut seg_series = Vec::new();
    let mut always_read = Vec::new();
    let mut seg_part = Vec::new();
    for (p, spec) in specs.iter().enumerate() {
        let n = spec.entries.len() as u64;
        let part_blake3 = *blake3::hash(format!("part-{p}").as_bytes()).as_bytes();
        let uncovered: Vec<u64> = (0..n).filter(|o| spec.uncovered[*o as usize]).collect();
        let covered: Vec<(u64, &Vec<usize>)> = spec
            .entries
            .iter()
            .enumerate()
            .filter(|(o, _)| !spec.uncovered[*o])
            .map(|(o, held)| (o as u64, held))
            .collect();
        let mut refs = Vec::new();
        let header = LeafHeader {
            tenant_hash: TENANT,
            part_blake3,
            entry_count: n,
            uncovered: &uncovered,
            bucket_bits: spec.bucket_bits,
        };
        if spec.label_value {
            let mut keys: BTreeSet<(Key8, u64)> = BTreeSet::new();
            for (o, held) in &covered {
                for s in *held {
                    for (_, value) in &universe[*s].labels {
                        keys.insert((key8_str(value), *o));
                    }
                }
            }
            let entries: Vec<KeyIndexLeafEntry> = keys
                .into_iter()
                .map(|(key8, entry_ordinal)| KeyIndexLeafEntry {
                    key8,
                    entry_ordinal,
                    blocks: vec![0],
                })
                .collect();
            put_entry_leaves(
                &store,
                &mut refs,
                p,
                METRICS_LABEL_VALUE_FIELD,
                KeyType::Str,
                &header,
                1,
                &entries,
            )
            .await;
        }
        let indexed: BTreeSet<usize> = covered
            .iter()
            .flat_map(|(_, held)| held.iter().copied())
            .collect();
        for pass in SeriesPass::ALL {
            let leaves = spec.series_leaves;
            if leaves == SeriesLeaves::None || leaves == SeriesLeaves::Missing(pass) {
                continue;
            }
            let header = LeafHeader {
                tenant_hash: if leaves == SeriesLeaves::OtherTenant(pass) {
                    OTHER_TENANT
                } else {
                    TENANT
                },
                part_blake3: if leaves == SeriesLeaves::StalePart(pass) {
                    *blake3::hash(format!("stale-part-{p}").as_bytes()).as_bytes()
                } else {
                    part_blake3
                },
                ..header
            };
            match pass {
                SeriesPass::A => {
                    let mut pairs: BTreeMap<Key8, BTreeSet<u64>> = BTreeMap::new();
                    for s in &indexed {
                        for (name, value) in &universe[*s].labels {
                            let key8 = series_label_pair_key8(name, value).expect("pair key8");
                            pairs.entry(key8).or_default().insert(skey(*s));
                        }
                    }
                    let postings: Vec<KeyIndexPostingEntry> = pairs
                        .into_iter()
                        .map(|(key8, skeys)| KeyIndexPostingEntry {
                            key8,
                            values: skeys.into_iter().collect(),
                        })
                        .collect();
                    put_posting_leaves(
                        &store,
                        &mut refs,
                        p,
                        pass.field(),
                        pass.key_type(),
                        &header,
                        spec.slices,
                        &postings,
                    )
                    .await;
                }
                SeriesPass::B => {
                    let postings: Vec<KeyIndexPostingEntry> = indexed
                        .iter()
                        .map(|s| KeyIndexPostingEntry {
                            key8: skey(*s).to_be_bytes(),
                            values: vec![universe[*s].class],
                        })
                        .collect();
                    put_posting_leaves(
                        &store,
                        &mut refs,
                        p,
                        pass.field(),
                        pass.key_type(),
                        &header,
                        spec.slices,
                        &postings,
                    )
                    .await;
                }
                SeriesPass::C => {
                    let mut keys: BTreeSet<(Key8, u64)> = BTreeSet::new();
                    for (o, held) in &covered {
                        for s in *held {
                            keys.insert((key8_i64(universe[*s].class as i64), *o));
                        }
                    }
                    let entries: Vec<KeyIndexLeafEntry> = keys
                        .into_iter()
                        .map(|(key8, entry_ordinal)| KeyIndexLeafEntry {
                            key8,
                            entry_ordinal,
                            blocks: vec![0],
                        })
                        .collect();
                    put_entry_leaves(
                        &store,
                        &mut refs,
                        p,
                        pass.field(),
                        pass.key_type(),
                        &header,
                        spec.slices,
                        &entries,
                    )
                    .await;
                }
            }
        }
        handles.push(SnapshotPartHandle {
            blake3: part_blake3.to_vec(),
            entry_count: n,
            watermark_hour: 0,
            key_index: refs,
        });
        for (o, held) in spec.entries.iter().enumerate() {
            if spec.present[o] {
                origin.push(Some(SegmentPartOrigin {
                    part: p as u32,
                    ordinal: o as u32,
                }));
                segments.push(segment(segments.len()));
                seg_series.push(held.clone());
                always_read.push(spec.uncovered[o]);
                seg_part.push(Some(p));
            }
        }
    }
    for recent in recents {
        origin.push(None);
        segments.push(segment(segments.len()));
        seg_series.push(recent.clone());
        always_read.push(true);
        seg_part.push(None);
    }
    let catalog = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
    Fx {
        catalog,
        store,
        snapshot: Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        parts: SnapshotParts::new(handles, origin),
        universe: universe.to_vec(),
        seg_series,
        always_read,
        seg_part,
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(future)
}

/// Refuses any reservation above `max` bytes.
struct ThresholdBudget {
    max: u64,
}

impl LeafDecodeBudget for ThresholdBudget {
    fn try_reserve(&self, bytes: u64) -> Result<LeafDecodeGuard, BudgetRefused> {
        if bytes > self.max {
            return Err(BudgetRefused { bytes });
        }
        Ok(LeafDecodeGuard::new(()))
    }
}

const UNLIMITED: ThresholdBudget = ThresholdBudget { max: u64::MAX };

fn input<'a>(
    fx: &'a Fx,
    accounting: &'a PhaseAccounting,
    reader: &'a dyn LeafRangeReader,
    budget: &'a dyn LeafDecodeBudget,
) -> ValueRouteInput<'a> {
    ValueRouteInput {
        catalog: &fx.catalog,
        snapshot: &fx.snapshot,
        parts: &fx.parts,
        signal: SIGNAL,
        tenant_hash: TENANT,
        field: METRICS_LABEL_VALUE_FIELD,
        accounting,
        reader,
        budget,
        limits: KeyIndexLeafLimits::default(),
        concurrency: 2,
    }
}

struct Run {
    routing: MetricRouting,
    key_index_gets: u64,
}

#[allow(clippy::result_large_err)]
fn run_with(
    fx: &Fx,
    plans: &[Vec<(&str, &str)>],
    options: SeriesRouteOptions,
    budget: &ThresholdBudget,
) -> Result<Run, RouteError> {
    block_on(async {
        let accounting = PhaseAccounting::new();
        let reader = StoreLeafReader::new(fx.store.clone());
        let totals = RouteTotals::new();
        let routing = route_metric_statement_under(
            input(fx, &accounting, &reader, budget),
            plans,
            options,
            &totals,
        )
        .await?;
        Ok(Run {
            routing,
            key_index_gets: accounting
                .key_index()
                .snapshot()
                .s3_requests(AccountedOp::Get),
        })
    })
}

fn run(fx: &Fx, plans: &[Vec<(&str, &str)>], options: SeriesRouteOptions) -> Run {
    run_with(fx, plans, options, &UNLIMITED).expect("route")
}

fn kept(routing: &MetricRouting, plan: usize, s: usize) -> bool {
    routing.plan_keeps[plan].as_ref().is_none_or(|keep| keep[s])
}

/// Whether segment `s` holds a series matching every pair of `plan`.
fn holds(fx: &Fx, plan: &[(&str, &str)], s: usize) -> bool {
    fx.seg_series[s].iter().any(|i| {
        plan.iter().all(|(name, value)| {
            fx.universe[*i]
                .labels
                .iter()
                .any(|(n, v)| n == name && v == value)
        })
    })
}

/// Eight entries, one series each: `job` alternates `a`/`b`, `env` is `x`
/// on entries 0, 1, 4, 5 and `y` on the rest. Each series is its own class.
fn eight_entries() -> (Vec<Series>, Vec<Vec<usize>>) {
    let universe: Vec<Series> = (0..8)
        .map(|o| {
            let job = if o % 2 == 0 { "a" } else { "b" };
            let env = if o % 4 < 2 { "x" } else { "y" };
            let instance = format!("i{o}");
            series(
                &[
                    ("__name__", "m"),
                    ("job", job),
                    ("env", env),
                    ("instance", &instance),
                ],
                o as u64,
            )
        })
        .collect();
    let entries = (0..8).map(|o| vec![o]).collect();
    (universe, entries)
}

const JOB_A: [(&str, &str); 2] = [("__name__", "m"), ("job", "a")];

fn body_bytes(fx: &Fx, part: usize, pass: SeriesPass) -> u64 {
    field_body_bytes(&fx.parts.parts[part], pass.field())
}

#[test]
fn the_route_future_is_send() {
    fn assert_send<T: Send>(_: &T) {}
    let (universe, entries) = eight_entries();
    let fx = block_on(fixture(&universe, &[PartSpec::simple(entries)], &[]));
    let accounting = PhaseAccounting::new();
    let reader = StoreLeafReader::new(fx.store.clone());
    let plans = vec![JOB_A.to_vec()];
    let future = route_metric_statement(
        input(&fx, &accounting, &reader, &UNLIMITED),
        &plans,
        SeriesRouteOptions::default(),
    );
    assert_send(&future);
}

#[test]
fn the_row_three_shape_opens_only_the_series_holders() {
    // Every entry holds `__name__="m"` and `env="prod"`, on two different
    // series; only entries 3 and 17 hold one series carrying both. The
    // label-value route proves neither value absent anywhere.
    let mut universe = Vec::new();
    let mut entries = Vec::new();
    for o in 0..32 {
        let instance = format!("i{o}");
        let a = universe.len();
        universe.push(series(
            &[("__name__", "m"), ("env", "dev"), ("instance", &instance)],
            a as u64,
        ));
        let b = universe.len();
        universe.push(series(
            &[
                ("__name__", "other"),
                ("env", "prod"),
                ("instance", &instance),
            ],
            b as u64,
        ));
        entries.push(vec![a, b]);
    }
    let both = universe.len();
    universe.push(series(&[("__name__", "m"), ("env", "prod")], both as u64));
    entries[3].push(both);
    entries[17].push(both);
    let spec = PartSpec::simple(entries);
    let plans = vec![vec![("__name__", "m"), ("env", "prod")]];

    let fx = block_on(fixture(&universe, std::slice::from_ref(&spec), &[]));
    let routed = run(&fx, &plans, SeriesRouteOptions::default());
    let label_value = run(
        &block_on(fixture(&universe, &[spec.without_series()], &[])),
        &plans,
        SeriesRouteOptions::default(),
    );

    let holders: Vec<bool> = (0..32).map(|o| o == 3 || o == 17).collect();
    assert_eq!(routed.routing.plan_keeps, vec![Some(holders)]);
    assert_eq!(label_value.routing.plan_keeps, vec![Some(vec![true; 32])]);
    assert_eq!(routed.routing.segments_pruned_by_series_index, 30);
    assert_eq!(routed.routing.segments_pruned_by_label_index, 0);
    assert_eq!(label_value.routing.segments_pruned_by_label_index, 0);
    assert!(label_value.routing.series.is_none());

    let figures = routed
        .routing
        .series
        .as_ref()
        .expect("the series route ran");
    assert_eq!(figures.plans_routed, 1);
    assert_eq!(figures.plan_parts_answered, 1);
    assert_eq!(figures.plan_parts_declined(), 0);
    for pass in SeriesPass::ALL {
        assert_eq!(figures.pass(pass).parts_probed, 1, "{pass:?}");
        assert_eq!(figures.pass(pass).plans_admitted, 1, "{pass:?}");
        assert!(figures.pass(pass).bytes_past_prefix > 0, "{pass:?}");
    }
    assert_eq!(figures.pass(SeriesPass::A).keys_probed, 2);
    assert_eq!(figures.pass(SeriesPass::B).keys_probed, 1);
    assert_eq!(figures.pass(SeriesPass::C).keys_probed, 1);
    // The plan was answered on its one part, so no label-value pass ran, and
    // every leaf GET is one of the series passes'.
    assert!(routed.routing.values.is_empty());
    let pass_gets: u64 = SeriesPass::ALL
        .iter()
        .map(|pass| figures.pass(*pass).leaf_get_requests)
        .sum();
    assert_eq!(pass_gets, routed.key_index_gets);
    // A prefix GET per pass and a run GET per probed bucket: pass A's two
    // pairs land in two buckets, passes B and C probe one key each.
    let a = figures.pass(SeriesPass::A);
    assert_eq!(a.distinct_buckets, 2);
    assert_eq!(a.leaf_get_requests, 3);
    assert_eq!(figures.pass(SeriesPass::B).leaf_get_requests, 2);
    assert_eq!(figures.pass(SeriesPass::C).leaf_get_requests, 2);
}

#[test]
fn leaves_with_no_series_fields_route_as_the_label_value_route_does() {
    let (universe, entries) = eight_entries();
    let mut second = PartSpec::simple(entries.clone());
    second.uncovered[2] = true;
    second.present[5] = false;
    let specs = vec![
        PartSpec::simple(entries).without_series(),
        second.without_series(),
    ];
    let fx = block_on(fixture(&universe, &specs, &[vec![0]]));
    let plans = vec![
        JOB_A.to_vec(),
        vec![("env", "y")],
        vec![("job", "b"), ("env", "x"), ("instance", "i1")],
    ];
    let routed = run(&fx, &plans, SeriesRouteOptions::default());
    let (values, gets) = block_on(async {
        let accounting = PhaseAccounting::new();
        let reader = StoreLeafReader::new(fx.store.clone());
        let values = route_values(input(&fx, &accounting, &reader, &UNLIMITED), &plans)
            .await
            .expect("route");
        let gets = accounting
            .key_index()
            .snapshot()
            .s3_requests(AccountedOp::Get);
        (values, gets)
    });
    assert!(routed.routing.series.is_none());
    assert_eq!(routed.routing.plan_keeps, values.plan_keeps);
    assert_eq!(routed.routing.values, values.values);
    assert_eq!(routed.key_index_gets, gets);
    let unread = (0..fx.snapshot.segments.len())
        .filter(|s| (0..plans.len()).all(|p| !kept(&routed.routing, p, *s)))
        .count() as u64;
    assert!(unread > 0);
    assert_eq!(routed.routing.segments_pruned_by_label_index, unread);
    assert_eq!(routed.routing.segments_pruned_by_series_index, 0);
}

/// Routes `specs` with `options` and asserts every plan reads what the
/// label-value route alone has it read, with `reason` counted once per
/// (plan, part) and nothing answered.
fn assert_falls_back(
    universe: &[Series],
    specs: &[PartSpec],
    plans: &[Vec<(&str, &str)>],
    options: SeriesRouteOptions,
    budget: &ThresholdBudget,
    reason: SeriesDecline,
) {
    let fx = block_on(fixture(universe, specs, &[]));
    let routed = run_with(&fx, plans, options, budget).expect("route");
    let stripped: Vec<PartSpec> = specs.iter().map(PartSpec::without_series).collect();
    let reference = run_with(
        &block_on(fixture(universe, &stripped, &[])),
        plans,
        SeriesRouteOptions::default(),
        budget,
    )
    .expect("route");
    assert_eq!(
        routed.routing.plan_keeps, reference.routing.plan_keeps,
        "{reason:?}"
    );
    assert_eq!(
        routed.routing.segments_pruned_by_label_index,
        reference.routing.segments_pruned_by_label_index,
        "{reason:?}"
    );
    assert_eq!(routed.routing.segments_pruned_by_series_index, 0);
    let figures = routed
        .routing
        .series
        .as_ref()
        .expect("the series route ran");
    let pairs = figures.plans_routed * specs.len() as u64;
    assert_eq!(figures.declined(reason), pairs, "{reason:?}: {figures:?}");
    assert_eq!(figures.plan_parts_declined(), pairs, "{reason:?}");
    assert_eq!(figures.plan_parts_answered, 0, "{reason:?}");
    let value_passes = |run: &Run| -> Vec<(String, u64)> {
        run.routing
            .values
            .iter()
            .map(|v| (v.key8_hex.clone(), v.segments_pruned))
            .collect()
    };
    assert_eq!(
        value_passes(&routed),
        value_passes(&reference),
        "{reason:?}"
    );
}

#[test]
fn a_part_lacking_a_series_field_falls_back_to_the_label_value_route() {
    let (universe, entries) = eight_entries();
    for pass in SeriesPass::ALL {
        let spec = PartSpec {
            series_leaves: SeriesLeaves::Missing(pass),
            ..PartSpec::simple(entries.clone())
        };
        assert_falls_back(
            &universe,
            &[spec],
            &[JOB_A.to_vec()],
            SeriesRouteOptions::default(),
            &UNLIMITED,
            SeriesDecline::NoSeriesRefs,
        );
    }
}

#[test]
fn a_rejected_series_leaf_falls_back_to_the_label_value_route() {
    let (universe, entries) = eight_entries();
    for pass in SeriesPass::ALL {
        let spec = PartSpec {
            series_leaves: SeriesLeaves::StalePart(pass),
            ..PartSpec::simple(entries.clone())
        };
        assert_falls_back(
            &universe,
            &[spec],
            &[JOB_A.to_vec()],
            SeriesRouteOptions::default(),
            &UNLIMITED,
            SeriesDecline::Rejected,
        );
    }
}

#[test]
fn a_series_leaf_of_another_tenant_is_an_isolation_error() {
    let (universe, entries) = eight_entries();
    for pass in SeriesPass::ALL {
        let spec = PartSpec {
            series_leaves: SeriesLeaves::OtherTenant(pass),
            ..PartSpec::simple(entries.clone())
        };
        let fx = block_on(fixture(&universe, &[spec], &[]));
        let result = run_with(
            &fx,
            &[JOB_A.to_vec()],
            SeriesRouteOptions::default(),
            &UNLIMITED,
        );
        assert!(
            matches!(result, Err(RouteError::Isolation { .. })),
            "{pass:?}"
        );
    }
}

#[test]
fn over_q_a_falls_back_to_the_label_value_route() {
    let (universe, entries) = eight_entries();
    assert_falls_back(
        &universe,
        &[PartSpec::simple(entries)],
        &[JOB_A.to_vec()],
        SeriesRouteOptions {
            q_a: 0,
            ..SeriesRouteOptions::default()
        },
        &UNLIMITED,
        SeriesDecline::OverQA,
    );
}

#[test]
fn over_s_max_falls_back_and_the_pre_check_is_skipped_when_the_body_fits() {
    // `job="a"` is on four series, so pass B is offered four keys.
    let (universe, entries) = eight_entries();
    let spec = PartSpec::simple(entries);
    let fx = block_on(fixture(&universe, std::slice::from_ref(&spec), &[]));
    let class_body = body_bytes(&fx, 0, SeriesPass::B);
    let objects_body = body_bytes(&fx, 0, SeriesPass::C);
    // Pass B's share is one byte short of the whole series_class body.
    let q_bc = objects_body + class_body - 1;
    let plans = [JOB_A.to_vec()];
    assert_falls_back(
        &universe,
        std::slice::from_ref(&spec),
        &plans,
        SeriesRouteOptions {
            s_max: 3,
            q_bc,
            ..SeriesRouteOptions::default()
        },
        &UNLIMITED,
        SeriesDecline::OverSMax,
    );
    let at_bound = run(
        &fx,
        &plans,
        SeriesRouteOptions {
            s_max: 4,
            q_bc,
            ..SeriesRouteOptions::default()
        },
    );
    let figures = at_bound.routing.series.as_ref().expect("ran");
    assert_eq!(figures.plan_parts_answered, 1, "{figures:?}");
    assert_eq!(figures.pass(SeriesPass::B).keys_probed, 4);
    // With the default Q_BC the whole body fits pass B's share, so S_max is
    // not checked at all.
    let skipped = run(
        &fx,
        &plans,
        SeriesRouteOptions {
            s_max: 3,
            ..SeriesRouteOptions::default()
        },
    );
    let figures = skipped.routing.series.as_ref().expect("ran");
    assert_eq!(figures.plan_parts_answered, 1, "{figures:?}");
    assert_eq!(figures.declined(SeriesDecline::OverSMax), 0);
}

#[test]
fn over_the_q_bc_share_falls_back_to_the_label_value_route() {
    let (universe, entries) = eight_entries();
    let spec = PartSpec::simple(entries);
    let fx = block_on(fixture(&universe, std::slice::from_ref(&spec), &[]));
    // Pass B's share is one byte: Q_BC less the whole class_objects body.
    let q_bc = body_bytes(&fx, 0, SeriesPass::C) + 1;
    assert_falls_back(
        &universe,
        &[spec],
        &[JOB_A.to_vec()],
        SeriesRouteOptions {
            q_bc,
            ..SeriesRouteOptions::default()
        },
        &UNLIMITED,
        SeriesDecline::OverQBC,
    );
}

#[test]
fn a_part_too_small_for_tier_one_falls_back_to_the_label_value_route() {
    // One segment in the snapshot: pass A's worst case is a prefix GET and
    // one run per key, three GETs against tier 1's two.
    let (universe, entries) = eight_entries();
    let mut spec = PartSpec::simple(entries);
    spec.present = (0..8).map(|o| o == 0).collect();
    assert_falls_back(
        &universe,
        &[spec],
        &[JOB_A.to_vec()],
        SeriesRouteOptions::default(),
        &UNLIMITED,
        SeriesDecline::OverTierOne,
    );
}

#[test]
fn a_refused_decode_reservation_falls_back_to_the_label_value_route() {
    let (universe, entries) = eight_entries();
    assert_falls_back(
        &universe,
        &[PartSpec::simple(entries)],
        &[JOB_A.to_vec()],
        SeriesRouteOptions::default(),
        &ThresholdBudget { max: 0 },
        SeriesDecline::ReservationRefused,
    );
}

#[test]
fn parts_past_the_ceiling_fall_back_to_the_label_value_route() {
    // Two parts and a one-part ceiling: the larger part is probed, the
    // other reads what the label-value route has it read.
    let (universe, entries) = eight_entries();
    let large = PartSpec::simple(entries.clone());
    let small = PartSpec::simple(entries[..6].to_vec());
    let specs = vec![small.clone(), large.clone()];
    let fx = block_on(fixture(&universe, &specs, &[]));
    let plans = [JOB_A.to_vec()];
    let routed = run(
        &fx,
        &plans,
        SeriesRouteOptions {
            max_parts: 1,
            ..SeriesRouteOptions::default()
        },
    );
    let reference = run(
        &block_on(fixture(
            &universe,
            &[small.without_series(), large.without_series()],
            &[],
        )),
        &plans,
        SeriesRouteOptions::default(),
    );
    let figures = routed.routing.series.as_ref().expect("ran");
    assert_eq!(figures.declined(SeriesDecline::BeyondCeiling), 1);
    assert_eq!(figures.plan_parts_answered, 1);
    for s in 0..fx.snapshot.segments.len() {
        if fx.seg_part[s] == Some(0) {
            assert_eq!(
                kept(&routed.routing, 0, s),
                kept(&reference.routing, 0, s),
                "segment {s} of the declined part"
            );
        } else {
            assert_eq!(kept(&routed.routing, 0, s), holds(&fx, &plans[0], s));
        }
    }
    // Part 0 was the label-value route's, part 1 the series route's.
    assert_eq!(routed.routing.segments_pruned_by_label_index, 3);
    assert_eq!(routed.routing.segments_pruned_by_series_index, 4);
}

/// On-object run bytes of `keys` in `pass`'s one slice on part 0.
fn pass_bytes(fx: &Fx, pass: SeriesPass, keys: &BTreeSet<Key8>) -> u64 {
    let handle = &fx.parts.parts[0];
    let leaf_ref = handle
        .key_index
        .iter()
        .find(|leaf| leaf.field == pass.field())
        .expect("one slice");
    let prefix = block_on(async {
        let reader = StoreLeafReader::new(fx.store.clone());
        let range = key_index_leaf_prefix_range(leaf_ref).expect("prefix range");
        reader
            .read_range(leaf_ref, range, &QueryAccounting::new())
            .await
            .expect("prefix")
    });
    let request = KeyIndexLeafRequest {
        tenant_hash: TENANT,
        signal: SIGNAL,
        part_blake3: &handle.blake3,
        part_entry_count: handle.entry_count,
        field: pass.field(),
        key_type: pass.key_type(),
    };
    let keys: Vec<Key8> = keys.iter().copied().collect();
    let plan =
        plan_key_index_leaf_set(&prefix, leaf_ref, &request, &keys, B_RUN).expect("set plan");
    run_bytes(&plan)
}

fn pair_keys(plan: &[(&str, &str)]) -> BTreeSet<Key8> {
    plan.iter()
        .map(|(name, value)| series_label_pair_key8(name, value).expect("key8"))
        .collect()
}

#[test]
fn a_shared_pass_over_q_a_admits_the_cheapest_plans_and_sends_the_rest_on() {
    let (universe, entries) = eight_entries();
    let spec = PartSpec {
        bucket_bits: 8,
        ..PartSpec::simple(entries)
    };
    let fx = block_on(fixture(&universe, std::slice::from_ref(&spec), &[]));
    let plans = vec![
        vec![("__name__", "m"), ("job", "a"), ("env", "x")],
        vec![("job", "b"), ("instance", "i3")],
        vec![("job", "a"), ("env", "y"), ("instance", "i6")],
    ];
    let own: Vec<u64> = plans
        .iter()
        .map(|plan| pass_bytes(&fx, SeriesPass::A, &pair_keys(plan)))
        .collect();
    let union: BTreeSet<Key8> = plans.iter().flat_map(|plan| pair_keys(plan)).collect();
    let cheapest = (0..plans.len())
        .min_by_key(|p| (own[*p], *p))
        .expect("a plan");
    let q_a = own[cheapest];
    assert!(
        pass_bytes(&fx, SeriesPass::A, &union) > q_a,
        "the statement's union exceeds Q_A"
    );
    // Every other plan alone adds bytes past the cheapest's.
    for p in 0..plans.len() {
        if p != cheapest {
            let mut with = pair_keys(&plans[cheapest]);
            with.extend(pair_keys(&plans[p]));
            assert!(pass_bytes(&fx, SeriesPass::A, &with) > q_a, "plan {p}");
        }
    }

    let routed = run(
        &fx,
        &plans,
        SeriesRouteOptions {
            q_a,
            ..SeriesRouteOptions::default()
        },
    );
    let figures = routed.routing.series.as_ref().expect("ran");
    assert_eq!(figures.plans_routed, 3);
    assert_eq!(figures.plan_parts_answered, 1);
    assert_eq!(figures.declined(SeriesDecline::OverQA), 2);
    let a = figures.pass(SeriesPass::A);
    assert_eq!((a.plans_offered, a.plans_admitted), (3, 1));
    assert_eq!(a.keys_offered, union.len() as u64);
    assert_eq!(a.keys_probed, pair_keys(&plans[cheapest]).len() as u64);
    assert_eq!(a.bytes_past_prefix, q_a);

    let reference = run(
        &block_on(fixture(&universe, &[spec.without_series()], &[])),
        &plans,
        SeriesRouteOptions::default(),
    );
    for (p, plan) in plans.iter().enumerate() {
        for s in 0..fx.snapshot.segments.len() {
            if p == cheapest {
                assert_eq!(kept(&routed.routing, p, s), holds(&fx, plan, s));
            } else {
                assert_eq!(
                    kept(&routed.routing, p, s),
                    kept(&reference.routing, p, s),
                    "plan {p} segment {s}"
                );
            }
        }
    }
}

#[test]
fn a_plan_with_one_pair_is_not_series_routed() {
    let (universe, entries) = eight_entries();
    let fx = block_on(fixture(&universe, &[PartSpec::simple(entries)], &[]));
    let plans = vec![vec![("job", "a")], vec![("job", "a"), ("job", "a")]];
    let routed = run(&fx, &plans, SeriesRouteOptions::default());
    assert!(routed.routing.series.is_none());
    assert_eq!(routed.routing.segments_pruned_by_label_index, 4);
}

#[test]
fn a_plan_whose_pairs_share_no_series_keeps_only_uncovered_entries() {
    // `job="a"` and `env="y"` are both present, never on one series.
    let universe = vec![
        series(&[("__name__", "m"), ("job", "a"), ("env", "x")], 0),
        series(&[("__name__", "m"), ("job", "b"), ("env", "y")], 1),
    ];
    let mut spec = PartSpec::simple(vec![vec![0], vec![1], vec![0, 1], vec![0], vec![1]]);
    spec.uncovered[4] = true;
    let fx = block_on(fixture(&universe, &[spec], &[]));
    let routed = run(
        &fx,
        &[vec![("job", "a"), ("env", "y")]],
        SeriesRouteOptions::default(),
    );
    assert_eq!(
        routed.routing.plan_keeps,
        vec![Some(vec![false, false, false, false, true])]
    );
    let figures = routed.routing.series.as_ref().expect("ran");
    assert_eq!(figures.plan_parts_answered, 1);
    assert_eq!(figures.pass(SeriesPass::B).parts_probed, 0);
    assert_eq!(routed.routing.segments_pruned_by_series_index, 4);
}

const NAMES: [&str; 3] = ["__name__", "job", "env"];
const VALUES: [&str; 3] = ["v0", "v1", "v2"];

fn universe_strategy() -> impl Strategy<Value = Vec<Series>> {
    prop::collection::vec(
        (
            0..VALUES.len(),
            prop::option::of(0..VALUES.len()),
            prop::option::of(0..VALUES.len()),
            0u64..4,
        ),
        1..12,
    )
    .prop_map(|series| {
        series
            .into_iter()
            .enumerate()
            .map(|(i, (name, job, env, class))| {
                let instance = format!("i{i}");
                let mut labels = vec![("__name__", VALUES[name]), ("instance", &instance)];
                if let Some(job) = job {
                    labels.push(("job", VALUES[job]));
                }
                if let Some(env) = env {
                    labels.push(("env", VALUES[env]));
                }
                self::series(&labels, class)
            })
            .collect()
    })
}

fn part_strategy(universe: usize) -> impl Strategy<Value = PartSpec> {
    (
        prop::collection::vec(
            (
                prop::collection::vec(0..universe, 0..4),
                prop::bool::weighted(0.2),
                prop::bool::weighted(0.85),
            ),
            1..12,
        ),
        prop_oneof![
            6 => Just(SeriesLeaves::All),
            1 => Just(SeriesLeaves::None),
            1 => prop::sample::select(SeriesPass::ALL.to_vec()).prop_map(SeriesLeaves::Missing),
            1 => prop::sample::select(SeriesPass::ALL.to_vec()).prop_map(SeriesLeaves::StalePart),
        ],
        prop::bool::weighted(0.8),
        1u8..7,
        1usize..4,
    )
        .prop_map(
            |(entries, series_leaves, label_value, bucket_bits, slices)| {
                let (held, flags): (Vec<Vec<usize>>, Vec<(bool, bool)>) = entries
                    .into_iter()
                    .map(|(held, uncovered, present)| (held, (uncovered, present)))
                    .unzip();
                let (uncovered, present) = flags.into_iter().unzip();
                PartSpec {
                    entries: held,
                    uncovered,
                    present,
                    series_leaves,
                    label_value,
                    bucket_bits,
                    slices,
                }
            },
        )
}

type Plans = Vec<Vec<(usize, usize)>>;

fn case_strategy() -> impl Strategy<Value = (Vec<Series>, Vec<PartSpec>, Vec<Vec<usize>>, Plans)> {
    universe_strategy().prop_flat_map(|universe| {
        let n = universe.len();
        (
            Just(universe),
            prop::collection::vec(part_strategy(n), 1..4),
            prop::collection::vec(prop::collection::vec(0..n, 0..3), 0..2),
            prop::collection::vec(
                prop::collection::vec((0..NAMES.len(), 0..VALUES.len()), 1..4),
                1..4,
            ),
        )
    })
}

fn options_strategy() -> impl Strategy<Value = SeriesRouteOptions> {
    (
        prop_oneof![3 => Just(SERIES_ROUTE_S_MAX), 1 => 0usize..4],
        prop_oneof![3 => Just(SERIES_ROUTE_Q_A), 1 => 0u64..400],
        prop_oneof![3 => Just(SERIES_ROUTE_Q_BC), 1 => 0u64..800],
        prop_oneof![3 => Just(KEY_INDEX_MAX_PROBED_PARTS), 1 => 1usize..3],
    )
        .prop_map(|(s_max, q_a, q_bc, max_parts)| SeriesRouteOptions {
            s_max,
            q_a,
            q_bc,
            max_parts,
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Every segment holding a series that matches a plan, and every
    /// uncovered or unfolded segment, is read by that plan, whatever the
    /// layout, the bounds and the declines; and the counters account for
    /// every (plan, part) pair and every segment no plan reads.
    #[test]
    fn the_series_route_never_drops_a_holder(
        (universe, specs, recents, plans) in case_strategy(),
        options in options_strategy(),
    ) {
        let fx = block_on(fixture(&universe, &specs, &recents));
        let plans: Vec<Vec<(&str, &str)>> = plans
            .iter()
            .map(|plan| plan.iter().map(|(n, v)| (NAMES[*n], VALUES[*v])).collect())
            .collect();
        let routed = run(&fx, &plans, options);
        let segments = fx.snapshot.segments.len();
        for (p, plan) in plans.iter().enumerate() {
            for s in 0..segments {
                if holds(&fx, plan, s) || fx.always_read[s] {
                    prop_assert!(
                        kept(&routed.routing, p, s),
                        "plan {p} {plan:?} dropped segment {s} holding {:?}: {:?}",
                        fx.seg_series[s],
                        routed.routing.series
                    );
                }
            }
        }
        let unread = (0..segments)
            .filter(|s| (0..plans.len()).all(|p| !kept(&routed.routing, p, *s)))
            .count() as u64;
        let any_unrouted = routed.routing.plan_keeps.iter().any(Option::is_none);
        prop_assert_eq!(
            routed.routing.segments_pruned_by_series_index
                + routed.routing.segments_pruned_by_label_index,
            if any_unrouted { 0 } else { unread }
        );
        if let Some(figures) = &routed.routing.series {
            let parts_with_segments = (0..specs.len())
                .filter(|p| fx.seg_part.contains(&Some(*p)))
                .count() as u64;
            prop_assert_eq!(
                figures.plan_parts_answered + figures.plan_parts_declined(),
                figures.plans_routed * parts_with_segments
            );
        }
    }
}
