use std::sync::atomic::{AtomicU64, Ordering};

use proptest::prelude::*;
use ravel_catalog::{
    CatalogConfig, KeyIndexLeafEntry, KeyIndexLeafInput, KeyIndexLeafPrefix, SegmentLevel,
    SegmentPartOrigin, SegmentRef, encode_key_index_leaf_with_bucket_bits,
};
use ravel_codec::key_idx::{key8_id16, key8_str};
use ravel_object_store::PutOptions;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;

use super::*;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const OTHER_TENANT: TenantHash = TenantHash([8u8; 16]);
const SIGNAL: Signal = Signal::Spans;
const FIELD: &str = "key";

/// One segment's rows: value id -> the blocks holding it.
type Rows = BTreeMap<u16, BTreeSet<u32>>;

#[derive(Debug, Clone)]
enum LeafKind {
    Good,
    /// Two slices cut at `1 << 63`.
    Split,
    /// This many slices of near-equal key8 width.
    Sliced(usize),
    /// `slices` slices as `Sliced`, slice `foreign` bound to another tenant.
    SlicedForeign {
        slices: usize,
        foreign: usize,
    },
    NoLeaf,
    /// One leaf, for another field.
    OtherField,
    /// One byte of the (first) leaf object XORed.
    Flip(usize),
    /// The leaf binds an earlier part's hash.
    StalePart,
    WrongKeyType,
    OtherTenant,
    OtherSignal,
}

#[derive(Debug, Clone)]
struct PartSpec {
    /// Per entry ordinal, the rows of its segment.
    entries: Vec<Rows>,
    uncovered: Vec<bool>,
    /// Whether each entry's segment is in the snapshot.
    present: Vec<bool>,
    watermark_hour: u32,
    bucket_bits: u8,
    leaf: LeafKind,
    /// What the leaf indexes, when it is not `entries`.
    leaf_rows: Option<Vec<Rows>>,
}

impl PartSpec {
    fn simple(entries: Vec<Rows>) -> Self {
        let n = entries.len();
        PartSpec {
            entries,
            uncovered: vec![false; n],
            present: vec![true; n],
            watermark_hour: 0,
            bucket_bits: 4,
            leaf: LeafKind::Good,
            leaf_rows: None,
        }
    }
}

struct Fixture {
    catalog: Catalog,
    store: Arc<MemoryStore>,
    snapshot: Snapshot,
    parts: SnapshotParts,
    /// Parallel to the snapshot's segments.
    rows: Vec<Rows>,
}

fn rows(pairs: &[(u16, &[u32])]) -> Rows {
    pairs
        .iter()
        .map(|(v, blocks)| (*v, blocks.iter().copied().collect()))
        .collect()
}

fn value_key8(key_type: KeyType, value: u16) -> Key8 {
    match key_type {
        KeyType::Id16 => {
            let mut id = [0u8; 16];
            id[..8].copy_from_slice(&(0x5eed_0000u64 + u64::from(value)).to_be_bytes());
            key8_id16(&id)
        }
        _ => key8_str(&format!("value-{value}")),
    }
}

fn other_key_type(key_type: KeyType) -> KeyType {
    if key_type == KeyType::Id16 {
        KeyType::Str
    } else {
        KeyType::Id16
    }
}

fn hash(label: &str) -> [u8; 32] {
    *blake3::hash(label.as_bytes()).as_bytes()
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

async fn fixture(specs: &[PartSpec], recents: &[Rows], key_type: KeyType) -> Fixture {
    let store = Arc::new(MemoryStore::new());
    let mut handles = Vec::new();
    let mut segments = Vec::new();
    let mut origin = Vec::new();
    let mut seg_rows = Vec::new();
    for (p, spec) in specs.iter().enumerate() {
        let n = spec.entries.len() as u64;
        let part_blake3 = hash(&format!("part-{p}"));
        let leaf_rows = spec.leaf_rows.as_ref().unwrap_or(&spec.entries);
        let mut entries: Vec<KeyIndexLeafEntry> = Vec::new();
        for (o, entry_rows) in leaf_rows.iter().enumerate() {
            if spec.uncovered.get(o).copied().unwrap_or(false) {
                continue;
            }
            for (v, blocks) in entry_rows {
                entries.push(KeyIndexLeafEntry {
                    key8: value_key8(key_type, *v),
                    entry_ordinal: o as u64,
                    blocks: blocks.iter().copied().collect(),
                });
            }
        }
        entries.sort_by_key(|a| (a.key8, a.entry_ordinal));
        let uncovered: Vec<u64> = (0..n).filter(|o| spec.uncovered[*o as usize]).collect();
        let cut = 1u64 << 63;
        let slices: Vec<(u64, u64)> = match spec.leaf {
            LeafKind::NoLeaf => Vec::new(),
            LeafKind::Split => vec![(0, cut - 1), (cut, u64::MAX)],
            LeafKind::Sliced(count) | LeafKind::SlicedForeign { slices: count, .. } => {
                let width = u64::MAX / count as u64;
                (0..count as u64)
                    .map(|i| {
                        let hi = if i + 1 == count as u64 {
                            u64::MAX
                        } else {
                            (i + 1) * width - 1
                        };
                        (i * width, hi)
                    })
                    .collect()
            }
            _ => vec![(0, u64::MAX)],
        };
        let mut refs = Vec::new();
        for (s, (lo, hi)) in slices.into_iter().enumerate() {
            let slice_entries: Vec<KeyIndexLeafEntry> = entries
                .iter()
                .filter(|e| (lo..=hi).contains(&u64::from_be_bytes(e.key8)))
                .cloned()
                .collect();
            let input = KeyIndexLeafInput {
                tenant_hash: match spec.leaf {
                    LeafKind::OtherTenant => OTHER_TENANT,
                    LeafKind::SlicedForeign { foreign, .. } if foreign == s => OTHER_TENANT,
                    _ => TENANT,
                },
                signal: match spec.leaf {
                    LeafKind::OtherSignal => Signal::Logs,
                    _ => SIGNAL,
                },
                part_blake3: match spec.leaf {
                    LeafKind::StalePart => hash(&format!("stale-part-{p}")),
                    _ => part_blake3,
                },
                part_entry_count: n,
                field: match spec.leaf {
                    LeafKind::OtherField => "other",
                    _ => FIELD,
                },
                key_type: match spec.leaf {
                    LeafKind::WrongKeyType => other_key_type(key_type),
                    _ => key_type,
                },
                key8_lo: lo,
                key8_hi: hi,
                uncovered_entry_ordinals: &uncovered,
                entries: &slice_entries,
            };
            let encoded = encode_key_index_leaf_with_bucket_bits(
                &input,
                spec.bucket_bits,
                &KeyIndexLeafLimits::default(),
            )
            .expect("encode leaf");
            let mut bytes = encoded.bytes;
            if let (LeafKind::Flip(at), 0) = (&spec.leaf, s) {
                let at = at % bytes.len();
                bytes[at] ^= 0x5a;
            }
            let mut leaf_ref = encoded.leaf_ref;
            leaf_ref.key = format!("t/{}/kidx/{p}/{s}", TENANT.to_hex());
            store
                .put(&leaf_ref.key, Bytes::from(bytes), PutOptions::default())
                .await
                .expect("put leaf");
            refs.push(leaf_ref);
        }
        handles.push(SnapshotPartHandle {
            blake3: part_blake3.to_vec(),
            entry_count: n,
            watermark_hour: spec.watermark_hour,
            key_index: refs,
        });
        for (o, entry_rows) in spec.entries.iter().enumerate() {
            if spec.present[o] {
                origin.push(Some(SegmentPartOrigin {
                    part: p as u32,
                    ordinal: o as u32,
                }));
                segments.push(segment(segments.len()));
                seg_rows.push(entry_rows.clone());
            }
        }
    }
    for recent in recents {
        origin.push(None);
        segments.push(segment(segments.len()));
        seg_rows.push(recent.clone());
    }
    let catalog = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
    Fixture {
        catalog,
        store,
        snapshot: Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        parts: SnapshotParts::new(handles, origin),
        rows: seg_rows,
    }
}

/// Counts every read it serves, independently of the accounting handle.
struct CountingReader {
    inner: StoreLeafReader,
    calls: AtomicU64,
    bytes: AtomicU64,
}

impl CountingReader {
    fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        CountingReader {
            inner: StoreLeafReader::new(store),
            calls: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }
}

impl LeafRangeReader for CountingReader {
    fn read_range<'a>(
        &'a self,
        leaf: &'a SnapshotKeyIndexLeafRef,
        range: Range<u64>,
        accounting: &'a QueryAccounting,
    ) -> BoxFuture<'a, Result<Bytes, LeafReadError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let bytes = self.inner.read_range(leaf, range, accounting).await?;
            self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            Ok(bytes)
        })
    }
}

/// Refuses any reservation above `max` bytes.
struct ThresholdBudget {
    max: u64,
    refused: AtomicU64,
}

impl ThresholdBudget {
    fn new(max: u64) -> Self {
        ThresholdBudget {
            max,
            refused: AtomicU64::new(0),
        }
    }
}

impl LeafDecodeBudget for ThresholdBudget {
    fn try_reserve(&self, bytes: u64) -> Result<LeafDecodeGuard, BudgetRefused> {
        if bytes > self.max {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return Err(BudgetRefused { bytes });
        }
        Ok(LeafDecodeGuard::new(()))
    }
}

fn key_set(key_type: KeyType, values: &BTreeSet<u16>) -> KeySet {
    KeySet::new(
        FIELD,
        key_type,
        values.iter().map(|v| value_key8(key_type, *v)).collect(),
    )
    .expect("key set")
}

struct Run {
    result: Result<(Vec<SegmentRoute>, RouteFigures), RouteError>,
    accounting: PhaseAccounting,
    reader: CountingReader,
    totals: RouteTotals,
}

async fn run(fx: &Fixture, keys: &KeySet, budget: &dyn LeafDecodeBudget, ceiling: Ceiling) -> Run {
    run_on(fx, keys, budget, ceiling, fx.store.clone()).await
}

/// [`run`] with the leaves read through `store` instead of the fixture's.
async fn run_on(
    fx: &Fixture,
    keys: &KeySet,
    budget: &dyn LeafDecodeBudget,
    ceiling: Ceiling,
    store: Arc<dyn ObjectStoreBackend>,
) -> Run {
    let accounting = PhaseAccounting::new();
    let reader = CountingReader::new(store);
    let totals = RouteTotals::new();
    let result = route_under(
        input(fx, keys, &accounting, &reader, budget),
        ceiling,
        &totals,
    )
    .await;
    Run {
        result,
        accounting,
        reader,
        totals,
    }
}

fn input<'a>(
    fx: &'a Fixture,
    keys: &'a KeySet,
    accounting: &'a PhaseAccounting,
    reader: &'a dyn LeafRangeReader,
    budget: &'a dyn LeafDecodeBudget,
) -> RouteInput<'a> {
    RouteInput {
        catalog: &fx.catalog,
        snapshot: &fx.snapshot,
        parts: &fx.parts,
        signal: SIGNAL,
        tenant_hash: TENANT,
        key_set: keys,
        accounting,
        reader,
        budget,
        limits: KeyIndexLeafLimits::default(),
    }
}

/// Every way `routes` disagrees with a whole scan of the segments for
/// `query`: a pruned segment holding a key, or a block set missing a block
/// that holds one.
fn violations(routes: &[SegmentRoute], rows: &[Rows], query: &BTreeSet<u16>) -> Vec<String> {
    let mut found = Vec::new();
    for (i, (route, seg)) in routes.iter().zip(rows).enumerate() {
        let needed: BTreeSet<u32> = seg
            .iter()
            .filter(|(v, _)| query.contains(v))
            .flat_map(|(_, blocks)| blocks.iter().copied())
            .collect();
        match route {
            SegmentRoute::Pruned if !needed.is_empty() => {
                found.push(format!("segment {i} pruned, holds blocks {needed:?}"));
            }
            SegmentRoute::Blocks(blocks) => {
                let got: BTreeSet<u32> = blocks.iter().copied().collect();
                if !needed.is_subset(&got) || blocks.is_empty() {
                    found.push(format!(
                        "segment {i} routed to {blocks:?}, needs {needed:?}"
                    ));
                }
            }
            _ => {}
        }
    }
    found
}

/// The leaf GET, wire-byte and decompressed figures agree with the reader's
/// own counts and with the caller's `keyIndex` handle, and no other phase
/// moved.
fn assert_accounting(run: &Run, figures: &RouteFigures) {
    let key_index = run.accounting.key_index().snapshot();
    let calls = run.reader.calls.load(Ordering::Relaxed);
    let bytes = run.reader.bytes.load(Ordering::Relaxed);
    assert_eq!(
        key_index.s3_requests(AccountedOp::Get),
        figures.leaf_get_requests
    );
    assert_eq!(
        key_index.s3_bytes(AccountedOp::Get),
        figures.leaf_wire_bytes
    );
    assert_eq!(figures.leaf_get_requests, calls);
    assert_eq!(figures.leaf_wire_bytes, bytes);
    let phases = run.accounting.snapshot();
    for other in [&phases.resolve, &phases.plan, &phases.probe, &phases.scan] {
        assert_eq!(other.total_s3_requests(), 0);
        assert_eq!(other.decompressed_bytes, 0);
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(future)
}

const UNBOUNDED: Ceiling = ADR_CEILING;

fn leaf_kind() -> impl Strategy<Value = LeafKind> {
    prop_oneof![
        4 => Just(LeafKind::Good),
        2 => Just(LeafKind::Split),
        1 => Just(LeafKind::NoLeaf),
        1 => Just(LeafKind::OtherField),
        1 => any::<usize>().prop_map(LeafKind::Flip),
        1 => Just(LeafKind::StalePart),
        1 => Just(LeafKind::WrongKeyType),
    ]
}

fn rows_strategy() -> impl Strategy<Value = Rows> {
    prop::collection::btree_map(0u16..30, prop::collection::btree_set(0u32..8, 1..=3), 0..=4)
}

fn part_spec() -> impl Strategy<Value = PartSpec> {
    (1usize..=6).prop_flat_map(|n| {
        (
            prop::collection::vec(rows_strategy(), n),
            prop::collection::vec(prop::bool::weighted(0.25), n),
            prop::collection::vec(prop::bool::weighted(0.8), n),
            0u32..4,
            1u8..=8,
            leaf_kind(),
        )
            .prop_map(
                |(entries, uncovered, present, watermark_hour, bucket_bits, leaf)| PartSpec {
                    entries,
                    uncovered,
                    present,
                    watermark_hour,
                    bucket_bits,
                    leaf,
                    leaf_rows: None,
                },
            )
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn router_never_prunes_a_segment_the_oracle_returns(
        specs in prop::collection::vec(part_spec(), 1..=5),
        recents in prop::collection::vec(rows_strategy(), 0..=2),
        id16 in any::<bool>(),
        present_keys in prop::collection::btree_set(0u16..40, 1..=6),
        extra_keys in prop_oneof![3 => Just(0usize), 1 => 0usize..=1018],
        ceiling_gets in prop_oneof![Just(KEY_INDEX_LEAF_GET_CEILING as u64), 1u64..=150],
        ceiling_parts in 1usize..=6,
        budget_max in prop::option::weighted(0.2, 64u64..4096),
    ) {
        let key_type = if id16 { KeyType::Id16 } else { KeyType::Str };
        let mut query = present_keys.clone();
        query.extend((0..extra_keys).map(|i| 1000 + i as u16));
        let keys = key_set(key_type, &query);
        let ceiling = Ceiling { leaf_gets: ceiling_gets, parts: ceiling_parts };
        let budget = ThresholdBudget::new(budget_max.unwrap_or(u64::MAX));
        let (fx, run) = block_on(async {
            let fx = fixture(&specs, &recents, key_type).await;
            let run = run(&fx, &keys, &budget, ceiling).await;
            (fx, run)
        });
        let (routes, figures) = run.result.as_ref().expect("route");
        prop_assert_eq!(routes.len(), fx.snapshot.segments.len());
        let bad = violations(routes, &fx.rows, &query);
        prop_assert!(bad.is_empty(), "{:?}", bad);

        // Recent segments, and every segment of a part whose leaf cannot
        // answer, stay with tier 1.
        for (route, origin) in routes.iter().zip(&fx.parts.origin) {
            let never_answers = match origin {
                None => true,
                Some(o) => matches!(
                    specs[o.part as usize].leaf,
                    LeafKind::NoLeaf | LeafKind::OtherField | LeafKind::StalePart
                        | LeafKind::WrongKeyType
                ),
            };
            if never_answers {
                prop_assert_eq!(route, &SegmentRoute::Uncovered);
            }
        }

        let with_segments = specs.iter().filter(|s| s.present.iter().any(|p| *p)).count() as u64;
        prop_assert_eq!(
            figures.parts_covered + figures.parts_uncovered + figures.parts_beyond_ceiling
                + figures.leaves_rejected + figures.reservations_refused
                + figures.skipped_by_budget,
            with_segments
        );
        prop_assert!(figures.leaf_get_requests <= ceiling.leaf_gets);
        let routed_parts: BTreeSet<u32> = routes
            .iter()
            .zip(&fx.parts.origin)
            .filter(|(r, _)| **r != SegmentRoute::Uncovered)
            .filter_map(|(_, o)| o.map(|o| o.part))
            .collect();
        prop_assert!(routed_parts.len() <= ceiling.parts);
        prop_assert!(figures.parts_covered as usize <= ceiling.parts);
        prop_assert_eq!(figures.keys_probed, query.len() as u64);
        prop_assert_eq!(budget.refused.load(Ordering::Relaxed), figures.reservations_refused);
        assert_accounting(&run, figures);
    }
}

#[tokio::test]
async fn covered_entries_prune_or_name_blocks_and_uncovered_entries_stay_uncovered() {
    let mut spec = PartSpec::simple(vec![
        rows(&[(1, &[0, 2])]),
        rows(&[(2, &[1])]),
        rows(&[(1, &[3])]),
        rows(&[(3, &[4]), (1, &[5])]),
    ]);
    spec.uncovered[2] = true;
    let fx = fixture(&[spec], &[rows(&[(9, &[0])])], KeyType::Str).await;
    let query = BTreeSet::from([1u16, 3]);
    let budget = ThresholdBudget::new(u64::MAX);
    let run = run(&fx, &key_set(KeyType::Str, &query), &budget, UNBOUNDED).await;
    let (routes, figures) = run.result.as_ref().expect("route");
    assert_eq!(
        routes,
        &vec![
            SegmentRoute::Blocks(vec![0, 2]),
            SegmentRoute::Pruned,
            SegmentRoute::Uncovered,
            SegmentRoute::Blocks(vec![4, 5]),
            SegmentRoute::Uncovered,
        ]
    );
    assert_eq!(figures.parts_covered, 1);
    assert!(violations(routes, &fx.rows, &query).is_empty());
    assert_accounting(&run, figures);
}

#[tokio::test]
async fn ceiling_admits_the_largest_parts_in_order() {
    // Every part holds the key in each entry, so each probe issues exactly
    // its worst case of 2 GETs (prefix and one bucket) and nothing is
    // refunded.
    let part = |entry_count: usize, watermark_hour: u32| {
        let mut spec = PartSpec::simple(vec![rows(&[(1, &[0])]); entry_count]);
        spec.watermark_hour = watermark_hour;
        spec
    };
    let specs: Vec<PartSpec> = [3, 7, 5, 9, 1].into_iter().map(|n| part(n, 0)).collect();
    let fx = fixture(&specs, &[], KeyType::Id16).await;
    let query = BTreeSet::from([1u16]);
    let keys = key_set(KeyType::Id16, &query);
    let budget = ThresholdBudget::new(u64::MAX);
    let admitted_routes = |routes: &[SegmentRoute], fx: &Fixture| -> BTreeSet<u32> {
        routes
            .iter()
            .zip(&fx.parts.origin)
            .filter(|(r, _)| **r != SegmentRoute::Uncovered)
            .map(|(_, o)| o.expect("sealed").part)
            .collect()
    };

    // L_max room for exactly 2 parts: the two largest by entry_count.
    let ceiling = Ceiling {
        leaf_gets: 5,
        parts: KEY_INDEX_MAX_PROBED_PARTS,
    };
    let run_gets = run(&fx, &keys, &budget, ceiling).await;
    let (routes, figures) = run_gets.result.as_ref().expect("route");
    assert_eq!(figures.parts_covered, 2);
    assert_eq!(figures.parts_beyond_ceiling, 3);
    assert_eq!(figures.leaf_get_requests, 4);
    assert_eq!(admitted_routes(routes, &fx), BTreeSet::from([3, 1]));
    for (route, origin) in routes.iter().zip(&fx.parts.origin) {
        let part = origin.expect("sealed").part;
        let expected = if part == 3 || part == 1 {
            SegmentRoute::Blocks(vec![0])
        } else {
            SegmentRoute::Uncovered
        };
        assert_eq!(route, &expected);
    }
    assert_accounting(&run_gets, figures);

    // P_max room for exactly 2 parts: the same two.
    let run_parts = run(
        &fx,
        &keys,
        &budget,
        Ceiling {
            leaf_gets: KEY_INDEX_LEAF_GET_CEILING as u64,
            parts: 2,
        },
    )
    .await;
    let (routes, figures) = run_parts.result.as_ref().expect("route");
    assert_eq!(figures.parts_beyond_ceiling, 3);
    assert_eq!(admitted_routes(routes, &fx), BTreeSet::from([3, 1]));

    // Equal entry_count: the latest watermark_hour first.
    let tied: Vec<PartSpec> = [5, 9, 7].into_iter().map(|h| part(2, h)).collect();
    let fx = fixture(&tied, &[], KeyType::Id16).await;
    let run_tied = run(
        &fx,
        &keys,
        &budget,
        Ceiling {
            leaf_gets: 2,
            parts: KEY_INDEX_MAX_PROBED_PARTS,
        },
    )
    .await;
    let (routes, figures) = run_tied.result.as_ref().expect("route");
    assert_eq!(figures.parts_beyond_ceiling, 2);
    assert_eq!(admitted_routes(routes, &fx), BTreeSet::from([1]));
}

#[tokio::test]
async fn refunded_gets_admit_the_next_part() {
    // Each part's worst case is 2 GETs; a key in an empty bucket costs only
    // the prefix, so the refund lets a third part in under a 4-GET ceiling.
    let mut specs: Vec<PartSpec> = (0..3)
        .map(|i| PartSpec::simple(vec![rows(&[(1, &[0])]); 3 - i]))
        .collect();
    for spec in &mut specs {
        spec.bucket_bits = 1;
    }
    let fx = fixture(&specs, &[], KeyType::Str).await;
    // A value whose bucket under 1 bit is empty in every part.
    let absent = (100u16..)
        .find(|v| {
            let probe = value_key8(KeyType::Str, *v);
            ravel_codec::key_idx::bucket_index(&probe, 1).expect("bucket")
                != ravel_codec::key_idx::bucket_index(&value_key8(KeyType::Str, 1), 1)
                    .expect("bucket")
        })
        .expect("a value in the other bucket");
    let query = BTreeSet::from([absent]);
    let budget = ThresholdBudget::new(u64::MAX);
    let run = run(
        &fx,
        &key_set(KeyType::Str, &query),
        &budget,
        Ceiling {
            leaf_gets: 4,
            parts: KEY_INDEX_MAX_PROBED_PARTS,
        },
    )
    .await;
    let (routes, figures) = run.result.as_ref().expect("route");
    assert_eq!(figures.parts_covered, 3);
    assert_eq!(figures.parts_beyond_ceiling, 0);
    assert_eq!(figures.leaf_get_requests, 3);
    assert!(routes.iter().all(|r| *r == SegmentRoute::Pruned));
}

#[tokio::test]
async fn stale_part_binding_leaves_the_part_uncovered() {
    // The segment holds key 1; the leaf indexes the previous part, where only
    // key 2 was. A reader that skipped the binding check would prune it.
    let mut spec = PartSpec::simple(vec![rows(&[(1, &[0])])]);
    spec.leaf = LeafKind::StalePart;
    spec.leaf_rows = Some(vec![rows(&[(2, &[0])])]);
    let fx = fixture(&[spec], &[], KeyType::Str).await;
    let query = BTreeSet::from([1u16]);
    let budget = ThresholdBudget::new(u64::MAX);
    let run = run(&fx, &key_set(KeyType::Str, &query), &budget, UNBOUNDED).await;
    let (routes, figures) = run.result.as_ref().expect("route");
    assert_eq!(routes, &vec![SegmentRoute::Uncovered]);
    assert_eq!(figures.leaves_rejected, 1);
    assert_eq!(figures.parts_covered, 0);
    assert!(violations(routes, &fx.rows, &query).is_empty());
}

#[tokio::test]
async fn another_tenants_leaf_fails_the_statement_and_counts_a_breach() {
    let good = PartSpec::simple(vec![rows(&[(1, &[0])])]);
    let mut foreign = PartSpec::simple(vec![rows(&[(1, &[0])])]);
    foreign.leaf = LeafKind::OtherTenant;
    let fx = fixture(&[good, foreign], &[], KeyType::Id16).await;
    let before = fx.catalog.isolation_breaches();
    let budget = ThresholdBudget::new(u64::MAX);
    let run = run(
        &fx,
        &key_set(KeyType::Id16, &BTreeSet::from([1u16])),
        &budget,
        UNBOUNDED,
    )
    .await;
    match &run.result {
        Err(RouteError::Isolation { leaf_key, source }) => {
            assert_eq!(leaf_key, &fx.parts.parts[1].key_index[0].key);
            assert!(
                matches!(source, CatalogError::FieldMismatch { field, .. } if *field == "tenant_hash"),
                "{source:?}"
            );
        }
        other => panic!("expected an isolation error, got {other:?}"),
    }
    assert_eq!(fx.catalog.isolation_breaches(), before + 1);
    // The reads issued before the failure are still charged.
    assert_eq!(
        run.accounting
            .key_index()
            .snapshot()
            .s3_requests(AccountedOp::Get),
        run.reader.calls.load(Ordering::Relaxed)
    );
}

#[tokio::test]
async fn a_refused_reservation_leaves_the_part_uncovered() {
    // Part 0's frame for key 1 carries 300 blocks, part 1's one block: a
    // 1,000-byte ceiling refuses only part 0's decoded bound.
    let wide: Vec<u32> = (0..300).collect();
    let big = PartSpec::simple(vec![rows(&[(1, &wide)]), rows(&[(2, &[0])])]);
    let small = PartSpec::simple(vec![rows(&[(1, &[0])]), rows(&[(3, &[1])])]);
    let fx = fixture(&[big, small], &[rows(&[(1, &[0])])], KeyType::Str).await;
    let query = BTreeSet::from([1u16]);
    let keys = key_set(KeyType::Str, &query);

    let unbounded = ThresholdBudget::new(u64::MAX);
    let full = run(&fx, &keys, &unbounded, UNBOUNDED).await;
    let (full_routes, full_figures) = full.result.as_ref().expect("route");
    let tight = ThresholdBudget::new(1_000);
    let refused = run(&fx, &keys, &tight, UNBOUNDED).await;
    let (routes, figures) = refused.result.as_ref().expect("route");

    assert_eq!(tight.refused.load(Ordering::Relaxed), 1);
    assert_eq!(
        figures,
        &RouteFigures {
            parts_covered: full_figures.parts_covered - 1,
            reservations_refused: 1,
            ..*full_figures
        }
    );
    assert_eq!(full_figures.reservations_refused, 0);
    assert_eq!(
        full_routes,
        &vec![
            SegmentRoute::Blocks(wide.clone()),
            SegmentRoute::Pruned,
            SegmentRoute::Blocks(vec![0]),
            SegmentRoute::Pruned,
            SegmentRoute::Uncovered,
        ]
    );
    assert_eq!(
        routes,
        &vec![
            SegmentRoute::Uncovered,
            SegmentRoute::Uncovered,
            SegmentRoute::Blocks(vec![0]),
            SegmentRoute::Pruned,
            SegmentRoute::Uncovered,
        ]
    );
    assert!(violations(routes, &fx.rows, &query).is_empty());
    assert!(violations(full_routes, &fx.rows, &query).is_empty());
    assert_accounting(&refused, figures);
}

#[tokio::test]
async fn a_dropped_key_is_caught_by_the_oracle() {
    let spec = PartSpec::simple(vec![
        rows(&[(1, &[0])]),
        rows(&[(2, &[3])]),
        rows(&[(4, &[1])]),
    ]);
    let fx = fixture(&[spec], &[], KeyType::Id16).await;
    let query = BTreeSet::from([1u16, 2]);
    let budget = ThresholdBudget::new(u64::MAX);

    let whole = run(&fx, &key_set(KeyType::Id16, &query), &budget, UNBOUNDED).await;
    let (routes, _) = whole.result.as_ref().expect("route");
    assert!(violations(routes, &fx.rows, &query).is_empty());

    let last = *query.last().expect("a key");
    let dropped: BTreeSet<u16> = query.iter().copied().filter(|v| *v != last).collect();
    let short = run(&fx, &key_set(KeyType::Id16, &dropped), &budget, UNBOUNDED).await;
    let (routes, _) = short.result.as_ref().expect("route");
    assert_eq!(
        violations(routes, &fx.rows, &query),
        vec!["segment 1 pruned, holds blocks {3}".to_string()]
    );
}

#[tokio::test]
async fn leaf_reads_are_charged_to_the_key_index_phase_with_exact_decompressed_bytes() {
    let mut split = PartSpec::simple(
        (0..6u16)
            .map(|i| rows(&[(i, &[u32::from(i)]), (i + 10, &[0, 1])]))
            .collect(),
    );
    split.leaf = LeafKind::Split;
    split.bucket_bits = 3;
    let plain = PartSpec::simple(vec![rows(&[(1, &[2]), (12, &[4])]), rows(&[(5, &[0])])]);
    let fx = fixture(&[split, plain], &[], KeyType::Str).await;
    let query = BTreeSet::from([1u16, 5, 12, 13, 99]);
    let keys = key_set(KeyType::Str, &query);
    let budget = Arc::new(ravel_memory::MemoryBudget::new(1 << 30));
    let run = run(&fx, &keys, &budget, UNBOUNDED).await;
    let (routes, figures) = run.result.as_ref().expect("route");
    assert!(violations(routes, &fx.rows, &query).is_empty());
    assert_eq!(figures.parts_covered, 2);
    assert_accounting(&run, figures);
    assert_eq!(budget.reserved(), 0, "every decode guard is released");

    // The frames the key set touches, read back from the stored leaves.
    let mut expected_decompressed = 0u64;
    let mut expected_gets = 0u64;
    for handle in &fx.parts.parts {
        let request = KeyIndexLeafRequest {
            tenant_hash: TENANT,
            signal: SIGNAL,
            part_blake3: &handle.blake3,
            part_entry_count: handle.entry_count,
            field: FIELD,
            key_type: KeyType::Str,
        };
        for leaf_ref in &handle.key_index {
            let leaf = fx
                .store
                .get(&leaf_ref.key, GetRange::Full)
                .await
                .expect("leaf")
                .data;
            let prefix = KeyIndexLeafPrefix::parse(
                &leaf[..leaf_ref.prefix_len as usize],
                leaf_ref,
                &request,
            )
            .expect("prefix");
            let in_slice: Vec<&Key8> = keys
                .keys()
                .iter()
                .filter(|k| {
                    (leaf_ref.key8_lo..=leaf_ref.key8_hi).contains(&u64::from_be_bytes(**k))
                })
                .collect();
            if in_slice.is_empty() {
                continue;
            }
            expected_gets += 1;
            let mut buckets = BTreeMap::new();
            for key8 in in_slice {
                if let Some(located) = prefix.locate(key8).expect("locate") {
                    buckets.insert(located.bucket, located.range);
                }
            }
            for range in buckets.values() {
                expected_gets += 1;
                let at = range.start as usize + 4;
                let len = u32::from_le_bytes(leaf[at..at + 4].try_into().expect("u32"));
                expected_decompressed += u64::from(len);
            }
        }
    }
    let key_index = run.accounting.key_index().snapshot();
    assert!(expected_decompressed > 0);
    assert_eq!(key_index.decompressed_bytes, expected_decompressed);
    assert_eq!(figures.leaf_get_requests, expected_gets);
}

#[tokio::test]
async fn origins_that_do_not_match_the_snapshot_are_invalid_input() {
    let mut fx = fixture(
        &[PartSpec::simple(vec![rows(&[(1, &[0])])])],
        &[],
        KeyType::Str,
    )
    .await;
    fx.snapshot.segments.push(segment(9));
    let budget = ThresholdBudget::new(u64::MAX);
    let run = run(
        &fx,
        &key_set(KeyType::Str, &BTreeSet::from([1u16])),
        &budget,
        UNBOUNDED,
    )
    .await;
    assert!(matches!(run.result, Err(RouteError::InvalidInput(_))));
}

#[tokio::test]
async fn the_bridged_byte_budget_is_the_parts_not_each_slices() {
    // Str keys hash to key8 (an Id16 key8 is the id's first 8 bytes, and
    // the fixture's ids all sit below the cut), so the set tiles both
    // slices of a split part. It is sized so that each
    // slice's bridged runs fit the part's allowance (`entry_count x
    // KEY_INDEX_T1_BYTES`, entry_count 1 here) on their own while their sum
    // does not. The allowance is the part's, so the second slice must
    // decline and the part reads skipped_by_budget.
    // FLIP: handing each slice the full allowance (the pre-fix form) answers
    // both slices and covers the part.
    let cap = ravel_catalog::KEY_INDEX_T1_BYTES;
    let cut = 1u64 << 63;
    let budget = ThresholdBudget::new(u64::MAX);
    let permissive = KeyIndexSetPolicy {
        b_run: KEY_INDEX_SET_PROBE_MAX_RUNS,
        max_bridged_bytes: u64::MAX,
    };
    let mut chosen = None;
    for n in (128u16..=1000).step_by(32) {
        let values: BTreeSet<u16> = (0..n).collect();
        let entry: Rows = values
            .iter()
            .map(|v| (*v, BTreeSet::from([0u32])))
            .collect();
        let mut spec = PartSpec::simple(vec![entry]);
        spec.leaf = LeafKind::Split;
        spec.bucket_bits = 8;
        let fx = fixture(&[spec], &[], KeyType::Str).await;
        let keys = key_set(KeyType::Str, &values);
        let handle = &fx.parts.parts[0];
        assert_eq!(handle.key_index.len(), 2, "a split part has two slices");
        let request = KeyIndexLeafRequest {
            tenant_hash: TENANT,
            signal: SIGNAL,
            part_blake3: &handle.blake3,
            part_entry_count: handle.entry_count,
            field: keys.field(),
            key_type: keys.key_type(),
        };
        let reader = CountingReader::new(fx.store.clone());
        let accounting = QueryAccounting::new();
        let mut per_slice = Vec::new();
        for (s, leaf_ref) in handle.key_index.iter().enumerate() {
            let slice_keys: Vec<Key8> = keys
                .keys()
                .iter()
                .copied()
                .filter(|k| (u64::from_be_bytes(*k) < cut) == (s == 0))
                .collect();
            let probe = fx
                .catalog
                .probe_key_index_leaf_set_with(
                    leaf_ref,
                    &request,
                    &slice_keys,
                    &KeyIndexLeafLimits::default(),
                    |range| {
                        let read = reader.read_range(leaf_ref, range, &accounting);
                        async move { read.await.map_err(|e| e.to_string()) }
                    },
                    &permissive,
                    |bytes| budget.try_reserve(bytes).ok(),
                )
                .await
                .expect("probe");
            assert!(
                matches!(probe.answer, KeyIndexSetAnswer::Answered { .. }),
                "slice {s} answers under a permissive policy: {:?}",
                probe.answer
            );
            per_slice.push(probe.bridged_bytes);
        }
        let (b0, b1) = (per_slice[0], per_slice[1]);
        if b0 > 0 && b1 > 0 && b0 <= cap && b1 <= cap && b0 + b1 > cap {
            chosen = Some((fx, keys, values, b0, b1));
            break;
        }
    }
    let (fx, keys, values, b0, b1) =
        chosen.expect("a key count whose slices each fit the part's allowance but not together");

    let both = run(&fx, &keys, &budget, ADR_CEILING).await;
    let (routes, figures) = both.result.as_ref().expect("route");
    assert_eq!(
        figures.skipped_by_budget, 1,
        "slices {b0} + {b1} over {cap}"
    );
    assert_eq!(figures.parts_covered, 0);
    assert!(routes.iter().all(|r| *r == SegmentRoute::Uncovered));

    // The same keys confined to the first slice fit the allowance alone and
    // the part is covered: the decline above was the sum, not one slice.
    let low: BTreeSet<u16> = values
        .iter()
        .copied()
        .filter(|v| u64::from_be_bytes(value_key8(KeyType::Str, *v)) < cut)
        .collect();
    let one = run(&fx, &key_set(KeyType::Str, &low), &budget, ADR_CEILING).await;
    let (_, figures) = one.result.as_ref().expect("route");
    assert_eq!(
        figures.skipped_by_budget, 0,
        "slice 0 alone reads {b0} of {cap}"
    );
    assert_eq!(figures.parts_covered, 1);
}

/// Two `Str` values whose key8s fall either side of a split part's cut.
fn values_across_the_cut() -> (u16, u16) {
    let cut = 1u64 << 63;
    let below = |v: &u16| u64::from_be_bytes(value_key8(KeyType::Str, *v)) < cut;
    let lo = (0u16..).find(below).expect("a value below the cut");
    let hi = (0u16..).find(|v| !below(v)).expect("a value above the cut");
    (lo, hi)
}

/// A route future dropped while a leaf GET is in flight still charges the
/// GETs it issued to the caller's `keyIndex` handle and to the process
/// totals.
/// FLIP: merging the leaf accounting after `route_parts` returns (rather
/// than on drop) leaves both at zero here.
#[tokio::test]
async fn a_dropped_route_future_still_charges_the_leaf_gets_it_issued() {
    let fx = fixture(
        &[PartSpec::simple(vec![rows(&[(1, &[0])])])],
        &[],
        KeyType::Str,
    )
    .await;
    let leaf_key = fx.parts.parts[0].key_index[0].key.clone();
    let faults = Arc::new(FaultStore::new(fx.store.clone(), FaultPlan::empty()));
    // The prefix GET is served; the bucket GET is held and never released.
    let gate = faults.hold(Op::Get, Some(leaf_key), Occurrence::Nth(2));
    let keys = key_set(KeyType::Str, &BTreeSet::from([1u16]));
    let accounting = PhaseAccounting::new();
    let reader = StoreLeafReader::new(faults.clone());
    let budget = ThresholdBudget::new(u64::MAX);
    let totals = RouteTotals::new();
    let mut future = Box::pin(route_under(
        input(&fx, &keys, &accounting, &reader, &budget),
        ADR_CEILING,
        &totals,
    ));
    tokio::select! {
        _ = &mut future => panic!("the route finished past a held GET"),
        _ = gate.wait_until_held(1) => {}
    }
    assert_eq!(
        accounting
            .key_index()
            .snapshot()
            .s3_requests(AccountedOp::Get),
        0
    );
    drop(future);
    let key_index = accounting.key_index().snapshot();
    assert_eq!(
        key_index.s3_requests(AccountedOp::Get),
        2,
        "prefix and bucket GETs"
    );
    assert!(
        key_index.s3_bytes(AccountedOp::Get) > 0,
        "the prefix's bytes"
    );
    assert_eq!(totals.leaf_gets(SIGNAL), 2);
    for outcome in KeyIndexPartOutcome::ALL {
        assert_eq!(
            totals.parts(SIGNAL, outcome),
            0,
            "no outcome for an unfinished pass"
        );
    }
}

/// The production spans reader charges a leaf GET when it is issued, not
/// when it returns: a route dropped while its bucket GET is held still
/// counts both GETs in `keyIndex`, on the uncached and the cached path.
/// FLIP: `record_s3_request` after the `store.get` await in
/// `SpanSegmentFetcher::read_object_extent` leaves the held GET uncounted
/// (1 instead of 2) on both paths.
#[tokio::test]
async fn a_dropped_span_leaf_read_still_charges_the_get_it_issued() {
    use crate::span_fetcher::{SpanLeafRangeReader, SpanSegmentFetcher};
    use ravel_cache::{Cache, CacheLimits};

    for cached in [false, true] {
        let fx = fixture(
            &[PartSpec::simple(vec![rows(&[(1, &[0])])])],
            &[],
            KeyType::Str,
        )
        .await;
        let leaf_key = fx.parts.parts[0].key_index[0].key.clone();
        let faults = Arc::new(FaultStore::new(fx.store.clone(), FaultPlan::empty()));
        let gate = faults.hold(Op::Get, Some(leaf_key), Occurrence::Nth(2));
        let mut fetcher = SpanSegmentFetcher::new(faults.clone());
        if cached {
            let cache: Arc<Cache<crate::fetcher::CacheFetchError>> =
                Arc::new(Cache::new(CacheLimits::new(1 << 20, 100, 1 << 20)));
            fetcher = fetcher.with_cache(cache);
        }
        let reader = SpanLeafRangeReader::new(&fetcher, TENANT);
        let keys = key_set(KeyType::Str, &BTreeSet::from([1u16]));
        let accounting = PhaseAccounting::new();
        let budget = ThresholdBudget::new(u64::MAX);
        let totals = RouteTotals::new();
        let mut future = Box::pin(route_under(
            input(&fx, &keys, &accounting, &reader, &budget),
            ADR_CEILING,
            &totals,
        ));
        tokio::select! {
            _ = &mut future => panic!("the route finished past a held GET (cached {cached})"),
            _ = gate.wait_until_held(1) => {}
        }
        drop(future);
        let key_index = accounting.key_index().snapshot();
        assert_eq!(
            key_index.s3_requests(AccountedOp::Get),
            2,
            "prefix and the held bucket GET (cached {cached})"
        );
        assert!(
            key_index.s3_bytes(AccountedOp::Get) > 0,
            "the prefix's bytes (cached {cached})"
        );
        assert_eq!(totals.leaf_gets(SIGNAL), 2, "cached {cached}");
    }
}

/// The logs reader charges a leaf GET when it is issued, the same as the
/// spans reader above: a route dropped while its bucket GET is held still
/// counts both GETs in `keyIndex`, on the uncached and the cached path.
/// FLIP: moving either `record_s3_request` in
/// `LogSegmentFetcher::read_leaf_range` after its `store.get` await leaves
/// the held GET uncounted on that path (1 instead of 2).
#[tokio::test]
async fn a_dropped_log_leaf_read_still_charges_the_get_it_issued() {
    use crate::log_fetcher::{LogLeafRangeReader, LogSegmentFetcher};
    use ravel_cache::{Cache, CacheLimits};

    for cached in [false, true] {
        let fx = fixture(
            &[PartSpec::simple(vec![rows(&[(1, &[0])])])],
            &[],
            KeyType::Str,
        )
        .await;
        let leaf_key = fx.parts.parts[0].key_index[0].key.clone();
        let faults = Arc::new(FaultStore::new(fx.store.clone(), FaultPlan::empty()));
        let gate = faults.hold(Op::Get, Some(leaf_key), Occurrence::Nth(2));
        let mut fetcher = LogSegmentFetcher::new(faults.clone());
        if cached {
            let cache: Arc<Cache<crate::fetcher::CacheFetchError>> =
                Arc::new(Cache::new(CacheLimits::new(1 << 20, 100, 1 << 20)));
            fetcher = fetcher.with_cache(cache);
        }
        let reader = LogLeafRangeReader::new(&fetcher, TENANT);
        let keys = key_set(KeyType::Str, &BTreeSet::from([1u16]));
        let accounting = PhaseAccounting::new();
        let budget = ThresholdBudget::new(u64::MAX);
        let totals = RouteTotals::new();
        let mut future = Box::pin(route_under(
            input(&fx, &keys, &accounting, &reader, &budget),
            ADR_CEILING,
            &totals,
        ));
        tokio::select! {
            _ = &mut future => panic!("the route finished past a held GET (cached {cached})"),
            _ = gate.wait_until_held(1) => {}
        }
        drop(future);
        let key_index = accounting.key_index().snapshot();
        assert_eq!(
            key_index.s3_requests(AccountedOp::Get),
            2,
            "prefix and the held bucket GET (cached {cached})"
        );
        assert!(
            key_index.s3_bytes(AccountedOp::Get) > 0,
            "the prefix's bytes (cached {cached})"
        );
        assert_eq!(totals.leaf_gets(SIGNAL), 2, "cached {cached}");
    }
}

/// A logs leaf range the fetcher's memory budget refuses is a refused
/// reservation, not a rejected leaf: the part reads uncovered, no GET is
/// issued, and the pass counts it under `reservationsRefused`.
/// FLIP: mapping `FetchMemoryExhausted` to `LeafReadError::Failed` in
/// `LogLeafRangeReader::read_range` counts it under `leavesRejected`.
#[tokio::test]
async fn a_refused_log_leaf_fetch_counts_as_a_refused_reservation() {
    use crate::log_fetcher::{LogLeafRangeReader, LogSegmentFetcher};

    let fx = fixture(
        &[PartSpec::simple(vec![rows(&[(1, &[0])])])],
        &[],
        KeyType::Str,
    )
    .await;
    let fetcher = LogSegmentFetcher::new(fx.store.clone())
        .with_memory_budget(Arc::new(ravel_memory::MemoryBudget::new(1)));
    let reader = LogLeafRangeReader::new(&fetcher, TENANT);
    let keys = key_set(KeyType::Str, &BTreeSet::from([1u16]));
    let accounting = PhaseAccounting::new();
    let budget = ThresholdBudget::new(u64::MAX);
    let totals = RouteTotals::new();
    let (routes, figures) = route_under(
        input(&fx, &keys, &accounting, &reader, &budget),
        ADR_CEILING,
        &totals,
    )
    .await
    .expect("route");

    assert_eq!(routes, vec![SegmentRoute::Uncovered]);
    assert_eq!(figures.reservations_refused, 1);
    assert_eq!(figures.leaves_rejected, 0);
    assert_eq!(figures.parts_covered, 0);
    assert_eq!(
        totals.parts(SIGNAL, KeyIndexPartOutcome::ReservationRefused),
        1
    );
    assert_eq!(totals.parts(SIGNAL, KeyIndexPartOutcome::Rejected), 0);
    assert_eq!(
        accounting
            .key_index()
            .snapshot()
            .s3_requests(AccountedOp::Get),
        0,
        "the refusal comes before the GET"
    );
}

/// The spans twin of the logs refusal above: a spans leaf range the
/// fetcher's memory budget refuses counts under `reservationsRefused`, not
/// `leavesRejected`, issues no GET, and leaves the part uncovered, so the
/// segment falls back to a full scan where the unrefused route answered
/// block 0.
/// FLIP: mapping `SpanFetchError::FetchMemoryExhausted` to
/// `LeafReadError::Failed` in `SpanLeafRangeReader::read_range` counts it
/// under `leavesRejected`.
#[tokio::test]
async fn a_refused_span_leaf_fetch_counts_as_a_refused_reservation() {
    use crate::span_fetcher::{SpanLeafRangeReader, SpanSegmentFetcher};

    let fx = fixture(
        &[PartSpec::simple(vec![rows(&[(1, &[0])])])],
        &[],
        KeyType::Str,
    )
    .await;
    let keys = key_set(KeyType::Str, &BTreeSet::from([1u16]));
    let budget = ThresholdBudget::new(u64::MAX);

    let unrefused = SpanSegmentFetcher::new(fx.store.clone());
    let reader = SpanLeafRangeReader::new(&unrefused, TENANT);
    let accounting = PhaseAccounting::new();
    let totals = RouteTotals::new();
    let (routes, figures) = route_under(
        input(&fx, &keys, &accounting, &reader, &budget),
        ADR_CEILING,
        &totals,
    )
    .await
    .expect("route");
    assert_eq!(routes, vec![SegmentRoute::Blocks(vec![0])]);
    assert_eq!(figures.parts_covered, 1);
    assert_eq!(figures.reservations_refused, 0);

    let fetcher = SpanSegmentFetcher::new(fx.store.clone())
        .with_memory_budget(Arc::new(ravel_memory::MemoryBudget::new(1)));
    let reader = SpanLeafRangeReader::new(&fetcher, TENANT);
    let accounting = PhaseAccounting::new();
    let totals = RouteTotals::new();
    let (routes, figures) = route_under(
        input(&fx, &keys, &accounting, &reader, &budget),
        ADR_CEILING,
        &totals,
    )
    .await
    .expect("route");

    // Uncovered scans every block, so block 0's rows still reach the answer.
    assert_eq!(routes, vec![SegmentRoute::Uncovered]);
    assert_eq!(figures.reservations_refused, 1);
    assert_eq!(figures.leaves_rejected, 0);
    assert_eq!(figures.parts_covered, 0);
    assert_eq!(
        totals.parts(SIGNAL, KeyIndexPartOutcome::ReservationRefused),
        1
    );
    assert_eq!(totals.parts(SIGNAL, KeyIndexPartOutcome::Rejected), 0);
    assert_eq!(
        accounting
            .key_index()
            .snapshot()
            .s3_requests(AccountedOp::Get),
        0,
        "the refusal comes before the GET"
    );
}

/// The control for the spans refusal: a spans leaf GET that fails in the
/// store is still a rejected leaf, not a refused reservation.
/// FLIP: mapping every `SpanFetchError` to `LeafReadError::ReservationRefused`
/// in `SpanLeafRangeReader::read_range` counts it under `reservationsRefused`.
#[tokio::test]
async fn a_failed_span_leaf_get_still_counts_as_a_rejected_leaf() {
    use crate::span_fetcher::{SpanLeafRangeReader, SpanSegmentFetcher};

    let fx = fixture(
        &[PartSpec::simple(vec![rows(&[(1, &[0])])])],
        &[],
        KeyType::Str,
    )
    .await;
    let leaf_key = fx.parts.parts[0].key_index[0].key.clone();
    let faults = Arc::new(FaultStore::new(
        fx.store.clone(),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("leaf gone".into()))
                .with_key_contains(leaf_key)
                .with_occurrence(Occurrence::Nth(1)),
        ),
    ));
    let fetcher = SpanSegmentFetcher::new(faults.clone());
    let reader = SpanLeafRangeReader::new(&fetcher, TENANT);
    let keys = key_set(KeyType::Str, &BTreeSet::from([1u16]));
    let accounting = PhaseAccounting::new();
    let budget = ThresholdBudget::new(u64::MAX);
    let totals = RouteTotals::new();
    let (routes, figures) = route_under(
        input(&fx, &keys, &accounting, &reader, &budget),
        ADR_CEILING,
        &totals,
    )
    .await
    .expect("route");

    assert_eq!(faults.fault_count(Op::Get, FaultKind::Permanent), 1);
    assert_eq!(routes, vec![SegmentRoute::Uncovered]);
    assert_eq!(figures.leaves_rejected, 1);
    assert_eq!(figures.reservations_refused, 0);
    assert_eq!(totals.parts(SIGNAL, KeyIndexPartOutcome::Rejected), 1);
    assert_eq!(
        totals.parts(SIGNAL, KeyIndexPartOutcome::ReservationRefused),
        0
    );
}

/// Admission walks the parts largest first and stops at the first whose
/// worst case does not fit; it does not skip it for a smaller part that
/// would (ADR-3692 decision 3, "until admitting the next part's worst case
/// would exceed L_max"). The large part tiles two slices with one key each
/// (worst 2 x (1 + 1) = 4); the small part holds both keys in one slice
/// (worst 1 + 2 = 3). Under a 3-GET ceiling a first-fit admission would
/// probe the small part; largest-first probes nothing.
/// FLIP: `continue` past a part that does not fit (first fit) covers the
/// small part under the 3-GET ceiling.
#[tokio::test]
async fn admission_is_largest_first_and_stops_at_the_first_part_that_does_not_fit() {
    let (lo, hi) = values_across_the_cut();
    let both = rows(&[(lo, &[0]), (hi, &[1])]);
    let mut large = PartSpec::simple(vec![both.clone(); 4]);
    large.leaf = LeafKind::Split;
    let small = PartSpec::simple(vec![both]);
    let fx = fixture(&[large, small], &[], KeyType::Str).await;
    let query = BTreeSet::from([lo, hi]);
    let keys = key_set(KeyType::Str, &query);
    let worst: Vec<u64> = fx
        .parts
        .parts
        .iter()
        .enumerate()
        .map(|(p, handle)| {
            plan_part(p, handle, &keys)
                .expect("plan")
                .expect("covered")
                .worst_gets
        })
        .collect();
    assert_eq!(worst, vec![4, 3], "fixture: the two parts' worst cases");
    let budget = ThresholdBudget::new(u64::MAX);
    let ceiling = |leaf_gets| Ceiling {
        leaf_gets,
        parts: KEY_INDEX_MAX_PROBED_PARTS,
    };

    let tight = run(&fx, &keys, &budget, ceiling(3)).await;
    let (routes, figures) = tight.result.as_ref().expect("route");
    assert_eq!(
        *figures,
        RouteFigures {
            parts_beyond_ceiling: 2,
            keys_probed: 2,
            ..RouteFigures::default()
        }
    );
    assert!(routes.iter().all(|r| *r == SegmentRoute::Uncovered));
    assert_eq!(tight.reader.calls.load(Ordering::Relaxed), 0);

    // At 4 the large part is admitted and spends its whole worst case, so
    // the small one still waits.
    let four = run(&fx, &keys, &budget, ceiling(4)).await;
    let (_, figures) = four.result.as_ref().expect("route");
    assert_eq!(figures.parts_covered, 1);
    assert_eq!(figures.parts_beyond_ceiling, 1);
    assert_eq!(figures.leaf_get_requests, 4);
    assert_accounting(&four, figures);
}

/// Leaf refs whose slices do not tile the key space reject the part before
/// any leaf is read.
/// FLIP: dropping `check_slice_tiling` from `select_key_index_slice` routes
/// the part through its first slice.
#[tokio::test]
async fn slices_that_do_not_tile_the_key_space_reject_the_part_before_any_get() {
    let (lo, hi) = values_across_the_cut();
    let mut split = PartSpec::simple(vec![rows(&[(lo, &[0]), (hi, &[1])])]);
    split.leaf = LeafKind::Split;
    let mut fx = fixture(&[split], &[], KeyType::Str).await;
    // A one-key gap between the two slices.
    fx.parts.parts[0].key_index[1].key8_lo += 1;
    let budget = ThresholdBudget::new(u64::MAX);
    let run = run(
        &fx,
        &key_set(KeyType::Str, &BTreeSet::from([lo])),
        &budget,
        UNBOUNDED,
    )
    .await;
    let (routes, figures) = run.result.as_ref().expect("route");
    assert_eq!(routes, &vec![SegmentRoute::Uncovered]);
    assert_eq!(figures.leaves_rejected, 1);
    assert_eq!(figures.leaf_get_requests, 0);
    assert_eq!(run.reader.calls.load(Ordering::Relaxed), 0);
}

/// A leaf GET that fails leaves the part uncovered and counted rejected; the
/// statement goes on. The fault fires on the bucket GET, after the prefix.
#[tokio::test]
async fn a_failed_leaf_get_leaves_the_part_uncovered() {
    let fx = fixture(
        &[
            PartSpec::simple(vec![rows(&[(1, &[0])])]),
            PartSpec::simple(vec![rows(&[(1, &[2])]), rows(&[(2, &[0])])]),
        ],
        &[],
        KeyType::Str,
    )
    .await;
    let failing = fx.parts.parts[1].key_index[0].key.clone();
    let faults = Arc::new(FaultStore::new(
        fx.store.clone(),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("leaf gone".into()))
                .with_key_contains(failing)
                .with_occurrence(Occurrence::Nth(2)),
        ),
    ));
    let budget = ThresholdBudget::new(u64::MAX);
    let run = run_on(
        &fx,
        &key_set(KeyType::Str, &BTreeSet::from([1u16])),
        &budget,
        UNBOUNDED,
        faults.clone(),
    )
    .await;
    assert_eq!(faults.fault_count(Op::Get, FaultKind::Permanent), 1);
    let (routes, figures) = run.result.as_ref().expect("route");
    assert_eq!(
        routes,
        &vec![
            SegmentRoute::Blocks(vec![0]),
            SegmentRoute::Uncovered,
            SegmentRoute::Uncovered,
        ]
    );
    assert_eq!(figures.parts_covered, 1);
    assert_eq!(figures.leaves_rejected, 1);
    // The failed GET was issued and is charged; it returned no bytes.
    assert_eq!(figures.leaf_get_requests, 4);
}

/// A flipped byte under the directory's crc, or under the bucket frame's,
/// leaves the part uncovered and counted rejected.
#[tokio::test]
async fn a_leaf_failing_its_crc_leaves_the_part_uncovered() {
    let entries = vec![rows(&[(1, &[0])])];
    let good = fixture(&[PartSpec::simple(entries.clone())], &[], KeyType::Str).await;
    let leaf_ref = &good.parts.parts[0].key_index[0];
    let prefix_len = leaf_ref.prefix_len as usize;
    let size = good.store.head(&leaf_ref.key).await.expect("leaf").size as usize;
    // One entry, so one non-empty bucket: the leaf's last frame.
    for at in [prefix_len - 1, size - 1] {
        let mut spec = PartSpec::simple(entries.clone());
        spec.leaf = LeafKind::Flip(at);
        let fx = fixture(&[spec], &[], KeyType::Str).await;
        let budget = ThresholdBudget::new(u64::MAX);
        let run = run(
            &fx,
            &key_set(KeyType::Str, &BTreeSet::from([1u16])),
            &budget,
            UNBOUNDED,
        )
        .await;
        let (routes, figures) = run.result.as_ref().expect("route");
        assert_eq!(routes, &vec![SegmentRoute::Uncovered], "flip at {at}");
        assert_eq!(figures.leaves_rejected, 1, "flip at {at}");
        assert_eq!(figures.parts_covered, 0, "flip at {at}");
    }
}

/// The signal half of the leaf isolation check: a leaf naming another
/// signal fails the statement and counts a breach, like another tenant's.
#[tokio::test]
async fn another_signals_leaf_fails_the_statement_and_counts_a_breach() {
    let mut foreign = PartSpec::simple(vec![rows(&[(1, &[0])])]);
    foreign.leaf = LeafKind::OtherSignal;
    let fx = fixture(&[foreign], &[], KeyType::Id16).await;
    let before = fx.catalog.isolation_breaches();
    let budget = ThresholdBudget::new(u64::MAX);
    let run = run(
        &fx,
        &key_set(KeyType::Id16, &BTreeSet::from([1u16])),
        &budget,
        UNBOUNDED,
    )
    .await;
    match &run.result {
        Err(RouteError::Isolation { source, .. }) => assert!(
            matches!(source, CatalogError::FieldMismatch { field, .. } if *field == "signal"),
            "{source:?}"
        ),
        other => panic!("expected an isolation error, got {other:?}"),
    }
    assert_eq!(fx.catalog.isolation_breaches(), before + 1);
    assert_eq!(run.totals.leaf_gets(SIGNAL), 1, "the prefix GET is counted");
}

/// The process totals add each finished pass's six part outcomes and its
/// leaf GETs under the pass's signal, and nothing under another.
#[tokio::test]
async fn route_totals_add_every_part_outcome_and_leaf_get_under_the_signal() {
    let mut stale = PartSpec::simple(vec![rows(&[(1, &[0])])]);
    stale.leaf = LeafKind::StalePart;
    stale.leaf_rows = Some(vec![rows(&[(2, &[0])])]);
    let mut none = PartSpec::simple(vec![rows(&[(1, &[0])])]);
    none.leaf = LeafKind::NoLeaf;
    let specs = [
        PartSpec::simple(vec![rows(&[(1, &[0])]); 3]),
        none,
        stale,
        PartSpec::simple(vec![rows(&[(1, &[1])])]),
    ];
    let fx = fixture(&specs, &[], KeyType::Str).await;
    let keys = key_set(KeyType::Str, &BTreeSet::from([1u16]));
    let budget = ThresholdBudget::new(u64::MAX);
    // Room for the 3-entry part and the stale one, not the last.
    let run = run(
        &fx,
        &keys,
        &budget,
        Ceiling {
            leaf_gets: 4,
            parts: KEY_INDEX_MAX_PROBED_PARTS,
        },
    )
    .await;
    let (_, figures) = run.result.as_ref().expect("route");
    assert_eq!(
        *figures,
        RouteFigures {
            parts_covered: 1,
            parts_uncovered: 1,
            parts_beyond_ceiling: 1,
            leaves_rejected: 1,
            keys_probed: 1,
            ..*figures
        }
    );
    for outcome in KeyIndexPartOutcome::ALL {
        assert_eq!(
            run.totals.parts(SIGNAL, outcome),
            figures.parts(outcome),
            "{}",
            outcome.as_str()
        );
        assert_eq!(run.totals.parts(Signal::Logs, outcome), 0);
        assert_eq!(run.totals.parts(Signal::Metrics, outcome), 0);
    }
    assert_eq!(run.totals.leaf_gets(SIGNAL), figures.leaf_get_requests);
    assert!(figures.leaf_get_requests > 0);
    assert_eq!(run.totals.leaf_gets(Signal::Logs), 0);
    assert_eq!(run.totals.leaf_gets(Signal::Metrics), 0);
    assert_eq!(
        KeyIndexPartOutcome::ALL.map(KeyIndexPartOutcome::as_str),
        [
            "covered",
            "uncovered",
            "beyond_ceiling",
            "rejected",
            "reservation_refused",
            "skipped_by_budget",
        ]
    );
}

/// Spans, logs and metrics each have their own row (ADR-3694 decision 3
/// routes metrics), and a metrics pass counts under its own row.
#[test]
fn every_routed_signal_has_its_own_row() {
    assert_eq!(
        key_index_routed_signals(),
        &[Signal::Spans, Signal::Logs, Signal::Metrics]
    );
    let totals = RouteTotals::new();
    let figures = RouteFigures {
        parts_covered: 2,
        ..RouteFigures::default()
    };
    totals.add_parts(Signal::Metrics, &figures);
    totals.add_leaf_gets(Signal::Metrics, 5);
    assert_eq!(
        totals.parts(Signal::Metrics, KeyIndexPartOutcome::Covered),
        2
    );
    assert_eq!(totals.leaf_gets(Signal::Metrics), 5);
    for signal in [Signal::Spans, Signal::Logs] {
        assert_eq!(totals.parts(signal, KeyIndexPartOutcome::Covered), 0);
        assert_eq!(totals.leaf_gets(signal), 0);
    }
}

#[test]
fn key_sets_are_sorted_distinct_and_bounded() {
    let a = value_key8(KeyType::Str, 1);
    let b = value_key8(KeyType::Str, 2);
    let set = KeySet::new(FIELD, KeyType::Str, vec![b, a, b]).expect("set");
    let mut sorted = vec![a, b];
    sorted.sort_unstable();
    assert_eq!(set.keys(), sorted.as_slice());
    assert_eq!(
        KeySet::new(FIELD, KeyType::Str, Vec::new()),
        Err(KeySetError::Empty)
    );
    let many: Vec<Key8> = (0..=KEY_SET_MAX_KEYS as u64)
        .map(u64::to_be_bytes)
        .collect();
    assert_eq!(
        KeySet::new(FIELD, KeyType::Str, many),
        Err(KeySetError::TooMany(KEY_SET_MAX_KEYS + 1))
    );
    let max: Vec<Key8> = (0..KEY_SET_MAX_KEYS as u64).map(u64::to_be_bytes).collect();
    assert_eq!(
        KeySet::new(FIELD, KeyType::Str, max)
            .expect("k keys")
            .keys()
            .len(),
        KEY_SET_MAX_KEYS
    );
}

#[test]
fn the_route_future_is_send() {
    fn assert_send<T: Send>(_: &T) {}
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
    let snapshot = Snapshot {
        segments: Vec::new(),
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let parts = SnapshotParts::new(Vec::new(), Vec::new());
    let keys = key_set(KeyType::Str, &BTreeSet::from([1u16]));
    let accounting = PhaseAccounting::new();
    let reader = StoreLeafReader::new(store);
    let budget = ThresholdBudget::new(u64::MAX);
    let future = route(RouteInput {
        catalog: &catalog,
        snapshot: &snapshot,
        parts: &parts,
        signal: SIGNAL,
        tenant_hash: TENANT,
        key_set: &keys,
        accounting: &accounting,
        reader: &reader,
        budget: &budget,
        limits: KeyIndexLeafLimits::default(),
    });
    assert_send(&future);
    let (routes, figures) = block_on(future).expect("route");
    assert!(routes.is_empty());
    assert_eq!(
        figures,
        RouteFigures {
            keys_probed: 1,
            ..RouteFigures::default()
        }
    );
}

fn value_input<'a>(
    fx: &'a Fixture,
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
        field: FIELD,
        accounting,
        reader,
        budget,
        limits: KeyIndexLeafLimits::default(),
        concurrency: 2,
    }
}

#[tokio::test]
async fn each_routed_value_reports_its_own_pass_and_what_it_alone_pruned() {
    // value-1 is missing only from segment 3; value-2 is missing from
    // segments 1, 2 and 3. Segment 3 is pruned by both values, so only
    // segments 1 and 2 are value-2's alone.
    let spec = PartSpec::simple(vec![
        rows(&[(1, &[0]), (2, &[1])]),
        rows(&[(1, &[0])]),
        rows(&[(1, &[2])]),
        rows(&[(3, &[0])]),
    ]);
    let fx = fixture(&[spec], &[], KeyType::Str).await;
    let accounting = PhaseAccounting::new();
    let reader = CountingReader::new(fx.store.clone());
    let budget = ThresholdBudget::new(u64::MAX);
    let totals = RouteTotals::new();
    let plans = vec![vec![
        ("__name__", "value-1"),
        ("job", "value-2"),
        ("env", "value-2"),
    ]];
    let routing = route_values_under(
        value_input(&fx, &accounting, &reader, &budget),
        &plans,
        UNBOUNDED,
        &totals,
    )
    .await
    .expect("route");

    assert_eq!(
        routing.plan_keeps,
        vec![Some(vec![true, false, false, false])]
    );
    assert_eq!(routing.values.len(), 2);
    let (first, second) = (&routing.values[0], &routing.values[1]);
    assert_eq!(first.labels, vec!["__name__".to_string()]);
    assert_eq!(second.labels, vec!["env".to_string(), "job".to_string()]);
    assert_eq!(first.key8_hex, hex::encode(key8_str("value-1")));
    assert_eq!(second.key8_hex, hex::encode(key8_str("value-2")));
    assert_eq!(first.key8_hex.len(), 16);
    assert_eq!((first.segments_pruned, second.segments_pruned), (1, 3));
    assert_eq!(
        (first.segments_pruned_alone, second.segments_pruned_alone),
        (0, 2)
    );
    for value in &routing.values {
        assert_eq!(value.figures.keys_probed, 1);
        assert_eq!(value.figures.parts_covered, 1);
        assert!(value.figures.leaf_get_requests > 0);
    }

    let key_index = accounting.key_index().snapshot();
    let leaf_gets: u64 = routing
        .values
        .iter()
        .map(|v| v.figures.leaf_get_requests)
        .sum();
    let leaf_bytes: u64 = routing
        .values
        .iter()
        .map(|v| v.figures.leaf_wire_bytes)
        .sum();
    assert_eq!(leaf_gets, key_index.s3_requests(AccountedOp::Get));
    assert_eq!(leaf_gets, key_index.total_s3_requests());
    assert_eq!(leaf_gets, reader.calls.load(Ordering::Relaxed));
    assert_eq!(leaf_bytes, key_index.s3_bytes(AccountedOp::Get));
}

#[tokio::test]
async fn a_route_that_did_not_run_reports_no_value() {
    let mut spec = PartSpec::simple(vec![rows(&[(1, &[0])]), rows(&[(2, &[0])])]);
    spec.leaf = LeafKind::OtherField;
    let fx = fixture(&[spec], &[], KeyType::Str).await;
    let accounting = PhaseAccounting::new();
    let reader = CountingReader::new(fx.store.clone());
    let budget = ThresholdBudget::new(u64::MAX);
    let totals = RouteTotals::new();
    let plans = vec![vec![("__name__", "value-1")]];
    let routing = route_values_under(
        value_input(&fx, &accounting, &reader, &budget),
        &plans,
        UNBOUNDED,
        &totals,
    )
    .await
    .expect("route");
    assert_eq!(routing, ValueRouting::unrouted(1));
    assert!(routing.values.is_empty());
    assert_eq!(reader.calls.load(Ordering::Relaxed), 0);
    assert_eq!(accounting.key_index().snapshot().total_s3_requests(), 0);
}

#[tokio::test]
async fn a_pending_erasure_routes_no_value() {
    let mut fx = fixture(
        &[PartSpec::simple(vec![
            rows(&[(1, &[0])]),
            rows(&[(2, &[0])]),
        ])],
        &[],
        KeyType::Str,
    )
    .await;
    fx.snapshot
        .pending_erasure
        .push(ravel_proto::commit::v1::ErasureRequest::default());
    let accounting = PhaseAccounting::new();
    let reader = CountingReader::new(fx.store.clone());
    let budget = ThresholdBudget::new(u64::MAX);
    let totals = RouteTotals::new();
    let plans = vec![vec![("__name__", "value-1")]];
    let routing = route_values_under(
        value_input(&fx, &accounting, &reader, &budget),
        &plans,
        UNBOUNDED,
        &totals,
    )
    .await
    .expect("route");
    assert_eq!(routing, ValueRouting::unrouted(1));
    assert!(routing.values.is_empty());
    assert_eq!(reader.calls.load(Ordering::Relaxed), 0);
    assert_eq!(accounting.key_index().snapshot().total_s3_requests(), 0);
}

#[tokio::test]
async fn a_value_only_another_plan_carries_prunes_nothing_from_this_plan() {
    // Plan A holds value-1; plan B holds value-1 and value-2. Segment 0
    // holds both values, segment 1 only value-1, segment 2 neither, and
    // segment 3 only value-2.
    let spec = PartSpec::simple(vec![
        rows(&[(1, &[0]), (2, &[1])]),
        rows(&[(1, &[0])]),
        rows(&[(3, &[0])]),
        rows(&[(2, &[0])]),
    ]);
    let fx = fixture(&[spec], &[], KeyType::Str).await;
    let accounting = PhaseAccounting::new();
    let reader = CountingReader::new(fx.store.clone());
    let budget = ThresholdBudget::new(u64::MAX);
    let totals = RouteTotals::new();
    let plans = vec![
        vec![("__name__", "value-1")],
        vec![("__name__", "value-1"), ("job", "value-2")],
    ];
    let routing = route_values_under(
        value_input(&fx, &accounting, &reader, &budget),
        &plans,
        UNBOUNDED,
        &totals,
    )
    .await
    .expect("route");

    // value-2 prunes segment 1, which plan A reads and plan B drops.
    assert_eq!(
        routing.plan_keeps,
        vec![
            Some(vec![true, true, false, false]),
            Some(vec![true, false, false, false]),
        ]
    );
    assert_eq!(routing.values.len(), 2);
    let (first, second) = (&routing.values[0], &routing.values[1]);
    assert_eq!(first.key8_hex, hex::encode(key8_str("value-1")));
    assert_eq!(second.key8_hex, hex::encode(key8_str("value-2")));
    assert_eq!(first.labels, vec!["__name__".to_string()]);
    assert_eq!(second.labels, vec!["job".to_string()]);
    assert_eq!((first.segments_pruned, second.segments_pruned), (2, 2));
    // value-1 alone pruned segment 3 in both plans and segment 2 in plan A,
    // which holds no other value, though value-2 also pruned segment 2 in
    // plan B. value-2 alone pruned segment 1 only: in plan B, the one plan
    // holding it, value-1 also pruned segment 2.
    assert_eq!(
        (first.segments_pruned_alone, second.segments_pruned_alone),
        (2, 1)
    );
}

/// Serves a range it has already read from memory as a cache hit with no
/// GET, as `read_leaf_range` does on `ReadOutcome::Hit`; any other range is
/// one GET through the store, a miss.
struct CachingLeafReader {
    store: CountingReader,
    cached: std::sync::Mutex<BTreeMap<(String, u64, u64), Bytes>>,
    hits: AtomicU64,
}

impl CachingLeafReader {
    fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        CachingLeafReader {
            store: CountingReader::new(store),
            cached: std::sync::Mutex::new(BTreeMap::new()),
            hits: AtomicU64::new(0),
        }
    }
}

impl LeafRangeReader for CachingLeafReader {
    fn read_range<'a>(
        &'a self,
        leaf: &'a SnapshotKeyIndexLeafRef,
        range: Range<u64>,
        accounting: &'a QueryAccounting,
    ) -> BoxFuture<'a, Result<Bytes, LeafReadError>> {
        Box::pin(async move {
            let key = (leaf.key.clone(), range.start, range.end);
            let cached = self.cached.lock().expect("cache lock").get(&key).cloned();
            if let Some(bytes) = cached {
                self.hits.fetch_add(1, Ordering::Relaxed);
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                return Ok(bytes);
            }
            let bytes = self.store.read_range(leaf, range, accounting).await?;
            accounting.record_cache_miss();
            self.cached
                .lock()
                .expect("cache lock")
                .insert(key, bytes.clone());
            Ok(bytes)
        })
    }
}

#[tokio::test]
async fn a_passes_leaf_cache_hits_reach_its_figures() {
    let fx = fixture(
        &[PartSpec::simple(vec![
            rows(&[(1, &[0]), (2, &[1])]),
            rows(&[(1, &[0])]),
        ])],
        &[],
        KeyType::Str,
    )
    .await;
    let reader = CachingLeafReader::new(fx.store.clone());
    let budget = ThresholdBudget::new(u64::MAX);
    let totals = RouteTotals::new();

    // A cold pass for value-1 warms the reader.
    let cold_accounting = PhaseAccounting::new();
    let cold = route_values_under(
        value_input(&fx, &cold_accounting, &reader, &budget),
        &[vec![("__name__", "value-1")]],
        UNBOUNDED,
        &totals,
    )
    .await
    .expect("cold route");
    let cold_gets = reader.store.calls.load(Ordering::Relaxed);
    assert!(cold_gets > 0);
    assert_eq!(cold.values[0].figures.leaf_get_requests, cold_gets);
    assert_eq!(cold.values[0].figures.leaf_cache_hits, 0);

    // value-1's second pass reads the same ranges, all cached; value-2's
    // pass, issued after it, reads whatever was not.
    let accounting = PhaseAccounting::new();
    let routing = route_values_under(
        ValueRouteInput {
            concurrency: 1,
            ..value_input(&fx, &accounting, &reader, &budget)
        },
        &[vec![("__name__", "value-1"), ("job", "value-2")]],
        UNBOUNDED,
        &totals,
    )
    .await
    .expect("route");
    let warm = &routing.values[0].figures;
    assert_eq!(routing.values[0].key8_hex, hex::encode(key8_str("value-1")));
    assert_eq!(warm.leaf_cache_hits, cold_gets);
    assert_eq!(warm.leaf_get_requests, 0);
    assert_eq!(warm.leaf_wire_bytes, 0);

    let store_gets = reader.store.calls.load(Ordering::Relaxed) - cold_gets;
    let hits = reader.hits.load(Ordering::Relaxed);
    let key_index = accounting.key_index().snapshot();
    let leaf_gets: u64 = routing
        .values
        .iter()
        .map(|v| v.figures.leaf_get_requests)
        .sum();
    let leaf_hits: u64 = routing
        .values
        .iter()
        .map(|v| v.figures.leaf_cache_hits)
        .sum();
    assert_eq!(leaf_hits, hits);
    assert_eq!(leaf_hits, key_index.cache_hits);
    assert_eq!(leaf_gets, store_gets);
    assert_eq!(leaf_gets, key_index.s3_requests(AccountedOp::Get));
    assert_eq!(leaf_gets, key_index.total_s3_requests());
}

/// Pending this many times, waking itself each time: a read that has to be
/// polled again.
struct Yields(u32);

impl std::future::Future for Yields {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 == 0 {
            return std::task::Poll::Ready(());
        }
        self.0 -= 1;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

/// Decrements the in-flight count when the read finishes or is dropped.
struct InFlightGuard<'a>(&'a AtomicU64);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Counts reads in flight around `inner`: a read joins on its first poll,
/// yields once, and leaves when it returns.
struct InFlightReader<'r> {
    inner: &'r CountingReader,
    in_flight: AtomicU64,
    peak: AtomicU64,
    /// Every range read, in issue order.
    ranges: std::sync::Mutex<Vec<(String, Range<u64>)>>,
}

impl<'r> InFlightReader<'r> {
    fn new(inner: &'r CountingReader) -> Self {
        InFlightReader {
            inner,
            in_flight: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            ranges: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn peak(&self) -> u64 {
        self.peak.load(Ordering::SeqCst)
    }
}

impl LeafRangeReader for InFlightReader<'_> {
    fn read_range<'a>(
        &'a self,
        leaf: &'a SnapshotKeyIndexLeafRef,
        range: Range<u64>,
        accounting: &'a QueryAccounting,
    ) -> BoxFuture<'a, Result<Bytes, LeafReadError>> {
        Box::pin(async move {
            self.ranges
                .lock()
                .expect("ranges")
                .push((leaf.key.clone(), range.clone()));
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            let _guard = InFlightGuard(&self.in_flight);
            self.peak.fetch_max(now, Ordering::SeqCst);
            Yields(1).await;
            self.inner.read_range(leaf, range, accounting).await
        })
    }
}

/// Routes `keys` over `fx` through an [`InFlightReader`], returning the run
/// and the peak number of reads in flight.
async fn run_in_flight(
    fx: &Fixture,
    keys: &KeySet,
    budget: &dyn LeafDecodeBudget,
) -> (Run, u64, Vec<(String, Range<u64>)>) {
    let accounting = PhaseAccounting::new();
    let reader = CountingReader::new(fx.store.clone());
    let totals = RouteTotals::new();
    let (result, peak, ranges) = {
        let in_flight = InFlightReader::new(&reader);
        let result = route_under(
            input(fx, keys, &accounting, &in_flight, budget),
            UNBOUNDED,
            &totals,
        )
        .await;
        let ranges = std::mem::take(&mut *in_flight.ranges.lock().expect("ranges"));
        (result, in_flight.peak(), ranges)
    };
    (
        Run {
            result,
            accounting,
            reader,
            totals,
        },
        peak,
        ranges,
    )
}

/// One part of `slices` slices over four entries: entry `o` holds the
/// values `v` with `v % 4 == o`, in block `v % 8`, and entry 3 is
/// uncovered.
fn sliced_spec(slices: usize, values: u16) -> PartSpec {
    let mut entries: Vec<Rows> = vec![Rows::new(); 4];
    for v in 0..values {
        entries[usize::from(v % 4)].insert(v, BTreeSet::from([u32::from(v % 8)]));
    }
    let mut spec = PartSpec::simple(entries);
    spec.uncovered[3] = true;
    spec.leaf = LeafKind::Sliced(slices);
    spec.bucket_bits = 6;
    spec
}

/// What probing every part's slices one after another gives: the routes,
/// and the GETs, wire bytes and decompressed bytes it cost.
#[derive(Debug)]
struct Serial {
    routes: Vec<SegmentRoute>,
    get_requests: u64,
    wire_bytes: u64,
    decompressed_bytes: u64,
    /// Every read it issued, GET 1s included, in issue order.
    reads: Vec<(String, Range<u64>)>,
}

/// Per slice of part 0, the bucket-run reads among `reads` (every read but
/// the slice's GET 1) and the bytes they ask for.
fn runs_by_slice<'r>(
    fx: &Fixture,
    reads: impl IntoIterator<Item = (&'r str, &'r Range<u64>)>,
) -> Vec<(u64, u64)> {
    let refs = &fx.parts.parts[0].key_index;
    let mut runs = vec![(0u64, 0u64); refs.len()];
    for (leaf, range) in reads {
        let s = slice_of(leaf);
        if key_index_leaf_prefix_range(&refs[s]).expect("range") != *range {
            runs[s].0 += 1;
            runs[s].1 += range.end - range.start;
        }
    }
    runs
}

/// The serial oracle's [`runs_by_slice`] for the values of `query` that
/// fall in part 0's first `slices` slices: what probing those slices one
/// after another reads before the slice that stops it.
async fn serial_runs_before(fx: &Fixture, query: &BTreeSet<u16>, slices: usize) -> Vec<(u64, u64)> {
    let before: BTreeSet<u16> = values_by_slice(fx, query.iter().copied())
        .into_iter()
        .take(slices)
        .flatten()
        .collect();
    let serial = serial_probe(fx, &key_set(KeyType::Str, &before)).await;
    let runs = runs_by_slice(
        fx,
        serial
            .reads
            .iter()
            .map(|(leaf, range)| (leaf.as_str(), range)),
    );
    assert!(
        runs[..slices].iter().all(|(n, _)| *n > 0),
        "every slice before the stopping one reads runs: {runs:?}"
    );
    runs
}

/// The serial oracle: each part's touched slices in slice order, each
/// through `probe_key_index_leaf_set_ranged` with one read in flight at a
/// time, the part's allowance charged slice by slice. Every slice must
/// answer.
async fn serial_probe(fx: &Fixture, keys: &KeySet) -> Serial {
    let one_read = tokio::sync::Mutex::new(());
    let mut serial = Serial {
        routes: vec![SegmentRoute::Uncovered; fx.snapshot.segments.len()],
        get_requests: 0,
        wire_bytes: 0,
        decompressed_bytes: 0,
        reads: Vec::new(),
    };
    let reads = std::sync::Mutex::new(Vec::new());
    let mut answers: BTreeMap<usize, PartAnswer> = BTreeMap::new();
    for (p, handle) in fx.parts.parts.iter().enumerate() {
        let mut slices: Vec<(&SnapshotKeyIndexLeafRef, Vec<Key8>)> = Vec::new();
        for key8 in keys.keys() {
            let leaf_ref = select_key_index_slice(&handle.key_index, FIELD, key8)
                .expect("slices tile")
                .expect("the part has leaves");
            match slices.last_mut() {
                Some((last, slice_keys)) if std::ptr::eq(*last, leaf_ref) => slice_keys.push(*key8),
                _ => slices.push((leaf_ref, vec![*key8])),
            }
        }
        let request = KeyIndexLeafRequest {
            tenant_hash: TENANT,
            signal: SIGNAL,
            part_blake3: &handle.blake3,
            part_entry_count: handle.entry_count,
            field: FIELD,
            key_type: keys.key_type(),
        };
        let mut bridged_left = KeyIndexSetPolicy::for_part(handle.entry_count).max_bridged_bytes;
        let mut answer = PartAnswer {
            blocks: BTreeMap::new(),
            uncovered: BTreeSet::new(),
        };
        for (leaf_ref, slice_keys) in &slices {
            let policy = KeyIndexSetPolicy {
                b_run: KEY_INDEX_SET_PROBE_MAX_RUNS,
                max_bridged_bytes: bridged_left,
            };
            let read = |range: Range<u64>| {
                reads
                    .lock()
                    .expect("reads")
                    .push((leaf_ref.key.clone(), range.clone()));
                let (store, key, one_read) = (fx.store.clone(), leaf_ref.key.clone(), &one_read);
                async move {
                    let _one = one_read.lock().await;
                    store
                        .get(&key, GetRange::Range(range.start, range.end))
                        .await
                        .map(|got| got.data)
                        .map_err(|e| e.to_string())
                }
            };
            let probe = ravel_catalog::probe_key_index_leaf_set_ranged(
                leaf_ref,
                &request,
                slice_keys,
                &KeyIndexLeafLimits::default(),
                &policy,
                read,
                |_| Some(()),
            )
            .await
            .expect("no breach");
            serial.get_requests += u64::from(probe.get_requests);
            serial.wire_bytes += probe.wire_bytes;
            serial.decompressed_bytes += probe.decompressed_bytes;
            bridged_left -= probe.bridged_bytes;
            assert!(
                merge_answer(&mut answer, probe.answer).is_ok(),
                "slice {} answers",
                leaf_ref.key
            );
        }
        answers.insert(p, answer);
    }
    serial.reads = reads.into_inner().expect("reads");
    for (route, origin) in serial.routes.iter_mut().zip(&fx.parts.origin) {
        let Some(origin) = origin else { continue };
        let answer = &answers[&(origin.part as usize)];
        let ordinal = u64::from(origin.ordinal);
        if answer.uncovered.contains(&ordinal) {
            continue;
        }
        *route = match answer.blocks.get(&ordinal) {
            Some(blocks) => SegmentRoute::Blocks(blocks.iter().copied().collect()),
            None => SegmentRoute::Pruned,
        };
    }
    serial
}

/// A part's slices are read concurrently, never more than
/// `LEAF_READ_CONCURRENCY` reads at once, and the routes and the leaf GET,
/// wire-byte and decompressed figures equal those of the serial oracle on
/// the same part.
/// FLIP: the serial probe peaks at 1 read in flight; an unbounded join over
/// the slices peaks at 24.
#[tokio::test]
async fn a_part_reads_its_slices_concurrently() {
    let fx = fixture(&[sliced_spec(24, 600)], &[], KeyType::Str).await;
    let query: BTreeSet<u16> = (0..600).step_by(2).chain(2000..2100).collect();
    let keys = key_set(KeyType::Str, &query);
    let budget = ThresholdBudget::new(u64::MAX);
    let (run, peak, ranges) = run_in_flight(&fx, &keys, &budget).await;
    let (routes, figures) = run.result.as_ref().expect("route");
    let leaves: BTreeSet<&str> = ranges.iter().map(|(leaf, _)| leaf.as_str()).collect();
    assert_eq!(leaves.len(), 24, "the key set touches every slice");
    assert_eq!(peak, ravel_catalog::LEAF_READ_CONCURRENCY as u64);
    assert_eq!(figures.parts_covered, 1);
    let serial = serial_probe(&fx, &keys).await;
    assert_eq!(routes, &serial.routes);
    assert_eq!(figures.leaf_get_requests, serial.get_requests);
    assert_eq!(figures.leaf_wire_bytes, serial.wire_bytes);
    assert_eq!(
        run.accounting.key_index().snapshot().decompressed_bytes,
        serial.decompressed_bytes
    );
    // Even values only, so entries 0 and 2 hold matches and entry 1 holds
    // none; entry 3 is uncovered.
    assert_eq!(
        routes,
        &vec![
            SegmentRoute::Blocks(vec![0, 4]),
            SegmentRoute::Pruned,
            SegmentRoute::Blocks(vec![2, 6]),
            SegmentRoute::Uncovered,
        ]
    );
    assert!(violations(routes, &fx.rows, &query).is_empty());
    // The oracle's figures on this fixture, as literals.
    assert_eq!(figures.leaf_get_requests, 305);
    assert_eq!(figures.leaf_wire_bytes, 18_569);
    assert_eq!(
        run.accounting.key_index().snapshot().decompressed_bytes,
        3_751
    );
    assert_accounting(&run, figures);
}

/// A part whose third slice's bridged bytes exceed what the first two left
/// of the allowance.
struct AllowanceCase {
    fx: Fixture,
    query: BTreeSet<u16>,
    /// The first three slices' bridged bytes for `query`.
    bridged: [u64; 3],
    /// Every slice's GET 1 bytes.
    prefixes: Vec<Bytes>,
}

/// One part of four slices over one entry of 4000 values, and a query
/// whose first two slices fit the part's allowance and whose third, though
/// it fits on its own, exceeds what they left; the fourth slice holds three
/// keys.
async fn allowance_case() -> AllowanceCase {
    let cap = ravel_catalog::KEY_INDEX_T1_BYTES;
    let values: Vec<u16> = (0..4000).collect();
    let entry: Rows = values
        .iter()
        .map(|v| (*v, BTreeSet::from([0u32])))
        .collect();
    let mut spec = PartSpec::simple(vec![entry]);
    spec.leaf = LeafKind::Sliced(4);
    spec.bucket_bits = 8;
    let fx = fixture(&[spec], &[], KeyType::Str).await;
    let handle = &fx.parts.parts[0];
    assert_eq!(handle.key_index.len(), 4);
    let request = KeyIndexLeafRequest {
        tenant_hash: TENANT,
        signal: SIGNAL,
        part_blake3: &handle.blake3,
        part_entry_count: handle.entry_count,
        field: FIELD,
        key_type: KeyType::Str,
    };
    // Per slice, its values ascending by key8, and the leaf's prefix bytes.
    let mut by_slice: Vec<Vec<(Key8, u16)>> = vec![Vec::new(); 4];
    for v in &values {
        let key8 = value_key8(KeyType::Str, *v);
        let s = handle
            .key_index
            .iter()
            .position(|l| (l.key8_lo..=l.key8_hi).contains(&u64::from_be_bytes(key8)))
            .expect("a slice holds every key");
        by_slice[s].push((key8, *v));
    }
    let mut prefixes = Vec::new();
    for (s, leaf_ref) in handle.key_index.iter().enumerate() {
        by_slice[s].sort_unstable();
        let range = key_index_leaf_prefix_range(leaf_ref).expect("prefix range");
        let got = fx
            .store
            .get(&leaf_ref.key, GetRange::Range(range.start, range.end))
            .await
            .expect("prefix");
        prefixes.push(got.data);
    }
    // The bridged bytes of the first `n` keys of slice `s`.
    let bridged = |s: usize, n: usize| -> u64 {
        let keys: Vec<Key8> = by_slice[s][..n].iter().map(|(k, _)| *k).collect();
        plan_key_index_leaf_set(
            &prefixes[s],
            &handle.key_index[s],
            &request,
            &keys,
            KEY_INDEX_SET_PROBE_MAX_RUNS,
        )
        .map(|plan| plan.bridged_bytes())
        .expect("plan")
    };
    let counts: Vec<usize> = (65..=320).step_by(5).collect();
    let mut chosen = None;
    'search: for &n in &counts {
        let (b0, b1) = (bridged(0, n), bridged(1, n));
        if b0 == 0 || b1 == 0 || b0 + b1 > cap {
            continue;
        }
        for &m in &counts {
            let b2 = bridged(2, m);
            if b2 > 0 && b2 <= cap && b0 + b1 + b2 > cap {
                chosen = Some((n, m, [b0, b1, b2]));
                break 'search;
            }
        }
    }
    let (n, m, b) = chosen.expect("key counts whose third slice crosses the allowance");
    let query: BTreeSet<u16> = by_slice[0][..n]
        .iter()
        .chain(&by_slice[1][..n])
        .chain(&by_slice[2][..m])
        .chain(&by_slice[3][..3])
        .map(|(_, v)| *v)
        .collect();
    AllowanceCase {
        fx,
        query,
        bridged: b,
        prefixes,
    }
}

/// The part's allowance is charged from the slices' prefixes in slice
/// order: the third slice's bridged bytes exceed what the first two left,
/// so the part is skipped by budget. The first two slices read exactly the
/// serial probe's runs, in count and bytes per slice, every slice's GET 1
/// is read and counted (the fourth's too), and no run of the third or
/// fourth slice is read.
/// FLIP: charging each slice against the full allowance covers the part;
/// the serial probe never reads the fourth slice's GET 1; skipping slice
/// 0's last run leaves slice 0 one run and its bytes short.
#[tokio::test]
async fn the_allowance_is_charged_in_slice_order() {
    let cap = ravel_catalog::KEY_INDEX_T1_BYTES;
    let AllowanceCase {
        fx,
        query,
        bridged: b,
        prefixes,
    } = allowance_case().await;
    let handle = &fx.parts.parts[0];
    let keys = key_set(KeyType::Str, &query);
    let budget = ThresholdBudget::new(u64::MAX);
    let (run, _, ranges) = run_in_flight(&fx, &keys, &budget).await;
    let (routes, figures) = run.result.as_ref().expect("route");
    assert_eq!(figures.skipped_by_budget, 1, "slices {b:?} over {cap}");
    assert_eq!(figures.parts_covered, 0);
    assert_eq!(routes, &vec![SegmentRoute::Uncovered]);
    let prefix_reads: BTreeMap<String, Range<u64>> = handle
        .key_index
        .iter()
        .map(|l| {
            (
                l.key.clone(),
                key_index_leaf_prefix_range(l).expect("range"),
            )
        })
        .collect();
    // Per slice, its GET 1 reads, and its run reads with their bytes.
    let mut gets_1 = [0u64; 4];
    let mut runs = [0u64; 4];
    let mut run_bytes = 0u64;
    for (leaf, range) in &ranges {
        let s = slice_of(leaf);
        if Some(range) == prefix_reads.get(leaf) {
            gets_1[s] += 1;
        } else {
            runs[s] += 1;
            run_bytes += range.end - range.start;
        }
    }
    assert_eq!(gets_1, [1, 1, 1, 1], "every slice's GET 1 is read once");
    assert_eq!(&runs[2..], &[0, 0], "no run of slice 2 or 3 is read");
    let read = runs_by_slice(
        &fx,
        ranges.iter().map(|(leaf, range)| (leaf.as_str(), range)),
    );
    assert_eq!(
        read[..2],
        serial_runs_before(&fx, &query, 2).await[..2],
        "slices 0 and 1 read the serial probe's runs, (count, bytes) per slice"
    );
    assert_eq!(figures.leaf_get_requests, ranges.len() as u64);
    assert_eq!(
        figures.leaf_wire_bytes,
        prefixes.iter().map(|p| p.len() as u64).sum::<u64>() + run_bytes
    );
    assert_accounting(&run, figures);
}

/// Fails slice 2's GET 1, after slice 5's GET 1 has been refused by the
/// reader's fetch reservation, and records the order the reads ended in.
struct LateRejectReader<'r> {
    inner: &'r CountingReader,
    ended: std::sync::Mutex<Vec<(String, Range<u64>)>>,
}

impl LeafRangeReader for LateRejectReader<'_> {
    fn read_range<'a>(
        &'a self,
        leaf: &'a SnapshotKeyIndexLeafRef,
        range: Range<u64>,
        accounting: &'a QueryAccounting,
    ) -> BoxFuture<'a, Result<Bytes, LeafReadError>> {
        Box::pin(async move {
            let asked = range.clone();
            let got = if leaf.key.ends_with("/2") {
                self.inner.calls.fetch_add(1, Ordering::Relaxed);
                accounting.record_s3_request(AccountedOp::Get);
                Yields(8).await;
                Err(LeafReadError::Failed("slice 2 is gone".to_string()))
            } else if leaf.key.ends_with("/5") {
                self.inner.calls.fetch_add(1, Ordering::Relaxed);
                accounting.record_s3_request(AccountedOp::Get);
                Err(LeafReadError::ReservationRefused(
                    "slice 5's fetch refused".to_string(),
                ))
            } else {
                self.inner.read_range(leaf, range, accounting).await
            };
            self.ended
                .lock()
                .expect("ended")
                .push((leaf.key.clone(), asked));
            got
        })
    }
}

/// When two slices fail, the earlier in slice order decides the part's
/// outcome, not the one whose read ended first: slice 2's failed GET 1
/// rejects the part although slice 5's refused fetch ended before it.
/// Slices 0 and 1 read exactly the serial probe's runs.
/// FLIP: deciding by completion order (an unordered GET 1 stream) counts
/// the part as a refused reservation; skipping slice 0's last run leaves
/// slice 0 one run and its bytes short.
#[tokio::test]
async fn the_first_failing_slice_in_order_decides() {
    let fx = fixture(&[sliced_spec(8, 600)], &[], KeyType::Str).await;
    let query: BTreeSet<u16> = (0..600).collect();
    let keys = key_set(KeyType::Str, &query);
    let budget = ThresholdBudget::new(u64::MAX);
    let accounting = PhaseAccounting::new();
    let reader = CountingReader::new(fx.store.clone());
    let totals = RouteTotals::new();
    let late = LateRejectReader {
        inner: &reader,
        ended: std::sync::Mutex::new(Vec::new()),
    };
    let result = route_under(
        input(&fx, &keys, &accounting, &late, &budget),
        UNBOUNDED,
        &totals,
    )
    .await;
    let ended = std::mem::take(&mut *late.ended.lock().expect("ended"));
    let position = |suffix: &str| {
        ended
            .iter()
            .position(|(k, _)| k.ends_with(suffix))
            .expect("slice read")
    };
    assert!(
        position("/5") < position("/2"),
        "slice 5's read ends first: {ended:?}"
    );
    let (routes, figures) = result.as_ref().expect("route");
    assert_eq!(figures.leaves_rejected, 1);
    assert_eq!(figures.reservations_refused, 0);
    assert!(routes.iter().all(|r| *r == SegmentRoute::Uncovered));
    // Every slice's GET 1, and runs of slices 0 and 1 only: exactly the
    // serial probe's, in count and bytes per slice.
    for s in 2..8 {
        let suffix = format!("/{s}");
        assert_eq!(
            ended.iter().filter(|(k, _)| k.ends_with(&suffix)).count(),
            1,
            "slice {s}: {ended:?}"
        );
    }
    let runs = runs_by_slice(
        &fx,
        ended.iter().map(|(leaf, range)| (leaf.as_str(), range)),
    );
    assert_eq!(
        runs[..2],
        serial_runs_before(&fx, &query, 2).await[..2],
        "(count, bytes) per slice"
    );
    assert_eq!(&runs[2..], &[(0, 0); 6]);
    assert_eq!(figures.leaf_get_requests, ended.len() as u64);
}

/// How a [`ScriptReader`] ends a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    Pass,
    Fail,
    /// Refused by the reader's fetch reservation, before any GET.
    Refuse,
}

/// One read as a [`ScriptReader`]'s script sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScriptedRead {
    slice: usize,
    /// The slice's GET 1, not a bucket run.
    prefix: bool,
    /// Its place among the pass's run reads, in issue order; 0 for GET 1.
    run: u64,
}

/// The slice index a fixture leaf key ends in.
fn slice_of(leaf_key: &str) -> usize {
    leaf_key
        .rsplit('/')
        .next()
        .and_then(|s| s.parse().ok())
        .expect("slice index")
}

/// Reads through `inner` as `script` says: pending for the polls it names,
/// then passing, failing, or refused. Records each read as it ends.
struct ScriptReader<'r, S> {
    inner: &'r CountingReader,
    script: S,
    runs_issued: AtomicU64,
    ended: std::sync::Mutex<Vec<(ScriptedRead, End)>>,
}

impl<S> LeafRangeReader for ScriptReader<'_, S>
where
    S: Fn(ScriptedRead) -> (u32, End) + Sync,
{
    fn read_range<'a>(
        &'a self,
        leaf: &'a SnapshotKeyIndexLeafRef,
        range: Range<u64>,
        accounting: &'a QueryAccounting,
    ) -> BoxFuture<'a, Result<Bytes, LeafReadError>> {
        let prefix = key_index_leaf_prefix_range(leaf).ok() == Some(range.clone());
        let run = if prefix {
            0
        } else {
            self.runs_issued.fetch_add(1, Ordering::SeqCst)
        };
        let read = ScriptedRead {
            slice: slice_of(&leaf.key),
            prefix,
            run,
        };
        let (polls, end) = (self.script)(read);
        Box::pin(async move {
            Yields(polls).await;
            let got = match end {
                End::Pass => self.inner.read_range(leaf, range, accounting).await,
                End::Fail => {
                    self.inner.calls.fetch_add(1, Ordering::Relaxed);
                    accounting.record_s3_request(AccountedOp::Get);
                    Err(LeafReadError::Failed(format!(
                        "{} {range:?} failed",
                        leaf.key
                    )))
                }
                End::Refuse => Err(LeafReadError::ReservationRefused(format!(
                    "{} {range:?} refused",
                    leaf.key
                ))),
            };
            self.ended.lock().expect("ended").push((read, end));
            got
        })
    }
}

/// Routes `keys` over `fx` through a [`ScriptReader`] running `script`,
/// returning the run and the reads in the order they ended.
async fn run_scripted<S>(
    fx: &Fixture,
    keys: &KeySet,
    budget: &dyn LeafDecodeBudget,
    script: S,
) -> (Run, Vec<(ScriptedRead, End)>)
where
    S: Fn(ScriptedRead) -> (u32, End) + Sync,
{
    let accounting = PhaseAccounting::new();
    let reader = CountingReader::new(fx.store.clone());
    let totals = RouteTotals::new();
    let (result, ended) = {
        let scripted = ScriptReader {
            inner: &reader,
            script,
            runs_issued: AtomicU64::new(0),
            ended: std::sync::Mutex::new(Vec::new()),
        };
        let result = route_under(
            input(fx, keys, &accounting, &scripted, budget),
            UNBOUNDED,
            &totals,
        )
        .await;
        let ended = std::mem::take(&mut *scripted.ended.lock().expect("ended"));
        (result, ended)
    };
    (
        Run {
            result,
            accounting,
            reader,
            totals,
        },
        ended,
    )
}

/// `values` grouped by the slice of part 0 whose key8 range holds them.
fn values_by_slice(fx: &Fixture, values: impl IntoIterator<Item = u16>) -> Vec<Vec<u16>> {
    let refs = &fx.parts.parts[0].key_index;
    let mut by_slice = vec![Vec::new(); refs.len()];
    for v in values {
        let key8 = u64::from_be_bytes(value_key8(KeyType::Str, v));
        let s = refs
            .iter()
            .position(|l| (l.key8_lo..=l.key8_hi).contains(&key8))
            .expect("a slice holds every key");
        by_slice[s].push(v);
    }
    by_slice
}

/// The routes a whole scan gives `sliced_spec`'s part for `query`: entry 3
/// uncovered, every other entry the blocks of the queried values it holds.
fn sliced_routes(fx: &Fixture, query: &BTreeSet<u16>) -> Vec<SegmentRoute> {
    fx.rows
        .iter()
        .enumerate()
        .map(|(o, seg)| {
            let blocks: BTreeSet<u32> = seg
                .iter()
                .filter(|(v, _)| query.contains(v))
                .flat_map(|(_, blocks)| blocks.iter().copied())
                .collect();
            match o {
                3 => SegmentRoute::Uncovered,
                _ if blocks.is_empty() => SegmentRoute::Pruned,
                _ => SegmentRoute::Blocks(blocks.into_iter().collect()),
            }
        })
        .collect()
}

/// The statement failed with `RouteError::Isolation` naming slice `foreign`
/// of part 0, the breach counter moved by exactly one from `before`, every
/// slice's GET 1 was read once, and no bucket run of any slice was read.
fn assert_fails_closed_at(
    fx: &Fixture,
    run: &Run,
    ended: &[(ScriptedRead, End)],
    foreign: usize,
    before: u64,
) {
    let refs = &fx.parts.parts[0].key_index;
    match &run.result {
        Err(RouteError::Isolation { leaf_key, source }) => {
            assert_eq!(leaf_key, &refs[foreign].key);
            assert!(
                matches!(source, CatalogError::FieldMismatch { field, .. } if *field == "tenant_hash"),
                "{source:?}"
            );
        }
        other => panic!("expected an isolation error, got {other:?}"),
    }
    assert_eq!(fx.catalog.isolation_breaches(), before + 1);
    assert!(
        ended.iter().all(|(read, _)| read.prefix),
        "no bucket run is read: {ended:?}"
    );
    for s in 0..refs.len() {
        assert_eq!(
            ended.iter().filter(|(read, _)| read.slice == s).count(),
            1,
            "slice {s}'s GET 1: {ended:?}"
        );
    }
    assert_eq!(
        run.accounting
            .key_index()
            .snapshot()
            .s3_requests(AccountedOp::Get),
        run.reader.calls.load(Ordering::Relaxed)
    );
}

/// Slice 1's breach fails the statement although slice 0's run read would
/// have rejected the part first, as probing the slices one after another
/// does: ADR-0050 section 2 fails the query on any breach seen. Phase 3
/// never starts, so slice 0's runs are not read, and the counter moves by
/// one.
/// FLIP: a752bafe3 reads slice 0's runs, lets the failed one reject the
/// part, and the statement goes on.
#[tokio::test]
async fn a_breach_after_a_failing_run_still_fails_the_statement() {
    let mut spec = sliced_spec(2, 600);
    spec.leaf = LeafKind::SlicedForeign {
        slices: 2,
        foreign: 1,
    };
    let fx = fixture(&[spec], &[], KeyType::Str).await;
    let before = fx.catalog.isolation_breaches();
    let keys = key_set(KeyType::Str, &(0..600).collect());
    let budget = ThresholdBudget::new(u64::MAX);
    let (run, ended) = run_scripted(&fx, &keys, &budget, |read| match read {
        ScriptedRead {
            slice: 0,
            prefix: false,
            ..
        } => (0, End::Fail),
        _ => (0, End::Pass),
    })
    .await;
    assert_fails_closed_at(&fx, &run, &ended, 1, before);
}

/// A breach at the stopping slice, with every earlier slice planning,
/// fails the statement before any bucket run is read: slice 2's foreign
/// header gives `RouteError::Isolation` and the counter moves by one.
/// FLIP: a752bafe3 reads the runs of slices 0 and 1 in phase 3 before
/// returning the breach.
#[tokio::test]
async fn a_breach_at_the_stopping_slice_fails_before_any_run_is_read() {
    let mut spec = sliced_spec(4, 600);
    spec.leaf = LeafKind::SlicedForeign {
        slices: 4,
        foreign: 2,
    };
    let fx = fixture(&[spec], &[], KeyType::Str).await;
    let before = fx.catalog.isolation_breaches();
    let keys = key_set(KeyType::Str, &(0..600).collect());
    let budget = ThresholdBudget::new(u64::MAX);
    let (run, ended) = run_scripted(&fx, &keys, &budget, |_| (0, End::Pass)).await;
    assert!(
        ended.iter().all(|(_, end)| *end == End::Pass),
        "every read answers: {ended:?}"
    );
    assert_fails_closed_at(&fx, &run, &ended, 2, before);
}

/// Slice 0's refused decode reservation decides the part before slice 2's
/// bridged bytes exceed what slices 0 and 1 left of the allowance: the part
/// counts as a refused reservation, not as skipped by budget.
/// FLIP: bd2e2ceaa decides the allowance in phase 2, before slice 0's runs
/// are decoded, and counts the part as skipped by budget.
#[tokio::test]
async fn a_refused_decode_before_a_slice_over_the_allowance_refuses_the_part() {
    let AllowanceCase { fx, query, .. } = allowance_case().await;
    let keys = key_set(KeyType::Str, &query);
    // With every reservation granted, slice 2 exceeds the allowance.
    let granted = run(&fx, &keys, &ThresholdBudget::new(u64::MAX), UNBOUNDED).await;
    let (_, granted) = granted.result.as_ref().expect("route");
    assert_eq!(granted.skipped_by_budget, 1);

    let budget = ThresholdBudget::new(0);
    let refused = run(&fx, &keys, &budget, UNBOUNDED).await;
    let (routes, figures) = refused.result.as_ref().expect("route");
    assert_eq!(figures.reservations_refused, 1);
    assert_eq!(figures.skipped_by_budget, 0);
    assert_eq!(budget.refused.load(Ordering::Relaxed), 1);
    assert_eq!(routes, &vec![SegmentRoute::Uncovered]);
    assert_accounting(&refused, figures);
}

/// Slice 0's failed run reads decide the part before slice 1's GET 1 is
/// refused by the reader's fetch reservation: the part is rejected.
/// FLIP: bd2e2ceaa decides slice 1's refused GET 1 in phase 2 and counts the
/// part as a refused reservation.
#[tokio::test]
async fn failed_runs_before_a_refused_prefix_reject_the_part() {
    let fx = fixture(&[sliced_spec(2, 600)], &[], KeyType::Str).await;
    let keys = key_set(KeyType::Str, &(0..600).collect());
    let budget = ThresholdBudget::new(u64::MAX);
    let (run, ended) = run_scripted(&fx, &keys, &budget, |read| match read {
        ScriptedRead {
            slice: 0,
            prefix: false,
            ..
        } => (0, End::Fail),
        ScriptedRead {
            slice: 1,
            prefix: true,
            ..
        } => (0, End::Refuse),
        _ => (0, End::Pass),
    })
    .await;
    let (routes, figures) = run.result.as_ref().expect("route");
    assert_eq!(figures.leaves_rejected, 1);
    assert_eq!(figures.reservations_refused, 0);
    assert!(routes.iter().all(|r| *r == SegmentRoute::Uncovered));
    assert!(
        ended.contains(&(
            ScriptedRead {
                slice: 1,
                prefix: true,
                run: 0
            },
            End::Refuse
        )),
        "{ended:?}"
    );
}

/// A breach in the GET 1 of a slice after the stopping one fails the
/// statement: slice 1's failed GET 1 would reject the part, but slice 3's
/// foreign header gives `RouteError::Isolation`, the counter moves by one,
/// and no run of slice 0 is read.
/// FLIP: a752bafe3 counts slice 3's breach, rejects the part at slice 1,
/// and the statement goes on.
#[tokio::test]
async fn a_breach_after_the_stopping_slice_fails_the_statement() {
    let mut spec = sliced_spec(4, 600);
    spec.leaf = LeafKind::SlicedForeign {
        slices: 4,
        foreign: 3,
    };
    let fx = fixture(&[spec], &[], KeyType::Str).await;
    let before = fx.catalog.isolation_breaches();
    let keys = key_set(KeyType::Str, &(0..600).collect());
    let budget = ThresholdBudget::new(u64::MAX);
    let (run, ended) = run_scripted(&fx, &keys, &budget, |read| match read {
        ScriptedRead {
            slice: 1,
            prefix: true,
            ..
        } => (0, End::Fail),
        _ => (0, End::Pass),
    })
    .await;
    assert!(
        ended.contains(&(
            ScriptedRead {
                slice: 1,
                prefix: true,
                run: 0
            },
            End::Fail
        )),
        "slice 1's GET 1 fails: {ended:?}"
    );
    assert_fails_closed_at(&fx, &run, &ended, 3, before);
}

/// Run reads that complete in reverse issue order, across slices, are still
/// decoded in body order per slice: the routes are the exact blocks a whole
/// scan gives, and equal the serial oracle's.
/// FLIP: `buffer_unordered` at phase 3 hands a decoder another run's bytes,
/// which rejects the part.
#[tokio::test]
async fn runs_completing_in_reverse_order_answer_as_the_serial_probe() {
    let fx = fixture(&[sliced_spec(4, 600)], &[], KeyType::Str).await;
    let query: BTreeSet<u16> = (0..600).step_by(15).collect();
    let keys = key_set(KeyType::Str, &query);
    let budget = ThresholdBudget::new(u64::MAX);
    let (run, ended) = run_scripted(&fx, &keys, &budget, |read| match read.prefix {
        true => (0, End::Pass),
        false => (64u64.saturating_sub(read.run) as u32, End::Pass),
    })
    .await;
    let (routes, figures) = run.result.as_ref().expect("route");
    assert_eq!(figures.parts_covered, 1);
    assert_eq!(routes, &sliced_routes(&fx, &query));
    assert_eq!(routes, &serial_probe(&fx, &keys).await.routes);
    let runs: Vec<ScriptedRead> = ended
        .iter()
        .filter(|(read, _)| !read.prefix)
        .map(|(read, _)| *read)
        .collect();
    let slices: BTreeSet<usize> = runs.iter().map(|r| r.slice).collect();
    assert_eq!(slices.len(), 4, "every slice has runs: {runs:?}");
    assert!(
        runs.iter()
            .enumerate()
            .any(|(i, a)| runs[i + 1..].iter().any(|b| b.slice < a.slice)),
        "a later slice's run ends before an earlier slice's: {runs:?}"
    );
    assert_accounting(&run, figures);
}

/// When runs of two slices fail, the earlier slice decides although the
/// later slice's failure ends first: slice 0's first run, refused after 32
/// polls, beats slice 1's runs, which fail at once.
/// FLIP: `buffer_unordered` at phase 3 takes slice 1's failure first and
/// rejects the part.
#[tokio::test]
async fn the_earlier_slices_failing_run_decides_over_a_faster_later_one() {
    let fx = fixture(&[sliced_spec(2, 600)], &[], KeyType::Str).await;
    let by_slice = values_by_slice(&fx, 0..600);
    let query: BTreeSet<u16> = by_slice[0][..4]
        .iter()
        .chain(&by_slice[1][..4])
        .copied()
        .collect();
    let keys = key_set(KeyType::Str, &query);
    let budget = ThresholdBudget::new(u64::MAX);
    let (run, ended) = run_scripted(&fx, &keys, &budget, |read| match read {
        ScriptedRead {
            slice: 0,
            prefix: false,
            run: 0,
        } => (32, End::Refuse),
        ScriptedRead {
            slice: 1,
            prefix: false,
            ..
        } => (0, End::Fail),
        _ => (0, End::Pass),
    })
    .await;
    let (routes, figures) = run.result.as_ref().expect("route");
    assert_eq!(figures.reservations_refused, 1);
    assert_eq!(figures.leaves_rejected, 0);
    assert!(routes.iter().all(|r| *r == SegmentRoute::Uncovered));
    let position = |slice: usize, end: End| {
        ended
            .iter()
            .position(|(read, e)| read.slice == slice && !read.prefix && *e == end)
            .expect("run read ended")
    };
    assert!(
        position(1, End::Fail) < position(0, End::Refuse),
        "slice 1's failure ends first: {ended:?}"
    );
}

/// Phase 3's reads are bounded on their own: a part of one slice, whose
/// GET 1 is the only phase-1 read, holds exactly `LEAF_READ_CONCURRENCY`
/// run reads in flight.
/// FLIP: a serial phase 3 peaks at 1.
#[tokio::test]
async fn a_parts_runs_peak_at_the_bound() {
    let fx = fixture(&[sliced_spec(1, 600)], &[], KeyType::Str).await;
    let query: BTreeSet<u16> = (0..600).collect();
    let keys = key_set(KeyType::Str, &query);
    let budget = ThresholdBudget::new(u64::MAX);
    let (run, peak, ranges) = run_in_flight(&fx, &keys, &budget).await;
    let (routes, figures) = run.result.as_ref().expect("route");
    assert_eq!(figures.parts_covered, 1);
    assert_eq!(routes, &sliced_routes(&fx, &query));
    let leaf = &fx.parts.parts[0].key_index[0];
    let prefix = key_index_leaf_prefix_range(leaf).expect("prefix range");
    let runs = ranges.iter().filter(|(_, range)| *range != prefix).count();
    assert_eq!(ranges.len(), runs + 1, "one GET 1");
    assert!(runs >= 20, "{runs} runs");
    assert_eq!(peak, ravel_catalog::LEAF_READ_CONCURRENCY as u64);
}

/// One GET 1 buffer; dropping it is counted.
struct LiveBuffer {
    bytes: Bytes,
    live: Arc<AtomicU64>,
}

impl AsRef<[u8]> for LiveBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for LiveBuffer {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Hands out GET 1's bytes in buffers whose drop it counts, as a production
/// reader's fetch reservation is released with its bytes, and records how
/// many are alive as each run read is issued.
struct PrefixLifeReader<'r> {
    inner: &'r CountingReader,
    live: Arc<AtomicU64>,
    live_at_run: std::sync::Mutex<Vec<u64>>,
}

impl LeafRangeReader for PrefixLifeReader<'_> {
    fn read_range<'a>(
        &'a self,
        leaf: &'a SnapshotKeyIndexLeafRef,
        range: Range<u64>,
        accounting: &'a QueryAccounting,
    ) -> BoxFuture<'a, Result<Bytes, LeafReadError>> {
        let prefix = key_index_leaf_prefix_range(leaf).ok() == Some(range.clone());
        if !prefix {
            self.live_at_run
                .lock()
                .expect("live")
                .push(self.live.load(Ordering::SeqCst));
        }
        Box::pin(async move {
            let bytes = self.inner.read_range(leaf, range, accounting).await?;
            if !prefix {
                return Ok(bytes);
            }
            self.live.fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::from_owner(LiveBuffer {
                bytes,
                live: self.live.clone(),
            }))
        })
    }
}

/// Each planned slice keeps its GET 1 bytes, and so a production reader's
/// fetch reservation for them, until the slice finishes: all 24 are alive
/// when the first run read is issued, fewer when the last one is, and none
/// once the route returns.
/// FLIP: bd2e2ceaa drops every GET 1 buffer in phase 1, so none is alive at
/// the first run read.
#[tokio::test]
async fn a_slices_get_1_bytes_live_until_the_slice_finishes() {
    let fx = fixture(&[sliced_spec(24, 600)], &[], KeyType::Str).await;
    let query: BTreeSet<u16> = (0..600).step_by(2).chain(2000..2100).collect();
    let keys = key_set(KeyType::Str, &query);
    let budget = ThresholdBudget::new(u64::MAX);
    let accounting = PhaseAccounting::new();
    let counting = CountingReader::new(fx.store.clone());
    let reader = PrefixLifeReader {
        inner: &counting,
        live: Arc::new(AtomicU64::new(0)),
        live_at_run: std::sync::Mutex::new(Vec::new()),
    };
    let (routes, figures) = route_under(
        input(&fx, &keys, &accounting, &reader, &budget),
        UNBOUNDED,
        &RouteTotals::new(),
    )
    .await
    .expect("route");
    assert_eq!(figures.parts_covered, 1);
    assert_eq!(routes, sliced_routes(&fx, &query));
    let live_at_run = std::mem::take(&mut *reader.live_at_run.lock().expect("live"));
    assert!(live_at_run.len() > 24, "{} runs", live_at_run.len());
    assert_eq!(live_at_run.first(), Some(&24));
    assert!(live_at_run.last().is_some_and(|&last| last < 24));
    assert_eq!(reader.live.load(Ordering::SeqCst), 0);
}
