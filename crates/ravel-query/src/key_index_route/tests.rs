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
    }
    assert_eq!(run.totals.leaf_gets(SIGNAL), figures.leaf_get_requests);
    assert!(figures.leaf_get_requests > 0);
    assert_eq!(run.totals.leaf_gets(Signal::Logs), 0);
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
