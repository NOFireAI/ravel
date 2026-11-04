use std::sync::atomic::{AtomicU64, Ordering};

use proptest::prelude::*;
use ravel_catalog::{
    CatalogConfig, KeyIndexLeafEntry, KeyIndexLeafInput, KeyIndexLeafPrefix, SegmentLevel,
    SegmentPartOrigin, SegmentRef, encode_key_index_leaf_with_bucket_bits,
};
use ravel_codec::key_idx::{key8_id16, key8_str};
use ravel_object_store::PutOptions;
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
                signal: SIGNAL,
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
            min_hour: 0,
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
    fn new(store: Arc<MemoryStore>) -> Self {
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
    ) -> BoxFuture<'a, Result<Bytes, String>> {
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
}

async fn run(fx: &Fixture, keys: &KeySet, budget: &dyn LeafDecodeBudget, ceiling: Ceiling) -> Run {
    let accounting = PhaseAccounting::new();
    let reader = CountingReader::new(fx.store.clone());
    let result = route_under(
        RouteInput {
            catalog: &fx.catalog,
            snapshot: &fx.snapshot,
            parts: &fx.parts,
            signal: SIGNAL,
            tenant_hash: TENANT,
            key_set: keys,
            accounting: &accounting,
            reader: &reader,
            budget,
            limits: KeyIndexLeafLimits::default(),
        },
        ceiling,
    )
    .await;
    Run {
        result,
        accounting,
        reader,
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
