//! Shared fixture and arms for the compaction grouping benches (issue #3254).
//!
//! Each arm reproduces the shape of `build_parts`'s `by_series` stage in
//! `src/build.rs`: insert every `(series_id, (input_idx, plan))` in canonical
//! input order, then drain the groups in ascending series-id order and consume
//! each one (the mixed-kind check and a read of every contribution). Every arm
//! returns an order-sensitive checksum, so two arms agree only when they drain
//! the same groups in the same order with the same contribution order.
//!
//! `tick` runs after every insert, after the key sort in arm C, and after
//! every consumed group. The timing bench passes a no-op; the allocation bench
//! samples the allocator there.
// Each bench binary uses a different subset of these helpers.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use ravel_maintain::read::SeriesPlan;
use ravel_segment::ValueKind;
use ravel_types::{LabelSet, SeriesId};

/// The RNG seed every geometry's series ids are drawn from.
pub const SEED: u64 = 0x3254_0B7E_E5EE_D001;

pub struct Geometry {
    pub name: &'static str,
    pub inputs: usize,
    pub series: usize,
    /// Share of each input's series ids that every input carries; the rest
    /// are unique to that input.
    pub repeated_pct: usize,
}

pub const GEOMETRIES: &[Geometry] = &[
    Geometry {
        name: "small",
        inputs: 4,
        series: 1_000,
        repeated_pct: 100,
    },
    Geometry {
        name: "medium",
        inputs: 8,
        series: 50_000,
        repeated_pct: 100,
    },
    Geometry {
        name: "large",
        inputs: 16,
        series: 200_000,
        repeated_pct: 100,
    },
    Geometry {
        name: "distinct",
        inputs: 8,
        series: 50_000,
        repeated_pct: 0,
    },
    Geometry {
        name: "mixed",
        inputs: 8,
        series: 50_000,
        repeated_pct: 50,
    },
];

pub fn geometry(name: &str) -> &'static Geometry {
    GEOMETRIES
        .iter()
        .find(|g| g.name == name)
        .unwrap_or_else(|| panic!("no geometry named {name}"))
}

/// One `Vec<SeriesPlan>` per input, each in ascending series-id order, as
/// `InputCatalog::series` is. Labels and runs are empty: the arms only read
/// the plan through a reference, so its heap payload does not enter the
/// measured stage.
pub fn fixture(g: &Geometry) -> Vec<Vec<SeriesPlan>> {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut random_id = || {
        let mut id = [0u8; 16];
        rng.fill_bytes(&mut id);
        id
    };
    let shared_count = g.series * g.repeated_pct / 100;
    let shared: Vec<[u8; 16]> = (0..shared_count).map(|_| random_id()).collect();
    (0..g.inputs)
        .map(|_| {
            let mut ids = shared.clone();
            ids.extend((shared_count..g.series).map(|_| random_id()));
            ids.sort_unstable();
            ids.into_iter()
                .map(|id| SeriesPlan {
                    series_id: SeriesId(id),
                    labels: LabelSet::default(),
                    kind: ValueKind::Scalar,
                    runs: Vec::new(),
                })
                .collect()
        })
        .collect()
}

/// The number of distinct series ids in a geometry's fixture.
pub fn distinct_series(g: &Geometry) -> usize {
    let shared = g.series * g.repeated_pct / 100;
    shared + g.inputs * (g.series - shared)
}

type Contributions<'a> = Vec<(usize, &'a SeriesPlan)>;

fn consume(acc: &mut u64, id: &[u8; 16], contributions: Contributions<'_>) {
    let mut kind: Option<ValueKind> = None;
    for (input_idx, plan) in &contributions {
        match kind {
            None => kind = Some(plan.kind),
            Some(k) => assert!(k == plan.kind, "series has mixed value kinds"),
        }
        *acc = acc.rotate_left(7)
            ^ (*input_idx as u64)
            ^ (plan.runs.len() as u64)
            ^ u64::from(plan.series_id.0[15]);
    }
    let mut head = [0u8; 8];
    head.copy_from_slice(&id[..8]);
    *acc = acc.wrapping_mul(0x0000_0100_0000_01b3) ^ u64::from_le_bytes(head);
}

/// Arm A: `std::collections::BTreeMap`, the code as it is in `build.rs`.
pub fn arm_std_btree(inputs: &[Vec<SeriesPlan>], tick: &mut impl FnMut()) -> u64 {
    let mut by_series: BTreeMap<[u8; 16], Contributions<'_>> = BTreeMap::new();
    for (idx, input) in inputs.iter().enumerate() {
        for series in input {
            by_series
                .entry(series.series_id.0)
                .or_default()
                .push((idx, series));
            tick();
        }
    }
    let mut acc = 0u64;
    for (id, contributions) in by_series {
        consume(&mut acc, &id, contributions);
        tick();
    }
    acc
}

/// Arm C: `HashMap`, then the keys collected into a `Vec`, sorted, and the
/// groups removed in that order. The key `Vec` and the sort are charged here.
pub fn arm_hash_sort(inputs: &[Vec<SeriesPlan>], tick: &mut impl FnMut()) -> u64 {
    let mut by_series: HashMap<[u8; 16], Contributions<'_>> = HashMap::new();
    for (idx, input) in inputs.iter().enumerate() {
        for series in input {
            by_series
                .entry(series.series_id.0)
                .or_default()
                .push((idx, series));
            tick();
        }
    }
    let mut keys: Vec<[u8; 16]> = by_series.keys().copied().collect();
    keys.sort_unstable();
    tick();
    let mut acc = 0u64;
    for id in keys {
        let contributions = by_series
            .remove(&id)
            .unwrap_or_else(|| panic!("a collected key is in the map"));
        consume(&mut acc, &id, contributions);
        tick();
    }
    acc
}

/// The arm labels in the order `order` names them (letters A and C in any
/// order; the timing bench reads it from `GROUPING_ARM_ORDER`).
pub fn arm_order(order: &str) -> Vec<char> {
    let arms: Vec<char> = order.chars().collect();
    for c in &arms {
        assert!(
            arm_label(*c).is_some(),
            "unknown arm {c:?}; use the letters A, C"
        );
    }
    arms
}

pub fn arm_label(arm: char) -> Option<&'static str> {
    match arm {
        'A' => Some("A_std_btreemap"),
        'C' => Some("C_hashmap_sort"),
        _ => None,
    }
}

/// Runs one arm. Generic over `tick` so a no-op tick inlines away.
pub fn run_arm(arm: char, inputs: &[Vec<SeriesPlan>], tick: &mut impl FnMut()) -> u64 {
    match arm {
        'A' => arm_std_btree(inputs, tick),
        'C' => arm_hash_sort(inputs, tick),
        other => panic!("unknown arm {other:?}"),
    }
}
