//! Shared fixture and arms for the RLOG postings accumulation benches (issue
//! #3254, candidate 2).
//!
//! Each arm reproduces the shape of the POSTINGS pass in `RlogWriter`'s
//! `build_object` and `build_object_columnar` (crates/ravel-logseg/src/writer.rs):
//! per block in ascending block order, per row, per indexed `(column id,
//! value)`, build the owned term key (`postings::term_key`: a clone of a string
//! value, the big-endian bytes of an integer), then
//! `entry(cid).or_default().entry(term).or_default().insert(block)`, with the
//! per-field distinct cap check after it. At the end every indexed field's
//! term map is removed and handed to the encoding boundary, which takes a
//! `BTreeMap<Vec<u8>, BTreeSet<u32>>` (`FieldTerms::Terms`); arms that use
//! another map convert to that type here and are charged for it. The checksum
//! walks every term and block id in the converted maps, in order.
//!
//! RLOG compaction in this crate (`src/rlog.rs`) encodes through the same
//! writer, via `RlogWriter::finish_compacted`.
// Each bench binary uses a different subset of these helpers.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// The RNG seed every postings geometry draws from.
pub const SEED: u64 = 0x3254_0B7E_E5EE_D002;

/// `RlogConfig::default().postings_max_distinct`.
pub const MAX_DISTINCT: usize = 10_000;

/// `RlogConfig::default().block_target_records`.
pub const ROWS_PER_BLOCK: usize = 8_192;

pub struct Geometry {
    pub name: &'static str,
    pub blocks: usize,
    /// Distinct values of the two string fields (`service.name`,
    /// `k8s.namespace.name` in the shipped default list); the third field is
    /// an integer `http.status_code` with 20 distinct values.
    pub service_terms: usize,
    pub namespace_terms: usize,
}

pub const GEOMETRIES: &[Geometry] = &[
    Geometry {
        name: "small",
        blocks: 2,
        service_terms: 200,
        namespace_terms: 40,
    },
    Geometry {
        name: "medium",
        blocks: 16,
        service_terms: 200,
        namespace_terms: 40,
    },
    Geometry {
        name: "large",
        blocks: 64,
        service_terms: 2_000,
        namespace_terms: 200,
    },
    Geometry {
        name: "highcard",
        blocks: 16,
        service_terms: 5_000,
        namespace_terms: 5_000,
    },
];

pub fn geometry(name: &str) -> &'static Geometry {
    GEOMETRIES
        .iter()
        .find(|g| g.name == name)
        .unwrap_or_else(|| panic!("no postings geometry named {name}"))
}

pub const SERVICE: u32 = 3;
pub const NAMESPACE: u32 = 7;
pub const STATUS: u32 = 11;
pub const INDEXED: [u32; 3] = [SERVICE, NAMESPACE, STATUS];

/// A row's indexed value, standing in for `ColumnValue`.
#[derive(Clone)]
pub enum Value {
    Str(Vec<u8>),
    I64(i64),
}

/// `postings::term_key` for the two value kinds the fixture carries.
fn term_key(v: &Value) -> Vec<u8> {
    match v {
        Value::Str(b) => b.clone(),
        Value::I64(i) => i.to_be_bytes().to_vec(),
    }
}

/// Terms of 8 to 40 bytes that share a handful of prefixes, as service and
/// namespace names do.
fn terms(rng: &mut StdRng, count: usize, prefixes: &[&str]) -> Vec<Vec<u8>> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-";
    let mut out: BTreeSet<Vec<u8>> = BTreeSet::new();
    while out.len() < count {
        let prefix = prefixes[rng.random_range(0..prefixes.len())].as_bytes();
        let len = rng.random_range(8..=40usize).max(prefix.len() + 1);
        let mut t = prefix.to_vec();
        while t.len() < len {
            t.push(ALPHABET[rng.random_range(0..ALPHABET.len())]);
        }
        out.insert(t);
    }
    let mut v: Vec<Vec<u8>> = out.into_iter().collect();
    // Shuffle so term choice is not correlated with term order.
    for i in (1..v.len()).rev() {
        v.swap(i, rng.random_range(0..=i));
    }
    v
}

/// Rows in object order, each with its indexed `(column id, value)` pairs, as
/// `ResolvedRow::indexed_terms`. Values are drawn uniformly per row.
pub fn fixture(g: &Geometry) -> Vec<Vec<(u32, Value)>> {
    let mut rng = StdRng::seed_from_u64(SEED);
    let services = terms(
        &mut rng,
        g.service_terms,
        &["payments-", "checkout-api-", "svc-", "frontend-web-", "inventory-worker-"],
    );
    let namespaces = terms(
        &mut rng,
        g.namespace_terms,
        &["prod-", "prod-eu-west-1-", "staging-", "team-"],
    );
    const STATUSES: [i64; 20] = [
        200, 201, 202, 204, 301, 302, 304, 400, 401, 403, 404, 405, 409, 422, 429, 500, 502,
        503, 504, 599,
    ];
    let mut s = StdRng::seed_from_u64(SEED ^ 1);
    (0..g.blocks * ROWS_PER_BLOCK)
        .map(|_| {
            vec![
                (
                    SERVICE,
                    Value::Str(services[s.random_range(0..services.len())].clone()),
                ),
                (
                    NAMESPACE,
                    Value::Str(namespaces[s.random_range(0..namespaces.len())].clone()),
                ),
                (STATUS, Value::I64(STATUSES[s.random_range(0..STATUSES.len())])),
            ]
        })
        .collect()
}

fn checksum(fields: &[(u32, BTreeMap<Vec<u8>, BTreeSet<u32>>)]) -> u64 {
    let mut acc = 0u64;
    for (cid, map) in fields {
        acc = acc.rotate_left(9) ^ u64::from(*cid);
        for (term, blocks) in map {
            for b in term {
                acc = acc.rotate_left(3) ^ u64::from(*b);
            }
            for blk in blocks {
                acc = acc.wrapping_mul(0x0000_0100_0000_01b3) ^ u64::from(*blk);
            }
        }
    }
    acc
}

/// Arm A: std maps throughout, the writer as it is.
pub fn arm_std(rows: &[Vec<(u32, Value)>], tick: &mut impl FnMut()) -> u64 {
    let mut postings_terms: BTreeMap<u32, BTreeMap<Vec<u8>, BTreeSet<u32>>> = BTreeMap::new();
    let mut postings_capped: BTreeSet<u32> = BTreeSet::new();
    for (blk, block_rows) in rows.chunks(ROWS_PER_BLOCK).enumerate() {
        let blk = blk as u32;
        for row in block_rows {
            for (cid, v) in row {
                if postings_capped.contains(cid) {
                    continue;
                }
                let field_map = postings_terms.entry(*cid).or_default();
                field_map.entry(term_key(v)).or_default().insert(blk);
                if field_map.len() > MAX_DISTINCT {
                    postings_terms.remove(cid);
                    postings_capped.insert(*cid);
                }
                tick();
            }
        }
    }
    let mut fields = Vec::new();
    for cid in INDEXED {
        if !postings_capped.contains(&cid) {
            fields.push((cid, postings_terms.remove(&cid).unwrap_or_default()));
        }
    }
    tick();
    checksum(&fields)
}

/// Arm B: `bbtree::unsync` maps and sets throughout, converted to the std
/// types at the encoding boundary.
pub fn arm_bbtree(rows: &[Vec<(u32, Value)>], tick: &mut impl FnMut()) -> u64 {
    use bbtree::unsync::{BBTreeMap, BBTreeSet};
    let mut postings_terms: BBTreeMap<u32, BBTreeMap<Vec<u8>, BBTreeSet<u32>>> =
        BBTreeMap::new();
    let mut postings_capped: BBTreeSet<u32> = BBTreeSet::new();
    for (blk, block_rows) in rows.chunks(ROWS_PER_BLOCK).enumerate() {
        let blk = blk as u32;
        for row in block_rows {
            for (cid, v) in row {
                if postings_capped.contains(cid) {
                    continue;
                }
                let field_map = postings_terms.entry(*cid).or_default();
                field_map.entry(term_key(v)).or_default().insert(blk);
                if field_map.len() > MAX_DISTINCT {
                    postings_terms.remove(cid);
                    postings_capped.insert(*cid);
                }
                tick();
            }
        }
    }
    let mut fields = Vec::new();
    for cid in INDEXED {
        if !postings_capped.contains(&cid) {
            let map = postings_terms.remove(&cid).unwrap_or_default();
            let map: BTreeMap<Vec<u8>, BTreeSet<u32>> = map
                .into_iter()
                .map(|(term, blocks)| (term, blocks.into_iter().collect()))
                .collect();
            fields.push((cid, map));
        }
    }
    tick();
    checksum(&fields)
}

/// Arm C: a `HashMap` term map per field (block sets stay `BTreeSet`),
/// collected into a `BTreeMap` at the encoding boundary.
pub fn arm_hash(rows: &[Vec<(u32, Value)>], tick: &mut impl FnMut()) -> u64 {
    let mut postings_terms: HashMap<u32, HashMap<Vec<u8>, BTreeSet<u32>>> = HashMap::new();
    let mut postings_capped: BTreeSet<u32> = BTreeSet::new();
    for (blk, block_rows) in rows.chunks(ROWS_PER_BLOCK).enumerate() {
        let blk = blk as u32;
        for row in block_rows {
            for (cid, v) in row {
                if postings_capped.contains(cid) {
                    continue;
                }
                let field_map = postings_terms.entry(*cid).or_default();
                field_map.entry(term_key(v)).or_default().insert(blk);
                if field_map.len() > MAX_DISTINCT {
                    postings_terms.remove(cid);
                    postings_capped.insert(*cid);
                }
                tick();
            }
        }
    }
    let mut fields = Vec::new();
    for cid in INDEXED {
        if !postings_capped.contains(&cid) {
            let map = postings_terms.remove(&cid).unwrap_or_default();
            fields.push((cid, map.into_iter().collect()));
        }
    }
    tick();
    checksum(&fields)
}

pub fn run_arm(arm: char, rows: &[Vec<(u32, Value)>], tick: &mut impl FnMut()) -> u64 {
    match arm {
        'A' => arm_std(rows, tick),
        'B' => arm_bbtree(rows, tick),
        'C' => arm_hash(rows, tick),
        other => panic!("unknown arm {other:?}"),
    }
}

/// The arm letters in the order `order` names them (`GROUPING_ARM_ORDER`).
pub fn arm_order(order: &str) -> Vec<char> {
    let arms: Vec<char> = order.chars().collect();
    for c in &arms {
        assert!(
            arm_label(*c).is_some(),
            "unknown arm {c:?}; use the letters A, B, C"
        );
    }
    arms
}

pub fn arm_label(arm: char) -> Option<&'static str> {
    match arm {
        'A' => Some("A_std_btreemap"),
        'B' => Some("B_bbtree_unsync"),
        'C' => Some("C_hashmap_collect"),
        _ => None,
    }
}
