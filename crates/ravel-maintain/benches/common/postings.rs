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
//! `tick` runs after every counted insert and after each field's conversion
//! at the encoding boundary. The timing bench passes a no-op; the allocation
//! bench samples the allocator there.
//!
//! RLOG compaction in this crate (`src/rlog.rs`) encodes through the same
//! writer, via `RlogWriter::finish_compacted`.
//!
//! Two row-order families share each geometry's term sets (issue #3263):
//! `random` draws every indexed value per row; `sorted` assigns each row a
//! stream, draws `service.name` and `k8s.namespace.name` once per stream and
//! orders rows by stream, as the writer's `(stream_ref, ts_ns)` sort does
//! before it cuts blocks, so a block carries those two fields in runs.
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
    /// Streams in the `sorted` family, twice `service_terms`; each draws its
    /// two string terms once, so some terms go unused.
    pub streams: usize,
}

pub const GEOMETRIES: &[Geometry] = &[
    Geometry {
        name: "small",
        blocks: 2,
        service_terms: 200,
        namespace_terms: 40,
        streams: 400,
    },
    Geometry {
        name: "medium",
        blocks: 16,
        service_terms: 200,
        namespace_terms: 40,
        streams: 400,
    },
    Geometry {
        name: "large",
        blocks: 64,
        service_terms: 2_000,
        namespace_terms: 200,
        streams: 4_000,
    },
    Geometry {
        name: "highcard",
        blocks: 16,
        service_terms: 5_000,
        namespace_terms: 5_000,
        streams: 10_000,
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

/// Row order of a fixture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    /// Every indexed value drawn uniformly per row.
    Random,
    /// Rows grouped by stream, the two string fields fixed per stream.
    Sorted,
}

pub const FAMILIES: [Family; 2] = [Family::Random, Family::Sorted];

impl Family {
    pub fn name(self) -> &'static str {
        match self {
            Family::Random => "random",
            Family::Sorted => "sorted",
        }
    }

    pub fn parse(s: &str) -> Option<Family> {
        FAMILIES.into_iter().find(|f| f.name() == s)
    }
}

const STATUSES: [i64; 20] = [
    200, 201, 202, 204, 301, 302, 304, 400, 401, 403, 404, 405, 409, 422, 429, 500, 502, 503, 504,
    599,
];

/// The two string term sets, identical for both families of one geometry.
fn term_sets(g: &Geometry) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut rng = StdRng::seed_from_u64(SEED);
    let services = terms(
        &mut rng,
        g.service_terms,
        &[
            "payments-",
            "checkout-api-",
            "svc-",
            "frontend-web-",
            "inventory-worker-",
        ],
    );
    let namespaces = terms(
        &mut rng,
        g.namespace_terms,
        &["prod-", "prod-eu-west-1-", "staging-", "team-"],
    );
    (services, namespaces)
}

/// Rows in object order, each with its indexed `(column id, value)` pairs, as
/// `ResolvedRow::indexed_terms`.
pub fn fixture_for(family: Family, g: &Geometry) -> Vec<Vec<(u32, Value)>> {
    match family {
        Family::Random => fixture(g),
        Family::Sorted => sorted_fixture(g).rows,
    }
}

/// The `sorted` family with each row's stream index kept beside it.
pub struct SortedFixture {
    /// Stream index of each row, ascending.
    pub row_stream: Vec<u32>,
    pub rows: Vec<Vec<(u32, Value)>>,
}

/// `g.streams` streams, each with a `service.name` and a `k8s.namespace.name`
/// drawn once; every row draws its stream and its `http.status_code`
/// uniformly. Rows are then grouped by stream index, keeping draw order
/// within a stream (the writer's timestamp order), and cut into blocks by
/// `ROWS_PER_BLOCK` as before.
pub fn sorted_fixture(g: &Geometry) -> SortedFixture {
    let (services, namespaces) = term_sets(g);
    let mut s = StdRng::seed_from_u64(SEED ^ 2);
    let stream_terms: Vec<(usize, usize)> = (0..g.streams)
        .map(|_| {
            (
                s.random_range(0..services.len()),
                s.random_range(0..namespaces.len()),
            )
        })
        .collect();
    let mut drawn: Vec<(u32, i64)> = (0..g.blocks * ROWS_PER_BLOCK)
        .map(|_| {
            (
                s.random_range(0..g.streams) as u32,
                STATUSES[s.random_range(0..STATUSES.len())],
            )
        })
        .collect();
    drawn.sort_by_key(|(stream, _)| *stream);
    let row_stream = drawn.iter().map(|(stream, _)| *stream).collect();
    let rows = drawn
        .into_iter()
        .map(|(stream, status)| {
            let (svc, ns) = stream_terms[stream as usize];
            vec![
                (SERVICE, Value::Str(services[svc].clone())),
                (NAMESPACE, Value::Str(namespaces[ns].clone())),
                (STATUS, Value::I64(status)),
            ]
        })
        .collect();
    SortedFixture { row_stream, rows }
}

/// Distinct terms per indexed field, in `INDEXED` order.
pub fn distinct_terms(rows: &[Vec<(u32, Value)>]) -> [usize; 3] {
    let mut sets: [BTreeSet<Vec<u8>>; 3] = Default::default();
    for row in rows {
        for (cid, v) in row {
            if let Some(i) = INDEXED.iter().position(|c| c == cid) {
                sets[i].insert(term_key(v));
            }
        }
    }
    [sets[0].len(), sets[1].len(), sets[2].len()]
}

/// The `random` family: rows in object order, values drawn uniformly per row.
pub fn fixture(g: &Geometry) -> Vec<Vec<(u32, Value)>> {
    let (services, namespaces) = term_sets(g);
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
                (
                    STATUS,
                    Value::I64(STATUSES[s.random_range(0..STATUSES.len())]),
                ),
            ]
        })
        .collect()
}

/// One field's term map in the type the encoder takes (`FieldTerms::Terms`).
type FieldTermMap = BTreeMap<Vec<u8>, BTreeSet<u32>>;

fn checksum(fields: &[(u32, FieldTermMap)]) -> u64 {
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
    let mut postings_terms: BTreeMap<u32, FieldTermMap> = BTreeMap::new();
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
            tick();
        }
    }
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
            tick();
        }
    }
    checksum(&fields)
}

pub fn run_arm(arm: char, rows: &[Vec<(u32, Value)>], tick: &mut impl FnMut()) -> u64 {
    match arm {
        'A' => arm_std(rows, tick),
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
            "unknown arm {c:?}; use the letters A, C"
        );
    }
    arms
}

pub fn arm_label(arm: char) -> Option<&'static str> {
    match arm {
        'A' => Some("A_std_btreemap"),
        'C' => Some("C_hashmap_collect"),
        _ => None,
    }
}
