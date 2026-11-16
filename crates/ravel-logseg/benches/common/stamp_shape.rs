//! The 104-declared-column record shape the declared-column stat stamp is
//! measured on (issue #1135), plus a reference copy of the pre-#1135 stamp so
//! a caller can compare both output and cost against it.
//!
//! Shared by `benches/stamp_declared.rs` and `tests/stamp_alloc_pin.rs` through
//! `#[path]`, so the benchmark and the allocation pin measure the same records.
//! The shape follows the ClickBench `hits` load the issue profiled: about a
//! hundred declared columns per record, almost all numeric, a quarter of them
//! also indexed, a few names duplicated on the record so the winner rule has
//! both tiers to order, and a stream layer that seeds names the records do not
//! all carry.
//!
//! Values are numeric throughout. A string value copies its bytes on every
//! projection, on both the old path and the new one, which adds equal cost to
//! both sides and hides the difference the benchmark exists to show; string
//! and bytes winners are covered by the differential property test in
//! `writer.rs` instead.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use ravel_logseg::record::{ColumnValue, FieldType, canonical_value_bytes, resolve_value};
use ravel_logseg::writer::stamp_probe::ProbeRecord;
use ravel_types::logstream::AttrValue;

/// Declared columns per record.
pub const DECLARED_COLUMNS: usize = 104;

/// One object's stamp inputs: the dynamic-column assignment, the indexed-field
/// list, one stream's resource and scope pairs, and the records.
pub struct Shape {
    pub columns: Vec<(String, FieldType, u32)>,
    pub indexed_fields: Vec<String>,
    pub stream_attrs: Vec<(String, AttrValue)>,
    pub records: Vec<Vec<(String, AttrValue)>>,
}

/// Builds `n_records` records over [`DECLARED_COLUMNS`] declared columns plus
/// two names that only the stream layer carries.
pub fn shape(n_records: usize) -> Shape {
    let mut columns: Vec<(String, FieldType, u32)> = (0..DECLARED_COLUMNS)
        .map(|i| (column_name(i), column_type(i), i as u32))
        .collect();
    columns.push((
        "stream_only_0".to_string(),
        FieldType::I64,
        DECLARED_COLUMNS as u32,
    ));
    columns.push((
        "stream_only_1".to_string(),
        FieldType::I64,
        DECLARED_COLUMNS as u32 + 1,
    ));

    // Every fourth column is also indexed, so both projections of the merged
    // view (POSTINGS terms and NumStat winners) run on most records.
    let indexed_fields: Vec<String> = (0..DECLARED_COLUMNS).step_by(4).map(column_name).collect();

    let stream_attrs = vec![
        ("service.name".to_string(), AttrValue::Str("bench".into())),
        ("stream_only_0".to_string(), AttrValue::I64(-1)),
        ("stream_only_1".to_string(), AttrValue::I64(-2)),
        // Carried by every record too, so the record winner overrides it.
        (column_name(0), AttrValue::I64(-3)),
        // The same key twice, as a resource and a scope attribute can repeat
        // it. Neither `stream_only_*` name is a tracked column, so both
        // entries are filtered out of the seed below and never reach a stamp;
        // the duplicate only exercises the untracked path of the merge.
        ("stream_only_0".to_string(), AttrValue::I64(-4)),
    ];

    let records = (0..n_records).map(record_attrs).collect();

    Shape {
        columns,
        indexed_fields,
        stream_attrs,
        records,
    }
}

fn column_name(i: usize) -> String {
    format!("col_{i:03}")
}

fn column_type(i: usize) -> FieldType {
    match i % 8 {
        5 => FieldType::F64,
        6 => FieldType::Bool,
        _ => FieldType::I64,
    }
}

/// One record: every declared column once, then four occurrences that land in
/// `attrs_raw`. Three repeat a name the record already carries columnar (a
/// duplicate `(name, type)` has no second column to take), and one carries a
/// type the name has no column for. Each duplicated name gets exactly one
/// overflow occurrence, which is what the merged-view rule needs to order the
/// two tiers without encoding any value.
fn record_attrs(r: usize) -> Vec<(String, AttrValue)> {
    let mut attrs = Vec::with_capacity(DECLARED_COLUMNS + 4);
    for i in 0..DECLARED_COLUMNS {
        attrs.push((column_name(i), declared_value(r, i)));
    }
    for i in [7usize, 23, 61] {
        attrs.push((column_name(i), declared_value(r + 1, i)));
    }
    attrs.push((column_name(42), AttrValue::F64(r as f64 + 0.5)));
    attrs
}

fn declared_value(r: usize, i: usize) -> AttrValue {
    match column_type(i) {
        FieldType::F64 => AttrValue::F64(r as f64 + i as f64 / 128.0),
        FieldType::Bool => AttrValue::Bool((r + i).is_multiple_of(2)),
        _ => AttrValue::I64((r * DECLARED_COLUMNS + i) as i64),
    }
}

type ColumnIndex = HashMap<u8, HashMap<String, u32>>;
type TrackedValues = Vec<(String, (u8, ColumnValue))>;
/// A stamp's two projections: POSTINGS terms and NumStat winners.
pub type StampOutputs = (Vec<(u32, ColumnValue)>, Vec<(u32, ColumnValue)>);

/// The pre-#1135 stamp: name-keyed maps and a per-slot rescan of the record's
/// occurrences, rebuilt here from `writer.rs` at 7c0c337 so the benchmark and
/// the allocation pin can compare against what the change replaced. It is a
/// reference copy on purpose and must not be tidied: any edit to it stops the
/// comparison from meaning anything.
pub struct Reference<'a> {
    names: Vec<&'a str>,
    seed: TrackedValues,
    indexed_names: HashSet<&'a str>,
    numstat_names: HashSet<&'a str>,
    column_of: ColumnIndex,
}

/// Builds the per-object state the old path built once per object.
pub fn reference_setup(shape: &Shape) -> Reference<'_> {
    let mut column_of: ColumnIndex = HashMap::new();
    for (name, ty, cid) in &shape.columns {
        column_of
            .entry(ty.to_u8())
            .or_default()
            .insert(name.clone(), *cid);
    }
    let indexed_names: HashSet<&str> = shape.indexed_fields.iter().map(String::as_str).collect();
    let numstat_names: HashSet<&str> = shape
        .columns
        .iter()
        .filter(|(_, ty, _)| matches!(ty, FieldType::I64 | FieldType::F64 | FieldType::Bool))
        .map(|(name, _, _)| name.as_str())
        .collect();

    let mut names: Vec<&str> = indexed_names
        .iter()
        .copied()
        .chain(numstat_names.iter().copied())
        .collect();
    names.sort_unstable();
    names.dedup();

    let mut seed: TrackedValues = Vec::new();
    for (k, v) in &shape.stream_attrs {
        if indexed_names.contains(k.as_str()) || numstat_names.contains(k.as_str()) {
            let (ty, cv) = resolve_value(v);
            seed.push((k.clone(), (ty.to_u8(), cv)));
        }
    }

    Reference {
        names,
        seed,
        indexed_names,
        numstat_names,
        column_of,
    }
}

impl Reference<'_> {
    /// One record's `(POSTINGS terms, NumStat winners)` by the old path.
    pub fn stamp(&self, rec: &ProbeRecord) -> StampOutputs {
        let winners = record_level_winners_slot(rec.columnar(), rec.overflow(), &self.names);
        let resolved = resolved_tracked_values(&self.seed, &winners);
        let indexed = indexed_term_columns(&resolved, &self.indexed_names, &self.column_of);
        let stat = stat_winner_columns(&resolved, &self.numstat_names, &self.column_of);
        (indexed, stat)
    }
}

fn column_lookup(column_of: &ColumnIndex, name: &str, ty_byte: u8) -> Option<u32> {
    column_of.get(&ty_byte)?.get(name).copied()
}

fn record_level_winners_slot(
    cols: &[(u32, u8, ColumnValue)],
    overflow: &[(u32, AttrValue)],
    names: &[&str],
) -> BTreeMap<String, (u8, ColumnValue)> {
    let mut slots: BTreeSet<u32> = BTreeSet::new();
    for (slot, _, _) in cols {
        slots.insert(*slot);
    }
    for (slot, _) in overflow {
        slots.insert(*slot);
    }

    let mut out: BTreeMap<String, (u8, ColumnValue)> = BTreeMap::new();
    for slot in slots {
        let mut combined: Vec<(u8, ColumnValue)> = Vec::new();
        let mut cs: Vec<(u8, ColumnValue)> = cols
            .iter()
            .filter(|(s, _, _)| *s == slot)
            .map(|(_, ty, cv)| (*ty, cv.clone()))
            .collect();
        cs.sort_by_key(|(ty, _)| *ty);
        combined.extend(cs);
        let mut keyed: Vec<(Vec<u8>, (u8, ColumnValue))> = overflow
            .iter()
            .filter(|(s, _)| *s == slot)
            .map(|(_, v)| {
                let (ty, cv) = resolve_value(v);
                (canonical_value_bytes(v), (ty.to_u8(), cv))
            })
            .collect();
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        combined.extend(keyed.into_iter().map(|(_, entry)| entry));
        if let Some(winner) = combined.pop() {
            out.insert(names[slot as usize].to_string(), winner);
        }
    }
    out
}

fn resolved_tracked_values(
    stream_seed: &[(String, (u8, ColumnValue))],
    winners: &BTreeMap<String, (u8, ColumnValue)>,
) -> TrackedValues {
    let mut merged: TrackedValues = stream_seed.to_vec();
    for (name, winner) in winners {
        if let Some(slot) = merged.iter_mut().find(|(mk, _)| mk == name) {
            slot.1 = winner.clone();
        } else {
            merged.push((name.clone(), winner.clone()));
        }
    }
    merged
}

fn stat_winner_columns(
    resolved: &[(String, (u8, ColumnValue))],
    numstat_names: &HashSet<&str>,
    column_of: &ColumnIndex,
) -> Vec<(u32, ColumnValue)> {
    let mut out: Vec<(u32, ColumnValue)> = Vec::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for (name, (ty_byte, cv)) in resolved {
        if !numstat_names.contains(name.as_str()) || !seen.insert(name.as_str()) {
            continue;
        }
        if !matches!(
            FieldType::from_u8(*ty_byte),
            Some(FieldType::I64 | FieldType::F64 | FieldType::Bool)
        ) {
            continue;
        }
        if let Some(cid) = column_lookup(column_of, name, *ty_byte) {
            out.push((cid, cv.clone()));
        }
    }
    out
}

fn indexed_term_columns(
    resolved: &[(String, (u8, ColumnValue))],
    indexed_names: &HashSet<&str>,
    column_of: &ColumnIndex,
) -> Vec<(u32, ColumnValue)> {
    let mut out = Vec::with_capacity(resolved.len());
    for (name, (ty_byte, cv)) in resolved {
        if !indexed_names.contains(name.as_str()) {
            continue;
        }
        if let Some(cid) = column_lookup(column_of, name, *ty_byte) {
            out.push((cid, cv.clone()));
        }
    }
    out
}
