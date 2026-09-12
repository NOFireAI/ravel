//! Building and reading the dictionary-encoded `labels` column.
//!
//! The physical representation is `Dictionary(Int32, Map(Utf8, Utf8))`: the
//! dictionary values hold one `Map` entry per distinct series in the batch,
//! and the Int32 keys select a series per row. Rows in a scan batch are
//! grouped by series (the scan sorts by `(series_id, ts, ...)`), so the key
//! run for a series is contiguous.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, DictionaryArray, Int32Array, MapArray,
    MapBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::Int32Type;
use ravel_promql::LabelMatcher;
use ravel_types::{Label, LabelSet};

use crate::error::SqlError;

/// Build a `Dictionary(Int32, Map)` labels column from the distinct label
/// sets (in dictionary-key order) and a per-row key into them.
pub fn build_labels_dict(distinct: &[LabelSet], keys: &[i32]) -> Result<ArrayRef, SqlError> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for set in distinct {
        for label in set.iter() {
            builder.keys().append_value(&label.name);
            builder.values().append_value(&label.value);
        }
        builder
            .append(true)
            .map_err(|e| SqlError::Internal(format!("map builder append: {e}")))?;
    }
    let values: MapArray = builder.finish();
    let keys = Int32Array::from(keys.to_vec());
    let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values))
        .map_err(|e| SqlError::Internal(format!("labels dictionary build: {e}")))?;
    Ok(Arc::new(dict))
}

/// Rebuild a `Dictionary(Int32, Map)` labels column so its dictionary holds
/// only the distinct label sets its rows actually reference, re-keying each
/// row to the compacted entries. The output type and every row's decoded
/// label set are identical to the input; only redundant dictionary entries
/// are dropped.
///
/// Slicing a `DictionaryArray` (as the dedup operator does, one winner row at
/// a time) rewrites the key run but retains the whole source values buffer, so
/// concatenating many such slices appends every slice's entire dictionary. The
/// concatenated dictionary therefore grows with the row count rather than with
/// the distinct-series count. This collapses it back: entries are deduplicated
/// by content, so the result carries exactly one entry per distinct label set
/// referenced by a non-null row, bounded by the distinct series in the batch
/// regardless of how many rows reference them.
pub fn compact_labels(labels: &ArrayRef) -> Result<ArrayRef, SqlError> {
    let dict = labels
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| SqlError::Internal("labels column is not Dictionary(Int32, _)".into()))?;
    let maps = dict
        .values()
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| SqlError::Internal("labels dictionary values are not a Map".into()))?;
    let entry_keys = maps
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SqlError::Internal("map keys are not Utf8".into()))?;
    let entry_values = maps
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SqlError::Internal("map values are not Utf8".into()))?;
    let offsets = maps.value_offsets();

    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    // Canonical bytes of a distinct label set -> its new dictionary key.
    let mut seen: HashMap<Vec<u8>, i32> = HashMap::new();
    // Old dictionary-entry index -> new key, so a run of rows sharing one old
    // entry canonicalizes it once rather than per row.
    let mut old_to_new: HashMap<usize, i32> = HashMap::new();
    let mut new_keys: Vec<Option<i32>> = Vec::with_capacity(dict.len());

    for i in 0..dict.len() {
        if dict.is_null(i) {
            new_keys.push(None);
            continue;
        }
        let old = resolve_key(dict.keys().value(i), maps.len())?;
        let new_key = match old_to_new.get(&old) {
            Some(&k) => k,
            None => {
                let k = intern_entry(
                    &mut builder,
                    &mut seen,
                    maps,
                    entry_keys,
                    entry_values,
                    offsets,
                    old,
                )?;
                old_to_new.insert(old, k);
                k
            }
        };
        new_keys.push(Some(new_key));
    }

    let values: MapArray = builder.finish();
    let keys = Int32Array::from(new_keys);
    let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values))
        .map_err(|e| SqlError::Internal(format!("labels dictionary compact: {e}")))?;
    Ok(Arc::new(dict))
}

/// Intern the `old`-th map entry of `maps` into `builder`, returning its
/// compacted key. Entries with identical canonical content share one key, so
/// the same series appearing under many retained sub-dictionaries collapses to
/// a single entry.
#[allow(clippy::too_many_arguments)]
fn intern_entry(
    builder: &mut MapBuilder<StringBuilder, StringBuilder>,
    seen: &mut HashMap<Vec<u8>, i32>,
    maps: &MapArray,
    entry_keys: &StringArray,
    entry_values: &StringArray,
    offsets: &[i32],
    old: usize,
) -> Result<i32, SqlError> {
    // A null map entry (no label set) is distinct from any real one; the
    // scan never builds one, but canonicalize it explicitly so the fallback
    // stays correct rather than aliasing an empty label set.
    if maps.is_null(old) {
        let canon = vec![0u8];
        if let Some(&k) = seen.get(&canon) {
            return Ok(k);
        }
        builder
            .append(false)
            .map_err(|e| SqlError::Internal(format!("map builder append: {e}")))?;
        let k = i32::try_from(seen.len())
            .map_err(|_| SqlError::Internal("labels dictionary exceeds i32 keys".into()))?;
        seen.insert(canon, k);
        return Ok(k);
    }

    let start = offsets[old] as usize;
    let end = offsets[old + 1] as usize;
    // Length-prefixed so no key/value byte sequence can forge a boundary, and
    // a null value is a distinct marker from an empty string. Leading `1`
    // separates a present entry from the null-entry marker above.
    let mut canon: Vec<u8> = vec![1u8];
    for j in start..end {
        let name = entry_keys.value(j);
        canon.extend_from_slice(&(name.len() as u64).to_le_bytes());
        canon.extend_from_slice(name.as_bytes());
        if entry_values.is_null(j) {
            canon.extend_from_slice(&u64::MAX.to_le_bytes());
        } else {
            let value = entry_values.value(j);
            canon.extend_from_slice(&(value.len() as u64).to_le_bytes());
            canon.extend_from_slice(value.as_bytes());
        }
    }
    if let Some(&k) = seen.get(&canon) {
        return Ok(k);
    }
    for j in start..end {
        builder.keys().append_value(entry_keys.value(j));
        if entry_values.is_null(j) {
            builder.values().append_null();
        } else {
            builder.values().append_value(entry_values.value(j));
        }
    }
    builder
        .append(true)
        .map_err(|e| SqlError::Internal(format!("map builder append: {e}")))?;
    let k = i32::try_from(seen.len())
        .map_err(|_| SqlError::Internal("labels dictionary exceeds i32 keys".into()))?;
    seen.insert(canon, k);
    Ok(k)
}

/// Resolve `key` for every row of a dictionary-encoded labels column,
/// returning a `Utf8` array (null where the label is absent). Used by the
/// `label` UDF.
pub fn lookup_label(labels: &ArrayRef, key: &str) -> Result<StringArray, SqlError> {
    let dict = labels
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| SqlError::Internal("labels column is not Dictionary(Int32, _)".into()))?;
    let maps = dict
        .values()
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| SqlError::Internal("labels dictionary values are not a Map".into()))?;

    // Resolve the label once per distinct dictionary entry, then fan out to
    // rows via the keys (labels repeat across a series' samples).
    let entry_values: Vec<Option<String>> = (0..maps.len())
        .map(|i| map_entry_lookup(maps, i, key))
        .collect::<Result<_, _>>()?;

    let mut out = StringBuilder::new();
    for i in 0..dict.len() {
        if dict.is_null(i) {
            out.append_null();
            continue;
        }
        let k = resolve_key(dict.keys().value(i), entry_values.len())?;
        match entry_values[k].as_deref() {
            Some(v) => out.append_value(v),
            None => out.append_null(),
        }
    }
    Ok(out.finish())
}

/// Evaluate `matcher` against every row of a dictionary-encoded labels
/// column, returning a `BooleanArray`. The matcher is resolved once per
/// distinct dictionary entry (labels repeat across a series' samples) and
/// fanned out to rows via the keys. Absence of the matcher's label reads as
/// the empty string, exactly as `LabelMatcher::is_match` and Prometheus do, so
/// this is byte-for-byte the semantics the pushdown prune assumes. A null
/// dictionary row (no series) is `false`.
pub fn eval_matcher(labels: &ArrayRef, matcher: &LabelMatcher) -> Result<BooleanArray, SqlError> {
    let dict = labels
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| SqlError::Internal("labels column is not Dictionary(Int32, _)".into()))?;
    let maps = dict
        .values()
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| SqlError::Internal("labels dictionary values are not a Map".into()))?;

    let entry_match: Vec<bool> = (0..maps.len())
        .map(|i| {
            let value = map_entry_lookup(maps, i, &matcher.name)?;
            let set = match value {
                Some(v) => LabelSet::new(vec![Label {
                    name: matcher.name.clone(),
                    value: v,
                }])
                .unwrap_or_default(),
                None => LabelSet::default(),
            };
            Ok::<bool, SqlError>(matcher.is_match(&set))
        })
        .collect::<Result<_, _>>()?;

    let mut out = BooleanBuilder::new();
    for i in 0..dict.len() {
        if dict.is_null(i) {
            out.append_value(false);
            continue;
        }
        let k = resolve_key(dict.keys().value(i), entry_match.len())?;
        out.append_value(entry_match[k]);
    }
    Ok(out.finish())
}

/// Resolve a dictionary key into a valid index into the `len` distinct
/// entries. A negative key (via `usize::try_from`) or an index `>= len` is a
/// corrupt column, so it becomes a typed `SqlError::Internal` rather than a
/// silent "absent label" / "no match". This mirrors the sibling decoder in
/// `output.rs`, which rejects the identical condition on the same
/// `Dictionary(Int32, Map)` column, so both decoders fail loudly and in the
/// same way. Callers handle a null dictionary row before calling this, since a
/// null row is legitimately "no series", not corruption.
fn resolve_key(key: i32, len: usize) -> Result<usize, SqlError> {
    let k = usize::try_from(key)
        .map_err(|_| SqlError::Internal(format!("negative dictionary key {key}")))?;
    if k >= len {
        return Err(SqlError::Internal(format!(
            "dictionary key {k} out of range for {len} entries"
        )));
    }
    Ok(k)
}

/// Look up `key` in the `row`-th map entry of `maps`.
fn map_entry_lookup(maps: &MapArray, row: usize, key: &str) -> Result<Option<String>, SqlError> {
    if maps.is_null(row) {
        return Ok(None);
    }
    let offsets = maps.value_offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let keys = maps
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SqlError::Internal("map keys are not Utf8".into()))?;
    let values = maps
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SqlError::Internal("map values are not Utf8".into()))?;
    for i in start..end {
        if keys.value(i) == key {
            return Ok(Some(values.value(i).to_string()));
        }
    }
    Ok(None)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::scalar::ScalarValue;
    use ravel_types::Label;

    use super::*;
    use crate::udf::label_udf;

    fn set(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(k, v)| Label {
                    name: (*k).to_string(),
                    value: (*v).to_string(),
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn dict_build_and_lookup_roundtrip() {
        let distinct = vec![
            set(&[("__name__", "cpu"), ("host", "a")]),
            set(&[("__name__", "mem")]),
        ];
        // rows: series 0, 0, 1 -> keys 0,0,1
        let keys = [0i32, 0, 1];
        let labels = build_labels_dict(&distinct, &keys).unwrap();

        let names = lookup_label(&labels, "__name__").unwrap();
        assert_eq!(names.value(0), "cpu");
        assert_eq!(names.value(1), "cpu");
        assert_eq!(names.value(2), "mem");

        // Absent label on a row whose series lacks it -> null.
        let hosts = lookup_label(&labels, "host").unwrap();
        assert_eq!(hosts.value(0), "a");
        assert!(hosts.is_null(2), "mem series has no host label");
    }

    #[test]
    fn eval_matcher_anchors_and_treats_absent_as_empty() {
        use ravel_promql::LabelMatcher;

        // rows: series 0 (host=web1), 0, 1 (no host)
        let distinct = vec![
            set(&[("__name__", "cpu"), ("host", "web1")]),
            set(&[("__name__", "cpu")]),
        ];
        let keys = [0i32, 0, 1];
        let labels = build_labels_dict(&distinct, &keys).unwrap();

        // Anchored regex `web.*` matches "web1" but the anchoring means a bare
        // "eb" would not; absent host reads as "" and does not match.
        let m = LabelMatcher::regex("host", "web.*").unwrap();
        let got = eval_matcher(&labels, &m).unwrap();
        assert!(got.value(0) && got.value(1), "web1 rows match web.*");
        assert!(!got.value(2), "series without host does not match web.*");

        // Negation keeps the absent-host series (absent reads as "", `"" !~
        // web.*`), matching Prometheus semantics.
        let nm = LabelMatcher::not_regex("host", "web.*").unwrap();
        let neg = eval_matcher(&labels, &nm).unwrap();
        assert!(!neg.value(0));
        assert!(neg.value(2), "absent-host series matches the negation");
    }

    #[test]
    fn out_of_range_key_is_typed_error_not_absent() {
        // Arrow's safe DictionaryArray constructors (`try_new`,
        // `ArrayDataBuilder::build`) validate key bounds, so a corrupt key is
        // unconstructible through safe APIs and cannot be driven end to end
        // without `unsafe`, which is denied workspace-wide. resolve_key is the
        // guard both lookup_label and eval_matcher call on every non-null row;
        // test it directly. A negative key and an out-of-range positive key
        // must each be a typed Internal error, never silently coerced to
        // "absent"; a valid key resolves.
        assert!(matches!(resolve_key(-1, 4), Err(SqlError::Internal(_))));
        assert!(matches!(resolve_key(4, 4), Err(SqlError::Internal(_))));
        assert!(matches!(resolve_key(3, 4), Ok(3)));
    }

    /// One raw map entry: `None` is a null map entry; `Some(pairs)` is a
    /// present entry, where `(name, None)` is a present key with a null
    /// value.
    type RawMapEntry<'a> = Option<&'a [(&'a str, Option<&'a str>)]>;

    /// Build a raw `Dictionary(Int32, Map(Utf8, Utf8))` array straight from
    /// map entries, bypassing `build_labels_dict` so a null map entry or a
    /// null value inside a present entry can be constructed.
    fn raw_map_dict(entries: &[RawMapEntry<'_>], keys: &[i32]) -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for entry in entries {
            match entry {
                None => {
                    builder.append(false).unwrap();
                }
                Some(pairs) => {
                    for (name, value) in *pairs {
                        builder.keys().append_value(name);
                        match value {
                            Some(v) => builder.values().append_value(v),
                            None => builder.values().append_null(),
                        }
                    }
                    builder.append(true).unwrap();
                }
            }
        }
        let values: MapArray = builder.finish();
        let keys = Int32Array::from(keys.to_vec());
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        Arc::new(dict)
    }

    /// Decode compacted row `i`'s `(name, value)` pairs, `None` for a null
    /// value, in stored order.
    fn decode_compacted(compacted: &ArrayRef, i: usize) -> Vec<(String, Option<String>)> {
        let dict = compacted
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let maps = dict.values().as_any().downcast_ref::<MapArray>().unwrap();
        let keys = maps.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let values = maps
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let entry = dict.keys().value(i) as usize;
        let offsets = maps.value_offsets();
        let start = offsets[entry] as usize;
        let end = offsets[entry + 1] as usize;
        (start..end)
            .map(|j| {
                (
                    keys.value(j).to_string(),
                    if values.is_null(j) {
                        None
                    } else {
                        Some(values.value(j).to_string())
                    },
                )
            })
            .collect()
    }

    #[test]
    fn compact_labels_keeps_aliasing_free_distinct_entries() {
        // A canonicalization that concatenated name+value bytes without
        // length-prefixing would map ("ab","c") and ("a","bc") to the same
        // byte string "abc", aliasing two different label sets into one
        // dictionary entry and silently rewriting one row's labels to the
        // other's. `intern_entry`'s canonical form length-prefixes every
        // string, so they must stay distinct.
        let distinct = vec![set(&[("ab", "c")]), set(&[("a", "bc")])];
        let keys = [0i32, 1];
        let labels = build_labels_dict(&distinct, &keys).unwrap();

        let compacted = compact_labels(&labels).unwrap();
        let dict = compacted
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let maps = dict.values().as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(
            maps.len(),
            2,
            "different label sets must not collapse into one dictionary entry"
        );
        assert_eq!(
            decode_compacted(&compacted, 0),
            vec![("ab".to_string(), Some("c".to_string()))],
            "row 0 must resolve to its own original label set"
        );
        assert_eq!(
            decode_compacted(&compacted, 1),
            vec![("a".to_string(), Some("bc".to_string()))],
            "row 1 must resolve to its own original label set, not row 0's"
        );
    }

    #[test]
    fn compact_labels_dedupes_null_map_entries() {
        // Rows 0, 1, and 3 all reference the null (no label set) source
        // entry at old index 0; row 2 references the one real entry at old
        // index 1. `intern_entry`'s `maps.is_null(old)` branch must
        // canonicalize the null entry once and every referencing row must
        // resolve to that same compacted entry, kept null and distinct from
        // any real one.
        let raw = raw_map_dict(&[None, Some(&[("host", Some("a"))])], &[0, 0, 1, 0]);

        let compacted = compact_labels(&raw).unwrap();
        let dict = compacted
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let maps = dict.values().as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(
            maps.len(),
            2,
            "one compacted entry for the null source entry, one for the real one"
        );

        let k0 = dict.keys().value(0);
        let k1 = dict.keys().value(1);
        let k2 = dict.keys().value(2);
        let k3 = dict.keys().value(3);
        assert_eq!(
            k0, k1,
            "both null-entry rows resolve to the same compacted key"
        );
        assert_eq!(k0, k3, "the third null-entry row also resolves to it");
        assert_ne!(k0, k2, "the real entry must not alias the null entry");
        assert!(
            maps.is_null(k0 as usize),
            "the compacted entry for a null source entry must still be null"
        );
        assert!(!maps.is_null(k2 as usize));
    }

    #[test]
    fn compact_labels_dedupes_entries_with_null_values() {
        // A present map entry with a null value (distinct from an absent
        // key) exercises `entry_values.is_null(j)`. Two rows share the same
        // source entry so the null-value canonicalization only fires once,
        // and the null must survive compaction rather than becoming an empty
        // string.
        let raw = raw_map_dict(
            &[
                Some(&[("host", None), ("region", Some("us"))]),
                Some(&[("host", Some("a"))]),
            ],
            &[0, 0, 1],
        );

        let compacted = compact_labels(&raw).unwrap();
        let dict = compacted
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let maps = dict.values().as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(maps.len(), 2);

        assert_eq!(
            decode_compacted(&compacted, 0),
            vec![
                ("host".to_string(), None),
                ("region".to_string(), Some("us".to_string())),
            ]
        );
        assert_eq!(
            decode_compacted(&compacted, 1),
            decode_compacted(&compacted, 0),
            "the two rows referencing the same source entry share one compacted entry"
        );
        assert_eq!(
            decode_compacted(&compacted, 2),
            vec![("host".to_string(), Some("a".to_string()))]
        );
    }

    #[test]
    fn udf_name_and_invocation() {
        // The registered UDF is named `label` and returns Utf8.
        let udf = label_udf();
        assert_eq!(udf.name(), "label");

        // Exercise the UDF's implementation directly (the DataFusion invoke
        // plumbing is covered by DataFusion's own tests).
        let distinct = vec![set(&[("__name__", "cpu")])];
        let labels = build_labels_dict(&distinct, &[0i32]).unwrap();
        let out = crate::udf::label_impl(&[
            ColumnarValue::Array(labels),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("__name__".to_string()))),
        ])
        .expect("invoke");
        match out {
            ColumnarValue::Array(a) => {
                let s = a.as_any().downcast_ref::<StringArray>().expect("utf8 out");
                assert_eq!(s.value(0), "cpu");
            }
            ColumnarValue::Scalar(_) => panic!("expected array output"),
        }
    }
}
