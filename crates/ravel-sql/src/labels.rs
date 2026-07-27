//! Building and reading the dictionary-encoded `labels` column.
//!
//! The physical representation is `Dictionary(Int32, Map(Utf8, Utf8))`: the
//! dictionary values hold one `Map` entry per distinct series in the batch,
//! and the Int32 keys select a series per row. Rows in a scan batch are
//! grouped by series (the scan sorts by `(series_id, ts, ...)`), so the key
//! run for a series is contiguous.

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
        let k = dict.keys().value(i) as usize;
        match entry_values.get(k).and_then(|v| v.as_deref()) {
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
        let k = dict.keys().value(i) as usize;
        out.append_value(entry_match.get(k).copied().unwrap_or(false));
    }
    Ok(out.finish())
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
