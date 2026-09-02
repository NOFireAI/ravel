//! The merged-`attrs` decode shared by every RLOG-backed table
//! (`logs` [`crate::logs_scan`], `alerts` [`crate::alerts_scan`], and `audit`
//! [`crate::audit_scan`]).
//!
//! All three signals ride RLOG's format verbatim (ADR-0040), so all three
//! expose their records' attributes through one `Map(Utf8, Utf8)` column built
//! the same way: each record's stream-identity (resource + scope) attributes
//! merged with its dynamic per-record attributes, the record winning on a key
//! collision, values rendered to text.
//!
//! This is the sole owner of that decode. Every table that materializes an
//! `attrs` column funnels through [`merged_attrs`] so one record produces the
//! same map regardless of which table read it, and a corrupt `stream_attrs`
//! blob surfaces as one client-visible class ([`SqlError::CorruptStreamAttrs`],
//! `MSG_CORRUPT`) rather than a different one per table.

use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use ravel_logseg::LogRecord;
use ravel_query::erasure::ErasurePredicate;
use ravel_types::logstream::AttrValue;

use crate::error::SqlError;

/// The `attrs` column contents for one record: its decoded stream-identity
/// (resource + scope) attributes overlaid with its dynamic per-record
/// attributes, the record's value winning on a key collision. Callers that
/// promote well-known keys into typed columns (alerts' `alert_id`/`rule_id`/
/// `state`/`generation`) read them out of this same merged view with
/// [`find_attr`], so a promoted column and the `attrs` map never disagree.
pub(crate) fn merged_attrs(r: &LogRecord) -> DFResult<Vec<(String, AttrValue)>> {
    let mut merged = decode_stream_attrs(&r.stream_attrs)?;
    for (k, v) in &r.attrs {
        if let Some(slot) = merged.iter_mut().find(|(mk, _)| mk == k) {
            slot.1 = v.clone();
        } else {
            merged.push((k.clone(), v.clone()));
        }
    }
    Ok(merged)
}

/// Scan-layer selective-erasure exclusion for the RLOG-backed tables
/// (`logs`/`alerts`/`audit`), ADR-0064 decision 2. Drops every record erased by
/// any predicate in `erasure`, matching against the **same** merged
/// resource + scope + record attribute view ([`merged_attrs`]) that the `attrs`
/// column exposes to the query surface. That is the authoritative exclusion: a
/// subject named only in a resource or scope attribute (`user_id`, `host.name`,
/// `service.instance.id`) is queryable through `attrs` yet invisible to a
/// record-attribute-only filter, so it must be matched here. This
/// mirrors [`ravel_query::erasure::is_erased_span`] but over the
/// [`AttrValue`]-typed merged map.
///
/// A no-op when `erasure` is empty. Fallible because the decode is fallible: a
/// corrupt `stream_attrs` blob must still error the query, exactly as
/// [`merged_attrs`] does inside `build_batch`, never silently drop or leak the
/// row. A `Vec::retain` closure cannot propagate an error, so the survivor set
/// is built explicitly.
pub(crate) fn retain_unerased(
    records: &mut Vec<LogRecord>,
    erasure: &[ErasurePredicate],
) -> DFResult<()> {
    if erasure.is_empty() {
        return Ok(());
    }
    let mut survivors = Vec::with_capacity(records.len());
    for r in std::mem::take(records) {
        let merged = merged_attrs(&r)?;
        let erased = erasure
            .iter()
            .any(|p| p.matches_log_attrs(&merged) && (!p.has_window() || p.ts_in_window(r.ts_ns)));
        if !erased {
            survivors.push(r);
        }
    }
    *records = survivors;
    Ok(())
}

/// Look up one key in a [`merged_attrs`] result, for tables that promote a
/// well-known attribute into a typed column. Returns the first match in blob
/// order (record attributes have already overwritten a colliding
/// resource/scope value in place, so first-match is the record-wins value).
pub(crate) fn find_attr<'a>(merged: &'a [(String, AttrValue)], key: &str) -> Option<&'a AttrValue> {
    merged.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// v1 stringification of a dynamic attribute value for a `Map(Utf8, Utf8)`
/// `attrs` column. Scalar values render to their natural text; `Bytes`, `List`,
/// and `Map` render to the lowercase hex of their canonical encoding, a
/// deterministic, injective form pending a richer typed column.
pub(crate) use ravel_logseg::record::attr_value_to_string;

// --- `attrs` column contents: merged resource/scope + record attributes ---

/// Decode the **top-level** resource and scope attribute entries of a
/// `stream_attrs` blob (a record's [`ravel_logseg::LogRecord::stream_attrs`],
/// the canonical `resource ++ scope-name ++ scope-version ++ scope-attrs`
/// bytes) into `(key, value)` pairs, resource entries first, then scope
/// entries. The scope name and version are length-prefixed *positional*
/// fields, not key-value entries, so they never become synthetic
/// `scope.name`/`scope.version` keys. Delegates the actual decode to
/// [`ravel_logseg::record::decode_stream_attrs`], the structured, full-fidelity
/// decoder shared with the reader.
///
/// A top-level entry whose value is itself a `Map` or `List` is decoded (so
/// the underlying walk stays in frame) but **omitted** from the returned
/// pairs: a resource/scope attribute whose value is a map or list is not
/// projected into the map column (a richer typed representation is a v-next
/// refinement). Per-record dynamic attributes with nested values are unaffected
/// -- they are merged in verbatim by [`merged_attrs`] and rendered by
/// [`attr_value_to_string`].
pub(crate) fn decode_stream_attrs(blob: &[u8]) -> DFResult<Vec<(String, AttrValue)>> {
    let attrs =
        ravel_logseg::record::decode_stream_attrs(blob).map_err(|e| corrupt(&e.to_string()))?;
    Ok(attrs
        .resource
        .into_iter()
        .chain(attrs.scope_attrs)
        .filter(|(_, v)| !matches!(v, AttrValue::Map(_) | AttrValue::List(_)))
        .collect())
}

fn corrupt(what: &str) -> DataFusionError {
    // A malformed blob here means a record we decoded carried corrupt canonical
    // stream_attrs bytes: the same data-integrity fault the fetcher reports as
    // `LogFetchError::Corrupt`, just detected one layer up. Surface it with the
    // identical client class/message (`MSG_CORRUPT`, `ErrorClass::Unavailable`)
    // via `SqlError::CorruptStreamAttrs`, not a distinct internal-error class,
    // so one underlying fault never maps to two client-visible classes. Never a
    // panic or a silently-wrong filter result.
    SqlError::CorruptStreamAttrs(what.to_string()).into()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_logseg::stream_attrs_bytes;

    use super::*;

    fn s(v: &str) -> AttrValue {
        AttrValue::Str(v.into())
    }

    #[test]
    fn decode_yields_top_level_resource_and_scope_scalars() {
        let blob = stream_attrs_bytes(
            &[
                ("service.name".into(), s("api")),
                ("port".into(), AttrValue::I64(8080)),
            ],
            "libscope",
            "2.1",
            &[("lib".into(), s("otel"))],
        );
        let got = decode_stream_attrs(&blob).unwrap();
        assert!(got.contains(&("service.name".to_string(), s("api"))));
        assert!(got.contains(&("port".to_string(), AttrValue::I64(8080))));
        assert!(got.contains(&("lib".to_string(), s("otel"))));
        // Scope name/version are positional, never synthesized into keys or values.
        assert!(
            !got.iter()
                .any(|(k, _)| k == "scope.name" || k == "scope.version")
        );
        assert!(
            !got.iter()
                .any(|(_, v)| matches!(v, AttrValue::Str(x) if x == "libscope" || x == "2.1"))
        );
    }

    #[test]
    fn decode_omits_nested_map_and_list_but_stays_in_frame() {
        let blob = stream_attrs_bytes(
            &[
                (
                    "k8s.labels".into(),
                    AttrValue::Map(vec![("service.name".into(), s("api"))]),
                ),
                ("tags".into(), AttrValue::List(vec![s("a"), s("b")])),
                ("host".into(), s("h1")),
            ],
            "s",
            "1",
            &[],
        );
        // Nested map/list top-level entries are consumed but omitted; the scalar
        // sibling still decodes, proving the walk stayed byte-aligned.
        assert_eq!(
            decode_stream_attrs(&blob).unwrap(),
            vec![("host".into(), s("h1"))]
        );
    }

    #[test]
    fn decode_roundtrips_scalar_types() {
        let blob = stream_attrs_bytes(
            &[
                ("b".into(), AttrValue::Bool(true)),
                ("f".into(), AttrValue::F64(-0.0)),
                ("by".into(), AttrValue::Bytes(vec![1, 2, 3])),
                ("i".into(), AttrValue::I64(-42)),
            ],
            "s",
            "1",
            &[],
        );
        let got: std::collections::BTreeMap<_, _> =
            decode_stream_attrs(&blob).unwrap().into_iter().collect();
        assert_eq!(got["b"], AttrValue::Bool(true));
        assert_eq!(got["by"], AttrValue::Bytes(vec![1, 2, 3]));
        assert_eq!(got["i"], AttrValue::I64(-42));
        // -0.0 preserved through the bit pattern (writer's f64::to_bits discipline).
        match &got["f"] {
            AttrValue::F64(x) => assert_eq!(x.to_bits(), (-0.0f64).to_bits()),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_truncated_blob_as_corrupt() {
        let blob = stream_attrs_bytes(&[("k".into(), s("v"))], "s", "1", &[]);
        // Chop the last byte: the value payload is now truncated.
        let err = decode_stream_attrs(&blob[..blob.len() - 1]).unwrap_err();
        // Surfaces as the shared corruption class, not a panic or wrong data.
        let sql = match err {
            DataFusionError::External(b) => b.downcast::<SqlError>().expect("SqlError"),
            other => panic!("expected External, got {other:?}"),
        };
        assert_eq!(sql.class(), crate::error::ErrorClass::Unavailable);
        assert_eq!(sql.client_message(), crate::error::MSG_CORRUPT);
    }

    #[test]
    fn merged_attrs_record_wins_collision_and_keeps_resource() {
        let resource = [("service.name".into(), s("api")), ("host".into(), s("h1"))];
        let r = LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(&resource, "sc", "1", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "sc", "1", &[]),
            ts_ns: 1,
            observed_ts_ns: 1,
            severity_num: 0,
            severity_text: String::new(),
            body: String::new(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![
                ("service.name".into(), s("override")),
                ("dyn".into(), s("v")),
            ],
        };
        let merged = merged_attrs(&r).unwrap();
        // Exactly one service.name entry (no duplicate key from the merge).
        assert_eq!(
            merged.iter().filter(|(k, _)| k == "service.name").count(),
            1
        );
        // find_attr returns the record-wins value.
        assert_eq!(find_attr(&merged, "service.name"), Some(&s("override")));
        assert_eq!(find_attr(&merged, "host"), Some(&s("h1")));
        assert_eq!(find_attr(&merged, "dyn"), Some(&s("v")));
        assert_eq!(find_attr(&merged, "absent"), None);
    }
}
