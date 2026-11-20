//! The logs erasure rewrite drops exactly the records the query path hides
//! (issue #4503).
//!
//! `ravel_maintain::erasure_rewrite::LogErasureMatcher` is a copy of the query
//! rule kept for dependency direction. If the rewrite drops less than the
//! query hides, an erased subject is served again once its request is swept;
//! if it drops more, the rewrite deletes records no request erased. This
//! property compares the two over generated records whose resource, scope and
//! record attributes share keys, repeat keys and mix value types.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use proptest::prelude::*;
use ravel_logseg::{AttrValue, LogRecord, stream_attrs_bytes};
use ravel_maintain::erasure_rewrite::{LogErasureMatcher, stream_view};
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_query::erasure::{ErasurePredicate, is_erased_log_row, merged_log_attrs};
use ravel_types::logstream::log_stream_id;

const KEYS: [&str; 3] = ["user_id", "host", "env"];
const VALUES: [&str; 2] = ["alice", "bob"];

fn value() -> impl Strategy<Value = AttrValue> {
    prop_oneof![
        4 => (0..VALUES.len()).prop_map(|i| AttrValue::Str(VALUES[i].to_string())),
        1 => any::<i64>().prop_map(AttrValue::I64),
        1 => Just(AttrValue::Map(vec![(
            "user_id".to_string(),
            AttrValue::Str("alice".to_string()),
        )])),
        1 => Just(AttrValue::List(vec![AttrValue::Str("alice".to_string())])),
    ]
}

fn attrs(max: usize) -> impl Strategy<Value = Vec<(String, AttrValue)>> {
    prop::collection::vec(
        ((0..KEYS.len()).prop_map(|i| KEYS[i].to_string()), value()),
        0..=max,
    )
}

fn matchers() -> impl Strategy<Value = Vec<(String, String)>> {
    prop::collection::vec(
        (0..KEYS.len(), 0..VALUES.len())
            .prop_map(|(k, v)| (KEYS[k].to_string(), VALUES[v].to_string())),
        1..=2,
    )
}

fn first_str<'a>(attrs: &'a [(String, AttrValue)], key: &str) -> Option<&'a str> {
    attrs.iter().find_map(|(k, v)| match v {
        AttrValue::Str(s) if k == key => Some(s.as_str()),
        _ => None,
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4096))]

    #[test]
    fn rewrite_drops_exactly_what_the_query_hides(
        resource in attrs(4),
        scope in attrs(3),
        record_attrs in attrs(4),
        matchers in matchers(),
        window in prop_oneof![
            Just((0i64, 0i64)),
            Just((2_000i64, 3_000i64)),
            Just((2_000i64, 0i64)),
            Just((0i64, 3_000i64)),
        ],
        ts_ns in prop_oneof![Just(1_000i64), Just(2_000i64), Just(2_999i64), Just(3_000i64)],
    ) {
        let blob = stream_attrs_bytes(&resource, "scope-lib", "1.0", &scope);
        let (start, end) = window;
        let record = LogRecord {
            stream_id: log_stream_id(&resource, "scope-lib", "1.0", &scope),
            stream_attrs: blob.clone(),
            ts_ns,
            observed_ts_ns: ts_ns,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: String::new(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: record_attrs.clone(),
        };

        let predicate = ErasurePredicate::new(matchers.clone(), start, end);
        let merged = merged_log_attrs(&record).expect("valid stream attrs");
        let hidden = is_erased_log_row(
            std::slice::from_ref(&predicate),
            ts_ns,
            |k| first_str(&record.attrs, k),
            |k| first_str(&merged, k),
        );

        let request = ErasureRequest {
            predicate: matchers
                .iter()
                .map(|(k, v)| ErasurePredicateMatcher { key: k.clone(), value: v.clone() })
                .collect(),
            window_start_ns: start,
            window_end_ns: end,
            ..ErasureRequest::default()
        };
        let stream = stream_view(&blob).expect("valid stream attrs");
        let dropped = LogErasureMatcher::from_request(&request)
            .drops_log_record(&record.attrs, &stream, ts_ns);

        prop_assert_eq!(dropped, hidden, "merged view {:?}", merged);
    }
}

/// The issue's case, pinned outside the generator: a subject named only in a
/// resource attribute, then only in a scope attribute, is dropped; a record
/// attribute with a different value shadows the resource value and is kept.
#[test]
fn a_stream_attribute_subject_is_dropped_and_a_shadowing_record_value_is_kept() {
    let request = ErasureRequest {
        predicate: vec![ErasurePredicateMatcher {
            key: "user_id".to_string(),
            value: "alice".to_string(),
        }],
        ..ErasureRequest::default()
    };
    let matcher = LogErasureMatcher::from_request(&request);
    let alice = vec![("user_id".to_string(), AttrValue::Str("alice".to_string()))];
    let bob = vec![("user_id".to_string(), AttrValue::Str("bob".to_string()))];

    let resource = stream_view(&stream_attrs_bytes(&alice, "s", "1", &[])).unwrap();
    assert!(matcher.drops_log_record(&[], &resource, 1));
    let scope = stream_view(&stream_attrs_bytes(&[], "s", "1", &alice)).unwrap();
    assert!(matcher.drops_log_record(&[], &scope, 1));
    assert!(!matcher.drops_log_record(&bob, &resource, 1));
    assert!(matcher.drops_log_record(&alice, &[], 1));
}

/// A key held twice in a record's own attributes: the query path reads the
/// first string value from the record view and the last from the merged
/// view (the overlay replaces one slot), so `user_id = carol` between them
/// is hidden by neither and the rewrite keeps the record.
#[test]
fn a_repeated_record_key_matches_as_the_query_path_reads_it() {
    let request = |subject: &str| ErasureRequest {
        predicate: vec![ErasurePredicateMatcher {
            key: "user_id".to_string(),
            value: subject.to_string(),
        }],
        ..ErasureRequest::default()
    };
    let record: Vec<(String, AttrValue)> = ["alice", "carol", "bob"]
        .iter()
        .map(|v| ("user_id".to_string(), AttrValue::Str((*v).to_string())))
        .collect();
    let drops = |subject: &str| {
        LogErasureMatcher::from_request(&request(subject)).drops_log_record(&record, &[], 1)
    };
    assert!(drops("alice"), "first value, record view");
    assert!(drops("bob"), "last value, merged view");
    assert!(!drops("carol"), "a middle value is in neither view");
}
