//! Gate test for the checked-in ClickBench mapping (issue #519), at
//! `benchmarks/clickbench/hits.mapping.toml`.
//!
//! `CounterID` is the one column in the flat `hits` table that acts as a
//! per-entity key. It must stay a `[[resource_attribute]]`, not a
//! `[[attribute]]`: only resource attributes are hashed into stream identity
//! (ADR-0029 `log_stream_id`), and `shard_for_log` uses that hash to pick a
//! shard. Left as a record attribute, every row's resource-attribute set is
//! empty, every row hashes to the same stream, and every write lands on one
//! shard regardless of `--shards` -- the single-shard, single-core load this
//! issue diagnosed. A future edit that moves `CounterID` back to
//! `[[attribute]]` (or drops it) reintroduces that bottleneck silently; this
//! test catches it the same way `clickbench_corpus.rs` keeps the corpus file
//! honest against the runbook.
#![allow(clippy::expect_used)]

use std::path::PathBuf;

use ravel_cli::load::{ColType, parse_mapping};

fn mapping_path() -> PathBuf {
    PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/clickbench/hits.mapping.toml"
    ))
}

#[test]
fn counter_id_is_the_sole_resource_attribute() {
    let text = std::fs::read_to_string(mapping_path()).expect("hits.mapping.toml is checked in");
    let mapping = parse_mapping(&text).expect("hits.mapping.toml parses");

    assert_eq!(
        mapping.resource_attributes.len(),
        1,
        "expected exactly one resource_attribute (CounterID); found {:?}",
        mapping
            .resource_attributes
            .iter()
            .map(|a| &a.key)
            .collect::<Vec<_>>()
    );
    let counter_id = &mapping.resource_attributes[0];
    assert_eq!(counter_id.key, "CounterID");
    assert_eq!(counter_id.column, "CounterID");
    assert_eq!(counter_id.value_type, ColType::I64);

    assert!(
        mapping.attributes.iter().all(|a| a.key != "CounterID"),
        "CounterID must not also appear as a record attribute"
    );
    assert_eq!(
        mapping.attributes.len(),
        103,
        "expected 103 record attributes now that CounterID moved to resource_attribute"
    );
}
