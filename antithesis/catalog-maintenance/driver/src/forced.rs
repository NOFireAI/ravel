//! The record race and the unfolded-record sweep (README.md, "The forced
//! races"): which bucket each one works on, when a held server PUT is fresh
//! enough to race, and which of a compaction record's inputs the store holds.

use std::collections::BTreeSet;
use std::time::Duration;

use ravel_antithesis_catalog_driver::keys::{self, RecordKey};
use ravel_antithesis_catalog_driver::proxy::{self, HOLD_CAP};

use crate::audit::{DATA_SUFFIX, InputKeys};

/// The driver releases a held record PUT once it is this old, whether or not
/// the `ravel-cli` compaction has published by then: before the proxy's own
/// cap, so the release, not the cap, decides when the PUT is sent.
pub const RELEASE_BY: Duration = Duration::from_secs(14);
const _: () = assert!(RELEASE_BY.as_secs() + 2 < HOLD_CAP.as_secs());

/// A held PUT older than this when the driver first sees it leaves too
/// little of [`RELEASE_BY`] for the compaction; the driver releases it and
/// holds the next one.
pub const MAX_HELD_AGE: Duration = Duration::from_secs(6);

/// The `(hour, shard)` of the first of `hours` whose bucket in some shard a
/// compaction would publish a record for, from a LIST of the tenant's logs
/// commit keys.
pub fn compactable_bucket(
    tenant_prefix: &str,
    hours: impl IntoIterator<Item = u64>,
    shards: u32,
    listed: &BTreeSet<String>,
) -> Option<(u64, u32)> {
    for hour in hours {
        for shard in 0..shards {
            let prefix = keys::bucket_prefix(tenant_prefix, shard, hour);
            let under = listed
                .range(prefix.clone()..)
                .take_while(|k| k.starts_with(&prefix));
            if keys::bucket(&prefix, under).compactable() {
                return Some((hour, shard));
            }
        }
    }
    None
}

/// What the proxy's `held` reply means for the record race.
#[derive(Debug, PartialEq, Eq)]
pub enum Held {
    /// Nothing is held yet.
    Nothing,
    /// A logs compaction record of the tenant, fresh enough to race.
    Race {
        key: String,
        record: RecordKey,
        age: Duration,
    },
    /// Anything else held: release it and hold the next.
    Skip(String),
}

/// Judge one `held` reply for the tenant at `tenant_prefix`.
pub fn judge_held(reply: &str, tenant_prefix: &str) -> Held {
    let Some((key, age)) = proxy::parse_held(reply) else {
        return Held::Nothing;
    };
    match keys::parse_record_key(&key) {
        Some(record)
            if record.tenant_prefix == tenant_prefix
                && record.signal == "l"
                && age <= MAX_HELD_AGE =>
        {
            Held::Race { key, record, age }
        }
        _ => Held::Skip(key),
    }
}

/// How long the `ravel-cli` compaction may run before a PUT held for `age`
/// is released.
pub fn cli_budget(age: Duration) -> Duration {
    RELEASE_BY.saturating_sub(age)
}

/// The keys of `inputs` that `listed` holds: each input's commit record and
/// every data object under its data prefix.
pub fn present_inputs(inputs: &[InputKeys], listed: &BTreeSet<String>) -> Vec<String> {
    let mut present = Vec::new();
    for input in inputs {
        if listed.contains(&input.commit) {
            present.push(input.commit.clone());
        }
        present.extend(
            listed
                .range(input.data_prefix.clone()..)
                .take_while(|k| k.starts_with(&input.data_prefix))
                .filter(|k| k.ends_with(DATA_SUFFIX))
                .cloned(),
        );
    }
    present
}

/// Whether the unfolded-record sweep reached what it exists for: HEAD held
/// the bytes it held before the compaction for the whole sequence, so no
/// fold folded the record, the record is still there, and the store held
/// its inputs before the clocks moved.
pub fn unfolded_reached(
    head_before: &[u8],
    head_after: Option<&[u8]>,
    record_present: bool,
    inputs_before: usize,
) -> bool {
    head_after == Some(head_before) && record_present && inputs_before > 0
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREFIX: &str = "t/0a0b/";
    const HOUR: u64 = 1_794_931_200 / 3600;

    fn listing(keys: &[String]) -> BTreeSet<String> {
        keys.iter().cloned().collect()
    }

    fn key(shard: u32, hour: u64, name: &str) -> String {
        format!("{}{name}", keys::bucket_prefix(PREFIX, shard, hour))
    }

    #[test]
    fn the_first_compactable_bucket_is_picked_in_hour_order() {
        let listed = listing(&[
            key(0, HOUR, "w1.1.00000000000000000001.cmt"),
            key(2, HOUR, "w1.1.00000000000000000002.cmt"),
            key(2, HOUR, "w2.1.00000000000000000001.cmt"),
            key(1, HOUR + 1, "w1.1.00000000000000000003.cmt"),
            key(1, HOUR + 1, "w1.1.00000000000000000004.cmt"),
        ]);
        assert_eq!(
            compactable_bucket(PREFIX, [HOUR, HOUR + 1], 4, &listed),
            Some((HOUR, 2))
        );
        assert_eq!(
            compactable_bucket(PREFIX, [HOUR + 1, HOUR], 4, &listed),
            Some((HOUR + 1, 1))
        );
        // A shard past the count is not looked at.
        assert_eq!(compactable_bucket(PREFIX, [HOUR], 2, &listed), None);
        // A bucket with a record already is not a candidate.
        let mut done = listed.clone();
        done.insert(key(2, HOUR, "l1.0011223344556677.cmt"));
        assert_eq!(compactable_bucket(PREFIX, [HOUR], 4, &done), None);
        // Another tenant's keys do not count.
        assert_eq!(compactable_bucket("t/ffff/", [HOUR], 4, &listed), None);
    }

    #[test]
    fn only_a_fresh_held_record_of_the_tenant_is_raced() {
        let record = key(3, HOUR, "l1.0011223344556677.cmt");
        assert_eq!(judge_held("none", PREFIX), Held::Nothing);
        assert_eq!(
            judge_held(&format!("held {record} 1500"), PREFIX),
            Held::Race {
                key: record.clone(),
                record: RecordKey {
                    tenant_prefix: PREFIX.to_string(),
                    signal: "l".to_string(),
                    shard: 3,
                    hour: HOUR,
                },
                age: Duration::from_millis(1500),
            }
        );
        let at_limit = format!("held {record} {}", MAX_HELD_AGE.as_millis());
        assert!(matches!(judge_held(&at_limit, PREFIX), Held::Race { .. }));
        let stale = format!("held {record} {}", MAX_HELD_AGE.as_millis() + 1);
        assert_eq!(judge_held(&stale, PREFIX), Held::Skip(record.clone()));
        assert_eq!(
            judge_held(&format!("held {record} 10"), "t/ffff/"),
            Held::Skip(record)
        );
        let metrics = "t/0a0b/m/c/0003/20261117T16/l1.0011223344556677.cmt";
        assert_eq!(
            judge_held(&format!("held {metrics} 10"), PREFIX),
            Held::Skip(metrics.to_string())
        );
    }

    #[test]
    fn the_compaction_gets_what_is_left_before_the_release() {
        assert_eq!(cli_budget(Duration::ZERO), RELEASE_BY);
        assert_eq!(cli_budget(MAX_HELD_AGE), RELEASE_BY - MAX_HELD_AGE);
        assert_eq!(cli_budget(RELEASE_BY + Duration::from_secs(1)), Duration::ZERO);
        // The freshest PUT the race takes still leaves the compaction most
        // of the window.
        assert!(cli_budget(MAX_HELD_AGE) >= Duration::from_secs(8));
    }

    #[test]
    fn present_inputs_are_commit_records_and_data_objects() {
        let inputs = vec![
            InputKeys {
                commit: key(0, HOUR, "w1.1.00000000000000000001.cmt"),
                data_prefix: format!("{PREFIX}l/l0/0000/w1.1.00000000000000000001."),
            },
            InputKeys {
                commit: key(0, HOUR, "w1.1.00000000000000000002.cmt"),
                data_prefix: format!("{PREFIX}l/l0/0000/w1.1.00000000000000000002."),
            },
        ];
        let data1 = format!("{}abcd{DATA_SUFFIX}", inputs[0].data_prefix);
        let listed = listing(&[
            inputs[0].commit.clone(),
            data1.clone(),
            format!("{}abcd.tmp", inputs[0].data_prefix),
            format!("{PREFIX}l/l0/0000/w1.1.000000000000000000010.ef{DATA_SUFFIX}"),
            format!("{}ef01{DATA_SUFFIX}", inputs[1].data_prefix),
        ]);
        assert_eq!(
            present_inputs(&inputs, &listed),
            vec![
                inputs[0].commit.clone(),
                data1,
                format!("{}ef01{DATA_SUFFIX}", inputs[1].data_prefix),
            ]
        );
        assert!(present_inputs(&inputs, &BTreeSet::new()).is_empty());
    }

    #[test]
    fn the_unfolded_sweep_counts_only_an_unmoved_head() {
        assert!(unfolded_reached(b"head", Some(b"head"), true, 3));
        assert!(!unfolded_reached(b"head", Some(b"head2"), true, 3));
        assert!(!unfolded_reached(b"head", None, true, 3));
        assert!(!unfolded_reached(b"head", Some(b"head"), false, 3));
        assert!(!unfolded_reached(b"head", Some(b"head"), true, 0));
    }
}
