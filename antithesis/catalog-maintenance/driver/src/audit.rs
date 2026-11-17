//! The store audit: what `ravel-cli catalog inspect` says HEAD names, against
//! what a LIST of the tenant's prefix says exists.
//!
//! The text form of `catalog inspect` prints every key HEAD names as a
//! `key=<object key>` token: each snapshot part, each part's column
//! statistics object and each key-index leaf. It then fetches and decodes
//! every part. `ravel-cli rlog footprint --tenant --json` lists and fetches
//! every data object the catalog resolves for the tenant. No `ravel-cli`
//! command prints a part's commit-record entries, so the audit cannot check
//! those; see the README's "Missing decoder" note.

use std::collections::BTreeSet;

/// What one `catalog inspect` run showed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Inspected {
    /// The HEAD exists and decoded.
    pub head_present: bool,
    pub watermark_hour: Option<u32>,
    pub created_unix_ns: Option<u64>,
    /// Every object key the HEAD names, in print order, deduplicated.
    pub named: Vec<String>,
    /// The snapshot part keys (`part[i] ... key=`), in print order.
    pub parts: Vec<String>,
    /// How many key-index leaf keys (`field=... key=`) were printed.
    pub kidx_leaves: usize,
}

/// One snapshot part as `catalog inspect --json` lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonPart {
    pub key: String,
    pub min_hour: u32,
    pub watermark_hour: u32,
    pub entry_count: u64,
    pub key_index_leaves: usize,
    pub retired_hours: Vec<u32>,
}

/// The parts of a `catalog inspect --json` HEAD, `None` when any part does
/// not have the expected fields.
pub fn json_parts(head: &serde_json::Value) -> Option<Vec<JsonPart>> {
    let u32_of = |v: &serde_json::Value, k: &str| {
        v.get(k)
            .and_then(serde_json::Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
    };
    head.get("parts")?
        .as_array()?
        .iter()
        .map(|p| {
            Some(JsonPart {
                key: p.get("key")?.as_str()?.to_string(),
                min_hour: u32_of(p, "min_hour")?,
                watermark_hour: u32_of(p, "watermark_hour")?,
                entry_count: p.get("entry_count")?.as_u64()?,
                key_index_leaves: usize::try_from(p.get("key_index_leaves")?.as_u64()?).ok()?,
                retired_hours: p
                    .get("retired_hours")?
                    .as_array()?
                    .iter()
                    .map(|h| h.as_u64().and_then(|n| u32::try_from(n).ok()))
                    .collect::<Option<Vec<u32>>>()?,
            })
        })
        .collect()
}

/// Whether the text parse found exactly the parts and key-index leaves the
/// JSON form lists for the same HEAD. An audit over a parse that found
/// nothing, or found a different set, would pass without checking anything.
pub fn cross_check(text: &Inspected, parts: &[JsonPart]) -> Result<(), String> {
    if parts.is_empty() {
        return Err("catalog inspect --json lists no parts".to_string());
    }
    let json_keys: Vec<&str> = parts.iter().map(|p| p.key.as_str()).collect();
    let text_keys: Vec<&str> = text.parts.iter().map(String::as_str).collect();
    if json_keys != text_keys {
        return Err(format!(
            "the text form names parts {text_keys:?}, the JSON form {json_keys:?}"
        ));
    }
    let leaves: usize = parts.iter().map(|p| p.key_index_leaves).sum();
    if leaves != text.kidx_leaves {
        return Err(format!(
            "the text form names {} key-index leaves, the JSON form counts {leaves}",
            text.kidx_leaves
        ));
    }
    Ok(())
}

/// The hours of `hours` that some part of HEAD covers: inside the part's
/// `min_hour..=watermark_hour`, not among its retired hours, in a part with
/// entries.
pub fn covered_hours(parts: &[JsonPart], hours: impl IntoIterator<Item = u32>) -> Vec<u32> {
    hours
        .into_iter()
        .filter(|h| {
            parts.iter().any(|p| {
                p.entry_count > 0
                    && (p.min_hour..=p.watermark_hour).contains(h)
                    && !p.retired_hours.contains(h)
            })
        })
        .collect()
}

/// The data object keys of `ravel-cli rlog footprint --json`'s `objects`,
/// `None` when the output is not that report.
pub fn footprint_keys(json: &str) -> Option<Vec<String>> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    v.get("objects")?
        .as_array()?
        .iter()
        .map(|o| o.get("key").and_then(|k| k.as_str()).map(str::to_string))
        .collect()
}

/// Parse the text form of `catalog inspect`. A HEAD that is absent prints
/// `HEAD at <key> is absent or unreadable`; one that is corrupt fails the
/// command before printing any field, which leaves `head_present` false.
pub fn parse_inspect(text: &str) -> Inspected {
    let mut out = Inspected::default();
    let mut seen = BTreeSet::new();
    for line in text.lines() {
        let line = line.trim();
        if let Some(v) = line.strip_prefix("watermark_hour: ") {
            out.watermark_hour = v.trim().parse().ok();
            out.head_present = true;
        } else if let Some(v) = line.strip_prefix("created_unix_ns: ") {
            out.created_unix_ns = v.trim().parse().ok();
        }
        for token in line.split_whitespace() {
            let Some(key) = token.strip_prefix("key=").filter(|k| !k.is_empty()) else {
                continue;
            };
            if line.starts_with("part[") {
                out.parts.push(key.to_string());
            } else if line.starts_with("field=") {
                out.kidx_leaves += 1;
            }
            if seen.insert(key.to_string()) {
                out.named.push(key.to_string());
            }
        }
    }
    out
}

/// The part key from `catalog inspect`'s
/// `Error: failed to fetch part <key>: object not found` (anyhow's report of a
/// `StoreError::NotFound`), and `None` for any other cause, as for
/// [`unfetched_object`]: only a not-found means HEAD names a part the store
/// does not hold.
pub fn unfetched_part(stderr: &str) -> Option<String> {
    stderr.lines().find_map(|line| {
        let (key, cause) = line
            .trim()
            .strip_prefix("Error: failed to fetch part ")?
            .split_once(": ")?;
        (!key.is_empty() && cause.trim() == "object not found").then(|| key.to_string())
    })
}

/// The data object key from `rlog footprint`'s
/// `Error: failed to fetch <key>: object not found` (anyhow's report of a
/// `StoreError::NotFound`): the catalog resolved a data object the store does
/// not hold. `None` for any other failure, including a fetch of `sys/tenancy`
/// or a HEAD, a key outside `<tenant_prefix>l/l0/` and `<tenant_prefix>l/l1/`
/// or not ending in [`DATA_SUFFIX`], and any store error other than
/// not-found; the audit then has no verdict.
pub fn unfetched_object(stderr: &str, tenant_prefix: &str) -> Option<String> {
    let l0 = format!("{tenant_prefix}l/l0/");
    let l1 = format!("{tenant_prefix}l/l1/");
    stderr.lines().find_map(|line| {
        let (key, cause) = line
            .trim()
            .strip_prefix("Error: failed to fetch ")?
            .split_once(": ")?;
        ((key.starts_with(&l0) || key.starts_with(&l1))
            && key.ends_with(DATA_SUFFIX)
            && cause.trim() == "object not found")
            .then(|| key.to_string())
    })
}

/// The suffix of every data object key Ravel writes, level 0 and level 1
/// alike: `.` followed by `ravel_commit::keys::DATA_SUFFIX` (`rseg`).
pub const DATA_SUFFIX: &str = ".rseg";

/// The keys of one level-0 input a compaction record names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputKeys {
    /// The input's commit record, in the compaction record's own directory.
    pub commit: String,
    /// The input's data object key up to its content hash, which the record
    /// does not carry: `<tenant_prefix>l/l0/<shard>/<writer>.<epoch>.<seq>.`.
    pub data_prefix: String,
}

/// The inputs of the compaction record at `record_key`, from what `ravel-cli
/// commit decode-compaction` printed for it. The keys follow
/// `ravel_commit::keys::commit_key` and `data_key`. `None` when the record
/// key is not a logs compaction record under `tenant_prefix`, or the output
/// lists another number of `writer_id=` lines than its `inputs:` count.
pub fn compaction_inputs(
    tenant_prefix: &str,
    record_key: &str,
    decoded: &str,
) -> Option<Vec<InputKeys>> {
    let rest = record_key
        .strip_prefix(tenant_prefix)?
        .strip_prefix("l/c/")?;
    let (dir, name) = record_key.rsplit_once('/')?;
    if !(name.starts_with("l1.") && name.ends_with(".cmt")) {
        return None;
    }
    let shard = rest.split('/').next().filter(|s| !s.is_empty())?;
    let mut count = None;
    let mut inputs = Vec::new();
    for line in decoded.lines() {
        let line = line.trim();
        if let Some(n) = line.strip_prefix("inputs: ") {
            count = Some(n.trim().parse::<usize>().ok()?);
            continue;
        }
        let Some(fields) = line.strip_prefix("writer_id=") else {
            continue;
        };
        let mut it = fields.split_whitespace();
        let writer = it.next()?;
        let epoch: u64 = it.next()?.strip_prefix("writer_epoch=")?.parse().ok()?;
        let seq: u64 = it.next()?.strip_prefix("writer_seq=")?.parse().ok()?;
        let id = format!("{writer}.{epoch}.{seq:020}");
        inputs.push(InputKeys {
            commit: format!("{dir}/{id}.cmt"),
            data_prefix: format!("{tenant_prefix}l/l0/{shard}/{id}."),
        });
    }
    (count? == inputs.len()).then_some(inputs)
}

/// The input keys of `inputs` that an earlier LIST held (`seen`) and
/// `listed` does not: the input's commit record, and every data object
/// under its data prefix ending in [`DATA_SUFFIX`].
pub fn swept_inputs(
    inputs: &[InputKeys],
    seen: &BTreeSet<String>,
    listed: &BTreeSet<String>,
) -> Vec<String> {
    let mut gone = Vec::new();
    for input in inputs {
        if seen.contains(&input.commit) && !listed.contains(&input.commit) {
            gone.push(input.commit.clone());
        }
        gone.extend(
            seen.range(input.data_prefix.clone()..)
                .take_while(|k| k.starts_with(&input.data_prefix))
                .filter(|k| k.ends_with(DATA_SUFFIX) && !listed.contains(*k))
                .cloned(),
        );
    }
    gone
}

/// What the sweep check has learned across audits: every key a LIST held,
/// and the swept inputs a GET confirmed gone. Keys are never reused, so a
/// confirmed key stays gone and counts once.
#[derive(Debug, Default)]
pub struct SweepLedger {
    seen: BTreeSet<String>,
    confirmed: BTreeSet<String>,
}

impl SweepLedger {
    /// Record the keys of one LIST.
    pub fn observe(&mut self, keys: impl IntoIterator<Item = String>) {
        self.seen.extend(keys);
    }

    /// The [`swept_inputs`] of `inputs` against `listed` that no earlier
    /// pass confirmed.
    pub fn unconfirmed(&self, inputs: &[InputKeys], listed: &BTreeSet<String>) -> Vec<String> {
        swept_inputs(inputs, &self.seen, listed)
            .into_iter()
            .filter(|k| !self.confirmed.contains(k))
            .collect()
    }

    /// Record a key a GET found absent; `false` when it was already counted.
    pub fn confirm(&mut self, key: &str) -> bool {
        self.confirmed.insert(key.to_string())
    }

    /// How many distinct swept inputs were confirmed.
    pub fn confirmed(&self) -> usize {
        self.confirmed.len()
    }
}

/// The keys HEAD names that the listing does not hold.
pub fn missing_named(named: &[String], listed: &BTreeSet<String>) -> Vec<String> {
    named
        .iter()
        .filter(|k| !listed.contains(*k))
        .cloned()
        .collect()
}

/// `t/<tenant_hash>/` from a HEAD key `t/<tenant_hash>/catalog/<signal>/HEAD`.
pub fn tenant_prefix(head_key: &str) -> Option<String> {
    let i = head_key.find("/catalog/")?;
    Some(format!("{}/", &head_key[..i]))
}

/// Compaction records (`l1.<input_set_hash16>.cmt`) among the listed logs
/// commit keys under `tenant_prefix`.
pub fn compaction_records<'a>(
    tenant_prefix: &str,
    listed: impl IntoIterator<Item = &'a String>,
) -> Vec<String> {
    let commits = format!("{tenant_prefix}l/c/");
    listed
        .into_iter()
        .filter(|k| k.starts_with(&commits))
        .filter(|k| {
            let name = k.rsplit('/').next().unwrap_or("");
            name.starts_with("l1.") && name.ends_with(".cmt")
        })
        .cloned()
        .collect()
}

/// L0 commit records (`<writer>.<epoch>.<seq>.cmt`) among the listed logs
/// commit keys.
pub fn l0_commit_records<'a>(
    tenant_prefix: &str,
    listed: impl IntoIterator<Item = &'a String>,
) -> Vec<String> {
    let commits = format!("{tenant_prefix}l/c/");
    listed
        .into_iter()
        .filter(|k| k.starts_with(&commits) && k.ends_with(".cmt"))
        .filter(|k| {
            let name = k.rsplit('/').next().unwrap_or("");
            !name.starts_with("l1.") && !name.starts_with("rw.")
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const INSPECT: &str = "\
store: s3 bucket ravel
format_version: 2
tenant_hash: 0a0b
signal: logs (2)
shard_count: 4
watermark_hour: 490001
folder_id: 11111111-2222-3333-4444-555555555555
created_unix_ns: 1764000000000000000
parts: 1
  part[0] range=0..=490001 key=t/0a0b/catalog/l/snap/490001.00aa.csnap blake3=ff size=10 entry_count=3
    column_stats (field 7): key=t/0a0b/catalog/l/idx/490001.00bb.cstat size=9
    key_index (field 8): 1
      field=1 slice=0x0000000000000000..=0xffffffffffffffff entry_count=3 uncovered_count=0 key=t/0a0b/catalog/l/idx/490001.00cc.kidx
    retired_hours (field 9): NONE
    header: format_version=2 min_hour=0 watermark_hour=490001 shard_count=4 entry_count=3 entries_uncompressed_len=99
    entries (decoded): 3
";

    #[test]
    fn inspect_text_names_parts_stats_and_leaves() {
        let i = parse_inspect(INSPECT);
        assert!(i.head_present);
        assert_eq!(i.watermark_hour, Some(490_001));
        assert_eq!(i.created_unix_ns, Some(1_764_000_000_000_000_000));
        assert_eq!(
            i.named,
            vec![
                "t/0a0b/catalog/l/snap/490001.00aa.csnap",
                "t/0a0b/catalog/l/idx/490001.00bb.cstat",
                "t/0a0b/catalog/l/idx/490001.00cc.kidx",
            ]
        );
    }

    fn json_head(parts: serde_json::Value) -> serde_json::Value {
        serde_json::json!({ "watermark_hour": 490001, "parts": parts })
    }

    fn one_part() -> serde_json::Value {
        serde_json::json!([{
            "min_hour": 0,
            "watermark_hour": 490001,
            "key": "t/0a0b/catalog/l/snap/490001.00aa.csnap",
            "blake3": "ff",
            "size": 10,
            "entry_count": 3,
            "key_index_leaves": 1,
            "retired_hours": [],
        }])
    }

    #[test]
    fn the_text_parse_matches_the_json_parts() {
        let parts = json_parts(&json_head(one_part())).expect("parts");
        let text = parse_inspect(INSPECT);
        assert_eq!(text.parts, vec!["t/0a0b/catalog/l/snap/490001.00aa.csnap"]);
        assert_eq!(text.kidx_leaves, 1);
        assert_eq!(cross_check(&text, &parts), Ok(()));
    }

    #[test]
    fn an_empty_or_mismatched_parse_fails_the_cross_check() {
        let parts = json_parts(&json_head(one_part())).expect("parts");
        // A text form the parser does not recognise names nothing.
        let empty = parse_inspect("parts: 1\n  part[0]: t/0a0b/catalog/l/snap/x.csnap\n");
        assert!(empty.named.is_empty());
        assert!(cross_check(&empty, &parts).is_err());
        // No parts at all in the JSON form.
        let none = json_parts(&json_head(serde_json::json!([]))).expect("parts");
        assert!(cross_check(&parse_inspect(INSPECT), &none).is_err());
        // A part the text form missed.
        let mut two = parts.clone();
        two.push(JsonPart {
            key: "t/0a0b/catalog/l/snap/490001.00dd.csnap".to_string(),
            ..parts[0].clone()
        });
        assert!(cross_check(&parse_inspect(INSPECT), &two).is_err());
        // A key-index leaf the text form missed.
        let text = parse_inspect(&INSPECT.replace("      field=1 ", "      fld=1 "));
        assert!(cross_check(&text, &parts).is_err());
    }

    #[test]
    fn covered_hours_need_a_part_with_entries_over_the_hour() {
        let mut parts = json_parts(&json_head(serde_json::json!([{
            "min_hour": 100, "watermark_hour": 110, "key": "k", "blake3": "", "size": 1,
            "entry_count": 4, "key_index_leaves": 0, "retired_hours": [104],
        }])))
        .expect("parts");
        assert_eq!(
            covered_hours(&parts, [99, 100, 104, 110, 111]),
            vec![100, 110]
        );
        parts[0].entry_count = 0;
        assert!(covered_hours(&parts, [100, 110]).is_empty());
        assert!(covered_hours(&[], [100]).is_empty());
    }

    #[test]
    fn footprint_objects_are_parsed() {
        let json = format!(
            r#"{{"total":{{}},"objects":[{{"key":"{L0_KEY}","level":0}},
                {{"key":"{L1_KEY}","level":1}}]}}"#
        );
        assert_eq!(
            footprint_keys(&json),
            Some(vec![L0_KEY.to_string(), L1_KEY.to_string()])
        );
        assert_eq!(footprint_keys("object_count: 2"), None);
    }

    /// Shapes from `ravel_commit::keys::data_key` and `l1_part_key`.
    const TENANT: &str = "t/7d3c0f5e9a1b2c4d6e8f00112233aabb/";
    const L0_KEY: &str = "t/7d3c0f5e9a1b2c4d6e8f00112233aabb/l/l0/0003/\
        5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000007.00112233aabbccdd.rseg";
    const L1_KEY: &str = "t/7d3c0f5e9a1b2c4d6e8f00112233aabb/l/l1/0001/20261117T16/\
        0f6eeb04e9886d29.0000.592aa104ca6bae9c.rseg";
    const DATA_KEY: &str = L0_KEY;

    #[test]
    fn a_data_object_not_found_under_the_tenant_is_missing() {
        for key in [L0_KEY, L1_KEY] {
            let err = format!("Error: failed to fetch {key}: object not found\n");
            assert_eq!(unfetched_object(&err, TENANT).as_deref(), Some(key));
        }
    }

    #[test]
    fn a_sys_tenancy_failure_has_no_verdict() {
        let err = "Error: failed to fetch sys/tenancy: transient error: 503 Slow Down\n";
        assert_eq!(unfetched_object(err, "t/0a0b/"), None);
        let err = "Error: failed to fetch sys/tenancy: object not found\n";
        assert_eq!(unfetched_object(err, "t/0a0b/"), None);
    }

    #[test]
    fn a_head_fetch_failure_has_no_verdict() {
        let err = "Error: failed to fetch HEAD t/0a0b/catalog/l/HEAD: timeout\n";
        assert_eq!(unfetched_object(err, "t/0a0b/"), None);
        let err = "Error: failed to fetch part t/0a0b/catalog/l/snap/1.aa.csnap: object not found";
        assert_eq!(unfetched_object(err, "t/0a0b/"), None);
    }

    #[test]
    fn a_transient_error_on_a_data_object_has_no_verdict() {
        for cause in [
            "transient error: connection reset",
            "timeout",
            "throttled, retry after 1000 ms",
            "access denied: RequestTimeTooSkewed",
        ] {
            let err = format!("Error: failed to fetch {DATA_KEY}: {cause}\n");
            assert_eq!(unfetched_object(&err, TENANT), None, "{cause}");
        }
    }

    #[test]
    fn a_not_found_outside_the_tenant_data_prefix_has_no_verdict() {
        let err = format!("Error: failed to fetch {DATA_KEY}: object not found\n");
        assert_eq!(
            unfetched_object(&err, "t/ffffffffffffffffffffffffffffffff/"),
            None
        );
        let err = "Error: failed to fetch t/0a0b/catalog/l/idx/1.bb.cstat: object not found\n";
        assert_eq!(unfetched_object(err, "t/0a0b/"), None);
        let commit = format!(
            "{TENANT}l/c/0003/20261117T16/5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000007.cmt"
        );
        let err = format!("Error: failed to fetch {commit}: object not found\n");
        assert_eq!(unfetched_object(&err, TENANT), None);
    }

    #[test]
    fn missing_head_named_object_is_reported() {
        let i = parse_inspect(INSPECT);
        let mut listed: BTreeSet<String> = i.named.iter().cloned().collect();
        listed.insert("t/0a0b/catalog/l/HEAD".to_string());
        assert!(missing_named(&i.named, &listed).is_empty());
        listed.remove("t/0a0b/catalog/l/idx/490001.00bb.cstat");
        assert_eq!(
            missing_named(&i.named, &listed),
            vec!["t/0a0b/catalog/l/idx/490001.00bb.cstat"]
        );
    }

    #[test]
    fn a_part_not_found_is_named() {
        let err =
            "Error: failed to fetch part t/0a0b/catalog/l/snap/1.aa.csnap: object not found\n";
        assert_eq!(
            unfetched_part(err).as_deref(),
            Some("t/0a0b/catalog/l/snap/1.aa.csnap")
        );
        assert_eq!(unfetched_part("Error: HEAD at k is corrupt: bad"), None);
    }

    #[test]
    fn a_part_fetch_failing_otherwise_has_no_verdict() {
        for cause in [
            "transient error: connection reset",
            "timeout",
            "throttled, retry after 1000 ms",
            "access denied: RequestTimeTooSkewed",
            "not found",
        ] {
            let err =
                format!("Error: failed to fetch part t/0a0b/catalog/l/snap/1.aa.csnap: {cause}\n");
            assert_eq!(unfetched_part(&err), None, "{cause}");
        }
    }

    const RECORD_KEY: &str = "t/0a0b/l/c/0002/20261117T16/l1.00112233aabbccdd.cmt";
    const DECODED: &str = "\
format_version: 1
tenant_hash: 0a0b
signal: 2
shard: 2
ingest_hour_bucket: 490001
level: 1
input_set_hash: 00112233aabbccdd
created_unix_ns: 1764000000000000000
inputs: 2
  writer_id=5f0c6a2e-8b1d-4c3a-9e7f-112233445566 writer_epoch=1 writer_seq=7
  writer_id=5f0c6a2e-8b1d-4c3a-9e7f-112233445566 writer_epoch=1 writer_seq=8
parts: 1
  part_index=0 first_series_id=00 last_series_id=ff content_hash=aa object_size=1 sample_count=1 series_count=1 run_count=1 min_event_ts_ns=0 max_event_ts_ns=1 segment_format_version=1
";

    #[test]
    fn compaction_inputs_name_the_commit_record_and_data_prefix() {
        let inputs = compaction_inputs("t/0a0b/", RECORD_KEY, DECODED).expect("inputs");
        assert_eq!(
            inputs,
            vec![
                InputKeys {
                    commit: "t/0a0b/l/c/0002/20261117T16/\
                        5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000007.cmt"
                        .to_string(),
                    data_prefix: "t/0a0b/l/l0/0002/\
                        5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000007."
                        .to_string(),
                },
                InputKeys {
                    commit: "t/0a0b/l/c/0002/20261117T16/\
                        5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000008.cmt"
                        .to_string(),
                    data_prefix: "t/0a0b/l/l0/0002/\
                        5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000008."
                        .to_string(),
                },
            ]
        );
    }

    #[test]
    fn compaction_inputs_refuse_a_short_or_foreign_decode() {
        // One input line lost: the count no longer matches.
        let short = DECODED.replace(
            "  writer_id=5f0c6a2e-8b1d-4c3a-9e7f-112233445566 writer_epoch=1 writer_seq=8\n",
            "",
        );
        assert_eq!(compaction_inputs("t/0a0b/", RECORD_KEY, &short), None);
        // No count at all.
        let uncounted = DECODED.replace("inputs: 2\n", "");
        assert_eq!(compaction_inputs("t/0a0b/", RECORD_KEY, &uncounted), None);
        // Another tenant, another signal, a level-0 record.
        assert_eq!(compaction_inputs("t/ffff/", RECORD_KEY, DECODED), None);
        let metrics = RECORD_KEY.replace("/l/c/", "/m/c/");
        assert_eq!(compaction_inputs("t/0a0b/", &metrics, DECODED), None);
        let l0 = "t/0a0b/l/c/0002/20261117T16/\
            5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000007.cmt";
        assert_eq!(compaction_inputs("t/0a0b/", l0, DECODED), None);
    }

    #[test]
    fn a_swept_input_is_one_an_earlier_list_held() {
        let inputs = compaction_inputs("t/0a0b/", RECORD_KEY, DECODED).expect("inputs");
        let commit7 = inputs[0].commit.clone();
        let data7 = format!("{}0011223344556677.rseg", inputs[0].data_prefix);
        let commit8 = inputs[1].commit.clone();
        let data8 = format!("{}8899aabbccddeeff.rseg", inputs[1].data_prefix);
        // Seq 70 of the same writer and epoch, which a prefix without the
        // padded seq and its trailing dot would match.
        let seq70 = "t/0a0b/l/l0/0002/\
            5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000070.0011223344556677.rseg"
            .to_string();
        let seen: BTreeSet<String> = [&commit7, &data7, &commit8, &data8, &seq70]
            .into_iter()
            .cloned()
            .collect();
        // Nothing gone yet.
        assert!(swept_inputs(&inputs, &seen, &seen).is_empty());
        // Input 7's record and data object swept; seq 70 swept too, but it
        // is no input.
        let listed: BTreeSet<String> = [&commit8, &data8, &RECORD_KEY.to_string()]
            .into_iter()
            .cloned()
            .collect();
        assert_eq!(
            swept_inputs(&inputs, &seen, &listed),
            vec![commit7.clone(), data7]
        );
        // A key never listed before is no evidence of a delete.
        assert!(swept_inputs(&inputs, &BTreeSet::new(), &listed).is_empty());
    }

    #[test]
    fn two_audits_after_one_deletion_count_it_once() {
        let inputs = compaction_inputs("t/0a0b/", RECORD_KEY, DECODED).expect("inputs");
        let commit7 = inputs[0].commit.clone();
        let commit8 = inputs[1].commit.clone();
        let mut ledger = SweepLedger::default();
        ledger.observe([commit7.clone(), commit8.clone()]);
        // commit7 is deleted; two later audits list the same bucket.
        let listed: BTreeSet<String> = [commit8].into_iter().collect();
        for _ in 0..2 {
            for key in ledger.unconfirmed(&inputs, &listed) {
                ledger.confirm(&key);
            }
            ledger.observe(listed.iter().cloned());
        }
        assert_eq!(ledger.confirmed(), 1);
        assert!(ledger.unconfirmed(&inputs, &listed).is_empty());
        assert!(!ledger.confirm(&commit7));
    }

    #[test]
    fn absent_head_names_nothing() {
        let i = parse_inspect(
            "store: s3\nHEAD at t/0a0b/catalog/l/HEAD is absent or unreadable: not found\n",
        );
        assert!(!i.head_present);
        assert!(i.named.is_empty());
    }

    #[test]
    fn compaction_and_l0_records_are_told_apart() {
        let p = tenant_prefix("t/0a0b/catalog/l/HEAD").expect("prefix");
        assert_eq!(p, "t/0a0b/");
        let listed: Vec<String> = [
            "t/0a0b/l/c/0000/20261117T16/5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000007.cmt",
            "t/0a0b/l/c/0000/20261117T16/l1.00112233aabbccdd.cmt",
            "t/0a0b/l/c/0000/20261117T16/rw.00112233aabbccdd.cmt",
            "t/0a0b/l/c/0000/20261117T16/retire.tmb",
            "t/0a0b/l/l0/0000/5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000007.00112233aabbccdd.rseg",
            "t/0a0b/m/c/0000/20261117T16/l1.00112233aabbccdd.cmt",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            compaction_records(&p, &listed),
            vec!["t/0a0b/l/c/0000/20261117T16/l1.00112233aabbccdd.cmt"]
        );
        assert_eq!(
            l0_commit_records(&p, &listed),
            vec![
                "t/0a0b/l/c/0000/20261117T16/5f0c6a2e-8b1d-4c3a-9e7f-112233445566.1.00000000000000000007.cmt"
            ]
        );
    }
}
