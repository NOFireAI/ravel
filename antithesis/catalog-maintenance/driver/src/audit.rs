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

/// The part key from `catalog inspect`'s `failed to fetch part <key>: ...`
/// error: HEAD names an object the store could not return.
pub fn unfetched_part(stderr: &str) -> Option<String> {
    let rest = &stderr[stderr.find("failed to fetch part ")? + "failed to fetch part ".len()..];
    let key = rest.split([':', ' ', '\n']).next()?;
    (!key.is_empty()).then(|| key.to_string())
}

/// The object key from `rlog footprint`'s `failed to fetch <key>: ...` error:
/// the catalog resolved a data object the store could not return.
pub fn unfetched_object(stderr: &str) -> Option<String> {
    let rest = &stderr[stderr.find("failed to fetch ")? + "failed to fetch ".len()..];
    let key = rest.split([':', ' ', '\n']).next()?;
    (!key.is_empty() && key != "part").then(|| key.to_string())
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
    fn footprint_objects_and_fetch_failures_are_parsed() {
        let json = r#"{"total":{},"objects":[{"key":"t/0a0b/l/d/0/1/a.rlog","level":0},
                       {"key":"t/0a0b/l/d/0/1/b.rlog","level":1}]}"#;
        assert_eq!(
            footprint_keys(json),
            Some(vec![
                "t/0a0b/l/d/0/1/a.rlog".to_string(),
                "t/0a0b/l/d/0/1/b.rlog".to_string()
            ])
        );
        assert_eq!(footprint_keys("object_count: 2"), None);
        assert_eq!(
            unfetched_object("Error: failed to fetch t/0a0b/l/d/0/1/a.rlog: not found\n")
                .as_deref(),
            Some("t/0a0b/l/d/0/1/a.rlog")
        );
        assert_eq!(unfetched_object("Error: failed to fetch part k: x"), None);
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
    fn an_unfetched_part_is_named() {
        let err = "Error: failed to fetch part t/0a0b/catalog/l/snap/1.aa.csnap: not found\n";
        assert_eq!(
            unfetched_part(err).as_deref(),
            Some("t/0a0b/catalog/l/snap/1.aa.csnap")
        );
        assert_eq!(unfetched_part("Error: HEAD at k is corrupt: bad"), None);
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
            "t/0a0b/l/c/0/490001/w1.1.7.cmt",
            "t/0a0b/l/c/0/490001/l1.00112233aabbccdd.cmt",
            "t/0a0b/l/c/0/490001/rw.00112233aabbccdd.cmt",
            "t/0a0b/l/c/0/490001/retire.tmb",
            "t/0a0b/m/c/0/490001/l1.00112233aabbccdd.cmt",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            compaction_records(&p, &listed),
            vec!["t/0a0b/l/c/0/490001/l1.00112233aabbccdd.cmt"]
        );
        assert_eq!(
            l0_commit_records(&p, &listed),
            vec!["t/0a0b/l/c/0/490001/w1.1.7.cmt"]
        );
    }
}
