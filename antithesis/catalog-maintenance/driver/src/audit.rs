//! The store audit: what `ravel-cli catalog inspect` says HEAD names, against
//! what a LIST of the tenant's prefix says exists.
//!
//! The text form of `catalog inspect` prints every key HEAD names as a
//! `key=<object key>` token: each snapshot part, each part's column
//! statistics object and each key-index leaf. It then fetches and decodes
//! every part. No `ravel-cli` command prints a part's entries, the commit
//! records and data objects a part names, so the audit cannot check those;
//! see the README's "Missing decoder" note.

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
            if let Some(key) = token.strip_prefix("key=")
                && !key.is_empty()
                && seen.insert(key.to_string())
            {
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
