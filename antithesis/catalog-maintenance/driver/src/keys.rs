//! The commit-record keys the forced races pick buckets by and the proxy
//! matches requests against (`ravel_commit::keys`): a bucket's records sit
//! under `t/<tenant_hash>/<signal>/c/<shard:04>/<YYYYMMDDTHH>/`, an L0 commit
//! record is `<writer>.<epoch>.<seq>.cmt`, a compaction record
//! `l1.<input_set_hash16>.cmt` and a rewrite record `rw.<...>.cmt`.

/// The `YYYYMMDDTHH` UTC text of a unix hour, as `ingest_hour_string`
/// writes it into commit keys.
pub fn hour_string(unix_hour: u64) -> String {
    let days = unix_hour / 24;
    let hour = unix_hour % 24;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}{m:02}{d:02}T{hour:02}")
}

/// The unix hour of a `YYYYMMDDTHH` text; `None` when it is not one.
pub fn parse_hour_string(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.len() != 11 || b[8] != b'T' {
        return None;
    }
    let digits = |r: std::ops::Range<usize>| -> Option<u64> {
        let part = s.get(r)?;
        if !part.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        part.parse().ok()
    };
    let (y, m, d, h) = (digits(0..4)?, digits(4..6)?, digits(6..8)?, digits(9..11)?);
    if !(1970..=9999).contains(&y) || !(1..=12).contains(&m) || d == 0 || h > 23 {
        return None;
    }
    let days = days_from_civil(y, m, d);
    // A day past its month's end would name another date.
    (civil_from_days(days) == (y, m, d)).then_some(days * 24 + h)
}

/// Howard Hinnant's `days_from_civil`, for dates from 1970 on.
fn days_from_civil(y: u64, m: u64, d: u64) -> u64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe).saturating_sub(719_468)
}

/// Howard Hinnant's `civil_from_days`, for day counts from the epoch on.
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Where a compaction record sits.
#[derive(Debug, PartialEq, Eq)]
pub struct RecordKey {
    /// `t/<tenant_hash>/`.
    pub tenant_prefix: String,
    /// The signal directory: `l` for logs.
    pub signal: String,
    pub shard: u32,
    pub hour: u64,
}

/// The parts of a compaction record key
/// `t/<tenant_hash>/<signal>/c/<shard:04>/<YYYYMMDDTHH>/l1.<hash16>.cmt`;
/// `None` for any other key.
pub fn parse_record_key(key: &str) -> Option<RecordKey> {
    let parts: Vec<&str> = key.split('/').collect();
    let [t, tenant, signal, c, shard, hour, name] = parts.as_slice() else {
        return None;
    };
    if *t != "t" || *c != "c" || tenant.is_empty() || signal.is_empty() {
        return None;
    }
    if shard.len() != 4 || !shard.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hash = name.strip_prefix("l1.")?.strip_suffix(".cmt")?;
    if hash.len() != 16 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(RecordKey {
        tenant_prefix: format!("t/{tenant}/"),
        signal: (*signal).to_string(),
        shard: shard.parse().ok()?,
        hour: parse_hour_string(hour)?,
    })
}

/// Where a level-0 logs commit record sits, and the flush identity it names.
#[derive(Debug, PartialEq, Eq)]
pub struct L0Key {
    /// `t/<tenant_hash>/`.
    pub tenant_prefix: String,
    pub shard: u32,
    pub hour: u64,
    /// `<writer>.<epoch>.<seq:020>`, shared with the record's data object.
    pub stem: String,
}

impl L0Key {
    /// `<tenant_prefix>l/l0/<shard:04>/<stem>.`: the data object of this flush
    /// is the one key under it ending in `.rseg` (`ravel_commit::keys::data_key`
    /// appends the content hash before the suffix).
    pub fn data_prefix(&self) -> String {
        format!(
            "{}l/l0/{:04}/{}.",
            self.tenant_prefix, self.shard, self.stem
        )
    }
}

/// The parts of a level-0 logs commit record key
/// `t/<tenant_hash>/l/c/<shard:04>/<YYYYMMDDTHH>/<uuid>.<epoch>.<seq:020>.cmt`
/// (`ravel_commit::keys::commit_key`); `None` for any other key, a compaction
/// or rewrite record and every other signal's record included. A metrics
/// shard records the commit keys it abandons, so `capture-commit` must never
/// take one of those.
pub fn parse_l0_commit_key(key: &str) -> Option<L0Key> {
    let parts: Vec<&str> = key.split('/').collect();
    let [t, tenant, signal, c, shard, hour, name] = parts.as_slice() else {
        return None;
    };
    if *t != "t" || *signal != "l" || *c != "c" || tenant.is_empty() {
        return None;
    }
    if shard.len() != 4 || !shard.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let stem = name.strip_suffix(".cmt")?;
    let [writer, epoch, seq] = stem.split('.').collect::<Vec<_>>()[..] else {
        return None;
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !is_uuid(writer) || !digits(epoch) || seq.len() != 20 || !digits(seq) {
        return None;
    }
    Some(L0Key {
        tenant_prefix: format!("t/{tenant}/"),
        shard: shard.parse().ok()?,
        hour: parse_hour_string(hour)?,
        stem: stem.to_string(),
    })
}

/// A hyphenated UUID: hex groups of 8, 4, 4, 4 and 12.
fn is_uuid(s: &str) -> bool {
    let groups: Vec<&str> = s.split('-').collect();
    groups.len() == 5
        && groups
            .iter()
            .zip([8, 4, 4, 4, 12])
            .all(|(g, n)| g.len() == n && g.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// `<tenant_prefix>l/c/<shard:04>/<YYYYMMDDTHH>/`: one logs bucket's records.
pub fn bucket_prefix(tenant_prefix: &str, shard: u32, hour: u64) -> String {
    format!("{tenant_prefix}l/c/{shard:04}/{}/", hour_string(hour))
}

/// One logs bucket, as a LIST of its prefix shows it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Bucket {
    /// L0 commit records.
    pub l0: usize,
    /// Its compaction records.
    pub records: Vec<String>,
    /// Whether it holds anything but L0 and compaction records: a rewrite
    /// record or a retention tombstone.
    pub other: bool,
}

impl Bucket {
    /// Whether a compaction of the bucket, once sealed, publishes a record:
    /// two inputs at least (`DEFAULT_MIN_COMPACTION_INPUTS`), no record yet,
    /// and nothing that would make it skip or rewrite instead.
    pub fn compactable(&self) -> bool {
        self.l0 >= 2 && self.records.is_empty() && !self.other
    }
}

/// The bucket under `prefix` (a [`bucket_prefix`]) from listed keys.
pub fn bucket<'a>(prefix: &str, listed: impl IntoIterator<Item = &'a String>) -> Bucket {
    let mut b = Bucket::default();
    for key in listed {
        let Some(name) = key.strip_prefix(prefix) else {
            continue;
        };
        if name.contains('/') {
            continue;
        }
        if name.starts_with("l1.") && name.ends_with(".cmt") {
            b.records.push(key.clone());
        } else if name.ends_with(".cmt") && !name.starts_with("rw.") {
            b.l0 += 1;
        } else {
            b.other = true;
        }
    }
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hour_strings_match_the_commit_keys() {
        // ravel-commit's own pins (ingest_hour_string_* in keys.rs).
        assert_eq!(hour_string(0), "19700101T00");
        assert_eq!(hour_string(23), "19700101T23");
        assert_eq!(hour_string(24), "19700102T00");
        assert_eq!(hour_string(30 * 24 + 24), "19700201T00");
        assert_eq!(hour_string(364 * 24 + 24), "19710101T00");
        // 2026-11-17T16:00Z, the hour in the s3.rs listing fixture.
        assert_eq!(hour_string(1_794_931_200 / 3600), "20261117T16");
        // A leap day.
        assert_eq!(hour_string(1_709_164_800 / 3600 + 5), "20240229T05");
    }

    #[test]
    fn hour_strings_round_trip() {
        for h in [0, 1, 23, 24, 8_760, 495_734, 498_592, 1_000_000] {
            assert_eq!(parse_hour_string(&hour_string(h)), Some(h), "{h}");
        }
        for bad in [
            "",
            "20261117 16",
            "20261117T24",
            "20261317T16",
            "20260230T00",
            "2026111T016",
            "+0261117T16",
            "20261117T1x",
        ] {
            assert_eq!(parse_hour_string(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn only_compaction_record_keys_parse() {
        let key = "t/0a0b/l/c/0003/20261117T16/l1.0011223344556677.cmt";
        assert_eq!(
            parse_record_key(key),
            Some(RecordKey {
                tenant_prefix: "t/0a0b/".to_string(),
                signal: "l".to_string(),
                shard: 3,
                hour: 1_794_931_200 / 3600,
            })
        );
        for other in [
            "t/0a0b/l/c/0003/20261117T16/w1.1.7.cmt",
            "t/0a0b/l/c/0003/20261117T16/rw.0011223344556677.cmt",
            "t/0a0b/l/c/0003/20261117T16/l1.00112233.cmt",
            "t/0a0b/l/c/3/20261117T16/l1.0011223344556677.cmt",
            "t/0a0b/l/c/0003/2026111716/l1.0011223344556677.cmt",
            "t/0a0b/catalog/logs/HEAD",
            "x/0a0b/l/c/0003/20261117T16/l1.0011223344556677.cmt",
            "t/0a0b/l/c/0003/20261117T16/l1.0011223344556677.cmt/x",
        ] {
            assert_eq!(parse_record_key(other), None, "{other}");
        }
    }

    #[test]
    fn only_level_0_logs_commit_record_keys_parse_as_l0() {
        let w = "5f0c6a2e-8b1d-4c3a-9e7f-112233445566";
        let key = format!("t/0a0b/l/c/0003/20261117T16/{w}.1794931200.00000000000000000007.cmt");
        let parsed = parse_l0_commit_key(&key).expect("an L0 key");
        assert_eq!(
            parsed,
            L0Key {
                tenant_prefix: "t/0a0b/".to_string(),
                shard: 3,
                hour: 1_794_931_200 / 3600,
                stem: format!("{w}.1794931200.00000000000000000007"),
            }
        );
        assert_eq!(
            parsed.data_prefix(),
            format!("t/0a0b/l/l0/0003/{w}.1794931200.00000000000000000007.")
        );
        for other in [
            "t/0a0b/l/c/0003/20261117T16/l1.0011223344556677.cmt".to_string(),
            "t/0a0b/l/c/0003/20261117T16/rw.0011223344556677.cmt".to_string(),
            format!("t/0a0b/m/c/0003/20261117T16/{w}.1.00000000000000000007.cmt"),
            format!("t/0a0b/l/c/3/20261117T16/{w}.1.00000000000000000007.cmt"),
            format!("t/0a0b/l/c/0003/2026111716/{w}.1.00000000000000000007.cmt"),
            format!("t/0a0b/l/c/0003/20261117T16/{w}.1.7.cmt"),
            format!("t/0a0b/l/c/0003/20261117T16/{w}.x.00000000000000000007.cmt"),
            format!("t/0a0b/l/c/0003/20261117T16/{w}.1.00000000000000000007.rseg"),
            "t/0a0b/l/c/0003/20261117T16/w1.1.00000000000000000007.cmt".to_string(),
            format!("t/0a0b/l/l0/0003/{w}.1.00000000000000000007.0011223344556677.rseg"),
            format!("x/0a0b/l/c/0003/20261117T16/{w}.1.00000000000000000007.cmt"),
        ] {
            assert_eq!(parse_l0_commit_key(&other), None, "{other}");
        }
    }

    #[test]
    fn a_bucket_is_counted_from_its_listing() {
        let hour = 1_794_931_200 / 3600;
        let prefix = bucket_prefix("t/0a0b/", 2, hour);
        assert_eq!(prefix, "t/0a0b/l/c/0002/20261117T16/");
        let key = |name: &str| format!("{prefix}{name}");
        let mut listed = vec![
            key("w1.1.1.cmt"),
            key("w1.1.2.cmt"),
            "t/0a0b/l/c/0002/20261117T17/w1.1.3.cmt".to_string(),
            "t/0a0b/l/c/0001/20261117T16/w1.1.4.cmt".to_string(),
        ];
        let b = bucket(&prefix, &listed);
        assert_eq!(b.l0, 2);
        assert!(b.compactable());
        listed.push(key("l1.0011223344556677.cmt"));
        let b = bucket(&prefix, &listed);
        assert_eq!(b.records, vec![key("l1.0011223344556677.cmt")]);
        assert!(!b.compactable());
        let tomb = vec![key("w1.1.1.cmt"), key("w1.1.2.cmt"), key("retire.tmb")];
        assert!(!bucket(&prefix, &tomb).compactable());
        assert!(!bucket(&prefix, &[key("w1.1.1.cmt")]).compactable());
    }
}
