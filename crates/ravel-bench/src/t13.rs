//! Shared pieces of the two T13 report-only experiments (issue #3711,
//! ADR-3692 work package 4): [`crate::structural_read`] and
//! [`crate::fulltext_sidecar`]. Both follow the T12 pre-registration standard
//! of [`crate::string_fingerprint`] and reuse its figure and band types.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

pub use crate::string_fingerprint::spec::{Band, Expect};
pub use crate::string_fingerprint::{Checked, Figure, check_figures};
use crate::string_fingerprint_report::{file_digest, git_head};

/// The CPU clock every CPU figure of both arms is read from.
pub const CPU_CLOCK: &str = "CLOCK_THREAD_CPUTIME_ID";

pub fn thread_cpu_s() -> f64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
}

/// `f`'s result and the thread CPU seconds it took.
pub fn cpu_time<T>(f: impl FnOnce() -> T) -> (T, f64) {
    let t = thread_cpu_s();
    let out = std::hint::black_box(f());
    (out, thread_cpu_s() - t)
}

/// Sorts `extents` (`(offset, len)`) and merges every extent that starts at
/// most `gap` bytes past the end of the run before it, overlaps included.
/// A zero-length extent is dropped. This is the structural read's coalescing
/// rule: no run cap and no whole-object fallback.
pub fn coalesce(extents: &[(u64, u64)], gap: u64) -> Vec<(u64, u64)> {
    let mut sorted: Vec<(u64, u64)> = extents.iter().copied().filter(|e| e.1 > 0).collect();
    sorted.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (start, len) in sorted {
        let end = start.saturating_add(len);
        match out.last_mut() {
            Some((s, l)) if start <= (*s + *l).saturating_add(gap) => {
                *l = end.max(*s + *l) - *s;
            }
            _ => out.push((start, len)),
        }
    }
    out
}

/// Row ordinals of one object to `(block, row within block)` and back, from
/// the per-block row counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowMap {
    /// `starts[b]` is block `b`'s first ordinal; one extra entry holds the
    /// object's row count.
    starts: Vec<u64>,
}

impl RowMap {
    pub fn new(counts: &[u32]) -> RowMap {
        let mut starts = Vec::with_capacity(counts.len() + 1);
        let mut at = 0u64;
        starts.push(0);
        for c in counts {
            at += u64::from(*c);
            starts.push(at);
        }
        RowMap { starts }
    }

    pub fn rows(&self) -> u64 {
        self.starts.last().copied().unwrap_or(0)
    }

    pub fn blocks(&self) -> usize {
        self.starts.len().saturating_sub(1)
    }

    pub fn counts(&self) -> Vec<u32> {
        self.starts
            .windows(2)
            .map(|w| (w[1] - w[0]) as u32)
            .collect()
    }

    /// The block holding `ordinal` (the last block whose first ordinal is at
    /// most it, which skips empty blocks) and the row within it.
    pub fn locate(&self, ordinal: u64) -> Option<(u32, u32)> {
        if ordinal >= self.rows() {
            return None;
        }
        let block = self.starts.partition_point(|s| *s <= ordinal) - 1;
        let row = ordinal - self.starts.get(block)?;
        Some((u32::try_from(block).ok()?, u32::try_from(row).ok()?))
    }

    pub fn ordinal(&self, block: u32, row: u32) -> Option<u64> {
        let b = block as usize;
        let start = *self.starts.get(b)?;
        let end = *self.starts.get(b + 1)?;
        let o = start + u64::from(row);
        (o < end).then_some(o)
    }

    /// The stored form: the block count then every block's row count, each
    /// an unsigned LEB128 varint.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        ravel_logseg::varint::put_uvarint(&mut out, self.blocks() as u64);
        for c in self.counts() {
            ravel_logseg::varint::put_uvarint(&mut out, u64::from(c));
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<RowMap, String> {
        let mut pos = 0usize;
        let get = |pos: &mut usize| {
            ravel_logseg::varint::get_uvarint(bytes, pos).map_err(|e| format!("row map: {e}"))
        };
        let n = get(&mut pos)?;
        if n > bytes.len() as u64 {
            return Err(format!("row map: {n} blocks in {} bytes", bytes.len()));
        }
        let mut counts = Vec::with_capacity(n as usize);
        for _ in 0..n {
            counts.push(u32::try_from(get(&mut pos)?).map_err(|_| "row map: count range")?);
        }
        if pos != bytes.len() {
            return Err(format!("row map: {} trailing bytes", bytes.len() - pos));
        }
        Ok(RowMap::new(&counts))
    }
}

/// SplitMix64: the seeded draw both arms use, so a seed names its draw on
/// every platform and toolchain.
#[derive(Clone, Debug)]
pub struct SplitMix(u64);

impl SplitMix {
    pub fn new(seed: u64) -> SplitMix {
        SplitMix(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`; `n` must be nonzero.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// `k` distinct values of `[0, n)`, ascending; all of them when `k >= n`.
    pub fn distinct(&mut self, k: usize, n: u64) -> Vec<u64> {
        if k as u64 >= n {
            return (0..n).collect();
        }
        let mut seen = HashSet::new();
        while seen.len() < k {
            seen.insert(self.below(n));
        }
        let mut out: Vec<u64> = seen.into_iter().collect();
        out.sort_unstable();
        out
    }
}

/// The median of `v` (the lower middle for an even count); `None` when empty.
pub fn median(v: &[f64]) -> Option<f64> {
    let mut s: Vec<f64> = v.to_vec();
    s.sort_by(f64::total_cmp);
    s.get((s.len().saturating_sub(1)) / 2).copied()
}

/// Every reported figure must be pre-registered and every pre-registered
/// figure named exactly; `name` prefixes the error.
pub fn validate_figure_names(
    name: &str,
    figures: &BTreeMap<String, Expect>,
    want: &[String],
) -> Result<(), String> {
    let want: HashSet<&str> = want.iter().map(String::as_str).collect();
    let have: HashSet<&str> = figures.keys().map(String::as_str).collect();
    let mut missing: Vec<&str> = want.difference(&have).copied().collect();
    let mut unknown: Vec<&str> = have.difference(&want).copied().collect();
    if missing.is_empty() && unknown.is_empty() {
        return Ok(());
    }
    missing.sort_unstable();
    unknown.sort_unstable();
    Err(format!(
        "{name}: figures must name every reported figure exactly: missing {missing:?}, unknown {unknown:?}"
    ))
}

/// The run stamp both arms write.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Stamp {
    pub git: String,
    pub arch: String,
    pub nproc: usize,
    pub mem_total_kib: u64,
    pub corpus_file: String,
    pub corpus_sha256: String,
    pub corpus_bytes: u64,
    pub cpu_clock: String,
    /// `debug` or `release`, from `debug_assertions`.
    pub profile: String,
}

impl Stamp {
    pub fn line(&self, arm: &str) -> String {
        format!(
            "{arm} stamp git={} arch={} nproc={} mem_total_kib={} corpus_file={} corpus_sha256={} \
             corpus_bytes={} cpu_clock={} profile={}",
            self.git,
            self.arch,
            self.nproc,
            self.mem_total_kib,
            self.corpus_file,
            self.corpus_sha256,
            self.corpus_bytes,
            self.cpu_clock,
            self.profile
        )
    }
}

fn command_out(cmd: &str, args: &[&str]) -> String {
    std::process::Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown".to_string(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_string(),
        )
}

fn mem_total_kib() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|m| {
            m.lines()
                .find_map(|l| l.strip_prefix("MemTotal:"))
                .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        })
        .unwrap_or(0)
}

/// Stamps the run and refuses a corpus whose sha256 is not the
/// pre-registered one, before any of it is read as input.
pub fn stamp_and_check_corpus(corpus: &Path, registered_sha256: &str) -> Result<Stamp, String> {
    if !corpus.is_file() {
        return Err(format!("corpus {} is not a readable file", corpus.display()));
    }
    let (corpus_sha256, corpus_bytes) = file_digest(corpus)?;
    if corpus_sha256 != registered_sha256 {
        return Err(format!(
            "refusing corpus {}: sha256 {corpus_sha256} is not the pre-registered {registered_sha256}",
            corpus.display()
        ));
    }
    Ok(Stamp {
        git: git_head(),
        arch: command_out("uname", &["-m"]),
        nproc: std::thread::available_parallelism().map_or(0, |n| n.get()),
        mem_total_kib: mem_total_kib(),
        corpus_file: corpus
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned()),
        corpus_sha256,
        corpus_bytes,
        cpu_clock: CPU_CLOCK.to_string(),
        profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
        .to_string(),
    })
}

/// Refuses a command-line parameter that differs from its pre-registered
/// value. `given` holds `(name, given, registered)` rendered as text.
pub fn refuse_drift(subject: &str, given: &[(&str, Option<String>, String)]) -> Result<(), String> {
    for (name, g, registered) in given {
        if let Some(g) = g
            && g != registered
        {
            return Err(format!(
                "subject {subject}: refusing --{name} {g}: the pre-registered value is {registered}"
            ));
        }
    }
    Ok(())
}

/// One subject's decision, as written into its report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub subject: String,
    pub go: bool,
    /// For an arm with competing variants, the variants that passed every
    /// criterion on this subject; empty otherwise.
    #[serde(default)]
    pub variants: Vec<String>,
    pub line: String,
}

/// A written report's decision and how many problems and misses it carried.
#[derive(Clone, Debug, PartialEq)]
pub struct SavedDecision {
    pub decision: Decision,
    pub problems: usize,
    pub misses: usize,
}

pub fn saved_decision(json: &str) -> Result<SavedDecision, String> {
    #[derive(Deserialize)]
    struct Saved {
        decision: Decision,
        checked: Checked,
    }
    let saved: Saved = serde_json::from_str(json).map_err(|e| format!("report: {e}"))?;
    Ok(SavedDecision {
        decision: saved.decision,
        problems: saved.checked.problems.len(),
        misses: saved.checked.misses.len(),
    })
}

/// Whether `saved` is a decision over `subjects`: every subject reported
/// exactly once without a problem, and nothing else reported.
pub fn complete(subjects: &[String], saved: &[SavedDecision]) -> bool {
    !subjects.is_empty()
        && subjects.iter().all(|s| {
            let mine: Vec<&SavedDecision> =
                saved.iter().filter(|d| &d.decision.subject == s).collect();
            matches!(mine.as_slice(), [d] if d.problems == 0)
        })
        && saved.iter().all(|d| subjects.contains(&d.decision.subject))
}

/// First record timestamp and the step between consecutive lines: line `i`
/// of a subject is written at `TS0_NS + i * TS_STEP_NS`.
pub const TS0_NS: i64 = 1_000_000_000;
pub const TS_STEP_NS: i64 = 1_000_000;
/// The catalog's `now` for every resolve; later than any line's timestamp
/// for up to 14.4 million lines.
pub const NOW_NS: i64 = 4 * 3600 * 1_000_000_000;
const SCOPE_NAME: &str = "t13-bench";
const SCOPE_VERSION: &str = "1.0";

/// The timestamp line `ordinal` is written at.
pub fn line_ts(ordinal: u64) -> i64 {
    TS0_NS + (ordinal as i64) * TS_STEP_NS
}

/// The body a corpus line is written as.
pub fn line_body(line: &[u8]) -> String {
    String::from_utf8_lossy(line).into_owned()
}

/// The identity both arms write their objects under.
pub fn identity(tenant_hash: &ravel_types::TenantHash, writer_seq: u64) -> ravel_logseg::ObjectIdentity {
    ravel_logseg::ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: *uuid::Uuid::from_u128(0x7313_0000 + u128::from(writer_seq)).as_bytes(),
        writer_epoch: 1,
        writer_seq,
    }
}

/// The records of `lines`, one stream, severity INFO, line `first + i` at
/// [`line_ts`].
pub fn line_records(lines: &[Vec<u8>], first: u64) -> Vec<ravel_logseg::LogRecord> {
    let res = vec![(
        "service.name".to_string(),
        ravel_logseg::AttrValue::Str("t13".to_string()),
    )];
    let stream_id = ravel_types::logstream::log_stream_id(&res, SCOPE_NAME, SCOPE_VERSION, &[]);
    let stream_attrs = ravel_logseg::stream_attrs_bytes(&res, SCOPE_NAME, SCOPE_VERSION, &[]);
    lines
        .iter()
        .enumerate()
        .map(|(i, l)| {
            let ts = line_ts(first + i as u64);
            ravel_logseg::LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: line_body(l),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            }
        })
        .collect()
}

/// One RLOG object of `records` from the production writer.
pub fn write_rlog(
    records: Vec<ravel_logseg::LogRecord>,
    cfg: ravel_logseg::RlogConfig,
    identity: ravel_logseg::ObjectIdentity,
) -> Result<Vec<u8>, String> {
    let mut w = ravel_logseg::RlogWriter::new(cfg, identity);
    for r in records {
        w.push(r).map_err(|e| format!("rlog push: {e}"))?;
    }
    w.finish().map_err(|e| format!("rlog finish: {e}"))
}

/// Every block's records of `bytes` under `columns`, in block order, through
/// the public reader with a match-all predicate.
pub fn decode_all(
    bytes: &[u8],
    columns: &ravel_logseg::ColumnSelection,
) -> Result<Vec<Vec<ravel_logseg::LogRecord>>, String> {
    let cfg = ravel_logseg::RlogConfig::default();
    let reader = ravel_logseg::RlogReader::new(bytes, &cfg).map_err(|e| format!("open: {e}"))?;
    let mut scan = reader
        .scan_blocks(&ravel_logseg::Predicate::And(vec![]), &[], columns)
        .map_err(|e| format!("scan: {e}"))?;
    let mut out = Vec::new();
    while let Some(b) = scan.next_block(bytes).map_err(|e| format!("decode: {e}"))? {
        out.push(b);
    }
    Ok(out)
}

/// Ranged GETs of one object, counted by the store that wraps them.
pub struct Io {
    pub store: std::sync::Arc<dyn ravel_object_store::ObjectStoreBackend>,
    pub key: String,
    pub concurrency: usize,
}

impl Io {
    pub async fn get(&self, range: ravel_object_store::GetRange) -> Result<bytes::Bytes, String> {
        self.store
            .get(&self.key, range)
            .await
            .map(|o| o.data)
            .map_err(|e| format!("get {}: {e}", self.key))
    }

    /// One GET per `(offset, len)`, at most `concurrency` in flight, in
    /// batches; the bytes come back with their offsets.
    pub async fn get_ranges(&self, ranges: &[(u64, u64)]) -> Result<Vec<(u64, bytes::Bytes)>, String> {
        let mut out = Vec::with_capacity(ranges.len());
        for batch in ranges.chunks(self.concurrency.max(1)) {
            let mut set = tokio::task::JoinSet::new();
            for &(start, len) in batch {
                let store = std::sync::Arc::clone(&self.store);
                let key = self.key.clone();
                set.spawn(async move {
                    store
                        .get(&key, ravel_object_store::GetRange::Range(start, start + len))
                        .await
                        .map(|o| (start, o.data))
                        .map_err(|e| format!("get {key} [{start}, +{len}): {e}"))
                });
            }
            while let Some(r) = set.join_next().await {
                out.push(r.map_err(|e| format!("get task: {e}"))??);
            }
        }
        out.sort_by_key(|(s, _)| *s);
        Ok(out)
    }
}

/// A subject's written report.
#[derive(Clone, Debug, Serialize)]
pub struct Report<P: Serialize> {
    pub arm: String,
    pub subject: String,
    pub stamp: Stamp,
    pub params: P,
    pub figures: Vec<Figure>,
    pub checked: Checked,
    pub decision: Decision,
    pub notes: Vec<String>,
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Plausible wrong coalescers, each kept so the exact-figure test shows
    /// it failing.
    fn coalesce_strict(extents: &[(u64, u64)], gap: u64) -> Vec<(u64, u64)> {
        let mut sorted = extents.to_vec();
        sorted.sort_unstable();
        let mut out: Vec<(u64, u64)> = Vec::new();
        for (s, l) in sorted {
            match out.last_mut() {
                Some((a, b)) if s < *a + *b + gap => *b = (s + l).max(*a + *b) - *a,
                _ => out.push((s, l)),
            }
        }
        out
    }

    fn coalesce_unsorted(extents: &[(u64, u64)], gap: u64) -> Vec<(u64, u64)> {
        let mut out: Vec<(u64, u64)> = Vec::new();
        for &(s, l) in extents {
            match out.last_mut() {
                Some((a, b)) if s <= *a + *b + gap && s >= *a => {
                    *b = (s + l).max(*a + *b) - *a;
                }
                _ => out.push((s, l)),
            }
        }
        out
    }

    fn coalesce_no_max(extents: &[(u64, u64)], gap: u64) -> Vec<(u64, u64)> {
        let mut sorted = extents.to_vec();
        sorted.sort_unstable();
        let mut out: Vec<(u64, u64)> = Vec::new();
        for (s, l) in sorted {
            match out.last_mut() {
                Some((a, b)) if s <= *a + *b + gap => *b = s + l - *a,
                _ => out.push((s, l)),
            }
        }
        out
    }

    #[test]
    fn coalesce_merges_within_the_gap_inclusive_sorted_and_through_overlaps() {
        let extents = [(300, 1), (15, 5), (0, 10), (100, 20), (105, 3), (50, 0)];
        let want = vec![(0, 20), (100, 20), (300, 1)];
        assert_eq!(coalesce(&extents, 5), want);
        // A gap one byte short of 5 leaves 0..10 and 15..20 apart.
        assert_eq!(coalesce(&extents, 4), vec![(0, 10), (15, 5), (100, 20), (300, 1)]);
        assert_ne!(coalesce_strict(&extents, 5), want);
        assert_ne!(coalesce_unsorted(&extents, 5), want);
        assert_ne!(coalesce_no_max(&extents, 5), want);
    }

    fn locate_off_by_one(counts: &[u32], ordinal: u64) -> Option<(u32, u32)> {
        let mut at = 0u64;
        for (b, c) in counts.iter().enumerate() {
            if ordinal <= at + u64::from(*c) && *c > 0 {
                return Some((b as u32, (ordinal - at) as u32));
            }
            at += u64::from(*c);
        }
        None
    }

    #[test]
    fn row_map_round_trips_and_skips_empty_blocks() {
        let map = RowMap::new(&[3, 2, 0, 4]);
        assert_eq!(map.rows(), 9);
        assert_eq!(map.blocks(), 4);
        let want = [
            (0, 0),
            (0, 1),
            (0, 2),
            (1, 0),
            (1, 1),
            (3, 0),
            (3, 1),
            (3, 2),
            (3, 3),
        ];
        for (o, w) in want.iter().enumerate() {
            assert_eq!(map.locate(o as u64), Some(*w), "ordinal {o}");
            assert_eq!(map.ordinal(w.0, w.1), Some(o as u64));
        }
        assert_eq!(map.locate(9), None);
        assert_eq!(map.ordinal(2, 0), None);
        assert_eq!(map.ordinal(1, 2), None);
        assert_eq!(RowMap::decode(&map.encode()).expect("decode"), map);
        assert_eq!(map.encode(), vec![4, 3, 2, 0, 4]);
        assert!(RowMap::decode(&[4, 3, 2, 0, 4, 9]).is_err());
        // A boundary-inclusive scan puts ordinal 3 in block 0.
        assert_ne!(locate_off_by_one(&[3, 2, 0, 4], 3), Some((1, 0)));
    }

    #[test]
    fn splitmix_draw_is_pinned() {
        // Reference values of SplitMix64 from seed 0.
        let mut r = SplitMix::new(0);
        assert_eq!(r.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(r.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(SplitMix::new(7).distinct(3, 3), vec![0, 1, 2]);
        let d = SplitMix::new(7).distinct(4, 100);
        assert_eq!(d.len(), 4);
        assert!(d.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn median_is_the_lower_middle() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), Some(2.0));
        assert_eq!(median(&[]), None);
    }

    #[test]
    fn drift_is_refused_and_agreement_passes() {
        let ok = refuse_drift("s", &[("seed", Some("3".into()), "3".into())]);
        assert!(ok.is_ok());
        let none = refuse_drift("s", &[("seed", None, "3".into())]);
        assert!(none.is_ok());
        let err = refuse_drift("s", &[("seed", Some("4".into()), "3".into())]).unwrap_err();
        assert!(err.contains("refusing --seed 4"), "{err}");
    }

    #[test]
    fn complete_needs_every_subject_once_without_a_problem() {
        let d = |s: &str, problems: usize| SavedDecision {
            decision: Decision {
                subject: s.into(),
                go: true,
                variants: vec![],
                line: String::new(),
            },
            problems,
            misses: 0,
        };
        let subjects = vec!["a".to_string(), "b".to_string()];
        assert!(complete(&subjects, &[d("a", 0), d("b", 0)]));
        assert!(!complete(&subjects, &[d("a", 0)]));
        assert!(!complete(&subjects, &[d("a", 0), d("b", 1)]));
        assert!(!complete(&subjects, &[d("a", 0), d("b", 0), d("b", 0)]));
        assert!(!complete(&subjects, &[d("a", 0), d("b", 0), d("c", 0)]));
    }
}
