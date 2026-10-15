//! Startup format-window check (ADR-2708 decision D5c, issue #2724).
//!
//! Reports catalog entries whose `segment_format_version` is outside this
//! build's reader window, so a format crossing shows up at startup instead of
//! at the first query that touches an old object. No reader window is widened
//! here (ADR-0531); the check only counts.
//!
//! For each tenant and each signal in [`CHECKED_SIGNALS`] it GETs the catalog
//! HEAD and then every snapshot part that HEAD names, and nothing else: one
//! logical read when there is no HEAD, otherwise `1 + parts` (the store may
//! split a large part into ranged requests), and no LIST, no HEAD request and
//! no write. A part that answers `NotFound` re-reads the HEAD and its parts
//! once, since a fold may have swept it in between. Up to
//! [`CHECK_CONCURRENCY`] pairs are read at once, and the whole run stops at a
//! deadline, after which the unfinished pairs are reported as not checked.
//! Commit records above the HEAD watermark (the unsealed hours a fold has not
//! taken in yet) are not read, because finding them needs a LIST of the
//! commit prefix.
//!
//! Advisory: the version is the writer's stamp in the catalog entry, not the
//! object's own trailer, so a clean report is not a proof that every object is
//! readable. The reader still checks each object's trailer on open.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::stream::{self, StreamExt};
use ravel_catalog::{PartLimits, decode_head, decode_part};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_types::{Signal, TenantHash};

use crate::config::FormatWindowCheck;

/// The signals the check reads, in report order. Metrics is RSEG; logs,
/// alerts and audit are RLOG; spans is RSPAN.
pub const CHECKED_SIGNALS: [Signal; 5] = [
    Signal::Metrics,
    Signal::Logs,
    Signal::Spans,
    Signal::Alerts,
    Signal::Audit,
];

/// The on-object format a signal's segments are written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Rseg,
    Rlog,
    Rspan,
}

fn format_of(signal: Signal) -> Option<Format> {
    match signal {
        Signal::Metrics => Some(Format::Rseg),
        Signal::Logs | Signal::Alerts | Signal::Audit => Some(Format::Rlog),
        Signal::Spans => Some(Format::Rspan),
        Signal::Profiles => None,
    }
}

/// Lower-case signal name used in log lines and the refusal message.
pub fn signal_name(signal: Signal) -> &'static str {
    match signal {
        Signal::Metrics => "metrics",
        Signal::Logs => "logs",
        Signal::Spans => "spans",
        Signal::Profiles => "profiles",
        Signal::Alerts => "alerts",
        Signal::Audit => "audit",
    }
}

/// Whether `version` is inside this build's reader window for `signal`'s
/// format, read from the reader's own window constant. A value that does not
/// fit a trailer's `u16` is outside every window, the same rule the SQL logs
/// scan applies before it opens an object. A signal with no checked format
/// (profiles) has no window and reads as outside it.
pub fn in_reader_window(signal: Signal, version: u32) -> bool {
    let Ok(version) = u16::try_from(version) else {
        return false;
    };
    match format_of(signal) {
        Some(Format::Rseg) => ravel_segment::SegmentVersion::from_number(version).is_some(),
        Some(Format::Rlog) => ravel_logseg::footer::SUPPORTED_VERSIONS.contains(version),
        Some(Format::Rspan) => ravel_rspan::footer::SUPPORTED_VERSIONS.contains(version),
        None => false,
    }
}

/// The reader window for `signal`'s format, as `oldest..=newest`.
pub fn reader_window_label(signal: Signal) -> String {
    match format_of(signal) {
        Some(Format::Rseg) => {
            let window = ravel_segment::SegmentVersion::WINDOW;
            let newest = window.first().map_or(0, |v| v.number());
            let oldest = window.last().map_or(0, |v| v.number());
            format!("RSEG {oldest}..={newest}")
        }
        Some(Format::Rlog) => {
            let w = ravel_logseg::footer::SUPPORTED_VERSIONS;
            format!("RLOG {}..={}", w.oldest(), w.newest())
        }
        Some(Format::Rspan) => {
            let w = ravel_rspan::footer::SUPPORTED_VERSIONS;
            format!("RSPAN {}..={}", w.oldest(), w.newest())
        }
        None => "none".to_string(),
    }
}

/// One (signal, version) pair outside the reader window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutOfWindow {
    pub signal: Signal,
    pub version: u32,
    /// Catalog entries carrying this pair, summed over every tenant checked.
    pub entries: u64,
    /// Tenants with at least one such entry.
    pub tenants: u64,
}

/// One (tenant, signal) whose catalog could not be read, so its versions are
/// unknown rather than in window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unchecked {
    pub tenant: TenantHash,
    pub signal: Signal,
    pub reason: String,
}

/// What one run of [`check`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FormatWindowReport {
    /// Tenants the check was given.
    pub tenants: usize,
    /// Out-of-window pairs in [`CHECKED_SIGNALS`] order, then by version.
    pub out_of_window: Vec<OutOfWindow>,
    /// Pairs the check could not read.
    pub unchecked: Vec<Unchecked>,
    /// GET requests issued, failed ones included.
    pub get_requests: u64,
    /// The deadline, when it elapsed before every pair was read. The pairs
    /// not read are in [`FormatWindowReport::unchecked`].
    pub deadline_elapsed: Option<Duration>,
}

impl FormatWindowReport {
    /// True only when every pair was read and none was out of window.
    pub fn is_clean(&self) -> bool {
        self.out_of_window.is_empty() && self.unchecked.is_empty()
    }
}

/// How many (tenant, signal) pairs the check reads at once.
pub const CHECK_CONCURRENCY: usize = 8;

#[derive(Default)]
struct Tally {
    entries: u64,
    tenants: u64,
}

/// The tenants startup hands [`check`]: the static fold tenants and every
/// `--alert-rules-file` tenant, sorted and without duplicates.
pub fn tenants_to_check(
    fold_tenants: &[TenantHash],
    alert_rule_tenants: &[TenantHash],
) -> Vec<TenantHash> {
    let mut tenants: Vec<TenantHash> = fold_tenants
        .iter()
        .chain(alert_rule_tenants)
        .copied()
        .collect();
    tenants.sort_by_key(|tenant| tenant.0);
    tenants.dedup();
    tenants
}

/// Read each tenant's HEAD and snapshot parts for every signal in
/// [`CHECKED_SIGNALS`] and count the entries outside the reader window, with
/// at most [`CHECK_CONCURRENCY`] pairs in flight. Never fails as a whole: a
/// read or decode error lands in [`FormatWindowReport::unchecked`] for that
/// (tenant, signal), and so does every pair not finished when `deadline`
/// elapses.
pub async fn check(
    store: &dyn ObjectStoreBackend,
    tenants: &[TenantHash],
    deadline: Duration,
) -> FormatWindowReport {
    let get_requests = AtomicU64::new(0);
    let pairs: Vec<(TenantHash, usize)> = tenants
        .iter()
        .flat_map(|tenant| (0..CHECKED_SIGNALS.len()).map(move |index| (*tenant, index)))
        .collect();
    let mut results = stream::iter(pairs.iter().copied().enumerate())
        .map(|(pair_index, (tenant, signal_index))| {
            let get_requests = &get_requests;
            async move {
                let signal = CHECKED_SIGNALS[signal_index];
                let outcome = check_pair(store, &tenant, signal, get_requests).await;
                (pair_index, outcome)
            }
        })
        .buffer_unordered(CHECK_CONCURRENCY);

    let mut outcomes: Vec<Option<PairOutcome>> = vec![None; pairs.len()];
    let timer = tokio::time::sleep(deadline);
    tokio::pin!(timer);
    let mut deadline_elapsed = false;
    loop {
        // Results first, so a run whose last pair finished in the same
        // instant the timer fired is not reported as cut short.
        tokio::select! {
            biased;
            next = results.next() => match next {
                Some((pair_index, outcome)) => outcomes[pair_index] = Some(outcome),
                None => break,
            },
            () = &mut timer => {
                deadline_elapsed = true;
                break;
            }
        }
    }
    drop(results);

    let mut report = FormatWindowReport {
        tenants: tenants.len(),
        deadline_elapsed: deadline_elapsed.then_some(deadline),
        ..FormatWindowReport::default()
    };
    let mut tallies: BTreeMap<(usize, u32), Tally> = BTreeMap::new();
    for ((tenant, signal_index), outcome) in pairs.into_iter().zip(outcomes) {
        let signal = CHECKED_SIGNALS[signal_index];
        let Some(outcome) = outcome else {
            report.unchecked.push(Unchecked {
                tenant,
                signal,
                reason: format!(
                    "not checked: --format-window-check-timeout ({}) elapsed first",
                    humantime::format_duration(deadline)
                ),
            });
            continue;
        };
        if let Err(reason) = outcome.result {
            report.unchecked.push(Unchecked {
                tenant,
                signal,
                reason,
            });
        }
        for (version, entries) in outcome.found {
            let tally = tallies.entry((signal_index, version)).or_default();
            tally.entries += entries;
            tally.tenants += 1;
        }
    }
    report.out_of_window = tallies
        .into_iter()
        .map(|((signal_index, version), tally)| OutOfWindow {
            signal: CHECKED_SIGNALS[signal_index],
            version,
            entries: tally.entries,
            tenants: tally.tenants,
        })
        .collect();
    report.get_requests = get_requests.load(Ordering::Relaxed);
    report
}

/// What one (tenant, signal) read found. `found` holds the out-of-window
/// counts; an `Err` in `result` is the reason the pair could not be fully
/// read, and counts from parts read before the failure stay in `found`.
#[derive(Clone)]
struct PairOutcome {
    found: BTreeMap<u32, u64>,
    result: Result<(), String>,
}

/// Why one read of a pair's catalog stopped.
enum ReadError {
    /// A part the HEAD names was not found: possibly swept by a fold that
    /// replaced the HEAD after this read of it.
    PartNotFound(String),
    Other(String),
}

/// One (tenant, signal). A part `NotFound` re-reads the HEAD and its parts
/// once, the way the catalog resolver treats a sweep race; a second one is a
/// check that could not run.
async fn check_pair(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    get_requests: &AtomicU64,
) -> PairOutcome {
    let mut found = BTreeMap::new();
    let mut result = read_catalog(store, tenant, signal, &mut found, get_requests).await;
    if matches!(result, Err(ReadError::PartNotFound(_))) {
        found.clear();
        result = read_catalog(store, tenant, signal, &mut found, get_requests).await;
    }
    PairOutcome {
        found,
        result: result.map_err(|err| match err {
            ReadError::PartNotFound(key) => {
                format!("snapshot part {key} was not found, twice, after re-reading its HEAD")
            }
            ReadError::Other(reason) => reason,
        }),
    }
}

/// A catalog object above this build's format version (ADR-0066 decision 2):
/// not corruption, and re-ingesting does not fix it.
const NEWER_BUILD: &str = "written by a newer build than this process reads; upgrade this process";

/// One read of the HEAD and every part it names, counting out-of-window
/// entries into `found`.
async fn read_catalog(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    found: &mut BTreeMap<u32, u64>,
    get_requests: &AtomicU64,
) -> Result<(), ReadError> {
    let key = crate::fold::head_key(tenant, signal);
    get_requests.fetch_add(1, Ordering::Relaxed);
    let head_bytes = match store.get(&key, GetRange::Full).await {
        Ok(got) => got.data,
        // Nothing folded for this pair: no catalog entries to check.
        Err(StoreError::NotFound) => return Ok(()),
        Err(err @ StoreError::AccessDenied(_)) => {
            return Err(ReadError::Other(format!(
                "GET {key} was denied ({err}). S3 answers a GET of an absent key with 403 \
                 unless the role holds s3:ListBucket on that key, so this role cannot tell an \
                 absent HEAD from a denied one; grant s3:ListBucket on {key} (deploy/iam/README.md, \
                 \"Bootstrap keys\")"
            )));
        }
        Err(err) => return Err(ReadError::Other(format!("GET {key} failed: {err}"))),
    };
    let head = decode_head(&head_bytes).map_err(|err| {
        ReadError::Other(if err.is_newer_format_version() {
            format!("HEAD {key} was {NEWER_BUILD}: {err}")
        } else {
            format!("HEAD {key} is corrupt: {err}")
        })
    })?;
    let limits = PartLimits::default();
    for part in &head.parts {
        get_requests.fetch_add(1, Ordering::Relaxed);
        let bytes = match store.get(&part.key, GetRange::Full).await {
            Ok(got) => got.data,
            Err(StoreError::NotFound) => return Err(ReadError::PartNotFound(part.key.clone())),
            Err(err) => {
                return Err(ReadError::Other(format!("GET {} failed: {err}", part.key)));
            }
        };
        if blake3::hash(&bytes).as_bytes().as_slice() != part.blake3.as_slice() {
            return Err(ReadError::Other(format!(
                "snapshot part {} does not match the blake3 its HEAD names",
                part.key
            )));
        }
        let decoded = decode_part(&bytes, &limits).map_err(|err| {
            ReadError::Other(if err.is_newer_format_version() {
                format!("snapshot part {} was {NEWER_BUILD}: {err}", part.key)
            } else {
                format!("snapshot part {} is corrupt: {err}", part.key)
            })
        })?;
        for entry in decoded.entries {
            if !in_reader_window(signal, entry.segment_format_version) {
                *found.entry(entry.segment_format_version).or_default() += 1;
            }
        }
    }
    Ok(())
}

/// Whether startup runs the check at all. Under [`FormatWindowCheck::Off`]
/// `main` calls neither [`check`] nor [`enforce`], so no store request is
/// made.
pub fn should_run(mode: FormatWindowCheck) -> bool {
    mode != FormatWindowCheck::Off
}

/// Log `report` and apply `mode`. Under [`FormatWindowCheck::Warn`] this logs
/// and returns `Ok`; under [`FormatWindowCheck::Refuse`] it returns `Err`
/// naming every out-of-window signal and version, or saying the check could
/// not run, whenever the report is not clean.
pub fn enforce(mode: FormatWindowCheck, report: &FormatWindowReport) -> anyhow::Result<()> {
    for found in &report.out_of_window {
        tracing::warn!(
            signal = signal_name(found.signal),
            version = found.version,
            entries = found.entries,
            tenants = found.tenants,
            reader_window = %reader_window_label(found.signal),
            "format-window check: catalog entries outside this build's reader window; queries \
             and compaction will refuse these objects until they are migrated (ADR-2708 D5)"
        );
    }
    for unchecked in &report.unchecked {
        tracing::warn!(
            tenant_hash = %unchecked.tenant.to_hex(),
            signal = signal_name(unchecked.signal),
            reason = %unchecked.reason,
            "format-window check could not run for this tenant and signal; its stored \
             versions are unknown, not in window"
        );
    }
    if let Some(deadline) = report.deadline_elapsed {
        tracing::warn!(
            timeout = %humantime::format_duration(deadline),
            "format-window check: --format-window-check-timeout elapsed; the tenants and \
             signals logged above as not checked have unknown stored versions"
        );
    }
    if report.tenants == 0 {
        tracing::info!(
            "format-window check: no statically known tenant (--tenant-token, \
             --tenant-token-file, --maintain-tenant, --alert-rules-file), nothing checked"
        );
    } else if report.is_clean() {
        tracing::info!(
            tenants = report.tenants,
            get_requests = report.get_requests,
            "format-window check: no catalog entry outside the reader window"
        );
    }

    if mode != FormatWindowCheck::Refuse || report.is_clean() {
        return Ok(());
    }
    let mut problems: Vec<String> = Vec::new();
    if let Some(deadline) = report.deadline_elapsed {
        let not_run = report
            .unchecked
            .iter()
            .filter(|unchecked| unchecked.reason.starts_with("not checked:"))
            .count();
        problems.push(format!(
            "the check could not run to completion: --format-window-check-timeout ({}) \
             elapsed with {not_run} (tenant, signal) pairs not checked",
            humantime::format_duration(deadline)
        ));
    }
    problems.extend(report.out_of_window.iter().map(|found| {
        format!(
            "signal {} version {} ({} entries in {} tenants; reader window {})",
            signal_name(found.signal),
            found.version,
            found.entries,
            found.tenants,
            reader_window_label(found.signal)
        )
    }));
    problems.extend(report.unchecked.iter().map(|unchecked| {
        format!(
            "the check could not run for tenant {} signal {}: {}",
            unchecked.tenant.to_hex(),
            signal_name(unchecked.signal),
            unchecked.reason
        )
    }));
    anyhow::bail!(
        "--format-window-check refuse: {}. Migrate the out-of-window objects, fix the store \
         error, or start with --format-window-check warn",
        problems.join("; ")
    )
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use ravel_catalog::{HEAD_FORMAT_VERSION, encode_head, encode_part};
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{InstrumentedStore, PutOptions};
    use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef};
    use ravel_types::TenantId;

    use super::*;

    /// Long enough that no test without a hold reaches it.
    const NO_DEADLINE: Duration = Duration::from_secs(3600);

    fn tenant(name: &str) -> TenantHash {
        TenantId::new(name).hash()
    }

    /// The newest version this build writes for `signal`: inside the window.
    fn current(signal: Signal) -> u32 {
        match signal {
            Signal::Metrics => u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
            Signal::Spans => u32::from(ravel_rspan::footer::SUPPORTED_VERSIONS.newest()),
            _ => u32::from(ravel_logseg::footer::SUPPORTED_VERSIONS.newest()),
        }
    }

    /// The version just below `signal`'s window floor: outside it.
    fn below_window(signal: Signal) -> u32 {
        let oldest = match signal {
            Signal::Metrics => ravel_segment::SegmentVersion::WINDOW
                .last()
                .expect("window is non-empty")
                .number(),
            Signal::Spans => ravel_rspan::footer::SUPPORTED_VERSIONS.oldest(),
            _ => ravel_logseg::footer::SUPPORTED_VERSIONS.oldest(),
        };
        u32::from(oldest) - 1
    }

    fn entry(seq: u64, version: u32) -> SnapshotEntry {
        SnapshotEntry {
            level: 0,
            shard: 0,
            ingest_hour_bucket: 10,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: seq,
            content_hash: vec![0xBB; 32],
            object_size: 100,
            min_event_ts_ns: 0,
            max_event_ts_ns: 100,
            sample_count: 1,
            series_count: 1,
            segment_format_version: version,
            created_unix_ns: 1_000,
            declared_column_stats: Vec::new(),
        }
    }

    /// Writes a HEAD for (tenant, signal) naming one part per element of
    /// `parts`, each part holding entries with the given versions.
    async fn seed(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        parts: &[&[u32]],
    ) {
        let signal_code = ravel_commit::signal::to_proto(signal) as u32;
        let mut refs = Vec::new();
        let mut seq = 0u64;
        for (index, versions) in parts.iter().enumerate() {
            let hour = u32::try_from(index).expect("small") * 10 + 10;
            let entries: Vec<SnapshotEntry> = versions
                .iter()
                .map(|version| {
                    seq += 1;
                    SnapshotEntry {
                        ingest_hour_bucket: hour,
                        ..entry(seq, *version)
                    }
                })
                .collect();
            let bytes = encode_part(tenant.0, signal_code, 1, hour, &entries).expect("encode part");
            let key = format!(
                "t/{}/catalog/{}/snap/{hour}.{index:016x}.csnap",
                tenant.to_hex(),
                signal.key_prefix()
            );
            refs.push(SnapshotPartRef {
                key: key.clone(),
                blake3: blake3::hash(&bytes).as_bytes().to_vec(),
                size: bytes.len() as u64,
                entry_count: entries.len() as u64,
                watermark_hour: hour,
                min_hour: hour,
                column_stats: None,
                key_index: Vec::new(),
            });
            store
                .put(&key, Bytes::from(bytes), PutOptions::default())
                .await
                .expect("put part");
        }
        let head = SnapshotHead {
            format_version: HEAD_FORMAT_VERSION,
            tenant_hash: tenant.0.to_vec(),
            signal: signal_code,
            shard_count: 1,
            watermark_hour: refs.iter().map(|p| p.watermark_hour).max().unwrap_or(0),
            parts: refs,
            postings: None,
            folder_id: vec![0x33; 16],
            created_unix_ns: 0,
            shard_generation_count: 1,
        };
        store
            .put(
                &crate::fold::head_key(tenant, signal),
                Bytes::from(encode_head(&head).expect("encode head")),
                PutOptions::default(),
            )
            .await
            .expect("put head");
    }

    #[tokio::test]
    async fn an_out_of_window_entry_is_reported_with_signal_and_version() {
        let store = MemoryStore::new();
        let t = tenant("acme");
        seed(&store, &t, Signal::Metrics, &[&[current(Signal::Metrics)]]).await;
        let old_alert = below_window(Signal::Alerts);
        seed(
            &store,
            &t,
            Signal::Alerts,
            &[&[current(Signal::Alerts), old_alert, old_alert]],
        )
        .await;

        let report = check(&store, &[t], NO_DEADLINE).await;
        assert!(report.unchecked.is_empty(), "{:?}", report.unchecked);
        assert_eq!(
            report.out_of_window,
            vec![OutOfWindow {
                signal: Signal::Alerts,
                version: old_alert,
                entries: 2,
                tenants: 1,
            }]
        );

        let err = enforce(FormatWindowCheck::Refuse, &report)
            .expect_err("refuse must fail on an out-of-window entry");
        let message = err.to_string();
        assert!(
            message.contains(&format!("signal alerts version {old_alert}")),
            "{message}"
        );
        enforce(FormatWindowCheck::Warn, &report).expect("warn starts");
    }

    #[tokio::test]
    async fn an_in_window_tenant_reports_nothing() {
        let store = MemoryStore::new();
        let t = tenant("acme");
        for signal in CHECKED_SIGNALS {
            seed(
                &store,
                &t,
                signal,
                &[&[current(signal)], &[current(signal)]],
            )
            .await;
        }
        let report = check(&store, &[t, tenant("never-folded")], NO_DEADLINE).await;
        assert!(report.is_clean(), "{report:?}");
        enforce(FormatWindowCheck::Refuse, &report).expect("a clean report passes refuse");
    }

    /// The documented cost: per (tenant, signal) one GET of HEAD, plus one GET
    /// per part when HEAD exists, and no LIST or HEAD request.
    #[tokio::test]
    async fn the_check_issues_no_list_requests() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let a = tenant("a");
        let b = tenant("b");
        // a: metrics 1 part, logs 2 parts, spans/alerts/audit absent.
        seed(&store, &a, Signal::Metrics, &[&[current(Signal::Metrics)]]).await;
        seed(
            &store,
            &a,
            Signal::Logs,
            &[&[current(Signal::Logs)], &[below_window(Signal::Logs)]],
        )
        .await;
        // b: audit 3 parts, the rest absent.
        let audit = [current(Signal::Audit)];
        seed(&store, &b, Signal::Audit, &[&audit, &audit, &audit]).await;
        let before = store.metrics().snapshot();

        let report = check(&store, &[a, b], NO_DEADLINE).await;

        let after = store.metrics().snapshot();
        // 2 tenants x 5 signals HEAD GETs, plus 1 + 2 + 3 part GETs.
        let expected_gets = 2 * 5 + (1 + 2 + 3);
        assert_eq!(after.list_calls() - before.list_calls(), 0);
        assert_eq!(after.head.calls - before.head.calls, 0);
        assert_eq!(after.put.calls - before.put.calls, 0);
        assert_eq!(after.get.calls - before.get.calls, expected_gets);
        assert_eq!(report.get_requests, expected_gets);
        assert_eq!(report.out_of_window.len(), 1);
    }

    #[tokio::test]
    async fn a_store_error_is_not_reported_as_clean() {
        let inner = MemoryStore::new();
        let t = tenant("acme");
        seed(&inner, &t, Signal::Spans, &[&[current(Signal::Spans)]]).await;
        let head = crate::fold::head_key(&t, Signal::Spans);
        let store = Arc::new(FaultStore::new(
            inner,
            FaultPlan::empty().with_rule(
                Rule::new(Op::Get, ScriptedFault::Timeout).with_key_contains(head.clone()),
            ),
        ));

        let report = check(store.as_ref(), &[t], NO_DEADLINE).await;
        assert_eq!(
            store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::Timeout),
            1,
            "the fault must have fired"
        );
        assert!(!report.is_clean());
        assert_eq!(report.unchecked.len(), 1);
        assert_eq!(report.unchecked[0].signal, Signal::Spans);
        assert!(report.out_of_window.is_empty());

        enforce(FormatWindowCheck::Warn, &report).expect("warn logs and starts");
        let err = enforce(FormatWindowCheck::Refuse, &report)
            .expect_err("refuse must not read an unreadable catalog as clean");
        assert!(
            err.to_string().contains("could not run for tenant"),
            "{err}"
        );
        assert!(err.to_string().contains("signal spans"), "{err}");
    }

    /// A corrupt HEAD, and a part whose bytes do not match the blake3 its HEAD
    /// names, are both a check that could not run.
    #[tokio::test]
    async fn a_corrupt_catalog_is_not_reported_as_clean() {
        let store = MemoryStore::new();
        let t = tenant("acme");
        store
            .put(
                &crate::fold::head_key(&t, Signal::Metrics),
                Bytes::from_static(b"not a head"),
                PutOptions::default(),
            )
            .await
            .expect("put head");
        seed(&store, &t, Signal::Logs, &[&[current(Signal::Logs)]]).await;
        let head = decode_head(
            &store
                .get(&crate::fold::head_key(&t, Signal::Logs), GetRange::Full)
                .await
                .expect("get head")
                .data,
        )
        .expect("decode head");
        store
            .put(
                &head.parts[0].key,
                Bytes::from_static(b"replaced part"),
                PutOptions::default(),
            )
            .await
            .expect("overwrite part");

        let report = check(&store, &[t], NO_DEADLINE).await;
        let signals: Vec<Signal> = report.unchecked.iter().map(|u| u.signal).collect();
        assert_eq!(signals, vec![Signal::Metrics, Signal::Logs]);
        assert!(report.unchecked[0].reason.contains("corrupt"), "{report:?}");
        assert!(report.unchecked[1].reason.contains("blake3"), "{report:?}");
        assert!(enforce(FormatWindowCheck::Refuse, &report).is_err());
    }

    async fn read_head(
        store: &dyn ObjectStoreBackend,
        t: &TenantHash,
        signal: Signal,
    ) -> SnapshotHead {
        decode_head(
            &store
                .get(&crate::fold::head_key(t, signal), GetRange::Full)
                .await
                .expect("get head")
                .data,
        )
        .expect("decode head")
    }

    /// ADR-0066 decision 2: a format version above this build's is not
    /// corruption. It is still a check that could not run.
    fn assert_newer_build(report: &FormatWindowReport, object: &str) {
        assert_eq!(report.unchecked.len(), 1, "{report:?}");
        let reason = &report.unchecked[0].reason;
        assert!(reason.starts_with(object), "{reason}");
        assert!(reason.contains("written by a newer build"), "{reason}");
        assert!(reason.contains("upgrade this process"), "{reason}");
        assert!(!reason.contains("corrupt"), "{reason}");
        assert!(!report.is_clean());
        assert!(enforce(FormatWindowCheck::Refuse, report).is_err());
    }

    #[tokio::test]
    async fn a_head_from_a_newer_build_is_not_reported_corrupt() {
        use prost::Message;

        let store = MemoryStore::new();
        let t = tenant("acme");
        seed(&store, &t, Signal::Metrics, &[&[current(Signal::Metrics)]]).await;
        let mut head = read_head(&store, &t, Signal::Metrics).await;
        head.format_version = HEAD_FORMAT_VERSION + 1;
        store
            .put(
                &crate::fold::head_key(&t, Signal::Metrics),
                Bytes::from(head.encode_to_vec()),
                PutOptions::default(),
            )
            .await
            .expect("put head");

        let report = check(&store, &[t], NO_DEADLINE).await;
        assert_newer_build(&report, "HEAD ");
    }

    #[tokio::test]
    async fn a_part_from_a_newer_build_is_not_reported_corrupt() {
        let store = MemoryStore::new();
        let t = tenant("acme");
        seed(&store, &t, Signal::Logs, &[&[current(Signal::Logs)]]).await;
        let mut head = read_head(&store, &t, Signal::Logs).await;
        let part_key = head.parts[0].key.clone();
        let mut part = store
            .get(&part_key, GetRange::Full)
            .await
            .expect("get part")
            .data
            .to_vec();
        // The envelope version byte follows the 4-byte magic.
        assert_eq!(part[4], ravel_catalog::VERSION);
        part[4] = ravel_catalog::VERSION + 1;
        head.parts[0].blake3 = blake3::hash(&part).as_bytes().to_vec();
        store
            .put(&part_key, Bytes::from(part), PutOptions::default())
            .await
            .expect("put part");
        store
            .put(
                &crate::fold::head_key(&t, Signal::Logs),
                Bytes::from(encode_head(&head).expect("encode head")),
                PutOptions::default(),
            )
            .await
            .expect("put head");

        let report = check(&store, &[t], NO_DEADLINE).await;
        assert_newer_build(&report, "snapshot part ");
    }

    #[test]
    fn the_checked_tenants_include_alert_rule_tenants_once_each() {
        let (a, b, c) = (tenant("a"), tenant("b"), tenant("c"));
        let tenants = tenants_to_check(&[b, a], &[c, a]);
        let mut expected = vec![a, b, c];
        expected.sort_by_key(|tenant| tenant.0);
        assert_eq!(tenants, expected);
        assert_eq!(tenants_to_check(&[], &[c]), vec![c]);
    }

    #[tokio::test]
    async fn an_absent_head_is_nothing_to_check_for_every_signal() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let t = tenant("fresh-bucket");
        let report = check(&store, &[t], NO_DEADLINE).await;
        assert!(report.is_clean(), "{report:?}");
        assert_eq!(report.get_requests, CHECKED_SIGNALS.len() as u64);
        assert_eq!(
            store.metrics().snapshot().get.calls,
            CHECKED_SIGNALS.len() as u64,
            "one NotFound HEAD GET per signal"
        );
        enforce(FormatWindowCheck::Refuse, &report).expect("refuse starts on a fresh bucket");
    }

    /// Answers `AccessDenied` for every key containing `denied`, the way S3
    /// answers a GET of an absent key for a role with no ListBucket grant on
    /// it. `FaultStore` scripts no AccessDenied.
    struct DenyingStore {
        inner: MemoryStore,
        denied: String,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for DenyingStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }
        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            if key.contains(&self.denied) {
                return Err(StoreError::AccessDenied(format!("{key}: 403 Forbidden")));
            }
            self.inner.get(key, range).await
        }
        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }
        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    #[tokio::test]
    async fn an_access_denied_head_names_the_list_grant() {
        let t = tenant("acme");
        let head = crate::fold::head_key(&t, Signal::Alerts);
        let store = DenyingStore {
            inner: MemoryStore::new(),
            denied: head.clone(),
        };

        let report = check(&store, &[t], NO_DEADLINE).await;
        assert_eq!(report.unchecked.len(), 1, "{report:?}");
        assert_eq!(report.unchecked[0].signal, Signal::Alerts);
        let reason = &report.unchecked[0].reason;
        assert!(reason.contains("s3:ListBucket"), "{reason}");
        assert!(
            reason.contains(&format!("grant s3:ListBucket on {head}")),
            "{reason}"
        );
        assert!(reason.contains("absent HEAD from a denied one"), "{reason}");

        enforce(FormatWindowCheck::Warn, &report).expect("warn logs and starts");
        let err = enforce(FormatWindowCheck::Refuse, &report)
            .expect_err("a denied HEAD is not evidence of an in-window catalog");
        assert!(err.to_string().contains("could not run"), "{err}");
        assert!(err.to_string().contains("s3:ListBucket"), "{err}");
    }

    /// One tenant's metrics HEAD GET is held and never released, so the
    /// deadline is the only way the check ends. Paused time auto-advances
    /// once every task is parked, so the test does not wait in wall time.
    #[tokio::test(start_paused = true)]
    async fn the_check_stops_at_its_deadline_and_reports_the_rest_unchecked() {
        let inner = MemoryStore::new();
        let fast = tenant("fast");
        let stuck = tenant("stuck");
        let old_log = below_window(Signal::Logs);
        seed(&inner, &fast, Signal::Logs, &[&[old_log]]).await;
        let store = FaultStore::new(inner, FaultPlan::empty());
        let gate = store.hold(
            Op::Get,
            Some(crate::fold::head_key(&stuck, Signal::Metrics)),
            Occurrence::Always,
        );
        let deadline = Duration::from_millis(250);

        let report = tokio::time::timeout(
            Duration::from_secs(600),
            check(&store, &[fast, stuck], deadline),
        )
        .await
        .expect("the check must end at its own deadline");

        assert_eq!(gate.held_count(), 1, "the hold must have fired");
        assert_eq!(report.deadline_elapsed, Some(deadline));
        // Every pair of `fast` finished and its out-of-window entry counts.
        assert_eq!(
            report.out_of_window,
            vec![OutOfWindow {
                signal: Signal::Logs,
                version: old_log,
                entries: 1,
                tenants: 1,
            }]
        );
        // Only the held pair is unfinished: with 10 pairs and 8 in flight,
        // the other nine all complete around it.
        let unchecked: Vec<(TenantHash, Signal)> = report
            .unchecked
            .iter()
            .map(|u| (u.tenant, u.signal))
            .collect();
        assert_eq!(unchecked, vec![(stuck, Signal::Metrics)]);
        assert!(
            report.unchecked[0]
                .reason
                .contains("--format-window-check-timeout"),
            "{report:?}"
        );

        enforce(FormatWindowCheck::Warn, &report).expect("warn starts after the deadline");
        let err = enforce(FormatWindowCheck::Refuse, &report)
            .expect_err("refuse exits when the deadline cut the check short");
        let message = err.to_string();
        assert!(message.contains("could not run to completion"), "{message}");
        assert!(
            message.contains("1 (tenant, signal) pairs not checked"),
            "{message}"
        );
    }

    /// The first GET of the part answers NotFound, as it would if a fold
    /// swept it between this HEAD read and the part read. The check re-reads
    /// HEAD once and reads the part again.
    #[tokio::test]
    async fn a_part_swept_between_head_and_part_is_retried_once() {
        let inner = InstrumentedStore::new(MemoryStore::new());
        let t = tenant("acme");
        let old_log = below_window(Signal::Logs);
        seed(&inner, &t, Signal::Logs, &[&[old_log]]).await;
        let store = FaultStore::new(
            inner,
            FaultPlan::empty().with_rule(
                Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
                    .with_key_contains("/catalog/l/snap/")
                    .with_occurrence(Occurrence::Nth(1)),
            ),
        );

        let report = check(&store, &[t], NO_DEADLINE).await;
        assert_eq!(
            store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::NotFoundBlip),
            1,
            "the fault must have fired"
        );
        assert!(report.unchecked.is_empty(), "{report:?}");
        assert_eq!(
            report.out_of_window,
            vec![OutOfWindow {
                signal: Signal::Logs,
                version: old_log,
                entries: 1,
                tenants: 1,
            }],
            "the retry counts the part once, not twice"
        );
        // 5 HEAD GETs, the failed part GET, the HEAD re-read, the part again.
        assert_eq!(report.get_requests, 5 + 3);
    }

    #[tokio::test]
    async fn a_part_not_found_twice_is_a_check_that_could_not_run() {
        let inner = MemoryStore::new();
        let t = tenant("acme");
        seed(&inner, &t, Signal::Logs, &[&[current(Signal::Logs)]]).await;
        let store = FaultStore::new(
            inner,
            FaultPlan::empty().with_rule(
                Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
                    .with_key_contains("/catalog/l/snap/"),
            ),
        );

        let report = check(&store, &[t], NO_DEADLINE).await;
        assert_eq!(
            store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::NotFoundBlip),
            2,
            "one retry, not more"
        );
        assert_eq!(report.unchecked.len(), 1, "{report:?}");
        assert!(
            report.unchecked[0].reason.contains("not found, twice"),
            "{report:?}"
        );
        assert!(enforce(FormatWindowCheck::Refuse, &report).is_err());
    }

    /// `main` gates the check on [`should_run`]; the binary test
    /// `the_default_starts_without_running_the_check` pins that it does.
    #[test]
    fn off_issues_no_store_requests() {
        assert!(!should_run(FormatWindowCheck::Off));
        assert!(should_run(FormatWindowCheck::Warn));
        assert!(should_run(FormatWindowCheck::Refuse));
    }

    #[test]
    fn the_window_comes_from_the_reader_constants() {
        for signal in CHECKED_SIGNALS {
            assert!(in_reader_window(signal, current(signal)), "{signal:?}");
            assert!(
                !in_reader_window(signal, below_window(signal)),
                "{signal:?}"
            );
            assert!(!in_reader_window(signal, current(signal) + 1), "{signal:?}");
            assert!(
                !in_reader_window(signal, u32::from(u16::MAX) + 1),
                "{signal:?}"
            );
        }
    }
}
