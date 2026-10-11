//! Startup format-window check (ADR-2708 decision D5c, issue #2724).
//!
//! Reports catalog entries whose `segment_format_version` is outside this
//! build's reader window, so a format crossing shows up at startup instead of
//! at the first query that touches an old object. No reader window is widened
//! here (ADR-0531); the check only counts.
//!
//! For each tenant and each signal in [`CHECKED_SIGNALS`] it GETs the catalog
//! HEAD and then every snapshot part that HEAD names, and nothing else: one
//! GET when there is no HEAD, otherwise `1 + parts` GETs, and no LIST, no
//! HEAD request and no write. Commit records above the HEAD watermark (the
//! unsealed hours a fold has not taken in yet) are not read, because finding
//! them needs a LIST of the commit prefix.
//!
//! Advisory: the version is the writer's stamp in the catalog entry, not the
//! object's own trailer, so a clean report is not a proof that every object is
//! readable. The reader still checks each object's trailer on open.

use std::collections::BTreeMap;

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
}

impl FormatWindowReport {
    /// True only when every pair was read and none was out of window.
    pub fn is_clean(&self) -> bool {
        self.out_of_window.is_empty() && self.unchecked.is_empty()
    }
}

#[derive(Default)]
struct Tally {
    entries: u64,
    tenants: u64,
}

/// Read each tenant's HEAD and snapshot parts for every signal in
/// [`CHECKED_SIGNALS`] and count the entries outside the reader window.
/// Never fails as a whole: a read or decode error lands in
/// [`FormatWindowReport::unchecked`] for that (tenant, signal).
pub async fn check(store: &dyn ObjectStoreBackend, tenants: &[TenantHash]) -> FormatWindowReport {
    let mut report = FormatWindowReport {
        tenants: tenants.len(),
        ..FormatWindowReport::default()
    };
    let mut tallies: BTreeMap<(usize, u32), Tally> = BTreeMap::new();
    for tenant in tenants {
        for (signal_index, signal) in CHECKED_SIGNALS.into_iter().enumerate() {
            let mut found: BTreeMap<u32, u64> = BTreeMap::new();
            if let Err(reason) =
                check_one(store, tenant, signal, &mut found, &mut report.get_requests).await
            {
                report.unchecked.push(Unchecked {
                    tenant: *tenant,
                    signal,
                    reason,
                });
            }
            for (version, entries) in found {
                let tally = tallies.entry((signal_index, version)).or_default();
                tally.entries += entries;
                tally.tenants += 1;
            }
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
    report
}

/// One (tenant, signal): count out-of-window entries into `found`. An `Err`
/// is the reason the pair could not be fully read; counts from parts read
/// before the failure stay in `found`.
async fn check_one(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    found: &mut BTreeMap<u32, u64>,
    get_requests: &mut u64,
) -> Result<(), String> {
    let key = crate::fold::head_key(tenant, signal);
    *get_requests += 1;
    let head_bytes = match store.get(&key, GetRange::Full).await {
        Ok(got) => got.data,
        // Nothing folded for this pair: no catalog entries to check.
        Err(StoreError::NotFound) => return Ok(()),
        Err(err) => return Err(format!("GET {key} failed: {err}")),
    };
    let head = decode_head(&head_bytes).map_err(|err| format!("HEAD {key} is corrupt: {err}"))?;
    let limits = PartLimits::default();
    for part in &head.parts {
        *get_requests += 1;
        let bytes = store
            .get(&part.key, GetRange::Full)
            .await
            .map_err(|err| format!("GET {} failed: {err}", part.key))?
            .data;
        if blake3::hash(&bytes).as_bytes().as_slice() != part.blake3.as_slice() {
            return Err(format!(
                "snapshot part {} does not match the blake3 its HEAD names",
                part.key
            ));
        }
        let decoded = decode_part(&bytes, &limits)
            .map_err(|err| format!("snapshot part {} is corrupt: {err}", part.key))?;
        for entry in decoded.entries {
            if !in_reader_window(signal, entry.segment_format_version) {
                *found.entry(entry.segment_format_version).or_default() += 1;
            }
        }
    }
    Ok(())
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

    if mode == FormatWindowCheck::Warn || report.is_clean() {
        return Ok(());
    }
    let mut problems: Vec<String> = report
        .out_of_window
        .iter()
        .map(|found| {
            format!(
                "signal {} version {} ({} entries in {} tenants; reader window {})",
                signal_name(found.signal),
                found.version,
                found.entries,
                found.tenants,
                reader_window_label(found.signal)
            )
        })
        .collect();
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
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{InstrumentedStore, PutOptions};
    use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef};
    use ravel_types::TenantId;

    use super::*;

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

        let report = check(&store, &[t]).await;
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
        let report = check(&store, &[t, tenant("never-folded")]).await;
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

        let report = check(&store, &[a, b]).await;

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

        let report = check(store.as_ref(), &[t]).await;
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

        let report = check(&store, &[t]).await;
        let signals: Vec<Signal> = report.unchecked.iter().map(|u| u.signal).collect();
        assert_eq!(signals, vec![Signal::Metrics, Signal::Logs]);
        assert!(report.unchecked[0].reason.contains("corrupt"), "{report:?}");
        assert!(report.unchecked[1].reason.contains("blake3"), "{report:?}");
        assert!(enforce(FormatWindowCheck::Refuse, &report).is_err());
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
