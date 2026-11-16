//! The process's view of `sys/compat`, the store-wide format activation record
//! (ADR-4233 decision 3), and its `/metrics` families.
//!
//! [`build_compat_view`] builds one [`CompatView`] per process, in every mode,
//! keyed by the deployment key (`--tenant-hash-key-file`), the one key every
//! mode accepts. [`start`](crate::start) reads the record once before any
//! writer starts and then spawns one [`spawn_refresh_loop`] task that re-reads
//! it every [`DEFAULT_COMPAT_REFRESH_INTERVAL`]. The alert evaluator also calls
//! [`refresh_once`] when it meets a lease from an epoch its view has not seen,
//! so two refreshes can overlap; [`CompatView`] keeps every monotone value
//! either read saw and the record from the read that started last. An unkeyed
//! bucket has no deployment key, so its process builds no view and every
//! writer keeps its built-in version.
//!
//! A refresh failure is logged and counted in
//! `ravel_compat_refresh_failures_total`, never fatal: the view keeps its last
//! successful read. Writers refuse when the last successful read is
//! `COMPAT_VIEW_HORIZON` old or older, or there has been none, with one
//! exception: a blind view, one no read has succeeded for and whose latest
//! failed read, if any, was refused as an `AccessDenied`, answers each
//! family's built-in version on a bucket whose provisioning record carries no
//! `compat_epoch`, so a credential without the grant on a store that was never
//! activated stops no writer. A 5xx, timeout or throttle before the first
//! successful read suspends that exception until the latest failed read is an
//! `AccessDenied` again, and a read that returned an object ends it for the
//! rest of the run. A record from a newer release is logged at error with
//! both versions.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use ravel_catalog::compat::{
    CompatClock, CompatError, CompatKeys, CompatRefusal, CompatView, KNOWN_FAMILIES,
};
use ravel_object_store::ObjectStoreBackend;
use tokio::task::JoinHandle;

pub use ravel_catalog::compat::DEFAULT_COMPAT_REFRESH_INTERVAL;

/// [`CompatClock`] over the system clock: the one wall-clock read of the
/// compat view, kept out of the library as `ravel-maintain`'s clock is.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemCompatClock;

impl CompatClock for SystemCompatClock {
    fn now_unix_ns(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}

/// Why a writer refused a publish, the `reason` label of
/// `ravel_compat_publish_refused_total`: one value per [`CompatRefusal`]
/// variant, then one per ingest-only ([`ravel_ingest::CompatRefusalReason`])
/// reason, then one per maintenance-only
/// [`ravel_maintain::CompatRunRefusal`] reason, and `other` for a reason this
/// enum does not name, so the label set stays closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CompatRefusalReason {
    ViewStale,
    ActiveVersionNotWritable,
    CompatRecordMissing,
    FenceFailed,
    ActiveVersionMoved,
    NoBucketView,
    PublishLifetimeAboveCompat,
    TargetNotActive,
    CompactionRecordNotActive,
    PublishLifetimeExpired,
    Other,
}

impl CompatRefusalReason {
    /// The view-policy reasons every writer can raise, so each series renders
    /// from the first scrape. A maintenance-only reason renders once counted.
    pub const ALL: [CompatRefusalReason; 3] = [
        CompatRefusalReason::ViewStale,
        CompatRefusalReason::ActiveVersionNotWritable,
        CompatRefusalReason::CompatRecordMissing,
    ];

    /// Every named reason, in label order.
    const NAMED: [CompatRefusalReason; 10] = [
        CompatRefusalReason::ViewStale,
        CompatRefusalReason::ActiveVersionNotWritable,
        CompatRefusalReason::CompatRecordMissing,
        CompatRefusalReason::FenceFailed,
        CompatRefusalReason::ActiveVersionMoved,
        CompatRefusalReason::NoBucketView,
        CompatRefusalReason::PublishLifetimeAboveCompat,
        CompatRefusalReason::TargetNotActive,
        CompatRefusalReason::CompactionRecordNotActive,
        CompatRefusalReason::PublishLifetimeExpired,
    ];

    /// The reason of a maintenance refusal counted under `reason`
    /// ([`ravel_maintain::CompatRunRefusal::reason`]), or of an ingest refusal
    /// counted under `reason` ([`ravel_ingest::CompatRefusalReason::as_str`]);
    /// `Other` for one this enum does not name.
    pub fn of_label(reason: &str) -> CompatRefusalReason {
        CompatRefusalReason::NAMED
            .into_iter()
            .find(|named| named.name() == reason)
            .unwrap_or(CompatRefusalReason::Other)
    }

    /// Alias of [`Self::of_label`] for a maintenance refusal reason.
    pub fn of_maintain(reason: &str) -> CompatRefusalReason {
        CompatRefusalReason::of_label(reason)
    }

    /// The reason of `refusal`.
    pub fn of(refusal: &CompatRefusal) -> CompatRefusalReason {
        match refusal {
            CompatRefusal::ViewStale { .. } => CompatRefusalReason::ViewStale,
            CompatRefusal::ActiveVersionNotWritable { .. } => {
                CompatRefusalReason::ActiveVersionNotWritable
            }
            CompatRefusal::CompatRecordMissing { .. } => CompatRefusalReason::CompatRecordMissing,
        }
    }

    /// The `reason` label value.
    pub fn name(self) -> &'static str {
        match self {
            CompatRefusalReason::ViewStale => "view_stale",
            CompatRefusalReason::ActiveVersionNotWritable => "active_version_not_writable",
            CompatRefusalReason::CompatRecordMissing => "compat_record_missing",
            CompatRefusalReason::FenceFailed => "fence_failed",
            CompatRefusalReason::ActiveVersionMoved => "active_version_moved",
            CompatRefusalReason::NoBucketView => "no_bucket_view",
            CompatRefusalReason::PublishLifetimeAboveCompat => "publish_lifetime_above_compat",
            CompatRefusalReason::TargetNotActive => "target_not_active",
            CompatRefusalReason::CompactionRecordNotActive => "compaction_record_not_active",
            CompatRefusalReason::PublishLifetimeExpired => "publish_lifetime_expired",
            CompatRefusalReason::Other => "other",
        }
    }

    /// Add one ingest router's per-signal refusals ([`ravel_ingest`]) to
    /// `series`, keyed by `family` (the signal's format family), summing into
    /// an existing `(family, reason)` entry or appending a new one. The ingest
    /// routers count these in their own `IngestMetrics`, apart from the server
    /// static and `ravel-maintain`, so `/metrics` sums them in here (#4301).
    pub fn merge_ingest_refusals(
        series: &mut Vec<(CompatFamilyLabel, CompatRefusalReason, u64)>,
        family: &str,
        snapshot: &ravel_ingest::CompatRefusalSnapshot,
    ) {
        let label = CompatFamilyLabel::of(family);
        for ingest_reason in ravel_ingest::CompatRefusalReason::ALL {
            let count = snapshot.get(ingest_reason);
            if count == 0 {
                continue;
            }
            let reason = CompatRefusalReason::of_label(ingest_reason.as_str());
            match series
                .iter_mut()
                .find(|(f, r, _)| *f == label && *r == reason)
            {
                Some((_, _, existing)) => *existing += count,
                None => series.push((label, reason, count)),
            }
        }
    }
}

/// A format family as a `/metrics` `family` label: one of
/// [`KNOWN_FAMILIES`], or `other` for a family only a newer release knows, so
/// the label set stays closed whatever `sys/compat` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompatFamilyLabel(&'static str);

impl CompatFamilyLabel {
    /// The label for `family`.
    pub fn of(family: &str) -> CompatFamilyLabel {
        CompatFamilyLabel(
            KNOWN_FAMILIES
                .iter()
                .find(|known| **known == family)
                .copied()
                .unwrap_or("other"),
        )
    }

    /// The label value.
    pub fn as_str(self) -> &'static str {
        self.0
    }
}

static REFRESH_FAILURES: AtomicU64 = AtomicU64::new(0);

type RefusalCounts = BTreeMap<(CompatFamilyLabel, CompatRefusalReason), u64>;

fn publish_refusals() -> &'static Mutex<RefusalCounts> {
    static REFUSALS: OnceLock<Mutex<RefusalCounts>> = OnceLock::new();
    REFUSALS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Count one publish a writer refused under the write policy, as
/// `ravel_compat_publish_refused_total{family,reason}`. Every writer in the
/// process counts here, whichever task it runs in.
pub fn record_publish_refusal(family: &str, refusal: &CompatRefusal) {
    *publish_refusals()
        .lock()
        .entry((
            CompatFamilyLabel::of(family),
            CompatRefusalReason::of(refusal),
        ))
        .or_insert(0) += 1;
}

/// Every `(family, reason)` series of `ravel_compat_publish_refused_total`:
/// each known family under each [`CompatRefusalReason::ALL`] reason, zero
/// until a refusal, plus every series a refusal counted, including a family
/// this build does not know as `other`. The server's writers and
/// `ravel-maintain`'s runs ([`ravel_maintain::compat_publish_refused_snapshot`])
/// count apart; a series is their sum.
pub fn publish_refusals_snapshot() -> Vec<(CompatFamilyLabel, CompatRefusalReason, u64)> {
    let server = publish_refusals().lock().clone();
    merge_refusals(server, ravel_maintain::compat_publish_refused_snapshot())
}

fn merge_refusals(
    server: RefusalCounts,
    maintain: Vec<(String, &'static str, u64)>,
) -> Vec<(CompatFamilyLabel, CompatRefusalReason, u64)> {
    let mut series: RefusalCounts = KNOWN_FAMILIES
        .iter()
        .flat_map(|family| {
            CompatRefusalReason::ALL
                .iter()
                .map(move |reason| ((CompatFamilyLabel::of(family), *reason), 0))
        })
        .collect();
    for (key, count) in server {
        *series.entry(key).or_insert(0) += count;
    }
    for (family, reason, count) in maintain {
        *series
            .entry((
                CompatFamilyLabel::of(&family),
                CompatRefusalReason::of_maintain(reason),
            ))
            .or_insert(0) += count;
    }
    series
        .into_iter()
        .map(|((family, reason), count)| (family, reason, count))
        .collect()
}

/// Failed `sys/compat` reads since the process started.
pub fn refresh_failures_total() -> u64 {
    REFRESH_FAILURES.load(Ordering::Relaxed)
}

/// The process's view of `sys/compat`, keyed by `deployment_key`. `None` on
/// an unkeyed bucket: without the key the record cannot be authenticated.
pub fn build_compat_view(
    store: Arc<dyn ObjectStoreBackend>,
    deployment_key: Option<&[u8; 32]>,
    clock: Arc<dyn CompatClock>,
) -> anyhow::Result<Option<CompatView>> {
    let Some(key) = deployment_key else {
        tracing::warn!(
            "no deployment key (--tenant-hash-key-file): sys/compat cannot be authenticated, so \
             this process installs no format write policy and every writer emits its built-in \
             version"
        );
        return Ok(None);
    };
    let keys = CompatKeys::from_file_keys(&[*key])?;
    Ok(Some(CompatView::new(store, keys, clock)))
}

/// Read `sys/compat` once into `view`. A failure is logged and counted, and
/// leaves the view as it was.
pub async fn refresh_once(view: &CompatView) {
    match view.refresh().await {
        Ok(refresh) => {
            for rolled_back in &refresh.rolled_back {
                tracing::error!(
                    family = %rolled_back.family,
                    seen = rolled_back.seen,
                    read = rolled_back.read,
                    "sys/compat read a family's active write version below one this process has \
                     already seen; keeping the seen version (ADR-4233 decision 3: monotone views)"
                );
            }
            tracing::debug!(
                present = refresh.present,
                read_epoch = ?refresh.read_epoch,
                seen_epoch = refresh.seen_epoch,
                "sys/compat refreshed"
            );
        }
        Err(err) => {
            REFRESH_FAILURES.fetch_add(1, Ordering::Relaxed);
            match &err {
                CompatError::CompatRecordNewer { got, ceiling, .. } => tracing::error!(
                    record_format_version = got,
                    max_read_version = ceiling,
                    "sys/compat was written by a newer release than this build reads; keeping \
                     the last view, which goes stale and refuses publishes after the horizon"
                ),
                _ => tracing::warn!(
                    error = %err,
                    "sys/compat refresh failed; keeping the last successful read"
                ),
            }
        }
    }
}

/// Re-read `sys/compat` into `view` every `interval`, starting one interval
/// from now. One task per process.
pub fn spawn_refresh_loop(view: CompatView, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            refresh_once(&view).await;
        }
    })
}

/// The fold's write-policy gate (ADR-4233 decision 3): each family's active
/// version is a ceiling, so a fold of `signal` is refused only when a catalog
/// family it writes would emit a version above the active one, or the active
/// version is not one this build reads. A refusal is logged, counted under
/// `ravel_compat_publish_refused_total`, and returned before the fold reads or
/// writes anything.
pub fn fold_compat_gate(
    catalog: &ravel_catalog::Catalog,
    tenant: &ravel_types::TenantHash,
    signal: ravel_types::Signal,
    now_ns: i64,
) -> Result<(), ravel_catalog::FoldCompatRefusal> {
    catalog
        .fold_compat_versions(signal, now_ns)
        .map(|_| ())
        .inspect_err(|refusal| {
            record_publish_refusal(refusal.family, &refusal.refusal);
            tracing::warn!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                family = refusal.family,
                %refusal,
                "catalog fold refused by the sys/compat write policy; nothing written this cycle"
            );
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ravel_maintain::CompatRunRefusal;

    fn every_maintain_refusal() -> Vec<CompatRunRefusal> {
        vec![
            CompatRunRefusal::Compat(CompatRefusal::ViewStale {
                last_read_unix_ns: None,
                now_unix_ns: 1,
            }),
            CompatRunRefusal::Compat(CompatRefusal::ActiveVersionNotWritable {
                family: "rseg".to_string(),
                active: 9,
                writable: vec![1],
            }),
            CompatRunRefusal::Compat(CompatRefusal::CompatRecordMissing {
                bucket_compat_epoch: 1,
                seen_compat_epoch: 0,
            }),
            CompatRunRefusal::PublishLifetimeAboveCompat {
                process_ns: 2,
                record_ns: 1,
            },
            CompatRunRefusal::TargetNotActive {
                target: 3,
                active: 2,
            },
            CompatRunRefusal::CompactionRecordNotActive { active: 1 },
            CompatRunRefusal::FenceFailed {
                compat_epoch: 4,
                detail: "x".to_string(),
            },
            CompatRunRefusal::PublishLifetimeExpired {
                elapsed_ns: 2,
                lifetime_ns: 1,
            },
        ]
    }

    #[test]
    fn every_maintain_refusal_reason_renders_under_its_own_label() {
        let refusals = every_maintain_refusal();
        for refusal in &refusals {
            let label = CompatRefusalReason::of_maintain(refusal.reason());
            assert_ne!(label, CompatRefusalReason::Other, "{refusal:?}");
            assert_eq!(label.name(), refusal.reason());
        }
        let mut labels: Vec<_> = refusals
            .iter()
            .map(|r| CompatRefusalReason::of_maintain(r.reason()))
            .collect();
        labels.dedup();
        assert_eq!(labels.len(), refusals.len());
        assert_eq!(
            CompatRefusalReason::of_maintain("a_reason_from_a_newer_build"),
            CompatRefusalReason::Other
        );
    }

    #[test]
    fn the_refusal_series_sum_server_and_maintenance_counts() {
        let mut server = RefusalCounts::new();
        server.insert(
            (
                CompatFamilyLabel::of("rseg"),
                CompatRefusalReason::ViewStale,
            ),
            2,
        );
        let maintain = vec![
            ("rseg".to_string(), "view_stale", 3),
            ("rlog".to_string(), "fence_failed", 5),
            ("a-newer-family".to_string(), "a_newer_reason", 7),
        ];
        let series = merge_refusals(server, maintain);
        let count = |family: &str, reason: CompatRefusalReason| {
            let label = CompatFamilyLabel::of(family);
            let found: Vec<u64> = series
                .iter()
                .filter(|(f, r, _)| *f == label && *r == reason)
                .map(|(_, _, c)| *c)
                .collect();
            assert_eq!(found.len(), 1, "{family} {reason:?}: {series:?}");
            found[0]
        };
        assert_eq!(count("rseg", CompatRefusalReason::ViewStale), 5);
        assert_eq!(count("rlog", CompatRefusalReason::FenceFailed), 5);
        assert_eq!(count("other", CompatRefusalReason::Other), 7);
        assert_eq!(count("rlog", CompatRefusalReason::ViewStale), 0);
        assert_eq!(
            series.len(),
            KNOWN_FAMILIES.len() * CompatRefusalReason::ALL.len() + 2
        );
    }
}
