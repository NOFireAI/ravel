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
//! successful read. Once a GET of `sys/compat` has returned an object, writers
//! refuse when the last successful read is older than `COMPAT_VIEW_HORIZON`.
//! Before that the view answers each family's built-in version, so a read
//! refused with `AccessDenied` on a store that was never activated stops no
//! writer. A record from a newer release is logged at error with both
//! versions.

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
/// variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CompatRefusalReason {
    ViewStale,
    ActiveVersionNotWritable,
    CompatRecordMissing,
}

impl CompatRefusalReason {
    /// Every reason, so each series renders from the first scrape.
    pub const ALL: [CompatRefusalReason; 3] = [
        CompatRefusalReason::ViewStale,
        CompatRefusalReason::ActiveVersionNotWritable,
        CompatRefusalReason::CompatRecordMissing,
    ];

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
/// each known family under each reason, zero until a refusal, plus `other`
/// once a refusal named a family this build does not know.
pub fn publish_refusals_snapshot() -> Vec<(CompatFamilyLabel, CompatRefusalReason, u64)> {
    let counts = publish_refusals().lock().clone();
    let mut series: RefusalCounts = KNOWN_FAMILIES
        .iter()
        .flat_map(|family| {
            CompatRefusalReason::ALL
                .iter()
                .map(move |reason| ((CompatFamilyLabel::of(family), *reason), 0))
        })
        .collect();
    series.extend(counts);
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
