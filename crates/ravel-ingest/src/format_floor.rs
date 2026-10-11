//! The writer policy over recorded format floors (ADR-1746 decisions 3 to 5).
//!
//! A provisioning record's format floor for a family asserts that no live
//! object of that family below the floor exists for the (tenant, signal). A
//! writer keeps the assertion true by refusing to publish an object whose
//! `segment_format_version` is below it ([`FormatFloorPolicy::admit`]), and an
//! operator can turn that refusal into a counted warning with
//! `--write-below-format-floor`, which makes the floor false on purpose. The
//! policy compares against the version of the object being written, never the
//! build's newest version: a writer can write an older version than its
//! newest.
//!
//! Ingest checks on the record its [`crate::GenerationSwitch`] already caches
//! for routing; `ravel-maintain` reads the record before a compaction, erasure
//! rewrite or migration builds its parts. Both count into one
//! [`FormatFloorMetrics`]. At startup [`warn_floors_above_writer`] reports every
//! recorded floor above what the build can write, without refusing to start.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use futures::stream::{self, StreamExt};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantHash};

/// A format family a floor can be recorded for, one per signal whose objects
/// this repository writes. The lowercase name is the `family` string
/// `raise_format_floor` records and `ravel-cli maintain migrate` defaults to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatFamily {
    /// RSEG, the metrics segment format.
    Rseg,
    /// RLOG, the log segment format.
    Rlog,
    /// RSPAN, the span segment format.
    Rspan,
}

impl FormatFamily {
    /// Every family, in a fixed order.
    pub const ALL: [FormatFamily; 3] = [FormatFamily::Rseg, FormatFamily::Rlog, FormatFamily::Rspan];

    /// The family string a floor is recorded under.
    pub fn as_str(self) -> &'static str {
        match self {
            FormatFamily::Rseg => "rseg",
            FormatFamily::Rlog => "rlog",
            FormatFamily::Rspan => "rspan",
        }
    }

    /// The family of the objects written for `signal`, or `None` for a signal
    /// that has no format floor (the audit and internal signals).
    pub fn for_signal(signal: Signal) -> Option<FormatFamily> {
        match signal {
            Signal::Metrics => Some(FormatFamily::Rseg),
            Signal::Logs => Some(FormatFamily::Rlog),
            Signal::Spans => Some(FormatFamily::Rspan),
            _ => None,
        }
    }

    /// The newest version of this family the build can write: its format
    /// crate's `SUPPORTED_VERSIONS.newest()`.
    pub fn writable_version(self) -> u32 {
        match self {
            FormatFamily::Rseg => u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
            FormatFamily::Rlog => u32::from(ravel_logseg::footer::SUPPORTED_VERSIONS.newest()),
            FormatFamily::Rspan => u32::from(ravel_rspan::footer::SUPPORTED_VERSIONS.newest()),
        }
    }

    fn index(self) -> usize {
        match self {
            FormatFamily::Rseg => 0,
            FormatFamily::Rlog => 1,
            FormatFamily::Rspan => 2,
        }
    }
}

impl fmt::Display for FormatFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A publish refused because the object's version is below the bucket's
/// recorded floor for its family (ADR-1746 decision 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "refusing to publish a {family} object at format version {writing}: the bucket's recorded \
     {family} format floor is {floor} (ADR-1746 decision 4)"
)]
pub struct BelowFormatFloor {
    pub family: FormatFamily,
    pub floor: u32,
    pub writing: u32,
}

/// The process's format-floor counters, one slot per [`FormatFamily`].
#[derive(Debug, Default)]
pub struct FormatFloorMetrics {
    refused: [AtomicU64; 3],
    writes_below: [AtomicU64; 3],
    above_writer: [AtomicU64; 3],
}

/// One family's figures, as `/metrics` renders them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatFloorFamilySnapshot {
    pub family: FormatFamily,
    /// Publishes refused below the floor.
    pub refused: u64,
    /// Publishes written below the floor under the override.
    pub writes_below: u64,
    /// (tenant, signal) buckets whose floor the startup check found above the
    /// writable version.
    pub above_writer: u64,
}

impl FormatFloorMetrics {
    /// Publishes of `family` refused below the floor.
    pub fn refused(&self, family: FormatFamily) -> u64 {
        self.refused[family.index()].load(Ordering::Relaxed)
    }

    /// Publishes of `family` written below the floor under the override.
    pub fn writes_below(&self, family: FormatFamily) -> u64 {
        self.writes_below[family.index()].load(Ordering::Relaxed)
    }

    /// Buckets the startup check found with a `family` floor above the
    /// writable version.
    pub fn above_writer(&self, family: FormatFamily) -> u64 {
        self.above_writer[family.index()].load(Ordering::Relaxed)
    }

    fn set_above_writer(&self, family: FormatFamily, count: u64) {
        self.above_writer[family.index()].store(count, Ordering::Relaxed);
    }

    /// Every family's figures, in [`FormatFamily::ALL`] order.
    pub fn snapshot(&self) -> [FormatFloorFamilySnapshot; 3] {
        FormatFamily::ALL.map(|family| FormatFloorFamilySnapshot {
            family,
            refused: self.refused(family),
            writes_below: self.writes_below(family),
            above_writer: self.above_writer(family),
        })
    }
}

static GLOBAL_METRICS: LazyLock<Arc<FormatFloorMetrics>> =
    LazyLock::new(|| Arc::new(FormatFloorMetrics::default()));

/// The process-wide counters `/metrics` renders and every default
/// [`FormatFloorPolicy`] counts into.
pub fn global_metrics() -> Arc<FormatFloorMetrics> {
    Arc::clone(&GLOBAL_METRICS)
}

/// What [`FormatFloorPolicy::admit`] allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloorAdmission {
    /// No floor is recorded for the family, or the object is at or above it.
    AtOrAboveFloor,
    /// The object is below the floor and the override let it through. The
    /// caller logs a warning; the write is already counted.
    BelowFloorOverridden(BelowFormatFloor),
}

/// A writer role's answer to a publish below a recorded floor: refuse it
/// (the default), or write it under `--write-below-format-floor` (ADR-1746
/// decision 5). Floors are never lowered by either.
#[derive(Debug, Clone)]
pub struct FormatFloorPolicy {
    write_below_floor: bool,
    metrics: Arc<FormatFloorMetrics>,
}

impl Default for FormatFloorPolicy {
    /// Refuse, counting into [`global_metrics`].
    fn default() -> Self {
        FormatFloorPolicy::new(false, global_metrics())
    }
}

impl FormatFloorPolicy {
    /// A policy that writes below a floor when `write_below_floor` is set and
    /// refuses otherwise, counting into `metrics`.
    pub fn new(write_below_floor: bool, metrics: Arc<FormatFloorMetrics>) -> Self {
        FormatFloorPolicy {
            write_below_floor,
            metrics,
        }
    }

    /// Whether the override is on.
    pub fn write_below_floor(&self) -> bool {
        self.write_below_floor
    }

    /// The counters this policy counts into.
    pub fn metrics(&self) -> &Arc<FormatFloorMetrics> {
        &self.metrics
    }

    /// Decide one publish of a `family` object at version `writing` into a
    /// bucket whose recorded floor for that family is `floor` (`None`: no
    /// floor recorded). Counts a refusal or an overridden write.
    pub fn admit(
        &self,
        family: FormatFamily,
        floor: Option<u32>,
        writing: u32,
    ) -> Result<FloorAdmission, BelowFormatFloor> {
        let Some(floor) = floor.filter(|floor| writing < *floor) else {
            return Ok(FloorAdmission::AtOrAboveFloor);
        };
        let below = BelowFormatFloor {
            family,
            floor,
            writing,
        };
        if self.write_below_floor {
            self.metrics.writes_below[family.index()].fetch_add(1, Ordering::Relaxed);
            Ok(FloorAdmission::BelowFloorOverridden(below))
        } else {
            self.metrics.refused[family.index()].fetch_add(1, Ordering::Relaxed);
            Err(below)
        }
    }
}

/// The WARN line of an ingest flush refused at flush open below a floor.
pub(crate) fn warn_flush_refused(
    signal: Signal,
    shard: u32,
    tenant: TenantHash,
    refused: &BelowFormatFloor,
) {
    tracing::warn!(
        signal = signal.key_prefix(),
        shard,
        tenant_hash = %tenant.to_hex(),
        family = refused.family.as_str(),
        floor = refused.floor,
        writing = refused.writing,
        "ravel-ingest: refusing a flush that would publish an object below the tenant's recorded \
         format floor; its rows are dropped and a strict write is answered with the refusal \
         (ADR-1746 decision 4)"
    );
}

/// The WARN line of an ingest flush written below a floor under
/// `--write-below-format-floor`.
pub(crate) fn warn_flush_written_below(
    signal: Signal,
    shard: u32,
    tenant: TenantHash,
    below: &BelowFormatFloor,
) {
    tracing::warn!(
        signal = signal.key_prefix(),
        shard,
        tenant_hash = %tenant.to_hex(),
        family = below.family.as_str(),
        floor = below.floor,
        writing = below.writing,
        "ravel-ingest: writing a flush below the tenant's recorded format floor under \
         --write-below-format-floor; the floor is now contradicted (ADR-1746 decision 5)"
    );
}

/// One (tenant, signal) whose recorded floor the startup check found above
/// the version the build writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorAboveWriter {
    pub tenant: TenantHash,
    pub signal: Signal,
    pub family: FormatFamily,
    pub floor: u32,
    pub writable: u32,
}

/// The signals whose provisioning records carry a floor a writer checks.
const FLOORED_SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Spans];

/// Provisioning-record reads the startup check keeps in flight at once.
const STARTUP_READ_CONCURRENCY: usize = 16;

/// The startup check (ADR-1746 decision 3): read the provisioning record of
/// every (tenant, signal) in `tenants` x {metrics, logs, spans}, one GET each,
/// and warn once per (tenant, signal, family) whose recorded floor is above
/// the family's [`FormatFamily::writable_version`], naming both numbers.
/// Sets `metrics`' `above_writer` gauge for each family to the count of such
/// buckets and returns them.
///
/// It never fails: a record that cannot be read is logged and left out of the
/// gauge, and the caller starts either way. `role` names the writer role in
/// the log line; `write_below_floor` says whether this process will refuse or
/// write those buckets' objects.
pub async fn warn_floors_above_writer(
    store: &dyn ObjectStoreBackend,
    tenants: &[TenantHash],
    role: &'static str,
    write_below_floor: bool,
    metrics: &FormatFloorMetrics,
) -> Vec<FloorAboveWriter> {
    let pairs = tenants
        .iter()
        .flat_map(|tenant| FLOORED_SIGNALS.map(|signal| (*tenant, signal)));
    let reads = stream::iter(pairs)
        .map(|(tenant, signal)| async move {
            let family = FormatFamily::for_signal(signal)?;
            match ravel_catalog::current_floor_from_store(store, &tenant, signal, family.as_str())
                .await
            {
                Ok(floor) => floor
                    .filter(|floor| *floor > family.writable_version())
                    .map(|floor| FloorAboveWriter {
                        tenant,
                        signal,
                        family,
                        floor,
                        writable: family.writable_version(),
                    }),
                Err(err) => {
                    tracing::warn!(
                        role,
                        tenant_hash = %tenant.to_hex(),
                        signal = signal.key_prefix(),
                        family = family.as_str(),
                        error = %err,
                        "format floor startup check could not read the provisioning record; \
                         the write path still checks the floor before every publish"
                    );
                    None
                }
            }
        })
        .buffer_unordered(STARTUP_READ_CONCURRENCY);
    let mut found: Vec<FloorAboveWriter> = reads.filter_map(std::future::ready).collect().await;
    found.sort_by_key(|f| (f.tenant.0, f.family.index()));
    for f in &found {
        tracing::warn!(
            role,
            tenant_hash = %f.tenant.to_hex(),
            signal = f.signal.key_prefix(),
            family = f.family.as_str(),
            floor = f.floor,
            writable_version = f.writable,
            write_below_format_floor = write_below_floor,
            "a recorded format floor is above the newest version this build writes; the process \
             starts and serves reads, and {} (ADR-1746)",
            if write_below_floor {
                "writes this bucket's objects below the floor, which contradicts it"
            } else {
                "refuses to publish this family's objects into this bucket"
            }
        );
    }
    for family in FormatFamily::ALL {
        let count = found.iter().filter(|f| f.family == family).count();
        metrics.set_above_writer(family, u64::try_from(count).unwrap_or(u64::MAX));
    }
    found
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn policy(write_below_floor: bool) -> FormatFloorPolicy {
        FormatFloorPolicy::new(write_below_floor, Arc::new(FormatFloorMetrics::default()))
    }

    #[test]
    fn no_floor_and_a_floor_at_the_version_admit() {
        let p = policy(false);
        assert_eq!(
            p.admit(FormatFamily::Rseg, None, 7),
            Ok(FloorAdmission::AtOrAboveFloor)
        );
        assert_eq!(
            p.admit(FormatFamily::Rseg, Some(7), 7),
            Ok(FloorAdmission::AtOrAboveFloor)
        );
        assert_eq!(p.metrics().refused(FormatFamily::Rseg), 0);
    }

    /// The comparison is against the version of the object being written. A
    /// writer whose newest version meets the floor still refuses an object it
    /// writes at an older version: with the floor at the build's newest RSEG
    /// version, an object one below it is refused. A check comparing
    /// `SUPPORTED_VERSIONS.newest()` instead admits it, so the `Err` assertion
    /// below is the line that flips.
    #[test]
    fn a_writer_below_its_newest_version_is_refused_by_a_floor_at_its_newest() {
        let p = policy(false);
        let newest = FormatFamily::Rseg.writable_version();
        assert_eq!(
            p.admit(FormatFamily::Rseg, Some(newest), newest - 1),
            Err(BelowFormatFloor {
                family: FormatFamily::Rseg,
                floor: newest,
                writing: newest - 1,
            })
        );
        assert_eq!(p.metrics().refused(FormatFamily::Rseg), 1);
        assert_eq!(p.metrics().refused(FormatFamily::Rlog), 0);
    }

    #[test]
    fn the_override_writes_below_the_floor_and_counts_it() {
        let p = policy(true);
        let below = BelowFormatFloor {
            family: FormatFamily::Rlog,
            floor: 9,
            writing: 5,
        };
        assert_eq!(
            p.admit(FormatFamily::Rlog, Some(9), 5),
            Ok(FloorAdmission::BelowFloorOverridden(below))
        );
        assert_eq!(p.metrics().writes_below(FormatFamily::Rlog), 1);
        assert_eq!(p.metrics().refused(FormatFamily::Rlog), 0);
    }

    async fn provision(store: &dyn ObjectStoreBackend, tenant: TenantHash, signal: Signal) {
        ravel_catalog::validate_or_adopt(
            store,
            &tenant,
            signal,
            1,
            1,
            ravel_catalog::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision");
    }

    async fn raise(store: &dyn ObjectStoreBackend, tenant: TenantHash, family: FormatFamily, v: u32) {
        let signal = match family {
            FormatFamily::Rseg => Signal::Metrics,
            FormatFamily::Rlog => Signal::Logs,
            FormatFamily::Rspan => Signal::Spans,
        };
        ravel_catalog::raise_format_floor(
            store,
            &tenant,
            signal,
            family.as_str(),
            v,
            ravel_catalog::FloorBasis {
                observed_entries: 0,
                observed_newest_created_unix_ns: 0,
                observed_shards: 1,
            },
            "test",
            2,
        )
        .await
        .expect("raise floor");
    }

    /// The startup check warns once per (tenant, signal, family) whose floor is
    /// above the writable version, naming both numbers, and counts it in the
    /// gauge; a floor at the writable version, a family with no floor and a
    /// tenant with no record produce neither.
    #[tokio::test]
    async fn the_startup_check_warns_and_gauges_only_floors_above_the_writer() {
        let store = ravel_object_store::memory::MemoryStore::new();
        let above = TenantHash([1; 16]);
        let at = TenantHash([2; 16]);
        let absent = TenantHash([3; 16]);
        for signal in FLOORED_SIGNALS {
            provision(&store, above, signal).await;
            provision(&store, at, signal).await;
        }
        let rseg = FormatFamily::Rseg.writable_version();
        let rspan = FormatFamily::Rspan.writable_version();
        raise(&store, above, FormatFamily::Rseg, rseg + 1).await;
        raise(&store, above, FormatFamily::Rspan, rspan + 2).await;
        raise(&store, at, FormatFamily::Rseg, rseg).await;
        let metrics = FormatFloorMetrics::default();
        let warns = crate::generation::WarnCapture::default();
        let _guard = warns.install();

        let found =
            warn_floors_above_writer(&store, &[above, at, absent], "ingest", false, &metrics).await;

        assert_eq!(
            found,
            vec![
                FloorAboveWriter {
                    tenant: above,
                    signal: Signal::Metrics,
                    family: FormatFamily::Rseg,
                    floor: rseg + 1,
                    writable: rseg,
                },
                FloorAboveWriter {
                    tenant: above,
                    signal: Signal::Spans,
                    family: FormatFamily::Rspan,
                    floor: rspan + 2,
                    writable: rspan,
                },
            ]
        );
        assert_eq!(
            warns.count("a recorded format floor is above the newest version this build writes"),
            2,
            "one warning per bucket above the writer, none for the floor at it"
        );
        assert_eq!(metrics.above_writer(FormatFamily::Rseg), 1);
        assert_eq!(metrics.above_writer(FormatFamily::Rlog), 0);
        assert_eq!(metrics.above_writer(FormatFamily::Rspan), 1);
    }

    #[test]
    fn families_map_from_their_signals_and_write_their_crates_newest_version() {
        assert_eq!(
            FormatFamily::for_signal(Signal::Metrics),
            Some(FormatFamily::Rseg)
        );
        assert_eq!(
            FormatFamily::for_signal(Signal::Logs),
            Some(FormatFamily::Rlog)
        );
        assert_eq!(
            FormatFamily::for_signal(Signal::Spans),
            Some(FormatFamily::Rspan)
        );
        assert_eq!(FormatFamily::for_signal(Signal::Audit), None);
        assert_eq!(
            FormatFamily::Rseg.writable_version(),
            u32::from(crate::SEGMENT_FORMAT_VERSION)
        );
        assert_eq!(
            FormatFamily::Rlog.writable_version(),
            u32::from(crate::LOG_SEGMENT_FORMAT_VERSION)
        );
        assert_eq!(
            FormatFamily::Rspan.writable_version(),
            u32::from(crate::SPAN_SEGMENT_FORMAT_VERSION)
        );
    }
}
