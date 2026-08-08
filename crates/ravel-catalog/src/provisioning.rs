//! Durable, startup-checked `shard_count` (ADR-0050 section 5, EC5).
//!
//! Each (tenant, signal) carries an immutable provisioning record at
//! `t/<tenant_hash>/<sig>/prov` recording the `shard_count` its data was
//! written under. `shard_count` lives only in process config otherwise, and
//! resolution iterates `0..shard_count` (catalog.rs), so a process configured
//! with a lower value than the data was written under silently omits every
//! series in the missing shards. This record turns that silent truncation into
//! a loud refusal: every ingest, catalog, and maintain touch validates the
//! configured value against the record before acting.
//!
//! This module is the one shared implementation the three consumers named in
//! ADR-0050 section 5 call: ingest-router first write, catalog first resolve,
//! and the maintain per-tenant loop. [`validate_or_adopt`] handles all three
//! ADR scenarios (no record + no data; no record + pre-ADR data; record
//! present) so no consumer reimplements the decision.
//!
//! Scope note: `shard_count` is immutable per (tenant, signal) in this
//! decision. Resharding (a shard-epoch map) is deferred to its own ADR (epic
//! EK); nothing here changes a recorded value, only refuses when config and
//! record disagree.

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{Signal, TenantHash};

/// Format floor written into every provisioning record this build emits, and
/// the highest record version it understands. A record declaring a higher
/// version is refused rather than misread under this layout (ADR-0050 section
/// 5), matching the `sys/tenancy` marker's version guard.
pub const PROVISIONING_FORMAT_VERSION: u32 = 1;

/// Object key for a (tenant, signal) provisioning record: `t/<hex>/<sig>/prov`.
/// Under the tenant's own prefix, alongside its `l0/` and `c/` shard data, not
/// a bucket-root `sys/` object (ADR-0050 section 5).
pub fn provisioning_key(tenant_hash: &TenantHash, signal: Signal) -> String {
    format!("t/{}/{}/prov", tenant_hash.to_hex(), signal.key_prefix())
}

/// Prefix under which one (tenant, signal)'s L0 (uncompacted) shard directories
/// live. Delimiter-listing it yields one common prefix per present shard.
fn l0_prefix(tenant_hash: &TenantHash, signal: Signal) -> String {
    format!("t/{}/{}/l0/", tenant_hash.to_hex(), signal.key_prefix())
}

/// Prefix under which one (tenant, signal)'s commit/compaction records live,
/// one shard directory each. Listed alongside `l0/` so a shard that has only
/// commit records (its L0 data already compacted and swept) is still observed.
fn commit_prefix(tenant_hash: &TenantHash, signal: Signal) -> String {
    format!("t/{}/{}/c/", tenant_hash.to_hex(), signal.key_prefix())
}

/// Map a domain [`Signal`] to the persisted `sys.v1.Signal` enum. The record
/// stores the signal so a misfiled record (right key, wrong body) is caught on
/// decode, the same defense commit records carry (ADR-0010 §10).
fn to_sys_signal(signal: Signal) -> sysproto::Signal {
    match signal {
        Signal::Metrics => sysproto::Signal::Metrics,
        Signal::Logs => sysproto::Signal::Logs,
        Signal::Spans => sysproto::Signal::Spans,
        Signal::Profiles => sysproto::Signal::Profiles,
        Signal::Alerts => sysproto::Signal::Alerts,
        Signal::Audit => sysproto::Signal::Audit,
    }
}

/// A typed provisioning failure. Every variant is fatal to the touch that
/// raised it: a static tenant's mismatch refuses startup, a dynamic tenant's
/// fails that one request (ADR-0050 section 5). None warn and continue.
#[derive(Debug, thiserror::Error)]
pub enum ProvisioningError {
    #[error("object store error on provisioning record {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("provisioning record {key:?} could not be decoded: {source}")]
    Decode {
        key: String,
        #[source]
        source: prost::DecodeError,
    },
    #[error(
        "provisioning record {key:?} declares format_version {got}, but this build only \
         understands version {PROVISIONING_FORMAT_VERSION}: refusing rather than misread a future \
         record format"
    )]
    UnsupportedVersion { key: String, got: u32 },
    #[error(
        "provisioning record {key:?} is misfiled: it records {field} {actual}, but the key it was \
         read under expects {expected}"
    )]
    CorruptRecord {
        key: String,
        field: &'static str,
        expected: String,
        actual: String,
    },
    /// The record exists and its `shard_count` disagrees with the configured
    /// value. This is the S1-E6 refusal (ADR-0050 section 5): a `FieldMismatch`
    /// naming the record, tenant, signal, expected, and actual.
    #[error(
        "provisioning record {key:?} for tenant {tenant_hash} signal {signal} records \
         shard_count {actual}, but this process is configured for {expected}: refusing to resolve \
         over a subset of shards (ADR-0050 section 5, S1-E6)"
    )]
    ShardCountMismatch {
        key: String,
        tenant_hash: String,
        signal: &'static str,
        expected: u32,
        actual: u32,
    },
    /// Pre-ADR data has a shard index at or above the configured count, so the
    /// configured value is provably hiding data. Adoption writes nothing and
    /// refuses (ADR-0050 section 5): adopting a value just proven wrong would
    /// bless the truncation this record exists to prevent.
    #[error(
        "cannot adopt tenant {tenant_hash} signal {signal}: existing data has shard index \
         {observed_shard}, at or above the configured shard_count {configured}, so the configured \
         value would hide shards {configured}..={observed_shard}. Refusing to write a provisioning \
         record (ADR-0050 section 5)"
    )]
    AdoptionWouldHideData {
        tenant_hash: String,
        signal: &'static str,
        configured: u32,
        observed_shard: u32,
    },
    /// The record's append-only shard-generation history (ADR-0052 section 1)
    /// violates a structural invariant. Fail closed, same discipline as every
    /// other corrupt-input path: a malformed history could route or scan over
    /// the wrong shard set. `defect` names the exact rule broken.
    #[error(
        "provisioning record {key:?} has a corrupt shard-generation history: {defect} \
         (ADR-0052 section 1)"
    )]
    CorruptGenerations {
        key: String,
        defect: GenerationDefect,
    },
    /// `provision reshard` was asked to append to a (tenant, signal) that has
    /// no provisioning record yet. A reshard extends an existing record's
    /// history; a tenant with no record has nothing to reshard (its first
    /// write pins generation 0).
    #[error(
        "cannot reshard tenant {tenant_hash} signal {signal}: no provisioning record exists at \
         {key:?}. A reshard appends to an existing record; the record is created on the tenant's \
         first ingest for this signal (ADR-0050 section 5, ADR-0052 section 1)"
    )]
    NoRecordToReshard {
        key: String,
        tenant_hash: String,
        signal: &'static str,
    },
    /// The reshard's computed `activation_hour` is not strictly in the future
    /// against the server clock at append time (ADR-0052 section 3). Rejected
    /// so a live writer either observes the new generation before it activates
    /// or has already fail-stopped on record staleness.
    #[error(
        "reshard activation_hour {activation_hour} is not in the future (current hour {now_hour}): \
         the lead time must place activation strictly ahead of the append (ADR-0052 section 3); \
         refusing to append"
    )]
    ActivationInPast { activation_hour: u32, now_hour: u32 },
    /// The reshard's new `shard_count` equals the currently-active generation's
    /// count, so the append would be a no-op "reshard" (ADR-0052 section 1:
    /// adjacent generations' counts must differ).
    #[error(
        "reshard to shard_count {shard_count} is a no-op: it equals the current generation's count; \
         adjacent generations must differ (ADR-0052 section 1); refusing to append"
    )]
    ReshardSameCount { shard_count: u32 },
    /// The reshard's new `shard_count` is outside the legal `1..=10000` range
    /// the 4-digit shard key field caps it to (ADR-0052 section 1/7).
    #[error(
        "reshard shard_count {shard_count} is out of range: it must be 1..=10000 (the 4-digit \
         shard key field caps it); refusing to append"
    )]
    ReshardCountOutOfRange { shard_count: u32 },
    /// A concurrent append moved the record's version between this reshard's
    /// read and its CasVersion write (ADR-0052 section 1, the EC4 set_gc_config
    /// pattern). The loser re-reads rather than silently overwriting the winner.
    #[error(
        "a concurrent reshard changed provisioning record {key:?} since this one read it \
         (CasVersion precondition failed): re-read and retry rather than overwrite the other \
         append"
    )]
    ReshardCasConflict { key: String },
    /// The record's append-only format-floor history (ADR-0066 decision 3)
    /// violates a structural invariant. Fail closed, same discipline as the
    /// generation history: a non-monotonic-per-family floor list could assert a
    /// false "no old object exists" and license deleting a still-needed reader.
    /// `defect` names the exact rule broken.
    #[error(
        "provisioning record {key:?} has a corrupt format-floor history: {defect} \
         (ADR-0066 decision 3)"
    )]
    CorruptFloors { key: String, defect: FloorDefect },
    /// `raise_format_floor` was called with an empty `family`. A floor governs a
    /// named format family; an empty name governs nothing.
    #[error("cannot raise a format floor for an empty family (ADR-0066 decision 3)")]
    FloorFamilyEmpty,
    /// `raise_format_floor` was called with a `family` containing an uppercase
    /// character. `family` is a lowercase format-family id and lookup is an
    /// exact string match, so an uppercase raise would silently create a
    /// second, independent family rather than extending the intended one.
    #[error(
        "cannot raise a format floor for family {0:?}: family must be lowercase \
         (ADR-0066 decision 3)"
    )]
    FloorFamilyNotLowercase(String),
    /// `raise_format_floor` was asked to raise a family's floor to a value at or
    /// below its current recorded floor. Floors are raised only, never lowered,
    /// and a re-raise to the same value records nothing new; both are refused so
    /// every appended entry is a genuine advance (mirroring the generation
    /// history's same-count refusal). `current` is 0 when no floor has ever been
    /// raised for the family.
    #[error(
        "cannot raise format floor for family {family:?} to {requested} in record {key:?}: it is \
         not strictly above the current floor {current} (floors are raised only, never lowered; \
         ADR-0066 decision 3)"
    )]
    FloorNotAboveCurrent {
        key: String,
        family: String,
        requested: u32,
        current: u32,
    },
    /// `raise_format_floor` was called on a (tenant, signal) that has no
    /// provisioning record yet. A floor asserts a fact about a (tenant, signal)'s
    /// live objects; a (tenant, signal) with no record has no pinned shard_count
    /// and nothing for a floor to govern. The record is created on the tenant's
    /// first ingest for this signal (ADR-0050 section 5), exactly as a reshard
    /// requires ([`ProvisioningError::NoRecordToReshard`]).
    #[error(
        "cannot raise a format floor for tenant {tenant_hash} signal {signal}: no provisioning \
         record exists at {key:?}. A floor extends an existing record; the record is created on \
         the tenant's first ingest for this signal (ADR-0066 decision 3)"
    )]
    NoRecordForFloor {
        key: String,
        tenant_hash: String,
        signal: &'static str,
    },
    /// A concurrent write moved the record's version between this floor raise's
    /// read and its CasVersion write (ADR-0066 decision 3, the
    /// [`append_generation`] CAS-conflict pattern). The loser re-reads rather
    /// than silently overwriting the winner.
    #[error(
        "a concurrent write changed provisioning record {key:?} since this floor raise read it \
         (CasVersion precondition failed): re-read and retry rather than overwrite the other write"
    )]
    FloorCasConflict { key: String },
}

/// The exact structural rule a corrupt shard-generation history broke
/// (ADR-0052 section 1). Each is a fail-closed decode error, never a silent
/// normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationDefect {
    /// `generations[0].shard_count` disagrees with the scalar `shard_count`.
    ScalarMismatch,
    /// Generation numbers are not a dense 0-based sequence (0, 1, 2, ...).
    NotDense,
    /// `activation_hour` is not strictly increasing across the list.
    ActivationNotIncreasing,
    /// Two adjacent generations have equal `shard_count` (a no-op reshard).
    AdjacentEqualCount,
    /// A generation's `shard_count` is outside `1..=10000`.
    CountOutOfRange,
}

impl std::fmt::Display for GenerationDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            GenerationDefect::ScalarMismatch => {
                "generations[0].shard_count does not equal the scalar shard_count"
            }
            GenerationDefect::NotDense => "generation numbers are not dense 0-based (0, 1, 2, ...)",
            GenerationDefect::ActivationNotIncreasing => {
                "activation_hour is not strictly increasing across generations"
            }
            GenerationDefect::AdjacentEqualCount => {
                "adjacent generations have equal shard_count (a no-op reshard)"
            }
            GenerationDefect::CountOutOfRange => "a generation shard_count is outside 1..=10000",
        };
        f.write_str(s)
    }
}

/// The exact structural rule a corrupt format-floor history broke (ADR-0066
/// decision 3). Each is a fail-closed decode error, never a silent
/// normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloorDefect {
    /// An entry has an empty `family` string (a floor governs a named family).
    EmptyFamily,
    /// An entry has `floor_version` 0. A floor of 0 asserts nothing ("no object
    /// below version 0"), and a valid raise is always strictly above the current
    /// (or implicit-zero) floor, so a stored 0 can never be produced by a
    /// legitimate append.
    ZeroFloor,
    /// A family's floors are not strictly increasing in append order: a later
    /// entry for a family has a `floor_version` at or below an earlier entry's
    /// for the same family. Floors are raised only, never lowered or repeated.
    NotIncreasing,
    /// An entry's `family` contains an uppercase ASCII character. `family`
    /// lookup is an exact string match ([`current_floor`]), so `"RSEG"` and
    /// `"rseg"` would silently be two independent families rather than one
    /// mistyped raise -- enforced here rather than left as a documented-only
    /// convention, since a case mismatch is easy to introduce by hand and hard
    /// to notice (it fails safe, as a missing floor, not as a wrong one, but
    /// it still means the wrong record answers "has this family's floor been
    /// raised").
    NotLowercase,
}

impl std::fmt::Display for FloorDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FloorDefect::EmptyFamily => "a format-floor entry has an empty family",
            FloorDefect::ZeroFloor => "a format-floor entry has floor_version 0 (a vacuous floor)",
            FloorDefect::NotIncreasing => {
                "a family's floor_version is not strictly increasing across the history \
                 (floors are raised only)"
            }
            FloorDefect::NotLowercase => {
                "a format-floor entry's family contains an uppercase character (family is a \
                 lowercase format-family id)"
            }
        };
        f.write_str(s)
    }
}

/// The inclusive upper bound on `shard_count`, set by the 4-digit zero-padded
/// shard key field (ADR-0052 section 7; `l0/0000/`..`l0/9999/`).
pub const MAX_SHARD_COUNT: u32 = 10_000;

/// The read-side scan slack window `S`, in ingest hours (ADR-0052 sections 3
/// and 4). A flush pins its ingest-hour bucket `h` at wall-clock time `t`, but
/// its records were routed up to `max_flush_delay` earlier and the flush lives
/// at most `max_flush_lifetime`, plus inter-writer clock skew; ADR-0052 section
/// 3 defines `S = ceil(max_flush_delay + max_flush_lifetime + max tolerated
/// clock skew)` in hours. A straggler routed under a retiring (larger) count
/// into an early hour of the successor generation therefore lands in a shard
/// index above the successor's range, and the scan rule ([`scan_count`]) must
/// keep the retiring generation's count in the scan set for `S` hours past the
/// successor's activation so that straggler is still found. With today's ingest
/// defaults (`max_flush_delay` 500ms + `max_flush_lifetime` 3600s), `S = 2`.
///
/// This is a manually-chosen constant local to `ravel-catalog`, deliberately
/// not a cross-crate reference to `ravel-ingest`'s flush config:
/// `ravel-catalog` must not depend on `ravel-ingest` (the dependency runs the
/// other way). If ingest's flush bounds ever change materially, this constant
/// must be revisited in lockstep; the derivation above is recorded so that
/// review is possible.
pub const DEFAULT_SCAN_SLACK_HOURS: u32 = 2;

/// Nanoseconds per unix hour, the unit `activation_hour` and `ingest_hour_bucket`
/// count in. Held here so the reshard append can derive the current hour from a
/// `now_ns` reading without depending on ravel-ingest.
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// One entry of a provisioning record's append-only shard-generation history,
/// decoded into a plain struct so the routing rule ([`active_shard_count`]) and
/// the router live switch never touch the proto type (ADR-0052 section 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardGeneration {
    /// Dense, 0-based generation number.
    pub generation: u32,
    /// The shard count this generation routes and scans under (`1..=10000`).
    pub shard_count: u32,
    /// The unix hour at and after which ingest routes with this generation's
    /// count.
    pub activation_hour: u32,
    /// Audit only: when this generation was appended.
    pub appended_unix_ns: i64,
}

/// The write-side routing rule (ADR-0052 section 2): the shard count of the
/// latest generation whose `activation_hour` is `<= hour`. `generations` must be
/// the normalized, validated history [`read_generations`] returns (non-empty,
/// dense, activation-increasing), so generation 0 (activation_hour 0) always
/// covers every hour and the result is well-defined.
///
/// A pure function with no I/O: the router calls it per flush against its cached
/// view, and EK2's read side will call the same rule to derive per-hour scan
/// sets. Returns generation 0's count for an empty slice as a defensive floor
/// (the divisor is never zero), but a validated history is never empty.
pub fn active_shard_count(generations: &[ShardGeneration], hour: u32) -> u32 {
    generations
        .iter()
        .filter(|g| g.activation_hour <= hour)
        .max_by_key(|g| g.generation)
        .or_else(|| generations.first())
        .map(|g| g.shard_count)
        .unwrap_or(1)
}

/// The read-side scan rule (ADR-0052 section 4): the size of the shard-index
/// domain a reader must scan for ingest-hour bucket `hour`. The read-side
/// sibling of [`active_shard_count`] (the write-side rule): same generation
/// history, different formula. Normatively,
///
/// ```text
/// scan_count(h) = max over generations g of count(g)
///                 where activation_hour(g) <= h
///                   and h < activation_hour(g+1) + S
///                 (activation_hour of a nonexistent successor = infinity)
/// ```
///
/// where `S` is `slack_hours` ([`DEFAULT_SCAN_SLACK_HOURS`]). A generation is
/// in the scan set for hour `h` once it has activated (`activation_hour(g) <=
/// h`) and until `S` hours past its successor's activation. The latest
/// activated generation always qualifies (its nonexistent-or-future successor
/// makes the upper bound infinite for it), so on an increase the wider new
/// range already covers a straggler routed under the old, smaller count; on a
/// decrease the retiring, larger count stays in the set for `S` hours so a
/// straggler routed under it into an early successor hour is still found.
///
/// A pure function with no I/O, called per hour by the resolve fan-out
/// (`catalog.rs`), the fold's bucket enumeration (`fold.rs`), and the maintain
/// scan loop (`services/ravel-server`). `generations` must be the normalized,
/// validated history [`read_generations`] returns (non-empty, dense,
/// activation-increasing, generation 0 at `activation_hour` 0). Returns
/// generation 0's count as a defensive floor for an empty slice (never a zero
/// divisor / empty scan range), but a validated history is never empty and the
/// floor branch is unreachable for one.
pub fn scan_count(generations: &[ShardGeneration], hour: u32, slack_hours: u32) -> u32 {
    let mut max_count = 0u32;
    for (idx, g) in generations.iter().enumerate() {
        if g.activation_hour > hour {
            continue;
        }
        // A nonexistent successor activates at infinity, so the last generation
        // (and any generation whose successor has not yet activated) has no
        // upper bound. `u64` keeps `activation + slack` from overflowing `u32`.
        let successor_activation = generations
            .get(idx + 1)
            .map_or(u64::MAX, |s| u64::from(s.activation_hour));
        let upper = successor_activation.saturating_add(u64::from(slack_hours));
        if u64::from(hour) < upper {
            max_count = max_count.max(g.shard_count);
        }
    }
    if max_count == 0 {
        // Only reachable for an empty (never-validated) slice; a validated
        // history always has generation 0 at activation_hour 0, which every
        // hour satisfies, so the latest activated generation always qualifies.
        generations.first().map_or(1, |g| g.shard_count)
    } else {
        max_count
    }
}

/// The union shard-index domain a reader must scan for *every* ingest-hour
/// bucket in `[start_hour, end_hour]` (inclusive): the range generalization of
/// [`scan_count`]. Used by the prefix-scan traversal path (ADR-0056,
/// `catalog::list_window_by_prefix`), which lists one per-shard subtree at a
/// time and so needs a single shard bound covering the whole window, not a
/// per-hour count.
///
/// A generation `g`'s per-hour scan-set is the half-open hour interval
///
/// ```text
/// [ activation_hour(g), successor_activation(g) + S )
/// ```
///
/// exactly the interval [`scan_count`] checks per hour (`activation_hour(g) <=
/// h` and `h < successor_activation(g) + S`, with a nonexistent successor
/// activating at infinity). `g` contributes to the range result iff that
/// interval intersects `[start_hour, end_hour]`, i.e.
///
/// ```text
/// activation_hour(g) <= end_hour   &&   start_hour < successor_activation(g) + S
/// ```
///
/// and the result is the max `shard_count` over every contributing generation.
/// This equals `max over h in [start_hour, end_hour] of scan_count(h)` (the
/// max of a max over hours is the max over the generations relevant to any of
/// those hours), but is computed in O(generations), never O(hours): the whole
/// point of the prefix path is to avoid work that scales with window width.
///
/// `generations` must be the normalized, validated history
/// [`read_generations`] returns (non-empty, dense, activation-increasing,
/// generation 0 at `activation_hour` 0). Returns generation 0's count as a
/// defensive floor for an empty slice, matching [`scan_count`]; a validated
/// history always has the latest generation activated at or before `end_hour`
/// with an unbounded (or wider-than-`start_hour`) successor bound, so the floor
/// branch is unreachable for one.
pub fn max_scan_count_over_range(
    generations: &[ShardGeneration],
    start_hour: u32,
    end_hour: u32,
    slack_hours: u32,
) -> u32 {
    let mut max_count = 0u32;
    for (idx, g) in generations.iter().enumerate() {
        // Lower end of g's scan interval past the range: g has not activated
        // by any hour in the range, so it contributes nothing.
        if g.activation_hour > end_hour {
            continue;
        }
        // Upper end of g's scan interval (exclusive): its successor's
        // activation plus the slack window; a nonexistent successor activates
        // at infinity. `u64` keeps `activation + slack` from overflowing `u32`,
        // matching `scan_count`.
        let successor_activation = generations
            .get(idx + 1)
            .map_or(u64::MAX, |s| u64::from(s.activation_hour));
        let upper = successor_activation.saturating_add(u64::from(slack_hours));
        // Interval `[activation_hour(g), upper)` intersects `[start_hour,
        // end_hour]` iff its lower end is `<= end_hour` (checked above) and its
        // upper end is `> start_hour`.
        if u64::from(start_hour) < upper {
            max_count = max_count.max(g.shard_count);
        }
    }
    if max_count == 0 {
        // Only reachable for an empty (never-validated) slice, exactly as in
        // `scan_count`: for a validated history the latest generation with
        // `activation_hour <= end_hour` always qualifies (its successor, if
        // any, activates strictly after `end_hour >= start_hour`, so the upper
        // bound exceeds `start_hour`).
        generations.first().map_or(1, |g| g.shard_count)
    } else {
        max_count
    }
}

/// The fold-time fan-out ceiling (ADR-0052 section 5): `max { count(g) :
/// activation_hour(g) <= watermark_hour }`, with **no** slack-window upper
/// bound. This is the value a fold stamps into `SnapshotHead.shard_count` /
/// `SnapshotPartHeader.shard_count`, the union shard range a rebuild must list,
/// and the value a reader must reproduce to validate a head. Both the writer
/// (`fold::fold_shard_ceiling`) and the reader
/// (`snapshot_resolve::head_generations_acceptable`) call this one function so
/// the two can never diverge (the fix for the read/write ceiling disagreement:
/// a decrease followed by a fold past the slack window used to have the writer
/// stamp the pre-decrease count while the reader recomputed the narrower
/// post-slack `scan_count`, hard-failing every query permanently).
///
/// Unlike [`scan_count`] -- a per-hour scan-set size that drops a retired
/// generation once its slack window closes -- the ceiling is monotonic in the
/// watermark: once a generation has activated at or before the watermark, its
/// count is included forever. A HEAD summarizes every hour up to its watermark,
/// including the hours in which a since-retired generation was the widest, so
/// its parts already carry that generation's shards and the reader must expect
/// the wider ceiling. The slack window never pulls in a generation whose
/// activation is past the watermark, so no upper bound is needed here.
///
/// `generations` must be the normalized, validated history [`read_generations`]
/// returns (non-empty, generation 0 at `activation_hour` 0). Returns 1 as a
/// defensive floor for an empty (never-validated) slice, matching
/// [`scan_count`]; a validated history always has generation 0 active for every
/// hour, so the floor branch is unreachable for one.
pub fn shard_ceiling(generations: &[ShardGeneration], watermark_hour: u32) -> u32 {
    generations
        .iter()
        .filter(|g| g.activation_hour <= watermark_hour)
        .map(|g| g.shard_count)
        .max()
        .unwrap_or(1)
}

/// Decode and validate a record's shard-generation history into the normalized
/// form the routing rule consumes (ADR-0052 section 1). An empty `generations`
/// list (every record EC5 wrote) becomes the single implicit generation 0 with
/// `activation_hour = 0` and the scalar `shard_count`, so a pre-ADR-0052 record
/// decodes identically before and after this change. A non-empty list is
/// validated fail-closed: dense 0-based generations, strictly increasing
/// `activation_hour`, adjacent counts differing, every count in `1..=10000`, and
/// `generations[0].shard_count == shard_count`. Any violation is a typed
/// [`ProvisioningError::CorruptGenerations`], never a panic or a silent fix.
pub fn read_generations(
    record: &sysproto::ProvisioningRecord,
    key: &str,
) -> Result<Vec<ShardGeneration>, ProvisioningError> {
    if record.generations.is_empty() {
        if !(1..=MAX_SHARD_COUNT).contains(&record.shard_count) {
            let corrupt = |defect: GenerationDefect| ProvisioningError::CorruptGenerations {
                key: key.to_string(),
                defect,
            };
            return Err(corrupt(GenerationDefect::CountOutOfRange));
        }
        return Ok(vec![ShardGeneration {
            generation: 0,
            shard_count: record.shard_count,
            activation_hour: 0,
            appended_unix_ns: record.created_unix_ns,
        }]);
    }

    let corrupt = |defect: GenerationDefect| ProvisioningError::CorruptGenerations {
        key: key.to_string(),
        defect,
    };

    let gens = &record.generations;
    if gens[0].shard_count != record.shard_count {
        return Err(corrupt(GenerationDefect::ScalarMismatch));
    }
    let mut out = Vec::with_capacity(gens.len());
    for (idx, g) in gens.iter().enumerate() {
        if g.generation as usize != idx {
            return Err(corrupt(GenerationDefect::NotDense));
        }
        if !(1..=MAX_SHARD_COUNT).contains(&g.shard_count) {
            return Err(corrupt(GenerationDefect::CountOutOfRange));
        }
        if idx > 0 {
            let prev = &gens[idx - 1];
            if g.activation_hour <= prev.activation_hour {
                return Err(corrupt(GenerationDefect::ActivationNotIncreasing));
            }
            if g.shard_count == prev.shard_count {
                return Err(corrupt(GenerationDefect::AdjacentEqualCount));
            }
        }
        out.push(ShardGeneration {
            generation: g.generation,
            shard_count: g.shard_count,
            activation_hour: g.activation_hour,
            appended_unix_ns: g.appended_unix_ns,
        });
    }
    Ok(out)
}

/// [`read_generations`], plus the same format-version and (tenant, signal)
/// misfile guard [`validate_record`] applies before it ever calls
/// `read_generations`. `read_generations` alone trusts that the record was
/// already resolved under the right key; a caller that decodes a record it
/// read directly (the ingest router's live-switch refresh, which does not go
/// through [`validate_or_adopt`]) has not had that guard applied yet, so a
/// record from a future format or a misfiled/corrupted record for a different
/// tenant or signal must be refused here rather than misread as this tenant's
/// generation history.
pub fn read_generations_checked(
    record: &sysproto::ProvisioningRecord,
    key: &str,
    tenant_hash: &TenantHash,
    signal: Signal,
) -> Result<Vec<ShardGeneration>, ProvisioningError> {
    if record.format_version > PROVISIONING_FORMAT_VERSION {
        return Err(ProvisioningError::UnsupportedVersion {
            key: key.to_string(),
            got: record.format_version,
        });
    }
    if record.tenant_hash.as_slice() != tenant_hash.0.as_slice() {
        return Err(ProvisioningError::CorruptRecord {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant_hash.to_hex(),
            actual: hex::encode(&record.tenant_hash),
        });
    }
    if record.signal != to_sys_signal(signal) as i32 {
        return Err(ProvisioningError::CorruptRecord {
            key: key.to_string(),
            field: "signal",
            expected: format!("{:?}", to_sys_signal(signal)),
            actual: format!("{}", record.signal),
        });
    }
    read_generations(record, key)
}

/// Read and normalize a (tenant, signal)'s shard-generation history straight
/// from the store, fresh and uncached, for the read-side scan rule (ADR-0052
/// section 4). `Ok(None)` when no provisioning record exists yet: the caller
/// treats absence as the single implicit generation 0 at its configured
/// `shard_count`, identical to a pre-ADR-0050 process (and to an empty
/// `generations` list, [`read_generations`]). A present record is fully
/// version/misfile/history-checked via [`read_generations_checked`], so a
/// corrupt or misfiled record fails closed rather than being read as
/// "assume generation 0".
///
/// The read paths (resolve, fold, maintain) call this on every operation
/// rather than caching a view (ADR-0052 section 4 design note): a
/// stale-in-the-wrong-direction cached view could silently under-scan a query,
/// and the operations are already GET-bounded, so the fresh GET is accepted in
/// preference to a second staleness/caching policy with its own proof
/// obligations. It does not route through the catalog's query-accounting GET
/// funnel, matching the existing [`validate_or_adopt`] provisioning read.
pub async fn read_generations_from_store(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
) -> Result<Option<Vec<ShardGeneration>>, ProvisioningError> {
    let key = provisioning_key(tenant_hash, signal);
    match read_record(store, &key).await? {
        Some(record) => Ok(Some(read_generations_checked(
            &record,
            &key,
            tenant_hash,
            signal,
        )?)),
        None => Ok(None),
    }
}

impl ProvisioningError {
    fn store(key: &str, source: StoreError) -> Self {
        ProvisioningError::Store {
            key: key.to_string(),
            source,
        }
    }
}

/// What [`validate_or_adopt`] did. Every variant is a success; a disagreement
/// is a [`ProvisioningError`], never a variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisioningCheck {
    /// A record was present and its `shard_count` matched the configured value.
    Matched,
    /// No record and no shard data: a fresh (tenant, signal). Nothing was
    /// written (the caller passed [`AbsentPolicy::AdoptIfData`]); the record is
    /// created on the tenant's first actual write. This is the fresh-tenant,
    /// fresh-deployment case that must never refuse (ADR-0050 section 5).
    FreshNoData,
    /// No record existed; the record was written from config. Covers both a
    /// genuine first write ([`AbsentPolicy::CreateFromConfig`] on empty data)
    /// and adoption of pre-ADR data whose shard indices are all in range.
    Written,
}

/// What [`validate_or_adopt`] is allowed to write when no record exists. Every
/// policy validates a *present* record identically and refuses on a
/// `shard_count` mismatch; they differ only in what happens when the record is
/// absent (ADR-0050 section 5 lists ingest and maintenance as adopters, and the
/// read path as write-free).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsentPolicy {
    /// Write the record from config on a fresh (tenant, signal), and adopt
    /// pre-ADR data whose shard indices are all in range. Used on a tenant's
    /// first actual ingest write, so the durable `shard_count` is pinned as
    /// data lands.
    CreateFromConfig,
    /// Do not create a record for a fresh (no-data) (tenant, signal), but do
    /// adopt pre-ADR data whose shard indices are all in range. Used by the
    /// startup static-tenant check, the maintain per-tenant loop, and
    /// `provision adopt`: a signal with no data yet has nothing to provision
    /// (the record is created on its first write), while a signal with pre-ADR
    /// data is adopted exactly once here.
    AdoptIfData,
    /// Never write anything: validate a present record, and pass through when
    /// absent regardless of whether shard data exists. Used on the catalog
    /// resolve (read) path, which must not mutate storage — a query-only node
    /// may run with write-restricted credentials, and adoption belongs to the
    /// ingest/maintenance/CLI paths, not to a read.
    CheckOnly,
}

/// Read a (tenant, signal)'s provisioning record, if present. `NotFound` is
/// `Ok(None)`, not an error: absence is a valid state with adoption semantics.
async fn read_record(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<sysproto::ProvisioningRecord>, ProvisioningError> {
    match store.get(key, GetRange::Full).await {
        Ok(outcome) => {
            let record =
                sysproto::ProvisioningRecord::decode(outcome.data.as_ref()).map_err(|source| {
                    ProvisioningError::Decode {
                        key: key.to_string(),
                        source,
                    }
                })?;
            Ok(Some(record))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(ProvisioningError::store(key, err)),
    }
}

/// Validate a decoded record against the (tenant, signal) it was read under and
/// the configured `shard_count`. A version, tenant_hash, or signal disagreement
/// is a corrupt/misfiled record; a `shard_count` disagreement is the S1-E6
/// mismatch.
fn validate_record(
    record: &sysproto::ProvisioningRecord,
    key: &str,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard_count: u32,
) -> Result<(), ProvisioningError> {
    if record.format_version > PROVISIONING_FORMAT_VERSION {
        return Err(ProvisioningError::UnsupportedVersion {
            key: key.to_string(),
            got: record.format_version,
        });
    }
    if record.tenant_hash.as_slice() != tenant_hash.0.as_slice() {
        return Err(ProvisioningError::CorruptRecord {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant_hash.to_hex(),
            actual: hex::encode(&record.tenant_hash),
        });
    }
    if record.signal != to_sys_signal(signal) as i32 {
        return Err(ProvisioningError::CorruptRecord {
            key: key.to_string(),
            field: "signal",
            expected: format!("{:?}", to_sys_signal(signal)),
            actual: format!("{}", record.signal),
        });
    }
    if record.shard_count != shard_count {
        return Err(ProvisioningError::ShardCountMismatch {
            key: key.to_string(),
            tenant_hash: tenant_hash.to_hex(),
            signal: signal.key_prefix(),
            expected: shard_count,
            actual: record.shard_count,
        });
    }
    // The scalar `shard_count` is generation 0's count and the configured value
    // still equals it (a reshard never touches gen 0). But the append-only
    // generation history (ADR-0052 section 1) must also be structurally sound on
    // every touch: a corrupt history could route or scan over the wrong shard
    // set. Validate it here so any consumer fails closed, exactly as it does on
    // a `shard_count` disagreement.
    read_generations(record, key)?;
    Ok(())
}

/// Parse the shard index out of a delimiter-listing common prefix such as
/// `t/<hex>/<sig>/l0/0007/`. Returns `None` for a segment that is not a plain
/// shard number; such a segment is not a numbered shard this check can hide, so
/// skipping it is safe (it never lowers the observed maximum).
fn shard_index_from_common_prefix(common_prefix: &str, listed_prefix: &str) -> Option<u32> {
    let rest = common_prefix.strip_prefix(listed_prefix)?;
    let segment = rest.strip_suffix('/').unwrap_or(rest);
    // Shard directories are fixed-width zero-padded (`format_shard`), so a
    // plain decimal parse recovers the index. Reject anything non-numeric.
    if segment.is_empty() || !segment.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    segment.parse::<u32>().ok()
}

/// The highest shard index with data present under `l0/` or `c/` for this
/// (tenant, signal), or `None` when neither prefix holds any shard directory.
/// Delimiter listing costs one request per prefix and returns the shard
/// directories as common prefixes.
async fn max_observed_shard(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
) -> Result<Option<u32>, ProvisioningError> {
    let mut max: Option<u32> = None;
    for prefix in [
        l0_prefix(tenant_hash, signal),
        commit_prefix(tenant_hash, signal),
    ] {
        let list = store
            .list_delimited(&prefix)
            .await
            .map_err(|err| ProvisioningError::store(&prefix, err))?;
        for common in &list.common_prefixes {
            if let Some(idx) = shard_index_from_common_prefix(common, &prefix) {
                max = Some(max.map_or(idx, |m| m.max(idx)));
            }
        }
    }
    Ok(max)
}

/// Build the record to persist for this (tenant, signal, shard_count).
fn build_record(
    tenant_hash: &TenantHash,
    signal: Signal,
    shard_count: u32,
    now_ns: i64,
) -> sysproto::ProvisioningRecord {
    sysproto::ProvisioningRecord {
        format_version: PROVISIONING_FORMAT_VERSION,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: to_sys_signal(signal) as i32,
        shard_count,
        created_unix_ns: now_ns,
        // A first-write/adoption record has no reshard history: the empty list
        // is read as the single implicit generation 0 (ADR-0052 section 1), and
        // encoding omits the field entirely, so this record is byte-identical to
        // one an EC5 build wrote before ADR-0052.
        generations: Vec::new(),
        // No floor is raised at first write: the empty list is "no floor ever
        // raised" (ADR-0066), and encoding omits the field, keeping a
        // freshly-built record byte-identical to a pre-EM record.
        format_floors: Vec::new(),
    }
}

/// Write the record with `CreateIfAbsent`. A racing loser (`AlreadyExists`)
/// re-reads the winner's record and validates the configured value against it,
/// so a race can never let an incompatible `shard_count` through (ADR-0050
/// section 5, mirroring the `sys/tenancy` write_marker race handling).
async fn write_record_race_safe(
    store: &dyn ObjectStoreBackend,
    key: &str,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard_count: u32,
    now_ns: i64,
) -> Result<ProvisioningCheck, ProvisioningError> {
    let record = build_record(tenant_hash, signal, shard_count, now_ns);
    match store
        .put(
            key,
            record.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
    {
        Ok(_) => Ok(ProvisioningCheck::Written),
        Err(StoreError::AlreadyExists) => {
            // A concurrent writer won. Validate our config against what landed
            // rather than assuming our own value was recorded.
            let winner = read_record(store, key).await?.ok_or_else(|| {
                // The winner's write and our failed CreateIfAbsent both saw the
                // object, so a subsequent absence is a store anomaly, not a
                // normal state. Surface it as a store error on the key.
                ProvisioningError::store(key, StoreError::NotFound)
            })?;
            validate_record(&winner, key, tenant_hash, signal, shard_count)?;
            Ok(ProvisioningCheck::Matched)
        }
        Err(err) => Err(ProvisioningError::store(key, err)),
    }
}

/// Validate the configured `shard_count` for one (tenant, signal) against its
/// durable provisioning record, adopting or creating the record as ADR-0050
/// section 5 prescribes. The single shared entry point for all consumers.
///
/// Three scenarios, kept distinct:
/// 1. No record, no shard data: fresh. With [`AbsentPolicy::CreateFromConfig`],
///    write the record now; with [`AbsentPolicy::AdoptIfData`] or
///    [`AbsentPolicy::CheckOnly`], return [`ProvisioningCheck::FreshNoData`]
///    without writing (the record is created on the first actual write). Either
///    way this never refuses: a brand-new tenant with no prior writes and no
///    record must start cleanly.
/// 2. No record, pre-ADR shard data present: adopt (except under
///    [`AbsentPolicy::CheckOnly`], which never writes and returns
///    [`ProvisioningCheck::FreshNoData`]). If every observed shard index is
///    `< shard_count`, write the record from config. If any is `>= shard_count`,
///    the configured value hides data:
///    [`ProvisioningError::AdoptionWouldHideData`], writing nothing.
/// 3. Record present: compare `shard_count`. Equal is
///    [`ProvisioningCheck::Matched`]; unequal is
///    [`ProvisioningError::ShardCountMismatch`]. Identical under every policy.
pub async fn validate_or_adopt(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard_count: u32,
    now_ns: i64,
    absent_policy: AbsentPolicy,
) -> Result<ProvisioningCheck, ProvisioningError> {
    let key = provisioning_key(tenant_hash, signal);

    if let Some(record) = read_record(store, &key).await? {
        validate_record(&record, &key, tenant_hash, signal, shard_count)?;
        return Ok(ProvisioningCheck::Matched);
    }

    // No record. The read path never writes and never needs to list: pass
    // through without adopting (adoption belongs to ingest/maintain/CLI).
    if matches!(absent_policy, AbsentPolicy::CheckOnly) {
        return Ok(ProvisioningCheck::FreshNoData);
    }

    // Decide fresh (scenario 1) vs pre-ADR data (scenario 2) from the shard
    // directories actually present. One listing answers both "is there data?"
    // and "what is the highest shard index?".
    match max_observed_shard(store, tenant_hash, signal).await? {
        None => match absent_policy {
            // No record and no data is the fresh case: adopt-if-data and the
            // read-only check both pass through without writing. CheckOnly
            // returns earlier (line above) so it does not reach here today, but
            // handling it explicitly keeps this arm correct if that early return
            // is ever moved, rather than leaving a panic a refactor could trip.
            AbsentPolicy::AdoptIfData | AbsentPolicy::CheckOnly => {
                Ok(ProvisioningCheck::FreshNoData)
            }
            AbsentPolicy::CreateFromConfig => {
                write_record_race_safe(store, &key, tenant_hash, signal, shard_count, now_ns).await
            }
        },
        Some(observed) if observed >= shard_count => {
            Err(ProvisioningError::AdoptionWouldHideData {
                tenant_hash: tenant_hash.to_hex(),
                signal: signal.key_prefix(),
                configured: shard_count,
                observed_shard: observed,
            })
        }
        Some(_) => {
            // Pre-ADR data, all shard indices in range: safe to adopt.
            write_record_race_safe(store, &key, tenant_hash, signal, shard_count, now_ns).await
        }
    }
}

/// The outcome of a successful [`append_generation`] reshard append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReshardOutcome {
    /// The generation number the append created (dense, one past the prior
    /// last).
    pub generation: u32,
    /// The full normalized history after the append, generation 0 first.
    pub generations: Vec<ShardGeneration>,
}

/// Append one shard generation to a (tenant, signal)'s provisioning record
/// under `CasVersion` (ADR-0052 section 1, mirroring EC4's `set_gc_config`).
/// This is the only legal mutation of the record: it appends exactly one
/// generation whose `activation_hour` is strictly in the future, and never
/// touches an existing byte of history.
///
/// Enforced fail-closed before any write:
/// - the record must already exist ([`ProvisioningError::NoRecordToReshard`]);
/// - its existing history must decode and validate ([`read_generations`]);
/// - `new_shard_count` must be `1..=10000`
///   ([`ProvisioningError::ReshardCountOutOfRange`]);
/// - `new_shard_count` must differ from the current last generation's count
///   ([`ProvisioningError::ReshardSameCount`]);
/// - `activation_hour` must exceed both the current last generation's
///   activation and the current server hour
///   ([`ProvisioningError::ActivationInPast`]).
///
/// When the record's history is still the implicit empty list, generation 0 is
/// materialized explicitly (count = the scalar `shard_count`, activation_hour 0)
/// before the new generation is appended, so the persisted list stays dense and
/// self-describing. The scalar `shard_count` is never changed.
///
/// A concurrent append that moved the version is a typed
/// [`ProvisioningError::ReshardCasConflict`]: the loser re-reads, never
/// silently overwrites the winner.
pub async fn append_generation(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    new_shard_count: u32,
    activation_hour: u32,
    now_ns: i64,
) -> Result<ReshardOutcome, ProvisioningError> {
    if !(1..=MAX_SHARD_COUNT).contains(&new_shard_count) {
        return Err(ProvisioningError::ReshardCountOutOfRange {
            shard_count: new_shard_count,
        });
    }
    let now_hour = u32::try_from(now_ns.div_euclid(NS_PER_HOUR).max(0)).unwrap_or(u32::MAX);
    let key = provisioning_key(tenant_hash, signal);

    let (record, version) = match store.get(&key, GetRange::Full).await {
        Ok(outcome) => {
            let record =
                sysproto::ProvisioningRecord::decode(outcome.data.as_ref()).map_err(|source| {
                    ProvisioningError::Decode {
                        key: key.clone(),
                        source,
                    }
                })?;
            (record, outcome.version)
        }
        Err(StoreError::NotFound) => {
            return Err(ProvisioningError::NoRecordToReshard {
                key,
                tenant_hash: tenant_hash.to_hex(),
                signal: signal.key_prefix(),
            });
        }
        Err(err) => return Err(ProvisioningError::store(&key, err)),
    };

    // Version, misfile, and history-corruption guard, then normalize the
    // existing history (fail closed on any of these) before extending it. A
    // record misfiled under the wrong tenant or signal must never be extended
    // as if it were this (tenant, signal)'s history: append_generation writes
    // the record back, so trusting a misfiled read here would durably corrupt
    // it rather than merely misread it.
    let mut history = read_generations_checked(&record, &key, tenant_hash, signal)?;
    let last = history.last().copied().unwrap_or(ShardGeneration {
        generation: 0,
        shard_count: record.shard_count,
        activation_hour: 0,
        appended_unix_ns: record.created_unix_ns,
    });

    if new_shard_count == last.shard_count {
        return Err(ProvisioningError::ReshardSameCount {
            shard_count: new_shard_count,
        });
    }
    if activation_hour <= last.activation_hour || activation_hour <= now_hour {
        return Err(ProvisioningError::ActivationInPast {
            activation_hour,
            now_hour,
        });
    }

    let new_generation = last.generation + 1;
    history.push(ShardGeneration {
        generation: new_generation,
        shard_count: new_shard_count,
        activation_hour,
        appended_unix_ns: now_ns,
    });

    // Persist the full, now-explicit history. The scalar shard_count stays
    // generation 0's count; every prior generation is carried verbatim.
    let mut new_record = record;
    new_record.generations = history
        .iter()
        .map(|g| sysproto::ShardGeneration {
            generation: g.generation,
            shard_count: g.shard_count,
            activation_hour: g.activation_hour,
            appended_unix_ns: g.appended_unix_ns,
        })
        .collect();

    match store
        .put(
            &key,
            new_record.encode_to_vec().into(),
            PutOptions {
                mode: PutMode::CasVersion(version),
                checksum: None,
            },
        )
        .await
    {
        Ok(_) => Ok(ReshardOutcome {
            generation: new_generation,
            generations: history,
        }),
        Err(StoreError::PreconditionFailed) => Err(ProvisioningError::ReshardCasConflict { key }),
        Err(err) => Err(ProvisioningError::store(&key, err)),
    }
}

// ---- ADR-0066 format-floor history (epic EM, EM-T3) ----
//
// Format floors live on the SAME `ProvisioningRecord` at the SAME
// `t/<hash>/<sig>/prov` key as the shard-generation history (field 7, additive,
// no format_version bump; see the proto doc comment). They are implemented here
// beside `append_generation`, not in a sibling module, precisely because they
// share that record and key: the floor mutator reuses `provisioning_key`,
// `read_record`, `to_sys_signal`, the tenant/signal misfile guard, and the
// `ProvisioningError` type, and keeping both mutators of one record in one file
// makes their shared-CAS interaction visible (a concurrent `append_generation`
// and `raise_format_floor` race each other's version, and the loser re-reads).
// `key_epoch.rs` became a sibling module because it is a *separate object*
// (`t/<hash>/enc`, its own record, key, and error type); a floor is not a
// separate object, so that precedent points the other way here.
//
// Shape: a flat, append-only list. The "current floor" for a family is the
// highest recorded `floor_version` for that family. A flat list is the simplest
// thing that matches "append-only" and needs no per-family index structure; the
// only invariant it must carry is per-family monotonicity, enforced on decode.
// A raise must be *strictly* above the current floor for its family: floors are
// raised only, never lowered, and a re-raise to the same value records nothing
// new, so it is refused — matching the generation history's same-count refusal
// and the key-epoch history's same-key refusal.

/// One entry of a provisioning record's append-only format-floor history
/// (ADR-0066 decision 3), decoded into a plain struct so consumers (release
/// engineering's floor checks, the migration job) never touch the proto type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatFloor {
    /// Lowercase format-family identifier ("rseg", "rlog", "rspan", "csnap", ...).
    pub family: String,
    /// No live object of `family` below this version exists for this (tenant,
    /// signal). Always `>= 1` in a valid history.
    pub floor_version: u32,
    /// When this floor was raised (unix ns); informational.
    pub raised_unix_ns: i64,
    /// The job identity that raised this floor; informational.
    pub raised_by: String,
}

/// The outcome of a successful [`raise_format_floor`] append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloorRaiseOutcome {
    /// The family whose floor was raised.
    pub family: String,
    /// The new (now current) floor for that family.
    pub floor_version: u32,
    /// The full format-floor history after the append, in append order.
    pub floors: Vec<FormatFloor>,
}

/// Decode and validate a record's format-floor history into a normalized list
/// (ADR-0066 decision 3). An empty `format_floors` list (every record written
/// before EM) is valid and decodes to an empty history: "no floor has ever been
/// raised". A non-empty list is validated fail-closed: no empty family, no
/// zero `floor_version`, and — per family — `floor_version` strictly increasing
/// in append order (floors raised only). Any violation is a typed
/// [`ProvisioningError::CorruptFloors`], never a panic or a silent fix.
pub fn read_floors(
    record: &sysproto::ProvisioningRecord,
    key: &str,
) -> Result<Vec<FormatFloor>, ProvisioningError> {
    let corrupt = |defect: FloorDefect| ProvisioningError::CorruptFloors {
        key: key.to_string(),
        defect,
    };
    // Highest floor_version seen so far per family. For a valid (strictly
    // increasing) history this equals the running max, so comparing each entry
    // against it with `<=` rejects both a lower and an equal repeat; on the
    // first violation we return, so a later corrupt entry is never reached.
    let mut last_for_family: std::collections::HashMap<&str, u32> =
        std::collections::HashMap::new();
    let mut out = Vec::with_capacity(record.format_floors.len());
    for f in &record.format_floors {
        if f.family.is_empty() {
            return Err(corrupt(FloorDefect::EmptyFamily));
        }
        if f.family.chars().any(|c| c.is_ascii_uppercase()) {
            return Err(corrupt(FloorDefect::NotLowercase));
        }
        if f.floor_version == 0 {
            return Err(corrupt(FloorDefect::ZeroFloor));
        }
        if let Some(&prev) = last_for_family.get(f.family.as_str())
            && f.floor_version <= prev
        {
            return Err(corrupt(FloorDefect::NotIncreasing));
        }
        last_for_family.insert(f.family.as_str(), f.floor_version);
        out.push(FormatFloor {
            family: f.family.clone(),
            floor_version: f.floor_version,
            raised_unix_ns: f.raised_unix_ns,
            raised_by: f.raised_by.clone(),
        });
    }
    Ok(out)
}

/// [`read_floors`], plus the same format-version and (tenant, signal) misfile
/// guard [`read_generations_checked`] applies. A caller that decodes a record it
/// read directly must refuse a record from a future format, or one
/// misfiled/corrupted under the wrong tenant or signal, rather than reading its
/// floors as this (tenant, signal)'s history — the same durable-corruption
/// hazard `raise_format_floor` guards against before writing back.
pub fn read_floors_checked(
    record: &sysproto::ProvisioningRecord,
    key: &str,
    tenant_hash: &TenantHash,
    signal: Signal,
) -> Result<Vec<FormatFloor>, ProvisioningError> {
    if record.format_version > PROVISIONING_FORMAT_VERSION {
        return Err(ProvisioningError::UnsupportedVersion {
            key: key.to_string(),
            got: record.format_version,
        });
    }
    if record.tenant_hash.as_slice() != tenant_hash.0.as_slice() {
        return Err(ProvisioningError::CorruptRecord {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant_hash.to_hex(),
            actual: hex::encode(&record.tenant_hash),
        });
    }
    if record.signal != to_sys_signal(signal) as i32 {
        return Err(ProvisioningError::CorruptRecord {
            key: key.to_string(),
            field: "signal",
            expected: format!("{:?}", to_sys_signal(signal)),
            actual: format!("{}", record.signal),
        });
    }
    read_floors(record, key)
}

/// The current floor for `family`: the highest recorded `floor_version` for it,
/// or `None` if no floor has ever been raised for that family. A pure function
/// over the normalized history [`read_floors`] returns.
pub fn current_floor(floors: &[FormatFloor], family: &str) -> Option<u32> {
    floors
        .iter()
        .filter(|f| f.family == family)
        .map(|f| f.floor_version)
        .max()
}

/// Read and normalize a (tenant, signal)'s full format-floor history straight
/// from the store, fresh and uncached. `Ok(None)` when no provisioning record
/// exists yet (no floor could have been raised). A present record is fully
/// version/misfile/history-checked via [`read_floors_checked`], so a corrupt or
/// misfiled record fails closed rather than being read as "no floors".
pub async fn read_floors_from_store(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
) -> Result<Option<Vec<FormatFloor>>, ProvisioningError> {
    let key = provisioning_key(tenant_hash, signal);
    match read_record(store, &key).await? {
        Some(record) => Ok(Some(read_floors_checked(
            &record,
            &key,
            tenant_hash,
            signal,
        )?)),
        None => Ok(None),
    }
}

/// The current floor for one `family` of a (tenant, signal), read fresh from the
/// store. `Ok(None)` when no provisioning record exists yet, or the record
/// exists but no floor has ever been raised for `family` — the documented "no
/// floor" default, not an error. A present record is fully checked via
/// [`read_floors_from_store`], so a corrupt or misfiled record fails closed.
pub async fn current_floor_from_store(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    family: &str,
) -> Result<Option<u32>, ProvisioningError> {
    match read_floors_from_store(store, tenant_hash, signal).await? {
        Some(floors) => Ok(current_floor(&floors, family)),
        None => Ok(None),
    }
}

/// Raise the format floor for one `family` of a (tenant, signal) by appending
/// one [`FormatFloor`] to its provisioning record under `CasVersion` (ADR-0066
/// decision 3, mirroring [`append_generation`]). This is the only legal mutation
/// of the floor history: it appends exactly one entry and never touches an
/// existing byte of history.
///
/// Enforced fail-closed before any write:
/// - `family` must be non-empty ([`ProvisioningError::FloorFamilyEmpty`]) and
///   lowercase ([`ProvisioningError::FloorFamilyNotLowercase`]);
/// - the record must already exist ([`ProvisioningError::NoRecordForFloor`]);
/// - the record's version, (tenant, signal), and existing floor history must
///   decode and validate ([`read_floors_checked`]) — a record misfiled under
///   another tenant or signal is never extended as this one's, since this writes
///   the record back and would otherwise durably corrupt it;
/// - `floor_version` must be strictly above the current floor for `family`
///   ([`ProvisioningError::FloorNotAboveCurrent`]; floors are raised only, and a
///   re-raise to the same value is refused as a no-op).
///
/// A concurrent write that moved the version is a typed
/// [`ProvisioningError::FloorCasConflict`]: the loser re-reads, never silently
/// overwrites the winner. The scalar `shard_count` and the generation history
/// are carried through verbatim; only `format_floors` grows.
pub async fn raise_format_floor(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    family: &str,
    floor_version: u32,
    raised_by: &str,
    now_ns: i64,
) -> Result<FloorRaiseOutcome, ProvisioningError> {
    if family.is_empty() {
        return Err(ProvisioningError::FloorFamilyEmpty);
    }
    if family.chars().any(|c| c.is_ascii_uppercase()) {
        return Err(ProvisioningError::FloorFamilyNotLowercase(
            family.to_string(),
        ));
    }
    let key = provisioning_key(tenant_hash, signal);

    let (record, version) = match store.get(&key, GetRange::Full).await {
        Ok(outcome) => {
            let record =
                sysproto::ProvisioningRecord::decode(outcome.data.as_ref()).map_err(|source| {
                    ProvisioningError::Decode {
                        key: key.clone(),
                        source,
                    }
                })?;
            (record, outcome.version)
        }
        Err(StoreError::NotFound) => {
            return Err(ProvisioningError::NoRecordForFloor {
                key,
                tenant_hash: tenant_hash.to_hex(),
                signal: signal.key_prefix(),
            });
        }
        Err(err) => return Err(ProvisioningError::store(&key, err)),
    };

    // Version, misfile, and history-corruption guard, then normalize the
    // existing floor history (fail closed on any of these) before extending it.
    let mut floors = read_floors_checked(&record, &key, tenant_hash, signal)?;
    let current = current_floor(&floors, family).unwrap_or(0);
    if floor_version <= current {
        return Err(ProvisioningError::FloorNotAboveCurrent {
            key,
            family: family.to_string(),
            requested: floor_version,
            current,
        });
    }

    floors.push(FormatFloor {
        family: family.to_string(),
        floor_version,
        raised_unix_ns: now_ns,
        raised_by: raised_by.to_string(),
    });

    // Persist the full history. shard_count and generations are carried verbatim.
    let mut new_record = record;
    new_record.format_floors = floors
        .iter()
        .map(|f| sysproto::FormatFloor {
            family: f.family.clone(),
            floor_version: f.floor_version,
            raised_unix_ns: f.raised_unix_ns,
            raised_by: f.raised_by.clone(),
        })
        .collect();

    match store
        .put(
            &key,
            new_record.encode_to_vec().into(),
            PutOptions {
                mode: PutMode::CasVersion(version),
                checksum: None,
            },
        )
        .await
    {
        Ok(_) => Ok(FloorRaiseOutcome {
            family: family.to_string(),
            floor_version,
            floors,
        }),
        Err(StoreError::PreconditionFailed) => Err(ProvisioningError::FloorCasConflict { key }),
        Err(err) => Err(ProvisioningError::store(&key, err)),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutMode, PutOptions};
    use std::sync::Arc;

    fn tenant() -> TenantHash {
        TenantHash([0xABu8; 16])
    }

    fn mem() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    /// Seed a shard directory so a (tenant, signal) looks like pre-ADR data
    /// with the given shard index present under `l0/`.
    async fn seed_l0_shard(
        store: &dyn ObjectStoreBackend,
        th: &TenantHash,
        signal: Signal,
        shard: u32,
    ) {
        let key = format!(
            "t/{}/{}/l0/{:04}/writer.0.{:020}.deadbeefdeadbeef.rseg",
            th.to_hex(),
            signal.key_prefix(),
            shard,
            1u64
        );
        store
            .put(&key, vec![1, 2, 3].into(), PutOptions::default())
            .await
            .expect("seed l0");
    }

    async fn seed_record(
        store: &dyn ObjectStoreBackend,
        th: &TenantHash,
        signal: Signal,
        shard_count: u32,
    ) {
        let key = provisioning_key(th, signal);
        let record = build_record(th, signal, shard_count, 1_000);
        store
            .put(
                &key,
                record.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed record");
    }

    /// Pin the sys.v1.Signal mapping value-for-value against ravel.commit.v1
    /// (via ravel_commit::signal), so the two never drift: a provisioning
    /// record and a commit record under the same `<sig>` prefix must decode to
    /// the same signal.
    #[test]
    fn sys_signal_mapping_matches_commit_signal() {
        for signal in [
            Signal::Metrics,
            Signal::Logs,
            Signal::Spans,
            Signal::Profiles,
            Signal::Alerts,
            Signal::Audit,
        ] {
            assert_eq!(
                to_sys_signal(signal) as i32,
                ravel_commit::signal::to_proto(signal) as i32,
                "sys and commit Signal enums disagree for {signal:?}"
            );
        }
    }

    #[tokio::test]
    async fn fresh_tenant_no_record_no_data_passes_through() {
        let store = mem();
        let out = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect("a fresh tenant with no record and no data must not refuse");
        assert_eq!(out, ProvisioningCheck::FreshNoData);
        // PassThrough must not have written a record.
        let got = store
            .get(
                &provisioning_key(&tenant(), Signal::Metrics),
                GetRange::Full,
            )
            .await;
        assert!(
            matches!(got, Err(StoreError::NotFound)),
            "no record written"
        );
    }

    #[tokio::test]
    async fn first_write_creates_record_from_config() {
        let store = mem();
        let out = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            1_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("first write creates the record");
        assert_eq!(out, ProvisioningCheck::Written);
        let record = read_record(
            store.as_ref(),
            &provisioning_key(&tenant(), Signal::Metrics),
        )
        .await
        .expect("read")
        .expect("record present");
        assert_eq!(record.shard_count, 4);
        assert_eq!(record.signal, sysproto::Signal::Metrics as i32);
    }

    #[tokio::test]
    async fn record_present_and_matching_is_ok() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        let out = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect("matching record");
        assert_eq!(out, ProvisioningCheck::Matched);
    }

    #[tokio::test]
    async fn record_present_and_disagreeing_is_shard_count_mismatch() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        let err = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            2,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect_err("a lower configured shard_count must refuse");
        match err {
            ProvisioningError::ShardCountMismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, 2);
                assert_eq!(actual, 4);
            }
            other => panic!("wrong error: {other}"),
        }
    }

    #[tokio::test]
    async fn adoption_writes_record_when_all_shards_in_range() {
        let store = mem();
        // Data across shards 0..=2, configured for 4: all in range.
        for shard in 0..=2 {
            seed_l0_shard(store.as_ref(), &tenant(), Signal::Logs, shard).await;
        }
        let out = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Logs,
            4,
            2_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect("adoption succeeds when all observed shards are in range");
        assert_eq!(out, ProvisioningCheck::Written);
        let record = read_record(store.as_ref(), &provisioning_key(&tenant(), Signal::Logs))
            .await
            .expect("read")
            .expect("record present");
        assert_eq!(record.shard_count, 4);
    }

    #[tokio::test]
    async fn adoption_refuses_and_writes_nothing_when_shard_out_of_range() {
        let store = mem();
        // Data at shard 5, configured for 4: shard 4 and 5 would be hidden.
        seed_l0_shard(store.as_ref(), &tenant(), Signal::Logs, 5).await;
        let err = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Logs,
            4,
            2_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect_err("an out-of-range shard must refuse adoption");
        match err {
            ProvisioningError::AdoptionWouldHideData {
                configured,
                observed_shard,
                ..
            } => {
                assert_eq!(configured, 4);
                assert_eq!(observed_shard, 5);
            }
            other => panic!("wrong error: {other}"),
        }
        // Nothing was written.
        let got = store
            .get(&provisioning_key(&tenant(), Signal::Logs), GetRange::Full)
            .await;
        assert!(
            matches!(got, Err(StoreError::NotFound)),
            "no record written"
        );
    }

    /// A commit-only shard (data compacted and swept, only records under `c/`)
    /// is still observed, so its index still bounds the safe shard_count.
    #[tokio::test]
    async fn adoption_observes_commit_only_shards() {
        let store = mem();
        let th = tenant();
        let key = format!(
            "t/{}/{}/c/{:04}/1970-01-01-00/writer.0.{:020}.cmt",
            th.to_hex(),
            Signal::Spans.key_prefix(),
            6,
            1u64
        );
        store
            .put(&key, vec![9].into(), PutOptions::default())
            .await
            .expect("seed commit record");
        let err = validate_or_adopt(
            store.as_ref(),
            &th,
            Signal::Spans,
            4,
            2_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect_err("a commit-only shard 6 out of range for shard_count 4 must refuse");
        assert!(matches!(
            err,
            ProvisioningError::AdoptionWouldHideData {
                observed_shard: 6,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn corrupt_record_decodes_to_typed_error_not_panic() {
        let store = mem();
        let key = provisioning_key(&tenant(), Signal::Metrics);
        // Bytes that are not a valid ProvisioningRecord: a truncated/garbage
        // protobuf. Decode must be a typed error, never a panic.
        store
            .put(
                &key,
                vec![0xFF, 0xFF, 0xFF, 0x07].into(),
                PutOptions::default(),
            )
            .await
            .expect("put garbage");
        let err = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect_err("garbage bytes must be a typed decode error");
        assert!(
            matches!(err, ProvisioningError::Decode { .. }),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn unsupported_future_version_is_refused() {
        let store = mem();
        let key = provisioning_key(&tenant(), Signal::Metrics);
        let mut record = build_record(&tenant(), Signal::Metrics, 4, 1_000);
        record.format_version = PROVISIONING_FORMAT_VERSION + 1;
        store
            .put(&key, record.encode_to_vec().into(), PutOptions::default())
            .await
            .expect("put future record");
        let err = validate_or_adopt(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect_err("a future format_version must be refused");
        assert!(matches!(err, ProvisioningError::UnsupportedVersion { .. }));
    }

    /// CreateIfAbsent race: the writer's own initial GET misses (an eventual
    /// blip, injected on the first Get of the prov key), but a concurrent
    /// winner's record is really present, so the CreateIfAbsent put returns
    /// AlreadyExists and the loser re-reads and validates against the winner
    /// rather than erroring. A matching winner is accepted.
    #[tokio::test]
    async fn create_if_absent_race_loser_revalidates_against_winner() {
        let inner = MemoryStore::new();
        let th = tenant();
        // Winner already wrote shard_count=4.
        seed_record(&inner, &th, Signal::Metrics, 4).await;
        // Loser's first GET of the prov key misses (eventual-consistency blip).
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
                .with_key_contains("/prov")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);

        // Configured to agree with the winner: the re-read validates and passes.
        let out = validate_or_adopt(
            &store,
            &th,
            Signal::Metrics,
            4,
            3_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("race loser re-reads and validates against the winner");
        assert_eq!(out, ProvisioningCheck::Matched);
        assert_eq!(
            store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::NotFoundBlip),
            1,
            "the blip must have fired for this to exercise the race path"
        );
    }

    /// Same race, but the loser's configured shard_count disagrees with the
    /// winner's record: the re-read surfaces the mismatch rather than silently
    /// accepting either value.
    #[tokio::test]
    async fn create_if_absent_race_loser_surfaces_mismatch() {
        let inner = MemoryStore::new();
        let th = tenant();
        seed_record(&inner, &th, Signal::Metrics, 4).await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
                .with_key_contains("/prov")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);

        let err = validate_or_adopt(
            &store,
            &th,
            Signal::Metrics,
            2,
            3_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect_err("a race loser configured for a different shard_count must refuse");
        assert!(matches!(err, ProvisioningError::ShardCountMismatch { .. }));
        assert_eq!(
            store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::NotFoundBlip),
            1
        );
    }

    // ---- ADR-0052 generation-versioning tests ----

    fn sg(generation: u32, shard_count: u32, activation_hour: u32) -> ShardGeneration {
        ShardGeneration {
            generation,
            shard_count,
            activation_hour,
            appended_unix_ns: 0,
        }
    }

    /// `active_shard_count`: single implicit generation covers every hour.
    #[test]
    fn active_shard_count_single_generation() {
        let gens = [sg(0, 4, 0)];
        assert_eq!(active_shard_count(&gens, 0), 4);
        assert_eq!(active_shard_count(&gens, 1_000), 4);
    }

    /// `active_shard_count`: at and around each activation boundary the latest
    /// generation whose activation_hour <= hour wins.
    #[test]
    fn active_shard_count_multiple_generations_at_boundaries() {
        // gen0 count 4 from hour 0; gen1 count 8 from hour 100; gen2 count 2
        // from hour 200.
        let gens = [sg(0, 4, 0), sg(1, 8, 100), sg(2, 2, 200)];
        assert_eq!(active_shard_count(&gens, 0), 4);
        assert_eq!(active_shard_count(&gens, 99), 4, "just before gen1");
        assert_eq!(active_shard_count(&gens, 100), 8, "at gen1 activation");
        assert_eq!(active_shard_count(&gens, 199), 8, "just before gen2");
        assert_eq!(active_shard_count(&gens, 200), 2, "at gen2 activation");
        assert_eq!(active_shard_count(&gens, 10_000), 2, "far after the last");
    }

    /// `scan_count`: a single implicit generation scans its whole count for
    /// every hour, exactly like the pre-ADR-0052 static `0..shard_count`.
    #[test]
    fn scan_count_single_generation() {
        let gens = [sg(0, 4, 0)];
        assert_eq!(scan_count(&gens, 0, DEFAULT_SCAN_SLACK_HOURS), 4);
        assert_eq!(scan_count(&gens, 1_000, DEFAULT_SCAN_SLACK_HOURS), 4);
    }

    /// `scan_count` at an increase boundary: before the activation only the old
    /// count is scanned; at and after it the new (wider) count is scanned. The
    /// slack window adds nothing on an increase because the old, smaller count
    /// is a subset of the new range, so the `max` is unchanged (ADR-0052
    /// section 3: "Increase: no slack needed").
    #[test]
    fn scan_count_increase_boundary() {
        // gen0 count 4 from hour 0; gen1 count 8 from hour 100. S = 2.
        let gens = [sg(0, 4, 0), sg(1, 8, 100)];
        assert_eq!(scan_count(&gens, 99, 2), 4, "just before the increase");
        assert_eq!(scan_count(&gens, 100, 2), 8, "at the increase activation");
        assert_eq!(scan_count(&gens, 101, 2), 8, "just inside the slack window");
        assert_eq!(scan_count(&gens, 102, 2), 8, "past the slack window");
        assert_eq!(scan_count(&gens, 10_000, 2), 8, "far after the increase");
    }

    /// `scan_count` at a decrease boundary: the retiring, larger count must stay
    /// in the scan set for exactly `S` hours past the successor's activation,
    /// then drop to the new, smaller count (ADR-0052 section 3: "Decrease: the
    /// retiring generation's count must remain in the scan set for `S` hours").
    #[test]
    fn scan_count_decrease_slack_window() {
        // gen0 count 8 from hour 0; gen1 count 4 from hour 100. S = 2.
        let gens = [sg(0, 8, 0), sg(1, 4, 100)];
        assert_eq!(
            scan_count(&gens, 99, 2),
            8,
            "before the decrease: old count"
        );
        assert_eq!(
            scan_count(&gens, 100, 2),
            8,
            "at activation: straggler under the old count still scanned"
        );
        assert_eq!(
            scan_count(&gens, 101, 2),
            8,
            "one hour into the slack window: old count still scanned"
        );
        assert_eq!(
            scan_count(&gens, 102, 2),
            4,
            "exactly S hours past activation: slack closed, new count only"
        );
        assert_eq!(scan_count(&gens, 200, 2), 4, "far past the decrease");
    }

    /// `scan_count` with `S = 0`: the retiring count drops the instant the
    /// successor activates, so a decrease has no straggler window at all. Pins
    /// that the slack argument, not a hard-coded constant, controls the window.
    #[test]
    fn scan_count_zero_slack_has_no_straggler_window() {
        let gens = [sg(0, 8, 0), sg(1, 4, 100)];
        assert_eq!(scan_count(&gens, 99, 0), 8);
        assert_eq!(
            scan_count(&gens, 100, 0),
            4,
            "successor takes over immediately"
        );
    }

    /// `scan_count` across three generations, an increase then a decrease, with
    /// overlapping slack windows resolved by `max`.
    #[test]
    fn scan_count_three_generations() {
        // gen0 count 4 @0; gen1 count 8 @100; gen2 count 2 @200. S = 2.
        let gens = [sg(0, 4, 0), sg(1, 8, 100), sg(2, 2, 200)];
        assert_eq!(scan_count(&gens, 50, 2), 4);
        assert_eq!(scan_count(&gens, 100, 2), 8, "increase to 8");
        assert_eq!(scan_count(&gens, 199, 2), 8);
        assert_eq!(
            scan_count(&gens, 200, 2),
            8,
            "decrease boundary: gen1's 8 held by slack"
        );
        assert_eq!(scan_count(&gens, 201, 2), 8, "still inside gen1's slack");
        assert_eq!(scan_count(&gens, 202, 2), 2, "slack closed: gen2's count");
    }

    /// `max_scan_count_over_range`: a single implicit generation scans its
    /// whole count for any range.
    #[test]
    fn max_scan_range_single_generation() {
        let gens = [sg(0, 4, 0)];
        assert_eq!(
            max_scan_count_over_range(&gens, 0, 0, DEFAULT_SCAN_SLACK_HOURS),
            4
        );
        assert_eq!(
            max_scan_count_over_range(&gens, 10, 1_000, DEFAULT_SCAN_SLACK_HOURS),
            4
        );
    }

    /// An increase: a range spanning the activation sees the wider new count; a
    /// range fully before it sees only the old count; a range fully after sees
    /// the new count. Equivalent to the per-hour `max` of `scan_count`.
    #[test]
    fn max_scan_range_increase() {
        // gen0 count 4 @0; gen1 count 8 @100. S = 2.
        let gens = [sg(0, 4, 0), sg(1, 8, 100)];
        assert_eq!(
            max_scan_count_over_range(&gens, 0, 99, 2),
            4,
            "range fully before the increase: old count only"
        );
        assert_eq!(
            max_scan_count_over_range(&gens, 0, 100, 2),
            8,
            "range spanning the activation: wider new count"
        );
        assert_eq!(
            max_scan_count_over_range(&gens, 200, 300, 2),
            8,
            "range fully after the increase: new count"
        );
    }

    /// A decrease: the retiring, larger count is in scope for exactly `S` hours
    /// past the successor's activation. A range whose only hours touching the
    /// old generation lie inside that slack window still yields the larger
    /// count (the record-inside case); a range entirely past the slack window
    /// yields only the new count (the record-outside case).
    #[test]
    fn max_scan_range_decrease_inside_vs_outside_slack() {
        // gen0 count 8 @0; gen1 count 4 @100. S = 2. gen0's scan interval is
        // [0, 100 + 2) = [0, 102); gen1's is [100, inf).
        let gens = [sg(0, 8, 0), sg(1, 4, 100)];
        assert_eq!(
            max_scan_count_over_range(&gens, 100, 101, 2),
            8,
            "range inside the slack window intersects gen0's interval: larger count"
        );
        assert_eq!(
            max_scan_count_over_range(&gens, 101, 101, 2),
            8,
            "single hour still inside the slack window: larger count"
        );
        assert_eq!(
            max_scan_count_over_range(&gens, 102, 200, 2),
            4,
            "range entirely past the slack window: new count only"
        );
        assert_eq!(
            max_scan_count_over_range(&gens, 200, 300, 2),
            4,
            "far past the decrease: new count"
        );
    }

    /// A range fully before, spanning, and fully after an activation, pinned
    /// against the equivalent per-hour `max` of `scan_count` so the O(gens)
    /// range formula provably matches the O(hours) definition.
    #[test]
    fn max_scan_range_matches_per_hour_max() {
        // gen0 count 4 @0; gen1 count 8 @100; gen2 count 2 @200. S = 2.
        let gens = [sg(0, 4, 0), sg(1, 8, 100), sg(2, 2, 200)];
        for (start, end) in [(0u32, 50u32), (0, 100), (50, 205), (150, 205), (203, 400)] {
            let per_hour_max = (start..=end)
                .map(|h| scan_count(&gens, h, 2))
                .max()
                .expect("non-empty range");
            assert_eq!(
                max_scan_count_over_range(&gens, start, end, 2),
                per_hour_max,
                "range [{start}, {end}] must equal the per-hour max of scan_count"
            );
        }
    }

    /// With `S = 0` the retiring count leaves the scan set the instant the
    /// successor activates, so a range starting at the activation hour sees
    /// only the new count.
    #[test]
    fn max_scan_range_zero_slack() {
        let gens = [sg(0, 8, 0), sg(1, 4, 100)];
        assert_eq!(
            max_scan_count_over_range(&gens, 99, 99, 0),
            8,
            "before activation: old count"
        );
        assert_eq!(
            max_scan_count_over_range(&gens, 100, 200, 0),
            4,
            "from activation with no slack: new count only"
        );
    }

    /// A large `activation_hour` near `u32::MAX` must not overflow when the
    /// slack is added: the arithmetic is done in `u64`.
    #[test]
    fn scan_count_no_overflow_near_u32_max() {
        let gens = [sg(0, 4, 0), sg(1, 8, u32::MAX - 1)];
        assert_eq!(
            scan_count(&gens, u32::MAX, 2),
            8,
            "last generation, no successor"
        );
        assert_eq!(scan_count(&gens, 0, 2), 4);
    }

    /// The decode-compatibility test the ADR mandates: a record with the exact
    /// byte shape EC5 wrote (no `generations` field set) decodes as generation 0
    /// with the scalar `shard_count`, identical to before ADR-0052. Proven at the
    /// byte level: the encoded bytes are unchanged and the decode yields one
    /// implicit generation.
    #[test]
    fn empty_generations_decode_as_implicit_generation_zero() {
        // An EC5-shaped record: no generations field.
        let record = sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count: 4,
            created_unix_ns: 1_234,
            generations: Vec::new(),
            format_floors: Vec::new(),
        };
        let key = provisioning_key(&tenant(), Signal::Metrics);
        let gens = read_generations(&record, &key).expect("empty history is valid");
        assert_eq!(gens.len(), 1, "read as a single implicit generation");
        assert_eq!(gens[0].generation, 0);
        assert_eq!(gens[0].shard_count, 4);
        assert_eq!(gens[0].activation_hour, 0);
        assert_eq!(
            active_shard_count(&gens, 999_999),
            4,
            "routes at the scalar count for every hour, exactly as before ADR-0052"
        );
        // And the routing behaviour is identical to a record with no history at
        // all: build_record must still encode to the pre-ADR-0052 byte shape
        // (no field 6), so an old reader is unaffected.
        let built = build_record(&tenant(), Signal::Metrics, 4, 1_234);
        assert!(
            built.generations.is_empty(),
            "a freshly built record carries no explicit generations"
        );
    }

    /// A zero (or out-of-range) `shard_count` on a record with no explicit
    /// generation history is refused, exactly as an explicit generation entry
    /// with an out-of-range count already was. Left unchecked, this becomes an
    /// index-out-of-bounds panic on the ingest hot path once a router uses the
    /// resulting `active_shard_count() == 0` to size its shard-actor set.
    #[test]
    fn empty_generations_rejects_zero_shard_count() {
        let record = sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count: 0,
            created_unix_ns: 0,
            generations: Vec::new(),
            format_floors: Vec::new(),
        };
        let key = provisioning_key(&tenant(), Signal::Metrics);
        let err = read_generations(&record, &key).expect_err("shard_count 0 must be refused");
        assert!(matches!(
            err,
            ProvisioningError::CorruptGenerations {
                defect: GenerationDefect::CountOutOfRange,
                ..
            }
        ));
    }

    /// [`read_generations_checked`] refuses a record from a future format
    /// version before ever trusting its generation history, matching the guard
    /// [`validate_record`] already applies on the [`validate_or_adopt`] path.
    #[test]
    fn read_generations_checked_rejects_future_format_version() {
        let record = sysproto::ProvisioningRecord {
            format_version: PROVISIONING_FORMAT_VERSION + 1,
            tenant_hash: tenant().0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count: 4,
            created_unix_ns: 0,
            generations: Vec::new(),
            format_floors: Vec::new(),
        };
        let key = provisioning_key(&tenant(), Signal::Metrics);
        let err = read_generations_checked(&record, &key, &tenant(), Signal::Metrics)
            .expect_err("a future format version must be refused");
        assert!(matches!(err, ProvisioningError::UnsupportedVersion { .. }));
    }

    /// [`read_generations_checked`] refuses a record misfiled for a different
    /// tenant: a correctly-shaped record at the wrong key must never be
    /// misread as this tenant's generation history and used to route its
    /// writes at another tenant's shard count.
    #[test]
    fn read_generations_checked_rejects_misfiled_tenant() {
        let other = TenantHash([0xEE; 16]);
        let record = sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: other.0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count: 4,
            created_unix_ns: 0,
            generations: Vec::new(),
            format_floors: Vec::new(),
        };
        let key = provisioning_key(&tenant(), Signal::Metrics);
        let err = read_generations_checked(&record, &key, &tenant(), Signal::Metrics)
            .expect_err("a record for a different tenant must be refused");
        assert!(matches!(
            err,
            ProvisioningError::CorruptRecord {
                field: "tenant_hash",
                ..
            }
        ));
    }

    /// [`read_generations_checked`] refuses a record misfiled for a different
    /// signal, the sibling of the tenant-misfile guard above.
    #[test]
    fn read_generations_checked_rejects_misfiled_signal() {
        let record = sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: sysproto::Signal::Logs as i32,
            shard_count: 4,
            created_unix_ns: 0,
            generations: Vec::new(),
            format_floors: Vec::new(),
        };
        let key = provisioning_key(&tenant(), Signal::Metrics);
        let err = read_generations_checked(&record, &key, &tenant(), Signal::Metrics)
            .expect_err("a record for a different signal must be refused");
        assert!(matches!(
            err,
            ProvisioningError::CorruptRecord {
                field: "signal",
                ..
            }
        ));
    }

    /// Each compatibility-rule violation is a typed `CorruptGenerations` decode
    /// error naming the exact defect, never a panic.
    #[test]
    fn corrupt_generation_histories_are_typed_errors() {
        let key = provisioning_key(&tenant(), Signal::Metrics);
        let base =
            |gens: Vec<sysproto::ShardGeneration>, scalar: u32| sysproto::ProvisioningRecord {
                format_version: 1,
                tenant_hash: tenant().0.to_vec(),
                signal: sysproto::Signal::Metrics as i32,
                shard_count: scalar,
                created_unix_ns: 0,
                generations: gens,
                format_floors: Vec::new(),
            };
        let g = |generation, shard_count, activation_hour| sysproto::ShardGeneration {
            generation,
            shard_count,
            activation_hour,
            appended_unix_ns: 0,
        };
        let expect_defect = |record: &sysproto::ProvisioningRecord, defect: GenerationDefect| {
            match read_generations(record, &key) {
                Err(ProvisioningError::CorruptGenerations { defect: got, .. }) => {
                    assert_eq!(got, defect, "wrong defect");
                }
                other => panic!("expected CorruptGenerations({defect:?}), got {other:?}"),
            }
        };

        // generations[0].shard_count != scalar shard_count.
        expect_defect(&base(vec![g(0, 8, 0)], 4), GenerationDefect::ScalarMismatch);
        // Non-dense generation numbers (0, then 2).
        expect_defect(
            &base(vec![g(0, 4, 0), g(2, 8, 100)], 4),
            GenerationDefect::NotDense,
        );
        // activation_hour not strictly increasing (100, then 100).
        expect_defect(
            &base(vec![g(0, 4, 0), g(1, 8, 0)], 4),
            GenerationDefect::ActivationNotIncreasing,
        );
        // Adjacent equal counts (4, then 4).
        expect_defect(
            &base(vec![g(0, 4, 0), g(1, 4, 100)], 4),
            GenerationDefect::AdjacentEqualCount,
        );
        // A count outside 1..=10000.
        expect_defect(
            &base(vec![g(0, 4, 0), g(1, 20_000, 100)], 4),
            GenerationDefect::CountOutOfRange,
        );
    }

    /// A successful reshard append: valid future activation, differing count,
    /// materializes an explicit generation 0 and appends generation 1. The
    /// scalar shard_count is unchanged.
    #[tokio::test]
    async fn append_generation_succeeds_and_materializes_history() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        // now = hour 10 (10 * NS_PER_HOUR); activate at hour 12.
        let now_ns = 10 * NS_PER_HOUR;
        let out = append_generation(store.as_ref(), &tenant(), Signal::Metrics, 8, 12, now_ns)
            .await
            .expect("a valid reshard appends");
        assert_eq!(out.generation, 1);
        assert_eq!(out.generations.len(), 2);
        assert_eq!(out.generations[0].generation, 0);
        assert_eq!(out.generations[0].shard_count, 4);
        assert_eq!(out.generations[0].activation_hour, 0);
        assert_eq!(out.generations[1].shard_count, 8);
        assert_eq!(out.generations[1].activation_hour, 12);

        // Re-read: scalar unchanged, history persisted and valid.
        let record = read_record(
            store.as_ref(),
            &provisioning_key(&tenant(), Signal::Metrics),
        )
        .await
        .expect("read")
        .expect("present");
        assert_eq!(record.shard_count, 4, "scalar shard_count never changes");
        let gens = read_generations(&record, "k").expect("valid history");
        assert_eq!(gens.len(), 2);
        assert_eq!(active_shard_count(&gens, 11), 4, "before activation");
        assert_eq!(active_shard_count(&gens, 12), 8, "at activation");
    }

    /// A reshard whose activation_hour is not strictly in the future is refused.
    #[tokio::test]
    async fn append_generation_rejects_past_activation() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        let now_ns = 10 * NS_PER_HOUR; // hour 10
        // Activation at hour 10 is not strictly in the future.
        let err = append_generation(store.as_ref(), &tenant(), Signal::Metrics, 8, 10, now_ns)
            .await
            .expect_err("activation in the past must be refused");
        assert!(
            matches!(err, ProvisioningError::ActivationInPast { .. }),
            "got: {err}"
        );
    }

    /// A reshard to the same count is a no-op and refused (adjacent counts must
    /// differ).
    #[tokio::test]
    async fn append_generation_rejects_same_count() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        let err = append_generation(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            4,
            12,
            10 * NS_PER_HOUR,
        )
        .await
        .expect_err("a same-count reshard must be refused");
        assert!(
            matches!(err, ProvisioningError::ReshardSameCount { .. }),
            "got: {err}"
        );
    }

    /// A reshard on a (tenant, signal) with no provisioning record is refused.
    #[tokio::test]
    async fn append_generation_rejects_missing_record() {
        let store = mem();
        let err = append_generation(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            8,
            12,
            10 * NS_PER_HOUR,
        )
        .await
        .expect_err("no record to reshard must be refused");
        assert!(
            matches!(err, ProvisioningError::NoRecordToReshard { .. }),
            "got: {err}"
        );
    }

    /// A concurrent append racing the same (tenant, signal): the first CAS wins,
    /// the second reads the same version and its stale CAS write is rejected as a
    /// typed `ReshardCasConflict`, never silently overwriting the winner. Proven
    /// with `FaultStore` stalling the loser's CAS `put` until after the winner
    /// commits.
    #[tokio::test]
    async fn append_generation_cas_conflict_on_concurrent_append() {
        let inner = MemoryStore::new();
        seed_record(&inner, &tenant(), Signal::Metrics, 4).await;
        // Inject a rejected conditional write on the CAS `put` of the prov key,
        // exactly what a concurrent append that moved the version would cause:
        // under `CasVersion` this surfaces as `PreconditionFailed`.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/prov"),
        );
        let store = FaultStore::new(inner, plan);
        let err = append_generation(&store, &tenant(), Signal::Metrics, 8, 12, 10 * NS_PER_HOUR)
            .await
            .expect_err("a CAS precondition failure must surface as a typed conflict");
        assert!(
            matches!(err, ProvisioningError::ReshardCasConflict { .. }),
            "got: {err}"
        );
        assert_eq!(
            store.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::FailedConditionalWrite
            ),
            1,
            "the injected precondition failure must have fired"
        );
    }

    // ---- ADR-0066 format-floor tests (epic EM, EM-T3) ----

    const RSEG: &str = "rseg";
    const RLOG: &str = "rlog";

    /// Read the current floor for a family straight from a record in the store.
    async fn floor_in_store(
        store: &dyn ObjectStoreBackend,
        th: &TenantHash,
        signal: Signal,
        family: &str,
    ) -> Option<u32> {
        current_floor_from_store(store, th, signal, family)
            .await
            .expect("read floor")
    }

    /// Raising a floor for a family with no prior floor creates the first entry;
    /// a second, higher floor for the same family appends and the read returns
    /// the new value.
    #[tokio::test]
    async fn raise_first_then_higher_floor_appends_and_reads_back() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;

        // No floor raised yet: read is None.
        assert_eq!(
            floor_in_store(store.as_ref(), &tenant(), Signal::Metrics, RSEG).await,
            None,
            "no floor raised yet"
        );

        let first = raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RSEG,
            5,
            "job",
            100,
        )
        .await
        .expect("first floor raises");
        assert_eq!(first.floor_version, 5);
        assert_eq!(first.floors.len(), 1);
        assert_eq!(
            floor_in_store(store.as_ref(), &tenant(), Signal::Metrics, RSEG).await,
            Some(5)
        );

        let second = raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RSEG,
            6,
            "job",
            200,
        )
        .await
        .expect("a higher floor appends");
        assert_eq!(second.floor_version, 6);
        assert_eq!(second.floors.len(), 2);
        assert_eq!(
            floor_in_store(store.as_ref(), &tenant(), Signal::Metrics, RSEG).await,
            Some(6),
            "read returns the newly raised floor"
        );

        // The scalar shard_count and generation history are untouched.
        let record = read_record(
            store.as_ref(),
            &provisioning_key(&tenant(), Signal::Metrics),
        )
        .await
        .expect("read")
        .expect("present");
        assert_eq!(
            record.shard_count, 4,
            "shard_count untouched by a floor raise"
        );
        assert!(record.generations.is_empty(), "generations untouched");
    }

    /// Raising a floor at or below the current floor for that family is refused
    /// with a typed error, and the record is unchanged (verified by read-back).
    #[tokio::test]
    async fn raise_at_or_below_current_is_refused_and_writes_nothing() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RSEG,
            6,
            "job",
            100,
        )
        .await
        .expect("floor 6 raised");

        // Equal: not strictly above.
        let err = raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RSEG,
            6,
            "job",
            200,
        )
        .await
        .expect_err("a re-raise to the same value must be refused");
        assert!(
            matches!(
                err,
                ProvisioningError::FloorNotAboveCurrent {
                    requested: 6,
                    current: 6,
                    ..
                }
            ),
            "got: {err}"
        );

        // Lower: also refused.
        let err = raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RSEG,
            3,
            "job",
            300,
        )
        .await
        .expect_err("a lower floor must be refused");
        assert!(
            matches!(
                err,
                ProvisioningError::FloorNotAboveCurrent {
                    requested: 3,
                    current: 6,
                    ..
                }
            ),
            "got: {err}"
        );

        // The record still records exactly the one floor at 6.
        let floors = read_floors_from_store(store.as_ref(), &tenant(), Signal::Metrics)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(floors.len(), 1, "no floor was appended");
        assert_eq!(floors[0].floor_version, 6);
    }

    /// Two families' floors are tracked independently: raising one never affects
    /// the other's recorded value.
    #[tokio::test]
    async fn two_families_are_independent() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RSEG,
            6,
            "job",
            100,
        )
        .await
        .expect("rseg floor");
        raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RLOG,
            2,
            "job",
            200,
        )
        .await
        .expect("rlog floor");
        // Raising rlog again does not touch rseg.
        raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RLOG,
            3,
            "job",
            300,
        )
        .await
        .expect("rlog floor raised");

        assert_eq!(
            floor_in_store(store.as_ref(), &tenant(), Signal::Metrics, RSEG).await,
            Some(6),
            "rseg unchanged by rlog raises"
        );
        assert_eq!(
            floor_in_store(store.as_ref(), &tenant(), Signal::Metrics, RLOG).await,
            Some(3)
        );
        // A family with no floor reads None even when others exist.
        assert_eq!(
            floor_in_store(store.as_ref(), &tenant(), Signal::Metrics, "rspan").await,
            None
        );
    }

    /// A concurrent CAS race: the loser reads the same version and its stale CAS
    /// write is rejected as a typed `FloorCasConflict`, never a silent overwrite.
    /// Proven with `FaultStore` turning the loser's CAS `put` into a
    /// `PreconditionFailed`, exactly what a concurrent write that moved the
    /// version would cause.
    #[tokio::test]
    async fn raise_floor_cas_conflict_on_concurrent_write() {
        let inner = MemoryStore::new();
        seed_record(&inner, &tenant(), Signal::Metrics, 4).await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/prov"),
        );
        let store = FaultStore::new(inner, plan);
        let err = raise_format_floor(&store, &tenant(), Signal::Metrics, RSEG, 5, "job", 100)
            .await
            .expect_err("a CAS precondition failure must surface as a typed conflict");
        assert!(
            matches!(err, ProvisioningError::FloorCasConflict { .. }),
            "got: {err}"
        );
        assert_eq!(
            store.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::FailedConditionalWrite
            ),
            1,
            "the injected precondition failure must have fired"
        );
    }

    /// Raising a floor on a record misfiled under the wrong tenant fails closed
    /// (the misfile guard), rather than durably corrupting it with an append.
    #[tokio::test]
    async fn raise_floor_rejects_misfiled_tenant() {
        let store = mem();
        // A well-formed record whose body names a different tenant, written under
        // this tenant's prov key: a misfile.
        let other = TenantHash([0xEE; 16]);
        let record = build_record(&other, Signal::Metrics, 4, 1_000);
        store
            .put(
                &provisioning_key(&tenant(), Signal::Metrics),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("put misfiled record");
        let err = raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RSEG,
            5,
            "job",
            100,
        )
        .await
        .expect_err("a misfiled record must be refused, not extended");
        assert!(
            matches!(
                err,
                ProvisioningError::CorruptRecord {
                    field: "tenant_hash",
                    ..
                }
            ),
            "got: {err}"
        );
    }

    /// Raising a floor for a signal misfiled under another signal's body also
    /// fails closed.
    #[tokio::test]
    async fn raise_floor_rejects_misfiled_signal() {
        let store = mem();
        // Record body says Logs, written under the Metrics prov key.
        let record = build_record(&tenant(), Signal::Logs, 4, 1_000);
        store
            .put(
                &provisioning_key(&tenant(), Signal::Metrics),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("put misfiled record");
        let err = raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RSEG,
            5,
            "job",
            100,
        )
        .await
        .expect_err("a signal-misfiled record must be refused");
        assert!(
            matches!(
                err,
                ProvisioningError::CorruptRecord {
                    field: "signal",
                    ..
                }
            ),
            "got: {err}"
        );
    }

    /// A corrupt (non-monotonic-per-family) floor history decodes as a typed
    /// error, never a panic — the same fail-closed discipline the generation
    /// history follows. Also covers the empty-family and zero-floor defects.
    #[test]
    fn corrupt_floor_histories_are_typed_errors() {
        let key = provisioning_key(&tenant(), Signal::Metrics);
        let base = |floors: Vec<sysproto::FormatFloor>| sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count: 4,
            created_unix_ns: 0,
            generations: Vec::new(),
            format_floors: floors,
        };
        let ff = |family: &str, floor_version: u32| sysproto::FormatFloor {
            family: family.to_string(),
            floor_version,
            raised_unix_ns: 0,
            raised_by: String::new(),
        };
        let expect_defect =
            |record: &sysproto::ProvisioningRecord, defect: FloorDefect| match read_floors(
                record, &key,
            ) {
                Err(ProvisioningError::CorruptFloors { defect: got, .. }) => {
                    assert_eq!(got, defect, "wrong defect");
                }
                other => panic!("expected CorruptFloors({defect:?}), got {other:?}"),
            };

        // Same family, later entry lower than earlier: not increasing.
        expect_defect(
            &base(vec![ff(RSEG, 6), ff(RSEG, 5)]),
            FloorDefect::NotIncreasing,
        );
        // Same family, equal repeat: also not strictly increasing.
        expect_defect(
            &base(vec![ff(RSEG, 6), ff(RSEG, 6)]),
            FloorDefect::NotIncreasing,
        );
        // Empty family.
        expect_defect(&base(vec![ff("", 5)]), FloorDefect::EmptyFamily);
        // Zero floor_version.
        expect_defect(&base(vec![ff(RSEG, 0)]), FloorDefect::ZeroFloor);
        // Uppercase family: family lookup is an exact string match, so this
        // would silently be a second family rather than RSEG.
        expect_defect(&base(vec![ff("RSEG", 5)]), FloorDefect::NotLowercase);

        // A valid interleaved history of two families decodes cleanly.
        let ok = base(vec![ff(RSEG, 5), ff(RLOG, 2), ff(RSEG, 6), ff(RLOG, 3)]);
        let floors = read_floors(&ok, &key).expect("a per-family-increasing history is valid");
        assert_eq!(floors.len(), 4);
        assert_eq!(current_floor(&floors, RSEG), Some(6));
        assert_eq!(current_floor(&floors, RLOG), Some(3));
    }

    /// Reading the floor for a family that never had one raised returns None,
    /// not an error, even on a record that carries floors for other families.
    #[tokio::test]
    async fn read_floor_for_never_raised_family_is_none() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        // No record-absent case and a present-but-unraised case both read None.
        assert_eq!(
            floor_in_store(store.as_ref(), &tenant(), Signal::Metrics, RSEG).await,
            None,
            "present record, family never raised"
        );
        assert_eq!(
            floor_in_store(store.as_ref(), &tenant(), Signal::Logs, RSEG).await,
            None,
            "no record for this signal at all"
        );
    }

    /// Raising a floor on a (tenant, signal) with no provisioning record is
    /// refused: a floor extends an existing record.
    #[tokio::test]
    async fn raise_floor_rejects_missing_record() {
        let store = mem();
        let err = raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            RSEG,
            5,
            "job",
            100,
        )
        .await
        .expect_err("no record to floor must be refused");
        assert!(
            matches!(err, ProvisioningError::NoRecordForFloor { .. }),
            "got: {err}"
        );
    }

    /// An empty family is refused up front, before any store touch.
    #[tokio::test]
    async fn raise_floor_rejects_empty_family() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        let err = raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            "",
            5,
            "job",
            100,
        )
        .await
        .expect_err("an empty family must be refused");
        assert!(
            matches!(err, ProvisioningError::FloorFamilyEmpty),
            "got: {err}"
        );
    }

    /// An uppercase family is refused up front, before any store touch: family
    /// lookup is an exact string match, so an uppercase raise would silently
    /// create a second, independent family rather than extending the intended
    /// one.
    #[tokio::test]
    async fn raise_floor_rejects_uppercase_family() {
        let store = mem();
        seed_record(store.as_ref(), &tenant(), Signal::Metrics, 4).await;
        let err = raise_format_floor(
            store.as_ref(),
            &tenant(),
            Signal::Metrics,
            "RSEG",
            5,
            "job",
            100,
        )
        .await
        .expect_err("an uppercase family must be refused");
        assert!(
            matches!(err, ProvisioningError::FloorFamilyNotLowercase(ref f) if f == "RSEG"),
            "got: {err}"
        );
    }

    /// A record built at first write carries no floors, and its bytes are
    /// unchanged by this field (additive: the empty list is omitted on encode),
    /// so a pre-EM reader is unaffected.
    #[test]
    fn built_record_has_no_floors_and_is_byte_compatible() {
        let built = build_record(&tenant(), Signal::Metrics, 4, 1_234);
        assert!(
            built.format_floors.is_empty(),
            "a freshly built record carries no floors"
        );
        let key = provisioning_key(&tenant(), Signal::Metrics);
        let floors = read_floors(&built, &key).expect("empty floor history is valid");
        assert!(floors.is_empty(), "empty history reads as no floors");
        assert_eq!(current_floor(&floors, RSEG), None);
    }
}
