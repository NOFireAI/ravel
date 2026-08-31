//! The reconciled MetricsBench report schema (ADR-0927, issue #936, task T7).
//!
//! Two provenance types existed before this module, and ADR-0927's Consequences
//! section names the trap directly: `sql_latency::Provenance` records the
//! object-store backend and a long list of executor knobs but no git commit,
//! toolchain, digest or hardware identification, while `report::Environment`
//! records the git commit and toolchain but no object-store accounting.
//! "T7 must reconcile them; extending one in ignorance of the other produces a
//! third shape."
//!
//! This module is that reconciliation: one [`Provenance`] carrying everything
//! either type recorded, plus everything ADR-0927 requires that neither had (a
//! schema version, the toolchain and git commit *together with* the backend and
//! its accounting, comparator versions and image digests, generator and corpus
//! digests, the protocol, the hardware, and the complete non-default
//! configuration). It does not grow a third bespoke flat struct that copies
//! forty named fields: the two existing types' bench-specific knobs live in the
//! generic [`ConfigEntry`] list (`store_backend`/`region` become named
//! [`Backend`] fields, `shard_count`/`max_flush_delay_ms`/the SQL executor
//! ceilings become config entries), so every field either type recorded is
//! representable here without duplicating its shape.
//!
//! The two existing types are the serialization of two already-shipped
//! benchmarks (`SqlLatencyReport`, `BenchReport`) and carry `#[serde(default)]`
//! fields precisely so old reports still deserialize; nesting them under a
//! shared core would break that back-compatibility, so they are left in place
//! and this is the single shape MetricsBench and any bench that adopts it
//! converge on.
//!
//! ## What this module guarantees
//!
//! - **Fail-closed validation** ([`validate`]): a missing, duplicate,
//!   non-finite, negative, or malformed measurement all fail, and an
//!   absent-but-expected figure fails exactly like an out-of-band one
//!   (ADR-0927's "Bands pre-registered" decision 5). "Exit 0" from a consumer
//!   means the report was checked and stood, not merely that the tool ran.
//! - **A renderer** ([`render`]) that derives its table FROM the artifact and
//!   is never hand-maintained. It follows the ClickBench runbook script's proven
//!   design: integrity is asserted by identity before any band is applied, the
//!   bands are named constants in one block, and a stated gap is a loud SKIP
//!   rather than a silently unasserted figure.
//! - **Per-query results are the source of truth.** A geometric mean may be
//!   included but never replaces the per-query rows, and the renderer cannot
//!   print a summary line without the rows behind it.
//! - **Every cost estimate carries the retry caveat** ([`RETRY_CAVEAT`],
//!   ADR-0927 decision 8, issue #928): request counts are logical-call counts,
//!   not billed requests, and the caveat is in the rendered output rather than
//!   only in a doc.
//!
//! Report-only, like the rest of `ravel-bench`: this never changes library
//! behaviour, it only describes and checks a measurement.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::allocator::Allocator;
use crate::promql_corpus::CostClass;

/// The report schema version every writer stamps: the newest version this build
/// emits. A report declaring a version outside [`SUPPORTED_VERSIONS`] is refused
/// rather than parsed optimistically (the same contract
/// [`crate::promql_corpus::CORPUS_FORMAT_VERSION`] carries): the field exists so
/// a schema change is a loud error, not a silently misread document.
///
/// Version 2 adds `Provenance::allocator` (issue #972). Two document shapes
/// under one version number would defeat the field: a reader built before the
/// allocator existed would accept a version-1 document it cannot fully read, and
/// nothing would tell a consumer which shape it holds.
pub const SCHEMA_VERSION: u32 = 2;

/// The set of report schema versions this build reads. Writers always stamp
/// [`SCHEMA_VERSION`]; readers accept that version and the immediately preceding
/// one, so a report emitted before version 2 added the allocator field still
/// parses (its absent `allocator` takes [`Allocator::Unknown`] from the serde
/// default) instead of being refused for a field it could not have carried.
///
/// The shape follows the persistent-format crates' `SUPPORTED_VERSIONS`
/// (`ravel_segment::format`, `ravel_logseg::footer`) so a reviewer reads one
/// idiom everywhere: a window at most two versions wide, never accepting
/// anything below its floor, and the single source both [`validate`] and the
/// error message read.
pub const SUPPORTED_VERSIONS: SupportedVersions = SupportedVersions::n_and_prev(SCHEMA_VERSION);

/// A window of accepted schema versions, the report-schema counterpart of the
/// persistent-format crates' type of the same name. `u32` here because
/// `schema_version` is a JSON number field rather than a packed trailer field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportedVersions {
    newest: u32,
    oldest: u32,
}

impl SupportedVersions {
    /// A window accepting exactly one version. The shape to return to once the
    /// version-1 reader is retired.
    pub const fn single(version: u32) -> Self {
        Self {
            newest: version,
            oldest: version,
        }
    }

    /// The N/N-1 window: accept `newest` and the immediately preceding version.
    /// The shape today, so a version-1 report (no `allocator` key) still parses.
    pub const fn n_and_prev(newest: u32) -> Self {
        // `newest` is always a real schema version (>= 1), so the predecessor
        // never underflows.
        Self {
            newest,
            oldest: newest - 1,
        }
    }

    /// The newest (always-written) version.
    pub const fn newest(&self) -> u32 {
        self.newest
    }

    /// The oldest accepted version, the window floor.
    pub const fn oldest(&self) -> u32 {
        self.oldest
    }

    /// Whether `version` is inside the accepted window.
    pub const fn contains(&self, version: u32) -> bool {
        version >= self.oldest && version <= self.newest
    }
}

impl std::fmt::Display for SupportedVersions {
    /// Renders every accepted version as a set (`{1, 2}`). A window is a set of
    /// versions, and an error message that printed one number while several are
    /// accepted would send a reader looking for the wrong mismatch.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("{")?;
        for (position, version) in (self.oldest..=self.newest).enumerate() {
            if position > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{version}")?;
        }
        f.write_str("}")
    }
}

/// The retry-blindness caveat every cost estimate carries (ADR-0927 decision 8,
/// issue #928). `object_store` retries below `InstrumentedStore` with
/// `max_retries = 10`, so one logical `get()` that retried nine times records
/// one request while S3 bills ten. Every request figure in *this bench report*
/// is a logical-call count, not a billed-request count, and under throttling the
/// real bill exceeds every counted number.
///
/// The gap is no longer unmeasured: issue #928 added a per-operation `attempts`
/// counter (`StoreMetrics::record_attempt`, surfaced as `ravel_store_attempts_total`
/// at the server's `/metrics`) that the S3 adapter's counting HTTP connector fills
/// in below the retry loop, so `attempts - calls` is the billed retry overhead.
/// This bench harness does not install that connector, so its own request figures
/// stay logical-call counts; the caveat says so rather than implying the gap is
/// unmeasurable anywhere. This lives in the rendered output, not only in a doc.
pub const RETRY_CAVEAT: &str = "request counts here are logical-call counts, not billed requests: \
    object_store retries below InstrumentedStore (max_retries=10), so under throttling the real \
    bill exceeds every counted number. This bench does not install the counting HTTP connector; \
    the billed-request count is measured separately as attempts (ravel_store_attempts_total) \
    (ADR-0927 decision 8, #928)";

/// The statuses ADR-0927 decision 6 fixes. Every outcome has exactly one, and
/// only [`ResultStatus::Ok`] is admissible in a timing table. Kept as a typed
/// enum rather than a bare string so a status the harness cannot spell is a
/// deserialization error, not a silently dropped row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultStatus {
    /// Answered, and the oracle agrees. The only status that is timed.
    Ok,
    /// Answered, but the oracle disagrees.
    Incorrect,
    /// Answered from an incomplete result set, and correct as far as it goes.
    Partial,
    /// No answer within the per-query deadline.
    Timeout,
    /// A transport or server error that is none of the above.
    Error,
    /// The engine does not implement the query.
    UnsupportedConstruct,
    /// The engine implements it and deliberately declined, returning no result.
    Refused,
}

impl ResultStatus {
    /// Whether a latency from a measurement with this status is admissible in a
    /// performance table. Only `ok` is timed (ADR-0927 decision 6).
    pub fn is_timed(self) -> bool {
        matches!(self, ResultStatus::Ok)
    }

    /// The slug this status renders as.
    pub fn slug(self) -> &'static str {
        match self {
            ResultStatus::Ok => "ok",
            ResultStatus::Incorrect => "incorrect",
            ResultStatus::Partial => "partial",
            ResultStatus::Timeout => "timeout",
            ResultStatus::Error => "error",
            ResultStatus::UnsupportedConstruct => "unsupported_construct",
            ResultStatus::Refused => "refused",
        }
    }
}

/// The hardware a run measured on. `instance_type` is `Option` because it is
/// knowable on a cloud host and not on a laptop; ADR-0927 asks for it "where
/// knowable" and a sentinel string would read as a real instance type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hardware {
    /// `uname -srm` or equivalent.
    pub os: String,
    /// The human CPU model string (`/proc/cpuinfo`'s `model name`).
    pub cpu_model: String,
    /// Logical cores on the measuring host. Zero is never valid.
    pub logical_cores: u32,
    /// The cloud instance type, when the host can name it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
}

/// The object-store backend a run measured against, and whether its requests
/// are billed. Reconciles `sql_latency::Provenance`'s
/// `store_backend`/`region`/`endpoint` with `report::Environment`'s
/// `store_backend`/`region` and `RequestCounts::backend_bills_requests`, so the
/// honest "these counts are real but free" (`MemoryStore`) versus "these counts
/// are billable" (S3) distinction rides beside the backend (ADR-0927 decision
/// 8, ADR-0075 decision 3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Backend {
    /// `"memory"`, `"minio"`, or `"s3"`.
    pub store_backend: String,
    /// Backend region, or the sentinel `"n/a"` for a backend with none, keeping
    /// the no-null contract.
    pub region: String,
    /// Backend endpoint, or `"n/a"` when the backend has none.
    pub endpoint: String,
    /// Whether a request against this backend is billed. `false` for
    /// `MemoryStore` (real counts, but free); `true` for S3. Only the real-S3
    /// lane produces a publishable cost claim (ADR-0927 decision 10).
    pub backend_bills_requests: bool,
}

/// One comparator engine a portable-lane run was measured against, pinned by
/// version and image digest (ADR-0927 decision 4). A Ravel-only diagnostic run
/// carries an empty comparator list; a portable-lane run that names a comparator
/// must name it completely, which [`validate`] enforces.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Comparator {
    /// The engine name, e.g. `"prometheus"`, `"victoriametrics"`.
    pub name: String,
    /// The engine version, e.g. `"3.13.1"`.
    pub version: String,
    /// The container image digest the deployment was pinned to.
    pub image_digest: String,
}

/// One non-default configuration setting the run applied. The complete
/// non-default configuration (ADR-0927 decision requirement) is carried as a
/// list of these rather than as a fixed set of named fields, so both
/// `report::Environment`'s `shard_count`/`max_flush_delay_ms` and
/// `sql_latency::Provenance`'s executor ceilings map into one shape without this
/// type having to grow a named field per knob of every bench that adopts it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigEntry {
    /// The setting name, as the CLI flag or config key spells it.
    pub key: String,
    /// The value it was set to, rendered as a string so heterogeneous knobs
    /// share one shape.
    pub value: String,
}

/// The reconciled provenance block: everything a MetricsBench figure needs
/// beside it to be evidence, and a schema version so the shape cannot change
/// silently.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    /// The report schema version, which must be one of [`SUPPORTED_VERSIONS`].
    /// A writer always stamps [`SCHEMA_VERSION`].
    pub schema_version: u32,
    /// The Ravel git commit the numbers describe.
    pub ravel_git_commit: String,
    /// The Rust toolchain that built the measuring binary (`rustc --version`).
    pub toolchain: String,
    /// The ingest/query protocol the run drove, e.g. `"remote_write_1.0"` for
    /// the portable lane or `"in_process_promql"` for a diagnostic run. Named
    /// because a Remote Write 1.0 figure is never comparable with a 2.0 or OTLP
    /// figure (ADR-0927 decision 2).
    pub protocol: String,
    /// The measuring host.
    pub hardware: Hardware,
    /// The object-store backend and its billing status.
    pub backend: Backend,
    /// The comparator engines, pinned by version and image digest. Empty for a
    /// Ravel-only diagnostic run.
    #[serde(default)]
    pub comparators: Vec<Comparator>,
    /// A digest identifying the deterministic generator, so a report names
    /// exactly which samples it measured: the `generation.digest`
    /// `metricsbench_gen` stamps on a real run, or the workload manifest's
    /// content digest a provenance stamp uses before a run exists.
    pub generator_digest: String,
    /// The content digest of the PromQL corpus the run measured, so a report
    /// names exactly which queries it ran.
    pub corpus_digest: String,
    /// The complete non-default configuration.
    #[serde(default)]
    pub config: Vec<ConfigEntry>,
    /// The heap allocator the measuring process actually ran under, resolved at
    /// runtime from its mapped libraries (`crate::allocator::active_allocator`):
    /// `"tcmalloc"`, `"jemalloc"`, `"mimalloc"`, `"system"` (glibc/musl), or
    /// `"unknown"` when the probe could not answer. Peak RSS moves by about 2x
    /// between the system allocator and a memory-returning one, and the allocator
    /// can arrive via `LD_PRELOAD` a compile-time `cfg!` cannot see, so it is read
    /// off `/proc/self/maps` (issue #972). Typed as [`Allocator`] so the value
    /// domain is shared with `sql_latency::Provenance` and an out-of-domain
    /// value is unrepresentable rather than merely rejected. Unlike the identity
    /// fields above, [`Allocator::Unknown`] is a legitimate recorded value here
    /// (the probe ran and could not answer), so it is deliberately NOT in
    /// [`checked_provenance_fields`]: an explicit unknown is honest, and a
    /// guessed allocator that read as verified would be the defect this closes.
    /// Added at schema version 2, so a version-1 report (which cannot carry the
    /// key) deserializes to [`Allocator::Unknown`]; a version-2 report that omits
    /// the key reads as `Unknown` too, which is the same honest answer rather
    /// than a defect. A report carrying an unrecognized allocator string is
    /// rejected at deserialize.
    #[serde(default = "default_allocator")]
    pub allocator: Allocator,
}

/// What a report written before the allocator was recorded (issue #972)
/// deserializes to: the explicit unknown, never a guessed allocator.
fn default_allocator() -> Allocator {
    Allocator::Unknown
}

/// One figure a measurement reports, named so the report is self-describing and
/// a consumer can find a figure by name rather than by position.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Figure {
    /// The figure name, e.g. `"median_ms"`, `"object_store_get_requests"`.
    pub name: String,
    /// The value. Validated finite and non-negative: a benchmark reports no
    /// negative latency or byte count, and a NaN would make every band
    /// comparison against it read as MET (the exact trap the runbook's
    /// `byte_count` guards against).
    pub value: f64,
}

/// One measured corpus query: its id, cost class, per-engine status, and the
/// figures behind it. Per-query results are the source of truth (ADR-0927): the
/// report-level geometric mean is derived from these and never replaces them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Measurement {
    /// The corpus entry id (`CorpusEntry::id`). Report rows are keyed by it, so
    /// it must be unique within a report.
    pub id: String,
    /// The cost class the entry falls into (ADR-0927 decision 5).
    pub class: CostClass,
    /// The outcome status (ADR-0927 decision 6). Only [`ResultStatus::Ok`] is
    /// timed.
    pub status: ResultStatus,
    /// The figures this measurement reports. An `ok` measurement must carry the
    /// timing figures in [`REQUIRED_TIMED_FIGURES`]; a non-`ok` measurement is
    /// not timed and carries none of them.
    #[serde(default)]
    pub figures: Vec<Figure>,
}

impl Measurement {
    /// The value of the figure named `name`, or `None` if absent.
    pub fn figure(&self, name: &str) -> Option<f64> {
        self.figures
            .iter()
            .find(|f| f.name == name)
            .map(|f| f.value)
    }
}

/// The timing figures every `ok` (timed) measurement must carry. An `ok`
/// measurement missing any of them fails validation exactly like an out-of-band
/// figure (ADR-0927 decision 5: "absent-but-expected fails identically to
/// out-of-band"). Non-`ok` measurements must carry none of them, because only
/// `ok` is timed.
pub const REQUIRED_TIMED_FIGURES: &[&str] = &["min_ms", "median_ms", "max_ms"];

/// The relative tolerance the report-level `geomean_ms` is checked against the
/// geometric mean recomputed from the timed rows' `median_ms`. A supplied value
/// farther than this fraction from the recomputed one is rejected as a mismatch
/// ([`ValidationError::GeomeanMismatch`]): "present but unverified" is the weaker
/// half of the guarantee, so the value is checked against the rows it claims to
/// summarize, not merely required to exist.
///
/// Relative, not absolute: the geomean scales with the medians, and a regime can
/// range from microseconds to seconds (six orders of magnitude), so a fixed
/// millisecond slop that is tight for one regime is meaningless for another.
///
/// The value is `1e-9`. Recomputing `exp(mean(ln median))` accumulates f64
/// rounding error on the order of `n * f64::EPSILON` (about `1e-14` relative for
/// dozens of rows), and a producer that computes the geomean by a
/// different-but-valid method (a scaled product rather than a log-sum) adds a
/// little more; `1e-9` sits several orders above that floor, so a faithfully
/// computed value always passes. A real disagreement (a stale or hand-typed
/// summary, or a geomean that summarizes the wrong rows) is a multiplicative
/// factor far larger than `1e-9` and is always caught.
pub const GEOMEAN_REL_TOLERANCE: f64 = 1e-9;

/// The full reconciled report: the provenance, the per-query measurements, and
/// an optional report-level geometric mean.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsBenchReport {
    /// The provenance block.
    pub provenance: Provenance,
    /// The per-query measurements, the source of truth.
    pub measurements: Vec<Measurement>,
    /// A report-level geometric mean of the timed medians, if the producer
    /// computed one. It may be included but never replaces the per-query rows;
    /// [`render`] cannot print it without them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub geomean_ms: Option<f64>,
}

/// Everything [`validate`] can reject, one variant per failure class the task
/// enumerates. Each names what is at fault and, where a figure is involved,
/// which measurement and which figure, so the message is the signal a consumer
/// acts on.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ValidationError {
    /// The report declares a schema version this build does not read. The error
    /// names the whole accepted set, not one number: several versions are
    /// readable, and reporting a single one would misdescribe the mismatch.
    #[error(
        "report declares schema version {found}, but this build reads versions {expected}; a \
         schema change is a version bump, never a silently misread document"
    )]
    UnsupportedSchemaVersion {
        /// The version the document declared.
        found: u32,
        /// The versions this build reads ([`SUPPORTED_VERSIONS`]).
        expected: SupportedVersions,
    },
    /// A required provenance field is absent (an empty string, the `"unknown"`
    /// sentinel, an empty digest, zero logical cores). An absent field is not
    /// evidence.
    #[error(
        "required provenance field `{field}` is missing or blank; a figure without its \
         provenance is not evidence (ADR-0927)"
    )]
    MissingProvenanceField {
        /// The field that was missing or blank.
        field: &'static str,
    },
    /// A comparator was named without a complete pin. A portable-lane comparator
    /// that cannot be reproduced is worse than none.
    #[error(
        "comparator at index {index} is missing its `{field}`; ADR-0927 decision 4 pins every \
         comparator by version and image digest"
    )]
    IncompleteComparator {
        /// The comparator's index in the list.
        index: usize,
        /// The field that was missing or blank.
        field: &'static str,
    },
    /// The report carries no measurements. Per-query results are the source of
    /// truth, and a report with none measures nothing.
    #[error("report carries no measurements; per-query results are the source of truth")]
    NoMeasurements,
    /// Two measurements share an id. A later row silently overwriting an earlier
    /// one keeps the count correct while changing which figures are checked (the
    /// runbook's duplicate-id trap).
    #[error(
        "measurement id `{id}` appears more than once; a duplicate row would overwrite an \
         earlier one and change which figures are checked"
    )]
    DuplicateMeasurement {
        /// The duplicated id.
        id: String,
    },
    /// A measurement is structurally malformed: a blank figure name, or an `ok`
    /// row with no figures, or a non-`ok` row carrying a timing figure it must
    /// not.
    #[error("measurement `{id}` is malformed: {reason}")]
    MalformedMeasurement {
        /// The offending measurement id.
        id: String,
        /// What is wrong with it.
        reason: String,
    },
    /// A figure value is not finite. A NaN is the worst case: every band
    /// comparison against it is False, so a malformed report would read as a
    /// pass (the runbook's `byte_count` trap).
    #[error(
        "measurement `{id}` figure `{figure}` is not finite ({value}); a NaN or infinity would \
         make a band comparison read as met"
    )]
    NonFiniteFigure {
        /// The offending measurement id.
        id: String,
        /// The offending figure name.
        figure: String,
        /// The value that was not finite.
        value: f64,
    },
    /// A figure value is negative. A benchmark reports no negative latency or
    /// byte count, and a negative would drag a total toward a passing figure.
    #[error(
        "measurement `{id}` figure `{figure}` is negative ({value}); no benchmark figure is \
         negative and one would drag a total toward a passing value"
    )]
    NegativeFigure {
        /// The offending measurement id.
        id: String,
        /// The offending figure name.
        figure: String,
        /// The negative value.
        value: f64,
    },
    /// A timed (`ok`) measurement is missing a figure it is required to carry.
    /// An absent-but-expected figure fails identically to an out-of-band one
    /// (ADR-0927 decision 5).
    #[error(
        "timed measurement `{id}` is missing required figure `{figure}`; an absent-but-expected \
         figure fails identically to an out-of-band one (ADR-0927 decision 5)"
    )]
    MissingExpectedFigure {
        /// The offending measurement id.
        id: String,
        /// The figure that was expected and absent.
        figure: &'static str,
    },
    /// The report-level geometric mean is present but not finite and positive. A
    /// summary figure that is a NaN or non-positive is not a summary.
    #[error("report geomean_ms is present but not finite and positive ({value})")]
    BadGeomean {
        /// The offending value.
        value: f64,
    },
    /// The report carries a geometric mean but not one measurement is timed. A
    /// summary figure with no per-query row behind it is not a result: per-query
    /// results are the source of truth and a geomean may accompany them but never
    /// stands in for them (ADR-0927).
    #[error(
        "report carries geomean_ms {value} but no measurement is timed; a summary figure with no \
         per-query row behind it is not a result (per-query results are the source of truth, \
         ADR-0927)"
    )]
    GeomeanWithoutTimedRow {
        /// The geomean value that had nothing behind it.
        value: f64,
    },
    /// The report-level geometric mean disagrees with the geometric mean
    /// recomputed from the timed rows' `median_ms`, beyond
    /// [`GEOMEAN_REL_TOLERANCE`]. A summary that contradicts the rows it claims
    /// to summarize is two results, not one; the error carries both so the
    /// disagreement is visible, not just the field name.
    #[error(
        "report geomean_ms {supplied} disagrees with the geometric mean {computed} recomputed \
         from the timed medians (relative tolerance {tolerance:e}); a summary that contradicts \
         its rows is not a result",
        tolerance = GEOMEAN_REL_TOLERANCE
    )]
    GeomeanMismatch {
        /// The value the report supplied.
        supplied: f64,
        /// The value recomputed from the timed medians.
        computed: f64,
    },
    /// A configuration entry has a blank key or value, or two entries share a
    /// key. A blank key or value is unrecorded configuration masquerading as
    /// recorded; two entries for one key make the applied value ambiguous. Both
    /// leave the run unreproducible while the block reads as complete, which is
    /// worse than an obviously missing field.
    #[error(
        "configuration entry at index {index} is invalid: {reason}; a blank or duplicated setting \
         is unreproducible configuration masquerading as complete"
    )]
    InvalidConfigEntry {
        /// The entry's index in the config list.
        index: usize,
        /// What is wrong with it.
        reason: String,
    },
    /// A timed measurement's timing figures are not ordered
    /// `min_ms <= median_ms <= max_ms`. An impossible latency range (a min above
    /// its median, or a median above its max) is a self-contradicting row; the
    /// error carries all three so the impossible triple is visible, not just the
    /// row id.
    #[error(
        "timed measurement `{id}` has timing figures out of order: min_ms {min_ms}, median_ms \
         {median_ms}, max_ms {max_ms}; a timed row requires min_ms <= median_ms <= max_ms"
    )]
    TimingOutOfOrder {
        /// The offending measurement id.
        id: String,
        /// The reported minimum.
        min_ms: f64,
        /// The reported median.
        median_ms: f64,
        /// The reported maximum.
        max_ms: f64,
    },
    /// A comparator's `image_digest` is present but not a content digest: a
    /// mutable tag (`latest`) or a truncated or upper-case hex string pins no
    /// bytes. A digest that does not pin bytes defeats the reason a digest is
    /// recorded, the same defect as a missing pin one layer out.
    #[error(
        "comparator at index {index} has image_digest `{value}`, which is not a content digest; a \
         digest must be `sha256:` followed by 64 lower-case hex characters, or a moving tag pins \
         nothing"
    )]
    InvalidImageDigest {
        /// The comparator's index in the list.
        index: usize,
        /// The value that was not a valid digest.
        value: String,
    },
}

/// Whether a *present* provenance value is an absent-value sentinel rather than
/// real data: blank (after trimming), or the `"unknown"` marker a gatherer on a
/// host without git or rustc used to emit. Every present provenance string flows
/// through this one predicate so the check cannot regress to a weaker
/// blank-only test on any single field (the exact defect this function closes:
/// eight fields rejected the sentinel while comparators and populated backend or
/// hardware fields rejected only blanks).
///
/// `"n/a"` is deliberately NOT a sentinel here: on `backend.region` /
/// `backend.endpoint` it is a real value meaning "this backend has none", not
/// missing data, so a field that legitimately carries it still passes. This
/// predicate governs the value of a field that IS present; a legitimately
/// optional field (`hardware.instance_type`) is exempt by being absent, never by
/// carrying a sentinel.
fn is_absent_value(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.is_empty() || trimmed.eq_ignore_ascii_case("unknown")
}

/// Every provenance string field whose presence must be real, paired with the
/// name a [`ValidationError::MissingProvenanceField`] reports. Coverage is by
/// construction: a field is checked once it is listed here, and it is checked
/// with the full [`is_absent_value`] predicate, never a hand-written weaker one,
/// because the caller applies that single predicate to every row. Adding a
/// provenance string field is one row here; a reviewer confirms the list against
/// the [`Provenance`] struct.
///
/// Optional fields (`hardware.instance_type`) appear only when present: an
/// absent optional field is legitimately absent, but a present one must carry a
/// real value, not a sentinel. `backend.region` / `backend.endpoint` are
/// included because [`is_absent_value`] treats their legitimate `"n/a"` as real
/// data, so listing them rejects blank and `"unknown"` without breaking the
/// no-endpoint case.
fn checked_provenance_fields(p: &Provenance) -> Vec<(&'static str, &str)> {
    let mut fields = vec![
        ("ravel_git_commit", p.ravel_git_commit.as_str()),
        ("toolchain", p.toolchain.as_str()),
        ("protocol", p.protocol.as_str()),
        ("hardware.os", p.hardware.os.as_str()),
        ("hardware.cpu_model", p.hardware.cpu_model.as_str()),
        ("backend.store_backend", p.backend.store_backend.as_str()),
        ("backend.region", p.backend.region.as_str()),
        ("backend.endpoint", p.backend.endpoint.as_str()),
        ("generator_digest", p.generator_digest.as_str()),
        ("corpus_digest", p.corpus_digest.as_str()),
    ];
    if let Some(instance_type) = p.hardware.instance_type.as_deref() {
        fields.push(("hardware.instance_type", instance_type));
    }
    fields
}

/// Whether `value` is a content digest that pins bytes, rather than a mutable
/// tag: `sha256:` followed by exactly 64 lower-case hex characters. This is the
/// same notion `deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh`
/// enforces on the deployment side (`@sha256:` + 64 `[0-9a-f]`), minus the `@`
/// that belongs to a full `repo:tag@digest` image reference: this field carries
/// the bare digest, not a whole reference. Only `sha256` is accepted: it is the
/// only algorithm the comparator images are published under, and an open-ended
/// `algo:hex` pattern would readmit a truncated or upper-case string that pins
/// nothing. Upper-case hex is rejected deliberately, matching the deploy check's
/// lower-case-only class, so the two cannot disagree on the same digest.
fn is_content_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Validate the provenance block, fail-closed. Every present identity field must
/// carry a real value (not blank, not the `"unknown"` sentinel), the schema
/// version must be one this build reads ([`SUPPORTED_VERSIONS`]), the hardware
/// must name at least one logical core, every named comparator must be
/// completely pinned by a real content digest, and the configuration must carry
/// no blank or duplicate key.
fn validate_provenance(p: &Provenance) -> Result<(), ValidationError> {
    if !SUPPORTED_VERSIONS.contains(p.schema_version) {
        return Err(ValidationError::UnsupportedSchemaVersion {
            found: p.schema_version,
            expected: SUPPORTED_VERSIONS,
        });
    }
    for (field, value) in checked_provenance_fields(p) {
        if is_absent_value(value) {
            return Err(ValidationError::MissingProvenanceField { field });
        }
    }
    if p.hardware.logical_cores == 0 {
        return Err(ValidationError::MissingProvenanceField {
            field: "hardware.logical_cores",
        });
    }
    for (index, c) in p.comparators.iter().enumerate() {
        // A named comparator is a populated field, so its pins reject the
        // sentinel too, not just blanks: a comparator recorded as
        // `prometheus=unknown=unknown` is an unreproducible pin masquerading as
        // one, the same defect one layer out.
        for (field, value) in [
            ("name", c.name.as_str()),
            ("version", c.version.as_str()),
            ("image_digest", c.image_digest.as_str()),
        ] {
            if is_absent_value(value) {
                return Err(ValidationError::IncompleteComparator { index, field });
            }
        }
        // Presence is not enough: a mutable tag (`latest`) or a truncated or
        // upper-case hex string is a digest field that pins no bytes, defeating
        // the reason a digest is recorded.
        if !is_content_digest(&c.image_digest) {
            return Err(ValidationError::InvalidImageDigest {
                index,
                value: c.image_digest.clone(),
            });
        }
    }
    // Configuration entries: a blank key or value is unrecorded configuration
    // masquerading as recorded, and two entries for one key make the applied
    // value ambiguous. A duplicate key is rejected even when both values match:
    // the gatherer emitted the same setting twice, and one copy may later
    // diverge, so the shape is wrong now regardless of the current values.
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for (index, c) in p.config.iter().enumerate() {
        if c.key.trim().is_empty() {
            return Err(ValidationError::InvalidConfigEntry {
                index,
                reason: "a blank key".to_string(),
            });
        }
        if c.value.trim().is_empty() {
            return Err(ValidationError::InvalidConfigEntry {
                index,
                reason: format!("key `{}` has a blank value", c.key),
            });
        }
        if !seen.insert(c.key.as_str()) {
            return Err(ValidationError::InvalidConfigEntry {
                index,
                reason: format!("key `{}` appears more than once", c.key),
            });
        }
    }
    Ok(())
}

/// Validate one measurement's figures and status shape, fail-closed.
fn validate_measurement(m: &Measurement) -> Result<(), ValidationError> {
    for f in &m.figures {
        if f.name.trim().is_empty() {
            return Err(ValidationError::MalformedMeasurement {
                id: m.id.clone(),
                reason: "a figure has a blank name".to_string(),
            });
        }
        if !f.value.is_finite() {
            return Err(ValidationError::NonFiniteFigure {
                id: m.id.clone(),
                figure: f.name.clone(),
                value: f.value,
            });
        }
        if f.value < 0.0 {
            return Err(ValidationError::NegativeFigure {
                id: m.id.clone(),
                figure: f.name.clone(),
                value: f.value,
            });
        }
    }
    // A figure named twice is malformed: a consumer that looks it up by name
    // would silently read the first and ignore the second.
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for f in &m.figures {
        if !seen.insert(f.name.as_str()) {
            return Err(ValidationError::MalformedMeasurement {
                id: m.id.clone(),
                reason: format!("figure `{}` appears more than once", f.name),
            });
        }
    }
    if m.status.is_timed() {
        // A timed row carries the full timing figure set. An absent one fails
        // exactly like an out-of-band one.
        if m.figures.is_empty() {
            return Err(ValidationError::MalformedMeasurement {
                id: m.id.clone(),
                reason: "an ok (timed) measurement carries no figures".to_string(),
            });
        }
        for figure in REQUIRED_TIMED_FIGURES {
            if m.figure(figure).is_none() {
                return Err(ValidationError::MissingExpectedFigure {
                    id: m.id.clone(),
                    figure,
                });
            }
        }
        // Every required timing figure is present (just checked), finite, and
        // non-negative (checked above), so the three unwraps are total and the
        // comparison is meaningful. An impossible range (a min above its median,
        // or a median above its max) is a self-contradicting row. `min == median
        // == max` is legal: a single-run measurement collapses the three.
        let min_ms = m.figure("min_ms").unwrap_or_default();
        let median_ms = m.figure("median_ms").unwrap_or_default();
        let max_ms = m.figure("max_ms").unwrap_or_default();
        if !(min_ms <= median_ms && median_ms <= max_ms) {
            return Err(ValidationError::TimingOutOfOrder {
                id: m.id.clone(),
                min_ms,
                median_ms,
                max_ms,
            });
        }
    } else {
        // Only `ok` is timed (ADR-0927 decision 6). A non-`ok` row carrying a
        // timing figure would let an untimed outcome sneak a latency into a
        // performance table.
        for figure in REQUIRED_TIMED_FIGURES {
            if m.figure(figure).is_some() {
                return Err(ValidationError::MalformedMeasurement {
                    id: m.id.clone(),
                    reason: format!(
                        "status `{}` is not timed, but the row carries timing figure `{figure}` \
                         (only ok is timed, ADR-0927 decision 6)",
                        m.status.slug()
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Validate a whole report, fail-closed. This is the one entry point a consumer
/// (and the renderer) runs before trusting any figure: a missing, duplicate,
/// non-finite, negative, or malformed measurement all fail here, and an
/// absent-but-expected figure fails exactly like an out-of-band one.
pub fn validate(report: &MetricsBenchReport) -> Result<(), ValidationError> {
    validate_provenance(&report.provenance)?;
    if report.measurements.is_empty() {
        return Err(ValidationError::NoMeasurements);
    }
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for m in &report.measurements {
        if !seen.insert(m.id.as_str()) {
            return Err(ValidationError::DuplicateMeasurement { id: m.id.clone() });
        }
        validate_measurement(m)?;
    }
    if let Some(g) = report.geomean_ms {
        if !(g.is_finite() && g > 0.0) {
            return Err(ValidationError::BadGeomean { value: g });
        }
        // A geomean summarizes the timed medians; a report whose rows are all
        // timeout/error/refused has no timed median behind it, so the geomean
        // summarizes nothing.
        let timed_medians: Vec<f64> = report
            .measurements
            .iter()
            .filter(|m| m.status.is_timed())
            .filter_map(|m| m.figure("median_ms"))
            .collect();
        if timed_medians.is_empty() {
            return Err(ValidationError::GeomeanWithoutTimedRow { value: g });
        }
        // Requiring a timed row to exist without checking the value is "present
        // but unverified": recompute the geometric mean from the timed medians
        // and reject a supplied value that disagrees. `exp(mean(ln))` rather than
        // an nth root of a product, which would overflow over dozens of
        // millisecond medians. Every timed row carries `median_ms`
        // (`validate_measurement` above enforced it), so the count and the sum
        // cover the same rows.
        let sum_ln: f64 = timed_medians.iter().map(|m| m.ln()).sum();
        let computed = (sum_ln / timed_medians.len() as f64).exp();
        if (g - computed).abs() > GEOMEAN_REL_TOLERANCE * computed {
            return Err(ValidationError::GeomeanMismatch {
                supplied: g,
                computed,
            });
        }
    }
    Ok(())
}

// --- Renderer bands (ADR-0927 decision 5). Re-register a band by editing
// exactly one line in this block, exactly as the ClickBench runbook script
// does. Each band is `None` until a reference run pre-registers it, and a
// `None` band is a loud SKIP, never a silent pass. ------------------------------

/// The band on how many of the report's measurements must be timed (`ok`),
/// registered per reference run. `None` until pre-registered, so the renderer
/// prints a loud SKIP rather than asserting nothing. A tuple is `(lo, hi)`
/// inclusive.
pub const TIMED_FRACTION_BAND: Option<(f64, f64)> = None;

/// The band on the report-level geometric mean, in milliseconds, registered per
/// reference run and regime. `None` until pre-registered.
pub const GEOMEAN_MS_BAND: Option<(f64, f64)> = None;

/// Everything [`render`] can fail on: a report that does not validate, or a
/// pre-registered band a real run missed.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RenderError {
    /// The report did not validate. Integrity is asserted by identity before any
    /// band is applied, so a band verdict is never computed over a report whose
    /// figures cannot be trusted.
    #[error("report failed validation before any band could be applied: {0}")]
    Invalid(#[from] ValidationError),
    /// A pre-registered band was missed.
    #[error("{0}")]
    BandViolation(String),
}

/// Render the report as the human-readable table, derived entirely from the
/// artifact. The table is never hand-maintained: every row and every figure is
/// read off `report`.
///
/// The order is the runbook's proven one: validate (integrity by identity)
/// first, then the provenance header, then the retry caveat, then the per-query
/// rows, then the per-class distribution, then the pre-registered bands (each a
/// loud SKIP when not registered), and only then the optional geometric mean,
/// which cannot appear without the per-query rows above it.
pub fn render(report: &MetricsBenchReport) -> Result<String, RenderError> {
    validate(report)?;

    let mut out = String::new();
    let p = &report.provenance;
    out.push_str("metricsbench report (schema v");
    let _ = writeln!(out, "{})", p.schema_version);
    let _ = writeln!(out, "  commit    : {}", p.ravel_git_commit);
    let _ = writeln!(out, "  toolchain : {}", p.toolchain);
    let _ = writeln!(out, "  protocol  : {}", p.protocol);
    let _ = writeln!(
        out,
        "  backend   : {} (region={}, endpoint={}, bills_requests={})",
        p.backend.store_backend,
        p.backend.region,
        p.backend.endpoint,
        p.backend.backend_bills_requests
    );
    let _ = writeln!(
        out,
        "  hardware  : {} | {} | {} logical cores{}",
        p.hardware.os,
        p.hardware.cpu_model,
        p.hardware.logical_cores,
        match &p.hardware.instance_type {
            Some(t) => format!(" | {t}"),
            None => String::new(),
        }
    );
    let _ = writeln!(out, "  allocator : {}", p.allocator);
    if p.comparators.is_empty() {
        out.push_str("  comparators: none (Ravel-only diagnostic run)\n");
    } else {
        for c in &p.comparators {
            let _ = writeln!(
                out,
                "  comparator: {} {} @ {}",
                c.name, c.version, c.image_digest
            );
        }
    }
    let _ = writeln!(out, "  generator : {}", p.generator_digest);
    let _ = writeln!(out, "  corpus    : {}", p.corpus_digest);
    for c in &p.config {
        let _ = writeln!(out, "  config    : {} = {}", c.key, c.value);
    }
    // The retry caveat rides in the rendered output, not only in a doc.
    let _ = writeln!(out, "  NOTE: {RETRY_CAVEAT}");

    // Per-query rows: the source of truth, always printed before any summary.
    out.push('\n');
    let _ = writeln!(
        out,
        "  {:<32} | {:<22} | {:<12} | {:>10} | {:>10} | {:>10}",
        "id", "class", "status", "min_ms", "median_ms", "max_ms"
    );
    let _ = writeln!(
        out,
        "  {:-<32}-+-{:-<22}-+-{:-<12}-+-{:-<10}-+-{:-<10}-+-{:-<10}",
        "", "", "", "", "", ""
    );
    let fig = |m: &Measurement, name: &str| {
        m.figure(name)
            .map_or_else(|| "-".to_string(), |v| format!("{v:.3}"))
    };
    for m in &report.measurements {
        let _ = writeln!(
            out,
            "  {:<32} | {:<22} | {:<12} | {:>10} | {:>10} | {:>10}",
            m.id,
            m.class.slug(),
            m.status.slug(),
            fig(m, "min_ms"),
            fig(m, "median_ms"),
            fig(m, "max_ms"),
        );
    }

    // Per-class distribution, derived from the rows. Every class is listed,
    // including the empty ones, so a class with no measurement is visible rather
    // than dropped.
    out.push('\n');
    out.push_str("  measurements by cost class (timed = status ok):\n");
    for class in CostClass::ALL {
        let in_class: Vec<&Measurement> = report
            .measurements
            .iter()
            .filter(|m| m.class == *class)
            .collect();
        let timed = in_class.iter().filter(|m| m.status.is_timed()).count();
        let _ = writeln!(
            out,
            "    {:<22} {} measured, {} timed",
            class.slug(),
            in_class.len(),
            timed
        );
    }

    // Pre-registered bands. Integrity above already stood; a band is applied
    // only now, and a band that is not pre-registered is a loud SKIP, never a
    // silent pass.
    out.push('\n');
    let total = report.measurements.len();
    let timed = report
        .measurements
        .iter()
        .filter(|m| m.status.is_timed())
        .count();
    let timed_fraction = timed as f64 / total as f64;
    match TIMED_FRACTION_BAND {
        None => {
            let _ = writeln!(
                out,
                "  SKIP timed-fraction band: not pre-registered for this run (measured {timed}/{total} = {timed_fraction:.3})"
            );
        }
        Some((lo, hi)) => {
            let _ = writeln!(
                out,
                "  timed fraction {timed_fraction:.3} (band {lo:.3}..={hi:.3})"
            );
            if !(lo..=hi).contains(&timed_fraction) {
                return Err(RenderError::BandViolation(format!(
                    "timed fraction {timed_fraction:.3} outside band {lo:.3}..={hi:.3}"
                )));
            }
        }
    }

    // The geometric mean is a summary and never a replacement: it is printed
    // last, after the per-query rows it derives from, and only when the producer
    // computed one. A report without those rows never reaches here (validation
    // refuses an empty measurement list above).
    match report.geomean_ms {
        None => {
            out.push_str(
                "  geomean: not computed (per-query rows above are the source of truth)\n",
            );
        }
        Some(g) => {
            let _ = writeln!(
                out,
                "  geomean {g:.3} ms over {timed} timed of {total} measured rows"
            );
            match GEOMEAN_MS_BAND {
                None => {
                    out.push_str("  SKIP geomean band: not pre-registered for this run\n");
                }
                Some((lo, hi)) => {
                    if !(lo..=hi).contains(&g) {
                        return Err(RenderError::BandViolation(format!(
                            "geomean {g:.3} ms outside band {lo:.3}..={hi:.3}"
                        )));
                    }
                }
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A provenance block that validates clean, for a test to mutate one field
    /// of.
    fn valid_provenance() -> Provenance {
        Provenance {
            schema_version: SCHEMA_VERSION,
            ravel_git_commit: "9fc85f421590d360e7979ee167eb38e166b45462".to_string(),
            toolchain: "rustc 1.90.0".to_string(),
            protocol: "remote_write_1.0".to_string(),
            hardware: Hardware {
                os: "Linux 6.8.0 x86_64".to_string(),
                cpu_model: "AMD EPYC 7R13".to_string(),
                logical_cores: 8,
                instance_type: Some("c6a.2xlarge".to_string()),
            },
            backend: Backend {
                store_backend: "s3".to_string(),
                region: "us-east-1".to_string(),
                endpoint: "n/a".to_string(),
                backend_bills_requests: true,
            },
            comparators: vec![Comparator {
                name: "prometheus".to_string(),
                version: "3.13.1".to_string(),
                image_digest:
                    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        .to_string(),
            }],
            generator_digest: "blake3:1111".to_string(),
            corpus_digest: "blake3:2222".to_string(),
            config: vec![ConfigEntry {
                key: "max_flush_delay_ms".to_string(),
                value: "2000".to_string(),
            }],
            allocator: Allocator::Tcmalloc,
        }
    }

    /// One timed (`ok`) measurement carrying the full required timing figure
    /// set.
    fn timed_measurement(id: &str) -> Measurement {
        Measurement {
            id: id.to_string(),
            class: CostClass::HighFanOut,
            status: ResultStatus::Ok,
            figures: vec![
                Figure {
                    name: "min_ms".to_string(),
                    value: 10.0,
                },
                Figure {
                    name: "median_ms".to_string(),
                    value: 12.5,
                },
                Figure {
                    name: "max_ms".to_string(),
                    value: 20.0,
                },
            ],
        }
    }

    fn valid_report() -> MetricsBenchReport {
        MetricsBenchReport {
            provenance: valid_provenance(),
            measurements: vec![timed_measurement("mb_fanout_total_rate")],
            geomean_ms: Some(12.5),
        }
    }

    /// The whole point of the fixtures: a report built from them validates
    /// clean, so every failure test below isolates exactly one defect.
    #[test]
    fn the_fixture_report_validates_clean() {
        validate(&valid_report()).expect("the fixture report validates");
    }

    /// ACCEPTANCE TEST (issue #936). A report whose provenance is missing a
    /// required field fails validation, naming the field.
    ///
    /// To watch this test fail against a report that HAS the field, keep the
    /// commit populated instead of blanking it: change the
    /// `report.provenance.ravel_git_commit = String::new();` line below to
    /// `report.provenance.ravel_git_commit = "deadbeef".to_string();`. The
    /// report then validates, `expect_err` finds `Ok(())`, and the test fails.
    #[test]
    fn a_report_missing_a_required_provenance_field_fails_validation() {
        let mut report = valid_report();
        report.provenance.ravel_git_commit = String::new();
        let err = validate(&report).expect_err("a blank git commit must fail validation");
        assert_eq!(
            err,
            ValidationError::MissingProvenanceField {
                field: "ravel_git_commit"
            },
            "the error must name the missing field, got {err:?}"
        );
    }

    /// The validator rejects the `"unknown"` sentinel a gatherer on a host
    /// without git used to emit, independently of the gatherer. This test pins
    /// the validator even after the gatherer can no longer produce the sentinel:
    /// a future gatherer regression that reintroduces it cannot slip a report
    /// carrying `ravel_git_commit: "unknown"` past `validate`.
    #[test]
    fn a_report_with_an_unknown_sentinel_git_commit_fails_validation() {
        let mut report = valid_report();
        report.provenance.ravel_git_commit = "unknown".to_string();
        let err = validate(&report).expect_err("an `unknown` sentinel git commit must fail");
        assert_eq!(
            err,
            ValidationError::MissingProvenanceField {
                field: "ravel_git_commit"
            },
            "the sentinel is treated as a missing field, got {err:?}"
        );
    }

    /// `instance_type` is optional: a report with it absent (not a sentinel)
    /// still validates. An explicit absence is not a defect; a sentinel string
    /// standing in for one would be.
    #[test]
    fn a_report_with_instance_type_absent_validates() {
        let mut report = valid_report();
        report.provenance.hardware.instance_type = None;
        validate(&report).expect("instance_type is optional; absent still validates");
    }

    /// A geomean with at least one timed row still validates: a summary that has
    /// per-query rows behind it is admissible.
    #[test]
    fn a_geomean_with_a_timed_row_validates() {
        let report = valid_report();
        assert!(
            report.measurements.iter().any(|m| m.status.is_timed()),
            "the fixture has a timed row"
        );
        assert!(report.geomean_ms.is_some(), "the fixture has a geomean");
        validate(&report).expect("a geomean with a timed row validates");
    }

    /// A geomean with zero timed rows fails with the distinct typed variant: a
    /// summary figure with no per-query row behind it is not a result (ADR-0927).
    /// The error says the geomean had nothing behind it, not that its value was
    /// malformed.
    ///
    /// This test fails against the pre-fix validator, whose geomean check was
    ///     if let Some(g) = report.geomean_ms && !(g.is_finite() && g > 0.0) {
    ///         return Err(ValidationError::BadGeomean { value: g });
    ///     }
    /// (report_schema.rs, the `geomean_ms` block near the end of `validate`): a
    /// finite positive geomean over all-untimed rows returned `Ok(())`, so
    /// `expect_err` found `Ok` and the test failed. The fix adds the
    /// `GeomeanWithoutTimedRow` arm beside `BadGeomean`.
    #[test]
    fn a_geomean_with_no_timed_row_fails_validation() {
        let mut report = valid_report();
        // The only measurement becomes untimed; a non-ok row must carry no
        // timing figures, so strip them. The geomean stays present.
        report.measurements[0].status = ResultStatus::Timeout;
        report.measurements[0].figures.clear();
        assert!(
            !report.measurements.iter().any(|m| m.status.is_timed()),
            "no row is timed after this mutation"
        );
        assert_eq!(report.geomean_ms, Some(12.5));
        let err =
            validate(&report).expect_err("a geomean over zero timed rows must fail validation");
        assert_eq!(err, ValidationError::GeomeanWithoutTimedRow { value: 12.5 });
    }

    /// One timed (`ok`) row whose `median_ms` is `median`; `min`/`max` are set to
    /// the same value so the row satisfies the `min <= median <= max` ordering
    /// rule (a single-run collapse of the three) while feeding `median` to the
    /// geomean recomputation.
    fn timed_with_median(id: &str, median: f64) -> Measurement {
        Measurement {
            id: id.to_string(),
            class: CostClass::HighFanOut,
            status: ResultStatus::Ok,
            figures: vec![
                Figure {
                    name: "min_ms".to_string(),
                    value: median,
                },
                Figure {
                    name: "median_ms".to_string(),
                    value: median,
                },
                Figure {
                    name: "max_ms".to_string(),
                    value: median,
                },
            ],
        }
    }

    /// The comparator sentinel defect one layer out: a pin recorded as
    /// `prometheus=unknown=unknown` used to validate, because the comparator
    /// fields rejected only blanks. It now fails, naming the first sentinel pin.
    #[test]
    fn a_comparator_pinned_with_the_unknown_sentinel_fails_validation() {
        let mut report = valid_report();
        report.provenance.comparators[0] = Comparator {
            name: "prometheus".to_string(),
            version: "unknown".to_string(),
            image_digest: "unknown".to_string(),
        };
        let err = validate(&report).expect_err("an `unknown` comparator pin must fail");
        assert_eq!(
            err,
            ValidationError::IncompleteComparator {
                index: 0,
                field: "version",
            }
        );
    }

    /// A populated backend field carrying the sentinel fails: `region = "unknown"`
    /// is a sentinel that reads like data, refused the same as a blank.
    #[test]
    fn a_populated_backend_field_with_the_unknown_sentinel_fails_validation() {
        let mut report = valid_report();
        report.provenance.backend.region = "unknown".to_string();
        let err = validate(&report).expect_err("an `unknown` region must fail");
        assert_eq!(
            err,
            ValidationError::MissingProvenanceField {
                field: "backend.region",
            }
        );
    }

    /// A populated optional hardware field carrying the sentinel fails: an absent
    /// `instance_type` is legitimate, but a present one that is `"unknown"` is a
    /// sentinel standing in for a real value.
    #[test]
    fn a_populated_instance_type_with_the_unknown_sentinel_fails_validation() {
        let mut report = valid_report();
        report.provenance.hardware.instance_type = Some("unknown".to_string());
        let err = validate(&report).expect_err("an `unknown` instance_type must fail");
        assert_eq!(
            err,
            ValidationError::MissingProvenanceField {
                field: "hardware.instance_type",
            }
        );
    }

    /// The regression the sentinel fix could easily introduce: `region` and
    /// `endpoint` carrying the legitimate `"n/a"` value (a backend with no
    /// region/endpoint) still VALIDATE. `"n/a"` is real data, not a sentinel.
    #[test]
    fn backend_region_and_endpoint_carrying_n_a_still_validate() {
        let mut report = valid_report();
        report.provenance.backend.region = "n/a".to_string();
        report.provenance.backend.endpoint = "n/a".to_string();
        validate(&report).expect("`n/a` on region/endpoint is a real value and validates");
    }

    /// FINDING 2. A geomean that disagrees with the timed medians fails with the
    /// distinct `GeomeanMismatch` variant, and the error carries both the
    /// supplied value and the value recomputed from the rows. One timed row of
    /// `median_ms = 12.5` and a supplied `geomean_ms = 1.0` are two contradictory
    /// results.
    ///
    /// To watch this test FAIL against the pre-fix validator, delete the
    /// recomputation block in `validate` (the `let sum_ln: f64 = ...` line
    /// through the `GeomeanMismatch` return, in the `if let Some(g) =
    /// report.geomean_ms` block). The pre-fix validator required only that a
    /// timed row EXIST, so `validate` returned `Ok(())` for a disagreeing
    /// geomean, `expect_err` found `Ok`, and the test failed.
    #[test]
    fn a_geomean_disagreeing_with_the_timed_medians_fails_validation() {
        let mut report = valid_report();
        // The fixture's single timed row has median_ms = 12.5.
        assert_eq!(report.measurements[0].figure("median_ms"), Some(12.5));
        report.geomean_ms = Some(1.0);
        let err = validate(&report).expect_err("a geomean disagreeing with the medians must fail");
        match err {
            ValidationError::GeomeanMismatch { supplied, computed } => {
                assert_eq!(
                    supplied, 1.0,
                    "the error carries the supplied value verbatim"
                );
                // The geomean of the single median 12.5 is that median, to within
                // the `exp(ln)` round-trip error the tolerance itself allows.
                assert!(
                    (computed - 12.5).abs() <= GEOMEAN_REL_TOLERANCE * 12.5,
                    "the error carries the recomputed geomean of the medians, got {computed}"
                );
            }
            other => panic!("wrong error variant: {other:?}"),
        }
    }

    /// A geomean computed correctly from the timed medians validates, over enough
    /// rows that the `exp(mean(ln))` accumulation is non-trivial, so the
    /// tolerance is exercised rather than assumed. The same computation is fed as
    /// the supplied value; the recomputation is bit-identical, so it passes well
    /// inside [`GEOMEAN_REL_TOLERANCE`].
    #[test]
    fn a_geomean_matching_the_timed_medians_validates_over_many_rows() {
        let medians: Vec<f64> = (1..=40).map(|i| 5.0 + f64::from(i) * 0.37).collect();
        let n = medians.len() as f64;
        let geomean = (medians.iter().map(|m| m.ln()).sum::<f64>() / n).exp();

        let mut report = valid_report();
        report.provenance.comparators.clear();
        report.measurements = medians
            .iter()
            .enumerate()
            .map(|(i, &m)| timed_with_median(&format!("mb_row_{i}"), m))
            .collect();
        report.geomean_ms = Some(geomean);
        validate(&report).expect("a faithfully computed geomean validates");

        // A value just outside the tolerance is rejected, proving the tolerance
        // is a real check rather than an always-true one.
        report.geomean_ms = Some(geomean * (1.0 + 2.0 * GEOMEAN_REL_TOLERANCE));
        let err = validate(&report).expect_err("a geomean outside the tolerance must fail");
        match err {
            ValidationError::GeomeanMismatch { supplied, computed } => {
                assert_eq!(supplied, geomean * (1.0 + 2.0 * GEOMEAN_REL_TOLERANCE));
                assert_eq!(computed, geomean);
            }
            other => panic!("wrong error variant: {other:?}"),
        }
    }

    /// A report with no geomean at all still validates: the geomean is optional
    /// and the per-query rows stand on their own.
    #[test]
    fn a_report_with_no_geomean_validates() {
        let mut report = valid_report();
        report.geomean_ms = None;
        validate(&report).expect("a report without a geomean validates");
    }

    /// Failure class: a schema version this build does not read.
    #[test]
    fn a_wrong_schema_version_fails_validation() {
        let mut report = valid_report();
        report.provenance.schema_version = SCHEMA_VERSION + 7;
        let err = validate(&report).expect_err("a wrong schema version must fail");
        assert_eq!(
            err,
            ValidationError::UnsupportedSchemaVersion {
                found: SCHEMA_VERSION + 7,
                expected: SUPPORTED_VERSIONS,
            }
        );
    }

    /// The supported-version window is the shape the rest of this module assumes:
    /// the writer stamps the newest version, the previous one is still read, and
    /// nothing below the floor or above the newest is accepted.
    #[test]
    fn the_supported_version_window_accepts_exactly_1_and_2() {
        assert_eq!(SCHEMA_VERSION, 2, "the writer stamps version 2");
        assert_eq!(SUPPORTED_VERSIONS.newest(), SCHEMA_VERSION);
        assert_eq!(SUPPORTED_VERSIONS.oldest(), 1);
        assert!(SUPPORTED_VERSIONS.contains(1));
        assert!(SUPPORTED_VERSIONS.contains(2));
        assert!(!SUPPORTED_VERSIONS.contains(0));
        assert!(!SUPPORTED_VERSIONS.contains(3));
        // The one-version shape this returns to once the version-1 reader is
        // retired accepts only its own version.
        let single = SupportedVersions::single(SCHEMA_VERSION);
        assert!(single.contains(SCHEMA_VERSION));
        assert!(!single.contains(SCHEMA_VERSION - 1));
        assert_eq!(single.to_string(), "{2}");
    }

    /// A version-1 document carries NO `allocator` key (the field did not exist
    /// at version 1) and must keep validating, reading as the explicit unknown.
    /// This is the whole reason the exact-equality check became a window: a
    /// version bump that refused every already-emitted report would break the
    /// artifacts it was supposed to disambiguate.
    ///
    /// To watch it fail: change `SUPPORTED_VERSIONS` to
    /// `SupportedVersions::single(SCHEMA_VERSION)`. Version 1 leaves the window,
    /// `validate` returns `UnsupportedSchemaVersion`, and `expect` panics.
    #[test]
    fn a_version_1_report_without_an_allocator_key_still_validates_as_unknown() {
        let mut doc = serde_json::to_value(valid_report()).expect("serialize");
        let provenance = doc["provenance"]
            .as_object_mut()
            .expect("provenance object");
        provenance.insert("schema_version".to_string(), serde_json::json!(1));
        provenance
            .remove("allocator")
            .expect("the version-2 fixture carried an allocator key to remove");
        let report: MetricsBenchReport =
            serde_json::from_value(doc).expect("a version-1 document deserializes");
        assert_eq!(report.provenance.schema_version, 1);
        assert_eq!(
            report.provenance.allocator,
            Allocator::Unknown,
            "an absent allocator is the explicit unknown, never a guess"
        );
        validate(&report).expect("a version-1 report still validates");
    }

    /// A version-2 document carrying an allocator validates and round-trips the
    /// exact value: the bump is what makes the allocator's presence readable from
    /// the version number alone.
    ///
    /// To watch it fail against the pre-change validator: change the version
    /// check in `validate_provenance` from
    /// `if !SUPPORTED_VERSIONS.contains(p.schema_version)` to
    /// `if p.schema_version != 1`, which is the exact-equality check against the
    /// old `SCHEMA_VERSION`. A version-2 document is then refused and the first
    /// `expect` panics.
    #[test]
    fn a_version_2_report_with_an_allocator_validates_and_round_trips() {
        let mut report = valid_report();
        report.provenance.schema_version = 2;
        report.provenance.allocator = Allocator::Jemalloc;
        validate(&report).expect("a version-2 report validates");

        let doc = serde_json::to_value(&report).expect("serialize");
        assert_eq!(doc["provenance"]["schema_version"], 2);
        assert_eq!(doc["provenance"]["allocator"], "jemalloc");
        let back: MetricsBenchReport = serde_json::from_value(doc).expect("deserialize");
        assert_eq!(back.provenance.schema_version, 2);
        assert_eq!(back.provenance.allocator, Allocator::Jemalloc);
        assert_eq!(back, report, "the version-2 document round-trips exactly");
        validate(&back).expect("the round-tripped version-2 report validates");
    }

    /// A version outside the window is still refused, above and below, and the
    /// message names the accepted SET rather than one number: a reader told
    /// "reads version 2" would go looking for the wrong mismatch when 1 is also
    /// accepted.
    ///
    /// To watch it fail: widen the window wrongly, with
    /// `SUPPORTED_VERSIONS = SupportedVersions { newest: 99, oldest: 0 }`. Every
    /// version this test tries is then inside the window, `validate` returns
    /// `Ok(())`, and `expect_err` panics.
    #[test]
    fn a_report_with_an_unsupported_schema_version_is_refused_naming_the_set() {
        for unsupported in [0, 3, 99] {
            let mut report = valid_report();
            report.provenance.schema_version = unsupported;
            let err = validate(&report).expect_err("an unsupported version must be refused");
            assert_eq!(
                err,
                ValidationError::UnsupportedSchemaVersion {
                    found: unsupported,
                    expected: SUPPORTED_VERSIONS,
                },
                "version {unsupported} must be refused by the version check"
            );
            let message = err.to_string();
            assert!(
                message.contains("versions {1, 2}"),
                "the message must name the supported set, got: {message}"
            );
        }
    }

    /// Failure class: duplicate measurement id.
    #[test]
    fn a_duplicate_measurement_id_fails_validation() {
        let mut report = valid_report();
        report
            .measurements
            .push(timed_measurement("mb_fanout_total_rate"));
        let err = validate(&report).expect_err("a duplicate id must fail");
        assert_eq!(
            err,
            ValidationError::DuplicateMeasurement {
                id: "mb_fanout_total_rate".to_string()
            }
        );
    }

    /// Failure class: a non-finite figure value.
    #[test]
    fn a_non_finite_figure_fails_validation() {
        let mut report = valid_report();
        report.measurements[0].figures[1].value = f64::NAN;
        let err = validate(&report).expect_err("a NaN figure must fail");
        match err {
            ValidationError::NonFiniteFigure { id, figure, value } => {
                assert_eq!(id, "mb_fanout_total_rate");
                assert_eq!(figure, "median_ms");
                assert!(value.is_nan(), "the reported value is the NaN itself");
            }
            other => panic!("wrong error variant: {other:?}"),
        }
    }

    /// Failure class: a negative figure value.
    #[test]
    fn a_negative_figure_fails_validation() {
        let mut report = valid_report();
        report.measurements[0].figures[0].value = -1.5;
        let err = validate(&report).expect_err("a negative figure must fail");
        assert_eq!(
            err,
            ValidationError::NegativeFigure {
                id: "mb_fanout_total_rate".to_string(),
                figure: "min_ms".to_string(),
                value: -1.5,
            }
        );
    }

    /// Failure class: a malformed measurement (a blank figure name).
    #[test]
    fn a_blank_figure_name_is_a_malformed_measurement() {
        let mut report = valid_report();
        report.measurements[0].figures[0].name = "   ".to_string();
        let err = validate(&report).expect_err("a blank figure name must fail");
        assert_eq!(
            err,
            ValidationError::MalformedMeasurement {
                id: "mb_fanout_total_rate".to_string(),
                reason: "a figure has a blank name".to_string(),
            }
        );
    }

    /// An absent-but-expected timing figure fails exactly like an out-of-band
    /// one (ADR-0927 decision 5): an `ok` row that drops `median_ms` is refused,
    /// naming the missing figure, not accepted with a hole.
    #[test]
    fn an_ok_measurement_missing_a_timing_figure_fails_validation() {
        let mut report = valid_report();
        report.measurements[0]
            .figures
            .retain(|f| f.name != "median_ms");
        let err = validate(&report).expect_err("a missing required figure must fail");
        assert_eq!(
            err,
            ValidationError::MissingExpectedFigure {
                id: "mb_fanout_total_rate".to_string(),
                figure: "median_ms",
            }
        );
    }

    /// Only `ok` is timed: a non-`ok` row carrying a timing figure is malformed,
    /// so an untimed outcome cannot smuggle a latency into a performance table.
    #[test]
    fn a_non_ok_measurement_carrying_a_timing_figure_is_malformed() {
        let mut report = valid_report();
        report.measurements[0].status = ResultStatus::Refused;
        // Leaves the timing figures on a refused row.
        let err = validate(&report).expect_err("a timed figure on a refused row must fail");
        match err {
            ValidationError::MalformedMeasurement { id, reason } => {
                assert_eq!(id, "mb_fanout_total_rate");
                assert!(reason.contains("only ok is timed"), "{reason}");
            }
            other => panic!("wrong error variant: {other:?}"),
        }
    }

    /// A named comparator missing its image digest fails: a comparator that
    /// cannot be reproduced is worse than none (ADR-0927 decision 4).
    #[test]
    fn an_incompletely_pinned_comparator_fails_validation() {
        let mut report = valid_report();
        report.provenance.comparators[0].image_digest = String::new();
        let err = validate(&report).expect_err("an unpinned comparator must fail");
        assert_eq!(
            err,
            ValidationError::IncompleteComparator {
                index: 0,
                field: "image_digest",
            }
        );
    }

    /// A report with no measurements is refused: per-query results are the
    /// source of truth, and a report with none measures nothing.
    #[test]
    fn a_report_with_no_measurements_fails_validation() {
        let mut report = valid_report();
        report.measurements.clear();
        report.geomean_ms = None;
        let err = validate(&report).expect_err("an empty report must fail");
        assert_eq!(err, ValidationError::NoMeasurements);
    }

    /// The renderer derives its table from the artifact and carries the retry
    /// caveat in its output, not only in a doc (ADR-0927 decision 8).
    #[test]
    fn the_renderer_derives_the_table_and_carries_the_retry_caveat() {
        let report = valid_report();
        let text = render(&report).expect("the fixture report renders");
        // Every per-query row is derived from the artifact.
        assert!(text.contains("mb_fanout_total_rate"), "{text}");
        assert!(text.contains("high_fan_out"), "{text}");
        assert!(
            text.contains("12.500"),
            "the median figure is rendered: {text}"
        );
        // The retry caveat is in the output.
        assert!(
            text.contains(RETRY_CAVEAT),
            "the retry caveat must be rendered: {text}"
        );
        assert!(text.contains("logical-call counts"), "{text}");
        // A gap with no pre-registered band is a loud SKIP, never a silent pass.
        assert!(
            text.contains("SKIP timed-fraction band: not pre-registered"),
            "an unregistered band must render a loud SKIP: {text}"
        );
    }

    /// The renderer cannot print a summary without the per-query rows behind it:
    /// a report that fails validation (here, an empty measurement list) is
    /// refused before any geomean line is produced.
    #[test]
    fn the_renderer_refuses_to_summarize_a_report_that_does_not_validate() {
        let mut report = valid_report();
        report.measurements.clear();
        let err = render(&report).expect_err("an invalid report must not render");
        assert!(
            matches!(err, RenderError::Invalid(ValidationError::NoMeasurements)),
            "wrong error: {err:?}"
        );
    }

    /// The report round-trips through JSON unchanged: this is the emission
    /// contract a producer serializes and a consumer (the renderer binary)
    /// reads back.
    #[test]
    fn a_report_round_trips_through_json() {
        let report = valid_report();
        let json = serde_json::to_string_pretty(&report).expect("serialize");
        let back: MetricsBenchReport = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(report, back);
        validate(&back).expect("the round-tripped report validates");
    }

    /// The allocator is serialized into provenance with its exact value, and a
    /// report written before the field existed deserializes to the explicit
    /// unknown rather than a guessed allocator (issue #972).
    ///
    /// To watch the serialization half fail: change `valid_report`'s provenance
    /// `allocator` to `Allocator::Jemalloc`; the exact-value assertion then reads
    /// `jemalloc == tcmalloc`. To watch the back-compat half fail: change
    /// `default_allocator` to return `Allocator::System`; the field-stripped report then
    /// deserializes to `system` and the last assertion reads `system == unknown`.
    #[test]
    fn provenance_records_the_allocator_and_defaults_it_to_unknown() {
        let report = valid_report();
        let value = serde_json::to_value(&report).expect("serialize");
        assert_eq!(
            value["provenance"]["allocator"], "tcmalloc",
            "the allocator serializes with its exact value: {value}"
        );

        // A report written before the field existed omits it, and deserializes
        // to the explicit unknown, never a guessed allocator.
        let mut doc = value;
        doc["provenance"]
            .as_object_mut()
            .expect("provenance object")
            .remove("allocator");
        let back: MetricsBenchReport =
            serde_json::from_value(doc).expect("deserialize a report with no allocator field");
        assert_eq!(back.provenance.allocator, Allocator::Unknown);
    }

    /// Every allocator the probe can produce round-trips through provenance by
    /// exact value, and an unrecognized allocator string is rejected at
    /// deserialize rather than laundered into `unknown` (issue #972): a garbage
    /// value in this slot would read as the honest "the probe could not answer".
    ///
    /// To watch the round-trip half fail: change any arm of `Allocator::as_str`
    /// (or the `rename_all`) so a variant serializes to a different string; the
    /// exact-value assertion for that variant then mismatches. To watch the
    /// reject half fail: add `#[serde(other)] Unknown` semantics (a catch-all)
    /// to `Allocator`; the unrecognized string then deserializes to `unknown`
    /// and `expect_err` panics.
    #[test]
    fn allocator_round_trips_by_value_and_rejects_an_unrecognized_string() {
        for (variant, text) in [
            (Allocator::Tcmalloc, "tcmalloc"),
            (Allocator::Jemalloc, "jemalloc"),
            (Allocator::Mimalloc, "mimalloc"),
            (Allocator::System, "system"),
            (Allocator::Unknown, "unknown"),
        ] {
            let mut report = valid_report();
            report.provenance.allocator = variant;
            let value = serde_json::to_value(&report).expect("serialize");
            assert_eq!(
                value["provenance"]["allocator"], text,
                "{variant} serializes to its exact value"
            );
            let back: MetricsBenchReport =
                serde_json::from_value(value).expect("deserialize a valid allocator");
            assert_eq!(back.provenance.allocator, variant);
        }

        let mut doc = serde_json::to_value(valid_report()).expect("serialize");
        doc["provenance"]["allocator"] = serde_json::json!("bogus-allocator");
        serde_json::from_value::<MetricsBenchReport>(doc)
            .expect_err("an unrecognized allocator string is rejected");
    }

    // --- FINDING 1: configuration entries are validated, not merely present. ---

    /// A multi-entry configuration map with distinct non-blank keys and values
    /// validates: the rule rejects blanks and duplicates, not a legitimate map.
    #[test]
    fn a_valid_config_map_validates() {
        let mut report = valid_report();
        report.provenance.config = vec![
            ConfigEntry {
                key: "max_flush_delay_ms".to_string(),
                value: "2000".to_string(),
            },
            ConfigEntry {
                key: "shard_count".to_string(),
                value: "8".to_string(),
            },
        ];
        validate(&report).expect("a distinct-key, non-blank config map validates");
    }

    /// A blank configuration key is refused: an unnamed setting is unrecorded
    /// configuration masquerading as recorded.
    #[test]
    fn a_blank_config_key_fails_validation() {
        let mut report = valid_report();
        report.provenance.config = vec![ConfigEntry {
            key: "   ".to_string(),
            value: "2000".to_string(),
        }];
        let err = validate(&report).expect_err("a blank config key must fail");
        assert_eq!(
            err,
            ValidationError::InvalidConfigEntry {
                index: 0,
                reason: "a blank key".to_string(),
            }
        );
    }

    /// A blank configuration value is refused: a named setting with no value
    /// records nothing about how the run was configured.
    #[test]
    fn a_blank_config_value_fails_validation() {
        let mut report = valid_report();
        report.provenance.config = vec![ConfigEntry {
            key: "max_flush_delay_ms".to_string(),
            value: "  ".to_string(),
        }];
        let err = validate(&report).expect_err("a blank config value must fail");
        assert_eq!(
            err,
            ValidationError::InvalidConfigEntry {
                index: 0,
                reason: "key `max_flush_delay_ms` has a blank value".to_string(),
            }
        );
    }

    /// Two entries for one key with DIFFERENT values are refused: the applied
    /// value is ambiguous, so the block reads as complete while recording an
    /// unreproducible setting.
    #[test]
    fn duplicate_config_keys_with_different_values_fail_validation() {
        let mut report = valid_report();
        report.provenance.config = vec![
            ConfigEntry {
                key: "max_flush_delay_ms".to_string(),
                value: "2000".to_string(),
            },
            ConfigEntry {
                key: "max_flush_delay_ms".to_string(),
                value: "3000".to_string(),
            },
        ];
        let err = validate(&report).expect_err("duplicate config keys must fail");
        assert_eq!(
            err,
            ValidationError::InvalidConfigEntry {
                index: 1,
                reason: "key `max_flush_delay_ms` appears more than once".to_string(),
            }
        );
    }

    /// Two entries for one key with the SAME value are also refused: the gatherer
    /// emitted the setting twice and one copy may later diverge, so the shape is
    /// wrong now regardless of the current values.
    #[test]
    fn duplicate_config_keys_with_the_same_value_fail_validation() {
        let mut report = valid_report();
        report.provenance.config = vec![
            ConfigEntry {
                key: "shard_count".to_string(),
                value: "8".to_string(),
            },
            ConfigEntry {
                key: "shard_count".to_string(),
                value: "8".to_string(),
            },
        ];
        let err = validate(&report).expect_err("same-value duplicate config keys must fail");
        assert_eq!(
            err,
            ValidationError::InvalidConfigEntry {
                index: 1,
                reason: "key `shard_count` appears more than once".to_string(),
            }
        );
    }

    // --- FINDING 2: timed rows require min_ms <= median_ms <= max_ms. ---

    /// A timed row with `min_ms` above its median is refused, and the error
    /// carries all three values so the impossible triple is visible.
    ///
    /// To watch this test FAIL against the pre-fix validator, delete the ordering
    /// block in `validate_measurement` (the `let min_ms = ...` line through the
    /// `TimingOutOfOrder` return, inside the `if m.status.is_timed()` arm). The
    /// pre-fix code did not check order, so `validate` returned `Ok(())`,
    /// `expect_err` found `Ok`, and the test failed. This was confirmed by
    /// commenting that block out and running this test before committing.
    #[test]
    fn a_timed_row_with_min_above_median_fails_validation() {
        let mut report = valid_report();
        // min_ms = 20, median_ms = 12.5, max_ms = 10: an impossible range.
        report.measurements[0].figures = vec![
            Figure {
                name: "min_ms".to_string(),
                value: 20.0,
            },
            Figure {
                name: "median_ms".to_string(),
                value: 12.5,
            },
            Figure {
                name: "max_ms".to_string(),
                value: 10.0,
            },
        ];
        // The geomean would summarize the median 12.5; keep it consistent so the
        // ordering error is what fails, not a geomean mismatch.
        report.geomean_ms = Some(12.5);
        let err = validate(&report).expect_err("an out-of-order timed row must fail");
        assert_eq!(
            err,
            ValidationError::TimingOutOfOrder {
                id: "mb_fanout_total_rate".to_string(),
                min_ms: 20.0,
                median_ms: 12.5,
                max_ms: 10.0,
            }
        );
    }

    /// A timed row whose median exceeds its max is refused, error carrying the
    /// triple. This exercises the second half of the `min <= median <= max`
    /// conjunction independently of the first.
    #[test]
    fn a_timed_row_with_median_above_max_fails_validation() {
        let mut report = valid_report();
        report.measurements[0].figures = vec![
            Figure {
                name: "min_ms".to_string(),
                value: 5.0,
            },
            Figure {
                name: "median_ms".to_string(),
                value: 30.0,
            },
            Figure {
                name: "max_ms".to_string(),
                value: 20.0,
            },
        ];
        report.geomean_ms = Some(30.0);
        let err = validate(&report).expect_err("a median above max must fail");
        assert_eq!(
            err,
            ValidationError::TimingOutOfOrder {
                id: "mb_fanout_total_rate".to_string(),
                min_ms: 5.0,
                median_ms: 30.0,
                max_ms: 20.0,
            }
        );
    }

    /// A single-run measurement (`min == median == max`) is legal and must not be
    /// rejected by an over-strict comparison: the three collapse to one value.
    #[test]
    fn a_timed_row_with_equal_timings_validates() {
        let mut report = valid_report();
        report.measurements[0].figures = vec![
            Figure {
                name: "min_ms".to_string(),
                value: 7.0,
            },
            Figure {
                name: "median_ms".to_string(),
                value: 7.0,
            },
            Figure {
                name: "max_ms".to_string(),
                value: 7.0,
            },
        ];
        report.geomean_ms = Some(7.0);
        validate(&report).expect("min == median == max is a legal single-run measurement");
    }

    /// The ordering rule applies only to timed statuses: a non-timed row carries
    /// no timing figures, so there is nothing to order and it still validates.
    #[test]
    fn a_non_timed_row_with_absent_timings_validates() {
        let mut report = valid_report();
        // A timed row exists so the geomean still has a row behind it.
        report.measurements = vec![
            timed_measurement("mb_fanout_total_rate"),
            Measurement {
                id: "mb_refused_row".to_string(),
                class: CostClass::HighFanOut,
                status: ResultStatus::Refused,
                figures: vec![],
            },
        ];
        validate(&report).expect("a non-timed row with no timings validates");
    }

    // --- FINDING 3: image_digest must be a content digest, not a mutable tag. ---

    /// A correct `sha256:` + 64 lower-case hex digest validates.
    #[test]
    fn a_correct_sha256_image_digest_validates() {
        let mut report = valid_report();
        report.provenance.comparators[0].image_digest =
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string();
        validate(&report).expect("a sha256 + 64 lower-case hex digest validates");
    }

    /// A mutable tag masquerading as a digest is refused.
    #[test]
    fn a_mutable_tag_image_digest_fails_validation() {
        let mut report = valid_report();
        report.provenance.comparators[0].image_digest = "latest".to_string();
        let err = validate(&report).expect_err("a `latest` tag is not a digest");
        assert_eq!(
            err,
            ValidationError::InvalidImageDigest {
                index: 0,
                value: "latest".to_string(),
            }
        );
    }

    /// A free-form string with no digest structure is refused.
    #[test]
    fn a_non_digest_image_digest_fails_validation() {
        let mut report = valid_report();
        report.provenance.comparators[0].image_digest = "not-a-digest".to_string();
        let err = validate(&report).expect_err("`not-a-digest` is not a digest");
        assert_eq!(
            err,
            ValidationError::InvalidImageDigest {
                index: 0,
                value: "not-a-digest".to_string(),
            }
        );
    }

    /// A digest one hex character short is refused: 63 hex characters is a
    /// truncated digest that pins nothing.
    #[test]
    fn a_63_character_hex_image_digest_fails_validation() {
        let mut report = valid_report();
        let short = format!("sha256:{}", "a".repeat(63));
        report.provenance.comparators[0].image_digest = short.clone();
        let err = validate(&report).expect_err("a 63-char hex digest must fail");
        assert_eq!(
            err,
            ValidationError::InvalidImageDigest {
                index: 0,
                value: short,
            }
        );
    }

    /// An upper-case hex digest is refused, matching the deploy check's
    /// lower-case-only class so the two notions of "digest" agree.
    #[test]
    fn an_uppercase_hex_image_digest_fails_validation() {
        let mut report = valid_report();
        let upper =
            "sha256:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF".to_string();
        report.provenance.comparators[0].image_digest = upper.clone();
        let err = validate(&report).expect_err("an upper-case hex digest must fail");
        assert_eq!(
            err,
            ValidationError::InvalidImageDigest {
                index: 0,
                value: upper,
            }
        );
    }

    /// An unknown key in a report is a deserialization error, not a silently
    /// ignored field: the schema is a frozen contract.
    #[test]
    fn an_unknown_report_key_is_a_deserialization_error() {
        let report = valid_report();
        let mut doc = serde_json::to_value(&report).expect("serialize");
        doc.as_object_mut()
            .expect("object")
            .insert("measrements".to_string(), serde_json::json!([]));
        let err = serde_json::from_value::<MetricsBenchReport>(doc)
            .expect_err("an unknown key must fail to deserialize");
        assert!(err.to_string().contains("measrements"), "{err}");
    }
}
