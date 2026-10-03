//! `execute_ddl` (ADR-2040 decisions D2, D4): the write path for
//! `CREATE [OR REPLACE] EXTERNAL TABLE` and `DROP TABLE`, admitted by
//! [`crate::validate::validate_ddl`] into a [`crate::validate::DdlIntent`]
//! before any grant, store, or manifest here is touched.
//!
//! This is a separate entry point from [`SqlExecutor::execute`] and
//! [`SqlExecutor::execute_accounted`], which stay read-only: nothing on the
//! read path ever writes a manifest, and nothing here plans or executes a
//! query. The caller (`POST /api/v1/sql` in services/ravel-server) must already
//! have checked the request's `ddl` capability;
//! [`SqlExecutor::execute_ddl`] performs no authorization of its own.
//!
//! # The CREATE path, in order
//!
//! 1. The tenant's grants record is read fresh ([`grants::list`]) and the
//!    statement's `LOCATION` resolved against it ([`grants::resolve_location`]);
//!    grants are not cached and are checked at every `CREATE`, the same rule
//!    the read path applies at every query resolve.
//! 2. The external store the resolved grant's profile names is opened.
//! 3. One existing object the grant admits is found under the `LOCATION`
//!    (a bounded listing, mirroring `services/ravel-cli`'s grant-add probe),
//!    and the two qualification probes run on it --
//!    [`probe_preconditions`] and [`probe_not_ravel_bucket`] -- *before*
//!    [`snapshot_location`] ever issues a footer read. A probe refusal costs
//!    a HEAD and a bounded listing, never a footer GET.
//! 4. [`snapshot_location`] reads the file list, decodes every footer, and
//!    derives the one schema they share.
//! 5. The manifest write is built from the snapshot and applied
//!    ([`ravel_pqtable::writer::apply`]), which owns its own compare-and-swap
//!    retry loop.
//!
//! `DROP TABLE` skips every step above but the last: it writes a dropped
//! manifest version directly, naming no grant, store, or snapshot.
//!
//! A `CREATE` is not checked against `max_s3_requests` or any byte budget:
//! grants-and-DDL work is outside query-cost accounting (ADR-2040
//! grants-and-DDL-cost amendment, 2026-10-01). `deadline` bounds the wall
//! time of the whole statement -- the grants read, both qualification
//! probes, the snapshot read, and the manifest write, not only the snapshot
//! step within it ([`DdlExecuteError::Deadline`] on expiry). The snapshot
//! step is passed that same `deadline` value for its own inner timeout, but
//! it starts only after the grants read and both qualification probes, so
//! its timer expires no earlier than the outer one. A statement that runs
//! out of time therefore reports [`DdlExecuteError::Deadline`], not the
//! snapshot's own deadline error.

use std::time::Duration;

use ravel_object_store::external::probe::{
    PreconditionProbeFailure, RavelBucketProbeFailure, probe_not_ravel_bucket, probe_preconditions,
};
use ravel_object_store::{ObjectStoreBackend, PageToken, StoreError};
use ravel_parquet::snapshot::{
    GrantedLocation, LocationSnapshot, PARQUET_SUFFIX, SnapshotError, snapshot_location,
};
use ravel_pqtable::grants::{self, Grant, GrantsError, KeyPrefix};
use ravel_pqtable::resolve;
use ravel_pqtable::writer::{self, WriteError};
use ravel_query::PhaseAccounting;
use ravel_types::TenantHash;

use crate::executor::SqlExecutor;
use crate::parquet::ExternalStoreError;
use crate::validate::{DdlIntent, DdlValidationError, validate_ddl};

/// The grace a manifest write holds before another apply may supersede it
/// (`ravel_pqtable::writer::apply`'s `min_grace_ms`), sourced from ADR-2040's
/// own Lifecycle text: 11 minutes. [`SqlExecutor::execute_ddl`] uses this
/// value only when the executor has no [`SqlExecutor::with_ddl_min_grace_ms`]
/// value installed; the HTTP wiring of issue #2054 installs the deployment's
/// own sweep grace (`sys/gc`'s `max_query_duration`) there instead.
pub const DEFAULT_MIN_GRACE_MS: u64 = 660_000;

/// How many listing pages [`one_object_under`] reads while looking for one
/// object under a resolved `LOCATION`. A `LOCATION` whose first admitted
/// object is this far into a listing is refused rather than probed, the same
/// bound `services/ravel-cli`'s grant-add probe uses.
const MAX_PROBE_LIST_PAGES: usize = 8;

/// The outcome of a committed or no-op `execute_ddl` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlOutcome {
    /// `CREATE [OR REPLACE] EXTERNAL TABLE` committed a new manifest version.
    Created {
        table: String,
        version: u64,
        /// Files the committed manifest names.
        files: usize,
        /// Listed entries skipped because they were directory markers
        /// (zero-byte objects at a `/`-suffixed key).
        skipped_directory_markers: u64,
        /// Listed entries skipped because their key did not end in
        /// [`ravel_parquet::snapshot::PARQUET_SUFFIX`].
        skipped_other_suffixes: u64,
    },
    /// `DROP TABLE` committed a dropped manifest version.
    Dropped { table: String, version: u64 },
    /// `IF NOT EXISTS` found the table already there, or `IF EXISTS` found it
    /// already absent (or already dropped); the statement wrote nothing.
    NoOp { table: String },
}

/// Why [`SqlExecutor::execute_ddl`] could not complete.
///
/// Every variant short of [`DdlExecuteError::Write`] is a refusal before any
/// manifest write is attempted; [`DdlExecuteError::Write`] is a refusal or a
/// failure of the write itself. The HTTP status each should take is noted per
/// variant below, and [`DdlExecuteError::class`] encodes that mapping, which
/// `POST /api/v1/sql` in services/ravel-server renders.
#[derive(Debug, thiserror::Error)]
pub enum DdlExecuteError {
    /// The statement failed the DDL gate (`crate::validate::validate_ddl`),
    /// before any grant, store, or manifest is touched. 400-class.
    #[error(transparent)]
    Validation(#[from] DdlValidationError),

    /// This executor has no Parquet sources configured at all, so neither a
    /// `CREATE` nor a `DROP` has a store to write to; or it has sources but
    /// no credential profiles, so a `CREATE` has no external store to open
    /// (a `DROP` still writes through Ravel's own store). 422-class.
    #[error(
        "this server has no Parquet credential profiles configured; CREATE EXTERNAL TABLE and \
         DROP TABLE cannot be served"
    )]
    NotConfigured,

    /// Reading the tenant's grants record, or resolving the statement's
    /// `LOCATION` against it, failed -- including a `LOCATION` outside every
    /// grant, a scheme/bucket mismatch, or one that only shares a prefix
    /// without being contained by it ([`GrantsError::LocationNotGranted`]).
    /// 422-class, except a grants-record store fault, which is 503-class.
    #[error("resolving the statement's LOCATION against this tenant's grants: {0}")]
    Location(#[from] GrantsError),

    /// The external store the resolved grant's profile names could not be
    /// opened. 503-class when opening it failed; 422-class when this server
    /// does not configure the profile, or the profile and bucket reach
    /// Ravel's own data bucket.
    #[error("opening the external store for profile {profile:?}: {source}")]
    ExternalStore {
        profile: String,
        #[source]
        source: ExternalStoreError,
    },

    /// A listing or HEAD read while looking for one object to run the
    /// qualification probes on failed. 503-class.
    #[error("looking for a probe object under {location:?}: {source}")]
    ProbeList {
        location: String,
        #[source]
        source: StoreError,
    },

    /// The `LOCATION` holds no object the grant admits, so the store's
    /// preconditions could not be probed. 422-class.
    #[error(
        "the location {location:?} holds no object admitted by its grant, so the store's \
         preconditions could not be probed: create the table over a location that already \
         holds at least one Parquet file"
    )]
    ProbeObjectEmpty { location: String },

    /// [`MAX_PROBE_LIST_PAGES`] listing pages were read while looking for a
    /// probe object, without finding one. 422-class.
    #[error(
        "no object under {location:?} admitted by its grant was found within the first \
         {pages} listing pages, so the store's preconditions could not be probed: create the \
         table over a narrower location, one whose first objects are listed sooner"
    )]
    ProbeObjectPageCapReached { location: String, pages: usize },

    /// The store behind the resolved grant does not qualify for pinned reads
    /// ([`probe_preconditions`]): it does not honor conditional reads the way
    /// a manifest's pinned file identity requires. 422-class.
    #[error("the store behind {location:?} does not qualify for pinned reads: {source}")]
    PreconditionProbe {
        location: String,
        #[source]
        source: PreconditionProbeFailure,
    },

    /// The bucket behind the resolved grant did not qualify as external
    /// ([`probe_not_ravel_bucket`]): it is the same bucket Ravel's own store
    /// writes, or carries Ravel's tenancy marker. 422-class.
    #[error("the bucket behind {location:?} did not qualify as external: {source}")]
    RavelBucketProbe {
        location: String,
        #[source]
        source: RavelBucketProbeFailure,
    },

    /// Reading the `LOCATION`'s file list and schema failed, including the
    /// process memory budget refusing a footer's reservation. 422-class,
    /// except a storage fault ([`SnapshotError::List`], [`SnapshotError::Store`]),
    /// which is 503-class; a corrupt or mismatched file
    /// ([`SnapshotError::Corrupt`], [`SnapshotError::SchemaMismatch`]), which
    /// is 500-class; and the inner snapshot deadline
    /// ([`SnapshotError::Deadline`]), which is 504-class like the outer one.
    #[error("reading the Parquet file list of {location:?}: {source}")]
    Snapshot {
        location: String,
        #[source]
        source: SnapshotError,
    },

    /// A `ravel.cast.<column>` option named a column the snapshotted Parquet
    /// schema does not have. `validate_ddl` cannot catch this: it has no
    /// schema to check against, only the statement's own options. 422-class.
    #[error(
        "OPTIONS key \"ravel.cast.{column}\" names a column that the Parquet schema under \
         LOCATION does not have"
    )]
    UnknownCastColumn { column: String },

    /// The manifest write failed. [`WriteError::TableExists`] is a plain
    /// `CREATE` on a table that already exists, without `IF NOT EXISTS`
    /// (409-class); [`WriteError::TableNotFound`] is `DROP TABLE` without `IF
    /// EXISTS` on a table that is not there (404-class);
    /// [`WriteError::Store`], [`WriteError::Resolve`] and
    /// [`WriteError::RetriesExhausted`] are retryable storage contention
    /// (503-class); the remaining variants
    /// ([`WriteError::EmptyFileList`], [`WriteError::NoPutBudget`],
    /// [`WriteError::VersionOverflow`], [`WriteError::Manifest`],
    /// [`WriteError::Key`], [`WriteError::Name`]) are a malformed or
    /// internally inconsistent write (422/500-class; see
    /// [`DdlExecuteError::class`]).
    #[error(transparent)]
    Write(#[from] WriteError),

    /// The whole statement -- the grants read, both qualification probes,
    /// the snapshot read, and the manifest write -- did not complete within
    /// `deadline`. A write already in flight when this fires is not rolled
    /// back: `deadline` bounds how long this call waits for the outcome, not
    /// whether `ravel_pqtable::writer::apply`'s own put reaches the store.
    /// 504-class.
    #[error("the statement did not complete within its deadline of {deadline:?}")]
    Deadline { deadline: Duration },
}

/// The client-visible class of a [`DdlExecuteError`], for HTTP status
/// selection. Plays the same role [`crate::error::ErrorClass`] plays for
/// [`crate::SqlError`], extended with the two statuses a DDL statement can
/// also return: a plain `CREATE` colliding with an existing table (409), and
/// a plain `DROP` naming a table that is not there (404).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlErrorClass {
    /// The statement is malformed or outside the accepted DDL subset. 400.
    BadRequest,
    /// A plain `CREATE` on a table that already exists. 409.
    Conflict,
    /// A plain `DROP` on a table that is not there. 404.
    NotFound,
    /// The statement is well-formed but cannot be served: no object admits
    /// the `LOCATION`, no file to snapshot, a budget exceeded. 422.
    Unsupported,
    /// A transient storage-layer fault, or contention a retry can resolve.
    /// 503.
    Unavailable,
    /// The wall deadline expired. 504.
    Timeout,
    /// A permanent data-integrity fault: a corrupt or misfiled stored
    /// record, or an internally inconsistent key or version. 500.
    Internal,
}

impl DdlExecuteError {
    /// The client-visible class, for HTTP status selection.
    pub fn class(&self) -> DdlErrorClass {
        match self {
            DdlExecuteError::Validation(_) => DdlErrorClass::BadRequest,
            DdlExecuteError::NotConfigured => DdlErrorClass::Unsupported,
            DdlExecuteError::Location(source) => grants_error_class(source),
            DdlExecuteError::ExternalStore { source, .. } => external_store_class(source),
            DdlExecuteError::ProbeList { .. } => DdlErrorClass::Unavailable,
            DdlExecuteError::ProbeObjectEmpty { .. } => DdlErrorClass::Unsupported,
            DdlExecuteError::ProbeObjectPageCapReached { .. } => DdlErrorClass::Unsupported,
            DdlExecuteError::PreconditionProbe { source, .. } => precondition_probe_class(source),
            DdlExecuteError::RavelBucketProbe { source, .. } => ravel_bucket_probe_class(source),
            DdlExecuteError::Snapshot { source, .. } => snapshot_error_class(source),
            DdlExecuteError::UnknownCastColumn { .. } => DdlErrorClass::Unsupported,
            DdlExecuteError::Write(source) => write_error_class(source),
            DdlExecuteError::Deadline { .. } => DdlErrorClass::Timeout,
        }
    }

    /// The message a client may see. A storage fault or an internal
    /// data-integrity fault collapses to a fixed string; every other class
    /// keeps its own text, derived only from the statement's own `LOCATION`,
    /// table name, and OPTIONS, which the caller already supplied --
    /// [`DdlExecuteError::PreconditionProbe`] and
    /// [`DdlExecuteError::RavelBucketProbe`] are handled separately below
    /// rather than through `self.to_string()`, because their own `Display`
    /// interpolates the probe's inner [`PreconditionProbeFailure`] or
    /// [`RavelBucketProbeFailure`], which for several variants carries a raw
    /// [`StoreError`] (backend host or response text) verbatim; each of
    /// those two variants instead gets a fixed message naming only
    /// `location`.
    ///
    /// The full `Display` of `self` stays available to the caller for
    /// server-side logging and is never produced here.
    pub fn client_message(&self) -> String {
        match self {
            DdlExecuteError::PreconditionProbe { location, source } => {
                if precondition_probe_class(source) == DdlErrorClass::Unavailable {
                    return crate::error::MSG_UNAVAILABLE.to_string();
                }
                match source {
                    PreconditionProbeFailure::Head { .. }
                    | PreconditionProbeFailure::MatchingPinRefused { .. } => format!(
                        "the store behind {location:?} refused a read of the probe object, so \
                         it cannot be qualified for pinned reads"
                    ),
                    PreconditionProbeFailure::WrongPinAccepted { .. }
                    | PreconditionProbeFailure::WrongPinWrongError { .. } => format!(
                        "the store behind {location:?} does not qualify for pinned reads: it \
                         does not honor read preconditions correctly"
                    ),
                }
            }
            DdlExecuteError::RavelBucketProbe { location, source } => {
                match ravel_bucket_probe_class(source) {
                    DdlErrorClass::Unavailable => crate::error::MSG_UNAVAILABLE.to_string(),
                    DdlErrorClass::Internal => crate::error::MSG_INTERNAL.to_string(),
                    DdlErrorClass::BadRequest
                    | DdlErrorClass::Conflict
                    | DdlErrorClass::NotFound
                    | DdlErrorClass::Unsupported
                    | DdlErrorClass::Timeout => match source {
                        RavelBucketProbeFailure::Inconclusive { .. } => format!(
                            "the bucket behind {location:?} could not be shown to be separate \
                             from Ravel's own data bucket: check that its grant's profile \
                             reaches the account and endpoint it is configured for"
                        ),
                        RavelBucketProbeFailure::ProbeWriteFailed { .. }
                        | RavelBucketProbeFailure::SameBucket { .. }
                        | RavelBucketProbeFailure::TenancyMarkerPresent { .. } => format!(
                            "the bucket behind {location:?} did not qualify as external: it is \
                             Ravel's own bucket"
                        ),
                    },
                }
            }
            DdlExecuteError::ExternalStore { source, .. } => match source {
                ExternalStoreError::Open { .. } => crate::error::MSG_UNAVAILABLE.to_string(),
                ExternalStoreError::UnknownProfile { .. } => {
                    "the credential profile this LOCATION's grant names is not configured on \
                     this server"
                        .to_string()
                }
                ExternalStoreError::RavelBucket { .. } => {
                    "the bucket this LOCATION's grant names is Ravel's own data bucket".to_string()
                }
            },
            DdlExecuteError::Snapshot {
                location,
                source: SnapshotError::MemoryExhausted { .. },
            } => match self.class() {
                DdlErrorClass::Unavailable => crate::error::MSG_UNAVAILABLE.to_string(),
                _ => format!(
                    "a Parquet footer under {location:?} is larger than this server's whole \
                     memory budget"
                ),
            },
            _ => match self.class() {
                DdlErrorClass::Unavailable => crate::error::MSG_UNAVAILABLE.to_string(),
                DdlErrorClass::Internal => crate::error::MSG_INTERNAL.to_string(),
                DdlErrorClass::BadRequest
                | DdlErrorClass::Conflict
                | DdlErrorClass::NotFound
                | DdlErrorClass::Unsupported
                | DdlErrorClass::Timeout => self.to_string(),
            },
        }
    }
}

/// [`DdlExecuteError::Location`]'s class: a storage fault or exhausted
/// compare-and-swap retry is retryable (503); a corrupt or misversioned
/// grants record is a permanent data fault (500); every other variant is a
/// well-formed statement this tenant's grants do not admit (422).
fn grants_error_class(err: &GrantsError) -> DdlErrorClass {
    match err {
        GrantsError::Store { .. } | GrantsError::RetriesExhausted { .. } => {
            DdlErrorClass::Unavailable
        }
        GrantsError::Decode { .. }
        | GrantsError::UnsupportedVersion { .. }
        | GrantsError::VersionBelowFloor { .. }
        | GrantsError::Misfiled { .. } => DdlErrorClass::Internal,
        GrantsError::InvalidLocation { .. }
        | GrantsError::EmptyProfile
        | GrantsError::NonCanonicalGrant { .. }
        | GrantsError::OverlapsOtherProfile { .. }
        | GrantsError::DuplicateGrant { .. }
        | GrantsError::LocationNotGranted { .. }
        | GrantsError::GrantNotFound { .. } => DdlErrorClass::Unsupported,
        GrantsError::Key(_) => DdlErrorClass::Internal,
    }
}

/// [`DdlExecuteError::PreconditionProbe`]'s class. A probe read that failed
/// with a retryable [`StoreError`] (throttling, a timeout, a transient
/// fault) never got an answer, so it is retryable (503) whichever half
/// failed. Any other failure is an answer, and it disqualifies the store
/// (422): a HEAD or a matching-pin read the store refuses outright, a
/// wrong-pin read it serves, or a wrong-pin read it refuses with something
/// other than a precondition failure. A permanent refusal is not reported as
/// retryable, because retrying it gets the same refusal.
fn precondition_probe_class(err: &PreconditionProbeFailure) -> DdlErrorClass {
    match err {
        PreconditionProbeFailure::Head { source, .. }
        | PreconditionProbeFailure::MatchingPinRefused { source, .. }
        | PreconditionProbeFailure::WrongPinWrongError { source, .. } => {
            if source.is_retryable() {
                DdlErrorClass::Unavailable
            } else {
                DdlErrorClass::Unsupported
            }
        }
        PreconditionProbeFailure::WrongPinAccepted { .. } => DdlErrorClass::Unsupported,
    }
}

/// [`DdlExecuteError::RavelBucketProbe`]'s class.
/// [`RavelBucketProbeFailure::ProbeWriteFailed`] is a failure to write the
/// probe object into Ravel's own bucket, so the question was never asked:
/// retryable (503) when the [`StoreError`] is, and otherwise a fault in
/// Ravel's own storage rather than in the caller's statement (500).
/// [`RavelBucketProbeFailure::Inconclusive`] (the candidate's read answered
/// neither a hit nor a clean miss) is 422: its usual cause is a profile that
/// addresses a different account or endpoint than configured, which no retry
/// clears, and the variant carries no typed error to tell a transient read
/// failure apart. [`RavelBucketProbeFailure::SameBucket`] and
/// [`RavelBucketProbeFailure::TenancyMarkerPresent`] are a qualification
/// verdict: the probe proves the candidate is Ravel's own bucket (422).
fn ravel_bucket_probe_class(err: &RavelBucketProbeFailure) -> DdlErrorClass {
    match err {
        RavelBucketProbeFailure::ProbeWriteFailed { source, .. } => {
            if source.is_retryable() {
                DdlErrorClass::Unavailable
            } else {
                DdlErrorClass::Internal
            }
        }
        RavelBucketProbeFailure::Inconclusive { .. }
        | RavelBucketProbeFailure::SameBucket { .. }
        | RavelBucketProbeFailure::TenancyMarkerPresent { .. } => DdlErrorClass::Unsupported,
    }
}

/// [`DdlExecuteError::ExternalStore`]'s class: a store that failed to open
/// is retryable (503); a profile the grant names that this server does not
/// configure, and a profile and bucket that reach Ravel's own data bucket,
/// are configuration verdicts no retry changes (422).
fn external_store_class(err: &ExternalStoreError) -> DdlErrorClass {
    match err {
        ExternalStoreError::Open { .. } => DdlErrorClass::Unavailable,
        ExternalStoreError::UnknownProfile { .. } | ExternalStoreError::RavelBucket { .. } => {
            DdlErrorClass::Unsupported
        }
    }
}

/// [`DdlExecuteError::Snapshot`]'s class: a listing or read fault is
/// retryable (503); a corrupt file or a schema mismatch across the
/// `LOCATION`'s files is a permanent data fault (500); the inner snapshot
/// deadline is a timeout (504), the same class as the outer
/// [`DdlExecuteError::Deadline`]. A footer read the memory budget refused
/// is retryable (503) when the read alone fits the budget's limit, since
/// other reservations free up, and 422 when it does not fit even an empty
/// budget. Every other variant is a well-formed `LOCATION` this build will
/// not snapshot as given (422).
fn snapshot_error_class(err: &SnapshotError) -> DdlErrorClass {
    match err {
        SnapshotError::List { .. } | SnapshotError::Store { .. } => DdlErrorClass::Unavailable,
        SnapshotError::MemoryExhausted {
            requested, limit, ..
        } => {
            if requested <= limit {
                DdlErrorClass::Unavailable
            } else {
                DdlErrorClass::Unsupported
            }
        }
        SnapshotError::Corrupt { .. } | SnapshotError::SchemaMismatch { .. } => {
            DdlErrorClass::Internal
        }
        SnapshotError::Deadline { .. } => DdlErrorClass::Timeout,
        SnapshotError::NoFiles { .. }
        | SnapshotError::TooManyFiles { .. }
        | SnapshotError::Unaddressable { .. }
        | SnapshotError::OutsideGrant { .. }
        | SnapshotError::FileChanged { .. }
        | SnapshotError::FileMissing { .. }
        | SnapshotError::EmptyFile { .. } => DdlErrorClass::Unsupported,
    }
}

/// [`DdlExecuteError::Write`]'s class: [`WriteError::TableExists`] and
/// [`WriteError::TableNotFound`] are the two statuses a DDL statement can
/// return besides the shared set (409 and 404); a storage fault, a newest
/// version deleted under every read attempt, or an exhausted
/// compare-and-swap retry is retryable (503); a version that has no
/// successor, a corrupt or newer-format manifest, a misfiled object under
/// the table prefix, or a malformed key or table name is a permanent data
/// fault (500); an empty file list or an unusable grace budget is a
/// well-formed statement this write refuses (422).
fn write_error_class(err: &WriteError) -> DdlErrorClass {
    match err {
        WriteError::TableExists { .. } => DdlErrorClass::Conflict,
        WriteError::TableNotFound { .. } => DdlErrorClass::NotFound,
        WriteError::Resolve(source) => match source {
            resolve::ResolveError::Store { .. } | resolve::ResolveError::Vanished { .. } => {
                DdlErrorClass::Unavailable
            }
            resolve::ResolveError::ForeignKey { .. }
            | resolve::ResolveError::Key(_)
            | resolve::ResolveError::Manifest(_) => DdlErrorClass::Internal,
        },
        WriteError::Store { .. } | WriteError::RetriesExhausted { .. } => {
            DdlErrorClass::Unavailable
        }
        WriteError::EmptyFileList { .. } | WriteError::NoPutBudget { .. } => {
            DdlErrorClass::Unsupported
        }
        WriteError::VersionOverflow { .. }
        | WriteError::Manifest(_)
        | WriteError::Key(_)
        | WriteError::Name(_) => DdlErrorClass::Internal,
    }
}

/// What [`one_object_under`] found under a resolved `LOCATION`.
#[derive(Debug, PartialEq, Eq)]
enum ProbeObject {
    /// The key of one object the grant admits.
    Found(String),
    /// The listing ran to its end without one.
    Empty,
    /// [`MAX_PROBE_LIST_PAGES`] pages were read without one, and the listing
    /// had more.
    PageCapReached,
}

/// One object the resolved grant admits under `key`, looked for within
/// [`MAX_PROBE_LIST_PAGES`] listing pages.
///
/// Mirrors `services/ravel-cli/src/parquet_grant.rs`'s `one_object_under`,
/// scoped to the statement's own resolved `KeyPrefix` (which can be narrower
/// than the whole grant's prefix) rather than the grant's prefix, since that
/// is the key space `execute_ddl` already has in hand from
/// [`grants::resolve_location`]. The listing runs under the directory key
/// with a trailing `/`, which is what excludes a sibling whose key merely
/// shares the same string prefix (`data/t10/` against `data/t1`). An object
/// is admitted when it has a `.parquet` suffix and a non-zero size, which
/// excludes a folder-marker object (zero bytes, often named exactly like
/// the directory key); neither would `snapshot_location` treat as a data
/// file. The [`grants::contains_key`] check on each listed key cannot fail
/// for a key under a location `resolve_location` admitted, and is kept as
/// defence in depth.
///
/// A non-directory `key` names one object directly; it is resolved with a
/// single HEAD and never falls through to a listing, which -- scoped to the
/// exact object key as a string prefix -- would also match an unrelated
/// sibling like `<key>.bak`.
async fn one_object_under(
    store: &dyn ObjectStoreBackend,
    grant: &Grant,
    key: &KeyPrefix,
) -> Result<ProbeObject, DdlExecuteError> {
    if !key.directory {
        // No suffix test here: ADR-2040 reads a single-object LOCATION
        // whatever its suffix, and `snapshot_location` accepts it.
        return match store.head(&key.key).await {
            Ok(meta) if meta.size > 0 => Ok(ProbeObject::Found(key.key.clone())),
            Ok(_) | Err(StoreError::NotFound) => Ok(ProbeObject::Empty),
            Err(source) => Err(DdlExecuteError::ProbeList {
                location: key.key.clone(),
                source,
            }),
        };
    }

    // `key.key` carries no trailing slash (`KeyPrefix`'s own invariant), so
    // listing it bare would also match a sibling directory sharing the same
    // string prefix (`data/orders` also prefixes `data/orders-backup/...`).
    // Scoping the listing to `<key>/` keeps it to this directory's own
    // children.
    let list_prefix = if key.key.is_empty() {
        String::new()
    } else {
        format!("{}/", key.key)
    };
    let mut page: Option<PageToken> = None;
    for _ in 0..MAX_PROBE_LIST_PAGES {
        let listed =
            store
                .list(&list_prefix, page)
                .await
                .map_err(|source| DdlExecuteError::ProbeList {
                    location: key.key.clone(),
                    source,
                })?;
        for meta in &listed.objects {
            if meta.size > 0
                && meta.key.ends_with(PARQUET_SUFFIX)
                && grants::contains_key(grant, &grant.profile, &grant.bucket, meta.key.as_bytes())
            {
                return Ok(ProbeObject::Found(meta.key.clone()));
            }
        }
        match listed.next {
            Some(next) => page = Some(next),
            None => return Ok(ProbeObject::Empty),
        }
    }
    Ok(ProbeObject::PageCapReached)
}

impl SqlExecutor {
    /// Execute a `CREATE [OR REPLACE] EXTERNAL TABLE` or `DROP TABLE`
    /// statement (ADR-2040 D2, D4) admitted by
    /// [`crate::validate::validate_ddl`].
    ///
    /// The caller must already have checked the request's `ddl` capability:
    /// this method performs no authorization of its own, the same contract
    /// [`SqlExecutor::execute`] and [`SqlExecutor::execute_accounted`] have
    /// for read access. `created_by` is recorded on the manifest verbatim
    /// (the caller's identity, however the caller names it); the manifest's
    /// own commit timestamp comes from this executor's injected
    /// `ravel_pqtable::clock::Clock` ([`SqlExecutor::with_clock`]), not from
    /// a caller-supplied value. `deadline` bounds the wall time of the whole
    /// statement -- from the grants read through the manifest write, not
    /// only the snapshot step within it -- the same role it plays for
    /// [`SqlExecutor::execute_accounted`]; past it this call returns
    /// [`DdlExecuteError::Deadline`].
    ///
    /// A `CREATE` is not checked against `max_s3_requests` or any byte
    /// budget: grants-and-DDL work is outside query-cost accounting
    /// (ADR-2040 grants-and-DDL-cost amendment, 2026-10-01).
    pub async fn execute_ddl(
        &self,
        tenant: TenantHash,
        statement: &str,
        created_by: &str,
        deadline: Duration,
    ) -> Result<DdlOutcome, DdlExecuteError> {
        // Validation is CPU over the statement text and touches no store, so
        // it runs before the timer: a refusal is never reported as a deadline.
        let intent = validate_ddl(statement)?;
        match tokio::time::timeout(
            deadline,
            self.execute_ddl_within_deadline(tenant, intent, statement, created_by, deadline),
        )
        .await
        {
            Ok(result) => result,
            Err(_elapsed) => Err(DdlExecuteError::Deadline { deadline }),
        }
    }

    /// The body of [`Self::execute_ddl`], run under its caller's
    /// `tokio::time::timeout`. `deadline` is threaded through unchanged to
    /// [`snapshot_location`]'s own inner bound; see [`Self::execute_ddl`]'s
    /// doc comment for how the two compose.
    async fn execute_ddl_within_deadline(
        &self,
        tenant: TenantHash,
        intent: DdlIntent,
        statement: &str,
        created_by: &str,
        deadline: Duration,
    ) -> Result<DdlOutcome, DdlExecuteError> {
        let parquet = self
            .parquet_sources()
            .ok_or(DdlExecuteError::NotConfigured)?;
        let ravel_store = parquet.ravel_store().as_ref();
        let clock = self.clock().as_ref();

        match intent {
            DdlIntent::CreateExternal {
                name,
                if_not_exists,
                or_replace,
                location,
                options,
            } => {
                // Plain `CREATE [IF NOT EXISTS]` (never `OR REPLACE`, which
                // always writes a fresh snapshot over whatever is there) on a
                // table that already exists is decided here, before any
                // grant is read, any store opened, or any object probed or
                // snapshotted: `writer::apply` would reach the same
                // NoOp/TableExists verdict itself, but only after paying for
                // the whole snapshot first.
                if !or_replace {
                    let existing = resolve::newest(ravel_store, &tenant, &name)
                        .await
                        .map_err(WriteError::from)?;
                    if existing.is_some_and(|manifest| manifest.is_live()) {
                        if if_not_exists {
                            return Ok(DdlOutcome::NoOp { table: name });
                        }
                        return Err(DdlExecuteError::Write(WriteError::TableExists {
                            table: name,
                        }));
                    }
                }

                let external_stores = parquet
                    .external_stores()
                    .ok_or(DdlExecuteError::NotConfigured)?;

                let tenant_grants = grants::list(ravel_store, &tenant).await?;
                let (grant, key) = grants::resolve_location(&tenant_grants, &location)?;

                let external = external_stores
                    .store(&grant.profile, &grant.bucket)
                    .map_err(|source| DdlExecuteError::ExternalStore {
                        profile: grant.profile.clone(),
                        source,
                    })?;

                let probe_key = match one_object_under(external.as_ref(), &grant, &key).await? {
                    ProbeObject::Found(key) => key,
                    ProbeObject::Empty => {
                        return Err(DdlExecuteError::ProbeObjectEmpty { location });
                    }
                    ProbeObject::PageCapReached => {
                        return Err(DdlExecuteError::ProbeObjectPageCapReached {
                            location,
                            pages: MAX_PROBE_LIST_PAGES,
                        });
                    }
                };
                // Both qualification probes run here, before `snapshot_location`
                // below issues its first footer GET: a probe refusal must cost
                // only the HEAD/listing above, never a footer read.
                probe_preconditions(external.as_ref(), &probe_key)
                    .await
                    .map_err(|source| DdlExecuteError::PreconditionProbe {
                        location: location.clone(),
                        source,
                    })?;
                probe_not_ravel_bucket(ravel_store, external.as_ref())
                    .await
                    .map_err(|source| DdlExecuteError::RavelBucketProbe {
                        location: location.clone(),
                        source,
                    })?;

                let grant_url = grant.url();
                let granted_location = GrantedLocation { grant, key };
                let phase_accounting = PhaseAccounting::new();
                let LocationSnapshot {
                    files,
                    schema,
                    skipped_directory_markers,
                    skipped_other_suffixes,
                } = snapshot_location(
                    external.as_ref(),
                    &granted_location,
                    parquet.get_limiter(),
                    self.process_memory_budget(),
                    deadline,
                    &phase_accounting,
                )
                .await
                .map_err(|source| DdlExecuteError::Snapshot {
                    location: location.clone(),
                    source,
                })?;
                let file_count = files.len();

                // `validate_ddl` admits any `ravel.cast.<column>` key whose
                // column name passes the charset rule (D5): it has no schema
                // to check the column against. The schema only exists once
                // the snapshot above has read it, so the column's presence
                // is checked here instead.
                for key in options.keys() {
                    if let Some(column) = key.strip_prefix("ravel.cast.")
                        && schema.column_with_name(column).is_none()
                    {
                        return Err(DdlExecuteError::UnknownCastColumn {
                            column: column.to_string(),
                        });
                    }
                }

                let write_intent = if or_replace {
                    writer::Intent::CreateOrReplace {
                        location,
                        grant: grant_url,
                        files,
                        options,
                        created_by: created_by.to_string(),
                        statement: statement.to_string(),
                    }
                } else {
                    writer::Intent::Create {
                        if_not_exists,
                        location,
                        grant: grant_url,
                        files,
                        options,
                        created_by: created_by.to_string(),
                        statement: statement.to_string(),
                    }
                };

                let outcome = writer::apply(
                    ravel_store,
                    &tenant,
                    &name,
                    write_intent,
                    clock,
                    self.ddl_min_grace_ms(),
                )
                .await?;

                Ok(match outcome {
                    writer::Outcome::Committed { version } => DdlOutcome::Created {
                        table: name,
                        version,
                        files: file_count,
                        skipped_directory_markers,
                        skipped_other_suffixes,
                    },
                    writer::Outcome::NoOp => DdlOutcome::NoOp { table: name },
                })
            }
            DdlIntent::Drop { name, if_exists } => {
                let write_intent = writer::Intent::Drop {
                    if_exists,
                    created_by: created_by.to_string(),
                    statement: statement.to_string(),
                };
                let outcome = writer::apply(
                    ravel_store,
                    &tenant,
                    &name,
                    write_intent,
                    clock,
                    self.ddl_min_grace_ms(),
                )
                .await?;
                Ok(match outcome {
                    writer::Outcome::Committed { version } => DdlOutcome::Dropped {
                        table: name,
                        version,
                    },
                    writer::Outcome::NoOp => DdlOutcome::NoOp { table: name },
                })
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_pqtable::grants::LocationDefect;
    use ravel_pqtable::keys::KeyError;
    use ravel_pqtable::manifest::ManifestError;
    use ravel_pqtable::names::{NameError, TableDefect};
    use ravel_pqtable::resolve::ResolveError;

    use super::*;
    use crate::validate::DdlValidationError;

    fn store_error() -> StoreError {
        StoreError::NotFound
    }

    /// A distinctive marker that stands in for backend-specific detail (an
    /// endpoint host, a raw response body) a [`StoreError`] can carry. Used
    /// to prove a client message derives only from `location`, never from
    /// the store's own error text.
    const SENTINEL: &str = "SENTINEL-BACKEND-DETAIL-9f3a";

    fn sentinel_store_error() -> StoreError {
        StoreError::Transient(SENTINEL.to_string())
    }

    fn permanent_sentinel_store_error() -> StoreError {
        StoreError::Permanent(SENTINEL.to_string())
    }

    #[test]
    fn validation_is_bad_request() {
        let err = DdlExecuteError::Validation(DdlValidationError::Empty);
        assert_eq!(err.class(), DdlErrorClass::BadRequest);
    }

    #[test]
    fn not_configured_is_unsupported() {
        assert_eq!(
            DdlExecuteError::NotConfigured.class(),
            DdlErrorClass::Unsupported
        );
    }

    // A profile the server does not configure, and a profile that reaches
    // Ravel's own bucket, are configuration verdicts no retry changes; the
    // client message names neither the profile nor the bucket.
    #[test]
    fn external_store_configuration_verdicts_are_unsupported_and_redacted() {
        for source in [
            ExternalStoreError::UnknownProfile {
                profile: SENTINEL.to_string(),
            },
            ExternalStoreError::RavelBucket {
                profile: SENTINEL.to_string(),
                bucket: SENTINEL.to_string(),
            },
        ] {
            let err = DdlExecuteError::ExternalStore {
                profile: SENTINEL.to_string(),
                source,
            };
            assert_eq!(err.class(), DdlErrorClass::Unsupported, "{err}");
            let message = err.client_message();
            assert!(!message.contains(SENTINEL), "{message}");
        }
    }

    #[test]
    fn probe_list_is_unavailable() {
        let err = DdlExecuteError::ProbeList {
            location: "s3://b/p".to_string(),
            source: store_error(),
        };
        assert_eq!(err.class(), DdlErrorClass::Unavailable);
    }

    #[test]
    fn probe_object_empty_is_unsupported() {
        let err = DdlExecuteError::ProbeObjectEmpty {
            location: "s3://b/p".to_string(),
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
    }

    #[test]
    fn probe_object_page_cap_reached_is_unsupported() {
        let err = DdlExecuteError::ProbeObjectPageCapReached {
            location: "s3://b/p".to_string(),
            pages: MAX_PROBE_LIST_PAGES,
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
    }

    // `PreconditionProbeFailure::Head` and `::MatchingPinRefused` with a
    // retryable `StoreError` are a failure to ask: `Unavailable`, and
    // `client_message` must redact the inner `StoreError` rather than echo
    // `self.to_string()`.

    #[test]
    fn precondition_probe_head_is_unavailable_and_redacted() {
        let err = DdlExecuteError::PreconditionProbe {
            location: "s3://b/p".to_string(),
            source: PreconditionProbeFailure::Head {
                key: "k".to_string(),
                source: sentinel_store_error(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unavailable);
        let message = err.client_message();
        assert_eq!(message, crate::error::MSG_UNAVAILABLE);
        assert!(!message.contains(SENTINEL), "{message}");
    }

    #[test]
    fn precondition_probe_matching_pin_refused_is_unavailable_and_redacted() {
        let err = DdlExecuteError::PreconditionProbe {
            location: "s3://b/p".to_string(),
            source: PreconditionProbeFailure::MatchingPinRefused {
                key: "k".to_string(),
                source: sentinel_store_error(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unavailable);
        let message = err.client_message();
        assert_eq!(message, crate::error::MSG_UNAVAILABLE);
        assert!(!message.contains(SENTINEL), "{message}");
    }

    // `WrongPinAccepted` and `WrongPinWrongError` are a genuine qualification
    // verdict: `Unsupported`, with a fixed client message naming only
    // `location` -- never the probe's own key, and never (for
    // `WrongPinWrongError`) the inner `StoreError` its `Display` carries.

    #[test]
    fn precondition_probe_wrong_pin_accepted_is_unsupported_and_names_location_only() {
        let err = DdlExecuteError::PreconditionProbe {
            location: "s3://b/p".to_string(),
            source: PreconditionProbeFailure::WrongPinAccepted {
                key: "distinct-probe-key-123".to_string(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
        let message = err.client_message();
        assert!(message.contains("s3://b/p"), "{message}");
        assert!(!message.contains("distinct-probe-key-123"), "{message}");
    }

    #[test]
    fn precondition_probe_wrong_pin_wrong_error_is_unsupported_and_redacted() {
        let err = DdlExecuteError::PreconditionProbe {
            location: "s3://b/p".to_string(),
            source: PreconditionProbeFailure::WrongPinWrongError {
                key: "k".to_string(),
                source: permanent_sentinel_store_error(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
        let message = err.client_message();
        assert!(message.contains("s3://b/p"), "{message}");
        assert!(!message.contains(SENTINEL), "{message}");
    }

    // A retryable store error on the wrong-pin read is a failure to ask, not
    // a verdict on the store's precondition support.
    #[test]
    fn precondition_probe_wrong_pin_transient_error_is_unavailable() {
        let err = DdlExecuteError::PreconditionProbe {
            location: "s3://b/p".to_string(),
            source: PreconditionProbeFailure::WrongPinWrongError {
                key: "k".to_string(),
                source: sentinel_store_error(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unavailable);
        assert_eq!(err.client_message(), crate::error::MSG_UNAVAILABLE);
    }

    // A HEAD or a matching-pin read the store refuses permanently is an
    // answer: retrying it gets the same refusal, so it is not `Unavailable`.
    #[test]
    fn precondition_probe_permanent_head_refusal_is_unsupported_and_redacted() {
        let err = DdlExecuteError::PreconditionProbe {
            location: "s3://b/p".to_string(),
            source: PreconditionProbeFailure::Head {
                key: "k".to_string(),
                source: StoreError::AccessDenied(SENTINEL.to_string()),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
        let message = err.client_message();
        assert!(message.contains("s3://b/p"), "{message}");
        assert!(
            message.contains("refused a read of the probe object"),
            "{message}"
        );
        assert!(!message.contains(SENTINEL), "{message}");
    }

    #[test]
    fn precondition_probe_permanent_matching_pin_refusal_is_unsupported_and_redacted() {
        let err = DdlExecuteError::PreconditionProbe {
            location: "s3://b/p".to_string(),
            source: PreconditionProbeFailure::MatchingPinRefused {
                key: "k".to_string(),
                source: permanent_sentinel_store_error(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
        let message = err.client_message();
        assert!(message.contains("s3://b/p"), "{message}");
        assert!(!message.contains(SENTINEL), "{message}");
    }

    // A probe write into Ravel's own bucket that fails permanently is a
    // fault in Ravel's storage, not in the caller's statement.
    #[test]
    fn ravel_bucket_probe_permanent_write_failure_is_internal_and_redacted() {
        let err = DdlExecuteError::RavelBucketProbe {
            location: "s3://b/p".to_string(),
            source: RavelBucketProbeFailure::ProbeWriteFailed {
                key: "k".to_string(),
                source: permanent_sentinel_store_error(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Internal);
        let message = err.client_message();
        assert_eq!(message, crate::error::MSG_INTERNAL);
        assert!(!message.contains(SENTINEL), "{message}");
    }

    // A retryable `ProbeWriteFailed` is a failure to ask (the probe object
    // never reached Ravel's own bucket): `Unavailable`, redacted.

    #[test]
    fn ravel_bucket_probe_write_failed_is_unavailable_and_redacted() {
        let err = DdlExecuteError::RavelBucketProbe {
            location: "s3://b/p".to_string(),
            source: RavelBucketProbeFailure::ProbeWriteFailed {
                key: "k".to_string(),
                source: sentinel_store_error(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unavailable);
        let message = err.client_message();
        assert_eq!(message, crate::error::MSG_UNAVAILABLE);
        assert!(!message.contains(SENTINEL), "{message}");
    }

    // `Inconclusive` usually means a profile addressing another account, which
    // no retry clears: `Unsupported`, with a message naming only `location`
    // that does not claim the bucket is Ravel's.
    #[test]
    fn ravel_bucket_probe_inconclusive_is_unsupported_and_redacted() {
        let err = DdlExecuteError::RavelBucketProbe {
            location: "s3://b/p".to_string(),
            source: RavelBucketProbeFailure::Inconclusive {
                key: "k".to_string(),
                detail: SENTINEL.to_string(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
        let message = err.client_message();
        assert!(message.contains("s3://b/p"), "{message}");
        assert!(!message.contains("Ravel's own bucket"), "{message}");
        assert!(!message.contains(SENTINEL), "{message}");
    }

    // `SameBucket` and `TenancyMarkerPresent` are a genuine qualification
    // verdict (the probe proves the candidate is Ravel's own bucket):
    // `Unsupported`, with a fixed client message naming only `location`.

    #[test]
    fn ravel_bucket_probe_same_bucket_is_unsupported_and_names_location_only() {
        let err = DdlExecuteError::RavelBucketProbe {
            location: "s3://b/p".to_string(),
            source: RavelBucketProbeFailure::SameBucket {
                key: "distinct-probe-key-123".to_string(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
        let message = err.client_message();
        assert!(message.contains("s3://b/p"), "{message}");
        assert!(!message.contains("distinct-probe-key-123"), "{message}");
    }

    #[test]
    fn ravel_bucket_probe_tenancy_marker_present_is_unsupported_and_names_location_only() {
        let err = DdlExecuteError::RavelBucketProbe {
            location: "s3://b/p".to_string(),
            source: RavelBucketProbeFailure::TenancyMarkerPresent {
                key: "distinct-probe-key-123".to_string(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
        let message = err.client_message();
        assert!(message.contains("s3://b/p"), "{message}");
        assert!(!message.contains("distinct-probe-key-123"), "{message}");
    }

    #[test]
    fn unknown_cast_column_is_unsupported() {
        let err = DdlExecuteError::UnknownCastColumn {
            column: "c".to_string(),
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
    }

    #[test]
    fn deadline_is_timeout() {
        let err = DdlExecuteError::Deadline {
            deadline: Duration::from_secs(5),
        };
        assert_eq!(err.class(), DdlErrorClass::Timeout);
    }

    #[test]
    fn grants_store_and_retries_exhausted_are_unavailable() {
        assert_eq!(
            grants_error_class(&GrantsError::Store {
                key: "k".to_string(),
                source: store_error(),
            }),
            DdlErrorClass::Unavailable
        );
        assert_eq!(
            grants_error_class(&GrantsError::RetriesExhausted { attempts: 3 }),
            DdlErrorClass::Unavailable
        );
    }

    #[test]
    #[allow(deprecated)]
    fn grants_decode_and_version_and_misfiled_are_internal() {
        for err in [
            GrantsError::Decode {
                key: "k".to_string(),
                source: prost::DecodeError::new("bad"),
            },
            GrantsError::UnsupportedVersion {
                key: "k".to_string(),
                got: 9,
                ceiling: 1,
            },
            GrantsError::VersionBelowFloor {
                key: "k".to_string(),
                got: 0,
                floor: 1,
            },
            GrantsError::Misfiled {
                key: "k".to_string(),
                expected: "a".to_string(),
                actual: "b".to_string(),
            },
            GrantsError::Key(KeyError::ZeroVersion),
        ] {
            assert_eq!(grants_error_class(&err), DdlErrorClass::Internal, "{err}");
        }
    }

    #[test]
    fn grants_semantic_refusals_are_unsupported() {
        for err in [
            GrantsError::InvalidLocation {
                url: "s3://b/..".to_string(),
                defect: LocationDefect::DotDot,
            },
            GrantsError::EmptyProfile,
            GrantsError::NonCanonicalGrant {
                url: "s3://b/p".to_string(),
                prefix: "/p/".to_string(),
            },
            GrantsError::OverlapsOtherProfile {
                url: "s3://b/p".to_string(),
                profile: "a".to_string(),
                existing: "s3://b/p".to_string(),
                existing_profile: "b".to_string(),
            },
            GrantsError::DuplicateGrant {
                url: "s3://b/p".to_string(),
                profile: "a".to_string(),
            },
            GrantsError::LocationNotGranted {
                url: "s3://b/p".to_string(),
            },
            GrantsError::GrantNotFound {
                url: "s3://b/p".to_string(),
            },
        ] {
            assert_eq!(
                grants_error_class(&err),
                DdlErrorClass::Unsupported,
                "{err}"
            );
        }
    }

    #[test]
    fn snapshot_storage_faults_are_unavailable() {
        for err in [
            SnapshotError::List {
                location: "l".to_string(),
                source: store_error(),
            },
            SnapshotError::Store {
                key: "k".to_string(),
                source: store_error(),
            },
        ] {
            assert_eq!(
                snapshot_error_class(&err),
                DdlErrorClass::Unavailable,
                "{err}"
            );
        }
    }

    #[test]
    fn snapshot_corrupt_and_schema_mismatch_are_internal() {
        for err in [
            SnapshotError::Corrupt {
                key: "k".to_string(),
                message: "m".to_string(),
            },
            SnapshotError::SchemaMismatch {
                key: "k".to_string(),
                first: "f".to_string(),
            },
        ] {
            assert_eq!(snapshot_error_class(&err), DdlErrorClass::Internal, "{err}");
        }
    }

    #[test]
    fn snapshot_deadline_is_timeout() {
        let err = SnapshotError::Deadline {
            location: "l".to_string(),
            deadline: Duration::from_secs(1),
        };
        assert_eq!(snapshot_error_class(&err), DdlErrorClass::Timeout);
    }

    #[test]
    fn snapshot_semantic_refusals_are_unsupported() {
        for err in [
            SnapshotError::NoFiles {
                location: "l".to_string(),
            },
            SnapshotError::TooManyFiles {
                location: "l".to_string(),
                limit: 1,
            },
            SnapshotError::Unaddressable {
                location: "l".to_string(),
                key: "k".to_string(),
            },
            SnapshotError::OutsideGrant {
                location: "l".to_string(),
                key: "k".to_string(),
                grant: "g".to_string(),
            },
            SnapshotError::FileChanged {
                key: "k".to_string(),
            },
            SnapshotError::FileMissing {
                key: "k".to_string(),
            },
            SnapshotError::EmptyFile {
                key: "k".to_string(),
            },
            SnapshotError::MemoryExhausted {
                key: "k".to_string(),
                requested: 2,
                reserved: 0,
                limit: 1,
            },
        ] {
            assert_eq!(
                snapshot_error_class(&err),
                DdlErrorClass::Unsupported,
                "{err}"
            );
        }
    }

    // A footer read that fits the budget's limit was refused because other
    // reservations hold the rest, which frees up: retryable, and the client
    // message must not carry the shared budget's occupancy.
    #[test]
    fn snapshot_memory_refusal_within_the_limit_is_unavailable_and_redacted() {
        let err = DdlExecuteError::Snapshot {
            location: "s3://b/p".to_string(),
            source: SnapshotError::MemoryExhausted {
                key: "k".to_string(),
                requested: 4096,
                reserved: 987_654_321,
                limit: 1_000_000_000,
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unavailable);
        let message = err.client_message();
        assert_eq!(message, crate::error::MSG_UNAVAILABLE);
        assert!(!message.contains("987654321"), "{message}");
    }

    // A footer larger than the whole budget never fits: 422, with a message
    // naming the location and none of the budget's figures.
    #[test]
    fn snapshot_memory_refusal_over_the_limit_is_unsupported_and_redacted() {
        let err = DdlExecuteError::Snapshot {
            location: "s3://b/p".to_string(),
            source: SnapshotError::MemoryExhausted {
                key: "k".to_string(),
                requested: 2_000_000_000,
                reserved: 987_654_321,
                limit: 1_000_000_000,
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
        let message = err.client_message();
        assert!(message.contains("s3://b/p"), "{message}");
        assert!(!message.contains("987654321"), "{message}");
        assert!(!message.contains("1000000000"), "{message}");
    }

    #[test]
    fn write_table_exists_is_conflict_and_table_not_found_is_not_found() {
        assert_eq!(
            write_error_class(&WriteError::TableExists {
                table: "t".to_string()
            }),
            DdlErrorClass::Conflict
        );
        assert_eq!(
            write_error_class(&WriteError::TableNotFound {
                table: "t".to_string()
            }),
            DdlErrorClass::NotFound
        );
    }

    #[test]
    fn write_store_resolve_and_retries_exhausted_are_unavailable() {
        for err in [
            WriteError::Store {
                key: "k".to_string(),
                source: store_error(),
            },
            WriteError::Resolve(ResolveError::Vanished {
                table: "t".to_string(),
                attempts: 3,
            }),
            WriteError::RetriesExhausted {
                table: "t".to_string(),
                attempts: 3,
            },
        ] {
            assert_eq!(write_error_class(&err), DdlErrorClass::Unavailable, "{err}");
        }
    }

    // A manifest a newer build wrote, or a misfiled object under the table
    // prefix, does not clear on retry, whichever resolve reports it.
    #[test]
    fn write_resolve_data_faults_are_internal() {
        for err in [
            WriteError::Resolve(ResolveError::Manifest(ManifestError::UnsupportedVersion {
                key: "k".to_string(),
                got: 9,
                ceiling: 1,
            })),
            WriteError::Resolve(ResolveError::ForeignKey {
                key: "k".to_string(),
                prefix: "p".to_string(),
                reason: "r".to_string(),
            }),
        ] {
            assert_eq!(write_error_class(&err), DdlErrorClass::Internal, "{err}");
        }
    }

    #[test]
    fn write_empty_file_list_and_no_put_budget_are_unsupported() {
        for err in [
            WriteError::EmptyFileList {
                table: "t".to_string(),
            },
            WriteError::NoPutBudget { min_grace_ms: 1 },
        ] {
            assert_eq!(write_error_class(&err), DdlErrorClass::Unsupported, "{err}");
        }
    }

    #[test]
    fn write_version_overflow_manifest_key_and_name_are_internal() {
        for err in [
            WriteError::VersionOverflow {
                table: "t".to_string(),
                version: u64::MAX,
            },
            WriteError::Manifest(ManifestError::UnsupportedVersion {
                key: "k".to_string(),
                got: 9,
                ceiling: 1,
            }),
            WriteError::Key(KeyError::ZeroVersion),
            WriteError::Name(NameError::InvalidTable {
                table: "T".to_string(),
                defect: TableDefect::Uppercase,
            }),
        ] {
            assert_eq!(write_error_class(&err), DdlErrorClass::Internal, "{err}");
        }
    }

    #[test]
    fn location_maps_through_grants_error_class() {
        let err = DdlExecuteError::Location(GrantsError::LocationNotGranted {
            url: "s3://b/p".to_string(),
        });
        assert_eq!(err.class(), DdlErrorClass::Unsupported);
    }

    #[test]
    fn snapshot_maps_through_snapshot_error_class() {
        let err = DdlExecuteError::Snapshot {
            location: "l".to_string(),
            source: SnapshotError::Corrupt {
                key: "k".to_string(),
                message: "m".to_string(),
            },
        };
        assert_eq!(err.class(), DdlErrorClass::Internal);
    }

    #[test]
    fn write_maps_through_write_error_class() {
        let err = DdlExecuteError::Write(WriteError::TableExists {
            table: "t".to_string(),
        });
        assert_eq!(err.class(), DdlErrorClass::Conflict);
    }

    #[test]
    fn client_message_redacts_unavailable_and_internal() {
        let unavailable = DdlExecuteError::ProbeList {
            location: "l".to_string(),
            source: store_error(),
        };
        assert_eq!(unavailable.client_message(), crate::error::MSG_UNAVAILABLE);

        let internal = DdlExecuteError::Snapshot {
            location: "l".to_string(),
            source: SnapshotError::Corrupt {
                key: "k".to_string(),
                message: "m".to_string(),
            },
        };
        assert_eq!(internal.client_message(), crate::error::MSG_INTERNAL);
    }

    #[test]
    fn client_message_echoes_everything_else() {
        let err = DdlExecuteError::UnknownCastColumn {
            column: "c".to_string(),
        };
        assert_eq!(err.client_message(), err.to_string());
    }
}
