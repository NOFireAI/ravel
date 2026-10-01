//! `execute_ddl` (ADR-2040 decisions D2, D4): the write path for
//! `CREATE [OR REPLACE] EXTERNAL TABLE` and `DROP TABLE`, admitted by
//! [`crate::validate::validate_ddl`] into a [`crate::validate::DdlIntent`]
//! before any grant, store, or manifest here is touched.
//!
//! This is a separate entry point from [`SqlExecutor::execute`] and
//! [`SqlExecutor::execute_accounted`], which stay read-only: nothing on the
//! read path ever writes a manifest, and nothing here plans or executes a
//! query. The caller (services/ravel-server, not yet wired -- issue #2054's
//! sibling task) must already have checked the request's `ddl` capability;
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
//! step keeps its own inner `deadline` as well, so a snapshot that alone
//! would run past it is reported as [`DdlExecuteError::Snapshot`] before the
//! outer timeout ever fires.

use std::time::Duration;

use ravel_object_store::external::probe::{
    PreconditionProbeFailure, RavelBucketProbeFailure, probe_not_ravel_bucket, probe_preconditions,
};
use ravel_object_store::{ObjectStoreBackend, PageToken, StoreError};
use ravel_parquet::snapshot::{
    GrantedLocation, LocationSnapshot, SnapshotError, snapshot_location,
};
use ravel_pqtable::grants::{self, Grant, GrantsError, KeyPrefix};
use ravel_pqtable::writer::{self, WriteError};
use ravel_query::PhaseAccounting;
use ravel_types::TenantHash;

use crate::executor::SqlExecutor;
use crate::parquet::ExternalStoreError;
use crate::validate::{DdlIntent, DdlValidationError, validate_ddl};

/// The grace a manifest write holds before another apply may supersede it
/// (`ravel_pqtable::writer::apply`'s `min_grace_ms`), sourced from ADR-2040's
/// own Lifecycle text: 11 minutes.
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
/// failure of the write itself. The HTTP status each should take (the `ddl`
/// capability wiring, services/ravel-server, is this task's sibling and not
/// yet in place) is noted per variant, but is a decision for that caller, not
/// encoded here: `DdlExecuteError` carries the distinction, not the mapping.
#[derive(Debug, thiserror::Error)]
pub enum DdlExecuteError {
    /// The statement failed the DDL gate (`crate::validate::validate_ddl`),
    /// before any grant, store, or manifest is touched. 400-class.
    #[error(transparent)]
    Validation(#[from] DdlValidationError),

    /// This executor has no Parquet sources configured at all, or has sources
    /// but no credential profiles: no external store can be opened for a
    /// `CREATE`, and [`ravel_pqtable::writer`] has nowhere to write a `DROP`
    /// either. 422-class.
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
    /// opened. 503-class.
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
    /// except a storage fault, which is 503-class.
    #[error("reading the Parquet file list of {location:?}: {source}")]
    Snapshot {
        location: String,
        #[source]
        source: SnapshotError,
    },

    /// The manifest write failed. [`WriteError::TableExists`] is a plain
    /// `CREATE` on a table that already exists, without `IF NOT EXISTS`
    /// (409-class); [`WriteError::TableNotFound`] is `DROP TABLE` without `IF
    /// EXISTS` on a table that is not there (404-class).
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
/// [`grants::resolve_location`]. Admission is decided by
/// [`grants::contains_key`], the same segment-wise rule the read path
/// applies, together with a `.parquet` suffix and a non-zero size: a bare
/// prefix match admits a folder-marker object (zero bytes, often named
/// exactly like the directory key) and a sibling whose key happens to share
/// the same string prefix, neither of which `snapshot_location` would ever
/// treat as a data file.
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
        return match store.head(&key.key).await {
            Ok(meta) if meta.size > 0 && key.key.ends_with(".parquet") => {
                Ok(ProbeObject::Found(key.key.clone()))
            }
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
                && meta.key.ends_with(".parquet")
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
        match tokio::time::timeout(
            deadline,
            self.execute_ddl_within_deadline(tenant, statement, created_by, deadline),
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
        statement: &str,
        created_by: &str,
        deadline: Duration,
    ) -> Result<DdlOutcome, DdlExecuteError> {
        let intent = validate_ddl(statement)?;
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
                    schema: _,
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
                    DEFAULT_MIN_GRACE_MS,
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
                    DEFAULT_MIN_GRACE_MS,
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
