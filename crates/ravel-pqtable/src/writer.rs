//! Applying a DDL intent as the next manifest version (ADR-2040 D1, D2).
//!
//! [`apply`] resolves the newest version N, decides from the intent and that
//! state what to write, and writes N+1 with `PutMode::CreateIfAbsent`. When
//! another writer took N+1 first, the store refuses the write; `apply` then
//! re-resolves and applies the same intent to the new state:
//!
//! - `CREATE IF NOT EXISTS` on a table that now exists is [`Outcome::NoOp`];
//! - plain `CREATE` on a table that now exists is [`WriteError::TableExists`];
//! - `CREATE OR REPLACE` and `DROP` write the next version on top.
//!
//! No version is overwritten, so a table's history is a total order with no
//! lost update. A dropped version counts as no table, and the next CREATE
//! continues its numbering.
//!
//! No call writes a version above [`MAX_MANIFEST_VERSION`]: when the version
//! an intent would write is above it, [`apply`] refuses with
//! [`WriteError::VersionAboveBound`] before any put. An intent that writes
//! nothing (`IF NOT EXISTS`, `IF EXISTS`) is still a no-op. The resolve it numbers from already ignores versions above
//! the bound ([`resolve::newest`]), so only a table whose newest version sits
//! at the bound itself is refused.
//!
//! Two mechanisms make a refused or failed write decidable. Every call carries
//! an apply nonce, so two callers that would otherwise encode byte-identical
//! manifests still produce different bodies and only the caller whose bytes
//! are there reads the version as its own. Before re-planning, a call reads
//! every version key it has already attempted and reports the one carrying its
//! own nonce as its commit. Reading only the newest version is not enough: a
//! put the store received but never acknowledged can land after `apply` moved
//! on, and another writer can commit on top of it in the meantime, which
//! leaves this call's write in place with someone else's above it.
//!
//! The manifest a writer resolved ages. Half of `min_grace_ms` is the budget
//! from a resolve to the end of the put that follows it. Both ends are read
//! from this call's own clock, so no allowance for the difference between this
//! process's clock and the store's comes off it. `apply` re-resolves rather
//! than putting when that budget is already spent by the time it would put,
//! and otherwise waits on the put with `tokio::time::timeout` for what is left
//! of it. A put that times out is treated as not committed: `apply` reads the
//! key to see whether its bytes landed and, if not, re-resolves. The timeout
//! bounds how long `apply` waits, not the request itself: a request the store
//! already received can still land after it, and the nonce is how a later
//! resolve recognises that write.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use rand::RngExt;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError};
use ravel_types::TenantHash;

use crate::clock::Clock;
use crate::keys::{KeyError, MAX_MANIFEST_VERSION, manifest_key};
use crate::manifest::{
    APPLY_NONCE_LEN, Manifest, ManifestError, ManifestMacKey, PARQUET_TABLE_FORMAT_VERSION,
    ParquetFile, encode_manifest_with,
};
use crate::names::{NameError, validate_table};
use crate::resolve::{self, ResolveError};

/// How many resolves [`apply`] makes before giving up. An attempt ends without
/// a commit when another writer committed that version first, when the
/// resolve aged past the budget before the put, or when the put timed out
/// without landing. A non-retryable store error ends the call at once.
pub const MAX_APPLY_ATTEMPTS: usize = 8;

/// A parsed DDL statement, applied by [`apply`]. The files are external: they
/// live in the tenant's own bucket and Ravel only records where they are and
/// which bytes it pinned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Intent {
    /// `CREATE EXTERNAL TABLE [IF NOT EXISTS] ... LOCATION <location>`.
    Create {
        if_not_exists: bool,
        /// The LOCATION URL as the statement gave it.
        location: String,
        /// The grant that admitted `location`, for audit.
        grant: String,
        files: Vec<ParquetFile>,
        options: BTreeMap<String, String>,
        created_by: String,
        statement: String,
    },
    /// `CREATE OR REPLACE EXTERNAL TABLE ... LOCATION <location>`.
    CreateOrReplace {
        location: String,
        grant: String,
        files: Vec<ParquetFile>,
        options: BTreeMap<String, String>,
        created_by: String,
        statement: String,
    },
    /// `DROP TABLE [IF EXISTS]`. A dropped version carries no location, grant
    /// or files, but still records who ran the statement and what it was.
    Drop {
        if_exists: bool,
        created_by: String,
        statement: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// This call wrote `version`.
    Committed { version: u64 },
    /// The intent required no write (`IF NOT EXISTS` on an existing table,
    /// `IF EXISTS` on a missing one).
    NoOp,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("table {table:?} already exists")]
    TableExists { table: String },
    #[error("table {table:?} does not exist")]
    TableNotFound { table: String },
    #[error("table {table:?} cannot be created with no files")]
    EmptyFileList { table: String },
    #[error("table {table:?}: gave up after {attempts} attempts without committing")]
    RetriesExhausted { table: String, attempts: usize },
    /// Half of `min_grace_ms` rounds down to zero, so no put could finish
    /// inside the resolve-to-put budget.
    #[error("min_grace_ms {min_grace_ms} leaves no resolve-to-put budget")]
    NoPutBudget { min_grace_ms: u64 },
    #[error("table {table:?}: version {version} has no successor")]
    VersionOverflow { table: String, version: u64 },
    /// The next version of `table` would be `version`, above
    /// [`MAX_MANIFEST_VERSION`] (`bound`). Refused before any put.
    #[error(
        "table {table:?}: the next manifest version {version} is above the version bound \
         {bound}, so no further DDL statement can be applied to this table"
    )]
    VersionAboveBound {
        table: String,
        version: u64,
        bound: u64,
    },
    /// A store failure on the manifest write. A retryable failure is reported
    /// only once this writer has checked that its bytes are not at the key.
    #[error("object store error on {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error(transparent)]
    Resolve(#[from] ResolveError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(transparent)]
    Name(#[from] NameError),
}

/// What the intent asks for against one resolved state.
enum Step {
    Done(Outcome),
    Write(Manifest),
}

/// 16 random bytes from `rand::rng()` (ChaCha12 seeded from the OS), so two
/// callers share a nonce only by a 128-bit collision whatever their caller
/// name, clock or process. Generated once per [`apply`] call and reused across
/// its retries, so a retry recognises its own earlier write.
fn apply_nonce() -> Vec<u8> {
    let nonce: [u8; APPLY_NONCE_LEN] = rand::rng().random();
    nonce.to_vec()
}

fn plan_step(
    table: &str,
    intent: &Intent,
    newest: Option<&Manifest>,
    now_ns: i64,
    nonce: &[u8],
) -> Result<Step, WriteError> {
    let next = match newest {
        None => 1,
        Some(m) => m
            .version
            .checked_add(1)
            .ok_or(WriteError::VersionOverflow {
                table: table.to_string(),
                version: m.version,
            })?,
    };
    let exists = newest.is_some_and(Manifest::is_live);
    let live = |location: &str,
                grant: &str,
                files: &[ParquetFile],
                options: &BTreeMap<String, String>,
                created_by: &str,
                statement: &str| {
        Step::Write(Manifest {
            table: table.to_string(),
            version: next,
            dropped: false,
            location: location.to_string(),
            grant: grant.to_string(),
            files: files.to_vec(),
            options: options.clone(),
            created_by: created_by.to_string(),
            created_unix_ns: now_ns,
            statement: statement.to_string(),
            apply_nonce: nonce.to_vec(),
        })
    };
    Ok(match intent {
        Intent::Create {
            if_not_exists: true,
            ..
        } if exists => Step::Done(Outcome::NoOp),
        Intent::Create { .. } if exists => {
            return Err(WriteError::TableExists {
                table: table.to_string(),
            });
        }
        Intent::Create {
            location,
            grant,
            files,
            options,
            created_by,
            statement,
            ..
        }
        | Intent::CreateOrReplace {
            location,
            grant,
            files,
            options,
            created_by,
            statement,
        } => live(location, grant, files, options, created_by, statement),
        Intent::Drop {
            if_exists: true, ..
        } if !exists => Step::Done(Outcome::NoOp),
        Intent::Drop { .. } if !exists => {
            return Err(WriteError::TableNotFound {
                table: table.to_string(),
            });
        }
        Intent::Drop {
            created_by,
            statement,
            ..
        } => Step::Write(Manifest {
            table: table.to_string(),
            version: next,
            dropped: true,
            location: String::new(),
            grant: String::new(),
            files: Vec::new(),
            options: BTreeMap::new(),
            created_by: created_by.to_string(),
            created_unix_ns: now_ns,
            statement: statement.to_string(),
            apply_nonce: nonce.to_vec(),
        }),
    })
}

/// True when `key` holds exactly `bytes`: this writer's own earlier attempt
/// landed, and the acknowledgement or the connection was lost.
async fn holds_own_write(
    store: &dyn ObjectStoreBackend,
    key: &str,
    bytes: &[u8],
) -> Result<bool, WriteError> {
    match store.get(key, GetRange::Full).await {
        Ok(outcome) => Ok(outcome.data.as_ref() == bytes),
        Err(StoreError::NotFound) => Ok(false),
        Err(source) => Err(WriteError::Store {
            key: key.to_string(),
            source,
        }),
    }
}

/// The newest version among `attempted` that carries this call's `nonce`, if
/// any. Every version this call put is read, not only the newest in the table:
/// a put whose acknowledgement never arrived can land after `apply` moved on,
/// and another writer can commit on top of it before `apply` looks again.
async fn own_commit(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    attempted: &[u64],
    nonce: &[u8],
) -> Result<Option<u64>, WriteError> {
    for &version in attempted.iter().rev() {
        if let Some(manifest) = resolve::read_version(store, tenant, table, version).await?
            && manifest.apply_nonce == nonce
        {
            return Ok(Some(version));
        }
    }
    Ok(None)
}

/// Apply `intent` to `table` as its next manifest version. See the module docs
/// for the race semantics. `Create` and `CreateOrReplace` with an empty file
/// list are refused before any store call.
///
/// `min_grace_ms` is the deployment's sweep floor ([`crate::sweep`]). Half of
/// it is the budget from this call's resolve to the end of its put; see the
/// module docs for what happens past it.
///
/// `mac_key` is the deployment's manifest MAC key, `None` in an unkeyed
/// deployment. The manifest is stamped [`PARQUET_TABLE_FORMAT_VERSION`] and
/// carries a MAC under `mac_key` once that stamp is
/// [`MAC_FORMAT_VERSION`](crate::manifest::MAC_FORMAT_VERSION) or
/// later (ADR-2430); at stamp 1 the key is unused.
pub async fn apply(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    intent: Intent,
    clock: &dyn Clock,
    min_grace_ms: u64,
    mac_key: Option<&ManifestMacKey>,
) -> Result<Outcome, WriteError> {
    apply_stamped(
        store,
        tenant,
        table,
        intent,
        clock,
        min_grace_ms,
        mac_key,
        PARQUET_TABLE_FORMAT_VERSION,
    )
    .await
}

/// [`apply`], stamping `format_version` instead of
/// [`PARQUET_TABLE_FORMAT_VERSION`], so the version 2 write path runs in tests
/// before the writer stamp moves.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_stamped(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    intent: Intent,
    clock: &dyn Clock,
    min_grace_ms: u64,
    mac_key: Option<&ManifestMacKey>,
    format_version: u32,
) -> Result<Outcome, WriteError> {
    validate_table(table)?;
    if let Intent::Create { files, .. } | Intent::CreateOrReplace { files, .. } = &intent
        && files.is_empty()
    {
        return Err(WriteError::EmptyFileList {
            table: table.to_string(),
        });
    }
    let nonce = apply_nonce();
    let budget_ms = min_grace_ms / 2;
    if budget_ms == 0 {
        return Err(WriteError::NoPutBudget { min_grace_ms });
    }
    let budget_ns = i64::try_from(budget_ms)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000_000);
    let mut attempted: Vec<u64> = Vec::new();
    for _ in 0..MAX_APPLY_ATTEMPTS {
        let resolved_at = clock.now_ns();
        let newest = resolve::newest(store, tenant, table).await?;
        if let Some(version) = own_commit(store, tenant, table, &attempted, &nonce).await? {
            return Ok(Outcome::Committed { version });
        }
        let manifest = match plan_step(table, &intent, newest.as_ref(), clock.now_ns(), &nonce)? {
            Step::Done(outcome) => return Ok(outcome),
            Step::Write(manifest) => manifest,
        };
        if manifest.version > MAX_MANIFEST_VERSION {
            return Err(WriteError::VersionAboveBound {
                table: table.to_string(),
                version: manifest.version,
                bound: MAX_MANIFEST_VERSION,
            });
        }
        let key = manifest_key(tenant, table, manifest.version)?;
        let bytes = encode_manifest_with(tenant, &manifest, format_version, mac_key)?;
        let remaining_ns = budget_ns.saturating_sub(clock.now_ns().saturating_sub(resolved_at));
        let Ok(remaining_ns) = u64::try_from(remaining_ns) else {
            continue;
        };
        if remaining_ns == 0 {
            continue;
        }
        if !attempted.contains(&manifest.version) {
            attempted.push(manifest.version);
        }
        let put = store.put(
            &key,
            Bytes::from(bytes.clone()),
            PutOptions::create_if_absent(),
        );
        let Ok(result) = tokio::time::timeout(Duration::from_nanos(remaining_ns), put).await else {
            if holds_own_write(store, &key, &bytes).await? {
                return Ok(Outcome::Committed {
                    version: manifest.version,
                });
            }
            continue;
        };
        match result {
            Ok(_) => {
                return Ok(Outcome::Committed {
                    version: manifest.version,
                });
            }
            Err(StoreError::AlreadyExists) => {
                if holds_own_write(store, &key, &bytes).await? {
                    return Ok(Outcome::Committed {
                        version: manifest.version,
                    });
                }
            }
            Err(source) => {
                // A retryable failure says nothing about whether the write
                // landed, so ask the store before reporting it.
                if source.is_retryable()
                    && let Some(version) =
                        own_commit(store, tenant, table, &attempted, &nonce).await?
                {
                    return Ok(Outcome::Committed { version });
                }
                return Err(WriteError::Store { key, source });
            }
        }
    }
    if let Some(version) = own_commit(store, tenant, table, &attempted, &nonce).await? {
        return Ok(Outcome::Committed { version });
    }
    Err(WriteError::RetriesExhausted {
        table: table.to_string(),
        attempts: MAX_APPLY_ATTEMPTS,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::clock::FixedClock;
    use prost::Message;
    use ravel_proto::parquet_table::v1 as pb;

    use crate::keys::manifest_key;
    use crate::manifest::{
        MAC_FORMAT_VERSION, MacStatus, decode_authenticated, encode_manifest, manifest_mac,
    };
    use crate::resolve::read_version;
    use crate::test_util::{
        CountingStore, TENANT_A, TEST_DEPLOYMENT_KEY, file_for, live_manifest, test_mac_key,
    };

    type Store = CountingStore<FaultStore<MemoryStore>>;

    /// A grace floor far larger than any clock movement these tests make, so
    /// only the deadline test itself crosses the resolve-to-put budget.
    const GRACE_MS: u64 = 600_000;

    /// A racing join that hangs is a lost update the assertions never reach,
    /// so every join is bounded.
    const JOIN_TIMEOUT: Duration = Duration::from_secs(10);

    fn create(if_not_exists: bool, seeds: &[u8], by: &str) -> Intent {
        Intent::Create {
            if_not_exists,
            location: "s3://customer/data/".into(),
            grant: "s3://customer/data".into(),
            files: seeds.iter().map(|&s| file_for(s)).collect(),
            options: BTreeMap::new(),
            created_by: by.into(),
            statement: format!("CREATE by {by}"),
        }
    }

    fn replace(seeds: &[u8], by: &str) -> Intent {
        Intent::CreateOrReplace {
            location: "s3://customer/data/".into(),
            grant: "s3://customer/data".into(),
            files: seeds.iter().map(|&s| file_for(s)).collect(),
            options: BTreeMap::from([("writer".into(), by.into())]),
            created_by: by.into(),
            statement: format!("REPLACE by {by}"),
        }
    }

    fn drop_table(if_exists: bool, by: &str) -> Intent {
        Intent::Drop {
            if_exists,
            created_by: by.into(),
            statement: format!("DROP by {by}"),
        }
    }

    /// The manifest `intent` should produce at `version`, with the nonce
    /// blanked: it is per-call and no caller can predict it.
    fn expected(intent: &Intent, version: u64, now_ns: i64) -> Manifest {
        match plan_step("hits", intent, None, now_ns, &[0; APPLY_NONCE_LEN]).expect("plan") {
            Step::Write(mut m) => {
                m.version = version;
                m
            }
            Step::Done(_) => panic!("intent writes nothing"),
        }
    }

    fn blank_nonce(mut m: Manifest) -> Manifest {
        assert_eq!(m.apply_nonce.len(), APPLY_NONCE_LEN);
        m.apply_nonce = vec![0; APPLY_NONCE_LEN];
        m
    }

    fn new_store(plan: FaultPlan) -> Arc<Store> {
        Arc::new(CountingStore::new(FaultStore::new(
            MemoryStore::new(),
            plan,
        )))
    }

    async fn apply_at(
        store: &dyn ObjectStoreBackend,
        table: &str,
        intent: Intent,
        now_ns: i64,
    ) -> Result<Outcome, WriteError> {
        apply(
            store,
            &TENANT_A,
            table,
            intent,
            &FixedClock::new(now_ns),
            GRACE_MS,
            None,
        )
        .await
    }

    /// Run `first` and `second` so both resolve the same newest version and
    /// both reach the write of the next version before either lands, then let
    /// `first` finish before `second`'s write reaches the store.
    async fn race_at(
        store: &Arc<Store>,
        contested_version: u64,
        first: (Intent, i64),
        second: (Intent, i64),
    ) -> (Result<Outcome, WriteError>, Result<Outcome, WriteError>) {
        let contested = manifest_key(&TENANT_A, "hits", contested_version).expect("key");
        let gate = store
            .inner
            .hold(Op::Put, Some(contested), Occurrence::Always);
        let s = Arc::clone(store);
        let a = tokio::spawn(async move { apply_at(&*s, "hits", first.0, first.1).await });
        gate.wait_until_held(1).await;
        let s = Arc::clone(store);
        let b = tokio::spawn(async move { apply_at(&*s, "hits", second.0, second.1).await });
        gate.wait_until_held(2).await;
        let held = gate.held();
        assert_eq!(held.len(), 2, "both writers must be at the contested write");
        assert!(gate.release(held[0]));
        let a = tokio::time::timeout(JOIN_TIMEOUT, a)
            .await
            .expect("writer a finished")
            .expect("join a");
        assert!(gate.release(held[1]));
        let b = tokio::time::timeout(JOIN_TIMEOUT, b)
            .await
            .expect("writer b finished")
            .expect("join b");
        (a, b)
    }

    async fn race(
        store: &Arc<Store>,
        contested_version: u64,
        first: Intent,
        second: Intent,
    ) -> (Result<Outcome, WriteError>, Result<Outcome, WriteError>) {
        race_at(store, contested_version, (first, 100), (second, 200)).await
    }

    fn assert_each_version_written_once<S>(store: &CountingStore<S>) {
        for (key, n) in store.accepted_puts() {
            assert_eq!(n, 1, "{key} was written {n} times");
        }
    }

    async fn read(store: &Store, version: u64) -> Manifest {
        read_version(store, &TENANT_A, "hits", version)
            .await
            .expect("read")
            .expect("present")
    }

    #[tokio::test]
    async fn racing_creates_resolve_by_intent_with_no_lost_update() {
        // CREATE IF NOT EXISTS vs CREATE IF NOT EXISTS: one commits, one no-ops.
        let store = new_store(FaultPlan::empty());
        let first = create(true, &[1], "a");
        let (a, b) = race(&store, 1, first.clone(), create(true, &[2], "b")).await;
        assert_eq!(a.expect("a"), Outcome::Committed { version: 1 });
        assert_eq!(b.expect("b"), Outcome::NoOp);
        assert_eq!(blank_nonce(read(&store, 1).await), expected(&first, 1, 100));
        assert_eq!(
            resolve::versions(&*store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1]
        );
        assert_each_version_written_once(&store);

        // CREATE vs CREATE: one commits, one is TableExists.
        let store = new_store(FaultPlan::empty());
        let first = create(false, &[1], "a");
        let (a, b) = race(&store, 1, first.clone(), create(false, &[2], "b")).await;
        assert_eq!(a.expect("a"), Outcome::Committed { version: 1 });
        assert!(
            matches!(b, Err(WriteError::TableExists { ref table }) if table == "hits"),
            "{b:?}"
        );
        assert_eq!(blank_nonce(read(&store, 1).await), expected(&first, 1, 100));
        assert_eq!(
            resolve::versions(&*store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1]
        );
        assert_each_version_written_once(&store);

        // CREATE OR REPLACE vs CREATE OR REPLACE on an existing table (N = 1):
        // N+1 and N+2, each with its own file list, the second writer newest.
        let store = new_store(FaultPlan::empty());
        let base = create(false, &[9], "base");
        assert_eq!(
            apply_at(&*store, "hits", base.clone(), 50)
                .await
                .expect("base"),
            Outcome::Committed { version: 1 }
        );
        let first = replace(&[1, 2], "a");
        let second = replace(&[3], "b");
        let (a, b) = race(&store, 2, first.clone(), second.clone()).await;
        assert_eq!(a.expect("a"), Outcome::Committed { version: 2 });
        assert_eq!(b.expect("b"), Outcome::Committed { version: 3 });
        assert_eq!(blank_nonce(read(&store, 1).await), expected(&base, 1, 50));
        assert_eq!(blank_nonce(read(&store, 2).await), expected(&first, 2, 100));
        assert_eq!(
            blank_nonce(read(&store, 3).await),
            expected(&second, 3, 200)
        );
        assert_eq!(
            resolve::newest(&*store, &TENANT_A, "hits")
                .await
                .expect("newest")
                .map(blank_nonce),
            Some(expected(&second, 3, 200))
        );
        assert_eq!(
            resolve::versions(&*store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1, 2, 3]
        );
        assert_each_version_written_once(&store);
    }

    #[tokio::test]
    async fn racing_identical_drops_at_one_instant_commit_once() {
        // Same statement, same caller, same clock: without the per-apply
        // nonce both writers encode the same bytes, the loser reads its
        // rival's write as its own, and one DROP is reported as two commits.
        let store = new_store(FaultPlan::empty());
        apply_at(&*store, "hits", create(false, &[1], "a"), 1)
            .await
            .expect("create");
        let (a, b) = race_at(
            &store,
            2,
            (drop_table(false, "a"), 500),
            (drop_table(false, "a"), 500),
        )
        .await;
        assert_eq!(a.expect("a"), Outcome::Committed { version: 2 });
        assert!(matches!(b, Err(WriteError::TableNotFound { .. })), "{b:?}");
        let dropped = read(&store, 2).await;
        assert!(dropped.dropped);
        assert_eq!(dropped.created_by, "a");
        assert_eq!(dropped.statement, "DROP by a");
        assert_eq!(
            resolve::versions(&*store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1, 2]
        );
        assert_each_version_written_once(&store);
    }

    #[tokio::test]
    async fn racing_drops_commit_once_and_the_loser_sees_no_table() {
        let store = new_store(FaultPlan::empty());
        apply_at(&*store, "hits", create(false, &[1], "a"), 1)
            .await
            .expect("create");
        let (a, b) = race(&store, 2, drop_table(false, "a"), drop_table(false, "b")).await;
        assert_eq!(a.expect("a"), Outcome::Committed { version: 2 });
        assert!(matches!(b, Err(WriteError::TableNotFound { .. })), "{b:?}");
        assert!(read(&store, 2).await.dropped);
        assert_each_version_written_once(&store);
    }

    #[tokio::test]
    async fn a_create_after_a_drop_continues_the_numbering() {
        let store = MemoryStore::new();
        let t = "hits";
        assert_eq!(
            apply_at(&store, t, drop_table(true, "a"), 1)
                .await
                .expect("drop"),
            Outcome::NoOp
        );
        assert!(matches!(
            apply_at(&store, t, drop_table(false, "a"), 1).await,
            Err(WriteError::TableNotFound { .. })
        ));
        assert_eq!(
            apply_at(&store, t, create(false, &[1], "a"), 1)
                .await
                .expect("create"),
            Outcome::Committed { version: 1 }
        );
        assert_eq!(
            apply_at(&store, t, drop_table(false, "a"), 2)
                .await
                .expect("drop"),
            Outcome::Committed { version: 2 }
        );
        assert_eq!(
            apply_at(&store, t, create(true, &[2], "b"), 3)
                .await
                .expect("create"),
            Outcome::Committed { version: 3 }
        );
        assert_eq!(
            apply_at(&store, t, create(true, &[4], "c"), 4)
                .await
                .expect("create"),
            Outcome::NoOp
        );
        let newest = resolve::newest(&store, &TENANT_A, t)
            .await
            .expect("newest")
            .map(blank_nonce);
        assert_eq!(newest, Some(expected(&create(true, &[2], "b"), 3, 3)));
    }

    /// Put `manifest` straight into `store`, as a write that bypassed `apply`.
    async fn put_direct(store: &dyn ObjectStoreBackend, manifest: &Manifest) {
        let key = manifest_key(&TENANT_A, &manifest.table, manifest.version).expect("key");
        let bytes = encode_manifest(&TENANT_A, manifest).expect("encode");
        store
            .put(&key, Bytes::from(bytes), PutOptions::create_if_absent())
            .await
            .expect("put");
    }

    #[tokio::test]
    async fn a_write_above_the_version_bound_is_refused_and_puts_nothing() {
        let store = InstrumentedStore::new(MemoryStore::new());
        put_direct(
            store.inner(),
            &live_manifest("hits", MAX_MANIFEST_VERSION, &[1]),
        )
        .await;
        for intent in [replace(&[2], "a"), drop_table(false, "a")] {
            let got = apply_at(&store, "hits", intent, 1).await;
            assert!(
                matches!(
                    got,
                    Err(WriteError::VersionAboveBound { ref table, version, bound })
                        if table == "hits"
                            && version == MAX_MANIFEST_VERSION + 1
                            && bound == MAX_MANIFEST_VERSION
                ),
                "{got:?}"
            );
        }
        // A statement that writes nothing is still answered as before.
        assert_eq!(
            apply_at(&store, "hits", create(true, &[3], "a"), 1)
                .await
                .expect("if not exists"),
            Outcome::NoOp
        );
        assert!(matches!(
            apply_at(&store, "hits", create(false, &[3], "a"), 1).await,
            Err(WriteError::TableExists { .. })
        ));
        assert_eq!(store.metrics().snapshot().op(StoreOp::Put).calls, 0);
        assert_eq!(
            resolve::versions(&store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![MAX_MANIFEST_VERSION]
        );
    }

    #[tokio::test]
    async fn a_write_exactly_at_the_version_bound_commits_and_the_next_is_refused() {
        let store = MemoryStore::new();
        put_direct(
            &store,
            &live_manifest("hits", MAX_MANIFEST_VERSION - 1, &[1]),
        )
        .await;
        assert_eq!(
            apply_at(&store, "hits", replace(&[2], "a"), 1)
                .await
                .expect("at the bound"),
            Outcome::Committed {
                version: MAX_MANIFEST_VERSION
            }
        );
        let got = apply_at(&store, "hits", replace(&[3], "a"), 2).await;
        assert!(
            matches!(
                got,
                Err(WriteError::VersionAboveBound { version, .. })
                    if version == MAX_MANIFEST_VERSION + 1
            ),
            "{got:?}"
        );
        assert_eq!(
            resolve::versions(&store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![MAX_MANIFEST_VERSION - 1, MAX_MANIFEST_VERSION]
        );
    }

    #[tokio::test]
    async fn a_forged_version_at_u64_max_does_not_block_the_next_statement() {
        // Without the bound the writer numbers from u64::MAX and every DDL
        // on the table fails with VersionOverflow.
        let store = MemoryStore::new();
        let t = "wedged";
        assert_eq!(
            apply_at(&store, t, create(false, &[1], "a"), 1)
                .await
                .expect("create"),
            Outcome::Committed { version: 1 }
        );
        put_direct(&store, &live_manifest(t, u64::MAX, &[9])).await;
        assert_eq!(
            apply_at(&store, t, drop_table(false, "a"), 2)
                .await
                .expect("drop"),
            Outcome::Committed { version: 2 }
        );
        assert_eq!(
            apply_at(&store, t, create(false, &[2], "b"), 3)
                .await
                .expect("create"),
            Outcome::Committed { version: 3 }
        );
        assert_eq!(
            resolve::versions(&store, &TENANT_A, t)
                .await
                .expect("versions"),
            vec![1, 2, 3, u64::MAX]
        );
    }

    #[tokio::test]
    async fn an_empty_file_list_is_refused_before_any_store_call() {
        // Every operation fails, so reaching the store at all would surface
        // as a store error instead of EmptyFileList.
        let mut plan = FaultPlan::empty();
        for op in [Op::Put, Op::Get, Op::Head, Op::List, Op::Delete] {
            plan = plan.with_rule(Rule::new(op, ScriptedFault::Permanent("reached".into())));
        }
        let store = new_store(plan);
        for intent in [create(false, &[], "a"), replace(&[], "a")] {
            let got = apply_at(&*store, "hits", intent, 1).await;
            assert!(
                matches!(got, Err(WriteError::EmptyFileList { .. })),
                "{got:?}"
            );
        }
        assert!(store.inner.counters_snapshot().values().all(|&n| n == 0));
    }

    #[tokio::test]
    async fn endless_conflicts_exhaust_the_retry_bound() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/pq/t/"),
        );
        let store = new_store(plan);
        let got = apply_at(&*store, "hits", create(false, &[1], "a"), 1).await;
        assert!(
            matches!(
                got,
                Err(WriteError::RetriesExhausted {
                    attempts: MAX_APPLY_ATTEMPTS,
                    ..
                })
            ),
            "{got:?}"
        );
        assert_eq!(
            store.inner.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::FailedConditionalWrite
            ),
            MAX_APPLY_ATTEMPTS as u64
        );
    }

    #[tokio::test]
    async fn a_retried_write_that_already_landed_is_this_writers_commit() {
        let mut store = CountingStore::new(MemoryStore::new());
        store.replay_create_if_absent = Some("/pq/t/".into());
        let got = apply_at(&store, "hits", create(false, &[1], "a"), 1).await;
        assert_eq!(got.expect("create"), Outcome::Committed { version: 1 });
        assert_eq!(
            resolve::versions(&store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1]
        );
    }

    #[tokio::test]
    async fn a_retryable_error_on_a_write_that_landed_is_this_writers_commit() {
        // DuplicateDelivery applies the put to the wrapped store and then
        // reports a retryable Transient, which is the shape of an
        // acknowledgement lost after the object was durable.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
                .with_key_contains("/pq/t/")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = new_store(plan);
        let got = apply_at(&*store, "hits", create(false, &[1], "a"), 1).await;
        assert_eq!(got.expect("create"), Outcome::Committed { version: 1 });
        assert_eq!(
            store.inner.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::DuplicateDelivery
            ),
            1
        );
        assert_eq!(
            resolve::versions(&*store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1]
        );
    }

    #[tokio::test]
    async fn a_resolve_that_ages_past_half_the_grace_floor_is_re_resolved() {
        let baseline = CountingStore::new(MemoryStore::new());
        let clock = FixedClock::new(0);
        assert_eq!(
            apply(
                &baseline,
                &TENANT_A,
                "hits",
                create(false, &[1], "a"),
                &clock,
                GRACE_MS,
                None,
            )
            .await
            .expect("create"),
            Outcome::Committed { version: 1 }
        );
        let lists_per_apply = baseline.list_count();
        assert!(lists_per_apply > 0);

        // The first resolve's LIST ages the clock to `age`, the second does
        // not, so the write lands on the first attempt when `age` leaves some
        // budget and on the second otherwise.
        for (age, attempts) in [
            (budget_ns() - 1_000_000, 1),
            (budget_ns(), 2),
            (budget_ns() + 1, 2),
        ] {
            let store = CountingStore::new(MemoryStore::new());
            let clock = FixedClock::new(0);
            store.bump_clock_on_list(clock.clone(), [age, 0]);
            assert_eq!(
                apply(
                    &store,
                    &TENANT_A,
                    "hits",
                    create(false, &[1], "a"),
                    &clock,
                    GRACE_MS,
                    None,
                )
                .await
                .expect("create"),
                Outcome::Committed { version: 1 }
            );
            assert_eq!(
                store.list_count(),
                attempts * lists_per_apply,
                "age {age} ns"
            );
            assert!(
                store
                    .listed_prefixes()
                    .iter()
                    .all(|p| p.contains("/pq/t/hits/v/")),
                "{:?}",
                store.listed_prefixes()
            );
        }
    }

    #[tokio::test]
    async fn the_apply_nonce_is_not_derivable_from_the_callers_inputs() {
        // Two processes with the same pid (pid 1 in two containers), the
        // same caller, clock tick and table, and the same count of earlier
        // applies would share any nonce computed from those inputs. Search
        // every counter value this test binary could have reached.
        let store = MemoryStore::new();
        apply_at(&store, "hits", create(false, &[1], "a"), 7)
            .await
            .expect("create");
        let nonce = read_version(&store, &TENANT_A, "hits", 1)
            .await
            .expect("read")
            .expect("present")
            .apply_nonce;
        assert_eq!(nonce.len(), APPLY_NONCE_LEN);
        for counter in 0u64..200_000 {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"a");
            hasher.update(&7i64.to_le_bytes());
            hasher.update(&std::process::id().to_le_bytes());
            hasher.update(&counter.to_le_bytes());
            hasher.update(b"hits");
            assert_ne!(
                &hasher.finalize().as_bytes()[..APPLY_NONCE_LEN],
                nonce.as_slice(),
                "nonce reproduced from its inputs at counter {counter}"
            );
        }

        let nonces: std::collections::HashSet<Vec<u8>> = (0..64).map(|_| apply_nonce()).collect();
        assert_eq!(nonces.len(), 64);
    }

    /// Half the grace floor, in nanoseconds: the whole resolve-to-put budget.
    fn budget_ns() -> i64 {
        i64::try_from(GRACE_MS / 2 * 1_000_000).expect("fits")
    }

    #[tokio::test(start_paused = true)]
    async fn a_put_that_outlives_the_budget_times_out_and_re_resolves() {
        let store = new_store(FaultPlan::empty());
        let first = manifest_key(&TENANT_A, "hits", 1).expect("key");
        let gate = store.inner.hold(Op::Put, Some(first), Occurrence::Nth(1));
        let started = tokio::time::Instant::now();
        // Without the timeout the held put never returns; the outer bound
        // turns that hang into a failure.
        let got = tokio::time::timeout(
            Duration::from_secs(3_600),
            apply_at(&*store, "hits", create(false, &[1], "a"), 1),
        )
        .await
        .expect("apply returned");
        assert_eq!(got.expect("create"), Outcome::Committed { version: 1 });
        // hygiene-allow: wall-clock -- start_paused tokio time; elapsed() is virtual and exact
        assert_eq!(
            started.elapsed(),
            Duration::from_nanos(u64::try_from(budget_ns()).expect("positive"))
        );
        assert_eq!(gate.held().len(), 1, "the first put was held");
        assert_eq!(
            resolve::versions(&*store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1]
        );
        assert_each_version_written_once(&store);
    }

    #[tokio::test(start_paused = true)]
    async fn a_timed_out_put_found_by_the_own_write_check_is_this_writers_commit() {
        let mut store = CountingStore::new(MemoryStore::new());
        store.stall_then_land_late = Some("/pq/t/".into());
        let got = tokio::time::timeout(
            Duration::from_secs(3_600),
            apply_at(&store, "hits", create(false, &[1], "a"), 1),
        )
        .await
        .expect("apply returned");
        // The own-write GET read nothing, then the stalled request landed;
        // the next attempt reads the version this call put and finds its own
        // nonce. Without that check the CREATE would see its own table and
        // report TableExists.
        assert_eq!(got.expect("create"), Outcome::Committed { version: 1 });
        assert_eq!(
            resolve::versions(&store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1]
        );
        assert_each_version_written_once(&store);
    }

    #[tokio::test]
    async fn a_grace_floor_with_no_room_for_any_put_is_refused() {
        let store = MemoryStore::new();
        for grace in [0, 1] {
            let got = apply(
                &store,
                &TENANT_A,
                "hits",
                create(false, &[1], "a"),
                &FixedClock::new(0),
                grace,
                None,
            )
            .await;
            assert!(
                matches!(got, Err(WriteError::NoPutBudget { min_grace_ms }) if min_grace_ms == grace),
                "{got:?}"
            );
        }
        assert_eq!(
            apply(
                &store,
                &TENANT_A,
                "hits",
                create(false, &[1], "a"),
                &FixedClock::new(0),
                2,
                None,
            )
            .await
            .expect("one ms of budget"),
            Outcome::Committed { version: 1 }
        );
    }

    #[tokio::test]
    async fn a_45_s_grace_floor_leaves_a_usable_put_budget() {
        // A 45 s floor halves to a 22.5 s resolve-to-put budget, which the
        // removed 30 s skew allowance used to consume entirely (every write
        // was refused with NoPutBudget). The budget is now measured end to end
        // on this call's own clock, so a put under such a floor commits.
        let store = MemoryStore::new();
        assert_eq!(
            apply(
                &store,
                &TENANT_A,
                "hits",
                create(false, &[1], "a"),
                &FixedClock::new(0),
                45_000,
                None,
            )
            .await
            .expect("45 s grace leaves a budget"),
            Outcome::Committed { version: 1 }
        );
    }

    /// Put `rival` at its own version just before the `nth` LIST, so a caller
    /// whose own write landed late finds someone else's version on top of it.
    fn supersede_on_list(store: &CountingStore<MemoryStore>, nth: usize, rival: &Manifest) {
        let key = manifest_key(&TENANT_A, &rival.table, rival.version).expect("key");
        let bytes = encode_manifest(&TENANT_A, rival).expect("encode");
        store.put_on_nth_list(nth, key, Bytes::from(bytes));
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_landing_write_another_writer_built_on_is_still_this_writers_commit() {
        // This call's put of version 1 stalls, so it times out, reads the key,
        // finds nothing, and re-resolves. The request lands in between, and
        // another writer commits version 2 on top of it before this call's
        // second LIST is answered. The newest version is then not this call's,
        // but version 1 is.
        for intent in [create(false, &[1], "a"), replace(&[1], "a")] {
            let mut store = CountingStore::new(MemoryStore::new());
            store.stall_then_land_late = Some("/pq/t/".into());
            supersede_on_list(&store, 2, &live_manifest("hits", 2, &[7]));
            let got = tokio::time::timeout(
                Duration::from_secs(3_600),
                apply_at(&store, "hits", intent, 1),
            )
            .await
            .expect("apply returned");
            assert_eq!(got.expect("apply"), Outcome::Committed { version: 1 });
            // No version 3: the intent was not applied a second time on top
            // of the rival's version 2.
            assert_eq!(
                resolve::versions(&store, &TENANT_A, "hits")
                    .await
                    .expect("versions"),
                vec![1, 2]
            );
            assert_each_version_written_once(&store);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_landing_drop_another_writer_built_on_is_not_applied_twice() {
        let mut store = CountingStore::new(MemoryStore::new());
        apply_at(&store, "hits", create(false, &[1], "a"), 1)
            .await
            .expect("create");
        // The DROP's own put of version 2 stalls and lands late; the rival's
        // version 3 is in place before the re-resolve.
        store.stall_then_land_late = Some("/pq/t/".into());
        let lists_before = store.list_count();
        supersede_on_list(&store, lists_before + 2, &live_manifest("hits", 3, &[7]));
        let got = tokio::time::timeout(
            Duration::from_secs(3_600),
            apply_at(&store, "hits", drop_table(false, "a"), 2),
        )
        .await
        .expect("apply returned");
        assert_eq!(got.expect("drop"), Outcome::Committed { version: 2 });
        assert_eq!(
            resolve::versions(&store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1, 2, 3]
        );
        assert!(
            read_version(&store, &TENANT_A, "hits", 2)
                .await
                .expect("read")
                .expect("present")
                .dropped
        );
        assert_each_version_written_once(&store);
    }

    /// The bytes stored at `table`'s version `version`.
    async fn stored(store: &MemoryStore, table: &str, version: u64) -> (String, Bytes) {
        let key = manifest_key(&TENANT_A, table, version).expect("key");
        let data = store.get(&key, GetRange::Full).await.expect("get").data;
        (key, data)
    }

    #[tokio::test]
    async fn the_version_two_write_path_macs_every_manifest_it_writes() {
        let store = MemoryStore::new();
        let key = test_mac_key();
        for (intent, version) in [
            (create(false, &[1], "a"), 1),
            (replace(&[2], "a"), 2),
            (drop_table(false, "a"), 3),
        ] {
            assert_eq!(
                apply_stamped(
                    &store,
                    &TENANT_A,
                    "hits",
                    intent,
                    &FixedClock::new(0),
                    GRACE_MS,
                    Some(&key),
                    MAC_FORMAT_VERSION,
                )
                .await
                .expect("apply"),
                Outcome::Committed { version }
            );
            let (object_key, bytes) = stored(&store, "hits", version).await;
            let (manifest, status) =
                decode_authenticated(&object_key, &bytes, &key).expect("decode");
            assert_eq!(status, MacStatus::Valid, "v{version}");
            // The stored MAC is the one `manifest_mac` computes for the same
            // manifest, so the sweep and the writer agree on the bytes.
            let body = pb::ParquetTableManifest::decode(bytes.as_ref()).expect("decode");
            assert_eq!(body.format_version, MAC_FORMAT_VERSION);
            assert_eq!(
                manifest_mac(&key, &TENANT_A, &manifest, MAC_FORMAT_VERSION)
                    .expect("mac")
                    .map(|m| m.to_vec()),
                Some(body.mac)
            );
            // A key derived from any other deployment key does not verify it.
            let other = ManifestMacKey::from_deployment_key(&[0x43; 32]);
            assert_eq!(
                decode_authenticated(&object_key, &bytes, &other)
                    .expect("decode")
                    .1,
                MacStatus::Invalid,
                "v{version}"
            );
        }
    }

    #[tokio::test]
    async fn a_version_two_manifest_written_without_the_key_does_not_verify() {
        let store = MemoryStore::new();
        apply_stamped(
            &store,
            &TENANT_A,
            "hits",
            create(false, &[1], "a"),
            &FixedClock::new(0),
            GRACE_MS,
            None,
            MAC_FORMAT_VERSION,
        )
        .await
        .expect("apply");
        let (object_key, bytes) = stored(&store, "hits", 1).await;
        assert_eq!(
            decode_authenticated(&object_key, &bytes, &test_mac_key())
                .expect("decode")
                .1,
            MacStatus::Absent
        );
    }

    /// Release A (ADR-2430 decision 1): the writer still stamps version 1 and
    /// MACs nothing, even holding the key, so a reader that predates version 2
    /// reads every manifest this build writes.
    #[tokio::test]
    async fn the_shipped_writer_stamps_version_one_without_a_mac() {
        assert_eq!(PARQUET_TABLE_FORMAT_VERSION, 1);
        let store = MemoryStore::new();
        let key = ManifestMacKey::from_deployment_key(&TEST_DEPLOYMENT_KEY);
        apply(
            &store,
            &TENANT_A,
            "hits",
            create(false, &[1], "a"),
            &FixedClock::new(0),
            GRACE_MS,
            Some(&key),
        )
        .await
        .expect("apply");
        let (object_key, bytes) = stored(&store, "hits", 1).await;
        let body = pb::ParquetTableManifest::decode(bytes.as_ref()).expect("decode");
        assert_eq!(body.format_version, 1);
        assert!(body.mac.is_empty());
        assert_eq!(
            decode_authenticated(&object_key, &bytes, &key)
                .expect("decode")
                .1,
            MacStatus::Unversioned
        );
        assert_eq!(
            bytes.as_ref(),
            encode_manifest(
                &TENANT_A,
                &read_version(&store, &TENANT_A, "hits", 1)
                    .await
                    .expect("read")
                    .expect("present")
            )
            .expect("encode")
            .as_slice()
        );
    }
}
