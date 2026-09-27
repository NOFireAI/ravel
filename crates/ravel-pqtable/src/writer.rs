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
//! Two mechanisms make a refused or failed write decidable. Every call carries
//! an apply nonce, so two callers that would otherwise encode byte-identical
//! manifests still produce different bodies and only the caller whose bytes
//! are there reads the version as its own. And the manifest a writer resolved
//! ages: `apply` re-resolves rather than putting when more than half of
//! `min_grace_ms` passed between its resolve and its put, so it never writes
//! against a view old enough for [`crate::sweep`] to have deleted what it read.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError};
use ravel_types::TenantHash;

use crate::clock::Clock;
use crate::keys::{KeyError, manifest_key};
use crate::manifest::{APPLY_NONCE_LEN, Manifest, ManifestError, ParquetFile, encode_manifest};
use crate::names::{NameError, validate_table};
use crate::resolve::{self, ResolveError};

/// How many times [`apply`] writes before giving up. A refused write that is
/// not this writer's own means another writer committed that version first,
/// so this bounds contention, not failures.
pub const MAX_APPLY_ATTEMPTS: usize = 8;

/// Distinguishes two applies started in the same process in the same clock
/// tick; the rest of the nonce's input distinguishes processes and callers.
static APPLY_COUNTER: AtomicU64 = AtomicU64::new(0);

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
    #[error("table {table:?}: gave up after {attempts} conflicting writes")]
    RetriesExhausted { table: String, attempts: usize },
    #[error("table {table:?}: version {version} has no successor")]
    VersionOverflow { table: String, version: u64 },
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

/// 16 bytes of BLAKE3 over the caller, the clock, this process and a counter.
/// Generated once per [`apply`] call and reused across its retries, so a retry
/// recognises its own earlier write and a second caller never does.
fn apply_nonce(created_by: &str, now_ns: i64, table: &str) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(created_by.as_bytes());
    hasher.update(&now_ns.to_le_bytes());
    hasher.update(&std::process::id().to_le_bytes());
    hasher.update(&APPLY_COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    hasher.update(table.as_bytes());
    hasher.finalize().as_bytes()[..APPLY_NONCE_LEN].to_vec()
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

/// Apply `intent` to `table` as its next manifest version. See the module docs
/// for the race semantics. `Create` and `CreateOrReplace` with an empty file
/// list are refused before any store call.
///
/// `min_grace_ms` is the deployment's sweep floor ([`crate::sweep`]). Half of
/// it is the budget between this call's resolve and its put; past that, the
/// call re-resolves instead of putting.
pub async fn apply(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    intent: Intent,
    clock: &dyn Clock,
    min_grace_ms: u64,
) -> Result<Outcome, WriteError> {
    validate_table(table)?;
    let created_by = match &intent {
        Intent::Create {
            files, created_by, ..
        }
        | Intent::CreateOrReplace {
            files, created_by, ..
        } => {
            if files.is_empty() {
                return Err(WriteError::EmptyFileList {
                    table: table.to_string(),
                });
            }
            created_by
        }
        Intent::Drop { created_by, .. } => created_by,
    };
    let nonce = apply_nonce(created_by, clock.now_ns(), table);
    let budget_ns = i64::try_from(min_grace_ms / 2)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000_000);
    for _ in 0..MAX_APPLY_ATTEMPTS {
        let resolved_at = clock.now_ns();
        let newest = resolve::newest(store, tenant, table).await?;
        let manifest = match plan_step(table, &intent, newest.as_ref(), clock.now_ns(), &nonce)? {
            Step::Done(outcome) => return Ok(outcome),
            Step::Write(manifest) => manifest,
        };
        let key = manifest_key(tenant, table, manifest.version)?;
        let bytes = encode_manifest(&manifest)?;
        if clock.now_ns().saturating_sub(resolved_at) > budget_ns {
            continue;
        }
        match store
            .put(
                &key,
                Bytes::from(bytes.clone()),
                PutOptions::create_if_absent(),
            )
            .await
        {
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
                if source.is_retryable() && holds_own_write(store, &key, &bytes).await? {
                    return Ok(Outcome::Committed {
                        version: manifest.version,
                    });
                }
                return Err(WriteError::Store { key, source });
            }
        }
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
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::clock::FixedClock;
    use crate::resolve::read_version;
    use crate::test_util::{CountingStore, TENANT_A, file_for};

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

    fn assert_each_version_written_once(store: &Store) {
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
            )
            .await
            .expect("create"),
            Outcome::Committed { version: 1 }
        );
        let lists_per_apply = baseline.list_count();
        assert!(lists_per_apply > 0);

        let store = CountingStore::new(MemoryStore::new());
        let clock = FixedClock::new(0);
        let budget_ns = i64::from(GRACE_MS as u32 / 2) * 1_000_000;
        // The first resolve's LIST ages the clock past the budget, the second
        // does not, so the write lands on the second attempt.
        store.bump_clock_on_list(clock.clone(), [budget_ns + 1, 0]);
        assert_eq!(
            apply(
                &store,
                &TENANT_A,
                "hits",
                create(false, &[1], "a"),
                &clock,
                GRACE_MS,
            )
            .await
            .expect("create"),
            Outcome::Committed { version: 1 }
        );
        assert_eq!(store.list_count(), 2 * lists_per_apply);
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
