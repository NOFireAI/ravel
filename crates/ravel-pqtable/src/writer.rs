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

use std::collections::BTreeMap;

use bytes::Bytes;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError};
use ravel_types::TenantHash;

use crate::keys::{KeyError, manifest_key};
use crate::manifest::{Manifest, ManifestError, ParquetFile, encode_manifest};
use crate::names::{NameError, validate_dataset, validate_table};
use crate::resolve::{self, ResolveError};

/// How many times [`apply`] writes before giving up. A refused write that is
/// not this writer's own means another writer committed that version first,
/// so this bounds contention, not failures.
pub const MAX_APPLY_ATTEMPTS: usize = 8;

/// A parsed DDL statement, applied by [`apply`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Intent {
    /// `CREATE EXTERNAL TABLE [IF NOT EXISTS]`.
    Create {
        if_not_exists: bool,
        dataset: String,
        files: Vec<ParquetFile>,
        options: BTreeMap<String, String>,
        created_by: String,
        statement: String,
    },
    /// `CREATE OR REPLACE EXTERNAL TABLE`.
    CreateOrReplace {
        dataset: String,
        files: Vec<ParquetFile>,
        options: BTreeMap<String, String>,
        created_by: String,
        statement: String,
    },
    /// `DROP TABLE [IF EXISTS]`.
    Drop { if_exists: bool },
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
    /// A store failure on the manifest write. For a retryable error the write
    /// may or may not have landed; re-resolve before deciding what happened.
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

fn plan_step(
    table: &str,
    intent: &Intent,
    newest: Option<&Manifest>,
    now_ns: i64,
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
    let live = |dataset: &str,
                files: &[ParquetFile],
                options: &BTreeMap<String, String>,
                created_by: &str,
                statement: &str| {
        Step::Write(Manifest {
            table: table.to_string(),
            version: next,
            dropped: false,
            dataset: dataset.to_string(),
            files: files.to_vec(),
            options: options.clone(),
            created_by: created_by.to_string(),
            created_unix_ns: now_ns,
            statement: statement.to_string(),
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
            dataset,
            files,
            options,
            created_by,
            statement,
            ..
        }
        | Intent::CreateOrReplace {
            dataset,
            files,
            options,
            created_by,
            statement,
        } => live(dataset, files, options, created_by, statement),
        Intent::Drop { if_exists: true } if !exists => Step::Done(Outcome::NoOp),
        Intent::Drop { .. } if !exists => {
            return Err(WriteError::TableNotFound {
                table: table.to_string(),
            });
        }
        Intent::Drop { .. } => Step::Write(Manifest {
            table: table.to_string(),
            version: next,
            dropped: true,
            dataset: String::new(),
            files: Vec::new(),
            options: BTreeMap::new(),
            created_by: String::new(),
            created_unix_ns: now_ns,
            statement: String::new(),
        }),
    })
}

/// True when `key` holds exactly `bytes`: a refused CreateIfAbsent whose
/// earlier attempt landed (a client retry after a lost acknowledgement).
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

/// Apply `intent` to `table` as its next manifest version. See the module
/// docs for the race semantics. `Create` and `CreateOrReplace` with an empty
/// file list are refused before any store call.
pub async fn apply(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    intent: Intent,
    now_ns: i64,
) -> Result<Outcome, WriteError> {
    validate_table(table)?;
    match &intent {
        Intent::Create { dataset, files, .. } | Intent::CreateOrReplace { dataset, files, .. } => {
            validate_dataset(dataset)?;
            if files.is_empty() {
                return Err(WriteError::EmptyFileList {
                    table: table.to_string(),
                });
            }
        }
        Intent::Drop { .. } => {}
    }
    for _ in 0..MAX_APPLY_ATTEMPTS {
        let newest = resolve::newest(store, tenant, table).await?;
        let manifest = match plan_step(table, &intent, newest.as_ref(), now_ns)? {
            Step::Done(outcome) => return Ok(outcome),
            Step::Write(manifest) => manifest,
        };
        let key = manifest_key(tenant, table, manifest.version)?;
        let bytes = encode_manifest(tenant, &manifest)?;
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
            Err(source) => return Err(WriteError::Store { key, source }),
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

    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::resolve::read_version;
    use crate::test_util::{CountingStore, TENANT_A, file_for};

    type Store = CountingStore<FaultStore<MemoryStore>>;

    fn create(if_not_exists: bool, seeds: &[u8], by: &str) -> Intent {
        Intent::Create {
            if_not_exists,
            dataset: "hits".into(),
            files: seeds
                .iter()
                .map(|&s| file_for(&TENANT_A, "hits", s))
                .collect(),
            options: BTreeMap::new(),
            created_by: by.into(),
            statement: format!("CREATE by {by}"),
        }
    }

    fn replace(seeds: &[u8], by: &str) -> Intent {
        Intent::CreateOrReplace {
            dataset: "hits".into(),
            files: seeds
                .iter()
                .map(|&s| file_for(&TENANT_A, "hits", s))
                .collect(),
            options: BTreeMap::from([("writer".into(), by.into())]),
            created_by: by.into(),
            statement: format!("REPLACE by {by}"),
        }
    }

    /// The manifest `intent` should produce at `version`.
    fn expected(intent: &Intent, version: u64, now_ns: i64) -> Manifest {
        match plan_step("hits", intent, None, now_ns).expect("plan") {
            Step::Write(mut m) => {
                m.version = version;
                m
            }
            Step::Done(_) => panic!("intent writes nothing"),
        }
    }

    fn new_store(plan: FaultPlan) -> Arc<Store> {
        Arc::new(CountingStore::new(FaultStore::new(
            MemoryStore::new(),
            plan,
        )))
    }

    /// Run `first` and `second` so both resolve the same newest version and
    /// both reach the write of the next version before either lands, then let
    /// `first` finish before `second`'s write reaches the store.
    async fn race(
        store: &Arc<Store>,
        contested_version: u64,
        first: Intent,
        second: Intent,
    ) -> (Result<Outcome, WriteError>, Result<Outcome, WriteError>) {
        let contested = manifest_key(&TENANT_A, "hits", contested_version).expect("key");
        let gate = store
            .inner
            .hold(Op::Put, Some(contested), Occurrence::Always);
        let s = Arc::clone(store);
        let a = tokio::spawn(async move { apply(&*s, &TENANT_A, "hits", first, 100).await });
        gate.wait_until_held(1).await;
        let s = Arc::clone(store);
        let b = tokio::spawn(async move { apply(&*s, &TENANT_A, "hits", second, 200).await });
        gate.wait_until_held(2).await;
        let held = gate.held();
        assert_eq!(held.len(), 2, "both writers must be at the contested write");
        assert!(gate.release(held[0]));
        let a = a.await.expect("join a");
        assert!(gate.release(held[1]));
        let b = b.await.expect("join b");
        (a, b)
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
        assert_eq!(read(&store, 1).await, expected(&first, 1, 100));
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
        assert_eq!(read(&store, 1).await, expected(&first, 1, 100));
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
            apply(&*store, &TENANT_A, "hits", base.clone(), 50)
                .await
                .expect("base"),
            Outcome::Committed { version: 1 }
        );
        let first = replace(&[1, 2], "a");
        let second = replace(&[3], "b");
        let (a, b) = race(&store, 2, first.clone(), second.clone()).await;
        assert_eq!(a.expect("a"), Outcome::Committed { version: 2 });
        assert_eq!(b.expect("b"), Outcome::Committed { version: 3 });
        assert_eq!(read(&store, 1).await, expected(&base, 1, 50));
        assert_eq!(read(&store, 2).await, expected(&first, 2, 100));
        assert_eq!(read(&store, 3).await, expected(&second, 3, 200));
        assert_eq!(
            resolve::newest(&*store, &TENANT_A, "hits")
                .await
                .expect("newest"),
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
    async fn racing_drops_commit_once_and_the_loser_sees_no_table() {
        let store = new_store(FaultPlan::empty());
        apply(&*store, &TENANT_A, "hits", create(false, &[1], "a"), 1)
            .await
            .expect("create");
        let (a, b) = race(
            &store,
            2,
            Intent::Drop { if_exists: false },
            Intent::Drop { if_exists: false },
        )
        .await;
        assert_eq!(a.expect("a"), Outcome::Committed { version: 2 });
        assert!(matches!(b, Err(WriteError::TableNotFound { .. })), "{b:?}");
        assert!(read(&store, 2).await.dropped);
        assert_each_version_written_once(&store);
    }

    #[tokio::test]
    async fn a_create_after_a_drop_continues_the_numbering() {
        let store = MemoryStore::new();
        let t = "hits";
        let c = |seed| create(false, &[seed], "a");
        assert_eq!(
            apply(&store, &TENANT_A, t, Intent::Drop { if_exists: true }, 1)
                .await
                .expect("drop"),
            Outcome::NoOp
        );
        assert!(matches!(
            apply(&store, &TENANT_A, t, Intent::Drop { if_exists: false }, 1).await,
            Err(WriteError::TableNotFound { .. })
        ));
        assert_eq!(
            apply(&store, &TENANT_A, t, c(1), 1).await.expect("create"),
            Outcome::Committed { version: 1 }
        );
        assert_eq!(
            apply(&store, &TENANT_A, t, Intent::Drop { if_exists: false }, 2)
                .await
                .expect("drop"),
            Outcome::Committed { version: 2 }
        );
        assert_eq!(
            apply(&store, &TENANT_A, t, create(true, &[2], "b"), 3)
                .await
                .expect("create"),
            Outcome::Committed { version: 3 }
        );
        assert_eq!(
            apply(&store, &TENANT_A, t, create(true, &[4], "c"), 4)
                .await
                .expect("create"),
            Outcome::NoOp
        );
        let newest = resolve::newest(&store, &TENANT_A, t).await.expect("newest");
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
            let got = apply(&*store, &TENANT_A, "hits", intent, 1).await;
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
        let got = apply(&*store, &TENANT_A, "hits", create(false, &[1], "a"), 1).await;
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
        let got = apply(&store, &TENANT_A, "hits", create(false, &[1], "a"), 1).await;
        assert_eq!(got.expect("create"), Outcome::Committed { version: 1 });
        assert_eq!(
            resolve::versions(&store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1]
        );
    }
}
