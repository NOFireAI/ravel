//! `ravel-cli typed-attr-column` (ADR-0090 decision 1): show and set a
//! tenant's durable declared typed attribute columns for the `logs` SQL table.
//!
//! ADR-0090 promises a per-tenant declared schema that an operator changes
//! without restarting a query process: `TenantConfig.typed_attr_columns` is the
//! durable half, and `ravel-server`'s cache-aside overlay reads it on a bounded
//! staleness horizon. Nothing wrote it. This module is that writer, the same
//! role `gc_config` plays for `sys/gc`.
//!
//! Both operations delegate entirely to the crate that owns the durable shape:
//! [`ravel_catalog::read_config`] and [`ravel_catalog::set_tenant_config`], with
//! [`ravel_catalog::validate_typed_attr_columns`] as the pre-write gate. There
//! is no CLI-side reimplementation of the validation rules or of the CAS swap,
//! so a declaration this command accepts is exactly one `ravel-server` accepts
//! from its flags, and a concurrent writer is caught as a version conflict
//! rather than silently overwritten.
//!
//! # Whole-list replacement
//!
//! `set` replaces the tenant's declaration wholesale: it is not additive and
//! has no per-key remove. `set <tenant>` with no specs writes an explicitly
//! empty declaration, which is a real state distinct from "no override" -- it
//! means "this tenant declares nothing, ignore the deployment default" (the
//! same present-but-empty-vs-absent distinction `indexed_fields` carries, and
//! the one `ravel-server`'s overlay honours). There is deliberately no way to
//! delete the field back to absent: `TenantConfigRecord` is a whole-record
//! CAS-replace shape with no field-clearing mutation, and inventing one
//! client-side would mean writing the record without going through
//! `set_tenant_config`.
//!
//! # The read-modify-write window
//!
//! `set` must read the tenant's current record to carry its other fields
//! (lifecycle state, admission caps, retention, indexed fields) through
//! unchanged, because `set_tenant_config` writes a whole record built from the
//! caller's in-memory `TenantConfig`. `set_tenant_config` then does its own
//! read to pin the `CasVersion` it swaps against, so two concurrent `set`
//! calls that both reach the swap are a caught conflict. A write landing
//! between this command's read and that internal read is NOT caught by the
//! CAS, and would lose whatever field that other writer changed: it is the
//! pre-existing hazard ADR-0090's Context section names for this record shape
//! ("a writer running older code that swaps, say, lifecycle state can silently
//! drop this ADR's field on that same write"), narrowed here to the smallest
//! window the current `ravel_catalog` API allows. Closing it would need a
//! `set_tenant_config` variant that takes the version the caller already read,
//! which `ravel-catalog` does not expose today.

use std::sync::Arc;

use ravel_catalog::{
    DeclaredColumnType, DeclaredTypedColumn, TenantConfig, TenantConfigSetOutcome,
    TenantLifecycleState, read_config, set_tenant_config, validate_typed_attr_columns,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantId;

/// Parse one `KEY:TYPE` declaration spec, the same syntax `ravel-server`'s
/// `--typed-attr-column` flag takes. Split on the LAST `:`, so an attribute key
/// may itself contain a colon. An unknown type spelling names the four
/// accepted ones rather than silently dropping a declaration.
fn parse_spec(spec: &str) -> anyhow::Result<DeclaredTypedColumn> {
    let spec = spec.trim();
    let (key, ty) = spec
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid declaration '{spec}', expected KEY:TYPE"))?;
    let key = key.trim();
    if key.is_empty() {
        anyhow::bail!(
            "invalid declaration '{spec}': the attribute key is empty, expected KEY:TYPE"
        );
    }
    let ty = match ty.trim().to_ascii_lowercase().as_str() {
        "str" => DeclaredColumnType::Str,
        "i64" => DeclaredColumnType::I64,
        "bool" => DeclaredColumnType::Bool,
        "bytes" => DeclaredColumnType::Bytes,
        other => anyhow::bail!(
            "invalid declaration '{spec}': unknown declared type '{other}', expected one of \
             str, i64, bool, bytes (f64, date, and timestamp are deferred by ADR-0090)"
        ),
    };
    Ok(DeclaredTypedColumn {
        key: key.to_string(),
        ty,
    })
}

/// The flag spelling for a declared type, so `show` reads back what `set` took.
fn spelling(ty: DeclaredColumnType) -> &'static str {
    match ty {
        DeclaredColumnType::Str => "str",
        DeclaredColumnType::I64 => "i64",
        DeclaredColumnType::Bool => "bool",
        DeclaredColumnType::Bytes => "bytes",
    }
}

/// `typed-attr-column show <TENANT>`: print the tenant's durable declaration.
///
/// Three states are reported distinctly, because they mean three different
/// things to a query: no config record at all, a record with no declaration
/// (both leave the deployment default in force), and a present declaration
/// (which replaces the default, including when it is explicitly empty).
pub async fn show(store: Arc<dyn ObjectStoreBackend>, tenant: &str) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    match read_config(store.as_ref(), &tenant_hash).await? {
        None => {
            println!(
                "no config record for tenant {tenant}: typed_attr_columns is unset, so this \
                 tenant's logs table uses the deployment default declaration \
                 (--typed-attr-column / --typed-attr-column-tenant)"
            );
        }
        Some((config, _version)) => match config.typed_attr_columns {
            None => {
                println!(
                    "tenant {tenant} has a config record but no typed_attr_columns override: \
                     the deployment default declaration applies"
                );
            }
            Some(columns) if columns.is_empty() => {
                println!(
                    "tenant {tenant} declares no typed attribute columns (an explicit empty \
                     override: the deployment default does NOT apply)"
                );
            }
            Some(columns) => {
                println!(
                    "tenant {tenant} declares {} typed attribute column(s), in schema-append \
                     order:",
                    columns.len()
                );
                for column in &columns {
                    println!("  {}:{}", column.key, spelling(column.ty));
                }
            }
        },
    }
    Ok(())
}

/// `typed-attr-column set <TENANT> [KEY:TYPE ...]`: replace the tenant's
/// durable declaration wholesale.
///
/// Validates the whole declaration before any write, so a violation (an empty
/// key, a duplicate key, the same key with two types, or a collision with one
/// of the nine fixed logs SQL column names) refuses and writes nothing. The
/// swap is `set_tenant_config`'s CAS-versioned whole-record replace, so a
/// concurrent `set` is reported as a conflict for the operator to re-run rather
/// than silently clobbering the other write.
///
/// Declaration order is preserved verbatim: it is the order the columns are
/// appended to the `logs` schema (ADR-0090 decision 3).
pub async fn set(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    specs: &[String],
    now_ns: i64,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let mut columns = Vec::with_capacity(specs.len());
    for spec in specs {
        columns.push(parse_spec(spec)?);
    }
    // The same operator-input gate a durable write and the server's flags run.
    // Checked here first so the failure is reported before the read, and
    // reported as this command's own error rather than as a store-write error.
    validate_typed_attr_columns(&columns)?;

    // Carry every other field of the record through unchanged; see the module
    // docs on the read-modify-write window.
    let existing = read_config(store.as_ref(), &tenant_hash).await?;
    let proposed = match existing {
        Some((config, _version)) => TenantConfig {
            typed_attr_columns: Some(columns.clone()),
            ..config
        },
        None => TenantConfig {
            typed_attr_columns: Some(columns.clone()),
            ..TenantConfig::new(TenantLifecycleState::Active)
        },
    };

    match set_tenant_config(store.as_ref(), &tenant_hash, &proposed, now_ns).await? {
        TenantConfigSetOutcome::Created => {
            println!(
                "created the config record for tenant {tenant} (it had none), with \
                 lifecycle_state=active and no other override"
            );
        }
        TenantConfigSetOutcome::Updated => {
            println!(
                "updated tenant {tenant}'s config record (swapped in place with CasVersion); \
                 every other field carried through unchanged"
            );
        }
    }
    if columns.is_empty() {
        println!(
            "typed_attr_columns: [] (an explicit empty declaration: this tenant declares \
             nothing and does not inherit the deployment default)"
        );
    } else {
        println!("typed_attr_columns, in schema-append order:");
        for column in &columns {
            println!("  {}:{}", column.key, spelling(column.ty));
        }
    }
    println!(
        "a query-serving process picks this up within its declared-column staleness horizon \
         (60s by default); no restart is needed"
    );
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
        PageToken, PutOptions, PutOutcome, StoreError,
    };
    use ravel_types::TenantHash;

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    async fn durable(store: &dyn ObjectStoreBackend, tenant: &str) -> Option<TenantConfig> {
        read_config(store, &TenantId::new(tenant).hash())
            .await
            .expect("read config")
            .map(|(c, _v)| c)
    }

    fn specs(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_string()).collect()
    }

    /// `set` then `show` round-trips exactly what was set, in order, and `show`
    /// on an untouched tenant reports it unset.
    #[tokio::test]
    async fn set_then_show_round_trips_the_declaration() {
        let store = store();
        show(store.clone(), "acme")
            .await
            .expect("show on an unset tenant is not an error");

        set(
            store.clone(),
            "acme",
            &specs(&["http.duration_ms:i64", "cache.hit:BOOL", "payload:bytes"]),
            1_000,
        )
        .await
        .expect("a valid declaration is written");

        let config = durable(store.as_ref(), "acme")
            .await
            .expect("the record now exists");
        assert_eq!(
            config.typed_attr_columns,
            Some(vec![
                DeclaredTypedColumn {
                    key: "http.duration_ms".to_string(),
                    ty: DeclaredColumnType::I64,
                },
                DeclaredTypedColumn {
                    key: "cache.hit".to_string(),
                    ty: DeclaredColumnType::Bool,
                },
                DeclaredTypedColumn {
                    key: "payload".to_string(),
                    ty: DeclaredColumnType::Bytes,
                },
            ]),
            "the declaration round-trips verbatim, in order, with a \
             case-insensitive type spelling"
        );
        show(store, "acme").await.expect("show prints it back");
    }

    /// `set` replaces the whole list rather than appending to it, and carries
    /// every other field of the record through unchanged.
    #[tokio::test]
    async fn set_replaces_the_list_and_preserves_other_fields() {
        let store = store();
        let tenant_hash = TenantId::new("acme").hash();
        // A record with unrelated overrides already set.
        set_tenant_config(
            store.as_ref(),
            &tenant_hash,
            &TenantConfig {
                max_active_series: Some(4_096),
                retention_ns: Some(1_234),
                indexed_fields: Some(vec!["service.name".to_string()]),
                ..TenantConfig::new(TenantLifecycleState::Suspended)
            },
            1,
        )
        .await
        .expect("seed the record");

        set(store.clone(), "acme", &specs(&["a:i64", "b:str"]), 2)
            .await
            .expect("first set");
        set(store.clone(), "acme", &specs(&["c:bool"]), 3)
            .await
            .expect("second set replaces the first");

        let config = durable(store.as_ref(), "acme").await.expect("present");
        assert_eq!(
            config
                .typed_attr_columns
                .as_ref()
                .map(|c| c.iter().map(|col| col.key.as_str()).collect::<Vec<_>>()),
            Some(vec!["c"]),
            "the list is replaced wholesale, never appended to"
        );
        assert_eq!(config.lifecycle_state, TenantLifecycleState::Suspended);
        assert_eq!(config.max_active_series, Some(4_096));
        assert_eq!(config.retention_ns, Some(1_234));
        assert_eq!(
            config.indexed_fields,
            Some(vec!["service.name".to_string()]),
            "every other override is carried through unchanged"
        );
    }

    /// An explicitly empty `set` is a real state, distinct from "unset".
    #[tokio::test]
    async fn set_with_no_specs_writes_an_explicit_empty_declaration() {
        let store = store();
        set(store.clone(), "acme", &[], 1)
            .await
            .expect("an empty declaration is valid");
        assert_eq!(
            durable(store.as_ref(), "acme")
                .await
                .expect("present")
                .typed_attr_columns,
            Some(Vec::new()),
            "an empty set is Some([]) (declare nothing), never None (no override)"
        );
    }

    /// A validation failure writes NOTHING: the prior state (or absence)
    /// survives, and `show` afterwards still reports it. Every rule
    /// `validate_typed_attr_columns` enforces is exercised, plus the two
    /// syntax failures this command parses.
    #[tokio::test]
    async fn a_validation_failure_writes_nothing() {
        let store = store();

        // Against a tenant with no record at all: still no record afterwards.
        for bad in [
            specs(&["dur:i64", "dur:i64"]), // duplicate key
            specs(&["dur:i64", "dur:str"]), // conflicting types
            specs(&["body:str"]),           // fixed-column collision
            specs(&[":i64"]),               // empty key
            specs(&["dur"]),                // no type
            specs(&["dur:f64"]),            // deferred type
        ] {
            let err = set(store.clone(), "acme", &bad, 1)
                .await
                .expect_err("an invalid declaration must be refused");
            assert!(
                durable(store.as_ref(), "acme").await.is_none(),
                "a refused set wrote a record anyway ({bad:?}, error: {err})"
            );
        }
        show(store.clone(), "acme")
            .await
            .expect("show still reports the tenant unset");

        // Against a tenant with a good declaration: it survives untouched.
        set(store.clone(), "acme", &specs(&["good:i64"]), 2)
            .await
            .expect("a valid declaration is written");
        let err = set(store.clone(), "acme", &specs(&["attrs:str"]), 3)
            .await
            .expect_err("a fixed-column collision must be refused");
        assert!(
            err.to_string().contains("fixed logs SQL column"),
            "the error names the rule: {err}"
        );
        assert_eq!(
            durable(store.as_ref(), "acme")
                .await
                .expect("present")
                .typed_attr_columns
                .map(|c| c.len()),
            Some(1),
            "the prior declaration survives a refused set"
        );
    }

    /// Two writers racing from the same starting version: one wins, the other
    /// gets a CAS conflict rather than silently clobbering it.
    ///
    /// The interleave is forced rather than hoped for: [`GatedStore`] holds
    /// every config-key PUT at a barrier until both writers have arrived, so
    /// both `set_tenant_config` calls have already read (and pinned) the same
    /// `CasVersion` before either swap lands. Without the gate, `MemoryStore`'s
    /// operations complete without yielding and the two futures would simply
    /// run one after the other, each reading the other's fresh version.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_concurrent_set_surfaces_the_cas_conflict() {
        let inner = store();
        let tenant = "acme";
        // Seed a record so both writers read a real version to swap against.
        set(inner.clone(), tenant, &specs(&["seed:i64"]), 1)
            .await
            .expect("seed");

        // One config PUT per `set`, two writers: two arrivals at the gate,
        // both after their reads.
        let gated: Arc<dyn ObjectStoreBackend> = Arc::new(GatedStore {
            inner: inner.clone(),
            gate: Arc::new(tokio::sync::Barrier::new(2)),
        });

        let a = tokio::spawn({
            let store = gated.clone();
            async move { set(store, "acme", &specs(&["a:i64"]), 10).await }
        });
        let b = tokio::spawn({
            let store = gated.clone();
            async move { set(store, "acme", &specs(&["b:str"]), 11).await }
        });
        let (ra, rb) = (a.await.expect("join a"), b.await.expect("join b"));

        let outcomes = [&ra, &rb];
        let winners = outcomes.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            winners, 1,
            "exactly one concurrent set must win: {ra:?} / {rb:?}"
        );
        let loser = outcomes
            .iter()
            .find(|r| r.is_err())
            .expect("one writer lost");
        let err = loser.as_ref().expect_err("the loser errored").to_string();
        assert!(
            err.contains("concurrent write") || err.contains("CAS"),
            "the loser must report the CAS conflict, not something else: {err}"
        );

        // And the winner's declaration is what is durable: no interleaved
        // half-write, and not the loser's list.
        let keys: Vec<String> = durable(inner.as_ref(), tenant)
            .await
            .expect("present")
            .typed_attr_columns
            .expect("declared")
            .into_iter()
            .map(|c| c.key)
            .collect();
        assert!(
            keys == vec!["a".to_string()] || keys == vec!["b".to_string()],
            "the durable record holds exactly one writer's declaration: {keys:?}"
        );
    }

    /// A store wrapper that makes every config-key PUT wait on a barrier, so
    /// two concurrent writers provably both read (and pin a version) before
    /// either swap lands. Every other operation passes straight through.
    struct GatedStore {
        inner: Arc<dyn ObjectStoreBackend>,
        gate: Arc<tokio::sync::Barrier>,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for GatedStore {
        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            if key.ends_with("/config") {
                self.gate.wait().await;
            }
            self.inner.put(key, data, opts).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
            self.inner.put_multipart(key).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    /// Sanity: the tenant hash the CLI writes under is the one a server reads,
    /// i.e. this command is tenant-hashed (it must run under the bucket's
    /// resolved scheme, unlike `gc-config`, whose object is at the bucket root).
    #[tokio::test]
    async fn the_record_is_written_under_the_tenants_own_prefix() {
        let store = store();
        set(store.clone(), "acme", &specs(&["dur:i64"]), 1)
            .await
            .expect("set");
        let hash: TenantHash = TenantId::new("acme").hash();
        assert!(
            read_config(store.as_ref(), &hash)
                .await
                .expect("read")
                .is_some(),
            "written under this tenant's config key"
        );
        assert!(
            read_config(store.as_ref(), &TenantId::new("globex").hash())
                .await
                .expect("read")
                .is_none(),
            "and not under any other tenant's"
        );
    }
}
