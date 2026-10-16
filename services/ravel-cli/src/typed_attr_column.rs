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
//!
//! # Exact-key index fields
//!
//! `set` also carries the tenant's exact-key index declaration (ADR-2707
//! decisions 3 and 6): which declared columns the logs flush writes into the
//! RLOG KEY_IDX section, and whether `trace_id` joins them. Each is optional on
//! the command line; an omitted one keeps the stored value. Turning either on
//! writes a version-4 config record, which a reader from an older release
//! refuses, so it needs the `--readers-rolled-out` opt-in (ADR-0066 R1).
//! Turning both off needs none.

use std::path::Path;
use std::sync::Arc;

use ravel_catalog::{
    DeclaredColumnType, DeclaredTypedColumn, StorageLayoutConfigError, StorageLayoutWrite,
    TenantConfig, TenantConfigSetOutcome, TenantLifecycleState, read_config, set_tenant_config,
    validate_typed_attr_columns,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantId;

use crate::load::{ColType, Mapping, parse_mapping};

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

/// The exact-key index part of a `set`: each field `None` keeps the stored
/// value.
#[derive(Debug, Clone, Default)]
pub struct KeyIndexSpec {
    /// The declared columns to index, replacing the stored list; `Some(vec![])`
    /// turns the column index off.
    pub columns: Option<Vec<String>>,
    /// Whether the logs flush indexes `trace_id`.
    pub index_trace_id: Option<bool>,
    /// The operator's version-4 opt-in, required to turn either on.
    pub write: StorageLayoutWrite,
}

/// The flag spelling for a declared type, so `show` reads back what `set` took.
pub(crate) fn spelling(ty: DeclaredColumnType) -> &'static str {
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
    let record = read_config(store.as_ref(), &tenant_hash).await?;
    match &record {
        None => {
            println!(
                "no config record for tenant {tenant}: typed_attr_columns is unset, so this \
                 tenant's logs table uses the deployment default declaration \
                 (--typed-attr-column / --typed-attr-column-tenant)"
            );
        }
        Some((config, _version)) => match &config.typed_attr_columns {
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
                for column in columns {
                    println!("  {}:{}", column.key, spelling(column.ty));
                }
            }
        },
    }
    if let Some((config, _version)) = &record {
        print_key_index(config)?;
    }
    Ok(())
}

/// Prints the key-index columns and the `trace_id` setting.
fn print_key_index(config: &TenantConfig) -> anyhow::Result<()> {
    for line in key_index_lines(config)? {
        println!("{line}");
    }
    Ok(())
}

/// The lines [`print_key_index`] prints, as `show` and `set` report them.
fn key_index_lines(config: &TenantConfig) -> anyhow::Result<Vec<String>> {
    let columns = config.key_index_columns()?;
    let mut lines = Vec::with_capacity(columns.len() + 2);
    if columns.is_empty() {
        lines.push("key_index: none".to_string());
    } else {
        lines.push("key_index, in declaration order:".to_string());
        for column in &columns {
            lines.push(format!("  {}:{}", column.key, spelling(column.ty)));
        }
    }
    lines.push(format!("index_trace_id: {}", config.index_trace_id()));
    Ok(lines)
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
///
/// `key_index` replaces the exact-key index fields its set members name and
/// keeps the others (see [`KeyIndexSpec`]).
pub async fn set(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    specs: &[String],
    key_index: &KeyIndexSpec,
    now_ns: i64,
) -> anyhow::Result<()> {
    let mut columns = Vec::with_capacity(specs.len());
    for spec in specs {
        columns.push(parse_spec(spec)?);
    }
    set_columns(store, tenant, columns, key_index, now_ns).await
}

/// `typed-attr-column set <TENANT> --from-mapping <TOML>`: derive the
/// declaration from a `load --mapping` document and write it through the same
/// CAS whole-list replace [`set`] uses (ADR-0100 decision 2).
///
/// Every `[[attribute]]` and `[[resource_attribute]]` entry becomes a declared
/// column of the same-named type. Resource attributes come first, in file
/// order, then record attributes in file order (see
/// [`derive_columns_from_mapping`]); that order is significant (it is the
/// schema-append order) and is stable across invocations of the same mapping.
/// `f64`-typed entries are skipped with a per-key warning on stderr, since
/// `DeclaredColumnType` has no `f64` variant; the rest are written. Duplicate
/// keys across the two lists, a key repeated with two types, or a key colliding
/// with a fixed logs SQL column name are rejected by the shared
/// `validate_typed_attr_columns` gate, writing nothing.
///
/// The loader never touches tenant config: this control-plane write stays in
/// the control-plane subcommand (ADR-0100 decision 2, rejected alternative "the
/// loader pushes the declaration itself").
pub async fn set_from_mapping(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    mapping_path: &Path,
    key_index: &KeyIndexSpec,
    now_ns: i64,
) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(mapping_path).map_err(|e| {
        anyhow::anyhow!(
            "failed to read --from-mapping {}: {e}",
            mapping_path.display()
        )
    })?;
    let mapping = parse_mapping(&text)?;
    let (columns, skipped) = derive_columns_from_mapping(&mapping);
    for warning in &skipped {
        eprintln!("{warning}");
    }
    set_columns(store, tenant, columns, key_index, now_ns).await
}

/// The `DeclaredColumnType` a mapping `ColType` maps to, or `None` for `F64`
/// (which has no declared-column counterpart, ADR-0100 decision 2).
fn declared_type(col_type: ColType) -> Option<DeclaredColumnType> {
    match col_type {
        ColType::Str => Some(DeclaredColumnType::Str),
        ColType::I64 => Some(DeclaredColumnType::I64),
        ColType::Bool => Some(DeclaredColumnType::Bool),
        ColType::Bytes => Some(DeclaredColumnType::Bytes),
        ColType::F64 => None,
    }
}

/// Derive the declared-column list from a `--mapping` document (ADR-0100
/// decision 2), returning the columns and one stderr warning per skipped `f64`
/// key.
///
/// Order is `[[resource_attribute]]` entries first, in file order, then
/// `[[attribute]]` entries in file order. Declared columns read the merged
/// resource + scope + record attribute view (`ravel-sql/src/rlog_attrs.rs`), so
/// a stream-level resource key is legitimately declarable. The order is
/// significant (schema-append order) and deterministic, so re-running the same
/// mapping yields byte-identical output; nothing is re-sorted. This is a single
/// pass over the two lists (plus the O(n) `validate_typed_attr_columns` the
/// caller runs), so it holds the ~105-column target with no quadratic step.
///
/// Duplicate or conflicting keys are NOT resolved here: the returned list may
/// carry a key twice, and `validate_typed_attr_columns` (run by the caller) is
/// the single authority that rejects that, so the rule is never reimplemented.
fn derive_columns_from_mapping(mapping: &Mapping) -> (Vec<DeclaredTypedColumn>, Vec<String>) {
    let mut columns =
        Vec::with_capacity(mapping.resource_attributes.len() + mapping.attributes.len());
    let mut skipped = Vec::new();
    for attr in mapping
        .resource_attributes
        .iter()
        .chain(mapping.attributes.iter())
    {
        match declared_type(attr.value_type) {
            Some(ty) => columns.push(DeclaredTypedColumn {
                key: attr.key.clone(),
                ty,
            }),
            None => skipped.push(format!(
                "warning: mapping key {:?} has type f64, which has no declared typed-column type \
                 (ADR-0100); skipping it. It stays queryable through attrs['{}'] but gets no \
                 typed column, so a SQL predicate over it pays a per-row string cast. The rest of \
                 the mapping is still written.",
                attr.key, attr.key
            )),
        }
    }
    (columns, skipped)
}

/// The shared write core behind [`set`] and [`set_from_mapping`]: validate the
/// declaration, then perform the CAS whole-list replace (read-modify-write
/// window documented in the module docs) and print the outcome.
async fn set_columns(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    columns: Vec<DeclaredTypedColumn>,
    key_index: &KeyIndexSpec,
    now_ns: i64,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    // The same operator-input gate a durable write and the server's flags run.
    // Checked here first so the failure is reported before the read, and
    // reported as this command's own error rather than as a store-write error.
    validate_typed_attr_columns(&columns)?;

    // Carry every other field of the record through unchanged; see the module
    // docs on the read-modify-write window.
    let existing = read_config(store.as_ref(), &tenant_hash).await?;
    let mut proposed = match existing {
        Some((config, _version)) => TenantConfig {
            typed_attr_columns: Some(columns.clone()),
            ..config
        },
        None => TenantConfig {
            typed_attr_columns: Some(columns.clone()),
            ..TenantConfig::new(TenantLifecycleState::Active)
        },
    };
    apply_key_index(&mut proposed, key_index)?;

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
    print_key_index(&proposed)?;
    println!(
        "a query-serving process picks this up within its declared-column staleness horizon \
         (60s by default); no restart is needed"
    );
    Ok(())
}

/// Applies `spec` to `proposed` and checks the result against the new
/// declaration, before any write. A kept key-index column the new declaration
/// no longer carries, or no longer declares i64, str or bytes, is refused here.
fn apply_key_index(proposed: &mut TenantConfig, spec: &KeyIndexSpec) -> anyhow::Result<()> {
    let opt_in_hint = |err: StorageLayoutConfigError| match err {
        StorageLayoutConfigError::WriterCannotEmit { .. } => anyhow::anyhow!(
            "{err}; from this command, pass --readers-rolled-out to turn the key index on"
        ),
        other => other.into(),
    };
    if let Some(columns) = &spec.columns {
        proposed
            .set_key_index_columns(columns.clone(), spec.write)
            .map_err(opt_in_hint)?;
    }
    if let Some(on) = spec.index_trace_id {
        proposed
            .set_index_trace_id(on, spec.write)
            .map_err(opt_in_hint)?;
    }
    proposed.key_index_columns().map_err(|err| {
        anyhow::anyhow!(
            "{err}; the stored key_index names a column this declaration drops or retypes, so \
             pass --key-index with the columns to keep. A column kept under a new type is \
             unindexed in compacted objects whose inputs mix the two types; it does not block \
             compaction or erasure"
        )
    })?;
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

    /// The raw record's `format_version`.
    async fn record_version(store: &dyn ObjectStoreBackend, tenant: &str) -> u32 {
        use prost::Message;
        let got = store
            .get(
                &ravel_catalog::config_key(&TenantId::new(tenant).hash()),
                GetRange::Full,
            )
            .await
            .expect("the record exists");
        ravel_proto::sys::v1::TenantConfigRecord::decode(got.data)
            .expect("the record decodes")
            .format_version
    }

    fn key_index(
        columns: Option<&[&str]>,
        index_trace_id: Option<bool>,
        write: StorageLayoutWrite,
    ) -> KeyIndexSpec {
        KeyIndexSpec {
            columns: columns.map(specs),
            index_trace_id,
            write,
        }
    }

    const KEYED: [&str; 3] = ["dur:i64", "svc:str", "blob:bytes"];

    /// `--key-index` and `--index-trace-id` under `--readers-rolled-out` write
    /// a version-4 record; the key-index columns read back in declaration
    /// order, and the lines `show` and `set` print name each with its type.
    #[tokio::test]
    async fn key_index_flags_write_a_version_4_record_and_show_reports_them() {
        let store = store();
        set(
            store.clone(),
            "acme",
            &specs(&KEYED),
            &key_index(
                Some(&["blob", "dur"]),
                Some(true),
                StorageLayoutWrite::ReadersRolledOut,
            ),
            1,
        )
        .await
        .expect("an opted-in key index is written");

        let config = durable(store.as_ref(), "acme").await.expect("present");
        assert_eq!(
            config.key_index_columns().expect("valid"),
            vec![
                DeclaredTypedColumn {
                    key: "dur".into(),
                    ty: DeclaredColumnType::I64
                },
                DeclaredTypedColumn {
                    key: "blob".into(),
                    ty: DeclaredColumnType::Bytes
                },
            ]
        );
        assert!(config.index_trace_id());
        assert_eq!(record_version(store.as_ref(), "acme").await, 4);
        assert_eq!(
            key_index_lines(&config).expect("lines"),
            [
                "key_index, in declaration order:",
                "  dur:i64",
                "  blob:bytes",
                "index_trace_id: true",
            ]
        );
    }

    /// A tenant with no key index reports both settings off, and its record
    /// stays below version 4.
    #[tokio::test]
    async fn show_reports_no_key_index_and_the_record_stays_below_version_4() {
        let store = store();
        set(
            store.clone(),
            "acme",
            &specs(&KEYED),
            &KeyIndexSpec::default(),
            1,
        )
        .await
        .expect("written");
        let config = durable(store.as_ref(), "acme").await.expect("present");
        assert_eq!(
            key_index_lines(&config).expect("lines"),
            ["key_index: none", "index_trace_id: false"]
        );
        assert!(record_version(store.as_ref(), "acme").await < 4);
    }

    /// Turning either index on without `--readers-rolled-out` is refused with
    /// the flag named, and nothing is written.
    #[tokio::test]
    async fn turning_the_key_index_on_without_the_opt_in_writes_nothing() {
        let store = store();
        for spec in [
            key_index(Some(&["dur"]), None, StorageLayoutWrite::Disabled),
            key_index(None, Some(true), StorageLayoutWrite::Disabled),
        ] {
            let err = set(store.clone(), "acme", &specs(&KEYED), &spec, 1)
                .await
                .expect_err("no opt-in must be refused");
            assert!(
                err.to_string().contains("pass --readers-rolled-out"),
                "the error names the flag: {err}"
            );
            assert!(
                durable(store.as_ref(), "acme").await.is_none(),
                "a refused set wrote a record ({spec:?})"
            );
        }
    }

    /// A bool column, an undeclared column and a column named twice are each
    /// refused before any write.
    #[tokio::test]
    async fn an_ineligible_key_index_column_writes_nothing() {
        let store = store();
        for (columns, expected) in [
            (&["flag"][..], "i64, str and bytes columns only"),
            (&["nope"][..], "is not a declared typed attribute column"),
            (&["dur", "dur"][..], "more than once"),
        ] {
            let err = set(
                store.clone(),
                "acme",
                &specs(&["dur:i64", "flag:bool"]),
                &key_index(Some(columns), None, StorageLayoutWrite::ReadersRolledOut),
                1,
            )
            .await
            .expect_err("an ineligible column must be refused");
            assert!(
                err.to_string().contains(expected),
                "{columns:?}: expected {expected:?} in {err}"
            );
            assert!(durable(store.as_ref(), "acme").await.is_none());
        }
    }

    /// An omitted flag keeps the stored value; an empty key-index list (what
    /// `--no-key-index` produces) and `--index-trace-id false` turn both off
    /// without the opt-in.
    #[tokio::test]
    async fn omitted_key_index_flags_keep_the_stored_values_and_off_needs_no_opt_in() {
        let store = store();
        set(
            store.clone(),
            "acme",
            &specs(&KEYED),
            &key_index(
                Some(&["svc"]),
                Some(true),
                StorageLayoutWrite::ReadersRolledOut,
            ),
            1,
        )
        .await
        .expect("on");

        set(
            store.clone(),
            "acme",
            &specs(&KEYED),
            &KeyIndexSpec::default(),
            2,
        )
        .await
        .expect("a set without the flags");
        let kept = durable(store.as_ref(), "acme").await.expect("present");
        assert_eq!(
            key_index_lines(&kept).expect("lines"),
            [
                "key_index, in declaration order:",
                "  svc:str",
                "index_trace_id: true"
            ]
        );
        assert_eq!(record_version(store.as_ref(), "acme").await, 4);

        set(
            store.clone(),
            "acme",
            &specs(&KEYED),
            &key_index(Some(&[]), Some(false), StorageLayoutWrite::Disabled),
            3,
        )
        .await
        .expect("turning both off needs no opt-in");
        let off = durable(store.as_ref(), "acme").await.expect("present");
        assert_eq!(
            key_index_lines(&off).expect("lines"),
            ["key_index: none", "index_trace_id: false"]
        );
        assert!(record_version(store.as_ref(), "acme").await < 4);
    }

    /// A declaration that drops a kept key-index column is refused with the
    /// remedy named, and the stored record is unchanged.
    #[tokio::test]
    async fn dropping_a_kept_key_index_column_is_refused() {
        let store = store();
        set(
            store.clone(),
            "acme",
            &specs(&KEYED),
            &key_index(Some(&["dur"]), None, StorageLayoutWrite::ReadersRolledOut),
            1,
        )
        .await
        .expect("on");
        let before = durable(store.as_ref(), "acme").await.expect("present");

        let err = set(
            store.clone(),
            "acme",
            &specs(&["svc:str"]),
            &KeyIndexSpec::default(),
            2,
        )
        .await
        .expect_err("a dropped key-index column must be refused");
        assert!(
            err.to_string().contains("pass --key-index"),
            "the error names the remedy: {err}"
        );
        assert!(
            err.to_string()
                .contains("it does not block compaction or erasure"),
            "the error says what a retype costs: {err}"
        );
        assert_eq!(durable(store.as_ref(), "acme").await, Some(before));

        // The remedy works for a retype: naming the column keeps it indexed
        // under its new type.
        set(
            store.clone(),
            "acme",
            &specs(&["dur:str"]),
            &key_index(Some(&["dur"]), None, StorageLayoutWrite::ReadersRolledOut),
            3,
        )
        .await
        .expect("a retyped column named in --key-index is kept");
        let lines = key_index_lines(&durable(store.as_ref(), "acme").await.expect("present"))
            .expect("lines");
        assert!(lines.contains(&"  dur:str".to_string()), "{lines:?}");
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
            &KeyIndexSpec::default(),
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

        set(
            store.clone(),
            "acme",
            &specs(&["a:i64", "b:str"]),
            &KeyIndexSpec::default(),
            2,
        )
        .await
        .expect("first set");
        set(
            store.clone(),
            "acme",
            &specs(&["c:bool"]),
            &KeyIndexSpec::default(),
            3,
        )
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
        set(store.clone(), "acme", &[], &KeyIndexSpec::default(), 1)
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
            let err = set(store.clone(), "acme", &bad, &KeyIndexSpec::default(), 1)
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
        set(
            store.clone(),
            "acme",
            &specs(&["good:i64"]),
            &KeyIndexSpec::default(),
            2,
        )
        .await
        .expect("a valid declaration is written");
        let err = set(
            store.clone(),
            "acme",
            &specs(&["attrs:str"]),
            &KeyIndexSpec::default(),
            3,
        )
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
        set(
            inner.clone(),
            tenant,
            &specs(&["seed:i64"]),
            &KeyIndexSpec::default(),
            1,
        )
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
            async move {
                set(
                    store,
                    "acme",
                    &specs(&["a:i64"]),
                    &KeyIndexSpec::default(),
                    10,
                )
                .await
            }
        });
        let b = tokio::spawn({
            let store = gated.clone();
            async move {
                set(
                    store,
                    "acme",
                    &specs(&["b:str"]),
                    &KeyIndexSpec::default(),
                    11,
                )
                .await
            }
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

    /// Write `text` to a temp `.toml` and return the dir (kept alive) and path.
    fn mapping_file(text: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("map.toml");
        std::fs::write(&path, text).expect("write mapping");
        (dir, path)
    }

    /// `set --from-mapping` derives a declared column from BOTH a
    /// `[[resource_attribute]]` and an `[[attribute]]` entry, in resource-then-
    /// record file order, and writes them through the CAS path.
    #[tokio::test]
    async fn from_mapping_derives_record_and_resource_attributes() {
        let store = store();
        let (_dir, path) = mapping_file(
            r#"
ts_column = "ts"
ts_unit = "nanos"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "http.status_code"
column = "status"
type = "i64"
"#,
        );
        set_from_mapping(store.clone(), "acme", &path, &KeyIndexSpec::default(), 1)
            .await
            .expect("a mapping-derived declaration is written");

        assert_eq!(
            durable(store.as_ref(), "acme")
                .await
                .expect("present")
                .typed_attr_columns,
            Some(vec![
                DeclaredTypedColumn {
                    key: "service.name".to_string(),
                    ty: DeclaredColumnType::Str,
                },
                DeclaredTypedColumn {
                    key: "http.status_code".to_string(),
                    ty: DeclaredColumnType::I64,
                },
            ]),
            "resource attributes derive first, then record attributes, both in file order"
        );
    }

    /// An `f64` mapping entry has no `DeclaredColumnType`: it is skipped with a
    /// per-key warning naming the key, and every other entry is still written.
    /// Flip the `None =>` arm of `declared_type` to map `F64` onto some type
    /// (or drop the `skipped.push`) and this fails: the f64 key would appear in
    /// the durable list, or the warning would be absent.
    #[tokio::test]
    async fn from_mapping_skips_f64_with_warning_and_writes_the_rest() {
        // Unit half: the derivation skips the f64 key with a warning and keeps
        // the rest, in order.
        let mapping = parse_mapping(
            r#"
ts_column = "ts"
ts_unit = "nanos"

[[attribute]]
key = "http.status_code"
column = "status"
type = "i64"

[[attribute]]
key = "latency_ms"
column = "latency"
type = "f64"

[[attribute]]
key = "cache.hit"
column = "hit"
type = "bool"
"#,
        )
        .expect("valid mapping");
        let (columns, skipped) = derive_columns_from_mapping(&mapping);
        assert_eq!(
            columns,
            vec![
                DeclaredTypedColumn {
                    key: "http.status_code".to_string(),
                    ty: DeclaredColumnType::I64,
                },
                DeclaredTypedColumn {
                    key: "cache.hit".to_string(),
                    ty: DeclaredColumnType::Bool,
                },
            ],
            "the f64 key is dropped from the list, the rest keep their order"
        );
        assert_eq!(skipped.len(), 1, "exactly the one f64 key is skipped");
        assert!(
            skipped[0].contains("latency_ms") && skipped[0].contains("f64"),
            "the warning names the skipped key and its type: {}",
            skipped[0]
        );

        // Integration half: the durable list written by the command excludes the
        // f64 key and carries the rest.
        let store = store();
        let (_dir, path) = mapping_file(
            r#"
ts_column = "ts"
ts_unit = "nanos"

[[attribute]]
key = "http.status_code"
column = "status"
type = "i64"

[[attribute]]
key = "latency_ms"
column = "latency"
type = "f64"
"#,
        );
        set_from_mapping(store.clone(), "acme", &path, &KeyIndexSpec::default(), 1)
            .await
            .expect("the rest is still written after skipping the f64 key");
        assert_eq!(
            durable(store.as_ref(), "acme")
                .await
                .expect("present")
                .typed_attr_columns,
            Some(vec![DeclaredTypedColumn {
                key: "http.status_code".to_string(),
                ty: DeclaredColumnType::I64,
            }]),
            "the f64 key is not declared; the i64 key is"
        );
    }

    /// A key appearing twice across the merged resource+record view is rejected
    /// by the shared `validate_typed_attr_columns` gate, and nothing is written.
    #[tokio::test]
    async fn from_mapping_rejects_a_key_declared_twice() {
        let store = store();
        let (_dir, path) = mapping_file(
            r#"
ts_column = "ts"
ts_unit = "nanos"

[[resource_attribute]]
key = "dur"
column = "dur_res"
type = "i64"

[[attribute]]
key = "dur"
column = "dur_rec"
type = "i64"
"#,
        );
        let err = set_from_mapping(store.clone(), "acme", &path, &KeyIndexSpec::default(), 1)
            .await
            .expect_err("a key declared twice must be refused");
        assert!(
            err.to_string().contains("more than once"),
            "the error names the duplicate-key rule: {err}"
        );
        assert!(
            durable(store.as_ref(), "acme").await.is_none(),
            "a refused derivation writes no record"
        );
    }

    /// Sanity: the tenant hash the CLI writes under is the one a server reads,
    /// i.e. this command is tenant-hashed (it must run under the bucket's
    /// resolved scheme, unlike `gc-config`, whose object is at the bucket root).
    #[tokio::test]
    async fn the_record_is_written_under_the_tenants_own_prefix() {
        let store = store();
        set(
            store.clone(),
            "acme",
            &specs(&["dur:i64"]),
            &KeyIndexSpec::default(),
            1,
        )
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
