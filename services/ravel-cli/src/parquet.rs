//! `ravel-cli parquet` (ADR-2040): inspect a tenant's Parquet table manifests,
//! delete the manifest versions a sweep has made superseded, and remove
//! forged versions: those above the manifest version bound, keys naming no
//! version, and one version an operator names.
//!
//! Every subcommand delegates to [`ravel_pqtable`]: `ls` reads through
//! [`ravel_pqtable::resolve`], `sweep` plans and executes through
//! [`ravel_pqtable::sweep`], which owns the age rule and the store-clock skew
//! margin, and `repair` lists, flags and deletes through
//! [`ravel_pqtable::repair`]. Nothing about the key layout, the grace
//! arithmetic or the version bound is restated here.
//!
//! `repair` runs under the Maintain credential, which may list and delete
//! manifest versions but not read them. Which versions it deletes is decided
//! from the listing alone; the writer and statement of each version are
//! printed when the credential can read them and reported unreadable
//! otherwise.
//!
//! # Where the sweep's minimum grace comes from
//!
//! [`ravel_pqtable::sweep::plan`] refuses a grace below the deployment's
//! minimum, which ADR-0050 section 4 makes a durable, deployment-wide value:
//! `sys/gc`'s `max_query_duration_ns`. A query process's own engine deadline
//! (ravel-server's `--gc-max-query-duration`) is validated at startup to be
//! `<=` that stored value (`ravel_maintain::validate_query_deadline`), so the
//! stored value bounds every deadline any query in this deployment can run
//! under. This command reads it with [`read_gc_config`] rather than restating
//! a default: a bucket with no `sys/gc` has never been started by a server, so
//! there is no deployment minimum to read and the sweep refuses instead of
//! inventing one.
//!
//! What the floor protects is a writer's resolve-to-put window, not a running
//! query, which reads a table's manifest once, when it resolves.
//! [`ravel_pqtable::writer::apply`] finishes its put within half of the
//! `min_grace_ms` its caller passes, which must be no larger than this stored
//! floor, and a sweep deletes a version only once the version
//! after it is older than the grace. Every version committed after a writer's
//! resolve is younger than that, so no sweep frees the version key the
//! writer's create-if-absent put targets while the writer is in flight. The
//! stored value is at least every process's `--gc-max-query-duration`, the
//! floor ADR-2040 names, so taking it is the stricter choice.

use std::sync::Arc;

use ravel_maintain::read_gc_config;
use ravel_object_store::ObjectStoreBackend;
use ravel_pqtable::keys::MAX_MANIFEST_VERSION;
use ravel_pqtable::manifest::{Manifest, ParquetFile};
use ravel_pqtable::repair::{self, Description, ListedVersion};
use ravel_pqtable::{resolve, sweep};
use ravel_types::TenantId;

/// Parse a humantime `--grace` value into milliseconds, the unit
/// [`sweep::plan`] takes.
pub fn parse_grace_ms(s: &str) -> anyhow::Result<u64> {
    let dur =
        humantime::parse_duration(s).map_err(|e| anyhow::anyhow!("invalid --grace '{s}': {e}"))?;
    u64::try_from(dur.as_millis()).map_err(|_| anyhow::anyhow!("--grace '{s}' is too large"))
}

/// One line per field of one manifest version, including every field of every
/// file it names.
///
/// Both destructurings are exhaustive on purpose: a field added to [`Manifest`]
/// or to [`ParquetFile`] stops this compiling rather than silently dropping out
/// of `ls`.
fn manifest_lines(manifest: &Manifest) -> Vec<String> {
    let Manifest {
        table,
        version,
        dropped,
        location,
        grant,
        files,
        options,
        created_by,
        created_unix_ns,
        statement,
        apply_nonce,
    } = manifest;
    let mut out = vec![
        format!("  version: {version}"),
        format!("    table: {table}"),
        format!("    dropped: {dropped}"),
        format!("    location: {location}"),
        format!("    grant: {grant}"),
        format!("    created_by: {created_by}"),
        format!("    created_unix_ns: {created_unix_ns}"),
        format!("    statement: {statement}"),
        format!("    apply_nonce: {}", hex::encode(apply_nonce)),
        format!("    options: {}", options.len()),
    ];
    out.extend(
        options
            .iter()
            .map(|(key, value)| format!("      {key}: {value}")),
    );
    out.push(format!("    files: {}", files.len()));
    for (index, file) in files.iter().enumerate() {
        let ParquetFile {
            profile,
            bucket,
            key,
            size,
            etag,
            version,
            row_count,
            footer_len,
        } = file;
        // The key is the bytes the listing returned. A manifest that decoded
        // holds only addressable keys, but printing is lossy rather than
        // fallible so an operator can still read a record they are diagnosing.
        out.extend([
            format!("      [{index}] key: {}", String::from_utf8_lossy(key)),
            format!("           profile: {profile}"),
            format!("           bucket: {bucket}"),
            format!("           size: {size}"),
            format!("           etag: {etag}"),
            format!("           version: {version}"),
            format!("           row_count: {row_count}"),
            format!("           footer_len: {footer_len}"),
        ]);
    }
    out
}

fn print_manifest(manifest: &Manifest) {
    for line in manifest_lines(manifest) {
        println!("{line}");
    }
}

/// `parquet ls`: every table's newest manifest version at or below the
/// manifest version bound, or every retained version of one named table.
pub async fn ls(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    table: Option<&str>,
) -> anyhow::Result<()> {
    let hash = TenantId::new(tenant).hash();
    let all = resolve::table_listings(store.as_ref(), &hash).await?;
    let selected: Vec<(&String, &resolve::TableListing)> = match table {
        Some(name) => all.iter().filter(|(t, _)| t.as_str() == name).collect(),
        None => all.iter().collect(),
    };
    if selected.is_empty() {
        match table {
            Some(name) => println!("tenant {tenant} has no Parquet table named {name}"),
            None => println!("tenant {tenant} has no Parquet tables"),
        }
        return Ok(());
    }
    for (name, listing) in selected {
        // With `--table`, every version at or below the version bound still
        // under the tenant's prefix; a version a sweep has already deleted is
        // not retained and cannot be printed. Without it, only the newest.
        // Neither reads a version above the bound or a key naming no version,
        // as no resolver does.
        let (bounded, above) = resolve::split_at_bound(&listing.versions);
        let wanted: &[u64] = if table.is_some() {
            bounded
        } else {
            bounded.last().map(std::slice::from_ref).unwrap_or(&[])
        };
        println!("table: {name} ({} retained versions)", bounded.len());
        let skipped = above.len() + listing.invalid_keys.len();
        if skipped > 0 {
            println!(
                "  {skipped} version key(s) above the version bound {MAX_MANIFEST_VERSION} or \
                 naming no version, skipped by every reader: run \
                 `ravel-cli parquet repair --tenant {tenant} --table {name}`"
            );
        }
        for &version in wanted {
            match resolve::read_version(store.as_ref(), &hash, name, version).await? {
                Some(manifest) => print_manifest(&manifest),
                // Listed and then deleted: a concurrent sweep, not a defect.
                None => println!("  version: {version} (deleted since it was listed)"),
            }
        }
    }
    Ok(())
}

/// `parquet sweep`: delete every manifest version superseded for longer than
/// `grace`, refusing a grace below the deployment's stored minimum.
pub async fn sweep(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    grace: &str,
    now_ns: i64,
) -> anyhow::Result<()> {
    let grace_ms = parse_grace_ms(grace)?;
    let min_grace_ms = deployment_min_grace_ms(store.as_ref()).await?;
    let hash = TenantId::new(tenant).hash();
    let now_ms = now_ns / 1_000_000;
    let plan = sweep::plan(store.as_ref(), &hash, now_ms, grace_ms, min_grace_ms).await?;
    let report = sweep::execute(store.as_ref(), &plan).await?;
    println!(
        "deleted {} manifest versions",
        report.manifests_deleted.len()
    );
    for key in &report.manifests_deleted {
        println!("  {key}");
    }
    Ok(())
}

/// What `parquet repair` does after it is told the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairAction {
    /// List and flag; delete nothing.
    List,
    /// List, flag, and delete exactly the flagged keys (`--delete`).
    DeleteFlagged,
    /// Delete exactly this one version's key (`--delete-version`).
    DeleteVersion(u64),
}

/// `parquet repair`: list every key under one table's `v/` prefix and flag
/// the versions above the manifest version bound and the keys naming no
/// version; with [`RepairAction::DeleteFlagged`] remove exactly the flagged
/// ones, and with [`RepairAction::DeleteVersion`] remove that one version.
pub async fn repair(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    table: &str,
    action: RepairAction,
) -> anyhow::Result<()> {
    for line in repair_lines(store.as_ref(), tenant, table, action).await? {
        println!("{line}");
    }
    Ok(())
}

/// What [`repair`] prints. Keys, and the writer and statement fields, are
/// chosen by whoever put a forged version, so they are printed quoted and
/// escaped.
async fn repair_lines(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    table: &str,
    action: RepairAction,
) -> anyhow::Result<Vec<String>> {
    let hash = TenantId::new(tenant).hash();
    if let RepairAction::DeleteVersion(version) = action {
        let key = repair::delete_version(store, &hash, table, version).await?;
        return Ok(vec![
            format!("deleted manifest version {version} of table {table}"),
            format!("  key: {key:?}"),
        ]);
    }
    let entries = repair::list(store, &hash, table).await?;
    let mut out = vec![format!(
        "table: {table} ({} keys listed, version bound {MAX_MANIFEST_VERSION})",
        entries.len()
    )];
    for entry in &entries {
        let version = match &entry.version {
            ListedVersion::Number(v) => v.to_string(),
            ListedVersion::Invalid { slot, reason } => format!("{slot:?} ({reason})"),
            ListedVersion::NotAVersion { reason } => format!("none ({reason})"),
        };
        let flag = match &entry.version {
            ListedVersion::Invalid { .. } => "  FLAGGED: names no version",
            _ if entry.flagged => "  FLAGGED: above the version bound",
            _ => "",
        };
        out.push(format!("  version: {version}{flag}"));
        out.push(format!("    key: {:?}", entry.key));
        out.push(format!(
            "    stored_unix_ms: {} (the store's clock)",
            entry.last_modified_unix_ms
        ));
        match repair::describe(store, entry).await {
            Description::Manifest(manifest) => {
                out.push(format!("    created_by: {:?}", manifest.created_by));
                out.push(format!("    statement: {:?}", manifest.statement));
                out.push(format!("    dropped: {}", manifest.dropped));
            }
            Description::Missing => out.push("    (deleted since it was listed)".to_string()),
            Description::Unreadable { reason } => {
                out.push(format!("    created_by, statement: unreadable ({reason})"));
            }
        }
    }
    let flagged: Vec<String> = entries
        .iter()
        .filter(|e| e.flagged)
        .map(|e| e.key.clone())
        .collect();
    if flagged.is_empty() {
        out.push("no versions above the version bound and no keys naming no version".to_string());
        return Ok(out);
    }
    if action != RepairAction::DeleteFlagged {
        out.push(format!(
            "{} version(s) flagged; rerun with --delete to remove exactly these",
            flagged.len()
        ));
        return Ok(out);
    }
    let deleted = repair::delete_flagged(store, &hash, table, &flagged).await?;
    out.push(format!("deleted {} manifest versions", deleted.len()));
    out.extend(deleted.iter().map(|key| format!("  {key:?}")));
    Ok(out)
}

/// The deployment's minimum sweep grace, in milliseconds: `sys/gc`'s
/// `max_query_duration_ns`. Refuses a bucket with no `sys/gc`.
async fn deployment_min_grace_ms(store: &dyn ObjectStoreBackend) -> anyhow::Result<u64> {
    let Some((values, _version)) = read_gc_config(store).await? else {
        anyhow::bail!(
            "sys/gc is not present, so this bucket has no deployment-wide \
             max_query_duration to take the sweep's minimum grace from: start a server against \
             it, or write one with `ravel-cli gc-config set`"
        );
    };
    u64::try_from(values.max_query_duration_ns / 1_000_000).map_err(|_| {
        anyhow::anyhow!(
            "sys/gc records a negative max_query_duration_ns ({})",
            values.max_query_duration_ns
        )
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_maintain::{GcConfigValues, set_gc_config};
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
    use ravel_object_store::memory::MemoryStore;
    use ravel_pqtable::keys::manifest_key;
    use ravel_pqtable::manifest::encode_manifest;

    use super::*;

    const NOW_NS: i64 = 1_700_000_000_000_000_000;

    #[test]
    fn grace_parses_the_humantime_grammar_into_milliseconds() {
        assert_eq!(parse_grace_ms("1h").expect("parse"), 3_600_000);
        assert_eq!(parse_grace_ms("90s").expect("parse"), 90_000);
        assert!(parse_grace_ms("soon").is_err());
    }

    /// Write a `sys/gc` whose `max_query_duration` is two hours.
    async fn store_with_two_hour_query_duration() -> MemoryStore {
        let store = MemoryStore::new();
        let mut values = GcConfigValues::maintain_defaults();
        values.max_query_duration_ns = 2 * 3_600_000_000_000;
        values.protection_horizon_ns = values.max_query_duration_ns
            + values.grace_ns
            + ravel_maintain::config::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
        set_gc_config(
            &store,
            values.into(),
            ravel_maintain::config::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            NOW_NS,
        )
        .await
        .expect("set");
        store
    }

    /// The minimum grace is read from `sys/gc`, not from a constant in this
    /// crate: a deployment that stores a two-hour `max_query_duration` gets a
    /// two-hour floor.
    #[tokio::test]
    async fn the_minimum_grace_is_the_stored_max_query_duration() {
        let store = store_with_two_hour_query_duration().await;
        assert_eq!(
            deployment_min_grace_ms(&store).await.expect("min"),
            2 * 3_600_000
        );
    }

    /// `sweep` itself, not just the helper, refuses a `--grace` below the
    /// stored minimum, and accepts one at it.
    #[tokio::test]
    async fn sweep_refuses_a_grace_below_the_stored_minimum() {
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(store_with_two_hour_query_duration().await);
        let err = sweep(Arc::clone(&store), "acme", "1h", NOW_NS)
            .await
            .expect_err("must refuse");
        assert!(
            format!("{err:#}").contains("grace 3600000 ms is below the minimum 7200000 ms"),
            "{err:#}"
        );
        sweep(store, "acme", "2h", NOW_NS)
            .await
            .expect("a grace at the minimum is accepted");
    }

    /// Every field of the manifest and of the file it names reaches the
    /// output. The exhaustive destructuring in [`manifest_lines`] is what
    /// makes a new field a compile error; this pins the values themselves.
    #[test]
    fn every_manifest_field_and_every_file_field_is_printed() {
        let manifest = Manifest {
            table: "hits".into(),
            version: 7,
            dropped: false,
            location: "s3://customer/data/".into(),
            grant: "s3://customer/data/".into(),
            files: vec![ParquetFile {
                profile: "prod".into(),
                bucket: "customer".into(),
                key: b"data/part-0.parquet".to_vec(),
                size: 4096,
                etag: "etag-1".into(),
                version: "gen-9".into(),
                row_count: 12,
                footer_len: 64,
            }],
            options: [("compression".to_string(), "zstd".to_string())]
                .into_iter()
                .collect(),
            created_by: "ravel-cli".into(),
            created_unix_ns: NOW_NS,
            statement: "CREATE TABLE hits".into(),
            apply_nonce: vec![0xab, 0xcd],
        };
        let printed = manifest_lines(&manifest).join("\n");
        for expected in [
            "version: 7",
            "table: hits",
            "dropped: false",
            "location: s3://customer/data/",
            "grant: s3://customer/data/",
            "created_by: ravel-cli",
            "created_unix_ns: 1700000000000000000",
            "statement: CREATE TABLE hits",
            "apply_nonce: abcd",
            "options: 1",
            "compression: zstd",
            "files: 1",
            "key: data/part-0.parquet",
            "profile: prod",
            "bucket: customer",
            "size: 4096",
            "etag: etag-1",
            "version: gen-9",
            "row_count: 12",
            "footer_len: 64",
        ] {
            assert!(
                printed.contains(expected),
                "{expected:?} missing:\n{printed}"
            );
        }
    }

    /// Versions 1 and the bound of `acme`'s `hits` through the writer's
    /// format, plus a forged version one above the bound and one at
    /// `u64::MAX`, in a store that counts deletes.
    async fn forged_table() -> InstrumentedStore<MemoryStore> {
        let store = InstrumentedStore::new(MemoryStore::new());
        let hash = TenantId::new("acme").hash();
        for (version, by) in [
            (1, "ddl"),
            (MAX_MANIFEST_VERSION, "ddl"),
            (MAX_MANIFEST_VERSION + 1, "forger"),
            (u64::MAX, "forger"),
        ] {
            let manifest = Manifest {
                table: "hits".into(),
                version,
                dropped: true,
                location: String::new(),
                grant: String::new(),
                files: Vec::new(),
                options: Default::default(),
                created_by: by.into(),
                created_unix_ns: NOW_NS,
                statement: format!("DROP TABLE hits -- {by}\u{1b}[2J"),
                apply_nonce: vec![0; ravel_pqtable::manifest::APPLY_NONCE_LEN],
            };
            store
                .inner()
                .put(
                    &manifest_key(&hash, "hits", version).expect("key"),
                    Bytes::from(encode_manifest(&hash, &manifest).expect("encode")),
                    PutOptions::create_if_absent(),
                )
                .await
                .expect("put");
        }
        store
    }

    fn keys_left(lines: &[String]) -> Vec<String> {
        lines
            .iter()
            .filter_map(|l| l.strip_prefix("    key: "))
            .map(str::to_string)
            .collect()
    }

    fn deletes(store: &InstrumentedStore<MemoryStore>) -> u64 {
        store.metrics().snapshot().op(StoreOp::Delete).calls
    }

    #[tokio::test]
    async fn repair_lists_and_flags_and_deletes_nothing_without_the_flag() {
        let store = forged_table().await;
        let lines = repair_lines(&store, "acme", "hits", RepairAction::List)
            .await
            .expect("repair");
        let printed = lines.join("\n");
        let flagged: Vec<&String> = lines.iter().filter(|l| l.contains("FLAGGED")).collect();
        assert_eq!(
            flagged,
            vec![
                &format!(
                    "  version: {}  FLAGGED: above the version bound",
                    MAX_MANIFEST_VERSION + 1
                ),
                &format!("  version: {}  FLAGGED: above the version bound", u64::MAX),
            ],
            "{printed}"
        );
        assert!(
            printed.contains(&format!("  version: {MAX_MANIFEST_VERSION}\n")),
            "{printed}"
        );
        assert_eq!(printed.matches("created_by: \"forger\"").count(), 2);
        assert_eq!(printed.matches("created_by: \"ddl\"").count(), 2);
        // The statement is escaped, so a forged one cannot drive the terminal.
        assert!(printed.contains("statement: \"DROP TABLE hits -- forger\\u{1b}[2J\""));
        assert!(!printed.contains('\u{1b}'), "{printed}");
        assert!(
            printed.ends_with("2 version(s) flagged; rerun with --delete to remove exactly these")
        );
        assert_eq!(deletes(&store), 0);
        assert_eq!(keys_left(&lines).len(), 4);
    }

    #[tokio::test]
    async fn repair_with_the_flag_deletes_exactly_the_flagged_versions() {
        let store = forged_table().await;
        let lines = repair_lines(&store, "acme", "hits", RepairAction::DeleteFlagged)
            .await
            .expect("repair");
        let hash = TenantId::new("acme").hash();
        let printed = lines.join("\n");
        assert!(printed.contains("deleted 2 manifest versions"), "{printed}");
        assert_eq!(deletes(&store), 2);
        assert_eq!(
            resolve::versions(&store, &hash, "hits")
                .await
                .expect("versions"),
            vec![1, MAX_MANIFEST_VERSION]
        );
        // Run again: nothing is flagged and nothing more is deleted.
        let lines = repair_lines(&store, "acme", "hits", RepairAction::DeleteFlagged)
            .await
            .expect("repair");
        assert_eq!(
            lines.last().map(String::as_str),
            Some("no versions above the version bound and no keys naming no version")
        );
        assert_eq!(deletes(&store), 2);
    }

    /// A key is chosen by whoever put it, control characters included, so
    /// every line that prints one escapes it.
    #[tokio::test]
    async fn repair_prints_every_key_escaped() {
        let store = forged_table().await;
        let hash = TenantId::new("acme").hash();
        let slot = "\u{1b}[2J\u{7}xxxxxxxxxxxxxxx";
        assert_eq!(slot.chars().count(), 20);
        let key = format!(
            "{}{slot}.pqm",
            ravel_pqtable::keys::manifest_prefix(&hash, "hits").expect("prefix")
        );
        store
            .inner()
            .put(&key, Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("put");
        for action in [RepairAction::List, RepairAction::DeleteFlagged] {
            let lines = repair_lines(&store, "acme", "hits", action)
                .await
                .expect("repair");
            let printed = lines.join("\n");
            assert!(!printed.contains('\u{1b}'), "{printed}");
            assert!(!printed.contains('\u{7}'), "{printed}");
            assert!(printed.contains(&format!("{key:?}")), "{printed}");
        }
        assert_eq!(deletes(&store), 3);
    }

    #[tokio::test]
    async fn delete_version_deletes_exactly_the_named_key_and_prints_it() {
        let store = forged_table().await;
        let hash = TenantId::new("acme").hash();
        for version in [2, 3] {
            let manifest = ravel_pqtable::manifest::Manifest {
                version,
                ..resolve::read_version(&store, &hash, "hits", 1)
                    .await
                    .expect("read")
                    .expect("v1")
            };
            store
                .inner()
                .put(
                    &manifest_key(&hash, "hits", version).expect("key"),
                    Bytes::from(encode_manifest(&hash, &manifest).expect("encode")),
                    PutOptions::create_if_absent(),
                )
                .await
                .expect("put");
        }
        let lines = repair_lines(&store, "acme", "hits", RepairAction::DeleteVersion(2))
            .await
            .expect("repair");
        let key = manifest_key(&hash, "hits", 2).expect("key");
        assert_eq!(
            lines,
            vec![
                "deleted manifest version 2 of table hits".to_string(),
                format!("  key: {key:?}"),
            ]
        );
        assert_eq!(deletes(&store), 1);
        assert_eq!(
            resolve::versions(&store, &hash, "hits")
                .await
                .expect("versions"),
            vec![
                1,
                3,
                MAX_MANIFEST_VERSION,
                MAX_MANIFEST_VERSION + 1,
                u64::MAX
            ]
        );
    }

    #[tokio::test]
    async fn delete_version_refuses_zero_and_above_the_bound_before_any_delete() {
        let store = forged_table().await;
        for version in [0, MAX_MANIFEST_VERSION + 1, u64::MAX] {
            let err = repair_lines(&store, "acme", "hits", RepairAction::DeleteVersion(version))
                .await
                .expect_err("must refuse");
            assert!(
                format!("{err:#}").contains(&format!("refusing to delete version {version}")),
                "{err:#}"
            );
        }
        let snapshot = store.metrics().snapshot();
        assert_eq!(snapshot.op(StoreOp::Delete).calls, 0);
        assert_eq!(snapshot.op(StoreOp::List).calls, 0);
    }

    /// Under the Maintain credential no manifest read is allowed. The
    /// listing still flags the forged versions and the delete still runs.
    #[tokio::test]
    async fn repair_decides_from_the_listing_when_reads_are_denied() {
        let store = forged_table().await;
        let denied = FaultStore::new(
            store,
            FaultPlan::empty().with_rule(Rule::new(
                Op::Get,
                ScriptedFault::Permanent("access denied".into()),
            )),
        );
        let lines = repair_lines(&denied, "acme", "hits", RepairAction::DeleteFlagged)
            .await
            .expect("repair");
        let printed = lines.join("\n");
        assert_eq!(
            printed.matches("created_by, statement: unreadable").count(),
            4,
            "{printed}"
        );
        assert!(printed.contains("deleted 2 manifest versions"), "{printed}");
        assert_eq!(deletes(denied.inner()), 2);
    }

    /// A bucket no server has bootstrapped has no deployment minimum, so the
    /// sweep refuses rather than falling back to a value of its own.
    #[tokio::test]
    async fn a_bucket_without_sys_gc_has_no_minimum_grace() {
        let store = MemoryStore::new();
        let err = deployment_min_grace_ms(&store)
            .await
            .expect_err("must refuse");
        assert!(
            format!("{err:#}").contains("sys/gc is not present"),
            "{err:#}"
        );
    }
}
