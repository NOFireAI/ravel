//! `ravel-cli parquet` (ADR-2040): inspect a tenant's Parquet table manifests,
//! delete the manifest versions a sweep has made superseded, and remove
//! forged versions: those above the manifest version bound, keys naming no
//! version, one version an operator names, and keys under a segment that is
//! not a valid table name.
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

use std::io::Write;
use std::sync::Arc;

use ravel_maintain::read_gc_config;
use ravel_object_store::ObjectStoreBackend;
use ravel_pqtable::keys::{MAX_MANIFEST_VERSION, store_path};
use ravel_pqtable::manifest::{Manifest, ParquetFile};
use ravel_pqtable::repair::{
    self, Description, ListedVersion, RepairError, StrayClass, StrayEntry,
};
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
    let listing = resolve::tenant_listing(store.as_ref(), &hash).await?;
    let stray = listing.invalid_table_keys.len();
    if stray > 0 {
        println!(
            "{stray} manifest-shaped key(s) under no valid table name, skipped by every \
             reader: run `ravel-cli parquet repair --tenant {tenant} --stray`"
        );
    }
    let all = listing.tables;
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
    let mut lines = Vec::new();
    let result = collect_repair(store.as_ref(), tenant, table, action, &mut lines).await;
    print_lines(&lines, &mut std::io::stdout())?;
    result
}

/// Write `lines` to `out`, one per line.
fn print_lines(lines: &[String], out: &mut (dyn Write + Send)) -> anyhow::Result<()> {
    for line in lines {
        writeln!(out, "{line}")?;
    }
    Ok(())
}

/// [`collect_repair`]'s lines, or its error.
#[cfg(test)]
async fn repair_lines(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    table: &str,
    action: RepairAction,
) -> anyhow::Result<Vec<String>> {
    let mut lines = Vec::new();
    collect_repair(store, tenant, table, action, &mut lines).await?;
    Ok(lines)
}

/// Push to `out` what [`repair`] prints, so a refusal after the listing
/// still prints the listing. Keys, and the writer and statement fields, are
/// chosen by whoever put a forged version, so they are printed quoted and
/// escaped.
async fn collect_repair(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    table: &str,
    action: RepairAction,
    out: &mut Vec<String>,
) -> anyhow::Result<()> {
    let hash = TenantId::new(tenant).hash();
    if let RepairAction::DeleteVersion(version) = action {
        let key = repair::delete_version(store, &hash, table, version).await?;
        out.extend([
            format!("deleted manifest version {version} of table {table}"),
            format!("  key: {key:?}"),
        ]);
        return Ok(());
    }
    let entries = repair::list(store, &hash, table).await?;
    out.push(format!(
        "table: {table} ({} keys listed, version bound {MAX_MANIFEST_VERSION})",
        entries.len()
    ));
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
        if entry.undeletable {
            // No request reaches the key, so there is no body to describe.
            out.push(format!(
                "    undeletable by Ravel: the store's path encoding sends a delete of this key \
                 to {:?}; delete the exact key with the Maintain credential through an S3 tool",
                store_path(&entry.key)
            ));
            continue;
        }
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
    let flagged = entries.iter().filter(|e| e.flagged).count();
    let deletable: Vec<String> = entries
        .iter()
        .filter(|e| e.flagged && !e.undeletable)
        .map(|e| e.key.clone())
        .collect();
    let undeletable: Vec<&String> = entries
        .iter()
        .filter(|e| e.undeletable)
        .map(|e| &e.key)
        .collect();
    if flagged == 0 && undeletable.is_empty() {
        out.push("no versions above the version bound and no keys naming no version".to_string());
        return Ok(());
    }
    if action != RepairAction::DeleteFlagged {
        out.push(if undeletable.is_empty() {
            format!("{flagged} version(s) flagged; rerun with --delete to remove exactly these")
        } else {
            format!(
                "{flagged} version(s) flagged; rerun with --delete to remove {} of them; it \
                 skips the {} key(s) undeletable by Ravel, marked above",
                deletable.len(),
                undeletable.len()
            )
        });
        return Ok(());
    }
    for key in &undeletable {
        out.push(format!(
            "skipped {key:?}: undeletable by Ravel; delete the exact key with the Maintain \
             credential through an S3 tool"
        ));
    }
    let deletion = repair::delete_flagged(store, &hash, table, &deletable)
        .await
        .map_err(|err| delete_failed(out, "manifest versions", err))?;
    push_deleted(out, "manifest versions", &deletion.deleted);
    let left: Vec<String> = undeletable
        .into_iter()
        .chain(&deletion.undeletable)
        .map(|k| format!("{k:?}"))
        .collect();
    if !left.is_empty() {
        anyhow::bail!(
            "{} key(s) undeletable by Ravel left in place: {}",
            left.len(),
            left.join(", ")
        );
    }
    Ok(())
}

/// Push the line counting `deleted`, then each deleted key, escaped.
fn push_deleted(out: &mut Vec<String>, what: &str, deleted: &[String]) {
    out.push(format!("deleted {} {what}", deleted.len()));
    out.extend(deleted.iter().map(|key| format!("  {key:?}")));
}

/// `err` from a repair delete. A delete that failed part-way
/// ([`RepairError::Delete`]) pushes the keys deleted before it first, so they
/// are printed before the error.
fn delete_failed(out: &mut Vec<String>, what: &str, err: RepairError) -> anyhow::Error {
    let RepairError::Delete { deleted, .. } = &err else {
        return err.into();
    };
    push_deleted(out, what, deleted);
    anyhow::Error::from(err).context(
        "deleted the keys printed above, then a delete failed; no key after it was deleted",
    )
}

/// `parquet repair --stray`: list every key under the tenant's manifest
/// prefix whose segment between `pq/t/` and `/v/` is not a valid table name;
/// with `delete`, remove those Ravel can delete, and fail naming every such
/// key still listed afterwards.
pub async fn repair_stray(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    delete: bool,
    include_reserved_names: bool,
) -> anyhow::Result<()> {
    write_repair_stray(
        store.as_ref(),
        tenant,
        delete,
        include_reserved_names,
        &mut std::io::stdout(),
    )
    .await
}

/// [`repair_stray`], writing to `out` rather than stdout. The deletions are
/// written before a failed delete or a failed listing after them is
/// reported.
async fn write_repair_stray(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    delete: bool,
    include_reserved_names: bool,
    out: &mut (dyn Write + Send),
) -> anyhow::Result<()> {
    let report = repair_stray_lines(store, tenant, delete, include_reserved_names).await?;
    print_lines(&report.lines, out)?;
    if let Some(err) = report.error {
        return Err(err);
    }
    if !report.remaining.is_empty() {
        let keys: Vec<String> = report.remaining.iter().map(|k| format!("{k:?}")).collect();
        anyhow::bail!(
            "{} key(s) under no valid table name still listed after --delete: {}",
            keys.len(),
            keys.join(", ")
        );
    }
    Ok(())
}

/// What [`repair_stray`] prints, and with `delete` the stray keys a listing
/// after the deletes still found, or why a delete or that listing failed.
struct StrayReport {
    lines: Vec<String>,
    remaining: Vec<String>,
    error: Option<anyhow::Error>,
}

/// Why `--delete` leaves `entry` in place, if it does.
fn stray_skip(entry: &StrayEntry, include_reserved_names: bool) -> Option<&'static str> {
    match entry.class {
        StrayClass::Deletable => None,
        StrayClass::Undeletable => Some(
            "undeletable by Ravel; delete the exact key with the Maintain credential through \
             an S3 tool",
        ),
        StrayClass::ReservedName if include_reserved_names => None,
        StrayClass::ReservedName => Some(
            "possibly a table created before the name was reserved; pass \
             --include-reserved-names to delete it",
        ),
    }
}

/// The lines of [`repair_stray`]. The keys are chosen by whoever put them, so
/// they are printed quoted and escaped.
async fn repair_stray_lines(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    delete: bool,
    include_reserved_names: bool,
) -> anyhow::Result<StrayReport> {
    let hash = TenantId::new(tenant).hash();
    let entries = repair::list_stray(store, &hash).await?;
    let mut lines = vec![format!(
        "tenant: {tenant} ({} key(s) under no valid table name)",
        entries.len()
    )];
    for entry in &entries {
        lines.push(format!("  key: {:?}", entry.key));
        lines.push(format!(
            "    stored_unix_ms: {} (the store's clock)",
            entry.last_modified_unix_ms
        ));
        match entry.class {
            StrayClass::Deletable => {}
            StrayClass::Undeletable => lines.push(format!(
                "    undeletable by Ravel: the store's path encoding sends a delete of this key \
                 to {:?}; delete the exact key with the Maintain credential through an S3 tool",
                store_path(&entry.key)
            )),
            StrayClass::ReservedName => lines.push(
                "    possibly a table created before the name was reserved: --delete skips it \
                 unless --include-reserved-names is passed"
                    .to_string(),
            ),
        }
    }
    let mut report = StrayReport {
        lines,
        remaining: Vec::new(),
        error: None,
    };
    if entries.is_empty() {
        report
            .lines
            .push("no keys under no valid table name; nothing to delete".to_string());
        return Ok(report);
    }
    let (skipped, kept): (Vec<&StrayEntry>, Vec<&StrayEntry>) = entries
        .iter()
        .partition(|entry| stray_skip(entry, include_reserved_names).is_some());
    let to_delete: Vec<String> = kept.iter().map(|entry| entry.key.clone()).collect();
    if !delete {
        report.lines.push(if skipped.is_empty() {
            format!(
                "{} key(s) listed; rerun with --delete to remove exactly these",
                entries.len()
            )
        } else {
            format!(
                "{} key(s) listed; rerun with --delete to remove {} of them; it skips the other \
                 {}, marked above",
                entries.len(),
                to_delete.len(),
                skipped.len()
            )
        });
        return Ok(report);
    }
    for entry in &skipped {
        if let Some(reason) = stray_skip(entry, include_reserved_names) {
            report
                .lines
                .push(format!("skipped {:?}: {reason}", entry.key));
        }
    }
    let deleted = match repair::delete_stray(store, &hash, &to_delete, include_reserved_names).await
    {
        Ok(deleted) => deleted,
        Err(err) => {
            report.error = Some(delete_failed(&mut report.lines, "key(s)", err));
            return Ok(report);
        }
    };
    push_deleted(&mut report.lines, "key(s)", &deleted);
    match repair::list_stray(store, &hash).await {
        Ok(entries) => report.remaining = entries.into_iter().map(|entry| entry.key).collect(),
        Err(err) => {
            report.error = Some(anyhow::Error::from(err).context(
                "deleted the keys printed above, then could not list the tenant's keys under no \
                 valid table name again to confirm none is left",
            ));
        }
    }
    Ok(report)
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
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
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
        table_with(&[
            (1, "ddl"),
            (MAX_MANIFEST_VERSION, "ddl"),
            (MAX_MANIFEST_VERSION + 1, "forger"),
            (u64::MAX, "forger"),
        ])
        .await
    }

    /// `acme`'s `hits` holding each of `versions`, written by its `created_by`,
    /// through the writer's format, in a store that counts deletes.
    async fn table_with(versions: &[(u64, &str)]) -> InstrumentedStore<MemoryStore> {
        let store = InstrumentedStore::new(MemoryStore::new());
        let hash = TenantId::new("acme").hash();
        for &(version, by) in versions {
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

    /// A key is chosen by whoever put it, control characters included. The
    /// path encoding changes such a key, so the listing reports it
    /// unaddressable: the command names it escaped, never raw, marks it
    /// undeletable by Ravel, and `--delete` never sends it a delete.
    #[tokio::test]
    async fn repair_names_a_key_holding_control_characters_escaped_and_never_deletes_it() {
        let store = forged_table().await;
        let hash = TenantId::new("acme").hash();
        let slot = "\u{1b}[2J\u{7}xxxxxxxxxxxxxxx";
        assert_eq!(slot.chars().count(), 20);
        let prefix = ravel_pqtable::keys::manifest_prefix(&hash, "hits").expect("prefix");
        let key = format!("{prefix}{slot}.pqm");
        store.inner().insert_foreign(&key, Bytes::from_static(b"x"));
        let named = format!(
            "    key: {key:?}\n    stored_unix_ms: 0 (the store's clock)\n    undeletable by \
             Ravel: the store's path encoding sends a delete of this key to {:?}; delete the \
             exact key with the Maintain credential through an S3 tool",
            store_path(&key)
        );
        for action in [RepairAction::List, RepairAction::DeleteFlagged] {
            let mut lines = Vec::new();
            let result = collect_repair(&store, "acme", "hits", action, &mut lines).await;
            let printed = lines.join("\n");
            assert!(!printed.contains('\u{1b}'), "{printed}");
            assert!(!printed.contains('\u{7}'), "{printed}");
            assert!(printed.contains(&named), "{printed}");
            // The key's own line, then the summary or the skipped line.
            assert_eq!(
                printed.matches("undeletable by Ravel").count(),
                2,
                "{printed}"
            );
            if action == RepairAction::List {
                result.expect("repair");
                assert!(
                    printed.ends_with(
                        "3 version(s) flagged; rerun with --delete to remove 2 of them; it skips \
                         the 1 key(s) undeletable by Ravel, marked above"
                    ),
                    "{printed}"
                );
            } else {
                let err = result.expect_err("an undeletable key is left");
                assert_eq!(
                    format!("{err:#}"),
                    format!("1 key(s) undeletable by Ravel left in place: {key:?}")
                );
                assert!(printed.contains("deleted 2 manifest versions"), "{printed}");
            }
        }
        assert_eq!(deletes(&store), 2);
        assert_eq!(unaddressable_under(&store, &prefix).await, [key]);
    }

    /// A key under the `v/` prefix that does not parse as a manifest key is
    /// never flagged, so when the listing also reports it unaddressable it is
    /// only reported: `--delete` removes the flagged version and succeeds,
    /// and the key is left in place.
    #[tokio::test]
    async fn repair_reports_an_unflagged_undeletable_key_and_does_not_fail_on_it() {
        let hash = TenantId::new("acme").hash();
        let prefix = ravel_pqtable::keys::manifest_prefix(&hash, "hits").expect("prefix");
        let junk = format!("{prefix}junk~");
        let junk_key = junk.as_str();
        let seeded = move || async move {
            let store = table_with(&[(1, "ddl"), (MAX_MANIFEST_VERSION + 1, "forger")]).await;
            store.inner().insert_foreign(junk_key, Bytes::from_static(b"x"));
            store
        };
        let named = format!(
            "    key: {junk:?}\n    stored_unix_ms: 0 (the store's clock)\n    undeletable by \
             Ravel: the store's path encoding sends a delete of this key to {:?}",
            store_path(&junk)
        );

        let store = seeded().await;
        let lines = repair_lines(&store, "acme", "hits", RepairAction::DeleteFlagged)
            .await
            .expect("an unflagged key is not a deletion candidate");
        let printed = lines.join("\n");
        assert!(printed.contains(&named), "{printed}");
        assert!(!printed.contains("skipped"), "{printed}");
        let flagged_key = manifest_key(&hash, "hits", MAX_MANIFEST_VERSION + 1).expect("key");
        assert!(
            printed.ends_with(&format!("deleted 1 manifest versions\n  {flagged_key:?}")),
            "{printed}"
        );
        assert_eq!(deletes(&store), 1);
        assert_eq!(
            resolve::versions(&store, &hash, "hits")
                .await
                .expect("versions"),
            vec![1]
        );
        assert_eq!(unaddressable_under(&store, &prefix).await, [junk.clone()]);

        // Nothing is flagged now; the key is still reported, and still kept.
        let lines = repair_lines(&store, "acme", "hits", RepairAction::DeleteFlagged)
            .await
            .expect("repair");
        let printed = lines.join("\n");
        assert!(printed.contains(&named), "{printed}");
        assert_eq!(
            lines.last().map(String::as_str),
            Some(
                "no versions above the version bound and no keys naming no version; 1 key(s) \
                 not flagged and undeletable by Ravel, marked above, are left in place"
            ),
            "{printed}"
        );
        assert_eq!(deletes(&store), 1);
        assert_eq!(unaddressable_under(&store, &prefix).await, [junk.clone()]);

        // On the same tree, the list-only run flags the one version.
        let listed = seeded().await;
        let lines = repair_lines(&listed, "acme", "hits", RepairAction::List)
            .await
            .expect("repair");
        let printed = lines.join("\n");
        assert!(printed.contains(&named), "{printed}");
        assert_eq!(
            lines.last().map(String::as_str),
            Some(
                "1 version(s) flagged; rerun with --delete to remove exactly these; 1 key(s) \
                 not flagged and undeletable by Ravel, marked above, are left in place"
            ),
            "{printed}"
        );
        assert_eq!(deletes(&listed), 0);
        assert_eq!(unaddressable_under(&listed, &prefix).await, [junk]);
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

    /// Handles keys the way the S3 adapter's `object_store` client does, and
    /// records the key every `delete` is sent to. A list page holding a key
    /// `Path::parse` refuses (a control character, an empty segment, a `.` or
    /// `..` segment; no key here starts or ends with `/`) fails, whether the
    /// page reports it as an object or unaddressable, and a delete
    /// goes to [`store_path`] of its key. With `lose_deletes` set, a delete
    /// reports success and removes nothing.
    struct S3Keys<S> {
        inner: S,
        deletes: std::sync::Mutex<Vec<String>>,
        lose_deletes: bool,
    }

    impl<S> S3Keys<S> {
        fn new(inner: S) -> Self {
            S3Keys {
                inner,
                deletes: std::sync::Mutex::new(Vec::new()),
                lose_deletes: false,
            }
        }

        fn deletes(&self) -> Vec<String> {
            self.deletes.lock().expect("lock").clone()
        }
    }

    fn s3_lists(key: &str) -> bool {
        key.split('/').all(|segment| {
            !matches!(segment, "" | "." | "..") && !segment.chars().any(|c| c.is_ascii_control())
        })
    }

    #[async_trait::async_trait]
    impl<S: ObjectStoreBackend> ObjectStoreBackend for S3Keys<S> {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: ravel_object_store::GetRange,
        ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
            self.inner.get(key, range).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, ravel_object_store::StoreError>
        {
            self.inner.put_multipart(key).await
        }

        async fn head(
            &self,
            key: &str,
        ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
            let page = self.inner.list(prefix, page).await?;
            if let Some(key) = page
                .objects
                .iter()
                .map(|meta| &meta.key)
                .chain(page.unaddressable.iter().map(|skipped| &skipped.key))
                .find(|key| !s3_lists(key))
            {
                return Err(ravel_object_store::StoreError::Permanent(format!(
                    "invalid path: {key:?}"
                )));
            }
            Ok(page)
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
            let path = store_path(key);
            self.deletes.lock().expect("lock").push(path.clone());
            if self.lose_deletes {
                return Ok(());
            }
            self.inner.delete(&path).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// The text after `t/<tenant_hash>/pq/t/` of `.pqm` keys under a segment
    /// that is not a valid table name, which the S3 adapter lists and
    /// deletes: upper case, a name reserved since tables could first be
    /// created, and a nested path.
    const STRAYS: [&str; 3] = [
        "Hits/v/00000000000000000001.pqm",
        "logs/v/00000000000000000001.pqm",
        "a/b/v/00000000000000000001.pqm",
    ];

    /// Stray keys no request reaches: a tilde, and a quote and a backslash in
    /// the version characters, all three percent-encoded by the path
    /// encoding. A listing reports them unaddressable, never as objects.
    const UNDELETABLE: [&str; 2] = [
        "Hits~/v/00000000000000000001.pqm",
        "Hits/v/\"\\xxxxxxxxxxxxxxxxxx.pqm",
    ];

    /// A manifest key of a table named with a word reserved after tables
    /// could be created.
    const RESERVED: &str = "l0/v/00000000000000000001.pqm";

    fn acme_key(rest: &str) -> String {
        let hash = TenantId::new("acme").hash();
        format!(
            "{}{rest}",
            ravel_pqtable::keys::tenant_manifest_prefix(&hash)
        )
    }

    /// `acme`'s keys for `rests`, ascending.
    fn sorted(rests: &[&str]) -> Vec<String> {
        let mut keys: Vec<String> = rests.iter().map(|rest| acme_key(rest)).collect();
        keys.sort();
        keys
    }

    /// [`forged_table`] plus the stray keys `rests` of `acme`, written raw at
    /// store time 5_000, behind [`S3Keys`].
    async fn stray_table(rests: &[&str]) -> S3Keys<InstrumentedStore<MemoryStore>> {
        let store = forged_table().await;
        store.inner().set_clock_ms(5_000);
        for rest in rests {
            store
                .inner()
                .insert_foreign(&acme_key(rest), Bytes::from_static(b"x"));
        }
        S3Keys::new(store)
    }

    /// The keys under `prefix` that a listing of `store` reports
    /// unaddressable.
    async fn unaddressable_under(store: &dyn ObjectStoreBackend, prefix: &str) -> Vec<String> {
        ravel_object_store::list_all_reporting(store, prefix)
            .await
            .expect("list")
            .unaddressable
            .sample
            .into_iter()
            .map(|skipped| skipped.key)
            .collect()
    }

    fn acme_prefix() -> String {
        ravel_pqtable::keys::tenant_manifest_prefix(&TenantId::new("acme").hash())
    }

    /// Every stray key [`stray_table`] is given by the tests below.
    fn every_stray() -> Vec<&'static str> {
        STRAYS
            .iter()
            .chain(&UNDELETABLE)
            .copied()
            .chain([RESERVED])
            .collect()
    }

    /// The [`UNDELETABLE`] keys are reported unaddressable by the listing.
    /// The command names each, escaped, with the key a request for it would
    /// reach and how to remove it, and counts them among the keys `--delete`
    /// skips.
    #[tokio::test]
    async fn repair_stray_names_every_stray_key_and_deletes_nothing() {
        let store = stray_table(&every_stray()).await;
        let report = repair_stray_lines(&store, "acme", false, false)
            .await
            .expect("repair");
        let printed = report.lines.join("\n");
        let listed: Vec<&str> = report
            .lines
            .iter()
            .filter_map(|l| l.strip_prefix("  key: "))
            .collect();
        let expected: Vec<String> = sorted(&every_stray())
            .iter()
            .map(|k| format!("{k:?}"))
            .collect();
        assert_eq!(listed, expected, "{printed}");
        assert_eq!(
            printed
                .matches("stored_unix_ms: 5000 (the store's clock)")
                .count(),
            6
        );
        assert_eq!(
            unaddressable_under(&store, &acme_prefix()).await,
            sorted(&UNDELETABLE)
        );
        for key in sorted(&UNDELETABLE) {
            assert_ne!(store_path(&key), key);
            let named = format!(
                "  key: {key:?}\n    stored_unix_ms: 5000 (the store's clock)\n    undeletable \
                 by Ravel: the store's path encoding sends a delete of this key to {:?}; delete \
                 the exact key with the Maintain credential through an S3 tool",
                store_path(&key)
            );
            assert!(printed.contains(&named), "{printed}");
        }
        assert_eq!(printed.matches("undeletable by Ravel").count(), 2);
        // Printed escaped, never raw.
        assert!(printed.contains("v/\\\"\\\\x"), "{printed}");
        assert!(!printed.contains("v/\""), "{printed}");
        let reserved = format!(
            "  key: {:?}\n    stored_unix_ms: 5000 (the store's clock)\n    possibly a table \
             created before the name was reserved",
            acme_key(RESERVED)
        );
        assert!(printed.contains(&reserved), "{printed}");
        assert_eq!(printed.matches("possibly a table").count(), 1);
        assert!(!printed.contains("/hits/"), "{printed}");
        assert!(
            printed.ends_with(
                "6 key(s) listed; rerun with --delete to remove 3 of them; it skips the other 3, \
                 marked above"
            ),
            "{printed}"
        );
        assert!(store.deletes().is_empty());
        assert!(report.remaining.is_empty());
    }

    #[tokio::test]
    async fn repair_stray_lists_deletable_keys_alone_as_exactly_these() {
        let store = stray_table(&STRAYS).await;
        let report = repair_stray_lines(&store, "acme", false, false)
            .await
            .expect("repair");
        assert_eq!(
            report.lines.last().map(String::as_str),
            Some("3 key(s) listed; rerun with --delete to remove exactly these")
        );
    }

    /// A key under a reserved name is skipped, and the listing after the
    /// deletes names it, so the command fails. A key the store's path
    /// encoding changes is skipped and named, never sent a delete that would
    /// land on another key, so it stays in place and the command fails
    /// naming it too.
    #[tokio::test]
    async fn repair_stray_with_the_flag_deletes_what_it_can_and_fails_naming_the_rest() {
        let store = Arc::new(stray_table(&every_stray()).await);
        let hash = TenantId::new("acme").hash();
        let report = repair_stray_lines(store.as_ref(), "acme", true, false)
            .await
            .expect("repair");
        let printed = report.lines.join("\n");
        assert_eq!(store.deletes(), sorted(&STRAYS), "{printed}");
        assert!(printed.contains("deleted 3 key(s)"), "{printed}");
        for key in sorted(&UNDELETABLE) {
            assert!(
                printed.contains(&format!(
                    "skipped {key:?}: undeletable by Ravel; delete the exact key with the \
                     Maintain credential through an S3 tool"
                )),
                "{printed}"
            );
        }
        assert!(
            printed.contains(&format!(
                "skipped {:?}: possibly a table created before the name was reserved; pass \
                 --include-reserved-names to delete it",
                acme_key(RESERVED)
            )),
            "{printed}"
        );
        let mut left = sorted(&UNDELETABLE);
        left.push(acme_key(RESERVED));
        left.sort();
        assert_eq!(report.remaining, left);
        // The valid table keeps every key it had, flagged ones included.
        assert_eq!(
            resolve::versions(store.as_ref(), &hash, "hits")
                .await
                .expect("versions"),
            vec![1, MAX_MANIFEST_VERSION, MAX_MANIFEST_VERSION + 1, u64::MAX]
        );

        // Run again through the command: nothing more is deleted, and the
        // run fails naming every key still there.
        let err = repair_stray(store.clone(), "acme", true, false)
            .await
            .expect_err("keys remain");
        let text = err.to_string();
        assert!(
            text.starts_with("3 key(s) under no valid table name still listed after --delete"),
            "{text}"
        );
        for key in &left {
            assert!(text.contains(&format!("{key:?}")), "{text}");
        }
        assert_eq!(store.deletes().len(), 3);

        // With reserved names included, the reserved one goes; the
        // unaddressable keys are still there, and the run still names them.
        let report = repair_stray_lines(store.as_ref(), "acme", true, true)
            .await
            .expect("repair");
        assert_eq!(store.deletes().len(), 4);
        assert_eq!(store.deletes()[3], acme_key(RESERVED));
        assert_eq!(report.remaining, sorted(&UNDELETABLE));
        assert_eq!(
            unaddressable_under(store.as_ref(), &acme_prefix()).await,
            sorted(&UNDELETABLE)
        );
    }

    #[tokio::test]
    async fn repair_stray_fails_naming_a_key_a_reported_delete_left_in_place() {
        let mut store = stray_table(&STRAYS).await;
        store.lose_deletes = true;
        let store = Arc::new(store);
        let err = repair_stray(store.clone(), "acme", true, false)
            .await
            .expect_err("keys remain");
        let text = err.to_string();
        assert!(
            text.starts_with("3 key(s) under no valid table name still listed after --delete"),
            "{text}"
        );
        for key in sorted(&STRAYS) {
            assert!(text.contains(&format!("{key:?}")), "{text}");
        }
        assert_eq!(store.deletes(), sorted(&STRAYS));
    }

    /// The listing after the deletes fails: the deleted keys are still
    /// written, then the command fails with the listing's error.
    #[tokio::test]
    async fn repair_stray_prints_its_deletions_when_the_listing_after_them_fails() {
        let store = forged_table().await;
        for rest in STRAYS {
            store
                .inner()
                .put(
                    &acme_key(rest),
                    Bytes::from_static(b"x"),
                    PutOptions::default(),
                )
                .await
                .expect("put");
        }
        let store = S3Keys::new(FaultStore::new(
            store,
            FaultPlan::empty().with_rule(
                Rule::new(Op::List, ScriptedFault::Permanent("listing refused".into()))
                    .with_occurrence(Occurrence::Nth(2)),
            ),
        ));
        let mut out = Vec::new();
        let err = write_repair_stray(&store, "acme", true, false, &mut out)
            .await
            .expect_err("the second listing fails");
        assert_eq!(store.inner.fault_count(Op::List, FaultKind::Permanent), 1);
        let printed = String::from_utf8(out).expect("utf-8");
        let mut deleted = vec!["deleted 3 key(s)".to_string()];
        deleted.extend(sorted(&STRAYS).iter().map(|key| format!("  {key:?}")));
        assert!(
            printed.ends_with(&format!("{}\n", deleted.join("\n"))),
            "{printed}"
        );
        assert_eq!(store.deletes(), sorted(&STRAYS));
        let text = format!("{err:#}");
        assert!(
            text.starts_with(
                "deleted the keys printed above, then could not list the tenant's keys under no \
                 valid table name again to confirm none is left"
            ),
            "{text}"
        );
        assert!(text.contains("listing refused"), "{text}");
    }

    /// A key under the table's own `v/` prefix that the store's path
    /// encoding changes is reported unaddressable by the listing, so the
    /// command names it undeletable by Ravel. `--delete` deletes every
    /// flagged version, never sends that key a delete, and fails naming it.
    #[tokio::test]
    async fn repair_names_and_never_deletes_a_key_the_path_encoding_changes() {
        let hash = TenantId::new("acme").hash();
        let prefix = ravel_pqtable::keys::manifest_prefix(&hash, "hits").expect("prefix");
        let tilde = format!("{prefix}~~~~~~~~~~~~~~~~~~~~.pqm");
        let store = S3Keys::new(forged_table().await);
        store
            .inner
            .inner()
            .insert_foreign(&tilde, Bytes::from_static(b"x"));
        let lines = repair_lines(&store, "acme", "hits", RepairAction::List)
            .await
            .expect("repair");
        let printed = lines.join("\n");
        assert!(
            printed.contains(&format!(
                "    key: {tilde:?}\n    stored_unix_ms: 0 (the store's clock)\n    undeletable \
                 by Ravel: the store's path encoding sends a delete of this key to {:?}; delete \
                 the exact key with the Maintain credential through an S3 tool",
                store_path(&tilde)
            )),
            "{printed}"
        );
        assert_eq!(keys_left(&lines).len(), 5, "{printed}");
        assert!(
            printed.ends_with(
                "3 version(s) flagged; rerun with --delete to remove 2 of them; it skips the 1 \
                 key(s) undeletable by Ravel, marked above"
            ),
            "{printed}"
        );
        assert_eq!(
            unaddressable_under(&store, &prefix).await,
            std::slice::from_ref(&tilde)
        );

        let mut lines = Vec::new();
        let err = collect_repair(
            &store,
            "acme",
            "hits",
            RepairAction::DeleteFlagged,
            &mut lines,
        )
        .await
        .expect_err("the tilde key is left");
        assert_eq!(
            format!("{err:#}"),
            format!("1 key(s) undeletable by Ravel left in place: {tilde:?}")
        );
        let printed = lines.join("\n");
        let forged = [
            manifest_key(&hash, "hits", MAX_MANIFEST_VERSION + 1).expect("key"),
            manifest_key(&hash, "hits", u64::MAX).expect("key"),
        ];
        assert!(
            printed.ends_with(&format!(
                "\nskipped {tilde:?}: undeletable by Ravel; delete the exact key with the \
                 Maintain credential through an S3 tool\ndeleted 2 manifest versions\n  {:?}\n  \
                 {:?}",
                forged[0], forged[1]
            )),
            "{printed}"
        );
        // Each flagged version is sent a delete at its own key; the tilde
        // key is never sent one.
        assert_eq!(store.deletes(), forged);
        assert_eq!(deletes(&store.inner), 2);
        assert_eq!(unaddressable_under(&store, &prefix).await, [tilde]);
        assert_eq!(
            resolve::versions(&store, &hash, "hits")
                .await
                .expect("versions"),
            vec![1, MAX_MANIFEST_VERSION]
        );
    }

    /// The second delete of `--stray --delete` fails: the listing and the
    /// key deleted before it are still written, then the command fails with
    /// the store error.
    #[tokio::test]
    async fn repair_stray_prints_the_keys_deleted_before_a_failed_delete() {
        let store = forged_table().await;
        for rest in STRAYS {
            store
                .inner()
                .put(
                    &acme_key(rest),
                    Bytes::from_static(b"x"),
                    PutOptions::default(),
                )
                .await
                .expect("put");
        }
        let store = S3Keys::new(FaultStore::new(
            store,
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Delete,
                    ScriptedFault::Permanent("delete refused".into()),
                )
                .with_occurrence(Occurrence::Nth(2)),
            ),
        ));
        let mut out = Vec::new();
        let err = write_repair_stray(&store, "acme", true, false, &mut out)
            .await
            .expect_err("the second delete fails");
        assert_eq!(store.inner.fault_count(Op::Delete, FaultKind::Permanent), 1);
        let keys = sorted(&STRAYS);
        assert_eq!(store.deletes(), keys[..2]);
        assert_eq!(deletes(store.inner.inner()), 1);
        let printed = String::from_utf8(out).expect("utf-8");
        assert!(
            printed.starts_with("tenant: acme (3 key(s) under no valid table name)\n"),
            "{printed}"
        );
        for key in &keys {
            assert!(printed.contains(&format!("  key: {key:?}\n")), "{printed}");
        }
        assert!(
            printed.ends_with(&format!("deleted 1 key(s)\n  {:?}\n", keys[0])),
            "{printed}"
        );
        let text = format!("{err:#}");
        assert!(
            text.starts_with(&format!(
                "deleted the keys printed above, then a delete failed; no key after it was \
                 deleted: object store error deleting {:?}, after deleting 1 key(s) before it",
                keys[1]
            )),
            "{text}"
        );
        assert!(text.contains("delete refused"), "{text}");
    }

    #[tokio::test]
    async fn repair_stray_with_the_flag_and_only_deletable_keys_succeeds() {
        let store = Arc::new(stray_table(&STRAYS).await);
        repair_stray(store.clone(), "acme", true, false)
            .await
            .expect("repair");
        assert_eq!(store.deletes(), sorted(&STRAYS));
        // Run again: nothing to delete, and nothing is.
        let report = repair_stray_lines(store.as_ref(), "acme", true, false)
            .await
            .expect("repair");
        assert_eq!(
            report.lines.last().map(String::as_str),
            Some("no keys under no valid table name; nothing to delete")
        );
        assert_eq!(store.deletes().len(), 3);
    }

    /// On S3 a stray key holding a control character fails the listing, so
    /// the command fails with the store error and deletes nothing.
    #[tokio::test]
    async fn repair_stray_fails_on_a_key_the_s3_adapter_cannot_list() {
        let control = "Hits/v/\u{1b}[2J\u{7}xxxxxxxxxxxxxxx.pqm";
        let store = stray_table(&[STRAYS[0], control]).await;
        let err = match repair_stray_lines(&store, "acme", true, false).await {
            Ok(report) => panic!("listed: {:?}", report.lines),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("invalid path"), "{err:#}");
        assert!(store.deletes().is_empty());
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
