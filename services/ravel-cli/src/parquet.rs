//! `ravel-cli parquet` (ADR-2040): inspect a tenant's Parquet table manifests
//! and delete the manifest versions a sweep has made superseded.
//!
//! Both subcommands delegate to [`ravel_pqtable`]: `ls` reads through
//! [`ravel_pqtable::resolve`], and `sweep` plans and executes through
//! [`ravel_pqtable::sweep`], which owns the age rule and the store-clock skew
//! margin. Nothing about the key layout or the grace arithmetic is restated
//! here.
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
//! [`ravel_pqtable::writer::apply`] finishes its put within half of the same
//! floor after its resolve, and a sweep deletes a version only once the version
//! after it is older than the grace. Every version committed after a writer's
//! resolve is younger than that, so no sweep frees the version key the
//! writer's create-if-absent put targets while the writer is in flight. The
//! stored value is at least every process's `--gc-max-query-duration`, the
//! floor ADR-2040 names, so taking it is the stricter choice.

use std::sync::Arc;

use ravel_maintain::read_gc_config;
use ravel_object_store::ObjectStoreBackend;
use ravel_pqtable::manifest::{Manifest, ParquetFile};
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

/// `parquet ls`: every table's newest manifest version, or every retained
/// version of one named table.
pub async fn ls(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    table: Option<&str>,
) -> anyhow::Result<()> {
    let hash = TenantId::new(tenant).hash();
    let all = resolve::tables(store.as_ref(), &hash).await?;
    let selected: Vec<(&String, &Vec<u64>)> = match table {
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
    for (name, versions) in selected {
        // With `--table`, every version still under the tenant's prefix; a
        // version a sweep has already deleted is not retained and cannot be
        // printed. Without it, only the newest.
        let wanted: &[u64] = if table.is_some() {
            versions
        } else {
            versions.last().map(std::slice::from_ref).unwrap_or(&[])
        };
        println!("table: {name} ({} retained versions)", versions.len());
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
    use ravel_maintain::{GcConfigValues, set_gc_config};
    use ravel_object_store::memory::MemoryStore;

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
            values,
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
