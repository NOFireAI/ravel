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

use std::sync::Arc;

use ravel_maintain::read_gc_config;
use ravel_object_store::ObjectStoreBackend;
use ravel_pqtable::manifest::Manifest;
use ravel_pqtable::{resolve, sweep};
use ravel_types::TenantId;

/// Parse a humantime `--grace` value into milliseconds, the unit
/// [`sweep::plan`] takes.
pub fn parse_grace_ms(s: &str) -> anyhow::Result<u64> {
    let dur = humantime::parse_duration(s)
        .map_err(|e| anyhow::anyhow!("invalid --grace '{s}': {e}"))?;
    u64::try_from(dur.as_millis()).map_err(|_| anyhow::anyhow!("--grace '{s}' is too large"))
}

/// Print every field of one manifest version, including every field of every
/// file it names.
fn print_manifest(manifest: &Manifest) {
    println!("  version: {}", manifest.version);
    println!("    table: {}", manifest.table);
    println!("    dropped: {}", manifest.dropped);
    println!("    location: {}", manifest.location);
    println!("    grant: {}", manifest.grant);
    println!("    created_by: {}", manifest.created_by);
    println!("    created_unix_ns: {}", manifest.created_unix_ns);
    println!("    statement: {}", manifest.statement);
    println!("    apply_nonce: {}", hex::encode(&manifest.apply_nonce));
    println!("    options: {}", manifest.options.len());
    for (key, value) in &manifest.options {
        println!("      {key}: {value}");
    }
    println!("    files: {}", manifest.files.len());
    for (index, file) in manifest.files.iter().enumerate() {
        // The key is the bytes the listing returned. A manifest that decoded
        // holds only addressable keys, but printing is lossy rather than
        // fallible so an operator can still read a record they are diagnosing.
        println!("      [{index}] key: {}", String::from_utf8_lossy(&file.key));
        println!("           profile: {}", file.profile);
        println!("           bucket: {}", file.bucket);
        println!("           size: {}", file.size);
        println!("           etag: {}", file.etag);
        println!("           version: {}", file.version);
        println!("           row_count: {}", file.row_count);
        println!("           footer_len: {}", file.footer_len);
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
    println!("deleted {} manifest versions", report.manifests_deleted.len());
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

    /// The minimum grace is read from `sys/gc`, not from a constant in this
    /// crate: a deployment that stores a two-hour `max_query_duration` gets a
    /// two-hour floor.
    #[tokio::test]
    async fn the_minimum_grace_is_the_stored_max_query_duration() {
        let store = MemoryStore::new();
        let mut values = GcConfigValues::maintain_defaults();
        values.max_query_duration_ns = 2 * 3_600_000_000_000;
        values.protection_horizon_ns = values.max_query_duration_ns + values.grace_ns
            + ravel_maintain::config::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
        set_gc_config(
            &store,
            values,
            ravel_maintain::config::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            NOW_NS,
        )
        .await
        .expect("set");
        assert_eq!(
            deployment_min_grace_ms(&store).await.expect("min"),
            2 * 3_600_000
        );
    }

    /// A bucket no server has bootstrapped has no deployment minimum, so the
    /// sweep refuses rather than falling back to a value of its own.
    #[tokio::test]
    async fn a_bucket_without_sys_gc_has_no_minimum_grace() {
        let store = MemoryStore::new();
        let err = deployment_min_grace_ms(&store)
            .await
            .expect_err("must refuse");
        assert!(format!("{err:#}").contains("sys/gc is not present"), "{err:#}");
    }
}
