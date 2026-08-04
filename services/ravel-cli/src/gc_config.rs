//! `ravel-cli gc-config` (ADR-0050 section 4, EC4): show and set the durable,
//! deployment-wide GC configuration object `sys/gc`.
//!
//! `set` is the single mutation path for `sys/gc` (every mode only ever reads
//! it at startup). It enforces `protection_horizon >= max_query_duration +
//! grace` at write time -- refusing a violating proposal without writing
//! anything -- and swaps the durable object with `CasVersion`, so a concurrent
//! `gc-config set` is caught as a conflict rather than silently overwritten.
//! Both operations delegate to [`ravel_maintain::gc_config`]; there is no
//! CLI-side reimplementation of the constraint or the swap.

use std::sync::Arc;

use ravel_maintain::{GcConfigValues, SetOutcome, read_gc_config, set_gc_config};
use ravel_object_store::ObjectStoreBackend;

/// Parse a humantime duration (e.g. `25h`, `24h`, `1h`) into saturating `i64`
/// nanoseconds, matching ravel-server's `--retention-*` duration convention.
fn parse_duration_ns(flag: &str, s: &str) -> anyhow::Result<i64> {
    let dur =
        humantime::parse_duration(s).map_err(|e| anyhow::anyhow!("invalid {flag} '{s}': {e}"))?;
    i64::try_from(dur.as_nanos()).map_err(|_| anyhow::anyhow!("{flag} '{s}' is too large"))
}

/// `gc-config show`: print the durable `sys/gc` values, or report that the
/// bucket has not been bootstrapped yet. A corrupt or future-version object is a
/// typed error, never a panic.
pub async fn show(store: Arc<dyn ObjectStoreBackend>) -> anyhow::Result<()> {
    match read_gc_config(store.as_ref()).await? {
        Some((v, _version)) => {
            println!("protection_horizon_ns: {}", v.protection_horizon_ns);
            println!("grace_ns: {}", v.grace_ns);
            println!("max_query_duration_ns: {}", v.max_query_duration_ns);
            println!("max_flush_lifetime_ns: {}", v.max_flush_lifetime_ns);
            println!(
                "flight_ticket_ceiling_ns (protection_horizon - grace): {}",
                v.flight_ceiling_ns()
            );
            println!(
                "satisfies_constraint (protection_horizon >= max_query_duration + grace): {}",
                v.satisfies_constraint()
            );
        }
        None => {
            println!(
                "sys/gc is not present: this bucket has not been bootstrapped by a server yet. \
                 The first process to touch the bucket writes it from the maintain defaults."
            );
        }
    }
    Ok(())
}

/// `gc-config set`: write a full new `sys/gc` from the four durations. Enforces
/// the constraint at write time and swaps with `CasVersion` (ADR-0050 section
/// 4). All four values are given together so the written config is a complete,
/// auditable record rather than a partial mutation whose constraint depends on
/// unshown existing fields.
#[allow(clippy::too_many_arguments)]
pub async fn set(
    store: Arc<dyn ObjectStoreBackend>,
    protection_horizon: &str,
    grace: &str,
    max_query_duration: &str,
    max_flush_lifetime: &str,
    now_ns: i64,
) -> anyhow::Result<()> {
    let proposed = GcConfigValues {
        protection_horizon_ns: parse_duration_ns("--protection-horizon", protection_horizon)?,
        grace_ns: parse_duration_ns("--grace", grace)?,
        max_query_duration_ns: parse_duration_ns("--max-query-duration", max_query_duration)?,
        max_flush_lifetime_ns: parse_duration_ns("--max-flush-lifetime", max_flush_lifetime)?,
    };
    match set_gc_config(store.as_ref(), proposed, now_ns).await? {
        SetOutcome::Created => {
            println!("sys/gc created (the bucket had no GC-config object yet)");
        }
        SetOutcome::Updated => {
            println!("sys/gc updated (swapped in place with CasVersion)");
        }
    }
    println!("protection_horizon_ns: {}", proposed.protection_horizon_ns);
    println!("grace_ns: {}", proposed.grace_ns);
    println!("max_query_duration_ns: {}", proposed.max_query_duration_ns);
    println!("max_flush_lifetime_ns: {}", proposed.max_flush_lifetime_ns);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, StoreError};

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    /// The write-time constraint check: a proposed configuration violating
    /// `protection_horizon >= max_query_duration + grace` is refused and writes
    /// nothing.
    #[tokio::test]
    async fn set_refuses_a_constraint_violating_proposal_and_writes_nothing() {
        let store = store();
        // protection_horizon 2h, but max_query_duration 1h + grace 24h = 25h.
        let err = set(store.clone(), "2h", "24h", "1h", "1h", 1_000)
            .await
            .expect_err("a constraint-violating proposal must be refused");
        assert!(
            err.to_string().contains("protection_horizon"),
            "the error names the constraint: {err}"
        );
        let got = store
            .get(ravel_maintain::GC_CONFIG_KEY, GetRange::Full)
            .await;
        assert!(
            matches!(got, Err(StoreError::NotFound)),
            "a refused set writes no object"
        );
    }

    /// A valid `set` creates the object, and a second valid `set` swaps it in
    /// place via `CasVersion`.
    #[tokio::test]
    async fn set_creates_then_updates() {
        let store = store();
        // 25h horizon = 1h max_query_duration + 24h grace: satisfies.
        set(store.clone(), "25h", "24h", "1h", "1h", 1)
            .await
            .expect("first valid set creates the object");
        let (v, _) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(v.protection_horizon_ns, 25 * 3_600_000_000_000);

        // A wider horizon, still valid, swaps in place.
        set(store.clone(), "50h", "24h", "1h", "1h", 2)
            .await
            .expect("second valid set updates the object");
        let (v2, _) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(v2.protection_horizon_ns, 50 * 3_600_000_000_000);
    }

    /// `show` on a fresh bucket reports "not present" rather than erroring.
    #[tokio::test]
    async fn show_on_unbootstrapped_bucket_reports_absence() {
        let store = store();
        show(store)
            .await
            .expect("show on a fresh bucket succeeds with a not-present notice");
    }
}
