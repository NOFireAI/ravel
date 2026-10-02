//! `ravel-cli gc-config` (ADR-0050 section 4): show and set the durable,
//! deployment-wide GC configuration object `sys/gc`.
//!
//! `set` is the single mutation path for `sys/gc` (every mode only ever reads
//! it at startup). It enforces `protection_horizon >= max_query_duration +
//! grace + clock_skew_allowance` at write time -- refusing a violating proposal
//! without writing anything (S1-02: the skew term stops a sweeper whose clock
//! leads a reader's from deleting a pinned snapshot) -- and swaps the durable
//! object with `CasVersion`, so a concurrent `gc-config set` is caught as a
//! conflict rather than silently overwritten. Both operations delegate to
//! [`ravel_maintain::gc_config`]; there is no CLI-side reimplementation of the
//! constraint or the swap. The clock-skew allowance is not stored in `sys/gc`
//! (it is a per-process input each maintain process re-validates against the
//! stored values at startup); it is a `set`-time input defaulting to the
//! deployment-wide [`DEFAULT_CLOCK_SKEW_ALLOWANCE_NS`] (5 min), matching the
//! sweeper's `CompactorConfig::clock_skew_allowance_ns`.
//!
//! `set --head-cache-ttl` writes format version 2, which records the HEAD cache
//! TTL every query-mode server process is held to (ADR-1133 decision 4).
//! Without the flag, `set` keeps the stored format version and, on version 2,
//! the recorded TTL.

use std::sync::Arc;

use ravel_maintain::config::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
use ravel_maintain::{GcConfigProposal, SetOutcome, read_gc_config, set_gc_config};
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
            println!("format_version: {}", v.format_version);
            println!("protection_horizon_ns: {}", v.protection_horizon_ns);
            println!("grace_ns: {}", v.grace_ns);
            println!("max_query_duration_ns: {}", v.max_query_duration_ns);
            println!("max_flush_lifetime_ns: {}", v.max_flush_lifetime_ns);
            println!(
                "{}",
                head_cache_ttl_line(v.format_version, v.head_cache_ttl_ns)
            );
            println!(
                "flight_ticket_ceiling_ns (protection_horizon - grace): {}",
                v.flight_ceiling_ns()
            );
            println!(
                "satisfies_constraint (protection_horizon >= max_query_duration + grace + \
                 clock_skew_allowance[{DEFAULT_CLOCK_SKEW_ALLOWANCE_NS}ns]): {}",
                v.satisfies_constraint(DEFAULT_CLOCK_SKEW_ALLOWANCE_NS)
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

/// The `head_cache_ttl_ns` line `show` and `set` print, naming where a version
/// 1 object's value comes from.
fn head_cache_ttl_line(format_version: u32, head_cache_ttl_ns: i64) -> String {
    if format_version == ravel_maintain::GC_FORMAT_VERSION_V1 {
        format!(
            "head_cache_ttl_ns: {head_cache_ttl_ns} (format version 1 records none; the compiled \
             default applies)"
        )
    } else {
        format!("head_cache_ttl_ns: {head_cache_ttl_ns}")
    }
}

/// `gc-config set`: write a full new `sys/gc` from the four durations. Enforces
/// the constraint `protection_horizon >= max_query_duration + grace +
/// clock_skew_allowance` at write time and swaps with `CasVersion` (ADR-0050
/// section 4). All four stored values are given together so the written config
/// is a complete, auditable record rather than a partial mutation whose
/// constraint depends on unshown existing fields. `clock_skew_allowance` is a
/// write-time input to the constraint (not a stored field), defaulting to the
/// deployment-wide [`DEFAULT_CLOCK_SKEW_ALLOWANCE_NS`]; an operator running
/// sweepers with a larger skew allowance passes it so the horizon is forced to
/// cover it (S1-02).
///
/// `head_cache_ttl` (`--head-cache-ttl`) writes format version 2 recording that
/// TTL: the ADR-1133 rollout flip, after which a build that reads only version
/// 1 refuses to start. Without it, the stored format version is kept, and a
/// stored version 2 keeps its recorded TTL. A TTL below this build's compiled
/// query head-cache TTL is refused, since every query-mode process would then
/// refuse to start.
#[allow(clippy::too_many_arguments)]
pub async fn set(
    store: Arc<dyn ObjectStoreBackend>,
    protection_horizon: &str,
    grace: &str,
    max_query_duration: &str,
    max_flush_lifetime: &str,
    clock_skew_allowance: Option<&str>,
    head_cache_ttl: Option<&str>,
    now_ns: i64,
) -> anyhow::Result<()> {
    let clock_skew_allowance_ns = match clock_skew_allowance {
        Some(s) => parse_duration_ns("--clock-skew-allowance", s)?,
        None => DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    };
    let proposed = GcConfigProposal {
        protection_horizon_ns: parse_duration_ns("--protection-horizon", protection_horizon)?,
        grace_ns: parse_duration_ns("--grace", grace)?,
        max_query_duration_ns: parse_duration_ns("--max-query-duration", max_query_duration)?,
        max_flush_lifetime_ns: parse_duration_ns("--max-flush-lifetime", max_flush_lifetime)?,
        head_cache_ttl_ns: head_cache_ttl
            .map(|s| parse_duration_ns("--head-cache-ttl", s))
            .transpose()?,
    };
    // A query-mode process refuses to start when its own TTL exceeds the
    // recorded one, and no server flag lowers that TTL below this build's
    // compiled default, so a smaller recorded value would stop every query
    // process at its next restart.
    if let Some(ttl_ns) = proposed.head_cache_ttl_ns
        && ttl_ns < ravel_catalog::DEFAULT_HEAD_CACHE_TTL_NS
    {
        anyhow::bail!(
            "--head-cache-ttl records head_cache_ttl_ns={ttl_ns}, below this build's query \
             head-cache TTL of {} ns; every query-mode process would refuse to start at its \
             next restart",
            ravel_catalog::DEFAULT_HEAD_CACHE_TTL_NS
        );
    }
    let (outcome, written) =
        set_gc_config(store.as_ref(), proposed, clock_skew_allowance_ns, now_ns).await?;
    match outcome {
        SetOutcome::Created => {
            println!("sys/gc created (the bucket had no GC-config object yet)");
        }
        SetOutcome::Updated => {
            println!("sys/gc updated (swapped in place with CasVersion)");
        }
    }
    println!("format_version: {}", written.format_version);
    println!("protection_horizon_ns: {}", written.protection_horizon_ns);
    println!("grace_ns: {}", written.grace_ns);
    println!("max_query_duration_ns: {}", written.max_query_duration_ns);
    println!("max_flush_lifetime_ns: {}", written.max_flush_lifetime_ns);
    println!(
        "{}",
        head_cache_ttl_line(written.format_version, written.head_cache_ttl_ns)
    );
    println!("clock_skew_allowance_ns (constraint input, not stored): {clock_skew_allowance_ns}");
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
        // protection_horizon 2h, but max_query_duration 1h + grace 24h = 25h
        // (already violates before the skew term is even added).
        let err = set(store.clone(), "2h", "24h", "1h", "1h", None, None, 1_000)
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
        // 26h horizon >= 1h max_query_duration + 24h grace + 5m default skew
        // allowance (= 25h5m): satisfies the skew-covering bound.
        set(store.clone(), "26h", "24h", "1h", "1h", None, None, 1)
            .await
            .expect("first valid set creates the object");
        let (v, _) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(v.protection_horizon_ns, 26 * 3_600_000_000_000);

        // A wider horizon, still valid, swaps in place.
        set(store.clone(), "50h", "24h", "1h", "1h", None, None, 2)
            .await
            .expect("second valid set updates the object");
        let (v2, _) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(v2.protection_horizon_ns, 50 * 3_600_000_000_000);
    }

    /// The CLI surface plumbs the clock-skew allowance into the constraint
    /// (S1-02): a 25h horizon meets max_query_duration 1h + grace 24h but leaves
    /// zero budget for the default 5m skew allowance, so `set` refuses it and
    /// writes nothing. Passing an explicit `--clock-skew-allowance 0` (a
    /// deployment that asserts zero cross-host skew) makes the same 25h horizon
    /// acceptable, proving the flag reaches the constraint rather than a fixed
    /// default.
    #[tokio::test]
    async fn set_refuses_horizon_that_omits_default_skew_then_accepts_with_zero_skew() {
        let store = store();
        // Default skew (None -> 5m): 25h < 1h + 24h + 5m, refused.
        let err = set(store.clone(), "25h", "24h", "1h", "1h", None, None, 1_000)
            .await
            .expect_err("25h omits the default 5m skew allowance and must be refused");
        assert!(
            err.to_string().contains("clock_skew_allowance"),
            "the error names the skew term: {err}"
        );
        let got = store
            .get(ravel_maintain::GC_CONFIG_KEY, GetRange::Full)
            .await;
        assert!(
            matches!(got, Err(StoreError::NotFound)),
            "a skew-uncovered set writes no object"
        );

        // Explicit zero skew: 25h >= 1h + 24h + 0, accepted.
        set(
            store.clone(),
            "25h",
            "24h",
            "1h",
            "1h",
            Some("0s"),
            None,
            2_000,
        )
        .await
        .expect("with an explicit zero skew allowance, 25h satisfies the bound");
        let (v, _) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(v.protection_horizon_ns, 25 * 3_600_000_000_000);
    }

    /// The stored proto's `format_version`, read without the decoder's
    /// version handling.
    async fn stored_format_version(store: &Arc<dyn ObjectStoreBackend>) -> u32 {
        use prost::Message;
        let got = store
            .get(ravel_maintain::GC_CONFIG_KEY, GetRange::Full)
            .await
            .expect("sys/gc present");
        ravel_proto::sys::v1::GcConfig::decode(got.data.as_ref())
            .expect("decodes")
            .format_version
    }

    /// `--head-cache-ttl` is the rollout flip: `set` writes format version 2
    /// recording that TTL, over a stored version 1.
    #[tokio::test]
    async fn set_with_head_cache_ttl_writes_version_2() {
        let store = store();
        set(store.clone(), "26h", "24h", "1h", "1h", None, None, 1)
            .await
            .expect("first set");
        assert_eq!(stored_format_version(&store).await, 1);
        set(
            store.clone(),
            "26h",
            "24h",
            "1h",
            "1h",
            None,
            Some("45s"),
            2,
        )
        .await
        .expect("set with --head-cache-ttl");
        assert_eq!(stored_format_version(&store).await, 2);
        let (v, _) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(v.format_version, 2);
        assert_eq!(v.head_cache_ttl_ns, 45_000_000_000);
    }

    /// Without `--head-cache-ttl`, `set` over a stored version 1 keeps
    /// version 1: no TTL is recorded, so older builds keep starting.
    #[tokio::test]
    async fn set_without_head_cache_ttl_over_version_1_stays_version_1() {
        let store = store();
        ravel_maintain::bootstrap_gc_config(
            store.as_ref(),
            ravel_maintain::GcConfigValues::maintain_defaults(),
            1,
        )
        .await
        .expect("bootstrap writes version 1");
        set(store.clone(), "50h", "24h", "1h", "1h", None, None, 2)
            .await
            .expect("set without the flag");
        assert_eq!(stored_format_version(&store).await, 1);
        let (v, _) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(v.protection_horizon_ns, 50 * 3_600_000_000_000);
        assert_eq!(
            v.head_cache_ttl_ns,
            ravel_catalog::DEFAULT_HEAD_CACHE_TTL_NS
        );
    }

    /// Without `--head-cache-ttl`, `set` over a stored version 2 keeps
    /// version 2 and its recorded TTL: it never writes version 1 over version
    /// 2, which would let older builds start again.
    #[tokio::test]
    async fn set_without_head_cache_ttl_over_version_2_keeps_version_2_and_its_ttl() {
        let store = store();
        set(
            store.clone(),
            "26h",
            "24h",
            "1h",
            "1h",
            None,
            Some("45s"),
            1,
        )
        .await
        .expect("flip to version 2");
        set(store.clone(), "50h", "24h", "1h", "1h", None, None, 2)
            .await
            .expect("set without the flag");
        assert_eq!(
            stored_format_version(&store).await,
            2,
            "set without the flag never writes version 1 over version 2"
        );
        let (v, _) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(v.format_version, 2);
        assert_eq!(
            v.head_cache_ttl_ns, 45_000_000_000,
            "the recorded TTL is kept"
        );
        assert_eq!(v.protection_horizon_ns, 50 * 3_600_000_000_000);
    }

    /// A `--head-cache-ttl` below this build's compiled query TTL is refused and
    /// writes nothing: a query-mode process cannot lower its own TTL, so it
    /// would refuse to start against the recorded value.
    #[tokio::test]
    async fn set_refuses_a_head_cache_ttl_below_the_compiled_default() {
        let store = store();
        let below = format!("{}ns", ravel_catalog::DEFAULT_HEAD_CACHE_TTL_NS - 1);
        let err = set(
            store.clone(),
            "26h",
            "24h",
            "1h",
            "1h",
            None,
            Some(&below),
            1,
        )
        .await
        .expect_err("a TTL below the compiled default must be refused");
        assert!(err.to_string().contains("head_cache_ttl_ns"), "got: {err}");
        let got = store
            .get(ravel_maintain::GC_CONFIG_KEY, GetRange::Full)
            .await;
        assert!(matches!(got, Err(StoreError::NotFound)));
        let exact = format!("{}ns", ravel_catalog::DEFAULT_HEAD_CACHE_TTL_NS);
        set(
            store.clone(),
            "26h",
            "24h",
            "1h",
            "1h",
            None,
            Some(&exact),
            2,
        )
        .await
        .expect("the compiled default itself is accepted");
    }

    /// A zero `--head-cache-ttl` is refused and writes nothing: version 2 must
    /// record a positive TTL.
    #[tokio::test]
    async fn set_refuses_a_zero_head_cache_ttl() {
        let store = store();
        let err = set(store.clone(), "26h", "24h", "1h", "1h", None, Some("0s"), 1)
            .await
            .expect_err("a zero TTL must be refused");
        assert!(err.to_string().contains("head_cache_ttl_ns"), "got: {err}");
        let got = store
            .get(ravel_maintain::GC_CONFIG_KEY, GetRange::Full)
            .await;
        assert!(matches!(got, Err(StoreError::NotFound)));
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
