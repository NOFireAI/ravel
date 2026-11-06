//! Bounded, deterministic per-series samples drawn from the real MetricsBench
//! "cardinality" and "history" profiles (`benchmarks/metrics/workload.json`),
//! for the timestamp and value codec bake-offs (ADR-3694 decision 7).
//!
//! [`metrics_gen::Generator`] does not expose its internal per-step
//! `Vec<GeneratedSample>` buffer publicly (it is built and consumed inside
//! `generate_step`, a private method); the only public surface is the encoded
//! line format `generate_into`/`generate_bytes` write. This module drives that
//! public API over a small, fixed number of scrape steps, then decodes the
//! emitted lines (`<ts_ms>\t<series>\t<payload>\n`, floats as `f 0x{bits}`)
//! back into per-series timestamp and value sequences, bounded to the first
//! `max_series` distinct series encountered. The manifest's own seed makes
//! this reproducible: same profile, same `steps`, same `max_series` always
//! yield the same streams.

use crate::metrics_gen::Generator;
use crate::metrics_workload::load_workload;
use std::collections::BTreeMap;

/// The committed, real-scale MetricsBench manifest (ADR-3694's Stage 0
/// baseline), resolved from this crate's manifest directory the same way
/// every other MetricsBench bin/test in this crate does.
pub const WORKLOAD_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benchmarks/metrics/workload.json"
);

/// One series' samples, in step order. `values` holds only the `Float`
/// samples (a stale marker or native histogram sample advances `ts_ms` for
/// other series but contributes no float value), so it may be shorter than
/// `ts_ms`.
pub struct SeriesSample {
    pub series: String,
    pub ts_ms: Vec<i64>,
    pub values: Vec<f64>,
}

/// Draws up to `max_series` series' worth of samples from `profile_name`
/// (`"cardinality"` or `"history"`) over `steps` scrapes of the committed
/// workload manifest. Returns the manifest's seed (for the report's "fixed
/// seed" statement) alongside the series, in first-encountered order.
pub fn sample_series(
    profile_name: &str,
    steps: u64,
    max_series: usize,
) -> Result<(u64, Vec<SeriesSample>), String> {
    let workload = load_workload(WORKLOAD_PATH).map_err(|e| e.to_string())?;
    let seed = workload.seed;
    let mut generator = Generator::new(&workload, profile_name, 1_700_000_000_000)
        .map_err(|e| e.to_string())?;
    let (bytes, _report) = generator.generate_bytes(steps).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&bytes);

    let mut by_series: BTreeMap<String, SeriesSample> = BTreeMap::new();
    let mut order: Vec<String> = Vec::new();
    for line in text.lines() {
        let mut parts = line.splitn(3, '\t');
        let (Some(ts_str), Some(series), Some(payload)) =
            (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let Ok(ts_ms) = ts_str.parse::<i64>() else {
            continue;
        };
        if !by_series.contains_key(series) {
            if order.len() >= max_series {
                continue;
            }
            order.push(series.to_string());
            by_series.insert(
                series.to_string(),
                SeriesSample {
                    series: series.to_string(),
                    ts_ms: Vec::new(),
                    values: Vec::new(),
                },
            );
        }
        let Some(entry) = by_series.get_mut(series) else {
            continue;
        };
        entry.ts_ms.push(ts_ms);
        if let Some(hex) = payload.strip_prefix("f 0x") {
            if let Ok(bits) = u64::from_str_radix(hex, 16) {
                entry.values.push(f64::from_bits(bits));
            }
        }
    }
    let series = order
        .into_iter()
        .filter_map(|key| by_series.remove(&key))
        .collect();
    Ok((seed, series))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn cardinality_profile_yields_bounded_series_with_real_manifest_seed() {
        let (seed, series) = sample_series("cardinality", 8, 5).expect("sample cardinality");
        // The real manifest's seed (benchmarks/metrics/workload.json), so a
        // stale or hand-edited copy fails loud rather than reproducing
        // different numbers under the same report claim.
        assert_eq!(seed, 927_000_933);
        assert!(
            series.len() <= 5,
            "max_series must bound the distinct series returned"
        );
        assert!(!series.is_empty(), "the cardinality profile must emit at least one series");
        for s in &series {
            assert!(
                s.ts_ms.windows(2).all(|w| w[1] >= w[0] - 2 * 15_000),
                "series {} timestamps should be close to step order (a bounded out-of-order shift aside)",
                s.series
            );
            assert_eq!(s.ts_ms.len(), 8, "8 steps requested for series {}", s.series);
        }
    }

    #[test]
    fn history_profile_is_reachable_and_bounded() {
        let (_seed, series) = sample_series("history", 4, 3).expect("sample history");
        assert!(series.len() <= 3);
        assert!(!series.is_empty(), "the history profile must emit at least one series");
    }
}
