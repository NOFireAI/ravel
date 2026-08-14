//! Classic-bucket histogram functions (plan section 8/P9): `histogram_quantile`
//! and `histogram_fraction` over `le`-labeled classic bucket series, as
//! opposed to native histograms (out of scope until P11's blocked ticket).
//! Both group the input instant vector by every label except `le`, treat
//! each group's series as one classic histogram's cumulative buckets, and
//! interpolate linearly within a bucket assuming a uniform distribution of
//! observations there (Prometheus' `bucketQuantile`/`bucketFraction`,
//! `promql/quantile.go`).
//!
//! Both functions require a `+Inf` bucket (without it the total observation
//! count, and so the top of the distribution, is unknown) and tolerate a
//! non-monotonic cumulative count (a reset, a federation merge, or an
//! otherwise corrupted classic histogram) by clamping each bucket to the
//! running maximum seen so far, matching Prometheus' `ensureMonotonic`. A
//! group failing either requirement, or with fewer than two usable buckets,
//! or a NaN scalar argument, evaluates to `NaN` for that group rather than
//! being omitted: the result vector always has one entry per group, exactly
//! like Prometheus. These cases now raise the annotations Prometheus does
//! (issues #178, #235), through the `&QueryWindow` threaded into both
//! functions: a malformed bucket set (missing `+Inf`, or fewer than two
//! usable buckets), a series excluded because its `le` label is missing or
//! does not parse as a float, and an out-of-range quantile argument are
//! `warnings`; a forced monotonicity fixup is an `info`. See
//! [`crate::Annotations`] for the warning/info distinction. The
//! native-histogram path's own Prometheus annotations are not wired here yet
//! (see this ticket's final report).

use std::collections::{HashMap, HashSet};

use ravel_types::{Label, LabelSet};

use crate::eval::{
    InstantVector, QueryWindow, classic_histogram_bad_buckets_warning, drop_metric_name,
    forced_monotonicity_info, invalid_quantile_warning,
};
use crate::histogram::{self, FloatHistogram};

use super::{FunctionDef, FunctionKind};

pub(crate) const FUNCTIONS: &[FunctionDef] = &[
    FunctionDef {
        name: "histogram_quantile",
        kind: FunctionKind::HistogramQuantile(histogram_quantile),
    },
    FunctionDef {
        name: "histogram_fraction",
        kind: FunctionKind::HistogramFraction(histogram_fraction),
    },
];

const LE_LABEL: &str = "le";

/// One classic bucket within a group: its upper bound (the parsed `le`
/// value) and cumulative observation count (the bucket series' own value).
#[derive(Clone, Copy)]
struct Bucket {
    upper_bound: f64,
    count: f64,
}

/// The result of [`group_by_bucket`]: the per-group cumulative buckets, plus
/// whether any input series was excluded because its `le` label was missing
/// or did not parse as a float. That exclusion is Prometheus'
/// `BadBucketLabelWarning`; like the out-of-range quantile warning it is a
/// whole-input property rather than a per-output-group one, so it is surfaced
/// as a single flag here and raised once by the caller via `ctx.warn`,
/// matching the compute-detects/caller-raises split the rest of this module
/// already uses for its annotations.
struct GroupedBuckets {
    groups: Vec<(LabelSet, Vec<Bucket>)>,
    bad_le_label: bool,
}

/// Groups `vector` by every label except `le` and `__name__` (Prometheus
/// drops the metric name from any function result). A series with no `le`
/// label, or one that does not parse as a float, is not a valid classic
/// bucket and is dropped from the histogram entirely (the pinned Prometheus
/// excludes it too); such an exclusion sets [`GroupedBuckets::bad_le_label`]
/// so the caller can raise the matching `BadBucketLabelWarning`.
fn group_by_bucket(vector: InstantVector) -> GroupedBuckets {
    let mut groups: Vec<(LabelSet, Vec<Bucket>)> = Vec::new();
    let mut index: HashMap<LabelSet, usize> = HashMap::new();
    let mut bad_le_label = false;
    for sample in vector {
        let Some(le_str) = sample.labels.get(LE_LABEL) else {
            bad_le_label = true;
            continue;
        };
        let Ok(upper_bound) = le_str.parse::<f64>() else {
            bad_le_label = true;
            continue;
        };
        let key: Vec<Label> = sample
            .labels
            .iter()
            .filter(|l| l.name != LE_LABEL && l.name != ravel_types::METRIC_NAME_LABEL)
            .cloned()
            .collect();
        // Filtering an already-deduplicated `LabelSet` cannot introduce a
        // duplicate name, so this reconstruction cannot fail.
        let key = LabelSet::new(key).unwrap_or_default();
        let bucket = Bucket {
            upper_bound,
            count: sample.value,
        };
        match index.get(&key) {
            Some(&i) => groups[i].1.push(bucket),
            None => {
                index.insert(key.clone(), groups.len());
                groups.push((key, vec![bucket]));
            }
        }
    }
    GroupedBuckets {
        groups,
        bad_le_label,
    }
}

/// Prometheus' `BadBucketLabelWarning` message: a classic-histogram series was
/// excluded from its group because its `le` label was missing or did not parse
/// as a float, so it could not be placed as a bucket. A warning, not an info:
/// a silently dropped series changes the computed quantile/fraction, which is
/// rarely what the caller intended.
fn bad_bucket_label_warning() -> String {
    "bucket label \"le\" is missing or has a malformed value on a classic \
     histogram series, which was excluded from the histogram"
        .to_string()
}

/// The outcome of [`prepare`]: the cleaned bucket list plus whether its
/// counts had to be clamped back into monotonic order (which the caller
/// surfaces as a `HistogramQuantileForcedMonotonicityInfo` info).
struct Prepared {
    buckets: Vec<Bucket>,
    forced_monotonic: bool,
}

/// Sorts `buckets` ascending by upper bound, requires a `+Inf` top bucket,
/// coalesces duplicate upper bounds by summing their counts (a repeated
/// `le` value, e.g. from a mis-scraped target), and clamps each bucket's
/// count to the running maximum seen so far so the cumulative sequence is
/// non-decreasing (Prometheus' `ensureMonotonic`). Returns `None` if the
/// group has fewer than two buckets before or after coalescing, or its
/// highest upper bound is not `+Inf`: not a well-formed classic histogram.
/// The returned `forced_monotonic` flag records whether any count actually
/// had to be clamped, so the caller can raise the matching info annotation.
fn prepare(mut buckets: Vec<Bucket>) -> Option<Prepared> {
    if buckets.len() < 2 {
        return None;
    }
    buckets.sort_by(|a, b| a.upper_bound.total_cmp(&b.upper_bound));
    if buckets[buckets.len() - 1].upper_bound != f64::INFINITY {
        return None;
    }

    let mut coalesced: Vec<Bucket> = Vec::with_capacity(buckets.len());
    for b in buckets {
        match coalesced.last_mut() {
            Some(last) if last.upper_bound == b.upper_bound => last.count += b.count,
            _ => coalesced.push(b),
        }
    }
    if coalesced.len() < 2 {
        return None;
    }

    let mut forced_monotonic = false;
    let mut running_max = coalesced[0].count;
    for b in coalesced.iter_mut().skip(1) {
        if b.count < running_max {
            b.count = running_max;
            forced_monotonic = true;
        } else {
            running_max = b.count;
        }
    }
    Some(Prepared {
        buckets: coalesced,
        forced_monotonic,
    })
}

/// Prometheus' `bucketQuantile`, applied to an already-[`prepare`]d bucket
/// list (at least two buckets, monotonic, the last one `+Inf`). `phi` is
/// validated by the caller before this runs.
fn bucket_quantile(phi: f64, buckets: &[Bucket]) -> f64 {
    let last = buckets.len() - 1;
    let total = buckets[last].count;
    if total == 0.0 {
        return f64::NAN;
    }
    let rank = phi * total;
    let b = buckets[..last]
        .iter()
        .position(|bucket| bucket.count >= rank)
        .unwrap_or(last);
    if b == last {
        return buckets[last - 1].upper_bound;
    }
    if b == 0 && buckets[0].upper_bound <= 0.0 {
        return buckets[0].upper_bound;
    }

    let bucket_end = buckets[b].upper_bound;
    let (bucket_start, count, rank) = if b > 0 {
        (
            buckets[b - 1].upper_bound.max(0.0),
            buckets[b].count - buckets[b - 1].count,
            rank - buckets[b - 1].count,
        )
    } else {
        (0.0, buckets[b].count, rank)
    };
    bucket_start + (bucket_end - bucket_start) * (rank / count)
}

/// Per-group annotation flags a classic-histogram computation raises (issue
/// #178): `bad_buckets` when the group is not a usable classic histogram
/// (fewer than two buckets, or no `+Inf`), `forced_monotonic` when its
/// cumulative counts had to be clamped. The phi/quantile out-of-range
/// warning is not per-group (it depends only on the scalar argument) and is
/// raised once by the caller instead.
#[derive(Default, Clone, Copy)]
struct ClassicGroupDiag {
    bad_buckets: bool,
    forced_monotonic: bool,
}

/// `phi` argument edges (checked ahead of any bucket validity check,
/// matching the pinned Prometheus): `NaN` is `NaN`, below `0` is `-Inf`,
/// above `1` is `+Inf`. Returns the group value plus the annotation flags
/// its buckets earned (only meaningful once `phi` is in range).
fn quantile_for_group(phi: f64, buckets: Vec<Bucket>) -> (f64, ClassicGroupDiag) {
    if phi.is_nan() {
        return (f64::NAN, ClassicGroupDiag::default());
    }
    if phi < 0.0 {
        return (f64::NEG_INFINITY, ClassicGroupDiag::default());
    }
    if phi > 1.0 {
        return (f64::INFINITY, ClassicGroupDiag::default());
    }
    match prepare(buckets) {
        Some(prepared) => (
            bucket_quantile(phi, &prepared.buckets),
            ClassicGroupDiag {
                bad_buckets: false,
                forced_monotonic: prepared.forced_monotonic,
            },
        ),
        None => (
            f64::NAN,
            ClassicGroupDiag {
                bad_buckets: true,
                forced_monotonic: false,
            },
        ),
    }
}

/// Split an input vector into native-histogram elements (keyed by their
/// labels with `__name__` dropped) and the remaining float elements (the
/// classic `le`-bucket series). A native histogram and a classic bucket family
/// that would land on the same output label set is the mixed case Prometheus
/// warns about (#178); the native value wins here (see [`histogram_quantile`]).
fn split_native_classic(vector: InstantVector) -> (Vec<(LabelSet, FloatHistogram)>, InstantVector) {
    let mut native = Vec::new();
    let mut classic = Vec::new();
    for sample in vector {
        match sample.histogram {
            Some(h) => native.push((drop_metric_name(sample.labels), h)),
            None => classic.push(sample),
        }
    }
    (native, classic)
}

/// `phi` argument edges for the native path, checked ahead of the histogram
/// (matching the pinned Prometheus, same order the classic path uses).
fn native_quantile(phi: f64, h: &FloatHistogram) -> f64 {
    if phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    histogram::histogram_quantile(phi, h)
}

fn histogram_quantile(phi: f64, vector: InstantVector, ctx: &QueryWindow) -> Vec<(LabelSet, f64)> {
    // An out-of-range (or NaN) `phi` clamps every group to an infinity (or
    // NaN); warn once for the whole call, not per group (Prometheus'
    // `InvalidQuantileWarning`, raised on `math.IsNaN(phi) || phi < 0 ||
    // phi > 1`, which `!contains` matches since NaN is in no range).
    if !(0.0..=1.0).contains(&phi) {
        ctx.warn(invalid_quantile_warning(phi));
    }
    let (native, classic) = split_native_classic(vector);
    let mut out = Vec::new();
    let mut seen: HashSet<LabelSet> = HashSet::new();
    for (labels, h) in native {
        out.push((labels.clone(), native_quantile(phi, &h)));
        seen.insert(labels);
    }
    let GroupedBuckets {
        groups,
        bad_le_label,
    } = group_by_bucket(classic);
    if bad_le_label {
        ctx.warn(bad_bucket_label_warning());
    }
    for (labels, buckets) in groups {
        // Mixed native+classic on the same output labels: native already
        // produced this group; drop the classic form (Prometheus keeps the
        // native value and warns, #178).
        if seen.contains(&labels) {
            continue;
        }
        let (value, diag) = quantile_for_group(phi, buckets);
        raise_classic_diag(diag, ctx);
        out.push((labels, value));
    }
    out
}

/// Raise a classic-histogram group's annotation flags onto `ctx`: a
/// malformed bucket set is a warning, forced monotonicity an info. Both
/// channels de-duplicate, so several malformed groups in one call collapse
/// to a single warning.
fn raise_classic_diag(diag: ClassicGroupDiag, ctx: &QueryWindow) {
    if diag.bad_buckets {
        ctx.warn(classic_histogram_bad_buckets_warning());
    }
    if diag.forced_monotonic {
        ctx.info(forced_monotonicity_info());
    }
}

/// The observation rank (cumulative count) at value `v`, by inverting
/// [`bucket_quantile`]'s own piecewise-linear interpolation: the rank
/// function and the quantile function assume the same uniform-within-bucket
/// distribution, so this is `bucket_quantile` read right-to-left.
fn bucket_rank_at(v: f64, buckets: &[Bucket]) -> f64 {
    let last = buckets.len() - 1;
    let total = buckets[last].count;
    if v == f64::INFINITY {
        return total;
    }
    if v <= buckets[0].upper_bound {
        if buckets[0].upper_bound <= 0.0 {
            return if v < buckets[0].upper_bound {
                0.0
            } else {
                buckets[0].count
            };
        }
        if v <= 0.0 {
            return 0.0;
        }
        return buckets[0].count * (v / buckets[0].upper_bound);
    }
    for i in 1..last {
        if v <= buckets[i].upper_bound {
            let bucket_start = buckets[i - 1].upper_bound.max(0.0);
            let bucket_end = buckets[i].upper_bound;
            let count = buckets[i].count - buckets[i - 1].count;
            let start_count = buckets[i - 1].count;
            return start_count + count * ((v - bucket_start) / (bucket_end - bucket_start));
        }
    }
    total
}

fn bucket_fraction(lower: f64, upper: f64, buckets: &[Bucket]) -> f64 {
    let total = buckets[buckets.len() - 1].count;
    let lower_rank = bucket_rank_at(lower, buckets);
    let upper_rank = bucket_rank_at(upper, buckets);
    ((upper_rank - lower_rank) / total).clamp(0.0, 1.0)
}

/// `lower`/`upper` argument edges, checked ahead of any bucket validity
/// check: either being `NaN` makes the result `NaN`; an empty or inverted
/// range (`lower >= upper`) is `0`; the full range (`-Inf`, `+Inf`) is
/// always `1` regardless of the histogram's own total, once it has one.
fn fraction_for_group(lower: f64, upper: f64, buckets: Vec<Bucket>) -> (f64, ClassicGroupDiag) {
    if lower.is_nan() || upper.is_nan() {
        return (f64::NAN, ClassicGroupDiag::default());
    }
    let Some(prepared) = prepare(buckets) else {
        return (
            f64::NAN,
            ClassicGroupDiag {
                bad_buckets: true,
                forced_monotonic: false,
            },
        );
    };
    let diag = ClassicGroupDiag {
        bad_buckets: false,
        forced_monotonic: prepared.forced_monotonic,
    };
    let prepared = prepared.buckets;
    if prepared[prepared.len() - 1].count == 0.0 {
        return (f64::NAN, diag);
    }
    if lower >= upper {
        return (0.0, diag);
    }
    if lower == f64::NEG_INFINITY && upper == f64::INFINITY {
        return (1.0, diag);
    }
    (bucket_fraction(lower, upper, &prepared), diag)
}

/// `lower`/`upper` edges for the native path (NaN argument -> NaN), then the
/// native fraction.
fn native_fraction(lower: f64, upper: f64, h: &FloatHistogram) -> f64 {
    if lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    histogram::histogram_fraction(lower, upper, h)
}

fn histogram_fraction(
    lower: f64,
    upper: f64,
    vector: InstantVector,
    ctx: &QueryWindow,
) -> Vec<(LabelSet, f64)> {
    let (native, classic) = split_native_classic(vector);
    let mut out = Vec::new();
    let mut seen: HashSet<LabelSet> = HashSet::new();
    for (labels, h) in native {
        out.push((labels.clone(), native_fraction(lower, upper, &h)));
        seen.insert(labels);
    }
    let GroupedBuckets {
        groups,
        bad_le_label,
    } = group_by_bucket(classic);
    if bad_le_label {
        ctx.warn(bad_bucket_label_warning());
    }
    for (labels, buckets) in groups {
        if seen.contains(&labels) {
            continue;
        }
        let (value, diag) = fraction_for_group(lower, upper, buckets);
        raise_classic_diag(diag, ctx);
        out.push((labels, value));
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn label_set(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(n, v)| Label {
                    name: (*n).to_string(),
                    value: (*v).to_string(),
                })
                .collect(),
        )
        .expect("valid labels")
    }

    fn sample(labels: &[(&str, &str)], value: f64) -> crate::eval::InstantSample {
        crate::eval::InstantSample {
            labels: label_set(labels),
            ts_ns: 0,
            orig_sample_ts_ns: 0,
            value,
            histogram: None,
        }
    }

    /// `histogram_quantile` with a throwaway annotation context, for the
    /// value-only tests below (the annotation wiring itself is covered by
    /// the dedicated `*_raises_*`/`*_warns_*` tests and by ravel-query's
    /// response-envelope tests).
    fn hq(phi: f64, vector: InstantVector) -> Vec<(LabelSet, f64)> {
        histogram_quantile(phi, vector, &QueryWindow::for_test())
    }

    /// `histogram_fraction` counterpart of [`hq`].
    fn hf(lower: f64, upper: f64, vector: InstantVector) -> Vec<(LabelSet, f64)> {
        histogram_fraction(lower, upper, vector, &QueryWindow::for_test())
    }

    fn wellformed_vector() -> InstantVector {
        vec![
            sample(
                &[("__name__", "http_bucket"), ("job", "a"), ("le", "0.1")],
                10.0,
            ),
            sample(
                &[("__name__", "http_bucket"), ("job", "a"), ("le", "0.5")],
                20.0,
            ),
            sample(
                &[("__name__", "http_bucket"), ("job", "a"), ("le", "1")],
                30.0,
            ),
            sample(
                &[("__name__", "http_bucket"), ("job", "a"), ("le", "+Inf")],
                40.0,
            ),
        ]
    }

    #[test]
    fn quantile_groups_by_labels_minus_le_and_drops_metric_name() {
        let got = hq(0.5, wellformed_vector());
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, label_set(&[("job", "a")]));
    }

    #[test]
    fn quantile_interpolates_within_the_owning_bucket() {
        // 40 total observations, phi=0.5 -> rank 20, exactly the boundary
        // of the (0.1, 0.5] bucket, which owns ranks (10, 20].
        let got = hq(0.5, wellformed_vector());
        assert_eq!(got[0].1, 0.5);
    }

    #[test]
    fn quantile_interpolates_the_lowest_bucket_from_zero() {
        // rank 5 of 10 falls inside the (-Inf, 0.1] bucket, interpolated
        // from 0 since its lower edge is <= 0.
        let got = hq(0.125, wellformed_vector());
        assert_eq!(got[0].1, 0.05);
    }

    #[test]
    fn quantile_top_of_range_is_the_highest_finite_bound() {
        let got = hq(1.0, wellformed_vector());
        assert_eq!(got[0].1, 1.0);
    }

    #[test]
    fn quantile_phi_out_of_range_returns_signed_infinity() {
        assert_eq!(hq(-1.0, wellformed_vector())[0].1, f64::NEG_INFINITY);
        assert_eq!(hq(2.0, wellformed_vector())[0].1, f64::INFINITY);
    }

    #[test]
    fn quantile_nan_phi_is_nan() {
        assert!(hq(f64::NAN, wellformed_vector())[0].1.is_nan());
    }

    #[test]
    fn quantile_missing_inf_bucket_is_nan() {
        let vector: InstantVector = wellformed_vector()
            .into_iter()
            .filter(|s| s.labels.get("le") != Some("+Inf"))
            .collect();
        let got = hq(0.5, vector);
        assert!(got[0].1.is_nan());
    }

    #[test]
    fn quantile_single_bucket_is_nan() {
        let vector = vec![sample(&[("le", "+Inf")], 10.0)];
        assert!(hq(0.5, vector)[0].1.is_nan());
    }

    #[test]
    fn quantile_clamps_a_non_monotonic_bucket_to_the_running_maximum() {
        let vector = vec![
            sample(&[("le", "0.1")], 10.0),
            sample(&[("le", "0.5")], 5.0), // dips below the 0.1 bucket
            sample(&[("le", "+Inf")], 20.0),
        ];
        // After the monotonicity fixup, the 0.5 bucket reads as 10 (clamped
        // up to the running max): rank 10 (phi=0.5 of a 20 total) exactly
        // matches the 0.1 bucket's own cumulative count, so it lands on
        // that bucket's own boundary rather than spilling into the next.
        let got = hq(0.5, vector);
        assert_eq!(got[0].1, 0.1);
    }

    #[test]
    fn quantile_unparseable_le_is_excluded() {
        let vector = vec![
            sample(&[("le", "not-a-number")], 10.0),
            sample(&[("le", "1")], 10.0),
            sample(&[("le", "+Inf")], 10.0),
        ];
        // Only two usable buckets remain, (1, count 10] and (+Inf, count
        // 10]: the whole mass is at or below 1, so phi=0.5 (rank 5)
        // interpolates halfway across the (0, 1] bucket.
        let got = hq(0.5, vector);
        assert_eq!(got[0].1, 0.5);
    }

    #[test]
    fn grouping_flags_a_missing_le_label() {
        // A series with no `le` label at all is not a classic bucket: it is
        // excluded and the bad-le-label flag is set so the caller warns.
        let vector = vec![
            sample(&[("job", "a")], 10.0),
            sample(&[("job", "a"), ("le", "1")], 10.0),
            sample(&[("job", "a"), ("le", "+Inf")], 10.0),
        ];
        let grouped = group_by_bucket(vector);
        assert!(grouped.bad_le_label, "missing le label sets the warn flag");
    }

    #[test]
    fn grouping_flags_an_unparseable_le_label() {
        // A present but non-numeric `le` label is likewise excluded and flags
        // the warning.
        let vector = vec![
            sample(&[("job", "a"), ("le", "not-a-number")], 10.0),
            sample(&[("job", "a"), ("le", "1")], 10.0),
            sample(&[("job", "a"), ("le", "+Inf")], 10.0),
        ];
        let grouped = group_by_bucket(vector);
        assert!(
            grouped.bad_le_label,
            "unparseable le label sets the warn flag"
        );
    }

    #[test]
    fn grouping_does_not_flag_a_wellformed_vector() {
        // Every series has a parseable `le`: no bad-le-label warning.
        let grouped = group_by_bucket(wellformed_vector());
        assert!(
            !grouped.bad_le_label,
            "a well-formed bucket set raises no bad-le-label warning"
        );
    }

    #[test]
    fn bad_bucket_label_warning_names_the_le_label() {
        // The message the caller raises when bad_le_label is set identifies
        // the offending label, matching Prometheus' BadBucketLabelWarning.
        assert!(bad_bucket_label_warning().contains("bucket label \"le\""));
    }

    #[test]
    fn fraction_full_range_is_one() {
        let got = hf(f64::NEG_INFINITY, f64::INFINITY, wellformed_vector());
        assert_eq!(got[0].1, 1.0);
    }

    #[test]
    fn fraction_empty_range_is_zero() {
        let got = hf(1.0, 1.0, wellformed_vector());
        assert_eq!(got[0].1, 0.0);

        let got = hf(2.0, 1.0, wellformed_vector());
        assert_eq!(got[0].1, 0.0);
    }

    #[test]
    fn fraction_is_the_inverse_of_quantile() {
        // The median (phi=0.5) computed above is exactly 0.5; the fraction
        // of observations at or below that same value should be 0.5.
        let quantile = hq(0.5, wellformed_vector())[0].1;
        let got = hf(f64::NEG_INFINITY, quantile, wellformed_vector());
        assert_eq!(got[0].1, 0.5);
    }

    #[test]
    fn fraction_nan_argument_is_nan() {
        assert!(hf(f64::NAN, 1.0, wellformed_vector())[0].1.is_nan());
        assert!(hf(0.0, f64::NAN, wellformed_vector())[0].1.is_nan());
    }

    #[test]
    fn fraction_missing_inf_bucket_is_nan() {
        let vector: InstantVector = wellformed_vector()
            .into_iter()
            .filter(|s| s.labels.get("le") != Some("+Inf"))
            .collect();
        let got = hf(0.0, 1.0, vector);
        assert!(got[0].1.is_nan());
    }

    #[test]
    fn no_bucket_series_yields_an_empty_vector() {
        assert!(hq(0.5, Vec::new()).is_empty());
        assert!(hf(0.0, 1.0, Vec::new()).is_empty());
    }
}
