//! Native (exponential) histogram value model and the Prometheus histogram
//! math the native-histogram function forms are built on (ADR-0017,
//! ADR-0021).
//!
//! [`FloatHistogram`] mirrors Prometheus' `histogram.FloatHistogram`: the
//! working value type every native-histogram PromQL operation reduces to.
//! Integer histograms are converted to this float form at the read boundary
//! (Prometheus does the same before evaluation), so the evaluator never sees
//! the `Int`/`Float` split that storage (`ravel_segment::HistogramCounts`)
//! keeps. Bucket counts are absolute (RW deltas are already resolved at
//! ingest), matching the segment model.
//!
//! ## Exactness status (read this before trusting the numbers)
//!
//! ADR-0021 requires mirroring Prometheus' float algorithms operation-for-
//! operation, verified by the differential gate. That differential gate
//! cannot run yet: no native-histogram sample can reach Ravel's evaluator,
//! because the query read path and the ingest path are still f64-only (see
//! the `histogram_native` corpus header). Every
//! algorithm here is therefore a structural port of Prometheus'
//! `model/histogram` and `promql/quantile.go`/`functions.go`, checked
//! against hand-computed fixtures rather than a live Prometheus. Two known
//! residues, to be resolved to ADR-0025 allowlist entries or exact ports once
//! the gate exists:
//!
//! * `bucket_bound` computes `2^(idx * 2^-scale)` arithmetically. Prometheus
//!   uses hard-coded `exponentialBounds` tables for `scale > 0` for bit-exact
//!   bounds; the arithmetic form may differ by a ULP for fractional
//!   exponents. `scale <= 0` bounds are exact powers of two and match.
//! * Differing zero-thresholds between two operands of `add`/`sub` are not
//!   reconciled (Prometheus' `reconcileZeroBuckets`); operands are required to
//!   share a zero-threshold. Every native-histogram code path that combines histograms
//!   (rate windows, aggregation groups) operates on one producer's series, so
//!   this holds in practice, but it is not the general Prometheus behavior.

use std::collections::BTreeMap;

/// Counter-reset provenance carried alongside a native histogram, mirroring
/// `ravel_segment::ResetHint` and Prometheus' `CounterResetHint`. Used by
/// [`FloatHistogram::detect_reset`] and set to [`ResetHint::Gauge`] on the
/// output of `rate`/`increase`/`delta` (Prometheus marks rate results as
/// gauge-typed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetHint {
    Unknown,
    Yes,
    No,
    Gauge,
}

/// A run of consecutive populated buckets on one side, mirroring
/// `ravel_segment::HistogramSpan` and Prometheus' `histogram.Span`. `offset`
/// is decoded exactly as Prometheus decodes it: a running bucket index starts
/// at 0, each span adds its `offset` to that index before emitting `length`
/// consecutive buckets, then advances one index per bucket. So the first
/// span's `offset` is the absolute index of its first bucket, and a later
/// span's `offset` is the gap since the previous span's end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub offset: i32,
    pub length: u32,
}

/// Prometheus' `histogram.FloatHistogram`, the working native-histogram value
/// for evaluation. Bucket vectors hold one absolute count per bucket declared
/// by the matching span list, in span order.
#[derive(Debug, Clone, PartialEq)]
pub struct FloatHistogram {
    pub counter_reset_hint: ResetHint,
    /// Prometheus `schema`. Base-2^(2^-scale) bucketing; `-53` selects custom
    /// boundaries carried in [`FloatHistogram::custom_values`].
    pub scale: i32,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    pub positive_spans: Vec<Span>,
    pub negative_spans: Vec<Span>,
    pub positive_buckets: Vec<f64>,
    pub negative_buckets: Vec<f64>,
    /// Ascending custom boundaries, present iff `scale == -53` (NHCB).
    pub custom_values: Vec<f64>,
}

/// The `scale` sentinel Prometheus uses for custom-boundary (NHCB)
/// histograms.
pub const CUSTOM_BUCKETS_SCALE: i32 = -53;

/// One native-histogram sample within a matrix window: its event timestamp
/// and value. The unit the histogram `rate`/`increase`/`delta` reducers
/// consume, named to keep those function-pointer signatures readable
/// (clippy `type_complexity`).
pub(crate) type TimedHistogram = (i64, FloatHistogram);

/// One cumulative-order bucket produced by [`FloatHistogram::all_buckets`]:
/// the half-open interval `(lower, upper]` and its absolute observation
/// count. Ordered ascending by `upper` across the whole value (negative side
/// most-negative-first, then the zero bucket, then the positive side).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket {
    pub lower: f64,
    pub upper: f64,
    pub count: f64,
}

impl FloatHistogram {
    /// Whether this value uses custom bucket boundaries (`scale == -53`).
    pub fn uses_custom_buckets(&self) -> bool {
        self.scale == CUSTOM_BUCKETS_SCALE
    }

    /// Multiply every population (buckets, zero, count) and the sum by
    /// `factor`, in place: Prometheus' `FloatHistogram.Mul`, used to apply
    /// rate's per-second / extrapolation factor and `avg`'s `1/n`. Order of
    /// operations mirrors Prometheus (zero, count, sum, then each bucket).
    pub fn mul(&mut self, factor: f64) {
        self.zero_count *= factor;
        self.count *= factor;
        self.sum *= factor;
        for b in &mut self.positive_buckets {
            *b *= factor;
        }
        for b in &mut self.negative_buckets {
            *b *= factor;
        }
    }

    /// Divide by `scalar` (Prometheus' `FloatHistogram.Div`): each population
    /// (zero, count, sum, and every bucket) is divided by `scalar` directly,
    /// field for field, exactly as Prometheus' `Div` does. This is NOT
    /// `mul(1/scalar)`: for a non-power-of-two divisor the two round
    /// differently, and only the direct division matches the pinned binary.
    /// Ravel's only callers pass a positive scalar (`avg`'s group size,
    /// `irate`'s sampled interval), so Prometheus' `scalar == 0`
    /// bucket-clearing and `scalar < 0` gauge-hint special cases are
    /// unreachable and not reproduced here.
    pub fn div(&mut self, scalar: f64) {
        self.zero_count /= scalar;
        self.count /= scalar;
        self.sum /= scalar;
        for b in &mut self.positive_buckets {
            *b /= scalar;
        }
        for b in &mut self.negative_buckets {
            *b /= scalar;
        }
    }

    /// Prometheus' `FloatHistogram.Equals`: *data* equality, not mathematical
    /// equality, so `count`/`sum`/`zero_count` and every bucket compare by bit
    /// pattern (`NaN`/`-0.0` are data-distinct), `scale`/`zero_threshold` by
    /// value, and the span layouts exactly. `counter_reset_hint` is
    /// deliberately not compared. Used by `changes` over native histograms,
    /// which counts a change whenever two adjacent samples are not `Equals`.
    pub fn equals(&self, other: &FloatHistogram) -> bool {
        if self.scale != other.scale
            || self.zero_threshold != other.zero_threshold
            || self.zero_count.to_bits() != other.zero_count.to_bits()
            || self.count.to_bits() != other.count.to_bits()
            || self.sum.to_bits() != other.sum.to_bits()
        {
            return false;
        }
        if self.uses_custom_buckets() != other.uses_custom_buckets() {
            return false;
        }
        if self.uses_custom_buckets()
            && !float_buckets_match(&self.custom_values, &other.custom_values)
        {
            return false;
        }
        self.positive_spans == other.positive_spans
            && self.negative_spans == other.negative_spans
            && float_buckets_match(&self.positive_buckets, &other.positive_buckets)
            && float_buckets_match(&self.negative_buckets, &other.negative_buckets)
    }

    /// `histogram_count`: the stored observation count.
    pub fn observation_count(&self) -> f64 {
        self.count
    }

    /// `histogram_sum`: the stored observation sum.
    pub fn observation_sum(&self) -> f64 {
        self.sum
    }

    /// Absolute bucket index -> count map for one side, decoding `spans`
    /// against `buckets` the way Prometheus decodes a span list.
    fn side_index_map(spans: &[Span], buckets: &[f64]) -> BTreeMap<i32, f64> {
        let mut map = BTreeMap::new();
        let mut bucket_pos = 0usize;
        let mut index = 0i32;
        for span in spans {
            index = index.saturating_add(span.offset);
            for _ in 0..span.length {
                if let Some(&count) = buckets.get(bucket_pos) {
                    map.insert(index, count);
                }
                bucket_pos += 1;
                index = index.saturating_add(1);
            }
        }
        map
    }

    fn positive_index_map(&self) -> BTreeMap<i32, f64> {
        Self::side_index_map(&self.positive_spans, &self.positive_buckets)
    }

    fn negative_index_map(&self) -> BTreeMap<i32, f64> {
        Self::side_index_map(&self.negative_spans, &self.negative_buckets)
    }

    /// The upper bound of positive bucket index `idx` (its lower bound is the
    /// upper bound of `idx - 1`). Prometheus' `getBound` for the exponential
    /// schemas; see the module doc's exactness note. For custom buckets the
    /// bound is the `idx`-th boundary (1-based, Prometheus' NHCB layout).
    fn bucket_bound(&self, idx: i32) -> f64 {
        if self.uses_custom_buckets() {
            // NHCB: bucket i (1-based) has upper bound custom_values[i-1];
            // the last bucket runs to +Inf.
            let i = idx as usize;
            if i == 0 {
                return f64::NEG_INFINITY;
            }
            return self
                .custom_values
                .get(i - 1)
                .copied()
                .unwrap_or(f64::INFINITY);
        }
        two_pow_scaled(idx, self.scale)
    }

    /// Every populated (and, on the interior, empty) bucket in cumulative
    /// ascending order, exactly the order Prometheus' `AllFloatBucketIterator`
    /// yields: negative buckets most-negative first, the zero bucket, then
    /// positive buckets. Empty buckets are omitted (callers skip zero-count
    /// buckets anyway); the zero bucket is emitted only when it has count.
    pub fn all_buckets(&self) -> Vec<Bucket> {
        let mut out = Vec::new();

        // Negative side, most negative first: bucket index i covers
        // (-bound(i), -bound(i-1)]. Ascending value order is descending index.
        let neg = self.negative_index_map();
        for (&idx, &count) in neg.iter().rev() {
            out.push(Bucket {
                lower: -self.bucket_bound(idx),
                upper: -self.bucket_bound(idx - 1),
                count,
            });
        }

        // Zero bucket.
        if self.zero_count != 0.0 {
            out.push(Bucket {
                lower: -self.zero_threshold,
                upper: self.zero_threshold,
                count: self.zero_count,
            });
        }

        // Positive side, ascending index: bucket index i covers
        // (bound(i-1), bound(i)].
        let pos = self.positive_index_map();
        for (&idx, &count) in pos.iter() {
            out.push(Bucket {
                lower: self.bucket_bound(idx - 1),
                upper: self.bucket_bound(idx),
                count,
            });
        }

        out
    }

    /// Detect whether `self` is a counter reset relative to `prev`,
    /// Prometheus' `FloatHistogram.DetectReset`: an explicit hint decides when
    /// present, otherwise any shrink in count, zero-count, or a per-bucket
    /// population is a reset. Used by the counter-reset compensation loop in
    /// [`histogram_rate`].
    pub fn detect_reset(&self, prev: &FloatHistogram) -> bool {
        match self.counter_reset_hint {
            ResetHint::Gauge => false,
            ResetHint::Yes => true,
            ResetHint::No => false,
            ResetHint::Unknown => {
                if self.count < prev.count || self.zero_count < prev.zero_count {
                    return true;
                }
                // A schema change or a shrink in any bucket population is a
                // reset. Comparing by absolute bucket index handles differing
                // span layouts.
                if self.scale != prev.scale {
                    return true;
                }
                bucket_shrank(&prev.positive_index_map(), &self.positive_index_map())
                    || bucket_shrank(&prev.negative_index_map(), &self.negative_index_map())
            }
        }
    }

    /// `self - other`, in place, for the two histograms sharing a schema and
    /// zero-threshold (Prometheus' `FloatHistogram.Sub` for the aligned case).
    /// Rebuilds spans from the merged index maps; the resulting per-bucket
    /// values are the exact float differences (`self_count - other_count` per
    /// index), which is what every downstream value comparison reads.
    pub fn sub_assign(&mut self, other: &FloatHistogram) {
        self.combine(other, -1.0);
    }

    /// `self + other`, in place (Prometheus' `FloatHistogram.Add`).
    pub fn add_assign(&mut self, other: &FloatHistogram) {
        self.combine(other, 1.0);
    }

    fn combine(&mut self, other: &FloatHistogram, sign: f64) {
        self.zero_count += sign * other.zero_count;
        self.count += sign * other.count;
        self.sum += sign * other.sum;

        let pos = combine_side(
            &self.positive_index_map(),
            &other.positive_index_map(),
            sign,
        );
        let neg = combine_side(
            &self.negative_index_map(),
            &other.negative_index_map(),
            sign,
        );
        let (ps, pb) = rebuild_side(&pos);
        let (ns, nb) = rebuild_side(&neg);
        self.positive_spans = ps;
        self.positive_buckets = pb;
        self.negative_spans = ns;
        self.negative_buckets = nb;
    }

    /// Down-convert to `target_scale` (`target_scale <= self.scale`) by
    /// merging each group of `2^(scale - target_scale)` adjacent buckets into
    /// one, Prometheus' `FloatHistogram.CopyToSchema`. Bucket bounds stay a
    /// subset, so quantiles computed after are on coarser buckets exactly as
    /// Prometheus does when a rate window mixes schemas. A no-op when the
    /// scales already match. Custom-bucket histograms cannot be rescaled and
    /// are returned unchanged.
    pub fn copy_to_scale(&self, target_scale: i32) -> FloatHistogram {
        if target_scale >= self.scale || self.uses_custom_buckets() {
            return self.clone();
        }
        let shift = self.scale - target_scale;
        let merge = |map: &BTreeMap<i32, f64>| -> BTreeMap<i32, f64> {
            let mut out: BTreeMap<i32, f64> = BTreeMap::new();
            for (&idx, &count) in map {
                // Prometheus maps bucket idx at schema s to
                // ((idx - 1) >> shift) + 1 at the coarser schema, so the
                // upper bounds of the merged buckets stay a subset.
                let merged = ((idx - 1) >> shift) + 1;
                *out.entry(merged).or_insert(0.0) += count;
            }
            out
        };
        let pos = merge(&self.positive_index_map());
        let neg = merge(&self.negative_index_map());
        let (ps, pb) = rebuild_side(&pos);
        let (ns, nb) = rebuild_side(&neg);
        FloatHistogram {
            counter_reset_hint: self.counter_reset_hint,
            scale: target_scale,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count,
            count: self.count,
            sum: self.sum,
            positive_spans: ps,
            negative_spans: ns,
            positive_buckets: pb,
            negative_buckets: nb,
            custom_values: Vec::new(),
        }
    }
}

/// True if any bucket present in `prev` shrank (or vanished) in `curr`: the
/// per-bucket half of Prometheus' reset detection.
fn bucket_shrank(prev: &BTreeMap<i32, f64>, curr: &BTreeMap<i32, f64>) -> bool {
    for (idx, &prev_count) in prev {
        let curr_count = curr.get(idx).copied().unwrap_or(0.0);
        if curr_count < prev_count {
            return true;
        }
    }
    false
}

/// Per-element bit-pattern equality of two bucket (or custom-value) vectors,
/// Prometheus' `floatBucketsMatch`: same length and every element equal by
/// `f64::to_bits`, so `NaN`/`-0.0` are data-distinct, matching Prometheus'
/// data-equality contract for [`FloatHistogram::equals`].
fn float_buckets_match(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

/// Merge two side index maps with `self + sign*other` per bucket, keeping a
/// bucket whenever either side has it.
fn combine_side(a: &BTreeMap<i32, f64>, b: &BTreeMap<i32, f64>, sign: f64) -> BTreeMap<i32, f64> {
    let mut out = a.clone();
    for (&idx, &count) in b {
        *out.entry(idx).or_insert(0.0) += sign * count;
    }
    out
}

/// Rebuild a span list + bucket vector from an absolute index -> count map,
/// one span per maximal run of consecutive indices. Empty (absent) indices
/// become span gaps; a zero-valued but present index stays an explicit
/// bucket (it can arise from a subtraction that cancels exactly, and
/// Prometheus keeps such buckets until an explicit compaction).
fn rebuild_side(map: &BTreeMap<i32, f64>) -> (Vec<Span>, Vec<f64>) {
    let mut spans: Vec<Span> = Vec::new();
    let mut buckets: Vec<f64> = Vec::new();
    let mut prev_index: Option<i32> = None;
    let mut running_index = 0i32;
    for (&idx, &count) in map {
        match prev_index {
            None => {
                spans.push(Span {
                    offset: idx,
                    length: 1,
                });
                running_index = idx;
            }
            Some(prev) if idx == prev + 1 => {
                if let Some(last) = spans.last_mut() {
                    last.length += 1;
                }
                running_index = idx;
            }
            Some(_) => {
                spans.push(Span {
                    offset: idx - running_index - 1,
                    length: 1,
                });
                running_index = idx;
            }
        }
        buckets.push(count);
        prev_index = Some(idx);
    }
    (spans, buckets)
}

/// `2^(idx * 2^-scale)`. Exact for `scale <= 0` (integer exponent, a power of
/// two); arithmetic (possible sub-ULP residue) for `scale > 0`. See the
/// module exactness note.
fn two_pow_scaled(idx: i32, scale: i32) -> f64 {
    if scale <= 0 {
        // Exponent is idx << (-scale), an exact integer; 2^e is exact where
        // representable. Compute in i64 to avoid shifting overflow for the
        // extreme indices the +Inf bucket boundary can reach.
        let exp = i64::from(idx) << (-scale);
        two_pow_int(exp)
    } else {
        let exponent = f64::from(idx) / f64::from(1i32 << scale);
        exp2(exponent)
    }
}

/// `2^exp` for an integer `exp`, saturating to 0 / +Inf outside the f64
/// exponent range rather than producing a denormal-rounding surprise.
fn two_pow_int(exp: i64) -> f64 {
    if exp > 1023 {
        return f64::INFINITY;
    }
    if exp < -1074 {
        return 0.0;
    }
    // powi is exact for a power of two within the normal range.
    2.0f64.powi(exp as i32)
}

/// `2^x`. `f64::exp2` where available via the standard library's `powf`
/// surrogate; kept in one place so the rounding choice is explicit.
fn exp2(x: f64) -> f64 {
    2.0f64.powf(x)
}

/// Prometheus `histogramQuantile` over one native histogram (promql/
/// quantile.go). `phi` argument edges are handled by the caller; this assumes
/// `0 <= phi <= 1` and a non-NaN phi. Returns `NaN` for an empty histogram.
pub fn histogram_quantile(phi: f64, h: &FloatHistogram) -> f64 {
    if h.count == 0.0 {
        return f64::NAN;
    }
    let buckets = h.all_buckets();
    if buckets.is_empty() {
        return f64::NAN;
    }
    let rank = phi * h.count;
    let mut cumulative = 0.0f64;
    let mut chosen = buckets[buckets.len() - 1];
    let mut chosen_is_last = true;
    for b in &buckets {
        if b.count == 0.0 {
            continue;
        }
        cumulative += b.count;
        if cumulative >= rank {
            chosen = *b;
            chosen_is_last = false;
            break;
        }
    }
    if chosen_is_last {
        // rank fell past the last populated bucket (float slack): its upper
        // bound is the answer, matching Prometheus returning bucket.Upper.
        return chosen.upper;
    }

    // Rank position within the chosen bucket.
    let count_before = cumulative - chosen.count;
    let mut lower = chosen.lower;
    let mut upper = chosen.upper;
    // A bucket straddling zero interpolates from the nearer signed zero-
    // threshold edge, matching Prometheus' zero-bucket adjustment.
    if !h.uses_custom_buckets() && lower < 0.0 && upper > 0.0 {
        if h.negative_buckets.is_empty() && !h.positive_buckets.is_empty() {
            lower = 0.0;
        } else if h.positive_buckets.is_empty() && !h.negative_buckets.is_empty() {
            upper = 0.0;
        }
    }
    if upper.is_infinite() {
        return lower;
    }
    if lower.is_infinite() {
        return upper;
    }
    let within = (rank - count_before) / chosen.count;
    interpolate_value(lower, upper, within, h.uses_custom_buckets())
}

/// The value at fractional position `within` (in `0..=1`) inside a bucket
/// `(lower, upper]`, the inverse of [`bucket_fraction`]. Native-histogram
/// buckets have exponentially spaced boundaries, so Prometheus places the
/// interpolated point on a log scale within the bucket
/// (`promql/quantile.go`): `lower * (upper/lower)^within`. Custom-boundary
/// (NHCB) buckets and any bucket with a zero or sign-crossing edge (the zero
/// bucket after the caller's straddle adjustment) fall back to linear, exactly
/// as Prometheus does.
fn interpolate_value(lower: f64, upper: f64, within: f64, custom: bool) -> f64 {
    if is_exponential_bucket(lower, upper, custom) {
        lower * (upper / lower).powf(within)
    } else {
        lower + (upper - lower) * within
    }
}

/// The fraction (in `0..=1`) of a bucket `(lower, upper]` that lies at or below
/// `value`, the inverse of [`interpolate_value`]. Exponential (log-scale) share
/// on native buckets, linear on custom/zero-straddling ones, matching
/// Prometheus' `histogramFraction`.
fn bucket_fraction(lower: f64, upper: f64, value: f64, custom: bool) -> f64 {
    if is_exponential_bucket(lower, upper, custom) {
        (value / lower).ln() / (upper / lower).ln()
    } else {
        let span = upper - lower;
        if span > 0.0 {
            (value - lower) / span
        } else {
            0.0
        }
    }
}

/// Whether a bucket's interior is interpolated on an exponential (log) scale:
/// a native-histogram bucket (`!custom`) whose bounds are finite, nonzero, and
/// share a sign. The zero bucket (a sign-crossing edge, or a `0` edge after
/// the one-sided straddle adjustment) and custom NHCB buckets are linear.
fn is_exponential_bucket(lower: f64, upper: f64, custom: bool) -> bool {
    !custom
        && lower.is_finite()
        && upper.is_finite()
        && lower != 0.0
        && upper != 0.0
        && lower.signum() == upper.signum()
}

/// Prometheus `histogramFraction` over one native histogram: the estimated
/// fraction of observations in `(lower, upper)`, interpolating the partial
/// buckets exponentially (native buckets) or linearly (custom/zero buckets).
/// Argument edges (`NaN`, inverted range) are handled by the caller.
pub fn histogram_fraction(lower: f64, upper: f64, h: &FloatHistogram) -> f64 {
    if h.count == 0.0 || lower >= upper {
        return 0.0;
    }
    let rank_at = |value: f64| -> f64 { fraction_rank(value, h) };
    ((rank_at(upper) - rank_at(lower)) / h.count).clamp(0.0, 1.0)
}

/// Cumulative observation count at `value`, interpolating within the owning
/// bucket, the inverse of the quantile interpolation (Prometheus'
/// `histogramFraction` accumulation).
fn fraction_rank(value: f64, h: &FloatHistogram) -> f64 {
    if value == f64::INFINITY {
        return h.count;
    }
    if value == f64::NEG_INFINITY {
        return 0.0;
    }
    let custom = h.uses_custom_buckets();
    let mut cumulative = 0.0f64;
    for b in h.all_buckets() {
        let mut lower = b.lower;
        let mut upper = b.upper;
        // Same zero-straddle adjustment as `histogram_quantile`: a one-sided
        // histogram's zero bucket is bounded at 0 on the populated side, so a
        // query value of exactly 0 sits on the bucket edge and contributes
        // none of the zero-bucket count, matching Prometheus.
        if !custom && lower < 0.0 && upper > 0.0 {
            if h.negative_buckets.is_empty() && !h.positive_buckets.is_empty() {
                lower = 0.0;
            } else if h.positive_buckets.is_empty() && !h.negative_buckets.is_empty() {
                upper = 0.0;
            }
        }
        if value >= upper {
            cumulative += b.count;
            continue;
        }
        if value <= lower {
            break;
        }
        // Partial bucket: exponential share on native buckets, linear on
        // custom/zero-straddling ones (the inverse of the quantile
        // interpolation).
        cumulative += b.count * bucket_fraction(lower, upper, value, custom);
        break;
    }
    cumulative
}

/// Prometheus `histogramRate` (promql/functions.go): reduce a window of
/// native-histogram samples to one histogram. `is_counter` enables the
/// counter-reset compensation loop; a gauge (`delta`) skips it. The extrapo-
/// lation/per-second scaling factor is applied by the caller, exactly as
/// Prometheus applies it in `extrapolatedRate` after `histogramRate` returns.
/// Returns `None` for fewer than two samples (no defined rate), matching the
/// float path, and also for a window mixing an exponential-schema sample
/// with a custom-buckets one: the custom-buckets sentinel scale sits far
/// outside the exponential range, and rescaling an exponential operand
/// toward it overflows `FloatHistogram::copy_to_scale`'s bucket-index shift
/// (issue #678). Prometheus does not consider the two comparable at all, so
/// refusing to combine them is conservative rather than a guessed
/// approximation of its exact annotation for this window shape (unverified:
/// the difftest generator cannot emit `custom_values`, so this case has no
/// oracle-checkable fixture).
pub fn histogram_rate(samples: &[TimedHistogram], is_counter: bool) -> Option<FloatHistogram> {
    if samples.len() < 2 {
        return None;
    }
    let uses_custom = samples[0].1.uses_custom_buckets();
    if samples
        .iter()
        .any(|(_, h)| h.uses_custom_buckets() != uses_custom)
    {
        return None;
    }
    let prev0 = &samples[0].1;
    let last = &samples[samples.len() - 1].1;

    // Coarsest schema across the window: down-convert so bucket bounds are a
    // common subset (Prometheus' minSchema pass).
    let mut min_scale = prev0.scale.min(last.scale);
    for (_, h) in &samples[1..samples.len() - 1] {
        min_scale = min_scale.min(h.scale);
    }

    let mut result = last.copy_to_scale(min_scale);
    result.sub_assign(&prev0.copy_to_scale(min_scale));

    if is_counter {
        let mut prev = prev0.clone();
        for (_, curr) in &samples[1..] {
            if curr.detect_reset(&prev) {
                result.add_assign(&prev.copy_to_scale(min_scale));
            }
            prev = curr.clone();
        }
    }

    result.counter_reset_hint = ResetHint::Gauge;
    Some(result)
}

/// Sum a group of native histograms into one, Prometheus' aggregation `sum`
/// over histogram samples: fold with [`FloatHistogram::add_assign`] from the
/// first member (down-converting each addend to the running schema so the
/// bounds stay aligned). Returns `None` for an empty group, or for a group
/// mixing an exponential-schema member with a custom-buckets one -- the same
/// unrescalable-schema-sentinel overflow [`histogram_rate`] guards against
/// (issue #678), unverified here for the same reason (no generator support
/// for `custom_values` fixtures).
pub fn sum_histograms<'a>(
    mut group: impl Iterator<Item = &'a FloatHistogram>,
) -> Option<FloatHistogram> {
    let first = group.next()?;
    let uses_custom = first.uses_custom_buckets();
    let mut acc = first.clone();
    for h in group {
        if h.uses_custom_buckets() != uses_custom {
            return None;
        }
        let scale = acc.scale.min(h.scale);
        if scale < acc.scale {
            acc = acc.copy_to_scale(scale);
        }
        acc.add_assign(&h.copy_to_scale(scale));
    }
    acc.counter_reset_hint = ResetHint::Gauge;
    Some(acc)
}

/// Average a group of native histograms: `sum / n` (Prometheus' `avg` over
/// histograms is `sum` then `Div(count)`). Returns `None` for an empty group.
pub fn avg_histograms<'a>(
    group: impl Iterator<Item = &'a FloatHistogram> + Clone,
) -> Option<FloatHistogram> {
    let n = group.clone().count();
    if n == 0 {
        return None;
    }
    let mut acc = sum_histograms(group)?;
    acc.div(n as f64);
    Some(acc)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// A simple positive-only histogram at scale 0: one span starting at
    /// index 1, `counts` consecutive buckets. Bucket i (1-based) upper bound
    /// is 2^i at scale 0.
    fn positive(scale: i32, first_index: i32, counts: &[f64], sum: f64) -> FloatHistogram {
        let total: f64 = counts.iter().sum();
        FloatHistogram {
            counter_reset_hint: ResetHint::Unknown,
            scale,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: total,
            sum,
            positive_spans: vec![Span {
                offset: first_index,
                length: counts.len() as u32,
            }],
            negative_spans: Vec::new(),
            positive_buckets: counts.to_vec(),
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        }
    }

    #[test]
    fn count_and_sum_are_field_reads() {
        let h = positive(0, 1, &[1.0, 2.0, 3.0], 42.0);
        assert_eq!(h.observation_count(), 6.0);
        assert_eq!(h.observation_sum(), 42.0);
    }

    #[test]
    fn bucket_bounds_at_scale_zero_are_powers_of_two() {
        let h = positive(0, 1, &[1.0], 0.0);
        // Index 1 upper bound 2^1 = 2, lower bound 2^0 = 1.
        assert_eq!(h.bucket_bound(0), 1.0);
        assert_eq!(h.bucket_bound(1), 2.0);
        assert_eq!(h.bucket_bound(3), 8.0);
    }

    #[test]
    fn negative_scale_bounds_are_exact() {
        // scale -1: base = 2^2 = 4. Index 1 upper bound = 4.
        let h = positive(-1, 1, &[1.0], 0.0);
        assert_eq!(h.bucket_bound(1), 4.0);
        assert_eq!(h.bucket_bound(2), 16.0);
    }

    #[test]
    fn all_buckets_ascending_with_zero_and_negative() {
        let mut h = positive(0, 1, &[4.0], 0.0); // (1,2] count 4
        h.zero_threshold = 0.5;
        h.zero_count = 2.0;
        h.count += 2.0;
        h.negative_spans = vec![Span {
            offset: 1,
            length: 1,
        }];
        h.negative_buckets = vec![3.0]; // (-2,-1] count 3
        h.count += 3.0;
        let buckets = h.all_buckets();
        // Ascending: (-2,-1] c3, (-0.5,0.5] c2, (1,2] c4.
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].lower, -2.0);
        assert_eq!(buckets[0].upper, -1.0);
        assert_eq!(buckets[0].count, 3.0);
        assert_eq!(buckets[1].lower, -0.5);
        assert_eq!(buckets[1].upper, 0.5);
        assert_eq!(buckets[2].upper, 2.0);
    }

    /// Build the `diff_native_hist` value at +0s: schema 0,
    /// zero_threshold 1e-9, zero_count 1, positive buckets [2,3,1] from index
    /// 1. Cumulative buckets: zero 1, (1,2] 2, (2,4] 3, (4,8] 1; total 7.
    fn diff_native_hist() -> FloatHistogram {
        let mut h = positive(0, 1, &[2.0, 3.0, 1.0], 0.0);
        h.zero_threshold = 1e-9;
        h.zero_count = 1.0;
        h.count += 1.0;
        h
    }

    #[test]
    fn quantile_interpolates_within_the_owning_bucket() {
        // Buckets (1,2]:2, (2,4]:2, total 4. Median rank 2 lands at the end
        // of the first bucket -> upper bound 2.
        let h = positive(0, 1, &[2.0, 2.0], 0.0);
        assert_eq!(histogram_quantile(0.5, &h), 2.0);
        // phi 0.25 -> rank 1, the log-scale midpoint of (1,2]: native buckets
        // interpolate exponentially, so 1 * (2/1)^0.5 = sqrt(2), not 1.5.
        let got = histogram_quantile(0.25, &h);
        assert!((got - 2.0f64.sqrt()).abs() < 1e-12, "got {got}");
    }

    #[test]
    fn quantile_interpolates_exponentially_within_native_bucket() {
        // Median rank 3.5 lands in (2,4] at within
        // 1/6. Prometheus interpolates exponentially: 2*(4/2)^(1/6) =
        // 2*2^(1/6) = 2.244..., not the old linear 2 + 2/6 = 2.333....
        let h = diff_native_hist();
        let got = histogram_quantile(0.5, &h);
        let want = 2.0 * 2.0f64.powf(1.0 / 6.0);
        assert!(
            (got - want).abs() < 1e-12,
            "exponential median: got {got}, want {want}"
        );
        // The regressed linear formula would have returned 2 + 2/6.
        assert!(
            (got - (2.0 + 2.0 / 6.0)).abs() > 1e-6,
            "should not be the linear value: got {got}"
        );
    }

    #[test]
    fn fraction_matches_prometheus_zero_bucket_and_exponential() {
        // histogram_fraction(0, 4, diff_native_hist).
        // rank_at(4) = 6 (through (2,4]); rank_at(0) = 0 because the one-sided
        // zero bucket is bounded at 0 on the populated side. => 6/7, matching
        // Prometheus, not Ravel's old linear 5.5/7.
        let h = diff_native_hist();
        let got = histogram_fraction(0.0, 4.0, &h);
        assert!((got - 6.0 / 7.0).abs() < 1e-12, "got {got}");
    }

    #[test]
    fn quantile_of_empty_histogram_is_nan() {
        let h = positive(0, 1, &[0.0], 0.0);
        assert!(histogram_quantile(0.5, &h).is_nan());
    }

    #[test]
    fn fraction_is_consistent_with_quantile() {
        let h = positive(0, 1, &[2.0, 2.0], 0.0);
        // Half the mass is at or below 2.0 (the median).
        assert_eq!(histogram_fraction(f64::NEG_INFINITY, 2.0, &h), 0.5);
        assert_eq!(
            histogram_fraction(f64::NEG_INFINITY, f64::INFINITY, &h),
            1.0
        );
    }

    #[test]
    fn sub_of_identical_histograms_is_empty_populations() {
        let a = positive(0, 1, &[2.0, 3.0], 10.0);
        let mut r = a.clone();
        r.sub_assign(&a);
        assert_eq!(r.count, 0.0);
        assert_eq!(r.sum, 0.0);
        assert!(r.positive_buckets.iter().all(|&c| c == 0.0));
    }

    #[test]
    fn add_then_sub_round_trips_counts() {
        let a = positive(0, 1, &[2.0, 3.0], 10.0);
        let b = positive(0, 2, &[5.0], 7.0); // overlaps index 2, adds index 3
        let mut r = a.clone();
        r.add_assign(&b);
        r.sub_assign(&b);
        assert_eq!(r.count, a.count);
        // Index 3 was introduced by b then cancelled to 0; it survives as an
        // explicit zero bucket (pre-compaction), so the count map matches a's
        // populations plus a zero.
        let map = r.positive_index_map();
        assert_eq!(map.get(&1), Some(&2.0));
        assert_eq!(map.get(&2), Some(&3.0));
    }

    #[test]
    fn detect_reset_on_count_shrink() {
        let prev = positive(0, 1, &[5.0], 0.0);
        let curr = positive(0, 1, &[3.0], 0.0);
        assert!(curr.detect_reset(&prev));
        assert!(!prev.detect_reset(&curr));
    }

    #[test]
    fn detect_reset_respects_explicit_hint() {
        let mut curr = positive(0, 1, &[3.0], 0.0);
        let prev = positive(0, 1, &[5.0], 0.0);
        curr.counter_reset_hint = ResetHint::No;
        assert!(
            !curr.detect_reset(&prev),
            "explicit No overrides the shrink"
        );
        curr.counter_reset_hint = ResetHint::Yes;
        let same = positive(0, 1, &[10.0], 0.0);
        assert!(curr.detect_reset(&same), "explicit Yes forces a reset");
    }

    #[test]
    fn rate_of_monotonic_window_is_the_difference() {
        // Two samples, counts 2 then 6 in bucket (1,2]. No reset. rate window
        // reduction (before the caller's per-second factor) is the plain
        // difference: bucket count 4, total count 4.
        let a = positive(0, 1, &[2.0], 4.0);
        let b = positive(0, 1, &[6.0], 12.0);
        let r = histogram_rate(&[(0, a), (60_000_000_000, b)], true).expect("2 samples");
        assert_eq!(r.count, 4.0);
        assert_eq!(r.sum, 8.0);
        assert_eq!(r.counter_reset_hint, ResetHint::Gauge);
    }

    #[test]
    fn rate_compensates_a_counter_reset() {
        // counts 10 -> 2 (reset) -> 5. Raw last-first = 5-10 = -5; reset at
        // the middle sample adds back prev (10): net count 5.
        let a = positive(0, 1, &[10.0], 100.0);
        let b = positive(0, 1, &[2.0], 20.0);
        let c = positive(0, 1, &[5.0], 50.0);
        let r = histogram_rate(&[(0, a), (60_000_000_000, b), (120_000_000_000, c)], true)
            .expect("3 samples");
        // last(5) - first(10) = -5; + reset compensation prev(10) = 5.
        assert_eq!(r.count, 5.0);
    }

    #[test]
    fn sum_histograms_adds_populations() {
        let a = positive(0, 1, &[1.0, 2.0], 3.0);
        let b = positive(0, 1, &[4.0, 5.0], 9.0);
        let list = [a, b];
        let r = sum_histograms(list.iter()).expect("non-empty");
        assert_eq!(r.count, 12.0);
        assert_eq!(r.sum, 12.0);
        let map = r.positive_index_map();
        assert_eq!(map.get(&1), Some(&5.0));
        assert_eq!(map.get(&2), Some(&7.0));
    }

    #[test]
    fn avg_histograms_divides_by_count() {
        let a = positive(0, 1, &[2.0], 4.0);
        let b = positive(0, 1, &[4.0], 8.0);
        let list = [a, b];
        let r = avg_histograms(list.iter()).expect("non-empty");
        assert_eq!(r.count, 3.0);
        assert_eq!(r.sum, 6.0);
    }

    #[test]
    fn div_divides_each_field_directly_not_via_reciprocal() {
        // Dividing by a non-power-of-two (3) is where direct division and
        // `mul(1/3)` diverge in the last place. Prometheus' `Div` divides
        // each field directly; the result must match `x / 3.0`, bit for bit,
        // NOT `x * (1.0 / 3.0)`.
        let mut h = positive(0, 1, &[10.0, 20.0], 30.0);
        h.zero_count = 5.0;
        h.count += 5.0; // 35 total
        h.div(3.0);
        assert_eq!(h.count.to_bits(), (35.0_f64 / 3.0).to_bits());
        assert_eq!(h.sum.to_bits(), (30.0_f64 / 3.0).to_bits());
        assert_eq!(h.zero_count.to_bits(), (5.0_f64 / 3.0).to_bits());
        assert_eq!(h.positive_buckets[0].to_bits(), (10.0_f64 / 3.0).to_bits());
        assert_eq!(h.positive_buckets[1].to_bits(), (20.0_f64 / 3.0).to_bits());
    }

    #[test]
    fn equals_ignores_counter_reset_hint_but_compares_populations() {
        let a = positive(0, 1, &[1.0, 2.0], 3.0);
        let mut b = a.clone();
        b.counter_reset_hint = ResetHint::Gauge;
        assert!(
            a.equals(&b),
            "counter_reset_hint is not part of data equality"
        );

        let c = positive(0, 1, &[1.0, 3.0], 3.0);
        assert!(!a.equals(&c), "a differing bucket population is not equal");

        let d = positive(0, 1, &[1.0, 2.0], 4.0);
        assert!(!a.equals(&d), "a differing sum is not equal");
    }

    #[test]
    fn copy_to_scale_merges_adjacent_buckets() {
        // scale 1 -> scale 0 merges pairs. Buckets at scale 1 indices 1,2
        // both fall into scale 0 index 1.
        let h = positive(1, 1, &[3.0, 4.0], 0.0);
        let coarse = h.copy_to_scale(0);
        assert_eq!(coarse.scale, 0);
        let map = coarse.positive_index_map();
        assert_eq!(map.get(&1), Some(&7.0));
    }

    /// A one-bucket custom-buckets (NHCB) histogram: `scale: -53` is
    /// Prometheus' sentinel for custom boundaries.
    fn custom_buckets(count: f64, sum: f64) -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: ResetHint::Unknown,
            scale: -53,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: Vec::new(),
            positive_buckets: vec![count],
            negative_buckets: Vec::new(),
            custom_values: vec![1.0, 2.0],
        }
    }

    #[test]
    fn histogram_rate_over_mismatched_exponential_and_custom_schemas_does_not_panic() {
        // Before #678's fix, min_scale was computed unconditionally from the
        // custom-buckets sentinel scale, and copy_to_scale's bucket-index
        // shift overflowed. The flipped behavior: None instead of a panic.
        let a = custom_buckets(3.0, 6.0);
        let b = positive(0, 0, &[10.0], 20.0);
        assert_eq!(
            histogram_rate(&[(0, a), (30_000_000_000, b)], true),
            None,
            "a schema-type mismatch is not combinable, not a panic"
        );
    }

    #[test]
    fn sum_histograms_over_mismatched_exponential_and_custom_schemas_does_not_panic() {
        let a = positive(0, 0, &[10.0], 20.0);
        let b = custom_buckets(3.0, 6.0);
        assert_eq!(
            sum_histograms([&a, &b].into_iter()),
            None,
            "a schema-type mismatch is not combinable, not a panic"
        );
    }
}
