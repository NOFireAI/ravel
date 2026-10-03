//! The `GroupValues` under-count compensation (issue #740, findings 2 and 3).
//!
//! DataFusion 54's `GroupValues::size()` reports `capacity() * entry_size`,
//! where `capacity()` is the hashbrown table's usable slot count at the 7/8
//! load factor and no control bytes are counted. The real allocation is the
//! full bucket count (`capacity / (7/8)`, a power of two) times the entry
//! size, plus one control byte per bucket and the group width. For an Int64
//! group table that gap is about 21%, which the #740 trace saw as 570 MB real
//! against 470 MB reported for 17M groups.
//!
//! On top of that steady gap, a table that is mid-resize holds the old and the
//! new allocation at once (finding 3), a transient bounded at 1.5x the settled
//! real size. [`compensated_group_values_ceiling`] folds both into one factor
//! and this test pins that the compensated ceiling bounds the real allocation,
//! red with the factor at 1.0.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_sql::{
    GROUP_VALUES_CEILING_COMPENSATION, GROUP_VALUES_RESIZE_TRANSIENT_FACTOR,
    compensated_group_values_ceiling,
};

/// Bytes a `GroupValues` slot occupies for an Int64 group key: the 8-byte
/// value plus the 8-byte group index the hashbrown table stores. This is the
/// `entry_size` `GroupValues::size()` multiplies `capacity()` by.
const ENTRY: usize = 16;

/// One control byte per bucket plus the SIMD group width hashbrown allocates
/// alongside the buckets; `size()` counts none of it.
const GROUP_CTRL_WIDTH: usize = 16;

/// The reported figure (`GroupValues::size()`): usable capacity times entry
/// size. `capacity == buckets * 7 / 8`.
fn reported_size(buckets: usize) -> usize {
    (buckets * 7 / 8) * ENTRY
}

/// The real allocation the #740 trace measured: every bucket, plus one control
/// byte per bucket, plus the group width.
fn real_size(buckets: usize) -> usize {
    buckets * ENTRY + buckets + GROUP_CTRL_WIDTH
}

/// The compensated ceiling bounds the real steady allocation of an Int64 group
/// table, and the reported figure alone (the factor-1.0 case) does not.
#[test]
fn the_compensated_ceiling_bounds_the_real_group_values_allocation() {
    // 2^25 buckets: usable 29,360,128 slots, the settled table behind the
    // #740 trace's 17M-group aggregate (reported ~470 MB, real ~570 MB).
    let buckets = 1usize << 25;
    let reported = reported_size(buckets);
    let real = real_size(buckets);

    // Sanity-anchor against the trace's decimal-MB figures.
    assert_eq!(reported, 469_762_048, "reported ~470 MB");
    assert_eq!(real, 570_425_360, "real ~570 MB");

    // The under-count ratio, pinned to two decimals (finding 2).
    let ratio = real as f64 / reported as f64;
    assert_eq!(
        (ratio * 100.0).round() / 100.0,
        1.21,
        "GroupValues::size() under-reports the real allocation by ~21%; got {ratio}"
    );

    // The fix: the compensated ceiling is at least the real allocation.
    let compensated = compensated_group_values_ceiling(reported);
    assert!(
        compensated >= real,
        "the compensated ceiling {compensated} must bound the real allocation {real}"
    );

    // Red with the factor at 1.0: an uncompensated ceiling is the reported
    // figure, which is below the real allocation and would let the table
    // overrun a budget it reported fitting.
    assert!(
        reported < real,
        "the uncompensated (factor 1.0) ceiling under-reserves: {reported} < {real}"
    );

    // The compensation also bounds the mid-resize transient peak (finding 3):
    // old + new = 1.5x the settled real size.
    let transient_peak = real + real / 2;
    assert!(
        compensated >= transient_peak,
        "the compensated ceiling {compensated} must bound the resize transient \
         {transient_peak} (1.5x the settled real size)"
    );
}

/// The compensation factor is the documented product `1.22 * 1.5 = 1.83`, and
/// it is a strict upper bound on the measured under-count ratio so
/// `compensated >= real` cannot depend on rounding luck.
#[test]
fn the_compensation_factor_is_the_documented_product_and_bounds_the_ratio() {
    assert_eq!(
        (GROUP_VALUES_CEILING_COMPENSATION * 100.0).round() / 100.0,
        1.83,
        "compensation is 1.22 (under-count) * 1.5 (resize transient)"
    );

    let buckets = 1usize << 25;
    let measured_ratio = real_size(buckets) as f64 / reported_size(buckets) as f64;
    assert!(
        GROUP_VALUES_CEILING_COMPENSATION > measured_ratio,
        "the factor {GROUP_VALUES_CEILING_COMPENSATION} must strictly exceed the measured \
         steady under-count ratio {measured_ratio}"
    );
}

/// The ceiling must bound the modelled peak at EVERY table size, not only
/// asymptotically. A purely multiplicative compensation does not: the reported
/// figure omits a fixed control-group allocation that does not scale, so at 8
/// buckets the real size is 152 against a reported 112, the modelled 1.5x peak
/// is 228, and 112 * 1.83 is 205. Every table below roughly 512 buckets was
/// under-bounded. Remove the fixed-overhead term from
/// `compensated_group_values_ceiling` and this goes red at the first size.
#[test]
fn the_compensated_ceiling_bounds_small_tables_too() {
    for buckets in [8usize, 16, 32, 64, 128, 256, 512] {
        let reported = reported_size(buckets);
        let modelled_peak =
            (real_size(buckets) as f64 * GROUP_VALUES_RESIZE_TRANSIENT_FACTOR).ceil() as usize;
        let ceiling = compensated_group_values_ceiling(reported);
        assert!(
            ceiling >= modelled_peak,
            "buckets={buckets}: ceiling {ceiling} must bound the modelled peak {modelled_peak}              (reported {reported}, real {})",
            real_size(buckets)
        );
    }
}
