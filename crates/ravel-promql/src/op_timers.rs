//! Always-on operator timers for issue #2445's end-to-end operator-share
//! measurement. Each wrapped call site does exactly two atomic adds
//! (elapsed nanoseconds, call count); no per-sample work, no cloning, no
//! recording beyond the two adds.

use std::sync::atomic::AtomicU64;

/// Total nanoseconds inside the plain-aggregation operator
/// (`sum`/`avg`/`min`/`max`/`count`/`group`/`stddev`/`stdvar`), INCLUDING
/// the label-set sort its caller performs ahead of it (`AGG_SORT_NS` below
/// is the same sort time, isolated so it can be reported on its own).
pub static AGG_NS: AtomicU64 = AtomicU64::new(0);
/// Number of plain-aggregation operator calls folded into `AGG_NS`.
pub static AGG_CALLS: AtomicU64 = AtomicU64::new(0);
/// Total nanoseconds sorting a plain aggregation's input vector into
/// label-set order before dispatch; already included in `AGG_NS`. Only
/// recorded for the plain-aggregation operators above, not for
/// `topk`/`bottomk`/`quantile`/`count_values`, which sort through the same
/// call site but are not folded into either counter.
pub static AGG_SORT_NS: AtomicU64 = AtomicU64::new(0);
/// Total nanoseconds inside one-to-one vector matching.
pub static MATCH_NS: AtomicU64 = AtomicU64::new(0);
/// Number of one-to-one matching calls folded into `MATCH_NS`.
pub static MATCH_CALLS: AtomicU64 = AtomicU64::new(0);
