//! The clock jump (ADR-4326 decision 5). Every container runs under
//! libfaketime, reading one offset file on a shared volume; the file starts
//! at `+0`, and the driver rewrites it once per timeline to move every clock
//! forward together, far enough that each hour the workload ingested live
//! before the jump is sealed and a fold, compaction and sweep run over it.

use std::path::Path;

const NS_PER_SECOND: u64 = 1_000_000_000;

/// The seal parameters the compose leaves at their defaults
/// (`DEFAULT_MAX_FLUSH_LIFETIME_NS`, `DEFAULT_CLOCK_SKEW_ALLOWANCE_NS` and
/// `DEFAULT_FOLD_SAFETY_MARGIN_NS` in crates/ravel-catalog/src/config.rs).
pub const MAX_FLUSH_LIFETIME_S: u64 = 3600;
pub const CLOCK_SKEW_ALLOWANCE_S: u64 = 300;
pub const FOLD_SAFETY_MARGIN_S: u64 = 900;

/// The offset the compose and the driver use: 150 minutes.
pub const DEFAULT_JUMP_S: u64 = 9000;

/// How far the driver's own clock may fall short of the jump and still count
/// as having jumped: the time between reading the clock before the write and
/// the reread that picks the new offset up runs the other way, so only a
/// shortfall means the offset was not applied.
const SHORTFALL_ALLOWED_S: u64 = 60;

/// The smallest jump that seals the ingest hour of every write acknowledged
/// before it, whatever second of its hour the write landed in: one hour plus
/// `max_flush_lifetime + clock_skew_allowance + fold_safety_margin`.
pub fn min_jump_s() -> u64 {
    3600 + MAX_FLUSH_LIFETIME_S + CLOCK_SKEW_ALLOWANCE_S + FOLD_SAFETY_MARGIN_S
}

/// The libfaketime timestamp-file line for a relative offset of `jump_s`.
pub fn offset_line(jump_s: u64) -> String {
    format!("+{jump_s}\n")
}

/// Whether `text`, the offset file's content, is the starting offset.
pub fn is_start_offset(text: &str) -> bool {
    text.trim() == "+0"
}

/// Replace the offset file whole: write a sibling and rename it over, so a
/// process rereading the file never sees a partial line, which libfaketime
/// would parse as a different offset.
pub fn write_offset(path: &Path, jump_s: u64) -> Result<(), String> {
    let tmp = path.with_extension("next");
    std::fs::write(&tmp, offset_line(jump_s)).map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("renaming onto {}: {e}", path.display()))
}

/// Whether the clock moved by the jump between `before_ns` and `after_ns`.
pub fn moved(before_ns: u64, after_ns: u64, jump_s: u64) -> bool {
    let want = jump_s.saturating_sub(SHORTFALL_ALLOWED_S) * NS_PER_SECOND;
    after_ns.saturating_sub(before_ns) >= want
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS_PER_HOUR: u64 = 3_600_000_000_000;

    /// The newest sealed hour at `now_ns`, as `sealed_watermark_hour` in
    /// crates/ravel-catalog/src/fold.rs computes it with the default
    /// parameters.
    fn sealed_watermark_hour(now_ns: u64) -> Option<u64> {
        let margin_ns =
            (MAX_FLUSH_LIFETIME_S + CLOCK_SKEW_ALLOWANCE_S + FOLD_SAFETY_MARGIN_S) * NS_PER_SECOND;
        let floor_hours = now_ns.checked_sub(margin_ns)? / NS_PER_HOUR;
        floor_hours.checked_sub(1)
    }

    #[test]
    fn the_watermark_mirrors_the_fold() {
        let margin_ns = 4800 * NS_PER_SECOND;
        // Sealed exactly when `now - margin` reaches the end of the hour.
        assert_eq!(
            sealed_watermark_hour(100 * NS_PER_HOUR + margin_ns),
            Some(99)
        );
        assert_eq!(
            sealed_watermark_hour(100 * NS_PER_HOUR + margin_ns - 1),
            Some(98)
        );
        assert_eq!(sealed_watermark_hour(margin_ns), None);
    }

    #[test]
    fn the_planned_jump_seals_every_hour_acked_before_it() {
        assert!(DEFAULT_JUMP_S >= min_jump_s());
        let hour = 500_000;
        for second in [0, 1, 1799, 3599] {
            let acked_ns = hour * NS_PER_HOUR + second * NS_PER_SECOND;
            let after = acked_ns + DEFAULT_JUMP_S * NS_PER_SECOND;
            assert!(
                sealed_watermark_hour(after) >= Some(hour),
                "second {second}"
            );
        }
        // The bound is tight: one second less leaves a write acknowledged at
        // the start of its hour unsealed.
        let acked_ns = hour * NS_PER_HOUR;
        let short = acked_ns + (min_jump_s() - 1) * NS_PER_SECOND;
        assert!(sealed_watermark_hour(short) < Some(hour));
        assert!(sealed_watermark_hour(acked_ns + min_jump_s() * NS_PER_SECOND) >= Some(hour));
    }

    #[test]
    fn the_offset_file_is_replaced_whole() {
        let dir = std::env::temp_dir().join(format!("clock-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("offset");
        std::fs::write(&path, "+0\n").expect("start");
        assert!(is_start_offset(
            &std::fs::read_to_string(&path).expect("read")
        ));
        write_offset(&path, 9000).expect("write");
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "+9000\n");
        assert!(!is_start_offset("+9000\n"));
        let left: Vec<_> = std::fs::read_dir(&dir).expect("list").collect();
        assert_eq!(left.len(), 1, "the sibling was renamed away");
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn a_clock_that_did_not_move_is_not_a_jump() {
        let before = 500_000 * NS_PER_HOUR;
        assert!(moved(before, before + 9000 * NS_PER_SECOND, 9000));
        assert!(moved(before, before + 8950 * NS_PER_SECOND, 9000));
        assert!(!moved(before, before + 3 * NS_PER_SECOND, 9000));
        assert!(!moved(before, before - NS_PER_SECOND, 9000));
    }
}
