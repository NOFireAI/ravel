//! The clock jump (ADR-4326 decision 5). Every container runs under
//! libfaketime, reading one offset file on a shared volume; the file starts
//! at `+0`, and the driver rewrites it to move every clock forward together:
//! once far enough that each hour the workload ingested live before the jump
//! is sealed and a fold and compaction run over it, then, once a compaction
//! record was seen, past the sweep's protection horizon and its pinned-query
//! window, so the sweep deletes a superseded compaction input.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

const NS_PER_SECOND: u64 = 1_000_000_000;

/// The seal parameters the compose leaves at their defaults
/// (`DEFAULT_MAX_FLUSH_LIFETIME_NS`, `DEFAULT_CLOCK_SKEW_ALLOWANCE_NS` and
/// `DEFAULT_FOLD_SAFETY_MARGIN_NS` in crates/ravel-catalog/src/config.rs).
pub const MAX_FLUSH_LIFETIME_S: u64 = 3600;
pub const CLOCK_SKEW_ALLOWANCE_S: u64 = 300;
pub const FOLD_SAFETY_MARGIN_S: u64 = 900;

/// The offset the compose and the driver use: 150 minutes.
pub const DEFAULT_JUMP_S: u64 = 9000;

/// The sweep terms `sys/gc` is bootstrapped from, which the compose leaves at
/// their defaults (`DEFAULT_MAX_QUERY_DURATION_NS` and `DEFAULT_GRACE_NS` in
/// crates/ravel-maintain/src/config.rs, `DEFAULT_HEAD_CACHE_TTL_NS` in
/// crates/ravel-catalog/src/config.rs).
pub const MAX_QUERY_DURATION_S: u64 = 3600;
pub const GRACE_S: u64 = 86_400;
pub const HEAD_CACHE_TTL_S: u64 = 30;

/// The first sweep jump: the protection horizon plus 15 minutes.
pub const DEFAULT_HORIZON_JUMP_S: u64 = 91_200;
/// The second sweep jump: the pinned-query window plus 570 s.
pub const DEFAULT_WINDOW_JUMP_S: u64 = 5400;

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

/// The default `protection_horizon` (`DEFAULT_PROTECTION_HORIZON_NS`):
/// `max_query_duration + grace + clock_skew_allowance`. The sweep deletes a
/// compaction record's inputs only once `now >= created_unix_ns + horizon`.
pub fn protection_horizon_s() -> u64 {
    MAX_QUERY_DURATION_S + GRACE_S + CLOCK_SKEW_ALLOWANCE_S
}

/// The pinned-query window (`PinnedQueryWindow::has_elapsed` in
/// crates/ravel-maintain/src/unnamed_marker.rs): `max_query_duration +
/// head_cache_ttl + 4 * clock_skew_allowance`. A sweep past the horizon that
/// finds an input unnamed by HEAD writes an unnamed-since marker and deletes
/// the input only on a pass at least this long after the marker.
pub fn pinned_window_s() -> u64 {
    MAX_QUERY_DURATION_S + HEAD_CACHE_TTL_S + 4 * CLOCK_SKEW_ALLOWANCE_S
}

/// Refuse sweep jumps that leave a superseded input in place. A compaction
/// record seen before the horizon jump was created no later than the jump's
/// start, so a sweep after it is past the record's horizon and writes the
/// input's unnamed-since marker; a sweep after the window jump is past that
/// marker's pinned-query window and deletes the input.
pub fn check_sweep_jumps(horizon_jump_s: u64, window_jump_s: u64) -> Result<(), String> {
    if horizon_jump_s < protection_horizon_s() {
        return Err(format!(
            "CLOCK_SWEEP_HORIZON_JUMP_SECS={horizon_jump_s} is below {}, the default \
             protection horizon",
            protection_horizon_s()
        ));
    }
    if window_jump_s < pinned_window_s() {
        return Err(format!(
            "CLOCK_SWEEP_WINDOW_JUMP_SECS={window_jump_s} is below {}, the default \
             pinned-query window",
            pinned_window_s()
        ));
    }
    Ok(())
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
/// would parse as a different offset. `offset_s` is the total offset from
/// the real clock.
pub fn write_offset(path: &Path, offset_s: u64) -> Result<(), String> {
    let tmp = staging_path(path);
    std::fs::write(&tmp, offset_line(offset_s)).map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("renaming onto {}: {e}", path.display()))
}

/// The sibling `write_offset` stages into: the file name with `.next`
/// appended, so `offset.v2` stages to `offset.v2.next`, not `offset.next`.
fn staging_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().map(OsString::from).unwrap_or_default();
    name.push(".next");
    path.with_file_name(name)
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
    fn the_staging_file_appends_to_the_whole_name() {
        assert_eq!(
            staging_path(Path::new("/faketime/offset.v2")),
            PathBuf::from("/faketime/offset.v2.next")
        );
        assert_eq!(
            staging_path(Path::new("/faketime/offset")),
            PathBuf::from("/faketime/offset.next")
        );
        let dir = std::env::temp_dir().join(format!("clock-test-v2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("offset.v2");
        let bystander = dir.join("offset.next");
        std::fs::write(&bystander, "keep\n").expect("bystander");
        write_offset(&path, 9000).expect("write");
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "+9000\n");
        assert_eq!(
            std::fs::read_to_string(&bystander).expect("read"),
            "keep\n",
            "the stage did not land on a sibling with the last extension replaced"
        );
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn the_sweep_terms_match_the_shipped_defaults() {
        // DEFAULT_PROTECTION_HORIZON_NS is 25 h 5 min; the pinned-query
        // window at the defaults is 3600 + 30 + 4 * 300 s.
        assert_eq!(protection_horizon_s(), 90_300);
        assert_eq!(pinned_window_s(), 4830);
    }

    #[test]
    fn the_sweep_jumps_make_a_superseded_input_deletable() {
        assert_eq!(
            check_sweep_jumps(DEFAULT_HORIZON_JUMP_S, DEFAULT_WINDOW_JUMP_S),
            Ok(())
        );
        // A jump of exactly a term is enough, one second less is refused.
        assert_eq!(
            check_sweep_jumps(protection_horizon_s(), pinned_window_s()),
            Ok(())
        );
        assert!(check_sweep_jumps(protection_horizon_s() - 1, DEFAULT_WINDOW_JUMP_S).is_err());
        assert!(check_sweep_jumps(DEFAULT_HORIZON_JUMP_S, pinned_window_s() - 1).is_err());
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
