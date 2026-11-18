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

/// `max_flush_lifetime + clock_skew_allowance + fold_safety_margin`: an hour
/// seals this long after it ends.
pub fn seal_margin_s() -> u64 {
    MAX_FLUSH_LIFETIME_S + CLOCK_SKEW_ALLOWANCE_S + FOLD_SAFETY_MARGIN_S
}

/// The newest sealed hour at `now_s`, as `sealed_watermark_hour` in
/// crates/ravel-catalog/src/fold.rs computes it with the default parameters.
pub fn sealed_watermark_hour(now_s: u64) -> Option<u64> {
    let floor_hours = now_s.checked_sub(seal_margin_s())? / 3600;
    floor_hours.checked_sub(1)
}

/// The first second after `now_s` at which the sealed watermark moves.
pub fn next_seal_s(now_s: u64) -> u64 {
    let phase = seal_margin_s() % 3600;
    (now_s + 3600 - phase) / 3600 * 3600 + phase
}

/// The fold race starts at least this long before the seal it straddles, so
/// a server fold that reads its clock up to `race::MAX_SERVER_DELAY_S` (plus a
/// margin) after the start still reads it before the seal, and so builds a
/// pre-seal HEAD the skewed fold can overtake.
const FOLD_RACE_TAIL_S: u64 = crate::race::MAX_SERVER_DELAY_S + 20;
/// And at most this long before it, so the skewed fold, `FOLD_SKEW_S` ahead,
/// reads its clock past the seal with this much to spare.
const FOLD_RACE_LEAD_S: u64 = crate::race::FOLD_SKEW_S - 15;
/// Where a nudge puts the clocks: this long before the next seal, inside the
/// race's window.
const FOLD_RACE_AIM_S: u64 = 150;
/// The smallest nudge: one the settle after it cannot overrun.
const MIN_NUDGE_S: u64 = 10;

/// The seal a fold race started at `now_s` straddles: the skewed fold sees
/// the hour that seals then as sealed and the server folds do not. `None`
/// when `now_s` is outside the race's window.
pub fn fold_race_seal(now_s: u64) -> Option<u64> {
    let seal = next_seal_s(now_s);
    (FOLD_RACE_TAIL_S..=FOLD_RACE_LEAD_S)
        .contains(&(seal - now_s))
        .then_some(seal)
}

/// How far to move every clock forward so a fold race started then is inside
/// its window; 0 when one started at `now_s` already is.
pub fn fold_race_nudge_s(now_s: u64) -> u64 {
    if fold_race_seal(now_s).is_some() {
        return 0;
    }
    next_seal_s(now_s + MIN_NUDGE_S + FOLD_RACE_AIM_S) - FOLD_RACE_AIM_S - now_s
}

/// `max_flush_lifetime + clock_skew_allowance`: a bucket is sealed for
/// compaction this long after its hour ends (`is_sealed` in
/// crates/ravel-maintain), 900 s before the fold seals the hour.
pub fn compaction_seal_margin_s() -> u64 {
    MAX_FLUSH_LIFETIME_S + CLOCK_SKEW_ALLOWANCE_S
}

/// How far past a seal a nudge puts the clocks, so the settle and a process
/// that reads its clock just before the reread still see the hour sealed.
const SEAL_SLACK_S: u64 = 30;

/// How far to move every clock forward so `hour` is sealed `margin_s` after
/// its end, with [`SEAL_SLACK_S`] to spare; 0 when it already is.
pub fn seal_nudge_s(now_s: u64, hour: u64, margin_s: u64) -> u64 {
    ((hour + 1) * 3600 + margin_s + SEAL_SLACK_S).saturating_sub(now_s)
}

/// `DEFAULT_INTERIOR_REVERIFY_NS` in crates/ravel-maintain/src/config.rs: the
/// maintain driver runs a full-keyspace sweep pass instead of the zoned one
/// only when this long passed since its last (`MaintainMemo::full_sweep_due`),
/// and an hour the clocks left far behind is reached by the full pass only.
pub const INTERIOR_REVERIFY_S: u64 = 21_600;

/// The second jump of the unfolded-record sweep (README.md, "The forced
/// races"): past the pinned-query window of a marker written right after the
/// first jump, and past the interior re-verify interval, so the server sweep
/// after it is a full pass that would delete an input it marked.
pub fn unfolded_window_jump_s() -> u64 {
    INTERIOR_REVERIFY_S.max(pinned_window_s()) + 600
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS_PER_HOUR: u64 = 3_600_000_000_000;

    fn sealed_watermark_hour(now_ns: u64) -> Option<u64> {
        super::sealed_watermark_hour(now_ns / NS_PER_SECOND)
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
    fn the_next_seal_is_where_the_watermark_moves() {
        let start = 1_800_000_000;
        for now in start..start + 2 * 3600 + 7 {
            let seal = next_seal_s(now);
            assert!(seal > now && seal - now <= 3600, "{now}");
            assert_eq!(sealed_watermark_hour(seal - 1), sealed_watermark_hour(now));
            assert_eq!(
                sealed_watermark_hour(seal),
                sealed_watermark_hour(now).map(|h| h + 1)
            );
        }
        // The default margin puts each seal at twenty past the hour.
        assert_eq!(next_seal_s(start) % 3600, 1200);
    }

    #[test]
    fn a_fold_race_in_its_window_straddles_the_seal() {
        let skew = crate::race::FOLD_SKEW_S;
        let delay = crate::race::MAX_SERVER_DELAY_S;
        let start = 1_800_000_000;
        let mut inside = 0;
        for now in start..start + 3600 {
            let Some(seal) = fold_race_seal(now) else {
                continue;
            };
            inside += 1;
            let w = sealed_watermark_hour(now);
            // The skewed fold sees one more sealed hour, the servers, posted
            // up to the delay bound later, none.
            assert_eq!(sealed_watermark_hour(now + skew), w.map(|h| h + 1));
            assert_eq!(sealed_watermark_hour(now + delay + 15), w);
            assert_eq!(seal, next_seal_s(now));
        }
        assert_eq!(inside, FOLD_RACE_LEAD_S - FOLD_RACE_TAIL_S + 1);
    }

    #[test]
    fn a_nudge_lands_inside_the_fold_race_window() {
        let start = 1_800_000_000;
        for now in start..start + 3600 {
            let nudge = fold_race_nudge_s(now);
            if fold_race_seal(now).is_some() {
                assert_eq!(nudge, 0);
                continue;
            }
            assert!((MIN_NUDGE_S..MIN_NUDGE_S + 3600).contains(&nudge), "{now}");
            // Settling for up to the window's slack after the nudge still
            // starts the race inside it.
            for settle in [0, 30, 80] {
                assert!(
                    fold_race_seal(now + nudge + settle).is_some(),
                    "{now}+{settle}"
                );
            }
        }
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
    fn a_seal_nudge_seals_the_hour_and_no_later_one() {
        // DEFAULT_MAX_FLUSH_LIFETIME_NS plus DEFAULT_CLOCK_SKEW_ALLOWANCE_NS.
        assert_eq!(compaction_seal_margin_s(), 3900);
        let hour = 500_000;
        let end = (hour + 1) * 3600;
        for now in [hour * 3600, hour * 3600 + 1799, end - 1, end + 3000] {
            for margin in [compaction_seal_margin_s(), seal_margin_s()] {
                let after = now + seal_nudge_s(now, hour, margin);
                assert!(after >= end + margin, "{now} {margin}");
                // Within the slack, so the next hour stays unsealed.
                assert!(after < end + 3600 + margin, "{now} {margin}");
            }
        }
        // The fold's seal of the hour comes after a compaction nudge.
        let now = hour * 3600 + 10;
        let after = now + seal_nudge_s(now, hour, compaction_seal_margin_s());
        assert!(sealed_watermark_hour(after * NS_PER_SECOND) < Some(hour));
        let after = now + seal_nudge_s(now, hour, seal_margin_s());
        assert_eq!(sealed_watermark_hour(after * NS_PER_SECOND), Some(hour));
        // Already sealed: no move.
        assert_eq!(
            seal_nudge_s(end + 7200, hour, compaction_seal_margin_s()),
            0
        );
    }

    #[test]
    fn the_unfolded_window_jump_makes_the_next_pass_full_and_past_the_window() {
        // DEFAULT_INTERIOR_REVERIFY_NS is 6 h.
        assert_eq!(INTERIOR_REVERIFY_S, 6 * 3600);
        let jump = unfolded_window_jump_s();
        assert_eq!(jump, 22_200);
        assert!(jump >= INTERIOR_REVERIFY_S && jump >= pinned_window_s());
        // A marker written up to the slack after the first jump is still
        // past its window after the second.
        assert!(jump - 600 >= pinned_window_s());
        // And the first jump is past the horizon of a record written just
        // before it.
        assert!(check_sweep_jumps(DEFAULT_HORIZON_JUMP_S, jump).is_ok());
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
