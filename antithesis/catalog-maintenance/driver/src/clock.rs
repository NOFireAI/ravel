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

/// libfaketime's reread interval, `FAKETIME_CACHE_DURATION` in
/// config/docker-compose.yaml: a process picks a new offset up at most this
/// long after the file changes.
pub const FAKETIME_CACHE_DURATION_S: u64 = 2;

/// How long after a clock move reached the driver's own clock it has reached
/// every faked clock: one reread interval plus a second's margin.
pub fn reach_wait_s(reread_s: u64) -> u64 {
    reread_s + 1
}

/// The offset the compose and the driver use: 150 minutes.
pub const DEFAULT_JUMP_S: u64 = 9000;

/// The sweep terms `sys/gc` is bootstrapped from, which the compose leaves at
/// their defaults (`DEFAULT_MAX_QUERY_DURATION_NS` and `DEFAULT_GRACE_NS` in
/// crates/ravel-maintain/src/config.rs, `DEFAULT_HEAD_CACHE_TTL_NS` in
/// crates/ravel-catalog/src/config.rs).
pub const MAX_QUERY_DURATION_S: u64 = 3600;
pub const GRACE_S: u64 = 86_400;
pub const HEAD_CACHE_TTL_S: u64 = 30;

/// The servers' store client request timeout (`DEFAULT_REQUEST_TIMEOUT` in
/// crates/ravel-object-store/src/s3.rs). A server's read of HEAD started
/// before HEAD changed can return the old HEAD up to this long after the
/// change, and the cache entry it fills lives `head_cache_ttl` from then.
pub const STORE_REQUEST_TIMEOUT_S: u64 = 20;

/// How long HEAD must have held still, in real time, before a clock move.
///
/// The sweep's delete gate (ADR-1133) measures its pinned-query window on the
/// wall clock and assumes that window also passed in real time: a query
/// process caches HEAD for `head_cache_ttl` on its monotonic clock, which no
/// jump moves. A jump makes the wall-clock window pass in seconds, so before
/// each one the driver waits until no server can still hold, or still be
/// reading, a HEAD older than the current one: the TTL plus
/// [`STORE_REQUEST_TIMEOUT_S`]. The driver sends no query meanwhile, since
/// every move runs under the gate's write guard.
pub fn head_quiet_s(head_cache_ttl_s: u64) -> u64 {
    head_cache_ttl_s + STORE_REQUEST_TIMEOUT_S
}

/// The HEAD cache TTL `ravel-cli gc-config show` prints for `sys/gc`, in
/// nanoseconds: its `head_cache_ttl_ns: <n>` line.
pub fn recorded_head_cache_ttl_ns(show: &str) -> Option<u64> {
    show.lines()
        .find_map(|l| l.trim().strip_prefix("head_cache_ttl_ns:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|n| n.parse().ok())
}

/// Refuse a HEAD cache TTL plan whose parts disagree: the TTL the compose
/// states for the servers (`HEAD_CACHE_TTL_SECS`), the one the sweep jumps
/// were sized with ([`HEAD_CACHE_TTL_S`], a term of [`pinned_window_s`]),
/// and, once known, the one `sys/gc` records, which bounds every query
/// process's TTL and which the delete gate is specified against.
pub fn check_head_cache_ttl(stated_s: u64, recorded_ns: Option<u64>) -> Result<(), String> {
    if stated_s != HEAD_CACHE_TTL_S {
        return Err(format!(
            "HEAD_CACHE_TTL_SECS={stated_s}, but the sweep jumps are sized for a \
             {HEAD_CACHE_TTL_S} s HEAD cache TTL"
        ));
    }
    match recorded_ns {
        Some(ns) if ns != stated_s * NS_PER_SECOND => Err(format!(
            "sys/gc records head_cache_ttl_ns={ns}, but HEAD_CACHE_TTL_SECS={stated_s}"
        )),
        _ => Ok(()),
    }
}

/// What one read of HEAD returned, for [`Quiet`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeadRead {
    Bytes(Vec<u8>),
    Absent,
    /// The read failed: whether HEAD changed is unknown.
    Failed,
}

/// How long HEAD has held still, from reads taken at injected times
/// (durations since any fixed start).
#[derive(Debug)]
pub struct Quiet {
    since: std::time::Duration,
    last: HeadRead,
}

impl Quiet {
    /// Start counting at `now`, from the HEAD read then.
    pub fn new(now: std::time::Duration, head: HeadRead) -> Quiet {
        Quiet {
            since: now,
            last: head,
        }
    }

    /// One more read at `now`, taken while `frozen` held (the proxy confirmed
    /// its freeze of HEAD since the previous read). A changed HEAD, a failed
    /// read or an unconfirmed freeze starts the count again.
    pub fn sample(&mut self, now: std::time::Duration, head: HeadRead, frozen: bool) {
        if !frozen || head == HeadRead::Failed || head != self.last {
            self.since = now;
        }
        self.last = head;
    }

    pub fn quiet_for(&self, now: std::time::Duration) -> std::time::Duration {
        now.saturating_sub(self.since)
    }

    /// HEAD as last read.
    pub fn head(&self) -> &HeadRead {
        &self.last
    }
}

/// How the quiet wait before a clock move runs: the span HEAD must hold
/// still, how many spans it may take, and how often it reads HEAD.
#[derive(Clone, Copy, Debug)]
pub struct QuietWait {
    pub need: std::time::Duration,
    pub tries: u32,
    pub poll: std::time::Duration,
}

/// The quiet wait before a clock move, over injected time and I/O: `now`
/// reads a monotonic clock, `sleep` waits, `freeze` asks the proxy to refuse
/// HEAD PUTs (whether it confirmed), `read` reads HEAD. Returns HEAD as last
/// read and how long it had held still once that reached `need`. `Err` when
/// no HEAD key is known, since HEAD can then be neither frozen nor read, and
/// after `tries` spans without a quiet one.
pub fn wait_quiet(
    head_key_known: bool,
    w: QuietWait,
    now: &mut dyn FnMut() -> std::time::Duration,
    sleep: &mut dyn FnMut(std::time::Duration),
    freeze: &mut dyn FnMut() -> bool,
    read: &mut dyn FnMut() -> HeadRead,
) -> Result<(HeadRead, std::time::Duration), String> {
    if !head_key_known {
        return Err("no HEAD key is known, so HEAD can be neither frozen nor read".to_string());
    }
    let start = now();
    let confirmed = freeze();
    let mut quiet = Quiet::new(now(), read());
    if !confirmed {
        quiet.sample(now(), HeadRead::Failed, false);
    }
    loop {
        let t = now();
        if quiet.quiet_for(t) >= w.need {
            return Ok((quiet.head().clone(), quiet.quiet_for(t)));
        }
        let waited = t.saturating_sub(start);
        if waited >= w.need * w.tries {
            return Err(format!(
                "HEAD did not hold still for {} s within {} s",
                w.need.as_secs(),
                waited.as_secs()
            ));
        }
        sleep(w.poll);
        let confirmed = freeze();
        let head = read();
        quiet.sample(now(), head, confirmed);
    }
}

/// Whether HEAD changed between the end of the quiet wait (`quiet`) and the
/// read after the move landed (`landed`); `None` when either read failed,
/// so nothing is known.
pub fn moved_while_landing(quiet: &HeadRead, landed: &HeadRead) -> Option<bool> {
    if *quiet == HeadRead::Failed || *landed == HeadRead::Failed {
        return None;
    }
    Some(quiet != landed)
}

/// Where the two sweep jumps (README.md, "The sweep jumps") stand. A jump
/// written is never written again, whether or not it reached the driver; a
/// jump the quiet wait gave up, before any write, is taken by the next call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SweepJumps {
    #[default]
    NotStarted,
    /// The horizon jump was written; `reached`: it reached the driver.
    HorizonWritten { reached: bool },
    /// The window jump was written too.
    Done { reached: bool },
}

/// The sweep jump a call takes next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SweepStep {
    Horizon,
    Window,
    Nothing,
}

impl SweepJumps {
    pub fn next(self) -> SweepStep {
        match self {
            SweepJumps::NotStarted => SweepStep::Horizon,
            SweepJumps::HorizonWritten { reached: true } => SweepStep::Window,
            _ => SweepStep::Nothing,
        }
    }

    /// The state after `step`: `Ok(reached)` when its offset was written,
    /// `Err` when the quiet wait gave it up before any write.
    pub fn after(self, step: SweepStep, outcome: Result<bool, ()>) -> SweepJumps {
        match (step, outcome) {
            (_, Err(())) => self,
            (SweepStep::Horizon, Ok(reached)) => SweepJumps::HorizonWritten { reached },
            (SweepStep::Window, Ok(reached)) => SweepJumps::Done { reached },
            (SweepStep::Nothing, Ok(_)) => self,
        }
    }
}

/// The first sweep jump: the protection horizon plus 15 minutes.
pub const DEFAULT_HORIZON_JUMP_S: u64 = 91_200;
/// The second sweep jump: past the pinned-query window and the interior
/// re-verify interval, plus 600 s ([`window_jump_floor_s`]).
pub const DEFAULT_WINDOW_JUMP_S: u64 = 22_200;

/// The smallest second sweep jump. It must pass the pinned-query window of
/// the markers written after the horizon jump, and also the interior
/// re-verify interval: the servers' first sweep after the horizon jump is a
/// full pass, and it runs during the quiet wait before the window jump, so
/// without the interval their next full pass, the only one that reaches the
/// hours the clocks left behind, would be 6 h of wall time away and only the
/// driver's own sweep would delete the inputs.
pub fn window_jump_floor_s() -> u64 {
    INTERIOR_REVERIFY_S.max(pinned_window_s())
}

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
/// marker's pinned-query window and deletes the input, and the servers' first
/// sweep after it is a full pass ([`window_jump_floor_s`]).
pub fn check_sweep_jumps(horizon_jump_s: u64, window_jump_s: u64) -> Result<(), String> {
    if horizon_jump_s < protection_horizon_s() {
        return Err(format!(
            "CLOCK_SWEEP_HORIZON_JUMP_SECS={horizon_jump_s} is below {}, the default \
             protection horizon",
            protection_horizon_s()
        ));
    }
    if window_jump_s < window_jump_floor_s() {
        return Err(format!(
            "CLOCK_SWEEP_WINDOW_JUMP_SECS={window_jump_s} is below {}, the larger of the \
             default pinned-query window and the interior re-verify interval",
            window_jump_floor_s()
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
    window_jump_floor_s() + 600
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
            check_sweep_jumps(protection_horizon_s(), window_jump_floor_s()),
            Ok(())
        );
        assert!(check_sweep_jumps(protection_horizon_s() - 1, DEFAULT_WINDOW_JUMP_S).is_err());
        assert!(check_sweep_jumps(DEFAULT_HORIZON_JUMP_S, window_jump_floor_s() - 1).is_err());
        // The window term is the interior re-verify interval, past which the
        // servers' next sweep is a full pass; a jump past only the pinned-query
        // window is refused.
        assert_eq!(window_jump_floor_s(), INTERIOR_REVERIFY_S);
        assert!(check_sweep_jumps(DEFAULT_HORIZON_JUMP_S, pinned_window_s()).is_err());
        assert_eq!(DEFAULT_WINDOW_JUMP_S, unfolded_window_jump_s());
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
    fn the_reread_interval_matches_the_compose() {
        let compose = include_str!("../../config/docker-compose.yaml");
        let line = format!("  FAKETIME_CACHE_DURATION: \"{FAKETIME_CACHE_DURATION_S}\"\n");
        assert_eq!(compose.matches(&line).count(), 1, "{line:?}");
        assert_eq!(compose.matches("FAKETIME_CACHE_DURATION:").count(), 1);
        // A held PUT is taken only once the move reached every clock.
        assert!(reach_wait_s(FAKETIME_CACHE_DURATION_S) >= 3);
    }

    #[test]
    fn the_quiet_span_is_the_ttl_and_a_late_head_read() {
        assert_eq!(head_quiet_s(HEAD_CACHE_TTL_S), 50);
        // A HEAD read that started just before the change returns at the
        // store timeout and is served for the TTL after that.
        assert!(head_quiet_s(HEAD_CACHE_TTL_S) >= STORE_REQUEST_TIMEOUT_S + HEAD_CACHE_TTL_S);
    }

    #[test]
    fn quiet_counts_only_an_unchanged_frozen_head() {
        use std::time::Duration;
        let s = Duration::from_secs;
        let a = HeadRead::Bytes(b"head a".to_vec());
        let b = HeadRead::Bytes(b"head b".to_vec());
        let mut q = Quiet::new(s(0), a.clone());
        q.sample(s(10), a.clone(), true);
        q.sample(s(20), a.clone(), true);
        assert_eq!(q.quiet_for(s(20)), s(20));
        // A new HEAD starts the count at the read that saw it.
        q.sample(s(25), b.clone(), true);
        assert_eq!(q.quiet_for(s(25)), s(0));
        assert_eq!(q.quiet_for(s(40)), s(15));
        assert_eq!(q.head(), &b);
        // So does a read that failed, and a freeze the proxy did not confirm.
        q.sample(s(41), HeadRead::Failed, true);
        assert_eq!(q.quiet_for(s(45)), s(4));
        q.sample(s(46), b.clone(), true);
        assert_eq!(q.quiet_for(s(50)), s(4));
        q.sample(s(51), b.clone(), false);
        assert_eq!(q.quiet_for(s(60)), s(9));
        // An absent HEAD that stays absent is quiet too.
        let mut q = Quiet::new(s(0), HeadRead::Absent);
        q.sample(s(60), HeadRead::Absent, true);
        assert_eq!(q.quiet_for(s(60)), s(60));
    }

    /// Run `wait_quiet` on a fake clock that only `sleep` advances, with
    /// `heads` giving HEAD at each read (the last repeats). Returns the
    /// result and how many freezes and reads it made.
    fn fake_wait(
        key_known: bool,
        heads: &[HeadRead],
    ) -> (
        Result<(HeadRead, std::time::Duration), String>,
        usize,
        usize,
    ) {
        use std::cell::Cell;
        use std::time::Duration;
        let t = Cell::new(Duration::ZERO);
        let (freezes, reads) = (Cell::new(0), Cell::new(0));
        let w = QuietWait {
            need: Duration::from_secs(50),
            tries: 4,
            poll: Duration::from_secs(1),
        };
        let r = wait_quiet(
            key_known,
            w,
            &mut || t.get(),
            &mut |d| t.set(t.get() + d),
            &mut || {
                freezes.set(freezes.get() + 1);
                true
            },
            &mut || {
                let i = reads.get();
                reads.set(i + 1);
                heads[i.min(heads.len() - 1)].clone()
            },
        );
        (r, freezes.get(), reads.get())
    }

    #[test]
    fn the_quiet_wait_refuses_a_move_with_no_head_key() {
        let (r, freezes, reads) = fake_wait(false, &[HeadRead::Absent]);
        assert!(r.is_err(), "{r:?}");
        assert_eq!((freezes, reads), (0, 0), "nothing frozen or read");
    }

    #[test]
    fn the_quiet_wait_ends_once_head_held_still_for_the_span() {
        use std::time::Duration;
        let a = HeadRead::Bytes(b"a".to_vec());
        let (r, freezes, reads) = fake_wait(true, std::slice::from_ref(&a));
        assert_eq!(r, Ok((a.clone(), Duration::from_secs(50))));
        assert_eq!(
            (freezes, reads),
            (51, 51),
            "one freeze and one read per second"
        );
        // A HEAD that changes at the 20th read is quiet 50 s after that.
        let b = HeadRead::Bytes(b"b".to_vec());
        let mut heads = vec![a.clone(); 20];
        heads.push(b.clone());
        let (r, _, reads) = fake_wait(true, &heads);
        assert_eq!(r, Ok((b, Duration::from_secs(50))));
        assert_eq!(reads, 71);
        // One that never holds still gives the move up at four spans.
        let flapping: Vec<HeadRead> = (0..300u32)
            .map(|i| HeadRead::Bytes(i.to_le_bytes().to_vec()))
            .collect();
        let (r, _, reads) = fake_wait(true, &flapping);
        assert!(r.is_err(), "{r:?}");
        assert_eq!(reads, 201);
    }

    #[test]
    fn a_landing_read_tells_a_change_only_when_both_reads_succeeded() {
        let a = HeadRead::Bytes(b"a".to_vec());
        let b = HeadRead::Bytes(b"b".to_vec());
        assert_eq!(moved_while_landing(&a, &a), Some(false));
        assert_eq!(moved_while_landing(&a, &b), Some(true));
        assert_eq!(moved_while_landing(&a, &HeadRead::Absent), Some(true));
        assert_eq!(moved_while_landing(&HeadRead::Failed, &a), None);
        assert_eq!(moved_while_landing(&a, &HeadRead::Failed), None);
    }

    #[test]
    fn a_sweep_jump_given_up_is_taken_by_the_next_call() {
        use SweepJumps::*;
        assert_eq!(NotStarted.next(), SweepStep::Horizon);
        // A horizon jump given up before its write is taken again.
        assert_eq!(NotStarted.after(SweepStep::Horizon, Err(())), NotStarted);
        let h = NotStarted.after(SweepStep::Horizon, Ok(true));
        assert_eq!(h, HorizonWritten { reached: true });
        assert_eq!(h.next(), SweepStep::Window);
        // So is a window jump given up after the horizon jump landed.
        assert_eq!(h.after(SweepStep::Window, Err(())), h);
        assert_eq!(
            h.after(SweepStep::Window, Err(())).next(),
            SweepStep::Window
        );
        let done = h.after(SweepStep::Window, Ok(true));
        assert_eq!(done, Done { reached: true });
        assert_eq!(done.next(), SweepStep::Nothing);
        // A jump written but not reached is never written again.
        let lost = NotStarted.after(SweepStep::Horizon, Ok(false));
        assert_eq!(lost.next(), SweepStep::Nothing);
        assert_eq!(
            h.after(SweepStep::Window, Ok(false)).next(),
            SweepStep::Nothing
        );
    }

    #[test]
    fn the_recorded_head_cache_ttl_is_read_from_gc_config_show() {
        let v1 = "format_version: 1\nprotection_horizon_ns: 90300000000000\n\
                  head_cache_ttl_ns: 30000000000 (format version 1 records none; the compiled \
                  default applies)\n";
        assert_eq!(recorded_head_cache_ttl_ns(v1), Some(30_000_000_000));
        assert_eq!(
            recorded_head_cache_ttl_ns("format_version: 2\nhead_cache_ttl_ns: 45000000000\n"),
            Some(45_000_000_000)
        );
        let absent = "sys/gc is not present: this bucket has not been bootstrapped by a server \
                      yet.";
        assert_eq!(recorded_head_cache_ttl_ns(absent), None);
        assert_eq!(recorded_head_cache_ttl_ns("head_cache_ttl_ns: x"), None);
    }

    #[test]
    fn a_head_cache_ttl_plan_that_disagrees_is_refused() {
        let ttl_ns = HEAD_CACHE_TTL_S * NS_PER_SECOND;
        assert_eq!(check_head_cache_ttl(HEAD_CACHE_TTL_S, None), Ok(()));
        assert_eq!(check_head_cache_ttl(HEAD_CACHE_TTL_S, Some(ttl_ns)), Ok(()));
        assert!(check_head_cache_ttl(HEAD_CACHE_TTL_S, Some(ttl_ns + 1)).is_err());
        assert!(check_head_cache_ttl(HEAD_CACHE_TTL_S + 15, None).is_err());
        assert!(
            check_head_cache_ttl(HEAD_CACHE_TTL_S + 15, Some(ttl_ns + 15 * NS_PER_SECOND)).is_err()
        );
    }

    #[test]
    fn the_compose_states_the_head_cache_ttl_and_overrides_it_nowhere() {
        let compose = include_str!("../../config/docker-compose.yaml");
        let line = format!("      HEAD_CACHE_TTL_SECS: \"{HEAD_CACHE_TTL_S}\"\n");
        assert_eq!(compose.matches(&line).count(), 1, "{line:?}");
        assert_eq!(compose.matches("HEAD_CACHE_TTL_SECS:").count(), 1);
        // The servers run the compiled TTL: no flag sets another, and init
        // writes no sys/gc that records another.
        assert_eq!(compose.matches("head-cache-ttl").count(), 0);
        let init = include_str!("../../ravel/init.sh");
        assert_eq!(init.matches("gc-config").count(), 0);
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
