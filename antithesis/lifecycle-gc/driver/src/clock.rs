//! The clock jumps. Every container runs under libfaketime reading one
//! offset file on a shared volume; the driver alone rewrites it to move
//! every clock forward together: once to seal the hours the erasure
//! requests cover, then past the sweep's protection horizon (and with it the
//! pinned-query window and the six-hour full-sweep cadence), as often as the
//! `.dreq` removal needs.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

const NS_PER_SECOND: u64 = 1_000_000_000;

/// One hour plus `max_flush_lifetime + clock_skew_allowance +
/// fold_safety_margin` (3600 + 300 + 900 s): the jump that seals the ingest
/// hour of every write acknowledged before it.
pub const MIN_SEAL_JUMP_S: u64 = 3600 + 3600 + 300 + 900;
/// `max_query_duration + grace + clock_skew_allowance` (3600 + 86400 + 300 s).
pub const PROTECTION_HORIZON_S: u64 = 90_300;
/// The six-hour `interior_reverify_ns` full-sweep cadence, which the
/// superseded-input sweep of an interior hour waits for.
pub const INTERIOR_REVERIFY_S: u64 = 6 * 3600;

/// The pinned-query window, `max_query_duration + head_cache_ttl + 4 *
/// clock_skew_allowance` (3600 + 30 + 1200 s): how long an unnamed-since
/// marker must age before the bucket it guards may be deleted.
pub const PINNED_WINDOW_S: u64 = 4830;

const SHORTFALL_ALLOWED_S: u64 = 60;

pub fn check_jumps(seal_s: u64, horizon_s: u64, retention_s: u64) -> Result<(), String> {
    // A retention jump must pass the pinned window, so a marker written
    // after a fresh tombstone ages past it, and leave that tombstone's
    // horizon well ahead after two jumps, so only a sweep that ignores the
    // horizon deletes.
    if retention_s <= PINNED_WINDOW_S + 600 || retention_s >= PROTECTION_HORIZON_S / 4 {
        return Err(format!(
            "CLOCK_RETENTION_JUMP_SECS={retention_s} must lie between {} and {}",
            PINNED_WINDOW_S + 600,
            PROTECTION_HORIZON_S / 4
        ));
    }
    if seal_s < MIN_SEAL_JUMP_S {
        return Err(format!(
            "CLOCK_SEAL_JUMP_SECS={seal_s} is below {MIN_SEAL_JUMP_S}, the jump that seals an \
             hour acknowledged at its first second"
        ));
    }
    if horizon_s < PROTECTION_HORIZON_S.max(INTERIOR_REVERIFY_S) {
        return Err(format!(
            "CLOCK_HORIZON_JUMP_SECS={horizon_s} is below the default protection horizon \
             {PROTECTION_HORIZON_S}"
        ));
    }
    Ok(())
}

pub fn is_start_offset(text: &str) -> bool {
    text.trim() == "+0"
}

/// Replace the offset file whole (write a sibling, rename it over), so a
/// process rereading it never parses a partial line.
pub fn write_offset(path: &Path, offset_s: u64) -> Result<(), String> {
    let mut name = path.file_name().map(OsString::from).unwrap_or_default();
    name.push(".next");
    let tmp: PathBuf = path.with_file_name(name);
    std::fs::write(&tmp, format!("+{offset_s}\n"))
        .map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("renaming onto {}: {e}", path.display()))
}

/// Whether the wall clock moved by at least the jump, less a small allowance.
pub fn moved(before_ns: u64, after_ns: u64, jump_s: u64) -> bool {
    after_ns.saturating_sub(before_ns) >= jump_s.saturating_sub(SHORTFALL_ALLOWED_S) * NS_PER_SECOND
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn the_shipped_jumps_pass_and_short_ones_are_refused() {
        assert_eq!(check_jumps(9000, 91_200, 9000), Ok(()));
        assert!(check_jumps(MIN_SEAL_JUMP_S - 1, 91_200, 9000).is_err());
        assert!(check_jumps(9000, PROTECTION_HORIZON_S - 1, 9000).is_err());
        assert!(check_jumps(9000, 91_200, PINNED_WINDOW_S).is_err());
        assert!(check_jumps(9000, 91_200, PROTECTION_HORIZON_S / 4).is_err());
    }

    #[test]
    fn the_offset_is_replaced_whole() {
        let dir = std::env::temp_dir().join(format!("lc-clock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("offset");
        std::fs::write(&path, "+0\n").expect("start");
        assert!(is_start_offset(
            &std::fs::read_to_string(&path).expect("read")
        ));
        write_offset(&path, 9000).expect("write");
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "+9000\n");
        assert_eq!(std::fs::read_dir(&dir).expect("ls").count(), 1);
        std::fs::remove_dir_all(&dir).expect("cleanup");
        assert!(moved(0, 9000 * NS_PER_SECOND, 9000));
        assert!(!moved(0, 3 * NS_PER_SECOND, 9000));
    }
}
