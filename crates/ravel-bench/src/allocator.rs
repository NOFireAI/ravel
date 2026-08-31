//! Which heap allocator is actually loaded, resolved at runtime.
//!
//! A benchmark's peak RSS moves by about 2x between the system allocator (glibc)
//! and a memory-returning allocator (tcmalloc/jemalloc), so an RSS figure
//! without the allocator recorded beside it is not comparable
//! (docs/guides/clickbench.md). The allocator can arrive via `LD_PRELOAD`, which
//! a compile-time `cfg!` cannot see, so this reads the process's own mapped
//! libraries from `/proc/self/maps` and reports what is there, `LD_PRELOAD`
//! included. When the probe cannot answer (a platform without `/proc`, an
//! unreadable or unrecognizable maps) it reports [`ALLOCATOR_UNKNOWN`] rather
//! than guessing: a wrong provenance value reads as verified, which is worse
//! than an explicit absent one.

/// A memory-returning allocator mapped as `libtcmalloc`.
pub const ALLOCATOR_TCMALLOC: &str = "tcmalloc";
/// A memory-returning allocator mapped as `libjemalloc`.
pub const ALLOCATOR_JEMALLOC: &str = "jemalloc";
/// A memory-returning allocator mapped as `libmimalloc`.
pub const ALLOCATOR_MIMALLOC: &str = "mimalloc";
/// No known allocator library is mapped: the process runs on the system
/// allocator (glibc/musl `malloc`). This is a positive finding read off a
/// readable maps, not a guess.
pub const ALLOCATOR_SYSTEM: &str = "system";
/// The probe could not answer: `/proc/self/maps` was unreadable, empty, or
/// unrecognizable. Reported instead of defaulting to a concrete allocator, which
/// would read as verified while being unproven.
pub const ALLOCATOR_UNKNOWN: &str = "unknown";

/// The allocator this process is actually running under, read from
/// `/proc/self/maps`. Returns [`ALLOCATOR_UNKNOWN`] when the maps cannot be read
/// (a platform without `/proc`, or a read error), never a guessed allocator.
pub fn active_allocator() -> String {
    match std::fs::read_to_string("/proc/self/maps") {
        Ok(maps) => allocator_from_maps(&maps),
        Err(_) => ALLOCATOR_UNKNOWN.to_string(),
    }
}

/// Resolve the allocator from the text of a `/proc/self/maps`, split out from
/// the `/proc` read so the parsing is testable against fixture text without a
/// real `/proc`. A mapped `libtcmalloc`/`libjemalloc`/`libmimalloc` wins; a
/// readable maps with recognizable entries but none of them is the system
/// allocator; text with no recognizable maps entry (empty or malformed) is
/// [`ALLOCATOR_UNKNOWN`], never a guess.
fn allocator_from_maps(maps: &str) -> String {
    let mut saw_entry = false;
    for line in maps.lines() {
        if !is_maps_entry(line) {
            continue;
        }
        saw_entry = true;
        if let Some(path) = maps_line_pathname(line) {
            if path.contains("libtcmalloc") {
                return ALLOCATOR_TCMALLOC.to_string();
            }
            if path.contains("libjemalloc") {
                return ALLOCATOR_JEMALLOC.to_string();
            }
            if path.contains("libmimalloc") {
                return ALLOCATOR_MIMALLOC.to_string();
            }
        }
    }
    if saw_entry {
        ALLOCATOR_SYSTEM.to_string()
    } else {
        ALLOCATOR_UNKNOWN.to_string()
    }
}

/// Whether `line` is a `/proc/self/maps` entry: its first field is a
/// `start-end` hex address range. This is what separates a readable maps (has
/// entries, none naming an allocator, so the system allocator) from
/// unrecognizable text (no entries, so unknown).
fn is_maps_entry(line: &str) -> bool {
    let Some(range) = line.split_whitespace().next() else {
        return false;
    };
    let Some((start, end)) = range.split_once('-') else {
        return false;
    };
    !start.is_empty()
        && !end.is_empty()
        && start.bytes().all(|b| b.is_ascii_hexdigit())
        && end.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The pathname field of a maps entry (the file backing the mapping): the sixth
/// whitespace-separated field, `address perms offset dev inode pathname`. `None`
/// for an anonymous mapping, which has no pathname.
fn maps_line_pathname(line: &str) -> Option<&str> {
    line.split_whitespace().nth(5)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A maps blob with tcmalloc preloaded, shaped like a real one: the
    /// executable, the preloaded allocator, and libc.
    const TCMALLOC_MAPS: &str = "\
55a1c0000000-55a1c0021000 r--p 00000000 08:01 100 /usr/bin/ravel-bench
7f2a10000000-7f2a10025000 r-xp 00000000 08:01 200 /usr/lib/x86_64-linux-gnu/libtcmalloc.so.4.5.9
7f2a20000000-7f2a20030000 r-xp 00000000 08:01 201 /usr/lib/x86_64-linux-gnu/libc.so.6
";

    /// The same shape with jemalloc mapped instead.
    const JEMALLOC_MAPS: &str = "\
55a1c0000000-55a1c0021000 r--p 00000000 08:01 100 /usr/bin/ravel-bench
7f2a10000000-7f2a10025000 r-xp 00000000 08:01 200 /usr/lib/x86_64-linux-gnu/libjemalloc.so.2
7f2a20000000-7f2a20030000 r-xp 00000000 08:01 201 /usr/lib/x86_64-linux-gnu/libc.so.6
";

    /// A readable maps with no allocator library mapped: only the executable,
    /// libc, and an anonymous mapping. This is the system allocator.
    const SYSTEM_MAPS: &str = "\
55a1c0000000-55a1c0021000 r--p 00000000 08:01 100 /usr/bin/ravel-bench
7f2a20000000-7f2a20030000 r-xp 00000000 08:01 201 /usr/lib/x86_64-linux-gnu/libc.so.6
7f2a30000000-7f2a30001000 rw-p 00000000 00:00 0
";

    /// A mapped tcmalloc is reported as `tcmalloc`.
    ///
    /// To watch it fail: delete the `if path.contains("libtcmalloc")` return arm
    /// in `allocator_from_maps`. The blob then falls through to the system
    /// allocator and the assertion reads `tcmalloc == system`.
    #[test]
    fn a_mapped_tcmalloc_is_reported_as_tcmalloc() {
        assert_eq!(allocator_from_maps(TCMALLOC_MAPS), ALLOCATOR_TCMALLOC);
    }

    /// A mapped jemalloc is reported as `jemalloc`.
    ///
    /// To watch it fail: delete the `if path.contains("libjemalloc")` return arm
    /// in `allocator_from_maps`.
    #[test]
    fn a_mapped_jemalloc_is_reported_as_jemalloc() {
        assert_eq!(allocator_from_maps(JEMALLOC_MAPS), ALLOCATOR_JEMALLOC);
    }

    /// A mapped mimalloc is reported as `mimalloc`.
    ///
    /// To watch it fail: delete the `if path.contains("libmimalloc")` return arm
    /// in `allocator_from_maps`.
    #[test]
    fn a_mapped_mimalloc_is_reported_as_mimalloc() {
        let maps =
            "7f2a10000000-7f2a10025000 r-xp 00000000 08:01 200 /usr/lib/libmimalloc.so.2.1\n";
        assert_eq!(allocator_from_maps(maps), ALLOCATOR_MIMALLOC);
    }

    /// A readable maps naming no known allocator is the system allocator, not
    /// unknown: the probe answered.
    ///
    /// To watch it fail: change the final `if saw_entry` block to return
    /// `ALLOCATOR_UNKNOWN` unconditionally. The assertion then reads
    /// `system == unknown`.
    #[test]
    fn maps_without_a_known_allocator_is_the_system_allocator() {
        assert_eq!(allocator_from_maps(SYSTEM_MAPS), ALLOCATOR_SYSTEM);
    }

    /// Empty maps is the explicit unknown, never a guessed allocator.
    ///
    /// To watch it fail: change the final `else` arm of `allocator_from_maps` to
    /// return `ALLOCATOR_SYSTEM`. The assertion then reads `unknown == system`.
    #[test]
    fn empty_maps_is_unknown_not_a_guess() {
        assert_eq!(allocator_from_maps(""), ALLOCATOR_UNKNOWN);
    }

    /// Text that is not a maps at all is the explicit unknown: no entry parses,
    /// so the probe cannot answer and must not fall back to the system
    /// allocator.
    ///
    /// To watch it fail: change the final `else` arm of `allocator_from_maps` to
    /// return `ALLOCATOR_SYSTEM`.
    #[test]
    fn malformed_maps_is_unknown_not_a_guess() {
        let maps = "this is not /proc/self/maps\njust some random text\n";
        assert_eq!(allocator_from_maps(maps), ALLOCATOR_UNKNOWN);
    }
}
