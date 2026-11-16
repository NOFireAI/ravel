//! Which heap allocator is actually loaded, resolved at runtime.
//!
//! A benchmark's peak RSS moves by about 2x between the system allocator (glibc)
//! and a memory-returning allocator (tcmalloc/jemalloc), so an RSS figure
//! without the allocator recorded beside it is not comparable
//! (docs/guides/clickbench.md). The allocator can arrive via `LD_PRELOAD`, which
//! a compile-time `cfg!` cannot see, so this reads the process's own mapped
//! libraries from `/proc/self/maps` and reports what is there, `LD_PRELOAD`
//! included. When the probe cannot answer (a platform without `/proc`, an
//! unreadable or unrecognizable maps, or two different allocator libraries
//! mapped at once) it reports [`Allocator::Unknown`] rather than guessing: a
//! wrong provenance value reads as verified, which is worse than an explicit
//! absent one.

/// The heap allocator a benchmark process ran under, resolved at runtime from
/// its mapped libraries. This is the one value domain the two provenance schemas
/// share (issue #972): a typed enum makes an out-of-domain allocator
/// unrepresentable rather than merely rejected, so a provenance value in this
/// slot can be trusted to be exactly one the probe can produce. Its serde
/// representation is the lowercase variant name (`"tcmalloc"`, `"jemalloc"`,
/// `"mimalloc"`, `"system"`, `"unknown"`), and an unrecognized string in a
/// serialized report is rejected at deserialize (serde's default for an enum
/// with no catch-all variant): a garbage value laundered into `Unknown` would
/// read as the honest "the probe ran and could not answer", the confusion the
/// field exists to avoid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Allocator {
    /// A memory-returning allocator mapped as `libtcmalloc`.
    Tcmalloc,
    /// A memory-returning allocator mapped as `libjemalloc`.
    Jemalloc,
    /// A memory-returning allocator mapped as `libmimalloc`.
    Mimalloc,
    /// No known allocator library is mapped: the process runs on the system
    /// allocator (glibc/musl `malloc`). This is a positive finding read off a
    /// readable maps, not a guess.
    System,
    /// The probe could not answer: `/proc/self/maps` was unreadable, empty, or
    /// unrecognizable, or a report predates the field. Recorded instead of
    /// defaulting to a concrete allocator, which would read as verified while
    /// being unproven.
    Unknown,
}

impl Allocator {
    /// The serialized form: the lowercase variant name, matching the serde
    /// representation. Used to render provenance without going through JSON.
    pub const fn as_str(self) -> &'static str {
        match self {
            Allocator::Tcmalloc => "tcmalloc",
            Allocator::Jemalloc => "jemalloc",
            Allocator::Mimalloc => "mimalloc",
            Allocator::System => "system",
            Allocator::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for Allocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The allocator this process is actually running under, read from
/// `/proc/self/maps`. Returns [`Allocator::Unknown`] when the maps cannot be
/// read (a platform without `/proc`, or a read error), never a guessed
/// allocator.
pub fn active_allocator() -> Allocator {
    match std::fs::read_to_string("/proc/self/maps") {
        Ok(maps) => allocator_from_maps(&maps),
        Err(_) => Allocator::Unknown,
    }
}

/// The allocator libraries the probe recognizes, each paired with the substring
/// a maps pathname carries when that library is mapped. One table rather than a
/// chain of tests, so the scan below cannot acquire a precedence order between
/// them: there is no order in which reporting one of two mapped allocators is
/// correct.
const RECOGNIZED_LIBRARIES: &[(&str, Allocator)] = &[
    ("libtcmalloc", Allocator::Tcmalloc),
    ("libjemalloc", Allocator::Jemalloc),
    ("libmimalloc", Allocator::Mimalloc),
];

/// Resolve the allocator from the text of a `/proc/self/maps`, split out from
/// the `/proc` read so the parsing is testable against fixture text without a
/// real `/proc`.
///
/// The whole text is scanned and the DISTINCT recognized allocator libraries are
/// collected before anything is decided, because the recognized set, not the
/// first match, is what the answer depends on:
///
/// - exactly one distinct recognized allocator: that allocator;
/// - two or more distinct recognized allocators: [`Allocator::Unknown`], because
///   the probe cannot say which of them governs allocation. A process started
///   with `LD_PRELOAD=libjemalloc.so` can also have `libtcmalloc.so` mapped as
///   some other library's transitive dependency, and a first-match scan in a
///   fixed library order would report tcmalloc as fact;
/// - no recognized allocator but at least one well-formed maps entry: the system
///   allocator, a positive finding read off a readable maps;
/// - no well-formed maps entry at all (empty or malformed text):
///   [`Allocator::Unknown`], never a guess.
///
/// Distinct libraries, not matching lines: a maps file lists several segments
/// per mapped object (`r--p`, `r-xp`, `rw-p`), so one preloaded allocator
/// normally matches three or more lines and counting lines would report every
/// single-allocator process as ambiguous.
fn allocator_from_maps(maps: &str) -> Allocator {
    let mut saw_entry = false;
    let mut recognized: Option<Allocator> = None;
    let mut ambiguous = false;
    for line in maps.lines() {
        if !is_maps_entry(line) {
            continue;
        }
        saw_entry = true;
        let Some(path) = maps_line_pathname(line) else {
            continue;
        };
        for &(library, allocator) in RECOGNIZED_LIBRARIES {
            if !path.contains(library) {
                continue;
            }
            match recognized {
                // The same library across its several segments is one allocator.
                Some(seen) if seen == allocator => {}
                Some(_) => ambiguous = true,
                None => recognized = Some(allocator),
            }
        }
    }
    if ambiguous {
        return Allocator::Unknown;
    }
    match recognized {
        Some(allocator) => allocator,
        None if saw_entry => Allocator::System,
        None => Allocator::Unknown,
    }
}

/// Whether `line` is a `/proc/self/maps` entry: its first field is a
/// `start-end` hex address range. This is what separates a readable maps (has
/// entries, none naming an allocator, so the system allocator) from
/// unrecognizable text (no entries, so unknown).
fn is_maps_entry(line: &str) -> bool {
    let mut fields = line.split_whitespace();
    let Some(range) = fields.next() else {
        return false;
    };
    let Some((start, end)) = range.split_once('-') else {
        return false;
    };
    let range_ok = !start.is_empty()
        && !end.is_empty()
        && start.bytes().all(|b| b.is_ascii_hexdigit())
        && end.bytes().all(|b| b.is_ascii_hexdigit());
    if !range_ok {
        return false;
    }
    // A hex range alone is not a maps record: `7f-80 not-a-maps-record` must
    // read as unrecognizable text (Unknown), never as evidence of a readable
    // maps (System). Require the mandatory columns too: a permissions field of
    // exactly four bytes from the maps alphabet, then offset, dev, and inode.
    let Some(perms) = fields.next() else {
        return false;
    };
    let perms_ok = perms.len() == 4
        && perms.bytes().zip(*b"rwxp").all(|(got, kind)| match kind {
            b'r' => got == b'r' || got == b'-',
            b'w' => got == b'w' || got == b'-',
            b'x' => got == b'x' || got == b'-',
            _ => got == b'p' || got == b's',
        });
    perms_ok && fields.next().is_some() && fields.next().is_some() && fields.next().is_some()
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

    #[test]
    fn a_near_miss_line_is_not_a_maps_entry_and_reads_unknown() {
        // The mandatory columns (perms, offset, dev, inode) are what separate
        // a maps record from text that merely starts with a hex range; without
        // them the probe must answer Unknown, never a guessed System.
        assert_eq!(allocator_from_maps(NEAR_MISS_MAPS), Allocator::Unknown);
        // And a real record still parses: the fixture set stays recognized.
        assert_eq!(allocator_from_maps(SYSTEM_MAPS), Allocator::System);
    }

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

    /// Near-miss text: a hex range followed by junk is NOT a maps record.
    /// Before the column validation, `7f-80 not-a-maps-record` set `saw_entry`
    /// and the probe answered `System` for unrecognizable text -- a guessed
    /// allocator, the exact thing this module must never produce.
    const NEAR_MISS_MAPS: &str = "\
7f-80 not-a-maps-record
deadbeef-cafebabe junk junk
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
    /// To watch it fail: delete the `("libtcmalloc", Allocator::Tcmalloc)` row
    /// from `RECOGNIZED_LIBRARIES`. The blob then falls through to the system
    /// allocator and the assertion reads `tcmalloc == system`.
    #[test]
    fn a_mapped_tcmalloc_is_reported_as_tcmalloc() {
        assert_eq!(allocator_from_maps(TCMALLOC_MAPS), Allocator::Tcmalloc);
    }

    /// A mapped jemalloc is reported as `jemalloc`.
    ///
    /// To watch it fail: delete the `("libjemalloc", Allocator::Jemalloc)` row
    /// from `RECOGNIZED_LIBRARIES`.
    #[test]
    fn a_mapped_jemalloc_is_reported_as_jemalloc() {
        assert_eq!(allocator_from_maps(JEMALLOC_MAPS), Allocator::Jemalloc);
    }

    /// A mapped mimalloc is reported as `mimalloc`.
    ///
    /// To watch it fail: delete the `("libmimalloc", Allocator::Mimalloc)` row
    /// from `RECOGNIZED_LIBRARIES`.
    #[test]
    fn a_mapped_mimalloc_is_reported_as_mimalloc() {
        let maps =
            "7f2a10000000-7f2a10025000 r-xp 00000000 08:01 200 /usr/lib/libmimalloc.so.2.1\n";
        assert_eq!(allocator_from_maps(maps), Allocator::Mimalloc);
    }

    /// A readable maps naming no known allocator is the system allocator, not
    /// unknown: the probe answered.
    ///
    /// To watch it fail: change the `None if saw_entry => Allocator::System` arm
    /// of the final `match recognized` to `Allocator::Unknown`. The assertion
    /// then reads `system == unknown`.
    #[test]
    fn maps_without_a_known_allocator_is_the_system_allocator() {
        assert_eq!(allocator_from_maps(SYSTEM_MAPS), Allocator::System);
    }

    /// Empty maps is the explicit unknown, never a guessed allocator.
    ///
    /// To watch it fail: change the final `None => Allocator::Unknown` arm of
    /// `allocator_from_maps` to `Allocator::System`. The assertion then reads
    /// `unknown == system`.
    #[test]
    fn empty_maps_is_unknown_not_a_guess() {
        assert_eq!(allocator_from_maps(""), Allocator::Unknown);
    }

    /// Text that is not a maps at all is the explicit unknown: no entry parses,
    /// so the probe cannot answer and must not fall back to the system
    /// allocator.
    ///
    /// To watch it fail: change the final `None => Allocator::Unknown` arm of
    /// `allocator_from_maps` to `Allocator::System`.
    #[test]
    fn malformed_maps_is_unknown_not_a_guess() {
        let maps = "this is not /proc/self/maps\njust some random text\n";
        assert_eq!(allocator_from_maps(maps), Allocator::Unknown);
    }

    /// Two DIFFERENT allocator libraries mapped at once is the honest unknown,
    /// not whichever one a fixed scan order names first. The realistic shape:
    /// `LD_PRELOAD=libjemalloc.so` governs allocation while `libtcmalloc.so`
    /// rides in as another library's transitive dependency. Asserted in both
    /// mapping orders, so a scan that resolved the tie by position rather than
    /// refusing it fails on one of the two.
    ///
    /// To watch it fail: change the `Some(_) => ambiguous = true` arm in
    /// `allocator_from_maps` to `Some(_) => {}`. That is the pre-change
    /// first-match behaviour, and the assertions then read
    /// `tcmalloc == unknown` and `jemalloc == unknown`.
    #[test]
    fn two_distinct_mapped_allocators_are_unknown_not_a_first_match_guess() {
        let tcmalloc_first = "\
55a1c0000000-55a1c0021000 r--p 00000000 08:01 100 /usr/bin/ravel-bench
7f2a10000000-7f2a10025000 r-xp 00000000 08:01 200 /usr/lib/x86_64-linux-gnu/libtcmalloc.so.4.5.9
7f2a18000000-7f2a18025000 r-xp 00000000 08:01 210 /usr/lib/x86_64-linux-gnu/libjemalloc.so.2
7f2a20000000-7f2a20030000 r-xp 00000000 08:01 201 /usr/lib/x86_64-linux-gnu/libc.so.6
";
        let jemalloc_first = "\
55a1c0000000-55a1c0021000 r--p 00000000 08:01 100 /usr/bin/ravel-bench
7f2a10000000-7f2a10025000 r-xp 00000000 08:01 210 /usr/lib/x86_64-linux-gnu/libjemalloc.so.2
7f2a18000000-7f2a18025000 r-xp 00000000 08:01 200 /usr/lib/x86_64-linux-gnu/libtcmalloc.so.4.5.9
7f2a20000000-7f2a20030000 r-xp 00000000 08:01 201 /usr/lib/x86_64-linux-gnu/libc.so.6
";
        assert_eq!(
            allocator_from_maps(tcmalloc_first),
            Allocator::Unknown,
            "two mapped allocators cannot resolve to the one listed first"
        );
        assert_eq!(
            allocator_from_maps(jemalloc_first),
            Allocator::Unknown,
            "the answer does not depend on the mapping order either"
        );
    }

    /// ONE allocator library mapped across its several segments is that
    /// allocator, not the ambiguous unknown. This is the normal shape of a real
    /// `/proc/self/maps`: the loader maps each object as an `r--p`/`r-xp`/`rw-p`
    /// run of segments, so a single preloaded jemalloc matches four lines here,
    /// beside an unrelated library mapped exactly the same way. This is the
    /// regression guard for the naive "count the matching lines" reading of the
    /// ambiguity rule, which would report Unknown for every single-allocator
    /// process: a worse defect than the first-match bug it replaced.
    ///
    /// To watch it fail: change the `Some(seen) if seen == allocator => {}` arm
    /// in `allocator_from_maps` to `Some(_seen) if false => {}`, which drops the
    /// distinct-library guard and leaves the scan counting matching lines. The
    /// assertion then reads `unknown == jemalloc`.
    #[test]
    fn one_allocator_across_several_segments_is_that_allocator() {
        let maps = "\
55a1c0000000-55a1c0021000 r--p 00000000 08:01 100 /usr/bin/ravel-bench
55a1c0021000-55a1c0089000 r-xp 00021000 08:01 100 /usr/bin/ravel-bench
7f2a10000000-7f2a10004000 r--p 00000000 08:01 210 /usr/lib/x86_64-linux-gnu/libjemalloc.so.2
7f2a10004000-7f2a10060000 r-xp 00004000 08:01 210 /usr/lib/x86_64-linux-gnu/libjemalloc.so.2
7f2a10060000-7f2a10062000 rw-p 00060000 08:01 210 /usr/lib/x86_64-linux-gnu/libjemalloc.so.2
7f2a10062000-7f2a10063000 ---p 00062000 08:01 210 /usr/lib/x86_64-linux-gnu/libjemalloc.so.2
7f2a20000000-7f2a20004000 r--p 00000000 08:01 220 /usr/lib/x86_64-linux-gnu/libssl.so.3
7f2a20004000-7f2a20050000 r-xp 00004000 08:01 220 /usr/lib/x86_64-linux-gnu/libssl.so.3
7f2a20050000-7f2a20052000 rw-p 00050000 08:01 220 /usr/lib/x86_64-linux-gnu/libssl.so.3
7f2a30000000-7f2a30030000 r-xp 00000000 08:01 201 /usr/lib/x86_64-linux-gnu/libc.so.6
7f2a40000000-7f2a40001000 rw-p 00000000 00:00 0
";
        assert_eq!(allocator_from_maps(maps), Allocator::Jemalloc);
    }
}
