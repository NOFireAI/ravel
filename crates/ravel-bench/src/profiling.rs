//! Optional CPU flamegraph lane for the ingest and query benchmark cores.
//!
//! This is the workspace's first CPU attribution of any kind: before it, every
//! claim about where ingest or query CPU goes was an argument from code shape,
//! not a measurement (issue #365). It is measurement infrastructure with no
//! production caller: the only thing that starts a `ProfileSession` is a human
//! (or CI) running one of the bench binaries with the environment variable set.
//! No production ingest/query path changes.
//!
//! The lane is gated behind the `profiling` cargo feature so a default build
//! never links `pprof`. When the feature is off this module is a set of no-op
//! stubs, so the bench cores can call it unconditionally.
//!
//! # Usage
//!
//! Set [`PROFILE_ENV`] to the SVG path you want written, then run a bench bin
//! built with `--features profiling`. The sampler brackets only the measured
//! region (it is started after fixture generation and stopped before the report
//! is assembled), so the flamegraph reflects the code under test rather than
//! workload construction. For a dense CPU picture, drive the ingest bench with
//! a high `--points-per-sec` (the total point count is `points-per-sec *
//! duration-secs`, so a value of `0` produces an empty workload and an empty
//! profile). CPU sampling only accrues on-CPU time, so the pacing this implies
//! does not dilute the flamegraph.
//!
//! # Frame pointers, and two refusals (issue #884)
//!
//! A release build omits frame pointers, and without them neither pprof's
//! unwinder nor `perf --call-graph dwarf` can walk out of inlined generic code:
//! Ravel's own hot path lands in `[unknown]`. One such profile put 33.95% of
//! samples there and sent a merge after the wrong call site; the same workload
//! rebuilt with `-C force-frame-pointers=yes` measured 0.00% and exposed the
//! real hot frame. Nothing about the two profiles looked different except the
//! flag.
//!
//! Build with `cargo profile-build` from `crates/ravel-bench` (a cargo alias in
//! that directory's `.cargo/config.toml`, which injects the flag through
//! `build.rustflags` so it reaches dependency crates too), or pass
//! `RUSTFLAGS="-C force-frame-pointers=yes"` on a build invoked from the
//! workspace root, where that alias is not discovered. Nothing inside this crate
//! can force the flag onto a `cargo run -p ravel-bench` launched from the
//! workspace root, so the flag is not the enforcement; these two are:
//!
//! - [`check_fp_prologue_count`] over the executable segments of the running
//!   binary, before the sampler is armed. A build that carries the flag in its
//!   configuration but did not apply it looks exactly like one that did, so the
//!   check reads the produced binary. See [`MIN_FP_PROLOGUES`].
//! - [`check_attribution`] over the finished profile. Above
//!   [`MAX_UNATTRIBUTED_SHARE`] the run exits non-zero instead of writing a
//!   flamegraph nobody should read.
//!
//! # The profiler perturbs the run it profiles
//!
//! This is a signal-based sampler: `pprof` arms `ITIMER_PROF`, and each
//! `SIGPROF` delivery unwinds the stack of the running thread. That unwind is
//! not free and it fires on the same threads doing the ingest/query work, so a
//! profiled run is measurably slower than an unprofiled one. The latency
//! figures a bench core reports while a profile is active (for example the ack
//! p50/p95/p99 in `ingest.rs`) are therefore inflated by the sampler and are
//! worse than the real latencies. Read the flamegraph for CPU *attribution*
//! only; quote latency numbers from an unprofiled run, never from a profiled
//! one.
//!
//! # Known instability under repeated execution
//!
//! A single [`ProfileSession`] holds one `pprof::ProfilerGuard` live across the
//! whole measured region. When that region runs each statement more than once
//! (`sql_latency_bench --runs N` with `N > 1`, where the guard brackets every
//! run of every corpus entry), the profiled process has been observed to
//! segfault; `--runs 1`, and any run with profiling off, is stable (issue
//! #616).
//!
//! The cause is upstream in the sampler, not in this crate's measurement loop
//! or in the SQL engine under test. `pprof` is a signal sampler: each `SIGPROF`
//! delivery unwinds the interrupted thread's stack from inside the signal
//! handler, and that unwind goes through the platform unwinder
//! (`backtrace::trace_unsynchronized`, i.e. libgcc's `_Unwind_Backtrace`),
//! which is not async-signal-safe. `pprof`'s own documentation carries the
//! gperftools warning verbatim: libgcc's unwind "is not safe to use from signal
//! handlers", and a tick that lands while a thread is mid-unwind or holds the
//! loader lock can deadlock or fault. The blocklist passed to the guard
//! mitigates but cannot close this: it only suppresses samples whose
//! interrupted instruction pointer is already inside a blocklisted segment, not
//! one that faults while unwinding out of application code.
//!
//! Why `--runs 3` crosses a line `--runs 1` does not: the plan, the data, and
//! the per-statement work are identical between the two. The only thing more
//! runs change is how long the guard stays armed, and therefore how many
//! async-signal-unsafe unwinds occur. The crash is probabilistic in that count,
//! so more executions raise the odds of one tick landing in the unsafe window.
//! That also means it is not deterministic on every host: it did not reproduce
//! under this crate's own repeated attempts on an aarch64 Linux box (default
//! and widened corpora, `--runs` up to 10, sampling frequency raised well past
//! the default), which is consistent with a probability that varies with
//! architecture, core count, and glibc rather than a fixed trigger. Do not read
//! a clean profiled `--runs 3` on one host as evidence the hazard is absent.
//!
//! Operational guidance: for a profiled pass, use `--runs 1`. One execution per
//! statement already produces a dense CPU flamegraph (the sampler accrues a
//! stack every ~1 ms of on-CPU time), and the latency numbers from a profiled
//! run are unusable anyway (see the section above), so the extra runs buy the
//! profile nothing while adding the exposure that risks the crash. Take latency
//! from a separate unprofiled multi-run pass.
//!
//! `sql_latency_bench` enforces this rather than trusting the operator to
//! remember it: a run with the profiler armed (`PROFILE_ENV` set and the
//! `profiling` feature compiled in) and `--runs > 1` is refused up front by
//! [`runs_supported_with_profiling`] before the guard is built, so the failure
//! is a clear error instead of a segfault partway through the first statement.
//! The profiler alone (`--runs 1`) and repeated runs alone (profiler off) are
//! both unaffected.
//!
//! # Concurrent-query segfault and the query-lane sampling rate (issue #680)
//!
//! The same hazard fires without repeated runs once the measured region goes
//! concurrent. On 2026-08-25 `sql_latency_bench` with `RAVEL_BENCH_PROFILE_SVG`
//! set died with exit 139 (SIGSEGV, no Rust panic) after 42 s, the first time
//! the logs scan lane ran eight segment prunes and eight scan partitions at
//! once; the same sampler had survived a 600 s run while the plan phase was
//! sequential, and the unprofiled run is fine. The mechanism is unchanged: each
//! `SIGPROF` unwind goes through libgcc's async-signal-unsafe path, and the
//! crash probability scales with the number of unwinds landing in the unsafe
//! window per unit of wall-clock. Concurrency multiplies that count without
//! multiplying the guard's lifetime, which is why sequential runs at the same
//! sampling rate survived.
//!
//! The fix lowers the sampling frequency for the query lanes only, from 997 Hz
//! to 199 Hz (`QUERY_SAMPLE_HZ`). This is the lever pprof's own documentation
//! makes available: the signal frequency directly sets how many unsafe unwinds
//! occur per second of on-CPU time, so cutting it about fivefold cuts the
//! exposure by the same factor, while 199 Hz (still twice pprof's default of
//! 99 Hz) keeps the flamegraph dense. The ingest lane keeps 997 Hz because its
//! measured region was sequential when the crash was observed.
//!
//! Two heavier options were considered and rejected for this task. Switching
//! pprof to its `frame-pointer` unwinder would sidestep libgcc entirely, but
//! pprof's README documents that unwinder as nightly-only, requiring
//! `cargo +nightly -Z build-std` to rebuild the standard library with correct
//! frame pointers, and even then "it's also not possible to ensure the safety
//! [...] the program will panic" on a bad frame pointer. That needs a
//! `.cargo/config` and toolchain change outside this crate, and pprof does not
//! document it as signal-safe, so it is not adopted here. Widening the blocklist
//! only suppresses samples whose interrupted instruction pointer already sits in
//! a blocklisted segment; it cannot suppress a fault while unwinding out of
//! application code, and the crash produced no Rust stack to point at new
//! libraries to add.
//!
//! When a flamegraph is needed for a workload that still faults under sampling,
//! `perf record --call-graph dwarf` on the host is the fallback: it unwinds out
//! of process and does not run inside the target's signal handler.

/// Environment variable naming the flamegraph SVG output path. When it is set
/// and the crate was built with the `profiling` feature, a bench core wraps its
/// measured region in a `pprof` CPU sampler and writes the flamegraph there. If
/// it is unset, no profiler is started even when the feature is compiled in.
pub const PROFILE_ENV: &str = "RAVEL_BENCH_PROFILE_SVG";

/// Refuse a profiled run that would execute each statement more than once
/// (issue #616). `pprof`'s signal sampler unwinds through libgcc's
/// async-signal-unsafe path on every `SIGPROF` tick, and the crash is
/// probabilistic in the number of unwinds; more runs of every statement raise
/// that count and the profiled process has been observed to segfault (see the
/// "Known instability under repeated execution" section of this module). One
/// guard around the whole measured region does not close the hazard, so rather
/// than segfault partway through a run this refuses the combination up front:
/// take the flamegraph from `--runs 1` (one execution already fills it) and the
/// latencies from a separate unprofiled multi-run pass.
///
/// Pure so the decision is testable without touching the process environment or
/// arming a real sampler: `profile_requested` is whether a sampler would run
/// (the `profiling` feature is on and [`PROFILE_ENV`] names a path), which
/// [`profile_requested`] resolves for the live path.
pub fn runs_supported_with_profiling(profile_requested: bool, runs: usize) -> Result<(), String> {
    if profile_requested && runs > 1 {
        return Err(format!(
            "--runs > 1 is not supported with {PROFILE_ENV}; the profiler and repeated execution \
             crash together, issue #616"
        ));
    }
    Ok(())
}

/// Whether a CPU sampler would actually run for this process: the `profiling`
/// feature is compiled in and [`PROFILE_ENV`] names a non-empty path. This is
/// the condition [`runs_supported_with_profiling`] gates on. With the feature
/// off no `pprof` guard is ever built, so the env var alone cannot crash a run
/// and this is always `false`.
#[cfg(feature = "profiling")]
pub fn profile_requested() -> bool {
    std::env::var_os(PROFILE_ENV).is_some_and(|v| !v.is_empty())
}

/// See the `profiling`-on variant. No sampler is linked in a default build, so
/// a set [`PROFILE_ENV`] cannot crash a repeated run and nothing is refused.
#[cfg(not(feature = "profiling"))]
pub fn profile_requested() -> bool {
    false
}

/// Maximum share of profile samples that may be unattributed (`[unknown]`)
/// before [`check_attribution`] refuses the profile.
///
/// A frame-pointer build of this workload measured 0.00% unattributed: the
/// issue #884 evidence is that rebuilding the same profile with
/// `-C force-frame-pointers=yes` took `[unknown]` from 33.95% to 0.00%. The
/// healthy value is therefore essentially zero, and this threshold is pure
/// headroom above it: a handful of genuinely unwindable ticks (a leaf entered
/// before its prologue ran, a boundary frame of the sampler itself) may resolve
/// to nothing without the profile being wrong. 2% sits an order of magnitude
/// below the 33.95% a build without frame pointers produced, so the failure this
/// guards -- a profile attributing tens of percent to `[unknown]` -- cannot
/// reach it, and a genuinely healthy profile cannot exceed it. The constant is
/// pinned by a test so loosening it is a deliberate edit, not a silent drift.
pub const MAX_UNATTRIBUTED_SHARE: f64 = 0.02;

/// Whether a resolved frame name attributes a sample to real code.
///
/// A frame the unwinder failed on surfaces under several names depending on the
/// tool that rendered it: `pprof` gives an unresolved frame the placeholder
/// `Unknown` (and drops a frame that yields no symbol at all, leaving an empty
/// name), `perf script` and the folded stacks inferno consumes write
/// `[unknown]`, and an unresolved address can come through as `??`. All of them
/// mean the same thing, so all of them count against the unattributed share.
pub fn frame_is_attributed(name: &str) -> bool {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return false;
    }
    !matches!(
        trimmed.to_ascii_lowercase().as_str(),
        "unknown" | "[unknown]" | "??" | "[unknown]:0"
    )
}

/// Fraction of `total` samples that were unattributed. Returns `0.0` for an
/// empty profile: no samples means nothing to attribute, which is a different
/// failure (an empty workload) from an unattributable one and not what
/// [`check_attribution`] guards.
pub fn unattributed_share(unattributed: u64, total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    unattributed as f64 / total as f64
}

/// Refuse a profile whose unattributed (`[unknown]`) share exceeds
/// [`MAX_UNATTRIBUTED_SHARE`]. Returns the measured share on success so the
/// caller can report it either way; the error names the likely cause (a build
/// without frame pointers) rather than the symptom, because that is the action
/// the operator must take. See issue #884: this exact ratio, read as "the hash
/// is cheap", was the unwinder failing, and the false negative shipped as #847.
pub fn check_attribution(unattributed: u64, total: u64) -> Result<f64, String> {
    let share = unattributed_share(unattributed, total);
    if share > MAX_UNATTRIBUTED_SHARE {
        return Err(format!(
            "[unknown] is {:.2}% of samples (threshold {:.2}%): this is almost certainly a build \
             without frame pointers, not a genuinely unattributable workload. Rebuild with \
             -C force-frame-pointers=yes (`cargo profile-build` from crates/ravel-bench, or \
             RUSTFLAGS=\"-C force-frame-pointers=yes\" on a build invoked from the workspace \
             root). Do not draw conclusions from this profile.",
            share * 100.0,
            MAX_UNATTRIBUTED_SHARE * 100.0
        ));
    }
    Ok(share)
}

/// The canonical x86-64 frame-pointer function prologue: `push %rbp` (0x55)
/// followed by `mov %rsp, %rbp` (0x48 0x89 0xe5). A `-C force-frame-pointers=yes`
/// build emits this at the head of essentially every non-leaf function; an
/// omit-frame-pointers build emits it for almost none. Scanning for the 4-byte
/// sequence needs no `objdump` and no debug info.
#[cfg(target_arch = "x86_64")]
const FP_PROLOGUE_X86_64: [u8; 4] = [0x55, 0x48, 0x89, 0xe5];

/// Plausibility floor for [`count_fp_prologues`] over a real bench binary's
/// executable segments.
///
/// Derived from measurement, not from taste. On the executor that implemented
/// issue #884 (x86-64 Linux, rustc from `rust-toolchain.toml`), in the PF_X
/// `PT_LOAD` segment of a release build of `ingest_bench` -- the smallest
/// profiling bin in this crate, so the weakest case for any floor:
///
/// | build | prologues | text bytes |
/// |---|---|---|
/// | `cargo profile-build` (`-C force-frame-pointers=yes`) | 9,739 | 8,662,848 |
/// | `cargo build --release --features profiling` (no flag) | 1,519 | 8,702,080 |
///
/// `query_latency_bench`, the other profiling bin, measured 12,712 with the
/// flag. A release build without the flag still emits the sequence for the
/// functions that genuinely need `%rbp`, which is why the no-flag figure is far
/// from zero and why a bare `> 0` check would pass a build the profile cannot
/// attribute.
///
/// The floor is 4,800: about half the 9,739 the weakest frame-pointer build
/// produced (so a bin with fewer functions, or a compiler that inlines more
/// aggressively, still passes), and 3.2x the 1,519 the same bin produced without
/// the flag (so the failure this exists to catch cannot reach it). Pinned by a
/// test.
pub const MIN_FP_PROLOGUES: usize = 4_800;

/// Byte prefix of the executable read to locate its executable segments. The
/// ELF program header table sits immediately after the 64-byte header in every
/// binary rustc/lld produce, so this is generous; the check reports rather than
/// guesses if the table lies past it.
#[cfg(target_arch = "x86_64")]
pub const ELF_HEADER_PREFIX_BYTES: usize = 64 * 1024;

/// Chunk size for [`count_fp_prologues_streamed`]. The executable segment of a
/// bench bin runs to tens of megabytes, and the profiling host is memory-bound
/// (the profiled run itself wants that memory), so the scan streams instead of
/// reading the image into one buffer.
pub const FP_SCAN_CHUNK_BYTES: usize = 64 * 1024;

/// Count canonical x86-64 frame-pointer prologues in a binary image. Pure over
/// the byte slice so it is testable without a real binary; the live check in
/// [`ProfileSession`] scans the executable segments of `std::env::current_exe`
/// through [`count_fp_prologues_streamed`].
#[cfg(target_arch = "x86_64")]
pub fn count_fp_prologues(image: &[u8]) -> usize {
    image
        .windows(FP_PROLOGUE_X86_64.len())
        .filter(|w| *w == FP_PROLOGUE_X86_64)
        .count()
}

/// Count prologues over `len` bytes read from `reader` in `chunk`-sized reads.
///
/// Each chunk keeps the last `FP_PROLOGUE_X86_64.len() - 1` bytes of the
/// previous one, so a prologue straddling a chunk boundary is counted exactly
/// once: a retained tail of 3 bytes cannot hold a 4-byte match on its own, so
/// every match it participates in extends into freshly read bytes and is seen by
/// exactly one iteration. A scan that dropped the carry-over would undercount by
/// up to 3 prologues per chunk boundary, which over a 30 MB segment is hundreds
/// of missed frames.
#[cfg(target_arch = "x86_64")]
pub fn count_fp_prologues_streamed<R: std::io::Read>(
    mut reader: R,
    len: u64,
    chunk: usize,
) -> std::io::Result<usize> {
    let overlap = FP_PROLOGUE_X86_64.len() - 1;
    let chunk = chunk.max(FP_PROLOGUE_X86_64.len());
    let mut scratch = vec![0u8; chunk];
    let mut window: Vec<u8> = Vec::with_capacity(chunk + overlap);
    let mut remaining = len;
    let mut count = 0usize;

    while remaining > 0 {
        let want = chunk.min(usize::try_from(remaining).unwrap_or(chunk));
        let read = reader.read(&mut scratch[..want])?;
        if read == 0 {
            break; // segment shorter than its header claims; count what exists
        }
        remaining -= read as u64;
        window.extend_from_slice(&scratch[..read]);
        count += count_fp_prologues(&window);
        let drop_to = window.len().saturating_sub(overlap);
        window.drain(..drop_to);
    }
    Ok(count)
}

/// Byte offset and length of every executable (`PT_LOAD` with `PF_X`) segment
/// declared by an ELF64 little-endian image, read from a prefix covering its
/// header and program header table.
///
/// The prologue scan reads these ranges only. Scanning the whole file would fold
/// in read-only data, debug info, and the symbol table, where the 4-byte pattern
/// occurs at rates that have nothing to do with whether code carries frame
/// pointers -- the count would move with the amount of debug info, not with the
/// flag under test.
///
/// Returns `Err` with a reason when the image is not an ELF64 LE binary or the
/// header table lies outside `prefix`. That is a "the check cannot run here"
/// answer (a Mach-O host, a truncated read), not a verdict on the binary.
#[cfg(target_arch = "x86_64")]
pub fn elf64_executable_ranges(prefix: &[u8]) -> Result<Vec<(u64, u64)>, String> {
    const EI_NIDENT: usize = 16;
    const ELFCLASS64: u8 = 2;
    const ELFDATA2LSB: u8 = 1;
    const PT_LOAD: u32 = 1;
    const PF_X: u32 = 1;

    if prefix.len() < EI_NIDENT {
        return Err(format!(
            "image is {} bytes, too short for an ELF header",
            prefix.len()
        ));
    }
    if &prefix[..4] != b"\x7fELF" {
        return Err("image is not ELF (no \\x7fELF magic)".to_string());
    }
    if prefix[4] != ELFCLASS64 || prefix[5] != ELFDATA2LSB {
        return Err(format!(
            "image is not ELF64 little-endian (class {}, data {})",
            prefix[4], prefix[5]
        ));
    }
    if prefix.len() < 64 {
        return Err(format!(
            "image is {} bytes, too short for an ELF64 header",
            prefix.len()
        ));
    }

    let u16_at = |off: usize| u16::from_le_bytes([prefix[off], prefix[off + 1]]) as usize;
    let u32_at = |off: usize| {
        u32::from_le_bytes([
            prefix[off],
            prefix[off + 1],
            prefix[off + 2],
            prefix[off + 3],
        ])
    };
    let u64_at = |off: usize| {
        u64::from_le_bytes([
            prefix[off],
            prefix[off + 1],
            prefix[off + 2],
            prefix[off + 3],
            prefix[off + 4],
            prefix[off + 5],
            prefix[off + 6],
            prefix[off + 7],
        ])
    };

    let phoff = usize::try_from(u64_at(0x20)).map_err(|_| "e_phoff exceeds usize".to_string())?;
    let phentsize = u16_at(0x36);
    let phnum = u16_at(0x38);
    if phentsize < 56 {
        return Err(format!("e_phentsize is {phentsize}, too small for ELF64"));
    }
    let table_end = phentsize
        .checked_mul(phnum)
        .and_then(|size| phoff.checked_add(size))
        .ok_or_else(|| "program header table size overflows usize".to_string())?;
    if table_end > prefix.len() {
        return Err(format!(
            "program header table ends at byte {table_end}, past the {} bytes read",
            prefix.len()
        ));
    }

    let mut ranges = Vec::new();
    for i in 0..phnum {
        let base = phoff + i * phentsize;
        if u32_at(base) != PT_LOAD || u32_at(base + 4) & PF_X == 0 {
            continue;
        }
        let offset = u64_at(base + 0x08);
        let filesz = u64_at(base + 0x20);
        if filesz > 0 {
            ranges.push((offset, filesz));
        }
    }
    Ok(ranges)
}

/// Refuse a binary carrying implausibly few frame-pointer prologues, which means
/// the profiling build's `-C force-frame-pointers=yes` did not take effect
/// (something overrode it, or it was never applied). Returns the count on
/// success. See issue #884, deliverable 3: a flag that silently did nothing
/// looks identical to one that worked, so verify the produced binary, not the
/// build configuration.
#[cfg(target_arch = "x86_64")]
pub fn check_fp_prologue_count(count: usize) -> Result<usize, String> {
    if count < MIN_FP_PROLOGUES {
        return Err(format!(
            "profiling build carries only {count} x86-64 frame-pointer prologues in its \
             executable segments (floor {MIN_FP_PROLOGUES}): -C force-frame-pointers=yes did not \
             take effect. Build with `cargo profile-build` from crates/ravel-bench, or with \
             RUSTFLAGS=\"-C force-frame-pointers=yes\" from the workspace root, so the flag \
             reaches every crate. Do not profile this binary: its stacks will not unwind."
        ));
    }
    Ok(count)
}

/// Count frame-pointer prologues in the executable segments of the ELF binary at
/// `path`, streaming each segment in [`FP_SCAN_CHUNK_BYTES`] chunks.
///
/// `Err` means the check could not run (not ELF64 LE, or the file could not be
/// read); the caller decides whether that is a refusal or a reported skip.
#[cfg(target_arch = "x86_64")]
pub fn count_exe_fp_prologues(path: &std::path::Path) -> Result<usize, String> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path)
        .map_err(|err| format!("cannot open {}: {err}", path.display()))?;
    let mut prefix = vec![0u8; ELF_HEADER_PREFIX_BYTES];
    let mut filled = 0usize;
    while filled < prefix.len() {
        let read = file
            .read(&mut prefix[filled..])
            .map_err(|err| format!("cannot read {}: {err}", path.display()))?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    prefix.truncate(filled);

    let ranges = elf64_executable_ranges(&prefix)?;
    if ranges.is_empty() {
        return Err(format!(
            "{} declares no executable PT_LOAD segment",
            path.display()
        ));
    }

    let mut count = 0usize;
    for (offset, len) in ranges {
        file.seek(SeekFrom::Start(offset))
            .map_err(|err| format!("cannot seek {} to {offset}: {err}", path.display()))?;
        count += count_fp_prologues_streamed(&mut file, len, FP_SCAN_CHUNK_BYTES)
            .map_err(|err| format!("cannot scan {} at {offset}: {err}", path.display()))?;
    }
    Ok(count)
}

/// Sampling frequency in Hz for the ingest lane. 997 (a prime near 1 kHz)
/// avoids aliasing against any periodic timer in the workload.
#[cfg(feature = "profiling")]
const SAMPLE_HZ: std::os::raw::c_int = 997;

/// Sampling frequency in Hz for the query lanes (`sql_latency_bench`,
/// `query_latency_bench`). Deliberately lower than the ingest lane; see the
/// "Concurrent-query segfault" section of the module doc (issue #680). 199 is a
/// prime near 200 Hz and still twice pprof's own default frequency of 99 Hz, so
/// the flamegraph stays dense while the count of async-signal-unsafe unwinds per
/// second of on-CPU time drops about fivefold.
#[cfg(feature = "profiling")]
const QUERY_SAMPLE_HZ: std::os::raw::c_int = 199;

#[cfg(feature = "profiling")]
mod imp {
    use std::path::PathBuf;

    use super::{PROFILE_ENV, QUERY_SAMPLE_HZ, SAMPLE_HZ};

    /// Verify the running bench binary was built with frame pointers before a
    /// real profiled run arms the sampler (issue #884, deliverable 3). A build
    /// that carries `-C force-frame-pointers=yes` in its configuration but did
    /// not actually apply it (an override, a stale binary) looks identical to
    /// one that worked until the flamegraph comes back as `[unknown]`, so this
    /// checks the produced binary rather than trusting the build: it counts
    /// x86-64 frame-pointer prologues in the executable segments of
    /// `current_exe` and refuses below [`super::MIN_FP_PROLOGUES`].
    ///
    /// Two outcomes other than pass/refuse, neither of them silent:
    ///
    /// - The check cannot run at all here (an architecture whose prologue is not
    ///   the x86-64 sequence, or an executable that is not ELF64 LE, such as a
    ///   Mach-O host). It says so on stderr and the run continues unverified.
    ///   Refusing would make the lane unusable on those platforms for a hazard
    ///   this check is simply not able to speak to.
    /// - The executable cannot be located or read on a platform where the check
    ///   does apply. That is a failure of the check, not an absence of it, so it
    ///   refuses: arming the sampler would produce exactly the unattributable
    ///   profile this exists to prevent.
    fn enforce_frame_pointer_build() {
        #[cfg(not(target_arch = "x86_64"))]
        {
            eprintln!(
                "profiling: frame-pointer verification does not run on {}: the prologue byte \
                 sequence it counts (push %rbp; mov %rsp, %rbp) is x86-64 specific. Continuing \
                 UNVERIFIED; if the profile comes back mostly [unknown], rebuild with \
                 -C force-frame-pointers=yes.",
                std::env::consts::ARCH
            );
        }
        #[cfg(target_arch = "x86_64")]
        {
            let exe = match std::env::current_exe() {
                Ok(p) => p,
                Err(err) => {
                    eprintln!(
                        "profiling: could not locate current executable to verify frame pointers: {err}. \
                         Refusing to profile: an unverified build produces an unattributable profile."
                    );
                    std::process::exit(1);
                }
            };
            let count = match super::count_exe_fp_prologues(&exe) {
                Ok(count) => count,
                Err(msg) => {
                    // The scan could not run over this image. Distinguish a
                    // format the check does not understand (say so, continue)
                    // from a file it could not read (refuse): the first is the
                    // check being inapplicable, the second is it failing.
                    if msg.contains("not ELF") || msg.contains("not ELF64") {
                        eprintln!(
                            "profiling: frame-pointer verification cannot run on this executable: \
                             {msg}. Continuing UNVERIFIED; if the profile comes back mostly \
                             [unknown], rebuild with -C force-frame-pointers=yes."
                        );
                        return;
                    }
                    eprintln!(
                        "profiling: {msg}. Refusing to profile: an unverified build produces an \
                         unattributable profile."
                    );
                    std::process::exit(1);
                }
            };
            match super::check_fp_prologue_count(count) {
                Ok(count) => {
                    eprintln!(
                        "profiling: verified {count} frame-pointer prologues (floor {}) in {}",
                        super::MIN_FP_PROLOGUES,
                        exe.display()
                    );
                }
                Err(msg) => {
                    eprintln!("profiling: {msg}");
                    std::process::exit(1);
                }
            }
        }
    }

    /// Count total and unattributed samples in a resolved report. A stack is
    /// unattributed when it resolves to no application frame: `pprof` drops a
    /// frame that yields no symbol at all and gives an unresolved frame a
    /// placeholder name, so a stack with no frame [`super::frame_is_attributed`]
    /// accepts is one the unwinder failed on -- the `[unknown]` share this
    /// guards. Returns `(unattributed, total)`.
    fn attribution_counts(report: &pprof::Report) -> (u64, u64) {
        let mut total: u64 = 0;
        let mut unattributed: u64 = 0;
        for (frames, count) in &report.data {
            let c = (*count).max(0) as u64;
            total += c;
            let has_named_frame = frames
                .frames
                .iter()
                .flatten()
                .any(|s| super::frame_is_attributed(&s.name()));
            if !has_named_frame {
                unattributed += c;
            }
        }
        (unattributed, total)
    }

    /// The query lanes run their statements across many worker threads
    /// concurrently, and each `SIGPROF` tick unwinds through libgcc, which is
    /// not async-signal-safe, so more concurrent unwinds per second of
    /// wall-clock raise the odds of a fault (issue #680). Sample those lanes
    /// slower. The ingest lane, whose measured region was sequential when the
    /// crash was observed, keeps the higher rate.
    fn sample_hz_for(label: &str) -> std::os::raw::c_int {
        if label.contains("sql") || label.contains("query") {
            QUERY_SAMPLE_HZ
        } else {
            SAMPLE_HZ
        }
    }

    /// An active (or inert) profiling session. Created via
    /// [`ProfileSession::from_env`] or [`ProfileSession::to_path`]; call
    /// [`ProfileSession::finish`] once the measured region ends to write the
    /// SVG. Holds a `pprof::ProfilerGuard` only while sampling.
    pub struct ProfileSession {
        active: Option<Active>,
    }

    struct Active {
        guard: pprof::ProfilerGuard<'static>,
        path: PathBuf,
        label: String,
        /// Whether [`ProfileSession::finish`] exits the process when the
        /// unattributed-share check fails. True for a real profiled run started
        /// from [`ProfileSession::from_env`] (refuse a profile that cannot
        /// attribute itself), false for [`ProfileSession::to_path`] callers such
        /// as this crate's own tests (report the share, do not kill the run).
        enforce: bool,
    }

    impl ProfileSession {
        /// Starts a session iff [`PROFILE_ENV`] names a path; otherwise returns
        /// an inert session whose `finish` does nothing.
        ///
        /// Before arming the sampler it verifies the running binary was built
        /// with frame pointers (issue #884): a profile off a binary without them
        /// is worthless, so refuse up front rather than produce it. This is the
        /// real-run path, so its profile is enforced -- `finish` refuses a
        /// profile that cannot attribute itself.
        ///
        /// See issue #680: the sampler must survive a concurrent query plan.
        pub fn from_env(label: &str) -> Self {
            match std::env::var_os(PROFILE_ENV) {
                Some(p) if !p.is_empty() => {
                    enforce_frame_pointer_build();
                    Self::build(label, PathBuf::from(p), true)
                }
                _ => ProfileSession { active: None },
            }
        }

        /// Whether a sampler is currently running (a session that resolved a
        /// path and built a guard). Bench cores read this to switch off their
        /// own concurrent instrumentation while profiling: pprof samples every
        /// on-CPU thread for the life of the guard, so a poller or sampler
        /// running alongside the code under test lands in the same flamegraph.
        pub fn is_active(&self) -> bool {
            self.active.is_some()
        }

        /// Starts a session writing to an explicit path, ignoring the
        /// environment. Used by the crate's own test and by any caller that has
        /// already resolved an output path. Does not enforce the
        /// unattributed-share check on `finish` (it reports the share instead)
        /// nor the frame-pointer build check, so a test can drive the sampler
        /// without the process exiting under it.
        pub fn to_path(label: &str, path: PathBuf) -> Self {
            Self::build(label, path, false)
        }

        /// Shared constructor for [`from_env`](Self::from_env) and
        /// [`to_path`](Self::to_path). `enforce` records whether `finish` refuses
        /// (exits) or merely reports when the profile fails the attribution
        /// check.
        fn build(label: &str, path: PathBuf, enforce: bool) -> Self {
            let hz = sample_hz_for(label);
            match pprof::ProfilerGuardBuilder::default()
                .frequency(hz)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
            {
                Ok(guard) => {
                    eprintln!("profiling: sampling '{label}' at {hz} Hz");
                    ProfileSession {
                        active: Some(Active {
                            guard,
                            path,
                            label: label.to_string(),
                            enforce,
                        }),
                    }
                }
                Err(err) => {
                    eprintln!("profiling: could not start sampler: {err}");
                    ProfileSession { active: None }
                }
            }
        }

        /// Stops sampling and writes the flamegraph SVG. Errors are reported to
        /// stderr and swallowed rather than propagated, so a profiling failure
        /// never discards the run's own report. Returns the written path when a
        /// session was active and the write succeeded.
        pub fn finish(self) -> Option<PathBuf> {
            let active = self.active?;
            let report = match active.guard.report().build() {
                Ok(report) => report,
                Err(err) => {
                    eprintln!("profiling: could not build report: {err}");
                    return None;
                }
            };
            // Refuse a profile that cannot attribute itself (issue #884). On the
            // enforced (real-run) path a failure exits the process before the
            // SVG is written, so a bogus flamegraph is never mistaken for a
            // finding; the unenforced path (tests, direct callers) reports the
            // share and continues.
            let (unattributed, total) = attribution_counts(&report);
            match super::check_attribution(unattributed, total) {
                Ok(share) => {
                    eprintln!(
                        "profiling: {:.2}% of {total} samples unattributed (threshold {:.2}%)",
                        share * 100.0,
                        super::MAX_UNATTRIBUTED_SHARE * 100.0
                    );
                }
                Err(msg) => {
                    eprintln!("profiling: {msg}");
                    if active.enforce {
                        std::process::exit(1);
                    }
                }
            }
            let file = match std::fs::File::create(&active.path) {
                Ok(file) => file,
                Err(err) => {
                    eprintln!(
                        "profiling: could not create {}: {err}",
                        active.path.display()
                    );
                    return None;
                }
            };
            match report.flamegraph(file) {
                Ok(()) => {
                    eprintln!(
                        "profiling: wrote '{}' flamegraph to {}",
                        active.label,
                        active.path.display()
                    );
                    Some(active.path)
                }
                Err(err) => {
                    eprintln!("profiling: could not write flamegraph: {err}");
                    None
                }
            }
        }
    }
}

#[cfg(not(feature = "profiling"))]
mod imp {
    use std::path::PathBuf;

    use super::PROFILE_ENV;

    /// No-op stub used when the `profiling` feature is off, so the bench cores
    /// can call the profiling API unconditionally without linking `pprof`.
    pub struct ProfileSession;

    impl ProfileSession {
        pub fn from_env(_label: &str) -> Self {
            if std::env::var_os(PROFILE_ENV).is_some_and(|v| !v.is_empty()) {
                eprintln!(
                    "profiling: {PROFILE_ENV} is set but this binary was built without the \
                     `profiling` feature; no flamegraph will be written. Rebuild with \
                     `--features profiling`."
                );
            }
            ProfileSession
        }

        pub fn to_path(_label: &str, _path: PathBuf) -> Self {
            ProfileSession
        }

        pub fn is_active(&self) -> bool {
            false
        }

        pub fn finish(self) -> Option<PathBuf> {
            None
        }
    }
}

pub use imp::ProfileSession;

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod manifest_tests {
    /// Guards that `pprof` cannot leak into a default build: it must be an
    /// `optional` dependency, and the only feature that activates it must be
    /// `profiling`. A mechanical read of this crate's own manifest, so it runs
    /// under the default `cargo test` (which does not link `pprof`) and needs
    /// no `cargo tree` subprocess. Equivalent to the `cargo tree -p
    /// ravel-bench` check the task calls for, but enforced in CI on every run.
    #[test]
    fn pprof_is_optional_and_only_behind_the_profiling_feature() {
        let manifest = include_str!("../Cargo.toml");

        let dep_line = manifest
            .lines()
            .find(|l| l.trim_start().starts_with("pprof ="))
            .expect("pprof dependency line present in Cargo.toml");
        assert!(
            dep_line.contains("optional = true"),
            "pprof must be an optional dependency so a default build never links it; got: {dep_line}"
        );

        // No feature other than `profiling` may activate pprof (as `dep:pprof`
        // or by bare name). Scan the [features] table line by line.
        let mut in_features = false;
        let mut current_feature = String::new();
        for line in manifest.lines() {
            let trimmed = line.trim();
            if trimmed == "[features]" {
                in_features = true;
                continue;
            }
            if in_features && trimmed.starts_with('[') {
                break; // left the [features] table
            }
            if !in_features {
                continue;
            }
            if let Some((name, _)) = trimmed.split_once('=') {
                let name = name.trim();
                if !name.is_empty() && !name.starts_with('#') {
                    current_feature = name.to_string();
                }
            }
            if trimmed.contains("pprof") && !trimmed.starts_with('#') {
                assert_eq!(
                    current_feature, "profiling",
                    "pprof may only be activated by the `profiling` feature, found under `{current_feature}`: {trimmed}"
                );
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod refusal_tests {
    use super::{PROFILE_ENV, runs_supported_with_profiling};

    /// A profiled run (`RAVEL_BENCH_PROFILE_SVG` set, sampler linked) with
    /// `--runs > 1` is refused up front with the exact message, rather than
    /// segfaulting partway through the run (issue #616). Reverting the guard to
    /// an unconditional `Ok(())` makes the first assertion fail. The decision is
    /// pure so it needs no live sampler or process-env mutation.
    #[test]
    fn repeated_runs_refused_only_when_profiling() {
        // The offending combination: profiler on and more than one run.
        let err = runs_supported_with_profiling(true, 3)
            .expect_err("a profiled multi-run pass must be refused");
        assert_eq!(
            err,
            format!(
                "--runs > 1 is not supported with {PROFILE_ENV}; the profiler and repeated \
                 execution crash together, issue #616"
            )
        );

        // Either alone is fine: a single profiled run is the supported profiling
        // path, and repeated runs with the profiler off are stable.
        assert!(
            runs_supported_with_profiling(true, 1).is_ok(),
            "one profiled run is the supported flamegraph path"
        );
        assert!(
            runs_supported_with_profiling(false, 3).is_ok(),
            "repeated runs are stable when no sampler is armed"
        );
        // A boundary: exactly one run is never refused, profiler or not.
        assert!(runs_supported_with_profiling(false, 1).is_ok());
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod attribution_tests {
    use super::{MAX_UNATTRIBUTED_SHARE, check_attribution, unattributed_share};

    /// The threshold is pinned so a change is a deliberate edit that fails this
    /// test, not a silent loosening (issue #884). If this fails, the constant
    /// moved: justify the new value in its doc comment before updating the pin.
    #[test]
    fn threshold_is_pinned() {
        assert_eq!(MAX_UNATTRIBUTED_SHARE, 0.02);
    }

    /// The check must reject the exact ratio that caused the damage (33.95%) and
    /// name the cause -- a build without frame pointers -- since the whole value
    /// of the message is telling the next operator what to do. Asserting only on
    /// an obviously-bad 90% input would let 33.95% through.
    #[test]
    fn rejects_the_damaging_ratio_and_names_the_cause() {
        let err = check_attribution(3395, 10_000).expect_err("33.95% unattributed must be refused");
        assert!(
            err.contains("33.95% of samples"),
            "message must report the measured share, got: {err}"
        );
        assert!(
            err.contains("frame pointers"),
            "message must name the likely cause, got: {err}"
        );
        assert!(
            err.contains("-C force-frame-pointers=yes"),
            "message must name the fix, got: {err}"
        );
    }

    /// The refusal message is the deliverable, not just the non-zero exit: it
    /// must state the measured share AND the threshold it violated, so an
    /// operator reading only stderr knows both numbers (issue #884,
    /// deliverable 2). Dropping either interpolation from the message fails
    /// here.
    #[test]
    fn refusal_message_names_the_measured_share_and_the_threshold() {
        let err = check_attribution(1_000, 4_000).expect_err("25% unattributed must be refused");
        assert!(
            err.contains("25.00% of samples"),
            "message must state the measured share, got: {err}"
        );
        assert!(
            err.contains("threshold 2.00%"),
            "message must state the threshold, got: {err}"
        );
        assert!(
            err.contains("Do not draw conclusions from this profile"),
            "message must say the profile is unusable, got: {err}"
        );
    }

    /// A synthetic summary either side of the 2% line: 2.01% is refused, 1.99%
    /// passes and reports its share back (issue #884, deliverable 4). Flipping
    /// the comparison in `check_attribution` to `>=` leaves this passing, so the
    /// pair below is the discriminating case, not decoration: relaxing it to
    /// `share > 0.03` fails the first assertion and tightening it to
    /// `share > 0.015` fails the second.
    #[test]
    fn synthetic_summary_fails_at_2_01_and_passes_at_1_99() {
        // 20_100 of 1_000_000 samples = 2.01% -> refused.
        let over = check_attribution(20_100, 1_000_000)
            .expect_err("2.01% is over the threshold and must be refused");
        assert!(
            over.contains("2.01% of samples"),
            "message must state the measured 2.01%, got: {over}"
        );

        // 19_900 of 1_000_000 samples = 1.99% -> accepted, share returned.
        let under = check_attribution(19_900, 1_000_000).expect("1.99% is under the threshold");
        assert!(
            (under - 0.0199).abs() < 1e-12,
            "share reported back is 1.99%, got {under}"
        );
    }

    /// Every placeholder an unresolved frame arrives under counts as
    /// unattributed, whichever tool rendered it; a real symbol does not.
    #[test]
    fn placeholder_frame_names_are_unattributed() {
        for name in [
            "",
            "  ",
            "Unknown",
            "unknown",
            "[unknown]",
            "??",
            "[UNKNOWN]",
        ] {
            assert!(
                !super::frame_is_attributed(name),
                "{name:?} is an unresolved frame"
            );
        }
        for name in [
            "ravel_types::series::hash_one",
            "__memmove_avx_unaligned_erms",
        ] {
            assert!(
                super::frame_is_attributed(name),
                "{name:?} is a resolved frame"
            );
        }
    }

    /// The boundary is what matters: a check that only rejects 90% would let
    /// 30% through. Exactly at the threshold passes (`>` not `>=`), one sample
    /// over fails, and the pass case reports the measured share.
    #[test]
    fn boundary_at_the_threshold() {
        // Exactly 2.00% (200 / 10_000): at the threshold, not over it -> pass.
        let at = check_attribution(200, 10_000).expect("exactly at threshold passes");
        assert!(
            (at - 0.02).abs() < 1e-12,
            "share reported back is 2%, got {at}"
        );

        // One sample over 2.00% (201 / 10_000 = 2.01%) -> fail.
        let over = check_attribution(201, 10_000);
        assert!(
            over.is_err(),
            "201/10000 = 2.01% is over the 2% threshold and must fail, got {over:?}"
        );

        // Well under -> pass.
        assert!(
            check_attribution(100, 10_000).is_ok(),
            "1% is under the threshold"
        );

        // Empty profile is not an attribution failure.
        assert_eq!(check_attribution(0, 0).expect("empty profile passes"), 0.0);
    }

    #[test]
    fn share_is_zero_for_empty_profile() {
        assert_eq!(unattributed_share(0, 0), 0.0);
        assert_eq!(unattributed_share(1, 4), 0.25);
    }
}

#[cfg(all(test, target_arch = "x86_64"))]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod frame_pointer_tests {
    use super::{
        FP_PROLOGUE_X86_64, FP_SCAN_CHUNK_BYTES, MIN_FP_PROLOGUES, check_fp_prologue_count,
        count_exe_fp_prologues, count_fp_prologues, count_fp_prologues_streamed,
        elf64_executable_ranges,
    };

    /// The floor is pinned so lowering it is a deliberate edit that fails this
    /// test (issue #884, deliverable 3). It is derived from measurement: 9,739
    /// prologues in a `cargo profile-build --bin ingest_bench` binary against
    /// 1,519 in the same bin built without the flag, so the floor sits at about
    /// half the frame-pointer figure and above three times the other. See
    /// `MIN_FP_PROLOGUES`.
    #[test]
    fn floor_is_pinned() {
        assert_eq!(MIN_FP_PROLOGUES, 4_800);
        // Both measured builds must land on the correct side of this floor,
        // which is the whole point of not writing `> 0`.
        assert!(check_fp_prologue_count(1_519).is_err(), "no-flag build");
        assert!(check_fp_prologue_count(9_739).is_ok(), "ingest_bench, flag");
        assert!(
            check_fp_prologue_count(12_712).is_ok(),
            "query_latency_bench, flag"
        );
    }

    /// A buffer holding exactly N non-overlapping prologues counts as N, and
    /// bytes that are not the pattern are not counted.
    #[test]
    fn counts_exact_prologues() {
        let mut image = Vec::new();
        for _ in 0..5 {
            image.extend_from_slice(&FP_PROLOGUE_X86_64);
            // Filler that is not the prologue between occurrences.
            image.extend_from_slice(&[0x90, 0x90, 0x90]);
        }
        assert_eq!(count_fp_prologues(&image), 5);
        assert_eq!(count_fp_prologues(&[0x90; 64]), 0);
        assert_eq!(count_fp_prologues(&[]), 0);
    }

    /// Build a fixture of `len` filler bytes with a prologue planted at each
    /// offset in `at`.
    fn fixture_with_prologues_at(len: usize, at: &[usize]) -> Vec<u8> {
        let mut image = vec![0x90u8; len];
        for &offset in at {
            image[offset..offset + FP_PROLOGUE_X86_64.len()].copy_from_slice(&FP_PROLOGUE_X86_64);
        }
        image
    }

    /// The streamed scan counts a known byte string exactly, including a
    /// prologue that straddles a chunk boundary. A carry-over of fewer than 3
    /// bytes (or none) misses the straddling one and this fails; the whole-slice
    /// count is the oracle it must agree with.
    #[test]
    fn streamed_scan_counts_across_chunk_boundaries() {
        let chunk = 16usize;
        // Offsets: one wholly inside chunk 0, one straddling the 0/1 boundary
        // (starts at 14, ends at 17), one wholly inside chunk 2, one straddling
        // the 2/3 boundary by a single byte (starts at 47, ends at 50).
        let at = [4usize, 14, 34, 47];
        let image = fixture_with_prologues_at(64, &at);

        assert_eq!(count_fp_prologues(&image), at.len(), "whole-slice oracle");
        assert_eq!(
            count_fp_prologues_streamed(image.as_slice(), image.len() as u64, chunk)
                .expect("streamed scan over an in-memory fixture"),
            at.len(),
            "streamed scan must agree with the whole-slice count"
        );

        // Also at a chunk size of exactly the pattern length, where every
        // boundary is a potential straddle, and at the production chunk size,
        // where none is.
        for chunk in [4usize, FP_SCAN_CHUNK_BYTES] {
            assert_eq!(
                count_fp_prologues_streamed(image.as_slice(), image.len() as u64, chunk)
                    .expect("streamed scan"),
                at.len(),
                "chunk size {chunk} must not change the count"
            );
        }

        // A prologue that begins inside the scanned range but ends past it is
        // NOT counted: only whole occurrences inside the range count. The
        // fixture is 66 bytes with a prologue at 62; the scan is told 64.
        let straddles_the_end = fixture_with_prologues_at(66, &[62]);
        assert_eq!(count_fp_prologues(&straddles_the_end[..64]), 0, "oracle");
        assert_eq!(
            count_fp_prologues_streamed(straddles_the_end.as_slice(), 64, chunk)
                .expect("streamed scan"),
            0
        );
    }

    /// The plausibility floor separates a frame-pointer build from an
    /// omit-frame-pointers one at the boundary specifically: exactly
    /// `MIN_FP_PROLOGUES` passes, one fewer fails and reports the count it saw.
    #[test]
    fn boundary_at_the_floor() {
        // Exactly at the floor -> pass, returns the count.
        assert_eq!(
            check_fp_prologue_count(MIN_FP_PROLOGUES).expect("at the floor passes"),
            MIN_FP_PROLOGUES
        );

        // One below the floor -> fail, and the message reports the shortfall
        // and the fix.
        let err =
            check_fp_prologue_count(MIN_FP_PROLOGUES - 1).expect_err("below the floor must fail");
        assert!(
            err.contains(&format!("only {} x86-64", MIN_FP_PROLOGUES - 1)),
            "message must report the count it saw, got: {err}"
        );
        assert!(
            err.contains(&format!("floor {MIN_FP_PROLOGUES}")),
            "message must report the floor it violated, got: {err}"
        );
        assert!(
            err.contains("-C force-frame-pointers=yes"),
            "message must name the fix, got: {err}"
        );

        // A stripped binary (near zero prologues) fails hard.
        assert!(check_fp_prologue_count(0).is_err());
    }

    /// Minimal ELF64 LE image: a 64-byte header plus `segments` program headers,
    /// each `(p_type, p_flags, p_offset, p_filesz)`, and `body` appended after
    /// the table.
    fn elf64_image(segments: &[(u32, u32, u64, u64)], body: &[u8]) -> Vec<u8> {
        const PHENTSIZE: usize = 56;
        let phoff: u64 = 64;
        let mut image = vec![0u8; 64];
        image[..4].copy_from_slice(b"\x7fELF");
        image[4] = 2; // ELFCLASS64
        image[5] = 1; // ELFDATA2LSB
        image[0x20..0x28].copy_from_slice(&phoff.to_le_bytes());
        image[0x36..0x38].copy_from_slice(&(PHENTSIZE as u16).to_le_bytes());
        image[0x38..0x3a].copy_from_slice(&(segments.len() as u16).to_le_bytes());
        for &(p_type, p_flags, p_offset, p_filesz) in segments {
            let mut phdr = vec![0u8; PHENTSIZE];
            phdr[0..4].copy_from_slice(&p_type.to_le_bytes());
            phdr[4..8].copy_from_slice(&p_flags.to_le_bytes());
            phdr[0x08..0x10].copy_from_slice(&p_offset.to_le_bytes());
            phdr[0x20..0x28].copy_from_slice(&p_filesz.to_le_bytes());
            image.extend_from_slice(&phdr);
        }
        assert_eq!(
            image.len(),
            64 + segments.len() * PHENTSIZE,
            "phdr table must end where the body begins"
        );
        image.extend_from_slice(body);
        image
    }

    /// Only executable `PT_LOAD` segments are scanned. A non-executable load
    /// segment (rodata, where the 4-byte pattern occurs for reasons unrelated to
    /// frame pointers) must not contribute.
    #[test]
    fn only_executable_load_segments_are_scanned() {
        const PT_LOAD: u32 = 1;
        const PT_NOTE: u32 = 4;
        const PF_X: u32 = 1;
        const PF_R: u32 = 4;

        let ranges = elf64_executable_ranges(&elf64_image(
            &[
                (PT_LOAD, PF_R, 0x1000, 0x400),        // rodata: skipped
                (PT_LOAD, PF_R | PF_X, 0x2000, 0x800), // text: scanned
                (PT_NOTE, PF_X, 0x3000, 0x100),        // not PT_LOAD: skipped
                (PT_LOAD, PF_R | PF_X, 0x4000, 0),     // empty: skipped
            ],
            &[],
        ))
        .expect("parse synthetic ELF64");
        assert_eq!(ranges, vec![(0x2000, 0x800)]);
    }

    /// A non-ELF or non-ELF64 image is reported as "the check cannot run here",
    /// with a reason, not as a verdict on the binary. The live path keys the
    /// continue-unverified branch off this wording.
    #[test]
    fn non_elf_images_report_that_the_check_cannot_run() {
        // A 64-bit Mach-O header (the macOS case), long enough that the
        // too-short branch cannot be what rejects it.
        let mut macho = vec![0u8; 32];
        macho[..8].copy_from_slice(&[0xcf, 0xfa, 0xed, 0xfe, 0x07, 0x00, 0x00, 0x01]);
        let err = elf64_executable_ranges(&macho).expect_err("Mach-O is not ELF");
        assert!(err.contains("not ELF"), "got: {err}");

        let mut elf32 = elf64_image(&[], &[]);
        elf32[4] = 1; // ELFCLASS32
        let err = elf64_executable_ranges(&elf32).expect_err("ELF32 is not scanned");
        assert!(err.contains("not ELF64"), "got: {err}");

        let err = elf64_executable_ranges(&[]).expect_err("empty image");
        assert!(err.contains("too short"), "got: {err}");
    }

    /// End to end over a file on disk: the scan reads the executable segment of
    /// a synthetic ELF64 binary and counts exactly the prologues planted there,
    /// ignoring the identical bytes planted in its non-executable segment.
    #[test]
    fn counts_prologues_in_a_file_on_disk() {
        const PT_LOAD: u32 = 1;
        const PF_X: u32 = 1;
        const PF_R: u32 = 4;

        // Body layout after the header + one 56-byte phdr table entry pair:
        // rodata at offset 0x200 (len 0x100, 7 prologues, must be ignored),
        // text at offset 0x400 (len 0x200, 3 prologues, must be counted).
        let mut body = vec![0x90u8; 0x600];
        let plant = |body: &mut Vec<u8>, base: usize, offsets: &[usize]| {
            for &off in offsets {
                let at = base + off;
                body[at..at + FP_PROLOGUE_X86_64.len()].copy_from_slice(&FP_PROLOGUE_X86_64);
            }
        };
        // Body starts at file offset 0x80 (64-byte header + 2 x 56-byte phdrs =
        // 176 = 0xb0); index into `body` is file_offset - 0xb0.
        let body_base = 64 + 2 * 56;
        plant(&mut body, 0x200 - body_base, &[0, 8, 16, 24, 32, 40, 48]);
        plant(&mut body, 0x400 - body_base, &[0, 100, 0x1fc]);

        let image = elf64_image(
            &[
                (PT_LOAD, PF_R, 0x200, 0x100),
                (PT_LOAD, PF_R | PF_X, 0x400, 0x200),
            ],
            &body,
        );

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("synthetic-elf");
        std::fs::write(&path, &image).expect("write fixture binary");

        assert_eq!(
            count_exe_fp_prologues(&path).expect("scan the fixture"),
            3,
            "only the executable segment's prologues count"
        );
    }
}

#[cfg(all(test, feature = "profiling"))]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The lane produces a non-empty SVG flamegraph for a short CPU-bound run.
    /// Only compiled with the `profiling` feature, so the default `cargo test`
    /// neither runs it nor links `pprof`.
    #[test]
    fn profiling_lane_writes_non_empty_svg() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "ravel-bench-profiling-test-{}.svg",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let session = ProfileSession::to_path("unit-test", path.clone());
        // Busy-spin so the CPU sampler collects at least a few stacks. This has
        // to burn real on-CPU time; a sleep would accrue no ITIMER_PROF ticks.
        let mut acc: u64 = 0;
        for i in 0..50_000_000u64 {
            acc = acc.wrapping_add(i).wrapping_mul(2_654_435_761);
        }
        std::hint::black_box(acc);
        let written = session.finish();

        assert_eq!(written.as_deref(), Some(path.as_path()));
        let bytes = std::fs::read(&path).expect("read written flamegraph");
        assert!(!bytes.is_empty(), "flamegraph SVG must not be empty");
        let head = String::from_utf8_lossy(&bytes[..bytes.len().min(256)]);
        assert!(
            head.contains("<svg") || head.contains("<?xml"),
            "written file should be an SVG document, got: {head}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
