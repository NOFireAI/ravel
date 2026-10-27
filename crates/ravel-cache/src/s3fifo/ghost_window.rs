//! Ghost window simulations (issue #3270): [`S3Fifo`] driven directly with
//! the three workloads its ghost sizing has to serve at once.
//!
//! - W1, the ClickBench hot sweep at the benchmark box's RAM cache: 43
//!   statements in order, each a warm call and three timed calls, all
//!   interactive. Most statements read 0.5 to 2 GB drawn from a shared pool
//!   of columns, introducing at most two new columns each; q29, q31, q32 and
//!   q33 read keys of their own at the sizes the sweep measured.
//! - W2, ADR-0046 decision 6 at the server geometry ADR-2677's decision 3
//!   deferral cites: an 11.5 GB cache, a 33 MB hot set of 16 KiB ranges each
//!   read once per thousand reads of 25 MB objects that an interactive scan
//!   reads continuously, the scan looping over 0.20 to 0.65 of the cache or
//!   reading a new object every time.
//! - W3, ADR-2677's loop case at W1's cache: a loop of 0.5 to 3 times the
//!   cache, of 1.3 MB or 25 MB entries, read five times, then a second one.
//!
//! The `w1_`, `w2_` and `w3_` tests pin the shipped sizing against the key
//! window it replaced; `ghost_window_report` prints every candidate.

use super::*;

const MB: u64 = 1_000_000;
const MIB: u64 = 1024 * 1024;

/// A ghost sizing under test.
#[derive(Clone, Copy, Debug)]
pub(super) enum Window {
    /// The sizing [`S3Fifo::new`] ships.
    Shipped,
    /// `max_bytes / max_entry_bytes` keys, the sizing before issue #3270.
    LargestEntryKeys,
    /// Ghost keys summing to at most this many bytes, and `max_entries`.
    Bytes(u64),
    /// The wider of the two: a ghost key is kept while it is within either
    /// the largest-entry key window or this many bytes, and `max_entries`.
    Either(u64),
    /// No bound at all: every key evicted from probation is remembered.
    Unbounded,
}

pub(super) fn fifo_with(limits: CacheLimits, window: Window) -> S3Fifo<()> {
    let mut fifo = S3Fifo::new(limits);
    if let Window::Shipped = window {
        return fifo;
    }
    let largest_entry_keys =
        (limits.max_bytes / limits.max_entry_bytes).min(limits.max_entries as u64) as usize;
    let (keys, window_keys, bytes) = match window {
        Window::Shipped | Window::LargestEntryKeys => (largest_entry_keys, 0, u64::MAX),
        Window::Bytes(bytes) => (limits.max_entries, 0, bytes),
        Window::Either(bytes) => (limits.max_entries, largest_entry_keys, bytes),
        Window::Unbounded => (usize::MAX, 0, u64::MAX),
    };
    fifo.ghost_max_keys = keys.max(1);
    fifo.ghost_window_keys = window_keys;
    fifo.ghost_window_bytes = bytes;
    fifo
}

fn key(tenant: u8, n: u64, len: u64) -> CacheKey {
    let mut content_hash = [0u8; 32];
    content_hash[..8].copy_from_slice(&n.to_le_bytes());
    CacheKey::new([tenant; 16], content_hash, 0, len)
}

/// One interactive read: a lookup, and on a miss an admission. True on a hit.
fn read(fifo: &mut S3Fifo<()>, key: CacheKey, metrics: &CacheMetrics) -> bool {
    if fifo.get(&key).is_some() {
        return true;
    }
    fifo.insert(key, (), key.len, metrics);
    false
}

/// Bytes served over bytes read, for one pass over `keys`.
fn pass(fifo: &mut S3Fifo<()>, keys: &[CacheKey], metrics: &CacheMetrics) -> f64 {
    let (mut hit, mut total) = (0u64, 0u64);
    for &k in keys {
        total += k.len;
        if read(fifo, k, metrics) {
            hit += k.len;
        }
    }
    hit as f64 / total as f64
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        splitmix64(self.0)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The benchmark box's RAM fetch cache and the server's two entry caps.
pub(super) const W1_MAX_BYTES: u64 = 6_954_184_212;

pub(super) fn w1_limits() -> CacheLimits {
    CacheLimits::new(W1_MAX_BYTES, 1_000_000, 64 * MIB)
}

const POOL_COLUMNS: u64 = 18;
const COLUMN_BYTES: u64 = 250 * MB;

/// The statements whose per-call bytes exceed the probation queue, with the
/// bytes and entry count each read per call.
pub(super) const LARGE: [(usize, u64, u64); 4] = [
    (29, 1_550 * MB, 1_192),
    (31, 1_300 * MB, 1_000),
    (32, 2_270 * MB, 1_632),
    (33, 1_890 * MB, 1_218),
];

/// The 43 statements' key lists, in order, `[0]` being q01.
pub(super) fn w1_statements() -> Vec<Vec<CacheKey>> {
    let mut rng = Rng(3270);
    let columns: Vec<Vec<CacheKey>> = (0..POOL_COLUMNS)
        .map(|c| {
            let mut column = Vec::new();
            let mut bytes = 0;
            while bytes < COLUMN_BYTES {
                let size = 800_000 + rng.below(1_000_000);
                column.push(key(1, c * 10_000 + column.len() as u64, size));
                bytes += size;
            }
            column
        })
        .collect();
    let mut introduced = 0u64;
    (1..=43usize)
        .map(|q| {
            if let Some(&(_, bytes, count)) = LARGE.iter().find(|(n, _, _)| *n == q) {
                return (0..count)
                    .map(|i| key(2, q as u64 * 100_000 + i, bytes / count))
                    .collect();
            }
            let want = 2 + rng.below(7);
            let mut chosen = Vec::new();
            while chosen.len() < 2 && introduced < POOL_COLUMNS && (chosen.len() as u64) < want {
                chosen.push(introduced);
                introduced += 1;
            }
            while (chosen.len() as u64) < want.min(introduced) {
                let c = rng.below(introduced);
                if !chosen.contains(&c) {
                    chosen.push(c);
                }
            }
            chosen.sort_unstable();
            chosen
                .iter()
                .flat_map(|&c| columns[c as usize].iter().copied())
                .collect()
        })
        .collect()
}

/// Each statement's working-set bytes and byte hit rate on its warm call and
/// three timed calls.
pub(super) fn run_w1(window: Window) -> Vec<(u64, [f64; 4])> {
    let metrics = CacheMetrics::default();
    let mut fifo = fifo_with(w1_limits(), window);
    w1_statements()
        .iter()
        .map(|keys| {
            let mut calls = [0.0; 4];
            for call in &mut calls {
                *call = pass(&mut fifo, keys, &metrics);
            }
            (keys.iter().map(|k| k.len).sum(), calls)
        })
        .collect()
}

pub(super) const W2_MAX_BYTES: u64 = 11_500_000_000;
const HOT_RANGE: u64 = 16 * 1024;
const HOT_RANGES: u64 = 33 * MB / HOT_RANGE;
const SCAN_OBJECT: u64 = 25 * MB;
const SCAN_READS_PER_HOT_READ: u64 = 1_000;

/// What the cold scan reads.
#[derive(Clone, Copy, Debug)]
pub(super) enum Scan {
    /// The same `n` objects over and over.
    Loop(u64),
    /// A new object every read.
    Unique,
}

/// Hot-set and scan byte hit rates over every read after the first of each.
pub(super) struct W2 {
    pub(super) hot: f64,
    pub(super) hot_rereads: u64,
    pub(super) scan: f64,
    pub(super) scan_hits: u64,
    pub(super) evictions: u64,
}

/// W2 for `scan_reads` scan reads, with the hot set read twice back to back
/// before the scan when `established`, else first read from scan read 3,000
/// on, while the scan already fills the cache.
pub(super) fn run_w2(window: Window, scan: Scan, established: bool, scan_reads: u64) -> W2 {
    let metrics = CacheMetrics::default();
    let mut fifo = fifo_with(CacheLimits::new(W2_MAX_BYTES, 1_000_000, 64 * MIB), window);
    let hot = |n: u64| key(1, 1_000_000 + n, HOT_RANGE);
    let hot_from = if established {
        for _ in 0..2 {
            for n in 0..HOT_RANGES {
                read(&mut fifo, hot(n), &metrics);
            }
        }
        0
    } else {
        3 * SCAN_READS_PER_HOT_READ
    };
    let mut hot_reads = if established { HOT_RANGES } else { 0 };
    let (mut hot_hits, mut hot_rereads) = (0u64, 0u64);
    let (mut scan_hits, mut scan_rereads) = (0u64, 0u64);
    for r in 0..scan_reads {
        let (n, reread) = match scan {
            Scan::Loop(len) => (r % len, r >= len),
            Scan::Unique => (r, false),
        };
        let hit = read(&mut fifo, key(2, n, SCAN_OBJECT), &metrics);
        if reread {
            scan_rereads += 1;
            scan_hits += u64::from(hit);
        }
        if r < hot_from {
            continue;
        }
        let due = (r - hot_from + 1) * HOT_RANGES / SCAN_READS_PER_HOT_READ
            + if established { HOT_RANGES } else { 0 };
        while hot_reads < due {
            let hit = read(&mut fifo, hot(hot_reads % HOT_RANGES), &metrics);
            if hot_reads >= HOT_RANGES {
                hot_rereads += 1;
                hot_hits += u64::from(hit);
            }
            hot_reads += 1;
        }
    }
    W2 {
        hot: hot_hits as f64 / hot_rereads as f64,
        hot_rereads,
        scan: if scan_rereads == 0 {
            0.0
        } else {
            scan_hits as f64 / scan_rereads as f64
        },
        scan_hits,
        evictions: metrics.snapshot().evictions,
    }
}

/// W3's entry sizes: the ClickBench range size and W2's scan object.
pub(super) const LOOP_ENTRIES: [u64; 2] = [1_300_000, SCAN_OBJECT];

/// W3 at W1's geometry: a loop of `multiple` times the cache, of `entry`-byte
/// entries, read `passes` times, then a second loop of the same size `passes`
/// times. The byte hit rate of every pass, first loop first.
pub(super) fn run_w3(window: Window, multiple: f64, entry: u64, passes: usize) -> Vec<f64> {
    let metrics = CacheMetrics::default();
    let limits = w1_limits();
    let mut fifo = fifo_with(limits, window);
    let n = (limits.max_bytes as f64 * multiple / entry as f64) as u64;
    let mut rates = Vec::new();
    for (tenant, base) in [(3u8, 0u64), (4, 1_000_000)] {
        let keys: Vec<CacheKey> = (base..base + n).map(|i| key(tenant, i, entry)).collect();
        for _ in 0..passes {
            rates.push(pass(&mut fifo, &keys, &metrics));
        }
    }
    rates
}

/// W1: every statement's calls 2 and 3 are served from the cache in full,
/// q29 and q31 to q33 included, where the key window before #3270 served
/// q31, q32 and q33 nothing.
///
/// FLIP: `GHOST_WINDOW_BYTES_TENTHS = 0` (the key window alone, the sizing
/// before #3270) serves q31 0.000 on calls 2 and 3.
#[test]
fn w1_every_statement_is_served_on_its_second_and_third_timed_call() {
    let shipped = run_w1(Window::Shipped);
    assert_eq!(shipped.len(), 43);
    for (q, (_, calls)) in shipped.iter().enumerate() {
        assert!(
            calls[2] >= 1.0 && calls[3] >= 1.0,
            "q{:02} served {:.3} and {:.3} on calls 2 and 3",
            q + 1,
            calls[2],
            calls[3]
        );
    }

    let before = run_w1(Window::LargestEntryKeys);
    for (q, _, _) in LARGE {
        let calls = before[q - 1].1;
        let served = calls[2] + calls[3];
        if q == 29 {
            assert!(served >= 2.0, "q29 before #3270: {calls:?}");
        } else {
            assert!(served <= 0.0, "q{q} before #3270: {calls:?}");
        }
    }
}

/// W2: the hot set's hit rate is no lower than under the key window before
/// #3270 for any scan, and a ghost wider than the cache lowers it. The scan's
/// own rate moves by at most one object: against the 600-object loop with no
/// hot set yet, the byte window keeps more 16 KiB hot ghosts, and promoting
/// one costs one 25 MB re-read of 14,400.
///
/// FLIP: an unbounded ghost (`ghost_max_keys` and `ghost_window_keys` at
/// `usize::MAX` in `S3Fifo::new`) serves the hot set 0.1210 against the
/// 600-object loop, under the 0.1413 the key window serves. A byte window
/// alone (`ghost_window_keys: 0`) serves that loop's scan 0.000 against
/// 0.645.
#[test]
fn w2_the_hot_set_is_served_as_well_as_before_under_every_scan() {
    for scan in [
        Scan::Loop(600),
        Scan::Loop(440),
        Scan::Loop(300),
        Scan::Unique,
    ] {
        for established in [true, false] {
            let reads = if established { 12_000 } else { 15_000 };
            let shipped = run_w2(Window::Shipped, scan, established, reads);
            let before = run_w2(Window::LargestEntryKeys, scan, established, reads);
            assert!(
                shipped.hot >= before.hot && shipped.scan_hits + 1 >= before.scan_hits,
                "{scan:?} established={established}: hot {:.6} scan {:.6}, before #3270 hot {:.6} scan {:.6}",
                shipped.hot,
                shipped.scan,
                before.hot,
                before.scan
            );
        }
    }

    let shipped = run_w2(Window::Shipped, Scan::Loop(600), true, 12_000);
    assert_eq!(shipped.hot_rereads, 24_168);
    assert!(
        (shipped.hot - 0.1413).abs() < 1e-4,
        "hot {:.4}",
        shipped.hot
    );
    let over_wide = run_w2(Window::Unbounded, Scan::Loop(600), true, 12_000);
    assert!(
        over_wide.hot < shipped.hot,
        "an unbounded ghost served the hot set {:.4}, the shipped one {:.4}",
        over_wide.hot,
        shipped.hot
    );
}

/// The repeated scans of [`w2_a_repeated_scan_of_up_to_0_35_of_the_cache_evicts_nothing`]:
/// 92, 115 and 161 objects of 25 MB, 0.20, 0.25 and 0.35 of `W2_MAX_BYTES`.
const W2_REPEATED_LOOPS: [u64; 3] = [92, 115, 161];

/// W2 against a repeated scan of 0.20, 0.25 or 0.35 of the cache: the hot
/// set and the scan's re-reads are served 1.000 under the shipped sizing and
/// under the key window before #3270, with the hot set read before the scan
/// or first read during it.
///
/// These cases do not tell ghost sizings apart, and no ghost width fails
/// them: the loop and the hot set together stay under `max_bytes`, so
/// nothing is evicted and no key reaches the ghost. An unbounded ghost and a
/// zero-byte one serve the same 1.000. The test pins that, so a case that
/// starts evicting has to be read again rather than passing as evidence.
///
/// FLIP: evicting above a quarter of `max_bytes` (`self.limits.max_bytes / 4`
/// in `evict_to_bounds`'s loop condition) evicts against the 115-object loop.
#[test]
fn w2_a_repeated_scan_of_up_to_0_35_of_the_cache_evicts_nothing() {
    for n in W2_REPEATED_LOOPS {
        for established in [true, false] {
            let reads = if established { 12_000 } else { 15_000 };
            let shipped = run_w2(Window::Shipped, Scan::Loop(n), established, reads);
            let before = run_w2(Window::LargestEntryKeys, Scan::Loop(n), established, reads);
            for (name, r) in [("shipped", &shipped), ("before #3270", &before)] {
                assert!(
                    r.evictions == 0 && r.hot >= 1.0 && r.scan >= 1.0,
                    "{n}-object loop, established={established}, {name}: {} evictions, hot {:.4}, scan {:.4}",
                    r.evictions,
                    r.hot,
                    r.scan
                );
            }
            assert!(shipped.hot >= before.hot);
        }
    }
}

/// The ghost queue holds at most twice the key cap plus 65 slots, live and
/// stale, and the map at most the key cap, under a churn that forgets ghosts
/// while new ones keep the count at the cap. The stale slots do pile up: the
/// queue passes twice the cap.
///
/// FLIP: compacting at `4 * self.ghosts.len() + 64` in `compact_ghost_queue`
/// lets the queue pass twice the cap plus 65.
#[test]
fn ghost_queue_slots_stay_within_twice_the_key_cap() {
    let metrics = CacheMetrics::default();
    let mut fifo = fifo_with(
        CacheLimits::new(152_000_000, 1_000_000, 64 * MIB),
        Window::Shipped,
    );
    let cap = fifo.ghost_max_keys;
    assert_eq!(cap, 1_000);
    let (mut peak_slots, mut peak_keys) = (0, 0);
    // Tenant 5's keys are never read again and keep the ghost at its cap;
    // tenants 6 to 8 are read again shortly after probation evicts them, so
    // each leaves a stale slot behind.
    for i in 0..30_000u64 {
        read(&mut fifo, key(5, i, HOT_RANGE), &metrics);
        for tenant in 6..=8 {
            read(&mut fifo, key(tenant, i, HOT_RANGE), &metrics);
            if let Some(back) = i.checked_sub(300) {
                read(&mut fifo, key(tenant, back, HOT_RANGE), &metrics);
            }
        }
        peak_slots = peak_slots.max(fifo.ghost.len());
        peak_keys = peak_keys.max(fifo.ghosts.len());
    }
    assert_eq!(peak_keys, cap);
    assert!(
        peak_slots > 2 * cap && peak_slots <= 2 * cap + 65,
        "peak queue slots {peak_slots} against a cap of {cap} keys"
    );
}

/// W3: every pass of every loop, at both entry sizes and from half the cache
/// to three times it, is served at least as well as under the key window
/// before #3270, and a loop just over the cache of ClickBench-sized entries
/// is served from its third pass where it was served nothing.
///
/// FLIP: a byte window alone of a quarter of `max_bytes`
/// (`ghost_window_keys: 0` and `ghost_window_bytes = max_bytes / 4` in
/// `S3Fifo::new`) serves the 1.3-times loop of 25 MB entries nothing, where
/// the key window serves 0.695 of its third pass. `GHOST_WINDOW_BYTES_TENTHS
/// = 0` fails the 1.05-times loop's floor.
#[test]
fn w3_no_loop_is_served_less_than_before() {
    for entry in LOOP_ENTRIES {
        for multiple in [0.5, 0.95, 1.05, 1.3, 1.5, 3.0] {
            let shipped = run_w3(Window::Shipped, multiple, entry, 5);
            let before = run_w3(Window::LargestEntryKeys, multiple, entry, 5);
            assert!(
                shipped.iter().zip(&before).all(|(now, then)| now >= then),
                "{entry} B entries, loop {multiple} C: {shipped:?}, before #3270 {before:?}"
            );
        }
    }
    let shipped = run_w3(Window::Shipped, 1.05, LOOP_ENTRIES[0], 5);
    assert!(shipped[2] > 0.85, "1.05 C loop, third pass: {shipped:?}");
}

/// The candidates the report compares.
fn candidates(max_bytes: u64) -> Vec<(&'static str, Window)> {
    let small = max_bytes / 10;
    vec![
        (
            "largest-entry keys (before #3270)",
            Window::LargestEntryKeys,
        ),
        ("bytes = small quota (0.1 C)", Window::Bytes(small)),
        ("bytes = 0.2 C", Window::Bytes(max_bytes / 5)),
        ("bytes = 0.25 C", Window::Bytes(max_bytes / 4)),
        ("bytes = 0.3 C", Window::Bytes(max_bytes * 3 / 10)),
        ("either keys or 0.25 C", Window::Either(max_bytes / 4)),
        ("shipped (either keys or 0.3 C)", Window::Shipped),
        ("bytes = 0.5 C", Window::Bytes(max_bytes / 2)),
        (
            "bytes = main quota (0.9 C)",
            Window::Bytes(max_bytes - small),
        ),
        ("bytes = C", Window::Bytes(max_bytes)),
        ("unbounded", Window::Unbounded),
    ]
}

/// Prints W1-W3 for every candidate. Run with
/// `cargo test -p ravel-cache --release -- --ignored --nocapture ghost_window_report`.
#[test]
#[ignore = "report harness: prints the issue #3270 simulation tables"]
fn ghost_window_report() {
    for (name, window) in candidates(W1_MAX_BYTES) {
        println!("== W1 {name}");
        let results = run_w1(window);
        let mut low = Vec::new();
        for (q, (bytes, calls)) in results.iter().enumerate() {
            println!(
                "q{:02} {:>5} MB  warm {:.3}  call1 {:.3}  call2 {:.3}  call3 {:.3}",
                q + 1,
                bytes / MB,
                calls[0],
                calls[1],
                calls[2],
                calls[3]
            );
            if calls[2].min(calls[3]) < 0.95 {
                low.push(q + 1);
            }
        }
        println!("W1 {name}: calls 2-3 under 0.95 on {low:?}");
    }
    for (name, window) in candidates(W2_MAX_BYTES) {
        for scan in [
            Scan::Loop(600),
            Scan::Loop(440),
            Scan::Loop(300),
            Scan::Loop(W2_REPEATED_LOOPS[2]),
            Scan::Loop(W2_REPEATED_LOOPS[1]),
            Scan::Loop(W2_REPEATED_LOOPS[0]),
            Scan::Unique,
        ] {
            for established in [true, false] {
                let reads = if established { 12_000 } else { 15_000 };
                let r = run_w2(window, scan, established, reads);
                println!(
                    "W2 {name}: {scan:?} established_first={established} hot {:.4} over {} rereads, scan {:.3}, {} evictions",
                    r.hot, r.hot_rereads, r.scan, r.evictions
                );
            }
        }
    }
    for (name, window) in candidates(W1_MAX_BYTES) {
        for entry in LOOP_ENTRIES {
            for multiple in [0.5, 0.95, 1.05, 1.3, 1.5, 3.0] {
                let rates: Vec<String> = run_w3(window, multiple, entry, 5)
                    .iter()
                    .map(|r| format!("{r:.3}"))
                    .collect();
                println!(
                    "W3 {name}: {} MB entries, loop {multiple} C passes {}",
                    entry as f64 / MB as f64,
                    rates.join(" ")
                );
            }
        }
    }
}
