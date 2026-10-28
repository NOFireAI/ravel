//! ADR-3326 Stage 0 item 1: a simulation of the decoded column block cache's
//! admission policy (docs/adrs/3326-decoded-column-block-cache.md, decisions
//! 3 and 4, Measurement). It models no cache this crate ships.
//!
//! Entries are keyed by (block, column) with a byte size, under one byte
//! budget, with two policies:
//!
//! - P1 "chosen": an Interactive access admits on first decode; a BulkScan
//!   access admits only on a second touch, through a ghost key list whose
//!   window spans the whole budget in bytes. A ghost hit admits the entry on
//!   that access, and the access is still a miss.
//! - P2 "first-touch": every access admits on first decode.
//!
//! Both evict with CLOCK over admitted entries, weighted by bytes: the hand
//! evicts until the new entry fits, an entry is inserted with its reference
//! bit clear and a hit sets it, and an entry larger than the budget is never
//! admitted.
//!
//! Trace A is the ClickBench protocol: the cache is emptied before each
//! statement, which then runs three tries over its (block, column) keys.
//! Trace B is production-shaped: a 2 GB interactive working set read in
//! round-robin passes, with a 12 GB one-pass bulk scan of fresh keys after
//! every 500 working-set accesses, at an 8 GB budget.
//!
//! Trace C is the admission-headroom amendment's q33 shape: 16 partitions
//! interleave column admissions with aggregate growth against one memory
//! budget, under best-effort admission (B) and admission with a headroom
//! margin (M).
//!
//! Trace D keeps Trace C's shape and margin admission, but the step racing a
//! refused aggregate retry is a checked scan reservation by every other
//! partition, under an evictor that frees the shortfall (M-exact) or the
//! shortfall plus the margin (M-slack).

use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;

const GB: u64 = 1_000_000_000;
const MB: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Intent {
    Interactive,
    BulkScan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Policy {
    Chosen,
    FirstTouch,
}

impl Policy {
    fn name(self) -> &'static str {
        match self {
            Policy::Chosen => "P1",
            Policy::FirstTouch => "P2",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    Compact,
    Today,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Layout::Compact => "compact",
            Layout::Today => "today",
        }
    }
}

type Key = (u64, &'static str);

#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    hits: u64,
    misses: u64,
    hit_bytes: u64,
    miss_bytes: u64,
    admissions: u64,
    evictions: u64,
    ghost_hits: u64,
}

impl Counts {
    fn accesses(&self) -> u64 {
        self.hits + self.misses
    }

    /// None when nothing was accessed.
    fn byte_share(&self) -> Option<f64> {
        let total = self.hit_bytes + self.miss_bytes;
        (total > 0).then(|| self.hit_bytes as f64 / total as f64)
    }

    fn count_share(&self) -> Option<f64> {
        let total = self.accesses();
        (total > 0).then(|| self.hits as f64 / total as f64)
    }
}

struct Entry {
    bytes: u64,
    referenced: bool,
}

struct Sim {
    policy: Policy,
    budget: u64,
    used: u64,
    entries: HashMap<Key, Entry>,
    /// The CLOCK ring; the hand is the front.
    ring: VecDeque<Key>,
    /// Ghost keys in insertion order, each with its bytes and the sequence
    /// number it was recorded under; a slot whose sequence no longer matches
    /// `ghost_live` was consumed by a ghost hit.
    ghost: VecDeque<(Key, u64, u64)>,
    ghost_live: HashMap<Key, u64>,
    ghost_bytes: u64,
    ghost_seq: u64,
    counts: Counts,
}

impl Sim {
    fn new(policy: Policy, budget: u64) -> Self {
        Self {
            policy,
            budget,
            used: 0,
            entries: HashMap::new(),
            ring: VecDeque::new(),
            ghost: VecDeque::new(),
            ghost_live: HashMap::new(),
            ghost_bytes: 0,
            ghost_seq: 0,
            counts: Counts::default(),
        }
    }

    fn contains(&self, key: &Key) -> bool {
        self.entries.contains_key(key)
    }

    /// One decode request. True on a hit: the key was admitted at access time.
    fn access(&mut self, key: Key, bytes: u64, intent: Intent) -> bool {
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.referenced = true;
            self.counts.hits += 1;
            self.counts.hit_bytes += bytes;
            return true;
        }
        self.counts.misses += 1;
        self.counts.miss_bytes += bytes;
        let admit = match (self.policy, intent) {
            (Policy::FirstTouch, _) | (Policy::Chosen, Intent::Interactive) => true,
            (Policy::Chosen, Intent::BulkScan) => {
                if self.ghost_live.remove(&key).is_some() {
                    self.ghost_bytes -= bytes;
                    self.counts.ghost_hits += 1;
                    true
                } else {
                    self.record_ghost(key, bytes);
                    false
                }
            }
        };
        if admit {
            self.admit(key, bytes);
        }
        false
    }

    fn record_ghost(&mut self, key: Key, bytes: u64) {
        if bytes > self.budget {
            return;
        }
        while self.ghost_bytes + bytes > self.budget {
            let Some((old, old_bytes, seq)) = self.ghost.pop_front() else {
                break;
            };
            if self.ghost_live.get(&old) == Some(&seq) {
                self.ghost_live.remove(&old);
                self.ghost_bytes -= old_bytes;
            }
        }
        self.ghost_seq += 1;
        self.ghost.push_back((key, bytes, self.ghost_seq));
        self.ghost_live.insert(key, self.ghost_seq);
        self.ghost_bytes += bytes;
    }

    fn admit(&mut self, key: Key, bytes: u64) {
        if bytes > self.budget {
            return;
        }
        while self.used + bytes > self.budget {
            let victim = self
                .ring
                .pop_front()
                .expect("ring holds every admitted entry");
            let entry = self.entries.get_mut(&victim).expect("ring key is admitted");
            if entry.referenced {
                entry.referenced = false;
                self.ring.push_back(victim);
            } else {
                self.used -= entry.bytes;
                self.entries.remove(&victim);
                self.counts.evictions += 1;
            }
        }
        self.entries.insert(
            key,
            Entry {
                bytes,
                referenced: false,
            },
        );
        self.ring.push_back(key);
        self.used += bytes;
        self.counts.admissions += 1;
    }
}

// ---------------------------------------------------------------------------
// Trace A: the ClickBench protocol.

/// stmt, intent, blocks_scanned, then `name:bytes_per_block_compact:bytes_per_block_today`
/// per column, or NOT_CACHED.
const TRACE_A: &str = "
q01 bulkScan 0 NOT_CACHED
q02 interactive 6747 ts:60444:60444 stream_ref:30222:30222 AdvEngineID:60444:60444
q03 bulkScan 13235 ts:60444:60444 stream_ref:30222:30222 AdvEngineID:60444:60444 ResolutionWidth:60444:60444
q04 bulkScan 13235 ts:60444:60444 stream_ref:30222:30222 UserID:60444:60444
q05 bulkScan 13235 ts:60444:60444 stream_ref:30222:30222 UserID:60444:60444
q06 bulkScan 13235 ts:60444:60444 stream_ref:30222:30222 SearchPhrase:147335:419335
q07 bulkScan 0 NOT_CACHED
q08 interactive 6747 ts:60444:60444 stream_ref:30222:30222 AdvEngineID:60444:60444
q09 interactive 13235 ts:60444:60444 stream_ref:30222:30222 RegionID:60444:60444 UserID:60444:60444
q10 interactive 13235 ts:60444:60444 stream_ref:30222:30222 RegionID:60444:60444 AdvEngineID:60444:60444 ResolutionWidth:60444:60444 UserID:60444:60444
q11 interactive 13235 ts:60444:60444 stream_ref:30222:30222 MobilePhoneModel:33999:305999 UserID:60444:60444
q12 interactive 13235 ts:60444:60444 stream_ref:30222:30222 MobilePhone:60444:60444 MobilePhoneModel:33999:305999 UserID:60444:60444
q13 interactive 13235 ts:60444:60444 stream_ref:30222:30222 SearchPhrase:147335:419335
q14 interactive 13235 ts:60444:60444 stream_ref:30222:30222 SearchPhrase:147335:419335 UserID:60444:60444
q15 interactive 13235 ts:60444:60444 stream_ref:30222:30222 SearchEngineID:60444:60444 SearchPhrase:147335:419335
q16 interactive 13235 ts:60444:60444 stream_ref:30222:30222 UserID:60444:60444
q17 interactive 13235 ts:60444:60444 stream_ref:30222:30222 UserID:60444:60444 SearchPhrase:147335:419335
q18 interactive 13235 ts:60444:60444 stream_ref:30222:30222 UserID:60444:60444 SearchPhrase:147335:419335
q19 interactive 13235 ts:60444:60444 stream_ref:30222:30222 UserID:60444:60444 EventTime:60444:60444 SearchPhrase:147335:419335
q20 interactive 6418 ts:60444:60444 stream_ref:30222:30222 UserID:60444:60444
q21 interactive 13235 NOT_CACHED
q22 interactive 13235 NOT_CACHED
q23 interactive 13235 NOT_CACHED
q24 interactive 10997 NOT_CACHED
q25 interactive 243 ts:60444:60444 stream_ref:30222:30222 SearchPhrase:147335:419335 EventTime:60444:60444
q26 interactive 13235 ts:60444:60444 stream_ref:30222:30222 SearchPhrase:147335:419335
q27 interactive 244 ts:60444:60444 stream_ref:30222:30222 SearchPhrase:147335:419335 EventTime:60444:60444
q28 interactive 13235 ts:60444:60444 stream_ref:30222:30222 CounterID:60444:60444 URL:429919:701919
q29 interactive 13235 ts:60444:60444 stream_ref:30222:30222 Referer:319606:591605
q30 bulkScan 13235 ts:60444:60444 stream_ref:30222:30222 ResolutionWidth:60444:60444
q31 interactive 13235 ts:60444:60444 stream_ref:30222:30222 SearchEngineID:60444:60444 ClientIP:60444:60444 IsRefresh:60444:60444 ResolutionWidth:60444:60444 SearchPhrase:147335:419335
q32 interactive 13235 ts:60444:60444 stream_ref:30222:30222 WatchID:60444:60444 ClientIP:60444:60444 IsRefresh:60444:60444 ResolutionWidth:60444:60444 SearchPhrase:147335:419335
q33 interactive 13235 ts:60444:60444 stream_ref:30222:30222 WatchID:60444:60444 ClientIP:60444:60444 IsRefresh:60444:60444 ResolutionWidth:60444:60444
q34 interactive 13235 ts:60444:60444 stream_ref:30222:30222 URL:429919:701919
q35 interactive 13235 ts:60444:60444 stream_ref:30222:30222 URL:429919:701919
q36 interactive 13235 ts:60444:60444 stream_ref:30222:30222 ClientIP:60444:60444
q37 interactive 138 ts:60444:60444 stream_ref:30222:30222 URL:429919:701919 CounterID:60444:60444 EventDate:60444:60444 DontCountHits:60444:60444 IsRefresh:60444:60444
q38 interactive 138 ts:60444:60444 stream_ref:30222:30222 Title:305250:577249 CounterID:60444:60444 EventDate:60444:60444 DontCountHits:60444:60444 IsRefresh:60444:60444
q39 interactive 122 ts:60444:60444 stream_ref:30222:30222 URL:429919:701919 CounterID:60444:60444 EventDate:60444:60444 IsRefresh:60444:60444 IsLink:60444:60444 IsDownload:60444:60444
q40 interactive 138 ts:60444:60444 stream_ref:30222:30222 TraficSourceID:60444:60444 SearchEngineID:60444:60444 AdvEngineID:60444:60444 Referer:319606:591605 URL:429919:701919 CounterID:60444:60444 EventDate:60444:60444 IsRefresh:60444:60444 URLHash:60444:60444
q41 interactive 138 ts:60444:60444 stream_ref:30222:30222 URLHash:60444:60444 EventDate:60444:60444 CounterID:60444:60444 IsRefresh:60444:60444 TraficSourceID:60444:60444 RefererHash:60444:60444
q42 interactive 138 ts:60444:60444 stream_ref:30222:30222 WindowClientWidth:60444:60444 WindowClientHeight:60444:60444 CounterID:60444:60444 EventDate:60444:60444 IsRefresh:60444:60444 DontCountHits:60444:60444 URLHash:60444:60444
q43 interactive 138 ts:60444:60444 stream_ref:30222:30222 EventTime:60444:60444 CounterID:60444:60444 EventDate:60444:60444 IsRefresh:60444:60444 DontCountHits:60444:60444
";

const BUDGETS: [u64; 3] = [4 * GB, 8 * GB, 12 * GB];
const LAYOUTS: [Layout; 2] = [Layout::Compact, Layout::Today];
const POLICIES: [Policy; 2] = [Policy::Chosen, Policy::FirstTouch];

struct Column {
    name: &'static str,
    compact: u64,
    today: u64,
}

impl Column {
    fn bytes(&self, layout: Layout) -> u64 {
        match layout {
            Layout::Compact => self.compact,
            Layout::Today => self.today,
        }
    }
}

struct Statement {
    name: &'static str,
    intent: Intent,
    blocks: u64,
    /// Empty for NOT_CACHED.
    columns: Vec<Column>,
}

impl Statement {
    fn set_bytes(&self, layout: Layout) -> u64 {
        self.blocks * self.columns.iter().map(|c| c.bytes(layout)).sum::<u64>()
    }

    fn cached(&self) -> bool {
        !self.columns.is_empty()
    }
}

fn statements() -> Vec<Statement> {
    TRACE_A
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut fields = line.split_whitespace();
            let name = fields.next().expect("stmt");
            let intent = match fields.next().expect("intent") {
                "interactive" => Intent::Interactive,
                "bulkScan" => Intent::BulkScan,
                other => panic!("unknown intent {other}"),
            };
            let blocks: u64 = fields.next().expect("blocks").parse().expect("blocks");
            let columns = fields
                .filter(|field| *field != "NOT_CACHED")
                .map(|field| {
                    let mut parts = field.split(':');
                    let name = parts.next().expect("column name");
                    let compact = parts.next().expect("compact").parse().expect("compact");
                    let today = parts.next().expect("today").parse().expect("today");
                    Column {
                        name,
                        compact,
                        today,
                    }
                })
                .collect();
            Statement {
                name,
                intent,
                blocks,
                columns,
            }
        })
        .collect()
}

/// One statement under one policy, budget and layout: the counts of each try.
struct StatementRun {
    tries: [Counts; 3],
}

fn run_statement(stmt: &Statement, policy: Policy, budget: u64, layout: Layout) -> StatementRun {
    // The protocol restarts the server before each statement.
    let mut sim = Sim::new(policy, budget);
    let mut tries = [Counts::default(); 3];
    for try_counts in &mut tries {
        let before = sim.counts;
        for block in 0..stmt.blocks {
            for column in &stmt.columns {
                sim.access((block, column.name), column.bytes(layout), stmt.intent);
            }
        }
        *try_counts = diff(sim.counts, before);
    }
    StatementRun { tries }
}

fn diff(after: Counts, before: Counts) -> Counts {
    Counts {
        hits: after.hits - before.hits,
        misses: after.misses - before.misses,
        hit_bytes: after.hit_bytes - before.hit_bytes,
        miss_bytes: after.miss_bytes - before.miss_bytes,
        admissions: after.admissions - before.admissions,
        evictions: after.evictions - before.evictions,
        ghost_hits: after.ghost_hits - before.ghost_hits,
    }
}

struct TraceARow {
    layout: Layout,
    budget: u64,
    stmt: usize,
    fits: bool,
    runs: [StatementRun; 2],
}

struct TraceA {
    statements: Vec<Statement>,
    rows: Vec<TraceARow>,
}

fn trace_a() -> &'static TraceA {
    static TRACE: OnceLock<TraceA> = OnceLock::new();
    TRACE.get_or_init(|| {
        let statements = statements();
        let mut rows = Vec::new();
        for layout in LAYOUTS {
            for budget in BUDGETS {
                for (i, stmt) in statements.iter().enumerate() {
                    rows.push(TraceARow {
                        layout,
                        budget,
                        stmt: i,
                        fits: stmt.set_bytes(layout) <= budget,
                        runs: POLICIES.map(|p| run_statement(stmt, p, budget, layout)),
                    });
                }
            }
        }
        TraceA { statements, rows }
    })
}

fn policy_index(policy: Policy) -> usize {
    POLICIES.iter().position(|p| *p == policy).expect("policy")
}

/// Try-3 byte hit share of every statement whose set fits and that caches
/// anything, for one policy, budget and layout.
fn fitting_try3(
    trace: &TraceA,
    policy: Policy,
    budget: u64,
    layout: Layout,
) -> Vec<(&'static str, f64)> {
    let p = policy_index(policy);
    trace
        .rows
        .iter()
        .filter(|r| r.layout == layout && r.budget == budget && r.fits)
        .filter_map(|r| {
            let share = r.runs[p].tries[2].byte_share()?;
            Some((trace.statements[r.stmt].name, share))
        })
        .collect()
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

fn share(value: Option<f64>) -> String {
    value.map_or_else(|| "  -  ".to_string(), |v| format!("{v:.3}"))
}

fn print_trace_a(trace: &TraceA) {
    println!("== Trace A: ClickBench protocol, cache emptied per statement, three tries");
    println!(
        "   cell = try byte hit share / count hit share; adm/ev/gh = admissions/evictions/ghost hits over the three tries"
    );
    for layout in LAYOUTS {
        for budget in BUDGETS {
            println!();
            println!("-- layout {}, budget {} GB", layout.name(), budget / GB);
            println!(
                "stmt intent blocks set_GB fits | P1 try1 try2 try3 adm/ev/gh | P2 try1 try2 try3 adm/ev/gh"
            );
            for row in trace
                .rows
                .iter()
                .filter(|r| r.layout == layout && r.budget == budget)
            {
                let stmt = &trace.statements[row.stmt];
                let mut line = format!(
                    "{} {} {:>5} {:>6.3} {} ",
                    stmt.name,
                    match stmt.intent {
                        Intent::Interactive => "int ",
                        Intent::BulkScan => "bulk",
                    },
                    stmt.blocks,
                    stmt.set_bytes(layout) as f64 / GB as f64,
                    if !stmt.cached() {
                        "n/c"
                    } else if row.fits {
                        "yes"
                    } else {
                        "no "
                    },
                );
                for (policy, run) in POLICIES.iter().zip(&row.runs) {
                    line.push_str(&format!("| {} ", policy.name()));
                    for t in &run.tries {
                        line.push_str(&format!(
                            "{}/{} ",
                            share(t.byte_share()),
                            share(t.count_share())
                        ));
                    }
                    let adm: u64 = run.tries.iter().map(|t| t.admissions).sum();
                    let ev: u64 = run.tries.iter().map(|t| t.evictions).sum();
                    let gh: u64 = run.tries.iter().map(|t| t.ghost_hits).sum();
                    line.push_str(&format!("{adm}/{ev}/{gh} "));
                }
                println!("{}", line.trim_end());
            }
        }
    }
    println!();
    println!(
        "== Trace A summary: try-3 byte hit share over statements whose set fits (NOT_CACHED excluded)"
    );
    println!("policy layout budget_GB n_fitting min median");
    for policy in POLICIES {
        for layout in LAYOUTS {
            for budget in BUDGETS {
                let shares: Vec<f64> = fitting_try3(trace, policy, budget, layout)
                    .into_iter()
                    .map(|(_, s)| s)
                    .collect();
                let min = shares.iter().copied().fold(f64::INFINITY, f64::min);
                println!(
                    "{} {:<7} {:>2} {:>2} {:.3} {:.3}",
                    policy.name(),
                    layout.name(),
                    budget / GB,
                    shares.len(),
                    min,
                    median(&shares)
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Trace B: a working set through scans larger than the budget.

const W_BLOCKS: u64 = 2_000;
const W_PASSES: u64 = 20;
const SCAN_BLOCKS: u64 = 12_000;
const SCAN_EVERY: u64 = 500;
const B_BUDGET: u64 = 8 * GB;
const SCANS: usize = (W_BLOCKS * W_PASSES / SCAN_EVERY) as usize;

struct ScanRow {
    /// W's admitted bytes over W's 2 GB, just before and just after the scan.
    before: f64,
    after: f64,
}

impl ScanRow {
    /// The share of what W had admitted before the scan that survives it.
    fn retained(&self) -> f64 {
        if self.before == 0.0 {
            1.0
        } else {
            self.after / self.before
        }
    }
}

struct TraceB {
    scans: Vec<ScanRow>,
    /// W's hit share per pass.
    passes: Vec<f64>,
    /// W's hit share over passes 2..=20.
    steady: f64,
    counts: Counts,
}

fn w_admitted_share(sim: &Sim) -> f64 {
    let admitted = (0..W_BLOCKS).filter(|b| sim.contains(&(*b, "w"))).count() as u64;
    admitted as f64 / W_BLOCKS as f64
}

fn run_trace_b(policy: Policy) -> TraceB {
    let mut sim = Sim::new(policy, B_BUDGET);
    let mut scans = Vec::new();
    let mut passes = Vec::new();
    let mut accesses = 0u64;
    let (mut steady_hits, mut steady_total) = (0u64, 0u64);
    for pass in 0..W_PASSES {
        let mut hits = 0u64;
        for block in 0..W_BLOCKS {
            if sim.access((block, "w"), MB, Intent::Interactive) {
                hits += 1;
            }
            accesses += 1;
            if accesses.is_multiple_of(SCAN_EVERY) {
                let before = w_admitted_share(&sim);
                // Every scan reads blocks no earlier scan read.
                let first = scans.len() as u64 * SCAN_BLOCKS;
                for scan_block in first..first + SCAN_BLOCKS {
                    sim.access((scan_block, "bulk"), MB, Intent::BulkScan);
                }
                let after = w_admitted_share(&sim);
                scans.push(ScanRow { before, after });
            }
        }
        passes.push(hits as f64 / W_BLOCKS as f64);
        if pass > 0 {
            steady_hits += hits;
            steady_total += W_BLOCKS;
        }
    }
    TraceB {
        scans,
        passes,
        steady: steady_hits as f64 / steady_total as f64,
        counts: sim.counts,
    }
}

fn trace_b() -> &'static [TraceB; 2] {
    static TRACE: OnceLock<[TraceB; 2]> = OnceLock::new();
    TRACE.get_or_init(|| POLICIES.map(run_trace_b))
}

fn print_trace_b(traces: &[TraceB; 2]) {
    println!(
        "== Trace B: W = 2,000 x 1 MB interactive, 12,000 x 1 MB fresh BulkScan after every 500 W accesses, 20 W passes, budget 8 GB"
    );
    println!(
        "   W_before/W_after = W's admitted bytes over its 2 GB around the scan; retained = after / before"
    );
    println!("scan pass | P1 W_before W_after retained | P2 W_before W_after retained");
    for i in 0..SCANS {
        let mut line = format!("{:>4} {:>4} ", i + 1, i as u64 * SCAN_EVERY / W_BLOCKS + 1);
        for (policy, trace) in POLICIES.iter().zip(traces) {
            let row = &trace.scans[i];
            line.push_str(&format!(
                "| {} {:.3} {:.3} {:.3} ",
                policy.name(),
                row.before,
                row.after,
                row.retained()
            ));
        }
        println!("{}", line.trim_end());
    }
    println!();
    println!("pass | P1 W_hit_share | P2 W_hit_share");
    for pass in 0..W_PASSES as usize {
        println!(
            "{:>4} | {:.3} | {:.3}",
            pass + 1,
            traces[0].passes[pass],
            traces[1].passes[pass]
        );
    }
    println!();
    for (policy, trace) in POLICIES.iter().zip(traces) {
        let min_retained = trace
            .scans
            .iter()
            .map(ScanRow::retained)
            .fold(f64::INFINITY, f64::min);
        let c = trace.counts;
        println!(
            "{}: W steady-state hit share (passes 2-20) {:.3}; min retained {:.3}; hits {} misses {} admissions {} evictions {} ghost_hits {}",
            policy.name(),
            trace.steady,
            min_retained,
            c.hits,
            c.misses,
            c.admissions,
            c.evictions,
            c.ghost_hits
        );
    }
}

// ---------------------------------------------------------------------------
// Trace C: q33 against one memory budget, with and without a headroom margin.

const GIB: u64 = 1 << 30;
const C_LIMIT: u64 = 20 * GB;
const C_OTHER: u64 = 3 * GB;
const C_ENTRY: u64 = 60_444;
const C_PARTITIONS: usize = 16;
const C_BLOCKS: u64 = 13_235;
const C_COLUMNS: usize = 4;
const C_GROW_EVERY: u64 = 64;
const C_GROW: u64 = 117_440_512;
const C_AGG_TARGET: u64 = 10 * GB;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Admission {
    /// Reserve whenever the entry fits the budget limit.
    BestEffort,
    /// Reserve only when the unreserved bytes after it stay at least the margin.
    Margin,
}

impl Admission {
    fn name(self) -> &'static str {
        match self {
            Admission::BestEffort => "B",
            Admission::Margin => "M",
        }
    }
}

const C_RUNS: [(Admission, u64); 4] = [
    (Admission::BestEffort, 8 * GB),
    (Admission::Margin, 8 * GB),
    (Admission::Margin, 4 * GB),
    (Admission::Margin, 12 * GB),
];

#[derive(Clone, Copy, Debug, Default)]
struct TraceCStats {
    /// Aggregate reservations refused after the evictor and one retry. The
    /// run continues past a refusal (the grow is dropped) so later ones count.
    refusals: u64,
    first_refusal_block: Option<u64>,
    peak_aggregate: u64,
    grows: u64,
    admissions: u64,
    skipped_margin: u64,
    skipped_limit: u64,
    cap_evictions: u64,
    evictor_passes: u64,
    evictor_freed: u64,
    min_unreserved: u64,
    resident_end: u64,
}

struct TraceC {
    admission: Admission,
    cap: u64,
    margin: u64,
    reserved: u64,
    /// Key to the number of partitions holding it; a held entry is never evicted.
    entries: HashMap<u64, u32>,
    /// The CLOCK ring; the hand is the front. Every key is fresh, so no
    /// reference bit is ever set and the hand only skips held entries.
    ring: VecDeque<u64>,
    resident: u64,
    next_key: u64,
    aggregate: u64,
    stats: TraceCStats,
}

impl TraceC {
    fn new(admission: Admission, cap: u64) -> Self {
        let n = cap / C_ENTRY;
        let resident = n * C_ENTRY;
        let reserved = C_OTHER + resident;
        Self {
            admission,
            cap,
            margin: GIB.max(C_LIMIT / 20),
            reserved,
            entries: (0..n).map(|k| (k, 0)).collect(),
            ring: (0..n).collect(),
            resident,
            next_key: n,
            aggregate: 0,
            stats: TraceCStats {
                min_unreserved: C_LIMIT - reserved,
                ..TraceCStats::default()
            },
        }
    }

    fn unreserved(&self) -> u64 {
        C_LIMIT - self.reserved
    }

    fn note_unreserved(&mut self) {
        self.stats.min_unreserved = self.stats.min_unreserved.min(self.unreserved());
    }

    /// Evicts the first unheld entry under the hand and releases its
    /// reservation. None when every entry is held.
    fn evict_one(&mut self) -> Option<u64> {
        for _ in 0..self.ring.len() {
            let key = self.ring.pop_front()?;
            if self.entries.get(&key).copied().unwrap_or(0) > 0 {
                self.ring.push_back(key);
                continue;
            }
            self.entries.remove(&key);
            self.resident -= C_ENTRY;
            self.reserved -= C_ENTRY;
            return Some(C_ENTRY);
        }
        None
    }

    /// One column offered for admission through the best-effort path: reserve
    /// first, then evict down to the cap. Some(key) when admitted.
    fn offer(&mut self, hold: bool) -> Option<u64> {
        let after = self.reserved + C_ENTRY;
        // Under M the margin covers the limit, so every M skip counts as a margin skip.
        let skip = match self.admission {
            Admission::BestEffort => (after > C_LIMIT).then_some(&mut self.stats.skipped_limit),
            Admission::Margin => (after > C_LIMIT || C_LIMIT - after < self.margin)
                .then_some(&mut self.stats.skipped_margin),
        };
        if let Some(count) = skip {
            *count += 1;
            return None;
        }
        self.reserved = after;
        self.note_unreserved();
        let key = self.next_key;
        self.next_key += 1;
        self.entries.insert(key, u32::from(hold));
        self.ring.push_back(key);
        self.resident += C_ENTRY;
        self.stats.admissions += 1;
        while self.resident > self.cap {
            if self.evict_one().is_none() {
                break;
            }
            self.stats.cap_evictions += 1;
        }
        Some(key)
    }

    fn try_grow(&mut self) -> bool {
        if self.reserved + C_GROW > C_LIMIT {
            return false;
        }
        self.reserved += C_GROW;
        self.aggregate += C_GROW;
        self.note_unreserved();
        self.stats.grows += 1;
        self.stats.peak_aggregate = self.stats.peak_aggregate.max(self.aggregate);
        true
    }

    /// A checked aggregate reservation at `block`: on refusal the evictor
    /// frees unheld entries for the shortfall, every other partition makes its
    /// next admission offer, and the caller retries once.
    fn grow(&mut self, block: u64) {
        if self.try_grow() {
            return;
        }
        let shortfall = self.reserved + C_GROW - C_LIMIT;
        self.stats.evictor_passes += 1;
        let mut freed = 0;
        while freed < shortfall {
            match self.evict_one() {
                Some(bytes) => freed += bytes,
                None => break,
            }
        }
        self.stats.evictor_freed += freed;
        for _ in 1..C_PARTITIONS {
            self.offer(false);
        }
        if !self.try_grow() {
            self.stats.refusals += 1;
            self.stats.first_refusal_block.get_or_insert(block);
        }
    }

    fn hold(&mut self, key: u64, delta: i32) {
        if let Some(pins) = self.entries.get_mut(&key) {
            *pins = pins.saturating_add_signed(delta);
        }
    }
}

fn run_trace_c(admission: Admission, cap: u64) -> TraceCStats {
    let mut sim = TraceC::new(admission, cap);
    let mut held: Vec<Vec<u64>> = vec![Vec::new(); C_PARTITIONS];
    let mut local = [0u64; C_PARTITIONS];
    for block in 0..C_BLOCKS {
        let p = (block % C_PARTITIONS as u64) as usize;
        for key in std::mem::take(&mut held[p]) {
            sim.hold(key, -1);
        }
        for _ in 0..C_COLUMNS {
            if let Some(key) = sim.offer(true) {
                held[p].push(key);
            }
        }
        local[p] += 1;
        if local[p].is_multiple_of(C_GROW_EVERY) && sim.aggregate < C_AGG_TARGET {
            sim.grow(block);
        }
    }
    sim.stats.resident_end = sim.resident;
    sim.stats
}

fn trace_c() -> &'static [TraceCStats; 4] {
    static TRACE: OnceLock<[TraceCStats; 4]> = OnceLock::new();
    TRACE.get_or_init(|| C_RUNS.map(|(admission, cap)| run_trace_c(admission, cap)))
}

fn trace_c_run(admission: Admission, cap: u64) -> TraceCStats {
    let i = C_RUNS
        .iter()
        .position(|r| *r == (admission, cap))
        .expect("Trace C run");
    trace_c()[i]
}

fn print_trace_c(runs: &[TraceCStats; 4]) {
    println!(
        "== Trace C: q33 shape, limit 20 GB, other 3 GB, 16 partitions, 13,235 blocks x 4 columns x 60,444 B, +117,440,512 B aggregate per partition every 64 blocks until 10 GB, margin max(1 GiB, 5% of limit)"
    );
    println!(
        "pol cap_GB refusals first_block grows peak_agg_GB cache_end_GB admissions skip_margin skip_limit cap_evict evictor_passes evictor_freed_MB min_unreserved_MB"
    );
    for ((admission, cap), s) in C_RUNS.iter().zip(runs) {
        println!(
            "{:<3} {:>6} {:>8} {:>11} {:>5} {:>11.3} {:>12.3} {:>10} {:>11} {:>10} {:>9} {:>14} {:>16.1} {:>17.1}",
            admission.name(),
            cap / GB,
            s.refusals,
            s.first_refusal_block
                .map_or_else(|| "-".to_string(), |b| b.to_string()),
            s.grows,
            s.peak_aggregate as f64 / GB as f64,
            s.resident_end as f64 / GB as f64,
            s.admissions,
            s.skipped_margin,
            s.skipped_limit,
            s.cap_evictions,
            s.evictor_passes,
            s.evictor_freed as f64 / MB as f64,
            s.min_unreserved as f64 / MB as f64,
        );
    }
}

// ---------------------------------------------------------------------------
// Trace D: q33 with concurrent checked reservations racing the refused retry.

const D_SCAN: u64 = 2_000_000;
const D_CAPS: [u64; 3] = [4 * GB, 8 * GB, 12 * GB];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Evictor {
    /// Frees the shortfall only.
    Exact,
    /// Frees the shortfall plus the margin.
    Slack,
}

impl Evictor {
    fn name(self) -> &'static str {
        match self {
            Evictor::Exact => "M-exact",
            Evictor::Slack => "M-slack",
        }
    }
}

const D_RUNS: [(Evictor, u64); 6] = [
    (Evictor::Exact, D_CAPS[0]),
    (Evictor::Exact, D_CAPS[1]),
    (Evictor::Exact, D_CAPS[2]),
    (Evictor::Slack, D_CAPS[0]),
    (Evictor::Slack, D_CAPS[1]),
    (Evictor::Slack, D_CAPS[2]),
];

#[derive(Clone, Copy, Debug, Default)]
struct TraceDStats {
    c: TraceCStats,
    scan_reservations: u64,
    scan_refusals: u64,
    /// Largest amount a refused aggregate retry was short by.
    max_short: u64,
}

/// Trace C's cache and budget under margin admission, where the step racing a
/// refused aggregate retry is a checked scan reservation by every other
/// partition. A partition releases its scan charges at its next block.
struct TraceD {
    c: TraceC,
    evictor: Evictor,
    scans: [u64; C_PARTITIONS],
    scan_reservations: u64,
    scan_refusals: u64,
    max_short: u64,
}

impl TraceD {
    fn new(evictor: Evictor, cap: u64) -> Self {
        Self {
            c: TraceC::new(Admission::Margin, cap),
            evictor,
            scans: [0; C_PARTITIONS],
            scan_reservations: 0,
            scan_refusals: 0,
            max_short: 0,
        }
    }

    /// One evictor pass for a refused reservation of `bytes`.
    fn evict_for(&mut self, bytes: u64) {
        let shortfall = (self.c.reserved + bytes).saturating_sub(C_LIMIT);
        let target = match self.evictor {
            Evictor::Exact => shortfall,
            Evictor::Slack => shortfall + self.c.margin,
        };
        self.c.stats.evictor_passes += 1;
        let mut freed = 0;
        while freed < target {
            match self.c.evict_one() {
                Some(bytes) => freed += bytes,
                None => break,
            }
        }
        self.c.stats.evictor_freed += freed;
    }

    fn try_scan(&mut self, p: usize) -> bool {
        if self.c.reserved + D_SCAN > C_LIMIT {
            return false;
        }
        self.c.reserved += D_SCAN;
        self.c.note_unreserved();
        self.scans[p] += D_SCAN;
        self.scan_reservations += 1;
        true
    }

    /// A nested checked reservation: on refusal the evictor runs and the
    /// reservation retries once; a second refusal drops it.
    fn scan(&mut self, p: usize) {
        if self.try_scan(p) {
            return;
        }
        self.evict_for(D_SCAN);
        if !self.try_scan(p) {
            self.scan_refusals += 1;
        }
    }

    fn release_scans(&mut self, p: usize) {
        self.c.reserved -= self.scans[p];
        self.scans[p] = 0;
    }

    /// Partition `p`'s checked aggregate reservation at `block`: on refusal the
    /// evictor runs, every other partition makes one checked scan reservation,
    /// and `p` retries once.
    fn grow(&mut self, p: usize, block: u64) {
        if self.c.try_grow() {
            return;
        }
        self.evict_for(C_GROW);
        for q in (0..C_PARTITIONS).filter(|&q| q != p) {
            self.scan(q);
        }
        if !self.c.try_grow() {
            let short = self.c.reserved + C_GROW - C_LIMIT;
            self.max_short = self.max_short.max(short);
            self.c.stats.refusals += 1;
            self.c.stats.first_refusal_block.get_or_insert(block);
        }
    }
}

fn run_trace_d(evictor: Evictor, cap: u64) -> TraceDStats {
    let mut sim = TraceD::new(evictor, cap);
    let mut held: Vec<Vec<u64>> = vec![Vec::new(); C_PARTITIONS];
    let mut local = [0u64; C_PARTITIONS];
    for block in 0..C_BLOCKS {
        let p = (block % C_PARTITIONS as u64) as usize;
        sim.release_scans(p);
        for key in std::mem::take(&mut held[p]) {
            sim.c.hold(key, -1);
        }
        for _ in 0..C_COLUMNS {
            if let Some(key) = sim.c.offer(true) {
                held[p].push(key);
            }
        }
        local[p] += 1;
        if local[p].is_multiple_of(C_GROW_EVERY) && sim.c.aggregate < C_AGG_TARGET {
            sim.grow(p, block);
        }
    }
    sim.c.stats.resident_end = sim.c.resident;
    TraceDStats {
        c: sim.c.stats,
        scan_reservations: sim.scan_reservations,
        scan_refusals: sim.scan_refusals,
        max_short: sim.max_short,
    }
}

fn trace_d() -> &'static [TraceDStats; 6] {
    static TRACE: OnceLock<[TraceDStats; 6]> = OnceLock::new();
    TRACE.get_or_init(|| D_RUNS.map(|(evictor, cap)| run_trace_d(evictor, cap)))
}

fn trace_d_run(evictor: Evictor, cap: u64) -> TraceDStats {
    let i = D_RUNS
        .iter()
        .position(|r| *r == (evictor, cap))
        .expect("Trace D run");
    trace_d()[i]
}

fn print_trace_d(runs: &[TraceDStats; 6]) {
    println!(
        "== Trace D: Trace C's q33 shape and budget, margin on admission; a refused aggregate grow runs the evictor, then each other partition makes one checked 2,000,000 B scan reservation (held to its next block), then the grow retries once"
    );
    println!(
        "policy  cap_GB refusals first_block max_short_MB grows peak_agg_GB cache_end_GB admissions skip_margin evictor_passes evictor_freed_MB scan_res scan_refused min_unreserved_MB"
    );
    for ((evictor, cap), d) in D_RUNS.iter().zip(runs) {
        let s = &d.c;
        println!(
            "{:<7} {:>6} {:>8} {:>11} {:>12.1} {:>5} {:>11.3} {:>12.3} {:>10} {:>11} {:>14} {:>16.1} {:>8} {:>12} {:>17.1}",
            evictor.name(),
            cap / GB,
            s.refusals,
            s.first_refusal_block
                .map_or_else(|| "-".to_string(), |b| b.to_string()),
            d.max_short as f64 / MB as f64,
            s.grows,
            s.peak_aggregate as f64 / GB as f64,
            s.resident_end as f64 / GB as f64,
            s.admissions,
            s.skipped_margin,
            s.evictor_passes,
            s.evictor_freed as f64 / MB as f64,
            d.scan_reservations,
            d.scan_refusals,
            s.min_unreserved as f64 / MB as f64,
        );
    }
}

// ---------------------------------------------------------------------------
// Tests.

#[test]
fn policy_mechanics() {
    let mut sim = Sim::new(Policy::Chosen, 3 * MB);
    // BulkScan: try 1 records a ghost, try 2 admits on the ghost hit, try 3 hits.
    assert!(!sim.access((0, "c"), MB, Intent::BulkScan));
    assert!(!sim.contains(&(0, "c")));
    assert!(!sim.access((0, "c"), MB, Intent::BulkScan));
    assert_eq!(sim.counts.ghost_hits, 1);
    assert!(sim.access((0, "c"), MB, Intent::BulkScan));
    // Interactive admits on first decode.
    assert!(!sim.access((1, "c"), MB, Intent::Interactive));
    assert!(sim.contains(&(1, "c")));
    // Larger than the budget: never admitted.
    sim.access((9, "c"), 4 * MB, Intent::Interactive);
    assert!(!sim.contains(&(9, "c")));
    // CLOCK: (0) and (1) are referenced, (2) is not; admitting 2 MB evicts
    // the unreferenced entry first, then the oldest after one sweep.
    sim.access((1, "c"), MB, Intent::Interactive);
    sim.access((2, "c"), MB, Intent::Interactive);
    sim.access((3, "c"), 2 * MB, Intent::Interactive);
    assert!(!sim.contains(&(2, "c")));
    assert!(sim.contains(&(3, "c")));
    assert!(sim.used <= sim.budget);
    assert_eq!(
        sim.counts.evictions, 2,
        "evicted (2) and one of the cleared entries"
    );

    // First-touch admits a BulkScan read at once.
    let mut sim = Sim::new(Policy::FirstTouch, 3 * MB);
    sim.access((0, "c"), MB, Intent::BulkScan);
    assert!(sim.contains(&(0, "c")));
}

#[test]
fn trace_a_parses_43_statements() {
    let stmts = statements();
    assert_eq!(stmts.len(), 43, "Trace A rows: {}", stmts.len());
    let not_cached: Vec<_> = stmts
        .iter()
        .filter(|s| !s.cached())
        .map(|s| s.name)
        .collect();
    assert_eq!(not_cached, ["q01", "q07", "q21", "q22", "q23", "q24"]);
    let bulk: Vec<_> = stmts
        .iter()
        .filter(|s| s.intent == Intent::BulkScan)
        .map(|s| s.name)
        .collect();
    assert_eq!(bulk, ["q01", "q03", "q04", "q05", "q06", "q07", "q30"]);
}

/// Figure (a): Trace A, P1, 8 GB, compact sizes: every fitting statement's
/// try-3 byte hit share is at least 0.95.
#[test]
fn trace_a_chosen_8gb_compact_fitting_try3_at_least_095() {
    let shares = fitting_try3(trace_a(), Policy::Chosen, 8 * GB, Layout::Compact);
    assert!(!shares.is_empty(), "no fitting statement at 8 GB compact");
    let low: Vec<_> = shares.iter().filter(|(_, s)| *s < 0.95).collect();
    let min = shares.iter().map(|(_, s)| *s).fold(f64::INFINITY, f64::min);
    assert!(
        low.is_empty(),
        "Trace A P1 8 GB compact: try-3 byte hit share min {min:.3} over {} fitting statements; under 0.95: {low:?}",
        shares.len()
    );
}

/// Figure (b): Trace B, P1: W retained at least 0.90 through every bulk scan.
#[test]
fn trace_b_chosen_retains_w_at_least_090() {
    let trace = &trace_b()[policy_index(Policy::Chosen)];
    assert_eq!(trace.scans.len(), SCANS, "bulk scans run");
    let retained: Vec<f64> = trace.scans.iter().map(ScanRow::retained).collect();
    let min = retained.iter().copied().fold(f64::INFINITY, f64::min);
    assert!(
        min >= 0.90,
        "Trace B P1: W retained min {min:.3} over {} scans, band at least 0.90",
        retained.len()
    );
    // After the first full pass, W's whole 2 GB is admitted after every scan.
    let first_full = (W_BLOCKS / SCAN_EVERY) as usize;
    let min_after = trace.scans[first_full..]
        .iter()
        .map(|r| r.after)
        .fold(f64::INFINITY, f64::min);
    assert!(
        min_after >= 0.90,
        "Trace B P1: W admitted share after scans from pass 2 on, min {min_after:.3}, band at least 0.90"
    );
}

/// Figure (c): Trace B, P2: the comparison, printed with no band.
#[test]
fn trace_b_first_touch_retained_reported() {
    let trace = &trace_b()[policy_index(Policy::FirstTouch)];
    assert_eq!(trace.scans.len(), SCANS, "bulk scans run");
    let min = trace
        .scans
        .iter()
        .map(ScanRow::retained)
        .fold(f64::INFINITY, f64::min);
    let mean = trace.scans.iter().map(ScanRow::retained).sum::<f64>() / SCANS as f64;
    println!(
        "Trace B P2: W retained min {min:.3} mean {mean:.3} over {SCANS} scans; steady-state W hit share {:.3}",
        trace.steady
    );
}

#[test]
fn decoded_sim_report() {
    let trace = trace_a();
    assert_eq!(
        trace.rows.len(),
        43 * BUDGETS.len() * LAYOUTS.len(),
        "Trace A rows"
    );
    print_trace_a(trace);
    println!();
    print_trace_b(trace_b());
}

/// Trace C (a): best-effort admission refuses q33's aggregate at least once.
#[test]
fn trace_c_best_effort_8gb_refuses() {
    let s = trace_c_run(Admission::BestEffort, 8 * GB);
    assert!(
        s.refusals >= 1,
        "Trace C B 8 GB: {} refusals (first at block {:?}), peak aggregate {} B, {} evictor passes freeing {} B; band at least 1",
        s.refusals,
        s.first_refusal_block,
        s.peak_aggregate,
        s.evictor_passes,
        s.evictor_freed
    );
}

/// Trace C (b): the margin keeps q33's aggregate admitted at an 8 GB cap.
#[test]
fn trace_c_margin_8gb_never_refuses() {
    let s = trace_c_run(Admission::Margin, 8 * GB);
    assert_eq!(
        s.refusals, 0,
        "Trace C M 8 GB: {} refusals (first at block {:?}), peak aggregate {} B",
        s.refusals, s.first_refusal_block, s.peak_aggregate
    );
    assert!(
        s.peak_aggregate >= C_AGG_TARGET && s.evictor_passes > 0,
        "Trace C M 8 GB: peak aggregate {} B (target {C_AGG_TARGET}), {} evictor passes; the run must reach the target under pressure",
        s.peak_aggregate,
        s.evictor_passes
    );
}

/// Trace C (c): the margin keeps q33's aggregate admitted at 4 GB and 12 GB.
#[test]
fn trace_c_margin_4gb_and_12gb_never_refuse() {
    for cap in [4 * GB, 12 * GB] {
        let s = trace_c_run(Admission::Margin, cap);
        assert_eq!(
            s.refusals,
            0,
            "Trace C M {} GB: {} refusals (first at block {:?}), peak aggregate {} B",
            cap / GB,
            s.refusals,
            s.first_refusal_block,
            s.peak_aggregate
        );
        assert!(
            s.peak_aggregate >= C_AGG_TARGET,
            "Trace C M {} GB: peak aggregate {} B, target {C_AGG_TARGET}",
            cap / GB,
            s.peak_aggregate
        );
    }
}

#[test]
fn decoded_sim_trace_c_report() {
    print_trace_c(trace_c());
}

/// Trace D (a): an evictor that frees the exact shortfall loses the freed
/// bytes to concurrent scan reservations and refuses q33's aggregate at 8 GB.
#[test]
fn trace_d_exact_8gb_refuses() {
    let d = trace_d_run(Evictor::Exact, 8 * GB);
    assert!(
        d.c.refusals >= 1,
        "Trace D M-exact 8 GB: {} refusals (first at block {:?}, max short {} B), peak aggregate {} B, {} evictor passes freeing {} B, {} scan reservations; band at least 1",
        d.c.refusals,
        d.c.first_refusal_block,
        d.max_short,
        d.c.peak_aggregate,
        d.c.evictor_passes,
        d.c.evictor_freed,
        d.scan_reservations
    );
}

/// Trace D (b): an evictor that frees the shortfall plus the margin never
/// refuses q33's aggregate at 4, 8 or 12 GB.
#[test]
fn trace_d_slack_never_refuses() {
    for cap in D_CAPS {
        let d = trace_d_run(Evictor::Slack, cap);
        assert_eq!(
            d.c.refusals,
            0,
            "Trace D M-slack {} GB: {} refusals (first at block {:?}, max short {} B), peak aggregate {} B, {} evictor passes freeing {} B",
            cap / GB,
            d.c.refusals,
            d.c.first_refusal_block,
            d.max_short,
            d.c.peak_aggregate,
            d.c.evictor_passes,
            d.c.evictor_freed
        );
        assert!(
            d.c.peak_aggregate >= C_AGG_TARGET,
            "Trace D M-slack {} GB: peak aggregate {} B, target {C_AGG_TARGET}",
            cap / GB,
            d.c.peak_aggregate
        );
    }
}

#[test]
fn decoded_sim_trace_d_report() {
    print_trace_d(trace_d());
}
