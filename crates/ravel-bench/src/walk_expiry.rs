//! The pure half of the pinned-walk expiry probe (issue #3779 item 1): the
//! pre-registered configuration and bands, the expiry cause labeller, the
//! figure set and its checker, and the stamps that void a run. The
//! `mcp_walk_expiry` bin drives the server and feeds this module; nothing here
//! reads a clock or a store.
//!
//! A walk is page one plus `pages_per_walk - 1` redemptions. It expires at
//! page n when the digest of page n's segment set Kn differs from page one's
//! K0, which is exactly the `SegmentSetPin::Required` check
//! `ravel_query_sql` makes. The cause is read from the key diff
//! (docs/catalog-and-mvcc.md, key layout), and one expiry can carry several
//! labels:
//!
//! - commit: Kn \ K0 holds an `l0/` key;
//! - compaction: K0 \ Kn holds an `l0/` key of some ingest hour h and Kn \ K0
//!   holds an `l1/` key of the same h (an `l0/` key carries no hour, so the
//!   caller supplies it from the snapshot);
//! - fold or retention: K0 \ Kn is non-empty and Kn \ K0 is empty;
//! - other: a changed set none of the three explains.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

/// The checked-in bands file, next to the bin.
pub const BANDS_TOML: &str = include_str!("bin/mcp_walk_expiry.bands.toml");

/// The only bands file version this module reads.
pub const BANDS_VERSION: u32 = 1;

/// The two pre-registered statements.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Statement {
    S1,
    S2,
}

impl Statement {
    pub const ALL: [Statement; 2] = [Statement::S1, Statement::S2];

    pub fn name(self) -> &'static str {
        match self {
            Statement::S1 => "s1",
            Statement::S2 => "s2",
        }
    }

    /// The statement text. S2 is S1 pruned to one metric name and grouped by
    /// series. Its `ORDER BY` names `series_id` after `c` because the page
    /// planner appends no tiebreak for it, and without one every series with
    /// the same count is one group: the walk would page by groups, not rows.
    pub fn sql(self, s2_family: &str) -> String {
        match self {
            Statement::S1 => "SELECT label(labels,'__name__') AS m, count(*) AS c FROM samples \
                              GROUP BY m ORDER BY c DESC"
                .to_string(),
            Statement::S2 => format!(
                "SELECT series_id, count(*) AS c FROM samples \
                 WHERE label(labels,'__name__') = '{}' GROUP BY series_id \
                 ORDER BY c DESC, series_id",
                s2_family.replace('\'', "''")
            ),
        }
    }
}

/// The four window roles, each fixed at page one's t0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowRole {
    /// Ends at the mint.
    Wp,
    /// Still open during the walk.
    Wf,
    /// A few hours old, possibly compacting.
    Wr,
    /// Old and sealed.
    Wo,
}

impl WindowRole {
    pub const ALL: [WindowRole; 4] = [
        WindowRole::Wp,
        WindowRole::Wf,
        WindowRole::Wr,
        WindowRole::Wo,
    ];

    pub fn name(self) -> &'static str {
        match self {
            WindowRole::Wp => "wp",
            WindowRole::Wf => "wf",
            WindowRole::Wr => "wr",
            WindowRole::Wo => "wo",
        }
    }
}

/// Which path ran a walk: the probe over `QueryService::sql_execute_at`, or
/// the real MCP `ravel_query_sql` over `POST /mcp`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    Probe,
    Mcp,
}

/// One cell of the grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Cell {
    pub transport: Transport,
    pub statement: Statement,
    pub window: WindowRole,
    pub delta_ms: u64,
}

impl Cell {
    pub fn label(&self) -> String {
        let prefix = match self.transport {
            Transport::Probe => "",
            Transport::Mcp => "mcp.",
        };
        format!(
            "{prefix}{}.{}.{}",
            self.statement.name(),
            self.window.name(),
            delta_name(self.delta_ms)
        )
    }
}

/// `1000` is `d1s`, `200` is `d200ms`.
pub fn delta_name(delta_ms: u64) -> String {
    if delta_ms.is_multiple_of(1000) {
        format!("d{}s", delta_ms / 1000)
    } else {
        format!("d{delta_ms}ms")
    }
}

// --- configuration -------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BandsFile {
    pub version: u32,
    pub production: ProfileConfig,
    pub smoke: ProfileConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileConfig {
    pub grid: Grid,
    pub windows: Windows,
    pub load: Load,
    pub schedule: Schedule,
    pub stamps: StampRules,
    pub bands: Bands,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Grid {
    pub walks_per_cell: u32,
    pub max_concurrent_walks: usize,
    pub pages_per_walk: u32,
    pub deltas_ms: Vec<u64>,
    pub mcp_crosscheck_walks: u32,
    pub mcp_crosscheck_delta_ms: u64,
}

impl Grid {
    /// The longest delay, the one the Wr band and the decision are read at.
    pub fn longest_delta_ms(&self) -> u64 {
        self.deltas_ms.iter().copied().max().unwrap_or(0)
    }
}

/// Seconds relative to t0, `[start, end)`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Windows {
    pub wp: [i64; 2],
    pub wf: [i64; 2],
    pub wr: [i64; 2],
    pub wo: [i64; 2],
}

impl Windows {
    pub fn offsets_s(&self, role: WindowRole) -> [i64; 2] {
        match role {
            WindowRole::Wp => self.wp,
            WindowRole::Wf => self.wf,
            WindowRole::Wr => self.wr,
            WindowRole::Wo => self.wo,
        }
    }

    /// The window `role` covers for a walk minted at `t0_ns`, as nanosecond
    /// `[start, end)`.
    pub fn range_ns(&self, role: WindowRole, t0_ns: i64) -> (i64, i64) {
        let [start, end] = self.offsets_s(role);
        (
            t0_ns.saturating_add(start.saturating_mul(1_000_000_000)),
            t0_ns.saturating_add(end.saturating_mul(1_000_000_000)),
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Load {
    pub active_series: u64,
    pub scrape_interval_ms: u64,
    pub series_per_body: usize,
    pub s2_family: String,
    pub s1_max_rows: u32,
    pub s2_max_rows: u32,
}

impl Load {
    pub fn max_rows(&self, statement: Statement) -> u32 {
        match statement {
            Statement::S1 => self.s1_max_rows,
            Statement::S2 => self.s2_max_rows,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    pub phase_a_after_s: u64,
    pub phase_b_after_s: u64,
    pub phase_c_after_s: u64,
    pub max_run_s: u64,
}

impl Schedule {
    /// The three phases in order: when each may start, from load start, and
    /// the windows it walks.
    pub fn phases(&self) -> [(u64, &'static [WindowRole]); 3] {
        [
            (self.phase_a_after_s, &[WindowRole::Wp, WindowRole::Wf]),
            (self.phase_b_after_s, &[WindowRole::Wr]),
            (self.phase_c_after_s, &[WindowRole::Wo]),
        ]
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StampRules {
    pub ingest_rate_tolerance: f64,
    pub rate_window_s: u64,
    pub listing_window_s: u64,
    pub listing_every_s: u64,
    pub commit_round_ms: u64,
    pub min_commit_rounds_per_listing: u64,
    pub sample_interval_ms: u64,
    pub min_retention_days: u64,
    pub require_sha_path: bool,
    pub require_wo_sealed: bool,
    pub require_seal_recorded: bool,
    pub require_l1_observed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Bands {
    pub finish_wp: PredictedBand,
    pub finish_wf: PredictedBand,
    pub commit_cause_share_live: RangeBand,
    pub finish_wr_longest_delta: RangeBand,
    pub finish_wo: RangeBand,
    pub commit_expiries_old: RangeBand,
    pub fold_expiries_wo_s2: RangeBand,
    pub short_walks: RangeBand,
    pub failed_walks: RangeBand,
    pub unresolved_hour_keys: RangeBand,
    pub decision_wr_compaction_fold_share: RangeBand,
    pub mcp_crosscheck_abs_diff: RangeBand,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RangeBand {
    pub lo: f64,
    pub hi: f64,
    pub prediction: String,
    pub miss_means: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Model {
    /// `exp(-min(k * delta, L) / c)`: a window that ends at the mint only
    /// sees commits holding samples older than t0, which stop arriving L
    /// after t0.
    ExpMinLag,
    /// `exp(-k * delta / c)`: a window still open sees every commit.
    ExpWalk,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PredictedBand {
    pub model: Model,
    pub tolerance: f64,
    pub prediction: String,
    pub miss_means: String,
}

/// Why a bands file was refused.
#[derive(Debug, thiserror::Error)]
pub enum BandsError {
    #[error("bands file does not parse: {0}")]
    Parse(String),
    #[error("bands file version {found}, this bin reads {BANDS_VERSION}")]
    Version { found: u32 },
    #[error("{profile}: {reason}")]
    Invalid {
        profile: &'static str,
        reason: String,
    },
}

/// Parse and check a bands file.
pub fn load_bands(text: &str) -> Result<BandsFile, BandsError> {
    let file: BandsFile = toml::from_str(text).map_err(|e| BandsError::Parse(e.to_string()))?;
    if file.version != BANDS_VERSION {
        return Err(BandsError::Version {
            found: file.version,
        });
    }
    for (profile, config) in [("production", &file.production), ("smoke", &file.smoke)] {
        config
            .validate()
            .map_err(|reason| BandsError::Invalid { profile, reason })?;
    }
    Ok(file)
}

impl ProfileConfig {
    fn validate(&self) -> Result<(), String> {
        let grid = &self.grid;
        if grid.walks_per_cell == 0 || grid.max_concurrent_walks == 0 {
            return Err("walks_per_cell and max_concurrent_walks must be positive".into());
        }
        if grid.pages_per_walk < 2 {
            return Err("pages_per_walk must be at least 2".into());
        }
        let distinct: BTreeSet<u64> = grid.deltas_ms.iter().copied().collect();
        if grid.deltas_ms.is_empty() || distinct.len() != grid.deltas_ms.len() {
            return Err("deltas_ms must be non-empty and distinct".into());
        }
        if grid.mcp_crosscheck_walks > 0 && !distinct.contains(&grid.mcp_crosscheck_delta_ms) {
            return Err("mcp_crosscheck_delta_ms must be one of deltas_ms".into());
        }
        for role in WindowRole::ALL {
            let [start, end] = self.windows.offsets_s(role);
            if start >= end {
                return Err(format!("window {} is empty", role.name()));
            }
        }
        let s = &self.schedule;
        if !(s.phase_a_after_s <= s.phase_b_after_s
            && s.phase_b_after_s <= s.phase_c_after_s
            && s.phase_c_after_s < s.max_run_s)
        {
            return Err("phases must start in order and before max_run_s".into());
        }
        // A phase may not start before its windows hold data.
        for (after_s, roles) in s.phases() {
            for role in roles {
                let oldest = -self.windows.offsets_s(*role)[0];
                if i64::try_from(after_s).unwrap_or(i64::MAX) < oldest {
                    return Err(format!(
                        "window {} reaches {oldest} s back but its phase starts {after_s} s \
                         after load start",
                        role.name()
                    ));
                }
            }
        }
        if self.load.scrape_interval_ms == 0 || self.load.series_per_body == 0 {
            return Err("scrape_interval_ms and series_per_body must be positive".into());
        }
        if self.load.s1_max_rows == 0 || self.load.s2_max_rows == 0 {
            return Err("max_rows must be positive".into());
        }
        let b = &self.bands;
        for (name, band) in [
            ("commit_cause_share_live", &b.commit_cause_share_live),
            ("finish_wr_longest_delta", &b.finish_wr_longest_delta),
            ("finish_wo", &b.finish_wo),
            ("commit_expiries_old", &b.commit_expiries_old),
            ("fold_expiries_wo_s2", &b.fold_expiries_wo_s2),
            ("short_walks", &b.short_walks),
            ("failed_walks", &b.failed_walks),
            ("unresolved_hour_keys", &b.unresolved_hour_keys),
            (
                "decision_wr_compaction_fold_share",
                &b.decision_wr_compaction_fold_share,
            ),
            ("mcp_crosscheck_abs_diff", &b.mcp_crosscheck_abs_diff),
        ] {
            if !(band.lo.is_finite() && band.hi.is_finite() && band.lo <= band.hi) {
                return Err(format!("band {name} needs finite lo <= hi"));
            }
        }
        for (name, band) in [("finish_wp", &b.finish_wp), ("finish_wf", &b.finish_wf)] {
            if !(band.tolerance.is_finite() && band.tolerance >= 0.0) {
                return Err(format!("band {name} needs a finite tolerance >= 0"));
            }
        }
        Ok(())
    }

    /// Every probe cell of the grid, in a stable order.
    pub fn probe_cells(&self) -> Vec<Cell> {
        let mut cells = Vec::new();
        for statement in Statement::ALL {
            for window in WindowRole::ALL {
                for &delta_ms in &self.grid.deltas_ms {
                    cells.push(Cell {
                        transport: Transport::Probe,
                        statement,
                        window,
                        delta_ms,
                    });
                }
            }
        }
        cells
    }

    /// The cross-check cells: S1 over Wp and Wo through MCP. Empty when the
    /// grid asks for no cross-check walks.
    pub fn mcp_cells(&self) -> Vec<Cell> {
        if self.grid.mcp_crosscheck_walks == 0 {
            return Vec::new();
        }
        [WindowRole::Wp, WindowRole::Wo]
            .into_iter()
            .map(|window| Cell {
                transport: Transport::Mcp,
                statement: Statement::S1,
                window,
                delta_ms: self.grid.mcp_crosscheck_delta_ms,
            })
            .collect()
    }

    pub fn walks_for(&self, cell: &Cell) -> u32 {
        match cell.transport {
            Transport::Probe => self.grid.walks_per_cell,
            Transport::Mcp => self.grid.mcp_crosscheck_walks,
        }
    }
}

/// The MetricsBench profile the load runs: the manifest's `ci` profile with
/// this run's active series and scrape interval, no churn, and enough steps
/// for `max_run_s`. `None` when the manifest has no `ci` profile.
pub fn load_profile(
    workload: &crate::metrics_workload::WorkloadFile,
    config: &ProfileConfig,
) -> Option<crate::metrics_workload::Profile> {
    let mut profile = workload.profile("ci")?.clone();
    let interval_s = (config.load.scrape_interval_ms / 1000).max(1);
    let steps = config.schedule.max_run_s / interval_s + 1;
    profile.name = "walk-expiry".into();
    profile.active_series = config.load.active_series;
    profile.scrape_interval_secs = interval_s;
    profile.churn_basis_points_per_hour = 0;
    profile.samples_per_series = steps;
    profile.duration_secs = steps * interval_s;
    profile.total_samples = steps * config.load.active_series;
    Some(profile)
}

// --- cause labels --------------------------------------------------------

/// What one data-object key is, from its path (docs/catalog-and-mvcc.md):
/// `t/<tenant>/<signal>/l0/<shard>/<file>` or
/// `t/<tenant>/<signal>/l1/<shard>/<ingest_hour>/<file>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyKind {
    L0,
    L1 { hour: u32 },
    Other,
}

pub fn key_kind(key: &str) -> KeyKind {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        ["t", _, _, "l0", _, _] => KeyKind::L0,
        ["t", _, _, "l1", _, hour, _] => match ravel_commit::keys::parse_ingest_hour_string(hour) {
            Ok(hour) => KeyKind::L1 { hour },
            Err(_) => KeyKind::Other,
        },
        _ => KeyKind::Other,
    }
}

/// The labels one expiry carries. Several may be set at once.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Causes {
    pub commit: bool,
    pub compaction: bool,
    pub fold_or_retention: bool,
    pub other: bool,
}

impl Causes {
    pub fn any(&self) -> bool {
        self.commit || self.compaction || self.fold_or_retention || self.other
    }
}

/// [`label_expiry`]'s answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Labelled {
    pub causes: Causes,
    /// `l0/` keys of K0 \ Kn whose ingest hour `hour_of` did not know. Each
    /// one leaves the compaction label undecidable for its hour.
    pub unresolved_hours: u64,
    pub added: u64,
    pub removed: u64,
}

/// Label the change from `k0` to `kn`. `hour_of` gives an `l0/` key's ingest
/// hour from the snapshot that resolved it. Equal sets carry no label.
pub fn label_expiry(
    k0: &BTreeSet<String>,
    kn: &BTreeSet<String>,
    hour_of: impl Fn(&str) -> Option<u32>,
) -> Labelled {
    let added: Vec<&String> = kn.difference(k0).collect();
    let removed: Vec<&String> = k0.difference(kn).collect();
    let mut out = Labelled {
        added: added.len() as u64,
        removed: removed.len() as u64,
        ..Labelled::default()
    };
    if added.is_empty() && removed.is_empty() {
        return out;
    }
    out.causes.commit = added.iter().any(|k| key_kind(k) == KeyKind::L0);
    let mut removed_l0_hours = BTreeSet::new();
    for key in removed.iter().filter(|k| key_kind(k) == KeyKind::L0) {
        match hour_of(key) {
            Some(hour) => {
                removed_l0_hours.insert(hour);
            }
            None => out.unresolved_hours += 1,
        }
    }
    let added_l1_hours: BTreeSet<u32> = added
        .iter()
        .filter_map(|k| match key_kind(k) {
            KeyKind::L1 { hour } => Some(hour),
            _ => None,
        })
        .collect();
    out.causes.compaction = removed_l0_hours
        .intersection(&added_l1_hours)
        .next()
        .is_some();
    out.causes.fold_or_retention = !removed.is_empty() && added.is_empty();
    out.causes.other =
        !(out.causes.commit || out.causes.compaction || out.causes.fold_or_retention);
    out
}

// --- page breaks ---------------------------------------------------------

fn same_tuple(a: &[ravel_sql::ResumeValue], b: &[ravel_sql::ResumeValue]) -> bool {
    use ravel_sql::ResumeValue;
    a.len() == b.len()
        && a.iter().zip(b).all(|pair| match pair {
            (ResumeValue::Float(a), ResumeValue::Float(b)) => a.to_bits() == b.to_bits(),
            (a, b) => a == b,
        })
}

/// How many rows a page keeps, as `ravel_query_sql` keeps them: the first
/// `max_rows` of the `tuples.len()` rows the statement returned (it returns
/// one more than the cap when the result goes on). Under an order that is not
/// total the adapter pages by groups of rows equal under the order, and a
/// group still open at the page's end is held back whole, so the next page
/// resumes after the last complete group. Zero means no complete group fits
/// one page, and the adapter mints no cursor.
pub fn page_break(
    tuples: &[Option<Vec<ravel_sql::ResumeValue>>],
    max_rows: usize,
    total_order: bool,
) -> usize {
    let rows = tuples.len();
    let kept = rows.min(max_rows);
    if total_order {
        return kept;
    }
    let same = |a: usize, b: usize| match (tuples.get(a), tuples.get(b)) {
        (Some(Some(a)), Some(Some(b))) => same_tuple(a, b),
        _ => false,
    };
    let Some(last) = kept.checked_sub(1) else {
        return 0;
    };
    if kept < rows && !same(last, kept) {
        return kept;
    }
    let mut start = last;
    while start > 0 && same(start - 1, last) {
        start -= 1;
    }
    start
}

// --- walks and tallies ---------------------------------------------------

/// How one walk ended.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "end", rename_all = "snake_case")]
pub enum WalkEnd {
    /// Every page after the first saw page one's segment set.
    Finished,
    /// Page `page` (1-based, at least 2) saw a different set.
    Expired { page: u32, causes: Causes },
    /// The result ran out after `pages` pages, before `pages_per_walk`.
    Short { pages: u32 },
    /// A page failed for a reason other than an expiry.
    Failed { page: u32, error: String },
}

/// One cell's walks.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct CellTally {
    pub walks: u64,
    pub finished: u64,
    pub expired: u64,
    pub short: u64,
    pub failed: u64,
    pub commit: u64,
    pub compaction: u64,
    pub fold_or_retention: u64,
    pub other: u64,
    /// Expiries with a compaction or a fold-or-retention label, each counted
    /// once.
    pub compaction_or_fold: u64,
}

impl CellTally {
    pub fn record(&mut self, end: &WalkEnd) {
        self.walks += 1;
        match end {
            WalkEnd::Finished => self.finished += 1,
            WalkEnd::Expired { causes, .. } => {
                self.expired += 1;
                self.commit += u64::from(causes.commit);
                self.compaction += u64::from(causes.compaction);
                self.fold_or_retention += u64::from(causes.fold_or_retention);
                self.other += u64::from(causes.other);
                self.compaction_or_fold += u64::from(causes.compaction || causes.fold_or_retention);
            }
            WalkEnd::Short { .. } => self.short += 1,
            WalkEnd::Failed { .. } => self.failed += 1,
        }
    }

    pub fn finish_share(&self) -> Option<f64> {
        (self.walks > 0).then(|| self.finished as f64 / self.walks as f64)
    }
}

// --- figures -------------------------------------------------------------

/// A figure's band once its prediction is resolved against the stamps.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ExpectedFigure {
    pub name: String,
    /// `None` when a predicted band could not be resolved (c or L missing).
    pub band: Option<(f64, f64)>,
    pub prediction: String,
    pub miss_means: String,
}

/// The measured inputs the predicted bands need.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Rates {
    /// c, the median L0 commit interval, in seconds.
    pub commit_interval_s: Option<f64>,
    /// L, the largest event-time lag of a commit, in seconds.
    pub commit_lag_s: Option<f64>,
}

/// The predicted finish share for `model` at `delta_ms` with `k` inter-page
/// delays, or `None` without c (and L, for [`Model::ExpMinLag`]).
pub fn predicted_share(model: Model, k: u32, delta_ms: u64, rates: Rates) -> Option<f64> {
    let c = rates.commit_interval_s.filter(|c| *c > 0.0)?;
    let walk_s = f64::from(k) * delta_ms as f64 / 1000.0;
    let exposed_s = match model {
        Model::ExpWalk => walk_s,
        Model::ExpMinLag => walk_s.min(rates.commit_lag_s?),
    };
    Some((-exposed_s / c).exp())
}

fn range(name: String, band: &RangeBand) -> ExpectedFigure {
    ExpectedFigure {
        name,
        band: Some((band.lo, band.hi)),
        prediction: band.prediction.clone(),
        miss_means: band.miss_means.clone(),
    }
}

/// Every figure the run must emit, each with its band.
pub fn expected_figures(config: &ProfileConfig, rates: Rates) -> Vec<ExpectedFigure> {
    let b = &config.bands;
    let k = config.grid.pages_per_walk - 1;
    let longest = config.grid.longest_delta_ms();
    let mut out = Vec::new();
    for statement in Statement::ALL {
        for (window, band) in [
            (WindowRole::Wp, &b.finish_wp),
            (WindowRole::Wf, &b.finish_wf),
        ] {
            for &delta_ms in &config.grid.deltas_ms {
                let predicted = predicted_share(band.model, k, delta_ms, rates);
                out.push(ExpectedFigure {
                    name: finish_name(statement, window, delta_ms),
                    band: predicted
                        .map(|p| ((p - band.tolerance).max(0.0), (p + band.tolerance).min(1.0))),
                    prediction: match predicted {
                        Some(p) => format!("{} = {p:.4}", band.prediction),
                        None => format!("{} (unresolved)", band.prediction),
                    },
                    miss_means: band.miss_means.clone(),
                });
            }
        }
        out.push(range(
            finish_name(statement, WindowRole::Wr, longest),
            &b.finish_wr_longest_delta,
        ));
        for &delta_ms in &config.grid.deltas_ms {
            out.push(range(
                finish_name(statement, WindowRole::Wo, delta_ms),
                &b.finish_wo,
            ));
        }
    }
    out.push(range(
        "commit_cause_share.wp_wf".into(),
        &b.commit_cause_share_live,
    ));
    out.push(range(
        "commit_expiries.wr_wo".into(),
        &b.commit_expiries_old,
    ));
    out.push(range("fold_expiries.wo.s2".into(), &b.fold_expiries_wo_s2));
    out.push(range("short_walks".into(), &b.short_walks));
    out.push(range("failed_walks".into(), &b.failed_walks));
    out.push(range(
        "unresolved_hour_keys".into(),
        &b.unresolved_hour_keys,
    ));
    out.push(range(
        decision_name(longest),
        &b.decision_wr_compaction_fold_share,
    ));
    for cell in config.mcp_cells() {
        out.push(range(
            crosscheck_name(cell.window, cell.delta_ms),
            &b.mcp_crosscheck_abs_diff,
        ));
    }
    out
}

pub fn finish_name(statement: Statement, window: WindowRole, delta_ms: u64) -> String {
    format!(
        "finish_share.{}.{}.{}",
        statement.name(),
        window.name(),
        delta_name(delta_ms)
    )
}

pub fn decision_name(delta_ms: u64) -> String {
    format!("decision.wr_compaction_fold_share.{}", delta_name(delta_ms))
}

pub fn crosscheck_name(window: WindowRole, delta_ms: u64) -> String {
    format!(
        "mcp_crosscheck.abs_diff.s1.{}.{}",
        window.name(),
        delta_name(delta_ms)
    )
}

/// The figures the tallies support. A figure whose denominator is zero is
/// left out, so the checker reports it missing rather than inventing a value.
pub fn compute_figures(
    config: &ProfileConfig,
    tallies: &BTreeMap<Cell, CellTally>,
    unresolved_hour_keys: u64,
) -> Vec<(String, f64)> {
    let longest = config.grid.longest_delta_ms();
    let probe = |statement, window, delta_ms| {
        tallies.get(&Cell {
            transport: Transport::Probe,
            statement,
            window,
            delta_ms,
        })
    };
    let mut out = Vec::new();
    for statement in Statement::ALL {
        for window in [WindowRole::Wp, WindowRole::Wf] {
            for &delta_ms in &config.grid.deltas_ms {
                if let Some(share) =
                    probe(statement, window, delta_ms).and_then(|t| t.finish_share())
                {
                    out.push((finish_name(statement, window, delta_ms), share));
                }
            }
        }
        if let Some(share) =
            probe(statement, WindowRole::Wr, longest).and_then(|t| t.finish_share())
        {
            out.push((finish_name(statement, WindowRole::Wr, longest), share));
        }
        for &delta_ms in &config.grid.deltas_ms {
            if let Some(share) =
                probe(statement, WindowRole::Wo, delta_ms).and_then(|t| t.finish_share())
            {
                out.push((finish_name(statement, WindowRole::Wo, delta_ms), share));
            }
        }
    }
    let probe_cells = || {
        tallies
            .iter()
            .filter(|(cell, _)| cell.transport == Transport::Probe)
    };
    let (live_commit, live_expired) = probe_cells()
        .filter(|(cell, _)| matches!(cell.window, WindowRole::Wp | WindowRole::Wf))
        .fold((0u64, 0u64), |(c, e), (_, t)| (c + t.commit, e + t.expired));
    if live_expired > 0 {
        out.push((
            "commit_cause_share.wp_wf".into(),
            live_commit as f64 / live_expired as f64,
        ));
    }
    let old_commit: u64 = probe_cells()
        .filter(|(cell, _)| matches!(cell.window, WindowRole::Wr | WindowRole::Wo))
        .map(|(_, t)| t.commit)
        .sum();
    out.push(("commit_expiries.wr_wo".into(), old_commit as f64));
    let wo_s2_fold: u64 = probe_cells()
        .filter(|(cell, _)| cell.window == WindowRole::Wo && cell.statement == Statement::S2)
        .map(|(_, t)| t.fold_or_retention)
        .sum();
    out.push(("fold_expiries.wo.s2".into(), wo_s2_fold as f64));
    let short: u64 = tallies.values().map(|t| t.short).sum();
    out.push(("short_walks".into(), short as f64));
    let failed: u64 = tallies.values().map(|t| t.failed).sum();
    out.push(("failed_walks".into(), failed as f64));
    out.push(("unresolved_hour_keys".into(), unresolved_hour_keys as f64));
    let (wr_cf, wr_walks) = probe_cells()
        .filter(|(cell, _)| cell.window == WindowRole::Wr && cell.delta_ms == longest)
        .fold((0u64, 0u64), |(c, w), (_, t)| {
            (c + t.compaction_or_fold, w + t.walks)
        });
    if wr_walks > 0 {
        out.push((decision_name(longest), wr_cf as f64 / wr_walks as f64));
    }
    for cell in config.mcp_cells() {
        let mcp = tallies.get(&cell).and_then(CellTally::finish_share);
        let probe = probe(Statement::S1, cell.window, cell.delta_ms).and_then(|t| t.finish_share());
        if let (Some(mcp), Some(probe)) = (mcp, probe) {
            out.push((
                crosscheck_name(cell.window, cell.delta_ms),
                (mcp - probe).abs(),
            ));
        }
    }
    out
}

/// Why the figure set does not hold.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum FigureError {
    Missing {
        name: String,
    },
    Duplicated {
        name: String,
        count: usize,
    },
    OutOfBand {
        name: String,
        value: f64,
        lo: f64,
        hi: f64,
        miss_means: String,
    },
    Unresolved {
        name: String,
    },
    Unexpected {
        name: String,
    },
}

impl fmt::Display for FigureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FigureError::Missing { name } => write!(f, "{name} was not emitted"),
            FigureError::Duplicated { name, count } => {
                write!(f, "{name} was emitted {count} times")
            }
            FigureError::OutOfBand {
                name,
                value,
                lo,
                hi,
                miss_means,
            } => write!(f, "{name} = {value} is outside [{lo}, {hi}]: {miss_means}"),
            FigureError::Unresolved { name } => {
                write!(f, "{name} has a predicted band the stamps cannot resolve")
            }
            FigureError::Unexpected { name } => write!(f, "{name} is not a pre-registered figure"),
        }
    }
}

/// Check that every expected figure is emitted exactly once and inside its
/// band, and that nothing unexpected is emitted. Empty means the set holds.
pub fn check_figures(emitted: &[(String, f64)], expected: &[ExpectedFigure]) -> Vec<FigureError> {
    let mut errors = Vec::new();
    for figure in expected {
        let values: Vec<f64> = emitted
            .iter()
            .filter(|(name, _)| *name == figure.name)
            .map(|(_, value)| *value)
            .collect();
        let value = match values.as_slice() {
            [] => {
                errors.push(FigureError::Missing {
                    name: figure.name.clone(),
                });
                continue;
            }
            [value] => *value,
            _ => {
                errors.push(FigureError::Duplicated {
                    name: figure.name.clone(),
                    count: values.len(),
                });
                continue;
            }
        };
        let Some((lo, hi)) = figure.band else {
            errors.push(FigureError::Unresolved {
                name: figure.name.clone(),
            });
            continue;
        };
        if !(value.is_finite() && lo <= value && value <= hi) {
            errors.push(FigureError::OutOfBand {
                name: figure.name.clone(),
                value,
                lo,
                hi,
                miss_means: figure.miss_means.clone(),
            });
        }
    }
    let known: BTreeSet<&str> = expected.iter().map(|f| f.name.as_str()).collect();
    let mut unexpected = BTreeSet::new();
    for (name, _) in emitted {
        if !known.contains(name.as_str()) && unexpected.insert(name.clone()) {
            errors.push(FigureError::Unexpected { name: name.clone() });
        }
    }
    errors
}

// --- stamps --------------------------------------------------------------

/// One listing of the commits of the last `listing_window_s`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct CommitListing {
    pub at_ns: i64,
    pub commits: u64,
    /// Commit rounds: commits created within `commit_round_ms` of the round's
    /// first commit are one round. The shards of one tenant flush together,
    /// and a walk cannot land between the commits of one round, so the
    /// interval that predicts an expiry is the one between rounds.
    pub rounds: u64,
    /// Gaps between consecutive rounds, in seconds.
    pub intervals_s: Vec<f64>,
    /// The largest `created - min_event_ts` among the listed commits.
    pub max_lag_s: Option<f64>,
}

/// One L0 commit as a listing sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct L0Commit {
    pub created_ns: i64,
    pub min_event_ts_ns: i64,
}

/// Build the listing at `at_ns` from the L0 commits a resolve returned,
/// keeping those created in `(at_ns - window_ns, at_ns]` and grouping them
/// into rounds of `round_ns`.
pub fn commit_listing(
    commits: &[L0Commit],
    at_ns: i64,
    window_ns: i64,
    round_ns: i64,
) -> CommitListing {
    let mut kept: Vec<L0Commit> = commits
        .iter()
        .copied()
        .filter(|c| c.created_ns > at_ns - window_ns && c.created_ns <= at_ns)
        .collect();
    kept.sort_by_key(|c| c.created_ns);
    let mut round_starts: Vec<i64> = Vec::new();
    for commit in &kept {
        match round_starts.last() {
            Some(start) if commit.created_ns - start <= round_ns => {}
            _ => round_starts.push(commit.created_ns),
        }
    }
    let intervals_s = round_starts
        .windows(2)
        .map(|w| (w[1] - w[0]) as f64 / 1e9)
        .collect();
    let max_lag_s = kept
        .iter()
        .map(|c| (c.created_ns - c.min_event_ts_ns) as f64 / 1e9)
        .fold(None, |acc: Option<f64>, lag| {
            Some(acc.map_or(lag, |a| a.max(lag)))
        });
    CommitListing {
        at_ns,
        commits: kept.len() as u64,
        rounds: round_starts.len() as u64,
        intervals_s,
        max_lag_s,
    }
}

/// The median of `values`, `None` when empty.
pub fn median(values: &[f64]) -> Option<f64> {
    let mut sorted: Vec<f64> = values.iter().copied().filter(|v| v.is_finite()).collect();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by(f64::total_cmp);
    let mid = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    })
}

/// Everything a run is stamped with. [`void_reasons`] judges it.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Stamps {
    /// Accepted samples a second, nominal and per rate window over the walk
    /// phase.
    pub ingest_rate_nominal_per_s: f64,
    pub ingest_rate_windows_per_s: Vec<f64>,
    pub ingest_failed_requests: u64,
    pub commit_listings: Vec<CommitListing>,
    /// c and L over every listing.
    pub rates: Rates,
    /// Seconds from an ingest hour's end to the first L1 part the sampler saw
    /// for it.
    pub hour_end_to_first_l1_s: BTreeMap<u32, f64>,
    /// Seconds from an ingest hour's end to the first fold watermark at or
    /// past it.
    pub fold_seal_after_hour_end_s: BTreeMap<u32, f64>,
    /// Ingest hours some Wr or Wo walk's page one read.
    pub wr_wo_hours: BTreeSet<u32>,
    /// Wo walks whose newest ingest hour was past the fold watermark at t0.
    pub wo_unsealed_walks: u64,
    pub pages: u64,
    pub pages_with_pending_erasure: u64,
    /// `None` is no retention, which never age-deletes.
    pub retention_days: Option<u64>,
    pub binary_path: String,
}

/// A 40-character lowercase hex path component, the commit a binary was
/// built from.
pub fn sha_in_path(path: &str) -> Option<&str> {
    path.split(['/', '\\', '-', '_', '.']).find(|part| {
        part.len() == 40 && part.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// Every reason the stamps void the run. Empty means the stamps hold.
pub fn void_reasons(stamps: &Stamps, rules: &StampRules) -> Vec<String> {
    let mut out = Vec::new();
    let nominal = stamps.ingest_rate_nominal_per_s;
    if stamps.ingest_rate_windows_per_s.is_empty() {
        out.push("ingest rate: no rate window was measured".to_string());
    }
    for (i, rate) in stamps.ingest_rate_windows_per_s.iter().enumerate() {
        if nominal <= 0.0 || ((rate - nominal) / nominal).abs() > rules.ingest_rate_tolerance {
            out.push(format!(
                "ingest rate: window {i} ran {rate:.1}/s, nominal {nominal:.1}/s, tolerance {}",
                rules.ingest_rate_tolerance
            ));
        }
    }
    if stamps.ingest_failed_requests > 0 {
        out.push(format!(
            "ingest: {} Remote Write requests failed",
            stamps.ingest_failed_requests
        ));
    }
    if stamps.commit_listings.is_empty() {
        out.push("c and L: no commit listing was taken".to_string());
    }
    for listing in &stamps.commit_listings {
        if listing.rounds < rules.min_commit_rounds_per_listing {
            out.push(format!(
                "c and L: the listing at {} saw {} commit rounds, fewer than {}",
                listing.at_ns, listing.rounds, rules.min_commit_rounds_per_listing
            ));
        }
    }
    if stamps.rates.commit_interval_s.is_none() {
        out.push("c: no commit interval was measured".to_string());
    }
    if stamps.rates.commit_lag_s.is_none() {
        out.push("L: no commit lag was measured".to_string());
    }
    if rules.require_l1_observed && stamps.hour_end_to_first_l1_s.is_empty() {
        out.push("hour-end-to-first-L1: no L1 part appeared during the run".to_string());
    }
    if rules.require_seal_recorded {
        if stamps.wr_wo_hours.is_empty() {
            out.push("fold seal: no Wr or Wo walk recorded its hours".to_string());
        }
        for hour in &stamps.wr_wo_hours {
            if !stamps.fold_seal_after_hour_end_s.contains_key(hour) {
                out.push(format!(
                    "fold seal: ingest hour {hour}, read by a Wr or Wo walk, has no recorded seal \
                     time"
                ));
            }
        }
    }
    if rules.require_wo_sealed && stamps.wo_unsealed_walks > 0 {
        out.push(format!(
            "fold seal: {} Wo walks read an hour the fold had not sealed at t0",
            stamps.wo_unsealed_walks
        ));
    }
    if stamps.pages == 0 {
        out.push("pending_erasure: no page ran".to_string());
    }
    if stamps.pages_with_pending_erasure > 0 {
        out.push(format!(
            "pending_erasure: {} pages read a snapshot with a pending erasure",
            stamps.pages_with_pending_erasure
        ));
    }
    if let Some(days) = stamps.retention_days
        && days < rules.min_retention_days
    {
        out.push(format!(
            "retention: {days} d is shorter than {} d",
            rules.min_retention_days
        ));
    }
    if rules.require_sha_path && sha_in_path(&stamps.binary_path).is_none() {
        out.push(format!(
            "binary: {} names no 40-character commit",
            stamps.binary_path
        ));
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const TENANT: &str = "0123456789abcdef0123456789abcdef";

    fn l0(name: &str) -> String {
        format!("t/{TENANT}/m/l0/0/{name}.1.1.aaaaaaaaaaaaaaaa.rseg")
    }

    fn l1(hour: u32, name: &str) -> String {
        format!(
            "t/{TENANT}/m/l1/0/{}/bbbbbbbbbbbbbbbb.0000.{name}.rseg",
            ravel_commit::keys::ingest_hour_string(hour)
        )
    }

    fn set(keys: &[&String]) -> BTreeSet<String> {
        keys.iter().map(|k| (*k).clone()).collect()
    }

    fn production() -> ProfileConfig {
        load_bands(BANDS_TOML)
            .expect("the checked-in bands file loads")
            .production
    }

    #[test]
    fn the_checked_in_bands_file_loads_and_names_every_band() {
        let file = load_bands(BANDS_TOML).expect("loads");
        let p = &file.production;
        assert_eq!(p.grid.walks_per_cell, 60);
        assert_eq!(p.grid.max_concurrent_walks, 6);
        assert_eq!(p.grid.deltas_ms, vec![1000, 10000, 60000]);
        assert_eq!(p.windows.wo, [-18000, -14400]);
        assert_eq!(p.probe_cells().len(), 24);
        assert_eq!(p.bands.finish_wo.lo, 0.98);
        // The whole run, warmup included, inside 7.5 h.
        assert!(p.schedule.max_run_s <= 27_000);
    }

    #[test]
    fn a_bands_file_with_an_unknown_key_is_refused() {
        let text = BANDS_TOML.replace("version = 1", "version = 1\nsurprise = 2");
        assert!(matches!(load_bands(&text), Err(BandsError::Parse(_))));
    }

    #[test]
    fn a_phase_that_starts_before_its_window_holds_data_is_refused() {
        let text = BANDS_TOML.replace("phase_c_after_s = 18300", "phase_c_after_s = 17000");
        let err = load_bands(&text).expect_err("Wo reaches 18000 s back");
        assert!(err.to_string().contains("window wo"), "{err}");
    }

    #[test]
    fn key_kinds_follow_the_key_layout() {
        assert_eq!(key_kind(&l0("w")), KeyKind::L0);
        assert_eq!(key_kind(&l1(490_000, "c")), KeyKind::L1 { hour: 490_000 });
        assert_eq!(
            key_kind(&format!("t/{TENANT}/m/c/0/20260101T00/w.1.1.cmt")),
            KeyKind::Other
        );
    }

    /// FLIP: with the same-hour condition dropped from compaction
    /// (`!removed_l0_hours.is_empty() && !added_l1_hours.is_empty()`), the
    /// different-hour case labels compaction and its assertion fails.
    #[test]
    fn the_cause_labeller_reads_the_key_diff() {
        let (a, b, c) = (l0("a"), l0("b"), l0("c"));
        let hour_of = |key: &str| match key {
            k if k == a => Some(100),
            k if k == b => Some(100),
            k if k == c => Some(101),
            _ => None,
        };

        let commit = label_expiry(&set(&[&a]), &set(&[&a, &b]), hour_of);
        assert_eq!(
            commit.causes,
            Causes {
                commit: true,
                ..Causes::default()
            }
        );

        let part = l1(100, "p");
        let compaction = label_expiry(&set(&[&a, &b]), &set(&[&part]), hour_of);
        assert_eq!(
            compaction.causes,
            Causes {
                compaction: true,
                ..Causes::default()
            }
        );

        let other_hour = l1(102, "q");
        let different_hour = label_expiry(&set(&[&c]), &set(&[&other_hour]), hour_of);
        assert_eq!(
            different_hour.causes,
            Causes {
                other: true,
                ..Causes::default()
            },
            "an L1 part of another hour is not the compaction of the removed l0"
        );

        let fold = label_expiry(&set(&[&a, &part]), &set(&[&part]), hour_of);
        assert_eq!(
            fold.causes,
            Causes {
                fold_or_retention: true,
                ..Causes::default()
            }
        );

        let mixed = label_expiry(&set(&[&a, &b]), &set(&[&part, &c]), hour_of);
        assert_eq!(
            mixed.causes,
            Causes {
                commit: true,
                compaction: true,
                ..Causes::default()
            }
        );

        let unchanged = label_expiry(&set(&[&a]), &set(&[&a]), hour_of);
        assert!(!unchanged.causes.any());

        let unknown = l0("z");
        let unresolved = label_expiry(&set(&[&unknown]), &set(&[&part]), hour_of);
        assert_eq!(unresolved.unresolved_hours, 1);
        assert!(unresolved.causes.other);
    }

    fn complete(config: &ProfileConfig, rates: Rates) -> Vec<(String, f64)> {
        expected_figures(config, rates)
            .into_iter()
            .map(|f| {
                let (lo, hi) = f.band.expect("resolved");
                (f.name, (lo + hi) / 2.0)
            })
            .collect()
    }

    /// FLIP: with the checker skipping a figure it finds no value for (the
    /// `[] => continue` arm without the push), the missing-figure case reports
    /// nothing and its assertion fails.
    #[test]
    fn walk_expiry_figures_are_present_exactly_once() {
        let config = production();
        let rates = Rates {
            commit_interval_s: Some(2.0),
            commit_lag_s: Some(5.0),
        };
        let expected = expected_figures(&config, rates);
        // 2 statements x (3 Wp + 3 Wf + 1 Wr + 3 Wo) + 7 pooled + 2 cross-check.
        assert_eq!(expected.len(), 29);

        let full = complete(&config, rates);
        assert_eq!(check_figures(&full, &expected), Vec::new());

        let mut missing = full.clone();
        let dropped = missing.remove(3).0;
        assert_eq!(
            check_figures(&missing, &expected),
            vec![FigureError::Missing { name: dropped }]
        );

        let mut duplicated = full.clone();
        duplicated.push(full[0].clone());
        assert_eq!(
            check_figures(&duplicated, &expected),
            vec![FigureError::Duplicated {
                name: full[0].0.clone(),
                count: 2
            }]
        );

        let mut out_of_band = full.clone();
        let short = out_of_band
            .iter_mut()
            .find(|(name, _)| name == "short_walks")
            .expect("short_walks is expected");
        short.1 = 1.0;
        let errors = check_figures(&out_of_band, &expected);
        assert!(
            matches!(errors.as_slice(), [FigureError::OutOfBand { name, value, .. }]
                if name == "short_walks" && *value == 1.0),
            "{errors:?}"
        );

        let mut unexpected = full.clone();
        unexpected.push(("finish_share.s1.wr.d1s".into(), 1.0));
        assert_eq!(
            check_figures(&unexpected, &expected),
            vec![FigureError::Unexpected {
                name: "finish_share.s1.wr.d1s".into()
            }]
        );

        let unresolved = expected_figures(&config, Rates::default());
        let errors = check_figures(&full, &unresolved);
        assert_eq!(
            errors
                .iter()
                .filter(|e| matches!(e, FigureError::Unresolved { .. }))
                .count(),
            12,
            "every Wp and Wf band needs c"
        );
    }

    #[test]
    fn predicted_bands_follow_the_two_models() {
        let rates = Rates {
            commit_interval_s: Some(10.0),
            commit_lag_s: Some(6.0),
        };
        // Wp: exposed min(3 * 1 s, 6 s) = 3 s; Wf: 3 s.
        let wp = predicted_share(Model::ExpMinLag, 3, 1000, rates).expect("resolved");
        assert_eq!(wp.to_bits(), (-0.3f64).exp().to_bits());
        // Wp at 60 s is capped at L = 6 s; Wf is not.
        let wp60 = predicted_share(Model::ExpMinLag, 3, 60_000, rates).expect("resolved");
        assert_eq!(wp60.to_bits(), (-0.6f64).exp().to_bits());
        let wf60 = predicted_share(Model::ExpWalk, 3, 60_000, rates).expect("resolved");
        assert_eq!(wf60.to_bits(), (-18.0f64).exp().to_bits());
        assert_eq!(
            predicted_share(Model::ExpMinLag, 3, 1000, Rates::default()),
            None
        );
    }

    #[test]
    fn figures_come_from_the_tallies() {
        let config = production();
        let mut tallies = BTreeMap::new();
        for cell in config.probe_cells().into_iter().chain(config.mcp_cells()) {
            let mut tally = CellTally::default();
            for i in 0..config.walks_for(&cell) {
                let end = if cell.window == WindowRole::Wp && i % 2 == 0 {
                    WalkEnd::Expired {
                        page: 2,
                        causes: Causes {
                            commit: true,
                            ..Causes::default()
                        },
                    }
                } else {
                    WalkEnd::Finished
                };
                tally.record(&end);
            }
            tallies.insert(cell, tally);
        }
        let figures = compute_figures(&config, &tallies, 0);
        let get = |name: &str| {
            figures
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| *v)
                .expect(name)
        };
        assert_eq!(get("finish_share.s1.wp.d1s"), 0.5);
        assert_eq!(get("finish_share.s2.wo.d60s"), 1.0);
        assert_eq!(get("commit_cause_share.wp_wf"), 1.0);
        assert_eq!(get("commit_expiries.wr_wo"), 0.0);
        assert_eq!(get("mcp_crosscheck.abs_diff.s1.wp.d10s"), 0.0);
        let expected = expected_figures(
            &config,
            Rates {
                commit_interval_s: Some(2.0),
                commit_lag_s: Some(5.0),
            },
        );
        let names: BTreeSet<&str> = figures.iter().map(|(n, _)| n.as_str()).collect();
        let wanted: BTreeSet<&str> = expected.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, wanted, "the tallies support exactly the figure set");
    }

    #[test]
    fn a_commit_listing_reads_intervals_and_lag_inside_its_window() {
        let s = 1_000_000_000i64;
        let commits = [
            L0Commit {
                created_ns: 100 * s,
                min_event_ts_ns: 97 * s,
            },
            L0Commit {
                created_ns: 104 * s,
                min_event_ts_ns: 103 * s,
            },
            L0Commit {
                created_ns: 102 * s,
                min_event_ts_ns: 95 * s,
            },
            L0Commit {
                created_ns: 10 * s,
                min_event_ts_ns: 0,
            },
            // A second shard's commit in the round that opened at 102 s.
            L0Commit {
                created_ns: 102 * s + 1_000,
                min_event_ts_ns: 101 * s,
            },
        ];
        let listing = commit_listing(&commits, 105 * s, 10 * s, s / 2);
        assert_eq!(listing.commits, 4);
        assert_eq!(listing.rounds, 3);
        assert_eq!(listing.intervals_s, vec![2.0, 2.0]);
        assert_eq!(listing.max_lag_s, Some(7.0));
        assert_eq!(median(&[3.0, 1.0, 2.0, 10.0]), Some(2.5));
        assert_eq!(median(&[]), None);
    }

    #[test]
    fn stamps_void_the_run_on_each_rule() {
        let rules = production().stamps;
        let good = Stamps {
            ingest_rate_nominal_per_s: 100.0,
            ingest_rate_windows_per_s: vec![95.0, 105.0],
            commit_listings: vec![CommitListing {
                commits: 200,
                rounds: 50,
                ..CommitListing::default()
            }],
            rates: Rates {
                commit_interval_s: Some(2.0),
                commit_lag_s: Some(4.0),
            },
            hour_end_to_first_l1_s: [(10, 4000.0)].into(),
            fold_seal_after_hour_end_s: [(10, 5000.0)].into(),
            wr_wo_hours: [10].into(),
            pages: 4,
            binary_path: "/mb/bin/0123456789abcdef0123456789abcdef01234567/mcp_walk_expiry".into(),
            ..Stamps::default()
        };
        assert_eq!(void_reasons(&good, &rules), Vec::<String>::new());

        let cases: Vec<(&str, Stamps)> = vec![
            (
                "ingest rate",
                Stamps {
                    ingest_rate_windows_per_s: vec![80.0],
                    ..good.clone()
                },
            ),
            (
                "pending_erasure",
                Stamps {
                    pages_with_pending_erasure: 1,
                    ..good.clone()
                },
            ),
            (
                "retention",
                Stamps {
                    retention_days: Some(2),
                    ..good.clone()
                },
            ),
            (
                "binary",
                Stamps {
                    binary_path: "target/release/mcp_walk_expiry".into(),
                    ..good.clone()
                },
            ),
            (
                "fold seal",
                Stamps {
                    wr_wo_hours: [10, 11].into(),
                    ..good.clone()
                },
            ),
            (
                "fold seal",
                Stamps {
                    wo_unsealed_walks: 1,
                    ..good.clone()
                },
            ),
            (
                "hour-end-to-first-L1",
                Stamps {
                    hour_end_to_first_l1_s: BTreeMap::new(),
                    ..good.clone()
                },
            ),
            (
                "c:",
                Stamps {
                    rates: Rates {
                        commit_interval_s: None,
                        commit_lag_s: Some(4.0),
                    },
                    ..good.clone()
                },
            ),
        ];
        for (rule, stamps) in cases {
            let reasons = void_reasons(&stamps, &rules);
            assert!(
                reasons.len() == 1 && reasons[0].starts_with(rule),
                "{rule}: {reasons:?}"
            );
        }
    }

    #[test]
    fn statements_plan_as_pinned_pages() {
        for statement in Statement::ALL {
            let plan = ravel_sql::plan_page(&statement.sql("metricsbench_requests_total"), None)
                .expect("both statements page");
            assert!(
                plan.order_from_other_rows,
                "{} orders by count, so its walk is Required-pinned",
                statement.name()
            );
            // A GROUP BY output is never total to the planner, so the walk
            // pages by groups of equal order tuples. S1 orders by the count
            // alone, which pages because the workload's four largest families
            // have different series counts; S2's series_id term keeps each of
            // its groups one row.
            assert!(!plan.total_order(), "{}", statement.name());
            let columns: Vec<&str> = plan.order_by.iter().map(|t| t.column.as_str()).collect();
            let wanted: &[&str] = match statement {
                Statement::S1 => &["c"],
                Statement::S2 => &["c", "series_id"],
            };
            assert_eq!(columns, wanted, "{}", statement.name());
        }
    }

    /// The series counts the bands file's `[production.load]` comment gives,
    /// which the S1 and S2 page counts rest on.
    #[test]
    fn the_production_load_has_the_family_sizes_the_bands_file_states() {
        let workload = crate::metrics_workload::load_workload(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../benchmarks/metrics/workload.json"
        ))
        .expect("the checked-in workload loads");
        let config = production();
        let profile = load_profile(&workload, &config).expect("a ci profile");
        let sizes: BTreeMap<&str, u64> = workload
            .families
            .iter()
            .map(|f| (f.name.as_str(), workload.family_series(&profile, f)))
            .collect();
        assert_eq!(sizes["metricsbench_gauge_cpu_percent"], 4000);
        assert_eq!(sizes["metricsbench_requests_total"], 3000);
        assert_eq!(sizes["metricsbench_request_duration_seconds"], 1500);
        assert_eq!(sizes["metricsbench_build_info"], 500);
        assert_eq!(config.load.s2_family, "metricsbench_requests_total");
        // S2: 3000 series at 600 rows a page is five pages, one more than a
        // walk reads.
        assert_eq!(
            sizes[config.load.s2_family.as_str()].div_ceil(u64::from(config.load.s2_max_rows)),
            5
        );
        assert_eq!(profile.scrape_interval_secs, 15);
        assert_eq!(profile.churn_basis_points_per_hour, 0);
    }

    #[test]
    fn a_page_breaks_where_ravel_query_sql_breaks_it() {
        use ravel_sql::ResumeValue::Int;
        let tuples = |values: &[i64]| -> Vec<Option<Vec<ravel_sql::ResumeValue>>> {
            values.iter().map(|v| Some(vec![Int(*v)])).collect()
        };
        // Total: the cap.
        assert_eq!(page_break(&tuples(&[5, 5, 5, 5]), 3, true), 3);
        // Grouped, the group closed at the cap: the cap.
        assert_eq!(page_break(&tuples(&[9, 8, 7, 6]), 3, false), 3);
        // Grouped, the third row's group runs on past the cap: held back.
        assert_eq!(page_break(&tuples(&[9, 8, 7, 7]), 3, false), 2);
        // One group larger than a page: nothing fits, no cursor.
        assert_eq!(page_break(&tuples(&[7, 7, 7, 7]), 3, false), 0);
        // Under the cap the last row's group may run on, so it is held back
        // too; nothing was cut, so the adapter mints no cursor either way.
        assert_eq!(page_break(&tuples(&[9, 8]), 3, false), 1);
    }
}
