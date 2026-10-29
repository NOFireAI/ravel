//! Figures, the report line format, the band table read from
//! `bench/agents/prereg.md`, the band check, and the price table.
//!
//! A figure is keyed `<scenario>.<temp>.<op>.<phase>.<metric>`. A report
//! line is
//!
//! ```text
//! agents-bench <key> value=<v> unit=<unit> [kind=<kind>]
//! ```
//!
//! with the value to three decimals. Every byte figure names its kind.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use super::Size;

/// The line prefix of a figure in the report.
pub const LINE_PREFIX: &str = "agents-bench ";
/// The line prefix of a derived dollar figure.
pub const USD_PREFIX: &str = "agents-bench-usd ";
/// The kind every agents and telemetry byte figure carries: object bodies at
/// the store client boundary, retries included, never compressed, not wire
/// bytes.
pub const OBJECT_BODY: &str = "object-body";
/// The kind of the heap figure: the largest sampled rise of live heap bytes.
pub const HEAP_LIVE_DELTA: &str = "heap-live-delta";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key {
    pub scenario: String,
    pub temp: String,
    pub op: String,
    pub phase: String,
    pub metric: String,
}

impl Key {
    pub fn new(scenario: &str, temp: &str, op: &str, phase: &str, metric: &str) -> Key {
        Key {
            scenario: scenario.to_string(),
            temp: temp.to_string(),
            op: op.to_string(),
            phase: phase.to_string(),
            metric: metric.to_string(),
        }
    }

    pub fn parse(text: &str) -> Option<Key> {
        let parts: Vec<&str> = text.split('.').collect();
        match parts.as_slice() {
            [scenario, temp, op, phase, metric] => {
                Some(Key::new(scenario, temp, op, phase, metric))
            }
            _ => None,
        }
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}.{}.{}",
            self.scenario, self.temp, self.op, self.phase, self.metric
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Figure {
    pub key: Key,
    /// Rounded to three decimals, as printed.
    pub value: f64,
    pub unit: String,
    pub kind: Option<String>,
}

/// `value` rounded to the three decimals the report prints.
pub fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

impl Figure {
    pub fn new(key: Key, value: f64, unit: &str, kind: Option<&str>) -> Figure {
        Figure {
            key,
            value: round3(value),
            unit: unit.to_string(),
            kind: kind.map(str::to_string),
        }
    }

    pub fn line(&self) -> String {
        let mut out = format!(
            "{LINE_PREFIX}{} value={:.3} unit={}",
            self.key, self.value, self.unit
        );
        if let Some(kind) = &self.kind {
            let _ = write!(out, " kind={kind}");
        }
        out
    }

    pub fn parse_line(line: &str) -> Option<Figure> {
        let rest = line.strip_prefix(LINE_PREFIX)?;
        let mut words = rest.split_whitespace();
        let key = Key::parse(words.next()?)?;
        let mut value = None;
        let mut unit = None;
        let mut kind = None;
        for word in words {
            let (name, v) = word.split_once('=')?;
            match name {
                "value" => value = Some(v.parse::<f64>().ok()?),
                "unit" => unit = Some(v.to_string()),
                "kind" => kind = Some(v.to_string()),
                _ => return None,
            }
        }
        Some(Figure {
            key,
            value: value?,
            unit: unit?,
            kind,
        })
    }

    pub fn is_bytes(&self) -> bool {
        self.key.metric.ends_with("_bytes")
    }
}

/// Every figure line of `figures`, in order, newline-terminated.
pub fn write_report(figures: &[Figure]) -> String {
    figures.iter().map(|f| f.line() + "\n").collect()
}

/// Every figure line of `text`; other lines are skipped.
pub fn parse_report(text: &str) -> Vec<Figure> {
    text.lines()
        .filter(|l| l.starts_with(LINE_PREFIX))
        .filter_map(Figure::parse_line)
        .collect()
}

/// One row of a band table.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// 1-based line in prereg.md.
    pub line: usize,
    pub size: String,
    pub scenario: String,
    pub temp: String,
    pub op: String,
    pub phase: String,
    pub metric: String,
    pub expected: f64,
    pub low: f64,
    pub high: f64,
}

const HEADER: [&str; 9] = [
    "size",
    "scenario",
    "temp",
    "op",
    "phase",
    "metric",
    "expected",
    "band",
    "reasoning",
];

fn cells(line: &str) -> Option<Vec<String>> {
    let inner = line.trim().strip_prefix('|')?.strip_suffix('|')?;
    Some(inner.split('|').map(|c| c.trim().to_string()).collect())
}

fn parse_band(text: &str) -> Option<(f64, f64)> {
    let inner = text.trim().strip_prefix('[')?.strip_suffix(']')?;
    let (low, high) = inner.split_once(',')?;
    Some((low.trim().parse().ok()?, high.trim().parse().ok()?))
}

fn pattern_matches(pattern: &str, value: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => value.starts_with(prefix),
        None => pattern == value,
    }
}

impl Row {
    fn is_wildcard(&self) -> bool {
        [
            &self.scenario,
            &self.temp,
            &self.op,
            &self.phase,
            &self.metric,
        ]
        .iter()
        .any(|p| p.contains('*'))
    }

    fn is_default(&self) -> bool {
        [
            &self.scenario,
            &self.temp,
            &self.op,
            &self.phase,
            &self.metric,
        ]
        .iter()
        .all(|p| p.as_str() == "*")
    }

    fn applies(&self, size: Size, heap: bool) -> bool {
        (self.size == "*" || self.size == size.name()) && (heap || self.phase != "heap")
    }

    fn matches(&self, key: &Key) -> bool {
        pattern_matches(&self.scenario, &key.scenario)
            && pattern_matches(&self.temp, &key.temp)
            && pattern_matches(&self.op, &key.op)
            && pattern_matches(&self.phase, &key.phase)
            && pattern_matches(&self.metric, &key.metric)
    }

    /// Whether `other` selects the same figures: a revision row that
    /// restates this one.
    fn same_selector(&self, other: &Row) -> bool {
        self.size == other.size
            && self.scenario == other.scenario
            && self.temp == other.temp
            && self.op == other.op
            && self.phase == other.phase
            && self.metric == other.metric
    }

    pub fn band(&self) -> String {
        format!("[{}, {}]", self.low, self.high)
    }
}

/// The band tables of prereg.md in document order: the original, then
/// each dated revision's. A later table takes precedence.
#[derive(Debug, Clone, Default)]
pub struct Bands {
    pub tables: Vec<Vec<Row>>,
}

impl Bands {
    pub fn parse(text: &str) -> Result<Bands, String> {
        let mut tables: Vec<Vec<Row>> = Vec::new();
        let mut in_table = false;
        for (index, line) in text.lines().enumerate() {
            let Some(row) = cells(line) else {
                in_table = false;
                continue;
            };
            if row.iter().map(String::as_str).eq(HEADER) {
                tables.push(Vec::new());
                in_table = true;
                continue;
            }
            if !in_table || row.iter().all(|c| c.chars().all(|ch| ch == '-')) {
                continue;
            }
            let [size, scenario, temp, op, phase, metric, expected, band, _] = row.as_slice()
            else {
                return Err(format!("prereg.md:{}: expected 9 cells", index + 1));
            };
            let expected = expected
                .parse::<f64>()
                .map_err(|_| format!("prereg.md:{}: expected is not a number", index + 1))?;
            let (low, high) = parse_band(band)
                .ok_or_else(|| format!("prereg.md:{}: band is not [low, high]", index + 1))?;
            if low > high {
                return Err(format!("prereg.md:{}: band low above high", index + 1));
            }
            if let Some(table) = tables.last_mut() {
                table.push(Row {
                    line: index + 1,
                    size: size.clone(),
                    scenario: scenario.clone(),
                    temp: temp.clone(),
                    op: op.clone(),
                    phase: phase.clone(),
                    metric: metric.clone(),
                    expected,
                    low,
                    high,
                });
            }
        }
        if tables.first().is_none_or(Vec::is_empty) {
            return Err("prereg.md holds no band table".to_string());
        }
        Ok(Bands { tables })
    }

    /// The row that decides `key`: the first match in the latest table that
    /// has one.
    pub fn decide(&self, key: &Key, size: Size, heap: bool) -> Option<&Row> {
        self.tables.iter().rev().find_map(|table| {
            table
                .iter()
                .find(|row| row.applies(size, heap) && row.matches(key))
        })
    }
}

/// Which rows a check holds a run to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope<'a> {
    /// A whole run: every exact row must match one figure, every wildcard
    /// row must decide one.
    All,
    /// One scenario's figures: exact rows for it must match one figure;
    /// wildcard rows are not held to deciding anything.
    Scenario(&'a str),
}

/// One figure's verdict.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub figure: Figure,
    pub row: Option<Row>,
    pub inside: bool,
}

/// The check of one run against its bands.
#[derive(Debug, Clone, Default)]
pub struct Checked {
    pub verdicts: Vec<Verdict>,
    pub failures: Vec<String>,
}

/// Check `figures` against `bands`. See the module docs and prereg.md.
pub fn check(
    figures: &[Figure],
    bands: &Bands,
    size: Size,
    heap: bool,
    scope: Scope<'_>,
) -> Checked {
    let mut out = Checked::default();
    let mut seen: BTreeMap<&Key, usize> = BTreeMap::new();
    for figure in figures {
        *seen.entry(&figure.key).or_default() += 1;
    }
    for (key, n) in &seen {
        if *n > 1 {
            out.failures
                .push(format!("{key}: printed {n} times, expected once"));
        }
    }
    let mut groups: BTreeMap<(&str, &str, &str), Vec<(&str, &str)>> = BTreeMap::new();
    for figure in figures {
        let k = &figure.key;
        groups
            .entry((&k.scenario, &k.temp, &k.op))
            .or_default()
            .push((&k.phase, &k.metric));
    }
    for ((scenario, temp, op), printed) in &groups {
        for (phase, metric) in expected_keys(op) {
            if !printed.contains(&(phase, metric)) {
                out.failures.push(format!(
                    "{scenario}.{temp}.{op}.{phase}.{metric}: expected figure missing"
                ));
            }
        }
    }
    let mut decided: Vec<BTreeMap<usize, usize>> = vec![BTreeMap::new(); bands.tables.len()];
    for figure in figures {
        if figure.is_bytes() && figure.kind.is_none() {
            out.failures
                .push(format!("{}: byte figure without kind", figure.key));
        }
        for (t, table) in bands.tables.iter().enumerate() {
            if let Some(row) = table
                .iter()
                .find(|row| row.applies(size, heap) && row.matches(&figure.key))
            {
                *decided[t].entry(row.line).or_default() += 1;
            }
        }
        let row = bands.decide(&figure.key, size, heap).cloned();
        let inside = row
            .as_ref()
            .is_some_and(|r| figure.value >= r.low && figure.value <= r.high);
        match &row {
            None => out.failures.push(format!("{}: no band row", figure.key)),
            Some(r) if !inside => out.failures.push(format!(
                "{}: {} outside {} (prereg.md:{}, expected {})",
                figure.key,
                figure.value,
                r.band(),
                r.line,
                r.expected
            )),
            Some(_) => {}
        }
        out.verdicts.push(Verdict {
            figure: figure.clone(),
            row,
            inside,
        });
    }
    for (t, table) in bands.tables.iter().enumerate() {
        for row in table.iter().filter(|r| r.applies(size, heap)) {
            if row.is_wildcard() {
                let restated = bands.tables[t + 1..]
                    .iter()
                    .flatten()
                    .any(|later| later.same_selector(row));
                let dead = !row.is_default()
                    && scope == Scope::All
                    && !restated
                    && !decided[t].contains_key(&row.line);
                if dead {
                    out.failures.push(format!(
                        "prereg.md:{}: band row decides no figure",
                        row.line
                    ));
                }
                continue;
            }
            if let Scope::Scenario(name) = scope
                && row.scenario != name
            {
                continue;
            }
            let matched = figures.iter().filter(|f| row.matches(&f.key)).count();
            if matched != 1 {
                out.failures.push(format!(
                    "prereg.md:{}: expected figure {}.{}.{}.{}.{} matched {matched} figures, expected 1",
                    row.line, row.scenario, row.temp, row.op, row.phase, row.metric
                ));
            }
        }
    }
    out
}

const AGENT_PHASES: [&str; 3] = ["resolve", "store", "publish"];
const AGENT_METRICS: [&str; 7] = [
    "get",
    "put",
    "cas",
    "list",
    "conflict",
    "body_in_bytes",
    "body_out_bytes",
];
const PERCENTILES: [&str; 3] = ["p50_ms", "p99_ms", "max_ms"];

/// Every `(phase, metric)` an operation of kind `op` prints. Listed here
/// rather than read from the meter, so a figure the meter stops printing
/// fails the check even where only a wildcard band row covers it.
fn expected_keys(op: &str) -> Vec<(&'static str, &'static str)> {
    let mut keys = vec![("ops", "count")];
    match op {
        "run" => return Vec::new(),
        "ingest" => {
            for metric in ["put", "get", "body_in_bytes", "body_out_bytes"] {
                keys.push(("telemetry", metric));
            }
        }
        _ => {
            for phase in AGENT_PHASES {
                for metric in AGENT_METRICS {
                    keys.push((phase, metric));
                }
            }
            keys.extend([
                ("index", "page_gets"),
                ("index", "planner_misses"),
                ("other", "delete"),
            ]);
        }
    }
    if op == "assign" {
        keys.extend([("latency", "delay_ms"), ("sweep", "passes")]);
    } else {
        keys.extend(PERCENTILES.map(|p| ("latency", p)));
    }
    keys
}

/// The price table of `bench/agents/prices.toml`, in dollars.
#[derive(Debug, Clone, PartialEq)]
pub struct Prices {
    pub label: String,
    pub date: String,
    pub source: String,
    pub get_per_1k: f64,
    pub put_per_1k: f64,
    pub cas_per_1k: f64,
    pub list_per_1k: f64,
    pub delete_per_1k: f64,
    pub transfer_per_gb: f64,
}

impl Prices {
    /// Read the flat `key = value` subset of TOML prices.toml uses.
    pub fn parse(text: &str) -> Result<Prices, String> {
        let mut values: BTreeMap<String, String> = BTreeMap::new();
        for (index, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (name, value) = line
                .split_once('=')
                .ok_or_else(|| format!("prices.toml:{}: not key = value", index + 1))?;
            let value = value.trim();
            let value = match value.strip_prefix('"') {
                Some(rest) => rest
                    .split_once('"')
                    .map(|(inner, _)| inner.to_string())
                    .ok_or_else(|| format!("prices.toml:{}: unterminated string", index + 1))?,
                None => value
                    .split('#')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_string(),
            };
            values.insert(name.trim().to_string(), value);
        }
        let text = |name: &str| {
            values
                .get(name)
                .cloned()
                .ok_or_else(|| format!("prices.toml: missing {name}"))
        };
        let number = |name: &str| -> Result<f64, String> {
            text(name)?
                .parse()
                .map_err(|_| format!("prices.toml: {name} is not a number"))
        };
        let prices = Prices {
            label: text("label")?,
            date: text("date")?,
            source: text("source")?,
            get_per_1k: number("get_per_1k")?,
            put_per_1k: number("put_per_1k")?,
            cas_per_1k: number("cas_per_1k")?,
            list_per_1k: number("list_per_1k")?,
            delete_per_1k: number("delete_per_1k")?,
            transfer_per_gb: number("transfer_per_gb")?,
        };
        if prices.label != "assumption" {
            return Err("prices.toml: label must be \"assumption\"".to_string());
        }
        Ok(prices)
    }
}

/// Dollars per million operations for each `(scenario, temp, op)` of
/// `figures`, from its per-op request and byte figures. Derived, never
/// checked against a band.
pub fn dollars(figures: &[Figure], prices: &Prices) -> Vec<(String, f64)> {
    let mut per_op: BTreeMap<String, f64> = BTreeMap::new();
    for f in figures {
        let price_per_1k = match (f.key.phase.as_str(), f.key.metric.as_str()) {
            ("resolve" | "store" | "publish" | "telemetry", "get") => prices.get_per_1k,
            ("resolve" | "store" | "publish" | "telemetry", "put") => prices.put_per_1k,
            ("resolve" | "store" | "publish", "cas") => prices.cas_per_1k,
            ("resolve" | "store" | "publish", "list") => prices.list_per_1k,
            ("other", "delete") => prices.delete_per_1k,
            (_, m) if m.ends_with("_bytes") && f.kind.as_deref() == Some(OBJECT_BODY) => {
                // Per GB, scaled to per 1,000 so one formula covers both.
                prices.transfer_per_gb * 1000.0 / 1e9
            }
            _ => continue,
        };
        if f.key.op == "run" || f.key.phase == "ops" {
            continue;
        }
        let name = format!("{}.{}.{}", f.key.scenario, f.key.temp, f.key.op);
        *per_op.entry(name).or_default() += f.value * price_per_1k / 1000.0 * 1e6;
    }
    per_op.into_iter().collect()
}

/// The derived dollar lines.
pub fn dollar_lines(figures: &[Figure], prices: &Prices) -> String {
    dollars(figures, prices)
        .into_iter()
        .map(|(name, usd)| {
            format!(
                "{USD_PREFIX}{name} usd_per_million_ops={usd:.4} label={} date={}\n",
                prices.label, prices.date
            )
        })
        .collect()
}
