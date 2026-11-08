//! MetricsBench Stage 1 holder counts (ADR-3694, issue #3968): for each corpus
//! statement with a positive equality matcher, how many of the data objects
//! the statement's fetch window reaches hold at least one series the
//! statement selects.
//!
//! The objects are the ones the folded snapshot names (L0 and L1), as the
//! catalog's resolve lists them. A statement's fetch window is the window
//! Ravel's engine resolves the snapshot over: for each metric selector, the
//! evaluation instant (an instant query) or the grid from `start` to the last
//! grid point at or before `end` (a range query), moved back by the
//! selector's offset (or pinned by its `@`), then padded back by the
//! selector's range (the 5 minute lookback for a vector selector, the matrix
//! range for a matrix selector, plus any enclosing subquery range); the
//! statement's window is the union over its selectors. An object is in the
//! window when its event time range overlaps it, both ends inclusive.
//!
//! The counts run without Ravel's router or key index: every listed object is
//! opened with `ravel_segment`'s public reader ([`open_from_full`],
//! [`decode_catalog_v5`], [`label_dict_strings`]), every series in its catalog
//! is tested against the statement's selectors, and its LABEL_DICT strings
//! are read as text. The figures a routed pass is judged against must not be
//! computed by the mechanism under test.
//!
//! A statement qualifies when one of its selectors carries a `label="value"`
//! matcher with a non-empty value (the metric name alone does not count). An
//! object holds the statement when it is in the window and one of its series
//! matches one of the statement's qualifying selectors in full: the metric
//! name and every matcher, `!=`, `=~` and `!~` included. `holders` is the
//! bound a series-level index could reach. It is not the label-value router's
//! bound: the router reads one object-wide set of label strings, so an object
//! holding `job="api"` on one series and `code="500"` on another is kept for
//! `{job="api",code="500"}` though no series matches.
//!
//! `value_holders` is the router's bound. Of the objects the resolve hands the
//! router (`named_in_window` when every selector names one metric, else
//! `segments_in_window`), it counts those whose LABEL_DICT holds, for at least
//! one selector, the selector's metric name and every value of its
//! `label="value"` matchers: the router probes each such value (`__name__`
//! included) and drops an object for a selector when one value is absent, and
//! prunes it only when every selector drops it. `expected_routed_prunes` is
//! the rest of those objects, the `segmentsPrunedByLabelIndex` a routed pass
//! reports; with one selector it is `named_in_window - value_holders`. An
//! object a `min_commit_token` adds outside that set is never routed, so it is
//! in neither figure. The router compares 8-byte hashes of the strings, so a
//! collision can keep an object `value_holders` leaves out; that shows as
//! fewer prunes than expected, never as more. Both are `None` for a statement
//! that is not `route_bound`.
//!
//! `segments_in_window` counts every object in the window, whatever metrics it
//! holds. It is not what an unrouted query fetches: Ravel's resolve also
//! prunes by name postings when every selector of the statement names the same
//! metric, so an unrouted pass fetches the objects in the window that hold
//! that metric (`named_in_window`), and every object in the window only when
//! the selectors name different metrics. The resolve then adds the L0 object
//! each `min_commit_token` names, whatever its metrics and event time (the
//! query lane passes one token per shard); `token_only_fetched` counts those
//! not already in the set. `unrouted_fetched_expected` is the sum, the
//! `segmentsFetched` an unrouted pass over a folded snapshot is expected to
//! report, and `None` when a token names no listed object, since the resolve
//! then reads an object this count did not see. ADR-3694 row 3's denominator
//! is the unrouted pass's `segmentsFetched` itself, never `segments_in_window`.
//!
//! A statement is `route_bound` when every one of its selectors carries a
//! qualifying matcher. A selector with only its metric name gives the
//! label-value router nothing to probe but the name, which the resolve's name
//! postings have mostly pruned by already; such a statement says why in
//! `route_bound_reason`, carries no `value_holders`, and is left out of any
//! comparison with a routed pass.

use std::collections::{BTreeMap, HashSet};

use ravel_commit::keys::parse_data_key;
use ravel_promql::{PlanAnchor, plan_selectors};
use ravel_query::log_series::log_metric_of;
use ravel_segment::{
    FooterLocation, ReaderLimits, decode_catalog_v5, label_dict_strings, open_from_full,
};
use ravel_types::CommitToken;
use serde::{Deserialize, Serialize};

use crate::metrics_query::QueryGrid;
use crate::promql_corpus::{CorpusEntry, EvalKind, SelectorMatch, selector_matches};

/// The method sentence written into `holders.json` and the run's stamp.
pub const HOLDERS_METHOD: &str = "the data objects the folded snapshot names (L0 and L1, from \
     the catalog's resolve) were downloaded and each one's full series catalog decoded with \
     ravel-segment's public reader (open_from_full + decode_catalog_v5), without the router or \
     key index; a statement qualifies when a selector has a label=\"value\" matcher with a \
     non-empty value; its fetch window is the union over its metric selectors of the evaluation \
     instant (instant) or the grid from start to the last grid point (range), moved back by the \
     selector's offset or pinned by its @, padded back by the selector's range (5m lookback for a \
     vector selector, the matrix range, plus enclosing subquery ranges), as Ravel's engine \
     resolves it; segments_in_window counts the objects whose event time range overlaps that \
     window (inclusive) whatever metrics they hold, named_in_window the ones among them with a \
     series of one of the statement's metric names, and holders the ones among them with a \
     series matching a qualifying selector in full (metric name and every matcher), the bound \
     of a series-level index, not of the label-value router; the resolved set is \
     named_in_window's objects when every selector names the same metric (Ravel's resolve then \
     prunes by that name's postings) and segments_in_window's otherwise; token_only_fetched \
     counts the L0 objects the query lane's min_commit_token values name (matched to listed keys \
     by shard, writer, epoch and seq) outside the resolved set, which the resolve adds whatever \
     their metrics and event time; unrouted_fetched_expected is the resolved set plus \
     token_only_fetched, null when a token names no listed object; value_holders, the \
     label-value router's bound, counts the resolved set's objects whose LABEL_DICT strings \
     (read with label_dict_strings, compared as text) hold, for at least one selector, its \
     metric name and every value of its label=\"value\" matchers, and expected_routed_prunes is \
     the resolved set's size minus value_holders (named_in_window - value_holders for one \
     selector), both null unless route_bound; route_bound is false when a selector has no \
     qualifying matcher, since the router then probes only that selector's metric name";

/// The footer section kind of LABEL_DICT (docs/segment-format.md).
const LABEL_DICT_KIND: u32 = 1;

/// One data object the snapshot names, as `ravel-cli catalog list --json`
/// prints it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedObject {
    pub key: String,
    /// `L0` or `L1`.
    pub level: String,
    pub min_event_ts_ns: i64,
    pub max_event_ts_ns: i64,
}

/// A closed nanosecond range: the window a statement's resolve reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct FetchWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl FetchWindow {
    fn overlaps(&self, object: &ListedObject) -> bool {
        object.min_event_ts_ns <= self.end_ns && self.start_ns <= object.max_event_ts_ns
    }
}

/// One qualifying statement's count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HolderCount {
    pub id: String,
    pub promql: String,
    pub eval: EvalKind,
    /// The qualifying selectors, as written.
    pub selectors: Vec<String>,
    /// Their positive equality matchers, `label="value"`, in selector order.
    pub positive_equalities: Vec<String>,
    /// The metric names of all the statement's selectors, each once, in
    /// selector order.
    pub metrics: Vec<String>,
    /// Every selector names the same metric, so Ravel's resolve prunes the
    /// snapshot by that name's postings.
    pub name_pruned: bool,
    pub fetch_window: FetchWindow,
    /// Data objects whose event time range overlaps `fetch_window`, whatever
    /// metrics they hold.
    pub segments_in_window: u64,
    /// Objects among those holding a series of one of `metrics`.
    pub named_in_window: u64,
    /// Listed objects a `min_commit_token` names that are outside the
    /// resolved set (`named_in_window`'s objects when `name_pruned`, else
    /// `segments_in_window`'s).
    pub token_only_fetched: u64,
    /// The `segmentsFetched` an unrouted pass over a folded snapshot reports:
    /// the resolved set plus `token_only_fetched`. `None` when a token names
    /// no listed object.
    pub unrouted_fetched_expected: Option<u64>,
    /// Objects in the window holding at least one series that matches a
    /// qualifying selector: a series-level index's bound, not the router's.
    pub holders: u64,
    /// Objects of the resolved set whose LABEL_DICT holds, for at least one
    /// selector, its metric name and every positive equality value: the
    /// label-value router's bound. `None` unless `route_bound`.
    pub value_holders: Option<u64>,
    /// The resolved set's size minus `value_holders`: the
    /// `segmentsPrunedByLabelIndex` a routed pass reports. `None` unless
    /// `route_bound`.
    pub expected_routed_prunes: Option<u64>,
    /// Every selector has a qualifying matcher, so the engine routes the
    /// statement and `value_holders` bounds what a routed pass opens.
    pub route_bound: bool,
    /// Why not, when `route_bound` is false.
    pub route_bound_reason: Option<String>,
}

/// A statement left out, and why.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HolderSkip {
    pub id: String,
    pub reason: String,
}

/// The `holders.json` document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HoldersReport {
    pub method: &'static str,
    pub profile: String,
    /// Data objects read, by level.
    pub objects_l0: u64,
    pub objects_l1: u64,
    /// Series decoded across them.
    pub series_total: u64,
    /// The `min_commit_token` values the query lane passes, as given.
    pub min_commit_tokens: u64,
    /// The listed objects those tokens name.
    pub token_objects: Vec<String>,
    /// Tokens naming no listed object, encoded; any one leaves every
    /// `unrouted_fetched_expected` `None`.
    pub tokens_unmatched: Vec<String>,
    pub statements: Vec<HolderCount>,
    /// Statements under the profile that have no qualifying selector, whose
    /// selectors the corpus reader refuses, or whose fetch window cannot be
    /// planned.
    pub skipped: Vec<HolderSkip>,
}

/// An object that could not be read, which voids the count.
#[derive(Debug, thiserror::Error)]
#[error("{key}: {detail}")]
pub struct HoldersError {
    pub key: String,
    pub detail: String,
}

/// The window Ravel's engine resolves the snapshot over for `promql` run as
/// `eval` on `grid` (see the module docs), or `None` when the statement has
/// no metric selector and so reads no metrics segment.
pub fn fetch_window(
    promql: &str,
    eval: EvalKind,
    grid: &QueryGrid,
) -> Result<Option<FetchWindow>, String> {
    let overflow = || "the window overflows nanoseconds".to_string();
    let ns = |ms: i64| ms.checked_mul(1_000_000).ok_or_else(overflow);
    let (eval_start_ms, eval_end_ms) = match eval {
        EvalKind::Instant => (grid.newest_ms, grid.newest_ms),
        EvalKind::Range => (grid.range_start_ms, grid.range_end_ms),
    };
    let plans = plan_selectors(promql, eval_start_ms, eval_end_ms).map_err(|e| e.to_string())?;
    let mut window: Option<FetchWindow> = None;
    for plan in plans
        .iter()
        .filter(|p| log_metric_of(&p.matchers).is_none())
    {
        let (start, end) = match (&plan.anchor, eval) {
            (PlanAnchor::Pinned(ts), _) => (*ts, *ts),
            (PlanAnchor::Window, EvalKind::Instant) => {
                let t = ns(grid.newest_ms)?
                    .checked_sub(plan.offset_ns)
                    .ok_or_else(overflow)?;
                (t, t)
            }
            (PlanAnchor::Window, EvalKind::Range) => {
                let start = ns(grid.range_start_ms)?;
                let end = ns(grid.range_end_ms)?;
                let step = ns(grid.range_step_ms)?;
                if step <= 0 || end < start {
                    return Err(format!(
                        "range grid {}..{} step {} ms is empty",
                        grid.range_start_ms, grid.range_end_ms, grid.range_step_ms
                    ));
                }
                let last = start
                    .checked_add(((end - start) / step) * step)
                    .ok_or_else(overflow)?;
                (
                    start.checked_sub(plan.offset_ns).ok_or_else(overflow)?,
                    last.checked_sub(plan.offset_ns).ok_or_else(overflow)?,
                )
            }
        };
        let w = FetchWindow {
            start_ns: start.checked_sub(plan.range_ns).ok_or_else(overflow)?,
            end_ns: end,
        };
        window = Some(match window {
            None => w,
            Some(u) => FetchWindow {
                start_ns: u.start_ns.min(w.start_ns),
                end_ns: u.end_ns.max(w.end_ns),
            },
        });
    }
    Ok(window)
}

/// A statement being counted: its qualifying selectors, its metric names, its
/// window, and the objects seen so far in the window, holding one of its
/// metrics, holding it, named only by a token, and holding its values.
struct Tally {
    entry: usize,
    selectors: Vec<SelectorMatch>,
    metrics: Vec<String>,
    route_unbound: Option<String>,
    window: FetchWindow,
    in_window: u64,
    named: u64,
    holders: u64,
    token_only: u64,
    value_holders: u64,
}

impl Tally {
    fn name_pruned(&self) -> bool {
        self.metrics.len() == 1
    }
}

/// Whether `token` names the L0 object at `key`. A key that does not parse as
/// an L0 data key names no token.
fn token_names(token: &CommitToken, key: &str) -> bool {
    parse_data_key(key).is_ok_and(|k| {
        k.shard == token.shard
            && k.writer_id == token.writer_id
            && k.epoch == token.epoch
            && k.seq == token.seq
    })
}

/// The LABEL_DICT strings of the object `bytes` whose footer `location`
/// describes.
fn label_dict(location: &FooterLocation, bytes: &[u8]) -> Result<HashSet<String>, String> {
    let section = location
        .footer
        .sections
        .iter()
        .find(|s| s.kind == LABEL_DICT_KIND)
        .ok_or_else(|| "object has no LABEL_DICT section".to_string())?;
    let stored = usize::try_from(section.offset)
        .ok()
        .zip(usize::try_from(section.len).ok())
        .and_then(|(offset, len)| bytes.get(offset..offset.checked_add(len)?))
        .ok_or_else(|| "LABEL_DICT range is outside the object".to_string())?;
    let strings = label_dict_strings(&location.footer, stored, ReaderLimits::default())
        .map_err(|e| format!("LABEL_DICT: {e}"))?;
    Ok(strings.into_iter().collect())
}

/// Counts holders over `objects`, each one listed object with its whole
/// bytes, for the entries of `corpus` that run under `profile` on `grid`.
/// `min_tokens` are the `min_commit_token` values the query lane passes.
pub fn count_holders<I>(
    corpus: &[CorpusEntry],
    profile: &str,
    grid: &QueryGrid,
    min_tokens: &[CommitToken],
    objects: I,
) -> Result<HoldersReport, HoldersError>
where
    I: IntoIterator<Item = Result<(ListedObject, Vec<u8>), HoldersError>>,
{
    let entries: Vec<&CorpusEntry> = corpus.iter().filter(|e| e.runs_under(profile)).collect();
    let mut tallies: Vec<Tally> = Vec::new();
    let mut skipped = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let skip = |reason: String| HolderSkip {
            id: entry.id.clone(),
            reason,
        };
        let selectors = match selector_matches(&entry.promql) {
            Ok(selectors) => selectors,
            Err(reason) => {
                skipped.push(skip(format!(
                    "the corpus reader refuses its selectors: {reason}"
                )));
                continue;
            }
        };
        let mut metrics: Vec<String> = Vec::new();
        for selector in &selectors {
            if !metrics.contains(&selector.metric) {
                metrics.push(selector.metric.clone());
            }
        }
        let route_unbound = selectors
            .iter()
            .find(|s| s.positive_equalities.is_empty())
            .map(|s| {
                format!(
                    "selector `{}` has no label=\"value\" matcher with a non-empty value, so the \
                     router probes only `{}` for it and keeps every object holding that name, \
                     which holders does not count",
                    s.text, s.metric
                )
            });
        let qualifying: Vec<SelectorMatch> = selectors
            .into_iter()
            .filter(|s| !s.positive_equalities.is_empty())
            .collect();
        if qualifying.is_empty() {
            skipped.push(skip(
                "no selector has a positive equality matcher".to_string(),
            ));
            continue;
        }
        match fetch_window(&entry.promql, entry.eval, grid) {
            Ok(Some(window)) => tallies.push(Tally {
                entry: index,
                selectors: qualifying,
                metrics,
                route_unbound,
                window,
                in_window: 0,
                named: 0,
                holders: 0,
                token_only: 0,
                value_holders: 0,
            }),
            Ok(None) => skipped.push(skip("no metric selector".to_string())),
            Err(reason) => skipped.push(skip(format!(
                "its fetch window cannot be planned: {reason}"
            ))),
        }
    }
    // metric name -> (tally, selector) pairs, so a series is tested only
    // against the selectors naming its metric.
    let mut by_metric: BTreeMap<String, Vec<(usize, usize)>> = BTreeMap::new();
    for (t, tally) in tallies.iter().enumerate() {
        for (s, selector) in tally.selectors.iter().enumerate() {
            by_metric
                .entry(selector.metric.clone())
                .or_default()
                .push((t, s));
        }
    }
    // metric name -> the tallies naming it in any selector.
    let mut by_name: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (t, tally) in tallies.iter().enumerate() {
        for metric in &tally.metrics {
            by_name.entry(metric.clone()).or_default().push(t);
        }
    }

    let limits = ReaderLimits::default();
    let (mut objects_l0, mut objects_l1, mut series_total) = (0u64, 0u64, 0u64);
    let mut in_window = vec![false; tallies.len()];
    let mut named = vec![false; tallies.len()];
    let mut held = vec![false; tallies.len()];
    let mut token_matched = vec![false; min_tokens.len()];
    let mut token_objects = Vec::new();
    for object in objects {
        let (listed, bytes) = object?;
        let fail = |detail: String| HoldersError {
            key: listed.key.clone(),
            detail,
        };
        let mut token_named = false;
        match listed.level.as_str() {
            "L0" => {
                objects_l0 += 1;
                for (matched, token) in token_matched.iter_mut().zip(min_tokens) {
                    if token_names(token, &listed.key) {
                        *matched = true;
                        token_named = true;
                    }
                }
            }
            "L1" => objects_l1 += 1,
            other => return Err(fail(format!("unknown level `{other}`"))),
        }
        if token_named {
            token_objects.push(listed.key.clone());
        }
        let location = open_from_full(&bytes, limits).map_err(|e| fail(e.to_string()))?;
        let catalog =
            decode_catalog_v5(&location.footer, &bytes, limits).map_err(|e| fail(e.to_string()))?;
        series_total += catalog.len() as u64;
        for (t, tally) in tallies.iter().enumerate() {
            in_window[t] = tally.window.overlaps(&listed);
        }
        named.iter_mut().for_each(|n| *n = false);
        held.iter_mut().for_each(|h| *h = false);
        for series in &catalog {
            let name = series.entry.labels.get("__name__");
            for &t in name
                .and_then(|name| by_name.get(name))
                .into_iter()
                .flatten()
            {
                named[t] |= in_window[t];
            }
            let Some(candidates) = name.and_then(|name| by_metric.get(name)) else {
                continue;
            };
            let labels: Vec<(String, String)> = series
                .entry
                .labels
                .iter()
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect();
            for &(t, s) in candidates {
                if in_window[t] && !held[t] && tallies[t].selectors[s].matches(&labels) {
                    held[t] = true;
                }
            }
        }
        let resolved: Vec<bool> = tallies
            .iter()
            .enumerate()
            .map(|(t, tally)| {
                if tally.name_pruned() {
                    named[t]
                } else {
                    in_window[t]
                }
            })
            .collect();
        let routed = |t: usize| resolved[t] && tallies[t].route_unbound.is_none();
        let dict = if (0..tallies.len()).any(routed) {
            label_dict(&location, &bytes).map_err(fail)?
        } else {
            HashSet::new()
        };
        for (t, tally) in tallies.iter_mut().enumerate() {
            tally.in_window += u64::from(in_window[t]);
            tally.named += u64::from(named[t]);
            tally.holders += u64::from(held[t]);
            tally.token_only += u64::from(token_named && !resolved[t]);
            if resolved[t] && tally.route_unbound.is_none() {
                let strings = &dict;
                let value_held = tally.selectors.iter().any(|s| {
                    strings.contains(&s.metric)
                        && s.positive_equalities
                            .iter()
                            .all(|(_, value)| strings.contains(value))
                });
                tally.value_holders += u64::from(value_held);
            }
        }
    }

    let tokens_unmatched: Vec<String> = min_tokens
        .iter()
        .zip(&token_matched)
        .filter(|(_, matched)| !**matched)
        .map(|(token, _)| token.encode())
        .collect();
    Ok(HoldersReport {
        method: HOLDERS_METHOD,
        profile: profile.to_string(),
        objects_l0,
        objects_l1,
        series_total,
        min_commit_tokens: min_tokens.len() as u64,
        token_objects,
        statements: tallies
            .into_iter()
            .map(|tally| {
                let entry = entries[tally.entry];
                let name_pruned = tally.name_pruned();
                let resolved = if name_pruned {
                    tally.named
                } else {
                    tally.in_window
                };
                let route_bound = tally.route_unbound.is_none();
                HolderCount {
                    id: entry.id.clone(),
                    promql: entry.promql.clone(),
                    eval: entry.eval,
                    positive_equalities: tally
                        .selectors
                        .iter()
                        .flat_map(|s| &s.positive_equalities)
                        .map(|(label, value)| format!("{label}=\"{value}\""))
                        .collect(),
                    selectors: tally.selectors.into_iter().map(|s| s.text).collect(),
                    metrics: tally.metrics,
                    name_pruned,
                    fetch_window: tally.window,
                    segments_in_window: tally.in_window,
                    named_in_window: tally.named,
                    token_only_fetched: tally.token_only,
                    unrouted_fetched_expected: tokens_unmatched
                        .is_empty()
                        .then_some(resolved + tally.token_only),
                    holders: tally.holders,
                    value_holders: route_bound.then_some(tally.value_holders),
                    expected_routed_prunes: route_bound
                        .then(|| resolved.saturating_sub(tally.value_holders)),
                    route_bound,
                    route_bound_reason: tally.route_unbound,
                }
            })
            .collect(),
        tokens_unmatched,
        skipped,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::segment_support::build_segment;
    use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

    const MIN: i64 = 60_000_000_000;

    /// The grid: instants at minute 100, ranges from minute 40 to minute 100
    /// by 7 minutes (last grid point minute 96).
    fn grid() -> QueryGrid {
        QueryGrid {
            newest_ms: 100 * 60_000,
            range_start_ms: 40 * 60_000,
            range_end_ms: 100 * 60_000,
            range_step_ms: 7 * 60_000,
        }
    }

    fn object(series: &[&[(&str, &str)]]) -> Vec<u8> {
        let tenant = TenantId::new("t");
        let raw = series
            .iter()
            .map(|pairs| {
                let labels = LabelSet::new(
                    pairs
                        .iter()
                        .map(|(name, value)| Label {
                            name: name.to_string(),
                            value: value.to_string(),
                        })
                        .collect(),
                )
                .expect("labels");
                let metric = labels.get("__name__").expect("metric").to_string();
                let id = SeriesId::compute(&tenant, &metric, &labels).expect("id");
                (
                    id,
                    labels,
                    vec![Sample {
                        ts_ns: 1_000_000_000,
                        value: 1.0,
                    }],
                )
            })
            .collect();
        build_segment(raw).bytes.to_vec()
    }

    /// An object listed as spanning minutes `from..=to`.
    fn listed(key: &str, level: &str, from: i64, to: i64) -> ListedObject {
        ListedObject {
            key: key.to_string(),
            level: level.to_string(),
            min_event_ts_ns: from * MIN,
            max_event_ts_ns: to * MIN,
        }
    }

    fn entry(id: &str, promql: &str) -> CorpusEntry {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "promql": promql,
            "constructs": [],
        }))
        .expect("entry")
    }

    fn range_entry(id: &str, promql: &str) -> CorpusEntry {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "promql": promql,
            "constructs": [],
            "eval": "range",
        }))
        .expect("entry")
    }

    fn window(promql: &str, eval: EvalKind) -> (i64, i64) {
        let w = fetch_window(promql, eval, &grid())
            .expect("plans")
            .expect("a metric selector");
        assert_eq!(w.start_ns % MIN, 0);
        assert_eq!(w.end_ns % MIN, 0);
        (w.start_ns / MIN, w.end_ns / MIN)
    }

    /// The window pads back by the 5 minute lookback for a vector selector
    /// and by the matrix range for a matrix selector, moves back by an
    /// offset, ends a range query at its last grid point (minute 96, not the
    /// requested end 100), and is the union over a statement's selectors.
    #[test]
    fn the_fetch_window_follows_the_engine_resolve() {
        assert_eq!(window("m", EvalKind::Instant), (95, 100));
        assert_eq!(window("rate(m[30m])", EvalKind::Instant), (70, 100));
        assert_eq!(window("m offset 10m", EvalKind::Instant), (85, 90));
        assert_eq!(window("rate(m[30m])", EvalKind::Range), (10, 96));
        assert_eq!(
            window("m + rate(n[1h] offset 20m)", EvalKind::Instant),
            (20, 100)
        );
        assert_eq!(fetch_window("1 + 1", EvalKind::Instant, &grid()), Ok(None));
    }

    /// Holders are objects in the window with a fully matching series:
    /// object `a` holds `job="api"` with `code!="500"`, `b` has `job="api"`
    /// only with code 500 (so a positive-equality-only test would wrongly
    /// count it), `c` has a different job, and `old` holds every statement
    /// but ends before any window starts, so it is neither in the window nor
    /// a holder. A statement with no positive equality is skipped.
    #[test]
    fn holders_count_objects_in_the_window_with_a_fully_matching_series() {
        let api_200: &[(&str, &str)] = &[
            ("__name__", "http_requests"),
            ("job", "api"),
            ("code", "200"),
        ];
        let objects = vec![
            (
                listed("a", "L1", 90, 99),
                object(&[
                    api_200,
                    &[
                        ("__name__", "http_requests"),
                        ("job", "web"),
                        ("code", "200"),
                    ],
                ]),
            ),
            (
                listed("b", "L0", 99, 101),
                object(&[&[
                    ("__name__", "http_requests"),
                    ("job", "api"),
                    ("code", "500"),
                ]]),
            ),
            (
                listed("c", "L1", 80, 96),
                object(&[&[
                    ("__name__", "http_requests"),
                    ("job", "db"),
                    ("code", "200"),
                ]]),
            ),
            (listed("old", "L1", 0, 9), object(&[api_200])),
        ];
        let corpus = vec![
            entry(
                "eq_and_ne",
                r#"sum(rate(http_requests{job="api",code!="500"}[5m]))"#,
            ),
            entry("eq_only", r#"http_requests{job="api"}"#),
            range_entry("eq_range", r#"rate(http_requests{job="api"}[1h])"#),
            entry("name_only", "sum(http_requests)"),
            entry("empty_eq", r#"http_requests{job=""}"#),
        ];
        let report = count_holders(&corpus, "ci", &grid(), &[], objects.into_iter().map(Ok))
            .expect("counts");
        assert_eq!((report.objects_l0, report.objects_l1), (1, 3));
        assert_eq!(report.series_total, 5);
        let counts: Vec<(&str, u64, u64)> = report
            .statements
            .iter()
            .map(|s| (s.id.as_str(), s.segments_in_window, s.holders))
            .collect();
        // eq_range's window is minutes -20..=96: `old` (0..=9) is in it and
        // holds it, while `b` (99..=101) starts after the last grid point.
        assert_eq!(
            counts,
            [("eq_and_ne", 3, 1), ("eq_only", 3, 2), ("eq_range", 3, 2)]
        );
        assert_eq!(
            report.statements[0].positive_equalities,
            vec![r#"job="api""#.to_string()]
        );
        for statement in &report.statements {
            assert!(statement.holders <= statement.segments_in_window);
        }
        let skipped: Vec<&str> = report.skipped.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(skipped, ["name_only", "empty_eq"]);
    }

    /// `named_in_window` leaves out an object in the window that holds none
    /// of the statement's metrics (`other`), and is 0 for a statement naming
    /// a metric no object holds; `unrouted_fetched_expected` follows it when
    /// every selector names one metric and is `segments_in_window` when they
    /// name two. A selector with no qualifying matcher makes the statement
    /// not route-bound, and says which selector.
    #[test]
    fn named_in_window_counts_the_objects_holding_the_statements_metrics() {
        let objects = vec![
            (
                listed("a", "L1", 90, 99),
                object(&[&[("__name__", "http_requests"), ("job", "api")]]),
            ),
            (
                listed("other", "L0", 90, 99),
                object(&[&[("__name__", "other_metric"), ("job", "api")]]),
            ),
            (
                listed("old_other", "L1", 0, 9),
                object(&[&[("__name__", "other_metric"), ("job", "api")]]),
            ),
        ];
        let corpus = vec![
            entry("eq_only", r#"http_requests{job="api"}"#),
            entry("absent", r#"absent_metric{job="api"}"#),
            entry(
                "two_metrics",
                r#"http_requests{job="api"} / other_metric{job="api"}"#,
            ),
            entry(
                "ratio_unbound",
                r#"sum(http_requests{job="api"}) / sum(http_requests)"#,
            ),
        ];
        let report = count_holders(&corpus, "ci", &grid(), &[], objects.into_iter().map(Ok))
            .expect("counts");
        type Counts<'a> = (&'a str, u64, u64, bool, Option<u64>, u64);
        let counts: Vec<Counts> = report
            .statements
            .iter()
            .map(|s| {
                (
                    s.id.as_str(),
                    s.segments_in_window,
                    s.named_in_window,
                    s.name_pruned,
                    s.unrouted_fetched_expected,
                    s.holders,
                )
            })
            .collect();
        assert_eq!(
            counts,
            [
                ("eq_only", 2, 1, true, Some(1), 1),
                ("absent", 2, 0, true, Some(0), 0),
                ("two_metrics", 2, 2, false, Some(2), 2),
                ("ratio_unbound", 2, 1, true, Some(1), 1),
            ]
        );
        assert_eq!(
            report.statements[2].metrics,
            ["http_requests", "other_metric"]
        );
        let bound: Vec<(&str, bool, Option<&str>)> = report
            .statements
            .iter()
            .map(|s| {
                (
                    s.id.as_str(),
                    s.route_bound,
                    s.route_bound_reason.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            bound,
            [
                ("eq_only", true, None),
                ("absent", true, None),
                ("two_metrics", true, None),
                (
                    "ratio_unbound",
                    false,
                    Some(
                        "selector `http_requests` has no label=\"value\" matcher with a \
                         non-empty value, so the router probes only `http_requests` for it \
                         and keeps every object holding that name, which holders does not \
                         count"
                    )
                ),
            ]
        );
    }

    /// One data object of the folded fixture: its metric, each series' labels
    /// besides `__name__`, and the event time of each series' one sample.
    struct FixtureObject {
        metric: &'static str,
        series: &'static [&'static [(&'static str, &'static str)]],
        ts_ns: i64,
    }

    const API: &[&[(&str, &str)]] = &[&[("job", "api")]];

    /// The four objects of the window tests: `a` and `other` in the window,
    /// `old_a` and `old_other` before it.
    fn window_specs() -> [FixtureObject; 4] {
        [
            FixtureObject {
                metric: "http_requests",
                series: API,
                ts_ns: fixture_newest_ns() - MIN,
            },
            FixtureObject {
                metric: "other_metric",
                series: API,
                ts_ns: fixture_newest_ns() - MIN,
            },
            FixtureObject {
                metric: "http_requests",
                series: API,
                ts_ns: fixture_old_ns(),
            },
            FixtureObject {
                metric: "other_metric",
                series: API,
                ts_ns: fixture_old_ns(),
            },
        ]
    }

    fn window_corpus() -> Vec<CorpusEntry> {
        vec![
            entry("eq_only", r#"http_requests{job="api"}"#),
            entry("absent", r#"absent_metric{job="api"}"#),
            entry(
                "two_metrics",
                r#"http_requests{job="api"} / other_metric{job="api"}"#,
            ),
        ]
    }

    /// The window check's two figures, side by side on a real folded
    /// snapshot: Ravel's engine, run unrouted with no `min_commit_token` over
    /// the same objects the count reads, fetches exactly
    /// `unrouted_fetched_expected` for each statement. `other` is in the
    /// window but outside `http_requests`' name postings, `absent_metric` is
    /// in no object's postings, and the out-of-window objects show that
    /// `segments_in_window` is not fetched + pruned.
    #[tokio::test]
    async fn unrouted_fetched_expected_is_what_the_engine_fetches() {
        use std::time::Duration;

        let fixture =
            folded_fixture(&window_specs(), ravel_catalog::CatalogConfig::default()).await;
        let grid = QueryGrid {
            newest_ms: fixture.newest_ns / 1_000_000,
            ..grid()
        };
        let report = count_holders(
            &window_corpus(),
            "ci",
            &grid,
            &[],
            fixture.objects.into_iter().map(Ok),
        )
        .expect("counts");
        let mut figures = Vec::new();
        for statement in &report.statements {
            let (_, stats) = fixture
                .engine
                .instant_with_stats(
                    fixture.tenant,
                    &statement.promql,
                    grid.newest_ms,
                    &[],
                    fixture.now,
                    Duration::from_secs(5),
                )
                .await
                .expect("query");
            figures.push((
                statement.id.as_str(),
                statement.segments_in_window,
                statement.unrouted_fetched_expected,
                stats.segments_fetched,
                stats.segments_pruned,
            ));
        }
        assert_eq!(
            figures,
            [
                ("eq_only", 2, Some(1), 1, 2),
                ("absent", 2, Some(0), 0, 4),
                ("two_metrics", 2, Some(2), 2, 0),
            ]
        );
    }

    /// A folded snapshot on a `MemoryStore` and the engine over it: one L0
    /// object per element of `specs` (shard 0, its own writer, hour 5000), as
    /// `catalog list --json` names it (its real data key), and each one's
    /// commit token, in `specs` order.
    struct Fixture {
        engine: ravel_query::QueryEngine,
        tenant: ravel_types::TenantHash,
        now: i64,
        newest_ns: i64,
        objects: Vec<(ListedObject, Vec<u8>)>,
        tokens: Vec<ravel_types::CommitToken>,
    }

    const FIXTURE_HOUR_NS: i64 = 3_600_000_000_000;
    const FIXTURE_HOUR: u32 = 5_000;

    fn fixture_newest_ns() -> i64 {
        i64::from(FIXTURE_HOUR) * FIXTURE_HOUR_NS + 50 * MIN
    }

    fn fixture_old_ns() -> i64 {
        i64::from(FIXTURE_HOUR) * FIXTURE_HOUR_NS + 10 * MIN
    }

    async fn folded_fixture(
        specs: &[FixtureObject],
        config: ravel_catalog::CatalogConfig,
    ) -> Fixture {
        use std::sync::Arc;

        use ravel_catalog::{
            Catalog, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
            DEFAULT_MAX_FLUSH_LIFETIME_NS,
        };
        use ravel_commit::publish::RetryPolicy;
        use ravel_commit::record::NewCommitRecord;
        use ravel_commit::{keys, publish, record};
        use ravel_object_store::ObjectStoreBackend;
        use ravel_object_store::memory::MemoryStore;
        use ravel_query::{EngineConfig, QueryEngine};
        use ravel_segment::{
            IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7,
        };
        use ravel_types::{CommitToken, Signal};

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("holders");
        let th = tenant.hash();
        let mut objects = Vec::new();
        let mut tokens = Vec::new();
        for (seq, spec) in specs.iter().enumerate() {
            let seq = seq as u64 + 1;
            let series = spec
                .series
                .iter()
                .map(|series| {
                    let mut pairs = vec![Label {
                        name: "__name__".to_string(),
                        value: spec.metric.to_string(),
                    }];
                    pairs.extend(series.iter().map(|(name, value)| Label {
                        name: name.to_string(),
                        value: value.to_string(),
                    }));
                    let labels = LabelSet::new(pairs).expect("labels");
                    SeriesInput {
                        series_id: SeriesId::compute(&tenant, spec.metric, &labels).expect("id"),
                        labels,
                        samples: vec![Sample {
                            ts_ns: spec.ts_ns,
                            value: 1.0,
                        }],
                    }
                })
                .collect();
            let writer_id = uuid::Uuid::new_v4();
            let written = SegmentWriter::write(
                series,
                SegmentIdentity {
                    tenant_hash: th.0,
                    shard: 0,
                    writer_id: writer_id.to_string(),
                    writer_epoch: 1,
                    writer_seq: seq,
                },
                IngestBounds {
                    min_ingest_ts_ns: 0,
                    max_ingest_ts_ns: 0,
                },
            )
            .expect("segment");
            let rec = record::build(NewCommitRecord {
                tenant_hash: th,
                signal: Signal::Metrics,
                shard: 0,
                writer_id,
                writer_epoch: 1,
                writer_seq: seq,
                object_size: written.bytes.len() as u64,
                content_hash: written.summary.blake3,
                sample_count: written.summary.sample_count,
                series_count: written.summary.series_count,
                min_event_ts_ns: written.summary.min_event_ts_ns,
                max_event_ts_ns: written.summary.max_event_ts_ns,
                min_ingest_ts_ns: written.summary.min_event_ts_ns,
                max_ingest_ts_ns: written.summary.max_event_ts_ns,
                segment_format_version: u32::from(VERSION_V7),
                created_unix_ns: 0,
                ingest_hour_bucket: FIXTURE_HOUR,
            })
            .expect("record");
            let data_key = keys::reconstruct_data_key(&rec).expect("data key");
            publish::put_data_object(store.as_ref(), &data_key, written.bytes.clone())
                .await
                .expect("put");
            publish::publish(store.as_ref(), &rec, &RetryPolicy::default())
                .await
                .expect("publish");
            objects.push((
                ListedObject {
                    key: data_key,
                    level: "L0".to_string(),
                    min_event_ts_ns: written.summary.min_event_ts_ns,
                    max_event_ts_ns: written.summary.max_event_ts_ns,
                },
                written.bytes.to_vec(),
            ));
            tokens.push(CommitToken {
                shard: 0,
                writer_id,
                epoch: 1,
                seq,
                ingest_hour_bucket: FIXTURE_HOUR,
            });
        }
        let now = (i64::from(FIXTURE_HOUR) + 1) * FIXTURE_HOUR_NS
            + DEFAULT_MAX_FLUSH_LIFETIME_NS
            + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
            + DEFAULT_FOLD_SAFETY_MARGIN_NS;
        let catalog = Catalog::new(Arc::clone(&store), config).expect("catalog");
        let folded = catalog
            .fold(&th, Signal::Metrics, uuid::Uuid::new_v4(), now, &[], None)
            .await
            .expect("fold");
        assert!(folded.postings_built, "the resolve prunes by name");
        Fixture {
            engine: QueryEngine::new(Arc::new(catalog), store, EngineConfig::default()),
            tenant: th,
            now,
            newest_ns: fixture_newest_ns(),
            objects,
            tokens,
        }
    }

    /// The resolve adds the object each `min_commit_token` names whatever
    /// its metrics and event time: the token of `old_other` (outside the
    /// window and outside `http_requests`' and `absent_metric`'s postings)
    /// makes the unrouted pass fetch it for every statement, on top of the
    /// objects in the window holding the statement's metric. The token of `a`
    /// adds nothing for `eq_only` and `two_metrics`, which fetch `a` anyway,
    /// and adds `a` for `absent`, whose postings prune every object.
    #[tokio::test]
    async fn the_min_commit_token_objects_are_fetched_whatever_their_metric() {
        use std::time::Duration;

        let fixture =
            folded_fixture(&window_specs(), ravel_catalog::CatalogConfig::default()).await;
        let tokens = [fixture.tokens[0].clone(), fixture.tokens[3].clone()];
        let token_keys = [
            fixture.objects[0].0.key.clone(),
            fixture.objects[3].0.key.clone(),
        ];
        let grid = QueryGrid {
            newest_ms: fixture.newest_ns / 1_000_000,
            ..grid()
        };
        let report = count_holders(
            &window_corpus(),
            "ci",
            &grid,
            &tokens,
            fixture.objects.into_iter().map(Ok),
        )
        .expect("counts");
        assert_eq!(report.min_commit_tokens, 2);
        assert_eq!(report.token_objects, token_keys);
        assert!(report.tokens_unmatched.is_empty());
        let mut figures = Vec::new();
        for statement in &report.statements {
            let (_, stats) = fixture
                .engine
                .instant_with_stats(
                    fixture.tenant,
                    &statement.promql,
                    grid.newest_ms,
                    &tokens,
                    fixture.now,
                    Duration::from_secs(5),
                )
                .await
                .expect("query");
            figures.push((
                statement.id.as_str(),
                statement.token_only_fetched,
                statement.unrouted_fetched_expected,
                stats.segments_fetched,
            ));
        }
        assert_eq!(
            figures,
            [
                ("eq_only", 1, Some(2), 2),
                ("absent", 2, Some(2), 2),
                ("two_metrics", 1, Some(3), 3),
            ]
        );
    }

    /// A token naming no listed object leaves every statement's
    /// `unrouted_fetched_expected` unknown, since the resolve reads an object
    /// the count did not see, and is reported encoded.
    #[tokio::test]
    async fn an_unmatched_token_leaves_the_unrouted_expectation_unknown() {
        let fixture =
            folded_fixture(&window_specs(), ravel_catalog::CatalogConfig::default()).await;
        let stray = CommitToken {
            seq: 99,
            ..fixture.tokens[0].clone()
        };
        let report = count_holders(
            &window_corpus(),
            "ci",
            &grid(),
            &[fixture.tokens[0].clone(), stray.clone()],
            fixture.objects.into_iter().map(Ok),
        )
        .expect("counts");
        assert_eq!(report.tokens_unmatched, [stray.encode()]);
        for statement in &report.statements {
            assert_eq!(
                statement.unrouted_fetched_expected, None,
                "{}",
                statement.id
            );
        }
    }

    /// `value_holders` and `expected_routed_prunes` against a routed pass on
    /// a snapshot folded with the metrics label index. `c` holds `job="api"`
    /// and `code="500"` on different series, so it holds no series of
    /// `cross` (holders 0) yet its LABEL_DICT holds both values and the
    /// router keeps it (value_holders 1). For `two_metrics` the resolve hands
    /// the router every object in the window; `a` lacks `job="web"` for the
    /// first selector and `other_metric` for the second, so it is the one
    /// pruned.
    #[tokio::test]
    async fn expected_routed_prunes_is_what_the_router_prunes() {
        use std::time::Duration;

        const API_200: &[&[(&str, &str)]] = &[&[("job", "api"), ("code", "200")]];
        const WEB_500: &[&[(&str, &str)]] = &[&[("job", "web"), ("code", "500")]];
        const SPLIT: &[&[(&str, &str)]] = &[
            &[("job", "api"), ("code", "200")],
            &[("job", "web"), ("code", "500")],
        ];
        let ts_ns = fixture_newest_ns() - MIN;
        let fixture = folded_fixture(
            &[
                FixtureObject {
                    metric: "http_requests",
                    series: API_200,
                    ts_ns,
                },
                FixtureObject {
                    metric: "http_requests",
                    series: WEB_500,
                    ts_ns,
                },
                FixtureObject {
                    metric: "http_requests",
                    series: SPLIT,
                    ts_ns,
                },
                FixtureObject {
                    metric: "other_metric",
                    series: API,
                    ts_ns,
                },
            ],
            ravel_catalog::CatalogConfig {
                build_key_index_leaves: true,
                metrics_label_index: true,
                metrics_label_index_max_ratio_ppm: 100_000_000,
                ..ravel_catalog::CatalogConfig::default()
            },
        )
        .await;
        let corpus = vec![
            entry("eq_only", r#"http_requests{job="api"}"#),
            entry("cross", r#"http_requests{job="api",code="500"}"#),
            entry(
                "two_metrics",
                r#"http_requests{job="web"} / other_metric{job="api"}"#,
            ),
            entry(
                "ratio_unbound",
                r#"sum(http_requests{job="api"}) / sum(http_requests)"#,
            ),
        ];
        let grid = QueryGrid {
            newest_ms: fixture.newest_ns / 1_000_000,
            ..grid()
        };
        let report = count_holders(
            &corpus,
            "ci",
            &grid,
            &[],
            fixture.objects.into_iter().map(Ok),
        )
        .expect("counts");
        let mut figures = Vec::new();
        for statement in &report.statements {
            let (_, stats) = fixture
                .engine
                .instant_with_stats(
                    fixture.tenant,
                    &statement.promql,
                    grid.newest_ms,
                    &[],
                    fixture.now,
                    Duration::from_secs(5),
                )
                .await
                .expect("query");
            figures.push((
                statement.id.as_str(),
                statement.holders,
                statement.value_holders,
                statement.expected_routed_prunes,
                stats.segments_pruned_by_label_index,
            ));
        }
        assert_eq!(
            figures,
            [
                ("eq_only", 2, Some(2), Some(1), 1),
                ("cross", 0, Some(1), Some(2), 2),
                ("two_metrics", 3, Some(3), Some(1), 1),
                ("ratio_unbound", 2, None, None, 0),
            ]
        );
    }

    /// An object that does not decode voids the count rather than being
    /// skipped.
    #[test]
    fn an_unreadable_object_is_an_error() {
        let corpus = vec![entry("eq_only", r#"http_requests{job="api"}"#)];
        let error = count_holders(
            &corpus,
            "ci",
            &grid(),
            &[],
            [Ok((
                listed("broken", "L0", 90, 99),
                b"not an object".to_vec(),
            ))],
        )
        .expect_err("refused");
        assert_eq!(error.key, "broken");
    }
}
