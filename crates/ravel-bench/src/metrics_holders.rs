//! MetricsBench Stage 1 holder counts (ADR-3694, issue #3968): for each corpus
//! statement with a positive equality matcher, how many of a tenant's data
//! objects hold at least one series the statement selects.
//!
//! The count is taken without Ravel's router or key index: every data object
//! is opened with `ravel_segment`'s public reader ([`open_from_full`],
//! [`decode_catalog_v5`]) and every series in its catalog is tested against
//! the statement's selectors. It is the denominator a routed query's
//! `segmentsFetched` is judged against, so it must not be computed by the
//! mechanism under test.
//!
//! A statement qualifies when one of its selectors carries a `label="value"`
//! matcher with a non-empty value (the metric name alone does not count). An
//! object holds the statement when one of its series matches one of the
//! statement's qualifying selectors in full: the metric name and every
//! matcher, `!=`, `=~` and `!~` included. The query's time window is not
//! applied, so an object whose matching series lie outside the window still
//! counts.

use std::collections::BTreeMap;

use ravel_segment::{ReaderLimits, decode_catalog_v5, open_from_full};
use serde::Serialize;

use crate::promql_corpus::{CorpusEntry, SelectorMatch, selector_matches};

/// The method sentence written into `holders.json` and the run's stamp.
pub const HOLDERS_METHOD: &str = "every data object (t/<tenant>/m/l0/*.rseg) was downloaded \
     and its full series catalog decoded with ravel-segment's public reader \
     (open_from_full + decode_catalog_v5), without the router or key index; a statement \
     qualifies when a selector has a label=\"value\" matcher with a non-empty value, and an \
     object holds it when one of its series matches a qualifying selector in full (metric \
     name and every matcher); the query window is not applied";

/// One qualifying statement's count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HolderCount {
    pub id: String,
    pub promql: String,
    /// The qualifying selectors, as written.
    pub selectors: Vec<String>,
    /// Their positive equality matchers, `label="value"`, in selector order.
    pub positive_equalities: Vec<String>,
    /// Data objects holding at least one matching series.
    pub holders: u64,
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
    /// Data objects read.
    pub objects_total: u64,
    /// Series decoded across them.
    pub series_total: u64,
    pub statements: Vec<HolderCount>,
    /// Statements under the profile that have no qualifying selector, or
    /// whose selectors the corpus reader refuses.
    pub skipped: Vec<HolderSkip>,
}

/// An object that could not be read, which voids the count.
#[derive(Debug, thiserror::Error)]
#[error("{key}: {detail}")]
pub struct HoldersError {
    pub key: String,
    pub detail: String,
}

/// A statement being counted: its qualifying selectors and the objects seen
/// so far that hold it.
struct Tally {
    entry: usize,
    selectors: Vec<SelectorMatch>,
    holders: u64,
}

/// Counts holders over `objects`, each `(key, bytes)` one whole data object,
/// for the entries of `corpus` that run under `profile`.
pub fn count_holders<I>(
    corpus: &[CorpusEntry],
    profile: &str,
    objects: I,
) -> Result<HoldersReport, HoldersError>
where
    I: IntoIterator<Item = Result<(String, Vec<u8>), HoldersError>>,
{
    let entries: Vec<&CorpusEntry> = corpus.iter().filter(|e| e.runs_under(profile)).collect();
    let mut tallies: Vec<Tally> = Vec::new();
    let mut skipped = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        match selector_matches(&entry.promql) {
            Ok(selectors) => {
                let qualifying: Vec<SelectorMatch> = selectors
                    .into_iter()
                    .filter(|s| !s.positive_equalities.is_empty())
                    .collect();
                if qualifying.is_empty() {
                    skipped.push(HolderSkip {
                        id: entry.id.clone(),
                        reason: "no selector has a positive equality matcher".to_string(),
                    });
                } else {
                    tallies.push(Tally {
                        entry: index,
                        selectors: qualifying,
                        holders: 0,
                    });
                }
            }
            Err(reason) => skipped.push(HolderSkip {
                id: entry.id.clone(),
                reason: format!("the corpus reader refuses its selectors: {reason}"),
            }),
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

    let limits = ReaderLimits::default();
    let mut objects_total = 0u64;
    let mut series_total = 0u64;
    let mut held = vec![false; tallies.len()];
    for object in objects {
        let (key, bytes) = object?;
        let fail = |detail: String| HoldersError {
            key: key.clone(),
            detail,
        };
        let location = open_from_full(&bytes, limits).map_err(|e| fail(e.to_string()))?;
        let catalog =
            decode_catalog_v5(&location.footer, &bytes, limits).map_err(|e| fail(e.to_string()))?;
        objects_total += 1;
        series_total += catalog.len() as u64;
        held.iter_mut().for_each(|h| *h = false);
        for series in &catalog {
            let Some(candidates) = series
                .entry
                .labels
                .get("__name__")
                .and_then(|name| by_metric.get(name))
            else {
                continue;
            };
            let labels: Vec<(String, String)> = series
                .entry
                .labels
                .iter()
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect();
            for &(t, s) in candidates {
                if !held[t] && tallies[t].selectors[s].matches(&labels) {
                    held[t] = true;
                }
            }
        }
        for (t, held) in held.iter().enumerate() {
            tallies[t].holders += u64::from(*held);
        }
    }

    Ok(HoldersReport {
        method: HOLDERS_METHOD,
        profile: profile.to_string(),
        objects_total,
        series_total,
        statements: tallies
            .into_iter()
            .map(|tally| {
                let entry = entries[tally.entry];
                HolderCount {
                    id: entry.id.clone(),
                    promql: entry.promql.clone(),
                    positive_equalities: tally
                        .selectors
                        .iter()
                        .flat_map(|s| &s.positive_equalities)
                        .map(|(label, value)| format!("{label}=\"{value}\""))
                        .collect(),
                    selectors: tally.selectors.into_iter().map(|s| s.text).collect(),
                    holders: tally.holders,
                }
            })
            .collect(),
        skipped,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::segment_support::build_segment;
    use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

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

    fn entry(id: &str, promql: &str) -> CorpusEntry {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "promql": promql,
            "constructs": [],
        }))
        .expect("entry")
    }

    /// Holders are objects with a fully matching series: object `a` holds
    /// `job="api"` with `code!="500"`, `b` has `job="api"` only with code 500
    /// (so a positive-equality-only test would wrongly count it), `c` has a
    /// different job. A statement with no positive equality is skipped.
    #[test]
    fn holders_count_objects_with_a_fully_matching_series() {
        let objects = vec![
            (
                "a".to_string(),
                object(&[
                    &[
                        ("__name__", "http_requests"),
                        ("job", "api"),
                        ("code", "200"),
                    ],
                    &[
                        ("__name__", "http_requests"),
                        ("job", "web"),
                        ("code", "200"),
                    ],
                ]),
            ),
            (
                "b".to_string(),
                object(&[&[
                    ("__name__", "http_requests"),
                    ("job", "api"),
                    ("code", "500"),
                ]]),
            ),
            (
                "c".to_string(),
                object(&[&[
                    ("__name__", "http_requests"),
                    ("job", "db"),
                    ("code", "200"),
                ]]),
            ),
        ];
        let corpus = vec![
            entry(
                "eq_and_ne",
                r#"sum(rate(http_requests{job="api",code!="500"}[5m]))"#,
            ),
            entry("eq_only", r#"http_requests{job="api"}"#),
            entry("name_only", "sum(http_requests)"),
            entry("empty_eq", r#"http_requests{job=""}"#),
        ];
        let report = count_holders(&corpus, "ci", objects.into_iter().map(Ok)).expect("counts");
        assert_eq!(report.objects_total, 3);
        assert_eq!(report.series_total, 4);
        let counts: Vec<(&str, u64)> = report
            .statements
            .iter()
            .map(|s| (s.id.as_str(), s.holders))
            .collect();
        assert_eq!(counts, [("eq_and_ne", 1), ("eq_only", 2)]);
        assert_eq!(
            report.statements[0].positive_equalities,
            vec![r#"job="api""#.to_string()]
        );
        let skipped: Vec<&str> = report.skipped.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(skipped, ["name_only", "empty_eq"]);
    }

    /// An object that does not decode voids the count rather than being
    /// skipped.
    #[test]
    fn an_unreadable_object_is_an_error() {
        let corpus = vec![entry("eq_only", r#"http_requests{job="api"}"#)];
        let error = count_holders(
            &corpus,
            "ci",
            [Ok(("broken".to_string(), b"not an object".to_vec()))],
        )
        .expect_err("refused");
        assert_eq!(error.key, "broken");
    }
}
