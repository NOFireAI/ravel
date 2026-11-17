//! The acked multiset: the seeded records, each stored exactly once, plus the
//! live writes judged by the ingest-durability model (an acknowledged record
//! at least once, any record at most as often as its attempts allow, nothing
//! the workload never sent). Fold, compaction and sweep rewrite which objects
//! hold the records; none of them may change this multiset.

use std::collections::{BTreeSet, HashMap};

use ravel_antithesis_driver::model::{self, Model};

#[derive(Default, Debug)]
pub struct Verdict {
    /// Seeded identities the rows do not contain.
    pub seeded_missing: Vec<String>,
    /// Seeded identities stored more than once.
    pub seeded_over: Vec<String>,
    /// Live-write findings, from the ingest-durability model.
    pub phantoms: Vec<String>,
    pub over: Vec<String>,
    pub missing: Vec<String>,
    pub rows: u64,
}

impl Verdict {
    pub fn holds(&self) -> bool {
        self.seeded_missing.is_empty()
            && self.seeded_over.is_empty()
            && self.phantoms.is_empty()
            && self.over.is_empty()
            && self.missing.is_empty()
    }

    /// Whether only absences are wrong, which a retried read may resolve
    /// (a node that has not yet listed the newest commit).
    pub fn only_missing(&self) -> bool {
        self.seeded_over.is_empty() && self.phantoms.is_empty() && self.over.is_empty()
    }
}

/// Judge `(write, record) -> count` rows. `seeded` is the seeded expectation;
/// every other row is judged against `live`, whose writes `live_expect` must
/// be present in full.
pub fn judge(
    seeded: &BTreeSet<(u64, u32)>,
    live: &Model,
    counts: &HashMap<(u64, u32), u64>,
    live_expect: &[u64],
) -> Verdict {
    let mut v = Verdict {
        rows: counts.values().sum(),
        ..Verdict::default()
    };
    let mut live_counts = HashMap::new();
    for (&key, &n) in counts {
        if seeded.contains(&key) {
            if n != 1 {
                v.seeded_over.push(format!(
                    "seeded write {} rec {}: stored {n} times",
                    key.0, key.1
                ));
            }
        } else {
            live_counts.insert(key, n);
        }
    }
    for key in seeded {
        if !counts.contains_key(key) {
            v.seeded_missing
                .push(format!("seeded write {} rec {} missing", key.0, key.1));
        }
    }
    let j = model::judge_logs(live, &live_counts, live_expect);
    v.phantoms = j.phantoms;
    v.over = j.over;
    v.over.extend(j.over_after_replay);
    v.missing = j.missing;
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seed::{Plan, SEED_WRITE_BASE};
    use ravel_antithesis_driver::model::{Attempt, Mode, Signal, Write};

    const NS_PER_HOUR: u64 = 3_600_000_000_000;

    fn stored(plan: &Plan) -> HashMap<(u64, u32), u64> {
        plan.rows().iter().map(|r| ((r.write, r.rec), 1)).collect()
    }

    fn live_model() -> Model {
        let mut m = Model::default();
        m.writes.insert(
            7,
            Write {
                id: 7,
                signal: Signal::Logs,
                mode: Mode::Strict,
                key: None,
                records: 2,
                samples: Vec::new(),
                ts_ns: 0,
                attempts: vec![Attempt::Acked {
                    header: "t1".into(),
                }],
            },
        );
        m.mark_acked(7);
        m
    }

    #[test]
    fn exact_rows_hold() {
        let plan = Plan::new(500_000 * NS_PER_HOUR, 3, 2, 4).expect("plan");
        let mut rows = stored(&plan);
        rows.insert((7, 0), 1);
        rows.insert((7, 1), 1);
        let v = judge(&plan.expected(false), &live_model(), &rows, &[7]);
        assert!(v.holds(), "{v:?}");
        assert_eq!(v.rows, 14);
    }

    #[test]
    fn model_detects_a_swapped_record() {
        let plan = Plan::new(500_000 * NS_PER_HOUR, 3, 2, 4).expect("plan");
        let v = judge(&plan.expected(true), &live_model(), &stored(&plan), &[]);
        assert!(!v.holds());
        assert_eq!(
            v.seeded_missing,
            vec![format!(
                "seeded write {} rec 4 missing",
                SEED_WRITE_BASE + 1
            )]
        );
        // The record the load really wrote is not expected anywhere.
        assert_eq!(v.phantoms.len(), 1, "{v:?}");
        assert!(v.phantoms[0].contains(&format!("write {SEED_WRITE_BASE} rec 0")));
    }

    #[test]
    fn a_duplicated_or_lost_seeded_record_fails() {
        let plan = Plan::new(500_000 * NS_PER_HOUR, 2, 1, 2).expect("plan");
        let expected = plan.expected(false);
        let mut rows = stored(&plan);
        rows.insert((SEED_WRITE_BASE, 1), 2);
        let v = judge(&expected, &Model::default(), &rows, &[]);
        assert_eq!(v.seeded_over.len(), 1);
        assert!(!v.only_missing() && !v.holds());

        let mut rows = stored(&plan);
        rows.remove(&(SEED_WRITE_BASE + 1, 0));
        let v = judge(&expected, &Model::default(), &rows, &[]);
        assert_eq!(v.seeded_missing.len(), 1);
        assert!(v.only_missing() && !v.holds());
    }

    #[test]
    fn an_acked_live_record_must_be_present() {
        let plan = Plan::new(500_000 * NS_PER_HOUR, 1, 1, 1).expect("plan");
        let mut rows = stored(&plan);
        rows.insert((7, 0), 1);
        let v = judge(&plan.expected(false), &live_model(), &rows, &[7]);
        assert_eq!(v.missing.len(), 1, "{v:?}");
    }
}
