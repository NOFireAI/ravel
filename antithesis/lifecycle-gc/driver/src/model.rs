//! The driver's record of what it wrote and erased, and the pure judgements
//! every read is checked with. Nothing here calls Ravel: whether a record is
//! erased is decided from the driver's own bookkeeping (which subject each
//! record carries, when its write was acknowledged, when each erasure
//! request was submitted and observed complete), never from a Ravel matcher.
//!
//! Events are ordered by one driver sequence counter (`Seq`), never by wall
//! time, which the clock jumps move.

use std::collections::{BTreeMap, HashMap};

pub type Seq = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Signal {
    Logs,
    Metrics,
}

impl Signal {
    pub fn name(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Metrics => "metrics",
        }
    }

    /// The key segment Ravel stores the signal under (`t/<hex>/<seg>/...`).
    pub fn key_segment(self) -> &'static str {
        match self {
            Signal::Logs => "l",
            Signal::Metrics => "m",
        }
    }

    /// The SQL table a commit token names.
    pub fn token_table(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Metrics => "samples",
        }
    }
}

/// Where a record carries its subject. Logs use the first three; a metrics
/// sample always carries it as a series label.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Placement {
    Record,
    Resource,
    Scope,
    Series,
}

impl Placement {
    pub fn name(self) -> &'static str {
        match self {
            Placement::Record => "record",
            Placement::Resource => "resource",
            Placement::Scope => "scope",
            Placement::Series => "series",
        }
    }

    pub fn parse(s: &str) -> Option<Placement> {
        match s.trim() {
            "record" => Some(Placement::Record),
            "resource" => Some(Placement::Resource),
            "scope" => Some(Placement::Scope),
            _ => None,
        }
    }
}

/// The subjects records carry. The first `ERASABLE` may be erased; the
/// `keep-*` sentinels never are, so every timeline has survivors.
pub const SUBJECTS: [&str; 8] = ["s0", "s1", "s2", "s3", "s4", "s5", "keep-0", "keep-1"];
pub const ERASABLE: usize = 6;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    InFlight,
    /// A strict 200 with no partial success and a commit-token header.
    Acked {
        header: String,
        seq: Seq,
    },
    /// Anything else: a buffered ack, a refusal, a partial success, no
    /// response. The records may or may not be stored, now or later.
    Indefinite,
}

#[derive(Clone, Debug)]
pub struct Write {
    pub signal: Signal,
    pub buffered: bool,
    pub ts_ns: u64,
    /// Record ids are `first_rec..first_rec + records.len()`.
    pub first_rec: u64,
    /// Each record's subject index and placement.
    pub records: Vec<(u8, Placement)>,
    pub outcome: Outcome,
}

impl Write {
    pub fn acked_seq(&self) -> Option<Seq> {
        match &self.outcome {
            Outcome::Acked { seq, .. } => Some(*seq),
            _ => None,
        }
    }

    /// Commit tokens of the acknowledgement, qualified with the table.
    pub fn tokens(&self) -> Vec<String> {
        match &self.outcome {
            Outcome::Acked { header, .. } => header
                .split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(|t| format!("{}:{t}", self.signal.token_table()))
                .collect(),
            _ => Vec::new(),
        }
    }

    pub fn ids(&self) -> std::ops::Range<u64> {
        self.first_rec..self.first_rec + self.records.len() as u64
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    pub request_id: String,
    pub signal: Signal,
    pub subject: u8,
    /// When the first `erase submit` attempt started. A write acknowledged
    /// before this is a victim.
    pub started: Seq,
    /// When an `erase submit` attempt returned success.
    pub acked: Option<Seq>,
    /// When the driver first listed this request's `.done`.
    pub done: Option<Seq>,
    /// When the driver first confirmed this request's `.dreq` gone.
    pub gone: Option<Seq>,
    pub dreq_key: Option<String>,
}

impl Request {
    /// The point after which the request counts as completed or swept.
    pub fn completed(&self) -> Option<Seq> {
        match (self.done, self.gone) {
            (Some(d), Some(g)) => Some(d.min(g)),
            (d, g) => d.or(g),
        }
    }
}

#[derive(Default, Debug)]
pub struct Model {
    pub writes: Vec<Write>,
    pub requests: Vec<Request>,
    next_rec: u64,
}

/// A returned record that an erasure covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hit {
    pub request: String,
    pub id: u64,
    pub subject: &'static str,
    pub placement: Placement,
}

#[derive(Default, Debug)]
pub struct ReadVerdict {
    /// `(id, copies read, copies written)` above the written count; an id
    /// the driver never wrote has a written count of 0.
    pub over: Vec<(u64, u64, u64)>,
    /// Requests acknowledged before the read and not yet observed complete.
    pub pending_checked: Vec<String>,
    pub pending_hits: Vec<Hit>,
    /// Requests observed complete or swept before the read.
    pub done_checked: Vec<String>,
    pub done_hits: Vec<Hit>,
    /// Requests whose `.dreq` was confirmed gone before the read.
    pub gone_checked: Vec<String>,
    /// Records of a survivor subject, acknowledged before some request
    /// whose `.dreq` was gone before the read, that the read returned:
    /// evidence the read was not empty for the victims' time range.
    pub survivors_before_gone: usize,
}

impl ReadVerdict {
    /// Per-placement counts of the done hits.
    pub fn done_hits_by_placement(&self) -> BTreeMap<&'static str, usize> {
        by_placement(&self.done_hits)
    }

    pub fn pending_hits_by_placement(&self) -> BTreeMap<&'static str, usize> {
        by_placement(&self.pending_hits)
    }
}

fn by_placement(hits: &[Hit]) -> BTreeMap<&'static str, usize> {
    let mut m = BTreeMap::new();
    for h in hits {
        *m.entry(h.placement.name()).or_insert(0) += 1;
    }
    m
}

impl Model {
    /// Register a write before it is sent, so a read that overlaps it can
    /// attribute its rows. Returns the write's index and first record id.
    pub fn register(
        &mut self,
        signal: Signal,
        buffered: bool,
        ts_ns: u64,
        records: Vec<(u8, Placement)>,
    ) -> (usize, u64) {
        let first = self.next_rec;
        self.next_rec += records.len() as u64;
        self.writes.push(Write {
            signal,
            buffered,
            ts_ns,
            first_rec: first,
            records,
            outcome: Outcome::InFlight,
        });
        (self.writes.len() - 1, first)
    }

    pub fn finish(&mut self, write: usize, outcome: Outcome) {
        if let Some(w) = self.writes.get_mut(write) {
            w.outcome = outcome;
        }
    }

    /// The write holding record `id` and the record's index in it.
    pub fn locate(&self, id: u64) -> Option<(&Write, usize)> {
        let i = self.writes.partition_point(|w| w.first_rec <= id);
        let w = self.writes.get(i.checked_sub(1)?)?;
        let k = usize::try_from(id - w.first_rec).ok()?;
        (k < w.records.len()).then_some((w, k))
    }

    /// Whether `subject` has no erasure request on `signal`, in any state:
    /// a request whose submit outcome is unknown may still erase it.
    pub fn is_survivor_subject(&self, signal: Signal, subject: u8) -> bool {
        !self
            .requests
            .iter()
            .any(|r| r.signal == signal && r.subject == subject)
    }

    /// Whether record `id` is a victim of `req`: same signal and subject,
    /// and its write was strictly acknowledged before the request's first
    /// submit attempt started.
    pub fn is_victim(&self, req: &Request, id: u64) -> bool {
        let Some((w, k)) = self.locate(id) else {
            return false;
        };
        w.signal == req.signal
            && !w.buffered
            && w.records[k].0 == req.subject
            && w.acked_seq().is_some_and(|a| a < req.started)
    }

    /// Every victim of `req`, in id order.
    pub fn victims(&self, req: &Request) -> Vec<u64> {
        self.writes
            .iter()
            .filter(|w| w.signal == req.signal && !w.buffered)
            .filter(|w| w.acked_seq().is_some_and(|a| a < req.started))
            .flat_map(|w| {
                w.ids()
                    .zip(w.records.iter())
                    .filter(|(_, (s, _))| *s == req.subject)
                    .map(|(id, _)| id)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Every survivor record of `signal` the eventually must read exactly
    /// once: a strictly acknowledged record whose subject has no request.
    pub fn survivors(&self, signal: Signal) -> Vec<u64> {
        self.writes
            .iter()
            .filter(|w| w.signal == signal && !w.buffered && w.acked_seq().is_some())
            .flat_map(|w| {
                w.ids()
                    .zip(w.records.iter())
                    .filter(|(_, (s, _))| self.is_survivor_subject(signal, *s))
                    .map(|(id, _)| id)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Inject one victim of a request completed before `issued` into
    /// `counts` (the self-test). Returns the id injected.
    pub fn inject_victim(
        &self,
        signal: Signal,
        issued: Seq,
        counts: &mut HashMap<u64, u64>,
    ) -> Option<u64> {
        let req = self.requests.iter().find(|r| {
            r.signal == signal
                && r.acked.is_some_and(|a| a < issued)
                && r.completed().is_some_and(|c| c < issued)
        })?;
        let id = *self.victims(req).first()?;
        let n = counts.entry(id).or_insert(0);
        *n = (*n).max(1);
        Some(id)
    }

    /// Judge one read of `signal`, issued at `issued`, that returned
    /// `counts` (record id to copies).
    pub fn judge_read(
        &self,
        signal: Signal,
        issued: Seq,
        counts: &HashMap<u64, u64>,
    ) -> ReadVerdict {
        let mut v = ReadVerdict::default();
        for (&id, &n) in counts {
            let written = match self.locate(id) {
                Some((w, _)) if w.signal == signal => 1,
                _ => 0,
            };
            if n > written {
                v.over.push((id, n, written));
            }
        }
        v.over.sort_unstable();
        let mut returned: Vec<u64> = counts
            .iter()
            .filter(|(_, n)| **n > 0)
            .map(|(id, _)| *id)
            .collect();
        returned.sort_unstable();
        for req in &self.requests {
            if req.signal != signal || !req.acked.is_some_and(|a| a < issued) {
                continue;
            }
            let completed = req.completed().is_some_and(|c| c < issued);
            let hits: Vec<Hit> = returned
                .iter()
                .filter(|id| self.is_victim(req, **id))
                .filter_map(|&id| {
                    let (w, k) = self.locate(id)?;
                    let (subject, placement) = w.records[k];
                    Some(Hit {
                        request: req.request_id.clone(),
                        id,
                        subject: SUBJECTS.get(usize::from(subject)).copied().unwrap_or("?"),
                        placement,
                    })
                })
                .collect();
            if completed {
                v.done_checked.push(req.request_id.clone());
                v.done_hits.extend(hits);
            } else {
                v.pending_checked.push(req.request_id.clone());
                v.pending_hits.extend(hits);
            }
            if req.gone.is_some_and(|g| g < issued) {
                v.gone_checked.push(req.request_id.clone());
                v.survivors_before_gone += returned
                    .iter()
                    .filter(|id| {
                        self.locate(**id).is_some_and(|(w, k)| {
                            w.signal == signal
                                && !w.buffered
                                && self.is_survivor_subject(signal, w.records[k].0)
                                && w.acked_seq().is_some_and(|a| a < req.started)
                        })
                    })
                    .count();
            }
        }
        v
    }
}

/// Survivors of a final read: every expected id exactly once.
#[derive(Default, Debug)]
pub struct SurvivorVerdict {
    pub missing: Vec<u64>,
    /// `(id, copies)` for an expected id read more than once.
    pub extra: Vec<(u64, u64)>,
}

pub fn judge_survivors(expected: &[u64], counts: &HashMap<u64, u64>) -> SurvivorVerdict {
    let mut v = SurvivorVerdict::default();
    for id in expected {
        match counts.get(id).copied().unwrap_or(0) {
            0 => v.missing.push(*id),
            1 => {}
            n => v.extra.push((*id, n)),
        }
    }
    v
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn ack(m: &mut Model, w: usize, seq: Seq) {
        m.finish(
            w,
            Outcome::Acked {
                header: "tok".into(),
                seq,
            },
        );
    }

    /// The #4503 shape: alice (s0) in a resource and a scope attribute, dave
    /// (s1) in a record attribute, bob (keep-0) survivors; both erased.
    fn shape_4503() -> Model {
        let mut m = Model::default();
        let (w0, _) = m.register(
            Signal::Logs,
            false,
            0,
            vec![
                (0, Placement::Resource),
                (0, Placement::Scope),
                (1, Placement::Record),
                (6, Placement::Record),
                (6, Placement::Resource),
            ],
        );
        ack(&mut m, w0, 1);
        for (i, subject) in [0u8, 1].into_iter().enumerate() {
            m.requests.push(Request {
                request_id: format!("r{i}"),
                signal: Signal::Logs,
                subject,
                started: 10,
                acked: Some(11),
                done: Some(20),
                gone: Some(30),
                dreq_key: None,
            });
        }
        m
    }

    fn counts(ids: &[u64]) -> HashMap<u64, u64> {
        ids.iter().map(|i| (*i, 1)).collect()
    }

    #[test]
    fn the_4503_shape_fails_only_after_completion_and_names_the_placements() {
        let m = shape_4503();
        // After the sweep, the unfixed rewrite serves alice's resource and
        // scope records again; dave stays erased.
        let v = m.judge_read(Signal::Logs, 31, &counts(&[0, 1, 3, 4]));
        assert!(v.over.is_empty());
        assert!(v.pending_hits.is_empty());
        assert_eq!(v.done_checked, vec!["r0", "r1"]);
        assert_eq!(
            v.done_hits_by_placement(),
            BTreeMap::from([("resource", 1), ("scope", 1)])
        );
        assert_eq!(v.gone_checked, vec!["r0", "r1"]);
        assert_eq!(v.survivors_before_gone, 4, "bob's two records, per request");
        // The same rows read while the requests were pending are hits while
        // pending, never hits after completion.
        let v = m.judge_read(Signal::Logs, 12, &counts(&[0, 1, 3, 4]));
        assert!(v.done_hits.is_empty());
        assert_eq!(v.pending_hits.len(), 2);
        // A read issued before the request ack judges nothing.
        let v = m.judge_read(Signal::Logs, 11, &counts(&[0, 1, 2, 3, 4]));
        assert!(v.pending_checked.is_empty() && v.done_checked.is_empty());
    }

    #[test]
    fn a_correct_read_passes_and_an_empty_one_proves_no_survivor() {
        let m = shape_4503();
        let v = m.judge_read(Signal::Logs, 31, &counts(&[3, 4]));
        assert!(v.over.is_empty() && v.done_hits.is_empty() && v.pending_hits.is_empty());
        assert_eq!(v.survivors_before_gone, 4);
        let v = m.judge_read(Signal::Logs, 31, &HashMap::new());
        assert!(v.done_hits.is_empty());
        assert_eq!(v.survivors_before_gone, 0, "an empty read is not evidence");
    }

    #[test]
    fn a_write_acked_after_the_submit_started_is_not_a_victim() {
        let mut m = shape_4503();
        let (w1, first) = m.register(Signal::Logs, false, 0, vec![(0, Placement::Record)]);
        ack(&mut m, w1, 10); // acked at the submit's start: not strictly before
        let v = m.judge_read(Signal::Logs, 31, &counts(&[first]));
        assert!(v.done_hits.is_empty() && v.over.is_empty());
        // A buffered or indefinite write is never a victim either.
        let (w2, f2) = m.register(Signal::Logs, true, 0, vec![(0, Placement::Record)]);
        ack(&mut m, w2, 2);
        let (_, f3) = m.register(Signal::Logs, false, 0, vec![(0, Placement::Record)]);
        let v = m.judge_read(Signal::Logs, 31, &counts(&[f2, f3]));
        assert!(v.done_hits.is_empty() && v.over.is_empty());
    }

    #[test]
    fn copies_above_the_written_count_and_unknown_ids_are_over() {
        let m = shape_4503();
        let mut c = counts(&[3]);
        c.insert(4, 2);
        c.insert(999, 1);
        let v = m.judge_read(Signal::Logs, 31, &c);
        assert_eq!(v.over, vec![(4, 2, 1), (999, 1, 0)]);
        // A logs id read back as a metrics sample was never written there.
        let v = m.judge_read(Signal::Metrics, 31, &counts(&[3]));
        assert_eq!(v.over, vec![(3, 1, 0)]);
    }

    #[test]
    fn survivors_exclude_every_requested_subject_and_need_exactly_one_copy() {
        let mut m = shape_4503();
        // An unacked request still makes its subject unconstrained.
        m.requests.push(Request {
            request_id: "r2".into(),
            signal: Signal::Logs,
            subject: 7,
            started: 40,
            acked: None,
            done: None,
            gone: None,
            dreq_key: None,
        });
        let (w, first) = m.register(Signal::Logs, false, 0, vec![(7, Placement::Record)]);
        ack(&mut m, w, 41);
        assert_eq!(m.survivors(Signal::Logs), vec![3, 4]);
        assert!(!m.survivors(Signal::Logs).contains(&first));
        let mut c = counts(&[3]);
        c.insert(4, 2);
        let v = judge_survivors(&m.survivors(Signal::Logs), &c);
        assert_eq!((v.missing, v.extra), (vec![], vec![(4, 2)]));
        let v = judge_survivors(&m.survivors(Signal::Logs), &counts(&[4]));
        assert_eq!(v.missing, vec![3]);
    }

    #[test]
    fn the_injection_adds_one_hit_after_completion_and_nothing_else() {
        let m = shape_4503();
        let mut c = counts(&[3, 4]);
        let id = m.inject_victim(Signal::Logs, 31, &mut c).expect("a victim");
        let v = m.judge_read(Signal::Logs, 31, &c);
        assert!(v.over.is_empty(), "{:?}", v.over);
        assert!(v.pending_hits.is_empty());
        assert_eq!(v.done_hits.len(), 1);
        assert_eq!(v.done_hits[0].id, id);
        // Nothing completed before the read: nothing to inject.
        let mut c = counts(&[3]);
        assert_eq!(m.inject_victim(Signal::Logs, 15, &mut c), None);
    }

    #[test]
    fn locate_maps_every_id_to_its_write() {
        let mut m = Model::default();
        let (_, a) = m.register(Signal::Logs, false, 0, vec![(0, Placement::Record); 3]);
        let (_, b) = m.register(Signal::Metrics, false, 0, vec![(1, Placement::Series); 2]);
        assert_eq!((a, b), (0, 3));
        assert_eq!(m.locate(2).map(|(w, k)| (w.first_rec, k)), Some((0, 2)));
        assert_eq!(m.locate(4).map(|(w, k)| (w.first_rec, k)), Some((3, 1)));
        assert!(m.locate(5).is_none());
    }
}
