//! The literal workload: a TRAINING set the summary is built from and a
//! disjoint EVALUATION set it is judged on, both drawn from the subject's own
//! values by a pre-registered seed before the summary is built.
//!
//! Per kind (equality, `LIKE '%literal%'`), `literals_per_kind` literals go to
//! each set, half present and half absent:
//!
//! - present equality: a value of the column, `gram_len..=eq_max_len` bytes;
//! - absent equality: such a value with one ASCII letter or digit replaced by
//!   another of the same class, kept only when no value equals it;
//! - present `LIKE`: a substring of a value, `like_min_len..=like_max_len`
//!   bytes;
//! - absent `LIKE`: such a substring perturbed the same way, kept only when
//!   no block's text contains it.
//!
//! Every literal of a kind is distinct, so no evaluation literal was seen at
//! build time. Each kind draws all its present literals, then all its absent
//! ones; the first half of each goes to training and the second half to
//! evaluation.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::{BlockValues, par_any};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Equals,
    Like,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Equals => "equals",
            Kind::Like => "like",
        }
    }
}

/// The workload's pre-registered parameters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkloadParams {
    pub seed: u64,
    /// Literals per kind in each set; even, half present and half absent.
    pub literals_per_kind: usize,
    pub gram_len: usize,
    pub eq_max_len: usize,
    pub like_min_len: usize,
    pub like_max_len: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Literal {
    pub kind: Kind,
    /// Whether some value equals (equality) or contains (`LIKE`) it.
    pub present: bool,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workload {
    pub training: Vec<Literal>,
    pub evaluation: Vec<Literal>,
}

/// Draw attempts allowed per literal before the draw gives up.
pub const ATTEMPTS_PER_LITERAL: usize = 10_000;

/// SplitMix64: a fixed, dependency-free generator, so a seed names the same
/// workload on every host.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`; `n` must be nonzero.
    pub fn below(&mut self, n: usize) -> usize {
        ((u128::from(self.next_u64()) * n as u128) >> 64) as usize
    }
}

/// Replaces one ASCII letter or digit of `s` with a different one of the same
/// class; `None` when `s` has none.
fn perturb(s: &str, rng: &mut Rng) -> Option<String> {
    let positions: Vec<usize> = s
        .bytes()
        .enumerate()
        .filter(|(_, b)| b.is_ascii_alphanumeric())
        .map(|(i, _)| i)
        .collect();
    if positions.is_empty() {
        return None;
    }
    let at = positions[rng.below(positions.len())];
    let mut bytes = s.as_bytes().to_vec();
    let old = bytes[at];
    let (base, span) = if old.is_ascii_digit() {
        (b'0', 10u8)
    } else if old.is_ascii_lowercase() {
        (b'a', 26u8)
    } else {
        (b'A', 26u8)
    };
    let offset = (old - base + 1 + rng.below(usize::from(span) - 1) as u8) % span;
    bytes[at] = base + offset;
    String::from_utf8(bytes).ok()
}

struct Draw<'a> {
    blocks: &'a [BlockValues],
    texts: &'a [String],
    starts: Vec<usize>,
    total: usize,
    p: WorkloadParams,
    rng: Rng,
}

impl Draw<'_> {
    fn random_value(&mut self) -> Option<&[u8]> {
        if self.total == 0 {
            return None;
        }
        let i = self.rng.below(self.total);
        let b = self.starts.partition_point(|&s| s <= i).checked_sub(1)?;
        let row = i - self.starts.get(b)?;
        self.blocks.get(b)?.get(row).map(Vec::as_slice)
    }

    fn equals_value(&mut self) -> Option<String> {
        let (lo, hi) = (self.p.gram_len, self.p.eq_max_len);
        let v = self.random_value()?;
        if v.len() < lo || v.len() > hi || v.contains(&0) {
            return None;
        }
        std::str::from_utf8(v).ok().map(str::to_string)
    }

    fn like_substring(&mut self) -> Option<String> {
        let (lo, hi) = (self.p.like_min_len, self.p.like_max_len);
        let v = self.random_value()?.to_vec();
        if v.len() < lo {
            return None;
        }
        let len = lo + self.rng.below(hi.min(v.len()) - lo + 1);
        let start = self.rng.below(v.len() - len + 1);
        let s = v.get(start..start + len)?;
        if s.contains(&0) {
            return None;
        }
        std::str::from_utf8(s).ok().map(str::to_string)
    }

    fn some_value_equals(&self, s: &str) -> bool {
        self.blocks
            .iter()
            .any(|b| b.iter().any(|v| v.as_slice() == s.as_bytes()))
    }

    fn candidate(&mut self, kind: Kind, present: bool) -> Option<String> {
        let base = match kind {
            Kind::Equals => self.equals_value()?,
            Kind::Like => self.like_substring()?,
        };
        if present {
            return Some(base);
        }
        let s = perturb(&base, &mut self.rng)?;
        let found = match kind {
            Kind::Equals => self.some_value_equals(&s),
            Kind::Like => par_any(self.texts, |t| t.contains(s.as_str())),
        };
        (!found).then_some(s)
    }

    fn many(
        &mut self,
        kind: Kind,
        present: bool,
        n: usize,
        taken: &mut HashSet<String>,
    ) -> Result<Vec<Literal>, String> {
        let mut out = Vec::with_capacity(n);
        let mut attempts = 0usize;
        while out.len() < n {
            if attempts >= ATTEMPTS_PER_LITERAL * n {
                return Err(format!(
                    "cannot draw {n} {} {} literals in {attempts} attempts (drew {})",
                    if present { "present" } else { "absent" },
                    kind.name(),
                    out.len()
                ));
            }
            attempts += 1;
            if let Some(text) = self.candidate(kind, present)
                && taken.insert(text.clone())
            {
                out.push(Literal {
                    kind,
                    present,
                    text,
                });
            }
        }
        Ok(out)
    }
}

/// Draws the workload from `blocks` (and `texts`, each block's values joined
/// by a 0x00 byte, for the absent-substring check).
pub fn draw(
    blocks: &[BlockValues],
    texts: &[String],
    p: &WorkloadParams,
) -> Result<Workload, String> {
    if p.literals_per_kind == 0 || !p.literals_per_kind.is_multiple_of(2) {
        return Err(format!(
            "literals_per_kind {} is not a positive even number",
            p.literals_per_kind
        ));
    }
    if p.like_min_len < p.gram_len || p.like_max_len < p.like_min_len {
        return Err("need gram_len <= like_min_len <= like_max_len".into());
    }
    if p.eq_max_len < p.gram_len {
        return Err("need gram_len <= eq_max_len".into());
    }
    let mut starts = Vec::with_capacity(blocks.len());
    let mut total = 0usize;
    for b in blocks {
        starts.push(total);
        total += b.len();
    }
    let mut d = Draw {
        blocks,
        texts,
        starts,
        total,
        p: *p,
        rng: Rng::new(p.seed),
    };
    let half = p.literals_per_kind / 2;
    let mut training = Vec::new();
    let mut evaluation = Vec::new();
    for kind in [Kind::Equals, Kind::Like] {
        let mut taken = HashSet::new();
        for present in [true, false] {
            let mut drawn = d.many(kind, present, 2 * half, &mut taken)?;
            let eval = drawn.split_off(half);
            training.extend(drawn);
            evaluation.extend(eval);
        }
    }
    Ok(Workload {
        training,
        evaluation,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::string_fingerprint::block_texts;

    fn blocks() -> Vec<BlockValues> {
        (0..6)
            .map(|b| {
                (0..5)
                    .map(|r| format!("host{b} request {r} served path /a{b}{r}").into_bytes())
                    .collect()
            })
            .collect()
    }

    fn params(seed: u64) -> WorkloadParams {
        WorkloadParams {
            seed,
            literals_per_kind: 4,
            gram_len: 3,
            eq_max_len: 64,
            like_min_len: 4,
            like_max_len: 10,
        }
    }

    #[test]
    fn draws_disjoint_verified_sets_deterministically() {
        let b = blocks();
        let t = block_texts(&b).expect("texts");
        let w = draw(&b, &t, &params(7)).expect("draw");
        assert_eq!(w, draw(&b, &t, &params(7)).expect("draw again"));
        assert_ne!(
            w.evaluation,
            draw(&b, &t, &params(8)).expect("other").evaluation
        );
        for kind in [Kind::Equals, Kind::Like] {
            let train: Vec<&Literal> = w.training.iter().filter(|l| l.kind == kind).collect();
            let eval: Vec<&Literal> = w.evaluation.iter().filter(|l| l.kind == kind).collect();
            assert_eq!(train.len(), 4);
            assert_eq!(eval.len(), 4);
            for set in [&train, &eval] {
                assert_eq!(set.iter().filter(|l| l.present).count(), 2);
            }
            for e in &eval {
                assert!(train.iter().all(|t| t.text != e.text), "{e:?} was seen");
            }
        }
        for l in w.training.iter().chain(&w.evaluation) {
            let held = match l.kind {
                Kind::Equals => b
                    .iter()
                    .flatten()
                    .any(|v| v.as_slice() == l.text.as_bytes()),
                Kind::Like => t.iter().any(|x| x.contains(l.text.as_str())),
            };
            assert_eq!(held, l.present, "{l:?}");
        }
    }

    #[test]
    fn refuses_when_no_value_is_eligible() {
        let b = blocks();
        let t = block_texts(&b).expect("texts");
        let p = WorkloadParams {
            eq_max_len: 5,
            gram_len: 3,
            ..params(1)
        };
        let err = draw(&b, &t, &p).unwrap_err();
        assert!(err.contains("cannot draw"), "{err}");
    }

    #[test]
    fn perturb_keeps_the_character_class() {
        let mut rng = Rng::new(3);
        for _ in 0..200 {
            let s = perturb("a1Z-", &mut rng).expect("has alnum");
            assert_ne!(s, "a1Z-");
            let diff: Vec<(u8, u8)> = s
                .bytes()
                .zip("a1Z-".bytes())
                .filter(|(x, y)| x != y)
                .collect();
            assert_eq!(diff.len(), 1);
            let (new, old) = diff[0];
            assert_eq!(new.is_ascii_digit(), old.is_ascii_digit());
            assert_eq!(new.is_ascii_lowercase(), old.is_ascii_lowercase());
            assert_eq!(new.is_ascii_uppercase(), old.is_ascii_uppercase());
        }
        assert!(perturb("--", &mut rng).is_none());
    }
}
