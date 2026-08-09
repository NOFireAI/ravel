//! Reproducibility digest (ADR-0068 deliverable 5c).
//!
//! Scope, deliberately narrow: the digest covers query **results only** --
//! series label sets, timestamps, and values (compared by `f64::to_bits`,
//! never `==`, per the repo-wide float discipline) -- and nothing else.
//!
//! It must never fold in commit tokens, object store keys, or writer ids.
//! Those are legitimately nondeterministic between two runs of the same
//! seed today: `IngestRouter` mints a fresh UUIDv4 writer id per instance
//! (the `RngSource` seam that would let a sim seed pin it is a later task,
//! #816), and object keys and commit tokens derive from that writer id and
//! from allocation order. A digest that hashed those would report
//! "nondeterministic" on every run even though the workload, the ingested
//! values, and the query results it actually reads back are all identical.
//! `seeded_cycle_reproducible` (tests/) proves this scope is exercised by
//! temporarily widening it back to a token and watching the test fail.
//!
//! Series and samples are sorted before mixing so that any latent
//! iteration-order nondeterminism upstream (the class of bug issue #801
//! fixed at the query combine stage) cannot leak through the digest either.

use ravel_promql::Value;
use ravel_types::LabelSet;

/// A cycle's reproducibility fingerprint. Two runs of the same
/// [`crate::seed::MasterSeed`] through the same driver must produce equal
/// digests; two different seeds are not required to differ (a hash
/// collision is always possible in principle) but in practice always will
/// for workloads of any size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Digest(pub u64);

/// FNV-1a accumulator. Chosen over `std::hash::DefaultHasher` for the same
/// reason as [`crate::seed::MasterSeed::sub_seed`]: it is fully specified
/// and stable across Rust toolchain versions, so a recorded digest from a
/// bug report stays comparable regardless of what compiler reproduces it.
#[derive(Debug, Clone)]
pub struct DigestBuilder {
    state: u64,
}

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

impl Default for DigestBuilder {
    fn default() -> Self {
        DigestBuilder { state: FNV_OFFSET }
    }
}

impl DigestBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mix_bytes(&mut self, bytes: &[u8]) -> &mut Self {
        for &byte in bytes {
            self.state ^= u64::from(byte);
            self.state = self.state.wrapping_mul(FNV_PRIME);
        }
        self
    }

    pub fn mix_u64(&mut self, v: u64) -> &mut Self {
        self.mix_bytes(&v.to_le_bytes())
    }

    pub fn mix_i64(&mut self, v: i64) -> &mut Self {
        self.mix_bytes(&v.to_le_bytes())
    }

    /// Mixes an `f64` by its bit pattern, never its value: this is what
    /// makes NaN payloads and `-0.0` vs `0.0` significant, matching the
    /// repo-wide float-comparison convention.
    pub fn mix_f64(&mut self, v: f64) -> &mut Self {
        self.mix_u64(v.to_bits())
    }

    pub fn mix_str(&mut self, s: &str) -> &mut Self {
        self.mix_u64(s.len() as u64);
        self.mix_bytes(s.as_bytes())
    }

    /// `LabelSet` already stores labels sorted by name (`LabelSet::new`
    /// enforces this), so mixing in iteration order is already canonical.
    pub fn mix_label_set(&mut self, labels: &LabelSet) -> &mut Self {
        self.mix_u64(labels.len() as u64);
        for label in labels.iter() {
            self.mix_str(&label.name);
            self.mix_str(&label.value);
        }
        self
    }

    pub fn finish(&self) -> Digest {
        Digest(self.state)
    }
}

/// A label set's sort key: its labels are already name-sorted by
/// `LabelSet::new`, so comparing the rendered `name=value,...` string
/// orders series canonically without depending on any hash-map iteration.
fn label_sort_key(labels: &LabelSet) -> String {
    labels
        .iter()
        .map(|l| format!("{}={}", l.name, l.value))
        .collect::<Vec<_>>()
        .join(",")
}

/// Folds one query's result into `builder`, canonicalizing series order
/// (and, for vectors, sample order within a series) first so iteration
/// order upstream can never change the digest.
pub fn mix_value(builder: &mut DigestBuilder, value: &Value) {
    match value {
        Value::Scalar(v) => {
            builder.mix_u64(0);
            builder.mix_f64(*v);
        }
        Value::String(s) => {
            builder.mix_u64(1);
            builder.mix_str(s);
        }
        Value::Vector(vector) => {
            builder.mix_u64(2);
            let mut samples: Vec<_> = vector.iter().collect();
            samples.sort_by_key(|a| label_sort_key(&a.labels));
            builder.mix_u64(samples.len() as u64);
            // `InstantSample::histogram` is deliberately not mixed in: every
            // histogram series `workload::generate_queries` emits is wrapped
            // in `histogram_count(...)`, which reduces to `value` and leaves
            // `histogram: None` on the result element, so this field never
            // carries data for any query this driver runs today. A future
            // query type that returns a bare native-histogram selector would
            // need to mix `sample.histogram` here too, or it silently drops
            // out of the reproducibility digest.
            for sample in samples {
                builder.mix_label_set(&sample.labels);
                builder.mix_i64(sample.ts_ns);
                builder.mix_f64(sample.value);
            }
        }
        Value::Matrix(matrix) => {
            builder.mix_u64(3);
            let mut series: Vec<_> = matrix.iter().collect();
            series.sort_by_key(|(a, _)| label_sort_key(a));
            builder.mix_u64(series.len() as u64);
            for (labels, points) in series {
                builder.mix_label_set(labels);
                builder.mix_u64(points.len() as u64);
                for point in points {
                    builder.mix_i64(point.ts_ns);
                    builder.mix_f64(point.value);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ravel_promql::InstantSample;
    use ravel_types::Label;

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(n, v)| Label {
                    name: (*n).to_string(),
                    value: (*v).to_string(),
                })
                .collect(),
        )
        .expect("valid label set")
    }

    #[test]
    fn vector_digest_is_order_independent() {
        let s1 = InstantSample {
            labels: labels(&[("__name__", "a")]),
            ts_ns: 1,
            orig_sample_ts_ns: 1,
            value: 1.0,
            histogram: None,
        };
        let s2 = InstantSample {
            labels: labels(&[("__name__", "b")]),
            ts_ns: 2,
            orig_sample_ts_ns: 2,
            value: 2.0,
            histogram: None,
        };

        let mut forward = DigestBuilder::new();
        mix_value(&mut forward, &Value::Vector(vec![s1.clone(), s2.clone()]));
        let mut backward = DigestBuilder::new();
        mix_value(&mut backward, &Value::Vector(vec![s2, s1]));

        assert_eq!(forward.finish(), backward.finish());
    }

    #[test]
    fn different_values_produce_different_digests() {
        let mut a = DigestBuilder::new();
        mix_value(&mut a, &Value::Scalar(1.0));
        let mut b = DigestBuilder::new();
        mix_value(&mut b, &Value::Scalar(2.0));
        assert_ne!(a.finish(), b.finish());
    }

    #[test]
    fn negative_zero_and_positive_zero_differ() {
        let mut a = DigestBuilder::new();
        mix_value(&mut a, &Value::Scalar(0.0));
        let mut b = DigestBuilder::new();
        mix_value(&mut b, &Value::Scalar(-0.0));
        assert_ne!(a.finish(), b.finish());
    }
}
