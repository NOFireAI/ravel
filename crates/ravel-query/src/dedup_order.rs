//! The duplicate-sample order every metrics read surface applies when several
//! samples share one `(series, ts)`: the PromQL engine's k-way merge and the
//! SQL streaming dedup operator both call [`serves_over`], so the two cannot
//! disagree on the same data.

use std::cmp::Ordering;

/// One candidate sample's ordering fields. The `(series, ts)` slot is the
/// grouping key and lives outside this struct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupKey {
    pub created_unix_ns: i64,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    pub in_page_index: u32,
    /// `f64::to_bits` of the sample value, so NaN payloads and `-0.0`
    /// against `0.0` order deterministically.
    pub value_bits: u64,
}

impl DedupKey {
    /// Builds a key from a `(created_unix_ns, writer_epoch, writer_seq,
    /// in_page_index)` priority tuple and the raw value.
    #[inline]
    pub fn new(priority: (i64, u64, u64, u32), value: f64) -> Self {
        let (created_unix_ns, writer_epoch, writer_seq, in_page_index) = priority;
        Self {
            created_unix_ns,
            writer_epoch,
            writer_seq,
            in_page_index,
            value_bits: value.to_bits(),
        }
    }

    /// Builds a key from named fields, for a caller that reads them from
    /// separate columns rather than holding a priority tuple, so two fields
    /// of the same type cannot be transposed by position.
    #[inline]
    pub fn from_parts(
        created_unix_ns: i64,
        writer_epoch: u64,
        writer_seq: u64,
        in_page_index: u32,
        value: f64,
    ) -> Self {
        Self {
            created_unix_ns,
            writer_epoch,
            writer_seq,
            in_page_index,
            value_bits: value.to_bits(),
        }
    }
}

/// Whether a query serves candidate `a` over candidate `b` at one
/// `(series, ts)` (docs/query-engine.md, docs/catalog-and-mvcc.md, ADR-0010
/// §5): the greatest `(created_unix_ns, writer_epoch, writer_seq,
/// in_page_index)` wins, and only on a full tie of those four does the
/// greatest value bit pattern (`f64::to_bits`) win. The order is total, so
/// the winner never depends on arrival order. Returns `false` for equal keys.
///
/// The provenance fields are cluster-local, so across federated clusters this
/// order is meaningful only for disjoint series identity (ADR-0071; see
/// docs/query-engine.md "Cross-cluster duplicate tie-break limitation").
#[inline]
pub fn serves_over(a: &DedupKey, b: &DedupKey) -> bool {
    a.created_unix_ns
        .cmp(&b.created_unix_ns)
        .then(a.writer_epoch.cmp(&b.writer_epoch))
        .then(a.writer_seq.cmp(&b.writer_seq))
        .then(a.in_page_index.cmp(&b.in_page_index))
        .then(a.value_bits.cmp(&b.value_bits))
        == Ordering::Greater
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: DedupKey = DedupKey {
        created_unix_ns: 10,
        writer_epoch: 10,
        writer_seq: 10,
        in_page_index: 10,
        value_bits: 10,
    };

    /// `hi` must win against `lo` in both argument orders.
    fn assert_wins(hi: DedupKey, lo: DedupKey) {
        assert!(serves_over(&hi, &lo), "{hi:?} must be served over {lo:?}");
        assert!(
            !serves_over(&lo, &hi),
            "{lo:?} must not be served over {hi:?}"
        );
    }

    #[test]
    fn each_field_decides_only_when_every_earlier_field_ties() {
        // Each row raises one field by one and lowers every later field to
        // zero; it must still win, so no later field can outrank it.
        let created = DedupKey {
            created_unix_ns: 11,
            writer_epoch: 0,
            writer_seq: 0,
            in_page_index: 0,
            value_bits: 0,
        };
        assert_wins(created, BASE);

        // Creation time is signed: a record created before the epoch loses
        // to one created after it.
        assert_wins(
            DedupKey {
                created_unix_ns: 1,
                ..BASE
            },
            DedupKey {
                created_unix_ns: -1,
                ..BASE
            },
        );

        let epoch = DedupKey {
            writer_epoch: 11,
            writer_seq: 0,
            in_page_index: 0,
            value_bits: 0,
            ..BASE
        };
        assert_wins(epoch, BASE);

        let seq = DedupKey {
            writer_seq: 11,
            in_page_index: 0,
            value_bits: 0,
            ..BASE
        };
        assert_wins(seq, BASE);

        let in_page = DedupKey {
            in_page_index: 11,
            value_bits: 0,
            ..BASE
        };
        assert_wins(in_page, BASE);

        let value = DedupKey {
            value_bits: 11,
            ..BASE
        };
        assert_wins(value, BASE);

        // And each earlier field outranks the next one directly.
        assert_wins(created, epoch);
        assert_wins(epoch, seq);
        assert_wins(seq, in_page);
        assert_wins(in_page, value);
    }

    #[test]
    fn value_bits_decide_only_on_a_full_provenance_tie() {
        let priority = (5, 4, 3, 2);
        // A larger value never beats a later write.
        let later_write = DedupKey::new((5, 4, 3, 3), 1.0);
        let bigger_value = DedupKey::new(priority, f64::MAX);
        assert_wins(later_write, bigger_value);

        // On a full tie the bit pattern decides, not numeric order.
        let neg_zero = DedupKey::new(priority, -0.0);
        let pos_zero = DedupKey::new(priority, 0.0);
        assert_eq!((-0.0f64).to_bits(), 0x8000_0000_0000_0000);
        assert_wins(neg_zero, pos_zero);

        let nan_a = DedupKey::new(priority, f64::from_bits(0x7ff8_0000_0000_0001));
        let nan_b = DedupKey::new(priority, f64::from_bits(0x7ff8_0000_0000_0002));
        assert_wins(nan_b, nan_a);
        // A NaN payload outranks every positive finite value by bits,
        // whatever `f64` comparison would say.
        assert_wins(nan_a, DedupKey::new(priority, f64::MAX));
        assert_wins(neg_zero, nan_b);

        // Identical keys: neither is served over the other.
        assert!(!serves_over(&nan_a, &nan_a));
        assert!(!serves_over(&BASE, &BASE));
    }
}
