//! Identity verification: after resolving a segment's footer, callers must
//! check it against the commit record that pointed at it (ADR-0010 §7).
//! Readers never trust `CommitRecord.object_key`; they reconstruct the key
//! from record fields, and verify the footer's identity fields match the
//! same record's after the suffix GET.

use ravel_proto::segment::v1::Footer;

use crate::error::SegmentError;

/// Segment identity fields as resolved from the commit record, to be
/// checked against the footer of the segment it points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedIdentity {
    pub tenant_hash: [u8; 16],
    pub shard: u32,
    pub writer_id: String,
    pub writer_epoch: u64,
    pub writer_seq: u64,
}

/// Verifies `footer`'s identity fields match `expected` exactly. Returns
/// [`SegmentError::IdentityMismatch`] naming the first mismatching field.
pub fn check_identity(footer: &Footer, expected: &ExpectedIdentity) -> Result<(), SegmentError> {
    if footer.tenant_hash.as_slice() != expected.tenant_hash.as_slice() {
        return Err(SegmentError::IdentityMismatch("tenant_hash"));
    }
    if footer.shard != expected.shard {
        return Err(SegmentError::IdentityMismatch("shard"));
    }
    if footer.writer_id != expected.writer_id {
        return Err(SegmentError::IdentityMismatch("writer_id"));
    }
    if footer.writer_epoch != expected.writer_epoch {
        return Err(SegmentError::IdentityMismatch("writer_epoch"));
    }
    if footer.writer_seq != expected.writer_seq {
        return Err(SegmentError::IdentityMismatch("writer_seq"));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn base_footer() -> Footer {
        Footer {
            tenant_hash: vec![7u8; 16],
            shard: 3,
            writer_id: "writer-a".to_string(),
            writer_epoch: 5,
            writer_seq: 9,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
            sample_count: 0,
            series_count: 0,
            sections: vec![],
            ..Default::default()
        }
    }

    fn base_expected() -> ExpectedIdentity {
        ExpectedIdentity {
            tenant_hash: [7u8; 16],
            shard: 3,
            writer_id: "writer-a".to_string(),
            writer_epoch: 5,
            writer_seq: 9,
        }
    }

    #[test]
    fn matching_identity_passes() {
        assert_eq!(check_identity(&base_footer(), &base_expected()), Ok(()));
    }

    #[test]
    fn tenant_hash_mismatch_is_rejected() {
        let mut expected = base_expected();
        expected.tenant_hash = [9u8; 16];
        assert_eq!(
            check_identity(&base_footer(), &expected),
            Err(SegmentError::IdentityMismatch("tenant_hash"))
        );
    }

    #[test]
    fn shard_mismatch_is_rejected() {
        let mut expected = base_expected();
        expected.shard = 4;
        assert_eq!(
            check_identity(&base_footer(), &expected),
            Err(SegmentError::IdentityMismatch("shard"))
        );
    }

    #[test]
    fn writer_id_mismatch_is_rejected() {
        let mut expected = base_expected();
        expected.writer_id = "writer-b".to_string();
        assert_eq!(
            check_identity(&base_footer(), &expected),
            Err(SegmentError::IdentityMismatch("writer_id"))
        );
    }

    #[test]
    fn epoch_mismatch_is_rejected() {
        let mut expected = base_expected();
        expected.writer_epoch = 6;
        assert_eq!(
            check_identity(&base_footer(), &expected),
            Err(SegmentError::IdentityMismatch("writer_epoch"))
        );
    }

    #[test]
    fn seq_mismatch_is_rejected() {
        let mut expected = base_expected();
        expected.writer_seq = 10;
        assert_eq!(
            check_identity(&base_footer(), &expected),
            Err(SegmentError::IdentityMismatch("writer_seq"))
        );
    }
}
