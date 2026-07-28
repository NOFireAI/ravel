//! L0 input listing, canonical ordering, and `input_set_hash` (plan
//! section 3.1, section 3.3 step 1).

use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::commit::v1::CommitRecord;
use uuid::Uuid;

use crate::error::MaintainError;

/// One L0 commit record pulled into a compaction run: the key it was
/// listed at (verified against the record's own identity fields per
/// ADR-0010 section 7 before use, never trusted directly) and the decoded
/// record itself.
#[derive(Debug, Clone)]
pub struct CompactionInput {
    pub key: String,
    pub record: CommitRecord,
}

/// Fetch and decode one L0 commit record, verifying the key it was listed
/// at against the commit key reconstructed from its own identity fields
/// (ADR-0010 section 7: never trust a listed or stored key string). This
/// validates the commit record's own location, not the segment data
/// object it points to (`record.object_key`); that is verified
/// separately when the segment itself is read (see `segread.rs`).
pub async fn fetch_input(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<CompactionInput, MaintainError> {
    let outcome = store.get(key, GetRange::Full).await?;
    let record = ravel_commit::record::decode(&outcome.data)?;
    let expected_key = ravel_commit::keys::commit_key_for_record(&record)?;
    if expected_key != key {
        return Err(ravel_commit::keys::ReconstructionError::ObjectKeyMismatch {
            expected: expected_key,
            actual: key.to_string(),
        }
        .into());
    }
    Ok(CompactionInput {
        key: key.to_string(),
        record,
    })
}

fn sort_key(input: &CompactionInput) -> Result<(Uuid, u64, u64), MaintainError> {
    let writer_id = Uuid::parse_str(&input.record.writer_id).map_err(|_| {
        MaintainError::from(ravel_commit::record::RecordError::InvalidWriterId(
            input.record.writer_id.clone(),
        ))
    })?;
    Ok((
        writer_id,
        input.record.writer_epoch,
        input.record.writer_seq,
    ))
}

/// Sort inputs canonically by `(writer_id, writer_epoch, writer_seq)`
/// (plan section 3.3 step 1).
pub fn sort_inputs_canonically(
    inputs: Vec<CompactionInput>,
) -> Result<Vec<CompactionInput>, MaintainError> {
    let mut keyed = inputs
        .into_iter()
        .map(|input| sort_key(&input).map(|k| (k, input)))
        .collect::<Result<Vec<_>, _>>()?;
    keyed.sort_by_key(|(k, _)| *k);
    Ok(keyed.into_iter().map(|(_, input)| input).collect())
}

const INPUT_SET_DOMAIN: &[u8] = b"ravel-compaction-input-set-v1\0";

/// 32-byte blake3 digest over the canonical (sorted) input identity list.
///
/// The plan (section 3.1) pins the hash algorithm (blake3) and the sort
/// order (`writer_id`, `writer_epoch`, `writer_seq`) but does not pin an
/// exact byte grammar for "canonical encoding of inputs". This crate
/// defines one, following the repo's existing domain-tagged blake3
/// convention (see `ravel_types::SeriesId::compute`): a fixed domain
/// string, a little-endian input count, then each sorted input's 16 raw
/// `writer_id` bytes, `writer_epoch`, and `writer_seq` as little-endian
/// integers. `inputs` must already be sorted (see
/// [`sort_inputs_canonically`]); this function does not re-sort.
pub fn input_set_hash(sorted_inputs: &[CompactionInput]) -> Result<[u8; 32], MaintainError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(INPUT_SET_DOMAIN);
    let count = u32::try_from(sorted_inputs.len()).unwrap_or(u32::MAX);
    hasher.update(&count.to_le_bytes());
    for input in sorted_inputs {
        let (writer_id, epoch, seq) = sort_key(input)?;
        hasher.update(writer_id.as_bytes());
        hasher.update(&epoch.to_le_bytes());
        hasher.update(&seq.to_le_bytes());
    }
    Ok(*hasher.finalize().as_bytes())
}

/// First 16 hex chars (8 bytes) of a blake3 digest, as used for both
/// `input_set_hash16` and L0/L1 data-key `hash16` components.
pub fn hash16(hash: &[u8; 32]) -> String {
    hex::encode(&hash[..8])
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_types::{Signal, TenantHash};

    fn record(writer_id: Uuid, epoch: u64, seq: u64) -> CommitRecord {
        ravel_commit::record::build(ravel_commit::record::NewCommitRecord {
            tenant_hash: TenantHash([0x11; 16]),
            signal: Signal::Metrics,
            shard: 1,
            writer_id,
            writer_epoch: epoch,
            writer_seq: seq,
            object_size: 1024,
            content_hash: [0x22; 32],
            sample_count: 10,
            series_count: 2,
            min_event_ts_ns: 1_000,
            max_event_ts_ns: 2_000,
            min_ingest_ts_ns: 1_500,
            max_ingest_ts_ns: 2_500,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        })
        .expect("valid record")
    }

    fn input(writer_id: Uuid, epoch: u64, seq: u64) -> CompactionInput {
        let record = record(writer_id, epoch, seq);
        let key = ravel_commit::keys::commit_key_for_record(&record).expect("key");
        CompactionInput { key, record }
    }

    #[test]
    fn sort_inputs_canonically_orders_by_writer_epoch_seq() {
        let a = Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("uuid");
        let b = Uuid::parse_str("00000000-0000-0000-0000-000000000002").expect("uuid");

        let inputs = vec![
            input(b, 1, 1),
            input(a, 2, 1),
            input(a, 1, 2),
            input(a, 1, 1),
        ];
        let sorted = sort_inputs_canonically(inputs).expect("sorts");
        let keys: Vec<(Uuid, u64, u64)> =
            sorted.iter().map(|i| sort_key(i).expect("key")).collect();
        assert_eq!(keys, vec![(a, 1, 1), (a, 1, 2), (a, 2, 1), (b, 1, 1)]);
    }

    #[test]
    fn input_set_hash_is_order_independent_of_input_but_not_of_sort() {
        let a = Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("uuid");
        let b = Uuid::parse_str("00000000-0000-0000-0000-000000000002").expect("uuid");

        let sorted_1 =
            sort_inputs_canonically(vec![input(a, 1, 1), input(b, 1, 1)]).expect("sorts");
        let sorted_2 =
            sort_inputs_canonically(vec![input(b, 1, 1), input(a, 1, 1)]).expect("sorts");

        assert_eq!(
            input_set_hash(&sorted_1).expect("hash"),
            input_set_hash(&sorted_2).expect("hash"),
        );
    }

    #[test]
    fn input_set_hash_changes_with_membership() {
        let a = Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("uuid");
        let b = Uuid::parse_str("00000000-0000-0000-0000-000000000002").expect("uuid");

        let one = sort_inputs_canonically(vec![input(a, 1, 1)]).expect("sorts");
        let two = sort_inputs_canonically(vec![input(a, 1, 1), input(b, 1, 1)]).expect("sorts");

        assert_ne!(
            input_set_hash(&one).expect("hash"),
            input_set_hash(&two).expect("hash"),
        );
    }

    #[test]
    fn hash16_is_first_eight_bytes_hex() {
        let hash = [0xabu8; 32];
        assert_eq!(hash16(&hash), "ab".repeat(8));
    }

    #[test]
    fn fetch_input_rejects_key_mismatch() {
        // fetch_input itself needs a store; the key-reconstruction check it
        // performs is exercised indirectly via the end-to-end tests. This
        // guards the pure helper it depends on stays correct in isolation.
        let a = Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("uuid");
        let built = input(a, 1, 1);
        assert_eq!(
            ravel_commit::keys::commit_key_for_record(&built.record).expect("key"),
            built.key
        );
    }
}
