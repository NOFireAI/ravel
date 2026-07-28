//! Read one L0 input segment (RSEG v1/v2/v3), verifying it against its
//! commit record's identity before trusting any of its bytes (ADR-0010
//! section 7), and expose its catalog for the k-way merge (plan section
//! 3.3 step 2).
//!
//! Fetches the whole object in one GET rather than suffix-probing the
//! footer first: compaction needs the TS/VAL/HIST page bytes of
//! (typically) every series in the input for verbatim copy, so a
//! two-step suffix-then-range fetch would not save a round trip in the
//! common case.

use bytes::Bytes;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::segment::v1::SectionKind;
use ravel_segment::{
    ExpectedIdentity, Footer, ReaderLimits, SegmentError, SeriesEntry, check_identity,
    decode_catalog, decode_catalog_v2, decode_catalog_v3, open_from_full,
};

use crate::error::MaintainError;
use crate::input::CompactionInput;

/// One decoded L0 segment, kept alongside its full object bytes (page
/// ranges from `entries` are sliced out of `bytes` verbatim during merge)
/// and the run provenance taken from its *commit record*, never its
/// footer (plan section 3.3 step 2: dedup priority is the commit
/// record's, since footer writer fields are only advisory).
pub struct DecodedInput {
    pub bytes: Bytes,
    pub footer: Footer,
    pub entries: Vec<SeriesEntry>,
    pub created_unix_ns: i64,
    pub writer_epoch: u64,
    pub writer_seq: u64,
}

fn section_slice<'a>(
    footer: &Footer,
    kind: SectionKind,
    bytes: &'a [u8],
) -> Result<&'a [u8], MaintainError> {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind as u32)
        .ok_or(SegmentError::MissingSection(section_name(kind)))?;
    let start = usize::try_from(section.offset).map_err(|_| SegmentError::SectionOutOfBounds)?;
    let len = usize::try_from(section.len).map_err(|_| SegmentError::SectionOutOfBounds)?;
    let end = start
        .checked_add(len)
        .ok_or(SegmentError::SectionOutOfBounds)?;
    bytes
        .get(start..end)
        .ok_or(MaintainError::Segment(SegmentError::SectionOutOfBounds))
}

fn section_name(kind: SectionKind) -> &'static str {
    match kind {
        SectionKind::LabelDict => "LABEL_DICT",
        SectionKind::SeriesTable => "SERIES_TABLE",
        SectionKind::SeriesIds => "SERIES_IDS",
        SectionKind::SeriesMeta => "SERIES_META",
        SectionKind::TsPages => "TS_PAGES",
        SectionKind::ValPages => "VAL_PAGES",
        SectionKind::HistPages => "HIST_PAGES",
        SectionKind::Unspecified => "UNSPECIFIED",
    }
}

fn decode_entries(
    footer: &Footer,
    version: u16,
    bytes: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<SeriesEntry>, MaintainError> {
    let label_dict = section_slice(footer, SectionKind::LabelDict, bytes)?;
    match version {
        1 => {
            let series_table = section_slice(footer, SectionKind::SeriesTable, bytes)?;
            Ok(decode_catalog(footer, label_dict, series_table, limits)?)
        }
        2 => {
            let series_ids = section_slice(footer, SectionKind::SeriesIds, bytes)?;
            let series_meta = section_slice(footer, SectionKind::SeriesMeta, bytes)?;
            Ok(decode_catalog_v2(
                footer,
                label_dict,
                series_ids,
                series_meta,
                limits,
            )?)
        }
        3 => {
            let series_ids = section_slice(footer, SectionKind::SeriesIds, bytes)?;
            let series_meta = section_slice(footer, SectionKind::SeriesMeta, bytes)?;
            Ok(decode_catalog_v3(
                footer,
                label_dict,
                series_ids,
                series_meta,
                limits,
            )?)
        }
        other => Err(MaintainError::UnsupportedInputVersion(other)),
    }
}

/// Fetch, verify, and decode one L0 compaction input. Identity
/// verification (`check_identity`) rejects a segment whose footer
/// disagrees with the commit record that named it, per ADR-0010 section
/// 7: a listed key or stored pointer is never trusted on its own.
pub async fn read_input(
    store: &dyn ObjectStoreBackend,
    input: &CompactionInput,
) -> Result<DecodedInput, MaintainError> {
    let record = &input.record;
    let outcome = store.get(&record.object_key, GetRange::Full).await?;
    let bytes = outcome.data;

    let limits = ReaderLimits::default();
    let loc = open_from_full(&bytes, limits)?;

    let tenant_hash: [u8; 16] = record.tenant_hash.as_slice().try_into().map_err(|_| {
        MaintainError::from(ravel_commit::record::RecordError::InvalidTenantHashLen(
            record.tenant_hash.len(),
        ))
    })?;
    let expected = ExpectedIdentity {
        tenant_hash,
        shard: record.shard,
        writer_id: record.writer_id.clone(),
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
    };
    check_identity(&loc.footer, &expected)?;

    if !(1..=3).contains(&loc.version) {
        return Err(MaintainError::UnsupportedInputVersion(loc.version));
    }
    let entries = decode_entries(&loc.footer, loc.version, &bytes, limits)?;

    Ok(DecodedInput {
        bytes,
        footer: loc.footer,
        entries,
        created_unix_ns: record.created_unix_ns,
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_commit::keys::commit_key_for_record;
    use ravel_commit::record::{NewCommitRecord, build};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{PutMode, PutOptions};
    use ravel_segment::{IngestBounds, SegmentIdentity, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
    use uuid::Uuid;

    fn tenant_hash() -> TenantHash {
        TenantHash([0x33; 16])
    }

    async fn put_v1_input(
        store: &MemoryStore,
        tenant_hash: &TenantHash,
        shard: u32,
        writer_id: Uuid,
    ) -> CompactionInput {
        let labels = LabelSet::new(vec![Label {
            name: "job".into(),
            value: "test".into(),
        }])
        .expect("labels");
        let series_id =
            SeriesId::compute(&TenantId::new("t"), "requests_total", &labels).expect("series id");
        let series = vec![SeriesInput {
            series_id,
            labels,
            samples: vec![Sample {
                ts_ns: 1_000,
                value: 42.0,
            }],
        }];
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 1_000,
            max_ingest_ts_ns: 1_000,
        };
        let written =
            ravel_segment::SegmentWriter::write(series, identity, bounds).expect("write v1");

        let record = build(NewCommitRecord {
            tenant_hash: *tenant_hash,
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: 1_000,
            max_ingest_ts_ns: 1_000,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        })
        .expect("valid record");

        store
            .put(
                &record.object_key,
                written.bytes,
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put segment object");
        let key = commit_key_for_record(&record).expect("commit key");
        store
            .put(
                &key,
                ravel_commit::record::encode(&record),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put commit record");

        CompactionInput { key, record }
    }

    #[tokio::test]
    async fn reads_and_decodes_a_v1_input() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let input = put_v1_input(&store, &th, 1, Uuid::new_v4()).await;

        let decoded = read_input(&store, &input).await.expect("read");
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.writer_epoch, 1);
        assert_eq!(decoded.writer_seq, 1);
        assert_eq!(decoded.created_unix_ns, 0);
    }

    #[tokio::test]
    async fn rejects_identity_mismatch_between_commit_record_and_footer() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let mut input = put_v1_input(&store, &th, 1, Uuid::new_v4()).await;
        // Tamper with the record's writer_seq after the segment was
        // written under a different identity: check_identity must reject
        // this rather than trust the (now-mismatched) pointer.
        input.record.writer_seq = 999;

        match read_input(&store, &input).await {
            Err(MaintainError::Segment(_)) => {}
            Ok(_) => panic!("expected identity mismatch to be rejected, got Ok"),
            Err(other) => panic!("expected a Segment identity error, got {other}"),
        }
    }
}
