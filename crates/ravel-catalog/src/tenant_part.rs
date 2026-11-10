//! The tenant-checked part decode for snapshot-part readers that hold no
//! [`Catalog`].
//!
//! [`check_part_tenant`] and [`decode_part_for_tenant`] are the public,
//! counter-free primitives (no [`Catalog`] held, so nothing to charge a
//! budget against or count a breach on): the GC reachability sweep
//! (`ravel-maintain`'s `reachability.rs`) uses them directly. Readers that
//! hold a `Catalog` instead go through [`Catalog::decode_tenant_part`], which
//! calls [`check_part_tenant`] for its header step and additionally charges
//! the catalog's memory budget and counts the refusal on
//! `ravel_catalog_isolation_breach_total`: `seal_divergence` (the scrub's
//! seal-divergence check, `catalog verify`, and the load's coverage check)
//! and `covering_postings` (the scrub's covering-postings load). The fold
//! (`fold.rs`) and the query path's part load (`snapshot_resolve.rs`) keep
//! their own inline tenant comparisons and do not call these functions.

use ravel_proto::catalog::v1::SnapshotPartHeader;
use ravel_types::TenantHash;

use crate::catalog::Catalog;
use crate::snapshot_format::{self, DecodedPart, PartLimits, SnapshotFormatError};

/// Why a snapshot part was refused. Each reader maps this to its own error
/// enum, adding the part key.
#[derive(Debug)]
pub enum TenantPartError {
    /// The header or the body does not decode.
    Corrupt(SnapshotFormatError),
    /// The header names another tenant (ADR-0050 §2). Both are hex.
    ForeignTenant { expected: String, actual: String },
    /// The catalog's memory budget refused the part's declared decoded size
    /// (ADR-1170, catalog decode charge amendment). Nothing was decoded.
    MemoryExhausted(ravel_memory::MemoryExhausted),
}

/// Why [`check_part_tenant`] or [`decode_part_for_tenant`] refused a part:
/// the same two causes as [`TenantPartError`], minus the budget charge
/// neither function performs.
#[derive(Debug)]
pub enum PartTenantError {
    /// The header or the body does not decode.
    Corrupt(SnapshotFormatError),
    /// The header names another tenant (ADR-0050 §2). Both are hex. Checked
    /// before the body is decoded, so a foreign part's corrupted body still
    /// reports this, never [`PartTenantError::Corrupt`].
    ForeignTenant { expected: String, actual: String },
}

/// Decodes `bytes`' header only and refuses it when the header names a
/// tenant other than `tenant` (ADR-0050 §2), before any body bytes are
/// decoded. Counts nothing: the caller holds no [`Catalog`] to count a
/// breach on.
pub fn check_part_tenant(
    tenant: &TenantHash,
    bytes: &[u8],
) -> Result<SnapshotPartHeader, PartTenantError> {
    let header = snapshot_format::decode_part_header(bytes).map_err(PartTenantError::Corrupt)?;
    if header.tenant_hash.as_slice() != tenant.0.as_slice() {
        return Err(PartTenantError::ForeignTenant {
            expected: tenant.to_hex(),
            actual: hex::encode(&header.tenant_hash),
        });
    }
    Ok(header)
}

/// [`check_part_tenant`] followed by a full [`snapshot_format::decode_part`]
/// when the tenant check passes. Counts nothing and charges no memory
/// budget; a caller holding a [`Catalog`] should use
/// [`Catalog::decode_tenant_part`] instead.
pub fn decode_part_for_tenant(
    tenant: &TenantHash,
    bytes: &[u8],
    limits: &PartLimits,
) -> Result<DecodedPart, PartTenantError> {
    check_part_tenant(tenant, bytes)?;
    snapshot_format::decode_part(bytes, limits).map_err(PartTenantError::Corrupt)
}

impl Catalog {
    /// Decode a snapshot part's bytes for `tenant`, refusing a part whose
    /// header names another tenant before its entries are decoded, then
    /// charging the header's declared `entries_uncompressed_len` to this
    /// catalog's memory budget before the decode.
    ///
    /// The tenant refusal counts one isolation breach on this catalog, the
    /// counter `ravel_catalog_isolation_breach_total` renders, and is checked
    /// first, so a foreign part is reported as a breach whatever the budget
    /// holds. The returned reservation is the decoded entries' charge: the
    /// caller holds it for as long as it holds the entries.
    pub fn decode_tenant_part(
        &self,
        tenant: &TenantHash,
        bytes: &[u8],
    ) -> Result<(DecodedPart, ravel_memory::Reservation), TenantPartError> {
        let header = check_part_tenant(tenant, bytes).map_err(|err| match err {
            PartTenantError::Corrupt(err) => TenantPartError::Corrupt(err),
            PartTenantError::ForeignTenant { expected, actual } => {
                self.record_isolation_breach();
                TenantPartError::ForeignTenant { expected, actual }
            }
        })?;
        let limits = PartLimits::default();
        let reservation = self
            .reserve_decoded(
                header.entries_uncompressed_len,
                limits.max_snapshot_part_bytes,
            )
            .map_err(TenantPartError::MemoryExhausted)?;
        let decoded =
            snapshot_format::decode_part(bytes, &limits).map_err(TenantPartError::Corrupt)?;
        Ok((decoded, reservation))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_proto::catalog::v1::SnapshotEntry;

    use super::*;
    use crate::snapshot_format::encode_part;

    const TENANT_A: TenantHash = TenantHash([0xAA; 16]);
    const TENANT_B: TenantHash = TenantHash([0xBB; 16]);

    fn entry(seq: u64) -> SnapshotEntry {
        SnapshotEntry {
            level: 0,
            ingest_hour_bucket: 5,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: seq,
            content_hash: vec![seq as u8; 32],
            object_size: 100,
            ..SnapshotEntry::default()
        }
    }

    fn part_for(tenant: TenantHash) -> Vec<u8> {
        encode_part(tenant.0, 1, 1, 10, &[entry(1), entry(2)]).expect("encode")
    }

    /// Flips the last byte of the compressed body (the byte just before the
    /// 8-byte body_crc32c/header_crc32c trailer), corrupting the body
    /// without touching the header or either checksum's own bytes.
    fn corrupt_body(bytes: &mut [u8]) {
        let len = bytes.len();
        bytes[len - 9] ^= 0xFF;
    }

    /// A same-tenant part decodes its entries.
    ///
    /// FLIP: compare against the wrong field (e.g. always `Ok`, or compare
    /// `header.signal` instead of `header.tenant_hash`) and this fails to
    /// return the entries encoded above.
    #[test]
    fn same_tenant_decodes() {
        let bytes = part_for(TENANT_A);
        let decoded =
            decode_part_for_tenant(&TENANT_A, &bytes, &PartLimits::default()).expect("same tenant");
        assert_eq!(decoded.entries.len(), 2);
    }

    /// A part encoded for tenant B, read as tenant A, is refused as
    /// `ForeignTenant` naming both hashes, never silently accepted.
    ///
    /// FLIP: skip the tenant check (W2) and this returns `Ok` instead of
    /// `Err(ForeignTenant { .. })`.
    #[test]
    fn foreign_tenant_is_refused() {
        let bytes = part_for(TENANT_B);
        let err = decode_part_for_tenant(&TENANT_A, &bytes, &PartLimits::default())
            .expect_err("foreign tenant");
        match err {
            PartTenantError::ForeignTenant { expected, actual } => {
                assert_eq!(expected, TENANT_A.to_hex());
                assert_eq!(actual, TENANT_B.to_hex());
            }
            other => panic!("expected ForeignTenant, got {other:?}"),
        }
    }

    /// A foreign part whose body is then corrupted still reports
    /// `ForeignTenant`, not `Corrupt`: the tenant is checked before the body
    /// is ever decoded.
    ///
    /// FLIP (W1): decode the whole part first and compare the header
    /// afterward, so the corrupted foreign body surfaces its `Corrupt` error
    /// before the tenant comparison ever runs, and this `match` falls to
    /// `other` and panics.
    #[test]
    fn foreign_tenant_with_corrupted_body_still_reports_foreign_tenant() {
        let mut bytes = part_for(TENANT_B);
        corrupt_body(&mut bytes);
        let err = decode_part_for_tenant(&TENANT_A, &bytes, &PartLimits::default())
            .expect_err("foreign tenant, corrupted body");
        match err {
            PartTenantError::ForeignTenant { expected, actual } => {
                assert_eq!(expected, TENANT_A.to_hex());
                assert_eq!(actual, TENANT_B.to_hex());
            }
            other => panic!("expected ForeignTenant even with a corrupted body, got {other:?}"),
        }
    }

    /// A same-tenant part with a corrupted body fails the body decode, not
    /// the tenant check.
    #[test]
    fn same_tenant_with_corrupted_body_reports_corrupt() {
        let mut bytes = part_for(TENANT_A);
        corrupt_body(&mut bytes);
        let err = decode_part_for_tenant(&TENANT_A, &bytes, &PartLimits::default())
            .expect_err("corrupted body");
        assert!(
            matches!(err, PartTenantError::Corrupt(_)),
            "expected Corrupt, got {err:?}"
        );
    }
}
