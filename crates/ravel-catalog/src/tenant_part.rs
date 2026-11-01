//! The tenant-checked part decode shared by the snapshot-part readers outside
//! the fold and the query path: `seal_divergence` (the scrub's seal-divergence
//! check, `catalog verify`, and the load's coverage check) and
//! `covering_postings` (the scrub's covering-postings load).

use ravel_types::TenantHash;

use crate::catalog::Catalog;
use crate::snapshot_format::{self, DecodedPart, PartLimits, SnapshotFormatError};

/// Why [`Catalog::decode_tenant_part`] refused a part. Each reader maps this
/// to its own error enum, adding the part key.
#[derive(Debug)]
pub(crate) enum TenantPartError {
    /// The header or the body does not decode.
    Corrupt(SnapshotFormatError),
    /// The header names another tenant (ADR-0050 §2). Already counted on the
    /// catalog's isolation-breach counter. Both are hex.
    ForeignTenant { expected: String, actual: String },
}

impl Catalog {
    /// Decode a snapshot part's bytes for `tenant`, refusing a part whose
    /// header names another tenant before its entries are decoded.
    ///
    /// The refusal counts one isolation breach on this catalog, the counter
    /// `ravel_catalog_isolation_breach_total` renders.
    pub(crate) fn decode_tenant_part(
        &self,
        tenant: &TenantHash,
        bytes: &[u8],
    ) -> Result<DecodedPart, TenantPartError> {
        let header =
            snapshot_format::decode_part_header(bytes).map_err(TenantPartError::Corrupt)?;
        if header.tenant_hash.as_slice() != tenant.0.as_slice() {
            self.record_isolation_breach();
            return Err(TenantPartError::ForeignTenant {
                expected: tenant.to_hex(),
                actual: hex::encode(&header.tenant_hash),
            });
        }
        snapshot_format::decode_part(bytes, &PartLimits::default())
            .map_err(TenantPartError::Corrupt)
    }
}
