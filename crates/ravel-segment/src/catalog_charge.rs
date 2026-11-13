//! The memory a catalog decode materializes beyond the section bytes it
//! decompresses, charged before the decode allocates it.

use crate::reader::SampleProvenance;

/// The most bytes the four decoded `encode_i64` provenance columns (created
/// delta, writer epoch, writer seq, in-page index) hold per flagged sample:
/// twice each value's 8 bytes, since an RLE column's buffer grows by
/// doubling from a capped initial capacity (`ravel_codec::encoding::decode_i64`).
pub const PROVENANCE_COLUMN_BYTES_PER_SAMPLE: u64 = 4 * 2 * std::mem::size_of::<i64>() as u64;

/// Bytes one decoded [`SampleProvenance`] holds.
pub const SAMPLE_PROVENANCE_BYTES: u64 = std::mem::size_of::<SampleProvenance>() as u64;

/// A ledger for what a catalog decode allocates beyond its decompressed
/// sections: the per-sample provenance extension (ADR-0092 decision 1). Its
/// columns store a run-wide constant in a few bytes and decode to
/// [`PROVENANCE_COLUMN_BYTES_PER_SAMPLE`] plus [`SAMPLE_PROVENANCE_BYTES`] per
/// flagged sample, so a section-sized charge does not cover them.
///
/// The `_charged` catalog decoders call [`admit`](Self::admit) with each
/// figure before allocating it, and [`release`](Self::release) with the same
/// figure once those bytes are freed. What stays admitted when a decode
/// returns is the provenance of the entries it returned. A decode that fails
/// releases nothing: the caller drops whatever its ledger admitted.
pub trait CatalogCharge {
    /// Called before the decode allocates `bytes`. `false` stops the decode
    /// with [`SegmentError::CatalogChargeRefused`](crate::SegmentError::CatalogChargeRefused)
    /// before it allocates them.
    fn admit(&mut self, bytes: u64) -> bool;

    /// Called once the decode has freed `bytes` that one earlier
    /// [`admit`](Self::admit) call of exactly that figure admitted.
    fn release(&mut self, bytes: u64);
}

/// A [`CatalogCharge`] that admits everything: the plain catalog decoders.
#[derive(Debug, Clone, Copy, Default)]
pub struct Uncharged;

impl CatalogCharge for Uncharged {
    fn admit(&mut self, _bytes: u64) -> bool {
        true
    }

    fn release(&mut self, _bytes: u64) {}
}

/// The decoded [`SampleProvenance`] bytes `columns` hold.
pub(crate) fn provenance_bytes(columns: &[Option<Vec<SampleProvenance>>]) -> u64 {
    columns
        .iter()
        .flatten()
        .map(|column| (column.len() as u64).saturating_mul(SAMPLE_PROVENANCE_BYTES))
        .fold(0, u64::saturating_add)
}
