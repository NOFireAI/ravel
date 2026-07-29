//! The per-signal codec seam (ADR-0032).
//!
//! Everything the compactor does *around* the bytes -- seal detection
//! ([`crate::scan`]), the `CreateIfAbsent` + convergence + abandonment publish
//! protocol ([`crate::publish`]), the advisory cursor, the canonical
//! `input_set_hash`, the part-size cap ([`crate::config`]), and the whole
//! crash-recovery/idempotency story -- is signal-agnostic and stays shared.
//! The only two things that know a signal's on-object format are:
//!
//! 1. decoding one input object's footer/identity/catalog metadata far enough
//!    to plan a merge, without fetching page/block bytes yet; and
//! 2. streaming a k-way merge of N inputs' catalogs into size-capped output
//!    parts, fetching page/block bytes lazily during the merge.
//!
//! [`SegmentCodec`] is exactly those two operations, one associated
//! [`SegmentCodec::Catalog`] type carrying whatever metadata (1) retained for
//! (2) to consume. It deliberately covers nothing else: the record assembly,
//! key reconstruction, convergence repair, and size cap all live in the shared
//! modules and take a [`crate::bucket::Bucket`] whose `signal` field already
//! routes them. The plan's memory bound is on *decoded* data: catalog metadata
//! plus one in-flight part buffer's decoded records, never the whole bucket's
//! decoded data at once (`build.rs`/`read.rs` header comments). [`RsegCodec`]
//! holds this as a contract on the impl, not a hint: its ranged reader
//! (`ravel_segment::open_from_suffix`) fetches only the footer and the
//! page/block bytes a part actually needs. [`crate::rlog::RlogCodec`] is a
//! documented exception on the *raw-bytes* axis only: RLOG has no ranged
//! `.rlog` section reader, so `build_parts` GETs every input object whole and
//! holds all N raw buffers resident for the merge (see that module's "Memory"
//! note). It still bounds *decoded* data to one part plus one stream, which is
//! the part the plan's bound is about; the raw-bytes gap is tracked as a
//! follow-up (a ranged RLOG reader), not a silent violation of a claimed
//! universal contract.
//!
//! [`RsegCodec`] is a behavior-preserving thin wrapper over the existing
//! `read.rs`/`build.rs` RSEG logic; [`crate::rlog::RlogCodec`] is the RLOG
//! implementation. [`crate::compact::compact_bucket`] dispatches on
//! `bucket.signal` to one or the other and runs the identical shared pipeline
//! around whichever it picked.

use ravel_object_store::ObjectStoreBackend;

use crate::bucket::Bucket;
use crate::build::BuiltPart;
use crate::config::CompactorConfig;
use crate::error::Result;
use crate::read::InputRecord;

/// The format-specific half of the compactor, implemented once per signal.
///
/// The lint allow is intentional: this trait is a crate-internal seam invoked
/// only through the monomorphized dispatch in [`crate::compact`], never behind
/// `dyn`, so it needs no `Send`-bounded return future (which is the only thing
/// `async_fn_in_trait` warns about).
#[allow(async_fn_in_trait)]
pub trait SegmentCodec {
    /// Per-input decoded catalog metadata: enough to plan the merge (identity,
    /// stream/series directory), never the page/block bytes themselves.
    type Catalog;

    /// Decode one input object's footer/identity/catalog metadata (read.rs's
    /// job). MUST NOT retain page/block bytes: peak memory across all inputs is
    /// bounded by this metadata, not by the bucket's data.
    async fn load_input_catalog(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        input: &InputRecord,
    ) -> Result<Self::Catalog>;

    /// Stream-merge every input into size-capped [`BuiltPart`]s and PUT each
    /// one `CreateIfAbsent` (build.rs's job). `catalogs` is aligned one-to-one
    /// with `inputs` in canonical input order. Fetches page/block bytes lazily;
    /// peak *decoded* memory is bounded by catalog metadata plus one in-flight
    /// part. RSEG additionally bounds raw fetched bytes (ranged reader); RLOG
    /// is a documented exception that holds all inputs' raw bytes resident
    /// (no ranged `.rlog` reader), while still bounding decoded data as above
    /// -- see the trait doc above and [`crate::rlog::RlogCodec`].
    async fn build_parts(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        bucket: &Bucket,
        inputs: &[InputRecord],
        catalogs: &[Self::Catalog],
        input_set_hash: &[u8; 32],
    ) -> Result<Vec<BuiltPart>>;
}

/// The metrics codec: a behavior-preserving wrapper over the RSEG logic in
/// `read.rs`/`build.rs`. It adds nothing and changes nothing; it only routes
/// the two RSEG operations through the [`SegmentCodec`] seam so
/// [`crate::compact`] can dispatch on signal.
pub struct RsegCodec;

impl SegmentCodec for RsegCodec {
    type Catalog = crate::read::InputCatalog;

    async fn load_input_catalog(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        input: &InputRecord,
    ) -> Result<Self::Catalog> {
        crate::read::load_input_catalog(store, config, input).await
    }

    async fn build_parts(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        bucket: &Bucket,
        inputs: &[InputRecord],
        catalogs: &[Self::Catalog],
        input_set_hash: &[u8; 32],
    ) -> Result<Vec<BuiltPart>> {
        crate::build::build_parts(store, config, bucket, inputs, catalogs, input_set_hash).await
    }
}
