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
//! decoded data at once (`build.rs`/`read.rs` header comments). RSEG and RLOG
//! hold this as a contract on the impl, not a hint, and additionally bound
//! *raw* fetched bytes: each has a ranged reader that reads the footer and
//! directory from a suffix probe and then fetches only the bytes a part
//! actually needs -- RSEG's `ravel_segment::open_from_suffix` fetching the
//! page bytes of one series' runs, RLOG's `ravel_logseg::open_from_suffix`
//! plus `RlogRangeReader` fetching the blocks of one stream (issue #275). For
//! those two, peak resident bytes -- decoded and raw -- are bounded by
//! catalog metadata plus one part plus one series/stream, never the whole
//! bucket. The earlier RLOG merge held every input object whole (RLOG then
//! had no ranged `.rlog` section reader); that gap is now closed.
//!
//! [`crate::rspan_codec::SpanCodec`] (RSPAN, spans) is the third
//! implementation and does NOT yet hold the same bound: `ravel-rspan` has no
//! ranged reader, so the codec fetches and decodes each input object whole
//! (raw bytes bounded to one input at a time; *decoded* records for the whole
//! bucket are held in memory across the merge). This is a named v1 tradeoff
//! (`rspan_codec.rs`'s own module doc), the same shape the RLOG merge had
//! before issue #275, not an oversight -- but it means the "bounded decoded
//! memory" sentence above is a two-codec-out-of-three contract today, not a
//! trait-wide guarantee. Closing it for RSPAN (a ranged reader over
//! `ravel-rspan`, mirroring `RlogRangeReader`) is a natural follow-up once
//! span bucket sizes in practice justify it.
//!
//! [`RsegCodec`] is a behavior-preserving thin wrapper over the existing
//! `read.rs`/`build.rs` RSEG logic; [`crate::rlog::RlogCodec`] is the RLOG
//! implementation; [`crate::rspan_codec::SpanCodec`] is the RSPAN
//! implementation. [`crate::compact::compact_bucket`] dispatches on
//! `bucket.signal` to whichever of the three it picked and runs the identical
//! shared pipeline around it.

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
    /// with `inputs` in canonical input order and is taken by value: this is
    /// the catalogs' last use, so a codec may move records out of them rather
    /// than clone (RSEG moves exemplars, issue #557). RSEG and RLOG fetch page/block
    /// bytes lazily by range, bounding peak *decoded* memory to catalog
    /// metadata plus one in-flight part, and additionally bound raw fetched
    /// bytes via their ranged readers (RSEG's `open_from_suffix`, RLOG's
    /// `RlogRangeReader`, issue #275) to one part plus one series/stream -- see
    /// the trait doc above and [`crate::rlog::RlogCodec`]. RSPAN
    /// ([`crate::rspan_codec::SpanCodec`]) fetches each input whole (raw bytes
    /// bounded to one input, decoded records unbounded across the merge) --
    /// see the trait doc's RSPAN paragraph.
    async fn build_parts(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        bucket: &Bucket,
        inputs: &[InputRecord],
        catalogs: Vec<Self::Catalog>,
        input_set_hash: &[u8; 32],
    ) -> Result<Vec<BuiltPart>>;

    /// Reject an input set [`crate::rewrite::rewrite_and_publish`] must not
    /// run over, ahead of any decode or PUT. The default accepts everything:
    /// a codec whose `build_parts` genuinely decodes and re-encodes bytes
    /// (RLOG, RSPAN) has nothing to reject on version grounds. A codec that
    /// only copies bytes verbatim without decoding them (RSEG, `build.rs`'s
    /// module doc) must override this to refuse an input recorded below the
    /// codec's current output version, since copying-verbatim from an older
    /// format is not a format migration, it's a mislabeled object.
    fn validate_rewrite_inputs(_inputs: &[InputRecord]) -> Result<()> {
        Ok(())
    }
}

/// The metrics codec: a behavior-preserving wrapper over the RSEG logic in
/// `read.rs`/`build.rs`. It adds nothing and changes nothing; it only routes
/// the two RSEG operations through the [`SegmentCodec`] seam so
/// [`crate::compact`] can dispatch on signal.
pub struct RsegCodec;

impl SegmentCodec for RsegCodec {
    type Catalog = crate::read::InputCatalog;

    fn validate_rewrite_inputs(inputs: &[InputRecord]) -> Result<()> {
        use crate::build::OUTPUT_FORMAT_VERSION;
        for input in inputs {
            if input.record.segment_format_version != OUTPUT_FORMAT_VERSION {
                return Err(crate::error::MaintainError::Invariant(format!(
                    "RSEG rewrite input {:?} is recorded as format version {}, current output version is {}: build_parts copies pages verbatim without decoding, so it cannot migrate an older RSEG object -- this input needs a real decode-and-re-encode path (ADR-0066), not this primitive",
                    input.commit_key, input.record.segment_format_version, OUTPUT_FORMAT_VERSION
                )));
            }
        }
        Ok(())
    }

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
        catalogs: Vec<Self::Catalog>,
        input_set_hash: &[u8; 32],
    ) -> Result<Vec<BuiltPart>> {
        crate::build::build_parts(store, config, bucket, inputs, catalogs, input_set_hash).await
    }
}
