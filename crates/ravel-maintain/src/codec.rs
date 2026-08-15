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
//! plus `RlogRangeReader` fetching the blocks of one stream. For
//! those two, peak resident bytes -- decoded and raw -- are bounded by
//! catalog metadata plus one part plus one series/stream, never the whole
//! bucket. The earlier RLOG merge held every input object whole (RLOG then
//! had no ranged `.rlog` section reader); that gap is now closed.
//!
//! [`crate::rspan_codec::SpanCodec`] (RSPAN, spans) is the third
//! implementation and now holds the same bound: `ravel-rspan`'s `RspanRangeReader` fetches SKIP_IDX by
//! range at catalog load, then the merge streams BLOCKS bytes one block per
//! input at a time (`rspan_codec.rs`'s own module doc). So "bounded decoded
//! memory" is now a trait-wide guarantee across all three codecs, not a
//! two-out-of-three contract.
//!
//! [`RsegCodec`] wraps the existing `read.rs`/`build.rs` RSEG logic: the
//! verbatim page-copy merge for current-version inputs, plus ADR-0066 decision
//! 5's decode-and-re-encode path for an input recorded below the current output
//! version; [`crate::rlog::RlogCodec`] is the RLOG
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
    /// than clone (RSEG moves exemplars). RSEG and RLOG fetch page/block
    /// bytes lazily by range, bounding peak *decoded* memory to catalog
    /// metadata plus one in-flight part, and additionally bound raw fetched
    /// bytes via their ranged readers (RSEG's `open_from_suffix`, RLOG's
    /// `RlogRangeReader`; RSPAN's `RspanRangeReader`)
    /// to one part plus one series/stream/block-per-input -- see the trait doc
    /// above, [`crate::rlog::RlogCodec`], and [`crate::rspan_codec::SpanCodec`].
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
    /// every codec's `build_parts` now decodes and re-encodes an input recorded
    /// below its current output version (RLOG and RSPAN always re-encode; RSEG
    /// gained the decode-and-re-encode path in ADR-0066 decision 5), so there is
    /// nothing to reject on the low end. A codec MAY override this to fail closed
    /// on a version it cannot handle -- notably one recorded *newer* than the
    /// current output version (ADR-0066 decision 2: newer-than-known is a typed
    /// refusal, never a silent downgrade), which [`RsegCodec`] does.
    fn validate_rewrite_inputs(_inputs: &[InputRecord]) -> Result<()> {
        Ok(())
    }
}

/// The metrics codec: the RSEG half of the seam over `read.rs`/`build.rs`. For
/// current-version inputs it is a behavior-preserving wrapper (the verbatim
/// page-copy merge, unchanged). For an input recorded below the current output
/// version it drives ADR-0066 decision 5's decode-and-re-encode rewrite: it
/// marks that input's object key so [`crate::build::build_parts`] decodes and
/// re-encodes its runs at the current version instead of copying them verbatim.
pub struct RsegCodec;

impl SegmentCodec for RsegCodec {
    type Catalog = crate::read::InputCatalog;

    fn validate_rewrite_inputs(inputs: &[InputRecord]) -> Result<()> {
        use crate::build::OUTPUT_FORMAT_VERSION;
        // Fail closed on newer-than-writable (ADR-0066 decision 2): an input
        // recorded ABOVE the current output version is a format this build
        // cannot write, so a rewrite would have to mislabel it downward. Refuse
        // it before any decode or PUT. An input recorded at or below the output
        // version is migratable forward -- `build_parts` copies a current-version
        // input verbatim and decodes-and-re-encodes an older one -- so it is
        // accepted here; whether an older object's BYTES are actually decodable
        // is the reader's `SUPPORTED_VERSIONS` window to enforce at open time,
        // not this metadata-only pre-check's. This is the flipped guard: the
        // pre-slice-B version refused any input recorded `!= OUTPUT_FORMAT_VERSION`
        // (older included), because RSEG could only copy pages verbatim.
        for input in inputs {
            let version = input.record.segment_format_version;
            if version > OUTPUT_FORMAT_VERSION {
                return Err(crate::error::MaintainError::Invariant(format!(
                    "RSEG rewrite input {:?} is recorded as format version {version}, newer than \
                     the current output version {OUTPUT_FORMAT_VERSION}: a rewrite migrates an \
                     older object forward and cannot write a version this build does not know, so \
                     a newer-than-writable input is refused fail-closed (ADR-0066 decision 2)",
                    input.commit_key
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
        use std::collections::HashSet;

        use crate::build::OUTPUT_FORMAT_VERSION;

        // The object keys of any input recorded below the current output version
        // (ADR-0066 decision 5). `catalogs` is aligned one-to-one with `inputs`
        // in canonical input order (rewrite_and_publish builds it that way, and
        // build_parts re-checks the lengths match), so an input's recorded
        // version pairs with its catalog's object key. build_parts decodes and
        // re-encodes exactly these inputs' runs; every other input keeps the
        // verbatim page-copy path. Normal compaction (every input at the current
        // version) yields an empty set, so the fast path runs unchanged.
        let migrate_keys: HashSet<String> = inputs
            .iter()
            .zip(&catalogs)
            .filter(|(input, _)| input.record.segment_format_version < OUTPUT_FORMAT_VERSION)
            .map(|(_, catalog)| catalog.object_key.clone())
            .collect();
        crate::build::build_parts(
            store,
            config,
            bucket,
            inputs,
            catalogs,
            &migrate_keys,
            input_set_hash,
        )
        .await
    }
}
