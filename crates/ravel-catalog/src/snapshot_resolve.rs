//! Snapshot-backed resolve (docs/metric-index-plan.md 5.1/5.3, ADR-0020).
//!
//! Reads HEAD through a TTL cache and its parts through an immutable
//! per-key cache, serves window hours at or below the watermark from part
//! entries, and leaves hours above the watermark to Phase 1 listing. Every
//! failure (HEAD absent or corrupt, part missing, hash mismatch, decode
//! error) degrades to `Ok(None)`, telling the caller to fall back to full
//! listing: this module can only ever make a query faster, never make it
//! return wrong data. Two mismatches are the loud exception, surfaced as a
//! hard `CatalogError::FieldMismatch` instead of a degrade: `shard_count`
//! (this catalog's own config disagrees with the index it is about to
//! trust), and, per ADR-0050 §2, `tenant_hash` on the HEAD or a postings
//! object (an isolation breach, never silently absorbed into a fallback
//! that could serve or reference foreign bytes).

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use ravel_commit::{keys, signal};
use ravel_object_store::{GetRange, StoreError};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::error::CatalogError;
use crate::fold::head_object_key;
use crate::provisioning::{ShardGeneration, shard_ceiling};
use crate::snapshot::{SegmentLevel, SegmentRef};
use crate::snapshot_format::{self, DecodedPart, DecodedPostings, PartLimits, PostingsLimits};

/// Leading sentinel that marks a `name_filter` string as a literal-prefix
/// range key rather than an exact `__name__` value (ADR-0061 decision 3,
/// EF-4/#724).
///
/// The postings-pruning pipeline threads a single `Option<&str>` filter from
/// each query language down through `Catalog::resolve_pruned` (which is out of
/// this change's scope and keeps its signature) into
/// [`SnapshotWindow::extract_into`]. To carry the new "literal-prefix-anchored
/// regex" case alongside the pre-existing exact-equality case without changing
/// that signature, the two producers (`ravel_query::engine` and
/// `ravel_sql::executor`) encode a prefix filter as this sentinel followed by
/// the literal prefix; an exact filter is passed verbatim, unchanged, so the
/// public `resolve_pruned` contract and every existing caller are untouched.
///
/// `U+0001` (Start Of Heading) is used because it is a control character that
/// no supported ingest path admits into a stored `__name__`. That property is
/// only a defence-in-depth convenience, not the correctness argument: the
/// prefix path in [`SnapshotWindow::postings_ordinals_for_filter`] additionally
/// unions the ordinals of any stored name equal to the *entire* encoded string,
/// so even a (nonexistent in practice) exact `__name__` value that literally
/// began with this byte could never have its segments pruned away. Pruning
/// therefore can never drop a segment a query could match, regardless of what
/// names a snapshot happens to hold -- the false-negative class this ticket
/// exists to prevent.
///
/// The two producers duplicate this constant's value inline (matching this
/// codebase's language-specific-enforcement precedent, #278 and ADR-0061
/// decision 1); [`PREFIX_FILTER_SENTINEL`]'s own value is pinned by a test so a
/// silent drift is caught.
pub(crate) const PREFIX_FILTER_SENTINEL: char = '\u{1}';

/// A usable snapshot: its watermark and every part HEAD named, already
/// verified and decoded.
pub(crate) struct SnapshotWindow {
    pub(crate) watermark_hour: u32,
    parts: Vec<Arc<DecodedPart>>,
    /// Decoded, part-bound name postings (P5b, docs/metric-index-plan.md
    /// 5.4), or `None` when postings are absent, unreadable, corrupt, or
    /// don't cleanly bind to `parts`. Always safe to treat as absent:
    /// postings are a pure pruning optimization, never a correctness
    /// dependency (`extract_into` falls back to including every entry).
    postings: Option<Arc<DecodedPostings>>,
}

impl SnapshotWindow {
    /// Extract entries for `[lower_hour, upper_hour]` (inclusive) from every
    /// part, filtered by event-time overlap with `query_range` exactly as
    /// `Catalog::list_hour_bucket` does, deduped into `out` by data key
    /// (docs/metric-index-plan.md 5.1 step 5). Entries are sorted
    /// hour-major within each part (docs/metric-index-plan.md 3.1), so the
    /// matching hour range is one contiguous slice found by
    /// `partition_point`.
    ///
    /// `name_filter`, when `Some`, is the query's `__name__` pruning key (P5b,
    /// ADR-0061 decision 3): either an exact equality value, or -- when it
    /// begins with [`PREFIX_FILTER_SENTINEL`] -- a literal prefix from a
    /// prefix-anchored `__name__` regex (`^foo.*$`). Either way, entries this
    /// snapshot's postings provably do not carry a matching name are skipped
    /// before the event-overlap check, and `*pruned` is incremented once per
    /// skipped entry (see [`Self::postings_ordinals_for_filter`]). Pruning only
    /// ever activates when
    /// postings are present, decoded, and bound to exactly the parts this
    /// window holds; any other case (`name_filter` is `None`, postings
    /// absent/corrupt, or more than one covered part, which today's
    /// single-part fold never produces but a future compaction phase might)
    /// falls back to considering every entry, exactly as `Catalog::resolve`
    /// always has. This can only ever narrow the result set matched by
    /// `query_range`, never widen it: exact semantics by default.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn extract_into(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        lower_hour: u32,
        upper_hour: u32,
        query_range: &TimeRange,
        name_filter: Option<&str>,
        pruned: &mut u64,
        out: &mut HashMap<String, SegmentRef>,
    ) -> Result<(), CatalogError> {
        let ordinals = self.postings_ordinals_for_filter(name_filter);
        for part in &self.parts {
            let entries = &part.entries;
            let start = entries.partition_point(|e| e.ingest_hour_bucket < lower_hour);
            let end = entries.partition_point(|e| e.ingest_hour_bucket <= upper_hour);
            match ordinals.as_deref() {
                None => {
                    for entry in &entries[start..end] {
                        self.maybe_insert(tenant, signal, entry, query_range, out)?;
                    }
                }
                Some(ords) => {
                    let lo = ords.partition_point(|&o| (o as usize) < start);
                    let hi = ords.partition_point(|&o| (o as usize) < end);
                    *pruned += ((end - start) - (hi - lo)) as u64;
                    for &ordinal in &ords[lo..hi] {
                        self.maybe_insert(
                            tenant,
                            signal,
                            &entries[ordinal as usize],
                            query_range,
                            out,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn maybe_insert(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        entry: &SnapshotEntry,
        query_range: &TimeRange,
        out: &mut HashMap<String, SegmentRef>,
    ) -> Result<(), CatalogError> {
        let event_range = TimeRange {
            start_ns: entry.min_event_ts_ns,
            end_ns: entry.max_event_ts_ns,
        };
        if !event_range.overlaps(query_range) {
            return Ok(());
        }
        let segment_ref = build_segment_ref_from_entry(tenant, signal, entry)?;
        out.entry(segment_ref.data_object_key.clone())
            .or_insert(segment_ref);
        Ok(())
    }

    /// Resolves `name_filter` to the sorted ordinal list of entries carrying
    /// that name, or `None` if pruning cannot safely apply (no filter, no
    /// usable postings, or more than one covered part). `Some(&[])` is a
    /// legitimate result: the name simply does not appear in this snapshot
    /// at all, so every candidate entry is pruned.
    fn postings_ordinals_for(&self, name_filter: Option<&str>) -> Option<&[u64]> {
        let name = name_filter?;
        if self.parts.len() != 1 {
            return None;
        }
        let postings = self.postings.as_ref()?;
        match postings
            .names
            .binary_search_by(|np| np.name.as_str().cmp(name))
        {
            Ok(idx) => Some(postings.names[idx].ordinals.as_slice()),
            Err(_) => Some(&[]),
        }
    }

    /// Resolves `name_filter` (as threaded through `Catalog::resolve_pruned`)
    /// to the sorted ordinal list of every entry a matching name carries,
    /// dispatching on the [`PREFIX_FILTER_SENTINEL`] encoding (ADR-0061
    /// decision 3):
    ///
    /// - `None` / an unencoded string -> the pre-existing exact-equality
    ///   lookup ([`Self::postings_ordinals_for`]), returned borrowed.
    /// - a string led by [`PREFIX_FILTER_SENTINEL`] -> the literal-prefix
    ///   range scan ([`Self::postings_prefix_ordinals_for`]) over the sentinel-
    ///   stripped prefix, returned owned.
    ///
    /// The `None` vs `Some(&[])` contract is identical to
    /// [`Self::postings_ordinals_for`]: `None` means pruning cannot safely
    /// apply at all (no filter, no usable postings, or more than one covered
    /// part), and `Some(empty)` means the filter genuinely matches zero names
    /// in this snapshot (so every candidate entry is pruned).
    ///
    /// The prefix arm additionally unions the ordinals of any stored name
    /// exactly equal to the *whole* encoded string. In normal operation that
    /// name cannot exist (a stored `__name__` never begins with the control
    /// sentinel), so the union is empty and pruning is exactly the prefix
    /// range. It exists solely so that if a caller's *exact* filter value ever
    /// coincidentally began with the sentinel byte and were misread here as a
    /// prefix, that value's own segments are still retained -- making a
    /// dropped-match (false-negative) impossible independent of any ingest
    /// name-charset guarantee.
    fn postings_ordinals_for_filter(&self, name_filter: Option<&str>) -> Option<Cow<'_, [u64]>> {
        let filter = name_filter?;
        match filter.strip_prefix(PREFIX_FILTER_SENTINEL) {
            None => self.postings_ordinals_for(Some(filter)).map(Cow::Borrowed),
            Some(prefix) => {
                let mut ords = self.postings_prefix_ordinals_for(prefix)?;
                // Defence-in-depth union; see the doc comment above. When the
                // prefix arm applies (single part, usable postings), the exact
                // lookup is `Some` too, so this never turns a prunable filter
                // into `None`.
                if let Some(exact) = self.postings_ordinals_for(Some(filter))
                    && !exact.is_empty()
                {
                    ords.extend_from_slice(exact);
                    ords.sort_unstable();
                    ords.dedup();
                }
                Some(Cow::Owned(ords))
            }
        }
    }

    /// Resolves a literal `prefix` to the sorted union of the ordinal lists of
    /// every name that starts with it, or `None` if pruning cannot safely
    /// apply (empty prefix, more than one covered part, or no usable
    /// postings). `Some(empty)` is a legitimate result: no name in this
    /// snapshot starts with `prefix`, so every candidate entry is pruned. The
    /// sibling of [`Self::postings_ordinals_for`] for ADR-0061 decision 3.
    ///
    /// `postings.names` is sorted ascending (the frozen postings invariant,
    /// `snapshot_format::postings`), so the names starting with `prefix` form
    /// one contiguous slice, located by two `partition_point`s -- never an
    /// over- or under-approximation. Because every entry carries exactly one
    /// `__name__`, the matched names' ordinal sets are disjoint; the union is
    /// their concatenation, sorted so [`Self::extract_into`]'s `partition_point`
    /// bounds hold (the `dedup` is belt-and-suspenders).
    ///
    /// An empty `prefix` is rejected (`None`, i.e. bypass) rather than treated
    /// as "match every name": a `.*`-only regex prunes nothing, so declining to
    /// prune is both correct and cheaper.
    fn postings_prefix_ordinals_for(&self, prefix: &str) -> Option<Vec<u64>> {
        if prefix.is_empty() {
            return None;
        }
        if self.parts.len() != 1 {
            return None;
        }
        let postings = self.postings.as_ref()?;
        let names = &postings.names;
        // Lower bound: first name that is not strictly less than `prefix`.
        let lo = names.partition_point(|np| np.name.as_str() < prefix);
        // Upper bound: first name that is >= `prefix` yet does NOT start with
        // it. For ascending-sorted names this predicate is monotone (every
        // name `< prefix` or `starts_with(prefix)` precedes every name that is
        // `> prefix` without the prefix), so `[lo, hi)` is exactly the
        // contiguous run of prefix matches.
        let hi = names.partition_point(|np| {
            let n = np.name.as_str();
            n < prefix || n.starts_with(prefix)
        });
        let mut ords: Vec<u64> = Vec::new();
        for np in &names[lo..hi] {
            ords.extend_from_slice(&np.ordinals);
        }
        ords.sort_unstable();
        ords.dedup();
        Some(ords)
    }
}

/// Outcome of loading every part a HEAD names.
enum PartLoadOutcome {
    Loaded(Vec<Arc<DecodedPart>>),
    /// A part GET returned `NotFound`: races GC of a just-superseded part
    /// (docs/metric-index-plan.md 5.1 step 2). The caller re-reads HEAD
    /// once and retries before falling back.
    NotFoundRace,
    Unusable,
    /// A part's own `header.tenant_hash` names a different tenant than the
    /// requester (ADR-0050 §2). Like the HEAD `tenant_hash` mismatch, this is
    /// an isolation breach, never absorbed into a listing fallback that could
    /// go on to serve the foreign part's bytes: the caller propagates it as a
    /// hard `CatalogError`.
    IsolationBreach(CatalogError),
}

impl Catalog {
    /// Resolve the current snapshot window for (tenant, signal), or `None`
    /// if no snapshot is usable right now (absent, corrupt, or its parts
    /// unreadable). Never returns an error for index-only failures: the
    /// index is a pure optimization (docs/metric-index-plan.md 5.1 step 2).
    ///
    /// `want_postings` gates the postings GET (#278 item 3): the postings
    /// object is only ever consulted to prune by an equality `__name__`
    /// filter, so a resolve with no such filter passes `false` and never
    /// fetches or decodes it. Passing `false` is equivalent to postings being
    /// absent, which `extract_into` already handles by considering every
    /// entry.
    /// Returns the resolved window (or `None` when no snapshot is usable) plus
    /// the generation history the head was ultimately validated against when a
    /// one-shot record re-read was performed (`Some(fresh)`), or `None` when
    /// the head validated under the caller's passed-in `generations`. The
    /// caller (`resolve_fanout`) must build its Phase 1 scan set from `fresh`
    /// whenever it is present, so the listing suffix is never scanned over a
    /// staler generation view than the one that validated the head (Finding 4).
    /// The re-read's fresher view is propagated even when the window itself
    /// turns out unusable, so a fall-back-to-listing pass still scans the
    /// correct range.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn resolve_snapshot_window(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        now_ns: i64,
        want_postings: bool,
        generations: &[ShardGeneration],
        accounting: &QueryAccounting,
    ) -> Result<(Option<SnapshotWindow>, Option<Vec<ShardGeneration>>), CatalogError> {
        let head_key = head_object_key(tenant, signal);
        let Some((head, revalidated)) = self
            .read_head(
                tenant,
                signal,
                &head_key,
                now_ns,
                false,
                generations,
                accounting,
            )
            .await?
        else {
            return Ok((None, None));
        };
        match self.load_snapshot_parts(tenant, &head, accounting).await {
            PartLoadOutcome::Loaded(parts) => {
                let postings = if want_postings {
                    self.load_snapshot_postings(tenant, &head, &parts, accounting)
                        .await?
                } else {
                    None
                };
                Ok((
                    Some(SnapshotWindow {
                        watermark_hour: head.watermark_hour,
                        parts,
                        postings,
                    }),
                    revalidated,
                ))
            }
            PartLoadOutcome::Unusable => Ok((None, revalidated)),
            PartLoadOutcome::IsolationBreach(err) => Err(err),
            PartLoadOutcome::NotFoundRace => {
                // At most one HEAD re-read (docs/metric-index-plan.md 5.1
                // step 2): bypass the TTL cache so a part GC'd since the
                // cached HEAD was read is not raced again.
                let Some((fresh_head, fresh_revalidated)) = self
                    .read_head(
                        tenant,
                        signal,
                        &head_key,
                        now_ns,
                        true,
                        generations,
                        accounting,
                    )
                    .await?
                else {
                    return Ok((None, revalidated));
                };
                // Prefer the re-read HEAD's own re-validated generations; fall
                // back to the first read's if the re-read validated under the
                // passed-in view.
                let revalidated = fresh_revalidated.or(revalidated);
                match self
                    .load_snapshot_parts(tenant, &fresh_head, accounting)
                    .await
                {
                    PartLoadOutcome::Loaded(parts) => {
                        let postings = if want_postings {
                            self.load_snapshot_postings(tenant, &fresh_head, &parts, accounting)
                                .await?
                        } else {
                            None
                        };
                        Ok((
                            Some(SnapshotWindow {
                                watermark_hour: fresh_head.watermark_hour,
                                parts,
                                postings,
                            }),
                            revalidated,
                        ))
                    }
                    PartLoadOutcome::IsolationBreach(err) => Err(err),
                    PartLoadOutcome::Unusable | PartLoadOutcome::NotFoundRace => {
                        Ok((None, revalidated))
                    }
                }
            }
        }
    }

    /// Load and verify this HEAD's name postings through the immutable
    /// postings cache (P5b, docs/metric-index-plan.md 5.4). Every failure
    /// mode short of a `tenant_hash` mismatch (no postings ref, GET error,
    /// hash mismatch, decode error, part-binding mismatch, entry-count
    /// mismatch) degrades to `Ok(None)`: postings are a pure pruning
    /// optimization, never surfaced as an error and never allowed to make
    /// the snapshot window itself unusable. A `tenant_hash` mismatch is the
    /// one loud exception (ADR-0050 §2): a postings object naming a
    /// different tenant is an isolation breach, not a degrade-and-continue
    /// case, so it is a hard `CatalogError::FieldMismatch` with no fallback.
    async fn load_snapshot_postings(
        &self,
        tenant: &TenantHash,
        head: &SnapshotHead,
        parts: &[Arc<DecodedPart>],
        accounting: &QueryAccounting,
    ) -> Result<Option<Arc<DecodedPostings>>, CatalogError> {
        let Some(postings_ref) = head.postings.as_ref() else {
            return Ok(None);
        };
        let Ok(expected_part_blake3) = head
            .parts
            .iter()
            .map(|p| <[u8; 32]>::try_from(p.blake3.as_slice()))
            .collect::<Result<Vec<[u8; 32]>, _>>()
        else {
            return Ok(None);
        };

        if let Some(cached) = self
            .postings_cache()
            .get(tenant, &postings_ref.key, accounting)
        {
            return Ok(Some(cached));
        }

        let data = match self
            .fetch_content_addressed(
                tenant,
                &postings_ref.key,
                &postings_ref.blake3,
                postings_ref.size,
                accounting,
            )
            .await
        {
            Ok(data) => data,
            Err(err) => {
                tracing::warn!(error = %err, key = %postings_ref.key, "postings GET failed, pruning disabled");
                return Ok(None);
            }
        };
        let digest = blake3::hash(&data);
        if digest.as_bytes().as_slice() != postings_ref.blake3.as_slice() {
            tracing::warn!(key = %postings_ref.key, "postings hash mismatch, pruning disabled");
            return Ok(None);
        }
        // ADR-0050 §2 (#528): the tenant_hash check runs BEFORE decode's
        // part-binding check. `decode_postings` rejects an object not bound to
        // this HEAD's exact part hashes with `PostingsPartBindingMismatch`,
        // which degrades to `Ok(None)` (stale derived data, no cross-tenant
        // signal). Reading the declared tenant_hash first, off the already
        // hash-verified bytes, means a foreign tenant_hash hard-fails as an
        // isolation breach even when the object also fails to bind, instead of
        // that breach being masked by the binding degrade.
        match snapshot_format::postings_declared_tenant_hash(&data) {
            Ok(declared) if declared != tenant.0 => {
                self.record_isolation_breach();
                return Err(CatalogError::FieldMismatch {
                    key: postings_ref.key.clone(),
                    field: "tenant_hash",
                    expected: tenant.to_hex(),
                    actual: hex::encode(declared),
                });
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(error = %err, key = %postings_ref.key, "postings header unreadable, pruning disabled");
                return Ok(None);
            }
        }
        let limits = PostingsLimits {
            max_postings_bytes: self.config().max_postings_bytes,
        };
        let decoded = match snapshot_format::decode_postings(&data, &limits, &expected_part_blake3)
        {
            Ok(decoded) => decoded,
            Err(err) => {
                tracing::warn!(error = %err, key = %postings_ref.key, "postings failed to decode, pruning disabled");
                return Ok(None);
            }
        };
        let total_entries: u64 = parts.iter().map(|p| p.entries.len() as u64).sum();
        if decoded.header.entry_count != total_entries {
            tracing::warn!(key = %postings_ref.key, "postings entry_count mismatch, pruning disabled");
            return Ok(None);
        }

        let decoded = Arc::new(decoded);
        self.postings_cache().insert(
            *tenant,
            postings_ref.key.clone(),
            decoded.clone(),
            data.len() as u64,
            self.config().postings_cache_entries,
        );
        Ok(Some(decoded))
    }

    /// Read HEAD, through the TTL cache unless `bypass_cache`. Any failure
    /// short of a `shard_count` or `tenant_hash` mismatch is logged and
    /// folded into `None` (fall back to listing). A `shard_count` mismatch
    /// is a loud error (docs/metric-index-plan.md 5.1 step 1), but under
    /// ADR-0052 section 5 "mismatch" no longer means "not equal to this
    /// process's static `shard_count`": the head's `shard_count` is the
    /// fan-out ceiling at fold time, so it is validated against the ceiling
    /// this reader computes from the generation history (`generations`) for
    /// the head's watermark hour, and an older head that simply predates a
    /// reshard (a lower `shard_generation_count`) is accepted because Phase 1
    /// listing covers the newer hours it lacks. A head claiming more
    /// generations than this reader knows forces exactly one fresh record
    /// re-read before failing closed (see
    /// [`Self::validate_head_against_generations`]). A `tenant_hash` mismatch
    /// is likewise loud (ADR-0050 §2): a HEAD naming a different tenant is an
    /// isolation breach, so it hard-fails instead of silently falling back to
    /// a listing pass that could still be influenced by the wrong index.
    #[allow(clippy::too_many_arguments)]
    async fn read_head(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        head_key: &str,
        now_ns: i64,
        bypass_cache: bool,
        generations: &[ShardGeneration],
        accounting: &QueryAccounting,
    ) -> Result<Option<(Arc<SnapshotHead>, Option<Vec<ShardGeneration>>)>, CatalogError> {
        if !bypass_cache
            && let Some(cached) = self.head_cache().get(
                tenant,
                signal,
                now_ns,
                self.config().head_cache_ttl_ns,
                accounting,
            )
        {
            // A cached head was validated when first admitted; the caller keeps
            // its own generation view (`None`).
            return Ok(Some((cached, None)));
        }

        let got = match self.guarded_get(head_key, GetRange::Full, accounting).await {
            Ok(got) => got,
            Err(StoreError::NotFound) => return Ok(None),
            Err(err) => {
                tracing::warn!(error = %err, key = %head_key, "HEAD GET failed, falling back to listing");
                return Ok(None);
            }
        };
        let head = match snapshot_format::decode_head(&got.data) {
            Ok(head) => head,
            Err(err) => {
                tracing::warn!(error = %err, key = %head_key, "HEAD failed to decode, falling back to listing");
                return Ok(None);
            }
        };
        if head.tenant_hash.as_slice() != tenant.0.as_slice() {
            self.record_isolation_breach();
            return Err(CatalogError::FieldMismatch {
                key: head_key.to_string(),
                field: "tenant_hash",
                expected: tenant.to_hex(),
                actual: hex::encode(&head.tenant_hash),
            });
        }
        if signal::from_proto(head.signal as i32) != Ok(signal) {
            tracing::warn!(key = %head_key, "HEAD signal mismatch, falling back to listing");
            return Ok(None);
        }
        let revalidated = self
            .validate_head_against_generations(tenant, signal, head_key, &head, generations)
            .await?;

        let bytes = got.data.len() as u64;
        let head = Arc::new(head);
        self.head_cache().insert(
            *tenant,
            signal,
            head.clone(),
            bytes,
            now_ns,
            self.config().head_cache_capacity,
        );
        Ok(Some((head, revalidated)))
    }

    /// Validate a HEAD's `shard_count` against the generation history under
    /// the ADR-0052 section 5 rule. `generations` is the reader's already-read
    /// view. A head is accepted if its `shard_count` equals the ceiling this
    /// reader computes for the head's watermark hour, or if the head predates
    /// the reader's view (`shard_generation_count` lower than the reader's
    /// generation count -- Phase 1 listing covers the newer hours it lacks).
    ///
    /// Two situations force exactly one fresh, uncached record re-read before
    /// failing closed:
    ///
    /// - the head claims *more* generations than the reader knows: the reader's
    ///   record view may be stale, and serving from a fewer-generation view
    ///   could under-scan; or
    /// - the head knew *fewer* generations than the reader but its own
    ///   watermark reaches into hours an unknown-to-the-head generation was
    ///   already active for (Finding 2): silently accepting would omit data
    ///   that landed in the newer, possibly-wider range within the head's own
    ///   watermark. `head_generations_acceptable` already rejected this case,
    ///   so it lands here rather than being served.
    ///
    /// Both reduce to "the head's generation count differs from the reader's".
    /// A head whose generation count *equals* the reader's yet whose ceiling
    /// disagrees is a genuine mismatch (not a staleness case): fail closed
    /// immediately without the extra read.
    ///
    /// Returns the generation history the head was ultimately validated
    /// against: `None` when the head was acceptable under the passed-in
    /// `generations` (the caller keeps its own view), or `Some(fresh)` when a
    /// re-read was performed and accepted -- the caller must then use `fresh`
    /// (never staler than the passed-in view) for its Phase 1 scan set, so the
    /// listing suffix is never scanned over a staler generation history than
    /// the one that validated the head (Finding 4).
    async fn validate_head_against_generations(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        head_key: &str,
        head: &SnapshotHead,
        generations: &[ShardGeneration],
    ) -> Result<Option<Vec<ShardGeneration>>, CatalogError> {
        if head_generations_acceptable(head, generations) {
            return Ok(None);
        }
        let reader_gen_count = generations.len() as u32;
        if head.shard_generation_count == reader_gen_count {
            // Same generation count, ceiling disagrees: a genuine mismatch, not
            // a staleness case, so fail closed now without a re-read.
            return Err(head_shard_count_mismatch(head_key, head, generations));
        }
        // Re-read once, bypassing any caching (the read is uncached by
        // construction), and recompute before failing.
        let fresh = self.read_scan_generations(tenant, signal).await?;
        if head_generations_acceptable(head, &fresh) {
            return Ok(Some(fresh));
        }
        Err(head_shard_count_mismatch(head_key, head, &fresh))
    }

    /// Load and verify every part HEAD names, through the immutable part
    /// cache. Parts are content-addressed, so a cache hit needs no
    /// re-verification.
    ///
    /// The uncached part GETs run concurrently under the resolve-wide
    /// semaphore (#278 item 2) rather than one await at a time. `buffered`
    /// preserves HEAD's part order, and the fold returns the first
    /// non-`Loaded` outcome in that order, so a multi-part snapshot yields
    /// exactly the same `NotFoundRace`/`Unusable` decision the sequential
    /// loop did.
    async fn load_snapshot_parts(
        &self,
        tenant: &TenantHash,
        head: &SnapshotHead,
        accounting: &QueryAccounting,
    ) -> PartLoadOutcome {
        // Owned part-ref clones, not borrowed `&SnapshotPartRef`s: a stream
        // closure that borrows each item infers a non-higher-ranked lifetime
        // for the future and fails to unify with axum's `Handler` blanket
        // impl at the HTTP router (the "FnOnce is not general enough" wall).
        let loaded: Vec<OnePartOutcome> = stream::iter(head.parts.iter().cloned())
            .map(|part_ref| async move { self.load_one_part(tenant, &part_ref, accounting).await })
            .buffered(crate::catalog::MAX_CONCURRENT_REQUESTS)
            .collect()
            .await;
        let mut parts = Vec::with_capacity(loaded.len());
        for outcome in loaded {
            match outcome {
                OnePartOutcome::Loaded(part) => parts.push(part),
                OnePartOutcome::NotFoundRace => return PartLoadOutcome::NotFoundRace,
                OnePartOutcome::Unusable => return PartLoadOutcome::Unusable,
                OnePartOutcome::IsolationBreach(err) => {
                    return PartLoadOutcome::IsolationBreach(err);
                }
            }
        }
        PartLoadOutcome::Loaded(parts)
    }

    /// Load, verify, and decode one snapshot part through the immutable part
    /// cache. The per-part half of [`load_snapshot_parts`](Self::load_snapshot_parts),
    /// factored out so the parts can be fetched concurrently (#278 item 2).
    async fn load_one_part(
        &self,
        tenant: &TenantHash,
        part_ref: &ravel_proto::catalog::v1::SnapshotPartRef,
        accounting: &QueryAccounting,
    ) -> OnePartOutcome {
        if let Some(cached) = self.part_cache().get(tenant, &part_ref.key, accounting) {
            return OnePartOutcome::Loaded(cached);
        }
        let data = match self
            .fetch_content_addressed(
                tenant,
                &part_ref.key,
                &part_ref.blake3,
                part_ref.size,
                accounting,
            )
            .await
        {
            Ok(data) => data,
            Err(StoreError::NotFound) => {
                tracing::warn!(key = %part_ref.key, "snapshot part not found, will re-read HEAD once");
                return OnePartOutcome::NotFoundRace;
            }
            Err(err) => {
                tracing::warn!(error = %err, key = %part_ref.key, "snapshot part GET failed, falling back to listing");
                return OnePartOutcome::Unusable;
            }
        };
        let digest = blake3::hash(&data);
        if digest.as_bytes().as_slice() != part_ref.blake3.as_slice() {
            tracing::warn!(key = %part_ref.key, "snapshot part hash mismatch, falling back to listing");
            return OnePartOutcome::Unusable;
        }
        let limits = PartLimits {
            max_snapshot_part_bytes: self.config().max_snapshot_part_bytes,
        };
        let decoded = match snapshot_format::decode_part(&data, &limits) {
            Ok(decoded) => Arc::new(decoded),
            Err(err) => {
                tracing::warn!(error = %err, key = %part_ref.key, "snapshot part failed to decode, falling back to listing");
                return OnePartOutcome::Unusable;
            }
        };
        // ADR-0050 §2: a part whose own header names a different tenant is an
        // isolation breach, hard-fail, never cached or served. The HEAD's
        // per-part blake3 was already verified above, but a HEAD that passes
        // that check can still reference a part object belonging to another
        // tenant; the part header's tenant_hash is the independent binding to
        // the requester (#527).
        if decoded.header.tenant_hash.as_slice() != tenant.0.as_slice() {
            self.record_isolation_breach();
            return OnePartOutcome::IsolationBreach(CatalogError::FieldMismatch {
                key: part_ref.key.clone(),
                field: "tenant_hash",
                expected: tenant.to_hex(),
                actual: hex::encode(&decoded.header.tenant_hash),
            });
        }
        self.part_cache().insert(
            *tenant,
            part_ref.key.clone(),
            decoded.clone(),
            data.len() as u64,
            self.config().snapshot_cache_parts,
        );
        OnePartOutcome::Loaded(decoded)
    }
}

/// One part's load result, folded back into a [`PartLoadOutcome`] over the
/// whole part set in HEAD order.
enum OnePartOutcome {
    Loaded(Arc<DecodedPart>),
    NotFoundRace,
    Unusable,
    /// This part's header names a foreign tenant (ADR-0050 §2, #527).
    IsolationBreach(CatalogError),
}

/// The ADR-0052 section 5 acceptance predicate for a HEAD's `shard_count`
/// against a reader's generation view. A head is acceptable if either:
///
/// - its `shard_count` equals the reader's fan-out ceiling
///   ([`shard_ceiling`], the same function the writer stamped it with) for the
///   head's watermark hour; or
/// - it is an *older* head (it knew fewer generations than the reader) **and**
///   its watermark predates the activation of the first generation it didn't
///   know about, so Phase 1 listing genuinely covers every hour the head lacks.
///
/// The watermark check on the older-head arm is the Finding 2 fix: without it,
/// this arm accepted any lower-`shard_generation_count` head unconditionally,
/// so a head whose own watermark reached into hours a newer, wider generation
/// was already active for would be served silently, omitting data that landed
/// in the new shard range within the head's own watermark -- a completed query
/// with a wrong answer.
///
/// A pre-ADR-0052 head carries `shard_generation_count` 0 as the "absent"
/// sentinel, yet it still knew the single implicit generation 0, so it is
/// treated as having known one generation: the first generation it didn't know
/// about is index `max(shard_generation_count, 1)`. (A modern fold stamps
/// `shard_generation_count = generations.len()`, so both 0 and 1 mean "knew
/// generation 0 only"; the first unknown is index 1 for both.) When that index
/// is at or past the end of the reader's history, the head claims to know
/// every generation the reader does -- there is no unknown generation left to
/// blame the ceiling disagreement on, so it is NOT covered by the
/// older-head/staleness rationale at all. This is reachable in practice for a
/// pre-ADR-0052 head (`shard_generation_count` absent, i.e. 0) read by a
/// reader whose own history is exactly one generation deep: the disagreement
/// is then a genuine `shard_count` mismatch between the head and generation
/// 0's own count, the same class of error the pre-ADR-0052 equality check
/// always raised loudly. Trusting it here would silently omit data below the
/// head's own watermark, for both enforcing and non-enforcing readers -- a
/// completed query returning an incomplete, unexplained answer.
fn head_generations_acceptable(head: &SnapshotHead, generations: &[ShardGeneration]) -> bool {
    if head.shard_count == shard_ceiling(generations, head.watermark_hour) {
        return true;
    }
    let reader_gen_count = generations.len() as u32;
    if head.shard_generation_count >= reader_gen_count {
        // Not an older head (it knew at least as many generations as the
        // reader): the ceiling disagreement is a genuine mismatch, not a
        // covered-by-listing staleness case.
        return false;
    }
    // Older head. Safe only if its watermark predates the activation of the
    // first generation it didn't know about; otherwise its parts don't cover
    // the wider range those unknown-to-the-head hours were written under.
    let first_unknown = head.shard_generation_count.max(1) as usize;
    match generations.get(first_unknown) {
        Some(first_unknown_gen) => head.watermark_hour < first_unknown_gen.activation_hour,
        // No generation past `first_unknown` exists: there's nothing unknown
        // to explain the disagreement away, so it's an unexplained mismatch,
        // not a covered-by-listing staleness case. Reject, matching the
        // `>=` branch above.
        None => false,
    }
}

/// The loud `FieldMismatch` a HEAD that fails [`head_generations_acceptable`]
/// yields, naming the reader's computed ceiling as `expected` and the head's
/// own `shard_count` as `actual`. Uses the same [`shard_ceiling`] the writer
/// stamped the head with. Stays excluded from listing fallback, exactly as the
/// equality check it replaces.
fn head_shard_count_mismatch(
    head_key: &str,
    head: &SnapshotHead,
    generations: &[ShardGeneration],
) -> CatalogError {
    CatalogError::FieldMismatch {
        key: head_key.to_string(),
        field: "shard_count",
        expected: shard_ceiling(generations, head.watermark_hour).to_string(),
        actual: head.shard_count.to_string(),
    }
}

fn build_segment_ref_from_entry(
    tenant: &TenantHash,
    signal: Signal,
    entry: &SnapshotEntry,
) -> Result<SegmentRef, CatalogError> {
    let entry_label = || {
        format!(
            "snapshot entry (level {}, shard {}, hour {})",
            entry.level, entry.shard, entry.ingest_hour_bucket
        )
    };
    let content_hash: [u8; 32] =
        entry
            .content_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: entry_label(),
                field: "content_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", entry.content_hash.len()),
            })?;

    // Level 1 is a compaction (L1) part. Its identity is carried in the
    // writer_* slots by the fold (`build_l1_snapshot_entry`): writer_id holds
    // the 32-byte input_set_hash, writer_epoch holds the part_index. Rebuild
    // exactly the `SegmentRef` a live listing produces from the compaction
    // record and part (`build_l1_segment_ref`): the L1 data key from
    // `keys::l1_part_key`, and writer_* left nil since an L1 part has no
    // writer identity (docs/metric-index-plan.md section 7).
    if entry.level != 0 {
        let input_set_hash: [u8; 32] =
            entry
                .writer_id
                .clone()
                .try_into()
                .map_err(|_| CatalogError::FieldMismatch {
                    key: entry_label(),
                    field: "writer_id (input_set_hash)",
                    expected: "32 bytes".to_string(),
                    actual: format!("{} bytes", entry.writer_id.len()),
                })?;
        let part_index =
            u32::try_from(entry.writer_epoch).map_err(|_| CatalogError::FieldMismatch {
                key: entry_label(),
                field: "writer_epoch (part_index)",
                expected: "u32".to_string(),
                actual: entry.writer_epoch.to_string(),
            })?;
        let data_object_key = keys::l1_part_key(
            tenant,
            signal,
            entry.shard,
            entry.ingest_hour_bucket,
            &hex16(&input_set_hash),
            part_index,
            &hex16(&content_hash),
        )?;
        return Ok(SegmentRef {
            data_object_key,
            object_size: entry.object_size,
            min_event_ts_ns: entry.min_event_ts_ns,
            max_event_ts_ns: entry.max_event_ts_ns,
            ingest_hour_bucket: entry.ingest_hour_bucket,
            sample_count: entry.sample_count,
            series_count: entry.series_count,
            shard: entry.shard,
            content_hash,
            writer_id: Uuid::nil(),
            writer_epoch: 0,
            writer_seq: 0,
            created_unix_ns: entry.created_unix_ns,
            level: SegmentLevel::L1 {
                input_set_hash,
                part_index,
            },
        });
    }

    let writer_id_bytes: [u8; 16] =
        entry
            .writer_id
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: entry_label(),
                field: "writer_id",
                expected: "16 bytes".to_string(),
                actual: format!("{} bytes", entry.writer_id.len()),
            })?;
    let writer_id = Uuid::from_bytes(writer_id_bytes);
    let data_object_key = keys::data_key(
        tenant,
        signal,
        entry.shard,
        writer_id,
        entry.writer_epoch,
        entry.writer_seq,
        &content_hash,
    )?;
    Ok(SegmentRef {
        data_object_key,
        object_size: entry.object_size,
        min_event_ts_ns: entry.min_event_ts_ns,
        max_event_ts_ns: entry.max_event_ts_ns,
        ingest_hour_bucket: entry.ingest_hour_bucket,
        sample_count: entry.sample_count,
        series_count: entry.series_count,
        shard: entry.shard,
        content_hash,
        writer_id,
        writer_epoch: entry.writer_epoch,
        writer_seq: entry.writer_seq,
        created_unix_ns: entry.created_unix_ns,
        level: SegmentLevel::L0,
    })
}

/// Lowercase hex of a 32-byte hash's first 8 bytes: the 16-char `hash16`
/// component the L0 data-key and L1 part-key layouts embed
/// (crates/ravel-commit/src/keys.rs), matching `hex::encode(&hash[..8])`.
fn hex16(hash: &[u8; 32]) -> String {
    hash[..8].iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_object_store::instrument::StoreOp;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{InstrumentedStore, ObjectStoreBackend, PutOptions};
    use ravel_proto::catalog::v1::{SnapshotPartRef, SnapshotPostingsRef};

    use super::*;
    use crate::config::CatalogConfig;
    use crate::snapshot_format::HEAD_FORMAT_VERSION;

    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    fn tenant() -> TenantHash {
        TenantHash([0xab; 16])
    }

    fn config(shard_count: u32) -> CatalogConfig {
        CatalogConfig {
            shard_count,
            ..Default::default()
        }
    }

    /// A HEAD with one syntactically valid but unfetched part ref: enough to
    /// pass `validate_head`'s shape checks without needing a real,
    /// resolvable part object, since the tenant_hash check runs before any
    /// part is loaded.
    fn head_with_tenant(tenant_hash: [u8; 16]) -> SnapshotHead {
        SnapshotHead {
            format_version: HEAD_FORMAT_VERSION,
            tenant_hash: tenant_hash.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as u32,
            shard_count: 1,
            watermark_hour: 10,
            parts: vec![SnapshotPartRef {
                key: "unused-part-key".to_string(),
                blake3: vec![0u8; 32],
                size: 1,
                entry_count: 0,
                watermark_hour: 10,
                min_hour: 0,
            }],
            folder_id: Uuid::new_v4().into_bytes().to_vec(),
            created_unix_ns: 0,
            postings: None,
            shard_generation_count: 1,
        }
    }

    #[tokio::test]
    async fn head_tenant_hash_mismatch_is_hard_error() {
        let instrumented = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let catalog = Catalog::new(instrumented.clone(), config(1)).expect("catalog");
        let tenant = tenant();
        let wrong_tenant = TenantHash([0xff; 16]);

        let head = head_with_tenant(wrong_tenant.0);
        let head_bytes = snapshot_format::encode_head(&head).expect("encode head");
        let head_key = head_object_key(&tenant, Signal::Metrics);
        instrumented
            .put(&head_key, Bytes::from(head_bytes), PutOptions::default())
            .await
            .expect("put head");

        let now_ns = 20 * NS_PER_HOUR;
        let range = TimeRange {
            start_ns: now_ns - 1_000,
            end_ns: now_ns,
        };
        let err = catalog
            .resolve(&tenant, Signal::Metrics, range, &[], now_ns)
            .await
            .expect_err("tenant_hash mismatch must hard-fail, never degrade to listing");
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "tenant_hash"),
            other => panic!("expected FieldMismatch, got {other:?}"),
        }
        assert_eq!(catalog.isolation_breaches(), 1);
        assert_eq!(
            instrumented.metrics().snapshot().op(StoreOp::List).calls,
            0,
            "a tenant_hash-mismatched HEAD must never fall back to listing"
        );
    }

    #[tokio::test]
    async fn postings_tenant_hash_mismatch_is_hard_error() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store, config(1)).expect("catalog");
        let tenant = tenant();
        let wrong_tenant = TenantHash([0xff; 16]);

        let mut head = head_with_tenant(tenant.0);
        let part_blake3: [u8; 32] = head.parts[0]
            .blake3
            .clone()
            .try_into()
            .expect("32-byte part blake3");

        let postings_bytes = snapshot_format::encode_postings(
            wrong_tenant.0,
            signal::to_proto(Signal::Metrics) as u32,
            &[part_blake3],
            0,
            &[],
        )
        .expect("encode postings");
        let postings_hash = *blake3::hash(&postings_bytes).as_bytes();
        let postings_key = "postings-key".to_string();
        catalog
            .store()
            .put(
                &postings_key,
                Bytes::from(postings_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put postings");
        head.postings = Some(SnapshotPostingsRef {
            key: postings_key,
            blake3: postings_hash.to_vec(),
            size: postings_bytes.len() as u64,
            name_count: 0,
            part_blake3: vec![part_blake3.to_vec()],
        });

        let accounting = QueryAccounting::new();
        let err = catalog
            .load_snapshot_postings(&tenant, &head, &[], &accounting)
            .await
            .expect_err("tenant_hash mismatch must hard-fail, never degrade pruning");
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "tenant_hash"),
            other => panic!("expected FieldMismatch, got {other:?}"),
        }
        assert_eq!(catalog.isolation_breaches(), 1);
    }

    /// #527 / ADR-0050 §2: a snapshot part whose own header names a foreign
    /// tenant, referenced by a HEAD that is otherwise valid for this tenant
    /// (correct HEAD tenant_hash, correct per-part blake3), must hard-fail on
    /// the part's tenant_hash, never be served, and must count the breach.
    #[tokio::test]
    async fn part_tenant_hash_mismatch_is_hard_error() {
        let instrumented = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let catalog = Catalog::new(instrumented.clone(), config(1)).expect("catalog");
        let tenant = tenant();
        let wrong_tenant = TenantHash([0xff; 16]);

        // A real, decodable part object whose header declares the wrong tenant.
        let part_bytes = snapshot_format::encode_part(
            wrong_tenant.0,
            signal::to_proto(Signal::Metrics) as u32,
            1,
            10,
            &[],
        )
        .expect("encode part");
        let part_hash = *blake3::hash(&part_bytes).as_bytes();
        let part_key = "part-key".to_string();
        instrumented
            .put(
                &part_key,
                Bytes::from(part_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put part");

        // A HEAD valid for THIS tenant that references the part by its correct
        // blake3: the existing HEAD tenant_hash and part blake3 checks both
        // pass, so only the part header betrays the foreign tenant.
        let head = SnapshotHead {
            format_version: HEAD_FORMAT_VERSION,
            tenant_hash: tenant.0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as u32,
            shard_count: 1,
            watermark_hour: 10,
            parts: vec![SnapshotPartRef {
                key: part_key,
                blake3: part_hash.to_vec(),
                size: part_bytes.len() as u64,
                entry_count: 0,
                watermark_hour: 10,
                min_hour: 0,
            }],
            folder_id: Uuid::new_v4().into_bytes().to_vec(),
            created_unix_ns: 0,
            postings: None,
            shard_generation_count: 1,
        };
        let head_bytes = snapshot_format::encode_head(&head).expect("encode head");
        let head_key = head_object_key(&tenant, Signal::Metrics);
        instrumented
            .put(&head_key, Bytes::from(head_bytes), PutOptions::default())
            .await
            .expect("put head");

        let now_ns = 20 * NS_PER_HOUR;
        let range = TimeRange {
            start_ns: now_ns - 1_000,
            end_ns: now_ns,
        };
        let err = catalog
            .resolve(&tenant, Signal::Metrics, range, &[], now_ns)
            .await
            .expect_err("a part naming a foreign tenant must hard-fail, never be served");
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "tenant_hash"),
            other => panic!("expected FieldMismatch, got {other:?}"),
        }
        assert_eq!(catalog.isolation_breaches(), 1);
        assert_eq!(
            instrumented.metrics().snapshot().op(StoreOp::List).calls,
            0,
            "a part tenant_hash mismatch must never fall back to listing"
        );
    }

    /// #528 / ADR-0050 §2: a postings object declaring a foreign tenant_hash
    /// that is NOT bound to this HEAD's part hashes must hard-fail on the
    /// tenant, not degrade to `Ok(None)` through the binding-mismatch path
    /// that runs first for a bound object.
    #[tokio::test]
    async fn postings_foreign_tenant_unbound_still_hard_fails() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store, config(1)).expect("catalog");
        let tenant = tenant();
        let wrong_tenant = TenantHash([0xff; 16]);

        let mut head = head_with_tenant(tenant.0);
        // Bind the postings to a part hash the HEAD does not carry (HEAD's
        // part blake3 is all-zero), so decode's part-binding check would
        // reject first and, unfixed, degrade to Ok(None).
        let unbound_part_blake3 = [0x77u8; 32];

        let postings_bytes = snapshot_format::encode_postings(
            wrong_tenant.0,
            signal::to_proto(Signal::Metrics) as u32,
            &[unbound_part_blake3],
            0,
            &[],
        )
        .expect("encode postings");
        let postings_hash = *blake3::hash(&postings_bytes).as_bytes();
        let postings_key = "postings-key".to_string();
        catalog
            .store()
            .put(
                &postings_key,
                Bytes::from(postings_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put postings");
        head.postings = Some(SnapshotPostingsRef {
            key: postings_key,
            blake3: postings_hash.to_vec(),
            size: postings_bytes.len() as u64,
            name_count: 0,
            part_blake3: vec![unbound_part_blake3.to_vec()],
        });

        let accounting = QueryAccounting::new();
        let err = catalog
            .load_snapshot_postings(&tenant, &head, &[], &accounting)
            .await
            .expect_err("foreign tenant_hash must hard-fail even when the object does not bind");
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "tenant_hash"),
            other => panic!("expected FieldMismatch, got {other:?}"),
        }
        assert_eq!(catalog.isolation_breaches(), 1);
    }

    // ---- ADR-0052 section 5: HEAD generation-aware validation predicate ----

    fn sg(generation: u32, shard_count: u32, activation_hour: u32) -> ShardGeneration {
        ShardGeneration {
            generation,
            shard_count,
            activation_hour,
            appended_unix_ns: 0,
        }
    }

    /// The ADR-0052 section 5 acceptance predicate, covering: a head whose
    /// shard_count equals the reader's ceiling (accepted); a head with the
    /// wrong count and no lower generation count (rejected); a genuinely older
    /// head (lower `shard_generation_count`, including the pre-ADR-0052
    /// absent/0 case) whose watermark predates the activation of the
    /// generation(s) it didn't know about (accepted -- Phase 1 listing covers
    /// those newer hours); a head with a lower `shard_generation_count` but a
    /// count mismatch that no unknown-to-it generation can explain, because the
    /// reader's own history has nothing past what the head already claims to
    /// know (rejected -- not covered by the older-head rationale, an
    /// unexplained mismatch); and a head claiming more generations than the
    /// reader (rejected by the predicate, which routes the caller to the
    /// one-shot record re-read).
    #[test]
    fn head_generations_predicate_covers_all_arms() {
        let mut head = head_with_tenant(tenant().0);
        head.watermark_hour = 50;
        head.parts[0].watermark_hour = 50;

        // Single-generation reader (no reshard): ceiling for any hour is 4.
        let one = [sg(0, 4, 0)];
        head.shard_count = 4;
        head.shard_generation_count = 1;
        assert!(
            head_generations_acceptable(&head, &one),
            "shard_count equals the ceiling"
        );

        head.shard_count = 8;
        head.shard_generation_count = 1;
        assert!(
            !head_generations_acceptable(&head, &one),
            "wrong count, not an older head: rejected"
        );

        head.shard_generation_count = 0;
        assert!(
            !head_generations_acceptable(&head, &one),
            "sgc 0 (pre-ADR-0052 absent sentinel) against a single-generation \
             reader has no unknown generation to explain the count mismatch: \
             not covered by the older-head rationale, so rejected, not silently \
             trusted (NEW-1 regression: this exact case used to be accepted, \
             silently dropping data below the head's watermark)"
        );

        // Two-generation reader (an increase 4 -> 8 at hour 100).
        let two = [sg(0, 4, 0), sg(1, 8, 100)];
        // At the head's watermark 50 the ceiling is still 4 (gen1 not yet
        // active), so a head that recorded 4 with the full generation count is
        // accepted.
        head.shard_count = 4;
        head.shard_generation_count = 2;
        assert!(
            head_generations_acceptable(&head, &two),
            "ceiling at the watermark hour matches"
        );

        head.shard_count = 99;
        head.shard_generation_count = 2;
        assert!(
            !head_generations_acceptable(&head, &two),
            "same generation count, wrong ceiling: rejected"
        );

        head.shard_count = 99;
        head.shard_generation_count = 3;
        assert!(
            !head_generations_acceptable(&head, &two),
            "head claims more generations than the reader and its count does not \
             match: predicate rejects so the caller forces one record re-read \
             before failing closed"
        );
    }

    /// Finding 1: after a decrease followed by a fold past the slack window, the
    /// fold-time ceiling the writer stamps and the ceiling the reader recomputes
    /// must agree, or every query hard-fails forever. gen0 count 4 @ hour 0,
    /// gen1 count 2 @ hour 500000 (a decrease); a fold at watermark 500010 is
    /// well past the slack window (500000 + S). The writer's shared
    /// `shard_ceiling` and the reader's acceptance predicate now use the same
    /// formula, so the head validates.
    #[test]
    fn decrease_past_slack_head_validates_against_reader_ceiling() {
        let gens = [sg(0, 4, 0), sg(1, 2, 500_000)];
        let watermark = 500_010;

        // The writer (fold) stamps the union-max ceiling: 4 (gen0, activated at
        // or before the watermark, is included with no slack cutoff).
        let ceiling = shard_ceiling(&gens, watermark);
        assert_eq!(ceiling, 4);

        // The point-in-time scan_count at the watermark drops the retired gen0
        // once its slack window has closed -> 2. Before the fix the reader
        // validated the head against this, disagreeing with the writer's 4 and
        // hard-failing every query permanently.
        assert_eq!(
            crate::provisioning::scan_count(
                &gens,
                watermark,
                crate::provisioning::DEFAULT_SCAN_SLACK_HOURS,
            ),
            2,
            "scan_count at the watermark excludes the slack-expired generation"
        );

        let mut head = head_with_tenant(tenant().0);
        head.watermark_hour = watermark;
        head.parts[0].watermark_hour = watermark;
        head.shard_count = ceiling; // exactly what the (fixed) writer stamps
        head.shard_generation_count = gens.len() as u32;

        assert!(
            head_generations_acceptable(&head, &gens),
            "a head stamped with the writer's ceiling must validate against the \
             reader's ceiling computed the same way"
        );
    }

    /// Finding 2: the older-head accept arm must not accept a head whose own
    /// watermark reaches into hours an unknown-to-the-head generation was
    /// already active for. Reader knows gen0 (count 4 @ 0) and gen1 (count 8 @
    /// 500000, an increase); a non-enforcing fold wrote a head that knew only
    /// gen0 (`shard_generation_count` 1) at watermark 500002 -- past gen1's
    /// activation. Accepting it would silently omit data that landed in the new
    /// range within the head's own watermark.
    #[test]
    fn older_head_reaching_unknown_generation_active_hours_is_rejected() {
        let gens = [sg(0, 4, 0), sg(1, 8, 500_000)];
        let mut head = head_with_tenant(tenant().0);
        head.shard_count = 4; // stamped under gen0 only
        head.shard_generation_count = 1;

        head.watermark_hour = 500_002; // reaches past gen1's activation
        head.parts[0].watermark_hour = 500_002;
        assert!(
            !head_generations_acceptable(&head, &gens),
            "an older head whose watermark reaches into an unknown generation's \
             active hours must not be silently accepted"
        );

        // The same older head with a watermark BEFORE gen1 activated is safe:
        // Phase 1 listing covers the newer hours it lacks. Make shard_count
        // disagree with the ceiling so acceptance can only come from the
        // older-head arm, not the equality arm.
        head.shard_count = 999;
        head.watermark_hour = 499_999;
        head.parts[0].watermark_hour = 499_999;
        assert!(
            head_generations_acceptable(&head, &gens),
            "an older head whose watermark predates the unknown generation's \
             activation is safe to accept"
        );

        // A pre-ADR-0052 head (shard_generation_count 0 sentinel) still knew the
        // implicit generation 0, so the first generation it didn't know is index
        // 1 (not 0): the same watermark rule applies, not an off-by-one reject.
        head.shard_generation_count = 0;
        head.shard_count = 4;
        head.watermark_hour = 499_999;
        head.parts[0].watermark_hour = 499_999;
        assert!(
            head_generations_acceptable(&head, &gens),
            "a pre-reshard head with a watermark predating gen1 is accepted"
        );
        head.watermark_hour = 500_002;
        head.parts[0].watermark_hour = 500_002;
        assert!(
            !head_generations_acceptable(&head, &gens),
            "a pre-reshard head whose watermark reaches into gen1's active hours \
             is rejected, same as a modern older head"
        );
    }

    // ---- ADR-0061 decision 3 (EF-4/#724): literal-prefix range scan ----

    use crate::snapshot_format::NamePostings;
    use ravel_proto::catalog::v1::{SnapshotPartHeader, SnapshotPostingsHeader};

    /// A `SnapshotWindow` with `part_count` empty parts and hand-built,
    /// ascending-sorted postings. The parts' contents are irrelevant to the
    /// ordinal-lookup methods under test (they only gate on `parts.len()`), so
    /// their entries are empty.
    fn window_with_names(names: &[(&str, &[u64])], part_count: usize) -> SnapshotWindow {
        let names = names
            .iter()
            .map(|(n, ords)| NamePostings {
                name: (*n).to_string(),
                ordinals: ords.to_vec(),
            })
            .collect();
        let postings = DecodedPostings {
            header: SnapshotPostingsHeader::default(),
            names,
        };
        let parts = (0..part_count)
            .map(|_| {
                Arc::new(DecodedPart {
                    header: SnapshotPartHeader::default(),
                    entries: Vec::new(),
                })
            })
            .collect();
        SnapshotWindow {
            watermark_hour: 0,
            parts,
            postings: Some(Arc::new(postings)),
        }
    }

    /// Names deliberately include an aliasing pair ("app" is itself a prefix of
    /// "apple") and interleaved ordinals across distinct names, so the union's
    /// sort is genuinely exercised. Ascending sort order:
    /// alpha, apex, app, apple, banana, zebra.
    fn sample_window(part_count: usize) -> SnapshotWindow {
        window_with_names(
            &[
                ("alpha", &[0]),
                ("apex", &[1, 4]),
                ("app", &[2]),
                ("apple", &[3, 5]),
                ("banana", &[6]),
                ("zebra", &[7]),
            ],
            part_count,
        )
    }

    #[test]
    fn prefix_sentinel_value_is_pinned() {
        // The two query-language producers (ravel_query::engine,
        // ravel_sql::executor) duplicate this literal inline; pin it here so a
        // silent drift on either side is caught by the end-to-end oracles and
        // this assertion together.
        assert_eq!(PREFIX_FILTER_SENTINEL, '\u{1}');
    }

    #[test]
    fn prefix_range_scan_covers_every_boundary_case() {
        let w = sample_window(1);

        // Zero matches: a real "prune everything" result, not a bypass.
        assert_eq!(
            w.postings_prefix_ordinals_for("qux"),
            Some(Vec::new()),
            "a prefix matching no name yields Some(empty), pruning every entry"
        );

        // Matches exactly the first name in sort order.
        assert_eq!(
            w.postings_prefix_ordinals_for("alph"),
            Some(vec![0]),
            "prefix isolating the first name"
        );

        // Matches exactly the last name in sort order.
        assert_eq!(
            w.postings_prefix_ordinals_for("zeb"),
            Some(vec![7]),
            "prefix isolating the last name"
        );

        // Matches multiple distinct names; the union is sorted across them.
        assert_eq!(
            w.postings_prefix_ordinals_for("ap"),
            Some(vec![1, 2, 3, 4, 5]),
            "apex|app|apple ordinals, unioned and sorted"
        );

        // Matches one name that is itself a prefix of another matching name.
        assert_eq!(
            w.postings_prefix_ordinals_for("app"),
            Some(vec![2, 3, 5]),
            "app and apple both match; app is a prefix of apple"
        );

        // An exact whole-name prefix still range-scans (it can alias a longer
        // name): "apple" matches only itself here.
        assert_eq!(w.postings_prefix_ordinals_for("apple"), Some(vec![3, 5]));
    }

    #[test]
    fn prefix_range_scan_bypasses_when_unsafe() {
        // Empty prefix (a `.*`-only regex) prunes nothing: bypass, not "match
        // all".
        assert_eq!(sample_window(1).postings_prefix_ordinals_for(""), None);

        // More than one covered part: pruning is confined to the single-part
        // case, exactly like the exact lookup.
        assert_eq!(sample_window(2).postings_prefix_ordinals_for("ap"), None);

        // No usable postings at all.
        let mut w = sample_window(1);
        w.postings = None;
        assert_eq!(w.postings_prefix_ordinals_for("ap"), None);
    }

    #[test]
    fn filter_dispatch_routes_prefix_and_exact() {
        let w = sample_window(1);

        // No filter: no pruning.
        assert!(w.postings_ordinals_for_filter(None).is_none());

        // Unencoded string: exact-equality lookup, borrowed, unchanged
        // behaviour. "apex" matches exactly one name.
        assert_eq!(
            w.postings_ordinals_for_filter(Some("apex")).as_deref(),
            Some([1u64, 4].as_slice())
        );
        // Exact lookup of a name that is a prefix of others must NOT range
        // scan: "app" exact is only app's ordinals, not apple's.
        assert_eq!(
            w.postings_ordinals_for_filter(Some("app")).as_deref(),
            Some([2u64].as_slice())
        );
        // A prefix (sentinel-encoded) of the very same text DOES range scan.
        let encoded = format!("{PREFIX_FILTER_SENTINEL}app");
        assert_eq!(
            w.postings_ordinals_for_filter(Some(&encoded)).as_deref(),
            Some([2u64, 3, 5].as_slice())
        );

        // Exact miss is Some(empty) (prune all); prefix miss likewise.
        assert_eq!(
            w.postings_ordinals_for_filter(Some("nope")).as_deref(),
            Some([].as_slice())
        );
        let encoded_miss = format!("{PREFIX_FILTER_SENTINEL}qux");
        assert_eq!(
            w.postings_ordinals_for_filter(Some(&encoded_miss))
                .as_deref(),
            Some([].as_slice())
        );
    }

    /// The defence-in-depth union: even if a stored name literally equalled the
    /// encoded (sentinel-led) string -- which no real ingest path produces --
    /// an exact filter misread as a prefix must never prune that name's
    /// segments away. Here a name equal to the encoded bytes exists AND does
    /// not start with the decoded prefix, so only the union keeps it.
    #[test]
    fn misread_exact_filter_never_drops_its_own_segments() {
        let sentinel = PREFIX_FILTER_SENTINEL;
        let encoded = format!("{sentinel}zzz");
        // Sorted ascending: the control byte (U+0001) sorts before ASCII
        // letters, so the encoded name is first.
        let w = window_with_names(
            &[
                (encoded.as_str(), &[9]),
                ("aaa", &[0]),
                ("zzzsomething", &[1]),
            ],
            1,
        );
        // Decoded prefix is "zzz": range-scans zzzsomething (ordinal 1), and
        // the union additionally retains the exact encoded name (ordinal 9).
        let got = w
            .postings_ordinals_for_filter(Some(&encoded))
            .expect("prunable")
            .into_owned();
        assert!(
            got.contains(&9),
            "the exact encoded name's segments must be retained (no false negative)"
        );
        assert!(got.contains(&1), "the genuine prefix match is present too");
    }
}
