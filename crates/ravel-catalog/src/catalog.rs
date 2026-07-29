//! Snapshot resolution: listing-based discovery over commit records
//! (docs/catalog-and-mvcc.md "Snapshot resolution", ADR-0003, ADR-0010 §2/§10).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use prost::Message;
use ravel_commit::keys::BucketEntry;
use ravel_commit::{keys, record, signal};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all};
use ravel_proto::commit::v1::{CommitRecord, CompactionPart, CompactionRecord};
use ravel_types::{CommitToken, Signal, TenantHash, TimeRange};
use uuid::Uuid;

use crate::cache::{CompactionRecordCache, HeadCache, PartCache, PostingsCache, RecordCache};
use crate::config::CatalogConfig;
use crate::error::CatalogError;
use crate::snapshot::{SegmentLevel, SegmentRef, Snapshot};

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Delay before the single retry on an exact `min_token` GET
/// (docs/catalog-and-mvcc.md step 4: "GET it directly ... with one retry").
/// `MemoryStore` is strongly consistent so tests never observe this delay;
/// it exists for real backends with brief propagation lag.
const MIN_TOKEN_RETRY_DELAY: Duration = Duration::from_millis(20);

/// Listing-based catalog over an object store backend (Phase 1, ADR-0003).
/// A future compaction phase folds commit records into immutable snapshot
/// objects behind a CAS'd HEAD pointer; this type's public API does not
/// change when that lands.
pub struct Catalog {
    store: Arc<dyn ObjectStoreBackend>,
    config: CatalogConfig,
    cache: RecordCache,
    compaction_cache: CompactionRecordCache,
    head_cache: HeadCache,
    part_cache: PartCache,
    postings_cache: PostingsCache,
    /// Count of unlisted L0 records observed postdating a compaction record
    /// in their bucket (docs/catalog-and-mvcc.md step 3: an interlock
    /// breach, since a flush should have sealed before compaction ran). The
    /// segment is still included for correctness (overlap harmlessness); the
    /// counter surfaces the anomaly to metrics.
    interlock_violations: AtomicU64,
    /// Count of buckets observed holding two compaction records with
    /// different `input_set_hash` (docs/catalog-and-mvcc.md step 3, §3.6 row
    /// 11: a sealed bucket must yield exactly one input set). Both parts
    /// sets plus all uncovered L0s are still included (harmless overlap);
    /// the counter surfaces the invariant breach for a human to investigate.
    compaction_input_set_conflicts: AtomicU64,
}

impl Catalog {
    /// Errors if `config.shard_count == 0` (a resolvable catalog needs at
    /// least one shard).
    pub fn new(
        store: Arc<dyn ObjectStoreBackend>,
        config: CatalogConfig,
    ) -> Result<Self, CatalogError> {
        if config.shard_count == 0 {
            return Err(CatalogError::InvalidConfig);
        }
        Ok(Catalog {
            store,
            config,
            cache: RecordCache::default(),
            compaction_cache: CompactionRecordCache::default(),
            head_cache: HeadCache::default(),
            part_cache: PartCache::default(),
            postings_cache: PostingsCache::default(),
            interlock_violations: AtomicU64::new(0),
            compaction_input_set_conflicts: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &CatalogConfig {
        &self.config
    }

    /// Number of writer-interlock violations observed across this catalog's
    /// lifetime (an L0 commit record not named in any compaction record's
    /// input list, whose `created_unix_ns` postdates that record). See
    /// docs/catalog-and-mvcc.md step 3.
    pub fn interlock_violations(&self) -> u64 {
        self.interlock_violations.load(Ordering::Relaxed)
    }

    /// Number of buckets observed with two compaction records carrying
    /// different `input_set_hash` (docs/catalog-and-mvcc.md step 3, §3.6
    /// row 11).
    pub fn compaction_input_set_conflicts(&self) -> u64 {
        self.compaction_input_set_conflicts.load(Ordering::Relaxed)
    }

    /// `pub(crate)`: lets `fold` (docs/metric-index-plan.md section 4) issue
    /// its own LIST/GET/PUT calls through the same store handle, in its own
    /// `impl Catalog` block in `fold.rs`, without duplicating the
    /// `store`/`config`/`cache` fields in a separate type.
    pub(crate) fn store(&self) -> &dyn ObjectStoreBackend {
        self.store.as_ref()
    }

    /// `pub(crate)`: lets `snapshot_resolve` (docs/metric-index-plan.md 5.1)
    /// share the decoded-HEAD cache from its own `impl Catalog` block.
    pub(crate) fn head_cache(&self) -> &HeadCache {
        &self.head_cache
    }

    /// `pub(crate)`: lets `snapshot_resolve` share the decoded-part cache.
    pub(crate) fn part_cache(&self) -> &PartCache {
        &self.part_cache
    }

    /// `pub(crate)`: lets `snapshot_resolve` share the decoded-postings
    /// cache (P5b).
    pub(crate) fn postings_cache(&self) -> &PostingsCache {
        &self.postings_cache
    }

    /// Resolve a query-time snapshot (docs/catalog-and-mvcc.md "Snapshot
    /// resolution"):
    ///
    /// 1. List every commit prefix for (shard, ingest-hour bucket)
    ///    overlapping `[range.start_ns - max_ingest_lag, now_ns +
    ///    clock_skew_allowance]`, decode and cache the records, and filter
    ///    by event-time overlap with `range`.
    /// 2. For each `min_token`, reconstruct its exact commit key and GET it
    ///    directly (never by re-listing), including its segment even if its
    ///    event range does not overlap `range` (read-your-write). Missing
    ///    after one retry is [`CatalogError::UnsatisfiableToken`].
    ///
    /// `now_ns` is always caller-supplied: this crate never reads a clock.
    ///
    /// The (shard, hour) loop issues one LIST per pair (Phase 1, ADR-0003);
    /// callers must keep `range`/`now_ns` and `config.shard_count` bounded
    /// or this call issues a very large number of LIST requests.
    pub async fn resolve(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
    ) -> Result<Snapshot, CatalogError> {
        self.resolve_impl(tenant, signal, range, min_tokens, now_ns, None)
            .await
    }

    /// Like [`Catalog::resolve`], but applies postings-based segment pruning
    /// (P5b, docs/metric-index-plan.md 5.4) when `name_filter` is `Some`: an
    /// equality `__name__` matcher value from the caller's query.
    ///
    /// Pruning only ever removes snapshot-sourced segments this snapshot's
    /// postings provably do not carry that name; listing- and
    /// `min_token`-sourced segments are never touched (they never pass
    /// through the postings-aware code path at all), and missing or corrupt
    /// postings silently degrade to the same behavior as [`Catalog::resolve`]
    /// (docs/metric-index-plan.md "exact semantics by default": approximate
    /// or unavailable pruning data must never fail a query or drop a
    /// segment that a matcher could actually match).
    ///
    /// A separate method rather than a `resolve` parameter: this keeps
    /// `Catalog::resolve`'s existing signature and every call site
    /// (in-crate and external) unchanged, per this ticket's "keep the
    /// `Catalog` public API stable if possible" requirement.
    pub async fn resolve_pruned(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
    ) -> Result<Snapshot, CatalogError> {
        self.resolve_impl(tenant, signal, range, min_tokens, now_ns, name_filter)
            .await
    }

    async fn resolve_impl(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
    ) -> Result<Snapshot, CatalogError> {
        let mut segments: HashMap<String, SegmentRef> = HashMap::new();
        let mut segments_pruned = 0u64;

        if let Some((window_start_hour, window_end_hour)) = self.window_hour_bounds(range, now_ns) {
            // A snapshot at watermark W serves every window hour <= W
            // straight from its parts; only the suffix above W is listed
            // (docs/metric-index-plan.md 5.1 step 3). No usable snapshot,
            // or a watermark below the window's start, falls back to
            // listing the whole window, unchanged from Phase 1.
            let window = self.resolve_snapshot_window(tenant, signal, now_ns).await?;
            let listing_start_hour = match &window {
                Some(window) if window.watermark_hour >= window_start_hour => {
                    let snapshot_end_hour = window_end_hour.min(window.watermark_hour);
                    window.extract_into(
                        tenant,
                        signal,
                        window_start_hour,
                        snapshot_end_hour,
                        &range,
                        name_filter,
                        &mut segments_pruned,
                        &mut segments,
                    )?;
                    window.watermark_hour.saturating_add(1)
                }
                _ => window_start_hour,
            };
            for shard in 0..self.config.shard_count {
                for hour in listing_start_hour..=window_end_hour {
                    self.list_hour_bucket(tenant, signal, shard, hour, &range, &mut segments)
                        .await?;
                }
            }
        }

        for token in min_tokens {
            self.resolve_min_token(tenant, signal, token, &mut segments)
                .await?;
        }

        let mut segments: Vec<SegmentRef> = segments.into_values().collect();
        // Deterministic total order: the cross-segment dedup provenance order
        // named in docs/catalog-and-mvcc.md (created_unix_ns, writer_epoch,
        // writer_seq), with shard then writer_id as final tiebreaks. writer_id
        // makes the key total over distinct segments: two same-shard segments
        // from different writers can tie on (created_unix_ns, writer_epoch,
        // writer_seq) (seq is monotonic only per (writer_id, epoch, shard),
        // ADR-0010 §3), and without an identity tiebreak the stable sort would
        // otherwise leave them in randomized HashMap iteration order (a4-F01).
        //
        // Mixed L0/L1 levels stay a deterministic total order
        // (docs/catalog-and-mvcc.md "Snapshot resolution"): an L1 part has
        // writer_epoch/seq == 0 and writer_id == nil, so it slots into the
        // same chain by its record's created_unix_ns and gets its
        // input_set_hash then part_index as the final tiebreaks (a level tag
        // separates the two tiebreak families). L0 ordering is unchanged: the
        // appended L1-only key components are constant across L0 refs.
        segments.sort_by_key(segment_sort_key);
        Ok(Snapshot {
            segments,
            segments_pruned,
        })
    }

    /// The (start_hour, end_hour) ingest-hour bucket range the listing
    /// window covers, inclusive, or `None` if the window is empty. Applies
    /// to every shard alike; callers cross it with `0..shard_count`
    /// themselves.
    fn window_hour_bounds(&self, range: TimeRange, now_ns: i64) -> Option<(u32, u32)> {
        let window_start_ns = range.start_ns.saturating_sub(self.config.max_ingest_lag_ns);
        let window_end_ns = now_ns.saturating_add(self.config.clock_skew_allowance_ns);
        if window_end_ns < 0 {
            return None;
        }
        let start_hour = window_start_ns.div_euclid(NS_PER_HOUR).max(0);
        let end_hour = window_end_ns.div_euclid(NS_PER_HOUR);
        if start_hour > end_hour {
            return None;
        }
        let start_hour = u32::try_from(start_hour).unwrap_or(0);
        let end_hour = u32::try_from(end_hour).unwrap_or(u32::MAX);
        Some((start_hour, end_hour))
    }

    async fn list_hour_bucket(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        hour: u32,
        range: &TimeRange,
        out: &mut HashMap<String, SegmentRef>,
    ) -> Result<(), CatalogError> {
        let prefix = keys::commit_shard_hour_prefix(tenant, signal, shard, hour)?;
        let objects = list_all(self.store.as_ref(), &prefix).await?;

        // Partition the listed keys by shape (docs/catalog-and-mvcc.md step
        // 2). An unrecognized shape is a fail-loud error, never a silent
        // skip (plan §3.1: fail-loud on layout drift).
        let mut l0_keys: Vec<String> = Vec::new();
        let mut compaction_keys: Vec<String> = Vec::new();
        let mut has_tombstone = false;
        for meta in objects {
            match keys::partition_bucket_entry(&meta.key)? {
                BucketEntry::CommitRecord(_) => l0_keys.push(meta.key),
                BucketEntry::CompactionRecord(_) => compaction_keys.push(meta.key),
                BucketEntry::Tombstone(_) => has_tombstone = true,
            }
        }

        // Tombstone present: the bucket contributes nothing (ADR-0019,
        // docs/catalog-and-mvcc.md step 3). Observing the tombstone also
        // invalidates this bucket's cached commit and compaction records
        // (ADR-0010 §10's promised trigger), so a later token-fallback GET
        // cannot serve a record the retention sweep is about to remove.
        if has_tombstone {
            self.invalidate_bucket_cache(tenant, &prefix);
            return Ok(());
        }

        // No compaction record: Phase 1 behavior, every overlapping L0.
        if compaction_keys.is_empty() {
            for key in &l0_keys {
                self.include_l0_if_overlaps(tenant, signal, shard, key, range, out)
                    .await?;
            }
            return Ok(());
        }

        // Compaction record(s) present (docs/catalog-and-mvcc.md step 3):
        // include each record's parts (event-bound filtered), collect the
        // input identities to exclude, and remember the newest record's
        // created_unix_ns for the interlock check on unlisted L0s.
        let mut excluded: HashSet<(String, u64, u64)> = HashSet::new();
        let mut newest_record_created_ns = i64::MIN;
        let mut input_set_hashes: HashSet<Vec<u8>> = HashSet::new();
        for ckey in &compaction_keys {
            let record = self
                .load_and_validate_compaction(tenant, signal, shard, ckey)
                .await?;
            input_set_hashes.insert(record.input_set_hash.clone());
            newest_record_created_ns = newest_record_created_ns.max(record.created_unix_ns);
            for part in &record.parts {
                let segment_ref = build_l1_segment_ref(&record, part, ckey)?;
                let event_range = TimeRange {
                    start_ns: segment_ref.min_event_ts_ns,
                    end_ns: segment_ref.max_event_ts_ns,
                };
                if event_range.overlaps(range) {
                    out.entry(segment_ref.data_object_key.clone())
                        .or_insert(segment_ref);
                }
            }
            for input in &record.inputs {
                excluded.insert((
                    input.writer_id.clone(),
                    input.writer_epoch,
                    input.writer_seq,
                ));
            }
        }

        // Two records with different input_set_hash in one bucket: both parts
        // sets are already included above (harmless overlap); alarm loudly
        // (docs/catalog-and-mvcc.md step 3, §3.6 row 11).
        if input_set_hashes.len() > 1 {
            self.compaction_input_set_conflicts
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                shard,
                hour,
                records = input_set_hashes.len(),
                "compaction interlock breach: sealed bucket holds multiple input sets"
            );
        }

        // L0 records: exclude exactly those named in an input list; include
        // any unlisted one normally, raising the interlock metric if it
        // postdates the newest compaction record (docs/catalog-and-mvcc.md
        // step 3).
        for key in &l0_keys {
            let record = self.load_and_validate(tenant, signal, shard, key).await?;
            let identity = (
                record.writer_id.clone(),
                record.writer_epoch,
                record.writer_seq,
            );
            if excluded.contains(&identity) {
                continue;
            }
            if record.created_unix_ns > newest_record_created_ns {
                self.interlock_violations.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    shard,
                    hour,
                    key = %key,
                    "writer-interlock violation: L0 record postdates its bucket's compaction record"
                );
            }
            let event_range = TimeRange {
                start_ns: record.min_event_ts_ns,
                end_ns: record.max_event_ts_ns,
            };
            if event_range.overlaps(range) {
                let segment_ref = build_segment_ref(key, &record)?;
                out.entry(segment_ref.data_object_key.clone())
                    .or_insert(segment_ref);
            }
        }
        Ok(())
    }

    /// Load one L0 commit record and, if its event range overlaps `range`,
    /// add its segment ref to `out`. The plain Phase 1 include path, shared
    /// by the no-compaction fast path.
    async fn include_l0_if_overlaps(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        key: &str,
        range: &TimeRange,
        out: &mut HashMap<String, SegmentRef>,
    ) -> Result<(), CatalogError> {
        let record = self.load_and_validate(tenant, signal, shard, key).await?;
        let event_range = TimeRange {
            start_ns: record.min_event_ts_ns,
            end_ns: record.max_event_ts_ns,
        };
        if event_range.overlaps(range) {
            let segment_ref = build_segment_ref(key, &record)?;
            out.entry(segment_ref.data_object_key.clone())
                .or_insert(segment_ref);
        }
        Ok(())
    }

    /// Load, decode, validate, and cache a compaction record, keyed by full
    /// object key, exactly as [`Catalog::load_and_validate`] does for commit
    /// records (docs/catalog-and-mvcc.md step 2). Validation covers
    /// tenant_hash/signal/shard against the (tenant, signal, shard) it was
    /// listed under plus the reconstruct-and-verify of its own key
    /// (ADR-0010 §7).
    async fn load_and_validate_compaction(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        key: &str,
    ) -> Result<Arc<CompactionRecord>, CatalogError> {
        if let Some(cached) = self.compaction_cache.get(tenant, key) {
            validate_compaction_expected_fields(&cached, tenant, signal, shard, key)?;
            return Ok(cached);
        }
        let got = self.store.get(key, GetRange::Full).await?;
        let record = CompactionRecord::decode(got.data.as_ref()).map_err(|e| {
            CatalogError::CompactionRecordDecode {
                key: key.to_string(),
                source: e,
            }
        })?;
        validate_compaction_expected_fields(&record, tenant, signal, shard, key)?;
        let record = Arc::new(record);
        self.compaction_cache.insert(
            *tenant,
            key.to_string(),
            record.clone(),
            self.config.cache_capacity_per_tenant,
        );
        Ok(record)
    }

    /// Drop the cached commit and compaction records for one bucket, keyed by
    /// its `c/<shard>/<hour>/` prefix. The single tombstone-observation
    /// invalidation path (ADR-0010 §10): called from both the listing resolve
    /// and the token fallback the moment a tombstone is seen. Both record
    /// caches key by full object key, and every record in a bucket shares this
    /// prefix, so one prefix drop clears the bucket from both.
    fn invalidate_bucket_cache(&self, tenant: &TenantHash, bucket_prefix: &str) {
        self.cache.invalidate_prefix(tenant, bucket_prefix);
        self.compaction_cache
            .invalidate_prefix(tenant, bucket_prefix);
    }

    async fn resolve_min_token(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        token: &CommitToken,
        out: &mut HashMap<String, SegmentRef>,
    ) -> Result<(), CatalogError> {
        let key = keys::commit_key_for_token(tenant, signal, token)?;
        if let Some(cached) = self.cache.get(tenant, &key)
            && validate_expected_fields(&cached, tenant, signal, token.shard, &key).is_ok()
        {
            let segment_ref = build_segment_ref(&key, &cached)?;
            out.entry(segment_ref.data_object_key.clone())
                .or_insert(segment_ref);
            return Ok(());
        }

        // Independent retry budgets for the two distinct failure modes. A
        // transient store fault (Throttled/Timeout/Transient) and a NotFound
        // (a real record not yet propagated) are unrelated, so they must not
        // draw from one shared budget: if they did, a transient blip would
        // spend the single NotFound-propagation retry the spec grants
        // (docs/catalog-and-mvcc.md step 4: "Absent after one retry"), so a
        // real but briefly-unpropagated commit would surface as a spurious
        // UnsatisfiableToken, violating read-your-write (a4-F02). NotFound
        // keeps exactly one retry (two probes) as documented; transient
        // errors keep their own independent single retry.
        let mut notfound_retries: u32 = 1;
        let mut transient_retries: u32 = 1;
        loop {
            match self.store.get(&key, GetRange::Full).await {
                Ok(got) => {
                    let record = record::decode(&got.data)?;
                    validate_expected_fields(&record, tenant, signal, token.shard, &key)?;
                    let record = Arc::new(record);
                    self.cache.insert(
                        *tenant,
                        key.clone(),
                        record.clone(),
                        self.config.cache_capacity_per_tenant,
                    );
                    let segment_ref = build_segment_ref(&key, &record)?;
                    out.entry(segment_ref.data_object_key.clone())
                        .or_insert(segment_ref);
                    return Ok(());
                }
                Err(StoreError::NotFound) if notfound_retries > 0 => {
                    notfound_retries -= 1;
                    tokio::time::sleep(MIN_TOKEN_RETRY_DELAY).await;
                }
                Err(StoreError::NotFound) => {
                    // The commit record is absent after one propagation retry.
                    // Before declaring the token unsatisfiable, try the
                    // compaction/tombstone fallback (docs/catalog-and-mvcc.md
                    // step 5): a swept-post-compaction record is served via its
                    // L1 parts, a tombstoned bucket is satisfied with zero
                    // segments.
                    return self
                        .resolve_min_token_fallback(tenant, signal, token, out)
                        .await;
                }
                Err(e) if e.is_retryable() && transient_retries > 0 => {
                    transient_retries -= 1;
                    tokio::time::sleep(MIN_TOKEN_RETRY_DELAY).await;
                }
                Err(e) => return Err(CatalogError::Store(e)),
            }
        }
    }

    /// Token fallback when the exact commit-record GET returned NotFound
    /// after its retry (docs/catalog-and-mvcc.md step 5). The token fully
    /// determines its (shard, ingest_hour) bucket, but not the
    /// `input_set_hash16` a compaction record's key embeds, so this LISTs the
    /// bucket to discover its compaction records and tombstone:
    ///
    /// - Tombstone present: satisfied with zero segments (the data was
    ///   retired, not lost).
    /// - The token's writer identity found in a compaction record's input
    ///   list: satisfied via that record's parts (all of them; the token's
    ///   data is somewhere among them, and read-your-write ignores event
    ///   range just as the L0 exact-token path does).
    /// - Neither: `unsatisfiable token`, unchanged.
    async fn resolve_min_token_fallback(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        token: &CommitToken,
        out: &mut HashMap<String, SegmentRef>,
    ) -> Result<(), CatalogError> {
        let prefix =
            keys::commit_shard_hour_prefix(tenant, signal, token.shard, token.ingest_hour_bucket)?;
        let objects = list_all(self.store.as_ref(), &prefix).await?;
        let mut compaction_keys: Vec<String> = Vec::new();
        for meta in objects {
            match keys::partition_bucket_entry(&meta.key)? {
                BucketEntry::Tombstone(_) => {
                    // Satisfied with zero segments (ADR-0019 decision 3), and
                    // observing the tombstone invalidates this bucket's cached
                    // records (ADR-0010 §10), same as the listing path.
                    self.invalidate_bucket_cache(tenant, &prefix);
                    return Ok(());
                }
                BucketEntry::CompactionRecord(_) => compaction_keys.push(meta.key),
                BucketEntry::CommitRecord(_) => {}
            }
        }

        let token_identity = (token.writer_id.to_string(), token.epoch, token.seq);
        for ckey in &compaction_keys {
            let record = self
                .load_and_validate_compaction(tenant, signal, token.shard, ckey)
                .await?;
            let covers = record.inputs.iter().any(|input| {
                (
                    input.writer_id.clone(),
                    input.writer_epoch,
                    input.writer_seq,
                ) == token_identity
            });
            if covers {
                for part in &record.parts {
                    let segment_ref = build_l1_segment_ref(&record, part, ckey)?;
                    out.entry(segment_ref.data_object_key.clone())
                        .or_insert(segment_ref);
                }
                return Ok(());
            }
        }

        Err(unsatisfiable_token(token))
    }

    /// `pub(crate)`: also called by `fold` (docs/metric-index-plan.md
    /// section 4) to load and validate commit records found by bucket
    /// listing, reusing this cache-first GET+decode+validate path.
    pub(crate) async fn load_and_validate(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        key: &str,
    ) -> Result<Arc<CommitRecord>, CatalogError> {
        if let Some(cached) = self.cache.get(tenant, key) {
            validate_expected_fields(&cached, tenant, signal, shard, key)?;
            return Ok(cached);
        }
        let got = self.store.get(key, GetRange::Full).await?;
        let record = record::decode(&got.data)?;
        validate_expected_fields(&record, tenant, signal, shard, key)?;
        let record = Arc::new(record);
        self.cache.insert(
            *tenant,
            key.to_string(),
            record.clone(),
            self.config.cache_capacity_per_tenant,
        );
        Ok(record)
    }
}

/// Deterministic total-order key over a mixed L0/L1 segment set
/// (docs/catalog-and-mvcc.md "Cross-segment duplicate samples" and "Snapshot
/// resolution"). L0 refs keep their exact previous order: the trailing L1
/// discriminator/`input_set_hash`/`part_index` components are constant
/// (0/[0; 32]/0) for every L0 ref, so they never reorder L0-vs-L0. L1 refs
/// carry `writer_epoch`/`writer_seq` == 0 and `writer_id` == nil, so they
/// order by their record's `created_unix_ns` then, past the level tag, by
/// `input_set_hash` then `part_index`.
fn segment_sort_key(s: &SegmentRef) -> (i64, u64, u64, u32, Uuid, u8, [u8; 32], u32) {
    let (level_tag, input_set_hash, part_index) = match &s.level {
        SegmentLevel::L0 => (0u8, [0u8; 32], 0u32),
        SegmentLevel::L1 {
            input_set_hash,
            part_index,
        } => (1u8, *input_set_hash, *part_index),
    };
    (
        s.created_unix_ns,
        s.writer_epoch,
        s.writer_seq,
        s.shard,
        s.writer_id,
        level_tag,
        input_set_hash,
        part_index,
    )
}

fn unsatisfiable_token(token: &CommitToken) -> CatalogError {
    CatalogError::UnsatisfiableToken {
        shard: token.shard,
        writer_id: token.writer_id.to_string(),
        epoch: token.epoch,
        seq: token.seq,
        ingest_hour_bucket: token.ingest_hour_bucket,
    }
}

/// Validate a decoded record's tenant_hash/signal/shard against the
/// (tenant, signal, shard) it was listed or addressed under (ADR-0010 §10:
/// checked on every cache hit and every fresh decode).
fn validate_expected_fields(
    record: &CommitRecord,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    key: &str,
) -> Result<(), CatalogError> {
    if record.tenant_hash.as_slice() != tenant.0.as_slice() {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant.to_hex(),
            actual: format!("{:?}", record.tenant_hash),
        });
    }
    if signal::from_proto(record.signal) != Ok(signal) {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "signal",
            expected: format!("{signal:?}"),
            actual: format!("{:?}", record.signal),
        });
    }
    if record.shard != shard {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "shard",
            expected: shard.to_string(),
            actual: record.shard.to_string(),
        });
    }
    Ok(())
}

/// Validate a decoded compaction record's tenant_hash/signal/shard against
/// the (tenant, signal, shard) it was listed or addressed under, and verify
/// its own key reconstructs to the key it was found at (ADR-0010 §7). The
/// compaction-record analog of [`validate_expected_fields`].
fn validate_compaction_expected_fields(
    record: &CompactionRecord,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    key: &str,
) -> Result<(), CatalogError> {
    if record.tenant_hash.as_slice() != tenant.0.as_slice() {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "tenant_hash",
            expected: tenant.to_hex(),
            actual: format!("{:?}", record.tenant_hash),
        });
    }
    if signal::from_proto(record.signal) != Ok(signal) {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "signal",
            expected: format!("{signal:?}"),
            actual: format!("{:?}", record.signal),
        });
    }
    if record.shard != shard {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "shard",
            expected: shard.to_string(),
            actual: record.shard.to_string(),
        });
    }
    keys::verify_compaction_record_key(record, key).map_err(|source| {
        CatalogError::Reconstruction {
            key: key.to_string(),
            source,
        }
    })?;
    Ok(())
}

/// Build an L1 [`SegmentRef`] from a compaction record and one of its parts,
/// reconstructing the part key from their identity fields (ADR-0010 §7,
/// never a stored string). `observed_ckey` names the compaction record for
/// error messages. The footer of the part object is later verified against
/// these same fields by the reader (docs/compaction-retention-plan.md §3.5).
fn build_l1_segment_ref(
    record: &CompactionRecord,
    part: &CompactionPart,
    observed_ckey: &str,
) -> Result<SegmentRef, CatalogError> {
    let data_object_key = keys::reconstruct_l1_part_key(record, part)?;
    let content_hash: [u8; 32] =
        part.content_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: observed_ckey.to_string(),
                field: "part content_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", part.content_hash.len()),
            })?;
    let input_set_hash: [u8; 32] =
        record
            .input_set_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: observed_ckey.to_string(),
                field: "input_set_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", record.input_set_hash.len()),
            })?;
    Ok(SegmentRef {
        data_object_key,
        object_size: part.object_size,
        min_event_ts_ns: part.min_event_ts_ns,
        max_event_ts_ns: part.max_event_ts_ns,
        ingest_hour_bucket: record.ingest_hour_bucket,
        sample_count: part.sample_count,
        series_count: part.series_count,
        shard: record.shard,
        content_hash,
        // A part has no writer identity of its own (plan §4): these are
        // never used for an L1 ref's identity or dedup.
        writer_id: Uuid::nil(),
        writer_epoch: 0,
        writer_seq: 0,
        created_unix_ns: record.created_unix_ns,
        level: SegmentLevel::L1 {
            input_set_hash,
            part_index: part.part_index,
        },
    })
}

fn build_segment_ref(key: &str, record: &CommitRecord) -> Result<SegmentRef, CatalogError> {
    let data_object_key =
        keys::verify_object_key(record).map_err(|source| CatalogError::Reconstruction {
            key: key.to_string(),
            source,
        })?;
    let content_hash: [u8; 32] =
        record
            .content_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: key.to_string(),
                field: "content_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", record.content_hash.len()),
            })?;
    let writer_id =
        Uuid::parse_str(&record.writer_id).map_err(|_| CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "writer_id",
            expected: "uuid".to_string(),
            actual: record.writer_id.clone(),
        })?;
    Ok(SegmentRef {
        data_object_key,
        object_size: record.object_size,
        min_event_ts_ns: record.min_event_ts_ns,
        max_event_ts_ns: record.max_event_ts_ns,
        ingest_hour_bucket: record.ingest_hour_bucket,
        sample_count: record.sample_count,
        series_count: record.series_count,
        shard: record.shard,
        content_hash,
        writer_id,
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
        created_unix_ns: record.created_unix_ns,
        level: SegmentLevel::L0,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::NewCommitRecord;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Op, ScriptedFault, Sequence,
    };
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    fn tenant() -> TenantHash {
        TenantHash([0xab; 16])
    }

    fn content_hash_for(payload: &[u8]) -> [u8; 32] {
        *blake3::hash(payload).as_bytes()
    }

    fn config(shard_count: u32) -> CatalogConfig {
        CatalogConfig {
            shard_count,
            ..Default::default()
        }
    }

    /// Build, PUT the data object, and publish a fully self-consistent
    /// commit record for one segment. Each call uses a fresh writer id.
    async fn publish_segment(
        store: &MemoryStore,
        shard: u32,
        seq: u64,
        ingest_hour_bucket: u32,
        created_unix_ns: i64,
        min_event_ts_ns: i64,
        max_event_ts_ns: i64,
    ) -> CommitRecord {
        let writer_id = Uuid::new_v4();
        let payload = format!("segment-{shard}-{seq}-{writer_id}").into_bytes();
        let content_hash = content_hash_for(&payload);
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns,
            max_event_ts_ns,
            min_ingest_ts_ns: min_event_ts_ns,
            max_ingest_ts_ns: max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store, &data_key, Bytes::from(payload))
            .await
            .expect("put data object");
        publish::publish(store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        record
    }

    #[tokio::test]
    async fn publish_then_resolve_returns_the_segment() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let record = publish_segment(&store, 0, 1, 500_000, now, now - 1_000, now).await;

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(snapshot.segments.len(), 1);
        let seg = &snapshot.segments[0];
        assert_eq!(
            seg.data_object_key,
            keys::reconstruct_data_key(&record).expect("key")
        );
        assert_eq!(seg.shard, 0);
        assert_eq!(seg.content_hash.to_vec(), record.content_hash);
    }

    #[tokio::test]
    async fn data_object_without_commit_record_is_never_visible() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let writer_id = Uuid::new_v4();
        let content_hash = content_hash_for(b"orphan");
        let key = keys::data_key(
            &tenant(),
            Signal::Metrics,
            0,
            writer_id,
            1,
            1,
            &content_hash,
        )
        .expect("data key");
        publish::put_data_object(store.as_ref(), &key, Bytes::from_static(b"orphan"))
            .await
            .expect("put data object");

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert!(snapshot.segments.is_empty());
    }

    #[tokio::test]
    async fn duplicate_publish_same_content_hash_is_idempotent_single_segment() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let payload = b"identical-bytes";
        let content_hash = content_hash_for(payload);
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::new_v4(),
            writer_epoch: 1,
            writer_seq: 1,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 100,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 100,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store.as_ref(), &data_key, Bytes::from_static(payload))
            .await
            .expect("put data object");
        let token1 = publish::publish(store.as_ref(), &record, &RetryPolicy::default())
            .await
            .expect("first publish");
        let token2 = publish::publish(store.as_ref(), &record, &RetryPolicy::default())
            .await
            .expect("retry publish is idempotent");
        assert_eq!(token1, token2);

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(snapshot.segments.len(), 1);
    }

    #[tokio::test]
    async fn different_content_hash_same_identity_is_split_brain() {
        let store = Arc::new(MemoryStore::new());
        let now = 500_000 * NS_PER_HOUR;
        let writer_id = Uuid::new_v4();
        let base = NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 7,
            object_size: 5,
            content_hash: content_hash_for(b"a"),
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 100,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 100,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        };
        let record_a = record::build(base.clone()).expect("valid record a");
        publish::publish(store.as_ref(), &record_a, &RetryPolicy::default())
            .await
            .expect("first publish");

        let mut record_b = record_a.clone();
        record_b.content_hash = content_hash_for(b"b").to_vec();
        let err = publish::publish(store.as_ref(), &record_b, &RetryPolicy::default())
            .await
            .expect_err("must be split-brain");
        assert!(matches!(err, publish::PublishError::SplitBrain { .. }));
    }

    #[tokio::test]
    async fn snapshot_is_stable_after_further_publishes() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let record_1 = publish_segment(&store, 0, 1, 500_000, now, now - 500, now).await;
        let first = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve 1");
        assert_eq!(first.segments.len(), 1);
        let first_key = first.segments[0].data_object_key.clone();
        assert_eq!(
            first_key,
            keys::reconstruct_data_key(&record_1).expect("key")
        );

        publish_segment(&store, 0, 2, 500_000, now, now - 500, now).await;
        let second = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve 2");
        assert_eq!(second.segments.len(), 2);

        // The snapshot returned earlier still names exactly the one segment
        // it was resolved with, unaffected by the later publish (MVCC: a
        // pinned snapshot, not a live view).
        assert_eq!(first.segments.len(), 1);
        assert_eq!(first.segments[0].data_object_key, first_key);
    }

    #[tokio::test]
    async fn event_time_filtering_excludes_out_of_range_segments() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let in_range = publish_segment(&store, 0, 1, 500_000, now, now - 100, now - 50).await;
        // Same ingest hour (so it IS listed), but its event range does not
        // overlap the query range.
        let _out_of_range = publish_segment(
            &store,
            0,
            2,
            500_000,
            now,
            now - 10_000_000_000,
            now - 9_000_000_000,
        )
        .await;

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(snapshot.segments.len(), 1);
        assert_eq!(
            snapshot.segments[0].data_object_key,
            keys::reconstruct_data_key(&in_range).expect("key")
        );
    }

    /// ADR-0010 §2 scenario: a writer with a skewed clock pins an
    /// ingest_hour_bucket well outside the `now`-anchored listing window.
    /// Its commit token still resolves via an exact GET.
    #[tokio::test]
    async fn min_token_resolves_even_when_its_hour_bucket_is_outside_the_listing_window() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        // Listing window (defaults: 2h lag, 5m skew) covers roughly hours
        // [499_997, 500_000]; this bucket is far outside it.
        let skewed_bucket = 500_000 - 10;
        let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
        let record = publish_segment(
            &store,
            0,
            1,
            skewed_bucket,
            skewed_created,
            skewed_created,
            skewed_created + 1_000,
        )
        .await;
        let token = record::token_for(&record).expect("token");

        let without_token = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve without token");
        assert!(
            without_token.segments.is_empty(),
            "must be outside the listing window"
        );

        let with_token = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect("resolve with token");
        assert_eq!(with_token.segments.len(), 1);
        assert_eq!(
            with_token.segments[0].data_object_key,
            keys::reconstruct_data_key(&record).expect("key")
        );
    }

    #[tokio::test]
    async fn min_token_unsatisfiable_when_commit_record_is_missing() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let token = CommitToken {
            shard: 0,
            writer_id: Uuid::new_v4(),
            epoch: 1,
            seq: 1,
            ingest_hour_bucket: 500_000,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[token], now)
            .await
            .expect_err("unsatisfiable");
        assert!(matches!(err, CatalogError::UnsatisfiableToken { .. }));
    }

    #[tokio::test]
    async fn object_key_mismatch_is_fatal_during_resolve() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let mut record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::new_v4(),
            writer_epoch: 1,
            writer_seq: 1,
            object_size: 10,
            content_hash: content_hash_for(b"x"),
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 100,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 100,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        })
        .expect("valid record");
        // object_key is informational; nothing rejects a record whose field
        // does not match its own reconstructed key at publish time (ADR-0010
        // §7: only readers verify it).
        record.object_key = "t/deliberately/wrong/key".to_string();
        publish::publish(store.as_ref(), &record, &RetryPolicy::default())
            .await
            .expect("publish corrupted record");

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("fatal object_key mismatch");
        assert!(matches!(err, CatalogError::Reconstruction { .. }));
    }

    #[tokio::test]
    async fn shard_field_mismatch_against_listing_path_is_detected() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(2)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        let writer_id = Uuid::new_v4();
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 1,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: 10,
            content_hash: content_hash_for(b"z"),
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 100,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 100,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        })
        .expect("valid record");
        // Self-consistent record (shard=1), placed under shard 0's commit
        // path: a cross-wiring bug the per-hit/decode field check must catch
        // (ADR-0010 §10), independent of the object_key check.
        let wrong_shard_key = keys::commit_key(
            &tenant(),
            Signal::Metrics,
            0,
            500_000,
            writer_id,
            record.writer_epoch,
            record.writer_seq,
        )
        .expect("key");
        store
            .put(
                &wrong_shard_key,
                record::encode(&record),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put");

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("field mismatch");
        assert!(matches!(
            err,
            CatalogError::FieldMismatch { field: "shard", .. }
        ));
    }

    #[tokio::test]
    async fn pagination_is_exercised_and_all_pages_are_returned() {
        let store = Arc::new(MemoryStore::with_page_size(2));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        for seq in 1..=5u64 {
            publish_segment(&store, 0, seq, 500_000, now, now - 100, now).await;
        }
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(snapshot.segments.len(), 5);
    }

    #[tokio::test]
    async fn listing_window_boundary_hours_are_inclusive() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                shard_count: 1,
                max_ingest_lag_ns: NS_PER_HOUR,
                clock_skew_allowance_ns: 0,
                ..Default::default()
            },
        )
        .expect("catalog");
        let now = 500_000 * NS_PER_HOUR; // exactly on the hour
        let range = TimeRange {
            start_ns: now,
            end_ns: now,
        };
        // window = [range.start - 1h, now + 0] => hours 499_999..=500_000.
        let at_start_hour = publish_segment(&store, 0, 1, 499_999, now, now, now).await;
        let at_end_hour = publish_segment(&store, 0, 2, 500_000, now, now, now).await;
        let just_outside = publish_segment(&store, 0, 3, 499_998, now, now, now).await;

        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        let found: Vec<_> = snapshot
            .segments
            .iter()
            .map(|s| s.data_object_key.clone())
            .collect();
        assert!(found.contains(&keys::reconstruct_data_key(&at_start_hour).expect("k1")));
        assert!(found.contains(&keys::reconstruct_data_key(&at_end_hour).expect("k2")));
        assert!(!found.contains(&keys::reconstruct_data_key(&just_outside).expect("k3")));
    }

    /// a4-F02 regression: the exact-`min_token` GET must give transient
    /// store faults and NotFound propagation blips independent retry
    /// budgets. A transient blip followed by a NotFound blip against a real,
    /// acked commit must still resolve (read-your-write), not surface as
    /// `UnsatisfiableToken`. The `#[92]` `FaultStore` sequencing API scripts
    /// the two distinct faults on one key that a single `Rule` cannot.
    #[tokio::test]
    async fn min_token_transient_then_notfound_still_resolves_the_real_commit() {
        let inner = MemoryStore::new();
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        // Skewed bucket well outside the listing window, so the only GET on
        // the token key comes from the exact-token resolve path (never the
        // listing pass).
        let skewed_bucket = 500_000 - 10;
        let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
        let record = publish_segment(
            &inner,
            0,
            1,
            skewed_bucket,
            skewed_created,
            skewed_created,
            skewed_created + 1_000,
        )
        .await;
        let token = record::token_for(&record).expect("token");
        let key = keys::commit_key_for_token(&tenant(), Signal::Metrics, &token).expect("key");

        // Script the token GET: first a transient blip, then a NotFound blip,
        // then a scripted pass-through that returns the real record. Under
        // the shared-budget defect the transient consumed the single retry
        // and the NotFound surfaced as UnsatisfiableToken on the second call.
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(key)
                .then_fault(ScriptedFault::Transient("token get blip".into()))
                .then_fault(ScriptedFault::NotFoundBlip)
                .then_passthrough(),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let snapshot = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect("valid token must resolve despite a transient then a NotFound blip");
        assert_eq!(snapshot.segments.len(), 1);
        assert_eq!(
            snapshot.segments[0].data_object_key,
            keys::reconstruct_data_key(&record).expect("key")
        );

        // All three scripted steps fired: the transient retry and the
        // NotFound retry drew from independent budgets, then the record read
        // succeeded.
        assert_eq!(store.sequence_progress(0), 3, "all three steps consumed");
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Transient),
            1,
            "transient blip fired once"
        );
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::NotFoundBlip),
            1,
            "NotFound blip fired once"
        );
    }

    /// The NotFound propagation budget stays at exactly one retry (two
    /// probes) as documented (docs/catalog-and-mvcc.md step 4: "Absent after
    /// one retry"). Two scripted NotFound blips exhaust the budget and
    /// surface `UnsatisfiableToken` as its own typed outcome, even though a
    /// third probe would have found the (present) record. This proves the
    /// fix did not widen the NotFound budget.
    #[tokio::test]
    async fn min_token_two_notfound_blips_surface_unsatisfiable_not_over_probing() {
        let inner = MemoryStore::new();
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        let skewed_bucket = 500_000 - 10;
        let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
        let record = publish_segment(
            &inner,
            0,
            1,
            skewed_bucket,
            skewed_created,
            skewed_created,
            skewed_created + 1_000,
        )
        .await;
        let token = record::token_for(&record).expect("token");
        let key = keys::commit_key_for_token(&tenant(), Signal::Metrics, &token).expect("key");

        // Two NotFound blips then a pass-through. One retry is documented, so
        // the second NotFound is terminal: the pass-through (step index 2) is
        // never reached even though the record is present.
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(key)
                .then_fault(ScriptedFault::NotFoundBlip)
                .then_fault(ScriptedFault::NotFoundBlip)
                .then_passthrough(),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let err = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect_err("two NotFound probes must surface UnsatisfiableToken");
        assert!(matches!(err, CatalogError::UnsatisfiableToken { .. }));

        // Exactly two probes (one retry) fired; the third step was not
        // consumed, proving the NotFound budget was not widened.
        assert_eq!(
            store.sequence_progress(0),
            2,
            "only two NotFound probes; pass-through never reached"
        );
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::NotFoundBlip),
            2,
            "both NotFound blips fired"
        );
    }

    // --- Retention (ADR-0019): tombstone exclusion, token semantics, and the
    // ADR-0010 §10 cache-invalidation-on-tombstone trigger. ---

    /// Write a retention tombstone into a bucket, exactly as ravel-maintain's
    /// retention sweep would.
    async fn write_tombstone(store: &MemoryStore, shard: u32, ingest_hour_bucket: u32) {
        let tombstone = ravel_proto::commit::v1::RetentionTombstone {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket,
            retired_at_ns: 0,
            retention_window_ns: 0,
            record_count_observed: 0,
        };
        let key = keys::retention_tombstone_key_for(&tombstone).expect("tombstone key");
        store
            .put(
                &key,
                tombstone.encode_to_vec().into(),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put tombstone");
    }

    /// A resolver that lists a tombstone excludes the entire bucket from the
    /// snapshot (ADR-0019 decision 3).
    #[tokio::test]
    async fn tombstoned_bucket_is_excluded_from_resolution() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR;
        publish_segment(&store, 0, 1, 500_000, now, now - 100, now).await;
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };

        // Baseline: the segment resolves.
        let before = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(before.segments.len(), 1);

        // Tombstone the bucket: it now contributes nothing.
        write_tombstone(&store, 0, 500_000).await;
        let after = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert!(after.segments.is_empty(), "tombstoned bucket excluded");
    }

    /// A `min_commit_token` whose bucket is tombstoned resolves as satisfied
    /// with zero segments, not `unsatisfiable token`: the data was retired on
    /// purpose (ADR-0019 decision 3). Uses a skewed bucket outside the listing
    /// window so only the exact-token path is exercised.
    #[tokio::test]
    async fn token_over_tombstoned_bucket_is_satisfied_with_zero_segments() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        let skewed_bucket = 500_000 - 10;
        let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
        let record = publish_segment(
            &store,
            0,
            1,
            skewed_bucket,
            skewed_created,
            skewed_created,
            skewed_created + 1_000,
        )
        .await;
        let token = record::token_for(&record).expect("token");

        // Retire the bucket and remove the commit record (post physical sweep).
        write_tombstone(&store, 0, skewed_bucket).await;
        let commit_key = keys::commit_key_for_token(&tenant(), Signal::Metrics, &token).expect("commit key");
        store.delete(&commit_key).await.expect("delete commit record");

        let snapshot = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect("tombstoned token is satisfied, not unsatisfiable");
        assert!(snapshot.segments.is_empty(), "satisfied with zero segments");
    }

    /// Observing a tombstone during resolution invalidates that bucket's
    /// cached commit records (ADR-0010 §10). Proven on the token path: a
    /// record cached before the tombstone would otherwise be served straight
    /// from cache (bypassing the tombstone), but the listing pass's
    /// invalidation drops it, so the token then resolves as satisfied-empty.
    #[tokio::test]
    async fn tombstone_observation_invalidates_cached_commit_records() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        // In-window bucket, so the listing pass observes the tombstone.
        let bucket_hour = 500_000;
        let record = publish_segment(&store, 0, 1, bucket_hour, now, now - 100, now).await;
        let token = record::token_for(&record).expect("token");

        // First resolve WITH the token: caches the commit record (listing pass
        // and the exact-token path both populate the record cache).
        let first = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect("resolve");
        assert_eq!(first.segments.len(), 1);

        // Tombstone the bucket and delete the underlying commit record (a
        // mid-sweep state: record gone, tombstone present). The cache still
        // holds the record.
        write_tombstone(&store, 0, bucket_hour).await;
        let commit_key = keys::commit_key_for_token(&tenant(), Signal::Metrics, &token).expect("commit key");
        store.delete(&commit_key).await.expect("delete commit record");

        // Resolve WITH the token again. The listing pass observes the tombstone
        // and invalidates the bucket's cache; the token path then misses the
        // cache, GETs NotFound, falls back, sees the tombstone, and is
        // satisfied with zero segments. Without invalidation, the token path
        // would hit the stale cache and wrongly return the segment.
        let second = catalog
            .resolve(
                &tenant(),
                Signal::Metrics,
                range,
                std::slice::from_ref(&token),
                now,
            )
            .await
            .expect("resolve");
        assert!(
            second.segments.is_empty(),
            "invalidated cache: tombstoned bucket serves nothing on the token path"
        );
    }
}
