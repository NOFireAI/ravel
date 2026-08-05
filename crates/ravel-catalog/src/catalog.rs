//! Snapshot resolution: listing-based discovery over commit records
//! (docs/catalog-and-mvcc.md "Snapshot resolution", ADR-0003, ADR-0010 §2/§10).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{self, StreamExt};
use parking_lot::Mutex;
use prost::Message;
use ravel_cache::{Cache, CacheKey, CacheLimits};
use ravel_commit::keys::BucketEntry;
use ravel_commit::{keys, record, signal};
use ravel_object_store::{GetOutcome, GetRange, ObjectMeta, ObjectStoreBackend, StoreError};
use ravel_proto::commit::v1::{CommitRecord, CompactionPart, CompactionRecord};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{CommitToken, Signal, TenantHash, TimeRange};
use uuid::Uuid;

use crate::cache::{CompactionRecordCache, HeadCache, PartCache, PostingsCache, RecordCache};
use crate::config::CatalogConfig;
use crate::error::CatalogError;
use crate::snapshot::{SegmentLevel, SegmentRef, Snapshot};

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Bound on the object-store requests one resolve keeps in flight at once
/// (#278 item 2). A cold resolve issues one LIST per (shard, hour) plus one
/// GET per uncached commit/compaction record and per snapshot part; these
/// used to run strictly one await at a time. They now run concurrently up to
/// this bound, held by [`Catalog::request_semaphore`] and acquired only
/// around a single leaf request, so the fan-out collapses from k sequential
/// round trips toward ceil(k / this) without ever exceeding it.
pub(crate) const MAX_CONCURRENT_REQUESTS: usize = 16;
/// Delay before the single retry on an exact `min_token` GET
/// (docs/catalog-and-mvcc.md step 4: "GET it directly ... with one retry").
/// `MemoryStore` is strongly consistent so tests never observe this delay;
/// it exists for real backends with brief propagation lag.
const MIN_TOKEN_RETRY_DELAY: Duration = Duration::from_millis(20);
/// Upper bound on store requests `Catalog::resolve_snapshot_window` issues
/// before any LIST runs, folded into `Catalog::estimated_catalog_requests`
/// (ADR-0044 decision 3): one HEAD GET, always attempted whenever the window
/// is non-empty (`resolve_impl` calls it unconditionally); plus one GET per
/// part a usable HEAD names, capped here at 1 part because every writer
/// today emits exactly one (metric-index-plan.md 3.1, "v1 writes exactly
/// one part; readers accept N parts" as an unused sharding escape hatch);
/// plus one postings GET, worst case, when the query has an equality
/// `__name__` filter (metric-index-plan.md 5.4).
///
/// This is not a structural bound: `SnapshotHead.parts` is `repeated` in the
/// wire format, and `resolve_snapshot_window` issues one GET per named part
/// regardless of how many there are. Part count is only knowable after the
/// HEAD GET this constant is trying to avoid, so a future multi-part writer
/// (the escape hatch metric-index-plan.md 3.2 reserves) would silently make
/// `estimated_catalog_requests` an under-estimate again. Flagged as an open
/// gap in ADR-0044 decision 3, not resolved here.
const SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND: u64 = 3;

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
    /// The RAM byte cache (ADR-0046 decisions 1-2): raw bytes of
    /// content-addressed objects (snapshot parts, postings), keyed by
    /// `(tenant_hash, content_hash, offset, len)`, consulted at
    /// [`Catalog::fetch_content_addressed`] before a store GET. `E` is
    /// `Infallible` because this cache is never used through
    /// `get_or_fetch`: every call site here does its own plain
    /// get-then-insert, mirroring the five decoded caches' idiom, so no
    /// upstream fetch-closure error type is ever constructed.
    byte_cache: Cache<std::convert::Infallible>,
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
    /// Count of hard isolation-breach failures observed: a HEAD or postings
    /// object whose `tenant_hash` does not match the requesting tenant, or a
    /// listing helper result whose key does not begin with the requesting
    /// tenant's prefix (ADR-0050 §2). Unlike the two counters above, each of
    /// these also fails the query: the count is a record of hard failures,
    /// not a harmless-overlap anomaly tally.
    isolation_breaches: AtomicU64,
    /// Bounds the object-store requests one resolve keeps in flight (#278
    /// item 2). Ephemeral, process-local, correctness-free: it changes only
    /// how many round trips overlap, never which segments a resolve returns.
    request_semaphore: Arc<tokio::sync::Semaphore>,
    /// Whether resolve validates the configured `shard_count` against each
    /// (tenant, signal)'s durable provisioning record (ADR-0050 section 5,
    /// EC5). Off for the many in-crate and `ravel-query`/`ravel-sql` callers
    /// that build a `Catalog` directly; the server turns it on
    /// ([`Catalog::with_provisioning_enforcement`]) so a query for a tenant
    /// whose record disagrees fails with a typed error instead of silently
    /// resolving over `0..shard_count` and dropping the missing shards.
    enforce_provisioning: bool,
    /// The (tenant, signal) pairs already validated against their provisioning
    /// record this process, so the read-path check is one store GET per pair,
    /// not one per resolve. Read-only enforcement: a record disagreement fails
    /// the query and is never adopted here (adoption is an ingest/maintain/CLI
    /// action; a query-only node may hold write-restricted credentials).
    provisioning_checked: Mutex<HashSet<(TenantHash, Signal)>>,
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
        let byte_cache_limits = CacheLimits::new(
            config.byte_cache_max_bytes,
            config.byte_cache_max_entries,
            config.byte_cache_max_entry_bytes,
        );
        Ok(Catalog {
            store,
            config,
            cache: RecordCache::default(),
            compaction_cache: CompactionRecordCache::default(),
            head_cache: HeadCache::default(),
            part_cache: PartCache::default(),
            postings_cache: PostingsCache::default(),
            byte_cache: Cache::new(byte_cache_limits),
            interlock_violations: AtomicU64::new(0),
            compaction_input_set_conflicts: AtomicU64::new(0),
            isolation_breaches: AtomicU64::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            enforce_provisioning: false,
            provisioning_checked: Mutex::new(HashSet::new()),
        })
    }

    /// Enable durable `shard_count` enforcement on the resolve path (ADR-0050
    /// section 5, EC5). With it on, the first resolve for each (tenant, signal)
    /// validates this catalog's configured `shard_count` against the
    /// `t/<tenant_hash>/<sig>/prov` record; a disagreement is a hard
    /// [`CatalogError`], never a silent resolve over a subset of shards. Absent
    /// or fresh records pass through (a query never writes a record). Off by
    /// default so only the process that opts in (the server's `build_catalog`)
    /// pays the one GET per (tenant, signal); every existing direct-construction
    /// caller is unchanged.
    pub fn with_provisioning_enforcement(mut self) -> Self {
        self.enforce_provisioning = true;
        self
    }

    /// Validate the configured `shard_count` for one (tenant, signal) against
    /// its provisioning record, once per pair (ADR-0050 section 5). A no-op
    /// when enforcement is off or the pair was already checked. Read-only:
    /// [`crate::AbsentPolicy::CheckOnly`] never writes, so a query-only node
    /// with write-restricted credentials still resolves.
    async fn enforce_provisioning_once(
        &self,
        tenant: &TenantHash,
        signal: Signal,
    ) -> Result<(), CatalogError> {
        if !self.enforce_provisioning {
            return Ok(());
        }
        if self
            .provisioning_checked
            .lock()
            .contains(&(*tenant, signal))
        {
            return Ok(());
        }
        let check = crate::provisioning::validate_or_adopt(
            self.store.as_ref(),
            tenant,
            signal,
            self.config.shard_count,
            0,
            crate::provisioning::AbsentPolicy::CheckOnly,
        )
        .await?;
        // Only a `Matched` result is safe to cache forever: the record is
        // immutable, so a match stays a match. `FreshNoData` means "no record
        // exists yet, nothing to validate against" -- caching it would skip the
        // real check once a later, higher-shard_count process writes the record
        // and lands data across shards this process would then silently omit
        // (records are immutable; a stale cache hit never re-checks). Re-check
        // on the next resolve until the record actually appears; that is one
        // extra GET per resolve until first ingest, which is self-limiting.
        if !matches!(check, crate::provisioning::ProvisioningCheck::FreshNoData) {
            self.provisioning_checked.lock().insert((*tenant, signal));
        }
        Ok(())
    }

    /// One store GET bounded by the resolve-wide in-flight semaphore (#278
    /// item 2). The permit is released the moment the GET resolves and is
    /// never held across another guarded request, so a resolve fanning out
    /// many records cannot wait on permits it already holds.
    ///
    /// The sole funnel for every GET a query issues (ADR-0044 decision 2,
    /// issue #421): `accounting` is credited one [`AccountedOp::Get`] request
    /// unconditionally, and its bytes only on success (`got.data.len()`,
    /// mirroring `InstrumentedStore`'s convention that a failed GET moves no
    /// bytes). Call sites never account for themselves.
    pub(crate) async fn guarded_get(
        &self,
        key: &str,
        range: GetRange,
        accounting: &QueryAccounting,
    ) -> Result<GetOutcome, StoreError> {
        let _permit =
            self.request_semaphore.acquire().await.map_err(|_| {
                StoreError::Transient("catalog request semaphore closed".to_string())
            })?;
        let result = self.store.get(key, range).await;
        accounting.record_s3_request(AccountedOp::Get);
        if let Ok(got) = &result {
            accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
        }
        result
    }

    /// One store GET for an object named by a content-addressed ref
    /// (`SnapshotPartRef`/`SnapshotPostingsRef`), consulted through the byte
    /// cache before falling through to [`Catalog::guarded_get`] (ADR-0046
    /// decisions 1-2). `content_hash` and `size` are the ref's own blake3 and
    /// declared size, both known before the fetch is planned; a hit records
    /// one cache hit and skips the GET (so `guarded_get`'s own accounting,
    /// which credits an S3 request unconditionally, never runs), a miss
    /// records one cache miss and falls through, admitting the bytes on
    /// success.
    ///
    /// When `content_hash` is not exactly 32 bytes the ref is malformed; this
    /// bypasses the cache entirely and calls `guarded_get` directly, exactly
    /// as an uncached GET would, leaving the existing hash-mismatch handling
    /// at the call site (which re-derives the same malformed comparison) to
    /// catch it. This never happens for a well-formed ref.
    ///
    /// The catalog HEAD has no counterpart to this method: `SnapshotHead`
    /// carries no content hash, so `read_head` has no `content_hash` value it
    /// could pass here, and it never calls this method. The byte cache is
    /// reachable only through this one function, which is the structural
    /// reason HEAD cannot be admitted to it.
    pub(crate) async fn fetch_content_addressed(
        &self,
        tenant: &TenantHash,
        key: &str,
        content_hash: &[u8],
        size: u64,
        accounting: &QueryAccounting,
    ) -> Result<Bytes, StoreError> {
        let Ok(content_hash) = <[u8; 32]>::try_from(content_hash) else {
            return Ok(self
                .guarded_get(key, GetRange::Full, accounting)
                .await?
                .data);
        };
        let cache_key = CacheKey::new(tenant.0, content_hash, 0, size);
        if let Some(bytes) = self.byte_cache.get(&cache_key) {
            accounting.record_cache_hit();
            accounting.add_cache_bytes(bytes.len() as u64);
            return Ok(bytes);
        }
        accounting.record_cache_miss();
        let got = self.guarded_get(key, GetRange::Full, accounting).await?;
        self.byte_cache.insert(cache_key, got.data.clone());
        Ok(got.data)
    }

    /// Prefix listing bounded by the same in-flight semaphore, draining every
    /// page (the [`ravel_object_store::list_all`] contract) with a permit
    /// acquired per page rather than one held across the whole listing.
    ///
    /// The sole funnel for every LIST a query issues (ADR-0044 decision 2):
    /// `accounting` is credited one [`AccountedOp::List`] request per page,
    /// unconditionally, mirroring `InstrumentedStore`'s convention that a
    /// LIST never moves bytes.
    ///
    /// Also the sole funnel for the ADR-0050 §2 LIST-prefix assertion: every
    /// returned key must begin with `tenant`'s prefix, or this is a hard
    /// isolation-breach `CatalogError::FieldMismatch`, never a silently
    /// dropped or served foreign key.
    async fn guarded_list_all(
        &self,
        tenant: &TenantHash,
        prefix: &str,
        accounting: &QueryAccounting,
    ) -> Result<Vec<ObjectMeta>, CatalogError> {
        let tenant_prefix = format!("t/{}/", tenant.to_hex());
        let mut out: Vec<ObjectMeta> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut page_token = None;
        loop {
            let page = {
                let _permit = self.request_semaphore.acquire().await.map_err(|_| {
                    StoreError::Transient("catalog request semaphore closed".to_string())
                })?;
                let page = self.store.list(prefix, page_token).await;
                accounting.record_s3_request(AccountedOp::List);
                page?
            };
            for meta in page.objects {
                if !meta.key.starts_with(&tenant_prefix) {
                    self.record_isolation_breach();
                    return Err(CatalogError::FieldMismatch {
                        key: prefix.to_string(),
                        field: "list_prefix",
                        expected: tenant_prefix,
                        actual: meta.key,
                    });
                }
                if seen.insert(meta.key.clone()) {
                    out.push(meta);
                }
            }
            match page.next {
                Some(next) => page_token = Some(next),
                None => break,
            }
        }
        Ok(out)
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

    /// Count of hard isolation-breach failures observed across this
    /// catalog's lifetime (ADR-0050 §2): a HEAD/postings tenant_hash
    /// mismatch or an out-of-prefix listing result. See
    /// docs/catalog-and-mvcc.md.
    pub fn isolation_breaches(&self) -> u64 {
        self.isolation_breaches.load(Ordering::Relaxed)
    }

    /// `pub(crate)`: lets `snapshot_resolve` (ADR-0050 §2) bump the
    /// isolation-breach counter from its own `impl Catalog` block before
    /// returning the hard `CatalogError::FieldMismatch`.
    pub(crate) fn record_isolation_breach(&self) {
        self.isolation_breaches.fetch_add(1, Ordering::Relaxed);
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

    /// `pub(crate)`: exposed for `cache`'s own tests to inspect and seed the
    /// byte cache directly (ADR-0046). Not used by `snapshot_resolve`, which
    /// only ever reaches the byte cache through
    /// [`Catalog::fetch_content_addressed`] -- hence `cfg(test)`, since no
    /// production code path needs this accessor.
    #[cfg(test)]
    pub(crate) fn byte_cache(&self) -> &Cache<std::convert::Infallible> {
        &self.byte_cache
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
        self.resolve_with_accounting(
            tenant,
            signal,
            range,
            min_tokens,
            now_ns,
            &QueryAccounting::new(),
        )
        .await
    }

    /// Like [`Catalog::resolve`], but records every S3 request and cache
    /// access this resolve makes into `accounting` (ADR-0044, issue #421).
    /// A separate method rather than an added `resolve` parameter: `resolve`
    /// is called from `ravel-query`, `ravel-sql`, and `services/ravel-server`
    /// (out of this task's scope), so its signature and every existing call
    /// site stay exactly as they were; only callers that want accounting
    /// need to switch to this entry point.
    pub async fn resolve_with_accounting(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        accounting: &QueryAccounting,
    ) -> Result<Snapshot, CatalogError> {
        self.resolve_impl(tenant, signal, range, min_tokens, now_ns, None, accounting)
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
        self.resolve_pruned_with_accounting(
            tenant,
            signal,
            range,
            min_tokens,
            now_ns,
            name_filter,
            &QueryAccounting::new(),
        )
        .await
    }

    /// Like [`Catalog::resolve_pruned`], but records every S3 request and
    /// cache access this resolve makes into `accounting` (ADR-0044, issue
    /// #421). See [`Catalog::resolve_with_accounting`] for why this is a
    /// separate method rather than a `resolve_pruned` parameter.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_pruned_with_accounting(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
        accounting: &QueryAccounting,
    ) -> Result<Snapshot, CatalogError> {
        self.resolve_impl(
            tenant,
            signal,
            range,
            min_tokens,
            now_ns,
            name_filter,
            accounting,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_impl(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        range: TimeRange,
        min_tokens: &[CommitToken],
        now_ns: i64,
        name_filter: Option<&str>,
        accounting: &QueryAccounting,
    ) -> Result<Snapshot, CatalogError> {
        // Durable shard_count enforcement on the read path (ADR-0050 section 5,
        // EC5): fail before the `0..shard_count` listing loop below if this
        // catalog's configured shard_count disagrees with the (tenant, signal)'s
        // provisioning record, so a lower value never silently drops the shards
        // it omits. A no-op unless enforcement was opted into.
        self.enforce_provisioning_once(tenant, signal).await?;

        let mut segments: HashMap<String, SegmentRef> = HashMap::new();
        let mut segments_pruned = 0u64;

        if let Some((window_start_hour, window_end_hour)) = self.window_hour_bounds(range, now_ns) {
            // A snapshot at watermark W serves every window hour <= W
            // straight from its parts; only the suffix above W is listed
            // (docs/metric-index-plan.md 5.1 step 3). No usable snapshot,
            // or a watermark below the window's start, falls back to
            // listing the whole window, unchanged from Phase 1.
            //
            // `name_filter.is_some()` gates the postings GET (#278 item 3):
            // the postings object is only ever consulted for an equality
            // `__name__` filter, so a query without one never fetches or
            // decodes it.
            let window = self
                .resolve_snapshot_window(tenant, signal, now_ns, name_filter.is_some(), accounting)
                .await?;
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
            // The (shard, hour) listing pass used to issue its LISTs one
            // await at a time; run them concurrently under the resolve-wide
            // semaphore instead (#278 item 2). Each bucket resolves into its
            // own map (bucket keys never collide across buckets), and the
            // per-bucket maps are merged back in the buffered (input) order so
            // the resulting segment set is byte-identical to the sequential
            // pass before the final deterministic sort.
            let mut bucket_coords = Vec::new();
            for shard in 0..self.config.shard_count {
                for hour in listing_start_hour..=window_end_hour {
                    bucket_coords.push((shard, hour));
                }
            }
            let bucket_maps: Vec<Result<HashMap<String, SegmentRef>, CatalogError>> =
                stream::iter(bucket_coords)
                    .map(|(shard, hour)| async move {
                        self.list_hour_bucket(tenant, signal, shard, hour, range, accounting)
                            .await
                    })
                    .buffered(MAX_CONCURRENT_REQUESTS)
                    .collect()
                    .await;
            for bucket in bucket_maps {
                for (key, segment_ref) in bucket? {
                    segments.entry(key).or_insert(segment_ref);
                }
            }
        }

        for token in min_tokens {
            self.resolve_min_token(tenant, signal, token, &mut segments, accounting)
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

    /// Upper bound on the store requests a `resolve`/`resolve_pruned` call
    /// for this `(range, now_ns)` window will issue before it has listed
    /// anything, computed without running any part of resolve itself
    /// (ADR-0044 decision 3, "catalog term": computed before
    /// `Catalog::resolve` runs, from `shard_count` and the number of hour
    /// buckets the padded window spans -- both inputs the planner already
    /// holds).
    ///
    /// Two parts:
    ///
    /// - LIST requests: one per `(shard, hour)` pair, i.e. what
    ///   `resolve_impl` issues with no snapshot watermark to shorten the
    ///   listed suffix (a watermark can only narrow the listed range, never
    ///   widen it, so this bound holds whether or not folding has run).
    /// - `SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`: the snapshot-window path
    ///   (`Catalog::resolve_snapshot_window`, docs/metric-index-plan.md
    ///   5.1/5.3) that `resolve_impl` tries first, whenever the window is
    ///   non-empty, before any LIST runs. This is not derivable from
    ///   `shard_count`/hour buckets and is folded in as a documented
    ///   constant instead -- see that constant's doc comment for the open
    ///   gap this leaves (ADR-0044 decision 3 does not account for it).
    ///
    /// The record GETs `resolve` also issues for whatever those LISTs turn up
    /// are not included here: which records exist, and how many, is only
    /// knowable after a LIST actually runs, so they cannot be bounded before
    /// resolve starts.
    pub fn estimated_catalog_requests(&self, range: TimeRange, now_ns: i64) -> u64 {
        match self.window_hour_bounds(range, now_ns) {
            Some((start_hour, end_hour)) => {
                let hours = u64::from(end_hour - start_hour) + 1;
                let list_requests = u64::from(self.config.shard_count).saturating_mul(hours);
                list_requests.saturating_add(SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND)
            }
            None => 0,
        }
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

    #[allow(clippy::too_many_arguments)]
    async fn list_hour_bucket(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        hour: u32,
        range: TimeRange,
        accounting: &QueryAccounting,
    ) -> Result<HashMap<String, SegmentRef>, CatalogError> {
        let mut out: HashMap<String, SegmentRef> = HashMap::new();
        let prefix = keys::commit_shard_hour_prefix(tenant, signal, shard, hour)?;
        let objects = self.guarded_list_all(tenant, &prefix, accounting).await?;

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
            return Ok(out);
        }

        // Warm the record caches for this bucket concurrently before the
        // sequential include logic runs (#278 item 2): the includes below
        // then hit the cache instead of each paying a serial GET. A GET
        // failure surfaces here, exactly as it would have from the first
        // sequential load.
        self.prewarm_commit_records(tenant, signal, shard, &l0_keys, accounting)
            .await?;
        self.prewarm_compaction_records(tenant, signal, shard, &compaction_keys, accounting)
            .await?;

        // No compaction record: Phase 1 behavior, every overlapping L0.
        if compaction_keys.is_empty() {
            for key in &l0_keys {
                self.include_l0_if_overlaps(
                    tenant, signal, shard, key, &range, &mut out, accounting,
                )
                .await?;
            }
            return Ok(out);
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
                .load_and_validate_compaction(tenant, signal, shard, ckey, accounting)
                .await?;
            input_set_hashes.insert(record.input_set_hash.clone());
            newest_record_created_ns = newest_record_created_ns.max(record.created_unix_ns);
            for part in &record.parts {
                let segment_ref = build_l1_segment_ref(&record, part, ckey)?;
                let event_range = TimeRange {
                    start_ns: segment_ref.min_event_ts_ns,
                    end_ns: segment_ref.max_event_ts_ns,
                };
                if event_range.overlaps(&range) {
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
            let record = self
                .load_and_validate(tenant, signal, shard, key, accounting)
                .await?;
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
            if event_range.overlaps(&range) {
                let segment_ref = build_segment_ref(key, &record)?;
                out.entry(segment_ref.data_object_key.clone())
                    .or_insert(segment_ref);
            }
        }
        Ok(out)
    }

    /// Concurrently load and cache every commit record in `keys` under the
    /// resolve-wide semaphore (#278 item 2), so a later cache-first read of
    /// each is a hit. Returns the first load error; the sequential include
    /// logic never re-issues a GET a successful prewarm already cached.
    async fn prewarm_commit_records(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        keys: &[String],
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        // Owned key clones, not borrowed slice items: a stream closure that
        // borrows each `&String` makes rustc infer a non-higher-ranked
        // lifetime for the future, which then fails to unify with axum's
        // `Handler` blanket impl at the HTTP router (the same "FnOnce is not
        // general enough" wall the prefetch closure in `ravel-query` hit).
        let loads: Vec<Result<(), CatalogError>> = stream::iter(keys.iter().cloned())
            .map(|key| async move {
                self.load_and_validate(tenant, signal, shard, &key, accounting)
                    .await
                    .map(|_| ())
            })
            .buffer_unordered(MAX_CONCURRENT_REQUESTS)
            .collect()
            .await;
        for load in loads {
            load?;
        }
        Ok(())
    }

    /// Compaction-record counterpart to
    /// [`prewarm_commit_records`](Self::prewarm_commit_records) (#278 item 2).
    async fn prewarm_compaction_records(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        keys: &[String],
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        let loads: Vec<Result<(), CatalogError>> = stream::iter(keys.iter().cloned())
            .map(|key| async move {
                self.load_and_validate_compaction(tenant, signal, shard, &key, accounting)
                    .await
                    .map(|_| ())
            })
            .buffer_unordered(MAX_CONCURRENT_REQUESTS)
            .collect()
            .await;
        for load in loads {
            load?;
        }
        Ok(())
    }

    /// Load one L0 commit record and, if its event range overlaps `range`,
    /// add its segment ref to `out`. The plain Phase 1 include path, shared
    /// by the no-compaction fast path.
    #[allow(clippy::too_many_arguments)]
    async fn include_l0_if_overlaps(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        key: &str,
        range: &TimeRange,
        out: &mut HashMap<String, SegmentRef>,
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        let record = self
            .load_and_validate(tenant, signal, shard, key, accounting)
            .await?;
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
    pub(crate) async fn load_and_validate_compaction(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        key: &str,
        accounting: &QueryAccounting,
    ) -> Result<Arc<CompactionRecord>, CatalogError> {
        if let Some(cached) = self.compaction_cache.get(tenant, key, accounting) {
            validate_compaction_expected_fields(self, &cached, tenant, signal, shard, key)?;
            return Ok(cached);
        }
        let got = self.guarded_get(key, GetRange::Full, accounting).await?;
        let bytes = got.data.len() as u64;
        let record = CompactionRecord::decode(got.data.as_ref()).map_err(|e| {
            CatalogError::CompactionRecordDecode {
                key: key.to_string(),
                source: e,
            }
        })?;
        validate_compaction_expected_fields(self, &record, tenant, signal, shard, key)?;
        let record = Arc::new(record);
        self.compaction_cache.insert(
            *tenant,
            key.to_string(),
            record.clone(),
            bytes,
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
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        let key = keys::commit_key_for_token(tenant, signal, token)?;
        if let Some(cached) = self.cache.get(tenant, &key, accounting)
            && validate_expected_fields(self, &cached, tenant, signal, token.shard, &key).is_ok()
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
            match self.guarded_get(&key, GetRange::Full, accounting).await {
                Ok(got) => {
                    let bytes = got.data.len() as u64;
                    let record = record::decode(&got.data)?;
                    validate_expected_fields(self, &record, tenant, signal, token.shard, &key)?;
                    let record = Arc::new(record);
                    self.cache.insert(
                        *tenant,
                        key.clone(),
                        record.clone(),
                        bytes,
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
                        .resolve_min_token_fallback(tenant, signal, token, out, accounting)
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
        accounting: &QueryAccounting,
    ) -> Result<(), CatalogError> {
        let prefix =
            keys::commit_shard_hour_prefix(tenant, signal, token.shard, token.ingest_hour_bucket)?;
        let objects = self.guarded_list_all(tenant, &prefix, accounting).await?;
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
                .load_and_validate_compaction(tenant, signal, token.shard, ckey, accounting)
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
        accounting: &QueryAccounting,
    ) -> Result<Arc<CommitRecord>, CatalogError> {
        if let Some(cached) = self.cache.get(tenant, key, accounting) {
            validate_expected_fields(self, &cached, tenant, signal, shard, key)?;
            return Ok(cached);
        }
        let got = self.guarded_get(key, GetRange::Full, accounting).await?;
        let bytes = got.data.len() as u64;
        let record = record::decode(&got.data)?;
        validate_expected_fields(self, &record, tenant, signal, shard, key)?;
        let record = Arc::new(record);
        self.cache.insert(
            *tenant,
            key.to_string(),
            record.clone(),
            bytes,
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
///
/// A `tenant_hash` disagreement is an isolation breach (ADR-0050 §2): a
/// commit record listed or addressed under one tenant's prefix that declares
/// another tenant. It is recorded on `ravel_catalog_isolation_breach_total`
/// before the hard `FieldMismatch` is returned, so an operator sees it on the
/// same counter the HEAD and postings breaches increment (#529). The
/// `catalog` handle is threaded in purely to reach that counter; the
/// rejection itself is unchanged.
fn validate_expected_fields(
    catalog: &Catalog,
    record: &CommitRecord,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    key: &str,
) -> Result<(), CatalogError> {
    if record.tenant_hash.as_slice() != tenant.0.as_slice() {
        catalog.record_isolation_breach();
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
/// compaction-record analog of [`validate_expected_fields`], and like it a
/// `tenant_hash` disagreement is recorded on
/// `ravel_catalog_isolation_breach_total` before the hard `FieldMismatch`
/// (ADR-0050 §2, #529). The `catalog` handle is threaded in purely to reach
/// that counter; the rejection itself is unchanged.
fn validate_compaction_expected_fields(
    catalog: &Catalog,
    record: &CompactionRecord,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    key: &str,
) -> Result<(), CatalogError> {
    if record.tenant_hash.as_slice() != tenant.0.as_slice() {
        catalog.record_isolation_breach();
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
    use ravel_object_store::InstrumentedStore;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault, Sequence,
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

    async fn seed_provisioning_record(store: &MemoryStore, shard_count: u32) {
        use ravel_proto::sys::v1 as sysproto;
        let record = sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count,
            created_unix_ns: 1,
            generations: Vec::new(),
        };
        store
            .put(
                &crate::provisioning::provisioning_key(&tenant(), Signal::Metrics),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed provisioning record");
    }

    /// With provisioning enforcement on (as `build_catalog` sets it), a resolve
    /// for a (tenant, signal) whose record disagrees with the configured
    /// shard_count fails with a typed error before the `0..shard_count` listing
    /// loop, so a lower shard_count never serves a truncated shard range
    /// (ADR-0050 section 5, S1-E6 query-path guard).
    #[tokio::test]
    async fn resolve_enforces_provisioning_record_mismatch() {
        let store = Arc::new(MemoryStore::new());
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        publish_segment(&store, 0, 1, 500_000, now, now - 1_000, now).await;
        // The tenant's data was written under shard_count=4.
        seed_provisioning_record(&store, 4).await;

        // A catalog configured for 2 shards, with enforcement on.
        let catalog = Catalog::new(store.clone(), config(2))
            .expect("catalog")
            .with_provisioning_enforcement();
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("a lower configured shard_count must fail the resolve");
        assert!(
            matches!(err, CatalogError::Provisioning(_)),
            "expected a provisioning failure, got: {err}"
        );
    }

    /// Enforcement is opt-in: a catalog built without
    /// `with_provisioning_enforcement` (as every existing direct-construction
    /// caller does) resolves normally even when a disagreeing record exists, so
    /// this change does not alter behavior for those callers.
    #[tokio::test]
    async fn resolve_without_enforcement_ignores_provisioning_record() {
        let store = Arc::new(MemoryStore::new());
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        publish_segment(&store, 0, 1, 500_000, now, now - 1_000, now).await;
        seed_provisioning_record(&store, 4).await;

        let catalog = Catalog::new(store.clone(), config(2)).expect("catalog");
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("enforcement off: resolve ignores the provisioning record");
    }

    /// Finding 2 regression: a `FreshNoData` result (no record yet) must not be
    /// cached as validated. A query-only catalog resolves an empty-record tenant
    /// (passes as fresh), then a real record appears written under a higher
    /// shard_count; the next resolve must re-check and surface the mismatch, not
    /// serve a truncated shard range from a stale "already validated" cache
    /// entry (records are immutable, so a wrongly-cached miss never re-checks).
    #[tokio::test]
    async fn resolve_rechecks_after_fresh_no_data_until_record_appears() {
        let store = Arc::new(MemoryStore::new());
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        publish_segment(&store, 0, 1, 500_000, now, now - 1_000, now).await;

        // Catalog configured for 2 shards, enforcement on. No provisioning
        // record exists yet: the first resolve sees `FreshNoData` and must pass.
        let catalog = Catalog::new(store.clone(), config(2))
            .expect("catalog")
            .with_provisioning_enforcement();
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("a fresh (no-record) tenant resolves cleanly");
        assert_eq!(snapshot.segments.len(), 1, "first resolve returns the data");

        // A separate higher-shard_count process now writes the real record and
        // (conceptually) lands data across shards 0..4. If the earlier
        // `FreshNoData` had been cached as validated, this resolve would skip the
        // check and silently serve only shards 0..2.
        seed_provisioning_record(&store, 4).await;

        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("once the real record appears the resolve must re-check and refuse");
        assert!(
            matches!(err, CatalogError::Provisioning(_)),
            "expected a provisioning failure from the re-check, got: {err}"
        );
    }

    #[tokio::test]
    async fn resolve_records_list_and_get_requests_into_accounting() {
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        let writer_id = Uuid::new_v4();
        let payload = format!("segment-0-1-{writer_id}").into_bytes();
        let content_hash = content_hash_for(&payload);
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 1_000,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 1_000,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");

        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };

        // A separate, uninstrumented catalog over identical data gives the
        // no-accounting baseline: accounting must be pure observation, never
        // a behavior change, so its resolved snapshot must match exactly.
        let plain_store = Arc::new(MemoryStore::new());
        publish::put_data_object(
            plain_store.as_ref(),
            &data_key,
            Bytes::from(payload.clone()),
        )
        .await
        .expect("put data object");
        publish::publish(plain_store.as_ref(), &record, &RetryPolicy::default())
            .await
            .expect("publish");
        let plain_catalog = Catalog::new(plain_store, config(1)).expect("catalog");
        let baseline = plain_catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("baseline resolve");

        // A fresh catalog and a fresh instrumented backend, seeded with the
        // exact same commit record and data object, isolates the resolve
        // under test from the baseline catalog's own caches.
        let instrumented_inner = MemoryStore::new();
        publish::put_data_object(&instrumented_inner, &data_key, Bytes::from(payload))
            .await
            .expect("put data object");
        publish::publish(&instrumented_inner, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        let store = Arc::new(InstrumentedStore::new(instrumented_inner));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let before = store.metrics().snapshot();
        let accounting = QueryAccounting::new();
        let snapshot = catalog
            .resolve_with_accounting(&tenant(), Signal::Metrics, range, &[], now, &accounting)
            .await
            .expect("accounted resolve");
        let after = store.metrics().snapshot();

        assert_eq!(snapshot.segments, baseline.segments);
        assert_eq!(snapshot.segments_pruned, baseline.segments_pruned);

        let acc = accounting.snapshot();
        let get_calls_diff = after.get.calls - before.get.calls;
        let list_calls_diff = after.list.calls - before.list.calls;
        let get_bytes_diff = after.get.bytes - before.get.bytes;
        let head_calls_diff = after.head.calls - before.head.calls;
        let list_delimited_calls_diff = after.list_delimited.calls - before.list_delimited.calls;
        let delete_calls_diff = after.delete.calls - before.delete.calls;

        // Catalog's two funnels only ever issue GET and LIST: no HEAD,
        // list_delimited, or DELETE call is on the resolve path.
        assert_eq!(head_calls_diff, 0);
        assert_eq!(list_delimited_calls_diff, 0);
        assert_eq!(delete_calls_diff, 0);

        assert_eq!(acc.s3_requests(AccountedOp::Get), get_calls_diff);
        assert_eq!(acc.s3_requests(AccountedOp::List), list_calls_diff);
        assert_eq!(acc.s3_requests(AccountedOp::Head), 0);
        assert_eq!(acc.s3_bytes(AccountedOp::Get), get_bytes_diff);
        assert_eq!(acc.s3_bytes(AccountedOp::List), 0);
        assert_eq!(
            acc.total_s3_requests(),
            get_calls_diff + list_calls_diff + head_calls_diff
        );
        assert_eq!(acc.total_s3_bytes(), get_bytes_diff);

        // At least one LIST (the hour bucket) and one GET (the commit
        // record, or the head probe) actually happened -- otherwise the
        // assertions above would be vacuously true.
        assert!(list_calls_diff >= 1);
        assert!(get_calls_diff >= 1);
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

    /// #278 item 2: the (shard, hour) listing pass and the per-bucket record
    /// GETs now run concurrently under a semaphore. Concurrency must not drop,
    /// duplicate, or reorder segments: the resolved set must be complete and
    /// its total order deterministic across repeated resolves.
    #[tokio::test]
    async fn parallel_listing_returns_complete_deterministic_snapshot() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(3)).expect("catalog");
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;

        // Many segments across every shard and two in-window hours, each with
        // a distinct created stamp so the ADR-0010 §5 total order is fully
        // determined (no ties left to HashMap iteration order).
        let mut published = Vec::new();
        let mut seq = 1u64;
        for shard in 0..3u32 {
            for hour in [499_999u32, 500_000u32] {
                for _ in 0..4 {
                    let created = i64::from(hour) * NS_PER_HOUR + (seq as i64) * 1_000;
                    let record =
                        publish_segment(&store, shard, seq, hour, created, created, created + 500)
                            .await;
                    published.push(keys::reconstruct_data_key(&record).expect("key"));
                    seq += 1;
                }
            }
        }

        let range = TimeRange {
            start_ns: 499_999 * NS_PER_HOUR,
            end_ns: now,
        };
        let first = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(
            first.segments.len(),
            published.len(),
            "every published segment must be listed exactly once"
        );
        let found: HashSet<String> = first
            .segments
            .iter()
            .map(|s| s.data_object_key.clone())
            .collect();
        for key in &published {
            assert!(found.contains(key), "missing segment {key}");
        }

        // A second resolve over the same concurrent pass must return the
        // identical order, not a completion-order permutation.
        let second = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        let order1: Vec<&str> = first
            .segments
            .iter()
            .map(|s| s.data_object_key.as_str())
            .collect();
        let order2: Vec<&str> = second
            .segments
            .iter()
            .map(|s| s.data_object_key.as_str())
            .collect();
        assert_eq!(
            order1, order2,
            "concurrent listing must yield a deterministic total order"
        );
    }

    /// #278 item 2: a fault on a commit-record GET must still surface as an
    /// error through the concurrent prewarm, never be swallowed into a
    /// silently short snapshot. A permanent GET fault fires on the record read
    /// (the absent HEAD GET degrades to listing as always), and the resolve
    /// fails loudly.
    #[tokio::test]
    async fn a_commit_record_get_fault_surfaces_through_the_concurrent_prewarm() {
        let inner = MemoryStore::new();
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        for seq in 1..=5u64 {
            publish_segment(&inner, 0, seq, 500_000, now, now - 1_000, now).await;
        }
        let plan = FaultPlan::empty().with_rule(Rule::new(
            Op::Get,
            ScriptedFault::Permanent("record get down".into()),
        ));
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let range = TimeRange {
            start_ns: now - NS_PER_HOUR,
            end_ns: now,
        };
        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("a record GET fault must surface, not be swallowed");
        assert!(matches!(err, CatalogError::Store(_)), "got {err:?}");
        assert!(
            store.fault_count(Op::Get, FaultKind::Permanent) >= 1,
            "the record GET fault must have actually fired"
        );
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
        let commit_key =
            keys::commit_key_for_token(&tenant(), Signal::Metrics, &token).expect("commit key");
        store
            .delete(&commit_key)
            .await
            .expect("delete commit record");

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
        let commit_key =
            keys::commit_key_for_token(&tenant(), Signal::Metrics, &token).expect("commit key");
        store
            .delete(&commit_key)
            .await
            .expect("delete commit record");

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

    /// Wraps a store, injecting one bogus foreign-tenant key into the final
    /// page of the one `list()` call whose prefix matches `target_prefix`:
    /// simulates a backend or key-layout bug a correctly behaving
    /// `MemoryStore` can never itself produce, to exercise the ADR-0050 §2
    /// LIST-prefix assertion in [`Catalog::guarded_list_all`]. Scoped to a
    /// single target prefix, not every listing call, so the test's
    /// `resolve` (which lists several (shard, hour) buckets to cover its
    /// ingest-lag window) sees exactly one violation.
    struct ForeignKeyInjectingStore<S> {
        inner: S,
        target_prefix: String,
        foreign_key: String,
    }

    #[async_trait::async_trait]
    impl<S: ObjectStoreBackend> ObjectStoreBackend for ForeignKeyInjectingStore<S> {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: ravel_object_store::PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            let mut page = self.inner.list(prefix, page).await?;
            if page.next.is_none() && prefix == self.target_prefix {
                page.objects.push(ObjectMeta {
                    key: self.foreign_key.clone(),
                    size: 0,
                    etag: ravel_object_store::Etag(String::new()),
                    version: ravel_object_store::Version(String::new()),
                    last_modified_unix_ms: 0,
                });
            }
            Ok(page)
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    #[tokio::test]
    async fn list_result_outside_tenant_prefix_is_hard_error() {
        let now = 500_000 * NS_PER_HOUR + 30 * 60_000_000_000;
        // The one (shard, hour) bucket `resolve`'s ingest-lag window is
        // guaranteed to cover: matching on this exact prefix means the
        // other buckets the window also lists stay untouched, so the test
        // observes exactly one violation.
        let target_prefix =
            keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, 500_000).expect("prefix");
        let store = Arc::new(ForeignKeyInjectingStore {
            inner: MemoryStore::new(),
            target_prefix,
            foreign_key: "t/deadbeefdeadbeefdeadbeefdeadbeef/m/c/s0000/h00500000/foreign-key"
                .to_string(),
        });
        let catalog = Catalog::new(store, config(1)).expect("catalog");
        let range = TimeRange {
            start_ns: now - 1_000,
            end_ns: now,
        };

        let err = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect_err("an out-of-prefix listing result must hard-fail");
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "list_prefix"),
            other => panic!("expected FieldMismatch, got {other:?}"),
        }
        assert_eq!(catalog.isolation_breaches(), 1);
    }

    /// #529 / ADR-0050 §2: a commit record and a compaction record whose
    /// tenant_hash disagrees with the prefix they were listed under are the
    /// highest-signal breach the metric must reflect. Both validators must
    /// count the breach on `ravel_catalog_isolation_breach_total`, not merely
    /// reject it. tenant_hash is checked before every other field (and before
    /// key reconstruction for the compaction record), so a foreign hash
    /// reaches exactly the breach branch.
    #[tokio::test]
    async fn record_tenant_hash_mismatch_records_isolation_breach() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store, config(1)).expect("catalog");
        let listed_under = tenant();
        let foreign = TenantHash([0xff; 16]);
        let now = 500_000 * NS_PER_HOUR;

        // A self-consistent commit record for the FOREIGN tenant, validated as
        // if it were listed under this tenant's prefix.
        let writer_id = Uuid::new_v4();
        let payload = b"payload".to_vec();
        let content_hash = content_hash_for(&payload);
        let commit = record::build(NewCommitRecord {
            tenant_hash: foreign,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now - 1_000,
            max_event_ts_ns: now,
            min_ingest_ts_ns: now - 1_000,
            max_ingest_ts_ns: now,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: 500_000,
        })
        .expect("valid record");

        let err = validate_expected_fields(
            &catalog,
            &commit,
            &listed_under,
            Signal::Metrics,
            0,
            "listed-under-key",
        )
        .expect_err("a commit record naming a foreign tenant must be rejected");
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "tenant_hash"),
            other => panic!("expected tenant_hash FieldMismatch, got {other:?}"),
        }
        assert_eq!(catalog.isolation_breaches(), 1);

        // A compaction record declaring the foreign tenant. tenant_hash is
        // checked before key reconstruction, so a default-filled record with a
        // foreign hash reaches the breach branch without a reconstructable key.
        let compaction = CompactionRecord {
            tenant_hash: foreign.0.to_vec(),
            signal: signal::to_proto(Signal::Metrics).into(),
            shard: 0,
            ..Default::default()
        };
        let err = validate_compaction_expected_fields(
            &catalog,
            &compaction,
            &listed_under,
            Signal::Metrics,
            0,
            "listed-under-ckey",
        )
        .expect_err("a compaction record naming a foreign tenant must be rejected");
        match err {
            CatalogError::FieldMismatch { field, .. } => assert_eq!(field, "tenant_hash"),
            other => panic!("expected tenant_hash FieldMismatch, got {other:?}"),
        }
        assert_eq!(catalog.isolation_breaches(), 2);
    }
}
