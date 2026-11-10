//! The HEAD-reachability delete blocker (ADR-0020), shared by every physical
//! delete that can race a fold.
//!
//! A snapshot the live `(tenant, signal)` HEAD names is what a resolver reads;
//! the protection horizon bounds a *pinned in-flight reader*, but it does not
//! on its own prove the *current* HEAD has stopped naming an object. Both the
//! retention sweep (whole tombstoned buckets) and the superseded-input sweep
//! (a compaction or rewrite record's inputs) therefore ask the same question
//! before deleting: does the live HEAD snapshot still reach this object?
//!
//! The three answers are fixed and identical for both callers:
//!
//! - **HEAD absent**: no snapshot names anything, so nothing is blocked
//!   (ADR-0020: the catalog index is a pure optimization; a missing HEAD
//!   degrades to listing).
//! - **HEAD, or a covering snapshot part, present but unreadable**: blocked
//!   fail-closed. Non-reachability cannot be proven from data that cannot be
//!   read, and a wrongly-permitted delete is unrecoverable while a delayed one
//!   is not. A HEAD or a part whose header names another tenant (ADR-0050
//!   §2), or a HEAD whose header names a signal other than the one swept, is
//!   one more cause: such a HEAD is refused before any of its parts is read,
//!   a foreign part before its entries are read, the same fail-closed answer
//!   as one that will not decode at all.
//! - **A decoded snapshot entry names the object**: blocked. The ordinary
//!   lagging-fold case.
//!
//! [`SnapshotReachability`] is the per-pass cache that keeps this affordable:
//! HEAD is read at most once per pass and each covering part at most once, so
//! a pass that gates many buckets or many superseded inputs of one
//! `(tenant, signal)` never pays a HEAD GET per candidate (ADR-0076 request
//! cost).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ravel_catalog::{
    DecodedPart, PartLimits, PartTenantError, decode_head, decode_part_for_tenant,
};
use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectMeta, ObjectStoreBackend, StoreError, list_all};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef};
use ravel_types::{Signal, TenantHash};

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::read_gate::MaintainReadGate;
use crate::unnamed_marker::{
    MarkerAnchor, MarkerReapOutcome, PinnedQueryWindow, UnnamedMarker, put_marker, reap_listed,
};
use ravel_cpu_gate::ReadSite;

/// [`SnapshotGate::Clear`] once `observed_unix_ns` is past the pinned-query
/// window on the deleting sweeper's clock, read now.
fn window_verdict(ctx: &MarkerContext<'_>, observed_unix_ns: i64) -> SnapshotGate {
    if ctx.window.has_elapsed(observed_unix_ns, ctx.clock.now_ns()) {
        SnapshotGate::Clear
    } else {
        SnapshotGate::Blocked(SnapshotBlock::PinnedWindow)
    }
}

/// The answer a pass that may not write markers gives a candidate whose
/// marker is missing or mismatched, or `None` for a deleting pass, which
/// writes one. A dry run reports what the deleting pass would decide after
/// writing a marker at this instant; the observing pass holds the candidate.
fn read_only_verdict(ctx: &MarkerContext<'_>) -> Option<SnapshotGate> {
    match ctx.policy {
        MarkerPolicy::Write => None,
        MarkerPolicy::DryRun => Some(window_verdict(ctx, ctx.clock.now_ns())),
        MarkerPolicy::Observe => Some(SnapshotGate::Blocked(SnapshotBlock::PinnedWindow)),
    }
}

/// Why a physical delete was blocked by HEAD reachability (ADR-0020
/// delete-blocker). Both variants delete nothing; they are distinguished so
/// each is separately observable in the maintain counters (a persistent
/// [`SnapshotBlock::Unreadable`] is an operator signal that HEAD or a part
/// cannot be read, not the ordinary lagging-fold case).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotBlock {
    /// A decoded snapshot entry names an object the delete would remove: the
    /// bucket it sits in (retention) or the object itself (superseded inputs).
    /// The ordinary case: the fold has not yet reconciled the hour.
    Named,
    /// HEAD, or a snapshot part covering the relevant hour, was present but
    /// could not be read (undecodable, checksum/hash mismatch, unsupported
    /// version, a header naming another tenant (ADR-0050 §2) or a HEAD
    /// header naming another signal, an entry whose
    /// identity fields do not fit the shape a fold writes, or a HEAD-named
    /// part that is missing). Blocked fail-closed:
    /// non-reachability cannot be proven from data that cannot be read, and a
    /// wrongly-permitted delete is unrecoverable while a delayed one is not.
    /// Also the answer for any doubt about the candidate's unnamed-since
    /// marker (ADR-1133 decision 6): a marker GET, PUT or DELETE error, or a
    /// body that does not decode. An anchor that cannot be read fails the
    /// pass instead, which deletes nothing either.
    Unreadable,
    /// No snapshot entry names the candidate, but its unnamed-since marker is
    /// missing, was written for another anchor, or is younger than the
    /// pinned-query window (ADR-1133 decision 3): a query that resolved a HEAD
    /// from before the drop may still be reading it. Clears on its own once
    /// the marker ages.
    PinnedWindow,
}

/// The result of gating one delete candidate on HEAD reachability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotGate {
    /// No snapshot entry names the candidate: the delete may proceed.
    Clear,
    /// The delete is blocked; the reason distinguishes the counters.
    Blocked(SnapshotBlock),
}

/// The identity of one physical object exactly as a snapshot entry carries it,
/// so "does the live HEAD still name this object" is decided without
/// reconstructing key strings on either side.
///
/// The frozen `SnapshotEntry` has no dedicated level-1 identity field, so the
/// fold overloads the writer slots for a compaction or rewrite output part:
/// `writer_id` carries the parent record's 32-byte `input_set_hash` and
/// `writer_epoch` carries the `part_index` (crates/ravel-catalog's
/// `build_l1_snapshot_entry`). [`snapshot_object`] is the one place that
/// convention is read here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SnapshotObject {
    /// A raw L0 flush: the `(writer_id, epoch, seq)` identity its commit
    /// record and its data object share.
    L0 {
        shard: u32,
        ingest_hour_bucket: u32,
        writer_id: [u8; 16],
        writer_epoch: u64,
        writer_seq: u64,
    },
    /// A compaction or rewrite output part: its parent record's input-set hash
    /// plus its own part index.
    L1 {
        shard: u32,
        ingest_hour_bucket: u32,
        input_set_hash: [u8; 32],
        part_index: u32,
    },
}

/// Read one decoded snapshot entry's object identity, or `None` if its
/// identity fields do not fit the shape a fold writes (a 16-byte `writer_id`
/// at level 0, a 32-byte `input_set_hash` and a `u32`-ranged `part_index`
/// above it). `None` is a fail-closed signal, never "does not match": an entry
/// whose identity cannot be read cannot be proven not to name the candidate.
fn snapshot_object(entry: &SnapshotEntry) -> Option<SnapshotObject> {
    if entry.level == 0 {
        let writer_id: [u8; 16] = entry.writer_id.as_slice().try_into().ok()?;
        Some(SnapshotObject::L0 {
            shard: entry.shard,
            ingest_hour_bucket: entry.ingest_hour_bucket,
            writer_id,
            writer_epoch: entry.writer_epoch,
            writer_seq: entry.writer_seq,
        })
    } else {
        let input_set_hash: [u8; 32] = entry.writer_id.as_slice().try_into().ok()?;
        let part_index = u32::try_from(entry.writer_epoch).ok()?;
        Some(SnapshotObject::L1 {
            shard: entry.shard,
            ingest_hour_bucket: entry.ingest_hour_bucket,
            input_set_hash,
            part_index,
        })
    }
}

/// Per-sweep-pass cache of the catalog HEAD and the snapshot parts it names,
/// so a pass that gates many candidates of one `(tenant, signal)` reads HEAD at
/// most once and each covering part at most once, rather than once per
/// candidate (a per-candidate HEAD GET would be an S3-request-cost regression
/// against the live cost-reduction epic, ADR-0076). A retention pass creates
/// one per [`crate::scan::scan_and_maintain_with_memo`] call and threads it
/// through [`crate::retention::maintain_bucket_with_reach`]; the
/// superseded-input sweep creates one per pass. The public entry points
/// ([`crate::retention::maintain_bucket`],
/// [`crate::retention::retention_sweep_bucket`],
/// [`crate::sweep::sweep_superseded`]) create a fresh one per call.
///
/// The cache is read-once-per-pass, not a durable cache. For retention it is
/// safe precisely because a retention tombstone is irreversible (ADR-0019
/// decision 2), so a bucket the fold has already dropped from HEAD is never
/// re-added. For superseded inputs it is safe because a fold never starts
/// naming an input a published compaction or rewrite record already
/// superseded. In both directions a HEAD read that DOES name the candidate
/// only ever delays a delete by one pass, the fail-safe direction.
///
/// It also caches the unnamed-since markers (ADR-1133) the pass reads: one
/// signal-wide LIST of `maint/unn/` the first time a candidate reaches the
/// marker step, and each marker body at most once.
#[derive(Default)]
pub struct SnapshotReachability {
    head: Option<HeadLoad>,
    /// The store version of the HEAD read, for the marker's forensic field.
    head_version: String,
    /// Decoded snapshot parts by object key. `None` = present but unreadable
    /// (fail-closed); `Some` = decoded and usable.
    parts: HashMap<String, Option<Arc<DecodedPart>>>,
    markers: MarkerCache,
    read_gate: MaintainReadGate,
}

/// Whether a pass may write and delete unnamed-since markers, or only read
/// them (ADR-1133 decisions 3 and 6: an observing pass and a dry run write
/// none).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarkerPolicy {
    /// A deleting pass: writes a missing marker, replaces a mismatched one and
    /// deletes the marker of a re-named candidate.
    Write,
    /// A dry run: writes and deletes nothing, and answers a missing or
    /// mismatched marker as the deleting pass would after writing one now.
    DryRun,
    /// The erasure-request sweep's observing pass: writes and deletes
    /// nothing, and holds a candidate with a missing or mismatched marker.
    Observe,
}

/// What the pass needs to evaluate the pinned-query window.
#[derive(Clone, Copy)]
pub(crate) struct MarkerContext<'a> {
    pub(crate) clock: &'a dyn Clock,
    pub(crate) window: PinnedQueryWindow,
    pub(crate) policy: MarkerPolicy,
}

impl<'a> MarkerContext<'a> {
    pub(crate) fn new(
        clock: &'a dyn Clock,
        config: &CompactorConfig,
        policy: MarkerPolicy,
    ) -> Self {
        Self {
            clock,
            window: PinnedQueryWindow::from_config(config),
            policy,
        }
    }
}

/// Unnamed-since marker requests and transitions of one pass. Every request is
/// issued through the pass's own store handle, so it is also counted wherever
/// that handle's requests are.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarkerStats {
    /// Signal-wide `maint/unn/` LISTs (each drained across all its pages).
    pub listings: usize,
    pub get_requests: usize,
    pub put_requests: usize,
    pub delete_requests: usize,
    /// Fresh markers this pass wrote (its first unnamed observation of a
    /// candidate, or a restart after a mismatch or a re-name).
    pub written: usize,
    /// Markers deleted because HEAD names their candidate again (ADR-1133
    /// decision 5).
    pub reset_renamed: usize,
    /// Markers deleted because their anchor is not the tombstone or record
    /// present now (ADR-1133 decision 2).
    pub reset_mismatched: usize,
    /// Markers deleted after the objects they gated.
    pub retired: usize,
}

#[derive(Default)]
struct MarkerCache {
    /// The pass's one signal-wide marker LIST: `None` until needed, then
    /// `Some(Ok(listing))` or `Some(Err(()))` when it failed (every marker is
    /// then read by GET).
    listing: Option<std::result::Result<Vec<ObjectMeta>, ()>>,
    listed: HashSet<String>,
    scope: Option<(TenantHash, Signal)>,
    bodies: HashMap<String, MarkerLoad>,
    /// Anchors this pass read and gated, so the reaper need not re-check them.
    gated_anchors: HashSet<String>,
    /// Marker keys this pass wrote or deleted.
    touched: HashSet<String>,
    /// Whether this pass gated a candidate under [`MarkerPolicy::Write`].
    gated_writable: bool,
    /// Whether this pass already ran the orphan reaper.
    reaped: bool,
    stats: MarkerStats,
}

#[derive(Clone)]
enum MarkerLoad {
    Absent,
    Present(UnnamedMarker),
    Unreadable,
}

/// The catalog HEAD as read once for a sweep pass.
enum HeadLoad {
    /// HEAD is absent: no snapshot names anything, so nothing is blocked
    /// (ADR-0020: the index is a pure optimization; a missing HEAD degrades to
    /// listing).
    Absent,
    /// HEAD is present but undecodable (or a newer format this build cannot
    /// read): fail-closed, every candidate is blocked.
    Unreadable,
    /// HEAD is present and decoded. Boxed so this large variant does not
    /// inflate every `HeadLoad` (`clippy::large_enum_variant`): `SnapshotHead`
    /// grew past 300 bytes when ADR-0850 added `column_stats`, while the other
    /// variants are unit-sized.
    Present(Box<SnapshotHead>),
}

impl SnapshotReachability {
    /// A fresh, empty cache for one sweep pass.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decodes HEAD, and hashes and decodes each snapshot part, on `gate`
    /// under the `reachability` site (ADR-1702). Each job owns the fetched
    /// bytes. The default runs both inline.
    #[must_use]
    pub fn with_read_gate(mut self, gate: MaintainReadGate) -> Self {
        self.read_gate = gate;
        self
    }

    /// The unnamed-since marker requests and transitions of this pass so far.
    pub fn marker_stats(&self) -> &MarkerStats {
        &self.markers.stats
    }

    /// The pinned-query window gate (ADR-1133 decisions 2, 3, 5 and 6) over a
    /// candidate whose HEAD-reachability answer is `head_gate`, keyed by
    /// `marker_key` and written for `anchor`.
    ///
    /// - HEAD names the candidate: its marker, if any, is deleted (a re-named
    ///   candidate restarts its window) and the answer stays
    ///   [`SnapshotBlock::Named`].
    /// - HEAD unreadable: unchanged, no marker is touched.
    /// - HEAD clear: [`SnapshotGate::Clear`] only for a marker written for
    ///   this anchor whose `observed_unix_ns` is past the window on this
    ///   sweeper's clock. A missing marker is written and a mismatched one is
    ///   deleted and rewritten (under [`MarkerPolicy::Write`]); both answer
    ///   [`SnapshotBlock::PinnedWindow`], as does an unaged one.
    ///
    /// Any marker GET, PUT or DELETE error, or an undecodable body, answers
    /// [`SnapshotBlock::Unreadable`]: nothing here ever turns a doubt into a
    /// delete. A failed marker LIST only makes the pass read each marker by
    /// GET. The alerts signal gets no marker and keeps its HEAD answer.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn marker_gate(
        &mut self,
        store: &dyn ObjectStoreBackend,
        ctx: &MarkerContext<'_>,
        tenant: &TenantHash,
        signal: Signal,
        marker_key: &str,
        anchor: &MarkerAnchor,
        head_gate: SnapshotGate,
    ) -> SnapshotGate {
        // Alert records are never folded into a catalog HEAD, so no query
        // pins one that names them; ADR-1133 scopes the marker out of them.
        if signal == Signal::Alerts {
            return head_gate;
        }
        self.scope_markers(tenant, signal);
        match head_gate {
            SnapshotGate::Blocked(SnapshotBlock::Named) => {
                if ctx.policy == MarkerPolicy::Write
                    && self
                        .marker_may_exist(store, tenant, signal, marker_key)
                        .await
                {
                    if self.delete_marker(store, marker_key).await.is_err() {
                        return SnapshotGate::Blocked(SnapshotBlock::Unreadable);
                    }
                    self.markers.stats.reset_renamed += 1;
                }
                return head_gate;
            }
            SnapshotGate::Blocked(_) => return head_gate,
            SnapshotGate::Clear => {}
        }
        if ctx.policy == MarkerPolicy::Write {
            self.markers.gated_writable = true;
        }
        self.markers.gated_anchors.insert(anchor.key.clone());
        match self.load_marker(store, tenant, signal, marker_key).await {
            MarkerLoad::Unreadable => SnapshotGate::Blocked(SnapshotBlock::Unreadable),
            MarkerLoad::Present(marker) if marker.anchor == *anchor => {
                window_verdict(ctx, marker.observed_unix_ns)
            }
            MarkerLoad::Present(marker) => {
                if let Some(verdict) = read_only_verdict(ctx) {
                    return verdict;
                }
                tracing::warn!(
                    key = %marker_key,
                    marker_anchor = %marker.anchor.key,
                    marker_anchor_unix_ns = marker.anchor.anchor_unix_ns,
                    anchor_unix_ns = anchor.anchor_unix_ns,
                    "unnamed-since marker was written for another anchor; deleting it and \
                     restarting the pinned-query window"
                );
                if self.delete_marker(store, marker_key).await.is_err() {
                    return SnapshotGate::Blocked(SnapshotBlock::Unreadable);
                }
                self.markers.stats.reset_mismatched += 1;
                self.write_fresh_marker(store, ctx, marker_key, anchor)
                    .await
            }
            MarkerLoad::Absent => {
                if let Some(verdict) = read_only_verdict(ctx) {
                    return verdict;
                }
                self.write_fresh_marker(store, ctx, marker_key, anchor)
                    .await
            }
        }
    }

    /// Delete a marker after the objects it gated are gone (ADR-1133 decision
    /// 6 ordering: objects, then the marker, then the tombstone or record).
    pub(crate) async fn retire_marker(
        &mut self,
        store: &dyn ObjectStoreBackend,
        marker_key: &str,
    ) -> std::result::Result<(), StoreError> {
        self.delete_marker(store, marker_key).await?;
        self.markers.stats.retired += 1;
        Ok(())
    }

    /// The orphan-marker rule at the end of a deleting pass: reaps when this
    /// pass gated a candidate, or when `full_pass` (the full-keyspace sweep,
    /// which the server runs on its `interior_reverify_ns` safety-net cadence
    /// and the CLI on every run). Reuses the pass's LIST when it has one. A
    /// failure is logged and never fails the pass: a leftover marker only
    /// costs a later reap.
    pub(crate) async fn reap_after_pass(
        &mut self,
        store: &dyn ObjectStoreBackend,
        clock: &dyn Clock,
        config: &CompactorConfig,
        tenant: &TenantHash,
        signal: Signal,
        full_pass: bool,
    ) -> Option<MarkerReapOutcome> {
        if config.dry_run || signal == Signal::Alerts || self.markers.reaped {
            return None;
        }
        if !self.markers.gated_writable && !full_pass {
            return None;
        }
        self.markers.reaped = true;
        let same_scope = self.markers.scope == Some((*tenant, signal));
        let listing = match (&self.markers.listing, same_scope) {
            (Some(Ok(listing)), true) => listing.clone(),
            _ => {
                self.markers.stats.listings += 1;
                match list_all(store, &keys::unnamed_marker_prefix(tenant, signal)).await {
                    Ok(listing) => listing,
                    Err(error) => {
                        tracing::warn!(
                            tenant_hash = %tenant.to_hex(),
                            signal = signal.key_prefix(),
                            %error,
                            "unnamed-marker reaper: LIST failed; leftover markers wait for a \
                             later pass"
                        );
                        return None;
                    }
                }
            }
        };
        Some(
            reap_listed(
                store,
                clock,
                config,
                tenant,
                signal,
                &listing,
                &self.markers.gated_anchors,
                &self.markers.touched,
            )
            .await,
        )
    }

    /// Point the marker cache at `(tenant, signal)`, dropping what it held for
    /// any other scope. Called before anything is recorded for a candidate, so
    /// the reset never discards this scope's own state.
    fn scope_markers(&mut self, tenant: &TenantHash, signal: Signal) {
        if self.markers.scope != Some((*tenant, signal)) {
            self.markers = MarkerCache {
                stats: std::mem::take(&mut self.markers.stats),
                scope: Some((*tenant, signal)),
                ..MarkerCache::default()
            };
        }
    }

    /// Load the pass's signal-wide marker LIST once. A failed LIST is
    /// remembered, and every marker is then read by GET instead.
    async fn ensure_marker_listing(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
    ) -> bool {
        self.scope_markers(tenant, signal);
        if self.markers.listing.is_none() {
            self.markers.stats.listings += 1;
            let listed = list_all(store, &keys::unnamed_marker_prefix(tenant, signal)).await;
            self.markers.listing = Some(match listed {
                Ok(listing) => {
                    self.markers.listed = listing.iter().map(|m| m.key.clone()).collect();
                    Ok(listing)
                }
                Err(error) => {
                    tracing::warn!(
                        tenant_hash = %tenant.to_hex(),
                        signal = signal.key_prefix(),
                        %error,
                        "unnamed-since marker LIST failed; reading each marker by GET this pass"
                    );
                    Err(())
                }
            });
        }
        matches!(self.markers.listing, Some(Ok(_)))
    }

    /// Whether a marker may exist at `marker_key`: listed, cached present, or
    /// unknown because the LIST failed.
    async fn marker_may_exist(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        marker_key: &str,
    ) -> bool {
        let listed = self.ensure_marker_listing(store, tenant, signal).await;
        match self.markers.bodies.get(marker_key) {
            Some(MarkerLoad::Absent) => false,
            Some(_) => true,
            None => !listed || self.markers.listed.contains(marker_key),
        }
    }

    async fn load_marker(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        marker_key: &str,
    ) -> MarkerLoad {
        let listed = self.ensure_marker_listing(store, tenant, signal).await;
        if let Some(cached) = self.markers.bodies.get(marker_key) {
            return cached.clone();
        }
        let load = if listed && !self.markers.listed.contains(marker_key) {
            MarkerLoad::Absent
        } else {
            self.get_marker(store, marker_key).await
        };
        self.markers
            .bodies
            .insert(marker_key.to_string(), load.clone());
        load
    }

    async fn get_marker(&mut self, store: &dyn ObjectStoreBackend, marker_key: &str) -> MarkerLoad {
        self.markers.stats.get_requests += 1;
        match store.get(marker_key, GetRange::Full).await {
            Ok(got) => match UnnamedMarker::decode(got.data.as_ref()) {
                Ok(marker) => MarkerLoad::Present(marker),
                Err(error) => {
                    tracing::warn!(
                        key = %marker_key,
                        %error,
                        "unnamed-since marker cannot be decoded; blocking its candidate's delete \
                         fail-closed"
                    );
                    MarkerLoad::Unreadable
                }
            },
            Err(StoreError::NotFound) => MarkerLoad::Absent,
            Err(error) => {
                tracing::warn!(
                    key = %marker_key,
                    %error,
                    "unnamed-since marker GET failed; blocking its candidate's delete fail-closed"
                );
                MarkerLoad::Unreadable
            }
        }
    }

    async fn write_fresh_marker(
        &mut self,
        store: &dyn ObjectStoreBackend,
        ctx: &MarkerContext<'_>,
        marker_key: &str,
        anchor: &MarkerAnchor,
    ) -> SnapshotGate {
        // Read here, after the HEAD GET this candidate was gated on has
        // returned, never at the pass's start: a reading taken before that GET
        // can predate the drop it records.
        let marker = UnnamedMarker {
            observed_unix_ns: ctx.clock.now_ns(),
            anchor: anchor.clone(),
            head_version: self.head_version.clone(),
        };
        self.markers.stats.put_requests += 1;
        match put_marker(store, marker_key, &marker).await {
            Ok(()) => {
                self.markers.stats.written += 1;
                self.markers.touched.insert(marker_key.to_string());
                self.markers.listed.insert(marker_key.to_string());
                let observed = marker.observed_unix_ns;
                self.markers
                    .bodies
                    .insert(marker_key.to_string(), MarkerLoad::Present(marker));
                window_verdict(ctx, observed)
            }
            Err(StoreError::AlreadyExists) => {
                // Another sweeper wrote it first: its marker stands if it was
                // written for the same anchor.
                let load = self.get_marker(store, marker_key).await;
                self.markers
                    .bodies
                    .insert(marker_key.to_string(), load.clone());
                match load {
                    MarkerLoad::Present(existing) if existing.anchor == *anchor => {
                        window_verdict(ctx, existing.observed_unix_ns)
                    }
                    MarkerLoad::Present(_) | MarkerLoad::Absent => {
                        SnapshotGate::Blocked(SnapshotBlock::PinnedWindow)
                    }
                    MarkerLoad::Unreadable => SnapshotGate::Blocked(SnapshotBlock::Unreadable),
                }
            }
            Err(error) => {
                tracing::warn!(
                    key = %marker_key,
                    %error,
                    "unnamed-since marker PUT failed; blocking its candidate's delete fail-closed"
                );
                SnapshotGate::Blocked(SnapshotBlock::Unreadable)
            }
        }
    }

    async fn delete_marker(
        &mut self,
        store: &dyn ObjectStoreBackend,
        marker_key: &str,
    ) -> std::result::Result<(), StoreError> {
        self.markers.stats.delete_requests += 1;
        match store.delete(marker_key).await {
            Ok(()) => {
                self.markers.touched.insert(marker_key.to_string());
                self.markers.listed.remove(marker_key);
                self.markers
                    .bodies
                    .insert(marker_key.to_string(), MarkerLoad::Absent);
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    key = %marker_key,
                    %error,
                    "unnamed-since marker DELETE failed; blocking its candidate's delete \
                     fail-closed"
                );
                Err(error)
            }
        }
    }

    /// Whether the live HEAD snapshot still reaches an object inside `bucket`
    /// (ADR-0020 delete-blocker). HEAD and each covering part are loaded at
    /// most once per pass and cached. HEAD absent -> [`SnapshotGate::Clear`];
    /// HEAD or any covering part unreadable -> fail-closed
    /// [`SnapshotBlock::Unreadable`]; a decoded entry naming this bucket's
    /// shard+hour -> [`SnapshotBlock::Named`].
    pub(crate) async fn bucket_gate(
        &mut self,
        store: &dyn ObjectStoreBackend,
        bucket: &Bucket,
    ) -> Result<SnapshotGate> {
        let (covering, skipped) = match self
            .covering_parts(
                store,
                &bucket.tenant_hash,
                bucket.signal,
                bucket.ingest_hour_bucket,
            )
            .await?
        {
            Covering::Clear => return Ok(SnapshotGate::Clear),
            Covering::Blocked(reason) => return Ok(SnapshotGate::Blocked(reason)),
            Covering::Parts { covering, skipped } => (covering, skipped),
        };

        for part_ref in &covering {
            match self
                .ensure_part(store, &bucket.tenant_hash, part_ref)
                .await?
            {
                // A covering part could not be read: cannot prove
                // non-reachability, fail closed.
                None => return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable)),
                Some(part) => {
                    // An object is physically inside this bucket iff its entry's
                    // shard and ingest hour match the bucket. Any such entry
                    // means the snapshot still names an object the sweep would
                    // delete.
                    if part.entries.iter().any(|e| {
                        e.shard == bucket.shard && e.ingest_hour_bucket == bucket.ingest_hour_bucket
                    }) {
                        return Ok(SnapshotGate::Blocked(SnapshotBlock::Named));
                    }
                }
            }
        }
        self.clear_or_block_on_skipped(store, &bucket.tenant_hash, &skipped)
            .await
    }

    /// Whether the live HEAD snapshot still names any of `objects`, all of
    /// which sit in `ingest_hour_bucket`. The object-granular counterpart of
    /// [`Self::bucket_gate`]: the superseded-input sweep deletes individual
    /// objects out of a bucket whose other objects (the compaction or rewrite
    /// outputs that superseded them) the snapshot legitimately still names, so
    /// a bucket-granular question would block it forever.
    ///
    /// Same three answers, same cache, same fail-closed direction. An empty
    /// `objects` is [`SnapshotGate::Clear`] without reading anything, so a pass
    /// with no delete candidate issues no HEAD GET at all.
    ///
    /// `objects` is indexed into a set once per call, so the cost is one hash
    /// lookup per snapshot entry rather than a scan of every candidate: a group
    /// holding a long supersession chain gates in time linear in the covering
    /// parts' entry count, not in its product with the group's size.
    pub(crate) async fn object_gate(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        ingest_hour_bucket: u32,
        objects: &[SnapshotObject],
    ) -> Result<SnapshotGate> {
        if objects.is_empty() {
            return Ok(SnapshotGate::Clear);
        }
        let wanted: HashSet<SnapshotObject> = objects.iter().copied().collect();
        let (covering, skipped) = match self
            .covering_parts(store, tenant, signal, ingest_hour_bucket)
            .await?
        {
            Covering::Clear => return Ok(SnapshotGate::Clear),
            Covering::Blocked(reason) => return Ok(SnapshotGate::Blocked(reason)),
            Covering::Parts { covering, skipped } => (covering, skipped),
        };

        for part_ref in &covering {
            let Some(part) = self.ensure_part(store, tenant, part_ref).await? else {
                return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable));
            };
            for entry in &part.entries {
                let Some(named) = snapshot_object(entry) else {
                    // An entry whose identity fields cannot be read: the same
                    // fail-closed answer an unreadable part gets.
                    return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable));
                };
                if wanted.contains(&named) {
                    return Ok(SnapshotGate::Blocked(SnapshotBlock::Named));
                }
            }
        }
        self.clear_or_block_on_skipped(store, tenant, &skipped)
            .await
    }

    /// The last step of both gates, reached only when every covering part came
    /// back clear and the gate is about to permit a delete: prove that the
    /// parts the hour-range filter skipped really were outside the hour.
    ///
    /// [`Self::covering_parts`] reads each part's range from the HEAD-level
    /// reference, not from the part itself. A reference whose declared range is
    /// narrower than the part it names would silently exclude a part that does
    /// hold entries for the gated hour, so a skip is only sound once
    /// [`Self::ensure_part`] has confirmed the two agree; it returns `None`
    /// when they do not, and that is the fail-closed answer here too.
    ///
    /// Done last, and only on the clearing path, so the ordinary held pass
    /// still reads nothing beyond HEAD and the covering parts: a delete this
    /// pass would not have performed anyway never pays for the proof.
    async fn clear_or_block_on_skipped(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        skipped: &[SnapshotPartRef],
    ) -> Result<SnapshotGate> {
        for part_ref in skipped {
            if self.ensure_part(store, tenant, part_ref).await?.is_none() {
                return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable));
            }
        }
        Ok(SnapshotGate::Clear)
    }

    /// Load HEAD once for the pass and split its part refs into the ones whose
    /// declared hour range covers `ingest_hour_bucket` and the ones it does
    /// not, or return the terminal answer when HEAD is absent or unreadable.
    /// The refs are owned clones so the borrow of `self.head` is released
    /// before the callers' part loads borrow `self` mutably.
    async fn covering_parts(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        ingest_hour_bucket: u32,
    ) -> Result<Covering> {
        match self.ensure_head(store, tenant, signal).await? {
            HeadStatus::Absent => Ok(Covering::Clear),
            HeadStatus::Unreadable => Ok(Covering::Blocked(SnapshotBlock::Unreadable)),
            HeadStatus::Present => match &self.head {
                Some(HeadLoad::Present(head)) => {
                    let (covering, skipped) = head.parts.iter().cloned().partition(|p| {
                        p.min_hour <= ingest_hour_bucket && ingest_hour_bucket <= p.watermark_hour
                    });
                    Ok(Covering::Parts { covering, skipped })
                }
                // Unreachable after `ensure_head` returned `Present`; block
                // fail-closed rather than panic (no unwrap/expect on a
                // production path).
                _ => Ok(Covering::Blocked(SnapshotBlock::Unreadable)),
            },
        }
    }

    /// Load HEAD once for the pass, caching the result. Returns a lightweight
    /// status; the decoded HEAD itself stays in `self.head` for the covering
    /// part-ref extraction in [`Self::covering_parts`]. A HEAD whose header
    /// names a tenant other than `tenant` is `Unreadable`, and none of its
    /// parts is read.
    async fn ensure_head(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
    ) -> Result<HeadStatus> {
        if self.head.is_none() {
            let head_key = catalog_head_key(tenant, signal);
            let load = match store.get(&head_key, GetRange::Full).await {
                Ok(got) => {
                    let version = got.version.0;
                    let data = got.data;
                    let size = data.len() as u64;
                    let decoded = self
                        .read_gate
                        .run(ReadSite::Reachability, size, move || {
                            decode_head(data.as_ref())
                        })
                        .await?;
                    match decoded {
                        // A HEAD naming another tenant (ADR-0050 §2) cannot
                        // prove anything about this tenant's objects.
                        Ok(head) if head.tenant_hash.as_slice() != tenant.0.as_slice() => {
                            tracing::error!(
                                key = %head_key,
                                expected_tenant = %tenant.to_hex(),
                                actual_tenant = %hex::encode(&head.tenant_hash),
                                "maintain sweep: snapshot HEAD names another tenant; blocking \
                                 deletes fail-closed"
                            );
                            HeadLoad::Unreadable
                        }
                        // A HEAD at this signal's key naming another signal
                        // describes some other catalog's objects, not these.
                        Ok(head)
                            if ravel_commit::signal::from_proto(head.signal as i32)
                                != Ok(signal) =>
                        {
                            tracing::error!(
                                key = %head_key,
                                expected_signal = ?signal,
                                actual_signal = head.signal,
                                "maintain sweep: snapshot HEAD names another signal; blocking \
                                 deletes fail-closed"
                            );
                            HeadLoad::Unreadable
                        }
                        Ok(head) => {
                            self.head_version = version;
                            HeadLoad::Present(Box::new(head))
                        }
                        Err(err) => {
                            // Present but undecodable/newer: fail-closed. Cannot
                            // prove non-reachability from a HEAD we cannot read.
                            tracing::warn!(
                                error = %err,
                                key = %head_key,
                                "maintain sweep: catalog HEAD failed to decode; blocking deletes \
                                 fail-closed this pass rather than proving non-reachability from an \
                                 unreadable HEAD"
                            );
                            HeadLoad::Unreadable
                        }
                    }
                }
                Err(StoreError::NotFound) => HeadLoad::Absent,
                Err(err) => return Err(MaintainError::Store(err)),
            };
            self.head = Some(load);
        }
        Ok(match &self.head {
            Some(HeadLoad::Absent) => HeadStatus::Absent,
            Some(HeadLoad::Unreadable) => HeadStatus::Unreadable,
            Some(HeadLoad::Present(_)) => HeadStatus::Present,
            None => HeadStatus::Unreadable,
        })
    }

    /// Load, verify (blake3 against the HEAD ref, the header's tenant against
    /// `tenant` (ADR-0050 §2), and the ref's hour range against the decoded
    /// header's), and decode one snapshot part once per pass, caching the
    /// result. `Ok(None)` is the fail-closed "present but unreadable" case (a
    /// missing HEAD-named part, a hash mismatch, a foreign tenant, a bounds
    /// mismatch, or a decode failure); a transient store fault propagates.
    ///
    /// The bounds check is what makes [`Self::covering_parts`]' range filter
    /// safe to trust. That filter skips every part whose
    /// `[min_hour, watermark_hour]` excludes the hour being gated, reading
    /// those bounds from the HEAD-level reference; a reference whose range is
    /// narrower than the part it names would then let the gate skip a part
    /// that does name the candidate, and clear a delete the snapshot still
    /// reaches.
    async fn ensure_part(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        part_ref: &SnapshotPartRef,
    ) -> Result<Option<Arc<DecodedPart>>> {
        if let Some(cached) = self.parts.get(&part_ref.key) {
            return Ok(cached.clone());
        }
        let load: Option<Arc<DecodedPart>> = match store.get(&part_ref.key, GetRange::Full).await {
            Ok(got) => {
                let data = got.data;
                let size = data.len() as u64;
                let expected = part_ref.blake3.clone();
                let tenant = *tenant;
                // The hash check and the decode run as one job on the read
                // gate (ADR-1702); `None` is a hash mismatch.
                let decoded = self
                    .read_gate
                    .run(ReadSite::Reachability, size, move || {
                        if blake3::hash(&data).as_bytes().as_slice() != expected.as_slice() {
                            return None;
                        }
                        let limits = PartLimits {
                            max_snapshot_part_bytes: ravel_catalog::DEFAULT_MAX_SNAPSHOT_PART_BYTES,
                        };
                        Some(decode_part_for_tenant(&tenant, data.as_ref(), &limits))
                    })
                    .await?;
                if let Some(decoded) = decoded {
                    match decoded {
                        Ok(part)
                            if part.header.min_hour != part_ref.min_hour
                                || part.header.watermark_hour != part_ref.watermark_hour =>
                        {
                            tracing::warn!(
                                key = %part_ref.key,
                                ref_min_hour = part_ref.min_hour,
                                ref_watermark_hour = part_ref.watermark_hour,
                                header_min_hour = part.header.min_hour,
                                header_watermark_hour = part.header.watermark_hour,
                                "maintain sweep: snapshot part reference range disagrees with the \
                                 part header; blocking deletes fail-closed"
                            );
                            None
                        }
                        Ok(part) => Some(Arc::new(part)),
                        Err(PartTenantError::ForeignTenant { expected, actual }) => {
                            tracing::error!(
                                key = %part_ref.key,
                                expected_tenant = %expected,
                                actual_tenant = %actual,
                                "maintain sweep: snapshot part names another tenant; blocking \
                                 deletes fail-closed"
                            );
                            None
                        }
                        Err(PartTenantError::Corrupt(err)) => {
                            tracing::warn!(
                                error = %err,
                                key = %part_ref.key,
                                "maintain sweep: snapshot part failed to decode; blocking deletes \
                                 fail-closed"
                            );
                            None
                        }
                    }
                } else {
                    tracing::warn!(
                        key = %part_ref.key,
                        "maintain sweep: snapshot part hash mismatch; blocking deletes fail-closed"
                    );
                    None
                }
            }
            // HEAD names a part that is not present. Anomalous (a HEAD-named
            // part is only deleted after HEAD stops naming it plus the
            // horizon): cannot read its entries, so fail closed.
            Err(StoreError::NotFound) => {
                tracing::warn!(
                    key = %part_ref.key,
                    "maintain sweep: HEAD-named snapshot part is missing; blocking deletes \
                     fail-closed"
                );
                None
            }
            Err(err) => return Err(MaintainError::Store(err)),
        };
        self.parts.insert(part_ref.key.clone(), load.clone());
        Ok(load)
    }
}

/// What [`SnapshotReachability::covering_parts`] resolved for one hour: either
/// a terminal gate answer that needs no part reads, or the parts to inspect.
enum Covering {
    /// HEAD is absent: nothing is blocked.
    Clear,
    /// HEAD itself decided the answer (unreadable).
    Blocked(SnapshotBlock),
    /// The HEAD-named parts split by the declared hour range: `covering` is
    /// read for entries naming the candidate, `skipped` only has its declared
    /// range checked against the part header, and only when the gate is
    /// otherwise about to clear.
    Parts {
        covering: Vec<SnapshotPartRef>,
        skipped: Vec<SnapshotPartRef>,
    },
}

/// Lightweight status returned by [`SnapshotReachability::ensure_head`], so the
/// decoded HEAD can stay owned in the cache while the caller decides how to
/// proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeadStatus {
    Absent,
    Unreadable,
    Present,
}

/// `t/<tenant_hash_hex>/catalog/<signal>/HEAD` -- the mutable head pointer for
/// one `(tenant, signal)` (docs/catalog-and-mvcc.md key layout, a frozen
/// contract). No public builder is exported from ravel-catalog, so it is
/// constructed here from the same pieces. This is the crate's only copy; the
/// catalog-object sweep in [`crate::sweep`] uses it too.
pub(crate) fn catalog_head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, ListPage, MultipartUpload, ObjectMeta, PageToken,
        PutOptions, PutOutcome,
    };

    use super::*;
    use crate::clock::FixedClock;
    use crate::unnamed_marker::MarkerKind;

    /// A behavior-preserving decorator that counts GETs per key, so a test can
    /// prove a part is read at most once per pass (the
    /// `SnapshotReachability::parts` cache) rather than once per gate call.
    struct CountingStore {
        inner: MemoryStore,
        gets: Mutex<HashMap<String, u64>>,
        deletes: Mutex<u64>,
    }

    impl CountingStore {
        fn new() -> Self {
            CountingStore {
                inner: MemoryStore::new(),
                gets: Mutex::new(HashMap::new()),
                deletes: Mutex::new(0),
            }
        }

        fn get_count(&self, key: &str) -> u64 {
            *self.gets.lock().expect("lock").get(key).unwrap_or(&0)
        }

        fn delete_count(&self) -> u64 {
            *self.deletes.lock().expect("lock")
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for CountingStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> std::result::Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> std::result::Result<GetOutcome, StoreError> {
            *self
                .gets
                .lock()
                .expect("lock")
                .entry(key.to_string())
                .or_insert(0) += 1;
            self.inner.get(key, range).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> std::result::Result<Box<dyn MultipartUpload + 'a>, StoreError> {
            self.inner.put_multipart(key).await
        }

        async fn head(&self, key: &str) -> std::result::Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> std::result::Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> std::result::Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
            *self.deletes.lock().expect("lock") += 1;
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    const TEST_TENANT: TenantHash = TenantHash([0xAA; 16]);
    const FOREIGN_TENANT: TenantHash = TenantHash([0xBB; 16]);
    const TEST_SHARD: u32 = 0;
    const TEST_HOUR: u32 = 5;
    const PART_KEY: &str = "t/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/catalog/m/snap/foreign.csnap";

    fn candidate_entry() -> SnapshotEntry {
        SnapshotEntry {
            level: 0,
            shard: TEST_SHARD,
            ingest_hour_bucket: TEST_HOUR,
            writer_id: vec![0xCC; 16],
            writer_epoch: 1,
            writer_seq: 1,
            content_hash: vec![1; 32],
            object_size: 100,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            sample_count: 0,
            series_count: 0,
            segment_format_version: 0,
            created_unix_ns: 0,
            declared_column_stats: Vec::new(),
        }
    }

    /// Plants a HEAD for `TEST_TENANT` naming one covering part, whose bytes
    /// are encoded for `FOREIGN_TENANT` instead: the HEAD-level reference
    /// (blake3, size, hour range) is correct for the bytes actually stored, so
    /// only the part's own header names a different tenant, exactly the state
    /// `ensure_part`'s blake3 check cannot catch and the tenant check must.
    /// `entries` controls whether the foreign part's own entries name the
    /// candidate bucket (fixture a) or not (fixture b).
    async fn plant_foreign_tenant_part(
        store: &dyn ObjectStoreBackend,
        entries: Vec<SnapshotEntry>,
    ) {
        plant_head_and_part(store, TEST_TENANT, FOREIGN_TENANT, entries).await;
    }

    /// Plants a HEAD at `TEST_TENANT`'s HEAD key whose header names
    /// `head_tenant`, naming one covering part at `PART_KEY` encoded for
    /// `part_tenant`, with a HEAD-level reference correct for the stored
    /// bytes.
    async fn plant_head_and_part(
        store: &dyn ObjectStoreBackend,
        head_tenant: TenantHash,
        part_tenant: TenantHash,
        entries: Vec<SnapshotEntry>,
    ) {
        plant_head_and_part_for_signal(store, head_tenant, Signal::Metrics, part_tenant, entries)
            .await;
    }

    /// [`plant_head_and_part`], with the HEAD's header naming `head_signal`
    /// while it is stored at the metrics HEAD key.
    async fn plant_head_and_part_for_signal(
        store: &dyn ObjectStoreBackend,
        head_tenant: TenantHash,
        head_signal: Signal,
        part_tenant: TenantHash,
        entries: Vec<SnapshotEntry>,
    ) {
        let part_bytes = ravel_catalog::encode_part(
            part_tenant.0,
            ravel_commit::signal::to_proto(Signal::Metrics) as u32,
            TEST_SHARD + 1,
            TEST_HOUR,
            &entries,
        )
        .expect("encode part for the foreign tenant");
        store
            .put(
                PART_KEY,
                bytes::Bytes::from(part_bytes.clone()),
                ravel_object_store::PutOptions::default(),
            )
            .await
            .expect("put part");
        let head = SnapshotHead {
            format_version: ravel_catalog::HEAD_FORMAT_VERSION,
            tenant_hash: head_tenant.0.to_vec(),
            signal: ravel_commit::signal::to_proto(head_signal) as u32,
            shard_count: TEST_SHARD + 1,
            watermark_hour: TEST_HOUR,
            parts: vec![SnapshotPartRef {
                key: PART_KEY.to_string(),
                blake3: blake3::hash(&part_bytes).as_bytes().to_vec(),
                size: part_bytes.len() as u64,
                entry_count: entries.len() as u64,
                watermark_hour: TEST_HOUR,
                min_hour: 0,
                column_stats: None,
                key_index: Vec::new(),
            }],
            folder_id: vec![0u8; 16],
            created_unix_ns: 1,
            postings: None,
            shard_generation_count: 1,
        };
        let head_bytes = ravel_catalog::encode_head(&head).expect("encode HEAD");
        store
            .put(
                &catalog_head_key(&TEST_TENANT, Signal::Metrics),
                bytes::Bytes::from(head_bytes),
                ravel_object_store::PutOptions::default(),
            )
            .await
            .expect("put HEAD");
    }

    fn candidate_bucket() -> Bucket {
        Bucket {
            tenant_hash: TEST_TENANT,
            signal: Signal::Metrics,
            shard: TEST_SHARD,
            ingest_hour_bucket: TEST_HOUR,
        }
    }

    /// A HEAD-named part whose header names another tenant blocks fail-closed
    /// as `Unreadable`, not as `Named`, even though the foreign part's own
    /// entries name the candidate bucket. The part is read once per pass.
    ///
    /// FLIP (W1): have `ensure_part` call `decode_part` directly instead of
    /// `decode_part_for_tenant` (skip the tenant check entirely). The foreign
    /// part's entry still matches the candidate's shard and hour, so the gate
    /// returns `Blocked(Named)` instead of `Blocked(Unreadable)`, and this
    /// assertion fails.
    #[tokio::test]
    async fn foreign_tenant_part_naming_the_candidate_blocks_unreadable() {
        let store = CountingStore::new();
        plant_foreign_tenant_part(&store, vec![candidate_entry()]).await;
        let mut reach = SnapshotReachability::new();
        let gate = reach
            .bucket_gate(&store, &candidate_bucket())
            .await
            .expect("gate");
        assert_eq!(gate, SnapshotGate::Blocked(SnapshotBlock::Unreadable));

        // Same pass, a second candidate in the same bucket: the part is
        // cached, not re-read.
        let gate2 = reach
            .bucket_gate(&store, &candidate_bucket())
            .await
            .expect("gate");
        assert_eq!(gate2, SnapshotGate::Blocked(SnapshotBlock::Unreadable));
        assert_eq!(
            store.get_count(PART_KEY),
            1,
            "the foreign-tenant part is read once per pass, then cached"
        );
        assert_eq!(
            store.get_count(&catalog_head_key(&TEST_TENANT, Signal::Metrics)),
            1,
            "HEAD is read once per pass, then cached"
        );
    }

    /// The dangerous case: a HEAD-named part whose header names another
    /// tenant, and whose entries do NOT name the candidate bucket at all,
    /// still blocks fail-closed as `Unreadable`. Without the tenant check, the
    /// gate would decode the part, find no entry naming the candidate, and
    /// answer `Clear`, permitting the delete.
    ///
    /// FLIP (W1, the dangerous direction): same change as above. With no
    /// entry matching the candidate, the gate now answers `Clear` instead of
    /// `Blocked(Unreadable)`, and this assertion fails.
    #[tokio::test]
    async fn foreign_tenant_part_naming_nothing_still_blocks_unreadable() {
        let store = CountingStore::new();
        plant_foreign_tenant_part(&store, Vec::new()).await;
        let mut reach = SnapshotReachability::new();
        let gate = reach
            .bucket_gate(&store, &candidate_bucket())
            .await
            .expect("gate");
        assert_eq!(
            gate,
            SnapshotGate::Blocked(SnapshotBlock::Unreadable),
            "a foreign-tenant part blocks fail-closed even when its own entries name nothing here"
        );
    }

    /// A HEAD at this tenant's key whose header names another tenant blocks
    /// fail-closed as `Unreadable` before any of its parts is read, even
    /// though the part it names is a valid part of this tenant whose entries
    /// name nothing here (the answer that, read from that part, would clear
    /// the delete).
    ///
    /// FLIP: drop the `head.tenant_hash` comparison in `ensure_head`. The
    /// HEAD is then trusted, its part is read and passes the part check, no
    /// entry names the candidate, and the gate answers `Clear` instead of
    /// `Blocked(Unreadable)`, so the first assertion fails.
    #[tokio::test]
    async fn foreign_tenant_head_blocks_unreadable_without_reading_parts() {
        let store = CountingStore::new();
        plant_head_and_part(&store, FOREIGN_TENANT, TEST_TENANT, Vec::new()).await;
        let mut reach = SnapshotReachability::new();
        let gate = reach
            .bucket_gate(&store, &candidate_bucket())
            .await
            .expect("gate");
        assert_eq!(
            gate,
            SnapshotGate::Blocked(SnapshotBlock::Unreadable),
            "a HEAD naming another tenant blocks fail-closed"
        );
        let objects = [snapshot_object(&candidate_entry()).expect("candidate identity")];
        let gate2 = reach
            .object_gate(&store, &TEST_TENANT, Signal::Metrics, TEST_HOUR, &objects)
            .await
            .expect("object gate");
        assert_eq!(gate2, SnapshotGate::Blocked(SnapshotBlock::Unreadable));
        assert_eq!(
            store.get_count(PART_KEY),
            0,
            "no part of a foreign-tenant HEAD is read"
        );
        assert_eq!(
            store.get_count(&catalog_head_key(&TEST_TENANT, Signal::Metrics)),
            1,
            "the refused HEAD is read once per pass, then cached"
        );
        assert_eq!(store.delete_count(), 0, "nothing is deleted");
    }

    /// A HEAD at this tenant's metrics key whose header names the logs signal
    /// blocks fail-closed as `Unreadable` before any of its parts is read,
    /// although the part it names is this tenant's own and names nothing
    /// here (the answer that, read from that part, would clear the delete).
    ///
    /// FLIP: drop the `head.signal` comparison in `ensure_head`. The HEAD is
    /// then trusted, its part is read, no entry names the candidate, and the
    /// gate answers `Clear` instead of `Blocked(Unreadable)`.
    #[tokio::test]
    async fn other_signal_head_blocks_unreadable_without_reading_parts() {
        let store = CountingStore::new();
        plant_head_and_part_for_signal(&store, TEST_TENANT, Signal::Logs, TEST_TENANT, Vec::new())
            .await;
        let mut reach = SnapshotReachability::new();
        let gate = reach
            .bucket_gate(&store, &candidate_bucket())
            .await
            .expect("gate");
        assert_eq!(
            gate,
            SnapshotGate::Blocked(SnapshotBlock::Unreadable),
            "a HEAD naming another signal blocks fail-closed"
        );
        assert_eq!(
            store.get_count(PART_KEY),
            0,
            "no part of an other-signal HEAD is read"
        );
        assert_eq!(store.delete_count(), 0, "nothing is deleted");
    }

    /// No marker for the alerts signal (ADR-1133 decision 6): a clear HEAD
    /// answer stays clear and the gate issues no marker request, while the same
    /// call for metrics writes one and holds.
    #[tokio::test]
    async fn the_alerts_signal_gets_no_marker() {
        let tenant = TenantHash([7; 16]);
        let clock = FixedClock::new(1_000);
        let config = CompactorConfig::default();
        let ctx = MarkerContext::new(&clock, &config, MarkerPolicy::Write);
        for (signal, expected, markers) in [
            (Signal::Alerts, SnapshotGate::Clear, 0),
            (
                Signal::Metrics,
                SnapshotGate::Blocked(SnapshotBlock::PinnedWindow),
                1,
            ),
        ] {
            let store = MemoryStore::new();
            let key = keys::retention_unnamed_marker_key(&tenant, signal, 0, 10).expect("key");
            let anchor = MarkerAnchor {
                kind: MarkerKind::Retention,
                key: keys::retention_tombstone_key(&tenant, signal, 0, 10).expect("tmb"),
                anchor_unix_ns: 1,
                version: "1".to_string(),
            };
            let mut reach = SnapshotReachability::new();
            let gate = reach
                .marker_gate(
                    &store,
                    &ctx,
                    &tenant,
                    signal,
                    &key,
                    &anchor,
                    SnapshotGate::Clear,
                )
                .await;
            assert_eq!(gate, expected, "{signal:?}");
            let listed = list_all(&store, &keys::unnamed_marker_prefix(&tenant, signal))
                .await
                .expect("list");
            assert_eq!(listed.len(), markers, "{signal:?}");
            assert_eq!(reach.marker_stats().put_requests, markers, "{signal:?}");
            assert_eq!(reach.marker_stats().listings, markers, "{signal:?}");
        }
    }
}
