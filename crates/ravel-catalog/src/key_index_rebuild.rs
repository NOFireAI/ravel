//! ADR-2707 decision 4, "Lifecycle": the explicit repair that attaches
//! per-part key-index leaves to parts already in HEAD, without re-encoding
//! them (`ravel-cli catalog fold --rebuild-key-index`, docs/catalog-and-mvcc.md
//! "Rollout").
//!
//! The leaves are built by the fold's own leaf build from the same ranged
//! KEY_IDX reads, so for a declared field a rebuilt leaf is byte-identical to
//! the one a fold with `build_key_index_leaves` on writes for the same part.
//! The fold also indexes fields a KEY_IDX section names that the tenant does
//! not declare; the rebuild does not.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use futures::stream::{self, StreamExt};
use ravel_codec::key_idx::KeyType;
use ravel_object_store::{PutMode, PutOptions, StoreError, UploadChecksum, Version};
use ravel_proto::catalog::v1::{SnapshotHead, SnapshotKeyIndexLeafRef, SnapshotPartRef};
use ravel_types::{Signal, TenantHash};

use crate::catalog::Catalog;
use crate::column_stats_build;
use crate::config::DEFAULT_SNAPSHOT_PART_MAX_ENTRIES;
use crate::error::CatalogError;
use crate::fold::{
    EntryKeyIndex, EntryKeyIndexRead, HeadState, MAX_HEAD_CAS_ATTEMPTS, RequestCounters,
    build_part_key_index_leaves, declared_key_index_fields, head_object_key, put_key_index_leaf,
    read_entry_key_index,
};
use crate::snapshot_format::{self, KeyIndexLeafLimits};
use crate::tenant_config::TenantConfigError;

/// Default `--batch-entries`: the entry total one rebuild batch stays at or
/// below, the fold's own part-sealing default.
pub const DEFAULT_KEY_INDEX_REBUILD_BATCH_ENTRIES: u64 = DEFAULT_SNAPSHOT_PART_MAX_ENTRIES as u64;

/// What [`Catalog::rebuild_key_index`] rebuilds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIndexRebuildOptions {
    /// Rebuild only this declared field. `None` rebuilds every declared field.
    pub field: Option<String>,
    /// A batch is the longest run of remaining unindexed parts whose entry
    /// total stays at or below this, and at least one part.
    pub batch_entries: u64,
}

impl Default for KeyIndexRebuildOptions {
    fn default() -> Self {
        Self {
            field: None,
            batch_entries: DEFAULT_KEY_INDEX_REBUILD_BATCH_ENTRIES,
        }
    }
}

/// One batch's publish, its HEAD CAS retries included.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct KeyIndexRebuildAttempt {
    /// Parts the batch read.
    pub parts: u64,
    /// Whole-object GETs of those parts.
    pub part_gets: u64,
    /// Ranged GETs of data objects (footer suffix, any extra footer range,
    /// KEY_IDX section) the batch issued.
    pub ranged_gets: u64,
    /// Leaf PUTs the batch issued.
    pub leaf_puts: u64,
    /// HEAD CAS writes the batch issued: 0 when no part gained a ref.
    pub head_cas_tries: u32,
    /// Parts the batch's published HEAD attached at least one ref to.
    pub parts_published: u64,
}

/// Outcome of [`Catalog::rebuild_key_index`].
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct KeyIndexRebuildReport {
    /// Entry count of the largest part that lacks a requested field's ref,
    /// read off the HEAD part refs before any part or data object GET.
    pub largest_unindexed_part_entries: u64,
    /// The fields the run rebuilds.
    pub fields: Vec<String>,
    /// Non-empty parts in HEAD lacking a ref for at least one of `fields`.
    pub parts_unindexed: u64,
    /// Parts a published HEAD attached at least one ref to.
    pub parts_rebuilt: u64,
    /// Parts given no ref this run because a ranged GET for one of their
    /// entries failed with a store error; a rerun reads them again.
    pub parts_deferred: u64,
    /// Leaves PUT, including any a lost CAS left unreferenced.
    pub leaves_written: u64,
    /// Total bytes of those leaves.
    pub leaf_bytes_written: u64,
    /// Bytes the ranged GETs returned: footer suffixes, any extra footer
    /// ranges, and KEY_IDX sections, as transferred, retries excluded.
    pub section_bytes_read: u64,
    /// Ranged GETs of data objects: footer suffixes, any extra footer
    /// ranges, and KEY_IDX sections together.
    pub ranged_gets: u64,
    /// For each part built (deferred parts excluded) and each field built for
    /// it, the part entries no leaf covers, summed.
    pub uncovered_entries: u64,
    /// Batches that reached their publish step.
    pub attempt_count: u64,
    pub attempts: Vec<KeyIndexRebuildAttempt>,
}

/// Why [`Catalog::rebuild_key_index`] stopped. Variants raised after work
/// began carry the report of what earlier batches published.
#[derive(Debug, thiserror::Error)]
pub enum KeyIndexRebuildError {
    #[error(
        "key-index field {field:?} is not declared for this tenant's {signal:?}; declared: {declared:?}"
    )]
    FieldNotDeclared {
        field: String,
        signal: Signal,
        declared: Vec<String>,
    },
    #[error("{0:?} objects carry no KEY_IDX section; the key-index rebuild covers logs and spans")]
    UnsupportedSignal(Signal),
    #[error(
        "tenant {tenant} declares no key-index field for {signal:?}; declare a key_index column or turn on trace-id indexing first"
    )]
    NoFieldDeclared { tenant: String, signal: Signal },
    #[error("tenant config read failed: {0}")]
    TenantConfig(#[from] TenantConfigError),
    #[error("HEAD at {key} does not decode; run a plain fold to rebuild it first")]
    CorruptHead { key: String },
    #[error(
        "HEAD CAS lost {attempts} times publishing batch {batch}; {} earlier part(s) published",
        report.parts_rebuilt
    )]
    CasRetriesExhausted {
        attempts: u32,
        batch: u64,
        report: Box<KeyIndexRebuildReport>,
    },
    #[error(
        "HEAD at {key} was absent or undecodable after a lost CAS publishing batch {batch}; {} earlier part(s) published",
        report.parts_rebuilt
    )]
    HeadLost {
        key: String,
        batch: u64,
        report: Box<KeyIndexRebuildReport>,
    },
    #[error("{source}; {} earlier part(s) published", report.parts_rebuilt)]
    Interrupted {
        source: CatalogError,
        report: Box<KeyIndexRebuildReport>,
    },
}

impl KeyIndexRebuildError {
    /// What earlier batches published before the stop, when work began.
    pub fn report(&self) -> Option<&KeyIndexRebuildReport> {
        match self {
            Self::CasRetriesExhausted { report, .. }
            | Self::HeadLost { report, .. }
            | Self::Interrupted { report, .. } => Some(report),
            Self::FieldNotDeclared { .. }
            | Self::NoFieldDeclared { .. }
            | Self::UnsupportedSignal(_)
            | Self::TenantConfig(_)
            | Self::CorruptHead { .. } => None,
        }
    }
}

/// A part HEAD names without a ref for some requested field, and those fields.
struct Target {
    part: SnapshotPartRef,
    missing: Vec<(String, KeyType)>,
}

fn missing_fields(
    part: &SnapshotPartRef,
    requested: &[(String, KeyType)],
) -> Vec<(String, KeyType)> {
    requested
        .iter()
        .filter(|(field, _)| !part.key_index.iter().any(|r| &r.field == field))
        .cloned()
        .collect()
}

/// Splits `targets` into runs whose entry total stays at or below
/// `batch_entries`, each holding at least one part.
fn batches(targets: Vec<Target>, batch_entries: u64) -> Vec<Vec<Target>> {
    let mut out: Vec<Vec<Target>> = Vec::new();
    let mut current: Vec<Target> = Vec::new();
    let mut total = 0u64;
    for target in targets {
        let entries = target.part.entry_count;
        if !current.is_empty() && total.saturating_add(entries) > batch_entries {
            out.push(std::mem::take(&mut current));
            total = 0;
        }
        total = total.saturating_add(entries);
        current.push(target);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Copies `head` with each built ref attached to the part whose `blake3` it
/// was built for, when that part is still named and still lacks the ref's
/// field. Returns the copy and how many parts gained a ref.
fn attach(
    head: &SnapshotHead,
    built: &[(Vec<u8>, Vec<SnapshotKeyIndexLeafRef>)],
) -> (SnapshotHead, u64) {
    let mut next = head.clone();
    let mut attached = 0u64;
    for (part_blake3, refs) in built {
        let Some(part) = next.parts.iter_mut().find(|p| &p.blake3 == part_blake3) else {
            continue;
        };
        let new: Vec<SnapshotKeyIndexLeafRef> = refs
            .iter()
            .filter(|r| !part.key_index.iter().any(|have| have.field == r.field))
            .cloned()
            .collect();
        if !new.is_empty() {
            part.key_index.extend(new);
            attached += 1;
        }
    }
    (next, attached)
}

impl Catalog {
    /// Attaches key-index leaves to the parts in `signal`'s HEAD that lack a
    /// ref for a declared field (or for `options.field` only), without
    /// re-encoding any part (ADR-2707 decision 4, "Lifecycle"). Runs
    /// regardless of [`crate::CatalogConfig::build_key_index_leaves`]. A
    /// tenant declaring no key-index field for `signal` is refused, as is an
    /// undeclared `options.field`, before HEAD is read.
    ///
    /// Each part is read once and verified against its `blake3`; each of its
    /// entries costs the fold's ranged KEY_IDX reads. The leaves are built and
    /// PUT by the fold's own functions under the fold's key layout, then HEAD
    /// is CAS-published with every part ref unchanged except its `key_index`.
    /// Writes no part, no `.csnap`, no `.cstat`, and lists nothing.
    ///
    /// Parts go in batches (see [`KeyIndexRebuildOptions::batch_entries`]),
    /// one HEAD publish each, of up to the fold's CAS retry budget. A lost CAS
    /// re-reads HEAD and attaches refs only to batch parts whose `blake3` the
    /// new HEAD still names; it reads no section again. A part already holding
    /// a ref for every requested field costs no GET, and a part holding some
    /// keeps them and gains the rest. A part one of whose entries' ranged
    /// GETs failed with a store error is deferred: it gains no ref this run,
    /// so a rerun reads it again.
    pub async fn rebuild_key_index(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        options: &KeyIndexRebuildOptions,
    ) -> Result<KeyIndexRebuildReport, KeyIndexRebuildError> {
        let tenant_config = crate::tenant_config::read_config_values(self.store(), tenant).await?;
        let Some(declared) = declared_key_index_fields(signal, tenant_config.as_ref(), tenant)
        else {
            return Err(KeyIndexRebuildError::UnsupportedSignal(signal));
        };
        let requested: Vec<(String, KeyType)> = match &options.field {
            None => declared,
            Some(field) => match declared.iter().find(|(name, _)| name == field) {
                Some(found) => vec![found.clone()],
                None => {
                    return Err(KeyIndexRebuildError::FieldNotDeclared {
                        field: field.clone(),
                        signal,
                        declared: declared.into_iter().map(|(name, _)| name).collect(),
                    });
                }
            },
        };
        if requested.is_empty() {
            return Err(KeyIndexRebuildError::NoFieldDeclared {
                tenant: tenant.to_hex(),
                signal,
            });
        }
        let mut report = KeyIndexRebuildReport {
            fields: requested.iter().map(|(name, _)| name.clone()).collect(),
            ..KeyIndexRebuildReport::default()
        };

        let head_key = head_object_key(tenant, signal);
        let mut counters = RequestCounters::default();
        let (mut head, mut version) = match self.get_head(&head_key, &mut counters).await {
            Ok(HeadState::Valid { head, version }) => (*head, version),
            Ok(HeadState::Absent) => return Ok(report),
            Ok(HeadState::Corrupt { .. }) => {
                return Err(KeyIndexRebuildError::CorruptHead { key: head_key });
            }
            Ok(HeadState::UnsupportedVersion { format_version }) => {
                return Err(KeyIndexRebuildError::Interrupted {
                    source: CatalogError::UnsupportedHeadVersion { format_version },
                    report: Box::new(report),
                });
            }
            Err(source) => {
                return Err(KeyIndexRebuildError::Interrupted {
                    source,
                    report: Box::new(report),
                });
            }
        };

        // An empty part has no entry to index.
        let targets: Vec<Target> = head
            .parts
            .iter()
            .filter(|part| part.entry_count > 0)
            .filter_map(|part| {
                let missing = missing_fields(part, &requested);
                (!missing.is_empty()).then(|| Target {
                    part: part.clone(),
                    missing,
                })
            })
            .collect();
        report.parts_unindexed = targets.len() as u64;
        report.largest_unindexed_part_entries = targets
            .iter()
            .map(|t| t.part.entry_count)
            .max()
            .unwrap_or(0);

        for (index, batch) in batches(targets, options.batch_entries)
            .into_iter()
            .enumerate()
        {
            let batch_number = index as u64 + 1;
            match self
                .rebuild_batch(
                    tenant,
                    signal,
                    &head_key,
                    batch,
                    &mut head,
                    &mut version,
                    &mut report,
                )
                .await
            {
                Ok(()) => {}
                Err(BatchError::CasExhausted(attempts)) => {
                    return Err(KeyIndexRebuildError::CasRetriesExhausted {
                        attempts,
                        batch: batch_number,
                        report: Box::new(report),
                    });
                }
                Err(BatchError::HeadLost) => {
                    return Err(KeyIndexRebuildError::HeadLost {
                        key: head_key,
                        batch: batch_number,
                        report: Box::new(report),
                    });
                }
                Err(BatchError::Catalog(source)) => {
                    return Err(KeyIndexRebuildError::Interrupted {
                        source,
                        report: Box::new(report),
                    });
                }
            }
        }
        Ok(report)
    }

    /// Builds one batch's leaves and publishes them. `head`/`version` are the
    /// HEAD this run last read or wrote, updated on every re-read and publish.
    #[allow(clippy::too_many_arguments)]
    async fn rebuild_batch(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        head_key: &str,
        batch: Vec<Target>,
        head: &mut SnapshotHead,
        version: &mut Version,
        report: &mut KeyIndexRebuildReport,
    ) -> Result<(), BatchError> {
        let mut attempt = KeyIndexRebuildAttempt::default();
        let mut counters = RequestCounters::default();
        let mut reads: HashMap<String, EntryKeyIndex> = HashMap::new();
        let mut failed_gets: HashSet<String> = HashSet::new();
        let mut built: Vec<(Vec<u8>, Vec<SnapshotKeyIndexLeafRef>)> = Vec::new();
        for target in batch {
            // A part an earlier batch's CAS loss showed replaced, or already
            // given the refs by someone else, costs nothing.
            let Some(current) = head.parts.iter().find(|p| p.blake3 == target.part.blake3) else {
                continue;
            };
            let missing: Vec<(String, KeyType)> = target
                .missing
                .into_iter()
                .filter(|(field, _)| !current.key_index.iter().any(|r| &r.field == field))
                .collect();
            if missing.is_empty() {
                continue;
            }
            let part = target.part;
            attempt.parts += 1;
            attempt.part_gets += 1;
            let entries = self.load_part_entries(&part, &mut counters).await?;
            let part_blake3: [u8; 32] =
                part.blake3
                    .as_slice()
                    .try_into()
                    .map_err(|_| CatalogError::FieldMismatch {
                        key: part.key.clone(),
                        field: "blake3",
                        expected: "32 bytes".to_string(),
                        actual: format!("{} bytes", part.blake3.len()),
                    })?;

            let data_keys: Vec<Result<String, EntryKeyIndex>> = entries
                .iter()
                .map(|entry| {
                    column_stats_build::segment_data_key(tenant, signal, entry)
                        .map_err(|e| Err(format!("data key: {e}")))
                })
                .collect();
            let mut unread: Vec<(String, u64)> = Vec::new();
            let mut queued: HashSet<&str> = HashSet::new();
            for (entry, key) in entries.iter().zip(&data_keys) {
                if let Ok(key) = key
                    && !reads.contains_key(key)
                    && queued.insert(key.as_str())
                {
                    unread.push((key.clone(), entry.object_size));
                }
            }
            let store = self.store();
            let unread_ref = &unread;
            let fresh: Vec<EntryKeyIndexRead> = stream::iter(0..unread.len())
                .map(move |i| {
                    let (key, object_size) = &unread_ref[i];
                    read_entry_key_index(store, tenant, signal, key, *object_size)
                })
                .buffered(self.config().fold_bucket_concurrency.max(1))
                .collect()
                .await;
            for ((key, _), read) in unread.into_iter().zip(fresh) {
                attempt.ranged_gets += read.gets;
                report.ranged_gets += read.gets;
                report.section_bytes_read += read.bytes;
                if read.get_failed {
                    if let Err(reason) = &read.fields {
                        tracing::warn!(
                            tenant = %tenant.to_hex(),
                            object_key = %key,
                            reason = %reason,
                            "KEY_IDX read GET failed; the parts naming this entry are deferred to a rerun"
                        );
                    }
                    failed_gets.insert(key.clone());
                }
                reads.insert(key, read.fields);
            }
            let failed_entries = data_keys
                .iter()
                .filter(|key| matches!(key, Ok(key) if failed_gets.contains(key)))
                .count();
            if failed_entries > 0 {
                report.parts_deferred += 1;
                tracing::warn!(
                    tenant = %tenant.to_hex(),
                    part_key = %part.key,
                    failed_entries,
                    "a KEY_IDX GET failed for this part's entries; the part gains no key-index ref this run and a rerun reads it again"
                );
                continue;
            }
            let not_read: EntryKeyIndex = Err("KEY_IDX not read".to_string());
            let entry_fields: Vec<&EntryKeyIndex> = data_keys
                .iter()
                .map(|key| match key {
                    Ok(key) => reads.get(key).unwrap_or(&not_read),
                    Err(read) => read,
                })
                .collect();
            let leaves = build_part_key_index_leaves(
                tenant,
                signal,
                part_blake3,
                &missing,
                false,
                &entry_fields,
                &KeyIndexLeafLimits::default(),
            );
            report.uncovered_entries += leaves.uncovered_entries;
            let mut refs = Vec::with_capacity(leaves.leaves.len());
            for leaf in leaves.leaves {
                attempt.leaf_puts += 1;
                let leaf_ref =
                    put_key_index_leaf(self.store(), tenant, signal, part.watermark_hour, leaf)
                        .await
                        .map_err(CatalogError::Store)?;
                report.leaves_written += 1;
                report.leaf_bytes_written += leaf_ref.size;
                refs.push(leaf_ref);
            }
            if !refs.is_empty() {
                built.push((part.blake3, refs));
            }
        }
        if attempt.parts == 0 {
            return Ok(());
        }
        report.attempt_count += 1;
        report.attempts.push(attempt);
        self.publish_batch(head_key, &built, head, version, report)
            .await
    }

    /// The batch's HEAD publish: attach, CAS, and on a lost CAS re-read HEAD
    /// and attach again, up to the fold's retry budget.
    async fn publish_batch(
        &self,
        head_key: &str,
        built: &[(Vec<u8>, Vec<SnapshotKeyIndexLeafRef>)],
        head: &mut SnapshotHead,
        version: &mut Version,
        report: &mut KeyIndexRebuildReport,
    ) -> Result<(), BatchError> {
        let mut tries = 0u32;
        loop {
            let (next, attached) = attach(head, built);
            if attached == 0 {
                return Ok(());
            }
            let bytes = snapshot_format::encode_head(&next).map_err(CatalogError::from)?;
            let crc = crc32c::crc32c(&bytes);
            let put = self
                .store()
                .put(
                    head_key,
                    Bytes::from(bytes),
                    PutOptions {
                        mode: PutMode::CasVersion(version.clone()),
                        checksum: Some(UploadChecksum::Crc32c(crc)),
                    },
                )
                .await;
            tries += 1;
            if let Some(attempt) = report.attempts.last_mut() {
                attempt.head_cas_tries = tries;
            }
            match put {
                Ok(outcome) => {
                    *head = next;
                    *version = outcome.version;
                    report.parts_rebuilt += attached;
                    if let Some(attempt) = report.attempts.last_mut() {
                        attempt.parts_published = attached;
                    }
                    return Ok(());
                }
                Err(StoreError::PreconditionFailed) | Err(StoreError::AlreadyExists) => {
                    if tries >= MAX_HEAD_CAS_ATTEMPTS {
                        return Err(BatchError::CasExhausted(tries));
                    }
                    let mut counters = RequestCounters::default();
                    match self.get_head(head_key, &mut counters).await? {
                        HeadState::Valid {
                            head: winner,
                            version: winner_version,
                        } => {
                            *head = *winner;
                            *version = winner_version;
                        }
                        HeadState::UnsupportedVersion { format_version } => {
                            return Err(
                                CatalogError::UnsupportedHeadVersion { format_version }.into()
                            );
                        }
                        HeadState::Absent | HeadState::Corrupt { .. } => {
                            return Err(BatchError::HeadLost);
                        }
                    }
                }
                Err(e) => return Err(CatalogError::Store(e).into()),
            }
        }
    }
}

enum BatchError {
    CasExhausted(u32),
    /// HEAD was absent or undecodable on the re-read after a lost CAS.
    HeadLost,
    Catalog(CatalogError),
}

impl From<CatalogError> for BatchError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use ravel_codec::key_idx::{key8_i64, key8_id16};
    use ravel_commit::keys;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault, Sequence,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta,
        ObjectStoreBackend, PageToken, PutOutcome,
    };
    use ravel_proto::catalog::v1::SnapshotPostingsRef;
    use ravel_proto::commit::v1::CommitRecord;

    use super::*;
    use crate::config::CatalogConfig;
    use crate::fold::KEY_INDEX_FOOTER_SUFFIX_LEN;
    use crate::fold::tests::{
        KIDX_HOUR, USER_ID, config, declare_user_id_key_index, fold_through, kidx_objects, matches,
        part_ordinals, probe_part, publish_trace_spans, publish_user_id_logs, read_signal_head,
        tenant,
    };
    use crate::snapshot_format::{KeyIndexLeafProbe, verify_key_index_leaf};

    /// Records every GET (key, range, bytes returned), every PUT key and every
    /// LIST. `head_race`, when set, is written over HEAD unconditionally just
    /// before the next HEAD CAS, which that CAS then loses.
    #[derive(Default)]
    struct RecordingStore {
        inner: MemoryStore,
        gets: Mutex<Vec<(String, GetRange, u64)>>,
        puts: Mutex<Vec<String>>,
        lists: Mutex<u64>,
        head_race: Mutex<Option<Bytes>>,
    }

    impl RecordingStore {
        fn clear(&self) {
            self.gets.lock().expect("lock").clear();
            self.puts.lock().expect("lock").clear();
            *self.lists.lock().expect("lock") = 0;
        }

        fn gets(&self) -> Vec<(String, GetRange, u64)> {
            self.gets.lock().expect("lock").clone()
        }

        /// Ranged GETs of data objects (keys outside the catalog prefix).
        fn data_ranged_gets(&self) -> Vec<(String, GetRange, u64)> {
            self.gets()
                .into_iter()
                .filter(|(key, range, _)| {
                    !key.contains("/catalog/") && !matches!(range, GetRange::Full)
                })
                .collect()
        }

        fn puts(&self) -> Vec<String> {
            self.puts.lock().expect("lock").clone()
        }

        fn lists(&self) -> u64 {
            *self.lists.lock().expect("lock")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for RecordingStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.puts.lock().expect("lock").push(key.to_string());
            if matches!(opts.mode, PutMode::CasVersion(_)) && key.ends_with("/HEAD") {
                let race = self.head_race.lock().expect("lock").take();
                if let Some(race) = race {
                    self.inner
                        .put(
                            key,
                            race,
                            PutOptions {
                                mode: PutMode::Overwrite,
                                checksum: None,
                            },
                        )
                        .await?;
                }
            }
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            let got = self.inner.get(key, range).await;
            let len = got.as_ref().map_or(0, |g| g.data.len() as u64);
            self.gets
                .lock()
                .expect("lock")
                .push((key.to_string(), range, len));
            got
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            *self.lists.lock().expect("lock") += 1;
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            *self.lists.lock().expect("lock") += 1;
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    fn recording_store() -> Arc<RecordingStore> {
        Arc::new(RecordingStore::default())
    }

    /// The switch-off catalog every fixture folds with and every rebuild runs
    /// under, so no leaf is built by anything but the rebuild.
    fn off_catalog(store: Arc<dyn ObjectStoreBackend>, part_max_entries: usize) -> Catalog {
        let cfg = CatalogConfig {
            snapshot_part_max_entries: part_max_entries,
            ..config(1)
        };
        assert!(!cfg.build_key_index_leaves);
        Catalog::new(store, cfg).expect("catalog")
    }

    async fn rebuild(catalog: &Catalog, signal: Signal) -> KeyIndexRebuildReport {
        catalog
            .rebuild_key_index(&tenant(), signal, &KeyIndexRebuildOptions::default())
            .await
            .expect("rebuild")
    }

    fn without_key_index(head: &SnapshotHead) -> SnapshotHead {
        let mut head = head.clone();
        for part in &mut head.parts {
            part.key_index.clear();
        }
        head
    }

    fn data_key(record: &CommitRecord) -> String {
        keys::reconstruct_data_key(record).expect("data key")
    }

    fn head_key(signal: Signal) -> String {
        head_object_key(&tenant(), signal)
    }

    /// ADR-2707 decision 4, "Lifecycle", the acceptance test: on a logs tenant
    /// declaring `user_id` (I64, `key_index`) and on a spans tenant, both
    /// folded with `build_key_index_leaves` off, the rebuild attaches one leaf
    /// per part, leaves every other byte of HEAD as it was (part key, blake3,
    /// entry count and column stats included), PUTs only the leaf and HEAD,
    /// and lists nothing. The probe then names exact (entry, block set) pairs:
    /// logs 10 sits in entry 1 block 0 and entry 2 blocks 0 and 1, 50 in entry
    /// 2 block 0, 99 nowhere; trace t1 in both span entries, t2 in the first.
    ///
    /// A rebuild that re-encoded the part changes its blake3 and PUTs a
    /// `.csnap`; one that read only the first entry answers 10 with entry 1
    /// alone and 50 as uncovered.
    #[tokio::test]
    async fn rebuild_attaches_leaves_to_a_stripped_part_and_the_probe_names_the_blocks() {
        let store = recording_store();
        declare_user_id_key_index(store.as_ref()).await;
        for (seq, user_ids) in [
            (1, &[10i64, 10, 20, 30, 40, 40][..]),
            (2, &[50, 10, 60, 10]),
            (3, &[70, 80]),
        ] {
            publish_user_id_logs(store.as_ref(), seq, KIDX_HOUR, user_ids, 2, 0, true).await;
        }
        let catalog = off_catalog(store.clone(), DEFAULT_SNAPSHOT_PART_MAX_ENTRIES);
        fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
        let before = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(before.parts.len(), 1);
        assert!(
            before.parts[0].key_index.is_empty(),
            "folded with leaves off"
        );
        assert!(kidx_objects(store.as_ref(), Signal::Logs).await.is_empty());

        store.clear();
        let report = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(
            report,
            KeyIndexRebuildReport {
                largest_unindexed_part_entries: 3,
                fields: vec![USER_ID.to_string()],
                parts_unindexed: 1,
                parts_rebuilt: 1,
                parts_deferred: 0,
                leaves_written: 1,
                leaf_bytes_written: report.leaf_bytes_written,
                section_bytes_read: report.section_bytes_read,
                ranged_gets: 6,
                uncovered_entries: 0,
                attempt_count: 1,
                attempts: vec![KeyIndexRebuildAttempt {
                    parts: 1,
                    part_gets: 1,
                    ranged_gets: 6,
                    leaf_puts: 1,
                    head_cas_tries: 1,
                    parts_published: 1,
                }],
            }
        );
        let after = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            without_key_index(&after),
            before,
            "every HEAD byte but key_index is unchanged"
        );
        let part = &after.parts[0];
        assert_eq!(part.key_index.len(), 1);
        let leaf = &part.key_index[0];
        assert_eq!(leaf.field, USER_ID);
        assert_eq!(leaf.part_blake3, part.blake3);
        assert_eq!(report.leaf_bytes_written, leaf.size);
        assert_eq!(
            store.puts(),
            vec![leaf.key.clone(), head_key(Signal::Logs)],
            "one leaf PUT and one HEAD CAS: no part, .csnap or .cstat"
        );
        assert_eq!(store.lists(), 0, "the rebuild lists nothing");
        let part_gets: Vec<String> = store
            .gets()
            .into_iter()
            .filter(|(key, range, _)| key == &part.key && matches!(range, GetRange::Full))
            .map(|(key, _, _)| key)
            .collect();
        assert_eq!(part_gets, vec![part.key.clone()], "the part is read once");

        let ordinal = part_ordinals(store.as_ref(), part).await;
        let probe = |v: i64| {
            probe_part(
                &catalog,
                Signal::Logs,
                part,
                KeyType::I64,
                USER_ID,
                key8_i64(v),
            )
        };
        assert_eq!(
            probe(10).await,
            matches(&[(ordinal[&1], &[0]), (ordinal[&2], &[0, 1])], &[])
        );
        assert_eq!(probe(50).await, matches(&[(ordinal[&2], &[0])], &[]));
        assert_eq!(
            probe(99).await,
            KeyIndexLeafProbe::Absent {
                uncovered_entry_ordinals: Vec::new()
            }
        );

        // Spans: trace_id needs no declaration.
        let store = recording_store();
        let (t1, t2, absent) = ([0x11u8; 16], [0x22u8; 16], [0x44u8; 16]);
        publish_trace_spans(store.as_ref(), 1, &[t1, t2]).await;
        publish_trace_spans(store.as_ref(), 2, &[t1, [0x33u8; 16]]).await;
        let catalog = off_catalog(store.clone(), DEFAULT_SNAPSHOT_PART_MAX_ENTRIES);
        fold_through(&catalog, Signal::Spans, KIDX_HOUR).await;
        let before = read_signal_head(store.as_ref(), Signal::Spans).await;
        assert!(before.parts[0].key_index.is_empty());
        store.clear();
        let report = rebuild(&catalog, Signal::Spans).await;
        assert_eq!(
            (
                report.fields.clone(),
                report.parts_rebuilt,
                report.ranged_gets
            ),
            (vec![ravel_rspan::TRACE_ID_FIELD.to_string()], 1, 4),
            "{report:?}"
        );
        let after = read_signal_head(store.as_ref(), Signal::Spans).await;
        assert_eq!(without_key_index(&after), before);
        let part = &after.parts[0];
        assert_eq!(
            store.puts(),
            vec![part.key_index[0].key.clone(), head_key(Signal::Spans)]
        );
        let ordinal = part_ordinals(store.as_ref(), part).await;
        let field = ravel_rspan::TRACE_ID_FIELD;
        let probe = |t: [u8; 16]| {
            probe_part(
                &catalog,
                Signal::Spans,
                part,
                KeyType::Id16,
                field,
                key8_id16(&t),
            )
        };
        assert_eq!(
            probe(t1).await,
            matches(&[(ordinal[&1], &[0]), (ordinal[&2], &[0])], &[])
        );
        assert_eq!(probe(t2).await, matches(&[(ordinal[&1], &[0])], &[]));
        assert_eq!(
            probe(absent).await,
            KeyIndexLeafProbe::Absent {
                uncovered_entry_ordinals: Vec::new()
            }
        );
    }

    /// Publishes two logs objects into each of `hours`, folds with the switch
    /// off and at most two entries to a part, so each hour is a part of its
    /// own, and returns the catalog with each part's data object keys.
    async fn two_entry_parts(
        store: &Arc<RecordingStore>,
        hours: u32,
    ) -> (Catalog, Vec<BTreeSet<String>>) {
        declare_user_id_key_index(store.as_ref()).await;
        let mut keys = Vec::new();
        for h in 0..hours {
            let mut part_keys = BTreeSet::new();
            for i in 0..2u64 {
                let seq = u64::from(h) * 2 + i + 1;
                let (record, _) = publish_user_id_logs(
                    store.as_ref(),
                    seq,
                    KIDX_HOUR + h,
                    &[10, seq as i64 * 100],
                    2,
                    0,
                    true,
                )
                .await;
                part_keys.insert(data_key(&record));
            }
            keys.push(part_keys);
        }
        let catalog = off_catalog(store.clone(), 2);
        fold_through(&catalog, Signal::Logs, KIDX_HOUR + hours - 1).await;
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts.len(), hours as usize);
        assert!(head.parts.iter().all(|p| p.entry_count == 2));
        (catalog, keys)
    }

    /// Three parts of two entries, `--batch-entries 2`, so each part is its own
    /// batch; every HEAD CAS of batch 3 is lost. The run fails with the CAS
    /// budget exhausted on batch 3 after publishing batches 1 and 2, which
    /// HEAD then names with refs. The failed run issues exactly four ranged
    /// GETs per batch, twelve in all, by the report and by the store's own
    /// record, however many CAS tries batch 3 spends. A rerun reads no section
    /// and no part object of parts 1 and 2, issues exactly the config GET, the
    /// HEAD GET, part 3's GET and its four ranged GETs, and completes part 3.
    ///
    /// A rebuild that published every batch in one CAS at the end leaves no
    /// ref on HEAD here; one that re-read sections on a CAS retry issues more
    /// than twelve ranged GETs; one that did not skip indexed parts re-reads
    /// parts 1 and 2 on the rerun.
    #[tokio::test]
    async fn rebuild_interrupted_publishes_earlier_batches_and_rerun_skips_them() {
        let inner = recording_store();
        let (_, part_keys) = two_entry_parts(&inner, 3).await;
        let plan = FaultPlan::empty().with_sequence(
            (0..MAX_HEAD_CAS_ATTEMPTS).fold(
                Sequence::new(Op::Put)
                    .with_key_contains(head_key(Signal::Logs))
                    .then_passthrough()
                    .then_passthrough(),
                |seq, _| seq.then_fault(ScriptedFault::FailedConditionalWrite),
            ),
        );
        let faulty = Arc::new(FaultStore::new(Arc::clone(&inner), plan));
        let catalog = off_catalog(faulty.clone(), 2);
        let options = KeyIndexRebuildOptions {
            field: None,
            batch_entries: 2,
        };
        inner.clear();
        let err = catalog
            .rebuild_key_index(&tenant(), Signal::Logs, &options)
            .await
            .expect_err("batch 3 loses every CAS");
        let KeyIndexRebuildError::CasRetriesExhausted {
            attempts,
            batch,
            report,
        } = &err
        else {
            panic!("{err:?}");
        };
        assert_eq!((*attempts, *batch), (MAX_HEAD_CAS_ATTEMPTS, 3));
        assert_eq!(err.report(), Some(report.as_ref()));
        assert_eq!(report.parts_rebuilt, 2, "{report:?}");
        assert_eq!(report.attempt_count, 3, "{report:?}");
        let tries: Vec<u32> = report.attempts.iter().map(|a| a.head_cas_tries).collect();
        assert_eq!(tries, vec![1, 1, MAX_HEAD_CAS_ATTEMPTS]);
        let ranged: Vec<u64> = report.attempts.iter().map(|a| a.ranged_gets).collect();
        assert_eq!(ranged, vec![4, 4, 4], "{report:?}");
        assert_eq!(report.ranged_gets, 12, "{report:?}");
        let read = inner.data_ranged_gets();
        assert_eq!(
            read.len(),
            12,
            "no section re-read on a CAS retry: {read:?}"
        );
        let read_keys: BTreeSet<String> = read.iter().map(|(key, _, _)| key.clone()).collect();
        let all_keys: BTreeSet<String> = part_keys.iter().flatten().cloned().collect();
        assert_eq!(read_keys, all_keys);
        assert_eq!(
            faulty.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
            u64::from(MAX_HEAD_CAS_ATTEMPTS)
        );
        let head = read_signal_head(inner.as_ref(), Signal::Logs).await;
        let refs: Vec<usize> = head.parts.iter().map(|p| p.key_index.len()).collect();
        assert_eq!(refs, vec![1, 1, 0], "batches 1 and 2 published");

        inner.clear();
        let catalog = off_catalog(inner.clone(), 2);
        let report = catalog
            .rebuild_key_index(&tenant(), Signal::Logs, &options)
            .await
            .expect("rerun");
        assert_eq!(
            (
                report.parts_unindexed,
                report.parts_rebuilt,
                report.ranged_gets
            ),
            (1, 1, 4),
            "{report:?}"
        );
        let gets = inner.gets();
        let data_keys: BTreeSet<String> = inner
            .data_ranged_gets()
            .into_iter()
            .map(|(key, _, _)| key)
            .collect();
        assert_eq!(data_keys, part_keys[2], "only part 3's objects are read");
        let whole: Vec<String> = gets
            .iter()
            .filter(|(_, range, _)| matches!(range, GetRange::Full))
            .map(|(key, _, _)| key.clone())
            .collect();
        assert_eq!(
            whole,
            vec![
                crate::tenant_config::config_key(&tenant()),
                head_key(Signal::Logs),
                head.parts[2].key.clone(),
            ]
        );
        assert_eq!(gets.len(), 3 + 4, "{gets:?}");
        let head = read_signal_head(inner.as_ref(), Signal::Logs).await;
        let refs: Vec<usize> = head.parts.iter().map(|p| p.key_index.len()).collect();
        assert_eq!(refs, vec![1, 1, 1]);
    }

    /// A batch is the longest run of parts whose entry total stays at or below
    /// `batch_entries`, and at least one part: two 2-entry parts are two
    /// batches at 1 (each part above the bound is a batch of its own) and at
    /// 3, and one batch at 4.
    #[tokio::test]
    async fn a_batch_is_the_longest_run_within_batch_entries_and_at_least_one_part() {
        for (batch_entries, want) in [(1, vec![1, 1]), (3, vec![1, 1]), (4, vec![2])] {
            let store = recording_store();
            let (catalog, _) = two_entry_parts(&store, 2).await;
            store.clear();
            let report = catalog
                .rebuild_key_index(
                    &tenant(),
                    Signal::Logs,
                    &KeyIndexRebuildOptions {
                        field: None,
                        batch_entries,
                    },
                )
                .await
                .expect("rebuild");
            let parts: Vec<u64> = report.attempts.iter().map(|a| a.parts).collect();
            assert_eq!(parts, want, "batch_entries {batch_entries}: {report:?}");
            assert_eq!(report.largest_unindexed_part_entries, 2);
            assert_eq!(report.parts_rebuilt, 2);
            let cas = store
                .puts()
                .iter()
                .filter(|key| key.ends_with("/HEAD"))
                .count();
            assert_eq!(cas, want.len(), "one HEAD CAS per batch");
        }
    }

    /// Three parts of two entries, `--batch-entries 4`: batch 1 is parts 1
    /// and 2, batch 2 is part 3. Between batch 1's leaf build and its HEAD CAS
    /// another writer publishes a HEAD that replaces part 2 (a different
    /// `part_blake3`), drops part 3, appends a new part (advancing the
    /// watermark), and changes the folder id, the postings ref and the shard
    /// generation count. The retry re-reads HEAD, reads no section again (the
    /// eight ranged GETs are batch 1's, by the report and by the store's
    /// record), and publishes the winner's HEAD byte for byte plus part 1's
    /// ref. The leaf built for the replaced part is stored, binds the old part,
    /// and no HEAD names it. Part 3 is gone, so batch 2 costs no GET.
    ///
    /// A rebuild that kept every built ref attaches a leaf binding another
    /// part's blake3 to the replacement; one that re-read sections on the
    /// retry issues more than eight ranged GETs; one that took the winner's
    /// parts but kept its own watermark, postings and generation count
    /// publishes a HEAD that is not the winner's outside `key_index`.
    #[tokio::test]
    async fn rebuild_lost_cas_publishes_the_winner_plus_refs_for_surviving_batch_parts() {
        let store = recording_store();
        let (catalog, part_keys) = two_entry_parts(&store, 3).await;
        let original = read_signal_head(store.as_ref(), Signal::Logs).await;
        let mut winner = original.clone();
        winner.parts[1].blake3 = vec![0xee; 32];
        winner.parts.truncate(2);
        let mut added = original.parts[2].clone();
        added.key = format!("{}.winner", added.key);
        added.blake3 = vec![0xdd; 32];
        added.min_hour = KIDX_HOUR + 5;
        added.watermark_hour = KIDX_HOUR + 5;
        winner.parts.push(added);
        winner.watermark_hour = KIDX_HOUR + 5;
        winner.folder_id = vec![0xab; 16];
        winner.created_unix_ns = original.created_unix_ns + 1;
        winner.shard_generation_count = original.shard_generation_count + 1;
        winner.postings = Some(SnapshotPostingsRef {
            key: "winner.postings".to_string(),
            blake3: vec![0xcc; 32],
            size: 7,
            name_count: 3,
            part_blake3: winner.parts.iter().map(|p| p.blake3.clone()).collect(),
        });
        assert_ne!(winner.watermark_hour, original.watermark_hour);
        assert_ne!(winner.postings, original.postings);
        assert_ne!(winner.folder_id, original.folder_id);
        *store.head_race.lock().expect("lock") = Some(Bytes::from(
            snapshot_format::encode_head(&winner).expect("encode"),
        ));

        store.clear();
        let report = catalog
            .rebuild_key_index(
                &tenant(),
                Signal::Logs,
                &KeyIndexRebuildOptions {
                    field: None,
                    batch_entries: 4,
                },
            )
            .await
            .expect("rebuild");
        assert_eq!(
            report,
            KeyIndexRebuildReport {
                largest_unindexed_part_entries: 2,
                fields: vec![USER_ID.to_string()],
                parts_unindexed: 3,
                parts_rebuilt: 1,
                parts_deferred: 0,
                leaves_written: 2,
                leaf_bytes_written: report.leaf_bytes_written,
                section_bytes_read: report.section_bytes_read,
                ranged_gets: 8,
                uncovered_entries: 0,
                attempt_count: 1,
                attempts: vec![KeyIndexRebuildAttempt {
                    parts: 2,
                    part_gets: 2,
                    ranged_gets: 8,
                    leaf_puts: 2,
                    head_cas_tries: 2,
                    parts_published: 1,
                }],
            }
        );
        let read = store.data_ranged_gets();
        assert_eq!(read.len(), 8, "no section re-read: {read:?}");
        let read_keys: BTreeSet<String> = read.iter().map(|(key, _, _)| key.clone()).collect();
        let batch_one: BTreeSet<String> = part_keys[0].union(&part_keys[1]).cloned().collect();
        assert_eq!(read_keys, batch_one, "batch 1's objects, each once");
        assert_eq!(
            report.section_bytes_read,
            read.iter().map(|(_, _, len)| len).sum::<u64>()
        );

        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts[0].key_index.len(), 1);
        assert_eq!(
            head.parts[0].key_index[0].part_blake3,
            original.parts[0].blake3
        );
        let mut expected = winner.clone();
        expected.parts[0].key_index = head.parts[0].key_index.clone();
        let published = store
            .get(&head_key(Signal::Logs), GetRange::Full)
            .await
            .expect("head")
            .data;
        assert_eq!(
            published.as_ref(),
            snapshot_format::encode_head(&expected)
                .expect("encode")
                .as_slice(),
            "the winner's HEAD byte for byte plus part 1's ref"
        );
        let referenced = &head.parts[0].key_index[0].key;
        let orphans: Vec<String> = kidx_objects(store.as_ref(), Signal::Logs)
            .await
            .into_iter()
            .filter(|key| key != referenced)
            .collect();
        assert_eq!(orphans.len(), 1, "{orphans:?}");
        let got = store
            .get(&orphans[0], GetRange::Full)
            .await
            .expect("the unreferenced leaf is stored");
        let summary =
            verify_key_index_leaf(&got.data, &KeyIndexLeafLimits::default()).expect("valid leaf");
        assert_eq!(summary.header.part_blake3, original.parts[1].blake3);
    }

    /// One batch of two parts. Between the leaf build and the HEAD CAS a
    /// racing rebuild publishes first and gives part 1 its `user_id` ref (the
    /// leaf this run built too, since leaves are content-addressed). The
    /// retry attaches part 2's ref alone: part 1 keeps exactly the one ref the
    /// winner gave it, and both parts probe. A retry that did not skip a field
    /// the part already carries gives part 1 a second `user_id` ref for the
    /// same slice and counts it as published.
    #[tokio::test]
    async fn rebuild_lost_cas_to_a_racing_rebuild_adds_no_second_ref_for_a_field() {
        let store = recording_store();
        let (catalog, _) = two_entry_parts(&store, 2).await;
        let key = head_key(Signal::Logs);
        let stripped = store.get(&key, GetRange::Full).await.expect("head").data;
        rebuild(&catalog, Signal::Logs).await;
        let indexed = read_signal_head(store.as_ref(), Signal::Logs).await;
        let mut winner = indexed.clone();
        winner.parts[1].key_index.clear();
        let current = store.get(&key, GetRange::Full).await.expect("head");
        store
            .put(
                &key,
                stripped,
                PutOptions {
                    mode: PutMode::CasVersion(current.version),
                    checksum: None,
                },
            )
            .await
            .expect("restore the stripped HEAD");
        *store.head_race.lock().expect("lock") = Some(Bytes::from(
            snapshot_format::encode_head(&winner).expect("encode"),
        ));

        store.clear();
        let report = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(
            (report.parts_rebuilt, report.leaves_written),
            (1, 2),
            "{report:?}"
        );
        assert_eq!(report.attempts.len(), 1, "{report:?}");
        assert_eq!(report.attempts[0].head_cas_tries, 2, "{report:?}");
        assert_eq!(report.attempts[0].parts_published, 1, "{report:?}");
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        let refs: Vec<usize> = head.parts.iter().map(|p| p.key_index.len()).collect();
        assert_eq!(refs, vec![1, 1], "one user_id ref a part: {head:?}");
        assert_eq!(head, indexed);
        for part in &head.parts {
            let ordinal = part_ordinals(store.as_ref(), part).await;
            let seqs: Vec<u64> = ordinal.keys().copied().collect();
            assert_eq!(seqs.len(), 2);
            let probe = probe_part(
                &catalog,
                Signal::Logs,
                part,
                KeyType::I64,
                USER_ID,
                key8_i64(10),
            )
            .await;
            assert_eq!(
                probe,
                matches(&[(ordinal[&seqs[0]], &[0]), (ordinal[&seqs[1]], &[0])], &[])
            );
        }
    }

    /// A logs tenant declaring `user_id` whose objects carry no KEY_IDX
    /// section: the part is read once, each entry costs its footer suffix GET
    /// alone, no leaf is built, and no HEAD CAS is issued (no PUT at all).
    /// Nothing marks the part as tried, so a rerun reads it again at the same
    /// cost. A rebuild that CASed HEAD for a batch that built no leaf reports
    /// a CAS try and PUTs HEAD.
    #[tokio::test]
    async fn a_part_with_no_section_for_the_field_publishes_nothing_and_is_read_again() {
        let store = recording_store();
        declare_user_id_key_index(store.as_ref()).await;
        for seq in 1..=2u64 {
            publish_user_id_logs(store.as_ref(), seq, KIDX_HOUR, &[10, 20], 2, 0, false).await;
        }
        let catalog = off_catalog(store.clone(), DEFAULT_SNAPSHOT_PART_MAX_ENTRIES);
        fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
        let before = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(before.parts.len(), 1);
        let part_key = before.parts[0].key.clone();
        for run in ["first run", "rerun"] {
            store.clear();
            let report = rebuild(&catalog, Signal::Logs).await;
            assert_eq!(
                report,
                KeyIndexRebuildReport {
                    largest_unindexed_part_entries: 2,
                    fields: vec![USER_ID.to_string()],
                    parts_unindexed: 1,
                    parts_rebuilt: 0,
                    parts_deferred: 0,
                    leaves_written: 0,
                    leaf_bytes_written: 0,
                    section_bytes_read: report.section_bytes_read,
                    ranged_gets: 2,
                    uncovered_entries: 2,
                    attempt_count: 1,
                    attempts: vec![KeyIndexRebuildAttempt {
                        parts: 1,
                        part_gets: 1,
                        ranged_gets: 2,
                        leaf_puts: 0,
                        head_cas_tries: 0,
                        parts_published: 0,
                    }],
                },
                "{run}"
            );
            let part_gets = store
                .gets()
                .into_iter()
                .filter(|(key, range, _)| key == &part_key && matches!(range, GetRange::Full))
                .count();
            assert_eq!(part_gets, 1, "{run}: the part is read");
            assert_eq!(store.data_ranged_gets().len(), 2, "{run}");
            assert!(
                store.puts().is_empty(),
                "{run}: no leaf PUT and no HEAD PUT: {:?}",
                store.puts()
            );
            assert_eq!(
                read_signal_head(store.as_ref(), Signal::Logs).await,
                before,
                "{run}"
            );
        }
    }

    /// A part GET that fails, and a part whose bytes do not match the
    /// `blake3` HEAD names, each stop the run with the typed `Interrupted`
    /// error once the fault has fired, and the batch publishes nothing: no
    /// HEAD PUT, HEAD as it was. Fixture: one batch of two parts, the fault
    /// on the second part's object.
    #[tokio::test]
    async fn a_failed_or_mismatched_part_get_interrupts_and_publishes_nothing() {
        for (fault, kind) in [
            (ScriptedFault::Timeout, FaultKind::Timeout),
            (ScriptedFault::CorruptRange, FaultKind::CorruptRange),
        ] {
            let inner = recording_store();
            two_entry_parts(&inner, 2).await;
            let before = read_signal_head(inner.as_ref(), Signal::Logs).await;
            let faulty = Arc::new(FaultStore::new(
                Arc::clone(&inner),
                FaultPlan::empty().with_rule(
                    Rule::new(Op::Get, fault).with_key_contains(before.parts[1].key.clone()),
                ),
            ));
            let catalog = off_catalog(faulty.clone(), 2);
            inner.clear();
            let err = catalog
                .rebuild_key_index(&tenant(), Signal::Logs, &KeyIndexRebuildOptions::default())
                .await
                .expect_err("part 2 does not load");
            assert_eq!(faulty.fault_count(Op::Get, kind), 1, "{kind:?}");
            let KeyIndexRebuildError::Interrupted { source, report } = &err else {
                panic!("{kind:?}: {err:?}");
            };
            if kind == FaultKind::Timeout {
                assert!(matches!(source, CatalogError::Store(_)), "{source:?}");
            } else {
                assert!(
                    matches!(
                        source,
                        CatalogError::FieldMismatch {
                            field: "blake3",
                            ..
                        }
                    ),
                    "{source:?}"
                );
            }
            assert_eq!(
                (report.parts_rebuilt, report.attempt_count),
                (0, 0),
                "{kind:?}: {report:?}"
            );
            assert!(
                !inner.puts().iter().any(|key| key.ends_with("/HEAD")),
                "{kind:?}: {:?}",
                inner.puts()
            );
            assert_eq!(
                read_signal_head(inner.as_ref(), Signal::Logs).await,
                before,
                "{kind:?}"
            );
        }
    }

    /// For a declared field the rebuild's leaf is the fold's leaf: the same
    /// data folded once with `build_key_index_leaves` on, and once with it
    /// off and then rebuilt, gives the same part, the same `key_index` ref
    /// and byte-identical leaf objects.
    #[tokio::test]
    async fn a_rebuilt_leaf_is_byte_identical_to_the_folds_leaf_for_a_declared_field() {
        async fn publish(store: &RecordingStore) {
            declare_user_id_key_index(store).await;
            for (seq, user_ids) in [
                (1, &[10i64, 10, 20, 30, 40, 40][..]),
                (2, &[50, 10, 60, 10]),
                (3, &[70, 80]),
            ] {
                publish_user_id_logs(store, seq, KIDX_HOUR, user_ids, 2, 0, true).await;
            }
        }
        let folded = recording_store();
        publish(&folded).await;
        let on = Catalog::new(
            folded.clone(),
            CatalogConfig {
                build_key_index_leaves: true,
                ..config(1)
            },
        )
        .expect("catalog");
        fold_through(&on, Signal::Logs, KIDX_HOUR).await;

        let rebuilt = recording_store();
        publish(&rebuilt).await;
        let off = off_catalog(rebuilt.clone(), DEFAULT_SNAPSHOT_PART_MAX_ENTRIES);
        fold_through(&off, Signal::Logs, KIDX_HOUR).await;
        rebuild(&off, Signal::Logs).await;

        let fold_head = read_signal_head(folded.as_ref(), Signal::Logs).await;
        let rebuild_head = read_signal_head(rebuilt.as_ref(), Signal::Logs).await;
        assert_eq!(fold_head.parts.len(), 1);
        assert_eq!(fold_head.parts[0].blake3, rebuild_head.parts[0].blake3);
        let fold_refs: Vec<SnapshotKeyIndexLeafRef> = fold_head.parts[0]
            .key_index
            .iter()
            .filter(|r| r.field == USER_ID)
            .cloned()
            .collect();
        assert_eq!(fold_refs.len(), 1);
        assert_eq!(fold_refs, rebuild_head.parts[0].key_index);
        for leaf in &fold_refs {
            let a = folded
                .get(&leaf.key, GetRange::Full)
                .await
                .expect("fold leaf");
            let b = rebuilt
                .get(&leaf.key, GetRange::Full)
                .await
                .expect("rebuilt leaf");
            assert_eq!(a.data, b.data, "{}", leaf.key);
        }
    }

    /// A ranged GET that fails with a store error for one entry defers its
    /// part: no leaf is built for it and it gains no ref this run, the report
    /// counts it in `parts_deferred`, and the other part of the batch
    /// publishes. The failed GET is a ranged GET the report counts and the
    /// store never served. A rerun with the fault cleared reads the deferred
    /// part again and attaches it, every entry covered.
    ///
    /// A rebuild that built the deferred part's leaf with the failed entry
    /// uncovered attaches a ref to it on the first run, which no rerun can
    /// then repair; one that dropped the whole batch publishes neither part.
    #[tokio::test]
    async fn a_failed_section_get_defers_the_part_and_a_rerun_attaches_it() {
        let inner = recording_store();
        let (_, part_keys) = two_entry_parts(&inner, 2).await;
        let failing = part_keys[1].iter().next().expect("a data key").clone();
        let faulty = Arc::new(FaultStore::new(
            Arc::clone(&inner),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Get, ScriptedFault::Timeout).with_key_contains(failing.clone()),
            ),
        ));
        let catalog = off_catalog(faulty.clone(), 2);
        inner.clear();
        let report = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(faulty.fault_count(Op::Get, FaultKind::Timeout), 1);
        assert_eq!(
            report,
            KeyIndexRebuildReport {
                largest_unindexed_part_entries: 2,
                fields: vec![USER_ID.to_string()],
                parts_unindexed: 2,
                parts_rebuilt: 1,
                parts_deferred: 1,
                leaves_written: 1,
                leaf_bytes_written: report.leaf_bytes_written,
                section_bytes_read: report.section_bytes_read,
                ranged_gets: 7,
                uncovered_entries: 0,
                attempt_count: 1,
                attempts: vec![KeyIndexRebuildAttempt {
                    parts: 2,
                    part_gets: 2,
                    ranged_gets: 7,
                    leaf_puts: 1,
                    head_cas_tries: 1,
                    parts_published: 1,
                }],
            }
        );
        assert_eq!(inner.data_ranged_gets().len(), 6);
        let head = read_signal_head(inner.as_ref(), Signal::Logs).await;
        let refs: Vec<usize> = head.parts.iter().map(|p| p.key_index.len()).collect();
        assert_eq!(refs, vec![1, 0], "part 2 is deferred");

        let catalog = off_catalog(inner.clone(), 2);
        inner.clear();
        let rerun = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(
            (
                rerun.parts_unindexed,
                rerun.parts_rebuilt,
                rerun.parts_deferred,
                rerun.leaves_written,
                rerun.uncovered_entries
            ),
            (1, 1, 0, 1, 0),
            "{rerun:?}"
        );
        let read_keys: BTreeSet<String> = inner
            .data_ranged_gets()
            .into_iter()
            .map(|(key, _, _)| key)
            .collect();
        assert_eq!(read_keys, part_keys[1]);
        let head = read_signal_head(inner.as_ref(), Signal::Logs).await;
        let refs: Vec<usize> = head.parts.iter().map(|p| p.key_index.len()).collect();
        assert_eq!(refs, vec![1, 1]);
        let part = &head.parts[1];
        let ordinal = part_ordinals(inner.as_ref(), part).await;
        assert_eq!(
            probe_part(
                &catalog,
                Signal::Logs,
                part,
                KeyType::I64,
                USER_ID,
                key8_i64(10)
            )
            .await,
            matches(&[(ordinal[&3], &[0]), (ordinal[&4], &[0])], &[])
        );
    }

    /// A logs tenant that declares no key-index field is refused before HEAD
    /// is read, with a typed error naming the tenant: with no tenant config
    /// (the config GET is the only request) and with a config that declares
    /// none. HEAD exists in both cases, and nothing is written.
    #[tokio::test]
    async fn a_logs_tenant_declaring_no_key_index_field_is_refused() {
        use crate::tenant_config::{TenantConfig, TenantLifecycleState};
        let store = recording_store();
        publish_user_id_logs(store.as_ref(), 1, KIDX_HOUR, &[10, 20], 2, 0, false).await;
        let catalog = off_catalog(store.clone(), DEFAULT_SNAPSHOT_PART_MAX_ENTRIES);
        fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
        for case in ["no tenant config", "a config declaring none"] {
            if case == "a config declaring none" {
                crate::tenant_config::set_tenant_config(
                    store.as_ref(),
                    &tenant(),
                    &TenantConfig::new(TenantLifecycleState::Active),
                    1,
                )
                .await
                .expect("write config");
            }
            store.clear();
            let err = catalog
                .rebuild_key_index(&tenant(), Signal::Logs, &KeyIndexRebuildOptions::default())
                .await
                .expect_err(case);
            assert!(
                matches!(
                    &err,
                    KeyIndexRebuildError::NoFieldDeclared { tenant: named, signal: Signal::Logs }
                        if *named == tenant().to_hex()
                ),
                "{case}: {err:?}"
            );
            let message = err.to_string();
            assert!(message.contains(&tenant().to_hex()), "{message}");
            assert!(message.contains("declares no key-index field"), "{message}");
            assert_eq!(err.report(), None);
            let gets: Vec<String> = store.gets().into_iter().map(|(key, _, _)| key).collect();
            assert_eq!(
                gets,
                vec![crate::tenant_config::config_key(&tenant())],
                "{case}"
            );
            assert!(store.puts().is_empty(), "{case}: {:?}", store.puts());
        }
    }

    /// The rebuild's ranged GET count is exactly two per entry rebuilt, and
    /// the bytes it reports read are exactly the footer suffixes plus the
    /// KEY_IDX sections, by the report and by the store's own record. Fixture:
    /// three 64-row logs objects, 8 rows to a block, 512 padding characters a
    /// row, one part: objects of 22,326, 22,343 and 22,326 bytes, sections of
    /// 2,912 each, 3 x 8,192 + 3 x 2,912 = 33,312 bytes read.
    #[tokio::test]
    async fn rebuild_ranged_gets_equal_twice_the_entries_rebuilt() {
        let store = recording_store();
        declare_user_id_key_index(store.as_ref()).await;
        let mut expected = Vec::new();
        for seq in 1..=3u64 {
            let user_ids: Vec<i64> = (0..64).map(|i| (seq as i64) * 1_000 + i).collect();
            let (record, bytes) =
                publish_user_id_logs(store.as_ref(), seq, KIDX_HOUR, &user_ids, 8, 512, true).await;
            let footer = ravel_logseg::footer::open(&bytes).expect("footer");
            let section = *footer
                .section(ravel_logseg::footer::kind::KEY_IDX)
                .expect("KEY_IDX section");
            let size = bytes.len() as u64;
            let suffix = size.min(KEY_INDEX_FOOTER_SUFFIX_LEN);
            expected.push((data_key(&record), GetRange::Suffix(suffix), suffix));
            expected.push((
                data_key(&record),
                GetRange::Range(section.offset, section.offset + section.len),
                section.len,
            ));
        }
        let catalog = off_catalog(store.clone(), DEFAULT_SNAPSHOT_PART_MAX_ENTRIES);
        fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
        store.clear();
        let report = rebuild(&catalog, Signal::Logs).await;

        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts[0].entry_count, 3);
        assert_eq!(
            report.ranged_gets,
            2 * head.parts[0].entry_count,
            "{report:?}"
        );
        assert_eq!(report.attempts[0].ranged_gets, report.ranged_gets);
        let mut seen = store.data_ranged_gets();
        seen.sort_by_key(|(key, range, len)| (key.clone(), format!("{range:?}"), *len));
        expected.sort_by_key(|(key, range, len)| (key.clone(), format!("{range:?}"), *len));
        assert_eq!(seen, expected);
        let expected_bytes: u64 = expected.iter().map(|(_, _, len)| len).sum();
        assert_eq!(expected_bytes, 33_312);
        assert_eq!(report.section_bytes_read, expected_bytes, "{report:?}");
    }

    /// An undeclared `--field` is refused, naming it and the declared set,
    /// before HEAD is read: the tenant config GET is the only request. A
    /// rerun on a HEAD whose every part is indexed issues the config and HEAD
    /// GETs only: no part or data object GET, no PUT, no CAS.
    #[tokio::test]
    async fn undeclared_field_is_refused_and_an_indexed_head_costs_no_get() {
        let store = recording_store();
        let (catalog, _) = two_entry_parts(&store, 2).await;
        store.clear();
        let err = catalog
            .rebuild_key_index(
                &tenant(),
                Signal::Logs,
                &KeyIndexRebuildOptions {
                    field: Some("order_id".to_string()),
                    ..KeyIndexRebuildOptions::default()
                },
            )
            .await
            .expect_err("undeclared field");
        match &err {
            KeyIndexRebuildError::FieldNotDeclared {
                field,
                signal,
                declared,
            } => {
                assert_eq!(field, "order_id");
                assert_eq!(*signal, Signal::Logs);
                assert_eq!(declared, &vec![USER_ID.to_string()]);
            }
            other => panic!("{other:?}"),
        }
        assert!(err.to_string().contains("\"order_id\""), "{err}");
        assert_eq!(err.report(), None);
        assert_eq!(store.gets().len(), 1, "{:?}", store.gets());
        assert!(store.puts().is_empty());

        let err = catalog
            .rebuild_key_index(
                &tenant(),
                Signal::Spans,
                &KeyIndexRebuildOptions {
                    field: Some(USER_ID.to_string()),
                    ..KeyIndexRebuildOptions::default()
                },
            )
            .await
            .expect_err("user_id is not a spans field");
        assert!(
            matches!(&err, KeyIndexRebuildError::FieldNotDeclared { declared, .. }
                if declared == &vec![ravel_rspan::TRACE_ID_FIELD.to_string()]),
            "{err:?}"
        );
        let err = catalog
            .rebuild_key_index(
                &tenant(),
                Signal::Metrics,
                &KeyIndexRebuildOptions::default(),
            )
            .await
            .expect_err("metrics objects carry no KEY_IDX");
        assert!(
            matches!(
                err,
                KeyIndexRebuildError::UnsupportedSignal(Signal::Metrics)
            ),
            "{err:?}"
        );

        let first = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(first.parts_rebuilt, 2, "{first:?}");
        let indexed = read_signal_head(store.as_ref(), Signal::Logs).await;
        store.clear();
        let rerun = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(
            rerun,
            KeyIndexRebuildReport {
                fields: vec![USER_ID.to_string()],
                ..KeyIndexRebuildReport::default()
            }
        );
        let whole: Vec<String> = store.gets().into_iter().map(|(key, _, _)| key).collect();
        assert_eq!(
            whole,
            vec![
                crate::tenant_config::config_key(&tenant()),
                head_key(Signal::Logs)
            ]
        );
        assert!(store.puts().is_empty(), "{:?}", store.puts());
        assert_eq!(
            read_signal_head(store.as_ref(), Signal::Logs).await,
            indexed
        );
    }

    /// A part holding a ref for one declared field keeps it unchanged and
    /// gains only the missing one: with `user_id` and `trace_id` declared and
    /// the part already naming a `trace_id` leaf, the rebuild builds and
    /// attaches `user_id` alone. `--field user_id` on a stripped part builds
    /// `user_id` only.
    #[tokio::test]
    async fn rebuild_keeps_existing_refs_and_adds_only_the_missing_fields() {
        use crate::tenant_config::StorageLayoutWrite;
        let store = recording_store();
        let (catalog, _) = two_entry_parts(&store, 1).await;
        let mut cfg = crate::tenant_config::read_config_values(store.as_ref(), &tenant())
            .await
            .expect("read config")
            .expect("config present");
        cfg.set_index_trace_id(true, StorageLayoutWrite::ReadersRolledOut)
            .expect("index trace_id");
        crate::tenant_config::set_tenant_config(store.as_ref(), &tenant(), &cfg, 2)
            .await
            .expect("write config");

        let options = KeyIndexRebuildOptions {
            field: Some(USER_ID.to_string()),
            ..KeyIndexRebuildOptions::default()
        };
        let report = catalog
            .rebuild_key_index(&tenant(), Signal::Logs, &options)
            .await
            .expect("rebuild user_id");
        assert_eq!(report.fields, vec![USER_ID.to_string()]);
        assert_eq!((report.leaves_written, report.uncovered_entries), (1, 0));
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        let user_id_ref = head.parts[0].key_index[0].clone();
        assert_eq!(head.parts[0].key_index.len(), 1);
        assert_eq!(user_id_ref.field, USER_ID);

        // Swap the user_id ref for a stand-in trace_id ref.
        let trace_ref = SnapshotKeyIndexLeafRef {
            field: ravel_logseg::key_index::TRACE_ID_FIELD.to_string(),
            key_type: u32::from(KeyType::Id16.as_u8()),
            ..user_id_ref.clone()
        };
        let mut swapped = head.clone();
        swapped.parts[0].key_index = vec![trace_ref.clone()];
        let current = store
            .get(&head_key(Signal::Logs), GetRange::Full)
            .await
            .expect("head");
        store
            .put(
                &head_key(Signal::Logs),
                Bytes::from(snapshot_format::encode_head(&swapped).expect("encode")),
                PutOptions {
                    mode: PutMode::CasVersion(current.version),
                    checksum: None,
                },
            )
            .await
            .expect("swap refs");

        store.clear();
        let report = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(
            report.fields,
            vec![
                USER_ID.to_string(),
                ravel_logseg::key_index::TRACE_ID_FIELD.to_string()
            ]
        );
        assert_eq!(
            (
                report.parts_rebuilt,
                report.leaves_written,
                report.uncovered_entries
            ),
            (1, 1, 0),
            "user_id alone is built: {report:?}"
        );
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts[0].key_index, vec![trace_ref, user_id_ref]);
    }
}
