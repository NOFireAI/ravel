//! ADR-2707 decision 4, "Lifecycle": the explicit repair that attaches
//! per-part key-index leaves to parts already in HEAD, without re-encoding
//! them (`ravel-cli catalog fold --rebuild-key-index`, docs/catalog-and-mvcc.md
//! "Rollout").
//!
//! The leaves are built by the fold's own leaf build from the same ranged
//! KEY_IDX reads, so a rebuilt leaf is byte-identical to the one a fold with
//! `build_key_index_leaves` on would have written for the same part.

use std::collections::HashMap;

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
    /// Parts in HEAD lacking a ref for at least one of `fields`.
    pub parts_unindexed: u64,
    /// Parts a published HEAD attached at least one ref to.
    pub parts_rebuilt: u64,
    /// Leaves PUT, including any a lost CAS left unreferenced.
    pub leaves_written: u64,
    /// Total bytes of those leaves.
    pub leaf_bytes_written: u64,
    /// Bytes the ranged GETs returned: footer suffixes, any extra footer
    /// ranges, and KEY_IDX sections, as transferred, retries excluded.
    pub section_bytes_read: u64,
    /// Ranged GETs of data objects, footer suffix and section together.
    pub ranged_gets: u64,
    /// For each part built and each field built for it, the part entries no
    /// leaf covers, summed.
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
    /// whatever [`crate::CatalogConfig::build_key_index_leaves`] says.
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
    /// keeps them and gains the rest.
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
        let mut report = KeyIndexRebuildReport {
            fields: requested.iter().map(|(name, _)| name.clone()).collect(),
            ..KeyIndexRebuildReport::default()
        };
        if requested.is_empty() {
            return Ok(report);
        }

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
            for (entry, key) in entries.iter().zip(&data_keys) {
                if let Ok(key) = key
                    && !reads.contains_key(key)
                    && !unread.iter().any(|(queued, _)| queued == key)
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
                if read.get_failed
                    && let Err(reason) = &read.fields
                {
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        object_key = %key,
                        reason = %reason,
                        "KEY_IDX read GET failed; the entry is uncovered by the rebuilt key-index leaves"
                    );
                }
                reads.insert(key, read.fields);
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
        FaultKind, FaultPlan, FaultStore, Op, ScriptedFault, Sequence,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta,
        ObjectStoreBackend, PageToken, PutOutcome,
    };
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
    /// HEAD then names with refs. A rerun reads no section and no part object
    /// of parts 1 and 2, issues exactly the config GET, the HEAD GET, part 3's
    /// GET and its four ranged GETs, and completes part 3.
    ///
    /// A rebuild that published every batch in one CAS at the end leaves no
    /// ref on HEAD here; one that did not skip indexed parts re-reads parts 1
    /// and 2 on the rerun.
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

    /// One batch of two parts. Between the leaf build and the HEAD CAS another
    /// writer publishes a HEAD in which the second part is replaced (a
    /// different `part_blake3`). The retry re-reads HEAD, reads no section
    /// again, and attaches a ref only to the part that survived; the leaf
    /// built for the replaced part is stored, binds the old part, and no HEAD
    /// names it. A rebuild that kept every built ref would attach a leaf
    /// binding another part's blake3 to the replacement.
    #[tokio::test]
    async fn rebuild_lost_cas_keeps_only_refs_whose_part_survived() {
        let store = recording_store();
        let (catalog, _) = two_entry_parts(&store, 2).await;
        let original = read_signal_head(store.as_ref(), Signal::Logs).await;
        let mut raced = original.clone();
        raced.parts[1].blake3 = vec![0xee; 32];
        *store.head_race.lock().expect("lock") = Some(Bytes::from(
            snapshot_format::encode_head(&raced).expect("encode"),
        ));

        store.clear();
        let report = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(report.leaves_written, 2, "{report:?}");
        assert_eq!(report.parts_rebuilt, 1, "{report:?}");
        assert_eq!(report.attempts.len(), 1, "{report:?}");
        assert_eq!(report.attempts[0].head_cas_tries, 2, "{report:?}");
        assert_eq!(report.attempts[0].parts_published, 1, "{report:?}");
        assert_eq!(report.ranged_gets, 8, "{report:?}");
        assert_eq!(store.data_ranged_gets().len(), 8, "no section re-read");

        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts[0].key_index.len(), 1);
        assert_eq!(
            head.parts[0].key_index[0].part_blake3,
            original.parts[0].blake3
        );
        assert!(
            head.parts[1].key_index.is_empty(),
            "the replaced part gains nothing: {:?}",
            head.parts[1].key_index
        );
        assert_eq!(head.parts[1].blake3, vec![0xee; 32]);
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
