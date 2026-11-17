//! ADR-2707 decision 4, "Lifecycle": the explicit repair that attaches
//! per-part key-index leaves to parts already in HEAD, without re-encoding
//! them (`ravel-cli catalog fold --rebuild-key-index`, docs/catalog-and-mvcc.md
//! "Rollout").
//!
//! The leaves are built by the fold's own incremental build
//! (`build_part_key_index`) from the same ranged KEY_IDX reads, in the same
//! passes under `key_index_build_max_pairs`, with no previous leaf to carry.
//! Each field holds at most `max(share, R) + 1` pairs at a time, where
//! `share = max(1, key_index_build_max_pairs / fields)` and `R` is the
//! field's largest key8 run, so with no run over the share the part holds at
//! most `key_index_build_max_pairs + fields` pairs, whatever its size. The
//! repair keeps each entry's KEY_IDX section from its first read for the
//! part's later passes, in part order while the kept bytes stay within
//! `CatalogConfig::key_index_repair_hold_cap` and the memory budget grants
//! them; each later pass reads every other entry's section again (issue
//! #4376). The share divides the budget by
//! the fields built, so a rebuilt leaf is byte-identical to the one a fold
//! with `build_key_index_leaves` on writes for the same part at the same
//! budget only when the rebuild builds the same fields the fold built. It
//! does not when a KEY_IDX section names a field the tenant does not declare
//! (the fold indexes it, the rebuild does not), and it does not when the
//! rebuild builds fewer fields than the tenant declares: `options.field`, or
//! a part that already holds refs for some declared fields and gains only the
//! rest. A larger share can then end a pass at another key8, and every pass
//! end is a slice end.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use ravel_codec::key_idx::KeyType;
use ravel_object_store::{GetRange, PutMode, PutOptions, StoreError, UploadChecksum, Version};
use ravel_proto::catalog::v1::{SnapshotHead, SnapshotKeyIndexLeafRef, SnapshotPartRef};
use ravel_types::{Signal, TenantHash};

use crate::catalog::Catalog;
use crate::config::DEFAULT_SNAPSHOT_PART_MAX_ENTRIES;
use crate::error::CatalogError;
use crate::fold::{
    HeadState, HeldLabelDicts, KeyIndexBuildCaller, KeyIndexBuildLimits, KeyIndexSectionHold,
    MAX_HEAD_CAS_ATTEMPTS, RequestCounters, build_part_key_index, declared_key_index_fields,
    head_object_key, key_index_leaf_limits, metrics_label_index_leaf_bytes_cap, newer_leaf_refusal,
};
use crate::snapshot_format::{
    self, key_index_leaf_format_version, key_index_leaf_prefix_range,
    key_index_leaf_version_accepted, key_index_leaf_version_is_newer,
    key_index_leaf_versions_for_signal,
};
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

/// One batch's publish, its HEAD CAS retries included. A batch that stopped
/// the run before its publish step is listed last, after the
/// `attempt_count` batches that reached it, so the attempts' GETs and PUTs
/// sum to the report's totals. It is the trailing attempt when
/// `attempts.len()` exceeds `attempt_count`; `head_cas_tries` 0 does not mark
/// it, since a batch that reached publish with every part deferred also
/// writes no HEAD.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct KeyIndexRebuildAttempt {
    /// Parts the batch read.
    pub parts: u64,
    /// Whole-object GETs of those parts.
    pub part_gets: u64,
    /// Ranged GETs of data objects (footer suffix, any extra footer range,
    /// KEY_IDX section) the batch issued: `footer_phase_ranged_gets` plus
    /// `section_phase_ranged_gets`.
    pub ranged_gets: u64,
    /// Of `ranged_gets`, the footer suffix and extra footer range GETs.
    pub footer_phase_ranged_gets: u64,
    /// Of `ranged_gets`, the section GETs.
    pub section_phase_ranged_gets: u64,
    /// Build passes over the batch's parts, summed
    /// ([`KeyIndexRebuildReport::passes`]).
    pub passes: u64,
    /// KEY_IDX section reads the batch issued, one per (entry, pass).
    pub sections_read: u64,
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
    /// Non-empty parts in HEAD lacking a ref for at least one of `fields`,
    /// or holding one whose leaf `format_version` the signal's reader
    /// refuses (a logs version-1 leaf, which the rebuild replaces).
    pub parts_unindexed: u64,
    /// Of `parts_unindexed`, the parts counted for a refused leaf version.
    pub parts_with_refused_leaves: u64,
    /// Ranged GETs of an existing leaf's `[0, prefix_len)`, one per ref of a
    /// requested field on a signal whose reader refuses version 1 (logs),
    /// issued before any batch to read each leaf's `format_version`.
    pub leaf_prefix_gets: u64,
    /// Bytes those prefix GETs returned, as transferred.
    pub leaf_prefix_bytes_read: u64,
    /// Parts a published HEAD attached at least one ref to.
    pub parts_rebuilt: u64,
    /// Parts given no ref this run because a ranged GET for one of their
    /// entries, or for one of their leaves' prefixes, failed with a store
    /// error; a rerun reads them again.
    pub parts_deferred: u64,
    /// Leaf PUTs issued, including any a lost CAS, a deferred part or a field
    /// over the byte cap left unreferenced. Counted when issued by the fold's
    /// build, so a run interrupted by a failed leaf PUT counts that PUT too.
    /// A different basis from the fold's
    /// [`FoldReport::key_index_leaves_written`](crate::FoldReport::key_index_leaves_written),
    /// which counts the leaves the published HEAD names.
    pub leaves_written: u64,
    /// Total bytes of the leaves those PUTs sent, the failed PUT's included.
    pub leaf_bytes_written: u64,
    /// Bytes the ranged GETs returned: footer suffixes, any extra footer
    /// ranges, and KEY_IDX sections, as transferred, retries excluded. The
    /// sum of `footer_phase_bytes_read` and `section_phase_bytes_read`.
    pub section_bytes_read: u64,
    /// Ranged GETs of data objects: footer suffixes, any extra footer
    /// ranges, and KEY_IDX sections together. The sum of
    /// `footer_phase_ranged_gets` and `section_phase_ranged_gets`.
    pub ranged_gets: u64,
    /// Footer phase of `ranged_gets`: each read's footer suffix GET and, when
    /// the footer is longer than the suffix, its extra footer range GET.
    pub footer_phase_ranged_gets: u64,
    /// Bytes the footer phase GETs returned, as transferred, retries
    /// excluded.
    pub footer_phase_bytes_read: u64,
    /// Section phase of `ranged_gets`: the GETs of the KEY_IDX section (the
    /// LABEL_DICT section on metrics) the footer names, one per section read
    /// that got that far.
    pub section_phase_ranged_gets: u64,
    /// Bytes the section phase GETs returned, as transferred, retries
    /// excluded.
    pub section_phase_bytes_read: u64,
    /// The pair budget every part was built under:
    /// [`CatalogConfig::key_index_build_max_pairs`](crate::CatalogConfig::key_index_build_max_pairs),
    /// at least 1. Set once the requested fields are known, before HEAD is
    /// read.
    pub key_index_build_max_pairs: u64,
    /// The most KEY_IDX section bytes the run kept for one part across the
    /// part's passes:
    /// [`CatalogConfig::key_index_repair_hold_cap`](crate::CatalogConfig::key_index_repair_hold_cap).
    /// 0 keeps none, so every pass reads every section by range. Set with
    /// `key_index_build_max_pairs`.
    pub section_hold_cap_bytes: u64,
    /// Entries whose section was kept after its first read, summed over the
    /// parts read, a deferred part's and a failed build's included. Each
    /// costs its footer and section GETs once rather than once per pass.
    /// Always 0 on metrics, whose LABEL_DICT sections are not kept.
    pub section_hold_entries: u64,
    /// The most section bytes kept at once, as read: the largest part's
    /// kept total, since a part's sections are released when its build
    /// ends. At most `section_hold_cap_bytes`.
    pub section_hold_peak_bytes: u64,
    /// Build passes, summed over the parts read, a deferred part's and a
    /// failed build's included. One pass serves every field the part builds
    /// and reads each of the part's entries' sections once, so when every
    /// read yields a section `sections_read` is the parts' entries times
    /// their passes; an entry whose read yields none is not read again.
    pub passes: u64,
    /// For each field built, the passes that started with it still building,
    /// summed over the same parts. Every field of a part starts building at
    /// its first pass and, because the repair carries no previous leaf, the
    /// build never restarts a field, so a part's passes are the largest of
    /// its fields' counts, not their sum. A part whose build failed and
    /// stopped the run is in `passes` but not here.
    pub passes_per_field: BTreeMap<String, u64>,
    /// KEY_IDX (LABEL_DICT on metrics) section reads, one per (entry, pass),
    /// whatever GETs each cost; a read that failed at its footer counts too.
    pub sections_read: u64,
    /// For each field built, its key pairs, one per distinct (key8, entry),
    /// summed over the parts built (deferred parts excluded, a part over the
    /// byte cap included). A field built with no pair is present with 0. For
    /// a field rebuilt after a later pass found a new uncovered entry, the
    /// pairs of the rebuild only.
    pub field_pairs: BTreeMap<String, u64>,
    /// For each part built (deferred parts excluded) and each field built for
    /// it, the part entries no leaf covers, summed.
    pub uncovered_entries: u64,
    /// Metrics parts given no `label_value` ref because their leaves would
    /// pass the byte cap
    /// [`CatalogConfig::metrics_label_index_max_ratio_ppm`](crate::CatalogConfig::metrics_label_index_max_ratio_ppm)
    /// sets; no leaf of that field is PUT for them after the pass that
    /// passed the cap.
    pub parts_over_cap: u64,
    /// Metrics parts left unindexed because a metrics erasure request was
    /// pending (or the pending listing failed): every part in
    /// `parts_unindexed`, with no part read.
    pub parts_erasure_gated: u64,
    /// LIST requests of the pending metrics erasure listing: one page per
    /// request, issued only on metrics with at least one unindexed part.
    pub erasure_list_requests: u64,
    /// GET requests of the same listing: one per pending request's `.dreq`
    /// object it read.
    pub erasure_get_requests: u64,
    /// Batches that reached their publish step.
    pub attempt_count: u64,
    /// Every batch that read a part: the `attempt_count` batches that reached
    /// their publish step, then the batch that stopped the run, if any.
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
    #[error(
        "{0:?} objects carry no KEY_IDX section; the key-index rebuild covers logs, spans, and metrics with metrics_label_index on"
    )]
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

/// Why [`Catalog::refused_leaf_refs`] stopped before listing a part's
/// refused refs.
enum RefusedLeafError {
    /// A leaf prefix GET failed; the part is deferred to a rerun.
    Store(StoreError),
    /// A leaf above the signal's read set (ADR-4233 H1): the rebuild fails the
    /// cycle rather than replace a leaf a newer build wrote.
    Newer(snapshot_format::SnapshotFormatError),
}

/// A part HEAD names without a usable ref for some requested field, and those
/// fields.
struct Target {
    part: SnapshotPartRef,
    /// Fields with no ref, then fields whose refs `replaces` names.
    missing: Vec<(String, KeyType)>,
    /// Keys of the refs whose leaf version the signal's reader refuses: the
    /// rebuild swaps them out of HEAD for the new refs of the same field.
    replaces: HashSet<String>,
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

/// Whether `part` still needs `field` built: it holds no ref for it, or every
/// ref it holds for it is one the run found refused.
fn still_needed(part: &SnapshotPartRef, field: &str, replaces: &HashSet<String>) -> bool {
    part.key_index
        .iter()
        .filter(|r| r.field == field)
        .all(|r| replaces.contains(&r.key))
}

/// One part's built refs, and the refused refs they stand in for.
struct BuiltPart {
    part_blake3: Vec<u8>,
    refs: Vec<SnapshotKeyIndexLeafRef>,
    replaces: HashSet<String>,
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
/// field or holds for it only refs the build replaces, which are removed.
/// Returns the copy and how many parts gained a ref.
fn attach(head: &SnapshotHead, built: &[BuiltPart]) -> (SnapshotHead, u64) {
    let mut next = head.clone();
    let mut attached = 0u64;
    for built in built {
        let Some(part) = next
            .parts
            .iter_mut()
            .find(|p| p.blake3 == built.part_blake3)
        else {
            continue;
        };
        let fields: HashSet<&str> = built
            .refs
            .iter()
            .map(|r| r.field.as_str())
            .filter(|field| still_needed(part, field, &built.replaces))
            .collect();
        if fields.is_empty() {
            continue;
        }
        part.key_index
            .retain(|have| !fields.contains(have.field.as_str()));
        part.key_index.extend(
            built
                .refs
                .iter()
                .filter(|r| fields.contains(r.field.as_str()))
                .cloned(),
        );
        attached += 1;
    }
    // Every part keeps its `retired_hours`, so the version is the one the
    // carried content requires.
    next.format_version = snapshot_format::head_format_version_for(&next);
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
    /// entries costs the fold's ranged KEY_IDX reads once per pass, or once
    /// for the part when its section is held
    /// ([`crate::CatalogConfig::key_index_repair_hold_bytes`]). The leaves
    /// are built and PUT by the fold's own build under the fold's key layout
    /// and pair budget, [`crate::CatalogConfig::key_index_build_max_pairs`]
    /// (see the module doc for the bound), then HEAD is CAS-published with
    /// every part ref unchanged except its `key_index`.
    /// Writes no part, no `.csnap`, no `.cstat`, and lists nothing on logs
    /// and spans.
    ///
    /// Metrics is rebuilt only under
    /// [`crate::CatalogConfig::metrics_label_index`], and refused as
    /// [`KeyIndexRebuildError::UnsupportedSignal`] without it. Each entry's
    /// `label_value` field is read from its object's LABEL_DICT by the
    /// fold's ranged reads. When at least one part is unindexed, the pending
    /// metrics erasure requests are listed once first; any pending request,
    /// or a listing that fails, ends the run with no part read and every
    /// unindexed part counted in [`KeyIndexRebuildReport::parts_erasure_gated`].
    /// A part whose leaves for a field would total more than the byte cap
    /// gains no ref for it and is counted in
    /// [`KeyIndexRebuildReport::parts_over_cap`]; the cap is checked at each
    /// pass end, so leaves earlier passes PUT stay unreferenced.
    ///
    /// Parts go in batches (see [`KeyIndexRebuildOptions::batch_entries`]),
    /// one HEAD publish each, of up to the fold's CAS retry budget. A lost CAS
    /// re-reads HEAD and attaches refs only to batch parts whose `blake3` the
    /// new HEAD still names; it reads no section again. A part already holding
    /// a ref for every requested field costs no GET, and a part holding some
    /// keeps them and gains the rest. A part one of whose entries' ranged
    /// GETs failed with a store error is deferred once that pass's reads are
    /// done, before its PUTs: it gains no ref this run, so a rerun reads it
    /// again, and leaves earlier passes PUT stay unreferenced.
    ///
    /// On logs, whose reader refuses a version-1 leaf (ADR-3692 amendment
    /// "Leaf format version 2"), every ref of a requested field also costs
    /// one ranged GET of its `[0, prefix_len)` before any batch, counted in
    /// [`KeyIndexRebuildReport::leaf_prefix_gets`] when issued. A field one of
    /// whose leaves is version 1 is built again, the new leaves are PUT at
    /// their own keys, and the publish replaces that field's refs on the part
    /// rather than adding to them. The old leaf object is neither overwritten
    /// nor deleted. A failed prefix GET defers the part. Spans accept version
    /// 1 and cost no prefix GET.
    pub async fn rebuild_key_index(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        options: &KeyIndexRebuildOptions,
    ) -> Result<KeyIndexRebuildReport, KeyIndexRebuildError> {
        let tenant_config = crate::tenant_config::read_config_values(self.store(), tenant).await?;
        let Some(declared) =
            declared_key_index_fields(signal, tenant_config.as_ref(), tenant, self.config())
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
            key_index_build_max_pairs: self.config().key_index_build_max_pairs.max(1),
            section_hold_cap_bytes: self.config().key_index_repair_hold_cap(),
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
        let mut targets: Vec<Target> = Vec::new();
        for part in head.parts.iter().filter(|part| part.entry_count > 0) {
            let mut missing = missing_fields(part, &requested);
            let replaces = match self
                .refused_leaf_refs(signal, part, &requested, &mut report)
                .await
            {
                Ok(replaces) => replaces,
                // A leaf a newer build wrote: the rebuild must not replace it
                // (ADR-4233 H1). Fail the cycle before any batch runs; the
                // report holds only the reads made so far.
                Err(RefusedLeafError::Newer(err)) => {
                    return Err(KeyIndexRebuildError::Interrupted {
                        source: CatalogError::SnapshotFormat(err),
                        report: Box::new(report),
                    });
                }
                Err(RefusedLeafError::Store(e)) => {
                    report.parts_deferred += 1;
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        part_key = %part.key,
                        error = %e,
                        "a key-index leaf prefix GET failed; the part keeps its refs this run and a rerun reads it again"
                    );
                    continue;
                }
            };
            if !replaces.is_empty() {
                report.parts_with_refused_leaves += 1;
                missing.extend(
                    requested
                        .iter()
                        .filter(|(field, _)| {
                            part.key_index
                                .iter()
                                .any(|r| &r.field == field && replaces.contains(&r.key))
                        })
                        .cloned(),
                );
            }
            if !missing.is_empty() {
                targets.push(Target {
                    part: part.clone(),
                    missing,
                    replaces,
                });
            }
        }
        report.parts_unindexed = targets.len() as u64;
        report.largest_unindexed_part_entries = targets
            .iter()
            .map(|t| t.part.entry_count)
            .max()
            .unwrap_or(0);

        if signal == Signal::Metrics && !targets.is_empty() {
            let gate = self.metrics_label_index_erasure_gate(tenant).await;
            report.erasure_list_requests = gate.list_requests;
            report.erasure_get_requests = gate.get_requests;
            if gate.gated {
                report.parts_erasure_gated = report.parts_unindexed;
                return Ok(report);
            }
        }

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

    /// The keys of `part`'s refs for `requested` fields whose leaf is at a
    /// `format_version` the signal's reader refuses but that is not newer than
    /// this build reads, read with one ranged GET of `[0, prefix_len)` per
    /// ref, each counted when issued. Empty, with no GET, for a signal that
    /// accepts every version its leaves can carry
    /// ([`key_index_leaf_versions_for_signal`]: spans and metrics). A ref whose
    /// prefix does not decode is left alone: the reader already treats it as
    /// uncovered, and replacing it is not this repair. A leaf above the
    /// signal's read set is [`RefusedLeafError::Newer`] (ADR-4233 H1, finding
    /// 3): replacing it would CAS a HEAD that drops the newer leaf, which the
    /// sweep then deletes, so the rebuild fails the cycle instead.
    async fn refused_leaf_refs(
        &self,
        signal: Signal,
        part: &SnapshotPartRef,
        requested: &[(String, KeyType)],
        report: &mut KeyIndexRebuildReport,
    ) -> Result<HashSet<String>, RefusedLeafError> {
        let mut refused = HashSet::new();
        if key_index_leaf_versions_for_signal(signal)
            .iter()
            .all(|&version| key_index_leaf_version_accepted(signal, version))
        {
            return Ok(refused);
        }
        for leaf in part
            .key_index
            .iter()
            .filter(|r| requested.iter().any(|(field, _)| *field == r.field))
        {
            let Ok(range) = key_index_leaf_prefix_range(leaf) else {
                continue;
            };
            report.leaf_prefix_gets += 1;
            let got = self
                .store()
                .get(&leaf.key, GetRange::Range(range.start, range.end))
                .await
                .map_err(RefusedLeafError::Store)?;
            report.leaf_prefix_bytes_read += got.data.len() as u64;
            if let Ok(version) = key_index_leaf_format_version(&got.data)
                && !key_index_leaf_version_accepted(signal, version)
            {
                if key_index_leaf_version_is_newer(signal, version) {
                    return Err(RefusedLeafError::Newer(newer_leaf_refusal(
                        &leaf.key, version,
                    )));
                }
                refused.insert(leaf.key.clone());
            }
        }
        // Every slice of a field is replaced together, so a field with one
        // refused slice hands all of its refs over.
        let fields: HashSet<&str> = part
            .key_index
            .iter()
            .filter(|r| refused.contains(&r.key))
            .map(|r| r.field.as_str())
            .collect();
        Ok(part
            .key_index
            .iter()
            .filter(|r| fields.contains(r.field.as_str()))
            .map(|r| r.key.clone())
            .collect())
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
        let built = match self
            .build_batch(tenant, signal, batch, head, &mut attempt, report)
            .await
        {
            Ok(built) => built,
            Err(e) => {
                // The stopped batch is listed with what it issued, so the
                // attempts sum to the totals, but it never reached its
                // publish step and is not counted in `attempt_count`.
                if attempt.parts > 0 {
                    report.attempts.push(attempt);
                }
                return Err(e);
            }
        };
        if attempt.parts == 0 {
            return Ok(());
        }
        report.attempt_count += 1;
        report.attempts.push(attempt);
        self.publish_batch(head_key, &built, head, version, report)
            .await
    }

    /// Reads and builds one batch's parts, counting what it issues into
    /// `attempt` and `report` as it goes, a failed build's requests included.
    async fn build_batch(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        batch: Vec<Target>,
        head: &SnapshotHead,
        attempt: &mut KeyIndexRebuildAttempt,
        report: &mut KeyIndexRebuildReport,
    ) -> Result<Vec<BuiltPart>, BatchError> {
        let mut counters = RequestCounters::default();
        let mut built: Vec<BuiltPart> = Vec::new();
        // A part the rebuild visits has no leaf for these fields, or only a
        // refused one, so there is nothing to carry and no held dictionary.
        let no_held = HeldLabelDicts::new(Vec::new(), 0);
        // Charged to the catalog's memory budget like the part decode above
        // each build; a cap of 0 holds nothing and builds exactly as with no
        // hold.
        let mut section_hold = (report.section_hold_cap_bytes > 0)
            .then(|| KeyIndexSectionHold::new(report.section_hold_cap_bytes, self));
        for target in batch {
            // A part an earlier batch's CAS loss showed replaced, or already
            // given the refs by someone else, costs nothing.
            let Some(current) = head.parts.iter().find(|p| p.blake3 == target.part.blake3) else {
                continue;
            };
            let missing: Vec<(String, KeyType)> = target
                .missing
                .into_iter()
                .filter(|(field, _)| still_needed(current, field, &target.replaces))
                .collect();
            if missing.is_empty() {
                continue;
            }
            let part = target.part;
            attempt.parts += 1;
            attempt.part_gets += 1;
            // The part's decode reservation is held until this iteration
            // drops `entries`; a refusal interrupts the run.
            let (entries, _part_charge) =
                self.load_part_entries(tenant, &part, &mut counters).await?;
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

            let leaf_bytes_cap = (signal == Signal::Metrics).then(|| {
                metrics_label_index_leaf_bytes_cap(
                    entries.iter().map(|e| e.object_size).sum(),
                    self.config().metrics_label_index_max_ratio_ppm,
                )
            });
            // The fold's own passes under the pair budget: each field holds at
            // most its share of `key_index_build_max_pairs` at a time.
            let before = counters;
            let entries_held_before = section_hold
                .as_ref()
                .map_or(0, KeyIndexSectionHold::entries_held);
            let built_part = build_part_key_index(
                self.store(),
                tenant,
                signal,
                part_blake3,
                part.watermark_hour,
                &entries,
                &missing,
                &[],
                no_held.part(0),
                section_hold.as_mut(),
                &KeyIndexBuildLimits {
                    leaf: key_index_leaf_limits(),
                    max_pairs: self.config().key_index_build_max_pairs,
                    concurrency: self.config().fold_bucket_concurrency,
                    leaf_bytes_cap,
                },
                KeyIndexBuildCaller::Repair,
                &mut counters,
            )
            .await;
            // A build that fails has still issued its reads and PUTs, so the
            // report counts them before the error interrupts the run.
            let ranged_gets = counters.key_index_ranged_gets - before.key_index_ranged_gets;
            let footer_gets = counters.key_index_footer_gets - before.key_index_footer_gets;
            let section_gets = counters.key_index_section_gets - before.key_index_section_gets;
            let passes = counters.key_index_build_passes - before.key_index_build_passes;
            let sections_read = counters.key_index_sections_read - before.key_index_sections_read;
            let leaf_puts = counters.put_requests - before.put_requests;
            attempt.ranged_gets += ranged_gets;
            attempt.footer_phase_ranged_gets += footer_gets;
            attempt.section_phase_ranged_gets += section_gets;
            attempt.passes += passes;
            attempt.sections_read += sections_read;
            attempt.leaf_puts += leaf_puts;
            report.ranged_gets += ranged_gets;
            report.footer_phase_ranged_gets += footer_gets;
            report.section_phase_ranged_gets += section_gets;
            report.section_bytes_read +=
                counters.key_index_bytes_read - before.key_index_bytes_read;
            report.footer_phase_bytes_read +=
                counters.key_index_footer_bytes_read - before.key_index_footer_bytes_read;
            report.section_phase_bytes_read +=
                counters.key_index_section_bytes_read - before.key_index_section_bytes_read;
            report.passes += passes;
            report.sections_read += sections_read;
            report.leaves_written += leaf_puts;
            report.leaf_bytes_written +=
                counters.key_index_leaf_bytes_put - before.key_index_leaf_bytes_put;
            if let Some(hold) = &section_hold {
                report.section_hold_entries += hold.entries_held() - entries_held_before;
                report.section_hold_peak_bytes =
                    report.section_hold_peak_bytes.max(hold.peak_held_bytes());
            }
            let leaves = built_part?;
            for (field, passes) in &leaves.field_passes {
                *report.passes_per_field.entry(field.clone()).or_insert(0) += passes;
            }
            if leaves.stopped_on_failed_get {
                report.parts_deferred += 1;
                tracing::warn!(
                    tenant = %tenant.to_hex(),
                    part_key = %part.key,
                    "a KEY_IDX GET failed for this part's entries; the part gains no key-index ref this run and a rerun reads it again"
                );
                continue;
            }
            if leaves.over_cap {
                report.parts_over_cap += 1;
            }
            for (field, pairs) in &leaves.field_pairs {
                *report.field_pairs.entry(field.clone()).or_insert(0) += pairs;
            }
            report.uncovered_entries += leaves.uncovered_entries;
            if !leaves.refs.is_empty() {
                built.push(BuiltPart {
                    part_blake3: part.blake3,
                    refs: leaves.refs,
                    replaces: target.replaces,
                });
            }
        }
        Ok(built)
    }

    /// The batch's HEAD publish: attach, CAS, and on a lost CAS re-read HEAD
    /// and attach again, up to the fold's retry budget.
    async fn publish_batch(
        &self,
        head_key: &str,
        built: &[BuiltPart],
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
    use crate::fold::METRICS_LABEL_VALUE_FIELD;
    use crate::fold::put_key_index_leaf;
    use crate::fold::tests::{
        KIDX_HOUR, USER_ID, assert_foreign_part_error, config, declare_user_id_key_index,
        fold_through, kidx_objects, matches, metrics_kidx_config, part_ordinals, probe_part,
        publish_label_fixture, publish_trace_spans, publish_user_id_logs,
        put_metrics_erasure_request, read_signal_head, repoint_head_at_foreign_part, tenant,
    };
    use crate::snapshot_format::{KeyIndexLeafLimits, KeyIndexLeafProbe, verify_key_index_leaf};

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
                parts_with_refused_leaves: 0,
                leaf_prefix_gets: 0,
                leaf_prefix_bytes_read: 0,
                parts_rebuilt: 1,
                parts_deferred: 0,
                leaves_written: 1,
                leaf_bytes_written: report.leaf_bytes_written,
                section_bytes_read: report.section_bytes_read,
                ranged_gets: 6,
                footer_phase_ranged_gets: 3,
                footer_phase_bytes_read: report.footer_phase_bytes_read,
                section_phase_ranged_gets: 3,
                section_phase_bytes_read: report.section_phase_bytes_read,
                key_index_build_max_pairs: crate::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS,
                // One part of three sections, every one kept under the
                // default cap.
                section_hold_cap_bytes: 1_296_000_000,
                section_hold_entries: 3,
                section_hold_peak_bytes: report.section_phase_bytes_read,
                passes: 1,
                passes_per_field: BTreeMap::from([(USER_ID.to_string(), 1)]),
                sections_read: 3,
                field_pairs: BTreeMap::from([(USER_ID.to_string(), 9)]),
                uncovered_entries: 0,
                parts_over_cap: 0,
                parts_erasure_gated: 0,
                erasure_list_requests: 0,
                erasure_get_requests: 0,
                attempt_count: 1,
                attempts: vec![KeyIndexRebuildAttempt {
                    parts: 1,
                    part_gets: 1,
                    ranged_gets: 6,
                    footer_phase_ranged_gets: 3,
                    section_phase_ranged_gets: 3,
                    passes: 1,
                    sections_read: 3,
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
    /// HEAD GET, one prefix GET for each of the leaves parts 1 and 2 hold (a
    /// logs leaf's version is read before it is trusted), part 3's GET and
    /// its four ranged GETs, and completes part 3.
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
        let prefixes: Vec<String> = gets
            .iter()
            .filter(|(key, range, _)| key.ends_with(".kidx") && !matches!(range, GetRange::Full))
            .map(|(key, _, _)| key.clone())
            .collect();
        assert_eq!(
            prefixes,
            vec![
                head.parts[0].key_index[0].key.clone(),
                head.parts[1].key_index[0].key.clone(),
            ],
            "one prefix GET per leaf parts 1 and 2 already hold"
        );
        assert_eq!(report.leaf_prefix_gets, 2, "{report:?}");
        assert_eq!(gets.len(), 3 + 4 + 2, "{gets:?}");
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
                parts_with_refused_leaves: 0,
                leaf_prefix_gets: 0,
                leaf_prefix_bytes_read: 0,
                parts_rebuilt: 1,
                parts_deferred: 0,
                leaves_written: 2,
                leaf_bytes_written: report.leaf_bytes_written,
                section_bytes_read: report.section_bytes_read,
                ranged_gets: 8,
                footer_phase_ranged_gets: 4,
                footer_phase_bytes_read: report.footer_phase_bytes_read,
                section_phase_ranged_gets: 4,
                section_phase_bytes_read: report.section_phase_bytes_read,
                key_index_build_max_pairs: crate::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS,
                section_hold_cap_bytes: 1_296_000_000,
                section_hold_entries: 4,
                section_hold_peak_bytes: report.section_hold_peak_bytes,
                passes: 2,
                passes_per_field: BTreeMap::from([(USER_ID.to_string(), 2)]),
                sections_read: 4,
                field_pairs: BTreeMap::from([(USER_ID.to_string(), 8)]),
                uncovered_entries: 0,
                parts_over_cap: 0,
                parts_erasure_gated: 0,
                erasure_list_requests: 0,
                erasure_get_requests: 0,
                attempt_count: 1,
                attempts: vec![KeyIndexRebuildAttempt {
                    parts: 2,
                    part_gets: 2,
                    ranged_gets: 8,
                    footer_phase_ranged_gets: 4,
                    section_phase_ranged_gets: 4,
                    passes: 2,
                    sections_read: 4,
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
                    parts_with_refused_leaves: 0,
                    leaf_prefix_gets: 0,
                    leaf_prefix_bytes_read: 0,
                    parts_rebuilt: 0,
                    parts_deferred: 0,
                    leaves_written: 0,
                    leaf_bytes_written: 0,
                    section_bytes_read: report.section_bytes_read,
                    ranged_gets: 2,
                    footer_phase_ranged_gets: 2,
                    // No section GET: every byte read is a footer byte.
                    footer_phase_bytes_read: report.section_bytes_read,
                    section_phase_ranged_gets: 0,
                    section_phase_bytes_read: 0,
                    key_index_build_max_pairs: crate::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS,
                    // No section was read, so none is kept.
                    section_hold_cap_bytes: 1_296_000_000,
                    section_hold_entries: 0,
                    section_hold_peak_bytes: 0,
                    passes: 1,
                    passes_per_field: BTreeMap::from([(USER_ID.to_string(), 1)]),
                    sections_read: 2,
                    field_pairs: BTreeMap::from([(USER_ID.to_string(), 0)]),
                    uncovered_entries: 2,
                    parts_over_cap: 0,
                    parts_erasure_gated: 0,
                    erasure_list_requests: 0,
                    erasure_get_requests: 0,
                    attempt_count: 1,
                    attempts: vec![KeyIndexRebuildAttempt {
                        parts: 1,
                        part_gets: 1,
                        ranged_gets: 2,
                        footer_phase_ranged_gets: 2,
                        section_phase_ranged_gets: 0,
                        passes: 1,
                        sections_read: 2,
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

    /// Issue #3320: the rebuild charges a part's decoded entry body to the
    /// catalog's memory budget before decoding it, and a refused reservation
    /// stops the run with the typed `Interrupted` error carrying
    /// `MemoryExhausted`, publishing nothing. Fixture: one batch of two parts
    /// under a budget one byte short of the first part's declared
    /// `entries_uncompressed_len`.
    ///
    /// FLIP: drop the `reserve_decoded` call in `Catalog::load_part_entries`
    /// and the rebuild succeeds, so `expect_err` panics.
    #[tokio::test]
    async fn a_refused_part_reservation_interrupts_the_rebuild() {
        let inner = recording_store();
        two_entry_parts(&inner, 2).await;
        let before = read_signal_head(inner.as_ref(), Signal::Logs).await;
        let part = inner
            .inner
            .get(&before.parts[0].key, GetRange::Full)
            .await
            .expect("first part present")
            .data;
        let declared = crate::snapshot_format::decode_part_header(&part)
            .expect("part header decodes")
            .entries_uncompressed_len;
        assert!(declared > 0, "the fixture part has a body to charge");
        assert!(
            declared <= crate::snapshot_format::PartLimits::default().max_snapshot_part_bytes,
            "the decoder would admit this body, so only the budget can refuse it"
        );
        let budget = Arc::new(ravel_memory::MemoryBudget::new(declared - 1));
        let catalog = off_catalog(inner.clone(), 2).with_memory_budget(budget.clone());
        inner.clear();
        let err = catalog
            .rebuild_key_index(&tenant(), Signal::Logs, &KeyIndexRebuildOptions::default())
            .await
            .expect_err("a refused part reservation must interrupt the rebuild");
        let KeyIndexRebuildError::Interrupted { source, report } = &err else {
            panic!("expected Interrupted, got {err:?}");
        };
        match source {
            CatalogError::MemoryExhausted(exhausted) => {
                assert_eq!(exhausted.requested, declared);
                assert_eq!(exhausted.reserved, 0);
                assert_eq!(exhausted.limit, declared - 1);
            }
            other => panic!("expected CatalogError::MemoryExhausted, got {other:?}"),
        }
        assert_eq!(
            (report.parts_rebuilt, report.attempt_count),
            (0, 0),
            "{report:?}"
        );
        assert_eq!(budget.reserved(), 0, "nothing stays charged");
        assert!(
            !inner.puts().iter().any(|key| key.ends_with("/HEAD")),
            "{:?}",
            inner.puts()
        );
        assert_eq!(read_signal_head(inner.as_ref(), Signal::Logs).await, before);
    }

    /// Issue #3539, ADR-0050 §2: the rebuild loads a part through the same
    /// tenant-checked load the fold uses, so a part whose header names another
    /// tenant stops the run with the typed `Interrupted` error carrying the
    /// `tenant_hash` mismatch, counts one isolation breach and publishes
    /// nothing. Fixture: one batch of two parts, the first re-pointed at a
    /// foreign copy whose blake3 the HEAD carries.
    ///
    /// FLIP: delete the `tenant_hash` comparison in
    /// `Catalog::load_part_entries` and the rebuild succeeds, so `expect_err`
    /// panics. FLIP: drop its `record_isolation_breach` call and
    /// `isolation_breaches()` reads 0.
    #[tokio::test]
    async fn a_foreign_tenant_hash_on_a_part_interrupts_the_rebuild() {
        let inner = recording_store();
        two_entry_parts(&inner, 2).await;
        let part_key = repoint_head_at_foreign_part(inner.as_ref(), Signal::Logs, 0).await;
        let before = read_signal_head(inner.as_ref(), Signal::Logs).await;
        let catalog = off_catalog(inner.clone(), 2);
        inner.clear();
        let err = catalog
            .rebuild_key_index(&tenant(), Signal::Logs, &KeyIndexRebuildOptions::default())
            .await
            .expect_err("a part naming another tenant must interrupt the rebuild");
        let KeyIndexRebuildError::Interrupted { source, report } = &err else {
            panic!("expected Interrupted, got {err:?}");
        };
        assert_foreign_part_error(source, &part_key);
        assert_eq!(catalog.isolation_breaches(), 1, "the breach is counted");
        assert_eq!(
            (report.parts_rebuilt, report.attempt_count),
            (0, 0),
            "{report:?}"
        );
        assert!(
            !inner.puts().iter().any(|key| key.ends_with("/HEAD")),
            "{:?}",
            inner.puts()
        );
        assert_eq!(read_signal_head(inner.as_ref(), Signal::Logs).await, before);
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

    /// Four logs objects of ten `user_id`s each, interleaved in key order
    /// (object `seq` holds `i * 4 + seq` for `i` in `0..10`), two rows to a
    /// block: one part of 40 (entry, key8) pairs, every key8 run one pair.
    async fn forty_pair_part(store: &RecordingStore) -> Vec<(u64, i64, u32)> {
        declare_user_id_key_index(store).await;
        let mut placed = Vec::new();
        for seq in 1..=4u64 {
            let user_ids: Vec<i64> = (0..10).map(|i| i * 4 + seq as i64).collect();
            for (row, v) in user_ids.iter().enumerate() {
                placed.push((seq, *v, (row / 2) as u32));
            }
            publish_user_id_logs(store, seq, KIDX_HOUR, &user_ids, 2, 0, true).await;
        }
        placed
    }

    /// Issue #4018: the repair builds under the fold's pair budget. A part of
    /// 40 `user_id` pairs, each key8 run one pair, rebuilt at
    /// `key_index_build_max_pairs` 8 (one field, a share of 8): the passes
    /// hold exactly the 40 pairs between them, in 5 passes, each pass's heap
    /// peaking at most at the share plus the one pair placed before a cut,
    /// 9, and ending holding none. With the section hold off
    /// (`key_index_repair_hold_bytes` 0), every pass reads the part's
    /// sections again, so the run issues 5 x 2 ranged GETs for each of the 4
    /// entries. Every one of the 40 keys then probes to its (entry, block),
    /// and the leaves equal, byte for byte, the ones a fold with the leaf
    /// build on writes for the same part at the same budget.
    ///
    /// A repair that collects every entry's reads before building, budgeted
    /// tiling or not, records no pass and issues 8 ranged GETs.
    #[tokio::test]
    async fn the_repair_holds_at_most_the_pair_budget_and_matches_the_fold_at_that_budget() {
        const MAX_PAIRS: u64 = 8;
        let rebuilt = recording_store();
        let placed = forty_pair_part(&rebuilt).await;
        let off = Catalog::new(
            rebuilt.clone(),
            CatalogConfig {
                key_index_build_max_pairs: MAX_PAIRS,
                key_index_repair_hold_bytes: Some(0),
                ..config(1)
            },
        )
        .expect("catalog");
        assert!(!off.config().build_key_index_leaves);
        fold_through(&off, Signal::Logs, KIDX_HOUR).await;
        crate::fold::take_key_index_pass_log_for_test();
        let report = rebuild(&off, Signal::Logs).await;
        let log = crate::fold::take_key_index_pass_log_for_test();

        let held: u64 = log.iter().map(|r| r.held).sum();
        assert_eq!(held, 40, "the passes hold every pair once: {log:?}");
        assert_eq!(log.len(), 5, "{log:?}");
        for record in &log {
            assert_eq!(record.field, USER_ID, "{record:?}");
            assert_eq!(record.share, MAX_PAIRS, "{record:?}");
            assert!(record.held <= MAX_PAIRS, "{record:?}");
            assert!(
                record.held_peak + record.open_slice <= MAX_PAIRS + 1,
                "{record:?}"
            );
            assert_eq!(record.open_slice, 0, "{record:?}");
        }
        let peak = log.iter().map(|r| r.held_peak).max().unwrap_or(0);
        assert_eq!(peak, MAX_PAIRS + 1, "{log:?}");
        assert_eq!(report.ranged_gets, 5 * 2 * 4, "{report:?}");
        assert_eq!(rebuilt.data_ranged_gets().len(), 5 * 2 * 4);
        assert_eq!(report.parts_rebuilt, 1, "{report:?}");
        assert_eq!(report.uncovered_entries, 0, "{report:?}");

        // Issue #4348: the budget, the passes and the pairs, and each ranged
        // GET under its phase. Every footer fits the suffix, so the footer
        // phase is the suffix GETs and the section phase the range GETs.
        assert_eq!(report.key_index_build_max_pairs, MAX_PAIRS, "{report:?}");
        assert_eq!(report.passes, 5, "{report:?}");
        assert_eq!(
            report.passes_per_field,
            BTreeMap::from([(USER_ID.to_string(), 5)]),
            "{report:?}"
        );
        assert_eq!(report.sections_read, 4 * 5, "{report:?}");
        assert_eq!(
            report.field_pairs,
            BTreeMap::from([(USER_ID.to_string(), 40)]),
            "{report:?}"
        );
        let (footer, section): (Vec<_>, Vec<_>) = rebuilt
            .data_ranged_gets()
            .into_iter()
            .partition(|(_, range, _)| matches!(range, GetRange::Suffix(_)));
        assert_eq!((footer.len(), section.len()), (4 * 5, 4 * 5));
        let footer_bytes: u64 = footer.iter().map(|(_, _, bytes)| bytes).sum();
        let section_bytes: u64 = section.iter().map(|(_, _, bytes)| bytes).sum();
        assert_ne!(footer_bytes, section_bytes);
        assert_eq!(
            (
                report.footer_phase_ranged_gets,
                report.section_phase_ranged_gets
            ),
            (4 * 5, 4 * 5),
            "{report:?}"
        );
        assert_eq!(
            report.footer_phase_ranged_gets + report.section_phase_ranged_gets,
            report.ranged_gets
        );
        assert_eq!(
            (
                report.footer_phase_bytes_read,
                report.section_phase_bytes_read
            ),
            (footer_bytes, section_bytes),
            "{report:?}"
        );
        assert_eq!(report.section_bytes_read, footer_bytes + section_bytes);
        assert_eq!(report.attempts.len(), 1, "{report:?}");
        let attempt = &report.attempts[0];
        assert_eq!(
            (
                attempt.passes,
                attempt.sections_read,
                attempt.footer_phase_ranged_gets,
                attempt.section_phase_ranged_gets,
                attempt.ranged_gets
            ),
            (5, 4 * 5, 4 * 5, 4 * 5, 5 * 2 * 4),
            "{attempt:?}"
        );

        let head = read_signal_head(rebuilt.as_ref(), Signal::Logs).await;
        let part = &head.parts[0];
        assert_eq!(part.entry_count, 4);
        assert_eq!(report.leaves_written, part.key_index.len() as u64);
        let ordinal = part_ordinals(rebuilt.as_ref(), part).await;
        for (seq, v, block) in &placed {
            assert_eq!(
                probe_part(
                    &off,
                    Signal::Logs,
                    part,
                    KeyType::I64,
                    USER_ID,
                    key8_i64(*v)
                )
                .await,
                matches(&[(ordinal[seq], &[*block])], &[]),
                "user_id {v}"
            );
        }
        for absent in [0i64, 41, -7] {
            assert_eq!(
                probe_part(
                    &off,
                    Signal::Logs,
                    part,
                    KeyType::I64,
                    USER_ID,
                    key8_i64(absent)
                )
                .await,
                KeyIndexLeafProbe::Absent {
                    uncovered_entry_ordinals: Vec::new()
                },
                "user_id {absent}"
            );
        }

        let folded = recording_store();
        forty_pair_part(&folded).await;
        let on = Catalog::new(
            folded.clone(),
            CatalogConfig {
                build_key_index_leaves: true,
                key_index_build_max_pairs: MAX_PAIRS,
                ..config(1)
            },
        )
        .expect("catalog");
        fold_through(&on, Signal::Logs, KIDX_HOUR).await;
        let fold_head = read_signal_head(folded.as_ref(), Signal::Logs).await;
        assert_eq!(fold_head.parts[0].blake3, part.blake3);
        assert_eq!(fold_head.parts[0].key_index, part.key_index);
        assert!(part.key_index.len() >= 5, "{:?}", part.key_index);
        for leaf in &part.key_index {
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

    /// Issue #4348: the passes of a two-field build are the part's passes, not
    /// the fields' passes summed. The 40-pair part with `trace_id` declared
    /// beside `user_id` at a budget of 8: each field's share is 4, so
    /// `user_id` takes 10 passes; no section names `trace_id`, so it leaves
    /// every entry uncovered and stops building after the first pass. With
    /// the section hold off, each of the 10 passes reads the 4 sections once
    /// by range.
    #[tokio::test]
    async fn the_repair_reports_the_parts_passes_and_each_fields_own() {
        use crate::tenant_config::StorageLayoutWrite;
        let store = recording_store();
        forty_pair_part(&store).await;
        let mut cfg = crate::tenant_config::read_config_values(store.as_ref(), &tenant())
            .await
            .expect("read config")
            .expect("config present");
        cfg.set_index_trace_id(true, StorageLayoutWrite::ReadersRolledOut)
            .expect("index trace_id");
        crate::tenant_config::set_tenant_config(store.as_ref(), &tenant(), &cfg, 2)
            .await
            .expect("write config");
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                key_index_build_max_pairs: 8,
                key_index_repair_hold_bytes: Some(0),
                ..config(1)
            },
        )
        .expect("catalog");
        fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
        let report = rebuild(&catalog, Signal::Logs).await;
        let trace_id = ravel_logseg::key_index::TRACE_ID_FIELD.to_string();
        assert_eq!(
            report.fields,
            vec![USER_ID.to_string(), trace_id.clone()],
            "{report:?}"
        );
        assert_eq!(report.passes, 10, "{report:?}");
        assert_eq!(
            report.passes_per_field,
            BTreeMap::from([(USER_ID.to_string(), 10), (trace_id.clone(), 1)]),
            "{report:?}"
        );
        assert_eq!(report.sections_read, 4 * 10, "{report:?}");
        assert_eq!(report.ranged_gets, 2 * 4 * 10, "{report:?}");
        assert_eq!(
            report.field_pairs,
            BTreeMap::from([(USER_ID.to_string(), 40), (trace_id, 0)]),
            "{report:?}"
        );
        assert_eq!(report.attempts[0].passes, 10, "{report:?}");
    }

    /// The 40-pair part at a budget of 8, rebuilt clean with the section hold
    /// off: the leaf keys in PUT order with their sizes. Pins the five passes
    /// of one leaf each that the interrupted-run tests count on.
    async fn forty_pair_leaves_in_put_order() -> Vec<(String, u64)> {
        let store = recording_store();
        forty_pair_part(&store).await;
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                key_index_build_max_pairs: 8,
                key_index_repair_hold_bytes: Some(0),
                ..config(1)
            },
        )
        .expect("catalog");
        fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
        store.clear();
        let report = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(
            (report.ranged_gets, report.leaves_written),
            (5 * 2 * 4, 5),
            "{report:?}"
        );
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        let sizes: std::collections::BTreeMap<String, u64> = head.parts[0]
            .key_index
            .iter()
            .map(|r| (r.key.clone(), r.size))
            .collect();
        let leaves: Vec<(String, u64)> = store
            .puts()
            .into_iter()
            .filter(|key| key.ends_with(".kidx"))
            .map(|key| {
                let size = sizes[&key];
                (key, size)
            })
            .collect();
        assert_eq!(leaves.len(), 5, "{leaves:?}");
        assert_eq!(
            report.leaf_bytes_written,
            leaves.iter().map(|(_, size)| size).sum::<u64>()
        );
        leaves
    }

    /// The same part and budget rebuilt, with the section hold cap `hold`
    /// (`None` the derived default), over a store whose `nth` leaf PUT (1
    /// based) times out before it reaches the store, under a 1 GiB memory
    /// budget. Returns the run's report, the recording store under the
    /// faults, the fault count and the memory budget.
    async fn forty_pair_rebuild_failing_leaf_put(
        nth: usize,
        hold: Option<u64>,
    ) -> (
        KeyIndexRebuildReport,
        Arc<RecordingStore>,
        u64,
        Arc<ravel_memory::MemoryBudget>,
    ) {
        let inner = recording_store();
        forty_pair_part(&inner).await;
        let cfg = CatalogConfig {
            key_index_build_max_pairs: 8,
            key_index_repair_hold_bytes: hold,
            ..config(1)
        };
        let clean = Catalog::new(inner.clone(), cfg).expect("catalog");
        fold_through(&clean, Signal::Logs, KIDX_HOUR).await;
        let plan = FaultPlan::empty().with_sequence(
            (1..nth)
                .fold(
                    Sequence::new(Op::Put).with_key_contains(".kidx"),
                    |seq, _| seq.then_passthrough(),
                )
                .then_fault(ScriptedFault::Timeout),
        );
        let faulty = Arc::new(FaultStore::new(Arc::clone(&inner), plan));
        let budget = Arc::new(ravel_memory::MemoryBudget::new(1 << 30));
        let catalog = Catalog::new(faulty.clone(), cfg)
            .expect("catalog")
            .with_memory_budget(Arc::clone(&budget));
        inner.clear();
        let err = catalog
            .rebuild_key_index(&tenant(), Signal::Logs, &KeyIndexRebuildOptions::default())
            .await
            .expect_err("the failed leaf PUT interrupts the run");
        let KeyIndexRebuildError::Interrupted { source, report } = err else {
            panic!("{err:?}");
        };
        assert!(
            matches!(source, CatalogError::Store(StoreError::Timeout)),
            "{source:?}"
        );
        assert_eq!(
            (report.parts_rebuilt, report.attempt_count),
            (0, 0),
            "{report:?}"
        );
        assert!(
            !inner.puts().iter().any(|key| key.ends_with("/HEAD")),
            "no HEAD CAS"
        );
        (
            *report,
            inner,
            faulty.fault_count(Op::Put, FaultKind::Timeout),
            budget,
        )
    }

    /// Issue #4297, item 3: the report counts leaf PUTs when issued, the
    /// failed one included. The second of five leaf PUTs times out: the
    /// report counts 2 leaves and the bytes of the first two leaves a clean
    /// run PUTs, while the store holds the first leaf alone.
    ///
    /// Against the code before #4297 the report counts 0 leaves and 0 bytes
    /// (a failed build's PUTs were dropped). A fix that counted only the PUTs
    /// that succeeded by dropping the last one on the error path counts 1.
    #[tokio::test]
    async fn a_failed_leaf_put_is_counted_as_issued_in_the_interrupted_report() {
        let clean = forty_pair_leaves_in_put_order().await;
        let (report, inner, faults, _) = forty_pair_rebuild_failing_leaf_put(2, Some(0)).await;
        assert_eq!(faults, 1);
        assert_eq!(
            (report.leaves_written, report.leaf_bytes_written),
            (2, clean[0].1 + clean[1].1),
            "{report:?}"
        );
        assert_eq!(
            kidx_objects(inner.as_ref(), Signal::Logs).await,
            vec![clean[0].0.clone()],
            "only the first leaf reached the store"
        );
    }

    /// Issue #4297, item 4: a build that fails still reports its reads and
    /// its leaf PUTs. With the section hold off, the fourth of five leaf PUTs
    /// times out, after the fourth pass's reads: the report counts 4 passes x
    /// 2 ranged GETs x 4
    /// entries, exactly the bytes the store served for them, 4 leaf PUTs and
    /// the bytes of the first four leaves a clean run PUTs. The stopped batch
    /// is listed as the one attempt, with its one part GET, those ranged GETs
    /// and those leaf PUTs, while `attempt_count`, the batches that reached
    /// their publish step, stays 0.
    ///
    /// Against the code before #4297 every one of these is 0. A fix that
    /// folded the GET counts on the error path but not the PUT counts reports
    /// 0 leaves and 0 leaf bytes. One that folded the totals but not the
    /// attempt lists no attempt, so its part GET reaches no field.
    #[tokio::test]
    async fn a_failed_build_reports_its_gets_bytes_and_leaf_puts() {
        let clean = forty_pair_leaves_in_put_order().await;
        let (report, inner, faults, _) = forty_pair_rebuild_failing_leaf_put(4, Some(0)).await;
        assert_eq!(faults, 1);
        let served = inner.data_ranged_gets();
        assert_eq!(served.len(), 4 * 2 * 4);
        assert_eq!(
            (
                report.ranged_gets,
                report.section_bytes_read,
                report.leaves_written,
                report.leaf_bytes_written,
            ),
            (
                4 * 2 * 4,
                served.iter().map(|(_, _, bytes)| bytes).sum::<u64>(),
                4,
                clean[..4].iter().map(|(_, size)| size).sum::<u64>(),
            ),
            "{report:?}"
        );
        assert_eq!(kidx_objects(inner.as_ref(), Signal::Logs).await.len(), 3);
        // The stopped batch read the one part with one whole-object GET.
        let part_gets = inner
            .gets()
            .into_iter()
            .filter(|(key, range, _)| key.ends_with(".csnap") && matches!(range, GetRange::Full))
            .count() as u64;
        assert_eq!(part_gets, 1);
        assert_eq!(report.attempt_count, 0, "{report:?}");
        assert_eq!(
            report.attempts,
            vec![KeyIndexRebuildAttempt {
                parts: 1,
                part_gets,
                ranged_gets: report.ranged_gets,
                footer_phase_ranged_gets: 4 * 4,
                section_phase_ranged_gets: 4 * 4,
                passes: 4,
                sections_read: 4 * 4,
                leaf_puts: report.leaves_written,
                head_cas_tries: 0,
                parts_published: 0,
            }],
            "the stopped batch is listed, its GETs and PUTs summing to the totals"
        );
        // The failed build's four passes count; it returned no per-field
        // figure, so none is reported for it.
        assert_eq!(
            (report.passes, report.sections_read),
            (4, 4 * 4),
            "{report:?}"
        );
        assert!(report.passes_per_field.is_empty(), "{report:?}");
        assert!(report.field_pairs.is_empty(), "{report:?}");
    }

    /// The 40-pair part's entries in part order, each with the footer bytes
    /// and the KEY_IDX section bytes one ranged read of it returns, read off
    /// the part and each object's own footer.
    async fn forty_pair_read_sizes(
        store: &RecordingStore,
        part: &SnapshotPartRef,
    ) -> Vec<(u64, u64)> {
        use ravel_logseg::footer::{SuffixOutcome, kind, open_from_suffix};
        let got = store.get(&part.key, GetRange::Full).await.expect("part");
        let entries =
            snapshot_format::decode_part(&got.data, &snapshot_format::PartLimits::default())
                .expect("part decodes")
                .entries;
        let mut sizes = Vec::new();
        for entry in &entries {
            let key = crate::column_stats_build::segment_data_key(&tenant(), Signal::Logs, entry)
                .expect("data key");
            let object = store.get(&key, GetRange::Full).await.expect("object");
            let SuffixOutcome::Ready(footer) =
                open_from_suffix(&object.data, object.data.len() as u64).expect("footer")
            else {
                panic!("the whole object covers its footer");
            };
            let section = footer.section(kind::KEY_IDX).expect("KEY_IDX section");
            sizes.push((
                entry.object_size.min(KEY_INDEX_FOOTER_SUFFIX_LEN),
                section.len,
            ));
        }
        assert_eq!(sizes.len(), 4);
        sizes
    }

    /// One repair of the 40-pair part at a budget of 8 (5 passes) under the
    /// section hold cap `hold`, with the catalog's memory budget limited to
    /// `memory_limit` for the repair alone.
    struct HoldRun {
        report: KeyIndexRebuildReport,
        /// Footer suffix and section range GETs the store served.
        footer_gets: u64,
        section_gets: u64,
        /// `(footer, section)` bytes of one read of each entry, in part order.
        sizes: Vec<(u64, u64)>,
        /// Every leaf the published HEAD names, with its bytes.
        leaves: Vec<(String, Bytes)>,
        /// `take_section_hold_log_for_test` after the run.
        hold_log: Vec<(u64, u64, usize)>,
        budget: Arc<ravel_memory::MemoryBudget>,
        /// The repair's reservation for decoding the part.
        part_charge: u64,
    }

    async fn forty_pair_rebuild_holding(hold: Option<u64>, memory_limit: u64) -> HoldRun {
        let store = recording_store();
        forty_pair_part(&store).await;
        let cfg = CatalogConfig {
            key_index_build_max_pairs: 8,
            key_index_repair_hold_bytes: hold,
            ..config(1)
        };
        fold_through(
            &Catalog::new(store.clone(), cfg).expect("catalog"),
            Signal::Logs,
            KIDX_HOUR,
        )
        .await;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(memory_limit));
        let catalog = Catalog::new(store.clone(), cfg)
            .expect("catalog")
            .with_memory_budget(Arc::clone(&budget));
        store.clear();
        crate::fold::take_section_hold_log_for_test();
        let report = rebuild(&catalog, Signal::Logs).await;
        let hold_log = crate::fold::take_section_hold_log_for_test();
        let (footer, section): (Vec<_>, Vec<_>) = store
            .data_ranged_gets()
            .into_iter()
            .partition(|(_, range, _)| matches!(range, GetRange::Suffix(_)));
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        let part = &head.parts[0];
        let mut leaves = Vec::new();
        for leaf in &part.key_index {
            let got = store.get(&leaf.key, GetRange::Full).await.expect("leaf");
            leaves.push((leaf.key.clone(), got.data));
        }
        let header = store.get(&part.key, GetRange::Full).await.expect("part");
        let part_charge = snapshot_format::decode_part_header(&header.data)
            .expect("part header")
            .entries_uncompressed_len;
        HoldRun {
            report,
            footer_gets: footer.len() as u64,
            section_gets: section.len() as u64,
            sizes: forty_pair_read_sizes(&store, part).await,
            leaves,
            hold_log,
            budget,
            part_charge,
        }
    }

    /// Issue #4376: a section hold cap of 0 is the repair as it was before
    /// the hold. The 40-pair part's 5 passes read each of its 4 entries'
    /// footer and section in every pass: 20 footer GETs and 20 section GETs,
    /// 5 times each entry's footer and section bytes, nothing held and no
    /// hold to release.
    #[tokio::test]
    async fn a_hold_cap_of_zero_reads_every_section_in_every_pass() {
        let run = forty_pair_rebuild_holding(Some(0), u64::MAX).await;
        let footer: u64 = run.sizes.iter().map(|(f, _)| f).sum();
        let section: u64 = run.sizes.iter().map(|(_, s)| s).sum();
        let report = &run.report;
        assert_eq!((run.footer_gets, run.section_gets), (20, 20));
        assert_eq!(
            (
                report.passes,
                report.footer_phase_ranged_gets,
                report.section_phase_ranged_gets,
                report.footer_phase_bytes_read,
                report.section_phase_bytes_read,
            ),
            (5, 20, 20, 5 * footer, 5 * section),
            "{report:?}"
        );
        assert_eq!(
            (
                report.section_hold_cap_bytes,
                report.section_hold_entries,
                report.section_hold_peak_bytes,
            ),
            (0, 0, 0),
            "{report:?}"
        );
        assert_eq!(run.hold_log, Vec::new());
        assert_eq!(run.leaves.len(), 5);
    }

    /// Issue #4376: a cap of exactly the 4 sections' bytes holds all of them
    /// after their first read, so the 5 passes cost one footer and one
    /// section GET per entry, 4 + 4, and one read's bytes of each. The held
    /// bytes peak at the 4 sections' sum, are 0 once the part's build ends,
    /// and the leaves are byte for byte the cap-0 run's.
    ///
    /// FLIP: drop the `hold.0.offer(ordinal, section)` call in
    /// `build_part_key_index` and the run reads 20 + 20 with nothing held.
    #[tokio::test]
    async fn a_cap_for_every_section_reads_each_section_once() {
        let unheld = forty_pair_rebuild_holding(Some(0), u64::MAX).await;
        let sum: u64 = unheld.sizes.iter().map(|(_, s)| s).sum();
        let footer: u64 = unheld.sizes.iter().map(|(f, _)| f).sum();
        let run = forty_pair_rebuild_holding(Some(sum), u64::MAX).await;
        assert_eq!(run.sizes, unheld.sizes);
        let report = &run.report;
        assert_eq!((run.footer_gets, run.section_gets), (4, 4));
        assert_eq!(
            (
                report.passes,
                report.sections_read,
                report.footer_phase_ranged_gets,
                report.section_phase_ranged_gets,
                report.footer_phase_bytes_read,
                report.section_phase_bytes_read,
            ),
            (5, 20, 4, 4, footer, sum),
            "{report:?}"
        );
        assert_eq!(
            (
                report.section_hold_cap_bytes,
                report.section_hold_entries,
                report.section_hold_peak_bytes,
            ),
            (sum, 4, sum),
            "{report:?}"
        );
        assert_eq!(run.hold_log, vec![(sum, 0, 0)]);
        assert_eq!(run.budget.reserved(), 0);
        assert_eq!(run.leaves, unheld.leaves);
    }

    /// Issue #4376, the partial hold: a cap of exactly the first 2 entries'
    /// sections holds those 2 (one footer and one section GET each) and
    /// reads the other 2 by range in all 5 passes (10 + 10), so 12 footer
    /// and 12 section GETs. The held bytes peak at the 2 sections' sum, at
    /// the cap, and the leaves are byte for byte the cap-0 run's.
    ///
    /// DISTINGUISHING: an all-or-nothing hold, which also drops the entries
    /// already held once one does not fit (clearing `by_ordinal` and
    /// `held_bytes` in `KeyIndexSectionHold::offer`'s over-cap branch),
    /// reads 20 + 20; a hold that ignores the cap (the `total <= self.cap`
    /// filter removed) holds all 4 and reads 4 + 4.
    #[tokio::test]
    async fn a_cap_for_two_sections_holds_the_first_two_and_rereads_the_rest() {
        let unheld = forty_pair_rebuild_holding(Some(0), u64::MAX).await;
        let s = &unheld.sizes;
        let cap = s[0].1 + s[1].1;
        let run = forty_pair_rebuild_holding(Some(cap), u64::MAX).await;
        assert_eq!(run.sizes, unheld.sizes);
        assert_partial_hold(&run, cap);
        assert_eq!(run.report.section_hold_cap_bytes, cap);
        assert_eq!(run.leaves, unheld.leaves);
    }

    /// The 2-of-4 hold's figures: entries 0 and 1 held, 2 and 3 read in all
    /// 5 passes.
    fn assert_partial_hold(run: &HoldRun, held: u64) {
        let s = &run.sizes;
        let report = &run.report;
        assert_eq!((run.footer_gets, run.section_gets), (12, 12));
        assert_eq!(
            (
                report.passes,
                report.footer_phase_ranged_gets,
                report.section_phase_ranged_gets,
                report.footer_phase_bytes_read,
                report.section_phase_bytes_read,
            ),
            (
                5,
                12,
                12,
                s[0].0 + s[1].0 + 5 * (s[2].0 + s[3].0),
                s[0].1 + s[1].1 + 5 * (s[2].1 + s[3].1),
            ),
            "{report:?}"
        );
        assert_eq!(held, s[0].1 + s[1].1);
        assert_eq!(
            (report.section_hold_entries, report.section_hold_peak_bytes),
            (2, held),
            "{report:?}"
        );
        assert!(report.section_hold_peak_bytes <= report.section_hold_cap_bytes);
        assert_eq!(run.hold_log, vec![(held, 0, 0)]);
        assert_eq!(run.budget.reserved(), 0);
    }

    /// Issue #4376, item 4: each held section is reserved against the
    /// catalog's memory budget, and a refused reservation ends the holding
    /// rather than the repair. Under the default cap, with the budget limited
    /// to the part's decode charge plus the first 2 entries' sections, the
    /// third entry's reservation is refused: the run holds the first 2 and
    /// reads the other 2 in every pass, exactly as under a cap of those 2,
    /// publishes the same leaves, and leaves nothing reserved.
    ///
    /// FLIP: hold a section without charging it (reserve 0 bytes rather than
    /// its length in `KeyIndexSectionHold::offer`) and all 4 are held: 4 + 4
    /// GETs.
    #[tokio::test]
    async fn a_refused_hold_reservation_falls_back_to_rereading() {
        let unheld = forty_pair_rebuild_holding(Some(0), u64::MAX).await;
        let s = &unheld.sizes;
        let limit = unheld.part_charge + s[0].1 + s[1].1;
        let run = forty_pair_rebuild_holding(None, limit).await;
        assert_eq!(run.sizes, unheld.sizes);
        // The cap derived at a budget of 8 pairs: 2,000,000,000 - 88 x 8.
        assert_eq!(run.report.section_hold_cap_bytes, 1_999_999_296);
        assert_partial_hold(&run, s[0].1 + s[1].1);
        assert_eq!(run.report.parts_rebuilt, 1);
        assert_eq!(run.leaves, unheld.leaves);
    }

    /// Issue #4376: a build that fails mid-part releases what the hold
    /// keeps. At the default cap all 4 sections are held from the first
    /// pass; the fourth of five leaf PUTs times out after the fourth pass,
    /// and the build ends holding the 4 sections and releases them all: the
    /// hold log reads (held, 0 after, 0 sections after) and the memory
    /// budget is back to 0. The reads are one per entry, 4 + 4.
    ///
    /// FLIP: drop `self.0.release()` from `ReleaseSectionHold::drop` and the
    /// log reads the 4 sections' bytes and 4 sections still held after the
    /// build. The same flip fails the lost-CAS test's two-part batch, whose
    /// second part then decodes the first part's held sections.
    #[tokio::test]
    async fn a_failed_build_releases_the_held_sections() {
        let unheld = forty_pair_rebuild_holding(Some(0), u64::MAX).await;
        let sum: u64 = unheld.sizes.iter().map(|(_, s)| s).sum();
        crate::fold::take_section_hold_log_for_test();
        let (report, inner, faults, budget) = forty_pair_rebuild_failing_leaf_put(4, None).await;
        let log = crate::fold::take_section_hold_log_for_test();
        assert_eq!(faults, 1);
        assert_eq!(log, vec![(sum, 0, 0)]);
        assert_eq!(budget.reserved(), 0);
        assert_eq!(
            (
                report.passes,
                report.footer_phase_ranged_gets,
                report.section_phase_ranged_gets,
                report.section_hold_entries,
                report.section_hold_peak_bytes,
            ),
            (4, 4, 4, 4, sum),
            "{report:?}"
        );
        assert_eq!(inner.data_ranged_gets().len(), 4 + 4);
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
        crate::fold::take_section_hold_log_for_test();
        let report = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(faulty.fault_count(Op::Get, FaultKind::Timeout), 1);
        // Issue #4376: the deferred part releases the section it held, the
        // one its other entry's read returned, like the built part before it.
        let other_section: u64 = inner
            .data_ranged_gets()
            .iter()
            .filter(|(key, range, _)| {
                part_keys[1].contains(key)
                    && *key != failing
                    && matches!(range, GetRange::Range(..))
            })
            .map(|(_, _, bytes)| bytes)
            .sum();
        assert_eq!(
            crate::fold::take_section_hold_log_for_test(),
            vec![
                (report.section_hold_peak_bytes, 0, 0),
                (other_section, 0, 0)
            ]
        );
        assert_eq!(
            report,
            KeyIndexRebuildReport {
                largest_unindexed_part_entries: 2,
                fields: vec![USER_ID.to_string()],
                parts_unindexed: 2,
                parts_with_refused_leaves: 0,
                leaf_prefix_gets: 0,
                leaf_prefix_bytes_read: 0,
                parts_rebuilt: 1,
                parts_deferred: 1,
                leaves_written: 1,
                leaf_bytes_written: report.leaf_bytes_written,
                section_bytes_read: report.section_bytes_read,
                ranged_gets: 7,
                // The failed GET is the deferred entry's footer suffix GET.
                footer_phase_ranged_gets: 4,
                footer_phase_bytes_read: report.footer_phase_bytes_read,
                section_phase_ranged_gets: 3,
                section_phase_bytes_read: report.section_phase_bytes_read,
                key_index_build_max_pairs: crate::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS,
                // Both entries of the built part and the deferred part's
                // entry whose GETs succeeded.
                section_hold_cap_bytes: 1_296_000_000,
                section_hold_entries: 3,
                section_hold_peak_bytes: report.section_hold_peak_bytes,
                // The deferred part's one pass counts; its pairs do not.
                passes: 2,
                passes_per_field: BTreeMap::from([(USER_ID.to_string(), 2)]),
                sections_read: 4,
                field_pairs: BTreeMap::from([(USER_ID.to_string(), 4)]),
                uncovered_entries: 0,
                parts_over_cap: 0,
                parts_erasure_gated: 0,
                erasure_list_requests: 0,
                erasure_get_requests: 0,
                attempt_count: 1,
                attempts: vec![KeyIndexRebuildAttempt {
                    parts: 2,
                    part_gets: 2,
                    ranged_gets: 7,
                    footer_phase_ranged_gets: 4,
                    section_phase_ranged_gets: 3,
                    passes: 2,
                    sections_read: 4,
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
    /// GETs and, on logs, one prefix GET per leaf: no part or data object
    /// GET, no PUT, no CAS.
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
        // Metrics objects carry no KEY_IDX: the metrics rebuild declares only
        // `label_value`, which the default config builds, and is refused
        // with `metrics_label_index` off.
        let metrics = rebuild(&catalog, Signal::Metrics).await;
        assert_eq!(
            metrics,
            KeyIndexRebuildReport {
                fields: vec!["label_value".to_string()],
                key_index_build_max_pairs: crate::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS,
                section_hold_cap_bytes: 1_296_000_000,
                ..KeyIndexRebuildReport::default()
            }
        );
        let no_label_index = Catalog::new(
            store.clone(),
            CatalogConfig {
                metrics_label_index: false,
                ..*catalog.config()
            },
        )
        .expect("catalog");
        let err = no_label_index
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
        let leaves: Vec<&SnapshotKeyIndexLeafRef> =
            indexed.parts.iter().flat_map(|p| &p.key_index).collect();
        assert_eq!(leaves.len(), 2);
        assert_eq!(
            rerun,
            KeyIndexRebuildReport {
                fields: vec![USER_ID.to_string()],
                leaf_prefix_gets: 2,
                leaf_prefix_bytes_read: leaves.iter().map(|l| u64::from(l.prefix_len)).sum(),
                key_index_build_max_pairs: crate::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS,
                section_hold_cap_bytes: 1_296_000_000,
                ..KeyIndexRebuildReport::default()
            }
        );
        let gets = store.gets();
        let whole: Vec<String> = gets
            .iter()
            .filter(|(_, range, _)| matches!(range, GetRange::Full))
            .map(|(key, _, _)| key.clone())
            .collect();
        assert_eq!(
            whole,
            vec![
                crate::tenant_config::config_key(&tenant()),
                head_key(Signal::Logs)
            ]
        );
        // A logs rerun reads each version-2 leaf's prefix and nothing else.
        let ranged: Vec<(String, GetRange)> = gets
            .into_iter()
            .filter(|(_, range, _)| !matches!(range, GetRange::Full))
            .map(|(key, range, _)| (key, range))
            .collect();
        let want: Vec<(String, GetRange)> = leaves
            .iter()
            .map(|l| (l.key.clone(), GetRange::Range(0, u64::from(l.prefix_len))))
            .collect();
        assert_eq!(ranged, want);
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

    /// Restamps part 0's only leaf to format_version 1, PUTs it at its own
    /// content-addressed key, and CASes HEAD to name it instead of the
    /// original. Returns the original ref and the version-1 ref.
    async fn swap_in_a_version_one_leaf(
        store: &RecordingStore,
        signal: Signal,
    ) -> (SnapshotKeyIndexLeafRef, SnapshotKeyIndexLeafRef) {
        let head = read_signal_head(store, signal).await;
        assert_eq!(head.parts[0].key_index.len(), 1);
        let original = head.parts[0].key_index[0].clone();
        let bytes = store
            .get(&original.key, GetRange::Full)
            .await
            .expect("leaf")
            .data;
        let v1 = snapshot_format::restamp_key_index_leaf_version(&bytes, 1).expect("restamp");
        let v1_ref = put_key_index_leaf(store, &tenant(), signal, head.parts[0].watermark_hour, v1)
            .await
            .expect("put the version-1 leaf");
        assert_ne!(v1_ref.key, original.key);
        let mut swapped = head.clone();
        swapped.parts[0].key_index = vec![v1_ref.clone()];
        let current = store
            .get(&head_key(signal), GetRange::Full)
            .await
            .expect("head");
        store
            .put(
                &head_key(signal),
                Bytes::from(snapshot_format::encode_head(&swapped).expect("encode")),
                PutOptions {
                    mode: PutMode::CasVersion(current.version),
                    checksum: None,
                },
            )
            .await
            .expect("swap refs");
        (original, v1_ref)
    }

    /// A logs part whose `user_id` ref names a format_version 1 leaf: the
    /// rebuild reads that leaf's prefix with one ranged GET (counted in
    /// `leaf_prefix_gets` and `leaf_prefix_bytes_read`), builds the field
    /// again, and the published HEAD holds exactly one `user_id` ref, a
    /// version-2 leaf at a key other than the version-1 leaf's. The
    /// version-1 object is still stored, byte for byte. A spans part holding
    /// a version-1 leaf is left alone and costs no prefix GET.
    ///
    /// A rebuild that skips parts holding a ref for the field leaves the
    /// version-1 ref in HEAD; one that attaches instead of replacing leaves
    /// two `user_id` refs.
    #[tokio::test]
    async fn a_logs_version_one_ref_is_replaced_and_its_leaf_is_kept() {
        let store = recording_store();
        let (catalog, _) = two_entry_parts(&store, 1).await;
        rebuild(&catalog, Signal::Logs).await;
        let (original, v1_ref) = swap_in_a_version_one_leaf(&store, Signal::Logs).await;
        let v1_bytes = store
            .get(&v1_ref.key, GetRange::Full)
            .await
            .expect("v1 leaf")
            .data;

        store.clear();
        let report = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(
            (
                report.parts_unindexed,
                report.parts_with_refused_leaves,
                report.leaf_prefix_gets,
                report.leaf_prefix_bytes_read,
                report.parts_rebuilt,
                report.leaves_written,
            ),
            (1, 1, 1, u64::from(v1_ref.prefix_len), 1, 1),
            "{report:?}"
        );
        let prefix_gets: Vec<(String, GetRange, u64)> = store
            .gets()
            .into_iter()
            .filter(|(key, _, _)| key.ends_with(".kidx"))
            .collect();
        assert_eq!(
            prefix_gets,
            vec![(
                v1_ref.key.clone(),
                GetRange::Range(0, u64::from(v1_ref.prefix_len)),
                u64::from(v1_ref.prefix_len)
            )]
        );

        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        let refs: Vec<&SnapshotKeyIndexLeafRef> = head.parts[0]
            .key_index
            .iter()
            .filter(|r| r.field == USER_ID)
            .collect();
        assert_eq!(refs.len(), 1, "{head:?}");
        let rebuilt = refs[0];
        assert_ne!(rebuilt.key, v1_ref.key);
        assert_eq!(rebuilt, &original, "the rebuilt leaf is the version-2 leaf");
        let rebuilt_bytes = store
            .get(&rebuilt.key, GetRange::Full)
            .await
            .expect("v2 leaf")
            .data;
        let summary =
            verify_key_index_leaf(&rebuilt_bytes, &KeyIndexLeafLimits::default()).expect("verify");
        assert_eq!(summary.header.format_version, 2);
        let kept = store
            .get(&v1_ref.key, GetRange::Full)
            .await
            .expect("the version-1 leaf is not deleted")
            .data;
        assert_eq!(kept, v1_bytes, "nor overwritten");

        // Spans accept version 1: nothing to replace, no prefix GET.
        let store = recording_store();
        publish_trace_spans(store.as_ref(), 1, &[[0x11u8; 16], [0x22u8; 16]]).await;
        let catalog = off_catalog(store.clone(), DEFAULT_SNAPSHOT_PART_MAX_ENTRIES);
        fold_through(&catalog, Signal::Spans, KIDX_HOUR).await;
        rebuild(&catalog, Signal::Spans).await;
        swap_in_a_version_one_leaf(&store, Signal::Spans).await;
        let before = read_signal_head(store.as_ref(), Signal::Spans).await;
        store.clear();
        let report = rebuild(&catalog, Signal::Spans).await;
        assert_eq!(
            (
                report.parts_unindexed,
                report.leaf_prefix_gets,
                report.attempt_count
            ),
            (0, 0, 0),
            "{report:?}"
        );
        assert!(
            store
                .gets()
                .iter()
                .all(|(key, _, _)| !key.ends_with(".kidx"))
        );
        assert_eq!(
            read_signal_head(store.as_ref(), Signal::Spans).await,
            before
        );
    }

    /// ADR-4233 H1, #4235 (finding 3): a `user_id` ref naming a leaf a newer
    /// build wrote -- a version above the logs read set -- fails the rebuild
    /// with `NewerPart` rather than being replaced like a refused version-1
    /// leaf. Replacing it would CAS a HEAD that drops the newer leaf for the
    /// sweep to delete. HEAD is left unchanged.
    ///
    /// FLIP: classify the newer version as a plain refused ref (drop the
    /// `key_index_leaf_version_is_newer` check in `refused_leaf_refs`) and the
    /// rebuild replaces the leaf and returns `Ok`, so `expect_err` panics.
    #[tokio::test]
    async fn a_newer_leaf_fails_the_rebuild_instead_of_being_replaced() {
        let store = recording_store();
        let (catalog, _) = two_entry_parts(&store, 1).await;
        rebuild(&catalog, Signal::Logs).await;
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        let original = head.parts[0].key_index[0].clone();
        let bytes = store
            .get(&original.key, GetRange::Full)
            .await
            .expect("leaf")
            .data;
        // A version above the logs read set (1, 2): a leaf a newer build wrote.
        let newer = snapshot_format::restamp_key_index_leaf_version(&bytes, 4).expect("restamp");
        let newer_ref = put_key_index_leaf(
            store.as_ref(),
            &tenant(),
            Signal::Logs,
            head.parts[0].watermark_hour,
            newer,
        )
        .await
        .expect("put the newer leaf");
        let mut swapped = head.clone();
        swapped.parts[0].key_index = vec![newer_ref.clone()];
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
        let before = read_signal_head(store.as_ref(), Signal::Logs).await;

        let err = catalog
            .rebuild_key_index(&tenant(), Signal::Logs, &KeyIndexRebuildOptions::default())
            .await
            .expect_err("a newer leaf must fail the rebuild");
        match &err {
            KeyIndexRebuildError::Interrupted {
                source:
                    CatalogError::SnapshotFormat(snapshot_format::SnapshotFormatError::NewerPart {
                        key,
                        kind,
                        version,
                        ..
                    }),
                ..
            } => {
                assert_eq!(key, &newer_ref.key);
                assert_eq!(*kind, snapshot_format::CatalogPartKind::KeyIndexLeaf);
                assert_eq!(*version, 4);
            }
            other => panic!("expected Interrupted(NewerPart), got {other:?}"),
        }
        assert_eq!(
            read_signal_head(store.as_ref(), Signal::Logs).await,
            before,
            "the rebuild left HEAD unchanged"
        );
    }

    /// A logs field whose refs name one version-2 leaf and one version-1
    /// leaf: the rebuild reads both prefixes, hands every ref of the field
    /// over, and HEAD ends with exactly one `user_id` ref, the version-2
    /// leaf a from-scratch build writes.
    ///
    /// A rebuild that replaced only the refused ref keeps the version-2 ref
    /// beside the rebuilt one, two `user_id` refs.
    #[tokio::test]
    async fn a_field_with_one_version_one_slice_hands_every_slice_over() {
        let store = recording_store();
        let (catalog, _) = two_entry_parts(&store, 1).await;
        rebuild(&catalog, Signal::Logs).await;
        let (original, v1_ref) = swap_in_a_version_one_leaf(&store, Signal::Logs).await;
        let mut head = read_signal_head(store.as_ref(), Signal::Logs).await;
        head.parts[0].key_index = vec![original.clone(), v1_ref.clone()];
        let current = store
            .get(&head_key(Signal::Logs), GetRange::Full)
            .await
            .expect("head");
        store
            .put(
                &head_key(Signal::Logs),
                Bytes::from(snapshot_format::encode_head(&head).expect("encode")),
                PutOptions {
                    mode: PutMode::CasVersion(current.version),
                    checksum: None,
                },
            )
            .await
            .expect("name both slices");

        store.clear();
        let report = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(
            (
                report.parts_with_refused_leaves,
                report.leaf_prefix_gets,
                report.parts_rebuilt,
                report.leaves_written,
            ),
            (1, 2, 1, 1),
            "{report:?}"
        );
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        let refs: Vec<&SnapshotKeyIndexLeafRef> = head.parts[0]
            .key_index
            .iter()
            .filter(|r| r.field == USER_ID)
            .collect();
        assert_eq!(refs, vec![&original], "{head:?}");
    }

    /// A logs part whose leaf prefix GET fails with a store error is
    /// deferred: the report counts the GET it issued and the part in
    /// `parts_deferred`, nothing is rebuilt, and HEAD still names the
    /// version-1 ref. A rerun with the fault cleared replaces it.
    ///
    /// A rebuild that skipped a ref whose prefix it could not read reports
    /// nothing deferred and leaves the part looking repaired; one that
    /// propagated the error interrupts the run.
    #[tokio::test]
    async fn a_failed_leaf_prefix_get_defers_the_part_and_a_rerun_replaces_it() {
        let inner = recording_store();
        let (_, _) = two_entry_parts(&inner, 1).await;
        let catalog = off_catalog(inner.clone(), 2);
        rebuild(&catalog, Signal::Logs).await;
        let (original, v1_ref) = swap_in_a_version_one_leaf(&inner, Signal::Logs).await;
        let faulty = Arc::new(FaultStore::new(
            Arc::clone(&inner),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Get, ScriptedFault::Timeout).with_key_contains(v1_ref.key.clone()),
            ),
        ));
        let catalog = off_catalog(faulty.clone(), 2);
        inner.clear();
        let report = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(faulty.fault_count(Op::Get, FaultKind::Timeout), 1);
        assert_eq!(
            (
                report.leaf_prefix_gets,
                report.leaf_prefix_bytes_read,
                report.parts_deferred,
                report.parts_with_refused_leaves,
                report.parts_rebuilt,
                report.leaves_written,
            ),
            (1, 0, 1, 0, 0, 0),
            "{report:?}"
        );
        let head = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts[0].key_index, vec![v1_ref.clone()]);

        let catalog = off_catalog(inner.clone(), 2);
        let rerun = rebuild(&catalog, Signal::Logs).await;
        assert_eq!(
            (
                rerun.parts_deferred,
                rerun.parts_with_refused_leaves,
                rerun.parts_rebuilt
            ),
            (0, 1, 1),
            "{rerun:?}"
        );
        let head = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts[0].key_index, vec![original]);
    }

    /// The switch-off catalog with `metrics_label_index` on and the byte cap
    /// ratio `ratio_ppm`: the metrics rebuild's catalog.
    fn metrics_rebuild_catalog(store: Arc<dyn ObjectStoreBackend>, ratio_ppm: u32) -> Catalog {
        let cfg = CatalogConfig {
            metrics_label_index: true,
            metrics_label_index_max_ratio_ppm: ratio_ppm,
            ..config(1)
        };
        assert!(!cfg.build_key_index_leaves);
        Catalog::new(store, cfg).expect("catalog")
    }

    /// With `metrics_label_index` on, the rebuild's metrics leaf is the
    /// fold's: the six-object fixture folded once with both switches on, and
    /// once with the leaf build off and then rebuilt, gives the same part,
    /// the same `label_value` ref and byte-identical leaf objects. The
    /// rebuild lists the pending erasure set once and reads each entry with
    /// a suffix GET and one LABEL_DICT range GET.
    #[tokio::test]
    async fn a_rebuilt_metrics_leaf_is_byte_identical_to_the_folds_leaf() {
        let folded = recording_store();
        publish_label_fixture(folded.as_ref()).await;
        let on = Catalog::new(folded.clone(), metrics_kidx_config()).expect("catalog");
        fold_through(&on, Signal::Metrics, KIDX_HOUR).await;

        let rebuilt = recording_store();
        publish_label_fixture(rebuilt.as_ref()).await;
        let off = off_catalog(rebuilt.clone(), DEFAULT_SNAPSHOT_PART_MAX_ENTRIES);
        fold_through(&off, Signal::Metrics, KIDX_HOUR).await;
        rebuilt.clear();
        let catalog = metrics_rebuild_catalog(rebuilt.clone(), 10_000);
        let report = rebuild(&catalog, Signal::Metrics).await;
        assert_eq!(report.fields, vec![METRICS_LABEL_VALUE_FIELD.to_string()]);
        assert_eq!(report.parts_rebuilt, 1, "{report:?}");
        assert_eq!(report.erasure_list_requests, 1, "{report:?}");
        assert_eq!(report.erasure_get_requests, 0, "{report:?}");
        assert_eq!(rebuilt.lists(), 1, "the pending erasure listing only");
        assert_eq!(report.ranged_gets, 2 * 6, "{report:?}");
        assert_eq!(rebuilt.data_ranged_gets().len(), 2 * 6);
        assert_eq!(report.uncovered_entries, 0, "{report:?}");

        let fold_head = read_signal_head(folded.as_ref(), Signal::Metrics).await;
        let rebuild_head = read_signal_head(rebuilt.as_ref(), Signal::Metrics).await;
        assert_eq!(fold_head.parts.len(), 1);
        assert_eq!(fold_head.parts[0].blake3, rebuild_head.parts[0].blake3);
        assert_eq!(fold_head.parts[0].key_index.len(), 1);
        assert_eq!(
            fold_head.parts[0].key_index,
            rebuild_head.parts[0].key_index
        );
        for leaf in &fold_head.parts[0].key_index {
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

    /// A pending metrics erasure request ends the rebuild before any part is
    /// read: no data object GET, no PUT, HEAD unchanged, the unindexed part
    /// counted as gated, and the one LIST and the one GET of the request's
    /// `.dreq` object counted.
    #[tokio::test]
    async fn a_pending_erasure_request_gates_the_metrics_rebuild() {
        let store = recording_store();
        publish_label_fixture(store.as_ref()).await;
        put_metrics_erasure_request(store.as_ref()).await;
        let off = off_catalog(store.clone(), DEFAULT_SNAPSHOT_PART_MAX_ENTRIES);
        fold_through(&off, Signal::Metrics, KIDX_HOUR).await;
        let before = read_signal_head(store.as_ref(), Signal::Metrics).await;
        store.clear();
        let report = rebuild(
            &metrics_rebuild_catalog(store.clone(), 10_000),
            Signal::Metrics,
        )
        .await;
        assert_eq!(report.parts_unindexed, 1, "{report:?}");
        assert_eq!(report.parts_erasure_gated, 1, "{report:?}");
        assert_eq!(report.erasure_list_requests, 1, "{report:?}");
        assert_eq!(report.erasure_get_requests, 1, "{report:?}");
        let dreq_gets = store
            .gets()
            .into_iter()
            .filter(|(key, _, _)| key.ends_with(".dreq"))
            .count();
        assert_eq!(dreq_gets, 1, "the one pending request's object");
        assert_eq!(report.parts_rebuilt, 0, "{report:?}");
        assert_eq!(report.attempt_count, 0, "{report:?}");
        assert_eq!(store.lists(), 1);
        assert_eq!(store.data_ranged_gets(), Vec::new());
        assert_eq!(store.puts(), Vec::<String>::new());
        assert_eq!(
            read_signal_head(store.as_ref(), Signal::Metrics).await,
            before
        );
    }

    /// A part whose metrics leaf would pass the byte cap gains no ref: no
    /// leaf PUT, no HEAD publish, the part counted over the cap and every
    /// entry uncovered.
    #[tokio::test]
    async fn a_metrics_part_over_the_cap_gains_no_ref_in_the_rebuild() {
        let store = recording_store();
        publish_label_fixture(store.as_ref()).await;
        let off = off_catalog(store.clone(), DEFAULT_SNAPSHOT_PART_MAX_ENTRIES);
        fold_through(&off, Signal::Metrics, KIDX_HOUR).await;
        let before = read_signal_head(store.as_ref(), Signal::Metrics).await;
        store.clear();
        let report = rebuild(&metrics_rebuild_catalog(store.clone(), 1), Signal::Metrics).await;
        assert_eq!(report.parts_over_cap, 1, "{report:?}");
        assert_eq!(report.uncovered_entries, 6, "{report:?}");
        assert_eq!(report.leaves_written, 0, "{report:?}");
        assert!(
            store.puts().iter().all(|key| !key.ends_with(".kidx")),
            "{:?}",
            store.puts()
        );
        assert_eq!(
            read_signal_head(store.as_ref(), Signal::Metrics).await,
            before
        );
    }
}
