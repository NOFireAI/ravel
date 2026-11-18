//! ADR-4146 decisions 1 and 2: the metrics series index of one re-encoded
//! part, the three fields `series_label`, `series_class` and
//! `class_objects`, built beside `label_value` under
//! [`CatalogConfig::metrics_series_index`].
//!
//! A series key `skey` is `u64::from_be_bytes` of the first 8 bytes of the
//! series' `SeriesId`, the order the router reads a `series_class` key and a
//! `series_label` posting value in. Each covered entry adds every series of
//! its SERIES_IDS to that series' entry set and the pairs of the series'
//! stored label set to its pair set. A class is a distinct entry set,
//! numbered in the order of its smallest member `skey`.

use ravel_codec::key_idx::{Key8, key8_i64, key8_id16};
use ravel_memory::MemoryBudget;

use super::*;
use crate::snapshot_format::{
    KEY_INDEX_LEAF_FORMAT_VERSION_3, KeyIndexPostingEntry, KeyIndexPostingLeafInput,
    METRICS_CLASS_OBJECTS_FIELD, METRICS_SERIES_CLASS_FIELD, METRICS_SERIES_LABEL_FIELD,
    check_key_index_leaf_set, encode_key_index_posting_leaf, restamp_key_index_leaf_version,
    series_label_pair_key8, split_key_index_postings, verify_key_index_leaf,
};

/// RSEG catalog section kinds (docs/segment-format.md) the series build
/// reads by range.
mod section_kind {
    pub(super) const LABEL_DICT: u32 = super::RSEG_LABEL_DICT_KIND;
    pub(super) const SERIES_IDS: u32 = 5;
    pub(super) const SERIES_META: u32 = 6;
    pub(super) const SERIES_IDX: u32 = 8;
    pub(super) const SERIES_META_CHUNKS: u32 = 9;
}

/// The three series-index fields and their key types, in the order
/// [`declared_key_index_fields`] lists them after `label_value`.
pub(crate) const METRICS_SERIES_INDEX_FIELDS: [(&str, KeyType); 3] = [
    (METRICS_SERIES_LABEL_FIELD, KeyType::Bytes),
    (METRICS_SERIES_CLASS_FIELD, KeyType::Id16),
    (METRICS_CLASS_OBJECTS_FIELD, KeyType::I64),
];

/// Whether `field` of `signal` is one of the three series-index fields,
/// which [`build_part_series_index`] builds and carries instead of the
/// generic leaf build.
pub(crate) fn is_series_index_field(signal: Signal, field: &str) -> bool {
    signal == Signal::Metrics
        && METRICS_SERIES_INDEX_FIELDS
            .iter()
            .any(|(name, _)| *name == field)
}

/// The series key of a `SeriesId`: its first 8 bytes, big-endian.
pub(crate) fn series_key(id: &ravel_types::SeriesId) -> u64 {
    u64::from_be_bytes(key8_id16(&id.0))
}

/// One object's series: `(skey, sorted pair key8s)` per series of its
/// SERIES_IDS, or why its entry is uncovered for the three fields.
pub(crate) type EntrySeries = Result<Vec<(u64, Vec<Key8>)>, String>;

/// The series of a decoded catalog: each series' key and the key8 of every
/// pair of its stored label set, `__name__` included.
pub(crate) fn entry_series_of_catalog(series: &[ravel_segment::SeriesEntry]) -> EntrySeries {
    series
        .iter()
        .map(|entry| {
            let mut pairs = entry
                .labels
                .iter()
                .map(|label| series_label_pair_key8(&label.name, &label.value))
                .collect::<Result<Vec<Key8>, _>>()
                .map_err(|e| format!("label pair: {e}"))?;
            pairs.sort_unstable();
            pairs.dedup();
            Ok((series_key(&entry.series_id), pairs))
        })
        .collect()
}

/// Reads a metrics object's series by range: the footer GETs of
/// [`open_rseg_footer_counted`], then one GET spanning its catalog sections
/// (LABEL_DICT, SERIES_IDS and SERIES_META, or SERIES_IDX and
/// SERIES_META_CHUNKS for a sparse object), decoded as the query's catalog
/// probe decodes them.
async fn read_entry_series_counted(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    key: &str,
    object_size: u64,
    budget: &Arc<MemoryBudget>,
    cost: &mut KeyIdxReadCost,
) -> Result<Vec<(u64, Vec<Key8>)>, KeyIdxReadError> {
    let location = open_rseg_footer_counted(store, tenant, key, object_size, cost).await?;
    let footer = &location.footer;
    let section = |kind: u32| footer.sections.iter().find(|s| s.kind == kind);
    let chunked = section(section_kind::SERIES_META_CHUNKS).is_some();
    let kinds: &[u32] = if chunked {
        &[
            section_kind::LABEL_DICT,
            section_kind::SERIES_IDS,
            section_kind::SERIES_IDX,
            section_kind::SERIES_META_CHUNKS,
        ]
    } else {
        &[
            section_kind::LABEL_DICT,
            section_kind::SERIES_IDS,
            section_kind::SERIES_META,
        ]
    };
    let mut ranges = Vec::with_capacity(kinds.len());
    for &kind in kinds {
        let found =
            section(kind).ok_or_else(|| format!("object has no catalog section of kind {kind}"))?;
        let end = found
            .offset
            .checked_add(found.len)
            .ok_or_else(|| format!("catalog section {kind} range overflows"))?;
        ranges.push((found.offset, end));
    }
    let start = ranges.iter().map(|r| r.0).min().unwrap_or(0);
    let end = ranges.iter().map(|r| r.1).max().unwrap_or(0);
    cost.section_gets += 1;
    let got = store
        .get(key, GetRange::Range(start, end))
        .await
        .map_err(|e| KeyIdxReadError::Get(format!("catalog sections GET: {e}")))?;
    cost.section_bytes += got.data.len() as u64;
    if got.data.len() as u64 != end - start {
        return Err(format!(
            "catalog sections GET returned {} bytes, expected {}",
            got.data.len(),
            end - start
        )
        .into());
    }
    let bytes: Vec<&[u8]> = ranges
        .iter()
        .map(|&(lo, hi)| &got.data[(lo - start) as usize..(hi - start) as usize])
        .collect();
    let limits = ReaderLimits::default();
    let mut charge = crate::BudgetCatalogCharge::new(budget);
    let decoded = if chunked {
        ravel_segment::decode_catalog_v5_chunked_charged(
            footer,
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
            limits,
            &mut charge,
        )
    } else {
        ravel_segment::decode_catalog_v4_charged(
            footer,
            bytes[0],
            bytes[1],
            bytes[2],
            limits,
            &mut charge,
        )
    }
    .map_err(|e| format!("catalog decode: {e}"))?;
    let series: Vec<ravel_segment::SeriesEntry> = decoded.into_iter().map(|e| e.entry).collect();
    Ok(entry_series_of_catalog(&series)?)
}

/// What the series build needs beyond the part.
pub(crate) struct SeriesIndexLimits<'a> {
    pub(crate) leaf: KeyIndexLeafLimits,
    pub(crate) concurrency: usize,
    /// Most bytes the three fields' leaves may total for the part.
    pub(crate) leaf_bytes_cap: u64,
    /// Charged with the provenance a ranged catalog decode materializes.
    pub(crate) budget: &'a Arc<MemoryBudget>,
}

/// Why a part's build wrote no series leaf, or that it wrote them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeriesIndexOutcome {
    /// The three fields' refs are in [`BuiltSeriesIndex::refs`].
    Built,
    /// The leaves would total more than the cap, or one key's posting or
    /// entries exceed the leaf body ceiling.
    OverCap,
    /// No entry of the part could be read.
    Uncovered,
    /// The encoder refused the entries.
    Refused,
    /// [`KeyIndexBuildCaller::Repair`] only: a ranged GET failed, so the
    /// repair defers the part. Nothing was PUT.
    Deferred,
}

/// One part's series-index build.
#[derive(Debug, Clone)]
pub(crate) struct BuiltSeriesIndex {
    pub(crate) outcome: SeriesIndexOutcome,
    /// Sorted by (field, key8_lo); empty unless the outcome is `Built`.
    pub(crate) refs: Vec<SnapshotKeyIndexLeafRef>,
    /// (entry, field) pairs the published refs leave uncovered: each
    /// uncovered entry counts once per field, and every entry of the part
    /// three times when no ref is published.
    pub(crate) uncovered_entries: u64,
    /// Per field: postings values for the posting fields, entries for
    /// `class_objects`.
    pub(crate) field_pairs: BTreeMap<String, u64>,
}

/// One series of the build: the part entries holding it and its pairs.
#[derive(Default)]
struct SeriesAcc {
    entries: BTreeSet<u64>,
    pairs: BTreeSet<Key8>,
}

/// A previous part's series fields mapped onto the new part.
struct SeriesCarry {
    /// Per previous ordinal: the new ordinal of the same data object.
    onto: Vec<Option<u64>>,
    /// Previous ordinals any of the three fields lists as uncovered.
    uncovered: HashSet<u64>,
    /// `(skey, previous entries, pairs)` per series of the previous part.
    series: Vec<(u64, Vec<u64>, Vec<Key8>)>,
}

/// GETs and verifies every slice of `source`'s three series fields, then
/// reads each series' previous entries from `series_class` and
/// `class_objects` and its pairs from `series_label`. `Unusable` when a field
/// has no ref, the refs do not tile or do not name the part, a leaf does not
/// verify, the three fields disagree with each other, or the previous part's
/// object keys are not known.
async fn load_series_carry(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    source: &CarrySource,
    ordinal_by_key: &HashMap<&str, u64>,
    limits: &KeyIndexLeafLimits,
    counters: &mut RequestCounters,
) -> Result<SeriesCarry, CarryFailure> {
    let refs = &source.part.key_index;
    if source.object_keys().len() as u64 != source.part.entry_count {
        return Err("previous part entry list does not match its ref"
            .to_string()
            .into());
    }
    let mut summaries: BTreeMap<&str, Vec<KeyIndexLeafSummary>> = BTreeMap::new();
    let mut uncovered: HashSet<u64> = HashSet::new();
    for (name, key_type) in METRICS_SERIES_INDEX_FIELDS {
        let slices: Vec<&SnapshotKeyIndexLeafRef> =
            refs.iter().filter(|leaf| leaf.field == name).collect();
        if slices.is_empty() {
            return Err(format!("previous part has no {name:?} leaf").into());
        }
        if slices
            .iter()
            .any(|leaf| leaf.key_type != u32::from(key_type.as_u8()))
        {
            return Err(format!("previous {name:?} leaf has another key type").into());
        }
        if slices.iter().any(|leaf| leaf.part_blake3 != source.part.blake3) {
            return Err(format!("field {name:?}: a leaf ref names another part").into());
        }
        select_key_index_slice(refs, name, &[0u8; 8])
            .map_err(|e| format!("field {name:?}: {e}"))?;
        let mut verified = Vec::with_capacity(slices.len());
        for leaf in slices {
            let (summary, _) = fetch_verified_leaf(
                store,
                tenant,
                Signal::Metrics,
                &source.part.blake3,
                source.part.entry_count,
                leaf,
                counters,
                |bytes| verify_key_index_leaf(bytes, limits),
            )
            .await?;
            verified.push(summary);
        }
        check_key_index_leaf_set(&verified).map_err(|e| format!("field {name:?}: {e}"))?;
        if let Some(first) = verified.first() {
            uncovered.extend(first.header.uncovered_entry_ordinals.iter().copied());
        }
        summaries.insert(name, verified);
    }

    let mut entries_of_class: HashMap<Key8, Vec<u64>> = HashMap::new();
    for summary in summaries
        .get(METRICS_CLASS_OBJECTS_FIELD)
        .into_iter()
        .flatten()
    {
        for entry in &summary.entries {
            entries_of_class
                .entry(entry.key8)
                .or_default()
                .push(entry.entry_ordinal);
        }
    }
    let mut pairs_of: HashMap<u64, Vec<Key8>> = HashMap::new();
    for summary in summaries
        .get(METRICS_SERIES_LABEL_FIELD)
        .into_iter()
        .flatten()
    {
        for posting in &summary.postings {
            for &skey in &posting.values {
                pairs_of.entry(skey).or_default().push(posting.key8);
            }
        }
    }
    let mut series = Vec::new();
    for summary in summaries
        .get(METRICS_SERIES_CLASS_FIELD)
        .into_iter()
        .flatten()
    {
        for posting in &summary.postings {
            let skey = u64::from_be_bytes(posting.key8);
            let mut entries = Vec::new();
            for &class in &posting.values {
                let class = i64::try_from(class)
                    .map_err(|_| format!("series {skey:#018x} names class {class}"))?;
                let held = entries_of_class.get(&key8_i64(class)).ok_or_else(|| {
                    format!("series {skey:#018x} names class {class}, which holds no entry")
                })?;
                entries.extend(held.iter().copied());
            }
            let pairs = pairs_of
                .remove(&skey)
                .ok_or_else(|| format!("series {skey:#018x} carries no pair"))?;
            series.push((skey, entries, pairs));
        }
    }
    if !pairs_of.is_empty() {
        return Err("a series_label posting names a series with no class"
            .to_string()
            .into());
    }
    let onto = source
        .object_keys()
        .iter()
        .map(|key| {
            key.as_deref()
                .and_then(|key| ordinal_by_key.get(key).copied())
        })
        .collect();
    Ok(SeriesCarry {
        onto,
        uncovered,
        series,
    })
}

// Test-only: per series build, the part ordinals carried from previous
// leaves, read from held bytes, and read by range.
#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SeriesBuildRecord {
    pub(crate) carried: Vec<u64>,
    pub(crate) held: Vec<u64>,
    pub(crate) ranged: Vec<u64>,
}

#[cfg(test)]
thread_local! {
    static SERIES_BUILD_LOG: std::cell::RefCell<Vec<SeriesBuildRecord>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(crate) fn take_series_build_log_for_test() -> Vec<SeriesBuildRecord> {
    SERIES_BUILD_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
}

/// Builds and PUTs one part's three series-index fields (ADR-4146 decision
/// 2), at leaf format version 3.
///
/// Inputs: a source in `sources` that holds all three fields and shares a
/// data object with the part gives, for every entry it covered, the series
/// the entry holds and their pairs, remapped by data object key; a series
/// whose remapped entry set is empty is dropped. Every other entry is read:
/// from `held` with no GET, or by range. An entry whose read or decode
/// fails, or whose data key cannot be derived, is uncovered in all three
/// fields. A data object mapped from two sources drops the whole carry.
///
/// The carried build writes the bytes a build of the same entries from
/// their objects writes, unless two distinct series share an `skey` and one
/// of them is in a previous entry the part no longer holds: the carry then
/// keeps that series' pairs, a superset.
///
/// Nothing is PUT unless the three fields together fit
/// `limits.leaf_bytes_cap` and every key fits the leaf body ceiling.
/// [`KeyIndexBuildCaller::Repair`] passes no source and no held series and
/// is deferred, with nothing PUT, when a ranged GET fails.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_part_series_index(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    part_blake3: [u8; 32],
    part_watermark: u32,
    part_entries: &[SnapshotEntry],
    sources: &[&CarrySource],
    held: HeldPart<'_>,
    limits: &SeriesIndexLimits<'_>,
    caller: KeyIndexBuildCaller,
    counters: &mut RequestCounters,
) -> Result<BuiltSeriesIndex, CatalogError> {
    let signal = Signal::Metrics;
    let n = part_entries.len();
    let part_entry_count = n as u64;
    let data_keys: Vec<Result<String, String>> = part_entries
        .iter()
        .map(|entry| {
            column_stats_build::segment_data_key(tenant, signal, entry)
                .map_err(|e| format!("data key: {e}"))
        })
        .collect();
    let mut uncovered: BTreeSet<u64> = (0..part_entry_count)
        .filter(|&o| data_keys[o as usize].is_err())
        .collect();
    let mut ordinal_by_key: HashMap<&str, u64> = HashMap::new();
    for (ordinal, key) in data_keys.iter().enumerate() {
        if let Ok(key) = key {
            ordinal_by_key.entry(key.as_str()).or_insert(ordinal as u64);
        }
    }
    let mut series: BTreeMap<u64, SeriesAcc> = BTreeMap::new();
    let mut carried = vec![false; n];
    #[cfg(test)]
    let mut record = SeriesBuildRecord::default();

    let sharing: Vec<&CarrySource> = sources
        .iter()
        .copied()
        .filter(|source| {
            METRICS_SERIES_INDEX_FIELDS
                .iter()
                .all(|(name, _)| source.part.key_index.iter().any(|l| l.field == *name))
                && source
                    .object_keys()
                    .iter()
                    .flatten()
                    .any(|key| ordinal_by_key.contains_key(key.as_str()))
        })
        .collect();
    let mut mapped: HashSet<&str> = HashSet::new();
    let duplicate = sharing.iter().find_map(|source| {
        source
            .object_keys()
            .iter()
            .flatten()
            .filter(|key| ordinal_by_key.contains_key(key.as_str()))
            .find(|key| !mapped.insert(key.as_str()))
    });
    if let Some(key) = duplicate {
        tracing::warn!(
            tenant = %tenant.to_hex(),
            object_key = %key,
            "a data object is listed twice in the previous parts; the series index carries no previous leaf and reads every entry"
        );
    } else {
        for source in sharing {
            let carry = match load_series_carry(
                store,
                tenant,
                source,
                &ordinal_by_key,
                &limits.leaf,
                counters,
            )
            .await
            {
                Ok(carry) => carry,
                Err(failure) => {
                    let reason = match failure.into_fold_error() {
                        Ok(err) => return Err(err),
                        Err(reason) => reason,
                    };
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        part = %source.part.key,
                        reason = %reason,
                        "previous part series leaves not carried; the series index reads that part's entries"
                    );
                    continue;
                }
            };
            for (previous, new) in carry.onto.iter().enumerate() {
                if let Some(new) = new
                    && !carry.uncovered.contains(&(previous as u64))
                    && let Some(slot) = carried.get_mut(*new as usize)
                {
                    *slot = true;
                }
            }
            for (skey, previous_entries, pairs) in carry.series {
                let entries: Vec<u64> = previous_entries
                    .iter()
                    .filter(|&&previous| !carry.uncovered.contains(&previous))
                    .filter_map(|&previous| carry.onto.get(previous as usize).copied().flatten())
                    .collect();
                if entries.is_empty() {
                    continue;
                }
                let acc = series.entry(skey).or_default();
                acc.entries.extend(entries);
                acc.pairs.extend(pairs);
            }
        }
    }

    let to_read: Vec<(u64, String, u64)> = (0..part_entry_count)
        .filter(|&o| !carried[o as usize])
        .filter_map(|o| {
            let key = data_keys[o as usize].as_ref().ok()?;
            Some((o, key.clone(), part_entries[o as usize].object_size))
        })
        .collect();
    #[cfg(test)]
    {
        record.carried = (0..part_entry_count)
            .filter(|&o| carried[o as usize])
            .collect();
    }
    let mut get_failed = false;
    {
        let targets = &to_read;
        let mut reads = stream::iter(0..targets.len())
            .map(move |i| {
                let (ordinal, key, object_size) = &targets[i];
                let held = held.get_series(*ordinal).cloned();
                async move {
                    match held {
                        Some(series) => (series, false, false, KeyIdxReadCost::default()),
                        None => {
                            let mut cost = KeyIdxReadCost::default();
                            let read = read_entry_series_counted(
                                store,
                                tenant,
                                key,
                                *object_size,
                                limits.budget,
                                &mut cost,
                            )
                            .await;
                            let (series, get_failed) = match read {
                                Ok(series) => (Ok(series), false),
                                Err(KeyIdxReadError::Get(reason)) => (Err(reason), true),
                                Err(KeyIdxReadError::Invalid(reason)) => (Err(reason), false),
                            };
                            (series, get_failed, true, cost)
                        }
                    }
                }
            })
            .buffered(limits.concurrency.max(1));
        let mut next = 0usize;
        while let Some((read, read_get_failed, ranged, cost)) = reads.next().await {
            let (ordinal, key, _) = &targets[next];
            let ordinal = *ordinal;
            next += 1;
            #[cfg(test)]
            if ranged {
                record.ranged.push(ordinal);
            } else {
                record.held.push(ordinal);
            }
            #[cfg(not(test))]
            let _ = ranged;
            counters.get_requests += cost.gets();
            counters.key_index_ranged_gets += cost.gets();
            counters.key_index_bytes_read += cost.bytes();
            counters.key_index_footer_gets += cost.footer_gets;
            counters.key_index_footer_bytes_read += cost.footer_bytes;
            counters.key_index_section_gets += cost.section_gets;
            counters.key_index_section_bytes_read += cost.section_bytes;
            match read {
                Ok(list) => {
                    for (skey, pairs) in list {
                        let acc = series.entry(skey).or_default();
                        acc.entries.insert(ordinal);
                        acc.pairs.extend(pairs);
                    }
                }
                Err(reason) => {
                    if read_get_failed {
                        get_failed = true;
                    }
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        object_key = %key,
                        reason = %reason,
                        "series catalog read failed; the entry is uncovered by this part's series index"
                    );
                    uncovered.insert(ordinal);
                }
            }
        }
    }
    #[cfg(test)]
    SERIES_BUILD_LOG.with(|log| log.borrow_mut().push(record));

    let not_built = |outcome| BuiltSeriesIndex {
        outcome,
        refs: Vec::new(),
        uncovered_entries: part_entry_count * METRICS_SERIES_INDEX_FIELDS.len() as u64,
        field_pairs: BTreeMap::new(),
    };
    if get_failed && caller == KeyIndexBuildCaller::Repair {
        return Ok(not_built(SeriesIndexOutcome::Deferred));
    }
    if uncovered.len() as u64 == part_entry_count {
        return Ok(not_built(SeriesIndexOutcome::Uncovered));
    }

    let fields = series_index_entries(series, &uncovered);
    let encoded = match encode_series_index(tenant, part_blake3, part_entry_count, &fields, limits)
    {
        Ok(encoded) => encoded,
        Err(KeyIndexLeafEncodeError::BodyOverCeiling { body, ceiling }) => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                body,
                ceiling,
                "a series-index key exceeds the key-index leaf body ceiling; the part gets no series index"
            );
            return Ok(not_built(SeriesIndexOutcome::OverCap));
        }
        Err(err) => {
            tracing::error!(
                tenant = %tenant.to_hex(),
                error = %err,
                "series-index leaf encode refused; the part gets no series index"
            );
            return Ok(not_built(SeriesIndexOutcome::Refused));
        }
    };
    let leaf_bytes: u64 = encoded.iter().map(|leaf| leaf.bytes.len() as u64).sum();
    if leaf_bytes > limits.leaf_bytes_cap {
        tracing::warn!(
            tenant = %tenant.to_hex(),
            leaf_bytes,
            cap = limits.leaf_bytes_cap,
            "the part's series-index leaves would exceed the byte cap; the part gets no series index"
        );
        return Ok(not_built(SeriesIndexOutcome::OverCap));
    }
    let mut refs = Vec::with_capacity(encoded.len());
    for leaf in encoded {
        let leaf_bytes = leaf.bytes.len() as u64;
        let put_result = put_key_index_leaf(store, tenant, signal, part_watermark, leaf).await;
        counters.put_requests += 1;
        counters.key_index_leaf_bytes_put += leaf_bytes;
        refs.push(put_result?);
    }
    refs.sort_by(|a, b| (&a.field, a.key8_lo).cmp(&(&b.field, b.key8_lo)));
    Ok(BuiltSeriesIndex {
        outcome: SeriesIndexOutcome::Built,
        refs,
        uncovered_entries: uncovered.len() as u64 * METRICS_SERIES_INDEX_FIELDS.len() as u64,
        field_pairs: fields.pairs(),
    })
}

/// The three fields' sorted entries, before slicing.
pub(crate) struct SeriesIndexEntries {
    pub(crate) series_label: Vec<KeyIndexPostingEntry>,
    pub(crate) series_class: Vec<KeyIndexPostingEntry>,
    pub(crate) class_objects: Vec<KeyIndexLeafEntry>,
    pub(crate) uncovered: Vec<u64>,
}

impl SeriesIndexEntries {
    fn pairs(&self) -> BTreeMap<String, u64> {
        let values = |postings: &[KeyIndexPostingEntry]| {
            postings.iter().map(|p| p.values.len() as u64).sum::<u64>()
        };
        BTreeMap::from([
            (
                METRICS_SERIES_LABEL_FIELD.to_string(),
                values(&self.series_label),
            ),
            (
                METRICS_SERIES_CLASS_FIELD.to_string(),
                values(&self.series_class),
            ),
            (
                METRICS_CLASS_OBJECTS_FIELD.to_string(),
                self.class_objects.len() as u64,
            ),
        ])
    }
}

/// The three fields of `series`, dropping every series no entry holds: the
/// classes are the distinct entry sets, numbered in ascending order of their
/// smallest member `skey`; `series_label` maps each pair to its ascending
/// `skey`s, `series_class` each `skey` to its class, and `class_objects`
/// each class to its entries at block 0.
fn series_index_entries(
    series: BTreeMap<u64, SeriesAcc>,
    uncovered: &BTreeSet<u64>,
) -> SeriesIndexEntries {
    let mut class_of_set: HashMap<Vec<u64>, u64> = HashMap::new();
    let mut class_sets: Vec<Vec<u64>> = Vec::new();
    let mut pair_postings: BTreeMap<Key8, Vec<u64>> = BTreeMap::new();
    let mut series_class = Vec::new();
    for (skey, acc) in series {
        if acc.entries.is_empty() {
            continue;
        }
        let set: Vec<u64> = acc.entries.into_iter().collect();
        let class = match class_of_set.get(&set) {
            Some(&class) => class,
            None => {
                let class = class_sets.len() as u64;
                class_of_set.insert(set.clone(), class);
                class_sets.push(set);
                class
            }
        };
        series_class.push(KeyIndexPostingEntry {
            key8: skey.to_be_bytes(),
            values: vec![class],
        });
        for pair in acc.pairs {
            pair_postings.entry(pair).or_default().push(skey);
        }
    }
    let series_label = pair_postings
        .into_iter()
        .map(|(key8, values)| KeyIndexPostingEntry { key8, values })
        .collect();
    let mut class_objects: Vec<KeyIndexLeafEntry> = class_sets
        .iter()
        .enumerate()
        .flat_map(|(class, set)| {
            set.iter().map(move |&entry_ordinal| KeyIndexLeafEntry {
                key8: key8_i64(class as i64),
                entry_ordinal,
                blocks: vec![0],
            })
        })
        .collect();
    class_objects.sort_by_key(|e| (e.key8, e.entry_ordinal));
    SeriesIndexEntries {
        series_label,
        series_class,
        class_objects,
        uncovered: uncovered.iter().copied().collect(),
    }
}

/// Slices and encodes the three fields: the two posting fields at version 3
/// by their own encoder, and `class_objects` by the kind-0 encoder,
/// restamped to version 3.
fn encode_series_index(
    tenant: &TenantHash,
    part_blake3: [u8; 32],
    part_entry_count: u64,
    fields: &SeriesIndexEntries,
    limits: &SeriesIndexLimits<'_>,
) -> Result<Vec<EncodedKeyIndexLeaf>, KeyIndexLeafEncodeError> {
    let mut leaves = Vec::new();
    for (field, key_type, postings) in [
        (
            METRICS_SERIES_LABEL_FIELD,
            KeyType::Bytes,
            &fields.series_label,
        ),
        (
            METRICS_SERIES_CLASS_FIELD,
            KeyType::Id16,
            &fields.series_class,
        ),
    ] {
        for slice in split_key_index_postings(postings, limits.leaf.max_body_bytes)? {
            leaves.push(encode_key_index_posting_leaf(
                &KeyIndexPostingLeafInput {
                    tenant_hash: *tenant,
                    signal: Signal::Metrics,
                    part_blake3,
                    part_entry_count,
                    field,
                    key_type,
                    key8_lo: slice.key8_lo,
                    key8_hi: slice.key8_hi,
                    uncovered_entry_ordinals: &fields.uncovered,
                    postings: &postings[slice.entries],
                },
                &limits.leaf,
            )?);
        }
    }
    for slice in split_key_index_entries(&fields.class_objects, limits.leaf.max_body_bytes)? {
        let leaf = encode_key_index_leaf(
            &KeyIndexLeafInput {
                tenant_hash: *tenant,
                signal: Signal::Metrics,
                part_blake3,
                part_entry_count,
                field: METRICS_CLASS_OBJECTS_FIELD,
                key_type: KeyType::I64,
                key8_lo: slice.key8_lo,
                key8_hi: slice.key8_hi,
                uncovered_entry_ordinals: &fields.uncovered,
                entries: &fields.class_objects[slice.entries],
            },
            &limits.leaf,
        )?;
        leaves.push(
            restamp_key_index_leaf_version(&leaf.bytes, KEY_INDEX_LEAF_FORMAT_VERSION_3)
                .map_err(|e| KeyIndexLeafEncodeError::InvalidInput(e.to_string()))?,
        );
    }
    Ok(leaves)
}
