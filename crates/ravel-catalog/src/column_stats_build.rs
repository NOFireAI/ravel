//! Fold-time exact per-object column statistics for configured typed logs
//! attribute columns (ADR-0850). Sibling artifact to the name-postings index
//! (`fold.rs`'s `build_postings`/`fetch_segment_names`): same identity model,
//! deliberately different failure discipline.
//!
//! A single segment's fetch, decode, or tally failure drops ONLY that
//! segment from the output (it simply gets no `ColumnStatsSegment` entry,
//! which the query-time reader treats as "no stats for this segment, force
//! scan fallback") -- never aborts the whole fold's column-stats artifact.
//! This is the opposite of `build_postings`'s all-or-nothing discipline, by
//! design: a name-postings index is one flat cross-segment structure with no
//! meaningful notion of "partially built", while column statistics are
//! naturally per-segment and independently useful.
//!
//! Only L0 entries carry real writer identity (`fold.rs`'s
//! `build_l1_snapshot_entry`: an L1 entry's writer_id/writer_epoch slots are
//! repurposed for input_set_hash/part_index), so column-stats coverage is
//! restricted to `entry.level == 0`. An L1 segment simply has no
//! `ColumnStatsSegment` entry and its queries fall back to scanning, exactly
//! like any other segment lacking stats.

use ravel_logseg::{ColumnSelection, FieldType, LogSegError, Predicate, RlogConfig, RlogReader};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue, DictEntry};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::snapshot_format::{DEFAULT_MAX_COLUMN_DICTIONARY_ENTRIES, SnapshotFormatError};
use crate::tenant_config::{DeclaredColumnType, DeclaredTypedColumn};

/// A failure building one segment's column statistics. Every variant is
/// caught by the caller, logged, and turned into "this segment has no
/// column-stats entry" -- never surfaced as a [`crate::error::CatalogError`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum ColumnStatsBuildError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("key error: {0}")]
    Key(#[from] ravel_commit::keys::KeyError),
    #[error("log segment error: {0}")]
    LogSeg(#[from] LogSegError),
    #[error("entry content_hash must be 32 bytes, got {0}")]
    BadContentHashLen(usize),
    #[error("entry writer_id must be 16 bytes, got {0}")]
    BadWriterIdLen(usize),
    #[error("column {name:?} declared Str but stored bytes are not valid UTF-8")]
    NonUtf8StrValue { name: String },
}

/// Maps [`DeclaredColumnType`] to the `ravel.sys.v1.TypedAttrColumnType`
/// proto tag `ColumnStat.declared_type` stores (same convention as
/// `SnapshotPartHeader.signal`: an i32 tag with no cross-file proto import).
/// `DeclaredColumnType::to_proto` in `tenant_config.rs` is module-private, so
/// this duplicates its match arms rather than reusing it. Kept strictly
/// distinct from [`declared_type_to_field_type`]: the two enums this
/// function and that one map to are numbered differently (BOOL=3/BYTES=4
/// here vs. Bool=4/Bytes=5 there) and must never be conflated.
fn declared_type_to_stats_tag(ty: DeclaredColumnType) -> u32 {
    match ty {
        DeclaredColumnType::Str => 1,
        DeclaredColumnType::I64 => 2,
        DeclaredColumnType::Bool => 3,
        DeclaredColumnType::Bytes => 4,
    }
}

/// Maps [`DeclaredColumnType`] to the [`ravel_logseg::FieldType`] used to
/// resolve a configured column against a logs object's `FIELD_DIR` via
/// [`ravel_logseg::ColumnarBlockView::resolve_attr`]. See
/// [`declared_type_to_stats_tag`] for why this is a separate mapping.
fn declared_type_to_field_type(ty: DeclaredColumnType) -> FieldType {
    match ty {
        DeclaredColumnType::Str => FieldType::Str,
        DeclaredColumnType::I64 => FieldType::I64,
        DeclaredColumnType::Bool => FieldType::Bool,
        DeclaredColumnType::Bytes => FieldType::Bytes,
    }
}

fn column_value_i64(v: i64) -> ColumnValue {
    ColumnValue {
        kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(v)),
    }
}

fn column_value_bool(v: bool) -> ColumnValue {
    ColumnValue {
        kind: Some(ravel_proto::catalog::v1::column_value::Kind::B(v)),
    }
}

fn column_value_str(v: String) -> ColumnValue {
    ColumnValue {
        kind: Some(ravel_proto::catalog::v1::column_value::Kind::StrUtf8(v)),
    }
}

fn column_value_bytes(v: Vec<u8>) -> ColumnValue {
    ColumnValue {
        kind: Some(ravel_proto::catalog::v1::column_value::Kind::BytesVal(v)),
    }
}

/// Folds `value` into `min`/`max` (exact, always tracked) and into `dict`
/// (exact up to [`DEFAULT_MAX_COLUMN_DICTIONARY_ENTRIES`] distinct values,
/// after which `dict` is permanently cleared to `None` -- never truncated to
/// a partial, silently-wrong-looking dictionary).
fn fold_value<T: Ord + Clone>(
    min: &mut Option<T>,
    max: &mut Option<T>,
    dict: &mut Option<std::collections::BTreeMap<T, u64>>,
    value: T,
) {
    match min {
        Some(m) if *m <= value => {}
        _ => *min = Some(value.clone()),
    }
    match max {
        Some(m) if *m >= value => {}
        _ => *max = Some(value.clone()),
    }
    if let Some(map) = dict {
        *map.entry(value).or_insert(0) += 1;
        if map.len() > DEFAULT_MAX_COLUMN_DICTIONARY_ENTRIES {
            *dict = None;
        }
    }
}

enum TallyState {
    I64 {
        min: Option<i64>,
        max: Option<i64>,
        dict: Option<std::collections::BTreeMap<i64, u64>>,
    },
    Bool {
        min: Option<bool>,
        max: Option<bool>,
        dict: Option<std::collections::BTreeMap<bool, u64>>,
    },
    Str {
        min: Option<String>,
        max: Option<String>,
        dict: Option<std::collections::BTreeMap<String, u64>>,
    },
    Bytes {
        min: Option<Vec<u8>>,
        max: Option<Vec<u8>>,
        dict: Option<std::collections::BTreeMap<Vec<u8>, u64>>,
    },
}

struct ColumnTally {
    ty: DeclaredColumnType,
    non_null_count: u64,
    null_count: u64,
    state: TallyState,
}

impl ColumnTally {
    fn new(ty: DeclaredColumnType) -> Self {
        let state = match ty {
            DeclaredColumnType::I64 => TallyState::I64 {
                min: None,
                max: None,
                dict: Some(std::collections::BTreeMap::new()),
            },
            DeclaredColumnType::Bool => TallyState::Bool {
                min: None,
                max: None,
                dict: Some(std::collections::BTreeMap::new()),
            },
            DeclaredColumnType::Str => TallyState::Str {
                min: None,
                max: None,
                dict: Some(std::collections::BTreeMap::new()),
            },
            DeclaredColumnType::Bytes => TallyState::Bytes {
                min: None,
                max: None,
                dict: Some(std::collections::BTreeMap::new()),
            },
        };
        Self {
            ty,
            non_null_count: 0,
            null_count: 0,
            state,
        }
    }

    fn observe_i64(&mut self, v: Option<i64>) {
        match v {
            None => self.null_count += 1,
            Some(x) => {
                self.non_null_count += 1;
                if let TallyState::I64 { min, max, dict } = &mut self.state {
                    fold_value(min, max, dict, x);
                }
            }
        }
    }

    fn observe_bool(&mut self, v: Option<bool>) {
        match v {
            None => self.null_count += 1,
            Some(x) => {
                self.non_null_count += 1;
                if let TallyState::Bool { min, max, dict } = &mut self.state {
                    fold_value(min, max, dict, x);
                }
            }
        }
    }

    fn observe_bytes(&mut self, v: Option<&[u8]>) {
        match v {
            None => self.null_count += 1,
            Some(x) => {
                self.non_null_count += 1;
                if let TallyState::Bytes { min, max, dict } = &mut self.state {
                    fold_value(min, max, dict, x.to_vec());
                }
            }
        }
    }

    /// Str columns are stored the same way Bytes columns are
    /// (`ColumnarBlockView::bytes_at` unifies string-dictionary and
    /// fixed-bytes storage), so the raw cell is validated as UTF-8 here
    /// before folding. A configured Str column whose stored bytes are not
    /// valid UTF-8 indicates the object's declared type disagrees with what
    /// was actually written; this aborts just this segment's build (the
    /// caller drops it entirely), never emits a lossy or reinterpreted
    /// value.
    fn observe_str(&mut self, v: Option<&[u8]>, name: &str) -> Result<(), ColumnStatsBuildError> {
        match v {
            None => {
                self.null_count += 1;
                Ok(())
            }
            Some(x) => {
                let s = std::str::from_utf8(x)
                    .map_err(|_| ColumnStatsBuildError::NonUtf8StrValue {
                        name: name.to_string(),
                    })?
                    .to_string();
                self.non_null_count += 1;
                if let TallyState::Str { min, max, dict } = &mut self.state {
                    fold_value(min, max, dict, s);
                }
                Ok(())
            }
        }
    }

    fn into_proto(self, name: String) -> ColumnStat {
        let declared_type = declared_type_to_stats_tag(self.ty);
        let (min, max, dictionary_present, dictionary) = match self.state {
            TallyState::I64 { min, max, dict } => (
                min.map(column_value_i64),
                max.map(column_value_i64),
                dict.is_some(),
                dict.unwrap_or_default()
                    .into_iter()
                    .map(|(v, count)| DictEntry {
                        value: Some(column_value_i64(v)),
                        count,
                    })
                    .collect(),
            ),
            TallyState::Bool { min, max, dict } => (
                min.map(column_value_bool),
                max.map(column_value_bool),
                dict.is_some(),
                dict.unwrap_or_default()
                    .into_iter()
                    .map(|(v, count)| DictEntry {
                        value: Some(column_value_bool(v)),
                        count,
                    })
                    .collect(),
            ),
            TallyState::Str { min, max, dict } => (
                min.map(column_value_str),
                max.map(column_value_str),
                dict.is_some(),
                dict.unwrap_or_default()
                    .into_iter()
                    .map(|(v, count)| DictEntry {
                        value: Some(column_value_str(v)),
                        count,
                    })
                    .collect(),
            ),
            TallyState::Bytes { min, max, dict } => (
                min.map(column_value_bytes),
                max.map(column_value_bytes),
                dict.is_some(),
                dict.unwrap_or_default()
                    .into_iter()
                    .map(|(v, count)| DictEntry {
                        value: Some(column_value_bytes(v)),
                        count,
                    })
                    .collect(),
            ),
        };
        ColumnStat {
            name,
            declared_type,
            non_null_count: self.non_null_count,
            null_count: self.null_count,
            min,
            max,
            dictionary_present,
            dictionary,
        }
    }
}

/// Fetches one L0 logs segment and tallies exact per-column statistics for
/// every column in `typed_columns`, over every row in the object (no content
/// filter: `Predicate::And(vec![])` is vacuously true, matching the
/// full-object scan `LogsScanExec`'s metadata-only path already relies on
/// for `COUNT(*)`).
///
/// A configured column absent from this object's `FIELD_DIR` -- because no
/// row ever set it, or because it overflowed into `attrs_raw` (the overflow
/// decision is made once per `(name, type)` for the whole object, so the two
/// cases are indistinguishable from here) -- gets no entry in the returned
/// segment's `columns` list at all, never a zero-filled stand-in: the reader
/// treats a missing column entry as "no stats for this segment, force
/// fallback," which is the only safe answer when overflow can't be ruled
/// out.
pub(crate) async fn fetch_segment_column_stats(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    entry: &ravel_proto::catalog::v1::SnapshotEntry,
    typed_columns: &[DeclaredTypedColumn],
) -> Result<ColumnStatsSegment, ColumnStatsBuildError> {
    debug_assert_eq!(entry.level, 0, "column stats are only built for L0 entries");

    let content_hash: [u8; 32] = entry
        .content_hash
        .as_slice()
        .try_into()
        .map_err(|_| ColumnStatsBuildError::BadContentHashLen(entry.content_hash.len()))?;
    let writer_id_bytes: [u8; 16] = entry
        .writer_id
        .as_slice()
        .try_into()
        .map_err(|_| ColumnStatsBuildError::BadWriterIdLen(entry.writer_id.len()))?;
    let writer_id = Uuid::from_bytes(writer_id_bytes);
    let data_key = ravel_commit::keys::data_key(
        tenant,
        signal,
        entry.shard,
        writer_id,
        entry.writer_epoch,
        entry.writer_seq,
        &content_hash,
    )?;
    let got = store.get(&data_key, GetRange::Full).await?;

    let cfg = RlogConfig::default();
    let reader = RlogReader::new(&got.data, &cfg)?;

    let mut selection = ColumnSelection::fixed_only();
    for col in typed_columns {
        selection = selection.with_attr(col.key.clone());
    }

    let mut tallies: std::collections::BTreeMap<String, ColumnTally> =
        std::collections::BTreeMap::new();
    let mut scan = reader.scan_blocks(&Predicate::And(vec![]), &[], &selection)?;
    while let Some(view) = scan.next_block_columnar(&got.data)? {
        for col in typed_columns {
            let field_ty = declared_type_to_field_type(col.ty);
            let Some(attr) = view.resolve_attr(&col.key, field_ty) else {
                continue;
            };
            let tally = tallies
                .entry(col.key.clone())
                .or_insert_with(|| ColumnTally::new(col.ty));
            match col.ty {
                DeclaredColumnType::I64 => {
                    for v in view.iter_i64(attr.column_id) {
                        tally.observe_i64(v);
                    }
                }
                DeclaredColumnType::Bool => {
                    for v in view.iter_bool(attr.column_id) {
                        tally.observe_bool(v);
                    }
                }
                DeclaredColumnType::Str => {
                    for v in view.iter_bytes(attr.column_id) {
                        tally.observe_str(v, &col.key)?;
                    }
                }
                DeclaredColumnType::Bytes => {
                    for v in view.iter_bytes(attr.column_id) {
                        tally.observe_bytes(v);
                    }
                }
            }
        }
    }

    let columns = tallies
        .into_iter()
        .map(|(name, tally)| tally.into_proto(name))
        .collect();

    Ok(ColumnStatsSegment {
        ingest_hour_bucket: entry.ingest_hour_bucket,
        shard: entry.shard,
        writer_id: entry.writer_id.clone(),
        writer_epoch: entry.writer_epoch,
        writer_seq: entry.writer_seq,
        columns,
    })
}

/// Decodes and part-binding-checks the previous fold's column-stats
/// baseline. An `Err` here means "reuse nothing, rebuild every segment's
/// stats from scratch" -- the same fallback discipline `load_previous_postings`
/// applies, just without a cache layer (column stats have no
/// `postings_cache()`-equivalent: they are fetched fresh from the store every
/// fold).
pub(crate) fn decode_previous_column_stats(
    bytes: &[u8],
    expected_part_blake3: &[[u8; 32]],
    limits: &crate::snapshot_format::ColumnStatsLimits,
) -> Result<Vec<ColumnStatsSegment>, SnapshotFormatError> {
    let decoded = crate::snapshot_format::decode_column_stats(bytes, limits)?;
    let actual: Result<Vec<[u8; 32]>, _> = decoded
        .header
        .part_blake3
        .iter()
        .map(|h| <[u8; 32]>::try_from(h.as_slice()))
        .collect();
    let actual = actual.map_err(|_| SnapshotFormatError::ColumnStatsPartBindingMismatch)?;
    if actual != expected_part_blake3 {
        return Err(SnapshotFormatError::ColumnStatsPartBindingMismatch);
    }
    Ok(decoded.segments)
}
