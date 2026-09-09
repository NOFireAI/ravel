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
//! ADR-0942 extends coverage to L1: [`fetch_segment_column_stats`] builds a
//! tally for an L0 or an L1 entry, reconstructing the L1 part key from the
//! writer_id/writer_epoch slots an L1 entry repurposes for
//! input_set_hash/part_index (`fold.rs`'s `build_l1_snapshot_entry`). The
//! returned [`ColumnStatsSegment`] carries the five-field identity tuple as the
//! builder saw it (`writer_id` is the entry's own writer_id). The fold's frozen
//! v1 (field-11) publish path uses it unchanged, so a v1 object stays
//! byte-for-byte as before; the v2 (field-13, part-keyed) publish path
//! overwrites `writer_id` with the covered part's content hash (the v2 join
//! key) before encoding. Both records come from one read of the object, via
//! [`SegmentColumnStatsCache`].

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
    #[error("L0 entry writer_id must be 16 bytes, got {0}")]
    BadWriterIdLen(usize),
    #[error("L1 entry writer_id (input_set_hash) must be 32 bytes, got {0}")]
    BadInputSetHashLen(usize),
    #[error("L1 entry writer_epoch (part_index) does not fit u32: {0}")]
    BadPartIndex(u64),
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
        /// Running exact sum of the non-null values, accumulated in `i128` so
        /// the fold itself never overflows (#861). Lowered to the proto's
        /// `i64` sum at [`ColumnTally::into_proto`]; a value that does not fit
        /// `i64` there is emitted as an ABSENT sum, not a truncated one, so a
        /// reader falls back to scanning rather than reading a wrong total.
        sum: i128,
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
                sum: 0,
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
                if let TallyState::I64 {
                    min,
                    max,
                    dict,
                    sum,
                } = &mut self.state
                {
                    fold_value(min, max, dict, x);
                    *sum += i128::from(x);
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
        // Integer sum for an I64 column only (#861), and only when the exact
        // `i128` fold fits `i64`; an overflow emits an absent sum so the reader
        // scans rather than reading a truncated total. Every non-I64 type is
        // left `None`: a float fold would be order-dependent (ADR-0024), and
        // Bool/Bytes/Str carry no additive semantics.
        let mut sum: Option<i64> = None;
        let (min, max, dictionary_present, dictionary) = match self.state {
            TallyState::I64 {
                min,
                max,
                dict,
                sum: total,
            } => {
                sum = i64::try_from(total).ok();
                (
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
                )
            }
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
            sum,
        }
    }
}

/// Fetches one logs segment (L0 or L1, ADR-0942) and tallies exact per-column
/// statistics for every column in `typed_columns`, over every row in the
/// object (no content filter: `Predicate::And(vec![])` is vacuously true,
/// matching the full-object scan `LogsScanExec`'s metadata-only path already
/// relies on for `COUNT(*)`).
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
    let data_key = segment_data_key(tenant, signal, entry)?;
    let columns = tally_object_columns(store, &data_key, typed_columns).await?;
    Ok(segment_from_columns(entry, columns))
}

/// The key of the RLOG object `entry` addresses.
fn segment_data_key(
    tenant: &TenantHash,
    signal: Signal,
    entry: &ravel_proto::catalog::v1::SnapshotEntry,
) -> Result<String, ColumnStatsBuildError> {
    let content_hash: [u8; 32] = entry
        .content_hash
        .as_slice()
        .try_into()
        .map_err(|_| ColumnStatsBuildError::BadContentHashLen(entry.content_hash.len()))?;
    // ADR-0942: both L0 and L1 entries are covered. An L0 entry carries the
    // 16-byte flush writer uuid and addresses its RLOG object by the L0 data
    // key; an L1 (compaction/rewrite output) entry repurposes the writer_*
    // slots (`fold::build_l1_snapshot_entry`) to carry the 32-byte
    // input_set_hash and the part_index, and addresses its RLOG part object by
    // the reconstructed L1 part key. Both object shapes are RLOG and decode
    // through the same reader, so the tally path itself is level-agnostic.
    if entry.level == 0 {
        let writer_id_bytes: [u8; 16] = entry
            .writer_id
            .as_slice()
            .try_into()
            .map_err(|_| ColumnStatsBuildError::BadWriterIdLen(entry.writer_id.len()))?;
        let writer_id = Uuid::from_bytes(writer_id_bytes);
        ravel_commit::keys::data_key(
            tenant,
            signal,
            entry.shard,
            writer_id,
            entry.writer_epoch,
            entry.writer_seq,
            &content_hash,
        )
        .map_err(ColumnStatsBuildError::from)
    } else {
        let input_set_hash: [u8; 32] = entry
            .writer_id
            .as_slice()
            .try_into()
            .map_err(|_| ColumnStatsBuildError::BadInputSetHashLen(entry.writer_id.len()))?;
        let part_index = u32::try_from(entry.writer_epoch)
            .map_err(|_| ColumnStatsBuildError::BadPartIndex(entry.writer_epoch))?;
        let input_set_hash16 = hex::encode(&input_set_hash[..8]);
        let hash16 = hex::encode(&content_hash[..8]);
        ravel_commit::keys::l1_part_key(
            tenant,
            signal,
            entry.shard,
            entry.ingest_hour_bucket,
            &input_set_hash16,
            part_index,
            &hash16,
        )
        .map_err(ColumnStatsBuildError::from)
    }
}

/// Reads the object at `data_key` and tallies one [`ColumnStat`] per
/// `typed_columns` entry the object actually carries. Depends only on the
/// object's bytes and `typed_columns`, which is what makes the per-object
/// memoization in [`SegmentColumnStatsCache`] exact.
async fn tally_object_columns(
    store: &dyn ObjectStoreBackend,
    data_key: &str,
    typed_columns: &[DeclaredTypedColumn],
) -> Result<Vec<ColumnStat>, ColumnStatsBuildError> {
    let got = store.get(data_key, GetRange::Full).await?;

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

    Ok(tallies
        .into_iter()
        .map(|(name, tally)| tally.into_proto(name))
        .collect())
}

/// Wraps one object's tallied columns in the record shape the fold publishes:
/// the five-field identity tuple as `entry` carries it (see this module's docs
/// for how the v1 and v2 publish paths each treat `writer_id`).
fn segment_from_columns(
    entry: &ravel_proto::catalog::v1::SnapshotEntry,
    columns: Vec<ColumnStat>,
) -> ColumnStatsSegment {
    ColumnStatsSegment {
        ingest_hour_bucket: entry.ingest_hour_bucket,
        shard: entry.shard,
        writer_id: entry.writer_id.clone(),
        writer_epoch: entry.writer_epoch,
        writer_seq: entry.writer_seq,
        columns,
    }
}

/// Whether a [`SegmentColumnStatsCache`] lookup issued a store GET, so the
/// caller charges `get_requests` for the reads that really happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatsFetch {
    Issued,
    Reused,
}

/// One fold's per-object column-statistics tallies, so the dual publish reads
/// each covered object once (#964).
///
/// The v1 (field 11) publish covers the L0 entries and the v2 (field 13)
/// publish covers the same L0 entries plus the L1 ones. Each pass building its
/// own tally cost two GETs and two full object scans per L0 part where one
/// serves both.
///
/// Keyed by the object key an entry addresses, not by the entry's identity
/// tuple or its content hash alone: a hit is then provably the same immutable
/// object (docs/object-store-contract.md), so the memoized columns are exactly
/// what a second read would have tallied, while two entries that share a
/// content hash under different identities still read their own objects. The
/// record's identity fields always come from the requesting entry, never from
/// whichever entry populated the tally.
///
/// Failures are not memoized: a segment whose fetch, decode, or tally fails
/// keeps its per-pass behavior (dropped from that publish, retried by the
/// next), so a transient store error cannot change what either object holds.
pub(crate) struct SegmentColumnStatsCache<'a> {
    typed_columns: &'a [DeclaredTypedColumn],
    tallied: std::collections::HashMap<String, Vec<ColumnStat>>,
}

impl<'a> SegmentColumnStatsCache<'a> {
    /// The declared columns are fixed for the cache's whole life: they are part
    /// of what a tally depends on, and the cache key does not carry them.
    pub(crate) fn new(typed_columns: &'a [DeclaredTypedColumn]) -> Self {
        Self {
            typed_columns,
            tallied: std::collections::HashMap::new(),
        }
    }

    /// The column-statistics record for `entry`, tallying its object on the
    /// first request for that object and reusing the tally afterwards. Equal to
    /// what [`fetch_segment_column_stats`] returns for the same entry, which is
    /// what the miss path calls: deriving the key is pure and cheap, so it costs
    /// nothing to derive it here for the lookup and let the builder derive it
    /// again for the read.
    pub(crate) async fn segment_column_stats(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        entry: &ravel_proto::catalog::v1::SnapshotEntry,
    ) -> Result<(ColumnStatsSegment, StatsFetch), ColumnStatsBuildError> {
        let data_key = segment_data_key(tenant, signal, entry)?;
        if let Some(columns) = self.tallied.get(&data_key) {
            return Ok((
                segment_from_columns(entry, columns.clone()),
                StatsFetch::Reused,
            ));
        }
        let segment =
            fetch_segment_column_stats(store, tenant, signal, entry, self.typed_columns).await?;
        self.tallied.insert(data_key, segment.columns.clone());
        Ok((segment, StatsFetch::Issued))
    }
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{InstrumentedStore, ObjectStoreBackend, PutOptions};
    use ravel_proto::catalog::v1::SnapshotEntry;
    use ravel_proto::catalog::v1::column_value::Kind;
    use ravel_types::logstream::{AttrValue, log_stream_id};

    const TENANT: TenantHash = TenantHash([0x5a; 16]);
    const WRITER: u128 = 0x1111_2222_3333_4444_5555_6666_7777_8888;

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: *Uuid::from_u128(WRITER).as_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    /// One log record on the single `service.name = api` stream carrying the
    /// given dynamic attributes.
    fn record(ts: i64, attrs: &[(String, AttrValue)]) -> LogRecord {
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        )];
        LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: format!("body {ts}"),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: attrs.to_vec(),
        }
    }

    fn i64_attr(key: &str, v: i64) -> (String, AttrValue) {
        (key.to_string(), AttrValue::I64(v))
    }

    /// Write `records` as a real L1 RLOG part object at its reconstructed L1
    /// part key (`keys::l1_part_key`), and return the matching L1
    /// [`SnapshotEntry`] in exactly the shape `fold::build_l1_snapshot_entry`
    /// produces: `writer_id` holds the 32-byte `input_set_hash`, `writer_epoch`
    /// holds the `part_index`. An L1 compaction output carries nil writer
    /// identity in its own footer.
    async fn write_l1(
        store: &dyn ObjectStoreBackend,
        ingest_hour_bucket: u32,
        input_set_hash: [u8; 32],
        part_index: u32,
        records: &[LogRecord],
    ) -> SnapshotEntry {
        let cfg = RlogConfig {
            block_target_records: 3,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: TENANT.0,
                shard: 0,
                writer_id: *Uuid::nil().as_bytes(),
                writer_epoch: 0,
                writer_seq: 0,
            },
        );
        for r in records {
            w.push(r.clone()).expect("push");
        }
        let bytes = w.finish().expect("finish");
        let content_hash = *blake3::hash(&bytes).as_bytes();
        let input_set_hash16 = hex::encode(&input_set_hash[..8]);
        let hash16 = hex::encode(&content_hash[..8]);
        let key = ravel_commit::keys::l1_part_key(
            &TENANT,
            Signal::Logs,
            0,
            ingest_hour_bucket,
            &input_set_hash16,
            part_index,
            &hash16,
        )
        .expect("l1 part key");
        store
            .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");

        SnapshotEntry {
            level: 1,
            shard: 0,
            ingest_hour_bucket,
            writer_id: input_set_hash.to_vec(),
            writer_epoch: u64::from(part_index),
            writer_seq: 0,
            content_hash: content_hash.to_vec(),
            object_size: 0,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            segment_format_version: 2,
            created_unix_ns: 0,
            declared_column_stats: Vec::new(),
        }
    }

    /// Write `records` as a real L0 RLOG object onto `store` at its true
    /// `data_key`, and return the matching L0 [`SnapshotEntry`]. Blocks are cut
    /// every 3 records so the tally spans several blocks.
    async fn write_l0(store: &dyn ObjectStoreBackend, records: &[LogRecord]) -> SnapshotEntry {
        let cfg = RlogConfig {
            block_target_records: 3,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(cfg, identity());
        for r in records {
            w.push(r.clone()).expect("push");
        }
        let bytes = w.finish().expect("finish");
        let content_hash = *blake3::hash(&bytes).as_bytes();
        let writer_id = Uuid::from_u128(WRITER);
        let key =
            ravel_commit::keys::data_key(&TENANT, Signal::Logs, 0, writer_id, 1, 1, &content_hash)
                .expect("data key");
        store
            .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");

        SnapshotEntry {
            level: 0,
            shard: 0,
            ingest_hour_bucket: 0,
            writer_id: writer_id.into_bytes().to_vec(),
            writer_epoch: 1,
            writer_seq: 1,
            content_hash: content_hash.to_vec(),
            object_size: 0,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            segment_format_version: 1,
            created_unix_ns: 0,
            declared_column_stats: Vec::new(),
        }
    }

    fn declared(key: &str, ty: DeclaredColumnType) -> DeclaredTypedColumn {
        DeclaredTypedColumn {
            key: key.to_string(),
            ty,
        }
    }

    fn i64_kind(v: &ColumnValue) -> i64 {
        match &v.kind {
            Some(Kind::I64(x)) => *x,
            other => panic!("expected I64 column value, got {other:?}"),
        }
    }

    /// The statistics a segment gets are the exact values its rows imply: a
    /// six-row object where `status` is 200,200,404,200,<absent>,500 yields
    /// non_null_count 5, null_count 1, the exact per-value dictionary, and
    /// min/max 200/500. A declared column no row sets (`absent`) gets no entry
    /// at all, and a dynamic attribute that is not declared (`extra`) is never
    /// tallied.
    #[tokio::test]
    async fn segment_stats_are_the_exact_values_the_rows_imply() {
        let store = MemoryStore::new();
        let records = vec![
            record(1, &[i64_attr("status", 200), i64_attr("extra", 7)]),
            record(2, &[i64_attr("status", 200)]),
            record(3, &[i64_attr("status", 404)]),
            record(4, &[i64_attr("status", 200)]),
            record(5, &[]), // no status: one null
            record(6, &[i64_attr("status", 500)]),
        ];
        let entry = write_l0(&store, &records).await;

        let typed = vec![
            declared("status", DeclaredColumnType::I64),
            declared("absent", DeclaredColumnType::I64),
        ];
        let seg = fetch_segment_column_stats(&store, &TENANT, Signal::Logs, &entry, &typed)
            .await
            .expect("build stats");

        assert_eq!(
            seg.columns.len(),
            1,
            "only `status` is present and declared"
        );
        let status = &seg.columns[0];
        assert_eq!(status.name, "status");
        assert_eq!(status.declared_type, 2, "I64 stats tag");
        assert_eq!(status.non_null_count, 5, "five rows set status");
        assert_eq!(status.null_count, 1, "row 5 left status absent");
        assert!(
            status.dictionary_present,
            "cardinality is well under the ceiling"
        );

        let dict: Vec<(i64, u64)> = status
            .dictionary
            .iter()
            .map(|e| (i64_kind(e.value.as_ref().expect("value")), e.count))
            .collect();
        assert_eq!(
            dict,
            vec![(200, 3), (404, 1), (500, 1)],
            "exact per-value counts, ascending"
        );
        assert_eq!(i64_kind(status.min.as_ref().expect("min")), 200);
        assert_eq!(i64_kind(status.max.as_ref().expect("max")), 500);
        assert_eq!(
            status.sum,
            Some(1504),
            "exact sum of the non-null values 200+200+404+200+500"
        );
    }

    /// #861: only an I64 column carries a sum. A declared `Bool` column is
    /// folded for min/max/null counts exactly like any other, but its stat
    /// leaves `sum` absent -- a bool has no additive semantics, and the
    /// metadata-only SUM/AVG path must fall back to scanning for it.
    ///
    /// Prove-the-test: making `into_proto` emit `sum` for the `Bool` arm makes
    /// `status_bool.sum` non-`None` and the assertion below fails.
    #[tokio::test]
    async fn a_bool_column_carries_no_sum() {
        let store = MemoryStore::new();
        let records = vec![
            record(1, &[("flag".to_string(), AttrValue::Bool(true))]),
            record(2, &[("flag".to_string(), AttrValue::Bool(false))]),
            record(3, &[("flag".to_string(), AttrValue::Bool(true))]),
        ];
        let entry = write_l0(&store, &records).await;

        let typed = vec![declared("flag", DeclaredColumnType::Bool)];
        let seg = fetch_segment_column_stats(&store, &TENANT, Signal::Logs, &entry, &typed)
            .await
            .expect("build stats");

        assert_eq!(seg.columns.len(), 1, "only `flag` is present and declared");
        let flag = &seg.columns[0];
        assert_eq!(flag.declared_type, 3, "Bool stats tag");
        assert_eq!(flag.non_null_count, 3);
        assert_eq!(flag.sum, None, "a Bool column carries no integer sum");
    }

    /// ADR-0942: `fetch_segment_column_stats` builds exact statistics for an L1
    /// (compaction-output) entry, reconstructing the L1 part key from the
    /// 32-byte `input_set_hash` and `part_index` the entry carries in its
    /// writer_* slots, and reading the same RLOG grammar an L0 object uses.
    ///
    /// Prove-the-test (deliverable 5): this test fails against the pre-ADR-0942
    /// builder. The exact line flipped is the data-key construction in
    /// [`fetch_segment_column_stats`]: it used to be the unconditional
    /// `let writer_id_bytes: [u8; 16] = entry.writer_id.as_slice().try_into()
    /// .map_err(|_| ColumnStatsBuildError::BadWriterIdLen(entry.writer_id.len()))?;`
    /// (plus a `debug_assert_eq!(entry.level, 0)` above it). Against that code
    /// this L1 entry's 32-byte `writer_id` makes the `try_into` fail, the call
    /// returns `Err(BadWriterIdLen(32))`, and the `.expect("build stats")` below
    /// panics before any value is checked.
    #[tokio::test]
    async fn fetch_segment_column_stats_covers_l1_entry() {
        let store = MemoryStore::new();
        let input_set_hash = [0x33u8; 32];
        let records = vec![
            record(1, &[i64_attr("status", 200)]),
            record(2, &[i64_attr("status", 404)]),
            record(3, &[i64_attr("status", 200)]),
            record(4, &[i64_attr("status", 500)]),
        ];
        let entry = write_l1(&store, 7, input_set_hash, 0, &records).await;

        let typed = vec![declared("status", DeclaredColumnType::I64)];
        let seg = fetch_segment_column_stats(&store, &TENANT, Signal::Logs, &entry, &typed)
            .await
            .expect("build stats");

        assert_eq!(seg.columns.len(), 1, "only `status` is declared");
        let status = &seg.columns[0];
        assert_eq!(status.non_null_count, 4);
        assert_eq!(status.null_count, 0);
        assert_eq!(i64_kind(status.min.as_ref().expect("min")), 200);
        assert_eq!(i64_kind(status.max.as_ref().expect("max")), 500);
        assert_eq!(status.sum, Some(1304), "200+404+200+500");
        let dict: Vec<(i64, u64)> = status
            .dictionary
            .iter()
            .map(|e| (i64_kind(e.value.as_ref().expect("value")), e.count))
            .collect();
        assert_eq!(dict, vec![(200, 2), (404, 1), (500, 1)]);
        // The builder passes the entry's identity tuple through unchanged:
        // writer_id is the L1 entry's 32-byte input_set_hash. The fold's v2
        // publish path overwrites writer_id with the part content hash (the v2
        // join key); the builder itself does not.
        assert_eq!(
            seg.writer_id,
            input_set_hash.to_vec(),
            "builder leaves writer_id as the entry carried it"
        );
    }

    /// #964: [`SegmentColumnStatsCache`] reads each covered object once. The
    /// second publish's request for an entry the first publish already tallied
    /// returns the same record with no second GET, while a different object
    /// still gets its own read.
    ///
    /// Prove-the-test: having `segment_column_stats` skip its cache lookup and
    /// always call `fetch_segment_column_stats` makes the repeat request report
    /// `StatsFetch::Issued` and the store's GET count 2 instead of 1.
    #[tokio::test]
    async fn cache_reads_each_covered_object_once() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let entry_a = write_l0(&store, &[record(1, &[i64_attr("status", 200)])]).await;
        let entry_b = write_l0(
            &store,
            &[
                record(2, &[i64_attr("status", 404)]),
                record(3, &[i64_attr("status", 500)]),
            ],
        )
        .await;
        assert_ne!(
            entry_a.content_hash, entry_b.content_hash,
            "two distinct objects"
        );

        let typed = vec![declared("status", DeclaredColumnType::I64)];
        let mut cache = SegmentColumnStatsCache::new(&typed);
        let gets = || store.metrics().snapshot().get.calls;
        let before = gets();

        let (first, fetch) = cache
            .segment_column_stats(&store, &TENANT, Signal::Logs, &entry_a)
            .await
            .expect("first request");
        assert_eq!(fetch, StatsFetch::Issued);
        assert_eq!(gets() - before, 1, "one read for the first request");

        // The second publish asking for the same entry.
        let (again, fetch) = cache
            .segment_column_stats(&store, &TENANT, Signal::Logs, &entry_a)
            .await
            .expect("repeat request");
        assert_eq!(fetch, StatsFetch::Reused);
        assert_eq!(gets() - before, 1, "the repeat request reads nothing");
        assert_eq!(again, first, "the repeat request returns the same record");

        // The reused record is exactly what a fresh build produces.
        let fresh = fetch_segment_column_stats(&store, &TENANT, Signal::Logs, &entry_a, &typed)
            .await
            .expect("fresh build");
        assert_eq!(again, fresh);

        let (other, fetch) = cache
            .segment_column_stats(&store, &TENANT, Signal::Logs, &entry_b)
            .await
            .expect("second object");
        assert_eq!(fetch, StatsFetch::Issued, "a different object is read");
        assert_ne!(
            other.columns, first.columns,
            "the second object's own statistics, not the cached ones"
        );
    }

    /// ADR-0942 safety lemma at the build boundary: a part-bound (v2) object
    /// whose covered-part binding does not match the parts asked for is a typed,
    /// graceful mismatch. The fold turns it into "rebuild the baseline"; it is
    /// never a hard error and never a decode of wrong data. (The query-time
    /// reader join, task A3, applies the same rule per segment: a resolved
    /// segment whose content_hash matches no record simply has no stats and the
    /// query scans it.)
    #[test]
    fn decode_previous_column_stats_mismatched_part_binding_is_graceful() {
        // A v2 record carries the covered part content hash in writer_id.
        let seg = ColumnStatsSegment {
            ingest_hour_bucket: 1,
            shard: 0,
            writer_id: vec![0xCC; 32],
            writer_epoch: 0,
            writer_seq: 0,
            columns: vec![],
        };
        let part_a = [0x0Au8; 32];
        let part_b = [0x0Bu8; 32];
        let bytes = crate::snapshot_format::encode_column_stats_v2(
            TENANT.0,
            3,
            vec![part_a.to_vec()],
            std::slice::from_ref(&seg),
        )
        .expect("v2 encodes");
        let limits = crate::snapshot_format::ColumnStatsLimits::default();

        // Asked for part B: the binding does not match -> typed graceful error.
        let err = decode_previous_column_stats(&bytes, &[part_b], &limits)
            .expect_err("mismatched part binding is rejected");
        assert_eq!(err, SnapshotFormatError::ColumnStatsPartBindingMismatch);

        // Asked for the true binding: decodes, record is content-hash keyed.
        let ok = decode_previous_column_stats(&bytes, &[part_a], &limits).expect("binding matches");
        assert_eq!(ok.len(), 1);
        assert_eq!(ok[0].writer_id, vec![0xCC; 32]);
    }
}
