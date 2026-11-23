//! Per-declared-column exact statistics on the commit-family wire records
//! (ADR-0873 decision 1): `CommitRecord.declared_column_stats` (field 20) and
//! `CompactionPart.declared_column_stats` (field 12).
//!
//! The typed vocabulary and the eligibility allowlist live in
//! [`ravel_types::declared_stats`]; this module is only the wire layer over
//! it: a [`DeclaredColumnStat`] in, a `DeclaredColumnMinMax` out, and back.
//!
//! Three rules govern the read direction, and all three come straight from
//! the ADR:
//!
//! - **Absence is permanent and legal.** An empty list means the record's
//!   object is uncovered for every declared column. Every record written
//!   before ADR-0873 is in that state forever (commit records are immutable
//!   and are never rewritten, ADR-0873 decision 5, migration class C), as is
//!   every metrics/spans record and any column declared after its flush
//!   opened. Nothing here treats absence as an error, a fallback, or an event
//!   worth logging.
//! - **A defective entry is dropped, never trusted, and never fatal.** An
//!   ineligible `declared_type`, a value kind that disagrees with it, a
//!   half-present min/max pair, a min above its max, an empty name, a null
//!   count that disagrees with the carrying record's row count, or a
//!   duplicate column all leave that one column uncovered for that segment
//!   and are counted for the caller's defect metric. The record itself still
//!   decodes: a bad statistic must not make a durable commit record
//!   unreadable.
//! - **Two clauses belong to the record, not to the entry.** Clauses 4 and 5
//!   of the predicate compare `null_count` against the segment's row count,
//!   which no `DeclaredColumnMinMax` carries: for a stamp it is the carrying
//!   record's own `sample_count` (`CommitRecord` field 11, `CompactionPart`
//!   field 6). So [`decode`] stays carrier-agnostic and [`read_commit_record`],
//!   [`read_compaction_part`], and [`read_snapshot_entry_twin`] are where those
//!   two clauses bind, and because those two clauses are exactly the row-count
//!   gate, they are the
//!   only functions that produce a [`ValidatedDeclaredStats`] (ADR-0873
//!   decision 2, "where the predicate binds": coverage is unconstructable
//!   without the full predicate).
//! - **The stamp side cannot express an ineligible type at all.** Encoding
//!   takes only [`DeclaredColumnStat`]s, whose constructor is the typed
//!   boundary that refuses ineligible types (ADR-0873 decision 2).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use ravel_proto::commit::v1::{
    CommitRecord, CompactionPart, DeclaredColumnMinMax, DeclaredColumnStatValue,
    declared_column_stat_value::Kind,
};
use ravel_types::declared_stats::{
    DeclaredColumnStat, DeclaredStatError, DeclaredStatType, DeclaredStatValue,
};

/// Which carrier a drop observation belongs to (ADR-0873 decision 2: the
/// defect metric carries a carrier label, because "which writer do I look at"
/// is the first question a nonzero value asks).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatCarrier {
    /// `CommitRecord.declared_column_stats` (field 20), read by
    /// [`read_commit_record`]: a flush's own stamp.
    CommitRecord,
    /// `CompactionPart.declared_column_stats` (field 12), read by
    /// [`read_compaction_part`]: a compaction or erasure-rewrite output part's
    /// recomputed stamp.
    CompactionPart,
    /// `SnapshotEntry.declared_column_stats` (field 15), the fold's copy, read
    /// through [`read_snapshot_entry_twin`].
    SnapshotEntry,
    /// The ADR-0850/0942 `.cstat` object's `ColumnStat` entries. Not a stamp
    /// carrier and not read by this module at all: it is the second carrier of
    /// the same statistics (ADR-0873 decision 4), its reader lives in
    /// `ravel_sql`, and its refusals are reported here through
    /// [`observe_declared_stat_drops`] so one metric covers both carriers of
    /// one union.
    Cstat,
}

impl StatCarrier {
    /// Every label, for a scrape that reports the whole set rather than only
    /// the carriers that happened to move.
    pub const ALL: [StatCarrier; 4] = [
        StatCarrier::CommitRecord,
        StatCarrier::CompactionPart,
        StatCarrier::SnapshotEntry,
        StatCarrier::Cstat,
    ];

    /// The stable metric label, in the dashed form ADR-0873 decision 2 names
    /// the carriers in.
    pub fn label(self) -> &'static str {
        match self {
            StatCarrier::CommitRecord => "commit-record",
            StatCarrier::CompactionPart => "compaction-part",
            StatCarrier::SnapshotEntry => "snapshot-entry",
            StatCarrier::Cstat => "cstat",
        }
    }

    /// Index into [`DROPS_OBSERVED`]. Private: the numbering is an
    /// implementation detail of the tally, not part of the label contract.
    fn index(self) -> usize {
        match self {
            StatCarrier::CommitRecord => 0,
            StatCarrier::CompactionPart => 1,
            StatCarrier::SnapshotEntry => 2,
            StatCarrier::Cstat => 3,
        }
    }
}

/// Per-carrier tallies behind [`declared_stat_drops_observed`], indexed by
/// [`StatCarrier::index`].
static DROPS_OBSERVED: [AtomicU64; 4] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// How many declared-column statistic drops this process has OBSERVED under
/// `carrier` since it started: the ADR-0873 decision 2 defect metric.
///
/// Observation semantics, and the name says so because the difference matters
/// when reading the number. Each READ of a defective carrier increments this by
/// that read's drop count, so one defective commit record read by a thousand
/// queries is a thousand observations, not one defect. The magnitude therefore
/// scales with query rate and states nothing about how many distinct defective
/// entries exist; what it states is that a defect is still being read. Alert on
/// "nonzero", and compare rates over equal windows, never magnitudes over
/// windows of different query volume.
///
/// Per-entry deduplication is deliberately absent rather than pending: it would
/// need unbounded state keyed by (object, column) on a path walked once per
/// segment per query, which is a memory leak in exchange for a figure whose
/// only use is "nonzero means investigate".
///
/// The label names the carrier the read was over, which is what points at a
/// writer: `commit-record` and `compaction-part` are read here,
/// `snapshot-entry` through [`read_snapshot_entry_twin`], and `cstat` is
/// reported by `ravel_sql`'s `.cstat` reader through
/// [`observe_declared_stat_drops`]. Absence of statistics never touches any
/// label: an unstamped record is the permanent, legal state of everything
/// written before ADR-0873, and a `.cstat` object that simply carries no entry
/// for a column is the ordinary uncovered case.
///
/// Monotonic and process-local, like every other in-process anomaly tally
/// here: a scrape reads the delta between samples.
pub fn declared_stat_drops_observed(carrier: StatCarrier) -> u64 {
    DROPS_OBSERVED[carrier.index()].load(Ordering::Relaxed)
}

/// The same metric summed over every carrier label, for a caller that wants the
/// one-number "is anything defective at all" signal. Same observation
/// semantics as [`declared_stat_drops_observed`], and the sum of a set of
/// per-carrier reads that are not taken atomically together, so it is a
/// snapshot for alerting and not an accounting identity.
pub fn declared_stat_drops_observed_total() -> u64 {
    StatCarrier::ALL
        .iter()
        .map(|carrier| declared_stat_drops_observed(*carrier))
        .sum()
}

/// Every carrier's tally, paired with its carrier, in [`StatCarrier::ALL`]
/// order.
///
/// The scrape-side accessor: a renderer that iterates this emits one series per
/// carrier label whether or not that carrier has ever dropped an entry, so a
/// label that stays at `0` is visibly covered rather than indistinguishable
/// from a carrier nobody wired up. Calling [`declared_stat_drops_observed`] per
/// carrier gives the same numbers; this exists so the renderer cannot iterate a
/// label list out of step with the tallies it reads.
///
/// Same observation semantics and the same non-atomicity as
/// [`declared_stat_drops_observed_total`]: four independent relaxed loads, a
/// snapshot for alerting rather than a consistent cut.
pub fn declared_stat_drops_observed_all() -> [(StatCarrier, u64); 4] {
    StatCarrier::ALL.map(|carrier| (carrier, declared_stat_drops_observed(carrier)))
}

/// Record `count` drop observations under `carrier`.
///
/// Public because one carrier of the ADR-0873 decision 4 union is read outside
/// this crate: `ravel_sql`'s `.cstat` reader refuses a duplicated column name
/// and an entry whose extrema presence disagrees with its row accounting, and
/// those refusals are the same defect class as a dropped stamp entry, under the
/// [`StatCarrier::Cstat`] label. Call it once per refusal, at the site that
/// refuses.
pub fn observe_declared_stat_drops(carrier: StatCarrier, count: usize) {
    if count == 0 {
        return;
    }
    DROPS_OBSERVED[carrier.index()].fetch_add(count as u64, Ordering::Relaxed);
}

/// Why one `DeclaredColumnMinMax` entry was dropped on decode.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum DeclaredStatDefect {
    /// The entry violates the typed contract in [`ravel_types::declared_stats`].
    #[error(transparent)]
    Invalid(#[from] DeclaredStatError),
    /// A `DeclaredColumnStatValue` message is present but sets no oneof arm.
    /// Distinct from an absent message, which is the legal "zero non-null
    /// values" statement: a present-but-empty value names no extremum at all.
    #[error("declared column {name:?}: {which} value message is present but sets no kind")]
    EmptyValueKind { name: String, which: &'static str },
    /// Clause 4: more NULLs than the segment has rows. Impossible for any
    /// correct writer, so nothing on the entry is trusted.
    #[error(
        "declared column {name:?}: null_count {null_count} exceeds the record's sample_count {sample_count}: an object cannot hold more NULLs than rows"
    )]
    NullCountAboveSampleCount {
        name: String,
        null_count: u64,
        sample_count: u64,
    },
    /// Clause 5: the presence of the extrema and the null count contradict
    /// each other. `extrema` says which direction fired: `"absent"` for a
    /// zero-non-null-values claim on a record that has non-NULL rows,
    /// `"present"` for extrema claimed over a column that reads NULL in every
    /// row.
    #[error(
        "declared column {name:?}: min and max are both {extrema}, so null_count {null_count} disagrees with sample_count {sample_count}: both-absent requires them equal, both-present requires null_count below sample_count"
    )]
    PresenceDisagreesWithNullCount {
        name: String,
        extrema: &'static str,
        null_count: u64,
        sample_count: u64,
    },
    /// Two or more entries name the same column. Which one wins is undefined,
    /// so ALL of them are dropped and the column is uncovered for this segment
    /// (ADR-0873 clause 6: keeping any one duplicate is silently picking
    /// between two claims about one immutable record). Every occurrence of the
    /// name carries this reason, including the first.
    #[error("duplicate entry for declared column {0:?}")]
    DuplicateName(String),
    /// ADR-4628 decision 2: fields 6 (`sum`) and 7 (`sum_wide`) are both
    /// present. A sum has one encoding, and a reader that preferred either
    /// field would be picking between two claims.
    #[error("declared column {name:?}: sum and sum_wide are both present")]
    BothSumFields { name: String },
    /// `sum_wide` (field 7) is present with a length other than 16 bytes.
    #[error("declared column {name:?}: sum_wide is {len} bytes, not 16")]
    SumWideLength { name: String, len: usize },
    /// `sum_wide` (field 7) holds a value that fits i64, which belongs in
    /// field 6.
    #[error("declared column {name:?}: sum_wide holds {sum}, which fits i64 and belongs in sum")]
    SumWideFitsI64 { name: String, sum: i128 },
    /// The sum lies outside `[non_null * min, non_null * max]`.
    #[error(
        "declared column {name:?}: sum {sum} lies outside [{low}, {high}], the bounds {non_null} non-null values between min and max allow"
    )]
    SumOutsideBounds {
        name: String,
        sum: i128,
        non_null: u64,
        low: i128,
        high: i128,
    },
    /// More zero-valued rows than non-null rows.
    #[error(
        "declared column {name:?}: zero_count {zero_count} exceeds the non-null count {non_null}"
    )]
    ZeroCountAboveNonNull {
        name: String,
        zero_count: u64,
        non_null: u64,
    },
    /// `min == max == 0` (or `false`) states every non-null row is the zero,
    /// so a zero count below the non-null count contradicts the extrema.
    #[error(
        "declared column {name:?}: min and max are both the zero, so zero_count {zero_count} must equal the non-null count {non_null}"
    )]
    ZeroCountBelowNonNullAllZero {
        name: String,
        zero_count: u64,
        non_null: u64,
    },
}

/// One dropped entry, with its position in the record's list so a defect
/// counter can be attributed without re-deriving it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedStatEntry {
    /// Index of the offending entry in the record's `declared_column_stats`.
    pub index: usize,
    /// The entry's `name` as stored, which may be empty or a duplicate.
    pub name: String,
    /// Why it was dropped.
    pub reason: DeclaredStatDefect,
}

/// A record's declared-column statistics after the **full** statistics
/// validity predicate has run: the entries that are trustworthy, and the ones
/// that were dropped.
///
/// This type is the coverage grant of ADR-0873 decision 2. Possessing one is
/// proof that every entry it reports as `covered` passed the entry-local
/// clauses **and** the row-count clauses (4 and 5) against the carrying
/// record's own row count. That guarantee is enforced by construction, not by
/// convention: the fields are private and the constructor
/// ([`decode_all_against_rows`]) is private to this module, so the only paths
/// to a `ValidatedDeclaredStats` outside this module are [`read_commit_record`],
/// [`read_compaction_part`], and [`read_snapshot_entry_twin`], each of which
/// supplies the carrier's row
/// count. There is no public constructor and no public `decode_all`: a caller
/// cannot obtain coverage without the row count, which is the whole point of
/// binding the predicate at the point of use rather than at the decode
/// boundary (ADR-0873 decision 2, "where the predicate binds").
///
/// `covered` empty with `dropped` empty is the normal permanent state of an
/// unstamped record, not a failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedDeclaredStats {
    /// Entries that passed the full predicate, in record order.
    covered: Vec<DeclaredColumnStat>,
    /// Entries that were dropped; each leaves its column uncovered for this
    /// segment. Feed the length to the defect metric (ADR-0873 decision 2).
    dropped: Vec<DroppedStatEntry>,
}

impl ValidatedDeclaredStats {
    /// The covered entries, read-only, in record order.
    pub fn covered(&self) -> &[DeclaredColumnStat] {
        &self.covered
    }

    /// The dropped entries, read-only, in record order.
    pub fn dropped(&self) -> &[DroppedStatEntry] {
        &self.dropped
    }

    /// Look up one covered declared column's statistics by name.
    pub fn column(&self, name: &str) -> Option<&DeclaredColumnStat> {
        self.covered.iter().find(|stat| stat.name() == name)
    }
}

fn value_to_proto(value: DeclaredStatValue) -> DeclaredColumnStatValue {
    let kind = match value {
        DeclaredStatValue::I64(v) => Kind::I64(v),
        DeclaredStatValue::Bool(b) => Kind::B(b),
    };
    DeclaredColumnStatValue { kind: Some(kind) }
}

fn value_from_proto(value: &DeclaredColumnStatValue) -> Option<DeclaredStatValue> {
    match value.kind {
        Some(Kind::I64(v)) => Some(DeclaredStatValue::I64(v)),
        Some(Kind::B(b)) => Some(DeclaredStatValue::Bool(b)),
        None => None,
    }
}

/// Encode one typed stat for the wire. Infallible: a [`DeclaredColumnStat`]
/// cannot hold an ineligible type or an inconsistent pair.
///
/// A sum that fits i64 goes to field 6 and one that does not goes to field 7
/// as 16 bytes little-endian two's complement, never both (ADR-4628
/// decision 1).
pub fn encode(stat: &DeclaredColumnStat) -> DeclaredColumnMinMax {
    let (sum, sum_wide) = match stat.sum() {
        None => (None, Vec::new()),
        Some(sum) => match i64::try_from(sum) {
            Ok(narrow) => (Some(narrow), Vec::new()),
            Err(_) => (None, sum.to_le_bytes().to_vec()),
        },
    };
    DeclaredColumnMinMax {
        name: stat.name().to_string(),
        declared_type: stat.declared_type().tag(),
        min: stat.min().map(value_to_proto),
        max: stat.max().map(value_to_proto),
        null_count: stat.null_count(),
        sum,
        sum_wide,
        zero_count: stat.zero_count(),
    }
}

/// Encode a whole stamp list for the wire.
pub fn encode_all(stats: &[DeclaredColumnStat]) -> Vec<DeclaredColumnMinMax> {
    stats.iter().map(encode).collect()
}

/// Decode one entry, or say why it cannot be trusted.
pub fn decode(entry: &DeclaredColumnMinMax) -> Result<DeclaredColumnStat, DeclaredStatDefect> {
    let declared_type = DeclaredStatType::from_tag(entry.declared_type)?;
    let extremum = |value: &Option<DeclaredColumnStatValue>,
                    which: &'static str|
     -> Result<Option<DeclaredStatValue>, DeclaredStatDefect> {
        match value {
            None => Ok(None),
            Some(value) => value_from_proto(value).map(Some).ok_or_else(|| {
                DeclaredStatDefect::EmptyValueKind {
                    name: entry.name.clone(),
                    which,
                }
            }),
        }
    };
    let min = extremum(&entry.min, "min")?;
    let max = extremum(&entry.max, "max")?;
    let sum = decode_sum(entry)?;
    let mut stat = DeclaredColumnStat::new(
        entry.name.clone(),
        declared_type,
        min,
        max,
        entry.null_count,
    )?;
    if let Some(sum) = sum {
        stat = stat.with_sum(sum)?;
    }
    if let Some(zero_count) = entry.zero_count {
        stat = stat.with_zero_count(zero_count)?;
    }
    Ok(stat)
}

/// Fields 6 and 7 as one optional sum, refusing the encodings ADR-4628
/// decision 2 rules out: both fields, a wide field that is not 16 bytes, and
/// a wide field holding a value field 6 could carry.
fn decode_sum(entry: &DeclaredColumnMinMax) -> Result<Option<i128>, DeclaredStatDefect> {
    if entry.sum_wide.is_empty() {
        return Ok(entry.sum.map(i128::from));
    }
    if entry.sum.is_some() {
        return Err(DeclaredStatDefect::BothSumFields {
            name: entry.name.clone(),
        });
    }
    let bytes: [u8; 16] =
        entry
            .sum_wide
            .as_slice()
            .try_into()
            .map_err(|_| DeclaredStatDefect::SumWideLength {
                name: entry.name.clone(),
                len: entry.sum_wide.len(),
            })?;
    let sum = i128::from_le_bytes(bytes);
    if i64::try_from(sum).is_ok() {
        return Err(DeclaredStatDefect::SumWideFitsI64 {
            name: entry.name.clone(),
            sum,
        });
    }
    Ok(Some(sum))
}

/// Clauses 4 and 5 of the predicate, checked against the row count of the
/// segment the entry describes.
///
/// Both compare `null_count` to `sample_count`, so both are decidable only
/// where a row count exists. A one-sided min/max pair never reaches here: it
/// is [`DeclaredStatError::PresenceMismatch`] at the entry layer.
fn row_count_defect(stat: &DeclaredColumnStat, sample_count: u64) -> Option<DeclaredStatDefect> {
    let null_count = stat.null_count();
    if null_count > sample_count {
        return Some(DeclaredStatDefect::NullCountAboveSampleCount {
            name: stat.name().to_string(),
            null_count,
            sample_count,
        });
    }
    // Absent extrema state zero non-null values, so every row must be NULL;
    // present extrema state at least one non-null value, so at least one row
    // must not be. Both directions are the same clause, and an implementation
    // that checks only the first accepts fabricated extrema over an all-NULL
    // column (issue #970 defect 2).
    let extrema = match (stat.min(), stat.max()) {
        (None, None) if null_count != sample_count => "absent",
        (Some(_), Some(_)) if null_count == sample_count => "present",
        _ => return aggregate_defect(stat, sample_count - null_count),
    };
    Some(DeclaredStatDefect::PresenceDisagreesWithNullCount {
        name: stat.name().to_string(),
        extrema,
        null_count,
        sample_count,
    })
}

/// The ADR-4628 decision 2 clauses that compare the sum and the zero count
/// with the non-null count. Runs only after clauses 4 and 5 hold, so
/// `non_null` is exact and is zero exactly when the extrema are absent.
fn aggregate_defect(stat: &DeclaredColumnStat, non_null: u64) -> Option<DeclaredStatDefect> {
    let name = || stat.name().to_string();
    if let Some(sum) = stat.sum() {
        let rank = |value: Option<DeclaredStatValue>| match value {
            Some(DeclaredStatValue::I64(v)) => i128::from(v),
            Some(DeclaredStatValue::Bool(b)) => i128::from(b),
            None => 0,
        };
        // |non_null| <= 2^64 - 1 and |extremum| <= 2^63, so neither product
        // leaves i128.
        let n = i128::from(non_null);
        let low = n.saturating_mul(rank(stat.min()));
        let high = n.saturating_mul(rank(stat.max()));
        if sum < low || sum > high {
            return Some(DeclaredStatDefect::SumOutsideBounds {
                name: name(),
                sum,
                non_null,
                low,
                high,
            });
        }
    }
    if let Some(zero_count) = stat.zero_count() {
        if zero_count > non_null {
            return Some(DeclaredStatDefect::ZeroCountAboveNonNull {
                name: name(),
                zero_count,
                non_null,
            });
        }
        let all_zero = matches!(
            (stat.min(), stat.max()),
            (
                Some(DeclaredStatValue::I64(0)),
                Some(DeclaredStatValue::I64(0))
            ) | (
                Some(DeclaredStatValue::Bool(false)),
                Some(DeclaredStatValue::Bool(false))
            )
        );
        if all_zero && zero_count < non_null {
            return Some(DeclaredStatDefect::ZeroCountBelowNonNullAllZero {
                name: name(),
                zero_count,
                non_null,
            });
        }
    }
    None
}

/// The shared read pass, and the sole constructor of [`ValidatedDeclaredStats`].
///
/// Private to the module by design (ADR-0873 decision 2): coverage must be
/// unconstructable without the full predicate, so the only public entry points
/// are [`read_commit_record`], [`read_compaction_part`], and
/// [`read_snapshot_entry_twin`], which always pass `Some(sample_count)`.
/// `sample_count` is `None` only for the module's own tests exercising the
/// entry-local clauses in isolation, in which case clauses 4 and 5 are not
/// decidable and the pass checks the entry-local clauses only.
///
/// `carrier` labels this pass's drop observations. It is a parameter rather
/// than a property of the entries because the entries of all three stamp
/// carriers are the same message: only the caller knows which record it read.
///
/// Never fails: a defective statistic leaves its column uncovered, it does not
/// make the record unreadable. Never fails on entry count either: the list is
/// bounded by the bytes prost already decoded and allocated, so the pass is
/// linear in work that has already been paid for. A count cap would have to
/// refuse a durable, immutable record whose declared-column count nothing in
/// the tenant-config path bounds, which is the one outcome this module exists
/// to avoid.
fn decode_all_against_rows(
    entries: &[DeclaredColumnMinMax],
    sample_count: Option<u64>,
    carrier: StatCarrier,
) -> ValidatedDeclaredStats {
    let mut covered = Vec::new();
    let mut dropped = Vec::new();
    // Clause 6 needs the whole entry set before any entry can be judged: a
    // duplicated name yields NO coverage, so whether the first occurrence is
    // trustworthy depends on an entry that comes after it. One counting pass,
    // then one judging pass. Names, not decoded stats: the name is stored
    // verbatim, so borrowing from `entries` costs no allocation per entry.
    let mut occurrences: HashMap<&str, usize> = HashMap::with_capacity(entries.len());
    for entry in entries.iter() {
        *occurrences.entry(entry.name.as_str()).or_insert(0) += 1;
    }
    for (index, entry) in entries.iter().enumerate() {
        // A duplicated name is dropped in EVERY occurrence, first included:
        // which of two same-name entries wins is undefined, and a reader that
        // keeps one is silently picking between two claims about one immutable
        // record -- the divergence ADR-0873 clause 6 forbids ("no coverage at
        // all, from every reader, under either ordering"). The defect belongs
        // to the name, so it is reported before the entry's own validity is
        // consulted: an individually valid entry beside a duplicate of itself
        // is still no coverage.
        let duplicated = occurrences.get(entry.name.as_str()).is_some_and(|n| *n > 1);
        let reason = if duplicated {
            DeclaredStatDefect::DuplicateName(entry.name.clone())
        } else {
            match decode(entry) {
                Ok(stat) => match sample_count.and_then(|rows| row_count_defect(&stat, rows)) {
                    Some(defect) => defect,
                    None => {
                        covered.push(stat);
                        continue;
                    }
                },
                Err(reason) => reason,
            }
        };
        dropped.push(DroppedStatEntry {
            index,
            name: entry.name.clone(),
            reason,
        });
    }
    observe_declared_stat_drops(carrier, dropped.len());
    ValidatedDeclaredStats { covered, dropped }
}

/// Stamp a commit record's declared-column statistics (field 20), replacing
/// whatever the in-memory record carried. Only ever applied to a record being
/// built: a published commit record is immutable.
pub fn stamp_commit_record(record: &mut CommitRecord, stats: &[DeclaredColumnStat]) {
    record.declared_column_stats = encode_all(stats);
}

/// Read a commit record's declared-column statistics, reconciled against the
/// record's own row count (`sample_count`, field 11). This is one of the three
/// functions that yield a [`ValidatedDeclaredStats`]: passing the row count
/// here is what makes clauses 4 and 5 bind, so the returned coverage cannot
/// exist without them.
pub fn read_commit_record(record: &CommitRecord) -> ValidatedDeclaredStats {
    decode_all_against_rows(
        &record.declared_column_stats,
        Some(record.sample_count),
        StatCarrier::CommitRecord,
    )
}

/// Read a snapshot entry's stamps through the commit-record twin the catalog
/// side builds for them (`SnapshotEntry.declared_column_stats`, field 15, whose
/// message is a field-for-field mirror of the commit-side one, ADR-0873
/// decision 1), labelling this pass's drop observations
/// [`StatCarrier::SnapshotEntry`].
///
/// The predicate is identical to [`read_commit_record`]'s -- a second
/// implementation is a second thing to keep in agreement -- and so is the row
/// count it binds against: `twin.sample_count` must be the ENTRY's own row
/// count. The only thing this function adds is the label, which exists because a
/// drop on the fold's copy points at a different writer than a drop on the
/// record the fold copied from.
///
/// `ravel_catalog::read_snapshot_entry` still calls [`read_commit_record`] on
/// its twin, so today's snapshot-entry drops are observed under the
/// `commit-record` label; re-pointing that one call site here is the remaining
/// step, and lives in a crate ADR-0873 wave 4 does not touch.
pub fn read_snapshot_entry_twin(twin: &CommitRecord) -> ValidatedDeclaredStats {
    decode_all_against_rows(
        &twin.declared_column_stats,
        Some(twin.sample_count),
        StatCarrier::SnapshotEntry,
    )
}

/// Stamp one compaction/rewrite output part's declared-column statistics
/// (field 12). The caller recomputes these over the rows the part holds and
/// never copies an input's stamp (ADR-0873 decision 3).
pub fn stamp_compaction_part(part: &mut CompactionPart, stats: &[DeclaredColumnStat]) {
    part.declared_column_stats = encode_all(stats);
}

/// Read one compaction/rewrite output part's declared-column statistics,
/// reconciled against the part's own row count (`sample_count`, field 6). Like
/// [`read_commit_record`], this is one of the three producers of a
/// [`ValidatedDeclaredStats`].
pub fn read_compaction_part(part: &CompactionPart) -> ValidatedDeclaredStats {
    decode_all_against_rows(
        &part.declared_column_stats,
        Some(part.sample_count),
        StatCarrier::CompactionPart,
    )
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use prost::Message;
    use ravel_types::declared_stats::{
        TYPED_ATTR_COLUMN_TYPE_BYTES, TYPED_ATTR_COLUMN_TYPE_F64, TYPED_ATTR_COLUMN_TYPE_STR,
        TYPED_ATTR_COLUMN_TYPE_UNSPECIFIED,
    };
    use ravel_types::{Signal, TenantHash};
    use uuid::Uuid;

    use crate::record::{self, NewCommitRecord};

    /// Entry-local read used by the wave-1 tests: the shared pass with no row
    /// count, so clauses 4 and 5 do not bind. Deliberately not a public
    /// function -- coverage is unconstructable without a carrier's row count
    /// (ADR-0873 decision 2), and this helper is the private route the read
    /// functions wrap, exposed here only to pin the entry-local clauses in
    /// isolation.
    fn decode_all(entries: &[DeclaredColumnMinMax]) -> ValidatedDeclaredStats {
        super::decode_all_against_rows(entries, None, StatCarrier::CommitRecord)
    }

    /// A `CommitRecord` as a writer that predates ADR-0873 encodes it: the
    /// same field numbers 1-19 and no field 20 in the schema at all, so its
    /// encoded bytes cannot contain the new field even accidentally.
    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyCommitRecord {
        #[prost(uint32, tag = "1")]
        format_version: u32,
        #[prost(bytes = "vec", tag = "2")]
        tenant_hash: Vec<u8>,
        #[prost(int32, tag = "3")]
        signal: i32,
        #[prost(uint32, tag = "4")]
        shard: u32,
        #[prost(string, tag = "5")]
        writer_id: String,
        #[prost(uint64, tag = "6")]
        writer_epoch: u64,
        #[prost(uint64, tag = "7")]
        writer_seq: u64,
        #[prost(string, tag = "8")]
        object_key: String,
        #[prost(uint64, tag = "9")]
        object_size: u64,
        #[prost(bytes = "vec", tag = "10")]
        content_hash: Vec<u8>,
        #[prost(uint64, tag = "11")]
        sample_count: u64,
        #[prost(uint64, tag = "12")]
        series_count: u64,
        #[prost(sfixed64, tag = "13")]
        min_event_ts_ns: i64,
        #[prost(sfixed64, tag = "14")]
        max_event_ts_ns: i64,
        #[prost(sfixed64, tag = "15")]
        min_ingest_ts_ns: i64,
        #[prost(sfixed64, tag = "16")]
        max_ingest_ts_ns: i64,
        #[prost(uint32, tag = "17")]
        segment_format_version: u32,
        #[prost(sfixed64, tag = "18")]
        created_unix_ns: i64,
        #[prost(uint32, tag = "19")]
        ingest_hour_bucket: u32,
    }

    /// Row count of the carriers these tests build, and the row count every
    /// generated stat is made consistent with. Clauses 4 and 5 tie
    /// `null_count` to the carrier's row count, so a strategy that drew
    /// `null_count` freely would generate entries the read side is required to
    /// drop.
    const SAMPLE_COUNT: u64 = 1_000;

    fn base_record() -> CommitRecord {
        record::build(NewCommitRecord {
            tenant_hash: TenantHash([0x11; 16]),
            signal: Signal::Logs,
            shard: 3,
            writer_id: Uuid::from_u128(7),
            writer_epoch: 100,
            writer_seq: 9,
            object_size: 4096,
            content_hash: [0x22; 32],
            sample_count: SAMPLE_COUNT,
            series_count: 4,
            min_event_ts_ns: 1_000,
            max_event_ts_ns: 2_000,
            min_ingest_ts_ns: 1_500,
            max_ingest_ts_ns: 2_500,
            segment_format_version: 4,
            created_unix_ns: 495_734 * 3_600_000_000_000,
            ingest_hour_bucket: 495_734,
        })
        .expect("valid record")
    }

    fn as_legacy(record: &CommitRecord) -> LegacyCommitRecord {
        LegacyCommitRecord {
            format_version: record.format_version,
            tenant_hash: record.tenant_hash.clone(),
            signal: record.signal,
            shard: record.shard,
            writer_id: record.writer_id.clone(),
            writer_epoch: record.writer_epoch,
            writer_seq: record.writer_seq,
            object_key: record.object_key.clone(),
            object_size: record.object_size,
            content_hash: record.content_hash.clone(),
            sample_count: record.sample_count,
            series_count: record.series_count,
            min_event_ts_ns: record.min_event_ts_ns,
            max_event_ts_ns: record.max_event_ts_ns,
            min_ingest_ts_ns: record.min_ingest_ts_ns,
            max_ingest_ts_ns: record.max_ingest_ts_ns,
            segment_format_version: record.segment_format_version,
            created_unix_ns: record.created_unix_ns,
            ingest_hour_bucket: record.ingest_hour_bucket,
        }
    }

    fn i64_stat(name: &str, min: i64, max: i64, null_count: u64) -> DeclaredColumnStat {
        DeclaredColumnStat::new(
            name,
            DeclaredStatType::I64,
            Some(DeclaredStatValue::I64(min)),
            Some(DeclaredStatValue::I64(max)),
            null_count,
        )
        .expect("valid i64 stat")
    }

    #[test]
    fn commit_record_round_trips_stamped_stats() {
        let mut record = base_record();
        let stats = vec![
            i64_stat("EventDate", -5, 19_000, 12),
            DeclaredColumnStat::new(
                "IsRefresh",
                DeclaredStatType::Bool,
                Some(DeclaredStatValue::Bool(false)),
                Some(DeclaredStatValue::Bool(true)),
                0,
            )
            .expect("valid bool stat"),
        ];
        stamp_commit_record(&mut record, &stats);
        let decoded = record::decode(&record::encode(&record)).expect("decode");
        let read = read_commit_record(&decoded);
        assert!(read.dropped().is_empty());
        assert_eq!(read.covered().to_vec(), stats);
        let event_date = read.column("EventDate").expect("EventDate covered");
        assert_eq!(event_date.min(), Some(DeclaredStatValue::I64(-5)));
        assert_eq!(event_date.max(), Some(DeclaredStatValue::I64(19_000)));
        assert_eq!(event_date.null_count(), 12);
        assert_eq!(decoded.format_version, 1);
    }

    #[test]
    fn compaction_part_round_trips_stamped_stats() {
        let mut part = base_part(2_000);
        let stats = vec![i64_stat("EventDate", 3, 4, 1)];
        stamp_compaction_part(&mut part, &stats);
        let bytes = part.encode_to_vec();
        let decoded = CompactionPart::decode(bytes.as_slice()).expect("decode part");
        let read = read_compaction_part(&decoded);
        assert!(read.dropped().is_empty());
        assert_eq!(read.covered().to_vec(), stats);
        assert_eq!(read.covered().len(), 1);
    }

    #[test]
    fn record_written_without_the_field_decodes_with_it_absent() {
        let record = base_record();
        let legacy_bytes = as_legacy(&record).encode_to_vec();
        // The old schema has no field 20, so the field's key byte pair
        // (tag 20, wire type 2 -> varint 0xa2 0x01) is not on the wire.
        assert!(
            !legacy_bytes.windows(2).any(|w| w == [0xa2, 0x01]),
            "legacy encoding must carry no field-20 key"
        );
        let decoded = record::decode(&legacy_bytes).expect("legacy record decodes");
        // Absent, asserted as absent: no entries at all, and therefore no
        // column and no defect. Not "one entry whose values are zero".
        assert_eq!(decoded.declared_column_stats.len(), 0);
        let read = read_commit_record(&decoded);
        assert!(read.covered().is_empty());
        assert!(read.dropped().is_empty());
        assert_eq!(read.column("EventDate"), None);
        // Absence of the field is a different state from an entry carrying
        // present-but-zero values, which decodes to a real covered column.
        let mut stamped = record;
        stamp_commit_record(&mut stamped, &[i64_stat("EventDate", 0, 0, 0)]);
        let stamped = record::decode(&record::encode(&stamped)).expect("decode stamped");
        let stamped_read = read_commit_record(&stamped);
        assert_eq!(stamped_read.covered().len(), 1);
        assert_eq!(
            stamped_read.column("EventDate").map(|s| s.min()),
            Some(Some(DeclaredStatValue::I64(0)))
        );
    }

    #[test]
    fn absent_extrema_are_distinct_from_zero_extrema_on_the_wire() {
        let all_null = DeclaredColumnStat::new("EventDate", DeclaredStatType::I64, None, None, 42)
            .expect("all-null stat");
        let zero = i64_stat("EventDate", 0, 0, 42);
        let all_null_wire = encode(&all_null);
        let zero_wire = encode(&zero);
        assert_eq!(all_null_wire.min, None);
        assert_eq!(
            zero_wire.min,
            Some(DeclaredColumnStatValue {
                kind: Some(Kind::I64(0))
            })
        );
        // A zero-valued extremum still occupies bytes on the wire; an absent
        // one occupies none. The two encodings differ by exactly eight bytes:
        // four per sub-message (field key, length, oneof arm key, value).
        assert_eq!(
            zero_wire.encode_to_vec().len() - all_null_wire.encode_to_vec().len(),
            8
        );
        let round_tripped = decode(&all_null_wire).expect("decode all-null");
        assert_eq!(round_tripped.min(), None);
        assert_eq!(round_tripped.max(), None);
        assert_eq!(round_tripped.null_count(), 42);
    }

    #[test]
    fn ineligible_declared_types_are_dropped_on_decode() {
        let mut record = base_record();
        record.declared_column_stats = vec![
            DeclaredColumnMinMax {
                name: "URL".to_string(),
                declared_type: TYPED_ATTR_COLUMN_TYPE_STR,
                min: None,
                max: None,
                null_count: 0,
                sum: None,
                sum_wide: Vec::new(),
                zero_count: None,
            },
            DeclaredColumnMinMax {
                name: "Payload".to_string(),
                declared_type: TYPED_ATTR_COLUMN_TYPE_BYTES,
                min: None,
                max: None,
                null_count: 0,
                sum: None,
                sum_wide: Vec::new(),
                zero_count: None,
            },
            DeclaredColumnMinMax {
                name: "Ratio".to_string(),
                declared_type: TYPED_ATTR_COLUMN_TYPE_F64,
                min: None,
                max: None,
                null_count: 0,
                sum: None,
                sum_wide: Vec::new(),
                zero_count: None,
            },
            DeclaredColumnMinMax {
                name: "Unset".to_string(),
                declared_type: TYPED_ATTR_COLUMN_TYPE_UNSPECIFIED,
                min: None,
                max: None,
                null_count: 0,
                sum: None,
                sum_wide: Vec::new(),
                zero_count: None,
            },
            encode(&i64_stat("EventDate", 1, 2, 0)),
        ];
        // Decoding the record itself still succeeds: a bad statistic never
        // makes a durable commit record unreadable.
        let decoded = record::decode(&record::encode(&record)).expect("record still decodes");
        let read = read_commit_record(&decoded);
        assert_eq!(
            read.covered().to_vec(),
            vec![i64_stat("EventDate", 1, 2, 0)]
        );
        assert_eq!(read.dropped().len(), 4);
        assert_eq!(
            read.dropped()[0],
            DroppedStatEntry {
                index: 0,
                name: "URL".to_string(),
                reason: DeclaredStatDefect::Invalid(DeclaredStatError::IneligibleType {
                    tag: TYPED_ATTR_COLUMN_TYPE_STR
                }),
            }
        );
        assert_eq!(
            read.dropped()[2].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::IneligibleType {
                tag: TYPED_ATTR_COLUMN_TYPE_F64
            })
        );
        assert_eq!(read.dropped()[3].index, 3);
        assert_eq!(read.column("Ratio"), None);
    }

    #[test]
    fn ineligible_type_is_refused_at_the_typed_boundary() {
        // The stamp side cannot express an ineligible type: the only way to
        // reach a declared type is through the allowlist.
        for tag in [
            TYPED_ATTR_COLUMN_TYPE_STR,
            TYPED_ATTR_COLUMN_TYPE_BYTES,
            TYPED_ATTR_COLUMN_TYPE_F64,
            TYPED_ATTR_COLUMN_TYPE_UNSPECIFIED,
        ] {
            assert_eq!(
                DeclaredStatType::from_tag(tag),
                Err(DeclaredStatError::IneligibleType { tag })
            );
        }
        // ...and every value a stamp can hold maps back to an allowlisted tag.
        let stamped = encode_all(&[i64_stat("EventDate", 1, 2, 0)]);
        assert_eq!(stamped.len(), 1);
        assert_eq!(stamped[0].declared_type, 2);
    }

    #[test]
    fn type_mismatched_and_half_present_entries_are_dropped() {
        let entries = vec![
            // BOOL column carrying an I64 min.
            DeclaredColumnMinMax {
                name: "IsRefresh".to_string(),
                declared_type: DeclaredStatType::Bool.tag(),
                min: Some(DeclaredColumnStatValue {
                    kind: Some(Kind::I64(0)),
                }),
                max: Some(DeclaredColumnStatValue {
                    kind: Some(Kind::B(true)),
                }),
                null_count: 0,
                sum: None,
                sum_wide: Vec::new(),
                zero_count: None,
            },
            // min present, max absent.
            DeclaredColumnMinMax {
                name: "EventDate".to_string(),
                declared_type: DeclaredStatType::I64.tag(),
                min: Some(DeclaredColumnStatValue {
                    kind: Some(Kind::I64(1)),
                }),
                max: None,
                null_count: 0,
                sum: None,
                sum_wide: Vec::new(),
                zero_count: None,
            },
            // min above max.
            DeclaredColumnMinMax {
                name: "Backwards".to_string(),
                declared_type: DeclaredStatType::I64.tag(),
                min: Some(DeclaredColumnStatValue {
                    kind: Some(Kind::I64(9)),
                }),
                max: Some(DeclaredColumnStatValue {
                    kind: Some(Kind::I64(8)),
                }),
                null_count: 0,
                sum: None,
                sum_wide: Vec::new(),
                zero_count: None,
            },
            // Present value message with no kind set.
            DeclaredColumnMinMax {
                name: "Empty".to_string(),
                declared_type: DeclaredStatType::I64.tag(),
                min: Some(DeclaredColumnStatValue { kind: None }),
                max: Some(DeclaredColumnStatValue { kind: None }),
                null_count: 0,
                sum: None,
                sum_wide: Vec::new(),
                zero_count: None,
            },
            // Empty column name.
            DeclaredColumnMinMax {
                name: String::new(),
                declared_type: DeclaredStatType::I64.tag(),
                min: None,
                max: None,
                null_count: 0,
                sum: None,
                sum_wide: Vec::new(),
                zero_count: None,
            },
        ];
        let read = decode_all(&entries);
        assert!(read.covered().is_empty());
        assert_eq!(read.dropped().len(), 5);
        assert_eq!(
            read.dropped()[0].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::ValueTypeMismatch {
                name: "IsRefresh".to_string(),
                declared: DeclaredStatType::Bool,
                actual: DeclaredStatType::I64,
                which: "min",
            })
        );
        assert_eq!(
            read.dropped()[1].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::PresenceMismatch {
                name: "EventDate".to_string(),
                min_state: "present",
                max_state: "absent",
            })
        );
        assert_eq!(
            read.dropped()[2].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::MinAboveMax {
                name: "Backwards".to_string(),
                min: DeclaredStatValue::I64(9),
                max: DeclaredStatValue::I64(8),
            })
        );
        assert_eq!(
            read.dropped()[3].reason,
            DeclaredStatDefect::EmptyValueKind {
                name: "Empty".to_string(),
                which: "min",
            }
        );
        assert_eq!(
            read.dropped()[4].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::EmptyName)
        );
    }

    #[test]
    fn duplicate_column_entries_all_drop() {
        // Both orderings of the same duplicate pair, which is what makes this
        // test able to fail: any pick-one resolution (first-wins, last-wins,
        // widest range, highest null count) satisfies whichever ordering puts
        // its pick where the assertion looks, so a single ordering cannot
        // distinguish "no coverage" from "a pick that happens to match".
        // ADR-0873 clause 6 fixes the agreed answer as no coverage at all.
        let narrow = i64_stat("EventDate", 1, 2, 0);
        let wide = i64_stat("EventDate", 100, 200, 5);
        for (first, second) in [(&narrow, &wide), (&wide, &narrow)] {
            let entries = vec![encode(first), encode(second)];
            let read = decode_all(&entries);
            assert!(
                read.covered().is_empty(),
                "a duplicated name grants no coverage at all"
            );
            assert_eq!(read.column("EventDate"), None);
            // BOTH occurrences are dropped, at their exact indices, which is
            // what the defect counter attributes by.
            assert_eq!(
                read.dropped().to_vec(),
                vec![
                    DroppedStatEntry {
                        index: 0,
                        name: "EventDate".to_string(),
                        reason: DeclaredStatDefect::DuplicateName("EventDate".to_string()),
                    },
                    DroppedStatEntry {
                        index: 1,
                        name: "EventDate".to_string(),
                        reason: DeclaredStatDefect::DuplicateName("EventDate".to_string()),
                    },
                ]
            );
        }
    }

    #[test]
    fn duplicates_do_not_disturb_the_order_of_the_entries_around_them() {
        // Order is record order for both lists, and a duplicated name drops
        // out entirely -- every occurrence of it -- without reordering the
        // singly-named entries around it.
        let entries = vec![
            encode(&i64_stat("Aaa", 1, 2, 0)),
            encode(&i64_stat("Bbb", 3, 4, 0)),
            encode(&i64_stat("Aaa", 5, 6, 0)),
            encode(&i64_stat("Ccc", 7, 8, 0)),
            encode(&i64_stat("Bbb", 9, 10, 0)),
        ];
        let read = decode_all(&entries);
        assert_eq!(read.covered().to_vec(), vec![i64_stat("Ccc", 7, 8, 0)]);
        assert_eq!(
            read.dropped()
                .iter()
                .map(|d| (d.index, d.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(0, "Aaa"), (1, "Bbb"), (2, "Aaa"), (4, "Bbb")]
        );
    }

    fn base_part(sample_count: u64) -> CompactionPart {
        CompactionPart {
            part_index: 0,
            first_series_id: vec![0x01; 16],
            last_series_id: vec![0x02; 16],
            content_hash: vec![0x03; 32],
            object_size: 8192,
            sample_count,
            series_count: 8,
            run_count: 1,
            min_event_ts_ns: 10,
            max_event_ts_ns: 20,
            segment_format_version: 3,
            declared_column_stats: vec![],
        }
    }

    /// A commit record of `sample_count` rows carrying `entries`, round-tripped
    /// through the wire so every assertion is about a decoded record.
    fn decoded_record(sample_count: u64, entries: Vec<DeclaredColumnMinMax>) -> CommitRecord {
        let mut record = base_record();
        record.sample_count = sample_count;
        record.declared_column_stats = entries;
        record::decode(&record::encode(&record)).expect("record decodes")
    }

    /// The same for the compaction-part carrier, whose row count is field 6.
    fn decoded_part(sample_count: u64, entries: Vec<DeclaredColumnMinMax>) -> CompactionPart {
        let mut part = base_part(sample_count);
        part.declared_column_stats = entries;
        CompactionPart::decode(part.encode_to_vec().as_slice()).expect("part decodes")
    }

    fn all_null_stat(name: &str, null_count: u64) -> DeclaredColumnStat {
        DeclaredColumnStat::new(name, DeclaredStatType::I64, None, None, null_count)
            .expect("valid all-null stat")
    }

    /// One entry in, one drop out: nothing covered, exactly one dropped entry
    /// at index 0, and the column reads as uncovered.
    fn assert_single_drop(read: &ValidatedDeclaredStats, name: &str, reason: DeclaredStatDefect) {
        assert!(read.covered().is_empty());
        assert_eq!(read.covered().len(), 0);
        assert_eq!(read.dropped().len(), 1);
        assert_eq!(
            read.dropped()[0],
            DroppedStatEntry {
                index: 0,
                name: name.to_string(),
                reason,
            }
        );
        assert_eq!(read.column(name), None);
    }

    // Clause 4: more NULLs than the segment has rows.

    #[test]
    fn null_count_above_sample_count_is_dropped_on_a_commit_record() {
        // Eight NULLs in a seven-row object.
        let entries = vec![encode(&i64_stat("EventDate", 1, 2, 8))];
        let record = decoded_record(7, entries);
        assert_single_drop(
            &read_commit_record(&record),
            "EventDate",
            DeclaredStatDefect::NullCountAboveSampleCount {
                name: "EventDate".to_string(),
                null_count: 8,
                sample_count: 7,
            },
        );
        // The clause is not decidable one layer down, which is why it lives at
        // the record layer: the same entry slice, with no row count to
        // reconcile against, is carrier-agnostically clean.
        let entry_only = decode_all(&record.declared_column_stats);
        assert_eq!(entry_only.dropped().len(), 0);
        assert_eq!(entry_only.covered().len(), 1);
    }

    #[test]
    fn null_count_above_sample_count_is_dropped_on_a_compaction_part() {
        let part = decoded_part(7, vec![encode(&i64_stat("EventDate", 1, 2, 8))]);
        assert_single_drop(
            &read_compaction_part(&part),
            "EventDate",
            DeclaredStatDefect::NullCountAboveSampleCount {
                name: "EventDate".to_string(),
                null_count: 8,
                sample_count: 7,
            },
        );
    }

    // Clause 5, absent direction: both extrema absent claims zero non-null
    // values, so every row must be NULL.

    #[test]
    fn absent_extrema_disagreeing_with_the_null_count_are_dropped_on_a_commit_record() {
        let record = decoded_record(7, vec![encode(&all_null_stat("EventDate", 4))]);
        assert_single_drop(
            &read_commit_record(&record),
            "EventDate",
            DeclaredStatDefect::PresenceDisagreesWithNullCount {
                name: "EventDate".to_string(),
                extrema: "absent",
                null_count: 4,
                sample_count: 7,
            },
        );
    }

    #[test]
    fn absent_extrema_disagreeing_with_the_null_count_are_dropped_on_a_compaction_part() {
        let part = decoded_part(7, vec![encode(&all_null_stat("EventDate", 4))]);
        assert_single_drop(
            &read_compaction_part(&part),
            "EventDate",
            DeclaredStatDefect::PresenceDisagreesWithNullCount {
                name: "EventDate".to_string(),
                extrema: "absent",
                null_count: 4,
                sample_count: 7,
            },
        );
    }

    // Clause 5, present direction: issue #970 defect 2. Extrema claimed for a
    // column whose own null count says every row reads NULL are fabricated. A
    // validator that checks only the absent direction above passes every other
    // case in this matrix and still accepts this entry.

    #[test]
    fn present_extrema_over_an_all_null_column_are_dropped_on_a_commit_record() {
        let record = decoded_record(7, vec![encode(&i64_stat("EventDate", 1, 2, 7))]);
        assert_single_drop(
            &read_commit_record(&record),
            "EventDate",
            DeclaredStatDefect::PresenceDisagreesWithNullCount {
                name: "EventDate".to_string(),
                extrema: "present",
                null_count: 7,
                sample_count: 7,
            },
        );
    }

    #[test]
    fn present_extrema_over_an_all_null_column_are_dropped_on_a_compaction_part() {
        let part = decoded_part(7, vec![encode(&i64_stat("EventDate", 1, 2, 7))]);
        assert_single_drop(
            &read_compaction_part(&part),
            "EventDate",
            DeclaredStatDefect::PresenceDisagreesWithNullCount {
                name: "EventDate".to_string(),
                extrema: "present",
                null_count: 7,
                sample_count: 7,
            },
        );
    }

    #[test]
    fn row_count_boundaries_that_are_valid_keep_their_coverage() {
        // Each pair is (the carrier's row count, the entry), and each is one
        // step from a case the two clauses above drop. A clause written with
        // the wrong comparison silently uncovers a valid column, which is
        // worse than the gap it closes.
        let cases = [
            // Both absent, null_count == sample_count: every row is NULL.
            (7, all_null_stat("EventDate", 7)),
            // Both present, one row short of all-NULL: exactly one non-null.
            (7, i64_stat("EventDate", 1, 2, 6)),
            // An empty object: 0 == 0 is agreement, not a defect.
            (0, all_null_stat("EventDate", 0)),
            // No NULLs at all, the common case.
            (7, i64_stat("EventDate", 1, 2, 0)),
        ];
        for (sample_count, stat) in cases {
            let entries = vec![encode(&stat)];
            let record = decoded_record(sample_count, entries.clone());
            let read = read_commit_record(&record);
            assert!(
                read.dropped().is_empty(),
                "record, sample_count {sample_count}"
            );
            assert_eq!(read.covered().to_vec(), vec![stat.clone()]);
            assert_eq!(read.covered().len(), 1);

            let part = decoded_part(sample_count, entries);
            let read = read_compaction_part(&part);
            assert!(
                read.dropped().is_empty(),
                "part, sample_count {sample_count}"
            );
            assert_eq!(read.covered().to_vec(), vec![stat]);
            assert_eq!(read.covered().len(), 1);
        }
    }

    #[test]
    fn one_row_count_defect_does_not_poison_the_rest_of_its_carrier() {
        // ADR-0873 decision 2, granularity split: the stamp is entry-granular.
        let valid_head = i64_stat("EventDate", 1, 2, 6);
        let valid_tail = i64_stat("Latency", 3, 4, 0);
        let entries = vec![
            encode(&valid_head),
            // Extrema over an all-NULL column, at index 1.
            encode(&i64_stat("Status", 200, 500, 7)),
            encode(&valid_tail),
        ];

        let record = decoded_record(7, entries.clone());
        // The carrying record decoded intact: a defective statistic never
        // makes a durable commit record unreadable.
        assert_eq!(record.declared_column_stats.len(), 3);
        assert_eq!(record.sample_count, 7);
        let read = read_commit_record(&record);
        assert_eq!(
            read.covered().to_vec(),
            vec![valid_head.clone(), valid_tail.clone()]
        );
        assert_eq!(read.covered().len(), 2);
        assert_eq!(read.dropped().len(), 1);
        assert_eq!(read.dropped()[0].index, 1);
        assert_eq!(read.dropped()[0].name, "Status");
        assert_eq!(read.column("Status"), None);
        assert_eq!(read.column("EventDate"), Some(&valid_head));

        let part = decoded_part(7, entries);
        assert_eq!(part.declared_column_stats.len(), 3);
        let read = read_compaction_part(&part);
        assert_eq!(
            read.covered().to_vec(),
            vec![valid_head, valid_tail.clone()]
        );
        assert_eq!(read.covered().len(), 2);
        assert_eq!(read.dropped().len(), 1);
        assert_eq!(read.dropped()[0].index, 1);
        assert_eq!(read.column("Status"), None);
        assert_eq!(read.column("Latency"), Some(&valid_tail));
    }

    #[test]
    fn an_invalid_duplicate_never_lets_its_twin_gain_coverage() {
        // A duplicated name grants no coverage in any occurrence (ADR-0873
        // clause 6), so an individually valid entry stays uncovered when
        // another entry claims its name -- even one dropped for a defect of
        // its own. The name is the conviction boundary, not the entry, so both
        // occurrences report DuplicateName rather than the first reporting its
        // own defect: the reader never got far enough to judge either entry on
        // its merits.
        let entries = vec![
            // Index 0: individually defective too (extrema present over an
            // all-NULL column), and it claims "d".
            encode(&i64_stat("d", 1, 9, 7)),
            // Index 1: individually valid, same name -- must NOT be covered.
            encode(&i64_stat("d", 1, 9, 3)),
            // Index 2: unrelated valid entry keeps its coverage.
            encode(&i64_stat("ok", 5, 6, 0)),
        ];
        let read = read_commit_record(&decoded_record(7, entries));
        assert_eq!(read.covered().len(), 1);
        assert_eq!(read.column("d"), None);
        assert_eq!(read.column("ok").map(|s| s.name()), Some("ok"));
        assert_eq!(read.dropped().len(), 2);
        for (slot, index) in [(0usize, 0usize), (1, 1)] {
            assert_eq!(read.dropped()[slot].index, index);
            assert!(
                matches!(
                    read.dropped()[slot].reason,
                    DeclaredStatDefect::DuplicateName(ref n) if n == "d"
                ),
                "got {:?}",
                read.dropped()[slot].reason
            );
        }
    }

    #[test]
    fn an_entry_count_above_any_declared_column_list_is_read_in_full() {
        // ADR-0873 decision 2 has no entry-count cap, and nothing in the
        // tenant-config declared-column path bounds the count either, so a
        // record carrying far more entries than any real declaration decodes
        // whole: no truncation, no error, no dropped entry. A cap added later
        // would fail here, which is where its derivation belongs.
        let count = 4_096;
        let entries: Vec<_> = (0..count)
            .map(|i| encode(&i64_stat(&format!("Column{i}"), 0, i, 0)))
            .collect();
        let read = decode_all(&entries);
        assert_eq!(read.covered().len(), usize::try_from(count).expect("fits"));
        assert!(read.dropped().is_empty());
        assert_eq!(
            read.column("Column4095").map(|s| s.max()),
            Some(Some(DeclaredStatValue::I64(4_095)))
        );
    }

    #[test]
    fn truncated_stamped_record_bytes_produce_a_typed_error() {
        let mut record = base_record();
        stamp_commit_record(&mut record, &[i64_stat("EventDate", -1, 1, 3)]);
        let bytes = record::encode(&record);
        // Cutting inside the field-20 submessage must be a typed decode
        // error, never a panic and never a partially trusted stat.
        let err = record::decode(&bytes[..bytes.len() - 1]).expect_err("truncated bytes refused");
        assert!(matches!(err, record::RecordError::Decode(_)), "got {err:?}");
        // Every prefix either fails with a typed error or decodes to a record
        // whose stats are a subset of what was written: prost decodes whole
        // length-delimited fields only, so no half-read stat is ever trusted.
        let written = read_commit_record(&record).covered().to_vec();
        assert_eq!(written.len(), 1);
        for cut in 0..bytes.len() {
            if let Ok(decoded) = record::decode(&bytes[..cut]) {
                let read = read_commit_record(&decoded);
                assert!(
                    read.dropped().is_empty(),
                    "cut {cut} produced a defective stat"
                );
                assert!(
                    read.covered().iter().all(|stat| written.contains(stat)),
                    "cut {cut} invented a stat"
                );
            }
        }
    }

    /// ADR-0873 decision 2 ("where the predicate binds"): a
    /// [`ValidatedDeclaredStats`] is the coverage grant, and it is
    /// unconstructable without the full predicate. Its fields are private and
    /// its only constructor (`decode_all_against_rows`) is private to the
    /// module, so outside this module the sole producers are
    /// [`read_commit_record`] and [`read_compaction_part`], each of which
    /// supplies the carrier's row count. A downstream (wave-2) reader that
    /// holds a `ValidatedDeclaredStats` therefore holds proof that the
    /// row-count clauses ran; it has no other way to build one.
    ///
    /// The structural half of the guarantee -- that no bypass exists -- is
    /// enforced by that privacy and cannot be exercised from outside the
    /// module, so it needs no runtime assertion (an attempted direct
    /// construction in a sibling crate would simply not compile). This test
    /// pins the constructive half: the two read functions ARE sufficient, so a
    /// reader needs nothing beyond them to obtain coverage.
    #[test]
    fn the_two_read_functions_are_the_sufficient_producers_of_coverage() {
        let mut record = base_record();
        stamp_commit_record(&mut record, &[i64_stat("EventDate", 1, 2, 0)]);
        let record = record::decode(&record::encode(&record)).expect("decode");
        let from_commit: ValidatedDeclaredStats = read_commit_record(&record);
        assert_eq!(from_commit.covered().len(), 1);
        assert!(from_commit.column("EventDate").is_some());

        let mut part = base_part(SAMPLE_COUNT);
        stamp_compaction_part(&mut part, &[i64_stat("EventDate", 1, 2, 0)]);
        let part = CompactionPart::decode(part.encode_to_vec().as_slice()).expect("decode part");
        let from_part: ValidatedDeclaredStats = read_compaction_part(&part);
        assert_eq!(from_part.covered().len(), 1);
        assert!(from_part.column("EventDate").is_some());
    }

    /// Regression (ADR-0873 decision 2): the row-count clauses cannot be
    /// skipped through any public path. An entry with `null_count >
    /// sample_count` (clause 4) is dropped by BOTH public readers, because both
    /// pass the carrier's row count to the shared pass and there is no public
    /// route that omits it.
    ///
    /// The closed bypass is demonstrated against the module-private entry-local
    /// pass: the same entries through `decode_all_against_rows(_, None)` -- the
    /// only unvalidated route, now private -- leave the offending entry
    /// COVERED, because with no row count clauses 4 and 5 cannot bind.
    /// Re-exposing that route is exactly the flip that would let the
    /// unvalidated entry grant coverage: in `read_commit_record`, passing
    /// `None` instead of `Some(record.sample_count)` to
    /// `decode_all_against_rows` reproduces the pre-fix hole, and it is
    /// unreachable from outside the module.
    #[test]
    fn row_count_clauses_are_not_skippable_through_any_public_path() {
        // Eight NULLs in a seven-row object: clause 4 violated.
        let entries = vec![encode(&i64_stat("EventDate", 1, 2, 8))];

        let record = decoded_record(7, entries.clone());
        let read = read_commit_record(&record);
        assert!(read.column("EventDate").is_none());
        assert_eq!(read.covered().len(), 0);
        assert_eq!(read.dropped().len(), 1);
        assert_eq!(
            read.dropped()[0].reason,
            DeclaredStatDefect::NullCountAboveSampleCount {
                name: "EventDate".to_string(),
                null_count: 8,
                sample_count: 7,
            }
        );

        let part = decoded_part(7, entries);
        let read = read_compaction_part(&part);
        assert!(read.column("EventDate").is_none());
        assert_eq!(read.covered().len(), 0);
        assert_eq!(read.dropped().len(), 1);

        // The unvalidated entry-local route is the bypass this change closes:
        // with no row count, the very same entry is covered. This is the state
        // re-exposing the flipped line would let a public reader observe.
        let unvalidated = decode_all(&record.declared_column_stats);
        assert_eq!(unvalidated.covered().len(), 1);
        assert_eq!(unvalidated.dropped().len(), 0);
        assert!(unvalidated.column("EventDate").is_some());
    }

    fn stat_strategy() -> impl Strategy<Value = DeclaredColumnStat> {
        let name = "[a-zA-Z][a-zA-Z0-9_]{0,12}";
        let i64_pair = (any::<i64>(), any::<i64>()).prop_map(|(a, b)| {
            let (min, max) = if a <= b { (a, b) } else { (b, a) };
            (
                Some(DeclaredStatValue::I64(min)),
                Some(DeclaredStatValue::I64(max)),
            )
        });
        let bool_pair = (any::<bool>(), any::<bool>()).prop_map(|(a, b)| {
            let (min, max) = if a <= b { (a, b) } else { (b, a) };
            (
                Some(DeclaredStatValue::Bool(min)),
                Some(DeclaredStatValue::Bool(max)),
            )
        });
        let extrema = prop_oneof![
            i64_pair.prop_map(|p| (DeclaredStatType::I64, p)),
            bool_pair.prop_map(|p| (DeclaredStatType::Bool, p)),
            // The all-null case: both extrema absent, still exact.
            prop::sample::select(vec![DeclaredStatType::I64, DeclaredStatType::Bool])
                .prop_map(|ty| (ty, (None, None))),
        ];
        // Both-present admits any null count below the row count; both-absent
        // admits exactly the row count, which is what "every row is NULL"
        // means.
        (name, extrema, 0..SAMPLE_COUNT).prop_map(|(name, (ty, (min, max)), non_null_nulls)| {
            let null_count = if min.is_some() {
                non_null_nulls
            } else {
                SAMPLE_COUNT
            };
            DeclaredColumnStat::new(name, ty, min, max, null_count).expect("strategy builds valid")
        })
    }

    /// Distinct names, so the duplicate rule does not shrink the list.
    fn stats_strategy() -> impl Strategy<Value = Vec<DeclaredColumnStat>> {
        prop::collection::vec(stat_strategy(), 0..6).prop_map(|stats| {
            let mut seen = Vec::new();
            let mut out = Vec::new();
            for stat in stats {
                if !seen.contains(&stat.name().to_string()) {
                    seen.push(stat.name().to_string());
                    out.push(stat);
                }
            }
            out
        })
    }

    proptest! {
        #[test]
        fn stamped_stats_round_trip_through_a_commit_record(stats in stats_strategy()) {
            let mut record = base_record();
            stamp_commit_record(&mut record, &stats);
            let decoded = record::decode(&record::encode(&record)).expect("decode");
            let read = read_commit_record(&decoded);
            prop_assert!(read.dropped().is_empty());
            prop_assert_eq!(read.covered().to_vec(), stats);
        }

        #[test]
        fn stamped_stats_round_trip_through_a_compaction_part(stats in stats_strategy()) {
            let mut part = base_part(SAMPLE_COUNT);
            stamp_compaction_part(&mut part, &stats);
            let bytes = part.encode_to_vec();
            let decoded = CompactionPart::decode(bytes.as_slice()).expect("decode");
            let read = read_compaction_part(&decoded);
            prop_assert!(read.dropped().is_empty());
            prop_assert_eq!(read.covered().to_vec(), stats);
        }

        // Truncating a stamped record at any point is a typed error or a
        // clean shorter record, never a panic and never a trusted stat that
        // was not written.
        #[test]
        fn truncated_stamped_bytes_never_panic(stats in stats_strategy(), cut in 0usize..512) {
            let mut record = base_record();
            stamp_commit_record(&mut record, &stats);
            let bytes = record::encode(&record);
            let n = cut.min(bytes.len());
            if let Ok(decoded) = record::decode(&bytes[..n]) {
                let read = read_commit_record(&decoded);
                prop_assert!(read.dropped().is_empty());
                prop_assert!(read.covered().iter().all(|s| stats.contains(s)));
            }
        }

        // Arbitrary bytes in the field-20 position: a decoded record must
        // either drop the entry or produce a self-consistent stat, never
        // panic and never a min above its max.
        #[test]
        fn arbitrary_bytes_never_panic(raw in prop::collection::vec(any::<u8>(), 0..192)) {
            if let Ok(record) = CommitRecord::decode(raw.as_slice()) {
                let read = read_commit_record(&record);
                for stat in read.covered() {
                    prop_assert!(!stat.name().is_empty());
                    if let (Some(min), Some(max)) = (stat.min(), stat.max()) {
                        prop_assert_eq!(min.stat_type(), stat.declared_type());
                        prop_assert_eq!(max.stat_type(), stat.declared_type());
                    } else {
                        prop_assert_eq!(stat.min(), None);
                        prop_assert_eq!(stat.max(), None);
                    }
                }
            }
        }

        #[test]
        fn arbitrary_entry_fields_decode_or_drop(
            name in ".{0,8}",
            declared_type in 0u32..8,
            min in proptest::option::of(any::<i64>()),
            max in proptest::option::of(any::<i64>()),
            null_count in any::<u64>(),
        ) {
            let entry = DeclaredColumnMinMax {
                name,
                declared_type,
                min: min.map(|v| DeclaredColumnStatValue { kind: Some(Kind::I64(v)) }),
                max: max.map(|v| DeclaredColumnStatValue { kind: Some(Kind::I64(v)) }),
                null_count,
                sum: None,
                sum_wide: Vec::new(),
                zero_count: None,
            };
            match decode(&entry) {
                Ok(stat) => {
                    // An accepted entry's type is exactly what the allowlist
                    // resolved the stored tag to, and every extremum it kept
                    // is an I64 one, because that is all this strategy
                    // stores: a BOOL entry is only ever accepted with both
                    // extrema absent.
                    prop_assert_eq!(
                        Ok(stat.declared_type()),
                        DeclaredStatType::from_tag(declared_type)
                    );
                    if stat.min().is_some() || stat.max().is_some() {
                        prop_assert_eq!(stat.declared_type(), DeclaredStatType::I64);
                        prop_assert_eq!(stat.min(), min.map(DeclaredStatValue::I64));
                        prop_assert_eq!(stat.max(), max.map(DeclaredStatValue::I64));
                    }
                    prop_assert_eq!(stat.null_count(), null_count);
                    prop_assert!(!stat.name().is_empty());
                }
                Err(_) => {
                    // Dropped, and the record-level read reports exactly one
                    // defect for it.
                    let read = decode_all(&[entry]);
                    prop_assert_eq!(read.covered().len(), 0);
                    prop_assert_eq!(read.dropped().len(), 1);
                }
            }
        }
    }

    /// The scrape-side accessor pairs each carrier with that carrier's own
    /// tally, in `StatCarrier::ALL` order, so a renderer iterating it cannot
    /// put one carrier's count under another's label.
    ///
    /// The delta is taken over `cstat`, the one carrier nothing in this crate
    /// observes on its own (`ravel_sql`'s `.cstat` reader is its only
    /// reporter). The tallies are process-global and the other tests in this
    /// binary run beside this one, moving the three read-side carriers as they
    /// go, so only the `cstat` delta is an exact figure here.
    #[test]
    fn drops_observed_all_pairs_every_carrier_with_its_own_tally() {
        let before = declared_stat_drops_observed_all();
        assert_eq!(
            before.map(|(carrier, _)| carrier),
            StatCarrier::ALL,
            "the accessor must report carriers in StatCarrier::ALL order"
        );

        observe_declared_stat_drops(StatCarrier::Cstat, 3);

        let after = declared_stat_drops_observed_all();
        let cstat = after
            .iter()
            .position(|(carrier, _)| *carrier == StatCarrier::Cstat)
            .expect("cstat is one of the four carriers");
        assert_eq!(
            after[cstat].1 - before[cstat].1,
            3,
            "three observations under cstat must land on the cstat pair exactly"
        );
    }

    // ADR-4628: the sum (fields 6 and 7) and the zero count (field 8).

    /// The bounds ADR-4628 decision 2 gives for a sum: an object holds at
    /// most 2^64 - 1 rows of i64 values.
    const MAX_ROWS: u64 = u64::MAX;

    /// A valid I64 stat over `rows` rows, all non-null, with the given sum
    /// and zero count.
    fn i64_sum_stat(
        min: i64,
        max: i64,
        null_count: u64,
        sum: Option<i128>,
        zero_count: Option<u64>,
    ) -> DeclaredColumnStat {
        let mut stat = i64_stat("UserID", min, max, null_count);
        if let Some(sum) = sum {
            stat = stat.with_sum(sum).expect("sum is entry-locally valid");
        }
        if let Some(zero_count) = zero_count {
            stat = stat
                .with_zero_count(zero_count)
                .expect("zero count is entry-locally valid");
        }
        stat
    }

    /// The field that carries the sum on the wire: `Some(false)` for field 6,
    /// `Some(true)` for field 7, `None` for no sum. Panics on both, which the
    /// encoder must never write.
    fn sum_field(wire: &DeclaredColumnMinMax) -> Option<bool> {
        match (wire.sum.is_some(), !wire.sum_wide.is_empty()) {
            (false, false) => None,
            (true, false) => Some(false),
            (false, true) => Some(true),
            (true, true) => panic!("encoder wrote both sum fields"),
        }
    }

    /// Encode `stat` onto a commit record of `sample_count` rows, through the
    /// commit wire and through the catalog mirror's wire, and read it back
    /// through both. Returns the commit-side wire entry for field assertions.
    fn round_trip_both_mirrors(
        stat: &DeclaredColumnStat,
        sample_count: u64,
    ) -> Result<DeclaredColumnMinMax, TestCaseError> {
        let mut record = base_record();
        record.sample_count = sample_count;
        stamp_commit_record(&mut record, std::slice::from_ref(stat));
        let decoded = record::decode(&record::encode(&record)).expect("record decodes");
        let read = read_commit_record(&decoded);
        prop_assert!(read.dropped().is_empty(), "dropped: {:?}", read.dropped());
        prop_assert_eq!(read.covered(), std::slice::from_ref(stat));

        // The catalog mirror has the same field numbers, so the commit
        // entry's bytes decode as it and its bytes decode back unchanged.
        let wire = decoded.declared_column_stats[0].clone();
        let mirror =
            ravel_proto::catalog::v1::DeclaredColumnMinMax::decode(wire.encode_to_vec().as_slice())
                .expect("catalog mirror decodes");
        prop_assert_eq!(mirror.sum, wire.sum);
        prop_assert_eq!(&mirror.sum_wide, &wire.sum_wide);
        prop_assert_eq!(mirror.zero_count, wire.zero_count);
        let back = DeclaredColumnMinMax::decode(mirror.encode_to_vec().as_slice())
            .expect("commit entry decodes from the mirror's bytes");
        prop_assert_eq!(&back, &wire);
        let twin = CommitRecord {
            sample_count,
            declared_column_stats: vec![back],
            ..Default::default()
        };
        let read = read_snapshot_entry_twin(&twin);
        prop_assert!(read.dropped().is_empty(), "dropped: {:?}", read.dropped());
        prop_assert_eq!(read.covered(), std::slice::from_ref(stat));
        Ok(wire)
    }

    /// Asserts field 6 carries a sum exactly when it fits i64, and field 7
    /// carries it as 16 bytes little-endian otherwise.
    fn assert_sum_field(wire: &DeclaredColumnMinMax, sum: i128) -> Result<(), TestCaseError> {
        match i64::try_from(sum) {
            Ok(narrow) => {
                prop_assert_eq!(
                    sum_field(wire),
                    Some(false),
                    "sum {} belongs in field 6",
                    sum
                );
                prop_assert_eq!(wire.sum, Some(narrow));
            }
            Err(_) => {
                prop_assert_eq!(
                    sum_field(wire),
                    Some(true),
                    "sum {} belongs in field 7",
                    sum
                );
                prop_assert_eq!(wire.sum_wide.len(), 16);
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&wire.sum_wide);
                prop_assert_eq!(i128::from_le_bytes(bytes), sum);
            }
        }
        Ok(())
    }

    /// The sum boundaries the property test must cover, each with the stat it
    /// is valid on and the field that must carry it.
    #[test]
    fn sum_boundaries_pick_field_six_or_seven_and_round_trip() {
        let low_extreme = -i128::from(MAX_ROWS) * (1i128 << 63);
        let high_extreme = i128::from(MAX_ROWS) * i128::from(i64::MAX);
        let cases: [(i128, bool); 7] = [
            (0, false),
            (i128::from(i64::MAX), false),
            (i128::from(i64::MIN), false),
            (i128::from(i64::MAX) + 1, true),
            (i128::from(i64::MIN) - 1, true),
            (low_extreme, true),
            (high_extreme, true),
        ];
        for (sum, wide) in cases {
            // Over u64::MAX rows spanning the whole i64 range, every case
            // lies inside [n * min, n * max].
            let stat = i64_sum_stat(i64::MIN, i64::MAX, 0, Some(sum), Some(1));
            let wire = round_trip_both_mirrors(&stat, MAX_ROWS).expect("round trip");
            assert_eq!(sum_field(&wire), Some(wide), "sum {sum}");
            assert_sum_field(&wire, sum).expect("field choice");
        }
        assert_eq!(low_extreme, -(i128::from(u64::MAX) << 63));
        assert!(low_extreme > i128::MIN && high_extreme < i128::MAX);
    }

    /// A sum drawn from the boundary points and from anywhere in the range.
    fn sum_in(low: i128, high: i128) -> impl Strategy<Value = i128> {
        let specials = vec![
            0,
            i128::from(i64::MAX),
            i128::from(i64::MIN),
            i128::from(i64::MAX) + 1,
            i128::from(i64::MIN) - 1,
            low,
            high,
        ];
        prop_oneof![
            prop::sample::select(specials),
            any::<i128>(),
            any::<i64>().prop_map(i128::from),
        ]
        .prop_map(move |s| s.clamp(low, high))
    }

    /// A row count, with u64::MAX and the small counts weighted in.
    fn rows_strategy() -> impl Strategy<Value = u64> {
        prop_oneof![Just(MAX_ROWS), 1u64..4, any::<u64>().prop_map(|n| n.max(1)),]
    }

    fn special_i64() -> impl Strategy<Value = i64> {
        prop_oneof![
            prop::sample::select(vec![
                i64::MIN,
                i64::MIN + 1,
                -1,
                0,
                1,
                i64::MAX - 1,
                i64::MAX
            ]),
            any::<i64>(),
        ]
    }

    /// A valid I64 or BOOL stat carrying any combination of the new fields,
    /// with the row count of the record that carries it.
    fn summed_stat_strategy() -> impl Strategy<Value = (DeclaredColumnStat, u64)> {
        let i64_entry = (
            rows_strategy(),
            any::<u64>(),
            special_i64(),
            special_i64(),
            any::<bool>(),
            any::<bool>(),
            any::<u64>(),
        )
            .prop_flat_map(|(rows, nulls, a, b, has_sum, has_zero, zero_seed)| {
                let null_count = nulls % rows;
                let non_null = rows - null_count;
                let (min, max) = if a <= b { (a, b) } else { (b, a) };
                let low = i128::from(non_null) * i128::from(min);
                let high = i128::from(non_null) * i128::from(max);
                sum_in(low, high).prop_map(move |sum| {
                    let zero_count = if min == 0 && max == 0 {
                        non_null
                    } else if min <= 0 && 0 <= max {
                        zero_seed % (non_null.saturating_add(1)).max(1)
                    } else {
                        0
                    };
                    let stat = i64_sum_stat(
                        min,
                        max,
                        null_count,
                        has_sum.then_some(sum),
                        has_zero.then_some(zero_count),
                    );
                    (stat, rows)
                })
            });
        let bool_entry = (
            rows_strategy(),
            any::<u64>(),
            any::<bool>(),
            any::<bool>(),
            any::<u64>(),
        )
            .prop_map(|(rows, nulls, a, b, zero_seed)| {
                let null_count = nulls % rows;
                let non_null = rows - null_count;
                let (min, max) = if a <= b { (a, b) } else { (b, a) };
                let zero_count = match (min, max) {
                    (false, false) => non_null,
                    (false, true) => zero_seed % non_null.saturating_add(1).max(1),
                    _ => 0,
                };
                let stat = DeclaredColumnStat::new(
                    "IsRefresh",
                    DeclaredStatType::Bool,
                    Some(DeclaredStatValue::Bool(min)),
                    Some(DeclaredStatValue::Bool(max)),
                    null_count,
                )
                .expect("valid bool stat")
                .with_zero_count(zero_count)
                .expect("valid zero count");
                (stat, rows)
            });
        let all_null = (rows_strategy(), any::<bool>(), any::<bool>()).prop_map(
            |(rows, has_sum, has_zero)| {
                let mut stat =
                    DeclaredColumnStat::new("UserID", DeclaredStatType::I64, None, None, rows)
                        .expect("all-null stat");
                if has_sum {
                    stat = stat.with_sum(0).expect("zero sum over no values");
                }
                if has_zero {
                    stat = stat.with_zero_count(0).expect("zero count over no values");
                }
                (stat, rows)
            },
        );
        prop_oneof![4 => i64_entry, 2 => bool_entry, 1 => all_null]
    }

    proptest! {
        /// T1 of issue #4664: every valid entry carrying the new fields
        /// encodes and decodes to itself through both mirrors, with the sum
        /// on field 6 exactly when it fits i64 and on field 7 otherwise.
        #[test]
        fn summed_stats_round_trip_through_both_mirrors(
            (stat, rows) in summed_stat_strategy()
        ) {
            let wire = round_trip_both_mirrors(&stat, rows)?;
            match stat.sum() {
                Some(sum) => assert_sum_field(&wire, sum)?,
                None => prop_assert_eq!(sum_field(&wire), None),
            }
            prop_assert_eq!(wire.zero_count, stat.zero_count());
            if stat.declared_type() == DeclaredStatType::Bool {
                prop_assert_eq!(sum_field(&wire), None);
            }
        }
    }

    /// The bytes the encoder wrote for this entry before ADR-4628, derived
    /// from the proto encoding of fields 1 to 5: name "EventDate", I64 (2),
    /// min -5 (zigzag 9), max 19,000 (zigzag 38,000), null count 12.
    const PRE_4628_ENTRY_BYTES: &[u8] = &[
        0x0a, 0x09, b'E', b'v', b'e', b'n', b't', b'D', b'a', b't', b'e', // name
        0x10, 0x02, // declared_type
        0x1a, 0x02, 0x08, 0x09, // min { i64: -5 }
        0x22, 0x04, 0x08, 0xf0, 0xa8, 0x02, // max { i64: 19000 }
        0x28, 0x0c, // null_count
    ];

    /// T3 of issue #4664, older writer: an entry with no field 6 to 8 decodes
    /// to the same typed entry as before, with the new fields absent, and an
    /// entry without them still encodes to the same bytes.
    #[test]
    fn an_entry_without_the_new_fields_decodes_as_before() {
        let expected = i64_stat("EventDate", -5, 19_000, 12);
        let wire = DeclaredColumnMinMax::decode(PRE_4628_ENTRY_BYTES).expect("entry decodes");
        assert_eq!(wire.sum, None);
        assert!(wire.sum_wide.is_empty());
        assert_eq!(wire.zero_count, None);
        let stat = decode(&wire).expect("entry is valid");
        assert_eq!(stat, expected);
        assert_eq!(stat.sum(), None);
        assert_eq!(stat.zero_count(), None);
        assert_eq!(encode(&expected).encode_to_vec(), PRE_4628_ENTRY_BYTES);
    }

    /// `DeclaredColumnMinMax` as a reader that predates ADR-4628 decodes it:
    /// fields 1 to 5 only, so fields 6 to 8 are skipped as unknown.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Pre4628DeclaredColumnMinMax {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(uint32, tag = "2")]
        declared_type: u32,
        #[prost(message, optional, tag = "3")]
        min: Option<DeclaredColumnStatValue>,
        #[prost(message, optional, tag = "4")]
        max: Option<DeclaredColumnStatValue>,
        #[prost(uint64, tag = "5")]
        null_count: u64,
    }

    /// T3 of issue #4664, older reader: an entry written with the new fields,
    /// decoded by a schema without them, gives the same min, max and null
    /// count and is still covered.
    #[test]
    fn an_older_reader_sees_the_same_min_max_and_null_count() {
        let wide = i128::from(i64::MAX) + 7;
        for stat in [
            i64_sum_stat(1, i64::MAX, 0, Some(wide), Some(0)),
            i64_sum_stat(-5, 19_000, 12, Some(-60), Some(3)),
        ] {
            let bytes = encode(&stat).encode_to_vec();
            let old = Pre4628DeclaredColumnMinMax::decode(bytes.as_slice()).expect("old decode");
            assert_eq!(old.null_count, stat.null_count());
            // What the old reader holds, as the message it would decode.
            let seen = DeclaredColumnMinMax::decode(old.encode_to_vec().as_slice())
                .expect("old fields decode");
            assert_eq!(seen.sum, None);
            assert!(seen.sum_wide.is_empty());
            assert_eq!(seen.zero_count, None);
            let mut record = base_record();
            record.sample_count = 1_000;
            record.declared_column_stats = vec![seen];
            let read = read_commit_record(&record::decode(&record::encode(&record)).expect("rec"));
            assert!(read.dropped().is_empty());
            let old_view = read.column("UserID").expect("covered for the old reader");
            assert_eq!(old_view.min(), stat.min());
            assert_eq!(old_view.max(), stat.max());
            assert_eq!(old_view.null_count(), stat.null_count());
            assert_eq!(old_view.sum(), None);
        }
    }
}
