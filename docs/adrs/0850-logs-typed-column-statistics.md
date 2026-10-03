# ADR-0850: exact per-object statistics for declared logs columns

Status: Accepted. Builds on ADR-0090 (declared typed attribute columns)
and ADR-0093 (their pushdown). Sibling artifact to ADR-0049's name
postings (phase P5a), same `SnapshotHead` sibling-ref pattern. Extends
the metadata-only execution path proved by
`crates/ravel-sql/tests/logs_count_from_stats.rs`. Issue #850, epic #849.

## Context

The ClickBench corpus: 34 of 41 statements read all 3,469 objects (12.03
GB, roughly a 1.2 s floor) because the query needs an answer a full scan
happens to produce, not because the query needs every row. Three of
those decompose into pure metadata:

- q07: `SELECT MIN(EventDate), MAX(EventDate) FROM hits` — the min/max of
  per-object minima/maxima.
- q02: `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0` — non-null
  count minus the exact count of the value 0.
- q08: `SELECT AdvEngineID, COUNT(*) FROM hits GROUP BY AdvEngineID` — a
  merge of exact per-object value-to-count dictionaries.

Three more decompose once the pack also carries a per-object integer sum
(#861, a cheap addition to this ADR): q03/q04 (`SUM(col + k) = SUM(col) +
k * non_null_count`) and q30 (`AVG(col) = SUM(col) / non_null_count`). The
sum is stored for integer (I64) columns only; a float fold would be
order-dependent (ADR-0024) and the hits corpus has no float columns, so an
absent sum is the exactly-correct "not decomposable, scan instead" signal.

`LogsScanExec::partition_statistics` and the `stats_are_exact` gate in
`crates/ravel-sql/src/logs_scan.rs` already prove the mechanism for
predicate-free and contained-ts-bound `COUNT(*)`: the catalog carries
`sample_count` on every `SegmentRef` today, DataFusion's stock
`AggregateStatistics` physical-optimizer rule rewrites the aggregate to a
literal when every resolved segment's statistics are exact, and
`LogsScanExec` never gets built. This ADR widens the same mechanism to
cover q02/q07/q08's aggregates by adding the per-object statistics that
`sample_count` doesn't carry: null-aware counts, min/max, and a bounded
exact value dictionary, for the columns a tenant has declared
(`TenantConfig::typed_attr_columns`, ADR-0090).

Two other ClickBench shapes are explicitly out of scope. q20 (point
postings) and q23 (gram covering) need an index pack keyed by value, not
a per-object summary. A `SUM`/`AVG` over a non-integer column, or over an
integer column whose per-object sum overflowed `i64` at fold time, also
still has to visit every row's value: it carries no stored sum and falls
back to scanning. Neither index pack is attempted here.

### Why a sibling artifact, not a `SegmentRef`/`Snapshot` field

`SegmentRef` has over 120 construction call sites and `Snapshot` has
about 47, spanning crates outside this task's scope
(`ravel-sql`/`ravel-catalog`/`ravel-logseg`). Adding fields to either
means touching every one of those sites for a feature most of them don't
care about. ADR-0049 already solved the identical problem for name
postings by putting `SnapshotPostingsRef` on `SnapshotHead` as an
optional sibling pointing at a separately fetched, separately encoded
object, joined back to the covered parts by content hash. This ADR
copies that shape for column statistics: `SnapshotColumnStatsRef` (field
11 on `SnapshotHead`, additive) points at a `.cstat` object holding one
`ColumnStatsSegment` per covered L0 segment.

The join key differs from the postings precedent on purpose. Name
postings bind ordinals to `SnapshotHead.parts` position, which is fine
because postings are one structure spanning the whole snapshot with no
natural per-segment identity. Column statistics are inherently
per-segment, so `ColumnStatsSegment` carries the same five-field identity
tuple `fold::entry_identity` already uses to key a `SnapshotEntry`
(`ingest_hour_bucket, shard, writer_id, writer_epoch, writer_seq`) and a
reader joins by that tuple, not by ordinal position. This is more robust
than ordinal binding (survives a snapshot's segment list being
reordered, filtered, or partially stale by one entry) at the cost of one
hash-map build per query, which is negligible next to a network GET.

### `attrs_raw` overflow does not threaten exactness

`RlogWriter` gives each object a fixed dynamic-column budget
(`crates/ravel-logseg/src/writer.rs`); a (name, type) pair either gets a
FIELD_DIR column slot for the whole object or it doesn't
(`column_lookup(&column_of, name, type)` is computed once per object and
consulted per row purely to route that row's value to the columnar page
or into `attrs_raw`; it never flips mid-object). So there is no row-level
partial-overflow case where some of a column's values land in a
dedicated column and others land in `attrs_raw` for the same object: a
configured column is either fully present as a FIELD_DIR column (every
occurrence in that object decodes through it) or entirely absent from
FIELD_DIR (every occurrence, if any, is in `attrs_raw`, unreachable
short of a full record re-decode). The fold-time stats builder therefore
only needs `FieldDir::column(name, declared_type)` to resolve: `None`
means "no exact statistic available for this column in this segment,"
which is exactly the existing "live object lacking statistics" fallback
gate from the safety lemma below, not a new special case.

### L1 segments are not covered

This cut computes `ColumnStatsSegment` records for L0 entries only
(`entries.iter().filter(|e| e.level == 0)` at `fold.rs:1513`). Removing
that filter alone does not produce L1 coverage. The `SnapshotEntry` side
is sound: `build_l1_snapshot_entry` (`fold.rs:368`) and
`build_rewrite_l1_snapshot_entry` (`fold.rs:415`) each produce a
`SnapshotEntry` whose `writer_id` carries the 32-byte `input_set_hash`
and whose `writer_epoch` carries the `part_index`, so the identities the
fold has to work from are unique. Four things then stand in the way, one
in the builder and three in the reader, which is why L1 coverage is real
design work and not a one-line fold change:

- **The builder refuses an L1 entry outright.** `build_column_stats_segment`
  (`crates/ravel-catalog/src/column_stats_build.rs:372`) carries
  `debug_assert_eq!(entry.level, 0)`, and at `:379` converts
  `entry.writer_id` into `[u8; 16]`, returning
  `ColumnStatsBuildError::BadWriterIdLen` when it is any other length
  (`:383`). An L1 entry's slot holds the 32-byte `input_set_hash`, so a
  fold that dropped the level filter would fail this conversion before any
  `ColumnStatsSegment` existed. The `debug_assert` compiles out of release
  builds and is therefore not the guard; the `BadWriterIdLen` error is, and
  it fires on every build profile.
- **The reader's lookup key collapses.** `segment_identity`
  (`crates/ravel-sql/src/logs_scan.rs:571`) builds the join key from the
  `SegmentRef`, not the `SnapshotEntry`. An L1 `SegmentRef` carries
  `writer_id = Uuid::nil()`, `writer_epoch = 0`, `writer_seq = 0`
  (`crates/ravel-catalog/src/catalog.rs:2987`); its real `input_set_hash`
  and `part_index` live in the `SegmentLevel::L1` variant, which
  `segment_identity` discards. So every L1 segment in one `(shard, hour)`
  bucket computes the same key `(bucket, shard, nil, 0, 0)`, unrelated to
  the unique key the fold wrote.
- **The resolver rejects the real key.**
  `crates/ravel-catalog/src/column_stats_resolve.rs:221` hard-requires a
  16-byte writer id (`<[u8; 16]>::try_from(segment.writer_id...)`, `continue`
  on failure), while a correctly-built L1 record's `writer_id` slot holds
  the 32-byte `input_set_hash`. A correctly-keyed L1 record would be
  built, PUT, and then dropped at load before any query saw it.
- **The nil-tuple key collides across parts.** Even if the resolver
  accepted it, keying L1 stats on the nil tuple to match `segment_identity`
  would collide: a `(shard, hour)` bucket routinely compacts to more than
  one part (`SegmentLevel::L1 { part_index }`), and two entries sharing an
  identity would silently merge their statistics. That is a wrong-answer
  path, not a coverage gap.

The current code is safe because it never reaches any of these: a
snapshot containing an L1 segment simply has no matching
`ColumnStatsSegment`, so any query touching that segment's queried column
falls back to scanning it. It is a coverage gap (a snapshot with
compacted history answers fewer queries from metadata alone), not a
correctness gap.

Covering L1 has two open shapes, and choosing between them is an owner
decision left to a follow-up. The first keeps this ADR's join model and
makes the stats key level-aware, carrying `input_set_hash + part_index`
for L1 instead of the writer tuple; that touches `EntryIdentity` and the
resolver in `ravel-catalog` and `segment_identity` in `ravel-sql`
together, since all three must agree on the wider key. The second is
ADR-0913, which chooses a different shape for the same problem by binding
the statistics state to the snapshot part rather than to a writer-identity
tuple, covering L0 and L1 uniformly and sidestepping the nil-tuple
collision entirely. This ADR records both as the open options and does
not pick one.

Column statistics are also rebuilt only inside `Catalog::fold`
(`fold.rs:780`), incrementally against the prior `.cstat` baseline. A
tenant that has already compacted and gone quiescent folds nothing, so it
rebuilds nothing, and no in-tree command forces a stats-only rebuild on an
idle tenant. So even once L1 coverage lands, an already-compacted tenant
gains it only on its next fold; there is no backfill path today.

## Decision

### 1. New protobuf types (`proto/ravel/catalog.proto`)

Additive fields/messages only, no renumbering:

- `SnapshotHead.column_stats` (field 11, `SnapshotColumnStatsRef`):
  mirrors `SnapshotPostingsRef`'s shape (key, blake3, size, a count,
  covered `part_blake3` list). Absent whenever the tenant has no
  declared typed columns, no L0 segment has been folded, or the last
  fold's stats build failed for any reason — readers must treat absence
  as "fall back to scanning," never as an error.
- `ColumnStatsHeader`: the envelope header, mirrors
  `SnapshotPostingsHeader`.
- `ColumnValue`: a `oneof` over `i64`/`bool`/`str_utf8`/`bytes_val`,
  covering ADR-0090's four declared types (`F64` is not a declared
  typed-attribute-column type and is out of scope here as it is there).
- `DictEntry { value, count }` and `ColumnStat { name, declared_type,
  non_null_count, null_count, min, max, dictionary_present, dictionary,
  sum }`: one column's exact statistics for one segment. `min`/`max` are
  proto3-absent (not a sentinel value) when `non_null_count == 0`.
  `dictionary_present = false` means the column's distinct-value count
  in this segment exceeded the cardinality ceiling: the dictionary is
  omitted outright, never truncated, so a reader can never mistake "over
  the ceiling" for "fewer than N distinct values." `sum` (proto3
  `optional`, #861) is the exact sum of the column's non-null values,
  present for an I64 column only and omitted when the exact sum overflowed
  `i64` at fold time; absence means "no exact sum, scan instead," and it is
  independent of `dictionary_present` so a high-cardinality integer column
  still decomposes `SUM`/`AVG`. The codec rejects a `sum` on a non-integer
  column and a `sum` that disagrees with the column's own dictionary.
- `ColumnStatsSegment { ingest_hour_bucket, shard, writer_id,
  writer_epoch, writer_seq, columns }`: the per-segment join key plus its
  columns' stats.

`declared_type` is a plain `uint32` carrying
`ravel.sys.v1.TypedAttrColumnType` as `i32`, the same convention
`SnapshotPartHeader.signal` already uses for a foreign enum domain: this
repository has zero cross-file proto imports (verified against every
file under `proto/ravel/`), and this ADR does not introduce the first
one.

### 2. Envelope and body format: `.cstat`, magic `RCST`, version 1

New `crates/ravel-catalog/src/snapshot_format/column_stats.rs`, modeled
directly on `part.rs` rather than `postings.rs`: `postings.rs`'s
hand-rolled varint name dictionary exists to make prefix lookup cheap
over a huge flat name space, which per-segment column statistics don't
need. The body reuses `part.rs`'s simpler convention instead: zstd
compressed, length-delimited `ColumnStatsSegment` protobuf messages
(`encode_length_delimited_to_vec`/`decode_length_delimited`), sorted by
the identity tuple for deterministic byte output. The envelope is magic
(`RCST`) + version (`1u8`) + reserved bytes + `ColumnStatsHeader` +
zstd body + trailing crc32c, matching `part.rs`'s framing. A
`format_constants_are_pinned`-style test locks the new magic/version the
same way the existing envelopes are locked.

Decode enforces a size ceiling (`ColumnStatsLimits`, mirroring
`PartLimits`) before inflating the zstd body, so a corrupt or hostile
`size`/header field cannot force an unbounded allocation.

### 3. Cardinality ceiling: 10,000 distinct values per (segment, column)

Chosen per segment, not per tenant-wide column, because the dictionary
is built and stored per segment: a segment holds one ingest hour's worth
of one shard's records, so 10,000 distinct values comfortably covers
legitimate categorical/enum-shaped columns (status codes, country codes,
`AdvEngineID`-style small-int dimensions) while bounding one segment's
dictionary to at most 10,000 `DictEntry` messages — small next to the
segment sizes in the corpus this feature targets. A column whose
distinct count exceeds the ceiling in a given segment (a synthetic ID
column, for instance) gets `dictionary_present = false` for that
segment; queries needing that segment's exact per-value counts fall back
to scanning it, while `non_null_count`/`null_count`/`min`/`max` (which
cost nothing extra to keep exact regardless of cardinality) remain
usable. The ceiling is a fold-time constant
(`DEFAULT_MAX_COLUMN_DICTIONARY_ENTRIES` in `column_stats.rs`), not
tenant-configurable in this cut. 10,000 also matches the existing
precedent `RlogConfig::postings_max_distinct` already sets for a
different per-object dictionary (the write-time POSTINGS term cap,
ADR-0049 decision 4); reusing the same order of magnitude rather than
inventing a new one is a deliberate choice, not a coincidence.

### 4. Fold-time build (`crates/ravel-catalog/src/fold.rs`)

A new `build_column_stats` function, structured like `build_postings`
(baseline carried forward for already-computed entries, freshly
decoded from `decode_start` onward, restricted to L0 entries) but with
one deliberate divergence: `build_postings` is documented all-or-nothing
because postings share one cross-entry ordinal space, so one entry's
decode failure poisons the whole rebuild. Column statistics are
independent per-segment records with no shared ordinal space, so a
single segment's decode failure (object missing, corrupt, or with no
FIELD_DIR entry for a configured column) drops only that segment's
`ColumnStatsSegment` — or, when it lacks a column entirely, that column
within it — rather than failing the whole build. This is strictly safer
than all-or-nothing here: it maximizes how much of a fold's work still
answers metadata-only queries instead of forcing every segment back to
full scans because one segment was awkward.

For each L0 entry, for each tenant-declared column: resolve
`FieldDir::column(name, declared_type)`; if absent, emit no `ColumnStat`
for that column in that segment (safe fallback, not a build failure).
If present, run one `RlogReader::scan_blocks` pass with an always-true
content predicate (`Predicate::And(vec![])`) and a `ColumnSelection`
covering every resolved declared column at once — one decode pass
computes every declared column's statistics together, rather than
mixing FIELD_DIR's null counts, SKIP_IDX's block min/max, and a separate
dictionary pass, which would risk the three quietly disagreeing. Tally
row count, non-null/null counts, min/max (by bit pattern for floats —
not applicable to the four declared types, but the same discipline
applies to any future extension), and a distinct-value histogram capped
at the cardinality ceiling, using the type-appropriate `iter_i64`/
`iter_bool`/`iter_bytes` accessor.

The tenant-config read at `fold.rs`'s existing config-values match
gains `typed_attr_columns` alongside what it already reads. The
attach sequence mirrors the postings block exactly: load the previous
column-stats baseline (any load failure is a graceful `None`, never a
fold failure), call `build_column_stats`, and on `Some` encode + hash +
`PutOptions::create_if_absent()`; on any encode/PUT failure,
`tracing::warn!` and fold without a stats ref, same as postings do
without a postings ref.

### 5. Reader side (`ravel-catalog` → `ravel-sql`)

A new `Catalog` method fetches, validates (hash match, part-binding
match against the resolved `Snapshot`'s parts, format version), and
decodes the referenced `.cstat` object, returning a structure joined
against `Snapshot.segments` by identity tuple. `ravel-sql` fetches this
alongside the existing async `Snapshot` resolution, before physical
planning, and threads the (optional) result synchronously through
`LogsTableProvider`/`LogsScanExec` construction the same way `Snapshot`
itself already flows through the test harness's `provider_over`. This
is required because DataFusion's optimizer rules and
`ExecutionPlan::partition_statistics` are synchronous, while the new
catalog object needs an async GET.

Two DataFusion mechanisms carry the three statements:

- q07 needs no new optimizer rule. It reuses the same stock
  `AggregateStatistics` rewrite the existing predicate-free `COUNT(*)`
  shortcut already relies on: `LogsScanExec::partition_statistics`
  reports exact `ColumnStatistics::min_value`/`max_value` for a declared
  column once every resolved segment has an exact `ColumnStat` for it.
- q02 and q08 need one new physical optimizer rule
  (`MetadataOnlyAggregate`, registered under the name
  `metadata_only_aggregate`), because DataFusion's stock `Statistics`
  type has no value-to-count structure and no rewrite from statistics
  for a filtered or grouped `COUNT(*)`. The rule matches `AggregateExec
  [COUNT(*), optional single-column GROUP BY]` over `LogsScanExec`, and
  replaces the whole subtree with a literal-values plan only when every
  resolved segment's dictionary for the relevant column both exists
  (`dictionary_present`) and is otherwise valid per the safety lemma
  below; any segment failing either check leaves the original plan
  untouched. Two branch contracts constrain when it fires:
  - **q02** (bare `COUNT(*)`) requires a residual filter, and that
    filter must be `col <> lit`: `not_equal_literal` accepts only
    `Operator::NotEq`, so `col = lit` (equality) declines. A
    predicate-free `COUNT(*)` also declines here, because DataFusion's
    own `AggregateStatistics` rule already answers it from
    `partition_statistics` (the q07 path above).
  - **q08** (single-column `GROUP BY`) declines outright when any filter
    is present: a filter combined with a `GROUP BY` is out of scope for
    this rule.

### 6. Observability: `metadata_only` marker

For q02/q08, the rule's literal-replacement output is wrapped in a new
`MetadataOnlyExec` leaf so a test can assert `plan_str.contains(
"MetadataOnlyExec")`. For q07, no new wrapper is introduced: the marker
is the pre-existing precedent already used by the predicate-free
`COUNT(*)` tests in `logs_count_from_stats.rs` — the absence of
`LogsScanExec` from the displayed plan. This ADR does not force q07
through `MetadataOnlyExec` for uniformity, since doing so would mean
diverging from a stock DataFusion rewrite path for no functional gain;
the two markers are deliberately non-uniform and this is called out
explicitly rather than papered over.

## Safety lemma

Absent, stale, corrupt, or version-mismatched statistics degrade to
scanning; they never produce a wrong answer. Concretely, every one of
these forces the affected segment (or the whole query, where the
condition is snapshot-wide) back to the scan path:

- `SnapshotHead.column_stats` is absent, or its referenced object is
  missing, fails its blake3 check, fails to decode, or reports a format
  version this reader doesn't understand.
- `SnapshotColumnStatsRef.part_blake3` doesn't match the resolved
  snapshot's covered parts (a stale stats object bound to a superseded
  part set).
- A live segment in the resolved snapshot has no matching
  `ColumnStatsSegment` (L1 segment, or an L0 segment folded before
  stats existed, or a segment the builder skipped after a decode
  failure), or has a matching segment with no `ColumnStat` for the
  queried column (never declared for that tenant at the time, or the
  column overflowed to `attrs_raw` for the whole object, per the
  `attrs_raw` analysis above).
- The queried column's dictionary is needed (q02/q08) but
  `dictionary_present` is false for any relevant segment (cardinality
  ceiling exceeded) — q02 in particular must never approximate: an
  inexact count of the literal being excluded is worse than a scan.
- A pending selective-erasure predicate (ADR-0064) is present on the
  resolved snapshot: erasure invalidates precomputed counts, so the
  metadata-only path is rejected for the ENTIRE query (not merely the
  affected segment) while any erasure is pending. `stats_are_exact`
  refuses whenever `self.erasure` is non-empty, so q02/q08 fall back to
  a full scan.
- The snapshot's pinned commit token (MVCC) admits a segment the
  resolved `Snapshot.segments` list didn't have when column stats were
  last folded (an uncovered segment): coverage is judged against the
  snapshot actually being queried, not the snapshot at fold time.

Each of these is a fallback to the existing scan path already exercised
by `logs_count_from_stats.rs`, not a new code path: this ADR adds ways
to short-circuit into metadata-only execution, never a new way to
answer incorrectly.

## Rejected alternatives

- **Fields directly on `SegmentRef`/`Snapshot`.** Rejected: 120+ and 47+
  construction call sites respectively, most outside this task's scope
  and none of them caring about per-column statistics.
- **Reusing the name-postings hand-rolled varint body encoding.**
  Rejected: that encoding earns its complexity for a huge flat
  cross-segment name space; per-segment column statistics are naturally
  segment-scoped protobuf records, and `part.rs`'s plain
  length-delimited convention is simpler and just as adequate.
- **Ordinal-position join to `SnapshotHead.parts`, matching the postings
  precedent.** Rejected in favor of the identity-tuple join: column
  statistics are per-segment, not one cross-snapshot structure, and the
  identity tuple survives snapshot segment-list reordering or partial
  staleness that an ordinal position would not.
- **All-or-nothing build, matching `build_postings` exactly.** Rejected:
  column statistics have no shared ordinal space across segments, so
  failing the whole build over one awkward segment throws away
  otherwise-usable statistics for every other segment in the fold.
- **Approximate/truncated dictionaries above the cardinality ceiling.**
  Rejected outright by the task's exactness invariant: q02 needs the
  exact count of one literal value, and a truncated dictionary could
  silently omit it.
- **Covering L1 segments in this cut.** Rejected for now: the blocker is
  not the fold-side `level == 0` filter. Dropping it alone fails in the
  builder, which converts `entry.writer_id` to `[u8; 16]` and returns
  `ColumnStatsBuildError::BadWriterIdLen` on an L1 entry's 32-byte
  `input_set_hash` (`column_stats_build.rs:379`), and it would still not
  reach the reader join. An L1 `SegmentRef` carries a nil writer tuple with
  its real `input_set_hash`/`part_index` in the `SegmentLevel::L1` variant that
  `segment_identity` drops, and the resolver rejects the 32-byte
  `input_set_hash` where it expects a 16-byte writer id; matching the nil
  tuple instead collides across the parts of one bucket, a wrong answer.
  A level-aware stats key spanning `EntryIdentity`, the resolver, and
  `segment_identity`, or ADR-0913's snapshot-part binding, is real design
  work. Deferred to a follow-up; the current cut degrades safely (fallback
  to scan) rather than incorrectly. See the "L1 segments are not covered"
  section above.

## Consequences

- q02, q07, and q08 answer from catalog metadata alone against a fully
  folded snapshot with declared columns and no pending erasure, cutting
  their object-store GETs to zero, matching the existing predicate-free
  `COUNT(*)` shortcut's shape.
- Every fold that has declared typed columns configured does one extra
  decode pass per L0 entry (bounded to the declared columns only) and
  one extra small object PUT; a tenant with no declared columns pays
  nothing new (the build produces no ref instead of an empty one).
- The catalog format gains one more additive `SnapshotHead` field and
  one more object kind per signal per fold; existing readers ignore it
  as required by proto3 field-addition semantics.
- L1-covered snapshots (any compacted history) fall back to scanning
  the L1 segments' contribution to a metadata-only query; this is a
  known, intentional coverage gap, not a silent wrong answer.
- `ravel-catalog` gains a new dependency on `ravel-logseg` (for
  `RlogReader`/`ColumnSelection`/`FieldDir` at fold time) — a new edge
  in the crate graph, verified acyclic (`ravel-logseg` has no dependency
  back on `ravel-catalog`).
