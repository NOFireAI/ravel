# ADR-0873: per-declared-column min/max on commit records and SegmentRef

Status: Proposed. Issue #873. Builds on ADR-0850 (logs typed column
statistics, the `.cstat` plane), ADR-0942 (re-key `.cstat` to snapshot-part
binding, and its backfill obligation), ADR-0066 (format migration machinery:
the N/N-1 window, recorded format floors, migration classes A-D), ADR-0101
(declarable f64 typed attribute columns), and ADR-0849 (the snapshot-bound
index plane, whose §1a names the commit record as a candidate statistics
carrier). Touches two frozen contracts (the commit-family protobufs under
`proto/ravel/commit.proto` and the catalog snapshot protobufs under
`proto/ravel/catalog.proto`), so the format-change skill's procedure governs
and this ADR is its first step.

## Context

`SegmentRef` carries an exact min/max for exactly one column: `ts`
(`min_event_ts_ns`/`max_event_ts_ns`, stamped on every `CommitRecord` and
`CompactionPart` since ADR-0002/0018). `LogsScanExec::partition_statistics`
already exploits it: under the `stats_are_exact` gate it reports an exact
`num_rows` and an exact `ts` span, and DataFusion's stock
`AggregateStatistics` physical-optimizer rule rewrites a predicate-free
`COUNT(*)`, `MIN(ts)`, or `MAX(ts)` to a literal with zero data GETs
(`crates/ravel-sql/src/logs_scan.rs:1861-1925`).

No other column has that property at the resolution layer. Issue #873's
motivating measurement: `SELECT MIN(EventDate), MAX(EventDate) FROM hits`
(ClickBench q07-shape, `EventDate` a declared I64 column) costs 1,181 ms
where stock DataFusion over parquet answers in 22.6 ms, because the min/max
must come from somewhere and the only somewhere is a scan.

### What ADR-0850/0942 already cover, and the structural gap they cannot

ADR-0850 built exact per-segment column statistics (min/max included) as a
fold-built sibling object (`.cstat`), and its reader is implemented:
`declared_min_max_all` joins the loaded `.cstat` against the touched
segments and `partition_statistics` reports `Precision::Exact` min/max for
declared columns (`logs_scan.rs:1904-1923`), so the stock
`AggregateStatistics` rule fires. ADR-0942 re-keys that object to the
snapshot part so compacted (L1) history is coverable, and names the forced
rebuild fold that backfills a quiescent tenant.

What no fold-built object can ever cover is the data the fold has not seen
or does not carry:

- **The live tail.** A snapshot's sealed hours come from `.csnap` parts, but
  everything above the fold watermark is resolved by LISTing and decoding
  commit records directly (docs/catalog-and-mvcc.md "Snapshot resolution",
  step 1-4), and the newest `max_flush_lifetime + clock_skew_allowance +
  fold_safety_margin` of every tenant is always unsealed by construction. A
  `.cstat` is built at fold time over folded entries; the recent tail is
  therefore *permanently* uncovered on a live tenant, not merely
  lagging-until-next-fold. ADR-0849's own Consequences state the resulting
  shape: "metadata-only execution is a benchmark capability" because "a
  live-ingest tenant always has an uncovered L0 tail by construction".
- **Token-resolved segments.** Read-your-write `min_token` segments are
  fetched by direct commit-record GET outside the snapshot (resolution step
  5); no snapshot-sibling object describes them.
- **Coverage bookkeeping is a second structure.** The `.cstat` plane needs
  its own object, its own ref on `SnapshotHead`, a hash-validated join, an
  extra async GET before planning, and (per ADR-0942) its own backfill pass
  to mean anything on an already-loaded tenant.

The hoist this ADR designs closes the structural gap rather than widening
the sibling plane: stamp the per-declared-column min/max **on the records
resolution already reads** — the `CommitRecord` at flush, the
`CompactionPart` at compaction — and carry it through the fold's
`SnapshotEntry` onto `SegmentRef`. Coverage then travels with snapshot
resolution itself: the live tail is covered the moment its commit record is
durable, token-resolved segments are covered by the same GET that satisfies
the token, and there is no join, no extra GET, and no separate coverage
declaration. This is exactly carrier candidate 2 from ADR-0849 §1a ("an
additive field on the commit record compaction already writes, which the
fold already reads — no new object class, but a frozen-contract change
under the format-change procedure"). This ADR is that procedure's ADR, for
min/max specifically.

### Where the values already exist at write time

Both writers of data objects already decode every row and build per-block
statistics:

- `RlogWriter` computes per-block NumStats (min/max) per `(name, type)`
  dynamic column for the SKIP_IDX section (`crates/ravel-logseg/src/writer.rs`,
  ADR-0093/0095), on the L0 flush path and — via the shared `build_object`
  pipeline behind `finish_compacted` — on the L1 compaction path.
- The selective-erasure rewrite (ADR-0064) re-encodes surviving rows through
  the same writer.

A whole-object min/max per declared column is a fold over per-block extrema
the writer already holds. The capture is a running comparison, not a new
decode pass — unlike ADR-0850's fold-time build, which pays one full decode
pass per segment precisely because the values were not kept at write time.

### Scope honesty, carried from issue #873

This fixes the q07 shape (`MIN`/`MAX`, and `COUNT` via the null count below)
and nothing else. Issue #873 states, correctly, that it does **not** fix q02
or q08: parquet-style per-object statistics cannot count rows matching a
predicate (`COUNT(*) WHERE col <> 0`) nor produce per-value `GROUP BY`
counts. Those need the exact value dictionaries that live in `.cstat`
(ADR-0850's `MetadataOnlyAggregate` rule), which this ADR leaves untouched.
The `.cstat` plane is not superseded: dictionaries and integer sums (#861)
stay there; only min/max (and null counts) gain the second, resolution-borne
carrier.

One correction to the issue's framing, reported rather than silently
absorbed: the issue reasons that the float total-order hazard "does not
arise" because declared columns have no F64 variant. That is true of the
tree today (`proto/ravel/sys.proto` `TypedAttrColumnType` ends at
`BYTES = 4`) but ADR-0101 is Accepted and adds `TYPED_ATTR_COLUMN_TYPE_F64 =
5`, so the hazard is scheduled, not hypothetical. Decision 2 therefore makes
the eligibility gate an explicit allowlist rather than an incidental
consequence of the current vocabulary.

## Decision

### 1. Wire changes: three additive fields, two mirrored messages, no bump

All changes are additive. No existing field is renumbered, reused, or given
new meaning. Field numbers, chosen as the lowest unclaimed number in each
message:

**`proto/ravel/commit.proto`** (package `ravel.commit.v1`) gains two
messages and two fields:

```proto
// The min or max of one declared column's non-null values in one object.
// Restricted to the stamp-eligible declared types (decision 2); a new
// eligible type adds a oneof arm, additively.
message DeclaredColumnStatValue {
  oneof kind {
    sint64 i64 = 1;
    bool b = 2;
  }
}

// Exact whole-object min/max (and null count) for one declared typed
// attribute column (ADR-0090), computed by the writer that encoded the
// object. min/max are message-typed for presence: both absent means the
// column had zero non-null values in this object, which is still an exact
// statement. An entry is only ever written for a column that resolved to a
// real FIELD_DIR column covering the whole object (the ADR-0850 attrs_raw
// rule); a column that overflowed gets no entry, never an entry with a
// guess.
message DeclaredColumnMinMax {
  string name = 1;
  uint32 declared_type = 2;  // ravel.sys.v1.TypedAttrColumnType as i32
  DeclaredColumnStatValue min = 3;
  DeclaredColumnStatValue max = 4;
  // Rows in this object where the declared column reads NULL (name absent,
  // or the stored variant mismatches declared_type). non_null = the
  // record's row count minus this.
  uint64 null_count = 5;
}
```

- `CommitRecord.declared_column_stats = 20;` (repeated `DeclaredColumnMinMax`).
  Fields 1-19 are in use; 20 is the lowest free number.
- `CompactionPart.declared_column_stats = 12;` (repeated
  `DeclaredColumnMinMax`). Fields 1-11 are in use. `RewriteRecord.parts` and
  `CompactionRecord.parts` both carry `CompactionPart`, so erasure-rewrite
  outputs get the field with no further wire change.

**`proto/ravel/catalog.proto`** (package `ravel.catalog.v1`):

- `SnapshotEntry.declared_column_stats = 15;` (repeated). Fields 1-14 are in
  use. The message is a same-file mirror of the commit-side pair
  (`DeclaredColumnStatValue`/`DeclaredColumnMinMax` redeclared in
  `ravel.catalog.v1`), because this repository has zero cross-file proto
  imports and ADR-0850 already established mirroring as the convention
  (`ColumnStat.declared_type` carries the sys enum as `uint32` for the same
  reason). The existing `ColumnValue` message is deliberately not reused: its
  oneof admits `str_utf8`/`bytes_val`, which are not stamp-eligible, and a
  narrower type makes an ineligible stamp unrepresentable rather than
  merely rejected.

**Version bump: none, and that is the deliberate answer, stated rather than
implied.** These are Class C records (decision 5): the evolution mechanism
for the commit-family and snapshot protobufs is the frozen field-number
space itself — "protos add fields, never renumber or reuse" (format-change
skill rule 2). `CommitRecord.format_version` stays 1,
`CompactionRecord.format_version` stays 1, and the `.csnap` envelope version
is untouched, on the ADR-0052 `generations` precedent that ADR-0066 made
normative: an addition that an old reader can safely ignore (absence of the
shortcut, never a wrong answer — decision 4) does not raise the reader
floor. ADR-0063's `SnapshotPartHeader.min_hour` (field 8, no envelope bump)
is the standing in-file precedent for an additive `SnapshotEntry`-side
change. A reader floor bump would force a flag-day for a field whose absence
is a permanently legal state.

**Rust-side carriage.** `SegmentRef`
(`crates/ravel-catalog/src/snapshot.rs:32`) gains one field,
`declared_column_stats` (a shared, cheaply clonable list; `SegmentRef` is
cloned per query). This is the field ADR-0850 declined to add, citing 120+
construction sites. That rejection was right for ADR-0850's payload and does
not bind here, for two reasons. First, scale: ADR-0850's payload is
dictionaries, counts, and sums — unbounded per-segment state that has no
business on a per-ref struct; this ADR's payload is a bounded min/max/null
triple per eligible declared column. Second, and decisive: the entire point
of #873 is coverage that rides resolution (live tail, token segments), and
only a `SegmentRef`-carried value can do that — a sibling object is
structurally fold-bound, as the Context section shows. The construction-site
cost is a mechanical one-line default at each literal and is paid once.

### 2. Eligibility: an explicit allowlist, {I64, BOOL}, gated fail-closed

Stamp eligibility is a named allowlist in one place
(`ravel-types` or `ravel-logseg`, single-sourced the way version constants
are per ADR-0066 decision 1):

- **`I64` and `BOOL` are eligible.** Both have a total order, fixed-width
  values, and existing write-time NumStats to fold over.
- **`STR` and `BYTES` are not.** Their extrema are unbounded byte strings; a
  stamped value would put arbitrary user data of arbitrary length on the
  commit record, the hot resolve path, and the per-record cache (sized at
  ~750 bytes/entry, docs/catalog-and-mvcc.md step 2). A truncated extremum
  is a bound, not an exact extremum, so capping the length would make the
  stat unusable for an `Exact`-precision answer. The `.cstat` plane remains
  the carrier for anything exact about string columns.
- **`F64` is not eligible, and its exclusion is a gate, not an accident.**
  ADR-0101 (Accepted) adds a declarable `f64` type; when its writer release
  lands, declared float columns will exist. Nothing about this ADR changes
  for them: `F64` is absent from the allowlist, so no writer stamps it, no
  reader accepts it, and `MIN`/`MAX` over a declared float column keeps
  scanning. Admitting `F64` later is an ADR amendment that must decide, at
  minimum: the comparator (bit-pattern total order per the repo invariant
  and ADR-0023, vs IEEE semantics), the NaN rule (the ADR-0101 §3 precedent:
  a page containing NaN has no usable ordered bound — at whole-object grain
  the sound default is that an object containing any NaN in the column gets
  no stamp), the `-0.0`/`+0.0` rule, and proof that the stamp's comparator
  agrees exactly with the scan-path aggregate's, because a statistics
  shortcut that orders floats differently from the executor it replaces is a
  wrong answer, not a slow one.

Enforcement is two-sided. Writers stamp only allowlisted types. Every
decoder validates each entry against the predicate below — the structural
clauses plus the entry's own carrier's vocabulary clause — before the
entry can grant coverage. An entry failing **any** applicable clause is dropped
whole — no field of it is used, including the fields that pass on their
own: a plausible min next to an impossible null count is evidence the
writer that produced the entry was broken, not a value to salvage. The
dropped column is simply uncovered for that segment (the safety rule of
decision 4), the drop is counted on a defect metric labelled by carrier,
and the carrying object itself still decodes; an invalid entry is never a
decode failure and never trusted partially.

The predicate below is named the **statistics validity predicate**. Its
clauses split into two groups with different binding scopes, and the split
is itself normative (the grouping criterion follows the clause list, so a
future clause is placed by argument, not by analogy):

- **The structural clauses (2-6) are carrier-independent.** They bind
  every carrier that can grant coverage under decision 4 — the stamp, at
  each of its decoders (commit record, compaction record, snapshot
  entry), **and** the ADR-0850/0942 `.cstat` `ColumnStat` entries, at the
  join against the touched segments — and any carrier a future ADR adds.
  The union of decision 4 is only as sound as its weakest member: for any
  segment one carrier alone covers, that carrier's validation *is* the
  guarantee, so a structural clause applied to one carrier and not
  another is not a partial safety measure, it is no guarantee at all for
  exactly the segments the unchecked carrier serves. This paragraph
  exists so a future carrier cannot be added under weaker validation by
  omission: granting coverage from any entry that has not passed the
  structural clauses is a violation of this ADR, not an implementation
  detail.
- **The vocabulary clause (1) is per-carrier.** It states what a given
  carrier may transport, and each carrier's defining ADR sets its
  vocabulary: {I64, BOOL} for the stamp (this decision), ADR-0090's four
  declared types for `.cstat` (ADR-0850). It is not a property of the
  statistic — a `.cstat` `STR` extremum is a perfectly coherent exact
  statistic that ADR-0850 ships and `declared_min_max_all` answers from
  today — so applying it carrier-independently is itself a defect.

Revision history, kept because both mis-scoping directions have now
occurred. The first revision of this amendment under-scoped the
structural clauses: it stated the predicate for the new stamp only,
leaving `.cstat` under its pre-existing, weaker reader checks — which
validate min/max presence shape and value kind but never reconcile the
entry's row accounting with the joined `SegmentRef.sample_count`, so a
`.cstat` entry claiming `non_null_count = 1, null_count = 0` for a
two-row segment passed validation and reached the exact `MIN`/`MAX` and
`COUNT` paths. The second revision over-corrected by over-scoping the
vocabulary clause: it bound the whole predicate, type allowlist included,
to every carrier, which would have invalidated every `STR`/`BYTES`
extremum `.cstat` legitimately carries under ADR-0850 and silently
degraded declared string columns from `Precision::Exact` answers to
scans. The explicit grouping exists so neither mistake has a third form.

The predicate is stated over the carrier-neutral quantities `(name,
declared_type, min, max, null_count, sample_count)`, where `sample_count`
is always the segment's row count as the reader resolves it: for a stamp,
the carrying record's row count (`CommitRecord`/`SnapshotEntry` field 11,
`CompactionPart` field 6); for a `.cstat` entry, the **joined**
`SegmentRef.sample_count` of the segment the entry is keyed to — never a
figure the entry carries about itself, because self-consistency of a
broken entry proves nothing. A `ColumnStat` additionally carries the
redundant pair (`non_null_count`, field 3, beside `null_count`, field 4),
so it gets one carrier-specific clause instantiating the same rule over
the redundancy: `non_null_count + null_count == sample_count`, exactly. An
entry whose redundant fields disagree with the joined row count is dropped
like any other invalid entry.

An entry, from either carrier, is valid iff all of:

1. *(vocabulary, per-carrier)* `declared_type` is in the entry's own
   carrier's vocabulary: for a stamp, the allowlist of this decision
   ({I64, BOOL} today); for a `.cstat` entry, the four ADR-0090 declared
   types ({I64, BOOL, STR, BYTES}, ADR-0850's `ColumnValue` oneof). A
   stamp carrying `STR`, and an entry on any carrier carrying a type no
   carrier admits (`F64`, until an amendment admits it somewhere), are
   equally invalid; a `.cstat` entry carrying `STR` or `BYTES` is valid
   under this clause — dropping it was the second revision's regression.
2. *(structural)* `min` and `max` are **both present or both absent**,
   where present means the message is set and its value kind matches
   `declared_type` under the carrier's value message
   (`DeclaredColumnStatValue` for the stamp, ADR-0850's `ColumnValue`
   for `.cstat`). A
   one-sided entry is not a usable range: it admits no exact answer for
   either aggregate, so it is invalid outright, never "half usable".
3. *(structural)* When both are present, `min <= max` under the
   authoritative comparator defined below for the entry's type.
4. *(structural)* `null_count <= sample_count`, with `sample_count` resolved as defined
   above (the carrying record's row count for a stamp, the joined
   `SegmentRef.sample_count` for a `.cstat` entry). For a `.cstat` entry,
   additionally `non_null_count + null_count == sample_count`, exactly.
5. *(structural)* Presence agrees with the null count, **in both
   directions**: both-absent (zero non-null values)
   requires `null_count == sample_count`; both-present requires
   `null_count < sample_count`.
6. *(structural)* `name` is non-empty, and at most one entry exists per
   `(name, declared_type)` in the carrier's entry set for the segment (the
   record's repeated field for a stamp, the segment's `ColumnStat` list
   for `.cstat`); duplicates are all dropped, since no rule could pick the
   right one. A reader that keeps the last duplicate it sees — the shape
   of the current `.cstat` join, which collects entries into a by-name map
   — is silently picking one, which is exactly what this clause forbids.

**The grouping criterion.** A clause is **structural** iff it is decidable
over the carrier-neutral tuple `(name, declared_type, min, max,
null_count, sample_count)` alone and its violation convicts the producing
writer of being broken regardless of which carrier transported the entry
and regardless of who consumes it — it tests the internal coherence of
one entry against the one immutable segment it describes. Name
uniqueness, `min <= max`, presence agreeing with the null count in both
directions, and the counts reconciling against the segment's
`sample_count` are all of this kind: no correct writer produces an entry
violating any of them, for any carrier, ever. A clause is **vocabulary**
iff it restricts what a particular carrier may transport, on grounds that
belong to the carrier rather than to the entry's coherence: the stamp
excludes `STR`/`BYTES` because of record size on the hot resolve path and
excludes `F64` pending a comparator decision (both above) — neither
reason says anything against the same statistic riding `.cstat`, where
ADR-0850 admits and serves it. A future clause is placed by asking which
of those two arguments carries it; a clause that needs carrier-specific
inputs to state (like clause 4's `non_null_count` redundancy arm) is
still structural when it instantiates a carrier-neutral rule over fields
only that carrier happens to carry.

The authoritative comparator is the scan-path aggregate's, by definition:
for I64, signed integer order (`i64::cmp`, which is what DataFusion's Int64
`MIN`/`MAX` accumulators apply); for BOOL, `false < true`; for STR and
BYTES — reachable only through the `.cstat` vocabulary — bytewise
lexicographic order over the raw bytes, which is what DataFusion's
Utf8/Binary `MIN`/`MAX` accumulators apply (UTF-8 makes code-point order
and byte order coincide, so the two readings of "string order" agree).
The single-sourced allowlist constant (opening of this decision) names
the comparator beside each type it admits, the `.cstat` vocabulary's two
extra types get theirs in the same constant module, and the writer's
fold, the decoder's clause 3, and the
read-side union all use that one definition. This is load-bearing, not
pedantry: the stamp *replaces* the aggregate's answer, so a stamp computed
under any other ordering is the same class of silent wrongness as a wrong
value — the F64 gate above exists precisely because floats are the case
where two plausible comparators genuinely disagree.

Tests that pin the predicate, one case per structural clause **per
carrier** — every
case below runs once against a stamp and once against a `.cstat` entry,
because a clause tested on one carrier proves nothing about the other's
code path: a one-sided entry, `min > max`, `null_count > sample_count`,
both-absent with `null_count != sample_count`, **both-present with
`null_count == sample_count`** (extrema claimed for an all-NULL column),
a value kind mismatching
`declared_type`, and a duplicate name. The both-present-all-NULL case is
listed by name because it is not hypothetical: it is defect 2 of issue
#970, live on `main` today — `declared_min_max_all`
(`crates/ravel-sql/src/logs_scan.rs:1266`) checks `non_null_count > 0 &&
(stat.min.is_none() || stat.max.is_none())` and never the mirror, so an
entry carrying extrema for an all-NULL column passes and its fabricated
extrema reach the exact `MIN`/`MAX` path. A validator that checks only
the both-absent branch of clause 5 passes every other case in this matrix
and still accepts that entry; only this case fails it, so a matrix
without it does not pin the clause.

The vocabulary clause gets per-carrier cases in both directions: a stamp
carrying `STR` is dropped; an `F64` entry is dropped on both carriers;
and the positive regression case this amendment's third revision exists
for — a `.cstat` entry for a declared `STR` column with extrema valid
under bytewise order and counts reconciling against the joined
`sample_count` **passes** the predicate, appears in the valid set, grants
coverage, and `partition_statistics` still reports `Precision::Exact`
min/max for that column with zero increments on the defect metric (same
case once more with `BYTES`). The assertion that fails if the vocabulary
clause is ever applied carrier-independently again is that entry's
presence and the `Exact` precision.

The `.cstat`
lane adds the redundancy case of clause 4: an entry with `non_null_count +
null_count != sample_count` of the joined segment — concretely, the
defect that motivated this rule, `non_null_count = 1, null_count = 0`
joined to a two-row segment — must be dropped and must not reach the
exact `MIN`/`MAX` or `COUNT` paths. Each negative case asserts the entry
is absent
from the valid set, the carrier-labelled defect metric incremented by
exactly one, the carrying object otherwise decoded intact, and
`partition_statistics` reporting `Precision::Absent` for that column. The
assertion that fails when the invariant breaks must be the entry's absence
(and the resulting `Absent` precision), not a log line.

### 3. Capture at write time

- **L0 flush (ravel-ingest, `log_shard.rs`).** The flush path learns the
  tenant's declared typed columns through the same durable-config
  bounded-staleness read that already supplies indexed fields to the writer
  (ADR-0079's override-cache pattern). For each declared, eligible column
  that resolved to a whole-object FIELD_DIR column, `RlogWriter` folds its
  per-block NumStats into a whole-object min/max and null count, returns
  them alongside `WriteStats` (the existing writer-to-committer channel,
  `finish_with_stats`), and `record::build` stamps them onto the
  `CommitRecord`. Staleness is fail-closed by construction: a column
  declared after flush open is missing from that object's stamps, so that
  segment is uncovered and scans; it converges at the next flush.
- **L1 compaction (ravel-maintain, `rlog.rs`).** The compactor already
  re-encodes every row through `finish_compacted_with_stats`; the same fold
  produces per-part values stamped onto each `CompactionPart` (field 12). It
  recomputes from the rows it writes, never by merging input stamps: inputs
  may predate a declaration, and recomputation is free at a point that is
  already decoding everything.
- **Erasure rewrite (ADR-0064).** Rewrite outputs are `CompactionPart`s and
  get the same treatment, with one rule promoted to a correctness invariant:
  a rewrite **must** recompute stamps over surviving rows and must never
  copy an input's stamp. A copied stamp could carry an erased row's value as
  the recorded extremum — a statistics answer that resurrects erased data.
  A test pins this: erase the row holding the column's maximum, assert the
  rewritten part's stamp shrinks.
- **Metrics and spans signals** never stamp the field: declared typed
  columns are a logs concept (ADR-0090). The field is simply absent, which
  is the legal default forever.

**Null-count capture, end to end.** The null count is observed in exactly
one place — the writer, at encode time — and carried unchanged to the
record; nothing downstream recomputes it and nothing defaults it.

One representation, stated once and used everywhere in this ADR: every
per-block and whole-object figure below counts **NULL rows**. That is what
`NumStat.null_count` stores in the tree today
(`crates/ravel-logseg/src/block.rs`: the struct doc, and the
`i64_stat`/`bool_stat`/`f64_stat` constructors, which increment it for
every row whose merged-view resolved value for the column's name is absent
or of another type). It is **not** a non-null count; the two are
complements — `non_null(block, column) = block_rows(block) −
nulls(block, column)` — and a fold written in one representation reading
state stored in the other reverses every block's contribution. An earlier
revision of this section called the two "equivalent"; they are not, and
the fold below is therefore written in NULL units only, with `non_null`
appearing solely as the derived complement above, never as a stored input.

1. `RlogWriter` already tracks per-block, per-`(name, type)` NumStats for
   SKIP_IDX, including `NumStat.null_count`, the block's NULL row count
   for the column. No new per-block state is needed for the count; the
   fold consumes the field as stored.
2. Per block, in NULL units: `nulls(block, column) =
   NumStat(block, column).null_count` when the block carries a NumStat
   entry for the column, and `nulls(block, column) = block_rows(block)`
   when it does not — absent-in-block reads NULL for every row of that
   block. The fold therefore runs over the object's block list, not over
   the NumStat map; summing only blocks that carry an entry undercounts.

   The missing-entry arm is sound only under a writer invariant this
   amendment states explicitly instead of assuming, because
   `BlockWriteOut` carries `stats: Vec<NumStat>` with no availability
   field: on the wire between writer and fold, a missing entry would
   otherwise be ambiguous between "column absent from this block"
   (contribute `block_rows` NULLs) and "statistics unavailable"
   (the object must get no entry for the column at all) — two states
   whose correct outcomes are opposite, so guessing either way is a
   wrong exact answer waiting to happen.

   **Writer invariant (block-statistics completeness).** `write_block`
   and `write_block_columnar` (`crates/ravel-logseg/src/block.rs`) emit
   exactly one `NumStat` for every I64/F64/Bool column in the block's
   plan set, unconditionally: each numeric arm of the per-plan match
   pushes its stat with no skip path, and a column that no row of the
   block resolves a value for still emits a real entry with
   `null_count == block_rows` and zeroed bit patterns (the
   no-contributor branches of `i64_stat`/`bool_stat`/`f64_stat`), never
   no entry. Conversely, the plan-set construction in `build_object`
   (`crates/ravel-logseg/src/writer.rs`: a block's plan set is the
   columns present in its rows union the columns any of its rows
   resolves a stat winner for) guarantees that a numeric column missing
   from a block's stats was not planned for that block, which by that
   construction means no row of the block resolves a value for its
   `(name, type)` — exactly the state where a reader materializing the
   declared column produces NULL for every row of the block. A missing
   entry therefore has one meaning, `block_rows` NULLs. "Computed but
   unavailable" is unreachable in the current writers: all blocks of an
   object are encoded in one `build_object` pass by one writer instance
   under one column allocation, so no block can predate the per-column
   state; no path drops or truncates a computed stat; and the stat
   constructors' saturating `u32` null-count adds cannot fire because
   block row counts sit orders of magnitude below `u32` range (writer
   chunking targets, and the format's own 2^24 per-block record cap,
   `MAX_RECORDS`).

   **What must change if that code changes.** Any future writer path
   that can plan a numeric column without emitting its stat, or can
   compute a stat it cannot vouch for (a partial decode of an input, an
   accounting failure it cannot reconcile), must not express that state
   as a missing entry — missing already means all-NULL. It must add an
   explicit availability signal to `BlockWriteOut` (an
   unavailable-marked entry, not an absent one), and the fold must map
   "unavailable" to emitting no object-level entry for that column,
   through step 4's fail-closed rule. The completeness test below pins
   the invariant so such a change fails at authoring time instead of
   silently inverting a block's NULL accounting.
3. Whole object, in the same NULL units:
   `null_count(object, column) = Σ over blocks of nulls(block, column)`.
   No term of this sum is ever `block_rows − x`; the only subtraction in
   this section is the derived complement definition above, and it is
   never an input to the fold.
4. The fold cross-checks itself: per block carrying an entry,
   `NumStat.null_count ≤ block_rows(block)`; whole object,
   `Σ block_rows == the object's row count` and
   `null_count(object, column) ≤ that row count`. Note what the old
   cross-check ("total non-null plus null_count equals the row count")
   cannot do: that identity is symmetric under swapping `null_count` and
   `non_null`, so it holds equally well for an inverted fold and can
   never catch the inversion — only the mixed-null test below can. If any
   cross-check fails, the writer emits **no entry for that column on that
   object**. Never an entry with `null_count: 0`.

   An earlier revision listed a further trigger here: "any block's
   NumStat for the column is unavailable where one is required (a block
   encoded before the per-column state existed, any accounting path the
   writer cannot reconcile)". That trigger named a state the current
   writers cannot produce (the completeness invariant of step 2) and
   that the fold could not detect if they did — with no availability
   field on `BlockWriteOut`, "unavailable" and "absent from the block"
   are the same missing entry, so a fold clause conditioned on
   unavailability is unimplementable as stated, and keeping it in the
   normative text implied a detection that does not exist. The
   fail-closed obligation it gestured at stands and transfers: a future
   writer change that introduces a real unavailability state arrives
   together with the explicit availability signal step 2 requires, and
   the fold maps that signal to no-entry through this step's rule.
   Under the invariant, the cross-checks above are reachable only
   through a writer bug; they are defensive, and they are tested as
   such (below).
5. The exact `(min, max, null_count)` triple travels as one unit through
   `finish_with_stats` (L0 flush) and `finish_compacted_with_stats`
   (compaction and the ADR-0064 rewrite) to `record::build` and the part
   builder, which stamp it verbatim.

Exactness is entry-granular, and that is what keeps the proto3 wire shape
safe without a field change: `null_count` is a plain `uint64`, whose
absence is indistinguishable from zero on the wire — tolerable **only**
because a writer is forbidden to emit an entry unless every field in it is
exact. A decoded, valid entry with `null_count: 0` therefore means exactly
zero NULLs, never "unknown". The distinction is the wrong-answer generator
this rule exists for: `COUNT(col)` rewrites to `sample_count −
null_count`, and `MIN`/`MAX` over an all-NULL column (`null_count ==
sample_count`, min/max absent) is NULL — neither is the answer for "no
NULLs", and a defaulted zero silently converts one case into the other
with `Precision::Exact` attached. The decision-2 predicate polices the
checkable slice of this at decode (clauses 4 and 5); the remainder is a
writer invariant pinned by test, not by decode.

Tests that pin the capture: an all-NULL object stamps min/max absent with
`null_count == sample_count` and the read side answers `COUNT(col) = 0`
exactly; an object where one block lacks the column entirely stamps the
exact null count including that block's full row count (the assertion is
the exact figure, not "nonzero").

Two tests pin the step-2 invariant and the step-4 defense, split by what
each can honestly reach:

- **Completeness (ravel-logseg).** A block written with a plan set
  containing a numeric column that no row of the block carries must
  return a `stats` vector whose column-id set equals exactly the plan
  set's numeric (I64/F64/Bool) column-id set, with the all-NULL column's
  entry present and `null_count == record_count`. The assertions that
  fail if emission ever becomes conditional are the set equality and
  that entry's **presence** (with its exact count), not any downstream
  figure. Run against both `write_block` and `write_block_columnar`,
  which are documented as line-for-line mirrors and can drift apart.
- **Cross-check fail-closed (fold unit test).** The whole-object fold is
  a pure function over per-block `(stats, block_rows)` and the object
  row count; feed it hand-built inconsistent inputs — a per-block
  `null_count` exceeding its `block_rows`, and block rows not summing to
  the object row count — and assert no entry is emitted for the column,
  never an entry with zero. Hand-built, not fault-injected through the
  real writer: the integrated writer cannot produce these states
  (that is the invariant), so an end-to-end fault-injection version of
  this test would be vacuous, and a `FaultStore`-style counter proving
  the fault fired has nothing to fire on.

One capture test is mandatory and is the one that pins the representation
itself: a **mixed-null block** — an object containing at least one block
where the column is NULL in some rows and non-NULL in others (say 5 rows,
2 NULL), asserting the exact whole-object `null_count`, on data chosen so
that the column's total NULL count differs from its total non-NULL count.
The reason it must exist is the reason the inversion above survived a
review: on an all-NULL or no-NULL block the per-block pair
`(null_count, non_null)` is `(R, 0)` or `(0, R)`, so a fold that swaps
the two representations maps each such block's contribution to the other
uniform value, a suite built only from uniform blocks can be satisfied by
a swapped fold whenever the swapped totals coincide (for example
equal-sized all-NULL and no-NULL blocks), and the swap-symmetric identity
cross-check passes always. Only a block with
`0 < null_count < block_rows` makes the two representations emit
different figures, so only this test fails when the fold is written in
the wrong one.

Added write cost, stated for the band it must stay in: per object, one
`O(blocks × eligible declared columns)` comparison fold over NumStats the
writer already computed (no extra decode, no extra pass), plus roughly
`name_len + 25` bytes per eligible column on the record — order tens to a
few hundred bytes per object for realistic declared sets, against records
that already run ~750 bytes and an object PUT that dwarfs both. The
per-record cache and `.csnap` `entries_uncompressed_len` grow by the same
order; the wide-schema case (ADR-0100, dozens of declared I64 columns) is
the sizing worst case and belongs in the implementation's pre-registered
figures.

### 4. Read side: union of carriers, behind the existing gate

The fold copies `CommitRecord.declared_column_stats` onto the
`SnapshotEntry` (field 15) exactly as it copies `sample_count` and the ts
bounds today, and `CompactionPart.declared_column_stats` onto L1/rewrite
entries. Resolution then populates `SegmentRef.declared_column_stats` from
whichever record it is already reading: a listed or token-resolved commit
record above the watermark, a `SnapshotEntry` below it, a
compaction/rewrite part either way.

`partition_statistics` fills a declared column's
`ColumnStatistics::min_value`/`max_value` (and `null_count`, which is what
lets the same stock rule also answer `COUNT(col)`) with
`Precision::Exact` only when **both** hold:

1. `stats_are_exact()` — the existing gate, unchanged and load-bearing: no
   content predicate, no prune predicate, `erasure.is_empty()` (a pending
   erasure invalidates committed statistics; ADR-0064 decision 2), and the
   ts bound fully contains every touched segment (so no extremum can be
   clipped away by a bound the stats don't see).
2. **Every** touched segment is covered for that column, where covered means:
   an entry with matching `(name, declared_type)` that passed decision 2's
   statistics validity predicate (a dropped entry is no coverage), from
   either carrier — a `SegmentRef` stamp or an ADR-0850/0942 `.cstat`
   entry for that segment and column. The predicate's structural clauses
   bind both carriers by decision 2 (each carrier additionally enforcing
   its own vocabulary clause), and coverage is what they gate: an
   entry from any carrier that has not passed them grants nothing. The
   two carriers are a union, per segment, per column. Any segment covered by
   neither leaves the column `Precision::Absent`, the `AggregateStatistics`
   rule silently does not fire, and the query scans — the ADR-0850 safety
   lemma, extended verbatim to the new carrier. Absence is never an error
   anywhere on this path.

**The union, defined precisely.** Both carriers are keyed by one segment
identity: the data object's content hash — `SegmentRef.content_hash`,
equal to `SnapshotEntry.content_hash` — which ADR-0942 already fixed as
the `.cstat` join key after correcting an earlier revision that keyed on
`SnapshotPartRef.blake3` (wrong because one part covers many segments, so
keying on it merges statistics across segments). This ADR introduces no
second identity and must not: the stamp lookup and the `.cstat` lookup for
one segment resolve through the same content hash, or "both carriers for
one segment" has no meaning.

Per `(content_hash, name, declared_type)` the reader resolves exactly one
value, by cases. Only entries that passed the statistics validity
predicate participate at all: an invalid entry is absent from the union
(no coverage), never one side of a conflict.

- **Stamp only, or `.cstat` only:** use it. This is the normal state — the
  live tail has only stamps, pre-stamp sealed history has only `.cstat`.
  A declared `STR`/`BYTES` column is *always* in this state: the stamp
  vocabulary excludes those types (decision 2), so the stamp side of the
  union is empty by construction, coverage is `.cstat`'s or nothing, and
  the ADR-0850 read path for string extrema continues unchanged — the
  union degenerating to one carrier is not an error and not a conflict.
- **Both:** the triples must be equal — min, max, and null_count each
  compared for exact equality (for the allowlisted types value equality
  and bit-identity coincide; a future F64 amendment must say which, and
  per the repo invariant it will be bit patterns). Equal: use the value —
  either carrier, they are identical. **Unequal in any field: that column
  is `Precision::Absent` for the whole query.** One conflicted segment
  poisons the column exactly as one uncovered segment does. The conflict
  is counted on its own metric, distinct from the decision-2 decode-defect
  metric, and logged with the segment's content hash and both triples.
- **Nothing is ever combined across carriers.** No sum of null_counts
  (both carriers describe the *same* rows once; a sum double-counts every
  NULL), no widening or narrowing of min/max, no preferring the "fresher"
  carrier. There is no arithmetic in the union, only equality.

A conflict is not a degraded mode to tolerate; it is evidence of a real
defect. The segment is immutable and both carriers claim to be exact
derivations of its contents, so disagreement means the writer's stamp
fold, the fold's copy, the `.cstat` build, or the object itself is wrong —
every one of those a bug or corruption. The query degrades safely to a
scan (never a wrong answer), but the conflict metric and log line exist so
an operator sees a ticket-shaped signal rather than a mysteriously slow
query, and the per-query coverage figure below counts a conflicted segment
as covered by neither carrier.

Tests that pin the union: both carriers present and equal — the rule fires
and the literal equals a full scan of the same data; both present with one
field unequal (once on min, once on null_count) — `Precision::Absent`, the
rule does not fire, and the conflict metric increments by exactly one
naming that segment; both present and equal with a nonzero null_count —
the `COUNT(col)` rewrite equals `sample_count − null_count` counted once,
which is the assertion that fails if any path sums the carriers.

The union is not an optimisation, it is what makes the feature exist on any
tenant with history: after this ADR ships, every tenant's data is split
between pre-stamp records (covered, if at all, by `.cstat`) and post-stamp
records. A reader consulting only stamps would stay `Absent` until retention
clears every pre-stamp record; a reader consulting only `.cstat` gains
nothing from the hoist. **The union reader is therefore a build obligation
of this ADR, not a follow-up.**

A per-query coverage figure (segments stamped / segments touched, per
carrier) is emitted under the existing per-phase cost accounting, because
this feature's characteristic failure is silent: a coverage regression
"reads as a slow query" (ADR-0942's phrase), and only a counter makes it a
ticket.

### 5. Migration class and convergence — the #944 question, answered plainly

This change spans two classes under ADR-0066 decision 4, and naming both is
the point (issue #944 is open on exactly this distinction):

- **The record fields are Class C** (immutable metadata records,
  additive-only). Class C has no convergence mechanism *by design*: commit
  records, compaction records, and rewrite records are never rewritten
  (repo invariant), so **no existing record ever gains the field**. There
  is no backfill that edits a Class C record; anything that looks like one
  is an in-place edit of an immutable object and does not happen.
- **The `SnapshotEntry` copy is Class B in mechanics only.** The fold
  rewrites `.csnap` parts, but the copy's *value* is bounded by its Class C
  source: a fold rebuild of any depth reproduces exactly the stamps the
  underlying records carry, and sealed parts are carried forward by
  reference and never re-listed (docs/catalog-and-mvcc.md "Fold reconcile
  pass"). ADR-0942's "Reported conflicts" section already establishes that
  ADR-0066's "the fold rewrites them continuously" premise is too strong
  for derived state bound to sealed history; this ADR is a second instance
  of the same shape, one level deeper: here even a forced rebuild fold
  cannot conjure the value, because the source records themselves lack it.

What that means for real tenants, stated without hedging:

- **A live tenant converges for new data only, immediately.** Every flush
  and every compaction after the writer upgrade stamps. Its pre-upgrade
  records never gain the field.
- **A quiescent, fully compacted, already-loaded tenant gains nothing from
  this ADR, ever, on its own.** No flush, no compaction, no fold-with-new-
  hours; its records are immutable and unstamped. This is precisely the
  reference corpus (`clickbench-v4`), so **the hoist alone moves the
  1,181 ms figure by zero on the corpus it was measured on.**
- Convergence of *coverage* (not of the records) is governed by the Class A
  forces on the data objects underneath: retention ages unstamped records
  out; rewrite-on-touch (any compaction or erasure rewrite) replaces
  unstamped inputs with stamped parts; and the ADR-0066 decision 5
  `maintain migrate` job could force-stamp sealed history — at the cost of
  rewriting every data object to change a statistic, which is
  disproportionate and is not this ADR's plan.
- **The backfill for sealed history is ADR-0942's, and it is a
  precondition, not an option.** The union reader (decision 4) covers
  sealed history through the part-bound `.cstat`, whose forced-rebuild
  backfill pass ADR-0942 already names as a build obligation. For the
  reference tenant the dependency chain is explicit: q07 answers from
  statistics there only after (a) this ADR's union reader lands and (b)
  ADR-0942's backfill pass has run. Any acceptance figure is taken after
  both and stamped with that precondition (the measurement-preconditions
  rule). On live tenants, (a) alone covers the tail immediately — which is
  the population `.cstat` structurally never reaches and the reason this
  ADR exists.

### 6. Dual-reader question (ADR-0066 decision 1) and reader retirement

There is no N-1 *wire* reader here: additive proto3 fields mean one reader
reads both old and new records under the same `format_version`, and the
absent-field branch **is** the old-format path. Two consequences:

- **The absence branch is permanent.** It is simultaneously the reader for
  every pre-stamp record and the fail-closed branch for staleness, type
  mismatch, and ineligible stamps (decision 2). It is never deleted: a
  Class C record lacking the field is legal for as long as retention keeps
  it live, which for an unbounded-retention tenant is forever. No recorded
  format floor is planned because no read path is ever retired; if a future
  change did want to require the stamp, it would need an
  audit-versions-style enumeration proving every live record carries it,
  floors recorded per ADR-0066 decision 3, and its own reviewed change —
  stated here so the option is on the record as floors-not-hope, and
  explicitly not exercised.
- **Readers-before-writers still binds, on the copiers.** The hazard is not
  the query path (an old query reader ignores the new fields and simply
  never shortcuts). It is the two processes that *re-encode* what they
  decode: the fold (decodes commit records, writes `SnapshotEntry`s) and
  the compactor (consumes L0 records, writes `CompactionPart`s). prost
  drops unknown fields on decode, so a lagging fold reading a stamped
  commit record writes a `SnapshotEntry` **without** field 15, and once it
  seals that hour the stripped copy is what every future resolve sees —
  sealed parts are never re-listed, and after the superseded-record sweep
  the snapshot copy can be the only surviving carrier. Silent, permanent
  coverage loss for those hours, recoverable only by a forced rebuild fold
  (records still present) or the `.cstat` plane (records gone). Same for a
  lagging compactor: stamped L0 inputs, unstamped L1 part, and the L0
  records are then superseded and swept. The rollout is therefore ordered
  exactly as ADR-0066 decision 1 orders version bumps: **every fold and
  compaction process is upgraded to copy (or produce) the field before any
  ingest process starts stamping.** This is a deployment gate of the
  implementing epic, with a mixed-version test (new-format record through
  an old-shaped fold must be detected by the coverage metric, not
  discovered by a slow query), not a hoped-for sequencing.

## Data flow

```mermaid
flowchart TD
  subgraph write["write time (values already in hand)"]
    ING["ravel-ingest flush<br/>RlogWriter NumStats per block"]
    CMP["ravel-maintain compaction / ADR-0064 rewrite<br/>re-encodes rows via finish_compacted"]
  end

  ING -->|"fold per-block NumStats,<br/>eligible declared columns only"| CR["CommitRecord<br/>declared_column_stats = 20"]
  CMP -->|"recompute over output rows,<br/>never copy input stamps"| CP["CompactionPart<br/>declared_column_stats = 12"]

  CR -->|"fold copies (upgraded fold only:<br/>a lagging fold strips the field)"| SE["SnapshotEntry (.csnap)<br/>declared_column_stats = 15"]
  CP --> SE

  CR -->|"listed / token-resolved,<br/>above the fold watermark"| REF["SegmentRef.declared_column_stats"]
  SE -->|"sealed hours,<br/>below the watermark"| REF
  CST[".cstat (ADR-0850/0942)<br/>pre-stamp + sealed history"] -->|"union keyed by content_hash;<br/>both carriers must be equal,<br/>conflict = Absent"| STATS

  REF --> STATS["LogsScanExec::partition_statistics<br/>gate: stats_are_exact() AND every<br/>touched segment covered, else Absent"]
  STATS -->|"Precision::Exact min/max/null_count"| AGG["DataFusion AggregateStatistics rule:<br/>MIN/MAX/COUNT to literal, zero data GETs"]
  STATS -->|"any segment uncovered:<br/>Absent, rule does not fire"| SCAN["LogsScanExec scan<br/>(slower, never wrong)"]
```

## Rejected alternatives

- **The deferred index plane (ADR-0849's `.istat` statistics packs).**
  Rejected for this problem because the hoist is strictly smaller and fires
  where the plane cannot. The plane builds off the acknowledgement path
  (fragments at compaction, packs at fold), so a live tenant always has an
  uncovered L0 tail and ADR-0849's metadata-only execution "cannot fire on a
  live tenant" by its own Consequences; the hoist stamps at flush, so the
  tail is covered the moment the commit record is durable. The plane needs
  an `IndexRoot`, three-dimensional coverage declarations, sweeper
  reference-set extension, and `HEAD`-writer gates before the first byte
  (ADR-0849 §1a, wave 0); the hoist's coverage bookkeeping is the snapshot
  itself — a segment's ref either carries the stamp or it does not, and the
  sweeper never sees a new object class. ADR-0849 §1a in fact lists the
  commit-record field as one of its three candidate statistics carriers;
  this ADR is that candidate, executed for min/max. The plane remains the
  right shape for what refs cannot carry (postings, grams, value packs) and
  is not superseded.
- **Doing nothing (rely on ADR-0850/0942 alone).** Rejected: the `.cstat`
  plane is structurally fold-bound. The always-unsealed recent tail and
  token-resolved segments are never covered, on any tenant, forever — so
  `MIN`/`MAX` on a live telemetry tenant (the actual product workload, not
  the quiescent benchmark corpus) keeps scanning no matter how many
  backfills run. It also leaves the extra GET + hash-validated join on
  every query that could shortcut, and q07-shape stays at 1,181 ms against
  22.6 ms wherever coverage is absent.
- **Stamp every dynamic `(name, type)` column, not just declared ones.**
  The writer has NumStats for all of them, so it is tempting. Rejected:
  record growth becomes a function of ingested shape rather than of an
  operator's declared contract (hundreds of dynamic columns are legal), the
  resolve path and record cache pay it on every tenant including ones that
  never query typed columns, and the declared set is the existing opt-in
  boundary for exactness features (ADR-0090; "approximation is opt-in and
  visible" cuts both ways — so is paying for exactness).
- **Include `STR`/`BYTES` extrema.** Rejected in this cut: unbounded
  user-data bytes on the hot metadata path, and an exact answer cannot be
  built from truncated extrema. `.cstat` already carries them for the
  fold-covered population; a future amendment could admit length-capped
  stamps as *bounds* for pruning, but bounds are a different feature with
  different precision semantics and must not be conflated with this ADR's
  `Precision::Exact` contract.
- **A per-object statistics sidecar next to each data object.** Rejected by
  construction, unchanged from ADR-0849 §2: one probe per object replaces
  3,469 data GETs with 3,469 sidecar GETs and moves nothing.
- **Bump `CommitRecord.format_version` to 2 for the addition.** Rejected:
  it converts a safely-ignorable addition into a flag-day (old readers
  fail closed on version 2 records they could have read perfectly well),
  contradicting the ADR-0052 precedent ADR-0066 made normative for Class C.
  The frozen field-number space is the version mechanism here.

## Consequences

- **What moves.** `MIN(col)`/`MAX(col)` (and `COUNT(col)`, via the exact
  null count) over declared I64/Bool columns answer from statistics with
  zero data GETs wherever the gate and coverage hold — including the live
  tail and token-resolved segments, which no fold-built structure can ever
  cover. On the reference corpus this lands only together with the union
  reader and ADR-0942's backfill (decision 5); the pre-registered
  acceptance figure for q07-shape is the stock-DataFusion order
  (tens of ms) with the catalog-resolve cost dominating, stamped with the
  backfill precondition.
- **What does not move.** q02 and q08 (predicate counts, `GROUP BY`
  counts): statistics cannot carry them; they remain `.cstat`-dictionary
  territory (ADR-0850). String-column extrema remain `.cstat`-only. Declared
  `f64` columns (once ADR-0101's writer release lands) keep scanning until
  a future amendment clears the float gate. Pre-stamp records never gain
  the field; their coverage is `.cstat`'s or nothing.
- **New wire surface.** Two mirrored messages and three additive fields
  (`CommitRecord` 20, `CompactionPart` 12, `SnapshotEntry` 15), all inert
  for old readers by proto3 additive semantics. No version bump, no reader
  floor change, no new object kind, no new key shape.
- **Costs.** Per object: an `O(blocks × eligible declared columns)`
  comparison fold at write time and ~tens-to-hundreds of bytes on the
  record; the per-record resolve cache and `.csnap` parts grow by the same
  order (the ~750 bytes/entry cache sizing note in docs/catalog-and-mvcc.md
  is updated by the implementation, and the wide-declared-schema worst case
  gets a pre-registered figure). Query-side: the stamp read is free (the
  bytes arrive with the records resolution already fetches).
- **Rollout gate.** Fold and compactor upgrade before ingest stamps
  (decision 6), enforced with a mixed-version test and the coverage
  metric; the failure it prevents is silent permanent coverage loss, not
  corruption.
- **Docs updated with the implementation, same commits:**
  docs/catalog-and-mvcc.md (commit-record and snapshot-entry field
  descriptions, cache sizing note), proto comments, and
  docs/query-engine.md's statistics-shortcut section. This ADR row is added
  to docs/adrs/README.md now.
- **Two new defect signals an operator must know.** Invalid entries
  dropped by the statistics validity predicate (decision 2, one metric
  labelled by carrier) and carrier conflicts (decision 4's equality rule)
  each land on their own metric. Both mean a
  writer-side or copy-side bug against immutable data, not load: a nonzero
  rate is a ticket, and the only query-visible symptom is statistics
  shortcuts quietly not firing.
- **Tests the implementation owes** (prove-the-test discipline): the
  erasure-rewrite recompute rule (erase the max, stamp must shrink); the
  statistics validity predicate of decision 2, one decode case per
  structural clause
  **per carrier** (one-sided min/max, `min > max`,
  `null_count > sample_count`, presence disagreeing with null count in
  **both** directions — both-absent with `null_count != sample_count`
  and both-present with `null_count == sample_count`, the latter being
  #970's live defect 2 — kind
  mismatch, duplicate name, each run against a stamp and
  against a `.cstat` entry, plus the `.cstat` redundancy case
  `non_null_count + null_count != sample_count`), each asserting the drop,
  the carrier-labelled defect metric, and the resulting `Absent`
  precision; the vocabulary cases (a `STR` stamp dropped, `F64` dropped
  on both carriers) and their positive mirror, the regression this
  amendment's third revision fixes: a valid `.cstat` `STR` (and `BYTES`)
  extremum passes, grants coverage, and yields `Precision::Exact`;
  the null-count capture cases of decision 3 (all-NULL object,
  block missing the column counted in full, the ravel-logseg
  completeness test pinning that a planned numeric column always gets a
  stats entry — presence and set equality, on both block writers — the
  fold-unit cross-check test on hand-built inconsistent inputs emitting
  no entry rather than zero, and the mixed-null block asserting the exact
  whole-object figure — the only case that distinguishes the NULL-count
  representation from its complement); the union cases of decision 4 (equal
  carriers fire and match a scan, one unequal field yields `Absent` plus
  exactly one conflict-metric increment, `COUNT(col)` proves null_count
  is never summed across carriers); mixed coverage (stamped tail +
  `.cstat` history + one uncovered segment) leaving precision `Absent`;
  the `stats_are_exact` erasure refusal extended to the new columns;
  proptest round-trip of the new messages with corrupt-input rejection
  typed, seed files checked in.
- **Amendment scope note.** The fail-closed amendments above (the
  statistics validity predicate with carrier-independent structural
  clauses and per-carrier vocabulary, entry-granular
  null-count exactness with the fold stated in NULL units only and the
  block-statistics completeness invariant named with the code that
  upholds it, equality-or-`Absent` union keyed by `content_hash`)
  change no wire
  field, no field number, no stamp eligibility, and no migration class;
  they
  constrain decoders (both carriers' — the `.cstat` join gains the row
  accounting and duplicate checks it previously lacked, while keeping
  its ADR-0850 `STR`/`BYTES` vocabulary), the writer fold,
  and the union reader that decisions 2-4 already established.
- **Interaction with ADR-0815 (clustered compaction).** ADR-0815 already
  plans additive per-part min/max bound fields for its clustering key;
  field 12 here is per *declared column* with exactness semantics, not a
  clustering-bound field. The implementing change coordinates field-number
  claims in `CompactionPart` with ADR-0815's, first-to-land takes the
  lower numbers; this ADR claims only field 12.

## Out-of-scope findings, reported not fixed

1. **docs/adrs/README.md index drift**: the index (111 rows) is missing
   entries for shipped ADRs including 0849, 0850, 0892, 0904, 0927, 0942,
   and 0954. This ADR adds its own row only, per the report-don't-silently-
   fix rule.
2. **Issue #873's float premise is time-limited**: "declared columns have no
   F64 variant" is true in-tree but ADR-0101 (Accepted) adds one; the gate
   in decision 2 exists because of that, and any reader of the issue should
   not treat the hazard as structural.

Refs: #873
