# ADR-0049: RLOG POSTINGS: exact block-level attribute pruning, opt-in per field

Status: Accepted

## Context

Attribute equality is the most common predicate in log search, and Ravel
cannot prune on it at all. `docs/query-engine.md` states the gap without
softening it:

> `attrs['k'] = 'v'` is not extracted into a fetch prune at all; it is
> evaluated entirely by DataFusion's residual over the merged `attrs`
> column.

The reason it was left there is sound and worth preserving. RLOG merges
resource, scope, and per-record attributes into one logical map with
record-wins precedence, so a stream-level (`STREAM_DIR`) match cannot
prune: a stream-level filter would drop a record whose match lives only in
its per-record attributes, and no residual can recover a dropped record.
ADR-0033 recorded that as Rejected Alternative A and named the fix
"a record-attribute-aware index".

This ADR is that index.

What already exists and constrains the design:

- `ravel-codec` holds the nine-codec registry, the blocked
  bloom, its section framing, and the normative tokenizer, extracted
  precisely so a second format could use them.
- RLOG's `BLOOM` section already gives cheap probabilistic pruning on word
  tokens and on exact string values of at most 64 bytes, field-scoped by
  column id. It is a bloom: a negative proves absence, a positive proves
  nothing.
- `FIELD_DIR` already splits attributes into per-key typed columns, capped
  at 1000 dynamic columns, with overflow folded into a scan-queryable
  `attrs_raw` column.
- ADR-0013's pruning-soundness invariant: a structure may prune only what
  it proves absent, and pruning may only ever widen the read set.

So the gap is not "no pruning" but "no *exact* pruning": the bloom cannot
answer `attrs['k'] = 'v'` precisely, and a 1% false-positive rate over
thousands of blocks means a selective query still scans most of them.

## Decision

### 1. A POSTINGS section, exact, at block granularity

A new RLOG section holds, per indexed field, a sorted term dictionary and
a posting list of **block indices** for each term. A query with an
equality or `IN` predicate on an indexed field intersects the posting
lists and scans only the surviving blocks.

Block granularity, not row. The scan already re-evaluates every predicate
exactly on decoded values, so row-precise postings would buy nothing at
read time and cost bytes at rest. This is a block-level set index
carried by Lucene's structure.

### 2. Sorted term blocks with a sparse index, not an FST

Lucene's finite-state term dictionary optimizes in-memory random access.
Over S3 the access pattern is different: one term probe should be one
small ranged GET, not a whole-section fetch and a graph walk.

The layout mirrors `SERIES_IDX`, which already solved this problem in
RSEG: a sparse term index holding every Nth term with its block offset,
then zstd-compressed term blocks, each individually addressable and
carrying its own crc32c. A probe binary-searches the sparse index, fetches
one term block, and reads one posting list.

### 3. Opt-in per field, never automatic

The indexed field list is explicit per-tenant configuration. Indexing
every attribute inflates cost without bound, and RLOG's existing
1000-column cap with visible overflow is the house style: bounded
by construction, with the excess still queryable and visibly unindexed.

A sensible default list is small and operator-editable
(`service.name`, `k8s.namespace.name`, `http.status_code` and similar).

### 4. A per-field distinct-value cap, degrading loudly to the bloom

Postings size scales with distinct values times blocks. A field that turns
out to be high cardinality (a request id, a user id, a trace id) would
inflate every object without bound.

When a field exceeds its distinct-value cap in one object, that field's
postings are dropped for that object and a counter is raised. The field
stays queryable through the existing bloom and the exact residual scan, so
the result does not change: only the cost does. This is the same
degrade-visibly pattern `attrs_raw` overflow already uses.

### 5. Soundness is unconditional

A missing, corrupt, or capped-out POSTINGS section degrades to today's
behaviour: bloom pruning plus an exact scan. Absent is always legal.
Postings prune only what they prove absent, and because they are exact
sets rather than sketches they introduce no false negatives at all.

Per-block crc32c on each term block, matching how the bloom section covers
its entries, because a ranged read cannot be verified by a whole-section
checksum.

### 6. Rebuilt at compaction, never merged

L0-to-L1 compaction re-blocks records, so an input's block indices are
meaningless in the output. `FIELD_DIR`, `SKIP_IDX`, and `BLOOM` are
already rebuilt from merged contents for exactly this reason; POSTINGS
joins them. No posting list is ever concatenated across inputs.

### 7. The SQL path extracts the predicate

`SpansPushdown`'s counterpart for logs gains attribute equality and `IN`
extraction, under the existing widen-only rule: DataFusion re-applies the
original predicate above the scan, so a prune may only widen the fetch,
never drop a true result. This is what finally closes ADR-0033 gap 2 and
makes `attrs['k'] = 'v'` prunable end to end.

Note the ADR-0033 subscript-planning gap is separate and still open: the
crate builds DataFusion with `features = ["sql"]` only, so no nested
`ExprPlanner` is registered and `attrs['k']` fails planning with a loud
error. Closing that is a prerequisite for this decision to be reachable
from SQL, and it is named here rather than assumed.

## Rejected alternatives

1. **A global inverted index across objects.** Rejected: it needs a
   coordinator, an unbounded rebuild, and a durability story, and object
   storage is the only durable backend. Per-object postings need none of
   those and are thrown away with the object.

2. **Row-granularity postings.** Rejected: the scan re-evaluates exactly
   anyway, so row precision changes no result and costs bytes at rest.

3. **Widen the bloom instead.** Cheaper, and it cannot be exact. Lowering
   the false-positive rate costs bits per element geometrically and never
   reaches zero, so a selective query over thousands of blocks still scans
   a tail of them.

4. **An FST term dictionary.** Deferred, not rejected on merit. The
   sparse-index-plus-term-blocks design is simpler, range-GET friendly,
   and mirrors a structure already proven in RSEG. Trigger for
   reconsidering: term-block fetches exceeding roughly 20% of a selective
   query's request count.

5. **Index every field automatically.** Rejected per decision 3.

6. **Term frequencies, positions, or scoring.** Rejected: observability
   log search is boolean filtering, not relevance ranking. BM25 has no
   consumer here, and the bytes would be pure cost.

7. **Postings for spans in the same change.** Deferred to its own
   decision. Spans get their cheap structures first (ADR-0045); a span
   postings section should follow the log one so it can copy a proven
   layout rather than co-invent one.

## Consequences

- One frozen-format change to RLOG, following the format-change procedure
  in full: `docs/log-segment-format.md` amended in the same change,
  checksum coverage reviewed for the new section, fuzz and property tests
  extended to the new grammar plus corrupt and truncated inputs, and the
  inspector taught to print it.

  **Amended: no trailer version bump.** This consequence
  originally required one. It was wrong. ADR-0029 already carves out new
  section kinds: RLOG readers MUST skip an unknown kind, and an absent
  section is legal, so adding one is purely additive in both directions.
  A new object carrying POSTINGS is readable by an old reader, which skips
  it, and an old object without one is readable by a new reader, for which
  absence is legal by decision 5 above.

  The correction matters beyond tidiness. Every trailer bump triggered a
  cascade of downstream breakage from version literals mirrored by hand
  across crates, sixteen sites for the RSEG bump alone. Requiring a bump
  that the format does not need would
  have bought that cost for nothing. The rule this leaves behind: a new
  section kind is additive and needs no bump; a change to an existing
  section's grammar, or to the trailer, does.
- Object size grows only for tenants that configure indexed fields.
  Indicative at the time of writing: 5 to 15% of object size for a four-field
  list, to be measured before the default list is set. **Measured: 0.13% for a
  four-field list over a 64k-record object, all four fields emitting
  postings** (three per-record HTTP attributes plus the resource-level
  `service.name`, which was made indexable by giving every indexed
  stream-level key its own FIELD_DIR column). The 5-to-15% estimate did not
  hold; it was a guess made before
  the block-granularity design existed and is roughly two orders of
  magnitude too high. POSTINGS scales with distinct values times blocks, both
  small for the low-cardinality default fields, and a high-cardinality field
  is bounded by the per-field distinct-value cap (decision 4) rather than by
  any percentage budget.
- Compaction gains work: postings are rebuilt, not merged.
- Query planning gains an index-selection step: with several indexed
  predicates, intersect the smallest posting lists first.
- The ADR-0033 subscript-planning gap becomes a prerequisite rather than a
  parallel nuisance, and is tracked as such.
- Nothing about durability, the commit protocol, the key layout, or
  snapshot resolution changes.

## Amendment: postings index the merged attribute view

Decision 7 said the SQL path extracts attribute equality and `IN`, and that
this closes ADR-0033 gap 2 and makes `attrs['k'] = 'v'` prunable end to end.
Two things were wrong with that, both found by building it.

### What was wrong

**The extraction cannot go through the reader's exact channel.**
`RlogReader::scan` evaluates every `content` arm exactly, per row, and
`equals` on an attribute reads a record's own dynamic column plus its
`attrs_raw` overflow only. It never reads the resource or scope blob,
because the write path keeps stream attributes out of the per-record
columns. SQL's `attrs` column is the merged view: resource and scope
attributes with the record's own overriding them on a key collision. So the
reader's per-record equality matches a strict subset of the SQL equality.
Pushdown is `Inexact`, which requires the scan to return a superset, and a
residual removes emitted rows but never restores a dropped one. This was
proved empirically, and the finding shipped instead of the push.

A prune-only channel answers it: arms that drive block pruning and never
the per-row filter.

**A prune-only channel is not enough either, because the index covers one
layer and the query spans two.** `FIELD_DIR` is object-wide, so one record
carrying a key as a per-record attribute makes it an indexed column for the
whole object, including for records whose value for that key lives in their
resource blob. Those records appear in no posting list for the term, so
probing it prunes their block away. This needs no key collision on a single
stream. It needs only the key present as a per-record attribute somewhere in
the object and as a resource attribute elsewhere, which two senders under
one tenant produce without arranging it.

`IN` was also wrong in decision 7. An `IN` list is a disjunction and the
prune channel intersects its arms, so one arm per element drops true
results. A sound disjunctive form is tracked separately.

### Decision

**POSTINGS indexes the merged attribute view**: for each record, the union
of its resource, scope, and own attributes, with its own winning on a key
collision, which is exactly what `ravel_sql::rlog_attrs::merged_attrs`
computes. The prune then answers the question SQL asks.

The `POSTINGS_VERSION` byte goes from 1 to 2. The grammar is unchanged; the
meaning of what a posting list contains is not, and a reader cannot tell the
two apart from the bytes. Same bytes with a different meaning is the failure
mode a version byte exists to prevent.

Readers keep both:

- version 2: probe the term and prune, since the index covers the same union
  the query does.
- version 1: prune only for a key that appears at no stream's resource or
  scope level anywhere in the object, which is the conservative rule
  previously landed. Every pre-existing object stays correct without a rewrite.

Decision 6 is unchanged in principle and gains a requirement: the compaction
rebuild indexes the merged view too, and writes version 2. A rebuild that
kept version 1 semantics under a version 2 byte would be the one way to make
this silently wrong.

The per-field distinct-value cap of decision 4 now counts merged values.
Resource attributes are low cardinality by nature, so this moves the count
little, and the cap already degrades loudly to the bloom.

### Why not keep the conservative rule

Because it excludes the keys the feature exists for. `service.name`,
`k8s.namespace.name`, and `deployment.environment` are resource attributes in
ordinary OTLP, and decision 3's own default indexed-field list names two of
them. Under the version 1 rule an object carrying them that way prunes
nothing at all. This would ship an index that is correct and close to
useless, and every measurement decision 4 asks for would report
a constant.

### Rejected alternatives

**Index both layers separately and intersect at probe time.** A posting list
per layer, unioned when the query spans both. It stores less than the merged
index, since a resource value is recorded once per stream rather than once
per block that holds the stream's records. It also reintroduces the
record-wins precedence at probe time, where getting it wrong is silent, and
saves little: records are sorted by stream, so a block usually holds one
stream and the merged index adds a handful of terms to it.

**Leave the conservative rule and document the limitation.** Honest, and it
is what is on main today. It defers the same decision to whoever first
measures the prune ratio and finds it is 1.0.

**Resolve the merged view at query time instead of index time.** The reader
would decode `stream_attrs` per block to decide whether a prune is safe. It
moves per-record work onto the path whose whole point is to avoid reading
records, and it still cannot recover a record the index never listed.

## Amendment 2026-08-20: precedence among a record's own duplicate occurrences

The 2026-08-03 amendment said POSTINGS indexes "the union of a record's
resource, scope, and own attributes, with its own winning on a key collision,
which is exactly what `ravel_sql::rlog_attrs::merged_attrs` computes". That
defined precedence *between* layers (record over resource/scope) but left
precedence *among* a single record's own multiple occurrences of one key
undefined. `LogRecord::attrs` is an ordered list that has always permitted
duplicate keys of any type mix (`encode_attrs` in `ravel-types` is written to
keep both), so this gap was reachable.

### What was wrong

Two functions folded a record's own duplicates in two different orders, and
nothing said which was authoritative:

- `writer.rs::indexed_term_columns` (write side) folded a record's attributes
  last-wins over their **original write-time occurrence order**.
- `reader.rs::rebuild_record` reconstructs a record's attributes in a fixed
  order the on-disk format dictates -- its FIELD_DIR columnar entries in
  `(name bytes, type)`-ascending order, then its `attrs_raw` overflow entries
  in the canonical `(key bytes, encoded value bytes)`-ascending order -- and
  `rlog_attrs::merged_attrs` (read side) then folds *that* list last-wins.

Write-time order and reconstruction order can disagree, so the POSTINGS term
for a key could be built from a value the query-time merged view does not
return, and an equality prune on the value the query truly needs could drop the
block that has it. This shipped and is reachable through the `attrs['k'] = 'v'`
pushdown; it has nothing to do with declared columns.

The on-disk format does not preserve write-time occurrence order, and inventing
a way to preserve it would break ADR-0029's frozen `attrs_raw` canonical
encoding. It is also unnecessary: no consumer needs write-time order, only a
single deterministic answer that write and read agree on.

### Decision

The winner among a record's own occurrences of one key is defined as **the last
entry in the read side's reconstruction order**, which is:

1. the record's columnar occurrences of that key, ascending by `FieldType`
   type byte (FIELD_DIR's `(name, type)` sort key restricted to one name);
   then
2. the record's overflow (`attrs_raw`) occurrences of that key, ascending by
   the frozen canonical `(key, encoded value)` order of `encode_attrs`
   (`ravel-types`); with the name fixed, this is the encoded-value-byte order.

The last entry of that combined order wins, then the record layer wins over the
stream (resource/scope) layer as before. This is exactly what `rebuild_record`
followed by `merged_attrs` already, incidentally, produces.

This amendment is what makes that incidental read-side behavior an intentional,
documented contract. It now governs both sides: `indexed_term_columns` computes
this same winner (its job is to *predict* what `rebuild_record` + `merged_attrs`
will report for an indexed key, not to independently pick one), and the read
side is its authoritative definition. No on-disk format changes: `ResolvedRow`'s
shape, `attrs_raw`'s encoding, and FIELD_DIR's layout are untouched, and the
`POSTINGS_VERSION` byte stays 2 (the change fixes which value a v2 list records,
not what a v2 list means).

### Version 1 objects

A version 1 POSTINGS list indexes the per-record column layer's first
occurrence per type only. A record-level duplicate key therefore can put the
merged-view winner in a value no v1 posting indexes: a same-type duplicate's
later occurrence lands in `attrs_raw`, which no posting covers, and a v1 object
records nothing that distinguishes "carried such a duplicate" from "did not".
Since that cannot be told apart per object without bookkeeping the v1 format
never wrote, a v1-grammar object now declines POSTINGS pruning for its equality
arms unconditionally, on both the content and prune channels. This is
widen-only (ADR-0013): it costs the pruning optimization on legacy objects,
never correctness, and it subsumes the narrower version-1 resource/scope
exclusion the 2026-08-03 amendment introduced (which covered only the
stream-level hazard, not the duplicate one).

### A distinct, adjacent hazard: cross-type stringification

Separately from the fold order, the reader resolved a merged-view prune
literal against a single `(name, type)` column: a pushed-down `attrs['k'] = 'v'`
is a `Str` literal (SQL's `attrs` is `Map(Utf8, Utf8)`), and a value of another
type that stringifies to the same text is a merged-view match that lives in a
different column's postings, which POSTINGS stores bit-exact per type, never
stringified. Probing only the `(name, Str)` column would prune away a block
whose match is, for example, an `I64` value with that text. The reader now
declines a `Str`-literal prune arm on a name that also has a non-`Str` column
(widen-only, ADR-0013). This is a different mechanism than the duplicate fold
order -- the term written is right, the column probed is wrong -- and is fixed
here because it sits in the same prune path.
