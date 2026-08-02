# ADR-0045: RSPAN v2 and v3: pruning columns, a shared codec crate, and a reachable spans table

Status: Accepted (2026-08-02)

## Context

RSPAN v1 (ADR-0041) got the two hard decisions right. Records sort by
`(trace_id, start_ts)`, so one trace's spans occupy a contiguous run of
blocks and a trace lookup is a bounded scan. The skip index stores a time
*interval* per block and prunes by overlap rather than containment, which
is correct for data that has a start and an end.

Then it stopped. RSPAN v1 is a correct span storage format and not yet a
span investigation format. Three gaps, all measurable against real
workloads:

1. **No predicate support beyond trace id and time.** `BlockEntry`
   (crates/ravel-rspan/src/skip_index.rs:20) carries `block_offset`,
   `block_len`, `block_crc32c`, `record_count`, `min_trace_id`,
   `max_trace_id`, `min_start_ts`, `max_end_ts`. The two most common
   trace queries in existence are `duration > 500ms` and
   `status = Error`. Neither can prune a single block today.
2. **Service and operation are not addressable.** `service.name` lives
   inside the merged `attrs` blob, which is stored as one canonical
   string per row (crates/ravel-rspan/src/block.rs:188). Finding every
   span of one service decodes every row of every block. Span `name` is a
   real column but has no bloom, so operation lookup is a full scan.
3. **Attributes are one opaque blob per row.** Any attribute predicate
   decodes the whole map for every row. RLOG solved this problem already,
   with per-key typed columns, a 1000-column cap, and an `attrs_raw`
   overflow that stays scan-queryable.

Two further findings from the codebase survey shape the scope:

**The spans SQL table exists and is unreachable.** `crates/ravel-sql`
ships `spans_schema.rs`, `spans_provider.rs`, `spans_pushdown.rs`,
`spans_scan.rs`, and `spans_fetcher.rs`, with tests. None of it is
registered: `SessionTable` (crates/ravel-sql/src/session.rs:92) has only
`Metrics` and `Logs`, and `build_session` registers only those two
tables. No production code path constructs `SpansTableProvider`. Every
pruning structure this ADR adds would be unreachable by any user until
that changes, so registration is in scope here rather than left as a
follow-up.

**The codecs are duplicated.** `ravel-logseg` has a full encoding
registry (nine codecs, encoding.rs:14), a blocked bloom filter
(bloom.rs), its section framing (bloom_section.rs), and a normative
tokenizer (tokenizer.rs). `ravel-rspan` has its own smaller copies of
plain, fixed-width, and bitmap (block.rs:615-721) and no bloom at all.
`ravel-rspan` currently has zero internal dependencies.

## Decision

### 1. Extract a `ravel-codec` crate before touching either format

Move `encoding`, `bloom`, `bloom_section`, and `tokenizer` out of
`ravel-logseg` into a new `ravel-codec` crate. `ravel-logseg` re-exports
them so no other crate's imports change. This is a mechanical move with
no behavior change, and it lands on its own before any format work.

The tokenizer is the reason this is not optional. `docs/log-segment-format.md`
calls it "a normative part of the format because it defines word
semantics identically on the write and read paths." A second
implementation of a normative tokenizer is the precise bug class that
definition exists to prevent, and RSPAN needs tokens for its own blooms.

### 2. RSPAN v2: pruning columns and a service column

Trailer version 2. Additive to the `SpanFooter` proto (new field
numbers), extended `BlockEntry` grammar, one new fixed column.

`BlockEntry` gains, per block:

- `min_duration_ns`, `max_duration_ns` as ivarint. Derived at write time
  from `end_ts - start_ts`, never stored per row. The writer already
  scans both endpoints to compute `max_end_ts` (block.rs:227), so this
  costs no extra pass.
- `status_mask`, one byte, three meaningful bits: any Unset, any Ok, any
  Error. A `status = Error` predicate prunes every block whose Error bit
  is clear.
- A per-block bloom over the tokens of `service.name` and of span `name`,
  field-scoped by column id exactly as RLOG's is, with its offset,
  length, and crc32c in the skip-index entry.

Column ids gain `service_name` (id 9) as a dictionary-encoded fixed
column, promoted out of the `attrs` map. It is present on essentially
every span, it is low cardinality, and it is the entry point of every
service dependency workflow. `merge_attrs`
(crates/ravel-rspan/src/record.rs:108) keeps producing the map; the
writer lifts `service.name` out of it into the column and does not
duplicate it in the blob.

### 3. RSPAN v3: attribute columns and span events

Trailer version 3, landing after v2 is green. Ports RLOG's proven
design rather than inventing a span-specific one: per-key typed columns
with per-type splitting, a 1000-dynamic-column cap, and overflow keys
folded into an `attrs_raw` column that stays scan-queryable but is never
pruned by field predicates.

Span events become nested columns (`event_ts`, `event_name`,
`event_attrs_blob`, with a per-row count) rather than the opaque
`_events_raw` attrs value ADR-0041 chose for v1. Exception stack traces
live in span events and are a primary investigation target, so they
belong in a column, not in a string inside a map. Links stay out of
scope; they get their own decision when a query needs them.

### 4. Single supported version, no dual reader

Ravel is pre-release. ADR-0027 already set this precedent for RSEG: one
supported version at a time, earlier versions rejected with a typed
`UnsupportedVersion` rather than carried. RSPAN v2 retires v1 and v3
retires v2, each in the change that introduces it.

This is the format-change skill's dual-reader question answered
explicitly: no deployed data exists in RSPAN v1 outside development
buckets, so no reader keeps two paths. If that stops being true before v3
lands, this decision must be revisited in its own ADR rather than
assumed.

### 5. Register the spans table

Add a `Spans` arm to `SessionTable` and register `SpansTableProvider` in
`build_session`, following the one-signal-per-query rule ADR-0033
established for logs: the target table is decided from the `FROM` clause
before planning, and a query naming two real tables is rejected before
any catalog listing. Extend `SpansPushdown` with the duration, status,
and service predicates the v2 structures make prunable, under the
existing widen-only rule (ADR-0013): pruning may only widen the read set,
and DataFusion re-applies the original predicate above the scan.

`duration_ns` is not a stored column. It is exposed as a computed SQL
column so `WHERE duration_ns > 5e8` is expressible, and the pushdown maps
it onto the block-level duration bounds.

### 6. A ranged RSPAN reader

`ravel-maintain`'s span compaction fetches and decodes every input object
whole (crates/ravel-maintain/src/rspan_codec.rs:156), holding the
bucket's decoded records in memory across the merge. RLOG had the same
problem and solved it with `RlogRangeReader` (issue #275). RSPAN gets the
equivalent, and the compactor uses it. This is a latent operational
failure, not a design question: span volume decides when the compactor
runs out of memory, not whether.

## Rejected alternatives

1. **Depend on `ravel-logseg` from `ravel-rspan` instead of extracting a
   crate.** Acyclic today and the modules are already public, so it would
   work. Rejected: a span format crate depending on the log format crate
   inverts the layering the CLAUDE.md doc map describes, and it pulls
   `ravel-proto`, `ravel-types`, and `blake3` into a crate that currently
   has no internal dependencies at all. The extraction costs one
   mechanical commit and leaves both formats as siblings.

2. **Duplicate the codecs and the tokenizer into `ravel-rspan`.**
   Rejected outright. The tokenizer is normative; two implementations
   diverge, and the divergence is silently wrong rather than loudly
   broken.

3. **Store `duration_ns` as a per-row column.** Rejected: it is exactly
   `end_ts - start_ts`, both of which are already stored, so a column
   would add bytes to every row to answer a question the block bounds
   answer for free. The computed SQL column gives the ergonomics without
   the storage.

4. **One version bump covering v2 and v3 together.** Rejected: the
   combined grammar change does not fit one reviewable unit of work, and
   a half-implemented grammar cannot be landed green. Two sequential
   bumps are cheap while pre-release and each lands complete.

5. **Add an inverted index over span attributes now.** Rejected for this
   ADR: postings are a separate decision with their own cost model, and
   the block-level bloom plus min/max structures here are the cheap
   90 percent. Span postings follow the log postings work, not precede
   it.

6. **Buffer traces at ingest for tail sampling or completeness.**
   Rejected: buffering whole traces before acknowledging contradicts the
   strict-ack durability model, and tail sampling belongs at the
   collector. A trace missing its root span is reported as incomplete on
   the result, never waited for.

7. **Keep the spans table unregistered and ship pruning anyway.**
   Rejected: it would deliver structures no user can reach, and the
   epic's value would be unmeasurable.

## Consequences

- Two frozen-format version bumps, each following the format-change
  procedure in full: ADR first, `docs/span-segment-format.md` amended in
  the same change, explicit version byte, checksum coverage reviewed for
  every new byte range, fuzz and property tests extended to the new
  grammar plus corrupt and truncated inputs, and `ravel-cli rspan
  inspect` taught to print the new fields.
- The new bloom bytes are covered by their own crc32c in the skip-index
  entry, matching how RLOG covers per-block blooms. The skip index itself
  remains a whole-section zstd blob under `Section.crc32c`, so its new
  fields inherit existing coverage.
- Bloom pruning is a cost knob, never a correctness knob: a negative is
  proof of absence and a positive is no information, so a missing or
  corrupt bloom degrades to a scan with a counter. A corrupt skip index
  stays a loud `Corrupted` error, because it carries the block framing.
- `ravel-rspan` gains its first internal dependency (`ravel-codec`).
- Storage grows by roughly 40 bytes per block for the new statistics plus
  the bloom, which is sized from its own block's distinct token count.
- The spans SQL table becomes a public surface. It inherits the same
  tenant resolution, budgets, and one-signal-per-query rule the samples
  and logs tables already have.
- Span links remain unstored. That is a named gap, not an oversight.
