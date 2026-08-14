# RSPAN: Ravel Span Segment Format

Persistent contract (ADR-0041). Any change bumps the trailer version. The
current trailer version is 3 (ADR-0054 added a mandatory per-block BLOOM
section over `service.name` and span-name tokens, and a block-local
dictionary-encoded `service_name` column, id 9). Trailer version history:

| version | ADR | added |
|---|---|---|
| 1 | ADR-0041 | initial RSPAN format (BLOCKS, SKIP_IDX) |
| 2 | ADR-0045 decision 2 | per-block duration bounds and a status mask in SKIP_IDX |
| 3 | ADR-0054 | mandatory BLOOM section and the `service_name` column |

Ravel is pre-release: one supported version at a time, earlier versions
rejected with the same typed `Corrupted` error as an unknown future version,
never carried by a dual reader (ADR-0045 decision 4, ADR-0054 decision 1,
ADR-0027 precedent). There is no in-place migration path across a version
bump: since no dual reader exists, code built against v3 cannot open a v1 or
v2 object to compact it forward. A v1 or v2 object outside development
buckets that must remain queryable has to be re-ingested from source under
v3; this is Ravel's accepted pre-release posture, not a gap to close later.
Parsers treat every offset, length, count, and tag read from stored bytes as
untrusted input: bounds-check everything, overflow-check every accumulation,
fuzz all decoders. No `unsafe`. Every violation is a typed `Corrupted` error,
never a panic and never wrong data.

RSPAN is a sibling of RLOG (docs/log-segment-format.md) and RSEG
(docs/segment-format.md), not an amendment: it copies their conventions
(16-byte trailer, protobuf footer, crc32c discipline, suffix-GET reader
protocol) and shares none of their bytes. All integers are little-endian.
"varint" means protobuf-style LEB128; "ivarint" means a signed value
zigzag-mapped then LEB128-encoded. A canonical LEB128 varint is at most 10
bytes and readers reject overlong encodings.

## Two departures from RLOG (ADR-0041)

RSPAN reuses RLOG's proven mechanics with exactly two decided differences,
both driven by the shape of span data rather than by a new mechanism:

1. **Sort/lookup key.** RLOG sorts records by `(stream_ref, ts)`, where a
   stream is a derived resource+scope identity. A span has no stream: a
   trace's spans deliberately cross services. `trace_id` *is* the primary key,
   so RSPAN sorts records by `(trace_id, start_ts)` and has no STREAM_DIR. A
   trace-id lookup is a bounded scan of the contiguous blocks whose trace_id
   range contains the id.
2. **Interval time bound.** A span has a start and an end. RLOG's skip index
   stores one point range `(min_ts, max_ts)` per block; RSPAN's stores a time
   *interval* `(min_start_ts, max_end_ts)`, pruned by overlap, not
   containment. A query window `[T1, T2]` prunes a block when
   `max_end_ts < T1 || min_start_ts > T2`.

RSPAN is deliberately leaner than RLOG: it has **no STREAM_DIR** (no derived
stream identity to catalog) and **no FIELD_DIR** (the merged attribute map is a
single `Map<Utf8, Utf8>` with one value type and no attr-level pruning, so it
is stored as one canonical blob per row rather than split into per-key
columns). The skip index is a single level, since a span object is one sorted
run with no second (stream-ref) dimension to summarize.

As of v3 (ADR-0054) RSPAN does carry a **BLOOM section**: service dependency
queries make `service.name` equality the entry point of most trace
investigation, and a linear attrs scan is the wrong tradeoff for it. The bloom
is a per-block token filter over `service.name` and span-name tokens; unlike
RLOG's degrade-on-corrupt bloom, RSPAN's BLOOM is mandatory and a missing or
malformed section is a typed `Corrupted` error, the same as a missing
SKIP_IDX. The `service.name` value is also lifted out of the attrs blob into
its own dictionary-encoded column (id 9) so a query reads it directly rather
than scanning the map. Span-events/links remain out of scope.

## Object layout

```
+---------------------------------------------------+
| BLOCKS       row blocks (column pages)            |  kind 1
| SKIP_IDX     interval + trace_id min/max index    |  kind 2
| BLOOM        per-block service/name token bloom   |  kind 3
| footer: SpanFooter protobuf bytes                 |
| trailer (16 bytes):                               |
|   footer_len:   u32                               |
|   footer_crc32c:u32                               |
|   version:      u16   (= 3)                       |
|   signal:       u8    (3 = spans)                 |
|   reserved:     u8    (= 0)                       |
|   magic:        [u8;4] = "RSP1"                   |
+---------------------------------------------------+
```

Writers emit the sections physically in kind order (1..3); readers rely only on
the footer's section offsets, never on adjacency. Bytes between sections are
permitted and MUST be `0x00`; readers never interpret them. All three sections
are mandatory: the reader rejects an object missing any of them as `Corrupted`.

`footer_crc32c` is computed over: the `SpanFooter` bytes, then `footer_len`
(u32 LE), `version` (u16 LE), `signal`, `reserved`, `magic`. Every trailer byte
except the crc field itself is covered.

## Reader protocol

Identical in shape to RLOG/RSEG:

1. Reject objects smaller than 16 bytes as `Corrupted`.
2. Suffix-GET the trailer (or the whole object if smaller). Verify `magic`,
   `version`, `signal`, `reserved`.
3. Require `footer_len > 0` and `16 + footer_len <= total_size`; otherwise
   `Corrupted`. If the suffix did not cover the footer, issue one more ranged
   GET.
4. Verify `footer_crc32c` (over the bytes defined above) before decoding the
   footer.
5. Validate the section table: all three mandatory kinds present, at most one
   of each, every range inside the section area, every `uncompressed_len`
   within the cap.

## SpanFooter

Defined in `proto/ravel/rspan.proto`
(`ravel.rspan.v1.SpanFooter`). Field numbers are frozen; only additive changes
with new field numbers are permitted. The message is separate from RLOG's
`ravel.logseg.v1.LogFooter`, not an extension of it: a span object has no
stream identity and its summary time bound is an interval, so the two footers
do not share a shape.

Fields:

- identity: `tenant_hash` (16 bytes), `shard`, `writer_id` (16 bytes),
  `writer_epoch`, `writer_seq`.
- summary: `min_start_ts_ns`, `max_end_ts_ns` (the object's time interval),
  `record_count`, `block_count`, and `min_trace_id` / `max_trace_id` (16 bytes
  each: the first and last record's trace_id in sort order).
- `sections`: the section table (`kind`, `offset`, `len`, `crc32c`, `comp`,
  `uncompressed_len`).
- compaction identity (mirrors ADR-0032's RLOG fields): `level`
  (0 = L0 flush object, 1 = L1 compacted part), `input_set_hash` (empty on L0),
  `part_index` (0 on L0). An L0 object written by the ordinary flush path
  stamps the sentinels; a future span compactor stamps its real values through
  `RspanWriter::finish_compacted`, which shares the whole encoding pipeline with
  `finish`, so an L0 write and an L1 merge of the same records are byte-
  identical except for these footer fields.

## Record shape

One row per span (`ravel_rspan::SpanRecord`), columnar in BLOCKS. Fixed column
ids (there are no dynamic columns):

| id | column          | type                | notes |
|----|-----------------|---------------------|-------|
| 0  | trace_id        | fixed 16 bytes      | always present; primary sort key |
| 1  | span_id         | fixed 8 bytes       | always present |
| 2  | parent_span_id  | fixed 8 bytes       | nullable (root spans have none) |
| 3  | name            | Utf8                | always present |
| 4  | start_ts_ns     | i64 (ns)            | always present |
| 5  | end_ts_ns       | i64 (ns)            | always present |
| 6  | status_code     | u8                  | OTLP status: 0 Unset, 1 Ok, 2 Error |
| 7  | status_message  | Utf8                | nullable |
| 8  | attrs           | Map<Utf8, Utf8>     | always present; canonical blob per row, minus `service.name` |
| 9  | service_name    | Utf8                | v3, ADR-0054; nullable; block-local dictionary |

The `attrs` column merges the resource, scope, and span attribute sets into one
map, following the exact resource+scope-wins-over-record convention
docs/log-segment-format.md documents for logs (reused, not redesigned): on a
key collision the resource/scope value wins over the span-level value, and
resource wins over scope. `ravel_rspan::merge_attrs` builds it. The map is
stored per row as one canonical blob (`uvarint(count)` then, per pair,
`uvarint(klen) key uvarint(vlen) value`, sorted ascending by key with unique
keys), so identical maps encode byte-identically.

**`service_name` (id 9, v3, ADR-0054).** The `service.name` value is lifted out
of the merged map into its own column at write time and is **not** duplicated
in the `attrs` blob. `merge_attrs` is unchanged and still produces the full
map; the extraction happens after. The reader re-inserts `service.name` into
the map when it rebuilds a `SpanRecord`, so a record round-trips byte-
identically. The column is nullable (a span whose merged attrs carry no
`service.name` has no value) and its id (9) also scopes the `service.name`
bloom (see BLOOM below). Its encoding is block-local dictionary (see BLOCKS).

**Span events and links** (OTLP's nested repeated fields) are out of scope for
v1. They are never decoded into first-class columns; if a producer decodes them
at all, they belong in `attrs` as an opaque blob value (for example
`"_events_raw"`), never silently dropped (ADR-0041: approximation opt-in and
visible).

## BLOCKS

A block holds a run of spans column by column: a header of page descriptors,
then the pages.

```
block:
  uvarint  record_count
  uvarint  page_count
  page_descs[page_count]:
    uvarint  column_id
    u8       enc          (1 Plain, 2 Bitmap, 3 FixedWidth, 4 Dict)
    u8       comp         (0 none, 2 zstd)
    uvarint  len          (stored, possibly compressed)
    uvarint  uncomp_len
  payload: the stored page bytes, in descriptor order
```

Columns are emitted in ascending column-id order for byte-deterministic output.
A nullable column that is present in some but not all rows of the block carries
a presence bitmap page (`enc = Bitmap`) immediately before its value page; a
nullable column absent from every row of the block occupies zero bytes.

Value encodings are plain (integers as ivarints; strings as
`uvarint(len)`-prefixed blobs; fixed-width values concatenated) except the
`service_name` column, which is dictionary-encoded (`enc = Dict`, v3). Each page
is independently wrapped in a zstd envelope (`comp = zstd`) when its encoded
form is at least 512 bytes and zstd is strictly smaller, else stored raw. The
`enc` tag is stored per page so a later version can add richer codecs without a
reader rewrite. A block's crc32c lives in its SKIP_IDX entry, not inline; the
reader verifies it before decoding anything.

The `service_name` column (id 9) uses a **block-local dictionary** (`enc =
Dict`, v3, ADR-0054). RSPAN has no segment-wide FIELD_DIR/STREAM_DIR to hold a
shared dictionary, and a block-local one needs no new section: the distinct
values of the block are written once, and each present row stores an index into
them. The page is:

```
dict page (enc = Dict):
  uvarint  dict_count
  values[dict_count]:  uvarint(len) bytes   (distinct, strictly ascending)
  indices[present]:    uvarint index         (< dict_count, one per present row)
```

The values are strictly ascending and deduplicated, so the encoding is
canonical and a block re-encodes byte-identically. Being nullable, the column
carries a presence bitmap page before the value page when it is present in some
but not all rows of the block, and occupies zero bytes when no row has a
`service.name` (the same nullable-column rule as `parent_span_id` and
`status_message`). The reader rejects a `dict_count` over its cap, a
non-ascending dictionary, and an out-of-range index as `Corrupted`.

## SKIP_IDX

One entry per block, in block order, zstd-compressed as a whole section.

```
skip_idx:
  u32 LE   block_count
  entries[block_count]:
    uvarint  block_offset       (relative to BLOCKS)
    uvarint  block_len
    u32 LE   block_crc32c
    uvarint  record_count
    [16]     min_trace_id
    [16]     max_trace_id
    ivarint  min_start_ts
    ivarint  max_end_ts
    ivarint  min_duration_ns    (v2, ADR-0045)
    ivarint  max_duration_ns    (v2, ADR-0045)
    u8       status_mask       (v2, ADR-0045)
```

`min_duration_ns`/`max_duration_ns` bound `end_ts_ns - start_ts_ns` over the
block's rows. They are derived at write time from the same endpoint scan that
already computes `min_start_ts`/`max_end_ts`, never stored per row. A negative
duration (a row whose `end_ts_ns` precedes its `start_ts_ns`) is a valid signed
value and ivarint encodes it natively; only true `i64` overflow of the
subtraction is rejected, as a typed `Corrupted` error at write time.

`status_mask` is one byte summarizing which OTLP status codes appear in the
block: bit 0 set when any row has `Unset`, bit 1 when any row has `Ok`, bit 2
when any row has `Error`. Bits 3-7 are reserved: the writer MUST emit them as
0, and the reader MUST reject a `status_mask` with any of them set as
`Corrupted`, the same as any other malformed field.

### Pruning soundness

A block is dropped only when its bounds prove no record in it can match:

- **Time.** Prune when the block's interval is disjoint from the query window:
  `max_end_ts < T1 || min_start_ts > T2`. Records sort by `(trace_id,
  start_ts)`, so `start_ts` is ordered within a block but `end_ts` is not; the
  entry therefore scans both endpoints when it is built.
- **Trace id.** When a trace-id predicate is present, prune when the id falls
  outside the block's `[min_trace_id, max_trace_id]` range. Because records sort
  by trace_id first, a single trace's spans occupy a contiguous run of blocks.
- **Duration (v2).** When a duration window `[D1, D2]` predicate is present,
  prune when the block's duration bound is disjoint from it: `max_duration_ns
  < D1 || min_duration_ns > D2`. Same overlap test as the time interval, over
  the derived `end_ts_ns - start_ts_ns` range instead of the raw timestamps.
- **Status (v2).** When a status-mask predicate is present, prune when the
  block's `status_mask` shares no bit with it: `(entry.status_mask & query_mask)
  == 0`. A block that might contain a matching status code is never pruned; a
  positive is not proof of a match, only the absence of a bit is proof of no
  match.

Survivors are read, crc-verified, decoded, and re-evaluated exactly per row:
interval overlap `start <= T2 && end >= T1`, and trace_id equality when
predicated.

Duration and status prune at block level only. `SpanQuery` carries no
duration or status field, so no per-row re-evaluation of either exists yet:
a caller that prunes on them must apply its own exact filter to the rows the
scan returns. The reader gains those fields when the query path that needs
them lands.

The block-level predicates are also sound only for a conjunctive query. Each
axis is an independent disjointness proof and a block is dropped when any one
of them proves no match, which is the correct AND-of-proofs for `a AND b`.
Under a disjunctive pushdown the same test would drop blocks that satisfy the
other branch. A future caller pushing `OR` must intersect differently.

A corrupt SKIP_IDX is
a loud `Corrupted` error, not a degrade: its bytes carry the block framing and
per-block checksums, so without it no block can be located or verified.

## BLOOM

One blocked token bloom filter per row block, positionally addressed: entry `i`
covers block `i` (v3, ADR-0054). The section is built and read through
`ravel-codec`'s `bloom_section` and `bloom` modules, shared with RLOG and not
reimplemented here. Each block's filter is blocked on 512-bit blocks, `k = 7`,
`m_bits = next_pow2(max(512, ceil(n * 9.585)))` for `n` distinct staged keys,
with three 64-bit hashes read from disjoint BLAKE3 digest ranges keyed by
`seed || field_id || token`. The seed is a fixed constant (0), stored inside
each serialized entry, so a reader recovers it from the bytes and a query never
needs it.

```
bloom section:
  u32 LE   entry_count            (one per block)
  entries[entry_count]:
    uvarint  entry_len
    u32 LE   entry_crc32c         (over the entry bytes)
    entry:   uvarint m_bits, u8 k, u64 LE seed, bit array (m_bits/8 bytes)
```

Two logical fields share the one section, distinguished by the `field_id`
hashed into every key:

- **`service.name`** tokens, under field id 9 (`COL_SERVICE_NAME`).
- **span `name`** tokens, under field id 3 (`COL_NAME`).

Tokens follow `ravel-codec`'s normative tokenizer (lowercase, split on any
non-alphanumeric character, 64-byte per-token truncation on a character
boundary), reused verbatim so a service-name or span-name equality query hits
the right field's bloom through one code path.

A bloom is false-positive-only (the widen-only pruning rule, ADR-0013): a
negative probe proves a token absent and lets a block be pruned; a positive
probe is no proof and the block is still scanned and re-evaluated exactly. For
an equality predicate `service_name = <lit>` or `name = <lit>`, the reader
tokenizes the literal and prunes a block only when the bloom proves some token
absent; a literal that tokenizes to nothing cannot prune.
`SkipIndex::candidate_blocks_with_bloom` applies this after the skip-index
prune, ANDing each predicate's proof exactly as the interval/trace/duration/
status axes do.

Unlike RLOG's bloom, RSPAN's BLOOM is **mandatory**: a missing section, a
whole-section crc mismatch, a per-entry crc mismatch, a truncated container, or
an entry count that disagrees with the block count is a typed `Corrupted`
error, never a silent degrade and never a panic.

## Checksum coverage

Same per-section/per-block crc32c discipline as RLOG/RSEG. Each section's
`crc32c` covers its stored bytes; each block's `crc32c` (in its SKIP_IDX entry)
covers the whole block; `footer_crc32c` covers the footer and trailer as defined
above. The BLOOM section (v3) has its own `Section.crc32c` over its stored bytes
like every other section, and additionally carries a per-entry `crc32c` in its
container framing, verified before an entry is probed; the shared
`bloom_section` code provides both and RSPAN adds no bloom checksum of its own.
The v2 `min_duration_ns`/`max_duration_ns`/`status_mask` fields add no new
checksum surface of their own: SKIP_IDX is read and verified as one
whole-section zstd blob under its `Section.crc32c`, so those fields inherit that
existing coverage exactly as the v1 fields did.

## Compaction (L0 → L1)

Compaction (ADR-0032's per-signal codec seam, `SpanCodec` in
`ravel-maintain`) rewrites many small L0 RSPAN flush objects for one sealed
`(tenant, shard, ingest-hour)` bucket into a handful of large L1 parts, the
span analogue of RSEG's and RLOG's L0→L1 compaction. The transaction
machinery (seal detection, `CreateIfAbsent` publish, convergence,
abandonment, the advisory cursor) is shared and signal-generic. An L1 part
is byte-for-byte a normal RSPAN object with `level = 1`; readers need no
special path.

RSPAN's merge is simpler than RLOG's because every `SpanRecord` field is
stored inline per row (there is no per-object stream directory a record
references indirectly, unlike RLOG's `STREAM_DIR`): a decoded span
re-encodes verbatim with no cross-object identity reconciliation. The merge
decodes each input's records, groups the union by `trace_id`, re-sorts by
`(trace_id, start_ts)` (this format's canonical order), and rebuilds
size-capped parts via `RspanWriter::finish_compacted`, splitting output on
trace boundaries so one trace's spans never straddle two parts.

Memory: `ravel-rspan` has no ranged section reader (no equivalent of RLOG's
`RlogRangeReader`), so the merge fetches and decodes each input object
whole. Raw bytes are bounded to one input at a time; decoded records for
the whole bucket are held in memory across the merge. This is the same
tradeoff RLOG's merge once accepted, not an oversight — a ranged
RSPAN reader is the natural follow-up once span bucket sizes in practice
justify it.
