# RSPAN: Ravel Span Segment Format

Persistent contract (ADR-0041). Any change bumps the trailer version. The
current trailer version is 4 (ADR-0045 decision 3 replaced v3's single opaque
`attrs` blob column with per-key string attribute columns and promoted span
events into nested columns). Trailer version history:

| version | ADR | added |
|---|---|---|
| 1 | ADR-0041 | initial RSPAN format (BLOCKS, SKIP_IDX) |
| 2 | ADR-0045 decision 2 | per-block duration bounds and a status mask in SKIP_IDX |
| 3 | ADR-0054 | mandatory BLOOM section and the `service_name` column |
| 4 | ADR-0045 decision 3 | per-key attribute columns (`attrs_raw` overflow) and nested span-event columns |
| 5 | ADR-2707 (proposed) | mandatory KEY_IDX section (kind 4): the exact per-object `trace_id` index |

**Proposed: trailer version 5 (ADR-2707).** Version 5 adds one mandatory
section, KEY_IDX (kind 4), and changes no other byte. No shipped writer emits
it yet: the implementing task lands the v5 writer, the v5 reader, the
compactor's output version and the marker below together, and until then the
reader accepts exactly 4. Every "v5" statement in this document describes
that pending change. Making a kind mandatory changes what a valid object is,
which is the case the versioning rule reserves the trailer bump for (see
docs/log-segment-format.md, "Version", for the rule and its carve-out), so
v5 retires v4 the way v4 retired v3: one supported version, no dual reader,
a v4 object refused with `UnsupportedVersion` from the trailer alone.

Ravel is pre-release: one supported version at a time, earlier versions
rejected with a typed `UnsupportedVersion` error, never carried by a dual
reader (ADR-0045 decision 4, ADR-0054 decision 1, ADR-0027 precedent). v4
retires v3 the same way v3 retired v2 and v2 retired v1: the reader accepts
only the current version. There is no in-place migration path across a version
bump: since no dual reader exists, code built against v4 cannot open a v1, v2,
or v3 object to compact it forward. An older object outside development buckets
that must remain queryable has to be re-ingested from source under v4; this is
Ravel's accepted pre-release posture, not a gap to close later.

<!-- reader-supported-versions: ravel_rspan = 4 -->
<!-- Checked against ravel_rspan::footer::SUPPORTED_VERSIONS by
     scripts/check_format_version_docs.py; keep it in step with the current
     trailer version above when the reader window changes. -->

**Upgrade and rollback posture at HEAD.** A trailer-version bump is a
non-rollbackable, forward-only data-migration event: the reader admits exactly
one version, so once any object at the new version exists, a build that predates
the bump cannot read it: it fails closed with a typed `UnsupportedVersion`, and
retention's version hold declines to delete it but cannot make it readable
(ADR-0531, 2026-09-27 amendment). A tombstoned bucket holding such an object is
kept past its retention window rather than swept, and is retired by the first
pass of a build that reads the version. The irreversible step is the first write
at the new version; before it, a rollback to the earlier build is safe. The
N/N-1 window described below is staged for the v1.0 release and is not a posture any
released build has had: ADR-0531 fixes the format-lifecycle activation
milestone at v1.0, which is distinct from the software's first public release
at 0.9.0 and has not shipped. Before v1.0 a trailer-version bump may break
backward compatibility outright.

**Version lifecycle and migration (ADR-0066, normative).** RSPAN is a Class A
bulk
data-object format; from v1.0 onward the supported-version window becomes
N/N-1 (single-sourced as
`ravel_rspan::footer::SUPPORTED_VERSIONS`, which holds exactly one version
today; the writer, reader gate, `audit-versions`, `migrate`, and the compactor's
output-version constant all read it), rolled out readers-before-writers: a
release writing N+1 requires a fleet already reading N+1, so the "no dual
reader" statement above is the pre-v1.0 state, not a standing rule. Once an
N-1 reader exists, RSPAN compaction already decodes every input's span records
and re-encodes them from scratch, so an old-version object is migrated forward
by the normal compaction and `maintain migrate` paths with no special carve-out;
retention also ages old objects out. The migrate job verifies and raises the
per-(tenant, signal) format floor, and N-1 read support is deleted only once
every bucket's floor is >= N, citing those floors.

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

1. **Sort/lookup key.** RLOG sorts records by `stream_ref` first, where a
   stream is a derived resource+scope identity, so each stream is one
   contiguous run; within it records are in `ts` order when the object carries
   no sort descriptor, and by time bucket, clustering key, then `ts` under one
   (ADR-2135). A span has no stream: a
   trace's spans deliberately cross services. `trace_id` *is* the primary key,
   so RSPAN sorts records by `(trace_id, start_ts)` and has no STREAM_DIR. A
   trace-id lookup is a bounded scan of the contiguous blocks whose trace_id
   range contains the id.
2. **Interval time bound.** A span has a start and an end. RLOG's skip index
   stores one point range `(min_ts, max_ts)` per block; RSPAN's stores a time
   *interval* `(min_start_ts, max_end_ts)`, pruned by overlap, not
   containment. A query window `[T1, T2]` prunes a block when
   `max_end_ts < T1 || min_start_ts > T2`.

RSPAN is leaner than RLOG in two ways: it has **no STREAM_DIR** (no derived
stream identity to catalog) and **no FIELD_DIR section**. As of v4 (ADR-0045
decision 3) the merged attribute map *is* split into per-key columns (RLOG's
design), but RSPAN carries the `(column_id, name)` directory **inside each
block** rather than in an object-wide FIELD_DIR section: a block is
self-describing, so the whole-object and ranged decode paths rebuild a record
from the block alone with no external directory. RSPAN's map is
`Map<Utf8, Utf8>`, so every dynamic column is a Utf8 string column and there is
no per-type split (RLOG's typed FIELD_DIR splits a key seen with two value
types into two columns; RSPAN's single value type makes that inapplicable, a
named simplification). The skip index is a single level, since a span object is
one sorted run with no second (stream-ref) dimension to summarize.

As of v3 (ADR-0054) RSPAN does carry a **BLOOM section**: service dependency
queries make `service.name` equality the entry point of most trace
investigation, and a linear attrs scan is the wrong tradeoff for it. The bloom
is a per-block token filter over `service.name` and span-name tokens; unlike
RLOG's degrade-on-corrupt bloom, RSPAN's BLOOM is mandatory and a missing or
malformed section is a typed `Corrupted` error, the same as a missing
SKIP_IDX. The `service.name` value is also lifted out of the attrs blob into
its own column (id 9) so a query reads it directly rather than scanning the map. As
of v4, span **events** are also promoted out of the attribute blob into nested
columns (see "Record shape" and "BLOCKS"); span **links** remain out of scope
and are not promoted (a `_links_raw` value round-trips as an ordinary
attribute, a named gap, not an oversight).

## Object layout

```
+---------------------------------------------------+
| BLOCKS       row blocks (column pages)            |  kind 1
| KEY_IDX      exact (trace_id prefix, block) index |  kind 4 (v5, ADR-2707 proposed)
| SKIP_IDX     interval + trace_id min/max index    |  kind 2
| BLOOM        per-block service/name token bloom   |  kind 3
| footer: SpanFooter protobuf bytes                 |
| trailer (16 bytes):                               |
|   footer_len:   u32                               |
|   footer_crc32c:u32                               |
|   version:      u16   (= 4; 5 under ADR-2707)     |
|   signal:       u8    (3 = spans)                 |
|   reserved:     u8    (= 0)                       |
|   magic:        [u8;4] = "RSP1"                   |
+---------------------------------------------------+
```

Writers emit the sections physically in kind order today (1..3). Under v5
the order is BLOCKS, KEY_IDX, SKIP_IDX, BLOOM: KEY_IDX sits between BLOCKS
and SKIP_IDX, the placement RLOG uses for the same kind, so a suffix probe
that covers the footer, BLOOM and SKIP_IDX does not have to span KEY_IDX's
bucket frames (about 6% of the object) to reach them. Readers rely only on
the footer's section offsets, never on adjacency. Bytes between sections are
permitted and MUST be `0x00`; readers never interpret them. All three sections
are mandatory: the reader rejects an object missing any of them as `Corrupted`.
Under v5 all four are: a v5 object missing KEY_IDX is `Corrupted` the same
way.

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
5. Validate the section table: all three mandatory kinds present (four under
   v5), at most one of each, every range inside the section area, every
   `uncompressed_len` within the cap. `validate_sections` does not refuse a
   kind it does not know; it checks presence of the mandatory kinds and the
   range and compression tag of every entry.

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

One row per span (`ravel_rspan::SpanRecord`), columnar in BLOCKS. Ids 0..=13
are reserved fixed columns; ids 14+ are dynamic per-key attribute columns:

| id | column           | type            | notes |
|----|------------------|-----------------|-------|
| 0  | trace_id         | fixed 16 bytes  | always present; primary sort key |
| 1  | span_id          | fixed 8 bytes   | always present |
| 2  | parent_span_id   | fixed 8 bytes   | nullable (root spans have none) |
| 3  | name             | Utf8            | always present |
| 4  | start_ts_ns      | i64 (ns)        | always present |
| 5  | end_ts_ns        | i64 (ns)        | always present |
| 6  | status_code      | u8              | OTLP status: 0 Unset, 1 Ok, 2 Error |
| 7  | status_message   | Utf8            | nullable |
| 8  | attrs_raw        | Utf8            | v4; nullable; canonical blob of overflow attributes |
| 9  | service_name     | Utf8            | v3, ADR-0054; nullable |
| 10 | event_count      | i64             | v4; per-row event count; present iff the block has events |
| 11 | event_ts         | i64             | v4; flattened, one entry per event |
| 12 | event_name       | Utf8            | v4; flattened, one entry per event |
| 13 | event_attrs_blob | bytes           | v4; flattened, one entry per event |
| 14+| `<attr name>`    | Utf8            | v4; one per in-budget attribute key, name in the block directory |

The caller-facing `attrs` map (`ravel_rspan::SpanRecord`, `Map<Utf8, Utf8>`)
merges the resource, scope, and span attribute sets following the exact
resource+scope-wins-over-record convention docs/log-segment-format.md documents
for logs (reused, not redesigned): on a key collision the resource/scope value
wins, and resource wins over scope. `ravel_rspan::merge_attrs` builds it. The
map is stored **not** as one blob but split across the columns below; the
reader reassembles the identical map, sorted ascending by key with unique keys,
so a `SpanRecord` round-trips byte-identically.

**Per-key attribute columns (ids 14+, v4, ADR-0045 decision 3).** Each
attribute key (after lifting out `service.name` and `_events_raw`) becomes its
own Utf8 column. Column ids are assigned object-wide: the distinct keys are
sorted ascending and the first 1000 get ids `14, 15, ...`. Each block records
the `(column_id, name)` of the dynamic columns it carries in its own directory
(see BLOCKS), so a block decodes with no object-wide FIELD_DIR.

**`attrs_raw` (id 8, v4).** Keys past the 1000-column budget fold, per row, into
this canonical blob (`uvarint(count)` then, per pair, `uvarint(klen) key
uvarint(vlen) value`, sorted ascending, unique keys). It is scan-queryable but
carries no per-key column, so it is never pruned by a field predicate. Nullable:
a row with no overflow has no value. This reuses v3's `attrs` blob column id;
v4 retires v3 with no dual reader, so the id is repurposed rather than
versioned.

**`service_name` (id 9, v3, ADR-0054).** The `service.name` value is lifted out
of the map into its own column and is **not** duplicated among the per-key
columns or `attrs_raw`. The reader re-inserts `service.name` into the map when
it rebuilds a `SpanRecord`. Nullable; its id (9) also scopes the `service.name`
bloom (see BLOOM below).

**Span events (ids 10-13, v4, ADR-0045 decision 3).** OTLP span events (which
carry the exception stack traces that are a primary investigation target) were
stored in v1 as an opaque `_events_raw` attribute value (a hex blob of
concatenated length-delimited `Span.Event` messages). v4 promotes them into
nested columns: a per-row `event_count`, and three columns flattened one entry
per event across the block: `event_ts` (the event `time_unix_nano`),
`event_name`, and `event_attrs_blob` (the event's opaque serialized bytes, the
round-trip source of truth). The writer decodes a parseable `_events_raw` value
into these columns; the reader reconstructs the identical `_events_raw` value
from the verbatim `event_attrs_blob`s, so a `SpanRecord` round-trips
byte-identically. A `_events_raw` value that is not a valid events blob stays an
ordinary attribute. Span **links** stay out of scope: a `_links_raw` value (or
any other reserved key RSPAN does not name) round-trips as an ordinary
attribute, never promoted to columns (a named gap, ADR-0045).

## BLOCKS

A block holds a run of spans column by column: a header of page descriptors,
then a directory naming the block's dynamic attribute columns, then the pages.

```
block:
  uvarint  record_count
  uvarint  page_count
  page_descs[page_count]:
    uvarint  column_id
    u8       enc          (ravel-codec registry: 1 Plain, 2 Constant, 3 Rle,
                           4 DeltaZigzag, 5 DoubleDelta, 6 ForBitpack, 7 Dict,
                           8 Bitmap, 9 FixedWidth)
    u8       comp         (0 none, 2 zstd)
    uvarint  len          (stored, possibly compressed)
    uvarint  uncomp_len
  uvarint  dyn_dir_count
  dyn_dir[dyn_dir_count]:                    (ascending by column_id)
    uvarint  column_id     (>= 14, the dynamic range)
    uvarint  name_len
    name:    name_len UTF-8 bytes
  payload: the stored page bytes, in descriptor order
```

Columns are emitted in ascending column-id order for byte-deterministic output.
A nullable column that is present in some but not all rows of the block carries
a presence bitmap page (`enc = Bitmap`) immediately before its value page; a
nullable column absent from every row of the block occupies zero bytes.

Value encodings are `ravel-codec`'s (the crate shared with RLOG, ADR-0045
decision 1): the writer measures the applicable codecs per page and keeps the
smallest, and the self-describing `enc` tag records the choice. Each page is
independently wrapped in a zstd envelope (`comp = zstd`) when its encoded form
is at least 512 bytes and zstd is strictly smaller, else stored raw. A block's
crc32c lives in its SKIP_IDX entry, not inline; the reader verifies it before
decoding anything, so every byte below (page descriptors, the dynamic
directory, and every page) is under that one checksum.

**Dynamic-column directory (v4).** RSPAN has no object-wide FIELD_DIR section;
each block instead lists, ascending by column id, the `(column_id, name)` of
the dynamic attribute columns (ids >= 14) it carries. The reader reads the
directory to map a decoded column back to its attribute name. It rejects a
count over its cap, a non-ascending sequence, an id below the dynamic range, a
truncated name, and non-UTF-8 names as `Corrupted`. Fixed columns (ids 0..=13)
are implicit and never appear in the directory.

**Per-key attribute columns (ids 14+).** Each is a Utf8 string column, nullable
(a row that lacks the key has no value), decoded via `ravel-codec`'s string
codec. A row's keys past the 1000-column budget are not columns; they live in
`attrs_raw` (id 8) instead.

**Nested event columns (ids 10-13, v4).** When any row of the block has an
event, the block carries `event_count` (id 10), one i64 per row (0 for a row
with no events), and three **flattened** columns, `event_ts` (id 11, i64),
`event_name` (id 12, Utf8), and `event_attrs_blob` (id 13, bytes), each with
`sum(event_count)` entries, one per event across the whole block in row order,
with no presence bitmap (every entry is present). The reader decodes
`event_count`, sums it to learn the flattened length, then decodes the three
value columns and slices them back into per-row event lists. It rejects a
negative `event_count`, a sum over its cap, an event value column present
without `event_count` (or vice versa), and an event value column that is not a
single value page. A block with no events carries none of ids 10-13.

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

## KEY_IDX (trailer version 5, ADR-2707, proposed)

The exact per-object `trace_id` index: every distinct `(key8, block ordinal)`
pair in the object, where `key8` is the first 8 bytes of the 16-byte
`trace_id` and the block ordinal is the SKIP_IDX entry index. The grammar is
the one docs/log-segment-format.md defines under "KEY_IDX" and is not
restated here: it is built and read through `ravel-codec`'s `key_index`
module, shared with RLOG exactly as BLOOM is shared through `bloom_section`
(ADR-0054), and the section's own `version` byte (1) is separate from the
trailer version. RSPAN fixes the parameters the shared grammar leaves to the
writer:

- exactly one field, named `trace_id`, key type `Id16` (1); a header naming
  any other field set is `Corrupted`;
- `bucket_bits = 8` at the writer, 1 to 16 accepted by the reader;
- the section is mandatory (v5) and sits between BLOCKS and SKIP_IDX, the
  RLOG placement, so the suffix probe that covers the footer, BLOOM and
  SKIP_IDX (a reader choice, as RLOG's probe length is) never has to span
  the bucket frames. A `trace_id =` lookup then issues one ranged GET for
  `[0, min(len, 64 KiB))` of the section, sized from the footer's section
  entry (the one-field header and directory are about 1 KB at `bucket_bits
  = 8`, so that read also carries the first buckets), and one for the
  bucket it needs when that bucket lies beyond the first read; neither GET
  depends on adjacency. A read that is not a `trace_id` lookup
  never touches the section.

A `trace_id =` lookup probes KEY_IDX after the SKIP_IDX range test and
before any block is read: an absent key proves the trace absent from every
block, so the object is dropped with no block read (the SKIP_IDX
`[min_trace_id, max_trace_id]` test is an interval, and with random ids
nearly every object passes it); a present key names the blocks that hold
any trace whose id shares the 8-byte prefix. The reader takes the
intersection of that block set with the SKIP_IDX candidate set (the blocks
whose `[min_trace_id, max_trace_id]` admit the id): a block in the KEY_IDX
set that the range test excludes holds only a colliding trace and is
skipped, and a block the range test admits that KEY_IDX does not name holds
no span of either trace and is skipped. Neither disagreement is an error:
both are the two indexes pruning on different evidence. The intersection is
read as its contiguous runs. Today's `RspanRangeReader::trace_block_span`
derives its own candidate set from SKIP_IDX and refuses a non-contiguous
one, so it cannot take the intersection; ADR-2707's T3 (which touches
`ravel-rspan` for this) adds
`RspanRangeReader::block_run_span(&self, trace_id: &[u8; 16], blocks: &[usize])
-> Result<TraceBlockSpan, SpanSegError>`, which takes one explicit
contiguous run of block ordinals (adjacent, ascending, in range; anything
else is `Corrupted`) and returns the same `TraceBlockSpan` that
`trace_block_span` returns, built inside the crate (the type's fields stay
private); `trace_block_span` becomes the SKIP_IDX-only convenience that
calls it on its own run. The reader splits the intersection into maximal
contiguous runs, calls `block_run_span` per run, issues one ranged GET per
returned span, and decodes each with `decode_trace(&span, bytes)` as
today. Spans of one trace sort contiguously (the object
sorts by `(trace_id, start_ts)`), so the intersection is one run in every
case but a prefix collision whose other trace sits between this trace's
blocks, which yields two runs and two GETs. A prefix collision (two traces sharing 8 leading
bytes) is thus at most one extra block read and at most one extra GET, and
the per-row `trace_id` equality the reader already applies removes it from
the result. An empty intersection with a present key is also legal and
yields no rows. `Corrupted` is reserved for the
section's own checks (checksums, a block ordinal at or past the SKIP_IDX
block count, unsorted or duplicate entries). The section
is the source the catalog fold reads, by ranged GET, to build the per-part
`.kidx` leaf (docs/catalog-and-mvcc.md, "Per-part key-index leaves"); it is
never rebuilt from rows.

Like BLOOM and SKIP_IDX, a missing, truncated or checksum-mismatched KEY_IDX
is a typed `Corrupted` error under v5, never a degrade and never a panic.

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

The v4 grammar adds new byte ranges only *inside* the block: the
dynamic-column directory, the per-key attribute column pages, the `attrs_raw`
page, and the four event columns. Every one of those bytes lives within the
block body, and the reader verifies the block's `crc32c` (from its SKIP_IDX
entry) over the whole block before it reads a single one of them: the block
crc32c is recomputed and compared first thing in `read_block`, and decoding
stops on a mismatch. So each new range a v4 reader interprets is under a
checksum the reader verifies on its access path (ADR-0010 §4). No v4 field is
read from bytes outside that crc, and RSPAN adds no separate per-column or
per-event checksum: the whole-block crc32c is the single covering checksum for
all of them, exactly as it already was for the v1..v3 columns.

The v5 KEY_IDX section (ADR-2707, proposed) is read by range, so its
whole-section `Section.crc32c` is stored but not what a probe verifies. Its
own framing carries the access-path checksums docs/log-segment-format.md's
coverage map lists: a header crc32c over every header byte, a directory
crc32c per field, and a per-bucket crc32c over each bucket's stored bytes,
each verified before the bytes under it are interpreted; the `Section`
entry's offset, length and `uncompressed_len` are under `footer_crc32c` as
for every other section.

## Compaction (L0 → L1)

Compaction (ADR-0032's per-signal codec seam, `SpanCodec` in
`ravel-maintain`) rewrites many small L0 RSPAN flush objects for one sealed
`(tenant, shard, ingest-hour)` bucket into a handful of large L1 segments, the
span analogue of RSEG's and RLOG's L0→L1 compaction. The transaction
machinery (seal detection, `CreateIfAbsent` publish, convergence,
abandonment, the advisory cursor) is shared and signal-generic. An L1 segment
is byte-for-byte a normal RSPAN object with `level = 1`; readers need no
special path.

RSPAN's merge is simpler than RLOG's because every `SpanRecord` field is
stored inline per row (there is no per-object stream directory a record
references indirectly, unlike RLOG's `STREAM_DIR`): a decoded span
re-encodes verbatim with no cross-object identity reconciliation. The merge
decodes each input's records, groups the union by `trace_id`, re-sorts by
`(trace_id, start_ts)` (this format's canonical order), and rebuilds
size-capped parts via `RspanWriter::finish_compacted`, splitting output on
trace boundaries so one trace's spans never straddle two parts. Under v5
(ADR-2707, proposed) `finish_compacted` emits KEY_IDX through the same
pipeline as `finish`, so an L1 object carries its own index and the catalog
fold rebuilds the part's `.kidx` leaf from it when the compaction record
lands.

Memory: `ravel-rspan` has no ranged section reader (no equivalent of RLOG's
`RlogRangeReader`), so the merge fetches and decodes each input object
whole. Raw bytes are bounded to one input at a time; decoded records for
the whole bucket are held in memory across the merge. A ranged RSPAN
reader is the follow-up once span bucket sizes in practice justify it.
