# RSEG v1: Ravel Segment Format (metrics)

Persistent contract (amended by ADR-0010 before first ship). Any change
bumps the version. Parsers treat every offset, length, and count as
untrusted input: bounds-check everything, fuzz all decoders. No `unsafe`.

> RSEG v2 amendment (ADR-0014): trailer `version = 2` changes the catalog
> section layout only. See "RSEG v2 amendment" below; everything above
> this note is the unchanged v1 spec and stays authoritative for v1
> objects, which remain readable indefinitely (docs/rseg-v2-plan.md §5).

All integers little-endian. "varint" means protobuf-style LEB128; signed
values use zigzag.

## Object layout

```
+--------------------------------------------+
| page data (concatenated pages, see below)  |
| footer: FooterProto bytes                  |
| trailer (16 bytes):                        |
|   footer_len:   u32                        |
|   footer_crc32c:u32                        |
|   version:      u16   (= 1)                |
|   signal:       u8    (1 = metrics)        |
|   reserved:     u8    (= 0)                |
|   magic:        [u8;4] = "RSG1"            |
+--------------------------------------------+
```

`footer_crc32c` is computed over: FooterProto bytes, then footer_len (u32
LE), version (u16 LE), signal, reserved, magic. Every trailer byte except
the crc field itself is covered (ADR-0010 §4).

Reader protocol:
1. Reject objects smaller than 16 bytes as Corrupted.
2. Suffix-GET 64 KiB (or the whole object if smaller). Verify magic,
   version, signal, reserved.
3. Require `footer_len > 0` and `16 + footer_len <= total_size`; otherwise
   Corrupted. If the suffix does not cover the footer, issue one more
   ranged GET.
4. Verify footer_crc32c (over the bytes defined above) before decoding.

## FooterProto

Defined in `proto/ravel/segment.proto` (`ravel.segment.v1.Footer`). Fields:

- `tenant_hash` (16 bytes), `shard`, `writer_id`, `writer_epoch`, `writer_seq`
- `min_event_ts_ns`, `max_event_ts_ns`, `min_ingest_ts_ns`, `max_ingest_ts_ns`
- `sample_count`, `series_count`
- `sections`: repeated Section { `kind`, `offset`, `len`, `crc32c`,
  `comp` (0=none, 1=lz4, 2=zstd), `uncompressed_len` }
- unknown section kinds MUST be skipped by readers (forward compatibility)

Validation (all violations are Corrupted, never panics):
- At most one section per known kind; LABEL_DICT, SERIES_TABLE, TS_PAGES,
  VAL_PAGES are all mandatory in v1.
- Every section `[offset, offset+len)` must lie within
  `[0, total_size - 16 - footer_len)`, with overflow-checked arithmetic.
- `uncompressed_len` is capped by config (default 1 GiB per section,
  64 MiB per page) and the decompressed size must equal it exactly.
- Identity fields (tenant_hash, shard, writer_id, epoch, seq) MUST match
  the commit record the reader resolved the segment from (ADR-0010 §7).

Section kinds v1:

| kind | name | content |
|---|---|---|
| 1 | LABEL_DICT | string dictionary |
| 2 | SERIES_TABLE | per-series metadata |
| 3 | TS_PAGES | timestamp pages container |
| 4 | VAL_PAGES | value pages container |

Writers emit sections physically in that order (readers must rely only on
footer offsets; placement is self-describing).

LABEL_DICT and SERIES_TABLE are compressed as whole sections (zstd level 3
default). TS_PAGES / VAL_PAGES containers are NOT compressed as a unit; the
pages inside are individually compressed so one series is readable alone.
Section crc32c covers the stored (compressed) bytes.

## LABEL_DICT (uncompressed form)

```
count: u32
count strings: len:varint bytes (UTF-8)
```
Ordinal = position. Ordinal 0 is always the metric name string "__name__";
remaining distinct strings follow in sorted lexicographic order. Readers
reject out-of-range ordinals and non-UTF-8 strings.

Writer edge rules: zero-sample series are dropped (a page cannot encode
zero values); duplicate series ids across input entries are a writer
error; an empty segment records min_event_ts_ns = max_event_ts_ns = 0.
The raw-fallback rule means a 1-sample series always stores VAL as enc 17
(Gorilla's first value alone is exactly 8 bytes, which is not smaller).

## SERIES_TABLE (uncompressed form)

```
count: u32
count entries:
  series_id:    [u8;16]
  label_count:  u16          (pairs, including __name__)
  label_count * (name_ord: varint, value_ord: varint)   sorted by name bytes
  sample_count: u32
  min_ts_ns:    i64
  max_ts_ns:    i64
  ts_page:  (offset: varint, len: varint)   relative to TS_PAGES section start
  val_page: (offset: varint, len: varint)   relative to VAL_PAGES section start
```

Entries sorted by series_id bytes. One page pair per series in v1 (L0
objects are small; multi-page chunking is a v2 concern and the format
leaves room: readers locate pages only through these offsets).

## Page format (TS and VAL)

```
enc:  u8      encoding of the uncompressed payload
comp: u8      0=none, 1=lz4 (block format with u32 uncompressed-size prefix)
crc:  u32     crc32c over: series_id (16 bytes) || enc || comp || stored payload
payload
```

The crc includes the owning series_id as a prefix (ADR-0010 §4): a page
read through a mis-planned range or attributed to the wrong series fails
closed, and a flipped enc/comp byte cannot cause silent misdecoding.

Encodings:

| enc | payload |
|---|---|
| 1 TS_DELTA_VARINT | first ts_ns as varint-zigzag from 0; then deltas varint-zigzag. Handles irregular and out-of-order deltas. Accumulation is overflow-checked; each decoded ts must lie within the series entry's [min_ts_ns, max_ts_ns]. |
| 16 VAL_GORILLA | Gorilla XOR bit stream: first f64 raw 64 bits; then XOR with previous, classic control-bit scheme per the Gorilla paper 4.1.2, padded to byte with zero bits. Pure bit manipulation: NaN payloads, -0.0, denormals round-trip exactly. |
| 17 VAL_RAW_F64 | count * f64 LE. Fallback when XOR encodes larger than raw. |

Writers choose VAL encoding per page by encoded size (raw fallback rule:
emit GORILLA unless size >= 8*count bytes). Record whichever was used.

Sample order within a page: ascending ts; ties keep insertion order, which
requires a STABLE sort in the writer. Duplicate timestamps are preserved at
L0. Cross-segment dedup order is defined in docs/catalog-and-mvcc.md;
values compare by f64 bit pattern everywhere.

## Checksums

- Whole-object blake3 is computed by the writer and recorded in the commit
  record; the first 16 hex chars are embedded in the data key.
- footer_crc32c guards footer + trailer; Section.crc32c guards each
  section; the page crc guards series binding, enc, comp, and payload. A
  reader verifies exactly the bytes it touches.

## Size targets

L0: 8 to 64 MiB nominal (adaptive batching); tests use tiny objects.
Suffix fetch default 64 KiB covers footers for expected L0 series counts
(footer size is ~40 bytes per section entry + fixed fields; SERIES_TABLE
lives in a section, not the footer, so the footer stays small even at high
cardinality).

## RSEG v2 amendment (ADR-0014)

Persistent contract, same status as v1 above. Decision record:
docs/adrs/0014-rseg-v2-series-catalog.md. Full derivation, measured
numbers, and the phased implementation plan: docs/rseg-v2-plan.md. This
section is normative for v2 objects; the v1 sections above are unchanged
and remain normative for v1 objects. Readers dispatch on the trailer
`version` field and must accept both 1 and 2 indefinitely (no compactor
exists yet to retire v1 objects; docs/consistency-model.md).

### Trailer and versioning

Trailer layout, `footer_crc32c` computation, and reader protocol steps
1-4 are unchanged. `version = 2`. `magic` stays `"RSG1"`: it identifies
the format family, not a specific layout; a v1-only reader that meets a
v2 object fails closed with `UnsupportedVersion(2)`, which is the
correct, checksum-covered error (the version byte is covered by
`footer_crc32c` exactly as in v1). Versions other than 1 and 2 are
`UnsupportedVersion`.

### Section kinds v2

| kind | name | content | comp (writer policy) |
|---|---|---|---|
| 1 | LABEL_DICT | string dictionary, ordering rule relaxed (below) | zstd |
| 3 | TS_PAGES | timestamp pages container, unchanged | none |
| 4 | VAL_PAGES | value pages container, alignment rule added (below) | none |
| 5 | SERIES_IDS | sorted series ids | none |
| 6 | SERIES_META | schema dictionary + columnar per-series metadata | zstd |

Kind 2 (SERIES_TABLE) is not emitted in v2 objects. Mandatory v2 section
kinds: LABEL_DICT, SERIES_IDS, SERIES_META, TS_PAGES, VAL_PAGES; at most
one section per known kind; unknown kinds still skipped. Writers emit
sections physically in the order 1, 5, 6, 3, 4; readers rely only on
footer offsets, as in v1. `comp` in the table above is writer policy;
SERIES_IDS is deliberately never zstd-compressed (BLAKE3 ids are
incompressible, so compressing them is pure encode cost with no size
win). Footer validation additions for v2 objects: SERIES_IDS `count`,
SERIES_META `count`, and FooterProto `series_count` must all be equal;
a mismatch is Corrupted.

Bytes between sections are permitted, exactly as in v1 (section
placement has always been self-describing; readers locate every section
through its footer offset and length, never by assuming adjacency). v2
writers use this to 8-byte-align the VAL_PAGES section offset (see
alignment rule below). Any inter-section bytes MUST be `0x00`; readers
never interpret them.

### LABEL_DICT ordering rule, v2

Grammar is unchanged (`count: u32`, then `count` strings as
`len:varint` + UTF-8 bytes). Ordinal 0 is still always `"__name__"`. The
v1 rule that the remaining distinct strings follow in sorted
lexicographic order does not apply to v2 objects: for v2, the order of
ordinals `1..count` is unspecified, and readers MUST NOT assume any
ordering beyond ordinal 0. (v1 objects are unaffected; their dictionary
remains sorted and readers keep relying on that for v1 decode where they
do today.)

### SERIES_IDS (uncompressed form)

```
count: u32
count * series_id: [u8;16]     strictly ascending by byte comparison
```

Readers reject non-ascending ids and any section length that is not
exactly `4 + 16*count`.

### SERIES_META (uncompressed form, before section-level zstd)

```
count: u32                      (must equal SERIES_IDS count)
schema_count: u32
schema_count schemas:
  name_count: varint            (<= 65535, matching v1's u16 label_count domain)
  name_count * name_ord: varint (LABEL_DICT ordinals; strictly ascending
                                 by referenced name bytes)
then 9 column blocks, in exactly this order, each encoded as
  block_len: varint, then block_len bytes:

  1 schema_ref:     count * varint    (< schema_count)
  2 value_ord:      series-major: for each series, name_count(schema)
                    varints, one value ordinal per schema name, in
                    schema name order
  3 sample_count:   count * varint    (must fit u32; > 0)
  4 min_ts_delta:   count * varint    (min_ts_ns - footer.min_event_ts_ns)
  5 ts_span:        count * varint    (max_ts_ns - min_ts_ns)
  6 ts_page_gap:    count * varint
  7 ts_page_len:    count * varint
  8 val_page_gap:   count * varint
  9 val_page_len:   count * varint
```

Semantics and validation (all violations Corrupted, never panics):

- Entry `i` across all columns describes the series at `SERIES_IDS[i]`;
  entries are sorted by series id, exactly as v1's SERIES_TABLE.
- A series' label set is its schema's names paired positionally with its
  value ordinals. Schema name lists are sorted by name bytes, so the
  materialized pair order matches v1's "sorted by name bytes" rule;
  canonical series identity (ADR-0005) is unaffected. Readers validate
  each schema once at schema decode (ordinals in range, names strictly
  ascending by byte comparison, therefore no duplicate names); per-series
  materialization still goes through the existing label-set construction
  path.
  - Writer note, stated because v2's relaxed LABEL_DICT order makes it
    load-bearing: a schema's `name_ord` sequence MUST be derived from
    names already sorted by byte comparison (the writer's input labels
    are already in that order, the same invariant v1 relies on), never
    by sorting ordinal values. In v1, LABEL_DICT was itself
    lexicographically sorted, so ordinal order and name-byte order
    coincided; v2 drops that dictionary invariant, so a v2 writer that
    sorts by ordinal instead of by name would silently reorder every
    schema's label pairs and corrupt canonical identity.
- `__name__` is pinned to LABEL_DICT ordinal 0 by explicit writer
  special-case, independent of any dictionary ordering rule (v1 assigns
  it before ranking the remaining sorted strings; v2 assigns it before
  interning the remaining strings in first-occurrence order). Neither
  version's ordinal-0 placement depends on the sort v2 removes.
- Timestamp bounds reconstruct as `min_ts_ns = footer.min_event_ts_ns +
  min_ts_delta` and `max_ts_ns = min_ts_ns + ts_span`, overflow-checked
  i64 arithmetic. Both deltas are non-negative by writer construction.
  Reconstructed bounds feed the same per-entry TS decode bounds check as
  v1.
- Page locations reconstruct per column pair as: `offset_0 = gap_0;
  offset_i = end_{i-1} + gap_i; end_i = offset_i + len_i`,
  overflow-checked, with `end_i <=` the owning section's `len`,
  independently for TS (blocks 6/7, over TS_PAGES) and VAL (blocks 8/9,
  over VAL_PAGES). Gaps are 0 except where the writer inserted alignment
  padding.
- Each block must consume exactly `block_len` bytes; the last block must
  end exactly at the section's uncompressed end; each block must contain
  exactly its declared element count. Trailing bytes anywhere are
  Corrupted.
- Pre-allocation from `count`, `schema_count`, and any `block_len` is
  capped by remaining input size, as the v1 reader already does for
  corrupt counts.

The decoded per-series representation for v2 is the same logical shape
v1 readers already produce (id, label set, sample count, ts bounds, page
ranges); consumers above the segment-format layer are version-blind.

### VAL_RAW_F64 page alignment, v2

- The VAL_PAGES section `offset` MUST be congruent to 0 mod 8 (writer
  inserts zero bytes before the section as needed).
- Within VAL_PAGES, every page whose `enc` is 17 (VAL_RAW_F64) MUST have
  its payload start (page offset + 6) congruent to 0 mod 8 relative to
  the section start, hence also relative to the object start. The writer
  inserts `0x00` pad bytes before such a page's header and records them
  in that series' `val_page_gap`. This rule applies regardless of the
  page's `comp` byte; today's writer always emits VAL pages with
  `comp = 0` (none), so in practice the aligned bytes are always the
  bytes a consumer would view. A future writer that ever compressed a
  VAL_RAW_F64 page would have to align the decompressed view instead;
  no such writer exists in v1 and none is planned for v2. Pages with
  other encodings have no alignment requirement.
- TS pages are never aligned (varint payloads are decoded, never viewed
  directly).

This rule exists so VAL_RAW_F64 payloads become eligible for aligned,
zero-copy views by consumers that need them (docs/adrs/0013); it adds no
new reader logic beyond the gap columns SERIES_META already carries.

### Checksum coverage, v2

Every byte a v2 reader interprets is covered exactly as strictly as the
corresponding v1 bytes: `footer_crc32c` covers the trailer (including the
version byte) and FooterProto; each section's `Section.crc32c` covers
that section's full stored bytes, verified before any of its content is
decoded; the page crc covers series-id-bound payload, unchanged. Pad
bytes (inter-section and intra-VAL_PAGES) are never interpreted by
readers; they fall under the enclosing section's crc when that section
is read in full, and under the whole-object blake3 in the commit record
regardless. No v2 addition removes, weakens, or relocates a checksum
that v1 has today.

### Unchanged in v2

Page format (header, page crc with series-id prefix binding, `enc`/`comp`
byte values, all three page encodings and their payload grammars), the
raw-fallback rule, sample ordering and stable-sort duplicate semantics,
footer/trailer mechanics apart from `version`, the suffix-read protocol,
section crc mechanics, reader resource caps, whole-object blake3 in the
commit record, the object key layout, and series identity. The writer
edge rules (zero-sample series dropped, duplicate ids rejected, empty
segment bounds = 0) carry over verbatim.
