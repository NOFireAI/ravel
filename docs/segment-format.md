# RSEG v1: Ravel Segment Format (metrics)

Persistent contract (amended by ADR-0010 before first ship). Any change
bumps the version. Parsers treat every offset, length, and count as
untrusted input: bounds-check everything, fuzz all decoders. No `unsafe`.

> RSEG v2 amendment (ADR-0014): trailer `version = 2` changes the catalog
> section layout only. See "RSEG v2 amendment" below; everything above
> this note is the unchanged v1 spec and stays authoritative for v1
> objects, which remain readable indefinitely (docs/rseg-v2-plan.md §5).
>
> RSEG v3 amendment (ADR-0017): trailer `version = 3` adds one section
> (HIST_PAGES, native-histogram values), one SERIES_META extension, and
> one page encoding, as a strict superset of v2. See "RSEG v3 amendment"
> below; v1 and v2 text above stays unchanged and authoritative for v1
> and v2 objects, which remain readable indefinitely
> (docs/rseg-v3-plan.md §5).

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

Writer note (non-normative): the current v2 writer emits the remaining
strings in sorted lexicographic order, the same as v1, because a sorted
dictionary compresses far better (issue #146 restored the sort after
issue #93 measured the first-occurrence order costing +532% compressed
LABEL_DICT bytes). This is a writer implementation choice, not a
contract: the rule above stays relaxed, existing v2 objects written in
first-occurrence order remain valid, and readers must not depend on the
order.

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
  special-case, independent of any dictionary ordering rule. Both v1 and
  v2 writers assign it ordinal 0 during the rank pass, before assigning
  the remaining strings their sorted ordinals (the v2 writer restored v1's
  sort in issue #146 because the sorted dictionary compresses far better;
  the normative ordering rule above stays relaxed, so this is a writer
  choice and readers still MUST NOT depend on any order beyond ordinal 0).
  Neither version's ordinal-0 placement depends on that sort.
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

## RSEG v3 amendment (ADR-0017)

Persistent contract, same status as v1 and v2 above. Decision record:
docs/adrs/0017-native-histograms-rseg-v3.md. Full derivation, value
model discussion, measured-size reasoning, and the phased implementation
plan: docs/rseg-v3-plan.md. This section is normative for v3 objects;
the v1 and v2 sections above are unchanged and remain normative for v1
and v2 objects. Readers dispatch on the trailer `version` field and must
accept 1, 2, and 3 indefinitely (no compactor exists yet to retire v1 or
v2 objects; docs/consistency-model.md).

v3 is a strict superset of v2: v2's catalog grammar, page format, and
section semantics are unchanged; v3 adds one section kind, one page
encoding, and three SERIES_META column blocks, for storing native
(exponential) histogram samples losslessly alongside scalar samples.

### Value model

A native histogram sample is span-based, the superset of OTLP's
contiguous shape and Prometheus's sparse shape:

- `scale`: signed, `>= -53`; `-53` means custom bucket boundaries via
  `custom_values` (Prometheus-only; no OTLP equivalent).
- `zero_count`, `zero_threshold`.
- `count`: total population; `sum`: optional.
- `reset_hint`: unknown / yes / no / gauge.
- Int/float duality: `zero_count`, `count`, and every bucket count are
  all u64 absolutes (integer histograms) or all f64 absolutes (float
  histograms) for one sample, never mixed within a sample.
- Per side (positive, negative): a list of `(offset, run of counts)`
  spans. `offset` is relative to the end of the previous span on that
  side (relative to index 0 for the first span). Gaps between spans are
  implicit zero-population buckets, never materialized.

Not carried: `min`/`max` (dropped at ingest, symmetric with classic-shape
handling); exemplars (out of scope for v3, docs/rseg-v3-plan.md §6).

### Trailer and versioning

Trailer layout, `footer_crc32c` computation, and reader protocol steps
1-4 are unchanged. `version = 3`. `magic` stays `"RSG1"`: it identifies
the format family, not a specific layout; a v1- or v2-only reader that
meets a v3 object fails closed with `UnsupportedVersion(3)`, checksum-
covered exactly as the v2 case. Versions other than 1, 2, 3 are
`UnsupportedVersion`.

### Section kinds v3

| kind | name | v3 content | comp (writer policy) |
|---|---|---|---|
| 1 | LABEL_DICT | unchanged from v2 | zstd |
| 3 | TS_PAGES | unchanged from v2; shared by scalar and histogram series | none |
| 4 | VAL_PAGES | unchanged from v2; scalar series only | none |
| 5 | SERIES_IDS | unchanged from v2 | none |
| 6 | SERIES_META | v2 grammar plus 3 column blocks (below) | zstd |
| 7 | HIST_PAGES | new: histogram-value pages, one page per histogram series (below) | none (pages self-compressed, same principle as VAL_PAGES) |

Kind 2 (SERIES_TABLE) is not emitted in v3, same as v2. Mandatory v3
kinds: LABEL_DICT, SERIES_IDS, SERIES_META, TS_PAGES, and at least one of
VAL_PAGES / HIST_PAGES (a segment with zero scalar series omits
VAL_PAGES; a segment with zero histogram series omits HIST_PAGES; a
segment cannot have zero series at all, per the existing empty-segment
rule). At most one section per known kind; unknown kinds still skipped.
Writers emit sections physically in the order 1, 5, 6, 3, 4, 7 (VAL_PAGES
before HIST_PAGES; HIST_PAGES is the new tail). v3 writers 8-byte-align
VAL_PAGES exactly as v2 ("VAL_RAW_F64 page alignment, v2" above).
HIST_PAGES has no alignment requirement: histogram payloads are never
viewed as a fixed-width Arrow buffer, only decoded field-by-field.

Footer validation additions for v3 objects, on top of v2's: SERIES_IDS
`count`, SERIES_META `count`, and FooterProto `series_count` must all be
equal (unchanged from v2); additionally, the count of series with
`value_kind = VAL_SCALAR` (below) must equal the number of non-empty
entries addressable in VAL_PAGES via `val_page_len != 0`, and
symmetrically for `value_kind = HIST_SPANS` against `hist_page_len != 0`
-- both violations Corrupted. If a segment has zero scalar series,
VAL_PAGES MUST be absent from `sections` (not merely present-and-empty);
symmetrically for HIST_PAGES and zero histogram series. A `sections`
entry for a kind whose mandatory-count is zero is Corrupted.

### LABEL_DICT, SERIES_IDS in v3

Unchanged from v2 (LABEL_DICT ordering rule and SERIES_IDS grammar
above).

### SERIES_META in v3

v2's grammar (`count`, `schema_count`, schemas, then column blocks 1-9:
`schema_ref`, `value_ord`, `sample_count`, `min_ts_delta`, `ts_span`,
`ts_page_gap`, `ts_page_len`, `val_page_gap`, `val_page_len`) is
unchanged, verbatim, including every validation rule in the v2 amendment
above. v3 appends three more column blocks, in this order, using the
same `block_len: varint` prefix framing as blocks 1-9:

```
10 value_kind:    count * u8      (0 = VAL_SCALAR, 1 = HIST_SPANS; any
                                    other byte value Corrupted)
11 hist_page_gap: count * varint
12 hist_page_len: count * varint
```

Semantics and validation, additive to v2's (all violations Corrupted,
never panics):

- Entry `i` in `value_kind` describes the series at `SERIES_IDS[i]`,
  same positional rule as every other column.
- `value_kind[i] = VAL_SCALAR` (0): `val_page_gap[i]`/`val_page_len[i]`
  (blocks 8/9) reconstruct that series' VAL page exactly as in v2;
  `hist_page_gap[i]` and `hist_page_len[i]` MUST both be `0`. A nonzero
  `hist_page_len[i]` on a scalar-kind series is Corrupted.
- `value_kind[i] = HIST_SPANS` (1): `val_page_gap[i]` and
  `val_page_len[i]` MUST both be `0` (a histogram series has no VAL
  page); `hist_page_gap[i]`/`hist_page_len[i]` reconstruct its HIST page
  the same running-sum way v2 reconstructs VAL page ranges: `offset_0 =
  hist_page_gap_0; offset_i = end_{i-1} + hist_page_gap_i; end_i =
  offset_i + hist_page_len_i`, overflow-checked, `end_i <=` the
  HIST_PAGES section's `len`. `hist_page_len[i] = 0` is Corrupted (a
  histogram series with a declared HIST_SPANS kind must have a
  non-empty page, same "zero-sample series dropped" principle as v1's
  VAL rule).
- `sample_count[i]`, `min_ts_delta[i]`, `ts_span[i]`, and the TS page
  columns (blocks 4-7) apply identically regardless of `value_kind`:
  every series, scalar or histogram, has exactly one TS page and the
  same timestamp-bounds reconstruction. A histogram series' `k`-th
  timestamp in its TS page corresponds positionally to its `k`-th
  histogram-value record in its HIST page, the same positional pairing
  v1/v2 use between a series' TS and VAL pages.
- Each of the three new blocks must consume exactly `block_len` bytes
  and contain exactly `count` elements; the last block (12) must end
  exactly at SERIES_META's uncompressed end. Trailing bytes anywhere are
  Corrupted.
- Pre-allocation from the new blocks' declared sizes is capped by
  remaining input size, same discipline as every existing count/length
  field.

Readers that only need v2-shape data parse blocks 1-9 exactly as today
and can skip blocks 10-12 wholesale via their `block_len` prefixes, at
zero cost -- the same lazy-skip property v2's column framing already
gives per-block for matcher-driven decode.

### HIST_PAGES (new section, uncompressed page-container form)

Container framing is byte-for-byte the same as TS_PAGES/VAL_PAGES: a
concatenation of pages located only through the SERIES_META gap/len
columns above, each page framed exactly per this document's "Page
format" (`enc: u8`, `comp: u8`, `crc: u32` over `series_id || enc ||
comp || payload`, then `payload`). HIST pages reuse the identical
container mechanics as VAL pages, adding only a new `enc` value and its
payload grammar.

New page encoding:

```
enc = 32   HIST_SPANS
```

(`page_enc` values 1 = TS_DELTA_VARINT, 16 = VAL_GORILLA, 17 =
VAL_RAW_F64 are unchanged; 32 is unused before v3. `comp` for HIST_SPANS
pages is always `0` in the v3 writer -- span/count data does not benefit
from per-page lz4 the way Gorilla-vs-raw tradeoffs do, and leaving `comp`
writer-chosen keeps the door open without a grammar change later.)

One HIST_SPANS page holds every histogram-value sample of its series, in
the same ascending-ts, insertion-order-preserving-ties order as the
series' TS page ("Sample order within a page" above). The page's record
count is implicit: it equals the series' `sample_count` from
SERIES_META, exactly as VAL pages today never restate their own count.
Payload is `sample_count` histogram records back to back:

```
one histogram record:
  flags:          u8
    bit 0   count_kind        0 = integer (u64 absolute), 1 = float (f64 absolute)
    bit 1   has_sum           1 = sum field present below
    bits 2-3 reset_hint       0 = UNKNOWN, 1 = YES, 2 = NO, 3 = GAUGE
    bits 4-7 reserved, writer MUST emit 0, reader MUST reject nonzero
  scale:          sint32 varint            (>= -53; -53 = custom boundaries)
  zero_threshold: f64 LE (8 bytes)
  zero_count:     varint (count_kind=0) | f64 LE (count_kind=1)
  count:          varint (count_kind=0) | f64 LE (count_kind=1)
  sum:            f64 LE, present iff has_sum
  custom_values:            present iff scale == -53
    custom_values_count: varint
    custom_values_count * f64 LE     (ascending boundaries)
  positive side:
    span_count: varint
    span_count * (offset: sint32 varint, length: varint)   length > 0
    bucket_counts: sum(length) entries, each
                   varint (count_kind=0) | f64 LE (count_kind=1)
  negative side: identical grammar to positive side
```

Validation, all violations Corrupted, never panics:

- `scale < -53` is Corrupted (only `-53` is the custom-boundaries
  sentinel; there is no meaning below it).
- `custom_values` is present if and only if `scale == -53`; a nonzero
  `custom_values_count` with `scale != -53`, or `scale == -53` with
  `custom_values_count == 0`, is Corrupted. Boundaries MUST be strictly
  ascending; non-ascending is Corrupted.
- `length` in every span MUST be `> 0` (a zero-length span is equivalent
  to a larger offset on the next span and is not a legal encoding).
- `span_count` and every `length` are capped by remaining input size
  before allocation, the existing count/length pre-allocation discipline
  applied to the new fields.
- `count` MUST be `>= zero_count` and `>= sum(all bucket_counts, both
  sides)`; both additions are overflow-checked in the count's kind (u64
  checked-add, or f64 accumulation using the same bit-pattern-exact
  accumulation the encoder used -- readers only bounds-check `count` for
  internal consistency, they do not recompute a canonical sum. NaN/Inf
  sums are legal float-histogram payloads and pass through unchanged,
  matching the Gorilla page's NaN/-0.0 transparency rule).
- Reading exactly `sample_count` records and ending exactly at the
  page's payload end (`page_len` from SERIES_META) is required; trailing
  bytes are Corrupted, short reads are Corrupted.
- `reserved` flag bits nonzero: Corrupted (forward-incompatibility is
  handled by the format-change procedure and a version bump, never by
  silently accepting unknown flag bits within one version).

Design notes: the flags byte carries `has_sum` and `count_kind`
per-record rather than hoisting them into SERIES_META because both can
legitimately differ sample-to-sample for one series (unlike `value_kind`,
which is fixed for a series' whole lifetime in a segment). Spans use
`offset`/`length` pairs rather than a bitmap so cost is `O(populated
buckets + span count)`, never `O(index range)`. Positive and negative
sides are fully independent lists (no shared span list with a sign bit)
because a value can have positive bucket 5 populated and negative bucket
5 empty; a shared list would need a sign field per span anyway. Full
rationale: docs/rseg-v3-plan.md §3.5.

### Protobuf changes (proto/ravel/segment.proto, additive only)

- `SectionKind` gains `SECTION_KIND_HIST_PAGES = 7`. Additive; no field
  numbers change, no existing value renumbered or reused (same pattern
  as v2's `SERIES_IDS = 5` / `SERIES_META = 6` addition).
- `Footer` message: no new fields, same as v2 -- everything v3 needs
  (section list, series_count, min_event_ts_ns) already exists.
- proto/ravel/commit.proto: no change. `CommitRecord.segment_format_version`
  (field 17) already carries `2` for v2 writes; v3 writes stamp it `3`
  (docs/rseg-v3-plan.md C8). The field is `uint32`, already wide enough.

### Checksum coverage, v3

Every byte a v3 reader interprets is covered exactly as strictly as the
corresponding v1/v2 bytes: `footer_crc32c` covers the trailer (including
the version byte) and FooterProto, including the new HIST_PAGES section
entry; SERIES_META's three new blocks (10-12) ride the section's
existing `Section.crc32c`, verified before any column (old or new) is
decoded, identical to how v2 added blocks 1-9 under the same crc that
v1's SERIES_TABLE used; HIST_PAGES pages are covered exactly as VAL_PAGES
pages are, by a per-page crc binding series_id, enc, comp, and payload,
checked on first touch. The HIST_SPANS `enc = 32` byte itself is covered
by the page crc it is an input to (a flipped byte fails the crc, not a
silent misdecode). HIST_PAGES pads nothing (no alignment rule), so there
is no new uncovered-on-selective-path byte class. No checksum is
weakened, removed, or moved. Full review: docs/rseg-v3-plan.md §4.

### Unchanged in v3

Page format (header, page crc with series-id prefix binding, `enc`/`comp`
byte values and grammars for TS_DELTA_VARINT/VAL_GORILLA/VAL_RAW_F64),
the raw-fallback rule, sample ordering and stable-sort duplicate
semantics, footer/trailer mechanics apart from `version`, the
suffix-read protocol, section crc mechanics, reader resource caps,
whole-object blake3 in the commit record, the object key layout, series
identity, LABEL_DICT/SERIES_IDS grammars, VAL_RAW_F64 page alignment, and
SERIES_META columns 1-9. The writer edge rules (zero-sample series
dropped, duplicate ids rejected, empty segment bounds = 0) carry over
verbatim and extend naturally to histogram series (a histogram series
with zero samples is dropped the same as a scalar one).

## RSEG v4 amendment (ADR-0018)

Persistent contract, same status as v1, v2, and v3 above. Decision
record: docs/adrs/0018-l0-l1-compaction.md. Full derivation and phased
implementation plan: docs/compaction-retention-plan.md. This section is
normative for v4 objects; the v1, v2, and v3 sections above are
unchanged and remain normative for v1, v2, and v3 objects respectively.
Readers dispatch on the trailer `version` field and must accept 1, 2, 3,
and 4 indefinitely until the v1 retirement path in
docs/compaction-retention-plan.md lands (docs/consistency-model.md).

v4 is the L0->L1 compaction output format, and a strict superset of the
real, already-merged v3 (ADR-0017, native histograms): v3's page format,
page encodings, section kinds, LABEL_DICT/SERIES_IDS grammars, and
HIST_PAGES grammar are unchanged. v4 adds no new section kind and no new
page encoding. It changes only two things: SERIES_META's per-series
columns 3-9 (v3 numbering) become per-*run* columns, because compaction
never deduplicates at rest -- a series that appears in more than one
input segment carries every input's samples forward as a separate run,
preserving each run's original dedup priority tuple
(`created_unix_ns`, `writer_epoch`, `writer_seq`, in-page index); and the
Footer gains five additive compaction-provenance fields. Trailer version
3 was already claimed by ADR-0017's native-histogram writer before this
compaction format's design landed; version 4 is the resolution recorded
in docs/compaction-retention-plan.md section 8.

A v4 writer never decodes or re-encodes a sample. TS, VAL, and HIST page
bytes are already fully framed (6-byte page header, then payload) in the
input objects the compactor read; `write_v4` copies each run's pages
verbatim into the output's TS_PAGES/VAL_PAGES/HIST_PAGES sections,
including HIST_PAGES bytes carried over from v3 histogram inputs, which
are treated as an opaque per-run blob and never touch the histogram
encoding described in the v3 section above.

### Trailer and versioning

Trailer layout, `footer_crc32c` computation, and reader protocol steps
1-4 are unchanged. `version = 4`. `magic` stays `"RSG1"`; a v1/v2/v3-only
reader that meets a v4 object fails closed with
`UnsupportedVersion(4)`, checksum-covered exactly as the v2 and v3
cases. Versions other than 1, 2, 3, 4 are `UnsupportedVersion`.

### Section kinds v4

Unchanged from v3: kinds 1 (LABEL_DICT), 3 (TS_PAGES), 4 (VAL_PAGES), 5
(SERIES_IDS), 7 (HIST_PAGES) carry the same content and comp policy as
v3; kind 6 (SERIES_META) carries the v4 grammar below instead of v3's.
Kind 2 (SERIES_TABLE) is still never emitted. Mandatory kinds, physical
section order (1, 5, 6, 3, 4, 7), VAL_PAGES 8-byte alignment, and
HIST_PAGES's lack of an alignment requirement are all unchanged from v3.
Footer validation additions from v3 (SERIES_IDS/SERIES_META/footer count
equality; scalar-kind and histogram-kind series counts against
`val_page_len`/`hist_page_len` nonzero counts) carry over unchanged; v4
does not relax any of them, it only adds the run dimension underneath
`value_kind`.

### SERIES_META in v4

v3's schema preamble (`count`, `schema_count`, schemas) is unchanged,
verbatim. Immediately after the schemas, v4 inserts one new fixed field,
`run_total: u32` (little-endian, same width and placement style as
`count`/`schema_count`), then 16 column blocks, in exactly this order,
each framed with the same `block_len: varint` prefix as v2/v3:

```
count: u32
schema_count: u32
schema_count schemas                      (unchanged from v2/v3)
run_total: u32                            (new: total runs, all series)

 1 schema_ref:         series-major, count * varint
 2 value_ord:          series-major, unchanged shape from v2/v3
 3 value_kind:         series-major, count * u8   (0 = VAL_SCALAR,
                        1 = HIST_SPANS; fixed for a series' whole run
                        list, same rule as v3)
 4 run_count:          series-major, count * varint (> 0; a series
                        surviving to this point always has >= 1 run)
 5 run_created_delta:  run-major, run_total * varint
                        (run.created_unix_ns - footer.base_created_unix_ns)
 6 run_epoch:          run-major, run_total * varint (run.writer_epoch)
 7 run_seq:            run-major, run_total * varint (run.writer_seq)
 8 run_sample_count:   run-major, run_total * varint (> 0)
 9 run_min_ts_delta:   run-major, run_total * varint
                        (run.min_ts_ns - footer.min_event_ts_ns)
10 run_ts_span:        run-major, run_total * varint
                        (run.max_ts_ns - run.min_ts_ns)
11 ts_page_gap:        run-major, run_total * varint (always 0: TS pages
                        are never alignment-padded, same as v1/v2/v3)
12 ts_page_len:        run-major, run_total * varint
13 val_page_gap:       run-major, run_total * varint (0 for HIST_SPANS
                        runs)
14 val_page_len:       run-major, run_total * varint (0 for HIST_SPANS
                        runs)
15 hist_page_gap:      run-major, run_total * varint (always 0, same as
                        v3)
16 hist_page_len:      run-major, run_total * varint (0 for VAL_SCALAR
                        runs)
```

"Series-major" means one entry per series, positionally matching
SERIES_IDS exactly as v1/v2/v3. "Run-major" means one entry per run,
runs ordered first by series (ascending series id), then within a
series by ascending `(created_unix_ns, writer_epoch, writer_seq)`; block
4 (`run_count`) is what lets a reader locate which contiguous run-major
slice belongs to which series, by running total over `run_count` the
same way v1 readers locate a series' labels by running total over
`name_count`.

Semantics and validation, on top of v2/v3's (all violations Corrupted,
never panics):

- `run_total` MUST equal the sum of `run_count` over all series; a
  mismatch is Corrupted (`SeriesCountMismatch`'s run-major counterpart).
- Every `run_count[i]` MUST be `> 0`. A series with zero runs must not
  appear in SERIES_IDS at all (the writer drops it, the same "zero-
  sample series dropped" principle v1 established for scalar samples,
  now applied at run granularity: a run with `sample_count == 0` is
  dropped first, and a series left with no runs afterward is dropped
  entirely).
- `value_kind[i]` fixes the kind for every run belonging to series `i`.
  A histogram-kind series' runs all have `val_page_gap`/`val_page_len ==
  0` and non-zero `hist_page_len`; a scalar-kind series' runs are the
  mirror image. Violating either is Corrupted, same rule as v3 applied
  per-run instead of per-series.
- `run_created_delta` reconstructs as `created_unix_ns =
  footer.base_created_unix_ns + run_created_delta`, non-negative by
  writer construction (the footer field is the minimum over all runs),
  overflow-checked i64 arithmetic.
- `run_min_ts_delta`/`run_ts_span` reconstruct a run's timestamp bounds
  exactly as v1/v2/v3 reconstruct a series' bounds, but per run:
  `min_ts_ns = footer.min_event_ts_ns + run_min_ts_delta`, `max_ts_ns =
  min_ts_ns + run_ts_span`.
- Page locations reconstruct per run, independently for TS
  (blocks 11/12), VAL (blocks 13/14), and HIST (blocks 15/16), using the
  same running-sum rule as v1/v2/v3: `offset_0 = gap_0; offset_i =
  end_{i-1} + gap_i; end_i = offset_i + len_i`, overflow-checked, `end_i
  <=` the owning section's `len`. Unlike v1/v2/v3, the running sum is
  over runs, not series -- a series with more than one run has more than
  one TS/VAL(or HIST) page, one per run, back to back in run order.
- Each of the 16 blocks must consume exactly its `block_len` and contain
  exactly its declared element count (`count` for blocks 1-4, `run_total`
  for blocks 5-16); the last block (16) must end exactly at SERIES_META's
  uncompressed end. Trailing bytes anywhere are Corrupted.
- Pre-allocation from `run_total` and every `block_len` is capped by
  remaining input size, the same discipline as every existing
  count/length field.

Readers that only need per-series identity (schema, labels, value_kind)
parse blocks 1-4 and can skip blocks 5-16 wholesale via their
`block_len` prefixes, at zero cost, the same lazy-skip property v2/v3
already give.

### Footer additive fields, v4

`proto/ravel/segment.proto`'s `Footer` message gains five fields,
additive only, field numbers 13-17, following the existing pattern of
v2's SERIES_IDS/SERIES_META section kinds:

```
sfixed64 base_created_unix_ns = 13;  // min created_unix_ns over all runs
uint32   ingest_hour_bucket   = 14;  // caller-supplied
bytes    input_set_hash       = 15;  // caller-supplied
uint32   part_index           = 16;  // caller-supplied
uint32   level                = 17;  // caller-supplied
```

All five are zero (or empty, for `input_set_hash`) and unused on v1, v2,
and v3 objects; v1/v2/v3 writers never populate them. `base_created_unix_ns`
is the only one of the five the writer derives itself (as the minimum
`created_unix_ns` across every run in the segment, the same way
`min_event_ts_ns` is derived from run bounds rather than taken from the
caller); the other four are opaque compaction provenance the caller
(the compactor) supplies and `write_v4` passes through unchanged.
`proto/ravel/commit.proto`'s `CommitRecord.segment_format_version` (field
17, `uint32`, already wide enough) stamps `4` for v4 writes.

### Checksum coverage, v4

Every byte a v4 reader interprets is covered exactly as strictly as the
corresponding v1/v2/v3 bytes: `footer_crc32c` covers the trailer
(including the version byte) and FooterProto, including the five new
Footer fields; SERIES_META's `run_total` field and all 16 column blocks
ride the section's existing `Section.crc32c`, verified before any
column (old or new) is decoded, identical to how v3 added its three
histogram blocks under the same crc v1/v2 already used; TS_PAGES,
VAL_PAGES, and HIST_PAGES pages copied verbatim from input runs keep
their original per-page crc unchanged (the crc binds `series_id || enc
|| comp || payload`, none of which a verbatim copy alters), and that crc
is re-verified by the v4 writer's caller before the copy the same as any
other page read, never re-derived by `write_v4` itself. No checksum is
weakened, removed, or moved.

### Unchanged in v4

Page format, all three page encodings and HIST_SPANS, the raw-fallback
rule, sample ordering and stable-sort duplicate semantics within a run,
footer/trailer mechanics apart from `version` and the five new additive
fields, the suffix-read protocol, section crc mechanics, reader resource
caps, whole-object blake3 in the commit record, the object key layout,
series identity, LABEL_DICT/SERIES_IDS grammars, VAL_RAW_F64 page
alignment, HIST_PAGES grammar, and SERIES_META blocks 1-2 (schema_ref,
value_ord). The writer edge rules (zero-sample run dropped, a series
left with zero runs dropped, duplicate series ids rejected, empty
segment bounds = 0) carry over and extend naturally to run granularity.

Reader support for v4 (quad-version dispatch) and a `ravel-cli` inspector
update are out of scope for this amendment and tracked separately
(docs/compaction-retention-plan.md, ticket P3); until that reader lands,
v4 objects can only be produced, not consumed, by this crate. This is a
deliberate, visible gap, not a silent one: no reader in this crate
claims to accept `version = 4` today, so a v4 object reaching an
unpatched reader fails closed with `UnsupportedVersion(4)` rather than
being silently misread as v3.
