# RSEG v5: Ravel Segment Format (metrics)

Persistent contract. ADR-0027 leaves exactly one supported version until the
first public release: v5. The reader accepts trailer `version = 5` only;
versions 1 through 4 fail closed with the same typed `UnsupportedVersion`
error as any unknown future version. Any change to the layout bumps the
version and retires the previous one in the same change (an ADR plus a
version bump, never an in-place edit under the same number).

Parsers treat every offset, length, and count as untrusted input:
bounds-check everything, fuzz all decoders, return typed errors and never
panic. No `unsafe`. All integers little-endian. "varint" means
protobuf-style LEB128; signed values use zigzag.

> History (all superseded/amended by ADR-0027, which retired v1-v4 as
> readable/writable versions): the v1 baseline and its row-major catalog;
> the v2 columnar catalog (ADR-0014); native histograms (ADR-0017); the
> multi-run compaction layout (ADR-0018); and the sparse id index and
> chunked catalog (ADR-0026). v5 is the union of those layouts, restated
> here as one self-contained specification. Earlier versions live only in
> those ADRs.

![RSEG object byte layout](diagrams/rseg-layout.svg)

## Object layout

```
+--------------------------------------------+
| page data (concatenated pages, see below)  |
| catalog + dictionary sections              |
| footer: FooterProto bytes                  |
| trailer (16 bytes):                        |
|   footer_len:   u32                        |
|   footer_crc32c:u32                        |
|   version:      u16   (= 5)                |
|   signal:       u8    (1 = metrics)        |
|   reserved:     u8    (= 0)                |
|   magic:        [u8;4] = "RSG1"            |
+--------------------------------------------+
```

`footer_crc32c` is computed over: FooterProto bytes, then footer_len (u32
LE), version (u16 LE), signal, reserved, magic. Every trailer byte except
the crc field itself is covered (ADR-0010 §4). `magic` identifies the format
family, not a specific layout; the version field selects the layout and is
covered by `footer_crc32c`, so a reader that meets any non-5 version fails
closed with `UnsupportedVersion`, never a silent misdecode.

### Reader protocol

1. Reject objects smaller than 16 bytes as Corrupted.
2. Suffix-GET 64 KiB (or the whole object if smaller). Verify magic,
   `version == 5`, signal, reserved.
3. Require `footer_len > 0` and `16 + footer_len <= total_size`; otherwise
   Corrupted. If the suffix does not cover the footer, issue one more ranged
   GET.
4. Verify footer_crc32c (over the bytes defined above) before decoding.

A selective read that already knows a target series id then follows the
"Sparse reader protocol" below; a whole-catalog read decodes SERIES_META (or
reassembles the chunked form) directly.

## FooterProto

Defined in `proto/ravel/segment.proto` (`ravel.segment.v1.Footer`). Fields:

- `tenant_hash` (16 bytes), `shard`, `writer_id`, `writer_epoch`, `writer_seq`
- `min_event_ts_ns`, `max_event_ts_ns`, `min_ingest_ts_ns`, `max_ingest_ts_ns`
- `sample_count`, `series_count`
- `sections`: repeated Section { `kind`, `offset`, `len`, `crc32c`,
  `comp` (0=none, 1=lz4, 2=zstd), `uncompressed_len` }
- compaction provenance: `base_created_unix_ns` (min `created_unix_ns` over
  all runs, derived by the writer), `ingest_hour_bucket`, `input_set_hash`,
  `part_index`, `level`. An L0 flush is not a compaction output and stamps
  L0 sentinels (`level = 0`, `part_index = 0`, `input_set_hash` all-zero);
  the compactor supplies real values.
- unknown section kinds MUST be skipped by readers (forward compatibility)

Validation (all violations are Corrupted, never panics):

- At most one section per known kind. Mandatory: LABEL_DICT (1), SERIES_IDS
  (5), TS_PAGES (3), and exactly one catalog body — either the whole
  SERIES_META (kind 6) or the sparse pair SERIES_IDX (8) + SERIES_META_CHUNKS
  (9) together. Carrying both bodies, or one half of the sparse pair without
  the other, is Corrupted. VAL_PAGES (4) and HIST_PAGES (7) are each
  conditional (below), with at least one present when `series_count > 0`.
- Every section `[offset, offset+len)` must lie within
  `[0, total_size - 16 - footer_len)`, with overflow-checked arithmetic.
- `uncompressed_len` is capped by config (default 1 GiB per section,
  64 MiB per page) and the decompressed size must equal it exactly.
- SERIES_IDS `count`, the catalog body's `count`, and FooterProto
  `series_count` must all be equal.
- Identity fields (tenant_hash, shard, writer_id, epoch, seq) MUST match the
  commit record the reader resolved the segment from (ADR-0010 §7).

### Section-kind registry

The section-kind numbers are a frozen contract. Retired kinds keep their
numbers forever and are never reused, so a stray old object stays detectably
foreign rather than plausibly parseable.

| kind | name | status | comp (writer policy) |
|---|---|---|---|
| 1 | LABEL_DICT | string dictionary | zstd |
| 2 | SERIES_TABLE | **retired with RSEG v1; never emitted, number reserved forever** | — |
| 3 | TS_PAGES | timestamp pages container (scalar and histogram series) | none |
| 4 | VAL_PAGES | scalar value pages container | none |
| 5 | SERIES_IDS | sorted series ids | none |
| 6 | SERIES_META | run-major columnar catalog (whole-section form) | zstd |
| 7 | HIST_PAGES | histogram-value pages | none (pages self-compressed) |
| 8 | SERIES_IDX | sparse id index (present at/above the threshold) | none |
| 9 | SERIES_META_CHUNKS | chunked SERIES_META (replaces kind 6 at/above the threshold) | none (per-frame zstd) |

Below the sparse-emission threshold a segment carries kind 6 and neither 8
nor 9; at or above it, kinds 8 + 9 replace kind 6 (see "Sparse catalog").
VAL_PAGES is present iff at least one series is scalar-kind; HIST_PAGES iff
at least one series is histogram-kind. A `sections` entry for a conditional
kind whose count is zero is Corrupted.

Writers emit sections physically in a fixed order (readers rely only on
footer offsets; placement is self-describing): `1, 5, 6, 3, 4, 7` below the
threshold, `1, 5, 9, 8, 3, <pad> 4, 7` at/above it. Bytes between sections
are permitted and MUST be `0x00`; the writer uses them to 8-byte-align the
VAL_PAGES section offset (see "VAL_RAW_F64 alignment"). Section crc32c covers
the stored (compressed) bytes.

## LABEL_DICT (uncompressed form)

```
count: u32
count strings: len:varint bytes (UTF-8)
```

Ordinal = position. Ordinal 0 is always the metric name string `"__name__"`,
pinned by the writer. The order of ordinals `1..count` is unspecified;
readers MUST NOT assume any ordering beyond ordinal 0. (The writer emits them
sorted lexicographically because a sorted dictionary compresses far better,
but that is a writer choice, not a contract.) Readers reject out-of-range
ordinals and non-UTF-8 strings.

## SERIES_IDS (uncompressed form)

```
count: u32
count * series_id: [u8;16]     strictly ascending by byte comparison
```

Readers reject non-ascending ids and any section length that is not exactly
`4 + 16*count`. SERIES_IDS is never zstd-compressed (BLAKE3 ids are
incompressible). Entry `i` across the whole catalog describes the series at
`SERIES_IDS[i]`.

## SERIES_META (whole-section form, before section-level zstd)

The run-major catalog. Compaction never deduplicates at rest: a series that
appears in more than one input carries every input's samples forward as a
separate *run*, each with its own dedup-priority provenance
(`created_unix_ns`, `writer_epoch`, `writer_seq`, in-page index). An L0 flush
produces exactly one run per series.

```
count: u32                      (must equal SERIES_IDS count)
schema_count: u32
schema_count schemas:
  name_count: varint            (<= 65535)
  name_count * name_ord: varint (LABEL_DICT ordinals; strictly ascending by
                                 referenced name bytes)
run_total: u32                  (total runs across all series)

then 16 column blocks, in exactly this order, each framed as
  block_len: varint, then block_len bytes:

  1 schema_ref:        series-major, count * varint      (< schema_count)
  2 value_ord:         series-major: for each series, name_count(schema)
                       value ordinals in schema name order
  3 value_kind:        series-major, count * u8   (0 = VAL_SCALAR,
                       1 = HIST_SPANS; fixed for a series' whole run list)
  4 run_count:         series-major, count * varint (> 0)
  5 run_created_delta: run-major, run_total * varint
                       (run.created_unix_ns - footer.base_created_unix_ns)
  6 run_epoch:         run-major, run_total * varint
  7 run_seq:           run-major, run_total * varint
  8 run_sample_count:  run-major, run_total * varint (> 0)
  9 run_min_ts_delta:  run-major, run_total * varint
                       (run.min_ts_ns - footer.min_event_ts_ns)
 10 run_ts_span:       run-major, run_total * varint  (max_ts_ns - min_ts_ns)
 11 ts_page_gap:       run-major, run_total * varint  (always 0)
 12 ts_page_len:       run-major, run_total * varint
 13 val_page_gap:      run-major, run_total * varint  (0 for HIST_SPANS runs)
 14 val_page_len:      run-major, run_total * varint  (0 for HIST_SPANS runs)
 15 hist_page_gap:     run-major, run_total * varint  (always 0)
 16 hist_page_len:     run-major, run_total * varint  (0 for VAL_SCALAR runs)
```

"Series-major" means one entry per series, positionally matching SERIES_IDS.
"Run-major" means one entry per run, runs ordered first by series (ascending
id) then within a series by ascending `(created_unix_ns, writer_epoch,
writer_seq)`; block 4 (`run_count`) lets a reader locate which contiguous
run-major slice belongs to which series by running total.

Semantics and validation (all violations Corrupted, never panics):

- A series' label set is its schema's names paired positionally with its
  value ordinals. Schema name lists are sorted by name bytes, so the
  materialized pair order is canonical (ADR-0005). A schema's `name_ord`
  sequence MUST be derived from names already sorted by byte comparison,
  never by sorting ordinal values (LABEL_DICT order is relaxed, so the two
  no longer coincide).
- `run_total` MUST equal the sum of `run_count`. Every `run_count[i]` > 0.
- `value_kind[i]` fixes the kind for every run of series `i`: a histogram
  series' runs all have `val_page_len == 0` and non-zero `hist_page_len`; a
  scalar series' runs are the mirror image. Violating either is Corrupted.
- Timestamp bounds reconstruct per run: `min_ts_ns = footer.min_event_ts_ns
  + run_min_ts_delta`, `max_ts_ns = min_ts_ns + run_ts_span`; provenance:
  `created_unix_ns = footer.base_created_unix_ns + run_created_delta`. All
  deltas non-negative by construction, overflow-checked i64.
- Page locations reconstruct per run, independently for TS (11/12), VAL
  (13/14), HIST (15/16): `offset_0 = gap_0; offset_i = end_{i-1} + gap_i;
  end_i = offset_i + len_i`, overflow-checked, `end_i <=` the owning
  section's `len`. A series with N runs has N TS pages (and N VAL or N HIST
  pages), back to back in run order.
- Each block consumes exactly its `block_len` and holds exactly its element
  count (`count` for blocks 1-4, `run_total` for 5-16); the last ends exactly
  at SERIES_META's uncompressed end. Trailing bytes are Corrupted.
- Pre-allocation from any count/length is capped by remaining input size.

Readers that only need per-series identity parse blocks 1-4 and skip 5-16
wholesale via their `block_len` prefixes, at zero cost.

## Sparse catalog

At or above the sparse-emission threshold — `series_count >= 4096` — the
whole-section SERIES_META (kind 6) is replaced by SERIES_META_CHUNKS (kind 9)
and SERIES_IDX (kind 8) is added, so a point lookup on a large object fetches
a few KB rather than the whole catalog. The two ship together. The threshold
is a writer-side constant, not a reader contract: presence is signalled by
the sections themselves. The stride is `K = 512` for both the sparse-id index
and the meta-chunk grouping: every Kth id is indexed, and every K series form
one chunk.

Below the threshold a v5 object carries the kind-6 catalog verbatim and its
grammar is identical to the pre-v5 compaction layout byte for byte, so a
small L0 object pays nothing for being v5 beyond the trailer version field.

### SERIES_IDX (kind 8, uncompressed body)

```
version:       u8    (= 1; SERIES_IDX layout version, internal to the
                      section, distinct from the trailer version)
flags:         u8    (reserved, 0)
reserved:      u16   (0)
stride:        u32   (K = 512)
series_count:  u32
sparse_count:  u32
sparse_count sparse entries, each:
    id:            [u8; 16]   (indexed series id, strictly ascending)
    ids_offset:    u64        (byte offset within SERIES_IDS payload: 4 + index*16)
    window_len:    u64        (byte length of the id window this entry heads)
    window_crc32c: u32        (crc32c over SERIES_IDS[ids_offset .. +window_len])
chunk_stride:  u32   (K = 512)
chunk_count:   u32
chunk_count chunk-directory entries, each:
    frame_offset:            u64  (stored offset within SERIES_META_CHUNKS)
    frame_stored_len:        u64  (stored/compressed frame length)
    frame_uncompressed_len:  u64
    first_index:             u32  (absolute series index of the frame's first row)
    n:                       u32  (series in the frame)
    frame_crc32c:            u32  (crc32c over the stored frame bytes)
```

Entry `p` indexes series `p*stride` and heads the window covering ids
`[p*stride, (p+1)*stride)` (the last runs to the SERIES_IDS payload end).
Chunk `k` covers series `[first_index, first_index + n)`; the directory is
dense, `first_index` running `0, K, 2K, ...`. A reader rejects
`version != 1` (`UnsupportedSparseIndexVersion`) and a zero stride. Beyond
that, `parse_series_idx` fully validates the section's internal structure
before any lookup runs: `sparse_count`/`chunk_count` must match
`series_count`/stride, each entry's `ids_offset` and `window_len` must match
their formulas exactly, and the chunk directory's `first_index`/`n` must form
the dense `0, K, 2K, ...` chain with no gap or overlap. Any violation is
`BadSparseIndex`, so a corrupt-but-crc-consistent index fails to parse rather
than silently answering lookups as absent. SERIES_IDX is small, always
fetched whole, and covered by its ordinary `Section.crc32c`; the per-window
and per-chunk crc32c it carries verify the *other* sections' range-GETs.

### SERIES_META_CHUNKS (kind 9)

A header identical to SERIES_META's preamble (`count`, `schema_count`,
schemas, `run_total`) then `chunk_count` zstd frames concatenated; each
frame's stored range and crc32c live in the SERIES_IDX chunk directory, not
inline. The header carries the shared schema list, used only by whole-catalog
reassembly. Each frame decompresses to a self-contained run-major column set
for one chunk of `n` series:

```
n: u32
frame_run_total: u32
ts_base:   varint   (running TS_PAGES end before this frame's first run)
val_base:  varint   (running VAL_PAGES end before this frame's first run)
hist_base: varint   (running HIST_PAGES end before this frame's first run)
then the same block_len-prefixed columns as SERIES_META blocks 1-16, scoped
to this frame (schema_ref/value_ord over n series, the run columns over
frame_run_total runs)
```

The column meanings and reconstruction arithmetic are SERIES_META's, verbatim,
except the per-run page running sum starts from the frame's
`ts_base`/`val_base`/`hist_base` rather than 0. Those bases are the running
section `end` accumulated over every run before this frame's first, so the
reconstructed absolute offsets are identical to a whole-section reconstruction.
`run_created_delta` reconstructs against `footer.base_created_unix_ns` and
`run_min_ts_delta` against `footer.min_event_ts_ns`, as in the whole form. A
page-location reader skips the `schema_ref`/`value_ord` blocks (labels are not
needed for a by-id fetch) via their `block_len` prefixes.

The chunked form re-lays the identical raw delta/gap/len columns plus per-frame
bases, so the whole-catalog decode of a sparse object is bit-identical to the
whole-catalog decode of the same batch below the threshold, and a sparse
point-probe of any series is bit-identical to that series' slice of it.

## Page format (TS, VAL, HIST)

```
enc:  u8      encoding of the uncompressed payload
comp: u8      0=none, 1=lz4 (block format with u32 uncompressed-size prefix)
crc:  u32     crc32c over: series_id (16 bytes) || enc || comp || stored payload
payload
```

The crc includes the owning series_id as a prefix (ADR-0010 §4): a page read
through a mis-planned range or attributed to the wrong series fails closed,
and a flipped enc/comp byte cannot cause silent misdecoding.

| enc | payload |
|---|---|
| 1 TS_DELTA_VARINT | first ts_ns as varint-zigzag from 0; then deltas varint-zigzag. Handles irregular and out-of-order deltas. Accumulation is overflow-checked; each decoded ts must lie within the run's [min_ts_ns, max_ts_ns]. |
| 16 VAL_GORILLA | Gorilla XOR bit stream: first f64 raw 64 bits; then XOR with previous, classic control-bit scheme (Gorilla paper 4.1.2), padded to byte. Pure bit manipulation: NaN payloads, -0.0, denormals round-trip exactly. |
| 17 VAL_RAW_F64 | count * f64 LE. Fallback when XOR encodes larger than raw. |
| 32 HIST_SPANS | native-histogram records (below). |

Writers choose VAL encoding per page by encoded size (raw-fallback rule: emit
GORILLA unless size `>= 8*count` bytes). The raw-fallback rule means a
1-sample series always stores VAL as enc 17. HIST_SPANS pages always use
`comp = 0`. TS_PAGES is shared by scalar and histogram series; a histogram
series' `k`-th timestamp corresponds positionally to its `k`-th histogram
record. A page's record count is implicit: it equals the run's
`run_sample_count`.

Sample order within a page: ascending ts; ties keep insertion order, which
requires a STABLE sort in the writer. Duplicate timestamps are preserved at
L0. Cross-segment dedup order is in docs/catalog-and-mvcc.md; values compare
by f64 bit pattern everywhere.

### Native histogram value model and HIST_SPANS record

A native histogram sample is span-based (the superset of OTLP's contiguous
shape and Prometheus's sparse shape): `scale` (signed, `>= -53`; `-53` =
custom boundaries via `custom_values`), `zero_count`/`zero_threshold`,
`count`, optional `sum`, `reset_hint`, and per-side (positive, negative) lists
of `(offset, run of counts)` spans. Int/float duality: all counts are u64
absolutes or all f64 absolutes for one sample, never mixed. `min`/`max` and
exemplars are not carried.

```
one histogram record:
  flags:          u8
    bit 0   count_kind        0 = integer (u64), 1 = float (f64)
    bit 1   has_sum
    bits 2-3 reset_hint       0 UNKNOWN, 1 YES, 2 NO, 3 GAUGE
    bits 4-7 reserved (writer MUST emit 0, reader MUST reject nonzero)
  scale:          sint32 varint            (>= -53; -53 = custom boundaries)
  zero_threshold: f64 LE
  zero_count:     varint (int) | f64 LE (float)
  count:          varint (int) | f64 LE (float)
  sum:            f64 LE, present iff has_sum
  custom_values:  present iff scale == -53
    custom_values_count: varint
    custom_values_count * f64 LE     (strictly ascending boundaries)
  positive side:
    span_count: varint
    span_count * (offset: sint32 varint, length: varint)   length > 0
    bucket_counts: sum(length) entries, each varint (int) | f64 LE (float)
  negative side: identical grammar
```

Validation, all violations Corrupted: `scale < -53`; `custom_values`
present-iff-`scale == -53` and strictly ascending; every span `length > 0`;
`count >= zero_count` and `>= sum(all bucket_counts)` (checked in the count's
kind; NaN/Inf float sums pass through unchanged); exactly `run_sample_count`
records ending exactly at the page payload end; nonzero reserved flag bits.

### VAL_RAW_F64 alignment

The VAL_PAGES section `offset` is congruent to 0 mod 8, and every VAL_RAW_F64
(enc 17) page's payload start (page offset + 6) is congruent to 0 mod 8
relative to the section start, so raw f64 payloads are eligible for aligned
zero-copy views (docs/adrs/0013). The writer inserts `0x00` pad before such a
page's header and records it in that run's `val_page_gap`. TS and HIST pages
are never aligned (varint/field-decoded, never viewed directly).

## Checksums

Every byte a reader interprets is checksum-verified before use (ADR-0010 §4).

- Whole-object blake3 is computed by the writer and recorded in the commit
  record; the first 16 hex chars are embedded in the data key.
- `footer_crc32c` guards footer + trailer (including the version byte).
- `Section.crc32c` guards each section's full stored bytes, verified before
  any of its content is decoded.
- Because a whole-section crc cannot check a range-GET, the sparse partial
  fetches carry their own crc32c: the id-window and meta-chunk-frame crc32c
  both live in SERIES_IDX (itself always fetched whole and section-crc
  covered). A whole SERIES_META_CHUNKS read verifies the section crc; a
  single-frame read verifies the frame's own crc32c from the directory.
- The page crc guards series binding, enc, comp, and payload, checked on
  first touch. Pages copied verbatim by the compactor keep their original
  per-page crc (a verbatim copy alters none of `series_id || enc || comp ||
  payload`).

## Sparse reader protocol

A selective read that already knows a target series id:

1. Suffix-probe and parse the footer (reader-protocol steps 1-4). If
   SERIES_IDX (kind 8) is absent, this is a below-threshold object: use the
   whole-catalog path against kind 6.
2. Fetch SERIES_IDX whole (its `Section.crc32c` verifies it) and parse it.
3. Binary-search the sparse ids for the window that must contain the target;
   a target below the smallest indexed id is absent.
4. Range-GET the SERIES_IDS window and verify it against the entry's
   `window_crc32c` before trusting it.
5. Binary-search the fetched window for the target id, yielding its absolute
   series index (or absent).
6. Look up the covering chunk, range-GET its stored frame, verify it against
   the chunk's `frame_crc32c`, then decompress.
7. Decode the target row's runs from the frame — each run's provenance,
   bounds, and TS/VAL/HIST page ranges — and range-GET those pages (per-page
   crc unchanged).

Composition with the metric index (ADR-0020): postings prune which segments a
query reads; SERIES_IDX prunes what a query reads within one segment.

## Size targets

L0: 8 to 64 MiB nominal (adaptive batching); tests use tiny objects. The
suffix-fetch default (64 KiB) covers footers for expected L0 series counts
(the catalog lives in sections, not the footer, so the footer stays small
even at high cardinality).

## Writer edge rules

Zero-sample series are dropped (a page cannot encode zero values); at run
granularity, a run with `sample_count == 0` is dropped and a series left with
no runs is dropped entirely. Duplicate series ids across the input batch are a
writer error. An empty segment records `min_event_ts_ns = max_event_ts_ns =
base_created_unix_ns = 0`.
