# RSEG v1: Ravel Segment Format (metrics)

Persistent contract (amended by ADR-0010 before first ship). Any change
bumps the version. Parsers treat every offset, length, and count as
untrusted input: bounds-check everything, fuzz all decoders. No `unsafe`.

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
