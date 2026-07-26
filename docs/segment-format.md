# RSEG v1: Ravel Segment Format (metrics)

Persistent contract. Any change bumps the version. Parsers treat every offset,
length, and count as untrusted input: bounds-check everything, fuzz all
decoders. No `unsafe`.

All integers little-endian. "varint" means protobuf-style LEB128; signed
values use zigzag.

## Object layout

```
+--------------------------------------------+
| page data (concatenated pages, see below)  |
| footer: FooterProto bytes                  |
| trailer (16 bytes):                        |
|   footer_len:   u32                        |
|   footer_crc32c:u32   (over FooterProto)   |
|   version:      u16   (= 1)                |
|   signal:       u8    (1 = metrics)        |
|   reserved:     u8    (= 0)                |
|   magic:        [u8;4] = "RSG1"            |
+--------------------------------------------+
```

Reader: suffix-GET 64 KiB (or whole object if smaller). Verify magic,
version, signal. If 16 + footer_len exceeds the suffix, issue one more ranged
GET. Verify footer_crc32c before decoding FooterProto.

## FooterProto

Defined in `proto/ravel/segment.proto` (`ravel.segment.v1.Footer`). Fields:

- `tenant_hash` (16 bytes), `shard`, `writer_id`, `writer_epoch`, `writer_seq`
- `min_event_ts_ns`, `max_event_ts_ns`, `min_ingest_ts_ns`, `max_ingest_ts_ns`
- `sample_count`, `series_count`
- `sections`: repeated Section { `kind`, `offset`, `len`, `crc32c`,
  `comp` (0=none, 1=lz4, 2=zstd), `uncompressed_len` }
- unknown section kinds MUST be skipped by readers (forward compatibility)

Section kinds v1:

| kind | name | content |
|---|---|---|
| 1 | LABEL_DICT | string dictionary |
| 2 | SERIES_TABLE | per-series metadata |
| 3 | TS_PAGES | timestamp pages container |
| 4 | VAL_PAGES | value pages container |

LABEL_DICT and SERIES_TABLE are compressed as whole sections (zstd level 3
default). TS_PAGES / VAL_PAGES containers are NOT compressed as a unit; the
pages inside are individually compressed so one series is readable alone.
Section crc32c covers the stored (compressed) bytes.

## LABEL_DICT (uncompressed form)

```
count: u32
count strings: len:varint bytes (UTF-8)
```
Ordinal = position. Ordinal 0 is always the metric name string "__name__".

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

Entries sorted by series_id bytes. One page pair per series in v1 (L0 objects
are small; multi-page chunking is a v2 concern and the format leaves room:
readers locate pages only through these offsets).

## Page format (TS and VAL)

```
enc:  u8      encoding of the uncompressed payload
comp: u8      0=none, 1=lz4 (block format with u32 uncompressed-size prefix)
crc:  u32     crc32c over the stored payload bytes (after compression)
payload
```

Encodings:

| enc | payload |
|---|---|
| 1 TS_DELTA_VARINT | first ts_ns as varint-zigzag from 0; then deltas varint-zigzag. Handles irregular and out-of-order deltas. |
| 16 VAL_GORILLA | Gorilla XOR bit stream: first f64 raw 64 bits; then XOR with previous, classic (11,52... control-bit scheme per the Gorilla paper §4.1.2), padded to byte with zero bits. |
| 17 VAL_RAW_F64 | count * f64 LE. Fallback when XOR encodes larger than raw. |

Writers choose VAL encoding per page by encoded size (raw fallback rule:
emit GORILLA unless size >= 8*count bytes). Record whichever was used.

Sample order within a page: ascending (ts, insertion order for equal ts).
Writers sort per series before encoding. Duplicate timestamps are preserved
at L0 (dedup is a compaction/query concern; PromQL evaluation takes the last
value for a timestamp).

## Checksums

- Whole-object blake3 hash is computed by the writer and recorded in the
  commit record, not inside the object.
- footer_crc32c guards the footer; Section.crc32c guards each section;
  page crc guards each page. A reader verifies exactly the bytes it touches.

## Size targets

L0: 8 to 64 MiB nominal (adaptive batching); tests use tiny objects.
Suffix fetch default 64 KiB covers footers for expected L0 series counts
(footer size is ~40 bytes per section entry + fixed fields; SERIES_TABLE
lives in a section, not the footer, so footer stays small even at high
cardinality).
