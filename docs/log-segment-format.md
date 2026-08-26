# RLOG: Ravel Log Segment Format

Persistent contract (ADR-0029). Any change bumps the trailer version. The
current trailer version is 4 (ADR-0699 moved BLOCKS to row groups with
column-major page placement and added PAGE_DIR). Version 3 is **readable and
not written**: it is the N-1 half of the supported window, described in its own
subsection under "BLOCKS" below. Version 2 added the footer's
compaction-identity fields and version 1 was the format-only initial release;
no reader accepts either any longer.

Version 4 changes the BLOCKS layout, deletes the per-block header, and
redefines the SKIP_IDX level-0 block crc, so it is a versioned change rather
than the additive kind ADR-0029's carve-out excepts -- whatever PAGE_DIR's own
kind number would imply on its own. Which layout an object uses is determined
by its trailer version, and mirrored by whether it carries a PAGE_DIR section.

Version 3 changed no byte layout: it changed which value a `NumStat` bounds
(see "SKIP_IDX" below). Nothing in the bytes distinguishes a v2 stat from a v3
one, which is exactly why the trailer version moved rather than a section-local
one. v2 read and write support was deleted in the same change that introduced
v3.

**Version lifecycle and migration (ADR-0066, normative).** RLOG is a Class A
bulk data-object format. The supported-version window is N/N-1 (single-sourced
as `ravel_logseg::footer::SUPPORTED_VERSIONS`; the writer, reader gate,
`audit-versions`, `migrate`, and the compactor's output-version constant all
read it), rolled out readers-before-writers: a release writing N+1
requires a fleet already reading N+1. RLOG compaction already decodes every
input's records and re-encodes them from scratch, so an old-version object is
migrated forward by the normal compaction and `maintain migrate` paths with no
special page-copy carve-out; retention also ages old objects out. The migrate
job verifies and raises the per-(tenant, signal) format floor, and N-1 read
support is deleted only once every bucket's floor is >= N, citing those floors.

The v3 → v4 bump (ADR-0699 decision 3) is the first RLOG bump to ship a dual
reader, so the window is two versions wide: `{3, 4}`. Both paths can therefore
migrate forward -- compaction always emits version 4, so a compacted tenant
converges in one pass, and `maintain migrate` rewrites the rest.
`audit-versions` reports the mix. A tenant that is never compacted keeps paying
version-3 read costs (whole blocks on the wire) until it is.

Parsers treat every offset, length, count, and tag read from stored bytes
as untrusted input: bounds-check everything, overflow-check every
accumulation, fuzz all decoders. No `unsafe`. Every violation is a typed
`Corrupted` error, never a panic and never wrong data.

RLOG is a sibling of RSEG (docs/segment-format.md), not an amendment: it
copies RSEG's conventions (16-byte trailer, protobuf footer, crc32c
discipline, suffix-GET reader protocol) and shares none of its bytes. All
integers are little-endian. "varint" means protobuf-style LEB128;
"ivarint" means a signed value zigzag-mapped then LEB128-encoded. A
canonical LEB128 varint is at most 10 bytes and readers reject overlong
encodings.

## Object layout

```
+---------------------------------------------------+
| STREAM_DIR   stream directory                     |  kind 1
| FIELD_DIR    field directory                      |  kind 2
| BLOCKS       row blocks (column pages)            |  kind 3
| SKIP_IDX     multi-level min/max index            |  kind 4
| PAGE_DIR     per row group, per column chunk,     |  kind 8
|              per page: offset/len/enc/comp/crc    |
| BLOOM        per-block token blooms               |  kind 5
| POSTINGS     per-field term -> block postings     |  kind 6 (optional)
| footer: LogFooter protobuf bytes                  |
| trailer (16 bytes):                               |
|   footer_len:   u32                               |
|   footer_crc32c:u32                               |
|   version:      u16   (= 4)                       |
|   signal:       u8    (2 = logs)                  |
|   reserved:     u8    (= 0)                       |
|   magic:        [u8;4] = "RLG1"                   |
+---------------------------------------------------+
```

Writers emit the sections physically in the order shown above (PAGE_DIR
between SKIP_IDX and BLOOM, not in kind order); readers rely only on the
footer's section offsets, never on adjacency or on kind order. Bytes between
sections are permitted and MUST be `0x00`; readers never interpret them.

`footer_crc32c` is computed over: the `LogFooter` bytes, then `footer_len`
(u32 LE), `version` (u16 LE), `signal`, `reserved`, `magic`. Every
trailer byte except the crc field itself is covered (ADR-0010 §4).

## Reader protocol

Identical in shape to RSEG v1:

1. Reject objects smaller than 16 bytes as `Corrupted`.
2. Suffix-GET the object's tail (or the whole object if smaller). Verify
   `magic`, `version`, `signal`, `reserved`. The suffix length is a reader
   choice, not a format constant: it should cover the footer and whatever tail
   sections the reader is about to need, and a reader that guesses short
   simply issues step 3's extra ranged GET. `ravel-query`'s log fetcher probes
   256 KiB, sized so that one probe carries the footer, SKIP_IDX and PAGE_DIR
   past the BLOOM section that sits between them (ADR-0699 decision 5, issue
   #766).
3. Require `footer_len > 0` and `16 + footer_len <= total_size`;
   otherwise `Corrupted`. If the suffix did not cover the footer, issue
   one more ranged GET.
4. Verify `footer_crc32c` (over the bytes defined above) before decoding
   the footer.

## LogFooter

Defined in `proto/ravel/logseg.proto` (`ravel.logseg.v1.LogFooter`).
Field numbers are frozen; only additive changes with new field numbers
are permitted.

- identity: `tenant_hash` (16 bytes), `shard`, `writer_id` (16 bytes),
  `writer_epoch`, `writer_seq`. These MUST match the commit record the
  reader resolved the object from (ADR-0010 §7).
- summary (skip index level 2): `min_ts_ns`, `max_ts_ns`,
  `min_observed_ts_ns`, `max_observed_ts_ns`, `record_count`,
  `block_count`, `stream_count`.
- `sections`: repeated `Section { kind, offset, len, crc32c, comp
  (0=none, 2=zstd), uncompressed_len }`.
- compaction identity (ADR-0032, field numbers 14-16, added in trailer
  version 2): `level` (uint32, 0 = L0 flush object, 1 = L1 compacted
  part), `input_set_hash` (bytes, over the sorted input list, same
  canonical convention as RSEG), `part_index` (uint32, part ordinal
  within one compaction output). RLOG carries no
  `base_created_unix_ns`-equivalent: unlike RSEG it has no cross-writer
  record dedup that needs a recovered per-run creation time (retry
  duplicates are structurally impossible at the RLOG write path).
- unknown section kinds MUST be skipped by readers (forward
  compatibility).

An **L0 flush object** (one object per writer flush, emitted by the
ingest log shard via `RlogWriter::finish`) stamps the compaction-identity
fields at their sentinels explicitly: `level = 0`, `input_set_hash`
empty, `part_index = 0`. An **L1 compacted part** is emitted by the
compactor (`ravel-maintain`) via `RlogWriter::finish_compacted`, which
runs the identical section-building pipeline as `finish` and stamps
`level = 1`, the compaction's `input_set_hash`, and the part's
`part_index` (see "Compaction (L0 → L1)" below). These three fields live
inside the protobuf-encoded `LogFooter` and are therefore already covered
end to end by `footer_crc32c` (verified before the footer is decoded);
they need no separate checksum.

Validation (all violations `Corrupted`, never panics):

- At most one section per known kind. All five v1 kinds
  (STREAM_DIR, FIELD_DIR, BLOCKS, SKIP_IDX, BLOOM) are mandatory. PAGE_DIR
  (kind 8, ADR-0699) is mandatory in version 4 and absent in version 3.
  POSTINGS (kind 6, ADR-0049) is optional: present only when the writer was
  given one or more indexed field names (`RlogWriter::with_indexed_fields`);
  absence is legal and never treated as corruption.
- Every section `[offset, offset+len)` lies within
  `[0, total_size - 16 - footer_len)`, with overflow-checked arithmetic.
- `uncompressed_len` is capped by config (default 1 GiB per section) and
  the decompressed length must equal it exactly.

### Section kinds

| kind | name | content | comp |
|---|---|---|---|
| 1 | STREAM_DIR | stream_id -> canonical resource+scope blob, block range | zstd |
| 2 | FIELD_DIR | field name+type -> column id, stats | zstd |
| 3 | BLOCKS | row blocks, per-column pages | none (per-page) |
| 4 | SKIP_IDX | skip index levels 0 and 1 | zstd |
| 5 | BLOOM | per-block token bloom filters | none (per-entry) |
| 6 | POSTINGS | per-field term -> block-index postings (optional) | none (per-block, zstd inside) |
| 7 | GRAM_IDX | reserved (ADR-0105, not implemented) | - |
| 8 | PAGE_DIR | per row group, per column chunk, per page: offset, length, enc/comp, crc32c | zstd |

STREAM_DIR, FIELD_DIR, and SKIP_IDX are compressed as whole sections
(zstd level 3 default) and always read whole. BLOCKS, BLOOM, and
POSTINGS are containers: not compressed as a unit, their entries
individually addressable so one block, one bloom entry, or one term
block is readable alone.

## STREAM_DIR (uncompressed form)

```
count: u32
count entries, sorted ascending by stream_id (16 bytes):
  stream_id:  [u8;16]
  blob_len:   varint
  blob:       blob_len bytes, the stream-identity preimage (below)
  first_blk:  varint   } block range containing
  last_blk:   varint   } this stream's records
```

The `stream_ref` used everywhere else is the entry's ordinal (0-based).
Sorting by `stream_id` makes lookup a binary search and makes the
directory mergeable at compaction with a linear pass. Readers reject a
non-ascending sequence and an entry count exceeding the configured cap.

### The identity blob

`blob` is exactly the byte string that `stream_id` is the hash of, minus
the domain string, in this concatenation order:

```
canonical(resource_attrs)         self-delimiting: leading entry count
varint(len(scope_name))    scope_name     UTF-8, no terminator
varint(len(scope_version)) scope_version  UTF-8, no terminator
canonical(scope_attrs)            self-delimiting: leading entry count
```

`canonical(..)` is the frozen canonical attribute-set encoding of
ADR-0029 (`ravel_types::logstream::canonical_attr_bytes`): a LEB128 entry
count, then each entry as `len(key) key encode_value(value)`, entries
sorted by `(key bytes, encoded value bytes)`. `varint(..)` is LEB128.
Both attribute sets carry their own entry count and both scope strings
are length-prefixed, so the concatenation is injective: no two distinct
resource+scope inputs produce the same blob.

It follows that

```
stream_id == blake3("ravel-logstream-v1" || blob)[..16]
```

for the recipe in ADR-0029, so a reader can verify a STREAM_DIR entry's
identity from the object alone, and log stream identity is recoverable
without a side table. The blob is never empty: an empty resource+scope
still encodes as four zero bytes (two zero entry counts, two zero string
lengths).

Writers build the blob with `ravel_logseg::stream_attrs_bytes` and hand
it to the writer in every record's `stream_attrs` field; the writer
stores the bytes from the first record seen for each `stream_id`
verbatim. Every record sharing a `stream_id` must carry identical
`stream_attrs`. A disagreement means either a caller bug or a stream-id
hash collision, and neither has a truthful blob, so the writer rejects
the whole object with `LogSegError::InconsistentStreamAttrs` rather than
silently keeping one of them. That is writer input validation, not object
corruption.

## FIELD_DIR (uncompressed form)

```
count: u32
count entries, sorted ascending by (name bytes, type):
  name_len:       varint
  name:           name_len UTF-8 bytes
  type:           u8   (1=str 2=i64 3=f64 4=bool 5=bytes)
  column_id:      varint
  present_blocks: varint    total blocks with >= 1 value for this field
  null_count:     varint    object-wide null count for this field
```

Fixed columns occupy reserved column ids 0..=9 (`ts`, `observed_ts`,
`stream_ref`, `severity_num`, `severity_text`, `body`, `trace_id`,
`span_id`, `flags`, `attrs_raw`) and never appear in FIELD_DIR. Dynamic
attribute columns start at column id 10. A key observed with two value
types yields two entries (per-type splitting). At most 1000 dynamic
columns per object; overflow keys are encoded into the `attrs_raw` fixed
column (canonical bytes, column id 9), which is scan-queryable but not
columnar and never pruned by field predicates. Readers reject a
non-ascending sequence, an entry count over the cap, an unknown type
byte, and truncation.

A FIELD_DIR column normally exists because at least one record carries the
key as a per-record attribute. Two kinds of key that appear only at stream
level (a resource or scope attribute) across the whole object, and per-record
on no record, get a `(name, type)` column anyway:

- An **indexed** field (POSTINGS, below), if the writer was told to index the
  name (`with_indexed_fields`), so its merged-view postings have a `column_id`
  to key by (ADR-0049).
- A **numeric** field (i64, f64, bool), so its SKIP_IDX numeric stat has a
  `column_id` to key by (ADR-0095). String and bytes keys get no such column,
  since they feed no stat.

Such a column is a POSTINGS or stat key, not a materialized value: no
row writes a per-record value to it, so it is all-null in every block, it gets
no page in any block, its `present_blocks` is 0, and the reader's exact
per-record equality on the key still reads only the per-record layer (it never
resolves against the resource or scope blob). It counts against the same 1000-
dynamic-column budget as any column; one that cannot fit degrades to bloom plus
exact scan and to no stat, always legal.

## BLOCKS

Records are sorted `(stream_ref ascending, ts_ns ascending)`. Target 8192
records per block, cap 8 MiB uncompressed. Per block, one page per column
that has at least one value in the block.

A **block** is a logical unit, not a byte range. It is what SKIP_IDX level 0,
BLOOM, and POSTINGS are keyed by, and nothing about its size or its pruning
granularity changed in version 4.

A **row group** is a run of `group_target_blocks` consecutive blocks (writer
configuration, default 32, so 262,144 records at the block target). BLOCKS is
the object's row groups laid end to end in block order. Within a row group the
pages are stored **grouped by column, then by block**: all of one column's
pages for that group's blocks are contiguous (a **column chunk**), and the
column chunks of one row group follow each other in ascending `column_id`
order. The last row group of an object is short; an object with fewer blocks
than `group_target_blocks` has exactly one row group, and its layout is the
same, so a small flush object pays nothing for the level (ADR-0699 decision 1).

```
BLOCKS (version 4):
  row group 0 (blocks 0..31):
    chunk column 0:   page(b0) page(b1) ... page(b31)
    chunk column 1:   page(b0) page(b1) ... page(b31)
    ...
    chunk column 104: page(b0) page(b1) ... page(b31)
  row group 1 (blocks 32..63):
    ...
```

There is no block header. Every page's `column_id`, `enc`, `comp`, `len`, and
`uncomp_len` -- the fields a version-3 block header carried inline -- live in
PAGE_DIR, together with the page's offset and its own crc32c. A page a query
does not want is therefore never read at all, rather than fetched and walked
past.

A column with values in only some rows of a block carries a presence
bitmap page (a bitmap-encoded page, `enc = 8`) immediately before the value
page, so decode restores exact row alignment. Both pages sit in the same column
chunk, in that order, so one block contributes one or two pages to one chunk.
A column absent from every row of a block is all-null and occupies zero bytes,
and contributes no page to the chunk.

The block's crc32c is stored in its SKIP_IDX level-0 entry, not inline, and
covers the concatenation of the block's pages in ascending `column_id` order --
which is what a whole-block read assembles once it has located them through
PAGE_DIR, and is the order PAGE_DIR lists them in. A whole-block reader
verifies it; a reader taking a subset of the columns cannot (it does not have
the other pages) and verifies each page's own crc32c instead. Both are
mandatory on their own access path, which is what keeps every interpreted byte
checksum-covered (ADR-0010 §4, "Checksum coverage map" below). This is what
lets a fetcher bring one coalesced byte range per `(row group, projected
column)` and hand it to a projected decode: the range's holes -- pages of
pruned blocks, and of columns the projection dropped -- are never interpreted,
so nothing depends on a checksum over them.

A reader may decode a *subset* of a block's columns (`read_block_columns`,
ADR-0087); the SQL logs scan uses this to decode only the columns a query
references. Under version 4 the subset is also a *fetch* subset: PAGE_DIR gives
each column chunk's byte extent, so a projection of `k` columns over a row
group is `k` contiguous ranges instead of one range per block. A skipped column
is indistinguishable from an absent one in the decoded result, so a reader that
projects is responsible for having asked for every column it goes on to read.

`block_offset` and `block_len` in a version-4 SKIP_IDX level-0 entry describe
the block's *page span*: from its first page's offset to the end of its last.
Because the pages are column-major, that span overlaps its neighbours' and
covers most of the row group. It is a superset range containing every one of
the block's pages, not the block's exact extent, and nothing locates a
version-4 page through it.

```
   one row block, columnar (unchanged by version 4; only page placement moved):

   rows ->   r0 r1 r2 r3 r4 ... r8191
   ts        [ delta / double-delta page       ]
   stream    [ RLE page: (ref,run)(ref,run) ...]
   sev_num   [ bit-packed FOR page             ]
   body      [ string plain: lengths | blob    ]
   attr.k    [ presence bitmap | FOR page      ]
```

### Version 3 (readable, not written)

A version-3 object has no PAGE_DIR and no row groups. Each block is one
contiguous byte range at its SKIP_IDX level-0 `block_offset`, of exactly
`block_len` bytes, and consecutive blocks follow each other:

```
block (version 3):
  header:
    record_count: varint
    page_count:   varint
    page_count page descriptors:
      column_id:  varint
      enc:        u8      encoding tag (see registry)
      comp:       u8      0=none, 2=zstd
      len:        varint  stored page bytes
      uncomp_len: varint  page bytes before compression
  pages: page_count pages concatenated, in descriptor order,
         each `len` bytes
```

The pages are in descriptor order, which is the block encoder's staging order:
the fixed columns in field order (so `flags`, column 8, precedes
`severity_text`, column 4), then the dynamic columns ascending. The block's
crc32c covers the complete block bytes, header and all pages, and the reader
verifies it before decoding anything in the block. A column-subset decode is a
read choice and not a format variant: the crc32c is still verified over the
complete block bytes, every page descriptor is still parsed, and every page's
stored extent is still walked, so a truncated or over-long block is rejected
exactly as it is under a whole-block decode.

The version-3 reader is the N-1 half of the supported-version window and is
deleted only by its own reviewed change, once every bucket's recorded format
floor is at 4 (ADR-0066 decision 1, ADR-0699 decision 3).

## PAGE_DIR (uncompressed form)

Mandatory in version 4, absent in version 3. Compressed as a whole section and
covered by its `Section.crc32c`, like SKIP_IDX: it is read whole on every open,
because without it no page can be located at all. A corrupt or undecodable
PAGE_DIR is a loud `Corrupted` error and never a degrade.

```
group_count: varint
group_count groups:
  first_block: varint      index of the group's first block, whole-object
  block_count: varint
  chunk_count: varint
  chunk_count chunks:
    column_id:   varint
    offset:      varint    absolute offset of the chunk's first page, into BLOCKS
    page_count:  varint
    page_count pages:
      block:       varint  block index within the group, ascending
      enc:         u8      encoding tag (see registry)
      comp:        u8      0=none, 2=zstd
      len:         varint  stored page bytes
      uncomp_len:  varint  page bytes before compression
      crc32c:      u32     over the page's stored bytes
```

Page offsets are derived, not stored: a chunk's pages are contiguous from the
chunk's `offset` in listed order, so the *n*th page begins at `offset` plus the
`len`s of the pages before it. That makes the section about 14 bytes per page:
a few KB for a two-block flush object, about 1.5 MB for a 10M-row L1 part with
105 columns, read once per object per query.

Two entries may name the same `block` within one chunk: a partially present
column contributes its presence bitmap page and then its value page, so
`page_count` is the number of *pages* the group's blocks carry for the column,
between one and two per block that carries it -- not the number of blocks that
carry it.

Groups partition the object's blocks into consecutive runs starting at block 0:
the first group's `first_block` is 0 and each subsequent group's continues
where the previous ended. The whole directory's `block_count` total equals the
SKIP_IDX level-0 entry count.

## Encodings (tag registry)

| tag | encoding | applies to |
|---|---|---|
| 1 | plain | any (fallback) |
| 2 | constant | one value repeated `count` times |
| 3 | RLE | runs of (value, run length) |
| 4 | delta-zigzag | i64, timestamps |
| 5 | double-delta | near-monotonic i64 runs |
| 6 | FOR bit-pack | i64 frame-of-reference |
| 7 | dictionary | strings, f64 bits, fixed-width |
| 8 | bitmap | bool columns, presence bitmaps |
| 9 | fixed-width | trace_id (16B), span_id (8B) |

The writer picks per page by measured encoded size, biased toward
`constant` then `RLE` on ties (they also decode fastest). An unknown
`enc` byte is a typed `Corrupted` error, never a panic or a guess. All
codecs are self-terminating against the caller-supplied element `count`
and MUST consume exactly the bytes handed to them; trailing or missing
bytes are `Corrupted`. Every decode allocation is bounded by `count`
(itself bounded by the block record cap) and by the remaining input.

f64 values travel as their `u64` bit pattern (`f64::to_bits`) everywhere;
NaN payloads and -0.0 are significant and survive round-trips exactly.
Comparisons are on the bit pattern, never `==`.

### Integer codec layouts (i64)

- **plain (1):** `count` values, each an ivarint.
- **constant (2):** one ivarint, the shared value. Applies only when all
  `count` values are equal (`count >= 1`).
- **RLE (3):** `run_count` varint, then `run_count` pairs
  `(value ivarint, run_len varint)`. The run lengths sum to `count`;
  each `run_len >= 1`.
- **delta-zigzag (4):** first value ivarint, then `count - 1` deltas,
  each `delta[i] = v[i] - v[i-1]` as an ivarint. Decode accumulates with
  overflow-checked addition; overflow is `Corrupted`. Emitted only when
  every delta is representable in i64 (checked at encode).
- **double-delta (5):** first value ivarint (`count >= 1`), then first
  delta ivarint (`count >= 2`), then `count - 2` deltas-of-deltas, each
  `dod[i] = (v[i]-v[i-1]) - (v[i-1]-v[i-2])` as an ivarint. Decode is
  overflow-checked; overflow is `Corrupted`. Emitted only when every
  intermediate is representable in i64.
- **FOR bit-pack (6):** `min` ivarint, `bit_width` u8 (`0..=64`), then
  `count` frame-of-reference values `(v - min)` bit-packed LSB-first as
  unsigned. `bit_width = 0` means every value equals `min` and no packed
  bytes follow. The offset `v - min` and the reconstruction
  `min + offset` use wrapping two's-complement arithmetic and are exact
  for the encoded range (`min` is the true minimum). Packed byte length
  is `ceil(count * bit_width / 8)`; a `bit_width > 64` or a packed length
  that disagrees with the payload is `Corrupted`.

### Bitmap codec layout

- **bitmap (8):** `count` bits packed LSB-first (bit `i` of the value
  sequence is bit `i % 8` of byte `i / 8`), zero-padded to a whole
  number of bytes. Payload length is exactly `ceil(count / 8)`.

### String codec layouts

`encode_strings` chooses dictionary when `distinct / total <= 0.5`, else
plain.

- **plain (1):** `count` lengths, each a varint, then the values
  concatenated as one blob. Decode reads the `count` lengths, then slices
  the blob; the lengths MUST sum to exactly the remaining blob length.
- **dictionary (7):** `dict_count` varint, then `dict_count` entries
  each `(len varint, bytes)` sorted ascending by byte value, then the
  `count` ids as a FOR bit-pack body (`bit_width` u8, then packed ids
  LSB-first) selecting into the dictionary. An id `>= dict_count` is
  `Corrupted`; a blob shorter than the offsets claim is `Corrupted`.

### f64-bits codec layouts

Callers pass values as `u64` (`f64::to_bits`). `encode_f64` tries
dictionary, else plain; constant when all equal.

- **plain (1):** `count` values, each 8 bytes LE.
- **constant (2):** one value, 8 bytes LE (`count >= 1`, all equal).
- **dictionary (7):** `dict_count` varint, then `dict_count` values each
  8 bytes LE sorted ascending by `u64` value, then the `count` ids as a
  FOR bit-pack body, as for strings.

### Fixed-width codec layouts

`encode_fixed` takes a `width`; every value is exactly `width` bytes.

- **fixed-width (9):** the `count` values concatenated raw; payload
  length is exactly `count * width`.
- **dictionary (7):** `dict_count` varint, then `dict_count` entries of
  `width` bytes each (no per-entry length) sorted ascending, then the
  `count` ids as a FOR bit-pack body.

### Page compression envelope

Each page's stored bytes are its encoded codec bytes, optionally zstd
compressed. The writer compresses a page only when its encoded length is
at least the 512-byte floor and zstd is strictly smaller; below the floor
zstd overhead exceeds the win and the page stays raw. The descriptor
records `comp` (0=none, 2=zstd), `len` (stored bytes), and `uncomp_len`
(encoded bytes before compression).

Readers reject an unknown `comp` tag, an `uncomp_len` above the config cap
(default 64 MiB per page) before allocating, and a decompressed length
that does not equal `uncomp_len`. Decompression allocates exactly
`uncomp_len` bytes and fails closed if the payload expands past it (zstd
bomb guard).

## SKIP_IDX (uncompressed form)

Skip-list shape: the coarse level is probed first, and the reader
descends only into survivors, so pruning cost scales with surviving data.

```
 level 2 (footer):    whole-object min/max ts, stream/record/block counts
                                    |
 level 1 (per 64      [ g0 ][ g1 ][ g2 ] ...   merged min/max ts,
 blocks):             min/max stream_ref, per-column min/max
                                    |
 level 0 (per block): [b0][b1] ... [bN]   min/max ts, min/max stream_ref,
                      record_count, block byte range and crc, per numeric
                      column min/max and null_count
```

```
count0: u32
count0 level-0 entries:
  block_offset:   varint   (into BLOCKS; version 4: the block's page span,
                           see "BLOCKS")
  block_len:      varint
  block_crc32c:   u32      crc32c over the block's pages concatenated in
                           ascending column_id order (version 4) or over the
                           block's complete stored bytes (version 3)
  record_count:   varint
  min_ts, max_ts: ivarint
  min_stream_ref, max_stream_ref: varint
  stat_count:     varint
  stat_count numeric stats:
    column_id: varint
    ty:        u8   (2=i64 3=f64 4=bool 5=bytes)
    min_bits:  8 bytes LE
    max_bits:  8 bytes LE
    null_count:varint
    has_nan:   u8   (0 or 1)
count1: u32
count1 level-1 entries: same fields as a level-0 entry with the byte
  range and crc omitted, merged over the entry's <= 64 children:
  mins/maxes by the per-type order, has_nan OR-ed, null_count as
  defined in "null_count at both levels" below
```

A level-1 stat merges the min/max bounds of only the children that carry a stat
for that column, so its bounds are the group's bounds only because every child
that resolves a value for the column carries one (see "What a numeric stat
bounds" below). A child block that resolved values but carried no stat would
read as "no information" for the bounds and be dropped from the merge silently,
leaving a level-1 entry that looks complete and bounds a subset of its group.
`null_count`, unlike the bounds, does account for the children that carry no
stat: see "null_count at both levels" below.

Fanout is 64: one level-1 entry per 64 level-0 blocks. A numeric stat
stores i64 as its two's-complement `u64` bit pattern (`v as u64`), and
f64 as its `to_bits` pattern; min/max for f64 use `total_cmp` order over
non-NaN values, and NaN values are counted in `has_nan` and excluded from
min/max. Readers reject a block count over the configured cap and any
truncation.

### What a numeric stat bounds (trailer version 3, ADR-0095, normative)

A numeric stat for a `(name, type)` dynamic column bounds, over the rows of
its block, each row's **resolved merged-view value** for that column's
attribute name, and only when that value's type is the column's type. A row
whose resolved value for the name is of another type, or which does not resolve
the name at all, contributes to `null_count` and to neither bound.

The resolved value is the one a reader reports for the name, which is
`ravel_sql::rlog_attrs::find_attr` over `merged_attrs`: the record's stream
layer (its resource attributes, then its scope attributes) seeds the view and
the record layer overrides it, the record winning a collision. A name the
record itself does not carry keeps its resource- or scope-level value -- the
ordinary OTLP shape, where an attribute like `service.version` lives only on
the resource -- and a stat over that name has to bound that value like any
other.

Within the record layer, when a record carries the name more than once, the
winning occurrence is fixed by the order `rebuild_record` lays a record's
attributes out and `merged_attrs` folds them last-wins (see "FIELD_DIR" and
POSTINGS "Version"): the record's columnar occurrences ascending by FIELD_DIR
type byte, then its `attrs_raw` overflow occurrences ascending by canonical
encoded value bytes, last entry wins.

The writer resolves this once per record and both POSTINGS and SKIP_IDX are
projections of that one resolved view, so the two sections cannot disagree
about which value a reader sees for a key.

Three consequences a reader must expect, all by design:

- A block's value page for the column may hold values outside the stat's
  `[min, max]`: a losing occurrence is still stored (nothing is dropped), it
  just does not widen the bounds. Anything reading the page directly must
  resolve the value itself, exactly as `rebuild_record` plus `merged_attrs` do.
- Conversely, the stat's `[min, max]` may cover values that appear in no value
  page of the block at all: a row that resolves the name off its stream layer
  contributes its resource- or scope-level value, which is stored once in
  STREAM_DIR and not per row.
- A stat's `null_count` may exceed the FIELD_DIR `null_count` for the same
  column, which counts raw column presence over the whole object rather than
  merged-view resolution per row.

A block carries a stat for every column some row of it resolves a value for,
which is a superset of the columns it has value pages for: a block where the
name is resolved only off the stream layer, with no per-record occurrence in the
block at all, still carries the stat, and that stat still bounds the resolved
values. The block's pages are unaffected -- a column no row of the block wrote a
value to gets no page, so a stat-only column costs zero BLOCKS bytes.

For the same reason, a numeric name (i64, f64, bool) that no record in the
object carries per-record still takes a dynamic column, on the strength of its
stream-level occurrences alone, so its stat has a column to be keyed by. That
mirrors what an indexed name already gets (see "FIELD_DIR"); the column holds no
value page anywhere in the object, it exists to key the stat and the postings.

A name still ends up with no column, and so no stat, when it overflows the
writer's dynamic-column budget. An absent stat is "no information": it prunes
nothing, exactly like an absent posting list.

This is what makes range pruning on these stats sound: where a stat exists, a
reader materializing a declared typed column produces exactly the value the
stat folded in (or NULL when that value is of another type), so a block whose
stat range cannot overlap a queried range holds no matching row. Under version
2 the stats bounded the raw columnar occurrences instead, which could exclude
the block holding the record a range query wanted; that is the defect version 3
fixes, and it is why a v2 object cannot be read as a v3 one.

### null_count at both levels (trailer version 3, normative)

A stat's `null_count` counts rows that resolve nothing the stat's `[min, max]`
bounds. It never contributes to pruning (`candidate_blocks` reads only min/max,
and null rows never satisfy a range); it is a per-column cardinality hint for a
future reader, and this section is its only contract.

- **Level-0** (per block). `null_count` is the number of rows of the block whose
  resolved merged-view value for the column's name is absent or of a type other
  than the column's type, i.e. the block's `record_count` minus the count of
  rows that resolve a value of the column's type (the rows the bounds fold in).
  A block carries a stat for every column it plans, and it plans a numeric
  column when some row carries a record-level occurrence of that column, or
  some row's merged-view winner is of its type. The first condition does not
  require the second, so a present level-0 stat may have
  `null_count == record_count`: rows carried an occurrence of the column, but
  every merged-view winner for the name was of another type. That is the
  cross-type duplicate case (ADR-0095), and such a stat carries
  `min_bits == max_bits == 0`, bounds that bound nothing. A stat is absent only
  when neither condition holds, and then no row of the block resolves the
  column either (see "What a numeric stat bounds").

- **Level-1** (per <= 64 children). `null_count` is the number of rows *beneath
  the entry* -- summed across all children -- that resolve nothing of the
  column's type. This counts, for each child, the rows that child's own stat
  reports null, PLUS every row of any child that carries no stat for the column
  at all: by the level-0 rule a child with no stat for a column resolves nothing
  of that type in any of its rows, so all `record_count` of them are null. The
  writer computes it as `sum(child.stat.null_count for children with the stat) +
  sum(child.record_count for children without it)`. A level-1 stat exists iff
  some child carries one, and a child's stat may itself be all-null, so a
  level-1 stat may also have `null_count == record_count`. An all-null child
  stat folds its degenerate `0`/`0` bounds into the merged min/max as well,
  widening them; pruning stays sound, because a wider range only keeps more
  blocks, but it is less precise.

The rule a reader must honor at both levels: **neither the presence of a stat
nor its bounds is evidence that any row resolves the column.** A column no row
resolves has two encodings, and they carry different information even though
they state the same fact. The stat is absent when no row carries an occurrence
of the column and no row's winner is of its type. It is present with
`null_count == record_count` when rows carry an occurrence but every winner is
of another type. They differ in whether bounds accompany the fact: an absent
stat carries none and prunes nothing, exactly like an absent posting list,
while a present stat always carries bounds, which in this case are the
degenerate `0`/`0` pair. A reader must not read a present stat as "at least one
row resolves this column", must not read an absent stat as
`null_count == record_count`, and must not use `null_count` to prune.

`null_count` is a `u32` written as a varint, and the level-1 sum above can in
principle exceed `u32::MAX` (up to 64 children, each with a `u32` `record_count`).
The merge saturates: a level-1 `null_count` of `u32::MAX` means "at least
`u32::MAX` null rows beneath this entry", an exact value lost to saturation. A
reader that consumes the field must treat `u32::MAX` as a saturated lower bound,
not an exact count. Saturation is the only approximation in the field and is
confined to this case; every non-saturated value is exact. (The current writer's
block sizing keeps real `record_count` values far below `u32::MAX / 64`, so
saturation is unreachable in practice; it is documented because the format
contract, not the current writer, bounds `record_count`.)

`candidate_blocks(ts_min, ts_max, stream_refs, numeric)` returns the level-0
block indices whose entries survive the coarse predicate. Alongside the ts and
stream-ref bounds, `numeric` carries prune-only inclusive range arms, one per
NumStat-eligible column: a block is dropped when its stat's min/max for that
column proves no overlap with the queried range, and a whole level-1 group is
dropped the same way before its blocks are examined. A column with no stat in
the entry under test prunes nothing there (absence is "no information"), so a
level-1 group carrying no stat for the column is descended into and a level-0
block with none survives. These arms drive block pruning only, through
`RlogReader::scan_blocks`'s prune channel (ADR-0095 decision 6); the exact,
exactly-typed range is re-evaluated above the scan by the caller. The pruning is
sound (ADR-0013): a block is dropped only when its bounds prove no record in it
can match. Precision is not guaranteed; survivors are scanned and re-evaluated
exactly.

## BLOOM

One blocked bloom filter per row block. The BLOOM section is a container:

```
count: u32
count entries, entry i for block i:
  entry_len: varint
  crc32c:    u32    crc32c over the entry's stored bytes (below)
  entry:     entry_len bytes:
    m_bits:  varint   (power of two, >= 512)
    k:       u8       (hash count, > 0)
    seed:    u64 LE
    bits:    m_bits / 8 bytes
```

Inserted keys are hashed as
`h = blake3(seed_le(8) || column_id_le(4) || token)`, reading three 64-bit
values from disjoint bytes of the digest:
`block = u64::from_le_bytes(h[0..8])`,
`g1 = u64::from_le_bytes(h[8..16])`, and
`g2 = u64::from_le_bytes(h[16..24]) | 1`. The filter is blocked on 512-bit
blocks: `block % (m_bits / 512)` selects the block, and for `i in 0..k`
the bit at `(block % (m_bits/512)) * 512 + ((g1 + i*g2) % 512)` is set
(all additions and the multiply wrap). Block selection and the
within-block offsets read disjoint digest bytes so the first probe is not
congruent to the block index (which would collapse most set bits onto two
offsets and wreck the false-positive rate). `k = 7`, chosen for a ~1%
false-positive rate; `m_bits = next_pow2(max(512, ceil(n * 9.585)))`
where `n` is the block's distinct `(column_id, token)` count (9.585 bits
per element for p = 0.01).

Inserted per block, all field-scoped by `column_id`:

- every word token (see Tokenizer) of `body` and of every string column
  value;
- the exact value, for string values of at most 64 bytes, to accelerate
  equality where the page was not dictionary-encoded.

Field-scoping (the `column_id` in the hash) means a `body` match never
collides with an `attr.k` match, so `has_word(body, 'timeout')` and
`has_word(attr_k, 'timeout')` prune independently.

The FPR is a pruning-efficiency knob, never a correctness knob. A false
positive costs one block scan; a false negative is impossible by
construction, which is what makes bloom-based skipping sound (ADR-0013).
Readers reject a truncated entry, an `m_bits` that is not a power of two
or is below 512, a `k` of 0, a `bits` length that is not `m_bits / 8`,
and an entry index outside `[0, count)`.

## POSTINGS

Optional (ADR-0049). Exact block-level pruning for equality
predicates on dynamic attribute fields the writer was told to index
(`RlogWriter::with_indexed_fields`), a stronger complement to BLOOM: a
bloom probe can only prove absence with a false-positive rate, but a
POSTINGS probe returns the exact set of blocks containing a value, so it
can prune all the way to zero blocks. Absence of the section, or of a
given field within it, is always legal: unindexed fields, fields never
seen by the writer, and fields dropped for exceeding their distinct-value
cap all fall back to bloom pruning plus an exact scan, with identical
query results to an object that never had POSTINGS at all.

Per indexed field, a sorted term dictionary maps each distinct value to
the sorted set of block indices holding a row with that value. The
dictionary is split into fixed-stride term blocks (`postings_stride` in
`RlogConfig`, default 128 terms), each independently zstd-compressed and
crc32c-verified, addressed through a sparse index holding every block's
first term -- the same two-piece sparse-index-plus-data-blocks shape as
RSEG's `SERIES_IDX`, collapsed into one section here since POSTINGS has
no separate whole-object summary to keep apart from its per-field detail.

```
version: u8            (this section's own grammar version, currently 2;
                         independent of the trailer version -- see "Version")
field_count: u32 LE
repeat field_count, ascending column_id:
  column_id: uvarint
  capped: u8                     (0 = postings present, 1 = dropped: over cap)
  if capped == 0:
    stride: uvarint              (terms per term block)
    term_count: uvarint          (total distinct terms for this field)
    block_count: uvarint
    repeat block_count, ascending first_term:
      first_term_len: uvarint
      first_term_bytes: [first_term_len]u8
      block_offset: u64 LE       (absolute offset from section start)
      block_stored_len: u64 LE   (compressed byte length)
      block_uncompressed_len: u64 LE
      block_crc32c: u32 LE       (over the stored/compressed bytes)
term_blocks: [remaining bytes]   (concatenated zstd frames, field then block
                                  order, exactly at the offsets above)
```

`column_id`, `capped`, `stride`, `term_count`, `block_count`, and
`first_term` fields are read eagerly when the section is opened, so a
probe only needs to decompress and crc-verify the one term block a
binary search over `first_term` lands on. Offset/length fields are
fixed-width `u64`/`u32` rather than varint so the header's total byte
length -- and therefore every block's absolute offset -- is computable in
one pass, with no fixed-point dependency on the offsets' own encoded size
(the same reasoning as RSEG's `SERIES_IDX`). A term's sort/equality key
(`ravel_logseg::postings::term_key`) is `Str`/`Bytes` verbatim, `I64` as
big-endian bytes, `F64` as its big-endian bit pattern (bit-exact, matching
the reader's `-0.0`/NaN-payload equality convention), and `Bool` as one
byte -- POSTINGS only ever serves equality/`IN` probes, never a range
scan, so the encoding only needs a consistent total order, not numeric
meaning.

One term block's payload, before compression:

```
term_count_in_block: uvarint     (<= stride)
repeat term_count_in_block, ascending term:
  term_len: uvarint
  term_bytes: [term_len]u8
  posting_count: uvarint
  repeat posting_count: delta-uvarint block index (first absolute, then
                                                    strictly increasing deltas)
```

Parse-time validation additionally requires each field's declared blocks
to tile the bytes following the header exactly: walking fields and blocks
in declaration order, the first block's `block_offset` must equal the
header's own length and every next block's `block_offset` must equal the
previous block's `block_offset + block_stored_len`, with the last block
ending exactly at the section's end. This catches a corrupted offset or
a gap/overlap between blocks, not just an offset past the section.

### Per-field distinct-value cap

A writer bounds per-field cardinality with `RlogConfig.postings_max_distinct`
(default 10,000): if one object's indexed field exceeds it, that field's
postings are dropped for the whole object (`capped = 1`) rather than
failing the write, and `WriteStats.postings_capped_fields` (from
`RlogWriter::finish_with_stats` / `finish_compacted_with_stats`) counts
how many fields this happened to. A capped field is queried exactly as an
unindexed one: bloom pruning plus an exact scan, never a narrowed or
missing result.

The count is over merged values (version 2): a field's distinct terms are
its distinct merged-view values across the object, so resource and scope
values count too. Resource attributes are low cardinality by nature, so
this moves the count little, and the cap already degrades loudly to the
bloom.

### Version

The POSTINGS `version` byte records what a posting list contains. The byte
layout is the same for every version; a reader cannot tell the meanings
apart from the bytes, which is why the version byte exists.

- version 1: a posting list indexes the per-record attribute layer only. A
  reader must not prune a merged-view query on a key that also appears at
  resource or scope level anywhere in the object; it declines and falls
  back to the exact scan.
- version 2: a posting list indexes the merged attribute view of each
  record. The merged view is the union of the record's resource, scope, and
  per-record attributes, the record winning on a key collision (the view
  `ravel_sql::rlog_attrs::merged_attrs` computes for the `attrs` column). A
  reader prunes a merged-view query directly. This is the version the writer
  emits (ADR-0049 amendment).

The version-2 writer gives a merged-view key that is resource- or scope-level
across the whole object (the ordinary OTLP `service.name` shape) a
stream-level-only FIELD_DIR column (above), so its merged-view postings exist
even without a per-record column. This adds entries under the existing
version-2 meaning (a posting list indexes the merged view) rather than
changing what a posting list means, so the bytes and their interpretation are
unchanged. A version-2 object that carries no posting list for a given key is
handled as "no information" (probe returns `Ok(None)`, no prune) exactly as
for any unindexed field.

The section decoder still accepts a version-1 byte and applies the
conservative rule above, but no stored object can reach it: POSTINGS version 1
was only ever written under trailer version 2, and a reader refuses that
trailer outright (see the top of this document -- v2 read support was deleted
with ADR-0095, with no dual-reader window). So the version-1 arm is dead for
stored data, and the "reader accepts both versions" migration story it used to
carry no longer applies to anything. It is kept as a decoder arm, not a
supported input: only the trailer version window decides what opens.

Adding POSTINGS did not bump the trailer `version` (2 at the time): ADR-0029's
versioning carve-out excepts a new section kind, since unknown kinds are
already skipped by old readers and an absent kind is already legal --
exactly POSTINGS's own fallback behavior. The POSTINGS `version` byte above
is this section's own grammar version, separate from the trailer version;
its 1 → 2 bump changed no bytes and needs no trailer bump. Only a change to
an *existing* section's grammar shape, or to a mandatory/optional kind's
legality, needs a trailer version bump and an ADR.

SKIP_IDX's v3 change (above) is the other side of that rule: SKIP_IDX is a
mandatory section that already existed, and redefining what one of its fields
means -- with no byte a reader could use to tell the two meanings apart -- is
precisely the case the carve-out does not cover, so it took the trailer bump
to 3 (ADR-0095).

Adding PAGE_DIR would have been free under the same carve-out, but version 4
is not an additive change: it re-lays BLOCKS, deletes the block header, and
redefines the SKIP_IDX level-0 block crc. Those are exactly the
existing-section grammar changes the carve-out does not cover, so it took the
trailer bump to 4 (ADR-0699 decision 3), which is also what lets the version
byte, rather than a section's presence, be what selects the layout.

## Compaction (L0 → L1)

Compaction (ADR-0032) rewrites many small L0 `.rlog` flush
objects for one sealed `(tenant, shard, ingest-hour)` bucket into a
handful of large L1 parts, the log analogue of RSEG's L0→L1 compaction
(ADR-0018). It lives in `ravel-maintain` behind a per-signal codec seam;
the transaction machinery (seal detection, `CreateIfAbsent` publish,
convergence, abandonment, the advisory cursor) is shared with RSEG and
signal-generic. An L1 part is byte-for-byte a normal RLOG object with
`level = 1`; readers need no special path.

The merge is defined entirely in terms of this format:

- **Global `stream_ref` remap.** The inputs' sorted `STREAM_DIR`s are
  merged into one global sorted stream ordering across all inputs. This
  ordering is used for iteration order (streams are merged in sorted
  `stream_id` order), for the cross-object identity check, and for
  splitting output on disjoint part boundaries (a stream never straddles
  two parts). It is not itself written to any object: when the merge
  splits into multiple parts, each part's own `STREAM_DIR` is built fresh
  by `RlogWriter` from only the streams that landed in that part, with
  dense `stream_ref` ordinals starting at zero per part, exactly as an
  ordinary L0 write already does. So there is no single merged directory
  shared across the output; the global ordering governs the merge, and
  each part re-derives its own local `stream_ref` numbering. Because
  `stream_id` is the canonical hash of a stream's resource+scope blob,
  two inputs may list the same `stream_id` only with byte-identical
  blobs; a disagreement is an upstream identity violation or a hash
  collision and is a hard, typed error (the cross-object form of the
  single-writer `InconsistentStreamAttrs` check), never a silent pick.
- **Re-sort, re-block, and re-group.** The merged records are re-sorted by
  `(stream_ref ascending, ts ascending)` and re-chunked at the same 8192
  record block target, then placed into row groups of `group_target_blocks`
  consecutive blocks with their pages column-major (ADR-0699 decision 1).
  Compaction is where full row groups arise: an L0 flush object is usually one
  short group, an L1 part is many full ones, which is what makes a narrow
  projection over a compacted tenant a few contiguous ranges per object
  instead of one per block. There is no record-level dedup: distinct
  submissions of identical content are distinct records (the write path
  makes retry duplicates structurally impossible), so the L1 object holds
  the union of all inputs' records.
- **Rebuilt directories and indexes.** `FIELD_DIR` is rebuilt from the
  merged column set under the same 1000-dynamic-column cap, with overflow
  keys folded into `attrs_raw` exactly as a single-object write does — the
  union of columns across inputs can exceed the cap even when no single
  input does. `SKIP_IDX` and every per-block `BLOOM` are rebuilt from the
  merged, re-blocked contents (each bloom sized by its own block's
  distinct-token count); an input's `SKIP_IDX`/`BLOOM` bytes are never
  reused or concatenated, since the merged block boundaries differ from
  any input's.

Because these are exactly the steps `RlogWriter` already performs for an
L0 write, the compactor performs them by decoding each input back to
records and feeding the merged records through the same writer via
`finish_compacted`; the L0 and L1 encoders are one implementation.

No input is decoded whole. The merge is a k-way streaming merge with one
cursor per input over the stream it is merging, and its read-side working
set is one row group's stored bytes plus one decoded block per input: the
row group is what one ranged GET brings (under version 4 a block's pages
are spread across the group's column chunks, so no smaller contiguous
range holds a whole block), and the group's blocks are decoded one at a
time out of those bytes, each released before the next. The one term that
scales with the data is the in-progress part's own buffer, bounded by the
part size cap, because a part's content-addressed key does not exist until
the whole part is encoded.

## Tokenizer

`tokens(text)` yields the word tokens used as bloom keys and as the unit
of word/phrase match. It is a normative part of the format because it
defines word semantics identically on the write and read paths:

- split on any non-alphanumeric character (`char::is_alphanumeric`,
  Unicode-aware);
- lowercase each character (`char::to_lowercase`, Unicode-aware), dropping
  any resulting character that is not alphanumeric. Full case mapping can
  emit a combining mark: U+0130 `İ` lowercases to `i` + U+0307 COMBINING
  DOT ABOVE (a Mark), so the mark is dropped and `İ` folds to `i`, matching
  what a query typed `istanbul` produces. This keeps every emitted
  character alphanumeric;
- truncate each token to its longest character-boundary prefix of at most
  64 bytes, measured on the folded characters so a length-changing
  lowercase can neither exceed the cap nor split a codepoint;
- drop empty tokens; keep duplicates (deduplication is the bloom's job).

A change to these rules is not a format version bump (token bytes are
not a pinned invariant; the bloom framing and POSTINGS structure are),
but it does change what an already-written object indexed. An object
written under an earlier fold keeps its old token keys in its bloom and
POSTINGS, so a query folded under the current rules can miss a word in
that object. The miss is a false negative only: a bloom or POSTINGS
negative prunes, and the exact scan re-tokenizes with the current rules,
so no wrong row is returned. Re-ingesting or compacting the object
rewrites its tokens under the current rules.

A multi-token query word is a phrase: the scan requires all its tokens to
be present, in order, in the tokenized field value; a single-token word
requires containment.

## Checksum coverage map

Every byte a reader interprets is covered by a checksum it can verify on
its access path (ADR-0010 §4):

| bytes | checksum | where it lives | when verified |
|---|---|---|---|
| footer proto + trailer (minus crc field) | `footer_crc32c` | trailer | before decoding the footer |
| STREAM_DIR stored bytes | `Section.crc32c` | footer section entry | before decoding the section |
| FIELD_DIR stored bytes | `Section.crc32c` | footer section entry | before decoding the section |
| SKIP_IDX stored bytes | `Section.crc32c` | footer section entry | before decoding the section |
| PAGE_DIR stored bytes | `Section.crc32c` | footer section entry | before decoding the section |
| one page's stored bytes | that page's `crc32c` | PAGE_DIR (version 4) | before decompressing the page |
| one block's pages concatenated in `column_id` order (v4), or its complete bytes including its header (v3) | `block_crc32c` | that block's SKIP_IDX level-0 entry | before decoding the block, by a reader that took every page of it |
| one BLOOM entry's stored bytes | per-entry `crc32c` | BLOOM container framing | before probing the entry |
| POSTINGS header (`column_id`, `capped`, `stride`, counts, `first_term`s, offsets, for every field) | whole-section `Section.crc32c` | footer section entry | before `PostingsSection::parse`, in `RlogReader::scan` |
| one POSTINGS term block's stored bytes | per-block `crc32c` | POSTINGS sparse-index entry | before decompressing the block a probe lands on |

BLOCKS and BLOOM have no whole-section crc because they are never read
whole: a selective scan touches a handful of blocks or blooms, and a
whole-section crc could not be verified without fetching the whole
section, defeating the point. Their per-page, per-block, and per-entry
crc32c are the access-path-verifiable equivalents.

Version 4's per-page crc32c exists for the same reason one level down. A
reader that fetched two of a hundred columns holds no more than those two
columns' pages, so it cannot verify the block crc without fetching the block
and defeating the projection. Every page it decompresses is covered by its own
checksum instead, and the `enc`/`comp` tags that decide how to interpret it sit
under PAGE_DIR's section crc, so a flipped tag fails a checksum before it can
cause a misdecode.

POSTINGS is read differently: `scan` always reads the section's stored
bytes in full to reach any block within it, so its whole-section
`Section.crc32c` is verified the same way STREAM_DIR's and FIELD_DIR's
is, before `PostingsSection::parse` touches the header. This is the only
protection an out-of-bounds or garbage `first_term`, offset, or count in
the header gets; earlier revisions of this format checked it only
structurally (ascending order, cap checks, exact block tiling) and not
by crc, which let a header byte flip route `probe` to the wrong block
without detection as long as ordering and per-block checksums still
passed. `probe` adds a second, independent check for a future reader
that fetches one block via a range read instead of the whole section: it
requires the decompressed block's first term to equal the sparse
entry's `first_term`, and every term in the block to sort below the next
entry's `first_term`. The `enc`/`comp` bytes of a page are covered by the
enclosing block's crc, so a flipped tag fails the crc rather than causing
a silent misdecode. Pad bytes between sections are never interpreted and
fall under the whole-object BLAKE3 in the commit record.

## Pruning soundness (invariant)

Extends the ADR-0013 rule to logs: a structure may prune only what it
proves absent.

- Skip-index min/max: a block is dropped only when its bounds prove no
  record matches the ts/stream/numeric predicate.
- POSTINGS is exact, not probabilistic: for an indexed field, a probed
  term's block list is the complete truth, so it may prune all the way to
  zero blocks. An unindexed or capped field's probe reports "no
  information" (not "term absent"), same as a field POSTINGS never heard
  of, and prunes nothing. `RlogReader::scan` applies it between skip-index
  and bloom pruning, so bloom only has to consider whatever POSTINGS
  could not already rule out.
- Bloom negative is proof of absence: skip the block. Bloom positive is
  no information: scan the block and evaluate the predicate exactly on
  decoded values.
- Regex/substring predicates consult blooms only when the planner can
  extract word literals that any match must contain; otherwise only
  time/stream/min-max pruning applies and the scan evaluates exactly.
- A missing or corrupt BLOOM section degrades to scanning without bloom
  pruning and surfaces a counter, never wrong results. A missing POSTINGS
  section, a missing per-field entry, or a corrupt section/entry likewise
  degrades to no postings pruning for the affected arm (`ScanStats`'
  `postings_degraded`), never wrong results. A corrupt or undecodable
  SKIP_IDX is a loud `Corrupted` error, not a degrade: its level-0 entries
  are the only source of block byte ranges and per-block checksums, so
  without it blocks cannot be located at all. Corrupt BLOCKS data is
  likewise a loud `Corrupted` error.

## Validation summary

All violations are `Corrupted`, never panics:

- object smaller than 16 bytes; bad magic; unsupported version or signal;
  non-zero reserved byte; `footer_len` zero or past the trailer; footer
  crc mismatch; footer proto decode failure.
- duplicate section kind; missing mandatory section; section range out of
  bounds or overflowing; section or page `uncompressed_len` over the cap;
  decompressed length not equal to the declared length; section crc
  mismatch.
- overlong or truncated varint; trailing bytes past a declared structure;
  unsorted STREAM_DIR or FIELD_DIR; entry count over the configured cap;
  unknown field type byte; unknown encoding or compression tag.
- codec: id out of dictionary range; delta/double-delta accumulation
  overflow; FOR `bit_width > 64` or packed length mismatch; a codec not
  consuming exactly its bytes.
- bloom: `m_bits` not a power of two or below 512; `k = 0`; `bits` length
  wrong; entry crc mismatch; entry index out of range.
- postings: whole-section crc mismatch (checked before the header is
  parsed at all); unknown section grammar version; field count or one
  field's term/block count over its cap; non-ascending `column_id`
  across fields or `first_term` within a field; a term block's declared
  `uncompressed_len` over its cap; a `block_offset` that does not exactly
  continue the running cursor (catches both an offset past the section
  and a gap or overlap between blocks); a term block's crc mismatch or a
  decompressed length not equal to its declared `uncompressed_len`; a
  decompressed block's first term not equal to its sparse-index entry's
  `first_term`, or any term in it not sorting below the next entry's
  `first_term`; terms within a decompressed block not ascending, or
  trailing bytes left over once the declared term count is consumed; a
  declared term or posting count the block's remaining bytes cannot
  support.
- page_dir: whole-section crc mismatch (checked before the section is decoded
  at all, so no offset in it is ever followed unverified); a group count over
  `MAX_BLOCKS`; a group with no blocks or no chunks; a group whose
  `first_block` does not continue where the previous group ended (catches both
  a gap and an overlap in the block partition); a chunk count above what the
  group's blocks could carry pages for (`block_count * MAX_PAGES`);
  non-ascending `column_id` across a group's chunks; a page count outside
  `1..=2 * block_count`; a page naming a block outside its group or going
  backwards within a chunk; an unknown `enc` tag; an overflowing chunk length
  or extent; a chunk whose extent ends past the BLOCKS section; a total block
  count disagreeing with the SKIP_IDX level-0 entry count; truncation; trailing
  bytes.
- page crc mismatch (version 4, before the page is decompressed).
- block crc mismatch.
