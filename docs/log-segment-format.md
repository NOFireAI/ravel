# RLOG: Ravel Log Segment Format

Persistent contract (ADR-0029). Any change bumps the trailer version. The
current trailer version is 2 (ADR-0032 added the footer's compaction-identity
fields; version 1 was the format-only initial release and no reader accepts it
any longer).
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
| BLOOM        per-block token blooms               |  kind 5
| footer: LogFooter protobuf bytes                  |
| trailer (16 bytes):                               |
|   footer_len:   u32                               |
|   footer_crc32c:u32                               |
|   version:      u16   (= 2)                       |
|   signal:       u8    (2 = logs)                  |
|   reserved:     u8    (= 0)                       |
|   magic:        [u8;4] = "RLG1"                   |
+---------------------------------------------------+
```

Writers emit the sections physically in kind order (1..5); readers rely
only on the footer's section offsets, never on adjacency. Bytes between
sections are permitted and MUST be `0x00`; readers never interpret them.

`footer_crc32c` is computed over: the `LogFooter` bytes, then `footer_len`
(u32 LE), `version` (u16 LE), `signal`, `reserved`, `magic`. Every
trailer byte except the crc field itself is covered (ADR-0010 §4).

## Reader protocol

Identical in shape to RSEG v1:

1. Reject objects smaller than 16 bytes as `Corrupted`.
2. Suffix-GET 64 KiB (or the whole object if smaller). Verify `magic`,
   `version`, `signal`, `reserved`.
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
  (STREAM_DIR, FIELD_DIR, BLOCKS, SKIP_IDX, BLOOM) are mandatory.
- Every section `[offset, offset+len)` lies within
  `[0, total_size - 16 - footer_len)`, with overflow-checked arithmetic.
- `uncompressed_len` is capped by config (default 1 GiB per section) and
  the decompressed length must equal it exactly.

### Section kinds v1

| kind | name | content | comp |
|---|---|---|---|
| 1 | STREAM_DIR | stream_id -> canonical resource+scope blob, block range | zstd |
| 2 | FIELD_DIR | field name+type -> column id, stats | zstd |
| 3 | BLOCKS | row blocks, per-column pages | none (per-page) |
| 4 | SKIP_IDX | skip index levels 0 and 1 | zstd |
| 5 | BLOOM | per-block token bloom filters | none (per-entry) |

STREAM_DIR, FIELD_DIR, and SKIP_IDX are compressed as whole sections
(zstd level 3 default) and always read whole. BLOCKS and BLOOM are
containers: not compressed as a unit, their entries individually
addressable so one block or one bloom is readable alone.

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

## BLOCKS

Records are sorted `(stream_ref ascending, ts_ns ascending)`. Target 8192
records per block, cap 8 MiB uncompressed. Per block, one page per column
that has at least one value in the block.

```
block:
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

A column with values in only some rows of a block carries a presence
bitmap page (a bitmap-encoded page, `enc = 8`) paired with the value
page, so decode restores exact row alignment. A column absent from a
block is all-null and occupies zero bytes. The block's crc32c (stored in
its SKIP_IDX level-0 entry, not inline) covers the complete block bytes
(header and all pages); the reader verifies it before decoding anything
in the block.

```
   one row block, columnar:

   rows ->   r0 r1 r2 r3 r4 ... r8191
   ts        [ delta / double-delta page       ]
   stream    [ RLE page: (ref,run)(ref,run) ...]
   sev_num   [ bit-packed FOR page             ]
   body      [ string plain: lengths | blob    ]
   attr.k    [ presence bitmap | FOR page      ]
```

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
  block_offset:   varint   (into BLOCKS)
  block_len:      varint
  block_crc32c:   u32      crc32c over the block's complete stored bytes
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
  range and crc omitted, mins/maxes merged over the entry's <= 64
  children
```

Fanout is 64: one level-1 entry per 64 level-0 blocks. A numeric stat
stores i64 as its two's-complement `u64` bit pattern (`v as u64`), and
f64 as its `to_bits` pattern; min/max for f64 use `total_cmp` order over
non-NaN values, and NaN values are counted in `has_nan` and excluded from
min/max. Readers reject a block count over the configured cap and any
truncation.

`candidate_blocks(ts_min, ts_max, stream_refs)` returns the level-0 block
indices whose entries survive the coarse predicate. The pruning is sound
(ADR-0013): a block is dropped only when its min/max bounds prove no
record in it can match. Precision is not guaranteed; survivors are
scanned and re-evaluated exactly.

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

## Compaction (L0 → L1)

Compaction (ADR-0032, issue #231) rewrites many small L0 `.rlog` flush
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
- **Re-sort and re-block.** The merged records are re-sorted by
  `(stream_ref ascending, ts ascending)` and re-chunked at the same 8192
  record block target. There is no record-level dedup: distinct
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

## Tokenizer

`tokens(text)` yields the word tokens used as bloom keys and as the unit
of word/phrase match. It is a normative part of the format because it
defines word semantics identically on the write and read paths:

- split on any non-alphanumeric character (`char::is_alphanumeric`,
  Unicode-aware);
- lowercase each token (`char::to_lowercase`, Unicode-aware);
- truncate each token to its longest character-boundary prefix of at most
  64 bytes;
- drop empty tokens; keep duplicates (deduplication is the bloom's job).

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
| one BLOCKS block's stored bytes | `block_crc32c` | that block's SKIP_IDX level-0 entry | before decoding the block |
| one BLOOM entry's stored bytes | per-entry `crc32c` | BLOOM container framing | before probing the entry |

BLOCKS and BLOOM have no whole-section crc because they are never read
whole: a selective scan touches a handful of blocks and their blooms, and
a whole-section crc could not be verified without fetching the whole
section, defeating the point. Their per-block and per-entry crc32c are the
access-path-verifiable equivalents. The `enc`/`comp` bytes of a page are
covered by the enclosing block's crc, so a flipped tag fails the crc
rather than causing a silent misdecode. Pad bytes between sections are
never interpreted and fall under the whole-object BLAKE3 in the commit
record.

## Pruning soundness (invariant)

Extends the ADR-0013 rule to logs: a structure may prune only what it
proves absent.

- Skip-index min/max: a block is dropped only when its bounds prove no
  record matches the ts/stream/numeric predicate.
- Bloom negative is proof of absence: skip the block. Bloom positive is
  no information: scan the block and evaluate the predicate exactly on
  decoded values.
- Regex/substring predicates consult blooms only when the planner can
  extract word literals that any match must contain; otherwise only
  time/stream/min-max pruning applies and the scan evaluates exactly.
- A missing or corrupt BLOOM section degrades to scanning without bloom
  pruning and surfaces a counter, never wrong results. A corrupt or
  undecodable SKIP_IDX is a loud `Corrupted` error, not a degrade: its
  level-0 entries are the only source of block byte ranges and per-block
  checksums, so without it blocks cannot be located at all. Corrupt
  BLOCKS data is likewise a loud `Corrupted` error.

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
- block crc mismatch.
