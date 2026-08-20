# Inspecting data

`ravel-cli` reads segments and commit records directly from the object
store. Nothing here needs `ravel-server` running. The examples below run
against the bucket that `make demo` writes to
([docs/guides/getting-started.md](getting-started.md)). Every command
needs the same store flags:

```sh
export RAVEL_S3_ENDPOINT=http://127.0.0.1:9000
export RAVEL_S3_BUCKET=ravel-dev
export RAVEL_S3_ACCESS_KEY=ravel
export RAVEL_S3_SECRET_KEY=ravel-dev-secret
```

## Key layout

![tenancy and key layout](../diagrams/tenancy-key-layout.svg)

Everything lives under one bucket root, prefixed by a hash of the tenant
name ([docs/catalog-and-mvcc.md](../catalog-and-mvcc.md)):

```
t/<tenant_hash>/m/l0/<shard>/<writer_id>.<epoch>.<seq>.<hash16>.rseg   segment (data) object
t/<tenant_hash>/m/c/<shard>/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt commit record
```

`tenant_hash` is BLAKE3 of the tenant name, hex-encoded. `m` is the signal
letter for metrics; logs use `l` and spans use `s`, and all three are
implemented. Logs have their own RLOG object format and an `rlog inspect`
walkthrough further down this guide; spans have the RSPAN format
([span-segment-format.md](../span-segment-format.md), ADR-0041) and a SQL
query path over the `spans` table. `shard` is
the ingest shard, zero-padded to 4 digits. `ingest_hour` is the UTC hour the
commit landed in (`YYYYMMDDTHH`). This lets the catalog find recent
commits by listing a small, bounded set of prefixes instead of the whole
bucket.

## `catalog list`: what's visible right now

```sh
cargo run -p ravel-cli -- catalog list --tenant demo-tenant --hours 1
```

```
t/3f2a.../m/l0/0000/6a9c....rseg shard=0 samples=120 series=3 min_event_ts_ns=1732400000000000000 max_event_ts_ns=1732400059000000000 created_unix_ns=1732400060123456789
1 segment(s)
```

This resolves the same catalog snapshot that a query would, over the last
`--hours` hours (default 1) and `--shards` shards (default 4; it must match
the shard count the writer used, `4` for `make demo`). Each line is
one committed segment. It shows the data object key it is stored under, its
shard, sample and series counts, its event-time span, and when the flush that
created it ran (`created_unix_ns`). This is the fastest way to get a real
key to feed into `segment inspect` or `commit decode`.

## `segment inspect`: what's inside one segment

![RSEG layout](../diagrams/rseg-layout.svg)

Every segment is RSEG v7. ADR-0027 leaves one supported version at a time,
and ADR-0092 moved it from v6 to v7. The command is:

```sh
cargo run -p ravel-cli -- segment inspect \
  "t/c5c5.../m/l0/0000/6a9c....rseg"
```

```
total_size: 924
trailer_offset: 908
version: 7
footer_offset: 660
tenant_hash: c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7
shard: 7
writer_id: golden-v7-writer
writer_epoch: 7
writer_seq: 70
min_event_ts_ns: 1650000000000000000
max_event_ts_ns: 1650000000000000758
min_ingest_ts_ns: -2000
max_ingest_ts_ns: 30000
sample_count: 15
series_count (footer): 9
base_created_unix_ns: 219
level: 1
input_set_hash: 4747474747474747474747474747474747474747474747474747474747474747
part_index: 2
sections:
  kind=1 name=LABEL_DICT offset=0 len=106 uncompressed_len=111 comp=2
  kind=5 name=SERIES_IDS offset=106 len=148 uncompressed_len=148 comp=0
  kind=6 name=SERIES_META offset=254 len=116 uncompressed_len=200 comp=2
  kind=3 name=TS_PAGES offset=370 len=75 uncompressed_len=75 comp=0
  kind=4 name=VAL_PAGES offset=448 len=75 uncompressed_len=75 comp=0
  kind=7 name=HIST_PAGES offset=523 len=96 uncompressed_len=96 comp=0
  kind=10 name=EXEMPLARS offset=619 len=41 uncompressed_len=41 comp=0
schema_count (derived): 1
  schema[0]: __name__,inst,job
series_count (decoded): 9
series:
  series_id=00000000000000000000000000000000 labels=__name__=golden_v7,inst=i0,job=job0 sample_count=1 min_ts_ns=1650000000000000000 max_ts_ns=1650000000000000000 value_kind=HIST_SPANS run_count=1
    run[0] created_unix_ns=219 writer_epoch=2 writer_seq=3 sample_count=1 ts_range=[370, 377) hist_range=[523, 555)
    hist[0]: ts_ns=1650000000000000000 scale=2 zero_threshold=0.000001 sum=0.125 reset_hint=UNKNOWN
      count_kind=INT zero_count=1 count=4
      positive: spans=[(0, 2)] counts=[2,1]
      negative: spans=[] counts=[]
  series_id=010000000000010000000100000001b3 labels=__name__=golden_v7,inst=i1,job=job1 sample_count=2 min_ts_ns=1650000000000000001 max_ts_ns=1650000000000000751 value_kind=VAL_SCALAR run_count=1
    run[0] created_unix_ns=220 writer_epoch=2 writer_seq=3 sample_count=2 ts_range=[377, 386) val_range=[448, 460)
  ... (7 more series)
```

Field by field:

- `total_size`, `trailer_offset`, `footer_offset`: the byte layout of the
  object. RSEG segments are footer-first-readable. The 16-byte trailer at
  the very end gives the footer's length and checksum. A reader therefore
  needs one suffix GET to find and validate the footer before it fetches
  anything else.
- `version`: the trailer format version, always `7`. A non-7 version (a
  stray pre-release object, including a retired v6) gets a typed error; Ravel
  never half-parses it.
- `tenant_hash`, `shard`, `writer_id`, `writer_epoch`, `writer_seq`: the
  identity components embedded in the object's key and its commit token. They
  let you confirm that a segment and a commit token or record agree on what
  wrote it.
- `min/max_event_ts_ns`: the span of sample timestamps inside the segment.
- `min/max_ingest_ts_ns`: when this server received those points.
- `base_created_unix_ns`, `level`, `part_index`: compaction provenance from
  the footer. An L0 flush stamps `level = 0`, `part_index = 0`. A compacted
  object carries real values (as here). `base_created_unix_ns` is the
  minimum run creation time, the base that the per-run `created_unix_ns`
  deltas reconstruct against.
- `sample_count`, `series_count (footer)`: the totals that the footer claims.
- `sections`: the object's sections and byte ranges. `kind=1` `LABEL_DICT`
  (the string table), `kind=5` `SERIES_IDS` (the sorted ids), `kind=6`
  `SERIES_META` (the run-major catalog: each series' schema, value kind,
  and per-run provenance and page ranges), `kind=3` `TS_PAGES`, `kind=4`
  `VAL_PAGES` (scalar values), `kind=7` `HIST_PAGES` (histogram values). A
  large object (`series_count >= 4096`) instead carries `kind=8`
  `SERIES_IDX` and `kind=9` `SERIES_META_CHUNKS` in place of the whole
  `SERIES_META`: the sparse catalog. `VAL_PAGES` is absent when no series
  is scalar, and `HIST_PAGES` when none is a histogram. `comp` is the raw wire
  integer (`0` none, `1` lz4, `2` zstd).

  `kind=10` `EXEMPLARS` holds the exemplars that samples in this object
  carried. The section is present only when at least one sample carried
  one. The example above shows no `kind=10` line, because its samples
  carried none. An absent `EXEMPLARS` section is normal and is not an
  error.
- `schema_count (derived)` / `schema[N]:`: SERIES_META groups series by
  distinct label-*name* set (a "schema"). Each line lists that schema's
  names, resolved through `LABEL_DICT`. `ravel-cli` derives this from the
  decoded per-series label sets.
- `series`: one line per series: id, resolved labels, sample count,
  event-timestamp bounds, `value_kind` (`VAL_SCALAR` or `HIST_SPANS`), and
  `run_count`. Each series then prints one `run[N]` line per run. Each
  `run[N]` line gives its provenance (`created_unix_ns`, `writer_epoch`,
  `writer_seq`), sample count, and the **absolute** byte ranges of that run's
  TS and VAL-or-HIST pages (`ts_range`/`val_range`/`hist_range`, half-open
  `[start, end)`). `ravel-cli` reconstructs these from SERIES_META the way
  the reader does before it fetches the bytes. An L0 flush produces one run
  per series; a compacted object can carry several.
- Every `HIST_SPANS` run is followed by one `hist[N]:` line per decoded
  histogram sample. Each line gives `scale`, `zero_threshold`, `sum` (`none`
  if absent), and `reset_hint`, then `count_kind` (`INT`/`FLOAT`) with
  `zero_count`/`count`, then the positive and negative sides' spans
  (`(offset, length)` pairs) and bucket counts, in stored order.
- `series_count (decoded)`: the series count from decoding the catalog, not
  just from trusting the footer. If it matches `series_count (footer)`, the
  segment is internally consistent.


## `rlog inspect`: what's inside one log segment

Log data lives in RLOG objects (`.rlog`), the columnar log segment format
(docs/log-segment-format.md, ADR-0029; trailer version 3, ADR-0095). RLOG is
a sibling of RSEG. It shares the same 16-byte trailer, protobuf footer, and
crc32c discipline, but it has its own sections and none of the bytes. Ingest, query, and lifecycle all run today: the production ingest path writes
RLOG objects through `ravel-ingest`'s log shard, the `logs` SQL table on
`POST /api/v1/sql` reads them back, and `ravel-maintain` compacts and retains
them. The walkthrough below drives the writer directly (in tests and
tooling), not through the ingest path, to show the format in isolation. The
command is:

```sh
cargo run -p ravel-cli -- rlog inspect "t/abab.../l/l0/0000/....rlog"
```

```
total_size: 732
version: 3
signal: 2
tenant_hash: abababababababababababababababab
shard: 3
writer_id: cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd
writer_epoch: 7
writer_seq: 42
min_ts_ns: 100
max_ts_ns: 250
min_observed_ts_ns: 105
max_observed_ts_ns: 255
record_count: 4
block_count: 2
stream_count: 2
level: 0
input_set_hash: 
part_index: 0
sections:
  kind=1 name=STREAM_DIR offset=0 len=64 comp=zstd uncompressed_len=96
  kind=2 name=FIELD_DIR offset=64 len=30 comp=zstd uncompressed_len=21
  kind=3 name=BLOCKS offset=94 len=214 comp=none uncompressed_len=214
  kind=4 name=SKIP_IDX offset=308 len=75 comp=zstd uncompressed_len=104
  kind=5 name=BLOOM offset=383 len=164 comp=none uncompressed_len=164
skip_index level 0 (2 block(s)):
  block[0] offset=0 len=105 crc32c=e97c8ea6 record_count=2 ts_range=[100, 200] stream_ref_range=[0, 0]
    stat column_id=10 type=i64 min_bits=200 max_bits=504 null_count=0 has_nan=false
  block[1] offset=105 len=109 crc32c=c009cac7 record_count=2 ts_range=[150, 250] stream_ref_range=[1, 1]
    stat column_id=10 type=i64 min_bits=200 max_bits=401 null_count=0 has_nan=false
stream_dir (2 entry(ies)):
  stream_ref=0 stream_id=01000000000000000000000000000000 blob_len=27 blocks=[0, 0]
  stream_ref=1 stream_id=02000000000000000000000000000000 blob_len=27 blocks=[1, 1]
field_dir (2 entry(ies)):
  column_id=10 name=code type=i64 present_blocks=2 null_count=0
  column_id=11 name=svc type=str present_blocks=2 null_count=0
```

Field by field:

- `total_size`, `version`, `signal`: the byte length of the object, the
  trailer format version (currently `3`; any other version gets a typed
  error, with no dual-reader path, so a stored v2 object is unreadable rather
  than migratable), and the signal byte (`2` = logs). Like
  RSEG, the object is footer-first-readable. The 16-byte trailer at the end
  gives the footer's length and crc. A reader therefore validates the footer
  in one suffix GET before it fetches anything else.
- `tenant_hash`, `shard`, `writer_id`, `writer_epoch`, `writer_seq`: the
  identity components. They must match the commit record that the reader
  resolved the object from. `writer_id` and `tenant_hash` are printed as hex.
- `min/max_ts_ns`: the span of record event timestamps.
  `min/max_observed_ts_ns`: the span of observed (ingest-side) timestamps.
  These four values plus the counts are the skip index's level 2, the
  whole-object summary in the footer.
- `record_count`, `block_count`, `stream_count`: the totals that the footer
  claims.
- `level`, `input_set_hash`, `part_index`: compaction provenance (ADR-0032),
  the same convention that RSEG uses. An L0 flush object (every object shown in
  this guide) stamps the sentinels `level=0`, empty `input_set_hash`, and
  `part_index=0`. A future L1 compacted object carries real values.
- `sections`: the five mandatory sections and their byte ranges. `kind=1`
  `STREAM_DIR` (stream_id to canonical resource+scope blob and block range),
  `kind=2` `FIELD_DIR` (dynamic attribute columns), `kind=3` `BLOCKS` (the
  columnar row blocks), `kind=4` `SKIP_IDX` (the multi-level min/max index),
  `kind=5` `BLOOM` (per-block token blooms). STREAM_DIR, FIELD_DIR, and
  SKIP_IDX use whole-section zstd (`comp=zstd`). BLOCKS and BLOOM are
  containers that a reader reads entry by entry, so they are `comp=none`.
  `comp` is printed by name (`none`/`zstd`).
- `skip_index level 0`: one line per row block. Each line gives its byte
  `offset` (into BLOCKS) and `len`, the `crc32c` that the reader verifies
  before it decodes the block, `record_count`, and the block's `ts_range` and
  `stream_ref_range` (both inclusive). The skip index prunes on those two
  ranges. Under each block line is one `stat` line per numeric column the
  block's records resolve a value for: `column_id`, `type`
  (`i64`/`f64`/`bool`/`bytes`), `min_bits`/`max_bits`, `null_count`, and
  `has_nan`. `min_bits`/`max_bits`
  are the bit pattern that the min/max are stored as: two's complement for
  i64, and `to_bits` for f64, so f64 comparison is bit-exact. In the example,
  both blocks carry column 10 (`code`), an i64 attribute. The string column
  `svc` is not numeric and so has no stat. Since trailer v3 (ADR-0095) a stat
  bounds the value each row *resolves* for the column's attribute name, not
  whatever sits in the column's value page. Resolution is what a query sees:
  the record's resource and scope attributes, overridden by the record's own,
  with a record carrying `code` twice (two types, or a duplicate that spilled
  into `attrs_raw`) reduced to the one value a read reports. A row whose
  resolved value is of another type, or which resolves the name to nothing,
  counts in `null_count` instead. Three things follow that look odd until you
  know the rule: a `stat`'s `null_count` can exceed the same column's
  `field_dir` `null_count`, which counts raw column presence; the bounds can
  exclude a value stored in the block; and a stat can appear for a column the
  block has no page for at all, when its records resolve the name off their
  resource or scope rather than carrying it themselves.
- `stream_dir`: one line per stream, in the object's sorted stream_id order.
  The line number is the `stream_ref` used everywhere else (the entry's
  0-based ordinal). `stream_id` is the 16-byte identity in hex. `blob_len` is
  the length of the canonical resource+scope attribute blob. `blocks` is the
  inclusive block range that holds that stream's records, printed half-open.
- `field_dir`: one line per dynamic attribute column: `column_id` (dynamic
  columns start at 10; fixed columns 0..=9 are implicit and never listed),
  `name`, `type`, `present_blocks` (blocks with at least one value), and the
  object-wide `null_count`. A key seen with two value types appears as two
  entries (per-type splitting).

A corrupt object never half-prints. The footer open protocol and every
section decode return a typed `Corrupted` error with a non-zero exit. A
corrupt SKIP_IDX in particular is loud, not a degrade, because its
level-0 entries are the only source of block byte ranges and per-block
checksums.

## `commit decode`: what a commit record says

```sh
cargo run -p ravel-cli -- commit decode \
  "t/3f2a.../m/c/0000/20251127T18/6a9c....cmt"
```

```
format_version: 2
tenant_hash: 3f2a...
signal: 1
shard: 0
writer_id: <uuid>
writer_epoch: 0
writer_seq: 0
object_key: t/3f2a.../m/l0/0000/6a9c....rseg
object_size: 8421
content_hash: 6a9c...
sample_count: 120
series_count: 3
min_event_ts_ns: 1732400000000000000
max_event_ts_ns: 1732400059000000000
min_ingest_ts_ns: 1732400060000000000
max_ingest_ts_ns: 1732400060050000000
segment_format_version: 5
created_unix_ns: 1732400060123456789
ingest_hour_bucket: 2025112718
```

A commit record never holds sample data itself. It is a small pointer plus
enough metadata to prune without opening the segment. `object_key` and
`object_size` name the segment that this record publishes. `content_hash` is
the blake3 hash embedded in that segment's own key (`hash16` above,
extended here to the full hash). It lets a retried commit PUT tell two cases
apart: "already published, same content" (safe) and "already published,
different content" (a fatal split-brain, because two different segments
should never share a `(writer_id, epoch, seq)`). `signal` is the
numeric signal code (`1` = metrics). `ingest_hour_bucket` is the same hour
encoded in the object's own key, and it is what the catalog groups listings
by.

## `idem inspect`: what an idempotency marker says

```sh
cargo run -p ravel-cli -- idem inspect \
  "t/3f2a.../l/idem/9a1c....0495972.idm"
```

```
magic: valid (RIDM)
version: valid
crc32c: valid
written_count: 42
commit_tokens: [v2:token-abc, v2:token-def]
```

An idempotency marker (ADR-0051 section 5) is the receipt a keyed log or
span ingest request writes after a successful flush, so a retry of the same
request replays this receipt instead of re-ingesting. `written_count` is the
row or span count the original request wrote; `commit_tokens` is the full
`x-ravel-commit-token` set the ack carried, one token per shard the
request's points flushed through.

A truncated, bad-magic, wrong-version, checksum-mismatched, or malformed
marker fails with the specific reason instead of a generic error or a
silent `valid`-looking report -- this command decodes through the same
function the ingest path itself uses to interpret a marker (there is
exactly one decoder for this format), so its verdict always matches what a
retried request would actually experience: a marker this command reports as
corrupt is a marker the ingest path also treats as a miss (fail-open to
at-least-once), never the reverse.
