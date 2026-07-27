# Inspecting data

`ravel-cli` reads segments and commit records straight out of the object
store; nothing here needs `ravel-server` running. The examples below run
against the bucket `make demo` writes to
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
(metrics; logs and spans would be `l`/`s`, not implemented yet). `shard` is
the ingest shard, zero-padded to 4 digits. `ingest_hour` is the UTC hour the
commit landed in (`YYYYMMDDTHH`), which is what lets the catalog find recent
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

This resolves the same catalog snapshot a query would, over the last
`--hours` hours (default 1) and `--shards` shards (default 4; must match
whatever shard count the writer used, `4` for `make demo`). Each line is
one committed segment: the data object key it's stored under, its shard,
sample and series counts, its event-time span, and when the flush that
created it ran (`created_unix_ns`). This is the fastest way to get a real
key to feed into `segment inspect` or `commit decode`.

## `segment inspect`: what's inside one segment

![RSEG layout](../diagrams/rseg-layout.svg)

```sh
cargo run -p ravel-cli -- segment inspect \
  "t/3f2a.../m/l0/0000/6a9c....rseg"
```

```
total_size: 8421
trailer_offset: 8405
version: 1
footer_offset: 8112
tenant_hash: 3f2a...
shard: 0
writer_id: <uuid>
writer_epoch: 0
writer_seq: 0
min_event_ts_ns: 1732400000000000000
max_event_ts_ns: 1732400059000000000
min_ingest_ts_ns: 1732400060000000000
max_ingest_ts_ns: 1732400060050000000
sample_count: 120
series_count (footer): 3
sections:
  kind=1 offset=0 len=64 uncompressed_len=64 comp=2
  kind=2 offset=64 len=512 uncompressed_len=512 comp=2
  kind=3 offset=576 len=1200 uncompressed_len=1200 comp=0
  kind=4 offset=1776 len=6336 uncompressed_len=6336 comp=0
series_count (decoded): 3
```

Field by field:

- `total_size`, `trailer_offset`, `footer_offset`: byte layout of the
  object. RSEG segments are footer-first-readable: the 16-byte trailer at
  the very end gives the footer's length and checksum, so a reader only
  needs one suffix GET to find and validate the footer before fetching
  anything else.
- `version`: the trailer format version, `1` here. `ravel-cli` reads both
  RSEG v1 and v2 objects (ADR-0014); see "`segment inspect`: RSEG v2
  objects" below for what changes when this reads `2`.
- `tenant_hash`, `shard`, `writer_id`, `writer_epoch`, `writer_seq`: the
  same identity components embedded in the object's key and in its commit
  token, which is how you confirm a segment and a commit token/record
  agree on what wrote it.
- `min/max_event_ts_ns`: the span of sample timestamps inside the segment,
  as reported by the client.
- `min/max_ingest_ts_ns`: the span of when this server received those
  points, always close to `created_unix_ns` below since Phase 1 flushes
  promptly.
- `sample_count`, `series_count (footer)`: totals the footer itself
  claims.
- `sections`: the four mandatory v1 sections and their byte ranges inside
  the object. `kind=1` is `LABEL_DICT` (the string table backing every
  label name/value in this segment), `kind=2` is `SERIES_TABLE` (one entry
  per series: its label set, sample count, timestamp span, and where its
  data pages live), `kind=3` is `TS_PAGES` (delta-encoded timestamps),
  `kind=4` is `VAL_PAGES` (Gorilla-encoded or raw f64 values, whichever is
  smaller for that series). `comp` is the section's compression as the raw
  wire integer (`ravel.segment.v1.Compression`: `0` none, `1` lz4, `2`
  zstd) -- `ravel-cli` prints the field as stored, not the resolved enum
  name. LABEL_DICT and SERIES_TABLE (v1) or SERIES_META (v2) are zstd by
  writer policy (`comp=2`); TS_PAGES/VAL_PAGES containers are never
  compressed as a whole section (`comp=0`), since the pages inside are
  individually compressed instead.
- `series_count (decoded)`: series count from actually decoding
  `LABEL_DICT` + `SERIES_TABLE`, not just trusting the footer's claimed
  count. Matching `series_count (footer)` means the segment is internally
  consistent.

## `segment inspect`: RSEG v2 objects

`segment inspect` reads a v2 object (trailer `version = 2`, ADR-0014,
docs/segment-format.md "RSEG v2 amendment") exactly the same way, over the
same command:

```sh
cargo run -p ravel-cli -- segment inspect \
  "t/3f2a.../m/l0/0000/7c1e....rseg"
```

```
total_size: 837
trailer_offset: 821
version: 2
footer_offset: 656
tenant_hash: cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd
shard: 11
writer_id: golden-v2-writer
writer_epoch: 5
writer_seq: 200
min_event_ts_ns: -200
max_event_ts_ns: 1900
min_ingest_ts_ns: -2000
max_ingest_ts_ns: 9000
sample_count: 67
series_count (footer): 4
sections:
  kind=1 name=LABEL_DICT offset=0 len=103 uncompressed_len=102 comp=2
  kind=5 name=SERIES_IDS offset=103 len=68 uncompressed_len=68 comp=0
  kind=6 name=SERIES_META offset=171 len=85 uncompressed_len=76 comp=2
  kind=3 name=TS_PAGES offset=256 len=115 uncompressed_len=115 comp=0
  kind=4 name=VAL_PAGES offset=376 len=280 uncompressed_len=280 comp=0
schema_count (derived): 3
  schema[0]: __name__,instance,method
  schema[1]: __name__
  schema[2]: __name__,instance,region
series_count (decoded): 4
series:
  series_id=11111111111111111111111111111111 labels=__name__=http_requests_total,instance=a,method=GET sample_count=40 min_ts_ns=1000 max_ts_ns=1663 ts_range=[256, 303) val_range=[376, 442)
  series_id=22222222222222222222222222222222 labels=__name__=http_requests_total,instance=b,method=POST sample_count=6 min_ts_ns=-200 max_ts_ns=900 ts_range=[303, 319) val_range=[442, 469)
  series_id=33333333333333333333333333333333 labels=__name__=cpu_seconds sample_count=1 min_ts_ns=42 max_ts_ns=42 ts_range=[319, 326) val_range=[474, 488) ALIGNMENT_GAP(ts_page_gap=0, val_page_gap=5)
  series_id=44444444444444444444444444444444 labels=__name__=memory_bytes,instance=a,region=us-east sample_count=20 min_ts_ns=0 max_ts_ns=1900 ts_range=[326, 371) val_range=[490, 656) ALIGNMENT_GAP(ts_page_gap=0, val_page_gap=2)
```

What's different from v1, field by field:

- `sections`: v2 objects carry `LABEL_DICT`, `SERIES_IDS`, and
  `SERIES_META` in place of v1's `SERIES_TABLE` (`kind=2`, never emitted by
  v2); `TS_PAGES`/`VAL_PAGES` are unchanged. Each line also carries a
  resolved `name=` for its `kind`, which v1's `sections` output doesn't
  print (v1's `sections` block is intentionally unchanged by v2 support;
  see the v1 example above).
- `schema_count (derived)` and the `schema[N]:` lines: SERIES_META groups
  series by their distinct label-*name* sets (a "schema"); each line lists
  that schema's label names, sorted, resolved through `LABEL_DICT`. This
  count is derived by `ravel-cli` from the decoded per-series label sets,
  not read directly from SERIES_META's on-disk schema dictionary (that
  dictionary isn't part of `ravel-segment`'s public decode API) -- for any
  segment this system's own writer produced, that's the same number,
  since every schema it writes is referenced by at least one series.
- `series`: one line per series, the v2 counterpart of what v1's
  `series_count (decoded)` count represents without enumerating -- series
  id, its resolved labels, sample count, event-timestamp bounds, and the
  **absolute** byte ranges of its TS/VAL pages (`ts_range`/`val_range`,
  half-open `[start, end)`), reconstructed from SERIES_META's
  gap/length columns the same way the reader itself does before fetching
  those bytes. A trailing `ALIGNMENT_GAP(ts_page_gap=…, val_page_gap=…)`
  appears only when either gap is nonzero: v2 pads `VAL_PAGES` so every
  raw-f64-encoded page starts 8-byte aligned (docs/segment-format.md
  "VAL_RAW_F64 page alignment, v2"), and this is where that padding shows
  up. Series `0x33...` and `0x44...` above both needed `val_page_gap`
  padding; neither needed `ts_page_gap` padding (TS pages are never
  aligned).

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
segment_format_version: 1
created_unix_ns: 1732400060123456789
ingest_hour_bucket: 2025112718
```

A commit record never holds sample data itself; it's a small pointer plus
enough metadata to prune without opening the segment. `object_key` and
`object_size` name the segment this record publishes; `content_hash` is
the blake3 hash embedded in that segment's own key (`hash16` above,
extended here to the full hash), which is how a retried commit PUT can
tell "already published, same content" (safe) from "already published,
different content" (a fatal split-brain, since two different segments
should never share a `(writer_id, epoch, seq)`) apart. `signal` is the
numeric signal code (`1` = metrics). `ingest_hour_bucket` is the same hour
encoded in the object's own key, and is what the catalog groups listings
by.
