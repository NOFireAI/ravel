# ADR-0014: RSEG v2: compact columnar series catalog, raw-f64 page alignment

Status: Accepted (2026-07-27); superseded by ADR-0027 (2026-07-28):
RSEG v2 support was removed pre-release, and this ADR remains the
record of the v2 design. Plan and byte-exact layout:
docs/rseg-v2-plan.md. docs/segment-format.md stays the authoritative v1
spec until implementation phase 1 lands the v2 amendment; this ADR
records the decision, not the amendment.

## Context

Issue #16 optimized RSEG v1 encode within the frozen format and hit a
wall: at 200k samples, encode throughput falls from 17.11 Melem/s at 100
series to 538.6 Kelem/s at 100,000 series (31.8x, BENCHMARKS.md). The
profiled remainder is catalog metadata, not sample encoding: SERIES_TABLE
raw is 7.6 MB at 100k series (larger than the page data), and the
dominant costs are the interner pass (~108 ms), the distinct-string sort
mandated by v1's sorted LABEL_DICT rule (~48 ms), zstd -3 over 13.2 MB of
catalog sections (~95 ms), and byte emission (~150 ms). The v1 grammar
forces ~76 bytes per series entry: fixed-width u32/i64 fields, per-entry
label-name ordinals, absolute page offsets, and 16 incompressible id
bytes fed through zstd.

Separately, ADR-0013 recorded that VAL_RAW_F64 payloads are unaligned in
v1, blocking Arrow zero-copy views, and deferred the alignment question
to an RSEG v2 decision; the X1 measurement (docs/x1-alignment-measurement.md,
raw-f64 = 8.78% of VAL pages, 10.71% of bytes) concluded alignment alone
does not justify opening the format-change procedure.

## Alternatives

1. Do nothing. Accept ~539 Kelem/s per writer at 100k-series flushes.
   High-cardinality tenants make segment build the shard-actor
   bottleneck on the flush path, and every query pays a 7.6 MB catalog
   scan per such segment. Rejected: the ceiling is format-mandated and
   grows with cardinality.
2. Writer-policy tricks only, no format change. Issue #16 already
   exhausted the in-format space (single-pass interning, scratch reuse,
   in-place page encode, lz4 floor). The one untried policy lever is
   dropping section compression (comp is per-section already): storing
   SERIES_TABLE uncompressed saves at most the ~95 ms zstd bucket
   (~1.33x) while growing the object by ~4.7 MB at this shape, inflating
   every catalog GET and the write path. It cannot touch the sort
   (format rule), the ~76-byte entry grammar, or emission. Rejected as a
   dead end; its one sound insight (never zstd the incompressible id
   column) is incorporated into the v2 layout instead.
3. Row-compact v2: keep the v1 row layout, replace fixed-width fields
   with varints and deltas in place. Smaller, minimal reader change, but
   ids stay interleaved with compressible data (zstd still eats them),
   columns cannot be skipped during lazy matching, and like-typed bytes
   never group for compression. Strictly dominated by option 4 at
   similar implementation cost.
4. Columnar v2 with schema sharing: split the catalog into SERIES_IDS
   (raw sorted ids, uncompressed) and SERIES_META (a schema dictionary
   of label-name lists, then per-field varint columns: schema refs,
   value ordinals, sample counts, footer-relative timestamp bounds,
   gap/len page addressing), and relax the LABEL_DICT ordering rule so
   the writer can skip the distinct-string sort. ~42 bytes/entry on the
   bench shape (vs ~76), zstd input for the table drops from 7.6 MB to
   ~2.6 MB, and the decoded output remains the existing `SeriesEntry`,
   keeping ravel-query version-blind.

Delta-encoding the series ids themselves was analyzed and rejected:
sorted BLAKE3 ids are uniform random, expected adjacent gaps at 100k
entries need 16-byte varints (worse than raw), and prefix sharing saves
~2 bytes while forfeiting fixed stride.

## Decision

Option 4, as RSEG version 2 (trailer version = 2, magic unchanged), plus
bundling the VAL_RAW_F64 8-byte alignment rule from the ADR-0013 track:
the version bump is being paid anyway, the marginal cost is a <= 7-byte
zero pad on ~9% of VAL pages carried by the gap columns that exist
regardless, and not bundling would force a third version later. X1's
no-go on opening the procedure for alignment alone stands; this is
opening it for the encode wall and amortizing.

Page format, encodings, checksum mechanics, suffix protocol, identity,
and key layout are unchanged. Protobuf changes are additive enum values
only (section kinds 5 and 6); commit records already carry
segment_format_version. Measured targets, the byte-exact grammar, the
checksum coverage table, and the six-phase implementation plan with
per-phase gates are in docs/rseg-v2-plan.md.

## Consequences

- Encode at 100k series improves an expected 1.4x-1.6x (hard gate 1.4x
  on the BENCHMARKS.md host; the 32x gap narrows to roughly 20-23x).
  The original within-8x goal remains out of reach of any catalog
  layout: the interner (~108 ms) and per-series page framing survive by
  design and are named writer-side follow-ups.
- Read side: catalog raw bytes to decompress and scan drop ~45% at high
  cardinality, lazy matching skips whole columns, and raw-f64 pages
  become alignment-eligible for the Arrow track (fetch-side aligned
  buffers remain that track's work).
- Dual readers forever, or until a compactor exists: Phase 1 never
  rewrites objects, so v1 stays readable indefinitely and the v1 decode
  path stays under CI permanently. Readers deploy before writers; the
  writer default flips to v2 only after the fleet reads v2, and
  rollback is a config flip that strands nothing.
- Two grammars in the fuzz/property matrix, the inspector, and the
  golden corpus, permanently; the plan's phases 4 and 5 carry that
  cost explicitly.
