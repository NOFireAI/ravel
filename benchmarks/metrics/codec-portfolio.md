# Metrics codec portfolio bake-off

Report-only measurement per ADR-3694 decision 7. **Nothing in this report
changes a production writer, reader, or on-object encoding byte.** Adopting
any candidate named here requires its own ADR and an RSEG format version
bump (docs/segment-format.md); this document is evidence for that future
decision, not the decision itself.

Raw program output (both binaries, full stdout, plus the DeXOR fetch) is
committed beside this file as `codec-portfolio.raw.txt`. Every figure quoted
below, ratios included, is copied verbatim from that file, except where the
text says it subtracts production's 6-byte page header or takes a byte
difference, and then the operands are quoted beside it. Line numbers are
not stable across runs, so figures are quoted by corpus, page size and codec.

## Method

- **Box**: AMD EPYC 7R13 Processor, 16 cores, 30.6 GiB memory, Linux
  7.0.0-1011-aws x86_64 (each binary prints this in its own header).
- **Toolchain**: rustc 1.97.1 (8bab26f4f 2026-07-14), release profile
  (`cargo run --release`).
- **Commit measured**: ec00b05effaaaf6193415d213e8f8796285d169b, printed by
  both binaries. It is a work-in-progress commit that is folded on merge.
  `crates/ravel-bench` (both bakeoffs and their challengers) and
  `crates/ravel-segment` (production's writer and reader) are identical in
  the commit that lands, which adds only this report and the raw file;
  other crates on `main` may have moved since.
- **Adoption rule, per row** (printed by both binaries): a challenger is
  adopted on a row only if it round-trips bit-exactly, uses at most 75 % of
  production's payload bytes, and decodes at least as fast as production.
  Timestamp rows are judged on `ratio_hdr` (challenger bytes over production
  bytes minus the 6-byte page header); value rows are judged on `ratio`,
  because `codec_bakeoff` already excludes that header from production.
  The report names every row that clears that rule; clearing it does not
  adopt anything (see the `dod_for` verdict).
- **Page sizes**: timestamps use `PAGE_LENS = [1, 2, 12, 60, 240, 3600]`
  samples per page; values use `PAGE_SIZES = [60, 500]`.
- **Rounds**: `ts_bakeoff` decodes each page repeatedly until a round covers
  400,000 timestamps (`SAMPLES_PER_ROUND`), after three warm-up calls, and
  reports the median of 9 rounds (`MEASURE_ROUNDS`) in millions of
  timestamps per second. `codec_bakeoff` reports the median of 9 timed
  rounds, each averaging 4000 encode or decode calls (`INNER_REPS`). All
  throughput is single-threaded.
- **Production decode is timed through the whole run.** Both binaries
  decode production through `decode_scalar_run`, which decodes the TS page
  and a VAL page and frames the run, while a challenger decodes only its own
  payload. A challenger whose figure is below production's is slower than
  production; a challenger whose figure is above production's is not proven
  faster than a TS-only or VAL-only production decode.
- **Peak memory: not measured.** No existing workspace dependency reports a
  peak (`stats_alloc` counts cumulative totals); a peak-tracking global
  allocator needs `unsafe`, which the workspace denies; and any instrumented
  global allocator adds per-allocation cost to the decode timings the rule
  judges. No new dependency was added for it.

### Sample counts

- Synthetic timestamp streams (`regular_ms`, `jitter_ms`, `nanos_jitter`,
  `irregular`) and synthetic value shapes are generated at exactly `n`
  samples for each page, deterministically (seeded from the stream name and
  page size, or seed 0 for value shapes).
- No unsorted timestamp corpus is measured. An earlier version of
  `ts_bakeoff` had a synthetic `out_of_order` stream; it was removed because
  `encode_run_v4` requires its values sorted by timestamp
  (`crates/ravel-segment/src/writer.rs`, its doc comment), so production
  never encodes an unsorted TS page and that row measured an input no writer
  produces.
- `metricsbench_cardinality` timestamps: seed=927000933, steps=8, series=1,
  samples=8, so only the n=1 and n=2 pages exist.
- `metricsbench_history` timestamps: seed=927000933, steps=256, series=1,
  samples=253 (from 256 steps), 0 out of order, 0 duplicates dropped, so
  pages run up to n=240. The stream is
  sorted and deduplicated before measurement because a run reaches the
  writer in timestamp order with one sample per timestamp.
- `metricsbench_history` values: the page joins up to 8 series end to end in
  first-encountered order until it holds `n` floats. This run joined 2
  series at n=60 and 2 series at n=500 (printed above each page block).

## Timestamp codecs (`ts_bakeoff`)

Production is the writer's own TS selection: it keeps the smaller of enc 1
(TS_DELTA_VARINT, LZ4 when that shrinks a payload of at least 64 bytes) and
enc 2 (TS_GCD_I64). The `note` column shows which one it picked. Production
bytes include the 6-byte RSEG page header; challenger bytes are raw payload.
`ratio` is as measured; `ratio_hdr` divides by production's bytes minus 6,
so the comparison is payload against payload. Verdicts use `ratio_hdr`.

Rows:

- `enc_i64/gcd`: production enc 2's payload measured on its own, in enc 2's
  grammar (docs/segment-format.md): `uvarint gcd`, one `Enc` tag byte, then
  `encode_i64` of the page divided by its GCD. It is a reference, not a
  challenger, and the summary does not judge it. Wherever production picks
  enc 2 it equals production's payload byte for byte (ratio_hdr 1.000 on all
  13 such rows; `irregular` n=12 is 22 bytes against production's 28).
- `dod_for` (challenger a): zigzag varint first value, zigzag varint first
  delta, then each later delta's difference from the previous delta,
  frame-of-reference bit-packed per 128-sample block (each block a zigzag
  varint minimum, a bit-width byte, then the packed offsets). It carries no
  sample count: production's TS page stores none (the reader takes it from
  the run entry), and every row here decodes with the count supplied from
  outside.
- `gcd_forced_dod` (challenger b): `uvarint gcd`, then the page divided by
  that GCD forced onto `encode_i64`'s double-delta arm alone, not its
  best-of search.

Both GCD rows carry their divisor inside the payload, as enc 2 does, so
every row compares payload against payload.

| corpus | n | production B/page | enc_i64/gcd B/page | dod_for B/page | ratio | ratio_hdr | gcd_forced_dod B/page | ratio | ratio_hdr |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| regular_ms | 1 | 7 | 11 | 9 | 1.286 | 9.000 | 10 | 1.429 | 10.000 |
| regular_ms | 2 | 12 | 12 | 14 | 1.167 | 2.333 | 11 | 0.917 | 1.833 |
| regular_ms | 12 | 27 | 21 | 16 | 0.593 | 0.762 | 21 | 0.778 | 1.000 |
| regular_ms | 60 | 28 | 70 | 16 | 0.571 | 0.727 | 69 | 2.464 | 3.136 |
| regular_ms | 240 | 31 | 250 | 18 | 0.581 | 0.720 | 249 | 8.032 | 9.960 |
| regular_ms | 3600 | 97 | 3610 | 72 | 0.742 | 0.791 | 3609 | 37.206 | 39.659 |
| jitter_ms | 3600 | 6651 | 6645 | 14129 | 2.124 | 2.126 | 6644 | 0.999 | 1.000 |
| nanos_jitter | 12 | 62 | 63 | 58 | 0.935 | 1.036 | 62 | 1.000 | 1.107 |
| nanos_jitter | 3600 | 16823 | 16817 | 14113 | 0.839 | 0.839 | 16816 | 1.000 | 1.000 |
| irregular | 12 | 28 | 22 | 69 | 2.464 | 3.136 | 21 | 0.750 | 0.955 |
| irregular | 3600 | 3616 | 3610 | 16858 | 4.662 | 4.670 | 3609 | 0.998 | 1.000 |
| metricsbench_history | 12 | 27 | 21 | 16 | 0.593 | 0.762 | 21 | 0.778 | 1.000 |
| metricsbench_history | 60 | 40 | 70 | 274 | 6.850 | 8.059 | 69 | 1.725 | 2.029 |
| metricsbench_history | 240 | 54 | 250 | 582 | 10.778 | 12.125 | 249 | 4.611 | 5.188 |

Decode throughput on the rows the verdicts cite (dec Mts/s):

| corpus | n | production | dod_for | gcd_forced_dod |
|---|---:|---:|---:|---:|
| regular_ms | 60 | 64.6 | 482.9 | 394.6 |
| regular_ms | 240 | 86.5 | 580.7 | 422.4 |
| jitter_ms | 3600 | 134.6 | 119.8 | 308.7 |
| nanos_jitter | 3600 | 101.8 | 120.1 | 147.4 |
| irregular | 12 | 40.9 | 91.4 | 279.9 |
| irregular | 240 | 150.6 | 106.8 | 422.5 |
| irregular | 3600 | 193.9 | 108.3 | 457.2 |

**dod_for: the rule clears it on two rows, `regular_ms` n=60 and
`regular_ms` n=240, and on no other row.** The binary's adoption summary
prints, verbatim:

```
  regular_ms                  60 adopt: dod_for          dod_for=clears  gcd_forced_dod=miss(bytes)
  regular_ms                 240 adopt: dod_for          dod_for=clears  gcd_forced_dod=miss(bytes)
```

At n=60 it is 16 bytes against production's 28 (22 without the header):
ratio_hdr 0.727, decoding at 482.9 against production's 64.6 Mts/s. At
n=240 it is 18 bytes against 31 (25 without the header): ratio_hdr 0.720,
decoding at 580.7 against 86.5 Mts/s. Both round-trip bit-exactly. All
three prongs clear on both rows.

This report does not adopt `dod_for`. ADR-3694 decision 7 makes any
adoption its own ADR with an RSEG format version bump, and the summary's
`adopt:` column is the bakeoff's per-row rule, not that decision. That ADR
would have to answer at least:

- The two clearing rows are exactly regular millisecond streams: every
  delta in `regular_ms` is the same 15 s, so every delta-of-delta is zero
  and each 128-sample block packs at bit width 0. How much of production's
  real traffic has that shape.
- The result on every other corpus. `dod_for` misses the byte bound on the
  other 29 rows. Its nearest misses are `regular_ms` n=12 and
  `metricsbench_history` n=12 (both 16 bytes against 27, ratio_hdr 0.762)
  and `regular_ms` n=3600 (72 against 97, ratio_hdr 0.791). On the real
  `metricsbench_history` stream it is larger than production's payload at
  n=1, 2, 60 and 240 (9.000, 2.333, 8.059, 12.125). On `nanos_jitter` it
  is smaller than production's payload from n=60 up (ratio_hdr 0.869 at
  n=60, 0.840 at n=240, 0.839 at n=3600, where it is 3.9203 B/sample
  against production's 4.6731), all above 0.75, and larger at n=1, 2 and
  12 (9.000, 2.333, 1.036). It is larger than production's payload at
  every page size on `jitter_ms` (ratio_hdr 2.126 at n=3600) and
  `irregular` (4.670 at n=3600). On `jitter_ms` n=3600 and `irregular`
  n=240 and n=3600 it also decodes slower than production (119.8 against
  134.6, 106.8 against 150.6, 108.3 against 193.9 Mts/s), a miss the
  throughput caveat above cannot explain away.
- Whether production's own selection already captures most of the gain.
  On both clearing rows production picks enc 1 (`enc=1`), and enc 1 already
  holds the page in 0.4667 B/sample at n=60 and 0.1292 B/sample at n=240,
  header included; `dod_for` takes those to 0.2667 and 0.0750, a saving of
  12 and 13 bytes per page as measured (6 and 7 bytes of payload). Whether
  enc 1 plus LZ4, or enc 2, gets within that margin on the pages real
  writers seal is a question for that ADR's own measurement, not this
  one's.

**gcd_forced_dod: reject.** It misses the byte bound on all 31 rows, so no
row adopts it. Its best row is `irregular` n=12: 21 bytes against
production's 28 (ratio_hdr 0.955). An earlier run of this report left
the `uvarint gcd` (here the 1000000000 divisor) and the tag byte out of both
GCD rows while production's enc 2 payload carried them, and so marked this
row as clearing; the whole lead was that uncharged overhead. Charged for its
divisor, its lead over production comes only from dropping
enc 2's tag byte and best-of search: on the 13 rows where production picks
enc 2 it is 0 or 1 byte smaller than production's payload (21 against 21 on
`regular_ms` n=12, 6644 against 6645 on `jitter_ms` n=3600, 3609 against
3610 on `irregular` n=3600). On every row where production picks enc 1 it
is larger than production's payload: `regular_ms` 3.136, 9.960 and 39.659
at n=60, 240 and 3600, and 10.000 on every n=1 page, where its divisor
alone outweighs enc 1's 1-byte payload. On the real `metricsbench_history`
stream it misses at every page size (10.000, 1.833, 1.000, 2.029, 5.188
for n=1 to 240).

## Value codecs (`codec_bakeoff`)

Production is the writer's own VAL selection among enc 16 (Gorilla), 17
(raw f64), 18 (ALP, one exponent per page) and 19 (GCD-delta-FOR); each
block prints which one it picked. Production bytes here exclude the 6-byte
page header, so `ratio` needs no header correction. Two new challengers,
`alp_vectorized` and `alp_rd`, run alongside the earlier set (`alp`,
`gcd_delta_for`, `chimp128`, `bytesplit_zstd`, `raw_f64`).

`alp_vectorized` picks one decimal exponent per 1024-value vector and no
factor; it is not the ALP paper's exponent-and-factor search.

| dataset | n | production B/sample | dec Mval/s | alp_vectorized B/sample | ratio | dec Mval/s | alp_rd B/sample | ratio | dec Mval/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| constant | 60 | 0.0833 | 64.9 | 0.0667 | 0.800 | 696.3 | 0.0833 | 1.000 | 701.9 |
| constant | 500 | 0.0100 | 92.7 | 0.0080 | 0.800 | 956.2 | 0.0100 | 1.000 | 956.8 |
| sparse | 60 | 0.5333 | 59.4 | 1.3167 | 2.469 | 207.2 | 1.3333 | 2.500 | 207.0 |
| noisy_float | 500 | 8.0000 | 87.2 | 8.4500 | 1.056 | 85.4 | 7.5500 | 0.944 | 72.1 |
| float_counter(ctrl) | 500 | 8.0000 | 85.9 | 8.3420 | 1.043 | 86.1 | 7.2680 | 0.908 | 74.0 |
| metricsbench_history | 60 | 2.4833 | 46.0 | 5.1333 | 2.067 | 110.2 | 5.1500 | 2.074 | 85.4 |
| metricsbench_history | 500 | 2.2780 | 60.1 | 5.0160 | 2.202 | 118.1 | 5.0180 | 2.203 | 118.1 |

The binary's adoption summary prints `adopt: none` for all 18 rows (9
datasets at 2 page sizes).

**alp_vectorized: reject.** Its best ratio is 0.800 (`constant`, n=60 and
n=500), above 0.75; the decode prong clears there (696.3 against 64.9 and
956.2 against 92.7 Mval/s), so bytes alone decide it. On
`metricsbench_history` it is more than twice production's size (2.067 at
n=60, 2.202 at n=500).

**alp_rd: reject.** Its best ratios are 0.908 (`float_counter(ctrl)`, n=500)
and 0.944 (`noisy_float`, n=500), both above 0.75, and both rows also miss
on decode (74.0 against 85.9 and 72.1 against 87.2 Mval/s). On
`metricsbench_history` it is 2.074 (n=60) and 2.203 (n=500).

## DeXOR (arXiv 2601.00695)

**Not evaluated in this report; tracked in issue #3925.** The abstract page
was reachable from this box for this run (`HTTP_CODE=200`; the command and
its output are at the end of the raw file). The earlier run of this report,
on a different executor, timed out on the same fetch. Nothing is implemented
under the DeXOR name here.

## Class closure: fixed-row coverage in ravel-segment

Production value-codec and timestamp-codec tests in `ravel-segment`, and
whether they pin the stale-marker and hard-float rows (`0x7ff0_0000_0000_0002`
and the others in `hard_values()`):

| File | Test | Pins the fixed rows? |
|---|---|---|
| `value_codecs.rs` | `alp_roundtrips_hard_floats_bit_exactly`, `gcd_for_roundtrips_hard_floats_bit_exactly` (via `hard_floats()`) | Yes, from before this task (`6a191f504`) |
| `value_codecs.rs` | `roundtrip_arbitrary_floats` (proptest, `any::<f64>()`) | No: arbitrary `f64`, so a given NaN payload is not reliably drawn |
| `value_codecs.rs` | `mutation_never_panics` (proptest) | No: integer-valued seeds with a flipped bit or truncation; a corruption test, not a float round trip |
| `gorilla.rs` | `special_values_roundtrip_exactly` | Yes, extended in this task with the stale marker and a second NaN payload |
| `gorilla.rs` | `proptest!` block (`any_f64` strategy, raw `u64` bits in one arm) | No: random sampling does not hit the stale-marker pattern; the fixed rows above pin it |
| `writer.rs` | `hard_values_round_trip_through_production_selection` (via `hard_values()`) | Yes, for one 12-value page framed through `frame_run` and `encode_run_v4` and read back with `decode_run`. It checks the single VAL encoding selection picks for that page (named in the failure message), not every encoding selection can reach |
| `writer.rs` | `val_page_never_larger_than_incumbent` (proptest, `any::<f64>()`) | No: arbitrary `f64` |
| `writer.rs` | `ts_page_never_larger_than_incumbent`, `direct_v7_emit_bit_parity` | Not applicable: `i64` timestamp domain |
| `ts_gcd.rs`, `ts_delta.rs`, `varint.rs`, `exemplars.rs` | (not modified) | Not applicable: integer encodings |

**Discrepancy to report, not silently fixed**: the dispatch's premise that
no production test pinned the stale marker does not hold for
`value_codecs.rs`: `hard_floats()` and its two consuming tests already
pinned `0x7ff0_0000_0000_0002` before this task started (`6a191f504`,
"feat(segment): adopt ALP, GCD-delta-FOR, and GCD timestamp page codecs").
This task extended fixed-row coverage to `gorilla.rs` and `writer.rs`.
