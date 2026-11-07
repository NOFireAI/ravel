# Metrics codec portfolio bake-off

Report-only measurement per ADR-3694 decision 7. **Nothing in this report
changes a production writer, reader, or on-object encoding byte.** Adopting
any candidate named here requires its own ADR and an RSEG format version
bump (docs/segment-format.md); this document is evidence for that future
decision, not the decision itself.

Raw program output (both binaries, full stdout, plus the DeXOR fetch) is
committed beside this file as `codec-portfolio.raw.txt`. Every figure quoted
below, ratios included, is copied verbatim from that file. Line numbers are
not stable across runs, so figures are quoted by corpus, page size and codec.

## Method

- **Box**: AMD EPYC 7R13 Processor, 16 cores, 30.6 GiB memory, Linux
  7.0.0-1011-aws x86_64 (each binary prints this in its own header).
- **Toolchain**: rustc 1.97.1 (8bab26f4f 2026-07-14), release profile
  (`cargo run --release`).
- **Commit measured**: c727640a7ab62672ef151b5fff29eaa4a58f8bf7, printed by
  both binaries. It is a work-in-progress commit that is folded on merge;
  the code under `crates/` is identical in the commit that lands, which adds
  only this report and the raw file.
- **Adoption rule, per row** (printed by both binaries): a challenger is
  adopted on a row only if it round-trips bit-exactly, uses at most 75 % of
  production's bytes per sample, and decodes at least as fast as production.
  A candidate is proposed for adoption only where rows clear that rule.
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

Challengers:

- `dod_for`: delta-of-delta, zigzag varint for the first delta, then
  frame-of-reference bit-packing per 128-sample block.
- `gcd_forced_dod`: page divided by its GCD, then forced onto
  `encode_i64`'s double-delta arm alone, not its best-of search.

| corpus | n | production B/page | dod_for B/page | ratio | ratio_hdr | gcd_forced_dod B/page | ratio | ratio_hdr |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| regular_ms | 1 | 7 | 10 | 1.429 | 10.000 | 1 | 0.143 | 1.000 |
| regular_ms | 2 | 12 | 15 | 1.250 | 2.500 | 6 | 0.500 | 1.000 |
| regular_ms | 12 | 27 | 17 | 0.630 | 0.810 | 16 | 0.593 | 0.762 |
| regular_ms | 60 | 28 | 17 | 0.607 | 0.773 | 64 | 2.286 | 2.909 |
| regular_ms | 240 | 31 | 20 | 0.645 | 0.800 | 244 | 7.871 | 9.760 |
| regular_ms | 3600 | 97 | 74 | 0.763 | 0.813 | 3604 | 37.155 | 39.604 |
| jitter_ms | 3600 | 6651 | 14131 | 2.125 | 2.127 | 6641 | 0.998 | 0.999 |
| nanos_jitter | 3600 | 16823 | 14115 | 0.839 | 0.839 | 16815 | 1.000 | 1.000 |
| irregular | 12 | 28 | 70 | 2.500 | 3.182 | 16 | 0.571 | 0.727 |
| irregular | 3600 | 3616 | 16860 | 4.663 | 4.670 | 3604 | 0.997 | 0.998 |
| metricsbench_history | 12 | 27 | 17 | 0.630 | 0.810 | 16 | 0.593 | 0.762 |
| metricsbench_history | 60 | 40 | 275 | 6.875 | 8.088 | 64 | 1.600 | 1.882 |
| metricsbench_history | 240 | 54 | 584 | 10.815 | 12.167 | 244 | 4.519 | 5.083 |

Decode throughput on the rows the verdicts cite (dec Mts/s):

| corpus | n | production | dod_for | gcd_forced_dod |
|---|---:|---:|---:|---:|
| regular_ms | 60 | 61.5 | 416.7 | 381.4 |
| jitter_ms | 3600 | 135.6 | 107.1 | 307.4 |
| nanos_jitter | 3600 | 101.4 | 106.6 | 162.6 |
| irregular | 12 | 38.9 | 86.4 | 286.3 |
| irregular | 3600 | 187.5 | 98.5 | 428.7 |

**dod_for: reject.** It misses the byte bound on all 31 rows, so no row
adopts it. On `regular_ms`, header-corrected, it sits between 0.773 (n=60)
and 0.813 (n=3600) for every page of 12 or more samples, and at 10.000 (n=1)
and 2.500 (n=2) below that: the as-measured 0.607 to 0.645 at n=12, 60 and
240 was the 6-byte header, not the codec. Its best row anywhere is
`regular_ms` n=60 at ratio_hdr 0.773. It does beat production's bytes on
`nanos_jitter`: n=3600 is 3.9208 B/sample against production's 4.6731
(ratio_hdr 0.839), and n=60 and n=240 are 0.872 and 0.842, still above
0.75. On `jitter_ms`, `irregular` and `metricsbench_history` it is larger
than production (ratio_hdr 2.127 at `jitter_ms` n=3600, 4.670 at `irregular`
n=3600, 8.088 at `metricsbench_history` n=60), and on `jitter_ms` and
`irregular` at n=240 and n=3600 it also decodes slower than production
(`jitter_ms` n=3600: 107.1 against 135.6 Mts/s), a miss the throughput
caveat above cannot explain away.

**gcd_forced_dod: reject.** The per-row rule marks it as clearing on one row
of 31, `irregular` n=12: 16 bytes against production's 28 (ratio_hdr 0.727)
and 286.3 against 38.9 Mts/s. That row does not support adoption: it is one
12-sample page of a synthetic corpus, its decode win is the ambiguous kind
described under Method, and the already-shipping `enc_i64/gcd` path
produces the same 16 bytes on that row. Every other row misses on bytes. Its
best `regular_ms` row is n=12 at ratio_hdr 0.762 (0.593 as measured); at
n=1 and n=2 it equals production's payload (1.000 and 1.000); from n=60 up,
where production picks enc 1, it is far larger (2.909, 9.760, 39.604). As
measured it is
smaller than production on `jitter_ms` n=3600 (1.8447 against 1.8475
B/sample) and `irregular` n=3600 (1.0011 against 1.0044), but those leads
are the page header: ratio_hdr 0.999 and 0.998. On the real
`metricsbench_history` stream it misses at every page size (1.000, 1.000,
0.762, 1.882, 5.083 for n=1 to 240).

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
| constant | 60 | 0.0833 | 65.2 | 0.0667 | 0.800 | 706.8 | 0.0833 | 1.000 | 698.5 |
| constant | 500 | 0.0100 | 92.7 | 0.0080 | 0.800 | 957.0 | 0.0100 | 1.000 | 952.4 |
| sparse | 60 | 0.5333 | 59.0 | 1.3167 | 2.469 | 207.5 | 1.3333 | 2.500 | 206.9 |
| noisy_float | 500 | 8.0000 | 87.1 | 8.4500 | 1.056 | 85.4 | 7.5500 | 0.944 | 70.2 |
| float_counter(ctrl) | 500 | 8.0000 | 86.4 | 8.3420 | 1.043 | 86.4 | 7.2680 | 0.908 | 71.9 |
| metricsbench_history | 60 | 2.4833 | 45.4 | 5.1333 | 2.067 | 110.2 | 5.1500 | 2.074 | 110.3 |
| metricsbench_history | 500 | 2.2780 | 60.0 | 5.0160 | 2.202 | 118.2 | 5.0180 | 2.203 | 118.1 |

The binary's adoption summary prints `adopt: none` for all 18 rows (9
datasets at 2 page sizes).

**alp_vectorized: reject.** Its best ratio is 0.800 (`constant`, n=60 and
n=500), above 0.75; the decode prong clears there (706.8 against 65.2 and
957.0 against 92.7 Mval/s), so bytes alone decide it. On
`metricsbench_history` it is more than twice production's size (2.067 at
n=60, 2.202 at n=500).

**alp_rd: reject.** Its best ratios are 0.908 (`float_counter(ctrl)`, n=500)
and 0.944 (`noisy_float`, n=500), both above 0.75, and both rows also miss
on decode (71.9 against 86.4 and 70.2 against 87.1 Mval/s). On
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
