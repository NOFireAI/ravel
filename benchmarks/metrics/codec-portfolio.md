# Metrics codec portfolio bake-off

Report-only measurement per ADR-3694 decision 7. **Nothing in this report
changes a production writer, reader, or on-object encoding byte.** Adopting
any candidate named here requires its own ADR and an RSEG format version
bump (docs/segment-format.md); this document is evidence for that future
decision, not the decision itself.

Raw program output (both binaries, full stdout) is committed beside this
file as `codec-portfolio.raw.txt`. Every figure quoted below is copied
verbatim from that file; line numbers are not stable across runs, so figures
are quoted by corpus/row instead.

## Method

- **Box**: Raspberry Pi 5 Model B Rev 1.1, 4 cores (`nproc`), 7.9 GiB memory,
  Linux 6.12.25+rpt-rpi-2712 aarch64.
- **Toolchain**: rustc 1.97.1 (8bab26f4f 2026-07-14), release profile
  (`cargo run --release`).
- **Commit measured**: ccc99d50c0a6050fdb145df3542de878393f7b74 (matches the
  tree both binaries ran against; printed by each binary into its own
  header).
- **Seeds**: `metrics_stream_sample::sample_series` draws from
  `benchmarks/metrics/workload.json` with the manifest seed `927000933`,
  bounded to 8 series for `codec_bakeoff`'s `metricsbench_history` dataset
  and to 1 series for `ts_bakeoff`'s `metricsbench_cardinality` /
  `metricsbench_history` streams. Synthetic streams (`regular_ms`,
  `jitter_ms`, `nanos_jitter`, `irregular`, `out_of_order`,
  `counter_int_resets`, `gauge_int`, `gauge_dec2`, `gauge_dec3`,
  `noisy_float`, `constant`, `sparse`, `float_counter(ctrl)`) are
  deterministic generators, not RNG-seeded.
- **Page sizes**: timestamps use `PAGE_LENS = [1, 2, 12, 60, 240, 3600]`
  samples/page (`ts_bakeoff.rs`); values use `PAGE_SIZES = [60, 500]`
  samples/page (`codec_bakeoff.rs`).
- **Rounds**: `codec_bakeoff` reports the median of `MEASURE_ROUNDS = 9`
  timed rounds, each round averaging `INNER_REPS = 4000` encode/decode
  calls, single-threaded. `ts_bakeoff` reports bytes/sample and bit-exact
  round-trip only; it does not instrument encode/decode throughput for
  timestamp codecs (see "Limitation" below).
- **Adoption rule** (both bakeoffs, stated in each binary's own header):
  a challenger is adopted only if it (a) round-trips bit-exactly, (b) uses
  `<= 75%` of production bytes/sample (`BYTE_RATIO_MAX = 0.75` in
  `codec_bakeoff.rs`), and (c) decodes at least as fast as production.
- **Limitation**: `ts_bakeoff` measures bytes/sample and round-trip
  correctness for `dod_for` and `gcd_forced_dod` but never measures their
  decode throughput, so prong (c) of the adoption rule is not evaluated for
  either timestamp challenger. A challenger that clears (a) and (b) is
  still not adopted below, because (c) cannot be confirmed from this run.
  Adding that instrumentation is follow-up work, not done here.

## Timestamp codecs (`ts_bakeoff`, ADR-0092 decision 6 / ADR-3694 decision 7)

Two new challengers, both proposed replacements for or additions to the
existing `encode_i64` best-of search (`enc_i64` / `enc_i64/gcd` columns,
already shipping):

- `dod_for`: delta-of-delta, zigzag-varint for the first delta, then
  frame-of-reference bit-packing per 128-sample block (`DOD_BLOCK_LEN`).
- `gcd_forced_dod`: page divided by its GCD, then forced onto
  `encode_i64`'s double-delta arm alone, never its best-of search.

Bytes/sample by corpus and page size (production bytes include the 6-byte
RSEG page header; every challenger column is raw codec output with no page
header, per the raw file's own header note):

| corpus | n | production | dod_for | ratio | gcd_forced_dod | ratio |
|---|---:|---:|---:|---:|---:|---:|
| regular_ms | 12 | 2.2500 | 1.4167 | 0.629 | 1.3333 | 0.593 |
| regular_ms | 60 | 0.4667 | 0.2833 | 0.607 | 1.0667 | 2.286 |
| regular_ms | 240 | 0.1292 | 0.0833 | 0.645 | 1.0167 | 7.869 |
| regular_ms | 3600 | 0.0269 | 0.0206 | 0.766 | 1.0011 | 37.216 |
| jitter_ms | 3600 | 1.8475 | 3.9253 | 2.125 | 1.8447 | 0.998 |
| nanos_jitter | 3600 | 4.6731 | 3.9208 | 0.839 | 4.6708 | 1.000 |
| irregular | 3600 | 1.0044 | 4.6833 | 4.663 | 1.0011 | 0.997 |
| out_of_order | 3600 | 0.8267 | 4.6639 | 5.641 | 1.0011 | 1.211 |
| metricsbench_history | 60 | 0.6667 | 4.5833 | 6.875 | 1.0667 | 1.600 |

**dod_for: reject.** On the exactly-regular stream it clears the 75%
bar at n=12/60/240 (ratio 0.607-0.645) but *misses it at n=3600*
(0.0206 / 0.0269 = **0.766**, quoted from the `regular_ms` n=3600 row), the
largest and most realistic page size for that corpus. On every
non-monotonic-friendly corpus it is worse than production, not better:
`jitter_ms` n=3600 is 3.9253 vs production 1.8475 (ratio 2.125, over double),
`irregular` n=3600 is 4.6833 vs 1.0044 (ratio 4.663), `out_of_order` n=3600
is 4.6639 vs 0.8267 (ratio 5.641), and `metricsbench_history` n=60 is 4.5833
vs 0.6667 (ratio 6.875). The adoption rule is "must not regress the
exactly-regular stream" *and* hold generally; dod_for fails the first at the
largest page size tested and fails the second everywhere jitter,
irregularity, or out-of-order arrival appears. Decode throughput is also
unmeasured (see Limitation), so even the regular-stream wins at smaller n
cannot clear prong (c).

**gcd_forced_dod: reject.** It never beats production and rarely beats the
already-shipping `enc_i64/gcd` best-of path it is derived from. Deciding
figure: on `jitter_ms` n=3600 it is bit-identical in bytes to `enc_i64/gcd`
(1.8447 vs 1.8447, both quoted from that row), meaning the forced
double-delta arm wins the existing best-of search on its own anyway and
`gcd_forced_dod` adds nothing; on `regular_ms` it is far worse than
production at every page size (e.g. n=3600: 1.0011 vs production's 0.0269,
ratio 37.2), because forcing double-delta throws away the GCD-scaling
production already exploits. No row in the raw file shows `gcd_forced_dod`
at or below 0.75 of production.

## Value codecs (`codec_bakeoff`, ADR-0092 decision 6)

Two new challengers (`AlpVectorized`, `AlpRd`), alongside the pre-existing
challenger set (`alp`, `gcd_delta_for`, `chimp128`, `bytesplit_zstd`,
`raw_f64`), measured against whatever production actually selects per
dataset (enc 16 Gorilla, 17 raw, 18 one-exponent ALP, or 19 GCD-delta-FOR).

Selected rows (full table in the raw file; `ratio` = challenger bytes /
production bytes):

| dataset | n | production B/s | alp_vectorized B/s | ratio | alp_rd B/s | ratio |
|---|---:|---:|---:|---:|---:|---:|
| constant | 60 | 0.0833 | 0.0667 | 0.800 | 0.0833 | 1.000 |
| constant | 500 | 0.0100 | 0.0080 | 0.800 | 0.0100 | 1.000 |
| sparse | 60 | 0.5333 | 1.3167 | 2.469 | 1.3333 | 2.500 |
| noisy_float | 500 | 8.0000 | 8.4500 | 1.056 | 7.5500 | 0.944 |
| float_counter(ctrl) | 500 | 8.0000 | 8.3420 | 1.043 | 7.2680 | 0.908 |
| metricsbench_history | 500 | 2.2780 | 5.0160 | 2.202 | 5.0180 | 2.203 |

**alp_vectorized: reject.** Binary's own adoption summary prints
`adopt: none` for every dataset/page-size combination (`codec-portfolio.raw.txt`,
"adoption summary" section). Deciding figure: its single best ratio across
the whole run is 0.800 (`constant`, both n=60 and n=500), which is above the
0.75 cutoff; on the real-traffic-derived `metricsbench_history` dataset it
is *worse* than production by more than 2x (ratio 2.067-2.202 across both
page sizes).

**alp_rd: reject.** Same binary-printed `adopt: none` for every row. Best
ratios are 0.908 (`float_counter(ctrl)`, n=500) and 0.944 (`noisy_float`,
n=500), both above 0.75; on `metricsbench_history` it is worse than
production by more than 2x (ratio 2.074-2.203), the same corpus
`alp_vectorized` also loses on.

Net result: the adoption summary printed by `codec_bakeoff` itself shows
`adopt: none` for all 9 datasets at both page sizes (18 rows total,
reproduced verbatim in the raw file's "adoption summary" section). Nothing
here proposes a change to the shipping value-codec portfolio.

## DeXOR (arXiv 2601.00695)

**Not evaluated: the paper was not reachable from the executor.** Both
`WebFetch` and a direct `curl -m 15 https://arxiv.org/abs/2601.00695` from
this box failed; the curl attempt returned `HTTP_CODE=000` with
`curl: (28) Connection timed out after 15001 milliseconds` (exit 28),
indicating the executor has no outbound network path to arxiv.org, not a
paper-specific failure. Nothing is implemented under the DeXOR name.

## Class closure: fixed-row coverage in ravel-segment

Every production value-codec and timestamp-codec proptest/fixed-value test
in `ravel-segment`, and whether it carries the stale-marker / hard-float
fixed rows (`0x7ff0_0000_0000_0002` and friends) this bake-off's adoption
rule depends on:

| File | Test | Carries fixed rows? |
|---|---|---|
| `value_codecs.rs` | `alp_roundtrips_hard_floats_bit_exactly`, `gcd_for_roundtrips_hard_floats_bit_exactly` (via `hard_floats()`) | Yes — pre-existing, predates this task (from `6a191f504`) |
| `value_codecs.rs` | `roundtrip_arbitrary_floats`, `mutation_never_panics` (proptest) | Arbitrary `f64`, includes NaN bit patterns by construction |
| `gorilla.rs` | `special_values_roundtrip_exactly` | Yes — extended this task's earlier commits with the stale marker and an additional NaN payload |
| `gorilla.rs` | `proptest!` block (`any_f64`-style `choice`/`raw` strategy) | Yes, structurally — covers the stale-marker bit pattern as part of full `u64` coverage |
| `writer.rs` | `hard_values_round_trip_through_production_selection` (via `hard_values()`) | Yes — added this task's earlier commits; exercises the real write path (`frame_run` -> `encode_run_v4` -> `frame_val_page`'s codec selection) and `decode_run`, not a bare codec call |
| `writer.rs` | `val_page_never_larger_than_incumbent` (proptest, `any::<f64>()`) | Arbitrary `f64`, includes NaN by construction |
| `writer.rs` | `ts_page_never_larger_than_incumbent`, `direct_v7_emit_bit_parity` | N/A — `i64` timestamp domain, no float stale-marker concern |
| `ts_gcd.rs`, `ts_delta.rs`, `varint.rs`, `exemplars.rs` | (not modified; `i64`/varint domain) | N/A — these encode integers, not floats; the stale-marker/NaN concern does not apply |

**Discrepancy to report, not silently fixed**: the dispatch's premise that
"no production proptest pins [the stale marker] today" does not hold for
`value_codecs.rs` — `hard_floats()` and its two consuming tests already
pinned `0x7ff0_0000_0000_0002` before this task started (git blame:
`6a191f504`, "feat(segment): adopt ALP, GCD-delta-FOR, and GCD timestamp
page codecs"). This task's own commits correctly extended coverage to
`gorilla.rs` and `writer.rs`, which genuinely lacked it.
