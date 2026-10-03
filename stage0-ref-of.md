# Stage 0: ref_of share of RLOG row-path encode (issue #2426, epic #2425)

Host: arm64 (uname -m), nproc 15, MemTotal 25769803776 bytes (24 GiB, Darwin, `sysctl hw.memsize`). CARGO_BUILD_JOBS=3.

uptime before: `10:55  up 6 days,  7:43, 19 users, load averages: 5.35 4.47 3.91`
uptime after:  `10:56  up 6 days,  7:44, 19 users, load averages: 6.17 4.88 4.10`

Command: `cargo run --release -p ravel-logseg --example stage0_ref_of`

Method: 20,000 records per object, 3 warm-ups discarded, 5 runs of 20 iterations, mode loop inside run, shape loop inside mode. Clone is outside the timed region. Mode 2 bytes == mode 0 bytes for all three shapes (checked before timing). Mode 1 asserted lookups == 20000 and entries == stream count on every iteration.

Caveats: the build timer is exact. The replay figure estimates in-place lookup cost from BELOW: the replay runs right after the build with the map hot in cache, while the real probes are interleaved with record resolution work. The host was shared and loaded (load 5 to 6); spread reflects that. SHARE = (build_ns + replay_ns) / (mode 1 encode - replay_ns), per run, using run means.

## Results (per object; median of 5 run means, min and max in brackets)

| shape | mode 0 encode ms | mode 1 build us | mode 1 replay us | lookups | entries | SHARE % | mode 2 encode ms | mode2/mode0 |
|---|---|---|---|---|---|---|---|---|
| 1 stream | 56.37 (55.43..58.57) | 1.0 (0.7..1.4) | 272.4 (269.5..317.5) | 20000 | 1 | 0.492 (0.487..0.551) | 56.67 (56.37..58.73) | 1.002 (0.967..1.044) |
| 1,000 streams | 43.75 (43.02..45.97) | 18.2 (16.3..18.4) | 330.0 (274.1..369.3) | 20000 | 1000 | 0.783 (0.703..0.850) | 44.84 (42.88..46.58) | 1.021 (0.933..1.065) |
| 20,000 streams | 65.89 (65.26..73.42) | 364.7 (347.8..453.1) | 453.2 (385.8..668.2) | 20000 | 20000 | 1.232 (1.204..1.450) | 64.33 (60.94..70.15) | 0.976 (0.830..1.065) |

## Pre-registered bands

- 1 stream, share under 0.5%: inside on the median (0.492%), but run 3 reads 0.551%, above the line; the band is met only marginally. Replay of 20,000 lookups on a single-entry map costs about 272 us, which dominates the figure.
- 1,000 streams, share under 2%: inside (0.783%, max 0.850%). Miss threshold (5%) not approached.
- 20,000 streams, share under 5%: inside (1.232%, max 1.450%). Miss threshold (10%) not approached.
- Mode 2 within run-to-run spread of mode 0: inside. Ratios 1.002, 1.021, 0.976, and every ratio range spans 1.0.

## Appendix: raw per-run numbers (per object, ns; mode 0/2 carry no counters)

| run | mode | shape | encode_ns | build_ns | replay_ns | lookups | entries |
|---|---|---|---|---|---|---|---|
| 1 | 0 | 1_stream | 58573833 | 0 | 0 | 0 | 0 |
| 1 | 0 | 1000_streams | 43018713 | 0 | 0 | 0 | 0 |
| 1 | 0 | 20000_streams | 65885940 | 0 | 0 | 0 | 0 |
| 1 | 1 | 1_stream | 55374912 | 998 | 276254 | 20000 | 1 |
| 1 | 1 | 1000_streams | 43175275 | 18427 | 312367 | 20000 | 1000 |
| 1 | 1 | 20000_streams | 65014598 | 348975 | 443004 | 20000 | 20000 |
| 1 | 2 | 1_stream | 56666638 | 0 | 0 | 0 | 0 |
| 1 | 2 | 1000_streams | 43317906 | 0 | 0 | 0 | 0 |
| 1 | 2 | 20000_streams | 64325681 | 0 | 0 | 0 | 0 |
| 2 | 0 | 1_stream | 56371002 | 0 | 0 | 0 | 0 |
| 2 | 0 | 1000_streams | 45966173 | 0 | 0 | 0 | 0 |
| 2 | 0 | 20000_streams | 65260600 | 0 | 0 | 0 | 0 |
| 2 | 1 | 1_stream | 55714906 | 715 | 269571 | 20000 | 1 |
| 2 | 1 | 1000_streams | 44619479 | 16802 | 330021 | 20000 | 1000 |
| 2 | 1 | 20000_streams | 65453202 | 347775 | 453188 | 20000 | 20000 |
| 2 | 2 | 1_stream | 56502794 | 0 | 0 | 0 | 0 |
| 2 | 2 | 1000_streams | 42877686 | 0 | 0 | 0 | 0 |
| 2 | 2 | 20000_streams | 65146254 | 0 | 0 | 0 | 0 |
| 3 | 0 | 1_stream | 55427952 | 0 | 0 | 0 | 0 |
| 3 | 0 | 1000_streams | 44947773 | 0 | 0 | 0 | 0 |
| 3 | 0 | 20000_streams | 66789096 | 0 | 0 | 0 | 0 |
| 3 | 1 | 1_stream | 58238387 | 1427 | 317454 | 20000 | 1 |
| 3 | 1 | 1000_streams | 45967658 | 18166 | 369344 | 20000 | 1000 |
| 3 | 1 | 20000_streams | 67621094 | 453117 | 519733 | 20000 | 20000 |
| 3 | 2 | 1_stream | 56371781 | 0 | 0 | 0 | 0 |
| 3 | 2 | 1000_streams | 45876121 | 0 | 0 | 0 | 0 |
| 3 | 2 | 20000_streams | 63282904 | 0 | 0 | 0 | 0 |
| 4 | 0 | 1_stream | 56248154 | 0 | 0 | 0 | 0 |
| 4 | 0 | 1000_streams | 43577719 | 0 | 0 | 0 | 0 |
| 4 | 0 | 20000_streams | 65871119 | 0 | 0 | 0 | 0 |
| 4 | 1 | 1_stream | 55276350 | 894 | 269490 | 20000 | 1 |
| 4 | 1 | 1000_streams | 45305154 | 18302 | 341933 | 20000 | 1000 |
| 4 | 1 | 20000_streams | 84893871 | 430244 | 668229 | 20000 | 20000 |
| 4 | 2 | 1_stream | 58732479 | 0 | 0 | 0 | 0 |
| 4 | 2 | 1000_streams | 44835394 | 0 | 0 | 0 | 0 |
| 4 | 2 | 20000_streams | 70149588 | 0 | 0 | 0 | 0 |
| 5 | 0 | 1_stream | 58339914 | 0 | 0 | 0 | 0 |
| 5 | 0 | 1000_streams | 43750144 | 0 | 0 | 0 | 0 |
| 5 | 0 | 20000_streams | 73418819 | 0 | 0 | 0 | 0 |
| 5 | 1 | 1_stream | 55814144 | 1040 | 272415 | 20000 | 1 |
| 5 | 1 | 1000_streams | 41574158 | 16262 | 274112 | 20000 | 1000 |
| 5 | 1 | 20000_streams | 62708423 | 364719 | 385763 | 20000 | 20000 |
| 5 | 2 | 1_stream | 57624746 | 0 | 0 | 0 | 0 |
| 5 | 2 | 1000_streams | 46578908 | 0 | 0 | 0 | 0 |
| 5 | 2 | 20000_streams | 60938202 | 0 | 0 | 0 | 0 |
