# Sample-PGO build of ravel-server

How to record a sample profile of ravel-server, build with it, and check
that the build used it, with `scripts/build-pgo.sh`. This page also records
why sample PGO is not part of the release build.

## Status

Deferred on 2026-11-21. Experiment R3 of
[ADR-4523](../adrs/4523-rust-1100-toolchain-and-runtime-experiments.md)
(decision 4) built ravel-server with a sample profile and measured it
against the same source built without one. The profile was used and saved
CPU repeatably, but less than the 5% the ADR set for adoption, and the
query tail guard of the mixed run missed. Release images keep the ordinary
build. The script stays in the repository as the reproduction of R3 and for
anyone who wants to repeat it. No release or image path calls it; only
its own test suite, `scripts/build-pgo.test.sh`, does, from a `ci.yml`
step. arm64 is not qualified.

## Decision record

Run `rust1100-full-20261121T0553Z` on one c6a.metal, source `a8f5ccd51`.
Arm B is rustc 1.100.0 with no profile; arm C is B plus
`-Cprofile-sample-use`. Ratios are C/B per round, five rounds.

| Figure | Result | Verdict | Source |
|---|---|---|---|
| Branch sampling gate | AMD BRS with `-e cpu/branch-brs/` | PASS | epic #4523 comment 7062870942 |
| Resolution gate | 20 of 20 of perf's top self-time symbols in the profile | PASS | epic #4523 comment 7062870942 |
| Profile-used gate | C's binary differs from B; the ravel_sql IR carries the SampleProfile summary; 8 functions have nonzero entry counts; 1 top-20 function is defined with `!prof` (thin coverage evidence) | PASS | epic #4523 comment 7062870942 |
| Arm B build | rustc 1.100.0 (LLVM 23.1.5), 551 s, 970,704,664 bytes | reported | epic #4523 comment 7062870942 |
| Arm C build | 654 s, 1,041,292,216 bytes (7.3% larger) | reported | epic #4523 comment 7062870942 |
| CPU-s per correct query, held-out warm suite of 13 statements | median 0.977 (rounds 0.977, 0.985, 0.983, 0.972, 0.973); 0 of 5 rounds at or under 0.95 | MISS against "at most 0.95 in at least 4 of 5" | epic #4523 comment 7062870942 |
| Query p99, mixed run | rounds 1.088, 1.024, 0.966, 1.053, 1.077; spread within arm 0.257 | MISS (3 of 5 above 1.05) | epic #4523 comment 7062870942 |
| Acknowledged ingest rate | rounds 0.997, 1.027, 0.978, 1.017, 1.032; spread within arm 0.051 | INCONCLUSIVE (every round is inside 5%, but the within-arm spread is above the 5% at which the pre-registration, epic #4523 comment 7055806818, makes a figure inconclusive) | epic #4523 comment 7062870942; spread from `out/readout-after-r3-rerun.txt` of the run |
| Peak RSS | rounds 1.004, 1.023, 0.953, 1.035, 0.994; spread within arm 0.064 | INCONCLUSIVE (same basis as the ingest rate) | epic #4523 comment 7062870942; spread from `out/readout-after-r3-rerun.txt` of the run |
| Rows | 9 differences over 150 comparisons, all `q18_user_phrase_limit` | MISS as computed (an unordered `LIMIT`, below) | epic #4523 comment 7062870942 |
| Per-phase GETs and wire bytes | 0 of 3,350 values outside the rule | PASS | epic #4523 comment 7062870942 |
| Best per-statement CPU | q39 0.942, q18 0.944, q30 0.966, q21 0.967 | reported | epic #4523 comment 7062870942 |

Rows: MISS as computed. All nine differences are in
`q18_user_phrase_limit`, which is `GROUP BY ... LIMIT 10` with no
`ORDER BY`: an unordered `LIMIT`, for which any 10 groups are a valid
answer. B differs from its own round 1 the same way C does. The comparator
has no rule for an unordered `LIMIT`, so it counts each such difference.

Decision: DEFER. The profile saved a median 2.3% CPU per query on the
held-out suite (1 - 0.977), in all five rounds, which is below the 5% adoption target, and the mixed-run
tail guard missed. Release builds do not change. arm64 is not qualified.

Any one of these would reopen it, each as a new issue that links here:

- a workload where the held-out suite meets the at-most-0.95 target in 4
  of 5 rounds with the mixed-run tail guard held;
- a newer LLVM in the pinned toolchain;
- arm64 qualification.

## Host requirements

- Bare-metal Linux x86_64 with branch-stack sampling. Every subcommand
  exits 6 on any other OS or architecture. AMD BRS on Zen 3 bare metal
  worked on c6a.metal; Intel LBR is the alternative. Virtualised
  c6a.4xlarge exposes no PMU at all (#4232), so `record` finds no event
  perf can open and exits 6.
- `perf`, allowed to open the event: a refusal that names
  `perf_event_paranoid` exits 4, not 6. Lower
  `kernel.perf_event_paranoid` or grant `CAP_PERFMON`.
- `llvm-profgen` for rustc's LLVM major. `scripts/build-pgo.sh
  check-tools` prints rustc's major and the tools it found. Use rustup's
  `llvm-tools` component if it ships `llvm-profgen` for this toolchain;
  otherwise install apt.llvm.org's `llvm-<major>` package, which puts it
  under `/usr/lib/llvm-<major>/bin`. `LLVM_PROFGEN` overrides both. An
  `llvm-profgen` of another major is refused with exit 3.
- `llvm-profdata`, from `rustup component add llvm-tools` (already in
  `rust-toolchain.toml`) or `LLVM_PROFDATA`.

## Recording a profile

1. Build an unstripped release binary: `cargo build --release --locked -p
   ravel-server` with the release image's features. The release profile
   sets `debug = 1`, so the binary carries the line tables
   `llvm-profgen` needs.
2. Start it against a loaded store.
3. Drive the training mix R3 used, and keep it running for the whole
   recording:
   - the corpus statements outside the held-out set;
   - the three PromQL range queries of the perfmap C phase;
   - OTLP logs ingest from one generator seed.

   Keep the held-out statements and the evaluation generator seed out of
   training. A profile trained on the statements it is judged on measures
   nothing.
4. Record:

   ```sh
   scripts/build-pgo.sh record --pid <server pid> \
     --binary target/release/ravel-server --seconds 300 --out pgo/
   ```

   `--binary` must be the exact binary the pid is running. Without
   `--event`, the script probes `cpu/branch-brs/`, then
   `br_inst_retired.near_taken:u`, then `cycles:u`, each with
   `perf record -b -e <event> -- true`, and keeps the first that perf can
   open. It then records the pid with that event. It exits 4 when that
   `perf record` exits non-zero (a wrong `--pid`, for example; the tail of
   perf's stderr is printed) or when `perf report --stats` counts zero
   `SAMPLE` events: perf writes a non-empty `perf.data` header even then.

   It converts the samples with `llvm-profgen` into `pgo/profile.prof` and
   checks resolution, R3's gate: each of the binary's top 20 self-time
   symbols in `perf report`, clone suffixes stripped, must be a function
   of the profile with nonzero total samples, and the 20 must include a
   `ravel_` symbol and a `datafusion` symbol. A report with fewer than 20
   symbols of the binary exits 4: record longer or under more load. Exit 4
   lists the symbols that are missing or have zero samples. On success it writes the 20
   symbols to `pgo/top-symbols.txt`, which `build` needs.

## Building with a profile

```sh
scripts/build-pgo.sh build --profile pgo/profile.prof
```

This runs `cargo build --release --locked -p ravel-server` with
`-Cprofile-sample-use=<absolute path>` appended to any `RUSTFLAGS` you set.
The features default to the release image's set, read from the root
Dockerfile; `--features` overrides them and `--target-dir` moves the
output.

It refuses a profile with no `top-symbols.txt` beside it (exit 4). After
the build, the script emits LLVM IR for the hottest Ravel crate named in
the profile's `provenance.txt` (ravel-sql if there is none) with the same
flags, and checks it for a `SampleProfile` summary, at least one nonzero
`function_entry_count`, and at least one `define` of a `top-symbols.txt`
symbol that carries `!prof`.

Exit 5 means the build could not be shown to use the profile: no `.ll`
file for the crate appeared, the flag did not reach rustc, the profile
matched none of the crate's functions, or it matched none of the top-20
functions defined there. Exit 1 after the build means the `cargo rustc`
that emits the IR failed. On either, the script moves the binary to
`<target>/release/ravel-server.pgo-unproven`, so nothing picks it up as a
plain build; do not use it. The fallback is the ordinary build with no
flag. The root Dockerfile, which builds the release images, runs `cargo
build --release --locked -p ravel-server --features
sql,flight-sql,otap,agents,mcp`, then splits out the debug info and strips
the shipped binary.

The script does not compare the PGO binary's hash with a plain build's, a
gate R3's harness also ran; do that by hand when you have both binaries.

## Refreshing

A sample profile tolerates source drift, so an old profile still builds.
Re-record when:

- the source has moved far from the profile's `source_sha` (the build
  prints a warning naming that SHA and HEAD when they differ);
- the toolchain's LLVM major changes;
- the hot path changes, for example a new query operator or a rewritten
  decoder.

A profile is valid only for the target it was recorded on.

## Provenance

`record` writes `provenance.txt` beside the profile, one `key=value` per
line:

| Key | Value |
|---|---|
| `source_sha` | `git rev-parse HEAD` when recorded |
| `binary_sha256` | hash of the `--binary` profiled |
| `rustc_version` | `rustc -V` |
| `llvm_major` | rustc's LLVM major |
| `llvm_profgen_version` | `llvm-profgen --version` LLVM line |
| `perf_event` | the event kept |
| `seconds` | recording length |
| `host` | `uname -srm` |
| `cpu_model` | `model name` from `/proc/cpuinfo` |
| `recorded_at` | UTC time |
| `profile_sha256` | hash of `profile.prof` |
| `hottest_ravel_crate` | the first Ravel crate among the top 20 symbols (a symbol naming several takes the longest crate name), else `ravel_sql` |
| `top_symbols` | `top-symbols.txt`, the file beside it holding the 20 stripped symbols, hottest first |

The training mix is not in the file: write it down beside the profile
when you record.

`build` writes `<binary>.provenance.txt` beside the built binary: every
profile line prefixed `profile_`, plus the build's `source_sha`,
`binary_sha256`, `rustc_version`, `features` and `rustflags`.

## Re-evaluating

To ask the adoption question again, use the adoption table of ADR-4523
decision 4: CPU-s per correct query on the held-out warm suite at most 0.95
of B in at least 4 of 5 rounds; mixed-workload p99, ingest rate and peak
RSS within 5%; rows and per-phase GET counts identical.

R3's box harness is under
`s3://ravel-clickbench-5b7101/reports/rust1100/harness/` and its results
under
`s3://ravel-clickbench-5b7101/reports/rust1100/rust1100-full-20261121T0553Z/out/`.
Use them as the reproduction, and file the result as a new issue on epic
#4523.
