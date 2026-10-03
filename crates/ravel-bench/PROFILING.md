# Profiling ravel-bench with frame pointers

A release build omits frame pointers. Without them `perf --call-graph dwarf`
and pprof's unwinder cannot unwind inlined generic code, and Ravel's own hot
path lands in `[unknown]`. This has cost the project twice: PR #847 read
"only 178 of 20,310 SipHash samples resolved to a ravel_* frame" as evidence
the hash was cheap when it was the unwinder failing, and the same profile put
33.95% of samples in `[unknown]`. Rebuilding with `-C force-frame-pointers=yes`
took that to 0.00% and resolved the dominant frame (`hash_one::<&u32>` at
16.34% self), which shipped as #876/#881.

## Build a profiling binary

From `crates/ravel-bench/` (config discovery is by current directory), with
`RUSTFLAGS` unset:

```sh
cd crates/ravel-bench
cargo profile-build            # cargo alias, see .cargo/config.toml
```

The `profile-build` alias is `cargo build --release --features profiling`
with `-C force-frame-pointers=yes` injected via `--config build.rustflags`.
`build.rustflags` propagates the flag to every crate in the build, dependency
crates included; a `build.rs` emitting `cargo:rustc-flags` would reach only
ravel-bench itself and leave the dependency frames (ravel-ingest, ravel-query,
...) as `[unknown]`, which is the exact failure this guards against. Because the
flag is scoped to the alias rather than a `[build]` table, ordinary
`cargo test` and gate runs keep their normal fingerprint.

Cargo discovers `.cargo/config.toml` from the current directory, so the alias
does not exist for a build invoked from the workspace root. There, and for
`cargo run`, pass the flag yourself:

```sh
RUSTFLAGS="-C force-frame-pointers=yes" \
  cargo run --release -p ravel-bench --features profiling --bin ingest_bench -- ...
```

No mechanism inside this crate can force the flag onto that invocation: a
crate-local `.cargo/config.toml` is not read from the workspace root, and a
`build.rs` cannot set codegen flags for its own crate. That is why the guards
below exist and are the actual enforcement, and why they check the produced
binary and the produced profile rather than the build configuration.

Then run a bench bin with the flamegraph output path set:

```sh
RAVEL_BENCH_PROFILE_SVG=/tmp/ingest.svg \
  ../../target/release/ingest_bench --store memory ...
```

## Two guards that fail closed

A build can carry the flag and still emit no prologues if something overrode
it, and a profile can come back unattributable for reasons other than the
build. Both are refused rather than silently produced:

1. **Frame-pointer build check.** Before arming the sampler, a real profiled
   run (`RAVEL_BENCH_PROFILE_SVG` set, `profiling` feature compiled in) scans
   the executable (`PT_LOAD` + `PF_X`) segments of its own binary for x86-64
   frame-pointer prologues (`push %rbp; mov %rsp, %rbp`, the bytes `55 48 89
   e5`), streaming them in 64 KiB chunks. Fewer than `MIN_FP_PROLOGUES` (4,800)
   means the flag did not take effect and the run exits non-zero with a message
   naming the fix. This checks the produced binary, not the build
   configuration: a flag that silently did nothing looks identical to one that
   worked.

   The floor is measured, not guessed. On an x86-64 Linux box, `ingest_bench`
   (the smallest profiling bin) carried 9,739 prologues built with the flag and
   1,519 without it; `query_latency_bench` carried 12,712 with it. 4,800 is
   about half the smallest passing figure and more than three times the failing
   one. A `> 0` check would have passed that 1,519 build.

   Where the check cannot run it says so and continues, rather than skipping
   silently: on a non-x86-64 architecture (the prologue bytes are
   architecture-specific) or a non-ELF64 executable (a macOS host). A failure
   to read the executable at all, on a platform where the check does apply, is
   a refusal.

2. **Unattributed-share check.** When the profile is finished, the share of
   samples that resolved to no application frame (`[unknown]`, `Unknown`, `??`,
   or no symbol) is computed and printed once. If it exceeds
   `MAX_UNATTRIBUTED_SHARE` (2%) the run refuses the profile -- exits non-zero
   without writing the SVG -- with a message naming the measured share, the
   threshold, and the likely cause. A frame-pointer build of this workload
   measured 0.00%, so 2% is generous headroom; 33.95% -- the number that caused
   the damage -- cannot reach it.

Neither threshold is a bare number: each is a documented constant in
`src/profiling.rs`, pinned by a test so a change is deliberate.
