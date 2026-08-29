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
   its own binary for x86-64 frame-pointer prologues (`push %rbp; mov %rsp,
   %rbp`). Fewer than `MIN_FP_PROLOGUES` (1000) means the flag did not take
   effect and the run exits with a message naming the fix. This checks the
   produced binary, not the build configuration: a flag that silently did
   nothing looks identical to one that worked.

2. **Unattributed-share check.** When the profile is finished, the share of
   samples that resolved to no application frame (`[unknown]`) is computed. If
   it exceeds `MAX_UNATTRIBUTED_SHARE` (2%) the run refuses the profile with a
   message naming the likely cause. A frame-pointer build of this workload
   measured 0.00%, so 2% is generous headroom; 33.95% -- the number that caused
   the damage -- cannot reach it.

Neither threshold is a bare number: each is a documented constant in
`src/profiling.rs`, pinned by a test so a change is deliberate.
