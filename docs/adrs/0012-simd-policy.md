# ADR-0012: SIMD policy - dependencies and autovectorization first, explicit SIMD behind benchmark-gated review

Status: Accepted (2026-07-27)

## Context

Telemetry databases invite hand-written SIMD: codecs, checksums, hashing,
and filtering all look vectorizable. Ravel denies `unsafe` workspace-wide,
`core::arch` intrinsics are unsafe, and `std::simd` is nightly-only. The
measured Phase 1 bottlenecks (BENCHMARKS.md) are per-series fixed overhead
and allocation churn, not scalar arithmetic in hot loops.

## Decision

Three tiers, in order:

1. Dependency-provided SIMD is the default and already covers the heavy
   primitives: blake3 (AVX2/AVX-512/NEON), crc32c (hardware CRC
   instructions), zstd and lz4 (SIMD-optimized C), arrow-rs kernels on the
   OTAP path. Choosing well-optimized dependencies is the SIMD strategy.
2. Autovectorization for our own hot loops. Write loops the compiler can
   vectorize (flat slices, no branches in the body, chunked iteration);
   verify with cargo-asm or profile deltas when it matters. Release and
   bench builds record their `target-cpu`; deployment builds set it to the
   fleet's actual microarchitecture so dependency dispatch and
   autovectorization engage.
3. Explicit SIMD only when a profile names a loop and tiers 1-2 leave a
   measured gap: safe wrapper crates (`wide`, `pulp`) first, `std::simd`
   when stable. Each use requires its own benchmark-gated review with an
   A/B off-switch (engineering rule: no optimization that cannot be
   disabled and compared) and, if any unsafe were ever involved, the full
   ADR-plus-fuzzing bar from the project rules. None is approved today.

Format evolution is where large SIMD wins actually live: Gorilla XOR is
bit-serial and SIMD-hostile by construction, while the RSEG v2 codec
candidates on the research list (ALP for floats, stream-vbyte-style
timestamp layouts) are designed for vectorized decode. Those decisions go
through the format-change procedure with codec benchmarks, per spec
section 10: data layout first, instructions second.

## Consequences

- No unsafe enters the codebase for SIMD; the deny stays intact.
- Perf work stays profile-driven; SIMD proposals without a profile and a
  failed tier-2 attempt are rejected in review.
- Bench reports must state `target-cpu` so numbers are comparable.
