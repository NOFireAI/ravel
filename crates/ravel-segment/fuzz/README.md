# ravel-segment fuzzing

Coverage-guided (libFuzzer) fuzz targets for the RSEG reader's untrusted-input
entry points: the footer/section parser and the v4/v5 catalog decoders. These
complement, rather than duplicate, the byte-mutation proptests in
`crates/ravel-segment`: the proptests replay a fixed catalog of
mutations (single-byte flips, truncation, block-boundary and cross-version
edits), while libFuzzer explores the input space on its own under coverage
feedback. This is a separately-invoked check; it is **not** part of `cargo test
-p ravel-segment` or the repository gate sequence.

## Why this is a separate workspace

`cargo-fuzz` and `libfuzzer-sys` require a **nightly** toolchain and sanitizer
build flags. The rest of the repository builds and gates on **stable** only. To
keep nightly out of the main build, this crate declares its own `[workspace]`
in `Cargo.toml`, which detaches it from the root workspace (`members =
["crates/*", ...]`). Consequences:

- `cargo fmt --all --check`, `cargo clippy --workspace`, and `cargo test` run
  from the repo root never descend into this crate and never need nightly.
- `cargo-fuzz` / `libfuzzer-sys` / `arbitrary` appear only in this crate's
  `Cargo.toml`, never in the root `Cargo.toml`.

Because of this isolation, all commands below must be run from **inside this
`fuzz/` directory** (or with `cargo fuzz`'s `--fuzz-dir`).

## One-time setup

```sh
rustup toolchain install nightly     # nightly is required by libFuzzer
cargo install cargo-fuzz             # the `cargo fuzz` subcommand
```

## Targets

| Target                       | Entry point(s) fuzzed                                              |
| ---------------------------- | ----------------------------------------------------------------- |
| `parse_footer`               | `parse_footer` (both trailer versions) + `open_from_suffix`       |
| `decode_catalog_v4`          | `decode_catalog_v4` (v4 run-major whole-catalog decode)           |
| `decode_catalog_matching_v4` | `decode_catalog_matching_v4` (v4 decode with an equality filter)  |
| `decode_catalog_v5`          | `decode_catalog_v5` (v5 sparse whole-catalog decode)              |
| `sparse_probe`               | `parse_series_idx` + `find_index_in_window` (v5 point-lookup path) |

Note on naming: the current public API (post ADR-0027, v5-only
readable/writable) names the catalog decoders `*_v4` (run-major catalog
grammar) and `*_v5` (sparse catalog grammar); v5 has no single
`decode_catalog_matching_v5`, its filtered/point-lookup surface is the sparse
probe covered by the `sparse_probe` target.

List targets any time with:

```sh
cargo fuzz list
```

## Running a target

From this directory, on nightly:

```sh
cargo +nightly fuzz run parse_footer
```

Swap `parse_footer` for any target name from the table. Useful options:

```sh
# Bound a CI-style run to 60 seconds:
cargo +nightly fuzz run decode_catalog_v5 -- -max_total_time=60

# Cap input size (footers/objects are small; keeps the fuzzer focused):
cargo +nightly fuzz run decode_catalog_v4 -- -max_len=4096
```

Corpora accumulate under `corpus/<target>/` and any crash reproducer is written
to `artifacts/<target>/`; both are git-ignored. Reproduce a crash with:

```sh
cargo +nightly fuzz run <target> artifacts/<target>/<crash-file>
```

## Just compile the targets (no fuzzing)

To type-check/link every target without running a campaign (what CI or a quick
local check would do):

```sh
cargo +nightly fuzz build
```

A finding is any panic, timeout, or sanitizer report: every reader entry point
here must reject malformed input with a typed `SegmentError`, never a panic or
out-of-bounds access.
