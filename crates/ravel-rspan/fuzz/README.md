# ravel-rspan fuzzing

Coverage-guided (libFuzzer) fuzz target for the RSPAN reader's untrusted-input
entry points: the whole-object reader (`RspanReader::new` + `scan`) and the
footer/suffix parsers (`open`, `open_from_suffix`). It complements, rather than
duplicates, the byte-mutation proptests in `crates/ravel-rspan` (the
`decode_never_panics` / `footer_corruption_is_typed_error` family): the
proptests replay a bounded catalog of mutations, while libFuzzer explores the
input space on its own under coverage feedback. This is a separately-invoked
check; it is **not** part of `cargo test -p ravel-rspan` or the repository gate
sequence.

## Why this is a separate workspace

`cargo-fuzz` and `libfuzzer-sys` require a **nightly** toolchain and sanitizer
build flags. The rest of the repository builds and gates on **stable** only. To
keep nightly out of the main build, this crate declares its own `[workspace]`
in `Cargo.toml`, which detaches it from the root workspace. Consequences:

- `cargo fmt --all --check`, `cargo clippy --workspace`, and `cargo test` run
  from the repo root never descend into this crate and never need nightly.
- `cargo-fuzz` / `libfuzzer-sys` appear only in this crate's `Cargo.toml`, never
  in the root `Cargo.toml`.

Because of this isolation, all commands below must be run from **inside this
`fuzz/` directory** (or with `cargo fuzz`'s `--fuzz-dir`).

## One-time setup

```sh
rustup toolchain install nightly     # nightly is required by libFuzzer
cargo install cargo-fuzz             # the `cargo fuzz` subcommand
```

## Targets

| Target         | Entry point(s) fuzzed                                          |
| -------------- | ------------------------------------------------------------- |
| `decode_rspan` | `RspanReader::new` + `scan`, and `open` / `open_from_suffix`   |

List targets any time with:

```sh
cargo fuzz list
```

## Running a target

From this directory, on nightly:

```sh
cargo +nightly fuzz run decode_rspan
```

Useful options:

```sh
# Bound a CI-style run to 60 seconds:
cargo +nightly fuzz run decode_rspan -- -max_total_time=60

# Cap input size (footers/objects are small; keeps the fuzzer focused):
cargo +nightly fuzz run decode_rspan -- -max_len=4096
```

Corpora accumulate under `corpus/<target>/` and any crash reproducer is written
to `artifacts/<target>/`; both are git-ignored. Reproduce a crash with:

```sh
cargo +nightly fuzz run decode_rspan artifacts/decode_rspan/<crash-file>
```

## Just compile the target (no fuzzing)

To type-check/link the target without running a campaign (what CI or a quick
local check would do):

```sh
cargo +nightly fuzz build
```

A finding is any panic, timeout, or sanitizer report: every reader entry point
here must reject malformed input with a typed `SpanSegError`, never a panic or
out-of-bounds access.
