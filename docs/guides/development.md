# Development loop and CI gates

This guide is for contributors changing Ravel's code. It covers the fast
local iteration loop and how CI accelerates the same gates.

## Fast local iteration

The gate list every commit must pass (also in CLAUDE.md) is:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p <your-crate>        # plus --workspace when cross-crate
```

Running all three after every edit is slow. Instead, while iterating use a
tight loop:

```sh
cargo check -p <crate>            # fast type/borrow feedback on one crate
cargo check --workspace           # only when the change is genuinely cross-crate
```

Run the full fmt/clippy/test gate list once, right before committing, not
after every edit.

This is a local development-loop cadence only. It changes nothing about what
CI enforces on a pull request: CI still runs the full fmt, clippy, and test
gates on every push. The cadence guidance only affects how you spend time
between edits locally.

## What CI does to keep those gates fast

CI runs the same gates but shares build work so the six workspace-compiling
jobs do not each recompile the overlapping crates from scratch.

- `Swatinem/rust-cache@v2` restores the dependency registry and build cache
  keyed on `Cargo.lock`.
- `sccache` (installed via `mozilla-actions/sccache-action`) wraps `rustc`
  with `RUSTC_WRAPPER=sccache` and stores compiled objects in the GitHub
  Actions cache backend (`SCCACHE_GHA_ENABLED=true`). That action also
  exports the GHA cache auth tokens (`ACTIONS_RESULTS_URL` /
  `ACTIONS_RUNTIME_TOKEN`) the backend needs, which a plain binary installer
  does not. Objects compiled by one job are reused by the others; no repo
  secret or setting beyond those two environment variables is required.
- Tests run under `cargo nextest run` (installed via
  `taiki-e/install-action@nextest`) instead of `cargo test`, for faster
  parallel execution and clearer output.
- The four jobs that reclaim runner disk before compiling share one
  composite action, `.github/actions/free-disk-space`, instead of copying
  the same shell block.

You do not need sccache or nextest locally; `cargo check` and the gate list
above are enough for the local loop.
