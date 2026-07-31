# Development loop and CI gates

This guide is for contributors who change Ravel's code. It covers the fast
local iteration loop and how CI accelerates the same gates.

## Fast local iteration

Every commit must pass this gate list (it is also in CLAUDE.md):

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p <your-crate>        # plus --workspace when cross-crate
```

All three after every edit is slow. While you iterate, use a tight loop
instead:

```sh
cargo check -p <crate>            # fast type/borrow feedback on one crate
cargo check --workspace           # only when the change is genuinely cross-crate
```

Run the full fmt/clippy/test gate list once, right before you commit, not
after every edit.

This is a local development-loop cadence only. It changes nothing about what
CI enforces on a pull request. CI still runs the full fmt, clippy, and test
gates on every push. This guidance affects only how you spend time between
edits on your machine.

## What CI does to keep those gates fast

CI runs the same gates, but it shares build work. The six workspace-compiling
jobs then do not each recompile the overlapping crates from scratch.

- `Swatinem/rust-cache@v2` restores the dependency registry and build cache
  keyed on `Cargo.lock`.
- `sccache` (installed via `mozilla-actions/sccache-action`) wraps `rustc`
  with `RUSTC_WRAPPER=sccache`. It stores compiled objects in the GitHub
  Actions cache backend (`SCCACHE_GHA_ENABLED=true`). That action also
  exports the GHA cache auth tokens (`ACTIONS_RESULTS_URL` /
  `ACTIONS_RUNTIME_TOKEN`) that the backend needs; a plain binary installer
  does not. One job compiles the objects, and the others reuse them. No repo
  secret or setting beyond those two environment variables is necessary.
- Tests run under `cargo nextest run` (installed via
  `taiki-e/install-action@nextest`) instead of `cargo test`, for faster
  parallel execution and clearer output.
- The four jobs that reclaim runner disk before compiling share one
  composite action, `.github/actions/free-disk-space`, instead of copying
  the same shell block.

You do not need sccache or nextest on your machine. `cargo check` and the gate
list above are enough for the local loop.
