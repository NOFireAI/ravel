# Development loop and CI gates

This guide is for contributors who change Ravel's code. It covers the fast
local iteration loop and how CI accelerates the same gates.

## Running Ravel from your own build (`make demo`)

The container-first quickstart in the [README](../../README.md) and the
[getting started guide](../guides/getting-started.md) runs a published image and needs no
toolchain. That is the right path for evaluating Ravel; it is the wrong path
when you are changing Ravel's code, because it does not run your build.

`make demo` is the from-source path for contributors:

```sh
make demo
```

It builds `ravel-server` and `ravel-cli` in release mode from the current tree,
starts `ravel-server --store s3` against the local MinIO stack, ingests one
generated OTLP export, and queries it back by commit token
([scripts/demo.sh](../../scripts/demo.sh)).

One capability difference to keep in mind: the published image is built with
`--features sql`, so `POST /api/v1/sql` works in the compose quickstart. `make
demo` builds the default feature set and does **not** enable `sql`, so the SQL
endpoint is unavailable on the from-source path. PromQL, ingest, and the rest
are identical on both. To exercise the SQL surface from source, run
`ravel-server` yourself with `--features sql`, or use the compose quickstart.

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
CI enforces. CI still runs the full fmt, clippy, and test gates on every
push. This guidance affects only how you spend time between edits on your
machine.

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
