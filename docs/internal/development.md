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

It first runs `make build`, a release build of the whole workspace with no
cargo features. Then [scripts/demo.sh](../../scripts/demo.sh) builds debug
`ravel-server` and `ravel-cli`, also with no features, starts
`ravel-server --store s3` against the local RustFS stack, ingests one
generated OTLP export, and queries it back by commit token. The demo runs the
debug binaries, not the release build.

The published image is built with `--features sql,flight-sql,otap,agents,mcp`.
`make demo` builds none of those features, so the demo server serves no SQL,
Flight SQL, OTAP, Agents, or MCP routes. PromQL and ingest behave the same on
both paths. To exercise a feature from source, run `ravel-server` yourself
with that `--features` list, or use the compose quickstart.

## Fast local iteration

While you iterate, use a tight loop on the crate you are changing:

```sh
cargo check -p <crate>            # fast type/borrow feedback on one crate
cargo check --workspace           # only when the change is genuinely cross-crate
cargo test -p <crate>
```

Before you commit, run the gate script once, not after every edit:

```sh
scripts/gates.sh -p <crate>       # the crate and everything that depends on it
```

[CONTRIBUTING.md](../../CONTRIBUTING.md#run-the-checks-before-a-pull-request)
lists what the script runs, its workspace form, and the documentation checks.
A plain `cargo fmt`, `cargo clippy`, and `cargo test` list is not equivalent:
it skips the `sql`, `flight-sql`, `esql` and other feature lanes. CI runs the
full gates on every push.

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
- The jobs that reclaim runner disk before compiling share one composite
  action, `.github/actions/free-disk-space`, instead of copying the same
  shell block. A lane that pulls no container image passes
  `background: "true"`: the same deletes run in a detached process and the
  job does not wait. A lane that does pull an image must keep the default,
  because the image prune would run beside the pull.
- Those lanes also run `.github/actions/disk-usage` twice: `phase: start`
  before the cleanup, which prints free space and samples it every 15
  seconds, and `phase: report` as the last step, which prints the lowest
  sample, what the background cleanup freed, a lower bound on the lowest
  free space without the cleanup, and fails the job when the lowest sample
  is under a 3 GiB floor. The cache save that runs after a job's last step
  is outside the measurement.

You do not need sccache or nextest on your machine. `cargo check` and the gate
list above are enough for the local loop.
