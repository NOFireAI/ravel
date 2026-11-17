# Develop Ravel in a Brig sandbox

You can run Claude Code on Ravel inside a [Brig](https://github.com/brig-sh/brig)
microVM. The sandbox has the Rust toolchain `rust-toolchain.toml` names,
cargo-nextest, cargo-deny, Node for the TypeScript SDK, and a crates.io
registry filled for `Cargo.lock`. Your Claude login and your GitHub token stay
on the host: Brig's broker adds them to requests on the way out.

Each piece of work gets its own clone and its own sandbox, with 8 vCPUs and
12 GB of memory.

## Set up once

1. Install brig from the main channel. The released v0.5.2 cannot run a
   Brigfile target.

   ```sh
   brew install --cask brig-sh/brig/brig@main
   ```

2. Log in to the GitHub container registry, if the `ravel-dev` image is not
   public: `docker login ghcr.io` with a token that has `read:packages`.

3. Store the two secrets the routes send:

   ```sh
   make brig-login
   ```

   This runs `brig secret import claude-code`, which stores
   `claude-credentials`, and then asks for `ravel-gh-token`. Make that a
   fine-grained token on `NOFireAI/ravel` with these permissions and no
   others:

   | Permission | Access |
   | --- | --- |
   | Contents | Read and write |
   | Pull requests | Read and write |

   Leave Workflows off. With it, a branch pushed from the guest could add a
   workflow that runs with the repository's secrets.

## Work on a ticket

Run these from your primary checkout or a worktree:

```sh
make brig-new NAME=4405-brig       # clone origin/main to ../store-brig/4405-brig, branch 4405-brig
make brig NAME=4405-brig           # Claude Code in that clone's sandbox
```

| Command | What it does |
| --- | --- |
| `make brig NAME=x ARGS=--continue` | Passes arguments to `claude` |
| `make brig-shell NAME=x` | A login shell in the sandbox instead of Claude Code |
| `make brig-plan NAME=x` | Shows what a run gets, and boots nothing |
| `make brig-stop NAME=x` | Stops the sandbox. The guest home and the clone stay |
| `make brig-rm NAME=x` | Removes the sandbox and its guest home. The clone stays |
| `make brig-update NAME=x` | Removes the sandbox, pulls the newest image, and runs |

`IMAGE=ghcr.io/nofireai/ravel-dev:sha-<sha7>` on any of these runs another
build of the image. Set `RAVEL_BRIG_CLONES` to keep clones somewhere other
than `../store-brig`.

When the branch is merged, remove the sandbox with `make brig-rm`, then
delete the clone directory.

## Inside the sandbox

- Gates run as on the host: `scripts/gates.sh -p <crate>`, `cargo nextest
  run -p <crate>`. Scoped gates diff against `origin/main`, so fetch it
  first when the clone is old.
- Build output goes to `target/brig/` in the clone, on host disk. A cold
  build there writes 54 GB or more. The full merge-tier gate, with its
  feature lanes, can need 188 GB per clone, so leave it to the host or CI.
- `CARGO_BUILD_JOBS` is 4, because the guest has no swap and several
  large test binaries linking at once can run it out of memory. Try
  `CARGO_BUILD_JOBS=8` for a lighter build. If a link is killed with
  signal 9, go back to 4.
- `git push` works on `origin`. Open, review and merge the pull request on
  the host.
- `gh` sees the API route as an Enterprise host, so name the repository:
  `gh pr view 123 -R github-api.brig.internal/NOFireAI/ravel`. It can read,
  reply to review threads and resolve them. It cannot comment, review, open,
  close or merge.

The sandbox reaches crates.io, `static.rust-lang.org` and the npm registry
directly. Everything else is refused, apart from GitHub and Anthropic
through the routes.

## Limits

- Brig runs no containers in the guest. `make rustfs`, `make quickstart`,
  `make demo` and the kind targets work on the host only.
- A linked git worktree does not work as the project: its `.git` file names
  a directory the sandbox does not mount. Use `make brig-new`.
- Writes outside the project and the guest home are lost at `brig stop`.
  The image's registry and toolchain come back at the next run. Crates
  fetched past the image's `Cargo.lock` are fetched again.
- Your `~/.claude` skills and memory are not in the guest. Use the sandbox
  to implement; review and land on the host.
- Any guest process can push to any branch the token can write, force-push
  included, for the 12 hours a route token lasts. Branch protection covers
  `main` only. The token's scope is the limit.
- The guest can write anything in the clone, including `.git/hooks`,
  `.git/config`, `Makefile`, `scripts/`, `Brigfile` and `.brig/*.yaml`.
  - Do not run host git or `make` inside a sandbox clone. `scripts/brig.sh`
    refuses to run from one, so the host side always comes from your own
    checkout.
  - The next `make brig` boots the clone's `Brigfile` and the profile and
    policies it names. Brig's approval covers those files by sha256, with
    the egress rules and the routes they resolve to, so a run after the
    guest has changed any of them asks you to approve again. Read the plan
    before you approve: a wider egress list or route rule shows up there.
    The approval does not cover the bytes behind the image tag.
- The Brigfile format is `v1alpha1`, with no compatibility promise. After
  you update brig, run `brig config --target dev -f <clone>/Brigfile`.

## Files

| File | What it holds |
| --- | --- |
| `Brigfile` | The `dev` target: profile, policies, size and arguments |
| `.brig/profile.yaml` | The image, the guest environment and the denied keys |
| `.brig/egress.yaml`, `.brig/claude.yaml`, `.brig/github.yaml` | The egress list and the routes |
| `.brig/Dockerfile`, `.brig/check-image.sh` | The guest image and its check, built by `.github/workflows/brig-image.yml` |
| `scripts/brig.sh` | The host side of the `make brig*` targets |
| `scripts/brig-dev.sh` | The guest side: the environment, then Claude Code or a shell |
