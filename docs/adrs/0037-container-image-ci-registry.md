# ADR-0037: CI-built container images published to GHCR

Status: Proposed (2026-07-31). Build and release tooling only: no change
to any frozen contract, ingest/query semantics, or durability invariant.

## Context

The root `Dockerfile` (ADR-0034 decision 5) and its CI companion
`Dockerfile.prebuilt` (decision 7) already produce `server` and
`operator` images. `.github/workflows/ci.yml`'s `docker-build` job
(ci.yml:665-710) builds both targets from the root `Dockerfile` on every
push that touches it, and `k8s-integration` (ci.yml:527-636) assembles
both from prebuilt binaries for the kind lane. Both are build-only: no
job tags or pushes an image anywhere, no registry is referenced anywhere
in the repo (`deploy/k8s/operator/operator.yaml:37` hardcodes
`image: ravel-operator:latest` as a placeholder the kind scripts
override), and no publish workflow exists.

A developer on Apple Silicon trying to build a shipping amd64 image
locally hits this:

```
1.478 info: syncing channel updates for 1.97.1-x86_64-unknown-linux-gnu
6.963 error: process didn't exit successfully: `rustc -vV` (signal: 11, SIGSEGV)
6.963 qemu: uncaught target signal 11 (Segmentation fault) - core dumped
```

This is Docker Desktop cross-emulating `linux/amd64` through QEMU on an
arm64 host; `rustc` reliably segfaults under QEMU's user-mode emulation.
It is not a bug in the Dockerfile, the Rust toolchain pin, or the
workspace: `docker-build` and `k8s-integration` both already build the
identical `Dockerfile` successfully today, because GitHub-hosted
`ubuntu-latest` runners are native amd64 and never invoke QEMU. The
actual gap is that nothing ever pushes what CI already proves it can
build, and there is no registry for a developer or a cluster to pull
from instead of building locally at all.

The repo has no existing cloud registry account, secret, or credential
of any kind (no AWS, no ECR references outside the failing local
command's `$ECR` env var, which is a developer-local convention, not
repo config). The GitHub org is `NOFireAI`, repo `store`.

## Decision

1. **Registry: GitHub Container Registry (GHCR), not ECR.** Images
   publish to `ghcr.io/nofireai/ravel-server` and
   `ghcr.io/nofireai/ravel-operator`. GHCR needs no new credential:
   Actions' ambient `GITHUB_TOKEN` authenticates with `packages: write`
   permission, scoped to this repo. ECR would require provisioning an
   AWS account/role, OIDC federation or long-lived keys, and ongoing
   IAM maintenance for a repo that otherwise has zero AWS footprint
   (object storage is generic S3-compatible, exercised against MinIO
   and floci in CI, never AWS). GHCR also makes `docker pull` from a
   laptop or a cluster a one-line, token-scoped operation instead of an
   AWS credential exchange.

2. **Build source: the root `Dockerfile`, not `Dockerfile.prebuilt`.**
   The published image is the same artifact `docker-build` already
   verifies compiles: correct shipping glibc (Debian 12, matching the
   `rust:1.97.1-bookworm` builder), includes `ravel-cli` in the server
   image, and has no cache-freshness assumptions about a runner's local
   `target/`. `Dockerfile.prebuilt` exists specifically to reuse
   `k8s-integration`'s warm sccache/rust-cache for a fast kind loop
   (Dockerfile.prebuilt:5-16) and ships a deliberately different glibc
   (Debian 13) and binary set (no `ravel-cli`) documented as CI-lane-only
   in its own header. Publishing from it would ship an image that
   diverges from what `docker-build` gates and would need its own glibc
   compatibility story for arbitrary pull targets.

3. **Runner: `ubuntu-latest`, matching `docker-build`.** Native amd64,
   no QEMU, so the SIGSEGV above cannot occur in CI. `linux/amd64` is
   the only platform published; see Rejected alternatives for why
   arm64 is deferred rather than added now.

4. **Trigger and tag scheme**, new workflow
   `.github/workflows/publish-images.yml`, gated the same way
   `docker-build` is (skip on docs-only changes) but not sharing its
   job, since this one needs registry credentials the build-verification
   job should never hold:
   - Push to `main`: tags `main` and `sha-<short-sha>`.
   - Push of a tag matching `v[0-9]+.[0-9]+.[0-9]+`: tags `X.Y.Z`, `X.Y`,
     `X`, and `latest`.
   - `workflow_dispatch`: tag `manual-<short-sha>`, for an on-demand
     publish without cutting a release tag.
   No tag scheme exists yet in the repo (workspace version is `0.1.0`
   pre-release, no git tags cut); this ADR establishes the first one.
   `docker/build-push-action` with `cache-from`/`cache-to: type=gha`
   amortizes the 57-minute cold-build cost (Dockerfile.prebuilt:8)
   across repeated pushes to the same branch.

5. **Visibility: public packages.** A private GHCR package would need
   every puller (a developer's laptop, a real k8s cluster, this repo's
   own `k8s-integration` job if it ever switched from local kind-load to
   a registry pull) to hold a PAT with `read:packages` and an
   `imagePullSecret` for cluster use. Nothing in this codebase is
   currently secret-gated at the image layer, and public GHCR packages
   still require write auth to push, so build integrity is unaffected.
   Access is then `docker pull ghcr.io/nofireai/ravel-server:<tag>`,
   no login step. Flipping to private later is a GHCR setting, not an
   architecture change.

6. **Documentation**: a new "Container images" section in README.md
   (pull commands, tag scheme, which Dockerfile produces the published
   image) and an update to `deploy/k8s/operator/operator.yaml`'s
   placeholder-tag comment pointing at the published `ravel-operator`
   tags as the real-cluster alternative to a kind-loaded local image.

## Rejected alternatives

- **ECR.** Rejected in decision 1: no existing AWS account or IAM
  presence in this repo to build on, and OIDC federation setup is pure
  overhead next to GHCR's zero-config `GITHUB_TOKEN` path for a
  GitHub-hosted project.
- **Multi-arch (amd64 + arm64) images now.** Deferred, not rejected
  outright: cross-building arm64 on an amd64 runner re-introduces the
  same QEMU-vs-rustc SIGSEGV this ADR exists to route around, so it
  needs GitHub's native arm64-hosted runners assembled into a manifest
  list, which is a larger, independently reviewable change. Nothing in
  the current deploy targets (kind on the CI host, `deploy/k8s/`
  examples) requires arm64 images today. Tracked as a follow-up, not
  blocking amd64 publishing.
- **Publishing from `Dockerfile.prebuilt`.** Rejected in decision 2: its
  glibc and binary-set differences from the shipping image are
  deliberate CI-lane optimizations, not properties a published,
  externally-pulled image should inherit.
- **Reusing the `docker-build` job for push.** Rejected in decision 4:
  that job's whole purpose (per its own comments, ci.yml:641-664) is a
  cheap, path-gated build-only proof that the shipping Dockerfile still
  works; giving it `packages: write` and registry credentials widens its
  blast radius for no benefit, since the publish workflow already
  re-verifies the build as its first step.

## Consequences

- First registry and first tag scheme in the repo; both are visible,
  reviewable config, not implicit convention.
- `docker-build` stays cheap and credential-free; only the new publish
  workflow can push.
- A real (non-kind) Kubernetes deployment gets a documented image
  source for the first time; `deploy/k8s/operator/operator.yaml`'s
  placeholder tag now has a stated real-world replacement.
- arm64 images remain a known gap, explicitly deferred rather than
  silently absent.
