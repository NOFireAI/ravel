# Evidence log: commands, exit codes, decisive output

Appended chronologically as the review runs. Each entry: command, real exit
code, and the shortest decisive output lines.

## Provenance

```
$ git rev-parse HEAD
527a16db2e4d47b2924e4de4a4db32d7583fda33   (exit 0)

$ git log -1 --format=%cI
2026-08-22T22:53:40+03:00                  (exit 0)

$ git tag --sort=-creatordate | head -20
(no output: repository has no tags)        (exit 0)

$ rustc --version
rustc 1.97.1 (8bab26f4f 2026-07-14)        (exit 0)

$ cargo --version
cargo 1.97.1 (c980f4866 2026-06-30)        (exit 0)

$ git status --short
?? node_modules                            (exit 0)
```

## Host facts

```
$ nproc            -> 4
$ free -h          -> 7.9Gi total, 6.6Gi available
$ df -h .          -> /dev/nvme0n1p2 468G total, 404G free (10% used)
```

## Tooling availability

```
$ cargo nextest --version  -> error: no such command: `nextest`   (absent)
$ cargo deny --version     -> error: no such command: `deny`      (absent)
$ cargo audit --version    -> error: no such command: `audit`     (absent)
$ which docker             -> /usr/bin/docker  (present)
$ docker ps                -> permission denied on /var/run/docker.sock (unusable)
```

Docker daemon unreachable (permission denied), so MinIO/kind/Kubernetes
integration paths are NOT ASSESSED (environmental). cargo-nextest absent, so
`cargo test` is used in place of `cargo nextest run`. cargo-deny and
cargo-audit absent; supply-chain review is done by reading Cargo.lock,
deny.toml, and Cargo.toml manifests directly.

## Inventory

```
$ find crates services -name '*.rs' | xargs wc -l | tail -1  -> 408442 total
$ grep -rl '#[test]|#[tokio::test]' crates services | wc -l  -> 507 files with tests
Workspace crates: 29 (crates/) + 4 services (ravel-cli, ravel-ingest-router,
  ravel-operator, ravel-server)
ADRs: 104 (docs/adrs/0001..0104)
CI workflows: ci.yml, bench-s3.yml, coderabbit-maintainer-review.yml,
  k8s-nightly.yml, publish-images.yml, quickstart-published.yml, sim-nightly.yml
```

## Build and test runs

(appended as each completes)
