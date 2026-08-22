# Evidence: commands run

Every command, real exit code, decisive output lines. Appended as the review progresses.

## Provenance

```
$ git rev-parse HEAD
527a16db2e4d47b2924e4de4a4db32d7583fda33          # exit 0
$ git log -1 --format=%cI
2026-08-22T22:53:40+03:00                          # exit 0
$ git tag --sort=-creatordate | head -20
(no tags in this clone)                            # exit 0
$ rustc --version
rustc 1.97.1 (8bab26f4f 2026-07-14)                # exit 0
$ cargo --version
cargo 1.97.1 (c980f4866 2026-06-30)                # exit 0
$ nproc; free -h; df -h .
8 cores, 15 GiB RAM, 100 GB free on /var/lib/fleet # exit 0
```

Note: the dispatch brief described the host as 8 GB RAM / 4 cores; measured host is 8 cores / 15 GiB. Build jobs still capped at --jobs 4 for safety.

## Inventory

```
$ ls crates/ services/
28 crates, 4 services (ravel-cli, ravel-ingest-router, ravel-operator, ravel-server)   # exit 0
$ ls docs/adrs/ | wc -l
105 (0001..0104 plus README)                       # exit 0
$ find crates services -name '*.rs' | wc -l ; ... | wc -l (lines)
690 files, 408442 lines                            # exit 0
$ git log --oneline | wc -l
1 (dispatch clone has squashed/single-commit history; commit-count-based maturity inference impossible and per charter not used)  # exit 0
$ which cargo-nextest cargo-deny cargo-audit
cargo-nextest present; cargo-deny absent; cargo-audit absent   # exit 0 for nextest
$ cargo metadata --format-version 1 > /tmp/cargo-metadata.json
exit 0
```
