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

## Build and lint gates

```
$ cargo fmt --all --check
exit 0 (clean)
$ cargo tree -d > /tmp/cargo-tree-d.log
exit 0; 1760 lines of duplicate-version output (analyzed under supply chain)
$ timeout 10 docker info; kind version; kubectl version --client
docker exit 0 (available); kind exit 127 (absent); kubectl exit 127 (absent)
$ cargo clippy --workspace --all-targets --jobs 4 -- -D warnings
exit 0 (clean), 2m59s wall. Decisive line: "Finished `dev` profile ... in 2m 59s", no warnings.
```

## Test batch 1 (high-risk crates, cargo-nextest)

```
$ cargo clippy -p ravel-server --features sql --all-targets --jobs 4 -- -D warnings
exit 0
$ cargo clippy -p ravel-server -p ravel-sql --features flight-sql --all-targets --jobs 4 -- -D warnings
exit 0
$ cargo nextest run --jobs 4 -p ravel-types -p ravel-object-store -p ravel-commit -p ravel-catalog -p ravel-segment -p ravel-logseg
exit 0: "Summary [16.811s] 975 tests run: 975 passed, 4 skipped"
$ cargo nextest run --jobs 4 -p ravel-ingest -p ravel-promql -p ravel-maintain -p ravel-rspan -p ravel-query
exit 0: "Summary [10.877s] 1250 tests run: 1250 passed, 2 skipped"
$ cargo nextest run --jobs 4 -p ravel-failure-tests
exit 0: "Summary [0.101s] 20 tests run: 20 passed, 0 skipped"
```

## Live MinIO for contract tests

```
$ docker run -d --name ravel-minio -p 127.0.0.1:19000:9000 ... minio/minio:latest server /data
exit 0 (first attempt on port 9000 failed exit 125, port already in use by unrelated host workload)
$ docker run --rm --network host minio/mc ... mc mb local/ravel-test
exit 0: "Bucket created successfully `local/ravel-test`."
```

## Test batch 2 (contract, differential, feature lanes)

```
$ RAVEL_MINIO_URL=http://127.0.0.1:19000 ... cargo nextest run --jobs 4 -p ravel-object-store
exit 0: "Summary [8.301s] 116 tests run: 116 passed, 0 skipped"
Decisive line: "PASS [1.182s] (112/116) ravel-object-store::contract minio_contract"
(the S3 contract suite ran against LIVE MinIO, not just MemoryStore)
$ scripts/fetch-prometheus.sh
exit 0: fetched sha256-verified prometheus-3.13.1.linux-amd64
$ RAVEL_DIFFTEST=1 RAVEL_DIFFTEST_PROM_BIN=.../prometheus cargo nextest run --jobs 4 -p ravel-promql-difftest
exit 0: "Summary [2.202s] 91 tests run: 91 passed, 0 skipped"
Decisive lines: "PASS ... difftest_selectors selector_and_error_corpus_match_pinned_prometheus"
"PASS ... conformance_table query_engine_doc_table_matches_a_real_run"
(PromQL differential vs a REAL Prometheus 3.13.1 binary executed and passed)
$ cargo nextest run --jobs 4 -p ravel-server --features sql
exit 0
$ cargo nextest run --jobs 4 -p ravel-server --features sql
exit 0: "Summary [219.862s] 623 tests run: 623 passed (2 slow), 2 skipped"
$ cargo nextest run --jobs 4 -p ravel-sql --test differential
exit 0: "Summary [5.406s] 14 tests run: 14 passed, 0 skipped"
$ cargo nextest run --jobs 4 -p ravel-server -p ravel-sql --features flight-sql
exit 0: "Summary [221.769s] 1092 tests run: 1092 passed (2 slow), 4 skipped"
$ cargo nextest run --jobs 4 -p ravel-segment --test fuzz_mutation
exit 0: "Summary [1.866s] 10 tests run: 10 passed, 0 skipped"
$ cargo nextest run --jobs 4 -p ravel-otap --test fuzz_mutation
exit 0: "Summary [0.480s] 5 tests run: 5 passed, 1 skipped"
$ cargo nextest run --jobs 4 -p ravel-otlp -p ravel-remote-write -p ravel-alerting -p ravel-analytics -p ravel-codec -p ravel-tenant-resolve -p ravel-cache -p ravel-fleet -p ravel-affinity -p ravel-sim
exit 0: "Summary [28.573s] 556 tests run: 556 passed, 0 skipped"
```

## Test batch 3 (services, doctests, store qualification)

```
$ cargo nextest run --jobs 4 -p ravel-server -p ravel-operator -p ravel-ingest-router -p ravel-cli
exit 0: "Summary [237.869s] 887 tests run: 887 passed (2 slow), 3 skipped"
$ cargo test --doc --workspace --jobs 4
exit 0: 32 "test result: ok" lines, zero failures
$ RAVEL_S3_ENDPOINT=http://127.0.0.1:19000 ... cargo run -p ravel-cli -- --store s3 ... store qualify
exit 0. All four blocking probes PASS against live MinIO:
  conditional_write_create_if_absent  PASS (loser rejected AlreadyExists, winner bytes intact)
  conditional_write_cas_version       PASS
  consistent_read_after_write         PASS (5 cycles)
  consistent_list_after_write         PASS (5 cycles)
  object_lock/versioning, bucket/versioning, lifecycle probes: "unknown (informational, non-blocking)"
  "wrote sys/qualification: s3://ravel-test@http://127.0.0.1:19000 qualified (suite v1)"
(confirms the qualification suite runs and also confirms its thinness: 4 blocking probes,
 bucket-protection state unobservable, as flagged in memos B and later verified)
```

## Supply chain

```
$ cargo-deny 0.18.4 check
exit 1: advisory DB parse failure on CVSS 4.0 entries (tool too old, environmental)
$ cargo-deny 0.20.2 check   (prebuilt musl binary from GitHub release)
exit 0: "advisories ok, bans ok, licenses ok, sources ok"
cargo-audit: not installed; cargo deny's advisories check covers RustSec, so not separately installed.
```
