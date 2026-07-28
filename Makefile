.PHONY: check fmt clippy test build minio minio-down demo bench audit difftest

check: fmt clippy test

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

build:
	cargo build --workspace --release

minio:
	docker compose -f deploy/docker-compose/minio.yml up -d

minio-down:
	docker compose -f deploy/docker-compose/minio.yml down

demo: build
	./scripts/demo.sh

bench:
	cargo bench --workspace

audit:
	cargo audit
	cargo deny check

# PromQL differential test (docs/promql-evaluator-plan.md section 5): fetches
# the pinned Prometheus binary (verifying its sha256; see
# scripts/fetch-prometheus.sh) and runs the corpus against both it and
# Ravel's own evaluator.
difftest:
	RAVEL_DIFFTEST_PROM_BIN="$$(scripts/fetch-prometheus.sh)" \
		RAVEL_DIFFTEST=1 \
		cargo test -p ravel-promql-difftest --test difftest_selectors -- --nocapture
