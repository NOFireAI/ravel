.PHONY: check fmt clippy test test-python doc-drift build minio minio-down demo kind-up kind-demo kind-down bench audit difftest

check: fmt clippy test

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

# Unit tests for scripts/process_metrics.py (SP0), which had no runner at all
# (ADR-0053 decision 6). stdlib unittest rather than pytest: the script and its
# tests deliberately take no third-party dependency, and CI has no Python
# environment beyond the interpreter. Run from scripts/ so the tests'
# `from process_metrics import ...` resolves without a package layout.
test-python:
	cd scripts && python3 -m unittest discover -p 'test_*.py' -v

# Derived counts in docs/query-engine.md's generated conformance block
# (ADR-0053 decision 6). No build, so it is cheap enough to run before a
# commit; the full regeneration lives in ravel-promql-difftest's
# conformance_table test.
doc-drift:
	./scripts/check-doc-drift.sh

build:
	cargo build --workspace --release

minio:
	docker compose -f deploy/docker-compose/minio.yml up -d

minio-down:
	docker compose -f deploy/docker-compose/minio.yml down

demo: build
	./scripts/demo.sh

# The same ingest/query round trip on a real local Kubernetes cluster, driven
# by the operator (ADR-0034 decision 6; docs/guides/kubernetes.md). No `build`
# prerequisite: kind-up.sh builds the binaries inside the container image, and
# kind-demo.sh only needs the gen_otlp_fixture example, which cargo builds
# on demand.
kind-up:
	./scripts/kind-up.sh

kind-demo:
	./scripts/kind-demo.sh

kind-down:
	./scripts/kind-down.sh

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
