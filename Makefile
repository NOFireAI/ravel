.PHONY: check fmt clippy test build minio minio-down demo bench audit

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
