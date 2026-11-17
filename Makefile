.PHONY: check fmt clippy test test-python test-hygiene doc-drift check-docs build rustfs rustfs-down demo quickstart quickstart-down kind-up kind-demo kind-down bench audit difftest archmap test-sdk lint-sdk brig brig-new brig-shell brig-plan brig-stop brig-rm brig-update brig-login

check: fmt clippy test

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

# Unit tests for the Python scripts under scripts/ (ADR-0053 decision 6).
# stdlib unittest rather than pytest: the scripts and their tests
# deliberately take no third-party dependency, and CI has no Python
# environment beyond the interpreter. Run from scripts/ so the tests'
# `from process_metrics import ...` resolves without a package layout.
# Supported interpreter floor: Python 3.9 or newer (fleet executors run
# 3.9.25). Keep the scripts import-clean on 3.9: `from __future__ import
# annotations` plus typing.Optional[...] rather than the 3.10 `X | None` form.
test-python:
	cd scripts && python3 -m unittest discover -p 'test_*.py' -v

# Grep-shaped hygiene guard over the test sources (issue #778): wall-clock
# assertions, unseeded randomness, untracked proptest regression seeds. Build
# free, so run it before a commit that touches tests. Its own cases run first.
test-hygiene:
	bash scripts/guards/check-test-hygiene.test.sh
	./scripts/guards/check-test-hygiene.sh

# Regenerate docs/explorer/stats.js (per-crate counts and the dependency
# graph the explorer page aggregates); commit it with any docs/explorer change.
archmap:
	python3 scripts/archmap/gen_stats.py

# Derived counts in docs/query-engine.md's generated conformance block
# (ADR-0053 decision 6). No build, so it is cheap enough to run before a
# commit; the full regeneration lives in ravel-promql-difftest's
# conformance_table test.
doc-drift:
	./scripts/check-doc-drift.sh

# Documentation gate (ADR-1040 decision 3): links, anchors, terminology,
# provenance, orphaned pages, SVG validity, and index completeness. Python
# stdlib only, so like test-python and doc-drift it is cheap enough to run
# before a commit. Every finding fails the gate; there is no baseline file.
check-docs:
	python3 scripts/check_docs.py
	python3 scripts/check-doc-claims.py

# TypeScript Agents SDK (ADR-3331 decision 11). Each target installs the
# locked dependencies with `npm ci` once, then runs its scripts. Needs Node 22
# (.node-version).
test-sdk:
	cd sdk/typescript && npm ci && npm run typecheck && npm test

lint-sdk:
	cd sdk/typescript && npm ci && npm run lint

build:
	cargo build --workspace --release

rustfs:
	docker compose -f deploy/docker-compose/rustfs.yml up -d

rustfs-down:
	docker compose -f deploy/docker-compose/rustfs.yml down

demo: build
	./scripts/demo.sh

# Container-first quickstart (ADR-0081): the whole stack from published images
# -- RustFS, ravel-server, an OpenTelemetry Collector, and Grafana -- with no
# cargo build. Override the pinned server image with RAVEL_IMAGE. This is the
# documented first run; `demo` above stays the from-source contributor path.
quickstart:
	docker compose -f deploy/docker-compose/ravel.yml up -d

quickstart-down:
	docker compose -f deploy/docker-compose/ravel.yml down

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

# Claude Code in a Brig sandbox (8 vCPUs, 12 GB) on a clone of its own, with
# the Claude login and the GitHub token kept on the host. Run these from your
# primary checkout or a worktree, never from inside a sandbox clone. Needs brig
# from the main channel: brew install --cask brig-sh/brig/brig@main.
# One Ravel sandbox at a time: each clone's first build writes 54 to 70 GB,
# so these refuse below 70 GB free, and all but brig-new refuse beside
# another running Ravel sandbox.
# When a ticket merges, make brig-rm NAME=x and delete ../store-brig/x.
# See docs/internal/dev-in-brig.md.
#
#   make brig-login                  store the secrets the routes send, once
#   make brig-new NAME=4405-x        clone origin/main into a sandbox clone
#   make brig NAME=4405-x            Claude Code in that clone's sandbox
#   make brig NAME=4405-x ARGS=--continue
#   make brig-shell NAME=4405-x      a login shell there instead
#   make brig-plan NAME=4405-x       what a run gets, booting nothing
#   make brig-stop NAME=4405-x       stop the sandbox
#   make brig-rm NAME=4405-x         remove the sandbox and its guest home
#   make brig-update NAME=4405-x     remove the sandbox, pull the newest image, run
brig:
	@scripts/brig.sh run "$(NAME)" $(ARGS)

brig-new brig-shell brig-plan brig-stop brig-rm brig-update:
	@scripts/brig.sh $(@:brig-%=%) "$(NAME)"

brig-login:
	@scripts/brig.sh login
