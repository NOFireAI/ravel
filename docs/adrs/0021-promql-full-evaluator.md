# ADR-0021: Full PromQL evaluator scope and phasing, differential harness against pinned Prometheus

Status: Accepted

## Context

ADR-0007 decided the approach: promql-parser for parsing, our own
evaluator, and a differential-testing gate ("no PromQL feature is done
until it passes differential tests against a pinned Prometheus binary").
What exists today is the Phase 1 slice of that decision: a selector-only
evaluator (instant and range, lookback, offset, all matcher types) in
crates/ravel-promql, with no function library, no binary operators, no
aggregations, no subqueries, and no differential harness at all. A
quality audit confirmed the gap and its consequence: the evaluator
is validated only against an oracle that shares its own assumptions, and
a real semantic defect (stale-marker NaNs surfacing as live values)
went unnoticed because no external oracle exists.

This ADR does not reopen ADR-0007's decisions. It records the decisions
needed to implement the rest of it: evaluator architecture, semantics
target, exactness policy, harness design, and CI placement.

## Decision

1. **Architecture: recursive tree-walk interpreter, per-step evaluation,
   synchronous SeriesSource retained.** The evaluator becomes a typed
   recursive interpreter over the promql-parser AST (values: scalar,
   string, instant vector, range matrix), evaluating range queries one
   step at a time exactly as Prometheus' engine does. PromQL numerics
   never lower to SQL or DataFusion (ADR-0006, ADR-0013 non-negotiable;
   both reaffirmed, not modified). The `SeriesSource` trait stays
   synchronous; a new selector-planning pass (`collect_selectors`) walks
   the AST and reports every selector's matchers, range, offset, and @
   timestamp so ravel-query can prefetch all needed windows before
   evaluation, replacing its current single-selector prefetch.

2. **Semantics target: Prometheus 3.x, one pinned patch release.** The
   reference is the pinned binary's behavior, not the documentation.
   Prometheus 3 semantics are the target throughout: left-open range
   selectors and lookback windows (the existing evaluator already
   implements exclusive-start), 3.x aggregation numerics, 3.x histogram
   edge cases. The pin is a concrete version and per-platform sha256 in
   the harness fetch script (initial pin: the current 3.5 LTS patch
   release; recorded exactly at rig-build time). Upgrading the pin is a
   deliberate change gated on a full differential run, like the
   arrow/datafusion version policy in ADR-0013.

3. **Exactness policy: mirror the algorithm, gate bit-exact, allowlist
   nothing silently.** Ravel implements Prometheus' floating-point
   algorithms operation-for-operation (counter-reset handling and
   extrapolation in rate/increase, Kahan-compensated and incremental
   accumulations where the pinned version uses them, Welford-style
   stddev/stdvar, quantile interpolation, histogram bucket coercion).
   The differential comparator compares values by `f64::to_bits`, with
   exactly two principled relaxations: NaN compares as a class because
   Prometheus' JSON API cannot carry NaN payloads (payload preservation
   stays covered by Ravel-internal bit-pattern tests, per the existing
   dedup-path discipline; -0.0 vs 0.0 remains significant and IS
   compared bit-exact), and result order is normalized by sorting on
   labels except for corpus entries marked order-sensitive (sort,
   sort_desc, topk, bottomk). Any further tolerance requires a
   per-function allowlist entry in the harness with a written
   justification; the allowlist starts empty and is expected to stay
   empty.

4. **Harness design: pinned static binary, remote-write ingestion,
   in-process Ravel stack.** A new dev-only workspace crate,
   crates/ravel-promql-difftest, generates seeded datasets, ingests the
   identical logical samples into both sides (Prometheus via its
   remote-write receiver on a version-pinned static binary; Ravel via
   the in-process ingest path over MemoryStore, not via OTLP
   translation, so the differential isolates evaluator and query
   semantics from ingest mapping), runs the same corpus of queries
   against both HTTP APIs, and diffs results under the policy in (3),
   including error-class and warning comparison. Binary over container:
   CI today runs no service containers (the MinIO suite already skips
   there), a static Go binary is trivially cached and checksum-pinned,
   and process startup is faster and less flaky than docker-in-CI.

5. **CI placement: curated corpus on every PR, extended sweep nightly
   and on demand.** The PR job downloads (cache-hit in the common case)
   the pinned binary and runs the curated deterministic corpus; target
   under ~3 minutes, no network beyond localhost, fixed seeds, so the
   flakiness argument against PR-gating does not apply. The extended
   run (large randomized-but-seeded query and dataset sweeps) runs on a
   schedule and via a manual trigger, because its value is breadth, not
   latency. Tradeoff accepted: PR gating costs a binary cache and ~3
   minutes per PR and buys immediate regression detection on the exact
   surface ADR-0007 declared gate-worthy; a nightly-only gate would let
   a semantics regression merge and surface a day later with the
   offending PR unidentified. Failing runs archive the query, dataset
   seed, and both raw JSON responses as CI artifacts.

6. **Scope boundary: native histograms are explicitly out.** The
   function library ships its classic-bucket histogram forms
   (histogram_quantile and histogram_fraction over `le`-labeled series,
   which need zero new storage) as an early, independent phase. The
   native-histogram forms (histogram_count, histogram_sum,
   histogram_avg, and the native paths of histogram_quantile and
   histogram_fraction) are a distinct final phase blocked on the
   separate histogram-storage planning effort (Remote Write/OTLP
   histogram ingest, possible RSEG change); nothing in this plan
   assumes a native-histogram representation exists. Functions the
   pinned Prometheus ships only behind
   `--enable-feature=promql-experimental-functions` (sort_by_label,
   sort_by_label_desc, limitk, limit_ratio, mad_over_time, info,
   double_exponential_smoothing) are deferred; if implemented later,
   their differential runs enable the same flag on the Prometheus side.

## Alternatives rejected

- Lower PromQL onto DataFusion: already rejected by ADR-0006/0013;
  nothing changed the facts (lookback, staleness, counter resets do not
  map onto relational operators without semantic loss).
- promtool tsdb backfill (OpenMetrics text) instead of remote write:
  offline and deterministic, but the text format cannot carry arbitrary
  f64 bit patterns, adds a block-construction step whose alignment rules
  become our problem, and exercises a code path production Prometheus
  ingestion does not use. Remote write carries exact f64 bits in
  protobuf and drives the same head-append path real deployments use.
- Scrape-based ingestion into Prometheus: timestamps assigned at scrape
  time; irreproducible by construction.
- Container-pinned Prometheus: acceptable for local dev convenience, but
  makes the CI job depend on a container runtime CI does not currently
  use; a digest-pinned image is documented as an optional local
  alternative, the checksummed binary is the contract.
- Differential gate nightly-only: rejected, see (5).

## Consequences

- ADR-0007's "done" gate becomes enforceable: every function/operator
  phase in the plan lands together with its corpus entries, and CI
  blocks regressions on every PR.
- The workspace gains a dev-only crate with heavier dev-dependencies
  (HTTP client, remote-write protobuf encoding via existing prost +
  snap); none of it is reachable from production crates.
- The evaluator's public API (`Evaluator::instant`/`range` in, Ravel
  types out) is preserved, but ravel-query's prefetch contract changes
  from "the one selector's window" to "every selector window the plan
  reports"; docs/query-engine.md is updated in the same phase.
- Known defects on this surface (staleness handling, unbounded grid) are
  fixed as part of the evaluator-core phase rather than patched
  separately, and the paren-unwrap fix becomes an explicit prerequisite
  landed before the core phase.
- A future Prometheus pin bump can surface upstream behavior changes as
  corpus diffs; that is the designed failure mode (drift becomes
  visible), same consequence ADR-0007 already accepted.
