# Agent G memo: observability product correctness

## Verdict

Ravel is a credible Prometheus-compatible long-term metrics store with unusually strong evidence discipline (a bit-exact differential gate against a pinned Prometheus 3.13.1, honest conformance tables, typed rejections on most unsupported paths), and its metrics ingest, classic histogram, exemplar, metadata, and Remote Write surfaces are genuinely production-shaped. Its one serious correctness defect is the native (exponential) histogram query surface: outside a short list of blessed patterns (histogram_quantile, histogram_fraction, histogram_count/_sum/_avg, and instant-query rate/sum/avg), native histogram data silently produces wrong numbers (zeros, phantom series) or silently vanishes, in both instant and range queries, with no warning, no annotation, and no typed error; I demonstrated this empirically against the real evaluator. Logs and traces are durable, compacted, retained, and SQL-queryable, but they are storage plus a query language, not finished log-search or tracing products: there is no LogQL/Loki surface, no trace-by-id API, no Tempo/Jaeger/TraceQL compatibility, and the project's own correlation guide tells users to point Grafana's exemplar link at an external tracing datasource. Alerting is architecturally sound (durable state, leased evaluation, Alertmanager v2 sink with auth) but operationally immature (static JSON rules file loaded at startup, no recording rules, no rules API). As a Prometheus replacement for float metrics and classic histograms it is close; as a unified observability backend it is technically present but product-incomplete in traces, logs UX, and native histograms, and those three should be labeled beta.

## Evidence

### Native (exponential) histograms: ingest and storage are real

- OTLP cumulative ExponentialHistogram points are admitted as native histogram samples and materialized into segment histogram values; delta temporality is rejected typed; scale -53 is rejected because OTLP cannot carry custom boundaries (crates/ravel-otlp/src/normalize.rs:16-29, 1041-1123). STRONGLY SUPPORTED.
- Remote Write 2.0 materializes native histograms field-for-field (crates/ravel-remote-write/src/rw2.rs:305-334; test `resolves_every_histogram_field_verbatim` at rw2.rs:613, test `materializes_histograms_and_exemplars` at rw2.rs:577). RW1 has a parallel resolve path. STRONGLY SUPPORTED.
- The read path exists end to end: `SeriesSource::query_histograms` (crates/ravel-promql/src/source.rs:227-233) is implemented by ravel-query's `MergedSource` (crates/ravel-query/src/engine.rs:2749), and the difftest corpus header records a confirmed end-to-end run (ingest through RSEG v5 through the evaluator) against real Prometheus on 2026-07-30 (crates/ravel-promql-difftest/corpus/histogram_native.txt:1-11). STRONGLY SUPPORTED.
- The blessed query patterns are differentially verified bit-exact (with one documented 2-ULP libm allowlist entry) against Prometheus 3.13.1: histogram_count/_sum/_avg, quantile clamp, median and p99 quantile, fraction, rate/increase, sum/avg aggregation, including the canonical Grafana p99 range query `histogram_quantile(0.99, selector)` (corpus/histogram_native.txt:52-157). My own probe confirms `histogram_quantile(0.9, sum(rate(h[5m])))` works in both instant and range mode. VERIFIED.

### Native histograms: the rest of the query surface silently lies

I built a probe binary against ravel-promql's public `Evaluator` and `TestSource` (which supports histogram series, crates/ravel-promql/src/testsource.rs:77-107) and ran the full operator surface over a native histogram counter series `h` and a float series `f`. Observed behavior (probe output quoted in "Tests or commands run"):

Silent wrong values (no annotation, no error):
- `h * 2`, `2 * h`, `h + h` return float 0 (Prometheus returns a scaled/added histogram). Cause: every binop arm reads only `s.value`, which is a 0.0 placeholder on histogram elements, and constructs `histogram: None` outputs (crates/ravel-promql/src/binop.rs:157-188 at 167-172 and 183, 334-341, 384-396; placeholder documented at crates/ravel-promql/src/eval.rs:99-105). VERIFIED.
- `h + on(job) f` returns 0 + 20 = 20 as a real-looking series (Prometheus drops the pair with an annotation). VERIFIED.
- Filter comparisons treat the histogram as 0.0: `h < 1` and `h == 0` return phantom series (Prometheus drops histogram members of comparisons with an info annotation); `h > 0` returns empty only by arithmetic accident. VERIFIED.
- `topk(1, h)` and `quantile(0.5, h)` emit elements valued 0 (Prometheus skips histograms with an info annotation). `abs(h)`, `ceil(h)`, `clamp_max(h, 5)` return 0-valued floats. VERIFIED.
- Range queries whose result is histogram-valued at any grid step return all-zero series: range `sum(h)`, range `h * 2`, and, most importantly, range `sum(rate(h[5m]))` returned `values=[0.0, 0.0, ...]` for a series whose instant evaluation is a correct histogram. Cause: the generic range-grid stitcher keeps only `s.value` per step (crates/ravel-promql/src/functions/mod.rs:558-601 at 567-583) and the `RangeCore::Generic` arm applies no histogram guard (crates/ravel-promql/src/eval.rs:704-711). VERIFIED.

Silent missing data (no annotation, no error):
- Range `rate(h[5m])`, `increase`, `delta`: the range arm of `RangeVectorFloatOrHist` deliberately reduces only float series (crates/ravel-promql/src/functions/mod.rs:415-444), while the instant arm unions in the histogram reduction (mod.rs:238-266). The same query returns data as an instant query and an empty matrix as a range query. VERIFIED.
- Range bare selector `h` returns an empty matrix: `eval_range_selector` calls only `source.query`, never `query_histograms` (crates/ravel-promql/src/eval.rs:1009-1072 at 1048). The matrix type itself cannot carry histograms (`RangeMatrix` holds float `Sample`s, eval.rs:146; the HTTP `MatrixResult` has only a `values` field, crates/ravel-query/src/http/json.rs:278-281, while the instant path renders a proper Prometheus `histogram` field, json.rs:264-274 and test at 787). VERIFIED.
- `sum_over_time`, `avg_over_time`, `count_over_time`, `last_over_time`, `resets` over `h[5m]` all return empty (Prometheus 3.x supports native histograms in several of these; count_over_time in particular is defined for them). VERIFIED (probe); the Prometheus-side expectation is DOCUMENTED CLAIM from Prometheus semantics, not re-verified here.

Guarded paths (typed error, correct failure behavior, credit due):
- Subquery over matched native histogram data returns `Error::Unsupported` (HTTP 422) precisely to avoid "a wrong empty answer" (crates/ravel-promql/src/eval.rs:938-961; docs/query-engine.md:314-321). `histogram_stddev`/`histogram_stdvar` are typed rejections (docs/query-engine.md:1114-1115). VERIFIED (probe printed the typed errors).

The contradiction: the codebase applies the "detect histogram data and reject typed rather than silently drop" principle to exactly one of the several paths with the identical hazard. The corpus header itself concedes raw histogram-valued results are deferred and annotation-emitting cases are excluded from the gate (corpus/histogram_native.txt:13-22), so the differential gate structurally cannot see any of the failures above. The internal rationale comments are stale in two places: crates/ravel-promql/src/histogram.rs:13-24 still claims no native histogram sample can reach the evaluator (contradicted by the corpus header and MergedSource), and functions/mod.rs:415-418 claims "no read path to feed native histograms into a range query yet" when the read path exists and the instant arm of the same registration uses it. CONTRADICTED (code vs its own comments).

- Aggregations: `sum`/`avg` over pure histogram groups are correct and difftest-pinned; a mixed float/histogram group is dropped (matching Prometheus' value behavior) but with no annotation, though the code comment describes Prometheus' annotation (crates/ravel-promql/src/aggregate.rs:251-271); `min`/`max`/`stddev`/`stdvar` ignore histogram members silently (aggregate.rs:299-314); `count` correctly counts them (aggregate.rs:292-297, probe). VERIFIED.

### Classic histograms, PromQL general surface, Prometheus interop

- Classic OTLP histograms and summaries explode to Prometheus-convention series (`_bucket`/`le`, `_sum`, `_count`, `quantile`) per ADR-0016 (crates/ravel-otlp/src/normalize.rs:13-15). `histogram_quantile` over classic buckets, bad-bucket warnings, and forced-monotonicity infos match the pinned Prometheus, including the subtle no-annotation case for a missing +Inf bucket (crates/ravel-promql/src/eval.rs:249-268; tests `histogram_quantile_missing_inf_bucket_is_nan_without_a_warning`, `histogram_quantile_single_bucket_still_surfaces_a_bad_buckets_warning`, `histogram_quantile_non_monotonic_buckets_surface_an_info` at eval.rs:1608-1671). STRONGLY SUPPORTED (difftest corpus histogram_classic.txt exists; I did not rerun the gate).
- Function registry covers the stable Prometheus surface (69 functions enumerated by grep over functions/*.rs), including rate/irate/increase/delta family, over_time family, label_replace/label_join, predict_linear, deriv, time functions, absent/absent_over_time. Missing: sort_by_label(_desc), mad_over_time, first_over_time, histogram_stddev/stdvar, double_exponential_smoothing, info() (all experimental or new in Prometheus; classified in docs/query-engine.md:984 and the conformance table). STRONGLY SUPPORTED.
- Annotations (warnings/infos) exist as a first-class channel with Prometheus' severity split and text (eval.rs:178-268) and are wired to the HTTP envelope. The gap is coverage on histogram paths, per above. VERIFIED for the covered cases (unit tests at eval.rs:1687, 1841).
- Staleness markers are honored end to end: OTLP NoRecordedValue maps to the stale NaN (normalize.rs:79-93), and the evaluator filters `STALE_NAN_BITS` in windows (eval.rs:869; dedicated test file crates/ravel-promql/tests/staleness_markers.rs). STRONGLY SUPPORTED.
- Counter reset handling is value-decrease based, as in Prometheus; OTLP `start_time_unix_nano` is not read anywhere in normalize.rs (grep returned nothing), so created-timestamp-assisted reset detection does not exist; RW created timestamps are counted and dropped (services/ravel-server/src/remote_write.rs:78-81). Equivalent to Prometheus without the created-timestamp feature. VERIFIED (grep).

### OTLP metrics semantic fidelity

- Delta temporality (Sum, Histogram, ExponentialHistogram) is rejected typed with a documented rejection class; stateless compute cannot do delta-to-cumulative (normalize.rs:16-18; docs/guides/ingest.md:179). Correct failure behavior, real ecosystem friction: delta-configured SDKs and vendors need a collector with deltatocumulative in front. VERIFIED (code + doc).
- Integer samples beyond 2^53 are admitted with a visible per-point precision-loss report (normalize.rs:5-12). Non-monotonic sums map to gauge kind. Unit and `_total` suffixing follows the collector's unitMapper convention (normalize.rs:1647-1705, ADR-0085), so OTLP and Prometheus-exporter arrivals of the same instrument share a name. STRONGLY SUPPORTED.
- Instrumentation scope is ignored for metric series identity, a documented deliberate simplification (normalize.rs:38-40). Consequence: two scopes emitting the same metric name and attributes in one tenant collapse to one series, and same-timestamp samples from both are deduplicated to one winner by commit order, silently. Prometheus' own OTLP ingestion distinguishes these via otel_scope_* labels; `target_info` is also absent (grep for target_info/otel_scope returned nothing outside logs). IMPLEMENTED WEAKLY VERIFIED (code read; collision behavior inferred from the documented dedup rule in crates/ravel-promql/src/source.rs:22-31).
- Attribute/name sanitization matches the OTel-Prometheus convention, including the documented leading-digit aliasing caveat (normalize.rs:47-53).

### Remote Write receiver

- RW 1.0 and 2.0 on `POST /api/v1/write`, strict acknowledgement only (buffered-mode header deliberately not honored here), content-type negotiation, 415/400/429/503 mapping, Retry-After, and the RW2 written-stats response headers (services/ravel-server/src/remote_write.rs:1-46, 155-160, 176, 339-359, 448). Compressed and decompressed body caps precede allocation (remote_write.rs:29-41). RW metadata now feeds the metadata sink (remote_write.rs:148-152). STRONGLY SUPPORTED.
- Active-series cap breaches reduce the written count inside a 2xx rather than erroring (remote_write.rs:125-131). For RW1 senders (no stats headers in that protocol) this is a silent partial drop from the sender's view; it is an explicit, documented admission policy with server-side counters, so I rate it hardening, not correctness. VERIFIED (code + doc comment).

### Prometheus HTTP API and Grafana

- Served: /api/v1/query, query_range (GET+POST), labels (GET), label/{name}/values, series (GET+POST), metadata, status/buildinfo, query_exemplars, plus /-/healthy and /-/ready (crates/ravel-query/src/http/mod.rs:149-163; compat.rs:70-71; services/ravel-server/src/exemplars.rs:243-245). The quickstart provisions a plain Grafana `type: prometheus` datasource (deploy/grafana/provisioning/datasources/ravel.yaml), and ADR-0039 records a real-Grafana Save & Test acceptance step. Instant native histogram elements render Prometheus' `histogram` JSON shape (json.rs:264-274). Missing: /api/v1/rules, /api/v1/alerts, format_query, parse_query, status/tsdb, POST /api/v1/labels. STRONGLY SUPPORTED.
- /api/v1/metadata returns real type/help/unit for post-ADR-0085 data from a metadata cache (http/mod.rs:143-148, README:44-49). STRONGLY SUPPORTED.

### Exemplars

- Stored in the RSEG EXEMPLARS section, capped at admission, copied through compaction, served on a Prometheus-shaped /api/v1/query_exemplars with trace_id/span_id labels, which is exactly what Grafana's exemplar click needs (services/ravel-server/src/exemplars.rs:1-35; docs/guides/correlation.md). OTLP native histogram exemplars are dropped informationally (normalize.rs:24-25); RW exemplars are materialized (rw2.rs test at 577). STRONGLY SUPPORTED.
- The Grafana exemplar-to-trace link requires "the tracing data source that holds the traces" (docs/guides/correlation.md:141-155): Ravel itself cannot be that datasource (see Traces), so the correlation story terminates outside the product unless the user hand-writes SQL.

### Logs

- OTLP logs: stream identity is the hash of resource plus scope (name, version, attributes), by construction equal between the id and the stored preimage (crates/ravel-otlp/src/logs_normalize.rs:5-13, 43-64). Severity, both timestamps (with receiver-clock fallback), trace_id/span_id, flags, and typed attributes (including nested arrays/kvlists) are preserved. Rejection is three-tier and visible via OTLP partial success. STRONGLY SUPPORTED.
- Structured bodies (array/kvlist) are rejected typed rather than stringified (logs_normalize.rs:332-350). Honest, but a real fidelity gap: OTel collectors and several SDKs emit structured bodies routinely; those records are refused, not degraded.
- Query surface is SQL only: `logs` table with ts, observed_ts, severity_num/text, body, trace_id, span_id, flags, attrs map (crates/ravel-sql/src/logs_schema.rs:37-84), per-tenant declared typed attribute columns (ADR-0090/0093, logs_schema.rs:115), `has_word` with postings acceleration (ADR-0049), streaming projected scans (ADR-0087), and Flight SQL. No LogQL, no Loki API (grep over crates/services found none), no live tail. For a Grafana user the logs experience is a SQL client, not a Logs panel.

### Traces

What exists, precisely: OTLP trace ingest (HTTP and gRPC, services/ravel-server/src/lib.rs:38, 1542-1543, traces_ingest.rs); durable RSPAN storage sorted by (trace_id, start_ts) with skip indexes, duration/status pruning, blooms over service and span name (docs/span-segment-format.md, ADR-0041/0045/0054); compaction, retention, and erasure through the generic maintain machinery (crates/ravel-maintain/src/publish.rs:292, rewrite.rs:275, rspan_codec.rs); and a registered `spans` SQL table with pushdown (crates/ravel-sql/src/session.rs:106-124; docs/guides/traces.md table at lines 54-66). Trace-by-id is a bounded per-object read but still lists every shard's segments in the window, documented honestly (guides/traces.md:30-38).

What does not exist: any trace-by-id HTTP API, trace search API, service/operation discovery endpoint, TraceQL, Jaeger or Tempo compatibility surface, or Grafana trace-view integration (grep for tempo/jaeger/traceql over docs and code: nothing). "We store spans durably and you can SQL them" is true and useful for batch investigation; it is not a production tracing backend a Grafana user can click into. The ravel-tracing-export crate is Ravel exporting its own spans to an external collector (ADR-0060), not a serving surface. VERIFIED (absence by grep plus guide reading).

### Alerting

- Engine: one generic rule shape covering PromQL threshold rules and SQL detection rules; pure state machine with pending/firing/resolved/suppressed; state derived by folding durable RLOG alert records, never in-memory timers, so a restarted evaluator resumes exactly (crates/ravel-alerting/src/lib.rs:1-56). Per-tenant evaluator tasks reuse the same query engines the HTTP endpoints serve (services/ravel-server/src/alerting.rs:1-50, 148-158). STRONGLY SUPPORTED.
- Duplicate evaluation is bounded by a per-tenant object-store lease (CreateIfAbsent fast path, TTL of three ticks, explicitly not a fencing token), and delivery is at-least-once with a bootstrap re-queue of non-terminal alerts after restart (alerting.rs:115-146, 355-363, 484-546, 555-587; module doc "No in-memory alert state"). Consequence: duplicate notifications are possible across lease handoffs; consumers must be idempotent, which the code documents. STRONGLY SUPPORTED.
- Sinks: generic webhook and Alertmanager /api/v2/alerts payload, bearer/basic auth with redaction discipline, 10 s timeout, failures retried next tick (services/ravel-server/src/alert_sink.rs:1-67). STRONGLY SUPPORTED.
- Operational shape: rules come from one static JSON file loaded at startup (services/ravel-server/src/config.rs:242-248; alerting.rs:1155-1200); changing a rule means editing a file and restarting; there is no rules API, no Prometheus rule-YAML compatibility, no /api/v1/rules for Grafana to display, and recording rules are explicitly deferred (docs/adrs/0043:86-96). NOT IMPLEMENTED (dynamic rule management, recording rules).

### Multi-tenant product operations (PM diligence)

- Cost attribution: per-query S3 request/byte accounting threaded explicitly (not task-local) through catalog, segment, and log fetchers, surfaced in query response stats and workload-class metrics (crates/ravel-types/src/accounting.rs:1-30, 484-531; ADR-0044/0061); ingest byte metrics per tenant (services/ravel-server/src/ingest_byte_metrics.rs). An operator can explain a bill to request-class granularity per tenant. STRONGLY SUPPORTED.
- Cross-tenant blast radius: layered admission (compressed-body cap, byte rate, series-creation rate, active-series cap, ADR-0051; process-wide ingest concurrency controller, remote_write.rs:138-141), query budgets and shard-aware request budgets (ADR-0075/0088), store request scheduling (ADR-0070). NOT ASSESSED in depth (Agent scope; I verified the surfaces exist and are wired on the RW path).
- Tenant control plane: static token=tenant CLI pairs or OIDC/JWKS resolution (crates/ravel-tenant-resolve/src/lib.rs:1-50; config.rs:61-66). Static tokens do not scale to hundreds of tenants; OIDC does. Alert rules, sinks, and several caps are restart-bound configuration; typed attribute columns have a durable override cache with CLI management (ADR-0079), which is the better pattern the rest should follow.
- Migration in: Prometheus can remote_write into Ravel today (1.0 and 2.0); bulk log import exists (ADR-0089); there is no TSDB-block or OTLP-file backfill importer for metrics history. Migration out: SQL and Flight SQL export every signal; formats are specified in-repo (segment-format.md, log-segment-format.md, span-segment-format.md) and Apache-2.0, so lock-in is low by open-format standards, but no tool emits Prometheus TSDB blocks or OTLP back out.
- No downsampled tier: every wide-range query reads raw hours; admitted openly (README:63-64, issue #118). Recent-hours path and read cache (ADR-0073/0046) mitigate the hot end only. This caps the "years of metrics, fast dashboards" story today.

### Who should and should not adopt (PM summary)

- Adopt today: platform teams wanting a Prometheus-API-compatible, S3-only, strictly-tenanted metrics store for float metrics and classic histograms, with Grafana dashboards, exemplars, and webhook/Alertmanager alerting; teams that value the commit-token read-your-write contract and per-tenant cost accounting; security/audit teams that want SQL over logs/spans/alerts as evidence.
- Should not adopt today: shops standardized on native histograms (silent wrong results outside blessed patterns); Loki/Tempo users expecting log-panel and trace-view UX; delta-temporality OTLP senders without a collector; anyone needing recording rules, dynamic alert management, or downsampled multi-year dashboards.
- Strongest differentiated value: the durability contract is user-visible, not an implementation property. The ack-means-S3-durable semantics plus commit-token read-your-writes is a real, testable product promise (README demo; docs/consistency-model.md), and disposable compute genuinely removes the stateful-tier operations that dominate Mimir/Loki/Tempo operation.
- Label beta/experimental: native histogram querying (beyond the blessed forms), traces querying, alerting configuration management, OTAP ingest (already feature-gated), distributed federation (already off by default).
- Maturity split: architecture maturity high; metrics feature completeness high (floats/classic) but uneven (native histograms); logs/traces feature completeness medium-low as products; operational maturity medium (operator, guides, cost model exist; restart-bound config); ecosystem maturity low outside the Prometheus datasource (no Loki/Tempo/Jaeger surfaces, no rule APIs).

## Failure scenarios

1. Native histogram dashboard silently zero or empty. A team adopts OTel exponential histograms; Ravel admits them by default. Their Grafana heatmap panel uses range `sum(rate(latency[$__rate_interval]))`: every step evaluates to a histogram element and the grid stitcher emits 0.0, so the panel renders a flat zero heatmap while p99 panels (histogram_quantile) on the same data work. An SLO alert rule on `sum(rate(latency[5m])) > X`-style expressions never fires. No warning, no error anywhere. (Probe outputs, functions/mod.rs:567-583, eval.rs:704-711.)
2. Same query, different answers by endpooint: `rate(h[5m])` returns correct histograms on /api/v1/query and an empty matrix on /api/v1/query_range (functions/mod.rs:415-444). A user debugging the empty graph in Explore (instant mode) concludes the data exists and blames Grafana.
3. Arithmetic on native histograms fabricates numbers: `h * 60` (per-minute scaling) or `h + on(...) f` produce 0-based floats that look like real series and feed downstream math (probe lines "inst h * 2", "inst h + f"). Threshold alerts built on such expressions evaluate real conditions on fabricated zeros.
4. Cross-scope metric collision: two instrumentation libraries in one service emit `requests_total` with identical attributes under different scopes; Ravel assigns one series identity (scope ignored, normalize.rs:38-40), and same-timestamp samples deduplicate to one winner. Counters undercount silently; Prometheus's own OTLP path would have kept them distinct via otel_scope_name.
5. Structured-body logs refused: a fleet emitting kvlist bodies gets per-record UnsupportedBodyKind rejections (logs_normalize.rs:347-349); if nobody watches partial-success responses, those logs are simply absent at investigation time. Visible in counters, invisible in the moment.
6. Alert rule change during an incident requires editing a JSON file and restarting ravel-server processes (config.rs:242-248); during the restart window leases lapse and hand off, with possible duplicate or delayed notifications (documented at-least-once).

## Tests or commands run

All commands ran on this host; exit codes are the real shell exit codes. Repository was never modified (probe crate lives in /tmp).

1. Read-only exploration: `ls`, `wc -l`, `sed -n`, and about twenty `grep -n`/`grep -rn` invocations over crates/, services/, docs/, deploy/ (all exit 0). Decisive greps: `grep -rn "query_histograms"` (found the MergedSource implementation at engine.rs:2749); `grep -rn "target_info|otel_scope"` over crates (no output, exit 1: absent); `grep -rn -i "logql|loki|tempo|jaeger|traceql"` over code (no relevant hits); `grep -n "start_time|created"` over normalize.rs (no output: OTLP start time unused); function-name extraction grep over functions/*.rs (69 names, listed above).
2. `df -h /tmp` — exit 0, `126G` free (headroom check before building).
3. Probe build+run 1: `cd /tmp/agentg-histprobe && CARGO_TARGET_DIR=... cargo run --jobs 4 --quiet > out.txt 2> err.txt` — exit 0. Decisive lines from out.txt: `inst h * 2: VECTOR n=1 [ (float value=0 ...) ]`; `range rate(h[5m]): MATRIX n_series=0`; `range sum(rate(h[5m])): MATRIX n_series=1 (labels=LabelSet([]) values=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])`; `inst sum_over_time(h[5m]): VECTOR n=0`; `range subquery rate(h[5m:1m]): ERROR unsupported PromQL construct: subquery over native histograms`; all with `warnings=[] infos=[]`.
4. Probe build+run 2 (rewritten main.rs): exit 0. Decisive lines from out2.txt: `range histogram_quantile(0.9, sum(rate(h[5m]))): MATRIX n_series=1 ... first=Some(6.773962498900218)` (canonical pattern works); `inst h + f (mixed): VECTOR n=1 [ (float value=20) ]`; `inst clamp_max(h, 5): VECTOR n=2 [ (float value=0) (float value=0) ]`; `inst last_over_time(h[5m]) (Prom returns hist): VECTOR n=0`; `inst histogram_stddev(h): ERROR unsupported PromQL construct: function call: histogram_stddev`.

I did not rerun the workspace test suites, the difftest, or the simulation (declared green by the review harness); no broad builds were run.

## Unknowns

- Prometheus 3.x le/quantile label normalization: Ravel formats classic-histogram `le` values with Rust float Display via `format_float` (crates/ravel-otlp/src/promcompat.rs:15-25, golden-vector tested against Go strconv), producing `le="1"`. I did not verify whether Prometheus 3's OTLP ingestion normalizes to `le="1.0"` (OpenMetrics canonical form); if it does, dashboards with hard-coded le matchers behave differently across the two backends. UNKNOWN, cheap to check against a live Prometheus.
- Exact Prometheus 3.13 behavior for each over_time function over native histograms (which produce values vs which skip with info). My claim that Ravel's silent-empty diverges is solid for count_over_time/last_over_time; the precise expected set for the others is from memory of Prometheus semantics, not re-verified. UNKNOWN in detail, direction of the gap not in doubt.
- Whether the server-level query path (QueryEngine + MergedSource over real RSEG objects, MinIO) reproduces every probe behavior byte-for-byte. The evaluator is the shared component and the difftest corpus header documents the end-to-end histogram flow, so I judge the risk that the server path differs as low; I did not spin up the full server (build cost). Labeled STRONGLY SUPPORTED rather than VERIFIED where relevant.
- Grafana's current native-histogram heatmap query shapes (they have changed across Grafana versions); my mainstream-path judgment for range `sum(rate(h[...]))` rests on it being the documented Grafana/Prometheus pattern, not on testing a live Grafana.

## Severity-ranked findings

### P1: Native histogram data silently yields wrong or missing results across most of the PromQL surface

- Scenario: any instant or range query over native-histogram series outside the blessed forms: binary operators (arith, comparisons, vector matching) fabricate 0-based values or phantom series; range queries with histogram-valued results return empty (rate/increase/delta, bare selector) or all-zero series (aggregates, arithmetic, anything through the generic grid); over_time family and resets return silently empty; topk/quantile emit 0-valued members. No annotation, warning, or typed error on any of these paths.
- Evidence: probe runs 1 and 2 (exact lines above); crates/ravel-promql/src/binop.rs:157-188, 334-341, 384-396; functions/mod.rs:415-444, 558-601; eval.rs:99-105, 146-153, 704-711, 1009-1072; json.rs:278-281; contrast with the guarded subquery path eval.rs:938-961 and the difftest exclusions at corpus/histogram_native.txt:13-22.
- Blast radius: every tenant sending OTLP exponential histograms (admitted by default) or RW2 native histograms; dashboards, ad-hoc queries, and PromQL alert rules all consume the fabricated values. Float-only tenants are entirely unaffected.
- Probability and preconditions: certain, deterministic, whenever such a query shape is issued over native-histogram data; requires sender-side native histogram opt-in (OTel exponential aggregation or Prometheus native-histograms feature), which is a growing but not yet majority configuration. That precondition is the only reason this is P1 rather than P0 under the rubric; for a native-histogram shop it is P0 in effect.
- Workaround: restrict native-histogram queries to histogram_quantile/_fraction/_count/_sum/_avg (instant and range) and instant-only rate/sum/avg; or have senders emit classic histograms.
- Recommended fix: short term, extend the existing subquery guard pattern (typed `Error::Unsupported`, 422) to (a) the grid stitcher when any step vector contains a histogram element, (b) all binop arms receiving a histogram element, (c) the over_time/reduction paths, and emit Prometheus' info/warn annotations where Prometheus drops. Long term, implement Prometheus' histogram semantics for binops and over_time, add matrix `histograms` rendering, and enable the histogram-aware difftest comparator the corpus header already plans.
- Proof test: unit tests asserting typed errors (mirroring `subquery_over_native_histogram_is_unsupported_in_a_range_query`, eval.rs:2444) for each guarded path; then difftest corpus entries for `h*2`, `h+h`, range `sum(rate(h[5m]))`, `count_over_time(h[5m])` once the comparator lands, which would have caught every behavior in this finding.

### P2: Traces are storage plus SQL, not a tracing backend

- Evidence: spans table and pruning real (session.rs:106-124, guides/traces.md); no by-id/search/service-discovery API, no Tempo/Jaeger/TraceQL (grep absence); correlation guide requires an external tracing datasource (correlation.md:141-155).
- Impact: "unified observability" and the exemplar-to-trace click both terminate outside the product; adopters must run Tempo/Jaeger anyway or accept SQL-only trace work. Fix: a minimal Tempo-compatible /api/traces/{id} plus search endpoint over the existing fetcher would close most of the gap. Proof: Grafana trace panel renders a Ravel-stored trace end to end.

### P2: Alerting configuration is static and API-less; no recording rules

- Evidence: config.rs:242-248 (JSON file at startup), ADR-0043 deferred list, absent /api/v1/rules//alerts. Impact: rule changes need restarts; Grafana cannot display rules; long-range query cost cannot be amortized into recorded series (compounds the no-downsampling limit). Fix: durable rule store (the typed-attr override-cache pattern, ADR-0079) plus the two read endpoints. Proof: rule CRUD without restart; Grafana alert list renders.

### P2: OTLP metrics ignore instrumentation scope in series identity; no otel_scope_* labels, no target_info

- Evidence: normalize.rs:38-40; grep absence; dedup rule source.rs:22-31. Impact: cross-scope same-name series merge silently, undercounting; divergence from Prometheus 3 OTLP behavior. Preconditions: duplicate instrumentation across scopes, realistic in large services. Fix: add scope name/version to identity behind the documented convention labels. Proof: two-scope ingest test asserting two series.

### P2: Structured (kvlist/array) OTLP log bodies are rejected, not stored

- Evidence: logs_normalize.rs:332-350. Typed and visible, but a fidelity gap versus every mainstream OTLP log pipeline. Fix: canonical JSON stringification with a marker attr, or a typed body column. Proof: ingest test round-tripping a kvlist body.

### P2: Delta temporality rejected; no downsampled tier; log/trace UX SQL-only

- Grouped ecosystem-expectation items, all documented honestly (normalize.rs:16-18; README:63-64; guides). Each is a deliberate scope cut with a visible failure mode; listed so adopters price in the collector processor, dashboard ranges, and query tooling they must bring.

### P3 items

- Stale internal rationale comments contradict shipped behavior (histogram.rs:13-24 "cannot reach the evaluator"; functions/mod.rs:415-418 "no read path yet"): per repo rules a stale doc is a bug; both also mask the P1 above. 
- Possible le-format divergence from Prometheus 3 normalization (promcompat.rs:15-25), UNKNOWN pending a live check.
- Native-histogram exemplars dropped at OTLP ingest (normalize.rs:24-25).
- RW1 senders cannot observe active-series-cap partial drops in-band (remote_write.rs:125-131).
- Missing minor Prometheus API endpoints (POST /api/v1/labels, rules/alerts, parse_query/format_query); missing annotations on mixed-group and float-function histogram drops even where the value behavior matches Prometheus.

## Confidence

- Native histogram silent-wrong/missing behaviors: high. Directly executed against the crate's own evaluator with its own public test source; code paths read and cited; no contrary guard found.
- Native histograms flow end to end in production storage: high-medium. Code plus the corpus header's recorded end-to-end run; I did not run the full server stack.
- Blessed-pattern PromQL correctness (floats, classic histograms, canonical native patterns): high, resting on the differential gate's design and the harness-declared green run; I independently reproduced the canonical pattern's values in-probe.
- Traces/logs/alerting completeness assessments: high for what exists and what is absent (grep plus guides plus code); medium on how much the absences matter to a given adopter.
- OTLP scope-collision loss mechanics: medium. Identity code and dedup contract are clear; I did not construct the two-scope collision end to end.
- PM judgments (adopter fit, beta labels): medium by nature; grounded in the verified capability matrix above.
