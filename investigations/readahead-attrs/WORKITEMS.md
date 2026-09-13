# Work items (draft, ranked; no issues or PRs created)

## 1. Per-key projection of `attrs['k']` without the merged map

- Scope: planner and scan only. In `crates/ravel-sql/src/map_field_planner.rs`
  (`get_field(attrs, 'k')` lowering, `:64-80`) and
  `crates/ravel-sql/src/logs_provider.rs` (`scan`, `:347-399`), collect the
  literal keys referenced through `attrs[...]` in projection and residual
  predicates; when the projection needs no whole `attrs` column, replace
  index 8 with synthetic per-key columns. In `crates/ravel-sql/src/logs_scan.rs`
  `resolve_columns` (`:512-565`) select only those keys' FIELD_DIR columns
  plus `attrs_raw` (as declared columns do, `:524-531`), keep
  `columnar_static_eligible` true, and in `build_columnar_batches`
  (`:4508-4556`) render each key as `Utf8` using the map form's rules.
- Semantics to preserve (pinned by `tests/attrs_map_vs_declared.rs`, extend
  it): record over resource/scope; within-record last-wins with `attrs_raw`
  after columnar occurrences and higher type byte winning
  (`docs/log-segment-format.md:586-591`, `reader.rs:1268-1275`); rendering of
  non-Str values as text (`record.rs:111-126`); nested resource values
  omitted (`rlog_attrs.rs:118-137`); NULL for absent; literal dotted keys;
  erased-row filtering on the merged view (`rlog_attrs.rs:60-90`); the
  residual `FilterExec` still evaluated above the scan.
- Dependencies: none on #1284. Interacts with ADR-0087's stated scope
  exclusion (`docs/adrs/0087:76-81`), which needs an amendment note.
- Acceptance: on `corpus/attrs-map.corpus.json` over the run-3 dataset,
  `page_stored_bytes_decoded` for `attr_eq_count`, `attr_group`,
  `attr_project_limit` within 1.5x of the declared form's 3,531 (band, not
  equality: `attrs_raw` pages stay selected); `rowpath_batches == 0`;
  results identical as multisets to the pre-change binary on the test
  fixture including the divergence row; `SELECT attrs FROM logs` and
  `SELECT *` unchanged and still row path.

## 2. Planning-path carry bound and the scan re-fetch

- Scope: `crates/ravel-sql/src/logs_scan.rs:2630-2700` (`compute_plan_counts`
  carry budget) and the plan/scan phase accounting. Today the first
  `plan_concurrency` segments to complete keep their whole-object bytes and
  the rest are re-fetched by the scan (measured 32 of 40 at 8 partitions).
- Options to measure, not decide here: (a) carry all objects up to a byte
  budget taken from ravel-memory once #1284 lands; (b) skip the plan-phase
  whole read for objects the scan will read whole anyway under `u64::MAX`
  (prune from footer/skip index only, and let the scan open do the read);
  (c) rely on the RAM cache and measure the warm case
  (`--cache-bytes 268435456`).
- Dependencies: #1284 for (a); #693 is the tracking issue.
- Acceptance: plan-phase bytes plus scan-phase bytes <= 1.15 x stored bytes
  on `selective_limit_planpath` at 8 partitions with no cache; no change in
  results.

## 3. Narrow the `attrs_raw` fallback to selected keys

- Scope: `crates/ravel-sql/src/logs_scan.rs:3578` (`has_attrs_raw_page`)
  and `crates/ravel-logseg/src/block.rs:924-933`. Decide the fallback on
  whether the overflow page can contain a SELECTED key, which needs the
  overflow page's key set or a per-block key bloom; without one the
  decision requires decoding the page, which is itself most of the cost.
- Dependencies: item 1 (defines the selected key set for the map form);
  possibly a format addition (a per-block overflow key list), which is an
  ADR and out of scope for a scan-only change.
- Acceptance: the test's overflow layout stays correct; a block whose
  overflow holds only unselected keys stays columnar (`rowpath_batches == 0`).

## 4. S3 partition-count sweep before any read-ahead work

- Scope: measurement only, on the reference host: `sql_latency_bench
  --tenant <id> --store s3` with the instrumented binary, sweeping
  `--fetch-concurrency` (or `--sql-partition-count` at fixed
  `--store-get-concurrency` once the bench exposes them separately) over
  8, 16, 32, 64; record `open_elapsed_max`, `decode_build_elapsed_max`,
  `stream_elapsed_max`, `cpu_ms`, `peak_rss_kb`.
- Decision rule (pre-register on the issue): if `decode_build_elapsed_max`
  is within 15 percent of `open_elapsed_max` at the largest partition count
  that fits memory, read-ahead has a role; otherwise close the read-ahead
  idea as configuration-only.
- Dependencies: BLOCKERS.md S3 lane.

## 5. Fetch-byte reservation (PR #1284) and the assembly pool live gauge

- Scope: land #1284; add `live_bytes` / `peak_live_bytes` to
  `AssemblyBufferStats` (`log_fetcher.rs:3079`) so the live set is observable
  under `byte-minimal`, which the stock policy never exercises.
- Acceptance: a test that checks out N buffers and asserts the gauge equals
  the sum of their object sizes, and the reservation refusing at the budget.

## 6. LIMIT as a fetch-stop hint on the local logs scan (#362)

- Scope: `logs_provider.rs:390` drops `_limit`; thread it into `LogsScanExec`
  so `NextSegment` stops opening once the partition has emitted `limit`
  rows for an unordered, predicate-free plan. Only sound without `ORDER BY`
  and without a residual filter above the scan.
- Acceptance: `SELECT ts FROM logs LIMIT 10` opens at most `partitions`
  segments (GET count band `<= partitions + resolve GETs`).

## 7. Documentation defect

- `crates/ravel-bench/src/bin/sql_latency_bench.rs` (`print_open_shapes`
  doc) names a `backend_bills_requests` field that `SqlLatencyReport` does
  not have; it lives on `report.rs:102`. One-line fix.
