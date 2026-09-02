# SQL conformance

<!--
  GENERATED FILE -- do not edit by hand.

  This document is rendered from the conformance registry in
  crates/ravel-sql/src/conformance.rs, annotated with the live verdict the
  conformance suite records for each construct. Regenerate with:

      REGEN_SQL_CONFORMANCE=1 cargo test -p ravel-sql --test conformance

  Editing the table by hand will be overwritten on the next run, and the
  suite fails if the committed file and the freshly rendered one disagree.
-->

This is the SQL half of the conformance scoring decided in
[ADR-0035](adrs/0035-conformance-scoring.md). It publishes what Ravel's SQL
surface supports, what it deliberately rejects, and what (if anything) is
neither -- scored over the surface Ravel actually claims, not over all of
DataFusion SQL (ADR-0035 Rejected Alternative A).

Ravel's SQL surface is a deliberately narrow allowlist, not general-purpose
SQL: a six-aggregate set, read-only `SELECT` only, and a single signal table
per query ([ADR-0033](adrs/0033-sql-query-over-logs.md),
[ADR-0022](adrs/0022-floating-aggregate-exactness.md)). Scoring against all of
DataFusion SQL would be meaningless (a tiny fixed percentage that never moves);
scoring against only the declared surface would read ~100% by construction. The
three-state scheme below is the resolution: enumerate the claimed surface, score
over the part with a deliberate verified position, and keep any unresolved
construct visible as an actionable miss.

## The `samples` table

The `samples` table exposes exactly four columns -- `ts`, `value`, `series_id`,
and `labels` -- where `value` is a single `Float64`
(`crates/ravel-sql/src/schema.rs`, `public_fields`). It has no column that can
represent a native histogram. Native-histogram samples are excluded from the
`samples` table entirely: a histogram sample carries no scalar float `value`,
so it is never materialized as a row and no query can observe it. `SELECT
count(*) FROM samples`, and every other count or aggregation over the table,
therefore undercount on tenants that ingest histograms -- the histogram samples
are silently absent, not present with a zero or null `value`. This is a
property of the table shape, not of any construct in the conformance table
below, so it holds for every row that reads `samples`.

## The three states

1. **Supported and covered** -- implemented, with a passing test proving it.
   These constructs are exercised by the two-layer differential gate
   (`crates/ravel-sql/tests/pipeline.rs` and
   `crates/ravel-sql/tests/differential.rs`) against an independent reference,
   and re-confirmed to execute by the conformance suite
   (`crates/ravel-sql/tests/conformance.rs`).
2. **Intentionally rejected** -- refused with a typed error, never a panic and
   never silently-wrong data. This is the allowlist working as designed: the
   aggregates outside the admitted six, every write/DDL statement, and a query
   spanning both signal tables. The conformance suite asserts each returns its
   declared typed error.
3. **Unclassified / broken** -- implemented but untested, or claimed-supported
   but actually wrong. No construct is declared into this state; a row lands
   here only when its live behavior contradicts its declaration, which fails
   the suite. A regression therefore shows up as a diff in this table in the
   same change that caused it.

The score is states 1 and 2 combined, over the total enumerated surface. A row
in state 3 lowers it; an out-of-scope construct Ravel rejects cleanly is state 2
and stays conformant.

## Score

- Supported and covered: 43
- Intentionally rejected: 68
- Unclassified / broken: 0
- **Conformance: 111 / 111 = 100.0%**

## Conformance table

| Category | Construct | Example | State | Evidence | Rationale |
| --- | --- | --- | --- | --- | --- |
| Aggregate | `approx_distinct` | `SELECT approx_distinct(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `approx_median` | `SELECT approx_median(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `approx_percentile_cont` | `SELECT approx_percentile_cont(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `approx_percentile_cont_with_weight` | `SELECT approx_percentile_cont_with_weight(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `array_agg` | `SELECT array_agg(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `avg` | `SELECT avg(value) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted aggregate (ADR-0022); exact against the differential gate |
| Aggregate | `bit_and` | `SELECT bit_and(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `bit_or` | `SELECT bit_or(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `bit_xor` | `SELECT bit_xor(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `bool_and` | `SELECT bool_and(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `bool_or` | `SELECT bool_or(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `corr` | `SELECT corr(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `count` | `SELECT count(value) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted aggregate (ADR-0022); exact against the differential gate |
| Aggregate | `covar` | `SELECT covar(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `covar_pop` | `SELECT covar_pop(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `covar_samp` | `SELECT covar_samp(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `first_value` | `SELECT first_value(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `grouping` | `SELECT grouping(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `last_value` | `SELECT last_value(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `max` | `SELECT max(value) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted aggregate (ADR-0022); exact against the differential gate |
| Aggregate | `mean` | `SELECT mean(value) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted aggregate (ADR-0022); exact against the differential gate |
| Aggregate | `median` | `SELECT median(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `min` | `SELECT min(value) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted aggregate (ADR-0022); exact against the differential gate |
| Aggregate | `nth_value` | `SELECT nth_value(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `percentile_cont` | `SELECT percentile_cont(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `quantile_cont` | `SELECT quantile_cont(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `regr_avgx` | `SELECT regr_avgx(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `regr_avgy` | `SELECT regr_avgy(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `regr_count` | `SELECT regr_count(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `regr_intercept` | `SELECT regr_intercept(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `regr_r2` | `SELECT regr_r2(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `regr_slope` | `SELECT regr_slope(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `regr_sxx` | `SELECT regr_sxx(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `regr_sxy` | `SELECT regr_sxy(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `regr_syy` | `SELECT regr_syy(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `stddev` | `SELECT stddev(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `stddev_pop` | `SELECT stddev_pop(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `stddev_samp` | `SELECT stddev_samp(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `string_agg` | `SELECT string_agg(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `sum` | `SELECT sum(value) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted aggregate (ADR-0022); exact against the differential gate |
| Aggregate | `var` | `SELECT var(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `var_pop` | `SELECT var_pop(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `var_population` | `SELECT var_population(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `var_samp` | `SELECT var_samp(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Aggregate | `var_sample` | `SELECT var_sample(value) FROM samples` | Intentionally rejected | `ValidationError::ExcludedAggregate` | outside the six-aggregate allowlist (ADR-0022 decision 2) |
| Clause / operator | `CASE` | `SELECT CASE WHEN value > 0 THEN 1 ELSE 0 END FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `DATE_TRUNC` | `SELECT date_trunc('hour', ts) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `Filter (WHERE)` | `SELECT ts, value FROM samples WHERE value > 0 ORDER BY series_id, ts` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | covered by the two-layer differential gate (tests/differential.rs) |
| Clause / operator | `GROUP BY` | `SELECT series_id, count(value) FROM samples GROUP BY series_id ORDER BY series_id` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | covered by the two-layer differential gate (tests/differential.rs) |
| Clause / operator | `GROUP BY ordinal` | `SELECT series_id, count(value) FROM samples GROUP BY 1 ORDER BY series_id` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `HAVING` | `SELECT series_id, count(value) FROM samples GROUP BY series_id HAVING count(value) > 2` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `IN list` | `SELECT value FROM samples WHERE value IN (1, 2)` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `LIKE` | `SELECT count(*) FROM logs WHERE body LIKE '%record 1%'` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | substring pattern match via the Ravel like UDF |
| Clause / operator | `LIMIT` | `SELECT ts, value FROM samples ORDER BY series_id, ts LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | covered by the two-layer differential gate (tests/differential.rs) |
| Clause / operator | `OFFSET` | `SELECT value FROM samples ORDER BY value OFFSET 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `ORDER BY` | `SELECT ts, value FROM samples ORDER BY ts` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | covered by the two-layer differential gate (tests/differential.rs) |
| Clause / operator | `Projection` | `SELECT ts, value FROM samples ORDER BY series_id, ts` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | covered by the two-layer differential gate (tests/differential.rs) |
| Clause / operator | `REGEXP_REPLACE backreference` | `SELECT regexp_replace('ab', '(a)(b)', '\2\1') FROM samples LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `count(DISTINCT)` | `SELECT count(DISTINCT series_id) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `date_part(minute)` | `SELECT date_part('minute', ts) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `declared i64 typed aggregate` | `SELECT sum(dur) FROM logs` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | typed predicate/aggregate over a typed attribute column (ADR-0090) |
| Clause / operator | `declared i64 typed comparison` | `SELECT ts FROM logs WHERE dur >= 20` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | typed predicate/aggregate over a typed attribute column (ADR-0090) |
| Scalar function | `abs` | `SELECT abs(-3.5) FROM samples LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | math family representative: abs(-3.5) = 3.5 |
| Scalar function | `current_date` | `SELECT current_date FROM samples` | Intentionally rejected | `ValidationError::ExcludedScalar` | nondeterministic or environment-reading; unattestable by the differential oracle (ADR-0097 decision 4) |
| Scalar function | `current_time` | `SELECT current_time FROM samples` | Intentionally rejected | `ValidationError::ExcludedScalar` | nondeterministic or environment-reading; unattestable by the differential oracle (ADR-0097 decision 4) |
| Scalar function | `current_timestamp` | `SELECT current_timestamp FROM samples` | Intentionally rejected | `ValidationError::ExcludedScalar` | nondeterministic or environment-reading; unattestable by the differential oracle (ADR-0097 decision 4) |
| Scalar function | `encode` | `SELECT encode('a', 'hex') FROM samples LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | encoding family representative: hex of the byte 0x61 = '61' |
| Scalar function | `has_word` | `SELECT count(*) FROM logs WHERE has_word(body, '1')` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | Ravel per-table scalar UDF, individually attested (ADR-0097 decision 8) |
| Scalar function | `label` | `SELECT label(labels, '__name__') FROM samples WHERE value = 3.0` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | Ravel per-table scalar UDF, individually attested (ADR-0097 decision 8) |
| Scalar function | `label_match` | `SELECT count(*) FROM samples WHERE label_match(labels, '__name__', 'b')` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | Ravel per-table scalar UDF, individually attested (ADR-0097 decision 8) |
| Scalar function | `length` | `SELECT length('résumé') FROM samples LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | character count, not byte count: 'résumé' is 6 characters but 7 bytes (the 'é's are two-byte UTF-8) |
| Scalar function | `now` | `SELECT now() FROM samples` | Intentionally rejected | `ValidationError::ExcludedScalar` | nondeterministic or environment-reading; unattestable by the differential oracle (ADR-0097 decision 4) |
| Scalar function | `rand` | `SELECT rand() FROM samples` | Intentionally rejected | `ValidationError::ExcludedScalar` | nondeterministic or environment-reading; unattestable by the differential oracle (ADR-0097 decision 4) |
| Scalar function | `random` | `SELECT random() FROM samples` | Intentionally rejected | `ValidationError::ExcludedScalar` | nondeterministic or environment-reading; unattestable by the differential oracle (ADR-0097 decision 4) |
| Scalar function | `regexp_replace` | `SELECT regexp_replace('foo123bar', '[0-9]+', 'X') FROM samples LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | regex family representative: first-match replace of the digit run = 'fooXbar' |
| Scalar function | `reverse` | `SELECT reverse('résumé') FROM samples LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | unicode family representative: reverse('résumé') = 'émusér' (character-wise, not byte-wise) |
| Scalar function | `to_char` | `SELECT to_char(make_date(2024, 7, 15), '%Y-%m-%d') FROM samples LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | datetime family representative: to_char over a tz-free Date32 = '2024-07-15' |
| Scalar function | `today` | `SELECT today() FROM samples` | Intentionally rejected | `ValidationError::ExcludedScalar` | nondeterministic or environment-reading; unattestable by the differential oracle (ADR-0097 decision 4) |
| Scalar function | `upper` | `SELECT upper('ab') FROM samples LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | string family representative: upper('ab') = 'AB' |
| Scalar function | `uuid` | `SELECT uuid() FROM samples` | Intentionally rejected | `ValidationError::ExcludedScalar` | nondeterministic or environment-reading; unattestable by the differential oracle (ADR-0097 decision 4) |
| Scalar function | `version` | `SELECT version() FROM samples` | Intentionally rejected | `ValidationError::ExcludedScalar` | nondeterministic or environment-reading; unattestable by the differential oracle (ADR-0097 decision 4) |
| Window function | `cume_dist` | `SELECT max(c) FROM (SELECT cume_dist() OVER (ORDER BY value) AS c FROM samples)` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted native window function (ADR-0097 decision 6) |
| Window function | `dense_rank` | `SELECT max(d) FROM (SELECT dense_rank() OVER (ORDER BY value) AS d FROM samples)` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted native window function (ADR-0097 decision 6) |
| Window function | `first_value` | `SELECT first_value(value) OVER (ORDER BY ts) FROM samples` | Intentionally rejected | `ValidationError::ExcludedWindow` | excluded window function; bare spelling is a separate ExcludedAggregate row (ADR-0097 decision 6) |
| Window function | `lag` | `SELECT count(p) FROM (SELECT lag(value) OVER (ORDER BY value) AS p FROM samples)` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted native window function (ADR-0097 decision 6) |
| Window function | `last_value` | `SELECT last_value(value) OVER (ORDER BY ts) FROM samples` | Intentionally rejected | `ValidationError::ExcludedWindow` | excluded window function; bare spelling is a separate ExcludedAggregate row (ADR-0097 decision 6) |
| Window function | `lead` | `SELECT count(nx) FROM (SELECT lead(value) OVER (ORDER BY value) AS nx FROM samples)` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted native window function (ADR-0097 decision 6) |
| Window function | `nth_value` | `SELECT nth_value(value, 2) OVER (ORDER BY ts) FROM samples` | Intentionally rejected | `ValidationError::ExcludedWindow` | excluded window function; bare spelling is a separate ExcludedAggregate row (ADR-0097 decision 6) |
| Window function | `ntile` | `SELECT max(n) FROM (SELECT ntile(2) OVER (ORDER BY value) AS n FROM samples)` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted native window function (ADR-0097 decision 6) |
| Window function | `percent_rank` | `SELECT max(pr) FROM (SELECT percent_rank() OVER (ORDER BY value) AS pr FROM samples)` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted native window function (ADR-0097 decision 6) |
| Window function | `rank` | `SELECT max(r) FROM (SELECT rank() OVER (ORDER BY value) AS r FROM samples)` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted native window function (ADR-0097 decision 6) |
| Window function | `row_number` | `SELECT max(rn) FROM (SELECT row_number() OVER (ORDER BY value) AS rn FROM samples)` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | admitted native window function (ADR-0097 decision 6) |
| Window frame | `avg (moving frame)` | `SELECT avg(value) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM samples` | Intentionally rejected | `SqlError::Execution` | moving-frame avg has no retract_batch, so DataFusion refuses it (ADR-0097 decision 5) |
| Write / DDL statement | `ALTER TABLE` | `ALTER TABLE samples ADD COLUMN x INT` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `COPY` | `COPY (SELECT 1) TO 's3://evil/out.parquet'` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `CREATE EXTERNAL TABLE` | `CREATE EXTERNAL TABLE t (a INT) STORED AS PARQUET LOCATION '/tmp/x'` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `CREATE SCHEMA` | `CREATE SCHEMA s` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `CREATE TABLE` | `CREATE TABLE t (a INT)` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `CREATE VIEW` | `CREATE VIEW v AS SELECT 1` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `DELETE` | `DELETE FROM samples` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `DROP` | `DROP TABLE samples` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `EXPLAIN` | `EXPLAIN SELECT 1` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `INSERT` | `INSERT INTO samples VALUES (1, 2.0)` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `PREPARE` | `PREPARE p AS SELECT 1` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `SET` | `SET datafusion.execution.batch_size = 1` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `Transaction control` | `BEGIN TRANSACTION` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `UPDATE` | `UPDATE samples SET value = 1` | Intentionally rejected | `ValidationError::NotReadOnly` | read-only endpoint; refused before planning (crate::validate) |
| Write / DDL statement | `Write nested in a query body` | `WITH c AS (SELECT 1) INSERT INTO samples VALUES (1, 2.0)` | Intentionally rejected | `ValidationError::WriteInQuery` | read-only endpoint; refused before planning (crate::validate) |
| Table dispatch | `both tables (cross-signal)` | `SELECT * FROM samples JOIN logs ON samples.ts = logs.ts` | Intentionally rejected | `SqlError::CrossSignalQuery` | one signal per query in v1 (ADR-0033 decision C) |
| Table dispatch | `logs -> Signal::Logs` | `SELECT ts, body FROM logs` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | logs dispatch via referenced_base_tables (ADR-0033) |
| Table dispatch | `samples -> Signal::Metrics` | `SELECT ts, value FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | metrics dispatch via referenced_base_tables (ADR-0033) |
