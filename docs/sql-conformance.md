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

- Supported and covered: 24
- Intentionally rejected: 55
- Unclassified / broken: 0
- **Conformance: 79 / 79 = 100.0%**

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
| Clause / operator | `HAVING` | `SELECT series_id, count(value) FROM samples GROUP BY series_id HAVING count(value) >= 2` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `IN list` | `SELECT value FROM samples WHERE value IN (1, 2)` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `LIMIT` | `SELECT ts, value FROM samples ORDER BY series_id, ts LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | covered by the two-layer differential gate (tests/differential.rs) |
| Clause / operator | `OFFSET` | `SELECT value FROM samples ORDER BY value OFFSET 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `ORDER BY` | `SELECT ts, value FROM samples ORDER BY ts` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | covered by the two-layer differential gate (tests/differential.rs) |
| Clause / operator | `Projection` | `SELECT ts, value FROM samples ORDER BY series_id, ts` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | covered by the two-layer differential gate (tests/differential.rs) |
| Clause / operator | `REGEXP_REPLACE backreference` | `SELECT regexp_replace('ab', '(a)(b)', '\2\1') FROM samples LIMIT 1` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `count(DISTINCT)` | `SELECT count(DISTINCT value) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `date_part(minute)` | `SELECT date_part('minute', ts) FROM samples` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | analytical clause/operator over typed columns (ADR-0090 decision 8) |
| Clause / operator | `declared i64 typed aggregate` | `SELECT sum(dur) FROM logs` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | typed predicate/aggregate over a declared column (ADR-0090) |
| Clause / operator | `declared i64 typed comparison` | `SELECT ts FROM logs WHERE dur >= 20` | Supported and covered | `tests/conformance.rs::supported_constructs_execute` | typed predicate/aggregate over a declared column (ADR-0090) |
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
