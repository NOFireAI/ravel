# ES|QL conformance

<!--
  GENERATED FILE -- do not edit by hand.
  Generated from crates/ravel-esql/manifest/esql-9.5.toml by
  crates/ravel-esql/tests/conformance.rs. Regenerate with:
      REGEN_ESQL_CONFORMANCE=1 cargo test -p ravel-esql --test conformance
-->

How much of the released ES|QL language Ravel's ES|QL front end accepts and runs (ADR-2887 D2). Target release line: 9.5.

The manifest this report is generated from was written from the published ES|QL language reference for the 9.5 line, without consulting the Elasticsearch source tree.
It has not been reconciled with the released 9.5 binary (ADR-2887 D3), so an entry can be missing, misnamed or carry the wrong preview flag.

## Figures

Each figure counts released entries only; development-only commands are listed below and are in no denominator.

| Figure | Score | What counts |
|---|---|---|
| Syntax coverage | 260/260 | released entries whose example the parser accepts |
| Execution compatibility | 50/260 | released entries whose example prepares and runs; a valid entry Ravel refuses is a gap |
| Transport compatibility | 0/13 | options of the Elasticsearch query endpoint that Ravel serves |

Negative statements: 14, each refused by the parser. They are counted on this line only and never raise a figure above.

## By kind

| Kind | Entries | Parsed | Executed |
|---|---|---|---|
| source_command | 5 | 5/5 | 2/5 |
| processing_command | 20 | 20/20 | 8/20 |
| directive | 1 | 1/1 | 0/1 |
| syntax | 19 | 19/19 | 8/19 |
| function | 152 | 152/152 | 7/152 |
| alias | 9 | 9/9 | 0/9 |
| operator | 26 | 26/26 | 19/26 |
| type | 22 | 22/22 | 6/22 |
| parameter | 6 | 6/6 | 0/6 |

## Functions by family

| Family | Entries | Parsed | Executed |
|---|---|---|---|
| scalar | 4 | 4/4 | 0/4 |
| aggregate | 16 | 16/16 | 5/16 |
| grouping | 3 | 3/3 | 1/3 |
| conversion | 19 | 19/19 | 0/19 |
| multivalue | 17 | 17/17 | 0/17 |
| date | 6 | 6/6 | 1/6 |
| string | 26 | 26/26 | 0/26 |
| math | 26 | 26/26 | 0/26 |
| ip | 2 | 2/2 | 0/2 |
| spatial | 15 | 15/15 | 0/15 |
| search | 5 | 5/5 | 0/5 |
| time_series | 13 | 13/13 | 0/13 |

## Unverified entries

Recalled as development-only, but not confirmed against the 9.5 reference. Each is kept out of the denominators; if the release ships one, it adds one to every denominator as a gap.

- `IP_LOCATION` (processing_command)
- `METRICS_INFO` (processing_command)
- `REGISTERED_DOMAIN` (processing_command)
- `TS_COLLAPSE` (processing_command)
- `TS_INFO` (processing_command)
- `URI_PARTS` (processing_command)
- `USER_AGENT` (processing_command)

## Development-only commands

The parser recognises each of these and refuses it with a typed error. None is in a denominator.

`EXPLAIN`, `METRICS`, `DEDUP`, `HIGHLIGHT`, `LOOKUP`, `JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`, `IP_LOCATION`, `METRICS_INFO`, `REGISTERED_DOMAIN`, `TS_COLLAPSE`, `TS_INFO`, `URI_PARTS`, `USER_AGENT`

## Divergences

Valid statements Ravel answers differently from the release, each pinned by a test.

- **An unmapped field reads an attribute key.** A field name that is not a native column or a typed attribute column resolves to the log record's attribute of that key, read as a keyword, and reads null where a record lacks it (ADR-2887 D10 rule 4). The release reports an unknown column. Test: `crates/ravel-sql/tests/esql/binder.rs::an_attribute_reads_null_when_absent`.
- **@timestamp and message shadow attributes.** @timestamp and message always name the record's timestamp and body; an attribute with either key is not reachable under that name. Test: `crates/ravel-sql/tests/esql/manifest.rs::timestamp_and_message_shadow_attributes_of_the_same_name`.
- **No rows after the query clock without an upper bound.** A statement with no upper bound on @timestamp and no request end returns no record later than the query clock (the ADR-2887 D11 amendment). Test: `crates/ravel-sql/tests/esql/window.rs::a_lower_bound_alone_returns_rows_up_to_the_clock_on_every_layout`.
- **Expression depth limit of 24.** A statement that binds to an expression deeper than 24 is refused with a verification error. Test: `crates/ravel-sql/tests/esql/limits.rs::an_expression_past_the_depth_limit_is_refused`.
- **Plan stage limit of 10.** A statement with more than 10 processing commands is refused with a verification error. Test: `crates/ravel-sql/tests/esql/limits.rs::a_statement_past_the_stage_limit_is_refused`.
- **10,000-row result cap.** A final LIMIT above 10,000 is lowered to 10,000. Test: `crates/ravel-sql/tests/esql/limits.rs::a_final_limit_above_the_ceiling_is_lowered_to_it`.
- **Overflow and division by zero give null without a warning.** Integer overflow, division by zero and remainder by zero give null, and the response carries no warning for them. Test: `crates/ravel-sql/tests/esql/manifest.rs::an_overflow_gives_null_without_a_warning`.
- **An integer literal is a long.** An integer literal that fits in 32 bits is typed long, where the release types it integer. Test: `crates/ravel-sql/tests/esql/manifest.rs::an_integer_literal_is_typed_long`.

## Transport

| Option | Description | State |
|---|---|---|
| `query` | the statement text in the request body | absent |
| `params` | positional and named parameter values | absent |
| `filter` | a query DSL filter applied before the statement | absent |
| `columnar` | values returned column by column | absent |
| `locale` | the locale for formatting | absent |
| `profile` | a profile of the execution returned with the result | absent |
| `include_ccs_metadata` | per-cluster metadata in the response | absent |
| `tables` | inline tables for lookup | absent |
| `format` | the response format: json, csv, tsv, txt, yaml, cbor, smile or arrow | absent |
| `delimiter` | the separator of the csv format | absent |
| `drop_null_columns` | columns that are null in every row left out | absent |
| `async query` | POST /_query/async, with wait_for_completion_timeout, keep_alive and keep_on_completion | absent |
| `warnings` | warnings returned as Warning response headers | absent |

## Entries

Every released entry. Parser, binder and executor give each layer's state: supported, refused with a typed error, or absent (the layer never sees or does not know the form).

| Kind | Name | Family | Preview | Parser | Binder | Executor | Needs | Example |
|---|---|---|---|---|---|---|---|---|
| source_command | `FROM` |  |  | supported | supported | supported |  | `FROM logs` |
| source_command | `ROW` |  |  | supported | supported | supported |  | `ROW a = 1, b = "x"` |
| source_command | `SHOW INFO` |  |  | supported | refused | refused |  | `SHOW INFO` |
| source_command | `TS` |  | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS r = SUM(RATE(requests))` |
| source_command | `PROMQL` |  | yes | supported | refused | refused | time series data stream | `PROMQL (rate(http_requests_total[5m]))` |
| processing_command | `WHERE` |  |  | supported | supported | supported |  | `FROM logs \| WHERE severity_num > 10` |
| processing_command | `EVAL` |  |  | supported | supported | supported |  | `FROM logs \| EVAL d2 = dur * 2` |
| processing_command | `KEEP` |  |  | supported | supported | supported |  | `FROM logs \| KEEP @timestamp, message` |
| processing_command | `DROP` |  |  | supported | supported | supported |  | `FROM logs \| DROP trace_id, span_id` |
| processing_command | `RENAME` |  |  | supported | supported | supported |  | `FROM logs \| RENAME message AS msg` |
| processing_command | `STATS` |  |  | supported | supported | supported |  | `FROM logs \| STATS c = COUNT(*)` |
| processing_command | `SORT` |  |  | supported | supported | supported |  | `FROM logs \| SORT severity_num DESC` |
| processing_command | `LIMIT` |  |  | supported | supported | supported |  | `FROM logs \| LIMIT 3` |
| processing_command | `DISSECT` |  |  | supported | refused | refused |  | `FROM logs \| DISSECT message "%{a} %{b}"` |
| processing_command | `GROK` |  |  | supported | refused | refused |  | `FROM logs \| GROK message "%{WORD:verb} %{NUMBER:code}"` |
| processing_command | `MV_EXPAND` |  | yes | supported | refused | refused |  | `FROM logs \| MV_EXPAND service` |
| processing_command | `ENRICH` |  |  | supported | refused | refused | enrich policy | `FROM logs \| ENRICH regions_policy ON region` |
| processing_command | `LOOKUP JOIN` |  |  | supported | refused | refused | lookup index | `FROM logs \| LOOKUP JOIN services_lookup ON service` |
| processing_command | `INLINE STATS` |  | yes | supported | refused | refused |  | `FROM logs \| INLINE STATS m = MAX(dur) BY service` |
| processing_command | `FORK` |  | yes | supported | refused | refused |  | `FROM logs \| FORK (WHERE dur > 5) (WHERE dur <= 5 \| LIMIT 2)` |
| processing_command | `FUSE` |  | yes | supported | refused | refused | search index | `FROM logs METADATA _id, _index, _score \| FORK (WHERE dur > 5) (WHERE dur <= 5) \| FUSE` |
| processing_command | `CHANGE_POINT` |  | yes | supported | refused | refused |  | `FROM logs \| STATS c = COUNT(*) BY t = BUCKET(@timestamp, 1 minute) \| CHANGE_POINT c ON t` |
| processing_command | `SAMPLE` |  | yes | supported | refused | refused |  | `FROM logs \| SAMPLE 0.5` |
| processing_command | `COMPLETION` |  | yes | supported | refused | refused | inference endpoint | `FROM logs \| COMPLETION answer = message WITH {"inference_id": "my-llm"}` |
| processing_command | `RERANK` |  | yes | supported | refused | refused | inference endpoint | `FROM logs \| RERANK "timeout" ON message WITH {"inference_id": "reranker"}` |
| directive | `SET` |  | yes | supported | refused | refused |  | `SET time_zone = "Europe/Paris"; ROW a = 1` |
| syntax | `FROM over several sources` |  |  | supported | refused | refused |  | `FROM logs, archive` |
| syntax | `FROM source pattern` |  |  | supported | refused | refused |  | `FROM logs-*` |
| syntax | `FROM remote source` |  |  | supported | refused | refused | remote cluster | `FROM remote:logs` |
| syntax | `FROM METADATA` |  |  | supported | refused | refused |  | `FROM logs METADATA _index` |
| syntax | `FROM subquery` |  | yes | supported | refused | refused |  | `FROM logs, (FROM logs \| WHERE dur > 5)` |
| syntax | `STATS BY` |  |  | supported | supported | supported |  | `FROM logs \| STATS c = COUNT(*) BY service` |
| syntax | `STATS aggregate WHERE` |  |  | supported | supported | supported |  | `FROM logs \| STATS c = COUNT(*) WHERE severity_num > 10 BY service` |
| syntax | `RENAME with =` |  |  | supported | supported | supported |  | `FROM logs \| RENAME msg = message` |
| syntax | `SORT NULLS FIRST and LAST` |  |  | supported | supported | supported |  | `FROM logs \| SORT dur ASC NULLS FIRST, service DESC NULLS LAST` |
| syntax | `LIMIT BY` |  | yes | supported | refused | refused |  | `FROM logs \| LIMIT 1 BY service` |
| syntax | `DISSECT APPEND_SEPARATOR` |  |  | supported | refused | refused |  | `FROM logs \| DISSECT message "%{+a} %{+a}" APPEND_SEPARATOR = "-"` |
| syntax | `ENRICH WITH` |  |  | supported | refused | refused | enrich policy | `FROM logs \| ENRICH regions_policy ON region WITH country = name` |
| syntax | `quoted identifier` |  |  | supported | supported | supported |  | `` FROM logs \| KEEP `message` `` |
| syntax | `triple-quoted string` |  |  | supported | supported | supported |  | `ROW s = """a "quoted" word"""` |
| syntax | `comments` |  |  | supported | supported | supported |  | `ROW a = 1 /* block */ // line` |
| syntax | `time duration literal` |  |  | supported | supported | supported |  | `FROM logs \| WHERE @timestamp > NOW() - 1 hour` |
| syntax | `date period literal` |  |  | supported | refused | refused |  | `FROM logs \| WHERE @timestamp > NOW() - 1 month` |
| syntax | `multivalue literal` |  |  | supported | refused | refused |  | `ROW a = [1, 2, 3]` |
| syntax | `function options map` |  | yes | supported | refused | refused | search index | `FROM logs \| WHERE MATCH(message, "timeout", {"fuzziness": 1})` |
| function | `AVG` | aggregate |  | supported | supported | supported |  | `FROM logs \| STATS a = AVG(dur)` |
| function | `COUNT` | aggregate |  | supported | supported | supported |  | `FROM logs \| STATS c = COUNT(dur), all = COUNT(*)` |
| function | `COUNT_DISTINCT` | aggregate |  | supported | refused | refused |  | `FROM logs \| STATS c = COUNT_DISTINCT(service)` |
| function | `MAX` | aggregate |  | supported | supported | supported |  | `FROM logs \| STATS m = MAX(dur)` |
| function | `MEDIAN` | aggregate |  | supported | refused | refused |  | `FROM logs \| STATS m = MEDIAN(dur)` |
| function | `MEDIAN_ABSOLUTE_DEVIATION` | aggregate |  | supported | refused | refused |  | `FROM logs \| STATS m = MEDIAN_ABSOLUTE_DEVIATION(dur)` |
| function | `MIN` | aggregate |  | supported | supported | supported |  | `FROM logs \| STATS m = MIN(dur)` |
| function | `PERCENTILE` | aggregate |  | supported | refused | refused |  | `FROM logs \| STATS p = PERCENTILE(dur, 95)` |
| function | `SAMPLE` | aggregate | yes | supported | refused | refused |  | `FROM logs \| STATS s = SAMPLE(message, 2)` |
| function | `ST_CENTROID_AGG` | aggregate |  | supported | refused | refused |  | `FROM logs \| STATS c = ST_CENTROID_AGG(location)` |
| function | `ST_EXTENT_AGG` | aggregate | yes | supported | refused | refused |  | `FROM logs \| STATS e = ST_EXTENT_AGG(location)` |
| function | `STD_DEV` | aggregate |  | supported | refused | refused |  | `FROM logs \| STATS s = STD_DEV(dur)` |
| function | `SUM` | aggregate |  | supported | supported | supported |  | `FROM logs \| STATS s = SUM(dur)` |
| function | `TOP` | aggregate |  | supported | refused | refused |  | `FROM logs \| STATS t = TOP(dur, 3, "desc")` |
| function | `VALUES` | aggregate | yes | supported | refused | refused |  | `FROM logs \| STATS v = VALUES(service)` |
| function | `WEIGHTED_AVG` | aggregate |  | supported | refused | refused |  | `FROM logs \| STATS w = WEIGHTED_AVG(dur, severity_num)` |
| function | `BUCKET` | grouping |  | supported | supported | supported |  | `FROM logs \| STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 minute)` |
| function | `CATEGORIZE` | grouping | yes | supported | refused | refused |  | `FROM logs \| STATS c = COUNT(*) BY k = CATEGORIZE(message)` |
| function | `TBUCKET` | grouping | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS r = MAX(RATE(requests)) BY TBUCKET(1 hour)` |
| function | `CASE` | scalar |  | supported | refused | refused |  | `FROM logs \| EVAL level = CASE(severity_num > 12, "high", "low")` |
| function | `COALESCE` | scalar |  | supported | refused | refused |  | `FROM logs \| EVAL s = COALESCE(service, "none")` |
| function | `GREATEST` | scalar |  | supported | refused | refused |  | `ROW g = GREATEST(1, 5, 3)` |
| function | `LEAST` | scalar |  | supported | refused | refused |  | `ROW l = LEAST(1, 5, 3)` |
| function | `TO_AGGREGATE_METRIC_DOUBLE` | conversion | yes | supported | refused | refused |  | `ROW a = TO_AGGREGATE_METRIC_DOUBLE(1.5)` |
| function | `TO_BOOLEAN` | conversion |  | supported | refused | refused |  | `ROW a = TO_BOOLEAN("true")` |
| function | `TO_CARTESIANPOINT` | conversion |  | supported | refused | refused |  | `ROW a = TO_CARTESIANPOINT("POINT(1 2)")` |
| function | `TO_CARTESIANSHAPE` | conversion |  | supported | refused | refused |  | `ROW a = TO_CARTESIANSHAPE("POINT(1 2)")` |
| function | `TO_DATEPERIOD` | conversion |  | supported | refused | refused |  | `ROW a = TO_DATEPERIOD("3 days")` |
| function | `TO_DATETIME` | conversion |  | supported | refused | refused |  | `ROW a = TO_DATETIME("2024-01-01T00:00:00Z")` |
| function | `TO_DATE_NANOS` | conversion |  | supported | refused | refused |  | `ROW a = TO_DATE_NANOS("2024-01-01T00:00:00.123456789Z")` |
| function | `TO_DEGREES` | conversion |  | supported | refused | refused |  | `ROW a = TO_DEGREES(3.14)` |
| function | `TO_DOUBLE` | conversion |  | supported | refused | refused |  | `ROW a = TO_DOUBLE("1.5")` |
| function | `TO_GEOPOINT` | conversion |  | supported | refused | refused |  | `ROW a = TO_GEOPOINT("POINT(1 2)")` |
| function | `TO_GEOSHAPE` | conversion |  | supported | refused | refused |  | `ROW a = TO_GEOSHAPE("POINT(1 2)")` |
| function | `TO_INTEGER` | conversion |  | supported | refused | refused |  | `ROW a = TO_INTEGER("42")` |
| function | `TO_IP` | conversion |  | supported | refused | refused |  | `ROW a = TO_IP("10.0.0.1")` |
| function | `TO_LONG` | conversion |  | supported | refused | refused |  | `ROW a = TO_LONG("42")` |
| function | `TO_RADIANS` | conversion |  | supported | refused | refused |  | `ROW a = TO_RADIANS(180.0)` |
| function | `TO_STRING` | conversion |  | supported | refused | refused |  | `ROW a = TO_STRING(42)` |
| function | `TO_TIMEDURATION` | conversion |  | supported | refused | refused |  | `ROW a = TO_TIMEDURATION("3 hours")` |
| function | `TO_UNSIGNED_LONG` | conversion |  | supported | refused | refused |  | `ROW a = TO_UNSIGNED_LONG("42")` |
| function | `TO_VERSION` | conversion |  | supported | refused | refused |  | `ROW a = TO_VERSION("1.2.3")` |
| function | `DATE_DIFF` | date |  | supported | refused | refused |  | `FROM logs \| EVAL d = DATE_DIFF("second", @timestamp, NOW())` |
| function | `DATE_EXTRACT` | date |  | supported | refused | refused |  | `FROM logs \| EVAL h = DATE_EXTRACT("hour_of_day", @timestamp)` |
| function | `DATE_FORMAT` | date |  | supported | refused | refused |  | `FROM logs \| EVAL d = DATE_FORMAT("yyyy-MM-dd", @timestamp)` |
| function | `DATE_PARSE` | date |  | supported | refused | refused |  | `ROW d = DATE_PARSE("yyyy-MM-dd", "2024-01-01")` |
| function | `DATE_TRUNC` | date |  | supported | refused | refused |  | `FROM logs \| EVAL h = DATE_TRUNC(1 hour, @timestamp)` |
| function | `NOW` | date |  | supported | supported | supported |  | `ROW t = NOW()` |
| function | `CIDR_MATCH` | ip |  | supported | refused | refused |  | `ROW m = CIDR_MATCH(TO_IP("10.0.0.1"), "10.0.0.0/8")` |
| function | `IP_PREFIX` | ip |  | supported | refused | refused |  | `ROW p = IP_PREFIX(TO_IP("10.1.2.3"), 24, 0)` |
| function | `ABS` | math |  | supported | refused | refused |  | `ROW a = ABS(-1)` |
| function | `ACOS` | math |  | supported | refused | refused |  | `ROW a = ACOS(0.5)` |
| function | `ASIN` | math |  | supported | refused | refused |  | `ROW a = ASIN(0.5)` |
| function | `ATAN` | math |  | supported | refused | refused |  | `ROW a = ATAN(0.5)` |
| function | `ATAN2` | math |  | supported | refused | refused |  | `ROW a = ATAN2(1.0, 2.0)` |
| function | `CBRT` | math |  | supported | refused | refused |  | `ROW a = CBRT(27.0)` |
| function | `CEIL` | math |  | supported | refused | refused |  | `ROW a = CEIL(1.2)` |
| function | `COS` | math |  | supported | refused | refused |  | `ROW a = COS(0.5)` |
| function | `COSH` | math |  | supported | refused | refused |  | `ROW a = COSH(0.5)` |
| function | `E` | math |  | supported | refused | refused |  | `ROW a = E()` |
| function | `EXP` | math |  | supported | refused | refused |  | `ROW a = EXP(1.0)` |
| function | `FLOOR` | math |  | supported | refused | refused |  | `ROW a = FLOOR(1.8)` |
| function | `HYPOT` | math |  | supported | refused | refused |  | `ROW a = HYPOT(3.0, 4.0)` |
| function | `LOG` | math |  | supported | refused | refused |  | `ROW a = LOG(2.0, 8.0)` |
| function | `LOG10` | math |  | supported | refused | refused |  | `ROW a = LOG10(100.0)` |
| function | `PI` | math |  | supported | refused | refused |  | `ROW a = PI()` |
| function | `POW` | math |  | supported | refused | refused |  | `ROW a = POW(2.0, 10)` |
| function | `ROUND` | math |  | supported | refused | refused |  | `ROW a = ROUND(1.2345, 2)` |
| function | `ROUND_TO` | math |  | supported | refused | refused |  | `ROW a = ROUND_TO(7, 0, 5, 10)` |
| function | `SIGNUM` | math |  | supported | refused | refused |  | `ROW a = SIGNUM(-3.0)` |
| function | `SIN` | math |  | supported | refused | refused |  | `ROW a = SIN(0.5)` |
| function | `SINH` | math |  | supported | refused | refused |  | `ROW a = SINH(0.5)` |
| function | `SQRT` | math |  | supported | refused | refused |  | `ROW a = SQRT(16.0)` |
| function | `TAN` | math |  | supported | refused | refused |  | `ROW a = TAN(0.5)` |
| function | `TANH` | math |  | supported | refused | refused |  | `ROW a = TANH(0.5)` |
| function | `TAU` | math |  | supported | refused | refused |  | `ROW a = TAU()` |
| function | `KQL` | search | yes | supported | refused | refused | search index | `FROM logs \| WHERE KQL("message: timeout")` |
| function | `MATCH` | search | yes | supported | refused | refused | search index | `FROM logs \| WHERE MATCH(message, "timeout")` |
| function | `MATCH_PHRASE` | search | yes | supported | refused | refused | search index | `FROM logs \| WHERE MATCH_PHRASE(message, "connection timeout")` |
| function | `MULTI_MATCH` | search | yes | supported | refused | refused | search index | `FROM logs \| WHERE MULTI_MATCH("timeout", message, service)` |
| function | `QSTR` | search | yes | supported | refused | refused | search index | `FROM logs \| WHERE QSTR("message:timeout")` |
| function | `ST_CONTAINS` | spatial |  | supported | refused | refused |  | `ROW c = ST_CONTAINS(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"), TO_GEOPOINT("POINT(1 1)"))` |
| function | `ST_DISJOINT` | spatial |  | supported | refused | refused |  | `ROW d = ST_DISJOINT(TO_GEOPOINT("POINT(5 5)"), TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_DISTANCE` | spatial |  | supported | refused | refused |  | `ROW d = ST_DISTANCE(TO_GEOPOINT("POINT(0 0)"), TO_GEOPOINT("POINT(1 1)"))` |
| function | `ST_ENVELOPE` | spatial | yes | supported | refused | refused |  | `ROW e = ST_ENVELOPE(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_GEOHASH` | spatial | yes | supported | refused | refused |  | `ROW h = ST_GEOHASH(TO_GEOPOINT("POINT(1 2)"), 5)` |
| function | `ST_GEOHEX` | spatial | yes | supported | refused | refused |  | `ROW h = ST_GEOHEX(TO_GEOPOINT("POINT(1 2)"), 5)` |
| function | `ST_GEOTILE` | spatial | yes | supported | refused | refused |  | `ROW t = ST_GEOTILE(TO_GEOPOINT("POINT(1 2)"), 5)` |
| function | `ST_INTERSECTS` | spatial |  | supported | refused | refused |  | `ROW i = ST_INTERSECTS(TO_GEOPOINT("POINT(1 1)"), TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_WITHIN` | spatial |  | supported | refused | refused |  | `ROW w = ST_WITHIN(TO_GEOPOINT("POINT(1 1)"), TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_X` | spatial |  | supported | refused | refused |  | `ROW x = ST_X(TO_GEOPOINT("POINT(1 2)"))` |
| function | `ST_XMAX` | spatial | yes | supported | refused | refused |  | `ROW x = ST_XMAX(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_XMIN` | spatial | yes | supported | refused | refused |  | `ROW x = ST_XMIN(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_Y` | spatial |  | supported | refused | refused |  | `ROW y = ST_Y(TO_GEOPOINT("POINT(1 2)"))` |
| function | `ST_YMAX` | spatial | yes | supported | refused | refused |  | `ROW y = ST_YMAX(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_YMIN` | spatial | yes | supported | refused | refused |  | `ROW y = ST_YMIN(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `BIT_LENGTH` | string |  | supported | refused | refused |  | `FROM logs \| EVAL n = BIT_LENGTH(message)` |
| function | `BYTE_LENGTH` | string |  | supported | refused | refused |  | `FROM logs \| EVAL n = BYTE_LENGTH(message)` |
| function | `CONCAT` | string |  | supported | refused | refused |  | `FROM logs \| EVAL s = CONCAT(message, "-", severity_text)` |
| function | `ENDS_WITH` | string |  | supported | refused | refused |  | `FROM logs \| WHERE ENDS_WITH(message, "a")` |
| function | `FROM_BASE64` | string |  | supported | refused | refused |  | `ROW s = FROM_BASE64("ZWxhc3RpYw==")` |
| function | `HASH` | string |  | supported | refused | refused |  | `FROM logs \| EVAL h = HASH("sha256", message)` |
| function | `LEFT` | string |  | supported | refused | refused |  | `FROM logs \| EVAL s = LEFT(message, 1)` |
| function | `LENGTH` | string |  | supported | refused | refused |  | `FROM logs \| EVAL n = LENGTH(message)` |
| function | `LOCATE` | string |  | supported | refused | refused |  | `FROM logs \| EVAL p = LOCATE(message, "a")` |
| function | `LTRIM` | string |  | supported | refused | refused |  | `ROW s = LTRIM("  a")` |
| function | `MD5` | string |  | supported | refused | refused |  | `FROM logs \| EVAL h = MD5(message)` |
| function | `REPEAT` | string |  | supported | refused | refused |  | `ROW s = REPEAT("ab", 3)` |
| function | `REPLACE` | string |  | supported | refused | refused |  | `FROM logs \| EVAL s = REPLACE(message, "a", "b")` |
| function | `REVERSE` | string |  | supported | refused | refused |  | `ROW s = REVERSE("abc")` |
| function | `RIGHT` | string |  | supported | refused | refused |  | `FROM logs \| EVAL s = RIGHT(message, 1)` |
| function | `RTRIM` | string |  | supported | refused | refused |  | `ROW s = RTRIM("a  ")` |
| function | `SHA1` | string |  | supported | refused | refused |  | `FROM logs \| EVAL h = SHA1(message)` |
| function | `SHA256` | string |  | supported | refused | refused |  | `FROM logs \| EVAL h = SHA256(message)` |
| function | `SPACE` | string |  | supported | refused | refused |  | `ROW s = SPACE(3)` |
| function | `SPLIT` | string |  | supported | refused | refused |  | `ROW s = SPLIT("a,b,c", ",")` |
| function | `STARTS_WITH` | string |  | supported | refused | refused |  | `FROM logs \| WHERE STARTS_WITH(message, "a")` |
| function | `SUBSTRING` | string |  | supported | refused | refused |  | `FROM logs \| EVAL s = SUBSTRING(message, 1, 1)` |
| function | `TO_BASE64` | string |  | supported | refused | refused |  | `ROW s = TO_BASE64("elastic")` |
| function | `TO_LOWER` | string |  | supported | refused | refused |  | `FROM logs \| EVAL s = TO_LOWER(severity_text)` |
| function | `TO_UPPER` | string |  | supported | refused | refused |  | `FROM logs \| EVAL s = TO_UPPER(message)` |
| function | `TRIM` | string |  | supported | refused | refused |  | `ROW s = TRIM("  a  ")` |
| function | `MV_APPEND` | multivalue |  | supported | refused | refused |  | `ROW m = MV_APPEND([1, 2], 3)` |
| function | `MV_AVG` | multivalue |  | supported | refused | refused |  | `ROW m = MV_AVG([1, 2, 3])` |
| function | `MV_CONCAT` | multivalue |  | supported | refused | refused |  | `ROW m = MV_CONCAT(["a", "b"], ",")` |
| function | `MV_COUNT` | multivalue |  | supported | refused | refused |  | `ROW m = MV_COUNT([1, 2, 3])` |
| function | `MV_DEDUPE` | multivalue |  | supported | refused | refused |  | `ROW m = MV_DEDUPE([1, 1, 2])` |
| function | `MV_FIRST` | multivalue |  | supported | refused | refused |  | `ROW m = MV_FIRST([1, 2, 3])` |
| function | `MV_LAST` | multivalue |  | supported | refused | refused |  | `ROW m = MV_LAST([1, 2, 3])` |
| function | `MV_MAX` | multivalue |  | supported | refused | refused |  | `ROW m = MV_MAX([1, 2, 3])` |
| function | `MV_MEDIAN` | multivalue |  | supported | refused | refused |  | `ROW m = MV_MEDIAN([1, 2, 3])` |
| function | `MV_MEDIAN_ABSOLUTE_DEVIATION` | multivalue |  | supported | refused | refused |  | `ROW m = MV_MEDIAN_ABSOLUTE_DEVIATION([1, 2, 5])` |
| function | `MV_MIN` | multivalue |  | supported | refused | refused |  | `ROW m = MV_MIN([1, 2, 3])` |
| function | `MV_PERCENTILE` | multivalue |  | supported | refused | refused |  | `ROW m = MV_PERCENTILE([1, 2, 3], 50)` |
| function | `MV_PSERIES_WEIGHTED_SUM` | multivalue |  | supported | refused | refused |  | `ROW m = MV_PSERIES_WEIGHTED_SUM([1.0, 2.0, 3.0], 1.5)` |
| function | `MV_SLICE` | multivalue |  | supported | refused | refused |  | `ROW m = MV_SLICE([1, 2, 3], 0, 1)` |
| function | `MV_SORT` | multivalue |  | supported | refused | refused |  | `ROW m = MV_SORT([3, 1, 2], "desc")` |
| function | `MV_SUM` | multivalue |  | supported | refused | refused |  | `ROW m = MV_SUM([1, 2, 3])` |
| function | `MV_ZIP` | multivalue |  | supported | refused | refused |  | `ROW m = MV_ZIP(["a", "b"], ["x", "y"], "-")` |
| function | `RATE` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS r = SUM(RATE(requests)) BY host` |
| function | `IRATE` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS r = SUM(IRATE(requests)) BY host` |
| function | `DELTA` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS d = SUM(DELTA(temperature)) BY host` |
| function | `IDELTA` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS d = SUM(IDELTA(temperature)) BY host` |
| function | `INCREASE` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS i = SUM(INCREASE(requests)) BY host` |
| function | `AVG_OVER_TIME` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS a = MAX(AVG_OVER_TIME(temperature)) BY host` |
| function | `COUNT_OVER_TIME` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS c = SUM(COUNT_OVER_TIME(temperature)) BY host` |
| function | `COUNT_DISTINCT_OVER_TIME` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS c = SUM(COUNT_DISTINCT_OVER_TIME(temperature)) BY host` |
| function | `FIRST_OVER_TIME` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS f = MAX(FIRST_OVER_TIME(temperature)) BY host` |
| function | `LAST_OVER_TIME` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS l = MAX(LAST_OVER_TIME(temperature)) BY host` |
| function | `MAX_OVER_TIME` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS m = MAX(MAX_OVER_TIME(temperature)) BY host` |
| function | `MIN_OVER_TIME` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS m = MIN(MIN_OVER_TIME(temperature)) BY host` |
| function | `SUM_OVER_TIME` | time_series | yes | supported | refused | refused | time series data stream | `TS metrics \| STATS s = SUM(SUM_OVER_TIME(temperature)) BY host` |
| alias | `INLINESTATS` (of `INLINE STATS`) |  | yes | supported | refused | refused |  | `FROM logs \| INLINESTATS m = MAX(dur) BY service` |
| alias | `TO_BOOL` (of `TO_BOOLEAN`) |  |  | supported | refused | refused |  | `ROW a = TO_BOOL("true")` |
| alias | `TO_DT` (of `TO_DATETIME`) |  |  | supported | refused | refused |  | `ROW a = TO_DT("2024-01-01T00:00:00Z")` |
| alias | `TO_DBL` (of `TO_DOUBLE`) |  |  | supported | refused | refused |  | `ROW a = TO_DBL("1.5")` |
| alias | `TO_INT` (of `TO_INTEGER`) |  |  | supported | refused | refused |  | `ROW a = TO_INT("42")` |
| alias | `TO_STR` (of `TO_STRING`) |  |  | supported | refused | refused |  | `ROW a = TO_STR(42)` |
| alias | `TO_UL` (of `TO_UNSIGNED_LONG`) |  |  | supported | refused | refused |  | `ROW a = TO_UL("42")` |
| alias | `TO_ULONG` (of `TO_UNSIGNED_LONG`) |  |  | supported | refused | refused |  | `ROW a = TO_ULONG("42")` |
| alias | `TO_VER` (of `TO_VERSION`) |  |  | supported | refused | refused |  | `ROW a = TO_VER("1.2.3")` |
| operator | `OR` |  |  | supported | supported | supported |  | `FROM logs \| WHERE dur > 10 OR dur < 5` |
| operator | `AND` |  |  | supported | supported | supported |  | `FROM logs \| WHERE dur > 1 AND dur < 20` |
| operator | `NOT` |  |  | supported | supported | supported |  | `FROM logs \| WHERE NOT dur > 10` |
| operator | `==` |  |  | supported | supported | supported |  | `FROM logs \| WHERE service == "api"` |
| operator | `!=` |  |  | supported | supported | supported |  | `FROM logs \| WHERE service != "api"` |
| operator | `<` |  |  | supported | supported | supported |  | `FROM logs \| WHERE dur < 10` |
| operator | `<=` |  |  | supported | supported | supported |  | `FROM logs \| WHERE dur <= 10` |
| operator | `>` |  |  | supported | supported | supported |  | `FROM logs \| WHERE dur > 10` |
| operator | `>=` |  |  | supported | supported | supported |  | `FROM logs \| WHERE dur >= 10` |
| operator | `+` |  |  | supported | supported | supported |  | `ROW a = 1 + 2` |
| operator | `-` |  |  | supported | supported | supported |  | `ROW a = 5 - 2` |
| operator | `*` |  |  | supported | supported | supported |  | `ROW a = 2 * 3` |
| operator | `/` |  |  | supported | supported | supported |  | `ROW a = 7 / 2` |
| operator | `%` |  |  | supported | supported | supported |  | `ROW a = 7 % 2` |
| operator | `unary -` |  |  | supported | supported | supported |  | `ROW a = -(1 + 2)` |
| operator | `IS NULL` |  |  | supported | supported | supported |  | `FROM logs \| WHERE dur IS NULL` |
| operator | `IS NOT NULL` |  |  | supported | supported | supported |  | `FROM logs \| WHERE dur IS NOT NULL` |
| operator | `IN` |  |  | supported | supported | supported |  | `FROM logs \| WHERE service IN ("api", "web")` |
| operator | `NOT IN` |  |  | supported | supported | supported |  | `FROM logs \| WHERE service NOT IN ("api")` |
| operator | `IN subquery` |  | yes | supported | refused | refused |  | `FROM logs \| WHERE service IN (FROM logs \| WHERE dur > 5 \| KEEP service)` |
| operator | `LIKE` |  |  | supported | refused | refused |  | `FROM logs \| WHERE message LIKE "a*"` |
| operator | `NOT LIKE` |  |  | supported | refused | refused |  | `FROM logs \| WHERE message NOT LIKE "a*"` |
| operator | `RLIKE` |  |  | supported | refused | refused |  | `FROM logs \| WHERE message RLIKE "a.*"` |
| operator | `NOT RLIKE` |  |  | supported | refused | refused |  | `FROM logs \| WHERE message NOT RLIKE "a.*"` |
| operator | `:` |  | yes | supported | refused | refused | search index | `FROM logs \| WHERE message : "timeout"` |
| operator | `::` |  |  | supported | refused | refused |  | `ROW a = "1"::long` |
| type | `keyword` |  |  | supported | supported | supported |  | `ROW a = "x"` |
| type | `text` |  |  | supported | refused | refused |  | `ROW a = "x"::text` |
| type | `long` |  |  | supported | supported | supported |  | `FROM logs \| KEEP severity_num` |
| type | `integer` |  |  | supported | refused | refused |  | `ROW a = "1"::integer` |
| type | `unsigned_long` |  |  | supported | refused | refused |  | `ROW a = "1"::unsigned_long` |
| type | `double` |  |  | supported | supported | supported |  | `ROW a = 1.5` |
| type | `boolean` |  |  | supported | supported | supported |  | `ROW a = true` |
| type | `date` |  |  | supported | refused | refused |  | `ROW a = "2024-01-01T00:00:00Z"::datetime` |
| type | `date_nanos` |  |  | supported | supported | supported |  | `FROM logs \| KEEP @timestamp` |
| type | `null` |  |  | supported | supported | supported |  | `ROW a = null` |
| type | `ip` |  |  | supported | refused | refused |  | `ROW a = "10.0.0.1"::ip` |
| type | `version` |  |  | supported | refused | refused |  | `ROW a = "1.2.3"::version` |
| type | `geo_point` |  |  | supported | refused | refused |  | `ROW a = "POINT(1 2)"::geo_point` |
| type | `geo_shape` |  |  | supported | refused | refused |  | `ROW a = "POINT(1 2)"::geo_shape` |
| type | `cartesian_point` |  |  | supported | refused | refused |  | `ROW a = "POINT(1 2)"::cartesian_point` |
| type | `cartesian_shape` |  |  | supported | refused | refused |  | `ROW a = "POINT(1 2)"::cartesian_shape` |
| type | `time_duration` |  |  | supported | refused | refused |  | `ROW a = 1 hour` |
| type | `date_period` |  |  | supported | refused | refused |  | `ROW a = 1 month` |
| type | `counter_long` |  | yes | supported | refused | refused | time series data stream | `TS metrics \| KEEP requests` |
| type | `counter_integer` |  | yes | supported | refused | refused | time series data stream | `TS metrics \| KEEP connections` |
| type | `counter_double` |  | yes | supported | refused | refused | time series data stream | `TS metrics \| KEEP bytes` |
| type | `aggregate_metric_double` |  | yes | supported | refused | refused |  | `ROW a = TO_AGGREGATE_METRIC_DOUBLE(1.5)` |
| parameter | `?` |  |  | supported | refused | refused |  | `ROW a = ?` |
| parameter | `?n` |  |  | supported | refused | refused |  | `ROW a = ?1` |
| parameter | `?name` |  |  | supported | refused | refused |  | `ROW a = ?limit` |
| parameter | `??` |  | yes | supported | refused | refused |  | `FROM logs \| KEEP ??` |
| parameter | `??n` |  | yes | supported | refused | refused |  | `FROM logs \| KEEP ??1` |
| parameter | `??name` |  | yes | supported | refused | refused |  | `FROM logs \| KEEP ??field` |
