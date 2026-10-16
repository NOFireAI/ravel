# ES|QL conformance

<!--
  GENERATED FILE -- do not edit by hand.
  Generated from crates/ravel-esql/manifest/esql-9.5.toml by
  crates/ravel-esql/tests/conformance.rs. Regenerate with:
      REGEN_ESQL_CONFORMANCE=1 cargo test -p ravel-esql --test conformance
-->

How much of the released ES|QL language Ravel's ES|QL front end accepts and runs (ADR-2887 D2). Target release line: 9.5.

The manifest this report is generated from was written from the published ES|QL language reference for the 9.5 line, without consulting the Elasticsearch source tree.
It has not been reconciled with the released 9.5.5 binary (ADR-2887 D3), so an entry can be missing, misnamed or carry the wrong preview flag, and an expected answer can be wrong.

## Figures

Each figure counts released entries only; development-only commands are listed below and are in no denominator.

| Figure | Score | What counts |
|---|---|---|
| Syntax coverage | 84/84 | released entries with a parser rule of their own whose example the parser accepts |
| Execution compatibility | 27/260 | released entries whose example runs and gives the release's answer, columns, types, rows and warnings; a valid entry Ravel refuses or answers differently is a gap |
| Transport compatibility | 0/13 | options of the Elasticsearch query endpoint that Ravel serves |

Ran and diverged: 24/260, released entries whose example runs and gives an answer that differs from the release's by exactly one known divergence: the answer differs as given, and undoing that divergence in it gives the release's columns, types, rows and warnings.

Ran: 51/260, released entries whose example runs, whether or not the answer matches.

Accepted through a generic rule: 176/176, released functions, aliases and types the parser reads only through the generic call rule `name(args)`, the generic cast rule `value::type`, or as a plain field. The parser does not check the name in any of these rules, so a made-up name parses the same way; they are not in syntax coverage.

Negative statements: 14, each refused by the parser. They are counted on this line only and never raise a figure above.

## By kind

| Kind | Entries | Parsed | Ran | Compatible |
|---|---|---|---|---|
| source_command | 5 | 5/5 | 2/5 | 0/5 |
| processing_command | 20 | 20/20 | 8/20 | 4/20 |
| directive | 1 | 1/1 | 0/1 | 0/1 |
| syntax | 19 | 19/19 | 8/19 | 6/19 |
| function | 152 | 152/152 | 7/152 | 6/152 |
| alias | 9 | 9/9 | 0/9 | 0/9 |
| operator | 26 | 26/26 | 19/26 | 4/26 |
| type | 22 | 22/22 | 7/22 | 7/22 |
| parameter | 6 | 6/6 | 0/6 | 0/6 |

## Functions by family

| Family | Entries | Parsed | Ran | Compatible |
|---|---|---|---|---|
| scalar | 4 | 4/4 | 0/4 | 0/4 |
| aggregate | 16 | 16/16 | 5/16 | 5/16 |
| grouping | 3 | 3/3 | 1/3 | 1/3 |
| conversion | 19 | 19/19 | 0/19 | 0/19 |
| multivalue | 17 | 17/17 | 0/17 | 0/17 |
| date | 6 | 6/6 | 1/6 | 0/6 |
| string | 26 | 26/26 | 0/26 | 0/26 |
| math | 26 | 26/26 | 0/26 | 0/26 |
| ip | 2 | 2/2 | 0/2 | 0/2 |
| spatial | 15 | 15/15 | 0/15 | 0/15 |
| search | 5 | 5/5 | 0/5 | 0/5 |
| time_series | 13 | 13/13 | 0/13 | 0/13 |

## Unverified entries

The manifest's author classified each of these as development-only from recollection of the 9.5 reference, and has not confirmed that classification against the release. Each is kept out of the denominators; if the release ships one, it adds one as a gap to the execution compatibility denominator and to the syntax coverage or generic-rule denominator its grammar selects.

- `IP_LOCATION` (processing_command)
- `METRICS_INFO` (processing_command)
- `REGISTERED_DOMAIN` (processing_command)
- `TS_COLLAPSE` (processing_command)
- `TS_INFO` (processing_command)
- `URI_PARTS` (processing_command)
- `USER_AGENT` (processing_command)

## Development-only commands

The parser recognises and refuses with a typed error each of these. None is in a denominator.

`EXPLAIN`, `METRICS`, `DEDUP`, `HIGHLIGHT`, `LOOKUP`, `JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`, `IP_LOCATION`, `METRICS_INFO`, `REGISTERED_DOMAIN`, `TS_COLLAPSE`, `TS_INFO`, `URI_PARTS`, `USER_AGENT`

## Refused through an earlier construct

Released entries whose example cannot reach the form without an earlier construct the binder refuses. The refusal names that construct, so these entries do not show that the binder knows their own form.

- `FUSE` (processing_command): refused at `FROM METADATA`
- `DISSECT APPEND_SEPARATOR` (syntax): refused at `DISSECT`
- `ENRICH WITH` (syntax): refused at `ENRICH`
- `function options map` (syntax): refused at `MATCH`
- `NOT LIKE` (operator): refused at `LIKE`
- `NOT RLIKE` (operator): refused at `RLIKE`
- `counter_long` (type): refused at `TS`
- `counter_integer` (type): refused at `TS`
- `counter_double` (type): refused at `TS`
- `aggregate_metric_double` (type): refused at `TO_AGGREGATE_METRIC_DOUBLE`
- `TBUCKET` (function): refused at `TS`
- `RATE` (function): refused at `TS`
- `IRATE` (function): refused at `TS`
- `DELTA` (function): refused at `TS`
- `IDELTA` (function): refused at `TS`
- `INCREASE` (function): refused at `TS`
- `AVG_OVER_TIME` (function): refused at `TS`
- `COUNT_OVER_TIME` (function): refused at `TS`
- `COUNT_DISTINCT_OVER_TIME` (function): refused at `TS`
- `FIRST_OVER_TIME` (function): refused at `TS`
- `LAST_OVER_TIME` (function): refused at `TS`
- `MAX_OVER_TIME` (function): refused at `TS`
- `MIN_OVER_TIME` (function): refused at `TS`
- `SUM_OVER_TIME` (function): refused at `TS`

## Known divergences

Valid statements Ravel answers differently from the release, each pinned by a test. The list is not shown to be complete: no test compares a Ravel answer with the released 9.5.5 binary, only with answers written by hand in the manifest, so a divergence nobody wrote down is not found.

- **An unmapped field reads an attribute key** (`attribute-fallback`). A field name that is not a native column or a typed attribute column resolves to the log record's attribute of that key, read as a keyword, and reads null where a record lacks it (ADR-2887 D10 rule 4). The release reports an unknown column. Test: `crates/ravel-sql/tests/esql/binder.rs::an_attribute_reads_null_when_absent`. No entry shows it: The release refuses a statement that names an unmapped field, so an example showing the divergence has no release answer to compare Ravel's with.
- **@timestamp and message shadow attributes** (`shadowing`). @timestamp and message always name the record's timestamp and body; an attribute with either key is not reachable under that name. Test: `crates/ravel-sql/tests/esql/manifest.rs::timestamp_and_message_shadow_attributes_of_the_same_name`. No entry shows it: The logs fixture carries no attribute named @timestamp or message, so no example run on it can reach a shadowed attribute.
- **No rows after the query clock without an upper bound** (`clock-bound`). A statement with no upper bound on @timestamp and no request end returns no record later than the query clock (the ADR-2887 D11 amendment). Test: `crates/ravel-sql/tests/esql/window.rs::a_lower_bound_alone_returns_rows_up_to_the_clock_on_every_layout`. No entry shows it: Every record of the logs fixture is earlier than its query clock, so no example run on it returns a record the bound removes.
- **Expression depth limit of 24** (`depth-limit`). A statement that binds to an expression deeper than 24 is refused with a verification error. Test: `crates/ravel-sql/tests/esql/limits.rs::an_expression_past_the_depth_limit_is_refused`. No entry shows it: No example binds to an expression deeper than 24.
- **Plan stage limit of 10** (`stage-limit`). A statement with more than 10 processing commands is refused with a verification error. Test: `crates/ravel-sql/tests/esql/limits.rs::a_statement_past_the_stage_limit_is_refused`. No entry shows it: No example has more than 10 processing commands.
- **10,000-row result cap** (`row-cap`). A final LIMIT above 10,000 is lowered to 10,000. Test: `crates/ravel-sql/tests/esql/limits.rs::a_final_limit_above_the_ceiling_is_lowered_to_it`. No entry shows it: The logs fixture has six records, so no example returns more than 10,000 rows.
- **Overflow and division by zero give null without a warning** (`overflow-null`). Integer overflow, division by zero and remainder by zero give null, and the response carries no warning for them. Test: `crates/ravel-sql/tests/esql/manifest.rs::an_overflow_gives_null_without_a_warning`. No entry shows it: No example overflows or divides by zero.
- **An integer literal is a long** (`integer-literal`). An integer literal that fits in 32 bits is typed long, where the release types it integer. Test: `crates/ravel-sql/tests/esql/manifest.rs::an_integer_literal_is_typed_long`. Shown by: `ROW`, `comments`, `+`, `-`, `*`, `/`, `%`, `unary -`.
- **FROM lists the native columns in a fixed order** (`from-columns`). FROM without KEEP returns the native columns in Ravel's fixed order (@timestamp, observed_ts, severity_num, severity_text, message, trace_id, span_id, flags, dur, region), where the release lists the mapped fields sorted by name. Test: `crates/ravel-sql/tests/esql/manifest.rs::from_lists_the_native_columns_in_a_fixed_order`. Shown by: `FROM`, `WHERE`, `DROP`, `SORT`, `LIMIT`, `time duration literal`, `OR`, `AND`, `NOT`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT NULL`.
- **NOW() is a date_nanos** (`now-type`). NOW() is typed date_nanos, where the release types it date at millisecond precision. Test: `crates/ravel-sql/tests/esql/manifest.rs::now_is_typed_date_nanos`. Shown by: `NOW`.

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

Grammar is `own` for a form the parser has a rule for, and `call`, `cast` or `field` for one it reads only through a generic rule. Divergence names the known divergence the entry's answer shows.

| Kind | Name | Family | Preview | Grammar | Parser | Binder | Executor | Divergence | Needs | Example |
|---|---|---|---|---|---|---|---|---|---|---|
| source_command | `FROM` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs` |
| source_command | `ROW` |  |  | own | supported | supported | supported | `integer-literal` |  | `ROW a = 1, b = "x"` |
| source_command | `SHOW INFO` |  |  | own | supported | refused | refused |  |  | `SHOW INFO` |
| source_command | `TS` |  | yes | own | supported | refused | refused |  | time series data stream | `TS metrics \| STATS r = SUM(RATE(requests))` |
| source_command | `PROMQL` |  | yes | own | supported | refused | refused |  | time series data stream | `PROMQL (rate(http_requests_total[5m]))` |
| processing_command | `WHERE` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE severity_num > 10` |
| processing_command | `EVAL` |  |  | own | supported | supported | supported |  |  | `FROM logs \| EVAL d2 = dur * 2 \| KEEP dur, d2` |
| processing_command | `KEEP` |  |  | own | supported | supported | supported |  |  | `FROM logs \| KEEP @timestamp, message` |
| processing_command | `DROP` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| DROP trace_id, span_id` |
| processing_command | `RENAME` |  |  | own | supported | supported | supported |  |  | `FROM logs \| RENAME message AS msg \| KEEP msg` |
| processing_command | `STATS` |  |  | own | supported | supported | supported |  |  | `FROM logs \| STATS c = COUNT(*)` |
| processing_command | `SORT` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| SORT severity_num DESC` |
| processing_command | `LIMIT` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| LIMIT 3` |
| processing_command | `DISSECT` |  |  | own | supported | refused | refused |  |  | `FROM logs \| DISSECT message "%{a} %{b}"` |
| processing_command | `GROK` |  |  | own | supported | refused | refused |  |  | `FROM logs \| GROK message "%{WORD:verb} %{NUMBER:code}"` |
| processing_command | `MV_EXPAND` |  | yes | own | supported | refused | refused |  |  | `FROM logs \| MV_EXPAND service` |
| processing_command | `ENRICH` |  |  | own | supported | refused | refused |  | enrich policy | `FROM logs \| ENRICH regions_policy ON region` |
| processing_command | `LOOKUP JOIN` |  |  | own | supported | refused | refused |  | lookup index | `FROM logs \| LOOKUP JOIN services_lookup ON service` |
| processing_command | `INLINE STATS` |  | yes | own | supported | refused | refused |  |  | `FROM logs \| INLINE STATS m = MAX(dur) BY service` |
| processing_command | `FORK` |  | yes | own | supported | refused | refused |  |  | `FROM logs \| FORK (WHERE dur > 5) (WHERE dur <= 5 \| LIMIT 2)` |
| processing_command | `FUSE` |  | yes | own | supported | refused | refused |  | search index | `FROM logs METADATA _id, _index, _score \| FORK (WHERE dur > 5) (WHERE dur <= 5) \| FUSE` |
| processing_command | `CHANGE_POINT` |  | yes | own | supported | refused | refused |  |  | `FROM logs \| STATS c = COUNT(*) BY t = BUCKET(@timestamp, 1 minute) \| CHANGE_POINT c ON t` |
| processing_command | `SAMPLE` |  | yes | own | supported | refused | refused |  |  | `FROM logs \| SAMPLE 0.5` |
| processing_command | `COMPLETION` |  | yes | own | supported | refused | refused |  | inference endpoint | `FROM logs \| COMPLETION answer = message WITH {"inference_id": "my-llm"}` |
| processing_command | `RERANK` |  | yes | own | supported | refused | refused |  | inference endpoint | `FROM logs \| RERANK "timeout" ON message WITH {"inference_id": "reranker"}` |
| directive | `SET` |  | yes | own | supported | refused | refused |  |  | `SET time_zone = "Europe/Paris"; ROW a = 1` |
| syntax | `FROM over several sources` |  |  | own | supported | refused | refused |  |  | `FROM logs, archive` |
| syntax | `FROM source pattern` |  |  | own | supported | refused | refused |  |  | `FROM logs-*` |
| syntax | `FROM remote source` |  |  | own | supported | refused | refused |  | remote cluster | `FROM remote:logs` |
| syntax | `FROM METADATA` |  |  | own | supported | refused | refused |  |  | `FROM logs METADATA _index` |
| syntax | `FROM subquery` |  | yes | own | supported | refused | refused |  |  | `FROM logs, (FROM logs \| WHERE dur > 5)` |
| syntax | `STATS BY` |  |  | own | supported | supported | supported |  |  | `FROM logs \| STATS c = COUNT(*) BY region` |
| syntax | `STATS aggregate WHERE` |  |  | own | supported | supported | supported |  |  | `FROM logs \| STATS c = COUNT(*) WHERE severity_num > 10 BY region` |
| syntax | `RENAME with =` |  |  | own | supported | supported | supported |  |  | `FROM logs \| RENAME msg = message \| KEEP msg` |
| syntax | `SORT NULLS FIRST and LAST` |  |  | own | supported | supported | supported |  |  | `FROM logs \| SORT region DESC NULLS LAST, dur ASC NULLS FIRST \| KEEP message` |
| syntax | `LIMIT BY` |  | yes | own | supported | refused | refused |  |  | `FROM logs \| LIMIT 1 BY service` |
| syntax | `DISSECT APPEND_SEPARATOR` |  |  | own | supported | refused | refused |  |  | `FROM logs \| DISSECT message "%{+a} %{+a}" APPEND_SEPARATOR = "-"` |
| syntax | `ENRICH WITH` |  |  | own | supported | refused | refused |  | enrich policy | `FROM logs \| ENRICH regions_policy ON region WITH country = name` |
| syntax | `quoted identifier` |  |  | own | supported | supported | supported |  |  | `` FROM logs \| KEEP `message` `` |
| syntax | `triple-quoted string` |  |  | own | supported | supported | supported |  |  | `ROW s = """a "quoted" word"""` |
| syntax | `comments` |  |  | own | supported | supported | supported | `integer-literal` |  | `ROW a = 1 /* block */ // line` |
| syntax | `time duration literal` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE @timestamp > NOW() - 1 hour` |
| syntax | `date period literal` |  |  | own | supported | refused | refused |  |  | `FROM logs \| WHERE @timestamp > NOW() - 1 month` |
| syntax | `multivalue literal` |  |  | own | supported | refused | refused |  |  | `ROW a = [1, 2, 3]` |
| syntax | `function options map` |  | yes | own | supported | refused | refused |  | search index | `FROM logs \| WHERE MATCH(message, "timeout", {"fuzziness": 1})` |
| function | `AVG` | aggregate |  | call | supported | supported | supported |  |  | `FROM logs \| STATS a = AVG(dur)` |
| function | `COUNT` | aggregate |  | call | supported | supported | supported |  |  | `FROM logs \| STATS c = COUNT(dur), all = COUNT(*)` |
| function | `COUNT_DISTINCT` | aggregate |  | call | supported | refused | refused |  |  | `FROM logs \| STATS c = COUNT_DISTINCT(service)` |
| function | `MAX` | aggregate |  | call | supported | supported | supported |  |  | `FROM logs \| STATS m = MAX(dur)` |
| function | `MEDIAN` | aggregate |  | call | supported | refused | refused |  |  | `FROM logs \| STATS m = MEDIAN(dur)` |
| function | `MEDIAN_ABSOLUTE_DEVIATION` | aggregate |  | call | supported | refused | refused |  |  | `FROM logs \| STATS m = MEDIAN_ABSOLUTE_DEVIATION(dur)` |
| function | `MIN` | aggregate |  | call | supported | supported | supported |  |  | `FROM logs \| STATS m = MIN(dur)` |
| function | `PERCENTILE` | aggregate |  | call | supported | refused | refused |  |  | `FROM logs \| STATS p = PERCENTILE(dur, 95)` |
| function | `SAMPLE` | aggregate | yes | call | supported | refused | refused |  |  | `FROM logs \| STATS s = SAMPLE(message, 2)` |
| function | `ST_CENTROID_AGG` | aggregate |  | call | supported | refused | refused |  |  | `FROM logs \| STATS c = ST_CENTROID_AGG(location)` |
| function | `ST_EXTENT_AGG` | aggregate | yes | call | supported | refused | refused |  |  | `FROM logs \| STATS e = ST_EXTENT_AGG(location)` |
| function | `STD_DEV` | aggregate |  | call | supported | refused | refused |  |  | `FROM logs \| STATS s = STD_DEV(dur)` |
| function | `SUM` | aggregate |  | call | supported | supported | supported |  |  | `FROM logs \| STATS s = SUM(dur)` |
| function | `TOP` | aggregate |  | call | supported | refused | refused |  |  | `FROM logs \| STATS t = TOP(dur, 3, "desc")` |
| function | `VALUES` | aggregate | yes | call | supported | refused | refused |  |  | `FROM logs \| STATS v = VALUES(service)` |
| function | `WEIGHTED_AVG` | aggregate |  | call | supported | refused | refused |  |  | `FROM logs \| STATS w = WEIGHTED_AVG(dur, severity_num)` |
| function | `BUCKET` | grouping |  | call | supported | supported | supported |  |  | `FROM logs \| STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 minute)` |
| function | `CATEGORIZE` | grouping | yes | call | supported | refused | refused |  |  | `FROM logs \| STATS c = COUNT(*) BY k = CATEGORIZE(message)` |
| function | `TBUCKET` | grouping | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS r = MAX(RATE(requests)) BY TBUCKET(1 hour)` |
| function | `CASE` | scalar |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL level = CASE(severity_num > 12, "high", "low")` |
| function | `COALESCE` | scalar |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL s = COALESCE(service, "none")` |
| function | `GREATEST` | scalar |  | call | supported | refused | refused |  |  | `ROW g = GREATEST(1, 5, 3)` |
| function | `LEAST` | scalar |  | call | supported | refused | refused |  |  | `ROW l = LEAST(1, 5, 3)` |
| function | `TO_AGGREGATE_METRIC_DOUBLE` | conversion | yes | call | supported | refused | refused |  |  | `ROW a = TO_AGGREGATE_METRIC_DOUBLE(1.5)` |
| function | `TO_BOOLEAN` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_BOOLEAN("true")` |
| function | `TO_CARTESIANPOINT` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_CARTESIANPOINT("POINT(1 2)")` |
| function | `TO_CARTESIANSHAPE` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_CARTESIANSHAPE("POINT(1 2)")` |
| function | `TO_DATEPERIOD` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_DATEPERIOD("3 days")` |
| function | `TO_DATETIME` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_DATETIME("2024-01-01T00:00:00Z")` |
| function | `TO_DATE_NANOS` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_DATE_NANOS("2024-01-01T00:00:00.123456789Z")` |
| function | `TO_DEGREES` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_DEGREES(3.14)` |
| function | `TO_DOUBLE` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_DOUBLE("1.5")` |
| function | `TO_GEOPOINT` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_GEOPOINT("POINT(1 2)")` |
| function | `TO_GEOSHAPE` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_GEOSHAPE("POINT(1 2)")` |
| function | `TO_INTEGER` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_INTEGER("42")` |
| function | `TO_IP` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_IP("10.0.0.1")` |
| function | `TO_LONG` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_LONG("42")` |
| function | `TO_RADIANS` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_RADIANS(180.0)` |
| function | `TO_STRING` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_STRING(42)` |
| function | `TO_TIMEDURATION` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_TIMEDURATION("3 hours")` |
| function | `TO_UNSIGNED_LONG` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_UNSIGNED_LONG("42")` |
| function | `TO_VERSION` | conversion |  | call | supported | refused | refused |  |  | `ROW a = TO_VERSION("1.2.3")` |
| function | `DATE_DIFF` | date |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL d = DATE_DIFF("second", @timestamp, NOW())` |
| function | `DATE_EXTRACT` | date |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL h = DATE_EXTRACT("hour_of_day", @timestamp)` |
| function | `DATE_FORMAT` | date |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL d = DATE_FORMAT("yyyy-MM-dd", @timestamp)` |
| function | `DATE_PARSE` | date |  | call | supported | refused | refused |  |  | `ROW d = DATE_PARSE("yyyy-MM-dd", "2024-01-01")` |
| function | `DATE_TRUNC` | date |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL h = DATE_TRUNC(1 hour, @timestamp)` |
| function | `NOW` | date |  | call | supported | supported | supported | `now-type` |  | `ROW t = NOW()` |
| function | `CIDR_MATCH` | ip |  | call | supported | refused | refused |  |  | `ROW m = CIDR_MATCH(TO_IP("10.0.0.1"), "10.0.0.0/8")` |
| function | `IP_PREFIX` | ip |  | call | supported | refused | refused |  |  | `ROW p = IP_PREFIX(TO_IP("10.1.2.3"), 24, 0)` |
| function | `ABS` | math |  | call | supported | refused | refused |  |  | `ROW a = ABS(-1)` |
| function | `ACOS` | math |  | call | supported | refused | refused |  |  | `ROW a = ACOS(0.5)` |
| function | `ASIN` | math |  | call | supported | refused | refused |  |  | `ROW a = ASIN(0.5)` |
| function | `ATAN` | math |  | call | supported | refused | refused |  |  | `ROW a = ATAN(0.5)` |
| function | `ATAN2` | math |  | call | supported | refused | refused |  |  | `ROW a = ATAN2(1.0, 2.0)` |
| function | `CBRT` | math |  | call | supported | refused | refused |  |  | `ROW a = CBRT(27.0)` |
| function | `CEIL` | math |  | call | supported | refused | refused |  |  | `ROW a = CEIL(1.2)` |
| function | `COS` | math |  | call | supported | refused | refused |  |  | `ROW a = COS(0.5)` |
| function | `COSH` | math |  | call | supported | refused | refused |  |  | `ROW a = COSH(0.5)` |
| function | `E` | math |  | call | supported | refused | refused |  |  | `ROW a = E()` |
| function | `EXP` | math |  | call | supported | refused | refused |  |  | `ROW a = EXP(1.0)` |
| function | `FLOOR` | math |  | call | supported | refused | refused |  |  | `ROW a = FLOOR(1.8)` |
| function | `HYPOT` | math |  | call | supported | refused | refused |  |  | `ROW a = HYPOT(3.0, 4.0)` |
| function | `LOG` | math |  | call | supported | refused | refused |  |  | `ROW a = LOG(2.0, 8.0)` |
| function | `LOG10` | math |  | call | supported | refused | refused |  |  | `ROW a = LOG10(100.0)` |
| function | `PI` | math |  | call | supported | refused | refused |  |  | `ROW a = PI()` |
| function | `POW` | math |  | call | supported | refused | refused |  |  | `ROW a = POW(2.0, 10)` |
| function | `ROUND` | math |  | call | supported | refused | refused |  |  | `ROW a = ROUND(1.2345, 2)` |
| function | `ROUND_TO` | math |  | call | supported | refused | refused |  |  | `ROW a = ROUND_TO(7, 0, 5, 10)` |
| function | `SIGNUM` | math |  | call | supported | refused | refused |  |  | `ROW a = SIGNUM(-3.0)` |
| function | `SIN` | math |  | call | supported | refused | refused |  |  | `ROW a = SIN(0.5)` |
| function | `SINH` | math |  | call | supported | refused | refused |  |  | `ROW a = SINH(0.5)` |
| function | `SQRT` | math |  | call | supported | refused | refused |  |  | `ROW a = SQRT(16.0)` |
| function | `TAN` | math |  | call | supported | refused | refused |  |  | `ROW a = TAN(0.5)` |
| function | `TANH` | math |  | call | supported | refused | refused |  |  | `ROW a = TANH(0.5)` |
| function | `TAU` | math |  | call | supported | refused | refused |  |  | `ROW a = TAU()` |
| function | `KQL` | search | yes | call | supported | refused | refused |  | search index | `FROM logs \| WHERE KQL("message: timeout")` |
| function | `MATCH` | search | yes | call | supported | refused | refused |  | search index | `FROM logs \| WHERE MATCH(message, "timeout")` |
| function | `MATCH_PHRASE` | search | yes | call | supported | refused | refused |  | search index | `FROM logs \| WHERE MATCH_PHRASE(message, "connection timeout")` |
| function | `MULTI_MATCH` | search | yes | call | supported | refused | refused |  | search index | `FROM logs \| WHERE MULTI_MATCH("timeout", message, service)` |
| function | `QSTR` | search | yes | call | supported | refused | refused |  | search index | `FROM logs \| WHERE QSTR("message:timeout")` |
| function | `ST_CONTAINS` | spatial |  | call | supported | refused | refused |  |  | `ROW c = ST_CONTAINS(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"), TO_GEOPOINT("POINT(1 1)"))` |
| function | `ST_DISJOINT` | spatial |  | call | supported | refused | refused |  |  | `ROW d = ST_DISJOINT(TO_GEOPOINT("POINT(5 5)"), TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_DISTANCE` | spatial |  | call | supported | refused | refused |  |  | `ROW d = ST_DISTANCE(TO_GEOPOINT("POINT(0 0)"), TO_GEOPOINT("POINT(1 1)"))` |
| function | `ST_ENVELOPE` | spatial | yes | call | supported | refused | refused |  |  | `ROW e = ST_ENVELOPE(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_GEOHASH` | spatial | yes | call | supported | refused | refused |  |  | `ROW h = ST_GEOHASH(TO_GEOPOINT("POINT(1 2)"), 5)` |
| function | `ST_GEOHEX` | spatial | yes | call | supported | refused | refused |  |  | `ROW h = ST_GEOHEX(TO_GEOPOINT("POINT(1 2)"), 5)` |
| function | `ST_GEOTILE` | spatial | yes | call | supported | refused | refused |  |  | `ROW t = ST_GEOTILE(TO_GEOPOINT("POINT(1 2)"), 5)` |
| function | `ST_INTERSECTS` | spatial |  | call | supported | refused | refused |  |  | `ROW i = ST_INTERSECTS(TO_GEOPOINT("POINT(1 1)"), TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_WITHIN` | spatial |  | call | supported | refused | refused |  |  | `ROW w = ST_WITHIN(TO_GEOPOINT("POINT(1 1)"), TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_X` | spatial |  | call | supported | refused | refused |  |  | `ROW x = ST_X(TO_GEOPOINT("POINT(1 2)"))` |
| function | `ST_XMAX` | spatial | yes | call | supported | refused | refused |  |  | `ROW x = ST_XMAX(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_XMIN` | spatial | yes | call | supported | refused | refused |  |  | `ROW x = ST_XMIN(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_Y` | spatial |  | call | supported | refused | refused |  |  | `ROW y = ST_Y(TO_GEOPOINT("POINT(1 2)"))` |
| function | `ST_YMAX` | spatial | yes | call | supported | refused | refused |  |  | `ROW y = ST_YMAX(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `ST_YMIN` | spatial | yes | call | supported | refused | refused |  |  | `ROW y = ST_YMIN(TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))` |
| function | `BIT_LENGTH` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL n = BIT_LENGTH(message)` |
| function | `BYTE_LENGTH` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL n = BYTE_LENGTH(message)` |
| function | `CONCAT` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL s = CONCAT(message, "-", severity_text)` |
| function | `ENDS_WITH` | string |  | call | supported | refused | refused |  |  | `FROM logs \| WHERE ENDS_WITH(message, "a")` |
| function | `FROM_BASE64` | string |  | call | supported | refused | refused |  |  | `ROW s = FROM_BASE64("ZWxhc3RpYw==")` |
| function | `HASH` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL h = HASH("sha256", message)` |
| function | `LEFT` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL s = LEFT(message, 1)` |
| function | `LENGTH` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL n = LENGTH(message)` |
| function | `LOCATE` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL p = LOCATE(message, "a")` |
| function | `LTRIM` | string |  | call | supported | refused | refused |  |  | `ROW s = LTRIM("  a")` |
| function | `MD5` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL h = MD5(message)` |
| function | `REPEAT` | string |  | call | supported | refused | refused |  |  | `ROW s = REPEAT("ab", 3)` |
| function | `REPLACE` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL s = REPLACE(message, "a", "b")` |
| function | `REVERSE` | string |  | call | supported | refused | refused |  |  | `ROW s = REVERSE("abc")` |
| function | `RIGHT` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL s = RIGHT(message, 1)` |
| function | `RTRIM` | string |  | call | supported | refused | refused |  |  | `ROW s = RTRIM("a  ")` |
| function | `SHA1` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL h = SHA1(message)` |
| function | `SHA256` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL h = SHA256(message)` |
| function | `SPACE` | string |  | call | supported | refused | refused |  |  | `ROW s = SPACE(3)` |
| function | `SPLIT` | string |  | call | supported | refused | refused |  |  | `ROW s = SPLIT("a,b,c", ",")` |
| function | `STARTS_WITH` | string |  | call | supported | refused | refused |  |  | `FROM logs \| WHERE STARTS_WITH(message, "a")` |
| function | `SUBSTRING` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL s = SUBSTRING(message, 1, 1)` |
| function | `TO_BASE64` | string |  | call | supported | refused | refused |  |  | `ROW s = TO_BASE64("elastic")` |
| function | `TO_LOWER` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL s = TO_LOWER(severity_text)` |
| function | `TO_UPPER` | string |  | call | supported | refused | refused |  |  | `FROM logs \| EVAL s = TO_UPPER(message)` |
| function | `TRIM` | string |  | call | supported | refused | refused |  |  | `ROW s = TRIM("  a  ")` |
| function | `MV_APPEND` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_APPEND([1, 2], 3)` |
| function | `MV_AVG` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_AVG([1, 2, 3])` |
| function | `MV_CONCAT` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_CONCAT(["a", "b"], ",")` |
| function | `MV_COUNT` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_COUNT([1, 2, 3])` |
| function | `MV_DEDUPE` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_DEDUPE([1, 1, 2])` |
| function | `MV_FIRST` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_FIRST([1, 2, 3])` |
| function | `MV_LAST` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_LAST([1, 2, 3])` |
| function | `MV_MAX` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_MAX([1, 2, 3])` |
| function | `MV_MEDIAN` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_MEDIAN([1, 2, 3])` |
| function | `MV_MEDIAN_ABSOLUTE_DEVIATION` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_MEDIAN_ABSOLUTE_DEVIATION([1, 2, 5])` |
| function | `MV_MIN` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_MIN([1, 2, 3])` |
| function | `MV_PERCENTILE` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_PERCENTILE([1, 2, 3], 50)` |
| function | `MV_PSERIES_WEIGHTED_SUM` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_PSERIES_WEIGHTED_SUM([1.0, 2.0, 3.0], 1.5)` |
| function | `MV_SLICE` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_SLICE([1, 2, 3], 0, 1)` |
| function | `MV_SORT` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_SORT([3, 1, 2], "desc")` |
| function | `MV_SUM` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_SUM([1, 2, 3])` |
| function | `MV_ZIP` | multivalue |  | call | supported | refused | refused |  |  | `ROW m = MV_ZIP(["a", "b"], ["x", "y"], "-")` |
| function | `RATE` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS r = SUM(RATE(requests)) BY host` |
| function | `IRATE` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS r = SUM(IRATE(requests)) BY host` |
| function | `DELTA` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS d = SUM(DELTA(temperature)) BY host` |
| function | `IDELTA` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS d = SUM(IDELTA(temperature)) BY host` |
| function | `INCREASE` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS i = SUM(INCREASE(requests)) BY host` |
| function | `AVG_OVER_TIME` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS a = MAX(AVG_OVER_TIME(temperature)) BY host` |
| function | `COUNT_OVER_TIME` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS c = SUM(COUNT_OVER_TIME(temperature)) BY host` |
| function | `COUNT_DISTINCT_OVER_TIME` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS c = SUM(COUNT_DISTINCT_OVER_TIME(temperature)) BY host` |
| function | `FIRST_OVER_TIME` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS f = MAX(FIRST_OVER_TIME(temperature)) BY host` |
| function | `LAST_OVER_TIME` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS l = MAX(LAST_OVER_TIME(temperature)) BY host` |
| function | `MAX_OVER_TIME` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS m = MAX(MAX_OVER_TIME(temperature)) BY host` |
| function | `MIN_OVER_TIME` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS m = MIN(MIN_OVER_TIME(temperature)) BY host` |
| function | `SUM_OVER_TIME` | time_series | yes | call | supported | refused | refused |  | time series data stream | `TS metrics \| STATS s = SUM(SUM_OVER_TIME(temperature)) BY host` |
| alias | `INLINESTATS` (of `INLINE STATS`) |  | yes | own | supported | refused | refused |  |  | `FROM logs \| INLINESTATS m = MAX(dur) BY service` |
| alias | `TO_BOOL` (of `TO_BOOLEAN`) |  |  | call | supported | refused | refused |  |  | `ROW a = TO_BOOL("true")` |
| alias | `TO_DT` (of `TO_DATETIME`) |  |  | call | supported | refused | refused |  |  | `ROW a = TO_DT("2024-01-01T00:00:00Z")` |
| alias | `TO_DBL` (of `TO_DOUBLE`) |  |  | call | supported | refused | refused |  |  | `ROW a = TO_DBL("1.5")` |
| alias | `TO_INT` (of `TO_INTEGER`) |  |  | call | supported | refused | refused |  |  | `ROW a = TO_INT("42")` |
| alias | `TO_STR` (of `TO_STRING`) |  |  | call | supported | refused | refused |  |  | `ROW a = TO_STR(42)` |
| alias | `TO_UL` (of `TO_UNSIGNED_LONG`) |  |  | call | supported | refused | refused |  |  | `ROW a = TO_UL("42")` |
| alias | `TO_ULONG` (of `TO_UNSIGNED_LONG`) |  |  | call | supported | refused | refused |  |  | `ROW a = TO_ULONG("42")` |
| alias | `TO_VER` (of `TO_VERSION`) |  |  | call | supported | refused | refused |  |  | `ROW a = TO_VER("1.2.3")` |
| operator | `OR` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE dur > 10 OR dur < 5` |
| operator | `AND` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE dur > 1 AND dur < 20` |
| operator | `NOT` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE NOT dur > 10` |
| operator | `==` |  |  | own | supported | supported | supported |  |  | `FROM logs \| WHERE message == "b" \| KEEP message, dur` |
| operator | `!=` |  |  | own | supported | supported | supported |  |  | `FROM logs \| WHERE message != "a" \| KEEP message` |
| operator | `<` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE dur < 10` |
| operator | `<=` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE dur <= 10` |
| operator | `>` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE dur > 10` |
| operator | `>=` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE dur >= 10` |
| operator | `+` |  |  | own | supported | supported | supported | `integer-literal` |  | `ROW a = 1 + 2` |
| operator | `-` |  |  | own | supported | supported | supported | `integer-literal` |  | `ROW a = 5 - 2` |
| operator | `*` |  |  | own | supported | supported | supported | `integer-literal` |  | `ROW a = 2 * 3` |
| operator | `/` |  |  | own | supported | supported | supported | `integer-literal` |  | `ROW a = 7 / 2` |
| operator | `%` |  |  | own | supported | supported | supported | `integer-literal` |  | `ROW a = 7 % 2` |
| operator | `unary -` |  |  | own | supported | supported | supported | `integer-literal` |  | `ROW a = -(1 + 2)` |
| operator | `IS NULL` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE dur IS NULL` |
| operator | `IS NOT NULL` |  |  | own | supported | supported | supported | `from-columns` |  | `FROM logs \| WHERE dur IS NOT NULL` |
| operator | `IN` |  |  | own | supported | supported | supported |  |  | `FROM logs \| WHERE message IN ("a", "b") \| KEEP message` |
| operator | `NOT IN` |  |  | own | supported | supported | supported |  |  | `FROM logs \| WHERE message NOT IN ("a") \| KEEP message` |
| operator | `IN subquery` |  | yes | own | supported | refused | refused |  |  | `FROM logs \| WHERE service IN (FROM logs \| WHERE dur > 5 \| KEEP service)` |
| operator | `LIKE` |  |  | own | supported | refused | refused |  |  | `FROM logs \| WHERE message LIKE "a*"` |
| operator | `NOT LIKE` |  |  | own | supported | refused | refused |  |  | `FROM logs \| WHERE message NOT LIKE "a*"` |
| operator | `RLIKE` |  |  | own | supported | refused | refused |  |  | `FROM logs \| WHERE message RLIKE "a.*"` |
| operator | `NOT RLIKE` |  |  | own | supported | refused | refused |  |  | `FROM logs \| WHERE message NOT RLIKE "a.*"` |
| operator | `:` |  | yes | own | supported | refused | refused |  | search index | `FROM logs \| WHERE message : "timeout"` |
| operator | `::` |  |  | own | supported | refused | refused |  |  | `ROW a = "1"::long` |
| type | `keyword` |  |  | own | supported | supported | supported |  |  | `ROW a = "x"` |
| type | `text` |  |  | cast | supported | refused | refused |  |  | `ROW a = "x"::text` |
| type | `long` |  |  | field | supported | supported | supported |  |  | `FROM logs \| KEEP severity_num` |
| type | `integer` |  |  | cast | supported | refused | refused |  |  | `ROW a = "1"::integer` |
| type | `unsigned_long` |  |  | cast | supported | refused | refused |  |  | `ROW a = "1"::unsigned_long` |
| type | `double` |  |  | own | supported | supported | supported |  |  | `ROW a = 1.5` |
| type | `boolean` |  |  | own | supported | supported | supported |  |  | `ROW a = true` |
| type | `date` |  |  | cast | supported | refused | refused |  |  | `ROW a = "2024-01-01T00:00:00Z"::datetime` |
| type | `date_nanos` |  |  | field | supported | supported | supported |  |  | `FROM logs \| KEEP @timestamp` |
| type | `null` |  |  | own | supported | supported | supported |  |  | `ROW a = null` |
| type | `ip` |  |  | cast | supported | refused | refused |  |  | `ROW a = "10.0.0.1"::ip` |
| type | `version` |  |  | cast | supported | refused | refused |  |  | `ROW a = "1.2.3"::version` |
| type | `geo_point` |  |  | cast | supported | refused | refused |  |  | `ROW a = "POINT(1 2)"::geo_point` |
| type | `geo_shape` |  |  | cast | supported | refused | refused |  |  | `ROW a = "POINT(1 2)"::geo_shape` |
| type | `cartesian_point` |  |  | cast | supported | refused | refused |  |  | `ROW a = "POINT(1 2)"::cartesian_point` |
| type | `cartesian_shape` |  |  | cast | supported | refused | refused |  |  | `ROW a = "POINT(1 2)"::cartesian_shape` |
| type | `time_duration` |  |  | own | supported | supported | supported |  |  | `FROM logs \| EVAL t = @timestamp + 1 hour \| KEEP t` |
| type | `date_period` |  |  | own | supported | refused | refused |  |  | `ROW a = 1 month` |
| type | `counter_long` |  | yes | field | supported | refused | refused |  | time series data stream | `TS metrics \| KEEP requests` |
| type | `counter_integer` |  | yes | field | supported | refused | refused |  | time series data stream | `TS metrics \| KEEP connections` |
| type | `counter_double` |  | yes | field | supported | refused | refused |  | time series data stream | `TS metrics \| KEEP bytes` |
| type | `aggregate_metric_double` |  | yes | call | supported | refused | refused |  |  | `ROW a = TO_AGGREGATE_METRIC_DOUBLE(1.5)` |
| parameter | `?` |  |  | own | supported | refused | refused |  |  | `ROW a = ?` |
| parameter | `?n` |  |  | own | supported | refused | refused |  |  | `ROW a = ?1` |
| parameter | `?name` |  |  | own | supported | refused | refused |  |  | `ROW a = ?limit` |
| parameter | `??` |  | yes | own | supported | refused | refused |  |  | `FROM logs \| KEEP ??` |
| parameter | `??n` |  | yes | own | supported | refused | refused |  |  | `FROM logs \| KEEP ??1` |
| parameter | `??name` |  | yes | own | supported | refused | refused |  |  | `FROM logs \| KEEP ??field` |
