# MetricsBench comparator deployments

Checked-in, digest-pinned deployments of the cross-engine comparators for the
MetricsBench metrics benchmark (ADR-0927). These are the systems the
MetricsBench harness (`crates/ravel-bench`) measures Ravel against in the
**portable lane**: Prometheus Remote Write 1.0 for ingest, the Prometheus HTTP
query API for reads. Every system here receives the same logical samples and the
same queries; no Ravel-only instrumentation is ever folded into a cross-engine
score (ADR-0927 decision 1).

Ravel, the system under test, is not in `docker-compose.yml`. It runs from the
overlay `ravel.yml` in this directory, on the same s3s-fs and at the same
envelope as the comparators (see "Ravel in the portable lane" below).
`deploy/docker-compose/ravel.yml` is the product quickstart, not a benchmark
deployment, and MetricsBench does not use it.

## The comparators

| Service | What it is | Durability / storage | Remote Write 1.0 endpoint | PromQL query endpoint |
|---|---|---|---|---|
| `prometheus` | Reference PromQL engine, pinned to v3.13.1 (the same version ADR-0927 uses as the correctness oracle) | **Local-disk TSDB.** Ack = in-memory head + on-disk WAL. Not object storage. | `http://127.0.0.1:9090/api/v1/write` | `http://127.0.0.1:9090/api/v1/query` |
| `victoriametrics` | Single-node VictoriaMetrics, accepts Prometheus remote write natively | **Local-disk storage.** Ack = local storage write. Not object storage. | `http://127.0.0.1:8428/api/v1/write` | `http://127.0.0.1:8428/prometheus/api/v1/query` |
| `mimir` | Grafana Mimir 3.2.3 monolithic (`-target=all`); the ADR-0927 object-storage-native PromQL system | **Object storage (S3).** Ack = local WAL; blocks ship to S3 (s3s-fs here) later. See caveat below. | `http://127.0.0.1:9009/api/v1/push` | `http://127.0.0.1:9009/prometheus/api/v1/query` |

Supporting services: `s3s` (s3s-fs, the S3 backend for Mimir and Ravel, see
"The object store" below) and `createbuckets` (a one-shot AWS CLI container
that creates Mimir's buckets and Ravel's `ravel-metricsbench` bucket, then
exits).

### The object store

The stack's S3 endpoint is s3s-fs, the file-system backend of the s3s project,
at the `s3s-fs-0.17.0-ravel.1` release of the `NOFireAI/s3s` fork (ADR-0927
amendment 2026-10-29). It replaced RustFS, which sat at its one-CPU cap while
ravel-server idled, so the store bounded the Ravel row. Figures measured on
RustFS are not compared with figures measured on s3s-fs.

No image carries the binary. `run.sh`'s `fetch-s3s-fs` step downloads the
release asset for the Docker engine's arch (`s3s-fs-linux-x86_64` or
`s3s-fs-linux-aarch64`) into a cache outside the output directory, refuses it
with exit 65 unless its sha256 equals the constant `run.sh` carries, and
exports its path as `S3S_FS_BIN`; `docker-compose.yml` bind-mounts that path
read-only into the `s3s` service and refuses to start without it. The binary
runs inside the engine's Linux containers, so the asset follows the engine's
arch, `docker version --format '{{.Server.Arch}}'`: `amd64` or `x86_64` picks
`x86_64`, `arm64` or `aarch64` picks `aarch64` (cases n7 and n8), so a macOS
host whose `uname -m` says `arm64` fetches the `aarch64` asset. Only when the
engine does not answer does `uname -m` stand in (case n5); `object_store` in
`stamps.json` records the engine arch and which of the two it came from. The
sha256 is computed with `sha256sum`, else `shasum -a 256` (macOS; cases n9
and n10), and a host with neither is refused with exit 65 (case n11). The
pinned values:

| Asset arch | sha256 |
|---|---|
| `x86_64` | `0ac5480b51389c1913f9645d0e1ff2ff1dfdd3a44bb396a7cb74c5e91176c01f` |
| `aarch64` | `6d5d47a9823e62a36a7ce4b702886ba0e474557084dae877998015ffdc6717ad` |

The binary is linked against glibc and runs on the same
`gcr.io/distroless/cc-debian12:nonroot` digest the root `Dockerfile` builds
on, as root (`user: "0:0"`) so it can write the root-owned named volume
`s3s-data`. Its envelope is the one RustFS had, `cpus: "1.0"` and
`memory: 2g` (`docker-compose.yml`, the `s3s` block). It logs to stdout, so
its log lands in `compose.log` with every other service's.

## Ravel in the portable lane

Ravel runs as one more comparator, from the overlay `ravel.yml` (ADR-0927
amendment 2026-10-26 A). The image under test is a required parameter with no
default; compose refuses to start the set without it (`required variable
RAVEL_IMAGE is missing a value`):

```sh
RAVEL_IMAGE=ghcr.io/nofireai/ravel-server:<tag>@sha256:<digest> \
  docker compose -f deploy/metricsbench/docker-compose.yml \
    -f deploy/metricsbench/ravel.yml up -d
```

The image must carry `--max-series` and `--max-samples` (#3306), which no
release up to 0.27.0 does: ravel-server refuses an unknown flag at startup.
Use a release after 0.27.0, or a `publish-images` manual build of a main
commit that contains #3306 (tagged `manual-<short sha>`).

The overlay adds two services. `ravel-qualify` runs `ravel-cli --store s3
store qualify` once, after `createbuckets`, because `ravel-server` refuses to
start on an S3 store that has not been qualified. `ravel-server` then starts
in `--mode all` (gateway, query and maintenance in one process, the
counterpart of Mimir's `-target=all`).

| Service | What it is | Durability / storage | Remote Write 1.0 endpoint | PromQL query endpoint |
|---|---|---|---|---|
| `ravel-server` | Ravel, `--mode all`, 4 ingest shards, one tenant `metricsbench` | **Object storage (S3), strict Remote Write.** A 2xx means the data object and the commit record are durable in object storage before the ack (ADR-0927 decision 3). No volume: no state lives on local disk. s3s-fs here, so the ADR-0927 decision 10 caveat above applies. | `http://127.0.0.1:4318/api/v1/write` | `http://127.0.0.1:4318/api/v1/query` |

Every write and query carries `Authorization: Bearer metricsbench-token`
(`--tenant-token metricsbench-token=metricsbench`). `/api/v1/status/buildinfo`
answers without it.

**Envelope.** The same `cpus: "2.0"` and `memory: 4g` limits as `prometheus`,
`victoriametrics` and `mimir`. Ravel's memory split is written as flags, not
left to the server's derivation from the container, and the server logs each
value with `source="flag"` at startup:

| Quantity | Bytes | How |
|---|---|---|
| Envelope | 4,294,967,296 | `memory: 4g` |
| Reserve outside the budget | 1,073,741,824 | max(min(2 GiB, envelope / 4), 256 MiB baseline + 512 MiB ingest buffer) |
| `--memory-budget-bytes` | 3,221,225,472 | envelope minus reserve |
| `--cache-max-bytes` (fetcher cache) | 805,306,368 | 25% of the budget, the derived share for a non-loopback S3 endpoint |
| `--catalog-cache-max-bytes` | 161,061,273 | 5% of the budget, the derived share |
| `--column-stats-cache-max-bytes` | 64,424,509 | 2% of the budget, the derived share (ADR-1413 decision 5) |
| Budget left for query working set | 2,190,433,322 | budget minus the three caches (1,030,792,150) |
| `--max-ingest-buffer-bytes` | 536,870,912 | the 512 MiB default, stated; held in the reserve |

**Disclosed settings** (each commented next to its flag in `ravel.yml`):

- `--max-ingest-lag 45d` replaces the 2h default, so the 30 d history profile
  is admitted when replayed. The one flag sets both the admission bound and the
  catalog listing window. A sample older than the window is dropped per sample
  while the request still answers 2xx (Remote Write 1.0 has no partial-success
  field); the local probe below shows the drop as an empty query result.
- `--max-flush-delay 500ms`, `--max-flush-delay-idle 40s`,
  `--min-flush-bytes 262144` replace the shipped 2 s active-tenant flush age
  (ADR-0927 flush-setting amendment, 2026-10-29). A Remote Write 2xx waits
  for the flush that stores its points, so at 2 s the replay measures the
  flush clock: on `cardinality` the default ingested 42,142 points per second
  and this setting 114,356 (#2821, E2). The three flags move as a set; the
  idle delay and the byte threshold keep their shipped values. Figures
  measured under the 2 s default are not compared with figures measured
  under this setting.
- `--audit-mode off`, the default, stated: no query-audit record is written,
  so no audit write sits on the query path.
- `--tenant-hash-unkeyed`: a fresh bucket refuses to start without a
  tenant-hash choice. It changes object key names only.
- The query and admission limits listed under "Frozen limits" below. The
  admission limits and the scanned-byte budget live in
  `config/ravel-limits.toml`, passed with `--limits-file`.

**The in-process lane is diagnostic, not portable.** The `metricsbench_ingest`
binary in `crates/ravel-bench` replays through an in-process `IngestRouter`
and queries an in-process `QueryEngine`, without HTTP, so it skips the
event-time admission `ravel-server` applies on `/api/v1/write`. It is the
diagnostic lane, and its report row says `endpoint: in-process` so it is not
read as the portable row (ADR-0927 amendment 2026-10-26 A). Ravel's portable
row is this service, driven through the same Remote Write and query endpoints
as every other system here.

**Compose stack (2026-10-26, ravel-server 0.27.0, darwin/arm64 Docker).**
`RAVEL_IMAGE=ghcr.io/nofireai/ravel-server:0.27.0 docker compose -f
docker-compose.yml -f ravel.yml up -d rustfs createbuckets ravel-qualify
ravel-server` brought the overlay up on the stack's object store, which was
RustFS at the time (the service was named `rustfs`). Results:

| Step or request | Result |
|---|---|
| `createbuckets`, `ravel-qualify` | both exited 0 |
| `ravel-server` limits (`docker inspect`) | 4,294,967,296 bytes memory, 2.0 CPUs |
| `GET /api/v1/status/buildinfo` | 200, `"version":"0.27.0","revision":""` |
| Remote Write 1.0, one sample at now-60 s, bearer token | 204 with `x-ravel-commit-token`; queried back with that token as `min_commit_token`: `42` |
| Remote Write 1.0, one sample at now-30 d, bearer token | 204 with `x-ravel-commit-token`; queried back: `7` |
| Remote Write 1.0 without the bearer token | 401 |

That run predates the column-statistics cache pin; the cache's derived value
is the one now pinned.

**Local probe (2026-10-26, ravel-server 0.26.0, linux/arm64).** The source of
the Ravel budget-refusal signatures below. The 0.26.0 binary, extracted from
the image, was run with the flags `ravel.yml` then carried, which did not yet
include `--column-stats-cache-max-bytes` (0.26.0 has no such flag), and
`--store memory` in place of `--store s3`, on a host with no docker daemon.
Both observed refusals are compiled-in query caps, which no cache flag
changes. Results:

| Request | HTTP | Result |
|---|---|---|
| `GET /api/v1/status/buildinfo`, no bearer | 200 | `"version":"0.26.0","revision":""` |
| Remote Write, one sample at now-60 s | 204 | queried back: `42` |
| Remote Write, one sample at now-30 d | 204 | queried back: `7` |
| Remote Write, one sample at now-50 d (outside 45d) | 204 | queried back: empty result (dropped per sample) |
| `count()` over 1,000 series | 200 | `1000` |
| `count()` over 10,001 series | 422 | max-series refusal (see "Budget-refusal signatures") |
| `query_range`, 12,001 points | 422 | range-points refusal (see "Budget-refusal signatures") |

`docker compose -f docker-compose.yml -f ravel.yml config` exits 0 with
`RAVEL_IMAGE` set and 1 with it unset or empty.

### Storage architecture is not interchangeable

Prometheus and VictoriaMetrics persist to **local disk**. Mimir is
**object-storage-native** and writes its durable blocks to S3 — but in this stack
S3 is provided by **s3s-fs**, which is local-disk-backed and charges no
per-request fees. Per ADR-0927 decision 10 (ADR-0075 decision 3), an s3s-fs result
is valid for correctness and CI only and is **never a publishable performance or
cost result**; a real-S3 substrate is required for that, exactly as Ravel's own
publishable numbers require real S3. Each service block in
`docker-compose.yml` states its own durability next to its configuration so this
distinction is readable without this README or the ADR open.

Every backend-specific behaviour that transforms, delays, or ages the stored data
— Prometheus' 2h head-block compaction and 45d retention, VictoriaMetrics'
background merges and disabled deduplication, Mimir's delayed block upload to S3
and its compactor — is disclosed in a comment next to the setting that causes it,
in `docker-compose.yml`, `config/prometheus.yml` or `config/mimir.yaml`. No
behaviour-changing tuning lives outside these files.

## Frozen limits

ADR-0927's 2026-10-26 amendment (decisions B and F) requires every ingest
window and query limit the MetricsBench corpus can reach to be set explicitly
and disclosed, so the comparators store the same logical samples and a limit
that fires is a disclosed refusal with a known signature, not an undefault
nobody chose. Each value below is written in the comparator's configuration,
with a comment naming the default it replaces, even where it equals that
default, so `capture-effective-config.sh` records it (the Prometheus
out-of-order window has no flag, so the capture reads it from
`/api/v1/status/config` into `config_settings`). Defaults are quoted from
each pinned version's own help (`prometheus --help`, `victoria-metrics -help`,
`mimir -help-all`) unless the source column says otherwise.

The sizing figures come from `benchmarks/metrics/workload.json` and
`benchmarks/metrics/metrics.corpus.json`: the history profile is 10,000 series
at a 15 s scrape over 30 d, the cardinality profile 1,000,000 series, the
longest range window in the corpus is `[7d]` (`mb_long_range_7d_avg`, history
only), and the largest declared range grid is history's 24 h at a 5 m step.
ADR-0927's 2026-10-26 amendment B specifies the generator's out-of-order
anomaly as a shift of one and a half scrape intervals, 22.5 s at 15 s, off the
scrape grid.

| Comparator | Setting | Value | Default replaced | Sized by | Source |
|---|---|---|---|---|---|
| Prometheus | `storage.tsdb.out_of_order_time_window` (config/prometheus.yml) | `1h` | `0` | Above the 22.5 s shift; covers a bulk replay arriving slightly out of order | No flag in `--help`; default read from the 3.13.1 binary's `/api/v1/status/config` (`outofordertimewindow: 0`) |
| Prometheus | `--query.max-samples` | `500000000` | `50000000` | Peak samples held at once, not samples read; a range function runs one series at a time. Largest peak: `mb_full_range_avg_over_time`'s intermediate matrix on cardinality, the `avg_over_time` argument over 400,000 series x 121 points = 48,400,000 before `avg()` collapses it (predicted), 3.2% under the default. `mb_long_range_7d_avg` selects 1,000 series x 40,320 samples and peaks near one series' window (predicted; falsification run pre-registered on #2821). Raised because if that run shows the flag counts samples read, the largest statement reads 400,000 x 240 = 96,000,000, above the default | `--query.max-samples=50000000  Maximum number of samples a single query can load into memory` |
| Prometheus | `--query.timeout` | `120s` | `2m` (unchanged) | Written so the capture shows it | `--query.timeout=2m  Maximum time a query may take before being aborted` |
| Prometheus | `--query.lookback-delta` | `5m` | `5m` (unchanged) | Stated, not changed | `--query.lookback-delta=5m  The maximum lookback duration for retrieving metrics during expression evaluations` |
| VictoriaMetrics | out-of-order window | none to set | not applicable | Accepts any order inside `-retentionPeriod` | v1.115.0 docs, `docs/victoriametrics/README.md`, "Backfilling": "VictoriaMetrics accepts historical data in arbitrary order of time via any supported ingestion method" |
| VictoriaMetrics | `-search.latencyOffset` | `0` | `30s` | An instant query at the newest sample sees that sample | `-search.latencyOffset duration  The time when data points become visible in query results after the collection ... (default 30s)` |
| VictoriaMetrics | `-search.maxPointsPerTimeseries` | `20000` | `30000` | History grid 24 h / 5 m + 1 = 289 points | `-search.maxPointsPerTimeseries int  The maximum points per a single timeseries returned from /api/v1/query_range ... (default 30000)` |
| VictoriaMetrics | `-search.maxSamplesPerQuery` | `1000000000` | `1000000000` (unchanged) | Raw samples read across all series, unlike Prometheus' peak count. Largest: `mb_full_range_avg_over_time` on cardinality, 400,000 series x 240 samples (the 30 m grid plus the 30 m window, 1 h at 15 s) = 96,000,000 | `-search.maxSamplesPerQuery int  The maximum number of raw samples a single query can process across all time series ... (default 1000000000)` |
| VictoriaMetrics | `-search.maxQueryDuration` | `120s` | `30s` | Equal to Prometheus' `--query.timeout` | `-search.maxQueryDuration duration  The maximum duration for query execution ... (default 30s)` |
| VictoriaMetrics | `-search.maxUniqueTimeseries` | `1000000` | derived from CPU and memory | Cardinality profile, 1,000,000 series | `When set to zero, the limit is automatically calculated based on -search.maxConcurrentRequests (inversely proportional) and memory available to the process (proportional).` |
| VictoriaMetrics | `-search.disableCache` | `true` | `false` | Historical replay; a production_cache regime re-enables it per run | `-search.disableCache  Whether to disable response caching. This may be useful when ingesting historical data.` |
| Mimir | `limits.out_of_order_time_window` | `1h` | `0s` | Same as Prometheus | `-ingester.out-of-order-time-window duration  Non-zero value enables out-of-order support`; `0s` read from the 3.2.3 binary's `/config` |
| Mimir | `limits.past_grace_period` | `0s` | `0s` (unchanged) | A 30 d replay is not refused for its age | `-validation.past-grace-period duration ... 0 to disable.` |
| Mimir | `querier.query_store_after` | `0s` | `12h` | Replayed timestamps follow the anchor, not the wall clock; every query reaches the store-gateway | `-querier.query-store-after duration ... 0 means all queries are sent to store. ... (default 12h0m0s)` |
| Mimir | `limits.query_ingesters_within` | `0s` | `13h` | Every query reaches the ingester, so unshipped samples stay visible | `-querier.query-ingesters-within duration ... 0 means all queries are sent to ingester. (default 13h)` |
| Mimir | `blocks_storage.bucket_store.sync_interval` | `15m` | `15m` (unchanged) | Readiness wait, not a query budget | `-blocks-storage.bucket-store.sync-interval duration  How frequently to scan the bucket, or to refresh the bucket index ... (default 15m0s)` |
| Mimir | `compactor.cleanup_interval` | `15m` | `15m` (unchanged) | Readiness wait, not a query budget | `-compactor.cleanup-interval duration  How frequently the compactor should run blocks cleanup and maintenance, as well as update the bucket index. (default 15m0s)` |
| Mimir | `limits.max_fetched_chunks_per_query` | `5000000` | `2000000` | Per statement, counting a shipped block twice (the querier reads the overlap from the ingester and the store-gateway). Largest read: `mb_full_range_avg_over_time` on cardinality, 96,000,000 samples / 120 = 800,000 chunks, up to 1,200,000 with one 2 h block cut per series, 2,400,000 counted twice. Largest history read: `mb_long_range_7d_avg`, 1,000 x 40,320 / 120 = 336,000. Out-of-order chunks add at most 450,000 (one in 800 of cardinality's 360,000,000 samples) | `-querier.max-fetched-chunks-per-query int  Maximum number of chunks that can be fetched in a single query ... (default 2000000)`; 120 per chunk observed (9,600 samples in 80 chunks per 2 h block of 20 series) |
| Mimir | `querier.max_samples` | `500000000` | `50000000` | As for Prometheus (peak samples held); does not fire under the default query engine (see below) | `-querier.max-samples int  Maximum number of samples a single query can load into memory. ... (default 50000000)` |
| Mimir | `limits.max_estimated_memory_consumption_per_query` | `2147483648` (2 GiB) | `0` (disabled) | Half the 4g envelope, which this one process shares with the ingester and the store-gateway; sized to the envelope, not the corpus (see the note below) | `-querier.max-estimated-memory-consumption-per-query uint  [experimental] ... only enforced when Mimir's query engine is in use. ... 0 to disable.` |
| Mimir | `querier.timeout` | `2m` | `2m` (unchanged) | Equal to Prometheus' `--query.timeout` | `-querier.timeout duration  The timeout for a query. ... (default 2m0s)` |
| Mimir | `limits.max_total_query_length` | `31d` | `0s` (no limit) | History's 30 d span plus 1 d; `max_query_length` does not exist in 3.2.3 | `-query-frontend.max-total-query-length duration  Limit the total query time range (end - start time).` |
| Mimir | `limits.max_partial_query_length` | `0s` | `0s` (unchanged) | No separate bound on split sub-queries | `-querier.max-partial-query-length duration  Limit the time range for partial queries at the querier level.` |
| Mimir | `limits.max_query_lookback` | `45d` | `0s` (disabled) | The 45 d retention; a silent clamp, not a refusal | `-querier.max-query-lookback duration ... the request will not fail but will be manipulated to only query data within the allowed time range. 0 to disable.` |
| Mimir | `frontend.query_result_response_format` | `protobuf` | `protobuf` (unchanged) | Native histograms under the default query sharding | `-ingester.native-histograms-ingestion-enabled ... To query native histograms with query-sharding enabled make sure to set -query-frontend.query-result-response-format to 'protobuf'.` |
| Mimir | `frontend.cache_results` | `false` | `false` (unchanged) | Parity with `-search.disableCache=true` | `-query-frontend.cache-results  Cache query results.` |
| Ravel | `--max-ingest-lag` | `45d` | `2h` | Raised: history's 30 d span plus replay time, equal to the 45 d retention on Prometheus and VictoriaMetrics | `--max-ingest-lag <DURATION> ... One flag drives BOTH the OTLP admission bound ... AND the catalog listing window ... (default: 2h)` |
| Ravel | future-skew bound | `10m` | `10m` (unchanged, no flag) | Kept: a replay of past event time does not reach it | `crates/ravel-otlp/src/limits.rs`, `max_future_skew_ns: 10 * MINUTE_NANOS` |
| Ravel | `max_active_series` (config/ravel-limits.toml) | `"unlimited"` | `200000` | Raised: the cardinality profile writes 1,000,000 series; parity with Mimir's `max_global_series_per_user: 0` | `docs/guides/admission-limits.md`: `Active-series cap (metrics) \| 200,000 \| max_active_series` |
| Ravel | `ingest_bytes_per_sec` (config/ravel-limits.toml) | `"unlimited"` | `33554432` (32 MiB/s, 64 MiB burst) | Raised: the replay sends as fast as the receiver acks, so a byte-rate cap measures the cap | `docs/guides/admission-limits.md`: `Ingest byte rate / burst \| 32 MiB/s / 64 MiB` |
| Ravel | `series_creation_rate_per_sec` (config/ravel-limits.toml) | `"unlimited"` | `10000` (100,000 burst) | Raised: the cardinality profile's first timestamp creates 1,000,000 series, ten times the burst | `docs/guides/admission-limits.md`: `Series-creation rate / burst \| 10,000/s / 100,000` |
| Ravel | `max_active_streams` (config/ravel-limits.toml) | `200000` | `200000` (unchanged) | Kept: the lane writes no logs | `docs/guides/admission-limits.md`: `Active-stream cap (logs) \| 200,000` |
| Ravel | `max_bytes_scanned` (config/ravel-limits.toml) | `"unlimited"` | `"unlimited"` (unchanged) | Kept: no comparator has a scanned-byte budget | `docs/guides/admission-limits.md`: `Query bytes-scanned budget \| unlimited` |
| Ravel | `--gc-max-query-duration` (query deadline) | `120s` | `11m` (derived) | Lowered to Prometheus' `--query.timeout`; a hit is refused(budget) | `--gc-max-query-duration <DURATION>  This process's query-engine deadline ... Default when unset: derived (host-independent): 11m` |
| Ravel | `--max-segments` | `1000000` | `1000000` (derived, unchanged) | Kept: 4 shards at this row's 500 ms flush age write at most 8 objects per second of ingest, so 1,000,000 needs 125,000 s (34.7 h) of replay before compaction; a hit is refused(budget) | `--max-segments <N> ... Default when unset: derived (host-independent): 1,000,000` |
| Ravel | `--max-s3-requests` | `1358600` | `1358600` (derived at this row's 500 ms flush delay, unchanged; `343400` at the shipped 2 s) | Kept: at one GET per object and 8 objects per second, a read over the whole uncompacted replay needs 169,825 s (47.2 h) of ingest to reach it, and `--max-segments` refuses first, at 125,000 s. Above about 1.36 requests per object (1,358,600 / 1,000,000), as range reads give, this cap refuses first instead; a hit is refused(budget). `services/ravel-server/tests/metricsbench_request_budget.rs` fails if this value, `ravel.yml`'s and the server's derivation from `ravel.yml`'s command disagree | `--max-s3-requests <COUNT> ... Omitted (the default): the cap is DERIVED from` the shard count, `the ingest flush cadence, and the seal margin`; 1,358,600 at 4 shards and 500 ms is pinned in `crates/ravel-query/src/segment_admission.rs`, `derived_budget_covers_healthy_tail_plus_stall_alert_window` |
| Ravel | `--max-series` | `1000000` | `10000` | Raised (#3306): the cardinality profile's 1,000,000 active series, equal to VictoriaMetrics' `-search.maxUniqueTimeseries`; the widest corpus union is 700,000 series. At the default, 15 of the 21 cardinality entries and 12 of the 22 churn entries would be refused | `--max-series <N>  Cap on the distinct series a single PromQL query may materialize ... Default when unset: ravel_query::DEFAULT_MAX_SERIES (10,000)` |
| Ravel | `--max-samples` | `500000000` | `10000000` | Raised (#3306) to Prometheus' and Mimir's value. Ravel counts total materialized samples after dedup, where Prometheus counts peak resident samples, so the equal numbers are not equal semantics; the comparator counting the same quantity is VictoriaMetrics' `-search.maxSamplesPerQuery`, at twice this value. No corpus entry comes near either: the largest read is 96,000,000. The bytes behind them are bounded by `--memory-budget-bytes`, which refuses an oversized materialization `memory exhausted` first. The flag also sizes the range selector-cache cap (2 x max_samples x 16 bytes): 16,000,000,000 bytes here, against 320,000,000 at the default, so that cap is inert and the memory budget alone bounds a range query's cached selector copies. At the default, 5 history entries would be refused, the largest `mb_long_range_7d_avg` at about 40.3M | `--max-samples <N>  Cap on the total samples (summed across series, after cross-segment dedup) a single PromQL query may materialize ... Default when unset: ravel_query::DEFAULT_MAX_SAMPLES (10,000,000)` |
| Ravel | points per range series | `11000` | `11000` (unchanged, no flag) | Kept, same as Prometheus and Mimir: history's grid is 289 points | `crates/ravel-promql/src/eval.rs`, `DEFAULT_MAX_RANGE_POINTS: usize = 11_000` |
| Ravel | total evaluation points per query | `1000000` | `1000000` (unchanged, no flag) | Kept: `mb_long_range_subquery_max` (`[1d:5m]`, 289 points) over the 289-point grid is 83,521 | `crates/ravel-promql/src/eval.rs`, `DEFAULT_MAX_TOTAL_EVAL_POINTS: u64 = 1_000_000` |
| All five: Prometheus, VictoriaMetrics, Mimir, Ravel, s3s-fs | `ulimits.nofile` (docker-compose.yml, ravel.yml) | `524288` soft and hard | Docker's container default: soft `1024`, hard `524288` | Equal descriptors for every long-running service, at Docker's hard default. The Go engines (Prometheus, VictoriaMetrics, Mimir) raise their soft limit to the hard limit themselves, so for them this changes nothing; ravel-server does not (#3412), and the s3s-fs release binary imports neither `setrlimit` nor `prlimit` (`nm -D`), so without it they run at 1,024 descriptors | Default as recorded on #2821; each service's block is pinned by `tests/run.test.sh` case o |

Limits the corpus does not reach, left at their defaults: VictoriaMetrics
`-search.maxSamplesPerSeries` (default 30000000; history holds 172,800 samples
per series) and `-search.maxPointsSubqueryPerTimeseries` (default 100000;
`mb_long_range_subquery_max`'s `[1d:5m]` is 289 points), and the 11,000 points
per series Prometheus and Mimir allow on `/api/v1/query_range`, which is not a
flag.

Ravel's flagged values are written in `ravel.yml` and its admission values in
`config/ravel-limits.toml`, each with a comment naming the default it
replaces. The series and samples caps are flags since #3306 (`--max-series`,
`--max-samples`), raised in `ravel.yml` like any other flagged value, and the
capture records them with the rest of the container's argv. Two query caps
(points per range series, total evaluation points) and the future-skew bound
are still compiled into the image with no flag or limits-file key; the source
column cites the constant, and the capture cannot record them. Neither of
those two caps refuses a corpus entry. The refusal counts the series and
samples rows give at the old defaults are predicted from `workload.json`
series counts and sample spacing, not observed: no corpus pass has run
against this deployment.

Under Mimir 3.2.3's default query engine, `querier.max_samples` did not fire:
with it set to 100, a query over 28,801 samples returned 200. That engine's
per-query bound is `max_estimated_memory_consumption_per_query`, frozen at
2 GiB so an oversized query is refused with
`err-mimir-max-estimated-memory-consumption-per-query` (a `refused(budget)`
outcome, matched by the signature table below) instead of exhausting the 4g
envelope. Unlike the other Mimir limits it is sized to the envelope, not to
the corpus, so a large history-profile statement may be refused on Mimir
where Prometheus answers it. The flag is marked experimental in 3.2.3. The
`max_samples` value is still frozen for the Prometheus engine.

Mimir readiness: a historical replay is not queried until the bucket index
lists a block covering the replay's oldest sample and the store-gateway has
loaded every indexed block. `config/mimir.yaml` names the file, page and
metrics to read, beside the `frontend` block.

### Budget-refusal signatures

What each comparator returns when a limit fires. Observed on 2026-10-26 by
running each limit at a deliberately tiny value against the pinned versions'
upstream release binaries for linux/arm64 (Prometheus 3.13.1, VictoriaMetrics
v1.115.0, Mimir 3.2.3 with this directory's `config/mimir.yaml` and a
filesystem bucket in place of the stack's S3 store), then restoring the pinned value. The
error text is the `error` field of the JSON body; numbers inside it vary with
the query.

| Comparator | Limit | HTTP | `errorType` | Error text |
|---|---|---|---|---|
| Prometheus | `--query.max-samples` | 422 | `execution` | `query processing would load too many samples into memory in query execution` |
| Prometheus | `--query.timeout` | 503 | `timeout` | `query timed out in expression evaluation` |
| Prometheus | 11,000 points per series (`query_range`) | 400 | `bad_data` | `exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)` |
| Prometheus | out-of-order window (Remote Write) | 400 | none (plain text) | `out of order sample`; the whole request is rejected, in-order samples beside it included |
| VictoriaMetrics | `-search.maxPointsPerTimeseries` | 422 | `422` | `... too many points for the given start=..., end=... and step=...: 241; the maximum number of points is 10; (see -search.maxPointsPerTimeseries command-line flag)` |
| VictoriaMetrics | `-search.maxSamplesPerQuery` | 422 | `422` | `cannot select more than -search.maxSamplesPerQuery=100 samples; possible solutions: ...` |
| VictoriaMetrics | `-search.maxUniqueTimeseries` | 422 | `422` | `... the number of matching timeseries exceeds 5; either narrow down the search or increase -search.max* command-line flag values (the most likely limit is -search.maxUniqueTimeseries); ...` |
| VictoriaMetrics | `-search.maxQueryDuration` | 422 | `422` | `timeout exceeded during query execution: 0.001 seconds (elapsed 1.143 seconds); the timeout can be adjusted with ` followed by the flag name; checked between evaluation phases, so a query can overrun before it fires |
| VictoriaMetrics | `-search.maxPointsSubqueryPerTimeseries` | 422 | `422` | `too many points for the given start=..., end=... and step=...: ...; the maximum number of points is 100000; (see -search.maxPointsSubqueryPerTimeseries command-line flag)` |
| Mimir | `limits.max_fetched_chunks_per_query` | 422 | `execution` | `the query exceeded the maximum number of chunks (limit: 1 chunks) (err-mimir-max-chunks-per-query). ...` |
| Mimir | `limits.max_estimated_memory_consumption_per_query` | 422 | `execution` | `the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: 1000 bytes) (err-mimir-max-estimated-memory-consumption-per-query). ...` |
| Mimir | `limits.max_total_query_length` | 400 | `bad_data` | `the total query time range exceeds the limit (query length: 5h59m45s, limit: 1h0m0s) (err-mimir-max-total-query-length). ...` |
| Mimir | `querier.timeout` | 503 | `timeout` | `context deadline exceeded: query timed out` |
| Mimir | 11,000 points per series (`query_range`) | 400 | `bad_data` | `exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)` |
| Mimir | `querier.max_samples` | not observed | | Did not fire under the default query engine (see above) |
| Mimir | out-of-order window (Remote Write) | 400 | none (plain text) | `... this sample is beyond the out-of-order time window of 1h (err-mimir-sample-timestamp-too-old) ...`; only that sample is rejected, the rest of the request is stored |
| Ravel | `--max-series` (observed at the then-fixed 10,000) | 422 | `execution` | `query matched 10001 series, exceeding the limit of 10000` |
| Ravel | points per range series (11,000, `query_range`) | 422 | `execution` | `range query resolution of 12001 evaluation points exceeds the maximum of 11000; widen step_ms or narrow the range` |
| Ravel | `--max-ingest-lag` (Remote Write) | 204 | none | Not a refusal: the too-old sample is dropped per sample and the request answers 2xx; it shows only as a missing sample |

The Ravel rows were observed on 2026-10-26 against ravel-server 0.26.0
(linux/arm64), run with the `ravel.yml` flags of that date (before the
column-statistics cache pin, a flag 0.26.0 lacks) and `--store memory`, at the
pinned values rather than tiny ones: 10,001 series and a 12,001-point range
were written and queried. Not observed on the pinned image: max samples per
query, total evaluation points, `--max-s3-requests`, `--max-segments`, the
`--gc-max-query-duration` deadline, and `max_bytes_scanned` (unlimited here).
Their signatures are not listed until observed.

The match key for a status mapper is the stable token: Mimir's `err-mimir-*`
identifier, VictoriaMetrics' flag name in the message, and Prometheus'
`errorType` with its fixed message. Ravel's `errorType` is `execution` for
both observed limits, so its key is the fixed message prefix: `query matched`
for max series, `range query resolution of` for range points.

## Launch the set

```sh
docker compose -f deploy/metricsbench/docker-compose.yml up -d
```

This starts the comparators alone; add `-f deploy/metricsbench/ravel.yml`
with `RAVEL_IMAGE` set to include Ravel (see "Ravel in the portable lane").
For a comparators-only launch, run `capture-effective-config.sh --no-ravel` so
the capture does not query a Ravel that is not running.

Every image in `docker-compose.yml` is pinned to an immutable `@sha256:`
digest (see below); Ravel's image is the `RAVEL_IMAGE` the run passes. Each
comparator, Ravel included, is given identical CPU (2.0) and memory (4g)
limits so no engine wins on hardware.

**The digest pins the manifest, not the platform.** These are multi-architecture
manifest-list digests, so the same digest resolves to different image bytes on
`linux/amd64` than on `linux/arm64`. A pinned digest therefore makes a run
reproducible *per platform*, not across platforms. Any run whose numbers are
reported must record the platform it resolved on, and a cross-host comparison
is only valid between hosts of the same architecture. Two hosts with the same
digest and different architectures are running different binaries, which is
exactly the kind of difference that reads as an engine result.

Tear down (and drop the local volumes):

```sh
docker compose -f deploy/metricsbench/docker-compose.yml down -v
```

With Ravel up, name both files and keep `RAVEL_IMAGE` set, since compose
interpolates the overlay on `down` too.

## Record what actually ran

The compose file pins what should run; this captures what did. With the set
up, run:

```sh
deploy/metricsbench/capture-effective-config.sh --out effective-config.json
```

`--prometheus URL`, `--victoriametrics URL`, `--mimir URL` and `--ravel URL`
override the loopback compose ports (9090, 8428, 9009, 4318); without `--out`
the document goes to stdout. Per comparator it records the version and revision the engine itself
reports, its runtime flags, and, when docker is reachable, the running
container's image ID, `.Config.Image`, `RepoDigests`, `Architecture` and `Os`.
The sources are:

- Prometheus: `/api/v1/status/buildinfo`, `/api/v1/status/flags`, and
  `/api/v1/status/config` for settings that live in `prometheus.yml` with no
  flag (recorded under `config_settings`, today the TSDB out-of-order window).
- VictoriaMetrics: the `vm_app_version` series on `/metrics` (`short_version`
  is the version, the full `version` label the revision) and `/flags`. Its
  `/api/v1/status/buildinfo` returns a fixed Prometheus-compatibility version
  rather than its own, so the script does not read it.
- Mimir: `/prometheus/api/v1/status/buildinfo`, and `/config`, the effective
  YAML including defaults (its `/api/v1/status/flags` is an empty
  compatibility stub). `querier_query_engine` is `querier.query_engine` read
  out of `/config`; `query_engine_default_for_version` is the documented
  default for the reported version: `prometheus` for 2.14 to 2.16, `mimir`
  (the Mimir Query Engine) from 2.17.
- Ravel: `/api/v1/status/buildinfo` (no bearer needed) for version and
  revision. Ravel serves no flags endpoint, so `runtime_flags` is the
  `ravel-server` container's command line, `.Config.Cmd` from `docker
  inspect`; it is unavailable when docker is not reachable. The limits file
  and the compiled-in query caps are not read. Every published ravel-server
  image reports an empty `revision`: the server reads it from `RAVEL_GIT_SHA`
  at compile time, and no image build sets it (issue #3312). The capture
  records it as unavailable and does not count it toward exit 1, since no
  image can supply it; Ravel's version still counts. `--no-ravel` skips Ravel
  for a comparators-only launch. The image ID and
  `RepoDigests` still identify the build. `compose ps` reads both `docker-compose.yml` and `ravel.yml`; when
  `RAVEL_IMAGE` is unset the script passes a placeholder so compose can
  interpolate the overlay, which affects only the listing.

A field that could not be fetched is `{"unavailable": "<reason>"}`; none is
omitted or left empty. The exception is a comparator skipped with
`--no-ravel`, recorded as `{"skipped": "--no-ravel"}` with no other fields and
no URL in `urls_queried`. When docker is not reachable, `image` is the string
`"unavailable: docker not reachable"`.

| Exit | Meaning |
|---|---|
| 0 | Every comparator answered its version call. |
| 1 | At least one comparator's version or revision is unavailable. The document is still written, since a partial capture is evidence, but it is not a successful capture. |
| 64 | Usage error. |
| 70 | The capture crashed or could not write the document. Nothing at `--out` belongs to this run; a file there is from an earlier one. |

The document's shape, abbreviated:

```json
{
  "captured_at": "2026-10-26T02:56:15Z",
  "script_revision": "<git HEAD of the checkout, or \"unavailable\">",
  "urls_queried": {
    "mimir": {
      "buildinfo": "http://127.0.0.1:9009/prometheus/api/v1/status/buildinfo",
      "runtime_flags": "http://127.0.0.1:9009/config"
    }
  },
  "comparators": {
    "prometheus": {"version": "3.13.1", "revision": "...", "runtime_flags": {"...": "..."}, "config_settings": {"storage.tsdb.out_of_order_time_window": "1h"}, "image": {"...": "..."}},
    "victoriametrics": {"version": "v1.115.0", "revision": "victoria-metrics-...-tags-v1.115.0-0-g...", "runtime_flags": {"...": "..."}, "image": {"...": "..."}},
    "mimir": {
      "version": "3.2.3",
      "revision": "...",
      "runtime_flags": {"format": "yaml", "effective_config": "..."},
      "querier_query_engine": "mimir",
      "query_engine_default_for_version": "mimir",
      "image": {
        "container": "...",
        "image_id": "sha256:...",
        "config_image": "grafana/mimir:3.2.3@sha256:...",
        "repo_digests": ["grafana/mimir@sha256:..."],
        "architecture": "amd64",
        "os": "linux"
      }
    },
    "ravel": {
      "version": "0.26.0",
      "revision": {"unavailable": "..."},
      "runtime_flags": {"source": "docker inspect .Config.Cmd of compose service ravel-server", "cmd": ["--mode", "all", "..."]},
      "image": {"...": "..."}
    }
  }
}
```

A run report cites this file by path and by its sha256 digest. `run.sh`
(below) calls it after ingest and before the query passes. Its cases run in CI's `doc-scripts` job:
`deploy/metricsbench/tests/capture-effective-config.test.sh`.

## Running a profile end to end

`run.sh` drives one profile through the whole set and stamps what ran. From
the repository root, with the three bins built
(`cargo build --release --locked -p ravel-bench --bin metricsbench_ingest
--bin metricsbench_query --bin metricsbench_report`):

```sh
deploy/metricsbench/run.sh \
  --profile ci \
  --ravel-image ghcr.io/nofireai/ravel-server:manual-0bf5fed@sha256:42116db1a8b2b0da52b5bc71219678d9b32d42aefb61ba46152ba9f9968d5ca7 \
  --ravel-source-commit 0bf5fedb2834df7dc87f8086701dd289b049efb0 \
  --bin-dir target/release \
  --out-dir metricsbench-out
```

That is the published form, the only one a comparable run may use. The
nightly (`metricsbench-nightly.yml`) runs the local-build form instead: it
builds Ravel from the commit it checked out with `docker build --target
server -t ravel-server:nightly-<sha> .`, reads the image's Id, and passes

```sh
deploy/metricsbench/run.sh \
  --profile ci \
  --ravel-image ravel-server:nightly-<sha> \
  --ravel-image-id sha256:<the image's Id> \
  --ravel-source-commit <sha> \
  --bin-dir target/release \
  --out-dir metricsbench-out
```

so every nightly measures the Ravel at the commit it checked out (`main`'s
head on the schedule), not a frozen image. The image is never pushed
(ADR-0037 decision 4 keeps publishing off plain `main` commits). A
local-build run is non-comparable and never publishable: `run.sh` accepts
`--ravel-image-id` with `--profile ci` only, and stamps
`ravel_image.kind: "local-build"` in `stamps.json` and `ravel_image_kind`
in the provenance it hands the report, so the run cannot be read as a
published image's (case p).

Every claim below names the case in `deploy/metricsbench/tests/run.test.sh`
that proves it; the cases stub docker, curl, the three bins and the capture
script, and run in CI's `doc-scripts` job.

| Flag | Meaning |
|---|---|
| `--profile NAME` | A profile in `benchmarks/metrics/workload.json`. |
| `--ravel-image REF@sha256:DIGEST` | The published form: a registry image pinned by its digest, set as `RAVEL_IMAGE`. A reference without a digest, and no `--ravel-image-id`, is refused with exit 65 (case c). Stamped `ravel_image.kind: "published"` (case a). |
| `--ravel-image REF --ravel-image-id sha256:ID` | The local-build form: an image built on this host, which has no registry digest, named by the local tag `REF` and identified by its content. Refused with exit 65 before anything starts unless `docker image inspect REF` reports `Id` equal to `ID`, and with any profile other than `ci`, since a local build is for the nightly only. Stamped `ravel_image.kind: "local-build"` with `image_id` and `reference`, and passed to provenance as `ravel_image_kind` and `ravel_image_id` (case p). Exactly one form is required: an id with a digest-pinned `--ravel-image`, an id with no `--ravel-image`, or no image at all exits 64 (case p). |
| `--ravel-source-commit SHA` | The full commit the image was built from, stamped as given (case a). |
| `--bin-dir DIR` | Holds `metricsbench_ingest`, `metricsbench_query` and `metricsbench_report`. A missing one is refused with exit 65 (case h). |
| `--out-dir DIR` | Created if absent. One that already holds `stamps.json`, `report.json`, `ingest.json` or `falsification.json` is refused with exit 65 (case h). |
| `--anchor-ms N` | The replay anchor, which must be on the profile's scrape grid (exit 65 otherwise, case h). Default: the profile's window ending at least 10 minutes before the run, on the grid, stamped `anchor_source` as from the clock. Ravel drops samples older than its 45-day `--max-ingest-lag`, so a fixed old anchor fails Ravel's read-back. |
| `--passes N`, `--reps N` | Passed to `metricsbench_query` as `--passes` and `--reps-per-pass` and stamped (case g); its own defaults (5 and 3) otherwise. |
| `--keep` | Leave the stack up; stamps say `kept (--keep)` and no `down -v` runs (case g). |
| `--rss-band CONTAINER:MIN:MAX` | Repeatable. The peak sampled memory of the container (matched by container name or compose service) must lie in `[MIN, MAX]` bytes; outside it, or with no samples, the run exits 75 (case d). |
| `--diagnostic-prometheus-max-samples N` | The falsification run, below. |

A compose project `metricsbench` that already has containers or volumes is
refused with exit 65 before anything starts (case h), since an earlier run's
data would be measured.

Steps, in order, stopping at the first failure, except a failed ingest
verification (step 6), after which the run goes on as a diagnostic:

1. `up`: `docker compose -f docker-compose.yml -f ravel.yml up -d` (case a).
2. `qualify`: `createbuckets` and `ravel-qualify` exited 0; otherwise exit
   70 naming the step (case i).
3. `image`: `docker image inspect` of the Ravel image, which must list the
   digest the reference names (case a), or, for a local build, still report
   the Id `--ravel-image-id` names (case p); the result is stamped.
4. `ready`: Prometheus `/-/ready`, VictoriaMetrics `/health`, Mimir
   `/ready` and Ravel `/api/v1/status/buildinfo` each answer 200 (case a).
5. `sampler`: every 5 s after the previous sample, the memory use of every
   running compose container from `docker stats --no-stream` is appended to
   `memory.jsonl` (case a).
6. `ingest`: `metricsbench_ingest` into all four portable endpoints with
   their read-back query URLs, output `ingest.json`. A read-back whose
   figures do not match what was sent is repeated every 500 ms for up to
   `--readback-settle-secs` (default 60, the same for every engine), since
   VictoriaMetrics acknowledges a write before it is queryable; each row's
   `readback` records `attempts` and `settled_after_ms`. Ravel's token reaches it
   only through `--ravel-bearer-token-env METRICSBENCH_RAVEL_TOKEN`, never on
   a command line (case a). A row whose read-back fails (an error, a
   refusal or a mismatch) or that stalls does not stop the bin: it is
   recorded and every later row is still replayed, and the bin exits 1 after
   the last row with `ingest.json` holding every row, the failed one with its
   mismatching `readback` or its `readback_error`
   (`metricsbench_ingest_smoke::a_failed_readback_does_not_stop_the_later_rows`),
   a stalled one under `stalled` with its projected seconds and ceiling (the
   `LaneError::IngestStalled` arm of `metricsbench_ingest`'s row loop; no
   test drives a stall through the bin, since its grace period is 60 s).
   On such a failed ingest verification run.sh records `ingest` as
   `failed_step` in `stamps.json` and goes on through every later step:
   `mimir-readiness` is stamped without being checked, `query` passes
   `--diagnostic-after-failed-ingest`, `validate` must refuse the report as
   a diagnostic of a failed run, and the run then exits 70 naming step
   ingest (cases q, q2). A later step's failure, an `--rss-band` breach
   included, is listed in `stamps.json` as `failed_after_ingest` and the
   exit stays 70 naming ingest (case q3). That failure ends the run there,
   so the steps after it do not run. When the failed row is the Prometheus
   oracle's (a failed read-back, a `row_failure`, a stall or a failed
   replay), `query` refuses the diagnostic and writes no `report.json`, so
   the `validate` and `memory` steps do not run and `failed_after_ingest` is
   `["query"]`. `memory-summary.json` is still written on the way out, with
   every `--rss-band` checked, so a breach is recorded there, but it is not
   listed in `stamps.json` and does not change the exit (case q4). Any other failing ingest (no
   report on stdout, or a report with no failed row, such as a band 4
   accounting failure) exits 70 naming the step at once, before capture or
   query, and the stack is still torn down (cases b, r).
   The `ci` profile keeps the ingest's
   in-process diagnostic row on a memory store (case a); every other profile
   passes `--no-in-process-row` (case l), since cardinality's and history's
   streams do not fit in memory beside four engine containers and the
   stack's s3s-fs is the engines' own store. Those runs lose one check: the
   portable rows' expected scalar digests are not compared with an
   in-process reference, only with each other (band 6 across rows).
   Each HTTP row is sent on the bin's defaults, 50 series shards of up to
   2000 points per request with one request in flight per shard; run.sh
   passes neither `--http-shards` nor `--http-batch-size` (case m). It passes
   `--max-ingest-secs` per profile: ci 600, cardinality 14400, history 72000,
   churn 18000, the diagnostic mode's ingest included (case m), and
   `--readback-timeout-secs`, the per-request timeout of each read-back
   query with its body read, per profile: ci 30, cardinality 900, history
   300, churn 900 (case m). A read-back that hangs on every query can hold
   a row for the 60 s settle window plus one read at that timeout per
   query. A read is 3 + 3W queries, W being the profile's day windows
   (the series read, the histogram series read and the stale-marker read,
   plus one query per window for each of up to three count probes): ci
   W = 1, 6 queries, about 4 minutes; cardinality W = 1, 6 queries, about
   91 minutes; churn W = 2, 9 queries, about 136 minutes; history W = 30,
   93 queries, about 7.8 hours. Each engine's own 120 s query deadline
   normally ends a query first, which caps history at about 3.1 hours.
   `--http-timeout-secs` bounds writes only. Each
   row's `readback.query_ms` records every read-back query's wall time in
   milliseconds, and a failed query names its elapsed time. A row whose
   projected wall time passes its ceiling stops that row only and makes the
   bin exit 1 after the last row, so the run goes on as a diagnostic and
   exits 70 naming step ingest (case q2); a profile with no
   ceiling in run.sh is refused with exit 65 before anything starts.
7. `mimir-readiness`: the ingest row for Mimir read back exactly what it
   accepted; the line is stamped (case a). After a failed ingest
   verification the line is stamped and the step is not checked (case q).
8. `capture`: `capture-effective-config.sh` into `effective-config.json`,
   which must exit 0; a partial capture (exit 1) fails the step with exit
   70 before any query runs (case k).
9. `provenance`: `metricsbench_report provenance` into `provenance.json`,
   pinning Prometheus, VictoriaMetrics and Mimir at the version and digest
   the capture recorded (case a). Its `ravel_git_commit` is
   `--ravel-source-commit`, not this checkout.
10. `fold-state`: `ravel_catalog_fold_last_success_timestamp_seconds` from
    Ravel's `/metrics` (case a), or `absent` (case j).
11. `query`: `metricsbench_query` against all four engines into
    `report.json`, with `--diagnostics diagnostics.json` for the per-entry
    ULP gaps against the oracle and every oracle retry (case a; the
    sidecar's fields are in benchmarks/metrics/README.md, "Portable query
    run"). After a failed ingest verification the run passes
    `--diagnostic-after-failed-ingest`, and `report.json` carries
    `run_verdict: "failed"` and `failed_bands` (case q). When the failed row
    is the Prometheus oracle's, `metricsbench_query` refuses instead, writes
    no report, and the run ends here (case q4).
12. `validate`: `metricsbench_report validate report.json` exits 0 (case a).
    After a failed ingest verification it must refuse the report, naming it
    a diagnostic of a failed run; a validate that accepts it fails the run
    (case q).
13. `memory`: the sampler stops, `memory-summary.json` records peak and
    final bytes per container and `OOMKilled` from `docker inspect` (case a),
    and every `--rss-band` is checked (case d). An OOM-killed container exits
    75 (case e).

Whatever the outcome, the compose logs go to `compose.log` and the stack is
torn down with `docker compose ... down -v` unless `--keep` is given (cases
a, b, d, f, i, k). On a failed step the memory summary is still written (case
b). s3s-fs logs to stdout, so its log is in `compose.log`; nothing is copied
out of its container. The `s3s` service is in the service list `compose.log`
is taken over (case o).

Before the stack starts, the `fetch-s3s-fs` step resolves the s3s-fs binary
(see "The object store"). A binary already in the cache whose sha256 equals
the pin is reused without a download (case n1); a cached binary with another
sha256 is refused with exit 65 and nothing is downloaded (case n3); a download
whose sha256 differs is refused with exit 65, nothing is cached and nothing
starts (case n2); a failed download exits 70 naming the step (case n6). An
engine arch other than `amd64`/`x86_64` or `arm64`/`aarch64` is refused with
exit 65 before anything starts (case n4), and `aarch64` fetches its own asset
against its own pin (cases n5 and n7). The cache is `$METRICSBENCH_S3S_FS_CACHE`, else
`$XDG_CACHE_HOME/ravel-metricsbench`, else `~/.cache/ravel-metricsbench`, so
the run's output directory does not carry the binary. The diagnostic mode
starts no object store and fetches nothing (case f).

`stamps.json` is written before the first step and completed at the end
(case a). It holds `ravel_image.kind` (`published` or `local-build`, cases a
and p), `ravel_image.reference`, `ravel_image.image_id` (local builds only,
case p), `ravel_image.resolved` (the `docker image inspect` result),
`ravel_source_commit`, `harness_commit`
(`git rev-parse HEAD` of the checkout holding `--bin-dir`, or
`unavailable: ...` outside one), `run_script_commit`, `profile`, `anchor_ms`
and `anchor_source`, `started_at` and `ended_at`, `store_kind: s3s-fs`, `host.arch`,
`instance_type` (EC2 IMDSv2, else `unknown`), `comparator_images` (each
comparator's image block as the capture recorded it), `ravel_catalog_fold`,
`mimir_readiness`, `in_process_row` (`present` or `skipped`, cases a, l and
f), `object_store` (release, asset arch, engine arch and its source, sha256,
URL, cached path and whether it came from the cache or a download; `not applicable` in the diagnostic mode;
cases a, f, n1, n5 and n7), `steps_completed`, `failed_step`,
`failed_after_ingest` (cases a, q and q3), `exit_code`, `status` and
`teardown`.

### The falsification run

With `--diagnostic-prometheus-max-samples N`, `run.sh` instead writes a
compose override setting Prometheus' `--query.max-samples=N` into
`<out>/prometheus-max-samples.override.yml`, starts Prometheus alone with it,
ingests the profile into Prometheus only, and sends `mb_long_range_7d_avg`'s
query once as an instant query at the newest sample. `falsification.json`
records the HTTP status, `status`, `errorType`, `error` and the result's
series count; no `report.json` is written and stamps say `diagnostic: true`
(case f). `--ravel-image` and `--ravel-source-commit` are optional here,
since no Ravel starts. A `--profile` the corpus does not declare
`mb_long_range_7d_avg` for (today only `history`) is refused with exit 65
before anything starts: elsewhere the query loads too few samples to reach
the limit, and its success would falsify nothing (case f2). A failed ingest
stops this run at once, whatever the failure: it has no query step to go on
to.

```sh
deploy/metricsbench/run.sh --profile history --bin-dir target/release \
  --out-dir falsify-out --diagnostic-prometheus-max-samples 50000000
```

| Exit | Meaning |
|---|---|
| 0 | Every step passed (cases a, f, g). |
| 64 | Usage error (cases h, p). |
| 65 | A refused input, or an s3s-fs binary whose sha256 is not the pin; nothing was started (cases c, h, p, n2, n3, n4). |
| 70 | A step failed; its name is in the message and in `stamps.json` as `failed_step` (cases b, i, k, n6, r). After a failed ingest verification (a row's read-back failed or a row stalled) the run queries as a diagnostic, records the failure, and exits 70 naming step ingest, even when a later step failed too; the first later step to fail ends the run, so an `--rss-band` breach reaches `failed_after_ingest` only when every step before `memory` passed, while `memory-summary.json` records it either way (cases q, q2, q3, q4). |
| 75 | An `--rss-band` was violated or a container was OOM-killed (cases d, e). |

## Verify the pins

```sh
deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh
```

Besides this directory's compose file, the script checks image and action
pins across the repository. All seven categories run in one invocation and each
is checked against its own expected count:

- **Compose images** (this directory, `docker-compose.yml` and the
  `ravel.yml` overlay): every image reference except the two
  `${RAVEL_IMAGE:?...}` references in `ravel.yml` (the image under test, a
  required run parameter, excluded by exact match) carries an `@sha256:`
  digest, every ADR-0927-required comparator and `ravel-server` are present,
  and both the total image-reference count (7, across both files) and the
  pin-required count (5) equal their expected totals.
- **Dockerfile base images**: every `FROM`/`ARG` base image reference in the
  root `Dockerfile` and `Dockerfile.prebuilt` carries an `@sha256:` digest
  (`FROM scratch` is exempt: no registry manifest exists for it), and the
  number of references equals the expected count.
- **Workflow actions**: every `uses:` reference in every workflow and
  composite action carries a 40-hex commit SHA pin, and the number of
  references equals the expected count.
- **Quickstart compose images** (`deploy/docker-compose/ravel.yml` and
  `deploy/docker-compose/rustfs.yml`): every `image:` reference except the two
  `${RAVEL_IMAGE:-...}` references in `ravel.yml` (Ravel's own released
  image, excluded by exact match) carries an `@sha256:` digest, and both the
  total image-line count (8, across both files) and the pin-required count
  (6) equal their expected totals. The `deploy/k8s` manifests are covered by
  their own category below.

- **Workflow docker invocations**: every image argument of a `docker run`,
  `docker pull` or `docker create` inside a `run:` block, across every
  workflow under `.github/workflows`, carries an `@sha256:` digest. Image
  references that come from a shell variable are excluded by exact match,
  since the variable holds either a locally built tag or a reference the
  workflow already resolved to a digest. Both the total (12) and the
  pin-required count (6) equal their expected totals. Backslash
  continuations are joined before matching, here-doc bodies are skipped, and
  every invocation on a line is scanned, not just the first.

- **Kubernetes manifest images**: every `image:` reference under
  `deploy/k8s` carries an `@sha256:` digest, with the two locally built
  placeholders that kind loads by tag (`ravel-server`, `ravel-operator`)
  excluded by exact match. The total and the pin-required count each equal
  their expected number, so an image added there without a digest fails the
  check rather than going unnoticed.

- **s3s-fs binary sha256** (`run.sh` and `docker-compose.yml`): `run.sh`
  carries both pinned sha256 constants exactly once each and maps each arch
  to its own; `fetch_s3s_fs` downloads exactly once and compares the cached
  binary and the download against the pin, each mismatch exiting 65, before
  it moves the download into the cache and exports `S3S_FS_BIN`; and
  `docker-compose.yml` takes the binary only from `${S3S_FS_BIN:?...}`, with
  no default. Each of these is shown failing by a case in
  `tests/every_comparator_pins_an_image_digest.test.sh` (`s3s_*`).

Exit 0 means all seven categories passed. Any unpinned reference, any missing
required comparator, or a reference count that drifts from any category's
expected number fails the check with a non-zero exit. The script prints what
it checked and how many references it found in each category.

This check runs in CI, wired into the `doc-scripts` job in
`.github/workflows/ci.yml`.

## Pinned image digests

Every digest below is the multi-arch manifest-list digest reported by the
named tag's own registry. The recipe differs by registry, so the two below
are not interchangeable: substituting a ghcr.io repo path into the Docker
Hub URL (or vice versa) resolves nothing.

**`prom/prometheus`, `victoriametrics/victoria-metrics`, `grafana/mimir`, and
`amazon/aws-cli`** are on Docker Hub and resolve with the Docker Hub registry
v2 API. The AWS CLI moved here from ECR Public because ECR Public refused the
nightly's anonymous pull with `Data limit exceeded` (run 40793034340, both
attempts); `amazon/aws-cli:2.37.2` resolves to the same digest ECR Public
served:

```sh
curl -sI -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
  https://registry-1.docker.io/v2/<repo>/manifests/<tag> \
  | grep -i docker-content-digest
```

An image on another registry, such as the quickstart's `ghcr.io/rustfs/rustfs`
that the example below resolves (see deploy/README.md for why the quickstart's
object store lives off Docker Hub), issues a bearer token from its own auth
endpoint rather than accepting one minted for Docker Hub, so the recipe is the
same shape with a different token URL per registry.

This is written as two curl steps rather than a one-liner so the token is
carried over by hand instead of captured into a shell variable.

```sh
# 1. Get a bearer token scoped to the repo, and read the token field out
#    of the JSON it prints.
curl -s "https://ghcr.io/token?service=ghcr.io&scope=repository:rustfs/rustfs:pull" \
  | jq -r .token

# 2. HEAD the manifest list, pasting that token in place of <TOKEN> below.
#    RustFS publishes an OCI image index, so the Accept header names that
#    media type.
curl -sI -H "Authorization: Bearer <TOKEN>" \
  -H "Accept: application/vnd.oci.image.index.v1+json" \
  https://ghcr.io/v2/rustfs/rustfs/manifests/1.0.0 \
  | grep -i docker-content-digest

# 3. The docker-content-digest response header is the pinned digest.
```

**`gcr.io/distroless/cc-debian12:nonroot`**, the `s3s` service's base, is not
resolved separately: it is the digest the root `Dockerfile` and
`Dockerfile.prebuilt` already pin, copied unchanged.

| Image | Tag | Digest |
|---|---|---|
| `prom/prometheus` | `v3.13.1` | `sha256:3c42b892cf723fa54d2f262c37a0e1f80aa8c8ddb1da7b9b0df9455a35a7f893` |
| `victoriametrics/victoria-metrics` | `v1.115.0` | `sha256:d8ac3a1776c8a9beead8bbd42a489c82249b1bfe9071dfd4813f34ebe36354bb` |
| `grafana/mimir` | `3.2.3` | `sha256:0ed56db7d7713707eff00224ddd4c3096afa6cf1cbe43a6f9391c537f1416099` |
| `gcr.io/distroless/cc-debian12` | `nonroot` | `sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f` |
| `amazon/aws-cli` | `2.37.2` | `sha256:e38214027df83cb6631adcf980a092a98d1d29788789bff2a0f424e87e3da8ed` |

The tag is kept in each `image:` reference alongside the digest for human
readability; the digest is what pins the run.

### Pins shared with deploy/docker-compose/ravel.yml and rustfs.yml

The two quickstart compose files are scanned by the same script as its fourth
category (see above). Their AWS CLI pin is the same tag and digest as the one
above. Their RustFS pin, `ghcr.io/rustfs/rustfs:1.0.0` at
`sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff`, is
no longer used by this stack. `deploy/README.md` lists all four of their pins and why
these registries were chosen.
