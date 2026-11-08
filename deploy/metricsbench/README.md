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
at the `s3s-fs-0.17.0-ravel.3` release of the `NOFireAI/s3s` fork (ADR-0927
amendments 2026-10-29, 2026-11-01 and 2026-11-02). It replaced RustFS, which
sat at its one-CPU cap while ravel-server idled, so the store bounded the
Ravel row. Figures measured on RustFS are not compared with figures measured
on s3s-fs. `ravel.2` is `ravel.1` plus `TCP_NODELAY` on every accepted
connection: `ravel.1` held each response's body until the client acknowledged
its head, which added a 40 ms delayed-ACK stall to every query that touched
storage. `ravel.3` is `ravel.2` plus atomic conditional writes: `ravel.2`
checked `If-None-Match` and `If-Match` before streaming the body and published
the body and its two sidecars separately, so concurrent conditional
writers could each succeed and a body could be left beside another write's
stored checksum. Figures measured on one release are not compared with
figures measured on another. The ClickBench entry still runs `ravel.1`.

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
| `x86_64` | `99592a8a64cbb773c34aee9966537eb4d1091d9a91b70699220f1f34dd4b384e` |
| `aarch64` | `92d7bbbb29bd4c5aee00b5f1c280c1402e799ed8a28f497e5ba813f1ebc3eecc` |

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

**Envelope.** The same `cpus: "2.0"` and `memory: 8g` limits as `prometheus`,
`victoriametrics` and `mimir`. The envelope was 4g until Mimir was
OOM-killed at 4g ingesting the `cardinality` profile; every engine's memory
was raised together, so figures taken at 4g are not compared with figures
taken at 8g. Ravel's memory split is written as flags, not left to the
server's derivation from the container, and the server logs each value with
`source="flag"` at startup:

| Quantity | Bytes | How |
|---|---|---|
| Envelope | 8,589,934,592 | `memory: 8g` |
| Reserve outside the budget | 2,147,483,648 | max(min(2 GiB, envelope / 4), 256 MiB baseline + 512 MiB ingest buffer) |
| `--memory-budget-bytes` | 6,442,450,944 | envelope minus reserve |
| `--cache-max-bytes` (fetcher cache) | 1,610,612,736 | 25% of the budget, the derived share for a non-loopback S3 endpoint |
| `--catalog-cache-max-bytes` | 322,122,547 | 5% of the budget, the derived share |
| `--column-stats-cache-max-bytes` | 128,849,018 | 2% of the budget, the derived share (ADR-1413 decision 5) |
| Budget left for query working set | 4,380,866,643 | budget minus the three caches (2,061,584,301) |
| `--max-ingest-buffer-bytes` | 536,870,912 | the 512 MiB default, stated; held in the reserve |

The three Go comparators are told their envelope through `GOMEMLIMIT=7GiB`
(ADR-0927 amendment 2026-11-05), set in `docker-compose.yml` on
`prometheus`, `victoriametrics` and `mimir`; its row is under "Frozen limits"
below.

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
- `--fetch-concurrency 32` replaces the derived default, `max(8, 2 x cores)`,
  which is 8 in the 2-CPU envelope (ADR-0927 fetch-concurrency amendment,
  2026-11-03). On real S3 the cardinality read-back is bound by GETs in
  flight: on the folded 1,000,000-series tenant the `/api/v1/series` listing
  took 264 to 277 s at 8, past the 120 s deadline, and 71 to 74 s at 32, with
  the same GETs and bytes and no memory refusal (#2821, comment 6601589888).
  The flag also sets the SQL partition count, which the corpus never uses.
  Figures measured at the derived 8 are not compared with figures measured
  at 32.
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
| VictoriaMetrics | `-search.maxSeries` | `1000000` | `30000` | The harness's cardinality read-back lists all 1,000,000 series through `/api/v1/series`; the default refuses it with a 422 | `-search.maxSeries int  The maximum number of time series, which may be returned from /api/v1/series ... (default 30000)` |
| VictoriaMetrics | `-search.disableCache` | `true` | `false` | Historical replay; a production_cache regime re-enables it per run | `-search.disableCache  Whether to disable response caching. This may be useful when ingesting historical data.` |
| Mimir | `limits.out_of_order_time_window` | `1h` | `0s` | Same as Prometheus | `-ingester.out-of-order-time-window duration  Non-zero value enables out-of-order support`; `0s` read from the 3.2.3 binary's `/config` |
| Mimir | `limits.past_grace_period` | `0s` | `0s` (unchanged) | A 30 d replay is not refused for its age | `-validation.past-grace-period duration ... 0 to disable.` |
| Mimir | `querier.query_store_after` | `0s` | `12h` | Replayed timestamps follow the anchor, not the wall clock; every query reaches the store-gateway | `-querier.query-store-after duration ... 0 means all queries are sent to store. ... (default 12h0m0s)` |
| Mimir | `limits.query_ingesters_within` | `0s` | `13h` | Every query reaches the ingester, so unshipped samples stay visible | `-querier.query-ingesters-within duration ... 0 means all queries are sent to ingester. (default 13h)` |
| Mimir | `blocks_storage.bucket_store.sync_interval` | `15m` | `15m` (unchanged) | Readiness wait, not a query budget | `-blocks-storage.bucket-store.sync-interval duration  How frequently to scan the bucket, or to refresh the bucket index ... (default 15m0s)` |
| Mimir | `compactor.cleanup_interval` | `15m` | `15m` (unchanged) | Readiness wait, not a query budget | `-compactor.cleanup-interval duration  How frequently the compactor should run blocks cleanup and maintenance, as well as update the bucket index. (default 15m0s)` |
| Mimir | `limits.max_fetched_chunks_per_query` | `5000000` | `2000000` | Per statement, counting a shipped block twice (the querier reads the overlap from the ingester and the store-gateway). Largest read: `mb_full_range_avg_over_time` on cardinality, 96,000,000 samples / 120 = 800,000 chunks, up to 1,200,000 with one 2 h block cut per series, 2,400,000 counted twice. Largest history read: `mb_long_range_7d_avg`, 1,000 x 40,320 / 120 = 336,000. Out-of-order chunks add at most 450,000 (one in 800 of cardinality's 360,000,000 samples) | `-querier.max-fetched-chunks-per-query int  Maximum number of chunks that can be fetched in a single query ... (default 2000000)`; 120 per chunk observed (9,600 samples in 80 chunks per 2 h block of 20 series) |
| Mimir | `querier.max_samples` | `500000000` | `50000000` | As for Prometheus (peak samples held); does not fire under the default query engine (see below) | `-querier.max-samples int  Maximum number of samples a single query can load into memory. ... (default 50000000)` |
| Mimir | `limits.max_estimated_memory_consumption_per_query` | `4294967296` (4 GiB) | `0` (disabled) | Half the 8g envelope, which this one process shares with the ingester and the store-gateway; sized to the envelope, not the corpus (see the note below) | `-querier.max-estimated-memory-consumption-per-query uint  [experimental] ... only enforced when Mimir's query engine is in use. ... 0 to disable.` |
| Mimir | `querier.timeout` | `2m` | `2m` (unchanged) | Equal to Prometheus' `--query.timeout` | `-querier.timeout duration  The timeout for a query. ... (default 2m0s)` |
| Mimir | `limits.max_total_query_length` | `31d` | `0s` (no limit) | History's 30 d span plus 1 d; `max_query_length` does not exist in 3.2.3 | `-query-frontend.max-total-query-length duration  Limit the total query time range (end - start time).` |
| Mimir | `limits.max_partial_query_length` | `0s` | `0s` (unchanged) | No separate bound on split sub-queries | `-querier.max-partial-query-length duration  Limit the time range for partial queries at the querier level.` |
| Mimir | `limits.max_query_lookback` | `45d` | `0s` (disabled) | The 45 d retention; a silent clamp, not a refusal | `-querier.max-query-lookback duration ... the request will not fail but will be manipulated to only query data within the allowed time range. 0 to disable.` |
| Mimir | `frontend.query_result_response_format` | `protobuf` | `protobuf` (unchanged) | Native histograms under the default query sharding | `-ingester.native-histograms-ingestion-enabled ... To query native histograms with query-sharding enabled make sure to set -query-frontend.query-result-response-format to 'protobuf'.` |
| Mimir | `frontend.cache_results` | `false` | `false` (unchanged) | Parity with `-search.disableCache=true` | `-query-frontend.cache-results  Cache query results.` |
| Mimir | gRPC message ceilings: `server.grpc_server_max_{recv,send}_msg_size` and `max_{recv,send}_msg_size` under `ingester_client.grpc_client_config`, `querier.store_gateway_client`, `frontend.grpc_client_config`, `frontend_worker.grpc_client_config`, `frontend_worker.query_scheduler_grpc_client_config` and `query_scheduler.grpc_client_config` | `536870912` (512 MiB) | `104857600` (100 MiB) | The harness's cardinality read-back lists all 1,000,000 series through `/api/v1/series`, a 133,231,002-byte gRPC message on both runs of 2026-11-03 (#2821); at the default that answers 500 `grpc: trying to send message larger than max`. 512 MiB is about four times that. In `-target=all` the response crosses a server ceiling and client ceilings, and a scaled check (30,000 series, ceilings at 1 MiB) failed with either side left small, so every pair is raised | `-server.grpc-max-send-msg-size-bytes int  Limit on the size of a gRPC message this server can send (bytes). (default 104857600)`; each client pair: `gRPC client max receive message size (bytes). (default 104857600)` |
| Ravel | `--max-ingest-lag` | `45d` | `2h` | Raised: history's 30 d span plus replay time, equal to the 45 d retention on Prometheus and VictoriaMetrics | `--max-ingest-lag <DURATION> ... One flag drives BOTH the OTLP admission bound ... AND the catalog listing window ... (default: 2h)` |
| Ravel | future-skew bound | `10m` | `10m` (unchanged, no flag) | Kept: a replay of past event time does not reach it | `crates/ravel-otlp/src/limits.rs`, `max_future_skew_ns: 10 * MINUTE_NANOS` |
| Ravel | `max_active_series` (config/ravel-limits.toml) | `"unlimited"` | `200000` | Raised: the cardinality profile writes 1,000,000 series; parity with Mimir's `max_global_series_per_user: 0` | `docs/guides/admission-limits.md`: `Active-series cap (metrics) \| 200,000 \| max_active_series` |
| Ravel | `ingest_bytes_per_sec` (config/ravel-limits.toml) | `"unlimited"` | `33554432` (32 MiB/s, 64 MiB burst) | Raised: the replay sends as fast as the receiver acks, so a byte-rate cap measures the cap | `docs/guides/admission-limits.md`: `Ingest byte rate / burst \| 32 MiB/s / 64 MiB` |
| Ravel | `series_creation_rate_per_sec` (config/ravel-limits.toml) | `"unlimited"` | `10000` (100,000 burst) | Raised: the cardinality profile's first timestamp creates 1,000,000 series, ten times the burst | `docs/guides/admission-limits.md`: `Series-creation rate / burst \| 10,000/s / 100,000` |
| Ravel | `max_active_streams` (config/ravel-limits.toml) | `200000` | `200000` (unchanged) | Kept: the lane writes no logs | `docs/guides/admission-limits.md`: `Active-stream cap (logs) \| 200,000` |
| Ravel | `max_bytes_scanned` (config/ravel-limits.toml) | `"unlimited"` | `"unlimited"` (unchanged) | Kept: no comparator has a scanned-byte budget | `docs/guides/admission-limits.md`: `Query bytes-scanned budget \| unlimited` |
| Ravel | `--gc-max-query-duration` (query deadline) | `120s` | `11m` (derived) | Lowered to Prometheus' `--query.timeout`; a hit is refused(budget) | `--gc-max-query-duration <DURATION>  This process's query-engine deadline ... Default when unset: derived (host-independent): 11m` |
| Ravel | `--max-segments` | `1000000` | `1000000` (derived, unchanged) | Kept: 4 shards at this row's 500 ms flush age write at most 8 objects per second of ingest, so 1,000,000 needs 125,000 s (34.7 h) of replay before compaction; a hit is refused(budget) | `--max-segments <N> ... Default when unset: derived (host-independent): 1,000,000` |
| Ravel | `--max-s3-requests` | `1527800` | `1527800` (derived at this row's 500 ms flush delay, unchanged; `385700` at the shipped 2 s) | Kept: at one GET per object and 8 objects per second, a read over the whole uncompacted replay needs 190,975 s (53.0 h) of ingest to reach it, and `--max-segments` refuses first, at 125,000 s. Above about 1.53 requests per object (1,527,800 / 1,000,000), as range reads give, this cap refuses first instead; a hit is refused(budget). `services/ravel-server/tests/metricsbench_request_budget.rs` fails if this value, `ravel.yml`'s and the server's derivation from `ravel.yml`'s command disagree | `--max-s3-requests <COUNT> ... Omitted (the default): the cap is DERIVED from` the shard count, `the ingest flush cadence, and the seal margin`; 1,527,800 at 4 shards and 500 ms is pinned in `crates/ravel-query/src/segment_admission.rs`, `derived_budget_covers_healthy_tail_plus_stall_alert_window` |
| Ravel | `--max-series` | `1000000` | `10000` | Raised (#3306): the cardinality profile's 1,000,000 active series, equal to VictoriaMetrics' `-search.maxUniqueTimeseries`; the widest corpus union is 700,000 series. At the default, 15 of the 21 cardinality entries and 12 of the 22 churn entries would be refused | `--max-series <N>  Cap on the distinct series a single PromQL query may materialize ... Default when unset: ravel_query::DEFAULT_MAX_SERIES (10,000)` |
| Ravel | `--max-samples` | `500000000` | `10000000` | Raised (#3306) to Prometheus' and Mimir's value. Ravel counts total materialized samples after dedup, where Prometheus counts peak resident samples, so the equal numbers are not equal semantics; the comparator counting the same quantity is VictoriaMetrics' `-search.maxSamplesPerQuery`, at twice this value. No corpus entry comes near either: the largest read is 96,000,000. The bytes behind them are bounded by `--memory-budget-bytes`, which refuses an oversized materialization `memory exhausted` first. The flag also sizes the range selector-cache cap (2 x max_samples x 16 bytes): 16,000,000,000 bytes here, against 320,000,000 at the default, so that cap is inert and the memory budget alone bounds a range query's cached selector copies. At the default, 5 history entries would be refused, the largest `mb_long_range_7d_avg` at about 40.3M | `--max-samples <N>  Cap on the total samples (summed across series, after cross-segment dedup) a single PromQL query may materialize ... Default when unset: ravel_query::DEFAULT_MAX_SAMPLES (10,000,000)` |
| Ravel | points per range series | `11000` | `11000` (unchanged, no flag) | Kept, same as Prometheus and Mimir: history's grid is 289 points | `crates/ravel-promql/src/eval.rs`, `DEFAULT_MAX_RANGE_POINTS: usize = 11_000` |
| Ravel | total evaluation points per query | `1000000` | `1000000` (unchanged, no flag) | Kept: `mb_long_range_subquery_max` (`[1d:5m]`, 289 points) over the 289-point grid is 83,521 | `crates/ravel-promql/src/eval.rs`, `DEFAULT_MAX_TOTAL_EVAL_POINTS: u64 = 1_000_000` |
| Prometheus, VictoriaMetrics, Mimir | `GOMEMLIMIT` (docker-compose.yml `environment`) | `7GiB` (7,516,192,768 bytes, 87.5% of the 8g envelope) | unset | The Go runtime's soft memory limit (ADR-0927 amendment 2026-11-05). Unset, the runtime collects only when the heap has grown GOGC percent (default 100, left unset here) over the last live heap, with no knowledge of the cgroup limit; with it, the runtime collects before the cgroup limit instead of overshooting it. It bounds heap overshoot, not the live set. The limit counts all memory the Go runtime maps and has not released, goroutine stacks and other non-heap runtime memory included; the 1 GiB left covers what the cgroup charges outside the runtime: page cache charged to the container, files mapped with `mmap`, kernel memory held for the process, and the binary itself. The envelope (8g, 2 CPUs) is unchanged. `capture-effective-config.sh` records it per run as `go_runtime`, and `tests/run.test.sh` case o2 pins each service's value in `docker-compose.yml` (and that none sets `GOGC`) | Go `runtime/debug.SetMemoryLimit` documentation: "The initial setting is math.MaxInt64 unless the GOMEMLIMIT environment variable is set"; the limit "includes all memory mapped, managed, and not released by the Go runtime" and excludes "OS kernel memory held on behalf of the process, memory allocated by C code, and memory mapped by syscall.Mmap" |
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
4 GiB so an oversized query is refused with
`err-mimir-max-estimated-memory-consumption-per-query` (a `refused(budget)`
outcome, matched by the signature table below) instead of exhausting the 8g
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
comparator, Ravel included, is given identical CPU (2.0) and memory (8g)
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
- Prometheus, VictoriaMetrics and Mimir, the Go comparators: `go_runtime` is
  `GOMEMLIMIT` read from the container's `.Config.Env` through `docker
  inspect`, as `{"source": "docker inspect .Config.Env", "GOMEMLIMIT":
  "7GiB"}`. The value is `"unset"` when the container's environment has no
  `GOMEMLIMIT`, and the field is `{"unavailable": "<reason>"}` when docker is
  not reachable or the inspect fails. Neither counts toward exit 1.
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
    "prometheus": {"version": "3.13.1", "revision": "...", "runtime_flags": {"...": "..."}, "config_settings": {"storage.tsdb.out_of_order_time_window": "1h"}, "go_runtime": {"source": "docker inspect .Config.Env", "GOMEMLIMIT": "7GiB"}, "image": {"...": "..."}},
    "victoriametrics": {"version": "v1.115.0", "revision": "victoria-metrics-...-tags-v1.115.0-0-g...", "runtime_flags": {"...": "..."}, "go_runtime": {"...": "..."}, "image": {"...": "..."}},
    "mimir": {
      "version": "3.2.3",
      "revision": "...",
      "runtime_flags": {"format": "yaml", "effective_config": "..."},
      "querier_query_engine": "mimir",
      "query_engine_default_for_version": "mimir",
      "go_runtime": {"source": "docker inspect .Config.Env", "GOMEMLIMIT": "7GiB"},
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
| `--bin-dir DIR` | Holds `metricsbench_ingest`, `metricsbench_query` and `metricsbench_report`, and `metricsbench_holders` with `MB_STAGE1=1`. A missing one is refused with exit 65 (case h). |
| `--out-dir DIR` | Created if absent. One that already holds `stamps.json`, `report.json`, `ingest.json` or `falsification.json` is refused with exit 65 (case h). |
| `--anchor-ms N` | The replay anchor, which must be on the profile's scrape grid (exit 65 otherwise, case h). Default: the profile's window ending at least 10 minutes before the run, on the grid, stamped `anchor_source` as from the clock. Ravel drops samples older than its 45-day `--max-ingest-lag`, so a fixed old anchor fails Ravel's read-back. |
| `--passes N`, `--reps N` | Passed to `metricsbench_query` as `--passes` and `--reps-per-pass` and stamped (case g); its own defaults (5 and 3) otherwise. A timed pass stops repeating a statement for an engine once that engine's repetition times out, and lists the repetitions left out in the diagnostics sidecar's `skipped_reps` (ADR-0927, 2026-11-06 amendment). |
| `--keep` | Leave the stack up; stamps say `kept (--keep)` and no `down -v` runs (case g). |
| `--rss-band CONTAINER:MIN:MAX` | Repeatable. The peak sampled memory of the container (matched by container name or compose service) must lie in `[MIN, MAX]` bytes; outside it, or with no samples, the `memory` step fails: a full run still runs steps 15 to 18 and then exits 75 (case d; step 14 lists the other exits), and the diagnostic mode exits 75 at once (case e3). |
| `--ravel-objects-band MIN:MAX` | The band the `objects` step checks the count of Ravel's data objects (`t/<tenant>/m/l0/*.rseg` keys) against, inclusive (cases v1, v2). It counts data objects only, not commit records, probes or the whole bucket, so the same figure is comparable between s3s-fs and real S3. A value not of the form `MIN:MAX`, a minimum above the maximum (case v3), or the flag with the falsification run (case v4) exits 64. |
| `--diagnostic-prometheus-max-samples N` | The falsification run, below. |
| `--folded-catalog-get-max-fraction F` | The most catalog GETs the folded read-back (step 18) may spend per data object the `objects` step listed, on top of the floor below, a decimal from 0 to 1; default 0.05 (case u), stamped (case u16). A malformed or out-of-range value exits 64 (cases u17 to u19). |
| `--folded-catalog-get-floor N` | The fixed part of the folded read-back's catalog GET limit (step 18), a whole number; default 64, stamped (cases u, u20, u23). A value that is not a whole number exits 64 (cases u24 to u27). |
| `--real-s3-bucket-prefix PREFIX`, `--real-s3-region REGION` | The real-S3 lane, below. One without the other, a prefix S3 cannot name or longer than 44 characters, a malformed region, or either with the falsification run exits 64 (cases s2 to s7). A local build is refused with exit 65 (case t). |

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
   ingest (cases q, q2). A later step's failure is listed in `stamps.json`
   as `failed_after_ingest` and the exit stays 70 naming ingest (cases q3,
   u6). A failed step ends the run there, so the steps after it do not run,
   except a failed `memory` step: steps 15 to 18 still run after it, and
   `memory` is then listed in `failed_after_ingest` (case q3). When the
   failed row is the Prometheus
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
8. `objects`: Ravel's bucket is listed with `aws s3api list-objects-v2`,
   run as `docker compose run --rm --no-deps -T --entrypoint aws
   createbuckets`, so each lane lists with its own endpoint and credentials:
   `ravel-metricsbench` through `--endpoint-url http://s3s:9000` on the
   portable stack (case v), `<PREFIX>-ravel` with no endpoint override in
   the real-S3 lane (case v8), where `real-s3.yml` leaves `createbuckets`
   the instance role's credentials. No `--no-paginate` or `--max-items` is
   passed, so the aws CLI follows every continuation token itself (its own
   default, which the stubbed cases cannot show); a listing that still
   carries a token fails the step with exit 70, as does a listing that
   exits non-zero (cases v7, v6).
   The listing is kept as `ravel-objects-listing.json`, and its keys are
   counted, and their sizes summed, by kind into `ravel-objects.json`, the
   same figures stamped as `ravel_objects`:
   `data` (`t/<tenant>/m/l0/*.rseg`), `commit` (`t/<tenant>/m/c/`),
   `qualify` (`sys/qualify/`, the store-qualify probes), `other` (the
   catalog and bookkeeping keys) and `total`, every key once (case v). The
   step fails when the kinds do not sum to the total; no listing can make
   that happen, so no case drives it. With `--ravel-objects-band` the data
   count must lie in the band; outside it the step fails with exit 70
   naming `objects` (cases v1, v2). Without the flag the counts are stamped
   only and the band is recorded as not set (case v). After a failed ingest
   verification the step still runs (case q), and a band breach is listed
   in `failed_after_ingest` (case v5).
9. `capture`: `capture-effective-config.sh` into `effective-config.json`,
   which must exit 0; a partial capture (exit 1) fails the step with exit
   70 before any query runs (case k).
10. `provenance`: `metricsbench_report provenance` into `provenance.json`,
    pinning Prometheus, VictoriaMetrics and Mimir at the version and digest
    the capture recorded (case a). Its `ravel_git_commit` is
    `--ravel-source-commit`, not this checkout.
11. `fold-state`: `ravel_catalog_fold_last_success_timestamp_seconds` from
    Ravel's `/metrics` (case a), or `absent` (case j). That scrape is kept
    as `ravel-metrics-at-query.txt`, and Mimir's `/metrics` is scraped
    beside it into `mimir-metrics-at-query.txt` (case w).
12. `query`: `metricsbench_query` against all four engines into
    `report.json`, with `--diagnostics diagnostics.json` for the per-entry
    ULP gaps against the oracle and every oracle retry (case a; the
    sidecar's fields are in benchmarks/metrics/README.md, "Portable query
    run"). After a failed ingest verification the run passes
    `--diagnostic-after-failed-ingest`, and `report.json` carries
    `run_verdict: "failed"` and `failed_bands` (case q). When the failed row
    is the Prometheus oracle's, `metricsbench_query` refuses instead, writes
    no report, and the run ends here (case q4).
    Every `ravel_cost` record in the sidecar carries `pass_label`
    `background-folded` and `fold_state`
    `ravel_catalog_fold_last_success_timestamp_seconds=<value>` when step 11
    read the gauge (case a), and `unfolded` with `absent: no
    ravel_catalog_fold_last_success_timestamp_seconds` when it did not (case
    j), and `unknown` with the scrape error when step 11's scrape failed
    (case z19). The record's fields are under "Stage 1" below.
    Once `metricsbench_query` has written the report, and before `validate`,
    `memory` or `stop`, Ravel's and Mimir's `/metrics` are scraped again
    into `ravel-metrics-after-query.txt` and `mimir-metrics-after-query.txt`
    (case w). These four scrapes are ADR-0927's 2026-11-04 cache-state
    disclosure: each engine's cache and object-store figures over the query
    phase are its after scrape less its before scrape. The outcomes are
    stamped as `query_scrapes`, below. A failed scrape (connection refused,
    a status other than 200, an empty body) is stamped and never fails the
    run, and leaves no `mimir-metrics-at-query.txt`,
    `ravel-metrics-after-query.txt` or `mimir-metrics-after-query.txt`
    (cases w1 to w3); a run with an `--rss-band` breach and a failed Mimir
    scrape exits 75 as without the failed scrape (case w4). `ravel-metrics-at-query.txt` keeps whatever Ravel answered,
    since step 11 and `readback-folded` read it. A run that ends at `query`
    takes no after pair, and both are stamped `not taken: the query step did
    not complete` (case q4).
13. `validate`: `metricsbench_report validate report.json` exits 0 (case a).
    After a failed ingest verification it must refuse the report, naming it
    a diagnostic of a failed run; a validate that accepts it fails the run
    (case q).
14. `memory`: the sampler stops, `memory-summary.json` records peak and
    final bytes per container and `OOMKilled` from `docker inspect` (case a),
    and every `--rss-band` is checked (case d). A breach (case d) or an
    OOM-killed container (case e) fails the step, but no longer prevents
    steps 15 to 18: the failure is listed in `stamps.json` as
    `deferred_failures`, the run says the memory step failed and goes on,
    and once step 18 is done it exits 75 with `failed_step` memory
    (cases d, e). After a failed ingest verification it exits 70 naming
    ingest instead, with `memory` in `failed_after_ingest`, whether steps
    15 to 18 ran (case q3) or were skipped (case e4). A step among 15 to 18
    that fails after it ends the run as it would without the memory
    failure, so in a run whose ingest passed that exit, 70 naming the
    step, wins over 75; `memory` stays in `deferred_failures` and the
    message names both failures (case e2). The diagnostic mode has no
    folded steps and exits 75 at once (case e3).

    After `memory`, once the sampler has stopped and `memory-summary.json`
    and the verdict are written, and before `stop`, a Ravel-only diagnostic
    cold pass runs, recorded under the regime `cold-ravel-diagnostic` (case
    y pins the order validate, memory, cold pass, stop, and that no memory
    sample is taken after the pass's first restart). Its restarts and
    queries therefore never enter an `--rss-band` peak, `final_bytes` or the
    `OOMKilled` flags. The entries are read from the corpus file
    (`$METRICSBENCH_CORPUS`, else `benchmarks/metrics/metrics.corpus.json`;
    the path used is stamped as `corpus`, cases x and y): every entry of
    class `long_range`, plus `mb_full_range_avg_over_time`, that is declared
    for the profile (no `profiles` list means every profile). A profile that
    selects none, `ci` with the shipped corpus, stamps
    `cold_ravel_diagnostic` `not applicable` with the corpus path and
    restarts nothing (cases x, y3); a corpus the pass cannot load is stamped
    `failed: ...` instead (case y7). For each entry, five times,
    `docker compose restart --no-deps ravel-server` runs (`--no-deps` keeps
    the restart to ravel-server alone; without it compose also restarts
    ravel-qualify, createbuckets and s3s, and the cold figure would include an
    object-store restart), Ravel answers 200 on
    `/api/v1/status/buildinfo` again, and `metricsbench_query` runs once
    (`--warmup-passes 0 --passes 1 --reps-per-pass 1`) against a corpus file
    holding that entry alone, with Prometheus as the oracle it requires and
    Ravel as the other engine; every cold query follows its own restart
    (case y). Each run writes its report and diagnostics under
    `cold-ravel-diagnostic/`, never to `report.json`, so no cold row joins
    the timed denominators (case y). `cold-ravel-diagnostic.json` holds
    `regime`, `comparable: false`, the corpus path and a note saying it is
    not comparable and that each repetition also queries the Prometheus
    oracle, and exactly one record per entry and repetition with its
    `status`, `latency_ms` and `ravel_cost` record (`accounting` and
    `phases`, the shape `diagnostics.json` carries) (case y). A failed
    restart (case y4), readiness wait (case y5) or query run (case y1) is
    recorded in that run's `status`, counted in the `cold_ravel_diagnostic`
    stamp's `failed_runs`, and does not fail the run. After the first
    readiness wait that times out, the remaining repetitions of every entry
    are recorded as `failed: skipped after ravel did not answer 200
    following <entry> repetition <n>` without another restart or wait, so a
    server that does not come back costs one wait, not one per repetition
    (case y5). A run whose record was
    never written gets a record with status `failed: no record was written
    for this run`, is counted in `failed_runs`, and is named by entry and
    repetition in the stamp's `missing_runs` (case y6). After a failed
    ingest verification the pass does not run and is stamped `not
    applicable` (case y2); a run that ends before the pass leaves it stamped
    `not taken: the run ended before the cold pass` (case b). This pass does
    not decide the cross-engine cold and warm procedure, which issue #3794
    (M9) owns. The provenance each cold run's `metricsbench_query` writes
    still says `regime: steady-state`, because the bin sets that key itself;
    the regime and `comparable: false` are in `cold-ravel-diagnostic.json`,
    in the stamp, and as `diagnostic_regime` and `comparable` config entries
    of the per-entry provenance the pass hands `metricsbench_query`.
15. `stop`: `docker compose stop ravel-server` only (case u). The fold below
    runs with `--writers-stopped`, which ADR-2677 decision 1 gives to "an
    operator whose writer has exited" and marks "UNSAFE under a live
    writer" (docs/adrs/2677-clickbench-floor-fetch-and-pruning.md:90-100):
    a commit record landing in an hour the fold has sealed lands in a
    bucket reconcile skips, invisible to non-token queries until a HEAD
    rebuild. The server is Ravel's only writer here, so it stays down
    from this step until `start`; the fold runs while it is stopped (cases
    u, u1). A failed stop exits 70 naming stop, and no fold runs (case
    u2a). `ravel_restart` says it stopped.
16. `fold`: `ravel-cli --store s3 --tenant-hash-unkeyed catalog fold
    --tenant metricsbench --shards 4 --signal metrics --writers-stopped`,
    run as `docker compose run --rm --no-deps -T ravel-qualify ...`, so it
    reads the store `ravel-qualify` carries in either lane: s3s-fs here,
    the real-S3 bucket under `real-s3.yml` (cases u, u10). Every writer has
    stopped by then: the ingest bin has exited and `ravel-server` is
    stopped. Its report goes to `fold.out` (case u). The step fails
    with exit 70 naming fold unless `seal_through_hour` is `Some(N)` (case
    u1) and `watermark_hour` is at or after the ingest hour (Unix seconds
    divided by 3600) the clock read when the ingest step returned (case u2);
    what those two clauses can and cannot catch is under step 18. A failed
    fold ends the run with `ravel-server` still stopped, and `start` does not
    run (cases u1, u2); the teardown removes the stack either way.
    `ravel_catalog_fold_writers_stopped` stamps `seal_through_hour`,
    `watermark_hour`, `entry_count`, `buckets_folded`, `no_op`, that
    `ingest_end_hour` and `precondition` (`met`, or why not) (cases u, u1,
    u2).
17. `start`: `docker compose up -d --no-deps --no-recreate ravel-server`,
    which starts the stopped `ravel-server` container alone, then Ravel's
    `/api/v1/status/buildinfo` answers 200 as in `ready`, before the
    read-back's first scrape (case u). `docker compose start ravel-server`
    would also start its dependencies, rerunning the one-shot
    `createbuckets` and `ravel-qualify`; on the real-S3 lane
    `createbuckets` refuses a bucket that holds objects and exits 1, so the
    step failed there. `--no-recreate` keeps it the same container the run
    measured before the fold. Case u asserts that this is the only compose
    start or up after the fold and that no one-shot service starts again. A failed start exits 70 naming start
    (case u2b). `ravel_restart` says it stopped before the fold and started
    after it (cases u, u1, u2a, u2b).
18. `readback-folded`: Ravel's `/metrics` into
    `ravel-metrics-before-folded-readback.txt`, then `metricsbench_ingest
    --readback-only --ingest-report ingest.json --readback-system ravel`
    with the ingest step's workload, profile, anchor, query URL, token
    variable and `--readback-timeout-secs`, and `--readback-settle-secs 60`,
    the bin's default the ingest step runs with, into `readback-folded.json`,
    then `/metrics` again into `ravel-metrics-after-folded-readback.txt`
    (case u). `readback_folded`
    stamps the bin's `exit_code`, `matches_expected`, `error`, the series
    query's `series_query_ms` (`readback.query_ms.series`), and between the
    two scrapes `store_get_calls_delta` (`ravel_store_calls_total` with
    `op="get"`) and `commit_record_cache_entries_delta`
    (`ravel_catalog_record_cache_entries` with `cache="commit"`) (case u).
    A non-zero exit fails the step with exit 70 naming readback-folded (case
    u3), and so does a scrape that did not answer 200, whose delta is then
    null (case u4).

    The step then checks that the read used the fold. `catalog_get_delta`
    is `store_get_calls_delta` less `fetch_cache_misses_delta`
    (`ravel_cache_misses_total` with `cache="fetch"`) between the same two
    scrapes (cases u, u11), the store GETs that did not fetch data, which a
    read on an unfolded catalog spends resolving one commit record per data
    object. `data_objects_listed` is `data.count` in `ravel-objects.json`,
    the `.rseg` data objects the `objects` step (step 8) listed in Ravel's
    bucket, the count the fraction was calibrated on (cases u, u20, u21).
    The limit is
    `--folded-catalog-get-floor` (default 64) plus
    `--folded-catalog-get-max-fraction` (default 0.05) times
    `data_objects_listed`. Beside it the step stamps a cross-check the
    limit does not use: `data_objects_flushed`, the sum of
    `ravel_ingest_flushes_by_size_total`, `_by_age_total`,
    `_by_age_adaptive_total`, `_by_age_floor_total` and `_by_arrival_total`
    with `signal="metrics"` in `ravel-metrics-at-query.txt`, the fold-state
    scrape taken before the stop. Those five are disjoint: each flush
    open bumps exactly one of them by its trigger
    (`record_flush`, crates/ravel-ingest/src/metrics.rs:956-964, called once
    per flush open at crates/ravel-ingest/src/shard.rs:2662), so sub-counts
    such as `ravel_ingest_rerouted_flushes_total` and other signals'
    samples are not added (case u). They count flush attempts, so an
    abandoned flush counts too; a scrape without them stamps null and does
    not fail the step (case u15). The floor is the part of a folded read's catalog
    cost that does not grow with the object count: the HEAD, the snapshot
    parts, the commit-token keys, and any GET the `--mode all` server's own
    maintenance or fold loop issues between the two scrapes, since
    `ravel_store_calls_total` counts the whole process. The step fails with
    exit 70 naming readback-folded when `catalog_get_delta` exceeds the
    limit (cases u12, u22; a failure after a failed ingest is listed in
    `failed_after_ingest`, case u13), and when either figure cannot be
    read: a scrape without the fetch-miss counter (case u14), or a
    `ravel-objects.json` that is gone (case u15a) or has no `data.count`
    (case u15b).

    A limit at or above `data_objects_listed` cannot tell a folded read
    from an unfolded one, which costs about one catalog GET per data
    object. Then the step neither fails nor reports `within`: the verdict
    is `not discriminating: limit L is at or above the N data objects
    listed, so a folded and an unfolded read cannot be told apart at this
    profile`, and the step passes (cases u16, u20). The ci profile the
    nightly workflow runs lands here: its 2026-11-02 real-S3 smoke flushed
    8 metrics data objects (`out-ci/ravel-metrics-at-query.txt` of run
    `mb2821-r1-20261102T063901Z-aa061edd1`), and its bucket listed the
    same 8 `.rseg` data objects. The cardinality profile does
    not: at its measured 15,248 listed
    data objects the limit is 64 + 0.05 x 15,248 = 826.4, far below the
    about 15,249 an unfolded read spends (cases u21, u22).

    `readback_folded` stamps `data_objects_listed`, `data_objects_flushed`,
    `catalog_get_delta`, `folded_catalog_get_max_fraction`,
    `folded_catalog_get_floor`, the computed `folded_catalog_get_limit` and
    `catalog_get_verdict` (`within`, `not discriminating: ...`, `breach:
    ...` or `could not measure: ...`) before the step fails, so a breach
    keeps its numbers (cases u, u12, u14, u15a, u20).

    The measured basis, from issue #2821 comment 6583379446 and the E8
    run: on a real-S3 run's unfolded read-back the GET delta was 29,029
    and the fetch-cache misses 13,780, leaving 15,249 against 15,248
    listed data objects, one commit record each; on an s3s-fs run after a
    restart, 38,698 less 29,652 is 9,046 against a commit-record cache
    growth of 9,042. An unfolded
    read sits near one catalog GET per data object, twenty times the
    default fraction, and at that object count the floor is under half a
    percent of it.

    After `readback-folded`, `ravel-object-spans.json` records the event-time
    spread of Ravel's data objects, read from the catalog snapshot the fold
    wrote: `ravel-cli catalog list --json` (one JSON document per line, one
    line per resolved metrics segment, L0 and L1, with its level as `L0` or
    `L1`, ingest hour, size and event bounds; the `store:` header goes to
    stderr) and `ravel-cli catalog inspect` (the number of snapshot parts
    the HEAD covers), both run on the `ravel-qualify` service (case x; case
    x5 reads a listing with L1 lines). The file holds
    the object count and bytes, the number of distinct ingest hours, the
    snapshot part count, and a histogram of each object's span (event
    maximum minus event minimum) with the object count and bytes of each
    bucket: under 1 h, 1 to 2 h, 2 to 6 h, 6 to 24 h (24 h included) and over
    24 h. The buckets sum to the listed object count, and the file records
    `ravel_objects_data_count` and `matches_ravel_objects` beside it, since
    `ravel-objects.json` counts only L0 keys; a difference is visible there
    rather than failing the run (case x0 pins the sum for a second count).
    Both are `null` when `ravel-objects.json` cannot be read (case x7).
    The stamp `object_spans` names both commands. A failed `catalog list`
    (case x2) or `catalog inspect` (case x4), a listing line that is not a
    JSON document (case x6), a missing snapshot HEAD (case x1), or a run
    with no fold (the fold's skip, case x3) is stamped `failed: <reason>`
    with no file, and the run still exits 0 where it would have. A run that
    ends before the step (a failed ingest, fold, start or folded read-back)
    leaves it stamped `not taken: the run ended before the object spans
    step` (cases b, u1, u2b, u3).

    `commit_record_cache_entries_delta` is a residency figure, the change
    in an eviction-bounded gauge, and after the stop and start its before
    value is near zero. It is stamped as context and the check does not use
    it.

    Neither of the fold step's two conditions can fail for a
    writers-stopped fold that succeeded. `fold_with_writers_stopped` sets
    `seal_through_hour` to the hour the clock reads
    (services/ravel-cli/src/catalog.rs:136-140), the fold applies that
    ceiling whenever the margin watermark is below it
    (crates/ravel-catalog/src/fold.rs:422-435, 1789-1792), and it moves the
    watermark only forward (fold.rs:1911-1912). The fold runs after the
    ingest step, so `watermark_hour` is at or after the ingest end hour by
    construction. The two clauses (cases u1, u2) guard against a malformed,
    truncated or stale `fold.out`, and against a CLI whose output format
    changed, not against an under-sealed fold. The check with teeth on
    whether the read used the fold is the catalog-GET check above. A no-op
    fold is not a failure, since on a long profile the server's own fold
    loop may already have sealed every hour; `no_op` and `entry_count` are
    stamped in `ravel_catalog_fold_writers_stopped` as context (case u).

Steps 15 to 18 also run after a failed ingest verification when the Ravel
row itself replayed and only its read-back failed or mismatched, which is the
case they measure: the run still exits 70 naming ingest (case u5), and a
failure among them is listed in `failed_after_ingest` (case u6). When the
Ravel row has a `row_failure` or stalled, the four are skipped and each of
their three stamps says `skipped:` and why (cases u7, u8). A failed `memory`
step no longer prevents them: they run after it in the order stop, fold,
start, readback-folded, and the run then exits 75 naming memory (cases d,
e), or 70 naming ingest after a failed ingest verification (case q3); in a
run whose ingest passed, a failure among the four exits 70 naming that step
instead (case e2). A run
that ends earlier, at `query` for a failed oracle row (case q4), does not
reach them. The diagnostic mode runs none of them and stamps each `not
applicable` (case u9).

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
`ravel_catalog_fold_writers_stopped`, `ravel_restart` and `readback_folded`
(steps 15 to 18; cases u, u7 and u9), `object_spans` (the object
event-time spread capture, after step 18; `not taken: ...` until it runs;
`not applicable` in the diagnostic mode; cases x to x7, b, u1, u2b, u3 and
f), `cold_ravel_diagnostic` (the cold pass after step 14; `not taken: ...`
until it runs; cases y to y7, b and f), `corpus` (the corpus file the run
used, `$METRICSBENCH_CORPUS` or the shipped one; cases x and y),
`query_scrapes` (an object with
`ravel_before`, `mimir_before`, `ravel_after` and `mimir_after`, each `ok`,
`failed: curl exit N`, `failed: HTTP N`, `failed: empty body` or `not taken:
...`, step 12; cases w, w1 to w3 and q4; `not applicable` in the diagnostic
mode, case f; `null` when the run ended before step 11), `mimir_readiness`, `ravel_objects`
(the contents of `ravel-objects.json`, case v; `not applicable` in the
diagnostic mode, case f), `in_process_row` (`present` or `skipped`, cases a, l and
f), `object_store` (release, asset arch, engine arch and its source, sha256,
URL, cached path and whether it came from the cache or a download; `not applicable` in the diagnostic mode;
cases a, f, n1, n5 and n7), `steps_completed`, `failed_step`,
`failed_after_ingest` (cases a, q and q3), `exit_code`, `status` and
`teardown`, and `stage1` (below; `{"enabled": false}` without `MB_STAGE1`,
case a).

### Stage 1 (`MB_STAGE1=1`)

ADR-3694's Stage 1 run (issue #3968) measures what the metrics label index
buys a folded catalog: the same corpus read once with key-index leaves and
no `label_value` leaf, and once with them, on one tenant, one binary and one
box. `MB_STAGE1=1` in the environment adds those steps to a run; `0` or unset
leaves the run as above, any other value exits 64 (cases z4, z6). It is
accepted with `--profile cardinality` and `--profile ci` only (another
profile exits 65 before anything starts, case z3) and not with
`--diagnostic-prometheus-max-samples` (exit 64, case z5).

**Server flags.** `ravel.yml` sets the server's entrypoint to
`/usr/local/bin/ravel-server ${RAVEL_SERVER_EXTRA_FLAGS:-}`, the image's own
`ENTRYPOINT` plus one variable, so the command line is the one above when the
variable is empty, which it is without `MB_STAGE1` (cases a and z0; z0
renders the file with `docker compose config` where the host has compose).
With `MB_STAGE1=1`, `up` runs with it set to `--fold-key-index-leaves`, so
ingest and every step before Stage 1 run with key-index leaves and without
`--fold-metrics-label-index` (case z). The main `report.json`'s provenance
then carries `comparable=false` and a `comparability_reason` naming those
flags (case z; case a pins that a run without `MB_STAGE1` carries neither).

**Steps.** After `readback-folded` (step 18), and only when it ran, these
run in this order (case z pins the order and each command's flags); a
failure ends the run with exit 70 naming the step. Every ravel-server
recreate below is `docker compose up -d --no-deps --force-recreate
ravel-server` with `RAVEL_SERVER_EXTRA_FLAGS` set as named, then
`/api/v1/status/buildinfo` answering 200 as in `ready` (exit 70 naming the
step when it does not: cases z8a, z8b and z8). Each recreate passes
`--disable-fold`, so from `stage1-fold` onward no background fold runs and
only `stage1-fold` and `stage1-leaves` change the snapshot;
`stage1.background_fold` stamps that.

1. `stage1-fold`: ravel-server recreated with `--disable-fold
   --fold-key-index-leaves` (stamped as `stage1.fold_restart`), then
   `ravel-cli catalog fold --signal metrics --json` on `ravel-qualify` with
   no leaf flag and no `--writers-stopped`, into `stage1-fold.json`.
   `stage1.fold` stamps its `wall_ms`, `watermark_hour`,
   `previous_watermark_hour`, `no_op`, `entry_count`, `buckets_folded`, the
   LIST, GET and PUT request counts and the key-index leaves and leaf bytes
   it wrote.
2. `stage1-unrouted`: ravel-server recreated with the same flags (stamped as
   `stage1.unrouted_restart`). Then `ravel-cli catalog inspect --signal
   metrics` into `stage1-unrouted.inspect.txt`, stamped as
   `stage1.unrouted_catalog`: the snapshot HEAD's `watermark_hour`, its
   `parts`, and `label_value_leaf_parts` and `label_value_leaf_refs` (the
   parts carrying a `label_value` key-index leaf ref, and those refs), all
   read from the HEAD object's own fields, with that source named. The pass
   is refused (exit 70 naming the step) when the text does not read as one
   HEAD whose part list matches its `parts` line (case z14) or when any part
   carries a `label_value` leaf (case z12). Then `metricsbench_query`
   against Prometheus and Ravel with the full corpus and
   `--passes`/`--reps`, `--pass-label folded-unrouted` and `--fold-state
   "folded: watermark_hour=W; parts=P; label_value leaf parts=L;
   ravel-server FLAGS"` with those figures and the server's flags, into
   `stage1-unrouted/` (`report.json`, `diagnostics.json`, and a
   `provenance.json` with `diagnostic_regime=stage1-folded-unrouted` and
   `comparable=false`). Then the cold pass's selection (as
   `cold_ravel_diagnostic` picks it; none on `ci`, so
   `stage1.unrouted_cold` says `not applicable`, case z1) cold, into
   `stage1-unrouted/cold.json`; a cold failure is stamped, not fatal.
   `stage1.unrouted_cold_records` then stamps the cold pass's record count:
   `records` (the runs with a `ravel_cost`, its `runs` less `failed_runs`),
   `expected_records` (its entries times `repetitions_per_entry`) and
   `records_short` (the difference), all 0 with `applicable` false when the
   cold stamp says `not applicable`; a cold stamp that failed leaves this
   stamp a `failed:` string instead, so its band reads it as missing.
3. `stage1-leaves`: `ravel-cli catalog fold --rebuild-key-index --signal
   metrics --metrics-label-index --json`, into `stage1-leaves.json`.
   `stage1.leaves` stamps `wall_ms` and the report's `leaves_written`,
   `leaf_bytes_written`, `leaf_prefix_bytes_read`, `section_bytes_read`,
   `attempt_count`, its part counts (`parts_unindexed`, `parts_rebuilt`,
   `parts_deferred`, `parts_over_cap`, `parts_erasure_gated`) and
   `uncovered_entries`, and under `requests` every request count the report
   gives, each by its own name: `data_object_ranged_gets`,
   `leaf_prefix_gets`, `erasure_list_requests`, `erasure_get_requests`, and
   `part_object_gets`, `leaf_puts` and `head_cas_puts` summed over the
   report's attempts. The report counts no GET of the snapshot HEAD, and
   `requests.not_reported` says so. Exit 70 on a failed rebuild (case z7).
4. `stage1-restart`: ravel-server recreated with `--disable-fold
   --fold-key-index-leaves --fold-metrics-label-index` (stamped as
   `stage1.restart`).
5. `stage1-routed`: the inspect again, into `stage1-routed.inspect.txt` and
   `stage1.routed_catalog`, refused when fewer parts carry a `label_value`
   leaf than `stage1-leaves` rebuilt (`parts_rebuilt`, case z13), and
   refused naming the figure when `parts_rebuilt` is not a whole number
   (case z24); then the passes as in step 2 with `--pass-label
   folded-routed`, into `stage1-routed/`, and `stage1.routed_cold_records`.
6. The two steady passes are compared into `stage1-compare.json`, stamped as
   `stage1.compare`: per corpus entry the set of `result_hash` values each
   pass's records carry, `entries_compared`, `entries_equal`,
   `entries_differing_count` with the differing entries and their hashes,
   and `entries_without_hash_count` with those entries. ADR-3694 row 9 is
   checked by status (`status_check` states the rule): per entry the set of
   `status` values each pass's records carry is compared, and
   `statements_status_differing` lists each entry whose two sets differ,
   with both sets, beside its count (an entry `incorrect` in both passes
   is not listed, case z21; one `ok` unrouted and `incorrect` routed is, case
   z22). Under `passes`, for `unrouted` and `routed` each: `records`,
   `records_answered` (status `ok` or `incorrect`, the two the oracle
   answers), `records_answered_null_hash` (an answered record with no
   `result_hash`), `records_status_unexpected` (any other status: `partial`,
   `timeout`, `error`, `unsupported_construct`, `refused` or a failure
   string), `records_label_index_null` (no whole-number
   `segmentsPrunedByLabelIndex`, counted apart and never as 0),
   `records_pruned_by_label_index` (one above 0) and
   `segments_pruned_by_label_index` (their sum over the records that carry
   one). A difference is recorded and the run still exits 0 (cases z2, z15,
   z16 and z22); the band file below is the verdict.
7. `stage1-holders`: `ravel-cli catalog list --json` lists the L0 and L1
   data objects the folded snapshot resolves into `stage1-objects.jsonl`;
   the bucket's L0 and L1 data objects (`t/*/m/l0/*.rseg`,
   `t/*/m/l1/*.rseg`) are downloaded with `aws s3 sync` on the
   `createbuckets` service into `stage1-objects/`; `metricsbench_holders`
   counts the listed ones into `holders.json`; and the download is deleted.
   On the real-S3 lane the bucket is `<PREFIX>-ravel` (case z11). Exit 70 on
   a failed listing, download or count, or a stamp that cannot read the
   count; on a failed download no count runs (case z10). `stage1.holders` stamps
   the bucket, the method, `objects_listed`, `objects_above_watermark`
   (listed objects whose ingest hour is after the unrouted snapshot's
   `watermark_hour`), `objects_l0`, `objects_l1`, `series_total`, per
   statement `segments_in_window`, `named_in_window`, `name_pruned`,
   `unrouted_fetched_expected`, `holders`, `route_bound` and
   `route_bound_reason`, the skipped count, the window check and the
   route-bound list. The window check compares the `segmentsFetched` of
   every one of a statement's `stage1-unrouted` records with its
   `unrouted_fetched_expected` (see holders.json below): one that differs is
   listed under `window_mismatches` with both figures, `named_in_window`,
   `segments_in_window` and the fetched values (case z17); an object in the
   window that holds none of the statement's metric, and a metric no object
   holds (0 expected, 0 fetched), match (case z20). A statement with no such
   record, a record with no whole-number `segmentsFetched`, or no
   whole-number expected figure is listed under `window_unchecked` (case
   z18), each with its count. `route_unbound` lists each statement whose
   `route_bound` is false with its reason (case z23).

A run whose ingest verification failed, or whose steps 15 to 18 were
skipped, runs none of them and stamps each `skipped:` and why (case z9).

**Record fields.** Every `ravel_cost` record (`diagnostics.json` of the query
step and of both Stage 1 passes) carries `entry`, `pass` (the timed pass
number, from 1), `rep`, `status`, `latency_ms`, and these copied from the
answer's `data.stats`, each `null` (never 0) when the answer has no such
member: `accounting`, `phases`, `segmentsFetched`, `segmentsPruned`,
`segmentsPrunedByDictionary`, `segmentsPrunedByLabelIndex`, `estimate` and
`io`. Then `pass_label` and `fold_state` as the run passed them, and
`result_hash`, `blake3:` and the hash of the answer's `resultType` and
`result` with the series sorted, so two answers with the same series hash
equal whatever order the engine listed them in, and every value hashed by
its bits (`null` for an answer that is not a successful envelope; the
encoding is in benchmarks/metrics/README.md, "Diagnostics sidecar"). For
example (values illustrative):

```json
{"entry": "mb_selective_job_rate_sum", "pass": 1, "rep": 2, "status": "ok",
 "latency_ms": 41.7, "accounting": {...}, "phases": {...},
 "segmentsFetched": 4, "segmentsPruned": 6, "segmentsPrunedByDictionary": 3,
 "segmentsPrunedByLabelIndex": 3, "estimate": null, "io": null,
 "pass_label": "folded-routed",
 "fold_state": "folded: watermark_hour=490001; parts=4; label_value leaf parts=4; ravel-server --disable-fold --fold-key-index-leaves --fold-metrics-label-index",
 "result_hash": "blake3:..."}
```

Outside Stage 1, the query step's records carry `pass_label` `unfolded`
while ravel-server has published no fold gauge, else `background-folded`,
and `fold_state` names the gauge stamped as `ravel_catalog_fold`. When the
`/metrics` scrape before the query step fails, they carry `pass_label`
`unknown`, and `fold_state`, like the `ravel_catalog_fold` stamp, says
`unknown` with the scrape error (case z19).

`readback-folded.json` also records, per read-back query (`series` and each
`samples.N`), `readback.query_cost`: `stats`, the answers' `data.stats`,
one per window (empty for `series`, whose answer has none); `phases`, per
phase (`resolve`, `plan`, `probe`, `scan`, `keyIndex`) the sum over those
answers of `s3_get_requests`, `s3_get_wire_bytes` and `s3_list_requests`
as the server counts them (`resolve` is the catalog's share; `null` for
`series`, and when any answer lacks a phase list or a figure, never a
partial sum); and the receiver's process-wide counters across the query,
which count every request the process served in that interval:
`process_store_get_delta` (`ravel_store_calls_total{op="get"}`),
`process_fetch_miss_delta` (`ravel_cache_misses_total{cache="fetch"}`) and
`process_catalog_get_delta` (their difference), each `null` with
`process_error` saying why when a scrape failed, lacked the family, or a
counter went down across the query. `run.sh` passes Ravel's `/metrics` URL
to the folded read-back for this with or without `MB_STAGE1`.

**holders.json.** How many data objects in each selective statement's fetch
window hold a series it asks for, counted without the router or the key
index. `segments_in_window` is every object whose event time range overlaps
the statement's fetch window, whatever metrics it holds; `named_in_window`
the ones among them holding a series of one of the statement's metric
names; `holders` the ones among those with a series matching a qualifying
selector in full. When every selector names the same metric
(`name_pruned`), Ravel's unrouted resolve prunes by that name's postings and
fetches `named_in_window` objects; when the selectors name different
metrics it does not prune by name and fetches `segments_in_window`. That
figure is `unrouted_fetched_expected`, and the run checks it against the
`segmentsFetched` the unrouted pass reported (step 7). ADR-3694 row 3's
holders-to-fetched ratio takes that unrouted `segmentsFetched` as its
denominator, not `segments_in_window`. A statement with a selector that has
no `label="value"` matcher with a non-empty value is `route_bound` false:
the router keeps every object holding that selector's metric, so `holders`
is no bound on what the routed pass opens, and the statement is left out of
every holders-versus-routed comparison. The method,
as `metricsbench_holders` writes it into the file and `stage1.holders.method`
stamps it:

> the data objects the folded snapshot names (L0 and L1, from the catalog's
> resolve) were downloaded and each one's full series catalog decoded with
> ravel-segment's public reader (open_from_full + decode_catalog_v5),
> without the router or key index; a statement qualifies when a selector has
> a label="value" matcher with a non-empty value; its fetch window is the
> union over its metric selectors of the evaluation instant (instant) or the
> grid from start to the last grid point (range), moved back by the
> selector's offset or pinned by its @, padded back by the selector's range
> (5m lookback for a vector selector, the matrix range, plus enclosing
> subquery ranges), as Ravel's engine resolves it; segments_in_window counts
> the objects whose event time range overlaps that window (inclusive)
> whatever metrics they hold, named_in_window the ones among them with a
> series of one of the statement's metric names, and holders the ones among
> them with a series matching a qualifying selector in full (metric name and
> every matcher); unrouted_fetched_expected is named_in_window when every
> selector names the same metric (Ravel's resolve then prunes by that
> name's postings) and segments_in_window otherwise; route_bound is false
> when a selector has no qualifying matcher, since the router then keeps
> every object holding that selector's metric

The file holds `method`, `profile`, `objects_l0`, `objects_l1`,
`series_total`, `statements` (per qualifying entry: `id`, `promql`, `eval`,
`selectors`, `positive_equalities`, `metrics`, `name_pruned`,
`fetch_window` with `start_ns` and `end_ns`, `segments_in_window`,
`named_in_window`, `unrouted_fetched_expected`, `holders`, `route_bound`
and `route_bound_reason`) and `skipped` (`id` and `reason` for each entry
that does not qualify).

**Checking a run.** `stage1-check.sh BANDS OUT-DIR` checks a run's figures
against a band file and is the pass/fail for a Stage 1 run.
`stage1-bands.ci.json` is the `ci` profile's:

```json
{"schema": "metricsbench-stage1-bands/1", "profile": "ci",
 "figures": [{"name": "leaves_parts_rebuilt", "file": "stamps.json",
              "path": "stage1.leaves.parts_rebuilt", "min": 1, "max": null,
              "basis": "predicted: ..."}, ...]}
```

`file` is relative to OUT-DIR; `path` is dot-separated keys, and a segment
`key[field=value]` selects the one element of the list under `key` whose
`field` equals `value` (`statements[id=mb_selective_job_rate_sum].holders`).
`min` and `max` are finite numbers or `null` for no bound, at least one set;
`basis` is required. The band file's `profile` must be the run's. Each
figure must be present exactly once and inside its band: a missing file,
key or element is `missing`; a key twice in its object, a selector matching
two elements, or a figure the band file names twice is `duplicated`; a value
that is not a finite number (a bool, a string, `null`, NaN or an infinity)
or lies outside the band is `out of band`. Exit 0 prints `ok NAME = VALUE
in [MIN, MAX] (FILE PATH)` per figure; exit 1 prints each failing figure on
stderr and the count; 64 is bad usage; 65 a band file that cannot be used
(a bound that is not a finite number among them) or is for another profile
(run.test.sh, the `stage1-check` cases). The `ci` bands require at least
one part rebuilt with a `label_value` leaf and no deferred, over-cap or
erasure-gated part and no uncovered entry; no `label_value` leaf part in
the unrouted snapshot and at least one in the routed one; at least one
compared entry, every compared entry's hashes equal and none missing; per
ADR-3694 row 9, no entry whose status set differs between the passes and,
in each pass, no record with a status other than `ok` or `incorrect` and no
`ok` or `incorrect` record without a hash (an entry `incorrect` in both
passes, a 1-ULP oracle miss, passes these); in each pass no record without
a `segmentsPrunedByLabelIndex` figure; no unrouted record and at least one
routed record with `segmentsPrunedByLabelIndex` above 0; in each pass a
cold record count present, with 0 records and none short on `ci`, where no
cold entry applies; no listed object above the watermark, no statement
whose unrouted `segmentsFetched` differs from its
`unrouted_fetched_expected` and no unchecked statement; and at least one
holder for `mb_selective_job_rate_sum`, which is route-bound. Every basis is a
prediction until a `stage1=true` dispatch measures it.

The nightly workflow's manual dispatch takes a `stage1` input (default
false) that sets `MB_STAGE1=1` and then runs `stage1-check.sh` with the `ci`
bands. On an AWS box, `aws/launch.sh` passes `MB_STAGE1` to both of the
box's `run.sh` invocations and `MB_STAGE1_BANDS` as the measured run's band
file (required for any profile but `ci`, which defaults to the `ci` bands;
launch refuses a missing one with 64, and `box-run.sh` refuses it with exit
21 before anything is built). The box checks the ci smoke against the `ci`
bands and the measured run against its band file, uploads each check's
output as `stage1-check.txt` beside its run, and ends with exit 22 when the
smoke's check fails and 23 when the measured run exited 0 and its check
failed (run.test.sh, the `box-run` cases).

### The real-S3 lane

ADR-0927 decision 10 says only real S3 produces publishable Ravel performance
and cost figures, because s3s-fs charges nothing per request. The two flags
above layer `real-s3.yml` after `ravel.yml`, which moves Ravel and Mimir onto
real S3 and changes no other setting. Prometheus and VictoriaMetrics stay on
local disk.

[docs/internal/metricsbench-aws-runbook.md](../../docs/internal/metricsbench-aws-runbook.md)
runs this lane end to end with the scripts under [aws/](aws/), from the
instance role to a published reference run.

Before the run, on an EC2 host:

1. Create four empty buckets in the region: `<PREFIX>-ravel`,
   `<PREFIX>-mimir-blocks`, `<PREFIX>-mimir-ruler` and
   `<PREFIX>-mimir-alertmanager`. `run.sh` does not create them, since bucket
   names are global and a bucket outlives the run.
2. Give the instance role read and write access to them.
3. Set the instance's metadata hop limit to 2. Both engines get credentials
   from the instance metadata service through the Docker bridge, one hop past
   the default limit.

`createbuckets` then checks the buckets instead of creating them, and fails the
`qualify` step when one is missing, unreadable or holds an object: a bucket
keeps its data after `down -v`, so each run needs fresh ones. `s3s` does not
start and `fetch-s3s-fs` does not run (case s).

`stamps.json` says `store_kind: "s3"` and lists the buckets under
`object_store`. The provenance says `--store-backend s3`, endpoint `n/a` and
`backend_bills_requests: true` (case s). Without the flags none of this happens
(case s1). The lane has no credentials to record: neither engine is given a
key.

### The falsification run

With `--diagnostic-prometheus-max-samples N`, `run.sh` instead writes a
compose override setting Prometheus' `--query.max-samples=N` into
`<out>/prometheus-max-samples.override.yml`, starts Prometheus alone with it,
ingests the profile into Prometheus only, and sends `mb_long_range_7d_avg`'s
query once as an instant query at the newest sample. `falsification.json`
records the HTTP status, `status`, `errorType`, `error` and the result's
series count; no `report.json` is written and stamps say `diagnostic: true`
(case f). `--ravel-image` and `--ravel-source-commit` are optional here,
since no Ravel starts; for the same reason the `objects` step does not run there (case f). It starts no Ravel or Mimir and runs no query step, so it takes none of the scrapes around the query step and stamps `query_scrapes` `not applicable` (case f). A `--profile` the corpus does not declare
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
| 64 | Usage error (cases h, p, s2 to s7, u17 to u19, u24 to u27, v3, v4). |
| 65 | A refused input, or an s3s-fs binary whose sha256 is not the pin; nothing was started (cases c, h, p, n2, n3, n4). |
| 70 | A step failed; its name is in the message and in `stamps.json` as `failed_step` (cases b, e2, i, k, n6, r, u1, u2, u2a, u2b, u3, u4, u12, u14, u15a, u15b, u22, v2, v6, v7). A step among 15 to 18 that fails after a failed `memory` step exits 70 naming that step rather than 75, with `memory` in `deferred_failures` (case e2). After a failed ingest verification (a row's read-back failed or a row stalled) the run queries as a diagnostic, records the failure, and exits 70 naming step ingest, even when a later step failed too; the first later step to fail ends the run, except `memory`, after which steps 15 to 18 still run, so an `--rss-band` breach reaches `failed_after_ingest` only when every step before `memory` passed and no step after it failed, while `memory-summary.json` records it either way (cases q, q2, q3, q4); such a run whose Ravel row has a `row_failure` lists `memory` in `failed_after_ingest` after the four are skipped (case e4); steps 15 to 18 still run when the Ravel row replayed, and a failure there is listed in `failed_after_ingest` (cases u5, u6, u13), and are skipped when it did not (cases u7, u8); a failed `objects` band check after a failed ingest is listed the same way (case v5). |
| 75 | An `--rss-band` was violated or a container was OOM-killed, and no later step failed. In a full run steps 15 to 18 run first and `stamps.json` lists `memory` in `deferred_failures` (cases d, e); the diagnostic mode exits at once (case e3). |

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
