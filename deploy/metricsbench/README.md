# MetricsBench comparator deployments

Checked-in, digest-pinned deployments of the cross-engine comparators for the
MetricsBench metrics benchmark (ADR-0927). These are the systems the
MetricsBench harness (`crates/ravel-bench`) measures Ravel against in the
**portable lane**: Prometheus Remote Write 1.0 for ingest, the Prometheus HTTP
query API for reads. Every system here receives the same logical samples and the
same queries; no Ravel-only instrumentation is ever folded into a cross-engine
score (ADR-0927 decision 1).

Ravel itself is not in this file. It is the system under test and is launched
separately (`deploy/docker-compose/ravel.yml`). This directory holds only the
comparators.

## The comparators

| Service | What it is | Durability / storage | Remote Write 1.0 endpoint | PromQL query endpoint |
|---|---|---|---|---|
| `prometheus` | Reference PromQL engine, pinned to v3.13.1 (the same version ADR-0927 uses as the correctness oracle) | **Local-disk TSDB.** Ack = in-memory head + on-disk WAL. Not object storage. | `http://127.0.0.1:9090/api/v1/write` | `http://127.0.0.1:9090/api/v1/query` |
| `victoriametrics` | Single-node VictoriaMetrics, accepts Prometheus remote write natively | **Local-disk storage.** Ack = local storage write. Not object storage. | `http://127.0.0.1:8428/api/v1/write` | `http://127.0.0.1:8428/prometheus/api/v1/query` |
| `mimir` | Grafana Mimir 3.2.3 monolithic (`-target=all`); the ADR-0927 object-storage-native PromQL system | **Object storage (S3).** Ack = local WAL; blocks ship to S3 (RustFS here) later. See caveat below. | `http://127.0.0.1:9009/api/v1/push` | `http://127.0.0.1:9009/prometheus/api/v1/query` |

Supporting services: `rustfs` (S3 backend for Mimir) and `createbuckets` (a
one-shot AWS CLI container that creates Mimir's buckets and then exits).

### Storage architecture is not interchangeable

Prometheus and VictoriaMetrics persist to **local disk**. Mimir is
**object-storage-native** and writes its durable blocks to S3 — but in this stack
S3 is provided by **RustFS**, which is local-disk-backed and charges no
per-request fees. Per ADR-0927 decision 10 (ADR-0075 decision 3), a RustFS result
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
| Prometheus | `--query.max-samples` | `500000000` | `50000000` | 10,000 series x 7 d / 15 s = 10,000 x 40,320 = 403,200,000, plus headroom | `--query.max-samples=50000000  Maximum number of samples a single query can load into memory` |
| Prometheus | `--query.timeout` | `120s` | `2m` (unchanged) | Written so the capture shows it | `--query.timeout=2m  Maximum time a query may take before being aborted` |
| Prometheus | `--query.lookback-delta` | `5m` | `5m` (unchanged) | Stated, not changed | `--query.lookback-delta=5m  The maximum lookback duration for retrieving metrics during expression evaluations` |
| VictoriaMetrics | out-of-order window | none to set | not applicable | Accepts any order inside `-retentionPeriod` | v1.115.0 docs, `docs/victoriametrics/README.md`, "Backfilling": "VictoriaMetrics accepts historical data in arbitrary order of time via any supported ingestion method" |
| VictoriaMetrics | `-search.latencyOffset` | `0` | `30s` | An instant query at the newest sample sees that sample | `-search.latencyOffset duration  The time when data points become visible in query results after the collection ... (default 30s)` |
| VictoriaMetrics | `-search.maxPointsPerTimeseries` | `20000` | `30000` | History grid 24 h / 5 m + 1 = 289 points | `-search.maxPointsPerTimeseries int  The maximum points per a single timeseries returned from /api/v1/query_range ... (default 30000)` |
| VictoriaMetrics | `-search.maxSamplesPerQuery` | `1000000000` | `1000000000` (unchanged) | 403,200,000, as for Prometheus | `-search.maxSamplesPerQuery int  The maximum number of raw samples a single query can process across all time series ... (default 1000000000)` |
| VictoriaMetrics | `-search.maxQueryDuration` | `120s` | `30s` | Equal to Prometheus' `--query.timeout` | `-search.maxQueryDuration duration  The maximum duration for query execution ... (default 30s)` |
| VictoriaMetrics | `-search.maxUniqueTimeseries` | `1000000` | derived from CPU and memory | Cardinality profile, 1,000,000 series | `When set to zero, the limit is automatically calculated based on -search.maxConcurrentRequests (inversely proportional) and memory available to the process (proportional).` |
| VictoriaMetrics | `-search.disableCache` | `true` | `false` | Historical replay; a production_cache regime re-enables it per run | `-search.disableCache  Whether to disable response caching. This may be useful when ingesting historical data.` |
| Mimir | `limits.out_of_order_time_window` | `1h` | `0s` | Same as Prometheus | `-ingester.out-of-order-time-window duration  Non-zero value enables out-of-order support`; `0s` read from the 3.2.3 binary's `/config` |
| Mimir | `limits.past_grace_period` | `0s` | `0s` (unchanged) | A 30 d replay is not refused for its age | `-validation.past-grace-period duration ... 0 to disable.` |
| Mimir | `querier.query_store_after` | `0s` | `12h` | Replayed timestamps follow the anchor, not the wall clock; every query reaches the store-gateway | `-querier.query-store-after duration ... 0 means all queries are sent to store. ... (default 12h0m0s)` |
| Mimir | `limits.query_ingesters_within` | `0s` | `13h` | Every query reaches the ingester, so unshipped samples stay visible | `-querier.query-ingesters-within duration ... 0 means all queries are sent to ingester. (default 13h)` |
| Mimir | `blocks_storage.bucket_store.sync_interval` | `15m` | `15m` (unchanged) | Readiness wait, not a query budget | `-blocks-storage.bucket-store.sync-interval duration  How frequently to scan the bucket, or to refresh the bucket index ... (default 15m0s)` |
| Mimir | `compactor.cleanup_interval` | `15m` | `15m` (unchanged) | Readiness wait, not a query budget | `-compactor.cleanup-interval duration  How frequently the compactor should run blocks cleanup and maintenance, as well as update the bucket index. (default 15m0s)` |
| Mimir | `limits.max_fetched_chunks_per_query` | `5000000` | `2000000` | 10,000 x 40,320 / 120 samples per chunk = 3,360,000 | `-querier.max-fetched-chunks-per-query int  Maximum number of chunks that can be fetched in a single query ... (default 2000000)`; 120 per chunk observed (9,600 samples in 80 chunks per 2 h block of 20 series) |
| Mimir | `querier.max_samples` | `500000000` | `50000000` | As for Prometheus | `-querier.max-samples int  Maximum number of samples a single query can load into memory. ... (default 50000000)` |
| Mimir | `limits.max_estimated_memory_consumption_per_query` | `0` | `0` (unchanged) | Not sized; see the note below | `-querier.max-estimated-memory-consumption-per-query uint  [experimental] ... only enforced when Mimir's query engine is in use. ... 0 to disable.` |
| Mimir | `querier.timeout` | `2m` | `2m` (unchanged) | Equal to Prometheus' `--query.timeout` | `-querier.timeout duration  The timeout for a query. ... (default 2m0s)` |
| Mimir | `limits.max_total_query_length` | `31d` | `0s` (no limit) | History's 30 d span plus 1 d; `max_query_length` does not exist in 3.2.3 | `-query-frontend.max-total-query-length duration  Limit the total query time range (end - start time).` |
| Mimir | `limits.max_partial_query_length` | `0s` | `0s` (unchanged) | No separate bound on split sub-queries | `-querier.max-partial-query-length duration  Limit the time range for partial queries at the querier level.` |
| Mimir | `limits.max_query_lookback` | `45d` | `0s` (disabled) | The 45 d retention; a silent clamp, not a refusal | `-querier.max-query-lookback duration ... the request will not fail but will be manipulated to only query data within the allowed time range. 0 to disable.` |
| Mimir | `frontend.query_result_response_format` | `protobuf` | `protobuf` (unchanged) | Native histograms under the default query sharding | `-ingester.native-histograms-ingestion-enabled ... To query native histograms with query-sharding enabled make sure to set -query-frontend.query-result-response-format to 'protobuf'.` |
| Mimir | `frontend.cache_results` | `false` | `false` (unchanged) | Parity with `-search.disableCache=true` | `-query-frontend.cache-results  Cache query results.` |

Limits the corpus does not reach, left at their defaults: VictoriaMetrics
`-search.maxSamplesPerSeries` (default 30000000; history holds 172,800 samples
per series) and `-search.maxPointsSubqueryPerTimeseries` (default 100000;
`mb_long_range_subquery_max`'s `[1d:5m]` is 289 points), and the 11,000 points
per series Prometheus and Mimir allow on `/api/v1/query_range`, which is not a
flag.

Under Mimir 3.2.3's default query engine, `querier.max_samples` did not fire:
with it set to 100, a query over 28,801 samples returned 200. That engine's
per-query bound is `max_estimated_memory_consumption_per_query`, which is off,
so on Mimir an oversized query fails on the 4g memory envelope rather than as
a budget refusal. The value is still frozen for the Prometheus engine.

Mimir readiness: a historical replay is not queried until the bucket index
lists a block covering the replay's oldest sample and the store-gateway has
loaded every indexed block. `config/mimir.yaml` names the file, page and
metrics to read, beside the `frontend` block.

### Budget-refusal signatures

What each comparator returns when a limit fires. Observed on 2026-10-26 by
running each limit at a deliberately tiny value against the pinned versions'
upstream release binaries for linux/arm64 (Prometheus 3.13.1, VictoriaMetrics
v1.115.0, Mimir 3.2.3 with this directory's `config/mimir.yaml` and a
filesystem bucket in place of RustFS), then restoring the pinned value. The
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

The match key for a status mapper is the stable token: Mimir's `err-mimir-*`
identifier, VictoriaMetrics' flag name in the message, and Prometheus'
`errorType` with its fixed message.

## Launch the set

```sh
docker compose -f deploy/metricsbench/docker-compose.yml up -d
```

Every image is pinned to an immutable `@sha256:` digest (see below). Each
comparator is given identical CPU (2.0) and memory (4g) limits so no engine
wins on hardware.

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

## Record what actually ran

The compose file pins what should run; this captures what did. With the set
up, run:

```sh
deploy/metricsbench/capture-effective-config.sh --out effective-config.json
```

`--prometheus URL`, `--victoriametrics URL` and `--mimir URL` override the
loopback compose ports (9090, 8428, 9009); without `--out` the document goes to
stdout. Per comparator it records the version and revision the engine itself
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

A field that could not be fetched is `{"unavailable": "<reason>"}`; none is
omitted or left empty. When docker is not reachable, `image` is the string
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
    }
  }
}
```

A run report cites this file by path and by its sha256 digest. Until the
MetricsBench runner calls the script before each measurement pass, this
section is its caller. Its cases run in CI's `doc-scripts` job:
`deploy/metricsbench/tests/capture-effective-config.test.sh`.

## Verify the pins

```sh
deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh
```

Besides this directory's compose file, the script checks image and action
pins across the repository. All six categories run in one invocation and each
is checked against its own expected count:

- **Compose images** (this directory): every image reference carries an
  `@sha256:` digest, every ADR-0927-required comparator is present, and the
  number of image references equals the expected count.
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
  workflow already resolved to a digest. Both the total (17) and the
  pin-required count (11) equal their expected totals. Backslash
  continuations are joined before matching, here-doc bodies are skipped, and
  every invocation on a line is scanned, not just the first.

- **Kubernetes manifest images**: every `image:` reference under
  `deploy/k8s` carries an `@sha256:` digest, with the two locally built
  placeholders that kind loads by tag (`ravel-server`, `ravel-operator`)
  excluded by exact match. The total and the pin-required count each equal
  their expected number, so an image added there without a digest fails the
  check rather than going unnoticed.

Exit 0 means all six categories passed. Any unpinned reference, any missing
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

**`prom/prometheus`, `victoriametrics/victoria-metrics`, and `grafana/mimir`**
are still on Docker Hub and resolve with the Docker Hub registry v2 API:

```sh
curl -sI -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
  https://registry-1.docker.io/v2/<repo>/manifests/<tag> \
  | grep -i docker-content-digest
```

**`ghcr.io/rustfs/rustfs` and `public.ecr.aws/aws-cli/aws-cli`** (see
deploy/README.md for why the object store and its client live off Docker Hub)
each issue a bearer token from their own auth endpoint rather than accepting
one minted for Docker Hub, so the recipe is the same shape with a different
token URL per registry.

This is written as two curl steps rather than a one-liner so the token is
carried over by hand instead of captured into a shell variable.

```sh
# 1. Get a bearer token scoped to the repo, and read the token field out
#    of the JSON it prints. For the AWS CLI image the token URL is
#    https://public.ecr.aws/token/?service=public.ecr.aws&scope=repository:aws-cli/aws-cli:pull
curl -s "https://ghcr.io/token?service=ghcr.io&scope=repository:rustfs/rustfs:pull" \
  | jq -r .token

# 2. HEAD the manifest list, pasting that token in place of <TOKEN> below.
#    RustFS publishes an OCI image index, so the Accept header names that
#    media type; the AWS CLI image publishes a Docker manifest list and takes
#    application/vnd.docker.distribution.manifest.list.v2+json instead.
curl -sI -H "Authorization: Bearer <TOKEN>" \
  -H "Accept: application/vnd.oci.image.index.v1+json" \
  https://ghcr.io/v2/rustfs/rustfs/manifests/1.0.0 \
  | grep -i docker-content-digest

# 3. The docker-content-digest response header is the pinned digest.
```

| Image | Tag | Digest |
|---|---|---|
| `prom/prometheus` | `v3.13.1` | `sha256:3c42b892cf723fa54d2f262c37a0e1f80aa8c8ddb1da7b9b0df9455a35a7f893` |
| `victoriametrics/victoria-metrics` | `v1.115.0` | `sha256:d8ac3a1776c8a9beead8bbd42a489c82249b1bfe9071dfd4813f34ebe36354bb` |
| `grafana/mimir` | `3.2.3` | `sha256:0ed56db7d7713707eff00224ddd4c3096afa6cf1cbe43a6f9391c537f1416099` |
| `ghcr.io/rustfs/rustfs` | `1.0.0` | `sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff` |
| `public.ecr.aws/aws-cli/aws-cli` | `2.37.2` | `sha256:e38214027df83cb6631adcf980a092a98d1d29788789bff2a0f424e87e3da8ed` |

The tag is kept in each `image:` reference alongside the digest for human
readability; the digest is what pins the run.

### Pins shared with deploy/docker-compose/ravel.yml and rustfs.yml

The two quickstart compose files are scanned by the same script as its fourth
category (see above). Their RustFS and AWS CLI pins are the same tag and digest
as the ones above. `deploy/README.md` lists all four of their pins and why
these registries were chosen.
