# Getting started

Run Ravel on your machine from published container images, send it real
telemetry, and query it back. You need git and Docker with `docker compose`.
Nothing is compiled. To build Ravel from source instead, see
[Building from source](#building-from-source).

The commands below match the [README quickstart](../../README.md#quickstart).
A weekly CI job runs that README's marked commands against the published
images.

## Start the stack

Clone the repository and start the stack from its root:

```sh
git clone https://github.com/NOFireAI/ravel.git
cd ravel
docker compose -f deploy/docker-compose/ravel.yml up -d
```

The stack runs these services, all from published images:

| Service | Address | What it does |
|---|---|---|
| RustFS | `127.0.0.1:9000` (S3 API), `127.0.0.1:9001` (console) | Object storage, with the `ravel-dev` bucket |
| `ravel-server` 0.31.1 | `127.0.0.1:4318` (HTTP), `127.0.0.1:4317` (gRPC) | Ravel, with token `demo-token` for tenant `demo-tenant` |
| OpenTelemetry Collector | `127.0.0.1:18888` (its own metrics) | Sends your host's CPU, load, memory, and network metrics, and its own logs |
| Grafana | `127.0.0.1:3000` (`admin` / `admin`) | Dashboards, with the Ravel datasource set up |

Two one-shot containers run first. One creates the bucket. The other
qualifies the store: `ravel-server` refuses to start on a bucket with no
qualification record. Both are safe to run again on an existing bucket. To
use another server image, set `RAVEL_IMAGE` before `up`.

> [!WARNING]
> Every credential here is a fixed development value (`demo-token`, the
> RustFS pair `ravel` / `ravel-dev-secret`, and the audit token key), and
> every port binds `127.0.0.1` only. Do not use this stack, or copy its
> settings, for anything a network can reach.

## Wait for the server

```sh
for i in $(seq 30); do curl -fs http://127.0.0.1:4318/readyz && echo ready && break; sleep 2; done
```

It prints `ready`, usually within a few seconds. Until then, a query fails
with "connection refused". If nothing prints after a minute, read the server
log:

```sh
docker compose -f deploy/docker-compose/ravel.yml logs ravel-server
```

The first host metrics arrive 10 to 20 seconds after the server is ready. The
Collector scrapes every 10 seconds and exports after each scrape. Each export
is acknowledged only after its data is stored in RustFS.

## Query your metrics

Every query needs the bearer token. Ask for CPU load with PromQL:

```sh
curl -s -H "Authorization: Bearer demo-token" \
  'http://127.0.0.1:4318/api/v1/query?query=system_cpu_load_average_1m'
```

The answer is the Prometheus JSON envelope (the `stats` object is trimmed
here):

```json
{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"system_cpu_load_average_1m"},"value":[1730000000.000,"0.42"]}]}}
```

- `status` is `success` or `error`. An error also carries `errorType` and
  `error`, with a 4xx or 5xx HTTP status.
- Each entry of `data.result` is one series: its labels under `metric`, and
  one `[timestamp, value]` pair. The value is a string, as in Prometheus.
- HTTP 401 means the `Authorization` header is missing or names a token the
  server does not know. A query route never answers that with an empty
  result. The exception is `/api/v1/metadata`, which returns an empty object,
  so do not use it to test a token.

## Query your logs

The Collector sends its own log lines when it starts. Count them with PromQL,
through the reserved `ravel_log_lines` metric name:

```sh
curl -s -H "Authorization: Bearer demo-token" \
  --data-urlencode 'query=sum by (job) (count_over_time(ravel_log_lines[1h]))' \
  'http://127.0.0.1:4318/api/v1/query'
```

The result has one series with `"job":"otelcol-contrib"`. The Collector logs
little after startup, so a window shorter than the time since `up`, such as
`[5m]`, can be empty. To read the lines themselves, use SQL:
`SELECT ts, body FROM logs LIMIT 5`.

## Query with SQL

```sh
curl -s -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer demo-token" \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT ts, value FROM samples LIMIT 1"}'
```

The answer wraps a described schema:

```json
{"status":"success","data":{"columns":[{"name":"ts","type":"Timestamp(ns)"},{"name":"value","type":"Float64"}],"rows":[[1751402400000000000,0.42]]},"stats":{}}
```

- `columns` lists each column's `name` and Arrow `type`, in projection order.
  Each entry of `rows` is positional against that list.
- `stats` is the query's cost accounting.
- A nanosecond timestamp is an integer, a `series_id` is a hex string, and a
  label or attribute map is a JSON object. `NaN`, `+Inf`, and `-Inf` are
  strings.
- `SELECT *` returns every column the table has. Name the columns when the
  shape matters to you.

The tables are `samples` (metrics), `logs`, `spans` (traces), `alerts`, and
`audit`. The [query guide](query.md) covers each one.

## Open Grafana

Open <http://127.0.0.1:3000> and sign in as `admin` / `admin`. The host-metrics
dashboard reads from Ravel. A panel needs two points to draw a line, so give it
about 30 seconds.

## When a result is empty

An empty result is a successful query that matched nothing: `"result":[]` in
PromQL, or `"rows":[]` in SQL. Check these causes in order:

1. **The first export has not landed.** Wait 20 seconds after the server is
   ready.
2. **The name differs from what you sent.** OTLP metric names get
   Prometheus-style unit and `_total` suffixes at ingest: a monotonic `foo`
   with unit `By` is stored as `foo_bytes_total`. List the stored names with
   `/api/v1/label/__name__/values`.
3. **This stack sends no such data.** The quickstart sends no traces, so
   `spans` is empty, and its only logs are the Collector's own.
4. **The data was too old.** Ravel refuses points outside its
   [event-time skew bounds](ingest.md#event-time-skew-bounds).
5. **The window misses the data.** A range that ends before the first sample
   is empty.

If none of these explains it, check delivery. The Collector's own metrics are
at `http://127.0.0.1:18888/metrics`. A non-zero `send_failed` counter means
Ravel refused the exports.

## Read your own write

The queries above read data another process wrote. To follow one write from
acknowledgement to query, run the walkthrough while the stack is up:

```sh
demo/walkthrough.sh
```

It sends one export, reads the `x-ravel-commit-token` response header, and
queries the write back with `min_commit_token`. It checks each step and exits
non-zero on a failure.

With the token, the catalog reads that write's commit record directly instead
of waiting for a fresh listing, so the query is sure to include the write. A
request that spans several shards gets one token per shard, comma-separated.
Pass the whole header value back. The
[catalog contract](../catalog-and-mvcc.md#commit-tokens) defines the token,
and [inspecting data](inspecting-data.md) shows the objects a write creates.

## Kill the server, keep the data

```sh
demo/kill-and-recover.sh
```

The script writes one sample under strict acknowledgement, kills the
`ravel-server` container with `SIGKILL`, replaces it with a fresh container,
and reads the sample back by its commit token. Only the bucket survives the
kill.

The guarantee covers strict acknowledgement, the default. Buffered
acknowledgement is opt-in per request: it answers before the flush and
returns no commit token, so a crash can lose its buffered window. The
[consistency model](../consistency-model.md#acknowledgement-semantics) states
both.

## Stop the stack

```sh
docker compose -f deploy/docker-compose/ravel.yml down
```

This stops and removes the containers. The stored data stays in
`rustfs-data/` at the repository root, and the next `up` reuses it.

To start again from an empty store, delete that directory after `down`:

> [!CAUTION]
> `rm -rf rustfs-data` permanently deletes everything the stack stored. On
> Linux the directory is owned by root, so it may need `sudo`.

## Next steps

- [Ingest](ingest.md): point your own services or Collector at Ravel.
- [Query](query.md): PromQL, SQL, and log queries. [Traces](traces.md): the
  `spans` table and trace lookup.
- [Operations](operations.md): run Ravel against your own bucket.
- [Consistency model](../consistency-model.md): what acknowledgement,
  visibility, and crash recovery guarantee.

## Building from source

This path is for changing Ravel's code. It needs the Rust toolchain pinned in
[`rust-toolchain.toml`](../../rust-toolchain.toml) (1.97.1; `rustup` installs
it on the first `cargo` run) and Docker with `docker compose`. The
[contributing guide](../../CONTRIBUTING.md) covers the checks to run before a
pull request.

A source build has no default cargo features. The published image is built
with `--features sql,flight-sql,otap,agents,mcp`; pass the features you need
to cargo. `--features` is a cargo argument, not a `ravel-server` flag.

### Start RustFS

```sh
make rustfs
```

This starts RustFS on `127.0.0.1:9000` (credentials `ravel` /
`ravel-dev-secret`) and a one-shot that creates the `ravel-dev` bucket with
Object Lock, versioning, and the lifecycle rules that
`--require-bucket-protection` checks. Data lives in `rustfs-data/`, shared
with the quickstart stack. `make rustfs-down` stops it and keeps the data. If
an old `rustfs-data/` from before Object Lock was set makes the bucket step
fail, delete the directory.

### Run the scripted demo

```sh
make demo
```

`make demo` first runs `make build`, a release build of the whole workspace
with no features. It then runs [scripts/demo.sh](../../scripts/demo.sh), which
builds debug `ravel-server` and `ravel-cli` with no features and uses those:

1. It starts RustFS if it is not running, qualifies the store, and generates
   an OTLP export with current timestamps.
2. It starts `ravel-server` on `127.0.0.1:14318` (HTTP) and `127.0.0.1:14317`
   (gRPC).
3. It sends the export, reads the commit token, and queries the series back
   with it.

If RustFS was already running, the script does not wait for the bucket
one-shot. Check it with
`docker compose -f deploy/docker-compose/rustfs.yml ps --all createbucket`;
it should show exit 0.

The output ends with these lines, then `[demo] demo complete`:

```
export result: commit_token=<base64url string>
query result: {"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[<ts>,"<value>"]}]}}
```

The demo server has no `sql` feature, so it does not serve
`POST /api/v1/sql`.

### Run a server by hand

A fresh bucket needs two choices before a server starts on it:

1. **Qualify the store** once with `ravel-cli store qualify`.
2. **Choose a tenant-hash scheme.** Use `--tenant-hash-key-file` for a real
   deployment, or `--tenant-hash-unkeyed` for a throwaway bucket. The choice
   is permanent for that bucket.

```sh
export RAVEL_S3_ENDPOINT=http://127.0.0.1:9000
export RAVEL_S3_BUCKET=ravel-dev
export RAVEL_S3_REGION=us-east-1
export RAVEL_S3_ACCESS_KEY=ravel
export RAVEL_S3_SECRET_KEY=ravel-dev-secret

cargo run -p ravel-cli -- --store s3 store qualify

cargo run -p ravel-server --features sql -- \
  --store s3 \
  --tenant-hash-unkeyed \
  --tenant-token devtoken=acme
```

The server listens on `127.0.0.1:4318` (HTTP) and `127.0.0.1:4317` (gRPC) and
accepts `Authorization: Bearer devtoken` for tenant `acme`. Both binaries read
the `RAVEL_S3_*` variables in place of the `--s3-*` flags. Drop
`--features sql` to build without the SQL endpoint.

The query audit is off here. To turn it on as the quickstart does, add
`--audit-mode required` and set `RAVEL_AUDIT_TOKEN_KEY` to a 64-hex-character
key; an unkeyed bucket has no deployment key to derive one from.

To use a Google Cloud Storage bucket, unset the `RAVEL_S3_*` variables
(`--store gcs` refuses them) and sign in with Application Default
Credentials, for example `gcloud auth application-default login`:

```sh
cargo run -p ravel-cli -- --store gcs --gcs-bucket my-ravel-dev store qualify

cargo run -p ravel-server --features sql -- \
  --store gcs --gcs-bucket my-ravel-dev \
  --tenant-hash-unkeyed \
  --tenant-token devtoken=acme
```

[Google Cloud Storage](operations/configuration.md#google-cloud-storage)
covers GKE Workload Identity and the flags `--store gcs` refuses.

### Send and read one export

Generate an OTLP metrics export with current timestamps, send it, and keep
the response headers:

```sh
cargo run -p ravel-server --example gen_otlp_fixture > fixture.pb

curl -s -D headers.txt -o /dev/null \
  -X POST http://127.0.0.1:4318/v1/metrics \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @fixture.pb

TOKEN=$(grep -i '^x-ravel-commit-token:' headers.txt | sed 's/^[^:]*: *//' | tr -d '\r')
```

The response is a strict acknowledgement: it returns after the segment and its
commit record are stored. Query the write back with its token:

```sh
curl -s -G http://127.0.0.1:4318/api/v1/query \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode "query=demo_requests_total" \
  --data-urlencode "min_commit_token=$TOKEN"
```

The answer uses the envelope in [Query your metrics](#query-your-metrics), and
the [empty-result causes](#when-a-result-is-empty) apply.
