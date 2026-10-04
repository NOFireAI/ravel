# Getting started

Bring up the container quickstart, send it real telemetry, and read the
telemetry back. This is the one recommended path to a first query, and it
needs only Docker. To build Ravel from source, which is a contributor
workflow, see [Building from source](#building-from-source).

The commands are in the [README quickstart](../../README.md#quickstart),
where continuous integration runs them against a live stack on every change.
Read on for what each response means, how long to wait before data appears,
and when an empty result is the correct answer.

## What you need

Docker with `docker compose`. The stack pulls every image from a registry, so
you need no Rust toolchain and no local build.

## Bring the stack up

```sh
docker compose -f deploy/docker-compose/ravel.yml up -d
```

The command starts five things, all from published images:

- RustFS on `127.0.0.1:9000` (S3 API) and `127.0.0.1:9001` (console), plus a
  one-shot that creates the `ravel-dev` bucket.
- A one-shot that qualifies the store. `ravel-server` refuses to start against
  a bucket that carries no qualification record, and it has no
  bootstrap-and-continue path. So a new bucket must be qualified before the
  server starts. The step is idempotent: on an already-qualified bucket it
  reports the existing record and exits 0.
- `ravel-server` from `ghcr.io/nofireai/ravel-server:0.22.0`, listening on
  `127.0.0.1:4318` (HTTP) and `127.0.0.1:4317` (gRPC), with the tenant token
  `demo-token` mapped to tenant `demo-tenant`. To override the image pin, set
  the `RAVEL_IMAGE` environment variable.
- An OpenTelemetry Collector that scrapes the CPU, load, memory, and network
  metrics of your host. It exports them to Ravel over the OpenTelemetry
  Protocol (OTLP) and authenticates with the demo bearer token.
- Grafana on `127.0.0.1:3000` (`admin` / `admin`) with the Ravel datasource
  already provisioned.

Every published port binds loopback (`127.0.0.1`) only, and every credential
is a fixed development value (`demo-token`, and `ravel` / `ravel-dev-secret`
for RustFS). None of it is for a network-reachable deployment.

The compose file also sets `RAVEL_AUDIT_TOKEN_KEY` to a fixed development key
so that the audit trail can tokenize query text. For anything beyond a laptop,
set your own 64-hex-character value with that same variable.

The server is usually ready a few seconds after `up -d` returns, because the
bucket creation and the store-qualify one-shot must finish first. Until the
server is ready, a query gets a connection refused and no error envelope.

## When data appears

The first samples reach Ravel roughly 10 to 20 seconds after the server
becomes ready. The Collector scrapes the host every 10 seconds and exports
immediately after each scrape.

Ravel acknowledges those exports under strict acknowledgement. When the
export call of the Collector returns, the data object and its commit record
are durably in RustFS. There is no separate flush to wait for. A query that
carries no commit token still depends on the catalog resolving a fresh
listing. Allow a couple of scrape intervals before you conclude that
something is wrong.

A Grafana panel needs at least two points to draw a line. Give the first
dashboard about 30 seconds before you read anything into a flat panel.

## Read the responses

Run the two `curl` commands from the
[README quickstart](../../README.md#quickstart).

A PromQL instant query answers with the Prometheus JSON envelope:

```json
{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"system_cpu_load_average_1m"},"value":[1730000000.000,"0.42"]}]}}
```

- `status` is `success` or `error`. On `error` the envelope also carries
  `errorType` and `error`, and the HTTP status is 4xx or 5xx.
- `data.result` is an array of series. Each entry has the series labels under
  `metric` and one `[timestamp, value]` pair under `value`. The value is a
  JSON string, as Prometheus renders it.
- HTTP 401 means that the `Authorization: Bearer` header is missing or carries
  a token that the server has no tenant for. The query routes never answer an
  unauthenticated request with an empty result. The one exception is
  `/api/v1/metadata`, which returns an empty object for a tenant it cannot
  resolve. Do not use that route to test a token.

A SQL query answers with a described-schema envelope. This is the `data`
object of a `SELECT ts, value FROM samples LIMIT 1`:

```json
{"columns":[{"name":"ts","type":"Timestamp(Nanosecond, None)"},{"name":"value","type":"Float64"}],"rows":[[1751402400000000000,0.42]]}
```

- Each entry of `columns` is an object with a `name` and an Arrow `type`, in
  projection order. Each entry of `rows` is an array of values, positional
  against that list.
- The full response wraps that object as
  `{"status":"success","data":{...},"stats":{...}}`, where `stats` is the
  cost accounting of the query.
- A nanosecond timestamp comes back as an integer, a `series_id` as a hex
  string, and a label or attribute map as a JSON object. `NaN`, `+Inf`, and
  `-Inf` come back as strings, as Prometheus renders them.
- The column list is the projection of the statement, so `SELECT *` returns
  every column that the table has. If the shape matters to you, name the
  columns that you want.

## Empty results

An empty result is a successful query that matched nothing. In the PromQL
envelope it is `"status":"success"` with `"result":[]`. In the SQL envelope it
is `"rows":[]`. It is not an error, and it does not mean that ingest is
broken. Examine these causes in order:

- **The first export has not landed yet.** Before roughly the first 20
  seconds, every query over host metrics is empty.
- **The metric name is not the name you exported.** OTLP metric names get the
  standard Prometheus-style unit and `_total` suffixes at ingest. For example,
  a monotonic `foo` with `unit: "By"` is stored as `foo_bytes_total`. Query
  `/api/v1/label/__name__/values` to see the names that Ravel holds. The
  [query guide](query.md) covers the naming rules.
- **The table is empty on this stack.** The quickstart Collector sends host
  metrics only, so `SELECT * FROM logs LIMIT 5` and
  `SELECT * FROM spans LIMIT 5` return zero rows on a fresh stack. Logs and
  traces need a Collector pipeline or an exporter that sends them.
- **The data was rejected at ingest for being too old.** Ravel refuses data
  points outside its event-time skew bound, so a replay of the fixture from
  yesterday stores nothing. See
  [event-time skew bounds](ingest.md#event-time-skew-bounds).
- **The query window does not cover the data.** A range query over a window
  that ends before the first sample is empty.

If none of these causes explains an empty query, find out whether the
Collector delivers at all. The Collector publishes its own metrics on
`127.0.0.1:18888`. A non-zero `send_failed` counter in that output means that
the exports left the Collector and Ravel refused them.

## Read your own write

The quickstart queries data that some other process wrote. To watch the
read-your-write path end to end, run the walkthrough with the stack up:

```sh
demo/walkthrough.sh
```

The script ingests one export, captures its `x-ravel-commit-token` response
header, and reads that write back with `min_commit_token`. It asserts every
step, so you do not need to read its output closely.

### What the commit token means

The commit token is base64url of
`v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`
([docs/catalog-and-mvcc.md](../catalog-and-mvcc.md#commit-tokens)). Those five
fields are what the server needed to flush your data: the shard it landed in,
the writer process that flushed it, and the sequence number and ingest-hour
bucket of that writer.

If you pass the token back as `min_commit_token`, the catalog reconstructs the
key of the commit record and GETs it directly. The catalog does not depend on
a fresh listing that includes the record. That makes read-your-write exact.

The token locates a **commit record**: a small protobuf that notes where the
data lives, its content hash, and its sample and series counts. The commit
record names a **data object**: the RSEG segment that holds your samples,
column-encoded and immutable. Object keys look like this
([docs/catalog-and-mvcc.md](../catalog-and-mvcc.md#key-layout-all-under-one-bucket-root)):

```
data key:   t/<tenant_hash>/m/l0/<shard>/<writer_id>.<epoch>.<seq>.<hash16>.rseg
commit key: t/<tenant_hash>/m/c/<shard>/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt
```

A request that spreads across shards gets one token per shard, comma-separated
in the header. Pass the whole header value back: the catalog requires every
token in it.

To list what the stack wrote, see [inspecting data](inspecting-data.md).

## Prove the durability claim

`demo/kill-and-recover.sh` ingests one export under strict acknowledgement and
`SIGKILL`s the `ravel-server` container. It then replaces the container with a
fresh one and reads the pre-kill sample back by its commit token. Nothing
crosses the kill except what is in RustFS. See
[kill the server, keep the data](../../README.md#kill-the-server-keep-the-data).

The claim applies to strict acknowledgement, which is the default. Buffered
acknowledgement is opt-in per request. It returns before the flush and carries
no commit token. So a crash loses its buffered window, and an abandoned flush
can drop already-acknowledged rows with no crash. The
[consistency model](../consistency-model.md#acknowledgement-semantics) is
normative for both.

## Stop the stack

```sh
docker compose -f deploy/docker-compose/ravel.yml down
```

`rustfs-data/` on your machine persists across runs. You can safely bring the
stack up again on the same directory:

- The `createbucket` one-shot reads the versioning and lifecycle configuration
  of the bucket first, and puts only what is not already set.
- The store-qualify one-shot is idempotent.

The same holds for `deploy/docker-compose/rustfs.yml` below, which shares that
directory and that `createbucket` script. To start from an empty store, delete
the directory.

## Where to go next

- [Ingest](ingest.md) for the write path, every accepted protocol, and the
  admission rules.
- [Query](query.md) for PromQL and SQL, and [traces](traces.md) for the `spans`
  table.
- [Operations](operations.md) for flags, storage credentials, and day-two work,
  and [caching](caching.md) for the read cache.
- [Consistency model](../consistency-model.md) for what acknowledgement,
  visibility, and crash recovery mean. It is normative.

## Building from source

This section needs a toolchain. It is for people who change the code of Ravel.
The [development guide](../internal/development.md) covers the workflow in
depth.

### Prerequisites

- Rust, pinned by [`rust-toolchain.toml`](../../rust-toolchain.toml) to 1.97.1
  (edition 2024). If you use `rustup`, it installs this version automatically
  the first time you run `cargo` in the repository.
- Docker with `docker compose`, for the local RustFS stack.

### Bring up RustFS

```sh
make rustfs
```

This runs `docker compose -f deploy/docker-compose/rustfs.yml up -d`
([deploy/docker-compose/rustfs.yml](../../deploy/docker-compose/rustfs.yml)).
It starts two things:

- RustFS on `127.0.0.1:9000` (S3 API) and `127.0.0.1:9001` (web console), with
  credentials `ravel` / `ravel-dev-secret`.
- A one-shot `createbucket` service. It creates the `ravel-dev` bucket with
  Object Lock, versioning and the lifecycle rules that
  `--require-bucket-protection` checks.

Data lives in `./rustfs-data` on your machine. `make rustfs-down` stops the
stack without deleting it.

If `./rustfs-data` remains from before that service set Object Lock, the
read-back of the service fails. Remove the directory to start over.

### Run the demo

```sh
make demo
```

`make demo` builds `ravel-server` and `ravel-cli` in release mode, then runs
[scripts/demo.sh](../../scripts/demo.sh). The script starts RustFS if it is
not already up.

- If the script started RustFS itself, it waits for the Compose `createbucket`
  service to exit. It stops if that service failed.
- If RustFS was already running, as after `make rustfs`, the script does not
  wait for that service and does not check it. Let `createbucket` finish
  first:
  `docker compose -f deploy/docker-compose/rustfs.yml ps --all createbucket`
  shows that it exited 0. If the bucket is missing, the store-qualify step
  reports it.

The demo then does these steps:

1. It generates a fresh OTLP metrics export with current timestamps.
2. It starts `ravel-server --store s3` on `127.0.0.1:14318` (HTTP) and
   `127.0.0.1:14317` (gRPC) against RustFS.
3. It posts the export and captures the `x-ravel-commit-token`.
4. It queries the series back with that token as `min_commit_token`.

The demo generates the fixture on every run and does not check it in. A stale
fixture falls outside the
[event-time skew bounds](ingest.md#event-time-skew-bounds), and then the demo
fails non-deterministically.

The expected output ends with two lines like this, then `[demo] demo complete`:

```
export result: commit_token=<base64url string>
query result: {"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[<ts>,"<value>"]}]}}
```

### SQL on the from-source path

`make demo` does not build the `sql` feature, so `POST /api/v1/sql` returns
nothing useful there. `--features` is a cargo argument and is not a
`ravel-server` flag. The feature is chosen when the binary is built. Ask cargo
for the feature, and pass the server's own flags after `--`.

A fresh bucket must pass two startup gates before a server runs on it. The
compose quickstart and `make demo` clear both for you. By hand, you clear
them:

1. Qualify the bucket once with `ravel-cli`. The server refuses to start
   against a bucket that carries no qualification record.
2. Choose a tenant-hash scheme. The server refuses to start against a fresh
   bucket with no scheme chosen. Use keyed with `--tenant-hash-key-file` for a
   real deployment, or unkeyed with `--tenant-hash-unkeyed` for a throwaway
   development bucket. Either choice is permanent for that bucket.

```sh
export RAVEL_S3_ENDPOINT=http://127.0.0.1:9000
export RAVEL_S3_BUCKET=ravel-dev
export RAVEL_S3_REGION=us-east-1
export RAVEL_S3_ACCESS_KEY=ravel
export RAVEL_S3_SECRET_KEY=ravel-dev-secret
export RAVEL_AUDIT_TOKEN_KEY=998626405d16aeca71f4fac7673b55213a774ba40401709022e81a27f050ffd8

cargo run -p ravel-cli -- --store s3 store qualify

cargo run -p ravel-server --features sql -- \
  --store s3 \
  --tenant-hash-unkeyed \
  --tenant-token devtoken=acme
```

The server binds the defaults, `127.0.0.1:4318` (HTTP) and `127.0.0.1:4317`
(gRPC). It accepts requests that carry `Authorization: Bearer devtoken` for
tenant `acme`.

- The environment variables stand in for the `--s3-*` flags, and both binaries
  read them the same way.
- `RAVEL_AUDIT_TOKEN_KEY` is the same development key that the compose stack
  uses. It is needed here for the same reason: this bucket is unkeyed, so
  there is no deployment key to derive one from.
- `store qualify` is idempotent: on an already-qualified bucket it reports the
  existing record and exits 0.
- To get the PromQL and ingest surfaces without the SQL endpoint, drop
  `--features sql` from the same command.

### Round trip by hand

An OTLP HTTP export is any valid `ExportMetricsServiceRequest` protobuf.
`cargo run -p ravel-server --example gen_otlp_fixture > fixture.pb` produces
one with current timestamps. Send it and keep the response headers:

```sh
curl -s -D headers.txt -o /dev/null \
  -X POST http://127.0.0.1:4318/v1/metrics \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @fixture.pb

grep -i ^x-ravel-commit-token headers.txt
```

The response is a strict-mode acknowledgement. It returns only after the
segment and its commit record are durably in the object store
([docs/consistency-model.md](../consistency-model.md#acknowledgement-semantics)).
Query the write back with the token from that header:

```sh
curl -s -G http://127.0.0.1:4318/api/v1/query \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode "query=demo_requests_total" \
  --data-urlencode "min_commit_token=<token from the header above>"
```

The envelope is the one in [read the responses](#read-the-responses), and the
same empty-result causes apply. To see the objects that this wrote, use
[inspecting data](inspecting-data.md).
