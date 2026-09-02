# Getting started

This is the one recommended path from nothing to a first query: bring up the
container quickstart, send it real telemetry, and read the telemetry back. It
needs Docker and nothing else. Building Ravel from source is a contributor
workflow, and it sits at the [end of this page](#building-from-source).

The commands themselves live in the
[README quickstart](../../README.md#quickstart), where continuous integration
executes them against a live stack on every change. This guide is the part the
README does not carry: what each response means, how long to wait before data
appears, and when an empty result is the correct answer rather than a broken
one.

## What you need

Docker with `docker compose`. No Rust toolchain, no compile, no local build of
any kind. Every image in the stack is pulled from a registry.

## Bring the stack up

```sh
docker compose -f deploy/docker-compose/ravel.yml up -d
```

That brings up five things, all from published images:

- MinIO on `127.0.0.1:9000` (S3 API) and `127.0.0.1:9001` (console), plus a
  one-shot that creates the `ravel-dev` bucket.
- A one-shot that qualifies the store. `ravel-server` refuses to start against a
  bucket that carries no qualification record, and there is deliberately no
  bootstrap-and-continue path for it, so a freshly created bucket has to be
  qualified before the server starts. The step is idempotent: on an
  already-qualified bucket it reports the existing record and exits 0.
- `ravel-server` from `ghcr.io/nofireai/ravel-server:0.11.0` (override the pin
  with the `RAVEL_IMAGE` environment variable), listening on `127.0.0.1:4318`
  (HTTP) and `127.0.0.1:4317` (gRPC), with the tenant token `demo-token` mapped
  to tenant `demo-tenant`.
- An OpenTelemetry Collector scraping your host's CPU, load, memory, and network
  metrics and exporting them to Ravel over the OpenTelemetry Protocol (OTLP),
  authenticating with the demo bearer token.
- Grafana on `127.0.0.1:3000` (`admin` / `admin`) with the Ravel datasource
  already provisioned.

Every published port binds loopback (`127.0.0.1`) only, and every credential is
a fixed development value (`demo-token`, and `ravel` / `ravel-dev-secret` for
MinIO). None of it is for a network-reachable deployment.

Because the store-qualify one-shot and the bucket creation both have to finish
before `ravel-server` starts, the server is usually ready a few seconds after
`up -d` returns. Until it is, a query gets a connection refused rather than an
error envelope.

## How long before data appears

The Collector scrapes the host every 10 seconds and exports immediately after
each scrape, so the first samples reach Ravel roughly 10 to 20 seconds after the
server becomes ready.

Ravel acknowledges those exports under strict acknowledgement: by the time the
Collector's export call returns, the data object and its commit record are
durably in MinIO. There is no separate flush to wait for. A query that carries
no commit token still depends on the catalog resolving a fresh listing, so allow
a couple of scrape intervals before concluding that something is wrong.

A Grafana panel needs at least two points to draw a line, so give the first
dashboard about 30 seconds before reading anything into a flat panel.

## Read the responses

Run the two `curl` commands from the
[README quickstart](../../README.md#quickstart). This is what comes back.

A PromQL instant query answers with the Prometheus JSON envelope:

```json
{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"system_cpu_load_average_1m"},"value":[1730000000.000,"0.42"]}]}}
```

- `status` is `success` or `error`. On `error` the envelope also carries
  `errorType` and `error`, and the HTTP status is 4xx or 5xx.
- `data.result` is an array of series. Each entry has the series labels under
  `metric` and one `[timestamp, value]` pair under `value`, with the value as a
  JSON string, exactly as Prometheus renders it.
- HTTP 401 means the `Authorization: Bearer` header is missing or carries a
  token the server has no tenant for. It is not an empty result: the query
  routes never answer an unauthenticated request with an empty one. The single
  exception is `/api/v1/metadata`, which returns an empty object for a tenant
  it cannot resolve, so do not use that route to test a token.

A SQL query answers with a described-schema envelope. This is the `data` object
of a `SELECT ts, value FROM samples LIMIT 1`:

```json
{"columns":[{"name":"ts","type":"Timestamp(Nanosecond, None)"},{"name":"value","type":"Float64"}],"rows":[[1751402400000000000,0.42]]}
```

- Each entry of `columns` is an object with a `name` and an Arrow `type`, in
  projection order. Each entry of `rows` is an array of values positional
  against that list.
- The full response wraps that object as
  `{"status":"success","data":{...},"stats":{...}}`, where `stats` is the
  query's cost accounting.
- A nanosecond timestamp comes back as an integer, a `series_id` as a hex
  string, and a label or attribute map as a JSON object. `NaN`, `+Inf`, and
  `-Inf` come back as strings, the way Prometheus renders them, so a client
  needs one rule and not two.
- The column list is the statement's projection, so `SELECT *` returns every
  column the table has. Ask for the columns you want if you care about the
  shape.

## When an empty result is the right answer

An empty result is a successful query that matched nothing. In the PromQL
envelope that is `"status":"success"` with `"result":[]`; in the SQL envelope it
is `"rows":[]`. Neither is an error, and neither means ingest is broken. The
common causes, in the order worth checking:

- **The first export has not landed yet.** Before roughly the first 20 seconds,
  every query over host metrics is legitimately empty.
- **The metric name is not the name you exported.** OTLP metric names get the
  standard Prometheus-style unit and `_total` suffixes at ingest, so a monotonic
  `foo` with `unit: "By"` is stored as `foo_bytes_total`. Query
  `/api/v1/label/__name__/values` to see the names Ravel actually holds. The
  [query guide](query.md) covers the naming rules.
- **The table is empty on this stack, by design.** The quickstart Collector
  sends host metrics only, so `SELECT * FROM logs LIMIT 5` and
  `SELECT * FROM spans LIMIT 5` return zero rows on a fresh stack. That is
  correct, not a failure: logs and traces need a Collector pipeline or an
  exporter that actually sends them.
- **The data was rejected at ingest for being too old.** Ravel refuses data
  points outside its event-time skew bound, so replaying yesterday's fixture
  stores nothing at all. See
  [event-time skew bounds](ingest.md#event-time-skew-bounds).
- **The query window does not cover the data.** A range query over a window that
  ends before the first sample is empty for the same reason as the first bullet.

If a query is empty and none of those explain it, ask the Collector whether it
is delivering at all: it publishes its own metrics on `127.0.0.1:18888`, and a
non-zero `send_failed` counter in that output means Ravel is refusing the
exports rather than that they never left.

## Read your own write

The quickstart above queries data that some other process wrote. To watch the
read-your-write path end to end, ingest one export, capture its
`x-ravel-commit-token` response header, and read that exact write back with
`min_commit_token`:

```sh
demo/walkthrough.sh
```

Run it with the stack up. It asserts every step, so a passing run is evidence
rather than output to read closely.

### What the commit token means

The commit token is not an opaque blob. It is base64url of
`v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`
([docs/catalog-and-mvcc.md](../catalog-and-mvcc.md#commit-tokens)). Those five
fields are exactly what the server needed to flush your data: which shard it
landed in, which writer process flushed it, and that writer's sequence number
and ingest-hour bucket. If you pass the token back as `min_commit_token`, the
catalog reconstructs the commit record's key directly and GETs it, instead of
listing and hoping a fresh listing includes it. This is what makes
read-your-write exact rather than probable.

The token locates a **commit record**: a small protobuf that notes where the
data lives, its content hash, and its sample and series counts. The commit
record in turn names a **data object**: the RSEG segment that holds your
samples, column-encoded and immutable. Object keys look like this
([docs/catalog-and-mvcc.md](../catalog-and-mvcc.md#key-layout-all-under-one-bucket-root)):

```
data key:   t/<tenant_hash>/m/l0/<shard>/<writer_id>.<epoch>.<seq>.<hash16>.rseg
commit key: t/<tenant_hash>/m/c/<shard>/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt
```

A request that spreads across shards gets one token per shard, comma-separated
in the header. Passing the whole header value back is correct: the catalog
requires every token in it.

To list what the stack actually wrote, see
[inspecting data](inspecting-data.md).

## Prove the durability claim

`demo/kill-and-recover.sh` ingests one export under strict acknowledgement,
`SIGKILL`s the `ravel-server` container, replaces it with a fresh one, and reads
the pre-kill sample back by its commit token. Nothing crosses the kill except
what is in MinIO. See
[kill the server, keep the data](../../README.md#kill-the-server-keep-the-data).

The claim it proves is specific to strict acknowledgement, which is the default.
Buffered acknowledgement, opt-in per request, returns before the flush and
carries no commit token, so a crash loses its buffered window. The
[consistency model](../consistency-model.md#acknowledgement-semantics) is
normative for both.

## Stop the stack

```sh
docker compose -f deploy/docker-compose/ravel.yml down
```

`minio-data/` on your machine persists across runs, and the store-qualify
one-shot is idempotent, so bringing the stack up again on the same directory is
safe. Delete that directory to start from an empty store.

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

Everything above needs no toolchain. This section does, and it is for people
changing Ravel's code rather than evaluating it. The
[development guide](../internal/development.md) covers the workflow in depth.

### Prerequisites for this section only

- Rust, pinned by [`rust-toolchain.toml`](../../rust-toolchain.toml) to 1.97.1
  (edition 2024). If you use `rustup`, it installs this version automatically
  the first time you run `cargo` in the repository.
- Docker with `docker compose`, for the local MinIO stack.

### Bring up MinIO

```sh
make minio
```

This runs `docker compose -f deploy/docker-compose/minio.yml up -d`
([deploy/docker-compose/minio.yml](../../deploy/docker-compose/minio.yml)). It
starts MinIO on `127.0.0.1:9000` (S3 API) and `127.0.0.1:9001` (web console),
with credentials `ravel` / `ravel-dev-secret`, and a one-shot `createbucket`
service that creates the `ravel-dev` bucket. Data lives in `./minio-data` on
your machine. `make minio-down` stops the stack without deleting it.

### Run the demo

```sh
make demo
```

`make demo` builds `ravel-server` and `ravel-cli` in release mode, then runs
[scripts/demo.sh](../../scripts/demo.sh), which starts MinIO if it is not
already up, creates the bucket, generates a fresh OTLP metrics export with
current timestamps, starts `ravel-server --store s3` on `127.0.0.1:14318`
(HTTP) and `127.0.0.1:14317` (gRPC) against MinIO, posts the export, captures
the `x-ravel-commit-token`, and queries the series back with that token as
`min_commit_token`.

It generates the fixture on every run and does not check it in, because a stale
fixture would fall outside the
[event-time skew bounds](ingest.md#event-time-skew-bounds) and make the demo
fail non-deterministically.

The expected output ends with two lines like this, then `[demo] demo complete`:

```
export result: commit_token=<base64url string>
query result: {"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[<ts>,"<value>"]}]}}
```

### SQL on the from-source path

`make demo` does not build the `sql` feature, so `POST /api/v1/sql` returns
nothing useful there. `--features` is a cargo argument, not a `ravel-server`
flag: the feature is chosen when the binary is built, so ask cargo for it and
pass the server's own flags after `--`.

Two startup gates stand between a fresh bucket and a running server. The
compose quickstart and `make demo` clear both for you; by hand you clear them
yourself. The server refuses to start against a bucket that carries no
qualification record, so qualify the bucket once with `ravel-cli`. It also
refuses to start against a fresh bucket with no tenant-hash scheme chosen, so
choose one: keyed with `--tenant-hash-key-file` for a real deployment, or
unkeyed with `--tenant-hash-unkeyed` for a throwaway development bucket.
Either choice is permanent for that bucket.

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

That binds the defaults, `127.0.0.1:4318` (HTTP) and `127.0.0.1:4317` (gRPC),
and accepts requests carrying `Authorization: Bearer devtoken` for tenant
`acme`. The environment variables stand in for the `--s3-*` flags, and both
binaries read them the same way. `store qualify` is idempotent: on an
already-qualified bucket it reports the existing record and exits 0. Drop
`--features sql` and the same command gives you the PromQL and ingest surfaces
without the SQL endpoint.

### The same round trip by hand

An OTLP HTTP export is any valid `ExportMetricsServiceRequest` protobuf;
`cargo run -p ravel-server --example gen_otlp_fixture > fixture.pb` produces one
with current timestamps. Send it and keep the response headers:

```sh
curl -s -D headers.txt -o /dev/null \
  -X POST http://127.0.0.1:4318/v1/metrics \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @fixture.pb

grep -i ^x-ravel-commit-token headers.txt
```

The response is a strict-mode acknowledgement: it returns only after the segment
and its commit record are durably in the object store
([docs/consistency-model.md](../consistency-model.md#acknowledgement-semantics)).
Query the write back with the token from that header:

```sh
curl -s -G http://127.0.0.1:4318/api/v1/query \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode "query=demo_requests_total" \
  --data-urlencode "min_commit_token=<token from the header above>"
```

The envelope is the same one described in
[read the responses](#read-the-responses) above, and the same empty-result
causes apply. To see the objects this wrote, use
[inspecting data](inspecting-data.md).
