# Getting started

There are two ways to run Ravel. The **container-first quickstart** pulls a
published image and needs only Docker — it is the fastest way to see Ravel
work, and it is presented first. The **from-source path** (`make demo`) builds
Ravel from the current tree and is for contributors changing the code; it comes
second here and is covered in more depth in the
[development guide](development.md).

One capability difference matters up front: the published image is built with
`--features sql`, so `POST /api/v1/sql` works in the compose quickstart. `make
demo` builds the default feature set and does not enable `sql`, so the SQL
endpoint is unavailable on the from-source path. PromQL and ingest behave the
same on both.

## Container-first quickstart

The only prerequisite is Docker with `docker compose`. No Rust toolchain.

```sh
docker compose -f deploy/docker-compose/ravel.yml up -d
```

This brings up, all from published images:

- MinIO on `127.0.0.1:9000` (S3 API) and `127.0.0.1:9001` (console), plus a
  one-shot that creates the `ravel-dev` bucket and a one-shot that qualifies the
  store (ADR-0050 section 6) so `ravel-server` will start against it.
- `ravel-server` from `ghcr.io/nofireai/ravel-server:0.9.3` (override the pin
  with the `RAVEL_IMAGE` environment variable), listening on `127.0.0.1:4318`
  (HTTP/OTLP) and `127.0.0.1:4317` (gRPC/OTLP), with the tenant token
  `demo-token` mapped to tenant `demo-tenant`.
- An OpenTelemetry Collector scraping your host's CPU, load, memory, and network
  metrics and exporting them to Ravel over OTLP, authenticating with the demo
  bearer token.
- Grafana on `127.0.0.1:3000` (`admin` / `admin`) with the Ravel datasource
  already provisioned.

Every published port binds loopback (`127.0.0.1`) only, and every credential is
a fixed development value (`demo-token`, and `ravel` / `ravel-dev-secret` for
MinIO). None of it is for a network-reachable deployment.

Open Grafana at <http://127.0.0.1:3000> to see your machine's metrics, or query
Ravel directly. A PromQL instant query (the bearer token is required, the same
as ingest):

```sh
curl -s -H "Authorization: Bearer demo-token" \
  'http://127.0.0.1:4318/api/v1/query?query=system_cpu_load_average_1m'
```

SQL over the `samples` (metrics) table, which the image serves out of the box
because it is built `--features sql`:

```sh
curl -s -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer demo-token" \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT * FROM samples LIMIT 5"}'
```

To watch the read-your-write path end to end — ingest one export, capture its
`x-ravel-commit-token`, and read that exact write back with `min_commit_token` —
run [demo/walkthrough.sh](../../demo/walkthrough.sh) while the stack is up. Stop
everything with:

```sh
docker compose -f deploy/docker-compose/ravel.yml down
```

`minio-data/` on your machine persists across runs; the store-qualify one-shot
is idempotent, so bringing the stack up again on the same directory is safe.

## The from-source path

The rest of this guide builds Ravel from the current tree with `make demo`. Use
it when you are changing Ravel's code; otherwise the quickstart above is faster.

## Prerequisites

- Rust, pinned by [`rust-toolchain.toml`](../../rust-toolchain.toml) to
  1.97.1 (edition 2024). If you use `rustup`, it installs this version
  automatically the first time you run `cargo` in the repo.
- Docker with `docker compose`, for the local MinIO stack. This guide marks
  every command that needs an object store.

## Bring up MinIO

```sh
make minio
```

This runs `docker compose -f deploy/docker-compose/minio.yml up -d`
([deploy/docker-compose/minio.yml](../../deploy/docker-compose/minio.yml)). It
starts a MinIO server on `127.0.0.1:9000` (S3 API) and `127.0.0.1:9001` (web
console), with credentials `ravel` / `ravel-dev-secret`. It also starts a
one-shot `createbucket` service that creates the `ravel-dev` bucket. Data lives
in `./minio-data` on your machine. `make minio-down` stops the stack but does
not delete it.

## The demo

```sh
make demo
```

`make demo` builds `ravel-server` and `ravel-cli` in release mode, then runs
[scripts/demo.sh](../../scripts/demo.sh). It is the fastest way to see the
whole ingest-to-query path work. Line by line, it does this:

1. It starts MinIO if it is not already running, and waits for
   `/minio/health/live`.
2. It creates the `ravel-dev` bucket (harmless if it already exists).
3. It builds `ravel-server` and `ravel-cli`.
4. It generates a fresh OTLP metrics export (`gen_otlp_fixture` example) with
   current timestamps. It generates the fixture on every run and does not check
   it in. Ingest rejects data points more than 2 hours old (see
   [ingest.md](ingest.md#event-time-skew)), so a stale fixture would make the
   demo fail non-deterministically.
5. It starts `ravel-server --store s3` on `127.0.0.1:14318` (HTTP) and
   `127.0.0.1:14317` (gRPC), pointed at MinIO, with one tenant token
   (`demo-token` maps to tenant `demo-tenant`).
6. It waits until the server answers `/api/v1/query`.
7. It `POST`s the OTLP fixture to `/v1/metrics` and captures the
   `x-ravel-commit-token` response header.
8. It queries `demo_requests_total` back via `/api/v1/query`, and passes that
   token as `min_commit_token`.
9. It prints both the commit token and the query response. It then checks that
   the response says `"status":"success"` and contains the series.

The expected output ends with two lines like this:

```
export result: commit_token=<base64url string>
query result: {"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[<ts>,"<value>"]}]}}
```

Then it prints `[demo] demo complete`.

### What the commit token means

The commit token is not an opaque blob. It is base64url of
`v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`
([docs/catalog-and-mvcc.md](../catalog-and-mvcc.md#commit-tokens)). Those five
fields are exactly what the server needed to flush your data: which shard it
landed in, which writer process flushed it, and that writer's sequence
number and ingest-hour bucket. If you pass the token back as `min_commit_token`,
the catalog reconstructs the commit record's key directly and GETs it, instead
of listing and hoping a fresh listing includes it. This is what makes
read-your-write work.

The token locates a **commit record**: a small protobuf that notes where the
data lives, its content hash, and its sample and series counts. The commit
record in turn names a **data object**: the actual RSEG segment that holds your
samples, column-encoded and immutable. Object keys look like this
([docs/catalog-and-mvcc.md](../catalog-and-mvcc.md#key-layout)):

```
data key:   t/<tenant_hash>/m/l0/<shard>/<writer_id>.<epoch>.<seq>.<hash16>.rseg
commit key: t/<tenant_hash>/m/c/<shard>/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt
```

To see this for real, list what the demo actually wrote. This needs MinIO; run
it after `make demo` (see [inspecting-data.md](inspecting-data.md) for more):

```sh
RAVEL_S3_ENDPOINT=http://127.0.0.1:9000 RAVEL_S3_BUCKET=ravel-dev \
RAVEL_S3_ACCESS_KEY=ravel RAVEL_S3_SECRET_KEY=ravel-dev-secret \
cargo run -p ravel-cli -- --store s3 catalog list --tenant demo-tenant --hours 1
```

Each printed line starts with a data object key. That is the segment that your
`min_commit_token` commit record points to.

## The same flow, by hand

Start `ravel-server` yourself against MinIO (`make minio` must be running):

```sh
cargo run -p ravel-server -- \
  --store s3 \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket ravel-dev \
  --s3-access-key ravel \
  --s3-secret-key ravel-dev-secret \
  --tenant-token devtoken=acme
```

This binds the defaults, `127.0.0.1:4318` (HTTP) and `127.0.0.1:4317`
(gRPC). It accepts requests that carry `Authorization: Bearer devtoken` for
tenant `acme`. An OTLP HTTP export is any valid `ExportMetricsServiceRequest`
protobuf; `cargo run -p ravel-server --example gen_otlp_fixture > fixture.pb`
produces one with current timestamps. Send one export and capture the response
headers:

```sh
curl -s -D headers.txt -o /dev/null \
  -X POST http://127.0.0.1:4318/v1/metrics \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @fixture.pb

grep -i ^x-ravel-commit-token headers.txt
```

The response is a strict-mode ack. It returns only after the segment and its
commit record are durably in the object store
([docs/consistency-model.md](../consistency-model.md#acknowledgement-semantics)).
Read the `x-ravel-commit-token` header (comma-separated if your points
landed in more than one shard) and query it back:

```sh
curl -s -G http://127.0.0.1:4318/api/v1/query \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode "query=demo_requests_total" \
  --data-urlencode "min_commit_token=<token from the header above>"
```

You get the same Prometheus-style JSON envelope that the demo checks for. For
everything else these two endpoints accept, see
[docs/guides/ingest.md](ingest.md) and [docs/guides/query.md](query.md).
