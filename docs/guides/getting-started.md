# Getting started

## Prerequisites

- Rust, pinned by [`rust-toolchain.toml`](../../rust-toolchain.toml) to
  1.97.1 (edition 2024). If you use `rustup`, it installs this version
  automatically the first time you run `cargo` in the repo.
- Docker with `docker compose`, for the local MinIO stack. Every command in
  this guide that needs an object store is marked so.

## Bring up MinIO

```sh
make minio
```

This runs `docker compose -f deploy/docker-compose/minio.yml up -d`
([deploy/docker-compose/minio.yml](../../deploy/docker-compose/minio.yml)): a
MinIO server on `127.0.0.1:9000` (S3 API) and `127.0.0.1:9001` (web console),
credentials `ravel` / `ravel-dev-secret`, plus a one-shot `createbucket`
service that creates the `ravel-dev` bucket. Data lives in `./minio-data` on
your machine; `make minio-down` stops the stack without deleting it.

## The demo

```sh
make demo
```

`make demo` builds `ravel-server` and `ravel-cli` in release mode, then runs
[scripts/demo.sh](../../scripts/demo.sh). It's the fastest way to see the
whole ingest-to-query path work. Line by line, it:

1. Starts MinIO if it isn't already running, and waits for
   `/minio/health/live`.
2. Creates the `ravel-dev` bucket (harmless if it already exists).
3. Builds `ravel-server` and `ravel-cli`.
4. Generates a fresh OTLP metrics export (`gen_otlp_fixture` example) with
   current timestamps. The fixture is generated on every run, not checked
   in, because ingest rejects data points more than 2 hours old (see
   [ingest.md](ingest.md#event-time-skew)) and a stale fixture would make
   the demo fail non-deterministically.
5. Starts `ravel-server --store s3` on `127.0.0.1:14318` (HTTP) and
   `127.0.0.1:14317` (gRPC), pointed at MinIO, with one tenant token
   (`demo-token` maps to tenant `demo-tenant`).
6. Waits until the server answers `/api/v1/query`.
7. `POST`s the OTLP fixture to `/v1/metrics` and captures the
   `x-ravel-commit-token` response header.
8. Queries `demo_requests_total` back via `/api/v1/query`, passing that
   token as `min_commit_token`.
9. Prints both the commit token and the query response, and checks the
   response says `"status":"success"` and contains the series.

Expected output ends with two lines like:

```
export result: commit_token=<base64url string>
query result: {"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[<ts>,"<value>"]}]}}
```

followed by `[demo] demo complete`.

### What the commit token means

The commit token is not an opaque blob; it's base64url of
`v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`
([docs/catalog-and-mvcc.md](../catalog-and-mvcc.md#commit-tokens)). Those five
fields are exactly what the server needed to flush your data: which shard it
landed in, which writer process flushed it, and that writer's sequence
number and ingest-hour bucket. Passing the token back as `min_commit_token`
lets the catalog reconstruct the commit record's key directly and GET it,
instead of listing and hoping a fresh listing includes it. That's what makes
read-your-write work.

The token locates a **commit record** (a small protobuf noting where the
data lives, its content hash, and its sample/series counts), which in turn
names a **data object**: the actual RSEG segment holding your samples,
column-encoded and immutable. Object keys look like this
([docs/catalog-and-mvcc.md](../catalog-and-mvcc.md#key-layout)):

```
data key:   t/<tenant_hash>/m/l0/<shard>/<writer_id>.<epoch>.<seq>.<hash16>.rseg
commit key: t/<tenant_hash>/m/c/<shard>/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt
```

To see this for real, list what the demo actually wrote (requires MinIO,
run after `make demo`; see [inspecting-data.md](inspecting-data.md) for more):

```sh
RAVEL_S3_ENDPOINT=http://127.0.0.1:9000 RAVEL_S3_BUCKET=ravel-dev \
RAVEL_S3_ACCESS_KEY=ravel RAVEL_S3_SECRET_KEY=ravel-dev-secret \
cargo run -p ravel-cli -- --store s3 catalog list --tenant demo-tenant --hours 1
```

Each printed line starts with a data object key: that's the segment the
commit record you passed as `min_commit_token` points to.

## The same flow, by hand

Start `ravel-server` yourself against MinIO (requires `make minio` to be
running):

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
(gRPC), and accepts requests carrying `Authorization: Bearer devtoken` for
tenant `acme`. Send one OTLP HTTP export (any valid
`ExportMetricsServiceRequest` protobuf; `cargo run -p ravel-server --example
gen_otlp_fixture > fixture.pb` produces one with current timestamps) and
capture the response headers:

```sh
curl -s -D headers.txt -o /dev/null \
  -X POST http://127.0.0.1:4318/v1/metrics \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @fixture.pb

grep -i ^x-ravel-commit-token headers.txt
```

The response is a strict-mode ack: it only returns once the segment and its
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

You get the same Prometheus-style JSON envelope the demo checks for. See
[docs/guides/ingest.md](ingest.md) and [docs/guides/query.md](query.md) for
everything else these two endpoints accept.
