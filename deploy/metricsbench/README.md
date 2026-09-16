# MetricsBench comparator deployments

Checked-in, digest-pinned deployments of the cross-engine comparators for the
MetricsBench metrics benchmark (ADR-0927, issue #934). These are the systems the
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
| `mimir` | Grafana Mimir monolithic (`-target=all`); the ADR-0927 object-storage-native PromQL system | **Object storage (S3).** Ack = local WAL; blocks ship to S3 (MinIO here) later. See caveat below. | `http://127.0.0.1:9009/api/v1/push` | `http://127.0.0.1:9009/prometheus/api/v1/query` |

Supporting services: `minio` (S3 backend for Mimir) and `createbuckets` (a
one-shot that creates Mimir's buckets and then exits).

### Storage architecture is not interchangeable

Prometheus and VictoriaMetrics persist to **local disk**. Mimir is
**object-storage-native** and writes its durable blocks to S3 — but in this stack
S3 is provided by **MinIO**, which is local-disk-backed and charges no
per-request fees. Per ADR-0927 decision 10 (ADR-0075 decision 3), a MinIO result
is valid for correctness and CI only and is **never a publishable performance or
cost result**; a real-S3 substrate is required for that, exactly as Ravel's own
publishable numbers require real S3. Each service block in
`docker-compose.yml` states its own durability next to its configuration so this
distinction is readable without this README or the ADR open.

Every backend-specific behaviour that transforms, delays, or ages the stored data
— Prometheus' 2h head-block compaction and 45d retention, VictoriaMetrics'
background merges and disabled deduplication, Mimir's delayed block upload to S3
and its compactor — is disclosed in a comment next to the setting that causes it,
in `docker-compose.yml` or `config/mimir.yaml`. No behaviour-changing tuning
lives outside these files.

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

## Verify the pins

```sh
deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh
```

Originally scoped to this directory's compose file alone (issue #934), the
script now also checks the two repo-wide supply-chain pins issue #1310 added,
plus the quickstart compose files issue #1720 added: every `FROM`/`ARG` base
image in the root `Dockerfile` and `Dockerfile.prebuilt`, every `uses:` action
reference under `.github/workflows/` and `.github/actions/`, and every
`image:` reference in `deploy/docker-compose/ravel.yml` and
`deploy/docker-compose/minio.yml`. All four categories run in one invocation
and each is checked against its own expected count:

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
  `deploy/docker-compose/minio.yml`): every `image:` reference except the two
  `${RAVEL_IMAGE:-...}` references in `ravel.yml` (Ravel's own released
  image, excluded by exact match) carries an `@sha256:` digest, and both the
  total image-line count (8, across both files) and the pin-required count
  (6) equal their expected totals. Scoped to these two files: `deploy/k8s`
  carries four registry images that are not yet pinned (`minio.yaml` lines
  49 and 140, `floci.yaml` lines 74 and 157) plus two locally built
  placeholders (`ravel-server`, `ravel-operator`); pinning the k8s manifests
  is a separate ticket.

Exit 0 means all four categories passed. Any unpinned reference, any missing
required comparator, or a reference count that drifts from any category's
expected number fails the check with a non-zero exit. The script prints what
it checked and how many references it found in each category.

This check runs in CI, wired into the `doc-scripts` job in
`.github/workflows/ci.yml`.

## Pinned image digests

Every digest below is the multi-arch manifest-list digest reported by the
named tag's own registry. The recipe differs by registry, so the two below
are not interchangeable: substituting a quay.io repo path into the Docker
Hub URL (or vice versa) resolves nothing.

**`prom/prometheus`, `victoriametrics/victoria-metrics`, and `grafana/mimir`**
are still on Docker Hub and resolve with the Docker Hub registry v2 API:

```sh
curl -sI -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
  https://registry-1.docker.io/v2/<repo>/manifests/<tag> \
  | grep -i docker-content-digest
```

**`quay.io/minio/minio` and `quay.io/minio/mc`** (see deploy/README.md for why
the MinIO pair lives on quay.io) resolve with quay's own v2 API instead: quay
issues a bearer token from a separate auth endpoint rather than accepting one
minted for Docker Hub.

This is written as two curl steps rather than a one-liner so the token is
carried over by hand instead of captured into a shell variable.

```sh
# 1. Get a bearer token scoped to the repo, and read the token field out
#    of the JSON it prints.
curl -s "https://quay.io/v2/auth?service=quay.io&scope=repository:minio/minio:pull" \
  | jq -r .token

# 2. HEAD the manifest list, pasting that token in place of <TOKEN> below,
#    with the manifest-list Accept header.
curl -sI -H "Authorization: Bearer <TOKEN>" \
  -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
  https://quay.io/v2/minio/minio/manifests/RELEASE.2025-04-08T15-41-24Z \
  | grep -i docker-content-digest

# 3. The docker-content-digest response header is the pinned digest.
```

| Image | Tag | Digest |
|---|---|---|
| `prom/prometheus` | `v3.13.1` | `sha256:3c42b892cf723fa54d2f262c37a0e1f80aa8c8ddb1da7b9b0df9455a35a7f893` |
| `victoriametrics/victoria-metrics` | `v1.115.0` | `sha256:d8ac3a1776c8a9beead8bbd42a489c82249b1bfe9071dfd4813f34ebe36354bb` |
| `grafana/mimir` | `2.14.2` | `sha256:2d3912435771d356ec03ae4729fb584b4d76a5f035d9dda40b563a55bb6760e3` |
| `quay.io/minio/minio` | `RELEASE.2025-04-08T15-41-24Z` | `sha256:8834ae47a2de3509b83e0e70da9369c24bbbc22de42f2a2eddc530eee88acd1b` |
| `quay.io/minio/mc` | `RELEASE.2025-04-08T15-39-49Z` | `sha256:7e3efb09c22c0882fbf341b9d99f61f94ae6c4c20a06f2f1a2b20ea8993d8952` |

The tag is kept in each `image:` reference alongside the digest for human
readability; the digest is what pins the run.

### Pins shared with deploy/docker-compose/ravel.yml and minio.yml (issue #1720)

The two quickstart compose files (`deploy/docker-compose/ravel.yml` and
`deploy/docker-compose/minio.yml`) are scanned by the same script as its
fourth category (see above), so their pins are recorded here too. The MinIO
pair is the exact same tag and digest this directory already uses above,
and is identical across both quickstart files (`minio.yml` is `ravel.yml`'s
standalone MinIO mirror); the other two are `ravel.yml`-only.

| Image | Tag | Digest |
|---|---|---|
| `quay.io/minio/minio` | `RELEASE.2025-04-08T15-41-24Z` | `sha256:8834ae47a2de3509b83e0e70da9369c24bbbc22de42f2a2eddc530eee88acd1b` |
| `quay.io/minio/mc` | `RELEASE.2025-04-08T15-39-49Z` | `sha256:7e3efb09c22c0882fbf341b9d99f61f94ae6c4c20a06f2f1a2b20ea8993d8952` |
| `ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector-contrib` | `0.160.0` | `sha256:799dc6cf12c96192af37b5bdba804da8c10b3bc563b43cb90c3f3c58d9572ad6` |
| `grafana/grafana` | `13.2.2` | `sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0` |

See `deploy/README.md` for why these four registries were chosen.

## Note on the acceptance check name

Issue #934 names the acceptance test
`metricsbench::deploy::tests::every_comparator_pins_an_image_digest`, a Rust test
path. That contradicts the issue's own scope line ("touches no crate"), and the
crate it would live in is being edited by a parallel task. The check is therefore
implemented as the dependency-free script
`tests/every_comparator_pins_an_image_digest.sh`, preserving the name exactly.
