# deploy/

Deployment assets for Ravel: Docker Compose stacks, Kubernetes manifests, and
the IAM policy documents the operator renders into Secrets.

## Why MinIO and the OpenTelemetry Collector are not pulled from Docker Hub

Docker Hub's anonymous pull allowance is per source IP and shared across
every project on a runner. A public image still fails to pull with "pull
access denied ... may require 'docker login'" once that shared allowance is
exhausted, which reads like a permissions problem rather than a rate limit.

Where an anonymous mirror exists on another registry, this directory pulls
from there instead: MinIO's images from `quay.io/minio/...`, and the
OpenTelemetry Collector from
`ghcr.io/open-telemetry/opentelemetry-collector-releases/...`. Neither
registry shares Docker Hub's allowance. Every file that does this points
back to this note with a one-line comment instead of repeating the
rationale.

An image left on Docker Hub in this directory (for example `grafana/grafana`
in `docker-compose/ravel.yml`) carries its own comment naming the other
registries checked and why none of them had a usable copy.

## Pinned image digests (docker-compose/ravel.yml)

Every third-party image in the quickstart compose file is pinned to a release
tag plus an immutable `@sha256:` manifest digest (issue #1720), the same
convention `deploy/metricsbench/` uses. The MinIO pair reuses the exact pins
already resolved for `deploy/metricsbench/docker-compose.yml`; see that
directory's README for the resolution recipe per registry.

| Image | Registry | Tag | Digest |
|---|---|---|---|
| `minio/minio` | quay.io | `RELEASE.2025-04-08T15-41-24Z` | `sha256:8834ae47a2de3509b83e0e70da9369c24bbbc22de42f2a2eddc530eee88acd1b` |
| `minio/mc` | quay.io | `RELEASE.2025-04-08T15-39-49Z` | `sha256:7e3efb09c22c0882fbf341b9d99f61f94ae6c4c20a06f2f1a2b20ea8993d8952` |
| `open-telemetry/opentelemetry-collector-releases/opentelemetry-collector-contrib` | ghcr.io | `0.160.0` | `sha256:799dc6cf12c96192af37b5bdba804da8c10b3bc563b43cb90c3f3c58d9572ad6` |
| `grafana/grafana` | docker.io | `13.2.2` | `sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0` |

`deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh` enforces
this: every `image:` line in `docker-compose/ravel.yml` except the two
`${RAVEL_IMAGE:-...}` references must carry a digest, checked in CI's
`doc-scripts` job.
