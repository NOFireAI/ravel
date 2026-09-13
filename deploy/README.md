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
