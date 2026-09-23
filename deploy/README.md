# deploy/

Deployment assets for Ravel: Docker Compose stacks, Kubernetes manifests, and
the IAM policy documents the operator renders into Secrets.

## Prometheus alerting rules (`prometheus/ravel.rules.yaml`)

`prometheus/ravel.rules.yaml` is the shipped alert rule file: 32 alerts in 8
groups. Each carries the threshold its source document states, and the
duration that document states where it states one. The sources are
[the observability guide](../docs/guides/observability.md), which explains the
groups it states durations for, and
[the troubleshooting guide](../docs/guides/operations/troubleshooting.md),
whose symptom table states the rest. 15 of the 32 carry no `for:` because the
row they come from states no duration; each of those says so in an
`as_documented` annotation, and every rule carries a `runbook` annotation
naming the section to read when it fires.

Load it from a Prometheus server's `rule_files:` key:

```yaml
rule_files:
  - /etc/prometheus/ravel.rules.yaml
```

or wrap the same `groups:` list in a `PrometheusRule` custom resource under
the Prometheus Operator. This directory ships the rule file alone, not a
scrape configuration: point your own Prometheus at the Ravel processes you
run.

`services/ravel-server/tests/shipped_rules_name_emitted_metrics.rs` pins the
names. It parses this file into its 8 groups and 32 rules, extracts the 38
distinct `ravel_` metric names their expressions and annotations reference,
and asserts each one appears on a `# TYPE` line of a `/metrics` body rendered
by a running server, so renaming a metric in the code fails the test instead
of leaving a rule here matching no series. Parsing rather than scanning is
what makes the rule count mean something: a corruption that leaves the
`- alert:` lines intact keeps a string count at 32 while Prometheus refuses
the whole file, and the reader the test uses refuses any line it cannot
account for, so that corruption fails there instead. It checks structure
only: a malformed `for:` duration or a broken `expr` is caught by Prometheus
on load rather than by the test (issue #1928).

The same test parses the six fenced `yaml` blocks the
[observability guide](../docs/guides/observability.md) prints, and asserts
that each of the 14 rules they hold carries the same expression, `for:`
duration and severity as the rule of that name here. The blocks stay on the
page because they are where each rule is explained, and the comparison is
what stops them drifting into rules a reader can copy but nothing else
ships.

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

## Pinned image digests (docker-compose/ravel.yml and docker-compose/minio.yml)

Every third-party image in the two quickstart compose files is pinned to a
release tag plus an immutable `@sha256:` manifest digest (issue #1720), the
same convention `deploy/metricsbench/` uses. `docker-compose/minio.yml` is
`docker-compose/ravel.yml`'s standalone MinIO-and-bucket mirror (same
credentials, same `ravel-dev` bucket), so it carries the identical MinIO
pins. The MinIO pair reuses the exact pins already resolved for
`deploy/metricsbench/docker-compose.yml`; see that directory's README for
the resolution recipe per registry.

| Image | Registry | Tag | Digest |
|---|---|---|---|
| `minio/minio` | quay.io | `RELEASE.2025-04-08T15-41-24Z` | `sha256:8834ae47a2de3509b83e0e70da9369c24bbbc22de42f2a2eddc530eee88acd1b` |
| `minio/mc` | quay.io | `RELEASE.2025-04-08T15-39-49Z` | `sha256:7e3efb09c22c0882fbf341b9d99f61f94ae6c4c20a06f2f1a2b20ea8993d8952` |
| `open-telemetry/opentelemetry-collector-releases/opentelemetry-collector-contrib` | ghcr.io | `0.160.0` | `sha256:799dc6cf12c96192af37b5bdba804da8c10b3bc563b43cb90c3f3c58d9572ad6` |
| `grafana/grafana` | docker.io | `13.2.2` | `sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0` |

`deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh` enforces
this: every `image:` line in `docker-compose/ravel.yml` and
`docker-compose/minio.yml`, except the two `${RAVEL_IMAGE:-...}` references
in `ravel.yml`, must carry a digest, checked in CI's `doc-scripts` job.
`deploy/k8s`'s four registry images carry digests too, checked by the same
job. The two locally built placeholders there (`ravel-server`,
`ravel-operator`) are loaded into kind by tag and are exempt by exact
string.
