# deploy/

Deployment assets for Ravel: Docker Compose stacks, Kubernetes manifests, and
the IAM policy documents the operator renders into Secrets.

## Prometheus alerting rules (`prometheus/ravel.rules.yaml`)

`prometheus/ravel.rules.yaml` is the shipped alert rule file: 33 alerts in 8
groups. Each carries the threshold its source document states, and the
duration that document states where it states one. The sources are
[the observability guide](../docs/guides/observability.md), which explains the
groups it states durations for, and
[the troubleshooting guide](../docs/guides/operations/troubleshooting.md),
whose symptom table states the rest. 15 of the 33 carry no `for:` because the
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
names. It parses this file into its 8 groups and 33 rules, extracts the 38
distinct `ravel_` metric names their expressions and annotations reference,
and asserts each one appears on a `# TYPE` line of a `/metrics` body rendered
by a running server, so renaming a metric in the code fails the test instead
of leaving a rule here matching no series. Parsing rather than scanning is
what makes the rule count mean something: a corruption that leaves the
`- alert:` lines intact keeps a string count at 33 while Prometheus refuses
the whole file, and the reader the test uses refuses any line it cannot
account for, so that corruption fails there instead. It checks structure
only: a malformed `for:` duration or a broken `expr` is caught by Prometheus
on load rather than by the test (issue #1928).

The same test parses the six fenced `yaml` blocks the
[observability guide](../docs/guides/observability.md) prints, and asserts
that each of the 15 rules they hold carries the same expression, `for:`
duration and severity as the rule of that name here. The blocks stay on the
page because they are where each rule is explained, and the comparison is
what stops them drifting into rules a reader can copy but nothing else
ships.

## Grafana dashboard (`grafana/dashboards-standalone/ravel.json`)

`grafana/dashboards-standalone/ravel.json` is the shipped dashboard over Ravel's own
`ravel_` families: 36 panels in 6 rows, named Ingest, Query, Catalog fold,
Maintenance, Object store and probe, and Alerting. Between them the panels
reference 107 distinct `ravel_` metric names. Import it into Grafana and pick
the Prometheus data source that scrapes your Ravel processes; the dashboard
carries a single `ds` data-source variable and no hardcoded data-source uid.

It lives OUTSIDE `grafana/dashboards/`, and that placement is load-bearing.
`deploy/docker-compose/ravel.yml` mounts the whole `grafana/dashboards`
directory into the quickstart's Grafana, and
`grafana/provisioning/dashboards/ravel.yaml` is a `type: file` provider over
that path, so anything dropped in there is auto-provisioned. The quickstart
scrapes no Ravel process: its collector declares only the `hostmetrics`
receiver, so no `ravel_` series exists in the only datasource that dashboard
could reach, and all 36 panels would draw nothing. A blank dashboard as the
quickstart's first impression is the same silent failure this file's own test
exists to prevent. Do not move it back under `grafana/dashboards/` without
also giving the quickstart a scrape path for ravel-server's `/metrics`.

`grafana/dashboards/ravel-overview.json` is a different thing and stays where
it is: it belongs to the quickstart compose stack and graphs the bundled
collector's host CPU, not any Ravel family.

The same test that pins the rule file's names,
`services/ravel-server/tests/shipped_rules_name_emitted_metrics.rs`, pins the
dashboard's. It parses the JSON, walks every object carrying a `targets`
array, extracts the metric names from each target's PromQL expression, and
asserts each one appears on a `# TYPE` line of a `/metrics` body rendered by a
running server, through the same scanner and the same rendered bodies the rule
file goes through. It also asserts that every one of the 37 metric names the
shipped rules alert on is graphed by some panel here, so a page always has a
panel to land on. The 6 rows, 36 panels, 104 targets and 107 names are pinned
as literals, so a walk that stops finding panels fails rather than checking an
empty set.

What that check covers is the metric NAMES. It does not validate the PromQL
around them, the label matchers inside a selector, the panel layout, or the
dashboard against Grafana's own schema, and it cannot tell that a panel will
draw data in your deployment: a name that renders here may still carry no
series for a label combination a panel splits on, or for a family behind a
setting your processes leave off.

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
