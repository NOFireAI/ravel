# deploy/

Deployment assets for Ravel: Docker Compose stacks, Kubernetes manifests, the
IAM policy documents the operator renders into Secrets, a Prometheus rule
file and a Grafana dashboard.

## Prometheus alerting rules (`prometheus/ravel.rules.yaml`)

Load the shipped alert rule file from a Prometheus server's `rule_files:` key:

```yaml
rule_files:
  - /etc/prometheus/ravel.rules.yaml
```

Under the Prometheus Operator, wrap the same `groups:` list in a
`PrometheusRule` custom resource. This directory ships no scrape
configuration: point your own Prometheus at the Ravel processes you run.

The file holds 36 alerts in 9 groups. Each carries the threshold its source
document states, and the duration that document states where it states one:

- [The observability guide](../docs/guides/observability.md) explains the
  groups it states durations for.
- [The troubleshooting guide](../docs/guides/operations/troubleshooting.md)
  symptom table states the rest. 17 of the 36 carry no `for:` because the row
  they come from states no duration; each of those says so in an
  `as_documented` annotation.

Every rule carries a `runbook` annotation naming the section to read when it
fires.

## Grafana dashboard (`grafana/dashboards-standalone/ravel.json`)

Import `grafana/dashboards-standalone/ravel.json` into Grafana and pick the
Prometheus data source that scrapes your Ravel processes. The dashboard
carries a single `ds` data-source variable and no hardcoded data-source uid.

It graphs Ravel's own `ravel_` families: 36 panels in 6 rows, named Ingest,
Query, Catalog fold, Maintenance, Object store and probe, and Alerting.
Between them the panels reference 117 distinct `ravel_` metric names.

Keep it outside `grafana/dashboards/`. `deploy/docker-compose/ravel.yml`
mounts that whole directory into the quickstart's Grafana, and
`grafana/provisioning/dashboards/ravel.yaml` is a `type: file` provider over
it, so anything there is auto-provisioned. The quickstart scrapes no Ravel
process: its collector declares only the `hostmetrics` receiver, so no
`ravel_` series exists in the only data source that dashboard could reach,
and all 36 panels would draw nothing. Move it there only after giving the
quickstart a scrape path for ravel-server's `/metrics`.

`grafana/dashboards/ravel-overview.json` belongs to the quickstart compose
stack and stays where it is. It graphs the bundled collector's host CPU, not
any Ravel family.

## How the rules and the dashboard are checked

`services/ravel-server/tests/shipped_rules_name_emitted_metrics.rs` checks
both files against a `/metrics` body rendered by a running server:

- It parses the rule file into its 9 groups and 36 rules and asserts that
  each of the 42 distinct `ravel_` metric names their expressions and
  annotations reference appears on a `# TYPE` line. It also checks each
  `for:` duration against Prometheus's duration grammar and parses every
  `expr` with Ravel's own PromQL parser.
- It parses the seven fenced `yaml` blocks the
  [observability guide](../docs/guides/observability.md) prints, and asserts
  that each of the 16 rules they hold carries the same expression, `for:`
  duration and severity as the rule of that name here.
- It extracts the metric names from every dashboard target's PromQL and
  asserts each one appears on a `# TYPE` line, and that every one of the 42
  names the rules alert on is graphed by some panel. The 6 rows, 36 panels,
  114 targets and 117 names are pinned as literals.

The dashboard check covers metric names only. It does not validate the
PromQL around them, the label matchers inside a selector, the panel layout,
or the dashboard against Grafana's schema. A panel can still draw nothing in
your deployment: a name that renders here may carry no series for a label
combination the panel splits on, or for a family behind a setting your
processes leave off.

## Image registries

Docker Hub's anonymous pull allowance is per source IP and shared across
every project on a runner. Once it runs out, a public image fails with "pull
access denied ... may require 'docker login'", which reads like a
permissions problem rather than a rate limit. So this directory pulls from
registries that do not share that allowance:

| Image | Registry |
|---|---|
| Object store (RustFS) | `ghcr.io/rustfs/rustfs` |
| Its client, the AWS CLI | `public.ecr.aws/aws-cli/aws-cli`, except in deploy/metricsbench, which pulls it from Docker Hub |
| OpenTelemetry Collector | `ghcr.io/open-telemetry/opentelemetry-collector-releases/...` |

Each file that does this points back here with a one-line comment. MinIO is
not used because its public images cannot be pulled without credentials from
Docker Hub or quay.io. An image left on Docker Hub (for example
`grafana/grafana` in `docker-compose/ravel.yml`) carries its own comment
naming the other registries checked and why none had a usable copy.

ECR Public throttles unauthenticated pulls per source IP on its own terms. A
burst gets `toomanyrequests: Rate exceeded`, which `docker run` reports as
exit 125. Where that matters:

- The CI jobs that create a RustFS bucket (ci.yml's `object-store-contract`
  and `bench-smoke`) do not pull the AWS CLI image. They use the aws CLI the
  GitHub runner image ships (`scripts/ci-create-bucket.sh`).
- The quickstart runs `createbucket` from ECR Public, and so does the
  scheduled `quickstart-published` workflow, which does not gate a merge.
- metricsbench-nightly's `metricsbench-rustfs` job runs the deploy/metricsbench
  stack through `deploy/metricsbench/run.sh` once a day and pulls Prometheus,
  VictoriaMetrics, Mimir and the AWS CLI for its `createbuckets` service from
  Docker Hub (deploy/metricsbench/README.md, "Pinned image digests", says
  why). It does not gate a merge either: a throttled pull fails that night's
  run and nothing else.
- `docker-compose/rustfs.yml`, `deploy/k8s`, `scripts/demo.sh`, `scripts/dr`,
  `scripts/chaos` and the interop-nightly Flight SQL test also pull the ECR
  Public image.

## Pinned image digests (docker-compose/ravel.yml and docker-compose/rustfs.yml)

Every third-party image in the two quickstart compose files is pinned to a
release tag plus an immutable `@sha256:` manifest digest, as in
`deploy/metricsbench/`. `docker-compose/rustfs.yml` is the standalone
object-store-and-bucket mirror of `docker-compose/ravel.yml` (same
credentials, same `ravel-dev` bucket), so it carries the same RustFS pins. The
RustFS and AWS CLI pins are the ones `deploy/metricsbench/docker-compose.yml`
uses; that directory's README has the resolution recipe per registry.

| Image | Registry | Tag | Digest |
|---|---|---|---|
| `rustfs/rustfs` | ghcr.io | `1.0.0` | `sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff` |
| `aws-cli/aws-cli` | public.ecr.aws | `2.37.2` | `sha256:e38214027df83cb6631adcf980a092a98d1d29788789bff2a0f424e87e3da8ed` |
| `open-telemetry/opentelemetry-collector-releases/opentelemetry-collector-contrib` | ghcr.io | `0.160.0` | `sha256:799dc6cf12c96192af37b5bdba804da8c10b3bc563b43cb90c3f3c58d9572ad6` |
| `grafana/grafana` | docker.io | `13.2.2` | `sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0` |

CI's `doc-scripts` job enforces the pins with
`deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh`:

- Every `image:` line in `docker-compose/ravel.yml` and
  `docker-compose/rustfs.yml` must carry a digest, except the two
  `${RAVEL_IMAGE:-...}` references in `ravel.yml`.
- The four registry images in `deploy/k8s` carry digests too. The two locally
  built placeholders there (`ravel-server`, `ravel-operator`) are loaded into
  kind by tag and are exempt by exact string.
