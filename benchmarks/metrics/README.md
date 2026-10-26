# MetricsBench artifacts

The versioned inputs to MetricsBench (ADR-0927). Two files, both loaded and
gated by code rather than read by hand:

| File | What it is | Loader and gate |
|---|---|---|
| `workload.json` | The workload manifest: seed, generator configuration, metric families, label distributions, and the four profiles | `ravel_bench::metrics_workload::load_workload` |
| `metrics.corpus.json` | The PromQL query corpus: 24 queries, each with stable id, typed cost class, and the registry constructs it exercises | `ravel_bench::promql_corpus::load_corpus` |

Both gates run before the first sample is generated:

```sh
cargo run -p ravel-bench --bin metricsbench_gen -- --profile ci --steps 20
```

That command loads and gates both artifacts, checks the bands they imply
(every cost class populated, every entry classed, every metric a query selects
actually emitted by the manifest), generates the profile, and prints one JSON
document with the workload summary, the corpus summary, and the generation
report. It exits non-zero on any gate or band failure, so exit 0 means the work
happened.

`--profile` has no default. The profile selects which data the run touches and
every alternative is plausible on any target, so a silent default is refused.

## Profiles

ADR-0927 decision 11. Cardinality, history and churn vary one axis each and pin
the others; collapsing them into a single scale factor would make a regression
unattributable.

| profile | active series | samples/series | scrape | duration | churn | total samples |
|---|---|---|---|---|---|---|
| `cardinality` | 1,000,000 | 360 | 15 s | 90 m | none | 360,000,000 |
| `history` | 10,000 | 172,800 | 15 s | 30 d | none | 1,728,000,000 |
| `churn` | 50,000 concurrent | 8,640 | 15 s | 36 h | 20%/h | 432,000,000 |
| `ci` | 1,000 | 120 | 15 s | 30 m | 5%/h | 120,000 |

The `ci` profile is marked non-comparable **in the artifact**, as a typed
`comparability` field carrying the reason:

```json
"comparability": {"non_comparable": {"reason": "..."}}
```

A reader that loads the manifest can therefore refuse to publish a `ci` figure
without knowing the rule. `metricsbench_gen --require-comparable` is that
refusal: it exits non-zero for a non-comparable profile, and also for a run
that covers only part of a comparable profile, because a truncated run is not
that profile. The gate refuses a manifest that marks `ci` comparable at all.

## Ingest artifact (`metricsbench_ingest`, ADR-0927 decision 11)

The bin's JSON report carries a `profile` and a `substrate` block so a reader
never mistakes a non-comparable or unbilled run for a publishable result.

`profile`:
- `name`: the `--profile` name.
- `comparable`: whether these figures may be published. Forced `false` when
  `steps_run` is less than `steps_declared`, regardless of the profile's own
  verdict.
- `comparability_reason`: why not, present only when `comparable` is `false`.
- `active_series`: series alive at any one instant, as declared.
- `steps_run`: steps this run actually generated.
- `steps_declared`: the profile's full declared step count
  (`samples_per_series`).
- `samples_per_series`, `scrape_interval_secs`, `duration_secs`,
  `total_samples`, `churn_basis_points_per_hour`: as declared.
- `label_cardinalities`: distinct values per FIXED label dimension, the
  scaling label not included: it is not a dimension (the manifest gate
  refuses to let it be declared as one), and its cardinality depends on the
  run's churn epochs rather than being a fixed, manifest-only count.
- `scaling_label`: the scaling label's `name` and its `cardinality_declared`,
  the distinct scaling-label values the profile's full declared run would
  emit, the generator's exact count.
- `families`: one entry per metric family, each with `name`, `instances`,
  `series_per_instance`.
- `run`: figures scoped to `steps_run`, never the declared full profile:
  `steps`, `total_series_created`, `logical_input_bytes`,
  `total_samples_generated`, `scaling_label_cardinality` (distinct
  scaling-label values this run actually emitted), all the generator's exact
  counts for this run; plus `anchor_ms`, the first step's timestamp, and
  `anchor_source`, `explicit` or `clock` (see below).

### Anchor (`--anchor-ms`)

`--anchor-ms <i64>` sets the timestamp, in milliseconds since the epoch, of
the first generated step, for example `--anchor-ms 1700000000000`. Given, no
clock is read to generate the stream, the report says
`anchor_source: "explicit"`, and two runs with the same anchor replay the same
bytes. Omitted, the anchor is `now - steps * scrape interval` so the newest
sample lands near now, and the report says `anchor_source: "clock"`. A run
meant to be compared with another passes `--anchor-ms` explicitly, with the
same value on both, so the two replay the same stream. The nightly lane passes
no anchor and keeps the clock default.

### Stream accounting

The bin streams the generator: it generates one scrape step at a time and
replays it in `--batch-size` batches, so memory is bounded by the batch size
and one step's width, not by the number of steps. Each comparator gets the
same stream regenerated from the same anchor, and the bin fails if its digest
differs from the Ravel row's.

Every line of the stream is classified as a scalar (float) sample, a native
histogram, a staleness marker, or malformed, and every row's `ingest` block
carries `stream_counts` with `generated_lines`, `scalar`, `native_histogram`,
`stale_marker` and `malformed`. The Remote Write 1.0 replay sends only the
scalar samples: `offered_samples` equals `stream_counts.scalar`, and
`excluded_native_histogram` and `excluded_stale_marker` name the two kinds it
does not send. The bin fails, naming the figures, unless the four counts sum
to `generated_lines`, `generated_lines` equals the generator's
`emitted_samples`, `malformed` is zero, and the offered samples split exactly
into accepted, rejected and dropped.

Because generation runs inside the replay, `elapsed_secs` is the sum of the
timed write segments (the `write_values` calls and the final flush for the
Ravel row; each encode-and-POST for a comparator row) and never includes
generating or classifying the stream, while `cpu_secs` covers the whole
replay, generation included. Every `ingest` block carries
`elapsed_basis: "timed_segments"` so a report written before this change can
be told apart: its `elapsed_secs` was one wall-clock span over the write loop
of a stream generated up front (admission and encoding were already outside
it), so the two bases differ by the generation now interleaved between
segments, which the sum excludes, and a throughput figure is compared across
the change only with that in mind.

`substrate`:
- `store_backend`: the `--store` kind (`memory` or `s3`).
- `endpoint_host`: the configured S3 endpoint's host, when `--store s3` and
  `RAVEL_S3_ENDPOINT` is set; absent for `memory` regardless of the env var.
- `backend_bills_requests`: true only for real S3 with no endpoint override
  (decision 10); false on `memory` and on any store behind a configured
  endpoint, such as the nightly lane's local RustFS.

## Determinism

One seed (`927000933`) drives everything. Every value and every injected
anomaly is a pure function of `(seed, family, instance, step)`, so the same seed
and manifest write byte-identical output; float payloads are encoded as
`f64::to_bits` hex so a `-0.0` or a NaN survives the stream. `--out <path>`
writes the stream; without it the stream is still encoded, counted and hashed.

The generator emits gauges, monotonic counters, classic histograms, native
histograms, staleness markers, counter resets, missing samples, out-of-order
samples, and hour-boundary churn. Anomaly precedence is fixed (missing beats
staleness beats the timestamp shift; a counter reset is independent) and
documented on `ravel_bench::metrics_gen`.

Every generation run reports exact figures, and `emitted_samples +
omitted_missing_samples` must equal `steps * active_series` or the run fails
rather than reporting a silent drop (ADR-0927's band 4).

## Query corpus

24 queries, three in each of the eight cost classes ADR-0927 decision 5 fixes:
`metadata_only`, `single_series`, `selective_multi_series`, `high_fan_out`,
`full_range`, `join`, `histogram`, `long_range`. The class is a typed enum on
the entry, not a comment.

Each entry names the constructs it exercises exactly as
`ravel_promql_difftest::scoring::REGISTRY` names them. The gate checks
membership in the registry, not membership in its supported subset: ADR-0927
decision 6 keeps queries Ravel refuses or does not implement in the corpus,
reported with their status and still counted in the corpus denominator, so a
supported-only gate would delete exactly those entries.

Three `long_range` entries restrict themselves to the profiles that generate
enough data for their window (`profiles: ["history"]`, and `["history",
"churn"]` for the 6-hour one). An entry with no `profiles` list runs under every
profile.

Adding a query means adding an entry here; nothing about the corpus lives in
code. `crates/ravel-bench/tests/metricsbench_artifacts.rs` pins the counts, so
an addition that skews the class balance or names an unknown construct fails
there.
