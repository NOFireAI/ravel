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
| `ci` | 1,000 | 120 | 15 s | 30 m | none | 120,000 |

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

`--anchor-ms <i64>` is required. It sets the timestamp, in milliseconds since
the epoch, of the first generated step, for example `--anchor-ms
1700000010000`. There is no default: the stream digest covers every sample's
timestamp, so an anchor read from the clock would make every run replay a
different stream. No clock is read to generate the stream, and two runs with
the same anchor replay the same bytes. The bin exits 64, naming the problem,
when the flag is missing, when the anchor is negative, when it is not a
multiple of the profile's scrape interval in milliseconds (the message shows
the remainder), or when the last step, `anchor_ms + (steps - 1) * interval_ms`,
lands past `9223372036854`, the largest millisecond whose nanosecond form fits
an `i64`. The nightly lane passes a fixed on-grid anchor and replays the same
frozen window every night.

The report's `profile.run` block records the run's provenance, so a reader can
regenerate the stream from it:
- `workload_version`: the manifest's format version.
- `workload_digest`: BLAKE3 (hex) over the manifest file's bytes.
- `seed`: the manifest's generator seed.
- `anchor_ms`: the `--anchor-ms` value.
- `stream_digest`: the generator's digest over the stream the run replayed.

Each row's `ingest` block adds `offered_digest`, BLAKE3 over the canonical
`(series, ts_ms, value_bits)` tuple of every sample that row offered, in
order. `comparable` is `true` only when the profile is publishable,
`steps_run` equals `steps_declared`, and `stream_digest` equals the digest
recomputed from the manifest and the anchor; `comparability_reason` names each
cause that failed. All five provenance fields and `offered_digest` are absent
from a report written before they existed and read back as unset.

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
does not send. Band 4 is two equations, and the bin fails, naming every
figure, unless both close:

- Classification identity: `scalar + native_histogram + stale_marker +
  malformed == generated_lines == emitted_samples`, with `malformed` zero and
  each of `scalar`, `native_histogram` and `stale_marker` equal to the
  generator's own `float_samples`, `native_histogram_samples` and
  `stale_markers`, so a classifier that swaps two kinds fails even though the
  sum still closes.
- Offering identity: `offered_samples + excluded_native_histogram +
  excluded_stale_marker == generated_lines`, read from the row's own fields,
  with `offered_samples == scalar` and `offered_samples == accepted_samples +
  rejected_samples + dropped_samples`.

`dropped_samples` counts samples in batches that were sent and never
acknowledged. A comparator row splits them: `rejected_by_status` maps each
non-retryable non-2xx HTTP status to the samples in batches answered with it
(for example `{"400": 500}`), and `retry_exhausted_samples` counts samples in
batches still failing with a 429, a 5xx or a transport error after
`--http-max-retries` retries. The two sum to `dropped_samples`. The in-process
Ravel row has no HTTP status and no retry loop, so both are empty there.
`rejected_samples` is the samples the lane refused before sending.

Because generation runs inside the replay, `elapsed_secs` is the sum of the
timed write segments (the `write_values` calls and the final flush for the
Ravel row; each encode-and-POST for a comparator row) and never includes
generating or classifying the stream, while `cpu_secs` covers the whole
replay, generation included. Every `ingest` block carries
`elapsed_basis: "timed_segments"` so a report written before this change can
be told apart: its `elapsed_secs` was one wall-clock span over the write loop
of a stream generated up front. For the Ravel row that span started after
admission and after the RW1.0 encode; for a comparator row the encode was
inside it, as it still is inside that row's timed segments. In both lanes
the only basis change is the generation now interleaved between segments,
which the sum excludes, and a throughput figure is compared across the
change only with that in mind.

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
