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
