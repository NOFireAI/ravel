# MetricsBench artifacts

The versioned inputs to MetricsBench (ADR-0927). Two files, both loaded and
gated by code rather than read by hand:

| File | What it is | Loader and gate |
|---|---|---|
| `workload.json` | The workload manifest (format version 2): seed, generator configuration, metric families, label distributions, and the four profiles with their range-evaluation grids | `ravel_bench::metrics_workload::load_workload` |
| `metrics.corpus.json` | The PromQL query corpus (format version 2): 24 queries, each with stable id, typed cost class, the registry constructs it exercises, and whether its answer is intended to be empty | `ravel_bench::promql_corpus::load_corpus`, then `ravel_bench::promql_corpus::gate_corpus_against_workload` |

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

`metricsbench_gen` runs the two loaders, not `gate_corpus_against_workload`:
that gate (see "Query corpus" below) runs in
`crates/ravel-bench/tests/metricsbench_artifacts.rs` against the checked-in
pair.

## Version 2

Both files moved to format version 2 together (ADR-0927, 2026-10-26
amendment, decisions B and E). A version-2 loader refuses a version-1 file:
the manifest by the `range_eval` block it lacks, the corpus by its version
number. There is no compatibility path. The stream changed with the
out-of-order shift (see "Determinism"), so a version-1 stream is reproduced
from the version-1 files in git history, not by this build.

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

### Range-evaluation grid (`range_eval`)

Every profile declares the grid its `eval: range` corpus entries run on, as
offsets in seconds back from the stream's newest sample: `end_offset_secs: 0`
is the newest sample and `start_offset_secs` counts backwards from it.

| profile | start offset | end offset | step | window | points |
|---|---|---|---|---|---|
| `cardinality` | 1,800 s | 0 | 15 s | 30 m | 121 |
| `history` | 86,400 s | 0 | 300 s | 24 h | 289 |
| `churn` | 21,600 s | 0 | 60 s | 6 h | 361 |
| `ci` | 1,485 s | 0 | 15 s | 24 m 45 s | 100 |

The manifest gate refuses a grid whose start is not after its end, whose step
is zero, does not divide the window, or is not a multiple of the scrape
interval, or whose start reaches past the profile's stream span, the time from
its first sample to its newest: `(samples_per_series - 1) *
scrape_interval_secs`, one scrape interval less than `duration_secs`. The
corpus gate adds the lookback: every range entry's window plus its longest
range-selector lookback must fit inside the stream span of each profile it
runs under, so every grid point's lookback starts at or after the first
sample. That is why `ci`'s window is 1,485 s: its stream spans 1,785 s and
every range entry it runs reads 5 minutes before each grid point.

## Ingest artifact (`metricsbench_ingest`, ADR-0927 decision 11)

The bin's JSON report carries a `profile` and a `substrate` block so a reader
never mistakes a non-comparable or unbilled run for a publishable result.

`profile`:
- `name`: the `--profile` name.
- `comparable`: whether these figures may be published. True only when the
  profile itself is publishable, `steps_run` equals `steps_declared`, and the
  recorded `stream_digest` equals the digest the bin regenerates from the
  manifest and the anchor; any one failing forces `false`.
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
  counts for this run; plus `anchor_ms`, the first step's timestamp (see
  below), and the provenance fields `workload_version`, `workload_digest`,
  `seed` and `stream_digest` described under "Stream accounting".

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

Each row's `ingest` block adds `offered_digest`, BLAKE3 over every point that
row offered, in order: a sample's canonical `(series, ts_ms, value_bits)`
tuple (a staleness marker by its bit pattern), and a native histogram's
series, timestamp and every field it is encoded with. `comparable` is `true`
only when the profile is publishable, `steps_run` equals `steps_declared`,
and `stream_digest` equals the digest
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
`stale_marker` and `malformed`. Every payload type is on the wire, to every
row:

- A scalar is a Remote Write 1.0 `Sample`.
- A staleness marker is a `Sample` whose value is the Prometheus stale NaN,
  bit pattern `0x7ff0000000000002`
  (`ravel_promql_difftest::generator::STALE_MARKER_BITS`). It is the one
  non-finite value the lane admits: any other NaN or infinity is refused
  before sending as `non_finite_value`
  (`metrics_ingest::tests::validate_exempts_exactly_the_stale_marker_bits`).
- A native histogram is a `Histogram` with integer counts: the line's
  schema, count, sum and positive deltas, one positive span `{offset: 0,
  length: <number of deltas>}`, zero threshold 0, zero count 0, no negative
  buckets, and reset hint `UNKNOWN`, because the generator's line carries no
  counter-reset flag; a receiver detects the reset from the counts
  (`metrics_ingest::tests::a_native_histogram_decodes_back_with_the_promised_wire_shape`).
  A histogram whose deltas decode to a negative bucket, or whose buckets do
  not sum to its count, is refused before sending as `invalid_histogram`.

Each row reports two offered classes, never folded into one. Scalars and
staleness markers are samples: `offered_samples`, `accepted_samples`,
`rejected_samples`, `dropped_samples`. Native histograms are their own class:
`offered_histograms`, `accepted_histograms`, `rejected_histograms`,
`dropped_histograms`
(`metrics_ingest::tests::native_histograms_are_their_own_offered_class`).
`excluded_native_histogram` and `excluded_stale_marker` stay in the report
and are zero on every row the bin writes
(`metrics_ingest::tests::the_ci_stream_replays_every_payload_type_with_zero_exclusions`);
a report written before the lane sent every type carries them non-zero.
Band 4 is two equations, and the bin fails, naming every figure, unless both
close:

- Classification identity: `scalar + native_histogram + stale_marker +
  malformed == generated_lines == emitted_samples`, with `malformed` zero and
  each of `scalar`, `native_histogram` and `stale_marker` equal to the
  generator's own `float_samples`, `native_histogram_samples` and
  `stale_markers`, so a classifier that swaps two kinds fails even though the
  sum still closes.
- Offering identity: `offered_samples + offered_histograms +
  excluded_native_histogram + excluded_stale_marker == generated_lines`, read
  from the row's own fields, with `offered_samples + excluded_stale_marker ==
  scalar + stale_marker`, `offered_histograms + excluded_native_histogram ==
  native_histogram`, `offered_samples == accepted_samples + rejected_samples +
  dropped_samples`, and the same split for histograms. A row that counts its
  histograms under `offered_samples` fails the second equality even when the
  total closes
  (`metrics_ingest::tests::a_row_that_folds_histograms_into_offered_samples_fails`).

`dropped_samples` counts samples in batches that were sent and never
acknowledged. An HTTP row splits them: `rejected_by_status` maps each
non-retryable non-2xx HTTP status to the samples in batches answered with it
(for example `{"400": 500}`), and `retry_exhausted_samples` counts samples in
batches still failing with a 429, a 5xx or a transport error after
`--http-max-retries` retries. The two sum to `dropped_samples`.
`histograms_rejected_by_status` and `retry_exhausted_histograms` split
`dropped_histograms` the same way. The in-process Ravel row has no HTTP
status and no retry loop, so all four are empty there. `rejected_samples`
and `rejected_histograms` are the points the lane refused before sending.
`--batch-size` and `--http-batch-size` count points: samples and histograms
together.

#### Rows and receivers

The in-process row is `system: ravel`, `endpoint: in-process`: the
diagnostic lane's replay through `IngestRouter`, which skips the event-time
admission `ravel-server` applies (ADR-0927, 2026-10-26 amendment, section
A). It carries every commit token its strict writes minted under
`commit_tokens`.

Each HTTP receiver gets a row only when its endpoint is passed, and its
`system` value is the name the query runner joins on:

| Flag | `system` | Ack |
|---|---|---|
| `--ravel-endpoint <url>` | `ravel` | `durable_on_ack` |
| `--prometheus-endpoint <url>` | `prometheus` | `buffered` |
| `--victoriametrics-endpoint <url>` | `victoriametrics` | `buffered` |
| `--mimir-endpoint <url>` | `mimir` | `buffered` |

`endpoint` on an HTTP row is the URL passed. The portable Ravel row writes to
`ravel-server`'s `/api/v1/write`. With `--ravel-bearer-token-env <VAR>` it
sends `Authorization: Bearer <token>`, the token read from the environment
variable `VAR` names, never from the command line. The bin exits 64 when that
variable is unset or empty, or when the flag is given without
`--ravel-endpoint` (`metricsbench_ingest_smoke::a_bearer_token_env_that_cannot_be_read_exits_64`).
The token is not written into the report: neither `endpoint` nor
`client_behavior` carries it
(`metrics_ingest::tests::the_portable_ravel_row_sends_the_bearer_and_keeps_the_last_commit_token`).
The row records `last_commit_token`, the `x-ravel-commit-token` header of
the last acknowledged request, verbatim (the server joins one token per
shard that request touched with commas). It is a diagnostic sample, not a
read-your-write fence: it names only the shards that one request touched.
Only the last value is kept, so `commit_tokens` is absent on this row. A
comparator row records neither.

On every HTTP row, `accepted_samples` and `accepted_histograms` count points
in requests the receiver answered 2xx. A Remote Write 1.0 response carries no
written count, so a point dropped after the acknowledgement is counted as
accepted; ravel-server, for one, answers 204 after admission or normalization
drops. The figures are acknowledged, not confirmed stored, until a read-back
confirms them.

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

An out-of-order sample is stamped one and a half scrape intervals early
(`3 * interval_ms / 2`, exact because the manifest gate requires a whole
scrape interval of at least one second). It lands halfway between two scrape
points of its series, so it is out of order and never repeats a timestamp the
series already carries. Only unshifted samples sit on the scrape grid. A
shifted sample is not always a regression on its series: after two
consecutive shifted steps the second is a full interval ahead of the first.

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
"churn"]` for the 6-hour one), and the `full_range` range entry
`mb_full_range_avg_over_time` leaves out `ci`, whose 30 minutes cannot hold
its 30-minute lookback beside any evaluation window. An entry with no
`profiles` list runs under every profile.

Corpus version 2 repairs two join entries, each carrying its reason in
`modified`:

- `mb_join_cpu_on_build_version` selects `metricsbench_build_info{version="1.25.0"}`
  on its right side. The generator cycles both versions under one `(job,
  instance)`, so the unqualified right side holds two series per key, the
  `group_left` match is many-to-many, and every engine answers with an error.
- `mb_join_saturated_and_busy` thresholds the request rate at `> 0.5`, not
  `> 10`. The counter advances by 0 to 15 per 15-second scrape, so the
  per-second rate is at most 1.0 and a threshold of 10 is never met.

An entry may carry `expected: {"intended_empty": true, "reason": "..."}`; the
gate refuses the flag without a non-blank reason. An empty oracle answer on an
entry without it is an invalid fixture, not an engine result (ADR-0927,
2026-10-26 amendment, decision E). No entry carries it
(`metricsbench_artifacts::the_two_repaired_joins_say_why_and_no_entry_is_intended_empty`):
`mb_histogram_native_p95` and `mb_histogram_native_fraction` carried it until
the ingest lane put native histograms on the wire (task T1c of #2821). The
three `absent*` entries are not marked:
`absent()` and `absent_over_time()` answer with one sample of value 1 when
their argument is empty, and the manifest guarantees the absent metric is
never emitted.

`gate_corpus_against_workload` checks the corpus against the manifest, for
every profile each entry runs under:

- every profile an entry names is declared;
- every range entry's window plus its longest lookback (`[5m]`, `[6h]`,
  `[7d]`, and a subquery's `[1d:5m]` range plus the lookback inside it) fits
  inside the profile's duration;
- unless the entry is intended-empty, every selector matches at least one
  series the generator writes, so a label value no family emits fails (a
  selector written directly inside `absent(` or `absent_over_time(` is
  exempt);
- the one side of a `group_left`/`group_right` match (the right side for
  `group_left`) holds at most one series per `on (...)` key.

Series identity comes from the generator's own label assignment. The query is
read as text, not parsed; a query outside what the reader handles (a selector
with no metric name, `offset`, `@`, `ignoring` on a join, a one side that is
not a bare selector, a regex other than literal alternatives with an optional
trailing `.*`) is refused rather than passed. The selector check proves each
selector reaches data, not that the whole expression's answer is non-empty.

Adding a query means adding an entry here; nothing about the corpus lives in
code. `crates/ravel-bench/tests/metricsbench_artifacts.rs` pins the counts, so
an addition that skews the class balance or names an unknown construct fails
there.
