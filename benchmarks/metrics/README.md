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
  dropped_samples + unsupported_samples`, and the same split for histograms
  (`offered == accepted + rejected + dropped + unsupported` per class). A row
  that counts its histograms under `offered_samples` fails the second
  equality even when the total closes
  (`metrics_ingest::tests::a_row_that_folds_histograms_into_offered_samples_fails`).
  Each class's drop must also be bucketed: `dropped_samples` equals
  `rejected_by_status` summed plus `retry_exhausted_samples`, and
  `dropped_histograms` equals `histograms_rejected_by_status` summed plus
  `retry_exhausted_histograms`, on every row, the in-process one included
  (`metrics_ingest::tests::dropped_points_must_be_bucketed_by_status_or_retry_exhaustion`).

`dropped_samples` counts samples sent and not accepted by the receiver. An
HTTP row splits them: `rejected_by_status` maps each non-retryable non-2xx
HTTP status to the samples it rejected (for example `{"400": 500}`), and
`retry_exhausted_samples` counts samples in batches that ended on a
retryable status after the retry budget, which may be zero
(`--http-max-retries`), or on a transport error. `retry_exhausted_by_status`
keys the first kind by that last status (for example `{"503": 500}`); a
transport error has no status and is counted only in the total
(`metrics_ingest::tests::comparator_drops_are_bucketed_by_status_and_by_retry_exhaustion`).
`histograms_rejected_by_status`, `retry_exhausted_histograms` and
`histograms_retry_exhausted_by_status` split `dropped_histograms` the same
way. The in-process Ravel row has no HTTP status and no retry loop, so all
six are empty there, and a strict write that did not ack fails the bucketing
identity: the drop budget is zero. `rejected_samples` and
`rejected_histograms` are the points the lane refused before sending.
`--batch-size` and `--http-batch-size` count points: samples and histograms
together.

#### Rows and receivers

The in-process row is `system: ravel-in-process`, `endpoint: in-process`:
the diagnostic lane's replay through `IngestRouter`, which skips the
event-time admission `ravel-server` applies (ADR-0927, 2026-10-26 amendment,
section A). Its name differs from the portable row's `ravel`, so a lookup by
`system` returns one row or the other, never the wrong one. It carries every
commit token its strict writes minted under `commit_tokens`.

Each HTTP receiver gets a row only when its endpoint is passed, and its
`system` value is the name the query runner joins on. Every HTTP endpoint
needs its query URL, the base its `/api/v1/query` and `/api/v1/series` are
appended to; the bin exits 64 without it
(`metricsbench_ingest::tests::every_endpoint_needs_its_query_url`), and
exits 64 on a query URL given without its endpoint
(`metricsbench_ingest::tests::a_query_url_without_its_endpoint_is_refused`):

| Flag | Query URL flag | `system` | Ack | `acceptance_basis` |
|---|---|---|---|---|
| `--ravel-endpoint <url>` | `--ravel-query-url` | `ravel` | `durable_on_ack`, refused when no response carries `x-ravel-commit-token` | `read_back` |
| `--prometheus-endpoint <url>` | `--prometheus-query-url` | `prometheus` | `buffered` | `acknowledged` |
| `--victoriametrics-endpoint <url>` | `--victoriametrics-query-url` | `victoriametrics` | `buffered` | `acknowledged` |
| `--mimir-endpoint <url>` | `--mimir-query-url` | `mimir` | `buffered` | `acknowledged` |

Every row sends Remote Write 1.0, the portable Ravel row included. Each
receiver's response is read by its own rule, and every HTTP row records the
rule's basis as `acceptance_basis`:

- Prometheus: a 400 rejects the whole request, every point of it under
  `rejected_by_status`.
- Mimir: a 400 is a per-sample soft rejection. Only the points the body names
  are rejected, matched on Mimir's rendering of a sample ("The affected sample
  has timestamp `<RFC3339Nano UTC>` and is from series `<name{labels}>`",
  taken from Mimir's main branch, not a pinned release); the rest of the
  request is accepted. A 400 whose body names no point of the request
  rejects it whole
  (`metrics_ingest::tests::prometheus_rejects_a_400_request_whole_and_mimir_only_the_named_sample`,
  `metrics_ingest::tests::a_mimir_400_naming_no_point_of_the_batch_rejects_it_whole`).
- VictoriaMetrics: a 2xx accepts every point except native histograms, which
  it acknowledges without storing (see the unsupported-class table below)
  (`metrics_ingest::tests::victoriametrics_acknowledges_histograms_it_cannot_store`).
- Ravel: a 2xx is a durable acknowledgement and a 400 rejects the request;
  ravel-server answers 204 after admission or normalization drops too, so its
  accepted set is checked by the read-back below.

`accepted_samples` and `accepted_histograms` count the points a row's rule
accepted. A Remote Write 1.0 response carries no written count, so on an
`acknowledged` row they are what the receiver acknowledged. The read-back
checks them only in part: it counts series and reads one probe series per
class, so it catches a dropped series, or a dropped point in a probe series,
not every dropped point.

`endpoint` on an HTTP row is the URL passed. The portable Ravel row writes to
`ravel-server`'s `/api/v1/write`. With `--ravel-bearer-token-env <VAR>` it
sends `Authorization: Bearer <token>`, the token read from the environment
variable `VAR` names, never from the command line. The bin exits 64 when that
variable is unset or empty, or when the flag is given without
`--ravel-endpoint` (`metricsbench_ingest_smoke::a_bearer_token_env_that_cannot_be_read_exits_64`).
The token is not written into the report: neither `endpoint` nor
`client_behavior` carries it
(`metrics_ingest::tests::the_portable_ravel_row_sends_the_bearer_and_keeps_the_latest_token_of_every_shard`).

Every row records its `tenant`. The in-process row writes under
`--tenant` (default `metricsbench`). The portable Ravel row records
`--ravel-tenant`, the tenant the operator declares ravel-server resolves its
requests to. It is declared, not observed: nothing on the wire carries it and
a `CommitToken` holds no tenant (its fields, `crates/ravel-types/src/lib.rs`,
are shard, writer, epoch, sequence and ingest hour). `--ravel-endpoint` requires it and it must
equal `--tenant`, so both Ravel rows describe one tenant's data; the bin
exits 64 otherwise
(`metricsbench_ingest_smoke::a_missing_or_mismatched_ravel_tenant_exits_64`),
and exits 64 on `--ravel-tenant` given without `--ravel-endpoint`
(`metricsbench_ingest::tests::a_ravel_tenant_without_the_ravel_endpoint_is_refused`).
A comparator is sent no tenant and records none.

The portable Ravel row records `commit_tokens_by_shard`, the latest
`x-ravel-commit-token` value per shard over every acknowledged request (the
server joins one token per shard a request touched with commas), and
`last_commit_token`, those tokens joined with commas in shard order: a
read-your-write fence over every shard the replay wrote, not only the shards
the last request touched. The query API decodes each `min_commit_token`
parameter as one token (`decode_commit_tokens` in
`crates/ravel-query/src/http/params.rs`), so a consumer splits
`last_commit_token` on commas and passes one parameter per token, or passes
the values of `commit_tokens_by_shard`, as the read-back does
(`ApiReader::get` in `crates/ravel-bench/src/metrics_ingest.rs`). A header entry that does not decode as a commit
token fails the replay
(`metrics_ingest::tests::an_undecodable_commit_token_is_refused`). The row
stays `durable_on_ack` only when some response carried a token; otherwise the
bin refuses it as unbacked durability
(`metrics_ingest::tests::a_durable_row_without_any_commit_token_is_refused`).
`commit_tokens` is absent on this row. A comparator row records none of them.

#### Read-back

After each HTTP replay the bin reads the row back over the receiver's
Prometheus query API and records `readback`: `series_count` (distinct series
`/api/v1/series` returns over the replay window for the metric names the
replay sent, so a receiver's own series, such as a self-scrape, are not
counted), `sample_count`
(`count_over_time` over one probe series per float class, evaluated at the
newest timestamp sent over the whole window), `histogram_series_count` (the
native-histogram metrics' series, absent on a receiver that does not store
them), `histogram_sample_count`, `stale_marker_honored` (an instant query at
the marker's timestamp returns no sample for its series), `expected` (the
same figures from what was sent, for the classes the receiver stores),
`mismatches` naming each figure that differs, and `matches_expected`. The
Ravel row passes every token in `commit_tokens_by_shard` as
`min_commit_token` on every query. The bin fails when `matches_expected` is
false
(`metrics_ingest::tests::readback_matches_what_was_stored_and_fails_one_series_off`,
`metrics_ingest::tests::readback_expectations_follow_what_was_sent_and_what_the_receiver_stores`).

#### Band 6: accepted digests per class

Every row carries `expected_digests` and `accepted_digests`, one BLAKE3
digest per class (`scalar`, `stale_marker`, `native_histogram`) over the
points sent and the points accepted, in offered order. The bin fails a row
whose accepted digest differs from its expected digest for any class it
stores; digests are compared, not counts, so a row that accepted as many
points as it was sent, but not the same ones, fails
(`metrics_ingest::tests::band6_compares_digests_not_counts`,
`metrics_ingest::tests::band6_fails_when_a_supporting_receiver_drops_one_point_of_any_class`).
Across rows, every portable row must carry the same expected digest for every
class, and each portable row's expected scalar digest must equal the
in-process row's
(`metrics_ingest::tests::portable_rows_with_different_expected_digests_fail`,
`metricsbench_ingest::tests::a_portable_scalar_digest_must_equal_the_in_process_one`,
`metrics_ingest::tests::the_in_process_and_portable_rows_carry_equal_expected_digests`).

A class a receiver acknowledges without storing is excluded from its band 6
comparison and its read-back, and counted under `unsupported_samples` or
`unsupported_histograms` instead of accepted; it is not a drop, so the drop
budget stays zero. Every row records the classes excluded for it, each with
its citation, as `unsupported_classes`. The table has one entry
(`metrics_ingest::tests::the_unsupported_class_table_names_only_victoriametrics_histograms`):

| Receiver | Class | Source |
|---|---|---|
| VictoriaMetrics | native histogram | `lib/prompb/prompb.go` at v1.115.0: `TimeSeries.unmarshalProtobuf` reads only labels (field 1) and samples (field 2), so histograms (field 4) are skipped while the request is answered 2xx |

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
  inside the profile's stream span, `(samples_per_series - 1) *
  scrape_interval_secs`, not inside its `duration_secs`;
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

## Report schema version 3

The run's result is a `MetricsBenchReport`
(`crates/ravel-bench/src/report_schema.rs`). Version 3 is what the writer
stamps; versions 1 and 2 still read and validate under their own rules, and a
version-3 field set on one of them is refused
(`a_geomean_on_a_v3_report_and_a_v3_field_on_a_v2_report_fail`). Check a file
with:

```sh
cargo run -p ravel-bench --bin metricsbench_report -- validate report.json
```

It exits 0 only when the report stands, and otherwise exits non-zero with the
failing rule on stderr (`tests/metricsbench_report.rs`,
`the_validate_subcommand_exits_non_zero_naming_the_failing_rule`). `render
--report` validates first and then prints, per engine, the denominators and
the count of each status before any latency, then a latency table over `ok`
rows only (`the_v3_render_prints_denominators_before_latency_over_ok_rows`).

**Row identity.** A version-3 row is keyed by (engine, id, query kind), and
the report lists every case the corpus defines in `cases_expected`. The rows
must cover exactly that set, compared as sets, so a duplicated row cannot
stand in for a missing one (ADR-0927 "Bands pre-registered" item 1;
`band_1_a_duplicated_row_cannot_hide_a_missing_case`). A range entry is one
case, not one per grid point (2026-10-26 amendment, section E). Every row
names its endpoint, and `in-process` is refused: that value marks a
diagnostic-lane row, never the portable one (amendment section A;
`an_in_process_endpoint_fails`). `oracle` is true on exactly the Prometheus
rows (decision 4; `the_oracle_flag_is_true_exactly_on_prometheus_rows`).

**Status across passes.** Each row carries its status per pass in
`pass_statuses`, and its `status` is `row_status` of them: the first non-`ok`
pass in pass order, else `ok`, so passes `[ok, timeout, error]` give `timeout`
(`row_status_is_the_first_non_ok_pass`). Only `ok` rows carry latency: the
`min_ms`/`median_ms`/`max_ms` figures, `reps_per_pass`, and one
`pass_medians_ms` entry per pass (decision 6; band 2,
`band_2_latency_on_an_incorrect_row_fails`). Each figure appears once per row
(band 5, `band_5_a_duplicated_figure_fails`). A `refused` row carries a
`refusal_reason` and no other status does, including a budget refusal
(decision 6 and amendment section F; `a_refused_row_without_a_reason_fails`,
`a_refusal_reason_on_an_error_row_fails`).

**Denominators.** `denominators` holds one entry per engine with the three
decision-6 denominators: `corpus` counts every case, `correctness` counts
`ok`, `incorrect` and `partial`, and `timing` and `coverage_success` count
`ok`. The validator recomputes them from the rows with
`metrics_correctness::Denominators::of` and requires an exact match; the
reported numbers are never trusted
(`denominators_are_recomputed_from_the_rows`).

**Variance.** With five or more passes, every `ok` row carries `rel_mad_pct`,
`median(|m_i - median(m)|) / median(m) * 100` over its pass medians, with no
pass dropped as an outlier ("Bands pre-registered" item 3; `rel_mad_pct`,
`rel_mad_pct_matches_hand_computed_five_pass_fixtures`). The validator
recomputes it and refuses a value that disagrees
(`a_rel_mad_disagreeing_with_the_pass_medians_fails`). With fewer than five
passes no row carries one and the provenance config carries
`variance=insufficient_passes`
(`fewer_than_five_passes_carry_no_rel_mad_and_record_insufficient_passes`). A
pass excluded for a recorded external cause is listed in `exclusions` with
that cause (`a_pass_exclusion_needs_a_cause`). Version 3 drops the
report-level `geomean_ms`: decision 6 rules out one figure across engines.
