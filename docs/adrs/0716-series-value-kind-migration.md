# ADR-0716: Series value-kind migration: kind-scoped merge in maintenance; identity and the ingest contract unchanged

Status: Proposed. Issue #716 (compaction), issue #789 (selective erasure),
epic #680. Issue #676 (PromQL range-function semantics over a mixed-kind
read) was split out of this decision and is addressed in "Interaction with
`#676`" below.

## Context

A metric series can change value kind over its lifetime: a scalar float
counter is migrated by its producer to a Prometheus native histogram (or
back), keeping the same metric name and label set. This is a sanctioned
workflow in the Prometheus ecosystem, not an anomaly: Prometheus series
identity is (name, labels), sample type is not part of it, and ADR-0005
adopted exactly that rule for Ravel ("Unit and type are metadata, not
identity (matches Prometheus)"). Scalar and native-histogram points get
byte-identical `SeriesId`/`LabelSet` construction; nothing distinguishes
them at identity time, by design.

What storage makes of that today, layer by layer:

- **Ingest** enforces value-kind homogeneity per shard-buffer lifetime,
  which is per flush and therefore per L0 segment. `TenantBuf::merge`
  checks every incoming point's kind against the kind its `series_id`
  already claims in the buffer or earlier in the same batch
  (`crates/ravel-ingest/src/shard.rs:227-262`), returning
  `WriteError::SeriesValueKindMismatch`
  (`crates/ravel-ingest/src/error.rs:42-48`) with the buffer untouched.
  The check's state clears on flush; ingest has no cross-flush memory of
  a series' kind. So a real migration -- float samples flushed, then
  histogram samples for the same label set in a later flush -- is accepted
  cleanly and lands as two internally homogeneous L0 objects. The
  `ValueKind` doc comment states the invariant precisely as segment-scoped:
  "homogeneous per series for its whole life *in a segment*"
  (`crates/ravel-ingest/src/value.rs:120-127`).
- **The RSEG format** can only express segment-scoped homogeneity anyway:
  the catalog carries one `value_kind` byte per series entry per object,
  cross-checked against the run's page kinds at decode
  (`crates/ravel-segment/src/sparse.rs:485-543`, `:627-653`), and an
  object rejects a duplicate `series_id` in its catalog
  (`DuplicateSeriesId` in `assemble_v4_body`). One object, one kind per
  series. Nothing in the frozen layout claims anything about the same
  series in a *different* object.
- **Compaction** is where the lifetime-scoped invariant that nothing else
  enforces is suddenly asserted. `build_parts` groups contributions across
  a compaction's inputs by bare `series_id`
  (`crates/ravel-maintain/src/build.rs:170-178`) and fails the whole
  bucket with `MaintainError::Invariant("series ... has mixed value kinds
  across inputs")` when the same id carries a scalar plan in one input and
  a histogram plan in another (`build.rs:199-205`). The failure is
  permanent for that input set: maintenance retries every cycle and every
  retry fails identically, so the bucket is never compacted and its L0
  objects are never superseded.
- **Selective erasure (#789)** rides the same grouping. The erasure
  rewrite groups every input catalog's series by bare id, mirroring
  `build_parts` (`crates/ravel-maintain/src/erasure_rewrite.rs:907-935`),
  and feeds the same per-series build, so a DSAR/erasure request whose
  scope contains a migrated series hits the same permanent failure. That
  is not an operational nuisance but a legal one: docs/consistency-model.md
  ("Deletion guarantees") promises rewrite completion within
  `erasure_rewrite_deadline` (default 72 h) with an alarm past it, and a
  request blocked by this defect can never complete, only alarm.
- **The client-visible error today** for the within-buffer rejection:
  `SeriesValueKindMismatch` is non-retryable, which the transports map to
  HTTP 500 (`services/ravel-server/src/remote_write.rs:204-227`) and gRPC
  `INTERNAL` (`services/ravel-server/src/otlp_grpc.rs:113-129`). Prometheus
  remote write treats 5xx as retryable, so a sender whose batch straddles a
  migration instant retries the identical batch indefinitely against a
  deterministic rejection.

The read path already meets mixed kinds. Two L0 flushes either side of a
migration are two live segments carrying the same id with different
`value_kind` bytes, and scan-set resolution unions live segments; #676
exists precisely because queries can observe this today. So "a series is
one kind forever" is not an invariant Ravel has; it is an invariant only
`build_parts` asserts, on data every other layer already accepts.

## Decision

Option (b): make the merge paths handle the mix. A series' value kind is a
**per-segment fact, not a lifetime fact**; kind migration across segments
is supported storage behavior. Series identity, the ingest contract, and
every frozen format stay exactly as they are.

### 1. Maintenance groups by (series_id, value_kind)

`build_parts` and the erasure rewrite's grouping key change from
`series_id` to `(series_id, value_kind)`, with `value_kind` ordered by its
frozen encoding byte (Scalar = 0 before Histogram = 1) so the BTreeMap
iteration order stays deterministic and id-adjacent. Within one
`(id, kind)` group, run order is unchanged: canonical input order, then
each object's own stored run order (the existing tie-break rule, ADR-0018
/ ADR-0047 decision 3).

A migrated series therefore yields two `SeriesBuild`s. Because an RSEG
object's catalog rejects a duplicate `series_id`, the part planner never
places both builds in the same output object: when consecutive builds
share a `series_id`, a part boundary is forced between them, whatever the
size-driven boundary would have done. Each output object remains a valid
current-version RSEG with a sorted, unique `SERIES_IDS` column and one
`value_kind` per series entry; the `Invariant` error for mixed kinds
within one grouped build is retained as a defense against a genuinely
corrupt input (an object whose catalog entry kind contradicts its pages is
already rejected at decode).

Run merging and per-sample dedup (ADR-0092) operate within a kind only. A
scalar sample and a histogram sample at the same `(series_id, ts_ns)` are
distinct samples; both are preserved, neither is compared against the
other, and no storage-side coercion exists. What a range function makes of
that pair is #676's decision, not this one.

Exemplars stay grouped by bare `series_id` and are assigned to exactly one
of the series' builds (the first in emission order, the scalar build when
both kinds are present). Placement is not semantically visible -- exemplar
reads union across live objects by series id -- and `exemplar_total`
conservation is unchanged.

### 2. The erasure rewrite inherits the same rule

`erasure_rewrite.rs` adopts the identical `(series_id, value_kind)`
grouping and forced-boundary rule. Erasure predicates match on labels
(equality matchers plus optional time range, ADR-0064), and a migrated
series has one label set, so a predicate that matches the series matches
both of its kinds: the rewrite drops matching scalar and histogram samples
alike, and the existing conservation gate
(`sum(output sample_count) + sum(dropped) == sum(input sample_count)`,
`crates/ravel-maintain/src/publish.rs`) holds over the union.

### 3. Already-mixed data: nothing to repair, the fix self-heals the queue

No existing segment is internally mixed -- the per-buffer ingest check has
always guaranteed per-object homogeneity, and the failure fires across
inputs, never within one object. Every already-affected bucket consists of
individually valid objects plus a merge job that fails on contact. So
there is no data migration, no rewrite pass, and no operator action:

- Blocked compactions are retried by the ordinary maintenance cycle
  (leased ownership and re-verification cadence per ADR-0065); the first
  cycle after the fixed `ravel-maintain` deploys succeeds and supersedes
  the inputs normally.
- Blocked erasure requests (#789) are re-attempted the same way. A pending
  `.dreq` past its 72 h deadline has already raised the alarm metric; the
  first post-deploy rewrite pass over its scope completes, the `.done`
  record lands through the unchanged verification path, and the alarm
  clears. Unblocking latency is deploy time plus one maintenance
  ownership/re-verify cadence (minutes to hours, bounded by
  `maintain_interior_reverify`, default 6 h), not a new mechanism.
- Rollout ordering is safe by fail-closed behavior: a not-yet-upgraded
  compactor that meets kind-split parts as inputs to a later compaction
  fails with the same typed `Invariant` error it fails with today --
  loudly, publishing nothing -- until it is upgraded. No corruption window
  exists in either direction.

### 4. No frozen contract is touched

- **Series identity**: unchanged. `ravel-series-v1` stays; kind remains
  metadata, per ADR-0005. No new domain string.
- **RSEG layout**: unchanged. Outputs are new valid instances of the
  current frozen version; per ADR-0064's recorded precedent, "producing
  new valid instances of a frozen format is not a format change". No
  version bump, no trailer change, no new section.
- **Protobuf schemas, commit tokens, object key layout**: untouched.
- ADR-0066 migration class: not applicable -- there is no format
  transition to converge, so no floor raise and no readers-before-writers
  window. (Were this decision instead taken as a per-run-kind layout
  change or an identity split, it would be Class A or Class D
  respectively; both are rejected below partly for that cost.)

### 5. The ingest contract: scope stated, mapping fixed, no new rejection

The per-buffer homogeneity check stays exactly as strong as it is -- it
guards the per-object format invariant and stays atomic and pre-mutation.
Cross-flush kind change stops being an accident of buffer lifetime and
becomes the documented contract: a series may change value kind between
flushes; each acknowledged write is durable under the normal strict-mode
semantics; no cross-flush kind state is added to ingest.

One client-visible change: `SeriesValueKindMismatch` is remapped from the
generic non-retryable arm (HTTP 500 / gRPC `INTERNAL`) to HTTP 400 / gRPC
`INVALID_ARGUMENT`. The rejection is a property of the request's own
payload (two kinds for one series inside one buffer window), and the
current 500 makes spec-compliant remote-write senders retry a
deterministic rejection forever, wedging their shard queue on a batch that
can never succeed. With 400 the sender drops the batch and the loss is
bounded to the one batch that straddled the migration instant within a
single buffer window. Normative doc changes (named here, not edited here):

- **docs/ingest.md**: in the metrics-pipeline section, document the kind
  check's scope (one shard buffer lifetime, i.e. one flush/one segment),
  that a cross-flush kind change is accepted and durable, and the 400 /
  `INVALID_ARGUMENT` mapping for the within-buffer mismatch.
- **docs/consistency-model.md**: the "Acknowledgement semantics" rejection
  paragraph gains the value-kind mismatch as a named whole-request
  rejection with its status mapping, and a sentence under visibility that
  a series' samples may carry different value kinds across commits, all
  visible under the normal per-commit atomicity.

The error's doc comment ("Not retryable: identical input reproduces the
same mismatch", `error.rs:44-48`) is corrected in the implementing commit:
identical input replayed after the buffer flushes can succeed; the
rejection is deterministic within a buffer window, not across time.

### 6. Interaction with #676 (query-layer mixed-kind read semantics)

Still necessary, and now load-bearing. This decision makes a mixed-kind
series a permanent, supported storage state rather than a transient L0
artifact, and it deliberately decides nothing about query semantics. The
storage contract the query layer gets: a scan may surface, for one
`series_id`, runs of both kinds inside one window, each run tagged by its
segment's `value_kind`, with no coercion and no dedup across kinds. #676
owns what PromQL range functions do with that (Prometheus's own behavior
-- skip mixed windows with a warning annotation -- is the compatibility
baseline it should weigh). Nothing here overlaps with it: this ADR fixes
merge-time grouping below the read path, #676 fixes evaluation above it,
and neither can substitute for the other. The one #676 input this ADR
changes: it can rely on compacted data preserving both kinds' samples
exactly, so its semantics need no carve-out for "compaction may have
dropped one side".

## Rejected alternatives

**(a) Reject the mix at ingest across flushes.** Requires a durable
per-series value-kind fact consultable against a cold `TenantBuf`. Every
version of that state is expensive or wrong:

- Object storage is the only durable backend, so the fact lives there. The
  merge path today is pure in-memory (zero object-store requests per
  point, `shard.rs:227-262`); any durable check adds at least one GET per
  cold series per buffer lifetime on a miss. Expected cost band: one
  catalog/registry probe is one object-store round trip, 5-50 ms against
  S3, versus the current sub-microsecond map lookup -- four to seven
  orders of magnitude on the first write of every series after every
  eviction (ADR-0069 evicts idle tenant state on purpose). A cold tenant's
  first scrape with 10,000 new series would issue ~10,000 GETs where today
  it issues zero. The measurement that would confirm or refute: the
  ADR-0104 stage-timing ingest bench, merge-stage ns/point and the
  per-flush store-GET counter (expected exactly 0 today) with the check on
  and off.
- A negative cache or bloom filter trades the GETs for fleet-wide memory
  proportional to active series and reintroduces the choice between
  fail-open on miss (the check stops being an invariant) and a synchronous
  GET on miss (the band above).
- It rejects a workflow Prometheus accepts and ADR-0005 deliberately
  matched, making kind identity-adjacent in the one system in the
  ecosystem that refuses the migration.
- It repairs nothing: every already-mixed pair of segments stays
  permanently uncompactable and unerasable, so #789 remains open until a
  merge-side fix ships anyway -- at which point the merge-side fix alone
  (this decision) was sufficient.

**(b-identity) Distinguish the kinds at identity time** (`ravel-series-v2`
domain string folding kind into the hash). Touches the canonical-identity
frozen contract (Class D under ADR-0066 decision 4: an identity bump is a
silent split, contained per bucket, never generically migrated). It splits
*every* series' identity, not just migrated ones, dangling every stored
`series_id`, posting, and commit-token-adjacent structure across the
boundary, and it permanently forks a migrated series into two series that
PromQL can no longer see as one -- diverging from Prometheus identity
semantics forever to fix a merge-time grouping bug.

**(b-format) One series entry with per-run value kinds** (move
`value_kind` from per-series to per-run in the catalog). Expresses in one
object what the forced part boundary expresses across two, at the price of
an RSEG layout change: a version bump, ADR-0066 Class A treatment,
readers-before-writers rollout, dual-reader window, golden/fuzz/inspector
updates. All of that buys back only the marginal object that the part
split costs a bucket containing a migrated series. Not worth a frozen
contract's procedure; can be revisited if mixed series ever become dense
enough that the extra parts show up in object-count budgets.

**(b-refuse) Keep refusing, but typed and recoverable** (skip the mixed
series, compact the rest, leave its L0 objects live). Leaves the migrated
series permanently uncompacted (unbounded L0 accumulation for exactly the
series least likely to be re-emitted under a new name) and, fatally for
`#789`, an erasure rewrite cannot "skip" a series its predicate matches:
skipping is non-completion, and the request blocks forever. Rejected
because it converts a legal deadline into a standing alarm.

**(ingest-side flush-on-kind-change)** Accept the within-buffer mixed
batch by force-flushing the buffered kind first. Removes the one remaining
client-visible rejection, but couples `merge` (synchronous, actor-side,
pre-mutation) to flush scheduling (asynchronous, pipelined under
`max_inflight_flushes`, ADR-0067), inverting the invariant that merge
never awaits I/O, for a rejection whose blast radius is one batch per
migration instant per series. Not taken; can be layered later without
revisiting this ADR since it changes no durable behavior.

## Acceptance criteria

Named tests, each with the assertion that fails if the claim is false:

- `compaction_merges_kind_migrated_series` (ravel-maintain): inputs are
  two L0 objects sharing one `series_id`, input A carrying exactly 100
  scalar samples, input B exactly 50 histogram samples, plus one
  unmigrated control series with 10 scalar samples split across both
  inputs. Asserts the compaction publishes (no `Invariant` error), the
  conservation gate passes with `sum(input) == sum(output) == 160`, the
  scalar multiset of the outputs equals the input scalar multiset as
  `(series_id, ts_ns, value_bits)` with values compared by bit pattern
  (exactly 110 entries), and the histogram sample count for the id is
  exactly 50.
- `compaction_forces_part_boundary_between_kinds` (ravel-maintain): the
  same migrated fixture sized so the size-driven planner would emit one
  part. Asserts exactly 2 output parts; that no output object's catalog
  carries the migrated `series_id` twice; that each output object's
  catalog entry for the id has a single `value_kind` consistent with every
  run it holds; and that each object's `SERIES_IDS` column is sorted and
  unique (decode succeeds with no `DuplicateSeriesId`).
- `compaction_cross_kind_same_timestamp_both_survive` (ravel-maintain):
  one scalar and one histogram sample at the identical `(series_id,
  ts_ns)` across the two inputs. Asserts both samples exist in the outputs
  (output sample count for the id is exactly 2) and dedup counters record
  0 cross-kind drops.
- `erasure_rewrite_completes_over_kind_migrated_series` (ravel-maintain,
  #789): a bucket holding a migrated series (120 scalar + 80 histogram
  samples) plus a victim series (30 samples) matched by the erasure
  predicate; the migrated series is NOT matched. Asserts the rewrite
  publishes with conservation `sum(out) 200 + dropped 30 == sum(in) 230`,
  and the migrated series' 200 samples survive split across
  kind-homogeneous outputs.
- `erasure_rewrite_erases_both_kinds_of_matched_series` (ravel-maintain,
  #789): the predicate matches the migrated series' label set. Asserts
  dropped == 200 (both kinds), output count for the id == 0, and
  conservation holds; with a `FaultStore` variant asserting the injected
  fault's counter fired where the test claims a retry path is exercised.
- `ingest_rejects_mixed_kind_within_one_buffer_as_invalid_argument`
  (ravel-ingest + transport tests): one batch carrying a scalar then a
  histogram point for one series. Asserts typed
  `SeriesValueKindMismatch`, HTTP 400 on remote write and gRPC
  `INVALID_ARGUMENT` on OTLP, and that the buffer is untouched: a
  follow-up homogeneous batch of exactly 5 points is accepted and the
  buffered point count reads exactly 5.
- `ingest_accepts_kind_change_across_flushes` (ravel-ingest): flush 3
  scalar samples for a label set, then write 2 histogram samples for the
  same label set. Asserts both writes ack with commit tokens (strict
  mode), and a token-pinned read resolves exactly 2 live segments carrying
  the same `series_id` with `value_kind` Scalar and Histogram
  respectively, 3 + 2 samples.
- `scan_surfaces_both_kinds_tagged` (ravel-query): over the compacted
  migrated fixture, the storage scan returns, for the one `series_id`,
  runs tagged Scalar totalling exactly 100 samples and runs tagged
  Histogram totalling exactly 50, no coercion. This pins the storage
  contract #676 builds on.
- `prop_mixed_kind_compaction_conserves_per_kind_multisets`
  (ravel-maintain, proptest): scalar and histogram samples across 2-4
  inputs for 1-8 series, a random subset migrated. Property: publish
  succeeds, per-kind sample multisets are conserved exactly, and no output
  object carries two catalog entries for one id.

  The generator is **constrained, not arbitrary**, and both constraints are
  load-bearing rather than conveniences:

  1. **One value kind per series per input.** A single input segment
     carrying both kinds for one `series_id` violates the segment
     homogeneity that ingest enforces within a flush
     (`ravel-ingest/src/value.rs`), so such a fixture is unreachable in
     production and would fail the property for a reason unrelated to this
     decision. Migration is expressed the way it actually happens: kind A
     in an earlier input, kind B in a later one.
  2. **Same-kind timestamps are unique within a series.** ADR-0092
     deduplicates same-kind samples sharing a timestamp, so exact multiset
     conservation is simply not the correct oracle when the generator can
     emit duplicates. Either generate distinct timestamps, or apply
     ADR-0092's deduplication when computing the expected multiset --
     pick one and say which in the test's doc comment.

  A generator that ignores either constraint produces failures that look
  like conservation bugs and are not, which is the most expensive kind of
  false positive in a property test. Default proptest case count for the crate; the regression seed
  file is checked in at the location matching the test's placement
  (`proptest-regressions/` for `src/`, `tests/<name>.proptest-regressions`
  for `tests/`).

## Consequences

- Compaction and erasure stop failing on migrated series; #716 and #789
  close with a code change confined to `ravel-maintain`'s grouping plus
  the ingest error mapping. Existing stalled work self-heals on the first
  post-deploy maintenance cycle with no migration job and no operator
  runbook step.
- A bucket containing M migrated series emits up to M more output parts
  per compaction than the size-driven baseline. Mixed series are
  migration events, expected rare per bucket; the marginal cost is one
  extra PUT and one extra catalog entry per migrated series per
  compaction, and the acceptance test pins the exact split (2 parts for 1
  migrated series that would otherwise fit in 1).
- Readers must tolerate one `series_id` appearing in more than one part of
  a single compaction's output, and both kinds inside one scan window.
  Both are already true of L0 today; this ADR makes them true of every
  tier and says so where the query layer can rely on it.
- The within-buffer rejection becomes a 400/`INVALID_ARGUMENT`, a visible
  API change: compliant remote-write senders now drop the straddling batch
  instead of retrying it forever. Bounded, documented data loss replaces
  an unbounded sender livelock. docs/ingest.md and
  docs/consistency-model.md change as named in Decision 5, in the
  implementing commit.
- No frozen contract changes, so no version bump, no floor, no dual-reader
  window; the un-upgraded-compactor window fails closed and converges on
  deploy.
- The hot ingest path is untouched: zero added lookups, zero added
  object-store requests per point (the band is exact -- the merge-path GET
  counter stays 0 -- and the ADR-0104 stage-timing bench is the check).
- #676 remains open and becomes the single owner of mixed-kind read
  semantics, with a stable storage contract under it.
