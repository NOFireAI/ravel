# ADR-0059: durability hardening (scrub, postings verification, reorder harness)

Status: Accepted

## Context

Ravel has a real, layered checksum hierarchy on both RSEG and RLOG objects
(whole-object blake3 at write time, footer/section/page crc32c on read),
but every layer of it is verified only when a query happens to touch the
covered bytes (`crates/ravel-segment/src/reader.rs`, `open_segment` in
`crates/ravel-query/src/fetcher.rs:680-769`). Bytes nobody queries —
a page for a series nobody reads, an unqueried section — are never
checked by anything, ever. `docs/object-store-contract.md` states this
design choice explicitly: "Ravel's integrity guarantee... is therefore
read-time only." This is S2-08: bit rot or a partially written object is
discovered by accident of query pattern, or not at all.

The closest thing to a scrubber that exists today, `ravel-cli maintain
verify-custody` (`services/ravel-cli/src/maintain.rs:424-437,461-655`),
already does the right check — a full-object blake3 rehash compared
against the commit record's `content_hash` — but only when an operator
runs it by hand, and it reports via `println!`, never through `/metrics`.

Name-postings objects (`crates/ravel-catalog/src/snapshot_format/
postings.rs`) bind to their covered part set via an exact blake3 check
(`decode_postings`, `PostingsPartBindingMismatch`), which protects against
staleness but says nothing about whether the postings' claims are actually
true of the underlying segment data. A *false positive* claim (postings
lists a name that isn't really there) is spot-checkable cheaply: resolve
one ordinal, suffix-GET that segment's catalog, check the name appears.
A *false negative* (S2-09's actual concern — a query gets "no match" when
the data has a match) is fundamentally not spot-checkable: catching it
requires re-deriving every covered segment's true name set from its own
catalog and diffing against what postings claims for it — proportional to
segment count, not to names. Today this check exists only inside
`cargo test` as a property test (`docs/metric-index-plan.md`, "postings
exactness property"); there is no production path that ever runs it.

Late-commit seal-loss detection (S2-04) already has a correct tool,
`ravel-cli catalog verify` (`services/ravel-cli/src/catalog.rs:147-266`):
it re-lists sealed commit records and diffs them against the folded
snapshot, catching under-counting from clock-skew seal divergence. It is
metadata-cost (reads commit records, not data objects), correct, and
completely unscheduled — the same "operator must remember to run a CLI"
gap epic ED closes for a different failure mode.

`ravel-object-store`'s `FaultStore` injects a wide range of faults
(timeouts, throttling, partial writes, corrupt ranges, duplicate
delivery) but explicitly does not support reordering completions — its
own module doc says so plainly: "'reordered completion' is mentioned as
an aspiration... but is not part of this crate's fault-plan surface"
(`crates/ravel-object-store/src/fault.rs:40-42`). Tracing the actual
write path (`crates/ravel-ingest/src/shard.rs:562-603`) shows the data-
object PUT is awaited to completion before the commit-record PUT is even
built — the two are never concurrent for one flush, so no code today
depends on their *completion order*, only on the data PUT being
*issued and awaited* before the commit PUT is attempted, which the code
already enforces by construction. The one place genuine completion-order
ambiguity exists today is multipart part uploads, whose contract already
states parts may complete out of submission order — and that assumption
has never been exercised against anything. S1-14's harness closes a real
testing-capability gap, not a live bug; this ADR says so plainly rather
than implying otherwise.

### Cost shape is genuinely different from every other scheduled task

Compaction, retention, sweep, and fold all work primarily off metadata
(commit/compaction records, footer identity fields, catalog entries) and
rarely read a whole existing data object. A scrubber verifying at-rest
integrity fundamentally must read enough of each object to re-verify its
checksums — at minimum the full crc-covered bytes, and for a whole-object
blake3 re-check (the only thing that actually proves the object matches
what was written), the entire object. This is the one scheduled task
whose cost is `O(total corpus bytes)` rather than `O(metadata)`, and it
needs to be sized as such rather than following the existing tasks'
"converge to steady state, mostly skip" memo pattern
(`MaintainMemo`), which assumes a decision becomes terminal — a scrubbed
object doesn't become "terminal," it needs periodic re-scrubbing.

## Decision

### 1. Rotating-cursor scrubber, two tiers, sharing one full-object read

A new scheduled task (`services/ravel-server/src/scrub.rs`, spawned the
same way fold/maintain are: `tokio::spawn`, jittered interval loop,
graceful shutdown) with two tiers:

- **Structural tier** (cheap, runs every tick): for each tracked
  (tenant, signal, shard), suffix-GET each object's footer and re-verify
  the footer/section crc32c hierarchy already defined
  (`docs/segment-format.md`, `docs/log-segment-format.md`) — the same
  cost class as fold's own catalog reads, no new read pattern.
- **Content tier** (expensive, rotating cursor): a persistent cursor
  (stored the same way the fold watermark is, per-(tenant, signal))
  advances through the full object corpus over a configured scrub
  period `P` (default 7 days), visiting one bounded slice per tick sized
  to keep sustained read bandwidth at `total corpus bytes / P`. For each
  object the cursor visits: full-object GET, blake3 rehash compared
  against the commit record's `content_hash` (bit-rot / partial-write
  detection, S2-08) — **and, on the same read, re-derive the object's
  true name set from its own catalog/label dictionary and diff against
  what the covering postings object(s) claim for it** (S2-09's false-
  negative check). This reuses the one expensive full-object read for
  both checks rather than paying for it twice — the strongest part of
  this design, since neither check alone would justify a dedicated full-
  object-read task on its own, but together they do.

The budget is explicit and reviewable: default `P = 7 days`, so sustained
scrub read bandwidth is bounded at `corpus_bytes / (7 * 86400)` bytes/sec,
a knob an operator sizes against their own corpus the same way `R` (the
admission-reconciliation interval, ADR-0057) is an operator-facing knob.

### 2. Seal-divergence check: schedule `catalog verify`'s comparison logic

Factor `ravel-cli catalog verify`'s comparison logic
(`services/ravel-cli/src/catalog.rs:147-266`) into a function the new
scrubber task (or the existing fold loop — metadata-cost, so either fits)
calls on the fold cadence, reporting missing/mismatched/orphaned counts
into the metrics family below instead of `println!`. The CLI command
itself stays for manual/ad-hoc use; this is a scheduling wrapper around
its existing, correct comparison logic, not a rewrite.

### 3. New metrics family, following `render_maintain_safety_family`'s convention

`ravel_scrub_checksum_mismatch_total{signal}`,
`ravel_scrub_postings_disagreement_total{signal}`,
`ravel_scrub_seal_divergence_total{signal}` (missing/mismatched/orphaned
as distinct label values on `reason`, matching the existing `Label`
enum), plus a gauge `ravel_scrub_cursor_position` (fraction of corpus
covered this rotation, for operator visibility into cadence). No
`tenant_hash` label, matching every existing family on the unauthenticated
`/metrics` route (ADR-0044 §4) — this is not a new decision, it is
following the established default every other family already follows.

### 4. Acceptance test targets a deterministic entry point, not cursor rotation

The acceptance criterion ("detects an injected single-bit flip... within
one scrub cycle") is tested against a `scrub_one_object(store, key)`
function directly — the same unit the cursor calls per slice — not by
waiting for a full corpus rotation in a test. This is named explicitly so
the implementing task builds a real per-object entry point the cursor
wraps, rather than a test that only passes by luck of where the cursor
happens to be. Corruption injection uses the existing, already-proven
pattern (`crates/ravel-failure-tests/tests/corruption.rs:54-64`: GET the
object, flip a bit, `PutMode::Overwrite` back over the same key) — no new
test infrastructure needed for this half. The postings-disagreement half
uses the same pattern against a postings object's key.

### 5. New `FaultStore` primitive: hold-and-release completion ordering

A new fault mechanism in `ravel-object-store`'s fault module: a named
gate that holds a matching operation until explicitly released, plus a
test-side handle to control release order across concurrently-issued
operations. This is genuinely new design work (the current `Rule`/
`Sequence` model resolves each call synchronously against a per-call
outcome table, not a hold/release protocol), scoped as its own task
within this epic, not a rider on the scrubber work. It targets the two
places completion order is contract-relevant today but never exercised:
multipart part completion (contract already assumes out-of-order-safe)
and cross-shard commit-record visibility ordering (each shard's flush is
independent; the router joins them with `join_all`, and nothing today
tests that a slow shard's delayed visibility doesn't corrupt a query
spanning that window).

## Rejected alternatives

**Whole-object blake3 rehash every tick for every object.** Rejected on
cost: this is the naive reading of "scrub everything," and its cost is
`O(corpus bytes) per tick` rather than `O(corpus bytes / P)` — unbounded
and unreviewable at any real corpus size. The rotating cursor gives the
same eventual coverage with an explicit, sized budget.

**A separate postings-verification pass, decoupled from the content-tier
scrub.** Rejected because it would pay for a second full-object-class
read (segment catalog reads, proportional to the same corpus) that the
content tier's cursor is already paying for scanning the same objects for
blake3 rehashing — piggybacking is strictly cheaper for the same
coverage.

**Treat S1-14 as closing a live correctness bug.** Rejected on the
evidence: no code path today has its correctness depend on object-store
completion order (the write path is sequential-by-issuance; concurrent
GET fan-out is order-insensitive by construction). Framing this as "fixes
a bug" would overclaim; framing it as "closes a defensive test-capability
gap for future concurrent-write code and the currently-untested multipart
out-of-order assumption" is the honest scope, stated in Context above.

**Fold the seal-divergence check into the existing sweep task instead of
its own metrics-reporting wrapper.** Rejected for locality: `catalog
verify`'s comparison logic already lives in `ravel-catalog`/`ravel-cli`
territory adjacent to fold, not sweep's territory; wrapping it near fold
keeps the code next to what it's checking.

## Consequences

- **Closes S2-08 and S2-09 together, on one shared expensive read per
  object**, over an explicit, operator-sized rotation period — not
  instantaneous coverage, a documented eventual-coverage guarantee with a
  stated worst-case staleness (`P`).
- **This is the first scheduled task whose cost scales with data volume,
  not metadata volume.** Its budget must be sized and monitored
  (`ravel_scrub_cursor_position`) like any other capacity-planning input,
  not assumed free the way fold/compaction/retention/sweep's metadata
  costs are.
- **S2-04's fix is a scheduling wrapper, not new detection logic** — the
  correct comparison already existed; it was only ever manually invoked.
- **S1-14 adds a genuinely new fault-injection primitive**
  (`ravel-object-store`), which other future work (a parallelized write
  path, a multipart-based large-segment writer) can build tests on top of
  — this ADR delivers the primitive and two concrete tests, not
  exhaustive coverage of every future concurrent-write scenario.
- **Does not add repair/rebuild for detected anomalies.** A checksum
  mismatch or postings disagreement is reported, not auto-repaired —
  repair for segment-level corruption has no clear safe action today
  (there is no redundant copy to repair from; ADR-0058's DR document
  states this explicitly). This scrubber's job is detection and alarming,
  matching the epic's acceptance criterion, not recovery.
