# ADR-0849: A snapshot-bound index plane on object storage

## Context

Ravel's read cost has a floor that no amount of CPU work removes. Measured on
the ClickBench `hits` corpus (tenant `clickbench-v4`: 3,469 objects **after L1
compaction** — 8,424 as loaded, which is the figure `docs/query-engine.md` and
`docs/guides/clickbench.md` quote for the same tenant — 12.03 GB, 99,997,497
rows) on an `r6a.4xlarge`:

- **34 of 41 statements read every one of the 3,469 objects and the entire
  12.03 GB.** There is no index and no clustering, so a statement that wants
  eleven rows pays for the whole corpus.
- The floor for anything touching the corpus is about **1.2 s**. Competitors
  answer the same statements from an index in **10-35 ms**: `q02` at 11 ms
  against our 1,195 ms, `q20` at 24 ms against our 3,216 ms.
- Hot totals over the 41 statements every system answered: ClickHouse 13.28 s,
  VictoriaLogs 79.97 s, **Ravel 91.79 s**, Elasticsearch 454.10 s.

CPU optimisation does not touch this. A correct hash change (#843/#847)
measured `core::hash::sip` at **8.30% before and 8.44% after** — no movement —
and the on-CPU profile is flat, with no self-time frame above 3.45%. An
independent review of the profiling approach reached the same conclusion from
the other side: *moving fewer bytes is worth more than moving bytes faster on
this box*.

Two capabilities already in the tree show the shape of the answer:

1. **Metadata-only execution already works.**
   `crates/ravel-sql/tests/logs_count_from_stats.rs` has passing tests
   (`predicate_free_count_answered_from_catalog_with_zero_gets`,
   `contained_ts_bound_reports_exact_count_and_span_with_zero_gets`) asserting
   both that no `LogsScanExec` appears in the plan and that the executor issues
   **zero** object-store GETs. It is narrow — predicate-free `COUNT(*)` and
   contained timestamp bounds — but the mechanism is proven in production.
2. **A snapshot-bound index object already exists.** Name postings live at
   `t/<tenant>/catalog/<signal>/idx/<watermark>.<hash16>.npost`, immutable,
   content-addressed, bound to the exact ordered snapshot-part hashes
   (`expected_part_blake3`), rejected outright when decoded against a different
   part set, and already swept by the existing supersession GC.

So this is an extension of existing architecture, not a new database.

### Why ADR-0049's rejection no longer binds

ADR-0049 rejected a global inverted index, and its stated reason was specific:

> it needs a coordinator, an unbounded rebuild, and a durability story, and
> object storage is the only durable backend.

Each of those has since been answered by machinery that shipped for other
reasons: catalog fold plus the `HEAD` CAS protocol is the coordinator;
immutable content-addressed packs rebuilt during compaction bound the rebuild;
S3 is the durability story, unchanged. **The rejection was correct when it was
written and its premises have expired.** That — not preference — is the ground
for revisiting it. ADR-0049's other rejections (row-granularity postings,
widening the bloom) are untouched and still stand.

## Decision

Build an **immutable, snapshot-bound index plane on object storage**, cached in
RAM and local NVMe, used two distinct ways: to skip objects and blocks that
cannot match, and to answer decomposable queries entirely from metadata.
Durable truth stays on S3; local state is a disposable cache. This preserves
the S3-only durability model rather than working around it.

```mermaid
flowchart TD
  HEAD["catalog/&lt;signal&gt;/HEAD<br/>(mutable, CAS)"] --> ROOT
  HEAD --> PARTS["snap/*.csnap<br/>snapshot parts (immutable)"]
  ROOT["idx/*.iroot<br/>IndexRoot (immutable)<br/>bound to ordered part hashes"]
  ROOT --> STATS["idx/*.istat<br/>typed statistics"]
  ROOT --> VALS["idx/*.ival<br/>bounded value counts"]
  ROOT --> PT["idx/*.ipost<br/>point postings"]
  ROOT --> GRAM["idx/*.igram<br/>gram postings"]

  subgraph plans["physical plans"]
    MA["MetadataAggregateExec<br/>zero data GETs"]
    IS["IndexedScanExec<br/>candidates then residual"]
    LS["LogsScanExec<br/>fail-open fallback"]
  end

  STATS --> MA
  VALS --> MA
  PT --> IS
  GRAM --> IS
  ROOT -.->|absent, stale, or<br/>version-mismatched| LS
```

### 1. Packs are immutable, content-addressed, and snapshot-bound

Every pack is an immutable object under the **existing**
`t/<tenant>/catalog/<signal>/idx/` prefix, distinguished by suffix. This is an
additive key-layout change: a new suffix under a prefix that already exists and
already carries `.npost`. **No new prefix — but a new lifecycle.** Discovery and
protection are separate things, and only the first is inherited: the sweeper
already *lists* the whole `idx/` prefix, so new suffixes are found, while the
reference set that decides what survives is exactly `HEAD.parts[].key` plus the
optional `postings.key`. Supersession GC therefore does **not** already cover
`.iroot` or leaf packs; protection must be extended before either is published.
§1a below is the normative statement of that work and it is wave 0.

Binding is **two-level, and this is load-bearing**. The `IndexRoot` alone binds
the exact ordered snapshot-part hashes, exactly as `.npost` does today. **Leaf
packs bind per part**, with ordinals **part-local**.

**A leaf binds the covered part's exact `SnapshotPartRef.blake3`, never its
`watermark_hour`.** `watermark_hour` is a stable *range label*, not a content
identity: a replacement part produced by compaction can carry the same
`watermark_hour` while changing entries and the ordinal mapping. Binding a leaf
to the label would accept a stale leaf against rewritten data and silently omit
matches — the same wrong-results class as the config-inferred coverage below,
reached by a different route. The stability of the label buys nothing here and
must not be mistaken for stability of the content. A regression test covers
exactly this: same `watermark_hour`, different part bytes, leaf must be
rejected.

Whole-set binding on every leaf was the draft's design and it is wrong. Every
content-changing fold appends or replaces a part, so exact-whole-set binding
would unbind **every** pack on **every** fold, even where no ordinal moved. For
one small `.npost` object that is free; for point postings over ~10^7 distinct
values plus gram packs over a 12 GB corpus it means rebuilding the entire index
plane per fold, and on a live-ingest tenant folding hourly the index rebuild
bytes can exceed the ingested bytes. It also contradicts the incremental build
pipeline in §3 — building fragments during compaction only to discard them on
every publish.

With two-level binding an appended tail invalidates only the root (small), and
a compacted hour invalidates only that hour's packs. A pack decoded against a
part set it does not bind is rejected rather than trusted, unchanged.

### 1a. The lifecycle is NOT inherited. It must be extended first.

The draft claimed new pack suffixes inherit the existing GC lifecycle. **That is
false, and the failure is destructive rather than leaky.**
`sweep_unreferenced_catalog_objects` lists the whole `idx/` prefix — so new
suffixes *are* swept — but its reference set is exactly `HEAD.parts[].key` plus
the optional `postings.key`. A pack the `HEAD` does not name is an unreferenced
object and is **deleted** once past the 25 h 05 m protection horizon.

Therefore, in this order, before any fold writes a pack:

1. Extend `SnapshotHead` to name the `IndexRoot`, and decide how the sweeper
   protects **leaves**: either the `HEAD` names every pack key, or the sweeper
   decodes roots to build its reference set. This interacts with the binding
   granularity above and must be settled with it.
2. Ship and fully roll out the sweeper's reference-set change **before** the
   first pack-writing fold reaches production.
3. **Gate every `HEAD` writer before the first root is published.** Upgrading
   the sweeper closes only half of the hazard: a lagging folder that CASes a
   `HEAD` without the index field strips the live root reference, and the *new*
   sweeper then correctly computes a reference set that omits it and reaps the
   packs. Either every `HEAD` writer must preserve the field it does not
   understand, or old writers must be blocked before the first root publication.
   Mixed-version folder-and-sweeper combinations are a required test matrix, not
   an afterthought.

**These are enforced gates, not a recommended order.** Publication of the first
`IndexRoot` is blocked until every `HEAD` writer in the fleet is known to
preserve fields it does not understand — either by having rolled the fleet, or
by refusing writes from versions that cannot. The rollout carries tests for both
mixed-version combinations explicitly: old sweeper against new `HEAD`, and new
sweeper against a writer that strips the field. A sequencing intention that
nothing checks is indistinguishable from no sequencing at all once the fleet is
mid-roll.

Two rolling-upgrade hazards, both the ADR-0066 decision 2 shape, and both must
be closed by those gates rather than by hope:

- an **old sweeper** decodes a new `HEAD`, drops the unknown proto field,
  computes the old reference set, and reaps live packs at the horizon;
- a **lagging folder** decodes the new `HEAD`, ignores the unknown index field,
  and CASes a `HEAD` *without* it — after which the sweeper reaps.

Correctness survives both via fail-open, but the index plane silently
self-destructs on every mixed-version window and must be rebuilt. This makes
`HEAD` and sweeper plumbing **wave 0** of the epic, not a later concern.

**Intermediate fragments have the same problem and the same answer.** The L1
fragments of §3 are objects no `HEAD` ever names, written by a compaction that
may die before the fold consumes them. They are therefore unreferenced from the
moment they exist. "Fold-side only" does not resolve this, because §3's own pipeline has L1
compaction *produce* fragments and a later catalog fold *consume* them: two
stages, not one atomic operation, so a fragment outlives its producer by
construction. The choice is therefore forced, and one of these is picked before
any code writes a fragment:

- **(a) No durable fragments**, with the packs built by whichever stage can
  actually read the rows. See the constraint below before assuming this is the
  fold.
- **(b) Durable fragments with a stated lifecycle**: an explicit key prefix
  distinct from published packs, a named owner, an abandonment deadline shorter
  than the protection horizon, and a reclamation rule keyed on that deadline
  rather than on `HEAD` reachability (a fragment is never `HEAD`-reachable, so
  the sweeper's reference-set rule cannot govern it).

**Option (a) as written is not implementable, and saying why fixes the section.**
"The fold recomputes it" assumes the fold decodes rows. It does not: **the fold
reads commit records, while the per-block statistics a pack needs live in
`SKIP_IDX` sections inside the data objects.** Only compaction and a
purpose-built maintenance pass ever decode those. So the real question is not
whether fragments are durable but **what carries statistics from the stage that
can compute them to the stage that publishes them**, and there are three
candidate carriers:

1. a durable fragment object, with the full lifecycle of (b) above;
2. an additive field on the commit record compaction already writes, which the
   fold already reads — no new object class, but a frozen-contract change under
   the format-change procedure;
3. a maintenance pass that reads objects and writes packs directly, skipping the
   fold as builder.

**This ADR deliberately does not pick between them here, and gates on that.** The
carrier is settled in the backfill ticket's spec, because backfill and the
incremental path must use the *same* carrier or the plane grows two build paths
that can disagree — and backfill is a precondition for measuring any of this
anyway.

To keep that deferral honest rather than open-ended: **no pack may be written
until the carrier is chosen, and the choice must name its schema, its producer
and consumer, its retry behaviour on a half-finished build, and its reclamation
rule.** If durable fragments win, the choice additionally states how a fragment
is protected while the fold is consuming it and what deadline abandons it. An
undecided carrier does not block wave 0 (`HEAD` and sweeper plumbing carry no
statistics), so this gate costs no sequencing — it only forbids enabling writes
into a pipeline whose middle is unspecified. What this ADR does
fix is the constraint: no design may assume the fold can compute statistics from
data it never reads.

Leaving this undefined would give an object class with no owner, reaped too
early or never — which is how storage leaks and mysterious mid-fold failures
both start.

### 2. Routing must never be per-object

A point lookup resolves through a small cached root to one or a few leaves.
Packs are sharded by field, index type, and value or hash range.

**A design that probes one index sidecar per data object is rejected by
construction**: it replaces 3,469 data GETs with 3,469 index GETs and moves
nothing. This is the constraint that rules out the obvious "put an index next
to each object" approach, and any future proposal must be checked against it.

**Normative routing budget.** "Small" and "a few" are not enforceable, so the
caps are stated and asserted per phase:

- **root probe: at most 1 GET** per (tenant, signal) per query, cache-missable;
- **leaf probes: a cap per index type**, each declared in the root and each a
  function of pack layout, never of object count: `L_point` for a single-field
  point predicate, `L_gram` for a gram lookup, and for a compound predicate a
  **global per-query ceiling** that the sum across fields may not exceed — an
  AND over `k` fields must not cost `k` times a single-field lookup by default,
  since the planner may probe the most selective field first and evaluate the
  rest as residual;
- the caps share **one per-query total**, so a mixed or compound plan reading
  both point and gram packs cannot spend `L_point + L_gram` and run two leaf
  waves: the total is what the round-trip bound is stated against, and the
  per-type caps only bound each type's share of it. Zero and negative caps are
  rejected in root validation, and a root declaring any probe at all must
  declare positive parallelism;
- **probe parallelism** is declared too, and it is **bound to the per-query
  total**, since `L`
  sequential probes and `L` concurrent probes are the same request count and
  very different latency. With `parallelism < L` the leaves resolve in
  `ceil(L / parallelism)` waves and cold routing costs
  `1 + ceil(L / parallelism)` sequential round trips, which breaks the
  two-round-trip bound stated below. **This ADR requires `parallelism >= L`** for every
  declared root, so the two-round-trip bound stays literally true rather than
  becoming a formula. Root validation rejects a root declaring `L > parallelism`,
  and the shape lint covers that case explicitly — it is a rejected input with a
  test, not a possibility to be assumed away;
- every cap has a **finite maximum fixed in root validation**, and a root
  declaring a larger one is rejected. Without a ceiling, "declared in the root"
  only means a root can declare whatever it likes, and a root declaring
  `L_point = 5000` would satisfy every rule in this section while probing more
  objects than the tenant has — the per-object design §2 rejects, arrived at
  through a validation gap rather than a design choice. The maxima are constants
  of the pack layout and are never a function of object count; the shape lint
  covers an oversized declaration as a rejected input;
- a predicate whose shape has **no declared cap** does not get a
  best-effort probe: it falls back to scanning. An uncapped path is how bounded
  routing degrades into the per-object design this section rejects;
- cold routing therefore costs **at most 2 sequential round trips** before the
  first data GET — call it 40-100 ms in-region, which is why cold index numbers
  are published separately from hot ones (see Consequences).

A query whose routing exceeds its budget fails the shape lint rather than
silently costing more than the scan it replaced. This makes the per-object
rejection above mechanical instead of a matter of review taste.

**There is no tenant-level membership filter today, and this ADR does not
pretend otherwise.** An earlier draft of this section claimed ADR-0050 §2 gave a
tenant-hash pre-check that could resolve an absent value at zero index GETs.
That was a misreading twice over: ADR-0050 §2 is `tenant_hash` mismatch failing
closed, a tenant *isolation* invariant, not value membership; and the filters
that do exist are ADR-0029's **per-block token blooms living inside the RLOG
object**, which cannot pre-empt a lookup because reaching them means opening the
object this design exists to avoid opening.

A tenant-scoped membership filter would be a **new pack type in this plane** —
cheap, worth building, and listed here as a candidate rather than borrowed from
an ADR that does not provide it. If one is built it must cover **token-resolved
segments** as well as snapshot data, or bypass the pre-check whenever a pinned
commit token contributes segments: those segments come from direct commit-record
GETs outside the snapshot, so a filter built only over snapshot data can report
a confident zero for a value that a token segment holds. That is a wrong result,
not a slow one.

### 3. The safety lemma

```text
query candidates = index matches within covered ordinal ranges
                 ∪ every segment overlapping an uncovered range
```

A missing, stale, corrupt, or version-mismatched index therefore makes a query
**slower, never wrong**.

**The unit is the ordinal range, not the object**, and the two are not
interchangeable. Coverage is declared per (entry-ordinal range x field x index
type), so an object can be partly covered — some ordinals indexed, others not,
or indexed for one field and not another. A lemma written over whole objects
would let a partly-covered object count as covered and drop the matches sitting
in its uncovered remainder, which is a false negative and a wrong answer. Either
the planner subtracts coverage at range granularity as written above, or it
falls back to treating any object with an uncovered range as wholly uncovered.
Both are sound; the second is simpler and strictly more conservative. What is
not sound is mixing them, and partial-coverage cases carry their own tests
precisely because whole-object reasoning looks correct until an object is
half-indexed.

**Coverage is not a bit.** It is three-dimensional — **(entry-ordinal range) x
(field) x (index type)** — declared in the root, with each pack
complete-by-construction for its declared scope (the encode-side validation
precedent in `encode_postings`). Two consequences that are correctness
requirements, not refinements:

- **A pack's field set is data declared in the root, never inferred from live
  config at read time.** `TenantConfig.indexed_fields` (ADR-0079) is mutable
  per-tenant config. Build a pack while field `F` is curated, de-curate `F`,
  let a Class-B rebuild run, and the new pack lacks `F`. A query on `F` against
  per-object coverage would then see "covered, zero index matches" and
  **wrongly exclude matching objects — a wrong result, not a slow one.** A field
  absent from the root's declaration means *uncovered for that field*,
  regardless of object coverage. With this rule Class B stands, because a
  rebuild need not reproduce identical packs, only packs valid for their own
  declaration.
- **A leaf that fails validation subtracts its own coverage.** A leaf is usable
  only after hash, version, part-binding and decode validation all pass. On any
  failure the planner removes *that leaf's* (ordinal range x field x index type)
  contribution from the root-declared coverage and scans the affected scope.
  Falling back to scanning while still counting the scope as covered is the
  worst of both: the prune omits matching entries and nothing reports it. Each
  failure mode gets its own test.
- **Partial coverage within an object** (some blocks indexed, an entry
  half-processed by L1) requires the ordinal-range dimension, or fold must never
  mark an entry covered until it is fully indexed.

**The lemma covers candidate pruning only.** `MetadataAggregateExec` is *not*
protected by the union-with-uncovered shape; its safety is the stricter
condition list in §5. Reading "slower, never wrong" as covering metadata
execution would ship a wrong count, so the two mechanisms are named separately
and must be tested separately.

**Enumerating uncovered objects costs no extra I/O.** Snapshot resolve already
decodes all part entries per query, so uncovered = live ordinals minus the
root's covered ranges, plus above-watermark listed segments, plus
token-resolved segments. Read-your-write token segments come from direct
commit-record GETs outside the snapshot and are therefore structurally in the
uncovered branch; the planner must place them there.

This is what lets coverage lag ingest, and it is why the index can be built off
the acknowledgement path:

- an L0 write is acknowledged **uncovered**; queries scan that recent tail
- **L1 compaction** builds index fragments while it is already decoding and
  rewriting rows
- **catalog fold** combines fragments into covering packs and publishes a new
  root through the existing `HEAD` CAS protocol

Synchronous L0 sidecar indexing is available as an **opt-in per-tenant
profile** for workloads needing point search on fresh data. It is not the
default, because it adds an object-store dependency to the acknowledgement
path.

### 4. Migration class

Index packs are **Class B** under ADR-0066 decision 4 — derived catalog
objects, alongside `.csnap`, `.npost` and `HEAD`. They are rebuildable **once a statistics carrier is chosen** (§3): rebuild-from-
commit-records holds only for the carrier that puts statistics on the commit
record, and the other two candidates rebuild from the objects instead. Under any
of the three the fold rewrites packs continuously, so a version bump needs no
migration tool: the upgraded fold emits the new version,
supersession GCs the old packs, and dual-read is needed only across the
rolling-upgrade window. A reader meeting an unsupported pack version treats it
as **absent** and falls back to scanning, which the safety lemma already makes
correct.

### 5. Three physical plans

- **`MetadataAggregateExec`** — emits from exact statistics and value counts,
  constructing no `LogsScanExec` at all.
- **`IndexedScanExec`** — resolves object and block candidates from covering
  packs, then evaluates residual predicates normally.
- **`LogsScanExec`** — the existing fail-open fallback, unchanged.

Metadata execution is selected **only** when every live **ordinal range** the
statement needs is covered **for every field and index type it reads** — not
merely when every live object appears covered, which is the whole-object
reasoning §3 rejects. A statement grouping on a field whose pack omits that
field is uncovered for it, however complete the object coverage looks. Also, the
complete predicate and aggregate are representable, type and null/NaN semantics
match the scan exactly, no pending selective erasure invalidates the counts, no
row-level policy requires row inspection, and the pinned commit token adds no
uncovered segment. Otherwise the planner uses an indexed scan or a full scan.

**The erasure condition needs a mechanism, and this ADR defers building it.**
Metadata execution is itself deferred (it cannot fire on a live tenant, see
Consequences), and the epoch exists only to make metadata execution safe. So
what follows is a **statement of the constraints any future epoch design must
satisfy, not a design this ADR commits to.** It is recorded here because the
constraints were established the expensive way and would otherwise be
rediscovered. Three of them are non-obvious and each was reached by getting it
wrong first:

- **the allocator is tenant-scoped, and `HEAD` CAS cannot provide it.** `HEAD` is
  per `(tenant, signal)`, so allocating from it gives acknowledgements on
  different signals duplicate or unordered values. A tenant-scoped allocator is
  a separate object with its own CAS;
- **the value is a generation counter, not a timestamp** — two acknowledgements
  can land in the same nanosecond and a comparison that ties admits a pack it
  should reject;
- **the epoch is derived from applied state, never from build time**, and the
  publication order between allocation, acknowledgement, rewrite and fold must
  be stated, not assumed;
- **the epoch a query compares against is pinned with the query snapshot**, or
  revalidated at a defined linearization point before results are returned.
  Reading the epoch separately from resolving the snapshot leaves a window in
  which the epoch advances mid-query and the answer belongs to neither state;
- **an acknowledgement must never become visible before its epoch CAS
  succeeds.** Either one CAS-controlled record carries both, or the
  acknowledgement publishes only after the epoch is allocated, with idempotent
  recovery for a crash between the two. An acknowledgement visible against an
  unadvanced epoch is a window in which a stale pack is admitted, which is the
  whole failure this mechanism exists to prevent.

The rest of this subsection is that constraint list.

**Why a clause is not enough.** ADR-0064 erasure is
acknowledged before the rewrite lands, and the fold that would refresh a pack
runs later still, so a stale pack can outlive an acknowledged erasure by up to
the rewrite plus fold interval (order 72 h). For **pruning** this is safe by the
lemma: over-selection is corrected by the scan's own row-level filter. For
**metadata execution** it is not — a `COUNT` served from a pack built before the
erasure returns the pre-erasure number, which is a wrong answer to a compliance
operation. The condition is therefore enforced by an erasure epoch, and the carrier does
not exist yet: `ErasureRequest`, `ErasureCompletion` and `RewriteRecord` carry
`created_unix_ns`, `requested_unix_ns` and `completed_unix_ns`, but no
scope-wide freshness value a pack could be compared against. Concretely:

- the tenant's **erasure epoch** is a **persistent, strictly increasing
  generation counter**, not a maximum over the live request set and not a bare
  wall-clock reading. A timestamp is the wrong carrier for the same reason build
  time was: two acknowledgements can land in the same nanosecond, and a
  comparison that ties admits a pack it should reject. The counter is allocated
  at acknowledgement and persisted atomically under the same CAS discipline the
  `HEAD` protocol already uses, so concurrent acknowledgements serialise rather
  than collide. ADR-0064 deletes `.dreq` after
  completion plus the horizon (and that deletion is a privacy requirement, not
  a cleanup convenience — the `.dreq` carries the subject identifier), while
  `.done` is permanent and carries no epoch. A maximum over live requests
  therefore *decreases* when a request is reaped, and a stale pack that was
  correctly rejected yesterday starts satisfying `pack_epoch >= tenant_epoch`
  tomorrow, readmitting pre-erasure counts. The high-water mark is advanced
  atomically at acknowledgement and retained after `.dreq` deletion. It is a
  bare integer and deliberately carries no request id, subject identifier, or
  anything else derived from the request, so retaining it permanently does not
  recreate the reason `.dreq` is deleted;
- **every metadata pack** — `.istat` statistics and `.ival` value counts alike,
  and anything else `MetadataAggregateExec` later reads — records an epoch
  **derived from the erasure state actually applied in the source data it
  summarises**, never from its own build wall-clock. Scoping this to statistics
  packs alone would leave a stale `.ival` free to answer a `GROUP BY` with
  pre-erasure counts, which is the same wrong answer by a different input.
  Selection validates **every** metadata input the plan will read, and a single
  failing pack sends the whole statement to a scan. Concretely it is the minimum, over the parts the pack covers, of
  the epoch each part's *content* reflects, which means the applied epoch has to
  be carried on the rewritten parts rather than inferred. Build time is the
  wrong clock: a pack built after acknowledgement but before the rewrite is
  visible still summarises pre-erasure rows, and stamping it with the new
  high-water mark would let it satisfy the comparison below and return
  pre-erasure counts — the very outcome the epoch exists to prevent. The
  alternative, blocking pack publication until rewrite and fold have
  incorporated the acknowledgement, is also sound and simpler to reason about,
  at the cost of stalling indexing behind every erasure; whichever is chosen is
  stated in the implementing ticket, but deriving the epoch from build time is
  not among the options;
- `MetadataAggregateExec` is selected only when `pack_epoch >= tenant_epoch`,
  and falls back to scanning otherwise.

Ordering matters and is stated so it cannot be assumed away: a request is
acknowledged before the rewrite lands and long before the fold republishes, so
the epoch must advance at **acknowledgement**, not at completion. The required
test walks acknowledgement to rewrite to fold and asserts three things: that the
epoch never decreases across the sequence, including across `.dreq` reclamation;
that a pack built **before** acknowledgement is rejected; and that a pack built
**after acknowledgement but before the rewrite is visible** is also rejected.
The third case is the one a build-time epoch passes and a content-derived epoch
catches, so testing only the first two would leave the mechanism looking correct
while the gap stayed open. An epoch that
advanced only on completion would leave exactly the window this condition
exists to close. "No pending selective erasure" as prose with no carrier is the
shape that ships unimplemented.

**Exactness or fallback, never estimation.** These figures answer queries
directly, so a bound is not sufficient: `q02` needs the exact count of one
value, and a truncated dictionary must force a scan rather than produce a
number.

### 6. Clustering is a companion, not a substitute

The narrow, time-leading slice of ADR-0815 — cluster during compaction, lead
with EventTime, publish narrow per-part bounds, exclude before any data GET — is
**not sequenced by this ADR, and must not be measured on the ClickBench tenant.**
An earlier draft landed it as item 2 on the strength of `q37`-`q43`, and
Consequences now shows that reasoning was backwards: on that corpus a global
EventTime sort would scatter the `CounterID` locality the surviving prune depends
on. Keeping "lands as item 2" here while Consequences says it would regress those
statements would leave the document contradicting itself.

What holds: clustering and indexing are companions, and a single sort key cannot
serve both time ranges and high-cardinality point lookups — the latter is what
point postings are for. What changes: EventTime-leading clustering is justified
by **organic telemetry**, where ingest arrival order already correlates with
event time and recent-window queries dominate, so the scattering hazard does not
arise. It is therefore gated on a pre-registered A/B against `q37`-`q43` before
touching any measured tenant, and on a target workload that is not a bulk-loaded
entity-sorted corpus. For *this* tenant the selective clustering key would be
`CounterID`, which is ADR-0815's deferred override-key path rather than the
time-leading slice.

## Rejected alternatives

1. **Per-object index sidecars.** Rejected: a lookup then probes one sidecar
   per object, turning 3,469 data GETs into 3,469 index GETs. It relocates the
   floor instead of removing it. This is the single most important rejection
   here because it is the design most people reach for first.

2. **A mutable B-tree or LSM on S3.** Rejected: object storage has no atomic
   multi-object update and poor economics for many small mutable writes. Every
   update becomes a read-modify-write of a shared node under contention.
   Immutable packs with an atomic root swap match what S3 is actually good at:
   immutable objects and a single CAS pointer.

3. **Row-granularity postings.** Rejected, unchanged from ADR-0049: the scan
   re-evaluates predicates exactly anyway, so row precision changes no result
   and costs bytes at rest. Block and object ordinals are the useful grain.

4. **Synchronous L0 indexing as the universal default.** Rejected as a default:
   it puts a second object-store dependency on the acknowledgement path, which
   trades write availability for read latency on every tenant to benefit the
   few that need fresh-data point search. Retained as an opt-in profile.

5. **Widening the existing per-object bloom filters instead.** Rejected,
   unchanged from ADR-0049: cheaper, but it cannot be exact, and lowering the
   false-positive rate costs bits geometrically without reaching zero. A
   selective query over thousands of blocks still scans a tail of them, and
   still opens every object to read the filter.

6. **Rely on clustering alone.** Rejected: one sort order cannot simultaneously
   serve time-range scans and high-cardinality `UserID` lookups. Clustering
   handles ranges; postings handle points; neither substitutes for the other.

7. **A general external-sort subsystem for arbitrary clustering keys, first.**
   Rejected as sequencing: it is the most complex piece of ADR-0815 and its
   value is unmeasured until basic time clustering has been landed and
   measured. Deferred, not rejected on merit.

## Consequences

**Queries that stop scanning.** `q02`, `q07` and `q08` become answerable at
zero data GETs from statistics alone. `q20` becomes proportional to matching
blocks rather than to corpus objects. `q23` is bounded by positive gram
candidates — true, and possibly vacuous, since a common substring in a web
corpus can appear in nearly every block, which is why item 4 is deferred behind
a selectivity measurement rather than assumed.

**`q37`-`q43` are fixed by block-granular statistics pruning (§5's
`IndexedScanExec` consuming per-object and per-block min/max), not by time
clustering, and clustering would make them worse.** An earlier draft said those statements open a proportional
fraction of objects once time clustering lands. That is wrong on mechanism and
on sign. Their date filter is `EventDate BETWEEN 15887 AND 15917`, all of July
2013 and therefore the entire corpus, so it prunes nothing. The selective
predicate is `CounterID = 62` at roughly 0.8% of rows, and the measured 144 of
17,731 surviving blocks is 0.81% — CounterID's row share, not a time window.
Pruning works today because the corpus arrives sorted by
`(CounterID, EventDate, ...)` and the load preserves those runs, leaving blocks
near-single-CounterID. A global EventTime sort would scatter each counter's rows
evenly, leave roughly 60 matching rows in nearly every block, and stop that arm
pruning at all. ADR-0815 carries the same misattribution.

**Queries that do not, and the honest count.** Full `SUM`/`AVG` over the corpus
still scans without materialised aggregate states. **An index alone does not
remove all 34 scans.** Reviewed statement by statement, the 41 account for exactly as follows, each
statement counted once:

| group | count | statements |
|---|---|---|
| already zero-GET today | 1 | `q01` |
| newly answered from metadata | 3 | `q02`, `q07`, `q08` |
| pruned by block-granular statistics | 7 | `q37`-`q43` |
| pruned by point postings | 1 | `q20` |
| uncertain, gram postings | 4 | `q21`-`q24` |
| **untouched by anything in this ADR** | **25** | the rest |
| total | 41 | |

So **11 statements move decisively** (3 + 7 + 1), four more may move by an
unmeasured amount, and 25 do not move at all. Point postings also help `q41` and
`q42`, but those are already inside the seven and are not counted twice. The 25
are `COUNT DISTINCT`, high-cardinality `GROUP BY`, SELECT-list arithmetic and
regexp, and that is where the hot-total gap against ClickHouse actually lives.
Roughly 25 statements (`COUNT DISTINCT`, high-cardinality `GROUP BY`,
SELECT-list arithmetic, regexp) are untouched by anything here, and that is where
the hot-total gap against ClickHouse actually lives. Rollups are item 5 and are a
different mechanism. Items 3 and 4 are the most machinery for the fewest
benchmark statements; their real justification is telemetry point lookup and log
substring search, which ClickBench does not measure, and the epic says so rather
than letting them fail their own stated justification.

**Coverage does not reach existing data without a backfill pass.** The build
pipeline above covers newly ingested data only: a tenant already fully compacted
and quiescent never re-compacts, so no fragment is ever built and no pack is
published for data that already exists. Fold cannot close this alone, because it
reads commit records while the per-block statistics a pack needs live in
`SKIP_IDX` sections inside the objects. An explicit index-build maintenance pass
is a precondition for measuring any of this on the reference tenant, which is
itself quiescent.

**Metadata-only execution is a benchmark capability until it is made hybrid.**
Selection requires every live object covered, and a live-ingest tenant always has
an uncovered L0 tail by construction — the same lag that makes building off the
acknowledgement path possible. So the zero-GET path fires only on quiescent,
fully folded tenants. The generalizable form answers from metadata over covered
ordinal ranges and scans the uncovered remainder, composed exactly as the pruning
lemma composes. This also bounds what the erasure epoch is for: it protects
metadata *answering* only, never pruning, so it is deferred with the executor it
serves rather than built alongside the statistics.

**New cost, with bands pre-registered before the first measurement.** Index packs
consume storage and fold time. Packs are subject to
the same MVCC protection horizon as the data snapshot they describe, so they
inherit the same delayed-reclamation behaviour already measured (a tenant holds
roughly 1.9x its live set during the 25 h 05 m horizon).

**Cost accounting is not optional here.** Root and leaf probes are their own
phases under the repo's per-phase cost rule, never folded into the scan's
counters. An index that removes 3,469 data GETs and silently adds 400 index
GETs must show as exactly that, or the plane cannot be judged. The routing
budget above is the assertable band.

**New failure mode, bounded by construction.** A corrupt or stale pack degrades
to scanning. That is a latency regression, never a correctness one, and it is
the property that makes the whole plane safe to land incrementally.

**Honesty about the comparison.** The competitors' 10-35 ms figures are hot,
local-index numbers. Cached roots and metadata-only execution can approach
them; a genuinely cold lookup needing several S3 round trips will not, and must
be reported separately. **Stateless is not cacheless**, and hot and cold index
results are to be published as distinct numbers rather than blended.

Refs: #849, #680, #815, #835, #843
Supersedes in part: ADR-0049 (the global-inverted-index rejection only)
