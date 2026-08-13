# ADR-0064: selective subject erasure and required bucket lifecycle configuration

Epic EJ (issue #460), program #450. Covers findings S4-10 (no selective or
per-subject deletion; the review's section 11 analysis concludes the current
guarantee is "query exclusion plus whole-object deletion, not erasure"),
S2-16 (bucket lifecycle and versioning interactions unaddressed), and S4-12
(delete on a versioned bucket leaves recoverable prior versions invisible to
`verify-custody`). Both review passes rate S4-10 OPEN, severity high,
certainty certain, and name it (with S4-04) "the most expensive architectural
mistake ... the two capabilities a compliance-bound customer will demand on
day one," getting "strictly more expensive with every TB stored."

## Context

### What deletion means in Ravel today

Every deletion primitive Ravel has destroys whole objects, never rows:

- **Age-based retention** (ADR-0019): a per-bucket `RetentionTombstone`
  written `CreateIfAbsent` to `t/<th>/<sig>/c/<shard>/<hour>/retire.tmb`,
  logical exclusion at snapshot resolution, then a horizon-gated physical
  sweep (`crates/ravel-maintain/src/retention.rs`). Granularity is one
  (tenant, signal, shard, ingest-hour) bucket.
- **Supersession sweep** (ADR-0018) and **orphan/unreferenced GC**
  (ADR-0010 §11, ADR-0048): delete redundant or record-less whole objects.
- **Whole-bucket destruction**: outside Ravel entirely.

A GDPR/CCPA erasure request names a *subject* — a label or attribute value
(`user_id="u123"`, `client.address=...`) scattered across every hour bucket,
every shard, both L0 and L1, for as long as the tenant has retained data.
Nothing can act on that. The failure-matrix row for S4-10 is blunt: detection
"N/A (no mechanism)", recovery "Destroy whole bucket, or wait for age-out."
ADR-0019's consequences anticipated this exactly: "the same tombstone-then-
sweep machinery is the natural substrate for future explicit deletion
(tenant offboarding, GDPR-style deletes); those need their own ADR for
selection semantics but no new visibility mechanism." This is that ADR.

### The immutability invariant shapes the only possible mechanisms

Data objects, commit records, manifests, and index objects are immutable
(repo invariant; docs/catalog-and-mvcc.md MVCC rules). "Selective deletion"
therefore cannot be in-place editing of any object. The mechanisms
compatible with the invariant are:

1. **Rewrite-and-supersede**: build new segments without the subject's
   records, publish a record that atomically swaps inputs for outputs in
   new snapshots (exactly compaction's publish-then-supersede shape), and
   let the existing horizon-gated sweep physically remove the inputs.
2. **Durable predicate + query-time exclusion**: a tombstone-like record
   that resolvers observe and queries filter by. Alone this is precisely
   the "query exclusion, not erasure" posture section 11 rejects, but it is
   the correct *bridge* while a rewrite is in flight.
3. **Crypto-shredding**: physical bytes survive; the key dies. Examined and
   rejected below.

### What the review requires of bucket configuration (S2-16, S4-12)

The review's hidden-assumption list includes "the bucket has no lifecycle
expiration or versioning rules (S2-A5, S4-A6, S5-A7)" — an assumption the
whole deletion and GC story rests on and that no document states. Two
concrete failure rows:

- Lifecycle expiration on data prefixes (S2-16): "Mass permanent loss under
  live commit records ... data unrecoverable." ADR-0019 already *rejected*
  S3 lifecycle rules as a retention mechanism for this reason, but rejecting
  them as our mechanism is not the same as telling operators they must not
  configure them. Meanwhile S5-19 notes incomplete-multipart reaping
  currently *depends* on an undocumented lifecycle rule.
- Delete on a versioned bucket (S4-12): "'Deleted' data recoverable as a
  prior version ... incomplete erasure", invisible to `verify-custody`. A
  versioned bucket silently converts every Ravel delete — retention,
  sweep, and this ADR's erasure — into a soft delete.

### Constraints already landed that this ADR must compose with

- **ADR-0055 (EE, landed)**: per-role credentials. Only the Maintain role
  holds any `s3:DeleteObject` grant, and only over `l0/`, `l1/`, `c/`,
  `idem/`. Every role is denied delete on `sys/*`, `prov`, `catalog/*`, and
  the audit prefix `t/<th>/u/**`. ADR-0055's consequences call this out by
  name: "EJ's own ADR should state explicitly which role performs selective
  deletion (most naturally an extension of Maintain's existing
  delete-capable role) rather than inventing a fifth role for it." Issue
  #460's note makes the reverse obligation binding: whichever ADR lands
  second must address the interaction. This ADR is second; §6 of the
  Decision does.
- **ADR-0042 (legal hold)**: a `LegalHoldCheck` consulted before every
  physical delete in sweep and retention; a hold present means the delete
  is skipped. Hold records are immutable set/clear audit records folded to
  current state (`crates/ravel-maintain/src/legal_hold.rs`). Erasure must
  define its resolution order against an active hold.
- **ADR-0042 decision 3 / ADR-0055 §3**: bucket-level S3 Object Lock is an
  out-of-band operator step Ravel can neither set nor enforce per object
  (`object_store` 0.14.1 has no per-PUT retention API); `store qualify`
  carries an informational Object Lock/versioning probe. A bucket-default
  retention period is a direct constraint on erasure latency.
- **ADR-0048**: compaction's record-count conservation gate (outputs must
  conserve input sample counts exactly, pre-publish). A rewrite that
  *deliberately* drops records needs its own conservation arithmetic, not
  an exemption.
- **ADR-0046 (read cache)**: query nodes hold raw byte ranges of immutable
  content-addressed objects on local disk, and S4-09 notes there is no
  delete-driven invalidation. The acceptance criterion "a subsequent query
  cannot return them from a cache" must hold against both tiers.
- **Catalog contents**: `SnapshotEntry` and `SnapshotPartHeader`
  (proto/ravel/catalog.proto) carry identities, hashes, counts, and
  timestamps — no label or attribute values. Name postings carry metric
  names only. So catalog objects (deny-deleted under ADR-0055) hold no
  subject identifiers, *provided* subject identifiers appear only as label/
  attribute values, never inside metric names. That proviso becomes a
  documented requirement (§7).

## Decision

Selective erasure is a **durable erasure request, immediate query-time
exclusion, then an asynchronous rewrite-and-supersede pass in Maintain,
then the existing horizon-gated physical sweep** — the same
transaction-first, exclusion-second, physical-removal-third shape every
other deletion in Ravel already has (docs/consistency-model.md "Deletion
and GC"), extended from bucket granularity to predicate granularity.

### 1. The erasure request: a durable, immutable predicate record

A new additive protobuf message `ErasureRequest` (proto/ravel/commit.proto)
and two additive keys in the object layout (docs/catalog-and-mvcc.md):

```
t/<tenant_hash>/<signal>/del/<request_id>.dreq    erasure request (CreateIfAbsent, immutable)
t/<tenant_hash>/<signal>/del/<request_id>.done    completion record (CreateIfAbsent, immutable)
```

`ErasureRequest` fields: identity (tenant_hash, signal, request_id,
created_unix_ns from the injected clock), a **predicate** — a conjunction of
exact-match label/attribute matchers (`key = value` pairs; metrics match
series labels, logs match row attributes, spans match span attributes) plus
an optional event-time range — and an optional free-text reason. v1
predicates are equality-only: exact semantics by default, no regex (see
Rejected Alternatives). Matching granularity: for metrics, every sample of
every series whose labels satisfy the conjunction (intersected with the time
range if present); for logs and spans, every row/span whose attributes
satisfy it.

Requests are submitted by `ravel-cli erase submit` under the **Admin**
credential — erasure is an operator/compliance workflow, deliberately not a
tenant-facing HTTP endpoint in v1 (see Rejected Alternatives). The request
is durable at `CreateIfAbsent` ack; that ack timestamp is when every latency
bound below starts.

The `.dreq` object necessarily contains the subject identifier — which is
itself personal data. It is therefore *not* retained forever: §5 defines its
deletion. The `.done` completion record carries only a blake3 hash of the
canonical predicate encoding, the per-bucket dropped counts, timestamps, and
any deferral cause — no plaintext subject identifier — and is permanent
audit evidence of the erasure.

### 2. Immediate logical exclusion (visibility guarantee)

Snapshot resolution gains one small LIST of `t/<th>/<sig>/del/` per resolve
(the prefix is empty for any tenant with no pending erasure, and the listing
shares the resolve's existing store round-trips; if it ever measures as a
cost problem, folding pending-request presence into the catalog HEAD is a
follow-up optimization, not a correctness change). Every pending `.dreq`
predicate is attached to the resolved snapshot, and the scan/materialization
layer filters matching series, rows, and spans out of results — after
fetch, after cache, before any result reaches the caller.

This is the same "durable object in the store, never resolver-side config"
principle ADR-0019 used to reject config-driven age filtering: exclusion
correctness anchors on an object every resolver observes identically, so
there is no config-rollout split-brain.

**Visibility bound**: a query whose snapshot resolves after the request is
durable can never return matching records. Queries already running keep
their pinned snapshots (snapshot isolation), bounded by
`max_query_duration`. So matching data is unreturnable by any query
starting more than 0 seconds — and by *all* queries within
`max_query_duration` (default 30 s) — after the ack.

**Caches**: the disk tier caches raw compressed byte ranges keyed by
content hash (ADR-0046); cached bytes of a pre-rewrite object can only be
consulted by a query whose snapshot references that object, and the filter
above applies to those bytes exactly as to fetched bytes, so no cache tier
can ever surface excluded records to a caller. This satisfies the
acceptance criterion at the semantic level immediately. Physical residue in
caches is bounded separately: the disk tier gains a per-entry max-age
(default 24 h, checked on hit and by the existing eviction walk), so raw
bytes of an erased subject persist on any node's disposable local disk at
most that long past the sweep; RAM-tier decoded structures for a superseded
object are dropped by the same invalidation trigger tombstone observation
already uses (ADR-0010 §10, ADR-0019 consequences). ADR-0046's own posture
("a node with its cache directory deleted mid-flight answers every query
correctly") means an operator needing faster physical cache scrubbing can
simply delete cache directories with zero correctness impact; the
operations guide documents this.

### 3. The rewrite pass: physical erasure by rewrite-and-supersede

A new maintenance rule in `Mode::Maintain` (alongside compaction, retention,
and sweep in `crates/ravel-maintain`), driven per tenant by the same loop:

1. **Scope**: for each pending `.dreq`, every sealed bucket whose live
   record set (`c/<shard>/<hour>/` listing) could overlap the predicate's
   time range (all buckets when no range is given). Event-time pruning uses
   the records' existing `min/max_event_ts_ns`; unsealed (current) buckets
   are deferred to the next pass — their data is already unreturnable via
   §2, and sealing is bounded by `max_ingest_lag` plus one bucket span.
2. **Rewrite**: read the bucket's live segments (L0 objects, or L1 parts if
   a compaction record is live), decode, drop matching records, re-encode
   into new segments of the **same frozen format version** (RSEG for
   metrics, RLOG for logs, RSPAN for spans — producing new valid instances
   of a frozen format is not a format change; no version bump needed), and
   PUT them under the existing L1 part key shape.
3. **Publish**: one new additive record type, `RewriteRecord`
   (proto/ravel/commit.proto; key `c/<shard>/<hour>/rw.<input_set_hash16>.cmt`,
   same prefix as every other record so the existing single LIST discovers
   it). It names its exact input set — L0 commit identities and/or the
   compaction record it supersedes — its output parts, the request_ids
   applied, and per-request dropped counts. Published with
   `CreateIfAbsent`; racing publishers resolve exactly as compaction races
   do.
4. **Conservation, adapted, not waived**: pre-publish the pass asserts
   `sum(output sample_count) + sum(dropped counts) == sum(input
   sample_count)`, the ADR-0048 gate rearranged for deliberate drops. Any
   inequality aborts with a typed error and publishes nothing; the inputs
   stay live and the abandoned parts age out under the unreferenced-part
   rule, exactly as an aborted compaction's do.
5. **Resolver semantics**: a `RewriteRecord` excludes its inputs from new
   snapshots exactly as a `CompactionRecord` does. But **overlap
   harmlessness does not hold for rewrites** — the outputs deliberately
   lack records the inputs contain, so a snapshot including both would
   resurrect erased records through query-time dedup. Two things close
   that hole: (a) the maintenance driver serializes compaction and rewrite
   per bucket — both already run inside the single per-tenant Maintain
   loop (single-replica Recreate deployment, ADR-0034), and epic EI's
   leased maintenance must preserve per-bucket exclusivity as a stated
   requirement; (b) the §2 query-time filter remains active for a
   request's predicate until the `.dreq` is removed in §5, which by
   construction happens only after no resolvable snapshot can still
   reference any pre-rewrite input. Correctness therefore never depends on
   the race not happening; the serialization is an efficiency measure.
6. **Physical removal**: the rewrite's inputs become superseded inputs to
   the existing sweep (`sweep_superseded`), deleted after
   `protection_horizon`, under the same `LegalHoldCheck` gate as every
   other delete.

## Amendment (2026-08-08): the rewrite key must bind to the applied request set, and a rewrite must be able to name a non-L0 predecessor

EJ-T1 (#750) implemented decision 1 and this decision's `RewriteRecord`
shape and found two problems the Stage 4 checkpoint proved rather than
merely argued.

**The collision.** `input_set_hash` was specified as "blake3 over the
sorted inputs" and the key as `rw.<input_set_hash16>.cmt`. A second
erasure batch over a bucket whose live record set has not otherwise
changed since a prior rewrite -- the ordinary case, since sealed buckets
are largely static -- names the identical L0 input set and therefore
hashes to the identical key. Published `CreateIfAbsent`, the second
batch can never land: it collides with the first rewrite's own record
and is silently rejected as already-existing. This defeats "once per
request batch" (decision 3 point 3) for every batch after the first over
a given bucket, which is a compliance dead end for a subject named in a
second, later DSAR against data a first DSAR already caused to be
rewritten.

**The predecessor gap.** Decision 3 point 3 already says a rewrite names
"L0 commit identities *and/or* the compaction record it supersedes" --
but the checkpoint found no field able to carry the second case, and
none able to name a *prior rewrite* as a predecessor at all, which
decision 3 point 3's "recursively" implication (a rewrite superseding an
earlier rewrite, for a bucket erased twice) requires. Reusing
`CompactionInputIdentity` (`RewriteRecord.inputs`, field 6) only names L0
identities; it cannot name a `CompactionRecord` or `RewriteRecord` key.

**Fix, both additive, no field renumbered:**

- `RewriteRecord` gains `string superseded_record_key = 11` (the next
  free field number): the exact key of the live `CompactionRecord` or
  `RewriteRecord` this rewrite supersedes as a whole, populated instead
  of `inputs` when the bucket's live record set is already a compaction
  or rewrite output rather than raw L0 objects. `inputs` (field 6) is
  used exactly as before when the live record set is raw L0. Exactly one
  of `inputs` (non-empty) or `superseded_record_key` (non-empty) is set;
  a decoder must reject a record with both empty or both non-empty as
  invalid. This directly satisfies decision 3 point 3's "and/or" clause,
  which was previously aspirational text with no schema behind it, and
  makes recursive supersession (rewrite-of-a-rewrite) nameable: the
  predecessor's key is just a string, regardless of whether it is a
  `l1.<hash16>.cmt` or an `rw.<hash16>.cmt` key.
- `input_set_hash`'s preimage is corrected to bind the applied request
  set, not only the input set: `blake3(canonical_input_bytes ++
  sorted(applied request_ids))`, where `canonical_input_bytes` is the
  sorted-inputs encoding the field already used (or, when
  `superseded_record_key` is set instead, that key's own bytes) and
  `sorted(applied request_ids)` is the lexicographically sorted list of
  `RewriteDrop.request_id` values this record applies (field 9). Two
  batches with different request_ids now hash differently and never
  collide, closing the bug. A retry of the *same* batch (same inputs,
  same applied request_ids -- the crash-and-retry case decision 3's
  `CreateIfAbsent` idempotency depends on) still hashes identically and
  still lands for free, so idempotent retry is preserved exactly as
  designed.

This is a key-derivation and schema-additivity correction to decision 3,
not a new decision; it must land before any task that encodes
supersession matching on top of `RewriteRecord` (EJ-T2) or that
publishes rewrite records (EJ-T4).

### 4. Completion, verification, and the stated worst-case bound

When every bucket in a request's scope has a live record set consisting
only of rewrite outputs with that request applied (re-verified by a fresh
LIST per bucket, the ADR-0048 re-verify discipline), the pass writes the
`.done` record. Completion is verified, not assumed.

**The deletion guarantee Ravel makes**, to be stated normatively in a new
"Deletion guarantees" section of docs/consistency-model.md:

| Stage | Guarantee | Worst-case bound (defaults) |
|---|---|---|
| Query exclusion | No query whose snapshot resolves after the request ack returns matching records, from store or any cache tier | immediate; all in-flight queries drained within `max_query_duration` (30 s) |
| Rewrite complete (`.done`) | Every live segment, index entry, and derived dataset is free of matching records | `erasure_rewrite_deadline`, default 72 h; a pending request older than this raises an alarm metric |
| Physical bytes gone from the bucket | Superseded inputs swept | `.done` + `protection_horizon` (default `max_query_duration` + 24 h grace) + one sweep interval — with defaults, under 4 days end to end |
| Physical bytes gone from query-node disk caches | Non-durable local copies aged out | sweep + disk-tier entry max-age (24 h); or immediately, by deleting cache directories |

Modifiers, each documented in the same section rather than silently
absorbed: **+D** if the operator enabled bucket-default Object Lock
retention D (§6); **+E_v** noncurrent-version expiration window if bucket
versioning is on (§7); **paused** under an overlapping legal hold (§6). The
un-modified default bound (< 4 days) sits comfortably inside GDPR's
one-month "without undue delay" window; the modifiers are what an operator
must budget deliberately.

Indexes and derived datasets: catalog snapshot entries for superseded
inputs resolve to NotFound → SnapshotInvalidated → re-resolve (the
existing path), and the next fold rebuilds snapshots over the rewrite
outputs; entries and postings themselves contain no subject values
(Context). Exemplar sections ride inside segments and are rewritten with
them. Idempotency markers store only key hashes and commit-token receipts.
The query-audit keyspace is the one derived store that may hold subject
values (S4-13, matcher values in audited query text); it is deny-deleted
under ADR-0055 and owned by epic EL — see Consequences.

**Correction (2026-08-08), folded into this section by EJ-T2's Stage 4
checkpoint:** "the next fold rebuilds snapshots over the rewrite outputs"
above is true only within `fold_reconcile_window_hours` (default 26h) of
the fold that observes the rewrite -- ADR-0063's incremental fold does not
re-list an already-folded hour outside that window, and a rewrite's inputs
stay physically GET-able (no `NotFound` to force a re-resolve) until the
horizon-gated sweep runs, so there was no other mechanism to fall back on.
EJ-T2 closed the in-window case by adding `RewriteRecord` to the fold
reconcile pass's trigger set (docs/catalog-and-mvcc.md "Fold reconcile
pass"), so a rewrite into a bucket within the window is picked up by the
very next fold, same as a late compaction record. The out-of-window case
remains open: the rewrite pass's own scope (§3.1, "every sealed bucket ...
all buckets when no range is given") is far wider than 26 hours, so a DSAR
against data outside the reconcile window is not automatically re-folded,
and the folded snapshot can keep serving the pre-erasure input until
something else forces a re-fold of that hour.

This is now a **binding requirement on EJ-T4** (the rewrite pass), not a
follow-up nicety: T4's completion verification (§4 above, "re-verified by
a fresh LIST per bucket") must not write `.done` for a bucket based on a
fresh LIST alone, because a fresh LIST is blind to what the FOLDED
snapshot currently serves -- it can correctly observe "this bucket's live
record set is now rewrite-output-only" and still write `.done` while a
stale folded snapshot, outside the reconcile window, keeps resolving the
pre-rewrite input. T4 must derive its "is this bucket's contribution
current" check the same way the resolver and the fold do (through
`resolve_rewrite_supersession` and `classify_bucket`, not a bucket LIST in
isolation), or must force a reconcile of every bucket in a request's scope
(regardless of window) before writing `.done`. Either approach is
acceptable; writing `.done` from a fresh-LIST check alone is not.

**The sibling case makes the same requirement sharper.** Decision 3 point 5
already says overlap harmlessness does not hold for rewrites. Two live
rewrites over one bucket, neither superseding the other, therefore defeat
each other: request A's subject is dropped from rewrite A's output but
still present in rewrite B's, and vice versa. A completion check phrased
as "this bucket's live record set is rewrite-output-only" reads TRUE in
exactly that state, so T4 would write `.done` for request A, §5 would
then delete A's `.dreq` after the horizon, the §2 query-time filter would
stop applying, and A's subject would be served permanently out of rewrite
B's output. T4's per-bucket completion condition for request R is
therefore not "rewrite-output-only" but "**every live (non-superseded)
rewrite record in this bucket names R in its `drops`**"; anything weaker
turns the sibling state into permanent, silent erasure failure. EJ-T2
makes the state observable: `Catalog::rewrite_sibling_conflicts` is
raised by every site that resolves rewrite supersession (snapshot
resolution, the index fold, and the read-your-write token fallback), so a
deployment that ever reaches it alarms rather than serving it as ordinary
overlap.

Also folded into this correction: `resolve_rewrite_supersession`'s
absent-predecessor case (a named `superseded_record_key` no longer present
in the bucket's live listing) currently stops the chase cleanly and
excludes only what it has already discovered up to that point. This is
sound when the predecessor was genuinely swept (its own inputs are gone
too), but is a silent under-exclusion if a sweep-ordering anomaly ever
left the predecessor's inputs live while the predecessor record itself was
removed. T4's completion verification is the intended safety net for this
case too (a live but un-superseded-per-the-chain input fails the "live
record set is rewrite-output-only" check), so T4 must not derive that
check independently of the same exclusion logic the resolver uses, or this
net has a hole matching the shape above.

## Amendment (2026-08-13): completion routes through the catalog resolver, and the `.done` scope is stated to match what the pass verifies

Issue #1000 closes the #997 checkpoint's F1 and F3 findings against the
landed rewrite pass (`services/ravel-server/src/maintain.rs`).

**F1 (completion diverged from the query path).** The pass decided
completion from its own bucket outcomes, which classify each bucket through
ravel-maintain's LOCAL one-hop `resolve_live_record` -- it picks a bucket's
live compaction/rewrite record but never computes which raw L0 inputs a
query still resolves through the full chain. That is precisely the blindness
this §4 correction forbids: a bucket whose live rewrite names the request,
but whose chain fails to exclude an L0 input a snapshot still serves (the
absent-predecessor / partial-input case, or a live sibling rewrite), read
"done" to the pass while the query kept serving the subject. Fixed by
routing completion through the SAME resolver the query runs:
`resolve_rewrite_supersession` (now `pub`) is called from a new
`ravel_maintain::bucket_erasure_completion`, which reconstructs
`Catalog::process_bucket`'s served set on a fresh per-bucket listing and
blocks a request whose subject is still served by a live L0 record,
compaction part, or sibling rewrite. `run_erasure_pass` writes a `.done`
only for a request no in-scope bucket blocks. The pass still resolves its
own rewrite inputs one-hop (the ADR permits that for the rewrite's own
decode); the residual is that a genuinely inconsistent bucket the two
resolvers disagree on is not re-rewritten this tick -- the gate blocks its
`.done` and the request alarms on `erasure_rewrite_deadline` rather than
falsely completing. Blocking is the safe failure.

**F3 (the `.done` scope over-asserted).** §4's table row claimed "every live
segment, index entry, and derived dataset is free of matching records," but
the pass walks only `c/<shard>/<hour>/` commit records. Determination: index
objects and ADR-0028 analytics CANNOT hold a record matching an erasure
subject, so the commit-record pass is sufficient, not under-asserting.
Index objects (`SnapshotEntry`, `SnapshotPartHeader`, name postings) carry
identities, hashes, counts, and metric names -- never label/attribute values
(§7 point 5's requirement). ADR-0028 analytics is a pure query-time stage
(`ravel-analytics` has no clock/IO/object-store/catalog) that runs *after*
the query-time exclusion filter and persists nothing durable, so a derived
result can never surface an erased subject and there is no derived object to
clear. docs/consistency-model.md's `.done` row and "Scope and interactions"
section are narrowed to state this proof explicitly. The two honest
residuals are unchanged and already tracked: the out-of-window folded
snapshot (§4 open item, above) and the query-audit keyspace (epic EL).

### 5. Erasing the erasure request itself

The `.dreq` contains the subject identifier, so it must not outlive its
purpose. A new sweep rule deletes `.dreq` when **all** hold: its `.done`
exists; `now >= done.created_unix_ns + protection_horizon`; and the
`LegalHoldCheck` passes. The horizon wait guarantees no resolvable snapshot
can still include a pre-rewrite input by the time the query-time filter
disappears, closing the §3.5 race window durably. The `.done` record (hash
only, no PII) is permanent.

### 6. Interaction with ADR-0055 (WORM / credential scoping / legal hold) — the landed-second obligation

Stated explicitly, per issue #460's note and ADR-0055's own consequence:

- **Erasure runs under the Maintain role. No fifth role.** The rewrite
  writes `l1/**` parts and `c/**` records and the sweep deletes `l0/`,
  `l1/`, `c/` objects — all inside grants Maintain already holds. ADR-0055
  predicted this placement; this ADR confirms it.
- **New prefix grants (ADR-0055 §1 table amendment, landed with this
  epic's first implementing commit):** Admin gains `CreateIfAbsent` write
  on `t/*/*/del/**` (submit) — Admin still deletes nothing, anywhere.
  Query and Maintain gain read on `del/**` (resolve-time listing; pass
  scoping). Maintain gains delete on `del/*.dreq` **only** (§5).
  `del/*.done` joins the deny-delete set for every role including
  Maintain: completion records are permanent erasure evidence, and they
  can be permanent precisely because they carry no subject identifier.
- **Erasure never touches ADR-0055's deny-delete prefixes.** `sys/*`,
  `prov`, `catalog/*`, and `u/**` hold no subject attribute values
  (Context; §7 requirement), so the WORM boundary and subject erasure are
  disjoint by construction — except the audit keyspace, which is EL's to
  fix (Consequences).
- **Legal hold wins over erasure, and the precedence is visible.** The
  rewrite pass and the superseded-input sweep both consult
  `LegalHoldCheck`; a bucket under an overlapping hold is skipped, the
  request stays pending, and its status (via `ravel-cli erase status` and
  the eventual `.done`) records `deferred: legal hold <scope>`. Query-time
  exclusion (§2) stays active throughout — a hold preserves evidence
  against destruction; it does not oblige Ravel to keep serving the data
  in query results. Erasure never clears a hold: clearing is the existing
  Admin-only, separately-audited `ravel-cli` legal-hold operation
  (ADR-0042/ADR-0055), a deliberate human act. When the hold clears, the
  next pass completes the erasure with no re-submission. The erasure-
  latency clock for held ranges is explicitly paused, and the deletion-
  guarantees doc says so — a deployment whose legal obligations can
  conflict (a litigation hold against an erasure demand) gets a truthful
  mechanical answer: the hold wins until an authorized human clears it.
- **Bucket-level Object Lock**: if the operator enabled compliance-mode
  default retention D (the out-of-band step ADR-0042 documents), S3 itself
  refuses the sweep's deletes until each object's retain-until passes; the
  physical bound becomes `max(bound, D)` and §7's required-configuration
  section instructs operators with erasure obligations to prefer scoped
  legal holds over blanket default retention, or keep D inside their
  erasure SLA. Ravel's code needs no change for this: sweep deletes
  already treat per-object failures as retryable residue and the
  tombstone/record-last ordering already tolerates partial passes.

### 7. Required bucket configuration (S2-16, S4-12)

A new normative "Required bucket configuration" section in
docs/object-store-contract.md (summarized in docs/guides/operations.md and
kubernetes.md), replacing the review's unstated assumption with a stated
contract:

1. **Object versioning: OFF unless deliberately paired.** On an
   unversioned bucket, Ravel's deletes are physical; every bound in §4
   holds as stated. If versioning is enabled (an operator DR choice), the
   operator MUST configure noncurrent-version expiration with
   `NoncurrentDays = E_v` plus expired-delete-marker cleanup on all `t/`
   prefixes, and every physical-erasure and retention bound gains +E_v.
   Versioning without that rule silently inverts every deletion guarantee
   in the system (S4-12) and is an unsupported configuration.
2. **No lifecycle expiration or transition-to-archival rules on any Ravel
   prefix** (`t/**`, `sys/**`). A lifecycle expiration deletes data out
   from under live commit records (S2-16: "mass permanent loss");
   transitions to non-instant-retrieval classes break reads. Ravel's own
   retention (ADR-0019) is the only sanctioned age-out mechanism.
   Instant-access storage-class transitions (e.g. to IA) are permitted.
3. **Exactly two sanctioned lifecycle rules**:
   `AbortIncompleteMultipartUpload` (recommended, 7 days — this also
   converts S5-19's undocumented dependency into a documented one), and
   the noncurrent-version expiration of point 1 when versioning is on.
4. **Object Lock**: bucket-default retention is supported but extends the
   erasure bound per §6; scoped legal holds (ADR-0042) are the recommended
   compliance mechanism when erasure obligations coexist.
5. **Subject identifiers live in label/attribute values, not metric
   names.** Name postings under the deny-deleted `catalog/` prefix retain
   metric names; a deployment that embeds subject IDs in metric names is
   outside the erasure guarantee, and the doc says so.

Enforcement teeth, matching what the platform can actually see:
`ravel-cli store qualify`'s existing informational probe (ADR-0055 §3) is
extended to report bucket versioning state and the presence/absence of the
sanctioned lifecycle rules where the backend exposes them, recorded
alongside `sys/qualification`; `ravel-cli verify-custody` gains a
versioning-aware mode that, on a versioned bucket, also lists noncurrent
versions under swept keys and reports "deleted but recoverable as prior
version" as a distinct anomaly class — closing S4-12's "invisible to
verify-custody" clause. Both remain informational-plus-alarming rather
than startup-blocking, consistent with ADR-0042/0055's honest-gap framing:
`object_store` cannot enforce bucket policy, so Ravel reports what it can
observe and documents what it requires.

## Rejected alternatives

**Query-time exclusion only (durable predicate tombstones, no rewrite).**
Rejected as the end state — it is, verbatim, the posture the review's
section 11 analysis already condemned: "query exclusion plus whole-object
deletion, not erasure." Bytes remain in the bucket, in backups, and in
caches indefinitely; a credential holder or a versioned-bucket restore
recovers them. It survives in this design only as the bounded bridge (§2)
between request and rewrite, which is what makes the acceptance
criterion's "a subsequent query cannot return them from a cache" hold
immediately rather than at rewrite completion.

**Crypto-shredding (per-subject or per-record keys; erasure = key
destruction).** Rejected for subject granularity. (a) Subjects are not
known at write time: an erasure request may name any attribute value, so
per-subject encryption would require partitioning every segment by every
potential future subject — but segments are deliberately columnar,
multi-series, content-addressed objects; per-subject partitioning explodes
object count and destroys the 8 MiB-object economics the whole store is
built on. (b) Per-record keys put a key lookup on every read path and make
query latency a KMS function. (c) Key destruction moves the erasure
guarantee's root of trust outside the bucket into a KMS, while "object
storage is the source of truth" is the repo's first invariant, and the
review already flags out-of-bucket durability dependencies as their own
risk class (S1-08). (d) Ravel does not even have per-*tenant* KMS yet
(S4-04, OPEN, epic EL #462). Tenant-granularity crypto-erasure via
per-tenant KMS revocation is EL's natural complement to this ADR — it
covers offboarding and backup-copy erasure, which rewrite cannot reach —
and this ADR deliberately leaves that layer to EL rather than half-building
it here. Flagged in Consequences.

**Synchronous rewrite-on-request (erase inline in the request path).**
Rejected. (a) Cost is unbounded and front-loaded: a subject spread across
every hour bucket means a full tenant scan-decode-reencode inside one
request. (b) It cannot actually deliver "immediately erased": superseded
inputs still must outlive `protection_horizon` for pinned queries, so the
physical bound barely moves. (c) It would put segment writes and deletes
in whatever process serves the request — under ADR-0055 that process's
credential would need write+delete grants that deliberately exist only in
Maintain; a synchronous path would reopen the credential boundary EE just
closed. (d) Visibility, the thing that genuinely must be immediate, is
already immediate via §2 without any of this cost.

**Applying erasure predicates during regular compaction instead of a
dedicated pass.** Rejected: compaction is a verbatim page copy with an
exact conservation gate (ADR-0048) and runs once per bucket — buckets
already compacted (the common case for a subject's history) would never be
revisited, so erasure latency would be unbounded for exactly the data most
likely to be covered. A dedicated `RewriteRecord` also keeps "this record
deliberately dropped N records for request R" auditable and
conservation-checkable, instead of overloading compaction's "outputs equal
inputs exactly" invariant with a second meaning.

**Regex or non-equality predicates in v1.** Rejected: erasure is
irreversible, so over-matching is unrecoverable; exact-match conjunctions
are auditable, cheap to evaluate at scan time, and match the shape of real
DSAR requests (a known identifier). Widening the predicate language is
additive later.

**A tenant-facing HTTP erasure endpoint in v1.** Rejected for now: a
tenant bearer token that can irreversibly destroy data is a new
threat-model surface (the review already documents forged-request and
audit-gap concerns on the existing surfaces), and DSAR fulfilment is an
operator workflow in practice. The CLI/Admin path establishes the
mechanism; an authenticated API can be layered on once EL's audit
hardening lands. Additive later.

**Deny-delete `del/` entirely and keep requests forever.** Rejected: the
request record contains the subject identifier — retaining it forever is
itself a GDPR violation (the same trap as S4-13's audit PII). Split
records (`.dreq` deletable after completion + horizon, `.done` permanent
and PII-free) get permanent audit evidence without permanent PII.

## Consequences

- **New additive frozen-contract surface, via this ADR as the required
  ADR**: two protobuf messages (`ErasureRequest`, `RewriteRecord`) in
  proto/ravel/commit.proto, and three additive key shapes (`del/*.dreq`,
  `del/*.done`, `c/.../rw.<hash>.cmt`) in docs/catalog-and-mvcc.md.
  Existing keys, messages, and segment formats are untouched; rewritten
  segments are new instances of existing frozen versions. No version
  bumps.
- **docs/consistency-model.md gains a normative "Deletion guarantees"
  section** (§4's table plus modifiers) and its Deletion-and-GC table
  gains two rules (rewrite supersession; `.dreq` removal).
  **docs/object-store-contract.md gains "Required bucket configuration"**
  (§7). Operations and kubernetes guides updated in the same commits as
  the behavior.
- **ADR-0055's role table is amended** (Admin write on `del/**`;
  Query/Maintain read; Maintain delete on `.dreq` only; `.done`
  deny-deleted) with matching IAM policy JSON in the operations guide,
  following ADR-0055's own in-place amendment precedent.
- **Maintain gains a third destructive responsibility** after retention
  and sweep. It stays inside the existing single per-tenant loop;
  per-bucket exclusivity between compaction and rewrite becomes a stated
  requirement on epic EI's (#459) leased-maintenance design.
- **Rewrite is the first machinery that decodes and re-encodes segments
  with intentional record drops.** The adapted conservation gate (outputs
  + dropped = inputs) is the guard rail; property tests must cover all
  three signal codecs' filter-and-reencode paths, and corrupt-input tests
  must produce typed errors, never partial rewrites.
- **Cost**: erasure of a widely-spread subject re-reads and re-writes
  every covered bucket once per request batch. The pass batches all
  pending requests per bucket into one rewrite (one `RewriteRecord` can
  apply many request_ids), so N concurrent DSARs cost one rewrite, not N.
  Storage temporarily doubles for covered buckets until the horizon-gated
  sweep clears inputs — same transient shape as compaction.
- **Interaction with EL (#462), flagged as required by this epic**: (a)
  the query-audit keyspace persists matcher values (S4-13) under a
  deny-deleted prefix, so audited erasure-adjacent queries can retain
  subject identifiers outside this ADR's reach — EL's PII
  policy (hash/tokenize matcher values) is what closes that, and this
  ADR's guarantee statement must name the audit keyspace as excluded
  until EL lands; (b) per-tenant KMS revocation (EL) is the
  tenant-granularity, backup-reaching crypto-erasure complement to this
  ADR's subject-granularity physical erasure — the deletion-guarantees
  doc will present them as the two layers they are.
- **Interaction with ADR-0058/0059 (DR posture)**: replicas or external
  backups of the bucket are outside Ravel's deletion reach by definition;
  the deletion-guarantees section states that erasure applies to the
  primary bucket, and operators with replicated buckets must apply the
  same lifecycle discipline (§7) to replicas — an honest scope statement,
  not a new mechanism.
- **What this ADR does not do**: no tenant-facing API (v1), no regex
  predicates, no crypto-shredding, no per-tenant KMS, no change to
  retention semantics, no fifth credential role.
- **Sequencing with EI (#459)**: EI's own ADR-0065 recommends EJ land
  after EI, because EJ's rewrite pass breaks EI's "interior zone is inert"
  scheduling assumption and needs its `invalidate` hook. This ADR's
  rewrite pass should call that hook once EI lands; if EJ's implementation
  starts first, it must not assume the hook exists and should coordinate
  with whoever is landing EI at the time.

## Stage-2-ready task decomposition (sketch, for the approval gate)

| ID | Title | Crates | Deps | Risk |
|---|---|---|---|---|
| T1 | `ErasureRequest`/`RewriteRecord` protos, codecs, key-layout doc, property tests | ravel-commit, proto/ | — | high (frozen-contract additive; format-change skill) |
| T2 | Resolver: `del/` listing, `RewriteRecord` supersession in snapshot resolution, predicate attach | ravel-catalog | T1 | high (visibility correctness) |
| T3 | Scan-time predicate filters, all three signals, post-cache | ravel-query, ravel-server fetchers | T1 | medium |
| T4 | Rewrite pass: decode-filter-reencode (RSEG/RLOG/RSPAN), conservation gate, publish, completion verify, legal-hold gate | ravel-maintain, ravel-segment/logseg codec use | T1, T2 | high (rides solo in its wave) |
| T5 | Sweep additions: rewrite-superseded inputs rule, `.dreq` removal rule; disk-cache entry max-age; RAM invalidation trigger | ravel-maintain, ravel-cache | T4 | high |
| T6 | `ravel-cli erase submit/status`; qualify probe + verify-custody versioning mode | ravel-cli, ravel-object-store (conformance) | T1 | medium |
| T7 | Normative docs: deletion guarantees, required bucket configuration, ADR-0055 table amendment + IAM JSON, guides | docs only | T1–T6 shapes | low |
| T8 | End-to-end reachability test: ingest all three signals → `erase submit` → immediate query exclusion (incl. warm cache node) → maintain pass → `.done` → physical absence via MemoryStore listing + FaultStore failure paths | ravel-server e2e | T2–T6 | high |

Wave shape: T1 solo (high-risk frozen-contract wave) → {T2, T3, T6} (zero
file overlap, distinct crates) → T4 solo → T5 solo → {T7, T8}. T8 is the
epic's end-to-end reachability acceptance, driven through the real server
entry points per the deliver-epic Stage 2 rule.
