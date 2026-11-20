# Delegated owner decisions for the 2026-11-20 items

Input: antithesis/owner-items-2026-11-20.md (session antithesis-gcp). Every decision
below is recorded as "Owner decision (delegated to Fable)". The human may override any
of them. Each section checks the brief's factual claims against the sources in this
checkout (main at b9303ba, the commit that added the brief) before relying on them, and
says where a claim does not hold.

Sections:

1. #4503 fix direction, ADR-0064 attribute scope, two review findings
2. #4503 remediation and communication
3. #4501 legal hold mid-pass: contract 1 or 2
4. #4428 swept-token fallback beside a stale HEAD
5. #4449 claims-on residual window: known-issue text, ADR-1029 amendment, fix direction
6. Order and priority for the next week
7. Lines for the human to type

## 1. #4503: fix direction and ADR-0064 attribute scope

### Claims checked against main

- `LogErasureMatcher::matches_attrs` (crates/ravel-maintain/src/erasure_rewrite.rs)
  tests a record's own `attrs` only, and `build_rewrite_logs` filters with
  `first_dropping_log_request(requests, &record.attrs, record.ts_ns)`. Holds. Its
  doc comment still says this matches the query path. That is stale.
- `ravel_query::erasure::retain_unerased_log_records` matches the merged
  resource + scope + record view (`merged_log_attrs`, record value wins on a key
  collision) and calls itself "the authoritative selective-erasure exclusion".
  `is_erased_log_row` tests the record view or the merged view, each as the first
  string value under a key. Holds.
- The rewrite's record-view match uses `any` over repeated keys; the query path
  uses the first value. The narrowing the PR describes is real: today's rewrite
  can drop a record queries still serve. That is the wrong direction for an
  irreversible delete and the fix removes it.
- `erasure_rewrite_bucket` returns `AlreadyApplied` when the bucket's live
  rewrite names every overlapping request. Holds, so a pending request whose
  buckets an older build rewrote with zero drops is never re-run by the fixed
  build on its own.
- ADR-0064 decision 1 says "logs match row attributes" and nothing about
  resource or scope attributes. `LifecycleGC.tla` `DreqRemovalCannotResurrect`
  and docs/consistency-model.md "Selective subject erasure" both promise the
  subject stays unreturnable after the `.dreq` is gone. Holds.
- Spans: the maintain matcher's doc says `ravel_rspan::merge_attrs` folds
  resource and scope into the span's attrs at ingest. Not re-verified in
  ravel-rspan here; the query and rewrite paths both read that one field, so
  parity does not depend on it.

### Decision

Approve the fix direction: the logs rewrite drops exactly the records the query
path hides, by applying `is_erased_log_row`'s rule (record view or merged
resource + scope + record view, first string value per key, window on `ts_ns`).
ADR-0064 gets a dated amendment stating the attribute scope per signal; land it
in the same pull request as the fix.

### Why

The contract is ADR-0064 decision 2 plus `DreqRemovalCannotResurrect`: nothing a
query hid while the request was pending may come back when the `.dreq` is swept.
On main that is false for every logs subject named only in a resource or scope
attribute, and `.done` is written with `dropped_count: 0`, so the operator has
evidence of completion for an erasure that did not happen. The two ways to
restore parity are to narrow the query filter to record attributes, or to widen
the rewrite to the merged view. Narrowing would re-expose records users already
saw hidden, including under pending requests, and would make a `user_id` carried
as a resource attribute (the common OTLP shape) unerasable. Widening drops only
records no query has returned since the request was acknowledged, so a reader
cannot observe the widening; it is still an irreversible delete over a wider set
than the ADR text names, which is why the amendment is required rather than
optional. The parity proptest in the PR is the right shape of proof: the
rewrite matcher and `is_erased_log_row` agree over generated attributes, and the
test was shown failing with the merged view off.

### Conditions and follow-ups

1. The amendment below lands in the same pull request as the matcher change, so
   the ADR never says "row attributes" while the code deletes on the merged
   view.
2. Review finding one (docs/deletion-and-gc.md states the stays-erased
   guarantee unconditionally): the fix must state the scope of the guarantee,
   not just add an exception sentence. Required wording, in substance: the
   guarantee holds for a request whose rewrite of each in-scope bucket ran on a
   build with this fix; a bucket an earlier build rewrote for the request is
   skipped as already applied and keeps the records that build did not match; the
   remedy is a new request with a new id for the same subject, whose rewrite
   drops them. The same sentence goes in the changelog fragment, and
   `ravel-cli erase status`'s per-bucket `dropped_count` of 0 on a logs request
   is named as the operator's signal that a bucket may need it.
3. Review finding two (stream-view cache keyed by the whole `stream_attrs` blob,
   uncharged): the cache must be keyed by `LogRecord::stream_id` (16 bytes,
   already on every record and derived from the blob), not by the blob bytes,
   and it must be bounded and charged. Cap it in bytes against the rewrite's
   existing memory accounting (the merge's decoded-heap estimate is the natural
   home); at the cap, decode per record rather than grow, and never fail the
   rewrite for cache pressure. One decode per stream per rewrite is the target,
   not a requirement.
4. The stale doc comment on `LogErasureMatcher` is replaced by one naming the
   merged view and `is_erased_log_row` as the rule it mirrors.
5. Gates: the PR's test step did not complete locally (three flaky runs). The
   merge runs `scripts/fleet-result-merge.sh` with its gates; nothing here waives
   that.

### Confidence

High on the direction and on the amendment. Medium on finding two's exact cap,
which the implementer sizes from the merge's existing targets.

### Draft amendment text for ADR-0064

Append at the end of docs/adrs/0064-selective-subject-erasure.md. The section
heading "1. The erasure request: a durable, immutable predicate record" exists
once; its prose must gain an inline pointer, so in decision 1 replace
"(`key = value` pairs; metrics match series labels, logs match row attributes,
spans match span attributes)" with "(`key = value` pairs; which attributes each
signal's matcher tests is stated by the attribute scope amendment below)".

```markdown
## Amendment (2026-11-20, #4503): the attribute scope of a matcher, per signal

<!-- amendment-applies: sections="1. The erasure request: a durable, immutable predicate record" pointer="attribute scope amendment" -->
<!-- amendment-supersedes: phrase="logs match row attributes" pointer="attribute scope amendment" -->

Decision 1 said logs match row attributes and did not say whether a subject
named only in a resource or scope attribute is in scope. The query-time
exclusion has matched the merged resource + scope + record view since the
change that made `retain_unerased_log_records` the authoritative exclusion,
while the rewrite matched record attributes only. A logs request naming a
resource or scope attribute was therefore hidden while pending, completed
with zero drops, and served again once its `.dreq` was swept, against
decision 2 and `DreqRemovalCannotResurrect` (#4503).

The scope is now stated, and it is the same for the query-time exclusion and
the rewrite, which is the invariant this amendment adds: the rewrite drops
exactly the records the exclusion hides, never more and never fewer.

- Metrics: the series labels. A windowless request drops every sample of a
  matching series; a windowed one drops the in-window samples.
- Logs: the record's own attributes, or its merged view, which is the
  stream's scalar resource and scope attributes overlaid with the record's
  own, the record's value winning on a key collision. A matcher reads the
  first string value under its key in each view; a non-string value never
  matches. The record is dropped when the request's window holds its
  `ts_ns` and the conjunction matches either view.
- Spans: the span's merged attributes, into which ingest already folds the
  resource and scope attributes.

A bucket that an earlier build rewrote for a request is skipped as already
applied by every later pass, so the records that build did not match stay in
its parts. A new request with a new id for the same subject rewrites that
bucket again. docs/deletion-and-gc.md states which attributes each signal's
matcher tests and the already-rewritten exception.
```

## 2. #4503: remediation and communication

### Claims checked against main

- `ravel-cli erase` has `submit` and `status` only (services/ravel-cli/src/main.rs).
  `status` prints per-bucket `dropped_count` from the `.done`.
- The `.done` carries a hash of the predicate, never the matchers (ADR-0064
  decision 1). The `.dreq`, which carries them, is swept after `.done` plus the
  protection horizon. So once a request is swept, no object in the store holds
  its predicate.

### Decision

Changelog plus a published advisory, no new command. The remedy is one existing
command, `ravel-cli erase submit` with the same matchers and a new request id,
and the advisory tells operators when they must run it.

### Why

A `resubmit` command would need the original predicate, and after the sweep the
store no longer has it; it would take the matchers as arguments, which is
`erase submit`. A command that lists "logs requests completed with zero drops"
cannot tell a resource-scoped subject from a subject that had no records, so it
would be a false signal either way. Tool work here delays the fix that closes the
exposure, and the exposure closes at the moment the new request is acknowledged:
query-time exclusion applies from the ack, before any rewrite runs (ADR-0064
decision 2). That makes the instruction short and safe: submit first, the
rewrite follows.

### Conditions and follow-ups

1. The changelog fragment `changelog.d/4503.fixed.md` carries the bullet below
   (or its substance), and the same text goes in the 0.35.0 release notes.
2. The advisory is published against 0.33.0 and 0.34.0 as a GitHub release
   note addendum and a pinned issue. Before publishing, confirm the first
   affected release from the history of the query-path change (the brief names
   1f33d5eeb); if an earlier release carries it, name that release too.
3. The advisory is outward facing. The human posts it (see section 7).
4. A request that is still pending at upgrade and whose buckets were already
   rewritten with zero drops needs the same re-submit: `AlreadyApplied` skips
   those buckets for the old id. The advisory says so.

### Confidence

High.

### Draft changelog bullet

```markdown
- **A logs erasure now drops records whose subject is named in a resource or
  scope attribute.** The rewrite matched a record's own attributes only, while
  queries hid the merged resource, scope and record view, so such a request was
  hidden while pending, completed with zero drops and served again once its
  request record was swept (#4503). The rewrite now drops exactly what queries
  hide. A bucket an earlier version already rewrote for a request is skipped as
  already applied, so a logs request submitted on 0.34.0 or earlier for a
  subject named in a resource or scope attribute must be submitted again with a
  new request id; the new request hides the records from the ack onward and its
  rewrite drops them. `ravel-cli erase status` shows such a request completed
  with `dropped_count=0` on every bucket.
```

### Draft advisory (plain language, for the 0.33.0 and 0.34.0 release pages)

```markdown
## Known issue in 0.33.0 and 0.34.0: logs erasure by resource or scope attribute does not erase

Who is affected: anyone who submitted a logs erasure request (`ravel-cli erase
submit --signal logs`) whose matcher names an attribute that arrives on the
resource or the instrumentation scope rather than on the log record itself.
`user_id` or `host.name` set on the OTLP resource is the usual case. Metrics
and spans requests are not affected. A logs request whose matcher names a
record attribute is not affected.

What happens: the request hides the matching records from every query as soon
as it is acknowledged, as designed. The rewrite that should remove them from
storage matches record attributes only, so it removes nothing and reports the
request completed with `dropped_count=0` on every bucket. About four and a half
days after completion, with default settings, the request record is swept and
the records are served again.

What to do now, on any version: submit the same request again with a new
request id. From the moment the new request is acknowledged the records are
hidden again. On 0.33.0 and 0.34.0 the new request will also complete with zero
drops and the records will return again after its sweep, so repeat this until
you have upgraded.

What to do after upgrading to the release that carries the fix for #4503: submit
the same request once more with a new request id. The fixed rewrite removes the
records from storage and the request completes with the real dropped counts.
This last submission is required even if a request is still pending at the time
of the upgrade: a bucket an earlier version already rewrote for a request is
skipped for that request id.

How to find affected requests: run `ravel-cli erase status --tenant <tenant>
--signal logs` and look for completed requests whose every bucket reports
`dropped_count=0`. Compare those against the attribute the request named.
```

## 3. #4501: legal hold written mid-pass

### Claims checked against main

- ADR-0042 decision 2: `LegalHoldCheck` "consults a per-tenant (or
  per-time-range) hold record before any physical delete in both the sweeper
  and the retention path; a hold present means the delete is skipped". Holds.
- crates/ravel-maintain/src/legal_hold.rs: `refresh` loads the hold set once;
  `is_protected` reads only the snapshot and does no I/O. Holds.
  services/ravel-server/src/maintain.rs refreshes once per tenant per tick and
  threads that snapshot through retention, every sweep rule, the erasure rewrite
  and the alert and audit retention passes. Holds.
- `LifecycleGC.tla`: `PlaceHold` changes `heldBuckets` at once, `RetentionSweep`
  and `SupersededSweep` guard on `~HeldObject(o, heldBuckets)`, and
  `HeldObjectNeverDeleted` reads the true hold set the delete observed at its
  own step. Holds. The model has no notion of a per-pass snapshot.
- The brief says the #4337 work puts `HeldObjectNeverDeleted` at three Rust
  sites and that `a_hold_placed_after_the_snapshot_trips_held_object_never_deleted`
  is in sweep.rs. Neither is on main at b9303ba: sweep.rs asserts the HEAD-gate
  and unreadable-HEAD invariants only, and no symbol or string of that name
  exists in crates/ravel-maintain. Both are on an unmerged branch. The decision
  below does not depend on them, but the #4337 merge must follow it.
- Not in the brief: the operator guides already state contract 2.
  docs/guides/operations/maintenance.md "Legal hold" says the hold "is not
  effective the instant the command returns", that a hold set after a tick's
  refresh "is not honored until the next one", and that the exposure window is
  one maintain interval, five minutes by default; troubleshooting.md "Data
  deleted despite hold" says the same. That window figure is wrong under
  either contract: the exposure is the remainder of the running pass, which on
  a large tenant is longer than the interval. So the repository already
  contradicts ADR-0042 and the model in its operator docs, independent of this
  decision.

### Decision

Contract 1: the hold set is re-read from the store immediately before each
delete batch, and a batch whose re-read fails deletes nothing. The guarantee
sentence, for ADR-0042, docs/deletion-and-gc.md and the `hold set` output:

> A legal hold whose set record was durable before a delete batch re-read the
> hold set is honored by that batch; every delete batch re-reads the hold set
> immediately before its first delete, and a batch that cannot read it deletes
> nothing.

The residual is the batch's own in-flight deletes: a hold written after the
re-read and before the batch's delete requests return is not honored by that
batch. That residual is stated, not hidden.

### Why

ADR-0042 is a compliance contract and its words are "before any physical
delete". A litigation hold whose effect waits for a running pass to end has no
bound an operator can state: a tick's destructive passes on a large tenant run
for as long as the sweep takes, and the guide's "one maintain interval" is the
scheduling gap, not the pass length. Contract 2 would therefore need a new
bound that does not exist today, and it would narrow the one property the
Antithesis lifecycle campaign already checks. Contract 1 matches the ADR, the
model and the Rust assertion the #4337 branch adds, and its cost is small and
proportional to deletes rather than to data: one LIST of the hold shard per
delete batch, with the GET of each commit record and audit object cacheable
within the process by key, since hold records are immutable. Deletes are rare
relative to planning work, and a tick that deletes nothing pays nothing new.
Fencing hold writes against running passes was the other contract-1 shape; it
adds a mutable coordination object to a path that today has none, and the
re-read gives the same guarantee without it.

### Conditions and follow-ups

1. Minimal change set, in one pull request on the #4505 branch or beside it:
   - ravel-maintain gains a hold source the delete sites call (an async
     `current()` returning a fresh `LegalHoldCheck`, over the store the pass
     already has), used at the superseded-input delete per chain group, the
     retention `delete_all` per bucket, the orphan quarantine move and purge,
     the `.dreq` sweep, the catalog sweep and the alert and audit retention
     deletes. Planning keeps the per-tick snapshot; only the delete decision
     re-reads.
   - A failed re-read skips that batch, counts on the existing refresh-failure
     counter, and the pass continues to the next batch (fail closed per batch,
     not per tick, so one transient fault does not stop every delete).
   - The commit-record and audit-object reads inside `load_hold_records` are
     cached per key for the process lifetime; the LIST is not.
   - ADR-0042 gets a dated amendment carrying the guarantee sentence and the
     residual (draft below). `LifecycleGC.tla` is unchanged.
   - maintenance.md "Legal hold" and troubleshooting.md "Data deleted despite
     hold" are rewritten to the sentence above; the five-minute figure goes.
     `hold set` prints "hold set: <scope>; honored by every delete batch that
     re-reads holds after now" or equivalent.
   - The #4505 test loses its `#[ignore]` and asserts `superseded_records_deleted`
     and `superseded_data_deleted` of 0 with `superseded_groups_held_by_legal_hold`
     of 1 for the group, so it pins the claim in the commit's own words.
2. #4337's Rust `HeldObjectNeverDeleted` sites judge from a hold set re-read at
   the delete, which is what this contract makes true; merge #4337 after this
   change, or rebase it onto it.
3. Until this lands, an Antithesis `HeldObjectNeverDeleted` failure is triaged
   against this window first, as the issue already says.

### Confidence

High on the contract. Medium on the list of delete sites being complete; the
implementer greps every `is_protected` call under a delete and treats a site
missing from the list above as in scope.

### Draft amendment text for ADR-0042

The heading "Decision" exists once in the file; decision 2's prose gains the
pointer "(the re-read before each delete batch is stated by the delete-batch
re-read amendment below)" after "a hold present means the delete is skipped".

```markdown
## Amendment (2026-11-20, #4501): the hold set is re-read before each delete batch

<!-- amendment-applies: sections="Decision" pointer="delete-batch re-read amendment" -->

Decision 2 says the hold record is consulted before any physical delete. The
shipped `LegalHoldCheck` loaded the hold set once per tenant per maintenance
tick and judged every delete of that tick from that snapshot, so a hold whose
record landed after the snapshot was not honored until the next tick, for as
long as the running pass took. ADR-0042, the lifecycle model's
`HeldObjectNeverDeleted` and the operator guides disagreed about this; the
guides said next tick, the ADR and the model said before the delete.

The ADR and the model stand. Every delete batch (a superseded chain group, a
retention bucket, an orphan quarantine move or purge, an erasure request
record, a catalog object batch, an alert or audit retention batch) re-reads
the hold set from the store immediately before its first delete and judges
its deletes from that read; planning decisions earlier in the pass keep the
per-tick snapshot. A batch whose re-read fails deletes nothing, is counted on
the refresh-failure counter, and the pass continues. The guarantee is: a legal
hold whose set record was durable before a delete batch re-read the hold set
is honored by that batch. The residual is the batch's own in-flight deletes:
a hold written after the re-read and before the batch's delete requests
return is not honored by that batch, and nothing short of a store-side
transaction could make it so. The cost is one LIST of the hold shard per
delete batch; hold commit records and audit objects are immutable and cached
by key within the process. `ravel-cli hold set` states the guarantee in its
output, and docs/guides/operations/maintenance.md "Legal hold" and
docs/deletion-and-gc.md carry the normative wording.
```

## 4. #4428: swept-token fallback beside a stale HEAD

### Claims checked against main

- `resolve_min_token_fallback` (crates/ravel-catalog/src/catalog.rs) serves a
  live rewrite's parts for a swept token through `live_rewrite_naming` and
  never calls `set_holds_a_replaced_object`. Holds. The present-record path
  does call it (the #4418 fix), and on a hit it adds the token's own input
  instead of the rewrite's parts. Holds.
- docs/deletion-and-gc.md "HEAD-referenced snapshot delete blocker": the
  superseded-input sweep asks the HEAD gate "over the entire chain at once,
  down to the raw inputs the oldest generation superseded: a HEAD naming
  anything in the chain holds all of it", and the chain group is "one
  indivisible delete unit". Holds. So while HEAD names M, raw or through a
  compaction part in R's chain, the group entered from R holds P too, and P's
  commit record is not deleted by that sweep.
- The other deleters cannot produce the state either: retention removes P only
  under a tombstone, and a tombstoned bucket resolves to zero segments on every
  path; orphan GC removes record-less objects, and P has a record; the catalog
  sweep removes catalog objects only.
- A HEAD that names M but not P: the fold lists an hour only once it is
  sealed, and a commit of a sealed hour landed before the seal, so a fold that
  names M lists P too. The one exception is an hour held open by an
  operator-asserted seal (ADR-2677), which every fold re-lists until the margin
  passes, so the gap between a fold naming M and one naming P is one fold
  interval, far inside the protection horizon the sweep waits on R.
- A pinned reader on an older HEAD naming M while the current HEAD names
  neither: the unnamed-since marker gate (ADR-1133) delays the delete past
  every validated query deadline. The readers outside that gate (`ravel-cli
  export`, the bench harness) are already documented as outside it in
  docs/consistency-model.md.
- The pmoust correction of 2026-11-19 ("the fold that names M also names P,
  and the superseded-input sweep holds an object HEAD names") is right, and
  the chain-group rule makes it stronger than stated: the sweep holds P because
  it holds the whole group, not only because HEAD names P.
- docs/deletion-and-gc.md line 651 says without condition that a token whose
  bucket is tombstoned "resolves as satisfied with zero segments". The brief's
  item 2 reads that as the present-record case adding an L0 input in a
  tombstoned bucket; `resolve_min_token` was not re-read line by line here, and
  docs/catalog-and-mvcc.md step 5 is the normative text the brief says is
  already right. The sentence should defer to it.

### Decision

Close item 1 as unreachable under the shipped sweep and resolver, with the
reasoning above recorded on the issue, and do not change the resolver now.
Land two small things instead: an invariant at the swept fallback that fires
if the state is ever reached, and the doc sentence fix. The fix of record, if
the invariant ever fires, is option two: serve the rewrite's parts and remove
from the set exactly the objects the rewrite replaced when every one of them
is fully covered by its effective inputs, and return a typed refusal otherwise.

### Why

The brief's own test builds the state by deleting P directly; no sweep path
reaches it, and the chain-group HEAD gate is the reason, which is a stronger
argument than "not found yet". A resolver change with no reachable trigger is
dead weight on the token path and a review and gate cost before 0.35 for no
observable behaviour. An `inv_always` at the fallback ("a swept-token fallback
never adds a rewrite's parts beside an object the rewrite replaced"), judged
with `set_holds_a_replaced_object` and compiled out of the default build, turns
the question into something the Antithesis lifecycle profile (T9, #4338) can
answer by reading with old tokens across erasure, rewrite and sweep, which is
the campaign's purpose. Typed refusal as the fix of record was rejected because
a stale HEAD is a steady state between folds and the refusal would re-resolve
into the same answer; removal of fully covered replaced objects is exact,
because a rewrite's parts carry every row of its effective inputs minus the
erased ones, and the erasure filter still applies while the `.dreq` is pending.

### Conditions and follow-ups

1. The issue gets the reasoning above and item 1 is closed as unreachable, with
   the invariant and the fix of record named so a later executor does not
   re-decide.
2. PR #4504's test is not merged in its current shape. It stays on its branch
   as reference material; the issue comment points at it. If the invariant
   fires, the test's "serves every surviving row once" arm becomes the fix's
   test.
3. The doc sentence at docs/deletion-and-gc.md line 651 is changed to state
   the rule by reference: a token whose bucket is tombstoned resolves as
   docs/catalog-and-mvcc.md step 5 describes, satisfied without the bucket's
   data, never as `unsatisfiable token`. Same pull request as the invariant.
4. `check_rewrite_siblings` already alarms on sibling rewrites; the new
   invariant uses the same details shape (tenant, signal, shard, hour, token).

### Confidence

Medium-high on unreachability: it rests on the chain-group rule and the
fold-after-seal rule, both normative and both implemented, and on the ADR-1133
window for pinned readers. Medium on "not worth a fix now"; the invariant is the
hedge.

## 5. #4449: claims-on residual window

### Claims checked against main

- crates/ravel-maintain/src/rewrite.rs: the compaction's last claim checkpoint
  (`Checkpoint::Publish`) runs, then `relist_changed`, then the record PUT.
  Holds. Between the re-list and the PUT nothing reads the claim or the bucket.
- crates/ravel-maintain/src/claim_guard.rs `checkpoint`: no store request
  within `renew_after_ns` of the last claim write, so a stolen claim is noticed
  only at a later checkpoint. Holds.
- ADR-1029, 2026-10-03 amendment, point 7: "An owner that stalls longer than
  the lease between its re-list and its record PUT is caught by neither: both
  ran before the steal, so it publishes, and the pass that stole the claim can
  publish too." Names the window, not the outcome. Holds.
- CHANGELOG.md 0.34.0 known issue: "With claims on, the default, the two are
  fenced." Overstated: they are fenced except in that window. Holds.
- Reader side: `process_bucket` serves an unsuperseded `l1` record's parts and
  the rewrite's parts together; `erasure_dominated_compaction_records` drops
  only version 2 records whose predecessor a rewrite supersedes; only the
  rewrite pass's `select_live_record` fails on two live records. Holds.
- ADR-1029 amendment point 6: making the two record kinds collide at one
  `CreateIfAbsent` "would have changed the frozen record key layout". So a
  shared create-only key is a key-layout change and needs an ADR under the
  repository's frozen-contract rule. Holds.

### Decision

Yes, correct both texts: the 0.34.0 known-issue note is edited in place to say
that claims narrow the race to one window and what that window does, and
ADR-1029 gets a dated amendment stating the outcome beside point 7. Fix
direction: a reader-side dominance rule, applied identically by snapshot
resolution, the token fallback, the index fold, the sweep and the erasure
pass, is the 0.35.0 fix. The shared create-only key is the long-term closure
and needs its own ADR; a fencing token is rejected.

### Why

The outcome matters more than the window: an operator who read "fenced" ran an
erasure believing it final, and the window serves the erased series again with
duplicated rows and then fails every later erasure of that bucket with
`MultipleLiveRecords`. The ADR has to say that, because point 7 is where the
next reader looks.

On the fix: a fencing token cannot close the window. The stall lands inside
the PUT, after every client-side check, and the object store offers no
condition on a different key than the one being written, so a CAS renewal right
before the PUT only narrows the window. The shared key closes it at the store,
but it changes the frozen record key layout, it does not repair a store already
holding two live sets, and a per-bucket generation key needs a generation the
publisher derives from a listing, which is the same list-then-act shape. The
reader rule closes the outcome without a format change: in this race both
passes planned from the same listing, so the compaction's inputs are exactly
the rewrite's effective inputs, and a rewrite's parts carry every row of those
inputs minus the erased ones. A version 1 compaction record not in a live
rewrite's chain whose every input is among that rewrite's effective inputs is
dominated by it: readers drop its parts, the sweep reclaims it and its parts in
the rewrite's chain group, as it already does for a dominated version 2 record,
and the erasure pass's `select_live_record` treats it as superseded rather than
failing. A record whose inputs are not all covered is a genuine conflict and
keeps today's behaviour, which a counter and a WARN line make visible. The
rule is deterministic, every reader applies it, and it also covers the
claimless callers, so the `--no-claim` advice becomes about wasted work, not
correctness.

### Conditions and follow-ups

1. The docs correction (changelog note and ADR-1029 amendment) lands this
   week as its own documentation pull request; it does not wait for the fix.
2. The dominance rule lands for 0.35.0 with: the rule in
   `erasure_dominated_compaction_records` (or a sibling it calls), the sweep's
   chain-group gathering, `select_live_record`, docs/catalog-and-mvcc.md step
   3, docs/deletion-and-gc.md's superseded-input row, a counter
   `ravel_catalog_dominated_compaction_records_total` or the existing sibling
   counter widened, and the #4507 test un-ignored and asserting that the
   served set is exactly the rewrite record's parts and the later erasure pass
   succeeds. The rule needs an ADR-1029 amendment of its own when it lands,
   since it narrows decision 2's "a claim bug cannot corrupt data" to "a claim
   bug costs a wasted compaction".
3. The claim renewal immediately before the PUT (the issue's proposed fix) is
   still worth adding as a narrowing, in the same change or after, but it is
   not the fix and must not be described as one.
4. The shared create-only key is decided by an ADR, held for the human (section
   7). It is not needed for 0.35.0 once the reader rule is in.
5. Priority: before 0.35.0, above #4501 and #4428. The 0.34.0 note already
   promises the fix in 0.35.0, the window is reachable by an ordinary VM or
   GC pause at the default lease, and Antithesis pause injection reaches it on
   purpose.

### Confidence

High on the texts. High on rejecting the fencing token. Medium-high on the
dominance rule: the input-equality argument is exact for the race the test
reproduces; the implementer must show the sweep side with a test that pins a
dominated version 1 record's parts and inputs reclaimed in one group.

### Draft corrected 0.34.0 known-issue text (CHANGELOG.md, edited in place)

```markdown
**Known issue: an erasure that races a compaction or migrate of the same
bucket can publish beside it (#4449).** When both publish, every reader serves
the erased series again, rows of that bucket are returned twice, and every
later erasure of the bucket fails with `MultipleLiveRecords`. With
`--maintain-claims off`, or with `ravel-cli maintain compact-bucket`,
`compact-tenant` or `migrate` run with `--no-claim` beside a live fleet,
nothing fences the two. With claims on, the default, the bucket claim and the
pre-publish re-list fence every case but one: a pass that stalls for longer
than the remaining claim lease (about 200 to 300 s at the default 300 s lease,
from a process freeze, a VM or container pause, or a PUT stalled inside the
client's timeout) between its re-list and its record PUT publishes anyway, and
so does the pass that took its claim. Keep claims on, keep the lease above the
longest pause the platform can impose, and treat `MultipleLiveRecords` from the
erasure pass as this issue. 0.33.0 has the same race. The fix, a reader-side
rule under which the rewrite dominates a compaction record over the same
inputs, is planned for 0.35.0.
```

### Draft amendment text for ADR-1029

The heading "Amendment (2026-10-03): the claim fences the compaction and
erasure rewrite publishes (issue #2199)" exists once; its point 7 gains the
pointer sentence "The outcome of that window is stated by the 2026-11-20
outcome amendment below." after "the pass that stole the claim can publish too."

```markdown
## Amendment (2026-11-20, #4449): what the residual publish window does, and how it closes

<!-- amendment-applies: sections="Amendment (2026-10-03): the claim fences the compaction and erasure rewrite publishes (issue #2199)" pointer="2026-11-20 outcome amendment" -->

Point 7 of the 2026-10-03 amendment names the window the claim and the
re-list leave open: an owner that stalls longer than the lease between its
re-list and its record PUT publishes, and so does the pass that stole its
claim. It did not say what that produces. Reproduced with claims on, with
injected clocks and a store gate parking the compaction inside its record
PUT: both records land, every reader serves the compaction's parts beside
the rewrite's, so the erased series is served again and the bucket's
surviving rows are returned twice, and every later erasure pass over the
bucket fails with `MultipleLiveRecords`, so the request never completes and
its `.dreq` is never retired. The window is reached by any stall of about
200 to 300 s at the default lease (the claim is at most a third of the lease
old at the publish checkpoint): a process freeze, a VM or container pause,
or a PUT stalled inside the client's timeout. A claim renewal immediately
before the PUT narrows it and cannot close it, because the stall can land
after any client-side check and the store conditions a write on its own key
only.

The closure is on the read side, applied identically by every bucket
resolution: a version 1 compaction record that is not in a live rewrite's
chain and whose every input is among that rewrite's effective inputs is
dominated by the rewrite. Readers drop its parts, the superseded-input sweep
reclaims it and its parts in the rewrite's chain group, as for a dominated
version 2 record, and the erasure pass treats it as superseded. In the race
above both passes planned from one listing, so the compaction's inputs are
exactly the rewrite's, and the rewrite's parts hold every row of those inputs
minus the erased ones; nothing is lost. A compaction record whose inputs are
not all covered is a genuine conflict and is served as today, counted and
warned. Decision 2's statement that a claim bug cannot corrupt data is
restored in this form: between a compaction and an erasure rewrite a lost or
late claim costs a wasted compaction, never a served erased row. Making the
two record kinds collide at one create-only key per bucket, which point 6
rejected as a key-layout change, remains the store-side closure and needs its
own ADR if it is ever wanted.
```

## 6. Order and priority for the next week

### Decision

Order, with the reason each item sits where it does:

1. **#4503 fix (PR #4511) with the ADR-0064 amendment, days 1 to 2.** Live
   data loss of an erasure guarantee in two released versions; the advisory
   cannot go out until the fix has a release to point at. Fix the two review
   findings per section 1, add the amendment, merge.
2. **#4503 changelog fragment and advisory, same day as the merge.** Text is
   drafted in section 2; the human posts the advisory.
3. **#4449 docs correction (0.34.0 note, ADR-1029 amendment), day 1, in
   parallel with item 1.** Docs only, no gate cost beyond the doc scripts;
   operators reading "fenced" today are misinformed.
4. **#4449 reader-side dominance rule, days 2 to 5, targeted at 0.35.0.** The
   one remaining way an erased series is served with default settings; the
   0.34.0 note already promises 0.35.0.
5. **#4501 contract 1 (re-read before each delete batch), ADR-0042 amendment,
   guide rewrite, #4505 test un-ignored, days 3 to 5.** A compliance contract
   the code does not meet, but the exposure needs a hold placed during a pass
   and no erased data is involved. Merge #4337 after it.
6. **#4428 invariant and doc sentence, end of the week or first idle
   executor.** No reachable trigger found; the invariant is cheap.

### Antithesis budget (about 888 compute hours left of 1200)

- Do not spend lifecycle hours on main before items 1 and 4 are on the branch
  under test: a run now re-finds #4503 and the #4449 window, both already
  reproduced deterministically, and burns budget on known findings.
- Reserve the larger share, about 600 hours, for the lifecycle profile (T9,
  #4338) after items 1, 4 and 5 land, with pause injection on and reads with
  old tokens across erasure, rewrite and sweep, which is what the #4428
  invariant and the #4449 rule need to be exercised against.
- Spend at most about 100 hours this week: one run on a build carrying items
  1, 3 and 4 as soon as item 4 is gate-green, pause injection on, to check the
  dominance rule under the race it closes.
- Keep the remainder, about 180 hours, unallocated for re-runs on a finding.
- Triage rule until the fixes land: `HeldObjectNeverDeleted` false before item
  5 is the #4501 window; two live record sets in one bucket before item 4 is
  #4449; a logs subject served after its `.dreq` is gone before item 1 is
  #4503. None of those is a new bug.

### Confidence

Medium. The day estimates assume one executor per item and the usual review
rounds; the ordering does not depend on them.

## 7. Lines for the human to type

Decisions above that only the human can authorize, each as the line to type or
the action to take:

1. **Post the #4503 advisory.** It is outward facing. The text is in section 2;
   confirm the first affected release before posting:
   `git log --oneline -1 1f33d5eeb` and the release tag that first contains it.
2. **Approve editing the released 0.34.0 changelog section in place.** The
   known-issue note is a note, not a change entry, so in-place correction is
   what this document recommends; a release-notes policy that forbids editing a
   released section would instead put the corrected text under 0.35.0 with a
   pointer. Type `edit in place` or `0.35.0 only`.
3. **Sign the ADR-0042 amendment.** It changes the stated behaviour of a
   compliance feature. The decision here is contract 1; the ADR owner signs
   the amendment text in section 3 or amends it.
4. **Open, or decline to open, an ADR for a shared create-only record key per
   bucket (#4449 long-term closure).** It changes the frozen record key layout
   and is not needed for 0.35.0 once the reader rule lands. Type `open ADR` or
   `not now`.
5. **Confirm the Antithesis allocation in section 6** (about 100 hours this
   week, about 600 reserved for the post-fix lifecycle runs, about 180
   unallocated), or give different figures.

Nothing else in this document is held: the #4503 fix direction and amendment,
the #4503 changelog-only remediation, the #4501 contract, the #4428 closure
with an invariant, the #4449 text corrections and fix direction, and the work
order are decided here, subject to override.
