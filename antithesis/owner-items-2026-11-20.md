# Owner items for a delegated decision (2026-11-20, session antithesis-gcp)

Context: the owner asked that these open decisions be made by a Fable agent. Each item below has the
question, then evidence copied from GitHub (executors cannot read GitHub). Repo files (ADRs, docs,
code, tests on the result branches named) are in this checkout at origin/main plus this file.

Branches with the reproductions and the fix (not in this checkout; their content is described below
and in the issue comments): antithesis-gcp/erasure-stream-attrs (PR #4511, #4503 fix, draft),
antithesis-gcp/4428-swept-fallback (PR #4504), antithesis-gcp/4501-mid-pass-hold (PR #4505),
antithesis-gcp/4449-claims-pause (PR #4507).

## The questions (from the session's owner-questions file)


1. #4503 fix (draft PR, branch antithesis-gcp/erasure-stream-attrs): the logs erasure rewrite will drop records whose resource or scope attributes match, the same records queries already hide. It widens an irreversible delete where ADR-0064 is silent on attribute scope. Approve the fix, and should ADR-0064 get a dated amendment naming the attribute scope (metrics labels; logs record OR merged resource+scope+record view; spans merged attrs)? I will not enqueue it without your OK.
2. #4503 remediation for existing stores: requests whose buckets the old code rewrote with zero drops are skipped as AlreadyApplied, so a still-pending request does not re-drop after the upgrade. Operators must submit a new request (new id) for the same subject. OK to say that in the changelog only, or do you want a tool (e.g. `ravel-cli erase resubmit`)?
3. #4501 (minimized test in PR #4505): pick contract 1 (re-check holds before each delete batch) or 2 (holds take effect next pass; ADR-0042 + model + CLI say so).
4. #4428 (repro PR #4504): fix shape for the swept-token fallback beside a stale HEAD: typed refusal until HEAD catches up, or serve the rewrite's parts and drop the replaced objects when they are exactly covered?
5. #4503 affects released v0.33/v0.34: a logs erasure by resource or scope attribute reports completed (.done) and the data returns once the .dreq is swept (~4.5 days with defaults). Publish a user advisory / known-issue note for v0.34 (and tell operators to re-submit such requests after upgrading to the fix)?
6. #4449 with claims ON (PR #4507): a compaction stalled longer than the claim lease (default 300 s) between its re-list and its record PUT publishes beside the erasure rewrite; the erased series is served again and rows duplicate. ADR-1029 amendment point 7 names the window, not the effect; the 0.34.0 known-issue note says claims on means 'fenced'. Correct the 0.34.0 known-issue text (and ADR-1029 amendment) to state the residual window and its effect?

## PR #4511 (draft) description, the #4503 fix

Fixes #4503: a logs erasure whose subject is named only in a resource or scope attribute was hidden while its request was pending, then served again once the request was swept, because the rewrite matched record attributes only.

## What changes

- `LogErasureMatcher` (crates/ravel-maintain/src/erasure_rewrite.rs) now applies the query path's rule (`ravel_query::erasure::is_erased_log_row`): a record is dropped when the request's window holds and its conjunction matches either the record's own attributes or the merged resource + scope + record view (record value wins on a key collision), each read as the first string value under a key. Each stream's attributes are decoded once per rewrite.
- One narrowing that comes with reading the record view the query path's way: a key repeated in a record's own attributes used to match any of its values in the rewrite, which could drop a record queries still served. It now matches as queries read it.
- docs/deletion-and-gc.md says which attributes a matcher tests per signal. Changelog fragment `changelog.d/4503.fixed.md` tells operators that a bucket an earlier version rewrote for a request is skipped for that request, so they submit a new request for the subject.

## Why this direction

Queries have hidden the merged view since 1f33d5eeb, and `LifecycleGC.tla` `DreqRemovalCannotResurrect` says removing the `.dreq` never brings an erased record back. Narrowing the query instead would re-expose records users already saw hidden. The rewrite now drops exactly what queries hide. This still widens an irreversible delete where ADR-0064 is silent on attribute scope, so it waits for an owner decision (and possibly a dated ADR-0064 amendment) before it merges.

## Tests

- `services/ravel-cli/tests/erasure_stream_attribute_rewrite.rs`: OTLP ingest router, `erase submit` for a resource/scope subject (alice) and a record-attribute subject (dave), the maintain rewrite and completion check, deletion of the `.dreq` objects, then reads through `export --signal logs`. On main all three alice records come back after the sweep; with this change they stay erased, dave stays erased and the bob survivors stay. A second test pins the changelog's remedy: a new request rewrites a bucket an earlier rewrite already rewrote.
- `crates/ravel-query/tests/erasure_rewrite_parity.rs`: a proptest (4096 cases) comparing the rewrite matcher with `is_erased_log_row` over generated resource/scope/record attributes with shared, repeated and non-string keys and timestamps inside and outside the window, plus pinned cases. Shown failing with the merged view turned off, and with the record view back on any-value matching.
- The end-to-end test fails with the production change reverted (`left: [alice-resource, alice-resource-web, alice-scope, bob-record, bob-resource]`).

## Gates

Run on a GCP build box as a non-root user, scoped `scripts/gates.sh -p ravel-maintain -p ravel-query -p ravel-cli` on this exact tree: fmt, workspace guards, and clippy over the whole reverse-dependency closure pass. The test step failed three times on three different timing-sensitive tests under a 24-job build on a shared box (`metrics_label_index_routing` once, `cache_warm` deadline warning once, plus a root-only and a nofile-limit environment issue fixed in between); each passes 5 of 5 run alone on this tree. The feature lanes after the test step did not run locally, so this PR relies on CI for them.

Refs: #4326, #4494

Open fleet-review findings on #4511 (round 1, to be fixed by the session, listed so the decision can account for them):
- should_fix docs/deletion-and-gc.md: the new paragraph states the stays-erased guarantee unconditionally, while the code and changelog fragment exempt a bucket an earlier version already rewrote for the same request.
- should_fix erasure_rewrite.rs: the stream-view cache is keyed by the whole stream_attrs blob (two hashes per record) and is uncharged against the rewrite's memory budget.

## Evidence: issues and comments

# #4503 Logs erasure of a resource or scope attribute subject is undone when the request is swept

A logs erasure whose subject is named only in a resource or scope attribute is hidden while its request is pending, then served again once the request is swept. The rewrite never drops those records.

## Contract

- ADR-0064 decision 2 and `docs/consistency-model.md` ("Selective subject erasure"): no query whose snapshot resolves after the request ack returns matching records.
- `crates/ravel-query/src/erasure.rs` (`retain_unerased_log_records`, since 1f33d5eeb): logs matchers test the merged resource + scope + record attribute view, and that is "the authoritative selective-erasure exclusion".
- `formal/tla/lifecycle/LifecycleGC.tla` `DreqRemovalCannotResurrect`: removing the `.dreq` never brings an erased record back.

## What the code does

`crates/ravel-maintain/src/erasure_rewrite.rs` `build_rewrite_logs` filters each record with `first_dropping_log_request(requests, &record.attrs, ...)`. `LogErasureMatcher::matches_attrs` reads the record's own `attrs` only. Its doc comment says this matches the query path, which stopped being true when the query path moved to the merged view.

So for `user_id = alice` where alice is a resource or scope attribute:

1. The query filter hides alice's records while the `.dreq` is pending.
2. The rewrite matches nothing, publishes a rewrite record with `dropped_count: 0`, and passes the conservation gate.
3. Completion is structural (no pre-rewrite object is served), so `.done` is written.
4. Once the `.dreq` is swept, nothing filters alice's records and every read serves them again.

Spans are not affected: `ravel_rspan::merge_attrs` folds resource and scope into the span's `attrs` at ingest. Metrics match series labels on both sides.

## Reproduction (deterministic, not an Antithesis run)

Found by a code audit for the Antithesis lifecycle campaign, then reproduced with an in-process test: `services/ravel-cli/tests/erasure_stream_attribute_rewrite.rs` on branch `antithesis-gcp/erasure-stream-attrs`, run against main 9f17cd15d.

The test writes six log records through the OTLP ingest router in two strict writes: alice in a resource attribute (two streams) and in a scope attribute, dave in a record attribute, and two bob survivors. It submits `user_id = alice` and `user_id = dave`, runs `erasure_rewrite_bucket` and `bucket_erasure_completion` on the sealed bucket, deletes both `.dreq` objects (the store effect of the request sweep), and reads through `ravel-cli export --signal logs`, which applies the query path's visibility rules.

Every step passes until the last read:

```
assertion `left == right` failed: after the requests are swept, only the rewrite keeps the erased subjects out: dave (record attribute) and alice (resource or scope attribute) must both stay erased
  left: ["alice-resource", "alice-resource-web", "alice-scope", "bob-record", "bob-resource"]
 right: ["bob-record", "bob-resource"]
```

dave stays erased, so the rewrite itself works. All three alice records come back.

## Proposed fix

Make the logs rewrite match the same merged view the query filter uses: a record is dropped when a request's window holds its `ts_ns` and the request's conjunction matches the record's resource + scope + record attributes, record winning on a key collision. This drops exactly the records queries already hide, so it widens nothing a reader could see. Pair it with the test above (un-ignored), and say in `docs/deletion-and-gc.md` which attributes a logs matcher tests.

Records ingested and erased under the current code stay in their rewritten parts. After the fix, a new request with the same predicate would drop them on its next rewrite. Whether existing `.done` requests need a re-run is an operator note for the changelog.

Refs: #4326, #4494




# #4501 Legal hold written mid-pass does not stop that pass's deletes (model vs code)

A legal hold that is written while a sweep or retention pass is running
does not protect objects that pass deletes after the write. The lifecycle
model and ADR-0042 say otherwise, so one of them needs to change.

What each source says:

- ADR-0042 decision 2: LegalHoldCheck "consults a per-tenant (or
  per-time-range) hold record before any physical delete in both the
  sweeper and the retention path; a hold present means the delete is
  skipped".
- formal/tla/lifecycle/LifecycleGC.tla: HeldObjectNeverDeleted reads "the
  hold state the delete observed at its own step". PlaceHold changes
  heldBuckets at once, and every sweep step checks it.
- crates/ravel-maintain/src/legal_hold.rs module doc ("The sync/async
  tension"): the hold set is loaded once before the pass into an in-memory
  snapshot, and LeaseCheck::is_protected reads only that snapshot. A hold
  record that lands after the snapshot's LIST is invisible until the next
  pass.

So a hold acknowledged to the operator at time T can still see objects
under its scope deleted after T, until the running pass ends. The model
does not allow this, so the model's guarantee is stronger than what the
code provides.

How it shows up: the #4337 work puts HeldObjectNeverDeleted at the orphan
quarantine move, the quarantine purge and retention's delete_all. Each
site is judged from a hold set re-read from the store at the delete,
which is what the model asks for. The test
a_hold_placed_after_the_snapshot_trips_held_object_never_deleted in
crates/ravel-maintain/src/sweep.rs pins the window: lease taken, hold
written, key deleted, property false. An Antithesis lifecycle profile that
places holds while maintain workers run can report this property false
on the shipped design. An earlier draft that judged from the store alone
produced, for a hold placed after the snapshot:

    a hold placed after the snapshot never makes the property false:
    [(false, "{\"key\": \"t/.../m/l1/0000/19700102T20/p.0\"}")]

Decision needed (owner):

1. Make the code match the model: re-check holds from the store just
   before each delete batch, or fence hold writes against running passes.
   This costs one extra hold LIST per batch.
2. Or make the model and ADR match the code: a hold takes effect from the
   start of the next pass. Write that into ADR-0042, model it in
   LifecycleGC.tla (PlaceHold only takes effect at the next refresh), make
   the hold CLI tell the operator when the hold is effective, and either
   narrow HeldObjectNeverDeleted's Rust site to holds older than the pass
   snapshot or record the window as a known Antithesis finding.

Until this is decided, triage a HeldObjectNeverDeleted failure in
Antithesis against this window before treating it as a new bug.

Refs: #4326, #4337


--- comment by pmoust at 2026-11-19T22:19:58Z
Minimized failing test for the window, on main 9f17cd15d (deterministic, not an Antithesis run), preserved for whichever contract the decision picks.

`crates/ravel-maintain/tests/legal_hold_mid_pass.rs` on branch `antithesis-gcp/4501-mid-pass-hold` uses the maintain tick's own entry points: seed two metrics L0 inputs and compact them, take the pass's hold snapshot with `LegalHoldCheck::refresh` (empty), write a hold over all three shard scopes with `write_hold_set` and confirm a fresh refresh sees it, move past the protection horizon, then run `sweep_shard` with the pass's snapshot. It asserts contract 1 (ADR-0042 decision 2, `HeldObjectNeverDeleted`):

```
assertion `left == right` failed: an object under a hold acknowledged before its delete is not deleted (ADR-0042 decision 2, HeldObjectNeverDeleted); sweep report: SweepReport { ... superseded_records_deleted: 2, superseded_data_deleted: 2, ... superseded_groups_held_by_legal_hold: 0, ... }
  left: 0
 right: 2
```

The test is `#[ignore]`d with the reason "contract decision pending #4501". Under option 1 it passes and loses the ignore. Under option 2 it is rewritten to assert the next-pass bound that ADR-0042, LifecycleGC.tla and the hold CLI would then state.



# #4428 fix(catalog): check the swept-token fallback for the #4418 rewrite double count

Two things the #4418 fix review found that were already on main before that fix. Neither is fixed by #4418.

## 1. The swept-record fallback may serve rows twice beside a stale HEAD

`resolve_min_token_fallback` (crates/ravel-catalog/src/catalog.rs) serves a live erasure rewrite's output parts when the token's commit record has been swept. It does not check whether the resolved set already holds another object that rewrite replaced.

The #4418 fix added exactly that check on the present-record path. Without the check, the case was a double count: HEAD folded before rewrite R landed names input L_M (or a compaction of it). R replaced both L_M and the token's input L_P. A query covers M's events and carries P's token. If L_P's commit record is already swept while HEAD still names L_M, the fallback would add R's parts beside L_M, and M's surviving rows would come back twice.

Whether that state is reachable depends on sweep ordering. The superseded-input sweep holds objects HEAD still names, and the `.dreq` release rule (ADR-0064 amendment) holds the request while HEAD can resolve an input R superseded. That does not obviously stop L_P's commit record being swept while L_M is still named. This needs a test that sets the state up and either shows the double count or pins why it cannot happen.

## 2. docs/deletion-and-gc.md overstates the tombstoned-token rule

`docs/deletion-and-gc.md` (around line 650) says without condition that "a token whose bucket is tombstoned resolves as satisfied with zero segments". On main, a present-record token in a tombstoned bucket gets its own L0 input added when the set holds no level-1 part of that bucket and no erasure is pending. That is still true after #4418, whose check runs only under those conditions; `docs/catalog-and-mvcc.md` step 5 now describes it exactly. No rows are wrong, because a tombstoned bucket serves nothing to any query. The sentence should say what the code does.

Refs: #4418, #4326


--- comment by pmoust at 2026-11-17T21:22:41Z
Release decision: this does not hold v0.34.0. It is pre-existing on main, and whether the state is reachable is unproven. The #4418 fix (#4429) remains the only gate from this area.


--- comment by pmoust at 2026-11-19T22:19:56Z
Item 1 reproduced at the catalog level, on main 9f17cd15d (deterministic test, not an Antithesis run).

The test is the swept-record twin of `token_beside_an_input_its_live_rewrite_replaced_adds_its_own_input` in `crates/ravel-catalog/tests/token_compaction_duplicate.rs`: HEAD is folded while it names M (raw, or a compaction part of M), then rewrite R lands over P and M with the `.dreq` pending, then P's commit record and data object are deleted (the superseded-input sweep's effect: R superseded P and HEAD never named it). A resolve with P's token then holds both R's part and the object HEAD names:

```
swept_token_beside_a_raw_input_its_live_rewrite_replaced:
R's part beside an input R replaced serves M's rows twice: [".../l/l0/0000/...044180002...rseg", ".../l/l1/0000/20270115T08/4e2060dd37103193.0000.e0e0e0e0e0e0e0e0.rseg"], rows 10

swept_token_beside_a_compaction_its_live_rewrite_replaced:
R's part beside an input R replaced serves M's rows twice: [".../l/l1/0000/20270115T08/1111111111111111.0000.c1c1c1c1c1c1c1c1.rseg", ".../l/l1/0000/20270115T08/4e2060dd37103193.0000.e0e0e0e0e0e0e0e0.rseg"], rows 10
```

Rows written survive as L + M - 1 = 6; the snapshot sums to 10, so M's 4 rows are served twice. The test accepts either a typed refusal or a snapshot that serves every surviving row once (R's part without the object it replaced); it fails only on the double count.

Not yet shown: that a real sweep reaches this state (P's commit record swept while HEAD still names M). The test builds the state directly. The planned Antithesis lifecycle profile (T9 #4338) reads with old tokens across erasure, rewrite and sweep to probe reachability. The test is on branch `antithesis-gcp/4428-swept-fallback`.


--- comment by pmoust at 2026-11-19T23:17:26Z
Correction to my comment above, from review on #4504: the fold that names M also names P, and the superseded-input sweep holds an object HEAD names, so a real sweep should not delete P while that HEAD is current. The test deletes P directly. Reaching the state in production needs a HEAD that names M (or a compaction of M) but not P, which I have not found a path to. Item 1 is a resolver behaviour pinned by the test, with reachability unproven.



# #4449 An erasure racing an unclaimed compaction can serve the erased sample

An erasure rewrite racing an unclaimed compaction or migrate of the same bucket can publish alongside it. Every reader then serves the erased sample, with duplicate rows, and nothing reports the breach. Present in v0.33.0 and v0.34.0.

## Reproduction

Fleet investigation against the #4419 test branch (`crates/ravel-compat-tests/tests/erasure_during_migrate.rs` with a probe mode). Samples: a, b, and an erased victim. Seed 10, with no claim participant on either pass and the store clock set:

```
Put .../c/0000/20260621T00/rw.3a9bfe5fbc301914.cmt
Put .../c/0000/20260621T00/l1.ae27848db35f6dfa.cmt
PROBE query after run: [(…001,…),(…001,…),(…002,…),(…002,…),(…003…, 4615063718147915776)]
PROBE query contains erased sample: true
PROBE erasure rerun: Err(erasure rewrite found more than one live record in a bucket (fatal invariant breach ...))
```

## Mechanism

- The erasure PUTs `rw.<hash>.cmt` create-only (`crates/ravel-maintain/src/erasure_rewrite.rs:2032-2056`). Compaction and migrate PUT `l1.<hash>.cmt` create-only. These are different keys, so neither create-only PUT refuses the other.
- Each pass re-lists the bucket before its PUT (`rewrite.rs:427-452`; erasure at `erasure_rewrite.rs:2716-2729`, compaction and migrate at `rewrite.rs:619-640`). The re-list and the PUT are not atomic, so two passes that both list before either PUTs both publish. ADR-1029's 2026-10-03 amendment, point 5, documents this window for claimless callers. It does not say that the outcome is an erased sample being served.
- Reader: only the erasure rewrite's `select_live_record` (`erasure_rewrite.rs:513-584`) detects more than one live record. `Catalog::process_bucket` (`crates/ravel-catalog/src/catalog.rs:3694` onward) includes the unsuperseded `l1` record's parts and the rewrite's parts with no error and no log line. `erasure_dominated_compaction_records` drops only version-2 records that name a superseded record (`catalog.rs:5360-5365`).
- A lost claim goes unnoticed. `ClaimGuard::checkpoint` (`crates/ravel-maintain/src/claim_guard.rs:760-797`) makes no store request within lease/3 of its last claim write, so an owner whose claim was taken over still publishes. In production this needs a contender clock ahead of the store by about the lease (300 s default); the reproduction used an unset test-store clock.

## Reachable configurations (shipped)

- Any maintain process with `--maintain-claims off`.
- `ravel-cli maintain compact-bucket`, `compact-tenant` or `migrate` with `--no-claim` against a live maintain fleet.
- Background migrate, which never claims. It is latent in v0.34.0; see #4448.

With default flags (claims on), the maintain tick's erasure and compaction share one claim participant and are fenced by the claim.

## What a fix needs (for the owner of ADR-1029 to decide)

- Readers fail closed, or at least log and count, when a bucket has more than one live record. Serving an erased sample silently is the worst outcome.
- The publish checkpoint always CAS-renews the claim, after the re-list and immediately before the PUT, not only after lease/3.
- Either erasure refuses to run while claims are off, or the claimless window is closed some other way (an erasure-dominates rule for a plain `l1` record, or a shared create-only key per bucket generation). Until then, document the outcome next to `--maintain-claims off` and `--no-claim`.


--- comment by pmoust at 2026-11-19T23:33:03Z
Reproduced with claims ON (the default), deterministically, on main 9f17cd15d. Not an Antithesis run.

`crates/ravel-maintain/tests/erasure_claim_pause.rs` (branch `antithesis-gcp/4449-claims-pause`) runs a claimed compaction A and a claimed erasure rewrite E over one sealed metrics bucket, each with its own claim participant, a 300 s lease and injected clocks. A store gate parks A inside its record PUT, which is after its last checkpoint (`rewrite.rs:630`, `Checkpoint::Publish`) and its re-list (`rewrite.rs:638`). The clock then moves past the lease, E steals the expired claim and publishes `rw.<h>.cmt`, and A's parked PUT (`publish.rs:353`) is released:

```
contract broken with claims on (A: Ran(Compacted { parts: 1, publish: Published })):
2 live record sets in one bucket: compaction [".../m/c/0007/20260621T00/l1.6e52ee1c795cd20b.cmt"], rewrite [".../rw.549ecb85e3c6a917.cmt"]
served {...549ecb85e3c6a917.0000...rseg, ...6e52ee1c795cd20b.0000...rseg}, not exactly the rewrite record's parts {...549ecb85e3c6a917.0000...rseg}
the erased series is served: ["victim@1000 = 5 x1", "victim@3000 = 3 x1"]
samples served more than once: ["keep@1000 = 1 x2", "keep@2000 = 2 x2"]
```

A later erasure pass over the bucket fails with `MultipleLiveRecords`. "Served" here is the segment set `Catalog::resolve` returns, decoded; query-level dedup was not exercised, and the state is after the `.dreq` is gone.

Two controls use the same contract check and pass: A paused at its part PUT with the shared clock moved ends `Cancelled { at: PartBoundary }`; A paused with its own clock not moving (the lease/3 renewal gap) is stopped by its re-list (`RewritePresent`). So the claim and the re-list do fence everything except the window between the re-list and the record PUT.

Between the re-list and the PUT A runs only a deadline check on its own clock (`publish.rs:268`), the conservation check and `compat_policy::before_put`; none reads the claim or the bucket. A process freeze (GC, VM stall, container pause) or an in-flight PUT stalled within the S3 client's request timeout reaches it once it outlasts the remaining lease (about 200 to 300 s at the default, since the claim is at most lease/3 old at the publish checkpoint).

Contract status: ADR-1029's 2026-10-03 amendment, point 7, names this window ("An owner that stalls longer than the lease between its re-list and its record PUT is caught by neither ... the pass that stole the claim can publish too"). What neither the ADR nor the 0.34.0 CHANGELOG ("With claims on, the default, the two are fenced") states is the outcome: the erased series is served again and rows are duplicated. The issue's proposed fix (renew right before the PUT) cannot close it, because A is parked after every client-side check. Closing it needs a shared create-only key per bucket for both record kinds, or a reader rule that resolves two live record sets.

The test is `#[ignore = "suspected defect #4449: ..."]`. The Antithesis lifecycle profile can reach this window with container pauses, so a run that reports it should be triaged here.


