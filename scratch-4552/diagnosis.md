# Issue #4552: why `complete-ignores-served-set` never falsifies its property

Status: diagnosed. Not built. Every fix changes what "served" means for this
property, or changes production logic or the ADR-4326 mutant mapping, so it
needs an owner decision (brief step 2).

## Cause: (b), by design. The code mutant is wider than TLA's `CompleteIgnoresServedSet`

The fresh read judges different buckets than the guard's `catalog_blocked`
set. That is not a bug in the read: the code mutant drops two gates, and the
property checks only one of them.

`bucket_erasure_completion` (crates/ravel-maintain/src/erasure_rewrite.rs)
returns one `blocked` set that mixes two gates:

1. **Open-bucket gate** (issue #1290). A bucket the gate may not judge yet
   (`may_judge_completion` is false: unsealed, or not yet plannable under the
   late-commit fence) blocks every request whose scope covered it at ack. The
   bucket is not read. In TLA this is `OpenBucketPermitsCompletion`, checked by
   `CompletionCoversEveryBucketOpenAtRequest`.
2. **Served-set gate.** For a judgeable bucket, the catalog resolver's live
   view still serves the subject. In TLA this is `~ServesNow("s1")`, checked
   by `CompletionImpliesNoPreRewriteExposure`.

`pass.catalog_blocked` is the union of both gates over listed ∪ derived hours.
The mutant (`maintain.rs` ~4166) drops the whole `contains` guard, so it
disables both gates. In TLA, `CompleteIgnoresServedSet` drops only clause 2
and keeps `OpenBucketPermitsCompletion` (LifecycleGC.tla `CompleteErasure`).

The property's evidence, `served_after_completion`, deliberately judges only
clause 2. It walks listed hours only (no derived hours) and `continue`s past
every bucket with `!may_judge_completion`. Its doc comment says so: "A bucket
the gate may not judge yet is left out:
CompletionCoversEveryBucketOpenAtRequest owns that case." The same decision is
recorded in two more places:

- traceability.md row 34, last column: "none: no assertion carries the
  open-hour union".
- antithesis-mutants.tsv: `completion-ignores-open-bucket none: ADR-4326
  decision 3 ships no mutant for CompletionCoversEveryBucketOpenAtRequest`.

In this run, the three requests acked after the sealing jump reached their
first non-deferred tick while their ack-open hours were unsealed
(`not_sealed=4`). That tick's rewrite pass had already rewritten the sealed
buckets (`rewritten=1 already_applied=3`), so the block that was left came from
buckets the fresh read does not judge. The mutant wrote `.done` there, and the
fresh read found no judged bucket serving the subject. The verdict was
`Some(true)`, so the property was evaluated and passed. Cause (a), an unknown
verdict, is ruled out by the evaluations below.

## Evidence 1: mutant local run at origin/main d23dbb245 on gcp2

- Run: `/home/builder/logs/4552/lr-mut-main/`, log `lr-mut-main.log`, rc 1.
- Images (built at d23dbb245 by `MUTANTS=1 scripts/build.sh all`):
  - server: `ravel-antithesis-server:lifecycle-gc-mutants` sha256:af2ff4ee5bab...
  - driver: sha256:5e2dead889ee...
  - build check: "registers the 4 lifecycle mutants".

Result:

```
FAIL: mutant lifecycle/complete-ignores-served-set ran, but "erasure: a completion is written only when no pre-rewrite object is still served" was never violated
Always	2	2	erasure: a completion is written only when no pre-rewrite object is still served
REPORT (mutant run, not gated): always-type assertion violated besides the mutant's: erasure: an erased subject is never returned after its request
```

Evaluations of the property per process: [hit, condition, request_id]

```
sdk-maint1.jsonl:  1 [true,true,"00000000-0000-4051-8000-0000000001cd"]   (logs)
sdk-maint2.jsonl:  1 [true,true,"00000000-0000-4084-8000-0000000001ae"]   (metrics)
```

maint2 wrote three `.done` in one tick (01ae, 0069, 01e5) but emitted one line.
In `served_after_completion`, `known` is a single flag for the whole read, and
each request's verdict is `Some(false)` if it is served, otherwise
`known.then_some(true)`. 01ae got `Some(true)`, so `known` was true and 0069
and 01e5 each got `Some(_)`, which means they were evaluated too. No file holds
a failing line (`[true,false,..]`). So there were 4 evaluations (maint1 1,
maint2 3), all true, and 2 emitted lines. The SDK output does not show repeated
passes.

The completing ticks (ANSI stripped, abridged):

```
maint1 16:33:36.092 erasure rewrite pass complete signal=Logs pending=1 rewritten=1 already_applied=3 not_sealed=4 deferred=false catalog_blocked=1
maint1 16:33:36.137 erasure request complete; .done written signal=Logs request_id=...01cd
maint2 16:33:54.429 erasure rewrite pass complete signal=Metrics pending=3 rewritten=1 already_applied=3 not_sealed=4 deferred=false catalog_blocked=2
maint2 16:33:54.469/.472/.476 .done written for ...01ae, ...0069, ...01e5
```

In the logs tick the one pending request was `catalog_blocked`, and the mutant
completed it anyway. That is the mutant's situation. Within the same 45 ms the
fresh read judged 01cd not served. Both reads judge a judgeable bucket with the
same function, so the block most likely came from a bucket the fresh read
skips: one of the `not_sealed=4` buckets, or the derived ack-open hour. The
unit test below proves that path directly.

Tick counts, from `ev3.sh` over the ANSI-stripped log:

- maint1: 17 passes. 1 had `deferred=false` and `catalog_blocked>=1`, and it
  was followed by 1 `.done` write. 14 had `deferred=true` and
  `catalog_blocked>=1`.
- maint2: 16 passes. 1 had `deferred=false` and `catalog_blocked>=1`,
  followed by 3 `.done` writes. 13 had `deferred=true` and
  `catalog_blocked>=1`, mostly a claim backoff (`cb=1`).

So the mutant made a difference in exactly one tick per node.

Driver:

- Request 0069 was acked before the 9000 s sealing jump.
- Requests 01ae, 01cd and 01e5 were acked after it.
- Once the `.dreq`s were swept, the final scans and token reads returned the
  victims of 01cd, 01ae and 01e5 (`hits_after_completion` count 32 in the
  first failing evaluation, placements record/resource/scope/series).

The defect the mutant plants is real and reachable. Only the driver's
end-to-end property catches it.

## Evidence 2: a unit test that reproduces the gap

- Test: `services/ravel-server/src/erasure_e2e.rs::mutant_completion_over_the_unsealed_ack_hour_is_judged`.
- Commits f0c96d96a and e877e52ec, local on the Mac branch
  `fix/4552-served-set-property` in `/Users/pmoust/nofire/store-wt-4552`. Not
  pushed.
- Patch: `runs/4552/repro-test.patch`.
- It adds `publish_subject_l0_in(.., hour)`. Hour 0 gives the same record as
  before.

Fixture:

- The subject's only L0 record is in the ack-open hour (listed, unsealed).
- The request is acked 60 s into that hour.
- One `run_erasure_pass` runs 20 s later.

Results on gcp2 at e877e52ec (`/home/builder/logs/4552/ut-*.log`):

- Default build: passes. The shipped gate withholds `.done`, and the mutant
  arm is a no-op.
- `--features ravel-invariant/capture,ravel-invariant/antithesis-mutants`:
  - The shipped arm passes.
  - Under the scoped mutant, `.done` is written and exactly one evaluation is
    captured (both asserts pass).
  - It fails on the last assert, line 1320:

```
CompletionImpliesNoPreRewriteExposure must evaluate false under the mutant: [(false, "{\"request_id\": \"00000000-0000-0000-0000-000045520002\", ...}")]
```

  The `false` is `is_violation`. The property was evaluated true.
- The existing `completion_waits_for_the_served_set` still passes in both
  builds. Its Part B is the judgeable-bucket case, where the property does go
  false. So the property works for clause 2. The workload never reaches
  clause 2 at a completion tick.

The harness runs with the late-commit fence Off and the workload with it On.
An unsealed bucket is not judgeable in either mode, so both take the same
branch.

The reproduction test fails the invariants-services lane by design. It must
not land without a fix.

## Stale doc (reported, not fixed)

The lifecycle-gc README gives the mutant's situation as "a completion tick
before the fold has moved HEAD off a rewritten input". The completion gate
never reads HEAD: it resolves the commit-prefix listing. In this run the
situation was a completion tick while the ack-open hour was unsealed.

## Options (owner decision)

A. **Widen the property's evidence to the open-bucket gate.**
   `served_after_completion` stops skipping unjudgeable buckets and unions the
   derived ack-open hours, as the pass does. It keeps the result of
   `bucket_erasure_completion` for those buckets, which blocks every in-scope
   request.
   - What it touches: only the evidence read; the production guard is unchanged.
   - Effect: the property then also carries clause 1 of
     `CompletionCoversEveryBucketOpenAtRequest`. The reproduction test and the
     workload would go false.
   - Docs to change: the doc comment, traceability.md rows 32 and 34, and
     possibly the tsv `completion-ignores-open-bucket` row.
   - Cost: for those buckets the check is the gate's own predicate run again,
     with no independent evidence. A derived hour with no commit yet would be
     reported as "a pre-rewrite object is still served" when no object exists.
   - A narrower variant A' covers listed unjudgeable buckets only. A listed
     bucket does hold pre-rewrite objects that queries serve, which fits the
     message literally, and the workload's ack-open hours are always listed
     because the driver writes all the time.
   - Either way this reverses a recorded decision about what the property
     means.

B. **Narrow the mutant to the TLA mutant.** Split
   `BucketErasureCompletion.blocked` into served and not-yet-judgeable (a
   ravel-maintain change), and have the mutant drop only the served part.
   - What it touches: production logic at the guard site.
   - Effect: code matches `CompleteIgnoresServedSet` exactly.
   - Cost: in lifecycle-gc the mutant then almost never changes behaviour. A
     sealed bucket still serving after its own tick's rewrite needs a late
     commit, an unwritable object or a lost race. The mutant would be
     unit-test-only (Part B) unless the workload gets a new forced situation.

C. **Retarget the mutant in ADR-4326.** Keep the mutant as it is (it is really
   CompleteIgnoresServedSet ∧ CompletionIgnoresOpenBucket) and give the open
   clause its own SUT assertion at the completion step, for example "erasure: a
   completion is written only once every bucket open at its acknowledgement is
   judged".
   - Change the tsv so this mutant targets the new property, or accept either.
   - Needs an ADR-4326 amendment (decision 3 says no mutant for that invariant)
     and a tsv format change if the mutant gets two properties.

D. **Accept the workload limit.** Record that the property is covered by the
   unit test, and that in lifecycle-gc this mutant is caught by the driver's
   `erasure: an erased subject is never returned after its request`, as it was
   here and in #4338 loop 2. This changes the mutant's tsv mapping or marks it
   as having no workload property. That is also ADR-level.

Recommendation: C, or A' if the owner prefers no ADR change.

- C keeps one assertion per TLA invariant, which traceability.md is built on,
  and turns today's "no assertion carries the open-hour union" gap into a real
  SUT check.
- A' is the smallest change, but it folds a second invariant into this
  property.

## Left on gcp2

- Worktrees, as builder, from `/home/builder/src`:
  - `/home/builder/wt/4552` (detached at d23dbb245, clean).
  - `/home/builder/wt/4552-ut` (detached at e877e52ec; its `target/` is
    6.9G).
- Branch `r4552-ut` in `/home/builder/src`.
- Bundles `/home/builder/r4552-ut.bundle` and `r4552-ut2.bundle`.
- Logs: `/home/builder/logs/4552/`. Scripts: `img.sh`, `lr.sh`, `ut.sh`,
  `ev.sh`, `ev2.sh` and `ev3.sh`.
- On the Mac: worktree `/Users/pmoust/nofire/store-wt-4552`, branch
  `fix/4552-served-set-property`, two local commits, not pushed.
- Images: `ravel-antithesis-{server,driver}:lifecycle-gc-mutants` are retagged
  to the d23dbb245 builds. The 7-day-old 30bba371b images are now untagged and
  have not been removed.
- The compose project `ravel-antithesis-lifecycle` is down with
  `--remove-orphans`. `local-run.sh:160` itself runs `docker compose down -v`
  on its own project before starting, the same trap as catalog-maintenance.
