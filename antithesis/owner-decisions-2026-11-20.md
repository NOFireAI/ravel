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

(pending)

## 3. #4501: legal hold written mid-pass

(pending)

## 4. #4428: swept-token fallback beside a stale HEAD

(pending)

## 5. #4449: claims-on residual window

(pending)

## 6. Order and priority for the next week

(pending)

## 7. Lines for the human to type

(pending)
