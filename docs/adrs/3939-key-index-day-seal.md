# ADR-3939: bound the key-index leaf build by sealing the tail at a UTC day

Status: Accepted (2026-11-08). Issue: #3939 (epic), #3927.
Date: 2026-11-07
Builds on: ADR-3692 (decision 5, the #3704 and pass-end slice amendments,
the Stage 1 amendment), ADR-0063 (hour-range parts, sealing policy, the
rejected delta log), ADR-2707 (per-part key-index leaves).
Amends: ADR-0063's sealing policy, for a tenant and signal folded with the
leaf switch on only.

## Context

A key-index leaf names the exact part it indexes: its header carries the
part's `part_blake3`, and the reader rejects a leaf whose hash differs from
the part in HEAD (`PartBindingMismatch`, kidx.rs). The tail part is
re-encoded on every advancing fold, so it gets a new hash every time, and
none of its previous leaves stay valid. The fold therefore rebuilds the
tail's whole leaf set on each advance (fold.rs, `build_part_key_index`;
the part ref is rebuilt with only the new refs).

ADR-0063 accepted a tail rewrite on each fold because the tail is bounded
by `snapshot_part_max_entries` (250,000 entries). That bound does not bound
the leaf build. The leaf build's cost is in key pairs, and at Stage 1's
rate a tail reaches 250,000 entries only after years: spans adds about 34
entries and 833,333 trace_id pairs an hour, logs about 23 entries, 4,166,667
request_id pairs and 833,333 trace_id pairs (box D,
https://github.com/NOFireAI/ravel/issues/3692#issuecomment-6700944105).
"A tenant under the cap is one tail for good" (ADR-3692 decision 5).

Box D measured one advance over a 25-hour tail: logs ran 27 build passes,
read 1.30 GB of sections and wrote 1.37 GB of leaves in 305.8 s, with a
fold peak of 1.99 GB; spans ran 3 passes and wrote 0.23 GB in 182.7 s. The
Stage 1 amendment projects 750 passes and 39.3 GB of logs leaf bytes per
advance at a 30-day tail (39.2 GB from the pair rates at 10.9 B a pair,
the basis of the cost table below). Each pass also ends a slice, so the advance
multiplies slices (request_id went from 5 to 27 in one advance), and a
multi-key lookup pays per slice it touches.

The read side constrains the fix. The router charges `1 + min(k, 2^bits,
B_run)` GETs per (part, slice) a key set touches, admits parts under
`L_max` = 2,048 leaf GETs and `P_max` = 1,024 parts, and leaves the rest to
the tier-1 probe. A lookup without a time range touches every part. So
splitting the tail into more indexed units bounds the write side and
multiplies the read side, and one index over the whole tail does the
reverse. There is no option that is cheap on both for a lookup with no
time range. A lookup with a time range touches only the parts its window
overlaps, one or two for a 1-hour window, under any option here.

```mermaid
flowchart LR
  subgraph today[Today: one tail part]
    T1[tail part, hours 0..H] -->|every advance| R1[rebuild all leaves: O(H) bytes, G passes, G slices]
  end
  subgraph proposed[Proposed: tail sealed at the UTC day]
    D1[day part, sealed] --> L1[leaves written once]
    D2[day part, sealed] --> L2[leaves written once]
    TT[tail part, today only] -->|every advance| R2[rebuild today's leaves: at most 24 h of pairs]
  end
```

## Decision

1. **Seal the tail at the UTC day when the leaf switch is on.** With
   `build_key_index_leaves` on, `partition_parts` also seals a part at
   every UTC day boundary of `ingest_hour_bucket` (hour divisible by 24),
   as well as at the entry threshold. A part therefore never spans two
   UTC days. The rule is a pure function of the entry set, like today's,
   so every fold of the same input produces the same parts. With the
   switch off, nothing changes.
2. **A sealed day part keeps its leaves.** It re-encodes to the same hash
   on later folds (fold.rs, sealed parts keep their bytes), so its leaf
   refs carry over and its leaves are never rebuilt unless the part
   itself changes (a late commit, compaction, retention or erasure into
   that day), in which case only that day is rebuilt. The fold forwards
   an unchanged part's ref, `key_index` included, by reference
   (fold.rs, `existing_by_blake3`). One rebuild per day remains: the part
   sealed at the day boundary is encoded with its own last ingest hour as
   its watermark, while the last tail carried the fold's watermark, so the
   two can hash differently, and the sealed day's leaves are then built
   once more at sealing. At a tenant's first day boundary there is a
   second reason: a lone part is encoded with `min_hour` 0 (the
   single-part override, fold.rs), and once a second part exists the
   sealed one takes its real first hour. D1 either makes the two encodings agree or
   counts that rebuild; either way it is one day's leaves once.
3. **No persistent format changes.** The proto, the kidx layout and the
   object key layout stay as they are. A part's leaves still bind its
   exact hash and still tile `[0, u64::MAX]` per field.
4. **Rollout.** The first fold with the switch on under this rule
   re-partitions a tenant's existing tail into day parts and builds their
   leaves, once, under the pair budget per part (all retained history, not
   only the tail: see the multi-day amendment). docs/catalog-and-mvcc.md
   ("Per-part key-index leaves") says only that the processes folding one
   tenant should agree on the switch. Under this rule a disagreement is
   far more expensive than today. A switch-off fold re-partitions the
   whole retained entry set back into entry-count parts: every part gets
   a new hash, is re-encoded and PUT, and loses its leaf refs. The next
   switch-on fold splits it into days again and rebuilds every day's
   leaves, which at a 30-day logs tenant is the full 39.2 GB and about 750
   passes, on every pair of folds. Today the same disagreement costs only
   the tail. Both rules still produce valid snapshots of the same entries,
   so the cost is waste, never wrong rows. Two things guard against it.
   D1 makes agreement a stated requirement in docs/catalog-and-mvcc.md,
   with the order: every folder of a tenant runs this version before the
   switch is turned on for that tenant. And D1 makes a disagreement
   visible: a switch-off fold whose previous HEAD carries leaf refs
   reports it in the fold report and logs a warning. It is not refused,
   because turning the switch off on purpose looks the same.

Decisions 5 and 6 (the carry source, and retention under leaf-bearing
parts) are added by the multi-day amendment below.

### What the seal costs and saves (predicted)

Per-hour rates from box D, 10.9 B of leaf per pair, the 8,000,000-pair
budget split per field, a 30-day retention window. The 100-key column is
for a lookup with no time range on the densest key field (spans trace_id,
logs request_id), charged one GET per touched slice plus one per key's
bucket.

| seal span | signal | passes per advance, worst | leaf bytes per advance, worst | indexed parts at 30 days | 1-key GETs, no range | 100-key GETs, no range | basis |
|---|---|---|---|---|---|---|---|
| none (today) | spans | 75 | 6.54 GB | 1 | 2 | about 175 | predicted from box D rates |
| none (today) | logs | 750 | 39.2 GB | 1 | 2 | about 200 | predicted from box D rates |
| UTC day (this ADR) | spans | 3 | 0.22 GB | 30 | 60 | about 3,090, over `L_max`: about 19 of 30 days covered | predicted from box D rates |
| UTC day (this ADR) | logs | 25 (26 scaled from box D) | 1.31 GB | 30 | 60 | about 3,780, over `L_max`: about 16 of 30 days covered | predicted from box D rates |
| 6 hours | spans | 1 | 0.05 GB | 120 | 240 | 100 keys in one slice bridge to 64 runs or decline | predicted from box D rates |
| 6 hours | logs | 7 | 0.33 GB | 120 | 240 | about 12,840, about 19 of 120 parts covered | predicted from box D rates |
| 1 hour | logs | 2 | 0.05 GB | 720 | 1,440 | 100 keys in one slice bridge or decline; at most 20 parts covered | predicted from box D rates |

The leaf bytes written per hour drop from O(tail) to O(the day so far):
the mean advance writes about half the worst figure. Days not covered by
`L_max` go to the tier-1 probe, so they cost time and GETs, never wrong
rows. The table costs the advance only; the multi-day amendment costs the
re-encode of a sealed day and retention, and its router ceiling covers
every day.

## Rejected alternatives

1. **Leaf generations per advance, merged size-tiered (ADR-2707 leaf
   format version 3).** Each advance writes leaves for its new entries
   only, as a new generation of the same part, and a merge keeps about
   log_T(H) generations. It is the cheapest on writes, but every leaf
   would stop binding an exact part hash and bind a part prefix instead,
   which removes the reader's one check that a leaf belongs to the part
   it is probing. A late commit or compaction that inserts an entry
   mid-part shifts ordinals and invalidates every generation, so the
   merge falls back to a full rebuild exactly when the tail changes most.
   It is ADR-0063's rejected delta log moved into the index: more read
   complexity and a format bump. It also does not cut read cost: a
   lookup pays per generation as it pays per part. Lost on format risk
   and on giving up the exact binding.
2. **Seal by pair count.** The cost unit is right, but the pair count is
   not on a `SnapshotEntry`; it comes from the KEY_IDX sections, which a
   fold with the switch off never reads. The partition would then depend
   on data the snapshot does not carry, and a repair or a reconcile that
   reads different sections could split differently. Lost on determinism.
3. **Lower `snapshot_part_max_entries`.** It would re-partition every
   tenant, switch on or off, and entries are the wrong unit: a tenant with
   few large objects still builds an unbounded tail. Lost on unit and
   blast radius.
4. **Size-tiered merge of sealed day parts (days into weeks).** It would
   cut the parts a no-range lookup touches from 30 to about 4 to 8, at a
   re-leafing of each entry once per tier and a periodic large merge
   (a week of logs is about 9.2 GB of leaves). Deferred, not rejected:
   it is the next step if Stage 2 shows no-range key-set lookups beyond
   two weeks matter, and it stacks on decision 1 without a format change.
   The multi-day amendment designs it as a two-way daily merge and gates
   its dispatch on a pre-registered Stage 2 band.
5. **Keep the single tail and fix slice multiplication only.** Stable
   slice boundaries would stop 5 slices becoming 27, but the advance would
   still read, hold and rewrite the whole tail's leaves. Lost on not
   bounding the advance.

## Consequences

- An advance rebuilds at most one UTC day of leaves. Logs: at most 26
  passes (25 by the pair rule, 26 scaling box D's measured 27 by 24/25)
  and 1.31 GB of leaf bytes. The fold peak is a base plus the held
  previous-leaf bytes, and only the second term scales with tail hours:
  box D's 1.99 GB at 25 hours less one hour of logs leaves (about
  0.055 GB) predicts about 1.93 GB for a full-day tail. That is under the
  2 GB RSS band with under 4% headroom, and ADR-3692's Stage 1 amendment
  reads the same measurement as at the line: a tenant ingesting about 5%
  more leaf pairs than box D misses it (restated to about 0.85 GB once the
  carry is held per pass, multi-day amendment). Spans:
  at most 3 passes and 0.22 GB.
- A lookup with a time range inside one UTC day touches one part, as
  today. A longer window touches one part per day it overlaps, and the
  router charges each part on its own, so splitting multiplies the read
  side here too: a 7-day `request_id IN (100 keys)` on logs goes from
  about 200 leaf GETs on one tail to about 7 x 126 = 882, and a single
  key from 2 to 14. A 1-hour window that crosses midnight goes from 2 to
  4. The multi-day amendment restates every multi-day lookup under its
  router ceiling.
- A lookup with no time range touches one part per day. A single key costs
  2 GETs per day (60 at 30 days). A 100-key list over 30 days exceeds
  `L_max` after about 16 to 19 days and leaves the rest to the tier-1
  probe (removed by the scaled ceiling the multi-day amendment adopts). Spans trace-by-ID lookups are mostly single keys.
- A no-range resolve reads one part object per day instead of one part.
- The switch-on and switch-off partitions differ, as the refs already do,
  so the existing rule that folders agree on the switch now also governs
  the partition.

## Open question

How much do key-set lookups with no time range over more than about two
weeks matter? The owner approved this decision without settling it, so
rejected alternative 4 (tiering day parts into weeks) stays deferred. D3
times a no-range 100-key lookup over every day part the run holds (A7),
and that figure decides whether alternative 4 becomes a follow-up.
Answered in the multi-day amendment: the owner said multi-day ID lookups
matter, and the week merge is designed and gated on a pre-registered band.

## Acceptance (pre-registered; a figure outside its band is a miss)

Rows W1 to W8 and F1 to F3 in the multi-day amendment extend this table.
A5 and A7 run on the multi-day states, and A7 at a reduced rate per day,
as the harness-state amendment states.

| check | target | miss | source |
|---|---|---|---|
| A1. advance at a full-day tail, logs, switch on | at most 26 build passes, at most 1.4 GB leaf bytes written, fold peak RSS at most 2.0 GB | more than 26 passes, over 1.4 GB, or over 2.0 GB RSS (predicted peak about 1.93 GB). An RSS miss with the passes and bytes in band is not fixed inside this decision: it opens a follow-up on a 6-hour seal span (costed above) or on the held previous-leaf rule | predicted; box D rates, comment 6700944105 |
| A2. advance at a full-day tail, spans | at most 3 passes, at most 0.25 GB leaf bytes | more than 3 passes or over 0.25 GB | predicted; box D rates, comment 6700944105 |
| A3. sealed days keep their leaves | from the second fold after day d seals, an advance issues 0 leaf PUTs and 0 previous-leaf GETs for day d's part | any leaf PUT or GET for a sealed day it did not change, after its sealing fold | predicted |
| A4. partition determinism | two folds of the same entry set produce byte-identical HEADs; the switch-off partition is unchanged | any difference | predicted |
| A5. no-range lookup, 7 days of day parts, logs request_id IN 100 | every day covered; GETs at most 7 x 126 | any day uncovered | predicted |
| A6. ADR-3692 row 18 (key-set lookup time) on the day-sealed tail | as row 18 | as row 18 | predicted; issue #3918 |
| A7. no-range lookup over every day part the run holds, logs request_id IN 100 | at most 126 GETs per day part; days covered equal to min(days held, floor(2,048 / GETs per day)) | over 126 GETs per day part, or fewer days covered than that | predicted from box D rates |

## Plan

| task | crates | depends on | risk |
|---|---|---|---|
| D1 day seal in `partition_parts` behind the switch, carry-over of sealed day parts' refs, tests A3 and A4; the fold report also carries `key_index_pair_budget` and `key_index_field_pairs` (#3948), so the harness can bound ADR-3692 row 7's `G` (it reads not computable until then); the decision 4 guards: a fold report field and a warning for a switch-off fold over a HEAD with leaf refs, and docs/catalog-and-mvcc.md stating that the folders of a tenant must agree on the switch and run this version before it is turned on, with the day seal under the sealing policy | ravel-catalog (fold), ravel-cli (report), docs/catalog-and-mvcc.md | none | high |
| D2 docs: the operator guide's cost section (D1 updates catalog-and-mvcc.md with the behaviour) | docs | D1 | low |
| D4 the investigations harness reads `key_index_pair_budget` and `key_index_field_pairs` and bounds row 7's `G` from them | benchmarks/investigations | D1 | low |
| D3 Stage 2 run on real S3: rows A1 to A7, ADR-3692 rows 7, 8, 11 and 18 | benchmarks, docs | D1, D4, #3917, #3918 | low |

The multi-day amendment adds tasks R1, C1, R2, H1 and D3', gates the week
merge, edits D1's spec, and orders the work.

## Amendment (issue #3939, multi-day lookups): the carry source, retention under leaf-bearing parts, and the week merge gate

<!-- amendment-applies: sections="Decision|What the seal costs and saves (predicted)|Rejected alternatives|Consequences|Open question|Acceptance (pre-registered; a figure outside its band is a miss)|Plan" pointer="multi-day amendment" -->

Date: 2026-11-08. Owner decisions of the same day: the router ceiling
scales with the key set (ADR-3692, the key-set ceiling amendment), retired
hours are hidden by the reader with no visibility lag, and the week merge
is decided on figures below.

### Two costs the decision did not count

**A dirty sealed day is rebuilt from sections.** The incremental build's
carry source is `PreviousTail::from_head` (crates/ravel-catalog/src/fold.rs),
which takes the previous HEAD's last part only. A sealed day that a
retention tombstone, a late compaction record or an erasure rewrite record
dirties is not the last part, so its leaves are rebuilt from the KEY_IDX
sections: for a logs day, 26 passes x 2 GETs x 552 entries = 28,704 ranged
GETs and 26 x 552 x 2.19 MB = 31.4 GB of section bytes (2.19 MB a read is
box D's 1,301,909,733 section bytes over 594 reads, comment 6700944105).
The retention frontier advances one hour per hour and each tombstoned hour
marks its covering part dirty (docs/catalog-and-mvcc.md,
"Retention-frontier reconcile"), so at a 30-day retention the oldest day
is re-encoded up to 24 times as it retires: up to 750 GB of section reads a
day for logs, predicted. The decision's bound holds for the advance and
not for the fold. Today's single tail avoids this only by being the last
part.

**Retention re-leafs the oldest part hour by hour even with a carry.** With
the carry generalised (decision 5), a retention re-encode costs one GET per
previous leaf (31 for a logs day) and rewrites the surviving leaves: on
average 0.65 GB per retired hour, 15.7 GB a day, about the advance's own
16.4 GB. Under a week part it would be 84 x 9.17 GB = 770 GB per retired
week. Decision 6 removes both.

**On the pass count.** The stated rate of 4,166,667 request_id pairs an
hour is 100,000,000 / 24 rounded up, so a logs day holds 100,000,000
pairs and the pair rule gives `ceil(100,000,000 / 4,000,000)` = 25 passes.
Box D's scaling, 27 x 24/25 = 25.9, gives 26. A1's band of 26 is the
conservative one, and this amendment's figures use 26.

### Decision 5. The carry source is every overlapping previous part, held per pass

The build takes as carry sources every part of the previous HEAD whose hour
range overlaps the part being encoded, each with its own map from previous
ordinal to new ordinal by data object key, so a re-encoded sealed day
carries from its own previous leaves and a merged part carries from each
of its sources. Entries are carried only when every declared field's
sources cover them, as today. Previous leaf bytes are no longer fetched
up front and held for the build: a leaf is fetched when a pass reaches its
`key8_lo` and dropped once the pass range has passed its `key8_hi`. Passes
ascend in key8 and a field's leaves are kept in key8 order, so this is the
existing `feed_carried_key_index` walk with a window. Held bytes fall from
the whole previous leaf set (1.31 GB at a logs day) to at most two slices
per field per source (2 x 2 x 43.6 MB = 0.17 GB for a logs day advance;
43.6 MB is a 4,000,000-pair slice at 10.9 B). The leaves a build writes
stay the leaves the one-call build writes at the same budget, as
`the_leaves_are_the_same_carried_from_scratch_and_after_a_restart` pins.
Predicted fold peak at a full-day logs tail: box D's 1.99 GB less its held
previous leaves (1,365,715,342 B x 24/25 = 1.31 GB) plus the window (0.17
GB) is about 0.85 GB, which restates the first Consequence's 1.93 GB.

### Decision 6. Retention under a leaf-bearing sealed part: the reader hides retired hours, the part drops at whole-day grain

`SnapshotPartRef` gains an additive field 9, `repeated uint32
retired_hours`: the ingest hours inside `[min_hour, watermark_hour]` whose
retention tombstone the fold has observed while the part still holds their
entries. Strictly ascending and inside the range, or `validate_head`
rejects the HEAD. Absent means none. Only a sealed part that carries
`key_index` refs receives the field; a part without leaves is re-encoded
as today, and with the switch off the field is never written.

The fold: when the frontier reconcile finds a tombstone for hour `h` whose
covering part is sealed, carries leaf refs, and whose UTC day is not yet
fully tombstoned, it adds `h` to that part's `retired_hours` in the ref it
carries forward (same key, blake3 and leaf refs; no part or leaf PUT) and
publishes under the one HEAD CAS. When every hour of a UTC day that the
part holds is tombstoned, a day part leaves HEAD with no PUT; a longer part
is re-encoded without that day through decision 5's carry and keeps only
other days' hours in `retired_hours`. A compaction record, an erasure
rewrite record or a late commit still dirties the part (a late commit only
into a held-open hour; see the harness-state amendment) and re-encodes it in
the same fold, so an erasure never lags; a re-encoded part drops its
retired hours' entries for good and its new ref carries none.

The reader: in `SnapshotWindow::extract_into`
(crates/ravel-catalog/src/snapshot_resolve.rs), an entry whose
`ingest_hour_bucket` is in its part ref's `retired_hours` is skipped before
a `SegmentRef` or `SegmentPartOrigin` is made for it. Both tiers consume
that output, so the tier-1 probe never opens the entry's object and the
tier-2 router never routes it; a leaf ordinal with no segment is the
router's existing no-origin arm. Ordinals do not shift, so leaves, postings
and per-segment statistics stay bound; the ref is read from the pinned
HEAD, so the decoded-part cache needs no invalidation. Nothing on the read
path reads a clock or the tenant's retention: the source of truth is the
tombstone objects in the commit buckets, observed by the fold and recorded
in HEAD, which is in object storage. Visibility changes at the fold that
observes the tombstone, as today. The physical delete lags: the sweep's
HEAD-reachability gate holds a retired hour's objects until the part
drops, up to 24 h beyond today on top of the protection horizon; the owner
accepted this.

Version: a HEAD whose part refs carry any `retired_hours` is written with
`format_version` 2; a HEAD with none is written with 1, byte-identical to
today. `validate_head` accepts 1 and 2, rejects a 1 carrying
`retired_hours` and a 2 carrying none, so the version is a function of the
content and two folds of the same input write the same bytes. Fields 6, 7
and 8 took no bump because an older reader ignoring them is only slower;
an older reader ignoring field 9 would serve a retired hour for up to 24 h,
which the owner excluded. An older reader meeting version 2 rejects the
HEAD (crates/ravel-catalog/src/snapshot_format/head.rs:29) and falls back
to the listing resolve (crates/ravel-catalog/src/snapshot_resolve.rs:704),
which honours `retire.tmb` tombstones (crates/ravel-catalog/src/catalog.rs:3613):
slow and right. An older folder fails loudly and never overwrites it
(fold.rs:3527, ADR-0066 decision 2); an older `--rebuild-key-index` repair,
which republishes a cloned HEAD (key_index_rebuild.rs:268), refuses at
decode instead of stripping the field. Writing 2 only on content confines
that fallback to tenants with a partially retired leaf-bearing part, the
tenants an un-bumped older reader would misread; an unconditional bump
would send every older reader on every tenant to the listing resolve for
the whole mixed window.

This is a persistent format change and R2 runs the format-change
procedure: this amendment is its ADR text (the HEAD is a class B derived
object; there is no migration, since a HEAD returns to version 1 when its
retired hours empty or the part re-encodes; the dual reader is {1, 2} with
the writer emitting 2 on content only). Checksum coverage is unchanged:
the field sits inside the protobuf HEAD, crc32c-guarded in transit, and
the parts it names stay blake3-bound. `HEAD_FORMAT_VERSION` becomes a
single-sourced pair (`HEAD_FORMAT_VERSIONS = [1, 2]` and
`head_format_version_for`) read by the writer, the reader gate and
`ravel-cli catalog inspect`, which prints the version and each part's
retired hours. Property and corrupt-input tests cover both versions and
the two version-content mismatches. The changelog fragment names logs,
spans and metrics, since HEAD carries every signal's catalog
(`check-format-bump-release-note.sh`). docs/catalog-and-mvcc.md's rollout
rule still says every reader and folder of a tenant runs this version
before its leaf switch turns on; with the version gate that rule protects
cost, not correctness.

### The router ceiling

ADR-3692's key-set ceiling amendment replaces `L_max` = 2,048 with
`P_max x (1 + min(k, B_run))` and admits a part only when its worst case is
at most `2 x entry_count`. Under it every multi-day lookup in the
Consequences is covered: 30 day parts x 126 = 3,780 leaf GETs against a
ceiling of 1,024 x 65 = 66,560, and 126 against a day part's tier-1 cost of
1,104.

### What multi-day lookups cost, restated (predicted)

Day parts, logs request_id, after #3917 (16 bucket reads in flight per
part) and with 16 parts probed concurrently. A part with `S` slices and 100
keys costs `T(S) + 100` GETs, `T(S) = S x (1 - (1 - 1/S)^100)` slices
touched; T(26) = 25.5, T(175) = 76, T(750) = 94. "Today" is the single
30-day tail with the switch on.

| lookup | today (one tail) | day parts | round trips, day parts | basis |
|---|---|---|---|---|
| 7 days, 100 keys | T(175) + 100 = 176 | 7 x 126 = 882 | 1 + ceil(126 / 16) = 9 (today 12) | predicted; comment 6700944105 rates |
| 7 days, 1 key | 2 | 14 | 2 | predicted |
| 1 hour across midnight | 2 | 4 | 2 | predicted |
| 30 days, no range, 100 keys | T(750) + 100 = 194 | 30 x 126 = 3,780, all covered | 1 + 8 x 2 = 17 | predicted; ceiling 66,560 |

The GET counts cannot be lower with per-part leaves: a leaf's prefix tells
the router only which buckets are empty, and a dense field's buckets are
never empty (about 940 keys a bucket for a logs day's request_id), so each
key costs one bucket GET per part in the window. Round trips do not grow
with the part count while parts are probed concurrently.

### Rejected alternative 4, designed and gated

The week merge is designed as a two-way daily merge: a completed day is
*ready* once it is out of the fixed reconcile window (`last >= 24 x (d + 1)
+ fold_reconcile_window_hours`, `last` the last entry's hour); the ready
days of the current week form one part, each not-yet-ready completed day is
a day part, a week whose seven days are ready is one part. Through decision
5 the merge reads no section and holds at most 2 sources x 2 fields x 2
slices x 43.6 MB = 0.35 GB. Predicted: the 7-day 100-key lookup falls from
882 to a mean of 511 (worst 592) GETs by weekday, the 30-day one from 3,780
to 1,130; round trips rise from 9 to 12 because the week part carries 176
GETs; leaf writes rise by 35.4 GB a week of merges plus 27.5 GB a week of
day-grain re-encodes of the retiring week part, +62.9 GB on the advance's
115 GB (+55%). At about 11 KB a leaf GET the merge saves about 4 MB a
lookup for 9 GB a day of writes, a break-even near 2,250 such lookups per
tenant per day. The decision is design only: its partition rule is fixed
here so D1's policy value can take it, and the task is dispatched only on
a miss of the flip rows below.

### Decision 4, corrected: the first switch-on fold re-partitions all history

Decision 4 says the first switch-on fold re-partitions "a tenant's existing
tail". It re-partitions the whole retained entry set: every part spanning
more than one UTC day is re-encoded, which at the default
`snapshot_part_max_entries` of 250,000 against box D's 552 logs entries a
day (23 an hour) is every part, and every day's leaves are built once. For a 30-day
logs tenant that is about 39.2 GB of leaves and 750 passes in one fold,
the cost decision 4 gives a switch disagreement, paid once.

### Decision 2, clarified: a carried part keeps exactly its refs

A part carried by reference keeps exactly the leaf refs it had; the fold
never builds leaves for a part it carries. A day sealed while the metrics
erasure gate held, or before a key-index field was declared, stays without
that field's leaves until `ravel-cli catalog fold --rebuild-key-index`
attaches them, as for any sealed part (ADR-2707 decision 4, Lifecycle). The
fold report counts such parts (`key_index_parts_missing_declared_leaves`)
and logs one warning per fold naming the fields, so the gap is visible.
Retrying the build in the fold was rejected: a field whose build ends
with no ref (over the metrics byte cap, every entry uncovered, no pairs)
would re-read the day's sections on every fold, about 28,704 ranged GETs
and 31.4 GB for a logs day, which A3 forbids. Declaring a key-index field
for a tenant with the switch on is therefore followed by the repair over
its sealed days. The repair builds in one pass today, about 6.8 GB of
pairs for a logs day, over the 2 GB band; #4018 gives it the pair budget.

### Acceptance (added rows; predicted, a figure outside its band is a miss)

W2 is measured on the C1 binary, W5 and F2 on the 30-day state at a
reduced rate per day, and F3 on both the 7-day and the 30-day state, as the
harness-state amendment states.

| check | target | miss | source |
|---|---|---|---|
| W1 advance at a full-day tail, logs, decision 5 landed | at most 26 passes, at most 1.4 GB leaf bytes, fold peak RSS at most 1.2 GB | more than 26 passes, over 1.4 GB, or RSS over 1.2 GB | predicted; comment 6700944105 (1.99 GB less 1.31 GB held plus 0.17 GB) |
| W2 one tombstoned hour in a sealed logs day, decision 5 alone | 31 previous-leaf GETs, 0 section GETs for unchanged entries, at most 1.31 GB written | any section GET for an unchanged entry | predicted; fold.rs `PreviousTail::from_head` and `feed_carried_key_index` |
| W3 one tombstoned hour in a sealed logs day, decision 6 | 0 part PUTs, 0 leaf PUTs, `retired_hours = [h]`; a resolve over h yields 0 segments on either tier from that fold; the part leaves HEAD with 0 PUTs at the day's last tombstone | any PUT before the drop, any segment of hour h | predicted |
| W4 7 days, 100 keys, logs request_id, day parts | at most 882 leaf GETs, 7 of 7 days covered, p50 within ADR-3692 row 18's bound | over 882, a day uncovered, or over the bound | predicted; comment 6700944105 |
| W5 30 days, no range, 100 keys | 3,780 leaf GETs, 30 of 30 covered, 0 tier-1 probes on those days, `parts_beyond_ceiling` 0 | any day uncovered | predicted |
| W6 7 days, 1 key | 14 leaf GETs, 2 round trips | over 14, or 3 or more round trips | predicted |
| W7 1 hour across midnight | 4 leaf GETs | over 4 | predicted |
| W8 determinism | as A4, and `retired_hours` identical across two folds observing the same tombstones | any difference | predicted |
| F1 (flip row) W4's p50 | within row 18's bound | over it: ship the week merge | predicted |
| F2 (flip row) W5's p50 | at most 2 x the same statement's unfolded p50 | over it: ship the week merge | predicted |
| F3 (flip row) leaf GETs per day part on W4 and W5, and S3 throttling | at most 139 (126 + 10%), no `SlowDown` | over 139, or any `SlowDown`: re-derive before deciding | predicted |

### Plan (added and edited rows)

H1 splits into H1a and H1b, as the harness-state amendment states.

| task | crates | depends on | risk |
|---|---|---|---|
| D1 as specced, with a policy value instead of a bool and a report line stating the from-sections cost of a dirty sealed day as owned by C1 | ravel-catalog, ravel-cli, docs | #3917 (same crate) | high |
| R1 the key-set ceiling and per-part tier-1 admission (ADR-3692's key-set ceiling amendment) | ravel-query, ravel-catalog (kidx.rs constants), docs/query-engine.md | #3917 and D1 (same files and crate) | low |
| C1 decision 5 | ravel-catalog (fold.rs), docs/catalog-and-mvcc.md | R1 (same crate) | medium |
| R2 decision 6: proto field 9, HEAD version 2 on content under the format-change procedure (dual reader {1, 2}, version-content check, both-version property tests, inspector, changelog fragment), fold frontier handling, resolve filter, rollout rule | ravel-proto, ravel-catalog, ravel-cli, ravel-sql and ravel-query (both classify UnsupportedHeadVersion against HEAD_FORMAT_VERSION and pin version 2 as the newer case in their tests), ravel-server tests, docs/catalog-and-mvcc.md, docs/consistency-model.md, changelog.d | C1 (fold.rs) | medium-high, alone |
| H1 harness rows W1 to W8 and F1 to F3 | benchmarks/investigations | D1 | low |
| D3' Stage 2 run: A1 to A7, W1 to W8, F1 to F3, ADR-3692 rows 7, 8, 11, 18 | benchmarks, docs | R2, H1 | low |
| M1 the week merge, dispatched only on an F1 to F3 miss | ravel-catalog, docs, its own ADR | C1, R2, D3' | high |

## Amendment (issue #3939, harness states): how Stage 2 builds day parts, which binary measures W2, and W5 and A7 at a reduced rate

<!-- amendment-applies: sections="Acceptance (pre-registered; a figure outside its band is a miss)" pointer="harness-state amendment" -->
<!-- amendment-supersedes: phrase="a late commit still dirties the part" pointer="harness-state amendment" -->

Date: 2026-11-09. Recorded before any Stage 2 figure exists, so it changes
what is measured and how, not a result.

**Day parts by ingest hour.** A day part is a UTC day of ingest hours,
which the server's clock assigns, so a corpus loaded through the server in
one sitting lands in one or two days. Stage 2 builds its multi-day states
with the hour-stride lane of `ravel-cli load` (`--ingest-hour-stride 1
--ingest-hour-start`), which drives the production log and span routers
under an offset clock (services/ravel-cli/src/load/hour_stride.rs), into a
fresh tenant, one fully acknowledged batch per shard per hour, every hour
strictly in the past, and the investigations harness already uses it for
its many-parts state. One `catalog fold --writers-stopped
--key-index-leaves` then builds every day's leaves once (decision 4,
corrected), and the advance rows run on a tail loaded through the server
after it. The lane writes one object per shard per hour: 192 entries a day
at 8 shards, against box D's 552. The leaf GET figures depend on slices
per day part, not on entries, and the run stamps both. Days covered do
depend on entries: ADR-3692's key-set ceiling amendment admits a part only
when its worst case is at most 2 x its segments in the routed snapshot, so
a harness day part with no time range admits at 384 against box D's 1,104,
and the headroom over the predicted 126 falls from about 9 x to about 3 x.
An uncovered day on these states is read against 384 first.

**Two states.** A 7-day state at box D's logs rate, for A5, W4, W6, W7, F1
and F3 on W4. A 30-day state at a reduced rate per day, for A7, W5, F2 and
F3 on W5,
because 30 days at box D's rate cost about 4 hours of fold and about 20
hours of corpus generation, which no single run's deadline holds. At the
reduced rate the per-day figures follow the slices the run stamps: with
`S` the most slices one day part carries for `request_id` (the harness's
`stamp.slices` figure) and `T(S)` as defined under "What multi-day lookups
cost, restated", A7 and W5 target at most `T(S) + 100` leaf GETs per day
part and `30 x (T(S) + 100)` in all, and F3's bound on the 30-day state is
`1.1 x (T(S) + 100)` per day part. The state must give `S` of at least 3,
so the multi-slice term is exercised; a run with fewer is not-run for those
rows, not a pass. 3,780 and 126 stay the box D instance of the same rule.
A7's days-covered clause, written against the 2,048 ceiling, reads under
ADR-3692's key-set ceiling amendment: every day held is covered while `30 x
(T(S) + 100)` is at most 66,560. F2 is a ratio to the same statement's
unfolded p50 and needs no restatement.

**W2 runs on the C1 binary.** W2's trigger is a tombstoned hour, and after
R2 a tombstoned hour in a sealed day takes the `retired_hours` path and is
not re-encoded. So W2, as written, is measured on the main SHA that carries
C1 and not R2, and the D3' run stamps the fold binary's SHA beside it.
Every other row runs on the R2 build. The mechanism W2 measures, a dirtied
sealed day carried from its own previous leaves, stays reachable on the R2
build through the other dirtying events below; a variant measured that way
records its trigger and does not stand in for W2.

**Decision 6, corrected.** A late level-0 commit is reconciled only in a
bucket that lists a compaction record, tombstone or rewrite record, or in a
held-open hour (crates/ravel-catalog/src/fold.rs `bucket_needs_reconcile`
and `held_open_hours`), so decision 6's "a late commit still dirties the
part" holds for a late commit into a held-open hour. Held-open hours run
from the fold margin up to the watermark, floored at
`fold_reconcile_window_hours` (default 26) below it, so they can reach into
the previous UTC day, whose part is sealed: a late commit there dirties
that sealed day part, as do a compaction record and an erasure rewrite
record anywhere in it.

**Plan.** H1 splits in two. H1a adds A1 to A7, W1 to W8 and F1 to F3 to the
investigations harness's bands, each reporting not-run until its state
exists; it touches benchmarks/investigations only. H1b adds the 7-day,
30-day and retired-hour states to the box scripts. D3' runs W2 on the C1
SHA.
