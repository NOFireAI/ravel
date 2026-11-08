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
   leaves, once, under the pair budget per part. docs/catalog-and-mvcc.md
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
rows.

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
  more leaf pairs than box D misses it. Spans:
  at most 3 passes and 0.22 GB.
- A lookup with a time range inside one UTC day touches one part, as
  today. A longer window touches one part per day it overlaps, and the
  router charges each part on its own, so splitting multiplies the read
  side here too: a 7-day `request_id IN (100 keys)` on logs goes from
  about 200 leaf GETs on one tail to about 7 x 126 = 882, and a single
  key from 2 to 14. A 1-hour window that crosses midnight goes from 2 to
  4.
- A lookup with no time range touches one part per day. A single key costs
  2 GETs per day (60 at 30 days). A 100-key list over 30 days exceeds
  `L_max` after about 16 to 19 days and leaves the rest to the tier-1
  probe. Spans trace-by-ID lookups are mostly single keys.
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

## Acceptance (pre-registered; a figure outside its band is a miss)

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
