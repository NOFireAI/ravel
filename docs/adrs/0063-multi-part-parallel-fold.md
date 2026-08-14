# ADR-0063: multi-part parallel fold

Status: Accepted

## Context

The catalog's fold (crates/ravel-catalog/src/fold.rs, ADR-0020) turns the per-(tenant, signal) commit layout into a snapshot: one content-addressed part object holding every live entry up to the sealed watermark, named by a small mutable HEAD that is the only CAS-updated object in the design. Every fold cycle (default every 5 minutes) rewrites the entire entry set into one new part and swaps HEAD.

The gap is that this single part has a hard ceiling: the decode-time resource bound `max_snapshot_part_bytes` (256 MiB uncompressed, `DEFAULT_MAX_SNAPSHOT_PART_BYTES` in snapshot_format/mod.rs) over entries of roughly 100-115 raw bytes each caps a part at about 10^6 entries in comfortable territory. The arithmetic: a high-cardinality tenant, or any tenant whose compaction lags, crosses the ceiling in under half a day at 10 TB/day and then folds not at all, degrading every query on that tenant to the 100k-request cold-resolve path. An adjacent cliff: above roughly 0.28 sealed segments per second per shard (1024 segments filling `max_segments` inside one hour), recent hours return `TooManySegments` before compaction can apply; a dead fold makes the degradation worse and longer. The acceptance probe: drive a tenant past 10^6 segments and confirm fold, query, and compaction all continue.

What already exists, and matters, because it makes the escape hatch cheap:

- `SnapshotHead.parts` is already `repeated SnapshotPartRef` (proto/ravel/catalog.proto), and the format pinned this from day one: "v1 writes exactly one part; readers accept N parts (union of entries; the multi-part case is the sharding escape hatch and needs no format change when first used)".
- Every reader already iterates `head.parts` generically: `snapshot_resolve::load_snapshot_parts` fetches parts concurrently (snapshot_resolve.rs:558) and unions their entries; `Catalog::fold`'s `load_previous_entries` concatenates all parts (fold.rs:885); `seal_divergence::verify_seal_divergence` and `ravel-cli catalog inspect` loop over `head.parts` (seal_divergence.rs:169, services/ravel-cli/src/catalog.rs:89). Head validation (snapshot_format/head.rs) already accepts N parts and requires `watermark_hour == max over parts`.
- The postings format already binds to a part *set*: `SnapshotPostingsRef.part_blake3` and `SnapshotPostingsHeader.part_blake3` are repeated, ordinals index the concatenated part entry order. Only the resolver's pruning path artificially restricts itself to `parts.len() == 1` (snapshot_resolve.rs:137).
- Entries are already sorted hour-major: `(ingest_hour_bucket, shard, writer_id, writer_epoch, writer_seq)` is both the part sort order (validated on encode and decode, snapshot_format/part.rs `validate_entries`) and the dedup identity. The identity's leading component being the hour is what makes hour-partitioned parts carry the global no-duplicate invariant for free (see Decision).

What does not exist:

- The fold writes exactly one part, rewriting all live entries every cycle: O(live entries) PUT bytes every 5 minutes, and a hard stop at the decode cap.
- Fold-side I/O is serial: one bucket LIST + record GETs at a time (fold.rs bucket loop), unlike the resolve path which already runs bucket LISTs concurrently under a semaphore.
- Nothing sweeps superseded `csnap`/`npost` objects. The plan (section 4 step 8) hands "unreferenced by the current HEAD and older than the protection horizon" to the GC track; ravel-maintain's sweep has rules for superseded L0s, unreferenced `l1/` parts, orphans, and `idem/` markers, but none for `t/<th>/catalog/`. Multi-part folding increases object count under that prefix, so this ADR pulls the rule into scope.
- The incremental fold lists only hours in `(W_old, W_new]` (`incremental_buckets`, fold.rs:361). A compaction record or retention tombstone that lands in an hour *after* that hour was folded is never applied to the snapshot short of a full rebuild triggered by HEAD corruption. Under a single ever-rewritten part this was masked; under parts that are deliberately not rewritten it must be addressed head-on (Decision, reconcile pass), because "continues to compact correctly" is the acceptance.

Frozen-format posture (format-change skill): the catalog snapshot formats (RCS1 part envelope, HEAD proto, RNP1 postings envelope) and the `t/<th>/catalog/<signal>/...` key shapes are persistent contracts. This design stays inside the contracts' pre-declared extension points: additive proto fields only, no envelope version bump, no new key shapes. The dual-reader analysis is in Consequences.

## Decision

Partition the snapshot into hour-range parts with a sealed/tail split, parallelize the fold's I/O, and keep exactly one CAS pointer: HEAD remains the single mutable object and the single unit of atomic visibility.

### 1. Hour-range-partitioned parts

Each part covers a contiguous, disjoint range of ingest hours `[min_hour, watermark_hour]`. Parts are ordered ascending by range in `SnapshotHead.parts`; the last part is the *tail* and is the only part an ordinary incremental fold rewrites. Because the entry sort order and the dedup identity both lead with `ingest_hour_bucket`, the concatenation of range-ordered parts is exactly the globally sorted entry set, and global duplicate-freedom follows from per-part validation plus range disjointness. No cross-part validation pass over entries is needed; the existing per-part `validate_entries` keeps doing all the work.

Sealing policy: when the tail's entry count reaches `CatalogConfig::snapshot_part_max_entries` (new, default 250_000, about 27 MB raw / 8 MB compressed at the measured 100-115 B/entry), the next fold starts a fresh tail whose `min_hour` is the sealed tail's `watermark_hour + 1`. Splits are always at hour boundaries. A single hour larger than the target produces one oversized part; the decode cap (256 MiB) still bounds it, and `max_segments` (1024 per shard-hour) bounds entries-per-hour at 1024 x shard fan-out, far below the cap for any plausible shard count.

Sealed parts are immutable in the ordinary fold. They are rewritten only by the reconcile pass (point 4) when a compaction record or tombstone lands in their range, and then only the covering part is rewritten.

### 2. Format changes (all additive, no version bump)

- `SnapshotPartHeader`: new field `uint32 min_hour = 8`. `decode_part` additionally validates `entry.ingest_hour_bucket >= min_hour` (the existing `<= watermark_hour` check stays). Absent (proto3 zero) means 0, which is the correct floor for every existing single-part object.
- `SnapshotPartRef`: new field `uint32 min_hour = 6`, so a reader and the fold can route by range without fetching part bytes.
- `validate_head` (snapshot_format/head.rs) gains: when `parts.len() > 1`, parts must be sorted by `min_hour` ascending, ranges must be disjoint (`parts[i].min_hour > parts[i-1].watermark_hour`), and each part's `min_hour <= watermark_hour`. Single-part heads keep v1 semantics unchanged, so every existing stored HEAD stays valid.
- `HEAD_FORMAT_VERSION` stays 1, envelope magic/version `RCS1`/1 stays, `RNP1`/1 stays. The multi-part reader path was declared in the v1 format contract ("readers accept N parts... needs no format change when first used"), so this is the exercise of a reserved capability, not an in-place edit of a frozen meaning. proto field numbers are new, never reused.
- Checksum coverage (format-change step 4): unchanged and complete. The new `min_hour` header field sits inside the part header, which is covered by `header_crc32c` (over magic..header inclusive); part refs live in HEAD, whose upload is crc32c-guarded in transit and whose named parts are bound by blake3 at every use.
- Key layout: unchanged. Parts keep `t/<th>/catalog/<signal>/snap/<watermark>.<hash16>.csnap` with the part's own watermark hour in the name (already per-part today); postings keep `idx/<watermark>.<hash16>.npost`. No new prefixes.

### 3. Parallel fold

Inside one `Catalog::fold` call:

- Bucket discovery I/O (LIST per (shard, hour), record GETs) runs concurrently under a semaphore, `CatalogConfig::fold_bucket_concurrency` (new, default 8), mirroring the resolve path's concurrent-LIST pattern. Results merge in deterministic bucket order so the fold stays reproducible byte-for-byte (content addressing depends on it).
- Only changed parts are encoded and PUT: the ordinary incremental fold touches the tail alone; a reconcile or rebuild encodes each changed part and PUTs them concurrently. Unchanged sealed parts are carried into the new HEAD by reference (same key, same blake3), never re-PUT.
- Memory drops from O(all live entries) to O(tail + newly folded + changed parts): the dedup `seen` map only ever needs the entries of parts being rewritten, because identity leads with the hour and sealed ranges are disjoint from the fold's input hours.
- The rebuild path (HEAD absent/corrupt/unreadable part) partitions the discovered hours into target-sized ranges and builds all parts in parallel, then publishes them under one HEAD CAS. This is where "parallel" buys the most: a 10^7-entry rebuild becomes N independent part builds instead of one giant serial encode.

### 4. Reconcile pass (compaction and retention reaching sealed parts)

Each fold, after the incremental step, re-lists the commit buckets for hours in `[watermark - fold_reconcile_window_hours, W_old]` (new config, default 26, chosen to cover `protection_horizon` (24 h) plus slack) and diffs the discovered compaction records and tombstones against what the covering parts already reflect. A bucket whose state changed (new `l1.*.cmt`, new `retire.tmb`) marks its covering part dirty; dirty parts are rewritten in the same fold and published under the same single HEAD CAS. The window is bounded, so reconcile cost is O(window x shard fan-out) LISTs per cycle, amortizable by running the pass every Nth fold (`fold_reconcile_every`, default 1).

The window is sound because the sweeper only deletes a compaction input after `created_unix_ns + protection_horizon` (sweep.rs `sweep_superseded`): any record whose supersession could invalidate snapshot entries is guaranteed to be observed by a reconcile pass before its inputs can be physically deleted. Tombstoned buckets drop out of the part exactly as the fold already drops them for newly sealed hours.

(Today, a compaction record landing after its hour was folded is never applied at all; see Consequences for the report on that pre-existing gap.)

### 5. Single-writer guarantee: exactly as strong as today

- HEAD remains the only mutable object and the only CAS (`PutMode::CasVersion`, `CreateIfAbsent` for first fold). One CAS publishes the entire new part set atomically. There are no per-part CAS operations: parts are immutable and content-addressed (`CreateIfAbsent`; `AlreadyExists` is idempotent success), so they need none, and giving them one would create a multi-object atomic-commit problem this design refuses to have.
- Torn state is impossible: a reader either sees the old HEAD (old complete part set, all still present, since superseded parts are only GC'd after the protection horizon) or the new HEAD (new complete set, all PUT before the CAS was attempted). Nothing in between is nameable.
- Crash mid-fold, after k of n part PUTs: HEAD unchanged, old snapshot fully intact, the k new parts are invisible orphans. The retrying fold recomputes byte-identical parts (deterministic input, deterministic merge order), lands on the same content-addressed keys, and `AlreadyExists` makes the retry free. Never-referenced parts age out under the new GC rule (point 6). This is the existing crash matrix with "orphan part" pluralized.
- Concurrent folders: unchanged CAS loop, bounded by `MAX_HEAD_CAS_ATTEMPTS`. The loser re-reads the winner's HEAD and rebases; rebasing is now cheaper because only the tail (and any dirty parts) differ from the winner's set.

### 6. GC of superseded snapshot objects

New ravel-maintain sweep rule (`sweep_unreferenced_catalog_objects`): list `t/<th>/catalog/<signal>/snap/` and `idx/`, GET HEAD, delete any object not named by the current HEAD (parts by key, postings by key) whose age exceeds the protection horizon. The age gate protects pinned queries and any reader holding a HEAD within its cache TTL, the same reasoning as the existing unreferenced-`l1/` rule. Dry-run honored like every other rule.

### 7. Postings under multi-part

- The resolver's `parts.len() == 1` pruning restriction (snapshot_resolve.rs:137) is replaced by the binding check the format already mandates: postings are usable iff `part_blake3` matches all of HEAD's parts in order (`validate_head` enforces this shape already). Ordinals keep indexing the concatenated part entry order.
- The fold's forward-merge baseline stays valid while only the tail grows: sealed parts keep their ordinal prefix stable. The existing "any L1 entry present => no postings ref" gate stays as-is; a reconcile that rewrites a sealed part therefore publishes no postings, exactly matching today's behavior once compaction reaches a tenant.

### 8. Acceptance

An end-to-end test in ravel-server (extending tests/fold_e2e.rs) configures a small `snapshot_part_max_entries`, drives a tenant's segment count past the single-part ceiling scaled down (e.g. cap 100 entries, publish 350 segments across several sealed hours plus a compaction record and a tombstone), and asserts through real server entry points: fold produces multiple parts under one HEAD; queries over the whole range return exactly the listing-path result; the compaction supersession and the tombstone are reflected after reconcile; a fold at every step stays under the per-part decode cap. Unit/property tests scaled the same way cover the format and crash matrix (FaultStore with asserted fault counters, per repo testing rules).

## Rejected alternatives

1. **Hash-partitioned parts (entries sharded by shard index or an identity-hash range, HEAD as manifest).** Every fold appends new hours across the whole hash domain, so every part is rewritten every cycle: per-fold write amplification stays O(live entries), which is the actual cost wall, parallel or not. It also destroys the free global no-duplicate proof (a duplicate identity could hide in the wrong hash part, so cross-part validation would need a full multi-part read) and destroys postings ordinal stability every cycle. Hour partitioning gets append-mostly stability directly from the existing hour-major sort; hash partitioning fights it. Lost on write amplification and on weakening decode-time validation.

2. **Append-only delta log of fold deltas with periodic compaction into base parts.** Best possible steady-state write cost (O(new) per fold, no tail rewrite), but the costs land in the wrong places: supersession and retention need anti-entry/tombstone record kinds in the persistent format (a genuine new format version, not an additive field); the sorted/no-duplicate invariant stops being decode-time-checkable per object and becomes a read-time merge property; every resolve pays an ordered merge across a chain whose partial loss (one missing middle delta) invalidates the whole suffix; and HEAD must atomically name the full chain anyway, so the CAS shape is unchanged while the read path, where correctness actually lives, gets strictly more complex. The tail rewrite this ADR keeps is bounded by `snapshot_part_max_entries`, so the delta log's extra savings are modest. Lost on read-path complexity and format risk for marginal gain.

3. **Multiple CAS pointers (per-range or per-shard-group HEADs, as ADR-0003 once sketched `HEAD/<shard-group>`).** Would allow truly independent parallel folders per partition, but a reader assembling N pointers read at different instants can observe a combined state no single fold ever published: watermarks torn across partitions, postings bindable to no consistent part set, and the seal-divergence verifier left without a single ground state to diff. Restoring atomicity requires a manifest-of-manifests with its own CAS, which reinstates the single pointer plus an extra layer. This is precisely the weakening of the single-writer guarantee this design forbids. Lost on the invariant itself.

4. **Raise the cap / stream the decode and keep one part.** Raising `max_snapshot_part_bytes` (and streaming zstd decode) moves the wall without removing it, keeps O(live entries) rewrite bytes every 5 minutes, keeps fold memory O(live entries), and makes the 5-minute cadence itself the next casualty (a multi-hundred-MiB PUT per tenant per cycle). Kick-the-can, not a redesign; the gap explicitly called for a redesign. Lost on not addressing the gap.

## Consequences

- **Scale**: per-tenant metadata is no longer bounded by one object. Steady-state fold cost drops from O(live entries) to O(new + tail); the 256 MiB decode cap becomes a per-part bound with unbounded part count (HEAD grows ~100 bytes per part ref; 10^7 entries is ~40 refs, ~4 KB).
- **Queries**: `load_snapshot_parts` can skip parts whose hour range does not overlap the window (a new cheap filter on `SnapshotPartRef.min_hour` / `watermark_hour`), shrinking bytes fetched and cache pressure for narrow windows. Hours <= W keep coming exclusively from parts; the Phase 1 listing suffix and its soundness argument (seal lemma) are untouched.
- **Dual-reader (format-change step 3)**: yes, both directions, benign. Old readers already union N parts, ignore the new proto fields, and their postings pruning self-disables on multi-part heads (safe: pruning is optimization-only). New readers accept every existing single-part object (`min_hour` absent decodes as 0). One transitional wrinkle: an old *folder* that wins a CAS against a new multi-part HEAD collapses the snapshot back to a single part. That is correct (readers cope, content addressing and CAS serialize it) but wasteful, and it re-imposes the ceiling while any old folder runs; deployment guidance is folders first, same ordering discipline the config already uses for seal margins. If the tenant is already past the single-part ceiling, an old folder's collapse attempt fails at encode (cap exceeded) and errors without touching HEAD: degraded (no fold progress until upgrade completes), never corrupt.
- **Reconcile closes a latent correctness gap**: today a compaction record or tombstone landing in an already-folded hour is never folded, and after the protection horizon `sweep_superseded` deletes L0 inputs that snapshot entries still name, leaving resolve to emit refs to deleted objects with no self-heal short of a HEAD-corruption rebuild. The reconcile pass fixes this structurally. (Reported separately as a pre-existing bug per repo rules; it exists independent of this work.)
- **New maintenance rule**: catalog snapshot GC finally exists; without it, multi-part folding would only enlarge an already-unswept prefix (today every 5-minute fold leaks one superseded part; nothing deletes it).
- **The recent-hours cliff is not fixed here**: the `TooManySegments` cliff is its own effort (the "recent-hours read path" item). This ADR removes the fold-ceiling contribution to that outage (folded state and compaction results stay live past 10^6 segments) but does not serve open-hour reads from segments.
- **Selective deletion (ADR-0064)**: hour-range parts *resolve* a coming interaction rather than create one. Subject deletion must rewrite derived indexes; range-partitioned parts localize that to the covering part(s), and that work can reuse this ADR's targeted part-rewrite-plus-single-CAS machinery verbatim. ADR-0064 cites this one for the snapshot half of erasure.
- **Format migration (ADR-0066)**: deliberately minimized surface. No envelope or HEAD version bump, additive proto fields only, no new key shapes; and catalog snapshots are derived, rebuildable state (rebuild-from-commit-layout is their migration path), so the migration machinery can exclude the catalog prefix entirely. If it nevertheless introduces catalog-layout changes, the collision point is `snapshot_format/` and this ADR's validation rules; whichever lands second must re-check the multi-part head validation clauses. ADR-0066 additionally found that the fold's HEAD-decode-failure path treats `UnsupportedHeadVersion` as corrupt and CAS-clobbers it (fold.rs:866-869) -- a live rolling-upgrade hazard that ADR-0066 fixes and that this format bump depends on landing first.
- **Tests and tooling (format-change steps 5-6)**: property/corrupt-input tests extend to min_hour ranges, multi-part head validation, and mixed old/new part objects; `ravel-cli catalog inspect` prints per-part hour ranges and the part count; `catalog verify` (seal divergence) already iterates parts and needs only range-aware expectations.
- **Costs accepted**: HEAD validation gets slightly stricter (multi-part ordering rules); the reconcile pass adds a bounded per-cycle LIST cost (window x shard fan-out, amortizable); postings pruning remains unavailable once compaction touches a tenant (unchanged from today).
