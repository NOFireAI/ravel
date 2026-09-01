# ADR-0058: commit-record reconstruction and DR posture

Status: Accepted

Amended by ADR-0077: decision 5's disaster-recovery document graduates
from an honest statement of absence to a normative DR runbook (tiers,
restore procedure, rehearsal-derived RPO/RTO). Decisions 1-4 stand
unchanged and become load-bearing steps of that runbook's restore
reconciliation.

## Context

Commit records are the sole metadata root in Ravel. A reader trusts nothing
about a data object's existence except a commit record naming it
(docs/object-store-contract.md: "never infer visibility from a successful
data PUT; only commit records confirm publication"). If commit records for a
shard/hour are lost — an accidental delete, a bad S3 lifecycle rule, a
fat-fingered prefix delete — the data objects they named become invisible to
every reader immediately, and the orphan sweeper (`crates/ravel-maintain/src/
sweep.rs`, rule 1) physically deletes them once they clear
`grace_ns + max_flush_lifetime_ns` (24h + 1h = 25h by default) with no record
naming them. This is the most dangerous flaw in the durability posture:
one bad lifecycle rule, no attacker, no alarm, permanent loss.

ADR-0048 (already shipped) closed half of this: a mass-orphan
circuit breaker halts the sweeper and trips
`ravel_maintain_orphan_breaker_tripped_total` when candidates exceed
`orphan_breaker_min_count` (default 50) *and*
`orphan_breaker_max_ratio` (default 0.10) of listed L0 objects for a shard.
ADR-0048's own Consequences section named the other half as future work:
*"the footer-derived commit-record reconstruction tool... is its own piece
of work."* This ADR is that work, plus two related gaps: no document
instructs an operator to stop `--mode maintain` before touching a
metadata anomaly, and Ravel's disaster-recovery posture is unstated, with
the one existing sentence on backup ("back up the bucket, or rely on its
durability") correct only in the happy path.

### The breaker's blind spot

The breaker's threshold is ratio- and count-based, sized to catch mass
loss (a bad lifecycle rule wiping a whole prefix). It does not catch small,
targeted loss: delete three commit records for one shard/hour and the
sweeper's candidate count for that bucket never approaches 50 or 10% of
that shard's L0 objects, so the breaker never trips. The three orphaned
data objects are deleted at hour 25 exactly as if they were abandoned
flushes — no metric, no log line above `debug`, nothing. This means the
acceptance criterion this ADR and ADR-0048 jointly serve — *"no data object may
be deleted without a distinct operator-visible alarm"* — does not hold
under the shipped breaker alone. A reconstruction tool nobody knows to run
in time is not a mitigation for this case; the trigger is part of this
epic's deliverable, not a follow-up.

### What a data-object footer can and cannot reconstruct

RSEG (`ravel.segment.v1.Footer`, proto/ravel/segment.proto:47-69) and RLOG
(`ravel.logseg.v1.LogFooter`, proto/ravel/logseg.proto:40-64) footers carry
most, but not all, of `CommitRecord`'s fields (proto/ravel/commit.proto:
16-38):

| Field | RSEG footer | RLOG footer | Reconstructable how |
|---|---|---|---|
| tenant_hash, shard, writer_id, writer_epoch, writer_seq | present | present | direct copy |
| min/max_event_ts_ns | present | present | direct copy |
| min/max_ingest_ts_ns | present | present (`min/max_observed_ts_ns`) | direct copy |
| sample_count / series_count | present | present (`record_count`) | direct copy |
| ingest_hour_bucket | present (field 14) | **absent** | see below |
| segment_format_version | trailer version byte | trailer version byte | direct copy |
| object_size | not in footer | not in footer | `ObjectMeta.size` from the same LIST that finds the object — no extra read |
| content_hash (32B blake3) | not in footer (only first 16 bits ride the data key's filename) | same | full-object GET + rehash — see below |
| created_unix_ns | not in footer; `base_created_unix_ns` is a *different* clock (set from `max_ingest_ts_ns`, not the flush-open time) | **absent entirely** | `ObjectMeta.last_modified` — see below |

**`content_hash` is exact, but not free.** Rehashing the object's own
stored bytes with blake3 gives the true content_hash of what is actually
there now — no approximation, since content_hash is defined over the
bytes, not over writer intent. The cost is a full-object GET per orphaned
object, not a footer-only suffix read (the same cost `ravel-cli maintain
verify-custody` already pays, manually, today). If the underlying bytes
have themselves rotted, reconstruction faithfully hashes the rotted bytes
and produces a record that matches them — that is the scrubber's problem
(ADR-0059), not this one's; the reconstruction step does not detect
rot, it only rebuilds the record that would have described whatever is
actually stored.

**`created_unix_ns` cannot be recovered exactly, and is load-bearing.**
It anchors the MVCC dedup tiebreak (docs/catalog-and-mvcc.md, "Cross-
segment duplicate samples") and, more consequentially, `sweep.rs`'s own
superseded-record horizon gate (`sweep.rs:20,335,338`: *"Horizon gate
anchored on the durable created_unix_ns... `now >= record.created_unix_ns
+ protection_horizon`"*) and retention's supersession-horizon anchor
(`retention.rs:99`, `clock.rs:8`) — both compare it against a wall-clock
horizon, not just against sibling records. Reconstruction uses the
orphaned data object's own `ObjectMeta.last_modified` (its real S3 write
timestamp) as the substitute. This is not "now at reconstruction time" —
it is a genuine historical timestamp, off from the true `created_unix_ns`
only by the latency between the data PUT completing and the commit-record
PUT that would have followed it (`crates/ravel-ingest/src/shard.rs:562-
603`: the data PUT is awaited to completion before the commit record is
even built), typically sub-second to low-second in practice. Every
horizon this value feeds is measured in hours (`DEFAULT_GRACE_NS` = 24h,
`protection_horizon` similarly scaled), so this substitution does not
meaningfully destabilize any horizon-anchored decision. The reconstructed
record is honestly a reconstruction, not a claim of exact original
provenance — this is Ravel's own "approximation opt-in and visible"
invariant applied to the one field that cannot be exact, stated plainly
rather than silently accepted.

**RLOG's missing `ingest_hour_bucket`** is derivable, not guessed.
`CommitRecord::validate` (`crates/ravel-commit/src/record.rs:143-151`)
enforces `ingest_hour_bucket <= floor(created_unix_ns / NS_PER_HOUR)` with
no lower bound — consistent with `ingest_hour_bucket` being derived from
the *earliest ingested sample's* timestamp, not the record's creation
time (this is what lets late-arriving data file under an older hour
bucket than the flush that wrote it). RLOG's footer carries
`min_observed_ts_ns` directly; deriving `ingest_hour_bucket =
floor(min_observed_ts_ns / NS_PER_HOUR)` matches this invariant by
construction and can be cross-checked against real RSEG objects (which
carry the field explicitly) as a test.

### The ADR-0055 gap

A reconstruction tool most naturally ships as a `ravel-cli` subcommand,
which runs under the Admin credential (ADR-0055). ADR-0055's role table
(§1) grants `c/**cmt` `CreateIfAbsent` write only to Gateway (for L0
commit records at ingest time); Admin's write column has no `c/**` grant
at all. Without an amendment, a correctly-designed reconstruction tool
would fail its own write with an access-denied error the moment it tried
to publish a rebuilt record. This must be decided here, not discovered at
decompose time.

### Existing adjacent tooling

`ravel-cli maintain verify-custody` (`services/ravel-cli/src/maintain.rs:
461-655`) already re-hashes every data object a *surviving* commit record
names and compares against the record's key — but it iterates commit
records to find data objects, so if a shard's commit records are gone
entirely, it iterates zero records and reports zero anomalies. It cannot
detect the missing-records scenario at all. `ravel-cli catalog verify`
(`services/ravel-cli/src/catalog.rs:147-266`) is a different tool solving
a different problem (fold/snapshot staleness) and is not
extended here. `ravel_commit::record::build`
(`crates/ravel-commit/src/record.rs:70-103`) and
`keys::commit_key_for_record` (`keys.rs:466-483`) are existing, tested
library functions that already do exactly the assemble-and-address step a
reconstruction tool needs — this ADR reuses them rather than duplicating
record-construction logic.

### DR posture, honestly

No document tells an operator to stop `--mode maintain` before touching a
metadata anomaly, despite ADR-0048's own Consequences section promising
this runbook step would land in `docs/guides/operations.md`. A running
maintenance loop keeps sweeping every tick while an operator restores
records by hand, racing the restore. Separately, `docs/guides/
operations.md`'s "Disposability" section states there is "nothing to back
up besides the object store bucket itself... rely on its durability" —
true and incomplete: the bucket itself is a single point of loss, with no
mandated versioning or cross-region replication, no backup cadence, no
restore drill, and no RTO/RPO statement anywhere. "Replica-bucket
failover" does not exist as a Ravel
feature at all (`S3Config` has exactly one bucket, one region); it would
be a hypothetical operator mitigation (pointing
at an S3-CRR replica bucket on outage), not a half-built Ravel mechanism.
Vanilla S3 CRR is asynchronous and gives no cross-bucket CAS or listing-
consistency guarantee, which every seal/GC/compaction correctness argument
in this system depends on — so naively failing over to a CRR replica
would silently violate invariants the system assumes hold. This needs to
be stated plainly, not implied to be a working mitigation.

## Decision

### 1. Orphan-presence signal (closes the breaker's blind spot)

Export a new gauge, `ravel_maintain_orphans_present{signal}` (no
`tenant_hash`, matching `render_maintain_safety_family`'s existing
convention and ADR-0044 §4's unauthenticated-route constraint): the count
of orphan candidates `sweep_orphans` already computes
(`sweep.rs:202-279`) before the breaker gate, for *every* pass, whether
or not the breaker trips. This is a few lines on data the sweeper already
holds — no new read, no new list, no change to deletion behavior. An
operator (or an alert rule: `ravel_maintain_orphans_present > 0` sustained
past, say, half the grace window) now has a signal for small-scale loss
that the breaker's ratio/count thresholds are deliberately too coarse to
catch. This is required for the epic's own acceptance criterion ("no data
object may be deleted without a distinct operator-visible alarm") to
actually hold at every scale, not just mass loss.

### 2. Reconstruction tool: `ravel-cli commit reconstruct`

A new `ravel-cli` subcommand. Inputs: tenant, signal, shard (scoped —
never a bucket-wide sweep by default, to bound blast radius and cost).
Procedure:

1. List the shard's `l0/` prefix (`list_all`, the same pattern
   `sweep_orphans` already uses) and the corresponding `c/` prefix; the
   set difference (by identity: writer_id/epoch/seq) is the candidate
   orphan set.
2. For each candidate: suffix-GET the footer (the same footer-first
   pattern `SegmentFetcher::open_segment` and `load_input_catalog`
   already use) to recover every field the footer carries directly, then
   a full-object GET to compute the exact `content_hash` and derive
   `object_size` from the same `ObjectMeta`.
3. Set `created_unix_ns` from the data object's own `ObjectMeta.
   last_modified`. Set `ingest_hour_bucket` from the footer directly
   (RSEG) or derive it from `min_observed_ts_ns` (RLOG), per the Context
   section above.
4. Build the record via the existing `ravel_commit::record::build`,
   address it via `keys::commit_key_for_record`, and write it with
   `PutMode::CreateIfAbsent` — never overwrite an existing commit record
   (if one now exists, e.g. a concurrent partial reconstruction or the
   original was somehow not actually lost, the tool reports a conflict
   and skips, it does not clobber).
5. Print a per-object report: reconstructed / already-present (skipped) /
   failed (with reason), and an exit code reflecting whether any failures
   occurred.

This is purely additive — it only ever writes `CreateIfAbsent`, never
deletes, so it does not need Maintain's delete grant.

### 3. ADR-0055 amendment: Admin gets `c/**cmt` write

Amend ADR-0055 §1's role table: Admin's write column gains
`s3:PutObject` on `t/*/*/c/*` (the same prefix Gateway already writes,
scoped the same way), justified specifically by this reconstruction tool.
No other role's grants change. This amendment lands as part of this
epic's implementation (touching `docs/guides/operations.md` and
`docs/guides/kubernetes.md`'s policy JSON the same way ADR-0057's
admission-keyspace addendum did), not as a separate ADR.

### 4. Stop-maintain-first runbook

A new section in `docs/guides/operations.md`, placed immediately
alongside the existing mass-orphan-breaker runbook
(`operations.md:1252-1326`): before restoring or reconstructing any
commit record, stop (or restrict via `--maintain-tenant`) every running
`--mode maintain` process for the affected tenant, run
`commit reconstruct`, verify via `ravel-cli maintain verify-custody` and
`ravel-cli catalog verify`, then resume maintenance. This closes the
stop-maintain-first gap and fulfills the promise ADR-0048's Consequences
section made but did not deliver.

### 5. Honest DR-posture document

A new `docs/guides/disaster-recovery.md` (cross-referenced from
"Disposability" in operations.md, replacing its overclaiming sentence):
states plainly that Ravel has no built-in backup mechanism, no cross-
region failover, and no RTO/RPO guarantee; that the bucket is a single
point of loss whose only mitigation available today is the object
store's own durability plus S3 versioning/Object Lock (already covered
informationally by ADR-0055 §3) as a *complement* to, not a substitute
for, real backups; that a genuine DR posture (periodic bucket-to-bucket
copy with an explicit staleness bound, or S3 Cross-Region Replication
accepted with its async/no-CAS caveats explicitly documented) is future
work this ADR does not build; and includes this epic's reconstruction
tool as the recovery path for the one failure mode (commit-record loss)
that this program does close today.

## Rejected alternatives

**Reconstruct via `verify-custody`'s existing scan, extended.**
`verify-custody` is commit-record-driven (it starts from records and
walks to data objects); extending it to also detect record-less data
objects would require inverting its whole iteration order, which is a
rewrite, not an extension. A dedicated tool with its own orphan-first
scan is simpler to reason about and to scope (single shard, bounded
blast radius) than retrofitting a verification tool into a repair tool.

**Full bucket-wide automatic reconstruction on breaker trip.** Automating
reconstruction the instant the breaker trips would remove the operator
from a decision that should stay a decision — the breaker's whole point
(ADR-0048 §4) is to halt and force a human look, not to auto-repair.
Automatic reconstruction could also mask a genuine attack (an adversary
deleting records specifically to trigger a mass "repair" that writes
attacker-controlled data) — out of scope for this ADR's threat model,
but a reason not to remove the human step regardless.

**Treat `last_modified`-derived `created_unix_ns` as exact and skip
documenting the approximation.** Rejected on Ravel's own "exact semantics
by default, approximation opt-in and visible" invariant — the honest
framing costs one paragraph and avoids a future reader assuming
reconstructed records are indistinguishable from original ones.

**Skip the orphan-presence gauge; rely on operators reading `sweep`
logs.** Rejected because this is exactly the "no alarm" gap — a
`debug`-level log line nobody is tailing is not an alarm, and
the fix is a few lines against data the sweeper already computes.

## Consequences

- **Closes the record-loss gap at every scale**, not just mass loss: the orphan-
  presence gauge catches small-scale record loss the breaker's
  ratio/count thresholds are too coarse for; the reconstruction tool
  recovers from it once noticed.
- **Reconstructed records are honest approximations on two fields**
  (`created_unix_ns` from data-object `last_modified`, not the true
  flush-open time; and for RLOG, `ingest_hour_bucket` derived rather than
  read directly) — both are argued above to be safe for every downstream
  consumer that reads them, but this ADR does not claim byte-for-byte
  fidelity to the original record.
- **Reconstruction does not detect or repair bit rot.** It rebuilds a
  record describing whatever bytes are currently stored; the scrubber
  (ADR-0059) is the mechanism that would have caught rot before this point.
- **Admin's credential grows one write grant** (ADR-0055 amendment) —
  narrowly scoped to the same `c/*` prefix Gateway already writes, no
  delete, no other prefix.
- **The DR document is honest about what does not exist** rather than
  implying a mechanism the system does not have — this may be
  uncomfortable to publish but is strictly safer than the status quo's
  overclaiming sentence.
- **Does not build cross-region failover or automated backups.** Filed
  explicitly as future work in the new DR document, not silently
  deferred.
