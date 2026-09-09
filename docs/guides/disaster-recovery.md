# Disaster recovery runbook

Normative. This runbook defines Ravel's disaster-recovery posture: three
operator-owned configuration levels, the platform-CLI controls each requires, a
deliberate verified restore procedure, and the rehearsal record from which the
only published recovery numbers come.

Three acronyms appear throughout, glossed here on first use and defined once
in the [glossary](../concepts.md#glossary):

- **RPO**, recovery point objective: how much recent data a recovery is
  allowed to lose.
- **RTO**, recovery time objective: how long a recovery is allowed to take.
- **RTC**, replication time control: the S3 replication option that puts a
  published ceiling on replication lag. It is the only thing that gives RPO a
  bound at all, which is why it recurs below.

The shape of the posture is deliberate and stated plainly: Ravel builds **no
in-product backup, export, or failover mechanism**. Disaster recovery is
operator-owned bucket-level controls, namely versioning,
noncurrent-version expiration, and cross-region cross-account replication,
specified normatively here, verified where the platform can see them, and
proven by a rehearsed restore. Object storage remains the only durable
backend; the replica is written by the platform's replication channel, never
by a Ravel process.

## What every level requires, before any of them

Independent of the disaster-recovery level below, the bucket-protection
contract in
[object-store-contract.md](../object-store-contract.md#required-bucket-configuration-adr-0064-7-adr-0072-decision-3)
asks for Object Lock in **compliance mode** on the protected prefixes,
paired with versioning:

- the deployment records under `sys/`,
- the per-(tenant, signal) provisioning records,
- the commit records,
- the catalog HEAD history.

Versioning is what makes the last of those work: a HEAD compare-and-swap
creates a new locked version rather than overwriting one. None of those four
prefix families holds an erasable subject value, deliberately, so locking them
never collides with an erasure request. The scoped posture is therefore not a
disaster-recovery choice and carries no erasure cost; it is the baseline the
commit and catalog layers already assume.

Scoping the lock takes an operator-run mechanism, and it is a requirement of
levels 0 and 1, not an optional extra. Level 2 replaces it with a bucket
default retention that reaches every object, so level 2 runs no mechanism and
the "no bucket default retention" item below does not apply to it. Object
Lock is enabled per bucket, not per prefix, and Ravel never sets retention on
an object. Objects under the protected prefixes carry **per-object retention**
applied by a mechanism the operator runs outside Ravel. Check off all of
these:

- [ ] Object Lock enabled on the bucket, with **no bucket default retention**
      (a default retention would lock the data objects too, which is level 2).
- [ ] Versioning ON.
- [ ] One of the two mechanisms below, applying per-object retention in
      compliance mode, for the chosen retention period, to new objects under
      `sys/`, the provisioning records, the commit records, and the catalog
      HEAD history.

| Mechanism | What it does | Coverage window |
|---|---|---|
| Event-driven function | A function subscribed to object-created events, filtered to the protected prefixes, calls the per-object retention API in compliance mode. Events can be delayed or lost and objects written before the subscription raise none, so it needs durable retry with a dead-letter queue, a one-time backfill over existing versions, and a periodic reconciliation against an all-versions S3 Inventory. | Each object is locked within seconds of its creation; a missed event is covered at the next reconciliation. |
| Scheduled batch job | An S3 Batch Operations job, run on a schedule and driven by an all-versions S3 Inventory manifest (`IncludedObjectVersions=All`) filtered to the same prefixes, sets the same retention on every listed version, current and noncurrent. | Up to the schedule interval plus the inventory delay plus the job's own execution and retry time. |

Between an object's creation and the moment the mechanism acts on it, the
object carries no retention and any credential that can delete can delete it.
That window is the residual exposure of the scoped posture. Choose the
mechanism whose window your compliance regime accepts. The AWS reference for
both is
[S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html).

`--require-bucket-protection` gates startup on the bucket half of that
posture only: Object Lock enabled and versioning on. With the flag set, a
bucket reporting Object Lock disabled, or a versioning misconfiguration,
refuses to start; a backend that cannot disclose its state warns once and
raises the `ravel_bucket_protection_unknown` gauge; a bucket reporting it
enabled starts clean. The flag cannot see whether the retention mechanism is
running, whether every protected object version carries retention, or whether
a bucket default retention is set; those are verified out of band with the
commands under "Verification" below. The flag is off by default, so an
existing deployment is unchanged until an operator turns it on. Enforcement
itself stays at the bucket and IAM layer either way: nothing in a Ravel
process can configure Object Lock.

What the levels below add on top of that baseline is replication, and, at
level 2, a **bucket default retention** across every object including the data
objects. That last one needs no mechanism, and it is the choice with an
erasure cost.

## The three levels

Each level states its erasure-bound consequence next to its protection. This
disclosure is load-bearing, not a footnote: turning on versioning and a bucket
default retention extends the physical erasure bound, and the tension
between disaster recovery and the erasure guarantee is resolved by stating
that cost, never by hiding it. Do not soften or omit it. The bounds the
modifiers apply to are in
[deletion-and-gc.md](../deletion-and-gc.md#modifiers-to-the-bound).

### level 0, no replication (default)

The bucket is versioned. Versioning is not optional here and it is not
scoped to some prefixes: the baseline above requires it, because Object Lock
cannot be enabled without it, and S3 versioning is bucket-wide. There is no
bucket that is versioned only under the protected prefixes. So every
overwrite and delete of a data object leaves a noncurrent version behind.

Level 0 pairs that versioning with a `NoncurrentDays = E_v`
noncurrent-version expiration rule and expired-delete-marker cleanup, and
adds no replica. Its physical erasure bound is the primary bound plus
`+E_v`, the same primary half level 1 states: erased bytes persist as
noncurrent versions until the rule reaps them. The base bounds are in
[consistency-model.md](../consistency-model.md); the `+E_v` modifier is in
[deletion-and-gc.md](../deletion-and-gc.md#modifiers-to-the-bound).

Versioning on without that lifecycle rule is not a level. Without the rule
the noncurrent residue is unbounded and the bounds in this guide do not
apply: every delete becomes a soft delete that survives indefinitely while
every layer above keeps reporting success.

RPO and RTO: none; bucket loss is total loss. This remains a **supported
posture**, but its blast radius is total: every durable byte (data objects,
commit records, manifests, catalog snapshots, control objects under `sys/`)
lives in one bucket, and Ravel itself cannot recover from losing it.

### level 1, replicated (the recommended posture)

Level 0 plus a replica. The primary keeps the level-0 configuration
unchanged: versioning on, the `NoncurrentDays = E_v` noncurrent-version
expiration rule, and expired-delete-marker cleanup.

Replication to a replica bucket:

- Replication **v2 configuration** with `DeleteMarkerReplication` **enabled**,
  RTC **recommended**.
- The replica lives in a **different region** and a **different account**,
  encrypted under a **different KMS key** (`ReplicaKmsKeyID`). Replication
  requires versioning on both buckets, so the replica is versioned too, and
  it carries its own `NoncurrentDays = E_v_r` expiration rule and
  expired-delete-marker cleanup.
- Ravel processes never hold replica-account credentials; the replication
  channel is the only writer to the replica.

**Erasure consequence, disclosed:** the primary physical erasure bound gains
`+E_v`. The replica's copy of an erased subject is physically gone within
replication lag plus `E_v_r` after the primary sweep, **provided
`DeleteMarkerReplication` is enabled**. See the mandate below.

### level 2, level 1 plus a bucket default retention

A bucket default retention `D` on the primary, the replica, or both. S3
applies it to every object at write time, so this level needs no mechanism and
has no coverage window. It is a strict superset of the scoped posture the
bucket-protection contract asks for: it reaches the data objects too, which is
where erasable subject values live. The physical erasure bound therefore
becomes `max(bound, D)`; query-time exclusion stays immediate either way.
Where you have erasure obligations, prefer **scoped legal holds** over blanket
default retention, or keep `D` inside the erasure service level agreement.

level 2 is **supported, but not part of the recommended baseline**. Its only
marginal protection over level 1 is against a compromised primary credential
purging version history, and level 1 already contains that threat: version-id
permanent deletes are never replicated, the replica lives in an account whose
credentials Ravel never holds, and the replica retains deleted data as
noncurrent versions for `E_v_r`. Making blanket retention mandatory would
impose `max(bound, D)` on every replicating deployment's erasure bound to
defend against a threat the cross-account replica already covers. Deployments
whose compliance regime demands bucket-wide write-once-read-many storage take
level 2 as a deliberate choice, with the erasure consequence disclosed.

## `DeleteMarkerReplication` is mandatory for erasure-obligated deployments

Every Ravel delete issues a **simple DELETE**; nothing in Ravel ever deletes
by version id. On a versioned bucket a simple DELETE becomes a **delete
marker**, and a delete marker replicates to the replica **only when
`DeleteMarkerReplication` is enabled**.

Therefore, for any deployment with erasure obligations,
`DeleteMarkerReplication` is **MANDATORY**. Omitting it has a concrete,
non-negotiable consequence: **erased bytes persist on the replica
indefinitely**, because the delete marker that would reap them never arrives.
That configuration is **unsupported** for any deployment with erasure
obligations, and
[deletion-and-gc.md](../deletion-and-gc.md#modifiers-to-the-bound) says
the same.

Note the deliberate asymmetry this buys: version-id permanent deletes are
**never** replicated at all, so a compromised primary credential cannot purge
the replica through the replication channel. That is the property that lets
the cross-account replica stand in for a bucket default retention at level 1.

## `E_v` is one knob controlling two windows

`E_v` is present from level 0, since the bucket is versioned there. **`E_v`
is one knob controlling two windows.**

- It is the **disaster-detection budget**: after an accidental or malicious
  mass delete, the operator has `E_v` to notice and restore the noncurrent
  versions on the primary. At level 1 the replica has its own window,
  replication lag plus `E_v_r`; the two windows run side by side and do not
  add up.
- It is simultaneously the **erasure-residue window**: erased bytes persist as
  noncurrent versions for `E_v`.

Choosing `E_v` is a compliance decision, not a tuning default. Set it
deliberately against **both** your detection objective and your erasure
service level agreement. This runbook refuses to pick a number for you.

## Platform-CLI verification checklist

Ravel cannot enforce most of this and does not pretend to. `ravel-cli store
qualify` probes whether Object Lock and versioning appear enabled and reports
whether a lifecycle rule is present, not what the rule says, and
**replication configuration is invisible to the object-store layer**, so
verification of the lifecycle values and the replication controls is an
explicit platform-CLI step and not something Ravel checked. The versioning
and `NoncurrentDays = E_v` lifecycle checks apply at level 0 and level 1
alike; the replication and replica checks are level 1 only. Run these against
the actual buckets, and treat a missing row or a differing value as a failed
check:

```sh
# Primary: versioning ON
aws s3api get-bucket-versioning --bucket <primary>

# Primary: the enabled lifecycle rules. Pass only when one row carries
# noncurrent_days equal to E_v, one carries expired_delete_markers true,
# and one carries abort_mpu_days of 7 or less (the contract's required
# AbortIncompleteMultipartUpload rule), AND each of those rows has an
# empty scope (both scope columns null: the rule covers the whole bucket)
# or a scope that, across the rows, covers every t/ prefix. A rule scoped
# to one prefix leaves noncurrent versions elsewhere unbounded, and the
# +E_v bound does not hold.
aws s3api get-bucket-lifecycle-configuration --bucket <primary> \
  --query 'Rules[?Status==`Enabled`].{id:ID,scope:Filter.Prefix,legacy_scope:Prefix,noncurrent_days:NoncurrentVersionExpiration.NoncurrentDays,expired_delete_markers:Expiration.ExpiredObjectDeleteMarker,abort_mpu_days:AbortIncompleteMultipartUpload.DaysAfterInitiation}' \
  --output table

# Primary: replication v2, DeleteMarkerReplication enabled, RTC (if required)
aws s3api get-bucket-replication --bucket <primary>

# Replica: versioning ON, and the same rule and scope check with
# noncurrent_days equal to E_v_r and expired_delete_markers true.
aws s3api get-bucket-versioning --bucket <replica>
aws s3api get-bucket-lifecycle-configuration --bucket <replica> \
  --query 'Rules[?Status==`Enabled`].{id:ID,scope:Filter.Prefix,legacy_scope:Prefix,noncurrent_days:NoncurrentVersionExpiration.NoncurrentDays,expired_delete_markers:Expiration.ExpiredObjectDeleteMarker}' \
  --output table

# Object Lock enabled on the bucket, and whether a bucket default
# retention D is set (a default retention is level 2; the scoped posture
# expects none here)
aws s3api get-object-lock-configuration --bucket <primary>

# The scoped posture, at every level: a recent object under a protected
# prefix carries per-object retention in compliance mode. Repeat for one
# object per protected prefix family, and for one noncurrent version (a
# superseded catalog HEAD is a good candidate): a mechanism fed by a
# current-version-only inventory leaves noncurrent versions unlocked.
aws s3api get-object-retention --bucket <primary> --key <recent-protected-key>
aws s3api list-object-versions --bucket <primary> --prefix <protected-prefix> --max-keys 5
aws s3api get-object-retention --bucket <primary> --key <protected-key> --version-id <noncurrent-version-id>
```

Confirm in the replication output that `DeleteMarkerReplication` is `Enabled`
(the mandate above), that the destination bucket is in a different account and
region under a different `ReplicaKmsKeyID`, and, if you need a stated RPO,
that RTC (`ReplicationTime`) is enabled. MinIO's bucket replication is
equivalent for these purposes; use the `mc replicate` equivalents against a
MinIO deployment.

## Restore procedure: the replica is a restore source, never a live failover target

The replica is asynchronous, has no cross-bucket compare-and-swap, and its
listing consistency covers only what has arrived. Pointing a live Ravel
deployment at the replica on outage would **silently violate** the
commit-then-visible ordering, the seal, garbage-collection and compaction
reasoning, and the sweeper's re-verify LIST: a data object could be present
without its commit record, or a record without its data, because replication
reordered or lagged them. So there is **no automatic or live failover**, and no
Ravel code path learns about a second bucket. Restore is a deliberate,
verified operation.

1. **Freeze.** Stop every Ravel process writing to the lost or suspect primary
   (region loss usually does this for you). Nothing may write to the restore
   target until step 5.
2. **Choose the restore bucket.** Either promote the replica in place or copy
   it to a fresh bucket. Both are sanctioned: objects are immutable and
   content-addressed, so every replicated object is bit-identical to its
   original; the only skew is presence or absence.
3. **Reconcile to a consistency point.** This repairs replication's lack of
   ordering, with three shipped tools:
   - `ravel-cli maintain verify-custody`, in its versioning-aware mode, finds
     **dangling commit records**: record replicated, data object not.
     Quarantine each (delete under the restore credential, with maintenance
     stopped; see
     [operations/troubleshooting.md](operations/troubleshooting.md#commit-records-were-deleted-out-of-band)).
     Each is counted as data loss against the measured RPO.
   - `ravel-cli commit reconstruct` recovers the opposite skew: **data object
     replicated, record not.** The object's footer carries everything a
     rebuilt record needs. Because ingest completes the data PUT before
     building the record, this converts "the record lagged replication" from
     loss into recovery: the effective RPO is the replication lag of the *data
     object*, not of the record pair.
   - `ravel-cli catalog verify` classifies catalog staleness. Catalog objects
     are derived; the fold rebuilds them over whatever the reconciled
     commit-record set is.
   - `sys/` control objects are each either self-healing (heartbeats,
     qualification, rewritten by the owning process on startup) or idempotent
     under create-if-absent (seal records, provisioning). Any `sys/` object
     found not to self-heal is a **blocking finding**, not a footnote.

   **Lag beyond the protection horizon.** Skew between related maintenance
   objects is bounded by the horizons that already gate the machinery: a
   compaction or rewrite record is published at least `protection_horizon`
   (about 25 h with defaults) before the sweep deletes its inputs, and an
   erasure `.dreq` is deleted at least `protection_horizon` after its `.done`.
   Within that envelope the reconciliation above is complete and the RPO
   definition below holds. If replication lag at disaster time may have
   exceeded `protection_horizon` (no RTC, replication degraded for a day or
   more), you must **also treat erasure state as suspect and re-submit any
   erasure request completed within the lag window**: a restored bucket could
   otherwise serve pre-rewrite inputs whose rewrite record, or whose
   exclusion-keeping `.dreq`, never arrived. Superseded-but-unswept compaction
   duplicates need no such care: overlap harmlessness holds for compaction,
   and only for compaction.
4. **Verify before serving.** `verify-custody` clean, `catalog verify` clean,
   and a canary query set over known-ingested data.
5. **Re-protect before the first process starts.** The restore bucket must
   meet the baseline before Ravel writes to it: versioning on with the
   primary's `NoncurrentDays = E_v` rule and expired-delete-marker cleanup
   installed (a promoted replica still carries `E_v_r`; replace it), and
   Object Lock enabled. Without the lifecycle rules the erasure bound does
   not hold for anything written from this point. At levels 0 and 1 that
   also means no default retention and the
   retention mechanism pointed at the restore bucket and backfilled over the
   restored objects: objects restored before the mechanism runs carry no
   retention, so run the backfill and confirm one current and one noncurrent
   version per protected prefix family carries retention, with the commands
   under "Verification". At level 2 it means the bucket default retention `D`
   set on the restore bucket before the restore copy, so every restored
   object is locked as it lands. The startup flag checks only the bucket half
   of this; the mechanism, or the default retention, is verified by hand.
6. **Resume.** Start Ravel against the restored bucket. Disposable compute
   pays off here: processes mint fresh writer ids and epochs, no local state
   exists to reconcile, and the operator issues fresh per-mode storage
   credentials scoped to the restore bucket.
7. **Replicate and close out.** Re-establish replication to a new replica,
   with the replica's own versioning and lifecycle rules, before declaring
   the incident closed. Until that is done the deployment is level 0.

## RPO and RTO: defined here, published only from a rehearsal

The recovery numbers must come from a real rehearsal, not from estimation.
This runbook therefore publishes **no number**. It defines what the numbers
mean and where they come from:

- **RPO** is the replication lag of acknowledged data at disaster time, plus
  any dangling-record quarantine from step 3. With RTC enabled it has a
  published ceiling (15 minutes for 99.99% of objects, S3's service level
  agreement); **without RTC it has no bound.** A deployment that needs a
  stated RPO enables RTC.
- **RTO** is wall-clock time from freeze to verified resume (steps 1 to 5),
  dominated by reconciliation and scaling with the restored object count. It
  is deployment-sized and cannot be honestly stated in the abstract.
- **Publication rule:** the rehearsal record below carries the measured
  numbers. Until the first rehearsal record exists, the fields read
  **"unmeasured."** No number is invented to fill them. A rehearsal that
  surfaces a blocking finding (a non-self-healing `sys/` object, a
  reconciliation step that fails) blocks publication until fixed. Rehearsals
  re-run when the restore-relevant machinery changes materially, and the
  record keeps its history.

## Rehearsal record

A rehearsal drives the restore procedure above against a real replica and
records the measured outcome here. Until a real rehearsal produces them, the
RPO and RTO fields state **unmeasured**; no estimate is published in their
place.

| Field | Value |
|---|---|
| Date | _unrehearsed_ |
| Environment (tier, store, region/account layout) | _unrehearsed_ |
| Object count restored | _unrehearsed_ |
| **Measured RPO** | **unmeasured** |
| **Measured RTO** | **unmeasured** |
| Anomalies found (blocking / non-blocking) | _unrehearsed_ |

Append a new row per rehearsal; keep prior rows as history.

### Chaos-evidence rehearsal records

A separate process-kill evidence lane lives under `scripts/chaos/`, with one
script per scenario and a shared library. Both scripts run the scenario
end to end against a real MinIO, and both take `--check` (equivalently
`--dry-run`) to validate their structure and dependencies without starting
MinIO, driving load, or issuing a real kill:

| Script | Scenario | Pinned oracle |
|---|---|---|
| `scripts/chaos/kill-ingest-flush.sh` | Drive load, `SIGKILL` the server mid-flush, restart. The kill fires the moment `ravel_ingest_flushes_by_size_total` rises past its pre-load baseline, which is flush-attempt time, so the kill lands inside the flush window. | Every write acknowledged under strict acknowledgement before the kill is durable and queryable after restart; no partial flush becomes visible; custody and catalog verification clean. |
| `scripts/chaos/kill-maintain-worker.sh` | Two `maintain` mode workers under leased maintenance, `SIGKILL` one mid-compaction with the sibling running. The kill fires while the victim owns units and has not yet logged its compaction record as published. | The sibling takes over the dead worker's units within the liveness bound plus one maintenance tick; no unit stays orphaned; the interrupted compaction completes under the conservation gate; the dead worker's partial outputs age out with no leak past the horizon; custody and catalog verification clean. |

A failure of the second scenario is release-blocking, not a flaky test. On any
oracle failure that script names the failed assertions and exits 2, distinct
from 1 for an ordinary failure and from anything above 2 for a setup or usage
error, so the distinction is legible in a rehearsal record.

Record each real run here under the same discipline as the table above. A run
without MinIO can only produce the `--check` result, which is not a rehearsal
record: a real end-to-end run against MinIO is what fills a row.

## Summary

| Level | Controls | Erasure-bound consequence | RPO/RTO |
|---|---|---|---|
| **Every level** | Object Lock enabled on the bucket and versioning ON; at levels 0 and 1, no bucket default retention and an operator-run mechanism applying per-object retention in compliance mode to `sys/`, provisioning records, commit records and catalog HEAD history (level 2 replaces the mechanism with its bucket default retention); `--require-bucket-protection` gates startup on the bucket half (Object Lock enabled, versioning on), and the mechanism or the default retention is verified out of band | None; those prefixes hold no erasable subject value | Not a recovery control |
| **level 0** (default) | Versioning + `NoncurrentDays = E_v` + expired-delete-marker cleanup; no replica | Primary `+E_v` | None; bucket loss is total loss |
| **level 1** (recommended) | Level 0 plus a replica: different region/account/KMS key, replication v2 with `DeleteMarkerReplication`, RTC recommended; the replica versioned with `NoncurrentDays = E_v_r` and expired-delete-marker cleanup | Primary `+E_v`; replica residue is replication lag + `E_v_r` (requires `DeleteMarkerReplication`) | Defined here; **unmeasured** until a rehearsal record exists. RTC gives RPO a 15-minute ceiling; without RTC, unbounded |
| **level 2** (optional) | level 1 plus a bucket default retention `D`, which S3 applies to every object including the data objects | `max(bound, D)`; query-time exclusion still immediate | As level 1 |

`DeleteMarkerReplication` is mandatory for any erasure-obligated deployment;
omitting it leaves erased bytes on the replica indefinitely and is
unsupported. No in-product backup, export, or failover exists; the replica is
a restore source only, reconciled with `verify-custody` and
`commit reconstruct` before it is served.

## Background

The posture, the mandate, the restore procedure, the rehearsal-only
publication rule, and the chaos lane are
[ADR-0077](../adrs/0077-dr-posture-and-chaos-evidence.md), which amends ADR-0058
decision 5. The erasure guarantee whose bound the tiers above modify is
ADR-0064; the bucket-protection contract is ADR-0072 decision 3; the
commit-record reconstruction tool is ADR-0058; the per-mode storage
credentials are ADR-0055.
