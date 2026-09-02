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
never collides with an erasure request. Scoped Object Lock on the protected
prefixes is therefore not a disaster-recovery choice and carries no erasure
cost; it is the baseline the commit and catalog layers already assume.

`--require-bucket-protection` gates startup on it. With the flag set, a bucket
reporting Object Lock disabled, or a versioning misconfiguration, refuses to
start; a backend that cannot disclose its state warns once and raises the
`ravel_bucket_protection_unknown` gauge; a bucket reporting it enabled starts
clean. The flag is off by default, so an existing deployment is unchanged
until an operator turns it on. Enforcement itself stays at the bucket and IAM
layer either way: nothing in a Ravel process can configure Object Lock.

What the levels below add on top of that baseline is replication, and, at
level 2, **bucket-default** retention across every prefix including the data
objects. That last one is the choice with an erasure cost.

## The three levels

Each level states its erasure-bound consequence next to its protection. This
disclosure is load-bearing, not a footnote: turning on versioning and
bucket-wide Object Lock extends the physical erasure bound, and the tension
between disaster recovery and the erasure guarantee is resolved by stating
that cost, never by hiding it. Do not soften or omit it. The bounds the
modifiers apply to are in
[deletion-and-gc.md](../deletion-and-gc.md#modifiers-to-the-bound).

### level 0, no replication (default)

Unversioned bucket beyond the protected prefixes. Erasure bounds hold as
stated in [consistency-model.md](../consistency-model.md) with no modifiers.
RPO and RTO: none; bucket loss is total loss. This remains a **supported
posture**, but its blast radius is total: every durable byte (data objects,
commit records, manifests, catalog snapshots, control objects under `sys/`)
lives in one bucket, and Ravel itself cannot recover from losing it.

### level 1, replicated (the recommended posture)

On the primary bucket:

- Versioning **ON**, paired with a `NoncurrentDays = E_v` noncurrent-version
  expiration rule and expired-delete-marker cleanup. Versioning on without
  that pairing is an unsupported configuration: it turns every Ravel delete
  into a soft delete while every layer above keeps reporting success.

Replication to a replica bucket:

- Replication **v2 configuration** with `DeleteMarkerReplication` **enabled**,
  RTC **recommended**.
- The replica lives in a **different region** and a **different account**,
  encrypted under a **different KMS key** (`ReplicaKmsKeyID`), and holds its
  own `NoncurrentDays = E_v_r` expiration rule.
- Ravel processes never hold replica-account credentials; the replication
  channel is the only writer to the replica.

**Erasure consequence, disclosed:** the primary physical erasure bound gains
`+E_v`. The replica's copy of an erased subject is physically gone within
replication lag plus `E_v_r` after the primary sweep, **provided
`DeleteMarkerReplication` is enabled**. See the mandate below.

### level 2, level 1 plus bucket-default Object Lock retention

Bucket-default retention `D` across every prefix on the primary, the replica,
or both. This is a strict superset of the scoped Object Lock the
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
the cross-account replica stand in for bucket-wide Object Lock at level 1.

## `E_v` is one knob controlling two windows

The load-bearing design insight of level 1: **`E_v` is one knob controlling two
windows.**

- It is the **disaster-detection budget**: after an accidental or malicious
  mass delete, the operator has `E_v_r`, plus `E_v` if the deletes were simple
  deletes, to notice and restore noncurrent versions.
- It is simultaneously the **erasure-residue window**: erased bytes persist as
  noncurrent versions for `E_v`.

Choosing `E_v` is a compliance decision, not a tuning default. Set it
deliberately against **both** your detection objective and your erasure
service level agreement. This runbook refuses to pick a number for you.

## Platform-CLI verification checklist

Ravel cannot enforce most of this and does not pretend to. `ravel-cli store
qualify` probes whether Object Lock and versioning appear enabled and reports
lifecycle-rule state, but **replication configuration is invisible to the
object-store layer**, so verification of the replication controls is an
explicit platform-CLI step and not something Ravel checked. Run these against
the actual buckets:

```sh
# Primary: versioning ON
aws s3api get-bucket-versioning --bucket <primary>

# Primary: NoncurrentDays = E_v expiration + expired-delete-marker cleanup
aws s3api get-bucket-lifecycle-configuration --bucket <primary>

# Primary: replication v2, DeleteMarkerReplication enabled, RTC (if required)
aws s3api get-bucket-replication --bucket <primary>

# Replica: versioning ON, NoncurrentDays = E_v_r expiration
aws s3api get-bucket-versioning --bucket <replica>
aws s3api get-bucket-lifecycle-configuration --bucket <replica>

# Object Lock: compliance mode on the protected prefixes at every level,
# and the bucket-default retention D that makes it level 2
aws s3api get-object-lock-configuration --bucket <primary>
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
5. **Resume.** Start Ravel against the restored bucket. Disposable compute
   pays off here: processes mint fresh writer ids and epochs, no local state
   exists to reconcile, and the operator issues fresh per-mode storage
   credentials scoped to the restore bucket.
6. **Re-protect.** Re-establish versioning, lifecycle rules, the protected
   prefixes' Object Lock, and replication to a new replica before declaring
   the incident closed. An unreplicated restored primary is level 0.

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
| **Every level** | Object Lock compliance mode on `sys/`, provisioning records, commit records and catalog HEAD history, paired with versioning; `--require-bucket-protection` to gate startup on it | None; those prefixes hold no erasable subject value | Not a recovery control |
| **level 0** (default) | No replication | None; bounds as in consistency-model.md | None; bucket loss is total loss |
| **level 1** (recommended) | Primary: versioning + `NoncurrentDays = E_v` + expired-delete-marker cleanup. Replica: different region/account/KMS key, replication v2 with `DeleteMarkerReplication`, RTC recommended, `NoncurrentDays = E_v_r` | Primary `+E_v`; replica residue is replication lag + `E_v_r` (requires `DeleteMarkerReplication`) | Defined here; **unmeasured** until a rehearsal record exists. RTC gives RPO a 15-minute ceiling; without RTC, unbounded |
| **level 2** (optional) | level 1 plus bucket-default Object Lock retention `D` across every prefix | `max(bound, D)`; query-time exclusion still immediate | As level 1 |

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
