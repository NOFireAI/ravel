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
- the catalog keyspace `t/*/catalog/*/*` (the HEAD pointer and its versions,
  and the snapshot and index objects the same pattern reaches).

Versioning is what makes the last of those work: a HEAD compare-and-swap
creates a new locked version rather than overwriting one. None of those four
prefix families spells a subject identifier into an object *key*, so naming
them in the lock never exposes a subject through the key pattern itself. What
those objects contain is a separate question, and it is where the erasure cost
lives. The deployment records, the provisioning records, and the catalog
keyspace are never targets of the three mechanisms that physically remove
tenant data (supersession GC, retention deletion, and subject erasure), so
locking those three costs nothing against those three mechanisms. The commit
records are not exempt even that far: they are deleted, once superseded, by
the same sweeps. Object Lock does not refuse that delete. Ravel's delete names
no version, so on the versioned bucket Object Lock requires it succeeds and
inserts a delete marker: the record reads as absent to Ravel and the sweep
carries on, while the locked version stays in storage until its retain-until
has passed and the noncurrent-version expiration rule removes it. The
retention period therefore extends how long the record's bytes physically
exist, not how long the sweep waits; see the object store contract's
"Required bucket configuration" for the bound.

The catalog keyspace carries a cost of its own, from a fourth mechanism. A
compliance lock on `t/*/catalog/*/*` costs an erasure obligation, not only a
reclamation delay. The unreferenced-catalog sweep deletes the snapshot and
index objects the current HEAD no longer names, and for a tenant that declares
a typed string or bytes attribute column a per-part column-statistics object
among them holds that subject's own column value. A lock over the keyspace
does not delay that delete, which succeeds as a delete marker, but the locked
version keeps the value in storage: the sweep deletes the object only once the
fold has reconciled that hour and the object is older than
`protection_horizon`, and the value then persists until its retain-until has
passed and the noncurrent-version expiration has fired. The maintenance IAM
policy Ravel ships permits that delete: its catalog delete-deny is scoped to
`catalog/<signal>/HEAD`, and the maintenance role holds delete on
`catalog/<signal>/snap/*` and `catalog/<signal>/idx/*`. An operator still
running a copy of `deploy/iam/maintain.json` from before that narrowing has
the old, open-ended bound until they re-apply it, because the sweep is
refused at its first catalog delete every pass. The four-step mechanism, the
exact bound, the IAM ceiling and the HEAD-scoping advice are in the object
store contract's "Required bucket
configuration" section, "A lock on the catalog family". The scoped posture is
therefore still not a disaster-recovery choice; it is the baseline the commit
and catalog layers already assume, with the commit-record family carrying the
physical-removal cost above and the catalog family that cost and, for those
tenants, the erasure bound.

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
      keyspace.

The retention period is unconstrained for the deployment records and the
provisioning records: no sweep deletes either of them, so no choice of period
delays anything. For the commit records and the catalog keyspace the period
does not delay the maintenance sweeps either. Every Ravel delete names no
version, so on the versioned bucket Object Lock requires it succeeds and
inserts a delete marker; Object Lock protects object versions, not the
current-version pointer, and refuses none of those deletes. The sweep carries
on as if the object were unlocked, and the key reads as absent to Ravel. What
the period extends is how long the locked version physically stays in
storage: it is removed only once its retain-until has passed and the
noncurrent-version expiration rule has fired, so a commit record's
physical-removal bound becomes the later of the sweep's bound plus `E_v` and
its retain-until. What does stop a sweep is a delete the store refuses, and
what refuses is a deny policy or a credential without `s3:DeleteObject`, not
the lock. S3 reports that refusal per key inside the 200 response to the
`DeleteObjects` request Ravel sends, and Ravel reads a per-key `AccessDenied`
as access denied. The superseded-input sweep then holds back only the one
supersession chain the refused key belongs to and still collects the others,
unless every delete in the pass was refused. Every other sweep fails the pass
on the first refusal: the unreferenced-catalog sweep abandons that tenant and
signal's whole pass, leaving every unreferenced catalog object behind the
refused one in place until the next maintenance tick, where the same object
is refused again. The catalog keyspace extends the erasure
bound too, for a tenant with a typed string or bytes attribute column and
only for such a tenant; that cost, its exact bound and the shipped-IAM ceiling
are in the object store contract's "Required bucket configuration" section,
"A lock on the catalog family".

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
posture only: Object Lock, versioning and the lifecycle rules. On S3, with the
flag set, the server refuses to start when Object Lock is disabled, when no
enabled `AbortIncompleteMultipartUpload` rule of seven days or less covers
`t/`, when a foreign expiration or transition rule targets `t/` or `sys/`, or
when versioning is on and the noncurrent-version expiration rule is missing or
fails its checks. Any other failed condition, and any condition it cannot
determine (every condition on a backend other than S3, or under an identity
without the read permissions, which no shipped IAM template grants), warns
once, starts, and is counted in the `ravel_bucket_protection_*` gauges. The
[deployment guide](operations/deployment.md#bucket-protection-at-startup)
lists the exact conditions. The flag cannot see whether the retention mechanism is
running, whether every protected object version carries retention, or whether
a bucket default retention is set; those are verified out of band, with the
commands in the "Platform-CLI verification checklist" below (`ravel-cli store
verify-protection` does not check object retention). The flag is off by
default, so an
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
where erasable subject values live. Ravel's deletes still succeed, as delete
markers, but a locked version cannot be removed until its retain-until has
passed, so the physical erasure bound becomes `max(bound + E_v, D)`;
query-time exclusion stays immediate either way.
Where you have erasure obligations, prefer **scoped legal holds** over blanket
default retention, or keep `D` inside the erasure service level agreement.

level 2 is **supported, but not part of the recommended baseline**. Its only
marginal protection over level 1 is against a compromised primary credential
purging version history, and level 1 already contains that threat: version-id
permanent deletes are never replicated, the replica lives in an account whose
credentials Ravel never holds, and the replica retains deleted data as
noncurrent versions for `E_v_r`. Making blanket retention mandatory would
impose `max(bound + E_v, D)` on every replicating deployment's erasure bound to
defend against a threat the cross-account replica already covers. Deployments
whose compliance regime demands bucket-wide write-once-read-many storage take
level 2 as a deliberate choice, with the erasure consequence disclosed.

## `DeleteMarkerReplication` is mandatory for erasure-obligated deployments

Every Ravel delete is a **simple delete**, a `DeleteObjects` request whose
body names only the key; nothing in Ravel ever deletes by version id. On a
versioned bucket a simple delete becomes a **delete
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
  versions on the primary. For a single key, see
  [restoring one overwritten key](#restore-one-overwritten-key-from-its-locked-prior-version). At level 1 the replica has its own window,
  replication lag plus `E_v_r`; the two windows run side by side and do not
  add up.
- It is simultaneously the **erasure-residue window**: erased bytes persist as
  noncurrent versions for `E_v`.

Choosing `E_v` is a compliance decision, not a tuning default. Set it
deliberately against **both** your detection objective and your erasure
service level agreement. This runbook refuses to pick a number for you.

## Platform-CLI verification checklist

Ravel cannot enforce most of this and does not pretend to. `ravel-cli store
verify-protection` runs the primary-bucket half of this checklist from the
bucket's own configuration (see "Running the checklist with ravel-cli" below):
versioning, the lifecycle values including `NoncurrentDays = E_v`, Object Lock
and delete-marker replication. It does not check per-object retention, the
replica bucket, the replication destination's account, region and KMS key, or
RTC; those stay platform-CLI steps. `ravel-cli store qualify` prints the same
bucket probes as informational lines and never fails on them. The versioning
and `NoncurrentDays = E_v` lifecycle checks apply at level 0 and level 1
alike; the replication and replica checks are level 1 only. The commands below
are the manual form of every check, and the only form for the replica. Run
them against the actual buckets, and treat a missing row or a differing value
as a failed check:

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

# The scoped posture, at every level: an object your retention mechanism
# covers carries per-object retention in compliance mode. Pick the object
# yourself rather than from a listing (a listing returns keys in key order,
# oldest shards and hours first): one written after the mechanism's last
# full run but no earlier than its coverage lag (for a scheduled job, its
# interval plus its inventory delay and run time; for an event-driven
# function, its reconciliation interval), under each prefix family the
# mechanism covers and nothing else (under the HEAD-only posture that is
# t/<tenant-hash>/catalog/<signal>/HEAD, not the snapshot and index objects
# beside it). Read the version list of that key, then the retention of its
# current version and of one noncurrent version: a mechanism fed by a
# current-version-only inventory leaves noncurrent versions unlocked. A
# lapsed RetainUntilDate means the object was locked and has aged out, not
# that the mechanism is off.
aws s3api list-object-versions --bucket <primary> --prefix <exact-key>
aws s3api get-object-retention --bucket <primary> --key <exact-key> --version-id <current-version-id>
aws s3api get-object-retention --bucket <primary> --key <exact-key> --version-id <noncurrent-version-id>
```

Confirm in the replication output that `DeleteMarkerReplication` is `Enabled`
(the mandate above), that the destination bucket is in a different account and
region under a different `ReplicaKmsKeyID`, and, if you need a stated RPO,
that RTC (`ReplicationTime`) is enabled. On an S3-compatible store that
implements bucket replication, use that store's own replication
configuration; the mandates above are what it has to satisfy.

### Running the checklist with ravel-cli

`ravel-cli store verify-protection` reads the primary bucket's versioning,
lifecycle, replication and Object Lock configuration over read-only requests
signed with the same `--store s3` credentials every other `ravel-cli` command
uses. Run it at least daily and after any change to the bucket's policy or
lifecycle rules:

```sh
ravel-cli --store s3 --s3-bucket <primary> ... store verify-protection \
  --expected-noncurrent-days <E_v> --expect-replication
```

- `--expected-noncurrent-days` is required: the `E_v` the noncurrent-version
  expiration rule covering `t/` must carry.
- `--expect-replication` makes `delete-marker-replication` count. Pass it at
  level 1.

`object-retention` is not checked by this command: it is always printed as
not checked and never moves the exit code, so exit `0` says nothing about
per-object retention. Check it by hand against the bucket's own retention
configuration: the retention mechanism you run, or the bucket default
retention at level 2, and the `get-object-lock-configuration`,
`list-object-versions` and `get-object-retention` commands in the checklist
above, with the permissions listed at the end of this section.

It prints one line per condition, the condition's identifier, then `pass`,
`fail` or `unknown`, then the reason, and a summary line last:

```
versioning                 pass
noncurrent-expiration      fail    rule "ravel": NoncurrentDays is 10, expected 30
expired-delete-marker      pass
abort-multipart            pass
rule-scope                 pass
no-foreign-rule            pass
delete-marker-replication  unknown not expected, does not affect the exit code: ...
object-lock                pass
object-retention           unknown not checked by this command, does not affect the exit code
verify-protection: FAIL: failed: noncurrent-expiration
```

| Condition | Passes when |
|---|---|
| `versioning` | versioning is `Enabled` |
| `noncurrent-expiration` | an enabled rule covering `t/` expires noncurrent versions after exactly `E_v` days |
| `expired-delete-marker` | an enabled rule covering `t/` removes expired delete markers |
| `abort-multipart` | an enabled rule covering `t/` aborts incomplete multipart uploads within 7 days |
| `rule-scope` | the rules above cover every `t/` prefix |
| `no-foreign-rule` | no other expiration or transition rule targets `t/` or `sys/` |
| `delete-marker-replication` | replication carries `DeleteMarkerReplication` `Enabled` |
| `object-lock` | Object Lock is enabled on the bucket |
| `object-retention` | never: not checked by this command, and never moves the exit code |

The exit code is the verdict: `0` only when every expected condition passes,
`1` when any expected condition fails (the summary names each), and `2` when
none fails but at least one could not be verified, or the bucket's control
plane could not be reached at all (the summary names each). `unknown` is never
`pass`: an access denial, an endpoint with no such API, a response that does
not parse, and a condition missing from the report all exit `2`, and so does a
store other than `--store s3`. A condition that is not expected is still
printed, marked as such, and does not move the exit code. A usage error, such
as a missing `--expected-noncurrent-days`, also exits `2`, before anything is
read and without the per-condition lines, and so does a report that could
not be written to stdout, so a script that treats `2` as "could not verify"
should also check that a summary line was printed.

A lifecycle rule counts as covering `t/` when its scope is the whole bucket,
exactly `t/`, or when enabled rules scoped to `t/0` through `t/f`, one per
lowercase hex digit, each carry the value. Any other split of `t/` cannot be
proven to cover it and reads `unknown`; restate the rules in one of those
shapes, or confirm the coverage by hand. A covering rule that also keeps
`NewerNoncurrentVersions`, covering rules that disagree on `NoncurrentDays`,
and a rule over part of `t/` that expires noncurrent versions sooner than
`E_v` each fail `noncurrent-expiration`.

The identity that runs it needs read-only access: `s3:GetBucketVersioning`,
`s3:GetLifecycleConfiguration`, `s3:GetReplicationConfiguration` and
`s3:GetBucketObjectLockConfiguration`. It writes nothing. Checking object
retention by hand with the `get-object-retention` and `list-object-versions`
commands in the checklist above also needs `s3:GetObjectRetention` and
`s3:ListBucketVersions`.

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

0. **Custody manifest.** Confirm, before touching anything else, that the
   material later steps depend on survived the loss of the primary cluster
   itself, because none of it is Ravel state and none of it replicates with
   the bucket:
   - The deployment key (`--tenant-hash-key-file`), which keys the tenant
     hash scheme. A keyed-tenancy deployment that cannot supply the same key
     back to the restored process will hash the same tenant differently and
     can never rejoin its own existing data.
   - The per-tenant KMS configuration (`--tenant-kms-config`), which maps
     each tenant to its KMS key id for `KmsRoutingStore`. Without it, writes
     for a tenant with a configured key silently fall back to the default
     store's key instead of failing, so this must be confirmed present, not
     assumed.
   - The admin credential used to mint the fresh per-mode storage credentials
     step 6 scopes to the restore bucket.
   All three must be held somewhere that survives the primary cluster's
   loss (a separate secrets manager, a cross-region vault, an offline copy)
   and not only on the primary cluster itself; a custody plan that stores
   them nowhere else is a single point of failure the rest of this runbook
   cannot work around.
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
   installed (a promoted replica still carries `E_v_r`; replace it), the
   `AbortIncompleteMultipartUpload` rule of seven days or less, and Object
   Lock enabled. Without the lifecycle rules the erasure bound does not hold
   for anything written from this point. A server started with
   `--require-bucket-protection` refuses a bucket without Object Lock or the
   multipart-abort rule only when its identity can read the bucket's
   versioning, lifecycle and Object Lock configuration. No template under
   `deploy/iam/` grants those reads, so under a shipped template the check
   reads every condition unknown, warns and starts: grant the three reads to
   the restore bucket's server role, or verify the bucket with `ravel-cli
   store verify-protection` before the first start. At levels 0 and 1 that
   also means no default retention and the
   retention mechanism pointed at the restore bucket and backfilled over the
   restored objects: objects restored before the mechanism runs carry no
   retention, so run the backfill and confirm one current and one noncurrent
   version per protected prefix family carries retention, with the commands
   in the platform-CLI verification checklist. At level 2 it means the bucket default retention `D`
   set on the restore bucket before the restore copy, so every restored
   object is locked as it lands. The startup flag checks only the bucket half
   of this (Object Lock, versioning and the lifecycle rules, but not the
   `NoncurrentDays` value against `E_v`, which `ravel-cli store
   verify-protection` checks); the mechanism, or the default retention, is
   verified by hand.
6. **Resume.** Start Ravel against the restored bucket. Disposable compute
   pays off here: processes mint fresh writer ids and epochs, no local state
   exists to reconcile, and the operator issues fresh per-mode storage
   credentials scoped to the restore bucket.
7. **Replicate and close out.** Re-establish replication to a new replica,
   with the replica's own versioning and lifecycle rules, before declaring
   the incident closed. Until that is done the deployment is level 0.

## Restore one overwritten key from its locked prior version

Object Lock protects object versions, not a key's current version (see
"Required bucket configuration" in
[the object store contract](../object-store-contract.md)). A credential with
write access can PUT a new body to a protected key such as a `sys/*` object or
a `t/<tenant-hash>/<signal>/prov` record, or delete it with no version id,
which inserts a delete marker. Neither request is refused, and every reader
then sees the new current version or no object at all. What compliance mode
guarantees is that the version the key held before stays in the bucket for
its retention period. Recovery is restoring that version as the current one.
This is a primary-bucket operation, not the replica restore above.

1. **Stop the writes.** Revoke or rotate the credential that made the bad
   write, and stop any process that would write the key again, before you
   restore anything; a restore under a live credential can be overwritten
   the same way.
2. **List the key's versions.** `--prefix` also matches longer keys, so read
   only the rows whose `Key` is exactly the one you are restoring:

   ```sh
   aws s3api list-object-versions --bucket <bucket> --prefix <exact-key> \
     --query '{versions: Versions[?Key==`<exact-key>`].[VersionId,IsLatest,LastModified,Size], markers: DeleteMarkers[?Key==`<exact-key>`].[VersionId,IsLatest,LastModified]}'
   ```

   The bad write is the row with `IsLatest` true: a version for an
   overwrite, a delete marker for a delete.
3. **Pick the version to restore.** It is the newest version written before
   the bad write. Confirm it is still locked, and fetch it to inspect:

   ```sh
   aws s3api get-object-retention --bucket <bucket> --key <exact-key> --version-id <good-version-id>
   aws s3api get-object --bucket <bucket> --key <exact-key> --version-id <good-version-id> restore-good.bin
   ```

   If more than one write came after it, check each candidate the same way
   and pick the last one you can show predates the compromise.
4. **Copy it back as the current version.** A copy of a version onto its own
   key adds a new current version with the same bytes; it works the same
   whether the current entry is a bad version or a delete marker. The locked
   versions, good and bad, are left in place, and the bad one ages out with
   the noncurrent-version rule once any retention it carries lapses.

   ```sh
   aws s3api copy-object --bucket <bucket> --key <exact-key> \
     --copy-source '<bucket>/<exact-key>?versionId=<good-version-id>' \
     --checksum-algorithm CRC64NVME
   ```

   `--checksum-algorithm CRC64NVME` stores the checksum the S3 store
   verifies on read under the default `--s3-upload-integrity crc64nvme`;
   without a stored checksum, reads of the key are counted as unverified.
   Omit it on an endpoint that runs with `--s3-upload-integrity off`. A copy
   does not carry the source version's SSE-KMS key over: if the good version
   was written under a KMS key (`--s3-kms-key`, or a tenant's key from
   `--tenant-kms-config`), read it from the `SSEKMSKeyId` that `aws s3api
   head-object --version-id <good-version-id>` prints and add
   `--server-side-encryption aws:kms --ssekms-key-id <key-id>` with it.
5. **Verify.** List the versions again: the newest row is your copy, with
   `IsLatest` true, and the good version and the bad one are both still
   listed. Fetch the current version and compare it byte for byte with the
   good one, then confirm the new current version carries retention under
   your retention posture (the bucket default at level 2, or your retention
   mechanism's next run otherwise), with the commands in the platform-CLI
   verification checklist:

   ```sh
   aws s3api get-object --bucket <bucket> --key <exact-key> restore-current.bin
   cmp restore-good.bin restore-current.bin
   aws s3api get-object-retention --bucket <bucket> --key <exact-key> --version-id <new-current-version-id>
   ```

   A running Ravel process can hold the rolled-back state in memory, so
   restart the processes that read the key; processes carry no local state,
   and a restart re-reads the current version.

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
end to end against a real RustFS, and both take `--check` (equivalently
`--dry-run`) to validate their structure and dependencies without starting
RustFS, driving load, or issuing a real kill:

| Script | Scenario | Pinned oracle |
|---|---|---|
| `scripts/chaos/kill-ingest-flush.sh` | Drive load, `SIGKILL` the server mid-flush, restart. The kill fires the moment `ravel_ingest_flushes_by_size_total` rises past its pre-load baseline, which is flush-attempt time, so the kill lands inside the flush window. | Every write acknowledged under strict acknowledgement before the kill is durable and queryable after restart; no partial flush becomes visible; custody and catalog verification clean. |
| `scripts/chaos/kill-maintain-worker.sh` | Two `maintain` mode workers under leased maintenance, `SIGKILL` one mid-compaction with the sibling running. The kill fires while the victim owns units and has not yet logged its compaction record as published. | The sibling takes over the dead worker's units within the liveness bound plus one maintenance tick; no unit stays orphaned; the interrupted compaction completes under the conservation gate; the dead worker's partial outputs age out with no leak past the horizon; custody and catalog verification clean. |

A failure of the second scenario is release-blocking, not a flaky test. On any
oracle failure that script names the failed assertions and exits 2; its oracle
path exits only 0 or 2, and 3 or more is a setup or usage error with no oracle
verdict, so the distinction is legible in a rehearsal record. The first
scenario exits 1 on an oracle failure and uses the same 3-or-more codes for
setup and usage errors.

Both scenarios run nightly in the `chaos` job of
`.github/workflows/k8s-nightly.yml`, against a RustFS the scripts start
themselves, and that job's error annotation names which exit code each
scenario returned. The helpers they rest on (the label-aware `/metrics`
parser, commit-token extraction, and the read-your-write check) are covered
without a store by `scripts/chaos/lib.test.sh`, which runs on every pull
request in ci.yml's `doc-scripts` job.

Record each real run here under the same discipline as the table above. A run
without RustFS can only produce the `--check` result, which is not a rehearsal
record: a real end-to-end run against RustFS is what fills a row.

## Summary

| Level | Controls | Erasure-bound consequence | RPO/RTO |
|---|---|---|---|
| **Every level** | Object Lock enabled on the bucket and versioning ON; at levels 0 and 1, no bucket default retention and an operator-run mechanism applying per-object retention in compliance mode to `sys/`, provisioning records, commit records and the catalog keyspace `t/*/catalog/*/*` (level 2 replaces the mechanism with its bucket default retention); `--require-bucket-protection` gates startup on the bucket half (Object Lock, versioning and the lifecycle rules), and the mechanism or the default retention is verified out of band | None for `sys/` and the provisioning records (no erasable subject value, and no sweep deletes them). For the commit records, `max(bound + E_v, R)` where `R` is the locked version's retain-until: the sweep's delete succeeds as a delete marker, and the locked version is removed once `R` and noncurrent-version expiry have both passed. For the catalog keyspace, the unreferenced-catalog sweep does delete its snapshot and index objects, and for any tenant with a typed string or bytes attribute column a stale per-part column-statistics object stores an erased value verbatim. That bound is not `max(bound + E_v, R)`: the object stays referenced until the fold reconciles that hour or HEAD is rebuilt, and the sweep deletes it only once it is also older than `protection_horizon`, so it is `max(max(T_f, T_w + protection_horizon) + S + E_v, R)`, where `T_f` is when the fold reconciles that hour (or HEAD is rebuilt), `T_w` is the stale object's `last_modified`, `S` is one sweep interval (default 5 min), and `R` is the locked version's retain-until. The maintenance IAM policy Ravel ships permits that delete (its catalog deny is scoped to `catalog/<signal>/HEAD`); a copy of that template predating the narrowing denies it outright and leaves the bound open-ended until it is re-applied. Scope the mechanism to `catalog/<signal>/HEAD` alone to drop the retention half of it | Not a recovery control |
| **level 0** (default) | Versioning + `NoncurrentDays = E_v` + expired-delete-marker cleanup; no replica | Primary `+E_v` | None; bucket loss is total loss |
| **level 1** (recommended) | Level 0 plus a replica: different region/account/KMS key, replication v2 with `DeleteMarkerReplication`, RTC recommended; the replica versioned with `NoncurrentDays = E_v_r` and expired-delete-marker cleanup | Primary `+E_v`; replica residue is replication lag + `E_v_r` (requires `DeleteMarkerReplication`) | Defined here; **unmeasured** until a rehearsal record exists. RTC gives RPO a 15-minute ceiling; without RTC, unbounded |
| **level 2** (optional) | level 1 plus a bucket default retention `D`, which S3 applies to every object including the data objects | `max(bound + E_v, D)`; query-time exclusion still immediate | As level 1 |

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
