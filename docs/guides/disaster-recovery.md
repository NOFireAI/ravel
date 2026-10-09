# Disaster recovery runbook

Use this runbook to choose a disaster-recovery level, verify the bucket
controls for that level, and restore after a loss. The runbook is normative.

You recover Ravel with bucket-level controls that you own: versioning,
noncurrent-version expiration, and cross-region cross-account replication.
Ravel builds **no in-product backup, export, or failover mechanism**. Object
storage is the only durable backend. The replication channel of the platform
writes the replica. A Ravel process never writes it.

Three acronyms appear on this page. The [glossary](../concepts.md#glossary)
defines them:

- **RPO**, recovery point objective: how much recent data a recovery is
  allowed to lose.
- **RTO**, recovery time objective: how long a recovery is allowed to take.
- **RTC**, replication time control: the S3 replication option that puts a
  published ceiling on replication lag. Only RTC gives RPO a bound.

## Baseline for every level

Every level needs Object Lock in **compliance mode** on the protected
prefixes, paired with versioning. The bucket-protection contract in
[object-store-contract.md](../object-store-contract.md#required-bucket-configuration-adr-0064-7-adr-0072-decision-3)
asks for this, independent of the disaster-recovery level. The protected
prefixes are:

- the deployment records under `sys/`,
- the per-(tenant, signal) provisioning records,
- the commit records,
- the catalog keyspace `t/*/catalog/*/*` (the HEAD pointer and its versions,
  and the snapshot and index objects the same pattern reaches).

Versioning makes the lock on the catalog keyspace work. A HEAD
compare-and-swap creates a new locked version and does not overwrite one.

This scoped posture is the baseline that the commit and catalog layers
assume. It is not a disaster-recovery choice.

Object Lock is enabled per bucket, not per prefix, and Ravel never sets
retention on an object. At levels 0 and 1, a mechanism that you run outside
Ravel applies **per-object retention** to the objects under the protected
prefixes. This mechanism is a requirement of levels 0 and 1. Level 2 replaces
it with a bucket default retention that reaches every object. Level 2
therefore runs no mechanism, and the "no bucket default retention" item below
does not apply to it.

Check off all of these:

- [ ] Object Lock enabled on the bucket, with **no bucket default retention**
      (a default retention would lock the data objects too, which is level 2).
- [ ] Versioning ON.
- [ ] One of the two mechanisms below, applying per-object retention in
      compliance mode, for the chosen retention period, to new objects under
      `sys/`, the provisioning records, the commit records, and the catalog
      keyspace.

### Retention mechanism

| Mechanism | What it does | Coverage window |
|---|---|---|
| Event-driven function | A function subscribed to object-created events, filtered to the protected prefixes, calls the per-object retention API in compliance mode. Events can be delayed or lost and objects written before the subscription raise none, so it needs durable retry with a dead-letter queue, a one-time backfill over existing versions, and a periodic reconciliation against an all-versions S3 Inventory. | Each object is locked within seconds of its creation; a missed event is covered at the next reconciliation. |
| Scheduled batch job | An S3 Batch Operations job, run on a schedule and driven by an all-versions S3 Inventory manifest (`IncludedObjectVersions=All`) filtered to the same prefixes, sets the same retention on every listed version, current and noncurrent. | Up to the schedule interval plus the inventory delay plus the job's own execution and retry time. |

Choose the mechanism whose window your compliance regime accepts. Between the
creation of an object and the moment the mechanism acts on it, the object
carries no retention. In that window, any credential that can delete can
delete it. That window is the residual exposure of the scoped posture. The
AWS reference for both mechanisms is
[S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html).

### Cost of the lock

None of the four prefix families spells a subject identifier into an object
*key*, so the lock never exposes a subject through the key pattern. The cost
comes from what the objects contain.

The lock does not delay a maintenance sweep. Every Ravel delete names no
version. On the versioned bucket that Object Lock requires, the delete
succeeds and inserts a delete marker. Object Lock protects object versions,
not the current-version pointer, and refuses none of those deletes. The key
reads as absent to Ravel, and the sweep carries on as if the object were
unlocked.

The locked version stays in storage until its retain-until has passed and the
noncurrent-version expiration rule has removed it. The retention period
therefore extends how long the bytes physically exist. It does not extend how
long the sweep waits.

The cost differs for each prefix family:

- **Deployment records and provisioning records.** No sweep deletes either of
  them, so the retention period is unconstrained and delays nothing.
- **Commit records.** Three mechanisms physically remove tenant data:
  supersession GC, retention deletion, and subject erasure. Their sweeps
  delete a commit record once it is superseded. The physical-removal bound of
  a commit record becomes the later of the sweep's bound plus `E_v` and its
  retain-until. See "Required bucket configuration" in the object store
  contract for the bound.
- **Catalog keyspace.** The three mechanisms above never target the catalog
  keyspace. A fourth mechanism does: the unreferenced-catalog sweep deletes
  the snapshot and index objects that the current HEAD no longer names. A
  compliance lock on `t/*/catalog/*/*` therefore carries the same
  physical-removal cost. For some tenants it also costs an erasure obligation,
  not only a reclamation delay.

The erasure obligation of the catalog keyspace applies to a tenant that
declares a typed string or bytes attribute column, and only to such a tenant:

- A per-part column-statistics object among the swept objects holds the
  column value of the subject.
- The sweep deletes the object only once the fold has reconciled that hour
  and the object is older than `protection_horizon`.
- The delete succeeds as a delete marker, but the locked version keeps the
  value in storage. The value persists until its retain-until has passed and
  the noncurrent-version expiration has fired.
- The maintenance IAM policy that Ravel ships permits that delete. Its catalog
  delete-deny is scoped to `catalog/<signal>/HEAD`, and the maintenance role
  holds delete on `catalog/<signal>/snap/*` and `catalog/<signal>/idx/*`.
- If you still run a copy of `deploy/iam/maintain.json` from before that
  narrowing, the bound is open-ended until you re-apply the template. The
  sweep is refused at its first catalog delete on every pass.

The four-step mechanism, the exact bound, the IAM ceiling and the
HEAD-scoping advice are in the object store contract's "Required bucket
configuration" section, "A lock on the catalog family".

### Refused deletes

A sweep stops only when the store refuses a delete. A deny policy refuses a
delete, and so does a credential without `s3:DeleteObject`. The lock does
not. S3 reports that refusal per key inside the 200 response to the
`DeleteObjects` request that Ravel sends. Ravel reads a per-key
`AccessDenied` as access denied.

- The superseded-input sweep holds back only the one supersession chain that
  the refused key belongs to. It still collects the other chains, unless
  every delete in the pass was refused.
- Every other sweep fails the pass on the first refusal. The
  unreferenced-catalog sweep then abandons the whole pass for that tenant
  and signal. Every unreferenced catalog object behind the refused one stays
  in place until the next maintenance tick, where the same object is refused
  again.

### Startup check

`--require-bucket-protection` gates startup on the bucket half of the
baseline only: Object Lock, versioning and the lifecycle rules. The flag is
off by default, so an existing deployment is unchanged until an operator
turns it on.

On S3, with the flag set, the server refuses to start in each of these cases:

- Object Lock is disabled.
- No enabled `AbortIncompleteMultipartUpload` rule of seven days or less
  covers `t/`.
- A foreign expiration or transition rule targets `t/` or `sys/`.
- Versioning is on and the noncurrent-version expiration rule is missing or
  fails its checks.

For any other failed condition, the server warns once, starts, and counts the
condition in the `ravel_bucket_protection_*` gauges. It does the same for a
condition that it cannot determine. The server cannot determine any condition
on a backend other than S3. It also cannot determine any condition under an
identity without the read permissions, which no shipped IAM template grants.
The [deployment guide](operations/deployment.md#bucket-protection-at-startup)
lists the exact conditions.

The flag cannot see these three things:

- whether the retention mechanism is running,
- whether every protected object version carries retention,
- whether a bucket default retention is set.

Verify them out of band, with the commands in the "Platform-CLI verification
checklist" below. `ravel-cli store verify-protection` does not check object
retention. Enforcement stays at the bucket and IAM layer with or without the
flag: nothing in a Ravel process can configure Object Lock.

## The three levels

The levels add replication to the baseline. Level 2 also adds a **bucket
default retention** across every object, including the data objects. That
retention needs no mechanism, and it has an erasure cost.

Versioning and a bucket default retention extend the physical erasure bound.
Each level states that cost next to its protection. The bounds that the
modifiers apply to are in
[deletion-and-gc.md](../deletion-and-gc.md#modifiers-to-the-bound).

### Level 0 (default)

Level 0 has no replica. It has these controls:

- Versioning on the whole bucket. The baseline requires it, because Object
  Lock cannot be enabled without it, and S3 versioning is bucket-wide. No
  bucket is versioned only under the protected prefixes. So every overwrite
  and delete of a data object leaves a noncurrent version behind.
- A `NoncurrentDays = E_v` noncurrent-version expiration rule.
- Expired-delete-marker cleanup.

The physical erasure bound of level 0 is the primary bound plus `+E_v`, the
same primary half that level 1 states. Erased bytes persist as noncurrent
versions until the rule removes them. The base bounds are in
[consistency-model.md](../consistency-model.md). The `+E_v` modifier is in
[deletion-and-gc.md](../deletion-and-gc.md#modifiers-to-the-bound).

Versioning on without that lifecycle rule is not a level. Without the rule,
the noncurrent residue is unbounded and the bounds in this guide do not
apply. Every delete becomes a soft delete that survives indefinitely, while
every layer above keeps reporting success.

RPO and RTO: none. Bucket loss is total loss. Level 0 is a **supported
posture**. Every durable byte (data objects, commit records, manifests,
catalog snapshots, control objects under `sys/`) lives in one bucket, and
Ravel cannot recover from losing that bucket.

### Level 1 (recommended)

Level 1 is level 0 plus a replica. The primary keeps the level-0
configuration unchanged: versioning on, the `NoncurrentDays = E_v`
noncurrent-version expiration rule, and expired-delete-marker cleanup.

Replication to a replica bucket:

- Replication **v2 configuration** with `DeleteMarkerReplication` **enabled**,
  RTC **recommended**.
- The replica lives in a **different region** and a **different account**,
  encrypted under a **different KMS key** (`ReplicaKmsKeyID`).
- Replication requires versioning on both buckets, so the replica is
  versioned too. It carries its own `NoncurrentDays = E_v_r` expiration rule
  and expired-delete-marker cleanup.
- Ravel processes never hold replica-account credentials. The replication
  channel is the only writer to the replica.

**Erasure consequence:** the primary physical erasure bound gains `+E_v`. The
replica's copy of an erased subject is physically gone within replication lag
plus `E_v_r` after the primary sweep, **provided `DeleteMarkerReplication` is
enabled**. See [Delete-marker replication](#delete-marker-replication).

### Level 2 (optional)

Level 2 is level 1 plus a bucket default retention `D` on the primary, the
replica, or both. S3 applies it to every object at write time, so this level
needs no mechanism and has no coverage window. Level 2 is **supported, but
not part of the recommended baseline**.

Level 2 is a strict superset of the scoped posture that the bucket-protection
contract asks for. It reaches the data objects too, which is where erasable
subject values live.

**Erasure consequence:** the deletes of Ravel still succeed, as delete
markers. A locked version cannot be removed until its retain-until has
passed, so the physical erasure bound becomes `max(bound + E_v, D)`.
Query-time exclusion stays immediate. If you have erasure obligations, prefer
**scoped legal holds** over blanket default retention, or keep `D` inside the
erasure service level agreement.

The only marginal protection of level 2 over level 1 is against a compromised
primary credential that purges version history. Level 1 already contains that
threat:

- Version-id permanent deletes are never replicated.
- The replica lives in an account whose credentials Ravel never holds.
- The replica retains deleted data as noncurrent versions for `E_v_r`.

Take level 2 when your compliance regime demands bucket-wide
write-once-read-many storage.

## Delete-marker replication

If the deployment has erasure obligations, enable `DeleteMarkerReplication`.
For such a deployment it is **MANDATORY**.

Every Ravel delete is a **simple delete**, a `DeleteObjects` request whose
body names only the key. Nothing in Ravel deletes by version id. On a
versioned bucket a simple delete becomes a **delete marker**. A delete marker
replicates to the replica **only when `DeleteMarkerReplication` is enabled**.

Without it, **erased bytes persist on the replica indefinitely**, because the
delete marker that removes them never arrives. That configuration is
**unsupported** for any deployment with erasure obligations.
[deletion-and-gc.md](../deletion-and-gc.md#modifiers-to-the-bound) says the
same.

Version-id permanent deletes are **never** replicated. A compromised primary
credential therefore cannot purge the replica through the replication
channel. This property lets the cross-account replica stand in for a bucket
default retention at level 1.

## The two `E_v` windows

`E_v` applies from level 0, because the bucket is versioned there. This one
value controls two windows:

- The **disaster-detection budget**. After an accidental or malicious mass
  delete, you have `E_v` to notice and restore the noncurrent versions on the
  primary. For a single key, see
  [Restore one overwritten key](#restore-one-overwritten-key). At level 1 the
  replica has its own window, replication lag plus `E_v_r`. The two windows
  run side by side and do not add up.
- The **erasure-residue window**. Erased bytes persist as noncurrent versions
  for `E_v`.

The choice of `E_v` is a compliance decision. Set it against **both** your
detection objective and your erasure service level agreement. This runbook
gives no number.

## Platform-CLI verification checklist

Ravel cannot enforce most of these controls. Run this checklist against the
actual buckets, and treat a missing row or a differing value as a failed
check.

`ravel-cli store verify-protection` runs the primary-bucket half of the
checklist from the configuration of the bucket. See
[Running the checklist with ravel-cli](#running-the-checklist-with-ravel-cli).

| Check | Run by `verify-protection` |
|---|---|
| Versioning | Yes |
| The lifecycle values, including `NoncurrentDays = E_v` | Yes |
| Object Lock | Yes |
| Delete-marker replication | Yes |
| Per-object retention | No |
| The replica bucket | No |
| The account, region and KMS key of the replication destination | No |
| RTC | No |

The checks that `verify-protection` does not run stay platform-CLI steps.
`ravel-cli store qualify` prints the same bucket probes as informational
lines and never fails on them.

The versioning and `NoncurrentDays = E_v` lifecycle checks apply at level 0
and level 1 alike. The replication and replica checks are level 1 only.

The commands below are the manual form of every check, and the only form for
the replica:

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

In the replication output, make sure that:

- `DeleteMarkerReplication` is `Enabled`. See
  [Delete-marker replication](#delete-marker-replication).
- The destination bucket is in a different account and region, under a
  different `ReplicaKmsKeyID`.
- If you need a stated RPO, RTC (`ReplicationTime`) is enabled.

On an S3-compatible store that implements bucket replication, use the
replication configuration of that store. It must satisfy the same mandates.

### Running the checklist with ravel-cli

`ravel-cli store verify-protection` reads the versioning, lifecycle,
replication and Object Lock configuration of the primary bucket. It writes
nothing. It uses read-only requests signed with the same `--store s3`
credentials that every other `ravel-cli` command uses.

Run it at least daily, and after any change to the policy or the lifecycle
rules of the bucket:

```sh
ravel-cli --store s3 --s3-bucket <primary> ... store verify-protection \
  --expected-noncurrent-days <E_v> --expect-replication
```

- `--expected-noncurrent-days` is required: the `E_v` the noncurrent-version
  expiration rule covering `t/` must carry.
- `--expect-replication` makes `delete-marker-replication` count. Pass it at
  level 1.

The identity that runs the command needs these read-only permissions:
`s3:GetBucketVersioning`, `s3:GetLifecycleConfiguration`,
`s3:GetReplicationConfiguration` and `s3:GetBucketObjectLockConfiguration`.

The command prints one line per condition: the identifier of the condition,
then `pass`, `fail` or `unknown`, then the reason. A summary line comes last:

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

The exit code is the verdict:

| Exit code | Meaning |
|---|---|
| `0` | Every expected condition passes. |
| `1` | An expected condition fails. The summary names each. |
| `2` | No condition fails, but at least one could not be verified, or the control plane of the bucket could not be reached at all. The summary names each. |

`unknown` is never `pass`. Each of these exits `2`:

- an access denial,
- an endpoint with no such API,
- a response that does not parse,
- a condition missing from the report,
- a store other than `--store s3`,
- a report that could not be written to stdout,
- a usage error, such as a missing `--expected-noncurrent-days`.

On a usage error, the command exits before it reads anything and prints no
per-condition lines. A script that treats `2` as "could not verify" must
therefore also check that a summary line was printed.

A condition that is not expected is still printed, marked as such, and does
not move the exit code.

#### Lifecycle rule coverage

A lifecycle rule counts as covering `t/` in each of these shapes:

- Its scope is the whole bucket.
- Its scope is `t/`, with nothing after it.
- Enabled rules scoped to `t/0` through `t/f`, one per lowercase hex digit,
  each carry the value.

Any other split of `t/` cannot be proven to cover it and reads `unknown`.
Restate the rules in one of those shapes, or confirm the coverage by hand.

Each of these fails `noncurrent-expiration`:

- a covering rule that also keeps `NewerNoncurrentVersions`,
- covering rules that disagree on `NoncurrentDays`,
- a rule over part of `t/` that expires noncurrent versions sooner than
  `E_v`.

#### Object retention

The command does not check `object-retention`. It always prints the condition
as not checked and never moves the exit code, so exit `0` says nothing about
per-object retention.

Check object retention by hand against the retention configuration of the
bucket. That configuration is the retention mechanism that you run, or the
bucket default retention at level 2. Use the `get-object-lock-configuration`,
`list-object-versions` and `get-object-retention` commands in the checklist
above. The `get-object-retention` and `list-object-versions` commands also
need `s3:GetObjectRetention` and `s3:ListBucketVersions`.

## Restore from the replica

Use the replica as a restore source. It is never a live failover target.
Ravel has **no automatic or live failover**, and no Ravel code path learns
about a second bucket. Restore is a verified operation that you run.

Do not point a live Ravel deployment at the replica during an outage. The
replica is asynchronous, has no cross-bucket compare-and-swap, and its listing
consistency covers only what has arrived. Replication reorders or delays
objects, so a data object can be present without its commit record, or a
record without its data. A live deployment on the replica **silently
violates** the commit-then-visible ordering, the seal, garbage-collection and
compaction reasoning, and the sweeper's re-verify LIST.

0. **Custody manifest.** Before you touch anything else, make sure that the
   material below survived the loss of the primary cluster. Later steps
   depend on it. None of it is Ravel state, and none of it replicates with
   the bucket.
   - The deployment key (`--tenant-hash-key-file`), which keys the tenant
     hash scheme. A keyed-tenancy deployment must supply the same key to the
     restored process. Otherwise it hashes the same tenant differently and
     can never rejoin its own existing data.
   - The per-tenant KMS configuration (`--tenant-kms-config`), which maps
     each tenant to its KMS key id for `KmsRoutingStore`. Without it, writes
     for a tenant with a configured key silently fall back to the key of the
     default store and do not fail. Make sure that it is present. Do not
     assume it.
   - The admin credential used to mint the fresh per-mode storage credentials
     that step 6 scopes to the restore bucket.
   - The audit token key (`RAVEL_AUDIT_TOKEN_KEY`, 64 hex characters), for
     every unkeyed deployment (`--tenant-hash-unkeyed`) and for any
     deployment that sets it explicitly. A keyed deployment that does not set
     it derives it from the deployment key, but an explicitly set key takes
     precedence over that derivation. An unkeyed deployment has nothing to
     derive it from, and a server that serves queries refuses to start under
     the default `--audit-text redacted` without it. Restoring with a
     different key starts the server but tokenizes new audit text differently
     from the audit records already in the bucket.

   All of it must be held somewhere that survives the loss of the primary
   cluster: a separate secrets manager, a cross-region vault, or an offline
   copy. If they are stored only on the primary cluster, they are a single
   point of failure that the rest of this runbook cannot work around.
1. **Freeze.** Stop every Ravel process that writes to the lost or suspect
   primary. Region loss usually does this for you. Let nothing write to the
   restore target until step 5.
2. **Choose the restore bucket.** Promote the replica in place, or copy it to
   a fresh bucket. Both are sanctioned. Objects are immutable and
   content-addressed, so every replicated object is bit-identical to its
   original. The only skew is presence or absence.
3. **Reconcile to a consistency point.** This step repairs the lack of
   ordering in replication, with three shipped tools:
   1. Run `ravel-cli maintain verify-custody` in its versioning-aware mode.
      It finds **dangling commit records**: record replicated, data object
      not.
   2. Quarantine each dangling commit record. To do so, delete it under the
      restore credential, with maintenance stopped. See
      [operations/troubleshooting.md](operations/troubleshooting.md#commit-records-were-deleted-out-of-band).
      Count each one as data loss against the measured RPO.
   3. Run `ravel-cli commit reconstruct`. It recovers the opposite skew:
      **data object replicated, record not.** The footer of the object
      carries everything that a rebuilt record needs. Ingest completes the
      data PUT before it builds the record, so a record that lagged
      replication is recovered and is not a loss. The effective RPO is the
      replication lag of the *data object*, not of the record pair.
   4. Run `ravel-cli catalog verify`. It classifies catalog staleness.
      Catalog objects are derived. The fold rebuilds them over the reconciled
      commit-record set.

   The `sys/` control objects are each either self-healing (heartbeats,
   qualification, rewritten by the owning process on startup) or idempotent
   under create-if-absent (seal records, provisioning). Any `sys/` object
   found not to self-heal is a **blocking finding**.

   **Lag beyond the protection horizon.** Replication lag at disaster time
   can have exceeded `protection_horizon` (no RTC, replication degraded for a
   day or more). In that case, **also treat erasure state as suspect and
   re-submit any erasure request completed within the lag window**. Otherwise
   a restored bucket can serve pre-rewrite inputs whose rewrite record, or
   whose exclusion-keeping `.dreq`, never arrived.

   The horizons that gate the machinery bound the skew between related
   maintenance objects:
   - A compaction or rewrite record is published at least
     `protection_horizon` (about 25 h with defaults) before the sweep deletes
     its inputs.
   - An erasure `.dreq` is deleted at least `protection_horizon` after its
     `.done`.

   Within that envelope the reconciliation above is complete and the RPO
   definition below holds. Superseded-but-unswept compaction duplicates need
   no such care: overlap harmlessness holds for compaction, and only for
   compaction.
4. **Verify before serving.** Make sure that `verify-custody` is clean and
   `catalog verify` is clean. Then run a canary query set over known-ingested
   data.
5. **Re-protect before the first process starts.** Make the restore bucket
   meet the baseline before Ravel writes to it. Without the lifecycle rules,
   the erasure bound does not hold for anything written from this point.
   1. Make sure that versioning is on.
   2. Make sure that the `NoncurrentDays = E_v` rule of the primary and
      expired-delete-marker cleanup are installed. A promoted replica still
      carries `E_v_r`. Replace it.
   3. Make sure that the `AbortIncompleteMultipartUpload` rule of seven days
      or less is installed.
   4. Make sure that Object Lock is enabled.
   5. At levels 0 and 1, make sure that no default retention is set. Point
      the retention mechanism at the restore bucket and run the backfill over
      the restored objects. Objects restored before the mechanism runs carry
      no retention.
   6. At levels 0 and 1, make sure that one current and one noncurrent
      version per protected prefix family carries retention. Use the commands
      in the platform-CLI verification checklist.
   7. At level 2, set the bucket default retention `D` on the restore bucket
      before the restore copy. Every restored object is then locked as it
      lands.

   A server started with `--require-bucket-protection` refuses a bucket
   without Object Lock or the multipart-abort rule. It does so only when its
   identity can read the bucket's versioning, lifecycle and Object Lock
   configuration. No
   template under `deploy/iam/` grants those reads. Under a shipped template
   the check therefore reads every condition unknown, warns and starts. Grant
   the three reads to the restore bucket's server role, or verify the bucket
   with `ravel-cli store verify-protection` before the first start.

   The startup flag checks only the bucket half of this step: Object Lock,
   versioning and the lifecycle rules. It does not check the `NoncurrentDays`
   value against `E_v`, which `ravel-cli store verify-protection` checks.
   Verify the mechanism, or the default retention, by hand.
6. **Resume.** Start Ravel against the restored bucket. Processes mint fresh
   writer ids and epochs, and no local state exists to reconcile. Issue fresh
   per-mode storage credentials scoped to the restore bucket.
7. **Replicate and close out.** Re-establish replication to a new replica,
   with the replica's own versioning and lifecycle rules, before you declare
   the incident closed. Until that is done the deployment is level 0.

## Restore one overwritten key

Use this procedure to restore the prior version of a protected key on the
primary bucket, after a bad write or a delete. It is not the replica restore
above.

Object Lock protects object versions, not a key's current version (see
"Required bucket configuration" in
[the object store contract](../object-store-contract.md)). A credential with
write access can PUT a new body to a protected key such as `sys/tenancy` or
a `t/<tenant-hash>/<signal>/prov` record. It can also delete the key with no
version id, which inserts a delete marker. Object Lock refuses neither
request. Every reader then sees the new current version or no object at all.

The shipped role templates in `deploy/iam/` deny the delete on these keys. A
leaked role credential can therefore overwrite one but not delete it. A
credential outside those templates can do both.

The version that the key held before stays in the bucket. Recovery restores
that version as the current one, within the window that applies:

| Prior version | Stays in the bucket |
|---|---|
| Covered by compliance-mode retention (at level 2, or once your retention mechanism has locked that version) | For its retention period. |
| Not covered by retention | Only until the noncurrent-version expiration rule removes it, `E_v` after it stopped being current. |
| `t/<tenant-hash>/enc` | Only until that rule removes it. This key is not under Object Lock at all. |

### When to escalate

Restoring a version discards every write that landed after it, not only the
bad one. Ravel rewrites some of these keys in normal operation:

- A `prov` record gains an entry when a shard generation or a format floor is
  appended.
- `t/<tenant-hash>/enc` gains an entry at each key-epoch change.
- `t/<tenant-hash>/catalog/<signal>/HEAD` moves on every fold.

Use this procedure on such a key only when the bad write is the only write
after the version you restore. In each of these cases, escalate and do not
restore:

- A legitimate write landed after the version. A writer can already have
  acted on the entry that the restore drops.
- Ingest, a fold or a maintenance pass ran while the bad version was current.
  Readers can act on the rolled-back version without writing the key. Ingest
  routers re-read a `prov` record within a minute and route new records on
  the shard history they read. Restoring the record moves none of those
  records back.
- The key is a rolled-back catalog HEAD. The maintenance sweep deletes the
  snapshot and index objects that the current HEAD does not name once they
  pass the protection horizon. After the bad write, the objects that the
  good version names can already be gone. A restored HEAD that names a
  deleted object fails reads even though the byte comparison of step 5
  passes.

### Procedure

1. **Stop the writes.** Do this before you restore anything. A restore under
   a live credential can be overwritten the same way.
   1. Revoke or rotate the credential that made the bad write.
   2. Stop any process that can write the key again.
2. **List the key's versions.** `--prefix` also matches longer keys. Read
   only the rows whose `Key` is the key that you restore, with no suffix:

   ```sh
   aws s3api list-object-versions --bucket <bucket> --prefix <exact-key> \
     --query '{versions: Versions[?Key==`"<exact-key>"`].[VersionId,IsLatest,LastModified,Size], markers: DeleteMarkers[?Key==`"<exact-key>"`].[VersionId,IsLatest,LastModified]}'
   ```

   The bad write is the row with `IsLatest` true: a version for an
   overwrite, a delete marker for a delete.
3. **Pick the version to restore.** It is the newest version written before
   the bad write. Make sure that it is still locked, and fetch it to inspect:

   ```sh
   aws s3api get-object-retention --bucket <bucket> --key <exact-key> --version-id <good-version-id>
   aws s3api get-object --bucket <bucket> --key <exact-key> --version-id <good-version-id> restore-good.bin
   ```

   If more than one write came after it, check each candidate the same way.
   Pick the last one that you can show predates the compromise.
4. **Copy it back as the current version.** A copy of a version onto its own
   key adds a new current version with the same bytes. It works the same
   whether the current entry is a bad version or a delete marker. The locked
   versions, good and bad, stay in place. The bad one ages out with the
   noncurrent-version rule once any retention it carries lapses.

   ```sh
   aws s3api copy-object --bucket <bucket> --key <exact-key> \
     --copy-source '<bucket>/<exact-key>?versionId=<good-version-id>' \
     --checksum-algorithm CRC64NVME
   ```

   - `--checksum-algorithm CRC64NVME` stores a checksum that the S3 store can
     recompute when it reads the key with `--s3-request-stored-checksum` on
     (the default).
   - Without the flag, AWS keeps the checksum algorithm of the source version
     on a copy. An endpoint that stores no checksum leaves reads of the key
     counted as unverified.
   - If the endpoint rejects checksum headers, omit the flag. Such an
     endpoint runs with `--s3-upload-integrity off`.
   - A copy does not carry the SSE-KMS key of the source version over. If the
     good version was written under a KMS key (`--s3-kms-key`, or a tenant's
     key from `--tenant-kms-config`), read it from the `SSEKMSKeyId` that
     `aws s3api head-object --bucket <bucket> --key <exact-key> --version-id
     <good-version-id>` prints. Then add `--server-side-encryption aws:kms
     --ssekms-key-id <key-id>` with it. The credential that runs the copy
     needs `kms:Decrypt` and `kms:GenerateDataKey` on that key.
5. **Verify.**
   1. List the versions again. The newest row is your copy, with `IsLatest`
      true. The good version and the bad one are both still listed.
   2. Fetch the current version and compare it byte for byte with the good
      one.
   3. Make sure that the new current version carries retention under your
      retention posture. That posture is the bucket default at level 2, or
      the next run of your retention mechanism otherwise. Use the commands in
      the platform-CLI verification checklist.

   ```sh
   aws s3api get-object --bucket <bucket> --key <exact-key> restore-current.bin
   cmp restore-good.bin restore-current.bin
   aws s3api get-object-retention --bucket <bucket> --key <exact-key> --version-id <new-current-version-id>
   ```

   Then restart the processes that read the key. A running Ravel process can
   hold the rolled-back state in memory. Processes carry no local state, and
   a restart re-reads the current version.

## RPO and RTO

This runbook publishes **no number**. The recovery numbers must come from a
real rehearsal, not from estimation. The definitions are:

- **RPO** is the replication lag of acknowledged data at disaster time, plus
  any dangling-record quarantine from step 3. With RTC enabled it has a
  published ceiling (15 minutes for 99.99% of objects, S3's service level
  agreement). **Without RTC it has no bound.** A deployment that needs a
  stated RPO enables RTC.
- **RTO** is wall-clock time from freeze to verified resume (steps 1 to 5).
  Reconciliation dominates it, and it scales with the restored object count.
  It depends on the size of the deployment, so this runbook states no general
  figure.
- **Publication rule:** the rehearsal record below carries the measured
  numbers. Until the first rehearsal record exists, the fields read
  **"unmeasured."** No number is invented to fill them. A rehearsal that
  surfaces a blocking finding (a non-self-healing `sys/` object, a
  reconciliation step that fails) blocks publication until fixed. Rehearsals
  re-run when the restore-relevant machinery changes materially, and the
  record keeps its history.
- **What a harness rehearsal's RPO is (owner decision, 2026-10-08):** the RPO
  recorded for a rehearsal run with `scripts/dr/rehearse.sh` is a
  restore-completeness figure, not the replication lag the definition above
  describes. The harness does not compute it; the operator fills the record
  from the canary check's result. It counts acknowledged samples (rows)
  missing from the target bucket after the restore: the samples `seed.sh` had
  acknowledged under strict ack, minus the samples the canary check reads back
  from bucket B. The seeded corpus carries one sample per export, so the
  acknowledged count is the number of exports that returned a commit token.
  The canary check fails on any difference, more samples as well as fewer. For a
  quiesced client-side mirror the expected value is exactly 0: `seed.sh`
  stops its writer before `replicate.sh` copies bucket A with
  `aws s3 cp --recursive`, so every acknowledged sample is already in A when
  the copy starts. Measuring replication lag needs bucket replication and a
  live writer during the disaster, and the harness uses neither.

## Rehearsal record

A rehearsal drives the restore procedure above against a real replica and
records the measured outcome here.

| Field | Value |
|---|---|
| Date | _unrehearsed_ |
| Environment (tier, store, region/account layout) | _unrehearsed_ |
| Object count restored | _unrehearsed_ |
| **Measured RPO (restore completeness: acknowledged samples missing after restore, not replication lag)** | **unmeasured** |
| **Measured RTO** | **unmeasured** |
| Anomalies found (blocking / non-blocking) | _unrehearsed_ |

Append a new row per rehearsal. Keep prior rows as history.

### Chaos-evidence rehearsal records

A separate process-kill evidence lane lives under `scripts/chaos/`, with one
script per scenario and a shared library. Both scripts run the scenario end
to end against a real RustFS. Both take `--check` (equivalently `--dry-run`)
to validate their structure and dependencies without starting RustFS, driving
load, or issuing a real kill:

| Script | Scenario | Pinned oracle |
|---|---|---|
| `scripts/chaos/kill-ingest-flush.sh` | Drive load, `SIGKILL` the server mid-flush, restart. The kill fires the moment `ravel_ingest_flushes_by_size_total` rises past its pre-load baseline, which is flush-attempt time, so the kill lands inside the flush window. | Every write acknowledged under strict acknowledgement before the kill is durable and queryable after restart; no partial flush becomes visible; custody and catalog verification clean. |
| `scripts/chaos/kill-maintain-worker.sh` | Two `maintain` mode workers under leased maintenance, `SIGKILL` one mid-compaction with the sibling running. The kill fires while the victim owns units and has not yet logged its compaction record as published. | The sibling takes over the dead worker's units within the liveness bound plus one maintenance tick; no unit stays orphaned; the interrupted compaction completes under the conservation gate; the dead worker's partial outputs age out with no leak past the horizon; custody and catalog verification clean. |

A failure of the second scenario is release-blocking. The exit codes are:

- The second script names the failed assertions and exits 2 on any oracle
  failure. Its oracle path exits only 0 or 2.
- The first script exits 1 on an oracle failure.
- For both scripts, 3 or more is a setup or usage error with no oracle
  verdict.

Both scenarios run nightly in the `chaos` job of
`.github/workflows/k8s-nightly.yml`, against a RustFS that the scripts start
themselves. The error annotation of that job names which exit code each
scenario returned.

`scripts/chaos/lib.test.sh` covers the helpers that the scripts rest on,
without a store: the label-aware `/metrics` parser, commit-token extraction,
and the read-your-write check. It runs on every pull request in the
`doc-scripts` job of ci.yml.

Record each real run here, under the same rule as the table above. Only a
real end-to-end run against RustFS fills a row. A run without RustFS can only
produce the `--check` result, which is not a rehearsal record.

## Summary

| Level | Controls | Erasure-bound consequence | RPO/RTO |
|---|---|---|---|
| **Every level** | Object Lock enabled on the bucket and versioning ON; at levels 0 and 1, no bucket default retention and an operator-run mechanism applying per-object retention in compliance mode to `sys/`, provisioning records, commit records and the catalog keyspace `t/*/catalog/*/*` (level 2 replaces the mechanism with its bucket default retention); `--require-bucket-protection` gates startup on the bucket half (Object Lock, versioning and the lifecycle rules), and the mechanism or the default retention is verified out of band | None for `sys/` and the provisioning records (no erasable subject value, and no sweep deletes them). For the commit records, `max(bound + E_v, R)` where `R` is the locked version's retain-until: the sweep's delete succeeds as a delete marker, and the locked version is removed once `R` and noncurrent-version expiry have both passed. For the catalog keyspace, the unreferenced-catalog sweep does delete its snapshot and index objects, and for any tenant with a typed string or bytes attribute column a stale per-part column-statistics object stores an erased value verbatim. That bound is not `max(bound + E_v, R)`: the object stays referenced until the fold reconciles that hour or HEAD is rebuilt, and the sweep deletes it only once it is also older than `protection_horizon`, so it is `max(max(T_f, T_w + protection_horizon) + S + E_v, R)`, where `T_f` is when the fold reconciles that hour (or HEAD is rebuilt), `T_w` is the stale object's `last_modified`, `S` is one sweep interval (default 5 min), and `R` is the locked version's retain-until. The maintenance IAM policy Ravel ships permits that delete (its catalog deny is scoped to `catalog/<signal>/HEAD`); a copy of that template predating the narrowing denies it outright and leaves the bound open-ended until it is re-applied. Scope the mechanism to `catalog/<signal>/HEAD` alone to drop the retention half of it | Not a recovery control |
| **level 0** (default) | Versioning + `NoncurrentDays = E_v` + expired-delete-marker cleanup; no replica | Primary `+E_v` | None; bucket loss is total loss |
| **level 1** (recommended) | Level 0 plus a replica: different region/account/KMS key, replication v2 with `DeleteMarkerReplication`, RTC recommended; the replica versioned with `NoncurrentDays = E_v_r` and expired-delete-marker cleanup | Primary `+E_v`; replica residue is replication lag + `E_v_r` (requires `DeleteMarkerReplication`) | Defined here; **unmeasured**. A harness rehearsal record publishes a restore-completeness RPO, not this replication-lag RPO, so the field stays unmeasured until a rehearsal with bucket replication and a live writer measures it. RTC gives RPO a 15-minute ceiling; without RTC, unbounded |
| **level 2** (optional) | level 1 plus a bucket default retention `D`, which S3 applies to every object including the data objects | `max(bound + E_v, D)`; query-time exclusion still immediate | As level 1 |

## Background

- The posture, the mandate, the restore procedure, the rehearsal-only
  publication rule, and the chaos lane are
  [ADR-0077](../adrs/0077-dr-posture-and-chaos-evidence.md), which amends
  ADR-0058 decision 5.
- The erasure guarantee whose bound the levels above modify is ADR-0064.
- The bucket-protection contract is ADR-0072 decision 3.
- The commit-record reconstruction tool is ADR-0058.
- The per-mode storage credentials are ADR-0055.
