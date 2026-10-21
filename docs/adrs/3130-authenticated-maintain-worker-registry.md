# ADR-3130: Authenticated maintain-worker registry records

Status: Accepted (2026-10-21). Applies the ADR-1918 decision to the
maintain-worker registry of ADR-0065. Changes the frozen `WorkerHeartbeat`
message in `proto/ravel/sys.proto` (additive field, `format_version` 1 to 2).
Issue #3130, epic #1918.

## Context

Every `--mode maintain` process writes a `WorkerHeartbeat` protobuf to
`sys/maintain/workers/<process_id>` every H (60 s) with `PutMode::Overwrite`
(`crates/ravel-fleet/src/worker_set.rs`). The live set built from those
records decides, by rendezvous hash, which process owns each maintenance unit:
per-shard retention and compaction, sweeps, subject erasure, query-audit and
alert retention, scrub rotation, the catalog fold, and the query-worker reap
(`services/ravel-server/src/maintain.rs`, `owns_unit`; `scrub.rs`; `fold.rs`).
Two readers build it: the maintain heartbeat task and scrub, each with its own
LIST and GETs.

The record carries no MAC, and `deploy/iam/maintain.json` grants the maintain
role `s3:PutObject` on `sys/maintain/*` (statement `MaintainWrite`) and
`s3:DeleteObject` on `sys/maintain/workers/*`. Any principal holding that role
can write a fresh record and join the live set. The reader also never checks
the body's `process_id` against the key (`sys.proto` says "the reader takes
identity from the key").

The effect is starvation, not a crash. ADR-0065 rejected per-unit leases on
purpose (`docs/adrs/0065-leased-distributed-maintenance.md`): a unit is taken
over only when its owner's heartbeat goes stale. A forged member that keeps its
stamp fresh owns its units, and nobody does that work: retention, compaction,
erasure, the fold. `ravel_maintain_units_stalled` counts only units a real
process owns and fails, so a phantom owner never shows up there. The only sign
is an inflated `ravel_maintain_workers_live`. Tenant hashes are listable under
`t/`, so a forger can try UUIDs offline until one wins chosen units. A single
real maintain process is exposed the same way: the phantom takes units away
from it.

Unlike the query tier, a maintain process holds no cluster key that fits.
`Cli::validate` refuses `--fragment-key-file` and `--sql-ticket-key-file`
outside query mode (`services/ravel-server/src/config.rs`). The deployment key
(`--tenant-hash-key-file`) is accepted in every mode, but it is absent on an
unkeyed bucket, which is the operator default without
`deploymentKeySecretRef`, and its fingerprint is pinned in `sys/tenancy`, so it
can never rotate. The operator generates no Secrets: every key is a
user-supplied Secret ref, because its RBAC grants Secrets `get` only.

Two constraints differ from ADR-1918:

- The record is protobuf in the frozen `sys.proto`, classified
  `("WorkerHeartbeat", Immutable(&[1]))` in
  `crates/ravel-catalog/tests/sys_proto_format_version_classification.rs`.
  Adding a field goes through the format-change procedure.
- `WorkerSet` is built in every mode, because its `process_id` also names the
  SQL spill root (`services/ravel-server/src/lib.rs`). A key required by the
  `WorkerSet` constructor would force gateway and query processes to hold a
  maintain key they never use.

## Decision

1. **A maintain key file.** New flag `--maintain-key-file`, in the format of
   `--fragment-key-file`: one 64-hex-character key per line, the first key
   mints and any key verifies, parsed by the same `parse_fragment_keys`.
   `Cli::validate` requires it with `--mode maintain` and refuses it in every
   other mode, since no other mode reads the registry. Every maintain process
   in a cluster reads the same file.

2. **`WorkerHeartbeat` gains `bytes mac = 5`, and `format_version` moves to
   2.** The MAC is `blake3::keyed_hash(record_key, claim)`, where
   `record_key = blake3::derive_key("ravel maintain worker record v1",
   file_key)`. That context differs from ADR-1918's, so a query record MAC
   never verifies as a maintain one. `claim` is built from field values, not
   encoded protobuf bytes, since protobuf encoding is not canonical:
   `format_version` (u32 BE), `process_id` (u64 BE length prefix, then the
   bytes), `started_unix_ns` (i64 BE), `heartbeat_unix_ns` (i64 BE). Before v1
   there is one supported version (ADR-0027's regime), so readers accept
   exactly `{2}`. A version-1 record is rejected as `missing_mac`, not decoded
   through a dual path. Migration class C (an immutable metadata record,
   additive change). The classification table entry becomes `Immutable(&[2])`.
   `docs/catalog-and-mvcc.md` is amended in the same change.

3. **Keys are required where membership happens, not where identity is
   made.** `WorkerSet::new` keeps building identity in every mode. Writing a
   heartbeat and reading the live set both require a non-empty
   `MaintainRecordKeys`, either as an argument or through a membership handle
   that can only be built from one. Building one from an empty list is a typed
   error, and nothing defaults to no keys. The production maintain spawn
   (`maintain::spawn`, with its heartbeat task) and scrub pass the keys loaded
   from `--maintain-key-file`. No production code path can publish or admit an
   unsigned record.

4. **The reader verifies identity and MAC before admitting.** Both live-set
   readers admit a sibling only when all of these hold:
   - the key parses as a UUID;
   - the body's `process_id` equals the key's UUID;
   - `format_version == 2`;
   - the MAC verifies under one of the reader's keys, compared in constant time.

   The record GET becomes a range read of 4096 + 1 bytes; a response longer
   than 4096 bytes is rejected as `oversize` without being decoded. A rejected
   record is skipped. A transport error on a GET keeps today's behaviour: the
   read fails, and the caller keeps its prior membership. Rejects are counted
   as `ravel_maintain_worker_record_rejects_total{reason}`, with the closed
   reasons and check order of ADR-1918 decision 4: `oversize`; `malformed` (a
   body that does not decode, or whose `process_id` does not match its key);
   `missing_mac` (including a version-1 record) and `bad_mac`; and
   `stale_stamp` for a verified record whose `heartbeat_unix_ns` is outside
   the window although its LIST mtime was fresh. A record skipped by the
   mtime check before its GET is not counted. The maintain registry has no
   drained stamp, so ADR-1918's drain exception does not apply. The counters
   live in a handle owned in `ravel-fleet`, snapshotted by the server and
   rendered from zero on the maintain process's /metrics, and the family is
   added to `docs/guides/observability.md`.

5. **The operator renders the mount from a required Secret ref.** The
   `RavelCluster` CRD gains `spec.maintain.keySecretRef` (a `LocalSecretRef`
   holding the key file under `keys`, like `fragmentKeySecretRef`). The
   maintain Deployment mounts it, passes `--maintain-key-file`, and includes
   the Secret's resourceVersion in its pod-template checksum, so a rotation
   rolls the pods. A cluster with `spec.maintain.enabled` (the default) and no
   `keySecretRef` is refused with a status condition naming the field, rather
   than rendered into pods that fail `Cli::validate`. The operator still
   generates no Secrets. `deploy/docker-compose` ships a development key file
   with a comment saying so.

6. **Land as one pull request.** The server half makes the flag mandatory, and
   the operator half renders it. Landed apart, either an operator-managed
   maintain tier refuses to start, or the operator renders a flag the server
   rejects. The two tasks are stacked and merged together.

```mermaid
flowchart LR
    subgraph members [Maintain processes: hold --maintain-key-file]
        M1[maintain A]
        M2[maintain B]
    end
    subgraph bucket [Object store, maintain-role writable]
        R[(sys/maintain/workers/uuid: WorkerHeartbeat v2 + mac)]
    end
    X[maintain-role principal without the key file]
    M1 -->|PUT record + MAC| R
    X -.->|PUT forged record: no valid MAC| R
    R -->|bounded GET, identity + MAC checked| M2
    M2 -->|rendezvous ownership over admitted members only| U[maintenance units]
```

## Rejected alternatives

- **Derive the record key from the deployment key.** Unkeyed buckets, the
  operator default and docker-compose, have no deployment key, so those
  clusters would get no protection. The key's fingerprint is pinned in
  `sys/tenancy`, so it can never rotate, and a leaked key would stay valid for
  the life of the bucket. It is also held by every tier and by the operator,
  which widens the set of holders for no benefit.
- **Reuse the fragment key file.** It is query-tier material, and maintain mode
  refuses it on purpose. Mounting it on maintain pods would hand them the key
  that mints query capabilities, which maintain processes have no other reason
  to hold.
- **Narrow the IAM grant.** S3 cannot scope a PUT to the writer's own UUID,
  the same reason as in ADR-1918.
- **Operator-generated key Secret.** This would need Secrets `create`/`patch`,
  and issue #126's `get`-only posture refuses that on purpose.
- **Put the MAC in a sibling object.** Two objects cannot be written
  atomically. A reader would see a record without its MAC, or a MAC without
  its record, on every heartbeat race, and would have to treat that as
  rejected.
- **Add the field without bumping `format_version`.** The classification test
  and the single-version regime both require the bump. The bump also makes an
  old reader skip a new record under its existing `format_version > 1` guard,
  instead of decoding it and ignoring the MAC.
- **A per-unit work lease, so a phantom owner's units are taken over.**
  ADR-0065 rejected this because a live but wedged worker starves its own
  units under any lease that a fresh heartbeat renews. Authenticating
  membership removes the phantom without reopening that decision.

## Consequences

- A principal holding maintain-role S3 write but not the key file can no
  longer add a member to the maintain live set. This closes the starvation
  path in Context.
- **Eviction remains, and it is cheaper than on the query tier.** The maintain
  role can overwrite or delete a legitimate member's record. Other members
  then drop it and take over its units. The evicted process still counts
  itself as a member, so it keeps working its own units too. The result is
  duplicated, idempotent work (ADR-0065's stated failure mode), not lost work.
- Every maintain deployment must provision a key file. Under the operator,
  that means creating a Secret and naming it in `spec.maintain.keySecretRef`;
  a cluster with the maintain tier enabled and no ref is refused with a status
  condition. There is no
  default key, because a key shipped with the code authenticates nothing.
- During a rolling deploy, old processes skip version-2 records (their
  `format_version > 1` guard) and new processes reject version-1 records. Each
  side sees a partial live set and may own units the other side also owns, so
  work is duplicated, idempotently, until the rollout completes.
- `sys/maintain/memo/<process_id>` sits under the same `sys/maintain/*`
  PutObject grant. A forged memo snapshot seeds verdicts for owned units
  (`seed_memo_from_snapshots`). It is the same class of exposure on a
  different record, and it is filed as its own issue (#3155) rather than folded in
  here.
- Docs updated with the implementation: `docs/catalog-and-mvcc.md` (the
  heartbeat record), `docs/reference/ravel-server-flags.md` (regenerated),
  `docs/guides/kubernetes.md`, `docs/guides/operations/deployment.md`, the
  doc comments in `worker_set.rs` that call the key "self-owned" or "single
  writer per key", and the ADR index.
