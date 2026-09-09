# ADR-0042: Compliance-grade custody - legal hold, per-tenant KMS, pluggable auth, verify-custody

Status: Accepted

## Context

A codebase survey found Ravel already has more of this than expected,
and one real gap the pinned `object_store` version cannot close:

- **Auth**: `crates/ravel-query/src/http/tenant.rs:17`'s `TenantResolver`
  trait is already the pluggable seam (`StaticBearerTokenResolver` is
  the only production impl; `services/ravel-server/src/tenant.rs:14`'s
  `FallbackResolver` already composes a `Vec<Arc<dyn TenantResolver>>`).
  A real OIDC/mTLS resolver is a new trait impl, not a rewrite.
- **Retention**: purely age-based (`crates/ravel-maintain/src/
  retention.rs`, ADR-0019). `RetentionConfig`'s `floor_ns` is a minimum-
  window validation, not a legal-hold/immutability-lock concept - no
  "this data cannot be deleted regardless of retention window" state
  exists anywhere.
- **Encryption**: zero app-layer SSE/KMS configuration
  (`crates/ravel-object-store/src/s3.rs`'s `AmazonS3Builder` sets only
  bucket/region/credentials/path-style). `object_store` 0.14.1 itself
  *does* expose `with_sse_kms_encryption`/`with_dsse_kms_encryption`/
  `with_ssec_encryption` on its builder (verified in vendored source,
  `aws/builder.rs`) - the capability exists upstream, Ravel just never
  wires it.
- **Object Lock (true WORM)**: `object_store` 0.14.1 has **no per-PUT
  Object Lock knob** (no retain-until-date or legal-hold header on the
  write path; its one Object Lock test only uploads to an
  already-Object-Lock-enabled bucket, setting no per-object state).
  Ravel cannot enforce S3-level, credential-proof WORM through this
  client. This is a real, load-bearing constraint on this ADR's scope,
  not a detail to gloss over.
- **Custody verification**: `services/ravel-cli/src/maintain.rs:226`'s
  `audit_versions` (a tenant/shard-scoped per-object walk with anomaly
  counting) is the template a `verify-custody` command extends, not a
  greenfield build.

## Decision

1. **Per-tenant SSE-KMS encryption**: `S3Config` gains an optional
   `kms_key_id: Option<String>` (per-tenant, sourced the same way
   tenant tokens are configured today - CLI flag or config file, one
   entry per tenant). `S3Store`'s builder calls `object_store`'s
   existing `with_sse_kms_encryption` when a tenant's config has one.
   No new crypto code in Ravel: the KMS call happens inside AWS S3
   itself on every PUT, exactly like any other SSE-KMS bucket. BYOK
   means "the tenant supplies their own `kms_key_id`", not "Ravel
   manages keys."
2. **Legal hold via the existing `LeaseCheck` seam, not S3 Object
   Lock.** The sweeper's crash-matrix design already provisioned a
   `LeaseCheck` hook
   ("consulted before every delete", currently the no-op `NoLeases`)
   as a seam for exactly this kind of future gate. A new
   `LegalHoldCheck` implementation consults a per-tenant (or
   per-time-range) hold record before any physical delete in both the
   sweeper and the retention path; a hold present means the delete is
   skipped, not queued or soft-failed, so re-evaluation is naturally
   idempotent on the next pass once the hold clears. Hold records are
   themselves immutable objects (set/cleared as new records, folded to
   derive current hold state - the same pattern ADR-0040 uses for
   alert state), never in-place mutation of a hold flag.
3. **Honest framing of "WORM"**: this ADR delivers Ravel-enforced legal
   hold (point 2), which stops Ravel's own deletion code from ever
   removing held data. It explicitly does **not** deliver credential-
   proof WORM (immutability that survives a compromised or malicious
   holder of the object-store credentials) - `object_store` 0.14 cannot
   set S3's own Object Lock retention on a PUT. Real WORM requires the
   operator to *additionally* enable S3 Object Lock at the bucket level
   (compliance mode, a default retention period) as an out-of-band,
   documented deployment step; docs/guides/operations.md gets a new
   "Compliance mode" section spelling out both layers and which
   threats each one covers. Claiming Ravel enforces WORM without that
   caveat would violate "exact semantics by default."
4. **Query and admin audit log**: uses `Signal::Audit` (ADR-0040).
   `ravel-server` writes an audit record at defined interception points
   (a query executed - tenant, query text, time range, result status; an
   admin action - retention change, legal hold set/cleared). Written by
   the server itself, never by a tenant-submitted request body, so a
   tenant cannot forge or suppress their own audit trail.
5. **`ravel-cli verify-custody`**: extends the `audit_versions` pattern
   (per-object walk, anomaly counting) to additionally verify the
   content-addressed chain: every data object's key-embedded `hash16`
   matches its actual content hash, every commit record's referenced
   inputs exist and match their recorded hashes, and (once legal hold
   lands) every held range has no delete gap. Decode-only, no format
   change; reuses the existing store-agnostic backend abstraction.
6. **Real authn (OIDC/mTLS)**: a new `TenantResolver` impl (`OidcResolver`
   validating a JWT against a configured issuer/JWKS, or `MtlsResolver`
   mapping a trusted, reverse-proxy-forwarded header carrying an
   already-verified client certificate's CN/SAN to a tenant) added to the
   `FallbackResolver` chain alongside `StaticBearerTokenResolver`, which
   stays available for local/dev use. No changes needed to any caller
   of `Arc<dyn TenantResolver>` - the trait boundary means every
   consumer (SQL, Flight, OTLP, remote-write, analytics) picks this up
   for free.

## Rejected alternatives

- **Claim full S3 Object Lock WORM support by hand-rolling raw S3 API
  calls that bypass `object_store` for the retain-until-date header.**
  Rejected: a second, parallel write path outside the
  `ObjectStoreBackend` trait's contract-tested abstraction would
  violate "no durability may depend on" an unaudited side channel, and
  duplicates the entire PUT retry/error-mapping story `object_store`
  already provides. If real per-object Object Lock becomes a hard
  requirement later, it is its own ADR extending the
  `ObjectStoreBackend` trait itself (a capability-gated new method,
  following the existing `Capabilities` pattern for `multipart`/
  `upload_checksum`), not a workaround in this epic.
- **Ravel-managed encryption keys (envelope encryption inside Ravel,
  not SSE-KMS).** Rejected: massive new scope (key management,
  rotation, a KMS-equivalent inside the product) for a compliance
  story S3's own SSE-KMS already solves at the storage layer for every
  other object in the bucket. BYOK via a tenant-supplied `kms_key_id`
  gets the audit/compliance win (tenant controls and can revoke the
  key) without Ravel becoming a key-management system.
- **A soft-delete "mark as held" queue instead of a pre-delete check.**
  Rejected: adds new mutable state and a reconciliation story; the
  `LeaseCheck`-style pre-delete gate is simpler, already-provisioned,
  and fails closed (nothing deletes until the check passes) rather
  than relying on a queue being drained correctly.

## Consequences

- No frozen-format change beyond ADR-0040's already-decided
  `Signal::Audit`; this ADR's legal-hold records are themselves
  `Signal::Audit`-shaped immutable records (a `kind: legal_hold` attr),
  not a new signal or section.
- Real per-object S3 Object Lock stays an explicit, named gap
  (out-of-band bucket configuration is necessary alongside Ravel's
  hold, not a Ravel-internal guarantee) - tracked as a follow-up ADR if
  `object_store` gains the capability or a direct-S3-SDK path is later
  justified.
- `StaticBearerTokenResolver` is not removed or deprecated; OIDC/mTLS
  are additive resolvers in the `FallbackResolver` chain, so existing
  dev/test flows and the `make demo` quickstart are unaffected.
- `S3Config`'s new `kms_key_id` is optional and per-tenant; a tenant
  with none configured gets today's behavior (whatever bucket-default
  SSE the deployment has) unchanged.

## Amendment: decision 1's mechanism is key-prefix routing, not a per-tenant `S3Config` field

Decision 1 as originally written says "`S3Config` gains an optional
`kms_key_id: Option<String>` (per-tenant, sourced the same way tenant
tokens are configured today...)". The change that actually wires this
into a running `ravel-server` (ADR-0062 decision 1, ADR-0072 decision 2)
ships a different mechanism: `S3Config.kms_key_id` stays a single,
process-wide field (ADR-0062 decision 1c's single-key posture,
`--s3-kms-key`) applied to the one default `S3Store` every deployment
already builds.

Per-tenant routing is a separate decorator, `KmsRoutingStore`
(`crates/ravel-object-store/src/kms_routing.rs`, already implemented and
unit-tested before this ADR was written), inserted between the default
`S3Store` and `InstrumentedStore` only when `--tenant-kms-config` names at
least one tenant. It intercepts `put`/`put_multipart` for keys under a
tenant's `t/<hash>/` prefix and routes them to a lazily-built, per-tenant
`S3Store` constructed with that tenant's own `kms_key_id`; every other
operation (`get`/`head`/`list`/`list_delimited`/`delete`) and every
non-configured tenant's writes fall through unchanged to the default
store. There is no per-tenant field on `S3Config` itself — there is one
`S3Config` per tenant, each a full clone of the default config with only
`kms_key_id` swapped, built on demand.

This is additive, not a reversal: decision 1's actual intent (BYOK, "the
tenant supplies their own `kms_key_id`") is unchanged, and the object-key
layout gains nothing beyond the already-authorized `t/<hash>/enc`
key-epoch record (ADR-0062 decision 1b). Only the "how" — a
per-tenant `S3Config` field versus a routing decorator over per-tenant
`S3Store` instances keyed by prefix — was wrong in the original text, and
is corrected here rather than left to mislead a future reader of decision
1 in isolation.
