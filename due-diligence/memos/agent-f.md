# Agent F: Security and multi-tenancy

Frozen commit: 527a16db2e4d47b2924e4de4a4db32d7583fda33
Scope: authentication, tenant isolation, resource exhaustion, data security,
unauthenticated surfaces. Supply-chain excluded (Agent K).

## Verdict

The multi-tenant isolation core is strong and holds by construction, not by
convention. Tenant identity is a keyed BLAKE3 hash pinned per bucket; every
object key, cache key, and idempotency marker is tenant-scoped; the three
cross-process trust boundaries (Flight SQL tickets, distributed-fragment
capabilities, cross-cluster federation) each authenticate with a keyed MAC or a
re-resolved credential and cross-check the wire tenant against signed claims in
constant time. JWT verification pins the algorithm to the JWKS key (not the
token header), requires issuer/exp/audience, and rejects symmetric keys and
plaintext non-loopback JWKS at startup. No P0 (cross-tenant read/write or remote
compromise) was found: every path where one tenant's hash could substitute for
another's is gated by a keyed MAC bound to the tenant, or by a resolver that
derives the tenant from the credential and ignores the wire value.

The weaknesses are misconfiguration-exposure gaps and a plaintext-by-default
network posture, not breaks in the isolation model. The most material finding is
that the `--dev-insecure-tenant-header` loopback guard checks only `--listen-http`
and not `--listen-grpc`, so the same header-trusting resolver is reachable on a
publicly-bound gRPC listener without tripping the startup refusal that exists to
prevent exactly this (P2). The public HTTP and gRPC listeners terminate no TLS in
process; the deployment assumes a fronting proxy, and nothing refuses to start
without one, so a forgotten proxy sends bearer tokens and tenant data in cleartext
(P2). Per-query scanned-byte cap ships `Unlimited` by default (P3).

Confidence: high on the code-verifiable mechanisms below; medium on operational
posture (TLS termination, IAM/bucket policy) that lives outside this repo.

## Threat model table

| Threat | Surface | Existing mitigation | Weakness | Severity | Recommendation |
|---|---|---|---|---|---|
| Malicious tenant reads another tenant's data | object keys `t/<hash>/…`, catalog, cache | keyed BLAKE3 tenant hash pinned per bucket (`ravel-types/src/lib.rs:141-158`); all keys prefixed with `to_hex()` hash; cache key carries `tenant_hash` (`ravel-cache` doc:10) | none found | - | keep |
| Compromised credential names arbitrary tenant on wire (distributed) | fragment `SeriesFetch` Pinned scope | per-query capability, keyed-BLAKE3 MAC over `{ver,tenant,signal,query,expiry}`, constant-time compared, wire `tenant_hash` must equal claims (`distrib.rs:575-639`) | capability replayable within TTL on plaintext Combined listener | P3 | prefer `--fragment-listener` (TLS) in prod |
| Federating cluster reads arbitrary tenant | fragment Resolve scope | tenant re-resolved from presented credential, wire `tenant_hash` overwritten (`distrib.rs:868-878`, `652-658`); fragment token rejected (test `federation_rejects_the_fragment_token`) | none found | - | keep |
| Forged/replayed Flight SQL ticket | Flight `DoGet` | keyed BLAKE3 MAC, `derive_key` domain-separated (`flight_ticket.rs:288-290`); client deadline clamped (`flight/stream.rs:115`); ticket tenant cross-checked vs resolved tenant (`flight/service.rs:445-451`) | ticket key is per-process random single-node; no TLS in process | P3 | front Flight with TLS |
| `alg:none`/algorithm confusion JWT | OIDC resolver | algorithm pinned from JWKS key, not token header; symmetric keys refused (`tenant-resolve/lib.rs:326-347, 405-445`) | none found | - | keep |
| On-path attacker swaps JWKS | OIDC JWKS fetch | plaintext non-loopback JWKS URL refused at startup (`main.rs:189-210`); 10s fetch timeout | trust still rooted in TLS to IdP (operator-provided) | - | keep |
| Unauthenticated client spoofs tenant | dev header resolver | loopback guard on `--listen-http` (`config.rs:2058`) | guard omits `--listen-grpc`; same resolver serves gRPC ingest | P2 | extend guard to `listen_grpc` |
| Direct exposure of mTLS listener | trusted `x-ravel-client-cert-cn` header | `MtlsResolver` only on dedicated listener; refused if it collides with public listeners (`config.rs:2071-2084, 2220-2239`) | trust still depends on proxy stripping the header at network layer | P2 | document + network policy |
| Decompression bomb | OTLP gzip, RW snappy, OTAP zstd | capped before allocation: `take(cap+1)` gzip (`otlp_http.rs:133-148`) and zstd (`otap/stream.rs:217-227`); snappy pre-checks `decompress_len` (`remote-write/snappy.rs:23-33`) | none found | - | keep |
| Oversized body pre-buffer | HTTP/gRPC ingest | `DefaultBodyLimit::max(16MiB)` (`otlp_http.rs:295`); gRPC `max_decoding_message_size(16MiB)` (`lib.rs:1534`) | none found | - | keep |
| Series/cardinality explosion | metrics/logs ingest | `max_active_series` cap + creation-rate token bucket (`admission.rs:87-99`); per-request structural caps (`otlp/limits.rs:35-52`) | per-process, xN replicas (documented ADR-0051) | P3 | keep |
| Unbounded single-query scan | query/SQL | fleet query-concurrency ceiling + per-query S3-request cap; SQL read-only single-statement, 64KiB stmt cap | `max_bytes_scanned` default `Unlimited` (`config.rs:2699-2702`) | P3 | ship a finite default |
| Secret in process listing | S3 credential | file>env>argv; file redacts, `RemoteClusterConfig`/`FragmentListenerSettings` Debug redacted | `--s3-secret-key` accepted on argv (`config.rs:101`) | P3 | prefer env/file only |
| Cleartext to object store | S3 backend | HTTPS to AWS regional endpoint | `allow_http = endpoint.is_some()` (`store.rs:298`): custom endpoint enables http | P3 | gate allow_http behind explicit flag |
| Tenant enumeration via metrics | `/metrics` (unauthenticated) | tenant labels fold to `"other"` unless `--metrics-tenant-labels` (`config.rs:355-364`, `metrics.rs:2376-2397`) | opt-in flag discloses `tenant_hash` + traffic on an unauthenticated route | P3 | keep opt-in; document |

## Evidence

### Authentication

Static bearer (`crates/ravel-tenant-resolve/src/lib.rs:71-120`): configured
tokens are stored only as `blake3::keyed_hash(process_local_key, token)`; an
incoming token is hashed the same way before a `HashMap` lookup. Plaintext is
never a map key and never byte-compared, closing the per-byte timing channel.
Per-resolver random key (`random_hash_key`, 244 bits getrandom). Tests
`static_bearer_stores_only_keyed_hashes_never_plaintext`,
`static_bearer_hash_key_is_per_resolver`.

OIDC/JWT (`lib.rs:185-446`): `DecodingEntry` pins one `Algorithm` taken from the
JWKS key's own declared `alg`; `Validation::new(entry.alg)` admits only that
algorithm, so a token's `alg` header is never trusted (`lib.rs:405-445`).
`set_required_spec_claims(["exp","iss"])`, issuer pinned, audience checked when
configured. `signing_algorithm` returns `None` for HS256/384/512, RSA-OAEP, and
unknown, so symmetric keys in a public JWKS are refused
(`parse_signing_keys`/`install_jwks` -> `NoUsableKeys`). Startup requires at
least one `--oidc-audience` (`config.rs:1421-1428`) and refuses plaintext
non-loopback JWKS (`main.rs:189-210`). JWKS fetch has a 10s timeout
(`JWKS_FETCH_TIMEOUT`). Tests: `algorithm_confusion_reusing_the_public_key…`,
`rsa_algorithm_confusion…`, `exp_claim_as_json_string_is_auth_error`
(CVE-2026-25537 regression), `symmetric_jwks_key_is_never_installed`,
`wrong_issuer_is_auth_error`, `token_signed_with_key_not_in_jwks_is_auth_error`.

mTLS trusted header (`lib.rs:448-522`): `MtlsResolver` reads a plain header and
is documented as X-Forwarded-For-class trust. Structural isolation verified:
`build_auth_resolver` returns the mTLS resolver in a separate
`ResolverBundle::mtls_resolver` field and never pushes it onto the public chain
(`services/ravel-server/src/tenant.rs:74-116`); `lib.rs` installs it only on the
`--mtls-listener` router (`lib.rs:966-992`, 1308-1395). `Cli::validate` refuses
`--mtls-listener` equal to `--listen-http`/`--listen-grpc`
(`config.rs:2232-2238`), refuses `--mtls-enabled` without a listener
(`2078-2084`), and refuses the dev-header + mTLS-on-http combination
(`2224-2231`). The claim "MtlsResolver is never in the chain for
`--listen-http`/`--listen-grpc`" is IMPLEMENTED/VERIFIED. If an operator exposes
the mTLS listener directly (no stripping proxy), any client sets the header and
selects any tenant; this is the documented residual precondition, warned at
startup (`lib.rs:124-137`).

Flight SQL ticket MAC (`crates/ravel-sql/src/flight_ticket.rs`): `RFT1` v6,
trailing `blake3::keyed_hash(key, payload)` (`MAC_LEN=32`), verified with
`ct_eq` before parse (`decode`, l.517-530). `derive_ticket_key` uses
`blake3::derive_key` with a versioned context over the cluster secret
(`l.269-290`), so token and ticket key are cryptographically independent.
`deadline_ns` is client-supplied and clamped
(`clamp_ticket_deadline_ns`, `flight/mod.rs:184-185`; enforced `flight/stream.rs:115-116`).
`DoGet` resolves the authoritative tenant from metadata and rejects a ticket
whose embedded tenant differs (`flight/service.rs:436-451`).

Distributed-fragment capability (`services/ravel-server/src/distrib.rs:575-639`,
`crates/ravel-query/src/distrib/codec.rs:101-157`): claims are
`{version,tenant_hash,signal,query_id,expires_unix_ns}`, MAC =
`blake3::keyed_hash(key, canonical_claims)`. Verify recomputes and compares in
constant time against all configured keys (rotation), then checks expiry,
tenant, and query/signal in fixed order. `constant_time_eq` compares length then
XOR-accumulates (`distrib.rs:372-381`). Tests
`capability_for_tenant_a_cannot_fetch_tenant_b`,
`rotation_verifies_capability_under_any_configured_key`,
`each_capability_reject_reason_is_labeled_and_counted`.

Cross-cluster federation (`distrib.rs:641-658, 843-889`): a Resolve-scope request
runs the ordinary `TenantResolver` over the credential and overwrites
`inner.tenant_hash` with the derived tenant, so a coordinator cannot name a tenant
on the wire; the cluster-internal fragment token is not in the tenant registry so
it is rejected. Tests `federation_resolves_tenant_from_credential_not_wire`,
`federation_wire_tenant_cannot_cross_to_another_tenant`,
`federation_rejects_the_fragment_token`. Federating credential (`--remote-cluster`)
is read from a file and redacted in Debug (`config.rs:1141-1156`).

### Tenant isolation

Tenant hash: `TenantHashScheme::v2_from_deployment_key` =
`blake3::derive_key("ravel-tenant-v2", key)` then
`keyed_hash(hash_key, tenant_id)[0..16]` (`ravel-types/src/lib.rs:141-158`);
V1 unkeyed is bare `blake3::hash`. Scheme is pinned per bucket in an immutable
`sys/tenancy` marker and every process refuses to start on a scheme/fingerprint
mismatch (`services/ravel-server/src/tenancy.rs:230-355`, `TenancyError`
variants). Object keys are uniformly `t/<hash>/…` (`ravel-catalog` grep:
`covering_postings.rs:101`, `provisioning.rs:38-51`, `key_epoch.rs:52`).
Idempotency markers key off `blake3("ravel-idem-v1" || tenant_id || client_key)`
(`ravel-ingest/src/idempotency.rs:9-17,137-146`). Recovery manifest binds
`tenant_hash` as AES-256-GCM AAD, so a manifest cannot be relocated to another
tenant (`tenancy.rs:399-466`; tests
`recovery_manifest_wrong_tenant_hash_aad_is_a_typed_error`, `…_wrong_key…`,
`…_tampered_ciphertext…`).

Every query surface authenticates first, before any resolve/GET:
`sql.rs:172`, `analytics.rs:219`, `exemplars.rs:259`, `query`/`handlers.rs:141`
(`authenticate` returns `TenantHash`, 401 on failure). Test
`an_unauthenticated_exemplar_request_is_not_audited`.

Pinned scope "trusts the wire tenant_hash" only after `verify_capability`
proves the capability's signed `tenant_hash` equals the wire value
(`distrib.rs:621-626`). Who can reach it: the service is registered only on the
public gRPC listener and, when configured, the dedicated TLS fragment listener,
never the HTTP or mTLS client listeners (`lib.rs:1614-1665, 1687-1736`); with a
dedicated listener the public one is `PublicFederation` and rejects Pinned
outright (`distrib.rs:880-886`).

### Resource exhaustion / admission layering

Order in `otlp_http::export_metrics` (`otlp_http.rs:457-517`):
(1) `ingest_concurrency.try_admit()` process in-flight shed;
(2) `tenant_resolver.resolve` (401);
(3) `admit_and_decode_body` = layer-2 byte-rate (`check_byte_rate`) on wire bytes
for identity, or gzip `peek_byte_rate` pre-check before inflate then
`check_byte_rate` on decompressed size (`otlp_http.rs:176-258`);
(4) protobuf decode;
(5) structural normalize under `IngestLimits` (`ingest.rs:118-198`);
(6) `check_series_creation_rate` then `admit_series` active-cap
(`ingest.rs:198`, `admission.rs` module doc:10-20). This matches the documented
layer-1/2/3/4 order and puts every cheap rejection before expensive work.
Decompression caps enforced during inflation (`take(cap+1)` gzip/zstd; snappy
`decompress_len` header pre-check). gRPC caps: `max_decoding_message_size(16MiB)`
on every service (`lib.rs:1532-1570`); tonic bounds compressed-frame
decompression to the same 16MiB. SQL complexity bounded by read-only
single-statement validation and `MAX_STATEMENT_LEN=64KiB`
(`ravel-sql/src/validate.rs`, `flight_ticket.rs:241`). PromQL regex matchers use
the `regex` crate (linear automaton, default 10MB compile cap), so no
catastrophic backtracking (`ravel-promql/src/functions/label.rs:76`).

### Data security

No TLS is terminated in-process on `--listen-http` or `--listen-grpc`
(`axum::serve`/`tonic Server::builder` with no `tls_config`, `lib.rs:1486-1494,
1634-1682`); only the dedicated fragment listener sets `ServerTlsConfig`
(`lib.rs:1709-1718`). The deployment assumes a fronting TLS proxy; nothing
refuses to start without one. Object-store TLS: `allow_http` is true whenever a
custom `--s3-endpoint` is set (`store.rs:298`), so MinIO/on-prem defaults to
plaintext. SSE-KMS: single key via `with_sse_kms_encryption(kms_key_id)`
(`s3.rs:257-263`); per-tenant routing via `KmsRoutingStore` clones the config
with the tenant's key ARN (`kms_routing.rs:224`); no explicit KMS encryption
context is set (key routing only). Credentials: rotating file > session token >
inline (`s3.rs:264-276`); `--s3-secret-key` still accepted on argv
(`config.rs:98-102`). Deployment/recovery keys and PEM material are redacted in
Debug (`tenancy.rs:66-77`, `config.rs:1083-1094, 1141-1156`).

### Unauthenticated surfaces

`/metrics` handler takes no headers and runs no resolver (`metrics.rs:3368-3372`);
tenant-labelled families fold to `tenant_hash="other"` unless
`--metrics-tenant-labels`, in which case the allowlist is exactly the tenants an
operator configured limits for (`lib.rs:906-921`). `/healthz` is a constant 200;
`/readyz` is startup-latch AND store-reachable, no per-probe store I/O, returns
only a status code (`health.rs:1-100`). None leak query text or tenant ids by
default.

## Failure scenarios

1. Operator runs `--dev-insecure-tenant-header --listen-http 127.0.0.1:4318
   --listen-grpc 0.0.0.0:4317`. Startup succeeds: `validate` only checks
   `listen_http.is_loopback()` (`config.rs:2058`). The dev-header resolver is in
   `config.tenant_resolver`, which backs the gRPC `GatewayState`
   (`lib.rs:1506-1520`). Any network client sends gRPC metadata
   `x-ravel-tenant: victim` and ingests/queries as any tenant. The guard that
   exists to make this impossible does not cover the gRPC listener. (P2)

2. Operator forgets the fronting TLS proxy. Bearer tokens, OIDC JWTs, tenant
   data, and commit tokens all traverse the network in cleartext on
   `--listen-http`/`--listen-grpc`; nothing refuses to start. (P2)

3. `--distributed-query` without `--fragment-listener`: fragment fetches run in
   `Combined` role on the plaintext public gRPC listener; a network eavesdropper
   observes cross-worker tenant segment data, and a captured capability is
   replayable until its deadline for the same tenant/query. (P3)

4. A tenant issues one PromQL/SQL query scanning the full retention window: with
   the shipped `max_bytes_scanned = Unlimited` only the per-query S3-request cap
   and fleet query-concurrency ceiling bound it, not scanned bytes. (P3)

## Tests or commands run

Read-only (no cargo build/clippy/test, per charter). Files read in full:
`crates/ravel-tenant-resolve/src/lib.rs`, `services/ravel-server/src/tenancy.rs`,
`services/ravel-server/src/tenant.rs`, `services/ravel-server/src/otlp_http.rs`,
`services/ravel-server/src/flight_auth.rs`,
`crates/ravel-query/src/distrib/codec.rs` (auth region),
`services/ravel-server/src/distrib.rs` (auth/capability region),
`crates/ravel-sql/src/flight_ticket.rs` (header region). Grep/glob across
`services/ravel-server/src/{config,lib,main,store,remote_write,metrics,health}.rs`,
`crates/ravel-ingest/src/{admission,idempotency}.rs`,
`crates/ravel-otlp/src/limits.rs`, `crates/ravel-remote-write/src/snappy.rs`,
`crates/ravel-otap/src/stream.rs`, `crates/ravel-object-store/src/{s3,kms_routing}.rs`,
`crates/ravel-types/src/lib.rs`, `crates/ravel-catalog/src/auth_token_map.rs`,
`services/ravel-ingest-router/src/key.rs`. Verified test names cited above exist
in the corresponding `#[cfg(test)]` modules.

## Unknowns

- IAM/bucket policy enforcement of the ADR-0055 per-role credential split is
  external to this repo (`ObjectStoreBackend` has no caller-identity parameter;
  in-process code is fully trusted for the whole bucket). NOT ASSESSED here;
  the isolation depends on the operator wiring distinct S3 credentials per role.
- Whether any production topology actually fronts the public listeners with TLS.
  UNKNOWN from code.
- Runtime KMS encryption-context binding (beyond key routing). NOT IMPLEMENTED in
  code read; relies on S3/KMS key-policy for the tenant-to-key binding.
- `ravel-operator` pod/ServiceAccount wiring not audited (out of charter files).

## Severity-ranked findings

### P2 — dev-header loopback guard omits the gRPC listener
Evidence label: VERIFIED (code), scenario UNKNOWN-in-prod.
`Cli::validate` refuses `--dev-insecure-tenant-header` only when
`--listen-http` is non-loopback (`config.rs:2058`), but the same
`DevHeaderTenantResolver` sits in `config.tenant_resolver`
(`tenant.rs:86-88`), which backs the OTLP gRPC `GatewayState`
(`lib.rs:1506-1520`); `metadata_to_headers` copies `x-ravel-tenant` into the
`HeaderMap` the resolver reads (`otlp_grpc.rs:29-46, 77-82`). A publicly-bound
gRPC listener therefore accepts unauthenticated tenant selection while startup
still succeeds. Fix: extend the guard to also require `listen_grpc.is_loopback()`
(and any future listener the shared chain backs).
File:line: `services/ravel-server/src/config.rs:2058`.

### P2 — public listeners terminate no TLS; no fail-closed on missing proxy
Evidence label: VERIFIED (code), operational.
`axum::serve`/`tonic` bind `--listen-http`/`--listen-grpc` with no TLS
(`lib.rs:1486-1494, 1634-1682`); only the fragment listener configures TLS.
Bearer tokens and tenant payloads are cleartext unless an external proxy
terminates TLS, and nothing refuses to start without it. Fix: document the
proxy requirement as load-bearing and/or add an explicit
`--insecure-plaintext-listeners` acknowledgement flag.
File:line: `services/ravel-server/src/lib.rs:1486`.

### P3 — per-query scanned-byte cap defaults to Unlimited
Evidence label: IMPLEMENTED (as designed).
`shipped_query_defaults()` sets `max_bytes_scanned: ByteLimit::Unlimited`
(`config.rs:2699-2702`). A single query's scan volume is bounded only by the
derived S3-request cap and fleet concurrency ceiling, not by bytes. Fix: ship a
finite default or require an explicit opt-out.
File:line: `services/ravel-server/src/config.rs:2699`.

### P3 — custom S3 endpoint silently enables plaintext object-store traffic
Evidence label: VERIFIED.
`allow_http = endpoint.is_some()` (`store.rs:298`): any `--s3-endpoint`
(MinIO/on-prem) turns on HTTP to the bucket, the source of truth. Fix: gate
`allow_http` behind an explicit flag rather than deriving it from endpoint
presence.
File:line: `services/ravel-server/src/store.rs:298`.

### P3 — plaintext, replayable-within-TTL fragment fan-out in Combined role
Evidence label: IMPLEMENTED (documented pre-amendment layout).
Without `--fragment-listener`, Pinned fetches run on the plaintext public gRPC
listener (`Combined`, `lib.rs:1652-1665`); the capability MAC preserves tenant
isolation and authenticity but does not encrypt segment data, and a captured
capability replays until `expires_unix_ns` (set to the query deadline,
`distrib.rs:1131`). Fix: recommend `--fragment-listener` with TLS for any
multi-node prod deployment.
File:line: `services/ravel-server/src/distrib.rs:1122-1133`.

### P3 — `--metrics-tenant-labels` discloses tenant hashes on an unauthenticated route
Evidence label: IMPLEMENTED (opt-in, warned in flag help).
`/metrics` is unauthenticated (`metrics.rs:3368`); with the flag set, real
`tenant_hash` labels and per-tenant traffic/query volumes are exposed to any
scraper (`config.rs:355-364`). Default folds to `"other"`. Fix: keep opt-in;
document that the scrape network must be trusted.
File:line: `services/ravel-server/src/config.rs:363`.

### P3 — S3 secret accepted on argv
Evidence label: VERIFIED.
`--s3-secret-key` (`config.rs:98-102`) is readable from `/proc/<pid>/cmdline` and
`ps`. Env and rotating-file paths exist and are preferred. Fix: drop the argv
form or warn when it is used.
File:line: `services/ravel-server/src/config.rs:101`.

## Confidence

High on the code-level mechanisms (keyed-hash tenancy, JWT algorithm pinning,
ticket/capability MACs, decompression and body caps, admission ordering,
auth-first query handlers): each is backed by a specific file:line and a named
regression or isolation test. Medium on operational posture (external TLS
termination, per-role IAM/bucket policy, KMS key policy) which is asserted by
docs/ADRs but not enforced by code in this repo and is therefore out of
code-verifiable reach. No P0/P1 cross-tenant or remote-compromise path was
found; the reported items are misconfiguration-exposure and plaintext-network
gaps, ranked P2/P3.
