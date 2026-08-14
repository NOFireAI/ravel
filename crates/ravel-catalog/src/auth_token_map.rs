//! Durable deployment-wide bearer-token map (ADR-0066 decision 6).
//!
//! The static `--tenant-token` allowlist becomes a durable control object at the
//! bucket root `sys/auth`: a map from a keyed hash of a bearer token to the
//! tenant id it authenticates as. The bucket NEVER holds a plaintext token; a
//! token is persisted only as `blake3::keyed_hash(deployment_key, token_bytes)`,
//! so the map is useless to an attacker who reads the bucket without the
//! deployment key, and a low-entropy token is not recoverable by an offline
//! dictionary attack (the key is load-bearing, the tenant-hash-v2 keyed-hashing
//! pattern the codebase already uses for audit tokens and the tenancy marker).
//!
//! Deployment-wide, not per-tenant, so it lives at the bucket root beside
//! `sys/tenancy` and `sys/gc`, never under a tenant prefix: an entry maps a token
//! to whichever tenant it grants, so the object cannot be keyed under any one
//! tenant.
//!
//! Mutation shape: whole-record CAS-replace, mirroring `sys/gc`'s `set_gc_config`
//! (read the current version, edit the entry set, swap under `CasVersion`), NOT
//! the append-only repeated-field shape of `provisioning`'s histories. The map is
//! current-state — a token is present or it is not — and revocation (removing an
//! entry) is a first-class operation an append-only history cannot express. A
//! concurrent write is a caught [`AuthTokenMapError::CasConflict`] the loser
//! re-reads, never a silent overwrite. On a fresh bucket the first
//! [`upsert_token`] bootstraps the object with `CreateIfAbsent`, the race-safe
//! first-write precedent `set_gc_config` and the provisioning record both use.
//!
//! This module builds and verifies the durable object and computes the keyed
//! hash. The bounded-staleness resolver that refreshes it on a horizon, does the
//! rate-limited on-miss re-read, and applies revocations fail-closed is a
//! separate resolver's job, not this module's; [`tenant_for_token`] is the pure lookup primitive that
//! resolver will call over an already-loaded map, nothing more.

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
// Aliased so the proto types read distinctly from this module's domain
// `AuthTokenMap` / `TokenEntry`, which share their conceptual names.
use ravel_proto::sys::v1::AuthTokenMap as ProtoAuthTokenMap;
use ravel_proto::sys::v1::TokenHashEntry as ProtoTokenHashEntry;

/// The bucket-root auth-map key (ADR-0066 decision 6). Fixed, deployment-wide,
/// never under a tenant prefix.
pub const AUTH_KEY: &str = "sys/auth";

/// Format version written into every `sys/auth` object this module emits, and
/// the highest version it reads. A future version is refused rather than misread
/// under the v1 layout, matching the `prov` / `enc` / `t/<hash>/config` records'
/// fail-closed-on-newer guard (each uses its own comparison operator; see each
/// module for its exact check).
///
/// Bumped 1 -> 2 for the `managed_by` ownership marker (ADR-0072 decision 4
/// amendment): `TokenHashEntry.managed_by` is `optional` and additive,
/// so the bump is a floor signal, not a wire necessity. This build accepts a
/// stored 1 or 2 unconditionally -- an absent `managed_by` decodes the same
/// way under either version, as unmanaged -- and always writes 2.
pub const AUTH_TOKEN_MAP_FORMAT_VERSION: u32 = 2;

/// [`TokenEntry::managed_by`] value the operator's reconcile loop stamps on
/// every entry it writes from a `tenantTokensSecretRef` Secret (ADR-0072
/// decision 4 amendment). The operator's remove/replace pass touches
/// only entries carrying this exact tag.
pub const MANAGED_BY_OPERATOR: &str = "operator";

/// [`TokenEntry::managed_by`] value `ravel-cli tenant token upsert` stamps by
/// default (ADR-0072 decision 4 amendment). Overridable at the CLI with
/// `--managed-by` for a caller that wants a different owner tag.
pub const MANAGED_BY_CLI: &str = "cli";

/// Width of a token hash: a full blake3 output, 32 bytes.
pub const TOKEN_HASH_LEN: usize = 32;

/// Width of the stored deployment-key fingerprint, 16 bytes, matching
/// `TenancyMarker::key_fingerprint`.
pub const KEY_FINGERPRINT_LEN: usize = 16;

/// The blake3 `derive_key` context for the deployment-key fingerprint. A fixed,
/// unique domain-separation string (distinct from the tenancy marker's), so this
/// object's fingerprint can never be confused with another keyed derivation.
const FINGERPRINT_CONTEXT: &str = "ravel-auth-v1-fingerprint";

/// The keyed hash of a bearer token under the deployment key:
/// `blake3::keyed_hash(deployment_key, token)`. Deterministic — the same token
/// under the same key always yields the same 32-byte hash — and one-way: the
/// bucket stores only this, never the token. Shared by every write and by
/// [`tenant_for_token`] so a resolved token is hashed exactly as a stored one
/// was.
pub fn token_hash(deployment_key: &[u8; 32], token: &[u8]) -> [u8; TOKEN_HASH_LEN] {
    *blake3::keyed_hash(deployment_key, token).as_bytes()
}

/// The 16-byte fingerprint of the deployment key:
/// `blake3::derive_key("ravel-auth-v1-fingerprint", deployment_key)` truncated to
/// 16 bytes (the `TenancyMarker::key_fingerprint` pattern). Stored in the object
/// so a reader configured with the wrong deployment key detects it and refuses,
/// rather than silently mismatching every token hash and failing all auth
/// opaquely. Never the key itself.
pub fn key_fingerprint(deployment_key: &[u8; 32]) -> [u8; KEY_FINGERPRINT_LEN] {
    let full = blake3::derive_key(FINGERPRINT_CONTEXT, deployment_key);
    let mut out = [0u8; KEY_FINGERPRINT_LEN];
    out.copy_from_slice(&full[..KEY_FINGERPRINT_LEN]);
    out
}

/// Width of the `token_fingerprint` a [`AuthTokenMapError::CrossTenantTokenCollision`]
/// names: a prefix of the stored `token_hash`, matching `ravel-cli tenant token
/// list`'s `TOKEN_FINGERPRINT_LEN` convention. Long enough to correlate a log
/// line with a `list` entry, short enough that this build never treats the
/// full keyed hash as a printable value either -- the hash is one-way and
/// deployment-key-scoped, but this module extends the "never a secret in an
/// error message" discipline to it anyway rather than drawing that line here.
const TOKEN_HASH_FINGERPRINT_LEN: usize = 8;

/// The short, printable fingerprint of a stored `token_hash` -- never the
/// hash itself and never the plaintext token -- for a
/// [`AuthTokenMapError::CrossTenantTokenCollision`] or a log line to name the
/// colliding token without exposing it.
fn hash_fingerprint_hex(hash: &[u8; TOKEN_HASH_LEN]) -> String {
    hex::encode(&hash[..TOKEN_HASH_FINGERPRINT_LEN])
}

/// One resolved token->tenant entry, decoded into a plain struct so consumers
/// never touch the proto type. `token_hash` is the 32-byte keyed hash, never a
/// plaintext token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenEntry {
    /// The 32-byte blake3 keyed hash of the bearer token.
    pub token_hash: [u8; TOKEN_HASH_LEN],
    /// The tenant this token authenticates as.
    pub tenant_id: String,
    /// Which writer owns this entry's lifecycle: [`MANAGED_BY_OPERATOR`],
    /// [`MANAGED_BY_CLI`], or a caller-chosen tag; `None` for unmanaged
    /// (every entry a pre-amendment writer wrote, and any entry a
    /// post-amendment writer creates without declaring an owner). The
    /// operator's remove/replace pass touches only entries whose
    /// `managed_by` is exactly `Some(MANAGED_BY_OPERATOR)` (ADR-0072
    /// decision 4 amendment).
    pub managed_by: Option<String>,
}

/// A deployment's decoded auth map: the deployment-key fingerprint it was written
/// under and its token entries. The pure, in-memory form the resolver caches and
/// [`tenant_for_token`] queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthTokenMap {
    /// Fingerprint of the deployment key the entries' hashes are keyed under.
    pub key_fingerprint: [u8; KEY_FINGERPRINT_LEN],
    /// The token->tenant entries; no `token_hash` repeats (enforced on
    /// decode, and on every write by [`write_map`]'s duplicate-hash guard).
    pub entries: Vec<TokenEntry>,
}

impl AuthTokenMap {
    /// Resolve a token hash to its tenant id, if present. A pure lookup over the
    /// decoded entries — the primitive the bounded-staleness resolver calls; it does no I/O,
    /// refresh, or on-miss re-read.
    pub fn tenant_for_hash(&self, hash: &[u8; TOKEN_HASH_LEN]) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| &e.token_hash == hash)
            .map(|e| e.tenant_id.as_str())
    }
}

/// Resolve a plaintext bearer token to its tenant id over an already-loaded map:
/// hash the token under `deployment_key` and look up the hash. The pure lookup
/// primitive; the plaintext is hashed and dropped, never compared or stored in
/// the clear. Returns `None` for an unknown token.
pub fn tenant_for_token<'a>(
    map: &'a AuthTokenMap,
    deployment_key: &[u8; 32],
    token: &[u8],
) -> Option<&'a str> {
    let hash = token_hash(deployment_key, token);
    map.tenant_for_hash(&hash)
}

/// The exact structural rule a corrupt auth map broke. Each is a fail-closed
/// decode error, never a silent normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMapDefect {
    /// An entry's `token_hash` is not exactly 32 bytes (not a blake3 output).
    BadHashLength,
    /// An entry's `tenant_id` is empty (a token must grant a named tenant).
    EmptyTenantId,
    /// Two entries share a `token_hash`, making resolution ambiguous.
    DuplicateTokenHash,
    /// The stored `key_fingerprint` is not exactly 16 bytes.
    BadFingerprintLength,
}

impl std::fmt::Display for AuthMapDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            AuthMapDefect::BadHashLength => {
                "a token_hash is not exactly 32 bytes (not a blake3 keyed hash)"
            }
            AuthMapDefect::EmptyTenantId => "a token entry has an empty tenant_id",
            AuthMapDefect::DuplicateTokenHash => {
                "two entries share a token_hash (ambiguous resolution)"
            }
            AuthMapDefect::BadFingerprintLength => "the key_fingerprint is not exactly 16 bytes",
        };
        f.write_str(s)
    }
}

/// A typed auth-map failure. Every variant is fatal to the touch that raised it,
/// the fail-closed-on-any-anomaly discipline the other durable-object modules
/// follow. None warn and continue.
#[derive(Debug, thiserror::Error)]
pub enum AuthTokenMapError {
    #[error("object store error on auth map {AUTH_KEY:?}: {source}")]
    Store {
        #[source]
        source: StoreError,
    },
    #[error("auth map {AUTH_KEY:?} could not be decoded: {source}")]
    Decode {
        #[source]
        source: prost::DecodeError,
    },
    #[error(
        "auth map {AUTH_KEY:?} declares format_version {got}, but this build only understands \
         version {AUTH_TOKEN_MAP_FORMAT_VERSION}: refusing rather than misread a future format"
    )]
    UnsupportedVersion { got: u32 },
    #[error("auth map {AUTH_KEY:?} is corrupt: {defect} (ADR-0066 decision 6)")]
    Corrupt { defect: AuthMapDefect },
    /// The stored map's `key_fingerprint` disagrees with the configured
    /// deployment key's. The reader is using the wrong deployment key: every
    /// token hash would silently mismatch and all auth would fail opaquely, so it
    /// refuses rather than open that silent hole (the `TenancyMarker` wrong-key
    /// guard, applied to auth).
    #[error(
        "auth map {AUTH_KEY:?} was written under a different deployment key (stored fingerprint \
         {stored}, configured {configured}): refusing to read tokens hashed under a key this \
         process does not hold (ADR-0066 decision 6)"
    )]
    WrongDeploymentKey { stored: String, configured: String },
    /// A caller passed an empty `tenant_id` to a write. A token must grant a
    /// named tenant; refused before any write so a corrupt entry is never
    /// persisted.
    #[error("cannot map a token to an empty tenant_id (ADR-0066 decision 6)")]
    EmptyTenantId,
    /// A concurrent write moved the object's version between this write's read and
    /// its `CasVersion` swap (or created it between a not-found read and the
    /// `CreateIfAbsent`). The loser re-reads rather than silently overwriting the
    /// winner (the `set_gc_config` CAS-conflict pattern).
    #[error(
        "a concurrent write changed auth map {AUTH_KEY:?} since this one read it (CAS precondition \
         failed): re-read and retry rather than overwrite the other write"
    )]
    CasConflict,
    /// Two different tenants presented a token that hashes to the same
    /// `token_hash` (ADR-0072 decision 4, second amendment). A token hash must resolve to exactly one tenant --
    /// [`tenant_for_token`] has no way to pick between two -- so this is
    /// refused before any write, never arbitrated by takeover. This is
    /// distinct from the same-tenant, different-`managed_by` case (the cli ->
    /// operator migration), which still takes the hash over in place: takeover
    /// only ever changes who owns a hash already resolving to this
    /// `tenant_id`, never which tenant it resolves to. Named by tenant id and
    /// a truncated, one-way token fingerprint only -- never the token value,
    /// and this build never prints the full stored hash either.
    #[error(
        "token {token_fingerprint} already authenticates tenant {existing_tenant:?}; refusing to \
         also grant it to tenant {attempted_tenant:?} (a token can resolve to only one tenant, \
         ADR-0072 decision 4)"
    )]
    CrossTenantTokenCollision {
        token_fingerprint: String,
        existing_tenant: String,
        attempted_tenant: String,
    },
}

/// Decode and validate a proto auth map, checking the version, the deployment-key
/// fingerprint, and the structural invariants fail-closed. A future format, a map
/// under the wrong deployment key, a bad hash length, an empty tenant id, or a
/// duplicate hash is refused rather than read.
fn decode_map(
    proto: &ProtoAuthTokenMap,
    deployment_key: &[u8; 32],
) -> Result<AuthTokenMap, AuthTokenMapError> {
    if proto.format_version > AUTH_TOKEN_MAP_FORMAT_VERSION {
        return Err(AuthTokenMapError::UnsupportedVersion {
            got: proto.format_version,
        });
    }
    if proto.key_fingerprint.len() != KEY_FINGERPRINT_LEN {
        return Err(AuthTokenMapError::Corrupt {
            defect: AuthMapDefect::BadFingerprintLength,
        });
    }
    let expected = key_fingerprint(deployment_key);
    if proto.key_fingerprint.as_slice() != expected.as_slice() {
        return Err(AuthTokenMapError::WrongDeploymentKey {
            stored: hex::encode(&proto.key_fingerprint),
            configured: hex::encode(expected),
        });
    }
    let mut entries = Vec::with_capacity(proto.entries.len());
    let mut seen: std::collections::HashSet<[u8; TOKEN_HASH_LEN]> =
        std::collections::HashSet::new();
    for e in &proto.entries {
        let hash: [u8; TOKEN_HASH_LEN] =
            e.token_hash
                .as_slice()
                .try_into()
                .map_err(|_| AuthTokenMapError::Corrupt {
                    defect: AuthMapDefect::BadHashLength,
                })?;
        if e.tenant_id.is_empty() {
            return Err(AuthTokenMapError::Corrupt {
                defect: AuthMapDefect::EmptyTenantId,
            });
        }
        if !seen.insert(hash) {
            return Err(AuthTokenMapError::Corrupt {
                defect: AuthMapDefect::DuplicateTokenHash,
            });
        }
        entries.push(TokenEntry {
            token_hash: hash,
            tenant_id: e.tenant_id.clone(),
            managed_by: e.managed_by.clone(),
        });
    }
    Ok(AuthTokenMap {
        key_fingerprint: expected,
        entries,
    })
}

/// Build the proto object from a domain map and the deployment key.
fn build_proto(map: &AuthTokenMap, updated_unix_ns: i64) -> ProtoAuthTokenMap {
    ProtoAuthTokenMap {
        format_version: AUTH_TOKEN_MAP_FORMAT_VERSION,
        key_fingerprint: map.key_fingerprint.to_vec(),
        entries: map
            .entries
            .iter()
            .map(|e| ProtoTokenHashEntry {
                token_hash: e.token_hash.to_vec(),
                tenant_id: e.tenant_id.clone(),
                managed_by: e.managed_by.clone(),
            })
            .collect(),
        updated_unix_ns,
    }
}

/// Read the deployment's auth map, returning the decoded map and the store
/// version needed for a later `CasVersion` swap. `Ok(None)` when no object
/// exists: a deployment that has provisioned no tokens yet (or uses only
/// OIDC/mTLS), a valid state, not an error. A present object is fully
/// version/fingerprint/structure-checked, so a corrupt object or one under the
/// wrong deployment key fails closed rather than being read as this deployment's
/// tokens.
pub async fn read_auth_map(
    store: &dyn ObjectStoreBackend,
    deployment_key: &[u8; 32],
) -> Result<Option<(AuthTokenMap, Version)>, AuthTokenMapError> {
    match store.get(AUTH_KEY, GetRange::Full).await {
        Ok(outcome) => {
            let proto = ProtoAuthTokenMap::decode(outcome.data.as_ref())
                .map_err(|source| AuthTokenMapError::Decode { source })?;
            let map = decode_map(&proto, deployment_key)?;
            Ok(Some((map, outcome.version)))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(source) => Err(AuthTokenMapError::Store { source }),
    }
}

/// The result of an [`upsert_token`] or [`remove_token`] write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOutcome {
    /// `sys/auth` did not exist and was created with `CreateIfAbsent`.
    Created,
    /// `sys/auth` existed and was swapped in place under `CasVersion`.
    Updated,
    /// A remove that matched no entry: nothing changed, no write issued.
    Unchanged,
}

/// Reject a map containing two entries with the same `token_hash` before it
/// is ever written. A belt-and-suspenders guard alongside [`decode_map`]'s
/// read-side check: no code path, present or future, may persist bytes
/// `decode_map` would then refuse to read back (ADR-0072 decision 4
/// amendment) -- a duplicate hash written once
/// bricks every subsequent read of `sys/auth`.
fn validate_no_duplicate_hashes(map: &AuthTokenMap) -> Result<(), AuthTokenMapError> {
    let mut seen: std::collections::HashSet<[u8; TOKEN_HASH_LEN]> =
        std::collections::HashSet::with_capacity(map.entries.len());
    for e in &map.entries {
        if !seen.insert(e.token_hash) {
            return Err(AuthTokenMapError::Corrupt {
                defect: AuthMapDefect::DuplicateTokenHash,
            });
        }
    }
    Ok(())
}

/// Write a new map under `CasVersion` (or create it with `CreateIfAbsent` when
/// none exists), the whole-record CAS-replace shared by [`upsert_token`] and
/// [`remove_token`]. `existing_version` is `Some` when a read observed an object
/// to swap, `None` when the read observed none. A concurrent write is a
/// [`AuthTokenMapError::CasConflict`], never a silent overwrite. Refuses
/// (before issuing any store call) a map that would leave two entries
/// sharing a `token_hash` -- see [`validate_no_duplicate_hashes`].
async fn write_map(
    store: &dyn ObjectStoreBackend,
    map: &AuthTokenMap,
    existing_version: Option<Version>,
    now_ns: i64,
) -> Result<SetOutcome, AuthTokenMapError> {
    validate_no_duplicate_hashes(map)?;
    let bytes = build_proto(map, now_ns).encode_to_vec();
    match existing_version {
        Some(version) => match store
            .put(
                AUTH_KEY,
                bytes.into(),
                PutOptions {
                    mode: PutMode::CasVersion(version),
                    checksum: None,
                },
            )
            .await
        {
            Ok(_) => Ok(SetOutcome::Updated),
            Err(StoreError::PreconditionFailed) => Err(AuthTokenMapError::CasConflict),
            Err(source) => Err(AuthTokenMapError::Store { source }),
        },
        None => match store
            .put(AUTH_KEY, bytes.into(), PutOptions::create_if_absent())
            .await
        {
            Ok(_) => Ok(SetOutcome::Created),
            Err(StoreError::AlreadyExists) => Err(AuthTokenMapError::CasConflict),
            Err(source) => Err(AuthTokenMapError::Store { source }),
        },
    }
}

/// Add or replace the token->tenant mapping for `token`, whole-record
/// CAS-replace (ADR-0066 decision 6). The token is hashed under `deployment_key`
/// and only its hash is persisted; the plaintext is never written. When no
/// entry for the hash exists, one is appended for `tenant_id`; when one
/// already exists for the SAME `tenant_id`, this is a no-op re-write; when one
/// exists for a DIFFERENT tenant, the call is refused with
/// [`AuthTokenMapError::CrossTenantTokenCollision`] before any write --
/// re-pointing a token to a different tenant is not a single-call operation,
/// since a token value cannot deterministically authenticate two tenants at
/// once (ADR-0072 decision 4, second amendment); revoke
/// the old tenant's token first, then upsert it for the new one. On a fresh
/// bucket the object is created with `CreateIfAbsent`; a concurrent write is a
/// [`AuthTokenMapError::CasConflict`] the caller re-reads and retries on.
pub async fn upsert_token(
    store: &dyn ObjectStoreBackend,
    deployment_key: &[u8; 32],
    token: &[u8],
    tenant_id: &str,
    now_ns: i64,
) -> Result<SetOutcome, AuthTokenMapError> {
    upsert_token_owned(store, deployment_key, token, tenant_id, None, now_ns).await
}

/// Same as [`upsert_token`], but stamps `managed_by` onto the entry
/// (overwriting whatever it carried before), so a later scoped remove/replace
/// pass ([`remove_tokens_by_tenant_owned_by`], [`replace_tenant_tokens`]) can
/// tell this write's owner from another writer's (ADR-0072 decision 4
/// amendment). `None` marks the entry unmanaged, matching every entry
/// [`upsert_token`] writes. Matches on `token_hash`: re-upserting a hash the
/// SAME `tenant_id` already owns under a different owner re-tags it to this
/// call's `managed_by` in place -- takeover, never a second entry for the
/// same hash (the cli -> operator migration, ADR-0072 decision 4 amendment). A hash already owned by a DIFFERENT tenant is refused outright with
/// [`AuthTokenMapError::CrossTenantTokenCollision`] before any write -- one
/// token value cannot deterministically authenticate two tenants, so this is
/// never arbitrated by takeover (ADR-0072 decision 4, second amendment).
pub async fn upsert_token_owned(
    store: &dyn ObjectStoreBackend,
    deployment_key: &[u8; 32],
    token: &[u8],
    tenant_id: &str,
    managed_by: Option<&str>,
    now_ns: i64,
) -> Result<SetOutcome, AuthTokenMapError> {
    if tenant_id.is_empty() {
        return Err(AuthTokenMapError::EmptyTenantId);
    }
    let hash = token_hash(deployment_key, token);
    let (mut map, version) = match read_auth_map(store, deployment_key).await? {
        Some((map, version)) => (map, Some(version)),
        None => (
            AuthTokenMap {
                key_fingerprint: key_fingerprint(deployment_key),
                entries: Vec::new(),
            },
            None,
        ),
    };
    match map.entries.iter_mut().find(|e| e.token_hash == hash) {
        Some(entry) if entry.tenant_id == tenant_id => {
            entry.managed_by = managed_by.map(str::to_string);
        }
        Some(entry) => {
            return Err(AuthTokenMapError::CrossTenantTokenCollision {
                token_fingerprint: hash_fingerprint_hex(&hash),
                existing_tenant: entry.tenant_id.clone(),
                attempted_tenant: tenant_id.to_string(),
            });
        }
        None => map.entries.push(TokenEntry {
            token_hash: hash,
            tenant_id: tenant_id.to_string(),
            managed_by: managed_by.map(str::to_string),
        }),
    }
    write_map(store, &map, version, now_ns).await
}

/// Remove the mapping for `token` (a revocation), whole-record CAS-replace. The
/// token is hashed under `deployment_key` to find the entry; the plaintext is
/// never written or compared in the clear. Returns [`SetOutcome::Unchanged`]
/// (issuing no write) when no object exists or no entry matches, so revoking an
/// already-absent token is a clean no-op. A concurrent write to an existing
/// object is a [`AuthTokenMapError::CasConflict`].
pub async fn remove_token(
    store: &dyn ObjectStoreBackend,
    deployment_key: &[u8; 32],
    token: &[u8],
    now_ns: i64,
) -> Result<SetOutcome, AuthTokenMapError> {
    let hash = token_hash(deployment_key, token);
    let (mut map, version) = match read_auth_map(store, deployment_key).await? {
        Some((map, version)) => (map, version),
        None => return Ok(SetOutcome::Unchanged),
    };
    let before = map.entries.len();
    map.entries.retain(|e| e.token_hash != hash);
    if map.entries.len() == before {
        return Ok(SetOutcome::Unchanged);
    }
    write_map(store, &map, Some(version), now_ns).await
}

/// Remove every token mapped to `tenant_id` (a tenant-wide revocation),
/// whole-record CAS-replace. Needs no plaintext and no per-token knowledge:
/// entries carry their `tenant_id` in the clear, so this filters on that field
/// alone. This is what makes revocation correct after a restart with no
/// in-memory history — the caller need not have ever seen this tenant's
/// tokens, only its name (ADR-0072 decision 4). Returns
/// [`SetOutcome::Unchanged`] (issuing no write) when no object exists or no
/// entry matches `tenant_id`. A concurrent write to an existing object is a
/// [`AuthTokenMapError::CasConflict`].
pub async fn remove_tokens_by_tenant(
    store: &dyn ObjectStoreBackend,
    deployment_key: &[u8; 32],
    tenant_id: &str,
    now_ns: i64,
) -> Result<SetOutcome, AuthTokenMapError> {
    let (mut map, version) = match read_auth_map(store, deployment_key).await? {
        Some((map, version)) => (map, version),
        None => return Ok(SetOutcome::Unchanged),
    };
    let before = map.entries.len();
    map.entries.retain(|e| e.tenant_id != tenant_id);
    if map.entries.len() == before {
        return Ok(SetOutcome::Unchanged);
    }
    write_map(store, &map, Some(version), now_ns).await
}

/// Remove every token mapped to `tenant_id` AND owned by `managed_by` (a
/// scoped tenant-wide revocation), whole-record CAS-replace. Unlike
/// [`remove_tokens_by_tenant`], an entry for the same tenant owned by a
/// different writer (a different `managed_by` tag, or unmanaged/absent) is
/// left untouched: the primitive the operator's reconcile loop uses so its
/// remove pass never revokes a CLI-provisioned or v1-shaped unmanaged entry
/// it never wrote (ADR-0072 decision 4 amendment). Returns
/// [`SetOutcome::Unchanged`] (issuing no write) when no object exists or no
/// entry matches both `tenant_id` and `managed_by`. A concurrent write to an
/// existing object is a [`AuthTokenMapError::CasConflict`].
pub async fn remove_tokens_by_tenant_owned_by(
    store: &dyn ObjectStoreBackend,
    deployment_key: &[u8; 32],
    tenant_id: &str,
    managed_by: &str,
    now_ns: i64,
) -> Result<SetOutcome, AuthTokenMapError> {
    let (mut map, version) = match read_auth_map(store, deployment_key).await? {
        Some((map, version)) => (map, version),
        None => return Ok(SetOutcome::Unchanged),
    };
    let before = map.entries.len();
    map.entries
        .retain(|e| !(e.tenant_id == tenant_id && e.managed_by.as_deref() == Some(managed_by)));
    if map.entries.len() == before {
        return Ok(SetOutcome::Unchanged);
    }
    write_map(store, &map, Some(version), now_ns).await
}

/// Replace `tenant_id`'s token set owned by `managed_by` with
/// `plaintext_tokens`, whole-record CAS-replace. Entries for `tenant_id`
/// whose `managed_by` matches exactly are dropped and replaced; entries for
/// other tenants, and entries for the same tenant owned by a different
/// writer (a different tag, or unmanaged), are otherwise untouched (ADR-0072
/// decision 4 amendment) -- a CLI-provisioned token for a tenant the
/// operator also manages via Secret survives an operator reconcile. Each
/// new entry is stamped with `managed_by`. Each token is hashed under
/// `deployment_key` before being stored; the plaintext is never persisted.
/// This is the primitive a Secret-driven reconcile loop uses: the Secret is
/// the tenant's whole desired token set for that owner, so converging to it
/// is a scoped replace, not an incremental upsert/remove pair.
///
/// `token_hash` is unique across the whole map (ADR-0072 decision 4
/// amendment), but the rule for a collision depends
/// on whether it crosses a tenant boundary (ADR-0072 decision 4, second
/// amendment):
///
/// - A `token_hash` already owned by THIS `tenant_id` (under a different
///   `managed_by`, or unmanaged) is dropped and the new entry takes over it
///   in place -- takeover, matching [`upsert_token_owned`]'s hash-keyed
///   semantics. Without this, a caller reusing a token value the same
///   tenant already holds under a different owner tag would make this call
///   append a second entry sharing that hash, which [`write_map`]'s
///   duplicate-hash guard now refuses to persist, rather than [`decode_map`]
///   refusing every later read.
/// - A `token_hash` already owned by a DIFFERENT tenant is refused outright
///   with [`AuthTokenMapError::CrossTenantTokenCollision`], before any
///   retain or write. A single token value cannot deterministically
///   authenticate two tenants, so this is never arbitrated by takeover: a
///   takeover here would make each tenant's own reconcile loop keep taking
///   the hash back for itself every cycle, converging on nothing (the flap
///   this amendment fixes) rather than a stable, converged, if incomplete,
///   result.
///
/// Returns [`SetOutcome::Unchanged`] (issuing no write) when the resulting
/// scoped entry set is byte-for-byte identical to the current one --
/// including on a fresh bucket with an empty `plaintext_tokens`, which
/// creates nothing. Converged reconciles must cost zero writes (ADR-0072
/// decision 3's Object Lock versioning makes a needless PUT expensive, not
/// just wasteful). On a fresh bucket with a non-empty result the object is
/// created with `CreateIfAbsent`; a concurrent write is a
/// [`AuthTokenMapError::CasConflict`] the caller re-reads and retries on.
pub async fn replace_tenant_tokens(
    store: &dyn ObjectStoreBackend,
    deployment_key: &[u8; 32],
    tenant_id: &str,
    plaintext_tokens: &[Vec<u8>],
    managed_by: Option<&str>,
    now_ns: i64,
) -> Result<SetOutcome, AuthTokenMapError> {
    if tenant_id.is_empty() {
        return Err(AuthTokenMapError::EmptyTenantId);
    }
    let (mut map, version) = match read_auth_map(store, deployment_key).await? {
        Some((map, version)) => (map, Some(version)),
        None => (
            AuthTokenMap {
                key_fingerprint: key_fingerprint(deployment_key),
                entries: Vec::new(),
            },
            None,
        ),
    };

    let desired_entries: Vec<TokenEntry> = plaintext_tokens
        .iter()
        .map(|token| TokenEntry {
            token_hash: token_hash(deployment_key, token),
            tenant_id: tenant_id.to_string(),
            managed_by: managed_by.map(str::to_string),
        })
        .collect();
    let mut desired_hashes: Vec<_> = desired_entries.iter().map(|e| e.token_hash).collect();
    desired_hashes.sort_unstable();

    let mut current_hashes: Vec<_> = map
        .entries
        .iter()
        .filter(|e| e.tenant_id == tenant_id && e.managed_by.as_deref() == managed_by)
        .map(|e| e.token_hash)
        .collect();
    current_hashes.sort_unstable();

    if current_hashes == desired_hashes {
        return Ok(SetOutcome::Unchanged);
    }

    // Refuse a cross-tenant collision before touching anything (ADR-0072
    // decision 4, second amendment): a desired hash
    // already present under a DIFFERENT tenant_id is never taken over here.
    // Checked against the map exactly as read, before any retain -- the
    // takeover below must never run on a candidate this loop would have
    // refused.
    let desired_hash_set: std::collections::HashSet<[u8; TOKEN_HASH_LEN]> =
        desired_entries.iter().map(|e| e.token_hash).collect();
    if let Some(other) = map
        .entries
        .iter()
        .find(|e| e.tenant_id != tenant_id && desired_hash_set.contains(&e.token_hash))
    {
        return Err(AuthTokenMapError::CrossTenantTokenCollision {
            token_fingerprint: hash_fingerprint_hex(&other.token_hash),
            existing_tenant: other.tenant_id.clone(),
            attempted_tenant: tenant_id.to_string(),
        });
    }

    // token_hash is unique within one tenant's entries (ADR-0072 decision 4
    // amendment): the last writer of a hash this
    // tenant already owns takes ownership of it, regardless of which
    // `managed_by` wrote it. Drop this tenant/owner's own prior entries AND
    // any pre-existing entry for this SAME tenant whose hash collides with a
    // desired entry -- the cross-tenant case was already refused above, so
    // every remaining collision here is same-tenant, different-owner, safe
    // to take over. Scoping the drop to only this tenant/owner (as a prior
    // version did) leaves a differently-owned colliding entry for the same
    // tenant in place, so the extend below would append a second entry with
    // the same hash and `write_map`'s duplicate-hash guard would then refuse
    // the write outright (previously: `decode_map` would refuse every
    // subsequent read instead, since the write went through unguarded).
    map.entries.retain(|e| {
        let owned_by_this_scope = e.tenant_id == tenant_id && e.managed_by.as_deref() == managed_by;
        let collides_with_desired =
            e.tenant_id == tenant_id && desired_hash_set.contains(&e.token_hash);
        !(owned_by_this_scope || collides_with_desired)
    });
    map.entries.extend(desired_entries);
    write_map(store, &map, version, now_ns).await
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use std::sync::Arc;

    const KEY: &[u8; 32] = &[0x42u8; 32];

    fn mem() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    #[test]
    fn auth_key_is_bucket_root() {
        assert_eq!(AUTH_KEY, "sys/auth", "deployment-wide, at the bucket root");
    }

    /// Round-trip: a map with two tokens under two tenants encodes, decodes, and
    /// resolves each token back to its tenant.
    #[tokio::test]
    async fn two_token_map_round_trips_and_resolves() {
        let store = mem();
        upsert_token(store.as_ref(), KEY, b"token-alpha", "tenant-a", 1)
            .await
            .expect("first upsert creates the object");
        upsert_token(store.as_ref(), KEY, b"token-beta", "tenant-b", 2)
            .await
            .expect("second upsert updates in place");

        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("object present");
        assert_eq!(map.entries.len(), 2);
        assert_eq!(
            tenant_for_token(&map, KEY, b"token-alpha"),
            Some("tenant-a")
        );
        assert_eq!(tenant_for_token(&map, KEY, b"token-beta"), Some("tenant-b"));
        assert_eq!(
            tenant_for_token(&map, KEY, b"token-unknown"),
            None,
            "an unknown token resolves to no tenant"
        );
    }

    /// The load-bearing security property: the bucket NEVER holds a plaintext
    /// token, only its blake3 keyed hash. Construct a token, compute the expected
    /// hash the same way the implementation does, and assert (a) the raw stored
    /// bytes contain the hash and never the plaintext, and (b) the decoded entry
    /// carries exactly that hash.
    #[tokio::test]
    async fn plaintext_token_never_round_trips_only_the_hash() {
        let store = mem();
        let plaintext: &[u8] = b"super-secret-bearer-token";
        upsert_token(store.as_ref(), KEY, plaintext, "tenant-a", 1)
            .await
            .expect("upsert");

        // The expected hash, computed exactly as the implementation does.
        let expected = token_hash(KEY, plaintext);
        assert_ne!(
            expected.as_slice(),
            plaintext,
            "sanity: the hash is not the plaintext"
        );

        // The raw bytes on the store hold the hash, never the plaintext.
        let raw = store.get(AUTH_KEY, GetRange::Full).await.expect("get");
        let raw_bytes = raw.data.as_ref();
        assert!(
            !contains_subslice(raw_bytes, plaintext),
            "the plaintext token must never appear in the stored object"
        );
        assert!(
            contains_subslice(raw_bytes, expected.as_slice()),
            "the stored object must carry the keyed hash"
        );

        // The decoded entry carries exactly the hash, and resolution goes only
        // through the hash.
        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(map.entries.len(), 1);
        assert_eq!(
            map.entries[0].token_hash, expected,
            "the entry stores the keyed hash, not the token"
        );
        assert_eq!(tenant_for_token(&map, KEY, plaintext), Some("tenant-a"));
    }

    /// A different deployment key yields a different hash for the same token, so a
    /// bucket read with the wrong key resolves nothing (the key is load-bearing).
    #[tokio::test]
    async fn different_key_yields_different_hash() {
        let other_key: &[u8; 32] = &[0x99u8; 32];
        assert_ne!(
            token_hash(KEY, b"t").as_slice(),
            token_hash(other_key, b"t").as_slice(),
            "the keyed hash depends on the deployment key"
        );
    }

    /// Upserting a token already mapped to a DIFFERENT tenant is refused with
    /// a typed [`AuthTokenMapError::CrossTenantTokenCollision`] before any
    /// write -- one token value cannot deterministically authenticate two
    /// tenants, so `upsert_token` must never re-point it the way it used to
    /// (ADR-0072 decision 4, second amendment).
    /// Flipped-line proof: this fails against the pre-fix code, where the
    /// `Some(entry)` match arm in `upsert_token_owned` unconditionally
    /// rewrote `entry.tenant_id` with no equality check against the caller's
    /// `tenant_id`; against that code this test observes `Ok(Updated)` and
    /// `tenant_for_token == Some("tenant-b")` instead of the error asserted
    /// here.
    #[tokio::test]
    async fn upsert_token_refuses_a_cross_tenant_collision() {
        let store = mem();
        upsert_token(store.as_ref(), KEY, b"shared-secret-value", "tenant-a", 1)
            .await
            .expect("create");
        let err = upsert_token(store.as_ref(), KEY, b"shared-secret-value", "tenant-b", 2)
            .await
            .expect_err("cross-tenant re-point must be refused");
        assert!(
            matches!(
                &err,
                AuthTokenMapError::CrossTenantTokenCollision {
                    existing_tenant,
                    attempted_tenant,
                    ..
                } if existing_tenant == "tenant-a" && attempted_tenant == "tenant-b"
            ),
            "unexpected error: {err:?}"
        );
        assert!(
            !err.to_string().contains("shared-secret-value"),
            "the error must never name the token value: {err}"
        );
        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            map.entries.len(),
            1,
            "the refused call must not touch the existing entry"
        );
        assert_eq!(
            tenant_for_token(&map, KEY, b"shared-secret-value"),
            Some("tenant-a")
        );
    }

    /// Re-upserting the same token for the SAME tenant is a harmless no-op
    /// rewrite, not a collision -- the same-tenant path the check in
    /// [`upsert_token_owned`] must still let through (door 1/2 regression
    /// guard, ADR-0072 decision 4 amendment).
    #[tokio::test]
    async fn upsert_same_token_same_tenant_is_idempotent() {
        let store = mem();
        upsert_token(store.as_ref(), KEY, b"tok", "tenant-a", 1)
            .await
            .expect("create");
        upsert_token(store.as_ref(), KEY, b"tok", "tenant-a", 2)
            .await
            .expect("re-upsert for the same tenant succeeds");
        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(map.entries.len(), 1);
        assert_eq!(tenant_for_token(&map, KEY, b"tok"), Some("tenant-a"));
    }

    /// Removing a token (revocation) drops its entry; a subsequent resolve fails.
    #[tokio::test]
    async fn remove_token_revokes() {
        let store = mem();
        upsert_token(store.as_ref(), KEY, b"tok", "tenant-a", 1)
            .await
            .expect("create");
        let outcome = remove_token(store.as_ref(), KEY, b"tok", 2)
            .await
            .expect("remove");
        assert_eq!(outcome, SetOutcome::Updated);
        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(tenant_for_token(&map, KEY, b"tok"), None, "token revoked");
    }

    /// Removing a token that was never present is a clean no-op: no error, no
    /// write, `Unchanged`.
    #[tokio::test]
    async fn remove_absent_token_is_unchanged() {
        let store = mem();
        // No object at all.
        let outcome = remove_token(store.as_ref(), KEY, b"never", 1)
            .await
            .expect("remove on empty bucket");
        assert_eq!(outcome, SetOutcome::Unchanged);
        assert!(
            read_auth_map(store.as_ref(), KEY)
                .await
                .expect("read")
                .is_none(),
            "no object was created by a no-op remove"
        );
    }

    /// An absent object is `Ok(None)`, not an error.
    #[tokio::test]
    async fn read_absent_map_is_none() {
        let store = mem();
        assert!(
            read_auth_map(store.as_ref(), KEY)
                .await
                .expect("absent is not an error")
                .is_none()
        );
    }

    /// A concurrent update racing an existing object: the loser's CAS `put` is a
    /// typed `CasConflict`, never a silent overwrite. Proven with `FaultStore`.
    #[tokio::test]
    async fn cas_conflict_on_concurrent_update() {
        let inner = MemoryStore::new();
        // Seed an object so upsert takes the CasVersion path.
        let seed = build_proto(
            &AuthTokenMap {
                key_fingerprint: key_fingerprint(KEY),
                entries: vec![],
            },
            1,
        );
        inner
            .put(
                AUTH_KEY,
                seed.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("sys/auth"),
        );
        let store = FaultStore::new(inner, plan);
        let err = upsert_token(&store, KEY, b"tok", "tenant-a", 2)
            .await
            .expect_err("a CAS precondition failure must surface as a typed conflict");
        assert!(matches!(err, AuthTokenMapError::CasConflict), "got: {err}");
        assert_eq!(
            store.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::FailedConditionalWrite
            ),
            1,
            "the injected precondition failure must have fired"
        );
    }

    /// A concurrent create race: the loser's CreateIfAbsent returns AlreadyExists
    /// and surfaces as a typed conflict.
    #[tokio::test]
    async fn cas_conflict_on_concurrent_create() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("sys/auth"),
        );
        let store = FaultStore::new(inner, plan);
        let err = upsert_token(&store, KEY, b"tok", "tenant-a", 1)
            .await
            .expect_err("a losing CreateIfAbsent race must surface as a typed conflict");
        assert!(matches!(err, AuthTokenMapError::CasConflict), "got: {err}");
    }

    /// A map written under a different deployment key is refused: the stored
    /// fingerprint disagrees with the configured key's, so reading it would
    /// silently mismatch every token hash.
    #[tokio::test]
    async fn read_rejects_wrong_deployment_key() {
        let store = mem();
        upsert_token(store.as_ref(), KEY, b"tok", "tenant-a", 1)
            .await
            .expect("write under KEY");
        let wrong_key: &[u8; 32] = &[0x01u8; 32];
        let err = read_auth_map(store.as_ref(), wrong_key)
            .await
            .expect_err("reading under the wrong deployment key must be refused");
        assert!(
            matches!(err, AuthTokenMapError::WrongDeploymentKey { .. }),
            "got: {err}"
        );
    }

    /// An empty tenant_id is refused before any write.
    #[tokio::test]
    async fn upsert_empty_tenant_is_refused() {
        let store = mem();
        let err = upsert_token(store.as_ref(), KEY, b"tok", "", 1)
            .await
            .expect_err("an empty tenant_id must be refused");
        assert!(
            matches!(err, AuthTokenMapError::EmptyTenantId),
            "got: {err}"
        );
        assert!(
            read_auth_map(store.as_ref(), KEY)
                .await
                .expect("read")
                .is_none(),
            "a refused upsert writes nothing"
        );
    }

    /// A duplicate token_hash in a stored object is a typed corrupt error, never
    /// an ambiguous silent resolution.
    #[tokio::test]
    async fn read_rejects_duplicate_token_hash() {
        let store = mem();
        let hash = token_hash(KEY, b"tok");
        let proto = build_proto(
            &AuthTokenMap {
                key_fingerprint: key_fingerprint(KEY),
                entries: vec![
                    TokenEntry {
                        token_hash: hash,
                        tenant_id: "tenant-a".into(),
                        managed_by: None,
                    },
                    TokenEntry {
                        token_hash: hash,
                        tenant_id: "tenant-b".into(),
                        managed_by: None,
                    },
                ],
            },
            1,
        );
        store
            .put(
                AUTH_KEY,
                proto.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed duplicate");
        let err = read_auth_map(store.as_ref(), KEY)
            .await
            .expect_err("a duplicate token_hash must be refused");
        assert!(
            matches!(
                err,
                AuthTokenMapError::Corrupt {
                    defect: AuthMapDefect::DuplicateTokenHash
                }
            ),
            "got: {err}"
        );
    }

    /// A bad token_hash length (not 32 bytes) is a typed corrupt error.
    #[tokio::test]
    async fn read_rejects_bad_hash_length() {
        let store = mem();
        let proto = ProtoAuthTokenMap {
            format_version: AUTH_TOKEN_MAP_FORMAT_VERSION,
            key_fingerprint: key_fingerprint(KEY).to_vec(),
            entries: vec![ProtoTokenHashEntry {
                token_hash: vec![0u8; 16], // too short
                tenant_id: "tenant-a".into(),
                managed_by: None,
            }],
            updated_unix_ns: 1,
        };
        store
            .put(
                AUTH_KEY,
                proto.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed bad hash");
        let err = read_auth_map(store.as_ref(), KEY)
            .await
            .expect_err("a bad hash length must be refused");
        assert!(
            matches!(
                err,
                AuthTokenMapError::Corrupt {
                    defect: AuthMapDefect::BadHashLength
                }
            ),
            "got: {err}"
        );
    }

    /// A future format_version is refused rather than misread.
    #[tokio::test]
    async fn read_rejects_future_format_version() {
        let store = mem();
        let proto = ProtoAuthTokenMap {
            format_version: AUTH_TOKEN_MAP_FORMAT_VERSION + 1,
            key_fingerprint: key_fingerprint(KEY).to_vec(),
            entries: vec![],
            updated_unix_ns: 1,
        };
        store
            .put(
                AUTH_KEY,
                proto.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed future");
        let err = read_auth_map(store.as_ref(), KEY)
            .await
            .expect_err("a future format_version must be refused");
        assert!(
            matches!(err, AuthTokenMapError::UnsupportedVersion { .. }),
            "got: {err}"
        );
    }

    /// A corrupt (undecodable) object body is a typed `Decode` error, never a
    /// panic.
    #[tokio::test]
    async fn corrupt_object_is_typed_decode_error() {
        let store = mem();
        store
            .put(
                AUTH_KEY,
                vec![0xFF, 0xFF, 0xFF, 0x07].into(),
                PutOptions::default(),
            )
            .await
            .expect("seed garbage");
        let err = read_auth_map(store.as_ref(), KEY)
            .await
            .expect_err("garbage must be a typed decode error, not a panic");
        assert!(
            matches!(err, AuthTokenMapError::Decode { .. }),
            "got: {err}"
        );
    }

    /// `remove_tokens_by_tenant` drops every entry for the named tenant and
    /// leaves other tenants' entries untouched, given no plaintext at all.
    #[tokio::test]
    async fn remove_tokens_by_tenant_revokes_only_that_tenant() {
        let store = mem();
        upsert_token(store.as_ref(), KEY, b"a1", "tenant-a", 1)
            .await
            .expect("create a1");
        upsert_token(store.as_ref(), KEY, b"a2", "tenant-a", 2)
            .await
            .expect("create a2");
        upsert_token(store.as_ref(), KEY, b"b1", "tenant-b", 3)
            .await
            .expect("create b1");

        let outcome = remove_tokens_by_tenant(store.as_ref(), KEY, "tenant-a", 4)
            .await
            .expect("remove by tenant");
        assert_eq!(outcome, SetOutcome::Updated);

        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(map.entries.len(), 1, "only tenant-b's entry survives");
        assert_eq!(tenant_for_token(&map, KEY, b"a1"), None);
        assert_eq!(tenant_for_token(&map, KEY, b"a2"), None);
        assert_eq!(tenant_for_token(&map, KEY, b"b1"), Some("tenant-b"));
    }

    /// A tenant with no entries (or no object at all) is a clean no-op.
    #[tokio::test]
    async fn remove_tokens_by_tenant_absent_is_unchanged() {
        let store = mem();
        let outcome = remove_tokens_by_tenant(store.as_ref(), KEY, "tenant-a", 1)
            .await
            .expect("remove on empty bucket");
        assert_eq!(outcome, SetOutcome::Unchanged);

        upsert_token(store.as_ref(), KEY, b"tok", "tenant-b", 2)
            .await
            .expect("create tenant-b entry");
        let outcome = remove_tokens_by_tenant(store.as_ref(), KEY, "tenant-a", 3)
            .await
            .expect("remove a tenant with no entries");
        assert_eq!(outcome, SetOutcome::Unchanged);
    }

    /// A concurrent update racing `remove_tokens_by_tenant` surfaces as a typed
    /// `CasConflict`, never a silent overwrite.
    #[tokio::test]
    async fn remove_tokens_by_tenant_cas_conflict_on_concurrent_update() {
        let inner = MemoryStore::new();
        let seed = build_proto(
            &AuthTokenMap {
                key_fingerprint: key_fingerprint(KEY),
                entries: vec![TokenEntry {
                    token_hash: token_hash(KEY, b"tok"),
                    tenant_id: "tenant-a".into(),
                    managed_by: None,
                }],
            },
            1,
        );
        inner
            .put(
                AUTH_KEY,
                seed.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("sys/auth"),
        );
        let store = FaultStore::new(inner, plan);
        let err = remove_tokens_by_tenant(&store, KEY, "tenant-a", 2)
            .await
            .expect_err("a CAS precondition failure must surface as a typed conflict");
        assert!(matches!(err, AuthTokenMapError::CasConflict), "got: {err}");
        assert_eq!(
            store.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::FailedConditionalWrite
            ),
            1,
            "the injected precondition failure must have fired"
        );
    }

    /// `replace_tenant_tokens` swaps in exactly the given token set for the
    /// named tenant, leaving other tenants' entries untouched, and drops
    /// whatever the tenant held before.
    #[tokio::test]
    async fn replace_tenant_tokens_replaces_exactly_that_tenant() {
        let store = mem();
        upsert_token(store.as_ref(), KEY, b"a-old", "tenant-a", 1)
            .await
            .expect("seed old tenant-a token");
        upsert_token(store.as_ref(), KEY, b"b1", "tenant-b", 2)
            .await
            .expect("seed tenant-b token");

        let outcome = replace_tenant_tokens(
            store.as_ref(),
            KEY,
            "tenant-a",
            &[b"a-new-1".to_vec(), b"a-new-2".to_vec()],
            None,
            3,
        )
        .await
        .expect("replace");
        assert_eq!(outcome, SetOutcome::Updated);

        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            map.entries.len(),
            3,
            "two new tenant-a entries, one tenant-b"
        );
        assert_eq!(
            tenant_for_token(&map, KEY, b"a-old"),
            None,
            "the old token was dropped"
        );
        assert_eq!(tenant_for_token(&map, KEY, b"a-new-1"), Some("tenant-a"));
        assert_eq!(tenant_for_token(&map, KEY, b"a-new-2"), Some("tenant-a"));
        assert_eq!(
            tenant_for_token(&map, KEY, b"b1"),
            Some("tenant-b"),
            "tenant-b's entry is untouched"
        );
    }

    /// `replace_tenant_tokens` on a fresh bucket creates the object.
    #[tokio::test]
    async fn replace_tenant_tokens_creates_on_fresh_bucket() {
        let store = mem();
        let outcome =
            replace_tenant_tokens(store.as_ref(), KEY, "tenant-a", &[b"tok".to_vec()], None, 1)
                .await
                .expect("replace on fresh bucket");
        assert_eq!(outcome, SetOutcome::Created);
        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(tenant_for_token(&map, KEY, b"tok"), Some("tenant-a"));
    }

    /// An empty tenant_id is refused before any write, matching `upsert_token`.
    #[tokio::test]
    async fn replace_tenant_tokens_empty_tenant_is_refused() {
        let store = mem();
        let err = replace_tenant_tokens(store.as_ref(), KEY, "", &[b"tok".to_vec()], None, 1)
            .await
            .expect_err("an empty tenant_id must be refused");
        assert!(
            matches!(err, AuthTokenMapError::EmptyTenantId),
            "got: {err}"
        );
        assert!(
            read_auth_map(store.as_ref(), KEY)
                .await
                .expect("read")
                .is_none(),
            "a refused replace writes nothing"
        );
    }

    /// A concurrent create race on `replace_tenant_tokens` surfaces as a typed
    /// conflict, matching `upsert_token`'s create path.
    #[tokio::test]
    async fn replace_tenant_tokens_cas_conflict_on_concurrent_create() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("sys/auth"),
        );
        let store = FaultStore::new(inner, plan);
        let err = replace_tenant_tokens(&store, KEY, "tenant-a", &[b"tok".to_vec()], None, 1)
            .await
            .expect_err("a losing CreateIfAbsent race must surface as a typed conflict");
        assert!(matches!(err, AuthTokenMapError::CasConflict), "got: {err}");
    }

    /// `replace_tenant_tokens` is a no-op -- no write issued -- when the
    /// tenant's resulting scoped entry set already matches the desired one: a converged Secret-driven reconcile must
    /// cost zero PUTs, not rewrite the whole map every interval.
    #[tokio::test]
    async fn replace_tenant_tokens_converged_is_unchanged() {
        let store = mem();
        let first = replace_tenant_tokens(
            store.as_ref(),
            KEY,
            "tenant-a",
            &[b"a1".to_vec(), b"a2".to_vec()],
            Some(MANAGED_BY_OPERATOR),
            1,
        )
        .await
        .expect("first replace creates the object");
        assert_eq!(first, SetOutcome::Created);

        let second = replace_tenant_tokens(
            store.as_ref(),
            KEY,
            "tenant-a",
            &[b"a1".to_vec(), b"a2".to_vec()],
            Some(MANAGED_BY_OPERATOR),
            2,
        )
        .await
        .expect("second replace with the same set");
        assert_eq!(
            second,
            SetOutcome::Unchanged,
            "an identical desired set must not rewrite the map"
        );
    }

    /// `replace_tenant_tokens` on a fresh bucket with an empty desired set is
    /// also `Unchanged`: nothing exists and nothing is wanted, so no object
    /// is created.
    #[tokio::test]
    async fn replace_tenant_tokens_empty_desired_on_fresh_bucket_is_unchanged() {
        let store = mem();
        let outcome = replace_tenant_tokens(
            store.as_ref(),
            KEY,
            "tenant-a",
            &[],
            Some(MANAGED_BY_OPERATOR),
            1,
        )
        .await
        .expect("replace with nothing to converge to");
        assert_eq!(outcome, SetOutcome::Unchanged);
        assert!(
            read_auth_map(store.as_ref(), KEY)
                .await
                .expect("read")
                .is_none(),
            "no object was created by a converged empty replace"
        );
    }

    /// `replace_tenant_tokens` is scoped by `managed_by`: replacing the
    /// operator-owned set for a tenant leaves that same tenant's
    /// CLI-provisioned entries untouched -- the
    /// operator's remove/replace pass must never revoke a token it did not
    /// write.
    #[tokio::test]
    async fn replace_tenant_tokens_is_scoped_to_its_managed_by_owner() {
        let store = mem();
        upsert_token_owned(
            store.as_ref(),
            KEY,
            b"cli-tok",
            "tenant-a",
            Some(MANAGED_BY_CLI),
            1,
        )
        .await
        .expect("seed a CLI-provisioned token for tenant-a");

        let outcome = replace_tenant_tokens(
            store.as_ref(),
            KEY,
            "tenant-a",
            &[b"op-tok".to_vec()],
            Some(MANAGED_BY_OPERATOR),
            2,
        )
        .await
        .expect("operator replace");
        assert_eq!(outcome, SetOutcome::Updated);

        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            tenant_for_token(&map, KEY, b"cli-tok"),
            Some("tenant-a"),
            "the CLI-provisioned entry survives an operator-scoped replace for the same tenant"
        );
        assert_eq!(tenant_for_token(&map, KEY, b"op-tok"), Some("tenant-a"));
    }

    /// `remove_tokens_by_tenant_owned_by` only removes entries owned by the
    /// given `managed_by` tag: a CLI-provisioned tenant and a v1-shaped
    /// unmanaged entry both survive an operator-scoped remove for the same
    /// tenant name; an operator-managed entry for
    /// that tenant is still removed.
    #[tokio::test]
    async fn remove_tokens_by_tenant_owned_by_spares_other_owners() {
        let store = mem();
        // A v1-shaped unmanaged entry (no managed_by field at all).
        upsert_token(store.as_ref(), KEY, b"legacy-tok", "tenant-a", 1)
            .await
            .expect("seed unmanaged (v1) entry");
        // A CLI-provisioned entry for the SAME tenant name.
        upsert_token_owned(
            store.as_ref(),
            KEY,
            b"cli-tok",
            "tenant-a",
            Some(MANAGED_BY_CLI),
            2,
        )
        .await
        .expect("seed CLI-owned entry");
        // An operator-managed entry for the same tenant.
        upsert_token_owned(
            store.as_ref(),
            KEY,
            b"op-tok",
            "tenant-a",
            Some(MANAGED_BY_OPERATOR),
            3,
        )
        .await
        .expect("seed operator-owned entry");

        let outcome = remove_tokens_by_tenant_owned_by(
            store.as_ref(),
            KEY,
            "tenant-a",
            MANAGED_BY_OPERATOR,
            4,
        )
        .await
        .expect("operator-scoped remove");
        assert_eq!(outcome, SetOutcome::Updated);

        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            tenant_for_token(&map, KEY, b"legacy-tok"),
            Some("tenant-a"),
            "an unmanaged (v1) entry must survive an operator-scoped remove"
        );
        assert_eq!(
            tenant_for_token(&map, KEY, b"cli-tok"),
            Some("tenant-a"),
            "a CLI-owned entry must survive an operator-scoped remove"
        );
        assert_eq!(
            tenant_for_token(&map, KEY, b"op-tok"),
            None,
            "the operator-owned entry for the same tenant is revoked"
        );
    }

    /// Door 1: an unmanaged/v1 entry for
    /// (tenant, token) followed by an operator-scoped `replace_tenant_tokens`
    /// for the SAME tenant and token must converge to one readable entry,
    /// not two sharing a hash. Against the pre-fix `retain` --
    /// `!(e.tenant_id == tenant_id && e.managed_by.as_deref() == managed_by)`
    /// -- the unmanaged entry's `managed_by` (`None`) never equals
    /// `Some("operator")`, so the predicate is false, the old entry survives,
    /// and `extend` appends a second entry with the identical hash; the
    /// subsequent `read_auth_map` then fails with
    /// `Corrupt(DuplicateTokenHash)`.
    #[tokio::test]
    async fn replace_tenant_tokens_takes_over_hash_from_unmanaged_entry() {
        let store = mem();
        upsert_token(store.as_ref(), KEY, b"shared-tok", "acme", 1)
            .await
            .expect("seed unmanaged (v1) entry");

        let outcome = replace_tenant_tokens(
            store.as_ref(),
            KEY,
            "acme",
            &[b"shared-tok".to_vec()],
            Some(MANAGED_BY_OPERATOR),
            2,
        )
        .await
        .expect("operator replace must take over the colliding hash, not duplicate it");
        assert_eq!(outcome, SetOutcome::Updated);

        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read-back must succeed: the map must stay decodable")
            .expect("present");
        assert_eq!(
            map.entries.len(),
            1,
            "the unmanaged entry and the operator entry must converge to one entry, not two \
             sharing a hash"
        );
        assert_eq!(tenant_for_token(&map, KEY, b"shared-tok"), Some("acme"));
        assert_eq!(
            map.entries[0].managed_by.as_deref(),
            Some(MANAGED_BY_OPERATOR),
            "last writer (the operator replace) takes ownership of the hash"
        );
    }

    /// Door 2: a CLI upsert flips an
    /// operator-owned token's `managed_by` to `"cli"` in place (expected,
    /// hash-keyed takeover semantics); the operator's next scoped replace for
    /// that same tenant/token must still converge to one entry, not append a
    /// second one tagged `"operator"`. Against the pre-fix `retain`, the
    /// entry is now owned by `"cli"`, so the operator-scoped predicate
    /// (`managed_by == Some("operator")`) does not match it, the entry
    /// survives, and `extend` appends a duplicate-hash `"operator"` entry;
    /// `read_auth_map` then fails with `Corrupt(DuplicateTokenHash)`.
    #[tokio::test]
    async fn replace_tenant_tokens_reclaims_hash_flipped_by_a_cli_upsert() {
        let store = mem();
        upsert_token_owned(
            store.as_ref(),
            KEY,
            b"shared-tok",
            "acme",
            Some(MANAGED_BY_OPERATOR),
            1,
        )
        .await
        .expect("seed operator-owned entry");
        // The CLI upserts the SAME token, flipping its owner to "cli" in place.
        upsert_token_owned(
            store.as_ref(),
            KEY,
            b"shared-tok",
            "acme",
            Some(MANAGED_BY_CLI),
            2,
        )
        .await
        .expect("cli upsert takes over the hash");

        let outcome = replace_tenant_tokens(
            store.as_ref(),
            KEY,
            "acme",
            &[b"shared-tok".to_vec()],
            Some(MANAGED_BY_OPERATOR),
            3,
        )
        .await
        .expect("operator reconcile must reclaim the hash, not duplicate it");
        assert_eq!(outcome, SetOutcome::Updated);

        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read-back must succeed: the map must stay decodable")
            .expect("present");
        assert_eq!(
            map.entries.len(),
            1,
            "the cli-flipped entry and the operator's replace must converge to one entry"
        );
        assert_eq!(
            map.entries[0].managed_by.as_deref(),
            Some(MANAGED_BY_OPERATOR),
            "the operator reclaims ownership on its next reconcile"
        );
    }

    /// Door 3 / flap regression (ADR-0072 decision 4 second amendment):
    /// two tenants sharing one token value, each converged via its own
    /// `replace_tenant_tokens` call (what `reconcile_sys_auth` does, once per
    /// tenant key in the Secret). The prior round's fix took the hash over
    /// for the second tenant -- `SetOutcome::Updated`, `tenant_for_token ==
    /// Some("globex")` -- which is exactly the defect this round fixes: the
    /// NEXT cycle would present "acme" again, take the hash back, and so on
    /// forever, never converging. The second call must instead be refused
    /// with a typed `CrossTenantTokenCollision` naming both tenants, write
    /// nothing, and leave "acme" holding the token.
    ///
    /// Flipped line: in `replace_tenant_tokens`, the pre-fix `retain`
    /// predicate's `collides_with_desired` term was
    /// `desired_hash_set.contains(&e.token_hash)` (no tenant check), which
    /// let this call drop acme's entry and take over the hash instead of
    /// refusing. This test fails against that line: it asserts an `Err`
    /// where the pre-fix code returned `Ok(SetOutcome::Updated)`.
    #[tokio::test]
    async fn replace_tenant_tokens_refuses_a_cross_tenant_collision() {
        let store = mem();
        replace_tenant_tokens(
            store.as_ref(),
            KEY,
            "acme",
            &[b"shared-tok".to_vec()],
            Some(MANAGED_BY_OPERATOR),
            1,
        )
        .await
        .expect("first tenant converges");

        let err = replace_tenant_tokens(
            store.as_ref(),
            KEY,
            "globex",
            &[b"shared-tok".to_vec()],
            Some(MANAGED_BY_OPERATOR),
            2,
        )
        .await
        .expect_err("a token another tenant already owns must be refused, not taken over");
        match &err {
            AuthTokenMapError::CrossTenantTokenCollision {
                existing_tenant,
                attempted_tenant,
                token_fingerprint,
            } => {
                assert_eq!(existing_tenant, "acme");
                assert_eq!(attempted_tenant, "globex");
                assert!(
                    !token_fingerprint.is_empty(),
                    "the fingerprint must be present so a human can correlate the refusal"
                );
            }
            other => panic!("expected CrossTenantTokenCollision, got {other:?}"),
        }
        let message = err.to_string();
        assert!(
            !message.contains("shared-tok"),
            "the error must never print the token value: {message}"
        );

        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read-back")
            .expect("present");
        assert_eq!(map.entries.len(), 1, "the refused call must write nothing");
        assert_eq!(
            tenant_for_token(&map, KEY, b"shared-tok"),
            Some("acme"),
            "acme, the first writer, keeps the token; globex's refused call changed nothing"
        );
    }

    /// The convergence property the flap violated, proven at this layer: once
    /// "acme" holds a token and "globex" has been refused it, repeating BOTH
    /// calls a second time (an identical reconcile cycle) must leave the map
    /// byte-for-byte unchanged -- "acme" hits the `Unchanged` fast path
    /// (already converged, no write), and "globex" is refused again with the
    /// same typed error, still writing nothing. The operator-level equivalent
    /// of this test (in `services/ravel-operator/src/controller.rs`) is the
    /// one that asserts zero `sys/auth` PUTs on cycle two; this one pins the
    /// same property directly on `replace_tenant_tokens`, one call at a time.
    #[tokio::test]
    async fn replace_tenant_tokens_cross_tenant_refusal_converges_on_repeat() {
        let store = mem();
        let cycle = |now_ns: i64| {
            let store = store.clone();
            async move {
                let acme = replace_tenant_tokens(
                    store.as_ref(),
                    KEY,
                    "acme",
                    &[b"shared-tok".to_vec()],
                    Some(MANAGED_BY_OPERATOR),
                    now_ns,
                )
                .await;
                let globex = replace_tenant_tokens(
                    store.as_ref(),
                    KEY,
                    "globex",
                    &[b"shared-tok".to_vec()],
                    Some(MANAGED_BY_OPERATOR),
                    now_ns + 1,
                )
                .await;
                (acme, globex)
            }
        };

        let (acme1, globex1) = cycle(1).await;
        assert!(
            matches!(acme1, Ok(SetOutcome::Created)),
            "acme creates the object first: {acme1:?}"
        );
        assert!(
            matches!(
                globex1,
                Err(AuthTokenMapError::CrossTenantTokenCollision { .. })
            ),
            "globex is refused on cycle one: {globex1:?}"
        );

        let (map_after_1, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");

        let (acme2, globex2) = cycle(2).await;
        assert!(
            matches!(acme2, Ok(SetOutcome::Unchanged)),
            "acme already owns exactly this hash: zero writes on cycle two: {acme2:?}"
        );
        assert!(
            matches!(
                globex2,
                Err(AuthTokenMapError::CrossTenantTokenCollision { .. })
            ),
            "globex is refused again on cycle two, identically: {globex2:?}"
        );

        let (map_after_2, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            map_after_1, map_after_2,
            "an identical second cycle must leave the map byte-for-byte unchanged, the exact \
             property the last-writer-wins takeover violated"
        );
    }

    /// Same-tenant, different-`managed_by`-owner takeover (doors 1 and 2)
    /// must still converge to one entry after this round's fix: the
    /// cross-tenant refusal above must not have disturbed the same-tenant
    /// case, which stays a legitimate takeover.
    #[tokio::test]
    async fn replace_tenant_tokens_same_tenant_cross_owner_still_converges() {
        let store = mem();
        // Seed an unmanaged (v1-shaped) entry, matching door 1.
        upsert_token(store.as_ref(), KEY, b"shared-tok", "acme", 1)
            .await
            .expect("seed unmanaged entry");

        let outcome = replace_tenant_tokens(
            store.as_ref(),
            KEY,
            "acme",
            &[b"shared-tok".to_vec()],
            Some(MANAGED_BY_OPERATOR),
            2,
        )
        .await
        .expect("same-tenant takeover across managed_by owners must still succeed");
        assert_eq!(outcome, SetOutcome::Updated);

        let (map, _v) = read_auth_map(store.as_ref(), KEY)
            .await
            .expect("read-back must succeed")
            .expect("present");
        assert_eq!(
            map.entries.len(),
            1,
            "same-tenant takeover across owners converges to one entry, not two"
        );
        assert_eq!(tenant_for_token(&map, KEY, b"shared-tok"), Some("acme"));
        assert_eq!(
            map.entries[0].managed_by.as_deref(),
            Some(MANAGED_BY_OPERATOR),
            "the operator's replace still takes ownership within the same tenant"
        );
    }

    /// Direct unit test on the write-side guard itself (belt-and-suspenders
    /// alongside `decode_map`'s read-side check): `write_map` must refuse a
    /// map containing two entries with the same `token_hash` before issuing
    /// any store call, so no caller can ever persist bytes `decode_map` would
    /// then refuse to read back. Against the pre-fix `write_map` (no
    /// `validate_no_duplicate_hashes` call), this `put` would succeed and the
    /// object would become permanently unreadable.
    #[tokio::test]
    async fn write_map_rejects_a_map_with_duplicate_hashes() {
        let store = mem();
        let hash = token_hash(KEY, b"tok");
        let map = AuthTokenMap {
            key_fingerprint: key_fingerprint(KEY),
            entries: vec![
                TokenEntry {
                    token_hash: hash,
                    tenant_id: "acme".into(),
                    managed_by: None,
                },
                TokenEntry {
                    token_hash: hash,
                    tenant_id: "globex".into(),
                    managed_by: Some(MANAGED_BY_OPERATOR.to_string()),
                },
            ],
        };

        let err = write_map(store.as_ref(), &map, None, 1)
            .await
            .expect_err("a map with a duplicate token_hash must be refused before any write");
        assert!(
            matches!(
                err,
                AuthTokenMapError::Corrupt {
                    defect: AuthMapDefect::DuplicateTokenHash
                }
            ),
            "got: {err}"
        );
        assert!(
            store.get(AUTH_KEY, GetRange::Full).await.is_err(),
            "the rejected write must not have reached the store at all"
        );
    }

    /// Byte-subslice search helper for the plaintext-never-stored assertion.
    fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
        if needle.is_empty() || needle.len() > haystack.len() {
            return false;
        }
        haystack.windows(needle.len()).any(|w| w == needle)
    }
}
