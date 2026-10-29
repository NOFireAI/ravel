//! Conditional-write outcomes the agents store depends on, checked on a real
//! S3-compatible endpoint against the MemoryStore oracle (ADR-3331 decision
//! 13). The agent HEAD is written with `CreateIfAbsent` when absent and with
//! `CasVersion` after that, and `_dir`/dependency objects race on
//! `CreateIfAbsent`; each test runs the same steps on both stores in the same
//! function and asserts the typed outcomes are equal.
//!
//! Gated on `RAVEL_REAL_STORE_URL`: unset, each test prints one skip line and
//! returns. Optional overrides: `RAVEL_REAL_STORE_BUCKET` (must exist;
//! default `ravel-chaos`), `RAVEL_REAL_STORE_ACCESS_KEY`,
//! `RAVEL_REAL_STORE_SECRET_KEY` (default the compose store's
//! `ravel`/`ravel-dev-secret`), `RAVEL_REAL_STORE_REGION`.
#![cfg(feature = "agents")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{MULTIPART_THRESHOLD, S3Config, S3Store};
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PutMode, PutOptions, PutOutcome, StoreError, Version,
};

const URL_VAR: &str = "RAVEL_REAL_STORE_URL";

/// How many times a raced `CreateIfAbsent` that surfaced as a retryable
/// conflict is retried; the object-store contract says to retry until the
/// answer is `AlreadyExists`.
const CONFLICT_RETRIES: usize = 20;

/// The typed outcome of a put, comparable across backends. `StoreError`
/// carries backend-specific messages, so only the variant is compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Ok,
    NotFound,
    AlreadyExists,
    PreconditionFailed,
    Other(&'static str),
}

fn kind(result: &Result<PutOutcome, StoreError>) -> Kind {
    match result {
        Ok(_) => Kind::Ok,
        Err(StoreError::NotFound) => Kind::NotFound,
        Err(StoreError::AlreadyExists) => Kind::AlreadyExists,
        Err(StoreError::PreconditionFailed) => Kind::PreconditionFailed,
        Err(StoreError::AccessDenied(_)) => Kind::Other("AccessDenied"),
        Err(StoreError::Throttled { .. }) => Kind::Other("Throttled"),
        Err(StoreError::Timeout) => Kind::Other("Timeout"),
        Err(StoreError::Transient(_)) => Kind::Other("Transient"),
        Err(StoreError::Permanent(_)) => Kind::Other("Permanent"),
        Err(StoreError::Unsupported { .. }) => Kind::Other("Unsupported"),
        Err(_) => Kind::Other("other"),
    }
}

fn real_store() -> Option<S3Store> {
    let Ok(url) = env::var(URL_VAR) else {
        return None;
    };
    let config = S3Config {
        bucket: env::var("RAVEL_REAL_STORE_BUCKET").unwrap_or_else(|_| "ravel-chaos".to_string()),
        region: env::var("RAVEL_REAL_STORE_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        allow_http: url.starts_with("http://"),
        endpoint: Some(url),
        access_key_id: env::var("RAVEL_REAL_STORE_ACCESS_KEY")
            .unwrap_or_else(|_| "ravel".to_string()),
        secret_access_key: env::var("RAVEL_REAL_STORE_SECRET_KEY")
            .unwrap_or_else(|_| "ravel-dev-secret".to_string()),
        force_path_style: true,
        kms_key_id: None,
        session_token: None,
        credentials_file: None,
        auth: Default::default(),
        instance_metadata_endpoint: None,
    };
    Some(S3Store::new(config).expect("S3Store::new must accept the real-store config"))
}

/// A run-unique key under the agents layout, so a persistent bucket never
/// hands this run an object an earlier run left behind.
fn run_key(case: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the epoch")
        .as_nanos();
    format!("t/real-store-{nanos}/ag/{case}/head")
}

fn put_opts(mode: PutMode) -> PutOptions {
    PutOptions {
        mode,
        ..PutOptions::default()
    }
}

/// The CAS forms the agent HEAD publish uses, as typed outcomes in order:
/// expected-absent on an absent key, expected-version with a never-issued
/// version on an absent key, expected-version matching, expected-version
/// stale, and expected-absent on a present key. The agents store never
/// deletes a HEAD, so no step CASes a deleted key.
async fn cas_steps(
    store: &dyn ObjectStoreBackend,
    key: &str,
    never_issued: Version,
) -> Vec<(&'static str, Kind)> {
    let mut out = Vec::new();

    let absent_cas = store
        .put(
            key,
            Bytes::from_static(b"v0"),
            put_opts(PutMode::CasVersion(never_issued)),
        )
        .await;
    out.push(("cas_version_on_absent_key", kind(&absent_cas)));

    let created = store
        .put(
            key,
            Bytes::from_static(b"v1"),
            PutOptions::create_if_absent(),
        )
        .await;
    out.push(("create_if_absent_on_absent_key", kind(&created)));
    let v1 = created
        .expect("expected-absent create on an absent key")
        .version;

    let updated = store
        .put(
            key,
            Bytes::from_static(b"v2"),
            put_opts(PutMode::CasVersion(v1.clone())),
        )
        .await;
    out.push(("cas_version_matching", kind(&updated)));
    updated.expect("expected-version update with the current version");

    let stale = store
        .put(
            key,
            Bytes::from_static(b"v3"),
            put_opts(PutMode::CasVersion(v1)),
        )
        .await;
    out.push(("cas_version_stale", kind(&stale)));

    let again = store
        .put(
            key,
            Bytes::from_static(b"v4"),
            PutOptions::create_if_absent(),
        )
        .await;
    out.push(("create_if_absent_on_present_key", kind(&again)));

    let body = store
        .get(key, GetRange::Full)
        .await
        .expect("get after the CAS sequence");
    assert_eq!(
        body.data.as_ref(),
        b"v2",
        "only the matching CAS may have written the key"
    );
    let _ = store.delete(key).await;
    out
}

#[tokio::test]
async fn cas_version_on_absent_key_matches_memory_oracle_on_rustfs() {
    let Some(real) = real_store() else {
        println!("skipping agents real-store CAS oracle test: {URL_VAR} not set");
        return;
    };
    let memory = MemoryStore::new();
    let key = run_key("cas");

    let memory_kinds = cas_steps(&memory, &key, Version("mem-v-999999".to_string())).await;
    // An ETag-shaped version the real store never issued for this key.
    let real_kinds = cas_steps(
        &real,
        &key,
        Version("\"00000000000000000000000000000000\"".to_string()),
    )
    .await;

    println!("memory oracle: {memory_kinds:?}");
    println!("real store:    {real_kinds:?}");
    assert_eq!(
        memory_kinds,
        vec![
            ("cas_version_on_absent_key", Kind::PreconditionFailed),
            ("create_if_absent_on_absent_key", Kind::Ok),
            ("cas_version_matching", Kind::Ok),
            ("cas_version_stale", Kind::PreconditionFailed),
            ("create_if_absent_on_present_key", Kind::AlreadyExists),
        ],
        "memory oracle outcomes"
    );
    assert_eq!(
        real_kinds, memory_kinds,
        "the real store must give the oracle's typed outcome at every step"
    );
}

/// One `CreateIfAbsent` put, retried while the backend reports a retryable
/// conditional-request conflict.
async fn create_if_absent(store: &dyn ObjectStoreBackend, key: &str, data: Bytes) -> Kind {
    for _ in 0..CONFLICT_RETRIES {
        let result = store
            .put(key, data.clone(), PutOptions::create_if_absent())
            .await;
        match &result {
            Err(e) if e.is_retryable() => continue,
            _ => return kind(&result),
        }
    }
    Kind::Other("retries exhausted")
}

/// Two concurrent `CreateIfAbsent` puts of different content on one key:
/// returns both outcomes and which payload the key holds afterwards.
async fn collide(store: &dyn ObjectStoreBackend, key: &str, size: usize) -> (Kind, Kind, u8) {
    let a = Bytes::from(vec![b'a'; size]);
    let b = Bytes::from(vec![b'b'; size]);
    let (ka, kb) = tokio::join!(
        create_if_absent(store, key, a),
        create_if_absent(store, key, b)
    );
    let stored = store
        .get(key, GetRange::Full)
        .await
        .expect("get after the collision");
    assert_eq!(stored.data.len(), size, "stored object size");
    let first = stored.data[0];
    assert!(
        stored.data.iter().all(|&byte| byte == first),
        "stored bytes must be one writer's payload, never a mix"
    );
    let _ = store.delete(key).await;
    (ka, kb, first)
}

/// Reduces a collision to the facts compared across stores: how many
/// writers won, the loser's typed answer, and whether the stored bytes are
/// the winner's.
fn collision_summary((ka, kb, stored): (Kind, Kind, u8)) -> (usize, Kind, bool) {
    let winners = [ka, kb].iter().filter(|k| **k == Kind::Ok).count();
    let (loser, winner_byte) = match (ka, kb) {
        (Kind::Ok, other) => (other, b'a'),
        (other, Kind::Ok) => (other, b'b'),
        (other, _) => (other, 0),
    };
    (winners, loser, stored == winner_byte)
}

#[tokio::test]
async fn create_if_absent_collision_matches_memory_oracle() {
    let Some(real) = real_store() else {
        println!("skipping agents real-store CreateIfAbsent collision test: {URL_VAR} not set");
        return;
    };
    let memory = MemoryStore::new();

    // CreateIfAbsent never takes the multipart path (the S3 adapter keeps
    // conditional puts on a single PUT at any size); the second size sits
    // above the multipart threshold to pin that a large conditional put
    // still has exactly one winner.
    for size in [64, MULTIPART_THRESHOLD + 1] {
        let key = run_key(&format!("collide-{size}"));
        let memory_summary = collision_summary(collide(&memory, &key, size).await);
        let real_summary = collision_summary(collide(&real, &key, size).await);
        println!("size {size}: memory oracle {memory_summary:?}, real store {real_summary:?}");
        assert_eq!(
            memory_summary,
            (1, Kind::AlreadyExists, true),
            "memory oracle collision at size {size}"
        );
        assert_eq!(
            real_summary, memory_summary,
            "the real store must give the oracle's collision outcome at size {size}"
        );
    }
}
