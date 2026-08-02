//! Empirical backend qualification (ADR-0050 section 6, adversarial review
//! finding S5-20, docs/object-store-contract.md "Semantics adapters MUST
//! honor").
//!
//! Ravel's commit protocol and catalog assume two properties of the backing
//! store that nothing previously checked at runtime: conditional writes
//! (`CreateIfAbsent` and `CasVersion` reject a losing writer without
//! applying it) and strong consistency (a `get`/`list` issued right after a
//! `put` always observes it). A backend can report `Capabilities{ .. }`
//! honestly or dishonestly; either way, [`run_conformance_suite`] exercises
//! the real behavior instead of trusting the self-report.
//!
//! The suite can only falsify these properties, never prove them: a pass
//! means the backend did not fail any probe run against it here and now, not
//! that it is correct under every load and network condition it will ever
//! see in production. Treat a pass as qualification, not proof.
//!
//! Every probe reports which specific [`Property`] it tested, pass or fail,
//! and a human-readable detail: an operator (or `ravel-cli store qualify`)
//! must be able to tell "this backend cannot do conditional writes" from
//! "this backend's listing is eventually consistent" rather than a single
//! opaque failure.

use bytes::Bytes;

use crate::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all};

/// Version of the probe set itself, recorded alongside a pass in
/// `sys/qualification` (ADR-0050 section 6). Bump this whenever a probe is
/// added, tightened, or its pass criteria changes, so an old qualification
/// record can be told apart from one taken under the current suite.
pub const CONFORMANCE_SUITE_VERSION: u32 = 1;

/// One property the object store contract requires, named so a failure
/// report can point at exactly what a backend cannot do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Property {
    /// `PutMode::CreateIfAbsent`: a second create on an existing key must
    /// fail with `AlreadyExists` and must not apply its bytes.
    ConditionalWriteCreateIfAbsent,
    /// `PutMode::CasVersion`: a put against a stale version must fail with
    /// `PreconditionFailed` and must not apply its bytes.
    ConditionalWriteCasVersion,
    /// A `get` of a key immediately after the `put` that created it must
    /// return exactly those bytes, every time.
    ConsistentReadAfterWrite,
    /// A `list`/`list_all` of a key's prefix immediately after the `put`
    /// that created it must include that key, every time.
    ConsistentListAfterWrite,
}

impl Property {
    /// Stable, greppable identifier -- this is what lands in CLI output and
    /// the qualification record, so an operator can search the contract doc
    /// and this module for the exact string.
    pub fn name(&self) -> &'static str {
        match self {
            Property::ConditionalWriteCreateIfAbsent => "conditional_write_create_if_absent",
            Property::ConditionalWriteCasVersion => "conditional_write_cas_version",
            Property::ConsistentReadAfterWrite => "consistent_read_after_write",
            Property::ConsistentListAfterWrite => "consistent_list_after_write",
        }
    }
}

/// Outcome of probing one [`Property`].
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub property: Property,
    pub passed: bool,
    /// Human-readable explanation: on failure, what was observed and why it
    /// violates the property; on pass, what was exercised.
    pub detail: String,
}

impl ProbeResult {
    fn pass(property: Property, detail: impl Into<String>) -> Self {
        ProbeResult {
            property,
            passed: true,
            detail: detail.into(),
        }
    }

    fn fail(property: Property, detail: impl Into<String>) -> Self {
        ProbeResult {
            property,
            passed: false,
            detail: detail.into(),
        }
    }
}

/// Result of a full conformance run: one [`ProbeResult`] per [`Property`].
#[derive(Debug, Clone)]
pub struct ConformanceReport {
    pub results: Vec<ProbeResult>,
}

impl ConformanceReport {
    /// Whether every probed property passed. A backend qualifies only when
    /// this is true.
    pub fn passed(&self) -> bool {
        self.results.iter().all(|r| r.passed)
    }

    /// The properties that failed, in probe order.
    pub fn failures(&self) -> impl Iterator<Item = &ProbeResult> {
        self.results.iter().filter(|r| !r.passed)
    }
}

/// Run every conformance probe against `store`, scoping all writes under
/// `scratch_prefix` (ADR-0050 section 6: `sys/qualify/<run-id>/`). Never
/// panics on a misbehaving backend: every probe treats an unexpected
/// `StoreError` or an unexpected success as a failed [`ProbeResult`], not a
/// crash, so this is safe to run against an unqualified or actively broken
/// backend.
pub async fn run_conformance_suite(
    store: &dyn ObjectStoreBackend,
    scratch_prefix: &str,
) -> ConformanceReport {
    let prefix = if scratch_prefix.ends_with('/') {
        scratch_prefix.to_string()
    } else {
        format!("{scratch_prefix}/")
    };
    let results = vec![
        probe_conditional_write_create_if_absent(store, &prefix).await,
        probe_conditional_write_cas_version(store, &prefix).await,
        probe_consistent_read_after_write(store, &prefix).await,
        probe_consistent_list_after_write(store, &prefix).await,
    ];
    ConformanceReport { results }
}

async fn probe_conditional_write_create_if_absent(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> ProbeResult {
    let property = Property::ConditionalWriteCreateIfAbsent;
    let key = format!("{prefix}cas/create-if-absent");

    if let Err(err) = store
        .put(
            &key,
            Bytes::from_static(b"winner"),
            PutOptions::create_if_absent(),
        )
        .await
    {
        return ProbeResult::fail(
            property,
            format!("first CreateIfAbsent put on a fresh key failed: {err}"),
        );
    }

    match store
        .put(
            &key,
            Bytes::from_static(b"loser"),
            PutOptions::create_if_absent(),
        )
        .await
    {
        Ok(_) => ProbeResult::fail(
            property,
            "a second CreateIfAbsent put on the same key succeeded; this backend does not \
             enforce conditional-create preconditions"
                .to_string(),
        ),
        Err(StoreError::AlreadyExists) => match store.get(&key, GetRange::Full).await {
            Ok(outcome) if outcome.data == Bytes::from_static(b"winner") => ProbeResult::pass(
                property,
                "losing writer correctly rejected with AlreadyExists, winner's bytes intact",
            ),
            Ok(_) => ProbeResult::fail(
                property,
                "the losing writer's bytes were applied despite AlreadyExists being returned"
                    .to_string(),
            ),
            Err(err) => {
                ProbeResult::fail(property, format!("could not verify winner content: {err}"))
            }
        },
        Err(other) => ProbeResult::fail(
            property,
            format!(
                "second CreateIfAbsent put failed with {other} instead of AlreadyExists \
                 (docs/object-store-contract.md: conditional-put failure mapping)"
            ),
        ),
    }
}

async fn probe_conditional_write_cas_version(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> ProbeResult {
    let property = Property::ConditionalWriteCasVersion;
    let key = format!("{prefix}cas/version");

    let first = match store
        .put(&key, Bytes::from_static(b"v1"), PutOptions::default())
        .await
    {
        Ok(outcome) => outcome,
        Err(err) => return ProbeResult::fail(property, format!("seed put failed: {err}")),
    };

    if let Err(err) = store
        .put(
            &key,
            Bytes::from_static(b"v2"),
            PutOptions {
                mode: PutMode::CasVersion(first.version.clone()),
                checksum: None,
            },
        )
        .await
    {
        return ProbeResult::fail(
            property,
            format!("CAS put against the current version was rejected: {err}"),
        );
    }

    match store
        .put(
            &key,
            Bytes::from_static(b"stale-writer"),
            PutOptions {
                mode: PutMode::CasVersion(first.version),
                checksum: None,
            },
        )
        .await
    {
        Ok(_) => ProbeResult::fail(
            property,
            "a CAS put against a stale version succeeded; this backend does not enforce \
             version preconditions"
                .to_string(),
        ),
        Err(StoreError::PreconditionFailed) => match store.get(&key, GetRange::Full).await {
            Ok(outcome) if outcome.data == Bytes::from_static(b"v2") => ProbeResult::pass(
                property,
                "stale-version writer correctly rejected with PreconditionFailed, current \
                 version's bytes intact",
            ),
            Ok(_) => ProbeResult::fail(
                property,
                "the stale writer's bytes were applied despite PreconditionFailed being returned"
                    .to_string(),
            ),
            Err(err) => {
                ProbeResult::fail(property, format!("could not verify current content: {err}"))
            }
        },
        Err(other) => ProbeResult::fail(
            property,
            format!(
                "stale CAS put failed with {other} instead of PreconditionFailed \
                 (docs/object-store-contract.md: conditional-put failure mapping)"
            ),
        ),
    }
}

/// How many put-then-check cycles each consistency probe runs. More than one
/// cycle matters: a backend can get lucky (or unlucky) once, and ADR-0050
/// section 6 explicitly calls for "repeated put-then-list cycles".
const CONSISTENCY_CYCLES: usize = 5;

async fn probe_consistent_read_after_write(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> ProbeResult {
    let property = Property::ConsistentReadAfterWrite;

    for i in 0..CONSISTENCY_CYCLES {
        let key = format!("{prefix}raw/{i}");
        let payload = Bytes::from(format!("payload-{i}"));
        if let Err(err) = store
            .put(&key, payload.clone(), PutOptions::default())
            .await
        {
            return ProbeResult::fail(property, format!("put {key} failed: {err}"));
        }
        match store.get(&key, GetRange::Full).await {
            Ok(outcome) if outcome.data == payload => {}
            Ok(outcome) => {
                return ProbeResult::fail(
                    property,
                    format!(
                        "read of {key} immediately after write returned {} bytes, expected {} \
                         -- read-after-write is not strongly consistent",
                        outcome.data.len(),
                        payload.len()
                    ),
                );
            }
            Err(err) => {
                return ProbeResult::fail(
                    property,
                    format!(
                        "read of {key} immediately after write failed with {err}; \
                         read-after-write is not strongly consistent"
                    ),
                );
            }
        }
    }
    ProbeResult::pass(
        property,
        format!("{CONSISTENCY_CYCLES} put-then-get cycles all returned the just-written bytes"),
    )
}

async fn probe_consistent_list_after_write(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> ProbeResult {
    let property = Property::ConsistentListAfterWrite;
    let list_prefix = format!("{prefix}law/");

    for i in 0..CONSISTENCY_CYCLES {
        let key = format!("{list_prefix}{i}");
        if let Err(err) = store
            .put(&key, Bytes::from_static(b"x"), PutOptions::default())
            .await
        {
            return ProbeResult::fail(property, format!("put {key} failed: {err}"));
        }
        match list_all(store, &list_prefix).await {
            Ok(objects) => {
                if !objects.iter().any(|meta| meta.key == key) {
                    return ProbeResult::fail(
                        property,
                        format!(
                            "listing {list_prefix} immediately after writing {key} did not \
                             include it -- listing is not strongly consistent"
                        ),
                    );
                }
            }
            Err(err) => {
                return ProbeResult::fail(property, format!("listing {list_prefix} failed: {err}"));
            }
        }
    }
    ProbeResult::pass(
        property,
        format!("{CONSISTENCY_CYCLES} put-then-list cycles all observed the just-written key"),
    )
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::HashSet;

    use parking_lot::Mutex;

    use super::*;
    use crate::fault::{FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use crate::memory::MemoryStore;
    use crate::{Capabilities, DelimitedList, GetOutcome, ListPage, ObjectMeta, PageToken};

    #[tokio::test]
    async fn conforming_backend_qualifies() {
        let store = MemoryStore::new();
        let report = run_conformance_suite(&store, "sys/qualify/test-1/").await;
        assert!(
            report.passed(),
            "expected every property to pass on the oracle, got failures: {:?}",
            report.failures().collect::<Vec<_>>()
        );
        assert_eq!(report.results.len(), 4);
    }

    /// Wraps `MemoryStore` and simulates eventually consistent listing: the
    /// call to `list`/`list_all` immediately following a key's `put` never
    /// includes it (the key becomes visible starting from the NEXT listing
    /// call instead). `put`, `get`, and `head` are untouched, so this isolates
    /// the list-after-write property alone -- conditional writes and
    /// read-after-write still pass on this store.
    struct WeakListStore {
        inner: MemoryStore,
        hidden_from_next_list: Mutex<HashSet<String>>,
    }

    impl WeakListStore {
        fn new() -> Self {
            WeakListStore {
                inner: MemoryStore::new(),
                hidden_from_next_list: Mutex::new(HashSet::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for WeakListStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<crate::PutOutcome, StoreError> {
            let outcome = self.inner.put(key, data, opts).await?;
            self.hidden_from_next_list.lock().insert(key.to_string());
            Ok(outcome)
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            let mut page_result = self.inner.list(prefix, page).await?;
            let mut hidden = self.hidden_from_next_list.lock();
            page_result.objects.retain(|meta| !hidden.remove(&meta.key));
            Ok(page_result)
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    #[tokio::test]
    async fn weak_list_backend_fails_qualification() {
        let store = WeakListStore::new();
        let report = run_conformance_suite(&store, "sys/qualify/test-2/").await;
        assert!(!report.passed());
        let failed: Vec<Property> = report.failures().map(|r| r.property).collect();
        assert_eq!(
            failed,
            vec![Property::ConsistentListAfterWrite],
            "only listing should be named; conditional writes and read-after-write are untouched"
        );
        let failure = report
            .results
            .iter()
            .find(|r| r.property == Property::ConsistentListAfterWrite)
            .expect("listing probe result present");
        assert!(failure.detail.contains("not strongly consistent"));
    }

    /// Wraps `MemoryStore` and drops every conditional-write precondition:
    /// every `put`, regardless of the requested `PutMode`, is applied as an
    /// unconditional overwrite. Models a backend that advertises S3
    /// compatibility but silently ignores `If-None-Match`/`If-Match`.
    struct NoCasStore {
        inner: MemoryStore,
    }

    impl NoCasStore {
        fn new() -> Self {
            NoCasStore {
                inner: MemoryStore::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for NoCasStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<crate::PutOutcome, StoreError> {
            self.inner
                .put(
                    key,
                    data,
                    PutOptions {
                        mode: PutMode::Overwrite,
                        checksum: opts.checksum,
                    },
                )
                .await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities {
                create_if_absent: false,
                cas_version: false,
                ..self.inner.capabilities()
            }
        }
    }

    #[tokio::test]
    async fn backend_without_conditional_writes_fails_qualification() {
        let store = NoCasStore::new();
        let report = run_conformance_suite(&store, "sys/qualify/test-3/").await;
        assert!(!report.passed());
        let failed: HashSet<&'static str> = report.failures().map(|r| r.property.name()).collect();
        assert!(failed.contains(Property::ConditionalWriteCreateIfAbsent.name()));
        assert!(failed.contains(Property::ConditionalWriteCasVersion.name()));
        // Listing and read-after-write are untouched by this backend.
        assert!(!failed.contains(Property::ConsistentListAfterWrite.name()));
        assert!(!failed.contains(Property::ConsistentReadAfterWrite.name()));
    }

    /// A transient fault on the very first probe call must surface as a
    /// named, typed [`ProbeResult`] failure, not a panic -- and the fault
    /// must actually have fired, not merely be configured (repo testing
    /// pattern: assert `FaultStore` counters).
    #[tokio::test]
    async fn transient_put_fault_surfaces_as_named_probe_failure() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Timeout)
                .with_key_contains("cas/create-if-absent")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        let report = run_conformance_suite(&store, "sys/qualify/test-4/").await;
        assert!(!report.passed());
        let failure = report
            .results
            .iter()
            .find(|r| r.property == Property::ConditionalWriteCreateIfAbsent)
            .expect("probe result present");
        assert!(!failure.passed);
        assert!(failure.detail.contains("timeout"));
        assert_eq!(store.fault_count(Op::Put, FaultKind::Timeout), 1);
    }
}
