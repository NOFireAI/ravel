//! The store cost of one [`SqlExecutor::execute_ddl`] call, split by phase
//! (issue #2374).
//!
//! DDL work is outside query-cost accounting (ADR-2040 grants-and-DDL-cost
//! amendment, 2026-10-01): it is never charged against a query budget or a
//! usage record. It is still store traffic, so [`DdlCost`] reports it, per
//! phase and per store operation, for the caller to export.
//!
//! The phases, in the order a `CREATE` runs them:
//!
//! - [`DdlPhase::Grant`]: the tenant's grants record read.
//! - [`DdlPhase::Probe`]: the bounded listing (or single-object HEAD) that
//!   finds one object under the `LOCATION`, and both qualification probes,
//!   on the external store and on Ravel's own store.
//! - [`DdlPhase::Snapshot`]: the file listing and footer reads of
//!   [`ravel_parquet::snapshot::snapshot_location`], taken from the
//!   [`PhaseAccounting`] it already records into.
//! - [`DdlPhase::Write`]: the manifest resolve and write, including the
//!   existence check a plain `CREATE` makes before reading any grant (the
//!   same resolve `ravel_pqtable::writer::apply` would otherwise make first).
//!
//! A `DROP TABLE` touches only [`DdlPhase::Write`]. Every store request the
//! statement issues is counted in exactly one phase.
//!
//! [`SqlExecutor::execute_ddl`]: crate::SqlExecutor::execute_ddl

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use ravel_object_store::instrument::{STORE_OP_COUNT, StoreOp};
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};
use ravel_query::PhaseAccounting;
use ravel_types::accounting::AccountedOp;

use crate::ddl::{DdlExecuteError, DdlOutcome};

/// Which bytes a [`DdlCost`] counts, in the words `/metrics` repeats for the
/// `ravel_sql_ddl_store_bytes_total` HELP text.
pub const DDL_COST_BYTES: &str = "Response-body bytes of completed GET requests as returned across the \
     object-store trait, undecoded (a ranged read counts the range returned); \
     request bodies, failed GETs, HEAD, LIST, PUT and DELETE count zero, and \
     retries below the trait are not counted";

/// One phase of a DDL statement's store traffic. See the module docs for
/// what each phase covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DdlPhase {
    Grant,
    Probe,
    Snapshot,
    Write,
}

/// Number of [`DdlPhase`] variants.
pub const DDL_PHASE_COUNT: usize = 4;

impl DdlPhase {
    /// Every variant, in the order a `CREATE` runs them (`index()` order).
    pub const ALL: [DdlPhase; DDL_PHASE_COUNT] = [
        DdlPhase::Grant,
        DdlPhase::Probe,
        DdlPhase::Snapshot,
        DdlPhase::Write,
    ];

    /// Dense array index, `< DDL_PHASE_COUNT` by construction.
    pub fn index(self) -> usize {
        match self {
            DdlPhase::Grant => 0,
            DdlPhase::Probe => 1,
            DdlPhase::Snapshot => 2,
            DdlPhase::Write => 3,
        }
    }

    /// The `phase` label value `/metrics` exports.
    pub fn name(self) -> &'static str {
        match self {
            DdlPhase::Grant => "grant",
            DdlPhase::Probe => "probe",
            DdlPhase::Snapshot => "snapshot",
            DdlPhase::Write => "write",
        }
    }
}

/// One phase's store requests, by operation, and its bytes.
///
/// A request is counted when it is issued, whether it then succeeds, fails,
/// or is abandoned at the statement deadline; it is one logical call at the
/// [`ObjectStoreBackend`] trait boundary, so a retry the backend makes below
/// that boundary is not a second request. Bytes are [`DDL_COST_BYTES`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DdlPhaseCost {
    requests: [u64; STORE_OP_COUNT],
    bytes: u64,
}

impl DdlPhaseCost {
    /// Requests of one operation kind.
    pub fn requests(&self, op: StoreOp) -> u64 {
        self.requests[op.index()]
    }

    /// Requests of every operation kind.
    pub fn total_requests(&self) -> u64 {
        self.requests.iter().sum()
    }

    /// Bytes, as defined by [`DDL_COST_BYTES`].
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// The store cost of one DDL statement, per [`DdlPhase`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DdlCost {
    phases: [DdlPhaseCost; DDL_PHASE_COUNT],
}

impl DdlCost {
    /// One phase's cost. A phase the statement never reached reads zero.
    pub fn phase(&self, phase: DdlPhase) -> &DdlPhaseCost {
        &self.phases[phase.index()]
    }

    /// Requests across every phase.
    pub fn total_requests(&self) -> u64 {
        self.phases.iter().map(DdlPhaseCost::total_requests).sum()
    }

    /// Bytes across every phase.
    pub fn total_bytes(&self) -> u64 {
        self.phases.iter().map(DdlPhaseCost::bytes).sum()
    }
}

/// What [`SqlExecutor::execute_ddl`] returns: the statement's result, and
/// the cost it accrued, which a failed statement reports too (whatever it
/// had issued before it failed or ran out of time).
///
/// [`SqlExecutor::execute_ddl`]: crate::SqlExecutor::execute_ddl
#[derive(Debug)]
pub struct DdlExecution {
    pub result: Result<DdlOutcome, DdlExecuteError>,
    pub cost: DdlCost,
}

#[derive(Debug, Default)]
struct PhaseCounters {
    requests: [AtomicU64; STORE_OP_COUNT],
    bytes: AtomicU64,
}

impl PhaseCounters {
    fn request(&self, op: StoreOp) {
        self.requests[op.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn add_bytes(&self, bytes: usize) {
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> DdlPhaseCost {
        let mut cost = DdlPhaseCost {
            bytes: self.bytes.load(Ordering::Relaxed),
            ..DdlPhaseCost::default()
        };
        for op in StoreOp::ALL {
            cost.requests[op.index()] = self.requests[op.index()].load(Ordering::Relaxed);
        }
        cost
    }
}

/// The live counters behind one statement's [`DdlCost`]. Created before the
/// statement's deadline timer starts, so a statement that runs out of time
/// still reports what it issued.
#[derive(Debug, Default)]
pub(crate) struct DdlCostRecorder {
    grant: Arc<PhaseCounters>,
    probe: Arc<PhaseCounters>,
    write: Arc<PhaseCounters>,
    snapshot: PhaseAccounting,
}

impl DdlCostRecorder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// `store`, counting into [`DdlPhase::Grant`].
    pub(crate) fn grant_store(&self, store: &Arc<dyn ObjectStoreBackend>) -> CostedStore {
        CostedStore::new(store, &self.grant)
    }

    /// `store`, counting into [`DdlPhase::Probe`].
    pub(crate) fn probe_store(&self, store: &Arc<dyn ObjectStoreBackend>) -> CostedStore {
        CostedStore::new(store, &self.probe)
    }

    /// `store`, counting into [`DdlPhase::Write`].
    pub(crate) fn write_store(&self, store: &Arc<dyn ObjectStoreBackend>) -> CostedStore {
        CostedStore::new(store, &self.write)
    }

    /// The accounting `snapshot_location` records into; read back as
    /// [`DdlPhase::Snapshot`]. Its store is passed unwrapped, so the snapshot
    /// step's requests are counted here and only here.
    pub(crate) fn snapshot_accounting(&self) -> &PhaseAccounting {
        &self.snapshot
    }

    pub(crate) fn cost(&self) -> DdlCost {
        let mut cost = DdlCost::default();
        cost.phases[DdlPhase::Grant.index()] = self.grant.snapshot();
        cost.phases[DdlPhase::Probe.index()] = self.probe.snapshot();
        cost.phases[DdlPhase::Write.index()] = self.write.snapshot();
        let pooled = self.snapshot.pooled_snapshot();
        let snapshot = &mut cost.phases[DdlPhase::Snapshot.index()];
        for (accounted, op) in [
            (AccountedOp::Get, StoreOp::Get),
            (AccountedOp::List, StoreOp::List),
            (AccountedOp::Head, StoreOp::Head),
        ] {
            snapshot.requests[op.index()] = pooled.s3_requests(accounted);
        }
        snapshot.bytes = pooled.total_s3_bytes();
        cost
    }
}

/// A store that counts every request into one phase's counters.
///
/// `snapshot_location` records its own requests into a [`PhaseAccounting`];
/// the grants read, the probes, and the manifest resolve and write record
/// nothing, so their stores are wrapped in this instead. Each request is
/// counted before it is awaited, the same point `snapshot_location` records
/// at, and the operation kinds match
/// [`ravel_object_store::instrument::InstrumentedStore`]'s: a pinned or
/// pin-reporting read is a GET, `pin_of` is a HEAD, `list_after` is a LIST.
pub(crate) struct CostedStore {
    inner: Arc<dyn ObjectStoreBackend>,
    counters: Arc<PhaseCounters>,
}

impl CostedStore {
    fn new(inner: &Arc<dyn ObjectStoreBackend>, counters: &Arc<PhaseCounters>) -> Self {
        Self {
            inner: Arc::clone(inner),
            counters: Arc::clone(counters),
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for CostedStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.counters.request(StoreOp::Put);
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.counters.request(StoreOp::Get);
        let outcome = self.inner.get(key, range).await?;
        self.counters.add_bytes(outcome.data.len());
        Ok(outcome)
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.counters.request(StoreOp::Get);
        let read = self.inner.get_pinned(key, range, pin).await?;
        self.counters.add_bytes(read.outcome.data.len());
        Ok(read)
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.counters.request(StoreOp::Get);
        let read = self.inner.get_with_pin(key, range).await?;
        self.counters.add_bytes(read.outcome.data.len());
        Ok(read)
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.counters.request(StoreOp::Head);
        self.inner.pin_of(key).await
    }

    // `put_multipart` keeps the trait's refusing default and `capabilities`
    // below reports `multipart: false` to match: a multipart upload is a
    // handle issuing an open-ended number of requests, which this wrapper
    // would otherwise let through uncounted. No DDL step uploads multipart.

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.counters.request(StoreOp::Head);
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.counters.request(StoreOp::List);
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.counters.request(StoreOp::List);
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.counters.request(StoreOp::ListDelimited);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.counters.request(StoreOp::Delete);
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::instrument::InstrumentedStore;
    use ravel_object_store::memory::MemoryStore;

    #[tokio::test]
    async fn costed_store_counts_every_request_and_only_completed_get_bytes() {
        let counted = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let inner: Arc<dyn ObjectStoreBackend> = counted.clone();
        let recorder = DdlCostRecorder::new();
        let store = recorder.probe_store(&inner);

        store
            .put("k", Bytes::from_static(b"hello"), PutOptions::default())
            .await
            .expect("put");
        store.get("k", GetRange::Full).await.expect("get");
        store
            .get("k", GetRange::Range(0, 2))
            .await
            .expect("ranged get");
        assert!(matches!(
            store.get("absent", GetRange::Full).await,
            Err(StoreError::NotFound)
        ));
        let (_, pin) = store.pin_of("k").await.expect("pin_of");
        store
            .get_pinned("k", GetRange::Full, &pin)
            .await
            .expect("pinned");
        store
            .get_with_pin("k", GetRange::Full)
            .await
            .expect("with pin");
        store.head("k").await.expect("head");
        store.list("", None).await.expect("list");
        store.list_after("", Some("a"), None).await.expect("after");
        store.list_delimited("").await.expect("delimited");
        store.delete("k").await.expect("delete");

        let cost = recorder.cost();
        let probe = cost.phase(DdlPhase::Probe);
        let metrics = counted.metrics().snapshot();
        for op in StoreOp::ALL {
            assert_eq!(probe.requests(op), metrics.op(op).calls, "{op:?}");
        }
        assert_eq!(probe.requests(StoreOp::Get), 5);
        assert_eq!(probe.requests(StoreOp::Head), 2);
        assert_eq!(probe.requests(StoreOp::List), 2);
        // Three full reads of five bytes and one two-byte range; the failed
        // read and the PUT payload count nothing.
        assert_eq!(probe.bytes(), 17);
        assert_eq!(cost.total_requests(), probe.total_requests());
        for phase in [DdlPhase::Grant, DdlPhase::Snapshot, DdlPhase::Write] {
            assert_eq!(*cost.phase(phase), DdlPhaseCost::default(), "{phase:?}");
        }
        assert!(!store.capabilities().multipart);
    }

    #[test]
    fn snapshot_phase_reads_the_phase_accounting() {
        let recorder = DdlCostRecorder::new();
        let accounting = recorder.snapshot_accounting();
        accounting.resolve().record_s3_request(AccountedOp::List);
        accounting.resolve().record_s3_request(AccountedOp::Head);
        accounting.probe().record_s3_request(AccountedOp::Get);
        accounting.probe().add_s3_bytes(AccountedOp::Get, 9);

        let cost = recorder.cost();
        let snapshot = cost.phase(DdlPhase::Snapshot);
        assert_eq!(snapshot.requests(StoreOp::List), 1);
        assert_eq!(snapshot.requests(StoreOp::Head), 1);
        assert_eq!(snapshot.requests(StoreOp::Get), 1);
        assert_eq!(snapshot.total_requests(), 3);
        assert_eq!(snapshot.bytes(), 9);
        assert_eq!(cost.total_bytes(), 9);
    }
}
