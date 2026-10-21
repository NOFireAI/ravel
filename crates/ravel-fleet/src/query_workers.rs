//! Query-worker registration and liveness for the ADR-0071 distributed read
//! fan-out.
//!
//! A query-role process that participates in fan-out registers itself the same
//! way a maintain worker does (ADR-0065 decision 1, [`crate::worker_set`]): it
//! writes, every heartbeat interval `H`, a [`QueryWorkerRecord`] to its own
//! key (by convention; the query role's grant covers the whole prefix, see
//! [`QueryWorkers::live_set`]):
//!
//! ```text
//! sys/query/workers/<process_id>
//! ```
//!
//! The write is [`PutMode::Overwrite`] -- single writer per key, no CAS, no
//! contention, exactly like the maintain heartbeat. On the same cadence a
//! reader lists `sys/query/workers/` and GETs siblings; the **live set** is
//! itself plus every sibling whose heartbeat stamp is within the staleness
//! window (`liveness_factor * H`, default `3 * H`) of the reader's own clock,
//! in either direction. The staleness predicate is `worker_set::is_stale`
//! reused verbatim (ADR-0065's `3 * H` rule, symmetric so a clock-skewed or
//! stuck future-dated record drops just like a stale past-dated one).
//!
//! Unlike the maintain heartbeat (`ravel.sys.v1.WorkerHeartbeat`, a protobuf
//! `sys/` object), the query-worker record is a JSON control-plane payload: it
//! is a transient membership advertisement, not a persistent stored format, so
//! it uses the same serde JSON encoding the rest of the control plane uses.
//!
//! # Record authentication (ADR-1918)
//!
//! The heartbeat object is `{"mac": "<64 hex>", ...record fields}`: the
//! [`QueryWorkerRecord`] fields plus a keyed BLAKE3 MAC over every one of
//! them, under a record key derived from the fragment key file
//! ([`RecordKeys`]). [`QueryWorkers::live_set`] admits a sibling only when that
//! MAC verifies under one of its own record keys. A principal that can write
//! the prefix but holds no fragment key file therefore cannot add a member to
//! the live set, or change the endpoint an existing member's record names.
//!
//! It can still evict one: overwriting a worker's record with garbage, or
//! replaying an older record that worker signed (a drained one included),
//! drops the worker from every reader's live set until its next heartbeat
//! lands. A holder of the fragment key file can sign any record it likes; it
//! is a cluster member (ADR-1689's trust model).
//!
//! `started_unix_ns` is the liveness timestamp: the writer stamps it with the
//! current clock on every heartbeat (mirroring the maintain heartbeat's
//! `heartbeat_unix_ns`), so a process that keeps beating stays live and one
//! that stops drops out after the staleness window. The field name follows the
//! frozen record shape #863 pins for the wiring tickets (#864, #865).
//!
//! `ravel-server`'s distributed query start path builds the process's one
//! [`QueryWorkers`] and runs its heartbeat loop.
//!
//! # Bounding the prefix (issue #1761)
//!
//! The query role deletes nothing (ADR-0055 section 1: `deploy/iam/query.json`
//! grants no `s3:DeleteObject`), so no query-side path removes a key. A
//! graceful drain overwrites the process's own record with
//! [`DRAINED_STAMP_NS`] instead ([`QueryWorkers::mark_drained`]), which every
//! reader skips at once, and a worker lost to a panic, a kill, an
//! out-of-memory event or a node loss leaves its last record behind. The two
//! bounds [`crate::worker_set`] applies to the maintain prefix apply here over
//! the same shared predicates:
//!
//! - [`live_set`] skips the GET for a key the LIST result already shows as
//!   older than the liveness window (`worker_set::mtime_stale`). A record's
//!   `started_unix_ns` is stamped no later than the write that set the
//!   modification time, so a key that looks past the window by its modification
//!   time can only hold a stamp at least as old.
//! - A key past the *reap horizon* (`worker_set::reap_horizon_ns`: the liveness
//!   window widened by `worker_set::REAP_WINDOW_FACTOR`) is deleted, which
//!   bounds the LIST itself. The extra width is the clock-skew margin between
//!   the object store's clock (which sets the modification time) and the
//!   reader's. The deleter is the maintain role, which holds the delete grant
//!   on this prefix: the maintain process that owns a fixed rendezvous unit
//!   (one per view of the maintain membership) calls
//!   [`reap_dead_query_workers`] on its tick. The reap judges only the LIST
//!   metadata, so it reads no record. Reaping is idempotent and costs a
//!   live-but-skewed worker at most one heartbeat interval of invisibility,
//!   since it rewrites its key every `H`.
//!
//! A backend reporting no usable modification time (`<= 0`) gets neither
//! treatment: its keys are read as before and never reaped. The same holds for
//! a modification time in the future, which means the store's clock runs ahead
//! of this reader's and is a reason to read the key rather than to drop it.
//!
//! [`live_set`]: QueryWorkers::live_set

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_object_store::{
    GetRange, ObjectMeta, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::worker_set::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LIVENESS_FACTOR, is_stale, mtime_stale, reap_horizon_ns,
};

/// The prefix every query-worker's heartbeat key lives under (ADR-0071): a new
/// additive control-plane prefix under `sys/query/`, sibling to the maintain
/// role's `sys/maintain/workers/`. Fixed verbatim; #864 and #865 depend on it.
pub const QUERY_WORKERS_PREFIX: &str = "sys/query/workers/";

/// The heartbeat stamp a draining worker writes over its own record
/// ([`QueryWorkers::mark_drained`]). `worker_set::is_stale` saturates, so this
/// stamp is past the liveness window for every reader clock and every window
/// short of `i64::MAX` nanoseconds.
pub const DRAINED_STAMP_NS: i64 = i64::MIN;

/// The liveness window, in nanoseconds, for a heartbeat interval and liveness
/// factor: `liveness_factor * H`, the factor clamped to at least 1. The one
/// computation [`QueryWorkers`] judges siblings by and [`reap_dead_query_workers`]
/// derives its reap horizon from.
pub fn liveness_window_ns(heartbeat_interval: Duration, liveness_factor: u32) -> i64 {
    let h = i64::try_from(heartbeat_interval.as_nanos()).unwrap_or(i64::MAX);
    h.saturating_mul(i64::from(liveness_factor.max(1)))
}

/// The liveness window of a query worker built by [`QueryWorkers::with_defaults`],
/// which is how every query-role process is built. The maintain role reaps
/// against it, since it runs no query worker of its own to ask.
pub fn default_liveness_window_ns() -> i64 {
    liveness_window_ns(DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LIVENESS_FACTOR)
}

/// The heartbeat key one query-worker process writes its own record to.
pub fn query_worker_key(process_id: &str) -> String {
    format!("{QUERY_WORKERS_PREFIX}{process_id}")
}

/// The process id a heartbeat key names, or `None` if the key is not a
/// well-formed `sys/query/workers/<uuid>` key. Worker identity is the key, not
/// the record body (mirrors [`crate::worker_set`]'s `process_id_of`), so a
/// record whose body disagrees with its key cannot claim another worker's
/// identity. This check alone does not stop a new identity, since the shipped
/// query-role IAM grant (`deploy/iam/query.json`) allows `PutObject` across the
/// whole `sys/query/workers/` prefix; the record MAC does (see the
/// [module docs](self)).
/// `list_all` yields the prefix itself and any unexpected nested key under it;
/// both parse to `None` and are skipped.
fn process_id_of(key: &str) -> Option<Uuid> {
    let raw = key.strip_prefix(QUERY_WORKERS_PREFIX)?;
    Uuid::parse_str(raw).ok()
}

/// One query-worker process's self-owned membership record (ADR-0071),
/// advertised at `sys/query/workers/<process_id>` with [`PutMode::Overwrite`].
///
/// A transient wire/control-plane advertisement, not a persistent stored
/// format: encoded as JSON (see the [module docs](self)). The field set is
/// fixed by #863; #864 and #865 read these names verbatim.
///
/// - `process_id`: the writing process's UUID as a string, matching the
///   `<process_id>` in the object key.
/// - `fragment_endpoint`: the address of this worker's dedicated TLS fragment
///   listener (host:port). Both distributed lanes dial it over mutual TLS: the
///   PromQL lane for `queryfrag` `SeriesFetch` slices, and the SQL lane for
///   Flight SQL slice `DoGet` (ADR-1689 decision 1).
/// - `protocol_version`: the `queryfrag` protocol version this worker speaks,
///   for the ADR-0071 version-skew fallback.
/// - `started_unix_ns`: the liveness timestamp (see the [module docs](self)),
///   re-stamped with the current clock on every heartbeat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryWorkerRecord {
    pub process_id: String,
    pub fragment_endpoint: String,
    pub protocol_version: u32,
    pub started_unix_ns: i64,
}

impl QueryWorkerRecord {
    /// JSON-encode this record for its heartbeat object.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Decode a heartbeat object's JSON bytes back into a record. A corrupt or
    /// future-shaped payload is a typed error the reader treats as absent.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// The `blake3::derive_key` context the record keys are derived under
/// (ADR-1918 decision 2). Fragment capabilities are MACed under the raw file
/// key and SQL tickets under keys derived with their own contexts, so a record
/// key is never a key either of them uses.
pub const RECORD_KEY_CONTEXT: &str = "ravel query worker record v1";

/// The largest heartbeat object [`QueryWorkers::live_set`] reads (ADR-1918
/// decision 5). The reader asks for one byte more, so a longer object is
/// recognised as oversize without being decoded.
pub const MAX_RECORD_BYTES: u64 = 4096;

/// The record keys of one process (ADR-1918 decision 2): one per
/// `--fragment-key-file` key, each derived as
/// `blake3::derive_key(RECORD_KEY_CONTEXT, file_key)`. The first mints; a
/// record verifies under any of them. Never empty.
#[derive(Clone)]
pub struct RecordKeys {
    keys: Vec<[u8; 32]>,
}

/// [`RecordKeys::from_file_keys`] was given no key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoRecordKeys;

impl std::fmt::Display for NoRecordKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("query-worker record keys need at least one fragment key")
    }
}

impl std::error::Error for NoRecordKeys {}

impl RecordKeys {
    /// Derive one record key per fragment file key, in order.
    pub fn from_file_keys(file_keys: &[[u8; 32]]) -> Result<Self, NoRecordKeys> {
        if file_keys.is_empty() {
            return Err(NoRecordKeys);
        }
        let keys = file_keys
            .iter()
            .map(|file_key| blake3::derive_key(RECORD_KEY_CONTEXT, file_key))
            .collect();
        Ok(RecordKeys { keys })
    }

    /// How many keys verify.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Always `false`: a `RecordKeys` cannot be built empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn mac(&self, record: &QueryWorkerRecord) -> blake3::Hash {
        // `from_file_keys` refuses an empty list, so `keys[0]` always exists;
        // the fallback keeps the type total without a panic path.
        let mint = self.keys.first().copied().unwrap_or([0; 32]);
        blake3::keyed_hash(&mint, &record_claim(record))
    }

    /// Whether `presented` is the MAC of `record` under any key. Each
    /// comparison is `blake3::Hash`'s constant-time equality.
    fn verifies(&self, record: &QueryWorkerRecord, presented: &blake3::Hash) -> bool {
        let claim = record_claim(record);
        self.keys
            .iter()
            .any(|key| blake3::keyed_hash(key, &claim) == *presented)
    }
}

impl std::fmt::Debug for RecordKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordKeys")
            .field("keys", &self.keys.len())
            .finish()
    }
}

/// The bytes a record MAC covers (ADR-1918 decision 1): `process_id` and
/// `fragment_endpoint`, each prefixed with its length as a big-endian u64,
/// then `protocol_version` (u32 BE) and `started_unix_ns` (i64 BE).
fn record_claim(record: &QueryWorkerRecord) -> Vec<u8> {
    let mut claim = Vec::with_capacity(
        8 + record.process_id.len() + 8 + record.fragment_endpoint.len() + 4 + 8,
    );
    for field in [&record.process_id, &record.fragment_endpoint] {
        let len = u64::try_from(field.len()).unwrap_or(u64::MAX);
        claim.extend_from_slice(&len.to_be_bytes());
        claim.extend_from_slice(field.as_bytes());
    }
    claim.extend_from_slice(&record.protocol_version.to_be_bytes());
    claim.extend_from_slice(&record.started_unix_ns.to_be_bytes());
    claim
}

/// The heartbeat object as stored: the record's fields plus its MAC.
#[derive(Serialize, Deserialize)]
struct SignedRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mac: Option<String>,
    #[serde(flatten)]
    record: QueryWorkerRecord,
}

/// Why [`QueryWorkers::live_set`] skipped a sibling's record (ADR-1918
/// decision 4). Rendered as the closed `reason` label of
/// `ravel_query_worker_record_rejects_total`. A key that is not a UUID, a
/// record skipped before the GET because its LIST modification time is outside
/// the liveness window, and a verified record stamped [`DRAINED_STAMP_NS`], are
/// not rejects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordReject {
    /// The object decoded but carried no `mac`.
    MissingMac,
    /// The `mac` is not 64 hex characters or verifies under no record key.
    BadMac,
    /// The object is longer than [`MAX_RECORD_BYTES`]; it was not decoded.
    Oversize,
    /// The object is empty or does not decode, or its `process_id` disagrees
    /// with its key.
    Malformed,
    /// The record verified, but its stamp is outside the liveness window
    /// although its modification time is inside it: a replayed older heartbeat
    /// or a clock skewed past the window.
    StaleStamp,
}

impl RecordReject {
    /// Every reason, in the fixed order the counters are indexed by.
    pub const ALL: [RecordReject; 5] = [
        RecordReject::MissingMac,
        RecordReject::BadMac,
        RecordReject::Oversize,
        RecordReject::Malformed,
        RecordReject::StaleStamp,
    ];

    /// The stable `reason` metric label value.
    pub fn reason(self) -> &'static str {
        match self {
            RecordReject::MissingMac => "missing_mac",
            RecordReject::BadMac => "bad_mac",
            RecordReject::Oversize => "oversize",
            RecordReject::Malformed => "malformed",
            RecordReject::StaleStamp => "stale_stamp",
        }
    }

    fn index(self) -> usize {
        match self {
            RecordReject::MissingMac => 0,
            RecordReject::BadMac => 1,
            RecordReject::Oversize => 2,
            RecordReject::Malformed => 3,
            RecordReject::StaleStamp => 4,
        }
    }
}

/// Cumulative [`RecordReject`] counts of one process's live-set reads.
#[derive(Debug, Default)]
pub struct RecordRejectMetrics {
    rejects: [AtomicU64; RecordReject::ALL.len()],
}

impl RecordRejectMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records rejected for `reason` so far.
    pub fn get(&self, reason: RecordReject) -> u64 {
        self.rejects[reason.index()].load(Ordering::Relaxed)
    }

    /// Every reason with its count, in [`RecordReject::ALL`] order.
    pub fn by_reason(&self) -> [(RecordReject, u64); RecordReject::ALL.len()] {
        RecordReject::ALL.map(|reason| (reason, self.get(reason)))
    }

    fn record(&self, reason: RecordReject) {
        self.rejects[reason.index()].fetch_add(1, Ordering::Relaxed);
    }
}

/// One query-role process's membership handle (ADR-0071), the query-side analog
/// of [`crate::worker_set::WorkerSet`]: a stable `process_id`, the endpoint and
/// protocol version it advertises, and the heartbeat/liveness timing. Held once
/// for the whole process.
#[derive(Debug, Clone)]
pub struct QueryWorkers {
    process_id: Uuid,
    fragment_endpoint: String,
    protocol_version: u32,
    heartbeat_interval: Duration,
    liveness_factor: u32,
    keys: RecordKeys,
    rejects: Arc<RecordRejectMetrics>,
}

impl QueryWorkers {
    /// A query worker with an explicit timing configuration. `liveness_factor`
    /// is clamped to at least 1, matching [`crate::worker_set::WorkerSet::new`].
    ///
    /// `fragment_endpoint` is the dedicated fragment listener address both
    /// distributed lanes dial (see [`QueryWorkerRecord`]). `keys` signs this
    /// process's record and verifies every sibling's (ADR-1918).
    pub fn new(
        fragment_endpoint: impl Into<String>,
        protocol_version: u32,
        keys: RecordKeys,
        heartbeat_interval: Duration,
        liveness_factor: u32,
    ) -> Self {
        QueryWorkers {
            process_id: Uuid::new_v4(),
            fragment_endpoint: fragment_endpoint.into(),
            protocol_version,
            heartbeat_interval,
            liveness_factor: liveness_factor.max(1),
            keys,
            rejects: Arc::new(RecordRejectMetrics::new()),
        }
    }

    /// A query worker with the ADR-0065 heartbeat defaults (`H = 60s`, `3 * H`
    /// liveness window) reused for the query role.
    pub fn with_defaults(
        fragment_endpoint: impl Into<String>,
        protocol_version: u32,
        keys: RecordKeys,
    ) -> Self {
        Self::new(
            fragment_endpoint,
            protocol_version,
            keys,
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_LIVENESS_FACTOR,
        )
    }

    /// Count [`live_set`](Self::live_set) rejects into `rejects` instead of
    /// this handle's own counters, so a caller can render them with its other
    /// metrics.
    pub fn with_record_rejects(mut self, rejects: Arc<RecordRejectMetrics>) -> Self {
        self.rejects = rejects;
        self
    }

    /// The sibling records [`live_set`](Self::live_set) has rejected, by
    /// reason.
    pub fn record_rejects(&self) -> &Arc<RecordRejectMetrics> {
        &self.rejects
    }

    /// This process's stable membership id. Names the one heartbeat key this
    /// process owns.
    pub fn process_id(&self) -> Uuid {
        self.process_id
    }

    /// The dedicated fragment listener endpoint this worker advertises (both
    /// distributed lanes).
    pub fn fragment_endpoint(&self) -> &str {
        &self.fragment_endpoint
    }

    /// The `queryfrag` protocol version this worker speaks.
    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// The heartbeat interval `H` this worker writes on.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// The staleness window in nanoseconds: `liveness_factor * H` (identical
    /// computation to [`crate::worker_set::WorkerSet`]).
    fn liveness_window_ns(&self) -> i64 {
        liveness_window_ns(self.heartbeat_interval, self.liveness_factor)
    }

    /// The record this process advertises at `now_ns` (`started_unix_ns` is the
    /// heartbeat stamp; see the [module docs](self)).
    fn record_at(&self, now_ns: i64) -> QueryWorkerRecord {
        QueryWorkerRecord {
            process_id: self.process_id.to_string(),
            fragment_endpoint: self.fragment_endpoint.clone(),
            protocol_version: self.protocol_version,
            started_unix_ns: now_ns,
        }
    }

    /// Write this process's heartbeat (`Overwrite`: single writer per key, no
    /// CAS). A failed write self-corrects on the next interval. `now_ns` is the
    /// injected clock reading stamped as `started_unix_ns` (the liveness stamp).
    pub async fn write_heartbeat(
        &self,
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
    ) -> Result<(), StoreError> {
        self.put_record(store, now_ns).await
    }

    /// Overwrite this process's own record with [`DRAINED_STAMP_NS`] as its
    /// heartbeat stamp. Called on graceful shutdown so a draining query worker
    /// drops out of every sibling coordinator's live set on that sibling's next
    /// read, rather than lingering until its last stamp ages past the `3 * H`
    /// staleness window. A PUT, not a delete: the query role holds
    /// `PutObject` on this prefix and no delete (ADR-0055 section 1). The key
    /// itself stays until the maintain role reaps it past the reap horizon.
    pub async fn mark_drained(&self, store: &dyn ObjectStoreBackend) -> Result<(), StoreError> {
        self.put_record(store, DRAINED_STAMP_NS).await
    }

    async fn put_record(
        &self,
        store: &dyn ObjectStoreBackend,
        stamp_ns: i64,
    ) -> Result<(), StoreError> {
        let record = self.record_at(stamp_ns);
        let key = query_worker_key(&record.process_id);
        let signed = SignedRecord {
            mac: Some(self.keys.mac(&record).to_hex().to_string()),
            record,
        };
        let bytes = serde_json::to_vec(&signed)
            .map_err(|e| StoreError::Permanent(format!("encode query worker heartbeat: {e}")))?;
        store
            .put(
                &key,
                bytes.into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await?;
        Ok(())
    }

    /// Compute the live query-worker set (ADR-0071): this process plus every
    /// non-stale sibling under `sys/query/workers/`, applying the exact
    /// `worker_set::is_stale` rule (`3 * H`, symmetric) to each record's
    /// heartbeat stamp. Self is always included (stamped `now_ns`), so a process
    /// never disowns itself. The returned set is sorted by `process_id` and
    /// deduplicated for a deterministic order.
    ///
    /// Worker identity is derived from the object key, never trusted from the
    /// record body (mirrors [`crate::worker_set::WorkerSet::live_set`]): a key
    /// that is not a well-formed `sys/query/workers/<uuid>` is skipped, and a
    /// record whose body `process_id` disagrees with the id its key names is
    /// skipped as malformed, under either identity.
    ///
    /// A sibling is admitted only when its record carries a MAC that verifies
    /// under one of this process's [`RecordKeys`] (ADR-1918), compared in
    /// constant time. The read is a range read of [`MAX_RECORD_BYTES`] plus
    /// one byte, so a longer object is rejected as oversize without being
    /// decoded. Each rejected record is skipped (treated as absent) and counted
    /// by [`RecordReject`] reason in [`record_rejects`](Self::record_rejects),
    /// under the first check it fails. Three skips are not counted: a key that
    /// is not a UUID, a key whose LIST modification time is outside the
    /// liveness window (never fetched), and a verified record stamped
    /// [`DRAINED_STAMP_NS`] (a graceful drain).
    /// A verified record with any other stamp outside the window is counted as
    /// [`RecordReject::StaleStamp`]. Only a failed
    /// LIST or GET is an `Err`, which the caller treats fail-open, never
    /// freezing fan-out on a transient read fault.
    ///
    /// A key the LIST result already shows as past the liveness window costs no
    /// GET at all (issue #1761; see the [module docs](self)), so the read cost
    /// tracks the live fleet rather than every query worker that ever ran.
    pub async fn live_set(
        &self,
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
    ) -> Result<Vec<QueryWorkerRecord>, StoreError> {
        let window = self.liveness_window_ns();
        let objects = list_all(store, QUERY_WORKERS_PREFIX).await?;
        let mut live = vec![self.record_at(now_ns)];
        for meta in objects {
            let Some(pid) = process_id_of(&meta.key) else {
                tracing::debug!(
                    key = %meta.key,
                    "query_workers: skipping a key that is not sys/query/workers/<uuid>"
                );
                continue;
            };
            if pid == self.process_id {
                continue;
            }
            if mtime_stale(now_ns, meta.last_modified_unix_ms, window) {
                // Already past the liveness window by the LIST's own metadata:
                // the body could only be older still, so it costs no GET
                // (issue #1761).
                continue;
            }
            let record = match self.read_sibling(store, &meta, pid).await? {
                Ok(record) => record,
                Err(reason) => {
                    tracing::debug!(
                        key = %meta.key,
                        reason = reason.reason(),
                        "query_workers: rejecting a sibling record"
                    );
                    self.rejects.record(reason);
                    continue;
                }
            };
            if record.started_unix_ns == DRAINED_STAMP_NS {
                // A graceful drain writes exactly this signed record, so it is
                // a departure rather than a reject.
                continue;
            }
            if is_stale(now_ns, record.started_unix_ns, window) {
                tracing::debug!(
                    key = %meta.key,
                    reason = RecordReject::StaleStamp.reason(),
                    "query_workers: rejecting a sibling record"
                );
                self.rejects.record(RecordReject::StaleStamp);
                continue;
            }
            live.push(record);
        }
        live.sort_unstable_by(|a, b| a.process_id.cmp(&b.process_id));
        live.dedup_by(|a, b| a.process_id == b.process_id);
        Ok(live)
    }

    /// Read and authenticate the record at `meta`, whose key names `pid`. The
    /// outer `Err` is a store failure; the inner one is the reject reason.
    async fn read_sibling(
        &self,
        store: &dyn ObjectStoreBackend,
        meta: &ObjectMeta,
        pid: Uuid,
    ) -> Result<Result<QueryWorkerRecord, RecordReject>, StoreError> {
        if meta.size == 0 {
            return Ok(Err(RecordReject::Malformed));
        }
        let got = match store
            .get(&meta.key, GetRange::Range(0, MAX_RECORD_BYTES + 1))
            .await
        {
            Ok(got) => got,
            // The object shrank to nothing between the LIST and the GET.
            Err(StoreError::InvalidRange(_)) => return Ok(Err(RecordReject::Malformed)),
            Err(err) => return Err(err),
        };
        let body = got.data.as_ref();
        if u64::try_from(body.len()).unwrap_or(u64::MAX) > MAX_RECORD_BYTES {
            return Ok(Err(RecordReject::Oversize));
        }
        let Ok(signed) = serde_json::from_slice::<SignedRecord>(body) else {
            return Ok(Err(RecordReject::Malformed));
        };
        // The body must name the process id its key does; trust the key.
        if signed.record.process_id != pid.to_string() {
            return Ok(Err(RecordReject::Malformed));
        }
        let Some(mac) = signed.mac else {
            return Ok(Err(RecordReject::MissingMac));
        };
        let Ok(presented) = blake3::Hash::from_hex(mac.as_bytes()) else {
            return Ok(Err(RecordReject::BadMac));
        };
        if !self.keys.verifies(&signed.record, &presented) {
            return Ok(Err(RecordReject::BadMac));
        }
        Ok(Ok(signed.record))
    }
}

/// What one reap pass over `sys/query/workers/` did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReapPass {
    /// Keys deleted.
    pub reaped: u64,
    /// Whether the store refused a delete as [`StoreError::AccessDenied`],
    /// which ended the pass.
    pub access_denied: bool,
}

/// Delete `keys`, each a dead query-worker record, and report what happened.
///
/// `delete` is idempotent, so two passes over the same dead key are not a race.
/// A transient failure on one key is logged per key and retried by the next
/// pass, since a key that outlives one tick costs one listing entry and nothing
/// else. An [`StoreError::AccessDenied`] is not per key: it means the caller's
/// credential lacks the delete grant on the whole prefix, so every remaining
/// delete would be refused the same way. The pass logs it once at error, with
/// the prefix and the number of keys left undeleted, and issues no further
/// delete.
pub async fn reap_keys(store: &dyn ObjectStoreBackend, keys: &[String]) -> ReapPass {
    let mut pass = ReapPass::default();
    for (index, key) in keys.iter().enumerate() {
        match store.delete(key).await {
            Ok(()) => pass.reaped += 1,
            Err(StoreError::AccessDenied(reason)) => {
                tracing::error!(
                    prefix = QUERY_WORKERS_PREFIX,
                    undeleted = keys.len().saturating_sub(index),
                    error = %reason,
                    "query_workers: the store denied deleting a dead query-worker record; \
                     this reap pass issues no further delete, and the prefix grows until the \
                     reaping credential is granted s3:DeleteObject on it"
                );
                pass.access_denied = true;
                break;
            }
            Err(err) => tracing::warn!(
                key = %key,
                error = %err,
                "query_workers: reaping a dead worker record failed; retried next tick"
            ),
        }
    }
    pass
}

/// Delete every record under `sys/query/workers/` past the reap horizon of
/// `liveness_window_ns` (issue #1761), judged from the modification time the
/// LIST result already carries. Costs one listing and reads no record.
///
/// The maintain role is the caller: it holds the delete grant on this prefix
/// and the query role does not (ADR-0055 section 1). Pass
/// [`default_liveness_window_ns`] unless the query workers were built with a
/// non-default timing.
///
/// Never deletes a key that does not parse as `sys/query/workers/<uuid>`
/// (something else's key under this prefix is not this function's to reap),
/// and never deletes a key whose modification time the backend reports as
/// unknown or as being in the future. The horizon is `reap_horizon_ns` of the
/// liveness window, so a worker whose record merely aged out of the live set
/// keeps a full extra window of grace against clock skew before it is reaped.
///
/// A failed LIST is an `Err` the caller can treat fail-open; the deletes go
/// through [`reap_keys`].
pub async fn reap_dead_query_workers(
    store: &dyn ObjectStoreBackend,
    now_ns: i64,
    liveness_window_ns: i64,
) -> Result<ReapPass, StoreError> {
    let horizon = reap_horizon_ns(liveness_window_ns);
    let reapable: Vec<String> = list_all(store, QUERY_WORKERS_PREFIX)
        .await?
        .into_iter()
        .filter(|meta| {
            process_id_of(&meta.key).is_some()
                && mtime_stale(now_ns, meta.last_modified_unix_ms, horizon)
        })
        .map(|meta| meta.key)
        .collect();
    Ok(reap_keys(store, &reapable).await)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    const H: Duration = Duration::from_secs(60);
    const H_NS: i64 = 60 * 1_000_000_000;
    /// `H` in milliseconds. The default liveness window is `3 * H_MS`
    /// (`DEFAULT_LIVENESS_FACTOR` is 3), which is how the tests spell it.
    const H_MS: u64 = 60 * 1_000;

    fn keys() -> RecordKeys {
        RecordKeys::from_file_keys(&[[7; 32]]).expect("one key")
    }

    fn worker() -> QueryWorkers {
        QueryWorkers::new("10.0.0.1:9443", 1, keys(), H, DEFAULT_LIVENESS_FACTOR)
    }

    /// Write `worker`'s heartbeat so the object carries a *store* modification
    /// time of `mtime_ms`, which is what the LIST result reports and therefore
    /// what the read-side skip and the reaper judge against. The oracle's clock
    /// is left at `restore_ms`, so anything written afterwards lands current.
    async fn beat_with_mtime(
        memory: &MemoryStore,
        store: &dyn ObjectStoreBackend,
        worker: &QueryWorkers,
        mtime_ms: u64,
        restore_ms: u64,
        stamp_ns: i64,
    ) {
        memory.set_clock_ms(mtime_ms);
        worker
            .write_heartbeat(store, stamp_ns)
            .await
            .expect("seed heartbeat");
        memory.set_clock_ms(restore_ms);
    }

    /// Every key currently under `sys/query/workers/`, sorted.
    async fn query_worker_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
        let mut keys: Vec<String> = list_all(store, QUERY_WORKERS_PREFIX)
            .await
            .expect("list query workers prefix")
            .into_iter()
            .map(|meta| meta.key)
            .collect();
        keys.sort();
        keys
    }

    /// Each reason indexes its own counter, at its position in `ALL`, so no
    /// two reasons share one and the arrays sized from `ALL.len()` cover
    /// every index.
    #[test]
    fn reject_index_is_the_position_in_all() {
        for (position, reason) in RecordReject::ALL.into_iter().enumerate() {
            assert_eq!(reason.index(), position, "{reason:?}");
        }
    }

    /// A record round-trips through its JSON encoding unchanged.
    #[test]
    fn record_json_round_trips() {
        let record = QueryWorkerRecord {
            process_id: Uuid::new_v4().to_string(),
            fragment_endpoint: "worker-7.internal:9443".to_string(),
            protocol_version: 1,
            started_unix_ns: -12_345,
        };
        let bytes = record.encode().expect("encode");
        let decoded = QueryWorkerRecord::decode(&bytes).expect("decode");
        assert_eq!(record, decoded);
    }

    /// A signed heartbeat object decodes as a plain [`QueryWorkerRecord`],
    /// the shape a reader from before record MACs reads it as: the `mac`
    /// field is ignored.
    #[tokio::test]
    async fn signed_object_decodes_as_a_plain_record() {
        let store = MemoryStore::new();
        let a = worker();
        a.write_heartbeat(&store, 42).await.expect("beat");
        let got = store
            .get(
                &query_worker_key(&a.process_id().to_string()),
                GetRange::Full,
            )
            .await
            .expect("get record");
        assert!(
            serde_json::from_slice::<serde_json::Value>(got.data.as_ref())
                .expect("json")
                .get("mac")
                .is_some()
        );
        assert_eq!(
            QueryWorkerRecord::decode(got.data.as_ref()).expect("decode"),
            a.record_at(42)
        );
    }

    /// A heartbeat object written before release B (ADR-1689 decision 4)
    /// still carries `flight_sql_endpoint`, the public gRPC address the
    /// removed plaintext SQL lane dialed. A rolling deploy has both shapes
    /// live for one heartbeat cycle, so it must still decode: the field is
    /// ignored, and the record keeps only `fragment_endpoint`, the address both
    /// lanes dial over TLS. Re-encoding the decoded record drops the field.
    #[test]
    fn pre_release_b_record_with_flight_sql_endpoint_decodes_and_ignores_it() {
        let json = br#"{"process_id":"p","fragment_endpoint":"10.0.0.1:9443","flight_sql_endpoint":"10.0.0.1:9000","protocol_version":5,"started_unix_ns":7}"#;
        let decoded = QueryWorkerRecord::decode(json).expect("pre-release-B record decodes");
        assert_eq!(
            decoded,
            QueryWorkerRecord {
                process_id: "p".to_string(),
                fragment_endpoint: "10.0.0.1:9443".to_string(),
                protocol_version: 5,
                started_unix_ns: 7,
            }
        );
        let reencoded = String::from_utf8(decoded.encode().expect("encode")).expect("utf8");
        assert!(
            !reencoded.contains("10.0.0.1:9000") && !reencoded.contains("flight_sql"),
            "the public gRPC address does not survive a decode: {reencoded}"
        );
    }

    /// A record from before the field was ever added also decodes: the
    /// release B shape is exactly that shape.
    #[test]
    fn record_from_before_the_field_existed_decodes() {
        let json = br#"{"process_id":"p","fragment_endpoint":"10.0.0.1:9443","protocol_version":1,"started_unix_ns":7}"#;
        let decoded = QueryWorkerRecord::decode(json).expect("record decodes");
        assert_eq!(decoded.fragment_endpoint, "10.0.0.1:9443");
        assert_eq!(decoded.protocol_version, 1);
        assert_eq!(decoded.started_unix_ns, 7);
    }

    /// Heartbeats round-trip through a store: two workers each write their own
    /// key, and each computes a live set that converges to include both.
    #[tokio::test]
    async fn live_sets_converge_to_include_both_workers() {
        let store = MemoryStore::new();
        let now = 1_000 * H_NS;
        let a = worker();
        let b = worker();

        a.write_heartbeat(&store, now).await.expect("a heartbeat");
        b.write_heartbeat(&store, now).await.expect("b heartbeat");

        let live_a = a.live_set(&store, now).await.expect("a live set");
        let ids: Vec<&str> = live_a.iter().map(|r| r.process_id.as_str()).collect();
        assert!(ids.contains(&a.process_id().to_string().as_str()));
        assert!(ids.contains(&b.process_id().to_string().as_str()));
        assert_eq!(live_a.len(), 2);
    }

    /// A sibling whose heartbeat is older than `3 * H` is excluded from the
    /// live set, while one exactly at the boundary stays (inclusive on the
    /// fresh side), reusing the worker_set staleness rule verbatim.
    #[tokio::test]
    async fn stale_sibling_is_excluded_at_three_h() {
        let store = MemoryStore::new();
        let a = worker();
        let b = worker();

        // `b` last beat at t=0; `a` reads at various later times.
        b.write_heartbeat(&store, 0).await.expect("b heartbeat");

        // Exactly 3*H old: still live (inclusive on the fresh side).
        let live = a.live_set(&store, 3 * H_NS).await.expect("live set");
        assert!(
            live.iter()
                .any(|r| r.process_id == b.process_id().to_string()),
            "3*H is still live"
        );

        // One nanosecond past 3*H: excluded.
        let live = a.live_set(&store, 3 * H_NS + 1).await.expect("live set");
        assert!(
            !live
                .iter()
                .any(|r| r.process_id == b.process_id().to_string()),
            "past 3*H is stale"
        );
        assert_eq!(live.len(), 1, "only self survives");
        assert_eq!(live[0].process_id, a.process_id().to_string());
        // The store reports no modification time, so the record was fetched
        // and verified: only its stamp kept it out.
        assert_eq!(
            a.record_rejects().by_reason(),
            [
                (RecordReject::MissingMac, 0),
                (RecordReject::BadMac, 0),
                (RecordReject::Oversize, 0),
                (RecordReject::Malformed, 0),
                (RecordReject::StaleStamp, 1),
            ],
            "a fetched, verified record with a stale stamp is one stale_stamp"
        );
    }

    /// A record whose body `process_id` disagrees with the id its key names is
    /// skipped: worker identity is the key, not the trusted body. A key that is
    /// not `sys/query/workers/<uuid>` at all is likewise skipped, not a panic.
    #[tokio::test]
    async fn record_with_body_key_mismatch_is_excluded() {
        let store = MemoryStore::new();
        let reader = worker();
        let now = 1_000 * H_NS;

        // A fresh, non-stale record written under the key of `key_id` but whose
        // body claims a different `body_id`. The stamp is current, so only the
        // identity mismatch can exclude it.
        let key_id = Uuid::new_v4();
        let body_id = Uuid::new_v4();
        assert_ne!(key_id, body_id);
        let forged = QueryWorkerRecord {
            process_id: body_id.to_string(),
            fragment_endpoint: "10.9.9.9:9443".to_string(),
            protocol_version: 1,
            started_unix_ns: now,
        };
        store
            .put(
                &query_worker_key(&key_id.to_string()),
                forged.encode().expect("encode forged").into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put forged");

        // A key under the prefix that is not a UUID at all.
        store
            .put(
                &format!("{QUERY_WORKERS_PREFIX}not-a-uuid"),
                b"{}".to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put junk key");

        let live = reader.live_set(&store, now).await.expect("live set");
        assert_eq!(live.len(), 1, "only the reader itself survives");
        assert_eq!(live[0].process_id, reader.process_id().to_string());
        assert!(
            !live
                .iter()
                .any(|r| r.process_id == body_id.to_string() || r.process_id == key_id.to_string()),
            "neither the key id nor the forged body id may enter the live set"
        );
        assert_eq!(
            reader.record_rejects().by_reason(),
            [
                (RecordReject::MissingMac, 0),
                (RecordReject::BadMac, 0),
                (RecordReject::Oversize, 0),
                (RecordReject::Malformed, 1),
                (RecordReject::StaleStamp, 0),
            ],
            "the mismatch is malformed; the non-UUID key is skipped uncounted"
        );
    }

    /// After a worker marks itself drained, a sibling's live set stops
    /// including it on the sibling's next read, without waiting for the `3 * H`
    /// staleness window to age the record out, and the drain issued no delete:
    /// the query role holds no delete grant. The key itself stays for the
    /// maintain role to reap.
    ///
    /// Every delete under the prefix is scripted to fail, so a drain that
    /// deleted anything shows up as a fired fault.
    #[tokio::test]
    async fn drained_worker_drops_from_the_live_set_without_a_delete() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Delete,
                ScriptedFault::Transient("a query-side delete".into()),
            )
            .with_key_contains(QUERY_WORKERS_PREFIX),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        let now = 1_000 * H_NS;
        let a = worker();
        let b = worker();

        a.write_heartbeat(&store, now).await.expect("a heartbeat");
        b.write_heartbeat(&store, now).await.expect("b heartbeat");

        // Both fresh: `a` sees both, at the same reader clock.
        let live = a.live_set(&store, now).await.expect("live set");
        assert_eq!(live.len(), 2, "both workers live before the drain");

        // `b` drains; `a` re-reads at the SAME clock, so only the drain (not
        // staleness) can drop `b`.
        b.mark_drained(&store).await.expect("b drain");
        let live = a.live_set(&store, now).await.expect("live set");
        assert_eq!(live.len(), 1, "only self survives after the sibling drains");
        assert_eq!(live[0].process_id, a.process_id().to_string());
        // The store reports no modification time, so the drained record was
        // fetched and verified, and its stamp is past every window.
        assert_eq!(
            a.record_rejects().by_reason().map(|(_, n)| n),
            [0; RecordReject::ALL.len()],
            "a drained record is a departure, not a reject"
        );

        assert_eq!(
            store.fault_count(Op::Delete, FaultKind::Transient),
            0,
            "the drain must issue no delete under sys/query/workers/"
        );
        assert!(
            query_worker_keys(&store)
                .await
                .contains(&query_worker_key(&b.process_id().to_string())),
            "the drained record is still there, for the maintain role to reap"
        );
    }

    /// A drained record is signed like a heartbeat: its MAC covers the
    /// [`DRAINED_STAMP_NS`] stamp under the minting key.
    #[tokio::test]
    async fn drained_record_carries_a_verifying_mac() {
        let store = MemoryStore::new();
        let a = worker();
        a.mark_drained(&store).await.expect("drain");
        let got = store
            .get(
                &query_worker_key(&a.process_id().to_string()),
                GetRange::Full,
            )
            .await
            .expect("get drained record");
        let signed: SignedRecord = serde_json::from_slice(got.data.as_ref()).expect("decode");
        assert_eq!(signed.record.started_unix_ns, DRAINED_STAMP_NS);
        let mac = signed.mac.expect("drained record carries a mac");
        let presented = blake3::Hash::from_hex(mac.as_bytes()).expect("64 hex");
        assert!(keys().verifies(&signed.record, &presented));
    }

    /// A far-future-dated record must be excluded exactly like a far-past one,
    /// not treated as live forever (symmetric staleness, ADR-0065 direction).
    #[tokio::test]
    async fn future_dated_sibling_is_excluded() {
        let store = MemoryStore::new();
        let a = worker();
        let phantom = worker();

        // The phantom's record claims a timestamp far past the liveness window
        // into the future relative to the reader's clock.
        phantom
            .write_heartbeat(&store, 100 * H_NS)
            .await
            .expect("phantom heartbeat");

        let live = a.live_set(&store, 0).await.expect("live set");
        assert!(
            !live
                .iter()
                .any(|r| r.process_id == phantom.process_id().to_string()),
            "a far-future-dated record must not read as live"
        );
        assert_eq!(live.len(), 1, "only self survives");
        assert_eq!(
            a.record_rejects().by_reason(),
            [
                (RecordReject::MissingMac, 0),
                (RecordReject::BadMac, 0),
                (RecordReject::Oversize, 0),
                (RecordReject::Malformed, 0),
                (RecordReject::StaleStamp, 1),
            ],
            "a fetched, verified record stamped past the window is one stale_stamp"
        );
    }

    /// A key whose LIST modification time is outside the liveness window is
    /// skipped before the GET and counts no reject, although its stamp is
    /// stale too and would count `stale_stamp` had it been fetched. The first
    /// GET under the prefix is scripted to fail, so any fetch fails the read.
    #[tokio::test]
    async fn mtime_stale_sibling_is_never_fetched_and_counts_nothing() {
        let now_ns = 1_000 * H_NS;
        let now_ms = 1_000 * H_MS;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("a query-workers-prefix GET".into()),
            )
            .with_key_contains(QUERY_WORKERS_PREFIX)
            .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        let me = worker();
        let dead = worker();
        beat_with_mtime(
            store.inner(),
            &store,
            &dead,
            now_ms - 10 * H_MS,
            now_ms,
            now_ns - 10 * H_NS,
        )
        .await;

        let live = me.live_set(&store, now_ns).await.expect("live set");
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Transient),
            0,
            "the mtime-stale key cost no GET"
        );
        assert_eq!(live.len(), 1, "only self survives");
        assert_eq!(
            me.record_rejects().by_reason().map(|(_, n)| n),
            [0; RecordReject::ALL.len()],
            "an mtime-stale skip is not a reject"
        );
    }

    /// `live_set` pays one GET per LIVE sibling, not per listed key (issue
    /// #1761): 500 dead keys and 2 live ones sit under the prefix, and the 4th
    /// GET under it is scripted to fail, so a cycle that fetches any key the
    /// LIST already showed as stale surfaces as a fired fault. Without the
    /// modification-time skip the GET count is one per listed key, so it scales
    /// with every query worker that ever ran.
    #[tokio::test]
    async fn live_set_costs_no_get_for_a_key_the_list_shows_stale() {
        let now_ns = 1_000 * H_NS;
        let now_ms = 1_000 * H_MS;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("a 3rd query-workers-prefix GET".into()),
            )
            .with_key_contains(QUERY_WORKERS_PREFIX)
            .with_occurrence(Occurrence::Nth(3)),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        store.inner().set_clock_ms(now_ms);

        let me = worker();
        let live_a = worker();
        let live_b = worker();
        beat_with_mtime(store.inner(), &store, &me, now_ms, now_ms, now_ns).await;
        beat_with_mtime(store.inner(), &store, &live_a, now_ms, now_ms, now_ns).await;
        beat_with_mtime(store.inner(), &store, &live_b, now_ms, now_ms, now_ns).await;
        for _ in 0..500 {
            beat_with_mtime(
                store.inner(),
                &store,
                &worker(),
                now_ms - 10 * H_MS,
                now_ms,
                now_ns - 10 * H_NS,
            )
            .await;
        }

        let read = me.live_set(&store, now_ns).await;
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Transient),
            0,
            "a 3rd GET under the query-workers prefix never happened: at most 2, one \
             per live sibling, with self skipped and the stale keys costing none"
        );
        let live = read.expect("live set");
        let ids: Vec<&str> = live.iter().map(|r| r.process_id.as_str()).collect();
        assert!(
            ids.contains(&live_a.process_id().to_string().as_str()),
            "the first live sibling is in the set"
        );
        assert!(
            ids.contains(&live_b.process_id().to_string().as_str()),
            "the second live sibling is in the set"
        );
        assert_eq!(
            live.len(),
            3,
            "self plus the two live siblings, nobody else"
        );
    }

    /// The reaper deletes exactly the keys past the reap horizon (issue #1761):
    /// dead workers go, a worker inside the clock-skew margin stays even though
    /// it is already out of the live set, fresh workers stay, a key whose
    /// modification time is in the future stays, and a key that is not
    /// `sys/query/workers/<uuid>` is left alone.
    async fn reap_case(memory: MemoryStore) {
        let now_ns = 1_000 * H_NS;
        let now_ms = 1_000 * H_MS;
        let store = memory;
        store.set_clock_ms(now_ms);

        let me = worker();
        let fresh = worker();
        // 4 * H old: past the 3 * H liveness window, inside the 6 * H horizon.
        let skewed = worker();
        // A store clock running ahead of this reader's: read, never reaped.
        let future = worker();
        let dead: Vec<QueryWorkers> = (0..3).map(|_| worker()).collect();

        beat_with_mtime(&store, &store, &me, now_ms, now_ms, now_ns).await;
        beat_with_mtime(&store, &store, &fresh, now_ms, now_ms, now_ns).await;
        beat_with_mtime(
            &store,
            &store,
            &skewed,
            now_ms - 4 * H_MS,
            now_ms,
            now_ns - 4 * H_NS,
        )
        .await;
        beat_with_mtime(&store, &store, &future, now_ms + 100 * H_MS, now_ms, now_ns).await;
        for d in &dead {
            beat_with_mtime(
                &store,
                &store,
                d,
                now_ms - 10 * H_MS,
                now_ms,
                now_ns - 10 * H_NS,
            )
            .await;
        }
        // A key under the same prefix that this mechanism does not own: never
        // reaped, however old it looks.
        store.set_clock_ms(now_ms - 100 * H_MS);
        store
            .put(
                &format!("{QUERY_WORKERS_PREFIX}not-a-uuid"),
                b"x".to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed foreign key");
        store.set_clock_ms(now_ms);

        let window = default_liveness_window_ns();
        let pass = reap_dead_query_workers(&store, now_ns, window)
            .await
            .expect("reap");
        assert_eq!(
            pass,
            ReapPass {
                reaped: 3,
                access_denied: false
            },
            "exactly the three keys past the horizon"
        );

        let mut expected = vec![
            query_worker_key(&me.process_id().to_string()),
            query_worker_key(&fresh.process_id().to_string()),
            query_worker_key(&skewed.process_id().to_string()),
            query_worker_key(&future.process_id().to_string()),
            format!("{QUERY_WORKERS_PREFIX}not-a-uuid"),
        ];
        expected.sort();
        assert_eq!(
            query_worker_keys(&store).await,
            expected,
            "own, fresh, inside-the-margin, future-dated and foreign keys all survive"
        );

        // Reaping again is idempotent: nothing left is past the horizon.
        assert_eq!(
            reap_dead_query_workers(&store, now_ns, window)
                .await
                .expect("second reap")
                .reaped,
            0
        );
    }

    #[tokio::test]
    async fn dead_query_worker_heartbeats_are_reaped_past_the_horizon() {
        reap_case(MemoryStore::new()).await;
    }

    /// The same case over a listing that pages: a reaper that only ever sees
    /// the first page leaves most of a long-lived prefix behind.
    #[tokio::test]
    async fn dead_query_worker_heartbeats_are_reaped_across_list_pages() {
        reap_case(MemoryStore::with_page_size(2)).await;
    }

    /// A transient failure on one delete is per key: the pass logs it, goes on
    /// to the remaining keys, and does not report a denial. Only
    /// `AccessDenied` ends a pass early.
    #[tokio::test]
    async fn a_transient_delete_failure_does_not_end_the_reap_pass() {
        let now_ns = 1_000 * H_NS;
        let now_ms = 1_000 * H_MS;
        let memory = MemoryStore::new();
        memory.set_clock_ms(now_ms);
        for _ in 0..3 {
            beat_with_mtime(
                &memory,
                &memory,
                &worker(),
                now_ms - 10 * H_MS,
                now_ms,
                now_ns - 10 * H_NS,
            )
            .await;
        }
        let store = FaultStore::new(
            memory,
            FaultPlan::empty().with_rule(
                Rule::new(Op::Delete, ScriptedFault::Transient("a blip".into()))
                    .with_occurrence(Occurrence::Nth(1)),
            ),
        );

        let pass = reap_dead_query_workers(&store, now_ns, default_liveness_window_ns())
            .await
            .expect("reap");

        assert_eq!(store.fault_count(Op::Delete, FaultKind::Transient), 1);
        assert_eq!(
            pass,
            ReapPass {
                reaped: 2,
                access_denied: false
            },
            "the two keys after the failed one were still deleted"
        );
        assert_eq!(query_worker_keys(&store).await.len(), 1);
    }
}
