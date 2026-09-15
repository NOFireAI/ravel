//! Query-worker registration and liveness for the ADR-0071 distributed read
//! fan-out.
//!
//! A query-role process that participates in fan-out registers itself the same
//! way a maintain worker does (ADR-0065 decision 1, [`crate::worker_set`]): it
//! writes, every heartbeat interval `H`, a [`QueryWorkerRecord`] to a key it
//! alone ever writes:
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
//! `started_unix_ns` is the liveness timestamp: the writer stamps it with the
//! current clock on every heartbeat (mirroring the maintain heartbeat's
//! `heartbeat_unix_ns`), so a process that keeps beating stays live and one
//! that stops drops out after the staleness window. The field name follows the
//! frozen record shape #863 pins for the wiring tickets (#864, #865).
//!
//! No production caller wires this yet by design; #864 (codec + engine seam)
//! and #865 (server surface) consume it.
//!
//! # Bounding the prefix (issue #1761)
//!
//! [`QueryWorkers::delete_heartbeat`] removes a key on a graceful drain and on
//! no other path, so a worker lost to a panic, a kill, an out-of-memory event
//! or a node loss left its key behind forever and every later [`live_set`] paid
//! a GET for it. The two bounds [`crate::worker_set`] applies to the maintain
//! prefix for issue #1679 apply here unchanged, over the same shared
//! predicates:
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
//!   reader's. [`QueryWorkers::live_set_read`] returns those keys from the SAME
//!   listing it makes for the live set, and the heartbeat loop deletes them
//!   through [`QueryWorkers::reap_keys`], so the reap costs no listing of its
//!   own. [`QueryWorkers::reap_dead_workers`] is the standalone form for a
//!   caller that is not already reading the live set, and it does pay one
//!   listing. Reaping is idempotent and costs a live-but-skewed worker at most
//!   one heartbeat interval of invisibility, since it rewrites its key every
//!   `H`.
//!
//! The reap needs `s3:DeleteObject` on this prefix, which the shipped query
//! role does not have: `deploy/iam/query.json` grants no delete at all. Under
//! that template every delete here is denied and logged, the GET-skip still
//! applies, and the prefix stays as large as it was. See the tracking issue on
//! the IAM templates before relying on the bound.
//!
//! A backend reporting no usable modification time (`<= 0`) gets neither
//! treatment: its keys are read as before and never reaped. The same holds for
//! a modification time in the future, which means the store's clock runs ahead
//! of this reader's and is a reason to read the key rather than to drop it.
//!
//! [`live_set`]: QueryWorkers::live_set

use std::time::Duration;

use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::worker_set::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LIVENESS_FACTOR, is_stale, mtime_stale, reap_horizon_ns,
};

/// The prefix every query-worker's heartbeat key lives under (ADR-0071): a new
/// additive control-plane prefix under `sys/query/`, sibling to the maintain
/// role's `sys/maintain/workers/`. Fixed verbatim; #864 and #865 depend on it.
pub const QUERY_WORKERS_PREFIX: &str = "sys/query/workers/";

/// The heartbeat key one query-worker process owns and alone ever writes.
pub fn query_worker_key(process_id: &str) -> String {
    format!("{QUERY_WORKERS_PREFIX}{process_id}")
}

/// The process id a heartbeat key names, or `None` if the key is not a
/// well-formed `sys/query/workers/<uuid>` key. Worker identity is the key, not
/// the record body (mirrors [`crate::worker_set`]'s `process_id_of`): the key
/// is the one thing a single writer alone controls, so deriving identity from
/// it means a record whose body disagrees with its key cannot smuggle a false
/// identity into the live set. `list_all` yields the prefix itself and any
/// unexpected nested key under it; both parse to `None` and are skipped.
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
/// - `fragment_endpoint`: where this worker serves the `queryfrag` `SeriesFetch`
///   surface (host:port); how the PromQL distributed lane reaches it to dispatch
///   a slice. Under the ADR-0071 amendment (dedicated fragment listener) this is
///   the dedicated TLS listener address, reached over TLS; without one it is the
///   public gRPC listener address, reached plaintext.
/// - `flight_sql_endpoint`: where this worker serves the Flight SQL `DoGet`
///   surface (host:port); how the SQL distributed lane reaches it to fetch a
///   slice. This is the public gRPC listener, which mounts Flight SQL alongside
///   OTLP and is dialed plaintext. It is a SEPARATE field from
///   `fragment_endpoint` because the two surfaces no longer share one listener:
///   after the amendment the dedicated fragment listener serves the `SeriesFetch`
///   `Pinned` surface only (TLS), and Flight SQL stays on the public gRPC
///   listener, so the SQL lane must dial the public address rather than the
///   fragment one (see `services/ravel-server/src/sql_distrib.rs`). Added
///   additively: `#[serde(default)]` decodes a pre-amendment record that never
///   carried the field to an empty string, which the SQL roster drops.
/// - `protocol_version`: the `queryfrag` protocol version this worker speaks,
///   for the ADR-0071 version-skew fallback.
/// - `started_unix_ns`: the liveness timestamp (see the [module docs](self)),
///   re-stamped with the current clock on every heartbeat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryWorkerRecord {
    pub process_id: String,
    pub fragment_endpoint: String,
    #[serde(default)]
    pub flight_sql_endpoint: String,
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

/// One query-role process's membership handle (ADR-0071), the query-side analog
/// of [`crate::worker_set::WorkerSet`]: a stable `process_id`, the endpoint and
/// protocol version it advertises, and the heartbeat/liveness timing. Held once
/// for the whole process.
#[derive(Debug, Clone)]
pub struct QueryWorkers {
    process_id: Uuid,
    fragment_endpoint: String,
    flight_sql_endpoint: String,
    protocol_version: u32,
    heartbeat_interval: Duration,
    liveness_factor: u32,
}

/// What one listing of `sys/query/workers/` yields: the live set, and the keys
/// past the reap horizon. Returned together because the heartbeat loop needs
/// both on the same cadence over the same prefix, so a second listing would be
/// pure cost. Mirrors [`crate::worker_set::LiveSetRead`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LiveSetRead {
    /// Workers inside the liveness window, including this process.
    pub live: Vec<QueryWorkerRecord>,
    /// Keys past the reap horizon, for [`QueryWorkers::reap_keys`] to delete.
    pub reapable: Vec<String>,
}

impl QueryWorkers {
    /// A query worker with an explicit timing configuration. `liveness_factor`
    /// is clamped to at least 1, matching [`crate::worker_set::WorkerSet::new`].
    ///
    /// `fragment_endpoint` is the `queryfrag` `SeriesFetch` address the PromQL
    /// lane dials; `flight_sql_endpoint` is the public gRPC address the SQL lane
    /// dials for Flight SQL `DoGet` (see [`QueryWorkerRecord`]).
    pub fn new(
        fragment_endpoint: impl Into<String>,
        flight_sql_endpoint: impl Into<String>,
        protocol_version: u32,
        heartbeat_interval: Duration,
        liveness_factor: u32,
    ) -> Self {
        QueryWorkers {
            process_id: Uuid::new_v4(),
            fragment_endpoint: fragment_endpoint.into(),
            flight_sql_endpoint: flight_sql_endpoint.into(),
            protocol_version,
            heartbeat_interval,
            liveness_factor: liveness_factor.max(1),
        }
    }

    /// A query worker with the ADR-0065 heartbeat defaults (`H = 60s`, `3 * H`
    /// liveness window) reused for the query role.
    pub fn with_defaults(
        fragment_endpoint: impl Into<String>,
        flight_sql_endpoint: impl Into<String>,
        protocol_version: u32,
    ) -> Self {
        Self::new(
            fragment_endpoint,
            flight_sql_endpoint,
            protocol_version,
            DEFAULT_HEARTBEAT_INTERVAL,
            DEFAULT_LIVENESS_FACTOR,
        )
    }

    /// This process's stable membership id. Names the one heartbeat key this
    /// process owns.
    pub fn process_id(&self) -> Uuid {
        self.process_id
    }

    /// The `queryfrag` `SeriesFetch` endpoint this worker advertises (PromQL
    /// distributed lane).
    pub fn fragment_endpoint(&self) -> &str {
        &self.fragment_endpoint
    }

    /// The Flight SQL `DoGet` endpoint this worker advertises (SQL distributed
    /// lane): its public gRPC listener.
    pub fn flight_sql_endpoint(&self) -> &str {
        &self.flight_sql_endpoint
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
        let h = i64::try_from(self.heartbeat_interval.as_nanos()).unwrap_or(i64::MAX);
        h.saturating_mul(i64::from(self.liveness_factor))
    }

    /// The record this process advertises at `now_ns` (`started_unix_ns` is the
    /// heartbeat stamp; see the [module docs](self)).
    fn record_at(&self, now_ns: i64) -> QueryWorkerRecord {
        QueryWorkerRecord {
            process_id: self.process_id.to_string(),
            fragment_endpoint: self.fragment_endpoint.clone(),
            flight_sql_endpoint: self.flight_sql_endpoint.clone(),
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
        let record = self.record_at(now_ns);
        let bytes = record
            .encode()
            .map_err(|e| StoreError::Permanent(format!("encode query worker heartbeat: {e}")))?;
        let key = query_worker_key(&record.process_id);
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

    /// Delete this process's own heartbeat record. Called on graceful shutdown
    /// so a draining query worker stops advertising itself to sibling
    /// coordinators immediately, rather than lingering in their live set until
    /// its stamp ages past the `3 * H` staleness window. Deletes only the one
    /// key this process owns (`sys/query/workers/<process_id>`); a single writer
    /// alone controls that key, so this never races another process. A missing
    /// key is not an error (`delete` is idempotent).
    pub async fn delete_heartbeat(&self, store: &dyn ObjectStoreBackend) -> Result<(), StoreError> {
        store
            .delete(&query_worker_key(&self.process_id.to_string()))
            .await
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
    /// skipped as malformed. A single writer alone controls its own key, so a
    /// body/key mismatch means a corrupt or forged record and must not enter
    /// the live set under either identity.
    ///
    /// A corrupt or mismatched sibling record is skipped (treated as absent,
    /// self-correcting next interval); only a failed LIST or GET is an `Err`,
    /// which the caller treats fail-open, never freezing fan-out on a transient
    /// read fault.
    ///
    /// A key the LIST result already shows as past the liveness window costs no
    /// GET at all (issue #1761; see the [module docs](self)), so the read cost
    /// tracks the live fleet rather than every query worker that ever ran.
    pub async fn live_set(
        &self,
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
    ) -> Result<Vec<QueryWorkerRecord>, StoreError> {
        Ok(self.live_set_read(store, now_ns).await?.live)
    }

    /// The live set AND the keys past the reap horizon, from ONE listing
    /// (issue #1761).
    ///
    /// The heartbeat loop needs both every interval, and the prefix is the
    /// same. Returning them together is what makes the reap free rather than a
    /// second LIST; [`crate::worker_set::WorkerSet::live_set_read`] is the same
    /// shape for the same reason. A caller that wants only one of the two still
    /// pays one listing, never two.
    pub async fn live_set_read(
        &self,
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
    ) -> Result<LiveSetRead, StoreError> {
        let window = self.liveness_window_ns();
        let horizon = reap_horizon_ns(window);
        let objects = list_all(store, QUERY_WORKERS_PREFIX).await?;
        let mut live = vec![self.record_at(now_ns)];
        let mut reapable = Vec::new();
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
                // (issue #1761). Past the wider reap horizon it is also this
                // listing's reap candidate, collected here so the delete needs
                // no listing of its own.
                if mtime_stale(now_ns, meta.last_modified_unix_ms, horizon) {
                    reapable.push(meta.key.clone());
                }
                continue;
            }
            let got = store.get(&meta.key, GetRange::Full).await?;
            let Ok(record) = QueryWorkerRecord::decode(got.data.as_ref()) else {
                tracing::debug!(
                    key = %meta.key,
                    "query_workers: skipping an undecodable sibling record"
                );
                continue;
            };
            // The record body must name the same process id its key does. A
            // disagreement is a corrupt or forged record; trust the key.
            if record.process_id != pid.to_string() {
                tracing::debug!(
                    key = %meta.key,
                    body_process_id = %record.process_id,
                    "query_workers: skipping a record whose body process_id disagrees with its key"
                );
                continue;
            }
            if is_stale(now_ns, record.started_unix_ns, window) {
                continue;
            }
            live.push(record);
        }
        live.sort_unstable_by(|a, b| a.process_id.cmp(&b.process_id));
        live.dedup_by(|a, b| a.process_id == b.process_id);
        Ok(LiveSetRead { live, reapable })
    }

    /// Delete the keys a [`live_set_read`] already identified. Issues no
    /// listing of its own, which is the whole point of taking them as an
    /// argument.
    ///
    /// `delete` is idempotent, so several coordinators reaping the same dead
    /// key concurrently is not a race. A failed individual delete is logged and
    /// retried by whichever process next lists the prefix, since a key that
    /// outlives one tick costs one listing entry and nothing else.
    ///
    /// [`live_set_read`]: Self::live_set_read
    pub async fn reap_keys(&self, store: &dyn ObjectStoreBackend, keys: &[String]) -> u64 {
        let mut reaped = 0;
        for key in keys {
            match store.delete(key).await {
                Ok(()) => reaped += 1,
                Err(err) => tracing::warn!(
                    key = %key,
                    error = %err,
                    "query_workers: reaping a dead worker record failed; retried next tick"
                ),
            }
        }
        reaped
    }

    /// Delete every record under `sys/query/workers/` that is past the reap
    /// horizon (issue #1761), judged from the modification time the LIST result
    /// already carries, and return how many were deleted.
    ///
    /// This is the standalone form and it costs one listing. The heartbeat loop
    /// does NOT use it: that loop already calls [`live_set_read`], which returns
    /// the same candidates from the listing it was making anyway, so it reaps
    /// through [`reap_keys`] and pays nothing extra. Use this where no live-set
    /// read is happening.
    ///
    /// Never deletes this process's own key, never deletes a key that does not
    /// parse as `sys/query/workers/<uuid>` (something else's key under a prefix
    /// this process does not own is not this function's to reap), and never
    /// deletes a key whose modification time the backend reports as unknown or
    /// as being in the future. The horizon is `reap_horizon_ns` of the liveness
    /// window, so a worker whose record merely aged out of the live set keeps a
    /// full extra window of grace against clock skew before it is reaped.
    ///
    /// A failed LIST is an `Err` the caller can treat fail-open; a failed
    /// individual delete is logged and retried by whichever process next lists
    /// the prefix.
    ///
    /// [`live_set_read`]: Self::live_set_read
    /// [`reap_keys`]: Self::reap_keys
    pub async fn reap_dead_workers(
        &self,
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
    ) -> Result<u64, StoreError> {
        let read = self.live_set_read(store, now_ns).await?;
        Ok(self.reap_keys(store, &read.reapable).await)
    }
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

    fn worker() -> QueryWorkers {
        QueryWorkers::new(
            "10.0.0.1:9443",
            "10.0.0.1:9000",
            1,
            H,
            DEFAULT_LIVENESS_FACTOR,
        )
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

    /// A record round-trips through its JSON encoding unchanged.
    #[test]
    fn record_json_round_trips() {
        let record = QueryWorkerRecord {
            process_id: Uuid::new_v4().to_string(),
            fragment_endpoint: "worker-7.internal:9443".to_string(),
            flight_sql_endpoint: "worker-7.internal:9000".to_string(),
            protocol_version: 1,
            started_unix_ns: -12_345,
        };
        let bytes = record.encode().expect("encode");
        let decoded = QueryWorkerRecord::decode(&bytes).expect("decode");
        assert_eq!(record, decoded);
    }

    /// A pre-amendment heartbeat object never carried `flight_sql_endpoint`. It
    /// must still decode (the field is `#[serde(default)]`), yielding an empty
    /// endpoint that the SQL roster drops, so a rolling deploy never fails a
    /// decode on an old sibling's record.
    #[test]
    fn record_without_flight_sql_endpoint_decodes_to_empty() {
        let json = br#"{"process_id":"p","fragment_endpoint":"10.0.0.1:9443","protocol_version":1,"started_unix_ns":7}"#;
        let decoded = QueryWorkerRecord::decode(json).expect("legacy record decodes");
        assert_eq!(
            decoded.flight_sql_endpoint, "",
            "missing field defaults empty"
        );
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
            flight_sql_endpoint: "10.9.9.9:9000".to_string(),
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
    }

    /// After a worker deletes its own heartbeat, a sibling's live set stops
    /// including it immediately, without waiting for the `3 * H` staleness
    /// window to age the record out.
    #[tokio::test]
    async fn deleted_heartbeat_drops_from_the_live_set_immediately() {
        let store = MemoryStore::new();
        let now = 1_000 * H_NS;
        let a = worker();
        let b = worker();

        a.write_heartbeat(&store, now).await.expect("a heartbeat");
        b.write_heartbeat(&store, now).await.expect("b heartbeat");

        // Both fresh: `a` sees both, at the same reader clock.
        let live = a.live_set(&store, now).await.expect("live set");
        assert_eq!(live.len(), 2, "both workers live before delete");

        // `b` drains and deletes its record; `a` re-reads at the SAME clock, so
        // only the deletion (not staleness) can drop `b`.
        b.delete_heartbeat(&store).await.expect("b delete");
        let live = a.live_set(&store, now).await.expect("live set");
        assert_eq!(
            live.len(),
            1,
            "only self survives after the sibling deletes"
        );
        assert_eq!(live[0].process_id, a.process_id().to_string());
        assert!(
            !live
                .iter()
                .any(|r| r.process_id == b.process_id().to_string()),
            "a deleted worker must not read as live even within its window"
        );

        // Idempotent: deleting an already-absent key is not an error.
        b.delete_heartbeat(&store)
            .await
            .expect("second delete is a no-op");
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
    /// it is already out of the live set, a fresh worker stays, a key whose
    /// modification time is in the future stays, this process's own key stays,
    /// and a key that is not `sys/query/workers/<uuid>` is left alone.
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

        let reaped = me.reap_dead_workers(&store, now_ns).await.expect("reap");
        assert_eq!(reaped, 3, "exactly the three keys past the horizon");

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
            me.reap_dead_workers(&store, now_ns)
                .await
                .expect("second reap"),
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

    /// The tick's listing serves both purposes: `live_set_read` returns the
    /// live set AND the reap candidates from ONE listing, so reaping costs no
    /// second LIST of the same prefix.
    ///
    /// Asserted through the FaultStore's LIST counter, because that is the
    /// claim: one listing, not two.
    #[tokio::test]
    async fn one_listing_serves_both_the_live_set_and_the_reap() {
        let now_ns = 1_000 * H_NS;
        let now_ms = 1_000 * H_MS;
        let memory = MemoryStore::new();
        memory.set_clock_ms(now_ms);

        let me = worker();
        let dead = worker();

        // Seed through the memory store directly: the rule below matches
        // `list`, and seeding is `put`, but going through the plain store
        // keeps the scripted occurrence counting only the read path.
        beat_with_mtime(&memory, &memory, &me, now_ms, now_ms, now_ns).await;
        beat_with_mtime(
            &memory,
            &memory,
            &dead,
            now_ms - 10 * H_MS,
            now_ms,
            now_ns - 10 * H_NS,
        )
        .await;

        // Fault the SECOND listing of this prefix: a shape that called
        // `live_set` and then `reap_dead_workers` would list twice and trip it.
        let store = FaultStore::new(
            memory,
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::List,
                    ScriptedFault::Transient("a second listing".into()),
                )
                .with_occurrence(Occurrence::Nth(2)),
            ),
        );

        let read = me.live_set_read(&store, now_ns).await.expect("one read");
        let reaped = me.reap_keys(&store, &read.reapable).await;

        assert_eq!(
            store.fault_count(Op::List, FaultKind::Transient),
            0,
            "the live set and the reap must come from one listing, not two"
        );
        assert_eq!(reaped, 1, "the dead worker's key was reaped");
        assert_eq!(read.live.len(), 1, "only this process is live");
        assert_eq!(read.live[0].process_id, me.process_id().to_string());
    }
}
