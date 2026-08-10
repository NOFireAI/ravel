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

use std::time::Duration;

use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::worker_set::{DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LIVENESS_FACTOR, is_stale};

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
/// - `fragment_endpoint`: where this worker serves the `queryfrag` fetch
///   surface (host:port); how a coordinator reaches it to dispatch a slice.
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
}

impl QueryWorkers {
    /// A query worker with an explicit timing configuration. `liveness_factor`
    /// is clamped to at least 1, matching [`crate::worker_set::WorkerSet::new`].
    pub fn new(
        fragment_endpoint: impl Into<String>,
        protocol_version: u32,
        heartbeat_interval: Duration,
        liveness_factor: u32,
    ) -> Self {
        QueryWorkers {
            process_id: Uuid::new_v4(),
            fragment_endpoint: fragment_endpoint.into(),
            protocol_version,
            heartbeat_interval,
            liveness_factor: liveness_factor.max(1),
        }
    }

    /// A query worker with the ADR-0065 heartbeat defaults (`H = 60s`, `3 * H`
    /// liveness window) reused for the query role.
    pub fn with_defaults(fragment_endpoint: impl Into<String>, protocol_version: u32) -> Self {
        Self::new(
            fragment_endpoint,
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

    /// The `queryfrag` endpoint this worker advertises.
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
        let h = i64::try_from(self.heartbeat_interval.as_nanos()).unwrap_or(i64::MAX);
        h.saturating_mul(i64::from(self.liveness_factor))
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
        Ok(live)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    const H: Duration = Duration::from_secs(60);
    const H_NS: i64 = 60 * 1_000_000_000;

    fn worker() -> QueryWorkers {
        QueryWorkers::new("10.0.0.1:9443", 1, H, DEFAULT_LIVENESS_FACTOR)
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
}
