//! The in-memory, concurrently-readable snapshot of gateway endpoints, kept up
//! to date by the [`EndpointSlice`] watcher (deliverable 2).
//!
//! # Ranking set vs. dial set
//!
//! The snapshot carries two things:
//!
//! - [`EndpointSnapshot::replicas`]: **every** endpoint the watched slices
//!   report, Ready or not, in the stable [`ReplicaId`] form
//!   [`ravel_affinity::rank`] orders. Ranking over the full membership (rather
//!   than only the currently-Ready pods) keeps a tenant's subset *identity*
//!   stable across a transient readiness blip: a pod that flaps NotReady is
//!   skipped at pick time (falling to position `S+1`, `S+2`, ...) rather than
//!   dropping out of the ranking and reshuffling the subset boundary.
//! - [`EndpointSnapshot::ready_addr`]: the dial address for **Ready** endpoints
//!   only. A ranked replica absent from this map is exactly the "not-Ready"
//!   signal the selection fallback ([`crate::select::pick`]) skips over. This is
//!   the "Ready snapshot" the router actually dials; Not-Ready endpoints are
//!   excluded from it (deliverable 2).
//!
//! Ranking over the full membership is what makes the unready-member fallback a
//! genuinely reachable code path: the not-Ready pod ranks into the subset and
//! is then skipped, rather than never being ranked at all.
//!
//! # Concurrency
//!
//! The shared snapshot is a `RwLock<Arc<EndpointSnapshot>>`, swapped in whole on
//! every watch event (`arc_swap` is not a workspace dependency). A reader
//! ([`EndpointStore::snapshot`]) only ever clones the `Arc` under a brief read
//! lock and then releases it, so it never holds the lock across the actual
//! ranking/selection work.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::ResourceExt;
use kube_runtime::watcher::Event;
use ravel_affinity::ReplicaId;

/// One parsed endpoint: its stable identity plus, when Ready and dialable, its
/// address.
#[derive(Clone, Debug)]
struct ParsedEndpoint {
    id: ReplicaId,
    /// `Some` only when the endpoint is Ready and its address parsed to a
    /// dialable `IP:port`.
    ready_addr: Option<SocketAddr>,
}

/// The immutable, atomically-swapped view the request path reads.
#[derive(Debug, Default)]
pub(crate) struct EndpointSnapshot {
    /// Every endpoint (Ready or not) as an HRW ranking candidate.
    pub replicas: Vec<ReplicaId>,
    /// Dial address for Ready endpoints only.
    pub ready_addr: HashMap<ReplicaId, SocketAddr>,
}

/// Internal per-slice state, plus the `Init`-relist buffer.
#[derive(Default)]
struct StoreInner {
    /// Current per-slice endpoints, keyed by slice object name. A Service's
    /// endpoints can span several slices, joined here.
    slices: HashMap<String, Vec<ParsedEndpoint>>,
    /// During a watcher relist (`Init` .. `InitDone`), endpoints buffer here and
    /// replace `slices` wholesale at `InitDone`, so a relist is atomic and never
    /// exposes a partially-listed set.
    pending: Option<HashMap<String, Vec<ParsedEndpoint>>>,
}

/// Watches EndpointSlices for one gateway Service and maintains the shared
/// [`EndpointSnapshot`].
pub(crate) struct EndpointStore {
    inner: Mutex<StoreInner>,
    snapshot: RwLock<Arc<EndpointSnapshot>>,
    /// Flipped `true` exactly once, at the first `InitDone`. Never reverts: a
    /// later relist re-buffers but does not un-sync a router that has already
    /// served.
    synced: AtomicBool,
    /// Dial by this port name; `None` uses the endpoint's first port.
    port_name: Option<String>,
}

impl EndpointStore {
    /// A store with an empty snapshot, not yet synced.
    pub(crate) fn new(port_name: Option<String>) -> Self {
        EndpointStore {
            inner: Mutex::new(StoreInner::default()),
            snapshot: RwLock::new(Arc::new(EndpointSnapshot::default())),
            synced: AtomicBool::new(false),
            port_name,
        }
    }

    /// Whether the watcher has completed at least one full sync. The proxy and
    /// `/readyz` both gate on this so no request is served from an empty subset
    /// during cold start.
    pub(crate) fn is_synced(&self) -> bool {
        self.synced.load(Ordering::Acquire)
    }

    /// Clone the current snapshot `Arc`, holding the read lock only for the
    /// clone. The caller ranks/selects on the returned `Arc` with no lock held.
    pub(crate) fn snapshot(&self) -> Arc<EndpointSnapshot> {
        // A poisoned lock carries only a plain data snapshot with no broken
        // invariant, so recovering the guard is safe and unwrap-free.
        self.snapshot
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Apply one watcher event, then rebuild and swap the snapshot.
    pub(crate) fn apply_event(&self, event: Event<EndpointSlice>) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match event {
            Event::Init => {
                // Start a fresh relist buffer; the live `slices` keep serving
                // until `InitDone` swaps the buffer in.
                inner.pending = Some(HashMap::new());
            }
            Event::InitApply(slice) => {
                let (name, endpoints) = self.parse_slice(&slice);
                inner
                    .pending
                    .get_or_insert_with(HashMap::new)
                    .insert(name, endpoints);
            }
            Event::InitDone => {
                if let Some(pending) = inner.pending.take() {
                    inner.slices = pending;
                }
                self.rebuild(&inner);
                // Latch synced only after the first complete relist is live.
                self.synced.store(true, Ordering::Release);
                return;
            }
            Event::Apply(slice) => {
                let (name, endpoints) = self.parse_slice(&slice);
                inner.slices.insert(name, endpoints);
            }
            Event::Delete(slice) => {
                inner.slices.remove(&slice.name_any());
            }
        }
        self.rebuild(&inner);
    }

    /// Recompute the snapshot from the current per-slice state and swap it in.
    fn rebuild(&self, inner: &StoreInner) {
        // Join endpoints across slices, deduping by ReplicaId. A pod that
        // appears more than once resolves to Ready-with-address if any
        // occurrence is.
        let mut merged: HashMap<ReplicaId, Option<SocketAddr>> = HashMap::new();
        for endpoints in inner.slices.values() {
            for ep in endpoints {
                let slot = merged.entry(ep.id.clone()).or_insert(None);
                if slot.is_none() {
                    *slot = ep.ready_addr;
                }
            }
        }

        let mut replicas = Vec::with_capacity(merged.len());
        let mut ready_addr = HashMap::new();
        for (id, addr) in merged {
            if let Some(addr) = addr {
                ready_addr.insert(id.clone(), addr);
            }
            replicas.push(id);
        }

        let snapshot = Arc::new(EndpointSnapshot {
            replicas,
            ready_addr,
        });
        *self.snapshot.write().unwrap_or_else(|e| e.into_inner()) = snapshot;
    }

    /// Extract `(slice name, endpoints)` from an [`EndpointSlice`].
    fn parse_slice(&self, slice: &EndpointSlice) -> (String, Vec<ParsedEndpoint>) {
        let name = slice.name_any();
        let port = select_port(slice, self.port_name.as_deref());
        let endpoints = slice
            .endpoints
            .iter()
            .filter_map(|ep| parse_endpoint(ep, &slice.address_type, port))
            .collect();
        (name, endpoints)
    }
}

/// Parse one [`k8s_openapi::api::discovery::v1::Endpoint`] into a
/// [`ParsedEndpoint`], or `None` when it has no stable identity (no
/// `targetRef.uid`) to rank on.
///
/// The identity is the pod UID (never the IP: ADR-0080 decision 3 requires
/// this because an IP can be reused by a different pod after churn). A Ready
/// endpoint whose address does not parse to a dialable `IP:port` still ranks
/// (its UID is present) but is not in the dial set.
fn parse_endpoint(
    ep: &k8s_openapi::api::discovery::v1::Endpoint,
    address_type: &str,
    port: Option<u16>,
) -> Option<ParsedEndpoint> {
    let uid = ep.target_ref.as_ref().and_then(|r| r.uid.as_deref())?;
    if uid.is_empty() {
        return None;
    }
    let id = ReplicaId::new(uid.as_bytes());

    let ready = ep
        .conditions
        .as_ref()
        .and_then(|c| c.ready)
        .unwrap_or(false);

    let ready_addr = if ready {
        dial_addr(ep, address_type, port)
    } else {
        None
    };

    Some(ParsedEndpoint { id, ready_addr })
}

/// The dialable `IP:port` for a Ready endpoint, or `None` when the address is
/// not an IP (e.g. an `FQDN` slice, which the EndpointSlice controller never
/// generates for Service endpoints) or there is no usable port.
fn dial_addr(
    ep: &k8s_openapi::api::discovery::v1::Endpoint,
    address_type: &str,
    port: Option<u16>,
) -> Option<SocketAddr> {
    if !address_type.eq_ignore_ascii_case("IPv4") && !address_type.eq_ignore_ascii_case("IPv6") {
        return None;
    }
    let ip: IpAddr = ep.addresses.first()?.parse().ok()?;
    Some(SocketAddr::new(ip, port?))
}

/// Select the port to dial from a slice: the one named `port_name`, or the
/// first port when no name is configured.
fn select_port(slice: &EndpointSlice, port_name: Option<&str>) -> Option<u16> {
    let ports = slice.ports.as_ref()?;
    let chosen = match port_name {
        Some(name) => ports.iter().find(|p| p.name.as_deref() == Some(name))?,
        None => ports.first()?,
    };
    u16::try_from(chosen.port?).ok()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) mod test_support {
    //! Helpers shared by this module's unit tests and the crate integration
    //! tests, for building synthetic EndpointSlices and driving the store
    //! without a real cluster.

    use super::*;

    /// A minimal endpoint description for a test slice.
    pub(crate) struct TestEndpoint {
        pub uid: &'static str,
        pub ip: &'static str,
        pub ready: bool,
    }

    /// Build an `IPv4` EndpointSlice named `name` for `service`, exposing one
    /// unnamed port `port`, carrying `endpoints`.
    pub(crate) fn slice(
        name: &str,
        service: &str,
        port: i32,
        endpoints: &[TestEndpoint],
    ) -> EndpointSlice {
        let eps: Vec<_> = endpoints
            .iter()
            .map(|e| {
                serde_json::json!({
                    "addresses": [e.ip],
                    "conditions": { "ready": e.ready },
                    "targetRef": { "kind": "Pod", "uid": e.uid },
                })
            })
            .collect();
        let doc = serde_json::json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {
                "name": name,
                "namespace": "ns",
                "labels": { "kubernetes.io/service-name": service },
            },
            "addressType": "IPv4",
            "ports": [ { "port": port } ],
            "endpoints": eps,
        });
        serde_json::from_value(doc).expect("valid EndpointSlice fixture")
    }

    /// Drive the store through a one-slice relist (`Init`, `InitApply`,
    /// `InitDone`), the shape the watcher emits on first connect.
    pub(crate) fn sync_one_slice(store: &EndpointStore, slice: EndpointSlice) {
        store.apply_event(Event::Init);
        store.apply_event(Event::InitApply(slice));
        store.apply_event(Event::InitDone);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[test]
    fn not_synced_until_init_done() {
        let store = EndpointStore::new(None);
        assert!(!store.is_synced(), "starts not synced");

        store.apply_event(Event::Init);
        let slice = slice(
            "gw-abc",
            "gw",
            8080,
            &[TestEndpoint {
                uid: "uid-a",
                ip: "10.0.0.1",
                ready: true,
            }],
        );
        store.apply_event(Event::InitApply(slice));
        assert!(!store.is_synced(), "not synced until InitDone");

        store.apply_event(Event::InitDone);
        assert!(store.is_synced(), "synced after InitDone");
    }

    #[test]
    fn ready_endpoints_are_dialable_not_ready_only_rank() {
        let store = EndpointStore::new(None);
        let slice = slice(
            "gw-abc",
            "gw",
            8080,
            &[
                TestEndpoint {
                    uid: "uid-ready",
                    ip: "10.0.0.1",
                    ready: true,
                },
                TestEndpoint {
                    uid: "uid-unready",
                    ip: "10.0.0.2",
                    ready: false,
                },
            ],
        );
        sync_one_slice(&store, slice);

        let snap = store.snapshot();
        assert_eq!(snap.replicas.len(), 2, "both endpoints rank");
        let ready = ReplicaId::new(b"uid-ready".to_vec());
        let unready = ReplicaId::new(b"uid-unready".to_vec());
        assert!(snap.replicas.contains(&ready) && snap.replicas.contains(&unready));
        assert_eq!(
            snap.ready_addr.get(&ready),
            Some(&"10.0.0.1:8080".parse().expect("addr"))
        );
        assert!(
            !snap.ready_addr.contains_key(&unready),
            "a not-Ready endpoint is excluded from the dial set"
        );
    }

    #[test]
    fn delete_removes_a_slice_from_the_snapshot() {
        let store = EndpointStore::new(None);
        let s = slice(
            "gw-abc",
            "gw",
            8080,
            &[TestEndpoint {
                uid: "uid-a",
                ip: "10.0.0.1",
                ready: true,
            }],
        );
        sync_one_slice(&store, s.clone());
        assert_eq!(store.snapshot().replicas.len(), 1);

        store.apply_event(Event::Delete(s));
        assert!(
            store.snapshot().replicas.is_empty(),
            "deleting the only slice empties the snapshot"
        );
    }

    #[test]
    fn endpoint_without_uid_is_skipped() {
        let doc = serde_json::json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": { "name": "gw-x", "namespace": "ns" },
            "addressType": "IPv4",
            "ports": [ { "port": 8080 } ],
            "endpoints": [ { "addresses": ["10.0.0.9"], "conditions": { "ready": true } } ],
        });
        let s: EndpointSlice = serde_json::from_value(doc).expect("valid slice");
        let store = EndpointStore::new(None);
        sync_one_slice(&store, s);
        assert!(
            store.snapshot().replicas.is_empty(),
            "an endpoint with no targetRef.uid has no stable identity and is skipped"
        );
    }

    #[test]
    fn named_port_is_selected() {
        let doc = serde_json::json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": { "name": "gw-x", "namespace": "ns" },
            "addressType": "IPv4",
            "ports": [ { "name": "grpc", "port": 4317 }, { "name": "http", "port": 4318 } ],
            "endpoints": [ {
                "addresses": ["10.0.0.5"],
                "conditions": { "ready": true },
                "targetRef": { "kind": "Pod", "uid": "uid-a" }
            } ],
        });
        let s: EndpointSlice = serde_json::from_value(doc).expect("valid slice");
        let store = EndpointStore::new(Some("http".to_string()));
        sync_one_slice(&store, s);
        let snap = store.snapshot();
        let id = ReplicaId::new(b"uid-a".to_vec());
        assert_eq!(
            snap.ready_addr.get(&id),
            Some(&"10.0.0.5:4318".parse().expect("addr")),
            "the named port, not the first port, is dialed"
        );
    }
}
