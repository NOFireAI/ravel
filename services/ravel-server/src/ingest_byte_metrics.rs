//! Per-tenant wire (compressed) request-body byte counting for OTLP ingest
//! (ADR-0084 decision 5).
//!
//! Layer-2 admission charges the byte-rate bucket on the *decompressed* size
//! for a compressed request (ADR-0084 decision 4), and that charged quantity is
//! what `ravel_admission_admitted_bytes_total` reports. On its own that number
//! cannot distinguish a tenant that doubled its telemetry from one that turned
//! compression off: both raise the charged (decompressed) bytes the same way.
//! This collector records the *wire* bytes each tenant actually sent alongside
//! it, so `/metrics` exposes both and their ratio is the tenant's effective
//! compression factor.
//!
//! It is a `ravel-server`-local counter, separate from the `ravel-ingest`
//! `AdmissionController`'s per-tenant usage: the byte-rate bucket and its
//! admitted/charged counters live there, and the charged basis is the
//! decompressed size, so the wire quantity has no home in that snapshot.

use std::collections::HashMap;

use parking_lot::Mutex;
use ravel_types::{Signal, TenantHash, TenantId};

/// Accumulated wire request-body bytes per `(tenant, signal)`. Keyed by the
/// tenant hash (not the full id), matching how the admission `/metrics` family
/// folds tenants, so the rendered cardinality is bounded the same way.
#[derive(Default)]
pub struct IngestByteMetrics {
    rows: Mutex<HashMap<(TenantHash, Signal), u64>>,
}

/// One `(tenant, signal)` row of accumulated wire bytes, for the `/metrics`
/// renderer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantWireBytes {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub wire_bytes_total: u64,
}

impl IngestByteMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `bytes` of wire request body (the on-the-wire size, i.e. the
    /// compressed size when the client compressed) to `(tenant, signal)`.
    /// Called on the admitted path only, right after the byte-rate charge
    /// succeeds, so it counts exactly what a tenant successfully ingested.
    pub fn record_wire_bytes(&self, tenant: &TenantId, signal: Signal, bytes: u64) {
        let mut rows = self.rows.lock();
        *rows.entry((tenant.hash(), signal)).or_insert(0) += bytes;
    }

    /// A point-in-time copy of every accumulated row, for `/metrics` rendering.
    pub fn snapshot(&self) -> Vec<TenantWireBytes> {
        self.rows
            .lock()
            .iter()
            .map(
                |(&(tenant_hash, signal), &wire_bytes_total)| TenantWireBytes {
                    tenant_hash,
                    signal,
                    wire_bytes_total,
                },
            )
            .collect()
    }
}
