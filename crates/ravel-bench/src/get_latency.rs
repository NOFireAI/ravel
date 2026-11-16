//! The `get_latency` field of the `sql_latency_bench` report (issue #2976):
//! every GET the in-process measurement issued, as time to first byte and time
//! to last byte, by read phase and requested size.
//!
//! [`GetLatencyCapture`] wraps the store the measurement reads through in an
//! [`InstrumentedStore`], so the figures come from
//! [`ravel_object_store::get_latency`], which documents what each duration
//! covers, how retries are counted, the size buckets, and the histogram
//! resolution. Latencies are wall-clock per request on this process's
//! monotonic clock, in milliseconds with microsecond resolution.

use std::sync::Arc;

use ravel_object_store::get_latency::{
    GetLatencySnapshot, LatencyQuantiles, SIZE_BUCKET_LABELS, size_bucket_bounds,
};
use ravel_object_store::{InstrumentedStore, ObjectStoreBackend, StoreMetrics};
use serde::{Deserialize, Serialize};

/// A store whose GETs are timed, and the handle to read the timings back.
pub struct GetLatencyCapture {
    store: Arc<dyn ObjectStoreBackend>,
    metrics: Arc<StoreMetrics>,
}

impl GetLatencyCapture {
    /// Time every GET issued through [`store`](Self::store), which forwards to
    /// `inner` unchanged.
    pub fn wrap(inner: &Arc<dyn ObjectStoreBackend>) -> Self {
        let instrumented = InstrumentedStore::new(Arc::clone(inner));
        let metrics = instrumented.metrics();
        GetLatencyCapture {
            store: Arc::new(instrumented),
            metrics,
        }
    }

    pub fn store(&self) -> &Arc<dyn ObjectStoreBackend> {
        &self.store
    }

    /// Every GET timed so far.
    pub fn report(&self) -> GetLatencyReport {
        GetLatencyReport::from_snapshot(&self.metrics.get_latency().snapshot())
    }
}

/// Every read phase, in `GetPhase::ALL` order (`resolve`, `plan`, `probe`,
/// `scan`, `unattributed`), each with every size bucket, empty ones included,
/// so the shape is the same for every report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetLatencyReport {
    pub phases: Vec<PhaseGetLatency>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PhaseGetLatency {
    pub phase: String,
    /// Successful GETs, in ascending size order.
    pub buckets: Vec<SizeBucketGetLatency>,
    /// GETs that returned an error, in no size bucket.
    pub failed: FailedGetLatency,
}

/// The successful GETs whose requested byte count fell in
/// `[min_bytes, max_bytes]` (`max_bytes` null for the overflow bucket).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SizeBucketGetLatency {
    pub bucket: String,
    pub min_bytes: u64,
    pub max_bytes: Option<u64>,
    pub count: u64,
    /// Null when `count` is 0.
    pub first_byte_ms: Option<LatencyMs>,
    /// Null when `count` is 0.
    pub total_ms: Option<LatencyMs>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FailedGetLatency {
    pub count: u64,
    /// Null when `count` is 0.
    pub total_ms: Option<LatencyMs>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct LatencyMs {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}

fn ms(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}

fn latency_ms(count: u64, q: &LatencyQuantiles) -> Option<LatencyMs> {
    (count > 0).then(|| LatencyMs {
        p50: ms(q.p50_micros),
        p90: ms(q.p90_micros),
        p99: ms(q.p99_micros),
        max: ms(q.max_micros),
    })
}

impl GetLatencyReport {
    pub fn from_snapshot(snapshot: &GetLatencySnapshot) -> Self {
        let phases = snapshot
            .phases
            .iter()
            .map(|phase| PhaseGetLatency {
                phase: phase.phase.name().to_string(),
                buckets: phase
                    .buckets
                    .iter()
                    .enumerate()
                    .map(|(index, bucket)| {
                        let (min_bytes, max_bytes) = size_bucket_bounds(index);
                        SizeBucketGetLatency {
                            bucket: SIZE_BUCKET_LABELS[index].to_string(),
                            min_bytes,
                            max_bytes,
                            count: bucket.count,
                            first_byte_ms: latency_ms(bucket.count, &bucket.first_byte),
                            total_ms: latency_ms(bucket.count, &bucket.total),
                        }
                    })
                    .collect(),
                failed: FailedGetLatency {
                    count: phase.failed_count,
                    total_ms: latency_ms(phase.failed_count, &phase.failed_total),
                },
            })
            .collect();
        GetLatencyReport { phases }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ravel_object_store::GetRange;
    use ravel_object_store::PutOptions;
    use ravel_object_store::get_latency::{GetPhase, InGetPhase, SIZE_BUCKET_COUNT};
    use ravel_object_store::memory::MemoryStore;

    /// The serialized field carries every phase by name, each with every size
    /// bucket and its count, `first_byte_ms` and `total_ms` quantiles, and the
    /// GETs land where they were issued: two 5 KiB scan GETs, one unscoped
    /// 100-byte GET.
    #[tokio::test]
    async fn the_report_field_is_per_phase_per_bucket() {
        let memory: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        memory
            .put(
                "seg",
                Bytes::from(vec![1u8; 10 * 1024]),
                PutOptions::default(),
            )
            .await
            .expect("put");
        let capture = GetLatencyCapture::wrap(&memory);
        let store = capture.store();
        async {
            for _ in 0..2 {
                store
                    .get("seg", GetRange::Range(0, 5 * 1024))
                    .await
                    .expect("scan get");
            }
        }
        .in_get_phase(GetPhase::Scan)
        .await;
        store
            .get("seg", GetRange::Range(0, 100))
            .await
            .expect("unscoped get");

        let json = serde_json::to_value(capture.report()).expect("serialize");
        let phases = json["phases"].as_array().expect("phases array");
        let names: Vec<&str> = phases
            .iter()
            .map(|p| p["phase"].as_str().expect("phase name"))
            .collect();
        assert_eq!(names, ["resolve", "plan", "probe", "scan", "unattributed"]);
        for phase in phases {
            let buckets = phase["buckets"].as_array().expect("buckets array");
            assert_eq!(buckets.len(), SIZE_BUCKET_COUNT);
            assert_eq!(buckets[0]["bucket"], "lt_4KiB");
            assert_eq!(buckets[SIZE_BUCKET_COUNT - 1]["bucket"], "gt_64MiB");
            assert_eq!(
                buckets[SIZE_BUCKET_COUNT - 1]["max_bytes"],
                serde_json::Value::Null
            );
            assert!(phase["failed"]["count"].is_u64());
        }

        let scan = &phases[3]["buckets"][1];
        assert_eq!(scan["bucket"], "4KiB_8KiB");
        assert_eq!(scan["min_bytes"], 4096);
        assert_eq!(scan["max_bytes"], 8191);
        assert_eq!(scan["count"], 2);
        for field in ["first_byte_ms", "total_ms"] {
            for q in ["p50", "p90", "p99", "max"] {
                assert!(scan[field][q].is_f64(), "{field}.{q} in {scan}");
            }
        }
        let unattributed = &phases[4]["buckets"][0];
        assert_eq!(unattributed["count"], 1);
        assert!(unattributed["total_ms"]["max"].is_f64());
        let empty = &phases[0]["buckets"][0];
        assert_eq!(empty["count"], 0);
        assert_eq!(empty["first_byte_ms"], serde_json::Value::Null);
        assert_eq!(empty["total_ms"], serde_json::Value::Null);
        let counted: u64 = phases
            .iter()
            .flat_map(|p| p["buckets"].as_array().expect("buckets").iter())
            .map(|b| b["count"].as_u64().expect("count"))
            .sum();
        assert_eq!(counted, 3, "no GET counted twice or dropped");
    }
}
