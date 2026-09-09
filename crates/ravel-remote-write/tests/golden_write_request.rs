//! End-to-end golden test: a snappy-compressed, protobuf-encoded RW1
//! `WriteRequest` shaped like a real Prometheus client payload, decoded and
//! normalized through the crate's public API in one pass. Unit tests in
//! `src/rw1.rs` and `src/normalize.rs` cover each stage in isolation; this
//! test exists to catch a break in how the two stages compose.
#![allow(clippy::expect_used)]

use prost::Message;
use ravel_otlp::IngestLimits;
use ravel_remote_write::decode_write_request;
use ravel_remote_write::normalize::normalize_resolved;
use ravel_remote_write::proto::prometheus::{Label, Sample, TimeSeries, WriteRequest};
use ravel_types::TenantId;

fn compress(bytes: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(bytes)
        .expect("compress fixture")
}

fn label(name: &str, value: &str) -> Label {
    Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}

#[test]
fn decodes_and_normalizes_a_realistic_write_request() {
    let req = WriteRequest {
        timeseries: vec![
            TimeSeries {
                labels: vec![
                    label("__name__", "process_cpu_seconds_total"),
                    label("job", "svc"),
                    label("instance", "10.0.0.1:9090"),
                ],
                samples: vec![
                    Sample {
                        value: 12.5,
                        timestamp: 1_700_000_000_000,
                    },
                    Sample {
                        value: 13.1,
                        timestamp: 1_700_000_015_000,
                    },
                ],
                exemplars: vec![],
                histograms: vec![],
            },
            TimeSeries {
                labels: vec![
                    label("__name__", "up"),
                    label("job", "svc"),
                    label("instance", "10.0.0.1:9090"),
                ],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1_700_000_000_000,
                }],
                exemplars: vec![],
                histograms: vec![],
            },
        ],
        metadata: vec![],
    };
    let body = compress(&req.encode_to_vec());

    let resolved = decode_write_request(&body, 10_000_000).expect("decode golden fixture");
    assert_eq!(resolved.series.len(), 2);

    let tenant = TenantId::new("t-golden");
    let out = normalize_resolved(
        &tenant,
        resolved,
        &IngestLimits::default(),
        1_700_000_020_000_000_000,
    );

    assert!(
        out.rejected.is_empty(),
        "unexpected rejections: {:?}",
        out.rejected
    );
    assert_eq!(out.points.len(), 3);
    assert_eq!(
        out.points[0].labels.get("__name__"),
        Some("process_cpu_seconds_total")
    );
    assert_eq!(out.points[0].sample.value, 12.5);
    assert_eq!(out.points[1].sample.value, 13.1);
    assert_eq!(out.points[2].labels.get("__name__"), Some("up"));

    // Same tenant/metric/labels must yield a stable series id across the
    // two samples of the first series.
    assert_eq!(out.points[0].series_id, out.points[1].series_id);
    assert_ne!(out.points[0].series_id, out.points[2].series_id);
}
