//! ADR-1702 task 9: `StreamState::decode_gated` decompresses every payload on
//! the write gate and decodes exactly what `decode` decodes inline.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use ravel_cpu_gate::{CpuGateConfig, InstantClock, WriteGate, WriteSite};
use ravel_otap::encode::{DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder};
use ravel_otap::stream::{StreamConfig, StreamState};

fn metric(name: &str, points: &[(i64, f64)]) -> MetricRow {
    MetricRow {
        name: name.to_string(),
        kind: MetricKind::Gauge,
        data_points: points
            .iter()
            .map(|&(time_unix_nano, value)| DataPointRow {
                exemplars: vec![],
                time_unix_nano,
                value,
                flags: 0,
                attrs: vec![],
            })
            .collect(),
    }
}

/// Two batches on one stream, the second decoding against the IPC schema the
/// first established, through a gated and an ungated `StreamState`. The
/// decoded batches are equal, and the gate ran one `otap_decode` job per
/// payload with an inline floor of 0, so every decompression went through it.
///
/// Fails with `decompress_on_gate` decompressing inline whatever gate is set:
/// the count reads `(0, 0)`.
#[tokio::test]
async fn decode_gated_decompresses_every_payload_on_the_write_gate() {
    let gate = Arc::new(WriteGate::new(
        CpuGateConfig {
            inline_floor_bytes: 0,
            ..CpuGateConfig::with_permits(1)
        },
        Arc::new(InstantClock::new()),
    ));
    let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder construction");
    let batches = [
        encoder
            .encode_batch(1, &[metric("cpu.load", &[(1_000, 0.5), (2_000, 0.75)])])
            .expect("encode batch 1"),
        encoder
            .encode_batch(2, &[metric("cpu.load", &[(3_000, 0.25)])])
            .expect("encode batch 2"),
    ];
    let payloads: u64 = batches.iter().map(|b| b.arrow_payloads.len() as u64).sum();
    assert!(payloads > 2, "the fixture spans several payloads");

    let mut inline = StreamState::new(StreamConfig::default());
    let mut gated =
        StreamState::new(StreamConfig::default()).with_write_gate(Some(Arc::clone(&gate)));
    for batch in batches {
        let expected = inline.decode(batch.clone()).expect("inline decode");
        let actual = gated.decode_gated(batch).await.expect("gated decode");
        assert_eq!(actual.batch_id, expected.batch_id);
        assert_eq!(actual.payloads, expected.payloads);
    }
    let site = gate
        .snapshot()
        .sites
        .into_iter()
        .find(|s| s.site == WriteSite::OtapDecode)
        .expect("otap_decode site");
    assert_eq!((site.jobs, site.inline), (payloads, 0));
}
