//! Unnamed-since markers (ADR-1133): the durable time a sweep first saw a
//! delete candidate that the live catalog HEAD no longer names.
//!
//! A query resolves one HEAD and reads what it names for up to
//! `max_query_duration`, and a resolver can be served a cached HEAD for up to
//! `head_cache_ttl` after the fold replaced it. The protection horizon and the
//! HEAD-reachability blocker say nothing about when HEAD stopped naming an
//! object, so the retention and superseded-input sweeps write one of these the
//! first time a pass finds a candidate unnamed, and delete only once it is
//! older than the pinned-query window ([`PinnedQueryWindow`]).
//!
//! A marker is immutable: written once with `CreateIfAbsent`, deleted after the
//! objects it gates, or replaced only by deleting it first (a re-named
//! candidate, or one whose anchor no longer matches). Its anchor time is the
//! writer's own clock reading, never a store `last_modified`.
//!
//! The gate itself lives in [`crate::reachability::SnapshotReachability`];
//! this module owns the body codec, the window arithmetic and the orphan
//! reaper.

use std::collections::HashSet;

use prost::Message;
use ravel_commit::keys;
use ravel_object_store::{
    ObjectMeta, ObjectStoreBackend, PutOptions, StoreError, UploadChecksum, list_all,
};
use ravel_proto::commit::v1::UnnamedSinceMarker;
use ravel_types::{Signal, TenantHash};

use crate::clock::Clock;
use crate::config::CompactorConfig;
use crate::error::Result;

/// The only marker body version this build writes and reads.
pub const UNNAMED_MARKER_FORMAT_VERSION: u32 = 1;

/// Which sweep a marker gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarkerKind {
    /// The retention sweep's whole tombstoned bucket, anchored on `retire.tmb`.
    Retention,
    /// The superseded-input sweep's chain groups entered from one compaction or
    /// rewrite record, anchored on that record.
    Superseded,
}

impl MarkerKind {
    fn to_wire(self) -> u32 {
        match self {
            MarkerKind::Retention => 1,
            MarkerKind::Superseded => 2,
        }
    }

    fn from_wire(kind: u32) -> Option<Self> {
        match kind {
            1 => Some(MarkerKind::Retention),
            2 => Some(MarkerKind::Superseded),
            _ => None,
        }
    }
}

/// The identity of the tombstone or record a marker was written for. A marker
/// whose anchor differs from the one present now counts as absent (ADR-1133
/// decision 2).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarkerAnchor {
    pub kind: MarkerKind,
    /// The `retire.tmb` or record key.
    pub key: String,
    /// `retired_at_ns` for a tombstone, `created_unix_ns` for a record.
    pub anchor_unix_ns: i64,
    /// The anchor's store version, from the GET that read it.
    pub version: String,
}

/// A decoded marker body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnnamedMarker {
    /// The writing sweeper's clock, read after the HEAD GET that found the
    /// candidate unnamed had returned.
    pub observed_unix_ns: i64,
    pub anchor: MarkerAnchor,
    /// The HEAD version that observation read; forensics only.
    pub head_version: String,
}

/// Why a marker body was refused. Every variant blocks the delete it gates.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MarkerDecodeError {
    #[error("unnamed-since marker body does not decode: {0}")]
    Undecodable(String),
    #[error(
        "unnamed-since marker format_version {0} is not one this build reads (it reads {UNNAMED_MARKER_FORMAT_VERSION})"
    )]
    UnsupportedFormatVersion(u32),
    #[error("unnamed-since marker anchor_kind {0} is not a known kind")]
    UnknownAnchorKind(u32),
}

impl UnnamedMarker {
    pub fn encode(&self) -> Vec<u8> {
        UnnamedSinceMarker {
            format_version: UNNAMED_MARKER_FORMAT_VERSION,
            observed_unix_ns: self.observed_unix_ns,
            anchor_kind: self.anchor.kind.to_wire(),
            anchor_key: self.anchor.key.clone(),
            anchor_unix_ns: self.anchor.anchor_unix_ns,
            anchor_version: self.anchor.version.clone(),
            head_version: self.head_version.clone(),
        }
        .encode_to_vec()
    }

    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, MarkerDecodeError> {
        let wire = UnnamedSinceMarker::decode(bytes)
            .map_err(|e| MarkerDecodeError::Undecodable(e.to_string()))?;
        if wire.format_version != UNNAMED_MARKER_FORMAT_VERSION {
            return Err(MarkerDecodeError::UnsupportedFormatVersion(
                wire.format_version,
            ));
        }
        let kind = MarkerKind::from_wire(wire.anchor_kind)
            .ok_or(MarkerDecodeError::UnknownAnchorKind(wire.anchor_kind))?;
        Ok(UnnamedMarker {
            observed_unix_ns: wire.observed_unix_ns,
            anchor: MarkerAnchor {
                kind,
                key: wire.anchor_key,
                anchor_unix_ns: wire.anchor_unix_ns,
                version: wire.anchor_version,
            },
            head_version: wire.head_version,
        })
    }
}

/// The pinned-query window (ADR-1133 decision 3): how long after a sweep first
/// saw a candidate unnamed a query that pinned an earlier HEAD can still be
/// reading it, plus the clock offsets between the processes involved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinnedQueryWindow {
    pub max_query_duration_ns: i64,
    pub head_cache_ttl_ns: i64,
    pub clock_skew_allowance_ns: i64,
}

impl PinnedQueryWindow {
    pub fn from_config(config: &CompactorConfig) -> Self {
        Self {
            max_query_duration_ns: config.max_query_duration_ns,
            head_cache_ttl_ns: config.head_cache_ttl_ns,
            clock_skew_allowance_ns: config.clock_skew_allowance_ns,
        }
    }

    /// `observed_unix_ns + max_query_duration + head_cache_ttl +
    /// 4 * clock_skew_allowance <= now_ns`, saturating so an absurd input
    /// holds rather than wraps open.
    pub fn has_elapsed(&self, observed_unix_ns: i64, now_ns: i64) -> bool {
        observed_unix_ns
            .saturating_add(self.max_query_duration_ns)
            .saturating_add(self.head_cache_ttl_ns)
            .saturating_add(self.clock_skew_allowance_ns.saturating_mul(4))
            <= now_ns
    }
}

/// Write a marker body with `CreateIfAbsent` and a CRC32C checksum.
pub(crate) async fn put_marker(
    store: &dyn ObjectStoreBackend,
    key: &str,
    marker: &UnnamedMarker,
) -> std::result::Result<(), StoreError> {
    let payload = marker.encode();
    let checksum = UploadChecksum::Crc32c(crc32c::crc32c(&payload));
    let opts = PutOptions::create_if_absent().with_checksum(checksum);
    store.put(key, payload.into(), opts).await.map(|_| ())
}

/// What one orphan-marker reap did (docs/deletion-and-gc.md, orphan-marker
/// rule).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarkerReapOutcome {
    /// Marker keys the signal-wide LIST returned.
    pub listed: usize,
    /// Markers deleted: anchor gone and `observed_unix_ns` older than the
    /// protection horizon.
    pub reaped: usize,
    /// Keys under `maint/unn/` that do not parse as a marker key: counted and
    /// left in place.
    pub unparseable: usize,
    /// Markers whose anchor is gone but whose body could not be read or
    /// decoded: left in place.
    pub unreadable: usize,
    /// Markers whose anchor HEAD, body GET or DELETE failed: counted, left in
    /// place, and the reap moves on to the next key.
    pub failed: usize,
    /// Anchor HEADs, marker body GETs and marker DELETEs the reap issued, on
    /// top of the LIST.
    pub head_requests: usize,
    pub get_requests: usize,
    pub delete_requests: usize,
}

/// The orphan-marker rule: LIST every marker of one `(tenant, signal)`, across
/// every shard, and delete each whose anchor no longer exists and whose
/// `observed_unix_ns` is older than `protection_horizon_ns` on this sweeper's
/// clock. A key that does not parse, or whose HEAD, GET or DELETE fails, is
/// counted and skipped. A dry run counts what it would reap and deletes
/// nothing.
pub async fn reap_orphan_unnamed_markers(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<MarkerReapOutcome> {
    let listed = list_all(store, &keys::unnamed_marker_prefix(tenant, signal)).await?;
    Ok(reap_listed(
        store,
        clock,
        config,
        tenant,
        signal,
        &listed,
        &HashSet::new(),
        &HashSet::new(),
    )
    .await)
}

/// [`reap_orphan_unnamed_markers`] over a listing the caller already holds.
/// Anchors in `known_present` were read by the calling pass and are not
/// re-checked, and keys in `skip` (markers the calling pass wrote or deleted)
/// are left alone.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reap_listed(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    tenant: &TenantHash,
    signal: Signal,
    listed: &[ObjectMeta],
    known_present: &HashSet<String>,
    skip: &HashSet<String>,
) -> MarkerReapOutcome {
    let mut outcome = MarkerReapOutcome {
        listed: listed.len(),
        ..MarkerReapOutcome::default()
    };
    for meta in listed {
        if skip.contains(&meta.key) {
            continue;
        }
        let anchor_key = match keys::parse_unnamed_marker_key(&meta.key) {
            Ok(parsed) if parsed.tenant_hash == *tenant && parsed.signal == signal => {
                parsed.anchor_key()
            }
            Ok(_) => Err(keys::KeyError::Malformed {
                key: meta.key.clone(),
                reason: "marker key names another tenant or signal".to_string(),
            }),
            Err(e) => Err(e),
        };
        let anchor_key = match anchor_key {
            Ok(k) => k,
            Err(error) => {
                outcome.unparseable += 1;
                tracing::warn!(
                    key = %meta.key,
                    %error,
                    "unnamed-marker reaper: a key under maint/unn/ is not a marker key; counted \
                     and left in place"
                );
                continue;
            }
        };
        if known_present.contains(&anchor_key) {
            continue;
        }
        outcome.head_requests += 1;
        match store.head(&anchor_key).await {
            Ok(_) => continue,
            Err(StoreError::NotFound) => {}
            Err(error) => {
                reap_failed(&mut outcome, &meta.key, "anchor HEAD", &error);
                continue;
            }
        }
        outcome.get_requests += 1;
        let body = match store
            .get(&meta.key, ravel_object_store::GetRange::Full)
            .await
        {
            Ok(got) => got.data,
            Err(StoreError::NotFound) => continue,
            Err(error) => {
                reap_failed(&mut outcome, &meta.key, "marker GET", &error);
                continue;
            }
        };
        let marker = match UnnamedMarker::decode(body.as_ref()) {
            Ok(m) => m,
            Err(error) => {
                outcome.unreadable += 1;
                tracing::warn!(
                    key = %meta.key,
                    %error,
                    "unnamed-marker reaper: an orphan marker's body cannot be read; left in place"
                );
                continue;
            }
        };
        let now = clock.now_ns();
        if marker
            .observed_unix_ns
            .saturating_add(config.protection_horizon_ns)
            >= now
        {
            continue;
        }
        if !config.dry_run {
            outcome.delete_requests += 1;
            if let Err(error) = store.delete(&meta.key).await {
                reap_failed(&mut outcome, &meta.key, "marker DELETE", &error);
                continue;
            }
        }
        outcome.reaped += 1;
    }
    outcome
}

fn reap_failed(outcome: &mut MarkerReapOutcome, key: &str, op: &str, error: &StoreError) {
    outcome.failed += 1;
    tracing::warn!(
        key = %key,
        op,
        %error,
        "unnamed-marker reaper: a request failed; marker left in place for a later pass"
    );
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn sample() -> UnnamedMarker {
        UnnamedMarker {
            observed_unix_ns: 1_234,
            anchor: MarkerAnchor {
                kind: MarkerKind::Superseded,
                key: "t/aa/m/c/0001/20260101T00/l1.0011223344556677.cmt".to_string(),
                anchor_unix_ns: 99,
                version: "v7".to_string(),
            },
            head_version: "h3".to_string(),
        }
    }

    #[test]
    fn marker_body_round_trips() {
        let m = sample();
        assert_eq!(UnnamedMarker::decode(&m.encode()).expect("decode"), m);
    }

    #[test]
    fn decode_refuses_an_unknown_format_version() {
        let mut wire = UnnamedSinceMarker::decode(sample().encode().as_slice()).expect("wire");
        wire.format_version = UNNAMED_MARKER_FORMAT_VERSION + 1;
        assert_eq!(
            UnnamedMarker::decode(&wire.encode_to_vec()),
            Err(MarkerDecodeError::UnsupportedFormatVersion(
                UNNAMED_MARKER_FORMAT_VERSION + 1
            ))
        );
        wire.format_version = 0;
        assert_eq!(
            UnnamedMarker::decode(&wire.encode_to_vec()),
            Err(MarkerDecodeError::UnsupportedFormatVersion(0))
        );
    }

    #[test]
    fn decode_refuses_an_unknown_anchor_kind() {
        let mut wire = UnnamedSinceMarker::decode(sample().encode().as_slice()).expect("wire");
        wire.anchor_kind = 3;
        assert_eq!(
            UnnamedMarker::decode(&wire.encode_to_vec()),
            Err(MarkerDecodeError::UnknownAnchorKind(3))
        );
    }

    #[test]
    fn decode_refuses_garbage() {
        assert!(matches!(
            UnnamedMarker::decode(&[0xff, 0xff, 0xff]),
            Err(MarkerDecodeError::Undecodable(_))
        ));
    }

    proptest::proptest! {
        #[test]
        fn decode_never_panics(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..64)) {
            let _ = UnnamedMarker::decode(&bytes);
        }
    }

    #[test]
    fn window_sums_every_term_with_four_skews() {
        let w = PinnedQueryWindow {
            max_query_duration_ns: 1_000,
            head_cache_ttl_ns: 100,
            clock_skew_allowance_ns: 10,
        };
        assert!(w.has_elapsed(5, 5 + 1_140));
        assert!(!w.has_elapsed(5, 5 + 1_139));
        assert!(!w.has_elapsed(i64::MAX - 1, i64::MAX - 1));
    }
}
