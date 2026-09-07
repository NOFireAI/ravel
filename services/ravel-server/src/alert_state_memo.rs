//! Tenant-wide latest-alert-state memo (issue #1294).
//!
//! [`crate::alerting::AlertEvaluator`] derives each alert's current state by
//! folding the tenant's whole `Signal::Alerts` transition history to the most
//! recent record per `alert_id` (ADR-0040 decision 3). Nothing maintains that
//! signal (`Signal::Alerts` is absent from `maintain::MAINTAINED_SIGNALS`), so
//! the history only grows, and a fold that re-reads all of it every tick costs
//! `ceil(N/page)` LISTs plus `2N` GETs where `N` is the cumulative transition
//! count, independent of the rule count the evaluator actually needs.
//!
//! This memo is a derived cache of that fold at one durable, tenant-wide key
//! (`t/<tenant_hash>/a/state/latest`), deliberately outside the
//! `t/<tenant>/a/c/` commit prefix so neither the fold nor ravel-sql's `alerts`
//! table ever lists it. It is never source of truth: the transition records
//! remain the only durable state (ADR-0040 decision 3 is untouched, no record
//! format changes). Following the `sys/maintain/memo` precedent (ADR-0065),
//! everything here is advisory and reconstructible; a lost, stale, or corrupt
//! memo costs a rescan, never correctness, because the reader always re-lists
//! the hours at or after the memo's watermark and re-folds them over the memo.
//!
//! # Wire format and versioning
//!
//! The memo carries an explicit `format_version` from day one. The reader is a
//! supported-set gate accepting exactly `{1}` and refusing `0` and any future
//! version it does not understand, so a forward-incompatible writer can never
//! be mistaken for a valid memo: an unsupported version falls back to a full
//! fold exactly as an absent or corrupt memo does. The writer is the alert
//! lease holder only, a single writer per key, using [`PutMode::Overwrite`].

use std::collections::HashMap;

use bytes::Bytes;
use ravel_alerting::{AlertId, AlertRecord, AlertState};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError};
use ravel_types::TenantHash;
use serde::{Deserialize, Serialize};

/// The only `format_version` this build writes and the sole member of the set
/// its reader accepts. Bumping it is an ADR-gated change; a reader that meets a
/// version outside its supported set falls back to a full fold.
pub const ALERT_STATE_MEMO_FORMAT_VERSION: u32 = 1;

/// The memo object key for a tenant's alert state.
///
/// Under the tenant's alert keyspace (`Signal::Alerts` prefix `a`) but outside
/// the `c/<shard>/` commit prefix that
/// [`crate::alerting::AlertEvaluator::load_latest_records`] folds and that
/// ravel-sql's `alerts` table reads, so neither ever lists this key or mistakes
/// it for a commit record. It is mutable, derived coordination state, not an
/// immutable data/commit object, so the object-key immutability rule does not
/// apply to it.
pub fn alert_state_memo_key(tenant: &TenantHash) -> String {
    format!("t/{}/a/state/latest", tenant.to_hex())
}

/// Decoding a memo failed. Both variants are non-fatal at the call site: the
/// evaluator logs and falls back to a full fold, then rewrites the memo.
#[derive(Debug, thiserror::Error)]
pub enum MemoError {
    /// The bytes are not a well-formed memo (truncated, not JSON, a field of
    /// the wrong type, or an `alert_id`/`state` that does not decode).
    #[error("alert state memo decode: {0}")]
    Decode(String),
    /// The memo is well-formed but carries a `format_version` this reader does
    /// not support.
    #[error(
        "unsupported alert state memo format_version {found}; this reader supports \
         {{{ALERT_STATE_MEMO_FORMAT_VERSION}}}"
    )]
    UnsupportedVersion { found: u32 },
}

/// The folded latest-state-per-`alert_id` snapshot, plus the watermark hour the
/// reader must re-list at or after to catch any transition written since the
/// memo was stamped.
#[derive(Debug, Clone)]
pub struct AlertStateMemo {
    /// `hour_bucket(now_ns)` at the moment this memo was written. Every alert
    /// record whose ingest hour is strictly below this is fully represented in
    /// `records`; the reader re-lists hours at or after it to fold in anything
    /// newer. Never above the writer's own tick hour, so no writer ever stamps
    /// a watermark past a record it has not folded.
    pub watermark_hour: u32,
    /// Latest record per `alert_id` as of `watermark_hour`.
    pub records: HashMap<AlertId, AlertRecord>,
}

/// The version header parsed before the full body, so an unsupported version is
/// reported as [`MemoError::UnsupportedVersion`] rather than a decode failure of
/// a body whose shape it does not share. Extra fields are ignored (no
/// `deny_unknown_fields`).
#[derive(Deserialize)]
struct WireHeader {
    format_version: u32,
}

/// The on-object memo shape. `AlertRecord` and `AlertId` carry no serde derives,
/// so records are mirrored field-for-field with `alert_id` and `state` in their
/// canonical string forms (the same forms the RLOG `attrs` use).
#[derive(Serialize, Deserialize)]
struct WireMemo {
    format_version: u32,
    watermark_hour: u32,
    records: Vec<WireRecord>,
}

#[derive(Serialize, Deserialize)]
struct WireRecord {
    /// Lowercase hex, as [`AlertId::to_hex`].
    alert_id: String,
    rule_id: String,
    /// `firing`/`resolved`/`pending`/`suppressed`, as [`AlertState::as_str`].
    state: String,
    generation: u32,
    ts_ns: i64,
    labels: Vec<(String, String)>,
    annotations: Vec<(String, String)>,
    body: String,
}

impl WireRecord {
    fn from_record(record: &AlertRecord) -> Self {
        WireRecord {
            alert_id: record.alert_id.to_hex(),
            rule_id: record.rule_id.clone(),
            state: record.state.as_str().to_string(),
            generation: record.generation,
            ts_ns: record.ts_ns,
            labels: record.labels.clone(),
            annotations: record.annotations.clone(),
            body: record.body.clone(),
        }
    }

    fn into_record(self) -> Result<(AlertId, AlertRecord), MemoError> {
        let alert_id =
            AlertId::from_hex(&self.alert_id).map_err(|err| MemoError::Decode(err.to_string()))?;
        let state = AlertState::parse(&self.state)
            .ok_or_else(|| MemoError::Decode(format!("unknown alert state {:?}", self.state)))?;
        let record = AlertRecord {
            alert_id,
            rule_id: self.rule_id,
            state,
            generation: self.generation,
            ts_ns: self.ts_ns,
            labels: self.labels,
            annotations: self.annotations,
            body: self.body,
        };
        Ok((alert_id, record))
    }
}

/// Serialize a memo to its on-object bytes, stamping the current
/// [`ALERT_STATE_MEMO_FORMAT_VERSION`].
pub fn encode(memo: &AlertStateMemo) -> Vec<u8> {
    let wire = WireMemo {
        format_version: ALERT_STATE_MEMO_FORMAT_VERSION,
        watermark_hour: memo.watermark_hour,
        records: memo.records.values().map(WireRecord::from_record).collect(),
    };
    // A `WireMemo` of owned Rust scalars and strings cannot fail to serialize;
    // treat any error as a decode-class failure rather than panicking.
    serde_json::to_vec(&wire).unwrap_or_default()
}

/// Parse memo bytes, gating on the supported version set before the body.
///
/// Never panics: a truncated or malformed object, an `alert_id`/`state` that
/// does not decode, or a version outside the supported set is a typed error the
/// caller turns into a full fold, not a crash.
pub fn decode(bytes: &[u8]) -> Result<AlertStateMemo, MemoError> {
    let header: WireHeader =
        serde_json::from_slice(bytes).map_err(|err| MemoError::Decode(err.to_string()))?;
    if header.format_version != ALERT_STATE_MEMO_FORMAT_VERSION {
        return Err(MemoError::UnsupportedVersion {
            found: header.format_version,
        });
    }
    let wire: WireMemo =
        serde_json::from_slice(bytes).map_err(|err| MemoError::Decode(err.to_string()))?;
    let mut records = HashMap::with_capacity(wire.records.len());
    for wire_record in wire.records {
        let (alert_id, record) = wire_record.into_record()?;
        records.insert(alert_id, record);
    }
    Ok(AlertStateMemo {
        watermark_hour: wire.watermark_hour,
        records,
    })
}

/// Read and decode the tenant's memo.
///
/// `Ok(None)` when no memo exists yet (the cold-start case). `Err` on any store
/// failure other than not-found, and on a corrupt or unsupported-version memo,
/// so the caller can log the distinction; in every non-`Some` case the caller
/// falls back to a full fold.
pub async fn read_alert_state_memo(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> anyhow::Result<Option<AlertStateMemo>> {
    let key = alert_state_memo_key(tenant);
    let outcome = match store.get(&key, GetRange::Full).await {
        Ok(outcome) => outcome,
        Err(StoreError::NotFound) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    Ok(Some(decode(&outcome.data)?))
}

/// Overwrite the tenant's memo. Called only by the alert lease holder, so this
/// is a single writer per key; `PutMode::Overwrite` because a stale memo is
/// harmless (the reader re-folds the tail) and last-write-wins during a brief
/// two-holder lease handover leaves correct derived state either way.
pub async fn write_alert_state_memo(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    memo: &AlertStateMemo,
) -> anyhow::Result<()> {
    let key = alert_state_memo_key(tenant);
    let body = Bytes::from(encode(memo));
    store
        .put(
            &key,
            body,
            PutOptions {
                mode: PutMode::Overwrite,
                checksum: None,
            },
        )
        .await?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn record(id: u8, state: AlertState, ts_ns: i64) -> AlertRecord {
        AlertRecord {
            alert_id: AlertId([id; 16]),
            rule_id: format!("rule-{id}"),
            state,
            generation: 0,
            ts_ns,
            labels: vec![("severity".to_string(), "page".to_string())],
            annotations: vec![("summary".to_string(), "high".to_string())],
            body: format!("alert {id}"),
        }
    }

    #[test]
    fn round_trip_preserves_every_field() {
        let mut records = HashMap::new();
        let a = record(1, AlertState::Firing, 100);
        let b = record(2, AlertState::Resolved, 200);
        records.insert(a.alert_id, a.clone());
        records.insert(b.alert_id, b.clone());
        let memo = AlertStateMemo {
            watermark_hour: 42,
            records,
        };

        let decoded = decode(&encode(&memo)).expect("round trips");
        assert_eq!(decoded.watermark_hour, 42);
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records.get(&a.alert_id), Some(&a));
        assert_eq!(decoded.records.get(&b.alert_id), Some(&b));
    }

    #[test]
    fn truncated_bytes_are_a_decode_error_not_a_panic() {
        let memo = AlertStateMemo {
            watermark_hour: 1,
            records: HashMap::new(),
        };
        let bytes = encode(&memo);
        let err = decode(&bytes[..bytes.len() / 2]).expect_err("truncation rejected");
        assert!(matches!(err, MemoError::Decode(_)), "got {err:?}");
    }

    #[test]
    fn version_zero_is_unsupported_not_decode() {
        let bytes = br#"{"format_version":0,"watermark_hour":1,"records":[]}"#;
        let err = decode(bytes).expect_err("version 0 rejected");
        assert!(
            matches!(err, MemoError::UnsupportedVersion { found: 0 }),
            "got {err:?}"
        );
    }

    #[test]
    fn future_version_is_unsupported_not_decode() {
        let bytes = br#"{"format_version":2,"watermark_hour":1,"records":[]}"#;
        let err = decode(bytes).expect_err("version 2 rejected");
        assert!(
            matches!(err, MemoError::UnsupportedVersion { found: 2 }),
            "got {err:?}"
        );
    }

    #[test]
    fn unknown_state_string_is_a_decode_error() {
        let bytes = br#"{"format_version":1,"watermark_hour":1,"records":[
            {"alert_id":"00000000000000000000000000000000","rule_id":"r","state":"exploded",
             "generation":0,"ts_ns":1,"labels":[],"annotations":[],"body":"b"}]}"#;
        let err = decode(bytes).expect_err("unknown state rejected");
        assert!(matches!(err, MemoError::Decode(_)), "got {err:?}");
    }
}
