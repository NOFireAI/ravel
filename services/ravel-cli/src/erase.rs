//! `ravel-cli erase` subcommands (ADR-0064 decision 1, issue #751): submit and
//! inspect selective (GDPR/CCPA subject) erasure requests.
//!
//! Erasure is an operator/compliance workflow, deliberately not a tenant-facing
//! HTTP endpoint in v1 (ADR-0064 decision 1, rejected alternatives). Like
//! `ravel-cli hold set/clear` (ADR-0048/ADR-0055, the only production mechanism
//! to place a legal hold), these subcommands run under the deployment's
//! **Admin** credential: ADR-0055 §6 (amended by this epic) grants Admin, and
//! only Admin, `CreateIfAbsent` write on `t/*/*/del/**`. The CLI performs no
//! credential check of its own -- gating is the object-store IAM policy the CLI
//! is configured with, exactly as with `hold set/clear` -- so running it
//! against a bucket whose credential lacks that grant fails at the `put` with
//! `AccessDenied`.
//!
//! `submit` builds an immutable [`ErasureRequest`] and writes it `.dreq` with
//! `CreateIfAbsent`; a retry of the same request id is idempotent. `status`
//! reports whether a request is still pending (a `.dreq` with no `.done`),
//! completed (a `.done` exists, carrying per-bucket dropped counts and any
//! deferral cause), or unknown.

use std::collections::HashSet;
use std::sync::Arc;

use ravel_commit::erasure;
use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError, list_all};
use ravel_proto::commit::v1::{
    ErasureCompletion, ErasureDeferralCause, ErasurePredicateMatcher, ErasureRequest,
};
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

use crate::maintain::SignalArg;

/// Parse a `key=value` predicate matcher argument. The key must be non-empty
/// (an empty key is rejected by [`erasure::validate_request`] too, but catching
/// it here gives a clearer message against the CLI argument). The value may be
/// empty and may itself contain `=`, so only the first `=` splits.
pub fn parse_matcher(s: &str) -> anyhow::Result<(String, String)> {
    match s.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_string(), value.to_string())),
        _ => anyhow::bail!(
            "matcher {s:?} must be in key=value form with a non-empty key (e.g. user_id=u123)"
        ),
    }
}

/// Whether [`submit`] wrote a fresh request or found one already present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// A fresh `.dreq` was written.
    Created,
    /// A `.dreq` for this request id already existed and was left untouched
    /// (the idempotent-retry path, `CreateIfAbsent` semantics).
    AlreadyPresent,
}

/// `erase submit`: build an [`ErasureRequest`] from a predicate (a conjunction
/// of exact-match matchers plus an optional event-time window) and an optional
/// free-text reason, and write it `.dreq` with `CreateIfAbsent`. Prints the
/// assigned `request_id` and the key it landed at.
///
/// `request_id` and `now_ns` are caller-supplied (the CLI generates a fresh
/// UUID and reads the wall clock; tests inject fixed values), matching this
/// repo's injected-clock discipline. A retry with the same `request_id` and the
/// same predicate re-sends byte-identical content, so `CreateIfAbsent` leaves
/// the original intact and reports [`SubmitOutcome::AlreadyPresent`].
#[allow(clippy::too_many_arguments)]
pub async fn submit(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    matchers: Vec<(String, String)>,
    window_start_ns: i64,
    window_end_ns: i64,
    reason: String,
    request_id: Uuid,
    now_ns: i64,
) -> anyhow::Result<SubmitOutcome> {
    let tenant_hash = TenantId::new(tenant).hash();
    let sig = signal.to_signal();
    let predicate: Vec<ErasurePredicateMatcher> = matchers
        .into_iter()
        .map(|(key, value)| ErasurePredicateMatcher { key, value })
        .collect();
    let record = ErasureRequest {
        format_version: erasure::FORMAT_VERSION,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: ravel_commit::signal::to_proto(sig) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: now_ns,
        predicate,
        window_start_ns,
        window_end_ns,
        reason,
    };
    // Validate before writing: an empty predicate matches every record, which
    // for an irreversible erasure is catastrophic, so the codec rejects it and
    // so must this submit (ADR-0064 decision 1).
    erasure::validate_request(&record)
        .map_err(|err| anyhow::anyhow!("refusing to submit a malformed erasure request: {err}"))?;

    let key = keys::erasure_request_key(&tenant_hash, sig, request_id)
        .map_err(|err| anyhow::anyhow!("failed to build erasure request key: {err}"))?;
    let body = erasure::encode_request(&record);

    match store.put(&key, body, PutOptions::create_if_absent()).await {
        Ok(_) => {
            println!("submitted erasure request {request_id}");
            println!("  key: {key}");
            Ok(SubmitOutcome::Created)
        }
        Err(StoreError::AlreadyExists) => {
            println!(
                "erasure request {request_id} already present (idempotent retry); left untouched"
            );
            println!("  key: {key}");
            Ok(SubmitOutcome::AlreadyPresent)
        }
        Err(err) => Err(anyhow::anyhow!(
            "failed to write erasure request {key}: {err}"
        )),
    }
}

/// The resolved state of one erasure request (ADR-0064 decision 4). `.done`
/// is terminal, so it is checked first: a request with both a `.dreq` and a
/// `.done` (the pre-`.dreq`-removal window, §5) reports [`RequestState::Done`].
#[derive(Debug, Clone)]
pub enum RequestState {
    /// A `.dreq` exists and no `.done` does: the request is still pending.
    Pending(ErasureRequest),
    /// A `.done` exists: the erasure completed (possibly deferred).
    Done(ErasureCompletion),
    /// Neither a `.dreq` nor a `.done` exists for this id.
    Unknown,
}

/// Classify one request id for a (tenant, signal). Reads the `.done` first
/// (terminal), then the `.dreq`; a `NotFound` on either is the expected
/// "absent", any other store error is surfaced. A present-but-corrupt record
/// is a typed error, never silently treated as absent.
pub async fn request_state(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    request_id: Uuid,
) -> anyhow::Result<RequestState> {
    let done_key = keys::erasure_completion_key(tenant_hash, signal, request_id)
        .map_err(|err| anyhow::anyhow!("failed to build completion key: {err}"))?;
    match store.get(&done_key, GetRange::Full).await {
        Ok(outcome) => {
            let completion = erasure::decode_completion(&outcome.data)
                .map_err(|err| anyhow::anyhow!("completion {done_key} is corrupt: {err}"))?;
            return Ok(RequestState::Done(completion));
        }
        Err(StoreError::NotFound) => {}
        Err(err) => return Err(anyhow::anyhow!("failed to read {done_key}: {err}")),
    }

    let dreq_key = keys::erasure_request_key(tenant_hash, signal, request_id)
        .map_err(|err| anyhow::anyhow!("failed to build request key: {err}"))?;
    match store.get(&dreq_key, GetRange::Full).await {
        Ok(outcome) => {
            let record = erasure::decode_request(&outcome.data)
                .map_err(|err| anyhow::anyhow!("request {dreq_key} is corrupt: {err}"))?;
            Ok(RequestState::Pending(record))
        }
        Err(StoreError::NotFound) => Ok(RequestState::Unknown),
        Err(err) => Err(anyhow::anyhow!("failed to read {dreq_key}: {err}")),
    }
}

/// `erase status`: report one request id, or (when `request_id` is `None`)
/// every request pending or completed for the (tenant, signal).
pub async fn status(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    request_id: Option<Uuid>,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let sig = signal.to_signal();

    match request_id {
        Some(id) => {
            let state = request_state(store.as_ref(), &tenant_hash, sig, id).await?;
            print_state(id, &state);
            Ok(())
        }
        None => {
            let prefix = keys::del_prefix(&tenant_hash, sig);
            let metas = list_all(store.as_ref(), &prefix)
                .await
                .map_err(|err| anyhow::anyhow!("failed to list {prefix}: {err}"))?;
            // Collect distinct request ids in listing order, folding each
            // request's `.dreq`/`.done` pair to a single id. A key that parses
            // as neither shape is layout drift; skip it rather than fail the
            // whole status listing (it is not one of this command's records).
            let mut seen = HashSet::new();
            let mut ids = Vec::new();
            for meta in &metas {
                let id = keys::parse_erasure_request_key(&meta.key)
                    .map(|p| p.request_id)
                    .or_else(|_| {
                        keys::parse_erasure_completion_key(&meta.key).map(|p| p.request_id)
                    });
                if let Ok(id) = id
                    && seen.insert(id)
                {
                    ids.push(id);
                }
            }
            if ids.is_empty() {
                println!("no erasure requests for tenant {tenant} signal {sig:?}");
                return Ok(());
            }
            for id in ids {
                let state = request_state(store.as_ref(), &tenant_hash, sig, id).await?;
                print_state(id, &state);
            }
            Ok(())
        }
    }
}

/// Human-readable name for an [`ErasureDeferralCause`], rejecting an unknown
/// enum value with its raw integer rather than misreading it (matching the
/// codec's own closed-enum discipline).
fn deferral_cause_name(raw: i32) -> String {
    match ErasureDeferralCause::try_from(raw) {
        Ok(ErasureDeferralCause::Unspecified) => "none (completed cleanly)".to_string(),
        Ok(ErasureDeferralCause::LegalHold) => "legal hold".to_string(),
        Err(_) => format!("unknown ({raw})"),
    }
}

/// Print one request's resolved state. Pending prints the predicate (which
/// necessarily names the subject -- the `.dreq` holds PII by design, and this
/// is the operator who submitted it); Done prints only the PII-free completion
/// evidence (per-bucket dropped counts, timestamps, deferral cause).
fn print_state(request_id: Uuid, state: &RequestState) {
    match state {
        RequestState::Pending(record) => {
            println!("{request_id}: PENDING");
            println!("  created_unix_ns: {}", record.created_unix_ns);
            let predicate = record
                .predicate
                .iter()
                .map(|m| format!("{}={}", m.key, m.value))
                .collect::<Vec<_>>()
                .join(" AND ");
            println!("  predicate: {predicate}");
            if record.window_start_ns != 0 || record.window_end_ns != 0 {
                println!(
                    "  event-time window: [{}, {})",
                    record.window_start_ns, record.window_end_ns
                );
            } else {
                println!("  event-time window: none (whole retained history)");
            }
            if !record.reason.is_empty() {
                println!("  reason: {}", record.reason);
            }
        }
        RequestState::Done(completion) => {
            println!("{request_id}: COMPLETED");
            println!("  requested_unix_ns: {}", completion.requested_unix_ns);
            println!("  completed_unix_ns: {}", completion.completed_unix_ns);
            println!(
                "  deferral cause: {}",
                deferral_cause_name(completion.deferral_cause)
            );
            let total: u64 = completion
                .bucket_drops
                .iter()
                .map(|d| d.dropped_count)
                .sum();
            println!(
                "  per-bucket dropped counts ({} bucket(s), {total} record(s) total):",
                completion.bucket_drops.len()
            );
            for drop in &completion.bucket_drops {
                println!(
                    "    signal={} shard={} ingest_hour_bucket={} dropped_count={}",
                    drop.signal, drop.shard, drop.ingest_hour_bucket, drop.dropped_count
                );
            }
        }
        RequestState::Unknown => {
            println!("{request_id}: UNKNOWN (no .dreq and no .done for this request id)");
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_proto::commit::v1::ErasureBucketDrop;

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    /// `submit` writes a well-formed `.dreq` at the reconstruct-verified key,
    /// and a retry of the same request id is idempotent: the second put reports
    /// AlreadyPresent and leaves the first writer's bytes intact
    /// (CreateIfAbsent semantics, ADR-0064 decision 1).
    #[tokio::test]
    async fn submit_writes_well_formed_dreq_and_is_idempotent() {
        let store = store();
        let tenant = "acme";
        let tenant_hash = TenantId::new(tenant).hash();
        let id = Uuid::from_u128(42);
        let matchers = vec![("user_id".to_string(), "u123".to_string())];

        let out = submit(
            store.clone(),
            tenant,
            SignalArg::Metrics,
            matchers.clone(),
            0,
            0,
            "dsar-4711".to_string(),
            id,
            1_700_000_000_000_000_000,
        )
        .await
        .expect("submit succeeds");
        assert_eq!(out, SubmitOutcome::Created);

        let key = keys::erasure_request_key(&tenant_hash, Signal::Metrics, id).expect("key");
        let got = store
            .get(&key, GetRange::Full)
            .await
            .expect("the .dreq must be present at the erasure request key");
        let decoded = erasure::decode_request(&got.data).expect("well-formed .dreq");
        assert_eq!(decoded.request_id, id.to_string());
        assert_eq!(decoded.predicate.len(), 1);
        assert_eq!(decoded.predicate[0].key, "user_id");
        assert_eq!(decoded.predicate[0].value, "u123");
        assert_eq!(decoded.reason, "dsar-4711");
        // The object landed at the key its own identity fields reconstruct to
        // (ADR-0010 §7 discipline), not merely at some key.
        keys::verify_erasure_request_key(&decoded, &key)
            .expect("stored key matches the record's own identity");

        // Idempotent retry: same id, same predicate -> byte-identical content,
        // so CreateIfAbsent rejects the loser and keeps the winner's bytes.
        let first_bytes = got.data.clone();
        let out2 = submit(
            store.clone(),
            tenant,
            SignalArg::Metrics,
            matchers,
            0,
            0,
            "dsar-4711".to_string(),
            id,
            1_700_000_000_000_000_000,
        )
        .await
        .expect("retry succeeds");
        assert_eq!(out2, SubmitOutcome::AlreadyPresent);
        let got2 = store
            .get(&key, GetRange::Full)
            .await
            .expect("still present");
        assert_eq!(
            first_bytes, got2.data,
            "CreateIfAbsent must leave the first writer's bytes intact on retry"
        );
    }

    /// An empty predicate matches every record; submit must refuse it (writing
    /// nothing) rather than issue a catastrophic all-records erasure.
    #[tokio::test]
    async fn submit_refuses_empty_predicate() {
        let store = store();
        let err = submit(
            store.clone(),
            "acme",
            SignalArg::Metrics,
            vec![],
            0,
            0,
            String::new(),
            Uuid::from_u128(1),
            0,
        )
        .await
        .expect_err("an empty predicate must be refused");
        assert!(
            err.to_string().contains("malformed erasure request"),
            "unexpected error: {err}"
        );
        let key = keys::erasure_request_key(
            &TenantId::new("acme").hash(),
            Signal::Metrics,
            Uuid::from_u128(1),
        )
        .expect("key");
        assert!(
            matches!(
                store.get(&key, GetRange::Full).await,
                Err(StoreError::NotFound)
            ),
            "a refused submit must write nothing"
        );
    }

    /// `request_state` distinguishes pending (a `.dreq`, no `.done`), completed
    /// (a `.done` exists), and unknown (neither), and a `.done` wins over a
    /// still-present `.dreq` (the pre-removal window, §5).
    #[tokio::test]
    async fn status_distinguishes_pending_done_and_unknown() {
        let store = store();
        let tenant = "acme";
        let tenant_hash = TenantId::new(tenant).hash();

        // Unknown: nothing submitted.
        let unknown = request_state(
            store.as_ref(),
            &tenant_hash,
            Signal::Metrics,
            Uuid::from_u128(999),
        )
        .await
        .expect("state read");
        assert!(matches!(unknown, RequestState::Unknown));

        // Pending: a submitted request with no completion.
        let pending_id = Uuid::from_u128(1);
        submit(
            store.clone(),
            tenant,
            SignalArg::Metrics,
            vec![("user_id".to_string(), "u1".to_string())],
            0,
            0,
            String::new(),
            pending_id,
            1,
        )
        .await
        .expect("submit pending");
        let pending = request_state(store.as_ref(), &tenant_hash, Signal::Metrics, pending_id)
            .await
            .expect("state read");
        assert!(matches!(pending, RequestState::Pending(_)));
        // Exercise the CLI print path for a single id too.
        status(store.clone(), tenant, SignalArg::Metrics, Some(pending_id))
            .await
            .expect("status runs");

        // Completed: a `.done` present, carrying per-bucket dropped counts.
        let done_id = Uuid::from_u128(2);
        let completion = ErasureCompletion {
            format_version: erasure::FORMAT_VERSION,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            request_id: done_id.to_string(),
            predicate_hash: vec![0x11; 32],
            bucket_drops: vec![ErasureBucketDrop {
                signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
                shard: 3,
                ingest_hour_bucket: 100,
                dropped_count: 7,
            }],
            requested_unix_ns: 1,
            completed_unix_ns: 2,
            deferral_cause: 0,
        };
        let done_key =
            keys::erasure_completion_key(&tenant_hash, Signal::Metrics, done_id).expect("key");
        store
            .put(
                &done_key,
                erasure::encode_completion(&completion),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("write .done");
        let done = request_state(store.as_ref(), &tenant_hash, Signal::Metrics, done_id)
            .await
            .expect("state read");
        match done {
            RequestState::Done(c) => {
                assert_eq!(c.request_id, done_id.to_string());
                assert_eq!(c.bucket_drops.len(), 1);
                assert_eq!(c.bucket_drops[0].dropped_count, 7);
            }
            other => panic!("expected Done, got {other:?}"),
        }

        // A `.done` wins even when a `.dreq` is still present for the same id.
        let dreq_key =
            keys::erasure_request_key(&tenant_hash, Signal::Metrics, done_id).expect("key");
        let request = ErasureRequest {
            format_version: erasure::FORMAT_VERSION,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            request_id: done_id.to_string(),
            created_unix_ns: 1,
            predicate: vec![ErasurePredicateMatcher {
                key: "user_id".to_string(),
                value: "u2".to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        };
        store
            .put(
                &dreq_key,
                erasure::encode_request(&request),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("write .dreq alongside .done");
        let still_done = request_state(store.as_ref(), &tenant_hash, Signal::Metrics, done_id)
            .await
            .expect("state read");
        assert!(
            matches!(still_done, RequestState::Done(_)),
            "a completion must win over a still-present request"
        );

        // The list mode surfaces both the pending and the completed request.
        status(store.clone(), tenant, SignalArg::Metrics, None)
            .await
            .expect("status list runs");
    }

    #[test]
    fn parse_matcher_splits_on_first_equals_and_requires_a_key() {
        assert_eq!(
            parse_matcher("user_id=u123").expect("ok"),
            ("user_id".to_string(), "u123".to_string())
        );
        // A value may itself contain '='.
        assert_eq!(
            parse_matcher("k=a=b").expect("ok"),
            ("k".to_string(), "a=b".to_string())
        );
        assert!(parse_matcher("=novalue").is_err(), "empty key rejected");
        assert!(parse_matcher("nokey").is_err(), "missing '=' rejected");
    }
}
