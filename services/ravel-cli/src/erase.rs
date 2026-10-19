//! `ravel-cli erase` subcommands (ADR-0064 decision 1): submit and
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
    /// (the idempotent-retry path, `CreateIfAbsent` semantics). Reached only
    /// when the existing record defines the same erasure as this submission.
    AlreadyPresent,
}

/// A reason [`submit`] refused to record a request without surfacing a plain
/// store error. Currently one case.
#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    /// A `.dreq` already exists under this `request_id`, but it defines a
    /// *different* erasure (different predicate matchers or window bounds) than
    /// the request just submitted. `request_id` is operator-chosen (often a
    /// DSAR ticket id) and the payload is PII-bearing, so this must fail loudly
    /// rather than report a false [`SubmitOutcome::AlreadyPresent`]: a silent
    /// success would signal that the new subject was erased and start a
    /// compliance (GDPR) clock on data never touched for the corrected
    /// predicate. Erasure requests are immutable, so the fix is a new
    /// `request_id`, not an overwrite.
    #[error(
        "a conflicting erasure request already exists under request_id {request_id} \
         with different predicate matchers or window bounds; it was NOT overwritten. \
         Erasure requests are immutable -- submit the corrected request under a new request_id"
    )]
    ConflictingRequest { request_id: Uuid },
}

/// `erase submit`: build an [`ErasureRequest`] from a predicate (a conjunction
/// of exact-match matchers plus an optional event-time window) and an optional
/// free-text reason, and write it `.dreq` with `CreateIfAbsent`. Prints the
/// assigned `request_id` and the key it landed at.
///
/// `request_id` and `now_ns` are caller-supplied (the CLI generates a fresh
/// UUID and reads the wall clock; tests inject fixed values), matching this
/// repo's injected-clock discipline. Because `now_ns` is stamped into
/// `created_unix_ns` on every call, two submissions of the same `request_id`
/// are never byte-identical, even when they define exactly the same erasure.
/// Idempotency is therefore defined over the erasure-defining fields -- the
/// predicate matchers and the event-time window bounds -- not the encoded
/// bytes. On a `CreateIfAbsent` rejection this reads the stored record back and
/// compares those fields: a genuine retry (same fields) reports
/// [`SubmitOutcome::AlreadyPresent`] and leaves the original intact, while a
/// request that reuses the `request_id` to define a *different* erasure is a
/// [`SubmitError::ConflictingRequest`], never a silent success.
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
            // CreateIfAbsent rejected the write: a `.dreq` already exists at
            // this key. That is a genuine idempotent retry only if the stored
            // record defines the *same* erasure. Read it back and compare the
            // erasure-defining fields; do NOT compare encoded bytes, because
            // `created_unix_ns` carries a fresh `now_ns` per call and a true
            // retry is therefore never byte-identical to the original.
            let existing = store.get(&key, GetRange::Full).await.map_err(|err| {
                anyhow::anyhow!("failed to read existing erasure request {key}: {err}")
            })?;
            let existing = erasure::decode_request(&existing.data).map_err(|err| {
                anyhow::anyhow!("existing erasure request {key} is corrupt: {err}")
            })?;
            if same_erasure(&existing, &record) {
                println!(
                    "erasure request {request_id} already present (idempotent retry); left untouched"
                );
                println!("  key: {key}");
                Ok(SubmitOutcome::AlreadyPresent)
            } else {
                Err(SubmitError::ConflictingRequest { request_id }.into())
            }
        }
        Err(err) => Err(anyhow::anyhow!(
            "failed to write erasure request {key}: {err}"
        )),
    }
}

/// The most erasure requests one `erase submit` fans out to under an IPCrypt
/// policy (ADR-2993 decision 8).
pub const MAX_IPCRYPT_REQUESTS: usize = 16;

/// One matcher whose key has an IPCrypt rule for the request's signal, and
/// the epoch its value was encrypted under in one planned request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedMatcher {
    /// The matcher's key, as written on the command line.
    pub key: String,
    /// The rule's key, as written in the policy.
    pub rule_key: String,
    pub epoch: u32,
}

/// One erasure request `erase submit` will write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRequest {
    pub request_id: Uuid,
    /// The full matcher set, encrypted values in place.
    pub matchers: Vec<(String, String)>,
    /// The encrypted matchers, in matcher order.
    pub epochs: Vec<EncryptedMatcher>,
}

/// A [`PlannedRequest`]'s matchers and epochs, before its id is assigned.
type PlanDraft = (Vec<(String, String)>, Vec<EncryptedMatcher>);

/// One matcher's candidate values: the value unchanged, or one ciphertext
/// per epoch of its rule.
enum MatcherValues {
    Plain(String),
    Encrypted {
        rule_key: String,
        values: Vec<(u32, String)>,
    },
}

/// The request id of one request in a multi-request fan-out: a UUID-shaped
/// blake3 hash of the base id and the request's epochs, so retrying the
/// command with the same base id re-submits the same requests.
pub fn derived_request_id(base: Uuid, epochs: &[EncryptedMatcher]) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ravel-cli erase submit ipcrypt epoch request v1\0");
    hasher.update(base.as_bytes());
    for matcher in epochs {
        hasher.update(&(matcher.key.len() as u64).to_le_bytes());
        hasher.update(matcher.key.as_bytes());
        hasher.update(&matcher.epoch.to_le_bytes());
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    uuid::Builder::from_random_bytes(bytes).into_uuid()
}

/// Plan the requests `matchers` become under `tenant`'s IPCrypt `rules`
/// (ADR-2993 decision 8). A matcher whose key has a rule for `signal` (looked
/// up by its sanitized form for metrics, by its raw key otherwise) carries
/// that rule's ciphertext, and the plan holds one request per combination of
/// the encrypted matchers' epochs (the rule's `epoch`, then each of its
/// `retired_epochs`). The matcher key stays as written. A value that is not
/// an address, and an empty value, stay unchanged. More than
/// [`MAX_IPCRYPT_REQUESTS`] requests is refused, naming the rules. With one
/// request it carries `base_id`; with several, each carries
/// [`derived_request_id`].
pub fn plan_requests(
    policy: &crate::ipcrypt::IpcryptPolicy,
    rules: &ravel_ipcrypt::rules::TenantRules,
    tenant: &str,
    signal: SignalArg,
    matchers: Vec<(String, String)>,
    base_id: Uuid,
) -> anyhow::Result<Vec<PlannedRequest>> {
    let mut candidates = Vec::with_capacity(matchers.len());
    for (key, value) in &matchers {
        let info = if value.is_empty() {
            None
        } else if signal == SignalArg::Metrics {
            rules.lookup_label(&crate::ipcrypt::sanitize_rule_key(key))
        } else {
            rules.lookup_raw(key)
        };
        let Some(info) = info else {
            candidates.push(MatcherValues::Plain(value.clone()));
            continue;
        };
        let mut values = Vec::with_capacity(1 + info.retired_epochs.len());
        for epoch in std::iter::once(info.epoch).chain(info.retired_epochs.iter().copied()) {
            let Some(epoch_key) = policy.epoch_key(tenant, info.variant, epoch)? else {
                anyhow::bail!(crate::ipcrypt::NO_DEPLOYMENT_KEY);
            };
            match epoch_key.encrypt(value) {
                (encrypted, ravel_ipcrypt::render::RenderOutcome::Encrypted) => {
                    values.push((epoch, encrypted));
                }
                (_, ravel_ipcrypt::render::RenderOutcome::Unparsed) => {
                    values.clear();
                    break;
                }
            }
        }
        if values.is_empty() {
            candidates.push(MatcherValues::Plain(value.clone()));
        } else {
            candidates.push(MatcherValues::Encrypted {
                rule_key: info.raw_key.to_string(),
                values,
            });
        }
    }

    let total = candidates.iter().try_fold(1usize, |acc, c| match c {
        MatcherValues::Plain(_) => Some(acc),
        MatcherValues::Encrypted { values, .. } => acc.checked_mul(values.len()),
    });
    if total.is_none_or(|n| n > MAX_IPCRYPT_REQUESTS) {
        let multiplying: Vec<String> = matchers
            .iter()
            .zip(&candidates)
            .filter_map(|((key, _), c)| match c {
                MatcherValues::Encrypted { rule_key, values } if values.len() > 1 => {
                    let epochs: Vec<String> = values.iter().map(|(e, _)| e.to_string()).collect();
                    Some(format!(
                        "rule {rule_key:?} (matcher {key:?}) has epochs [{}]",
                        epochs.join(", ")
                    ))
                }
                _ => None,
            })
            .collect();
        let count = total.map_or_else(|| "too many".to_string(), |n| n.to_string());
        anyhow::bail!(
            "refusing to submit: the IPCrypt-encrypted matchers need one erasure request per \
             combination of their rules' epochs, {count} requests, more than \
             {MAX_IPCRYPT_REQUESTS}: {}. Drop retired_epochs whose data has aged out of \
             retention, or submit fewer encrypted matchers per command. Nothing was submitted",
            multiplying.join("; ")
        );
    }

    let mut plans: Vec<PlanDraft> = vec![(Vec::with_capacity(matchers.len()), Vec::new())];
    for ((key, _), candidate) in matchers.iter().zip(candidates) {
        match candidate {
            MatcherValues::Plain(value) => {
                for (set, _) in &mut plans {
                    set.push((key.clone(), value.clone()));
                }
            }
            MatcherValues::Encrypted { rule_key, values } => {
                let mut next = Vec::with_capacity(plans.len() * values.len());
                for (set, epochs) in &plans {
                    for (epoch, encrypted) in &values {
                        let mut set = set.clone();
                        set.push((key.clone(), encrypted.clone()));
                        let mut epochs = epochs.clone();
                        epochs.push(EncryptedMatcher {
                            key: key.clone(),
                            rule_key: rule_key.clone(),
                            epoch: *epoch,
                        });
                        next.push((set, epochs));
                    }
                }
                plans = next;
            }
        }
    }
    let single = plans.len() == 1;
    Ok(plans
        .into_iter()
        .map(|(matchers, epochs)| PlannedRequest {
            request_id: if single {
                base_id
            } else {
                derived_request_id(base_id, &epochs)
            },
            matchers,
            epochs,
        })
        .collect())
}

/// What [`submit_with_ipcrypt`] submitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpcryptSubmitReport {
    pub base_id: Uuid,
    pub requests: Vec<(PlannedRequest, SubmitOutcome)>,
    pub created: usize,
    pub already_present: usize,
}

/// `erase submit` under an IPCrypt policy (ADR-2993 decision 8):
/// [`plan_requests`], then the `sys/ipcrypt/policy` marker check, then one
/// [`submit`] per planned request. A failure part-way names the base id, so
/// rerunning with it as `--request-id` finishes the requests left out.
#[allow(clippy::too_many_arguments)]
pub async fn submit_with_ipcrypt(
    store: Arc<dyn ObjectStoreBackend>,
    policy: &crate::ipcrypt::IpcryptPolicy,
    tenant: &str,
    signal: SignalArg,
    matchers: Vec<(String, String)>,
    window_start_ns: i64,
    window_end_ns: i64,
    reason: String,
    base_id: Uuid,
    now_ns: i64,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<IpcryptSubmitReport> {
    // Plan before the marker check, so a refused plan writes nothing at all,
    // not even the marker a first policy run would create.
    let rules = policy.rules_for(tenant)?;
    let plans = plan_requests(policy, &rules, tenant, signal, matchers, base_id)?;
    policy
        .check_marker_logged(store.as_ref(), "erase submit", out)
        .await?;
    let total = plans.len();
    if total > 1 {
        writeln!(
            out,
            "erasure base request id {base_id}: submitting {total} requests, one per IPCrypt \
             epoch combination"
        )?;
    }
    let mut report = IpcryptSubmitReport {
        base_id,
        requests: Vec::with_capacity(total),
        created: 0,
        already_present: 0,
    };
    for (done, plan) in plans.into_iter().enumerate() {
        let outcome = submit(
            store.clone(),
            tenant,
            signal,
            plan.matchers.clone(),
            window_start_ns,
            window_end_ns,
            reason.clone(),
            plan.request_id,
            now_ns,
        )
        .await
        .map_err(|err| {
            if total > 1 {
                err.context(format!(
                    "erasure request {} failed after {done} of {total} were submitted; rerun \
                     with --request-id {base_id} to finish",
                    plan.request_id
                ))
            } else {
                err
            }
        })?;
        match outcome {
            SubmitOutcome::Created => report.created += 1,
            SubmitOutcome::AlreadyPresent => report.already_present += 1,
        }
        report.requests.push((plan, outcome));
    }
    if total > 1 {
        writeln!(out, "erasure base request id {base_id}:")?;
        for (plan, _) in &report.requests {
            let epochs: Vec<String> = plan
                .epochs
                .iter()
                .map(|m| format!("{} epoch {}", m.key, m.epoch))
                .collect();
            writeln!(out, "  {} ({})", plan.request_id, epochs.join(", "))?;
        }
        writeln!(
            out,
            "{total} erasure requests: {} created, {} already present",
            report.created, report.already_present
        )?;
    }
    Ok(report)
}

/// Whether two erasure requests define the *same* erasure: they agree on every
/// field that determines which records get dropped. That is the predicate
/// matchers (compared as an unordered multiset, since matcher order is not
/// significant to the conjunction) and the event-time window bounds. Fields
/// that do not change what is erased are ignored -- notably `created_unix_ns`
/// (a fresh wall-clock read on every submit) and the free-text `reason` -- so a
/// genuine idempotent retry compares equal despite never being byte-identical.
/// The `request_id`, `signal`, and `tenant_hash` are not compared here because
/// they are encoded in the object key: two records reaching this comparison
/// already share a key and therefore all three.
fn same_erasure(a: &ErasureRequest, b: &ErasureRequest) -> bool {
    if a.window_start_ns != b.window_start_ns || a.window_end_ns != b.window_end_ns {
        return false;
    }
    let mut am: Vec<(&str, &str)> = a
        .predicate
        .iter()
        .map(|m| (m.key.as_str(), m.value.as_str()))
        .collect();
    let mut bm: Vec<(&str, &str)> = b
        .predicate
        .iter()
        .map(|m| (m.key.as_str(), m.value.as_str()))
        .collect();
    am.sort_unstable();
    bm.sort_unstable();
    am == bm
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

        // Idempotent retry with the *real* production shape: same id, same
        // predicate and window, but a later `now_ns` (main.rs passes a fresh
        // now_ns() per invocation). The two encodings therefore differ in
        // `created_unix_ns` and are NOT byte-identical, yet the retry must
        // still be recognized as idempotent by comparing erasure-defining
        // fields, report AlreadyPresent, and leave the first writer's bytes
        // (with the original timestamp) intact.
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
            1_700_000_000_000_000_042,
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

    /// A retry that reuses a `request_id` but changes the erasure-defining
    /// predicate must be rejected, not silently reported as an idempotent
    /// AlreadyPresent. request_id is operator-chosen (a DSAR ticket id), the
    /// payload is PII-bearing, and a false "already erased" signal starts a
    /// GDPR clock on data never erased for the corrected matcher, so the second
    /// submit returns [`SubmitError::ConflictingRequest`] and the original
    /// record is left untouched.
    #[tokio::test]
    async fn submit_rejects_conflicting_request_id_with_different_predicate() {
        let store = store();
        let tenant = "acme";
        let tenant_hash = TenantId::new(tenant).hash();
        let id = Uuid::from_u128(4711);

        submit(
            store.clone(),
            tenant,
            SignalArg::Metrics,
            vec![("user_id".to_string(), "u123".to_string())],
            0,
            0,
            "dsar-4711".to_string(),
            id,
            1_700_000_000_000_000_000,
        )
        .await
        .expect("first submit succeeds");

        let key = keys::erasure_request_key(&tenant_hash, Signal::Metrics, id).expect("key");
        let first_bytes = store
            .get(&key, GetRange::Full)
            .await
            .expect("first .dreq present")
            .data;

        // Same request_id, DIFFERENT subject: this is a distinct erasure that
        // happens to reuse the id, not a retry.
        let err = submit(
            store.clone(),
            tenant,
            SignalArg::Metrics,
            vec![("user_id".to_string(), "u999".to_string())],
            0,
            0,
            "dsar-4711".to_string(),
            id,
            1_700_000_000_000_000_099,
        )
        .await
        .expect_err("a conflicting request under the same id must be rejected");
        assert!(
            matches!(
                err.downcast_ref::<SubmitError>(),
                Some(SubmitError::ConflictingRequest { request_id }) if *request_id == id
            ),
            "expected SubmitError::ConflictingRequest, got: {err}"
        );

        // The original record is untouched: the second (conflicting) subject
        // was never recorded.
        let after = store
            .get(&key, GetRange::Full)
            .await
            .expect("original .dreq still present")
            .data;
        assert_eq!(
            first_bytes, after,
            "a rejected conflicting submit must not overwrite the original request"
        );
        let decoded = erasure::decode_request(&after).expect("well-formed .dreq");
        assert_eq!(decoded.predicate[0].value, "u123");

        // A window-only difference (same matchers) is equally a conflict.
        let err = submit(
            store.clone(),
            tenant,
            SignalArg::Metrics,
            vec![("user_id".to_string(), "u123".to_string())],
            1,
            1_000,
            "dsar-4711".to_string(),
            id,
            1_700_000_000_000_000_100,
        )
        .await
        .expect_err("a differing window under the same id must be rejected");
        assert!(
            matches!(
                err.downcast_ref::<SubmitError>(),
                Some(SubmitError::ConflictingRequest { .. })
            ),
            "expected SubmitError::ConflictingRequest for a window change, got: {err}"
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

/// `erase submit` under an IPCrypt policy (ADR-2993 decision 8).
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod ipcrypt_tests {
    use super::*;
    use crate::ipcrypt::{IpcryptPolicy, sanitize_rule_key};
    use ravel_ipcrypt::marker::{IPCRYPT_POLICY_KEY, check_marker};
    use ravel_ipcrypt::policy::{Variant, parse_policy_file};
    use ravel_ipcrypt::render::RenderOutcome;
    use ravel_ipcrypt::rules::EpochKey;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule as FaultRule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;

    const TENANT: &str = "acme";
    const DEPLOYMENT_KEY: [u8; 32] = [0x5a; 32];
    const NOW_NS: i64 = 1_700_000_000_000_000_000;
    const CLIENT: &str = "198.51.100.101";
    const HOST: &str = "198.51.100.102";

    /// `client.ip` at epoch 2 with epoch 1 retired, `host.ip` at epoch 3 with
    /// epoch 1 retired.
    const TWO_EPOCH_POLICY: &str = r#"
[[defaults.rules]]
key = "client.ip"
variant = "deterministic"
epoch = 2
retired_epochs = [1]

[[defaults.rules]]
key = "host.ip"
variant = "prefix"
epoch = 3
retired_epochs = [1]
"#;

    /// Five epochs on each of two rules: 25 requests for both matchers.
    const FIVE_EPOCH_POLICY: &str = r#"
[[defaults.rules]]
key = "client.ip"
variant = "deterministic"
epoch = 5
retired_epochs = [1, 2, 3, 4]

[[defaults.rules]]
key = "host.ip"
variant = "prefix"
epoch = 5
retired_epochs = [1, 2, 3, 4]
"#;

    fn policy(text: &str) -> IpcryptPolicy {
        let file = parse_policy_file(text, sanitize_rule_key).expect("policy parses");
        IpcryptPolicy::new(file, Some(&DEPLOYMENT_KEY)).expect("policy loads")
    }

    /// `value` under `variant` and `epoch`, derived from the deployment key
    /// directly rather than through the rules the submit resolved.
    fn ciphertext(variant: Variant, epoch: u32, value: &str) -> String {
        let key = EpochKey::new(
            &DEPLOYMENT_KEY,
            &TenantId::new(TENANT).hash(),
            variant,
            epoch,
        )
        .expect("key derives");
        let (encrypted, outcome) = key.encrypt(value);
        assert_eq!(outcome, RenderOutcome::Encrypted);
        encrypted
    }

    fn matcher(key: &str, value: &str) -> (String, String) {
        (key.to_string(), value.to_string())
    }

    async fn submit_logs(
        store: &Arc<dyn ObjectStoreBackend>,
        policy: &IpcryptPolicy,
        matchers: Vec<(String, String)>,
        base: Uuid,
    ) -> anyhow::Result<IpcryptSubmitReport> {
        submit_with_ipcrypt(
            Arc::clone(store),
            policy,
            TENANT,
            SignalArg::Logs,
            matchers,
            0,
            0,
            "dsar-1".to_string(),
            base,
            NOW_NS,
            &mut Vec::new(),
        )
        .await
    }

    /// The stored `.dreq` for `id`, decoded.
    async fn stored_request(
        store: &Arc<dyn ObjectStoreBackend>,
        signal: Signal,
        id: Uuid,
    ) -> ErasureRequest {
        let key = keys::erasure_request_key(&TenantId::new(TENANT).hash(), signal, id)
            .expect("request key");
        let got = store.get(&key, GetRange::Full).await.expect(".dreq stored");
        erasure::decode_request(&got.data).expect("well-formed .dreq")
    }

    fn predicate(request: &ErasureRequest) -> Vec<(String, String)> {
        request
            .predicate
            .iter()
            .map(|m| (m.key.clone(), m.value.clone()))
            .collect()
    }

    async fn request_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
        list_all(store, "")
            .await
            .expect("list")
            .into_iter()
            .map(|o| o.key)
            .filter(|k| k.contains("/del/"))
            .collect()
    }

    #[tokio::test]
    async fn one_request_per_epoch_with_derived_ids_and_each_epochs_ciphertext() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let policy = policy(TWO_EPOCH_POLICY);
        let base = Uuid::from_u128(7);
        let report = submit_logs(&store, &policy, vec![matcher("client.ip", CLIENT)], base)
            .await
            .expect("submit");

        assert_eq!(report.base_id, base);
        assert_eq!(report.requests.len(), 2);
        assert_eq!((report.created, report.already_present), (2, 0));
        for ((plan, outcome), epoch) in report.requests.iter().zip([2u32, 1]) {
            assert_eq!(*outcome, SubmitOutcome::Created);
            let expected_epochs = vec![EncryptedMatcher {
                key: "client.ip".to_string(),
                rule_key: "client.ip".to_string(),
                epoch,
            }];
            assert_eq!(plan.epochs, expected_epochs);
            assert_eq!(plan.request_id, derived_request_id(base, &expected_epochs));
            assert_ne!(plan.request_id, base);
            let stored = stored_request(&store, Signal::Logs, plan.request_id).await;
            assert_eq!(
                predicate(&stored),
                vec![matcher(
                    "client.ip",
                    &ciphertext(Variant::Deterministic, epoch, CLIENT)
                )],
                "epoch {epoch}"
            );
        }
        assert_ne!(
            report.requests[0].0.request_id,
            report.requests[1].0.request_id
        );
        assert_eq!(request_keys(store.as_ref()).await.len(), 2);
        let marker = ravel_ipcrypt::marker::read_marker(store.as_ref())
            .await
            .expect("read marker")
            .map(|(marker, _)| marker.fingerprint);
        assert_eq!(
            marker,
            Some(policy.fingerprint()),
            "the submit created the marker"
        );
    }

    #[tokio::test]
    async fn two_encrypted_matchers_with_two_epochs_each_give_four_requests() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let policy = policy(TWO_EPOCH_POLICY);
        let base = Uuid::from_u128(8);
        let report = submit_logs(
            &store,
            &policy,
            vec![
                matcher("client.ip", CLIENT),
                matcher("service.name", "api"),
                matcher("host.ip", HOST),
            ],
            base,
        )
        .await
        .expect("submit");

        assert_eq!(report.requests.len(), 4);
        assert_eq!(report.created, 4);
        let mut seen = HashSet::new();
        for (client_epoch, host_epoch) in [(2u32, 3u32), (2, 1), (1, 3), (1, 1)] {
            let want = vec![
                matcher(
                    "client.ip",
                    &ciphertext(Variant::Deterministic, client_epoch, CLIENT),
                ),
                matcher("service.name", "api"),
                matcher("host.ip", &ciphertext(Variant::Prefix, host_epoch, HOST)),
            ];
            let (plan, _) = report
                .requests
                .iter()
                .find(|(plan, _)| plan.matchers == want)
                .unwrap_or_else(|| panic!("no request for epochs {client_epoch}/{host_epoch}"));
            let stored = stored_request(&store, Signal::Logs, plan.request_id).await;
            assert_eq!(predicate(&stored), want);
            assert!(seen.insert(plan.request_id));
        }
        assert_eq!(request_keys(store.as_ref()).await.len(), 4);
    }

    #[tokio::test]
    async fn more_than_sixteen_requests_is_refused_before_any_write() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let policy = policy(FIVE_EPOCH_POLICY);
        let err = submit_logs(
            &store,
            &policy,
            vec![matcher("client.ip", CLIENT), matcher("host.ip", HOST)],
            Uuid::from_u128(9),
        )
        .await
        .expect_err("25 requests must be refused");
        let message = format!("{err:#}");
        assert!(message.contains("25 requests, more than 16"), "{message}");
        assert!(
            message
                .contains("rule \"client.ip\" (matcher \"client.ip\") has epochs [5, 1, 2, 3, 4]"),
            "{message}"
        );
        assert!(message.contains("rule \"host.ip\""), "{message}");
        let all = list_all(store.as_ref(), "").await.expect("list");
        assert!(
            all.is_empty(),
            "a refused plan writes nothing: {:?}",
            all.iter().map(|o| o.key.as_str()).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn a_retry_with_the_same_base_id_resubmits_the_same_requests() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let policy = policy(TWO_EPOCH_POLICY);
        let base = Uuid::from_u128(10);
        let matchers = vec![matcher("client.ip", CLIENT), matcher("host.ip", HOST)];
        let first = submit_logs(&store, &policy, matchers.clone(), base)
            .await
            .expect("first submit");
        let mut out = Vec::new();
        let retry = submit_with_ipcrypt(
            Arc::clone(&store),
            &policy,
            TENANT,
            SignalArg::Logs,
            matchers,
            0,
            0,
            "dsar-1".to_string(),
            base,
            NOW_NS,
            &mut out,
        )
        .await
        .expect("a retry is not a conflict");
        let ids = |report: &IpcryptSubmitReport| -> Vec<Uuid> {
            report.requests.iter().map(|(p, _)| p.request_id).collect()
        };
        assert_eq!(ids(&first), ids(&retry));
        assert_eq!((retry.created, retry.already_present), (0, 4));
        assert_eq!(request_keys(store.as_ref()).await.len(), 4);

        let out = String::from_utf8(out).expect("utf-8");
        assert!(
            out.contains(&format!("erasure base request id {base}:\n")),
            "{out}"
        );
        for id in ids(&retry) {
            assert!(out.contains(&format!("  {id} (client.ip epoch ")), "{out}");
        }
        assert!(
            out.ends_with("4 erasure requests: 0 created, 4 already present\n"),
            "{out}"
        );
    }

    #[tokio::test]
    async fn a_single_request_keeps_the_base_id() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let policy =
            policy("[[defaults.rules]]\nkey = \"client.ip\"\nvariant = \"deterministic\"\n");
        let base = Uuid::from_u128(11);
        let report = submit_logs(&store, &policy, vec![matcher("client.ip", CLIENT)], base)
            .await
            .expect("submit");
        assert_eq!(report.requests.len(), 1);
        assert_eq!(report.requests[0].0.request_id, base);
        let stored = stored_request(&store, Signal::Logs, base).await;
        assert_eq!(
            predicate(&stored),
            vec![matcher(
                "client.ip",
                &ciphertext(Variant::Deterministic, 1, CLIENT)
            )]
        );
    }

    /// A metric matcher names the label as stored; the rule is found through
    /// the sanitized key and the key itself is submitted as written.
    #[tokio::test]
    async fn a_metric_matcher_is_ruled_through_its_sanitized_label_name() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let policy =
            policy("[[defaults.rules]]\nkey = \"client.ip\"\nvariant = \"deterministic\"\n");
        let base = Uuid::from_u128(12);
        submit_with_ipcrypt(
            Arc::clone(&store),
            &policy,
            TENANT,
            SignalArg::Metrics,
            vec![matcher("client_ip", CLIENT)],
            0,
            0,
            String::new(),
            base,
            NOW_NS,
            &mut Vec::new(),
        )
        .await
        .expect("submit");
        let stored = stored_request(&store, Signal::Metrics, base).await;
        assert_eq!(
            predicate(&stored),
            vec![matcher(
                "client_ip",
                &ciphertext(Variant::Deterministic, 1, CLIENT)
            )]
        );

        // A label stored as sent over Remote Write keeps its dotted name in
        // the matcher, and the same rule encrypts its value.
        let dotted = Uuid::from_u128(14);
        submit_with_ipcrypt(
            Arc::clone(&store),
            &policy,
            TENANT,
            SignalArg::Metrics,
            vec![matcher("client.ip", CLIENT)],
            0,
            0,
            String::new(),
            dotted,
            NOW_NS,
            &mut Vec::new(),
        )
        .await
        .expect("submit");
        let stored = stored_request(&store, Signal::Metrics, dotted).await;
        assert_eq!(
            predicate(&stored),
            vec![matcher(
                "client.ip",
                &ciphertext(Variant::Deterministic, 1, CLIENT)
            )]
        );
    }

    #[tokio::test]
    async fn a_value_that_is_not_an_address_is_submitted_unchanged() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let policy = policy(TWO_EPOCH_POLICY);
        let base = Uuid::from_u128(13);
        let report = submit_logs(&store, &policy, vec![matcher("client.ip", "unknown")], base)
            .await
            .expect("submit");
        assert_eq!(report.requests.len(), 1);
        assert!(report.requests[0].0.epochs.is_empty());
        let stored = stored_request(&store, Signal::Logs, base).await;
        assert_eq!(predicate(&stored), vec![matcher("client.ip", "unknown")]);
    }

    #[tokio::test]
    async fn a_marker_mismatch_refuses_and_writes_no_request() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let other = [7u8; 32];
        check_marker(store.as_ref(), other)
            .await
            .expect("seed marker");
        let policy = policy(TWO_EPOCH_POLICY);
        let err = submit_logs(
            &store,
            &policy,
            vec![matcher("client.ip", CLIENT)],
            Uuid::from_u128(14),
        )
        .await
        .expect_err("a different policy must be refused");
        let message = format!("{err:#}");
        assert!(message.contains("refusing to erase submit"), "{message}");
        assert!(message.contains(&hex::encode(other)), "{message}");
        assert!(
            message.contains(&hex::encode(policy.fingerprint())),
            "{message}"
        );
        assert!(request_keys(store.as_ref()).await.is_empty());
    }

    #[tokio::test]
    async fn a_marker_read_failure_refuses_and_writes_nothing() {
        let faulty = Arc::new(FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                FaultRule::new(Op::Get, ScriptedFault::Transient("marker read".into()))
                    .with_key_contains(IPCRYPT_POLICY_KEY),
            ),
        ));
        let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
        let policy = policy(TWO_EPOCH_POLICY);
        let err = submit_logs(
            &store,
            &policy,
            vec![matcher("client.ip", CLIENT)],
            Uuid::from_u128(15),
        )
        .await
        .expect_err("a failed marker read must refuse");
        assert!(
            format!("{err:#}").contains("refusing to erase submit"),
            "{err:#}"
        );
        assert_eq!(faulty.fault_count(Op::Get, FaultKind::Transient), 1);
        let keys = list_all(faulty.inner(), "").await.expect("list");
        assert!(keys.is_empty(), "{keys:?}");
    }

    /// A write failing part-way names the base id, and rerunning with it
    /// submits only the requests left out.
    #[tokio::test]
    async fn a_failure_part_way_names_the_base_id_and_a_rerun_finishes() {
        let faulty = Arc::new(FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                FaultRule::new(Op::Put, ScriptedFault::Transient("second request".into()))
                    .with_key_contains("/del/")
                    .with_occurrence(Occurrence::Nth(2)),
            ),
        ));
        let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
        let policy = policy(TWO_EPOCH_POLICY);
        let base = Uuid::from_u128(16);
        let matchers = vec![matcher("client.ip", CLIENT), matcher("host.ip", HOST)];
        let err = submit_logs(&store, &policy, matchers.clone(), base)
            .await
            .expect_err("the second write fails");
        let message = format!("{err:#}");
        assert!(
            message.contains(&format!(
                "failed after 1 of 4 were submitted; rerun with --request-id {base} to finish"
            )),
            "{message}"
        );
        assert_eq!(faulty.fault_count(Op::Put, FaultKind::Transient), 1);
        assert_eq!(request_keys(faulty.inner()).await.len(), 1);

        let rerun = submit_logs(&store, &policy, matchers, base)
            .await
            .expect("rerun");
        assert_eq!((rerun.created, rerun.already_present), (3, 1));
        assert_eq!(request_keys(faulty.inner()).await.len(), 4);
    }
}
