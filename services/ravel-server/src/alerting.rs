//! Per-tenant alert-rule evaluator (ADR-0043).
//!
//! One background tokio task per tenant that has rules, mirroring
//! [`crate::maintain`]'s shape exactly: a config struct, `spawn`/`run_loop`, a
//! jittered interval, and a `oneshot` shutdown per task. Each tick evaluates
//! every rule configured for that tenant, writes a `Signal::Alerts` record for
//! any state transition, and then notifies the configured sinks.
//!
//! This module is only the driver. The rule shape, the condition test, the
//! state machine, the generation guard, and the record encoding all live in
//! `ravel-alerting`, which is pure logic with no I/O; the query engines are the
//! same `QueryEngine` and `SqlExecutor` instances `/api/v1/query` and
//! `/api/v1/sql` serve from, reused as libraries rather than called over the
//! network (ADR-0043 consequence 2).
//!
//! # No in-memory alert state
//!
//! Ravel's compute processes are disposable, so "how long has this been
//! pending" is never a timer in this process (ADR-0043 decision 3). Every tick
//! folds the tenant's durable `Signal::Alerts` history to the most recent
//! record per `alert_id` and hands that to
//! [`ravel_alerting::evaluate_transition`], which derives pending-duration from
//! the record's own timestamp. A restarted evaluator resumes exactly where the
//! records left it.
//!
//! The one piece of state that is deliberately in-memory is the undelivered
//! notification set ([`AlertEvaluator::run_tick`]). Losing it on a crash would
//! silently downgrade delivery to at-most-once across a restart, since nothing
//! would ever re-enter it for an alert that is still firing but was not the
//! reason for this tick's read. [`AlertEvaluator::bootstrap_undelivered`]
//! closes that gap: the first tick after a (re)start that successfully reads
//! history re-queues every non-terminal alert for one delivery attempt, which
//! is what keeps sinks inside the ADR-0043 decision 6 at-least-once contract
//! across a restart, not just within one process's uptime.
//!
//! # Reading alert history
//!
//! [`AlertEvaluator::load_latest_records`] is a direct RLOG read over the
//! tenant's alert commit records, not a query. It lists
//! `t/<tenant>/a/c/<shard>/`, decodes each commit record, reads the RLOG object
//! it names, and keeps the greatest-timestamp record per `alert_id`. Going
//! through commit records rather than listing data objects is what makes an
//! abandoned write invisible: a data object with no commit record is an orphan
//! and must never be folded into state.
//!
//! This is deliberately not a query planner. The `alerts` SQL table (ADR-0043
//! decision 7) is separate work; the evaluator needs only "the latest record
//! per alert_id for one tenant", and the cost is bounded by the number of
//! transitions, not by ingest volume, because a record is written only on a
//! transition (ADR-0043 decision 4).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ravel_alerting::{
    AlertId, AlertRecord, AlertState, DEFAULT_REPEAT_INTERVAL, QueryResultSummary, Rule,
    RuleCondition, RuleQuery, ThresholdOp, compute_alert_id, condition_met, evaluate_transition,
    write_alert_record,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::rng::{RngSource, SystemRng};
use ravel_commit::{keys, publish, record};
use ravel_ingest::{Clock, LOG_SEGMENT_FORMAT_VERSION};
use ravel_logseg::{ObjectIdentity, Predicate, RlogConfig, RlogReader};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all};
use ravel_promql::Value as PromqlValue;
use ravel_query::{Coverage, QueryEngine};
use ravel_types::{Signal, TenantHash, TenantId};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::alert_sink::{AlertNotification, AlertSink, DEFAULT_SINK_TIMEOUT, deliver};

/// Default `--alert-eval-interval-secs`: 60 seconds, Prometheus' own default
/// rule-evaluation interval.
pub const DEFAULT_ALERT_EVAL_INTERVAL: Duration = Duration::from_secs(60);

/// Default listing window for a SQL detection rule: the query sees segments
/// whose event time falls in the last 5 minutes. Only bounds which segments
/// `Catalog::resolve` lists; the statement's own `WHERE` still applies above
/// the scan.
pub const DEFAULT_SQL_LOOKBACK: Duration = Duration::from_secs(5 * 60);

/// Wall-clock ceiling on one rule's query. Matches the query engine's own
/// default request deadline, so a rule cannot outlive what an HTTP client of
/// the same engine would be granted.
pub const DEFAULT_QUERY_DEADLINE: Duration = Duration::from_secs(30);

/// The shard every alert record is written to.
///
/// Alerts are transition-rate data, not ingest-rate data: one object per state
/// change per rule. Sharding exists to spread flush contention across
/// concurrent writers, and there is exactly one alert writer per tenant per
/// process, so a second shard would only widen the fold's LIST fan-out for no
/// write-path benefit. Fixed rather than configurable so a reader always knows
/// where an alert history lives.
pub const ALERT_SHARD: u32 = 0;

/// The writer epoch every alert record carries. Flush identity is
/// `(writer_id, epoch, seq)` and each evaluator task mints a fresh v4
/// `writer_id` at spawn, so uniqueness comes from the id and the per-task
/// sequence; the epoch is a constant rather than a fencing token because
/// nothing leases the alert keyspace.
const ALERT_WRITER_EPOCH: u64 = 1;

const NS_PER_HOUR: i64 = 3_600 * 1_000_000_000;
const NS_PER_MS: i64 = 1_000_000;

/// Lease lifetime as a multiple of the evaluation interval. A holder
/// renews the lease at the start of every tick, so the lease must outlive a
/// couple of missed ticks (a slow query, jitter, a brief GC pause) or a healthy
/// holder would keep losing it to a standby. Three intervals lets a holder miss
/// two consecutive ticks before a standby may take over, while still bounding
/// how long a genuinely dead holder blocks failover to roughly three intervals.
const LEASE_TTL_TICKS: u32 = 3;

/// The per-tenant alert lease object body: who holds it and until when.
/// Small JSON, mirroring how the rules file is already JSON; the value is
/// advisory coordination state, not a frozen persistent format.
#[derive(Debug, Serialize, Deserialize)]
struct AlertLease {
    /// The holding evaluator's `writer_id`, hex-encoded. Minted fresh per task,
    /// so a replica can distinguish its own lease from a peer's.
    holder: String,
    /// Wall-clock nanosecond at or after which any replica may take the lease
    /// over, even one held by a different `holder`.
    expiry_ns: i64,
}

/// The lease object key for a tenant's alert evaluation.
///
/// Lives under the tenant's alert keyspace (`Signal::Alerts` prefix `a`) but
/// deliberately outside the `c/<shard>/` commit prefix that
/// [`AlertEvaluator::load_latest_records`] folds, so the fold never lists it and
/// never mistakes it for a commit record. It is mutable coordination state, not
/// an immutable data/commit object, so the object-key immutability rule does not
/// apply to it.
fn alert_lease_key(tenant: &TenantHash) -> String {
    format!("t/{}/a/alert-lease", tenant.to_hex())
}

/// The query engines an evaluator runs rules against: the very instances the
/// query endpoints serve from, not a second construction of them.
#[derive(Clone)]
pub struct AlertQueryEngines {
    /// The `QueryEngine` behind `/api/v1/query`, for [`RuleQuery::Promql`].
    pub promql: Arc<QueryEngine>,
    /// The `SqlExecutor` behind `/api/v1/sql`, for [`RuleQuery::Sql`]. `None`
    /// in a build with the `sql` feature on but no SQL surface mounted.
    #[cfg(feature = "sql")]
    pub sql: Option<Arc<ravel_sql::SqlExecutor>>,
}

/// Everything the evaluator task needs beyond the store and the engines.
/// Shaped after [`crate::maintain::MaintenanceTaskConfig`].
#[derive(Debug, Clone)]
pub struct AlertEvalConfig {
    pub enabled: bool,
    pub interval: Duration,
    /// Static per-tenant rules (ADR-0043 decision 2), loaded once at startup by
    /// [`load_rules_file`]. One evaluator task is spawned per key.
    pub rules: Arc<HashMap<TenantHash, Vec<Rule>>>,
    /// Notification targets, shared by every tenant's evaluator.
    pub sinks: Arc<Vec<AlertSink>>,
    /// Wall deadline for one rule's query.
    pub query_deadline: Duration,
    /// Wall deadline for one sink HTTP request.
    pub sink_timeout: Duration,
    /// Listing window a SQL rule's query resolves over, ending at the tick's
    /// clock reading.
    pub sql_lookback: Duration,
}

impl Default for AlertEvalConfig {
    fn default() -> Self {
        AlertEvalConfig {
            enabled: false,
            interval: DEFAULT_ALERT_EVAL_INTERVAL,
            rules: Arc::new(HashMap::new()),
            sinks: Arc::new(Vec::new()),
            query_deadline: DEFAULT_QUERY_DEADLINE,
            sink_timeout: DEFAULT_SINK_TIMEOUT,
            sql_lookback: DEFAULT_SQL_LOOKBACK,
        }
    }
}

/// Handle to every spawned evaluator task, for clean shutdown (mirrors
/// [`crate::maintain::MaintenanceTasks`]).
pub struct AlertEvalTasks {
    shutdown: Vec<oneshot::Sender<()>>,
    handles: Vec<JoinHandle<()>>,
}

impl AlertEvalTasks {
    pub fn none() -> Self {
        AlertEvalTasks {
            shutdown: Vec::new(),
            handles: Vec::new(),
        }
    }

    pub async fn shutdown(self) {
        for tx in self.shutdown {
            let _ = tx.send(());
        }
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}

/// Spawn one evaluator loop per tenant that has rules. Returns immediately;
/// tasks run until [`AlertEvalTasks::shutdown`].
///
/// Fails only if the shared HTTP client cannot be built, which is a startup
/// misconfiguration rather than a runtime condition.
pub fn spawn(
    store: Arc<dyn ObjectStoreBackend>,
    engines: AlertQueryEngines,
    clock: Arc<dyn Clock>,
    config: AlertEvalConfig,
) -> anyhow::Result<AlertEvalTasks> {
    if !config.enabled || config.rules.is_empty() {
        return Ok(AlertEvalTasks::none());
    }

    let mut shutdown = Vec::new();
    let mut handles = Vec::new();
    // Sorted so the spawn order (and therefore the log order) is stable across
    // runs; a HashMap iteration order is not.
    let mut tenants: Vec<TenantHash> = config.rules.keys().copied().collect();
    tenants.sort_unstable_by_key(|t| t.0);

    for tenant in tenants {
        let Some(rules) = config.rules.get(&tenant) else {
            continue;
        };
        let mut evaluator = AlertEvaluator::new(
            store.clone(),
            engines.clone(),
            clock.clone(),
            tenant,
            rules.clone(),
            &config,
        )?;
        // Warn once per rule whose repeat cadence is finer than the tick can
        // deliver: the effective cadence is `repeat_interval` rounded up to the
        // next tick, so a sub-tick interval is silently coarsened to the tick
        // (ADR-0043 "repeat notifications while firing", decision 3). A `None`
        // interval uses the default and an explicit zero disables repeats, so
        // neither warns.
        for rule in rules.iter() {
            if let Some(repeat) = rule.repeat_interval
                && !repeat.is_zero()
                && repeat < config.interval
            {
                tracing::warn!(
                    tenant = %tenant.to_hex(),
                    rule_id = %rule.rule_id,
                    repeat_interval_secs = repeat.as_secs(),
                    eval_interval_secs = config.interval.as_secs(),
                    "alert rule repeat_interval is shorter than the evaluation interval; the \
                     tick bounds the effective repeat cadence"
                );
            }
        }
        let (tx, rx) = oneshot::channel();
        let interval = config.interval;
        // Production OS-entropy jitter (ADR-0068 decision 2); the harness does
        // not drive alert evaluators, so there is no injected variant.
        let rng: Arc<dyn RngSource> = Arc::new(SystemRng);
        let handle = tokio::spawn(async move {
            run_loop(&mut evaluator, interval, rng, rx).await;
        });
        shutdown.push(tx);
        handles.push(handle);
        tracing::info!(
            tenant = %tenant.to_hex(),
            rules = config.rules.get(&tenant).map_or(0, Vec::len),
            interval_secs = config.interval.as_secs(),
            "alert evaluator started"
        );
    }
    Ok(AlertEvalTasks { shutdown, handles })
}

async fn run_loop(
    evaluator: &mut AlertEvaluator,
    interval: Duration,
    rng: Arc<dyn RngSource>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval, rng.as_ref())) => {}
            _ = &mut shutdown => return,
        }
        let report = evaluator.run_tick().await;
        tracing::info!(
            tenant = %evaluator.tenant.to_hex(),
            rules_evaluated = report.rules_evaluated,
            rules_failed = report.rules_failed,
            records_written = report.records_written,
            repeats_queued = report.repeats_queued,
            notifications_delivered = report.notifications_delivered,
            notifications_failed = report.notifications_failed,
            "alert evaluation tick complete"
        );
    }
}

/// Up to 10% jitter over `base`, so co-started replicas' evaluation ticks do
/// not run in lockstep (same rationale as the fold and maintenance tasks).
fn jittered(base: Duration, rng: &dyn RngSource) -> Duration {
    let jitter_bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if jitter_bound_ms == 0 {
        return base;
    }
    let extra_ms = rng.jitter_ms(jitter_bound_ms);
    base + Duration::from_millis(extra_ms)
}

/// What one tick did, for logs and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AlertEvalReport {
    /// Rules whose query ran and whose condition was decided.
    pub rules_evaluated: u32,
    /// Rules skipped this tick because the query, the condition, or the write
    /// failed. Every one is logged; the rule is retried next tick.
    pub rules_failed: u32,
    /// Transition records durably written this tick.
    pub records_written: u32,
    /// Repeat notifications this tick queued for a still-`Firing` alert
    /// (ADR-0043 "repeat notifications while firing" amendment). A repeat
    /// re-sends the folded latest record with no new durable write; it is
    /// delivered by the same `flush_sinks` path, so it is also counted in
    /// `notifications_delivered` once a sink accepts it.
    pub repeats_queued: u32,
    /// Notifications this tick delivered to every configured sink, including
    /// ones carried over from an earlier tick's failure.
    pub notifications_delivered: u32,
    /// Notifications still undelivered after this tick's attempt.
    pub notifications_failed: u32,
    /// `true` when the alert history could not be read, so no rule was
    /// evaluated at all. Never acts on a partial history: doing so would
    /// re-fire an alert that is already firing.
    pub history_unavailable: bool,
    /// `true` when another replica held the tenant's alert lease this tick, so
    /// this replica skipped rule evaluation. Expected steady state in a
    /// multi-replica deployment, not an error.
    pub lease_not_held: bool,
    /// `true` when the lease could not be acquired or renewed because the
    /// object store failed. Distinct from `lease_not_held`: this is a
    /// store error, not a peer holding the lease. Rule evaluation is skipped and
    /// retried next tick.
    pub lease_unavailable: bool,
}

/// The evaluator for one tenant.
pub struct AlertEvaluator {
    store: Arc<dyn ObjectStoreBackend>,
    engines: AlertQueryEngines,
    clock: Arc<dyn Clock>,
    tenant: TenantHash,
    rules: Vec<Rule>,
    sinks: Arc<Vec<AlertSink>>,
    http: reqwest::Client,
    query_deadline: Duration,
    /// How long a lease this evaluator writes stays valid before any replica may
    /// take it over. Derived from the evaluation interval at construction
    /// ([`LEASE_TTL_TICKS`] times it).
    lease_ttl: Duration,
    /// Only read by the `sql` feature's [`AlertEvaluator::run_sql`]; a build
    /// without that feature rejects SQL rules before it would be needed.
    #[cfg_attr(not(feature = "sql"), allow(dead_code))]
    sql_lookback: Duration,
    /// Fresh per task, so `(writer_id, epoch, seq)` is unique without any
    /// cross-process coordination.
    writer_id: Uuid,
    next_seq: u64,
    /// Transitions written but not yet accepted by every sink, keyed by
    /// `alert_id` so a newer transition supersedes an older undelivered one.
    /// Bounded by the rule count.
    undelivered: HashMap<AlertId, AlertNotification>,
    /// Per-alert duplicate suppressor for the repeat pass (ADR-0043 "repeat
    /// notifications while firing" amendment, decision 2): the
    /// `(anchor_ts_ns, window)` of the last repeat window a notification was
    /// queued in. `anchor_ts_ns` is the firing record's own `ts_ns`; a mark
    /// whose anchor differs from the current firing record's `ts_ns` is stale
    /// (a resolve-then-refire re-anchored the episode) and is ignored, so the
    /// new episode re-anchors cleanly instead of inheriting the old window
    /// count. This is a suppressor, not the schedule: the schedule is derived
    /// from `record.ts_ns` plus the clock every tick, so losing this map
    /// (restart, lease failover) costs at most one extra send and never a
    /// silence.
    repeat_marks: HashMap<AlertId, (i64, u64)>,
    /// Set after the first tick that successfully reads alert history.
    /// `undelivered` is process-local, so a restart loses whatever was
    /// in flight; without this, that loss is permanent (a still-firing alert
    /// that was mid-retry when the process died is never notified again,
    /// since nothing re-enters `undelivered` for it). On the first successful
    /// tick, every alert whose folded state is not terminal is seeded into
    /// `undelivered` once, which is what keeps sink delivery inside the
    /// at-least-once contract ADR-0043 decision 6 actually states, rather
    /// than at-most-once across a restart.
    bootstrapped: bool,
}

impl AlertEvaluator {
    /// Build an evaluator for one tenant. `rules` are that tenant's rules only.
    pub fn new(
        store: Arc<dyn ObjectStoreBackend>,
        engines: AlertQueryEngines,
        clock: Arc<dyn Clock>,
        tenant: TenantHash,
        rules: Vec<Rule>,
        config: &AlertEvalConfig,
    ) -> anyhow::Result<AlertEvaluator> {
        let http = reqwest::Client::builder()
            .timeout(config.sink_timeout)
            .build()?;
        Ok(AlertEvaluator {
            store,
            engines,
            clock,
            tenant,
            rules,
            sinks: config.sinks.clone(),
            http,
            query_deadline: config.query_deadline,
            lease_ttl: config.interval * LEASE_TTL_TICKS,
            sql_lookback: config.sql_lookback,
            writer_id: Uuid::new_v4(),
            next_seq: 1,
            undelivered: HashMap::new(),
            repeat_marks: HashMap::new(),
            bootstrapped: false,
        })
    }

    /// One evaluation pass over every rule of this tenant, then one delivery
    /// pass over every undelivered notification.
    ///
    /// Split out from [`run_loop`] so a test can drive a single deterministic
    /// tick without the timer, exactly as [`crate::maintain::run_tick`] is.
    ///
    /// Ordering is the ADR-0043 decision 6 guarantee in code: every record is
    /// PUT and its commit record published before [`Self::flush_sinks`] is
    /// reached, and a sink failure only leaves an entry in `undelivered` for
    /// the next tick. No sink result can prevent, delay past its own write, or
    /// alter a record.
    pub async fn run_tick(&mut self) -> AlertEvalReport {
        let mut report = AlertEvalReport::default();
        let now_ns = self.clock.now_ns();

        let mut latest = match self.load_latest_records().await {
            Ok(latest) => latest,
            Err(err) => {
                tracing::warn!(
                    tenant = %self.tenant.to_hex(),
                    error = %err,
                    "alert evaluation: could not read alert history; skipping tick"
                );
                report.history_unavailable = true;
                // Still attempt delivery: a notification stuck from an earlier
                // tick does not depend on this tick's history read.
                self.flush_sinks(&mut report).await;
                return report;
            }
        };

        if !self.bootstrapped {
            self.bootstrap_undelivered(&latest);
            self.bootstrapped = true;
        }

        // only the lease holder for this tenant evaluates rules and writes
        // transition records this tick, so two `--mode all`/`--mode query`
        // replicas configured with the same rules do not both fire and notify
        // every transition. History reading and bootstrap above are unguarded on
        // purpose (they are read-only and idempotent, and bootstrap must still
        // recover this process's own in-flight notifications after a restart);
        // only the write path is gated. `flush_sinks` below always runs, so a
        // notification already queued at this replica is still retried even on a
        // tick where a peer holds the lease.
        let hold_lease = match self.acquire_lease(now_ns).await {
            Ok(held) => held,
            Err(err) => {
                tracing::warn!(
                    tenant = %self.tenant.to_hex(),
                    error = %err,
                    "alert evaluation: could not acquire the tenant lease; skipping rule \
                     evaluation this tick"
                );
                report.lease_unavailable = true;
                false
            }
        };

        if hold_lease {
            // `rules` is moved out for the loop so `self` stays mutably
            // borrowable for the write path; it is put back before returning.
            let rules = std::mem::take(&mut self.rules);
            for rule in &rules {
                match self.evaluate_rule(rule, &mut latest, now_ns).await {
                    Ok(written) => {
                        report.rules_evaluated += 1;
                        if written {
                            report.records_written += 1;
                        }
                    }
                    Err(err) => {
                        report.rules_failed += 1;
                        tracing::warn!(
                            tenant = %self.tenant.to_hex(),
                            rule_id = %rule.rule_id,
                            error = %err,
                            "alert evaluation: rule failed; retried next tick"
                        );
                    }
                }
            }
            // Repeat pass (ADR-0043 "repeat notifications while firing"): after
            // rule evaluation, on the lease holder only, re-queue a notification
            // for each rule whose folded latest record is still Firing and whose
            // repeat window has advanced. This reads the same `latest` the loop
            // above updated on any transition, so a rule that just resolved this
            // tick folds to Resolved here and does not repeat. No durable record
            // is written for a repeat (decision 4 stands); it rides the existing
            // `undelivered` map and `flush_sinks` drain.
            for rule in &rules {
                self.queue_repeat_if_due(rule, &latest, now_ns, &mut report);
            }
            self.rules = rules;
        } else if !report.lease_unavailable {
            report.lease_not_held = true;
            tracing::debug!(
                tenant = %self.tenant.to_hex(),
                "alert evaluation: another replica holds the tenant lease; skipping rule \
                 evaluation this tick"
            );
        }

        self.flush_sinks(&mut report).await;
        report
    }

    /// Try to own this tenant's alert lease for this tick.
    ///
    /// Two replicas can be configured with the same rules for the same tenant.
    /// Their folds are independent, so without coordination both evaluate every
    /// tick, both write a transition record, and both notify it. This is a
    /// lightweight object-store lease, not a general leader election: the
    /// evaluator tries to own a small lease object under the tenant's alert
    /// keyspace before it writes anything.
    ///
    /// Returns `Ok(true)` when this replica now holds the lease (it created it,
    /// renewed its own, or took over an expired one), `Ok(false)` when a live
    /// lease is held by a different replica or a takeover race was lost, and
    /// `Err` only on an object-store failure.
    ///
    /// The lease is not a fencing token and does not make the record write
    /// mutually exclusive at the store layer: alert records are still unique by
    /// `(writer_id, epoch, seq)`, so a brief two-owner overlap during a handover
    /// at worst duplicates a transition record and its at-least-once
    /// notification, which the fold's `(ts_ns, epoch, seq)` tie-break already
    /// tolerates and ADR-0043 decision 6 already permits. What it removes is the
    /// steady-state case of two healthy replicas both firing every rule every
    /// tick.
    async fn acquire_lease(&self, now_ns: i64) -> anyhow::Result<bool> {
        let key = alert_lease_key(&self.tenant);
        let identity = self.writer_id.to_string();
        let ttl_ns = i64::try_from(self.lease_ttl.as_nanos()).unwrap_or(i64::MAX);
        let lease = AlertLease {
            holder: identity.clone(),
            expiry_ns: now_ns.saturating_add(ttl_ns),
        };
        let body = Bytes::from(serde_json::to_vec(&lease)?);

        // Fast path: no lease object yet. `CreateIfAbsent` is atomic (ADR-0002),
        // so at most one racing replica wins the create.
        match self
            .store
            .put(&key, body.clone(), PutOptions::create_if_absent())
            .await
        {
            Ok(_) => return Ok(true),
            Err(StoreError::AlreadyExists) => {}
            Err(err) => return Err(err.into()),
        }

        // A lease already exists. Read it: we may renew our own or take over one
        // that has expired, but never displace a live lease held by a peer.
        let current = self.store.get(&key, GetRange::Full).await?;
        let existing: AlertLease = serde_json::from_slice(&current.data)?;
        let held_by_other = existing.holder != identity;
        if held_by_other && existing.expiry_ns > now_ns {
            return Ok(false);
        }

        // Ours to renew, or expired and up for grabs. Version-guard the write so
        // two replicas that both observe the same expired lease cannot both take
        // it: exactly one CAS succeeds, the loser skips this tick.
        match self
            .store
            .put(
                &key,
                body,
                PutOptions {
                    mode: PutMode::CasVersion(current.version),
                    checksum: None,
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(StoreError::PreconditionFailed) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Seeds `undelivered` from durable history, once, on the first tick that
    /// successfully reads it. Every alert whose folded state is `Pending` or
    /// `Firing` (a live, unresolved episode) is queued for one delivery
    /// attempt this tick; `Resolved` needs no notification restart (nothing
    /// is pending on it), and `Suppressed` is an intentional silence that a
    /// restart must not undo by notifying on it.
    ///
    /// Without this, `undelivered` is empty on every fresh process, so a
    /// notification stuck in flight when the process died before this fix is
    /// never retried: the record it names is durable and correct, but the
    /// sink call for it is gone forever, silently downgrading the ADR-0043
    /// decision 6 at-least-once contract to at-most-once across a restart.
    /// `previous_state: None` here is a known approximation (this evaluator
    /// has no prior record to pair the seeded one with, only the latest),
    /// the same approximation `AlertNotification::new` already documents for
    /// a pending-then-firing episode longer than one transition.
    fn bootstrap_undelivered(&mut self, latest: &HashMap<AlertId, AlertRecord>) {
        for (alert_id, record) in latest {
            if matches!(record.state, AlertState::Pending | AlertState::Firing) {
                self.undelivered
                    .entry(*alert_id)
                    .or_insert_with(|| AlertNotification::new(record.clone(), None));
            }
        }
    }

    /// Re-queue a repeat notification for `rule` when its folded latest record
    /// is still `Firing` and the current repeat window has not yet been queued
    /// (ADR-0043 "repeat notifications while firing" amendment).
    ///
    /// The window index is
    ///
    /// ```text
    /// k = (now_ns - record.ts_ns) / repeat_interval
    /// ```
    ///
    /// with integer division and a backward-clock-step clamp to zero (matching
    /// the pending-duration clamp elsewhere in this file). A repeat is due when
    /// `k >= 1` and the per-alert mark shows window `k` was not already queued
    /// for this firing episode.
    ///
    /// # No durable write, ever
    ///
    /// This method touches only `undelivered` and `repeat_marks`. It never calls
    /// [`Self::publish`] or [`write_alert_record`], so a repeat can never write a
    /// durable record: the record is state, a notification is not (decision 4).
    ///
    /// # Anchor staleness
    ///
    /// A mark whose `anchor_ts_ns` differs from the current firing record's
    /// `ts_ns` is from a prior episode (a resolve-then-refire re-anchored the
    /// alert) and is ignored, so a new episode re-anchors cleanly instead of
    /// inheriting the old episode's window count and staying silent. Losing the
    /// mark entirely (restart, lease failover) costs at most one extra send, not
    /// a silence, because `k` is re-derived from the durable record every tick.
    ///
    /// # Only firing repeats
    ///
    /// `Pending`, `Resolved`, and `Suppressed` never repeat, and a rule with no
    /// folded record (it never fired) has nothing to repeat. A rule removed from
    /// the config is not in `self.rules`, so this method is never called for it
    /// and its alert falls silent for Alertmanager to auto-resolve.
    fn queue_repeat_if_due(
        &mut self,
        rule: &Rule,
        latest: &HashMap<AlertId, AlertRecord>,
        now_ns: i64,
        report: &mut AlertEvalReport,
    ) {
        // `None` is the default cadence; an explicit zero disables repeats for
        // this rule (a consumer that opens a ticket per POST).
        let interval = rule.repeat_interval.unwrap_or(DEFAULT_REPEAT_INTERVAL);
        if interval.is_zero() {
            return;
        }
        let alert_id = compute_alert_id(&rule.rule_id, &rule.labels);
        let Some(record) = latest.get(&alert_id) else {
            return;
        };
        if record.state != AlertState::Firing {
            return;
        }
        // `.max(1)` guards against a zero interval_ns from an overflow saturation
        // (unreachable: the zero interval already returned above), keeping the
        // division well-defined.
        let interval_ns = i64::try_from(interval.as_nanos())
            .unwrap_or(i64::MAX)
            .max(1);
        let elapsed_ns = now_ns.saturating_sub(record.ts_ns).max(0);
        let window = (elapsed_ns / interval_ns) as u64;
        if window < 1 {
            return;
        }
        let already_queued = match self.repeat_marks.get(&alert_id) {
            // Same episode: this window is already covered only if a mark at or
            // beyond it exists.
            Some((anchor, marked)) if *anchor == record.ts_ns => *marked >= window,
            // No mark, or a mark from a prior (resolved) episode: not covered.
            _ => false,
        };
        if already_queued {
            return;
        }
        // Compose with any entry already queued for this alert this tick (a
        // fresh transition write, or a bootstrap redelivery): the map is keyed
        // by `alert_id`, so `or_insert_with` never doubles a send, and that
        // existing send counts as this window's send. `previous_state: None`
        // mirrors `bootstrap_undelivered`: a repeat is a non-transition re-send
        // with no prior record to pair, the same single-step-fold approximation
        // `AlertNotification::new` already documents.
        self.undelivered
            .entry(alert_id)
            .or_insert_with(|| AlertNotification::new(record.clone(), None));
        self.repeat_marks.insert(alert_id, (record.ts_ns, window));
        report.repeats_queued += 1;
    }

    /// Evaluate one rule: run its query, decide the condition, fold to the
    /// prior record, and write a record if this is a transition. Returns
    /// whether a record was written.
    async fn evaluate_rule(
        &mut self,
        rule: &Rule,
        latest: &mut HashMap<AlertId, AlertRecord>,
        now_ns: i64,
    ) -> anyhow::Result<bool> {
        let alert_id = compute_alert_id(&rule.rule_id, &rule.labels);
        let summary = self.run_query(rule, now_ns).await?;
        let met = condition_met(&rule.condition, &summary)?;
        let prior = latest.get(&alert_id).cloned();
        let transition = evaluate_transition(rule, prior.as_ref(), met, now_ns);

        if !transition.write_record {
            return Ok(false);
        }

        // guard against a backward wall-clock step (an NTP correction can
        // move `now_ns` behind the prior record's `ts_ns`).
        // `load_latest_records` orders records by `(ts_ns, epoch, seq)` with
        // `ts_ns` first, so a record stamped at or before its predecessor sorts
        // behind it and never becomes the folded "latest" -- the evaluator would
        // then re-transition from the same stale state every tick instead of
        // converging. Stamp the new record strictly after the prior one so the
        // per-alert sequence is monotonic regardless of clock jitter. The
        // transition decision above still uses the true `now_ns`; pending-
        // duration elapse already clamps a backward step to zero, so only the
        // durable stamp needs correcting here.
        let stamp_ns = match prior.as_ref() {
            Some(p) => now_ns.max(p.ts_ns.saturating_add(1)),
            None => now_ns,
        };

        let seq = self.next_seq;
        self.next_seq += 1;
        let identity = ObjectIdentity {
            tenant_hash: self.tenant.0,
            shard: ALERT_SHARD,
            writer_id: self.writer_id.into_bytes(),
            writer_epoch: ALERT_WRITER_EPOCH,
            writer_seq: seq,
        };
        // `input_alert_generations` is empty because no rule can consume alert
        // records yet: the `alerts` SQL table (ADR-0043 decision 7) is separate
        // work. `compute_generation` handles that case explicitly - a rule
        // whose query names the alerts table still gets generation 1, its
        // structural hop depth, rather than being mistaken for an ordinary
        // metric rule. Once the table lands, the generations of the rows a
        // tick actually consumed pass through here.
        let Some(bytes) = write_alert_record(
            rule,
            &transition,
            prior.as_ref(),
            &[],
            stamp_ns,
            RlogConfig::default(),
            identity,
        )?
        else {
            return Ok(false);
        };

        // Decode what was encoded rather than rebuilding it: the notification
        // then carries exactly the record that is about to become durable,
        // generation included, with no second computation to drift.
        let written = decode_single_record(&bytes)?;
        // Publish with the same corrected stamp so the commit record's event
        // time matches the record's `ts_ns`.
        self.publish(bytes, seq, stamp_ns).await?;

        tracing::info!(
            tenant = %self.tenant.to_hex(),
            rule_id = %rule.rule_id,
            alert_id = %written.alert_id.to_hex(),
            state = written.state.as_str(),
            generation = written.generation,
            "alert transition recorded"
        );

        // The record is durable from here on; everything below is notification.
        self.undelivered.insert(
            alert_id,
            AlertNotification::new(written.clone(), prior.as_ref()),
        );
        latest.insert(alert_id, written);
        Ok(true)
    }

    /// Run a rule's query and summarize the result into the shape its condition
    /// tests.
    ///
    /// Partial federated coverage is a failed evaluation, not a usable answer
    /// (ADR-0071 "partial results are consent-gated and envelope-visible"
    /// amendment, decision 5). A rule evaluated over a result that omits a
    /// skipped remote's data can fire, or worse resolve, on data that was never
    /// read. Returning an error here puts the rule on the existing per-rule
    /// failure path in [`Self::run_tick`]: it is logged, counted in
    /// `rules_failed`, the prior alert state is left exactly as it was, no
    /// transition record is written, and the next tick retries. A per-rule
    /// opt-in to evaluate on partial coverage is deliberately not offered here;
    /// the amendment leaves that to the alerting surface's own work.
    async fn run_query(&self, rule: &Rule, now_ns: i64) -> anyhow::Result<QueryResultSummary> {
        match &rule.query {
            RuleQuery::Promql(text) => {
                let (value, coverage) = self
                    .engines
                    .promql
                    .instant(
                        self.tenant,
                        text,
                        now_ns.div_euclid(NS_PER_MS),
                        &[],
                        now_ns,
                        self.query_deadline,
                    )
                    .await?;
                if let Coverage::Partial { skipped } = coverage {
                    anyhow::bail!(
                        "alert rule query ran over partial federated coverage (degraded \
                         clusters: {}); refusing to evaluate the rule on an incomplete result",
                        skipped.join(", ")
                    );
                }
                promql_summary(value)
            }
            RuleQuery::Sql(text) => self.run_sql(text, now_ns).await,
        }
    }

    #[cfg(feature = "sql")]
    async fn run_sql(&self, text: &str, now_ns: i64) -> anyhow::Result<QueryResultSummary> {
        let Some(executor) = self.engines.sql.as_ref() else {
            anyhow::bail!("SQL alert rule needs a SQL executor, and this process mounts none");
        };
        let lookback_ns = i64::try_from(self.sql_lookback.as_nanos()).unwrap_or(i64::MAX);
        let request = ravel_sql::SqlRequest {
            sql: text.to_string(),
            window: ravel_types::TimeRange {
                start_ns: now_ns.saturating_sub(lookback_ns),
                end_ns: now_ns,
            },
            // A rule reads whatever is committed at tick time; there is no
            // read-your-write token to honour, because nothing wrote on this
            // rule's behalf.
            min_tokens: Vec::new(),
            now_ns,
            deadline: self.query_deadline,
        };
        let outcome = executor.execute(self.tenant, &request).await?;
        Ok(QueryResultSummary::RowCount(
            outcome.output.num_rows() as u64
        ))
    }

    #[cfg(not(feature = "sql"))]
    async fn run_sql(&self, _text: &str, _now_ns: i64) -> anyhow::Result<QueryResultSummary> {
        anyhow::bail!(
            "SQL alert rules require the `sql` feature; this build of ravel-server has it disabled"
        )
    }

    /// PUT the RLOG object and publish its commit record, the same two-step any
    /// signal-tagged write in this codebase performs: a `CreateIfAbsent` data
    /// PUT with a CRC32C upload checksum, then a `CreateIfAbsent` commit record
    /// (ADR-0002). Until the commit record lands the object is an orphan, and
    /// [`Self::load_latest_records`] does not read orphans.
    async fn publish(&self, bytes: Vec<u8>, seq: u64, now_ns: i64) -> anyhow::Result<()> {
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data = Bytes::from(bytes);
        let commit = record::build(NewCommitRecord {
            tenant_hash: self.tenant,
            signal: Signal::Alerts,
            shard: ALERT_SHARD,
            writer_id: self.writer_id,
            writer_epoch: ALERT_WRITER_EPOCH,
            writer_seq: seq,
            object_size: data.len() as u64,
            content_hash,
            // One alert record per object, on one stream (the rule's).
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: now_ns,
            max_event_ts_ns: now_ns,
            min_ingest_ts_ns: now_ns,
            max_ingest_ts_ns: now_ns,
            segment_format_version: u32::from(LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns: now_ns,
            ingest_hour_bucket: hour_bucket(now_ns),
        })?;
        let data_key = keys::reconstruct_data_key(&commit)?;
        publish::put_data_object(self.store.as_ref(), &data_key, data).await?;
        publish::publish(self.store.as_ref(), &commit, &RetryPolicy::default()).await?;
        Ok(())
    }

    /// Fold this tenant's whole alert history to the most recent record per
    /// `alert_id` (ADR-0040 decision 3).
    ///
    /// Records are reached through commit records, never by listing data
    /// objects: an object whose commit never landed is an abandoned write and
    /// must not influence state. Ties on `ts_ns` break on the writer's
    /// `(epoch, seq)`, which is monotonic per writer, so two records stamped in
    /// the same nanosecond still fold deterministically.
    ///
    /// Any malformed entry aborts the whole read rather than being skipped: a
    /// partial history would look like "this alert is not firing" and re-fire
    /// an alert that already is.
    async fn load_latest_records(&self) -> anyhow::Result<HashMap<AlertId, AlertRecord>> {
        let prefix = keys::commit_shard_prefix(&self.tenant, Signal::Alerts, ALERT_SHARD)?;
        let entries = list_all(self.store.as_ref(), &prefix).await?;

        let cfg = RlogConfig::default();
        let mut best: HashMap<AlertId, ((i64, u64, u64), AlertRecord)> = HashMap::new();
        for meta in entries {
            let parsed = match keys::partition_bucket_entry(&meta.key)? {
                keys::BucketEntry::CommitRecord(parsed) => parsed,
                // tolerate (skip) a compaction record rather than
                // hard-erroring. Alerts are not a maintained signal today
                // (`Signal::Alerts` is absent from `maintain::MAINTAINED_SIGNALS`),
                // so nothing writes a `CompactionRecord` under this prefix yet.
                // But the moment compaction is enabled for this signal, one will
                // appear here, and a fold that bailed on it would hard-error
                // every tick. Skip it so the fold keeps working from whatever L0
                // `CommitRecord`s remain reachable.
                //
                // Scope note: this is only "does not crash". Once compaction is
                // actually enabled for `Signal::Alerts`, the records folded into
                // the L1 part this `CompactionRecord` names must be read from
                // that part for full correctness. Reading L1 alert parts is out
                // of scope here (no producer exists yet) and is deliberately left
                // to the change that turns compaction on for this signal.
                keys::BucketEntry::CompactionRecord(_) => continue,
                // A retention tombstone (or any other shape) under the alerts
                // commit prefix is still unexpected and a real signal of layout
                // drift; refuse rather than guess.
                other => anyhow::bail!(
                    "unexpected {other:?} under the alerts commit prefix; the evaluator folds \
                     only L0 alert commit records and skips L1 compaction records"
                ),
            };
            let commit = record::decode(&self.store.get(&meta.key, GetRange::Full).await?.data)?;
            let data_key = keys::verify_object_key(&commit)?;
            let object = self.store.get(&data_key, GetRange::Full).await?;
            let reader = RlogReader::new(&object.data, &cfg)?;
            let (rows, _stats) = reader.scan(&Predicate::And(Vec::new()))?;
            for row in &rows {
                let alert = AlertRecord::from_log_record(row)?;
                let order = (alert.ts_ns, parsed.epoch, parsed.seq);
                match best.get(&alert.alert_id) {
                    Some((seen, _)) if *seen >= order => {}
                    _ => {
                        best.insert(alert.alert_id, (order, alert));
                    }
                }
            }
        }
        Ok(best
            .into_iter()
            .map(|(id, (_order, record))| (id, record))
            .collect())
    }

    /// Attempt delivery of every undelivered notification to every sink,
    /// dropping an entry only once all sinks accepted it.
    ///
    /// A sink that fails leaves its notification in place, so the next tick
    /// retries it from the latest record (ADR-0043 decision 6) without needing
    /// a new transition to occur. A partial success re-sends to the sinks that
    /// already accepted, which is inside the at-least-once contract.
    async fn flush_sinks(&mut self, report: &mut AlertEvalReport) {
        if self.sinks.is_empty() {
            self.undelivered.clear();
            return;
        }
        let pending: Vec<(AlertId, AlertNotification)> = self
            .undelivered
            .iter()
            .map(|(id, n)| (*id, n.clone()))
            .collect();
        for (alert_id, notification) in pending {
            let mut all_ok = true;
            for sink in self.sinks.iter() {
                if let Err(err) = deliver(&self.http, sink, &notification).await {
                    all_ok = false;
                    tracing::warn!(
                        tenant = %self.tenant.to_hex(),
                        sink = sink.kind(),
                        url = sink.url(),
                        alert_id = %alert_id.to_hex(),
                        error = %err,
                        "alert sink delivery failed; the record is durable and delivery is \
                         retried next tick"
                    );
                }
            }
            if all_ok {
                self.undelivered.remove(&alert_id);
                report.notifications_delivered += 1;
            } else {
                report.notifications_failed += 1;
            }
        }
    }
}

/// The hour bucket a commit record stamped at `now_ns` belongs to. A pre-epoch
/// reading yields 0, which `ravel_commit::record::validate` treats as "not
/// meaningfully set" and skips, rather than failing the write on a skewed
/// clock.
fn hour_bucket(now_ns: i64) -> u32 {
    u32::try_from(now_ns.div_euclid(NS_PER_HOUR)).unwrap_or(0)
}

/// Decode the single alert record out of a freshly encoded one-record object.
fn decode_single_record(bytes: &[u8]) -> anyhow::Result<AlertRecord> {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(bytes, &cfg)?;
    let (rows, _stats) = reader.scan(&Predicate::And(Vec::new()))?;
    let [row] = rows.as_slice() else {
        anyhow::bail!(
            "an alert object must hold exactly one record, found {}",
            rows.len()
        );
    };
    Ok(AlertRecord::from_log_record(row)?)
}

/// Map a PromQL result onto the numeric summary a threshold condition tests.
///
/// A scalar result is a one-element vector: `condition_met` asks whether *any*
/// value satisfies the comparator, and a scalar has exactly one. A range vector
/// or string is a rule-authoring error, surfaced rather than silently treated
/// as "not firing".
///
/// Native-histogram elements are dropped: their `value` is a `0.0` placeholder
/// (the real data is in the histogram), and comparing that against a threshold
/// would fire on a meaningless zero. This matches Prometheus, which drops
/// histogram samples from float-only operations.
fn promql_summary(value: PromqlValue) -> anyhow::Result<QueryResultSummary> {
    match value {
        PromqlValue::Vector(samples) => Ok(QueryResultSummary::Numeric(
            samples
                .iter()
                .filter(|s| s.histogram.is_none())
                .map(|s| s.value)
                .collect(),
        )),
        PromqlValue::Scalar(v) => Ok(QueryResultSummary::Numeric(vec![v])),
        other => anyhow::bail!(
            "a PromQL alert rule must evaluate to an instant vector or a scalar, got {}",
            other.type_name()
        ),
    }
}

// --- Rule configuration (ADR-0043 decision 2) ------------------------------
//
// Rules come from a JSON file named by `--alert-rules-file`, not from a
// repeatable CLI flag. `--tenant-token TOKEN=TENANT` and
// `--retention-tenant TENANT=DURATION` are repeatable flags because their value
// is a single scalar per tenant; a rule is not. It carries free-form PromQL or
// SQL text (spaces, quotes, `=`, `>`), a label map, an annotation map, and an
// optional duration, and squeezing that into a `KEY=VALUE` flag would mean
// inventing an escaping mini-language for shell-hostile query text. ADR-0043
// decision 2 explicitly allows either form.
//
// JSON rather than YAML because `serde_json` is already a workspace dependency
// and no YAML parser is; the file is also valid YAML for anyone who prefers to
// author it that way and convert.

/// The `--alert-rules-file` document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertRulesFile {
    pub rules: Vec<RuleSpec>,
}

/// One rule as written in the config file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSpec {
    /// The tenant id this rule belongs to, matching a `--tenant-token`'s
    /// tenant.
    pub tenant: String,
    pub rule_id: String,
    /// The PromQL expression to evaluate. Exactly one of `promql` or `sql`.
    #[serde(default)]
    pub promql: Option<String>,
    /// The SQL statement to evaluate. Exactly one of `promql` or `sql`.
    #[serde(default)]
    pub sql: Option<String>,
    pub condition: ConditionSpec,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub annotations: HashMap<String, String>,
    /// Pending-before-firing delay as a humantime duration (`5m`, `30s`),
    /// PromQL alerting's `for`. Omitted fires on the first tick the condition
    /// holds.
    #[serde(default, rename = "for")]
    pub for_duration: Option<String>,
    #[serde(default)]
    pub max_alert_generation: Option<u32>,
    /// How often a rule that stays firing re-notifies its sinks, as a humantime
    /// duration (`1m`, `30s`), the ADR-0043 "repeat notifications while firing"
    /// amendment. Omitted uses [`ravel_alerting::DEFAULT_REPEAT_INTERVAL`];
    /// `0s` disables repeats for this rule. Parsed exactly like `for`, so an
    /// unparseable value rejects the rules file at startup with the rule id.
    #[serde(default)]
    pub repeat_interval: Option<String>,
}

/// How a rule's query result maps to firing.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConditionSpec {
    /// Fires when any series value satisfies `value <op> threshold`.
    Threshold { op: ThresholdOpSpec, value: f64 },
    /// Fires when the query returned at least one row.
    NonEmptyResult,
}

/// The six PromQL alerting comparators.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThresholdOpSpec {
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
    Ne,
}

impl From<ThresholdOpSpec> for ThresholdOp {
    fn from(spec: ThresholdOpSpec) -> ThresholdOp {
        match spec {
            ThresholdOpSpec::Gt => ThresholdOp::Gt,
            ThresholdOpSpec::Ge => ThresholdOp::Ge,
            ThresholdOpSpec::Lt => ThresholdOp::Lt,
            ThresholdOpSpec::Le => ThresholdOp::Le,
            ThresholdOpSpec::Eq => ThresholdOp::Eq,
            ThresholdOpSpec::Ne => ThresholdOp::Ne,
        }
    }
}

/// Read and validate the `--alert-rules-file` document at `path`.
pub fn load_rules_file(path: &Path) -> anyhow::Result<HashMap<TenantHash, Vec<Rule>>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("could not read alert rules file {path:?}: {e}"))?;
    parse_rules(&text)
        .map_err(|e| anyhow::anyhow!("invalid alert rules file {}: {e}", path.display()))
}

/// Parse and validate a rules document, grouping the rules by tenant.
///
/// Validation is strict at startup rather than per tick: an unknown field, a
/// rule naming neither or both query languages, an unparseable `for`, a
/// condition that cannot apply to its query's result shape, or two rules in one
/// tenant that would produce the same `alert_id` all fail the process here
/// instead of logging once a minute forever.
pub fn parse_rules(text: &str) -> anyhow::Result<HashMap<TenantHash, Vec<Rule>>> {
    let file: AlertRulesFile = serde_json::from_str(text)?;
    let mut out: HashMap<TenantHash, Vec<Rule>> = HashMap::new();
    let mut seen: HashMap<(TenantHash, AlertId), String> = HashMap::new();

    for spec in file.rules {
        if spec.tenant.is_empty() {
            anyhow::bail!("rule {:?} has an empty tenant", spec.rule_id);
        }
        if spec.rule_id.is_empty() {
            anyhow::bail!("a rule for tenant {:?} has an empty rule_id", spec.tenant);
        }
        let query = match (&spec.promql, &spec.sql) {
            (Some(text), None) => RuleQuery::Promql(text.clone()),
            (None, Some(text)) => RuleQuery::Sql(text.clone()),
            _ => anyhow::bail!(
                "rule {:?} must set exactly one of \"promql\" or \"sql\"",
                spec.rule_id
            ),
        };
        let condition = match spec.condition {
            ConditionSpec::Threshold { op, value } => RuleCondition::Threshold {
                op: op.into(),
                threshold: value,
            },
            ConditionSpec::NonEmptyResult => RuleCondition::NonEmptyResult,
        };
        // The two shapes are not interchangeable: a threshold reads per-series
        // values (PromQL) and a nonempty-result reads a row count (SQL).
        // `condition_met` would return a typed error every tick; catch it once,
        // here.
        match (&query, &condition) {
            (RuleQuery::Promql(_), RuleCondition::NonEmptyResult) => anyhow::bail!(
                "rule {:?} pairs a PromQL query with a non_empty_result condition; PromQL rules \
                 take a threshold condition",
                spec.rule_id
            ),
            (RuleQuery::Sql(_), RuleCondition::Threshold { .. }) => anyhow::bail!(
                "rule {:?} pairs a SQL query with a threshold condition; SQL rules take a \
                 non_empty_result condition",
                spec.rule_id
            ),
            _ => {}
        }
        let for_duration = match &spec.for_duration {
            Some(text) => Some(humantime::parse_duration(text).map_err(|e| {
                anyhow::anyhow!(
                    "rule {:?} has an invalid \"for\" {text:?}: {e}",
                    spec.rule_id
                )
            })?),
            None => None,
        };
        let repeat_interval = match &spec.repeat_interval {
            Some(text) => Some(humantime::parse_duration(text).map_err(|e| {
                anyhow::anyhow!(
                    "rule {:?} has an invalid \"repeat_interval\" {text:?}: {e}",
                    spec.rule_id
                )
            })?),
            None => None,
        };

        let labels = sorted_pairs(spec.labels);
        let rule = Rule {
            rule_id: spec.rule_id.clone(),
            query,
            condition,
            labels,
            annotations: sorted_pairs(spec.annotations),
            for_duration,
            max_alert_generation: spec.max_alert_generation,
            repeat_interval,
        };

        let tenant = TenantId::new(&spec.tenant).hash();
        let alert_id = compute_alert_id(&rule.rule_id, &rule.labels);
        if let Some(other) = seen.insert((tenant, alert_id), rule.rule_id.clone()) {
            anyhow::bail!(
                "rules {other:?} and {:?} share tenant {:?} and produce the same alert identity; \
                 give them different rule ids or different labels",
                rule.rule_id,
                spec.tenant
            );
        }
        out.entry(tenant).or_default().push(rule);
    }
    Ok(out)
}

/// A label or annotation map as the sorted `(name, value)` pairs `Rule` holds.
fn sorted_pairs(map: HashMap<String, String>) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = map.into_iter().collect();
    pairs.sort_unstable();
    pairs
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn rules_for(text: &str, tenant: &str) -> Vec<Rule> {
        let parsed = parse_rules(text).expect("valid rules");
        parsed
            .get(&TenantId::new(tenant).hash())
            .cloned()
            .unwrap_or_default()
    }

    const PROMQL_RULE: &str = r#"{
      "rules": [
        {
          "tenant": "acme",
          "rule_id": "high-cpu",
          "promql": "cpu_usage",
          "condition": {"type": "threshold", "op": "gt", "value": 0.9},
          "labels": {"severity": "page", "team": "sre"},
          "annotations": {"summary": "cpu is hot"},
          "for": "5m",
          "max_alert_generation": 3
        }
      ]
    }"#;

    #[test]
    fn parses_a_promql_threshold_rule() {
        let rules = rules_for(PROMQL_RULE, "acme");
        assert_eq!(rules.len(), 1);
        let rule = &rules[0];
        assert_eq!(rule.rule_id, "high-cpu");
        assert_eq!(rule.query, RuleQuery::Promql("cpu_usage".to_string()));
        assert_eq!(
            rule.condition,
            RuleCondition::Threshold {
                op: ThresholdOp::Gt,
                threshold: 0.9
            }
        );
        assert_eq!(
            rule.labels,
            vec![
                ("severity".to_string(), "page".to_string()),
                ("team".to_string(), "sre".to_string()),
            ],
            "labels are sorted so the alert identity is order-independent"
        );
        assert_eq!(rule.for_duration, Some(Duration::from_secs(300)));
        assert_eq!(rule.max_alert_generation, Some(3));
    }

    #[test]
    fn parses_a_sql_detection_rule() {
        let text = r#"{
          "rules": [
            {
              "tenant": "acme",
              "rule_id": "failed-logins",
              "sql": "select * from logs where has_word(body, 'denied')",
              "condition": {"type": "non_empty_result"}
            }
          ]
        }"#;
        let rules = rules_for(text, "acme");
        assert!(matches!(rules[0].query, RuleQuery::Sql(_)));
        assert_eq!(rules[0].condition, RuleCondition::NonEmptyResult);
        assert_eq!(rules[0].for_duration, None);
    }

    #[test]
    fn groups_rules_by_tenant() {
        let text = r#"{
          "rules": [
            {"tenant": "a", "rule_id": "r1", "promql": "x",
             "condition": {"type": "threshold", "op": "gt", "value": 1}},
            {"tenant": "b", "rule_id": "r2", "promql": "y",
             "condition": {"type": "threshold", "op": "lt", "value": 1}},
            {"tenant": "a", "rule_id": "r3", "promql": "z",
             "condition": {"type": "threshold", "op": "ne", "value": 1}}
          ]
        }"#;
        let parsed = parse_rules(text).expect("valid");
        assert_eq!(parsed.len(), 2, "two tenants");
        assert_eq!(parsed[&TenantId::new("a").hash()].len(), 2);
        assert_eq!(parsed[&TenantId::new("b").hash()].len(), 1);
    }

    #[test]
    fn rejects_a_rule_with_neither_or_both_query_languages() {
        let neither = r#"{"rules": [{"tenant": "a", "rule_id": "r",
          "condition": {"type": "non_empty_result"}}]}"#;
        assert!(parse_rules(neither).is_err());

        let both = r#"{"rules": [{"tenant": "a", "rule_id": "r", "promql": "x", "sql": "y",
          "condition": {"type": "non_empty_result"}}]}"#;
        assert!(parse_rules(both).is_err());
    }

    #[test]
    fn rejects_a_condition_that_cannot_apply_to_its_query() {
        // Caught at startup, not once per tick as a ResultShapeMismatch.
        let promql_nonempty = r#"{"rules": [{"tenant": "a", "rule_id": "r", "promql": "x",
          "condition": {"type": "non_empty_result"}}]}"#;
        assert!(parse_rules(promql_nonempty).is_err());

        let sql_threshold = r#"{"rules": [{"tenant": "a", "rule_id": "r", "sql": "select 1",
          "condition": {"type": "threshold", "op": "gt", "value": 1}}]}"#;
        assert!(parse_rules(sql_threshold).is_err());
    }

    #[test]
    fn rejects_two_rules_with_the_same_alert_identity() {
        // Same rule_id and same labels in one tenant: both would write records
        // under one alert_id and fight over its state every tick.
        let text = r#"{
          "rules": [
            {"tenant": "a", "rule_id": "r", "promql": "x",
             "condition": {"type": "threshold", "op": "gt", "value": 1}},
            {"tenant": "a", "rule_id": "r", "promql": "y",
             "condition": {"type": "threshold", "op": "gt", "value": 2}}
          ]
        }"#;
        assert!(parse_rules(text).is_err());

        // Distinguishing labels make them separate alerts, which is fine.
        let distinguished = r#"{
          "rules": [
            {"tenant": "a", "rule_id": "r", "promql": "x", "labels": {"shard": "1"},
             "condition": {"type": "threshold", "op": "gt", "value": 1}},
            {"tenant": "a", "rule_id": "r", "promql": "y", "labels": {"shard": "2"},
             "condition": {"type": "threshold", "op": "gt", "value": 2}}
          ]
        }"#;
        assert_eq!(rules_for(distinguished, "a").len(), 2);
    }

    #[test]
    fn rejects_unknown_fields_and_bad_durations() {
        let typo = r#"{"rules": [{"tenant": "a", "rule_id": "r", "promql": "x",
          "condition": {"type": "threshold", "op": "gt", "value": 1}, "labelz": {}}]}"#;
        assert!(
            parse_rules(typo).is_err(),
            "a misspelled field must fail startup, not be silently ignored"
        );

        let bad_for = r#"{"rules": [{"tenant": "a", "rule_id": "r", "promql": "x",
          "condition": {"type": "threshold", "op": "gt", "value": 1}, "for": "soon"}]}"#;
        assert!(parse_rules(bad_for).is_err());
    }

    #[test]
    fn parses_repeat_interval_with_default_and_disable() {
        let doc = |ri: &str| {
            format!(
                r#"{{"rules": [{{"tenant": "a", "rule_id": "r", "promql": "x",
              "condition": {{"type": "threshold", "op": "gt", "value": 1}},
              "repeat_interval": "{ri}"}}]}}"#
            )
        };
        // An explicit duration parses like `for`.
        assert_eq!(
            rules_for(&doc("2m"), "a")[0].repeat_interval,
            Some(Duration::from_secs(120))
        );
        // An explicit zero is legal and disables repeats for the rule.
        assert_eq!(
            rules_for(&doc("0s"), "a")[0].repeat_interval,
            Some(Duration::ZERO)
        );
        // Absent means None, so the evaluator applies DEFAULT_REPEAT_INTERVAL.
        let absent = r#"{"rules": [{"tenant": "a", "rule_id": "r", "promql": "x",
          "condition": {"type": "threshold", "op": "gt", "value": 1}}]}"#;
        assert_eq!(rules_for(absent, "a")[0].repeat_interval, None);
    }

    #[test]
    fn rejects_an_unparseable_repeat_interval_naming_the_rule() {
        let bad = r#"{"rules": [{"tenant": "a", "rule_id": "r", "promql": "x",
          "condition": {"type": "threshold", "op": "gt", "value": 1},
          "repeat_interval": "soon"}]}"#;
        let err = parse_rules(bad)
            .expect_err("an unparseable repeat_interval rejects the rules file")
            .to_string();
        assert!(
            err.contains("repeat_interval"),
            "the error names the field: {err}"
        );
        assert!(err.contains("\"r\""), "the error names the rule id: {err}");
    }

    #[test]
    fn every_threshold_comparator_parses() {
        for (text, expected) in [
            ("gt", ThresholdOp::Gt),
            ("ge", ThresholdOp::Ge),
            ("lt", ThresholdOp::Lt),
            ("le", ThresholdOp::Le),
            ("eq", ThresholdOp::Eq),
            ("ne", ThresholdOp::Ne),
        ] {
            let doc = format!(
                r#"{{"rules": [{{"tenant": "a", "rule_id": "r", "promql": "x",
                   "condition": {{"type": "threshold", "op": "{text}", "value": 1}}}}]}}"#
            );
            assert_eq!(
                rules_for(&doc, "a")[0].condition,
                RuleCondition::Threshold {
                    op: expected,
                    threshold: 1.0
                }
            );
        }
    }

    #[test]
    fn promql_summary_maps_each_result_shape() {
        use ravel_promql::InstantSample;
        use ravel_types::LabelSet;

        let empty = LabelSet::new(Vec::new()).expect("empty label set");
        let vector = PromqlValue::Vector(vec![
            InstantSample {
                labels: empty.clone(),
                ts_ns: 0,
                orig_sample_ts_ns: 0,
                value: 1.5,
                histogram: None,
            },
            InstantSample {
                labels: empty,
                ts_ns: 0,
                orig_sample_ts_ns: 0,
                value: 2.5,
                histogram: None,
            },
        ]);
        assert_eq!(
            promql_summary(vector).expect("vector summarizes"),
            QueryResultSummary::Numeric(vec![1.5, 2.5])
        );

        assert_eq!(
            promql_summary(PromqlValue::Scalar(7.0)).expect("scalar summarizes"),
            QueryResultSummary::Numeric(vec![7.0]),
            "a scalar is a one-value vector for condition purposes"
        );

        // A range vector or a string is a rule-authoring error, surfaced
        // rather than silently read as "not firing".
        assert!(promql_summary(PromqlValue::Matrix(Vec::new())).is_err());
        assert!(promql_summary(PromqlValue::String("x".into())).is_err());
    }

    #[test]
    fn hour_bucket_is_the_unix_hour_and_never_panics() {
        assert_eq!(hour_bucket(0), 0);
        assert_eq!(hour_bucket(NS_PER_HOUR), 1);
        assert_eq!(hour_bucket(NS_PER_HOUR + 1), 1);
        assert_eq!(hour_bucket(NS_PER_HOUR - 1), 0);
        // A pre-epoch clock reading yields 0, which commit-record validation
        // treats as "not set" rather than rejecting the write.
        assert_eq!(hour_bucket(-1), 0);
        assert_eq!(hour_bucket(i64::MIN), 0);
    }

    #[test]
    fn notification_started_at_follows_the_prior_record() {
        let rule = &rules_for(PROMQL_RULE, "acme")[0];
        let pending = ravel_alerting::build_transition_record(rule, AlertState::Pending, 0, 100);
        let firing = ravel_alerting::build_transition_record(rule, AlertState::Firing, 0, 400);
        let notification = AlertNotification::new(firing, Some(&pending));
        assert_eq!(notification.started_at_ns, 100);
        assert_eq!(notification.previous_state, Some(AlertState::Pending));
    }

    #[test]
    fn a_disabled_config_spawns_no_tasks() {
        let tasks = AlertEvalTasks::none();
        assert!(tasks.shutdown.is_empty());
        assert!(tasks.handles.is_empty());
    }
}

/// Tick-level tests that need a real store, catalog, and `QueryEngine`: the
/// backward-clock monotonic stamp, the compaction-record tolerance in
/// the fold, and the cross-replica lease. Kept apart from the
/// pure parsing tests above because they pull in the ingest/segment/query
/// stack; the seeding here mirrors `tests/alerting_e2e.rs`'s harness.
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tick_tests {
    use super::*;

    use std::sync::atomic::{AtomicI64, Ordering};

    use ravel_catalog::{Catalog, CatalogConfig};
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::{EngineConfig, QueryEngine};
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId};

    const NS_PER_SEC: i64 = 1_000_000_000;
    /// Half an hour past the epoch, inside the first ingest hour, exactly as the
    /// e2e harness uses so `Catalog::resolve`'s listing window covers the seed.
    const NOW_NS: i64 = 30 * 60 * NS_PER_SEC;
    const METRIC: &str = "cpu_usage";
    const TENANT: &str = "acme";

    /// A clock a test moves by hand, forward or backward, to drive clock-skew
    /// paths with no wall-clock sleep.
    struct TestClock(AtomicI64);

    impl TestClock {
        fn at(now_ns: i64) -> Arc<TestClock> {
            Arc::new(TestClock(AtomicI64::new(now_ns)))
        }

        fn set(&self, now_ns: i64) {
            self.0.store(now_ns, Ordering::SeqCst);
        }
    }

    impl Clock for TestClock {
        fn now_ns(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn label_set(metric: &str) -> LabelSet {
        LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels")
    }

    /// Publish one real RSEG segment holding `metric` at `samples`, plus its
    /// commit record, for `tenant`. Mirrors the e2e harness.
    async fn publish_metric(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantId,
        samples: &[(i64, f64)],
    ) {
        let tenant_hash = tenant.hash();
        let labels = label_set(METRIC);
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(tenant, METRIC, &labels).expect("series id"),
            labels,
            samples: samples
                .iter()
                .map(|(ts_ns, value)| Sample {
                    ts_ns: *ts_ns,
                    value: *value,
                })
                .collect(),
        }];
        let writer_id = Uuid::from_u128(9_001);
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let written = SegmentWriter::write(
            series,
            identity,
            IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 0,
            },
        )
        .expect("write segment");

        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid commit record");

        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    /// A store seeded with one series at 1.0 (above the rule threshold 0.9)
    /// shortly before `NOW_NS`.
    async fn seeded_store() -> Arc<dyn ObjectStoreBackend> {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new(TENANT);
        publish_metric(store.as_ref(), &tenant, &[(NOW_NS - 30 * NS_PER_SEC, 1.0)]).await;
        store
    }

    fn threshold_rule() -> Rule {
        Rule {
            rule_id: "high-cpu".to_string(),
            query: RuleQuery::Promql(METRIC.to_string()),
            condition: RuleCondition::Threshold {
                op: ThresholdOp::Gt,
                threshold: 0.9,
            },
            labels: vec![("severity".to_string(), "page".to_string())],
            annotations: vec![("summary".to_string(), "cpu is hot".to_string())],
            for_duration: None,
            max_alert_generation: None,
            repeat_interval: None,
        }
    }

    /// `threshold_rule` with an explicit repeat cadence.
    fn repeat_rule(repeat_interval: Option<Duration>) -> Rule {
        Rule {
            repeat_interval,
            ..threshold_rule()
        }
    }

    /// A one-entry folded-latest map holding `rule`'s alert in `state` at
    /// `ts_ns`, keyed exactly as the evaluator folds it.
    fn latest_with(rule: &Rule, state: AlertState, ts_ns: i64) -> HashMap<AlertId, AlertRecord> {
        let record = ravel_alerting::build_transition_record(rule, state, 0, ts_ns);
        let mut latest = HashMap::new();
        latest.insert(compute_alert_id(&rule.rule_id, &rule.labels), record);
        latest
    }

    /// Build an evaluator over `store`, sharing `clock` so a test can move time.
    /// Each call mints a fresh `writer_id`, so two evaluators over one store are
    /// two distinct "replicas" for the lease test.
    fn evaluator(store: Arc<dyn ObjectStoreBackend>, clock: Arc<TestClock>) -> AlertEvaluator {
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
        let engine = QueryEngine::new(catalog, Arc::clone(&store), EngineConfig::default());
        let config = AlertEvalConfig {
            enabled: true,
            ..AlertEvalConfig::default()
        };
        AlertEvaluator::new(
            store,
            AlertQueryEngines {
                promql: Arc::new(engine),
                #[cfg(feature = "sql")]
                sql: None,
            },
            clock,
            TenantId::new(TENANT).hash(),
            vec![threshold_rule()],
            &config,
        )
        .expect("build evaluator")
    }

    /// Every alert record this tenant has, read through its commit records and
    /// sorted by `ts_ns`, exactly as any reader would.
    async fn read_alert_records(
        store: &dyn ObjectStoreBackend,
        tenant: TenantHash,
    ) -> Vec<AlertRecord> {
        let prefix =
            keys::commit_shard_prefix(&tenant, Signal::Alerts, ALERT_SHARD).expect("prefix");
        let cfg = RlogConfig::default();
        let mut out = Vec::new();
        for meta in list_all(store, &prefix).await.expect("list") {
            // Skip anything that is not an L0 commit record (e.g. the
            // compaction-record test injects one under this prefix).
            if !matches!(
                keys::partition_bucket_entry(&meta.key).expect("classify"),
                keys::BucketEntry::CommitRecord(_)
            ) {
                continue;
            }
            let commit = record::decode(
                &store
                    .get(&meta.key, GetRange::Full)
                    .await
                    .expect("get commit")
                    .data,
            )
            .expect("decode commit");
            let data_key = keys::verify_object_key(&commit).expect("object key");
            let object = store
                .get(&data_key, GetRange::Full)
                .await
                .expect("get data");
            let reader = RlogReader::new(&object.data, &cfg).expect("open rlog");
            let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
            for row in &rows {
                out.push(AlertRecord::from_log_record(row).expect("decode alert record"));
            }
        }
        out.sort_by_key(|r| r.ts_ns);
        out
    }

    /// a backward wall-clock step between ticks must still produce a
    /// strictly-increasing `ts_ns`, and the evaluator must converge rather than
    /// re-transition from stale state every tick. The instant query sees the
    /// seed at `NOW`, then a clock stepped behind the seed empties the vector so
    /// the condition clears and the alert resolves -- and that resolve must be
    /// stamped after the firing record it supersedes, or the fold would keep
    /// picking the firing record and re-resolve forever.
    #[tokio::test]
    async fn a_backward_clock_step_keeps_ts_ns_monotonic_and_converges() {
        let store = seeded_store().await;
        let tenant = TenantId::new(TENANT).hash();
        let clock = TestClock::at(NOW_NS);
        let mut evaluator = evaluator(Arc::clone(&store), Arc::clone(&clock));

        // Tick 1 at NOW: fires.
        assert_eq!(evaluator.run_tick().await.records_written, 1, "onset fires");
        let firing = &read_alert_records(store.as_ref(), tenant).await[0];
        assert_eq!(firing.state, AlertState::Firing);
        assert_eq!(firing.ts_ns, NOW_NS);
        let firing_ts = firing.ts_ns;

        // Clock steps 100s behind NOW. The only sample now sits in the future
        // relative to the query instant, so the instant vector empties and the
        // condition clears: this tick resolves.
        let stepped_back = NOW_NS - 100 * NS_PER_SEC;
        clock.set(stepped_back);
        assert_eq!(
            evaluator.run_tick().await.records_written,
            1,
            "the cleared condition resolves even under a backward clock step"
        );

        let records = read_alert_records(store.as_ref(), tenant).await;
        assert_eq!(records.len(), 2, "firing then resolved");
        let resolved = records
            .iter()
            .find(|r| r.state == AlertState::Resolved)
            .expect("a resolved record");
        assert!(
            resolved.ts_ns > firing_ts,
            "the resolved record must be stamped strictly after the firing record it \
             supersedes (got resolved ts {} vs firing ts {}), even though the wall clock \
             stepped back to {}",
            resolved.ts_ns,
            firing_ts,
            stepped_back
        );
        assert_eq!(
            resolved.ts_ns,
            firing_ts + 1,
            "the corrected stamp is exactly prior.ts_ns + 1 when now_ns is behind it"
        );

        // Tick 3, clock still behind: the fold must land on the resolved record
        // (greatest ts), so the still-cleared condition writes nothing. Without
        // the fix the fold would keep picking the firing record and resolve
        // again every tick -- an infinite re-transition loop.
        assert_eq!(
            evaluator.run_tick().await.records_written,
            0,
            "the evaluator converges: no re-transition once resolved is the folded latest"
        );
        assert_eq!(
            read_alert_records(store.as_ref(), tenant).await.len(),
            2,
            "history is unchanged; no runaway resolve records"
        );
    }

    /// `load_latest_records` must skip a `CompactionRecord` under the
    /// alerts commit prefix instead of erroring. Constructed directly in the
    /// store: today nothing produces one, so this is forward-compatible
    /// groundwork for when compaction is enabled for `Signal::Alerts`.
    #[tokio::test]
    async fn load_latest_records_skips_a_compaction_record() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new(TENANT).hash();
        // A validly-shaped L1 compaction record key under the alerts commit
        // prefix. The fold classifies it by key shape and skips before ever
        // reading the object, so the body is irrelevant.
        let key = keys::compaction_record_key(
            &tenant,
            Signal::Alerts,
            ALERT_SHARD,
            0,
            "0123456789abcdef",
        )
        .expect("compaction record key");
        store
            .put(&key, Bytes::from_static(b"unused"), PutOptions::default())
            .await
            .expect("put compaction record");

        let evaluator = evaluator(Arc::clone(&store), TestClock::at(NOW_NS));
        let latest = evaluator
            .load_latest_records()
            .await
            .expect("a compaction record is skipped, not an error");
        assert!(
            latest.is_empty(),
            "no commit records remain once the compaction record is skipped"
        );
    }

    /// two evaluator instances (two "replicas") over the same store,
    /// evaluating the same tick, must not both fire and write. The lease holder
    /// writes the transition; the other finds a live lease held by a different
    /// identity and skips evaluation entirely.
    #[tokio::test]
    async fn only_the_lease_holder_writes_when_two_replicas_evaluate_one_tick() {
        let store = seeded_store().await;
        let tenant = TenantId::new(TENANT).hash();
        let clock = TestClock::at(NOW_NS);

        let mut a = evaluator(Arc::clone(&store), Arc::clone(&clock));
        let mut b = evaluator(Arc::clone(&store), Arc::clone(&clock));

        let a_report = a.run_tick().await;
        let b_report = b.run_tick().await;

        assert_eq!(
            a_report.records_written, 1,
            "the lease holder fires the transition"
        );
        assert!(!a_report.lease_not_held, "replica a holds the lease");

        assert_eq!(
            b_report.records_written, 0,
            "the replica without the lease does not write"
        );
        assert!(
            b_report.lease_not_held,
            "replica b saw a live lease held by replica a and skipped evaluation"
        );

        assert_eq!(
            read_alert_records(store.as_ref(), tenant).await.len(),
            1,
            "exactly one transition record exists across both replicas"
        );
    }

    /// once the holder's lease expires, a second replica takes it over and
    /// then evaluates. Proves the lease is a lease, not a permanent lock: a dead
    /// holder does not block a tenant forever.
    #[tokio::test]
    async fn a_second_replica_takes_over_an_expired_lease() {
        let store = seeded_store().await;
        let clock = TestClock::at(NOW_NS);

        let a = evaluator(Arc::clone(&store), Arc::clone(&clock));
        let b = evaluator(Arc::clone(&store), Arc::clone(&clock));

        // a acquires; b is blocked while a's lease is live.
        assert!(a.acquire_lease(NOW_NS).await.expect("a acquires"));
        assert!(
            !b.acquire_lease(NOW_NS).await.expect("b blocked"),
            "b cannot take a live lease held by a"
        );

        // Past a's lease lifetime (LEASE_TTL_TICKS * default interval), b takes
        // over; a, now the non-holder, is in turn blocked by b's fresh lease.
        let past_expiry = NOW_NS
            + (LEASE_TTL_TICKS as i64) * DEFAULT_ALERT_EVAL_INTERVAL.as_nanos() as i64
            + NS_PER_SEC;
        assert!(
            b.acquire_lease(past_expiry).await.expect("b takes over"),
            "b takes over once a's lease has expired"
        );
        assert!(
            !a.acquire_lease(past_expiry).await.expect("a now blocked"),
            "a no longer holds the lease after b took it over"
        );
    }

    // --- Repeat pass (ADR-0043 "repeat notifications while firing") ----------
    //
    // These drive `queue_repeat_if_due` directly with a hand-built folded-latest
    // map, so the state gate, the anchor-staleness rule, the backward-clock
    // clamp, the disable switch, and the default cadence are each pinned without
    // the query stack. `queue_repeat_if_due` writes no durable record by
    // construction (it calls neither `publish` nor `write_alert_record`), which
    // the tick-level e2e tests confirm end to end.

    /// Only a `Firing` folded record repeats. `Pending`, `Resolved`, and
    /// `Suppressed` never do, even long past the interval.
    #[tokio::test]
    async fn repeat_pass_only_queues_for_a_firing_record() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut ev = evaluator(store, TestClock::at(NOW_NS));
        let rule = repeat_rule(Some(Duration::from_secs(60)));
        let alert_id = compute_alert_id(&rule.rule_id, &rule.labels);
        // Five windows past a record stamped at NOW_NS.
        let due = NOW_NS + 300 * NS_PER_SEC;

        for state in [
            AlertState::Pending,
            AlertState::Resolved,
            AlertState::Suppressed,
        ] {
            let latest = latest_with(&rule, state, NOW_NS);
            let mut report = AlertEvalReport::default();
            ev.repeat_marks.clear();
            ev.undelivered.clear();
            ev.queue_repeat_if_due(&rule, &latest, due, &mut report);
            assert_eq!(report.repeats_queued, 0, "{state:?} must never repeat");
            assert!(
                ev.undelivered.is_empty(),
                "{state:?} queues no notification"
            );
        }

        let latest = latest_with(&rule, AlertState::Firing, NOW_NS);
        let mut report = AlertEvalReport::default();
        ev.repeat_marks.clear();
        ev.undelivered.clear();
        ev.queue_repeat_if_due(&rule, &latest, due, &mut report);
        assert_eq!(report.repeats_queued, 1, "a firing record repeats when due");
        assert!(
            ev.undelivered.contains_key(&alert_id),
            "the repeat is queued into undelivered"
        );
    }

    /// A window mark left by a prior (resolved) episode must not suppress the
    /// new episode's repeats: the mark's anchor differs from the new firing
    /// record's `ts_ns`, so it is ignored and the schedule re-anchors. Made
    /// non-vacuous by the old mark sitting at a strictly higher window (5) than
    /// the new episode's first repeat (1).
    #[tokio::test]
    async fn a_stale_window_mark_does_not_suppress_a_new_firing_episode() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut ev = evaluator(store, TestClock::at(NOW_NS));
        let rule = repeat_rule(Some(Duration::from_secs(60)));
        let alert_id = compute_alert_id(&rule.rule_id, &rule.labels);

        // A mark from a prior episode that reached window 5.
        ev.repeat_marks.insert(alert_id, (NOW_NS, 5));

        // The new firing episode is anchored much later; only window 1 elapsed.
        let new_anchor = NOW_NS + 10_000 * NS_PER_SEC;
        let latest = latest_with(&rule, AlertState::Firing, new_anchor);
        let mut report = AlertEvalReport::default();
        ev.queue_repeat_if_due(&rule, &latest, new_anchor + 60 * NS_PER_SEC, &mut report);

        assert_eq!(
            report.repeats_queued, 1,
            "window 1 of the new episode repeats despite the stale window-5 mark"
        );
        assert_eq!(
            ev.repeat_marks[&alert_id],
            (new_anchor, 1),
            "the mark re-anchored to the new episode's firing record"
        );
    }

    /// A backward clock step clamps the window to zero, so no repeat is queued
    /// (matching the pending-duration clamp).
    #[tokio::test]
    async fn a_backward_clock_step_queues_no_repeat() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut ev = evaluator(store, TestClock::at(NOW_NS));
        let rule = repeat_rule(Some(Duration::from_secs(60)));
        let latest = latest_with(&rule, AlertState::Firing, NOW_NS);

        let mut report = AlertEvalReport::default();
        ev.queue_repeat_if_due(&rule, &latest, NOW_NS - 120 * NS_PER_SEC, &mut report);
        assert_eq!(
            report.repeats_queued, 0,
            "a clock behind the firing record clamps to window 0"
        );
    }

    /// An explicit `0s` repeat_interval disables repeats for the rule, no matter
    /// how much time has elapsed.
    #[tokio::test]
    async fn an_explicit_zero_interval_disables_repeats() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut ev = evaluator(store, TestClock::at(NOW_NS));
        let rule = repeat_rule(Some(Duration::ZERO));
        let latest = latest_with(&rule, AlertState::Firing, NOW_NS);

        let mut report = AlertEvalReport::default();
        ev.queue_repeat_if_due(&rule, &latest, NOW_NS + 600 * NS_PER_SEC, &mut report);
        assert_eq!(report.repeats_queued, 0, "0s disables repeats for the rule");
    }

    /// An absent repeat_interval uses `DEFAULT_REPEAT_INTERVAL` (60s): no repeat
    /// just under it, one repeat at it.
    #[tokio::test]
    async fn an_absent_interval_uses_the_default_cadence() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut ev = evaluator(store, TestClock::at(NOW_NS));
        let rule = repeat_rule(None);
        let latest = latest_with(&rule, AlertState::Firing, NOW_NS);

        let mut report = AlertEvalReport::default();
        ev.queue_repeat_if_due(&rule, &latest, NOW_NS + 59 * NS_PER_SEC, &mut report);
        assert_eq!(
            report.repeats_queued, 0,
            "just under the 60s default: no repeat"
        );

        ev.repeat_marks.clear();
        ev.undelivered.clear();
        let mut report = AlertEvalReport::default();
        ev.queue_repeat_if_due(&rule, &latest, NOW_NS + 60 * NS_PER_SEC, &mut report);
        assert_eq!(
            report.repeats_queued, 1,
            "at the 60s default cadence a repeat is due"
        );
    }
}
