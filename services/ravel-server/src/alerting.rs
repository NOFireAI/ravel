//! Per-tenant alert-rule evaluator (ADR-0043, issue #382).
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
use rand::RngExt as _;
use ravel_alerting::{
    AlertId, AlertRecord, AlertState, QueryResultSummary, Rule, RuleCondition, RuleQuery,
    ThresholdOp, compute_alert_id, condition_met, evaluate_transition, write_alert_record,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::{Clock, LOG_SEGMENT_FORMAT_VERSION};
use ravel_logseg::{ObjectIdentity, Predicate, RlogConfig, RlogReader};
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_promql::Value as PromqlValue;
use ravel_query::QueryEngine;
use ravel_types::{Signal, TenantHash, TenantId};
use serde::Deserialize;
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
        let (tx, rx) = oneshot::channel();
        let interval = config.interval;
        let handle = tokio::spawn(async move {
            run_loop(&mut evaluator, interval, rx).await;
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
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval)) => {}
            _ = &mut shutdown => return,
        }
        let report = evaluator.run_tick().await;
        tracing::info!(
            tenant = %evaluator.tenant.to_hex(),
            rules_evaluated = report.rules_evaluated,
            rules_failed = report.rules_failed,
            records_written = report.records_written,
            notifications_delivered = report.notifications_delivered,
            notifications_failed = report.notifications_failed,
            "alert evaluation tick complete"
        );
    }
}

/// Up to 10% jitter over `base`, so co-started replicas' evaluation ticks do
/// not run in lockstep (same rationale as the fold and maintenance tasks).
fn jittered(base: Duration) -> Duration {
    let jitter_bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if jitter_bound_ms == 0 {
        return base;
    }
    let extra_ms = rand::rng().random_range(0..=jitter_bound_ms);
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
    /// Notifications this tick delivered to every configured sink, including
    /// ones carried over from an earlier tick's failure.
    pub notifications_delivered: u32,
    /// Notifications still undelivered after this tick's attempt.
    pub notifications_failed: u32,
    /// `true` when the alert history could not be read, so no rule was
    /// evaluated at all. Never acts on a partial history: doing so would
    /// re-fire an alert that is already firing.
    pub history_unavailable: bool,
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
            sql_lookback: config.sql_lookback,
            writer_id: Uuid::new_v4(),
            next_seq: 1,
            undelivered: HashMap::new(),
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

        // `rules` is moved out for the loop so `self` stays mutably borrowable
        // for the write path; it is put back before returning.
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
        self.rules = rules;

        self.flush_sinks(&mut report).await;
        report
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
            now_ns,
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
        self.publish(bytes, seq, now_ns).await?;

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
    async fn run_query(&self, rule: &Rule, now_ns: i64) -> anyhow::Result<QueryResultSummary> {
        match &rule.query {
            RuleQuery::Promql(text) => {
                let value = self
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
                // Alerts are not a maintained signal today (see
                // `maintain::MAINTAINED_SIGNALS`), so nothing writes a
                // compaction record or a retention tombstone under this
                // prefix. If something starts to, folding only the L0 records
                // would silently lose history, so refuse rather than guess.
                other => anyhow::bail!(
                    "unexpected {other:?} under the alerts commit prefix; the evaluator folds \
                     only L0 alert commit records"
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

        let labels = sorted_pairs(spec.labels);
        let rule = Rule {
            rule_id: spec.rule_id.clone(),
            query,
            condition,
            labels,
            annotations: sorted_pairs(spec.annotations),
            for_duration,
            max_alert_generation: spec.max_alert_generation,
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
