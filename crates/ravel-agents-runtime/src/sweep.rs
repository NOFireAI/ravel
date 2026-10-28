//! The authoritative reconciliation sweep over one tenant (ADR-3331
//! decision 8).
//!
//! `_wake` hints and `_dir` entries are advisory: a hint can be missing,
//! duplicated, stale or early, and a directory entry says only that an agent
//! may exist. A pass walks both and, for every agent it reaches, reads HEAD
//! and acts on that read alone:
//!
//! - every timer HEAD holds as scheduled and due is fired, one CAS per timer
//!   with the generation that read holds (`timers::Coordinator::fire_due`);
//! - an agent whose HEAD records accepted inputs or an active turn (running
//!   or blocked) is offered to the dispatch set, so the next poll can lease
//!   it;
//! - a hint the wake half listed is deleted once HEAD, as read and then
//!   advanced by this pass's fires, holds no scheduled timer in its bucket,
//!   which covers a generation delivered, cancelled or superseded. No key
//!   is deleted that the pass did not list
//!   (`tests::a_lost_hint_is_found_by_the_directory_half`)
//!   (`timers::tests::fire_and_cancel_race_resolves_at_head_and_a_stale_hint_never_fires_a_cancelled_generation`).
//!
//! A hint for a bucket the clock has not reached yet is not listed, and a
//! listed hint whose timer is not yet due fires nothing and is kept
//! (`tests::a_premature_hint_fires_nothing_and_is_kept`). Two hints for one
//! generation fire it once: the second is answered from the root the first
//! one's fire published (`tests::duplicate_hints_for_one_generation_fire_it_once`).
//!
//! A hint whose PUT was lost is not needed: the directory half reaches the
//! agent within one pass when the directory fits the pass's directory GET
//! budget (`tests::a_lost_hint_is_found_by_the_directory_half`), else within
//! `ceil(agents / directory GETs per pass)` passes, where a pass gives the
//! directory half at least `max_head_gets - max_head_gets / 2` reads
//! (`sweep_pass`; ten agents at four reads a pass take three passes in
//! `tests::a_bounded_pass_walks_the_directory_in_list_order_across_passes`).
//! A hint delete can race a schedule into the same bucket made after the
//! HEAD read: the DELETE in `sweep_wake` is unconditional, so it can remove
//! the new hint, and `sweep_dir` skips an agent this pass already read. The
//! timer then waits for the directory half's next visit, a latency within
//! the same bound. It is never fired twice, since a fire is the fenced CAS
//! `timers::Coordinator::fire_due` issues on the generation HEAD holds.
//!
//! The directory half lists `_dir/` entries (`AgentStore::list_dir_after`),
//! not HEADs, as the directory amendment to ADR-3331 decision 8 records.
//! The index is complete by construction: a create writes the agent's
//! `_dir` entry before the HEAD write that creates it and fails without
//! writing HEAD when the entry cannot be written
//! (`ravel-agents-store`'s `a_create_whose_dir_entry_fails_writes_no_head`),
//! so every agent with a HEAD has an entry. An entry whose HEAD is absent
//! costs one read and is skipped.
//!
//! An entry can still go missing outside the runtime, for example by hand.
//! Such an agent is reached only through its hints, and the pass then
//! writes the entry back: after the directory half, every agent the wake
//! half read with a HEAD present and whose entry the directory half did not
//! list in the same pass gets one `CreateIfAbsent` PUT
//! (`tests::a_hint_reached_agent_gets_its_missing_dir_entry_back`). An agent
//! the directory half listed gets none, so a tenant whose directory fits one
//! pass pays nothing for the repair; in a larger one, a hint-reached agent
//! outside the pass's directory slice costs one PUT that answers
//! `AlreadyExists`. The repair issues no read, and at most one PUT per HEAD
//! read the wake half made, so the GET budget bounds it. A PUT that fails is
//! logged and tried again on the next pass that reaches the agent
//! (`tests::the_wake_half_reads_its_share_of_the_get_cap_and_resumes_across_passes`,
//! where every repair PUT is refused).
//!
//! A listing page whose keys the store cannot address, or that do not parse
//! as hints or agent ids, still moves its half's cursor past the page
//! (`tests::a_wake_page_of_unaddressable_keys_moves_the_cursor_past_it`,
//! `tests::a_dir_page_of_unaddressable_keys_moves_the_cursor_past_it`).
//!
//! # Bounds and fairness
//!
//! A pass issues at most [`SweepConfig::max_list_pages`] LISTs and
//! [`SweepConfig::max_head_gets`] HEAD reads of its own. The wake half runs
//! first on half of each budget (rounded down), then the directory half on
//! the rest plus whatever the wake half left. Each half resumes strictly
//! after the last key the previous pass processed, so a budget smaller than
//! the tenant walks it over several passes in LIST order, and no agent is
//! read twice before every agent was read once: for the directory half in
//! `tests::a_bounded_pass_walks_the_directory_in_list_order_across_passes`,
//! for the wake half, which reads exactly its share of the cap, in
//! `tests::the_wake_half_reads_its_share_of_the_get_cap_and_resumes_across_passes`.
//! The wake cursor goes back to the start once it reaches a bucket past the
//! clock, the directory cursor once it reaches the end of the listing; a
//! pass never wraps. Within one pass an agent's HEAD is read at most once.
//! A HEAD read that finds the root absent is retried by the store before it
//! is believed, and each such read counts once against the cap. The GET and
//! CAS a fire issues inside the publication skeleton are not counted against
//! the cap; they show in the store's counters.
//!
//! # Reachability
//!
//! A pass runs from [`Coordinator::sweep_once`], and from worker polls once
//! [`Coordinator::with_sweep`] enabled it, at most once per
//! [`SweepConfig::interval_ms`] across every poller. A poll runs one only
//! after its scan found nothing to dispatch, so a ready assignment never
//! waits for a pass (`tests::a_poll_with_a_ready_turn_runs_no_sweep_pass`),
//! and a pass that fired a timer or loaded an agent makes the same poll scan
//! again before it waits
//! (`tests::an_idle_poll_sweeps_on_the_interval_and_takes_what_the_pass_fired`).
//! Nothing in `ravel-server` schedules either yet; T19 (#3353) does.
//!
//! Each pass's counters are a flat [`SweepReport`], summed into
//! [`crate::SweepMetricsSnapshot`] for `ravel-server` to render. Nothing
//! renders them until T19 does.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;

use ravel_agents_core::error::{AgentError, ErrorKind, SafeIds};
use ravel_agents_core::head::Head;
use ravel_agents_core::identity::AgentId;
use ravel_agents_core::keys::DueBucket;
use ravel_agents_store::{HeadState, WakeHint};
use ravel_types::TenantHash;

use crate::Coordinator;
use crate::ownership::ms_to_ns;
use crate::timers::armed_in;

/// Default least time between two poll-driven passes.
pub const DEFAULT_SWEEP_INTERVAL_MS: u64 = 5_000;
/// Default cap on the LISTs of one pass.
pub const DEFAULT_SWEEP_MAX_LIST_PAGES: u32 = 8;
/// Default cap on the HEAD reads of one pass.
pub const DEFAULT_SWEEP_MAX_HEAD_GETS: u32 = 64;

/// Sweep tuning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepConfig {
    /// Least time between two passes started from worker polls.
    pub interval_ms: u64,
    /// LISTs one pass may issue, over both halves.
    pub max_list_pages: u32,
    /// HEAD reads one pass may issue, over both halves.
    pub max_head_gets: u32,
}

impl Default for SweepConfig {
    fn default() -> Self {
        SweepConfig {
            interval_ms: DEFAULT_SWEEP_INTERVAL_MS,
            max_list_pages: DEFAULT_SWEEP_MAX_LIST_PAGES,
            max_head_gets: DEFAULT_SWEEP_MAX_HEAD_GETS,
        }
    }
}

/// What one pass did: a flat set of counters, with no per-phase split. The
/// store's own counters split the same requests by phase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// LISTs issued, wake and directory halves together.
    pub list_pages: u64,
    /// HEAD reads issued by the pass itself.
    pub head_gets: u64,
    /// Hints in a due bucket the pass acted on.
    pub hints_read: u64,
    /// Hints deleted.
    pub hints_deleted: u64,
    /// `TimerFired` replies the pass's CASes published.
    pub timers_fired: u64,
    /// Agents with work the pass added to the dispatch set.
    pub agents_loaded: u64,
    /// Missing `_dir` entries the pass wrote back for agents it reached
    /// through a hint.
    pub dir_entries_repaired: u64,
}

/// Where the next pass resumes: the last key each half processed.
#[derive(Debug, Default)]
pub(crate) struct SweepCursor {
    wake: Option<String>,
    dir: Option<String>,
}

/// One agent as this pass found it.
struct Seen {
    /// The newest root: as read, or as its last fire published. `None` when
    /// HEAD was absent, unreadable or of an unsupported format.
    head: Option<Head>,
    /// Every due timer was fired or found settled.
    clean: bool,
}

struct Pass {
    report: SweepReport,
    lists_left: u32,
    gets_left: u32,
    seen: HashMap<AgentId, Seen>,
    /// Agents the wake half read with a HEAD present, in read order.
    hinted: Vec<AgentId>,
    /// Agents whose `_dir` entry the directory half listed.
    listed: HashSet<AgentId>,
    now: i64,
}

impl Coordinator {
    /// Use `config` for every pass and start passes from worker polls.
    #[must_use]
    pub fn with_sweep(mut self, config: SweepConfig) -> Coordinator {
        self.sweep = config;
        self.sweep_on_poll = true;
        self
    }

    pub fn sweep_config(&self) -> &SweepConfig {
        &self.sweep
    }

    /// Run one pass over `tenant`, which must be this coordinator's. A
    /// pass waits for one already running to finish. A LIST that fails ends
    /// its half, the other half still runs, and the first error is returned
    /// after both; the pass's counters are recorded either way.
    pub async fn sweep_once(&self, tenant: &TenantHash) -> Result<SweepReport, AgentError> {
        if tenant != self.store.tenant() {
            return Err(AgentError::new(
                ErrorKind::AgentGone,
                SafeIds::default(),
                "tenant is not this coordinator's",
            ));
        }
        let mut cursor = self.sweep_cursor.lock().await;
        self.sweep_pass(&mut cursor).await
    }

    /// A pass from a worker poll, when enabled and the interval has passed.
    /// Pollers claim the slot by compare-exchange on the last-pass stamp, so
    /// pollers woken together run one pass; one that finds a pass running
    /// skips it. Only a poll that found nothing to dispatch calls this, so a
    /// ready assignment never waits for a pass. Returns whether the pass
    /// fired a timer or loaded an agent, which is new work to scan for.
    pub(crate) async fn sweep_from_poll(&self) -> bool {
        if !self.sweep_on_poll {
            return false;
        }
        let interval = ms_to_ns(self.sweep.interval_ms);
        let last = self.last_sweep_unix_ns.load(Ordering::Acquire);
        let now = self.now();
        if now.saturating_sub(last) < interval
            || self
                .last_sweep_unix_ns
                .compare_exchange(last, now, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return false;
        }
        let Ok(mut cursor) = self.sweep_cursor.try_lock() else {
            return false;
        };
        match self.sweep_pass(&mut cursor).await {
            Ok(report) => report.timers_fired > 0 || report.agents_loaded > 0,
            Err(error) => {
                tracing::warn!(%error, "agents sweep pass failed");
                // A failed half still ran the other; scan again in case it
                // found work.
                true
            }
        }
    }

    async fn sweep_pass(&self, cursor: &mut SweepCursor) -> Result<SweepReport, AgentError> {
        let config = self.sweep;
        let mut pass = Pass {
            report: SweepReport::default(),
            lists_left: config.max_list_pages / 2,
            gets_left: config.max_head_gets / 2,
            seen: HashMap::new(),
            hinted: Vec::new(),
            listed: HashSet::new(),
            now: self.now(),
        };
        let wake = self.sweep_wake(&mut pass, cursor).await;
        pass.lists_left += config.max_list_pages - config.max_list_pages / 2;
        pass.gets_left += config.max_head_gets - config.max_head_gets / 2;
        let dir = self.sweep_dir(&mut pass, cursor).await;
        if dir.is_ok() {
            self.repair_dir_entries(&mut pass).await;
        }
        self.record_sweep(&pass.report);
        wake.and(dir).map(|()| pass.report)
    }

    async fn sweep_wake(
        &self,
        pass: &mut Pass,
        cursor: &mut SweepCursor,
    ) -> Result<(), AgentError> {
        let now_bucket = DueBucket::for_due(pass.now.max(0)).map_or(0, DueBucket::index);
        loop {
            if pass.lists_left == 0 {
                return Ok(());
            }
            pass.lists_left -= 1;
            pass.report.list_pages += 1;
            let page = self.store.list_wake_hints(cursor.wake.as_deref()).await?;
            for hint in &page.items {
                if hint.bucket > now_bucket {
                    cursor.wake = None;
                    return Ok(());
                }
                if !pass.seen.contains_key(&hint.agent) {
                    if pass.gets_left == 0 {
                        return Ok(());
                    }
                    self.visit(pass, &hint.agent).await;
                    if pass.seen.get(&hint.agent).is_some_and(|s| s.head.is_some()) {
                        pass.hinted.push(hint.agent);
                    }
                }
                pass.report.hints_read += 1;
                let stale = pass
                    .seen
                    .get(&hint.agent)
                    .and_then(|s| s.head.as_ref().filter(|_| s.clean))
                    .is_some_and(|head| !armed_in(head, hint.bucket));
                if stale {
                    self.delete_hint(pass, hint).await;
                }
                cursor.wake = Some(hint.key.clone());
            }
            if !page.more {
                cursor.wake = None;
                return Ok(());
            }
            // The page's raw last key, which moves the cursor past keys that
            // parsed into no hint. A page reporting more always has one; if
            // a backend ever returned none, ending the half beats re-listing
            // the same page.
            match page.last_key {
                Some(last) => cursor.wake = Some(last),
                None => return Ok(()),
            }
        }
    }

    async fn sweep_dir(&self, pass: &mut Pass, cursor: &mut SweepCursor) -> Result<(), AgentError> {
        loop {
            if pass.lists_left == 0 {
                return Ok(());
            }
            pass.lists_left -= 1;
            pass.report.list_pages += 1;
            let page = self.store.list_dir_after(cursor.dir.as_deref()).await?;
            pass.listed
                .extend(page.items.iter().map(|entry| entry.agent));
            for entry in &page.items {
                if !pass.seen.contains_key(&entry.agent) {
                    if pass.gets_left == 0 {
                        return Ok(());
                    }
                    self.visit(pass, &entry.agent).await;
                }
                cursor.dir = Some(entry.key.clone());
            }
            if !page.more {
                cursor.dir = None;
                return Ok(());
            }
            match page.last_key {
                Some(last) => cursor.dir = Some(last),
                None => return Ok(()),
            }
        }
    }

    /// Restore the `_dir` entry of every agent the wake half reached whose
    /// HEAD exists and whose entry the directory half did not list in this
    /// pass: one `CreateIfAbsent` PUT each, which is `AlreadyExists` when
    /// the entry is there after all. At most one per HEAD read the wake half
    /// made, so the GET budget bounds it.
    async fn repair_dir_entries(&self, pass: &mut Pass) {
        let hinted = std::mem::take(&mut pass.hinted);
        for agent in hinted {
            if pass.listed.contains(&agent) {
                continue;
            }
            match self.store.ensure_dir_entry(&agent).await {
                Ok(true) => pass.report.dir_entries_repaired += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::debug!(agent = %agent.to_hex(), %error, "agents sweep could not repair a dir entry");
                }
            }
        }
    }

    /// Read HEAD once, fire what it holds as due and offer its work. The
    /// read attaches no sealed page, so it is the one GET the budget
    /// charges: scheduled timers are never sealed and HEAD alone says
    /// whether the inbox holds an input (`ravel_agents_core::pages`).
    async fn visit(&self, pass: &mut Pass, agent: &AgentId) {
        pass.gets_left = pass.gets_left.saturating_sub(1);
        pass.report.head_gets += 1;
        let seen = match self.store.read_head_shallow(agent).await {
            Ok(HeadState::Present { head, .. }) => {
                let fired = self.fire_due(agent, *head, pass.now).await;
                pass.report.timers_fired += fired.fired;
                self.offer(pass, agent, &fired.head);
                Seen {
                    head: Some(fired.head),
                    clean: fired.clean,
                }
            }
            Ok(HeadState::Absent) => Seen {
                head: None,
                clean: false,
            },
            Ok(HeadState::Unsupported { found }) => {
                if self.unsupported_reported.lock().insert(*agent) {
                    tracing::warn!(
                        agent = %agent.to_hex(),
                        format_version = found,
                        "agents sweep found an agent whose HEAD format this release cannot read; its timers are not fired"
                    );
                }
                Seen {
                    head: None,
                    clean: false,
                }
            }
            Err(error) => {
                tracing::debug!(agent = %agent.to_hex(), %error, "agents sweep could not read HEAD");
                Seen {
                    head: None,
                    clean: false,
                }
            }
        };
        pass.seen.insert(*agent, seen);
    }

    /// Put an agent whose root has inputs or an active turn into the
    /// dispatch set.
    fn offer(&self, pass: &mut Pass, agent: &AgentId, head: &Head) {
        let loaded = self.agents.lock().contains_key(agent);
        if loaded {
            self.observe(agent, head);
            return;
        }
        if head.active_turn.is_some() || head.has_inbox() {
            self.observe(agent, head);
            pass.report.agents_loaded += 1;
            self.wake.notify_waiters();
        }
    }

    async fn delete_hint(&self, pass: &mut Pass, hint: &WakeHint) {
        if self.store.delete_wake_hint(hint).await.is_ok() {
            pass.report.hints_deleted += 1;
        }
    }
}

#[cfg(test)]
mod tests;
