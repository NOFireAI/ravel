//! The page loop of one pinned `ravel_query_sql` walk (issue #3779), shared by
//! the `mcp_walk_expiry` bin and the in-process maintenance test.
//!
//! Page one runs at the walk's t0 through `QueryService::sql_execute_at` with
//! the `SqlRequest` `ravel_query_sql` builds; each later page resumes from the
//! last kept row's order tuple and resolves at t0. A page whose segment-set
//! digest differs from page one's ends the walk as an expiry, labelled from the
//! key diff by [`crate::walk_expiry::label_expiry`]. The caller owns the clock
//! and the wait between pages: [`PinnedWalk::step`] runs one page.

use std::collections::{BTreeSet, HashMap};

use ravel_catalog::Catalog;
use ravel_mcp::budget::McpEffectiveBudgets;
use ravel_query::RequestBudgets;
use ravel_server::service::QueryService;
use ravel_sql::{ResumePosition, SqlRequest};
use ravel_types::{Signal, TenantHash, TimeRange};

use crate::walk_expiry::{self as we, WalkEnd};

/// `Call::request` for one page: the window as listing bound and row filter,
/// the row cap and the clamped MCP budgets.
pub fn page_request(
    budgets: &McpEffectiveBudgets,
    sql: String,
    window: TimeRange,
    now_ns: i64,
) -> SqlRequest {
    let query = &budgets.query;
    SqlRequest {
        sql,
        window,
        min_tokens: Vec::new(),
        now_ns,
        deadline: budgets.deadline,
        row_window: true,
        max_rows: Some(budgets.max_rows as usize),
        budgets: Some(RequestBudgets {
            max_bytes_scanned: Some(query.max_bytes_scanned),
            max_store_requests: Some(query.max_store_requests),
            max_segments: Some(query.max_segments),
        }),
    }
}

/// What one walk reads, fixed at page one.
pub struct WalkSpec<'a> {
    pub service: &'a QueryService,
    pub catalog: &'a Catalog,
    pub tenant_hash: TenantHash,
    pub budgets: &'a McpEffectiveBudgets,
    pub sql: String,
    pub window: TimeRange,
    pub t0_ns: i64,
    pub pages: u32,
}

/// One page's result.
#[derive(Debug)]
pub struct PageReport {
    /// 1-based.
    pub page: u32,
    pub pending_erasure: bool,
    /// `l0/` keys an expiry's labeller could not place in an ingest hour.
    pub unresolved_hours: u64,
    /// `Some` once the walk has ended, at this page.
    pub end: Option<WalkEnd>,
}

/// A walk in progress.
pub struct PinnedWalk<'a> {
    spec: WalkSpec<'a>,
    pages_run: u32,
    k0: BTreeSet<String>,
    k0_digest: [u8; 32],
    resume: Option<ResumePosition>,
    hours: HashMap<String, u32>,
    ended: bool,
}

impl<'a> PinnedWalk<'a> {
    /// Ready to run page one: reads each resolved segment's ingest hour at t0
    /// so an expiry can place a removed `l0/` key. An error is page one's.
    pub async fn start(spec: WalkSpec<'a>) -> Result<Self, (u32, anyhow::Error)> {
        let mut walk = PinnedWalk {
            spec,
            pages_run: 0,
            k0: BTreeSet::new(),
            k0_digest: [0u8; 32],
            resume: None,
            hours: HashMap::new(),
            ended: false,
        };
        walk.hour_lookup().await.map_err(|e| (1, e))?;
        Ok(walk)
    }

    async fn hour_lookup(&mut self) -> anyhow::Result<()> {
        let snapshot = self
            .spec
            .catalog
            .resolve(
                &self.spec.tenant_hash,
                Signal::Metrics,
                self.spec.window,
                &[],
                self.spec.t0_ns,
            )
            .await?;
        for segment in snapshot.segments {
            self.hours
                .insert(segment.data_object_key, segment.ingest_hour_bucket);
        }
        Ok(())
    }

    /// Pages run so far.
    pub fn pages_run(&self) -> u32 {
        self.pages_run
    }

    /// Page one's segment set; empty before page one runs.
    pub fn k0(&self) -> &BTreeSet<String> {
        &self.k0
    }

    /// A resolved key's ingest hour, from the resolves at t0.
    pub fn hour_of(&self, key: &str) -> Option<u32> {
        self.hours.get(key).copied()
    }

    /// Run the next page. `now_ns` is the call's own clock; page one should
    /// pass t0. Calling it again after the walk ended is an error.
    pub async fn step(&mut self, now_ns: i64) -> Result<PageReport, (u32, anyhow::Error)> {
        let page = self.pages_run + 1;
        if self.ended {
            return Err((page, anyhow::anyhow!("the walk already ended")));
        }
        let plan = ravel_sql::plan_page(&self.spec.sql, self.resume.as_ref())
            .map_err(|e| (page, e.into()))?;
        let order_columns: Vec<String> = plan.order_by.iter().map(|t| t.column.clone()).collect();
        let request = page_request(
            self.spec.budgets,
            plan.statement.clone(),
            self.spec.window,
            now_ns,
        );
        let outcome = self
            .spec
            .service
            .sql_execute_at(self.spec.tenant_hash, &request, self.spec.t0_ns)
            .await
            .map_err(|e| (page, anyhow::anyhow!("{e:?}")))?;
        self.pages_run = page;
        let mut report = PageReport {
            page,
            pending_erasure: !outcome.pending_erasure.is_empty(),
            unresolved_hours: 0,
            end: None,
        };
        let digest =
            ravel_mcp::cursor::segment_set_digest(outcome.segment_set.iter().map(String::as_str));
        let kn: BTreeSet<String> = outcome.segment_set.iter().cloned().collect();
        if page == 1 {
            self.hour_lookup().await.map_err(|e| (page, e))?;
            self.k0 = kn;
            self.k0_digest = digest;
        } else if digest != self.k0_digest {
            let labelled = we::label_expiry(&self.k0, &kn, |key| self.hours.get(key).copied());
            report.unresolved_hours = labelled.unresolved_hours;
            return Ok(self.end(
                report,
                WalkEnd::Expired {
                    page,
                    causes: labelled.causes,
                },
            ));
        }
        if page >= self.spec.pages {
            return Ok(self.end(report, WalkEnd::Finished));
        }
        if !outcome.stats.row_cap_hit {
            return Ok(self.end(report, WalkEnd::Short { pages: page }));
        }
        let max_rows = self.spec.budgets.max_rows as usize;
        let tuples = outcome.output.resume_tuples(&order_columns);
        let kept = we::page_break(&tuples, max_rows, plan.total_order());
        let Some(tuple) = kept
            .checked_sub(1)
            .and_then(|last| tuples.into_iter().nth(last))
            .flatten()
        else {
            // No complete group fits a page, so the adapter mints no cursor:
            // the walk ends here.
            return Ok(self.end(report, WalkEnd::Short { pages: page }));
        };
        self.resume = Some(ResumePosition::new(tuple));
        Ok(report)
    }

    fn end(&mut self, mut report: PageReport, end: WalkEnd) -> PageReport {
        self.ended = true;
        report.end = Some(end);
        report
    }
}
