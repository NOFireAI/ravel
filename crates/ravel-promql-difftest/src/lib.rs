//! Differential test harness comparing Ravel's PromQL evaluator against a
//! pinned real Prometheus binary (ADR-0021 decisions 4-5,
//! docs/promql-evaluator-plan.md section 5). Dev-only: no production crate
//! depends on this one.
//!
//! Modules mirror the section 5.1 pipeline: [`generator`] builds a seeded
//! dataset, [`encode`] turns it into a Remote Write 1.0 payload for the
//! Prometheus side, [`ravel_stack`] feeds the identical tuples into Ravel's
//! own ingest/query stack, [`prometheus_process`] and [`prometheus_client`]
//! drive the pinned binary, [`corpus`] parses query fixtures, and
//! [`comparator`] and [`runner`] execute a corpus against both stacks and
//! diff the results.
//!
//! [`scoring`] sits on top of all of that: it classifies the PromQL surface
//! per ADR-0035 and turns a corpus run into the per-construct conformance
//! table published in `docs/query-engine.md` (issue #133).

pub mod comparator;
pub mod corpus;
pub mod encode;
pub mod generator;
pub mod prometheus_client;
pub mod prometheus_process;
mod proto;
pub mod ravel_stack;
pub mod runner;
pub mod scoring;
