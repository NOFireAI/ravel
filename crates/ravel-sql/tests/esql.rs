//! ES|QL bound and lowered over the `logs` table (`ravel_sql::esql`), run
//! through the executor against a published fixture.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

#[path = "esql_support.rs"]
mod support;

#[path = "esql_binder.rs"]
mod esql_binder;
#[path = "esql_lowering.rs"]
mod esql_lowering;
#[path = "esql_types.rs"]
mod esql_types;
#[path = "esql_window.rs"]
mod esql_window;
