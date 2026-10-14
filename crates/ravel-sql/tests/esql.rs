//! ES|QL bound and lowered over the `logs` table (`ravel_sql::esql`), run
//! through the executor against a published fixture.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

#[path = "esql/support.rs"]
mod support;

#[path = "esql/binder.rs"]
mod binder;
#[path = "esql/bucket.rs"]
mod bucket;
#[path = "esql/limits.rs"]
mod limits;
#[path = "esql/lowering.rs"]
mod lowering;
#[path = "esql/types.rs"]
mod types;
#[path = "esql/window.rs"]
mod window;
