//! ClickBench Parquet lane (ADR-2040, issue #2055 task T5b): the suite
//! definition, result comparator, and synthetic fixture a later task's
//! engines and acceptance test consume. Each module here is unit-tested on
//! its own; nothing in this module wires an engine to a live object store.

pub mod suite;
