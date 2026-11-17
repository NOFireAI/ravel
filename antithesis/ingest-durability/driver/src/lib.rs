//! The ingest-durability driver's acked-record model and its OTLP and SQL
//! client, as a library so other Antithesis drivers (catalog-maintenance)
//! judge rows against the same model.

pub mod model;
pub mod otlp;
pub mod ravel;
