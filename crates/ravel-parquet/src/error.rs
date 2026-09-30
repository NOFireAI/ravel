use std::sync::Arc;

use ravel_object_store::StoreError;

/// Why a read of one manifest file failed.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ParquetReadError {
    /// The object no longer matches the ETag the manifest pinned: it was
    /// overwritten after the table was created.
    #[error(
        "Parquet file {key} changed after the table was created; run CREATE OR REPLACE on the \
         table to read its current files"
    )]
    FileChanged { key: String },
    /// The object, or the version of it the manifest pinned, no longer exists.
    #[error(
        "Parquet file {key} no longer exists as the table recorded it; run CREATE OR REPLACE on \
         the table to read its current files"
    )]
    FileMissing { key: String },
    #[error("reading Parquet file {key}: {source}")]
    Store {
        key: String,
        #[source]
        source: Arc<StoreError>,
    },
    /// The bytes or the requested range disagree with what the manifest
    /// recorded about the file.
    #[error("Parquet file {key}: {message}")]
    Corrupt { key: String, message: String },
    #[error("reading Parquet file {key}: the concurrent read it waited on was lost")]
    LeaderLost { key: String },
    /// Reserving the bytes of a range against the process memory budget was
    /// refused, so the range was not requested. The same facts as
    /// `ravel_query::FetchError::FetchMemoryExhausted`, which the SQL layer
    /// maps it to.
    #[error(
        "fetch memory exhausted: requested {requested} bytes, {reserved} of {limit} byte budget \
         already reserved"
    )]
    MemoryExhausted {
        requested: u64,
        reserved: u64,
        limit: u64,
    },
    /// The request would have taken the query's S3 request count past its
    /// budget, so it was not issued. `requests` is the count it would have
    /// reached.
    #[error("S3 request budget exceeded: {requests} requests, maximum {max}")]
    RequestBudgetExceeded { requests: u64, max: u64 },
    /// The request would have taken the query's scanned wire bytes past its
    /// budget, so it was not issued. `scanned` is the total it would have
    /// reached.
    #[error("bytes-scanned budget exceeded: {scanned} bytes, maximum {max}")]
    BytesBudgetExceeded { scanned: u64, max: u64 },
}

impl ParquetReadError {
    pub(crate) fn into_parquet(self) -> parquet::errors::ParquetError {
        parquet::errors::ParquetError::External(Box::new(self))
    }
}

/// Why a [`crate::ParquetTableProvider`] could not be built.
#[derive(Debug, thiserror::Error)]
pub enum ParquetTableError {
    #[error("Parquet table {table} is dropped")]
    Dropped { table: String },
    #[error("Parquet table {table} lists no files")]
    NoFiles { table: String },
    #[error("Parquet table {table}: no store was supplied for profile {profile}, bucket {bucket}")]
    NoStore {
        table: String,
        profile: String,
        bucket: String,
    },
    #[error("Parquet table {table}: option {key} = {value:?} is invalid: {reason}")]
    Option {
        table: String,
        key: String,
        value: String,
        reason: String,
    },
    #[error("Parquet table {table}: {source}")]
    Read {
        table: String,
        #[source]
        source: ParquetReadError,
    },
    #[error("Parquet table {table}: {message}")]
    Schema { table: String, message: String },
    #[error("Parquet table {table}: {source}")]
    Plan {
        table: String,
        #[source]
        source: datafusion::error::DataFusionError,
    },
}
