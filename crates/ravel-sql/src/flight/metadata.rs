//! The Flight SQL catalog/metadata surface: catalogs, schemas, tables, table
//! types, and SqlInfo.
//!
//! Ravel exposes the same five logical tables to every tenant (`samples`,
//! `logs`, `spans`, `alerts`, `audit`), in one schema of one catalog, so every
//! answer here is a constant. That makes the
//! interesting property a negative one: nothing in this module is
//! tenant-derived, so no metadata response can differ between two tenants and
//! therefore none can disclose another tenant's existence, table set, or
//! naming. The caller is still authenticated first (see the service module) --
//! default-deny does not become optional just because the payload is constant
//!.
//!
//! The row builders come from `arrow_flight::sql::metadata` rather than being
//! hand-rolled: those types own the protocol's exact column names, types, and
//! nullability, and they apply the request's own catalog/schema/table-name
//! filter patterns at `build()` time. Re-deriving that here would be a second
//! copy of the spec that could drift from the one the client library reads.

use std::sync::OnceLock;

use arrow_flight::error::FlightError;
use arrow_flight::sql::metadata::{
    GetCatalogsBuilder, GetDbSchemasBuilder, GetTablesBuilder, SqlInfoData, SqlInfoDataBuilder,
};
use arrow_flight::sql::{
    CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, SqlInfo,
    SqlSupportedTransaction,
};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use tonic::Status;

use crate::alerts_schema::alerts_schema;
use crate::audit_schema::audit_schema;
use crate::logs_schema::logs_schema;
use crate::schema::public_schema;
use crate::session::{ALERTS_TABLE, AUDIT_TABLE, LOGS_TABLE, SAMPLES_TABLE, SPANS_TABLE};
use crate::spans_schema::spans_schema;

/// The single catalog name every tenant sees.
pub const CATALOG_NAME: &str = "ravel";
/// The single schema name every tenant sees.
pub const SCHEMA_NAME: &str = "public";
/// The single table type every tenant sees.
pub const TABLE_TYPE: &str = "TABLE";

/// A metadata batch could not be built. These are constant inputs, so a
/// failure here is a bug in this module or in arrow, never caller-driven:
/// the detail is logged and the client gets a fixed message.
fn build_error(what: &str, err: &FlightError) -> Status {
    tracing::error!(error = %err, "failed to build flight sql {what} metadata");
    Status::internal("failed to build catalog metadata")
}

/// The one-row `CommandGetCatalogs` response.
pub(super) fn catalogs() -> Result<RecordBatch, Status> {
    let mut builder = GetCatalogsBuilder::new();
    builder.append(CATALOG_NAME);
    builder.build().map_err(|err| build_error("catalogs", &err))
}

/// The schema of a `CommandGetCatalogs` response, for `GetFlightInfo`.
pub(super) fn catalogs_schema() -> SchemaRef {
    GetCatalogsBuilder::new().schema()
}

/// The `CommandGetDbSchemas` response, with the request's own filters applied
/// by the builder.
pub(super) fn db_schemas(query: CommandGetDbSchemas) -> Result<RecordBatch, Status> {
    let mut builder: GetDbSchemasBuilder = query.into_builder();
    builder.append(CATALOG_NAME, SCHEMA_NAME);
    builder
        .build()
        .map_err(|err| build_error("db schemas", &err))
}

pub(super) fn db_schemas_schema(query: CommandGetDbSchemas) -> SchemaRef {
    let builder: GetDbSchemasBuilder = query.into_builder();
    builder.schema()
}

/// The `CommandGetTables` response: one row per registered table (ADR-1101
/// decision 1), each carrying that table's public schema when the request
/// asked for schemas.
///
/// This is static catalog metadata built from the five schema functions,
/// independent of the per-query session, which still registers exactly the one
/// table a query targets. The `logs` row carries the base schema: a tenant with
/// declared typed attribute columns (ADR-0090) sees a wider `logs` schema on
/// its own statement's `FlightInfo`, which is per tenant and per query and so
/// cannot be reported here, where every answer is a constant.
pub(super) fn tables(query: CommandGetTables) -> Result<RecordBatch, Status> {
    let mut builder: GetTablesBuilder = query.into_builder();
    for (name, schema) in [
        (SAMPLES_TABLE, public_schema()),
        (LOGS_TABLE, logs_schema()),
        (SPANS_TABLE, spans_schema()),
        (ALERTS_TABLE, alerts_schema()),
        (AUDIT_TABLE, audit_schema()),
    ] {
        builder
            .append(CATALOG_NAME, SCHEMA_NAME, name, TABLE_TYPE, &schema)
            .map_err(|err| build_error("tables", &err))?;
    }
    builder.build().map_err(|err| build_error("tables", &err))
}

pub(super) fn tables_schema(query: CommandGetTables) -> SchemaRef {
    let builder: GetTablesBuilder = query.into_builder();
    builder.schema()
}

/// The one-row `CommandGetTableTypes` response.
///
/// `GetTableTypesBuilder` is not re-exported by arrow-flight, so the builder
/// is reached through `CommandGetTableTypes::into_builder` and left
/// unnamed rather than being hand-rolled from the protocol's column list.
pub(super) fn table_types(query: CommandGetTableTypes) -> Result<RecordBatch, Status> {
    let mut builder = query.into_builder();
    builder.append(TABLE_TYPE);
    builder
        .build()
        .map_err(|err| build_error("table types", &err))
}

pub(super) fn table_types_schema(query: CommandGetTableTypes) -> SchemaRef {
    query.into_builder().schema()
}

/// The server's SqlInfo answers, built once.
///
/// Every value is a compile-time constant describing what this server is and
/// what it refuses to do, so it is built once per process rather than
/// re-derived per request. The read-only and no-DDL entries are not
/// decorative: they are the machine-readable statement of the same invariant
/// `crate::validate` enforces, so a driver that consults SqlInfo before
/// issuing DDL learns the answer without a rejected round trip.
fn sql_info_data() -> Result<&'static SqlInfoData, Status> {
    static SQL_INFO: OnceLock<Result<SqlInfoData, String>> = OnceLock::new();
    match SQL_INFO.get_or_init(|| build_sql_info().map_err(|err| err.to_string())) {
        Ok(data) => Ok(data),
        Err(message) => {
            tracing::error!(error = %message, "failed to build flight sql info");
            Err(Status::internal("failed to build catalog metadata"))
        }
    }
}

fn build_sql_info() -> Result<SqlInfoData, FlightError> {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "Ravel");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "58");
    // The read-only invariant, restated where a driver can read it.
    builder.append(SqlInfo::FlightSqlServerReadOnly, true);
    builder.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransaction::None as i32,
    );
    builder.append(SqlInfo::FlightSqlServerCancel, false);
    builder.append(SqlInfo::SqlDdlCatalog, false);
    builder.append(SqlInfo::SqlDdlSchema, false);
    builder.append(SqlInfo::SqlDdlTable, false);
    builder.build()
}

/// The `CommandGetSqlInfo` response, filtered to the requested info ids.
pub(super) fn sql_info(query: CommandGetSqlInfo) -> Result<RecordBatch, Status> {
    let data = sql_info_data()?;
    query
        .into_builder(data)
        .build()
        .map_err(|err| build_error("sql info", &err))
}

pub(super) fn sql_info_schema(query: CommandGetSqlInfo) -> Result<SchemaRef, Status> {
    let data = sql_info_data()?;
    Ok(query.into_builder(data).schema())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use datafusion::arrow::array::StringArray;

    fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        let index = batch.schema().index_of(name).expect("column exists");
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column")
    }

    #[test]
    fn catalogs_lists_exactly_one_catalog() {
        let batch = catalogs().expect("built");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(column(&batch, "catalog_name").value(0), CATALOG_NAME);
    }

    #[test]
    fn schemas_lists_exactly_one_schema() {
        let batch = db_schemas(CommandGetDbSchemas::default()).expect("built");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(column(&batch, "catalog_name").value(0), CATALOG_NAME);
        assert_eq!(column(&batch, "db_schema_name").value(0), SCHEMA_NAME);
    }

    /// ADR-1101 decision 1: every registered table is listed, and each row
    /// carries that table's own public schema. The schema half is the half a
    /// name-only assertion would miss: listing `alerts` under the `samples`
    /// schema is a discoverable surface that lies about its columns.
    ///
    /// The rows come back sorted by table name, not in the order [`tables`]
    /// appends them: `GetTablesBuilder::build` orders its output by
    /// (catalog, schema, table), which is what the Flight SQL specification
    /// requires of a `CommandGetTables` response. The expectation below is
    /// therefore the sorted list, which is what a client actually sees.
    ///
    /// `table_schema` is the IPC-encapsulated schema message the builder
    /// writes, so it is compared by decoding it back through arrow-flight's own
    /// `IpcMessage` conversion rather than by re-deriving the bytes.
    #[test]
    fn tables_lists_the_five_registered_tables() {
        use arrow_flight::IpcMessage;
        use datafusion::arrow::array::BinaryArray;
        use datafusion::arrow::datatypes::Schema;

        let expected: Vec<(&str, SchemaRef)> = vec![
            (ALERTS_TABLE, alerts_schema()),
            (AUDIT_TABLE, audit_schema()),
            (LOGS_TABLE, logs_schema()),
            (SAMPLES_TABLE, public_schema()),
            (SPANS_TABLE, spans_schema()),
        ];

        let batch = tables(CommandGetTables {
            include_schema: true,
            ..CommandGetTables::default()
        })
        .expect("built");
        assert_eq!(batch.num_rows(), expected.len());

        let names = column(&batch, "table_name");
        let got: Vec<&str> = (0..batch.num_rows()).map(|row| names.value(row)).collect();
        let want: Vec<&str> = expected.iter().map(|(name, _)| *name).collect();
        assert_eq!(got, want, "every registered table, in table-name order");

        let types = column(&batch, "table_type");
        let index = batch
            .schema()
            .index_of("table_schema")
            .expect("table_schema column");
        let schemas = batch
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("table_schema is binary");
        for (row, (name, schema)) in expected.iter().enumerate() {
            assert_eq!(types.value(row), TABLE_TYPE);
            let message = IpcMessage(bytes::Bytes::copy_from_slice(schemas.value(row)));
            let carried: Schema = Schema::try_from(message).expect("decodes");
            assert_eq!(
                &carried,
                schema.as_ref(),
                "the {name} row must carry the {name} schema"
            );
        }
    }

    /// The builders apply the request's own filter patterns, so a request
    /// naming a different catalog gets nothing rather than the table rows
    /// under a name it did not ask for.
    #[test]
    fn a_non_matching_catalog_filter_yields_no_rows() {
        let query = CommandGetTables {
            catalog: Some("not-ravel".to_string()),
            ..CommandGetTables::default()
        };
        let batch = tables(query).expect("built");
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn table_types_lists_exactly_one_type() {
        let batch = table_types(CommandGetTableTypes::default()).expect("built");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(column(&batch, "table_type").value(0), TABLE_TYPE);
    }

    /// The constant SqlInfo set must actually build; a broken entry would
    /// otherwise only surface as an internal error at runtime.
    #[test]
    fn sql_info_builds_and_reports_a_read_only_server() {
        let batch = sql_info(CommandGetSqlInfo::default()).expect("built");
        assert!(batch.num_rows() > 0);
        assert!(sql_info_schema(CommandGetSqlInfo::default()).is_ok());
    }
}
