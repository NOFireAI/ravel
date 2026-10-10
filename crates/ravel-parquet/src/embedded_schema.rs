//! A bounded check of the Arrow schema a Parquet footer embeds as its
//! `ARROW:schema` key-value, run by the footer walk before anything decodes
//! it.
//!
//! parquet 59.3.0's Arrow schema conversion, which the reader runs at every
//! scan open and the snapshot runs to describe a file, base64-decodes that
//! value, drops the 8-byte IPC prefix it starts with, and verifies the
//! flatbuffer with the verifier's default options: an apparent size of
//! 2 GiB and a million tables. The verifier counts a range again each time
//! an offset points at it, and the conversion copies it again each time, so
//! a small value whose field names all point at one long string passes
//! verification and converts to gigabytes.
//!
//! [`check_arrow_schema_value`] refuses a value longer than
//! [`MAX_ARROW_SCHEMA_BYTES`], and verifies the rest with bounds tied to the
//! verified bytes' own length: an apparent size of
//! [`APPARENT_SIZE_MULTIPLE`] times it and a table per [`TABLE_BYTES`] of
//! it. The writer shares nothing but vtables, so a schema it produces
//! expands to less than twice its own length. It then counts what the
//! conversion builds, once per reference the verifier allowed, for the
//! caller's estimate.

use std::mem::size_of;
use std::sync::Arc;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::{self, root_as_message_with_opts};
use flatbuffers::{ForwardsUOffset, Vector, VerifierOptions};

use crate::footer_shape::{ARC_COUNTS, POINTER};

/// The key-value key whose value parquet's Arrow reader decodes as an Arrow
/// IPC schema (parquet's `ARROW_SCHEMA_META_KEY`); no other key-value is
/// decoded.
pub(crate) const ARROW_SCHEMA_KEY: &[u8] = b"ARROW:schema";

/// Longest `ARROW:schema` value accepted, in base64 bytes. Arrow's writer
/// encodes an Int64 column named `column_NNNN` in about 75 base64 bytes
/// (`a_wide_writer_schema_fits_the_length_cap` measures 10,000 of them), so
/// this holds a schema of about 55,000 such columns.
pub(crate) const MAX_ARROW_SCHEMA_BYTES: usize = 4 << 20;

/// The verifier's apparent size bound, per byte verified. The writer's
/// schemas measure at most 1.85 (`writer_schemas_verify_within_the_bounds`,
/// on 1,000 unnamed Null fields), the vtables it shares between fields
/// being the bytes counted more than once; four leaves twice that, and
/// bounds what any value expands to at four times its length.
pub(crate) const APPARENT_SIZE_MULTIPLE: usize = 4;

/// The verifier's table bound is one table per this many bytes verified.
/// Every table holds at least its 4-byte vtable offset, and the writer's
/// densest schema, unnamed fields of type Null, measures 20 bytes per table,
/// more than twice this.
pub(crate) const TABLE_BYTES: usize = 8;

/// What the conversion allocates per field reference: the field as
/// `fb_to_schema` collects it (into a `Vec` that may hold twice what it
/// uses) and then behind an `Arc` in its parent's list, the copy the
/// reader's schema conversion makes of it with that copy's `Arc` and slot,
/// and a dictionary field's two boxed types.
const ARROW_FIELD_BYTES: u64 =
    4 * size_of::<Field>() as u64 + 2 * size_of::<DataType>() as u64 + 2 * (ARC_COUNTS + POINTER);
/// What the conversion allocates per custom metadata entry, beside its
/// key's and value's bytes: three hash table slots with their control
/// bytes (a table that has grown is at least seven sixteenths full), in the
/// converted schema and in the copy the reader's schema conversion makes.
const ARROW_ENTRY_BYTES: u64 = 2 * 3 * (size_of::<(String, String)>() as u64 + 1);
/// Copies the conversion makes of a field name, a metadata key or value, or
/// a timezone: the converted schema's, and the reader's own schema's.
const ARROW_BYTE_COPIES: u64 = 2;
/// A union's type id, paired with its field in the union's field list,
/// collected into a `Vec` and then into that list.
const UNION_ID_BYTES: u64 = 2 * size_of::<(i8, Arc<Field>)>() as u64;
/// The schema itself and its metadata map, in both copies.
const ARROW_SCHEMA_BYTES: u64 = 2 * size_of::<Schema>() as u64 + ARC_COUNTS;

/// Refuse an `ARROW:schema` value that is longer than
/// [`MAX_ARROW_SCHEMA_BYTES`], is not base64, does not verify within the
/// bounds the module doc describes, or is not a schema message. Returns an
/// upper estimate of what converting it to an Arrow schema allocates,
/// beside the copies of the value itself the footer walk charges.
pub(crate) fn check_arrow_schema_value(value: &[u8]) -> Result<u64, String> {
    if value.len() > MAX_ARROW_SCHEMA_BYTES {
        return Err(format!(
            "footer ARROW:schema value is {} bytes, longer than the \
             {MAX_ARROW_SCHEMA_BYTES}-byte limit",
            value.len()
        ));
    }
    let decoded = BASE64_STANDARD
        .decode(value)
        .map_err(|err| format!("footer ARROW:schema value is not base64: {err}"))?;
    let message = ipc_message(&decoded);
    let message =
        root_as_message_with_opts(&verifier_options(message.len()), message).map_err(|err| {
            format!(
                "footer ARROW:schema does not verify within the bounds of its {} bytes: {}",
                message.len(),
                err.to_string().trim_end()
            )
        })?;
    let schema = message
        .header_as_schema()
        .ok_or_else(|| "footer ARROW:schema is not a schema message".to_string())?;
    let mut tally = Tally::default();
    tally.metadata(schema.custom_metadata());
    if let Some(fields) = schema.fields() {
        for field in fields.iter() {
            tally.field(field);
        }
    }
    Ok(tally.charge())
}

/// The bounds a `len`-byte flatbuffer is verified with.
pub(crate) fn verifier_options(len: usize) -> VerifierOptions {
    VerifierOptions {
        max_tables: len / TABLE_BYTES,
        max_apparent_size: len.saturating_mul(APPARENT_SIZE_MULTIPLE),
        ..VerifierOptions::default()
    }
}

/// The flatbuffer parquet's reader verifies from a decoded value: the bytes
/// after an 8-byte prefix starting with four `0xff` bytes when the value is
/// longer than that prefix, else the whole value.
pub(crate) fn ipc_message(decoded: &[u8]) -> &[u8] {
    match decoded {
        [0xff, 0xff, 0xff, 0xff, _, _, _, _, rest @ ..] if !rest.is_empty() => rest,
        _ => decoded,
    }
}

/// What the conversion builds, counted once per reference.
#[derive(Default)]
struct Tally {
    fields: u64,
    entries: u64,
    bytes: u64,
    type_ids: u64,
}

impl Tally {
    /// A field and, as the conversion does, every child it lists. The
    /// verifier's depth bound bounds the recursion.
    fn field(&mut self, field: ipc::Field<'_>) {
        self.fields += 1;
        self.bytes += field.name().map_or(0, str::len) as u64;
        self.metadata(field.custom_metadata());
        if let Some(timestamp) = field.type_as_timestamp() {
            self.bytes += timestamp.timezone().map_or(0, str::len) as u64;
        }
        if let Some(union) = field.type_as_union() {
            self.type_ids += union.typeIds().map_or(0, |ids| ids.len()) as u64;
        }
        if let Some(children) = field.children() {
            for child in children.iter() {
                self.field(child);
            }
        }
    }

    fn metadata(&mut self, entries: Option<Vector<'_, ForwardsUOffset<ipc::KeyValue<'_>>>>) {
        for entry in entries.iter().flat_map(Vector::iter) {
            self.entries += 1;
            self.bytes += entry.key().map_or(0, str::len) as u64;
            self.bytes += entry.value().map_or(0, str::len) as u64;
        }
    }

    fn charge(&self) -> u64 {
        ARROW_SCHEMA_BYTES
            .saturating_add(self.fields.saturating_mul(ARROW_FIELD_BYTES))
            .saturating_add(self.entries.saturating_mul(ARROW_ENTRY_BYTES))
            .saturating_add(self.bytes.saturating_mul(ARROW_BYTE_COPIES))
            .saturating_add(self.type_ids.saturating_mul(UNION_ID_BYTES))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) mod tests {
    use datafusion::arrow::ipc::convert::fb_to_schema;
    use datafusion::arrow::ipc::{
        FieldBuilder, MessageBuilder, MessageHeader, MetadataVersion, NullBuilder, SchemaBuilder,
        Type, root_as_message,
    };
    use flatbuffers::FlatBufferBuilder;
    use parquet::file::metadata::ParquetMetaDataReader;

    use super::*;
    use crate::test_support::{footer_len_of, parquet_bytes};

    /// A schema message of `fields` fields that are all one table, named
    /// `name_len` bytes and of type Null when `typed`, else nameless and
    /// typeless, holding `padding` bytes no offset points at, behind the
    /// 8-byte prefix the writer puts on it.
    pub(crate) fn shared_field_schema(
        fields: usize,
        name_len: usize,
        typed: bool,
        padding: usize,
    ) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();
        fbb.create_vector(&vec![0u8; padding]);
        let name = typed.then(|| fbb.create_string(&"n".repeat(name_len)));
        let null = typed.then(|| NullBuilder::new(&mut fbb).finish());
        let mut field = FieldBuilder::new(&mut fbb);
        if let (Some(name), Some(null)) = (name, null) {
            field.add_name(name);
            field.add_nullable(true);
            field.add_type_type(Type::Null);
            field.add_type_(null.as_union_value());
        }
        let field = field.finish();
        let fields = fbb.create_vector(&vec![field; fields]);
        let mut schema = SchemaBuilder::new(&mut fbb);
        schema.add_fields(fields);
        let schema = schema.finish();
        let mut message = MessageBuilder::new(&mut fbb);
        message.add_version(MetadataVersion::V5);
        message.add_header_type(MessageHeader::Schema);
        message.add_header(schema.as_union_value());
        let message = message.finish();
        fbb.finish(message, None);
        let message = fbb.finished_data();
        let mut out = vec![0xff; 4];
        out.extend((message.len() as u32).to_le_bytes());
        out.extend(message);
        out
    }

    /// What the schema converted from `value` holds: its fields, and its
    /// metadata map.
    pub(crate) fn converted_size(value: &[u8]) -> u64 {
        let decoded = BASE64_STANDARD.decode(value).expect("base64");
        let message = root_as_message(ipc_message(&decoded)).expect("verifies");
        let schema = fb_to_schema(message.header_as_schema().expect("a schema"));
        let metadata: usize = schema
            .metadata()
            .iter()
            .map(|(k, v)| k.capacity() + v.capacity() + size_of::<(String, String)>())
            .sum();
        (schema.fields().size() + metadata) as u64
    }

    /// Guards: the apparent size bound in `verifier_options`. The review's
    /// value: a thousand fields naming one 64 KiB string, which the default
    /// options pass and the conversion would copy into 64 MiB of names.
    #[test]
    fn fields_sharing_one_long_name_are_refused() {
        let schema = shared_field_schema(1000, 64 << 10, true, 0);
        let message = ipc_message(&schema);
        assert!(
            root_as_message(message).is_ok(),
            "the default bounds pass it"
        );
        assert_eq!(
            check_arrow_schema_value(BASE64_STANDARD.encode(&schema).as_bytes())
                .expect_err("refused"),
            format!(
                "footer ARROW:schema does not verify within the bounds of its {} bytes: \
                 Apparent size too large.",
                message.len()
            )
        );
    }

    /// The most fields per byte the bounds admit: Null fields that are all
    /// one table, with no name, padded just enough to pass. Their charge
    /// covers what converting them holds.
    #[test]
    fn the_densest_schema_the_bounds_admit_is_charged_what_it_converts_to() {
        let fields = 1000;
        let (padding, value) = (0..)
            .step_by(256)
            .map(|padding| {
                let schema = shared_field_schema(fields, 0, true, padding);
                (padding, BASE64_STANDARD.encode(schema))
            })
            .find(|(_, value)| check_arrow_schema_value(value.as_bytes()).is_ok())
            .expect("some padding passes");
        let charge = check_arrow_schema_value(value.as_bytes()).expect("passes");
        let converted = converted_size(value.as_bytes());
        assert!(
            converted <= charge,
            "{padding} bytes of padding: converted {converted}, charged {charge}"
        );
    }

    /// Guards: the table bound in `verifier_options`. Nameless, typeless
    /// fields that are all one table, padded with bytes nothing points at,
    /// stay inside the apparent size bound, but not the table bound.
    #[test]
    fn more_tables_than_the_bytes_hold_are_refused() {
        let schema = shared_field_schema(1000, 0, false, 2000);
        let message = ipc_message(&schema);
        let apparent_only = VerifierOptions {
            max_tables: usize::MAX,
            ..verifier_options(message.len())
        };
        assert!(root_as_message_with_opts(&apparent_only, message).is_ok());
        assert_eq!(
            check_arrow_schema_value(BASE64_STANDARD.encode(&schema).as_bytes())
                .expect_err("refused"),
            format!(
                "footer ARROW:schema does not verify within the bounds of its {} bytes: \
                 Too many tables.",
                message.len()
            )
        );
    }

    /// Guards: the length check in `check_arrow_schema_value`, which comes
    /// before the value is decoded.
    #[test]
    fn a_value_longer_than_the_limit_is_refused() {
        let value = vec![b'A'; MAX_ARROW_SCHEMA_BYTES + 1];
        assert_eq!(
            check_arrow_schema_value(&value).expect_err("refused"),
            "footer ARROW:schema value is 4194305 bytes, longer than the 4194304-byte limit"
        );
        let at_limit = check_arrow_schema_value(&value[1..]).expect_err("refused");
        assert!(!at_limit.contains("limit"), "{at_limit}");
    }

    /// Guards: `Tally::charge`. A writer schema of two fields named `a` and
    /// `b` with no metadata is charged the schema, two fields and two copies
    /// of each name's byte, exactly.
    #[test]
    fn the_charge_counts_each_field_and_name() {
        let bytes = parquet_bytes(&[1], &["x"]);
        let end = bytes.len() - 8;
        let footer = &bytes[end - footer_len_of(&bytes) as usize..end];
        let metadata = ParquetMetaDataReader::decode_metadata(footer).expect("footer");
        let value = metadata
            .file_metadata()
            .key_value_metadata()
            .and_then(|kv| kv.iter().find(|kv| kv.key == "ARROW:schema"))
            .and_then(|kv| kv.value.clone())
            .expect("ArrowWriter embeds its schema");
        let charge = check_arrow_schema_value(value.as_bytes()).expect("passes");
        assert_eq!(
            charge,
            ARROW_SCHEMA_BYTES + 2 * ARROW_FIELD_BYTES + 2 * ARROW_BYTE_COPIES
        );
        assert!(converted_size(value.as_bytes()) <= charge);
    }

    #[test]
    fn a_value_that_is_not_base64_is_refused() {
        assert_eq!(
            check_arrow_schema_value(b"not base64!").expect_err("refused"),
            "footer ARROW:schema value is not base64: Invalid symbol 32, offset 3."
        );
    }
}
