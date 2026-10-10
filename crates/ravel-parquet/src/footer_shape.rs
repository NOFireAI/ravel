//! A structural check of a Parquet footer's Thrift compact encoding, run
//! before the footer is handed to the `parquet` decoder.
//!
//! The decoder sizes a collection from the count its header declares and
//! does not compare that count with the bytes that remain, so a footer of a
//! few bytes can ask for an allocation of many gigabytes before a single
//! element is read; an allocation the allocator refuses aborts the process,
//! which no `catch_unwind` contains.
//!
//! [`check_footer_shape`] walks the footer the way parquet 59.3.0's decoder
//! reads it, without building anything. The decoder picks how to read a
//! field by its id, not by the type in the field header, so the walk does
//! the same: [`field`] is the decoder's table of the fields it reads, per
//! struct, and a field whose header type differs from the type the decoder
//! reads for that id is refused. Every other field is skipped exactly as the
//! decoder skips it, and a skip the decoder would get wrong (a list of
//! booleans, whose elements it skips without reading a byte) is refused. A
//! set, a map or a UUID in a skipped field is refused too, although the
//! decoder skips them, and so is a field the decoder reads appearing twice
//! in one struct. A collection whose declared count could not fit
//! in the bytes after its header, at one byte per element (two per map
//! entry), is refused.
//!
//! The schema is a flattened tree whose `num_children` values the decoder
//! sizes each group's child list from, and recurses on once per level. The
//! walk runs the decoder's tree construction over the elements as it reads
//! them and refuses a negative `num_children`, one larger than the elements
//! after it, elements that are not exactly one tree, and a tree deeper than
//! [`MAX_SCHEMA_DEPTH`]. It refuses a row group that comes before any schema
//! or does not have one column chunk per leaf column, as the decoder does.
//!
//! A footer that passes can still decode to many times its own size: a
//! column chunk of a few bytes on the wire becomes a [`ColumnChunkMetaData`]
//! of a few hundred, and the decoder reserves one per leaf column for every
//! row group it reads. The walk therefore also returns an upper estimate of
//! the bytes the decoder allocates, from the counts it has checked, which
//! the caller reserves from its memory budget before decoding.
//!
//! An `ARROW:schema` key-value's value is an Arrow schema the Arrow reader
//! decodes from the metadata; the walk verifies it with bounds tied to its
//! length and adds what converting it allocates to the estimate
//! ([`check_arrow_schema_value`]).
//!
//! The walk does work linear in the footer's length, and its recursion is
//! bounded by the fixed nesting of the decoder's structs plus
//! [`SKIP_DEPTH`].

use std::mem::size_of;
use std::ops::Range;

use parquet::basic::ColumnOrder;
use parquet::file::metadata::{
    ColumnChunkMetaData, KeyValue, ParquetMetaData, RowGroupMetaData, SortingColumn,
};
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor, Type};

use crate::embedded_schema::{ARROW_SCHEMA_KEY, check_arrow_schema_value};

/// How deep the decoder's skip of an unknown field nests before it refuses
/// (parquet's `DEFAULT_SKIP_DEPTH`); the walk refuses at the same depth.
const SKIP_DEPTH: u8 = 64;

/// Most row groups the decoder reads: it numbers them with an `i16`
/// ordinal and refuses the footer at the first that does not fit.
const MAX_ROW_GROUPS: u64 = i16::MAX as u64 + 1;

// What the decoder allocates, per item the walk counts. Each is the size of
// the type the decoder builds, plus the slot holding it and an `Arc`'s two
// reference counts where the decoder puts it behind one.
pub(crate) const POINTER: u64 = size_of::<usize>() as u64;
pub(crate) const ARC_COUNTS: u64 = 2 * POINTER;
/// The metadata itself, and one schema descriptor per schema read.
const METADATA_BYTES: u64 = size_of::<ParquetMetaData>() as u64;
const SCHEMA_BYTES: u64 = size_of::<SchemaDescriptor>() as u64 + ARC_COUNTS;
/// parquet's crate-private `SchemaElement`, which the decoder collects every
/// element of the schema into before it builds the tree. Its fields are an
/// `Option` of a `LogicalType`, a `&str`, five `Option<i32>` and three
/// `Option`s of fieldless enums; `schema_element_bound_covers_its_fields`
/// checks this bound against their sizes.
const SCHEMA_ELEMENT_BYTES: u64 = 160;
/// One node of the schema tree, and its slot in its parent's child list.
const SCHEMA_NODE_BYTES: u64 = size_of::<Type>() as u64 + ARC_COUNTS + POINTER;
/// One leaf column's descriptor, and its slots in the descriptor's leaf and
/// leaf-to-root lists.
const LEAF_BYTES: u64 = size_of::<ColumnDescriptor>() as u64 + ARC_COUNTS + 2 * POINTER;
/// One name in a leaf's path, which the decoder copies for every leaf, beside
/// the name's own bytes.
const PATH_NAME_BYTES: u64 = size_of::<String>() as u64;
const ROW_GROUP_BYTES: u64 = size_of::<RowGroupMetaData>() as u64;
const COLUMN_CHUNK_BYTES: u64 = size_of::<ColumnChunkMetaData>() as u64;
const KEY_VALUE_BYTES: u64 = size_of::<KeyValue>() as u64;
const SORTING_COLUMN_BYTES: u64 = size_of::<SortingColumn>() as u64;
const COLUMN_ORDER_BYTES: u64 = size_of::<ColumnOrder>() as u64;
/// The boxed geospatial statistics of a column chunk, whose type parquet
/// does not export without its `experimental` feature: four `f64` and four
/// `Option<f64>` bounds and an `Option<Vec<i32>>`, about 128 bytes.
const GEO_STATISTICS_BYTES: u64 = 256;
/// Copies made of a string or binary field the decoder reads: a schema name
/// is copied into its tree node and again into the Arrow field it is
/// converted to, a `GeometryType` CRS is copied and cloned, a key-value's
/// key and value are copied by the decoder and again into the map the Arrow
/// reader parses them into, and an `ARROW:schema` value is also decoded
/// from base64 into three quarters of its length. Every other one is copied
/// once.
const BINARY_COPIES: u64 = 3;

/// Deepest nesting of groups a footer's schema may declare, the root
/// included. The decoder builds the schema tree, and every walk over it,
/// by recursing once per level, and copies each leaf's full path; real
/// schemas nest a handful of levels.
pub(crate) const MAX_SCHEMA_DEPTH: usize = 64;

/// Longest varint the compact protocol writes: ten bytes for a 64-bit value.
const MAX_VARINT_BYTES: usize = 10;

// Compact protocol type codes.
const T_STOP: u8 = 0;
const T_BOOL_TRUE: u8 = 1;
const T_BOOL_FALSE: u8 = 2;
const T_BYTE: u8 = 3;
const T_I16: u8 = 4;
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_DOUBLE: u8 = 7;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_SET: u8 = 10;
const T_MAP: u8 = 11;
const T_STRUCT: u8 = 12;

/// The structs and unions the decoder reads from a footer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    FileMetaData,
    SchemaElement,
    LogicalType,
    DecimalType,
    TimeType,
    IntType,
    VariantType,
    GeometryType,
    GeographyType,
    TimeUnit,
    RowGroup,
    ColumnChunk,
    ColumnMetaData,
    Statistics,
    PageEncodingStats,
    SizeStatistics,
    GeospatialStatistics,
    BoundingBox,
    SortingColumn,
    KeyValue,
    ColumnOrder,
}

impl Shape {
    /// A union: exactly one field, then the stop byte.
    fn is_union(self) -> bool {
        matches!(self, Self::LogicalType | Self::TimeUnit | Self::ColumnOrder)
    }
}

/// How the decoder reads a field it knows.
#[derive(Clone, Copy, Debug)]
enum Kind {
    Bool,
    Byte,
    I16,
    /// Also every enum: the decoder reads an enum as an i32.
    I32,
    I64,
    Double,
    /// Also every string.
    Binary,
    Struct(Shape),
    /// A union variant without fields: the decoder reads one `0x00` byte.
    Empty,
    List(Elem),
}

/// The element the decoder reads from a known list. The list header's own
/// element type must match it, as the decoder also requires.
#[derive(Clone, Copy, Debug)]
enum Elem {
    I32,
    I64,
    Struct(Shape),
}

impl Kind {
    /// The field header type a writer puts on this kind.
    fn code(self) -> u8 {
        match self {
            Self::Bool => T_BOOL_TRUE,
            Self::Byte => T_BYTE,
            Self::I16 => T_I16,
            Self::I32 => T_I32,
            Self::I64 => T_I64,
            Self::Double => T_DOUBLE,
            Self::Binary => T_BINARY,
            Self::Struct(_) | Self::Empty => T_STRUCT,
            Self::List(_) => T_LIST,
        }
    }

    /// Whether a field header of type `code` carries this kind. A struct
    /// field's boolean is its header type, true or false.
    fn matches(self, code: u8) -> bool {
        match self {
            Self::Bool => code == T_BOOL_TRUE || code == T_BOOL_FALSE,
            other => code == other.code(),
        }
    }
}

impl Elem {
    fn code(self) -> u8 {
        match self {
            Self::I32 => T_I32,
            Self::I64 => T_I64,
            Self::Struct(_) => T_STRUCT,
        }
    }
}

/// What the decoder does with field `id` of `shape`.
#[derive(Debug)]
enum Field {
    Read(Kind),
    /// Skip the field by its header type.
    Skip,
    /// The walk refuses the field.
    Refuse(&'static str),
}

/// The decoder's field table: parquet 59.3.0's `parquet_metadata_from_bytes`,
/// `read_row_group`, `read_column_chunk` and `read_column_metadata`
/// (src/file/metadata/thrift/mod.rs, called with no options), the
/// `thrift_struct!`/`thrift_union!` definitions they read, and the
/// hand-written `LogicalType` and `ColumnOrder` unions (src/basic.rs).
/// A field id not listed is skipped by the decoder; so is a field it reads
/// past (`ColumnMetaData` 3 and 8, `RowGroup` 6).
fn field(shape: Shape, id: i16) -> Field {
    use Field::{Read, Refuse, Skip};
    use Kind::{Binary, Bool, Byte, Double, Empty, I16, I32, I64, List, Struct};
    use Shape as S;
    match (shape, id) {
        (S::FileMetaData, 1) => Read(I32),
        (S::FileMetaData, 2) => Read(List(Elem::Struct(S::SchemaElement))),
        (S::FileMetaData, 3) => Read(I64),
        (S::FileMetaData, 4) => Read(List(Elem::Struct(S::RowGroup))),
        (S::FileMetaData, 5) => Read(List(Elem::Struct(S::KeyValue))),
        (S::FileMetaData, 6) => Read(Binary),
        (S::FileMetaData, 7) => Read(List(Elem::Struct(S::ColumnOrder))),
        // Read or skipped depending on parquet's `encryption` feature; an
        // encrypted file is refused anyway.
        (S::FileMetaData | S::ColumnChunk, 8 | 9) => Refuse("encryption metadata"),

        (S::SchemaElement, 1 | 2 | 3 | 5 | 6 | 7 | 8 | 9) => Read(I32),
        (S::SchemaElement, 4) => Read(Binary),
        (S::SchemaElement, 10) => Read(Struct(S::LogicalType)),

        (S::LogicalType, 1 | 2 | 3 | 4 | 6 | 11 | 12 | 13 | 14 | 15) => Read(Empty),
        (S::LogicalType, 5) => Read(Struct(S::DecimalType)),
        (S::LogicalType, 7) => Read(Struct(S::TimeType)),
        (S::LogicalType, 8) => Read(Struct(S::TimeType)),
        (S::LogicalType, 10) => Read(Struct(S::IntType)),
        (S::LogicalType, 16) => Read(Struct(S::VariantType)),
        (S::LogicalType, 17) => Read(Struct(S::GeometryType)),
        (S::LogicalType, 18) => Read(Struct(S::GeographyType)),
        (S::DecimalType, 1 | 2) => Read(I32),
        (S::TimeType, 1) => Read(Bool),
        (S::TimeType, 2) => Read(Struct(S::TimeUnit)),
        (S::IntType, 1) => Read(Byte),
        (S::IntType, 2) => Read(Bool),
        (S::VariantType, 1) => Read(Byte),
        (S::GeometryType, 1) => Read(Binary),
        (S::GeographyType, 1) => Read(Binary),
        (S::GeographyType, 2) => Read(I32),
        (S::TimeUnit, 1..=3) => Read(Empty),
        (S::TimeUnit, _) => Refuse("an unknown TimeUnit"),

        (S::RowGroup, 1) => Read(List(Elem::Struct(S::ColumnChunk))),
        (S::RowGroup, 2 | 3 | 5) => Read(I64),
        (S::RowGroup, 4) => Read(List(Elem::Struct(S::SortingColumn))),
        (S::RowGroup, 7) => Read(I16),

        (S::ColumnChunk, 1) => Read(Binary),
        (S::ColumnChunk, 2 | 4 | 6) => Read(I64),
        (S::ColumnChunk, 3) => Read(Struct(S::ColumnMetaData)),
        (S::ColumnChunk, 5 | 7) => Read(I32),

        (S::ColumnMetaData, 1 | 4 | 15) => Read(I32),
        (S::ColumnMetaData, 2) => Read(List(Elem::I32)),
        (S::ColumnMetaData, 5 | 6 | 7 | 9 | 10 | 11 | 14) => Read(I64),
        (S::ColumnMetaData, 12) => Read(Struct(S::Statistics)),
        (S::ColumnMetaData, 13) => Read(List(Elem::Struct(S::PageEncodingStats))),
        (S::ColumnMetaData, 16) => Read(Struct(S::SizeStatistics)),
        (S::ColumnMetaData, 17) => Read(Struct(S::GeospatialStatistics)),

        (S::Statistics, 1 | 2 | 5 | 6) => Read(Binary),
        (S::Statistics, 3 | 4) => Read(I64),
        (S::Statistics, 7 | 8) => Read(Bool),
        (S::PageEncodingStats, 1..=3) => Read(I32),
        (S::SizeStatistics, 1) => Read(I64),
        (S::SizeStatistics, 2 | 3) => Read(List(Elem::I64)),
        (S::GeospatialStatistics, 1) => Read(Struct(S::BoundingBox)),
        (S::GeospatialStatistics, 2) => Read(List(Elem::I32)),
        (S::BoundingBox, 1..=8) => Read(Double),
        (S::SortingColumn, 1) => Read(I32),
        (S::SortingColumn, 2 | 3) => Read(Bool),
        (S::KeyValue, 1 | 2) => Read(Binary),
        (S::ColumnOrder, 1) => Read(Empty),

        _ => Skip,
    }
}

/// Walk `footer` as a `FileMetaData` the way the decoder reads it, and
/// refuse it if any field's header type differs from the type the decoder
/// reads for its id, if a struct repeats a field the decoder reads, if any
/// collection declares more elements than the bytes after its header could
/// hold, if a field the decoder skips is one it would skip wrongly or
/// refuse, if its schema is not the bounded tree
/// the module doc describes, if a row group does not match the schema, if
/// an `ARROW:schema` value fails [`check_arrow_schema_value`], or if it is
/// not well-formed. Bytes after the struct's end are not inspected; the
/// decoder is the authority on everything this does not check.
///
/// Returns an upper estimate of the bytes the decoder allocates for the
/// footer, its input aside: the lists it collects, a column chunk per leaf
/// column for every row group it reads, the schema tree and each leaf's
/// path, copies of the strings and binaries it reads, and the Arrow schema
/// converted from every `ARROW:schema` value.
pub(crate) fn check_footer_shape(footer: &[u8]) -> Result<u64, String> {
    let mut walk = Walk {
        bytes: footer,
        pos: 0,
        leaves: None,
        decoded: METADATA_BYTES,
    };
    walk.read_struct(Shape::FileMetaData)?;
    Ok(walk.decoded)
}

struct Walk<'a> {
    bytes: &'a [u8],
    pos: usize,
    /// The leaf columns of the last schema read, which every row group
    /// read after it must have one column chunk for.
    leaves: Option<u64>,
    /// The estimate [`check_footer_shape`] returns, so far.
    decoded: u64,
}

/// A value the walk reads, where it needs it.
enum Value {
    Int(i64),
    /// Where a binary's bytes are in the footer.
    Bytes(Range<usize>),
    Other,
}

/// What the walk reads from one struct: for a `SchemaElement`, what the
/// tree builder reads, and for a `KeyValue`, its key and value. The walk
/// refuses a field read twice, so each is its field's only value.
#[derive(Default)]
struct Element {
    has_type: bool,
    num_children: Option<i32>,
    name_len: u64,
    key: Option<Range<usize>>,
    value: Option<Range<usize>>,
}

/// A group of the schema tree that the next element is inside.
struct Open {
    /// Its children still to come.
    children: u64,
    /// What its name adds to the path of each leaf below it.
    path: u64,
}

/// The decoder's `parquet_schema_from_array`, run over the schema elements
/// as the walk reads them, without building the tree: the elements are a
/// depth-first listing in which a group's `num_children` following elements
/// are its children.
struct SchemaTree {
    elements: u64,
    open: Vec<Open>,
    /// The sum of `path` over `open`.
    prefix: u64,
    leaves: u64,
    /// The bytes of every leaf's copied path.
    paths: u64,
}

impl SchemaTree {
    fn new(elements: u64) -> Self {
        Self {
            elements,
            open: Vec::new(),
            prefix: 0,
            leaves: 0,
            paths: 0,
        }
    }

    fn add(&mut self, index: u64, element: &Element) -> Result<(), String> {
        let root = self.open.is_empty();
        if root && index > 0 {
            return Err(format!(
                "footer schema element {index} starts a second root"
            ));
        }
        let children = match element.num_children {
            None => 0,
            Some(n) => u64::try_from(n)
                .map_err(|_| format!("footer schema element {index} declares {n} children"))?,
        };
        let after = self.elements - index - 1;
        if children > after {
            return Err(format!(
                "footer schema element {index} declares {children} children with {after} \
                 elements after it"
            ));
        }
        // The root's name is not part of a leaf's path.
        let path = if root {
            0
        } else {
            PATH_NAME_BYTES.saturating_add(element.name_len)
        };
        if children > 0 {
            if self.open.len() >= MAX_SCHEMA_DEPTH {
                return Err(format!(
                    "footer schema nests deeper than {MAX_SCHEMA_DEPTH} levels at element {index}"
                ));
            }
            self.open.push(Open { children, path });
            self.prefix = self.prefix.saturating_add(path);
            return Ok(());
        }
        // A primitive column, or a group with no children.
        if !root && element.has_type {
            self.leaves += 1;
            self.paths = self.paths.saturating_add(self.prefix).saturating_add(path);
        }
        // Close every group this element was the last child of.
        while let Some(group) = self.open.last_mut() {
            group.children -= 1;
            if group.children > 0 {
                break;
            }
            self.prefix = self.prefix.saturating_sub(group.path);
            self.open.pop();
        }
        Ok(())
    }

    /// The leaf column count, and the bytes the decoder allocates for the
    /// leaves' descriptors and paths, once every element is added.
    fn finish(self) -> Result<(u64, u64), String> {
        if self.elements == 0 {
            return Err("footer schema has no elements".to_string());
        }
        if !self.open.is_empty() {
            return Err(format!(
                "footer schema's num_children describe more than its {} elements",
                self.elements
            ));
        }
        let leaves = self.leaves.saturating_mul(LEAF_BYTES);
        Ok((self.leaves, leaves.saturating_add(self.paths)))
    }
}

impl Walk<'_> {
    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn byte(&mut self) -> Result<u8, String> {
        let b = *self
            .bytes
            .get(self.pos)
            .ok_or_else(|| format!("footer ends at byte {} inside a value", self.pos))?;
        self.pos += 1;
        Ok(b)
    }

    fn advance(&mut self, n: usize) -> Result<(), String> {
        if n > self.remaining() {
            return Err(format!(
                "footer declares {n} bytes at byte {} with {} left",
                self.pos,
                self.remaining()
            ));
        }
        self.pos += n;
        Ok(())
    }

    /// An unsigned varint, decoded as the decoder's `read_vlq` decodes it.
    /// The decoder reads a longer one too; no writer produces it.
    fn varint(&mut self) -> Result<u64, String> {
        let start = self.pos;
        let mut value: u64 = 0;
        for i in 0..MAX_VARINT_BYTES {
            let b = self.byte()?;
            value |= u64::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(format!(
            "footer varint at byte {start} runs past {MAX_VARINT_BYTES} bytes"
        ))
    }

    /// A zigzag varint, as the decoder's `read_zig_zag` decodes it.
    fn zigzag(&mut self) -> Result<i64, String> {
        let raw = self.varint()?;
        Ok((raw >> 1) as i64 ^ -((raw & 1) as i64))
    }

    /// Refuse a declared count that the bytes left could not hold at
    /// `min_bytes` per element.
    fn count_fits(&self, count: u64, min_bytes: u64, what: &str) -> Result<usize, String> {
        let left = self.remaining() as u64;
        if count.saturating_mul(min_bytes) > left {
            return Err(format!(
                "footer {what} at byte {} declares {count} elements with {left} bytes left",
                self.pos
            ));
        }
        usize::try_from(count).map_err(|_| format!("footer {what} count {count} does not fit"))
    }

    /// A list or set header, as the decoder's `read_list_begin` reads it:
    /// `None` for the single byte `0x00`, which the decoder takes as an
    /// empty list (some writers put element type 0 on one), else the
    /// element type and the declared count.
    fn list_header(&mut self) -> Result<Option<(u8, u64)>, String> {
        let at = self.pos;
        let header = self.byte()?;
        if header == 0 {
            return Ok(None);
        }
        let element = header & 0x0f;
        if element == T_STOP || element > T_STRUCT {
            return Err(format!(
                "footer list at byte {at} has element type {element}"
            ));
        }
        let short = u64::from(header >> 4);
        let count = if short == 15 { self.varint()? } else { short };
        if count > i32::MAX as u64 {
            return Err(format!(
                "footer list at byte {at} declares {count} elements, past i32"
            ));
        }
        Ok(Some((element, count)))
    }

    /// A struct's field header: `None` at its stop byte, else the header
    /// type and the field id, computed as the decoder's `read_field_begin`
    /// computes it from `last`.
    fn field_header(&mut self, last: i16) -> Result<Option<(u8, i16)>, String> {
        let at = self.pos;
        let header = self.byte()?;
        let code = header & 0x0f;
        if code == T_STOP {
            return Ok(None);
        }
        if code > T_STRUCT {
            return Err(format!(
                "footer has unknown compact type {code} at byte {at}"
            ));
        }
        let delta = header >> 4;
        let id = if delta == 0 {
            self.zigzag()? as i16
        } else {
            last.checked_add(i16::from(delta))
                .ok_or_else(|| format!("footer field id at byte {at} overflows i16"))?
        };
        Ok(Some((code, id)))
    }

    /// Read a struct of `shape` field by field, as the decoder does, and
    /// return what [`Element`] keeps of it. A `KeyValue` whose key is
    /// `ARROW:schema` has its value checked and its conversion charged.
    fn read_struct(&mut self, shape: Shape) -> Result<Element, String> {
        let start = self.pos;
        let mut last = 0i16;
        let mut fields = 0usize;
        // The ids of the fields read so far; every id the decoder reads is
        // below 64.
        let mut read = 0u64;
        let mut element = Element::default();
        loop {
            let at = self.pos;
            let Some((code, id)) = self.field_header(last)? else {
                if shape.is_union() && fields == 0 {
                    return Err(format!("footer {shape:?} at byte {start} has no field"));
                }
                if shape == Shape::KeyValue {
                    self.check_key_value(&element)?;
                }
                return Ok(element);
            };
            if shape.is_union() && fields == 1 {
                return Err(format!(
                    "footer {shape:?} at byte {start} has more than one field"
                ));
            }
            match field(shape, id) {
                Field::Read(kind) => {
                    if !kind.matches(code) {
                        return Err(format!(
                            "footer field {id} of {shape:?} at byte {at} has compact type \
                             {code}, where the decoder reads {kind:?}"
                        ));
                    }
                    // No writer repeats a field. The decoder keeps the last
                    // value of most, skips a second schema, and appends a row
                    // group's second column chunk list to its first, so a
                    // repeat decodes to something other than what was walked.
                    let bit = 1u64 << (id & 63);
                    if read & bit != 0 {
                        return Err(format!(
                            "footer field {id} of {shape:?} at byte {at} repeats a field read \
                             before"
                        ));
                    }
                    read |= bit;
                    let value = self.read_value(kind, shape, id)?;
                    match (shape, id, value) {
                        (Shape::SchemaElement, 1, _) => element.has_type = true,
                        // The decoder's `read_i32` truncates the varint.
                        (Shape::SchemaElement, 5, Value::Int(n)) => {
                            element.num_children = Some(n as i32);
                        }
                        (Shape::SchemaElement, 4, Value::Bytes(range)) => {
                            element.name_len = range.len() as u64;
                        }
                        (Shape::KeyValue, 1, Value::Bytes(range)) => element.key = Some(range),
                        (Shape::KeyValue, 2, Value::Bytes(range)) => element.value = Some(range),
                        _ => {}
                    }
                }
                Field::Skip => self.skip(code, SKIP_DEPTH)?,
                Field::Refuse(what) => {
                    return Err(format!(
                        "footer field {id} of {shape:?} at byte {at} carries {what}"
                    ));
                }
            }
            last = id;
            fields += 1;
        }
    }

    fn read_value(&mut self, kind: Kind, shape: Shape, id: i16) -> Result<Value, String> {
        match kind {
            // The value is the header type.
            Kind::Bool => Ok(Value::Other),
            Kind::Byte => self.advance(1).map(|()| Value::Other),
            Kind::I16 | Kind::I32 | Kind::I64 => self.zigzag().map(Value::Int),
            Kind::Double => self.advance(8).map(|()| Value::Other),
            Kind::Binary => {
                let bytes = self.binary()?;
                self.charge((bytes.len() as u64).saturating_mul(BINARY_COPIES));
                Ok(Value::Bytes(bytes))
            }
            Kind::Struct(inner) => {
                if inner == Shape::GeospatialStatistics {
                    self.charge(GEO_STATISTICS_BYTES);
                }
                self.read_struct(inner).map(|_| Value::Other)
            }
            Kind::Empty => {
                let at = self.pos;
                if self.byte()? != T_STOP {
                    return Err(format!(
                        "footer field {id} of {shape:?} at byte {at} is not an empty struct"
                    ));
                }
                Ok(Value::Other)
            }
            Kind::List(elem) => self.read_list(elem, shape, id).map(|()| Value::Other),
        }
    }

    /// A binary's bytes, as their range in the footer.
    fn binary(&mut self) -> Result<Range<usize>, String> {
        let len = self.varint()?;
        let n =
            usize::try_from(len).map_err(|_| format!("footer binary length {len} does not fit"))?;
        self.advance(n)?;
        Ok(self.pos - n..self.pos)
    }

    /// Check a `KeyValue`'s value as an Arrow schema, and charge converting
    /// it, when its key is `ARROW:schema`. The decoder keeps every key-value
    /// and the Arrow reader decodes the last with that key; each is checked.
    fn check_key_value(&mut self, element: &Element) -> Result<(), String> {
        let bytes = self.bytes;
        let is_schema = element
            .key
            .clone()
            .and_then(|key| bytes.get(key))
            .is_some_and(|key| key == ARROW_SCHEMA_KEY);
        let value = element.value.clone().and_then(|value| bytes.get(value));
        if let (true, Some(value)) = (is_schema, value) {
            let converted = check_arrow_schema_value(value)?;
            self.charge(converted);
        }
        Ok(())
    }

    /// A known list: the decoder reads `elem` for every declared element.
    fn read_list(&mut self, elem: Elem, shape: Shape, id: i16) -> Result<(), String> {
        let at = self.pos;
        let list = (shape, id);
        if list == (Shape::FileMetaData, 4) && self.leaves.is_none() {
            return Err(format!(
                "footer row groups at byte {at} come before the schema"
            ));
        }
        let (code, count) = self.list_header()?.unwrap_or((elem.code(), 0));
        if code != elem.code() {
            return Err(format!(
                "footer list at byte {at} in field {id} of {shape:?} has element type {code}, \
                 where the decoder reads {elem:?}"
            ));
        }
        let count = self.count_fits(count, 1, "list")?;
        let declared = count as u64;
        // What the decoder allocates per declared element, before it reads
        // the first one.
        let each = match list {
            (Shape::FileMetaData, 2) => return self.read_schema(count),
            (Shape::FileMetaData, 4) => {
                if declared > MAX_ROW_GROUPS {
                    return Err(format!(
                        "footer list at byte {at} declares {declared} row groups, more than \
                         the decoder's {MAX_ROW_GROUPS}"
                    ));
                }
                // Each row group reserves a column chunk per leaf column.
                let leaves = self.leaves.unwrap_or_default();
                ROW_GROUP_BYTES.saturating_add(leaves.saturating_mul(COLUMN_CHUNK_BYTES))
            }
            (Shape::RowGroup, 1) => {
                if self.leaves != Some(declared) {
                    return Err(format!(
                        "footer row group at byte {at} has {count} column chunks for {} columns",
                        self.leaves.unwrap_or_default()
                    ));
                }
                0
            }
            (Shape::FileMetaData, 5) => KEY_VALUE_BYTES,
            (Shape::FileMetaData, 7) => COLUMN_ORDER_BYTES,
            (Shape::RowGroup, 4) => SORTING_COLUMN_BYTES,
            (Shape::SizeStatistics, 2 | 3) => size_of::<i64>() as u64,
            (Shape::GeospatialStatistics, 2) => size_of::<i32>() as u64,
            // Encodings and page encoding statistics fold into a bit mask.
            _ => 0,
        };
        self.charge(declared.saturating_mul(each));
        for _ in 0..count {
            match elem {
                Elem::I32 | Elem::I64 => {
                    self.varint()?;
                }
                Elem::Struct(inner) => {
                    self.read_struct(inner)?;
                }
            }
        }
        Ok(())
    }

    /// The schema's `count` elements, checked as the tree the decoder builds
    /// from them.
    fn read_schema(&mut self, count: usize) -> Result<(), String> {
        let elements = count as u64;
        self.charge(SCHEMA_BYTES);
        self.charge(elements.saturating_mul(SCHEMA_ELEMENT_BYTES + SCHEMA_NODE_BYTES));
        let mut tree = SchemaTree::new(elements);
        for index in 0..count {
            let element = self.read_struct(Shape::SchemaElement)?;
            tree.add(index as u64, &element)?;
        }
        let (leaves, decoded) = tree.finish()?;
        self.leaves = Some(leaves);
        self.charge(decoded);
        Ok(())
    }

    fn charge(&mut self, bytes: u64) {
        self.decoded = self.decoded.saturating_add(bytes);
    }

    /// Skip a value of header type `code` as the decoder's `skip_till_depth`
    /// does, with `depth` levels left.
    fn skip(&mut self, code: u8, depth: u8) -> Result<(), String> {
        let at = self.pos;
        if depth == 0 {
            return Err(format!(
                "footer nests deeper than {SKIP_DEPTH} levels at byte {at}"
            ));
        }
        match code {
            // A struct field's boolean is carried in its header type.
            T_BOOL_TRUE | T_BOOL_FALSE => Ok(()),
            T_BYTE => self.advance(1),
            T_I16 | T_I32 | T_I64 => self.varint().map(|_| ()),
            T_DOUBLE => self.advance(8),
            T_BINARY => self.binary().map(|_| ()),
            T_STRUCT => {
                let mut last = 0i16;
                while let Some((code, id)) = self.field_header(last)? {
                    self.skip(code, depth - 1)?;
                    last = id;
                }
                Ok(())
            }
            T_LIST | T_SET => {
                let Some((element, count)) = self.list_header()? else {
                    return Ok(());
                };
                let count = self.count_fits(count, 1, "list")?;
                // The decoder skips a boolean element without reading a
                // byte. The walk does not skip a set or a map.
                if element == T_BOOL_TRUE || element == T_BOOL_FALSE {
                    return Err(format!(
                        "footer list at byte {at} holds booleans in a field the decoder skips"
                    ));
                }
                if code == T_SET || element == T_SET || element == T_MAP {
                    return Err(format!(
                        "footer set or map at byte {at} is in a field the decoder skips"
                    ));
                }
                for _ in 0..count {
                    self.skip(element, depth - 1)?;
                }
                Ok(())
            }
            T_MAP => {
                let count = self.varint()?;
                if count != 0 {
                    self.byte()?;
                    self.count_fits(count, 2, "map")?;
                }
                Err(format!(
                    "footer set or map at byte {at} is in a field the decoder skips"
                ))
            }
            other => Err(format!(
                "footer has unknown compact type {other} at byte {at}"
            )),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;
    use bytes::Bytes;
    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
        Float64Array, Int8Array, Int64Array, Int64Builder, ListBuilder, MapBuilder, RecordBatch,
        StringArray, StringBuilder, StructArray, Time64MicrosecondArray, TimestampNanosecondArray,
        UInt32Array,
    };
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{
        DataType, Field as ArrowField, Schema, TimeUnit, UnionFields, UnionMode,
    };
    use datafusion::arrow::ipc::root_as_message_with_opts;
    use flatbuffers::VerifierOptions;
    use parquet::arrow::{ArrowWriter, encode_arrow_schema};
    use parquet::file::metadata::{KeyValue, ParquetMetaDataReader, SortingColumn};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use proptest::prelude::*;
    use ravel_memory::MemoryBudget;

    use super::*;
    use crate::embedded_schema::tests::{converted_size, shared_field_schema};
    use crate::embedded_schema::{
        APPARENT_SIZE_MULTIPLE, MAX_ARROW_SCHEMA_BYTES, TABLE_BYTES, ipc_message,
    };
    use crate::reader::{DecodeError, decode_footer as decode_reserved};
    use crate::test_support::{binary_parquet_bytes, footer_len_of, parquet_bytes};

    fn passes(footer: &[u8]) -> Result<(), String> {
        check_footer_shape(footer).map(|_| ())
    }

    /// The reader's `decode_footer` under an unlimited budget.
    fn decode_footer(footer: &[u8], data_end: u64) -> Result<Arc<ParquetMetaData>, String> {
        let budget = Arc::new(MemoryBudget::unlimited());
        decode_reserved(footer, data_end, |bytes| budget.reserve(bytes))
            .map(|decoded| decoded.metadata)
            .map_err(|err| match err {
                DecodeError::Refused(message) => message,
                DecodeError::Reserve(err) => format!("{err:?}"),
            })
    }

    fn footer(bytes: &[u8]) -> &[u8] {
        let end = bytes.len() - 8;
        &bytes[end - footer_len_of(bytes) as usize..end]
    }

    /// A varint of `value`.
    fn uvarint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let b = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(b);
                return out;
            }
            out.push(b | 0x80);
        }
    }

    /// A `FileMetaData` holding only field 10, which the decoder skips, of
    /// header type `code` with `value` after the header, then the stop byte.
    fn skipped_field(code: u8, value: &[u8]) -> Vec<u8> {
        let mut out = vec![code, 20]; // explicit field id 10, zigzag
        out.extend_from_slice(value);
        out.push(T_STOP);
        out
    }

    /// A list header declaring `count` elements of `element`.
    fn list_header(element: u8, count: u64) -> Vec<u8> {
        let mut out = vec![0xf0 | element];
        out.extend(uvarint(count));
        out
    }

    fn write(schema: Schema, columns: Vec<ArrayRef>, properties: WriterProperties) -> Bytes {
        let schema = Arc::new(schema);
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch");
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, schema, Some(properties)).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        Bytes::from(out)
    }

    fn ints(n: i64) -> ArrayRef {
        Arc::new(Int64Array::from((0..n).collect::<Vec<_>>()))
    }

    fn strings(n: i64) -> ArrayRef {
        Arc::new(StringArray::from(
            (0..n).map(|i| format!("row-{i}")).collect::<Vec<_>>(),
        ))
    }

    /// Footers parquet 59.3.0's writer produces, covering every footer
    /// struct it writes: many row groups, nested groups, key-value metadata
    /// beside `ARROW:schema`, page statistics with column and offset
    /// indexes, bloom filters, sorting columns, and a schema of logical
    /// types.
    fn writer_files() -> &'static [(&'static str, Bytes)] {
        static FILES: std::sync::OnceLock<Vec<(&'static str, Bytes)>> = std::sync::OnceLock::new();
        FILES.get_or_init(build_writer_files)
    }

    fn build_writer_files() -> Vec<(&'static str, Bytes)> {
        let mut files = vec![
            ("small", parquet_bytes(&[1, 2, 3], &["x", "y", "z"])),
            ("binary", binary_parquet_bytes(&[b"abc", b"", &[0xff; 300]])),
        ];

        let two = || {
            Schema::new(vec![
                ArrowField::new("a", DataType::Int64, false),
                ArrowField::new("b", DataType::Utf8, true),
            ])
        };
        files.push((
            "many row groups",
            write(
                two(),
                vec![ints(2000), strings(2000)],
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(10))
                    .build(),
            ),
        ));
        files.push((
            "statistics, indexes, bloom filters, sorting columns, key-value metadata",
            write(
                two(),
                vec![ints(500), strings(500)],
                WriterProperties::builder()
                    .set_statistics_enabled(EnabledStatistics::Page)
                    .set_write_page_header_statistics(true)
                    .set_bloom_filter_enabled(true)
                    .set_data_page_row_count_limit(50)
                    .set_write_batch_size(50)
                    .set_sorting_columns(Some(vec![SortingColumn {
                        column_idx: 0,
                        descending: false,
                        nulls_first: true,
                    }]))
                    .set_key_value_metadata(Some(vec![
                        KeyValue::new("origin".to_string(), "ravel".to_string()),
                        KeyValue::new("no-value".to_string(), None),
                    ]))
                    .build(),
            ),
        ));

        // Nested groups: a struct holding a list, and a map.
        let mut tags = ListBuilder::new(StringBuilder::new());
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        for i in 0..20 {
            tags.values().append_value(format!("t{i}"));
            tags.values().append_value("u");
            tags.append(i % 3 != 0);
            map.keys().append_value(format!("k{i}"));
            map.values().append_value(i);
            map.append(true).expect("map row");
        }
        let tags = Arc::new(tags.finish()) as ArrayRef;
        let map = Arc::new(map.finish()) as ArrayRef;
        let inner = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("x", DataType::Int64, false)),
                ints(20),
            ),
            (
                Arc::new(ArrowField::new("tags", tags.data_type().clone(), true)),
                tags,
            ),
        ]);
        let inner = Arc::new(inner) as ArrayRef;
        files.push((
            "nested",
            write(
                Schema::new(vec![
                    ArrowField::new("s", inner.data_type().clone(), false),
                    ArrowField::new("m", map.data_type().clone(), false),
                ]),
                vec![inner, map],
                WriterProperties::builder().build(),
            ),
        ));

        // Logical types.
        let n = 8;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(
                Decimal128Array::from((0..n).map(i128::from).collect::<Vec<_>>())
                    .with_precision_and_scale(20, 3)
                    .expect("decimal"),
            ),
            Arc::new(Date32Array::from((0..n as i32).collect::<Vec<_>>())),
            Arc::new(Time64MicrosecondArray::from((0..n).collect::<Vec<_>>())),
            Arc::new(
                TimestampNanosecondArray::from((0..n).collect::<Vec<_>>()).with_timezone("UTC"),
            ),
            Arc::new(Int8Array::from((0..n as i8).collect::<Vec<_>>())),
            Arc::new(UInt32Array::from((0..n as u32).collect::<Vec<_>>())),
            cast(
                &Float32Array::from((0..n).map(|i| i as f32).collect::<Vec<_>>()),
                &DataType::Float16,
            )
            .expect("float16"),
            Arc::new(Float64Array::from(
                (0..n).map(|i| i as f64).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                (0..n).map(|i| i % 2 == 0).collect::<Vec<_>>(),
            )),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter((0..n).map(|i| [i as u8; 16])).expect("fixed"),
            ),
            strings(n),
        ];
        let fields: Vec<ArrowField> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| ArrowField::new(format!("c{i}"), c.data_type().clone(), false))
            .collect();
        files.push((
            "logical types",
            write(
                Schema::new(fields),
                columns,
                WriterProperties::builder()
                    .set_statistics_enabled(EnabledStatistics::Page)
                    .build(),
            ),
        ));
        files
    }

    /// The walk mirrors parquet 59.3.0's decoder; any other version needs
    /// the field table and the decoder's allocations checked again.
    #[test]
    fn the_parquet_version_the_walk_mirrors_is_the_one_resolved() {
        assert_eq!(
            resolved_parquet(include_str!("../../../Cargo.lock")),
            "59.3.0",
            "ravel-parquet no longer resolves parquet 59.3.0: re-check footer_shape's field \
             table and decode estimate against the new decoder before changing this test"
        );
    }

    /// The parquet version `lock` resolves for ravel-parquet. Its dependency
    /// line names the version only when the lock holds more than one, so
    /// `"parquet",` is read from the lock's one parquet package. Anything
    /// else is returned as found, so it fails the comparison.
    fn resolved_parquet(lock: &str) -> String {
        let packages = |name: &str| {
            let line = format!("\nname = \"{name}\"\n");
            lock.split("[[package]]")
                .filter(move |package| package.contains(&line))
        };
        let dependencies: Vec<&str> = packages("ravel-parquet")
            .flat_map(str::lines)
            .map(str::trim)
            .filter(|line| line.starts_with("\"parquet"))
            .collect();
        match dependencies.as_slice() {
            ["\"parquet\","] => {
                let versions: Vec<&str> = packages("parquet")
                    .filter_map(|package| {
                        package
                            .lines()
                            .find_map(|line| line.strip_prefix("version = \""))
                    })
                    .map(|version| version.trim_end_matches('"'))
                    .collect();
                match versions.as_slice() {
                    [version] => version.to_string(),
                    _ => format!("{versions:?}"),
                }
            }
            [one] => match one
                .strip_prefix("\"parquet ")
                .and_then(|rest| rest.strip_suffix("\","))
            {
                Some(version) => version.to_string(),
                None => one.to_string(),
            },
            _ => format!("{dependencies:?}"),
        }
    }

    /// Guards: `resolved_parquet`, for both spellings of the dependency.
    #[test]
    fn the_resolved_parquet_version_is_read_from_either_spelling() {
        let lock = |dependency: &str, versions: &[&str]| {
            let mut lock = format!(
                "[[package]]\nname = \"ravel-parquet\"\nversion = \"0.22.0\"\ndependencies = \
                 [\n \"bytes\",\n {dependency}\n]\n"
            );
            for version in versions {
                lock.push_str(&format!(
                    "\n[[package]]\nname = \"parquet\"\nversion = \"{version}\"\n"
                ));
            }
            lock
        };
        let two = ["58.4.0", "59.1.0"];
        assert_eq!(
            resolved_parquet(&lock("\"parquet 58.4.0\",", &two)),
            "58.4.0"
        );
        assert_eq!(
            resolved_parquet(&lock("\"parquet 59.1.0\",", &two)),
            "59.1.0"
        );
        assert_eq!(
            resolved_parquet(&lock("\"parquet\",", &["58.4.0"])),
            "58.4.0"
        );
        assert_eq!(
            resolved_parquet(&lock("\"parquet\",", &["58.5.0"])),
            "58.5.0"
        );
        assert_ne!(resolved_parquet(&lock("\"parquet\",", &two)), "58.4.0");
        assert_ne!(resolved_parquet(&lock("", &["58.4.0"])), "58.4.0");
    }

    /// The bounds the estimate uses for types it cannot measure cover what
    /// it can measure of them. `SchemaElement`'s fields, laid out with no
    /// padding but the struct's own alignment, fit in its bound. This sums
    /// the sizes of the fields of a crate-private struct it cannot name, so
    /// it is a lower bound check: it cannot see padding between them.
    #[test]
    fn schema_element_bound_covers_its_fields() {
        use parquet::basic::{ConvertedType, LogicalType, Repetition, Type as Physical};
        let fields = size_of::<Option<LogicalType>>()
            + size_of::<&str>()
            + 5 * size_of::<Option<i32>>()
            + size_of::<Option<Physical>>()
            + size_of::<Option<Repetition>>()
            + size_of::<Option<ConvertedType>>();
        assert!(
            fields.next_multiple_of(8) as u64 <= SCHEMA_ELEMENT_BYTES,
            "{fields}"
        );
        // The geospatial statistics' fields: four f64, four Option<f64>, and
        // an Option<Vec<i32>>.
        let geo = 4 * size_of::<f64>() + 4 * size_of::<Option<f64>>() + size_of::<Vec<i32>>();
        assert!(geo as u64 <= GEO_STATISTICS_BYTES, "{geo}");
    }

    /// The per-chunk footer sizes `MAX_FOOTER_BYTES` is justified by.
    #[test]
    fn writer_footers_hold_the_bytes_per_column_chunk_the_limit_assumes() {
        let per_chunk = |width: usize, groups: usize, statistics| {
            let fields: Vec<ArrowField> = (0..width)
                .map(|i| ArrowField::new(format!("column_{i}"), DataType::Int64, true))
                .collect();
            let columns = (0..width).map(|_| ints(1000)).collect();
            let properties = WriterProperties::builder()
                .set_max_row_group_row_count(Some(1000 / groups))
                .set_statistics_enabled(statistics)
                .build();
            let bytes = write(Schema::new(fields), columns, properties);
            footer(&bytes).len() / (groups * width)
        };
        let bare = per_chunk(50, 100, EnabledStatistics::None);
        let narrow = per_chunk(50, 100, EnabledStatistics::Page);
        let wide = per_chunk(1000, 1, EnabledStatistics::Page);
        assert!((60..=70).contains(&bare), "{bare}");
        assert!((115..=135).contains(&narrow), "{narrow}");
        assert!((215..=230).contains(&wide), "{wide}");
        const { assert!(crate::reader::MAX_FOOTER_BYTES / 230 >= 290_000) };
    }

    /// Guards: the per-row-group charge in `read_list`. One more row group
    /// costs a `RowGroupMetaData` and a `ColumnChunkMetaData` per leaf
    /// column, exactly, whatever its few bytes on the wire.
    #[test]
    fn each_row_group_is_charged_a_column_chunk_per_leaf() {
        let schema = [
            element("schema", Some(3), true),
            element("a", None, false),
            element("b", None, false),
            element("c", None, false),
        ];
        let chunk = fields(&[(2, T_I64, zigzag(4))]);
        let mut columns = list_header(T_STRUCT, 3);
        (0..3).for_each(|_| columns.extend(&chunk));
        let row_group = fields(&[(1, T_LIST, columns)]);
        let one = check_footer_shape(&schema_footer(&schema, std::slice::from_ref(&row_group)))
            .expect("one");
        let two = check_footer_shape(&schema_footer(&schema, &[row_group.clone(), row_group]))
            .expect("two");
        assert_eq!(two - one, ROW_GROUP_BYTES + 3 * COLUMN_CHUNK_BYTES);
        assert_eq!(COLUMN_CHUNK_BYTES, size_of::<ColumnChunkMetaData>() as u64);
    }

    /// Guards: the path charge in `SchemaTree::add`. The decoder copies every
    /// ancestor's name into each leaf's path, so lengthening a group's name
    /// by ten bytes costs ten bytes per leaf below it, beside the copies of
    /// the name itself.
    #[test]
    fn each_leaf_is_charged_its_copied_path() {
        let named = |group: &str| {
            schema_footer(
                &[
                    element("schema", Some(1), true),
                    element(group, Some(2), false),
                    element("a", None, false),
                    element("b", None, false),
                ],
                &[],
            )
        };
        let short = check_footer_shape(&named("g")).expect("short");
        let long = check_footer_shape(&named("g0123456789")).expect("long");
        assert_eq!(long - short, 10 * 3 + 2 * 10);
    }

    /// parquet 59.3.0 checks that an INT96 column's statistics are exactly
    /// 12 bytes and returns an error otherwise, so a binary column retyped as
    /// INT96, its statistics 13 and 20 bytes long, passes the walk and is
    /// refused by the decoder with a typed error rather than a panic.
    #[test]
    fn an_int96_statistic_of_the_wrong_length_is_refused_by_the_decoder() {
        let bytes = binary_parquet_bytes(&[&[1; 13], &[2; 20]]);
        let mut shape = footer(&bytes).to_vec();
        // Column c: type BYTE_ARRAY, repetition REQUIRED, name "c".
        let leaf = [0x15, 0x0c, 0x25, 0x00, 0x18, 0x01, b'c'];
        let at = shape
            .windows(leaf.len())
            .position(|window| window == leaf)
            .expect("column c");
        shape[at + 1] = 0x06; // INT96
        assert_eq!(passes(&shape), Ok(()));
        assert_eq!(
            decode_footer(&shape, u64::MAX).expect_err("refused"),
            "footer: Parquet error: Incorrect Int96 min statistics"
        );
    }

    /// Guards: the `MAX_ROW_GROUPS` check in `read_list`, the decoder's own
    /// limit on how many row groups it numbers.
    #[test]
    fn more_row_groups_than_the_decoder_numbers_are_refused() {
        let schema = [element("schema", Some(1), true), element("a", None, false)];
        let groups = |count: u64| {
            let mut footer = schema_footer(&schema, &[]);
            // Replace the empty row group list and the stop byte after it.
            footer.truncate(footer.len() - 3);
            footer.extend(list_header(T_STRUCT, count));
            footer.extend(std::iter::repeat_n(T_STOP, count as usize + 1));
            footer
        };
        assert!(check_footer_shape(&groups(MAX_ROW_GROUPS)).is_ok());
        assert_eq!(
            check_footer_shape(&groups(MAX_ROW_GROUPS + 1)).expect_err("refused"),
            "footer list at byte 27 declares 32769 row groups, more than the decoder's 32768"
        );
    }

    /// Every footer the writer produces passes the walk and decodes, with
    /// every check `decode_footer` runs, and its decoded metadata fits in
    /// the estimate the walk returns.
    #[test]
    fn every_footer_the_writer_produces_passes_and_decodes() {
        for (name, bytes) in writer_files() {
            let shape = footer(bytes);
            let estimate = check_footer_shape(shape).unwrap_or_else(|err| panic!("{name}: {err}"));
            let data_end = (bytes.len() - 8 - shape.len()) as u64;
            let metadata =
                decode_footer(shape, data_end).unwrap_or_else(|err| panic!("{name}: {err}"));
            let decoded = ParquetMetaDataReader::decode_metadata(shape).expect("decodes");
            assert!(
                decoded.memory_size() as u64 <= estimate,
                "{name}: {} > {estimate}",
                decoded.memory_size()
            );
            assert_eq!(metadata.num_row_groups(), decoded.num_row_groups());
        }
    }

    /// The `ARROW:schema` value `footer` carries.
    fn arrow_schema_value(footer: &[u8]) -> String {
        ParquetMetaDataReader::decode_metadata(footer)
            .expect("footer")
            .file_metadata()
            .key_value_metadata()
            .and_then(|kv| kv.iter().find(|kv| kv.key == "ARROW:schema"))
            .and_then(|kv| kv.value.clone())
            .expect("ArrowWriter embeds its schema")
    }

    /// The `ARROW:schema` values of the writer files, and the schemas
    /// Arrow's encoder writes for wide schemas, for the densest one it
    /// writes (unnamed Null fields), and for one of every kind the
    /// conversion counts: field and schema metadata, a dictionary, a
    /// timezone, a union and nested types.
    fn writer_schemas() -> Vec<(String, String)> {
        let mut schemas: Vec<(String, String)> = writer_files()
            .iter()
            .map(|(name, bytes)| (name.to_string(), arrow_schema_value(footer(bytes))))
            .collect();
        let wide = Schema::new(
            (0..1000)
                .map(|i| ArrowField::new(format!("column_{i:04}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        );
        schemas.push((
            "1,000 Int64 columns".to_string(),
            encode_arrow_schema(&wide),
        ));
        let nulls = Schema::new(
            (0..1000)
                .map(|_| ArrowField::new("", DataType::Null, true))
                .collect::<Vec<_>>(),
        );
        schemas.push((
            "1,000 unnamed Nulls".to_string(),
            encode_arrow_schema(&nulls),
        ));
        let tagged = |field: ArrowField| {
            field.with_metadata(HashMap::from([("tag".to_string(), "value".to_string())]))
        };
        let union = UnionFields::try_new(
            [0, 1],
            [
                ArrowField::new("i", DataType::Int32, true),
                ArrowField::new("s", DataType::Utf8, true),
            ],
        )
        .expect("union");
        let every = Schema::new_with_metadata(
            vec![
                tagged(ArrowField::new_dictionary(
                    "d",
                    DataType::Int32,
                    DataType::Utf8,
                    true,
                )),
                ArrowField::new(
                    "t",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("Europe/Athens".into())),
                    true,
                ),
                ArrowField::new("u", DataType::Union(union, UnionMode::Dense), true),
                tagged(ArrowField::new_list(
                    "l",
                    tagged(ArrowField::new_list_field(DataType::Int64, true)),
                    true,
                )),
                ArrowField::new_struct(
                    "s",
                    vec![
                        ArrowField::new("x", DataType::Float64, false),
                        ArrowField::new("y", DataType::Binary, true),
                    ],
                    true,
                ),
            ],
            HashMap::from([("origin".to_string(), "ravel".to_string())]),
        );
        schemas.push(("every kind".to_string(), encode_arrow_schema(&every)));
        schemas
    }

    /// The least apparent size and table count `message` verifies with.
    fn verified_size(message: &[u8]) -> (usize, usize) {
        let verifies = |max_tables, max_apparent_size| {
            let options = VerifierOptions {
                max_tables,
                max_apparent_size,
                ..VerifierOptions::default()
            };
            root_as_message_with_opts(&options, message).is_ok()
        };
        let least = |verifies: &dyn Fn(usize) -> bool| {
            let (mut low, mut high) = (0usize, 1usize << 32);
            while low < high {
                let mid = low + (high - low) / 2;
                if verifies(mid) {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }
            low
        };
        (
            least(&|n| verifies(usize::MAX, n)),
            least(&|n| verifies(n, usize::MAX)),
        )
    }

    /// The measurements `embedded_schema`'s bounds are justified by: every
    /// schema the writer produces has an apparent size of at most 1.85 times
    /// its length and at most a table per 20 bytes, half of each bound or
    /// less, and each one's charge covers what converting it holds.
    #[test]
    fn writer_schemas_verify_within_the_bounds() {
        for (name, value) in writer_schemas() {
            let decoded = BASE64_STANDARD.decode(&value).expect("base64");
            let message = ipc_message(&decoded);
            let (apparent, tables) = verified_size(message);
            let len = message.len();
            assert!(
                100 * apparent <= 185 * len,
                "{name}: apparent size {apparent} of {len} bytes"
            );
            assert!(20 * tables <= len, "{name}: {tables} tables in {len} bytes");
            const { assert!(2 * 185 <= 100 * APPARENT_SIZE_MULTIPLE && 2 * TABLE_BYTES <= 20) };
            let charge = check_arrow_schema_value(value.as_bytes()).expect(&name);
            assert!(converted_size(value.as_bytes()) <= charge, "{name}");
        }
    }

    /// The length cap holds a 10,000-column schema, about 75 base64 bytes a
    /// column, with room to spare.
    #[test]
    fn a_wide_writer_schema_fits_the_length_cap() {
        let wide = Schema::new(
            (0..10_000)
                .map(|i| ArrowField::new(format!("column_{i:04}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        );
        let len = encode_arrow_schema(&wide).len();
        assert!((700_000..=800_000).contains(&len), "{len}");
        assert!(4 * len <= MAX_ARROW_SCHEMA_BYTES, "{len}");
    }

    /// A footer of one Int64 column whose key-value metadata is `entries`.
    fn key_value_footer(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
        let schema = [element("schema", Some(1), true), element("a", None, false)];
        let mut footer = schema_footer(&schema, &[]);
        footer.pop(); // the stop byte
        footer.push(0x10 | T_LIST); // field 5, after field 4
        footer.extend(list_header(T_STRUCT, entries.len() as u64));
        for (key, value) in entries {
            let binary = |bytes: &[u8]| {
                let mut out = uvarint(bytes.len() as u64);
                out.extend(bytes);
                out
            };
            footer.extend(fields(&[
                (1, T_BINARY, binary(key)),
                (2, T_BINARY, binary(value)),
            ]));
        }
        footer.push(T_STOP);
        footer
    }

    /// Guards: the `check_key_value` call in `read_struct`, before the
    /// footer is decoded. The review's value, a thousand fields naming one
    /// long string, is refused, as is a value past the length cap; under
    /// another key the same value is not interpreted and passes.
    #[test]
    fn an_arrow_schema_the_bounds_refuse_refuses_the_footer() {
        let shared = BASE64_STANDARD.encode(shared_field_schema(1000, 64 << 10, true, 0));
        let refused = decode_footer(
            &key_value_footer(&[(ARROW_SCHEMA_KEY, shared.as_bytes())]),
            0,
        )
        .expect_err("refused");
        assert!(
            refused.starts_with("footer ARROW:schema does not verify within the bounds of its ")
                && refused.ends_with(" bytes: Apparent size too large."),
            "{refused}"
        );
        let long = vec![b'A'; MAX_ARROW_SCHEMA_BYTES + 1];
        assert_eq!(
            decode_footer(&key_value_footer(&[(ARROW_SCHEMA_KEY, &long)]), 0).expect_err("refused"),
            "footer ARROW:schema value is 4194305 bytes, longer than the 4194304-byte limit"
        );
        let other = key_value_footer(&[(b"ARROW:schemx", shared.as_bytes())]);
        assert!(check_footer_shape(&other).is_ok());
        // Every ARROW:schema entry is checked, not only the first.
        let valid = arrow_schema_value(footer(&writer_files()[0].1));
        let twice = key_value_footer(&[
            (ARROW_SCHEMA_KEY, valid.as_bytes()),
            (ARROW_SCHEMA_KEY, shared.as_bytes()),
        ]);
        assert!(check_footer_shape(&twice).is_err());
    }

    /// Guards: the `charge` call in `check_key_value`. A writer footer's
    /// estimate exceeds the same footer's with its `ARROW:schema` key
    /// renamed by exactly what converting the schema is charged.
    #[test]
    fn the_estimate_includes_the_arrow_schema() {
        let shape = footer(&writer_files()[0].1);
        let value = arrow_schema_value(shape);
        let at = shape
            .windows(ARROW_SCHEMA_KEY.len())
            .position(|window| window == ARROW_SCHEMA_KEY)
            .expect("the key's bytes are in the footer");
        let mut renamed = shape.to_vec();
        renamed[at + ARROW_SCHEMA_KEY.len() - 1] = b'x';
        let with = check_footer_shape(shape).expect("with");
        let without = check_footer_shape(&renamed).expect("without");
        let converted = check_arrow_schema_value(value.as_bytes()).expect("passes");
        assert_eq!(with - without, converted);
        assert!(converted > 0);
    }

    /// Guards: the header type check in `read_struct`. The review's footer:
    /// field 5 with header type i32, which a walk trusting the header skips
    /// as a varint, while the decoder reads field 5 of `FileMetaData` as a
    /// list of `KeyValue` and sizes it from the varint.
    #[test]
    fn a_known_field_with_another_header_type_is_refused() {
        let footer = [0x55, 0xfc, 0xff, 0xff, 0xff, 0xff, 0x07, 0x00];
        assert_eq!(
            decode_footer(&footer, 0).expect_err("refused"),
            "footer field 5 of FileMetaData at byte 0 has compact type 5, where the decoder \
             reads List(Struct(KeyValue))"
        );
    }

    /// Guards: the same check for a boolean header type. A walk trusting
    /// the header reads nothing for field 5 and realigns on the next byte
    /// as a field header (field 20, an i32), so it accepts this footer;
    /// the decoder reads that byte as the header of field 5's list,
    /// declaring i32::MAX elements.
    #[test]
    fn a_lone_bool_header_on_a_list_field_is_refused() {
        let footer = [0x51, 0xf5, 0xff, 0xff, 0xff, 0xff, 0x07, 0x00];
        assert_eq!(
            check_footer_shape(&footer).expect_err("refused"),
            "footer field 5 of FileMetaData at byte 0 has compact type 1, where the decoder \
             reads List(Struct(KeyValue))"
        );
    }

    /// Guards: the element type check in `read_list`. The decoder reads a
    /// known list's elements as its own type whatever the header says.
    #[test]
    fn a_known_list_with_another_element_type_is_refused() {
        let mut footer = vec![0x59];
        footer.extend(list_header(T_I32, 2));
        footer.extend([0, 0, T_STOP]);
        assert_eq!(
            check_footer_shape(&footer).expect_err("refused"),
            "footer list at byte 1 in field 5 of FileMetaData has element type 5, where the \
             decoder reads Struct(KeyValue)"
        );
    }

    /// Guards: the boolean element check in `skip`. The review's footer:
    /// field 10, which the decoder skips, is a list of eight booleans,
    /// which the decoder skips without reading a byte, so it then reads
    /// the bytes a walk skipped as booleans as field 5, a list declaring
    /// i32::MAX elements.
    #[test]
    fn a_skipped_list_of_booleans_is_refused() {
        let footer = [
            0x09, 0x14, 0x81, 0x09, 0x0a, 0xfc, 0xff, 0xff, 0xff, 0xff, 0x07, 0x00,
        ];
        assert_eq!(
            decode_footer(&footer, 0).expect_err("refused"),
            "footer list at byte 2 holds booleans in a field the decoder skips"
        );
    }

    /// Guards: the count check in `skip` for a list of booleans, which the
    /// decoder's skip loops over once per declared element; a list whose
    /// count fits is refused for its element type instead.
    #[test]
    fn a_bool_list_declaring_more_elements_than_bytes_is_refused() {
        let shape = skipped_field(T_LIST, &list_header(T_BOOL_TRUE, 1 << 30));
        assert_eq!(
            check_footer_shape(&shape).expect_err("refused"),
            "footer list at byte 8 declares 1073741824 elements with 1 bytes left"
        );
        let mut value = list_header(T_BOOL_TRUE, 2);
        value.extend([1, 2]);
        assert_eq!(
            check_footer_shape(&skipped_field(T_LIST, &value)).expect_err("refused"),
            "footer list at byte 2 holds booleans in a field the decoder skips"
        );
    }

    /// Guards: the count check in `skip` for a map; a map whose count fits
    /// is refused because the decoder cannot skip one.
    #[test]
    fn a_map_declaring_more_entries_than_bytes_is_refused() {
        let mut value = uvarint(1 << 30);
        value.push((T_I32 << 4) | T_I32);
        assert_eq!(
            check_footer_shape(&skipped_field(T_MAP, &value)).expect_err("refused"),
            "footer map at byte 8 declares 1073741824 elements with 1 bytes left"
        );
        let value = [2, (T_I32 << 4) | T_I32, 0, 0, 0, 0];
        assert_eq!(
            check_footer_shape(&skipped_field(T_MAP, &value)).expect_err("refused"),
            "footer set or map at byte 2 is in a field the decoder skips"
        );
    }

    /// Guards: the `count_fits` call in `read_list`. Without it the walk
    /// accepts a footer declaring two billion key-value pairs, which the
    /// decoder sizes an allocation from.
    #[test]
    fn a_list_declaring_more_elements_than_bytes_is_refused() {
        let mut shape = vec![0x59];
        shape.extend(list_header(T_STRUCT, i32::MAX as u64));
        shape.push(T_STOP);
        assert_eq!(
            check_footer_shape(&shape).expect_err("refused"),
            "footer list at byte 7 declares 2147483647 elements with 1 bytes left"
        );
        // The same check in a skipped list.
        let shape = skipped_field(T_LIST, &list_header(T_STRUCT, i32::MAX as u64));
        assert_eq!(
            check_footer_shape(&shape).expect_err("refused"),
            "footer list at byte 8 declares 2147483647 elements with 1 bytes left"
        );
        // A count of real one-byte elements is accepted.
        let mut value = list_header(T_STRUCT, 3);
        value.extend([T_STOP, T_STOP, T_STOP]);
        assert_eq!(passes(&skipped_field(T_LIST, &value)), Ok(()));
    }

    /// Guards: the `0x00` case in `list_header`. The decoder reads that
    /// byte as an empty list of element type byte: a skipped field carrying
    /// one decodes, and a known list carrying one fails the decoder's own
    /// element type check, which the walk leaves to the decoder.
    #[test]
    fn a_zero_byte_list_header_is_an_empty_list() {
        let head = [
            0x15, 0x02, // version 1
            0x19, 0x2c, // schema: two elements
            0x48, 0x06, b's', b'c', b'h', b'e', b'm', b'a', 0x15, 0x02, 0x00, // root, 1 child
            0x15, 0x04, 0x25, 0x00, 0x18, 0x01, b'a', 0x00, // a: required INT64
            0x16, 0x00, // num_rows 0
        ];
        let skipped = [
            head.as_slice(),
            &[0x19, 0x0c],     // row_groups: an empty list of structs
            &[0x09, 20, 0x00], // field 10, skipped: the 0x00 empty list
            &[0x00],
        ]
        .concat();
        assert_eq!(passes(&skipped), Ok(()));
        let metadata = decode_footer(&skipped, 0).expect("decodes");
        assert_eq!(metadata.num_row_groups(), 0);
        assert_eq!(metadata.file_metadata().schema_descr().num_columns(), 1);

        let known = [head.as_slice(), &[0x19, 0x00], &[0x00]].concat();
        assert_eq!(passes(&known), Ok(()));
        assert_eq!(
            decode_footer(&known, 0).expect_err("refused"),
            "footer: Parquet error: Expected list element type of Struct but got Byte"
        );
    }

    /// A struct of `(id, header type, value)` fields, then the stop byte.
    fn fields(fields: &[(i16, u8, Vec<u8>)]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut last = 0;
        for (id, code, value) in fields {
            out.push((((id - last) as u8) << 4) | code);
            out.extend(value);
            last = *id;
        }
        out.push(T_STOP);
        out
    }

    fn zigzag(value: i64) -> Vec<u8> {
        uvarint(((value << 1) ^ (value >> 63)) as u64)
    }

    /// A `SchemaElement` named `name`: a required INT64 column when
    /// `children` is `None`, else a group (required unless it is the root).
    fn element(name: &str, children: Option<i32>, root: bool) -> Vec<u8> {
        let mut out = Vec::new();
        if children.is_none() {
            out.push((1, T_I32, zigzag(2)));
        }
        if !root {
            out.push((3, T_I32, zigzag(0)));
        }
        let mut bytes = uvarint(name.len() as u64);
        bytes.extend(name.as_bytes());
        out.push((4, T_BINARY, bytes));
        if let Some(n) = children {
            out.push((5, T_I32, zigzag(i64::from(n))));
        }
        fields(&out)
    }

    /// A footer of `schema` and `row_groups`, each a list of encoded structs.
    fn schema_footer(schema: &[Vec<u8>], row_groups: &[Vec<u8>]) -> Vec<u8> {
        let list = |items: &[Vec<u8>]| {
            let mut out = list_header(T_STRUCT, items.len() as u64);
            items.iter().for_each(|item| out.extend(item));
            out
        };
        fields(&[
            (1, T_I32, zigzag(1)),
            (2, T_LIST, list(schema)),
            (3, T_I64, zigzag(0)),
            (4, T_LIST, list(row_groups)),
        ])
    }

    /// A schema of `groups` nested groups, the root first, each with one
    /// child, around one column.
    fn chain(groups: usize) -> Vec<Vec<u8>> {
        let mut schema = vec![element("schema", Some(1), true)];
        for level in 1..groups {
            schema.push(element(&format!("g{level}"), Some(1), false));
        }
        schema.push(element("leaf", None, false));
        schema
    }

    /// Guards: the `children > after` check in `SchemaTree::add`. The
    /// decoder sizes a group's child list from `num_children` before it
    /// reads one child.
    #[test]
    fn a_schema_element_declaring_more_children_than_elements_is_refused() {
        let footer = schema_footer(&[element("schema", Some(i32::MAX), true)], &[]);
        assert_eq!(
            check_footer_shape(&footer).expect_err("refused"),
            "footer schema element 0 declares 2147483647 children with 0 elements after it"
        );
        let footer = schema_footer(
            &[element("schema", Some(2), true), element("a", None, false)],
            &[],
        );
        assert_eq!(
            check_footer_shape(&footer).expect_err("refused"),
            "footer schema element 0 declares 2 children with 1 elements after it"
        );
        let footer = schema_footer(&[element("schema", Some(-1), true)], &[]);
        assert_eq!(
            check_footer_shape(&footer).expect_err("refused"),
            "footer schema element 0 declares -1 children"
        );
    }

    /// Guards: `SchemaTree::finish` and the second-root check in
    /// `SchemaTree::add`: the elements must be exactly the one tree their
    /// `num_children` values describe.
    #[test]
    fn a_schema_that_is_not_one_tree_is_refused() {
        let footer = schema_footer(
            &[
                element("schema", Some(2), true),
                element("g", Some(1), false),
                element("a", None, false),
            ],
            &[],
        );
        assert_eq!(
            check_footer_shape(&footer).expect_err("refused"),
            "footer schema's num_children describe more than its 3 elements"
        );
        let footer = schema_footer(
            &[
                element("schema", Some(1), true),
                element("a", None, false),
                element("b", None, false),
            ],
            &[],
        );
        assert_eq!(
            check_footer_shape(&footer).expect_err("refused"),
            "footer schema element 2 starts a second root"
        );
        assert_eq!(
            check_footer_shape(&schema_footer(&[], &[])).expect_err("refused"),
            "footer schema has no elements"
        );
    }

    /// A nested schema the decoder builds passes, and decodes.
    #[test]
    fn a_nested_schema_passes() {
        let footer = schema_footer(
            &[
                element("schema", Some(2), true),
                element("g", Some(2), false),
                element("a", None, false),
                element("b", None, false),
                element("c", None, false),
            ],
            &[],
        );
        let metadata = decode_footer(&footer, 0).expect("decodes");
        assert_eq!(metadata.file_metadata().schema_descr().num_columns(), 3);
    }

    /// Guards: the depth check in `SchemaTree::add`. The decoder recurses
    /// once per level of the schema, with no bound of its own.
    #[test]
    fn a_schema_deeper_than_the_bound_is_refused() {
        let footer = schema_footer(&chain(MAX_SCHEMA_DEPTH), &[]);
        let metadata = decode_footer(&footer, 0).expect("decodes");
        assert_eq!(metadata.file_metadata().schema_descr().num_columns(), 1);
        let footer = schema_footer(&chain(MAX_SCHEMA_DEPTH + 1), &[]);
        assert_eq!(
            check_footer_shape(&footer).expect_err("refused"),
            "footer schema nests deeper than 64 levels at element 64"
        );
    }

    /// Guards: the row group checks in `read_list`, which the decoder makes
    /// too: a row group has one column chunk per leaf column of the schema
    /// before it, and there is a schema before it.
    #[test]
    fn row_groups_must_follow_a_schema_and_match_its_columns() {
        let chunk = fields(&[(2, T_I64, zigzag(4))]);
        let mut columns = list_header(T_STRUCT, 2);
        columns.extend(&chunk);
        columns.extend(&chunk);
        let row_group = fields(&[(1, T_LIST, columns)]);
        let schema = [element("schema", Some(1), true), element("a", None, false)];
        assert_eq!(
            check_footer_shape(&schema_footer(&schema, &[row_group])).expect_err("refused"),
            "footer row group at byte 30 has 2 column chunks for 1 columns"
        );
        assert_eq!(
            check_footer_shape(&[0x49, 0x0c, 0x00]).expect_err("refused"),
            "footer row groups at byte 1 come before the schema"
        );
    }

    /// Guards: the repeated field check in `read_struct`. The review's
    /// footer: a second schema list after the row groups, which the decoder
    /// skips, keeping the first schema, while a walk reading it checks later
    /// row groups against the second.
    #[test]
    fn a_second_schema_after_the_row_groups_is_refused() {
        let first = [element("schema", Some(1), true), element("a", None, false)];
        let second = [
            element("schema", Some(2), true),
            element("a", None, false),
            element("b", None, false),
        ];
        let mut footer = schema_footer(&first, &[]);
        footer.pop(); // the stop byte
        footer.push(T_LIST); // field 2, its id written out after field 4
        footer.extend(zigzag(2));
        footer.extend(list_header(T_STRUCT, second.len() as u64));
        second.iter().for_each(|element| footer.extend(element));
        footer.push(T_STOP);
        let decoded = ParquetMetaDataReader::decode_metadata(&footer).expect("decodes");
        assert_eq!(decoded.file_metadata().schema_descr().num_columns(), 1);
        assert_eq!(
            check_footer_shape(&footer).expect_err("refused"),
            "footer field 2 of FileMetaData at byte 29 repeats a field read before"
        );
    }

    /// Guards: the same check inside a row group, where the decoder appends
    /// a second column chunk list to the first, so the row group holds two
    /// column chunks per leaf where the estimate charges one.
    #[test]
    fn a_row_group_repeating_its_column_chunks_is_refused() {
        let schema = [element("schema", Some(1), true), element("a", None, false)];
        // A column chunk with the column metadata the decoder requires: an
        // INT64 column `a`, PLAIN, uncompressed, at offset 4.
        let mut encodings = list_header(T_I32, 1);
        encodings.extend(zigzag(0));
        let mut path = list_header(T_BINARY, 1);
        path.extend([1, b'a']);
        let meta = fields(&[
            (1, T_I32, zigzag(2)),
            (2, T_LIST, encodings),
            (3, T_LIST, path),
            (4, T_I32, zigzag(0)),
            (5, T_I64, zigzag(0)),
            (6, T_I64, zigzag(0)),
            (7, T_I64, zigzag(0)),
            (9, T_I64, zigzag(4)),
        ]);
        let chunk = fields(&[(2, T_I64, zigzag(4)), (3, T_STRUCT, meta)]);
        let mut columns = list_header(T_STRUCT, 1);
        columns.extend(&chunk);
        let mut row_group = vec![0x10 | T_LIST];
        row_group.extend(&columns);
        row_group.push(T_LIST); // field 1 again, its id written out
        row_group.extend(zigzag(1));
        row_group.extend(&columns);
        row_group.extend([0x10 | T_I64, 0, 0x10 | T_I64, 0, T_STOP]);
        let footer = schema_footer(&schema, &[row_group]);
        let decoded = ParquetMetaDataReader::decode_metadata(&footer).expect("decodes");
        assert_eq!(decoded.row_group(0).columns().len(), 2);
        assert_eq!(
            check_footer_shape(&footer).expect_err("refused"),
            "footer field 1 of RowGroup at byte 58 repeats a field read before"
        );
    }

    /// Every id the decoder reads fits the walk's 64-bit record of the
    /// fields read in a struct.
    #[test]
    fn every_field_the_decoder_reads_has_an_id_below_64() {
        use Shape as S;
        let shapes = [
            S::FileMetaData,
            S::SchemaElement,
            S::LogicalType,
            S::DecimalType,
            S::TimeType,
            S::IntType,
            S::VariantType,
            S::GeometryType,
            S::GeographyType,
            S::TimeUnit,
            S::RowGroup,
            S::ColumnChunk,
            S::ColumnMetaData,
            S::Statistics,
            S::PageEncodingStats,
            S::SizeStatistics,
            S::GeospatialStatistics,
            S::BoundingBox,
            S::SortingColumn,
            S::KeyValue,
            S::ColumnOrder,
        ];
        for shape in shapes {
            for id in (i16::MIN..0).chain(64..=i16::MAX) {
                assert!(
                    !matches!(field(shape, id), Field::Read(_)),
                    "{shape:?} {id}"
                );
            }
        }
    }

    /// Guards: the depth check in `skip`, which the decoder applies at the
    /// same depth.
    #[test]
    fn skipped_nesting_past_the_decoders_bound_is_refused() {
        let nest = |levels: usize| {
            let mut value = vec![0x10 | T_STRUCT; levels - 1];
            value.extend(std::iter::repeat_n(T_STOP, levels));
            skipped_field(T_STRUCT, &value)
        };
        let deepest = nest(usize::from(SKIP_DEPTH));
        assert_eq!(passes(&deepest), Ok(()));
        let decoded = ParquetMetaDataReader::decode_metadata(&deepest).expect_err("no version");
        assert!(
            decoded.to_string().contains("version is missing"),
            "{decoded}"
        );
        let deeper = nest(usize::from(SKIP_DEPTH) + 1);
        assert_eq!(
            check_footer_shape(&deeper).expect_err("refused"),
            "footer nests deeper than 64 levels at byte 66"
        );
        let decoded = ParquetMetaDataReader::decode_metadata(&deeper).expect_err("too deep");
        assert!(
            decoded.to_string().contains("cannot parse past"),
            "{decoded}"
        );
        assert!(check_footer_shape(&nest(100_000)).is_err());
    }

    #[test]
    fn a_binary_longer_than_the_footer_is_refused() {
        let shape = skipped_field(T_BINARY, &uvarint(1 << 40));
        assert_eq!(
            check_footer_shape(&shape).expect_err("refused"),
            "footer declares 1099511627776 bytes at byte 8 with 1 left"
        );
    }

    #[test]
    fn malformed_encodings_are_refused() {
        let mut value = vec![0xff; 11];
        value.push(0);
        assert_eq!(
            check_footer_shape(&skipped_field(T_I64, &value)).expect_err("refused"),
            "footer varint at byte 2 runs past 10 bytes"
        );
        assert_eq!(
            check_footer_shape(&[0x1d, T_STOP]).expect_err("refused"),
            "footer has unknown compact type 13 at byte 0"
        );
        assert_eq!(
            check_footer_shape(&[0x15, 0]).expect_err("refused"),
            "footer ends at byte 2 inside a value"
        );
        assert_eq!(
            check_footer_shape(&[]).expect_err("refused"),
            "footer ends at byte 0 inside a value"
        );
        assert_eq!(
            check_footer_shape(&skipped_field(T_LIST, &[0x10])).expect_err("refused"),
            "footer list at byte 2 has element type 0"
        );
        assert_eq!(
            check_footer_shape(&skipped_field(T_SET, &[0x15, 0])).expect_err("refused"),
            "footer set or map at byte 2 is in a field the decoder skips"
        );
        assert_eq!(
            check_footer_shape(&[0x89, 0x00]).expect_err("refused"),
            "footer field 8 of FileMetaData at byte 0 carries encryption metadata"
        );
    }

    /// The unions: one field, then the stop byte; an empty variant is the
    /// single byte `0x00`.
    #[test]
    fn union_rules_are_the_decoders() {
        // FileMetaData field 7, one ColumnOrder.
        let orders = |order: &[u8]| {
            let mut out = vec![0x79, 0x1c];
            out.extend_from_slice(order);
            out.push(T_STOP);
            out
        };
        assert_eq!(passes(&orders(&[0x1c, 0x00, 0x00])), Ok(()));
        assert_eq!(
            check_footer_shape(&orders(&[0x00])).expect_err("refused"),
            "footer ColumnOrder at byte 2 has no field"
        );
        assert_eq!(
            check_footer_shape(&orders(&[0x1c, 0x00, 0x1c, 0x00, 0x00])).expect_err("refused"),
            "footer ColumnOrder at byte 2 has more than one field"
        );
        assert_eq!(
            check_footer_shape(&orders(&[0x1c, 0x10, 0x00])).expect_err("refused"),
            "footer field 1 of ColumnOrder at byte 3 is not an empty struct"
        );
        // An unknown variant is skipped by its header type.
        assert_eq!(passes(&orders(&[0x25, 0x07, 0x00])), Ok(()));
    }

    #[test]
    fn explicit_field_ids_and_every_scalar_kind_walk() {
        let mut value = vec![T_I32];
        value.extend(uvarint(200)); // explicit zigzag field id
        value.push(0x02);
        value.extend([0x10 | T_BOOL_TRUE, 0x10 | T_BOOL_FALSE]);
        value.extend([0x10 | T_BYTE, 7]);
        value.extend([0x10 | T_I16, 3]);
        value.extend([0x10 | T_I64, 0x80, 0x01]);
        value.push(0x10 | T_DOUBLE);
        value.extend([0; 8]);
        value.extend([0x10 | T_BINARY, 2, b'h', b'i']);
        value.extend([0x10 | T_LIST, 0x25, 1, 2]);
        value.push(T_STOP);
        assert_eq!(passes(&skipped_field(T_STRUCT, &value)), Ok(()));
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = check_footer_shape(&bytes);
        }

        /// A writer footer with one or several bytes replaced never panics
        /// the walk, and every one the walk accepts `decode_footer` decodes
        /// or refuses. One it decodes holds no more than the walk's estimate:
        /// the decoder's peak allocation cannot be observed here (a counting
        /// allocator needs `unsafe`), its retained `memory_size` can.
        #[test]
        fn a_mutated_writer_footer_never_panics(
            file in any::<proptest::sample::Index>(),
            edits in proptest::collection::vec(
                (any::<proptest::sample::Index>(), any::<u8>()),
                1..4,
            ),
        ) {
            let files = writer_files();
            let bytes = &files[file.index(files.len())].1;
            let mut shape = footer(bytes).to_vec();
            for (at, to) in edits {
                let i = at.index(shape.len());
                shape[i] = to;
            }
            if let Ok(estimate) = check_footer_shape(&shape)
                && let Ok(metadata) = decode_footer(&shape, u64::MAX)
            {
                prop_assert!(metadata.memory_size() as u64 <= estimate);
            }
        }
    }
}
