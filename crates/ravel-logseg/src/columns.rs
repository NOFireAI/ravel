//! The column set a scan needs decoded (ADR-0087 decision 3).
//!
//! [`crate::block::read_block_columns`] takes raw column ids, but a caller
//! outside this crate cannot know a dynamic attribute's column id: ids are
//! assigned per object by its FIELD_DIR. [`ColumnSelection`] is the caller-facing
//! form -- fixed columns by name, dynamic columns by *attribute* name -- which
//! [`RlogReader`](crate::RlogReader) resolves against the object it opened.
//!
//! Two columns are always decoded, whatever the selection says: `ts` and
//! `stream_ref`. Every predicate evaluation path and every rebuilt record needs
//! them (`ts` for the exact ts-range re-check, `stream_ref` to resolve a record
//! back to its stream identity and its resource/scope attributes), so naming
//! them would be noise and forgetting them would be a bug.
//!
//! A record's resource and scope attributes are *not* a block column at all:
//! they live in STREAM_DIR and are reached through `stream_ref`. They therefore
//! cost nothing to keep and are always present in a rebuilt record, however
//! narrow the selection.

use std::collections::{BTreeSet, HashSet};

use crate::field_dir::FieldDir;
use crate::record::{
    COL_ATTRS_RAW, COL_BODY, COL_FLAGS, COL_OBSERVED_TS, COL_SEVERITY_NUM, COL_SEVERITY_TEXT,
    COL_SPAN_ID, COL_STREAM_REF, COL_TRACE_ID, COL_TS,
};

/// Which columns a scan needs decoded from each block.
///
/// Build one with [`ColumnSelection::all`] (today's behavior: decode
/// everything) or [`ColumnSelection::fixed_only`] plus the `with_*` builders.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSelection {
    /// Decode every column in the block, ignoring every other field here.
    all: bool,
    /// Decode every dynamic attribute column plus the `attrs_raw` overflow.
    all_attrs: bool,
    /// Explicitly requested fixed column ids (`ts`/`stream_ref` are implicit).
    fixed: BTreeSet<u32>,
    /// Requested dynamic attribute names, resolved per object against FIELD_DIR.
    attrs: BTreeSet<String>,
}

impl ColumnSelection {
    /// Decode every column, exactly as the reader did before column projection
    /// existed. This is what compaction's ranged reader and
    /// [`RlogReader::scan_pruned`](crate::RlogReader::scan_pruned) use.
    #[must_use]
    pub fn all() -> Self {
        ColumnSelection {
            all: true,
            all_attrs: false,
            fixed: BTreeSet::new(),
            attrs: BTreeSet::new(),
        }
    }

    /// The narrowest selection: `ts` and `stream_ref` only. Add what the query
    /// needs with the `with_*` builders.
    #[must_use]
    pub fn fixed_only() -> Self {
        ColumnSelection {
            all: false,
            all_attrs: false,
            fixed: BTreeSet::new(),
            attrs: BTreeSet::new(),
        }
    }

    /// True when this selection decodes every column.
    #[must_use]
    pub fn is_all(&self) -> bool {
        self.all
    }

    /// True when this selection decodes every dynamic attribute column (either
    /// because it decodes everything, or because a query referenced the merged
    /// `attrs` view).
    #[must_use]
    pub fn wants_all_attrs(&self) -> bool {
        self.all || self.all_attrs
    }

    /// Decode `observed_ts`.
    #[must_use]
    pub fn with_observed_ts(self) -> Self {
        self.with_fixed(COL_OBSERVED_TS)
    }

    /// Decode `severity_num`.
    #[must_use]
    pub fn with_severity_num(self) -> Self {
        self.with_fixed(COL_SEVERITY_NUM)
    }

    /// Decode `severity_text`.
    #[must_use]
    pub fn with_severity_text(self) -> Self {
        self.with_fixed(COL_SEVERITY_TEXT)
    }

    /// Decode `body`.
    #[must_use]
    pub fn with_body(self) -> Self {
        self.with_fixed(COL_BODY)
    }

    /// Decode `trace_id`.
    #[must_use]
    pub fn with_trace_id(self) -> Self {
        self.with_fixed(COL_TRACE_ID)
    }

    /// Decode `span_id`.
    #[must_use]
    pub fn with_span_id(self) -> Self {
        self.with_fixed(COL_SPAN_ID)
    }

    /// Decode `flags`.
    #[must_use]
    pub fn with_flags(self) -> Self {
        self.with_fixed(COL_FLAGS)
    }

    /// Decode the dynamic column(s) carrying attribute `name`, plus the
    /// `attrs_raw` overflow column (a record can carry the attribute there
    /// instead of in a column, and the two are indistinguishable to a caller
    /// naming a key).
    ///
    /// A name can map to more than one column id: FIELD_DIR keys a column by
    /// `(name, type)`, so the same key seen with a string value and an integer
    /// value in one object occupies two columns. All of them are decoded.
    #[must_use]
    pub fn with_attr(mut self, name: impl Into<String>) -> Self {
        self.attrs.insert(name.into());
        self
    }

    /// Decode every dynamic attribute column plus `attrs_raw`. This is what a
    /// query referencing the SQL `attrs` map column at all resolves to,
    /// including `SELECT *` (ADR-0087: per-key `attrs['k']` projection is out
    /// of scope, and the merged map's contract is that every key is present).
    #[must_use]
    pub fn with_all_attrs(mut self) -> Self {
        self.all_attrs = true;
        self
    }

    fn with_fixed(mut self, column_id: u32) -> Self {
        self.fixed.insert(column_id);
        self
    }

    /// Resolve to the concrete block column ids for one object, or `None` when
    /// every column is wanted (which [`crate::block::read_block_columns`] takes
    /// as "no filter").
    pub(crate) fn resolve(&self, field_dir: &FieldDir) -> Option<HashSet<u32>> {
        if self.all {
            return None;
        }
        let mut out: HashSet<u32> = HashSet::new();
        // Always needed: the exact ts re-check and the stream-identity lookup.
        out.insert(COL_TS);
        out.insert(COL_STREAM_REF);
        out.extend(self.fixed.iter().copied());
        if self.all_attrs {
            out.insert(COL_ATTRS_RAW);
            out.extend(field_dir.entries().iter().map(|e| e.column_id));
        } else if !self.attrs.is_empty() {
            // An attribute named here may live in a dynamic column or have
            // overflowed into `attrs_raw`; both must be readable.
            out.insert(COL_ATTRS_RAW);
            for e in field_dir.entries() {
                if self.attrs.contains(&e.name) {
                    out.insert(e.column_id);
                }
            }
        }
        Some(out)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::record::FieldType;
    use crate::writer::{ObjectIdentity, RlogConfig, RlogWriter};
    use crate::{LogRecord, RlogReader};
    use ravel_types::logstream::AttrValue;

    fn dir() -> FieldDir {
        // Build a real object so the FIELD_DIR is the writer's, not a fixture.
        let identity = ObjectIdentity {
            tenant_hash: [0u8; 16],
            shard: 0,
            writer_id: [0u8; 16],
            writer_epoch: 0,
            writer_seq: 0,
        };
        let cfg = RlogConfig::default();
        let mut w = RlogWriter::new(cfg, identity);
        let resource = [("service.name".to_string(), AttrValue::Str("api".into()))];
        let mut rec = LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(&resource, "s", "1", &[]),
            stream_attrs: crate::record::stream_attrs_bytes(&resource, "s", "1", &[]),
            ts_ns: 1,
            observed_ts_ns: 1,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "b".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![
                ("k".to_string(), AttrValue::Str("v".into())),
                ("n".to_string(), AttrValue::I64(1)),
            ],
        };
        w.push(rec.clone()).expect("push");
        // Same key with a different value type: two FIELD_DIR columns for "k".
        rec.ts_ns = 2;
        rec.attrs = vec![("k".to_string(), AttrValue::I64(7))];
        w.push(rec).expect("push");
        let bytes = w.finish().expect("finish");
        let reader = RlogReader::new(&bytes, &cfg).expect("open");
        reader.field_dir().clone()
    }

    #[test]
    fn all_resolves_to_no_filter() {
        assert!(ColumnSelection::all().resolve(&dir()).is_none());
        assert!(ColumnSelection::all().is_all());
    }

    #[test]
    fn fixed_only_still_keeps_ts_and_stream_ref() {
        let got = ColumnSelection::fixed_only()
            .resolve(&dir())
            .expect("a filter");
        assert_eq!(got, HashSet::from([COL_TS, COL_STREAM_REF]));
    }

    #[test]
    fn named_fixed_columns_are_added() {
        let got = ColumnSelection::fixed_only()
            .with_body()
            .with_flags()
            .resolve(&dir())
            .expect("a filter");
        assert_eq!(
            got,
            HashSet::from([COL_TS, COL_STREAM_REF, COL_BODY, COL_FLAGS])
        );
    }

    #[test]
    fn one_attr_name_resolves_to_every_column_id_for_that_name_plus_overflow() {
        let field_dir = dir();
        let want: HashSet<u32> = field_dir
            .entries()
            .iter()
            .filter(|e| e.name == "k")
            .map(|e| e.column_id)
            .collect();
        // The fixture writes `k` as both Str and I64, so this is the
        // multi-column case, not a single-column one.
        assert_eq!(
            want.len(),
            2,
            "fixture must exercise two columns for one key"
        );
        assert!(field_dir.column("k", FieldType::Str).is_some());
        assert!(field_dir.column("k", FieldType::I64).is_some());

        let got = ColumnSelection::fixed_only()
            .with_attr("k")
            .resolve(&field_dir)
            .expect("a filter");
        let mut expect = want;
        expect.extend([COL_TS, COL_STREAM_REF, COL_ATTRS_RAW]);
        assert_eq!(got, expect);
        // `n` is a different key and is not decoded.
        let n = field_dir.column("n", FieldType::I64).expect("n column");
        assert!(!got.contains(&n.column_id));
    }

    #[test]
    fn all_attrs_resolves_to_every_dynamic_column_plus_overflow() {
        let field_dir = dir();
        let got = ColumnSelection::fixed_only()
            .with_all_attrs()
            .resolve(&field_dir)
            .expect("a filter");
        assert!(got.contains(&COL_ATTRS_RAW));
        for e in field_dir.entries() {
            assert!(got.contains(&e.column_id), "missing column {}", e.column_id);
        }
        assert!(
            ColumnSelection::fixed_only()
                .with_all_attrs()
                .wants_all_attrs()
        );
    }
}
