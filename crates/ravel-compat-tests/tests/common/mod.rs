//! Readers shared by the compat test binaries: decode every sample of an RSEG
//! object through the segment reader, and snapshot a store so a test can show
//! a step wrote nothing.

#![allow(dead_code, clippy::expect_used, clippy::unwrap_used)]

pub mod pipeline;
pub mod tap;

use std::collections::BTreeMap;
use std::fmt;

use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_segment::{
    ReaderLimits, SeriesEntryV4, ValueKind, decode_catalog_v5, decode_run_pages_soa,
    open_from_full, plan_ranges_v4,
};

/// One series of one RSEG object as the segment reader decodes it: labels in
/// label-set order, the stored `SeriesId`, and every scalar sample as
/// `(ts_ns, value.to_bits())` in stored order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DecodedSeries {
    pub labels: Vec<(String, String)>,
    pub series_id: [u8; 16],
    pub samples: Vec<(i64, u64)>,
}

impl fmt::Display for DecodedSeries {
    /// `name=value,...<TAB>series-id-hex<TAB>ts:bits-hex,...`, the line shape of
    /// a corpus `samples.txt` after its leading key column.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let labels: Vec<String> = self
            .labels
            .iter()
            .map(|(n, v)| format!("{n}={v}"))
            .collect();
        let samples: Vec<String> = self
            .samples
            .iter()
            .map(|(ts, bits)| format!("{ts}:{bits:016x}"))
            .collect();
        write!(
            f,
            "{}\t{}\t{}",
            labels.join(","),
            hex(&self.series_id),
            samples.join(",")
        )
    }
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn range(object: &[u8], (offset, len): (u64, u64)) -> &[u8] {
    let start = usize::try_from(offset).expect("offset fits");
    let end = start + usize::try_from(len).expect("len fits");
    &object[start..end]
}

/// Decode every series and sample of a whole RSEG object, in catalog order.
/// Opening the trailer is the version check: an object outside this build's
/// read window fails here with the reader's typed error.
pub fn decode_rseg(object: &[u8]) -> Result<Vec<DecodedSeries>, ravel_segment::SegmentError> {
    let limits = ReaderLimits::default();
    let loc = open_from_full(object, limits)?;
    let entries = decode_catalog_v5(&loc.footer, object, limits)?;
    let refs: Vec<&SeriesEntryV4> = entries.iter().collect();
    let mut planned = plan_ranges_v4(&loc.footer, &refs)?.into_iter();
    let mut out = Vec::with_capacity(entries.len());
    let mut scratch = Vec::new();
    for entry in &entries {
        assert_eq!(
            entry.entry.value_kind,
            ValueKind::Scalar,
            "the compat fixtures write scalar samples only"
        );
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for run in &entry.runs {
            let planned = planned.next().expect("one planned range per run");
            decode_run_pages_soa(
                &entry.entry.series_id,
                run,
                range(object, planned.ts_range),
                range(object, planned.val_range),
                limits,
                &mut scratch,
                &mut timestamps,
                &mut values,
            )?;
        }
        out.push(DecodedSeries {
            labels: entry
                .entry
                .labels
                .iter()
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect(),
            series_id: entry.entry.series_id.0,
            samples: timestamps
                .into_iter()
                .zip(values)
                .map(|(ts, v)| (ts, v.to_bits()))
                .collect(),
        });
    }
    Ok(out)
}

/// Every key in the store with its bytes, for a before/after comparison.
pub async fn snapshot(store: &dyn ObjectStoreBackend) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    for meta in list_all(store, "").await.expect("list the store") {
        let bytes = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get a listed key")
            .data;
        out.insert(meta.key, bytes.to_vec());
    }
    out
}

/// The keys of every data object (`.rseg` suffix, which every bulk signal's
/// data key carries) in the store.
pub async fn data_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    list_all(store, "")
        .await
        .expect("list the store")
        .into_iter()
        .map(|m| m.key)
        .filter(|k| k.ends_with(".rseg"))
        .collect()
}
