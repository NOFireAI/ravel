//! The RSEG reader's supported-version window (ADR-0066 decision 1), from the
//! outside: which trailer versions a real object can carry and still be read,
//! what a version outside the window produces, and that the trailer gate and
//! the structural validator admit exactly the same set.
//!
//! The window holds only v7 today, so an "outside the window" fixture is a real
//! v7 object with its trailer version field overwritten. That is not a
//! reconstructed old object and does not pretend to be one: the version field
//! is read before anything that depends on the layout (docs/segment-format.md
//! reader protocol step 2), so those bytes are exactly what a reader meets when
//! a peer writes a version it does not admit, which is the situation under
//! test.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_segment::{
    IngestBounds, ReaderLimits, SUPPORTED_VERSIONS, SegmentError, SegmentIdentity, SegmentVersion,
    SegmentWriter, SeriesInput, TRAILER_LEN, TrailerClass, VERSION_V7, classify_trailer,
    open_from_full, parse_footer, validate_sections,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

const TRAILER_LEN_USIZE: usize = TRAILER_LEN as usize;
/// Byte offset of the trailer's `version: u16` within the 16-byte trailer
/// (footer_len 4 + footer_crc32c 4), docs/segment-format.md.
const VERSION_OFFSET_IN_TRAILER: usize = 8;

fn labels(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::new(
        pairs
            .iter()
            .map(|(n, v)| Label {
                name: (*n).to_string(),
                value: (*v).to_string(),
            })
            .collect(),
    )
    .expect("valid labels")
}

/// A minimal real, valid current-version object (one series, one sample).
fn one_sample_object() -> Vec<u8> {
    let series = vec![SeriesInput {
        series_id: SeriesId([0x7F; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "lonely_metric")]),
        samples: vec![Sample {
            ts_ns: 123,
            value: 9.75,
        }],
    }];
    SegmentWriter::write(
        series,
        SegmentIdentity {
            tenant_hash: [3u8; 16],
            shard: 1,
            writer_id: "test-writer".to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        },
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 1000,
        },
    )
    .expect("writes")
    .bytes
    .to_vec()
}

/// Overwrite an object's trailer version field in place.
fn stamp_version(bytes: &mut [u8], version: u16) {
    let at = bytes.len() - TRAILER_LEN_USIZE + VERSION_OFFSET_IN_TRAILER;
    bytes[at..at + 2].copy_from_slice(&version.to_le_bytes());
}

/// The minimal object with its trailer version field overwritten.
fn object_stamped(version: u16) -> Vec<u8> {
    let mut bytes = one_sample_object();
    stamp_version(&mut bytes, version);
    bytes
}

/// The typed error a parse rejected with, or `None` when it did not reject.
fn parse_error(bytes: &[u8]) -> Option<SegmentError> {
    parse_footer(bytes.len() as u64, bytes).err()
}

/// An object whose version is inside the window reads normally, end to end,
/// and reports the version it carries.
#[test]
fn a_version_inside_the_window_reads_normally() {
    let object = one_sample_object();
    let loc = open_from_full(&object, ReaderLimits::default()).expect("a v7 object opens");
    assert_eq!(loc.version, VERSION_V7);
    assert!(
        SUPPORTED_VERSIONS.contains(loc.version),
        "a successfully opened object always carries an admitted version"
    );
}

/// An object whose version is outside the window fails with the exact typed
/// rejection, carrying the version it declared, and fails there rather than
/// anywhere deeper: the version is read before the footer crc, so a stamped
/// object (whose crc no longer covers its version) still reports
/// `UnsupportedVersion`, not `FooterCrcMismatch`.
#[test]
fn a_version_outside_the_window_fails_with_unsupported_version() {
    for version in [0u16, 1, 6, 8, 100, u16::MAX] {
        assert!(
            !SUPPORTED_VERSIONS.contains(version),
            "fixture version {version} must be outside today's window"
        );
        let object = object_stamped(version);
        assert_eq!(
            open_from_full(&object, ReaderLimits::default()).err(),
            Some(SegmentError::UnsupportedVersion(version)),
            "version {version}"
        );
    }
}

/// The trailer gate and the structural validator admit exactly the same set,
/// swept over the whole `u16` domain. Both resolve through the window's single
/// token point, so this cannot fail today; it fails the moment either grows a
/// version literal of its own, which is the drift that made an N-1 object pass
/// the gate and then be rejected by the validator.
#[test]
fn the_trailer_gate_and_the_validator_admit_the_same_versions() {
    let mut object = one_sample_object();
    let loc = open_from_full(&object, ReaderLimits::default()).expect("opens");
    let region = loc.footer_offset;

    for version in 0..=u16::MAX {
        let admitted = SUPPORTED_VERSIONS.contains(version);
        let rejection = Some(SegmentError::UnsupportedVersion(version));

        stamp_version(&mut object, version);
        let gate_rejected = parse_error(&object) == rejection;
        let validator_rejected =
            validate_sections(&loc.footer, version, region, ReaderLimits::default()).err()
                == rejection;

        assert_eq!(
            gate_rejected, !admitted,
            "version {version}: the trailer gate must reject exactly what the window excludes"
        );
        assert_eq!(
            validator_rejected, !admitted,
            "version {version}: the validator must reject exactly what the window excludes"
        );
    }
}

/// Every admitted version has a rule set: the validator, handed a token's own
/// number and a real footer for that version, never answers
/// `UnsupportedVersion`. With one variant this is one case; at a bump it is the
/// check that the newly admitted version was given a validator rather than
/// only an entry in the window.
#[test]
fn every_admitted_version_reaches_a_rule_set() {
    let object = one_sample_object();
    let loc = open_from_full(&object, ReaderLimits::default()).expect("opens");
    for version in SUPPORTED_VERSIONS.versions() {
        let number = version.number();
        assert_ne!(
            validate_sections(
                &loc.footer,
                number,
                loc.footer_offset,
                ReaderLimits::default()
            )
            .err(),
            Some(SegmentError::UnsupportedVersion(number)),
            "admitted version {number} has no rule set"
        );
    }
}

/// The trailer probe classifies from 16 bytes, and separates the two kinds of
/// "cannot read this" a caller holding a delete decision must not collapse.
#[test]
fn classify_trailer_separates_an_old_version_from_corruption() {
    let object = one_sample_object();
    let total = object.len() as u64;
    let tail = &object[object.len() - TRAILER_LEN_USIZE..];
    assert_eq!(
        classify_trailer(total, tail),
        TrailerClass::Readable(SegmentVersion::V7)
    );

    let stamped = object_stamped(8);
    assert_eq!(
        classify_trailer(total, &stamped[stamped.len() - TRAILER_LEN_USIZE..]),
        TrailerClass::OutsideVersionWindow(8),
        "a version this build does not admit is not corruption"
    );

    // Bad magic: no build reads these bytes.
    let mut smashed = object.clone();
    let last = smashed.len() - 1;
    smashed[last] ^= 0xFF;
    assert_eq!(
        classify_trailer(total, &smashed[smashed.len() - TRAILER_LEN_USIZE..]),
        TrailerClass::Corrupt(SegmentError::BadMagic)
    );

    // Too small to hold a trailer at all.
    assert_eq!(
        classify_trailer(4, &[0u8; 4]),
        TrailerClass::Corrupt(SegmentError::TooSmall { size: 4 })
    );

    // A well-formed suffix whose object is large enough but whose supplied tail
    // is short is reported as truncated, not guessed at.
    assert_eq!(
        classify_trailer(total, &tail[..4]),
        TrailerClass::Corrupt(SegmentError::Truncated)
    );
}

proptest! {
    /// Arbitrary bytes in the trailer position never panic and never produce a
    /// `Readable` answer for a version the window excludes: the probe either
    /// names a typed corruption, names the version as outside the window, or
    /// reports a version the window admits. `OutsideVersionWindow` and
    /// `Readable` partition exactly on window membership.
    #[test]
    fn classify_trailer_is_typed_on_arbitrary_bytes(raw in proptest::collection::vec(any::<u8>(), TRAILER_LEN_USIZE..64)) {
        let total = raw.len() as u64;
        match classify_trailer(total, &raw) {
            TrailerClass::Readable(v) => {
                prop_assert!(SUPPORTED_VERSIONS.contains(v.number()));
            }
            TrailerClass::OutsideVersionWindow(v) => {
                prop_assert!(!SUPPORTED_VERSIONS.contains(v));
            }
            TrailerClass::Corrupt(_) => {}
        }
    }

    /// The probe and a full parse apply the same version gate to the same
    /// bytes: whenever `parse_footer` rejects with `UnsupportedVersion(v)`, the
    /// probe says `OutsideVersionWindow(v)`, and never the other way round.
    #[test]
    fn the_probe_and_a_full_parse_share_one_gate(version in any::<u16>(), tail_noise in any::<u8>()) {
        let mut object = object_stamped(version);
        // Perturb a payload byte so the fixture is not merely the writer's own
        // output with one field changed.
        let at = object.len() / 2;
        object[at] ^= tail_noise;
        let total = object.len() as u64;

        let probe = classify_trailer(total, &object[object.len() - TRAILER_LEN_USIZE..]);
        let parsed = parse_error(&object);
        let rejection = Some(SegmentError::UnsupportedVersion(version));

        if SUPPORTED_VERSIONS.contains(version) {
            prop_assert!(matches!(probe, TrailerClass::Readable(v) if v.number() == version));
            prop_assert_ne!(parsed, rejection);
        } else {
            prop_assert_eq!(probe, TrailerClass::OutsideVersionWindow(version));
            prop_assert_eq!(parsed, rejection);
        }
    }
}
