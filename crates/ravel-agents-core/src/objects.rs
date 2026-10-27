//! Codecs for the immutable objects stored under `o/<object_hash>`.
//!
//! Each object is written once and named by the blake3 of its bytes, so
//! these codecs never re-encode a decoded value. Decoding applies the same
//! gate as HEAD: `format_version == 1` read first, then the full message,
//! then the seal, then every typed field.

use prost::Message as _;
use ravel_proto::agents::v1 as pb;

use crate::head::{
    CodecError, EffectRecord, EventEntry, FORMAT_VERSION, InboxEntry, Receipt, ResultEntry, SEAL,
    StepRecord, TimerEntry, check_format_version, check_seal, effect_from_proto, effect_to_proto, event_from_proto,
    event_to_proto, inbox_from_proto, inbox_to_proto, peek_format_version, receipt_from_proto,
    receipt_to_proto, result_from_proto, result_to_proto, step_from_proto, step_to_proto,
    timer_from_proto, timer_to_proto,
};
use crate::identity::Id128;

fn decode_gated<M: prost::Message + Default>(
    bytes: &[u8],
    version_and_seal: impl Fn(&M) -> (u32, u32),
) -> Result<M, CodecError> {
    peek_format_version(bytes)?;
    let message = M::decode(bytes).map_err(|e| CodecError::Decode(e.to_string()))?;
    let (version, seal) = version_and_seal(&message);
    check_format_version(version)?;
    check_seal(seal)?;
    Ok(message)
}

fn turn(bytes: &[u8], field: &'static str) -> Result<Id128, CodecError> {
    Id128::from_slice(bytes).ok_or(CodecError::Invalid { field })
}

macro_rules! entry_page {
    ($encode:ident, $decode:ident, $msg:ident, $entry:ty, $to:ident, $from:ident) => {
        pub fn $encode(entries: &[$entry]) -> Vec<u8> {
            pb::$msg {
                format_version: FORMAT_VERSION,
                entries: entries.iter().map($to).collect(),
                seal: SEAL,
            }
            .encode_to_vec()
        }

        pub fn $decode(bytes: &[u8]) -> Result<Vec<$entry>, CodecError> {
            let page: pb::$msg = decode_gated(bytes, |m: &pb::$msg| (m.format_version, m.seal))?;
            page.entries.into_iter().map($from).collect()
        }
    };
}

entry_page!(
    encode_inbox_page,
    decode_inbox_page,
    InboxPage,
    InboxEntry,
    inbox_to_proto,
    inbox_from_proto
);
entry_page!(
    encode_receipt_page,
    decode_receipt_page,
    ReceiptPage,
    Receipt,
    receipt_to_proto,
    receipt_from_proto
);
entry_page!(
    encode_result_page,
    decode_result_page,
    ResultPage,
    ResultEntry,
    result_to_proto,
    result_from_proto
);
entry_page!(
    encode_event_page,
    decode_event_page,
    EventPage,
    EventEntry,
    event_to_proto,
    event_from_proto
);

pub fn encode_timer_table(timers: &[TimerEntry]) -> Vec<u8> {
    pb::TimerTable {
        format_version: FORMAT_VERSION,
        timers: timers.iter().map(timer_to_proto).collect(),
        seal: SEAL,
    }
    .encode_to_vec()
}

pub fn decode_timer_table(bytes: &[u8]) -> Result<Vec<TimerEntry>, CodecError> {
    let table: pb::TimerTable = decode_gated(bytes, |m: &pb::TimerTable| (m.format_version, m.seal))?;
    table.timers.into_iter().map(timer_from_proto).collect()
}

pub fn encode_step_transcript(turn_id: Id128, steps: &[StepRecord]) -> Vec<u8> {
    pb::StepTranscript {
        format_version: FORMAT_VERSION,
        turn_id: turn_id.0.to_vec(),
        steps: steps.iter().map(step_to_proto).collect(),
        seal: SEAL,
    }
    .encode_to_vec()
}

pub fn decode_step_transcript(bytes: &[u8]) -> Result<(Id128, Vec<StepRecord>), CodecError> {
    let transcript: pb::StepTranscript = decode_gated(bytes, |m: &pb::StepTranscript| (m.format_version, m.seal))?;
    let turn_id = turn(&transcript.turn_id, "step_transcript.turn_id")?;
    let steps = transcript
        .steps
        .into_iter()
        .map(step_from_proto)
        .collect::<Result<_, _>>()?;
    Ok((turn_id, steps))
}

pub fn encode_effect_journal(turn_id: Id128, effects: &[EffectRecord]) -> Vec<u8> {
    pb::EffectJournal {
        format_version: FORMAT_VERSION,
        turn_id: turn_id.0.to_vec(),
        effects: effects.iter().map(effect_to_proto).collect(),
        seal: SEAL,
    }
    .encode_to_vec()
}

pub fn decode_effect_journal(bytes: &[u8]) -> Result<(Id128, Vec<EffectRecord>), CodecError> {
    let journal: pb::EffectJournal = decode_gated(bytes, |m: &pb::EffectJournal| (m.format_version, m.seal))?;
    let turn_id = turn(&journal.turn_id, "effect_journal.turn_id")?;
    let effects = journal
        .effects
        .into_iter()
        .map(effect_from_proto)
        .collect::<Result<_, _>>()?;
    Ok((turn_id, effects))
}

/// One agent state version; `state` is opaque to Ravel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSnapshot {
    pub state_version: u64,
    pub content_type: String,
    pub state: Vec<u8>,
}

impl StateSnapshot {
    pub fn encode(&self) -> Vec<u8> {
        pb::StateSnapshot {
            format_version: FORMAT_VERSION,
            state_version: self.state_version,
            content_type: self.content_type.clone(),
            state: self.state.clone(),
            seal: SEAL,
        }
        .encode_to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<StateSnapshot, CodecError> {
        let raw: pb::StateSnapshot = decode_gated(bytes, |m: &pb::StateSnapshot| (m.format_version, m.seal))?;
        Ok(StateSnapshot {
            state_version: raw.state_version,
            content_type: raw.content_type,
            state: raw.state,
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::head::tests::{
        effect_record, event_entry, inbox_entry, receipt, result_entry, step_record, timer_entry,
    };
    use proptest::collection::vec;
    use proptest::prelude::*;

    fn assert_prefixes_refused<T: std::fmt::Debug>(
        bytes: &[u8],
        decode: impl Fn(&[u8]) -> Result<T, CodecError>,
    ) -> Result<(), TestCaseError> {
        for len in 0..bytes.len() {
            prop_assert!(decode(&bytes[..len]).is_err(), "prefix {} decoded", len);
        }
        Ok(())
    }

    fn assert_flips_typed<T>(
        bytes: &[u8],
        flips: &[(prop::sample::Index, u8)],
        decode: impl Fn(&[u8]) -> Result<T, CodecError>,
    ) {
        let mut bytes = bytes.to_vec();
        for (at, value) in flips {
            let i = at.index(bytes.len());
            bytes[i] = *value;
        }
        let _ = decode(&bytes);
    }

    #[test]
    fn a_newer_object_version_is_refused() {
        let bytes = pb::InboxPage {
            format_version: 2,
            entries: Vec::new(),
            seal: SEAL,
        }
        .encode_to_vec();
        assert_eq!(
            decode_inbox_page(&bytes),
            Err(CodecError::UnsupportedFormat { found: 2 })
        );
    }

    /// `bytes` with field 1 written again, as `version`, before the seal.
    fn with_second_version(mut bytes: Vec<u8>, version: u8) -> Vec<u8> {
        let seal = bytes.split_off(bytes.len() - 6);
        bytes.extend_from_slice(&[0x08, version]);
        bytes.extend_from_slice(&seal);
        bytes
    }

    #[test]
    fn a_repeated_format_version_field_is_gated_on_its_decoded_value() {
        let found: Result<(), CodecError> = Err(CodecError::UnsupportedFormat { found: 2 });
        let page = with_second_version(encode_inbox_page(&[]), 2);
        assert_eq!(peek_format_version(&page), Ok(FORMAT_VERSION));
        assert_eq!(decode_inbox_page(&page).map(|_| ()), found.clone());
        let table = with_second_version(encode_timer_table(&[]), 2);
        assert_eq!(decode_timer_table(&table).map(|_| ()), found.clone());
        let transcript = with_second_version(encode_step_transcript(Id128([1; 16]), &[]), 2);
        assert_eq!(decode_step_transcript(&transcript).map(|_| ()), found.clone());
        let journal = with_second_version(encode_effect_journal(Id128([1; 16]), &[]), 2);
        assert_eq!(decode_effect_journal(&journal).map(|_| ()), found.clone());
        let snapshot = StateSnapshot {
            state_version: 1,
            content_type: String::new(),
            state: Vec::new(),
        };
        let snapshot = with_second_version(snapshot.encode(), 2);
        assert_eq!(StateSnapshot::decode(&snapshot).map(|_| ()), found);
    }

    type Flips = Vec<(prop::sample::Index, u8)>;

    fn flips() -> impl Strategy<Value = Flips> {
        vec((any::<prop::sample::Index>(), any::<u8>()), 1..4)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn inbox_page_round_trips(e in vec(inbox_entry(), 0..4), f in flips()) {
            let bytes = encode_inbox_page(&e);
            prop_assert_eq!(decode_inbox_page(&bytes), Ok(e));
            assert_prefixes_refused(&bytes, decode_inbox_page)?;
            assert_flips_typed(&bytes, &f, decode_inbox_page);
        }

        #[test]
        fn receipt_page_round_trips(e in vec(receipt(), 0..4), f in flips()) {
            let bytes = encode_receipt_page(&e);
            prop_assert_eq!(decode_receipt_page(&bytes), Ok(e));
            assert_prefixes_refused(&bytes, decode_receipt_page)?;
            assert_flips_typed(&bytes, &f, decode_receipt_page);
        }

        #[test]
        fn result_page_round_trips(e in vec(result_entry(), 0..4), f in flips()) {
            let bytes = encode_result_page(&e);
            prop_assert_eq!(decode_result_page(&bytes), Ok(e));
            assert_prefixes_refused(&bytes, decode_result_page)?;
            assert_flips_typed(&bytes, &f, decode_result_page);
        }

        #[test]
        fn event_page_round_trips(e in vec(event_entry(), 0..4), f in flips()) {
            let bytes = encode_event_page(&e);
            prop_assert_eq!(decode_event_page(&bytes), Ok(e));
            assert_prefixes_refused(&bytes, decode_event_page)?;
            assert_flips_typed(&bytes, &f, decode_event_page);
        }

        #[test]
        fn timer_table_round_trips(e in vec(timer_entry(), 0..4), f in flips()) {
            let bytes = encode_timer_table(&e);
            prop_assert_eq!(decode_timer_table(&bytes), Ok(e));
            assert_prefixes_refused(&bytes, decode_timer_table)?;
            assert_flips_typed(&bytes, &f, decode_timer_table);
        }

        #[test]
        fn step_transcript_round_trips(
            t in any::<[u8; 16]>(), e in vec(step_record(), 0..4), f in flips(),
        ) {
            let bytes = encode_step_transcript(Id128(t), &e);
            prop_assert_eq!(decode_step_transcript(&bytes), Ok((Id128(t), e)));
            assert_prefixes_refused(&bytes, decode_step_transcript)?;
            assert_flips_typed(&bytes, &f, decode_step_transcript);
        }

        #[test]
        fn effect_journal_round_trips(
            t in any::<[u8; 16]>(), e in vec(effect_record(), 0..4), f in flips(),
        ) {
            let bytes = encode_effect_journal(Id128(t), &e);
            prop_assert_eq!(decode_effect_journal(&bytes), Ok((Id128(t), e)));
            assert_prefixes_refused(&bytes, decode_effect_journal)?;
            assert_flips_typed(&bytes, &f, decode_effect_journal);
        }

        #[test]
        fn state_snapshot_round_trips(
            state_version in any::<u64>(),
            content_type in ".{0,12}",
            state in vec(any::<u8>(), 0..64),
            f in flips(),
        ) {
            let snapshot = StateSnapshot { state_version, content_type, state };
            let bytes = snapshot.encode();
            prop_assert_eq!(StateSnapshot::decode(&bytes), Ok(snapshot));
            assert_prefixes_refused(&bytes, StateSnapshot::decode)?;
            assert_flips_typed(&bytes, &f, StateSnapshot::decode);
        }
    }
}
