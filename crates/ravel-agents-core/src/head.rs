//! The HEAD record model and its codec (ADR-3331 decision 3).
//!
//! `AgentHead` is CAS-mutable, so the decoder is a gate: it reads only
//! `format_version == 1` with the `INLINE` representation and refuses
//! anything else as a typed [`CodecError`] before any field is trusted
//! (`a_newer_format_version_is_refused_before_the_rest_is_read`,
//! `paged_representation_and_page_refs_are_refused_by_version_one`,
//! `a_repeated_format_version_field_is_gated_on_its_decoded_value`). The
//! encoder never re-encodes a decoded prost value; it builds a fresh
//! `AgentHead` from the typed [`Head`]. A field this binary does not know,
//! inside a version 1 record, is therefore dropped by the next CAS write
//! (`an_unknown_field_within_version_1_is_dropped_on_re_encode`), which is
//! why every additive change to `AgentHead` bumps `format_version` (ADR-0066
//! R1): a newer writer's record is refused here before anything is dropped.

use prost::Message as _;
use ravel_proto::agents::v1 as pb;
use ravel_types::TenantHash;

use crate::error::ErrorKind;
use crate::identity::{Hash256, Id128, ObjectHash};
use crate::keys::{AgentKey, AgentKind};

/// The only `AgentHead` format version this release reads or writes.
pub const FORMAT_VERSION: u32 = 1;

/// Value of the `seal` field every top-level message ends with ("RAG1").
pub const SEAL: u32 = 0x5241_4731;

/// Cap on encoded HEAD bytes; a transition past it fails `BudgetExceeded`.
pub const MAX_HEAD_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("protobuf decode failed: {0}")]
    Decode(String),
    #[error("record is truncated: the seal field is absent")]
    Truncated,
    #[error("record seal is {0:#010x}, expected {SEAL:#010x}")]
    BadSeal(u32),
    #[error("record has no format_version")]
    MissingFormatVersion,
    #[error("format_version {found} is not supported by this release")]
    UnsupportedFormat { found: u32 },
    #[error("index representation {found} is not supported by format version 1")]
    UnsupportedRepresentation { found: i32 },
    #[error("field {field} is missing or out of range")]
    Invalid { field: &'static str },
}

impl CodecError {
    /// The API error kind, for the two variants that are a version skew
    /// rather than corruption.
    pub fn kind(&self) -> Option<ErrorKind> {
        match self {
            CodecError::UnsupportedFormat { .. } | CodecError::UnsupportedRepresentation { .. } => {
                Some(ErrorKind::UnsupportedFormat)
            }
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct VersionPeek {
    #[prost(uint32, tag = "1")]
    format_version: u32,
}

/// Read field 1 of any record in this file without decoding the rest, so a
/// newer version is refused even when its later fields do not parse.
pub fn peek_format_version(bytes: &[u8]) -> Result<u32, CodecError> {
    // Field 1 is encoded first, so its key byte (tag 1, varint) leads.
    let version = match bytes.split_first() {
        Some((0x08, mut rest)) => prost::encoding::decode_varint(&mut rest)
            .map_err(|e| CodecError::Decode(e.to_string()))
            .map(|v| u32::try_from(v).unwrap_or(u32::MAX))?,
        _ => {
            VersionPeek::decode(bytes)
                .map_err(|e| CodecError::Decode(e.to_string()))?
                .format_version
        }
    };
    check_format_version(version).map(|()| FORMAT_VERSION)
}

/// Gate every top-level occurrence of field 1 of a HEAD before it is
/// decoded. Each occurrence is read as the raw 64-bit varint: prost
/// truncates a `uint32` varint above `u32::MAX` to its low 32 bits, so a
/// trailing `2^32 + 1` behind a leading 1 (the occurrence protobuf keeps)
/// would otherwise decode as version 1 and pass [`check_format_version`].
/// Such a value is refused as `UnsupportedFormat { found: u32::MAX }`
/// (`tests::a_format_version_varint_above_u32_is_refused_before_decode`).
fn gate_every_format_version(bytes: &[u8]) -> Result<(), CodecError> {
    use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
    let decode = |e: prost::DecodeError| CodecError::Decode(e.to_string());
    let mut rest = bytes;
    while !rest.is_empty() {
        let (tag, wire_type) = decode_key(&mut rest).map_err(decode)?;
        if tag != 1 {
            skip_field(wire_type, tag, &mut rest, DecodeContext::default()).map_err(decode)?;
            continue;
        }
        if wire_type != WireType::Varint {
            return Err(CodecError::Decode(
                "format_version is not a varint".to_string(),
            ));
        }
        let raw = decode_varint(&mut rest).map_err(decode)?;
        check_format_version(u32::try_from(raw).unwrap_or(u32::MAX))?;
    }
    Ok(())
}

/// Gate on the `format_version` a full decode settled on. Protobuf keeps the
/// last occurrence of a repeated scalar field, so a record that writes field
/// 1 twice passes [`peek_format_version`] on its first occurrence and only
/// this check sees the value the decoded message carries. A HEAD is also
/// gated occurrence by occurrence before decode, which this check cannot do
/// for a value prost truncated.
pub(crate) fn check_format_version(version: u32) -> Result<(), CodecError> {
    match version {
        0 => Err(CodecError::MissingFormatVersion),
        FORMAT_VERSION => Ok(()),
        found => Err(CodecError::UnsupportedFormat { found }),
    }
}

pub(crate) fn check_seal(seal: u32) -> Result<(), CodecError> {
    match seal {
        SEAL => Ok(()),
        0 => Err(CodecError::Truncated),
        other => Err(CodecError::BadSeal(other)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectRef {
    pub hash: ObjectHash,
    pub size_bytes: u64,
}

impl ObjectRef {
    pub fn of(bytes: &[u8]) -> ObjectRef {
        ObjectRef {
            hash: ObjectHash::of(bytes),
            size_bytes: bytes.len() as u64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    Active,
    Tombstoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cancellation {
    None,
    Requested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnBlock {
    None,
    NonDeterministicReplay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultStatus {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerStatus {
    Scheduled,
    Delivered,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepStatus {
    Started,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPolicy {
    ProviderIdempotency,
    AtLeastOnce,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectStatus {
    Intent,
    Authorized,
    InDoubt,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTurn {
    pub input_accept_seq: u64,
    pub turn_id: Id128,
    pub base_state_version: u64,
    pub handler: String,
    pub handler_version: String,
    pub handler_digest: Hash256,
    pub cancellation: Cancellation,
    pub block: TurnBlock,
    pub started_unix_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputSource {
    Message {
        request_id: String,
        request_hash: Hash256,
    },
    Timer {
        timer_id: Id128,
        generation: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxEntry {
    pub accept_seq: u64,
    pub handler: String,
    pub source: InputSource,
    pub payload: Option<ObjectRef>,
    pub accepted_unix_ns: i64,
    pub failed_attempts: u32,
    /// The turn that last failed retryably on this input.
    pub last_failed_turn_id: Option<Id128>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub request_id: String,
    pub handler: String,
    pub request_hash: Hash256,
    pub accept_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultEntry {
    pub accept_seq: u64,
    pub status: ResultStatus,
    pub output: Option<ObjectRef>,
    pub error: String,
    pub turn_id: Option<Id128>,
    pub dead_lettered: bool,
    pub finished_unix_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventEntry {
    pub event_seq: u64,
    pub accept_seq: u64,
    pub turn_id: Id128,
    pub event_type: String,
    pub payload: Option<ObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerEntry {
    pub timer_id: Id128,
    pub timer_key: String,
    pub generation: u64,
    pub due_unix_ns: i64,
    pub handler: String,
    pub payload: Option<ObjectRef>,
    pub status: TimerStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepRecord {
    pub step_id: Id128,
    pub step_key: String,
    pub tool_name: String,
    pub tool_version: String,
    pub input_hash: Hash256,
    pub status: StepStatus,
    pub output: Option<ObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectRecord {
    pub effect_id: Id128,
    pub step_key: String,
    pub tool_name: String,
    pub tool_version: String,
    pub input_hash: Hash256,
    pub policy: RecoveryPolicy,
    pub status: EffectStatus,
    pub attempt: u32,
    pub result: Option<ObjectRef>,
    pub result_attempt: u32,
    pub turn_id: Id128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxIntent {
    pub effect_id: Id128,
    pub intent_key: String,
    pub tool_name: String,
    pub tool_version: String,
    pub policy: RecoveryPolicy,
    pub input: Option<ObjectRef>,
    pub turn_id: Id128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Quota {
    pub referenced_bytes: u64,
    pub mailbox_depth: u64,
    pub receipt_count: u64,
    pub timer_count: u64,
}

/// One agent's root, format version 1, `INLINE` representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    pub tenant: TenantHash,
    pub kind: AgentKind,
    pub key: AgentKey,
    pub incarnation: u64,
    pub root_revision: u64,
    /// Empty when no owner has activated the agent in this incarnation.
    pub owner_id: String,
    pub epoch: u64,
    pub owner_lease_hint_unix_ns: i64,
    pub state_version: u64,
    pub state: Option<ObjectRef>,
    pub next_accept_seq: u64,
    pub active_turn: Option<ActiveTurn>,
    pub timer_generation: u64,
    pub next_event_seq: u64,
    pub lifecycle: Lifecycle,
    pub quota: Quota,
    pub inbox: Vec<InboxEntry>,
    pub receipts: Vec<Receipt>,
    pub results: Vec<ResultEntry>,
    pub events: Vec<EventEntry>,
    pub timers: Vec<TimerEntry>,
    pub steps: Vec<StepRecord>,
    pub effects: Vec<EffectRecord>,
    pub outbox: Vec<OutboxIntent>,
}

impl Head {
    /// Canonical bytes: prost's deterministic encoding of a freshly built
    /// message, every field in tag order, `seal` last.
    pub fn encode(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<Head, CodecError> {
        peek_format_version(bytes)?;
        gate_every_format_version(bytes)?;
        let raw = pb::AgentHead::decode(bytes).map_err(|e| CodecError::Decode(e.to_string()))?;
        check_format_version(raw.format_version)?;
        check_seal(raw.seal)?;
        Head::from_proto(raw)
    }

    fn to_proto(&self) -> pb::AgentHead {
        pb::AgentHead {
            format_version: FORMAT_VERSION,
            tenant_hash: self.tenant.0.to_vec(),
            kind: self.kind.as_str().to_string(),
            key: self.key.as_str().to_string(),
            incarnation: self.incarnation,
            root_revision: self.root_revision,
            owner_id: self.owner_id.clone(),
            epoch: self.epoch,
            owner_lease_hint_unix_ns: self.owner_lease_hint_unix_ns,
            state_version: self.state_version,
            state: self.state.map(ref_to_proto),
            representation: pb::IndexRepresentation::Inline as i32,
            inbox_page: None,
            next_accept_seq: self.next_accept_seq,
            receipt_index: None,
            active_turn: self.active_turn.as_ref().map(turn_to_proto),
            step_transcript: None,
            effect_journal: None,
            outbox: None,
            timer_table: None,
            timer_generation: self.timer_generation,
            result_index: None,
            event_page: None,
            next_event_seq: self.next_event_seq,
            lifecycle: match self.lifecycle {
                Lifecycle::Active => pb::LifecycleStatus::Active,
                Lifecycle::Tombstoned => pb::LifecycleStatus::Tombstoned,
            } as i32,
            quota: Some(pb::QuotaCounters {
                referenced_bytes: self.quota.referenced_bytes,
                mailbox_depth: self.quota.mailbox_depth,
                receipt_count: self.quota.receipt_count,
                timer_count: self.quota.timer_count,
            }),
            inbox: self.inbox.iter().map(inbox_to_proto).collect(),
            receipts: self.receipts.iter().map(receipt_to_proto).collect(),
            results: self.results.iter().map(result_to_proto).collect(),
            events: self.events.iter().map(event_to_proto).collect(),
            timers: self.timers.iter().map(timer_to_proto).collect(),
            steps: self.steps.iter().map(step_to_proto).collect(),
            effects: self.effects.iter().map(effect_to_proto).collect(),
            outbox_intents: self.outbox.iter().map(outbox_to_proto).collect(),
            seal: SEAL,
        }
    }

    fn from_proto(raw: pb::AgentHead) -> Result<Head, CodecError> {
        match raw.representation {
            r if r == pb::IndexRepresentation::Inline as i32 => {}
            r if r == pb::IndexRepresentation::Paged as i32 => {
                return Err(CodecError::UnsupportedRepresentation { found: r });
            }
            _ => return Err(invalid("representation")),
        }
        for (field, page) in [
            ("inbox_page", &raw.inbox_page),
            ("receipt_index", &raw.receipt_index),
            ("step_transcript", &raw.step_transcript),
            ("effect_journal", &raw.effect_journal),
            ("outbox", &raw.outbox),
            ("timer_table", &raw.timer_table),
            ("result_index", &raw.result_index),
            ("event_page", &raw.event_page),
        ] {
            if page.is_some() {
                return Err(invalid(field));
            }
        }
        let tenant = <[u8; 16]>::try_from(raw.tenant_hash.as_slice())
            .map(TenantHash)
            .map_err(|_| invalid("tenant_hash"))?;
        let lifecycle = match pb::LifecycleStatus::try_from(raw.lifecycle) {
            Ok(pb::LifecycleStatus::Active) => Lifecycle::Active,
            Ok(pb::LifecycleStatus::Tombstoned) => Lifecycle::Tombstoned,
            _ => return Err(invalid("lifecycle")),
        };
        let quota = raw.quota.ok_or(invalid("quota"))?;
        Ok(Head {
            tenant,
            kind: AgentKind::new(raw.kind).map_err(|_| invalid("kind"))?,
            key: AgentKey::new(raw.key).map_err(|_| invalid("key"))?,
            incarnation: raw.incarnation,
            root_revision: raw.root_revision,
            owner_id: raw.owner_id,
            epoch: raw.epoch,
            owner_lease_hint_unix_ns: raw.owner_lease_hint_unix_ns,
            state_version: raw.state_version,
            state: ref_from_proto(raw.state, "state")?,
            next_accept_seq: raw.next_accept_seq,
            active_turn: raw.active_turn.map(turn_from_proto).transpose()?,
            timer_generation: raw.timer_generation,
            next_event_seq: raw.next_event_seq,
            lifecycle,
            quota: Quota {
                referenced_bytes: quota.referenced_bytes,
                mailbox_depth: quota.mailbox_depth,
                receipt_count: quota.receipt_count,
                timer_count: quota.timer_count,
            },
            inbox: collect(raw.inbox, inbox_from_proto)?,
            receipts: collect(raw.receipts, receipt_from_proto)?,
            results: collect(raw.results, result_from_proto)?,
            events: collect(raw.events, event_from_proto)?,
            timers: collect(raw.timers, timer_from_proto)?,
            steps: collect(raw.steps, step_from_proto)?,
            effects: collect(raw.effects, effect_from_proto)?,
            outbox: collect(raw.outbox_intents, outbox_from_proto)?,
        })
    }

    /// Every object this root references, for quota and for the store's
    /// dependency upload.
    pub fn object_refs(&self) -> impl Iterator<Item = ObjectRef> + '_ {
        self.state
            .iter()
            .copied()
            .chain(self.inbox.iter().filter_map(|e| e.payload))
            .chain(self.results.iter().filter_map(|r| r.output))
            .chain(self.events.iter().filter_map(|e| e.payload))
            .chain(self.timers.iter().filter_map(|t| t.payload))
            .chain(self.steps.iter().filter_map(|s| s.output))
            .chain(self.effects.iter().filter_map(|e| e.result))
            .chain(self.outbox.iter().filter_map(|o| o.input))
    }

    /// The counters this root's lists imply.
    pub fn computed_quota(&self) -> Quota {
        Quota {
            referenced_bytes: self
                .object_refs()
                .fold(0u64, |acc, r| acc.saturating_add(r.size_bytes)),
            mailbox_depth: self.inbox.len() as u64,
            receipt_count: self.receipts.len() as u64,
            timer_count: self
                .timers
                .iter()
                .filter(|t| t.status == TimerStatus::Scheduled)
                .count() as u64,
        }
    }
}

fn invalid(field: &'static str) -> CodecError {
    CodecError::Invalid { field }
}

fn collect<P, T>(
    items: Vec<P>,
    convert: fn(P) -> Result<T, CodecError>,
) -> Result<Vec<T>, CodecError> {
    items.into_iter().map(convert).collect()
}

fn id128(bytes: &[u8], field: &'static str) -> Result<Id128, CodecError> {
    Id128::from_slice(bytes).ok_or(invalid(field))
}

fn hash256(bytes: &[u8], field: &'static str) -> Result<Hash256, CodecError> {
    Hash256::from_slice(bytes).ok_or(invalid(field))
}

pub(crate) fn ref_to_proto(r: ObjectRef) -> pb::ObjectRef {
    pb::ObjectRef {
        object_hash: r.hash.0.to_vec(),
        size_bytes: r.size_bytes,
    }
}

pub(crate) fn ref_from_proto(
    r: Option<pb::ObjectRef>,
    field: &'static str,
) -> Result<Option<ObjectRef>, CodecError> {
    r.map(|r| {
        Ok(ObjectRef {
            hash: ObjectHash::from_slice(&r.object_hash).ok_or(invalid(field))?,
            size_bytes: r.size_bytes,
        })
    })
    .transpose()
}

fn turn_to_proto(t: &ActiveTurn) -> pb::ActiveTurn {
    pb::ActiveTurn {
        input_accept_seq: t.input_accept_seq,
        turn_id: t.turn_id.0.to_vec(),
        base_state_version: t.base_state_version,
        handler: t.handler.clone(),
        handler_version: t.handler_version.clone(),
        handler_digest: t.handler_digest.0.to_vec(),
        cancellation: match t.cancellation {
            Cancellation::None => pb::CancellationState::None,
            Cancellation::Requested => pb::CancellationState::Requested,
        } as i32,
        block: match t.block {
            TurnBlock::None => pb::TurnBlock::None,
            TurnBlock::NonDeterministicReplay => pb::TurnBlock::NonDeterministicReplay,
        } as i32,
        started_unix_ns: t.started_unix_ns,
    }
}

fn turn_from_proto(t: pb::ActiveTurn) -> Result<ActiveTurn, CodecError> {
    Ok(ActiveTurn {
        input_accept_seq: t.input_accept_seq,
        turn_id: id128(&t.turn_id, "active_turn.turn_id")?,
        base_state_version: t.base_state_version,
        handler: t.handler,
        handler_version: t.handler_version,
        handler_digest: hash256(&t.handler_digest, "active_turn.handler_digest")?,
        cancellation: match pb::CancellationState::try_from(t.cancellation) {
            Ok(pb::CancellationState::None) => Cancellation::None,
            Ok(pb::CancellationState::Requested) => Cancellation::Requested,
            _ => return Err(invalid("active_turn.cancellation")),
        },
        block: match pb::TurnBlock::try_from(t.block) {
            Ok(pb::TurnBlock::None) => TurnBlock::None,
            Ok(pb::TurnBlock::NonDeterministicReplay) => TurnBlock::NonDeterministicReplay,
            _ => return Err(invalid("active_turn.block")),
        },
        started_unix_ns: t.started_unix_ns,
    })
}

pub(crate) fn inbox_to_proto(e: &InboxEntry) -> pb::InboxEntry {
    let mut out = pb::InboxEntry {
        accept_seq: e.accept_seq,
        handler: e.handler.clone(),
        payload: e.payload.map(ref_to_proto),
        accepted_unix_ns: e.accepted_unix_ns,
        failed_attempts: e.failed_attempts,
        last_failed_turn_id: e
            .last_failed_turn_id
            .map(|t| t.0.to_vec())
            .unwrap_or_default(),
        ..Default::default()
    };
    match &e.source {
        InputSource::Message {
            request_id,
            request_hash,
        } => {
            out.source = pb::InputSource::Message as i32;
            out.request_id = request_id.clone();
            out.request_hash = request_hash.0.to_vec();
        }
        InputSource::Timer {
            timer_id,
            generation,
        } => {
            out.source = pb::InputSource::Timer as i32;
            out.timer_id = timer_id.0.to_vec();
            out.timer_generation = *generation;
        }
    }
    out
}

pub(crate) fn inbox_from_proto(e: pb::InboxEntry) -> Result<InboxEntry, CodecError> {
    let source = match pb::InputSource::try_from(e.source) {
        Ok(pb::InputSource::Message) if e.timer_id.is_empty() && e.timer_generation == 0 => {
            InputSource::Message {
                request_hash: hash256(&e.request_hash, "inbox.request_hash")?,
                request_id: e.request_id,
            }
        }
        Ok(pb::InputSource::Timer) if e.request_id.is_empty() && e.request_hash.is_empty() => {
            InputSource::Timer {
                timer_id: id128(&e.timer_id, "inbox.timer_id")?,
                generation: e.timer_generation,
            }
        }
        _ => return Err(invalid("inbox.source")),
    };
    Ok(InboxEntry {
        accept_seq: e.accept_seq,
        handler: e.handler,
        source,
        payload: ref_from_proto(e.payload, "inbox.payload")?,
        accepted_unix_ns: e.accepted_unix_ns,
        failed_attempts: e.failed_attempts,
        last_failed_turn_id: if e.last_failed_turn_id.is_empty() {
            None
        } else {
            Some(id128(&e.last_failed_turn_id, "inbox.last_failed_turn_id")?)
        },
    })
}

pub(crate) fn receipt_to_proto(r: &Receipt) -> pb::Receipt {
    pb::Receipt {
        request_id: r.request_id.clone(),
        handler: r.handler.clone(),
        request_hash: r.request_hash.0.to_vec(),
        accept_seq: r.accept_seq,
    }
}

pub(crate) fn receipt_from_proto(r: pb::Receipt) -> Result<Receipt, CodecError> {
    Ok(Receipt {
        request_hash: hash256(&r.request_hash, "receipt.request_hash")?,
        request_id: r.request_id,
        handler: r.handler,
        accept_seq: r.accept_seq,
    })
}

pub(crate) fn result_to_proto(r: &ResultEntry) -> pb::ResultEntry {
    pb::ResultEntry {
        accept_seq: r.accept_seq,
        status: match r.status {
            ResultStatus::Completed => pb::ResultStatus::Completed,
            ResultStatus::Failed => pb::ResultStatus::Failed,
            ResultStatus::Cancelled => pb::ResultStatus::Cancelled,
        } as i32,
        output: r.output.map(ref_to_proto),
        error: r.error.clone(),
        turn_id: r.turn_id.map(|t| t.0.to_vec()).unwrap_or_default(),
        dead_lettered: r.dead_lettered,
        finished_unix_ns: r.finished_unix_ns,
    }
}

pub(crate) fn result_from_proto(r: pb::ResultEntry) -> Result<ResultEntry, CodecError> {
    Ok(ResultEntry {
        accept_seq: r.accept_seq,
        status: match pb::ResultStatus::try_from(r.status) {
            Ok(pb::ResultStatus::Completed) => ResultStatus::Completed,
            Ok(pb::ResultStatus::Failed) => ResultStatus::Failed,
            Ok(pb::ResultStatus::Cancelled) => ResultStatus::Cancelled,
            _ => return Err(invalid("result.status")),
        },
        output: ref_from_proto(r.output, "result.output")?,
        error: r.error,
        turn_id: if r.turn_id.is_empty() {
            None
        } else {
            Some(id128(&r.turn_id, "result.turn_id")?)
        },
        dead_lettered: r.dead_lettered,
        finished_unix_ns: r.finished_unix_ns,
    })
}

pub(crate) fn event_to_proto(e: &EventEntry) -> pb::EventEntry {
    pb::EventEntry {
        event_seq: e.event_seq,
        accept_seq: e.accept_seq,
        turn_id: e.turn_id.0.to_vec(),
        event_type: e.event_type.clone(),
        payload: e.payload.map(ref_to_proto),
    }
}

pub(crate) fn event_from_proto(e: pb::EventEntry) -> Result<EventEntry, CodecError> {
    Ok(EventEntry {
        event_seq: e.event_seq,
        accept_seq: e.accept_seq,
        turn_id: id128(&e.turn_id, "event.turn_id")?,
        event_type: e.event_type,
        payload: ref_from_proto(e.payload, "event.payload")?,
    })
}

pub(crate) fn timer_to_proto(t: &TimerEntry) -> pb::TimerEntry {
    pb::TimerEntry {
        timer_id: t.timer_id.0.to_vec(),
        timer_key: t.timer_key.clone(),
        generation: t.generation,
        due_unix_ns: t.due_unix_ns,
        handler: t.handler.clone(),
        payload: t.payload.map(ref_to_proto),
        status: match t.status {
            TimerStatus::Scheduled => pb::TimerStatus::Scheduled,
            TimerStatus::Delivered => pb::TimerStatus::Delivered,
            TimerStatus::Cancelled => pb::TimerStatus::Cancelled,
        } as i32,
    }
}

pub(crate) fn timer_from_proto(t: pb::TimerEntry) -> Result<TimerEntry, CodecError> {
    Ok(TimerEntry {
        timer_id: id128(&t.timer_id, "timer.timer_id")?,
        timer_key: t.timer_key,
        generation: t.generation,
        due_unix_ns: t.due_unix_ns,
        handler: t.handler,
        payload: ref_from_proto(t.payload, "timer.payload")?,
        status: match pb::TimerStatus::try_from(t.status) {
            Ok(pb::TimerStatus::Scheduled) => TimerStatus::Scheduled,
            Ok(pb::TimerStatus::Delivered) => TimerStatus::Delivered,
            Ok(pb::TimerStatus::Cancelled) => TimerStatus::Cancelled,
            _ => return Err(invalid("timer.status")),
        },
    })
}

pub(crate) fn step_to_proto(s: &StepRecord) -> pb::StepRecord {
    pb::StepRecord {
        step_id: s.step_id.0.to_vec(),
        step_key: s.step_key.clone(),
        tool_name: s.tool_name.clone(),
        tool_version: s.tool_version.clone(),
        input_hash: s.input_hash.0.to_vec(),
        status: match s.status {
            StepStatus::Started => pb::StepStatus::Started,
            StepStatus::Completed => pb::StepStatus::Completed,
        } as i32,
        output: s.output.map(ref_to_proto),
    }
}

pub(crate) fn step_from_proto(s: pb::StepRecord) -> Result<StepRecord, CodecError> {
    Ok(StepRecord {
        step_id: id128(&s.step_id, "step.step_id")?,
        step_key: s.step_key,
        tool_name: s.tool_name,
        tool_version: s.tool_version,
        input_hash: hash256(&s.input_hash, "step.input_hash")?,
        status: match pb::StepStatus::try_from(s.status) {
            Ok(pb::StepStatus::Started) => StepStatus::Started,
            Ok(pb::StepStatus::Completed) => StepStatus::Completed,
            _ => return Err(invalid("step.status")),
        },
        output: ref_from_proto(s.output, "step.output")?,
    })
}

fn policy_to_proto(p: RecoveryPolicy) -> i32 {
    let policy = match p {
        RecoveryPolicy::ProviderIdempotency => pb::RecoveryPolicy::ProviderIdempotency,
        RecoveryPolicy::AtLeastOnce => pb::RecoveryPolicy::AtLeastOnce,
        RecoveryPolicy::Manual => pb::RecoveryPolicy::Manual,
    };
    policy as i32
}

fn policy_from_proto(p: i32, field: &'static str) -> Result<RecoveryPolicy, CodecError> {
    match pb::RecoveryPolicy::try_from(p) {
        Ok(pb::RecoveryPolicy::ProviderIdempotency) => Ok(RecoveryPolicy::ProviderIdempotency),
        Ok(pb::RecoveryPolicy::AtLeastOnce) => Ok(RecoveryPolicy::AtLeastOnce),
        Ok(pb::RecoveryPolicy::Manual) => Ok(RecoveryPolicy::Manual),
        _ => Err(invalid(field)),
    }
}

pub(crate) fn effect_to_proto(e: &EffectRecord) -> pb::EffectRecord {
    pb::EffectRecord {
        effect_id: e.effect_id.0.to_vec(),
        step_key: e.step_key.clone(),
        tool_name: e.tool_name.clone(),
        tool_version: e.tool_version.clone(),
        input_hash: e.input_hash.0.to_vec(),
        policy: policy_to_proto(e.policy),
        status: match e.status {
            EffectStatus::Intent => pb::EffectStatus::Intent,
            EffectStatus::Authorized => pb::EffectStatus::Authorized,
            EffectStatus::InDoubt => pb::EffectStatus::InDoubt,
            EffectStatus::Completed => pb::EffectStatus::Completed,
        } as i32,
        attempt: e.attempt,
        result: e.result.map(ref_to_proto),
        result_attempt: e.result_attempt,
        turn_id: e.turn_id.0.to_vec(),
    }
}

pub(crate) fn effect_from_proto(e: pb::EffectRecord) -> Result<EffectRecord, CodecError> {
    Ok(EffectRecord {
        effect_id: id128(&e.effect_id, "effect.effect_id")?,
        step_key: e.step_key,
        tool_name: e.tool_name,
        tool_version: e.tool_version,
        input_hash: hash256(&e.input_hash, "effect.input_hash")?,
        policy: policy_from_proto(e.policy, "effect.policy")?,
        status: match pb::EffectStatus::try_from(e.status) {
            Ok(pb::EffectStatus::Intent) => EffectStatus::Intent,
            Ok(pb::EffectStatus::Authorized) => EffectStatus::Authorized,
            Ok(pb::EffectStatus::InDoubt) => EffectStatus::InDoubt,
            Ok(pb::EffectStatus::Completed) => EffectStatus::Completed,
            _ => return Err(invalid("effect.status")),
        },
        attempt: e.attempt,
        result: ref_from_proto(e.result, "effect.result")?,
        result_attempt: e.result_attempt,
        turn_id: id128(&e.turn_id, "effect.turn_id")?,
    })
}

fn outbox_to_proto(o: &OutboxIntent) -> pb::OutboxIntent {
    pb::OutboxIntent {
        effect_id: o.effect_id.0.to_vec(),
        intent_key: o.intent_key.clone(),
        tool_name: o.tool_name.clone(),
        tool_version: o.tool_version.clone(),
        policy: policy_to_proto(o.policy),
        input: o.input.map(ref_to_proto),
        turn_id: o.turn_id.0.to_vec(),
    }
}

fn outbox_from_proto(o: pb::OutboxIntent) -> Result<OutboxIntent, CodecError> {
    Ok(OutboxIntent {
        effect_id: id128(&o.effect_id, "outbox.effect_id")?,
        intent_key: o.intent_key,
        tool_name: o.tool_name,
        tool_version: o.tool_version,
        policy: policy_from_proto(o.policy, "outbox.policy")?,
        input: ref_from_proto(o.input, "outbox.input")?,
        turn_id: id128(&o.turn_id, "outbox.turn_id")?,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    fn object_ref() -> impl Strategy<Value = ObjectRef> {
        (any::<[u8; 32]>(), any::<u64>()).prop_map(|(h, size_bytes)| ObjectRef {
            hash: ObjectHash(h),
            size_bytes,
        })
    }

    fn id() -> impl Strategy<Value = Id128> {
        any::<[u8; 16]>().prop_map(Id128)
    }

    fn h256() -> impl Strategy<Value = Hash256> {
        any::<[u8; 32]>().prop_map(Hash256)
    }

    fn policy() -> impl Strategy<Value = RecoveryPolicy> {
        prop_oneof![
            Just(RecoveryPolicy::ProviderIdempotency),
            Just(RecoveryPolicy::AtLeastOnce),
            Just(RecoveryPolicy::Manual),
        ]
    }

    pub(crate) fn inbox_entry() -> impl Strategy<Value = InboxEntry> {
        (
            any::<u64>(),
            ".{0,8}",
            prop_oneof![
                (".{0,8}", h256()).prop_map(|(request_id, request_hash)| InputSource::Message {
                    request_id,
                    request_hash
                }),
                (id(), any::<u64>()).prop_map(|(timer_id, generation)| InputSource::Timer {
                    timer_id,
                    generation
                }),
            ],
            proptest::option::of(object_ref()),
            any::<i64>(),
            any::<u32>(),
            proptest::option::of(id()),
        )
            .prop_map(
                |(
                    accept_seq,
                    handler,
                    source,
                    payload,
                    accepted_unix_ns,
                    failed_attempts,
                    last_failed_turn_id,
                )| {
                    InboxEntry {
                        accept_seq,
                        handler,
                        source,
                        payload,
                        accepted_unix_ns,
                        failed_attempts,
                        last_failed_turn_id,
                    }
                },
            )
    }

    pub(crate) fn receipt() -> impl Strategy<Value = Receipt> {
        (".{0,8}", ".{0,8}", h256(), any::<u64>()).prop_map(
            |(request_id, handler, request_hash, accept_seq)| Receipt {
                request_id,
                handler,
                request_hash,
                accept_seq,
            },
        )
    }

    pub(crate) fn result_entry() -> impl Strategy<Value = ResultEntry> {
        (
            any::<u64>(),
            prop_oneof![
                Just(ResultStatus::Completed),
                Just(ResultStatus::Failed),
                Just(ResultStatus::Cancelled)
            ],
            proptest::option::of(object_ref()),
            ".{0,8}",
            proptest::option::of(id()),
            any::<bool>(),
            any::<i64>(),
        )
            .prop_map(
                |(accept_seq, status, output, error, turn_id, dead_lettered, finished_unix_ns)| {
                    ResultEntry {
                        accept_seq,
                        status,
                        output,
                        error,
                        turn_id,
                        dead_lettered,
                        finished_unix_ns,
                    }
                },
            )
    }

    pub(crate) fn event_entry() -> impl Strategy<Value = EventEntry> {
        (
            any::<u64>(),
            any::<u64>(),
            id(),
            ".{0,8}",
            proptest::option::of(object_ref()),
        )
            .prop_map(
                |(event_seq, accept_seq, turn_id, event_type, payload)| EventEntry {
                    event_seq,
                    accept_seq,
                    turn_id,
                    event_type,
                    payload,
                },
            )
    }

    pub(crate) fn timer_entry() -> impl Strategy<Value = TimerEntry> {
        (
            id(),
            ".{0,8}",
            any::<u64>(),
            any::<i64>(),
            ".{0,8}",
            proptest::option::of(object_ref()),
            prop_oneof![
                Just(TimerStatus::Scheduled),
                Just(TimerStatus::Delivered),
                Just(TimerStatus::Cancelled)
            ],
        )
            .prop_map(
                |(timer_id, timer_key, generation, due_unix_ns, handler, payload, status)| {
                    TimerEntry {
                        timer_id,
                        timer_key,
                        generation,
                        due_unix_ns,
                        handler,
                        payload,
                        status,
                    }
                },
            )
    }

    pub(crate) fn step_record() -> impl Strategy<Value = StepRecord> {
        (
            id(),
            ".{0,8}",
            ".{0,8}",
            ".{0,8}",
            h256(),
            prop_oneof![Just(StepStatus::Started), Just(StepStatus::Completed)],
            proptest::option::of(object_ref()),
        )
            .prop_map(
                |(step_id, step_key, tool_name, tool_version, input_hash, status, output)| {
                    StepRecord {
                        step_id,
                        step_key,
                        tool_name,
                        tool_version,
                        input_hash,
                        status,
                        output,
                    }
                },
            )
    }

    pub(crate) fn effect_record() -> impl Strategy<Value = EffectRecord> {
        (
            (id(), ".{0,8}", ".{0,8}", ".{0,8}", h256(), policy()),
            (
                prop_oneof![
                    Just(EffectStatus::Intent),
                    Just(EffectStatus::Authorized),
                    Just(EffectStatus::InDoubt),
                    Just(EffectStatus::Completed)
                ],
                any::<u32>(),
                proptest::option::of(object_ref()),
                any::<u32>(),
                id(),
            ),
        )
            .prop_map(
                |(
                    (effect_id, step_key, tool_name, tool_version, input_hash, policy),
                    (status, attempt, result, result_attempt, turn_id),
                )| EffectRecord {
                    effect_id,
                    step_key,
                    tool_name,
                    tool_version,
                    input_hash,
                    policy,
                    status,
                    attempt,
                    result,
                    result_attempt,
                    turn_id,
                },
            )
    }

    fn outbox_intent() -> impl Strategy<Value = OutboxIntent> {
        (
            id(),
            ".{0,8}",
            ".{0,8}",
            ".{0,8}",
            policy(),
            proptest::option::of(object_ref()),
            id(),
        )
            .prop_map(
                |(effect_id, intent_key, tool_name, tool_version, policy, input, turn_id)| {
                    OutboxIntent {
                        effect_id,
                        intent_key,
                        tool_name,
                        tool_version,
                        policy,
                        input,
                        turn_id,
                    }
                },
            )
    }

    fn active_turn() -> impl Strategy<Value = ActiveTurn> {
        (
            any::<u64>(),
            id(),
            any::<u64>(),
            ".{0,8}",
            ".{0,8}",
            h256(),
            any::<bool>(),
            any::<bool>(),
            any::<i64>(),
        )
            .prop_map(
                |(seq, turn_id, base, handler, version, digest, cancel, blocked, started)| {
                    ActiveTurn {
                        input_accept_seq: seq,
                        turn_id,
                        base_state_version: base,
                        handler,
                        handler_version: version,
                        handler_digest: digest,
                        cancellation: if cancel {
                            Cancellation::Requested
                        } else {
                            Cancellation::None
                        },
                        block: if blocked {
                            TurnBlock::NonDeterministicReplay
                        } else {
                            TurnBlock::None
                        },
                        started_unix_ns: started,
                    }
                },
            )
    }

    pub(crate) fn head() -> impl Strategy<Value = Head> {
        (
            (
                any::<[u8; 16]>(),
                "\\PC{1,16}",
                "\\PC{1,16}",
                any::<u64>(),
                any::<u64>(),
                ".{0,8}",
                any::<u64>(),
                any::<i64>(),
                any::<u64>(),
                proptest::option::of(object_ref()),
                any::<u64>(),
                proptest::option::of(active_turn()),
            ),
            (
                any::<u64>(),
                any::<u64>(),
                any::<bool>(),
                any::<[u64; 4]>(),
                vec(inbox_entry(), 0..3),
                vec(receipt(), 0..3),
                vec(result_entry(), 0..3),
                vec(event_entry(), 0..3),
                vec(timer_entry(), 0..3),
                vec(step_record(), 0..3),
                vec(effect_record(), 0..3),
                vec(outbox_intent(), 0..3),
            ),
        )
            .prop_map(
                |(
                    (
                        tenant,
                        kind,
                        key,
                        incarnation,
                        root_revision,
                        owner_id,
                        epoch,
                        lease,
                        state_version,
                        state,
                        next_accept_seq,
                        active_turn,
                    ),
                    (
                        timer_generation,
                        next_event_seq,
                        tombstoned,
                        quota,
                        inbox,
                        receipts,
                        results,
                        events,
                        timers,
                        steps,
                        effects,
                        outbox,
                    ),
                )| Head {
                    tenant: TenantHash(tenant),
                    kind: AgentKind::new(kind).expect("kind"),
                    key: AgentKey::new(key).expect("key"),
                    incarnation,
                    root_revision,
                    owner_id,
                    epoch,
                    owner_lease_hint_unix_ns: lease,
                    state_version,
                    state,
                    next_accept_seq,
                    active_turn,
                    timer_generation,
                    next_event_seq,
                    lifecycle: if tombstoned {
                        Lifecycle::Tombstoned
                    } else {
                        Lifecycle::Active
                    },
                    quota: Quota {
                        referenced_bytes: quota[0],
                        mailbox_depth: quota[1],
                        receipt_count: quota[2],
                        timer_count: quota[3],
                    },
                    inbox,
                    receipts,
                    results,
                    events,
                    timers,
                    steps,
                    effects,
                    outbox,
                },
            )
    }

    pub(crate) fn sample_head() -> Head {
        Head {
            tenant: TenantHash([7; 16]),
            kind: AgentKind::new("order").expect("kind"),
            key: AgentKey::new("o-1").expect("key"),
            incarnation: 1,
            root_revision: 1,
            owner_id: String::new(),
            epoch: 0,
            owner_lease_hint_unix_ns: 0,
            state_version: 0,
            state: None,
            next_accept_seq: 1,
            active_turn: None,
            timer_generation: 0,
            next_event_seq: 1,
            lifecycle: Lifecycle::Active,
            quota: Quota::default(),
            inbox: Vec::new(),
            receipts: Vec::new(),
            results: Vec::new(),
            events: Vec::new(),
            timers: Vec::new(),
            steps: Vec::new(),
            effects: Vec::new(),
            outbox: Vec::new(),
        }
    }

    #[test]
    fn the_seal_is_the_last_six_bytes() {
        let bytes = sample_head().encode();
        // key (2047 << 3 | 5) as a varint, then 0x52414731 little-endian.
        assert_eq!(
            bytes[bytes.len() - 6..],
            [0xfd, 0x7f, 0x31, 0x47, 0x41, 0x52]
        );
    }

    #[test]
    fn two_roots_differing_only_in_root_revision_encode_differently() {
        let a = sample_head();
        let mut b = a.clone();
        b.root_revision += 1;
        assert_ne!(a.encode(), b.encode());
        let mut c = a.clone();
        c.incarnation += 1;
        assert_ne!(a.encode(), c.encode());
    }

    #[test]
    fn a_newer_format_version_is_refused_before_the_rest_is_read() {
        let mut raw = sample_head().to_proto();
        raw.format_version = 2;
        let mut bytes = raw.encode_to_vec();
        assert_eq!(
            Head::decode(&bytes),
            Err(CodecError::UnsupportedFormat { found: 2 })
        );
        // Garbage after field 1 does not mask the version verdict.
        bytes.truncate(2);
        bytes.extend_from_slice(&[0xff, 0xff, 0xff]);
        assert_eq!(
            Head::decode(&bytes),
            Err(CodecError::UnsupportedFormat { found: 2 })
        );
        assert_eq!(
            CodecError::UnsupportedFormat { found: 2 }.kind(),
            Some(ErrorKind::UnsupportedFormat)
        );
    }

    #[test]
    fn paged_representation_and_page_refs_are_refused_by_version_one() {
        let mut raw = sample_head().to_proto();
        raw.representation = pb::IndexRepresentation::Paged as i32;
        assert_eq!(
            Head::decode(&raw.encode_to_vec()),
            Err(CodecError::UnsupportedRepresentation { found: 2 })
        );
        let mut raw = sample_head().to_proto();
        raw.inbox_page = Some(ref_to_proto(ObjectRef::of(b"page")));
        assert_eq!(
            Head::decode(&raw.encode_to_vec()),
            Err(CodecError::Invalid {
                field: "inbox_page"
            })
        );
    }

    #[test]
    fn missing_or_wrong_seal_is_typed() {
        let mut raw = sample_head().to_proto();
        raw.seal = 0;
        assert_eq!(
            Head::decode(&raw.encode_to_vec()),
            Err(CodecError::Truncated)
        );
        raw.seal = 1;
        assert_eq!(
            Head::decode(&raw.encode_to_vec()),
            Err(CodecError::BadSeal(1))
        );
        assert_eq!(Head::decode(&[]), Err(CodecError::MissingFormatVersion));
    }

    #[test]
    fn a_repeated_format_version_field_is_gated_on_its_decoded_value() {
        // Field 1 written twice: the leading 1 passes the peek, and protobuf
        // keeps the last value, 2.
        let mut bytes = sample_head().encode();
        let seal = bytes.split_off(bytes.len() - 6);
        bytes.extend_from_slice(&[0x08, 0x02]);
        bytes.extend_from_slice(&seal);
        assert_eq!(peek_format_version(&bytes), Ok(FORMAT_VERSION));
        assert_eq!(
            Head::decode(&bytes),
            Err(CodecError::UnsupportedFormat { found: 2 })
        );
    }

    #[test]
    fn a_format_version_varint_above_u32_is_refused_before_decode() {
        // 2^32 + 1 as a varint: prost decodes a uint32 field by truncating
        // the u64, so a full decode alone reads this as version 1.
        let mut above = Vec::new();
        prost::encoding::encode_varint((1u64 << 32) + 1, &mut above);
        let truncated = {
            let mut probe = vec![0x08];
            probe.extend_from_slice(&above);
            pb::AgentHead::decode(probe.as_slice())
                .expect("prost accepts it")
                .format_version
        };
        assert_eq!(truncated, 1, "prost truncates the varint");
        let refused = CodecError::UnsupportedFormat { found: u32::MAX };
        // The only occurrence, leading.
        let mut lone = sample_head().encode();
        lone.splice(0..2, [0x08].into_iter().chain(above.iter().copied()));
        assert_eq!(peek_format_version(&lone), Err(refused.clone()));
        assert_eq!(Head::decode(&lone), Err(refused.clone()));
        // A later occurrence behind a leading 1, where it is the value
        // protobuf keeps: the leading peek passes it, and the full gate
        // refuses it before decode.
        let mut repeated = sample_head().encode();
        let seal = repeated.split_off(repeated.len() - 6);
        repeated.push(0x08);
        repeated.extend_from_slice(&above);
        repeated.extend_from_slice(&seal);
        assert_eq!(peek_format_version(&repeated), Ok(FORMAT_VERSION));
        assert_eq!(gate_every_format_version(&repeated), Err(refused.clone()));
        assert_eq!(Head::decode(&repeated), Err(refused));
        // Field 1 under another wire type is corrupt, not a version.
        assert!(matches!(
            gate_every_format_version(&[0x0d, 1, 0, 0, 0]),
            Err(CodecError::Decode(_))
        ));
    }

    #[test]
    fn an_unknown_field_within_version_1_is_dropped_on_re_encode() {
        // A v1 reader accepts a field it does not know inside a v1 record and
        // drops it on re-encode, which is why every additive change to
        // AgentHead bumps format_version (ADR-0066 R1): only the version gate
        // keeps a CAS write from losing a newer writer's field.
        let head = sample_head();
        let mut bytes = head.encode();
        // field 1000, varint 1, inserted before the seal
        let seal = bytes.split_off(bytes.len() - 6);
        bytes.extend_from_slice(&[0xc0, 0x3e, 0x01]);
        bytes.extend_from_slice(&seal);
        let decoded = Head::decode(&bytes).expect("decodes");
        assert_eq!(decoded, head);
        assert_eq!(decoded.encode(), head.encode());
        assert_ne!(decoded.encode(), bytes);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        // decode(encode(h)) == h makes encode injective: equal bytes for two
        // heads would decode to one head.
        #[test]
        fn head_round_trips(h in head()) {
            prop_assert_eq!(Head::decode(&h.encode()), Ok(h));
        }

        #[test]
        fn every_strict_prefix_is_a_typed_error(h in head(), cut in any::<prop::sample::Index>()) {
            let bytes = h.encode();
            let len = cut.index(bytes.len());
            prop_assert!(Head::decode(&bytes[..len]).is_err());
        }

        #[test]
        fn corrupt_bytes_never_panic(
            h in head(),
            flips in vec((any::<prop::sample::Index>(), any::<u8>()), 1..4),
        ) {
            let mut bytes = h.encode();
            for (at, value) in flips {
                let i = at.index(bytes.len());
                bytes[i] = value;
            }
            let _ = Head::decode(&bytes);
        }
    }
}
