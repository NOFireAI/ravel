//! The record races and the unfolded-record sweep (README.md, "The forced
//! races"): which bucket each one works on, when a held server PUT is fresh
//! enough to race, what a record race changes in the store before the
//! release and how it undoes that, and which of a compaction record's inputs
//! the store holds.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ravel_antithesis_catalog_driver::keys::{self, RecordKey};
use ravel_antithesis_catalog_driver::proxy::{self, HOLD_CAP};

use crate::audit::{DATA_SUFFIX, InputKeys};
use crate::token::Token;

/// The driver releases a held record PUT once it is this old, whether or not
/// the `ravel-cli` compaction has published by then: before the proxy's own
/// cap, so the release, not the cap, decides when the PUT is sent.
pub const RELEASE_BY: Duration = Duration::from_secs(14);
const _: () = assert!(RELEASE_BY.as_secs() + 2 < HOLD_CAP.as_secs());

/// In a record race that tampers, the `ravel-cli` compaction must have
/// finished by the time the held PUT is this old, or the race tampers with
/// nothing. The tamper's raw S3 requests and the release then have the rest
/// of the hold cap, so the release, not the cap, still sends the PUT.
pub const TAMPER_BY: Duration = Duration::from_secs(10);
const _: () = assert!(TAMPER_BY.as_secs() + 8 <= HOLD_CAP.as_secs());

/// How long after the store answered the released PUT the driver waits
/// before it undoes its tamper once faults stopped: three of the servers'
/// 20 s store request timeouts, so the loser's GET, HEAD and second GET of
/// the record have been answered before the record's version changes again.
pub const RESTORE_AFTER: Duration = Duration::from_secs(3 * crate::clock::STORE_REQUEST_TIMEOUT_S);

/// The same wait while faults may run: the servers' store client retries one
/// operation for up to 180 s plus one 20 s request (`retry_timeout +
/// request_timeout` in crates/ravel-object-store/src/s3.rs), so one of the
/// loser's requests retried to that bound is still answered before the
/// undo. Three retried to it would not be; README.md, "The forced races",
/// names that residual.
pub const RESTORE_AFTER_FAULTS: Duration = Duration::from_secs(200);

/// How long the driver keeps starting undo attempts the store refused
/// before it leaves the undo pending for the next clock move or the
/// eventually.
pub const UNDO_FOR: Duration = Duration::from_secs(60);

/// How long a tampering race waits for a server's held record PUT after
/// the nudge; the plain record race waits 120 s. Servers send one within a
/// second of the nudge in practice.
pub const TAMPER_HELD_WAIT: Duration = Duration::from_secs(45);

/// One raw S3 request at its bound: curl's `--max-time` (`s3::MAX_TIME_S`).
const S3_S: u64 = crate::s3::MAX_TIME_S;
/// One proxy control command at its bound: the 2 s connect and 5 s read
/// timeouts of `proxy::control`.
const PROXY_CMD_S: u64 = 7;
/// `clock::reach_wait_s`: one reread interval plus a second. Also the most
/// two processes' faked clocks differ by.
const REACH_WAIT_S: u64 = crate::clock::FAKETIME_CACHE_DURATION_S + 1;
/// One round of `Shared::all_nodes_serve` at its bound, which the settle's
/// wait can start just before its 120 s are up: two ravel nodes' readiness
/// (2 s each) and SQL (`READ_TIMEOUT`, 30 s, each), two maintenance nodes'
/// readiness (2 s each), and the 2 s pause between rounds.
const SERVE_ROUND_S: u64 = 2 * (2 + crate::READ_TIMEOUT.as_secs()) + 2 * 2 + 2;

/// From the nudge's write of the clock offset to the tamper, every term at
/// its bound.
pub const BEFORE_TAMPER_S: u64 =
    // shift_clock: wait for the move to reach the driver, one reread plus 30 s.
    (crate::clock::FAKETIME_CACHE_DURATION_S + 30)
    // shift_clock: its reach wait.
    + REACH_WAIT_S
    // shift_clock: the GET of HEAD after the move (`head_read`).
    + S3_S
    // settle: its reach wait.
    + REACH_WAIT_S
    // settle: the 120 s `wait_serving`, and one serving round past it.
    + 120 + SERVE_ROUND_S
    // `hold-record`.
    + PROXY_CMD_S
    // The held wait, and one poll round past it: `held`, `release`, the
    // 0.5 s pause, `hold-record`, the 0.25 s pause.
    + TAMPER_HELD_WAIT.as_secs() + 3 * PROXY_CMD_S + 1
    // The compaction's budget, measured from the held PUT's age.
    + TAMPER_BY.as_secs()
    // The GET of the CLI's record before the tamper.
    + S3_S;

/// From the tamper's start to the first undo attempt, every term at its
/// bound.
pub const TAMPER_TAIL_S: u64 =
    // The tamper: the part's GET and DELETE (a forged record is one PUT).
    2 * S3_S
    // The GET that checks the tamper is in place.
    + S3_S
    // `release`.
    + PROXY_CMD_S
    // `held_result_of`: `HELD_RESULT_WAIT`, one command past it, the 0.1 s
    // pause.
    + crate::HELD_RESULT_WAIT.as_secs() + PROXY_CMD_S + 1
    // The 2 s for the server's handling of the store's answer.
    + 2
    // The GET of the record after the release.
    + S3_S
    // The wait before the undo while faults may run.
    + RESTORE_AFTER_FAULTS.as_secs();

/// One undo attempt at its bound: the GET of the key and the PUT back.
pub const UNDO_ATTEMPT_S: u64 = 2 * S3_S;

/// Seconds from the nudge until the bucket's hour seals for the fold: the
/// nudge lands the clocks `SEAL_SLACK_S` past the compaction seal, which is
/// `FOLD_SAFETY_MARGIN_S` before the fold seal.
pub const FOLD_SEAL_AFTER_NUDGE_S: u64 =
    crate::clock::FOLD_SAFETY_MARGIN_S - crate::clock::SEAL_SLACK_S;

/// The whole tampering race, every term at its bound.
pub const TAMPER_RACE_S: u64 =
    // From the nudge to the tamper.
    BEFORE_TAMPER_S
    // From the tamper to the first undo attempt.
    + TAMPER_TAIL_S
    // The undo's retries: attempts start for `UNDO_FOR`, and the last one
    // started can take a whole attempt past it.
    + UNDO_FOR.as_secs() + UNDO_ATTEMPT_S
    // A server's faked clock can run this far ahead of the driver's.
    + REACH_WAIT_S;
const _: () = assert!(TAMPER_RACE_S <= FOLD_SEAL_AFTER_NUDGE_S);

/// The unix second the bucket hour `hour` seals for the fold
/// (`clock::seal_margin_s` after its end).
pub fn fold_seal_s(hour: u64) -> u64 {
    (hour + 1) * 3600 + crate::clock::seal_margin_s()
}

/// The last second of the driver's clock at which an undo attempt on a
/// tamper in `hour` may start and still finish before any server's clock
/// reaches the hour's fold seal.
pub fn undo_by_s(hour: u64) -> u64 {
    fold_seal_s(hour).saturating_sub(UNDO_ATTEMPT_S + REACH_WAIT_S)
}

/// Whether a race may tamper with a record of `hour` at `now_s`: the rest
/// of the race at its bounds still ends before [`undo_by_s`]. The race runs
/// no tamper otherwise, so a slow run up to the tamper never carries one
/// past the fold seal.
pub fn tamper_fits(now_s: u64, hour: u64) -> bool {
    now_s + TAMPER_TAIL_S <= undo_by_s(hour)
}

/// The wait before the undo: [`restore_after`], cut short so the first
/// undo attempt still starts by [`undo_by_s`].
pub fn restore_wait(now_s: u64, hour: u64, faults_may_run: bool) -> Duration {
    restore_after(faults_may_run).min(Duration::from_secs(undo_by_s(hour).saturating_sub(now_s)))
}

/// What the undo of a tamper in bucket hour `hour` does next, at `now_s`,
/// after `attempts` refused attempts that started `tried` ago.
#[derive(Debug, PartialEq, Eq)]
pub enum UndoStep {
    /// Start an attempt. `overran` when it starts past [`undo_by_s`], so it
    /// may finish after the fold seal: the cap fires, and the undo still
    /// runs, since giving it up would leave the tamper in the store.
    Attempt { overran: bool },
    /// Stop retrying: at least one attempt ran, and [`UNDO_FOR`] is up.
    GiveUp,
}

/// See [`UndoStep`]. The first attempt always starts.
pub fn undo_step(now_s: u64, hour: u64, tried: Duration, attempts: u32) -> UndoStep {
    if attempts > 0 && tried >= UNDO_FOR {
        return UndoStep::GiveUp;
    }
    UndoStep::Attempt {
        overran: now_s > undo_by_s(hour),
    }
}

/// Seconds of the driver's clock from the nudge to `now_s`, against the
/// [`FOLD_SEAL_AFTER_NUDGE_S`] budget: the nudge puts the clocks that long
/// before the fold seal of `hour`.
pub fn race_elapsed_s(now_s: u64, hour: u64) -> i64 {
    now_s as i64 - (fold_seal_s(hour) - FOLD_SEAL_AFTER_NUDGE_S) as i64
}

/// Whether the record a loser met changed after the release: `after`, read
/// 2 s after it, against what was in place at the release, the forged
/// record in a divergent-record race and the CLI's record otherwise.
/// `None` when either read failed or found nothing.
pub fn record_changed(
    before: Option<&[u8]>,
    after: Option<&[u8]>,
    undo: Option<&Undo>,
) -> Option<bool> {
    let at_release = match undo {
        Some(Undo::Record { forged, .. }) => Some(forged.as_slice()),
        _ => before,
    };
    Some(after? != at_release?)
}

/// The wait before the undo: [`RESTORE_AFTER_FAULTS`] while faults may run,
/// [`RESTORE_AFTER`] once they stopped.
pub fn restore_after(faults_may_run: bool) -> Duration {
    if faults_may_run {
        RESTORE_AFTER_FAULTS
    } else {
        RESTORE_AFTER
    }
}

/// What a record race does to the store after the `ravel-cli` compaction
/// published and before the driver releases the held server PUT.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tamper {
    /// Nothing: the plain record race.
    None,
    /// Rewrite the record with its input-set hash changed after the 8
    /// bytes its key carries ([`diverge`]).
    Diverge,
    /// Delete one part the record names ([`part_to_delete`]).
    DeletePart,
}

impl Tamper {
    /// The race's name in log lines.
    pub fn race(self) -> &'static str {
        match self {
            Tamper::None => "record race",
            Tamper::Diverge => "divergent-record race",
            Tamper::DeletePart => "missing-part race",
        }
    }
}

/// The fields of `CompactionRecord` (proto/ravel/commit.proto) a tamper
/// reads; prost skips the rest.
#[derive(Clone, PartialEq, prost::Message)]
struct CompactionRecord {
    #[prost(bytes = "vec", tag = "8")]
    input_set_hash: Vec<u8>,
    #[prost(message, repeated, tag = "9")]
    parts: Vec<CompactionPart>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CompactionPart {
    #[prost(uint32, tag = "1")]
    part_index: u32,
    #[prost(bytes = "vec", tag = "4")]
    content_hash: Vec<u8>,
}

/// `input_set_hash` (field 8, wire type 2) framed as protobuf writes a
/// 32-byte value: its key byte and its length.
const HASH_FRAME: [u8; 2] = [(8 << 3) | 2, 32];

/// The record at `record_key` decoded, with the 16 hex digits its key names
/// checked against the first 8 bytes of its 32-byte input-set hash.
fn decode_record(record_key: &str, record: &[u8]) -> Result<CompactionRecord, String> {
    use prost::Message;
    keys::parse_record_key(record_key)
        .ok_or_else(|| format!("{record_key} is not a compaction record key"))?;
    let hash16 = record_key
        .rsplit_once("/l1.")
        .and_then(|(_, name)| name.strip_suffix(".cmt"))
        .ok_or_else(|| format!("{record_key} names no input-set hash"))?;
    let decoded = CompactionRecord::decode(record)
        .map_err(|e| format!("{record_key} does not decode: {e}"))?;
    if decoded.input_set_hash.len() != 32 {
        return Err(format!(
            "{record_key}: the input-set hash is {} bytes, not 32",
            decoded.input_set_hash.len()
        ));
    }
    if !hex(&decoded.input_set_hash[..8]).eq_ignore_ascii_case(hash16) {
        return Err(format!(
            "{record_key}: the input-set hash does not start with the key's {hash16}"
        ));
    }
    Ok(decoded)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The keys of the parts the compaction record `record` at `record_key`
/// names, by part index: `<tenant>/<signal>/l1/<shard:04>/<hour>/
/// <input_set_hash16>.<part:04>.<content_hash16>.rseg`, as `l1_part_key`
/// in crates/ravel-commit/src/keys.rs builds them. `Err` when the key is not
/// a compaction record key, the bytes do not decode, the hash does not match
/// the key, or a part's content hash is not 32 bytes.
pub fn winner_parts(record_key: &str, record: &[u8]) -> Result<Vec<String>, String> {
    let decoded = decode_record(record_key, record)?;
    let rk = keys::parse_record_key(record_key)
        .ok_or_else(|| format!("{record_key} is not a compaction record key"))?;
    let dir = format!(
        "{}{}/l1/{:04}/{}/",
        rk.tenant_prefix,
        rk.signal,
        rk.shard,
        keys::hour_string(rk.hour)
    );
    let input16 = hex(&decoded.input_set_hash[..8]);
    let mut parts = decoded.parts;
    parts.sort_by_key(|p| p.part_index);
    parts
        .iter()
        .map(|p| {
            if p.part_index > 9999 {
                return Err(format!(
                    "{record_key}: part index {} does not fit the key's 4 digits",
                    p.part_index
                ));
            }
            if p.content_hash.len() != 32 {
                return Err(format!(
                    "{record_key}: part {} has a {}-byte content hash",
                    p.part_index,
                    p.content_hash.len()
                ));
            }
            Ok(format!(
                "{dir}{input16}.{:04}.{}.rseg",
                p.part_index,
                hex(&p.content_hash[..8])
            ))
        })
        .collect()
}

/// The part a missing-part race deletes: the record's first part.
pub fn part_to_delete(record_key: &str, record: &[u8]) -> Result<String, String> {
    winner_parts(record_key, record)?
        .into_iter()
        .next()
        .ok_or_else(|| format!("{record_key} names no part"))
}

/// The record's bytes with the last byte of its input-set hash flipped, and
/// nothing else changed, so no field it does not know is lost to a
/// re-encode. The first 8 bytes of the hash, which the key carries, stay, so
/// the record still verifies against its key, and a compaction of the same
/// inputs now finds a winner whose hash differs from its own. `Err` unless
/// the record decodes, its hash matches the key, and the framed hash occurs
/// exactly once in the bytes.
pub fn diverge(record_key: &str, record: &[u8]) -> Result<Vec<u8>, String> {
    let decoded = decode_record(record_key, record)?;
    let mut framed = HASH_FRAME.to_vec();
    framed.extend_from_slice(&decoded.input_set_hash);
    let at: Vec<usize> = record
        .windows(framed.len())
        .enumerate()
        .filter(|(_, w)| *w == framed.as_slice())
        .map(|(i, _)| i)
        .collect();
    let [start] = at.as_slice() else {
        return Err(format!(
            "{record_key}: the framed input-set hash occurs {} times, not once",
            at.len()
        ));
    };
    let mut forged = record.to_vec();
    forged[start + framed.len() - 1] ^= 0xff;
    Ok(forged)
}

/// What a tamper changed, so the driver can tell whether it was in place at
/// the release and undo it once the loser resolved.
#[derive(Debug, PartialEq, Eq)]
pub enum Undo {
    /// The record at `key` was overwritten with `forged`; `original` is the
    /// `ravel-cli` compaction's.
    Record {
        key: String,
        original: Vec<u8>,
        forged: Vec<u8>,
    },
    /// The part at `key` was deleted; `bytes` are its content.
    Part { key: String, bytes: Vec<u8> },
}

impl Undo {
    /// The key the tamper changed.
    pub fn key(&self) -> &str {
        match self {
            Undo::Record { key, .. } | Undo::Part { key, .. } => key,
        }
    }

    /// Whether the key holds what the tamper left (`current`, `None` when
    /// the key is absent): the forged record, or no part.
    pub fn in_place(&self, current: Option<&[u8]>) -> bool {
        match self {
            Undo::Record { forged, .. } => current == Some(forged.as_slice()),
            Undo::Part { .. } => current.is_none(),
        }
    }

    /// The bytes to put back at [`Undo::key`] when it holds `current`; `None`
    /// when the tamper is no longer in place. A record a loser overwrote
    /// stays: the `mo-diverge-overwrites-record` mutant PUT its own record,
    /// whose parts that loser had PUT before it.
    pub fn restore(&self, current: Option<&[u8]>) -> Option<&[u8]> {
        if !self.in_place(current) {
            return None;
        }
        Some(match self {
            Undo::Record { original, .. } => original,
            Undo::Part { bytes, .. } => bytes,
        })
    }
}

/// A held PUT older than this when the driver first sees it leaves too
/// little of [`RELEASE_BY`] for the compaction; the driver releases it and
/// holds the next one.
pub const MAX_HELD_AGE: Duration = Duration::from_secs(6);

/// The `(hour, shard)` of the first of `hours` whose bucket in some shard a
/// compaction would publish a record for, from a LIST of the tenant's logs
/// commit keys.
pub fn compactable_bucket(
    tenant_prefix: &str,
    hours: impl IntoIterator<Item = u64>,
    shards: u32,
    listed: &BTreeSet<String>,
) -> Option<(u64, u32)> {
    for hour in hours {
        for shard in 0..shards {
            let prefix = keys::bucket_prefix(tenant_prefix, shard, hour);
            let under = listed
                .range(prefix.clone()..)
                .take_while(|k| k.starts_with(&prefix));
            if keys::bucket(&prefix, under).compactable() {
                return Some((hour, shard));
            }
        }
    }
    None
}

/// The first of `writes` (a write id and its decoded tokens) with a token
/// whose commit record its bucket's LIST in `listings` (keyed by
/// [`keys::bucket_prefix`]) holds, in a bucket a compaction would publish a
/// record for. That bucket has no compaction record yet, so the one a
/// compaction of it publishes is created after every clock move before the
/// LIST. Returns the write id and the bucket's `(hour, shard)`.
pub fn uncompacted_input(
    tenant_prefix: &str,
    writes: &[(u64, Vec<Token>)],
    listings: &BTreeMap<String, BTreeSet<String>>,
) -> Option<(u64, u64, u32)> {
    writes.iter().find_map(|(id, tokens)| {
        tokens.iter().find_map(|t| {
            let hour = u64::from(t.hour);
            let prefix = keys::bucket_prefix(tenant_prefix, t.shard, hour);
            let listed = listings.get(&prefix)?;
            (listed.contains(&t.commit_key(tenant_prefix))
                && keys::bucket(&prefix, listed).compactable())
            .then_some((*id, hour, t.shard))
        })
    })
}

/// What the proxy's `held` reply means for the record race.
#[derive(Debug, PartialEq, Eq)]
pub enum Held {
    /// Nothing is held yet.
    Nothing,
    /// A logs compaction record of the tenant, fresh enough to race.
    Race {
        key: String,
        record: RecordKey,
        age: Duration,
    },
    /// Anything else held: release it and hold the next.
    Skip(String),
}

/// Judge one `held` reply for the tenant at `tenant_prefix`.
pub fn judge_held(reply: &str, tenant_prefix: &str) -> Held {
    let Some((key, age)) = proxy::parse_held(reply) else {
        return Held::Nothing;
    };
    match keys::parse_record_key(&key) {
        Some(record)
            if record.tenant_prefix == tenant_prefix
                && record.signal == "l"
                && age <= MAX_HELD_AGE =>
        {
            Held::Race { key, record, age }
        }
        _ => Held::Skip(key),
    }
}

/// The status the store answered the held PUT of `held_key` with, from the
/// proxy's `held-result`; `None` when that names another key or none.
pub fn held_status(held_key: &str, held_result: Option<&(String, u16)>) -> Option<u16> {
    held_result
        .filter(|(key, _)| key == held_key)
        .map(|(_, status)| *status)
}

/// Whether a server's record PUT lost to the `ravel-cli` compaction, judged
/// from the store: the CLI's record was in the store before the release, the
/// driver released the held PUT before the proxy's cap forwarded it on its
/// own (`released_at_age`, `None` when the release failed), and the store
/// refused the held PUT as a create of a key that exists (`412`, or `409`).
/// A `403` (a request signed on a clock the store no longer agrees with), a
/// `5xx` or a `200` never reached `AlreadyExists` in the server.
pub fn record_lost(
    published_before_release: bool,
    released_at_age: Option<Duration>,
    held_status: Option<u16>,
) -> bool {
    published_before_release
        && released_at_age.is_some_and(|age| age <= HOLD_CAP)
        && matches!(held_status, Some(412) | Some(409))
}

/// Whether a record race that tampers reached its situation: the server's
/// PUT lost as [`record_lost`] judges it, and a read just before the release
/// found the tamper in place ([`Undo::in_place`]), so the loser that the
/// store's refusal sends to the winner record finds the forged record, or
/// the record without one of its parts.
pub fn tampered_lost(
    published_before_release: bool,
    released_at_age: Option<Duration>,
    held_status: Option<u16>,
    tamper_in_place: bool,
) -> bool {
    tamper_in_place && record_lost(published_before_release, released_at_age, held_status)
}

/// How long the `ravel-cli` compaction may run before a PUT held for `age`
/// is released, or, in a race that tampers, before the tamper.
pub fn cli_budget(age: Duration, tamper: Tamper) -> Duration {
    match tamper {
        Tamper::None => RELEASE_BY,
        Tamper::Diverge | Tamper::DeletePart => TAMPER_BY,
    }
    .saturating_sub(age)
}

/// The keys of `inputs` that `listed` holds: each input's commit record and
/// every data object under its data prefix.
pub fn present_inputs(inputs: &[InputKeys], listed: &BTreeSet<String>) -> Vec<String> {
    let mut present = Vec::new();
    for input in inputs {
        if listed.contains(&input.commit) {
            present.push(input.commit.clone());
        }
        present.extend(
            listed
                .range(input.data_prefix.clone()..)
                .take_while(|k| k.starts_with(&input.data_prefix))
                .filter(|k| k.ends_with(DATA_SUFFIX))
                .cloned(),
        );
    }
    present
}

/// The keys of `inputs` deleted since `before`, the LIST taken just before
/// a jump: held by `before` ([`present_inputs`]) and absent from `listed`, a
/// later LIST. An input already gone at `before`, deleted earlier in the
/// timeline, never counts.
pub fn deleted_since(
    inputs: &[InputKeys],
    before: &BTreeSet<String>,
    listed: &BTreeSet<String>,
) -> Vec<String> {
    present_inputs(inputs, before)
        .into_iter()
        .filter(|k| !listed.contains(k))
        .collect()
}

/// Whether the unfolded-record sweep reached what it exists for: HEAD held
/// the bytes it held before the compaction for the whole sequence, so no
/// fold folded the record, the record is still there, and the store held
/// its inputs before the clocks moved.
pub fn unfolded_reached(
    head_before: &[u8],
    head_after: Option<&[u8]>,
    record_present: bool,
    inputs_before: usize,
) -> bool {
    head_after == Some(head_before) && record_present && inputs_before > 0
}

/// Whether HEAD stayed unreadable through the unreadable-HEAD sweep: after
/// the jump past the protection horizon and the wait for the servers' sweeps,
/// HEAD still held the garbage the driver wrote. With server HEAD PUTs
/// refused this holds whether or not a server swept; that a server's sweep
/// read the undecodable HEAD is the SUT's own "delete pass refused on an
/// unreadable HEAD", which local-run.sh requires beside this one.
pub fn unreadable_head_reached(head_after: Option<&[u8]>, garbage: &[u8]) -> bool {
    head_after == Some(garbage)
}

/// The catalog snapshot prefix `t/<tenant>/catalog/<signal>/snap/` beside a
/// HEAD key `t/<tenant>/catalog/<signal>/HEAD`.
pub fn snap_prefix(head_key: &str) -> Option<String> {
    let dir = head_key.strip_suffix("/HEAD")?;
    dir.contains("/catalog/").then(|| format!("{dir}/snap/"))
}

/// The keys of `before` under `prefix` that `after` does not hold.
pub fn deleted_under(
    prefix: &str,
    before: &BTreeSet<String>,
    after: &BTreeSet<String>,
) -> Vec<String> {
    before
        .range(prefix.to_string()..)
        .take_while(|k| k.starts_with(prefix))
        .filter(|k| !after.contains(*k))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREFIX: &str = "t/0a0b/";
    const HOUR: u64 = 1_794_931_200 / 3600;

    fn listing(keys: &[String]) -> BTreeSet<String> {
        keys.iter().cloned().collect()
    }

    fn key(shard: u32, hour: u64, name: &str) -> String {
        format!("{}{name}", keys::bucket_prefix(PREFIX, shard, hour))
    }

    #[test]
    fn the_first_compactable_bucket_is_picked_in_hour_order() {
        let listed = listing(&[
            key(0, HOUR, "w1.1.00000000000000000001.cmt"),
            key(2, HOUR, "w1.1.00000000000000000002.cmt"),
            key(2, HOUR, "w2.1.00000000000000000001.cmt"),
            key(1, HOUR + 1, "w1.1.00000000000000000003.cmt"),
            key(1, HOUR + 1, "w1.1.00000000000000000004.cmt"),
        ]);
        assert_eq!(
            compactable_bucket(PREFIX, [HOUR, HOUR + 1], 4, &listed),
            Some((HOUR, 2))
        );
        assert_eq!(
            compactable_bucket(PREFIX, [HOUR + 1, HOUR], 4, &listed),
            Some((HOUR + 1, 1))
        );
        // A shard past the count is not looked at.
        assert_eq!(compactable_bucket(PREFIX, [HOUR], 2, &listed), None);
        // A bucket with a record already is not a candidate.
        let mut done = listed.clone();
        done.insert(key(2, HOUR, "l1.0011223344556677.cmt"));
        assert_eq!(compactable_bucket(PREFIX, [HOUR], 4, &done), None);
        // Another tenant's keys do not count.
        assert_eq!(compactable_bucket("t/ffff/", [HOUR], 4, &listed), None);
    }

    #[test]
    fn the_uncompacted_input_is_a_written_commit_record_in_a_bucket_without_a_record() {
        const W1: &str = "00000000-0000-0000-0000-000000000001";
        const W2: &str = "00000000-0000-0000-0000-000000000002";
        let token = |shard: u32, writer: &str, seq: u64| Token {
            shard,
            writer: writer.to_string(),
            epoch: 1,
            seq,
            hour: HOUR as u32,
        };
        let listings = |keys: &[String]| {
            let mut by_bucket: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            for shard in 0..4 {
                by_bucket.insert(keys::bucket_prefix(PREFIX, shard, HOUR), BTreeSet::new());
            }
            for k in keys {
                if let Some((bucket, _)) = k.rsplit_once('/') {
                    by_bucket
                        .entry(format!("{bucket}/"))
                        .or_default()
                        .insert(k.clone());
                }
            }
            by_bucket
        };
        // Write 7 has inputs in shards 0 and 2, write 8 in shard 2 only.
        let a = token(0, W1, 1);
        let b = token(2, W1, 2);
        let c = token(2, W2, 1);
        let writes = vec![(7, vec![a.clone(), b.clone()]), (8, vec![c.clone()])];
        // Shard 0 holds one commit record, too few to compact; shard 2 holds
        // two, so write 7's second token is the first that qualifies.
        let mut listed = vec![
            a.commit_key(PREFIX),
            b.commit_key(PREFIX),
            c.commit_key(PREFIX),
        ];
        assert_eq!(
            uncompacted_input(PREFIX, &writes, &listings(&listed)),
            Some((7, HOUR, 2))
        );
        // Once shard 2 has a compaction record, its inputs may predate a
        // clock move, so no write qualifies.
        listed.push(key(2, HOUR, "l1.0011223344556677.cmt"));
        assert_eq!(uncompacted_input(PREFIX, &writes, &listings(&listed)), None);
        // A compactable bucket that does not hold the token's own commit
        // record (another write's two records) gives that token nothing.
        let others = vec![
            a.commit_key(PREFIX),
            key(0, HOUR, &format!("{W2}.1.00000000000000000009.cmt")),
        ];
        let only_a = vec![(9, vec![token(0, W1, 5)])];
        assert_eq!(uncompacted_input(PREFIX, &only_a, &listings(&others)), None);
        assert_eq!(
            uncompacted_input(PREFIX, &[(9, vec![a.clone()])], &listings(&others)),
            Some((9, HOUR, 0))
        );
        // A bucket with no LIST (it failed) gives nothing.
        assert_eq!(uncompacted_input(PREFIX, &writes, &BTreeMap::new()), None);
    }

    #[test]
    fn the_fresh_compaction_nudge_stays_inside_the_protection_horizon() {
        // The eventually's fresh compaction seals the bucket of a write it
        // just acknowledged: the current hour, or the one before for a write
        // that crossed the hour boundary. That bucket is not sealed before
        // the nudge, so no record of it predates the move; and the move is
        // far below the horizon, so even one that did would keep its inputs.
        let now = HOUR * 3600 + 3599;
        for hour in [HOUR - 1, HOUR] {
            let nudge =
                crate::clock::seal_nudge_s(now, hour, crate::clock::compaction_seal_margin_s());
            assert!(nudge > 0);
            assert!(nudge < crate::clock::protection_horizon_s());
        }
        let start = HOUR * 3600;
        let largest =
            crate::clock::seal_nudge_s(start, HOUR, crate::clock::compaction_seal_margin_s());
        assert_eq!(
            largest,
            3600 + crate::clock::compaction_seal_margin_s() + 30
        );
        assert!(largest < crate::clock::protection_horizon_s());
    }

    #[test]
    fn only_a_fresh_held_record_of_the_tenant_is_raced() {
        let record = key(3, HOUR, "l1.0011223344556677.cmt");
        assert_eq!(judge_held("none", PREFIX), Held::Nothing);
        assert_eq!(
            judge_held(&format!("held {record} 1500"), PREFIX),
            Held::Race {
                key: record.clone(),
                record: RecordKey {
                    tenant_prefix: PREFIX.to_string(),
                    signal: "l".to_string(),
                    shard: 3,
                    hour: HOUR,
                },
                age: Duration::from_millis(1500),
            }
        );
        let at_limit = format!("held {record} {}", MAX_HELD_AGE.as_millis());
        assert!(matches!(judge_held(&at_limit, PREFIX), Held::Race { .. }));
        let stale = format!("held {record} {}", MAX_HELD_AGE.as_millis() + 1);
        assert_eq!(judge_held(&stale, PREFIX), Held::Skip(record.clone()));
        assert_eq!(
            judge_held(&format!("held {record} 10"), "t/ffff/"),
            Held::Skip(record)
        );
        let metrics = "t/0a0b/m/c/0003/20261117T16/l1.0011223344556677.cmt";
        assert_eq!(
            judge_held(&format!("held {metrics} 10"), PREFIX),
            Held::Skip(metrics.to_string())
        );
    }

    #[test]
    fn the_compaction_gets_what_is_left_before_the_release() {
        let none = Tamper::None;
        assert_eq!(cli_budget(Duration::ZERO, none), RELEASE_BY);
        assert_eq!(cli_budget(MAX_HELD_AGE, none), RELEASE_BY - MAX_HELD_AGE);
        assert_eq!(
            cli_budget(RELEASE_BY + Duration::from_secs(1), none),
            Duration::ZERO
        );
        // The freshest PUT the race takes still leaves the compaction most
        // of the window.
        assert!(cli_budget(MAX_HELD_AGE, none) >= Duration::from_secs(8));
        // A race that tampers stops waiting for the compaction earlier, so
        // the tamper and the release land well inside the hold cap; the
        // compaction, which takes about 0.1 s, still gets seconds.
        for tamper in [Tamper::Diverge, Tamper::DeletePart] {
            assert_eq!(cli_budget(Duration::ZERO, tamper), TAMPER_BY);
            assert!(cli_budget(MAX_HELD_AGE, tamper) >= Duration::from_secs(4));
            assert!(TAMPER_BY + Duration::from_secs(8) <= HOLD_CAP);
        }
    }

    /// The `CompactionRecord` fields a test record carries, one of them
    /// (`created_unix_ns`) unknown to the driver's own decode.
    #[derive(Clone, PartialEq, prost::Message)]
    struct FullRecord {
        #[prost(uint32, tag = "1")]
        format_version: u32,
        #[prost(bytes = "vec", tag = "2")]
        tenant_hash: Vec<u8>,
        #[prost(uint32, tag = "4")]
        shard: u32,
        #[prost(bytes = "vec", tag = "8")]
        input_set_hash: Vec<u8>,
        #[prost(message, repeated, tag = "9")]
        parts: Vec<FullPart>,
        #[prost(sfixed64, tag = "10")]
        created_unix_ns: i64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct FullPart {
        #[prost(uint32, tag = "1")]
        part_index: u32,
        #[prost(bytes = "vec", tag = "4")]
        content_hash: Vec<u8>,
        #[prost(uint64, tag = "5")]
        object_size: u64,
    }

    fn input_hash() -> Vec<u8> {
        (0u8..32)
            .map(|i| i.wrapping_mul(7).wrapping_add(1))
            .collect()
    }

    fn record_bytes(hash: &[u8], parts: &[(u32, u8)]) -> Vec<u8> {
        use prost::Message;
        FullRecord {
            format_version: 1,
            tenant_hash: vec![0x0a; 16],
            shard: 3,
            input_set_hash: hash.to_vec(),
            parts: parts
                .iter()
                .map(|&(part_index, fill)| FullPart {
                    part_index,
                    content_hash: vec![fill; 32],
                    object_size: 4096,
                })
                .collect(),
            created_unix_ns: 1_794_931_200_123_456_789,
        }
        .encode_to_vec()
    }

    fn record_key_for(hash: &[u8]) -> String {
        key(3, HOUR, &format!("l1.{}.cmt", hex(&hash[..8])))
    }

    #[test]
    fn the_part_to_delete_is_the_records_first_part_at_its_l1_key() {
        let hash = input_hash();
        let record_key = record_key_for(&hash);
        // Parts in the record out of index order.
        let record = record_bytes(&hash, &[(1, 0xbb), (0, 0xaa)]);
        let dir = format!("{PREFIX}l/l1/0003/{}/", keys::hour_string(HOUR));
        let h16 = hex(&hash[..8]);
        assert_eq!(
            winner_parts(&record_key, &record),
            Ok(vec![
                format!("{dir}{h16}.0000.aaaaaaaaaaaaaaaa.rseg"),
                format!("{dir}{h16}.0001.bbbbbbbbbbbbbbbb.rseg"),
            ])
        );
        assert_eq!(
            part_to_delete(&record_key, &record),
            Ok(format!("{dir}{h16}.0000.aaaaaaaaaaaaaaaa.rseg"))
        );
        // A record with no part has none to delete.
        assert!(part_to_delete(&record_key, &record_bytes(&hash, &[])).is_err());
        // The key must name the record's own hash.
        let mut other = hash.clone();
        other[0] ^= 1;
        assert!(winner_parts(&record_key_for(&other), &record).is_err());
        // Not a compaction record key, or not a record.
        assert!(winner_parts(&key(3, HOUR, "w1.1.00000000000000000001.cmt"), &record).is_err());
        assert!(winner_parts(&record_key, b"\xff\xff\xff").is_err());
        // A part whose content hash is not 32 bytes names no key.
        let mut short = {
            use prost::Message;
            FullRecord::decode(record.as_slice()).expect("decode")
        };
        short.parts[0].content_hash.truncate(8);
        let short = {
            use prost::Message;
            short.encode_to_vec()
        };
        assert!(winner_parts(&record_key, &short).is_err());
        // A part index past the key's 4 digits names no key either, and says
        // so.
        let wide = record_bytes(&hash, &[(10_000, 0xaa)]);
        let err = winner_parts(&record_key, &wide).expect_err("index too wide");
        assert!(err.contains("part index 10000"), "{err}");
    }

    #[test]
    fn diverge_flips_only_the_hash_byte_past_the_keys_eight() {
        use prost::Message;
        let hash = input_hash();
        let record_key = record_key_for(&hash);
        let record = record_bytes(&hash, &[(0, 0xaa), (1, 0xbb)]);
        let forged = diverge(&record_key, &record).expect("diverges");
        assert_eq!(forged.len(), record.len());
        let changed: Vec<usize> = (0..record.len())
            .filter(|&i| record[i] != forged[i])
            .collect();
        assert_eq!(changed.len(), 1, "{changed:?}");
        let was = FullRecord::decode(record.as_slice()).expect("decode");
        let now = FullRecord::decode(forged.as_slice()).expect("decode forged");
        // The hash differs in its last byte only, so the key still names it
        // and every other field, the unknown one included, is the same.
        assert_eq!(now.input_set_hash[..31], was.input_set_hash[..31]);
        assert_ne!(now.input_set_hash[31], was.input_set_hash[31]);
        assert_eq!(
            FullRecord {
                input_set_hash: was.input_set_hash.clone(),
                ..now.clone()
            },
            was
        );
        assert!(winner_parts(&record_key, &forged).is_ok());
        // A hash framed twice in the bytes (a repeated field, the last of
        // which protobuf keeps) is refused rather than half patched.
        let mut twice = record.clone();
        twice.extend_from_slice(&HASH_FRAME);
        twice.extend_from_slice(&hash);
        assert_eq!(
            FullRecord::decode(twice.as_slice())
                .expect("decode")
                .input_set_hash,
            hash
        );
        assert!(diverge(&record_key, &twice).is_err());
        // A key that does not name the record's hash.
        let mut other = hash.clone();
        other[7] ^= 1;
        assert!(diverge(&record_key_for(&other), &record).is_err());
    }

    #[test]
    fn the_tampering_race_at_every_bound_ends_before_the_fold_seal() {
        // The nudge lands 30 s past the compaction seal, 900 s before the
        // fold seal.
        assert_eq!(FOLD_SEAL_AFTER_NUDGE_S, 870);
        // Every term, each from the constant it stands for, so dropping one
        // from a sum fails here.
        let reread = crate::clock::FAKETIME_CACHE_DURATION_S;
        let reach = crate::clock::reach_wait_s(reread);
        let s3 = crate::s3::MAX_TIME_S;
        let serve_round = 2 * (2 + crate::READ_TIMEOUT.as_secs()) + 2 * 2 + 2;
        let before_tamper = (reread + 30) // shift_clock: the move reaching the driver
            + reach // shift_clock: its reach wait
            + s3 // shift_clock: the GET of HEAD
            + reach // settle: its reach wait
            + 120 + serve_round // settle: wait_serving and one round past it
            + PROXY_CMD_S // hold-record
            + TAMPER_HELD_WAIT.as_secs() + 3 * PROXY_CMD_S + 1 // held wait, one poll round
            + TAMPER_BY.as_secs() // the compaction
            + s3; // the GET of the CLI's record
        assert_eq!(BEFORE_TAMPER_S, before_tamper);
        let tail = 2 * s3 // the tamper: two raw S3 requests
            + s3 // the in-place GET
            + PROXY_CMD_S // release
            + crate::HELD_RESULT_WAIT.as_secs() + PROXY_CMD_S + 1 // the held result
            + 2
            + s3 // the GET after the release
            + RESTORE_AFTER_FAULTS.as_secs();
        assert_eq!(TAMPER_TAIL_S, tail);
        assert_eq!(UNDO_ATTEMPT_S, 2 * s3);
        assert_eq!(
            TAMPER_RACE_S,
            before_tamper + tail + UNDO_FOR.as_secs() + 2 * s3 + reach
        );
        // The figures: 372 + 347 + 60 + 60 + 3 = 842.
        assert_eq!(BEFORE_TAMPER_S, 372);
        assert_eq!(TAMPER_TAIL_S, 347);
        assert_eq!(TAMPER_RACE_S, 842);
    }

    #[test]
    fn a_tamper_happens_and_is_undone_only_before_the_fold_seal() {
        let seal = (HOUR + 1) * 3600 + 4800;
        assert_eq!(fold_seal_s(HOUR), seal);
        // The last undo attempt starts one attempt and one clock skew
        // before the seal.
        assert_eq!(undo_by_s(HOUR), seal - 60 - 3);
        // Right after the nudge every wait fits.
        let nudged = (HOUR + 1) * 3600 + 3900 + 30;
        assert!(tamper_fits(nudged + BEFORE_TAMPER_S, HOUR));
        let last = undo_by_s(HOUR) - TAMPER_TAIL_S;
        assert!(tamper_fits(last, HOUR));
        assert!(!tamper_fits(last + 1, HOUR));
        // An earlier hour, already past its seal, is never tampered.
        assert!(!tamper_fits(nudged, HOUR - 1));
        // The wait before the undo is cut so the first attempt starts by
        // the deadline, and is zero past it.
        assert_eq!(restore_wait(nudged, HOUR, true), RESTORE_AFTER_FAULTS);
        assert_eq!(restore_wait(nudged, HOUR, false), RESTORE_AFTER);
        assert_eq!(
            restore_wait(undo_by_s(HOUR) - 50, HOUR, true),
            Duration::from_secs(50)
        );
        assert_eq!(
            restore_wait(undo_by_s(HOUR) + 5, HOUR, true),
            Duration::ZERO
        );
        // The elapsed time printed for the race counts from the nudge.
        assert_eq!(race_elapsed_s(nudged, HOUR), 0);
        assert_eq!(race_elapsed_s(nudged + 842, HOUR), 842);
    }

    #[test]
    fn a_late_undo_fires_the_cap_and_still_runs() {
        let by = undo_by_s(HOUR);
        // In time: an attempt, no cap.
        assert_eq!(
            undo_step(by, HOUR, Duration::ZERO, 0),
            UndoStep::Attempt { overran: false }
        );
        // Past the deadline the cap fires, and the undo still runs: the
        // first attempt, and every retry while UNDO_FOR lasts.
        assert_eq!(
            undo_step(by + 1, HOUR, Duration::ZERO, 0),
            UndoStep::Attempt { overran: true }
        );
        assert_eq!(
            undo_step(by + 300, HOUR, UNDO_FOR - Duration::from_millis(1), 3),
            UndoStep::Attempt { overran: true }
        );
        // Even with UNDO_FOR spent before the first attempt, one runs.
        assert_eq!(
            undo_step(by + 300, HOUR, UNDO_FOR, 0),
            UndoStep::Attempt { overran: true }
        );
        // After refused attempts for all of UNDO_FOR it gives up, and the
        // undo is left pending.
        assert_eq!(undo_step(by, HOUR, UNDO_FOR, 1), UndoStep::GiveUp);
        assert_eq!(undo_step(by + 300, HOUR, UNDO_FOR, 4), UndoStep::GiveUp);
    }

    #[test]
    fn a_record_change_after_the_release_is_judged_against_what_the_loser_met() {
        let forged = Undo::Record {
            key: "r".to_string(),
            original: b"cli".to_vec(),
            forged: b"forged".to_vec(),
        };
        // Divergent-record race: the forged record still there is no
        // change, though it differs from the CLI's; a loser's own record is.
        assert_eq!(
            record_changed(Some(b"cli"), Some(b"forged"), Some(&forged)),
            Some(false)
        );
        assert_eq!(
            record_changed(Some(b"cli"), Some(b"loser"), Some(&forged)),
            Some(true)
        );
        // Otherwise the CLI's record is what the loser met.
        let part = Undo::Part {
            key: "p".to_string(),
            bytes: b"part".to_vec(),
        };
        assert_eq!(
            record_changed(Some(b"cli"), Some(b"cli"), Some(&part)),
            Some(false)
        );
        assert_eq!(
            record_changed(Some(b"cli"), Some(b"loser"), None),
            Some(true)
        );
        assert_eq!(record_changed(Some(b"cli"), None, None), None);
        assert_eq!(record_changed(None, Some(b"cli"), None), None);
    }

    #[test]
    fn the_undo_waits_out_a_retried_request_while_faults_may_run() {
        assert_eq!(restore_after(false), RESTORE_AFTER);
        assert_eq!(restore_after(true), RESTORE_AFTER_FAULTS);
        // One store operation retried to its bound: 180 s of retries plus
        // one 20 s request.
        assert!(
            RESTORE_AFTER_FAULTS
                >= Duration::from_secs(180 + crate::clock::STORE_REQUEST_TIMEOUT_S)
        );
        assert!(RESTORE_AFTER >= Duration::from_secs(3 * crate::clock::STORE_REQUEST_TIMEOUT_S));
    }

    #[test]
    fn a_tamper_is_undone_only_while_it_is_in_place() {
        let record = Undo::Record {
            key: "r".to_string(),
            original: b"cli".to_vec(),
            forged: b"forged".to_vec(),
        };
        assert!(record.in_place(Some(b"forged")));
        assert_eq!(record.restore(Some(b"forged")), Some(&b"cli"[..]));
        // A loser overwrote the forged record (the mutant): its record stays.
        assert!(!record.in_place(Some(b"loser")));
        assert_eq!(record.restore(Some(b"loser")), None);
        // The forged PUT never landed, or a sweep removed the record.
        assert_eq!(record.restore(Some(b"cli")), None);
        assert_eq!(record.restore(None), None);
        let part = Undo::Part {
            key: "p".to_string(),
            bytes: b"part".to_vec(),
        };
        assert_eq!(part.key(), "p");
        assert!(part.in_place(None));
        assert_eq!(part.restore(None), Some(&b"part"[..]));
        // The DELETE never landed, or the part is back already.
        assert!(!part.in_place(Some(b"part")));
        assert_eq!(part.restore(Some(b"part")), None);
    }

    #[test]
    fn a_tampered_race_counts_only_with_the_tamper_in_place_at_the_release() {
        let fresh = Some(Duration::from_secs(10));
        assert!(tampered_lost(true, fresh, Some(412), true));
        // The store refused the held PUT, but a read just before the release
        // did not find the tamper: the loser met the plain winner.
        assert!(!tampered_lost(true, fresh, Some(412), false));
        // The tamper was in place, but the race was not lost as
        // record_lost judges it.
        assert!(!tampered_lost(true, fresh, Some(200), true));
        assert!(!tampered_lost(false, fresh, Some(412), true));
        assert!(!tampered_lost(
            true,
            Some(HOLD_CAP + Duration::from_millis(1)),
            Some(412),
            true
        ));
    }

    #[test]
    fn present_inputs_are_commit_records_and_data_objects() {
        let inputs = vec![
            InputKeys {
                commit: key(0, HOUR, "w1.1.00000000000000000001.cmt"),
                data_prefix: format!("{PREFIX}l/l0/0000/w1.1.00000000000000000001."),
            },
            InputKeys {
                commit: key(0, HOUR, "w1.1.00000000000000000002.cmt"),
                data_prefix: format!("{PREFIX}l/l0/0000/w1.1.00000000000000000002."),
            },
        ];
        let data1 = format!("{}abcd{DATA_SUFFIX}", inputs[0].data_prefix);
        let listed = listing(&[
            inputs[0].commit.clone(),
            data1.clone(),
            format!("{}abcd.tmp", inputs[0].data_prefix),
            format!("{PREFIX}l/l0/0000/w1.1.000000000000000000010.ef{DATA_SUFFIX}"),
            format!("{}ef01{DATA_SUFFIX}", inputs[1].data_prefix),
        ]);
        assert_eq!(
            present_inputs(&inputs, &listed),
            vec![
                inputs[0].commit.clone(),
                data1,
                format!("{}ef01{DATA_SUFFIX}", inputs[1].data_prefix),
            ]
        );
        assert!(present_inputs(&inputs, &BTreeSet::new()).is_empty());
    }

    #[test]
    fn only_a_deletion_after_the_snapshot_counts() {
        let inputs = vec![
            InputKeys {
                commit: key(0, HOUR, "w1.1.00000000000000000001.cmt"),
                data_prefix: format!("{PREFIX}l/l0/0000/w1.1.00000000000000000001."),
            },
            InputKeys {
                commit: key(0, HOUR, "w1.1.00000000000000000002.cmt"),
                data_prefix: format!("{PREFIX}l/l0/0000/w1.1.00000000000000000002."),
            },
        ];
        let data2 = format!("{}ef01{DATA_SUFFIX}", inputs[1].data_prefix);
        // The first input was deleted earlier in the timeline: the LIST
        // just before the jump holds only the second.
        let before = listing(&[inputs[1].commit.clone(), data2.clone()]);
        // A poll that still holds the second sees no deletion since then,
        // though the first stays gone.
        assert!(deleted_since(&inputs, &before, &before).is_empty());
        // Without the snapshot, the ledger that saw both inputs reports the
        // earlier deletion at once.
        let mut ledger = crate::audit::SweepLedger::default();
        let mut both = before.clone();
        both.insert(inputs[0].commit.clone());
        ledger.observe(both.iter().cloned());
        assert!(!ledger.unconfirmed(&inputs, &before).is_empty());
        // The second input deleted after the jump counts.
        let after = listing(&[data2]);
        assert_eq!(
            deleted_since(&inputs, &before, &after),
            vec![inputs[1].commit.clone()]
        );
    }

    #[test]
    fn only_a_store_refused_create_after_the_cli_published_is_a_lost_record_put() {
        let fresh = Some(Duration::from_secs(10));
        // The CLI's record was in place and the store refused the held create
        // (412, or 409): lost.
        assert!(record_lost(true, fresh, Some(412)));
        assert!(record_lost(true, fresh, Some(409)));
        // Published before the release, but the store refused the held PUT
        // for skew (403): it never reached AlreadyExists in the server. This
        // rules out the old rule, published_before_release alone.
        assert!(!record_lost(true, fresh, Some(403)));
        assert!(!record_lost(true, fresh, Some(503)));
        assert!(!record_lost(true, fresh, Some(500)));
        // A held PUT the store accepted (200) is a win, never lost. This
        // rules out a rule that counts any held PUT.
        assert!(!record_lost(true, fresh, Some(200)));
        // No held-result for the held key.
        assert!(!record_lost(true, fresh, None));
        // The CLI had not published before the release.
        assert!(!record_lost(false, fresh, Some(412)));
        // The release failed, or came after the proxy's cap forwarded the
        // PUT on its own.
        assert!(!record_lost(true, None, Some(412)));
        assert!(record_lost(true, Some(HOLD_CAP), Some(412)));
        assert!(!record_lost(
            true,
            Some(HOLD_CAP + Duration::from_millis(1)),
            Some(412)
        ));
    }

    #[test]
    fn the_held_status_is_the_held_keys_only() {
        let record = key(3, HOUR, "l1.0011223344556677.cmt");
        let other = key(2, HOUR, "l1.8899aabbccddeeff.cmt");
        assert_eq!(
            held_status(&record, Some(&(record.clone(), 412))),
            Some(412)
        );
        assert_eq!(held_status(&record, Some(&(other, 412))), None);
        assert_eq!(held_status(&record, None), None);
    }

    #[test]
    fn the_unreadable_head_sweep_counts_only_the_garbage_still_in_place() {
        let garbage = b"antithesis: not a catalog HEAD (unreadable-HEAD sweep 0)";
        assert!(unreadable_head_reached(Some(garbage), garbage));
        // A repair got through: HEAD holds valid bytes after the wait.
        assert!(!unreadable_head_reached(
            Some(b"\x0a\x04RHD1valid"),
            garbage
        ));
        // Other garbage (another rule's), or no HEAD at all.
        assert!(!unreadable_head_reached(
            Some(b"antithesis: not a catalog HEAD"),
            garbage
        ));
        assert!(!unreadable_head_reached(None, garbage));
    }

    #[test]
    fn the_snapshot_prefix_sits_beside_head() {
        assert_eq!(
            snap_prefix("t/0a0b/catalog/l/HEAD").as_deref(),
            Some("t/0a0b/catalog/l/snap/")
        );
        assert_eq!(snap_prefix("t/0a0b/catalog/l/HEADX"), None);
        assert_eq!(snap_prefix("t/0a0b/l/HEAD"), None);
    }

    #[test]
    fn deleted_keys_are_the_earlier_listings_under_the_prefix() {
        let before = listing(&[
            "t/0a0b/catalog/l/snap/1.aa.csnap".to_string(),
            "t/0a0b/catalog/l/snap/2.bb.csnap".to_string(),
            "t/0a0b/catalog/l/HEAD".to_string(),
            "t/0a0b/l/x".to_string(),
        ]);
        let after = listing(&["t/0a0b/catalog/l/snap/2.bb.csnap".to_string()]);
        assert_eq!(
            deleted_under("t/0a0b/catalog/l/snap/", &before, &after),
            vec!["t/0a0b/catalog/l/snap/1.aa.csnap".to_string()]
        );
        assert!(deleted_under("t/0a0b/catalog/l/snap/", &before, &before).is_empty());
    }

    #[test]
    fn the_unfolded_sweep_counts_only_an_unmoved_head() {
        assert!(unfolded_reached(b"head", Some(b"head"), true, 3));
        assert!(!unfolded_reached(b"head", Some(b"head2"), true, 3));
        assert!(!unfolded_reached(b"head", None, true, 3));
        assert!(!unfolded_reached(b"head", Some(b"head"), false, 3));
        assert!(!unfolded_reached(b"head", Some(b"head"), true, 0));
    }
}
