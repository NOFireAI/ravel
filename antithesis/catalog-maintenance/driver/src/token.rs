//! Token reads across the catalog's maintenance lifecycle: where a write's
//! commit token points in the store, which lifecycle state its level-0 input
//! is in, and the driver's own judgement of what a token read returned.
//!
//! A commit token (`CommitToken::encode` in ravel-types, a frozen contract)
//! is base64url without padding over `v2:<shard>:<writer>:<epoch>:<seq>:<hour>`
//! and fully determines its commit record key
//! (`ravel_commit::keys::commit_key_for_token`). The lifecycle state is
//! decided from raw listings and decodes the driver already takes; the
//! verdict on the rows comes from the ingest-durability model, never from a
//! Ravel resolver.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use ravel_antithesis_catalog_driver::keys;
use ravel_antithesis_driver::model::{self, Model};
use ravel_antithesis_driver::ravel::SqlFailure;

use crate::audit::InputKeys;

/// The stable client message of an unsatisfiable token
/// (`ravel_sql::error::MSG_UNSATISFIABLE`), served with a 503.
pub const MSG_UNSATISFIABLE: &str = "requested commit token is not yet visible; retry";

/// One decoded commit token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token {
    pub shard: u32,
    pub writer: String,
    pub epoch: u64,
    pub seq: u64,
    pub hour: u32,
}

impl Token {
    /// The token's commit record key under `tenant_prefix` (`t/<hash>/`):
    /// `<prefix>l/c/<shard:04>/<YYYYMMDDTHH>/<writer>.<epoch>.<seq:020>.cmt`,
    /// the shape `audit::compaction_inputs` gives each input's commit record.
    pub fn commit_key(&self, tenant_prefix: &str) -> String {
        format!(
            "{}{}.{}.{:020}.cmt",
            keys::bucket_prefix(tenant_prefix, self.shard, u64::from(self.hour)),
            self.writer,
            self.epoch,
            self.seq
        )
    }
}

/// Decode one token as the server's `Ravel-Commit-Token` header carries it.
pub fn decode(raw: &str) -> Result<Token, String> {
    let bytes = base64url_decode(raw.trim()).ok_or_else(|| format!("{raw:?} is not base64url"))?;
    let text = String::from_utf8(bytes).map_err(|_| format!("{raw:?} is not UTF-8"))?;
    let parts: Vec<&str> = text.split(':').collect();
    let ["v2", shard, writer, epoch, seq, hour] = parts.as_slice() else {
        return Err(format!("{text:?} is not a v2 commit token"));
    };
    let num = |s: &str| s.parse::<u64>().map_err(|_| format!("{text:?}: {s:?}"));
    if writer.len() != 36 || !writer.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        return Err(format!("{text:?}: writer {writer:?} is not a UUID"));
    }
    Ok(Token {
        shard: u32::try_from(num(shard)?).map_err(|_| format!("{text:?}: shard"))?,
        writer: (*writer).to_string(),
        epoch: num(epoch)?,
        seq: num(seq)?,
        hour: u32::try_from(num(hour)?).map_err(|_| format!("{text:?}: hour"))?,
    })
}

/// The tokens of a commit-token header (comma-separated, as
/// `Write::tokens` splits it), each kept beside its raw form.
pub fn decode_header(header: &str) -> Result<Vec<(String, Token)>, String> {
    let tokens: Vec<(String, Token)> = header
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| decode(t).map(|tok| (t.to_string(), tok)))
        .collect::<Result<_, _>>()?;
    if tokens.is_empty() {
        return Err(format!("{header:?} holds no token"));
    }
    Ok(tokens)
}

fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    let value = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        } as u32)
    };
    let bytes = s.as_bytes();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut acc = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            acc |= value(c)? << (18 - 6 * i);
        }
        let n = chunk.len() - 1;
        for i in 0..n {
            out.push((acc >> (16 - 8 * i)) as u8);
        }
        // Trailing bits a canonical encoder leaves zero.
        let used = 8 * n;
        let spare = 6 * chunk.len() - used;
        if spare > 0 && ((acc >> (24 - 6 * chunk.len())) & ((1 << spare) - 1)) != 0 {
            return None;
        }
    }
    Some(out)
}

/// The key prefix of every output part of the compaction record at
/// `record_key` (`ravel_commit::keys::l1_part_key`):
/// `t/<hash>/l/l1/<shard:04>/<YYYYMMDDTHH>/<input_set_hash16>.`.
pub fn output_prefix(record_key: &str) -> Option<String> {
    let rk = keys::parse_record_key(record_key)?;
    let name = record_key.rsplit('/').next()?;
    let hash = name.strip_prefix("l1.")?.strip_suffix(".cmt")?;
    Some(format!(
        "{}{}/l1/{:04}/{}/{hash}.",
        rk.tenant_prefix,
        rk.signal,
        rk.shard,
        keys::hour_string(rk.hour)
    ))
}

/// The first decoded compaction record whose inputs name `commit_key`, and
/// that input.
pub fn covering<'a>(
    commit_key: &str,
    records: &'a BTreeMap<String, Vec<InputKeys>>,
) -> Option<(&'a str, &'a InputKeys)> {
    records.iter().find_map(|(record, inputs)| {
        inputs
            .iter()
            .find(|i| i.commit == commit_key)
            .map(|i| (record.as_str(), i))
    })
}

/// What one store audit showed of a HEAD that did not move while it ran:
/// its watermark and every data object `rlog footprint` resolved under it.
#[derive(Clone, Debug, Default)]
pub struct HeadEvidence {
    pub created_unix_ns: u64,
    pub watermark_hour: u64,
    pub resolved: BTreeSet<String>,
}

impl HeadEvidence {
    /// Whether this HEAD serves the input's hour from the compaction
    /// record's output: the hour is at or below the watermark, so HEAD's
    /// parts alone serve it, the catalog resolved an output part of the
    /// record, and it resolved no data object of the input.
    pub fn names_output(&self, hour: u32, record_key: &str, input: &InputKeys) -> bool {
        let Some(out) = output_prefix(record_key) else {
            return false;
        };
        let under = |p: &str| {
            self.resolved
                .range(p.to_string()..)
                .take_while(|k| k.starts_with(p))
                .next()
                .is_some()
        };
        u64::from(hour) <= self.watermark_hour && under(&out) && !under(&input.data_prefix)
    }
}

/// The lifecycle state of one token's level-0 input, from evidence taken
/// before the read and the commit record's presence after it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InputState {
    pub commit_key: String,
    /// A decoded compaction record naming the input, listed before the read.
    pub covering_record: Option<String>,
    /// Whether the commit record was in the store before the read: `None`
    /// when neither a LIST nor a GET could tell.
    pub commit_before: Option<bool>,
    /// The same after the read.
    pub commit_after: Option<bool>,
    /// The last stable HEAD the audit saw before the read served the hour
    /// from the covering record's output.
    pub head_named_output: bool,
}

impl InputState {
    /// No compaction record named the input before the read.
    pub fn before_compaction(&self) -> bool {
        self.covering_record.is_none()
    }

    /// Compacted, and the commit record was still there after the read, so
    /// throughout it (a deleted key never returns).
    pub fn compacted_record_present(&self) -> bool {
        self.covering_record.is_some() && self.commit_after == Some(true)
    }

    /// Compacted, and the commit record was gone before the read.
    pub fn swept(&self) -> bool {
        self.covering_record.is_some() && self.commit_before == Some(false)
    }

    pub fn label(&self) -> &'static str {
        if self.swept() {
            "input commit record swept"
        } else if self.head_named_output {
            "HEAD named the compaction output"
        } else if self.before_compaction() {
            "before compaction"
        } else {
            "compacted"
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "state": self.label(),
            "commit_key": self.commit_key,
            "covering_record": self.covering_record,
            "commit_before": self.commit_before,
            "commit_after": self.commit_after,
            "head_named_output": self.head_named_output,
        })
    }
}

/// What one token read came to.
#[derive(Debug)]
pub enum Outcome {
    /// The read answered rows, judged against the model.
    Conclusive(Judged),
    /// The server refused the token as unsatisfiable on the read and on its
    /// retry: an acknowledged token always resolves, so this is a verdict.
    Unsatisfiable(String),
    /// No verdict: no response, a timeout, or a refusal of another class.
    Inconclusive { class: &'static str, detail: String },
}

impl Outcome {
    /// Whether the read gave a verdict on the write.
    pub fn conclusive(&self) -> bool {
        !matches!(self, Outcome::Inconclusive { .. })
    }

    /// Whether the verdict holds: every acked record exactly once.
    pub fn holds(&self) -> bool {
        matches!(self, Outcome::Conclusive(j) if j.holds())
    }

    pub fn class(&self) -> &'static str {
        match self {
            Outcome::Conclusive(_) => "conclusive",
            Outcome::Unsatisfiable(_) => "unsatisfiable",
            Outcome::Inconclusive { class, .. } => class,
        }
    }
}

/// The class of a failed read. `unsatisfiable` is the token refusal; the
/// rest give no verdict.
pub fn failure_class(e: &SqlFailure) -> &'static str {
    match e {
        SqlFailure::NoResponse(_) => "no_response",
        SqlFailure::BadBody(_) => "bad_body",
        SqlFailure::Status(503, body) if body.contains(MSG_UNSATISFIABLE) => "unsatisfiable",
        SqlFailure::Status(503, _) => "unavailable",
        SqlFailure::Status(504, _) => "timeout",
        SqlFailure::Status(422, _) => "unsupported",
        SqlFailure::Status(500, _) => "internal",
        SqlFailure::Status(400, _) => "bad_request",
        SqlFailure::Status(_, _) => "other_status",
    }
}

/// The rows of one conclusive read of one write, judged by the
/// ingest-durability model.
#[derive(Debug, Default)]
pub struct Judged {
    pub rows: u64,
    pub phantoms: Vec<String>,
    pub over: Vec<String>,
    pub missing: Vec<String>,
    /// Whether the self-check duplicated a returned record.
    pub injected: bool,
}

impl Judged {
    pub fn holds(&self) -> bool {
        self.phantoms.is_empty() && self.over.is_empty() && self.missing.is_empty()
    }
}

/// Judge the `(write, record) -> count` rows of a read of write `id`. The
/// write was picked with `max_copies() == 1`, so the model's bound is
/// exactly once: a second copy is `over`, an absent record `missing`, a
/// record or write never sent a phantom. With `inject`, one returned record
/// is counted twice first (the self-check).
pub fn judge(m: &Model, mut counts: HashMap<(u64, u32), u64>, id: u64, inject: bool) -> Judged {
    let injected = inject && inject_duplicate(&mut counts);
    let j = model::judge_logs(m, &counts, &[id]);
    let mut over = j.over;
    over.extend(j.over_after_replay);
    Judged {
        rows: counts.values().sum(),
        phantoms: j.phantoms,
        over,
        missing: j.missing,
        injected,
    }
}

/// Count the smallest returned `(write, record)` once more; `false` when no
/// row was returned.
pub fn inject_duplicate(counts: &mut HashMap<(u64, u32), u64>) -> bool {
    match counts.keys().min().copied() {
        Some(k) => {
            *counts.entry(k).or_insert(0) += 1;
            true
        }
        None => false,
    }
}

/// One acknowledgement the driver received: which node, the writer identity
/// its first token names, the driver's ticks when the attempt was sent and
/// when its answer arrived, and the shard count it was routed at. Ticks come
/// from one counter, so a tick ordering is an ordering in real time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ack {
    pub node: usize,
    pub writer: String,
    pub sent: u64,
    pub answered: u64,
    /// The shard count every hour of the attempt routes at, from the send to
    /// the answer and each token's hour (`audit::ShardHistory::routed_count`);
    /// `None` when the provisioning record does not prove one count.
    pub count: Option<u32>,
}

/// Every acknowledgement, per node and per write. A ravel-server's log
/// router builds one shard set per shard count, the first time that count
/// routes, and each set draws its own writer identity, which lasts as long
/// as the process. So within one process a writer identity changes only
/// with the count, and a node acknowledging at one count under a new writer
/// identity was restarted. A reshard rebuilds the set without a restart, so
/// two acknowledgements at different counts, or at a count the record did
/// not prove, are no restart evidence.
#[derive(Debug, Default)]
pub struct AckLedger {
    by_node: BTreeMap<usize, Vec<Ack>>,
    by_write: BTreeMap<u64, Ack>,
}

impl AckLedger {
    pub fn record(&mut self, write: u64, ack: Ack) {
        self.by_write.entry(write).or_insert_with(|| ack.clone());
        self.by_node.entry(ack.node).or_default().push(ack);
    }

    pub fn of_write(&self, write: u64) -> Option<&Ack> {
        self.by_write.get(&write)
    }

    /// Evidence that `node` restarted after `write` was acknowledged: an
    /// acknowledgement from `node` sent after the write's answer arrived (or
    /// the write's own, when `node` acknowledged it), and a later one, sent
    /// after that one's answer, routed at the same known shard count under
    /// another writer identity. `None` when the acknowledgements show no such
    /// change.
    pub fn restarted_since(&self, write: u64, node: usize) -> Option<(Ack, Ack)> {
        let w = self.by_write.get(&write)?;
        let acks = self.by_node.get(&node)?;
        let anchors = acks
            .iter()
            .filter(|a| a.count.is_some() && (a.sent > w.answered || *a == w));
        for x in anchors {
            if let Some(y) = acks
                .iter()
                .find(|y| y.sent > x.answered && y.count == x.count && y.writer != x.writer)
            {
                return Some((x.clone(), y.clone()));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ravel_antithesis_driver::model::{Attempt, Mode, Signal, Write};

    const WRITER: &str = "0b6e7c1a-1d2e-4f50-9a6b-7c8d9e0f1a2b";
    // base64url of `v2:3:<WRITER>:1794931200:7:498592`, as `CommitToken::encode`
    // writes it.
    const TOKEN: &str =
        "djI6MzowYjZlN2MxYS0xZDJlLTRmNTAtOWE2Yi03YzhkOWUwZjFhMmI6MTc5NDkzMTIwMDo3OjQ5ODU5Mg";
    // `v2:0:<WRITER>:1:12345678901:0`.
    const TOKEN2: &str =
        "djI6MDowYjZlN2MxYS0xZDJlLTRmNTAtOWE2Yi03YzhkOWUwZjFhMmI6MToxMjM0NTY3ODkwMTow";

    #[test]
    fn a_token_decodes_to_its_commit_key() {
        let t = decode(TOKEN).expect("token");
        assert_eq!(
            t,
            Token {
                shard: 3,
                writer: WRITER.to_string(),
                epoch: 1_794_931_200,
                seq: 7,
                hour: 498_592,
            }
        );
        assert_eq!(
            t.commit_key("t/0a0b/"),
            format!("t/0a0b/l/c/0003/20261117T16/{WRITER}.1794931200.00000000000000000007.cmt")
        );
        let t2 = decode(TOKEN2).expect("token");
        assert_eq!(
            (t2.shard, t2.epoch, t2.seq, t2.hour),
            (0, 1, 12_345_678_901, 0)
        );
        let both = decode_header(&format!("{TOKEN}, {TOKEN2}")).expect("header");
        assert_eq!(both.len(), 2);
        assert_eq!(both[1].0, TOKEN2);
    }

    #[test]
    fn the_commit_key_matches_the_compaction_input_shape() {
        let t = decode(TOKEN).expect("token");
        let prefix = "t/0a0b/";
        let record = format!(
            "{}l1.0011223344556677.cmt",
            keys::bucket_prefix(prefix, 3, 498_592)
        );
        let decoded = format!(
            "inputs: 1\n  writer_id={WRITER} writer_epoch=1794931200 writer_seq=7\nparts: 1\n"
        );
        let inputs = crate::audit::compaction_inputs(prefix, &record, &decoded).expect("inputs");
        assert_eq!(inputs[0].commit, t.commit_key(prefix));
        let mut records = BTreeMap::new();
        records.insert(record.clone(), inputs);
        let (r, input) = covering(&t.commit_key(prefix), &records).expect("covered");
        assert_eq!(r, record);
        assert_eq!(
            input.data_prefix,
            format!("t/0a0b/l/l0/0003/{WRITER}.1794931200.00000000000000000007.")
        );
        assert!(covering("t/0a0b/l/c/0003/20261117T16/other.cmt", &records).is_none());
    }

    #[test]
    fn malformed_tokens_are_refused() {
        for bad in [
            "",
            "!!!!",
            "djI6",
            // `v3:...`
            "djM6MzowYjZlN2MxYS0xZDJlLTRmNTAtOWE2Yi03YzhkOWUwZjFhMmI6MTo3OjQ5ODU5Mg",
            // padded
            "djI6MzowYjZlN2MxYS0xZDJlLTRmNTAtOWE2Yi03YzhkOWUwZjFhMmI6MTc5NDkzMTIwMDo3OjQ5ODU5Mg==",
        ] {
            assert!(decode(bad).is_err(), "{bad:?}");
        }
        assert!(decode_header(" , ").is_err());
    }

    #[test]
    fn base64url_round_trips_every_tail_length() {
        // "a", "ab", "abc", "abcd" and the URL-safe alphabet's two symbols.
        for (enc, dec) in [
            ("YQ", &b"a"[..]),
            ("YWI", b"ab"),
            ("YWJj", b"abc"),
            ("YWJjZA", b"abcd"),
            ("-_8", &[0xfb, 0xff]),
        ] {
            assert_eq!(base64url_decode(enc).as_deref(), Some(dec), "{enc}");
        }
        // Non-zero spare bits are not a canonical encoding.
        assert_eq!(base64url_decode("YR"), None);
        assert_eq!(base64url_decode("Y"), None);
    }

    #[test]
    fn the_output_prefix_follows_the_l1_part_key() {
        let record = "t/0a0b/l/c/0003/20261117T16/l1.0011223344556677.cmt";
        assert_eq!(
            output_prefix(record).as_deref(),
            Some("t/0a0b/l/l1/0003/20261117T16/0011223344556677.")
        );
        assert_eq!(output_prefix("t/0a0b/l/c/0003/20261117T16/w.1.2.cmt"), None);
    }

    #[test]
    fn head_names_the_output_only_with_the_output_resolved_and_the_input_not() {
        let record = "t/0a0b/l/c/0003/20261117T16/l1.0011223344556677.cmt";
        let input = InputKeys {
            commit: format!("t/0a0b/l/c/0003/20261117T16/{WRITER}.1.00000000000000000007.cmt"),
            data_prefix: format!("t/0a0b/l/l0/0003/{WRITER}.1.00000000000000000007."),
        };
        let out = "t/0a0b/l/l1/0003/20261117T16/0011223344556677.00000.ffeeddccbbaa9988.rseg";
        let l0 = format!("{}aabbccddeeff0011.rseg", input.data_prefix);
        let head = |wm: u64, keys: &[&str]| HeadEvidence {
            created_unix_ns: 1,
            watermark_hour: wm,
            resolved: keys.iter().map(|k| k.to_string()).collect(),
        };
        assert!(head(498_592, &[out]).names_output(498_592, record, &input));
        // The hour above the watermark is served by listing, not by HEAD.
        assert!(!head(498_591, &[out]).names_output(498_592, record, &input));
        // The input still resolved beside the output.
        assert!(!head(498_592, &[out, &l0]).names_output(498_592, record, &input));
        assert!(!head(498_592, &[&l0]).names_output(498_592, record, &input));
    }

    #[test]
    fn states_follow_the_evidence() {
        let mut s = InputState {
            commit_key: "k".into(),
            ..InputState::default()
        };
        assert!(s.before_compaction() && !s.compacted_record_present() && !s.swept());
        s.covering_record = Some("r".into());
        s.commit_before = Some(true);
        s.commit_after = Some(true);
        assert!(s.compacted_record_present() && !s.swept());
        assert_eq!(s.label(), "compacted");
        s.head_named_output = true;
        assert_eq!(s.label(), "HEAD named the compaction output");
        // Deleted during the read: neither present throughout nor swept before.
        s.commit_after = Some(false);
        assert!(!s.compacted_record_present() && !s.swept());
        s.commit_before = Some(false);
        assert!(s.swept());
        assert_eq!(s.label(), "input commit record swept");
        // An unknown presence claims nothing.
        s.commit_before = None;
        s.commit_after = None;
        assert!(!s.swept() && !s.compacted_record_present());
    }

    fn model_with(id: u64, records: u32) -> Model {
        let mut m = Model::default();
        m.writes.insert(
            id,
            Write {
                id,
                signal: Signal::Logs,
                mode: Mode::Strict,
                key: None,
                records,
                samples: Vec::new(),
                ts_ns: 0,
                attempts: vec![Attempt::Acked {
                    header: TOKEN.into(),
                }],
            },
        );
        m.mark_acked(id);
        m
    }

    #[test]
    fn exactly_once_holds_and_every_deviation_fails() {
        let m = model_with(4, 3);
        let exact: HashMap<(u64, u32), u64> = (0..3).map(|r| ((4, r), 1)).collect();
        let j = judge(&m, exact.clone(), 4, false);
        assert!(j.holds(), "{j:?}");
        assert_eq!(j.rows, 3);

        // An obsolete input served beside its replacement: a record twice.
        let mut twice = exact.clone();
        twice.insert((4, 1), 2);
        let j = judge(&m, twice, 4, false);
        assert_eq!(j.over.len(), 1, "{j:?}");
        assert!(!j.holds());

        // An omission.
        let mut short = exact.clone();
        short.remove(&(4, 2));
        let j = judge(&m, short, 4, false);
        assert_eq!(j.missing.len(), 1, "{j:?}");

        // A 200 with no rows is a verdict too.
        let j = judge(&m, HashMap::new(), 4, false);
        assert_eq!(j.missing.len(), 3);

        // A record the write never had.
        let mut phantom = exact.clone();
        phantom.insert((4, 3), 1);
        assert_eq!(judge(&m, phantom, 4, false).phantoms.len(), 1);

        // The self-check duplicates one returned record.
        let j = judge(&m, exact, 4, true);
        assert!(j.injected && !j.holds() && j.over.len() == 1, "{j:?}");
        assert!(!judge(&m, HashMap::new(), 4, true).injected);
    }

    #[test]
    fn refusals_are_classed_and_only_unsatisfiable_is_a_verdict() {
        let unsat = SqlFailure::Status(
            503,
            format!(
                "{{\"status\":\"error\",\"errorType\":\"unavailable\",\"error\":\"{MSG_UNSATISFIABLE}\"}}"
            ),
        );
        assert_eq!(failure_class(&unsat), "unsatisfiable");
        assert_eq!(
            failure_class(&SqlFailure::Status(
                503,
                "upstream storage temporarily unavailable".into()
            )),
            "unavailable"
        );
        assert_eq!(
            failure_class(&SqlFailure::Status(504, String::new())),
            "timeout"
        );
        assert_eq!(
            failure_class(&SqlFailure::NoResponse("reset".into())),
            "no_response"
        );
        assert!(Outcome::Unsatisfiable("x".into()).conclusive());
        assert!(!Outcome::Unsatisfiable("x".into()).holds());
        let inc = Outcome::Inconclusive {
            class: "timeout",
            detail: String::new(),
        };
        assert!(!inc.conclusive() && !inc.holds());
    }

    fn ack(node: usize, writer: &str, sent: u64, answered: u64) -> Ack {
        at(node, writer, sent, answered, Some(4))
    }

    fn at(node: usize, writer: &str, sent: u64, answered: u64, count: Option<u32>) -> Ack {
        Ack {
            node,
            writer: writer.into(),
            sent,
            answered,
            count,
        }
    }

    #[test]
    fn a_shard_set_rebuilt_for_a_reshard_is_not_a_restart() {
        let mut l = AckLedger::default();
        l.record(1, at(0, "a", 1, 2, Some(4)));
        // The increase activates: the same process builds its 6-shard set
        // under a new identity.
        l.record(2, at(0, "b", 3, 4, Some(6)));
        assert!(l.restarted_since(1, 0).is_none());
        // The decrease activates: the process routes on its 4-shard set
        // again, under the identity it had.
        l.record(3, at(0, "a", 5, 6, Some(4)));
        assert!(l.restarted_since(1, 0).is_none());
        assert!(l.restarted_since(2, 0).is_none());
        // An attempt whose hours the record did not pin to one count is no
        // evidence either way, as an anchor or as the change.
        l.record(4, at(0, "c", 7, 8, None));
        l.record(5, at(0, "d", 9, 10, None));
        assert!(l.restarted_since(1, 0).is_none());
        assert!(l.restarted_since(4, 0).is_none());
        // A new identity at a count the node already routed at: restarted.
        l.record(6, at(0, "e", 11, 12, Some(6)));
        let (x, y) = l.restarted_since(2, 0).expect("restart");
        assert_eq!((x.writer.as_str(), y.writer.as_str()), ("b", "e"));
        let (x, y) = l.restarted_since(1, 0).expect("restart");
        assert_eq!((x.writer.as_str(), y.writer.as_str()), ("b", "e"));
    }

    #[test]
    fn a_new_writer_identity_after_the_write_is_a_restart() {
        let mut l = AckLedger::default();
        l.record(1, ack(0, "a", 1, 2));
        l.record(2, ack(1, "b", 3, 4));
        // Node 0 answers under a new identity after the write: restarted.
        l.record(3, ack(0, "c", 5, 6));
        let (x, y) = l.restarted_since(1, 0).expect("restart");
        assert_eq!((x.writer.as_str(), y.writer.as_str()), ("a", "c"));
        // Node 1 kept its identity.
        l.record(4, ack(1, "b", 7, 8));
        assert!(l.restarted_since(1, 1).is_none());
        assert!(l.restarted_since(2, 1).is_none());
        // Write 3 came after the change on node 0: no restart since it.
        assert!(l.restarted_since(3, 0).is_none());
    }

    #[test]
    fn a_change_that_may_predate_the_write_is_not_counted() {
        let mut l = AckLedger::default();
        l.record(1, ack(0, "a", 1, 2));
        // Write 2 lands on node 1 while node 0 restarts unseen.
        l.record(2, ack(1, "b", 3, 4));
        // Node 0's first ack after write 2 is already under the new identity:
        // the restart may have happened before write 2 was acknowledged.
        l.record(3, ack(0, "c", 5, 6));
        assert!(l.restarted_since(2, 0).is_none());
        // A second change after that one is after write 2.
        l.record(4, ack(0, "d", 7, 8));
        assert!(l.restarted_since(2, 0).is_some());
        // An overlapping pair (sent before the anchor's answer) is no order.
        let mut l = AckLedger::default();
        l.record(1, ack(0, "a", 1, 4));
        l.record(2, ack(0, "e", 3, 5));
        assert!(l.restarted_since(1, 0).is_none());
    }
}
