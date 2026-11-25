//! The late-records race (README.md, "The late-records race"; ADR-4326's
//! late-replay amendment): a server's level-0 commit record PUT is refused by
//! the proxy on every attempt and abandoned, and the driver delivers the same
//! bytes to the same key through raw S3 once every clock is past the bucket's
//! late-commit cutoff, so the maintenance servers plan a bucket that holds a
//! late commit record (ADR-4561).

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use antithesis_sdk::{assert_always, assert_sometimes};
use serde_json::json;

use ravel_antithesis_catalog_driver::keys::{self, L0Key};
use ravel_antithesis_catalog_driver::proxy::{self, Captured};
use ravel_antithesis_driver::model::Attempt;

use crate::audit::{self, DATA_SUFFIX, InputKeys};
use crate::clock;
use crate::{CLI_TIMEOUT_S, NS_PER_SECOND, Shared, WRITE_TIMEOUT, lock, now_ns};

/// Races the eventually runs until one reached its situation.
pub const RACES_EVENTUALLY: u32 = 3;
/// The hour a race writes into must have this long left when the write is
/// sent, so the capture, the top-up writes and the abandonment all happen in
/// it. In the eventually a shorter remainder is skipped by a move to the next
/// hour's start.
pub const MIN_LEFT_S: u64 = 600;
/// How far into the next hour that move lands.
const HOUR_START_SLACK_S: u64 = 5;
/// How far past the cutoff the clock move puts the driver's clock.
pub const AIM_PAST_CUTOFF_S: u64 = 5;
/// The pause after the move, beside the move's own reach wait.
const SETTLE_S: u64 = 3;
/// `LATE_COMMIT_SETTLE_NS` in crates/ravel-maintain/src/config.rs: no
/// maintenance pass plans the bucket before every clock reads the cutoff plus
/// this.
pub const FENCE_SETTLE_S: u64 = 60;
/// How long the race polls `captured` after the write returned.
const CAPTURE_WAIT: Duration = Duration::from_secs(30);
/// No PUT of the captured key for this long, once the write's node counted
/// an abandoned flush, means its server gave up: above `object_store`'s 15 s
/// cap on one backoff and the flush loop's 2 s.
const QUIET: Duration = Duration::from_secs(20);
/// The longest the race waits for the server to abandon the flush: its whole
/// commit-PUT budget. The flush's publish loop makes five attempts
/// (`put_retry_max_attempts` 4 in crates/ravel-ingest), each one store-client
/// operation of up to its 180 s retry timeout plus one 20 s request
/// (crates/ravel-object-store/src/s3.rs), with at most 2 s of backoff between
/// attempts; then the quiet span itself. The 3600 s flush deadline is longer.
pub const ABANDON_WAIT_S: u64 = 5 * (180 + 20) + 4 * 2 + QUIET.as_secs();
/// The write's node's `/metrics` read, at its bound.
const METRICS_TIMEOUT: Duration = Duration::from_secs(5);
/// One abandonment poll round past [`ABANDON_WAIT_S`]: `captured`, the
/// metrics read and the pause.
const ABANDON_POLL: Duration = Duration::from_secs(1);
/// Top-up writes, at most, that give the bucket its two on-time inputs.
const FILL_WRITES: u32 = 4;
/// Maintenance plans a bucket from this many on-time commit records at least
/// (`DEFAULT_MIN_COMPACTION_INPUTS`); the late record is not one of them.
const MIN_ON_TIME: usize = 2;
/// How long the race waits for the maintenance servers' compaction record of
/// the bucket after the replay.
pub const RECORD_WAIT: Duration = Duration::from_secs(180);
const RECORD_POLL: Duration = Duration::from_secs(5);

/// One raw S3 request at its bound: curl's `--max-time`.
const S3_S: u64 = crate::s3::MAX_TIME_S;
/// One listing page at its bound: three attempts, a second apart.
const LIST_S: u64 = 3 * S3_S + 2;
/// One proxy control command at its bound.
const PROXY_CMD_S: u64 = proxy::CONTROL_DEADLINE.as_secs();
/// `clock::reach_wait_s`.
const REACH_WAIT_S: u64 = clock::FAKETIME_CACHE_DURATION_S + 1;
/// `shift_clock` after its offset write: the wait for the move to reach the
/// driver (one reread plus 30 s), its reach wait, and its GET of HEAD.
const SHIFT_TAIL_S: u64 = clock::FAKETIME_CACHE_DURATION_S + 30 + REACH_WAIT_S + S3_S;
/// One `nudge` at its bound past its quiet wait: `shift_clock`'s tail, then the
/// settle's reach wait, its 120 s serving wait and one serving round past it
/// (two ravel nodes' readiness and SQL, two maintenance nodes' readiness, the
/// 2 s pause).
const NUDGE_TAIL_S: u64 =
    SHIFT_TAIL_S + REACH_WAIT_S + 120 + 2 * (2 + crate::READ_TIMEOUT.as_secs()) + 2 * 2 + 2;

/// From the clock move's offset write to the end of the race, every leg at
/// its bound: the move's own tail, the settle, the replay, the LIST that
/// judges it, the wait for the compaction record with one poll round past
/// it, the record's decode, and the two maintenance nodes' `/metrics` reads.
pub const AFTER_MOVE_S: u64 = SHIFT_TAIL_S
    + SETTLE_S
    + S3_S
    + LIST_S
    + RECORD_WAIT.as_secs()
    + LIST_S
    + RECORD_POLL.as_secs()
    + CLI_TIMEOUT_S as u64
    + 2 * METRICS_TIMEOUT.as_secs();
/// The legs after the move start at the cutoff plus the aim and end before
/// the fold seals the hour, `FOLD_SAFETY_MARGIN_S` after the cutoff, with a
/// reach wait to spare for a server clock ahead of the driver's.
const _: () =
    assert!(AIM_PAST_CUTOFF_S + AFTER_MOVE_S + REACH_WAIT_S <= clock::FOLD_SAFETY_MARGIN_S);
/// What is left of [`AFTER_MOVE_S`] once `shift_clock` returned.
const AFTER_SHIFT_S: u64 = AFTER_MOVE_S - SHIFT_TAIL_S;

/// One race at its bound, for the local loop's `END_LIMIT`, the clock moves'
/// quiet waits aside (the local loop counts each move separately): the move
/// into the next hour with its settle, `capture-commit`, the write, the
/// capture poll with one command past it, the top-up writes (three attempts
/// and one LIST each), the `/metrics` read before the write, the abandonment
/// wait with one poll round past it, `pass`, the data object's LIST and HEAD,
/// then everything after the move.
pub const RACE_S: u64 = NUDGE_TAIL_S
    // `pass` and `capture-commit`.
    + 2 * PROXY_CMD_S
    + WRITE_TIMEOUT.as_secs()
    + CAPTURE_WAIT.as_secs()
    + PROXY_CMD_S
    + FILL_WRITES as u64 * ((crate::MAX_RETRIES as u64 + 1) * WRITE_TIMEOUT.as_secs() + LIST_S)
    + METRICS_TIMEOUT.as_secs()
    + ABANDON_WAIT_S
    + PROXY_CMD_S
    + METRICS_TIMEOUT.as_secs()
    + ABANDON_POLL.as_secs()
    + PROXY_CMD_S
    + LIST_S
    + S3_S
    + AFTER_MOVE_S
    // The closing `pass`.
    + PROXY_CMD_S;

/// The bucket's late-commit cutoff C(H) in unix seconds: the hour's end plus
/// `max_flush_lifetime + clock_skew_allowance` (`Bucket::late_commit_cutoff_ns`,
/// ADR-4561 R1), a whole second already.
pub fn cutoff_s(hour: u64) -> u64 {
    (hour + 1) * 3600 + clock::compaction_seal_margin_s()
}

/// The unix second the fold seals `hour`.
pub fn fold_seal_s(hour: u64) -> u64 {
    (hour + 1) * 3600 + clock::seal_margin_s()
}

/// How far to move every clock at `now_s` so the driver's reads the cutoff of
/// `hour` plus [`AIM_PAST_CUTOFF_S`]; 0 when it already is past that.
pub fn move_s(now_s: u64, hour: u64) -> u64 {
    (cutoff_s(hour) + AIM_PAST_CUTOFF_S).saturating_sub(now_s)
}

/// How far to move every clock at `now_s` so the current hour has at least
/// [`MIN_LEFT_S`] left: to just past the next hour's start, or 0.
pub fn hour_start_move_s(now_s: u64) -> u64 {
    let left = 3600 - now_s % 3600;
    if left >= MIN_LEFT_S {
        0
    } else {
        left + HOUR_START_SLACK_S
    }
}

/// Whether the legs left once `shift_clock` returned at `now_s` still end
/// before `hour`'s fold seal.
pub fn fits_after_shift(now_s: u64, hour: u64) -> bool {
    now_s + AFTER_SHIFT_S + REACH_WAIT_S <= fold_seal_s(hour)
}

/// The judgement on the LIST taken after the replay: the replayed record's
/// stamp is at or after the cutoff (ADR-4561 R2), the bucket holds no
/// compaction record yet, and the driver's clock is still under the cutoff
/// plus the fence's settle, so no server can have planned the bucket before
/// the record was there.
pub fn judge_replay(
    stamp_ms: Option<i64>,
    cutoff_s: u64,
    records: &[String],
    now_s: u64,
) -> Result<(), String> {
    let cutoff_ms = i64::try_from(cutoff_s * 1000).unwrap_or(i64::MAX);
    match stamp_ms {
        None => return Err("the LIST does not show the replayed record's stamp".to_string()),
        Some(ms) if ms < cutoff_ms => {
            return Err(format!(
                "the replayed record is stamped {ms} ms, before the cutoff {cutoff_ms} ms"
            ));
        }
        Some(_) => {}
    }
    if !records.is_empty() {
        return Err(format!(
            "the bucket already holds a compaction record: {records:?}"
        ));
    }
    if now_s >= cutoff_s + FENCE_SETTLE_S {
        return Err(format!(
            "the driver's clock read {now_s}, past the cutoff plus the settle ({})",
            cutoff_s + FENCE_SETTLE_S
        ));
    }
    Ok(())
}

/// Whether a decoded compaction record names `key` as an input.
pub fn names(inputs: &[InputKeys], key: &str) -> bool {
    inputs.iter().any(|i| i.commit == key)
}

/// What one race found, for its details.
#[derive(Default)]
struct Found {
    hour: Option<u64>,
    write: Option<u64>,
    attempt: Option<String>,
    captured_key: Option<String>,
    checksum: Option<String>,
    body_bytes: Option<usize>,
    puts: Option<u64>,
    quiet_ms: Option<u128>,
    abandoned_after_s: Option<f64>,
    data_key: Option<String>,
    on_time_l0: Option<usize>,
    fills: u32,
    moved_by_s: Option<u64>,
    stamp_ms: Option<i64>,
    replayed_at_s: Option<u64>,
    record: Option<String>,
    record_inputs: Option<usize>,
}

/// One late-records race: the steps of README.md, "The late-records race".
/// The caller holds the gate's write guard from before the capture to the end.
/// Returns whether the race reached its situation.
pub fn race(s: &Shared, reason: &str, node: usize) -> bool {
    const WHAT: &str = "late-records race";
    let Some(prefix) = s.known_prefix() else {
        println!("{WHAT}: no HEAD key known yet");
        return false;
    };
    s.retry_pending_undos(WHAT);
    if !lock(&s.pending_undos).is_empty() {
        println!("{WHAT}: a tamper undo is pending, so no clock may move");
        return false;
    }
    if !s.nudge(WHAT, &hour_start_move_s) {
        return false;
    }
    let now_s = now_ns() / NS_PER_SECOND;
    if hour_start_move_s(now_s) != 0 {
        println!("{WHAT}: the hour has under {MIN_LEFT_S} s left after the move");
        return false;
    }
    let n = s.late_races.fetch_add(1, Ordering::SeqCst);
    let mut found = Found {
        hour: Some(now_s / 3600),
        ..Found::default()
    };
    let outcome = run(s, &prefix, node, &mut found);
    // Whatever happened, no rule stands past the race.
    let _ = s.proxy_cmd("pass");
    let reached = outcome.is_ok();
    let details = json!({
        "reason": reason,
        "race": n,
        "node": node,
        "hour": found.hour,
        "write": found.write,
        "attempt": found.attempt,
        "captured_key": found.captured_key,
        "checksum": found.checksum,
        "body_bytes": found.body_bytes,
        "refused_puts": found.puts,
        "quiet_ms": found.quiet_ms,
        "abandoned_after_s": found.abandoned_after_s,
        "data_key": found.data_key,
        "on_time_l0": found.on_time_l0,
        "fill_writes": found.fills,
        "moved_by_s": found.moved_by_s,
        "cutoff_s": found.hour.map(cutoff_s),
        "replayed_stamp_ms": found.stamp_ms,
        "replayed_at_s": found.replayed_at_s,
        "compaction_record": found.record,
        "record_inputs": found.record_inputs,
        "not_reached": outcome.as_ref().err(),
        "race_budget_s": RACE_S,
    });
    println!("{WHAT}: {details}");
    assert_sometimes!(
        reached,
        "catalog: a server planned a bucket holding a late commit record",
        &details
    );
    if reached {
        s.late_reached.store(true, Ordering::SeqCst);
    }
    reached
}

fn run(s: &Shared, prefix: &str, node: usize, found: &mut Found) -> Result<(), String> {
    const WHAT: &str = "late-records race";
    let hour = found.hour.unwrap_or_default();
    // Step 2: refuse and capture the next level-0 commit record PUT, which
    // under the write gate is this write's flush.
    s.proxy_cmd("pass")?;
    s.proxy_cmd("capture-commit")?;
    let abandoned_before = metric(s, &s.ravel.nodes[node], ABANDONED);
    let sent = Instant::now();
    let Some((id, attempt)) = s.write_once(1, 1, node) else {
        return Err("the model is full".to_string());
    };
    found.write = Some(id);
    found.attempt = Some(format!("{attempt:?}"));
    if matches!(attempt, Attempt::Acked { .. }) {
        return Err("the write was acknowledged, so nothing was captured".to_string());
    }
    let captured = poll_captured(s, |c| c.body.is_some(), CAPTURE_WAIT).ok_or_else(|| {
        format!(
            "no commit record PUT was captured within {} s",
            CAPTURE_WAIT.as_secs()
        )
    })?;
    let checksum_len = captured.checksum.as_ref().map_or(0, |(_, v)| v.len());
    println!(
        "{WHAT}: captured {} (checksum {}, {checksum_len} chars; body {} bytes; {} PUTs refused)",
        captured.key,
        captured
            .checksum
            .as_ref()
            .map_or("-", |(name, _)| name.as_str()),
        captured.body.as_ref().map_or(0, Vec::len),
        captured.puts
    );
    found.captured_key = Some(captured.key.clone());
    found.checksum = captured
        .checksum
        .as_ref()
        .map(|(name, v)| format!("{name}:{v}"));
    found.body_bytes = captured.body.as_ref().map(Vec::len);
    let key = keys::parse_l0_commit_key(&captured.key)
        .ok_or_else(|| format!("{} is not a level-0 commit record key", captured.key))?;
    if key.tenant_prefix != prefix || key.hour != hour {
        return Err(format!(
            "{} is not of this tenant's hour {hour}, so not this write's",
            captured.key
        ));
    }
    let Some(checksum) = captured.checksum.clone() else {
        return Err("the captured PUT carried no checksum header".to_string());
    };
    let Some(body) = captured.body.clone() else {
        return Err("the captured PUT's body was not kept".to_string());
    };
    // Two on-time inputs, written while the hour still has time left, so the
    // servers' plan of the bucket publishes a record.
    let bucket = keys::bucket_prefix(prefix, key.shard, key.hour);
    top_up(s, &bucket, key.hour, found)?;
    // The server keeps retrying the refused PUT inside its commit-PUT budget,
    // and the rule refuses each retry; only once it gave up is the key the
    // driver's to deliver.
    let quiet = wait_abandoned(s, node, abandoned_before, sent)?;
    found.puts = Some(quiet.puts);
    found.quiet_ms = Some(quiet.quiet.as_millis());
    found.abandoned_after_s = Some(sent.elapsed().saturating_sub(quiet.quiet).as_secs_f64());
    println!(
        "{WHAT}: the server gave {} up after {} refused PUTs, {:.1} s after the write",
        captured.key,
        quiet.puts,
        found.abandoned_after_s.unwrap_or_default()
    );
    s.proxy_cmd("pass")?;
    // Step 3: the data object the record names is stored.
    found.data_key = Some(data_object(s, &key)?);
    // Step 4: every clock to the cutoff plus the aim.
    let c_s = cutoff_s(hour);
    if now_ns() / NS_PER_SECOND >= c_s {
        return Err("the driver's clock is already past the cutoff".to_string());
    }
    let jump = s
        .shift_clock(WHAT, &|now_s| move_s(now_s, hour))
        .map_err(|e| format!("the clock move was given up: {e}"))?;
    if !jump.reached_driver {
        return Err("the clock move did not reach the driver".to_string());
    }
    if jump.by_s > 0 {
        println!(
            "clock: moved forward by {} s outside the planned jumps ({WHAT})",
            jump.by_s
        );
    }
    found.moved_by_s = Some(jump.by_s);
    std::thread::sleep(Duration::from_secs(SETTLE_S));
    let now_s = now_ns() / NS_PER_SECOND;
    if !fits_after_shift(now_s, hour) {
        return Err(format!(
            "at {now_s} the rest of the race ({AFTER_SHIFT_S} s) would pass the fold seal {}",
            fold_seal_s(hour)
        ));
    }
    // Step 5: the replay, create-only, byte-identical, with the server's
    // checksum; then the LIST that judges it.
    let headers = [format!("{}: {}", checksum.0, checksum.1)];
    match s.store.put_create(&captured.key, &body, &headers) {
        Ok(true) => {}
        Ok(false) => return Err("the replay found the key stored (412)".to_string()),
        Err(e) => return Err(format!("the replay failed: {e}")),
    }
    let stamped = s.store.list_stamped(&bucket)?;
    let now_s = now_ns() / NS_PER_SECOND;
    found.replayed_at_s = Some(now_s);
    let stamp = stamped
        .iter()
        .find(|(k, _)| *k == captured.key)
        .and_then(|(_, ms)| *ms);
    found.stamp_ms = stamp;
    let listed: Vec<String> = stamped.into_iter().map(|(k, _)| k).collect();
    let records = keys::bucket(&bucket, &listed).records;
    println!(
        "{WHAT}: replayed {}: last_modified {stamp:?} ms against the cutoff C(H) {} ms; driver clock {now_s} s",
        captured.key,
        c_s * 1000
    );
    judge_replay(stamp, c_s, &records, now_s)?;
    // Step 6: the maintenance servers' compaction record of the bucket.
    let record = wait_for_record(s, &bucket)?;
    found.record = Some(record.clone());
    let out = s
        .cli
        .run(&["commit", "decode-compaction", &record], CLI_TIMEOUT_S);
    if !out.ok() {
        return Err(format!(
            "commit decode-compaction {record}: {}",
            out.summary()
        ));
    }
    let inputs = audit::compaction_inputs(prefix, &record, &out.stdout)
        .ok_or_else(|| format!("commit decode-compaction {record} printed no input list"))?;
    found.record_inputs = Some(inputs.len());
    let named = names(&inputs, &captured.key);
    println!(
        "{WHAT}: {record} names {} inputs; the late record {} is {}",
        inputs.len(),
        captured.key,
        if named {
            "among them"
        } else {
            "not among them"
        }
    );
    assert_always!(
        !named,
        "catalog: a server's compaction record names no late commit record",
        &json!({
            "record": record,
            "late_commit_key": captured.key,
            "inputs": inputs.iter().map(|i| i.commit.clone()).collect::<Vec<_>>(),
            "cutoff_s": c_s,
            "replayed_stamp_ms": stamp,
        })
    );
    lock(&s.inputs_of).insert(record, inputs);
    for maint in &s.maint.nodes {
        println!(
            "{WHAT}: {maint} {LATE_RECORDS}{{signal=\"logs\"}} = {:?}",
            metric(s, maint, LATE_RECORDS)
        );
    }
    Ok(())
}

/// The ingest metric counting flushes abandoned past their retry budget.
const ABANDONED: &str = "ravel_ingest_abandoned_retry_exhausted_total";
/// The maintenance metric counting late commit records plans left out
/// (ADR-4561 R8).
const LATE_RECORDS: &str = "ravel_maintain_late_commit_records_total";

/// The sum of `name`'s logs samples on `node`'s `/metrics`; `None` when the
/// page cannot be read or has no such sample.
fn metric(s: &Shared, node: &str, name: &str) -> Option<f64> {
    let page = s
        .admin
        .get(format!("{node}/metrics"))
        .timeout(METRICS_TIMEOUT)
        .send()
        .ok()
        .filter(|r| r.status().is_success())?
        .text()
        .ok()?;
    metric_sum(&page, name)
}

/// The sum of `name`'s samples labelled `signal="logs"` in a Prometheus text
/// page; `None` when there is none.
pub fn metric_sum(page: &str, name: &str) -> Option<f64> {
    let values: Vec<f64> = page
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix(name)?;
            let (labels, value) = rest.strip_prefix('{')?.split_once('}')?;
            labels
                .split(',')
                .any(|l| l.trim() == "signal=\"logs\"")
                .then(|| value.trim().parse().ok())?
        })
        .collect();
    (!values.is_empty()).then(|| values.iter().sum())
}

/// Wait, up to [`ABANDON_WAIT_S`], until the write's node counted one more
/// abandoned flush than `before` (or its metrics cannot be read) and no PUT
/// of the captured key came for [`QUIET`]. The capture rule stands all the
/// while, so no retry lands on time.
fn wait_abandoned(
    s: &Shared,
    node: usize,
    before: Option<f64>,
    sent: Instant,
) -> Result<Captured, String> {
    let deadline = sent + Duration::from_secs(ABANDON_WAIT_S);
    loop {
        let c = s
            .proxy_cmd("captured")
            .ok()
            .and_then(|r| proxy::parse_captured(&r));
        let now = metric(s, &s.ravel.nodes[node], ABANDONED);
        let counted = match (before, now) {
            (Some(b), Some(n)) => n > b,
            _ => true,
        };
        match c {
            Some(c) if counted && c.quiet >= QUIET => return Ok(c),
            Some(c) if Instant::now() >= deadline => {
                return Err(format!(
                    "the server had not abandoned {} {ABANDON_WAIT_S} s after the write: {} PUTs \
                     refused, quiet {} ms, abandoned flushes {before:?} then {now:?}",
                    c.key,
                    c.puts,
                    c.quiet.as_millis()
                ));
            }
            None => return Err("the proxy no longer reports the capture".to_string()),
            Some(_) => std::thread::sleep(ABANDON_POLL),
        }
    }
}

/// The proxy's `captured` once `done` holds of it, polled for up to `wait`.
fn poll_captured(s: &Shared, done: impl Fn(&Captured) -> bool, wait: Duration) -> Option<Captured> {
    let deadline = Instant::now() + wait;
    loop {
        let c = s
            .proxy_cmd("captured")
            .ok()
            .and_then(|r| proxy::parse_captured(&r));
        if c.as_ref().is_some_and(&done) {
            return c;
        }
        if Instant::now() >= deadline {
            if let Some(c) = &c {
                println!(
                    "late-records race: captured {} so far: {} PUTs refused, quiet {} ms",
                    c.key,
                    c.puts,
                    c.quiet.as_millis()
                );
            }
            return None;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Write until the bucket under `bucket` holds [`MIN_ON_TIME`] level-0 records
/// and nothing else, up to [`FILL_WRITES`] writes, all in `hour`.
fn top_up(s: &Shared, bucket: &str, hour: u64, found: &mut Found) -> Result<(), String> {
    let n = s.ravel.nodes.len().max(1);
    let mut next = 0usize;
    loop {
        let listed = s.store.list(bucket)?;
        let b = keys::bucket(bucket, &listed);
        found.on_time_l0 = Some(b.l0);
        if !b.records.is_empty() || b.other {
            return Err(format!(
                "the bucket holds more than level-0 records: {listed:?}"
            ));
        }
        if b.l0 >= MIN_ON_TIME {
            return Ok(());
        }
        if found.fills >= FILL_WRITES || now_ns() / crate::NS_PER_HOUR != hour {
            return Err(format!(
                "the bucket holds {} level-0 records after {} top-up writes",
                b.l0, found.fills
            ));
        }
        found.fills += 1;
        // Eight streams reach a given one of four shards nine times in ten.
        s.write(8, 8, &mut || {
            next += 1;
            next % n
        });
    }
}

/// The key of the data object `key` names: the one `.rseg` under its data
/// prefix, confirmed by a HEAD.
fn data_object(s: &Shared, key: &L0Key) -> Result<String, String> {
    let listed: BTreeSet<String> = s.store.list(&key.data_prefix())?.into_iter().collect();
    let data: Vec<&String> = listed.iter().filter(|k| k.ends_with(DATA_SUFFIX)).collect();
    let [data] = data[..] else {
        return Err(format!(
            "{} data objects under {}, not one: the replay must not publish a record without its data",
            data.len(),
            key.data_prefix()
        ));
    };
    if !s.store.exists(data)? {
        return Err(format!("{data} is listed but a HEAD finds nothing"));
    }
    Ok(data.clone())
}

/// The first compaction record in the bucket, polled for up to
/// [`RECORD_WAIT`].
fn wait_for_record(s: &Shared, bucket: &str) -> Result<String, String> {
    let deadline = Instant::now() + RECORD_WAIT;
    loop {
        if let Ok(listed) = s.store.list(bucket)
            && let Some(record) = keys::bucket(bucket, &listed).records.into_iter().next()
        {
            return Ok(record);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "no server published a compaction record of the bucket within {} s",
                RECORD_WAIT.as_secs()
            ));
        }
        std::thread::sleep(RECORD_POLL);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// 2026-11-24T10:00Z, the bucket hour of a maint1 warn line in a plain
    /// local run: `ingest_hour_bucket=498754 ... cutoff_ns=1795521900000000000`.
    const HOUR: u64 = 498_754;

    #[test]
    fn the_cutoff_is_the_one_the_maintenance_servers_log() {
        assert_eq!(cutoff_s(HOUR) * NS_PER_SECOND, 1_795_521_900_000_000_000);
        assert_eq!(cutoff_s(HOUR), (HOUR + 1) * 3600 + 3900);
        assert_eq!(
            fold_seal_s(HOUR) - cutoff_s(HOUR),
            clock::FOLD_SAFETY_MARGIN_S
        );
    }

    #[test]
    fn the_move_lands_the_driver_just_past_the_cutoff() {
        let c = cutoff_s(HOUR);
        let mid_hour = HOUR * 3600 + 1200;
        assert_eq!(mid_hour + move_s(mid_hour, HOUR), c + AIM_PAST_CUTOFF_S);
        assert_eq!(move_s(c + AIM_PAST_CUTOFF_S, HOUR), 0);
        assert_eq!(move_s(c + 100, HOUR), 0);
    }

    #[test]
    fn a_short_hour_is_skipped_to_the_next_hours_start() {
        let start = HOUR * 3600;
        assert_eq!(hour_start_move_s(start), 0);
        assert_eq!(hour_start_move_s(start + 3600 - MIN_LEFT_S), 0);
        assert_eq!(
            hour_start_move_s(start + 3600 - MIN_LEFT_S + 1),
            MIN_LEFT_S - 1 + HOUR_START_SLACK_S
        );
        let late = start + 3599;
        assert_eq!(
            late + hour_start_move_s(late),
            start + 3600 + HOUR_START_SLACK_S
        );
    }

    #[test]
    fn the_legs_after_the_move_fit_before_the_fold_seal() {
        // 65 + 3 + 30 + 92 + 180 + 92 + 5 + 300 + 10 = 777 s, and
        // 5 + 777 + 3 <= 900.
        assert_eq!(AFTER_MOVE_S, 777);
        let landed = cutoff_s(HOUR) + AIM_PAST_CUTOFF_S + SHIFT_TAIL_S + SETTLE_S;
        assert!(fits_after_shift(landed, HOUR));
        let last = fold_seal_s(HOUR) - AFTER_SHIFT_S - REACH_WAIT_S;
        assert!(fits_after_shift(last, HOUR));
        assert!(!fits_after_shift(last + 1, HOUR));
    }

    #[test]
    fn a_replay_counts_only_stamped_late_into_an_unplanned_bucket_in_time() {
        let c = cutoff_s(HOUR);
        let at = i64::try_from(c * 1000).expect("a millisecond stamp");
        let now = c + 13;
        assert_eq!(judge_replay(Some(at), c, &[], now), Ok(()));
        assert_eq!(
            judge_replay(Some(at + 999), c, &[], c + FENCE_SETTLE_S - 1),
            Ok(())
        );
        assert!(judge_replay(Some(at - 1), c, &[], now).is_err());
        assert!(judge_replay(None, c, &[], now).is_err());
        let record = vec!["t/0a0b/l/c/0001/20261124T10/l1.0011223344556677.cmt".to_string()];
        assert!(judge_replay(Some(at), c, &record, now).is_err());
        assert!(judge_replay(Some(at), c, &[], c + FENCE_SETTLE_S).is_err());
    }

    #[test]
    fn a_record_names_the_late_key_only_as_an_input() {
        let late = "t/0a0b/l/c/0001/20261124T10/w.1.00000000000000000003.cmt";
        let input = |commit: &str| InputKeys {
            commit: commit.to_string(),
            data_prefix: String::new(),
        };
        let on_time = [
            input("t/0a0b/l/c/0001/20261124T10/w.1.00000000000000000001.cmt"),
            input("t/0a0b/l/c/0001/20261124T10/w.1.00000000000000000002.cmt"),
        ];
        assert!(!names(&on_time, late));
        let mut all = on_time.to_vec();
        all.push(input(late));
        assert!(names(&all, late));
    }

    #[test]
    fn the_abandonment_wait_covers_the_whole_commit_put_budget() {
        // Five attempts of 180 s plus a 20 s request, 2 s between, then 20 s.
        assert_eq!(ABANDON_WAIT_S, 1028);
    }

    #[test]
    fn logs_samples_of_a_metric_are_summed() {
        let page = "# TYPE ravel_ingest_abandoned_retry_exhausted_total counter\n\
                    ravel_ingest_abandoned_retry_exhausted_total{mode=\"all\",signal=\"logs\"} 2\n\
                    ravel_ingest_abandoned_retry_exhausted_total{mode=\"all\",signal=\"metrics\"} 7\n\
                    ravel_ingest_abandoned_retry_exhausted_total_x{signal=\"logs\"} 9\n\
                    ravel_maintain_late_commit_records_total{signal=\"logs\"} 3\n";
        assert_eq!(metric_sum(page, ABANDONED), Some(2.0));
        assert_eq!(metric_sum(page, LATE_RECORDS), Some(3.0));
        assert_eq!(metric_sum(page, "ravel_absent_total"), None);
    }

    #[test]
    fn the_local_loop_budgets_each_late_records_race_at_its_bound() {
        let script = include_str!("../../scripts/local-run.sh");
        let set: Vec<&str> = script
            .lines()
            .filter(|l| l.starts_with("LATE_RACE_S="))
            .collect();
        assert_eq!(set, [format!("LATE_RACE_S={RACE_S}")]);
    }
}
