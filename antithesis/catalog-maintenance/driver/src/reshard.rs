//! The reshard-ownership profile (antithesis/reshard-ownership/README.md):
//! the catalog-maintenance workload with the tenant resharded under it.
//! Compiled only into `driver-reshard` (cargo feature `reshard`; that README
//! says why it is a binary of its own).
//!
//! Before the sealing jump the driver appends an increase from 4 to 6
//! shards through `ravel-cli-ant` with the shortest lead, so the jump
//! activates it. Once it has, the drawn rule `decrease_drawn` runs, rarely
//! and at most once per timeline while faults may run, a decrease back to 4
//! with a burst of writes in its last two seconds, whose stragglers it reads
//! back with their tokens, then a read with a forged token for a record never
//! written. The eventually then forces, in order: writes until both new
//! shards acknowledged one; the stale-writer fence (refused reads of the
//! provisioning record across a move past the writers' lead horizon); the
//! decrease, unless the drawn rule already read a straggler back; and the
//! forged read.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use antithesis_sdk::{assert_always, assert_reachable, assert_sometimes};
use hegel::TestCase;
use hegel::generators as gs;
use ravel_antithesis_driver::model::{self, Write};
use ravel_antithesis_driver::ravel::{self, SqlFailure};
use serde_json::json;

use crate::cli::AntCli;
use crate::token::{self, Token};
use crate::{
    CLI_TIMEOUT_S, NS_PER_HOUR, NS_PER_SECOND, READ_TIMEOUT, Shared, WINDOW_SLACK_NS, audit, clock,
    lock, now_ns, release_and_halt, seed, writes_query,
};

/// The count the increase reshards to; the decrease goes back to
/// [`seed::SHARDS`].
pub const INCREASED: u32 = 6;
/// `provision reshard --lead-hours`: the shortest lead the CLI accepts.
const LEAD_HOURS: u32 = 2;
/// How long the fence waits, after refusing the record's reads, before its
/// grace writes: past the routers' 60 s view refresh interval
/// (`DEFAULT_REFRESH_INTERVAL_NS` in crates/ravel-ingest), so each write finds
/// its view stale and its re-read refused.
const STALE_VIEW_WAIT: Duration = Duration::from_secs(75);
/// The decrease's straggler burst: writes in the last `BURST_WINDOW` before
/// the activation, one every `BURST_SPACING`.
const BURST_WINDOW: Duration = Duration::from_secs(2);
const BURST_SPACING: Duration = Duration::from_millis(50);
/// Where the move before the burst lands, ahead of the activation: room for
/// the move's reread wait and the settle.
const BURST_LEAD_S: u64 = 30;
/// Decrease cycles the eventually runs: the second only if the first left
/// no straggler.
const DECREASE_CYCLES: u32 = 2;
/// Records, and streams, of the activation and burst writes. A stream's
/// shard follows from its resource (`wl-svc-<i>`, the same for every
/// write), so a write reaches a shard only if one of its streams hashes
/// there: 8 streams reached shard 5 and never shard 4 at 6 shards, 32 reach
/// every shard (CHECKS-1, 2c).
const WIDE_WRITE: u32 = 32;
/// Writes the eventually adds, at most, for both new shards to acknowledge.
const ACTIVATION_WRITES: u32 = 24;
/// Stragglers a decrease cycle reads back, at most.
const STRAGGLER_READS: usize = 4;
/// How often `decrease_drawn` runs once its preconditions hold, like the
/// other forced races' rules.
const DECREASE_DRAW: f64 = 0.02;
/// The local loop only (`local/docker-compose.local.yaml`): `1` makes
/// `decrease_drawn` skip its draw, so a local run takes the drawn path.
const FORCE_DRAWN_DECREASE: &str = "WORKLOAD_FORCE_DRAWN_DECREASE";

/// What the profile has done so far.
#[derive(Debug, Default)]
pub struct State {
    /// The activation hour of the increase, once appended.
    increase: Mutex<Option<u32>>,
    /// The activation hour of the latest decrease, once appended.
    decrease: Mutex<Option<u32>>,
    /// The shards at or above [`seed::SHARDS`] some acknowledged write's
    /// token named.
    added_shards: Mutex<BTreeSet<u32>>,
    /// Set once `decrease_drawn` started its cycle; the rule runs it once
    /// per timeline.
    drawn_started: AtomicBool,
    /// Set once `decrease_drawn` found a straggler and read it back; the
    /// eventually runs its own decrease cycles only while this is unset.
    drawn_reached: AtomicBool,
    /// `WORKLOAD_FORCE_DRAWN_DECREASE=1`.
    force_drawn: bool,
}

impl State {
    pub fn from_env() -> State {
        State {
            force_drawn: std::env::var(FORCE_DRAWN_DECREASE).is_ok_and(|v| v == "1"),
            ..State::default()
        }
    }
}

/// Whether `decrease_drawn` may run: the eventually is on, the sealing jump
/// reached the driver, the increase's activation hour has begun and the rule
/// has not started before in this timeline.
pub fn drawn_decrease_due(
    eventually_on: bool,
    jump_landed: bool,
    increase_hour: Option<u32>,
    now_ns: u64,
    started: bool,
) -> bool {
    eventually_on
        && jump_landed
        && !started
        && increase_hour.is_some_and(|h| now_ns >= u64::from(h) * NS_PER_HOUR)
}

/// Sets `flag`; true only for the call that found it unset.
pub fn claim_once(flag: &AtomicBool) -> bool {
    flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
}

/// The from, to and activation hour `provision reshard` printed:
/// `<signal>: resharded shard_count <from> -> <to>, appended generation <g>
/// activating at unix hour <h> (in <n>h)`.
pub fn parse_reshard(out: &str) -> Option<(u32, u32, u32)> {
    let line = out.lines().find(|l| l.contains("resharded shard_count"))?;
    let rest = line.split("resharded shard_count ").nth(1)?;
    let (from, rest) = rest.split_once(" -> ")?;
    let (to, rest) = rest.split_once(',')?;
    let hour = rest.split("activating at unix hour ").nth(1)?;
    let hour = hour.split_whitespace().next()?;
    Some((from.parse().ok()?, to.parse().ok()?, hour.parse().ok()?))
}

/// When each write of the straggler burst is sent, as delays from `now_ns`:
/// one every `spacing` from `window` before the start of `activation_hour`,
/// every one strictly before it. `None` when `now_ns` is already past the
/// burst's start, so the burst cannot be sent whole.
pub fn burst_delays(
    now_ns: u64,
    activation_hour: u32,
    window: Duration,
    spacing: Duration,
) -> Option<Vec<Duration>> {
    let boundary_ns = u64::from(activation_hour) * NS_PER_HOUR;
    let window_ns = u64::try_from(window.as_nanos()).ok()?;
    let spacing_ns = u64::try_from(spacing.as_nanos()).ok()?.max(1);
    let start_ns = boundary_ns.checked_sub(window_ns)?;
    if now_ns > start_ns {
        return None;
    }
    Some(
        (0..)
            .map(|k| start_ns + k * spacing_ns)
            .take_while(|&t| t < boundary_ns)
            .map(|t| Duration::from_nanos(t - now_ns))
            .collect(),
    )
}

/// A token for a record no writer wrote: `t`'s writer, epoch and hour, at
/// shard `shard` (at or above the hour's active count) and a sequence number
/// no writer reaches.
pub fn forge(t: &Token, shard: u32) -> Token {
    Token {
        shard,
        writer: t.writer.clone(),
        epoch: t.epoch,
        seq: u64::MAX,
        hour: t.hour,
    }
}

/// Before the sealing jump: append the increase once, so the jump (9000 s,
/// more than the two-hour lead) activates it.
pub fn before_sealing_jump(s: &Shared) {
    if lock(&s.reshard.increase).is_some() {
        return;
    }
    match provision(s, INCREASED) {
        Ok(hour) => *lock(&s.reshard.increase) = Some(hour),
        Err(e) => println!("reshard: the increase before the sealing jump: {e}"),
    }
}

/// `provision reshard` through `ravel-cli-ant` to `count` shards; returns the
/// activation hour, after reading the record again so the driver's history
/// holds the new generation.
fn provision(s: &Shared, count: u32) -> Result<u32, String> {
    let count_arg = count.to_string();
    let lead_arg = LEAD_HOURS.to_string();
    let out = AntCli::new().run(
        &s.args(&[
            "provision",
            "reshard",
            "--shard-count",
            &count_arg,
            "--lead-hours",
            &lead_arg,
        ]),
        CLI_TIMEOUT_S,
    );
    println!(
        "reshard: provision reshard --shard-count {count}: {}\n{}",
        out.summary(),
        out.stdout.trim()
    );
    if !out.ok() {
        return Err(out.summary());
    }
    let (_, to, hour) = parse_reshard(&out.stdout)
        .ok_or_else(|| format!("no reshard line in {:?}", out.stdout.trim()))?;
    if to != count {
        return Err(format!("resharded to {to}, not {count}"));
    }
    reread_history(s);
    Ok(hour)
}

/// Read the provisioning record now, whatever hour the cache was read for.
fn reread_history(s: &Shared) {
    let hour = now_ns() / NS_PER_HOUR;
    let mut history = lock(&s.history);
    match audit::generations(&s.prov_key, s.store.get(&s.prov_key), seed::SHARDS) {
        Ok(generations) => {
            *history = audit::ShardHistory {
                generations,
                read_hour: hour,
            }
        }
        Err(e) => println!("reshard: reading the record again: {e}"),
    }
}

/// Every acknowledged attempt, with the tokens it returned and the count it
/// was routed at (`Shared::record_ack`).
pub fn observe_ack(s: &Shared, id: u64, sent_ns: u64, tokens: &[Token], count: Option<u32>) {
    let added: Vec<u32> = tokens
        .iter()
        .map(|t| t.shard)
        .filter(|&shard| shard >= seed::SHARDS)
        .collect();
    let shards: Vec<u32> = tokens.iter().map(|t| t.shard).collect();
    let hours: Vec<u32> = tokens.iter().map(|t| t.hour).collect();
    if !added.is_empty() {
        lock(&s.reshard.added_shards).extend(added.iter().copied());
        assert_reachable!(
            "reshard: a generation activated",
            &json!({ "write": id, "shards": shards, "hours": hours, "count": count })
        );
    }
    let resharded = lock(&s.history).generations.len() > 1;
    let Some(count) = count.filter(|_| resharded) else {
        return;
    };
    assert_always!(
        shards.iter().all(|&shard| shard < count),
        "reshard: an acknowledged write sits below the shard count it was routed at",
        &json!({ "write": id, "shards": shards, "hours": hours, "count": count })
    );
    let decrease = *lock(&s.reshard.decrease);
    if let Some(a) = decrease
        && count == seed::SHARDS
        && sent_ns / NS_PER_HOUR >= u64::from(a)
    {
        assert_reachable!(
            "reshard: a write routed after the decrease activated was acknowledged",
            &json!({ "write": id, "shards": shards, "hours": hours, "activation_hour": a })
        );
    }
}

/// The new-count key check, on a HEAD that held still through an audit:
/// every acknowledged token at a shard the increase added, in an hour the
/// increase routes and HEAD has folded, and the data objects HEAD's
/// footprint resolved for those shards (`audit::new_shard_keys`).
pub fn check_new_shards(s: &Shared, prefix: &str, watermark_hour: u64, resolved: &[String]) {
    let tokens: Vec<Token> = {
        let history = lock(&s.history);
        let m = lock(&s.model);
        m.acked_logs
            .iter()
            .filter_map(|id| m.writes.get(id))
            .filter_map(|w| w.acked_header().and_then(|h| token::decode_header(h).ok()))
            .flatten()
            .map(|(_, t)| t)
            .filter(|t| {
                t.shard >= seed::SHARDS
                    && u64::from(t.hour) <= watermark_hour
                    && audit::active_count(&history.generations, u64::from(t.hour)) == INCREASED
            })
            .collect()
    };
    if tokens.is_empty() {
        return;
    }
    let found = audit::new_shard_keys(prefix, resolved, &tokens);
    let want: Vec<u32> = (seed::SHARDS..INCREASED).collect();
    let named: Vec<u32> = found.keys().copied().collect();
    assert_sometimes!(
        want.iter().all(|shard| found.contains_key(shard)),
        "reshard: HEAD's footprint named data of a folded post-activation hour at every shard the increase added",
        &json!({
            "watermark_hour": watermark_hour,
            "tokens": tokens.len(),
            "token_shards": tokens.iter().map(|t| t.shard).collect::<BTreeSet<_>>(),
            "named_shards": named,
            "examples": found.values().filter_map(|k| k.first()).collect::<Vec<_>>(),
        })
    );
}

/// The rule `decrease_drawn`: rarely, once the sealing jump activated the
/// increase, and at most once per timeline, one decrease cycle under the
/// gate's write guard, its straggler reads under a read guard, then the
/// forged token read with no guard, all while faults may run. The eventually
/// keeps its own cycles for a timeline where this never read a straggler
/// back.
pub fn decrease_drawn(s: &Shared, tc: &TestCase) {
    let due = drawn_decrease_due(
        s.eventually_on,
        lock(&s.jump).is_some_and(|j| j.reached_driver),
        *lock(&s.reshard.increase),
        now_ns(),
        s.reshard.drawn_started.load(Ordering::SeqCst),
    );
    if !due {
        return;
    }
    if !s.reshard.force_drawn && !tc.draw(gs::weighted_booleans(DECREASE_DRAW)) {
        return;
    }
    let quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
    if s.stop.stopping() {
        release_and_halt(quiet);
    }
    if !claim_once(&s.reshard.drawn_started) {
        return;
    }
    println!(
        "reshard: drawn decrease: starting (forced by {FORCE_DRAWN_DECREASE}: {})",
        s.reshard.force_drawn
    );
    let stragglers = decrease_cycle(s, "drawn", 0);
    drop(quiet);
    let read = {
        let _g = s.enter();
        read_stragglers(s, &stragglers)
    };
    if read > 0 {
        s.reshard.drawn_reached.store(true, Ordering::SeqCst);
    }
    println!(
        "reshard: drawn decrease: {} straggler(s) found, {read} read back",
        stragglers.len()
    );
    forged_read(s, "drawn");
}

/// The eventually's reshard situations, after the sealing jump. The caller
/// holds the gate's write guard.
pub fn eventually(s: &Shared) {
    if !ensure_increase(s) {
        println!("reshard: the increase is not active; skipping the reshard situations");
        return;
    }
    activation_writes(s);
    fence(s);
    if s.reshard.drawn_reached.load(Ordering::SeqCst) {
        println!("reshard: the drawn decrease read a straggler back; no decrease cycle here");
    } else {
        for cycle in 0..DECREASE_CYCLES {
            let stragglers = decrease_cycle(s, "eventually", cycle);
            read_stragglers(s, &stragglers);
            if !stragglers.is_empty() {
                break;
            }
        }
    }
    forged_read(s, "eventually");
}

fn active_now(s: &Shared) -> u32 {
    let hour = now_ns() / NS_PER_HOUR;
    audit::active_count(&s.history(hour).generations, hour)
}

/// The increase is active now, appending it and moving the clocks to its
/// activation first if it is not.
fn ensure_increase(s: &Shared) -> bool {
    if active_now(s) == INCREASED {
        return true;
    }
    let pending = *lock(&s.reshard.increase);
    let hour = match pending {
        Some(hour) if u64::from(hour) * NS_PER_HOUR > now_ns() => hour,
        _ => match provision(s, INCREASED) {
            Ok(hour) => {
                *lock(&s.reshard.increase) = Some(hour);
                hour
            }
            Err(e) => {
                println!("reshard: the increase: {e}");
                return false;
            }
        },
    };
    let target_s = u64::from(hour) * 3600 + 60;
    s.nudge("reshard: increase activation", &|now_s| {
        target_s.saturating_sub(now_s)
    });
    active_now(s) == INCREASED
}

/// One write of `records` records over as many streams to `node`; returns
/// the write when it was acknowledged.
fn write_to(s: &Shared, records: u32, node: usize) -> Option<Write> {
    let (id, acked) = s.write_with_id(records, records, &mut || node);
    let id = id?;
    let w = lock(&s.model).writes.get(&id).cloned();
    if acked { w } else { None }
}

/// The last attempt of write `id`, for the details.
fn last_attempt(s: &Shared, id: Option<u64>) -> String {
    id.and_then(|id| {
        lock(&s.model)
            .writes
            .get(&id)
            .and_then(|w| w.attempts.last().cloned())
    })
    .map_or_else(|| "none".to_string(), |a| format!("{a:?}"))
}

/// Writes after the activation until both shards the increase added
/// acknowledged one, so a folded post-activation hour holds data at each.
fn activation_writes(s: &Shared) {
    let n = s.ravel.nodes.len().max(1);
    let want: BTreeSet<u32> = (seed::SHARDS..INCREASED).collect();
    for i in 0..ACTIVATION_WRITES {
        if lock(&s.reshard.added_shards).is_superset(&want) {
            break;
        }
        write_to(s, WIDE_WRITE, (i as usize) % n);
    }
    println!(
        "reshard: shards the increase added that acknowledged a write: {:?}",
        lock(&s.reshard.added_shards)
    );
}

/// Move every clock forward like `Shared::nudge`, without waiting for every
/// node to serve after: while the record's reads are refused no node
/// answers SQL.
fn move_unsettled(s: &Shared, reason: &str, by_s: &dyn Fn(u64) -> u64) -> bool {
    let Ok(jump) = s.shift_clock(reason, by_s) else {
        return false;
    };
    if jump.by_s > 0 {
        println!(
            "clock: moved forward by {} s outside the planned jumps ({reason})",
            jump.by_s
        );
    }
    std::thread::sleep(Duration::from_secs(clock::reach_wait_s(s.clock.reread_s)));
    jump.reached_driver
}

/// The fence situation (D2): an acknowledged write per node, so each router
/// holds a view; then the proxy refuses every GET and HEAD of the
/// provisioning record; after the refresh interval a write per node routes
/// on its grace-extended view; every clock moves 60 s past the start of the
/// hour the stale-view wait ends in plus the two-hour lead, past every view's
/// horizon; a write per node is fenced;
/// the refusal is lifted and every node must serve again.
fn fence(s: &Shared) -> bool {
    let n = s.ravel.nodes.len();
    let before: Vec<bool> = (0..n)
        .map(|node| (0..3).any(|_| write_to(s, 4, node).is_some()))
        .collect();
    if !before.iter().all(|&a| a) {
        println!("reshard: fence: not every node acknowledged a write first: {before:?}");
        return false;
    }
    let key = s.prov_key.clone();
    let refused = s
        .proxy_cmd("pass")
        .and_then(|_| s.proxy_cmd(&format!("refuse-read {key}")))
        .is_ok();
    let refused_ns = now_ns();
    let mut grace = Vec::new();
    let mut fenced = Vec::new();
    let mut moved = false;
    let mut horizon_hour = refused_ns / NS_PER_HOUR + audit::MIN_LEAD_HOURS;
    if refused {
        std::thread::sleep(STALE_VIEW_WAIT);
        // A read already in flight when the rule was set may still have
        // refreshed a view during the wait; no read can after it, since the
        // grace writes' own re-reads are refused.
        horizon_hour = now_ns() / NS_PER_HOUR + audit::MIN_LEAD_HOURS;
        for node in 0..n {
            let (id, acked) = s.write_with_id(4, 4, &mut || node);
            grace.push(json!({ "node": node, "acked": acked, "last": last_attempt(s, id) }));
        }
        let target_s = horizon_hour * 3600 + 60;
        moved = move_unsettled(s, "reshard: fence", &|now_s| target_s.saturating_sub(now_s));
        for node in 0..n {
            let (id, acked) = s.write_with_id(4, 4, &mut || node);
            fenced.push(json!({ "node": node, "acked": acked, "last": last_attempt(s, id) }));
        }
    }
    let thawed = s
        .proxy_cmd(&format!("thaw-read {key}"))
        .and_then(|_| s.proxy_cmd("pass"))
        .is_ok();
    let recovered = s.wait_serving(Duration::from_secs(300), "reshard: after thaw-read");
    assert_always!(
        recovered.is_ok(),
        "catalog: the system recovers once faults stop",
        &json!({ "after": "thaw-read", "last_error": recovered.as_ref().err() })
    );
    let past = now_ns() / NS_PER_HOUR >= horizon_hour;
    let reached = refused && moved && past;
    let details = json!({
        "prov_key": key,
        "refused": refused,
        "refused_ns": refused_ns,
        "horizon_hour": horizon_hour,
        "moved": moved,
        "past_horizon": past,
        "grace_writes": grace,
        "fenced_writes": fenced,
        "thawed": thawed,
        "recovered": recovered.is_ok(),
    });
    println!("reshard: fence: {details}");
    assert_sometimes!(
        reached,
        "reshard: the clocks passed the writers' lead horizon while the provisioning record's reads were refused",
        &details
    );
    reached
}

/// One decrease cycle: with 6 shards active (after an increase back to 6 if
/// an earlier cycle left 4), append the decrease to 4, move every clock to
/// just before its activation, and send a write every 50 ms in its last
/// two seconds. A write routed at 6 that flushes in the activation hour is a
/// straggler: a token at shard 4 or 5 in an hour that routes at 4. Returns
/// the stragglers, for [`read_stragglers`]. `reason` is `drawn` or
/// `eventually`, for the details. The caller holds the gate's write guard.
fn decrease_cycle(s: &Shared, reason: &str, cycle: u32) -> Vec<Write> {
    if active_now(s) != INCREASED {
        match provision(s, INCREASED) {
            Ok(hour) => {
                let target_s = u64::from(hour) * 3600 + 60;
                s.nudge("reshard: increase before a decrease", &|now_s| {
                    target_s.saturating_sub(now_s)
                });
            }
            Err(e) => {
                println!("reshard: decrease cycle {cycle} ({reason}): the increase: {e}");
                return Vec::new();
            }
        }
        if active_now(s) != INCREASED {
            println!("reshard: decrease cycle {cycle} ({reason}): 6 shards are not active");
            return Vec::new();
        }
    }
    let a = match provision(s, seed::SHARDS) {
        Ok(hour) => hour,
        Err(e) => {
            println!("reshard: decrease cycle {cycle} ({reason}): {e}");
            return Vec::new();
        }
    };
    *lock(&s.reshard.decrease) = Some(a);
    let target_s = (u64::from(a) * 3600).saturating_sub(BURST_LEAD_S);
    if !s.nudge("reshard: decrease burst", &|now_s| {
        target_s.saturating_sub(now_s)
    }) {
        return Vec::new();
    }
    let Some(delays) = burst_delays(now_ns(), a, BURST_WINDOW, BURST_SPACING) else {
        println!("reshard: decrease cycle {cycle} ({reason}): past the burst's start already");
        return Vec::new();
    };
    let n = s.ravel.nodes.len().max(1);
    let started = std::time::Instant::now();
    let ids: Vec<(Option<u64>, bool)> = std::thread::scope(|scope| {
        let handles: Vec<_> = delays
            .iter()
            .enumerate()
            .map(|(k, &delay)| {
                scope.spawn(move || {
                    std::thread::sleep(delay.saturating_sub(started.elapsed()));
                    s.write_with_id(WIDE_WRITE, WIDE_WRITE, &mut || k % n)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or((None, false)))
            .collect()
    });
    let burst: Vec<Write> = {
        let m = lock(&s.model);
        ids.iter()
            .filter(|(_, acked)| *acked)
            .filter_map(|(id, _)| id.and_then(|id| m.writes.get(&id).cloned()))
            .collect()
    };
    let acked = burst.len();
    let stragglers: Vec<Write> = burst
        .into_iter()
        .filter(|w| {
            w.acked_header()
                .and_then(|h| token::decode_header(h).ok())
                .is_some_and(|ts| {
                    ts.iter()
                        .any(|(_, t)| t.hour == a && t.shard >= seed::SHARDS)
                })
        })
        .collect();
    let details = json!({
        "reason": reason,
        "cycle": cycle,
        "activation_hour": a,
        "planned": delays.len(),
        "sent": ids.iter().filter(|(id, _)| id.is_some()).count(),
        "acked": acked,
        "stragglers": stragglers.iter().map(|w| w.id).collect::<Vec<_>>(),
        "straggler_tokens": stragglers.iter().take(3).map(|w| w.acked_header()).collect::<Vec<_>>(),
    });
    println!("reshard: decrease burst: {details}");
    assert_sometimes!(
        !stragglers.is_empty(),
        "reshard: a write routed before a decrease landed at a retired shard in its activation hour",
        &details
    );
    // Wait until the activation hour has begun on every clock, then read.
    let boundary_ns = u64::from(a) * NS_PER_HOUR;
    while now_ns() < boundary_ns + NS_PER_SECOND {
        std::thread::sleep(Duration::from_millis(200));
    }
    // A write per node routed at 4 after the activation: a node restarted
    // later shows its new identity against this one at the same count
    // (`token::AckLedger::restarted_since`).
    let anchored: Vec<bool> = (0..n)
        .map(|node| (0..3).any(|_| write_to(s, 4, node).is_some()))
        .collect();
    if !anchored.iter().all(|&a| a) {
        println!(
            "reshard: decrease cycle {cycle} ({reason}): not every node acknowledged at 4: {anchored:?}"
        );
    }
    stragglers
}

/// Each straggler of a decrease cycle (up to [`STRAGGLER_READS`],
/// single-attempt writes only) read back with its tokens on every node.
/// Returns how many stragglers were read.
fn read_stragglers(s: &Shared, stragglers: &[Write]) -> usize {
    let mut read = 0;
    for w in stragglers
        .iter()
        .filter(|w| w.max_copies() == 1)
        .take(STRAGGLER_READS)
    {
        for node in 0..s.ravel.nodes.len() {
            s.lifecycle_read(w, node, false, "reshard: straggler");
        }
        read += 1;
    }
    read
}

/// The forged token read (D4 part 1): an acknowledged write's token moved to
/// a shard at or above its hour's active count and a sequence number no
/// writer reaches, so its record was never written. Every node must refuse
/// it as unsatisfiable, and the SUT evaluates `resharding: a commit token
/// whose record is durable resolves across a reshard` with the record
/// absent. `reason` is `drawn` or `eventually`, for the details.
fn forged_read(s: &Shared, reason: &str) {
    let picked: Option<(Write, Token, u32)> = {
        let history = lock(&s.history);
        let m = lock(&s.model);
        m.acked_logs
            .iter()
            .filter_map(|id| m.writes.get(id))
            .filter(|w| w.max_copies() == 1)
            .find_map(|w| {
                let (_, t) = token::decode_header(w.acked_header()?)
                    .ok()?
                    .into_iter()
                    .next()?;
                let active = audit::active_count(&history.generations, u64::from(t.hour));
                Some((w.clone(), t, active))
            })
    };
    let Some((w, t, active)) = picked else {
        println!("reshard: no acknowledged write to forge a token from");
        return;
    };
    let forged = forge(&t, active);
    let qualified = format!("logs:{}", forged.encode());
    let start = w.ts_ns.saturating_sub(WINDOW_SLACK_NS) as f64 / 1e9;
    let end = w.ts_ns.saturating_add(WINDOW_SLACK_NS) as f64 / 1e9;
    for node in 0..s.ravel.nodes.len() {
        let mut answer = String::new();
        let mut refused = false;
        for attempt in 0..3 {
            if attempt > 0 {
                std::thread::sleep(Duration::from_secs(2));
            }
            match s.ravel.sql(
                node,
                &writes_query(&[w.id]),
                start,
                end,
                std::slice::from_ref(&qualified),
                READ_TIMEOUT,
            ) {
                Ok(value) => {
                    let rows = model::log_counts(ravel::rows(&value)).map_or(0, |c| c.len());
                    answer = format!("answered {rows} rows");
                    break;
                }
                Err(e) => {
                    let class = token::failure_class(&e);
                    answer = format!("{class}: {e}");
                    if class == "unsatisfiable" {
                        refused = true;
                        break;
                    }
                    if !matches!(e, SqlFailure::Status(503, _) | SqlFailure::NoResponse(_)) {
                        break;
                    }
                }
            }
        }
        let details = json!({
            "reason": reason,
            "node": s.ravel.nodes[node],
            "write": w.id,
            "from_token": format!("{t:?}"),
            "forged_shard": forged.shard,
            "active_count": active,
            "answer": answer,
        });
        println!("reshard: forged token read: {details}");
        assert_sometimes!(
            refused,
            "reshard: a forged token for a record never written was refused as unsatisfiable",
            &details
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reshard_line_parses() {
        let out = "l: resharded shard_count 4 -> 6, appended generation 1 activating at unix \
                   hour 498721 (in 2h)\n";
        assert_eq!(parse_reshard(out), Some((4, 6, 498_721)));
        assert_eq!(parse_reshard("nothing"), None);
        assert_eq!(
            parse_reshard("l: resharded shard_count 6 -> 4, appended generation 2"),
            None
        );
    }

    #[test]
    fn the_burst_fills_the_last_two_seconds_at_50_ms() {
        let a = 498_724_u32;
        let boundary = u64::from(a) * NS_PER_HOUR;
        let now = boundary - 30 * NS_PER_SECOND;
        let d = burst_delays(now, a, BURST_WINDOW, BURST_SPACING).expect("in time");
        assert_eq!(d.len(), 40);
        assert_eq!(d[0], Duration::from_secs(28));
        assert_eq!(d[1] - d[0], Duration::from_millis(50));
        // The last write leaves 50 ms before the activation, none at or after.
        assert_eq!(
            Duration::from_nanos(boundary - now) - d[39],
            Duration::from_millis(50)
        );
        // Sent at the burst's start exactly: the first delay is zero.
        let d = burst_delays(boundary - 2 * NS_PER_SECOND, a, BURST_WINDOW, BURST_SPACING)
            .expect("at the start");
        assert_eq!(d[0], Duration::ZERO);
        // Past the burst's start: refused rather than sent short.
        assert_eq!(
            burst_delays(boundary - NS_PER_SECOND, a, BURST_WINDOW, BURST_SPACING),
            None
        );
    }

    #[test]
    fn the_drawn_decrease_waits_for_the_activated_increase_and_runs_once() {
        let h = 498_724_u32;
        let at = u64::from(h) * NS_PER_HOUR;
        assert!(drawn_decrease_due(true, true, Some(h), at, false));
        let later = at + NS_PER_HOUR;
        assert!(drawn_decrease_due(true, true, Some(h), later, false));
        // Each precondition missing on its own.
        assert!(!drawn_decrease_due(false, true, Some(h), at, false));
        assert!(!drawn_decrease_due(true, false, Some(h), at, false));
        assert!(!drawn_decrease_due(true, true, None, at, false));
        assert!(!drawn_decrease_due(true, true, Some(h), at - 1, false));
        assert!(!drawn_decrease_due(true, true, Some(h), at, true));
    }

    #[test]
    fn the_once_flag_is_claimed_by_the_first_caller_only() {
        let flag = AtomicBool::new(false);
        assert!(claim_once(&flag));
        assert!(!claim_once(&flag));
        assert!(!claim_once(&flag));
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn a_forged_token_keeps_the_identity_and_hour_at_an_unwritten_seq() {
        let t = Token {
            shard: 2,
            writer: "0b6e7c1a-1d2e-4f50-9a6b-7c8d9e0f1a2b".into(),
            epoch: 1_794_931_200,
            seq: 7,
            hour: 498_592,
        };
        let f = forge(&t, 4);
        assert_eq!(
            (f.shard, f.seq, f.hour, f.epoch),
            (4, u64::MAX, t.hour, t.epoch)
        );
        assert_eq!(f.writer, t.writer);
        assert_eq!(token::decode(&f.encode()), Ok(f));
    }
}
