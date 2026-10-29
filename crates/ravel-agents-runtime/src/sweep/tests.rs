#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use ravel_agents_core::error::{AgentError, BudgetCap};
use ravel_agents_core::head::TimerStatus;
use ravel_agents_core::kernel::Limits;
use ravel_agents_core::keys::{dir_key, dir_prefix, wake_prefix};
use ravel_agents_core::wire::{SendRequest, TimerRequest};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_types::TenantHash;
use serde_json::json;

use crate::tests::{T0, create, head, id_of, poll_now, send, worker};
use crate::timers::tests::{
    SEC, TENANT, hint_keys, rig, rig_over, rig_with, schedule, status, tick_inputs, timer,
};
use crate::{FillSnapshot, SweepConfig, SweepMetricsSnapshot, SweepReport};

fn lost_hints() -> Arc<FaultStore<MemoryStore>> {
    Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("hint lost".to_string()))
                .with_key_contains("/_wake/")
                .with_occurrence(Occurrence::Always),
        ),
    ))
}

#[tokio::test(start_paused = true)]
async fn duplicate_hints_for_one_generation_fire_it_once() {
    let r = rig();
    create(&r.co, "o-1").await;
    // A reschedule into a later bucket leaves the first hint behind, so two
    // hints stand for generation 2.
    schedule(&r.co, "o-1", "t", T0 + 10 * SEC).await;
    let g2 = schedule(&r.co, "o-1", "t", T0 + 70 * SEC).await;
    assert_eq!(g2.generation, 2);
    assert_eq!(hint_keys(r.backend.as_ref()).await.len(), 2);

    r.now.advance(80 * SEC);
    let before = r.store.counters();
    let deletes = r.store.wake_hint_deletes();
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!(r.store.counters().since(&before).publish.cas, 1);
    assert_eq!(r.store.wake_hint_deletes() - deletes, 2);
    assert_eq!(
        (
            pass.hints_read,
            pass.hints_deleted,
            pass.timers_fired,
            pass.head_gets
        ),
        (2, 2, 1, 1)
    );
    let h = head(&r.store, "o-1").await;
    assert_eq!(status(&h, "t"), TimerStatus::Delivered);
    assert_eq!(tick_inputs(&h), 1);
    assert!(hint_keys(r.backend.as_ref()).await.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_premature_hint_fires_nothing_and_is_kept() {
    let r = rig();
    create(&r.co, "o-1").await;
    // Due 50 s into the bucket the clock is in, so the hint is listed early.
    schedule(&r.co, "o-1", "t", T0 + 50 * SEC).await;
    r.now.advance(10 * SEC);

    let before = r.store.counters();
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!(r.store.counters().since(&before).publish.cas, 0);
    assert_eq!(
        (pass.hints_read, pass.hints_deleted, pass.timers_fired),
        (1, 0, 0)
    );
    assert_eq!(hint_keys(r.backend.as_ref()).await.len(), 1);
    assert_eq!(
        status(&head(&r.store, "o-1").await, "t"),
        TimerStatus::Scheduled
    );

    r.now.advance(40 * SEC);
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!((pass.timers_fired, pass.hints_deleted), (1, 1));
}

#[tokio::test(start_paused = true)]
async fn a_lost_hint_is_found_by_the_directory_half() {
    let fault = lost_hints();
    let r = rig_over(fault.clone(), SweepConfig::default());
    create(&r.co, "o-1").await;
    schedule(&r.co, "o-1", "t", T0 + 10 * SEC).await;
    assert_eq!(fault.fault_count(Op::Put, FaultKind::Permanent), 1);
    assert!(hint_keys(r.backend.as_ref()).await.is_empty());

    r.now.advance(10 * SEC);
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!(
        pass,
        SweepReport {
            wake_list_pages: 1,
            dir_list_pages: 1,
            head_gets: 1,
            hints_read: 0,
            hints_deleted: 0,
            timers_fired: 1,
            agents_loaded: 0,
            dir_entries_repaired: 0,
        }
    );
    assert_eq!(tick_inputs(&head(&r.store, "o-1").await), 1);
}

#[tokio::test(start_paused = true)]
async fn a_bounded_pass_walks_the_directory_in_list_order_across_passes() {
    let fault = Arc::new(FaultStore::new(
        MemoryStore::with_page_size(3),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("hint lost".to_string()))
                .with_key_contains("/_wake/")
                .with_occurrence(Occurrence::Always),
        ),
    ));
    let r = rig_over(
        fault,
        SweepConfig {
            max_head_gets: 4,
            ..SweepConfig::default()
        },
    );
    let keys: Vec<String> = (0..10).map(|i| format!("o-{i}")).collect();
    for key in &keys {
        create(&r.co, key).await;
        schedule(&r.co, key, "t", T0 + SEC).await;
    }
    r.now.advance(SEC);
    // LIST order is agent id order.
    let mut order = keys.clone();
    order.sort_by_key(|k| id_of(k).to_hex());

    let mut delivered = BTreeSet::new();
    let mut fired_per_pass = Vec::new();
    let mut gets_per_pass = Vec::new();
    for _ in 0..3 {
        let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
        let mut now_delivered = BTreeSet::new();
        for key in &keys {
            if status(&head(&r.store, key).await, "t") == TimerStatus::Delivered {
                now_delivered.insert(key.clone());
            }
        }
        let fired: BTreeSet<String> = now_delivered.difference(&delivered).cloned().collect();
        assert_eq!(pass.timers_fired, fired.len() as u64);
        fired_per_pass.push(fired);
        gets_per_pass.push(pass.head_gets);
        delivered = now_delivered;
    }
    let expect = |range: std::ops::Range<usize>| -> BTreeSet<String> {
        order[range].iter().cloned().collect()
    };
    assert_eq!(
        fired_per_pass,
        vec![expect(0..4), expect(4..8), expect(8..10)]
    );
    assert_eq!(gets_per_pass, vec![4, 4, 2]);

    // Every agent was read once; the next pass starts over.
    let fourth = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!((fourth.head_gets, fourth.timers_fired), (4, 0));
}

#[tokio::test(start_paused = true)]
async fn the_wake_half_reads_its_share_of_the_get_cap_and_resumes_across_passes() {
    // The agents are created over the plain store, their `_dir` entries are
    // then removed, and the sweeping coordinator's store refuses every
    // `_dir` PUT, so the repair cannot restore them and only the wake half
    // reaches an agent.
    let shared = Arc::new(MemoryStore::new());
    let setup = rig_over(shared.clone(), SweepConfig::default());
    let fault = Arc::new(FaultStore::new(
        shared.clone(),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("dir lost".to_string()))
                .with_key_contains("/_dir/")
                .with_occurrence(Occurrence::Always),
        ),
    ));
    let r = rig_over(
        fault.clone(),
        SweepConfig {
            max_head_gets: 4,
            ..SweepConfig::default()
        },
    );
    // Each agent holds a timer due now and one due later in the same bucket,
    // so one hint per agent stands for both and is kept after the first
    // fires: a pass that restarted the wake listing would re-read the same
    // hints and never reach the later ones.
    let keys: Vec<String> = (0..5).map(|i| format!("o-{i}")).collect();
    for key in &keys {
        create(&setup.co, key).await;
        schedule(&setup.co, key, "a", T0 + SEC).await;
        schedule(&setup.co, key, "b", T0 + 50 * SEC).await;
        shared
            .delete(&dir_key(&TENANT, &id_of(key)).expect("dir key"))
            .await
            .expect("delete dir entry");
    }
    assert_eq!(hint_keys(r.backend.as_ref()).await.len(), 5);
    r.now.advance(SEC);
    // Wake keys share one bucket, so LIST order is agent id order.
    let mut order = keys.clone();
    order.sort_by_key(|k| id_of(k).to_hex());

    let mut delivered = BTreeSet::new();
    let mut fired_per_pass = Vec::new();
    let mut reports = Vec::new();
    for _ in 0..3 {
        let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
        let mut now_delivered = BTreeSet::new();
        for key in &keys {
            if status(&head(&r.store, key).await, "a") == TimerStatus::Delivered {
                now_delivered.insert(key.clone());
            }
        }
        fired_per_pass.push(
            now_delivered
                .difference(&delivered)
                .cloned()
                .collect::<BTreeSet<String>>(),
        );
        reports.push((
            pass.head_gets,
            pass.hints_read,
            pass.timers_fired,
            pass.hints_deleted,
        ));
        delivered = now_delivered;
    }
    let expect = |range: std::ops::Range<usize>| -> BTreeSet<String> {
        order[range].iter().cloned().collect()
    };
    assert_eq!(
        fired_per_pass,
        vec![expect(0..2), expect(2..4), expect(4..5)]
    );
    // Half of a cap of four per pass, and the directory half reads nothing.
    assert_eq!(reports, vec![(2, 2, 2, 0), (2, 2, 2, 0), (1, 1, 1, 0)]);
    assert_eq!(hint_keys(r.backend.as_ref()).await.len(), 5);
    // Each agent the wake half read got one refused repair PUT.
    assert_eq!(fault.fault_count(Op::Put, FaultKind::Permanent), 5);
    assert_eq!(r.co.sweep_metrics().dir_entries_repaired, 0);
}

#[tokio::test(start_paused = true)]
async fn a_hint_reached_agent_gets_its_missing_dir_entry_back() {
    let r = rig();
    create(&r.co, "o-1").await;
    let dir = dir_key(&TENANT, &id_of("o-1")).expect("dir key");

    // Entry present: the directory half lists it, so no PUT.
    schedule(&r.co, "o-1", "a", T0 + SEC).await;
    r.now.advance(SEC);
    let before = r.store.counters();
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    let spent = r.store.counters().since(&before);
    assert_eq!((pass.timers_fired, pass.dir_entries_repaired), (1, 0));
    assert_eq!(spent.publish.put, 0);
    let fire_gets = spent.resolve.get;

    // Entry missing: one conditional PUT writes it back.
    r.backend.delete(&dir).await.expect("delete dir entry");
    schedule(&r.co, "o-1", "b", r.now.get() + SEC).await;
    r.now.advance(SEC);
    let before = r.store.counters();
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    let spent = r.store.counters().since(&before);
    assert_eq!((pass.timers_fired, pass.dir_entries_repaired), (1, 1));
    assert_eq!(spent.publish.put, 1);
    assert_eq!(spent.resolve.get, fire_gets, "the repair reads nothing");
    r.backend.head(&dir).await.expect("dir entry restored");
    assert_eq!(r.co.sweep_metrics().dir_entries_repaired, 1);

    // Restored: the next pass lists it and writes nothing.
    let before = r.store.counters();
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!(pass.dir_entries_repaired, 0);
    assert_eq!(r.store.counters().since(&before).publish.put, 0);
}

#[tokio::test(start_paused = true)]
async fn a_poll_with_a_ready_turn_runs_no_sweep_pass() {
    let r = rig();
    create(&r.co, "o-1").await;
    let w = worker(&r.co, "v1");
    // The first poll lists the directory and runs the first pass.
    assert!(poll_now(&r.co, &w, "v1").await.is_none());
    assert_eq!(r.co.sweep_metrics().passes, 1);

    send(&r.co, "o-1", "r-1", json!(1)).await;
    r.now.advance(5 * SEC);
    let before = r.store.counters();
    let assigned = poll_now(&r.co, &w, "v1").await;
    assert!(assigned.is_some(), "the ready turn is assigned");
    assert_eq!(r.co.sweep_metrics().passes, 1, "no pass on this poll");
    assert_eq!(r.store.counters().since(&before).resolve.list, 0);
}

#[tokio::test(start_paused = true)]
async fn an_idle_poll_sweeps_on_the_interval_and_takes_what_the_pass_fired() {
    let r = rig();
    create(&r.co, "o-1").await;
    let w = worker(&r.co, "v1");
    assert!(poll_now(&r.co, &w, "v1").await.is_none());
    assert_eq!(r.co.sweep_metrics().passes, 1);
    // A timer whose input the worker handles, due one interval from now.
    r.co.schedule_timer(&TimerRequest {
        handler: "step".to_string(),
        ..timer("o-1", "t", T0 + 5 * SEC)
    })
    .await
    .expect("schedule");

    r.now.advance(5 * SEC - 1);
    assert!(poll_now(&r.co, &w, "v1").await.is_none());
    assert_eq!(r.co.sweep_metrics().passes, 1);

    r.now.advance(1);
    let assigned = poll_now(&r.co, &w, "v1").await;
    assert_eq!(r.co.sweep_metrics().passes, 2);
    assert_eq!(r.co.sweep_metrics().timers_fired, 1);
    // The same non-waiting poll scans again after the pass and takes the
    // input the fire enqueued.
    assert!(assigned.is_some(), "the fired timer's turn is assigned");
}

#[tokio::test(start_paused = true)]
async fn sweep_counters_move_by_exactly_what_a_pass_did() {
    let r = rig();
    create(&r.co, "o-1").await;
    create(&r.co, "o-2").await;
    schedule(&r.co, "o-1", "t", T0 + SEC).await;
    send(&r.co, "o-2", "r-1", json!(1)).await;
    r.now.advance(SEC);

    // A coordinator with nothing loaded, as after a restart.
    let cold = r.cold(SweepConfig::default());
    let store_before = r.store.counters();
    let deletes = r.store.wake_hint_deletes();
    let pass = cold.sweep_once(&TENANT).await.expect("sweep");
    let expected = SweepReport {
        wake_list_pages: 1,
        dir_list_pages: 1,
        head_gets: 2,
        hints_read: 1,
        hints_deleted: 1,
        timers_fired: 1,
        agents_loaded: 2,
        dir_entries_repaired: 0,
    };
    assert_eq!(pass, expected);
    assert_eq!(
        cold.sweep_metrics(),
        SweepMetricsSnapshot {
            passes: 1,
            wake_list_pages: 1,
            dir_list_pages: 1,
            head_gets: 2,
            hints_read: 1,
            hints_deleted: 1,
            timers_fired: 1,
            agents_loaded: 2,
            dir_entries_repaired: 0,
            directory_cycles: 1,
            last_directory_cycle_ms: 0,
        }
    );
    let spent = r.store.counters().since(&store_before);
    assert_eq!((spent.resolve.list, spent.publish.cas), (2, 1));
    assert_eq!(r.store.wake_hint_deletes() - deletes, 1);
    assert_eq!(cold.metrics().agents_loaded, 2);

    // A second pass reads both through the directory and changes nothing.
    let pass = cold.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!(
        pass,
        SweepReport {
            wake_list_pages: 1,
            dir_list_pages: 1,
            head_gets: 2,
            ..SweepReport::default()
        }
    );
    assert_eq!(
        cold.sweep_metrics(),
        SweepMetricsSnapshot {
            passes: 2,
            wake_list_pages: 2,
            dir_list_pages: 2,
            head_gets: 4,
            hints_read: 1,
            hints_deleted: 1,
            timers_fired: 1,
            agents_loaded: 2,
            dir_entries_repaired: 0,
            directory_cycles: 2,
            last_directory_cycle_ms: 0,
        }
    );
    assert_eq!(r.co.sweep_metrics(), SweepMetricsSnapshot::default());
}

#[tokio::test(start_paused = true)]
async fn polls_start_at_most_one_pass_per_interval() {
    let r = rig();
    create(&r.co, "o-1").await;
    schedule(&r.co, "o-1", "t", T0 + SEC).await;
    r.now.advance(SEC);
    let w = worker(&r.co, "v1");

    poll_now(&r.co, &w, "v1").await;
    assert_eq!(r.co.sweep_metrics().passes, 1);
    assert_eq!(r.co.sweep_metrics().timers_fired, 1);
    poll_now(&r.co, &w, "v1").await;
    assert_eq!(r.co.sweep_metrics().passes, 1);

    r.now.advance(5 * SEC - 1);
    poll_now(&r.co, &w, "v1").await;
    assert_eq!(r.co.sweep_metrics().passes, 1);
    r.now.advance(1);
    poll_now(&r.co, &w, "v1").await;
    assert_eq!(r.co.sweep_metrics().passes, 2);
}

/// Two keys under `prefix` that sort before every real entry and that no
/// operation can address, written as a foreign writer would.
fn plant_unaddressable(backend: &MemoryStore, prefix: &str) {
    for name in ["0//a", "0//b"] {
        backend.insert_foreign(&format!("{prefix}{name}"), bytes::Bytes::new());
    }
}

#[tokio::test(start_paused = true)]
async fn a_wake_page_of_unaddressable_keys_moves_the_cursor_past_it() {
    let backend = Arc::new(MemoryStore::with_page_size(2));
    let r = rig_over(backend.clone(), SweepConfig::default());
    create(&r.co, "o-1").await;
    schedule(&r.co, "o-1", "t", T0 + SEC).await;
    // The listing fills its first page with these two keys and reports
    // more, so the page parses into no hint.
    plant_unaddressable(&backend, &wake_prefix(&TENANT).expect("prefix"));
    r.now.advance(SEC);

    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    // Page one: the two planted keys. Page two: the real hint, and the
    // listing ends there. Then one directory page, whose entry the wake
    // half already read.
    assert_eq!(
        pass,
        SweepReport {
            wake_list_pages: 2,
            dir_list_pages: 1,
            head_gets: 1,
            hints_read: 1,
            hints_deleted: 1,
            timers_fired: 1,
            agents_loaded: 0,
            dir_entries_repaired: 0,
        }
    );
    assert_eq!(tick_inputs(&head(&r.store, "o-1").await), 1);
}

#[tokio::test(start_paused = true)]
async fn a_dir_page_of_unaddressable_keys_moves_the_cursor_past_it() {
    let fault = Arc::new(FaultStore::new(
        MemoryStore::with_page_size(2),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("hint lost".to_string()))
                .with_key_contains("/_wake/")
                .with_occurrence(Occurrence::Always),
        ),
    ));
    let r = rig_over(fault.clone(), SweepConfig::default());
    create(&r.co, "o-1").await;
    schedule(&r.co, "o-1", "t", T0 + SEC).await;
    assert_eq!(fault.fault_count(Op::Put, FaultKind::Permanent), 1);
    plant_unaddressable(fault.inner(), &dir_prefix(&TENANT).expect("prefix"));
    r.now.advance(SEC);

    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    // One empty wake page, then the planted directory page, then the page
    // holding the agent's entry.
    assert_eq!(
        pass,
        SweepReport {
            wake_list_pages: 1,
            dir_list_pages: 2,
            head_gets: 1,
            hints_read: 0,
            hints_deleted: 0,
            timers_fired: 1,
            agents_loaded: 0,
            dir_entries_repaired: 0,
        }
    );
    assert_eq!(tick_inputs(&head(&r.store, "o-1").await), 1);
}

#[tokio::test(start_paused = true)]
async fn a_sweep_of_another_tenant_is_refused() {
    let r = rig();
    let refused = r.co.sweep_once(&TenantHash([8; 16])).await;
    assert!(refused.is_err());
    assert_eq!(r.co.sweep_metrics().passes, 0);
}

#[tokio::test(start_paused = true)]
async fn a_hint_reached_agent_outside_the_dir_slice_is_repaired_once_across_passes() {
    // Two-key pages and two LISTs a pass: the wake half's one page holds the
    // one hint, and the directory half lists two entries a pass, so the
    // hinted agent, last of eight in LIST order, is not listed by the first
    // three passes while the wake half reaches it on each.
    let r = rig_over(
        Arc::new(MemoryStore::with_page_size(2)),
        SweepConfig {
            max_list_pages: 2,
            ..SweepConfig::default()
        },
    );
    let keys: Vec<String> = (0..8).map(|i| format!("o-{i}")).collect();
    for key in &keys {
        create(&r.co, key).await;
    }
    let mut order = keys.clone();
    order.sort_by_key(|k| id_of(k).to_hex());
    let hinted = order[7].clone();
    // A timer due now and one due later in the same bucket keep the one hint
    // standing after the first fires.
    schedule(&r.co, &hinted, "a", T0 + SEC).await;
    schedule(&r.co, &hinted, "b", T0 + 50 * SEC).await;
    r.now.advance(SEC);

    let mut puts = Vec::new();
    for _ in 0..3 {
        let before = r.store.counters();
        let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
        assert_eq!(pass.dir_list_pages, 1);
        assert_eq!(pass.hints_read, 1, "the wake half reaches the agent");
        assert_eq!(pass.dir_entries_repaired, 0, "the entry is there");
        puts.push(r.store.counters().since(&before).publish.put);
    }
    assert_eq!(puts, vec![1, 0, 0], "one AlreadyExists PUT, then none");
}

#[tokio::test(start_paused = true)]
async fn an_orphan_dir_entry_costs_one_read_and_is_skipped() {
    let backend = Arc::new(MemoryStore::new());
    let r = rig_over(backend.clone(), SweepConfig::default());
    // The entry a create leaves when its entry PUT lands and its HEAD write
    // does not.
    backend.insert_foreign(
        &dir_key(&TENANT, &id_of("o-1")).expect("dir key"),
        bytes::Bytes::new(),
    );
    for _ in 0..2 {
        let before = r.store.counters();
        let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
        assert_eq!(
            pass,
            SweepReport {
                wake_list_pages: 1,
                dir_list_pages: 1,
                head_gets: 1,
                ..SweepReport::default()
            }
        );
        let spent = r.store.counters().since(&before);
        assert_eq!((spent.publish.put, spent.publish.cas), (0, 0));
    }

    // The next create reuses it: its CreateIfAbsent answers AlreadyExists.
    create(&r.co, "o-1").await;
    let listed = r.store.list_agents(None).await.expect("list").agents;
    assert_eq!(listed, vec![id_of("o-1")]);
}

#[tokio::test(start_paused = true)]
async fn a_directory_cycle_is_counted_and_timed_when_the_cursor_wraps() {
    // Ten agents at four directory reads a pass: each cycle takes three
    // passes, reading 4, 4 and 2 HEADs, and wraps on the third.
    let r = rig_over(
        Arc::new(MemoryStore::with_page_size(3)),
        SweepConfig {
            max_head_gets: 4,
            ..SweepConfig::default()
        },
    );
    for i in 0..10 {
        create(&r.co, &format!("o-{i}")).await;
    }
    let mut gets = Vec::new();
    let mut cycles = Vec::new();
    let mut last_ms = Vec::new();
    // The clock moves after each pass: the first cycle spans 7 + 5 s, the
    // second, which starts one second after the first wrapped, 2 + 3 s.
    for advance in [7, 5, 1, 2, 3, 0] {
        let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
        let metrics = r.co.sweep_metrics();
        gets.push(pass.head_gets);
        cycles.push(metrics.directory_cycles);
        last_ms.push(metrics.last_directory_cycle_ms);
        r.now.advance(advance * SEC);
    }
    assert_eq!(gets, vec![4, 4, 2, 4, 4, 2]);
    assert_eq!(cycles, vec![0, 0, 1, 1, 1, 2]);
    assert_eq!(last_ms, vec![0, 0, 12_000, 12_000, 12_000, 5_000]);
}

#[tokio::test(start_paused = true)]
async fn a_vanished_dir_entry_is_restored_once_the_directory_cursor_wraps() {
    // The layout of
    // `a_hint_reached_agent_outside_the_dir_slice_is_repaired_once_across_passes`:
    // the directory half lists two entries a pass, the wake half reaches the
    // hinted agent, last in LIST order, on every pass.
    let r = rig_over(
        Arc::new(MemoryStore::with_page_size(2)),
        SweepConfig {
            max_list_pages: 2,
            ..SweepConfig::default()
        },
    );
    let keys: Vec<String> = (0..8).map(|i| format!("o-{i}")).collect();
    for key in &keys {
        create(&r.co, key).await;
    }
    let mut order = keys.clone();
    order.sort_by_key(|k| id_of(k).to_hex());
    let hinted = order[7].clone();
    schedule(&r.co, &hinted, "a", T0 + SEC).await;
    schedule(&r.co, &hinted, "b", T0 + 50 * SEC).await;
    r.now.advance(SEC);

    // The first pass remembers the entry through an AlreadyExists PUT, and
    // the entry is then removed by hand.
    let before = r.store.counters();
    r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!(r.store.counters().since(&before).publish.put, 1);
    let dir = dir_key(&TENANT, &id_of(&hinted)).expect("dir key");
    r.backend.delete(&dir).await.expect("delete dir entry");

    // Seven entries remain: the passes list two, two, then one and wrap.
    let mut puts = Vec::new();
    let mut repaired = Vec::new();
    let mut cycles = Vec::new();
    for _ in 0..3 {
        let before = r.store.counters();
        let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
        assert_eq!(pass.hints_read, 1, "the wake half reaches the agent");
        puts.push(r.store.counters().since(&before).publish.put);
        repaired.push(pass.dir_entries_repaired);
        cycles.push(r.co.sweep_metrics().directory_cycles);
    }
    assert_eq!(cycles, vec![0, 0, 1]);
    assert_eq!(puts, vec![0, 0, 1], "remembered until the wrap");
    assert_eq!(repaired, vec![0, 0, 1]);
    r.backend.head(&dir).await.expect("dir entry restored");
}

#[tokio::test(start_paused = true)]
async fn a_visit_reads_only_the_head_of_an_agent_with_sealed_pages() {
    let mut small = crate::tests::limits();
    small.kernel.max_page_entries = 2;
    let r = rig_with(Arc::new(MemoryStore::new()), SweepConfig::default(), small);
    create(&r.co, "o-1").await;
    for i in 1..=7 {
        send(&r.co, "o-1", &format!("r-{i}"), json!({"i": i})).await;
    }
    schedule(&r.co, "o-1", "t", T0 + 3600 * SEC).await;
    let h = head(&r.store, "o-1").await;
    assert!(h.index.inbox.pages.len() >= 2, "{:?}", h.index.inbox.pages);

    let before = r.store.counters();
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    let moved = r.store.counters().since(&before).total();
    assert_eq!((pass.head_gets, pass.timers_fired), (1, 0));
    assert_eq!(moved.get, 1, "one HEAD GET and no page GET");
}

/// Small caps, so every fill is an exact fraction: two entries a page,
/// eight pages a list, eight receipts.
fn fill_limits() -> ravel_agents_store::StoreLimits {
    let mut limits = crate::tests::limits();
    limits.kernel.max_page_entries = 2;
    limits.kernel.max_index_pages = 8;
    limits.kernel.max_receipts = 8;
    limits
}

/// Run one turn of `key` to completion, committing `events` events.
async fn run_turn(co: &crate::Coordinator, worker_id: &str, key: &str, events: usize) {
    use ravel_agents_core::wire::{EventSpecBody, TurnOutcome, WorkerCommitRequest};
    let assignment = poll_now(co, worker_id, "v1").await.expect("a turn");
    assert_eq!(assignment.lease.agent_id, id_of(key).to_hex());
    co.commit(&WorkerCommitRequest {
        lease: assignment.lease.clone(),
        outcome: TurnOutcome::Completed {
            state: json!({"n": 1}),
            output: serde_json::Value::Null,
            events: (0..events)
                .map(|i| EventSpecBody {
                    event_type: "e".to_string(),
                    payload: json!({"i": i}),
                })
                .collect(),
            timers: Vec::new(),
            intents: Vec::new(),
        },
    })
    .await
    .expect("commit");
}

/// Five agents in `_dir` LIST order: receipts at 7 of 8; 6 receipts with 6
/// results (2 sealed pages) and 12 events (5 sealed pages, 2 inline); 13
/// cancelled timers (6 sealed pages, 1 inline); one receipt; and, last,
/// nothing. The highest fill is never the last agent's, and the events and
/// timers figures differ from their inline entry counts.
#[tokio::test(start_paused = true)]
async fn a_directory_cycle_publishes_the_highest_fill_and_the_near_cap_counts() {
    let r = rig_with(
        Arc::new(MemoryStore::new()),
        SweepConfig::default(),
        fill_limits(),
    );
    let keys: Vec<String> = (0..5).map(|i| format!("f-{i}")).collect();
    for key in &keys {
        create(&r.co, key).await;
    }
    let mut order = keys.clone();
    order.sort_by_key(|k| id_of(k).to_hex());
    let w = worker(&r.co, "v1");
    for i in 0..6 {
        send(&r.co, &order[1], &format!("r-{i}"), json!({"i": i})).await;
        run_turn(&r.co, &w, &order[1], 2).await;
    }
    for i in 0..7 {
        send(&r.co, &order[0], &format!("r-{i}"), json!({"i": i})).await;
    }
    for i in 0..13 {
        let t = format!("t-{i}");
        schedule(&r.co, &order[2], &t, T0 + 3600 * SEC).await;
        crate::timers::tests::cancel(&r.co, &order[2], &t).await;
    }
    send(&r.co, &order[3], "r-0", json!({})).await;

    let busy = head(&r.store, &order[1]).await;
    assert_eq!(
        (
            busy.index.events.pages.len(),
            busy.index.results.pages.len()
        ),
        (5, 2)
    );
    let timed = head(&r.store, &order[2]).await;
    assert_eq!(timed.index.timers.pages.len(), 6);
    let mut sizes = Vec::new();
    for key in &order {
        sizes.push(head(&r.store, key).await.encode().len());
    }
    let largest = sizes.iter().copied().max().expect("five agents");
    assert_eq!(sizes.iter().position(|s| *s == largest), Some(1));
    assert_eq!(r.co.capacity_metrics().fill, None, "no cycle yet");

    let before = r.store.counters();
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    let moved = r.store.counters().since(&before).total();
    assert_eq!(pass.head_gets, 5);
    assert_eq!(
        (moved.get, moved.list),
        (5, 2),
        "one HEAD GET an agent, no page GET for the fill"
    );
    let fill = r.co.capacity_metrics().fill.expect("the cycle completed");
    assert_eq!(
        fill,
        FillSnapshot {
            max_ratio: [0.875, 0.25, 0.625, 0.75],
            head_bytes_max_ratio: largest as f64 / 262_144.0,
            near_cap: [2, 0, 0, 1],
            agents: 5,
        }
    );
}

/// The fill is published when the directory cursor wraps, not per pass: ten
/// agents at four reads a pass publish after the third pass, over all ten.
#[tokio::test(start_paused = true)]
async fn the_fill_is_published_only_when_a_directory_cycle_completes() {
    let r = rig_with(
        Arc::new(MemoryStore::with_page_size(3)),
        SweepConfig {
            max_head_gets: 4,
            ..SweepConfig::default()
        },
        fill_limits(),
    );
    for i in 0..10 {
        let key = format!("o-{i}");
        create(&r.co, &key).await;
        for j in 0..i.min(6) {
            send(&r.co, &key, &format!("r-{j}"), json!({})).await;
        }
    }
    let mut published = Vec::new();
    for _ in 0..3 {
        r.co.sweep_once(&TENANT).await.expect("sweep");
        published.push(r.co.capacity_metrics().fill.map(|f| f.agents));
    }
    assert_eq!(published, vec![None, None, Some(10)]);
    let fill = r.co.capacity_metrics().fill.expect("published");
    assert_eq!(fill.max_ratio[0], 0.75);
    // Agents o-6 to o-9 hold six receipts each.
    assert_eq!(fill.near_cap[0], 4);
}

/// Send to `key` until a send is refused, and return the refusal.
async fn send_until_refused(co: &crate::Coordinator, key: &str) -> AgentError {
    for i in 0..10_000 {
        let req = SendRequest {
            agent: crate::tests::agent_ref(key),
            handler: "step".to_string(),
            request_id: format!("r-{i}"),
            payload: json!({"i": i}),
        };
        if let Err(e) = co.send(&req).await {
            return e;
        }
    }
    panic!("no send to {key} was refused");
}

/// Every cap a test can reach, refused once through the coordinator: the
/// refusal names its cap, and `budget_refusals` moves by exactly one, under
/// that cap only.
#[tokio::test(start_paused = true)]
async fn each_cap_refusal_moves_its_counter_by_exactly_one() {
    type Shape = fn(&mut Limits);
    let cases: [(BudgetCap, Shape); 5] = [
        (BudgetCap::Mailbox, |l| l.max_mailbox_depth = 2),
        (BudgetCap::Receipts, |l| l.max_receipts = 3),
        (BudgetCap::Timers, |l| l.max_timers = 1),
        (BudgetCap::IndexPages, |l| {
            l.max_page_entries = 2;
            l.max_index_pages = 1;
        }),
        (BudgetCap::HeadBytes, |l| l.max_head_bytes = 1_024),
    ];
    for (cap, shape) in cases {
        let mut limits = crate::tests::limits();
        shape(&mut limits.kernel);
        let r = rig_with(Arc::new(MemoryStore::new()), SweepConfig::default(), limits);
        create(&r.co, "o-1").await;
        let error = if cap == BudgetCap::Timers {
            schedule(&r.co, "o-1", "a", T0 + SEC).await;
            r.co.schedule_timer(&timer("o-1", "b", T0 + SEC))
                .await
                .expect_err("a second timer is past the cap")
        } else {
            send_until_refused(&r.co, "o-1").await
        };
        assert_eq!(error.cap, Some(cap), "{error:?}");
        let mut expected = [0; 5];
        expected[BudgetCap::ALL.iter().position(|c| *c == cap).expect("cap")] = 1;
        assert_eq!(r.co.capacity_metrics().budget_refusals, expected, "{cap:?}");
    }
}

/// A refusal of a per-command bound names no cap and is not counted.
#[tokio::test(start_paused = true)]
async fn a_per_command_refusal_is_not_counted_as_a_cap() {
    let r = rig();
    create(&r.co, "o-1").await;
    let long = "x".repeat(ravel_agents_core::kernel::Limits::default().max_string_bytes + 1);
    let error =
        r.co.send(&SendRequest {
            agent: crate::tests::agent_ref("o-1"),
            handler: "step".to_string(),
            request_id: long,
            payload: json!({}),
        })
        .await
        .expect_err("the request id is past the string cap");
    assert_eq!(
        (error.kind, error.cap),
        (ravel_agents_core::error::ErrorKind::BudgetExceeded, None)
    );
    assert_eq!(r.co.capacity_metrics().budget_refusals, [0; 5]);
}
