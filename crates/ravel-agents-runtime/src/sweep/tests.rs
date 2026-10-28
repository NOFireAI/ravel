#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use ravel_agents_core::head::TimerStatus;
use ravel_agents_core::keys::{dir_key, dir_prefix, wake_prefix};
use ravel_agents_core::wire::TimerRequest;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_types::TenantHash;
use serde_json::json;

use crate::tests::{T0, create, head, id_of, poll_now, send, worker};
use crate::timers::tests::{
    SEC, TENANT, hint_keys, rig, rig_over, schedule, status, tick_inputs, timer,
};
use crate::{SweepConfig, SweepMetricsSnapshot, SweepReport};

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
            list_pages: 2,
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
        list_pages: 2,
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
            list_pages: 2,
            head_gets: 2,
            hints_read: 1,
            hints_deleted: 1,
            timers_fired: 1,
            agents_loaded: 2,
            dir_entries_repaired: 0,
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
            list_pages: 2,
            head_gets: 2,
            ..SweepReport::default()
        }
    );
    assert_eq!(
        cold.sweep_metrics(),
        SweepMetricsSnapshot {
            passes: 2,
            list_pages: 4,
            head_gets: 4,
            hints_read: 1,
            hints_deleted: 1,
            timers_fired: 1,
            agents_loaded: 2,
            dir_entries_repaired: 0,
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
            list_pages: 3,
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
            list_pages: 3,
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
