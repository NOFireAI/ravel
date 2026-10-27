use super::*;
use crate::command::{EventSpec, HandlerPin};
use crate::identity::Hash256;
use crate::keys::{AgentKey, AgentKind};
use proptest::prelude::*;
use ravel_types::TenantHash;

const SECOND: i64 = 1_000_000_000;

fn blob(tag: &[u8]) -> ObjectRef {
    ObjectRef::of(tag)
}

fn create_cmd() -> Command {
    Command::Create(Create {
        tenant: TenantHash([7; 16]),
        kind: AgentKind::new("order").expect("kind"),
        key: AgentKey::new("o-1").expect("key"),
    })
}

fn pin() -> HandlerPin {
    HandlerPin {
        handler: "h".to_string(),
        version: "1".to_string(),
        digest: Hash256([1; 32]),
    }
}

fn inc_fence(head: &Head) -> IncarnationFence {
    IncarnationFence {
        incarnation: head.incarnation,
    }
}

fn owner_fence(head: &Head) -> OwnerFence {
    OwnerFence {
        incarnation: head.incarnation,
        owner_id: head.owner_id.clone(),
        epoch: head.epoch,
    }
}

fn turn_fence(head: &Head) -> TurnFence {
    let turn = head.active_turn.as_ref().expect("an active turn");
    TurnFence {
        owner: owner_fence(head),
        turn_id: turn.turn_id,
        base_state_version: turn.base_state_version,
        handler_version: turn.handler_version.clone(),
        handler_digest: turn.handler_digest,
    }
}

fn enqueue_cmd(head: &Head, request_id: &str, payload: &[u8]) -> Command {
    Command::Enqueue(Enqueue {
        fence: inc_fence(head),
        request_id: request_id.to_string(),
        handler: "h".to_string(),
        request_hash: identity::request_hash("h", payload),
        payload: Some(blob(payload)),
    })
}

fn takeover_cmd(head: &Head, owner: &str) -> Command {
    Command::Takeover(Takeover {
        incarnation: head.incarnation,
        owner_id: owner.to_string(),
        observed_epoch: head.epoch,
        lease_hint_unix_ns: 0,
    })
}

fn begin_cmd(head: &Head) -> Command {
    Command::BeginTurn(BeginTurn {
        fence: owner_fence(head),
        handlers: vec![pin()],
    })
}

fn intent(key: &str, policy: RecoveryPolicy) -> IntentSpec {
    IntentSpec {
        intent_key: key.to_string(),
        tool_name: "mail".to_string(),
        tool_version: "1".to_string(),
        input_hash: identity::step_input_hash("mail", "1", key.as_bytes()),
        policy,
        input: Some(blob(key.as_bytes())),
    }
}

fn complete_cmd(head: &Head, state: &[u8]) -> CompleteTurn {
    CompleteTurn {
        fence: turn_fence(head),
        observed_root_revision: head.root_revision,
        new_state: blob(state),
        output: Some(blob(b"out")),
        events: Vec::new(),
        timer_changes: Vec::new(),
        intents: Vec::new(),
    }
}

fn fail_cmd(head: &Head, terminal: bool, max_attempts: u32) -> Command {
    Command::FailTurn(FailTurn {
        fence: turn_fence(head),
        error: "boom".to_string(),
        terminal,
        max_attempts,
    })
}

struct Sim {
    kernel: Kernel,
    head: Option<Head>,
    now: i64,
}

impl Sim {
    fn new() -> Sim {
        Sim::with_limits(Limits::default())
    }

    fn with_limits(limits: Limits) -> Sim {
        Sim {
            kernel: Kernel::new(limits),
            head: None,
            now: 1_000 * SECOND,
        }
    }

    fn head(&self) -> &Head {
        self.head.as_ref().expect("a head")
    }

    fn step(&mut self, command: &Command) -> Result<Transition, AgentError> {
        let transition = self
            .kernel
            .apply(self.head.as_ref(), command, self.now, &DerivedIds)?;
        if let Transition::Publish { head, .. } = &transition {
            self.head = Some(head.clone());
        }
        Ok(transition)
    }

    fn run(&mut self, command: &Command) -> Reply {
        self.step(command).expect("command applies").reply().clone()
    }

    fn refuse(&mut self, command: &Command) -> ErrorKind {
        let before = self.head.clone();
        let kind = self.step(command).expect_err("command is refused").kind;
        assert_eq!(self.head, before, "a refusal published");
        kind
    }

    /// Create, take ownership as `w1`, enqueue one input and begin its turn.
    fn started() -> Sim {
        let mut sim = Sim::new();
        sim.run(&create_cmd());
        sim.run(&takeover_cmd(sim.head(), "w1"));
        sim.run(&enqueue_cmd(sim.head(), "r1", b"p1"));
        sim.run(&begin_cmd(sim.head()));
        sim
    }
}

#[test]
fn turn_commit_publishes_state_input_result_events_and_intents_together() {
    let sim = Sim::started();
    let before = sim.head().clone();
    let turn = before.active_turn.clone().expect("turn");
    assert!(before.inbox.iter().any(|e| e.accept_seq == 1));
    assert!(before.results.is_empty());

    let mut complete = complete_cmd(&before, b"state-1");
    complete.events = vec![
        EventSpec {
            event_type: "shipped".to_string(),
            payload: Some(blob(b"e1")),
        },
        EventSpec {
            event_type: "billed".to_string(),
            payload: None,
        },
    ];
    complete.intents = vec![intent("send-receipt", RecoveryPolicy::ProviderIdempotency)];
    let transition = sim
        .kernel
        .apply(
            Some(&before),
            &Command::CompleteTurn(complete),
            sim.now,
            &DerivedIds,
        )
        .expect("complete applies");
    let Transition::Publish {
        head: after,
        effects,
        reply,
    } = transition
    else {
        panic!("complete_turn must publish one root");
    };

    // One root carries every change.
    assert_eq!(after.root_revision, before.root_revision + 1);
    // 1. the new state
    assert_eq!(after.state_version, before.state_version + 1);
    assert_eq!(after.state, Some(blob(b"state-1")));
    // 2. the consumed input
    assert!(after.inbox.iter().all(|e| e.accept_seq != 1));
    assert_eq!(after.active_turn, None);
    // 3. its terminal result
    assert_eq!(
        after.results,
        vec![ResultEntry {
            accept_seq: 1,
            status: ResultStatus::Completed,
            output: Some(blob(b"out")),
            error: String::new(),
            turn_id: Some(turn.turn_id),
            dead_lettered: false,
            finished_unix_ns: sim.now,
        }]
    );
    // 4. the buffered events, numbered in order
    let events: Vec<(u64, u64, &str)> = after
        .events
        .iter()
        .map(|e| (e.event_seq, e.accept_seq, e.event_type.as_str()))
        .collect();
    assert_eq!(events, vec![(1, 1, "shipped"), (2, 1, "billed")]);
    assert_eq!(after.next_event_seq, 3);
    // 5. the effect intents, journaled and in the outbox
    let effect_id = identity::effect_id(1, turn.turn_id, "send-receipt");
    assert_eq!(after.effects.len(), 1);
    assert_eq!(after.effects[0].effect_id, effect_id);
    assert_eq!(after.effects[0].status, EffectStatus::Intent);
    assert_eq!(after.effects[0].turn_id, turn.turn_id);
    assert_eq!(after.outbox.len(), 1);
    assert_eq!(after.outbox[0].effect_id, effect_id);

    assert_eq!(after.quota, after.computed_quota());
    assert_eq!(
        effects,
        vec![
            Effect::OperationTerminal { accept_seq: 1 },
            Effect::DispatchIntents {
                effect_ids: vec![effect_id]
            },
        ]
    );
    assert_eq!(
        reply,
        Reply::TurnCommitted {
            accept_seq: 1,
            state_version: 1,
            rebased: false
        }
    );
    assert_eq!(Head::decode(&after.encode()), Ok(after));
}

#[test]
fn a_commit_rebases_onto_a_root_that_moved_without_touching_the_turn() {
    let mut sim = Sim::started();
    let stale = complete_cmd(sim.head(), b"s");
    sim.run(&enqueue_cmd(sim.head(), "r2", b"p2"));
    assert_ne!(stale.observed_root_revision, sim.head().root_revision);
    let reply = sim.run(&Command::CompleteTurn(stale));
    assert_eq!(
        reply,
        Reply::TurnCommitted {
            accept_seq: 1,
            state_version: 1,
            rebased: true
        }
    );
    assert_eq!(sim.head().inbox.len(), 1);
    assert_eq!(sim.head().inbox[0].accept_seq, 2);
}

#[test]
fn a_commit_after_takeover_is_ownership_lost() {
    let mut sim = Sim::started();
    let stale = complete_cmd(sim.head(), b"s");
    sim.run(&takeover_cmd(sim.head(), "w2"));
    assert_eq!(
        sim.refuse(&Command::CompleteTurn(stale)),
        ErrorKind::OwnershipLost
    );
}

#[test]
fn a_commit_under_another_handler_version_is_incompatible() {
    let mut sim = Sim::started();
    let mut cmd = complete_cmd(sim.head(), b"s");
    cmd.fence.handler_version = "2".to_string();
    assert_eq!(
        sim.refuse(&Command::CompleteTurn(cmd)),
        ErrorKind::IncompatibleHandler
    );
}

#[test]
fn retries_are_idempotent_and_conflicts_are_typed() {
    let mut sim = Sim::new();
    sim.run(&create_cmd());
    assert_eq!(sim.run(&create_cmd()), Reply::Exists { incarnation: 1 });
    let first = sim.run(&enqueue_cmd(sim.head(), "r1", b"p"));
    assert_eq!(
        first,
        Reply::Accepted {
            accept_seq: 1,
            duplicate: false
        }
    );
    let again = sim.step(&enqueue_cmd(sim.head(), "r1", b"p")).expect("dup");
    assert_eq!(
        again,
        Transition::Unchanged {
            reply: Reply::Accepted {
                accept_seq: 1,
                duplicate: true
            }
        }
    );
    assert_eq!(
        sim.refuse(&enqueue_cmd(sim.head(), "r1", b"other")),
        ErrorKind::IdempotencyConflict
    );

    sim.run(&takeover_cmd(sim.head(), "w1"));
    sim.run(&begin_cmd(sim.head()));
    let complete = Command::CompleteTurn(complete_cmd(sim.head(), b"s"));
    sim.run(&complete);
    assert_eq!(
        sim.step(&complete).expect("retry"),
        Transition::Unchanged {
            reply: Reply::AlreadyCommitted { accept_seq: 1 }
        }
    );
}

#[test]
fn failed_turns_retry_under_a_new_turn_id_then_dead_letter() {
    let mut sim = Sim::started();
    let first_turn = sim.head().active_turn.clone().expect("turn").turn_id;
    assert_eq!(
        sim.run(&fail_cmd(sim.head(), false, 2)),
        Reply::TurnRetry {
            accept_seq: 1,
            failed_attempts: 1
        }
    );
    assert_eq!(sim.head().inbox[0].failed_attempts, 1);
    sim.run(&begin_cmd(sim.head()));
    let second_turn = sim.head().active_turn.clone().expect("turn").turn_id;
    assert_ne!(first_turn, second_turn);
    assert_eq!(
        sim.run(&fail_cmd(sim.head(), false, 2)),
        Reply::TurnFailed {
            accept_seq: 1,
            dead_lettered: true
        }
    );
    assert!(sim.head().inbox.is_empty());
    assert_eq!(sim.head().results[0].status, ResultStatus::Failed);
    assert_eq!(sim.head().results[0].error, "boom");
    assert_eq!(sim.head().state_version, 0);
}

#[test]
fn a_terminal_failure_is_not_dead_lettered() {
    let mut sim = Sim::started();
    assert_eq!(
        sim.run(&fail_cmd(sim.head(), true, 5)),
        Reply::TurnFailed {
            accept_seq: 1,
            dead_lettered: false
        }
    );
}

#[test]
fn cancel_settles_pending_input_and_flags_the_running_turn() {
    let mut sim = Sim::started();
    sim.run(&enqueue_cmd(sim.head(), "r2", b"p2"));
    let cancel = |head: &Head, accept_seq| {
        Command::Cancel(Cancel {
            fence: inc_fence(head),
            accept_seq,
        })
    };
    assert_eq!(
        sim.run(&cancel(sim.head(), 2)),
        Reply::Cancelled { accept_seq: 2 }
    );
    assert_eq!(
        sim.run(&cancel(sim.head(), 1)),
        Reply::CancelRequested { accept_seq: 1 }
    );
    assert_eq!(
        sim.step(&cancel(sim.head(), 1)).expect("again"),
        Transition::Unchanged {
            reply: Reply::CancelRequested { accept_seq: 1 }
        }
    );
    sim.run(&Command::CompleteTurn(complete_cmd(sim.head(), b"s")));
    let head = sim.head();
    assert_eq!(head.state_version, 0);
    assert_eq!(head.state, None);
    assert_eq!(head.results[1].status, ResultStatus::Cancelled);
    assert_eq!(
        sim.run(&cancel(sim.head(), 1)),
        Reply::AlreadyTerminal {
            accept_seq: 1,
            status: ResultStatus::Cancelled
        }
    );
    assert_eq!(sim.refuse(&cancel(sim.head(), 9)), ErrorKind::AgentGone);
}

#[test]
fn timers_fire_once_and_a_late_cancel_reports_already_fired() {
    let mut sim = Sim::new();
    sim.run(&create_cmd());
    let due = sim.now + 5 * SECOND;
    let schedule = |head: &Head, due_unix_ns| {
        Command::ScheduleTimer(ScheduleTimer {
            fence: inc_fence(head),
            timer: TimerSpec {
                timer_key: "remind".to_string(),
                due_unix_ns,
                handler: "h".to_string(),
                payload: None,
            },
        })
    };
    let timer_id = identity::timer_id(1, "remind");
    assert_eq!(
        sim.run(&schedule(sim.head(), due)),
        Reply::TimerScheduled {
            timer_id,
            generation: 1
        }
    );
    // A reschedule supersedes the first generation.
    sim.run(&schedule(sim.head(), due));
    let fire = |head: &Head, generation| {
        Command::FireTimer(FireTimer {
            fence: inc_fence(head),
            timer_id,
            generation,
        })
    };
    assert_eq!(sim.run(&fire(sim.head(), 2)), Reply::TimerNotDue);
    sim.now = due;
    assert_eq!(sim.run(&fire(sim.head(), 1)), Reply::TimerStale);
    assert_eq!(
        sim.run(&fire(sim.head(), 2)),
        Reply::TimerFired { accept_seq: 1 }
    );
    assert_eq!(
        sim.head().inbox[0].source,
        InputSource::Timer {
            timer_id,
            generation: 2
        }
    );
    assert_eq!(sim.run(&fire(sim.head(), 2)), Reply::TimerStale);
    let cancel = Command::CancelTimer(CancelTimer {
        fence: inc_fence(sim.head()),
        timer_key: "remind".to_string(),
    });
    assert_eq!(sim.run(&cancel), Reply::AlreadyFired);
    assert_eq!(sim.head().quota.timer_count, 0);
}

#[test]
fn a_manual_effect_goes_in_doubt_and_blocks_the_turn_until_reconciled() {
    let mut sim = Sim::started();
    let commit = Command::CommitIntent(CommitIntent {
        fence: turn_fence(sim.head()),
        intent: intent("charge", RecoveryPolicy::Manual),
    });
    let Reply::IntentCommitted { effect_id } = sim.run(&commit) else {
        panic!("intent not committed");
    };
    assert_eq!(
        sim.step(&commit).expect("retry"),
        Transition::Unchanged {
            reply: Reply::IntentCommitted { effect_id }
        }
    );
    let authorize = |authority, attempt| {
        Command::AuthorizeAttempt(AuthorizeAttempt {
            authority,
            effect_id,
            attempt,
        })
    };
    let owner = Authority::Owner(owner_fence(sim.head()));
    assert_eq!(
        sim.run(&authorize(owner.clone(), 1)),
        Reply::AttemptAuthorized {
            effect_id,
            attempt: 1
        }
    );
    // A retry of the same authorization is idempotent.
    assert_eq!(
        sim.run(&authorize(owner.clone(), 1)),
        Reply::AttemptAuthorized {
            effect_id,
            attempt: 1
        }
    );
    let Reply::Blocked(error) = sim.run(&authorize(owner.clone(), 2)) else {
        panic!("a second manual attempt must block");
    };
    assert_eq!(error.kind, ErrorKind::EffectInDoubt);
    assert_eq!(sim.head().effects[0].status, EffectStatus::InDoubt);
    assert_eq!(sim.refuse(&authorize(owner, 2)), ErrorKind::EffectInDoubt);
    assert_eq!(
        sim.refuse(&Command::CompleteTurn(complete_cmd(sim.head(), b"s"))),
        ErrorKind::EffectInDoubt
    );

    let reconcile = Authority::Reconcile(inc_fence(sim.head()));
    assert_eq!(
        sim.run(&authorize(reconcile, 2)),
        Reply::AttemptAuthorized {
            effect_id,
            attempt: 2
        }
    );
    let result = |head: &Head, attempt, tag: &[u8]| {
        Command::RecordEffectResult(RecordEffectResult {
            fence: inc_fence(head),
            effect_id,
            attempt,
            result: blob(tag),
        })
    };
    assert_eq!(
        sim.refuse(&result(sim.head(), 3, b"r")),
        ErrorKind::OwnershipLost
    );
    assert_eq!(
        sim.run(&result(sim.head(), 2, b"r")),
        Reply::EffectResultRecorded { effect_id }
    );
    assert_eq!(
        sim.step(&result(sim.head(), 2, b"r")).expect("retry"),
        Transition::Unchanged {
            reply: Reply::EffectResultRecorded { effect_id }
        }
    );
    assert_eq!(
        sim.refuse(&result(sim.head(), 2, b"other")),
        ErrorKind::IdempotencyConflict
    );
    assert_eq!(
        sim.run(&result(sim.head(), 1, b"late")),
        Reply::LateResultIgnored { effect_id }
    );
    sim.run(&Command::CompleteTurn(complete_cmd(sim.head(), b"s")));
    assert_eq!(sim.head().state_version, 1);
}

#[test]
fn an_at_least_once_effect_may_be_retried_by_its_owner() {
    let mut sim = Sim::started();
    let Reply::IntentCommitted { effect_id } = sim.run(&Command::CommitIntent(CommitIntent {
        fence: turn_fence(sim.head()),
        intent: intent("ping", RecoveryPolicy::AtLeastOnce),
    })) else {
        panic!("intent not committed");
    };
    for attempt in 1..=3 {
        let reply = sim.run(&Command::AuthorizeAttempt(AuthorizeAttempt {
            authority: Authority::Owner(owner_fence(sim.head())),
            effect_id,
            attempt,
        }));
        assert_eq!(reply, Reply::AttemptAuthorized { effect_id, attempt });
    }
    assert_eq!(
        sim.refuse(&Command::AuthorizeAttempt(AuthorizeAttempt {
            authority: Authority::Owner(owner_fence(sim.head())),
            effect_id,
            attempt: 5,
        })),
        ErrorKind::OwnershipLost
    );
}

#[test]
fn a_step_replayed_with_another_input_blocks_the_turn() {
    let mut sim = Sim::started();
    let step = |head: &Head, input: &[u8], status| {
        Command::RecordStep(RecordStep {
            fence: turn_fence(head),
            step_key: "lookup".to_string(),
            tool_name: "db".to_string(),
            tool_version: "1".to_string(),
            input_hash: identity::step_input_hash("db", "1", input),
            status,
            output: None,
        })
    };
    assert!(matches!(
        sim.run(&step(sim.head(), b"a", StepStatus::Started)),
        Reply::StepRecorded(_)
    ));
    assert!(matches!(
        sim.run(&step(sim.head(), b"a", StepStatus::Completed)),
        Reply::StepRecorded(_)
    ));
    assert!(matches!(
        sim.run(&step(sim.head(), b"a", StepStatus::Started)),
        Reply::StepReplayed(r) if r.status == StepStatus::Completed
    ));
    let Reply::Blocked(error) = sim.run(&step(sim.head(), b"b", StepStatus::Started)) else {
        panic!("a mismatched replay must block");
    };
    assert_eq!(error.kind, ErrorKind::NonDeterministicReplay);
    assert_eq!(
        sim.head().active_turn.as_ref().map(|t| t.block),
        Some(TurnBlock::NonDeterministicReplay)
    );
    assert_eq!(
        sim.refuse(&Command::CompleteTurn(complete_cmd(sim.head(), b"s"))),
        ErrorKind::NonDeterministicReplay
    );
    // The blocked turn can still fail, which clears the block.
    sim.run(&fail_cmd(sim.head(), true, 1));
    assert_eq!(sim.head().active_turn, None);
}

#[test]
fn a_tombstoned_agent_is_gone_until_recreated_under_a_new_incarnation() {
    let mut sim = Sim::started();
    let old = sim.head().clone();
    sim.run(&Command::Tombstone(Tombstone {
        fence: inc_fence(&old),
    }));
    assert_eq!(
        sim.run(&Command::Tombstone(Tombstone {
            fence: inc_fence(&old),
        })),
        Reply::AlreadyTombstoned
    );
    assert_eq!(
        sim.refuse(&enqueue_cmd(&old, "r9", b"p")),
        ErrorKind::AgentGone
    );
    assert_eq!(sim.run(&create_cmd()), Reply::Created { incarnation: 2 });
    let head = sim.head();
    assert_eq!(head.epoch, old.epoch);
    assert_eq!(head.owner_id, "");
    assert_eq!(head.next_accept_seq, 1);
    assert!(head.inbox.is_empty() && head.results.is_empty() && head.receipts.is_empty());
    assert_eq!(
        sim.refuse(&enqueue_cmd(&old, "r9", b"p")),
        ErrorKind::AgentGone
    );
    assert_eq!(
        sim.refuse(&Command::CompleteTurn(complete_cmd(&old, b"s"))),
        ErrorKind::AgentGone
    );
}

#[test]
fn logical_caps_and_the_byte_cap_are_typed() {
    let mut sim = Sim::with_limits(Limits {
        max_mailbox_depth: 1,
        ..Limits::default()
    });
    sim.run(&create_cmd());
    sim.run(&enqueue_cmd(sim.head(), "r1", b"p"));
    assert_eq!(
        sim.refuse(&enqueue_cmd(sim.head(), "r2", b"p")),
        ErrorKind::MailboxFull
    );

    let mut sim = Sim::with_limits(Limits {
        max_head_bytes: 64,
        ..Limits::default()
    });
    sim.run(&create_cmd());
    assert_eq!(
        sim.refuse(&enqueue_cmd(sim.head(), "a-long-request-id", b"p")),
        ErrorKind::BudgetExceeded
    );
}

#[test]
fn a_missing_agent_only_accepts_create() {
    let mut sim = Sim::new();
    let head = crate::head::tests::sample_head();
    assert_eq!(
        sim.refuse(&enqueue_cmd(&head, "r", b"p")),
        ErrorKind::AgentGone
    );
    assert_eq!(sim.run(&create_cmd()), Reply::Created { incarnation: 1 });
    assert_eq!(sim.head().root_revision, 1);
}

/// One abstract step of the property test. The harness turns it into a
/// command against the current root, fenced the way a well-behaved caller
/// would fence it, except for the `Stale*` steps.
#[derive(Debug, Clone)]
enum Op {
    Enqueue {
        request: u8,
        payload: u8,
    },
    Takeover {
        owner: u8,
    },
    Begin,
    Complete {
        events: u8,
        intent: Option<u8>,
        timer: Option<u8>,
        stale_view: bool,
    },
    Fail {
        terminal: bool,
    },
    Cancel {
        accept_seq: u8,
    },
    Schedule {
        key: u8,
        due_s: u8,
    },
    Fire {
        key: u8,
    },
    CancelTimer {
        key: u8,
    },
    Step {
        key: u8,
        input: u8,
    },
    Intent {
        key: u8,
        manual: bool,
    },
    Authorize {
        pick: u8,
        reconcile: bool,
        retry: bool,
    },
    EffectResult {
        pick: u8,
        tag: u8,
    },
    Tombstone,
    Recreate,
    Advance {
        s: u8,
    },
    StaleEpoch {
        which: u8,
    },
    StaleIncarnation {
        which: u8,
    },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0u8..6, 0u8..3).prop_map(|(request, payload)| Op::Enqueue { request, payload }),
        1 => (0u8..3).prop_map(|owner| Op::Takeover { owner }),
        4 => Just(Op::Begin),
        4 => (0u8..3, proptest::option::of(0u8..3), proptest::option::of(0u8..3), any::<bool>())
            .prop_map(|(events, intent, timer, stale_view)| Op::Complete { events, intent, timer, stale_view }),
        2 => any::<bool>().prop_map(|terminal| Op::Fail { terminal }),
        1 => (0u8..8).prop_map(|accept_seq| Op::Cancel { accept_seq }),
        1 => (0u8..3, 0u8..10).prop_map(|(key, due_s)| Op::Schedule { key, due_s }),
        1 => (0u8..3).prop_map(|key| Op::Fire { key }),
        1 => (0u8..3).prop_map(|key| Op::CancelTimer { key }),
        1 => (0u8..3, 0u8..2).prop_map(|(key, input)| Op::Step { key, input }),
        1 => (0u8..3, any::<bool>()).prop_map(|(key, manual)| Op::Intent { key, manual }),
        1 => (any::<u8>(), any::<bool>(), any::<bool>())
            .prop_map(|(pick, reconcile, retry)| Op::Authorize { pick, reconcile, retry }),
        1 => (any::<u8>(), 0u8..2).prop_map(|(pick, tag)| Op::EffectResult { pick, tag }),
        1 => Just(Op::Tombstone),
        1 => Just(Op::Recreate),
        1 => (0u8..10).prop_map(|s| Op::Advance { s }),
        1 => any::<u8>().prop_map(|which| Op::StaleEpoch { which }),
        1 => any::<u8>().prop_map(|which| Op::StaleIncarnation { which }),
    ]
}

fn timer_key(key: u8) -> String {
    format!("t{key}")
}

fn effect_at(head: &Head, pick: u8) -> Option<&EffectRecord> {
    if head.effects.is_empty() {
        return None;
    }
    head.effects.get(pick as usize % head.effects.len())
}

/// A fence-carrying command against `head`, with `epoch` or `incarnation`
/// replaced by a value that is not the current one.
fn stale_command(head: &Head, which: u8, stale_epoch: bool) -> Command {
    let mut owner = owner_fence(head);
    let mut inc = inc_fence(head);
    if stale_epoch {
        owner.epoch = if head.epoch == 0 { 1 } else { head.epoch - 1 };
    } else {
        let wrong = if head.incarnation == 1 {
            2
        } else {
            head.incarnation - 1
        };
        owner.incarnation = wrong;
        inc.incarnation = wrong;
    }
    let turn = TurnFence {
        owner: owner.clone(),
        turn_id: head
            .active_turn
            .as_ref()
            .map_or(Id128([0; 16]), |t| t.turn_id),
        base_state_version: head.state_version,
        handler_version: "1".to_string(),
        handler_digest: Hash256([1; 32]),
    };
    match (which % 4, stale_epoch) {
        (0, _) => Command::BeginTurn(BeginTurn {
            fence: owner,
            handlers: vec![pin()],
        }),
        (1, _) => Command::CompleteTurn(CompleteTurn {
            fence: turn,
            observed_root_revision: head.root_revision,
            new_state: blob(b"stale"),
            output: None,
            events: Vec::new(),
            timer_changes: Vec::new(),
            intents: Vec::new(),
        }),
        (2, _) => Command::FailTurn(FailTurn {
            fence: turn,
            error: String::new(),
            terminal: true,
            max_attempts: 1,
        }),
        (_, true) => Command::AuthorizeAttempt(AuthorizeAttempt {
            authority: Authority::Owner(owner),
            effect_id: effect_at(head, which).map_or(Id128([0; 16]), |e| e.effect_id),
            attempt: effect_at(head, which).map_or(1, |e| e.attempt + 1),
        }),
        (_, false) => Command::Enqueue(Enqueue {
            fence: inc,
            request_id: "stale".to_string(),
            handler: "h".to_string(),
            request_hash: Hash256([0; 32]),
            payload: None,
        }),
    }
}

fn command_for(head: Option<&Head>, op: &Op, now: i64) -> Option<(Command, bool)> {
    let Some(head) = head else {
        return Some((create_cmd(), false));
    };
    let has_turn = head.active_turn.is_some();
    let command = match op {
        Op::Enqueue { request, payload } => enqueue_cmd(head, &format!("r{request}"), &[*payload]),
        Op::Takeover { owner } => takeover_cmd(head, &format!("w{owner}")),
        Op::Begin => begin_cmd(head),
        Op::Complete {
            events,
            intent: with_intent,
            timer,
            stale_view,
        } if has_turn => {
            let mut c = complete_cmd(head, &head.state_version.to_le_bytes());
            if *stale_view {
                c.observed_root_revision -= 1;
            }
            c.events = (0..*events)
                .map(|i| EventSpec {
                    event_type: format!("e{i}"),
                    payload: None,
                })
                .collect();
            c.intents = with_intent
                .iter()
                .map(|k| intent(&format!("i{k}"), RecoveryPolicy::AtLeastOnce))
                .collect();
            c.timer_changes = timer
                .iter()
                .map(|k| {
                    TimerChange::Schedule(TimerSpec {
                        timer_key: timer_key(*k),
                        due_unix_ns: now + SECOND,
                        handler: "h".to_string(),
                        payload: None,
                    })
                })
                .collect();
            Command::CompleteTurn(c)
        }
        Op::Fail { terminal } if has_turn => fail_cmd(head, *terminal, 2),
        Op::Cancel { accept_seq } => Command::Cancel(Cancel {
            fence: inc_fence(head),
            accept_seq: u64::from(*accept_seq),
        }),
        Op::Schedule { key, due_s } => Command::ScheduleTimer(ScheduleTimer {
            fence: inc_fence(head),
            timer: TimerSpec {
                timer_key: timer_key(*key),
                due_unix_ns: now + i64::from(*due_s) * SECOND,
                handler: "h".to_string(),
                payload: None,
            },
        }),
        Op::Fire { key } => {
            let timer_id = identity::timer_id(head.incarnation, &timer_key(*key));
            let generation = head
                .timers
                .iter()
                .find(|t| t.timer_id == timer_id)
                .map_or(0, |t| t.generation);
            Command::FireTimer(FireTimer {
                fence: inc_fence(head),
                timer_id,
                generation,
            })
        }
        Op::CancelTimer { key } => Command::CancelTimer(CancelTimer {
            fence: inc_fence(head),
            timer_key: timer_key(*key),
        }),
        Op::Step { key, input } if has_turn => Command::RecordStep(RecordStep {
            fence: turn_fence(head),
            step_key: format!("s{key}"),
            tool_name: "db".to_string(),
            tool_version: "1".to_string(),
            input_hash: identity::step_input_hash("db", "1", &[*input]),
            status: StepStatus::Completed,
            output: None,
        }),
        Op::Intent { key, manual } if has_turn => Command::CommitIntent(CommitIntent {
            fence: turn_fence(head),
            intent: intent(
                &format!("i{key}"),
                if *manual {
                    RecoveryPolicy::Manual
                } else {
                    RecoveryPolicy::AtLeastOnce
                },
            ),
        }),
        Op::Authorize {
            pick,
            reconcile,
            retry,
        } => {
            let effect = effect_at(head, *pick)?;
            Command::AuthorizeAttempt(AuthorizeAttempt {
                authority: if *reconcile {
                    Authority::Reconcile(inc_fence(head))
                } else {
                    Authority::Owner(owner_fence(head))
                },
                effect_id: effect.effect_id,
                attempt: if *retry {
                    effect.attempt
                } else {
                    effect.attempt + 1
                },
            })
        }
        Op::EffectResult { pick, tag } => {
            let effect = effect_at(head, *pick)?;
            Command::RecordEffectResult(RecordEffectResult {
                fence: inc_fence(head),
                effect_id: effect.effect_id,
                attempt: effect.attempt,
                result: blob(&[*tag]),
            })
        }
        Op::Tombstone => Command::Tombstone(Tombstone {
            fence: inc_fence(head),
        }),
        Op::Recreate => create_cmd(),
        Op::StaleEpoch { which } => return Some((stale_command(head, *which, true), true)),
        Op::StaleIncarnation { which } => {
            return Some((stale_command(head, *which, false), true));
        }
        Op::Complete { .. }
        | Op::Fail { .. }
        | Op::Step { .. }
        | Op::Intent { .. }
        | Op::Advance { .. } => return None,
    };
    Some((command, false))
}

fn check_root(head: &Head) -> Result<(), TestCaseError> {
    let mut seqs: Vec<u64> = head
        .inbox
        .iter()
        .map(|e| e.accept_seq)
        .chain(head.results.iter().map(|r| r.accept_seq))
        .collect();
    seqs.sort_unstable();
    let dense: Vec<u64> = (1..head.next_accept_seq).collect();
    prop_assert_eq!(seqs, dense, "accept_seq is not dense");
    prop_assert_eq!(head.quota, head.computed_quota());
    if let Some(turn) = &head.active_turn {
        prop_assert!(
            head.inbox
                .iter()
                .any(|e| e.accept_seq == turn.input_accept_seq)
        );
        prop_assert_eq!(turn.base_state_version, head.state_version);
    }
    prop_assert_eq!(Head::decode(&head.encode()), Ok(head.clone()));
    Ok(())
}

/// How much of the kernel one property run reached.
#[derive(Debug, Default)]
struct Coverage {
    publishes: u64,
    commits: u64,
    rebased_commits: u64,
    dead_letters: u64,
    timer_fires: u64,
    effect_results: u64,
    blocks: u64,
    recreates: u64,
    stale_refusals: u64,
}

fn run_ops(ops: &[Op], coverage: &mut Coverage) -> Result<(), TestCaseError> {
    let mut sim = Sim::with_limits(Limits {
        max_mailbox_depth: 4,
        max_receipts: 6,
        max_timers: 2,
        max_head_bytes: MAX_HEAD_BYTES,
    });
    for op in ops {
        if let Op::Advance { s } = op {
            sim.now += i64::from(*s) * SECOND;
            continue;
        }
        let Some((command, stale)) = command_for(sim.head.as_ref(), op, sim.now) else {
            continue;
        };
        let before = sim.head.clone();
        let outcome = sim
            .kernel
            .apply(before.as_ref(), &command, sim.now, &DerivedIds);
        if stale {
            let Err(error) = outcome else {
                return Err(TestCaseError::fail(format!(
                    "stale fence accepted: {command:?}"
                )));
            };
            let expected = match &before {
                Some(h)
                    if h.lifecycle == Lifecycle::Active && matches!(op, Op::StaleEpoch { .. }) =>
                {
                    ErrorKind::OwnershipLost
                }
                _ => ErrorKind::AgentGone,
            };
            prop_assert_eq!(error.kind, expected, "{:?}", command);
            coverage.stale_refusals += 1;
            continue;
        }
        let Ok(Transition::Publish {
            head: after, reply, ..
        }) = outcome
        else {
            continue;
        };
        coverage.publishes += 1;
        match &reply {
            Reply::TurnCommitted { rebased, .. } => {
                coverage.commits += 1;
                coverage.rebased_commits += u64::from(*rebased);
            }
            Reply::TurnFailed {
                dead_lettered: true,
                ..
            } => coverage.dead_letters += 1,
            Reply::TimerFired { .. } => coverage.timer_fires += 1,
            Reply::EffectResultRecorded { .. } => coverage.effect_results += 1,
            Reply::Blocked(_) => coverage.blocks += 1,
            Reply::Created { incarnation } if *incarnation > 1 => coverage.recreates += 1,
            _ => {}
        }
        let revision = before.as_ref().map_or(0, |h| h.root_revision);
        prop_assert_eq!(after.root_revision, revision + 1);
        if let Some(before) = &before {
            if before.incarnation == after.incarnation {
                if !matches!(command, Command::CompleteTurn(_)) {
                    prop_assert_eq!(after.state_version, before.state_version);
                    prop_assert_eq!(after.state, before.state);
                }
                prop_assert!(after.state_version >= before.state_version);
                prop_assert!(after.state_version <= before.state_version + 1);
                // Results are append-only, so an input gets at most one
                // terminal result and it never changes.
                prop_assert!(after.results.starts_with(&before.results));
                prop_assert!(after.next_accept_seq >= before.next_accept_seq);
                prop_assert!(after.epoch >= before.epoch);
            } else {
                prop_assert_eq!(after.incarnation, before.incarnation + 1);
                prop_assert_eq!(after.state_version, 0);
                prop_assert_eq!(after.epoch, before.epoch);
            }
        }
        check_root(&after)?;
        sim.head = Some(after);
    }
    Ok(())
}

#[test]
fn kernel_sequences_keep_the_head_invariants() {
    let config = ProptestConfig {
        cases: 10_000,
        // What `proptest!` sets, so a caught case lands in proptest-regressions/.
        source_file: Some(file!()),
        ..ProptestConfig::default()
    };
    let mut runner = proptest::test_runner::TestRunner::new(config);
    let coverage = std::cell::RefCell::new(Coverage::default());
    let result = runner.run(&proptest::collection::vec(op(), 1..40), |ops| {
        run_ops(&ops, &mut coverage.borrow_mut())
    });
    if let Err(failure) = result {
        panic!("{failure}");
    }
    // Each path the invariants are meant to cover must actually be reached.
    let c = coverage.into_inner();
    let floors = [
        ("publishes", c.publishes, 20_000),
        ("commits", c.commits, 1_000),
        ("rebased_commits", c.rebased_commits, 500),
        ("dead_letters", c.dead_letters, 15),
        ("timer_fires", c.timer_fires, 40),
        ("effect_results", c.effect_results, 20),
        ("blocks", c.blocks, 12),
        ("recreates", c.recreates, 500),
        ("stale_refusals", c.stale_refusals, 4_000),
    ];
    eprintln!("{c:?}");
    for (name, seen, floor) in floors {
        assert!(seen >= floor, "{name}: {seen} below {floor}; {c:?}");
    }
}
