use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;

use crate::orchestration::*;

struct Harness {
    orchestrator: Orchestrator,
    now: DateTime<Utc>,
    raw: AtomicUsize,
}

impl Harness {
    fn new() -> Self {
        Self::with_resolver(None)
    }

    fn with_resolver(resolver: Option<Arc<dyn ProviderResolver>>) -> Self {
        let now = Utc.with_ymd_and_hms(2026, 11, 4, 12, 0, 0).unwrap();
        let ids = Arc::new(AtomicUsize::new(0));
        let mut orchestrator = Orchestrator::new_in_memory()
            .with_clock(move || now)
            .with_id_generator(move |prefix, _parts| {
                let next = ids.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(format!("{prefix}:test-{next}"))
            });
        if let Some(res) = resolver {
            orchestrator = orchestrator.with_provider_resolver(res);
        }
        Self {
            orchestrator,
            now,
            raw: AtomicUsize::new(0),
        }
    }

    fn create_thread(&self, id: &str) -> DispatchResult {
        self.orchestrator
            .dispatch(Command {
                command_type: COMMAND_THREAD_CREATE.to_string(),
                command_id: format!("create-{id}"),
                thread_id: id.to_string(),
                payload: CommandPayload::CreateThread(CreateThread {
                    project_id: "project-1".to_string(),
                    title: format!("Thread {id}"),
                    model_selection: model("codex"),
                    runtime_mode: RUNTIME_FULL_ACCESS.to_string(),
                    interaction_mode: INTERACTION_DEFAULT.to_string(),
                    branch: None,
                    worktree_path: None,
                    created_by: ACTOR_USER.to_string(),
                    creation_source: CREATION_WEB.to_string(),
                }),
            })
            .expect("create thread")
    }

    fn send(&self, thread_id: &str, command_id: &str, message_id: &str, text: &str) -> DispatchResult {
        self.orchestrator
            .dispatch(Command {
                command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
                command_id: command_id.to_string(),
                thread_id: thread_id.to_string(),
                payload: CommandPayload::DispatchMessage(DispatchMessage {
                    message_id: message_id.to_string(),
                    text: text.to_string(),
                    attachments: Vec::new(),
                    title_seed: String::new(),
                    model_selection: None,
                    dispatch_mode: DispatchMode {
                        mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                        target_run_id: String::new(),
                    },
                    created_by: ACTOR_USER.to_string(),
                    creation_source: CREATION_WEB.to_string(),
                }),
            })
            .expect("send message")
    }

    fn next_raw(&self) -> Option<RawEventID> {
        let next = self.raw.fetch_add(1, Ordering::SeqCst) + 1;
        Some(format!("raw-{next}"))
    }

    fn start_provider_turn(&self, thread_id: &str) -> (Run, ProviderTurn) {
        let snapshot = self.orchestrator.snapshot(thread_id).expect("snapshot");
        let run = snapshot.projection.active_run().expect("active run");
        let mut provider_thread = snapshot
            .projection
            .provider_thread(run.provider_thread_id.as_deref().unwrap())
            .expect("provider thread");
        let session_id = format!("session-{thread_id}");
        let session = ProviderSession {
            id: session_id.clone(),
            driver: provider_thread.driver.clone(),
            provider_instance_id: provider_thread.provider_instance_id.clone(),
            status: SESSION_READY.to_string(),
            cwd: "/workspace".to_string(),
            model: None,
            capabilities: ProviderCapabilities::default(),
            created_at: self.now,
            updated_at: self.now,
            last_error: None,
        };
        self.orchestrator
            .ingest(IngestInput {
                thread_id: run.thread_id.clone(),
                provider_session_id: session_id.clone(),
                provider_instance_id: run.provider_instance_id.clone(),
                raw_event_id: self.next_raw(),
                event: ProviderEvent {
                    event_type: PROVIDER_EVENT_SESSION_UPDATED.to_string(),
                    driver: provider_thread.driver.clone(),
                    provider_session: Some(session),
                    ..Default::default()
                },
                ..Default::default()
            })
            .expect("attach provider session");

        provider_thread.provider_session_id = Some(session_id.clone());
        provider_thread.native_thread_ref = Some(ProviderRef {
            driver: provider_thread.driver.clone(),
            native_id: Some(format!("native-{thread_id}")),
            strength: NATIVE_REF_STRONG.to_string(),
            ..Default::default()
        });
        provider_thread.status = PROVIDER_THREAD_ACTIVE.to_string();
        self.orchestrator
            .ingest(IngestInput {
                thread_id: run.thread_id.clone(),
                provider_session_id: session_id.clone(),
                provider_instance_id: run.provider_instance_id.clone(),
                raw_event_id: self.next_raw(),
                event: ProviderEvent {
                    event_type: PROVIDER_EVENT_THREAD_UPDATED.to_string(),
                    driver: provider_thread.driver.clone(),
                    provider_thread: Some(provider_thread.clone()),
                    ..Default::default()
                },
                ..Default::default()
            })
            .expect("attach provider thread");

        let turn = ProviderTurn {
            id: derive_provider_turn_id(&provider_thread.driver, "turn-1"),
            provider_thread_id: provider_thread.id.clone(),
            node_id: run.root_node_id.clone().unwrap(),
            run_attempt_id: run.active_attempt_id.clone(),
            native_turn_ref: Some(ProviderRef {
                driver: provider_thread.driver.clone(),
                native_id: Some("turn-1".to_string()),
                strength: NATIVE_REF_STRONG.to_string(),
                ..Default::default()
            }),
            ordinal: 1,
            status: PROVIDER_TURN_RUNNING.to_string(),
            started_at: None,
            completed_at: None,
            token_usage: None,
        };
        self.orchestrator
            .ingest(IngestInput {
                thread_id: run.thread_id.clone(),
                run_id: Some(run.id.clone()),
                node_id: run.root_node_id.clone(),
                provider_session_id: session_id,
                provider_instance_id: run.provider_instance_id.clone(),
                raw_event_id: self.next_raw(),
                event: ProviderEvent {
                    event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                    driver: provider_thread.driver,
                    provider_turn: Some(turn.clone()),
                    ..Default::default()
                },
                ..Default::default()
            })
            .expect("start provider turn");

        (run, turn)
    }

    fn finish_provider_turn(
        &self,
        run: &Run,
        mut turn: ProviderTurn,
        status: &str,
    ) -> IngestResult {
        turn.status = status.to_string();
        self.orchestrator
            .ingest(IngestInput {
                thread_id: run.thread_id.clone(),
                run_id: Some(run.id.clone()),
                node_id: run.root_node_id.clone(),
                provider_session_id: format!("session-{}", run.thread_id),
                provider_instance_id: run.provider_instance_id.clone(),
                raw_event_id: self.next_raw(),
                event: ProviderEvent {
                    event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                    provider_turn: Some(turn),
                    ..Default::default()
                },
                ..Default::default()
            })
            .expect("finish provider turn")
    }
}

fn model(instance: &str) -> ModelSelection {
    ModelSelection {
        instance_id: instance.to_string(),
        model: "model-1".to_string(),
        options: Default::default(),
    }
}

#[test]
fn test_dispatch_creates_run_graph_and_replays_receipt() {
    let h = Harness::new();
    let created = h.create_thread("thread-1");
    assert_eq!(created.sequence, 1);
    assert_eq!(created.stored_events.len(), 1);

    let sent = h.send("thread-1", "send-1", "message-1", "hello");
    assert_eq!(sent.effects.len(), 1);
    assert_eq!(sent.effects[0].kind, EFFECT_PROVIDER_TURN_START);

    let snapshot = h.orchestrator.snapshot("thread-1").unwrap();
    let projection = &snapshot.projection;
    assert_eq!(snapshot.snapshot_sequence, sent.sequence);
    assert_eq!(projection.runs.len(), 1);
    assert_eq!(projection.attempts.len(), 1);
    assert_eq!(projection.nodes.len(), 1);
    assert_eq!(projection.messages.len(), 1);
    assert_eq!(projection.turn_items.len(), 1);

    let run = &projection.runs[0];
    assert_eq!(run.id, derive_run_id("thread-1", 1));
    assert_eq!(run.status, RUN_STARTING);
    assert_eq!(run.ordinal, 1);
    assert_eq!(projection.nodes[0].kind, NODE_ROOT_TURN);
    assert!(projection.nodes[0].counts_for_run);

    let replayed = h
        .orchestrator
        .dispatch(Command {
            command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
            command_id: "send-1".to_string(),
            thread_id: "thread-1".to_string(),
            payload: CommandPayload::DispatchMessage(DispatchMessage {
                message_id: "message-1".to_string(),
                text: "hello".to_string(),
                dispatch_mode: DispatchMode {
                    mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            }),
        })
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.sequence, sent.sequence);
    assert!(replayed.effects.is_empty());
    assert_eq!(replayed.stored_events.len(), sent.stored_events.len());

    let err = h
        .orchestrator
        .dispatch(Command {
            command_type: COMMAND_RUN_INTERRUPT.to_string(),
            command_id: "send-1".to_string(),
            thread_id: "thread-1".to_string(),
            payload: CommandPayload::InterruptRun(InterruptRun {
                run_id: run.id.clone(),
                ..Default::default()
            }),
        })
        .unwrap_err();
    assert!(err.is(ErrorKind::CommandIDConflict), "got {err}");
}

#[test]
fn test_queued_run_starts_only_after_root_terminal() {
    let h = Harness::new();
    h.create_thread("thread-1");
    h.send("thread-1", "send-1", "message-1", "first");
    let (run, turn) = h.start_provider_turn("thread-1");
    let queued = h.send("thread-1", "send-2", "message-2", "second");
    assert!(queued.effects.is_empty());

    let snapshot = h.orchestrator.snapshot("thread-1").unwrap();
    assert_eq!(snapshot.projection.runs[1].status, RUN_QUEUED);

    let finished = h.finish_provider_turn(&run, turn, PROVIDER_TURN_COMPLETED);
    assert_eq!(finished.effects.len(), 1);
    assert_eq!(finished.effects[0].kind, EFFECT_PROVIDER_TURN_START);

    let snapshot = h.orchestrator.snapshot("thread-1").unwrap();
    assert_eq!(snapshot.projection.runs[0].status, RUN_COMPLETED);
    assert_eq!(snapshot.projection.runs[1].status, RUN_STARTING);
    for item in &snapshot.projection.turn_items {
        if item.item_type == "user_message"
            && item.run_id.as_deref() == Some(snapshot.projection.runs[1].id.as_str())
        {
            assert_eq!(item.data.get("inputIntent"), Some(&json!("queued_turn")));
            assert_eq!(item.status, TURN_ITEM_COMPLETED);
        }
    }
}

#[test]
fn test_child_turn_completion_does_not_complete_parent_run() {
    let h = Harness::new();
    h.create_thread("thread-1");
    h.send("thread-1", "send-1", "message-1", "parent");
    let (run, root_turn) = h.start_provider_turn("thread-1");

    let child_node = ExecutionNode {
        id: "node-child".to_string(),
        thread_id: run.thread_id.clone(),
        run_id: Some(run.id.clone()),
        parent_node_id: run.root_node_id.clone(),
        root_node_id: run.root_node_id.clone().unwrap(),
        kind: NODE_SUBAGENT.to_string(),
        status: NODE_RUNNING.to_string(),
        counts_for_run: false,
        provider_thread_id: run.provider_thread_id.clone(),
        ..Default::default()
    };
    h.orchestrator
        .ingest(IngestInput {
            thread_id: run.thread_id.clone(),
            run_id: Some(run.id.clone()),
            raw_event_id: h.next_raw(),
            event: ProviderEvent {
                event_type: PROVIDER_EVENT_NODE_UPDATED.to_string(),
                node: Some(child_node.clone()),
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();

    let child_turn = ProviderTurn {
        id: "provider-turn-child".to_string(),
        provider_thread_id: run.provider_thread_id.clone().unwrap(),
        node_id: child_node.id.clone(),
        ordinal: 1,
        status: PROVIDER_TURN_COMPLETED.to_string(),
        ..Default::default()
    };
    h.orchestrator
        .ingest(IngestInput {
            thread_id: run.thread_id.clone(),
            run_id: Some(run.id.clone()),
            node_id: Some(child_node.id.clone()),
            raw_event_id: h.next_raw(),
            event: ProviderEvent {
                event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                provider_turn: Some(child_turn),
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();

    let snapshot = h.orchestrator.snapshot("thread-1").unwrap();
    let current_run = snapshot.projection.run(&run.id).unwrap();
    let current_child = snapshot.projection.node(&child_node.id).unwrap();
    assert_eq!(current_run.status, RUN_RUNNING);
    assert_eq!(current_child.status, NODE_COMPLETED);

    h.finish_provider_turn(&run, root_turn, PROVIDER_TURN_COMPLETED);
    let snapshot = h.orchestrator.snapshot("thread-1").unwrap();
    let current_run = snapshot.projection.run(&run.id).unwrap();
    assert_eq!(current_run.status, RUN_COMPLETED);
}

#[test]
fn test_interrupt_acknowledgement_is_not_terminal() {
    let h = Harness::new();
    h.create_thread("thread-1");
    h.send("thread-1", "send-1", "message-1", "work");
    let (run, turn) = h.start_provider_turn("thread-1");

    let result = h
        .orchestrator
        .dispatch(Command {
            command_type: COMMAND_RUN_INTERRUPT.to_string(),
            command_id: "stop-1".to_string(),
            thread_id: run.thread_id.clone(),
            payload: CommandPayload::InterruptRun(InterruptRun {
                run_id: run.id.clone(),
                reason: "user stop".to_string(),
                hold_queue: true,
            }),
        })
        .unwrap();
    assert_eq!(result.effects.len(), 1);
    assert_eq!(result.effects[0].kind, EFFECT_PROVIDER_TURN_INTERRUPT);

    let snapshot = h.orchestrator.snapshot(&run.thread_id).unwrap();
    let current = snapshot.projection.run(&run.id).unwrap();
    assert_eq!(current.status, RUN_RUNNING);

    h.finish_provider_turn(&run, turn, PROVIDER_TURN_INTERRUPTED);
    let snapshot = h.orchestrator.snapshot(&run.thread_id).unwrap();
    let current = snapshot.projection.run(&run.id).unwrap();
    assert_eq!(current.status, RUN_INTERRUPTED);

    let mut found_result = false;
    for item in &snapshot.projection.turn_items {
        if item.item_type == "run_interrupt_result" {
            found_result = true;
            assert_eq!(item.status, TURN_ITEM_INTERRUPTED);
            assert_eq!(
                item.parent_item_id.as_deref(),
                Some(derive_run_signal_turn_item_id(&run.id, "interrupt-request").as_str())
            );
        }
    }
    assert!(found_result, "missing run_interrupt_result");
}

#[test]
fn test_stop_holds_automatic_work_until_queue_resume() {
    let h = Harness::new();
    h.create_thread("thread-1");
    h.send("thread-1", "send-1", "message-1", "work");
    let (run, turn) = h.start_provider_turn("thread-1");

    h.orchestrator
        .dispatch(Command {
            command_type: COMMAND_RUN_INTERRUPT.to_string(),
            command_id: "stop-1".to_string(),
            thread_id: run.thread_id.clone(),
            payload: CommandPayload::InterruptRun(InterruptRun {
                run_id: run.id.clone(),
                reason: String::new(),
                hold_queue: true,
            }),
        })
        .unwrap();

    h.orchestrator
        .dispatch(Command {
            command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
            command_id: "wake-1".to_string(),
            thread_id: run.thread_id.clone(),
            payload: CommandPayload::DispatchMessage(DispatchMessage {
                message_id: "wake-message".to_string(),
                text: "background wake".to_string(),
                dispatch_mode: DispatchMode {
                    mode_type: DISPATCH_QUEUE_AFTER_ACTIVE.to_string(),
                    ..Default::default()
                },
                created_by: ACTOR_AGENT.to_string(),
                creation_source: CREATION_PROVIDER.to_string(),
                ..Default::default()
            }),
        })
        .unwrap();

    let finished = h.finish_provider_turn(&run, turn, PROVIDER_TURN_INTERRUPTED);
    assert!(finished.effects.is_empty());

    let snapshot = h.orchestrator.snapshot(&run.thread_id).unwrap();
    assert!(snapshot.projection.runs[1].queue_held);
    assert_eq!(snapshot.projection.runs[1].status, RUN_QUEUED);

    let resumed = h
        .orchestrator
        .dispatch(Command {
            command_type: COMMAND_QUEUE_RESUME.to_string(),
            command_id: "resume-1".to_string(),
            thread_id: run.thread_id.clone(),
            payload: CommandPayload::None,
        })
        .unwrap();
    assert_eq!(resumed.effects.len(), 1);
    assert_eq!(resumed.effects[0].kind, EFFECT_PROVIDER_TURN_START);

    let snapshot = h.orchestrator.snapshot(&run.thread_id).unwrap();
    assert_eq!(snapshot.projection.runs[1].status, RUN_STARTING);
    assert!(!snapshot.projection.runs[1].queue_held);
}

#[test]
fn test_runtime_request_response_requires_live_callback() {
    let h = Harness::new();
    h.create_thread("thread-1");
    h.send("thread-1", "send-1", "message-1", "work");
    let (run, turn) = h.start_provider_turn("thread-1");

    let node_id = "approval-node".to_string();
    let request = RuntimeRequest {
        id: "request-1".to_string(),
        node_id: node_id.clone(),
        provider_turn_id: Some(turn.id),
        kind: REQUEST_COMMAND.to_string(),
        status: REQUEST_PENDING.to_string(),
        response_capability: ResponseCapability {
            capability_type: "live".to_string(),
            provider_session_id: Some("session-thread-1".to_string()),
            ..Default::default()
        },
        created_at: h.now,
        ..Default::default()
    };
    h.orchestrator
        .ingest(IngestInput {
            thread_id: run.thread_id.clone(),
            run_id: Some(run.id.clone()),
            node_id: Some(node_id),
            raw_event_id: h.next_raw(),
            event: ProviderEvent {
                event_type: PROVIDER_EVENT_RUNTIME_REQUEST_UPDATED.to_string(),
                runtime_request: Some(request.clone()),
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();

    let response = h
        .orchestrator
        .dispatch(Command {
            command_type: COMMAND_RUNTIME_REQUEST_RESPOND.to_string(),
            command_id: "answer-1".to_string(),
            thread_id: run.thread_id.clone(),
            payload: CommandPayload::RuntimeRequestResponse(RuntimeRequestResponse {
                request_id: request.id.clone(),
                decision: Some("accept".to_string()),
                ..Default::default()
            }),
        })
        .unwrap();
    assert_eq!(response.effects.len(), 1);
    assert_eq!(
        response.effects[0].kind,
        EFFECT_PROVIDER_RUNTIME_REQUEST_RESPOND
    );

    let snapshot = h.orchestrator.snapshot(&run.thread_id).unwrap();
    let resolved = snapshot.projection.runtime_request(&request.id).unwrap();
    assert_eq!(resolved.status, REQUEST_RESOLVED);
    assert_eq!(resolved.decision.as_deref(), Some("accept"));
}

#[test]
fn test_fork_is_lazy_and_resolves_native_on_first_run() {
    let resolver: Arc<dyn ProviderResolver> = Arc::new(|id: &str| {
        Ok(ProviderInfo {
            driver: id.to_string(),
            capabilities: ProviderCapabilities {
                threads: ThreadCapabilities {
                    can_fork_thread: true,
                    ..Default::default()
                },
                turns: TurnCapabilities {
                    supports_queued_messages: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        })
    });
    let h = Harness::with_resolver(Some(resolver));
    h.create_thread("thread-1");
    h.send("thread-1", "send-1", "message-1", "source");
    let (run, turn) = h.start_provider_turn("thread-1");
    h.finish_provider_turn(&run, turn, PROVIDER_TURN_COMPLETED);

    let forked = h
        .orchestrator
        .dispatch(Command {
            command_type: COMMAND_THREAD_FORK.to_string(),
            command_id: "fork-1".to_string(),
            thread_id: "thread-2".to_string(),
            payload: CommandPayload::ForkThread(ForkThread {
                source_thread_id: "thread-1".to_string(),
                source_point: ForkSourcePoint {
                    point_type: "run".to_string(),
                    run_id: run.id.clone(),
                    checkpoint_id: String::new(),
                },
                ..Default::default()
            }),
        })
        .unwrap();
    assert_eq!(forked.stored_events.len(), 2);

    let snapshot = h.orchestrator.snapshot("thread-2").unwrap();
    assert!(snapshot.projection.provider_threads.is_empty());
    assert_eq!(snapshot.projection.context_transfers[0].status, "pending");

    h.send("thread-2", "send-2", "message-2", "continue");
    let snapshot = h.orchestrator.snapshot("thread-2").unwrap();
    let transfer = &snapshot.projection.context_transfers[0];
    let provider_thread = &snapshot.projection.provider_threads[0];
    assert_eq!(transfer.status, "consumed");
    assert_eq!(
        transfer.resolution.as_ref().unwrap().strategy,
        "native_fork"
    );
    assert!(provider_thread.forked_from.is_some());
    assert!(snapshot.projection.context_handoffs.is_empty());
}

#[test]
fn test_replay_matches_snapshot_and_raw_events_are_idempotent() {
    let h = Harness::new();
    h.create_thread("thread-1");
    h.send("thread-1", "send-1", "message-1", "hello");
    let (run, mut turn) = h.start_provider_turn("thread-1");
    let raw = h.next_raw();
    turn.status = PROVIDER_TURN_COMPLETED.to_string();
    let input = IngestInput {
        thread_id: run.thread_id.clone(),
        run_id: Some(run.id.clone()),
        node_id: run.root_node_id.clone(),
        provider_session_id: "session-thread-1".to_string(),
        provider_instance_id: run.provider_instance_id.clone(),
        raw_event_id: raw,
        event: ProviderEvent {
            event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
            provider_turn: Some(turn),
            ..Default::default()
        },
        ..Default::default()
    };
    let first = h.orchestrator.ingest(input.clone()).unwrap();
    let second = h.orchestrator.ingest(input).unwrap();
    assert!(second.replayed);
    assert_eq!(second.sequence, first.sequence);

    let events = h.orchestrator.events(&EventFilter::default()).unwrap();
    let replayed = replay(&events).unwrap();
    let snapshot = h.orchestrator.snapshot(&run.thread_id).unwrap();
    let expected = serde_json::to_string(&snapshot.projection).unwrap();
    let actual = serde_json::to_string(&replayed[&run.thread_id]).unwrap();
    assert_eq!(expected, actual);

    let remaining = h
        .orchestrator
        .events(&EventFilter {
            after_sequence: Some(snapshot.snapshot_sequence),
            ..Default::default()
        })
        .unwrap();
    assert!(remaining.is_empty());
}

#[test]
fn test_provider_return_uses_delta_handoff() {
    let h = Harness::new();
    h.create_thread("thread-1");
    h.send("thread-1", "send-1", "message-1", "codex work");
    let (run1, turn1) = h.start_provider_turn("thread-1");
    h.finish_provider_turn(&run1, turn1, PROVIDER_TURN_COMPLETED);

    let claude = model("claude");
    h.orchestrator
        .dispatch(Command {
            command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
            command_id: "send-2".to_string(),
            thread_id: "thread-1".to_string(),
            payload: CommandPayload::DispatchMessage(DispatchMessage {
                message_id: "message-2".to_string(),
                text: "claude work".to_string(),
                model_selection: Some(claude),
                dispatch_mode: DispatchMode {
                    mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            }),
        })
        .unwrap();
    let (run2, turn2) = h.start_provider_turn("thread-1");
    h.finish_provider_turn(&run2, turn2, PROVIDER_TURN_COMPLETED);

    let codex = model("codex");
    let result = h
        .orchestrator
        .dispatch(Command {
            command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
            command_id: "send-3".to_string(),
            thread_id: "thread-1".to_string(),
            payload: CommandPayload::DispatchMessage(DispatchMessage {
                message_id: "message-3".to_string(),
                text: "back to codex".to_string(),
                model_selection: Some(codex),
                dispatch_mode: DispatchMode {
                    mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            }),
        })
        .unwrap();
    let EffectPayload::TurnStart(ref start) = result.effects[0].payload else {
        panic!("expected TurnStart effect");
    };
    let handoff = start.context_handoff.as_ref().expect("handoff");
    assert_eq!(handoff.strategy, "delta_since_target_last_seen");
    assert_eq!(handoff.covered_run_ordinals.from, 2);
    assert_eq!(handoff.covered_run_ordinals.to, 2);
    assert!(handoff.summary_text.contains("claude work"));
    assert!(!handoff.summary_text.contains("codex work"));

    let snapshot = h.orchestrator.snapshot("thread-1").unwrap();
    assert_eq!(snapshot.projection.context_transfers.len(), 2);
    assert_eq!(snapshot.projection.context_handoffs.len(), 2);
    assert!(snapshot.projection.runs[2].context_handoff_id.is_some());
}

#[test]
fn test_rejected_command_receipt_is_stable() {
    let h = Harness::new();
    h.create_thread("thread-1");
    let command = Command {
        command_type: COMMAND_RUN_INTERRUPT.to_string(),
        command_id: "bad-stop".to_string(),
        thread_id: "thread-1".to_string(),
        payload: CommandPayload::InterruptRun(InterruptRun {
            run_id: "missing-run".to_string(),
            ..Default::default()
        }),
    };
    let err = h.orchestrator.dispatch(command.clone()).unwrap_err();
    assert!(err.is(ErrorKind::CommandRejected), "got {err}");
    let err2 = h.orchestrator.dispatch(command).unwrap_err();
    assert!(err2.is(ErrorKind::CommandPreviouslyRejected), "got {err2}");
}

#[test]
fn test_event_json_round_trip_preserves_concrete_payload() {
    let h = Harness::new();
    let created = h.create_thread("thread-1");
    let data = serde_json::to_vec(&created.stored_events[0]).unwrap();
    let decoded: StoredEvent = serde_json::from_slice(&data).unwrap();
    let EventPayload::Thread(ref thread) = decoded.event.payload else {
        panic!("expected Thread payload");
    };
    assert_eq!(thread.id, "thread-1");
    assert_eq!(decoded.sequence, 1);
}

#[test]
fn test_memory_store_commit_is_atomic_on_event_failure() {
    let store = Arc::new(MemoryStore::new());
    let o = Orchestrator::new(Arc::clone(&store) as Arc<dyn Store>);
    let created = o
        .dispatch(Command {
            command_type: COMMAND_THREAD_CREATE.to_string(),
            command_id: "create".to_string(),
            thread_id: "thread-1".to_string(),
            payload: CommandPayload::CreateThread(CreateThread {
                project_id: "project".to_string(),
                title: "Before".to_string(),
                model_selection: model("codex"),
                ..Default::default()
            }),
        })
        .unwrap();
    let snapshot = store.snapshot("thread-1").unwrap();
    let mut changed = snapshot.projection.thread.clone();
    changed.title = "After".to_string();

    let res = store.commit(CommitInput {
        thread_id: "thread-1".to_string(),
        expected_sequence: created.sequence,
        events: vec![
            Event {
                id: "event-valid".to_string(),
                event_type: EVENT_THREAD_METADATA_UPDATED.to_string(),
                thread_id: "thread-1".to_string(),
                occurred_at: snapshot.projection.thread.updated_at,
                payload: EventPayload::Thread(changed.clone()),
                ..Default::default()
            },
            Event {
                id: "event-invalid".to_string(),
                event_type: EVENT_THREAD_METADATA_UPDATED.to_string(),
                thread_id: "other-thread".to_string(),
                occurred_at: snapshot.projection.thread.updated_at,
                payload: EventPayload::Thread(changed),
                ..Default::default()
            },
        ],
        ..Default::default()
    });
    assert!(res.is_err());

    let events = store.read(&EventFilter::default()).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, created.sequence);

    let after = store.snapshot("thread-1").unwrap();
    assert_eq!(after.projection.thread.title, "Before");
    assert_eq!(after.snapshot_sequence, created.sequence);
}

#[test]
fn test_memory_store_detects_concurrent_sequence() {
    let store = MemoryStore::new();
    let err = store
        .commit(CommitInput {
            thread_id: "missing".to_string(),
            expected_sequence: 1,
            events: vec![Event {
                id: "event".to_string(),
                event_type: EVENT_THREAD_CREATED.to_string(),
                thread_id: "missing".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap_err();
    assert!(err.is(ErrorKind::SequenceConflict));
}

#[test]
fn test_deterministic_ids_match_t3code_encoding() {
    let got = derive_run_id("thread:provider/native conversation", 2);
    let want = "run:thread:thread%3Aprovider%2Fnative%20conversation:ordinal:2";
    assert_eq!(got, want);

    let got_provider = derive_provider_thread_id("codex", "work/account", "native/1");
    let want_provider =
        "provider-thread:provider:codex:provider-instance:work%2Faccount:native-thread:native%2F1";
    assert_eq!(got_provider, want_provider);
}
