use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::orchestration::command::*;
use crate::orchestration::errors::*;
use crate::orchestration::event::*;
use crate::orchestration::ids::*;
use crate::orchestration::orchestrator::*;
use crate::orchestration::projection::*;
use crate::orchestration::store::*;
use crate::orchestration::types::*;

pub type ProviderEventType = String;

pub const PROVIDER_EVENT_APP_THREAD_CREATED: &str = "app_thread.created";
pub const PROVIDER_EVENT_SESSION_UPDATED: &str = "provider_session.updated";
pub const PROVIDER_EVENT_THREAD_UPDATED: &str = "provider_thread.updated";
pub const PROVIDER_EVENT_TURN_UPDATED: &str = "provider_turn.updated";
pub const PROVIDER_EVENT_NODE_UPDATED: &str = "node.updated";
pub const PROVIDER_EVENT_SUBAGENT_UPDATED: &str = "subagent.updated";
pub const PROVIDER_EVENT_MESSAGE_UPDATED: &str = "message.updated";
pub const PROVIDER_EVENT_TURN_ITEM_UPDATED: &str = "turn_item.updated";
pub const PROVIDER_EVENT_RUNTIME_REQUEST_UPDATED: &str = "runtime_request.updated";
pub const PROVIDER_EVENT_PLAN_UPDATED: &str = "plan.updated";
pub const PROVIDER_EVENT_BACKGROUND_WORK_UPDATED: &str = "background_work.updated";
pub const PROVIDER_EVENT_COMMAND_OUTPUT_DELTA: &str = "command_output.delta";
pub const PROVIDER_EVENT_TURN_TERMINAL: &str = "turn.terminal";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProviderFailure {
    #[serde(default)]
    pub class: String,
    #[serde(default)]
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub code: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reset_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProviderRetry {
    #[serde(default)]
    pub attempt: i64,
    #[serde(default)]
    pub max_attempts: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub delay_ms: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TerminalEvent {
    #[serde(default)]
    pub provider_thread_id: ProviderThreadID,
    #[serde(default)]
    pub provider_turn_id: ProviderTurnID,
    #[serde(default)]
    pub run_ordinal: i64,
    #[serde(default)]
    pub status: ProviderTurnStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<ProviderFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<ProviderRetry>,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub failure_item_ordinal: i64,
    #[serde(default)]
    pub thread_disposition: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProviderEvent {
    #[serde(rename = "type", default)]
    pub event_type: ProviderEventType,
    #[serde(default)]
    pub driver: ProviderDriverKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_thread: Option<Thread>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub create_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session: Option<ProviderSession>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_thread: Option<ProviderThread>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn: Option<ProviderTurn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<ExecutionNode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent: Option<Subagent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<ConversationMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_item: Option<TurnItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_request: Option<RuntimeRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<PlanArtifact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<TerminalEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RunGuard {
    pub run_id: RunID,
    pub active_attempt_id: RunAttemptID,
    pub expected_status: RunStatus,
}

#[derive(Debug, Clone, Default)]
pub struct IngestInput {
    pub provider_session_id: ProviderSessionID,
    pub provider_instance_id: ProviderInstanceID,
    pub command_id: Option<CommandID>,
    pub thread_id: ThreadID,
    pub run_id: Option<RunID>,
    pub node_id: Option<NodeID>,
    pub raw_event_id: Option<RawEventID>,
    pub event: ProviderEvent,
    pub guard: Option<RunGuard>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct IngestResult {
    #[serde(default)]
    pub sequence: u64,
    #[serde(default)]
    pub stored_events: Vec<StoredEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effects: Vec<Effect>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub replayed: bool,
}

impl Orchestrator {
    pub fn ingest(&self, input: IngestInput) -> Result<IngestResult, Error> {
        if input.thread_id.is_empty() {
            return Err(Error::with_detail(
                ErrorKind::InvalidProviderEvent,
                "threadId is required",
            ));
        }
        let thread_lock = self.lock(&input.thread_id);
        let _guard = thread_lock.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(ref raw_event_id) = input.raw_event_id {
            let existing = self.store.read(&EventFilter {
                raw_event_id: Some(raw_event_id.clone()),
                limit: 1,
                ..Default::default()
            })?;
            if !existing.is_empty() {
                let snapshot = self.store.snapshot(&input.thread_id)?;
                return Ok(IngestResult {
                    sequence: snapshot.snapshot_sequence,
                    stored_events: Vec::new(),
                    effects: Vec::new(),
                    replayed: true,
                });
            }
        }

        let snapshot = match self.store.snapshot(&input.thread_id) {
            Ok(snap) => snap,
            Err(err) => {
                if is_not_found(&err)
                    && input.event.event_type == PROVIDER_EVENT_APP_THREAD_CREATED
                    && input
                        .event
                        .app_thread
                        .as_ref()
                        .map(|t| t.id == input.thread_id)
                        .unwrap_or(false)
                {
                    ThreadSnapshot::default()
                } else {
                    return Err(err);
                }
            }
        };

        if let Some(ref guard) = input.guard {
            let valid = snapshot
                .projection
                .run(&guard.run_id)
                .map(|run| {
                    (guard.expected_status.is_empty() || run.status == guard.expected_status)
                        && run.active_attempt_id.as_deref() == Some(guard.active_attempt_id.as_str())
                })
                .unwrap_or(false);
            if !valid {
                return Err(Error::with_detail(
                    ErrorKind::SequenceConflict,
                    format!("provider event is stale for run {}", guard.run_id),
                ));
            }
        }

        let mut plan = CommandPlan {
            orchestrator: self,
            command: Command {
                thread_id: input.thread_id.clone(),
                command_id: input.command_id.clone().unwrap_or_default(),
                ..Default::default()
            },
            now: (self.now)(),
            events: Vec::new(),
            effects: Vec::new(),
            projection: snapshot.projection,
            expected_sequences: HashMap::new(),
        };

        let event_type = input.event.event_type.clone();
        let thread_id = input.thread_id.clone();
        self.normalize_provider_event(&mut plan, &input)
            .map_err(|err| {
                Error::with_detail(
                    err.kind,
                    format!("normalize {event_type} for thread {thread_id}: {err}"),
                )
            })?;

        if plan.events.is_empty() {
            return Ok(IngestResult {
                sequence: snapshot.snapshot_sequence,
                stored_events: Vec::new(),
                effects: Vec::new(),
                replayed: false,
            });
        }

        let committed = self
            .store
            .commit(CommitInput {
                thread_id: input.thread_id,
                expected_sequence: snapshot.snapshot_sequence,
                expected_sequences: plan.expected_sequences,
                events: plan.events,
                effects: plan.effects.clone(),
                receipt: None,
                command_id: input.command_id,
            })
            .map_err(|err| Error::with_detail(err.kind, format!("commit provider event: {err}")))?;

        Ok(IngestResult {
            sequence: committed.sequence,
            stored_events: committed.stored_events,
            effects: plan.effects,
            replayed: false,
        })
    }

    fn normalize_provider_event(
        &self,
        plan: &mut CommandPlan<'_>,
        input: &IngestInput,
    ) -> Result<(), Error> {
        let event = &input.event;
        let mut base = EventSpec {
            event_type: String::new(),
            thread_id: input.thread_id.clone(),
            run_id: input.run_id.clone(),
            node_id: input.node_id.clone(),
            driver: event.driver.clone(),
            instance_id: input.provider_instance_id.clone(),
            raw_event_id: input.raw_event_id.clone(),
            payload: EventPayload::default(),
        };

        match event.event_type.as_str() {
            PROVIDER_EVENT_APP_THREAD_CREATED => {
                let Some(ref app_thread) = event.app_thread else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "appThread is required",
                    ));
                };
                base.event_type = EVENT_THREAD_CREATED.to_string();
                if !plan.projection.thread.id.is_empty() {
                    if event.create_only {
                        return Ok(());
                    }
                    base.event_type = EVENT_THREAD_METADATA_UPDATED.to_string();
                }
                base.payload = EventPayload::Thread(app_thread.clone());
                plan.emit_and_apply(base)?;
                Ok(())
            }
            PROVIDER_EVENT_SESSION_UPDATED => {
                let Some(ref session) = event.provider_session else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "providerSession is required",
                    ));
                };
                base.event_type = EVENT_PROVIDER_SESSION_UPDATED.to_string();
                base.payload = EventPayload::ProviderSession(session.clone());
                plan.emit_and_apply(base)?;
                Ok(())
            }
            PROVIDER_EVENT_THREAD_UPDATED => {
                let Some(ref provider_thread) = event.provider_thread else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "providerThread is required",
                    ));
                };
                base.event_type = EVENT_PROVIDER_THREAD_UPDATED.to_string();
                base.payload = EventPayload::ProviderThread(provider_thread.clone());
                plan.emit_and_apply(base)?;
                Ok(())
            }
            PROVIDER_EVENT_TURN_UPDATED => {
                let Some(ref provider_turn) = event.provider_turn else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "providerTurn is required",
                    ));
                };
                self.plan_provider_turn(plan, base, provider_turn.clone(), None)
            }
            PROVIDER_EVENT_NODE_UPDATED => {
                let Some(ref node) = event.node else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "node is required",
                    ));
                };
                if let Some(current) = plan.projection.node(&node.id) {
                    if nodes_equal(&current, node) {
                        return Ok(());
                    }
                }
                base.event_type = EVENT_NODE_UPDATED.to_string();
                base.thread_id = node.thread_id.clone();
                base.run_id = node.run_id.clone();
                base.node_id = Some(node.id.clone());
                base.payload = EventPayload::ExecutionNode(node.clone());
                plan.emit_and_apply(base)?;
                Ok(())
            }
            PROVIDER_EVENT_SUBAGENT_UPDATED => {
                let Some(ref subagent) = event.subagent else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "subagent is required",
                    ));
                };
                base.event_type = EVENT_SUBAGENT_UPDATED.to_string();
                base.thread_id = subagent.thread_id.clone();
                base.run_id = subagent.run_id.clone();
                base.node_id = Some(subagent.id.clone());
                base.payload = EventPayload::Subagent(subagent.clone());
                plan.emit_and_apply(base)?;
                Ok(())
            }
            PROVIDER_EVENT_MESSAGE_UPDATED => {
                let Some(ref message) = event.message else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "message is required",
                    ));
                };
                base.event_type = EVENT_MESSAGE_UPDATED.to_string();
                base.thread_id = message.thread_id.clone();
                base.run_id = message.run_id.clone();
                base.node_id = message.node_id.clone();
                base.payload = EventPayload::ConversationMessage(message.clone());
                plan.emit_and_apply(base)?;
                Ok(())
            }
            PROVIDER_EVENT_TURN_ITEM_UPDATED => {
                let Some(ref turn_item) = event.turn_item else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "turnItem is required",
                    ));
                };
                base.event_type = EVENT_TURN_ITEM_UPDATED.to_string();
                base.thread_id = turn_item.thread_id.clone();
                base.run_id = turn_item.run_id.clone();
                base.node_id = turn_item.node_id.clone();
                base.payload = EventPayload::TurnItem(turn_item.clone());
                plan.emit_and_apply(base)?;
                Ok(())
            }
            PROVIDER_EVENT_RUNTIME_REQUEST_UPDATED => {
                let Some(ref runtime_request) = event.runtime_request else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "runtimeRequest is required",
                    ));
                };
                let request = runtime_request.clone();
                base.event_type = EVENT_RUNTIME_REQUEST_UPDATED.to_string();
                base.node_id = Some(request.node_id.clone());
                base.payload = EventPayload::RuntimeRequest(request.clone());
                plan.emit_and_apply(base)?;

                if request.status == REQUEST_PENDING {
                    if let Some(mut node) = plan.projection.node(&request.node_id) {
                        if let Some(ref node_run_id) = node.run_id.clone() {
                            node.status = NODE_WAITING.to_string();
                            plan.emit_and_apply(EventSpec {
                                event_type: EVENT_NODE_UPDATED.to_string(),
                                thread_id: node.thread_id.clone(),
                                run_id: node.run_id.clone(),
                                node_id: Some(node.id.clone()),
                                driver: event.driver.clone(),
                                instance_id: input.provider_instance_id.clone(),
                                raw_event_id: input.raw_event_id.clone(),
                                payload: EventPayload::ExecutionNode(node),
                            })?;
                            if let Some(mut run) = plan.projection.run(node_run_id) {
                                if run.status == RUN_RUNNING {
                                    run.status = RUN_WAITING.to_string();
                                    plan.emit_and_apply(EventSpec {
                                        event_type: EVENT_RUN_UPDATED.to_string(),
                                        thread_id: run.thread_id.clone(),
                                        run_id: Some(run.id.clone()),
                                        node_id: run.root_node_id.clone(),
                                        driver: event.driver.clone(),
                                        instance_id: run.provider_instance_id.clone(),
                                        raw_event_id: input.raw_event_id.clone(),
                                        payload: EventPayload::Run(run),
                                    })?;
                                }
                            }
                        }
                    }
                }
                Ok(())
            }
            PROVIDER_EVENT_PLAN_UPDATED => {
                let Some(ref plan_in) = event.plan else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "plan is required",
                    ));
                };
                let plan_artifact = clone_json(plan_in)
                    .map_err(|e| Error::internal(format!("clone plan: {e}")))?;
                let plan_artifact =
                    with_plan_step_durations(plan_artifact, &plan.projection.plans, plan.now);
                base.event_type = EVENT_PLAN_UPDATED.to_string();
                base.thread_id = plan_artifact.thread_id.clone();
                base.run_id = plan_artifact.run_id.clone();
                base.node_id = Some(plan_artifact.node_id.clone());
                base.payload = EventPayload::PlanArtifact(plan_artifact);
                plan.emit_and_apply(base)?;
                Ok(())
            }
            PROVIDER_EVENT_TURN_TERMINAL => {
                let Some(ref terminal) = event.terminal else {
                    return Err(Error::with_detail(
                        ErrorKind::InvalidProviderEvent,
                        "terminal is required",
                    ));
                };
                let mut turn = plan
                    .projection
                    .provider_turn(&terminal.provider_turn_id)
                    .ok_or_else(|| {
                        Error::with_detail(
                            ErrorKind::InvalidProviderEvent,
                            format!("provider turn {} not found", terminal.provider_turn_id),
                        )
                    })?;
                if terminal.status == PROVIDER_TURN_FAILED && terminal.failure.is_some() {
                    let item = provider_failure_turn_item(terminal, input, plan.now);
                    base.run_id = item.run_id.clone();
                    base.node_id = item.node_id.clone();
                    let fail_spec = EventSpec {
                        event_type: EVENT_TURN_ITEM_UPDATED.to_string(),
                        thread_id: base.thread_id.clone(),
                        run_id: base.run_id.clone(),
                        node_id: base.node_id.clone(),
                        driver: base.driver.clone(),
                        instance_id: base.instance_id.clone(),
                        raw_event_id: base.raw_event_id.clone(),
                        payload: EventPayload::TurnItem(item),
                    };
                    plan.emit_and_apply(fail_spec)?;
                }
                turn.status = terminal.status.clone();
                self.plan_provider_turn(plan, base, turn, Some(terminal))
            }
            PROVIDER_EVENT_BACKGROUND_WORK_UPDATED | PROVIDER_EVENT_COMMAND_OUTPUT_DELTA => Ok(()),
            _ => Err(Error::with_detail(
                ErrorKind::InvalidProviderEvent,
                format!("unknown provider event {:?}", event.event_type),
            )),
        }
    }

    fn plan_provider_turn(
        &self,
        plan: &mut CommandPlan<'_>,
        mut base: EventSpec,
        mut turn: ProviderTurn,
        terminal: Option<&TerminalEvent>,
    ) -> Result<(), Error> {
        if turn.provider_thread_id.is_empty() || turn.node_id.is_empty() || turn.ordinal < 1 {
            return Err(Error::with_detail(
                ErrorKind::InvalidProviderEvent,
                "provider turn identity and ordinal are required",
            ));
        }
        let now = plan.now;
        if turn.status == PROVIDER_TURN_RUNNING {
            turn.started_at = Some(update_time(turn.started_at, now));
        }
        if provider_turn_status_is_terminal(&turn.status) {
            turn.completed_at = Some(update_time(turn.completed_at, now));
        }
        let base_driver = base.driver.clone();
        let base_instance = base.instance_id.clone();
        let base_raw = base.raw_event_id.clone();

        base.event_type = EVENT_PROVIDER_TURN_UPDATED.to_string();
        base.node_id = Some(turn.node_id.clone());
        base.payload = EventPayload::ProviderTurn(turn.clone());
        plan.emit_and_apply(base)?;

        // Pending native user inputs cannot outlive their provider callback turn.
        if provider_turn_status_is_terminal(&turn.status) {
            let requests = plan.projection.runtime_requests.clone();
            for mut request in requests {
                if request.provider_turn_id.as_deref() != Some(turn.id.as_str())
                    || request.status != REQUEST_PENDING
                {
                    continue;
                }
                request.status = REQUEST_CANCELLED.to_string();
                request.resolved_at = Some(now);
                plan.emit_and_apply(EventSpec {
                    event_type: EVENT_RUNTIME_REQUEST_UPDATED.to_string(),
                    thread_id: plan.command.thread_id.clone(),
                    node_id: Some(request.node_id.clone()),
                    driver: base_driver.clone(),
                    instance_id: base_instance.clone(),
                    raw_event_id: base_raw.clone(),
                    payload: EventPayload::RuntimeRequest(request),
                    ..Default::default()
                })?;
            }
        }

        let mut node = plan.projection.node(&turn.node_id).ok_or_else(|| {
            Error::with_detail(
                ErrorKind::InvalidProviderEvent,
                format!(
                    "provider turn {} references unknown node {}",
                    turn.id, turn.node_id
                ),
            )
        })?;
        node.provider_turn_id = Some(turn.id.clone());
        if node.provider_thread_id.is_none() {
            node.provider_thread_id = Some(turn.provider_thread_id.clone());
        }
        if turn.status == PROVIDER_TURN_RUNNING {
            node.status = NODE_RUNNING.to_string();
            node.started_at = Some(update_time(node.started_at, now));
        } else if provider_turn_status_is_terminal(&turn.status) {
            node.status = node_status_for_turn(&turn.status);
            node.completed_at = Some(update_time(node.completed_at, now));
        }
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_NODE_UPDATED.to_string(),
            thread_id: node.thread_id.clone(),
            run_id: node.run_id.clone(),
            node_id: Some(node.id.clone()),
            driver: base_driver.clone(),
            instance_id: base_instance.clone(),
            raw_event_id: base_raw.clone(),
            payload: EventPayload::ExecutionNode(node.clone()),
        })?;

        let Some(ref node_run_id) = node.run_id else {
            return Ok(());
        };
        let mut run = plan.projection.run(node_run_id).ok_or_else(|| {
            Error::with_detail(
                ErrorKind::InvalidProviderEvent,
                format!("node {} references unknown run {}", node.id, node_run_id),
            )
        })?;

        let is_active_root = node.kind == NODE_ROOT_TURN
            && node.counts_for_run
            && run.root_node_id.as_deref() == Some(node.id.as_str())
            && run.active_attempt_id.is_some()
            && turn.run_attempt_id.is_some()
            && run.active_attempt_id == turn.run_attempt_id;
        if !is_active_root {
            return Ok(());
        }

        let active_attempt_id = run.active_attempt_id.clone().unwrap();
        let mut attempt = plan
            .projection
            .attempts
            .iter()
            .find(|c| c.id == active_attempt_id)
            .cloned()
            .ok_or_else(|| {
                Error::with_detail(
                    ErrorKind::InvalidProviderEvent,
                    format!("active attempt {active_attempt_id} not found"),
                )
            })?;

        attempt.provider_turn_id = Some(turn.id.clone());
        if turn.status == PROVIDER_TURN_RUNNING {
            attempt.status = ATTEMPT_RUNNING.to_string();
            attempt.started_at = Some(update_time(attempt.started_at, now));
            run.status = RUN_RUNNING.to_string();
            run.started_at = Some(update_time(run.started_at, now));
        } else if provider_turn_status_is_terminal(&turn.status)
            && !run_status_is_terminal(&run.status)
        {
            attempt.status = attempt_status_for_turn(&turn.status);
            attempt.completed_at = Some(update_time(attempt.completed_at, now));
            run.status = run_status_for_turn(&turn.status);
            run.started_at = Some(update_time(run.started_at, now));
            run.completed_at = Some(update_time(run.completed_at, now));
        }

        plan.emit_and_apply(EventSpec {
            event_type: EVENT_RUN_ATTEMPT_UPDATED.to_string(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run.id.clone()),
            node_id: Some(node.id.clone()),
            driver: base_driver.clone(),
            instance_id: base_instance.clone(),
            raw_event_id: base_raw.clone(),
            payload: EventPayload::RunAttempt(attempt),
        })?;
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_RUN_UPDATED.to_string(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run.id.clone()),
            node_id: Some(node.id.clone()),
            driver: base_driver.clone(),
            instance_id: base_instance.clone(),
            raw_event_id: base_raw.clone(),
            payload: EventPayload::Run(run.clone()),
        })?;

        if run_status_is_terminal(&run.status) {
            self.finalize_delegated_task(plan, &run)?;
            if run.status == RUN_INTERRUPTED {
                let request_item_id =
                    derive_run_signal_turn_item_id(&run.id, "interrupt-request");
                let has_request = plan
                    .projection
                    .turn_items
                    .iter()
                    .any(|item| item.id == request_item_id);
                if has_request {
                    let result_item = TurnItem {
                        created_by: String::new(),
                        creation_source: String::new(),
                        id: derive_run_signal_turn_item_id(&run.id, "interrupt-result"),
                        thread_id: run.thread_id.clone(),
                        run_id: Some(run.id.clone()),
                        node_id: run.root_node_id.clone(),
                        provider_thread_id: run.provider_thread_id.clone(),
                        provider_turn_id: Some(turn.id.clone()),
                        native_item_ref: None,
                        parent_item_id: Some(request_item_id),
                        ordinal: next_turn_item_ordinal(&plan.projection),
                        status: TURN_ITEM_INTERRUPTED.to_string(),
                        title: "Run interrupted".to_string(),
                        started_at: Some(now),
                        completed_at: Some(now),
                        updated_at: now,
                        item_type: "run_interrupt_result".to_string(),
                        message_id: String::new(),
                        text: String::new(),
                        request_id: String::new(),
                        data: BTreeMap::new(),
                    };
                    plan.emit_and_apply(EventSpec {
                        event_type: EVENT_TURN_ITEM_UPDATED.to_string(),
                        thread_id: run.thread_id.clone(),
                        run_id: Some(run.id.clone()),
                        node_id: run.root_node_id.clone(),
                        driver: base_driver,
                        instance_id: base_instance,
                        raw_event_id: base_raw.clone(),
                        payload: EventPayload::TurnItem(result_item),
                    })?;
                }
            }
            if let Some(mut provider_thread) =
                plan.projection.provider_thread(&turn.provider_thread_id)
            {
                provider_thread.status = if terminal
                    .map(|t| t.thread_disposition == "broken")
                    .unwrap_or(false)
                {
                    PROVIDER_THREAD_ERROR.to_string()
                } else {
                    PROVIDER_THREAD_IDLE.to_string()
                };
                provider_thread.updated_at = now;
                plan.emit_and_apply(EventSpec {
                    event_type: EVENT_PROVIDER_THREAD_UPDATED.to_string(),
                    thread_id: run.thread_id.clone(),
                    run_id: Some(run.id.clone()),
                    driver: provider_thread.driver.clone(),
                    instance_id: provider_thread.provider_instance_id.clone(),
                    raw_event_id: base_raw,
                    payload: EventPayload::ProviderThread(provider_thread),
                    ..Default::default()
                })?;
            }
            return self.start_next_queued_run(plan);
        }
        Ok(())
    }
}

fn nodes_equal(left: &ExecutionNode, right: &ExecutionNode) -> bool {
    let Ok(left_clone) = clone_json(left) else {
        return false;
    };
    let Ok(right_clone) = clone_json(right) else {
        return false;
    };
    json_values_equal(&left_clone, &right_clone)
}

fn node_status_for_turn(status: &str) -> NodeStatus {
    match status {
        PROVIDER_TURN_COMPLETED => NODE_COMPLETED.to_string(),
        PROVIDER_TURN_INTERRUPTED => NODE_INTERRUPTED.to_string(),
        PROVIDER_TURN_FAILED => NODE_FAILED.to_string(),
        PROVIDER_TURN_CANCELLED => NODE_CANCELLED.to_string(),
        _ => NODE_PENDING.to_string(),
    }
}

fn attempt_status_for_turn(status: &str) -> AttemptStatus {
    match status {
        PROVIDER_TURN_COMPLETED => ATTEMPT_COMPLETED.to_string(),
        PROVIDER_TURN_INTERRUPTED => ATTEMPT_INTERRUPTED.to_string(),
        PROVIDER_TURN_FAILED => ATTEMPT_FAILED.to_string(),
        PROVIDER_TURN_CANCELLED => ATTEMPT_CANCELLED.to_string(),
        _ => ATTEMPT_PENDING.to_string(),
    }
}

fn run_status_for_turn(status: &str) -> RunStatus {
    match status {
        PROVIDER_TURN_COMPLETED => RUN_COMPLETED.to_string(),
        PROVIDER_TURN_INTERRUPTED => RUN_INTERRUPTED.to_string(),
        PROVIDER_TURN_FAILED => RUN_FAILED.to_string(),
        PROVIDER_TURN_CANCELLED => RUN_CANCELLED.to_string(),
        _ => RUN_RUNNING.to_string(),
    }
}

fn provider_failure_turn_item(
    terminal: &TerminalEvent,
    input: &IngestInput,
    now: DateTime<Utc>,
) -> TurnItem {
    let failure_msg = terminal
        .failure
        .as_ref()
        .map(|f| f.message.clone())
        .unwrap_or_default();
    let mut data = BTreeMap::new();
    data.insert("failure".to_string(), json!(terminal.failure));
    data.insert("retry".to_string(), json!(terminal.retry));
    TurnItem {
        created_by: String::new(),
        creation_source: String::new(),
        id: join_id(
            "turn-item",
            &[
                &"provider-turn",
                &terminal.provider_turn_id,
                &"failure",
                &terminal.failure_item_ordinal,
            ],
        ),
        thread_id: input.thread_id.clone(),
        run_id: input.run_id.clone(),
        node_id: input.node_id.clone(),
        provider_thread_id: Some(terminal.provider_thread_id.clone()),
        provider_turn_id: Some(terminal.provider_turn_id.clone()),
        native_item_ref: None,
        parent_item_id: None,
        ordinal: terminal.failure_item_ordinal,
        status: TURN_ITEM_FAILED.to_string(),
        title: failure_msg,
        started_at: Some(now),
        completed_at: Some(now),
        updated_at: now,
        item_type: "provider_failure".to_string(),
        message_id: String::new(),
        text: String::new(),
        request_id: String::new(),
        data,
    }
}

pub(crate) fn with_plan_step_durations(
    mut next: PlanArtifact,
    existing: &[PlanArtifact],
    now: DateTime<Utc>,
) -> PlanArtifact {
    if next.kind != "todo_list" {
        return next;
    }
    let previous = existing
        .iter()
        .find(|p| p.id == next.id && p.kind == next.kind);
    for step in &mut next.steps {
        let prior = previous.and_then(|prev| {
            prev.steps
                .iter()
                .find(|candidate| candidate.id == step.id && candidate.text == step.text)
        });
        match step.status.as_str() {
            "running" => {
                if let Some(p) = prior {
                    if p.status == "running" && p.duration_anchor_at.is_some() {
                        step.duration_anchor_at = p.duration_anchor_at;
                    } else {
                        step.duration_anchor_at = Some(now);
                    }
                } else {
                    step.duration_anchor_at = Some(now);
                }
            }
            "completed" => {
                if let Some(p) = prior {
                    if p.status == "completed" {
                        step.duration_ms = p.duration_ms;
                        continue;
                    }
                    if let Some(anchor) = p.duration_anchor_at {
                        let duration = (now - anchor).num_milliseconds().max(0);
                        step.duration_ms = Some(duration);
                    }
                }
            }
            _ => {}
        }
    }
    next
}
