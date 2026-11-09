use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use sha2::{Digest, Sha256};

use crate::orchestration::command::{
    Effect, EffectPayload, RuntimeRequestRespondEffect, TurnInterruptEffect, TurnStartEffect,
    EFFECT_PROVIDER_RUNTIME_REQUEST_RESPOND, EFFECT_PROVIDER_TURN_INTERRUPT,
    EFFECT_PROVIDER_TURN_START,
};
use crate::orchestration::errors::{Error, ErrorKind};
use crate::orchestration::ids::{
    derive_message_from_provider_item_id, derive_node_from_provider_item_id,
    derive_turn_item_from_provider_item_id, MessageID, ProviderInstanceID, ProviderSessionID,
    ProviderTurnID, TurnItemID,
};
use crate::orchestration::orchestrator::{Orchestrator, ProviderInfo};
use crate::orchestration::outbox::{EffectExecutor, ExecutionContext};
use crate::orchestration::provider::{
    IngestInput, ProviderEvent, ProviderFailure, RunGuard, PROVIDER_EVENT_MESSAGE_UPDATED,
    PROVIDER_EVENT_NODE_UPDATED, PROVIDER_EVENT_SESSION_UPDATED, PROVIDER_EVENT_THREAD_UPDATED,
    PROVIDER_EVENT_TURN_ITEM_UPDATED, PROVIDER_EVENT_TURN_UPDATED,
};
use crate::orchestration::types::{
    ConversationMessage, ExecutionNode, ProviderCapabilities, ProviderDriverKind, ProviderRef,
    ProviderSession, ProviderThread, ProviderTurn, ProviderTurnStatus, TurnCapabilities, TurnItem,
    ACTOR_AGENT, CREATION_PROVIDER, MESSAGE_ASSISTANT, NATIVE_REF_STRONG, NODE_ASSISTANT_MESSAGE,
    NODE_COMPLETED, NODE_RUNNING, PROVIDER_THREAD_ACTIVE, PROVIDER_TURN_CANCELLED,
    PROVIDER_TURN_COMPLETED, PROVIDER_TURN_FAILED, PROVIDER_TURN_INTERRUPTED,
    PROVIDER_TURN_RUNNING, SESSION_RUNNING, SESSION_STOPPED, TURN_ITEM_COMPLETED,
    TURN_ITEM_FAILED, TURN_ITEM_RUNNING,
};

use super::acp::ACPAdapter;
use super::claude::ClaudeAdapter;
use super::codex::CodexAdapter;
use super::models::Model;
use super::pi::PiAdapter;

pub type EmitFn =
    Arc<dyn Fn(&ExecutionContext, ProviderEvent, &[u8]) -> Result<(), Error> + Send + Sync>;

#[derive(Clone)]
pub struct RunRequest {
    pub start: TurnStartEffect,
    pub session: Arc<Mutex<ProviderSession>>,
    pub provider_thread: Arc<Mutex<ProviderThread>>,
    pub provider_turn: Arc<Mutex<ProviderTurn>>,
    pub tool_types: Arc<Mutex<HashMap<String, String>>>,
    pub state_mu: Arc<Mutex<()>>,
}

impl RunRequest {
    pub fn current_turn(&self) -> ProviderTurn {
        self.provider_turn
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn current_provider_thread(&self) -> ProviderThread {
        self.provider_thread
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn current_session(&self) -> ProviderSession {
        self.session
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn set_turn_status(&self, status: &str) {
        let mut turn = self.provider_turn.lock().unwrap_or_else(|e| e.into_inner());
        turn.status = status.to_string();
    }

    pub fn set_turn(&self, updated: ProviderTurn) {
        let mut turn = self.provider_turn.lock().unwrap_or_else(|e| e.into_inner());
        *turn = updated;
    }

    pub fn set_provider_thread(&self, updated: ProviderThread) {
        let mut pt = self
            .provider_thread
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *pt = updated;
    }

    pub fn record_tool_type(&self, tool_id: &str, item_type: &str) {
        let mut guard = self.tool_types.lock().unwrap_or_else(|e| e.into_inner());
        guard.insert(tool_id.to_string(), item_type.to_string());
    }

    pub fn lookup_tool_type(&self, tool_id: &str) -> String {
        let guard = self.tool_types.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .get(tool_id)
            .cloned()
            .unwrap_or_else(|| "dynamic_tool".to_string())
    }
}

pub trait Adapter: Send + Sync {
    fn driver(&self) -> ProviderDriverKind;
    fn capabilities(&self) -> ProviderCapabilities;
    fn run(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        emit: &EmitFn,
    ) -> Result<(), Error>;

    fn handle_frame(
        &self,
        _ctx: &ExecutionContext,
        _request: &RunRequest,
        _frame: &[u8],
        _emit: &EmitFn,
    ) -> Result<(), Error> {
        Err(Error::sentinel(ErrorKind::UnsupportedCapability))
    }

    fn interrupt(&self, _ctx: &ExecutionContext, _effect: TurnInterruptEffect) -> Result<(), Error> {
        Err(Error::sentinel(ErrorKind::UnsupportedCapability))
    }

    fn respond_runtime_request(
        &self,
        _ctx: &ExecutionContext,
        _effect: RuntimeRequestRespondEffect,
    ) -> Result<(), Error> {
        Err(Error::sentinel(ErrorKind::UnsupportedCapability))
    }

    fn close(&self) -> Result<(), Error> {
        Ok(())
    }

    fn models(&self, _ctx: &ExecutionContext, _cwd: &str) -> Result<Vec<Model>, Error> {
        Ok(vec![Model {
            id: "default".to_string(),
            label: "Default".to_string(),
            options: Vec::new(),
        }])
    }
}

#[derive(Clone)]
struct ActiveTurn {
    ctx: ExecutionContext,
    adapter: Arc<dyn Adapter>,
}

#[derive(Clone)]
pub struct Executor {
    orchestrator: Arc<Orchestrator>,
    adapters: Arc<Mutex<HashMap<ProviderInstanceID, Arc<dyn Adapter>>>>,
    active: Arc<Mutex<HashMap<ProviderTurnID, ActiveTurn>>>,
}

impl Executor {
    pub fn new(orchestrator: Arc<Orchestrator>) -> Self {
        let adapters: Arc<Mutex<HashMap<ProviderInstanceID, Arc<dyn Adapter>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let adapters_for_resolver = Arc::clone(&adapters);
        orchestrator.set_provider_resolver(Arc::new(move |instance_id: &str| {
            let guard = adapters_for_resolver
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(adapter) = guard.get(instance_id) {
                Ok(ProviderInfo {
                    driver: adapter.driver(),
                    capabilities: adapter.capabilities(),
                })
            } else {
                Ok(ProviderInfo {
                    driver: instance_id.to_string(),
                    capabilities: ProviderCapabilities {
                        turns: TurnCapabilities {
                            supports_queued_messages: true,
                            terminal_status_quality: "strong".to_string(),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                })
            }
        }));
        Self {
            orchestrator,
            adapters,
            active: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn orchestrator(&self) -> &Arc<Orchestrator> {
        &self.orchestrator
    }

    pub fn register<A: Adapter + 'static>(&self, instance_id: impl Into<String>, adapter: A) {
        self.register_arc(instance_id, Arc::new(adapter));
    }

    pub fn register_arc(&self, instance_id: impl Into<String>, adapter: Arc<dyn Adapter>) {
        let mut guard = self.adapters.lock().unwrap_or_else(|e| e.into_inner());
        guard.insert(instance_id.into(), adapter);
    }

    pub fn register_defaults(&self) {
        self.register("codex", CodexAdapter::default());
        self.register("claude", ClaudeAdapter::default());
        self.register("claudeAgent", ClaudeAdapter::default());
        self.register("pi", PiAdapter::default());
        self.register(
            "agy",
            ACPAdapter {
                binary: "agy".to_string(),
                args: vec![
                    "acp".to_string(),
                    "--sandbox".to_string(),
                    "--disable-telemetry".to_string(),
                ],
                driver_kind: "agy".to_string(),
                load_method: "session/load".to_string(),
                mode: "plan".to_string(),
                ..Default::default()
            },
        );
        self.register(
            "fx",
            ACPAdapter {
                binary: "fx".to_string(),
                args: vec!["acp".to_string()],
                driver_kind: "fx".to_string(),
                load_method: "session/resume".to_string(),
                mode: "ask".to_string(),
                ..Default::default()
            },
        );
    }

    pub fn models(
        &self,
        ctx: &ExecutionContext,
        instance_id: &str,
        cwd: &str,
    ) -> Result<Vec<Model>, Error> {
        let adapter = {
            let guard = self.adapters.lock().unwrap_or_else(|e| e.into_inner());
            guard.get(instance_id).cloned()
        };
        let Some(adapter) = adapter else {
            return Err(Error::sentinel(ErrorKind::UnsupportedCapability));
        };
        adapter.models(ctx, cwd)
    }

    pub fn close(&self) -> Result<(), Error> {
        let adapters: Vec<Arc<dyn Adapter>> = {
            let guard = self.adapters.lock().unwrap_or_else(|e| e.into_inner());
            guard.values().cloned().collect()
        };
        let mut first_err = None;
        for adapter in adapters {
            if let Err(err) = adapter.close() {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    fn start_turn(
        &self,
        ctx: &ExecutionContext,
        effect: &Effect,
        start: TurnStartEffect,
    ) -> Result<(), Error> {
        let adapter = {
            let guard = self.adapters.lock().unwrap_or_else(|e| e.into_inner());
            guard.get(&start.run.provider_instance_id).cloned()
        }
        .ok_or_else(|| {
            Error::provider(format!(
                "no CLI adapter registered for {}",
                start.run.provider_instance_id
            ))
        })?;

        let cwd = match start.thread.worktree_path {
            Some(ref p) if !p.is_empty() => p.clone(),
            _ => std::env::current_dir()
                .map_err(|e| Error::provider(e.to_string()))?
                .to_string_lossy()
                .into_owned(),
        };

        let session_id: ProviderSessionID =
            stable_id("provider-session:cli:", &start.provider_thread.id);
        let turn_id: ProviderTurnID = stable_id("provider-turn:cli:", &start.attempt.id);
        let model = start.run.model_selection.model.clone();
        let now = Utc::now();
        let session = ProviderSession {
            id: session_id.clone(),
            driver: adapter.driver(),
            provider_instance_id: start.run.provider_instance_id.clone(),
            status: SESSION_RUNNING.to_string(),
            cwd,
            model: Some(model),
            capabilities: adapter.capabilities(),
            created_at: now,
            updated_at: now,
            last_error: None,
        };
        let mut provider_thread = start.provider_thread.clone();
        provider_thread.driver = adapter.driver();
        provider_thread.provider_session_id = Some(session_id.clone());
        provider_thread.status = PROVIDER_THREAD_ACTIVE.to_string();
        provider_thread.updated_at = now;

        let root_node_id = start.run.root_node_id.clone().unwrap_or_default();
        let turn = ProviderTurn {
            id: turn_id.clone(),
            provider_thread_id: provider_thread.id.clone(),
            node_id: root_node_id,
            run_attempt_id: Some(start.attempt.id.clone()),
            native_turn_ref: None,
            ordinal: 1,
            status: PROVIDER_TURN_RUNNING.to_string(),
            started_at: None,
            completed_at: None,
            token_usage: None,
        };

        let request = RunRequest {
            start: start.clone(),
            session: Arc::new(Mutex::new(session.clone())),
            provider_thread: Arc::new(Mutex::new(provider_thread.clone())),
            provider_turn: Arc::new(Mutex::new(turn.clone())),
            tool_types: Arc::new(Mutex::new(HashMap::new())),
            state_mu: Arc::new(Mutex::new(())),
        };

        let run_ctx = ctx.child();
        {
            let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
            active.insert(
                turn_id.clone(),
                ActiveTurn {
                    ctx: run_ctx.clone(),
                    adapter: Arc::clone(&adapter),
                },
            );
        }

        let mut item_ordinals_init: HashMap<TurnItemID, i64> = HashMap::new();
        let mut next_item_ordinal_init = 1i64;
        if let Ok(snapshot) = self.orchestrator.snapshot(&start.thread.id) {
            for item in &snapshot.projection.turn_items {
                item_ordinals_init.insert(item.id.clone(), item.ordinal);
                if item.ordinal >= next_item_ordinal_init {
                    next_item_ordinal_init = item.ordinal + 1;
                }
            }
        }

        struct EmitState {
            sequence: usize,
            pending: Vec<Effect>,
            assistant_messages: HashMap<MessageID, bool>,
            item_ordinals: HashMap<TurnItemID, i64>,
            next_item_ordinal: i64,
        }

        let emit_state = Arc::new(Mutex::new(EmitState {
            sequence: 0,
            pending: Vec::new(),
            assistant_messages: HashMap::new(),
            item_ordinals: item_ordinals_init,
            next_item_ordinal: next_item_ordinal_init,
        }));

        let req_for_emit = request.clone();
        let driver = adapter.driver();
        let effect_id = effect.id.clone();
        let effect_command_id = effect.command_id.clone();
        let orch = Arc::clone(&self.orchestrator);
        let session_id_for_emit = session_id.clone();
        let start_for_emit = start.clone();
        let emit_state_clone = Arc::clone(&emit_state);

        let emit: EmitFn = Arc::new(
            move |_emit_ctx: &ExecutionContext,
                  mut event: ProviderEvent,
                  raw: &[u8]|
                  -> Result<(), Error> {
                let _state_guard = req_for_emit
                    .state_mu
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let sequence_at = {
                    let mut st = emit_state_clone.lock().unwrap_or_else(|e| e.into_inner());
                    st.sequence += 1;
                    if event.driver.is_empty() {
                        event.driver = driver.clone();
                    }
                    if let Some(ref pt_turn) = event.provider_turn {
                        req_for_emit.set_turn(pt_turn.clone());
                    }
                    if let Some(ref pt_thread) = event.provider_thread {
                        req_for_emit.set_provider_thread(pt_thread.clone());
                    }
                    if let Some(ref msg) = event.message {
                        if msg.role == MESSAGE_ASSISTANT {
                            st.assistant_messages.insert(msg.id.clone(), msg.streaming);
                        }
                    }
                    if let Some(mut item) = event.turn_item.take() {
                        let ordinal = if let Some(&ord) = st.item_ordinals.get(&item.id) {
                            ord
                        } else {
                            let ord = st.next_item_ordinal;
                            st.next_item_ordinal += 1;
                            st.item_ordinals.insert(item.id.clone(), ord);
                            ord
                        };
                        item.ordinal = ordinal;
                        event.turn_item = Some(item);
                    }
                    st.sequence
                };

                let mut hasher = Sha256::new();
                hasher.update(format!("{}:{}:", effect_id, sequence_at).as_bytes());
                hasher.update(raw);
                let raw_id = format!("raw-event:cli:{}", hex::encode(hasher.finalize()));

                let result = orch.ingest(IngestInput {
                    provider_session_id: session_id_for_emit.clone(),
                    provider_instance_id: start_for_emit.run.provider_instance_id.clone(),
                    command_id: Some(effect_command_id.clone()),
                    thread_id: start_for_emit.thread.id.clone(),
                    run_id: Some(start_for_emit.run.id.clone()),
                    node_id: start_for_emit.run.root_node_id.clone(),
                    raw_event_id: Some(raw_id),
                    event,
                    guard: Some(RunGuard {
                        run_id: start_for_emit.run.id.clone(),
                        active_attempt_id: start_for_emit.attempt.id.clone(),
                        expected_status: String::new(),
                    }),
                })?;

                let mut st = emit_state_clone.lock().unwrap_or_else(|e| e.into_inner());
                st.pending.extend(result.effects);
                Ok(())
            },
        );

        let cleanup = || {
            run_ctx.cancel();
            let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
            active.remove(&turn_id);
        };

        if let Err(err) = emit(
            &run_ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_SESSION_UPDATED.to_string(),
                provider_session: Some(session.clone()),
                ..Default::default()
            },
            br#"{"synthetic":"session.running"}"#,
        ) {
            cleanup();
            return Err(err);
        }
        if let Err(err) = emit(
            &run_ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_THREAD_UPDATED.to_string(),
                provider_thread: Some(provider_thread.clone()),
                ..Default::default()
            },
            br#"{"synthetic":"provider-thread.active"}"#,
        ) {
            cleanup();
            return Err(err);
        }
        if let Err(err) = emit(
            &run_ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                provider_turn: Some(turn.clone()),
                ..Default::default()
            },
            br#"{"synthetic":"provider-turn.running"}"#,
        ) {
            cleanup();
            return Err(err);
        }

        let run_err = adapter.run(&run_ctx, &request, &emit);
        let final_ctx = ExecutionContext::background();

        let mut message_ids: Vec<(MessageID, bool)> = {
            let st = emit_state.lock().unwrap_or_else(|e| e.into_inner());
            st.assistant_messages
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect()
        };
        message_ids.sort_by(|a, b| a.0.cmp(&b.0));

        for (message_id, streaming) in message_ids {
            if !streaming {
                continue;
            }
            let snapshot = match self.orchestrator.snapshot(&start.thread.id) {
                Ok(s) => s,
                Err(err) => {
                    cleanup();
                    return Err(err);
                }
            };
            for mut message in snapshot.projection.messages.clone() {
                if message.id != message_id {
                    continue;
                }
                message.streaming = false;
                message.updated_at = Utc::now();
                if let Err(err) = emit(
                    &final_ctx,
                    ProviderEvent {
                        event_type: PROVIDER_EVENT_MESSAGE_UPDATED.to_string(),
                        message: Some(message.clone()),
                        ..Default::default()
                    },
                    br#"{"synthetic":"message.completed"}"#,
                ) {
                    cleanup();
                    return Err(err);
                }
                if let Some(ref node_id) = message.node_id {
                    if let Some(mut node) = snapshot.projection.node(node_id) {
                        node.status = NODE_COMPLETED.to_string();
                        node.completed_at = Some(Utc::now());
                        if let Err(err) = emit(
                            &final_ctx,
                            ProviderEvent {
                                event_type: PROVIDER_EVENT_NODE_UPDATED.to_string(),
                                node: Some(node),
                                ..Default::default()
                            },
                            br#"{"synthetic":"node.completed"}"#,
                        ) {
                            cleanup();
                            return Err(err);
                        }
                    }
                }
                for mut item in snapshot.projection.turn_items.clone() {
                    if item.message_id != message_id {
                        continue;
                    }
                    item.status = TURN_ITEM_COMPLETED.to_string();
                    let now_item = Utc::now();
                    item.completed_at = Some(now_item);
                    item.updated_at = now_item;
                    if let Err(err) = emit(
                        &final_ctx,
                        ProviderEvent {
                            event_type: PROVIDER_EVENT_TURN_ITEM_UPDATED.to_string(),
                            turn_item: Some(item),
                            ..Default::default()
                        },
                        br#"{"synthetic":"turn-item.completed"}"#,
                    ) {
                        cleanup();
                        return Err(err);
                    }
                }
            }
        }

        let mut status: ProviderTurnStatus = PROVIDER_TURN_COMPLETED.to_string();
        let native_status = request.current_turn().status;
        if native_status == PROVIDER_TURN_INTERRUPTED || native_status == PROVIDER_TURN_CANCELLED {
            status = native_status;
        }
        if let Err(ref r_err) = run_err {
            status = PROVIDER_TURN_FAILED.to_string();
            if run_ctx.is_cancelled() {
                status = PROVIDER_TURN_INTERRUPTED.to_string();
            }
            let failure = failure_from_error(r_err);
            let mut data = BTreeMap::new();
            data.insert(
                "failure".to_string(),
                serde_json::to_value(&failure).unwrap_or_default(),
            );
            let cur_pt = request.current_provider_thread();
            let item = TurnItem {
                id: format!("turn-item:cli-failure:{}", turn_id),
                thread_id: start.thread.id.clone(),
                run_id: Some(start.run.id.clone()),
                node_id: start.run.root_node_id.clone(),
                provider_thread_id: Some(cur_pt.id),
                provider_turn_id: Some(turn_id.clone()),
                ordinal: 1,
                status: TURN_ITEM_FAILED.to_string(),
                title: failure.message.clone(),
                item_type: "provider_failure".to_string(),
                data,
                updated_at: Utc::now(),
                ..Default::default()
            };
            if let Err(err) = emit(
                &final_ctx,
                ProviderEvent {
                    event_type: PROVIDER_EVENT_TURN_ITEM_UPDATED.to_string(),
                    turn_item: Some(item),
                    ..Default::default()
                },
                br#"{"synthetic":"provider-turn.failure"}"#,
            ) {
                cleanup();
                return Err(err);
            }
        }

        let mut session_final = request.current_session();
        session_final.status = SESSION_STOPPED.to_string();
        session_final.updated_at = Utc::now();
        if let Err(err) = emit(
            &final_ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_SESSION_UPDATED.to_string(),
                provider_session: Some(session_final),
                ..Default::default()
            },
            br#"{"synthetic":"session.stopped"}"#,
        ) {
            cleanup();
            return Err(err);
        }

        let mut turn_final = request.current_turn();
        turn_final.status = status;
        turn_final.completed_at = Some(Utc::now());
        if let Err(err) = emit(
            &final_ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                provider_turn: Some(turn_final),
                ..Default::default()
            },
            br#"{"synthetic":"provider-turn.terminal"}"#,
        ) {
            cleanup();
            return Err(err);
        }

        cleanup();

        let pending_effects = {
            let mut st = emit_state.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut st.pending)
        };
        for next in pending_effects {
            self.execute(&final_ctx, &next)?;
        }

        run_err
    }
}

impl EffectExecutor for Executor {
    fn execute(&self, ctx: &ExecutionContext, effect: &Effect) -> Result<(), Error> {
        match effect.kind.as_str() {
            EFFECT_PROVIDER_TURN_START => {
                let EffectPayload::TurnStart(ref start) = effect.payload else {
                    return Err(Error::provider(
                        "provider.turn.start payload is not TurnStartEffect",
                    ));
                };
                self.start_turn(ctx, effect, (**start).clone())
            }
            EFFECT_PROVIDER_TURN_INTERRUPT => {
                let EffectPayload::TurnInterrupt(ref interrupt) = effect.payload else {
                    return Err(Error::provider(
                        "provider.turn.interrupt payload is not TurnInterruptEffect",
                    ));
                };
                let active = {
                    let guard = self.active.lock().unwrap_or_else(|e| e.into_inner());
                    guard.get(&interrupt.provider_turn_id).cloned()
                };
                let Some(active) = active else {
                    return Err(Error::with_detail(
                        ErrorKind::UnsupportedCapability,
                        format!("provider turn {} is not active", interrupt.provider_turn_id),
                    ));
                };
                match active.adapter.interrupt(ctx, interrupt.clone()) {
                    Ok(()) => Ok(()),
                    Err(err) if err.is(ErrorKind::UnsupportedCapability) => {
                        active.ctx.cancel();
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }
            EFFECT_PROVIDER_RUNTIME_REQUEST_RESPOND => {
                let EffectPayload::RuntimeRequestRespond(ref response) = effect.payload else {
                    return Err(Error::provider(
                        "provider.runtime-request.respond payload is not RuntimeRequestRespondEffect",
                    ));
                };
                let Some(ref turn_id) = response.request.provider_turn_id else {
                    return Err(Error::with_detail(
                        ErrorKind::RequestNotRespondable,
                        "runtime request has no provider turn",
                    ));
                };
                let active = {
                    let guard = self.active.lock().unwrap_or_else(|e| e.into_inner());
                    guard.get(turn_id).cloned()
                };
                let Some(active) = active else {
                    return Err(Error::with_detail(
                        ErrorKind::RequestNotRespondable,
                        "provider turn is not active",
                    ));
                };
                active
                    .adapter
                    .respond_runtime_request(ctx, response.clone())
            }
            other => Err(Error::with_detail(
                ErrorKind::UnsupportedCapability,
                format!("effect {} is not supported by the CLI executor", other),
            )),
        }
    }
}

fn failure_from_error(err: &Error) -> ProviderFailure {
    if let Some(turn_err) = err.as_turn_error() {
        return ProviderFailure {
            class: turn_err.class.clone(),
            message: turn_err.message.clone(),
            code: turn_err.code.clone(),
            reset_at: turn_err.reset_at.clone(),
        };
    }
    ProviderFailure {
        class: "unknown".to_string(),
        message: err.to_string(),
        code: String::new(),
        reset_at: String::new(),
    }
}

pub fn stable_id(prefix: &str, scope: &str) -> String {
    let hash = Sha256::digest(scope.as_bytes());
    format!("{}{}", prefix, hex::encode(&hash[..16]))
}

pub fn emit_assistant(
    ctx: &ExecutionContext,
    request: &RunRequest,
    emit: &EmitFn,
    native_item_id: &str,
    text: &str,
    streaming: bool,
    raw: &[u8],
) -> Result<(), Error> {
    let now = Utc::now();
    let provider_thread = request.current_provider_thread();
    let provider_turn = request.current_turn();
    let driver = provider_thread.driver.clone();
    let node_id = derive_node_from_provider_item_id(&driver, native_item_id);
    let message_id = derive_message_from_provider_item_id(&driver, native_item_id);
    let native_ref = ProviderRef {
        driver: driver.clone(),
        native_id: Some(native_item_id.to_string()),
        strength: NATIVE_REF_STRONG.to_string(),
        ..Default::default()
    };
    let node_status = if streaming {
        NODE_RUNNING
    } else {
        NODE_COMPLETED
    };
    let node = ExecutionNode {
        id: node_id.clone(),
        thread_id: request.start.thread.id.clone(),
        run_id: Some(request.start.run.id.clone()),
        parent_node_id: request.start.run.root_node_id.clone(),
        root_node_id: request
            .start
            .run
            .root_node_id
            .clone()
            .unwrap_or_default(),
        kind: NODE_ASSISTANT_MESSAGE.to_string(),
        status: node_status.to_string(),
        counts_for_run: false,
        provider_thread_id: Some(provider_thread.id.clone()),
        provider_turn_id: Some(provider_turn.id.clone()),
        native_item_ref: Some(native_ref.clone()),
        runtime_request_id: None,
        checkpoint_scope_id: None,
        started_at: Some(now),
        completed_at: if streaming { None } else { Some(now) },
    };
    emit(
        ctx,
        ProviderEvent {
            event_type: PROVIDER_EVENT_NODE_UPDATED.to_string(),
            node: Some(node),
            ..Default::default()
        },
        raw,
    )?;

    let message = ConversationMessage {
        created_by: ACTOR_AGENT.to_string(),
        creation_source: CREATION_PROVIDER.to_string(),
        id: message_id.clone(),
        thread_id: request.start.thread.id.clone(),
        run_id: Some(request.start.run.id.clone()),
        node_id: Some(node_id.clone()),
        role: MESSAGE_ASSISTANT.to_string(),
        text: text.to_string(),
        attachments: Vec::new(),
        streaming,
        created_at: now,
        updated_at: now,
    };
    emit(
        ctx,
        ProviderEvent {
            event_type: PROVIDER_EVENT_MESSAGE_UPDATED.to_string(),
            message: Some(message.clone()),
            ..Default::default()
        },
        raw,
    )?;

    let status = if streaming {
        TURN_ITEM_RUNNING
    } else {
        TURN_ITEM_COMPLETED
    };
    let item = TurnItem {
        created_by: ACTOR_AGENT.to_string(),
        creation_source: CREATION_PROVIDER.to_string(),
        id: derive_turn_item_from_provider_item_id(&driver, native_item_id),
        thread_id: message.thread_id,
        run_id: message.run_id,
        node_id: Some(node_id),
        provider_thread_id: Some(provider_thread.id),
        provider_turn_id: Some(provider_turn.id),
        native_item_ref: Some(native_ref),
        ordinal: 1,
        status: status.to_string(),
        item_type: "assistant_message".to_string(),
        message_id,
        text: text.to_string(),
        completed_at: if streaming { None } else { Some(now) },
        updated_at: now,
        ..Default::default()
    };
    emit(
        ctx,
        ProviderEvent {
            event_type: PROVIDER_EVENT_TURN_ITEM_UPDATED.to_string(),
            turn_item: Some(item),
            ..Default::default()
        },
        raw,
    )
}

pub fn prompt_text(start: &TurnStartEffect) -> String {
    match start.context_handoff {
        None => start.message.text.clone(),
        Some(ref handoff) => format!(
            "<context_handoff id=\"{}\" strategy=\"{}\">\n{}\n</context_handoff>\n\n{}",
            handoff.id, handoff.strategy, handoff.summary_text, start.message.text
        ),
    }
}
