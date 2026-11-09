use std::collections::{HashMap, HashSet};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::orchestration::command::{RuntimeRequestRespondEffect, TurnInterruptEffect};
use crate::orchestration::errors::{Error, ErrorKind};
use crate::orchestration::ids::ProviderTurnID;
use crate::orchestration::outbox::ExecutionContext;
use crate::orchestration::provider::{
    ProviderEvent, PROVIDER_EVENT_THREAD_UPDATED, PROVIDER_EVENT_TURN_UPDATED,
};
use crate::orchestration::types::{
    IdentityCapabilities, ModelSelection, ProviderCapabilities, ProviderDriverKind, ProviderRef,
    StreamingCapabilities, ThreadCapabilities, TokenUsage, TurnCapabilities, INTERACTION_DEFAULT,
    NATIVE_REF_STRONG, PROVIDER_TURN_INTERRUPTED, REQUEST_COMMAND, REQUEST_FILE_CHANGE,
    RUNTIME_AUTO, RUNTIME_AUTO_ACCEPT_EDITS, RUNTIME_FULL_ACCESS, TURN_ITEM_COMPLETED,
    TURN_ITEM_FAILED, TURN_ITEM_RUNNING,
};

use super::adapter::{emit_assistant, prompt_text, Adapter, EmitFn, RunRequest};
use super::models::{Model, ModelOption};
use super::rpc::{
    from_slice_lenient, from_value_lenient, normalize_rpc_id, RpcClient, RpcClientOptions,
    RpcRequest,
};
use super::tools::{emit_tool_call, RequestBroker, ToolCall};

struct CodexInvocation {
    request: RunRequest,
    emit: EmitFn,
    native_thread_id: String,
    native_turn_id: Mutex<String>,
    done_tx: Mutex<Option<mpsc::Sender<Result<(), Error>>>>,
}

impl CodexInvocation {
    fn finish(&self, res: Result<(), Error>) {
        if let Some(tx) = self
            .done_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = tx.send(res);
        }
    }
}

pub(crate) struct CodexSession {
    client: Arc<RpcClient>,
    by_thread: Mutex<HashMap<String, Arc<CodexInvocation>>>,
    by_turn: Mutex<HashMap<ProviderTurnID, Arc<CodexInvocation>>>,
    known_threads: Mutex<HashSet<String>>,
    thread_models: Mutex<HashMap<String, String>>,
    broker: RequestBroker,
}

#[derive(Default)]
pub struct CodexAdapter {
    pub binary: String,
    pub args: Vec<String>,
    pub(crate) sessions: Mutex<HashMap<String, Arc<CodexSession>>>,
}

impl CodexAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    fn session_for(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        emit: &EmitFn,
    ) -> Result<(Arc<CodexSession>, String, String), Error> {
        let cwd = request.current_session().cwd;
        let session = {
            let mut guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(existing) = guard.get(&cwd) {
                Arc::clone(existing)
            } else {
                let binary = if self.binary.is_empty() {
                    "codex".to_string()
                } else {
                    self.binary.clone()
                };
                let mut args = vec!["app-server".to_string()];
                args.extend(self.args.clone());

                let client = RpcClient::start(RpcClientOptions {
                    binary,
                    args,
                    dir: cwd.clone(),
                    env: vec!["CODEX_INTERNAL_ORIGINATOR_OVERRIDE=T3 Code".to_string()],
                    id_prefix: String::new(),
                    on_notification: None,
                    on_response: None,
                    on_request: None,
                })?;

                let session = Arc::new(CodexSession {
                    client: Arc::clone(&client),
                    by_thread: Mutex::new(HashMap::new()),
                    by_turn: Mutex::new(HashMap::new()),
                    known_threads: Mutex::new(HashSet::new()),
                    thread_models: Mutex::new(HashMap::new()),
                    broker: RequestBroker::new(),
                });

                let session_for_notif = Arc::clone(&session);
                client.set_on_notification(Some(Arc::new(move |raw: &[u8]| {
                    Self::handle_notification(&session_for_notif, raw);
                })));

                let session_for_req = Arc::clone(&session);
                client.set_on_request(Some(Arc::new(
                    move |req: RpcRequest| -> Result<Value, Error> {
                        Self::handle_native_request(&session_for_req, req)
                    },
                )));

                #[derive(Deserialize, Default)]
                struct InitResp {
                    #[serde(rename = "userAgent", default)]
                    _user_agent: String,
                }
                if let Err(err) = client.call::<InitResp>(
                    ctx,
                    "initialize",
                    json!({
                        "clientInfo": {
                            "name": "T3 Code",
                            "title": "T3 Code",
                            "version": "0.1.7",
                        },
                        "capabilities": {
                            "experimentalApi": true,
                            "optOutNotificationMethods": ["turn/diff/updated"],
                            "extensions": {
                                "io.modelcontextprotocol/ui": {
                                    "mimeTypes": ["text/html;profile=mcp-app"]
                                }
                            }
                        }
                    }),
                ) {
                    client.close();
                    return Err(err);
                }
                if let Err(err) = client.notify("initialized", None) {
                    client.close();
                    return Err(err);
                }
                guard.insert(cwd.clone(), Arc::clone(&session));
                session
            }
        };

        let mut native_thread_id = request
            .current_provider_thread()
            .native_thread_ref
            .as_ref()
            .and_then(|r| r.native_id.clone())
            .unwrap_or_default();
        let mut model = request.start.run.model_selection.model.clone();
        let mut method = "thread/start";
        let mut params = serde_json::Map::new();
        params.insert("cwd".to_string(), Value::String(cwd));
        params.insert(
            "config".to_string(),
            json!({
                "tools.update_plan.enabled": true,
                "mcp_optional_startup_grace_ms": 10000,
            }),
        );
        if !model.is_empty() && model != "default" {
            params.insert("model".to_string(), Value::String(model.clone()));
        }
        if !native_thread_id.is_empty() {
            method = "thread/resume";
            params.insert(
                "threadId".to_string(),
                Value::String(native_thread_id.clone()),
            );
            params.insert("excludeTurns".to_string(), Value::Bool(true));
        }

        if !native_thread_id.is_empty()
            && session
                .known_threads
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&native_thread_id)
        {
            if model == "default" {
                if let Some(m) = session
                    .thread_models
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&native_thread_id)
                    .cloned()
                {
                    if !m.is_empty() {
                        model = m;
                    }
                }
            }
            return Ok((session, native_thread_id, model));
        }

        #[derive(Deserialize, Default)]
        struct ThreadResponse {
            #[serde(default)]
            thread: ThreadObj,
        }
        #[derive(Deserialize, Default)]
        struct ThreadObj {
            #[serde(default)]
            id: String,
            #[serde(default)]
            model: String,
        }

        let response: ThreadResponse =
            session.client.call(ctx, method, Value::Object(params))?;
        if response.thread.id.is_empty() {
            return Err(Error::provider(format!(
                "Codex {} returned no thread id",
                method
            )));
        }
        native_thread_id = response.thread.id;
        if model == "default" && !response.thread.model.is_empty() {
            model = response.thread.model.clone();
        }
        session
            .known_threads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(native_thread_id.clone());
        session
            .thread_models
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(native_thread_id.clone(), response.thread.model);

        let mut thread = request.current_provider_thread();
        thread.native_thread_ref = Some(ProviderRef {
            driver: self.driver(),
            native_id: Some(native_thread_id.clone()),
            strength: NATIVE_REF_STRONG.to_string(),
            ..Default::default()
        });
        thread.updated_at = Utc::now();
        let raw_msg = format!(r#"{{"method":"{}","kind":"response"}}"#, method);
        emit(
            ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_THREAD_UPDATED.to_string(),
                provider_thread: Some(thread),
                ..Default::default()
            },
            raw_msg.as_bytes(),
        )?;
        Ok((session, native_thread_id, model))
    }

    fn handle_notification(session: &CodexSession, raw: &[u8]) {
        let Ok(frame) = from_slice_lenient::<CodexFrame>(raw) else {
            return;
        };
        let mut native_thread_id = frame.params.thread_id.clone();
        if native_thread_id.is_empty() {
            native_thread_id = frame.params.thread.id.clone();
        }
        let invocation = {
            let guard = session.by_thread.lock().unwrap_or_else(|e| e.into_inner());
            guard.get(&native_thread_id).cloned()
        };
        let Some(invocation) = invocation else {
            return;
        };
        if !frame.params.turn.id.is_empty() {
            *invocation
                .native_turn_id
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = frame.params.turn.id.clone();
        }
        let adapter = CodexAdapter::default();
        if let Err(err) = adapter.handle_frame(
            &ExecutionContext::background(),
            &invocation.request,
            raw,
            &invocation.emit,
        ) {
            invocation.finish(Err(err));
            return;
        }
        if frame.method == "turn/completed"
            || frame.frame_type == "turn.completed"
            || frame.frame_type == "turn.failed"
        {
            invocation.finish(Ok(()));
        }
    }

    fn handle_native_request(session: &CodexSession, request: RpcRequest) -> Result<Value, Error> {
        #[derive(Deserialize, Default)]
        struct ApprovalPayload {
            #[serde(rename = "threadId", default)]
            thread_id: String,
            #[serde(rename = "itemId", default)]
            item_id: String,
            #[serde(default)]
            command: String,
            #[serde(default)]
            cwd: String,
            #[serde(default)]
            reason: String,
        }
        let payload: ApprovalPayload = from_value_lenient(request.params)
            .map_err(|e| Error::provider(e.to_string()))?;
        let invocation = {
            let guard = session.by_thread.lock().unwrap_or_else(|e| e.into_inner());
            guard.get(&payload.thread_id).cloned()
        }
        .ok_or_else(|| Error::provider("unknown Codex approval thread"))?;

        let kind = if request.method.contains("fileChange") {
            REQUEST_FILE_CHANGE
        } else {
            REQUEST_COMMAND
        };
        let response = session.broker.ask(
            &ExecutionContext::background(),
            &invocation.request,
            &invocation.emit,
            &normalize_rpc_id(&request.id),
            kind.to_string(),
            "Codex approval",
            json!({
                "itemId": payload.item_id,
                "command": payload.command,
                "cwd": payload.cwd,
                "reason": payload.reason,
            }),
        )?;
        let mut decision = "decline".to_string();
        if let Some(ref dec) = response.decision {
            match dec.as_str() {
                "accept" | "acceptForSession" | "acceptAlways" => decision = dec.clone(),
                "cancel" => decision = "cancel".to_string(),
                _ => {}
            }
        }
        Ok(json!({ "decision": decision }))
    }
}

impl Adapter for CodexAdapter {
    fn driver(&self) -> ProviderDriverKind {
        "codex".to_string()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            threads: ThreadCapabilities {
                can_fork_thread: true,
                can_fork_from_turn: true,
                exposes_native_thread_id: true,
                ..Default::default()
            },
            turns: TurnCapabilities {
                exposes_native_turn_id: true,
                emits_turn_started: true,
                emits_turn_completed: true,
                supports_interrupt: true,
                supports_queued_messages: true,
                terminal_status_quality: "strong".to_string(),
                ..Default::default()
            },
            streaming: StreamingCapabilities {
                streams_assistant_text: true,
                emits_message_completed: true,
                ..Default::default()
            },
            identity: IdentityCapabilities {
                native_thread_ids: NATIVE_REF_STRONG.to_string(),
                native_turn_ids: NATIVE_REF_STRONG.to_string(),
                native_item_ids: NATIVE_REF_STRONG.to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        emit: &EmitFn,
    ) -> Result<(), Error> {
        let (session, native_thread_id, model) = self.session_for(ctx, request, emit)?;
        let (done_tx, done_rx) = mpsc::channel();
        let turn_id = request.current_turn().id;
        let invocation = Arc::new(CodexInvocation {
            request: request.clone(),
            emit: Arc::clone(emit),
            native_thread_id: native_thread_id.clone(),
            native_turn_id: Mutex::new(String::new()),
            done_tx: Mutex::new(Some(done_tx)),
        });
        {
            session
                .by_thread
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(native_thread_id.clone(), Arc::clone(&invocation));
            session
                .by_turn
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(turn_id.clone(), Arc::clone(&invocation));
        }
        let cleanup = || {
            session
                .by_thread
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&native_thread_id);
            session
                .by_turn
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&turn_id);
        };

        let run_res = (|| -> Result<(), Error> {
            let params = codex_turn_start_params(request, &native_thread_id, &model);
            #[derive(Deserialize, Default)]
            struct TurnStartedResp {
                #[serde(default)]
                turn: TurnIdObj,
            }
            #[derive(Deserialize, Default)]
            struct TurnIdObj {
                #[serde(default)]
                id: String,
            }
            let started: TurnStartedResp = session.client.call(ctx, "turn/start", params)?;
            if !started.turn.id.is_empty() {
                *invocation
                    .native_turn_id
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = started.turn.id.clone();
                let mut turn = request.current_turn();
                turn.native_turn_ref = Some(ProviderRef {
                    driver: self.driver(),
                    native_id: Some(started.turn.id),
                    strength: NATIVE_REF_STRONG.to_string(),
                    ..Default::default()
                });
                emit(
                    ctx,
                    ProviderEvent {
                        event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                        provider_turn: Some(turn),
                        ..Default::default()
                    },
                    br#"{"method":"turn/start","kind":"response"}"#,
                )?;
            }

            loop {
                if ctx.is_cancelled() {
                    return Err(Error::provider("execution context cancelled"));
                }
                match done_rx.recv_timeout(Duration::from_millis(20)) {
                    Ok(res) => return res,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if session.client.is_closed() {
                            if let Ok(res) = done_rx.try_recv() {
                                return res;
                            }
                            return Err(Error::provider(format!(
                                "Codex app-server closed during turn: {}",
                                session.client.closed_error()
                            )));
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(Error::provider("Codex turn channel closed"));
                    }
                }
            }
        })();

        cleanup();
        run_res
    }

    fn handle_frame(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        raw: &[u8],
        emit: &EmitFn,
    ) -> Result<(), Error> {
        let frame: CodexFrame =
            from_slice_lenient(raw).map_err(|e| Error::provider(e.to_string()))?;
        let mut thread_native_id = frame.thread_id.clone();
        if !frame.params.thread.id.is_empty() {
            thread_native_id = frame.params.thread.id.clone();
        }
        if !frame.result.thread.id.is_empty() {
            thread_native_id = frame.result.thread.id.clone();
        }
        if !thread_native_id.is_empty() {
            let mut thread = request.current_provider_thread();
            thread.native_thread_ref = Some(ProviderRef {
                driver: self.driver(),
                native_id: Some(thread_native_id),
                strength: NATIVE_REF_STRONG.to_string(),
                ..Default::default()
            });
            thread.updated_at = Utc::now();
            emit(
                ctx,
                ProviderEvent {
                    event_type: PROVIDER_EVENT_THREAD_UPDATED.to_string(),
                    provider_thread: Some(thread),
                    ..Default::default()
                },
                raw,
            )?;
        }

        let mut turn_native_id = frame.params.turn_id.clone();
        if !frame.params.turn.id.is_empty() {
            turn_native_id = frame.params.turn.id.clone();
        }
        if !frame.result.turn.id.is_empty() {
            turn_native_id = frame.result.turn.id.clone();
        }
        if !turn_native_id.is_empty() {
            let mut turn = request.current_turn();
            turn.native_turn_ref = Some(ProviderRef {
                driver: self.driver(),
                native_id: Some(turn_native_id),
                strength: NATIVE_REF_STRONG.to_string(),
                ..Default::default()
            });
            emit(
                ctx,
                ProviderEvent {
                    event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                    provider_turn: Some(turn),
                    ..Default::default()
                },
                raw,
            )?;
        }

        let item = if !frame.params.item.id.is_empty() {
            &frame.params.item
        } else {
            &frame.item
        };
        if !item.id.is_empty() && (item.item_type == "commandExecution" || item.item_type == "fileChange")
        {
            let mut status = TURN_ITEM_RUNNING.to_string();
            if frame.method == "item/completed" {
                status = TURN_ITEM_COMPLETED.to_string();
            }
            if frame.method == "item/failed" {
                status = TURN_ITEM_FAILED.to_string();
            }
            if status == TURN_ITEM_COMPLETED {
                if let Some(code) = item.exit_code {
                    if code != 0 {
                        status = TURN_ITEM_FAILED.to_string();
                    }
                }
            }
            let item_type_name = if item.item_type == "fileChange" {
                "file_change"
            } else {
                "command_execution"
            };
            let output = if !item.output.is_empty() {
                item.output.clone()
            } else {
                item.aggregated_output.clone()
            };
            let mut file_name = String::new();
            let mut diff = String::new();
            for change in &item.changes {
                if file_name.is_empty() {
                    file_name = change.path.clone();
                }
                diff.push_str(&change.diff);
            }
            emit_tool_call(
                ctx,
                request,
                emit,
                ToolCall {
                    id: item.id.clone(),
                    item_type: item_type_name.to_string(),
                    title: item.command.clone(),
                    status,
                    input: json!({
                        "command": item.command,
                        "cwd": item.cwd,
                    }),
                    output: Value::String(output),
                    exit_code: item.exit_code,
                    file_name,
                    diff,
                },
                raw,
            )?;
        }

        if !item.id.is_empty()
            && (item.item_type == "agent_message" || item.item_type == "agentMessage")
            && !item.text.is_empty()
        {
            emit_assistant(ctx, request, emit, &item.id, &item.text, false, raw)?;
        }
        if frame.method == "item/agentMessage/delta" && !frame.params.item_id.is_empty() {
            return Ok(());
        }

        let mut usage = frame.usage;
        if frame.params.token_usage.last.total_tokens != 0 {
            usage.input_tokens = frame.params.token_usage.last.input_tokens;
            usage.cached_input_tokens = frame.params.token_usage.last.cached_input_tokens;
            usage.output_tokens = frame.params.token_usage.last.output_tokens;
            usage.reasoning_output_tokens = frame.params.token_usage.last.reasoning_output_tokens;
        }
        if usage.input_tokens != 0 || usage.output_tokens != 0 {
            let mut turn = request.current_turn();
            let max_tokens = if frame.params.token_usage.model_context_window != 0 {
                Some(frame.params.token_usage.model_context_window)
            } else {
                None
            };
            turn.token_usage = Some(TokenUsage {
                used_tokens: usage.input_tokens + usage.output_tokens,
                max_tokens,
                input_tokens: usage.input_tokens,
                cached_input_tokens: usage.cached_input_tokens,
                output_tokens: usage.output_tokens,
                reasoning_output_tokens: usage.reasoning_output_tokens,
                prompt_cache_ttl_ms: None,
                prompt_cache_expires_at: String::new(),
                updated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true),
            });
            emit(
                ctx,
                ProviderEvent {
                    event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                    provider_turn: Some(turn),
                    ..Default::default()
                },
                raw,
            )?;
        }

        if frame.frame_type == "turn.failed"
            || frame.frame_type == "error"
            || frame.params.turn.status == "failed"
        {
            let mut message = frame.error.message;
            if let Some(err_obj) = frame.params.turn.error {
                message = err_obj.message;
            }
            if message.trim().is_empty() {
                message = "Codex turn failed.".to_string();
            }
            return Err(Error::turn("unknown", message, "", ""));
        }

        if frame.frame_type == "turn.completed" || frame.method == "turn/completed" {
            if frame.params.turn.status == "interrupted" || frame.params.turn.status == "cancelled"
            {
                request.set_turn_status(PROVIDER_TURN_INTERRUPTED);
            }
            return Ok(());
        }
        Ok(())
    }

    fn interrupt(&self, ctx: &ExecutionContext, effect: TurnInterruptEffect) -> Result<(), Error> {
        let sessions: Vec<Arc<CodexSession>> = {
            let guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            guard.values().cloned().collect()
        };
        for session in sessions {
            let invocation = {
                let guard = session.by_turn.lock().unwrap_or_else(|e| e.into_inner());
                guard.get(&effect.provider_turn_id).cloned()
            };
            if let Some(inv) = invocation {
                let native_turn_id = inv
                    .native_turn_id
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                if !native_turn_id.is_empty() {
                    return session.client.call_unit(
                        ctx,
                        "turn/interrupt",
                        json!({
                            "threadId": inv.native_thread_id,
                            "turnId": native_turn_id,
                        }),
                    );
                }
            }
        }
        Err(Error::with_detail(
            ErrorKind::UnsupportedCapability,
            format!("Codex turn {} is not active", effect.provider_turn_id),
        ))
    }

    fn respond_runtime_request(
        &self,
        _ctx: &ExecutionContext,
        response: RuntimeRequestRespondEffect,
    ) -> Result<(), Error> {
        let sessions: Vec<Arc<CodexSession>> = {
            let guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            guard.values().cloned().collect()
        };
        for session in sessions {
            if let Some(ref native_ref) = response.request.native_request_ref {
                if let Some(ref native_id) = native_ref.native_id {
                    if session.broker.has_pending(native_id) {
                        return session.broker.respond(response);
                    }
                }
            }
        }
        Err(Error::with_detail(
            ErrorKind::RequestNotRespondable,
            "Codex runtime request is not live",
        ))
    }

    fn close(&self) -> Result<(), Error> {
        let sessions: Vec<Arc<CodexSession>> = {
            let mut guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            let list = guard.values().cloned().collect();
            guard.clear();
            list
        };
        for session in sessions {
            session.broker.cancel_all();
            session.client.close();
        }
        Ok(())
    }

    fn models(&self, ctx: &ExecutionContext, cwd: &str) -> Result<Vec<Model>, Error> {
        let binary = if self.binary.is_empty() {
            "codex".to_string()
        } else {
            self.binary.clone()
        };
        let mut args = vec!["app-server".to_string()];
        args.extend(self.args.clone());
        let client = RpcClient::start(RpcClientOptions {
            binary,
            args,
            dir: cwd.to_string(),
            env: vec!["CODEX_INTERNAL_ORIGINATOR_OVERRIDE=T3 Code".to_string()],
            ..Default::default()
        })?;
        let res = (|| -> Result<Vec<Model>, Error> {
            let _ = client.call::<Value>(
                ctx,
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "T3 Code",
                        "title": "T3 Code",
                        "version": "0.1.7",
                    },
                    "capabilities": {
                        "experimentalApi": true,
                    }
                }),
            )?;
            client.notify("initialized", None)?;
            let mut models = Vec::new();
            let mut cursor = String::new();
            loop {
                let mut params = serde_json::Map::new();
                if !cursor.is_empty() {
                    params.insert("cursor".to_string(), Value::String(cursor.clone()));
                }
                #[derive(Deserialize, Default)]
                struct ModelListResponse {
                    #[serde(default)]
                    data: Vec<CodexNativeModel>,
                    #[serde(rename = "nextCursor", default)]
                    next_cursor: Option<String>,
                }
                #[derive(Deserialize, Default)]
                struct CodexNativeModel {
                    #[serde(default)]
                    model: String,
                    #[serde(rename = "displayName", default)]
                    display_name: String,
                    #[serde(rename = "defaultReasoningEffort", default)]
                    default_reasoning_effort: String,
                    #[serde(rename = "supportedReasoningEfforts", default)]
                    supported_reasoning_efforts: Vec<EffortItem>,
                    #[serde(rename = "serviceTiers", default)]
                    service_tiers: Vec<TierItem>,
                    #[serde(rename = "additionalSpeedTiers", default)]
                    additional_speed_tiers: Vec<String>,
                }
                #[derive(Deserialize, Default)]
                struct EffortItem {
                    #[serde(rename = "reasoningEffort", default)]
                    reasoning_effort: String,
                }
                #[derive(Deserialize, Default)]
                struct TierItem {
                    #[serde(default)]
                    id: String,
                }
                let response: ModelListResponse =
                    client.call(ctx, "model/list", Value::Object(params))?;
                for native in response.data {
                    let label = if native.display_name.is_empty() {
                        native.model.clone()
                    } else {
                        native.display_name
                    };
                    let mut options = Vec::new();
                    let efforts: Vec<String> = native
                        .supported_reasoning_efforts
                        .into_iter()
                        .map(|e| e.reasoning_effort)
                        .collect();
                    if !efforts.is_empty() {
                        let default_effort = if native.default_reasoning_effort.is_empty() {
                            efforts[0].clone()
                        } else {
                            native.default_reasoning_effort
                        };
                        options.push(ModelOption {
                            id: "reasoningEffort".to_string(),
                            label: "Reasoning".to_string(),
                            option_type: "select".to_string(),
                            values: efforts,
                            default: Some(Value::String(default_effort)),
                        });
                    }
                    let mut tiers: Vec<String> =
                        native.service_tiers.into_iter().map(|t| t.id).collect();
                    tiers.extend(native.additional_speed_tiers);
                    if !tiers.is_empty() {
                        let mut values = vec!["standard".to_string()];
                        values.extend(tiers);
                        options.push(ModelOption {
                            id: "serviceTier".to_string(),
                            label: "Service Tier".to_string(),
                            option_type: "select".to_string(),
                            values,
                            default: Some(Value::String("standard".to_string())),
                        });
                    }
                    models.push(Model {
                        id: native.model,
                        label,
                        options,
                    });
                }
                cursor = response.next_cursor.unwrap_or_default();
                if cursor.is_empty() {
                    break;
                }
            }
            Ok(models)
        })();
        client.close();
        res
    }
}

fn codex_selection_settings(selection: &ModelSelection) -> Value {
    let mut settings = serde_json::Map::new();
    settings.insert("model".to_string(), Value::String(selection.model.clone()));
    if let Some(Value::String(effort)) = selection.options.get("reasoningEffort") {
        if !effort.is_empty() {
            settings.insert(
                "reasoning_effort".to_string(),
                Value::String(effort.clone()),
            );
        }
    }
    Value::Object(settings)
}

fn codex_turn_start_params(request: &RunRequest, native_thread_id: &str, model: &str) -> Value {
    let mut approval_policy = "untrusted";
    let mut approvals_reviewer = "user";
    let mut sandbox_type = "readOnly";
    match request.start.thread.runtime_mode.as_str() {
        RUNTIME_AUTO_ACCEPT_EDITS => {
            approval_policy = "on-request";
            sandbox_type = "workspaceWrite";
        }
        RUNTIME_AUTO => {
            approval_policy = "on-request";
            approvals_reviewer = "auto_review";
            sandbox_type = "workspaceWrite";
        }
        RUNTIME_FULL_ACCESS => {
            approval_policy = "never";
            sandbox_type = "dangerFullAccess";
        }
        _ => {}
    }
    let interaction_mode = if request.start.thread.interaction_mode.is_empty() {
        INTERACTION_DEFAULT
    } else {
        request.start.thread.interaction_mode.as_str()
    };
    let cwd = request.current_session().cwd;
    let mut params = serde_json::Map::new();
    params.insert(
        "threadId".to_string(),
        Value::String(native_thread_id.to_string()),
    );
    params.insert(
        "input".to_string(),
        json!([
            {
                "type": "text",
                "text": prompt_text(&request.start),
            }
        ]),
    );
    params.insert("cwd".to_string(), Value::String(cwd));
    params.insert("model".to_string(), Value::String(model.to_string()));
    params.insert("summary".to_string(), Value::String("detailed".to_string()));
    params.insert(
        "approvalPolicy".to_string(),
        Value::String(approval_policy.to_string()),
    );
    params.insert(
        "approvalsReviewer".to_string(),
        Value::String(approvals_reviewer.to_string()),
    );
    params.insert("sandboxPolicy".to_string(), json!({ "type": sandbox_type }));
    params.insert(
        "collaborationMode".to_string(),
        json!({
            "mode": interaction_mode,
            "settings": codex_selection_settings(&request.start.run.model_selection),
        }),
    );
    if let Some(Value::String(tier)) = request.start.run.model_selection.options.get("serviceTier")
    {
        if !tier.is_empty() {
            params.insert("serviceTier".to_string(), Value::String(tier.clone()));
        }
    }
    Value::Object(params)
}

#[derive(Deserialize, Default)]
struct CodexFrame {
    #[serde(rename = "type", default)]
    frame_type: String,
    #[serde(default)]
    thread_id: String,
    #[serde(default)]
    method: String,
    #[serde(default)]
    params: CodexParams,
    #[serde(default)]
    result: CodexResult,
    #[serde(default)]
    item: CodexItem,
    #[serde(default)]
    usage: CodexLegacyUsage,
    #[serde(default)]
    error: CodexErrorMsg,
}

#[derive(Deserialize, Default)]
struct CodexParams {
    #[serde(rename = "threadId", default)]
    thread_id: String,
    #[serde(rename = "turnId", default)]
    turn_id: String,
    #[serde(rename = "itemId", default)]
    item_id: String,
    #[serde(default)]
    thread: CodexIdObj,
    #[serde(default)]
    turn: CodexTurnObj,
    #[serde(default)]
    item: CodexItem,
    #[serde(rename = "tokenUsage", default)]
    token_usage: CodexTokenUsage,
}

#[derive(Deserialize, Default)]
struct CodexResult {
    #[serde(default)]
    thread: CodexIdObj,
    #[serde(default)]
    turn: CodexIdObj,
}

#[derive(Deserialize, Default)]
struct CodexIdObj {
    #[serde(default)]
    id: String,
}

#[derive(Deserialize, Default)]
struct CodexTurnObj {
    #[serde(default)]
    id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    error: Option<CodexErrorMsg>,
}

#[derive(Deserialize, Default)]
struct CodexErrorMsg {
    #[serde(default)]
    message: String,
}

#[derive(Deserialize, Default)]
struct CodexItem {
    #[serde(default)]
    id: String,
    #[serde(rename = "type", default)]
    item_type: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    cwd: String,
    #[serde(rename = "exitCode")]
    exit_code: Option<i64>,
    #[serde(default)]
    output: String,
    #[serde(rename = "aggregatedOutput", default)]
    aggregated_output: String,
    #[serde(default)]
    changes: Vec<CodexChange>,
}

#[derive(Deserialize, Default)]
struct CodexChange {
    #[serde(default)]
    path: String,
    #[serde(default)]
    diff: String,
}

#[derive(Deserialize, Default)]
struct CodexTokenUsage {
    #[serde(default)]
    last: CodexTokenCounts,
    #[serde(rename = "modelContextWindow", default)]
    model_context_window: i64,
}

#[derive(Deserialize, Default)]
struct CodexTokenCounts {
    #[serde(rename = "totalTokens", default)]
    total_tokens: i64,
    #[serde(rename = "inputTokens", default)]
    input_tokens: i64,
    #[serde(rename = "cachedInputTokens", default)]
    cached_input_tokens: i64,
    #[serde(rename = "outputTokens", default)]
    output_tokens: i64,
    #[serde(rename = "reasoningOutputTokens", default)]
    reasoning_output_tokens: i64,
}

#[derive(Deserialize, Default)]
struct CodexLegacyUsage {
    #[serde(default)]
    input_tokens: i64,
    #[serde(default)]
    cached_input_tokens: i64,
    #[serde(default)]
    output_tokens: i64,
    #[serde(default)]
    reasoning_output_tokens: i64,
}
