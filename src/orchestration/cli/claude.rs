use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use chrono::{SecondsFormat, TimeZone, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::orchestration::command::{RuntimeRequestRespondEffect, TurnInterruptEffect};
use crate::orchestration::errors::{Error, ErrorKind};
use crate::orchestration::ids::{new_uuid_v4, ProviderThreadID};
use crate::orchestration::outbox::ExecutionContext;
use crate::orchestration::provider::{
    ProviderEvent, PROVIDER_EVENT_THREAD_UPDATED, PROVIDER_EVENT_TURN_UPDATED,
};
use crate::orchestration::types::{
    IdentityCapabilities, ProviderCapabilities, ProviderDriverKind, ProviderRef, RuntimeMode,
    StreamingCapabilities, ThreadCapabilities, TokenUsage, TurnCapabilities, NATIVE_REF_NONE,
    NATIVE_REF_STRONG, PROVIDER_TURN_INTERRUPTED, REQUEST_COMMAND, REQUEST_FILE_CHANGE,
    REQUEST_FILE_READ, REQUEST_PERMISSION, RUNTIME_AUTO, RUNTIME_AUTO_ACCEPT_EDITS,
    RUNTIME_FULL_ACCESS, TURN_ITEM_COMPLETED, TURN_ITEM_FAILED, TURN_ITEM_RUNNING,
};

use super::adapter::{emit_assistant, prompt_text, Adapter, EmitFn, RunRequest};
use super::models::{Model, ModelOption};
use super::rpc::{from_slice_lenient, normalize_rpc_id, RpcClient, RpcClientOptions, RpcRequest};
use super::tools::{emit_tool_call, RequestBroker, ToolCall};

struct ClaudeInvocation {
    request: RunRequest,
    emit: EmitFn,
    done_tx: Mutex<Option<mpsc::Sender<Result<(), Error>>>>,
}

impl ClaudeInvocation {
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

pub(crate) struct ClaudeSession {
    client: Arc<RpcClient>,
    native_id: String,
    turn_lock: Mutex<()>,
    current: Mutex<Option<Arc<ClaudeInvocation>>>,
    model: Mutex<String>,
    broker: RequestBroker,
    permission_mode: Mutex<String>,
    effort: Mutex<String>,
    fast_mode: Mutex<bool>,
}

#[derive(Default)]
pub struct ClaudeAdapter {
    pub binary: String,
    pub(crate) sessions: Mutex<HashMap<ProviderThreadID, Arc<ClaudeSession>>>,
}

impl ClaudeAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    fn session_for(&self, request: &RunRequest) -> Result<Arc<ClaudeSession>, Error> {
        let pt_id = request.current_provider_thread().id;
        let mut guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = guard.get(&pt_id) {
            return Ok(Arc::clone(existing));
        }
        let binary = if self.binary.is_empty() {
            "claude".to_string()
        } else {
            self.binary.clone()
        };
        let permission_mode = claude_permission_mode(&request.start.thread.runtime_mode);
        let mut args = vec![
            "--print".to_string(),
            "--input-format".to_string(),
            "stream-json".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--verbose".to_string(),
            "--safe-mode".to_string(),
            "--restricted".to_string(),
            "--permission-mode".to_string(),
            permission_mode.to_string(),
            "--permission-prompts".to_string(),
            "host".to_string(),
            "--strict-mcp-config".to_string(),
        ];
        let mut initial_effort = String::new();
        if let Some(Value::String(effort)) = request.start.run.model_selection.options.get("effort")
        {
            if !effort.is_empty() {
                args.push("--effort".to_string());
                args.push(effort.clone());
                initial_effort = effort.clone();
            }
        }
        let initial_fast = matches!(
            request.start.run.model_selection.options.get("fastMode"),
            Some(Value::Bool(true))
        );
        if initial_fast {
            args.push("--settings".to_string());
            args.push(r#"{"fastMode":true}"#.to_string());
        }

        let native_id = if let Some(ref existing_id) = request
            .current_provider_thread()
            .native_thread_ref
            .as_ref()
            .and_then(|r| r.native_id.clone())
        {
            args.push("--resume".to_string());
            args.push(existing_id.clone());
            existing_id.clone()
        } else {
            let uuid = new_uuid_v4().map_err(Error::provider)?;
            args.push("--session-id".to_string());
            args.push(uuid.clone());
            uuid
        };

        let cwd = request.current_session().cwd;
        let client = RpcClient::start(RpcClientOptions {
            binary,
            args,
            dir: cwd,
            ..Default::default()
        })?;

        let session = Arc::new(ClaudeSession {
            client: Arc::clone(&client),
            native_id,
            turn_lock: Mutex::new(()),
            current: Mutex::new(None),
            model: Mutex::new(request.start.run.model_selection.model.clone()),
            broker: RequestBroker::new(),
            permission_mode: Mutex::new(permission_mode.to_string()),
            effort: Mutex::new(initial_effort),
            fast_mode: Mutex::new(initial_fast),
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

        guard.insert(pt_id, Arc::clone(&session));
        Ok(session)
    }

    fn handle_notification(session: &ClaudeSession, raw: &[u8]) {
        let invocation = {
            let guard = session.current.lock().unwrap_or_else(|e| e.into_inner());
            guard.clone()
        };
        let Some(invocation) = invocation else {
            return;
        };
        let Ok(frame) = from_slice_lenient::<ClaudeFrame>(raw) else {
            return;
        };
        if frame.frame_type == "result"
            && (frame.stop_reason == "interrupted" || frame.terminal_reason == "interrupted")
        {
            invocation
                .request
                .set_turn_status(PROVIDER_TURN_INTERRUPTED);
        }
        let adapter = ClaudeAdapter::default();
        if let Err(err) = adapter.handle_frame(
            &ExecutionContext::background(),
            &invocation.request,
            raw,
            &invocation.emit,
        ) {
            invocation.finish(Err(err));
            return;
        }
        if frame.frame_type == "result" {
            invocation.finish(Ok(()));
        }
    }

    fn handle_native_request(session: &ClaudeSession, request: RpcRequest) -> Result<Value, Error> {
        #[derive(Deserialize, Default)]
        struct ControlFrame {
            #[serde(default)]
            request: ControlInner,
        }
        #[derive(Deserialize, Default)]
        struct ControlInner {
            #[serde(default)]
            subtype: String,
            #[serde(default)]
            tool_name: String,
            #[serde(default)]
            input: Value,
        }
        let frame: ControlFrame = from_slice_lenient(&request.raw)
            .map_err(|_| Error::provider("unsupported Claude control request"))?;
        if frame.request.subtype != "can_use_tool" {
            return Err(Error::provider("unsupported Claude control request"));
        }
        let kind = match frame.request.tool_name.as_str() {
            "Bash" | "PowerShell" => REQUEST_COMMAND,
            "Read" => REQUEST_FILE_READ,
            "Edit" | "Write" | "NotebookEdit" => REQUEST_FILE_CHANGE,
            _ => REQUEST_PERMISSION,
        };
        let invocation = {
            let guard = session.current.lock().unwrap_or_else(|e| e.into_inner());
            guard.clone()
        }
        .ok_or_else(|| Error::provider("Claude permission request has no active turn"))?;

        let response = session.broker.ask(
            &ExecutionContext::background(),
            &invocation.request,
            &invocation.emit,
            &normalize_rpc_id(&request.request_id),
            kind.to_string(),
            &format!("Allow {}?", frame.request.tool_name),
            frame.request.input,
        )?;
        if let Some(ref dec) = response.decision {
            if dec == "accept" || dec == "acceptForSession" || dec == "acceptAlways" {
                return Ok(json!({ "behavior": "allow" }));
            }
        }
        Ok(json!({
            "behavior": "deny",
            "message": "Denied in g0code",
        }))
    }
}

impl Adapter for ClaudeAdapter {
    fn driver(&self) -> ProviderDriverKind {
        "claudeAgent".to_string()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            threads: ThreadCapabilities {
                can_read_thread_snapshot: true,
                exposes_native_thread_id: true,
                ..Default::default()
            },
            turns: TurnCapabilities {
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
                native_turn_ids: NATIVE_REF_NONE.to_string(),
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
        let session = self.session_for(request)?;
        let _turn_guard = session.turn_lock.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(Value::String(effort)) = request.start.run.model_selection.options.get("effort")
        {
            if !effort.is_empty() {
                let mut cur_effort = session.effort.lock().unwrap_or_else(|e| e.into_inner());
                if *cur_effort != *effort {
                    session.client.control_request(
                        ctx,
                        json!({
                            "subtype": "set_effort",
                            "effort": effort,
                        }),
                    )?;
                    *cur_effort = effort.clone();
                }
            }
        }
        if let Some(Value::Bool(fast_mode)) =
            request.start.run.model_selection.options.get("fastMode")
        {
            let mut cur_fast = session.fast_mode.lock().unwrap_or_else(|e| e.into_inner());
            if *cur_fast != *fast_mode {
                session.client.control_request(
                    ctx,
                    json!({
                        "subtype": "apply_flag_settings",
                        "settings": { "fastMode": fast_mode },
                    }),
                )?;
                *cur_fast = *fast_mode;
            }
        }
        let permission_mode = claude_permission_mode(&request.start.thread.runtime_mode);
        {
            let mut cur_mode = session
                .permission_mode
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if *cur_mode != permission_mode {
                session.client.control_request(
                    ctx,
                    json!({
                        "subtype": "set_permission_mode",
                        "mode": permission_mode,
                    }),
                )?;
                *cur_mode = permission_mode.to_string();
            }
        }
        let model = &request.start.run.model_selection.model;
        if !model.is_empty() && model != "default" {
            let mut cur_model = session.model.lock().unwrap_or_else(|e| e.into_inner());
            if *cur_model != *model {
                session.client.control_request(
                    ctx,
                    json!({
                        "subtype": "set_model",
                        "model": model,
                    }),
                )?;
                *cur_model = model.clone();
            }
        }

        let (done_tx, done_rx) = mpsc::channel();
        let invocation = Arc::new(ClaudeInvocation {
            request: request.clone(),
            emit: Arc::clone(emit),
            done_tx: Mutex::new(Some(done_tx)),
        });
        *session.current.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&invocation));

        let run_res = (|| -> Result<(), Error> {
            session.client.send_record(&json!({
                "type": "user",
                "message": {
                    "role": "user",
                    "content": prompt_text(&request.start),
                },
                "parent_tool_use_id": Value::Null,
                "session_id": session.native_id,
            }))?;

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
                                "Claude query closed during turn: {}",
                                session.client.closed_error()
                            )));
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(Error::provider("Claude turn channel closed"));
                    }
                }
            }
        })();

        *session.current.lock().unwrap_or_else(|e| e.into_inner()) = None;
        run_res
    }

    fn handle_frame(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        raw: &[u8],
        emit: &EmitFn,
    ) -> Result<(), Error> {
        let frame: ClaudeFrame =
            from_slice_lenient(raw).map_err(|e| Error::provider(e.to_string()))?;
        if frame.frame_type == "query.interrupt" {
            request.set_turn_status(PROVIDER_TURN_INTERRUPTED);
        }
        if !frame.session_id.is_empty() {
            let mut thread = request.current_provider_thread();
            thread.native_thread_ref = Some(ProviderRef {
                driver: self.driver(),
                native_id: Some(frame.session_id.clone()),
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

        if frame.frame_type == "assistant" && !frame.message.id.is_empty() {
            let mut text = String::new();
            for content in &frame.message.content {
                if content.content_type == "text" {
                    text.push_str(&content.text);
                }
                if content.content_type == "tool_use" && !content.id.is_empty() {
                    let item_type = if content.name == "Bash" || content.name == "PowerShell" {
                        "command_execution"
                    } else if content.name == "Edit" || content.name == "Write" {
                        "file_change"
                    } else {
                        "dynamic_tool"
                    };
                    request.record_tool_type(&content.id, item_type);
                    emit_tool_call(
                        ctx,
                        request,
                        emit,
                        ToolCall {
                            id: content.id.clone(),
                            item_type: item_type.to_string(),
                            title: content.name.clone(),
                            status: TURN_ITEM_RUNNING.to_string(),
                            input: content.input.clone(),
                            ..Default::default()
                        },
                        raw,
                    )?;
                }
                if content.content_type == "tool_result" && !content.tool_use_id.is_empty() {
                    let status = if content.is_error {
                        TURN_ITEM_FAILED
                    } else {
                        TURN_ITEM_COMPLETED
                    };
                    let item_type = request.lookup_tool_type(&content.tool_use_id);
                    emit_tool_call(
                        ctx,
                        request,
                        emit,
                        ToolCall {
                            id: content.tool_use_id.clone(),
                            item_type,
                            status: status.to_string(),
                            output: content.content.clone(),
                            ..Default::default()
                        },
                        raw,
                    )?;
                }
            }
            if !text.is_empty() {
                emit_assistant(ctx, request, emit, &frame.message.id, &text, false, raw)?;
            }
        }

        if frame.usage.output_tokens != 0 || frame.usage.input_tokens != 0 {
            let mut turn = request.current_turn();
            let mut max_tokens = None;
            for u in frame.model_usage.values() {
                if u.context_window > 0 {
                    max_tokens = Some(u.context_window);
                    break;
                }
            }
            turn.token_usage = Some(TokenUsage {
                used_tokens: frame.usage.input_tokens + frame.usage.output_tokens,
                max_tokens,
                input_tokens: frame.usage.input_tokens,
                cached_input_tokens: frame.usage.cached_input_tokens,
                output_tokens: frame.usage.output_tokens,
                reasoning_output_tokens: 0,
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

        if frame.frame_type == "assistant" && frame.is_api_error_message {
            let class = if frame.api_error == "usage_limit_reached" || frame.api_error_status == 429
            {
                "usage_limit"
            } else {
                "unknown"
            };
            let reset_at = if frame.api_error_params.rate_limit_info.resets_at > 0 {
                Utc.timestamp_opt(frame.api_error_params.rate_limit_info.resets_at, 0)
                    .single()
                    .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Secs, true))
                    .unwrap_or_default()
            } else {
                String::new()
            };
            let mut message = frame.result.clone();
            if message.is_empty() {
                for content in &frame.message.content {
                    message.push_str(&content.text);
                }
            }
            return Err(Error::turn(class, message, frame.api_error, reset_at));
        }

        if frame.frame_type == "result" && frame.is_error {
            let class = if frame.api_error == "usage_limit_reached" || frame.api_error_status == 429
            {
                "usage_limit"
            } else {
                "unknown"
            };
            let reset_at = if frame.rate_limit_info.resets_at > 0 {
                Utc.timestamp_opt(frame.rate_limit_info.resets_at, 0)
                    .single()
                    .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Secs, true))
                    .unwrap_or_default()
            } else {
                String::new()
            };
            let message = if frame.result.is_empty() {
                format!("Claude turn failed ({}).", frame.api_error)
            } else {
                frame.result
            };
            return Err(Error::turn(class, message, frame.api_error, reset_at));
        }
        Ok(())
    }

    fn interrupt(&self, ctx: &ExecutionContext, effect: TurnInterruptEffect) -> Result<(), Error> {
        let sessions: Vec<Arc<ClaudeSession>> = {
            let guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            guard.values().cloned().collect()
        };
        for session in sessions {
            let current = {
                let guard = session.current.lock().unwrap_or_else(|e| e.into_inner());
                guard.clone()
            };
            if let Some(cur) = current {
                if cur.request.current_turn().id == effect.provider_turn_id {
                    return session
                        .client
                        .control_request(ctx, json!({ "subtype": "interrupt" }));
                }
            }
        }
        Err(Error::with_detail(
            ErrorKind::UnsupportedCapability,
            format!("Claude turn {} is not active", effect.provider_turn_id),
        ))
    }

    fn respond_runtime_request(
        &self,
        _ctx: &ExecutionContext,
        response: RuntimeRequestRespondEffect,
    ) -> Result<(), Error> {
        let sessions: Vec<Arc<ClaudeSession>> = {
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
            "Claude runtime request is not live",
        ))
    }

    fn close(&self) -> Result<(), Error> {
        let sessions: Vec<Arc<ClaudeSession>> = {
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

    fn models(&self, _ctx: &ExecutionContext, _cwd: &str) -> Result<Vec<Model>, Error> {
        let options = vec![
            ModelOption {
                id: "effort".to_string(),
                label: "Thinking".to_string(),
                option_type: "select".to_string(),
                values: vec![
                    "low".to_string(),
                    "medium".to_string(),
                    "high".to_string(),
                    "xhigh".to_string(),
                    "max".to_string(),
                ],
                default: Some(Value::String("high".to_string())),
            },
            ModelOption {
                id: "fastMode".to_string(),
                label: "Fast mode".to_string(),
                option_type: "boolean".to_string(),
                values: Vec::new(),
                default: Some(Value::Bool(false)),
            },
        ];
        Ok(vec![
            Model {
                id: "default".to_string(),
                label: "Default".to_string(),
                options: options.clone(),
            },
            Model {
                id: "claude-opus-5-5".to_string(),
                label: "Claude Opus 5.5".to_string(),
                options: options.clone(),
            },
            Model {
                id: "claude-sonnet-5-5".to_string(),
                label: "Claude Sonnet 5.5".to_string(),
                options: options.clone(),
            },
            Model {
                id: "claude-haiku-5-5".to_string(),
                label: "Claude Haiku 5.5".to_string(),
                options,
            },
        ])
    }
}

fn claude_permission_mode(mode: &RuntimeMode) -> &'static str {
    match mode.as_str() {
        RUNTIME_AUTO_ACCEPT_EDITS => "acceptEdits",
        RUNTIME_AUTO | RUNTIME_FULL_ACCESS => "auto",
        _ => "manual",
    }
}

#[derive(Deserialize, Default)]
struct ClaudeFrame {
    #[serde(rename = "type", default)]
    frame_type: String,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    is_error: bool,
    #[serde(default)]
    is_api_error_message: bool,
    #[serde(default)]
    result: String,
    #[serde(default)]
    stop_reason: String,
    #[serde(default)]
    terminal_reason: String,
    #[serde(default)]
    api_error: String,
    #[serde(default)]
    api_error_status: i64,
    #[serde(default)]
    message: ClaudeMessage,
    #[serde(default)]
    rate_limit_info: ClaudeRateLimitInfo,
    #[serde(default)]
    api_error_params: ClaudeApiErrorParams,
    #[serde(default)]
    usage: ClaudeUsage,
    #[serde(rename = "modelUsage", default)]
    model_usage: HashMap<String, ClaudeModelUsage>,
}

#[derive(Deserialize, Default)]
struct ClaudeMessage {
    #[serde(default)]
    id: String,
    #[serde(default)]
    content: Vec<ClaudeContent>,
}

#[derive(Deserialize, Default)]
struct ClaudeContent {
    #[serde(rename = "type", default)]
    content_type: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    tool_use_id: String,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    content: Value,
    #[serde(default)]
    is_error: bool,
}

#[derive(Deserialize, Default)]
struct ClaudeRateLimitInfo {
    #[serde(rename = "resetsAt", default)]
    resets_at: i64,
}

#[derive(Deserialize, Default)]
struct ClaudeApiErrorParams {
    #[serde(default)]
    rate_limit_info: ClaudeRateLimitInfo,
}

#[derive(Deserialize, Default)]
struct ClaudeUsage {
    #[serde(default)]
    input_tokens: i64,
    #[serde(default)]
    output_tokens: i64,
    #[serde(rename = "cache_read_input_tokens", default)]
    cached_input_tokens: i64,
}

#[derive(Deserialize, Default)]
struct ClaudeModelUsage {
    #[serde(rename = "contextWindow", default)]
    context_window: i64,
}
