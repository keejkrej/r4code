use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::orchestration::cli::adapter::{emit_assistant, prompt_text, Adapter, EmitFn, RunRequest};
use crate::orchestration::cli::models::{Model, ModelOption};
use crate::orchestration::cli::rpc::{
    from_slice_lenient, from_value_lenient, RpcClient, RpcClientOptions, RpcRequest,
};
use crate::orchestration::cli::tools::{emit_tool_call, RequestBroker, ToolCall};
use crate::orchestration::command::{RuntimeRequestRespondEffect, TurnInterruptEffect};
use crate::orchestration::errors::{Error, ErrorKind};
use crate::orchestration::ids::{
    derive_turn_item_from_provider_item_id, ProviderThreadID, ProviderTurnID,
};
use crate::orchestration::outbox::ExecutionContext;
use crate::orchestration::provider::{
    ProviderEvent, PROVIDER_EVENT_THREAD_UPDATED, PROVIDER_EVENT_TURN_ITEM_UPDATED,
    PROVIDER_EVENT_TURN_UPDATED,
};
use crate::orchestration::types::{
    IdentityCapabilities, ModelSelection, ProviderCapabilities, ProviderDriverKind, ProviderRef,
    SessionCapabilities, StreamingCapabilities, ThreadCapabilities, TokenUsage, TurnCapabilities,
    TurnItem, ACTOR_AGENT, CREATION_PROVIDER, NATIVE_REF_NONE, NATIVE_REF_STRONG,
    PROVIDER_TURN_INTERRUPTED, REQUEST_COMMAND, REQUEST_FILE_CHANGE, REQUEST_FILE_READ,
    REQUEST_PERMISSION, TURN_ITEM_RUNNING,
};

pub struct ACPAdapter {
    pub binary: String,
    pub args: Vec<String>,
    pub driver_kind: ProviderDriverKind,
    pub load_method: String,
    pub mode: String,
    pub(crate) states: Arc<Mutex<HashMap<ProviderTurnID, Arc<Mutex<ACPTurnState>>>>>,
    pub(crate) sessions: Arc<Mutex<HashMap<ProviderThreadID, Arc<ACPSession>>>>,
}

impl Default for ACPAdapter {
    fn default() -> Self {
        Self::new("acp")
    }
}

#[derive(Default)]
pub(crate) struct ACPTurnState {
    assistant_text: BTreeMap<String, String>,
    reasoning_text: BTreeMap<String, String>,
    prompting: bool,
}

pub(crate) struct ACPSession {
    client: Arc<RpcClient>,
    native_id: String,
    run_lock: Mutex<()>,
    current: Arc<RwLock<Option<ACPInvocation>>>,
    broker: Arc<RequestBroker>,
    configured: Mutex<HashMap<String, Value>>,
}

#[derive(Clone)]
struct ACPInvocation {
    request: RunRequest,
    emit: EmitFn,
}

impl ACPAdapter {
    pub fn new(driver_kind: &str) -> Self {
        Self {
            binary: String::new(),
            args: Vec::new(),
            driver_kind: driver_kind.to_string(),
            load_method: String::new(),
            mode: String::new(),
            states: Arc::new(Mutex::new(HashMap::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn state(&self, id: &ProviderTurnID) -> Arc<Mutex<ACPTurnState>> {
        let mut states = self.states.lock().unwrap();
        states
            .entry(id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(ACPTurnState::default())))
            .clone()
    }

    fn session_for(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        emit: &EmitFn,
    ) -> Result<Arc<ACPSession>, Error> {
        let provider_thread_id = request.current_provider_thread().id;
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(existing) = sessions.get(&provider_thread_id).cloned() {
            return Ok(existing);
        }
        if self.binary.is_empty() {
            return Err(Error::provider("ACP binary is required"));
        }
        let args = if self.args.is_empty() {
            vec!["acp".to_string()]
        } else {
            self.args.clone()
        };

        let current_holder: Arc<RwLock<Option<ACPInvocation>>> = Arc::new(RwLock::new(None));
        let broker = Arc::new(RequestBroker::new());
        let driver = self.driver();
        let states = self.states.clone();

        let ch_notif = current_holder.clone();
        let driver_notif = driver.clone();
        let states_notif = states.clone();

        let ch_req = current_holder.clone();
        let br_req = broker.clone();
        let cwd = request.current_session().cwd;

        let client = RpcClient::start(RpcClientOptions {
            binary: self.binary.clone(),
            args,
            dir: cwd.clone(),
            on_notification: Some(Arc::new(move |raw| {
                let current = ch_notif.read().unwrap().clone();
                if let Some(inv) = current {
                    let bg_ctx = ExecutionContext::background();
                    let _ = handle_acp_frame(
                        &driver_notif,
                        &states_notif,
                        &bg_ctx,
                        &inv.request,
                        raw,
                        &inv.emit,
                    );
                }
            })),
            on_request: Some(Arc::new(move |req| {
                handle_acp_native_request(&ch_req, &br_req, req)
            })),
            ..Default::default()
        })?;

        let init_res: Result<(String, String), Error> = (|| {
            let _: Value = client.call(
                ctx,
                "initialize",
                json!({
                    "protocolVersion": 2,
                    "clientCapabilities": {
                        "fs": {"readTextFile": false, "writeTextFile": false},
                        "terminal": false,
                        "session": {"notices": {}},
                        "elicitation": {"form": {}}
                    },
                    "clientInfo": {"name": "g0code", "title": "g0code", "version": "0.0.0"}
                }),
            )?;

            let mut method = "session/new".to_string();
            let mut params = json!({
                "cwd": cwd,
                "mcpServers": []
            });
            if let Some(native_id) = request
                .current_provider_thread()
                .native_thread_ref
                .as_ref()
                .and_then(|r| r.native_id.as_ref())
            {
                method = if self.load_method.is_empty() {
                    "session/load".to_string()
                } else {
                    self.load_method.clone()
                };
                params
                    .as_object_mut()
                    .unwrap()
                    .insert("sessionId".to_string(), Value::String(native_id.clone()));
            }

            let session_result: ACPSessionResult = match client.call(ctx, &method, params.clone()) {
                Ok(res) => res,
                Err(_) if method != "session/new" => {
                    let fallback = if method == "session/load" {
                        "session/resume"
                    } else {
                        "session/load"
                    };
                    client.call(ctx, fallback, params)?
                }
                Err(err) => return Err(err),
            };

            let mut native_id = session_result.session_id;
            if native_id.is_empty() {
                if let Some(existing_id) = request
                    .current_provider_thread()
                    .native_thread_ref
                    .as_ref()
                    .and_then(|r| r.native_id.as_ref())
                {
                    native_id = existing_id.clone();
                }
            }
            if native_id.is_empty() {
                return Err(Error::provider(format!(
                    "ACP {} returned no sessionId",
                    method
                )));
            }
            Ok((native_id, method))
        })();

        let (native_id, method) = match init_res {
            Ok(pair) => pair,
            Err(err) => {
                client.close();
                return Err(err);
            }
        };

        let mut thread = request.current_provider_thread();
        thread.native_thread_ref = Some(ProviderRef {
            driver: self.driver(),
            native_id: Some(native_id.clone()),
            strength: NATIVE_REF_STRONG.to_string(),
            ..Default::default()
        });
        thread.updated_at = Utc::now();
        let raw_method = format!(r#"{{"method":"{}"}}"#, method);
        if let Err(err) = emit(
            ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_THREAD_UPDATED.to_string(),
                provider_thread: Some(thread),
                ..Default::default()
            },
            raw_method.as_bytes(),
        ) {
            client.close();
            return Err(err);
        }

        let session = Arc::new(ACPSession {
            client: client.clone(),
            native_id,
            run_lock: Mutex::new(()),
            current: current_holder,
            broker,
            configured: Mutex::new(HashMap::new()),
        });

        let config_res: Result<(), Error> = (|| {
            let effort = acp_effort(&request.start.run.model_selection);
            if !effort.is_empty() {
                self.set_config(ctx, &session, "effort", Value::String(effort))?;
            }
            if !self.mode.is_empty() {
                self.set_config(ctx, &session, "mode", Value::String(self.mode.clone()))?;
            }
            let model = &request.start.run.model_selection.model;
            if !model.is_empty() && model != "default" {
                self.set_config(ctx, &session, "model", Value::String(model.clone()))?;
            }
            Ok(())
        })();

        if let Err(err) = config_res {
            client.close();
            return Err(err);
        }

        sessions.insert(provider_thread_id, session.clone());
        Ok(session)
    }

    fn set_config(
        &self,
        ctx: &ExecutionContext,
        session: &ACPSession,
        id: &str,
        value: Value,
    ) -> Result<(), Error> {
        if let Some(s) = value.as_str() {
            if s.is_empty() {
                return Ok(());
            }
        }
        {
            let configured = session.configured.lock().unwrap();
            if configured.get(id) == Some(&value) {
                return Ok(());
            }
        }
        session.client.call_unit(
            ctx,
            "session/set_config_option",
            json!({
                "sessionId": session.native_id,
                "configId": id,
                "value": value
            }),
        )?;
        session
            .configured
            .lock()
            .unwrap()
            .insert(id.to_string(), value);
        Ok(())
    }

    fn apply_acp_options(
        &self,
        ctx: &ExecutionContext,
        session: &ACPSession,
        request: &RunRequest,
    ) -> Result<(), Error> {
        let effort = acp_effort(&request.start.run.model_selection);
        self.set_config(ctx, session, "effort", Value::String(effort))?;
        let model = &request.start.run.model_selection.model;
        if !model.is_empty() && model != "default" {
            self.set_config(ctx, session, "model", Value::String(model.clone()))?;
        }
        for (id, value) in &request.start.run.model_selection.options {
            match id.as_str() {
                "model" | "effort" | "reasoningEffort" => continue,
                _ => self.set_config(ctx, session, id, value.clone())?,
            }
        }
        Ok(())
    }
}

fn acp_effort(selection: &ModelSelection) -> String {
    if let Some(effort) = selection.options.get("effort").and_then(|v| v.as_str()) {
        if !effort.is_empty() {
            return effort.to_string();
        }
    }
    selection
        .options
        .get("reasoningEffort")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

fn handle_acp_native_request(
    current_holder: &Arc<RwLock<Option<ACPInvocation>>>,
    broker: &Arc<RequestBroker>,
    req: RpcRequest,
) -> Result<Value, Error> {
    if req.method != "session/request_permission" {
        return Err(Error::provider(format!(
            "unsupported ACP client callback: {}",
            req.method
        )));
    }
    #[derive(Default, Deserialize)]
    struct PermissionToolCall {
        #[serde(rename = "toolCallId", default)]
        tool_call_id: String,
        #[serde(default)]
        name: String,
        #[serde(default)]
        title: String,
        #[serde(rename = "rawInput", default)]
        raw_input: Value,
    }
    #[derive(Default, Deserialize)]
    struct PermissionOption {
        #[serde(rename = "optionId", default)]
        option_id: String,
        #[serde(default)]
        kind: String,
    }
    #[derive(Default, Deserialize)]
    struct PermissionPayload {
        #[serde(rename = "toolCall", default)]
        tool_call: PermissionToolCall,
        #[serde(default)]
        options: Vec<PermissionOption>,
    }
    let payload: PermissionPayload =
        from_value_lenient(req.params).map_err(|e| Error::provider(e.to_string()))?;
    let invocation = current_holder
        .read()
        .unwrap()
        .clone()
        .ok_or_else(|| Error::provider("ACP permission request has no active turn"))?;
    let kind = match payload.tool_call.name.as_str() {
        "bash" | "execute" => REQUEST_COMMAND,
        "read_file" => REQUEST_FILE_READ,
        "write_file" | "edit" => REQUEST_FILE_CHANGE,
        _ => REQUEST_PERMISSION,
    };
    let ctx = ExecutionContext::background();
    let response = broker.ask(
        &ctx,
        &invocation.request,
        &invocation.emit,
        &payload.tool_call.tool_call_id,
        kind.to_string(),
        &payload.tool_call.title,
        payload.tool_call.raw_input,
    )?;
    if response.decision.as_deref() == Some("cancel") {
        return Ok(json!({"outcome": {"outcome": "cancelled"}}));
    }
    let wanted = match response.decision.as_deref() {
        Some("acceptForSession") | Some("acceptAlways") => "allow_always",
        Some("decline") => "reject_once",
        _ => "allow_once",
    };
    let mut option_id = String::new();
    for option in &payload.options {
        if option.kind == wanted || option.option_id == wanted {
            option_id = option.option_id.clone();
            break;
        }
    }
    if option_id.is_empty() && !payload.options.is_empty() {
        option_id = payload.options[0].option_id.clone();
    }
    Ok(json!({
        "outcome": {
            "outcome": "selected",
            "optionId": option_id
        }
    }))
}

impl Adapter for ACPAdapter {
    fn driver(&self) -> ProviderDriverKind {
        if self.driver_kind.is_empty() {
            "acp".to_string()
        } else {
            self.driver_kind.clone()
        }
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            sessions: SessionCapabilities {
                supports_provider_switching_via_handoff: true,
                ..Default::default()
            },
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
                streams_reasoning: true,
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

    fn models(&self, ctx: &ExecutionContext, cwd: &str) -> Result<Vec<Model>, Error> {
        if self.binary.is_empty() {
            return Err(Error::provider("ACP binary is required"));
        }
        let args = if self.args.is_empty() {
            vec!["acp".to_string()]
        } else {
            self.args.clone()
        };
        let client = RpcClient::start(RpcClientOptions {
            binary: self.binary.clone(),
            args,
            dir: cwd.to_string(),
            ..Default::default()
        })?;
        let res = (|| {
            client.call_unit(
                ctx,
                "initialize",
                json!({
                    "protocolVersion": 2,
                    "clientCapabilities": {
                        "fs": {"readTextFile": false, "writeTextFile": false},
                        "terminal": false
                    },
                    "clientInfo": {"name": "g0code", "version": "0.1.0"}
                }),
            )?;
            let response: ACPSessionNewResponse = client.call(
                ctx,
                "session/new",
                json!({"cwd": cwd, "mcpServers": []}),
            )?;
            if !response.session_id.is_empty() {
                let _ = client.call_unit(
                    &ExecutionContext::background(),
                    "session/close",
                    json!({"sessionId": response.session_id}),
                );
            }
            let mut current = response.models.current_model_id.clone();
            let config_by_id: HashMap<String, ACPConfigOption> = response
                .config_options
                .iter()
                .map(|o| (o.id.clone(), o.clone()))
                .collect();
            if current.is_empty() {
                if let Some(m) = config_by_id.get("model") {
                    current = m
                        .current_value
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                }
            }
            if current.is_empty() && !response.models.available_models.is_empty() {
                current = response.models.available_models[0].model_id.clone();
            }
            if current.is_empty() {
                current = "default".to_string();
            }
            let make_model = |id: String, label: String| -> Model {
                let mut model = Model {
                    id,
                    label,
                    options: Vec::new(),
                };
                for option in &response.config_options {
                    if option.category == "mode" || option.id == "model" {
                        continue;
                    }
                    if option.options.is_empty() {
                        continue;
                    }
                    let values: Vec<String> =
                        option.options.iter().map(|c| c.value.clone()).collect();
                    model.options.push(ModelOption {
                        id: option.id.clone(),
                        label: option.category.clone(),
                        option_type: option.option_type.clone(),
                        values,
                        default: if option.current_value.is_null() {
                            None
                        } else {
                            Some(option.current_value.clone())
                        },
                    });
                }
                model
            };
            if response.models.available_models.is_empty() {
                return Ok(vec![make_model(current.clone(), current)]);
            }
            let mut models = Vec::with_capacity(response.models.available_models.len());
            for native in response.models.available_models {
                models.push(make_model(native.model_id, native.name));
            }
            Ok(models)
        })();
        client.close();
        res
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        emit: &EmitFn,
    ) -> Result<(), Error> {
        let session = self.session_for(ctx, request, emit)?;
        let _guard = session.run_lock.lock().unwrap();
        self.apply_acp_options(ctx, &session, request)?;

        let invocation = ACPInvocation {
            request: request.clone(),
            emit: emit.clone(),
        };
        *session.current.write().unwrap() = Some(invocation);
        struct ClearCurrent(Arc<RwLock<Option<ACPInvocation>>>);
        impl Drop for ClearCurrent {
            fn drop(&mut self) {
                *self.0.write().unwrap() = None;
            }
        }
        let _clear = ClearCurrent(session.current.clone());

        let turn_id = request.current_turn().id;
        let state = self.state(&turn_id);
        state.lock().unwrap().prompting = true;

        #[derive(Default, Deserialize)]
        struct PromptUsage {
            #[serde(rename = "inputTokens", default)]
            input_tokens: i64,
            #[serde(rename = "outputTokens", default)]
            output_tokens: i64,
            #[serde(rename = "cacheReadTokens", default)]
            cache_read_tokens: i64,
            #[serde(rename = "cachedReadTokens", default)]
            cached_read_tokens_alt: i64,
            #[serde(rename = "reasoningTokens", default)]
            reasoning_tokens: i64,
            #[serde(rename = "thoughtTokens", default)]
            thought_tokens: i64,
        }
        #[derive(Default, Deserialize)]
        struct PromptResult {
            #[serde(rename = "stopReason", default)]
            stop_reason: String,
            #[serde(default)]
            usage: PromptUsage,
        }

        let prompt_result: PromptResult = session.client.call(
            ctx,
            "session/prompt",
            json!({
                "sessionId": session.native_id,
                "prompt": [{"type": "text", "text": prompt_text(&request.start)}]
            }),
        )?;

        if prompt_result.stop_reason == "cancelled" {
            request.set_turn_status(PROVIDER_TURN_INTERRUPTED);
        }
        if prompt_result.usage.input_tokens != 0 || prompt_result.usage.output_tokens != 0 {
            let cached =
                prompt_result.usage.cache_read_tokens + prompt_result.usage.cached_read_tokens_alt;
            let reasoning =
                prompt_result.usage.reasoning_tokens + prompt_result.usage.thought_tokens;
            let mut turn = request.current_turn();
            turn.token_usage = Some(TokenUsage {
                used_tokens: prompt_result.usage.input_tokens + prompt_result.usage.output_tokens,
                max_tokens: None,
                input_tokens: prompt_result.usage.input_tokens,
                cached_input_tokens: cached,
                output_tokens: prompt_result.usage.output_tokens,
                reasoning_output_tokens: reasoning,
                updated_at: Utc::now().to_rfc3339(),
                ..Default::default()
            });
            emit(
                ctx,
                ProviderEvent {
                    event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                    provider_turn: Some(turn),
                    ..Default::default()
                },
                br#"{"method":"session/prompt","kind":"response"}"#,
            )?;
        }

        let assistant_entries: Vec<(String, String)> = {
            let st = state.lock().unwrap();
            st.assistant_text
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        for (native_item_id, text) in assistant_entries {
            emit_assistant(
                ctx,
                request,
                emit,
                &native_item_id,
                &text,
                false,
                br#"{"synthetic":"acp.message_end"}"#,
            )?;
        }
        Ok(())
    }

    fn handle_frame(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        raw: &[u8],
        emit: &EmitFn,
    ) -> Result<(), Error> {
        handle_acp_frame(&self.driver(), &self.states, ctx, request, raw, emit)
    }

    fn respond_runtime_request(
        &self,
        _ctx: &ExecutionContext,
        response: RuntimeRequestRespondEffect,
    ) -> Result<(), Error> {
        let sessions = self.sessions.lock().unwrap();
        if let Some(native_id) = response
            .request
            .native_request_ref
            .as_ref()
            .and_then(|r| r.native_id.as_ref())
        {
            for session in sessions.values() {
                if session.broker.has_pending(native_id) {
                    return session.broker.respond(response);
                }
            }
        }
        Err(Error::with_detail(
            ErrorKind::RequestNotRespondable,
            "ACP runtime request is not live",
        ))
    }

    fn interrupt(&self, _ctx: &ExecutionContext, effect: TurnInterruptEffect) -> Result<(), Error> {
        let sessions = self.sessions.lock().unwrap();
        for session in sessions.values() {
            let current = session.current.read().unwrap().clone();
            if let Some(inv) = current {
                if inv.request.current_turn().id == effect.provider_turn_id {
                    return session.client.notify(
                        "session/cancel",
                        Some(json!({"sessionId": session.native_id})),
                    );
                }
            }
        }
        Err(Error::with_detail(
            ErrorKind::UnsupportedCapability,
            format!("ACP turn {} is not active", effect.provider_turn_id),
        ))
    }

    fn close(&self) -> Result<(), Error> {
        let mut sessions = self.sessions.lock().unwrap();
        for (_, session) in sessions.drain() {
            session.broker.cancel_all();
            let _ = session.client.call_unit(
                &ExecutionContext::background(),
                "session/close",
                json!({"sessionId": session.native_id}),
            );
            session.client.close();
        }
        Ok(())
    }
}

#[derive(Default, Clone, Deserialize)]
struct ACPConfigChoice {
    #[serde(default)]
    value: String,
    #[serde(default)]
    #[allow(dead_code)]
    name: String,
}

#[derive(Default, Clone, Deserialize)]
struct ACPConfigOption {
    #[serde(default)]
    id: String,
    #[serde(default)]
    category: String,
    #[serde(rename = "type", default)]
    option_type: String,
    #[serde(rename = "currentValue", default)]
    current_value: Value,
    #[serde(default)]
    options: Vec<ACPConfigChoice>,
}

#[derive(Default, Deserialize)]
struct ACPAvailableModel {
    #[serde(rename = "modelId", default)]
    model_id: String,
    #[serde(default)]
    name: String,
}

#[derive(Default, Deserialize)]
struct ACPModelsBlock {
    #[serde(rename = "currentModelId", default)]
    current_model_id: String,
    #[serde(rename = "availableModels", default)]
    available_models: Vec<ACPAvailableModel>,
}

#[derive(Default, Deserialize)]
struct ACPSessionNewResponse {
    #[serde(rename = "sessionId", default)]
    session_id: String,
    #[serde(default)]
    models: ACPModelsBlock,
    #[serde(rename = "configOptions", default)]
    config_options: Vec<ACPConfigOption>,
}

#[derive(Default, Deserialize)]
struct ACPSessionResult {
    #[serde(rename = "sessionId", default)]
    session_id: String,
}

fn handle_acp_frame(
    driver: &str,
    states: &Arc<Mutex<HashMap<ProviderTurnID, Arc<Mutex<ACPTurnState>>>>>,
    ctx: &ExecutionContext,
    request: &RunRequest,
    raw: &[u8],
    emit: &EmitFn,
) -> Result<(), Error> {
    #[derive(Default, Deserialize)]
    struct ACPUpdate {
        #[serde(rename = "sessionUpdate", default)]
        session_update: String,
        #[serde(rename = "messageId", default)]
        message_id: String,
        #[serde(rename = "toolCallId", default)]
        tool_call_id: String,
        #[serde(default)]
        #[allow(dead_code)]
        name: String,
        #[serde(default)]
        title: String,
        #[serde(default)]
        kind: String,
        #[serde(default)]
        status: String,
        #[serde(rename = "rawInput", default)]
        raw_input: Value,
        #[serde(default)]
        content: Value,
    }
    #[derive(Default, Deserialize)]
    struct ACPParams {
        #[serde(rename = "sessionId", default)]
        #[allow(dead_code)]
        session_id: String,
        #[serde(default)]
        update: ACPUpdate,
    }
    #[derive(Default, Deserialize)]
    struct ACPResult {
        #[serde(rename = "sessionId", default)]
        session_id: String,
        #[serde(rename = "stopReason", default)]
        stop_reason: String,
    }
    #[derive(Default, Deserialize)]
    struct ACPFrame {
        #[serde(default)]
        kind: String,
        #[serde(default)]
        method: String,
        #[serde(default)]
        params: ACPParams,
        #[serde(default)]
        result: ACPResult,
    }

    let frame: ACPFrame = from_slice_lenient(raw).map_err(|e| Error::provider(e.to_string()))?;
    let turn_id = request.current_turn().id;
    let state_arc = {
        let mut map = states.lock().unwrap();
        map.entry(turn_id)
            .or_insert_with(|| Arc::new(Mutex::new(ACPTurnState::default())))
            .clone()
    };

    if frame.method == "session/prompt" && frame.kind != "response" {
        state_arc.lock().unwrap().prompting = true;
    }
    if frame.method == "session/prompt"
        && frame.kind == "response"
        && frame.result.stop_reason == "cancelled"
    {
        request.set_turn_status(PROVIDER_TURN_INTERRUPTED);
    }
    if !frame.result.session_id.is_empty() {
        let mut thread = request.current_provider_thread();
        thread.native_thread_ref = Some(ProviderRef {
            driver: driver.to_string(),
            native_id: Some(frame.result.session_id),
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

    let mut state = state_arc.lock().unwrap();
    if frame.method != "session/update" || !state.prompting {
        return Ok(());
    }
    let update = frame.params.update;
    if update.session_update == "tool_call" || update.session_update == "tool_call_update" {
        let item_type = match update.kind.as_str() {
            "edit" => "file_change",
            "execute" => "command_execution",
            _ => "dynamic_tool",
        };
        let status = if update.status.is_empty() {
            TURN_ITEM_RUNNING.to_string()
        } else {
            update.status
        };
        return emit_tool_call(
            ctx,
            request,
            emit,
            ToolCall {
                id: update.tool_call_id,
                item_type: item_type.to_string(),
                title: update.title,
                status,
                input: update.raw_input,
                output: update.content,
                ..Default::default()
            },
            raw,
        );
    }

    #[derive(Default, Deserialize)]
    struct ACPContent {
        #[serde(rename = "type", default)]
        content_type: String,
        #[serde(default)]
        text: String,
    }
    let content: ACPContent = if update.content.is_object() {
        serde_json::from_value(update.content).map_err(|e| Error::provider(e.to_string()))?
    } else {
        ACPContent::default()
    };
    if content.content_type != "text" {
        return Ok(());
    }

    match update.session_update.as_str() {
        "agent_message_chunk" => {
            let native_item_id = if update.message_id.is_empty() {
                format!("{}:assistant", request.start.attempt.id)
            } else {
                update.message_id
            };
            let entry = state.assistant_text.entry(native_item_id.clone()).or_default();
            entry.push_str(&content.text);
            let full_text = entry.clone();
            emit_assistant(ctx, request, emit, &native_item_id, &full_text, true, raw)
        }
        "agent_thought_chunk" => {
            let native_item_id = if update.message_id.is_empty() {
                format!("{}:reasoning", request.start.attempt.id)
            } else {
                update.message_id
            };
            let entry = state.reasoning_text.entry(native_item_id.clone()).or_default();
            entry.push_str(&content.text);
            let full_text = entry.clone();
            let item = TurnItem {
                id: derive_turn_item_from_provider_item_id(driver, &native_item_id),
                thread_id: request.start.thread.id.clone(),
                run_id: Some(request.start.run.id.clone()),
                node_id: request.start.run.root_node_id.clone(),
                provider_thread_id: Some(request.current_provider_thread().id),
                provider_turn_id: Some(request.current_turn().id),
                ordinal: 1,
                status: TURN_ITEM_RUNNING.to_string(),
                item_type: "reasoning".to_string(),
                text: full_text,
                created_by: ACTOR_AGENT.to_string(),
                creation_source: CREATION_PROVIDER.to_string(),
                updated_at: Utc::now(),
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
        _ => Ok(()),
    }
}
