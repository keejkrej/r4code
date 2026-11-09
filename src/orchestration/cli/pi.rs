use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};

use crate::orchestration::cli::adapter::{emit_assistant, prompt_text, Adapter, EmitFn, RunRequest};
use crate::orchestration::cli::models::{Model, ModelOption};
use crate::orchestration::cli::rpc::{from_slice_lenient, RpcClient, RpcClientOptions};
use crate::orchestration::cli::tools::{emit_tool_call, RequestBroker, ToolCall};
use crate::orchestration::command::{RuntimeRequestRespondEffect, TurnInterruptEffect};
use crate::orchestration::errors::{Error, ErrorKind};
use crate::orchestration::ids::{derive_turn_item_from_provider_item_id, ProviderThreadID};
use crate::orchestration::outbox::ExecutionContext;
use crate::orchestration::provider::{
    ProviderEvent, PROVIDER_EVENT_THREAD_UPDATED, PROVIDER_EVENT_TURN_ITEM_UPDATED,
    PROVIDER_EVENT_TURN_UPDATED,
};
use crate::orchestration::types::{
    IdentityCapabilities, ProviderCapabilities, ProviderDriverKind, ProviderRef, RuntimeMode,
    StreamingCapabilities, ThreadCapabilities, TokenUsage, TurnCapabilities, TurnItem, ACTOR_AGENT,
    CREATION_PROVIDER, NATIVE_REF_STRONG, PROVIDER_TURN_INTERRUPTED, REQUEST_PERMISSION,
    TURN_ITEM_COMPLETED, TURN_ITEM_FAILED, TURN_ITEM_RUNNING,
};

const PI_PERMISSION_EXTENSION: &str = r#"export default function(pi) {
  const readOnly = new Set(["read", "grep", "find", "ls"]);
  const edits = new Set(["edit", "write"]);
  pi.on("tool_call", async (event, ctx) => {
    const mode = process.env.G0CODE_RUNTIME_MODE || "approval-required";
    if (mode === "full-access" || readOnly.has(event.toolName)) return;
    if (mode === "auto-accept-edits" && edits.has(event.toolName)) return;
    const approved = await ctx.ui.confirm(
      "Allow " + event.toolName + "?",
      JSON.stringify(event.input),
      { signal: ctx.signal },
    );
    if (!approved) {
      return { block: true, reason: event.toolName + " was declined in g0code." };
    }
  });
}
"#;

pub struct PiAdapter {
    pub binary: String,
    pub session_dir: String,
    pub(crate) sessions: Arc<Mutex<HashMap<ProviderThreadID, Arc<PiSession>>>>,
    pub(crate) models_discovered: Arc<AtomicBool>,
}

impl Default for PiAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl PiAdapter {
    pub fn new() -> Self {
        Self {
            binary: String::new(),
            session_dir: String::new(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            models_discovered: Arc::new(AtomicBool::new(false)),
        }
    }

    fn permission_extension(&self) -> Result<String, Error> {
        let dir = if !self.session_dir.is_empty() {
            PathBuf::from(&self.session_dir)
        } else {
            let cache = std::env::var("HOME")
                .map(|h| PathBuf::from(h).join(".cache"))
                .unwrap_or_else(|_| std::env::temp_dir());
            cache.join("g0code").join("pi")
        };
        fs::create_dir_all(&dir).map_err(|e| Error::provider(e.to_string()))?;
        let path = dir.join("g0code-permission.ts");
        fs::write(&path, PI_PERMISSION_EXTENSION).map_err(|e| Error::provider(e.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = fs::metadata(&path) {
                let mut perms = meta.permissions();
                perms.set_mode(0o600);
                let _ = fs::set_permissions(&path, perms);
            }
        }
        Ok(path.to_string_lossy().into_owned())
    }

    fn session_for(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        emit: &EmitFn,
    ) -> Result<Arc<PiSession>, Error> {
        let provider_thread_id = request.current_provider_thread().id;
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(existing) = sessions.get(&provider_thread_id).cloned() {
            if existing.runtime_mode == request.start.thread.runtime_mode {
                return Ok(existing);
            }
            existing.broker.cancel_all();
            existing.client.close();
            sessions.remove(&provider_thread_id);
        }

        let binary = if self.binary.is_empty() {
            "pi".to_string()
        } else {
            self.binary.clone()
        };
        let mut args = vec![
            "--mode".to_string(),
            "rpc".to_string(),
            "--tools".to_string(),
            "read,grep,find,ls,bash,edit,write".to_string(),
            "--no-extensions".to_string(),
            "--no-mcp".to_string(),
            "--no-skills".to_string(),
            "--no-prompt-templates".to_string(),
            "--no-context-files".to_string(),
            "--no-approve".to_string(),
            "--offline".to_string(),
        ];
        let ext_path = self.permission_extension()?;
        args.push("--extension".to_string());
        args.push(ext_path);
        if !self.session_dir.is_empty() {
            args.push("--session-dir".to_string());
            args.push(self.session_dir.clone());
        }

        let env = vec![format!(
            "G0CODE_RUNTIME_MODE={}",
            request.start.thread.runtime_mode
        )];

        let current_holder: Arc<RwLock<Option<Arc<PiInvocation>>>> = Arc::new(RwLock::new(None));
        let broker = Arc::new(RequestBroker::new());
        let client_holder: Arc<RwLock<Option<Arc<RpcClient>>>> = Arc::new(RwLock::new(None));

        let ch_notif = current_holder.clone();
        let br_notif = broker.clone();
        let cl_notif = client_holder.clone();
        let handle_cb = Arc::new(move |raw: &[u8]| {
            handle_pi_notification(&ch_notif, &br_notif, &cl_notif, raw);
        });
        let cb1 = handle_cb.clone();
        let cb2 = handle_cb;

        let client = RpcClient::start(RpcClientOptions {
            binary,
            args,
            dir: request.current_session().cwd,
            id_prefix: "g0code-".to_string(),
            env,
            on_notification: Some(Arc::new(move |raw| cb1(raw))),
            on_response: Some(Arc::new(move |raw| cb2(raw))),
            ..Default::default()
        })?;
        *client_holder.write().unwrap() = Some(client.clone());

        let init_res: Result<String, Error> = (|| {
            if let Some(native_id) = request
                .current_provider_thread()
                .native_thread_ref
                .as_ref()
                .and_then(|r| r.native_id.as_ref())
            {
                let mut rec = serde_json::Map::new();
                rec.insert("type".to_string(), Value::String("switch_session".to_string()));
                rec.insert("sessionPath".to_string(), Value::String(native_id.clone()));
                let response: PiFrame = client.request_record(ctx, rec)?;
                if !response.success {
                    return Err(Error::provider("Pi switch_session failed"));
                }
            }
            let mut state_rec = serde_json::Map::new();
            state_rec.insert("type".to_string(), Value::String("get_state".to_string()));
            let state: PiFrame = client.request_record(ctx, state_rec)?;
            if !state.success || state.data.session_file.is_empty() {
                return Err(Error::provider(
                    "Pi get_state returned no persisted sessionFile",
                ));
            }
            if !self.models_discovered.load(Ordering::SeqCst) {
                let mut mod_rec = serde_json::Map::new();
                mod_rec.insert(
                    "type".to_string(),
                    Value::String("get_available_models".to_string()),
                );
                if client.request_record::<PiFrame>(ctx, mod_rec).is_ok() {
                    self.models_discovered.store(true, Ordering::SeqCst);
                }
            }
            let mut entries_rec = serde_json::Map::new();
            entries_rec.insert("type".to_string(), Value::String("get_entries".to_string()));
            let _baseline: PiFrame = client.request_record(ctx, entries_rec)?;
            Ok(state.data.session_file)
        })();

        let native_id = match init_res {
            Ok(id) => id,
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
        if let Err(err) = emit(
            ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_THREAD_UPDATED.to_string(),
                provider_thread: Some(thread),
                ..Default::default()
            },
            br#"{"type":"response","command":"get_state"}"#,
        ) {
            client.close();
            return Err(err);
        }

        let session = Arc::new(PiSession {
            client,
            native_id,
            run_lock: Mutex::new(()),
            state: Mutex::new(PiSessionState {
                model: String::new(),
                session_name: String::new(),
                thinking: String::new(),
            }),
            current: current_holder,
            runtime_mode: request.start.thread.runtime_mode.clone(),
            broker,
        });
        sessions.insert(provider_thread_id, session.clone());
        Ok(session)
    }

    fn apply_selection(
        &self,
        ctx: &ExecutionContext,
        session: &PiSession,
        request: &RunRequest,
    ) -> Result<(), Error> {
        let mut st = session.state.lock().unwrap();
        let model = &request.start.run.model_selection.model;
        if !model.is_empty() && model != "default" && *model != st.model {
            let Some((provider, model_id)) = model.split_once('/') else {
                return Err(Error::provider(format!(
                    "Pi model {:?} must use provider/model format",
                    model
                )));
            };
            if provider.is_empty() || model_id.is_empty() {
                return Err(Error::provider(format!(
                    "Pi model {:?} must use provider/model format",
                    model
                )));
            }
            let mut rec = serde_json::Map::new();
            rec.insert("type".to_string(), Value::String("set_model".to_string()));
            rec.insert("provider".to_string(), Value::String(provider.to_string()));
            rec.insert("modelId".to_string(), Value::String(model_id.to_string()));
            let response: PiFrame = session.client.request_record(ctx, rec)?;
            if !response.success {
                return Err(Error::provider(format!("Pi set_model failed for {}", model)));
            }
            st.model = model.clone();
        }
        if let Some(thinking) = request
            .start
            .run
            .model_selection
            .options
            .get("thinking")
            .and_then(|v| v.as_str())
        {
            if !thinking.is_empty() && thinking != st.thinking {
                let mut rec = serde_json::Map::new();
                rec.insert(
                    "type".to_string(),
                    Value::String("set_thinking_level".to_string()),
                );
                rec.insert("level".to_string(), Value::String(thinking.to_string()));
                let response: PiFrame = session.client.request_record(ctx, rec)?;
                if !response.success {
                    return Err(Error::provider(format!(
                        "Pi set_thinking_level failed for {}",
                        thinking
                    )));
                }
                st.thinking = thinking.to_string();
            }
        }
        let title = &request.start.thread.title;
        if !title.is_empty() && *title != st.session_name {
            let mut rec = serde_json::Map::new();
            rec.insert(
                "type".to_string(),
                Value::String("set_session_name".to_string()),
            );
            rec.insert("name".to_string(), Value::String(title.clone()));
            let _response: PiFrame = session.client.request_record(ctx, rec)?;
            st.session_name = title.clone();
        }
        Ok(())
    }
}

struct PiSessionState {
    model: String,
    session_name: String,
    thinking: String,
}

pub(crate) struct PiSession {
    client: Arc<RpcClient>,
    #[allow(dead_code)]
    native_id: String,
    run_lock: Mutex<()>,
    state: Mutex<PiSessionState>,
    current: Arc<RwLock<Option<Arc<PiInvocation>>>>,
    runtime_mode: RuntimeMode,
    broker: Arc<RequestBroker>,
}

struct PiInvocation {
    request: RunRequest,
    emit: EmitFn,
    done_tx: Mutex<Option<mpsc::Sender<Result<(), Error>>>>,
}

impl PiInvocation {
    fn finish(&self, res: Result<(), Error>) {
        if let Some(tx) = self.done_tx.lock().unwrap().take() {
            let _ = tx.send(res);
        }
    }
}

fn handle_pi_notification(
    current_holder: &Arc<RwLock<Option<Arc<PiInvocation>>>>,
    broker: &Arc<RequestBroker>,
    client_holder: &Arc<RwLock<Option<Arc<RpcClient>>>>,
    raw: &[u8],
) {
    let invocation = current_holder.read().unwrap().clone();
    let Some(invocation) = invocation else {
        return;
    };
    let Ok(mut frame) = from_slice_lenient::<PiFrame>(raw) else {
        return;
    };
    if frame.frame_type == "extension_ui_request" && frame.method == "confirm" {
        #[derive(Deserialize)]
        struct ExtMsg {
            #[serde(default)]
            message: String,
        }
        if let Ok(ext) = serde_json::from_slice::<ExtMsg>(raw) {
            frame.text = ext.message;
        }
        let br = broker.clone();
        let cl = client_holder.read().unwrap().clone();
        let inv = invocation.clone();
        thread::spawn(move || {
            let ctx = ExecutionContext::background();
            let res = br.ask(
                &ctx,
                &inv.request,
                &inv.emit,
                &frame.id,
                REQUEST_PERMISSION.to_string(),
                &frame.title,
                Value::String(frame.text.clone()),
            );
            let mut confirmed = false;
            let mut cancelled = false;
            match res {
                Err(_) => cancelled = true,
                Ok(resp) => {
                    if let Some(dec) = resp.decision.as_deref() {
                        confirmed = dec == "accept"
                            || dec == "acceptForSession"
                            || dec == "acceptAlways";
                        cancelled = dec == "cancel";
                    }
                }
            }
            if let Some(c) = cl {
                let _ = c.send_record(&json!({
                    "type": "extension_ui_response",
                    "id": frame.id,
                    "confirmed": confirmed,
                    "cancelled": cancelled
                }));
            }
        });
        return;
    }
    if frame.frame_type == "turn_end" && frame.message.stop_reason == "aborted" {
        invocation.request.set_turn_status(PROVIDER_TURN_INTERRUPTED);
    }
    if frame.frame_type == "agent_settled" && frame.aborted {
        invocation.request.set_turn_status(PROVIDER_TURN_INTERRUPTED);
    }
    let ctx = ExecutionContext::background();
    if let Err(err) = handle_pi_frame(&ctx, &invocation.request, raw, &invocation.emit) {
        invocation.finish(Err(err));
        return;
    }
    if frame.frame_type == "agent_settled" {
        invocation.finish(Ok(()));
    }
}

impl Adapter for PiAdapter {
    fn driver(&self) -> ProviderDriverKind {
        "pi".to_string()
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
                streams_reasoning: true,
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

    fn models(&self, ctx: &ExecutionContext, cwd: &str) -> Result<Vec<Model>, Error> {
        let binary = if self.binary.is_empty() {
            "pi".to_string()
        } else {
            self.binary.clone()
        };
        let mut args = vec![
            "--mode".to_string(),
            "rpc".to_string(),
            "--no-tools".to_string(),
            "--no-extensions".to_string(),
            "--no-mcp".to_string(),
            "--no-skills".to_string(),
            "--no-prompt-templates".to_string(),
            "--no-context-files".to_string(),
            "--no-approve".to_string(),
            "--offline".to_string(),
        ];
        if !self.session_dir.is_empty() {
            args.push("--session-dir".to_string());
            args.push(self.session_dir.clone());
        }
        let client = RpcClient::start(RpcClientOptions {
            binary,
            args,
            dir: cwd.to_string(),
            id_prefix: "g0code-".to_string(),
            ..Default::default()
        })?;
        let res = (|| {
            let mut state_rec = serde_json::Map::new();
            state_rec.insert("type".to_string(), Value::String("get_state".to_string()));
            let _state: Value = client.request_record(ctx, state_rec)?;
            let mut mod_rec = serde_json::Map::new();
            mod_rec.insert(
                "type".to_string(),
                Value::String("get_available_models".to_string()),
            );
            let response: PiModelsResponse = client.request_record(ctx, mod_rec)?;
            let mut models = Vec::with_capacity(response.data.models.len());
            for native in response.data.models {
                let label = if native.name.is_empty() {
                    native.id.clone()
                } else {
                    native.name.clone()
                };
                let mut model = Model {
                    id: format!("{}/{}", native.provider, native.id),
                    label,
                    options: Vec::new(),
                };
                let levels = pi_thinking_levels(&native);
                if !levels.is_empty() {
                    let default_level = if native
                        .thinking_level_map
                        .get("high")
                        .is_some_and(|v| v.is_some())
                    {
                        "high"
                    } else {
                        "medium"
                    };
                    model.options.push(ModelOption {
                        id: "thinking".to_string(),
                        label: "Thinking".to_string(),
                        option_type: "select".to_string(),
                        values: levels,
                        default: Some(Value::String(default_level.to_string())),
                    });
                }
                models.push(model);
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
        self.apply_selection(ctx, &session, request)?;

        let (done_tx, done_rx) = mpsc::channel();
        let invocation = Arc::new(PiInvocation {
            request: request.clone(),
            emit: emit.clone(),
            done_tx: Mutex::new(Some(done_tx)),
        });
        *session.current.write().unwrap() = Some(invocation);
        struct ClearCurrent(Arc<RwLock<Option<Arc<PiInvocation>>>>);
        impl Drop for ClearCurrent {
            fn drop(&mut self) {
                *self.0.write().unwrap() = None;
            }
        }
        let _clear = ClearCurrent(session.current.clone());

        session.client.send_record(&json!({
            "type": "prompt",
            "message": prompt_text(&request.start),
            "streamingBehavior": "steer"
        }))?;

        loop {
            if ctx.is_cancelled() {
                return Err(Error::provider("execution context cancelled"));
            }
            match done_rx.recv_timeout(Duration::from_millis(20)) {
                Ok(res) => {
                    res?;
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if session.client.is_closed() {
                        let cerr = session.client.closed_error();
                        return Err(Error::provider(format!(
                            "Pi RPC closed during turn: {}",
                            cerr
                        )));
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        for command in ["get_state", "get_entries", "get_session_stats"] {
            let mut rec = serde_json::Map::new();
            rec.insert("type".to_string(), Value::String(command.to_string()));
            let frame: Value = session.client.request_record(ctx, rec)?;
            let raw = serde_json::to_vec(&frame).unwrap_or_default();
            self.handle_frame(ctx, request, &raw, emit)?;
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
        handle_pi_frame(ctx, request, raw, emit)
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
            "Pi runtime request is not live",
        ))
    }

    fn interrupt(&self, ctx: &ExecutionContext, effect: TurnInterruptEffect) -> Result<(), Error> {
        let sessions = self.sessions.lock().unwrap();
        for session in sessions.values() {
            let current = session.current.read().unwrap().clone();
            if let Some(inv) = current {
                if inv.request.current_turn().id == effect.provider_turn_id {
                    let mut rec = serde_json::Map::new();
                    rec.insert("type".to_string(), Value::String("abort".to_string()));
                    let _: Value = session.client.request_record(ctx, rec)?;
                    return Ok(());
                }
            }
        }
        Err(Error::with_detail(
            ErrorKind::UnsupportedCapability,
            format!("Pi turn {} is not active", effect.provider_turn_id),
        ))
    }

    fn close(&self) -> Result<(), Error> {
        let mut sessions = self.sessions.lock().unwrap();
        for (_, session) in sessions.drain() {
            session.broker.cancel_all();
            session.client.close();
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PiNativeModel {
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    provider: String,
    #[serde(default)]
    reasoning: bool,
    #[serde(rename = "thinkingLevelMap", default)]
    thinking_level_map: HashMap<String, Option<String>>,
}

#[derive(Debug, Default, Deserialize)]
struct PiModelsResponse {
    #[serde(default)]
    data: PiModelsData,
}

#[derive(Debug, Default, Deserialize)]
struct PiModelsData {
    #[serde(default)]
    models: Vec<PiNativeModel>,
}

fn pi_thinking_levels(model: &PiNativeModel) -> Vec<String> {
    if !model.reasoning {
        return Vec::new();
    }
    let mut levels = Vec::with_capacity(7);
    for level in ["off", "minimal", "low", "medium", "high", "xhigh", "max"] {
        let Some(Some(val)) = model.thinking_level_map.get(level) else {
            continue;
        };
        if (level == "xhigh" || level == "max") && val.is_empty() {
            continue;
        }
        levels.push(level.to_string());
    }
    levels
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PiContent {
    #[serde(rename = "type", default)]
    content_type: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    thinking: String,
}

fn deserialize_pi_content_list<'de, D>(deserializer: D) -> Result<Vec<PiContent>, D::Error>
where
    D: Deserializer<'de>,
{
    let val = Value::deserialize(deserializer)?;
    match val {
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                if item.is_object() {
                    if let Ok(c) = serde_json::from_value::<PiContent>(item) {
                        out.push(c);
                    }
                }
            }
            Ok(out)
        }
        _ => Ok(Vec::new()),
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PiUsage {
    #[serde(default)]
    input: i64,
    #[serde(default)]
    output: i64,
    #[serde(rename = "cacheRead", default)]
    cache_read: i64,
    #[serde(rename = "totalTokens", default)]
    total_tokens: i64,
    #[serde(default)]
    reasoning: i64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PiMessage {
    #[serde(default)]
    role: String,
    #[serde(rename = "responseId", default)]
    response_id: String,
    #[serde(rename = "stopReason", default)]
    stop_reason: String,
    #[serde(default, deserialize_with = "deserialize_pi_content_list")]
    content: Vec<PiContent>,
    #[serde(default)]
    usage: PiUsage,
}

fn deserialize_pi_message<'de, D>(deserializer: D) -> Result<PiMessage, D::Error>
where
    D: Deserializer<'de>,
{
    let val = Value::deserialize(deserializer)?;
    if val.is_object() {
        Ok(serde_json::from_value(val).unwrap_or_default())
    } else {
        Ok(PiMessage::default())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PiAssistantMessageEvent {
    #[serde(rename = "type", default)]
    event_type: String,
    #[serde(rename = "contentIndex", default)]
    content_index: i64,
    #[serde(default)]
    content: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PiEntry {
    #[serde(default)]
    id: String,
    #[serde(rename = "type", default)]
    entry_type: String,
    #[serde(default, deserialize_with = "deserialize_pi_message")]
    message: PiMessage,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PiDataTokens {
    #[serde(default)]
    input: i64,
    #[serde(default)]
    output: i64,
    #[serde(rename = "cacheRead", default)]
    cache_read: i64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PiContextUsage {
    #[serde(default)]
    tokens: i64,
    #[serde(rename = "contextWindow", default)]
    context_window: i64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PiData {
    #[serde(rename = "sessionId", default)]
    session_id: String,
    #[serde(rename = "sessionFile", default)]
    session_file: String,
    #[serde(default)]
    entries: Vec<PiEntry>,
    #[serde(default)]
    tokens: PiDataTokens,
    #[serde(rename = "contextUsage", default)]
    context_usage: PiContextUsage,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PiFrame {
    #[serde(rename = "type", default)]
    frame_type: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    success: bool,
    #[serde(default)]
    aborted: bool,
    #[serde(default)]
    method: String,
    #[serde(default)]
    title: String,
    #[serde(skip)]
    text: String,
    #[serde(rename = "toolCallId", default)]
    tool_call_id: String,
    #[serde(rename = "toolName", default)]
    tool_name: String,
    #[serde(default)]
    args: Value,
    #[serde(rename = "partialResult", default)]
    partial_result: Value,
    #[serde(default)]
    result: Value,
    #[serde(rename = "isError", default)]
    is_error: bool,
    #[serde(default, deserialize_with = "deserialize_pi_message")]
    message: PiMessage,
    #[serde(default)]
    usage: PiUsage,
    #[serde(rename = "assistantMessageEvent", default)]
    assistant_message_event: PiAssistantMessageEvent,
    #[serde(default)]
    data: PiData,
}

fn handle_pi_frame(
    ctx: &ExecutionContext,
    request: &RunRequest,
    raw: &[u8],
    emit: &EmitFn,
) -> Result<(), Error> {
    let frame: PiFrame = from_slice_lenient(raw).map_err(|e| Error::provider(e.to_string()))?;
    if frame.frame_type.starts_with("tool_execution_") && !frame.tool_call_id.is_empty() {
        let item_type = match frame.tool_name.as_str() {
            "bash" => "command_execution",
            "edit" | "write" => "file_change",
            _ => "dynamic_tool",
        };
        let mut status = TURN_ITEM_RUNNING.to_string();
        let mut output = frame.partial_result.clone();
        if frame.frame_type == "tool_execution_end" {
            status = if frame.is_error {
                TURN_ITEM_FAILED.to_string()
            } else {
                TURN_ITEM_COMPLETED.to_string()
            };
            output = frame.result.clone();
        }
        return emit_tool_call(
            ctx,
            request,
            emit,
            ToolCall {
                id: frame.tool_call_id,
                item_type: item_type.to_string(),
                title: frame.tool_name,
                status,
                input: frame.args,
                output,
                ..Default::default()
            },
            raw,
        );
    }

    let mut native_session_id = if frame.frame_type == "session" {
        frame.id.clone()
    } else {
        String::new()
    };
    if !frame.data.session_id.is_empty() {
        native_session_id = frame.data.session_id.clone();
    }
    if !native_session_id.is_empty() {
        let mut thread = request.current_provider_thread();
        thread.native_thread_ref = Some(ProviderRef {
            driver: "pi".to_string(),
            native_id: Some(native_session_id),
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

    if frame.frame_type == "message_update"
        && frame.assistant_message_event.event_type == "thinking_end"
    {
        let native_id = format!(
            "{}:reasoning:{}",
            request.start.attempt.id, frame.assistant_message_event.content_index
        );
        let item = TurnItem {
            id: derive_turn_item_from_provider_item_id("pi", &native_id),
            thread_id: request.start.thread.id.clone(),
            run_id: Some(request.start.run.id.clone()),
            node_id: request.start.run.root_node_id.clone(),
            provider_thread_id: Some(request.current_provider_thread().id),
            provider_turn_id: Some(request.current_turn().id),
            ordinal: frame.assistant_message_event.content_index + 1,
            status: TURN_ITEM_COMPLETED.to_string(),
            item_type: "reasoning".to_string(),
            text: frame.assistant_message_event.content.clone(),
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
        )?;
    }

    if frame.frame_type == "message_update" && frame.usage.total_tokens > 0 {
        emit_pi_usage(
            ctx,
            request,
            emit,
            raw,
            frame.usage.total_tokens,
            0,
            frame.usage.input,
            frame.usage.cache_read,
            frame.usage.output,
            frame.usage.reasoning,
        )?;
    }

    if frame.frame_type == "message_end" && frame.message.role == "assistant" {
        let mut text = String::new();
        for content in &frame.message.content {
            if content.content_type == "text" {
                text.push_str(&content.text);
            }
        }
        let native_item_id = if frame.message.response_id.is_empty() {
            format!("{}:assistant", request.start.attempt.id)
        } else {
            frame.message.response_id.clone()
        };
        if !text.is_empty() {
            emit_assistant(ctx, request, emit, &native_item_id, &text, false, raw)?;
        }
        let mut turn = request.current_turn();
        turn.native_turn_ref = Some(ProviderRef {
            driver: "pi".to_string(),
            native_id: Some(native_item_id),
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
        if frame.message.stop_reason == "error" {
            return Err(Error::turn(
                "unknown",
                "Pi assistant turn ended with an error.",
                "",
                "",
            ));
        }
    }

    if frame.frame_type == "response" && frame.command == "get_entries" && frame.success {
        for entry in &frame.data.entries {
            if entry.entry_type == "message" && entry.message.role == "user" {
                let mut turn = request.current_turn();
                turn.native_turn_ref = Some(ProviderRef {
                    driver: "pi".to_string(),
                    native_id: Some(entry.id.clone()),
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
                break;
            }
        }
    }

    if frame.frame_type == "response" && frame.command == "get_session_stats" && frame.success {
        let used = frame.data.context_usage.tokens;
        let max_tokens = frame.data.context_usage.context_window;
        emit_pi_usage(
            ctx,
            request,
            emit,
            raw,
            used,
            max_tokens,
            frame.data.tokens.input,
            frame.data.tokens.cache_read,
            frame.data.tokens.output,
            0,
        )?;
    }

    if frame.frame_type == "response" && !frame.success && frame.command == "prompt" {
        return Err(Error::turn(
            "unknown",
            "Pi rejected the prompt.",
            "prompt_rejected",
            "",
        ));
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_pi_usage(
    ctx: &ExecutionContext,
    request: &RunRequest,
    emit: &EmitFn,
    raw: &[u8],
    used: i64,
    max_tokens: i64,
    input: i64,
    cached: i64,
    output: i64,
    reasoning: i64,
) -> Result<(), Error> {
    let mut turn = request.current_turn();
    turn.token_usage = Some(TokenUsage {
        used_tokens: used,
        max_tokens: if max_tokens > 0 {
            Some(max_tokens)
        } else {
            None
        },
        input_tokens: input,
        cached_input_tokens: cached,
        output_tokens: output,
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
        raw,
    )
}
