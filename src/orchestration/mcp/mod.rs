use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::orchestration::cli::from_value_lenient;
use crate::orchestration::command::{
    CancelQueuedRun, Command, CommandPayload, CreateThread, DelegateTask, DispatchMessage,
    DispatchMode, Effect, InterruptRun, RuntimeRequestResponse, COMMAND_DELEGATED_TASK_REQUEST,
    COMMAND_MESSAGE_DISPATCH, COMMAND_QUEUED_RUN_CANCEL, COMMAND_RUN_INTERRUPT,
    COMMAND_RUNTIME_REQUEST_RESPOND, COMMAND_THREAD_CREATE, DISPATCH_QUEUE_AFTER_ACTIVE,
    DISPATCH_RESTART_ACTIVE, DISPATCH_START_IMMEDIATELY, DISPATCH_STEER_ACTIVE,
};
use crate::orchestration::errors::{Error, ErrorKind};
use crate::orchestration::ids::{
    derive_delegated_task_node_id, new_uuid_v4, CommandID, ContextTransferID, MessageID, NodeID,
    ProjectID, ProviderDriverKind, ProviderInstanceID, RunID, RuntimeRequestID, ThreadID,
    TurnItemID,
};
use crate::orchestration::orchestrator::Orchestrator;
use crate::orchestration::outbox::{EffectExecutor, ExecutionContext};
use crate::orchestration::types::{
    run_status_is_active, run_status_is_terminal, runtime_mode_above, ConversationMessage,
    InteractionMode, ModelSelection, Run, RuntimeMode, RuntimeRequest, Subagent, TurnItem,
    ACTOR_AGENT, CREATION_MCP, INTERACTION_DEFAULT, INTERACTION_PLAN, MESSAGE_ASSISTANT,
    REQUEST_PENDING, REQUEST_USER_INPUT, RUNTIME_FULL_ACCESS, RUN_QUEUED,
};

const RUNNING_TASK_NEXT: &str =
    "Completion wakes this thread, so end your turn. Do not wait on it or poll it.";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Provider {
    #[serde(rename = "instanceId")]
    pub instance_id: ProviderInstanceID,
    pub driver: ProviderDriverKind,
    #[serde(default)]
    pub models: Vec<String>,
    pub available: bool,
    #[serde(rename = "runtimeModes", skip_serializing_if = "Vec::is_empty", default)]
    pub runtime_modes: Vec<RuntimeMode>,
}

pub type OnEffectErrorFn = Arc<dyn Fn(&Effect, &Error) + Send + Sync>;

#[derive(Clone, Default)]
pub struct Config {
    pub providers: Vec<Provider>,
    pub default_project_id: ProjectID,
    pub default_worktree_path: Option<String>,
    pub default_target: ModelSelection,
    pub parent_thread_id: ThreadID,
    pub on_effect_error: Option<OnEffectErrorFn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failure {
    #[serde(rename = "_tag")]
    pub tag: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Target {
    #[serde(rename = "providerInstanceId", default)]
    pub provider_instance_id: String,
    #[serde(rename = "driverKind", default)]
    pub driver_kind: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub options: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct CapabilitiesInput {
    #[serde(default)]
    pub search: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DelegateTaskInput {
    #[serde(default)]
    pub task: String,
    #[serde(default)]
    pub target: Option<Target>,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub isolation: String,
    #[serde(default)]
    pub mode: String,
    #[serde(rename = "timeoutMs", default)]
    pub timeout_ms: i64,
    #[serde(rename = "clientRequestId", default)]
    pub client_request_id: String,
    #[serde(rename = "runtimeMode", default)]
    pub runtime_mode: String,
    #[serde(rename = "interactionMode", default)]
    pub interaction_mode: String,
    #[serde(rename = "parentThreadId", default)]
    pub parent_thread_id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct CreateThreadEntry {
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub target: Option<Target>,
    #[serde(rename = "runtimeMode", default)]
    pub runtime_mode: String,
    #[serde(rename = "interactionMode", default)]
    pub interaction_mode: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct CreateThreadsInput {
    #[serde(default)]
    pub threads: Vec<CreateThreadEntry>,
    #[serde(rename = "clientRequestId", default)]
    pub client_request_id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TaskInput {
    #[serde(rename = "taskId", default)]
    pub task_id: String,
    #[serde(rename = "parentThreadId", default)]
    pub parent_thread_id: String,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ThreadListInput {
    #[serde(rename = "projectId", default)]
    pub project_id: String,
    #[serde(rename = "allProjects", default)]
    pub all_projects: bool,
    #[serde(default)]
    pub statuses: Vec<String>,
    #[serde(rename = "titleContains", default)]
    pub title_contains: String,
    #[serde(rename = "includeSubagents", default)]
    pub include_subagents: bool,
    #[serde(default)]
    pub cursor: i64,
    #[serde(default)]
    pub limit: i64,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ThreadReadInput {
    #[serde(rename = "threadId", default)]
    pub thread_id: String,
    #[serde(rename = "itemId", default)]
    pub item_id: String,
    #[serde(rename = "textOffset", default)]
    pub text_offset: usize,
    #[serde(default)]
    pub view: String,
    #[serde(rename = "afterPosition", default)]
    pub after_position: i64,
    #[serde(default)]
    pub limit: usize,
    #[serde(rename = "runLimit", default)]
    pub run_limit: usize,
    #[serde(rename = "maxCharsPerItem", default)]
    pub max_chars_per_item: usize,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ThreadSendInput {
    #[serde(rename = "threadId", default)]
    pub thread_id: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub mode: String,
    #[serde(rename = "clientRequestId", default)]
    pub client_request_id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ThreadWaitInput {
    #[serde(rename = "threadId", default)]
    pub thread_id: String,
    #[serde(rename = "runId", default)]
    pub run_id: String,
    #[serde(rename = "timeoutMs", default)]
    pub timeout_ms: i64,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ThreadInterruptInput {
    #[serde(rename = "threadId", default)]
    pub thread_id: String,
    #[serde(rename = "runId", default)]
    pub run_id: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub stop: bool,
    #[serde(rename = "clientRequestId", default)]
    pub client_request_id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PendingRequestInput {
    #[serde(rename = "threadId", default)]
    pub thread_id: String,
    #[serde(default)]
    pub action: String,
    #[serde(rename = "requestId", default)]
    pub request_id: String,
    #[serde(default)]
    pub decision: Option<String>,
    #[serde(default)]
    pub feedback: Option<String>,
    #[serde(default)]
    pub answers: Option<BTreeMap<String, Value>>,
    #[serde(rename = "clientRequestId", default)]
    pub client_request_id: String,
}

#[derive(Clone)]
pub struct Server {
    orchestrator: Arc<Orchestrator>,
    effects: Option<Arc<dyn EffectExecutor>>,
    config: Config,
    pending: Arc<Mutex<HashMap<String, ExecutionContext>>>,
}

#[derive(Deserialize)]
struct RpcInRequest {
    #[allow(dead_code)]
    #[serde(default)]
    jsonrpc: String,
    #[serde(default)]
    id: Option<Value>,
    #[serde(default)]
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Serialize)]
struct RpcOutResponse {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcOutError>,
}

#[derive(Serialize)]
struct RpcOutError {
    code: i32,
    message: String,
}

impl Server {
    pub fn new(
        orchestrator: Arc<Orchestrator>,
        effects: Option<Arc<dyn EffectExecutor>>,
        config: Config,
    ) -> Self {
        Self {
            orchestrator,
            effects,
            config,
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn serve<R, W>(&self, ctx: &ExecutionContext, input: R, output: W) -> Result<(), Error>
    where
        R: Read,
        W: Write + Send + 'static,
    {
        let writer = Arc::new(Mutex::new(output));
        let mut reader = BufReader::new(input);
        let mut handles = Vec::new();
        let mut line = String::new();

        let loop_res = loop {
            line.clear();
            let n = match reader.read_line(&mut line) {
                Ok(n) => n,
                Err(e) => break Err(Error::provider(e.to_string())),
            };
            if n == 0 {
                break Ok(());
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let req: RpcInRequest = match serde_json::from_str(trimmed) {
                Ok(r) => r,
                Err(decode_err) => {
                    Self::write_response(
                        &writer,
                        &RpcOutResponse {
                            jsonrpc: "2.0",
                            id: None,
                            result: None,
                            error: Some(RpcOutError {
                                code: -32700,
                                message: decode_err.to_string(),
                            }),
                        },
                    );
                    continue;
                }
            };
            if req.method == "exit" {
                break Ok(());
            }
            let Some(req_id) = req.id.clone() else {
                self.handle_notification(&req);
                continue;
            };
            if req_id.is_null() {
                self.handle_notification(&req);
                continue;
            }

            let req_ctx = ctx.child();
            let key = request_key(&req_id);
            self.pending
                .lock()
                .unwrap()
                .insert(key.clone(), req_ctx.clone());

            let server = self.clone();
            let writer_clone = writer.clone();
            let handle = thread::spawn(move || {
                let (result, rpc_err) = server.handle(&req_ctx, req);
                req_ctx.cancel();
                server.pending.lock().unwrap().remove(&key);
                Self::write_response(
                    &writer_clone,
                    &RpcOutResponse {
                        jsonrpc: "2.0",
                        id: Some(req_id),
                        result: if rpc_err.is_some() { None } else { Some(result) },
                        error: rpc_err,
                    },
                );
            });
            handles.push(handle);
        };

        {
            let map = self.pending.lock().unwrap();
            for c in map.values() {
                c.cancel();
            }
        }
        for h in handles {
            let _ = h.join();
        }
        loop_res
    }

    fn write_response<W: Write>(writer: &Arc<Mutex<W>>, resp: &RpcOutResponse) {
        if let Ok(mut encoded) = serde_json::to_vec(resp) {
            encoded.push(b'\n');
            if let Ok(mut guard) = writer.lock() {
                let _ = guard.write_all(&encoded);
                let _ = guard.flush();
            }
        }
    }

    fn handle_notification(&self, req: &RpcInRequest) {
        if req.method != "notifications/cancelled" {
            return;
        }
        if let Some(req_id) = req.params.get("requestId") {
            let key = request_key(req_id);
            if let Some(ctx) = self.pending.lock().unwrap().get(&key) {
                ctx.cancel();
            }
        }
    }

    fn handle(&self, ctx: &ExecutionContext, req: RpcInRequest) -> (Value, Option<RpcOutError>) {
        match req.method.as_str() {
            "initialize" => (
                json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {
                        "name": "g0code-orchestrator",
                        "title": "g0code Orchestrator",
                        "version": "0.1.0"
                    }
                }),
                None,
            ),
            "ping" => (json!({}), None),
            "tools/list" => (json!({"tools": self.tools()}), None),
            "tools/call" => {
                #[derive(Default, Deserialize)]
                struct CallParams {
                    #[serde(default)]
                    name: String,
                    #[serde(default)]
                    arguments: Value,
                }
                let params: CallParams = match from_value_lenient(req.params) {
                    Ok(p) => p,
                    Err(err) => {
                        return (
                            Value::Null,
                            Some(RpcOutError {
                                code: -32602,
                                message: err.to_string(),
                            }),
                        )
                    }
                };
                match self.call_tool(ctx, &params.name, params.arguments) {
                    Ok(res) => (call_tool_result(&res, false), None),
                    Err(failure) => {
                        let val = serde_json::to_value(failure).unwrap_or(Value::Null);
                        (call_tool_result(&val, true), None)
                    }
                }
            }
            "shutdown" => (Value::Null, None),
            _ => (
                Value::Null,
                Some(RpcOutError {
                    code: -32601,
                    message: format!("method not found: {}", req.method),
                }),
            ),
        }
    }

    pub fn tools(&self) -> Vec<Tool> {
        vec![
            make_tool(
                "orchestrator_capabilities",
                "List providers, models, options, and runtime modes for delegate_task.",
                false,
                &["search"],
            ),
            make_tool(
                "delegate_task",
                "Start one child agent with only task; no parent history. Keep taskId for task_status and task_cancel.",
                false,
                &[
                    "task",
                    "target",
                    "title",
                    "role",
                    "mode",
                    "timeoutMs",
                    "clientRequestId",
                    "runtimeMode",
                    "interactionMode",
                    "parentThreadId",
                ],
            ),
            make_tool(
                "task_status",
                "Read one delegated task and its durable result.",
                true,
                &["taskId", "parentThreadId"],
            ),
            make_tool(
                "task_cancel",
                "Stop one delegated task and its child thread.",
                false,
                &["taskId", "reason", "parentThreadId"],
            ),
            make_tool(
                "create_threads",
                "Create one to twenty ordinary top-level threads in this checkout.",
                false,
                &["threads", "clientRequestId"],
            ),
            make_tool(
                "t3_thread_list",
                "List durable threads newest first with bounded offset pagination.",
                true,
                &[
                    "projectId",
                    "allProjects",
                    "statuses",
                    "titleContains",
                    "includeSubagents",
                    "cursor",
                    "limit",
                ],
            ),
            make_tool(
                "t3_thread_read",
                "Read durable thread state, recent runs, and timeline items.",
                true,
                &[
                    "threadId",
                    "itemId",
                    "textOffset",
                    "view",
                    "afterPosition",
                    "limit",
                    "runLimit",
                    "maxCharsPerItem",
                ],
            ),
            make_tool(
                "t3_thread_send",
                "Send or queue a follow-up message on an app-owned thread.",
                false,
                &["threadId", "message", "mode", "clientRequestId"],
            ),
            make_tool(
                "t3_thread_wait",
                "Wait for a selected run to settle. Timeout never cancels work.",
                true,
                &["threadId", "runId", "timeoutMs"],
            ),
            make_tool(
                "t3_thread_interrupt",
                "Interrupt an active run through the normal run.interrupt command.",
                false,
                &["threadId", "runId", "reason", "stop", "clientRequestId"],
            ),
            make_tool(
                "t3_pending_request",
                "List, read, or respond to pending provider requests on a thread.",
                false,
                &[
                    "threadId",
                    "action",
                    "requestId",
                    "decision",
                    "feedback",
                    "answers",
                    "clientRequestId",
                ],
            ),
        ]
    }

    pub fn call_tool(
        &self,
        ctx: &ExecutionContext,
        name: &str,
        raw: Value,
    ) -> Result<Value, Failure> {
        let raw = if raw.is_null() { json!({}) } else { raw };
        match name {
            "orchestrator_capabilities" => {
                let input: CapabilitiesInput = decode_input(raw)?;
                self.capabilities(input)
            }
            "create_threads" => {
                let input: CreateThreadsInput = decode_input(raw)?;
                self.create_threads(ctx, input)
            }
            "t3_thread_list" => {
                let input: ThreadListInput = decode_input(raw)?;
                self.thread_list(input)
            }
            "t3_thread_read" => {
                let input: ThreadReadInput = decode_input(raw)?;
                self.thread_read(input)
            }
            "t3_thread_send" => {
                let input: ThreadSendInput = decode_input(raw)?;
                self.thread_send(ctx, input)
            }
            "t3_thread_wait" => {
                let input: ThreadWaitInput = decode_input(raw)?;
                self.thread_wait(ctx, input)
            }
            "t3_thread_interrupt" => {
                let input: ThreadInterruptInput = decode_input(raw)?;
                self.thread_interrupt(ctx, input)
            }
            "t3_pending_request" => {
                let input: PendingRequestInput = decode_input(raw)?;
                self.pending_request(ctx, input)
            }
            "delegate_task" => {
                let input: DelegateTaskInput = decode_input(raw)?;
                self.delegate_task(ctx, input)
            }
            "task_status" => {
                let input: TaskInput = decode_input(raw)?;
                self.task_status(input, false)
            }
            "task_cancel" => {
                let input: TaskInput = decode_input(raw)?;
                self.task_cancel(ctx, input)
            }
            _ => Err(invalid_request(format!("unknown tool: {}", name))),
        }
    }

    fn run_effects(&self, effects: Vec<Effect>) {
        let Some(ref executor) = self.effects else {
            return;
        };
        for effect in effects {
            let exec = executor.clone();
            let on_err = self.config.on_effect_error.clone();
            thread::spawn(move || {
                let ctx = ExecutionContext::background();
                if let Err(err) = exec.execute(&ctx, &effect) {
                    if let Some(cb) = on_err {
                        cb(&effect, &err);
                    }
                }
            });
        }
    }

    fn capabilities(&self, input: CapabilitiesInput) -> Result<Value, Failure> {
        let mut providers = Vec::with_capacity(self.config.providers.len());
        let search = input.search.trim().to_lowercase();
        for provider in &self.config.providers {
            let models: Vec<String> = if !search.is_empty() {
                provider
                    .models
                    .iter()
                    .filter(|m| m.to_lowercase().contains(&search))
                    .cloned()
                    .collect()
            } else if provider.models.len() > 8 {
                provider.models[..8].to_vec()
            } else {
                provider.models.clone()
            };
            let omitted = provider.models.len().saturating_sub(models.len());
            providers.push(json!({
                "instanceId": provider.instance_id,
                "driverKind": provider.driver,
                "models": models,
                "available": provider.available,
                "canDelegate": provider.available,
                "runtimeModes": provider.runtime_modes,
                "omittedModels": omitted,
            }));
        }
        Ok(json!({
            "providers": providers,
            "features": {
                "polling": true,
                "cancellation": true,
                "batchThreadCreation": true,
            }
        }))
    }

    fn create_threads(
        &self,
        _ctx: &ExecutionContext,
        input: CreateThreadsInput,
    ) -> Result<Value, Failure> {
        if input.threads.is_empty() || input.threads.len() > 20 {
            return Err(invalid_request(
                "threads must contain between 1 and 20 entries",
            ));
        }
        let req_key = request_key_or_random(&input.client_request_id);
        let mut results = Vec::with_capacity(input.threads.len());
        for (index, entry) in input.threads.iter().enumerate() {
            let (target, runtime_mode, interaction_mode) = self.resolve_target(
                entry.target.as_ref(),
                &entry.runtime_mode,
                &entry.interaction_mode,
                None,
            )?;
            let (project_id, worktree) = self.default_workspace()?;
            let idx_str = index.to_string();
            let thread_id: ThreadID = scoped_id("thread:mcp", &req_key, &["thread", &idx_str]);
            let command_id: CommandID = scoped_id("command:mcp", &req_key, &["create", &idx_str]);
            let title = if !entry.title.is_empty() {
                entry.title.clone()
            } else if !entry.prompt.is_empty() {
                truncate(&entry.prompt, 80)
            } else {
                "MCP thread".to_string()
            };

            self.orchestrator
                .dispatch(Command {
                    command_type: COMMAND_THREAD_CREATE.to_string(),
                    command_id,
                    thread_id: thread_id.clone(),
                    payload: CommandPayload::CreateThread(CreateThread {
                        project_id,
                        title,
                        model_selection: target.clone(),
                        runtime_mode,
                        interaction_mode,
                        worktree_path: worktree,
                        created_by: ACTOR_AGENT.to_string(),
                        creation_source: CREATION_MCP.to_string(),
                        ..Default::default()
                    }),
                })
                .map_err(failure_from_error)?;

            if !entry.prompt.is_empty() {
                let message_id: MessageID =
                    scoped_id("message:mcp", &req_key, &["message", &idx_str]);
                let send_cmd_id: CommandID =
                    scoped_id("command:mcp", &req_key, &["send", &idx_str]);
                let result = self
                    .orchestrator
                    .dispatch(Command {
                        command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
                        command_id: send_cmd_id,
                        thread_id: thread_id.clone(),
                        payload: CommandPayload::DispatchMessage(DispatchMessage {
                            message_id,
                            text: entry.prompt.clone(),
                            attachments: Vec::new(),
                            dispatch_mode: DispatchMode {
                                mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                                ..Default::default()
                            },
                            created_by: ACTOR_AGENT.to_string(),
                            creation_source: CREATION_MCP.to_string(),
                            ..Default::default()
                        }),
                    })
                    .map_err(failure_from_error)?;
                self.run_effects(result.effects);
            }

            let snapshot = self
                .orchestrator
                .snapshot(&thread_id)
                .map_err(failure_from_error)?;
            let mut run_id: Option<RunID> = None;
            let mut status = "idle".to_string();
            if let Some(run) = latest_run(&snapshot.projection.runs) {
                run_id = Some(run.id.clone());
                status = run.status.clone();
            }
            results.push(json!({
                "threadId": thread_id,
                "runId": run_id,
                "status": status,
                "title": snapshot.projection.thread.title,
                "createdBy": ACTOR_AGENT,
                "creationSource": CREATION_MCP,
                "providerInstanceId": target.instance_id,
                "model": target.model,
            }));
        }
        Ok(json!({"threads": results}))
    }

    fn thread_list(&self, input: ThreadListInput) -> Result<Value, Failure> {
        let snapshots = self.orchestrator.list().map_err(failure_from_error)?;
        let mut project_id = input.project_id.clone();
        if project_id.is_empty() && !input.all_projects {
            project_id = self.config.default_project_id.clone();
            if !self.config.parent_thread_id.is_empty() {
                if let Ok(snap) = self.orchestrator.snapshot(&self.config.parent_thread_id) {
                    project_id = snap.projection.thread.project_id;
                }
            }
        }

        let mut threads_with_ts: Vec<(DateTime<Utc>, Value)> = Vec::new();
        let title_filter = input.title_contains.to_lowercase();
        for snapshot in snapshots {
            let thread = snapshot.projection.thread;
            if thread.deleted_at.is_some()
                || (!input.include_subagents
                    && thread.lineage.relationship_to_parent.as_deref() == Some("subagent"))
                || (!input.all_projects && !project_id.is_empty() && thread.project_id != project_id)
                || (!title_filter.is_empty()
                    && !thread.title.to_lowercase().contains(&title_filter))
            {
                continue;
            }
            let mut status = "idle".to_string();
            let mut latest_run_id: Option<RunID> = None;
            if let Some(latest) = latest_run(&snapshot.projection.runs) {
                status = latest.status.clone();
                latest_run_id = Some(latest.id.clone());
            }
            if !input.statuses.is_empty() && !input.statuses.contains(&status) {
                continue;
            }
            let updated_at = thread.updated_at;
            threads_with_ts.push((
                updated_at,
                json!({
                    "threadId": thread.id,
                    "projectId": thread.project_id,
                    "title": thread.title,
                    "createdBy": thread.created_by,
                    "creationSource": thread.creation_source,
                    "status": status,
                    "latestRunId": latest_run_id,
                    "providerInstanceId": thread.provider_instance_id,
                    "model": thread.model_selection.model,
                    "runtimeMode": thread.runtime_mode,
                    "interactionMode": thread.interaction_mode,
                    "parentThreadId": thread.lineage.parent_thread_id,
                    "relationshipToParent": thread.lineage.relationship_to_parent,
                    "itemCount": snapshot.projection.turn_items.len(),
                    "createdAt": thread.created_at,
                    "updatedAt": thread.updated_at,
                }),
            ));
        }

        threads_with_ts.sort_by(|a, b| b.0.cmp(&a.0));
        let threads: Vec<Value> = threads_with_ts.into_iter().map(|(_, v)| v).collect();
        let total = threads.len();
        let mut cursor = input.cursor.max(0) as usize;
        if cursor > total {
            cursor = total;
        }
        let mut limit = if input.limit == 0 {
            50
        } else {
            input.limit.max(0) as usize
        };
        if limit > 100 {
            limit = 100;
        }
        let end = total.min(cursor + limit);
        let next_cursor: Option<usize> = if end < total { Some(end) } else { None };
        let project_result: Option<ProjectID> = if !input.all_projects {
            Some(project_id)
        } else {
            None
        };
        Ok(json!({
            "projectId": project_result,
            "currentThreadId": self.config.parent_thread_id,
            "threads": &threads[cursor..end],
            "nextCursor": next_cursor,
            "total": total,
        }))
    }

    fn thread_read(&self, input: ThreadReadInput) -> Result<Value, Failure> {
        if input.thread_id.is_empty() {
            return Err(invalid_request("threadId is required"));
        }
        let snapshot = self
            .orchestrator
            .snapshot(&input.thread_id)
            .map_err(failure_from_error)?;
        let projection = snapshot.projection;
        let thread = &projection.thread;

        let mut status = "idle".to_string();
        let mut latest_run_id: Option<RunID> = None;
        if let Some(latest) = latest_run(&projection.runs) {
            status = latest.status.clone();
            latest_run_id = Some(latest.id.clone());
        }
        let active_run_id: Option<RunID> = projection.active_run().map(|r| r.id);
        let pending = projection
            .runtime_requests
            .iter()
            .filter(|r| r.status == REQUEST_PENDING)
            .count();

        let mut run_limit = if input.run_limit == 0 {
            10
        } else {
            input.run_limit
        };
        run_limit = run_limit.min(50);
        let runs_slice = if projection.runs.len() > run_limit {
            &projection.runs[projection.runs.len() - run_limit..]
        } else {
            &projection.runs[..]
        };
        let recent_runs: Vec<Value> = runs_slice
            .iter()
            .map(|run| {
                json!({
                    "runId": run.id,
                    "ordinal": run.ordinal,
                    "status": run.status,
                    "providerInstanceId": run.provider_instance_id,
                    "model": run.model_selection.model,
                    "requestedAt": run.requested_at,
                    "startedAt": run.started_at,
                    "completedAt": run.completed_at,
                })
            })
            .collect();

        let items: Vec<&TurnItem> = if !input.item_id.is_empty() {
            let target_id: TurnItemID = input.item_id.clone();
            projection
                .turn_items
                .iter()
                .filter(|item| item.id == target_id)
                .collect()
        } else {
            projection.turn_items.iter().collect()
        };

        let mut filtered: Vec<&TurnItem> = Vec::new();
        for item in items {
            if input.view != "activity"
                && item.item_type != "user_message"
                && item.item_type != "assistant_message"
                && item.item_type != "proposed_plan"
            {
                continue;
            }
            if input.item_id.is_empty() && item.ordinal <= input.after_position {
                continue;
            }
            filtered.push(item);
        }

        let mut limit = if input.limit == 0 { 50 } else { input.limit };
        limit = limit.min(100);
        let has_more = filtered.len() > limit;
        if has_more {
            filtered.truncate(limit);
        }

        let mut max_chars = if input.max_chars_per_item == 0 {
            50_000
        } else {
            input.max_chars_per_item
        };
        max_chars = max_chars.min(50_000);

        let mut result_items = Vec::with_capacity(filtered.len());
        for item in &filtered {
            let offset = if !input.item_id.is_empty() {
                input.text_offset
            } else {
                0
            };
            let mut chars: Vec<char> = item.text.chars().collect();
            let clamped_offset = offset.min(chars.len());
            chars = chars[clamped_offset..].to_vec();
            let truncated = chars.len() > max_chars;
            if truncated {
                chars.truncate(max_chars);
            }
            let next_offset: Option<usize> = if truncated {
                Some(clamped_offset + max_chars)
            } else {
                None
            };
            let title: Option<&str> = if item.title.is_empty() {
                None
            } else {
                Some(&item.title)
            };
            let text_result: Option<String> = if item.text.is_empty() {
                None
            } else {
                Some(chars.into_iter().collect())
            };
            let msg_id: Option<&str> = if item.message_id.is_empty() {
                None
            } else {
                Some(&item.message_id)
            };
            let created_by: Option<&str> = if item.created_by.is_empty() {
                None
            } else {
                Some(&item.created_by)
            };
            let creation_source: Option<&str> = if item.creation_source.is_empty() {
                None
            } else {
                Some(&item.creation_source)
            };
            result_items.push(json!({
                "position": item.ordinal,
                "visibility": "local",
                "sourceThreadId": thread.id,
                "itemId": item.id,
                "runId": item.run_id,
                "messageId": msg_id,
                "createdBy": created_by,
                "creationSource": creation_source,
                "type": item.item_type,
                "status": item.status,
                "title": title,
                "text": text_result,
                "textTruncated": truncated,
                "nextTextOffset": next_offset,
                "updatedAt": item.updated_at,
            }));
        }

        let next_position: Option<i64> = filtered.last().map(|item| item.ordinal);
        Ok(json!({
            "thread": {
                "threadId": thread.id,
                "projectId": thread.project_id,
                "title": thread.title,
                "createdBy": thread.created_by,
                "creationSource": thread.creation_source,
                "status": status,
                "latestRunId": latest_run_id,
                "activeRunId": active_run_id,
                "providerInstanceId": thread.provider_instance_id,
                "model": thread.model_selection.model,
                "runtimeMode": thread.runtime_mode,
                "interactionMode": thread.interaction_mode,
                "branch": thread.branch,
                "worktreePath": thread.worktree_path,
                "parentThreadId": thread.lineage.parent_thread_id,
                "relationshipToParent": thread.lineage.relationship_to_parent,
                "runCount": projection.runs.len(),
                "itemCount": projection.turn_items.len(),
                "pendingRequestCount": pending,
                "archived": thread.archived_at.is_some(),
                "createdAt": thread.created_at,
                "updatedAt": thread.updated_at,
            },
            "recentRuns": recent_runs,
            "items": result_items,
            "nextPosition": next_position,
            "hasMore": has_more,
        }))
    }

    fn thread_send(&self, _ctx: &ExecutionContext, input: ThreadSendInput) -> Result<Value, Failure> {
        if input.thread_id.is_empty() || input.message.trim().is_empty() {
            return Err(invalid_request("threadId and message are required"));
        }
        let key = request_key_or_random(&input.client_request_id);
        let thread_id: ThreadID = input.thread_id;
        let snapshot = self
            .orchestrator
            .snapshot(&thread_id)
            .map_err(failure_from_error)?;

        let active_run = snapshot.projection.active_run();
        let active = active_run.is_some();
        let (mode, delivery) = match input.mode.as_str() {
            "" | "auto" => {
                if active {
                    (DISPATCH_QUEUE_AFTER_ACTIVE, "queued")
                } else {
                    (DISPATCH_START_IMMEDIATELY, "started")
                }
            }
            "queue" => (DISPATCH_QUEUE_AFTER_ACTIVE, "queued"),
            "steer" => (DISPATCH_STEER_ACTIVE, "steered"),
            "restart" => (DISPATCH_RESTART_ACTIVE, "restarted"),
            _ => return Err(invalid_request("unknown send mode")),
        };

        let message_id: MessageID = scoped_id("message:mcp", &key, &["message"]);
        let command_id: CommandID = scoped_id("command:mcp", &key, &["send"]);

        if let Some(run) = active_run {
            if mode == DISPATCH_STEER_ACTIVE || mode == DISPATCH_RESTART_ACTIVE {
                let result = self
                    .orchestrator
                    .dispatch(Command {
                        command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
                        command_id,
                        thread_id: thread_id.clone(),
                        payload: CommandPayload::DispatchMessage(DispatchMessage {
                            message_id: message_id.clone(),
                            text: input.message,
                            attachments: Vec::new(),
                            dispatch_mode: DispatchMode {
                                mode_type: mode.to_string(),
                                target_run_id: run.id.clone(),
                                ..Default::default()
                            },
                            created_by: ACTOR_AGENT.to_string(),
                            creation_source: CREATION_MCP.to_string(),
                            ..Default::default()
                        }),
                    })
                    .map_err(failure_from_error)?;
                self.run_effects(result.effects);
                return Ok(json!({
                    "threadId": thread_id,
                    "messageId": message_id,
                    "runId": run.id,
                    "status": run.status,
                    "delivery": delivery,
                }));
            }
        }

        let result = self
            .orchestrator
            .dispatch(Command {
                command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
                command_id,
                thread_id: thread_id.clone(),
                payload: CommandPayload::DispatchMessage(DispatchMessage {
                    message_id: message_id.clone(),
                    text: input.message,
                    attachments: Vec::new(),
                    dispatch_mode: DispatchMode {
                        mode_type: mode.to_string(),
                        ..Default::default()
                    },
                    created_by: ACTOR_AGENT.to_string(),
                    creation_source: CREATION_MCP.to_string(),
                    ..Default::default()
                }),
            })
            .map_err(failure_from_error)?;
        self.run_effects(result.effects);
        let updated = self
            .orchestrator
            .snapshot(&thread_id)
            .map_err(failure_from_error)?;
        let run = latest_run(&updated.projection.runs).cloned().unwrap_or_default();
        Ok(json!({
            "threadId": thread_id,
            "messageId": message_id,
            "runId": run.id,
            "status": run.status,
            "delivery": delivery,
        }))
    }

    fn thread_wait(&self, ctx: &ExecutionContext, input: ThreadWaitInput) -> Result<Value, Failure> {
        let snapshot = self
            .orchestrator
            .snapshot(&input.thread_id)
            .map_err(failure_from_error)?;
        let run_id: Option<RunID> = if !input.run_id.is_empty() {
            Some(input.run_id.clone())
        } else {
            latest_run(&snapshot.projection.runs).map(|r| r.id.clone())
        };
        let Some(run_id) = run_id else {
            return Ok(json!({
                "threadId": input.thread_id,
                "runId": Value::Null,
                "status": "idle",
                "timedOut": false,
            }));
        };

        let timeout = if input.timeout_ms <= 0 {
            Duration::from_secs(600)
        } else {
            Duration::from_millis(input.timeout_ms as u64)
        };
        let deadline = Instant::now() + timeout;

        loop {
            let snapshot = self
                .orchestrator
                .snapshot(&input.thread_id)
                .map_err(failure_from_error)?;
            let Some(run) = snapshot.projection.run(&run_id) else {
                return Err(Failure {
                    tag: "OrchestratorMcpFailure".to_string(),
                    code: "run_not_found".to_string(),
                    message: format!("run not found: {}", run_id),
                });
            };
            if run_status_is_terminal(&run.status) {
                return Ok(json!({
                    "threadId": input.thread_id,
                    "runId": run.id,
                    "status": run.status,
                    "timedOut": false,
                }));
            }
            if ctx.is_cancelled() {
                return Err(failure_from_error(Error::provider("context canceled")));
            }
            if Instant::now() >= deadline {
                return Ok(json!({
                    "threadId": input.thread_id,
                    "runId": run.id,
                    "status": run.status,
                    "timedOut": true,
                }));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn thread_interrupt(
        &self,
        _ctx: &ExecutionContext,
        input: ThreadInterruptInput,
    ) -> Result<Value, Failure> {
        let thread_id: ThreadID = input.thread_id;
        let snapshot = self
            .orchestrator
            .snapshot(&thread_id)
            .map_err(failure_from_error)?;
        let run: Option<Run> = if !input.run_id.is_empty() && !input.stop {
            snapshot.projection.run(&input.run_id)
        } else {
            snapshot.projection.active_run()
        };
        let Some(run) = run else {
            return Ok(json!({
                "threadId": thread_id,
                "runId": Value::Null,
                "status": "no_active_run",
            }));
        };
        if run.id.is_empty() {
            return Ok(json!({
                "threadId": thread_id,
                "runId": Value::Null,
                "status": "no_active_run",
            }));
        }
        if run_status_is_terminal(&run.status) {
            return Ok(json!({
                "threadId": thread_id,
                "runId": run.id,
                "status": run.status,
            }));
        }
        let key = request_key_or_random(&input.client_request_id);
        let result = self
            .orchestrator
            .dispatch(Command {
                command_type: COMMAND_RUN_INTERRUPT.to_string(),
                command_id: scoped_id("command:mcp", &key, &["interrupt"]),
                thread_id: thread_id.clone(),
                payload: CommandPayload::InterruptRun(InterruptRun {
                    run_id: run.id.clone(),
                    reason: input.reason,
                    hold_queue: input.stop,
                }),
            })
            .map_err(failure_from_error)?;
        self.run_effects(result.effects);
        Ok(json!({
            "threadId": thread_id,
            "runId": run.id,
            "status": "interrupt_requested",
        }))
    }

    fn pending_request(
        &self,
        _ctx: &ExecutionContext,
        input: PendingRequestInput,
    ) -> Result<Value, Failure> {
        let snapshot = self
            .orchestrator
            .snapshot(&input.thread_id)
            .map_err(failure_from_error)?;
        match input.action.as_str() {
            "list" => {
                let requests: Vec<RuntimeRequest> = snapshot
                    .projection
                    .runtime_requests
                    .into_iter()
                    .filter(|r| r.status == REQUEST_PENDING)
                    .collect();
                Ok(json!({
                    "threadId": input.thread_id,
                    "requests": requests,
                }))
            }
            "read" => {
                let req_id: RuntimeRequestID = input.request_id.clone();
                let Some(request) = snapshot.projection.runtime_request(&req_id) else {
                    return Err(Failure {
                        tag: "OrchestratorMcpFailure".to_string(),
                        code: "invalid_request".to_string(),
                        message: format!("request not found: {}", input.request_id),
                    });
                };
                Ok(json!({
                    "threadId": input.thread_id,
                    "request": request,
                }))
            }
            "respond" => {
                let key = request_key_or_random(&input.client_request_id);
                let result = self
                    .orchestrator
                    .dispatch(Command {
                        command_type: COMMAND_RUNTIME_REQUEST_RESPOND.to_string(),
                        command_id: scoped_id("command:mcp", &key, &["respond"]),
                        thread_id: input.thread_id.clone(),
                        payload: CommandPayload::RuntimeRequestResponse(RuntimeRequestResponse {
                            request_id: input.request_id.clone(),
                            decision: input.decision,
                            feedback: input.feedback,
                            answers: input.answers.unwrap_or_default(),
                        }),
                    })
                    .map_err(failure_from_error)?;
                self.run_effects(result.effects);
                Ok(json!({
                    "threadId": input.thread_id,
                    "requestId": input.request_id,
                    "sequence": result.sequence,
                }))
            }
            _ => Err(invalid_request("action must be list, read, or respond")),
        }
    }

    fn delegate_task(
        &self,
        ctx: &ExecutionContext,
        input: DelegateTaskInput,
    ) -> Result<Value, Failure> {
        if input.task.trim().is_empty() {
            return Err(invalid_request("task is required"));
        }
        if input.isolation == "worktree" {
            return Err(Failure {
                tag: "OrchestratorMcpFailure".to_string(),
                code: "invalid_request".to_string(),
                message: "worktree isolation requires the launch adapter".to_string(),
            });
        }
        let parent_id = if !self.config.parent_thread_id.is_empty() {
            self.config.parent_thread_id.clone()
        } else {
            input.parent_thread_id.clone()
        };
        if parent_id.is_empty() {
            return Err(Failure {
                tag: "OrchestratorMcpFailure".to_string(),
                code: "parent_not_active".to_string(),
                message: "delegate_task requires parentThreadId in standalone mode".to_string(),
            });
        }
        let parent_snapshot = self
            .orchestrator
            .snapshot(&parent_id)
            .map_err(failure_from_error)?;
        let Some(parent_run) = parent_snapshot.projection.active_run() else {
            return Err(Failure {
                tag: "OrchestratorMcpFailure".to_string(),
                code: "parent_not_active".to_string(),
                message: "the parent run is not active".to_string(),
            });
        };
        let Some(ref root_node_id) = parent_run.root_node_id else {
            return Err(Failure {
                tag: "OrchestratorMcpFailure".to_string(),
                code: "parent_not_active".to_string(),
                message: "the parent run is not active".to_string(),
            });
        };

        let (target, runtime_mode, interaction_mode) = self.resolve_target(
            input.target.as_ref(),
            &input.runtime_mode,
            &input.interaction_mode,
            Some(&parent_id),
        )?;
        let key = request_key_or_random(&input.client_request_id);
        let command_id: CommandID = scoped_id("command:mcp", &key, &["delegate"]);
        let mut task = input.task.clone();
        let role_instr = role_instruction(&input.role);
        if !role_instr.is_empty() {
            task = format!("{}\n\n{}", role_instr, task);
        }

        let result = self
            .orchestrator
            .dispatch(Command {
                command_type: COMMAND_DELEGATED_TASK_REQUEST.to_string(),
                command_id: command_id.clone(),
                thread_id: parent_id.clone(),
                payload: CommandPayload::DelegateTask(DelegateTask {
                    parent_thread_id: parent_id.clone(),
                    parent_run_id: parent_run.id.clone(),
                    parent_node_id: root_node_id.clone(),
                    task,
                    title: input.title,
                    model_selection: target,
                    runtime_mode,
                    interaction_mode,
                    created_by: ACTOR_AGENT.to_string(),
                    creation_source: CREATION_MCP.to_string(),
                }),
            })
            .map_err(failure_from_error)?;
        self.run_effects(result.effects);

        let task_id: NodeID = derive_delegated_task_node_id(&command_id);
        let mut wait_timed_out = false;
        if input.mode == "wait" {
            wait_timed_out =
                self.wait_for_delegated_task(ctx, &parent_id, &task_id, input.timeout_ms)?;
        }
        let mut status = self.task_status(
            TaskInput {
                task_id,
                parent_thread_id: parent_id,
                reason: String::new(),
            },
            input.mode == "wait",
        )?;
        if let Some(obj) = status.as_object_mut() {
            obj.insert("waitTimedOut".to_string(), Value::Bool(wait_timed_out));
        }
        Ok(status)
    }

    fn wait_for_delegated_task(
        &self,
        ctx: &ExecutionContext,
        parent_id: &ThreadID,
        task_id: &NodeID,
        timeout_ms: i64,
    ) -> Result<bool, Failure> {
        let timeout = if timeout_ms <= 0 {
            Duration::from_secs(600)
        } else {
            Duration::from_millis(timeout_ms as u64)
        };
        let deadline = Instant::now() + timeout;
        loop {
            let result = self.task_status(
                TaskInput {
                    task_id: task_id.clone(),
                    parent_thread_id: parent_id.clone(),
                    reason: String::new(),
                },
                true,
            )?;
            if let Some(values) = result.as_object() {
                if values.get("workState").and_then(|v| v.as_str()) == Some("result_available")
                    || values.get("hasPendingQuestion").and_then(|v| v.as_bool()) == Some(true)
                    || values.get("pendingApproval").is_some_and(|v| !v.is_null())
                {
                    return Ok(false);
                }
            }
            if ctx.is_cancelled() {
                return Err(failure_from_error(Error::provider("context canceled")));
            }
            if Instant::now() >= deadline {
                return Ok(true);
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn task_status(&self, input: TaskInput, waiting: bool) -> Result<Value, Failure> {
        let parent_id = if !self.config.parent_thread_id.is_empty() {
            self.config.parent_thread_id.clone()
        } else {
            input.parent_thread_id.clone()
        };
        let parent_snapshot = self
            .orchestrator
            .snapshot(&parent_id)
            .map_err(failure_from_error)?;
        let task: Option<&Subagent> = parent_snapshot
            .projection
            .subagents
            .iter()
            .find(|s| s.id == input.task_id);
        let Some(task) = task else {
            return Err(Failure {
                tag: "OrchestratorMcpFailure".to_string(),
                code: "task_not_found".to_string(),
                message: format!("task not found: {}", input.task_id),
            });
        };
        let Some(ref child_thread_id) = task.child_thread_id else {
            return Err(Failure {
                tag: "OrchestratorMcpFailure".to_string(),
                code: "task_not_found".to_string(),
                message: "task has no local child thread".to_string(),
            });
        };
        let child_snapshot = self
            .orchestrator
            .snapshot(child_thread_id)
            .map_err(failure_from_error)?;

        let mut child_run_id: Option<RunID> = None;
        let mut status = task.status.clone();
        if let Some(first_run) = child_snapshot.projection.runs.first() {
            child_run_id = Some(first_run.id.clone());
            status = first_run.status.clone();
        }
        let work_state = if task.completed_at.is_some() {
            "result_available"
        } else {
            "working"
        };
        let mut has_pending = false;
        let mut latest_terminal_run_id: Option<RunID> = None;
        let mut latest_terminal_status: Option<String> = None;
        let mut latest_terminal_summary: Option<String> = None;
        for run in &child_snapshot.projection.runs {
            if run_status_is_active(&run.status) || run.status == RUN_QUEUED {
                has_pending = true;
            }
            if run_status_is_terminal(&run.status) {
                latest_terminal_run_id = Some(run.id.clone());
                latest_terminal_status = Some(run.status.clone());
                latest_terminal_summary =
                    latest_assistant(&child_snapshot.projection.messages, &run.id);
            }
        }
        let mut result_transfer_id: Option<ContextTransferID> = None;
        for transfer in &parent_snapshot.projection.context_transfers {
            if transfer.transfer_type == "subagent_result"
                && transfer.source_thread_id == *child_thread_id
            {
                result_transfer_id = Some(transfer.id.clone());
            }
        }
        let mut has_question = false;
        let mut pending_approval = Value::Null;
        for request in &child_snapshot.projection.runtime_requests {
            if request.status != REQUEST_PENDING {
                continue;
            }
            if request.kind == REQUEST_USER_INPUT {
                has_question = true;
            } else {
                pending_approval = json!({
                    "requestId": request.id,
                    "kind": request.kind,
                    "runId": child_run_id,
                });
            }
        }
        let mut result = json!({
            "taskId": task.id,
            "childThreadId": task.child_thread_id,
            "childRunId": child_run_id,
            "childNodeId": task.id,
            "status": status,
            "workState": work_state,
            "hasPendingChildRuns": has_pending,
            "providerInstanceId": task.provider_instance_id,
            "model": task.model,
            "summary": task.result,
            "resultContextTransferId": result_transfer_id,
            "latestTerminalRunId": latest_terminal_run_id,
            "latestTerminalStatus": latest_terminal_status,
            "latestTerminalSummary": latest_terminal_summary,
            "hasPendingQuestion": has_question,
            "pendingApproval": pending_approval,
            "waitTimedOut": false,
        });
        if work_state == "working" && !waiting {
            if let Some(obj) = result.as_object_mut() {
                obj.insert(
                    "next".to_string(),
                    Value::String(RUNNING_TASK_NEXT.to_string()),
                );
            }
        }
        Ok(result)
    }

    fn task_cancel(&self, _ctx: &ExecutionContext, input: TaskInput) -> Result<Value, Failure> {
        let status = self.task_status(input.clone(), false)?;
        let child_thread_id = status
            .get("childThreadId")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let snapshot = self
            .orchestrator
            .snapshot(&child_thread_id)
            .map_err(failure_from_error)?;
        let key = request_key_or_random("");
        let mut changed = false;
        for run in &snapshot.projection.runs {
            if run_status_is_active(&run.status) {
                let result = self
                    .orchestrator
                    .dispatch(Command {
                        command_type: COMMAND_RUN_INTERRUPT.to_string(),
                        command_id: scoped_id("command:mcp", &key, &["cancel", &run.id]),
                        thread_id: child_thread_id.clone(),
                        payload: CommandPayload::InterruptRun(InterruptRun {
                            run_id: run.id.clone(),
                            reason: input.reason.clone(),
                            hold_queue: true,
                        }),
                    })
                    .map_err(failure_from_error)?;
                self.run_effects(result.effects);
                changed = true;
            } else if run.status == RUN_QUEUED {
                self.orchestrator
                    .dispatch(Command {
                        command_type: COMMAND_QUEUED_RUN_CANCEL.to_string(),
                        command_id: scoped_id("command:mcp", &key, &["cancel-queued", &run.id]),
                        thread_id: child_thread_id.clone(),
                        payload: CommandPayload::CancelQueuedRun(CancelQueuedRun {
                            run_id: run.id.clone(),
                        }),
                    })
                    .map_err(failure_from_error)?;
                changed = true;
            }
        }
        if !changed {
            return Ok(json!({
                "taskId": input.task_id,
                "childThreadId": child_thread_id,
                "status": status.get("status").cloned().unwrap_or(Value::Null),
            }));
        }
        Ok(json!({
            "taskId": input.task_id,
            "childThreadId": child_thread_id,
            "status": "interrupted",
        }))
    }

    fn resolve_target(
        &self,
        target: Option<&Target>,
        runtime_value: &str,
        interaction_value: &str,
        parent_override: Option<&str>,
    ) -> Result<(ModelSelection, RuntimeMode, InteractionMode), Failure> {
        let parent_id = parent_override.unwrap_or(&self.config.parent_thread_id);
        let mut model = self.config.default_target.clone();
        let mut runtime_mode: RuntimeMode = RUNTIME_FULL_ACCESS.to_string();
        let mut interaction_mode: InteractionMode = INTERACTION_DEFAULT.to_string();

        if !parent_id.is_empty() {
            if let Ok(snapshot) = self.orchestrator.snapshot(parent_id) {
                model = snapshot.projection.thread.model_selection;
                runtime_mode = snapshot.projection.thread.runtime_mode;
                interaction_mode = snapshot.projection.thread.interaction_mode;
            }
        }
        if let Some(target) = target {
            if !target.provider_instance_id.is_empty() {
                model.instance_id = target.provider_instance_id.clone();
            } else if !target.driver_kind.is_empty() {
                for provider in &self.config.providers {
                    if provider.driver == target.driver_kind && provider.available {
                        model.instance_id = provider.instance_id.clone();
                        break;
                    }
                }
            }
            if !target.model.is_empty() {
                model.model = target.model.clone();
            }
            if let Some(ref opts) = target.options {
                model.options = opts.clone();
            }
        }
        if model.instance_id.is_empty() || model.model.is_empty() {
            return Err(Failure {
                tag: "OrchestratorMcpFailure".to_string(),
                code: "provider_unavailable".to_string(),
                message: "no default provider/model configured".to_string(),
            });
        }
        if !self.config.providers.is_empty() {
            let provider = self
                .config
                .providers
                .iter()
                .rev()
                .find(|p| p.instance_id == model.instance_id);
            let Some(provider) = provider else {
                return Err(Failure {
                    tag: "OrchestratorMcpFailure".to_string(),
                    code: "provider_unavailable".to_string(),
                    message: format!("provider is unavailable: {}", model.instance_id),
                });
            };
            if !provider.available {
                return Err(Failure {
                    tag: "OrchestratorMcpFailure".to_string(),
                    code: "provider_unavailable".to_string(),
                    message: format!("provider is unavailable: {}", model.instance_id),
                });
            }
        }
        if !runtime_value.is_empty() && runtime_value != "inherit" {
            runtime_mode = runtime_value.to_string();
        }
        if !interaction_value.is_empty() && interaction_value != "inherit" {
            interaction_mode = interaction_value.to_string();
        }
        if !parent_id.is_empty() {
            if let Ok(snapshot) = self.orchestrator.snapshot(parent_id) {
                if runtime_mode_above(&runtime_mode, &snapshot.projection.thread.runtime_mode) {
                    return Err(Failure {
                        tag: "OrchestratorMcpFailure".to_string(),
                        code: "runtime_mode_escalation_denied".to_string(),
                        message: "target runtime mode exceeds the caller".to_string(),
                    });
                }
                if snapshot.projection.thread.interaction_mode == INTERACTION_PLAN
                    && interaction_mode == INTERACTION_DEFAULT
                {
                    return Err(Failure {
                        tag: "OrchestratorMcpFailure".to_string(),
                        code: "interaction_mode_escalation_denied".to_string(),
                        message: "plan caller cannot launch default-mode work".to_string(),
                    });
                }
            }
        }
        Ok((model, runtime_mode, interaction_mode))
    }

    fn default_workspace(&self) -> Result<(ProjectID, Option<String>), Failure> {
        if !self.config.parent_thread_id.is_empty() {
            if let Ok(snapshot) = self.orchestrator.snapshot(&self.config.parent_thread_id) {
                return Ok((
                    snapshot.projection.thread.project_id,
                    snapshot.projection.thread.worktree_path,
                ));
            }
        }
        if self.config.default_project_id.is_empty() {
            return Err(invalid_request(
                "projectId is required outside a parent thread",
            ));
        }
        Ok((
            self.config.default_project_id.clone(),
            self.config.default_worktree_path.clone(),
        ))
    }
}

impl std::fmt::Display for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "g0code MCP server with {} providers",
            self.config.providers.len()
        )
    }
}

fn make_tool(name: &str, description: &str, read_only: bool, properties: &[&str]) -> Tool {
    let mut property_map = serde_json::Map::with_capacity(properties.len());
    for &prop in properties {
        property_map.insert(prop.to_string(), json!({}));
    }
    Tool {
        name: name.to_string(),
        description: description.to_string(),
        input_schema: json!({
            "type": "object",
            "properties": property_map,
            "additionalProperties": true,
        }),
        annotations: Some(json!({
            "readOnlyHint": read_only,
            "destructiveHint": !read_only,
            "idempotentHint": read_only,
        })),
    }
}

fn decode_input<T: serde::de::DeserializeOwned>(raw: Value) -> Result<T, Failure> {
    from_value_lenient(raw).map_err(|e| invalid_request(e.to_string()))
}

fn call_tool_result(value: &Value, is_error: bool) -> Value {
    let text = serde_json::to_string(value).unwrap_or_default();
    json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": value,
        "isError": is_error,
    })
}

fn request_key(id: &Value) -> String {
    match id {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn invalid_request(detail: impl Into<String>) -> Failure {
    Failure {
        tag: "OrchestratorMcpFailure".to_string(),
        code: "invalid_request".to_string(),
        message: detail.into(),
    }
}

fn failure_from_error(err: Error) -> Failure {
    let code = if err.is(ErrorKind::ThreadNotFound) {
        "thread_not_found"
    } else if err.is(ErrorKind::RunNotInterruptible) {
        "thread_not_interruptible"
    } else if err.is(ErrorKind::UnsupportedCapability) {
        "provider_unavailable"
    } else if err.is(ErrorKind::InvalidCommand) {
        "invalid_request"
    } else {
        "orchestration_error"
    };
    Failure {
        tag: "OrchestratorMcpFailure".to_string(),
        code: code.to_string(),
        message: err.to_string(),
    }
}

fn latest_run(runs: &[Run]) -> Option<&Run> {
    runs.last()
}

fn scoped_id(prefix: &str, key: &str, parts: &[&str]) -> String {
    let mut joined = String::from(key);
    for p in parts {
        joined.push(':');
        joined.push_str(p);
    }
    let hash = Sha256::digest(joined.as_bytes());
    format!("{}:{}", prefix, hex::encode(&hash[..16]))
}

fn request_key_or_random(value: &str) -> String {
    if !value.is_empty() {
        return value.to_string();
    }
    new_uuid_v4()
        .unwrap_or_else(|_| "random-key".to_string())
        .replace('-', "")
}

fn truncate(value: &str, limit: usize) -> String {
    let chars: Vec<char> = value.trim().chars().collect();
    if chars.len() <= limit {
        chars.into_iter().collect()
    } else {
        chars[..limit].into_iter().collect()
    }
}

fn role_instruction(role: &str) -> String {
    match role {
        "" => String::new(),
        "implementation" => "Role: implement the requested change and verify it.".to_string(),
        "research" => "Role: research the question and report evidence and uncertainty.".to_string(),
        "review" => "Role: review the work critically and prioritize concrete findings.".to_string(),
        "design" => "Role: produce a coherent design with tradeoffs.".to_string(),
        "test" => "Role: build and run focused verification.".to_string(),
        "general" => "Role: complete the task using the available tools.".to_string(),
        other => format!("Role: {}", other),
    }
}

fn latest_assistant(messages: &[ConversationMessage], run_id: &RunID) -> Option<String> {
    let mut result = None;
    for msg in messages {
        if msg.run_id.as_deref() == Some(run_id.as_str()) && msg.role == MESSAGE_ASSISTANT {
            result = Some(msg.text.clone());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Condvar;
    use tempfile::TempDir;

    use crate::orchestration::cli::{CodexAdapter, Executor, ReplayAdapter};
    use crate::orchestration::provider::{
        IngestInput, ProviderEvent, PROVIDER_EVENT_RUNTIME_REQUEST_UPDATED,
    };
    use crate::orchestration::store::MemoryStore;
    use crate::orchestration::types::ResponseCapability;

    struct MemPipeState {
        buf: VecDeque<u8>,
        closed: bool,
    }

    struct MemPipe;

    impl MemPipe {
        fn pair() -> (MemPipeReader, MemPipeWriter) {
            let inner = Arc::new((
                Mutex::new(MemPipeState {
                    buf: VecDeque::new(),
                    closed: false,
                }),
                Condvar::new(),
            ));
            (
                MemPipeReader {
                    inner: inner.clone(),
                },
                MemPipeWriter { inner },
            )
        }
    }

    struct MemPipeReader {
        inner: Arc<(Mutex<MemPipeState>, Condvar)>,
    }

    impl Read for MemPipeReader {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            let (lock, cvar) = &*self.inner;
            let mut st = lock.lock().unwrap();
            while st.buf.is_empty() && !st.closed {
                st = cvar.wait(st).unwrap();
            }
            if st.buf.is_empty() && st.closed {
                return Ok(0);
            }
            let n = out.len().min(st.buf.len());
            for slot in out.iter_mut().take(n) {
                *slot = st.buf.pop_front().unwrap();
            }
            Ok(n)
        }
    }

    struct MemPipeWriter {
        inner: Arc<(Mutex<MemPipeState>, Condvar)>,
    }

    impl Write for MemPipeWriter {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            let (lock, cvar) = &*self.inner;
            let mut st = lock.lock().unwrap();
            if st.closed {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "pipe closed",
                ));
            }
            st.buf.extend(data);
            cvar.notify_all();
            Ok(data.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Drop for MemPipeWriter {
        fn drop(&mut self) {
            let (lock, cvar) = &*self.inner;
            let mut st = lock.lock().unwrap();
            st.closed = true;
            cvar.notify_all();
        }
    }

    struct BlockingParentExecutor {
        executor: Executor,
        block: AtomicBool,
        released: (Mutex<bool>, Condvar),
    }

    impl BlockingParentExecutor {
        fn release(&self) {
            let (lock, cvar) = &self.released;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
        }
    }

    impl EffectExecutor for BlockingParentExecutor {
        fn execute(&self, ctx: &ExecutionContext, effect: &Effect) -> Result<(), Error> {
            if self.block.swap(false, Ordering::SeqCst) {
                let (lock, cvar) = &self.released;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = cvar.wait(released).unwrap();
                }
            }
            self.executor.execute(ctx, effect)
        }
    }

    fn read_simple_frames() -> Vec<Vec<u8>> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("orchestration")
            .join("cli")
            .join("testdata")
            .join("t3code-v0.1.7")
            .join("simple")
            .join("codex_transcript.ndjson");
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);
        let mut frames = Vec::new();
        for line in reader.lines() {
            let line = line.unwrap();
            if line.trim().is_empty() {
                continue;
            }
            #[derive(Deserialize)]
            struct Entry {
                #[serde(rename = "type", default)]
                entry_type: String,
                #[serde(default)]
                frame: Value,
            }
            let entry: Entry = serde_json::from_str(&line).unwrap();
            if entry.entry_type == "emit_inbound" {
                frames.push(serde_json::to_vec(&entry.frame).unwrap());
            }
        }
        frames
    }

    #[test]
    fn test_mcp_orchestrator_workflow() {
        let ctx = ExecutionContext::new();
        let store = Arc::new(MemoryStore::new());
        let o = Arc::new(Orchestrator::new(store));
        let cli_executor = Executor::new(o.clone());
        let replay = ReplayAdapter::new(CodexAdapter::new(), read_simple_frames());
        cli_executor.register("codex", replay);

        let blocking_exec = Arc::new(BlockingParentExecutor {
            executor: cli_executor.clone(),
            block: AtomicBool::new(true),
            released: (Mutex::new(false), Condvar::new()),
        });
        let temp_dir = TempDir::new().unwrap();
        let workspace = temp_dir.path().to_string_lossy().into_owned();

        let server = Server::new(
            o.clone(),
            Some(blocking_exec.clone()),
            Config {
                providers: vec![Provider {
                    instance_id: "codex".to_string(),
                    driver: "codex".to_string(),
                    models: vec!["default".to_string()],
                    available: true,
                    runtime_modes: Vec::new(),
                }],
                default_project_id: "project-test".to_string(),
                default_worktree_path: Some(workspace),
                default_target: ModelSelection {
                    instance_id: "codex".to_string(),
                    model: "default".to_string(),
                    options: BTreeMap::new(),
                },
                ..Default::default()
            },
        );

        let (in_reader, in_writer) = MemPipe::pair();
        let (out_reader, out_writer) = MemPipe::pair();

        let server_bg = server.clone();
        let ctx_bg = ctx.clone();
        let serve_handle = thread::spawn(move || server_bg.serve(&ctx_bg, in_reader, out_writer));

        struct TestClient {
            writer: MemPipeWriter,
            reader: BufReader<MemPipeReader>,
            next_id: i64,
        }
        impl TestClient {
            fn call(&mut self, method: &str, params: Value) -> Value {
                self.next_id += 1;
                let id = self.next_id;
                let mut req_bytes = serde_json::to_vec(&json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": method,
                    "params": params,
                }))
                .unwrap();
                req_bytes.push(b'\n');
                self.writer.write_all(&req_bytes).unwrap();
                self.writer.flush().unwrap();

                loop {
                    let mut line = String::new();
                    self.reader.read_line(&mut line).unwrap();
                    let resp: Value = serde_json::from_str(&line).unwrap();
                    if resp.get("id").and_then(|v| v.as_i64()) != Some(id) {
                        continue;
                    }
                    assert!(
                        resp.get("error").is_none_or(|v| v.is_null()),
                        "RPC {} failed: {:?}",
                        method,
                        resp.get("error")
                    );
                    return resp.get("result").cloned().unwrap_or(Value::Null);
                }
            }

            fn tool(&mut self, name: &str, arguments: Value) -> Value {
                let res = self.call(
                    "tools/call",
                    json!({
                        "name": name,
                        "arguments": arguments,
                    }),
                );
                assert_ne!(
                    res.get("isError").and_then(|v| v.as_bool()),
                    Some(true),
                    "tool {} failed: {:?}",
                    name,
                    res
                );
                res.get("structuredContent")
                    .cloned()
                    .expect("structuredContent")
            }
        }

        let mut client = TestClient {
            writer: in_writer,
            reader: BufReader::new(out_reader),
            next_id: 0,
        };

        let initialized = client.call(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "mcp-test", "version": "1"}
            }),
        );
        assert_eq!(
            initialized.get("protocolVersion").and_then(|v| v.as_str()),
            Some("2025-06-18")
        );

        let tools = client.call("tools/list", json!({}));
        let tool_list = tools.get("tools").and_then(|v| v.as_array()).unwrap();
        assert_eq!(tool_list.len(), 11);

        let created = client.tool(
            "create_threads",
            json!({
                "threads": [{
                    "prompt": "Respond with the following text: fixture simple ok",
                    "title": "MCP parent"
                }],
                "clientRequestId": "parent-create"
            }),
        );
        let parent_entry = &created["threads"][0];
        let parent_id = parent_entry["threadId"].as_str().unwrap().to_string();
        let parent_run_id = parent_entry["runId"].as_str().unwrap().to_string();
        assert_eq!(parent_entry["status"].as_str(), Some("starting"));

        let delegated = client.tool(
            "delegate_task",
            json!({
                "task": "Return the child result only.",
                "mode": "wait",
                "timeoutMs": 2000,
                "clientRequestId": "delegate-1",
                "parentThreadId": parent_id
            }),
        );
        let task_id = delegated["taskId"].as_str().unwrap().to_string();
        let child_id = delegated["childThreadId"].as_str().unwrap().to_string();
        assert_eq!(delegated["workState"].as_str(), Some("result_available"));
        assert_eq!(delegated["summary"].as_str(), Some("fixture simple ok"));
        assert!(!delegated["resultContextTransferId"].is_null());
        assert_eq!(delegated["status"].as_str(), Some("completed"));

        let child_read = client.tool("t3_thread_read", json!({"threadId": child_id}));
        for item in child_read["items"].as_array().unwrap() {
            if item["type"].as_str() == Some("user_message") {
                assert_eq!(item["text"].as_str(), Some("Return the child result only."));
            }
        }

        let status = client.tool(
            "task_status",
            json!({"taskId": task_id, "parentThreadId": parent_id}),
        );
        assert_eq!(status["summary"], delegated["summary"]);
        assert_eq!(status["childRunId"], delegated["childRunId"]);

        let retried = client.tool(
            "delegate_task",
            json!({
                "task": "Return the child result only.",
                "clientRequestId": "delegate-1",
                "parentThreadId": parent_id
            }),
        );
        assert_eq!(retried["taskId"].as_str(), Some(task_id.as_str()));
        assert_eq!(retried["childThreadId"].as_str(), Some(child_id.as_str()));

        blocking_exec.release();

        let wait = client.tool(
            "t3_thread_wait",
            json!({
                "threadId": parent_id,
                "runId": parent_run_id,
                "timeoutMs": 2000
            }),
        );
        assert_eq!(wait["status"].as_str(), Some("completed"));
        assert_eq!(wait["timedOut"].as_bool(), Some(false));

        let read = client.tool("t3_thread_read", json!({"threadId": parent_id}));
        let mut found_assistant = false;
        for item in read["items"].as_array().unwrap() {
            if item["type"].as_str() == Some("assistant_message")
                && item["text"].as_str() == Some("fixture simple ok")
            {
                found_assistant = true;
            }
            if item["type"].as_str() == Some("user_message") {
                assert_eq!(item["createdBy"].as_str(), Some("agent"));
                assert_eq!(item["creationSource"].as_str(), Some("mcp"));
            }
        }
        assert!(found_assistant, "parent result missing: {:?}", read);

        let listed = client.tool("t3_thread_list", json!({"includeSubagents": false}));
        assert_eq!(listed["total"].as_u64(), Some(1));
        let listed_all = client.tool("t3_thread_list", json!({"includeSubagents": true}));
        assert_eq!(listed_all["total"].as_u64(), Some(2));

        let sent = client.tool(
            "t3_thread_send",
            json!({
                "threadId": parent_id,
                "message": "Respond with the following text: fixture simple ok",
                "clientRequestId": "followup-1"
            }),
        );
        assert_eq!(sent["delivery"].as_str(), Some("started"));
        assert_eq!(sent["status"].as_str(), Some("starting"));

        let wait2 = client.tool(
            "t3_thread_wait",
            json!({
                "threadId": parent_id,
                "runId": sent["runId"],
                "timeoutMs": 2000
            }),
        );
        assert_eq!(wait2["status"].as_str(), Some("completed"));

        let idle = client.tool(
            "create_threads",
            json!({
                "threads": [{"title": "Idle"}],
                "clientRequestId": "idle-1"
            }),
        );
        let idle_again = client.tool(
            "create_threads",
            json!({
                "threads": [{"title": "Idle"}],
                "clientRequestId": "idle-1"
            }),
        );
        assert_eq!(
            idle["threads"][0]["threadId"],
            idle_again["threads"][0]["threadId"]
        );

        let interrupt = client.tool("t3_thread_interrupt", json!({"threadId": parent_id}));
        assert_eq!(interrupt["status"].as_str(), Some("no_active_run"));

        let parent_snapshot = o.snapshot(&parent_id).unwrap();
        let active_pt_id = parent_snapshot
            .projection
            .thread
            .active_provider_thread_id
            .as_ref()
            .unwrap();
        let provider_thread = parent_snapshot
            .projection
            .provider_thread(active_pt_id)
            .unwrap();
        let request = RuntimeRequest {
            id: "mcp-pending-1".to_string(),
            node_id: parent_snapshot.projection.runs[0]
                .root_node_id
                .clone()
                .unwrap(),
            kind: REQUEST_USER_INPUT.to_string(),
            status: REQUEST_PENDING.to_string(),
            response_capability: ResponseCapability {
                capability_type: "live".to_string(),
                provider_session_id: provider_thread.provider_session_id.clone(),
                ..Default::default()
            },
            created_at: Utc::now(),
            ..Default::default()
        };
        o.ingest(IngestInput {
            thread_id: parent_id.clone(),
            provider_instance_id: "codex".to_string(),
            raw_event_id: Some("mcp-pending-raw".to_string()),
            event: ProviderEvent {
                event_type: PROVIDER_EVENT_RUNTIME_REQUEST_UPDATED.to_string(),
                runtime_request: Some(request),
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();

        let pending = client.tool(
            "t3_pending_request",
            json!({"threadId": parent_id, "action": "list"}),
        );
        assert_eq!(pending["requests"].as_array().unwrap().len(), 1);

        client.tool(
            "t3_pending_request",
            json!({
                "threadId": parent_id,
                "action": "respond",
                "requestId": "mcp-pending-1",
                "answers": {"answer": "yes"},
                "clientRequestId": "answer-1"
            }),
        );
        let read_req = client.tool(
            "t3_pending_request",
            json!({
                "threadId": parent_id,
                "action": "read",
                "requestId": "mcp-pending-1"
            }),
        );
        assert_eq!(read_req["request"]["status"].as_str(), Some("resolved"));

        drop(client);
        serve_handle.join().unwrap().unwrap();
        cli_executor.close().unwrap();
    }
}
