use std::collections::BTreeMap;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::orchestration::errors::{Error, ErrorKind, require_id};
use crate::orchestration::event::StoredEvent;
use crate::orchestration::ids::*;
use crate::orchestration::types::*;

pub type CommandType = String;

pub const COMMAND_THREAD_CREATE: &str = "thread.create";
pub const COMMAND_THREAD_ARCHIVE: &str = "thread.archive";
pub const COMMAND_THREAD_UNARCHIVE: &str = "thread.unarchive";
pub const COMMAND_THREAD_DELETE: &str = "thread.delete";
pub const COMMAND_THREAD_RUNTIME_MODE_SET: &str = "thread.runtime-mode.set";
pub const COMMAND_MESSAGE_DISPATCH: &str = "message.dispatch";
pub const COMMAND_RUN_INTERRUPT: &str = "run.interrupt";
pub const COMMAND_RUNTIME_REQUEST_RESPOND: &str = "runtime-request.respond";
pub const COMMAND_THREAD_FORK: &str = "thread.fork";
pub const COMMAND_DELEGATED_TASK_REQUEST: &str = "delegated_task.request";
pub const COMMAND_QUEUED_RUN_CANCEL: &str = "queued-run.cancel";
pub const COMMAND_QUEUE_RESUME: &str = "queue.resume";
pub const COMMAND_PROVIDER_SWITCH: &str = "provider.switch";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CreateThread {
    #[serde(default)]
    pub project_id: ProjectID,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub model_selection: ModelSelection,
    #[serde(default)]
    pub runtime_mode: RuntimeMode,
    #[serde(default)]
    pub interaction_mode: InteractionMode,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub worktree_path: Option<String>,
    #[serde(default)]
    pub created_by: Actor,
    #[serde(default)]
    pub creation_source: CreationSource,
}

pub type DispatchModeType = String;
pub const DISPATCH_DEFER_START: &str = "defer_start";
pub const DISPATCH_STEER_ACTIVE: &str = "steer_active";
pub const DISPATCH_RESTART_ACTIVE: &str = "restart_active";
pub const DISPATCH_QUEUE_AFTER_ACTIVE: &str = "queue_after_active";
pub const DISPATCH_START_IMMEDIATELY: &str = "start_immediately";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DispatchMode {
    #[serde(rename = "type", default)]
    pub mode_type: DispatchModeType,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target_run_id: RunID,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DispatchMessage {
    #[serde(default)]
    pub message_id: MessageID,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub title_seed: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_selection: Option<ModelSelection>,
    #[serde(default)]
    pub dispatch_mode: DispatchMode,
    #[serde(default)]
    pub created_by: Actor,
    #[serde(default)]
    pub creation_source: CreationSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InterruptRun {
    #[serde(default)]
    pub run_id: RunID,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub hold_queue: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRequestResponse {
    #[serde(default)]
    pub request_id: RuntimeRequestID,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub answers: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ForkSourcePoint {
    #[serde(rename = "type", default)]
    pub point_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: RunID,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub checkpoint_id: CheckpointID,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DelegateTask {
    #[serde(default)]
    pub parent_thread_id: ThreadID,
    #[serde(default)]
    pub parent_run_id: RunID,
    #[serde(default)]
    pub parent_node_id: NodeID,
    #[serde(default)]
    pub task: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub title: String,
    #[serde(default)]
    pub model_selection: ModelSelection,
    #[serde(default)]
    pub runtime_mode: RuntimeMode,
    #[serde(default)]
    pub interaction_mode: InteractionMode,
    #[serde(default)]
    pub created_by: Actor,
    #[serde(default)]
    pub creation_source: CreationSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ForkThread {
    #[serde(default)]
    pub source_thread_id: ThreadID,
    #[serde(default)]
    pub source_point: ForkSourcePoint,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub title: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub side_chat: bool,
    #[serde(default)]
    pub created_by: Actor,
    #[serde(default)]
    pub creation_source: CreationSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CancelQueuedRun {
    #[serde(default)]
    pub run_id: RunID,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProviderSwitch {
    #[serde(default)]
    pub model_selection: ModelSelection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeModeChange {
    #[serde(default)]
    pub runtime_mode: RuntimeMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CommandPayload {
    #[default]
    None,
    CreateThread(CreateThread),
    DispatchMessage(DispatchMessage),
    InterruptRun(InterruptRun),
    RuntimeRequestResponse(RuntimeRequestResponse),
    DelegateTask(DelegateTask),
    ForkThread(ForkThread),
    CancelQueuedRun(CancelQueuedRun),
    ProviderSwitch(ProviderSwitch),
    RuntimeModeChange(RuntimeModeChange),
    Raw(Value),
}

impl CommandPayload {
    pub fn as_create_thread(&self) -> Option<&CreateThread> {
        match self {
            Self::CreateThread(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_dispatch_message(&self) -> Option<&DispatchMessage> {
        match self {
            Self::DispatchMessage(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_interrupt_run(&self) -> Option<&InterruptRun> {
        match self {
            Self::InterruptRun(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_runtime_request_response(&self) -> Option<&RuntimeRequestResponse> {
        match self {
            Self::RuntimeRequestResponse(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_delegate_task(&self) -> Option<&DelegateTask> {
        match self {
            Self::DelegateTask(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_fork_thread(&self) -> Option<&ForkThread> {
        match self {
            Self::ForkThread(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_cancel_queued_run(&self) -> Option<&CancelQueuedRun> {
        match self {
            Self::CancelQueuedRun(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_provider_switch(&self) -> Option<&ProviderSwitch> {
        match self {
            Self::ProviderSwitch(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_runtime_mode_change(&self) -> Option<&RuntimeModeChange> {
        match self {
            Self::RuntimeModeChange(v) => Some(v),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Command {
    pub command_type: CommandType,
    pub command_id: CommandID,
    pub thread_id: ThreadID,
    pub payload: CommandPayload,
}

pub fn validate_command(command: &Command) -> Result<(), Error> {
    require_id("commandId", &command.command_id)?;
    require_id("threadId", &command.thread_id)?;

    let invalid = |msg: String| Error::with_detail(ErrorKind::InvalidCommand, msg);

    match command.command_type.as_str() {
        COMMAND_THREAD_CREATE => {
            let CommandPayload::CreateThread(payload) = &command.payload else {
                return Err(invalid(
                    "thread.create payload must be CreateThread".to_string(),
                ));
            };
            require_id("projectId", &payload.project_id)?;
            if payload.title.trim().is_empty()
                || payload.model_selection.model.trim().is_empty()
                || payload.model_selection.instance_id.is_empty()
            {
                return Err(invalid(
                    "title and model selection are required".to_string(),
                ));
            }
        }
        COMMAND_MESSAGE_DISPATCH => {
            let CommandPayload::DispatchMessage(payload) = &command.payload else {
                return Err(invalid(
                    "message.dispatch payload must be DispatchMessage".to_string(),
                ));
            };
            if payload.message_id.is_empty() {
                return Err(invalid("messageId is required".to_string()));
            }
            match payload.dispatch_mode.mode_type.as_str() {
                ""
                | DISPATCH_DEFER_START
                | DISPATCH_QUEUE_AFTER_ACTIVE
                | DISPATCH_START_IMMEDIATELY => {}
                DISPATCH_STEER_ACTIVE | DISPATCH_RESTART_ACTIVE => {
                    if payload.dispatch_mode.target_run_id.is_empty() {
                        return Err(invalid(format!(
                            "targetRunId is required for {}",
                            payload.dispatch_mode.mode_type
                        )));
                    }
                }
                other => {
                    return Err(invalid(format!("unknown dispatch mode {other:?}")));
                }
            }
        }
        COMMAND_RUN_INTERRUPT => {
            let CommandPayload::InterruptRun(payload) = &command.payload else {
                return Err(invalid(
                    "run.interrupt requires InterruptRun with runId".to_string(),
                ));
            };
            if payload.run_id.is_empty() {
                return Err(invalid(
                    "run.interrupt requires InterruptRun with runId".to_string(),
                ));
            }
        }
        COMMAND_RUNTIME_REQUEST_RESPOND => {
            let CommandPayload::RuntimeRequestResponse(payload) = &command.payload else {
                return Err(invalid(
                    "runtime-request.respond requires RuntimeRequestResponse with requestId"
                        .to_string(),
                ));
            };
            if payload.request_id.is_empty() {
                return Err(invalid(
                    "runtime-request.respond requires RuntimeRequestResponse with requestId"
                        .to_string(),
                ));
            }
        }
        COMMAND_DELEGATED_TASK_REQUEST => {
            let CommandPayload::DelegateTask(payload) = &command.payload else {
                return Err(invalid(
                    "delegated_task.request requires an active parent, task, and model selection"
                        .to_string(),
                ));
            };
            if payload.parent_thread_id != command.thread_id
                || payload.parent_run_id.is_empty()
                || payload.parent_node_id.is_empty()
                || payload.task.trim().is_empty()
                || payload.model_selection.instance_id.is_empty()
                || payload.model_selection.model.is_empty()
            {
                return Err(invalid(
                    "delegated_task.request requires an active parent, task, and model selection"
                        .to_string(),
                ));
            }
        }
        COMMAND_THREAD_FORK => {
            let CommandPayload::ForkThread(payload) = &command.payload else {
                return Err(invalid(
                    "thread.fork requires a different sourceThreadId".to_string(),
                ));
            };
            if payload.source_thread_id.is_empty() || payload.source_thread_id == command.thread_id
            {
                return Err(invalid(
                    "thread.fork requires a different sourceThreadId".to_string(),
                ));
            }
            match payload.source_point.point_type.as_str() {
                "latest_stable" | "run" | "checkpoint" => {}
                other => {
                    return Err(invalid(format!("unknown fork source point {other:?}")));
                }
            }
        }
        COMMAND_QUEUED_RUN_CANCEL => {
            let CommandPayload::CancelQueuedRun(payload) = &command.payload else {
                return Err(invalid(
                    "queued-run.cancel requires CancelQueuedRun with runId".to_string(),
                ));
            };
            if payload.run_id.is_empty() {
                return Err(invalid(
                    "queued-run.cancel requires CancelQueuedRun with runId".to_string(),
                ));
            }
        }
        COMMAND_THREAD_RUNTIME_MODE_SET => {
            let CommandPayload::RuntimeModeChange(payload) = &command.payload else {
                return Err(invalid(
                    "thread.runtime-mode.set requires a known mode".to_string(),
                ));
            };
            if !matches!(
                payload.runtime_mode.as_str(),
                RUNTIME_APPROVAL_REQUIRED
                    | RUNTIME_AUTO_ACCEPT_EDITS
                    | RUNTIME_AUTO
                    | RUNTIME_FULL_ACCESS
            ) {
                return Err(invalid(
                    "thread.runtime-mode.set requires a known mode".to_string(),
                ));
            }
        }
        COMMAND_PROVIDER_SWITCH => {
            let CommandPayload::ProviderSwitch(payload) = &command.payload else {
                return Err(invalid(
                    "provider.switch requires a model selection".to_string(),
                ));
            };
            if payload.model_selection.instance_id.is_empty()
                || payload.model_selection.model.trim().is_empty()
            {
                return Err(invalid(
                    "provider.switch requires a model selection".to_string(),
                ));
            }
        }
        COMMAND_THREAD_ARCHIVE
        | COMMAND_THREAD_UNARCHIVE
        | COMMAND_THREAD_DELETE
        | COMMAND_QUEUE_RESUME => {
            if command.payload != CommandPayload::None {
                return Err(invalid(format!(
                    "{} does not take a payload",
                    command.command_type
                )));
            }
        }
        other => {
            return Err(invalid(format!("unknown command type {other:?}")));
        }
    }
    Ok(())
}

pub fn creation_defaults(actor: &str, source: &str) -> (Actor, CreationSource) {
    let actor = if actor.is_empty() {
        ACTOR_USER.to_string()
    } else {
        actor.to_string()
    };
    let source = if source.is_empty() {
        CREATION_WEB.to_string()
    } else {
        source.to_string()
    };
    (actor, source)
}

pub fn thread_defaults(thread: &mut Thread) {
    if thread.runtime_mode.is_empty() {
        thread.runtime_mode = RUNTIME_FULL_ACCESS.to_string();
    }
    if thread.interaction_mode.is_empty() {
        thread.interaction_mode = INTERACTION_DEFAULT.to_string();
    }
}

pub type EffectKind = String;
pub const EFFECT_PROVIDER_TURN_START: &str = "provider.turn.start";
pub const EFFECT_PROVIDER_TURN_INTERRUPT: &str = "provider.turn.interrupt";
pub const EFFECT_PROVIDER_RUNTIME_REQUEST_RESPOND: &str = "provider.runtime-request.respond";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartEffect {
    #[serde(default)]
    pub thread: Thread,
    #[serde(default)]
    pub run: Run,
    #[serde(default)]
    pub attempt: RunAttempt,
    #[serde(default)]
    pub provider_thread: ProviderThread,
    #[serde(default)]
    pub message: ConversationMessage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_handoff: Option<ContextHandoff>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TurnInterruptEffect {
    #[serde(default)]
    pub provider_session_id: ProviderSessionID,
    #[serde(default)]
    pub provider_thread_id: ProviderThreadID,
    #[serde(default)]
    pub provider_turn_id: ProviderTurnID,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRequestRespondEffect {
    #[serde(default)]
    pub request: RuntimeRequest,
    #[serde(default)]
    pub response: RuntimeRequestResponse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectPayload {
    TurnStart(Box<TurnStartEffect>),
    TurnInterrupt(TurnInterruptEffect),
    RuntimeRequestRespond(RuntimeRequestRespondEffect),
    Unknown(Value),
}

impl Default for EffectPayload {
    fn default() -> Self {
        Self::Unknown(Value::Null)
    }
}

impl EffectPayload {
    pub fn to_value(&self) -> Result<Value, serde_json::Error> {
        match self {
            Self::TurnStart(v) => serde_json::to_value(v),
            Self::TurnInterrupt(v) => serde_json::to_value(v),
            Self::RuntimeRequestRespond(v) => serde_json::to_value(v),
            Self::Unknown(v) => Ok(v.clone()),
        }
    }

    pub fn from_value(kind: &str, data: Value) -> Result<Self, serde_json::Error> {
        match kind {
            EFFECT_PROVIDER_TURN_START => serde_json::from_value(data)
                .map(|v| Self::TurnStart(Box::new(v))),
            EFFECT_PROVIDER_TURN_INTERRUPT => {
                serde_json::from_value(data).map(Self::TurnInterrupt)
            }
            EFFECT_PROVIDER_RUNTIME_REQUEST_RESPOND => {
                serde_json::from_value(data).map(Self::RuntimeRequestRespond)
            }
            _ => Ok(Self::Unknown(data)),
        }
    }
}

/// Effect is post-commit provider work. Its ID is deterministic so an executor
/// can deduplicate retries. A durable adapter persists these in an outbox.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Effect {
    pub id: String,
    pub kind: EffectKind,
    pub command_id: CommandID,
    pub thread_id: ThreadID,
    pub payload: EffectPayload,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireEffect {
    #[serde(default)]
    id: String,
    #[serde(default)]
    kind: EffectKind,
    #[serde(default)]
    command_id: CommandID,
    #[serde(default)]
    thread_id: ThreadID,
    #[serde(default)]
    payload: Value,
}

impl Serialize for Effect {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let payload = self
            .payload
            .to_value()
            .map_err(serde::ser::Error::custom)?;
        let wire = WireEffect {
            id: self.id.clone(),
            kind: self.kind.clone(),
            command_id: self.command_id.clone(),
            thread_id: self.thread_id.clone(),
            payload,
        };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Effect {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = WireEffect::deserialize(deserializer)?;
        let payload = EffectPayload::from_value(&wire.kind, wire.payload)
            .map_err(DeError::custom)?;
        Ok(Effect {
            id: wire.id,
            kind: wire.kind,
            command_id: wire.command_id,
            thread_id: wire.thread_id,
            payload,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DispatchResult {
    #[serde(default)]
    pub sequence: u64,
    #[serde(default)]
    pub stored_events: Vec<StoredEvent>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub replayed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effects: Vec<Effect>,
}
