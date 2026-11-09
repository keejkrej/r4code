use chrono::{DateTime, Utc};
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::orchestration::ids::*;
use crate::orchestration::types::*;

pub type EventType = String;

pub const EVENT_THREAD_CREATED: &str = "thread.created";
pub const EVENT_THREAD_ARCHIVED: &str = "thread.archived";
pub const EVENT_THREAD_UNARCHIVED: &str = "thread.unarchived";
pub const EVENT_THREAD_DELETED: &str = "thread.deleted";
pub const EVENT_THREAD_SETTLED: &str = "thread.settled";
pub const EVENT_THREAD_UNSETTLED: &str = "thread.unsettled";
pub const EVENT_THREAD_SNOOZED: &str = "thread.snoozed";
pub const EVENT_THREAD_UNSNOOZED: &str = "thread.unsnoozed";
pub const EVENT_THREAD_PINNED: &str = "thread.pinned";
pub const EVENT_THREAD_UNPINNED: &str = "thread.unpinned";
pub const EVENT_THREAD_METADATA_UPDATED: &str = "thread.metadata-updated";
pub const EVENT_THREAD_VISITED: &str = "thread.visited";
pub const EVENT_THREAD_MARKED_UNREAD: &str = "thread.marked-unread";
pub const EVENT_THREAD_RUNTIME_MODE_UPDATED: &str = "thread.runtime-mode-updated";
pub const EVENT_THREAD_INTERACTION_MODE_UPDATED: &str = "thread.interaction-mode-updated";
pub const EVENT_THREAD_MODEL_SELECTION_UPDATED: &str = "thread.model-selection-updated";
pub const EVENT_THREAD_PROVIDER_SWITCHED: &str = "thread.provider-switched";
pub const EVENT_RUN_CREATED: &str = "run.created";
pub const EVENT_RUN_UPDATED: &str = "run.updated";
pub const EVENT_RUN_BACKGROUND_WORK_CANCELLED: &str = "run.background-work-cancelled";
pub const EVENT_RUN_ATTEMPT_CREATED: &str = "run-attempt.created";
pub const EVENT_RUN_ATTEMPT_UPDATED: &str = "run-attempt.updated";
pub const EVENT_NODE_UPDATED: &str = "node.updated";
pub const EVENT_SUBAGENT_UPDATED: &str = "subagent.updated";
pub const EVENT_PROVIDER_SESSION_ATTACHED: &str = "provider-session.attached";
pub const EVENT_PROVIDER_SESSION_UPDATED: &str = "provider-session.updated";
pub const EVENT_PROVIDER_SESSION_DETACHED: &str = "provider-session.detached";
pub const EVENT_PROVIDER_THREAD_UPDATED: &str = "provider-thread.updated";
pub const EVENT_PROVIDER_TURN_UPDATED: &str = "provider-turn.updated";
pub const EVENT_RUNTIME_REQUEST_UPDATED: &str = "runtime-request.updated";
pub const EVENT_MESSAGE_UPDATED: &str = "message.updated";
pub const EVENT_TURN_ITEM_UPDATED: &str = "turn-item.updated";
pub const EVENT_PLAN_UPDATED: &str = "plan.updated";
pub const EVENT_CHECKPOINT_SCOPE_CREATED: &str = "checkpoint-scope.created";
pub const EVENT_CHECKPOINT_CAPTURED: &str = "checkpoint.captured";
pub const EVENT_CHECKPOINT_ROLLBACK_REQUESTED: &str = "checkpoint.rollback-requested";
pub const EVENT_CONTEXT_HANDOFF_UPDATED: &str = "context-handoff.updated";
pub const EVENT_CONTEXT_TRANSFER_CREATED: &str = "context-transfer.created";
pub const EVENT_CONTEXT_TRANSFER_UPDATED: &str = "context-transfer.updated";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderSessionDetached {
    #[serde(default)]
    pub provider_session_id: ProviderSessionID,
    #[serde(default = "default_zero_time")]
    pub detached_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RunBackgroundWorkCancelled {
    #[serde(default)]
    pub run_id: RunID,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointRollbackRequest {
    #[serde(default)]
    pub request_id: CommandID,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub scope_id: CheckpointScopeID,
    #[serde(default)]
    pub checkpoint_id: CheckpointID,
    #[serde(default)]
    pub restore_files: bool,
    #[serde(default = "default_zero_time")]
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventPayload {
    Thread(Thread),
    Run(Run),
    RunBackgroundWorkCancelled(RunBackgroundWorkCancelled),
    RunAttempt(RunAttempt),
    ExecutionNode(ExecutionNode),
    Subagent(Subagent),
    ProviderSession(ProviderSession),
    ProviderSessionDetached(ProviderSessionDetached),
    ProviderThread(ProviderThread),
    ProviderTurn(ProviderTurn),
    RuntimeRequest(RuntimeRequest),
    ConversationMessage(ConversationMessage),
    TurnItem(TurnItem),
    PlanArtifact(PlanArtifact),
    CheckpointScope(CheckpointScope),
    Checkpoint(Checkpoint),
    CheckpointRollbackRequest(CheckpointRollbackRequest),
    ContextHandoff(ContextHandoff),
    ContextTransfer(ContextTransfer),
    Raw(Value),
}

impl Default for EventPayload {
    fn default() -> Self {
        Self::Raw(Value::Null)
    }
}

impl EventPayload {
    pub fn to_value(&self) -> Result<Value, serde_json::Error> {
        match self {
            Self::Thread(v) => serde_json::to_value(v),
            Self::Run(v) => serde_json::to_value(v),
            Self::RunBackgroundWorkCancelled(v) => serde_json::to_value(v),
            Self::RunAttempt(v) => serde_json::to_value(v),
            Self::ExecutionNode(v) => serde_json::to_value(v),
            Self::Subagent(v) => serde_json::to_value(v),
            Self::ProviderSession(v) => serde_json::to_value(v),
            Self::ProviderSessionDetached(v) => serde_json::to_value(v),
            Self::ProviderThread(v) => serde_json::to_value(v),
            Self::ProviderTurn(v) => serde_json::to_value(v),
            Self::RuntimeRequest(v) => serde_json::to_value(v),
            Self::ConversationMessage(v) => serde_json::to_value(v),
            Self::TurnItem(v) => serde_json::to_value(v),
            Self::PlanArtifact(v) => serde_json::to_value(v),
            Self::CheckpointScope(v) => serde_json::to_value(v),
            Self::Checkpoint(v) => serde_json::to_value(v),
            Self::CheckpointRollbackRequest(v) => serde_json::to_value(v),
            Self::ContextHandoff(v) => serde_json::to_value(v),
            Self::ContextTransfer(v) => serde_json::to_value(v),
            Self::Raw(v) => Ok(v.clone()),
        }
    }

    pub fn from_value(event_type: &str, data: Value) -> Result<Self, String> {
        let decode_err = |e: serde_json::Error| format!("decode {event_type} payload: {e}");
        match event_type {
            EVENT_THREAD_CREATED
            | EVENT_THREAD_ARCHIVED
            | EVENT_THREAD_UNARCHIVED
            | EVENT_THREAD_DELETED
            | EVENT_THREAD_SETTLED
            | EVENT_THREAD_UNSETTLED
            | EVENT_THREAD_SNOOZED
            | EVENT_THREAD_UNSNOOZED
            | EVENT_THREAD_PINNED
            | EVENT_THREAD_UNPINNED
            | EVENT_THREAD_METADATA_UPDATED
            | EVENT_THREAD_VISITED
            | EVENT_THREAD_MARKED_UNREAD
            | EVENT_THREAD_RUNTIME_MODE_UPDATED
            | EVENT_THREAD_INTERACTION_MODE_UPDATED
            | EVENT_THREAD_MODEL_SELECTION_UPDATED
            | EVENT_THREAD_PROVIDER_SWITCHED => {
                serde_json::from_value(data).map(Self::Thread).map_err(decode_err)
            }
            EVENT_RUN_CREATED | EVENT_RUN_UPDATED => {
                serde_json::from_value(data).map(Self::Run).map_err(decode_err)
            }
            EVENT_RUN_BACKGROUND_WORK_CANCELLED => serde_json::from_value(data)
                .map(Self::RunBackgroundWorkCancelled)
                .map_err(decode_err),
            EVENT_RUN_ATTEMPT_CREATED | EVENT_RUN_ATTEMPT_UPDATED => serde_json::from_value(data)
                .map(Self::RunAttempt)
                .map_err(decode_err),
            EVENT_NODE_UPDATED => serde_json::from_value(data)
                .map(Self::ExecutionNode)
                .map_err(decode_err),
            EVENT_SUBAGENT_UPDATED => serde_json::from_value(data)
                .map(Self::Subagent)
                .map_err(decode_err),
            EVENT_PROVIDER_SESSION_ATTACHED | EVENT_PROVIDER_SESSION_UPDATED => {
                serde_json::from_value(data)
                    .map(Self::ProviderSession)
                    .map_err(decode_err)
            }
            EVENT_PROVIDER_SESSION_DETACHED => serde_json::from_value(data)
                .map(Self::ProviderSessionDetached)
                .map_err(decode_err),
            EVENT_PROVIDER_THREAD_UPDATED => serde_json::from_value(data)
                .map(Self::ProviderThread)
                .map_err(decode_err),
            EVENT_PROVIDER_TURN_UPDATED => serde_json::from_value(data)
                .map(Self::ProviderTurn)
                .map_err(decode_err),
            EVENT_RUNTIME_REQUEST_UPDATED => serde_json::from_value(data)
                .map(Self::RuntimeRequest)
                .map_err(decode_err),
            EVENT_MESSAGE_UPDATED => serde_json::from_value(data)
                .map(Self::ConversationMessage)
                .map_err(decode_err),
            EVENT_TURN_ITEM_UPDATED => serde_json::from_value(data)
                .map(Self::TurnItem)
                .map_err(decode_err),
            EVENT_PLAN_UPDATED => serde_json::from_value(data)
                .map(Self::PlanArtifact)
                .map_err(decode_err),
            EVENT_CHECKPOINT_SCOPE_CREATED => serde_json::from_value(data)
                .map(Self::CheckpointScope)
                .map_err(decode_err),
            EVENT_CHECKPOINT_CAPTURED => serde_json::from_value(data)
                .map(Self::Checkpoint)
                .map_err(decode_err),
            EVENT_CHECKPOINT_ROLLBACK_REQUESTED => serde_json::from_value(data)
                .map(Self::CheckpointRollbackRequest)
                .map_err(decode_err),
            EVENT_CONTEXT_HANDOFF_UPDATED => serde_json::from_value(data)
                .map(Self::ContextHandoff)
                .map_err(decode_err),
            EVENT_CONTEXT_TRANSFER_CREATED | EVENT_CONTEXT_TRANSFER_UPDATED => {
                serde_json::from_value(data)
                    .map(Self::ContextTransfer)
                    .map_err(decode_err)
            }
            _ => Ok(Self::Raw(data)),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Thread(_) => "Thread",
            Self::Run(_) => "Run",
            Self::RunBackgroundWorkCancelled(_) => "RunBackgroundWorkCancelled",
            Self::RunAttempt(_) => "RunAttempt",
            Self::ExecutionNode(_) => "ExecutionNode",
            Self::Subagent(_) => "Subagent",
            Self::ProviderSession(_) => "ProviderSession",
            Self::ProviderSessionDetached(_) => "ProviderSessionDetached",
            Self::ProviderThread(_) => "ProviderThread",
            Self::ProviderTurn(_) => "ProviderTurn",
            Self::RuntimeRequest(_) => "RuntimeRequest",
            Self::ConversationMessage(_) => "ConversationMessage",
            Self::TurnItem(_) => "TurnItem",
            Self::PlanArtifact(_) => "PlanArtifact",
            Self::CheckpointScope(_) => "CheckpointScope",
            Self::Checkpoint(_) => "Checkpoint",
            Self::CheckpointRollbackRequest(_) => "CheckpointRollbackRequest",
            Self::ContextHandoff(_) => "ContextHandoff",
            Self::ContextTransfer(_) => "ContextTransfer",
            Self::Raw(_) => "Raw",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub id: EventID,
    pub thread_id: ThreadID,
    pub run_id: Option<RunID>,
    pub node_id: Option<NodeID>,
    pub driver: ProviderDriverKind,
    pub provider_instance_id: ProviderInstanceID,
    pub raw_event_id: Option<RawEventID>,
    pub occurred_at: DateTime<Utc>,
    pub event_type: EventType,
    pub payload: EventPayload,
}

impl Default for Event {
    fn default() -> Self {
        Self {
            id: String::new(),
            thread_id: String::new(),
            run_id: None,
            node_id: None,
            driver: String::new(),
            provider_instance_id: String::new(),
            raw_event_id: None,
            occurred_at: default_zero_time(),
            event_type: String::new(),
            payload: EventPayload::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireEvent {
    #[serde(default)]
    id: EventID,
    #[serde(default)]
    thread_id: ThreadID,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    run_id: Option<RunID>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    node_id: Option<NodeID>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    driver: ProviderDriverKind,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    provider_instance_id: ProviderInstanceID,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    raw_event_id: Option<RawEventID>,
    #[serde(default = "default_zero_time")]
    occurred_at: DateTime<Utc>,
    #[serde(rename = "type", default)]
    event_type: EventType,
    #[serde(default)]
    payload: Value,
}

impl Serialize for Event {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let payload = self
            .payload
            .to_value()
            .map_err(serde::ser::Error::custom)?;
        let wire = WireEvent {
            id: self.id.clone(),
            thread_id: self.thread_id.clone(),
            run_id: self.run_id.clone(),
            node_id: self.node_id.clone(),
            driver: self.driver.clone(),
            provider_instance_id: self.provider_instance_id.clone(),
            raw_event_id: self.raw_event_id.clone(),
            occurred_at: self.occurred_at,
            event_type: self.event_type.clone(),
            payload,
        };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Event {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = WireEvent::deserialize(deserializer)?;
        let payload = EventPayload::from_value(&wire.event_type, wire.payload)
            .map_err(DeError::custom)?;
        Ok(Event {
            id: wire.id,
            thread_id: wire.thread_id,
            run_id: wire.run_id,
            node_id: wire.node_id,
            driver: wire.driver,
            provider_instance_id: wire.provider_instance_id,
            raw_event_id: wire.raw_event_id,
            occurred_at: wire.occurred_at,
            event_type: wire.event_type,
            payload,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct StoredEvent {
    #[serde(default)]
    pub sequence: u64,
    #[serde(default)]
    pub command_id: Option<CommandID>,
    #[serde(default)]
    pub event: Event,
}
