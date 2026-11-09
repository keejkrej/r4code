use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

pub use crate::orchestration::ids::*;

pub type Actor = String;
pub const ACTOR_USER: &str = "user";
pub const ACTOR_AGENT: &str = "agent";
pub const ACTOR_SYSTEM: &str = "system";

pub type CreationSource = String;
pub const CREATION_WEB: &str = "web";
pub const CREATION_MOBILE: &str = "mobile";
pub const CREATION_MCP: &str = "mcp";
pub const CREATION_PROVIDER: &str = "provider";
pub const CREATION_SERVER: &str = "server";

pub type RuntimeMode = String;
pub const RUNTIME_APPROVAL_REQUIRED: &str = "approval-required";
pub const RUNTIME_AUTO_ACCEPT_EDITS: &str = "auto-accept-edits";
pub const RUNTIME_AUTO: &str = "auto";
pub const RUNTIME_FULL_ACCESS: &str = "full-access";

pub fn runtime_mode_rank(mode: &str) -> i32 {
    match mode {
        RUNTIME_APPROVAL_REQUIRED => 0,
        RUNTIME_AUTO_ACCEPT_EDITS => 1,
        RUNTIME_AUTO => 2,
        RUNTIME_FULL_ACCESS => 3,
        _ => 0,
    }
}

pub fn runtime_mode_above(mode: &str, ceiling: &str) -> bool {
    runtime_mode_rank(mode) > runtime_mode_rank(ceiling)
}

pub type InteractionMode = String;
pub const INTERACTION_DEFAULT: &str = "default";
pub const INTERACTION_PLAN: &str = "plan";

pub type NativeRefStrength = String;
pub const NATIVE_REF_STRONG: &str = "strong";
pub const NATIVE_REF_WEAK: &str = "weak";
pub const NATIVE_REF_NONE: &str = "none";

pub fn default_zero_time() -> DateTime<Utc> {
    DateTime::<Utc>::MIN_UTC
}

pub fn is_zero_time(dt: &DateTime<Utc>) -> bool {
    *dt == DateTime::<Utc>::MIN_UTC
}

pub(crate) fn is_false(v: &bool) -> bool {
    !*v
}

pub(crate) fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

pub fn clone_json<T: Serialize + serde::de::DeserializeOwned>(
    value: &T,
) -> Result<T, serde_json::Error> {
    let encoded = serde_json::to_vec(value)?;
    serde_json::from_slice(&encoded)
}

pub fn json_values_equal<T: Serialize>(left: &T, right: &T) -> bool {
    let Ok(left_val) = serde_json::to_value(left) else {
        return false;
    };
    let Ok(right_val) = serde_json::to_value(right) else {
        return false;
    };
    left_val == right_val
}

fn null_to_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    let opt = Option::<T>::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProviderRef {
    #[serde(default)]
    pub driver: ProviderDriverKind,
    #[serde(default)]
    pub native_id: Option<String>,
    #[serde(default)]
    pub strength: NativeRefStrength,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ModelSelection {
    #[serde(default)]
    pub instance_id: ProviderInstanceID,
    #[serde(default)]
    pub model: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty", deserialize_with = "null_to_default")]
    pub options: BTreeMap<String, Value>,
}

impl ModelSelection {
    pub fn equal(&self, other: &Self) -> bool {
        self.instance_id == other.instance_id
            && self.model == other.model
            && json_values_equal(&self.options, &other.options)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Attachment {
    #[serde(rename = "type", default)]
    pub attachment_type: String,
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub mime_type: String,
    #[serde(default)]
    pub size_bytes: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ThreadLineage {
    #[serde(default)]
    pub parent_thread_id: Option<ThreadID>,
    #[serde(default)]
    pub relationship_to_parent: Option<String>,
    #[serde(default)]
    pub root_thread_id: ThreadID,
    #[serde(default, skip_serializing_if = "is_false")]
    pub side_chat: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ForkedFrom {
    #[serde(rename = "type", default)]
    pub fork_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub thread_id: ThreadID,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: RunID,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub node_id: NodeID,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub provider_thread_id: ProviderThreadID,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn_id: Option<ProviderTurnID>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Thread {
    #[serde(default)]
    pub created_by: Actor,
    #[serde(default)]
    pub creation_source: CreationSource,
    #[serde(default)]
    pub id: ThreadID,
    #[serde(default)]
    pub project_id: ProjectID,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub provider_instance_id: ProviderInstanceID,
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
    pub active_provider_thread_id: Option<ProviderThreadID>,
    #[serde(default)]
    pub lineage: ThreadLineage,
    #[serde(default)]
    pub forked_from: Option<ForkedFrom>,
    #[serde(default = "default_zero_time")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_zero_time")]
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub archived_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub settled_override: Option<String>,
    #[serde(default)]
    pub settled_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_visited_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub deleted_at: Option<DateTime<Utc>>,
}

impl Default for Thread {
    fn default() -> Self {
        Self {
            created_by: String::new(),
            creation_source: String::new(),
            id: String::new(),
            project_id: String::new(),
            title: String::new(),
            provider_instance_id: String::new(),
            model_selection: ModelSelection::default(),
            runtime_mode: String::new(),
            interaction_mode: String::new(),
            branch: None,
            worktree_path: None,
            active_provider_thread_id: None,
            lineage: ThreadLineage::default(),
            forked_from: None,
            created_at: default_zero_time(),
            updated_at: default_zero_time(),
            archived_at: None,
            settled_override: None,
            settled_at: None,
            last_visited_at: None,
            deleted_at: None,
        }
    }
}

pub type RunStatus = String;
pub const RUN_PREPARING: &str = "preparing";
pub const RUN_QUEUED: &str = "queued";
pub const RUN_STARTING: &str = "starting";
pub const RUN_RUNNING: &str = "running";
pub const RUN_WAITING: &str = "waiting";
pub const RUN_COMPLETED: &str = "completed";
pub const RUN_INTERRUPTED: &str = "interrupted";
pub const RUN_FAILED: &str = "failed";
pub const RUN_CANCELLED: &str = "cancelled";
pub const RUN_ROLLED_BACK: &str = "rolled_back";

pub fn run_status_is_terminal(status: &str) -> bool {
    matches!(
        status,
        RUN_COMPLETED | RUN_INTERRUPTED | RUN_FAILED | RUN_CANCELLED | RUN_ROLLED_BACK
    )
}

pub fn run_status_is_active(status: &str) -> bool {
    matches!(
        status,
        RUN_PREPARING | RUN_STARTING | RUN_RUNNING | RUN_WAITING
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    #[serde(default)]
    pub id: RunID,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub ordinal: i64,
    #[serde(default)]
    pub provider_instance_id: ProviderInstanceID,
    #[serde(default)]
    pub model_selection: ModelSelection,
    #[serde(default)]
    pub provider_thread_id: Option<ProviderThreadID>,
    #[serde(default)]
    pub user_message_id: MessageID,
    #[serde(default)]
    pub root_node_id: Option<NodeID>,
    #[serde(default)]
    pub active_attempt_id: Option<RunAttemptID>,
    #[serde(default)]
    pub status: RunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_position: Option<i64>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub queue_held: bool,
    #[serde(default = "default_zero_time")]
    pub requested_at: DateTime<Utc>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub checkpoint_id: Option<CheckpointID>,
    #[serde(default)]
    pub context_handoff_id: Option<ContextHandoffID>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_continuation_of_run_id: Option<RunID>,
}

impl Default for Run {
    fn default() -> Self {
        Self {
            id: String::new(),
            thread_id: String::new(),
            ordinal: 0,
            provider_instance_id: String::new(),
            model_selection: ModelSelection::default(),
            provider_thread_id: None,
            user_message_id: String::new(),
            root_node_id: None,
            active_attempt_id: None,
            status: String::new(),
            queue_position: None,
            queue_held: false,
            requested_at: default_zero_time(),
            started_at: None,
            completed_at: None,
            checkpoint_id: None,
            context_handoff_id: None,
            restart_continuation_of_run_id: None,
        }
    }
}

impl Run {
    pub fn is_terminal(&self) -> bool {
        run_status_is_terminal(&self.status)
    }

    pub fn is_active(&self) -> bool {
        run_status_is_active(&self.status)
    }
}

pub type AttemptStatus = String;
pub const ATTEMPT_PENDING: &str = "pending";
pub const ATTEMPT_RUNNING: &str = "running";
pub const ATTEMPT_COMPLETED: &str = "completed";
pub const ATTEMPT_INTERRUPTED: &str = "interrupted";
pub const ATTEMPT_FAILED: &str = "failed";
pub const ATTEMPT_CANCELLED: &str = "cancelled";
pub const ATTEMPT_SUPERSEDED: &str = "superseded";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RunAttempt {
    #[serde(default)]
    pub id: RunAttemptID,
    #[serde(default)]
    pub run_id: RunID,
    #[serde(default)]
    pub attempt_ordinal: i64,
    #[serde(default)]
    pub root_node_id: NodeID,
    #[serde(default)]
    pub provider_instance_id: ProviderInstanceID,
    #[serde(default)]
    pub provider_thread_id: ProviderThreadID,
    #[serde(default)]
    pub provider_turn_id: Option<ProviderTurnID>,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub status: AttemptStatus,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
}

pub type NodeKind = String;
pub const NODE_ROOT_TURN: &str = "root_turn";
pub const NODE_ASSISTANT_MESSAGE: &str = "assistant_message";
pub const NODE_REASONING: &str = "reasoning";
pub const NODE_PLAN: &str = "plan";
pub const NODE_TODO_LIST: &str = "todo_list";
pub const NODE_TOOL_CALL: &str = "tool_call";
pub const NODE_APPROVAL_REQUEST: &str = "approval_request";
pub const NODE_USER_INPUT_REQUEST: &str = "user_input_request";
pub const NODE_SUBAGENT: &str = "subagent";
pub const NODE_HOOK: &str = "hook";
pub const NODE_SYSTEM: &str = "system";

pub type NodeStatus = String;
pub const NODE_IDLE: &str = "idle";
pub const NODE_PENDING: &str = "pending";
pub const NODE_RUNNING: &str = "running";
pub const NODE_WAITING: &str = "waiting";
pub const NODE_COMPLETED: &str = "completed";
pub const NODE_INTERRUPTED: &str = "interrupted";
pub const NODE_FAILED: &str = "failed";
pub const NODE_CANCELLED: &str = "cancelled";
pub const NODE_ROLLED_BACK: &str = "rolled_back";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionNode {
    #[serde(default)]
    pub id: NodeID,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub run_id: Option<RunID>,
    #[serde(default)]
    pub parent_node_id: Option<NodeID>,
    #[serde(default)]
    pub root_node_id: NodeID,
    #[serde(default)]
    pub kind: NodeKind,
    #[serde(default)]
    pub status: NodeStatus,
    #[serde(default)]
    pub counts_for_run: bool,
    #[serde(default)]
    pub provider_thread_id: Option<ProviderThreadID>,
    #[serde(default)]
    pub provider_turn_id: Option<ProviderTurnID>,
    #[serde(default)]
    pub native_item_ref: Option<ProviderRef>,
    #[serde(default)]
    pub runtime_request_id: Option<RuntimeRequestID>,
    #[serde(default)]
    pub checkpoint_scope_id: Option<CheckpointScopeID>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subagent {
    #[serde(default)]
    pub id: NodeID,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub run_id: Option<RunID>,
    #[serde(default)]
    pub parent_node_id: NodeID,
    #[serde(default)]
    pub origin: String,
    #[serde(default)]
    pub created_by: Actor,
    #[serde(default)]
    pub driver: ProviderDriverKind,
    #[serde(default)]
    pub provider_instance_id: ProviderInstanceID,
    #[serde(default)]
    pub provider_thread_id: Option<ProviderThreadID>,
    #[serde(default)]
    pub child_thread_id: Option<ThreadID>,
    #[serde(default)]
    pub native_task_ref: Option<ProviderRef>,
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_selection: Option<ModelSelection>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub completion_wake: String,
    #[serde(default)]
    pub status: NodeStatus,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub progress: String,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default = "default_zero_time")]
    pub updated_at: DateTime<Utc>,
}

impl Default for Subagent {
    fn default() -> Self {
        Self {
            id: String::new(),
            thread_id: String::new(),
            run_id: None,
            parent_node_id: String::new(),
            origin: String::new(),
            created_by: String::new(),
            driver: String::new(),
            provider_instance_id: String::new(),
            provider_thread_id: None,
            child_thread_id: None,
            native_task_ref: None,
            prompt: String::new(),
            title: None,
            model: None,
            model_selection: None,
            completion_wake: String::new(),
            status: String::new(),
            progress: String::new(),
            result: None,
            started_at: None,
            completed_at: None,
            updated_at: default_zero_time(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SessionCapabilities {
    #[serde(default)]
    pub supports_multiple_provider_threads_per_session: bool,
    #[serde(default)]
    pub supports_model_switch_in_session: bool,
    #[serde(default)]
    pub supports_provider_switching_via_handoff: bool,
    #[serde(default)]
    pub supports_runtime_mode_switch_in_session: bool,
    #[serde(default)]
    pub pending_requests_survive_restart: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ThreadCapabilities {
    #[serde(default)]
    pub can_create_empty_thread: bool,
    #[serde(default)]
    pub can_read_thread_snapshot: bool,
    #[serde(default)]
    pub can_rollback_thread: bool,
    #[serde(default)]
    pub can_fork_thread: bool,
    #[serde(default)]
    pub can_fork_from_turn: bool,
    #[serde(default)]
    pub can_fork_from_subagent_thread: bool,
    #[serde(default)]
    pub exposes_native_thread_id: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TurnCapabilities {
    #[serde(default)]
    pub exposes_native_turn_id: bool,
    #[serde(default)]
    pub emits_turn_started: bool,
    #[serde(default)]
    pub emits_turn_completed: bool,
    #[serde(default)]
    pub supports_interrupt: bool,
    #[serde(default)]
    pub supports_active_steering: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub active_steering_interrupts_tools: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub active_steering_accepts_attachments: bool,
    #[serde(default)]
    pub supports_steering_by_interrupt_restart: bool,
    #[serde(default)]
    pub supports_queued_messages: bool,
    #[serde(default)]
    pub terminal_status_quality: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct StreamingCapabilities {
    #[serde(default)]
    pub streams_assistant_text: bool,
    #[serde(default)]
    pub streams_reasoning: bool,
    #[serde(default)]
    pub streams_tool_output: bool,
    #[serde(default)]
    pub streams_plan_text: bool,
    #[serde(default)]
    pub emits_message_completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ToolCapabilities {
    #[serde(default)]
    pub exposes_tool_item_ids: bool,
    #[serde(default)]
    pub emits_tool_started: bool,
    #[serde(default)]
    pub emits_tool_completed: bool,
    #[serde(default)]
    pub emits_tool_output: bool,
    #[serde(default)]
    pub supports_mcp_tools: bool,
    #[serde(default)]
    pub supports_dynamic_tool_callbacks: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalCapabilities {
    #[serde(default)]
    pub supports_command_approval: bool,
    #[serde(default)]
    pub supports_file_read_approval: bool,
    #[serde(default)]
    pub supports_file_change_approval: bool,
    #[serde(default)]
    pub supports_apply_patch_approval: bool,
    #[serde(default)]
    pub approvals_have_native_request_ids: bool,
    #[serde(default)]
    pub approval_callbacks_are_live_only: bool,
    #[serde(default)]
    pub approvals_can_originate_from_subagents: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PlanningCapabilities {
    #[serde(default)]
    pub emits_plan_updated: bool,
    #[serde(default)]
    pub emits_todo_list: bool,
    #[serde(default)]
    pub emits_proposed_plan: bool,
    #[serde(default)]
    pub supports_structured_questions: bool,
    #[serde(default)]
    pub plan_deltas_have_item_ids: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubagentCapabilities {
    #[serde(default)]
    pub supports_subagents: bool,
    #[serde(default)]
    pub exposes_subagent_thread_ids: bool,
    #[serde(default)]
    pub emits_subagent_lifecycle: bool,
    #[serde(default)]
    pub can_wait_for_subagents: bool,
    #[serde(default)]
    pub can_close_subagents: bool,
    #[serde(default)]
    pub can_fork_subagent_thread: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ContextCapabilities {
    #[serde(default)]
    pub accepts_system_context: bool,
    #[serde(default)]
    pub accepts_developer_context: bool,
    #[serde(default)]
    pub accepts_synthetic_user_context: bool,
    #[serde(default)]
    pub can_generate_summaries: bool,
    #[serde(default)]
    pub can_consume_handoff_summaries: bool,
    #[serde(default)]
    pub supports_delta_handoff: bool,
    #[serde(default)]
    pub supports_full_thread_handoff: bool,
    #[serde(default)]
    pub max_recommended_handoff_chars: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointCapabilities {
    #[serde(default)]
    pub app_can_checkpoint_filesystem: bool,
    #[serde(default)]
    pub supports_nested_checkpoint_scopes: bool,
    #[serde(default)]
    pub provider_can_rollback_conversation: bool,
    #[serde(default)]
    pub provider_rollback_returns_snapshot: bool,
    #[serde(default)]
    pub provider_can_read_conversation_snapshot: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct IdentityCapabilities {
    #[serde(default)]
    pub native_thread_ids: NativeRefStrength,
    #[serde(default)]
    pub native_turn_ids: NativeRefStrength,
    #[serde(default)]
    pub native_item_ids: NativeRefStrength,
    #[serde(default)]
    pub native_request_ids: NativeRefStrength,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuntimePolicyCapabilities {
    #[serde(default)]
    pub enforcement: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProviderCapabilities {
    #[serde(default)]
    pub sessions: SessionCapabilities,
    #[serde(default)]
    pub threads: ThreadCapabilities,
    #[serde(default)]
    pub turns: TurnCapabilities,
    #[serde(default)]
    pub streaming: StreamingCapabilities,
    #[serde(default)]
    pub tools: ToolCapabilities,
    #[serde(default)]
    pub approvals: ApprovalCapabilities,
    #[serde(default)]
    pub planning: PlanningCapabilities,
    #[serde(default)]
    pub subagents: SubagentCapabilities,
    #[serde(default)]
    pub context: ContextCapabilities,
    #[serde(default)]
    pub checkpointing: CheckpointCapabilities,
    #[serde(default)]
    pub identity: IdentityCapabilities,
    #[serde(default)]
    pub runtime_policy: RuntimePolicyCapabilities,
}

pub type ProviderSessionStatus = String;
pub const SESSION_STARTING: &str = "starting";
pub const SESSION_READY: &str = "ready";
pub const SESSION_RUNNING: &str = "running";
pub const SESSION_WAITING: &str = "waiting";
pub const SESSION_STOPPED: &str = "stopped";
pub const SESSION_ERROR: &str = "error";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderSession {
    #[serde(default)]
    pub id: ProviderSessionID,
    #[serde(default)]
    pub driver: ProviderDriverKind,
    #[serde(default)]
    pub provider_instance_id: ProviderInstanceID,
    #[serde(default)]
    pub status: ProviderSessionStatus,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub capabilities: ProviderCapabilities,
    #[serde(default = "default_zero_time")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_zero_time")]
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub last_error: Option<String>,
}

impl Default for ProviderSession {
    fn default() -> Self {
        Self {
            id: String::new(),
            driver: String::new(),
            provider_instance_id: String::new(),
            status: String::new(),
            cwd: String::new(),
            model: None,
            capabilities: ProviderCapabilities::default(),
            created_at: default_zero_time(),
            updated_at: default_zero_time(),
            last_error: None,
        }
    }
}

pub type ProviderThreadStatus = String;
pub const PROVIDER_THREAD_NOT_LOADED: &str = "not_loaded";
pub const PROVIDER_THREAD_IDLE: &str = "idle";
pub const PROVIDER_THREAD_ACTIVE: &str = "active";
pub const PROVIDER_THREAD_ARCHIVED: &str = "archived";
pub const PROVIDER_THREAD_CLOSED: &str = "closed";
pub const PROVIDER_THREAD_ERROR: &str = "error";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProviderThreadForkSource {
    #[serde(default)]
    pub provider_thread_id: ProviderThreadID,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn_id: Option<ProviderTurnID>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<CheckpointID>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderThread {
    #[serde(default)]
    pub id: ProviderThreadID,
    #[serde(default)]
    pub driver: ProviderDriverKind,
    #[serde(default)]
    pub provider_instance_id: ProviderInstanceID,
    #[serde(default)]
    pub provider_session_id: Option<ProviderSessionID>,
    #[serde(default)]
    pub app_thread_id: Option<ThreadID>,
    #[serde(default)]
    pub owner_node_id: Option<NodeID>,
    #[serde(default)]
    pub native_thread_ref: Option<ProviderRef>,
    #[serde(default)]
    pub native_conversation_head_ref: Option<ProviderRef>,
    #[serde(default)]
    pub status: ProviderThreadStatus,
    #[serde(default)]
    pub first_run_ordinal: Option<i64>,
    #[serde(default)]
    pub last_run_ordinal: Option<i64>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub handoff_ids: Vec<ContextHandoffID>,
    #[serde(default)]
    pub forked_from: Option<ProviderThreadForkSource>,
    #[serde(default = "default_zero_time")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_zero_time")]
    pub updated_at: DateTime<Utc>,
}

impl Default for ProviderThread {
    fn default() -> Self {
        Self {
            id: String::new(),
            driver: String::new(),
            provider_instance_id: String::new(),
            provider_session_id: None,
            app_thread_id: None,
            owner_node_id: None,
            native_thread_ref: None,
            native_conversation_head_ref: None,
            status: String::new(),
            first_run_ordinal: None,
            last_run_ordinal: None,
            handoff_ids: Vec::new(),
            forked_from: None,
            created_at: default_zero_time(),
            updated_at: default_zero_time(),
        }
    }
}

pub type ProviderTurnStatus = String;
pub const PROVIDER_TURN_PENDING: &str = "pending";
pub const PROVIDER_TURN_RUNNING: &str = "running";
pub const PROVIDER_TURN_COMPLETED: &str = "completed";
pub const PROVIDER_TURN_INTERRUPTED: &str = "interrupted";
pub const PROVIDER_TURN_FAILED: &str = "failed";
pub const PROVIDER_TURN_CANCELLED: &str = "cancelled";

pub fn provider_turn_status_is_terminal(status: &str) -> bool {
    matches!(
        status,
        PROVIDER_TURN_COMPLETED
            | PROVIDER_TURN_INTERRUPTED
            | PROVIDER_TURN_FAILED
            | PROVIDER_TURN_CANCELLED
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsage {
    #[serde(default)]
    pub used_tokens: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub input_tokens: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub cached_input_tokens: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub output_tokens: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub reasoning_output_tokens: i64,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "promptCacheTtlMs")]
    pub prompt_cache_ttl_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prompt_cache_expires_at: String,
    #[serde(default)]
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProviderTurn {
    #[serde(default)]
    pub id: ProviderTurnID,
    #[serde(default)]
    pub provider_thread_id: ProviderThreadID,
    #[serde(default)]
    pub node_id: NodeID,
    #[serde(default)]
    pub run_attempt_id: Option<RunAttemptID>,
    #[serde(default)]
    pub native_turn_ref: Option<ProviderRef>,
    #[serde(default)]
    pub ordinal: i64,
    #[serde(default)]
    pub status: ProviderTurnStatus,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<TokenUsage>,
}

impl ProviderTurn {
    pub fn is_terminal(&self) -> bool {
        provider_turn_status_is_terminal(&self.status)
    }
}

pub type RuntimeRequestKind = String;
pub const REQUEST_COMMAND: &str = "command";
pub const REQUEST_FILE_READ: &str = "file-read";
pub const REQUEST_FILE_CHANGE: &str = "file-change";
pub const REQUEST_MCP_ELICITATION: &str = "mcp-elicitation";
pub const REQUEST_PERMISSION: &str = "permission";
pub const REQUEST_DYNAMIC_TOOL_CALL: &str = "dynamic_tool_call";
pub const REQUEST_USER_INPUT: &str = "user_input";
pub const REQUEST_AUTH_REFRESH: &str = "auth_refresh";

pub type RuntimeRequestStatus = String;
pub const REQUEST_PENDING: &str = "pending";
pub const REQUEST_RESOLVED: &str = "resolved";
pub const REQUEST_EXPIRED: &str = "expired";
pub const REQUEST_CANCELLED: &str = "cancelled";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResponseCapability {
    #[serde(rename = "type", default)]
    pub capability_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<ProviderSessionID>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub supports_feedback: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRequest {
    #[serde(default)]
    pub id: RuntimeRequestID,
    #[serde(default)]
    pub node_id: NodeID,
    #[serde(default)]
    pub provider_turn_id: Option<ProviderTurnID>,
    #[serde(default)]
    pub native_request_ref: Option<ProviderRef>,
    #[serde(default)]
    pub kind: RuntimeRequestKind,
    #[serde(default)]
    pub status: RuntimeRequestStatus,
    #[serde(default)]
    pub response_capability: ResponseCapability,
    #[serde(default = "default_zero_time")]
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub resolved_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty", deserialize_with = "null_to_default")]
    pub answers: BTreeMap<String, Value>,
}

impl Default for RuntimeRequest {
    fn default() -> Self {
        Self {
            id: String::new(),
            node_id: String::new(),
            provider_turn_id: None,
            native_request_ref: None,
            kind: String::new(),
            status: String::new(),
            response_capability: ResponseCapability::default(),
            created_at: default_zero_time(),
            resolved_at: None,
            decision: None,
            feedback: None,
            answers: BTreeMap::new(),
        }
    }
}

pub type MessageRole = String;
pub const MESSAGE_USER: &str = "user";
pub const MESSAGE_ASSISTANT: &str = "assistant";
pub const MESSAGE_SYSTEM: &str = "system";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationMessage {
    #[serde(default)]
    pub created_by: Actor,
    #[serde(default)]
    pub creation_source: CreationSource,
    #[serde(default)]
    pub id: MessageID,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub run_id: Option<RunID>,
    #[serde(default)]
    pub node_id: Option<NodeID>,
    #[serde(default)]
    pub role: MessageRole,
    #[serde(default)]
    pub text: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub attachments: Vec<Attachment>,
    #[serde(default)]
    pub streaming: bool,
    #[serde(default = "default_zero_time")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_zero_time")]
    pub updated_at: DateTime<Utc>,
}

impl Default for ConversationMessage {
    fn default() -> Self {
        Self {
            created_by: String::new(),
            creation_source: String::new(),
            id: String::new(),
            thread_id: String::new(),
            run_id: None,
            node_id: None,
            role: String::new(),
            text: String::new(),
            attachments: Vec::new(),
            streaming: false,
            created_at: default_zero_time(),
            updated_at: default_zero_time(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PlanStep {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_anchor_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UserInputOption {
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub preview: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UserInputQuestion {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub header: String,
    #[serde(default)]
    pub question: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub options: Vec<UserInputOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PlanArtifact {
    #[serde(default)]
    pub id: PlanID,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub run_id: Option<RunID>,
    #[serde(default)]
    pub node_id: NodeID,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub markdown: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty", deserialize_with = "null_to_default")]
    pub steps: Vec<PlanStep>,
    #[serde(default, skip_serializing_if = "Vec::is_empty", deserialize_with = "null_to_default")]
    pub questions: Vec<UserInputQuestion>,
}

pub type TurnItemStatus = String;
pub const TURN_ITEM_PENDING: &str = "pending";
pub const TURN_ITEM_RUNNING: &str = "running";
pub const TURN_ITEM_WAITING: &str = "waiting";
pub const TURN_ITEM_COMPLETED: &str = "completed";
pub const TURN_ITEM_INTERRUPTED: &str = "interrupted";
pub const TURN_ITEM_FAILED: &str = "failed";
pub const TURN_ITEM_CANCELLED: &str = "cancelled";
pub const TURN_ITEM_DECLINED: &str = "declined";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnItem {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_by: Actor,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub creation_source: CreationSource,
    #[serde(default)]
    pub id: TurnItemID,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub run_id: Option<RunID>,
    #[serde(default)]
    pub node_id: Option<NodeID>,
    #[serde(default)]
    pub provider_thread_id: Option<ProviderThreadID>,
    #[serde(default)]
    pub provider_turn_id: Option<ProviderTurnID>,
    #[serde(default)]
    pub native_item_ref: Option<ProviderRef>,
    #[serde(default)]
    pub parent_item_id: Option<TurnItemID>,
    #[serde(default)]
    pub ordinal: i64,
    #[serde(default)]
    pub status: TurnItemStatus,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default = "default_zero_time")]
    pub updated_at: DateTime<Utc>,
    #[serde(rename = "type", default)]
    pub item_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message_id: MessageID,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_id: RuntimeRequestID,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty", deserialize_with = "null_to_default")]
    pub data: BTreeMap<String, Value>,
}

impl Default for TurnItem {
    fn default() -> Self {
        Self {
            created_by: String::new(),
            creation_source: String::new(),
            id: String::new(),
            thread_id: String::new(),
            run_id: None,
            node_id: None,
            provider_thread_id: None,
            provider_turn_id: None,
            native_item_ref: None,
            parent_item_id: None,
            ordinal: 0,
            status: String::new(),
            title: String::new(),
            started_at: None,
            completed_at: None,
            updated_at: default_zero_time(),
            item_type: String::new(),
            message_id: String::new(),
            text: String::new(),
            request_id: String::new(),
            data: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointScope {
    #[serde(default)]
    pub id: CheckpointScopeID,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub run_id: Option<RunID>,
    #[serde(default)]
    pub node_id: NodeID,
    #[serde(default)]
    pub parent_scope_id: Option<CheckpointScopeID>,
    #[serde(default)]
    pub provider_thread_id: Option<ProviderThreadID>,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub ordinal_within_parent: i64,
    #[serde(default)]
    pub advances_app_run_count: bool,
    #[serde(default)]
    pub cwd: String,
    #[serde(default = "default_zero_time")]
    pub created_at: DateTime<Utc>,
}

impl Default for CheckpointScope {
    fn default() -> Self {
        Self {
            id: String::new(),
            thread_id: String::new(),
            run_id: None,
            node_id: String::new(),
            parent_scope_id: None,
            provider_thread_id: None,
            kind: String::new(),
            ordinal_within_parent: 0,
            advances_app_run_count: false,
            cwd: String::new(),
            created_at: default_zero_time(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointFileSummary {
    #[serde(default)]
    pub path: String,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub additions: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub deletions: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    #[serde(default)]
    pub id: CheckpointID,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub scope_id: CheckpointScopeID,
    #[serde(default)]
    pub run_id: Option<RunID>,
    #[serde(default)]
    pub node_id: NodeID,
    #[serde(default)]
    pub parent_checkpoint_id: Option<CheckpointID>,
    #[serde(default)]
    pub ordinal_within_scope: i64,
    #[serde(default)]
    pub app_run_ordinal: Option<i64>,
    #[serde(rename = "ref", default)]
    pub checkpoint_ref: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub files: Vec<CheckpointFileSummary>,
    #[serde(default = "default_zero_time")]
    pub captured_at: DateTime<Utc>,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            id: String::new(),
            thread_id: String::new(),
            scope_id: String::new(),
            run_id: None,
            node_id: String::new(),
            parent_checkpoint_id: None,
            ordinal_within_scope: 0,
            app_run_ordinal: None,
            checkpoint_ref: String::new(),
            status: String::new(),
            files: Vec::new(),
            captured_at: default_zero_time(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ContextSourcePoint {
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunID>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<CheckpointID>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_item_id: Option<TurnItemID>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_thread_ref: Option<ProviderRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn_ref: Option<ProviderRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ContextTransferResolution {
    #[serde(default)]
    pub strategy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_thread_ref: Option<ProviderRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_handoff_id: Option<ContextHandoffID>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextTransfer {
    #[serde(default)]
    pub id: ContextTransferID,
    #[serde(rename = "type", default)]
    pub transfer_type: String,
    #[serde(default)]
    pub source_thread_id: ThreadID,
    #[serde(default)]
    pub target_thread_id: ThreadID,
    #[serde(default)]
    pub source_point: ContextSourcePoint,
    #[serde(default)]
    pub base_point: Option<ContextSourcePoint>,
    #[serde(default)]
    pub source_provider_instance_id: Option<ProviderInstanceID>,
    #[serde(default)]
    pub target_provider_instance_id: Option<ProviderInstanceID>,
    #[serde(default)]
    pub target_run_id: Option<RunID>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub resolution: Option<ContextTransferResolution>,
    #[serde(default)]
    pub created_by: Actor,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default = "default_zero_time")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_zero_time")]
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub consumed_at: Option<DateTime<Utc>>,
}

impl Default for ContextTransfer {
    fn default() -> Self {
        Self {
            id: String::new(),
            transfer_type: String::new(),
            source_thread_id: String::new(),
            target_thread_id: String::new(),
            source_point: ContextSourcePoint::default(),
            base_point: None,
            source_provider_instance_id: None,
            target_provider_instance_id: None,
            target_run_id: None,
            status: String::new(),
            resolution: None,
            created_by: String::new(),
            error: None,
            created_at: default_zero_time(),
            updated_at: default_zero_time(),
            consumed_at: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CoveredRunOrdinals {
    #[serde(default)]
    pub from: i64,
    #[serde(default)]
    pub to: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextHandoff {
    #[serde(default)]
    pub id: ContextHandoffID,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<ContextTransferID>,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub target_run_id: RunID,
    #[serde(default, deserialize_with = "null_to_default")]
    pub from_provider_thread_ids: Vec<ProviderThreadID>,
    #[serde(default)]
    pub to_provider_thread_id: ProviderThreadID,
    #[serde(default)]
    pub covered_run_ordinals: CoveredRunOrdinals,
    #[serde(default)]
    pub strategy: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub summary_message_id: Option<MessageID>,
    #[serde(default)]
    pub summary_text: String,
    #[serde(default)]
    pub created_by_provider_instance_id: Option<ProviderInstanceID>,
    #[serde(default = "default_zero_time")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_zero_time")]
    pub updated_at: DateTime<Utc>,
}

impl Default for ContextHandoff {
    fn default() -> Self {
        Self {
            id: String::new(),
            transfer_id: None,
            thread_id: String::new(),
            target_run_id: String::new(),
            from_provider_thread_ids: Vec::new(),
            to_provider_thread_id: String::new(),
            covered_run_ordinals: CoveredRunOrdinals::default(),
            strategy: String::new(),
            status: String::new(),
            summary_message_id: None,
            summary_text: String::new(),
            created_by_provider_instance_id: None,
            created_at: default_zero_time(),
            updated_at: default_zero_time(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Projection {
    #[serde(default)]
    pub thread: Thread,
    #[serde(default, deserialize_with = "null_to_default")]
    pub runs: Vec<Run>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub attempts: Vec<RunAttempt>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub nodes: Vec<ExecutionNode>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub subagents: Vec<Subagent>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub provider_sessions: Vec<ProviderSession>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub provider_threads: Vec<ProviderThread>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub provider_turns: Vec<ProviderTurn>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub runtime_requests: Vec<RuntimeRequest>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub messages: Vec<ConversationMessage>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub plans: Vec<PlanArtifact>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub turn_items: Vec<TurnItem>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub checkpoint_scopes: Vec<CheckpointScope>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub checkpoints: Vec<Checkpoint>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub context_handoffs: Vec<ContextHandoff>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub context_transfers: Vec<ContextTransfer>,
    #[serde(default = "default_zero_time")]
    pub updated_at: DateTime<Utc>,
}

impl Default for Projection {
    fn default() -> Self {
        Self {
            thread: Thread::default(),
            runs: Vec::new(),
            attempts: Vec::new(),
            nodes: Vec::new(),
            subagents: Vec::new(),
            provider_sessions: Vec::new(),
            provider_threads: Vec::new(),
            provider_turns: Vec::new(),
            runtime_requests: Vec::new(),
            messages: Vec::new(),
            plans: Vec::new(),
            turn_items: Vec::new(),
            checkpoint_scopes: Vec::new(),
            checkpoints: Vec::new(),
            context_handoffs: Vec::new(),
            context_transfers: Vec::new(),
            updated_at: default_zero_time(),
        }
    }
}

impl Projection {
    pub fn run(&self, id: &str) -> Option<Run> {
        self.runs.iter().find(|r| r.id == id).cloned()
    }

    pub fn node(&self, id: &str) -> Option<ExecutionNode> {
        self.nodes.iter().find(|n| n.id == id).cloned()
    }

    pub fn provider_thread(&self, id: &str) -> Option<ProviderThread> {
        self.provider_threads.iter().find(|t| t.id == id).cloned()
    }

    pub fn provider_turn(&self, id: &str) -> Option<ProviderTurn> {
        self.provider_turns.iter().find(|t| t.id == id).cloned()
    }

    pub fn runtime_request(&self, id: &str) -> Option<RuntimeRequest> {
        self.runtime_requests.iter().find(|r| r.id == id).cloned()
    }

    pub fn active_run(&self) -> Option<Run> {
        self.runs.iter().rev().find(|r| r.is_active()).cloned()
    }

    pub fn next_run_ordinal(&self) -> i64 {
        let mut next = 1;
        for run in &self.runs {
            if run.ordinal >= next {
                next = run.ordinal + 1;
            }
        }
        next
    }
}

pub fn latest_provider_turn_for_attempt(
    turns: &[ProviderTurn],
    attempt_id: &str,
) -> Option<ProviderTurn> {
    let mut latest: Option<ProviderTurn> = None;
    for turn in turns {
        if turn.run_attempt_id.as_deref() != Some(attempt_id) {
            continue;
        }
        if latest.as_ref().is_none_or(|l| turn.ordinal > l.ordinal) {
            latest = Some(turn.clone());
        }
    }
    latest
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSnapshot {
    #[serde(default)]
    pub projection: Projection,
    #[serde(default)]
    pub snapshot_sequence: u64,
}
