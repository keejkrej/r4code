use std::collections::{BTreeMap, HashMap};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value};

use crate::orchestration::command::{RuntimeRequestRespondEffect, RuntimeRequestResponse};
use crate::orchestration::errors::{Error, ErrorKind};
use crate::orchestration::ids::{
    derive_node_from_provider_item_id, derive_turn_item_from_provider_item_id, NodeID,
    RuntimeRequestID, TurnItemID,
};
use crate::orchestration::outbox::ExecutionContext;
use crate::orchestration::provider::{
    ProviderEvent, PROVIDER_EVENT_NODE_UPDATED, PROVIDER_EVENT_RUNTIME_REQUEST_UPDATED,
    PROVIDER_EVENT_TURN_ITEM_UPDATED,
};
use crate::orchestration::types::{
    ExecutionNode, NodeStatus, ProviderRef, ResponseCapability, RuntimeRequest, RuntimeRequestKind,
    TurnItem, TurnItemStatus, ACTOR_AGENT, CREATION_PROVIDER, NATIVE_REF_STRONG,
    NODE_APPROVAL_REQUEST, NODE_PENDING, NODE_RUNNING, NODE_TOOL_CALL, NODE_WAITING,
    REQUEST_PENDING, TURN_ITEM_CANCELLED, TURN_ITEM_COMPLETED, TURN_ITEM_FAILED,
    TURN_ITEM_INTERRUPTED, TURN_ITEM_PENDING, TURN_ITEM_WAITING,
};

use super::adapter::{stable_id, EmitFn, RunRequest};

#[derive(Debug, Clone, Default)]
pub struct ToolCall {
    pub id: String,
    pub item_type: String,
    pub title: String,
    pub status: TurnItemStatus,
    pub input: Value,
    pub output: Value,
    pub exit_code: Option<i64>,
    pub file_name: String,
    pub diff: String,
}

pub fn emit_tool_call(
    ctx: &ExecutionContext,
    request: &RunRequest,
    emit: &EmitFn,
    call: ToolCall,
    raw: &[u8],
) -> Result<(), Error> {
    let now = Utc::now();
    let provider_thread = request.current_provider_thread();
    let provider_turn = request.current_turn();
    let node_id = derive_node_from_provider_item_id(&provider_thread.driver, &call.id);
    let mut node_status: NodeStatus = NODE_RUNNING.to_string();
    match call.status.as_str() {
        TURN_ITEM_PENDING | TURN_ITEM_WAITING => {
            node_status = call.status.clone();
        }
        TURN_ITEM_COMPLETED | TURN_ITEM_FAILED | TURN_ITEM_CANCELLED | TURN_ITEM_INTERRUPTED => {
            node_status = call.status.clone();
        }
        _ => {}
    }
    let completed_at = if node_status != NODE_RUNNING
        && node_status != NODE_PENDING
        && node_status != NODE_WAITING
    {
        Some(now)
    } else {
        None
    };
    let native_ref = ProviderRef {
        driver: provider_thread.driver.clone(),
        native_id: Some(call.id.clone()),
        strength: NATIVE_REF_STRONG.to_string(),
        ..Default::default()
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
        kind: NODE_TOOL_CALL.to_string(),
        status: node_status,
        counts_for_run: false,
        provider_thread_id: Some(provider_thread.id.clone()),
        provider_turn_id: Some(provider_turn.id.clone()),
        native_item_ref: Some(native_ref.clone()),
        runtime_request_id: None,
        checkpoint_scope_id: None,
        started_at: Some(now),
        completed_at,
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

    let mut data = BTreeMap::new();
    data.insert("input".to_string(), call.input);
    data.insert("output".to_string(), call.output);
    if let Some(code) = call.exit_code {
        data.insert("exitCode".to_string(), json!(code));
    }
    if !call.file_name.is_empty() {
        data.insert("fileName".to_string(), Value::String(call.file_name));
    }
    if !call.diff.is_empty() {
        data.insert("diffStr".to_string(), Value::String(call.diff));
    }
    let item = TurnItem {
        created_by: ACTOR_AGENT.to_string(),
        creation_source: CREATION_PROVIDER.to_string(),
        id: derive_turn_item_from_provider_item_id(&provider_thread.driver, &call.id),
        thread_id: request.start.thread.id.clone(),
        run_id: Some(request.start.run.id.clone()),
        node_id: Some(node_id),
        provider_thread_id: Some(provider_thread.id),
        provider_turn_id: Some(provider_turn.id),
        native_item_ref: Some(native_ref),
        ordinal: 1,
        status: call.status,
        title: call.title,
        item_type: call.item_type,
        updated_at: now,
        completed_at,
        data,
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

#[derive(Clone)]
struct PendingNativeRequest {
    _id: RuntimeRequestID,
    sender: mpsc::Sender<RuntimeRequestResponse>,
}

#[derive(Clone, Default)]
pub struct RequestBroker {
    pending: Arc<Mutex<HashMap<String, PendingNativeRequest>>>,
    closed: Arc<Mutex<bool>>,
}

impl RequestBroker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn has_pending(&self, native_id: &str) -> bool {
        let guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        guard.contains_key(native_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn ask(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        emit: &EmitFn,
        native_id: &str,
        kind: RuntimeRequestKind,
        title: &str,
        input: Value,
    ) -> Result<RuntimeRequestResponse, Error> {
        let provider_turn_id = request.current_turn().id;
        let provider_thread = request.current_provider_thread();
        let session = request.current_session();
        let request_id: RuntimeRequestID = stable_id(
            "runtime-request:cli:",
            &format!("{}:{}", provider_turn_id, native_id),
        );
        let node_id: NodeID = format!("node:runtime-request:{}", request_id);
        let native_ref = ProviderRef {
            driver: provider_thread.driver.clone(),
            native_id: Some(native_id.to_string()),
            strength: NATIVE_REF_STRONG.to_string(),
            ..Default::default()
        };
        let now = Utc::now();
        let runtime_request = RuntimeRequest {
            id: request_id.clone(),
            node_id: node_id.clone(),
            provider_turn_id: Some(provider_turn_id.clone()),
            native_request_ref: Some(native_ref),
            kind,
            status: REQUEST_PENDING.to_string(),
            response_capability: ResponseCapability {
                capability_type: "live".to_string(),
                provider_session_id: Some(session.id),
                ..Default::default()
            },
            created_at: now,
            ..Default::default()
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
            kind: NODE_APPROVAL_REQUEST.to_string(),
            status: NODE_WAITING.to_string(),
            counts_for_run: false,
            provider_thread_id: Some(provider_thread.id.clone()),
            provider_turn_id: Some(provider_turn_id.clone()),
            runtime_request_id: Some(request_id.clone()),
            started_at: Some(now),
            ..Default::default()
        };
        let mut data = BTreeMap::new();
        data.insert("input".to_string(), input);
        let item_id: TurnItemID = format!("turn-item:runtime-request:{}", request_id);
        let item = TurnItem {
            created_by: ACTOR_AGENT.to_string(),
            creation_source: CREATION_PROVIDER.to_string(),
            id: item_id,
            thread_id: request.start.thread.id.clone(),
            run_id: Some(request.start.run.id.clone()),
            node_id: Some(node_id),
            provider_thread_id: Some(provider_thread.id),
            provider_turn_id: Some(provider_turn_id),
            status: TURN_ITEM_WAITING.to_string(),
            title: title.to_string(),
            item_type: "approval_request".to_string(),
            request_id: request_id.clone(),
            updated_at: now,
            data,
            ..Default::default()
        };

        emit(
            ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_NODE_UPDATED.to_string(),
                node: Some(node),
                ..Default::default()
            },
            &[],
        )?;
        emit(
            ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_RUNTIME_REQUEST_UPDATED.to_string(),
                runtime_request: Some(runtime_request),
                ..Default::default()
            },
            &[],
        )?;
        emit(
            ctx,
            ProviderEvent {
                event_type: PROVIDER_EVENT_TURN_ITEM_UPDATED.to_string(),
                turn_item: Some(item),
                ..Default::default()
            },
            &[],
        )?;

        let (tx, rx) = mpsc::channel();
        {
            let mut guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            guard.insert(
                native_id.to_string(),
                PendingNativeRequest {
                    _id: request_id,
                    sender: tx,
                },
            );
        }

        let res = loop {
            if ctx.is_cancelled() {
                break Err(Error::provider("execution context cancelled"));
            }
            if *self.closed.lock().unwrap_or_else(|e| e.into_inner()) {
                break Ok(RuntimeRequestResponse {
                    decision: Some("cancel".to_string()),
                    ..Default::default()
                });
            }
            match rx.recv_timeout(Duration::from_millis(20)) {
                Ok(resp) => break Ok(resp),
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break Ok(RuntimeRequestResponse {
                        decision: Some("cancel".to_string()),
                        ..Default::default()
                    });
                }
            }
        };

        {
            let mut guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            guard.remove(native_id);
        }
        res
    }

    pub fn respond(&self, response: RuntimeRequestRespondEffect) -> Result<(), Error> {
        let Some(ref native_ref) = response.request.native_request_ref else {
            return Err(Error::provider(format!(
                "runtime request {} has no native ref",
                response.request.id
            )));
        };
        let Some(ref native_id) = native_ref.native_id else {
            return Err(Error::provider(format!(
                "runtime request {} has no native ref",
                response.request.id
            )));
        };
        let pending = {
            let guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            guard.get(native_id).cloned()
        };
        let Some(pending) = pending else {
            return Err(Error::with_detail(
                ErrorKind::RequestNotRespondable,
                format!("runtime request {} is no longer live", response.request.id),
            ));
        };
        let _ = pending.sender.send(response.response);
        Ok(())
    }

    pub fn cancel_all(&self) {
        let list: Vec<PendingNativeRequest> = {
            let mut guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            let v = guard.values().cloned().collect();
            guard.clear();
            v
        };
        for req in list {
            let _ = req.sender.send(RuntimeRequestResponse {
                decision: Some("cancel".to_string()),
                ..Default::default()
            });
        }
    }

    pub fn close(&self) {
        *self.closed.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.cancel_all();
    }
}
