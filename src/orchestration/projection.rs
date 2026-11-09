use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::orchestration::errors::*;
use crate::orchestration::event::*;
use crate::orchestration::ids::*;
use crate::orchestration::types::*;

pub(crate) fn empty_projection(event: &Event, thread: Thread) -> Projection {
    Projection {
        thread,
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
        updated_at: event.occurred_at,
    }
}

pub(crate) fn upsert_by_id<T: Clone, F: Fn(&T) -> &str>(items: &[T], next: T, id_fn: F) -> Vec<T> {
    let next_id = id_fn(&next);
    for (index, item) in items.iter().enumerate() {
        if id_fn(item) == next_id {
            let mut updated = items.to_vec();
            updated[index] = next;
            return updated;
        }
    }
    let mut updated = items.to_vec();
    updated.push(next);
    updated
}

pub(crate) fn is_queued_provider_thread_placeholder(thread: &ProviderThread) -> bool {
    thread.status == PROVIDER_THREAD_NOT_LOADED
        && thread.first_run_ordinal.is_none()
        && thread.native_thread_ref.is_none()
        && thread.provider_session_id.is_none()
}

fn payload_type_error(event: &Event, expected: &str) -> Error {
    Error::with_detail(
        ErrorKind::InvalidProviderEvent,
        format!(
            "{} payload must be {}, got {}",
            event.event_type,
            expected,
            event.payload.type_name()
        ),
    )
}

/// Apply folds one normalized event into a projection. Entity rows are upserted
/// in insertion order, making the result stable for replay and tests.
pub fn apply(projection: &Projection, event: &Event) -> Result<Projection, Error> {
    if event.thread_id.is_empty() || is_zero_time(&event.occurred_at) {
        return Err(Error::with_detail(
            ErrorKind::InvalidProviderEvent,
            "event thread and occurredAt are required",
        ));
    }

    if event.event_type == EVENT_THREAD_CREATED {
        let EventPayload::Thread(thread) = &event.payload else {
            return Err(Error::with_detail(
                ErrorKind::InvalidProviderEvent,
                format!("thread.created payload is {}", event.payload.type_name()),
            ));
        };
        return Ok(empty_projection(event, thread.clone()));
    }
    if projection.thread.id.is_empty() {
        return Err(Error::from(ErrorKind::ThreadNotFound));
    }

    let mut base = projection.clone();
    base.updated_at = event.occurred_at;
    base.thread.updated_at = event.occurred_at;

    match event.event_type.as_str() {
        EVENT_THREAD_ARCHIVED
        | EVENT_THREAD_UNARCHIVED
        | EVENT_THREAD_DELETED
        | EVENT_THREAD_SETTLED
        | EVENT_THREAD_UNSETTLED
        | EVENT_THREAD_SNOOZED
        | EVENT_THREAD_UNSNOOZED
        | EVENT_THREAD_PINNED
        | EVENT_THREAD_UNPINNED
        | EVENT_THREAD_METADATA_UPDATED
        | EVENT_THREAD_RUNTIME_MODE_UPDATED
        | EVENT_THREAD_INTERACTION_MODE_UPDATED
        | EVENT_THREAD_MODEL_SELECTION_UPDATED
        | EVENT_THREAD_PROVIDER_SWITCHED => {
            let EventPayload::Thread(thread) = &event.payload else {
                return Err(payload_type_error(event, "Thread"));
            };
            base.thread = thread.clone();
        }
        EVENT_THREAD_VISITED | EVENT_THREAD_MARKED_UNREAD => {
            let EventPayload::Thread(thread) = &event.payload else {
                return Err(payload_type_error(event, "Thread"));
            };
            // Read watermarks do not constitute thread activity.
            base = projection.clone();
            base.thread = thread.clone();
        }
        EVENT_RUN_CREATED | EVENT_RUN_UPDATED => {
            let EventPayload::Run(run) = &event.payload else {
                return Err(payload_type_error(event, "Run"));
            };
            base.runs = upsert_by_id(&base.runs, run.clone(), |row| &row.id);
        }
        EVENT_RUN_BACKGROUND_WORK_CANCELLED => {
            let EventPayload::RunBackgroundWorkCancelled(_) = &event.payload else {
                return Err(payload_type_error(event, "RunBackgroundWorkCancelled"));
            };
        }
        EVENT_RUN_ATTEMPT_CREATED | EVENT_RUN_ATTEMPT_UPDATED => {
            let EventPayload::RunAttempt(attempt) = &event.payload else {
                return Err(payload_type_error(event, "RunAttempt"));
            };
            base.attempts = upsert_by_id(&base.attempts, attempt.clone(), |row| &row.id);
        }
        EVENT_NODE_UPDATED => {
            let EventPayload::ExecutionNode(node) = &event.payload else {
                return Err(payload_type_error(event, "ExecutionNode"));
            };
            base.nodes = upsert_by_id(&base.nodes, node.clone(), |row| &row.id);
        }
        EVENT_SUBAGENT_UPDATED => {
            let EventPayload::Subagent(subagent) = &event.payload else {
                return Err(payload_type_error(event, "Subagent"));
            };
            base.subagents = upsert_by_id(&base.subagents, subagent.clone(), |row| &row.id);
        }
        EVENT_PROVIDER_SESSION_ATTACHED | EVENT_PROVIDER_SESSION_UPDATED => {
            let EventPayload::ProviderSession(session) = &event.payload else {
                return Err(payload_type_error(event, "ProviderSession"));
            };
            base.provider_sessions =
                upsert_by_id(&base.provider_sessions, session.clone(), |row| &row.id);
        }
        EVENT_PROVIDER_SESSION_DETACHED => {
            let EventPayload::ProviderSessionDetached(detached) = &event.payload else {
                return Err(payload_type_error(event, "ProviderSessionDetached"));
            };
            base.provider_sessions
                .retain(|session| session.id != detached.provider_session_id);
        }
        EVENT_PROVIDER_THREAD_UPDATED => {
            let EventPayload::ProviderThread(thread) = &event.payload else {
                return Err(payload_type_error(event, "ProviderThread"));
            };
            if thread.app_thread_id.as_deref() == Some(base.thread.id.as_str())
                && !is_queued_provider_thread_placeholder(thread)
            {
                base.thread.active_provider_thread_id = Some(thread.id.clone());
            }
            base.provider_threads =
                upsert_by_id(&base.provider_threads, thread.clone(), |row| &row.id);
        }
        EVENT_PROVIDER_TURN_UPDATED => {
            let EventPayload::ProviderTurn(turn) = &event.payload else {
                return Err(payload_type_error(event, "ProviderTurn"));
            };
            let mut turn = turn.clone();
            if turn.token_usage.is_none() {
                for current in &base.provider_turns {
                    if current.id == turn.id {
                        turn.token_usage = current.token_usage.clone();
                        break;
                    }
                }
            }
            base.provider_turns = upsert_by_id(&base.provider_turns, turn, |row| &row.id);
        }
        EVENT_RUNTIME_REQUEST_UPDATED => {
            let EventPayload::RuntimeRequest(request) = &event.payload else {
                return Err(payload_type_error(event, "RuntimeRequest"));
            };
            base.runtime_requests =
                upsert_by_id(&base.runtime_requests, request.clone(), |row| &row.id);
        }
        EVENT_MESSAGE_UPDATED => {
            let EventPayload::ConversationMessage(message) = &event.payload else {
                return Err(payload_type_error(event, "ConversationMessage"));
            };
            base.messages = upsert_by_id(&base.messages, message.clone(), |row| &row.id);
            let mut run_ordinals: HashMap<RunID, i64> = HashMap::with_capacity(base.runs.len());
            for run in &base.runs {
                run_ordinals.insert(run.id.clone(), run.ordinal);
            }
            // Conversation history is grouped by app run even when a queued user
            // message was durably accepted before the previous assistant finished.
            base.messages.sort_by(|left, right| {
                let left_ordinal = left
                    .run_id
                    .as_ref()
                    .map(|id| run_ordinals.get(id).copied().unwrap_or(0))
                    .unwrap_or(-1);
                let right_ordinal = right
                    .run_id
                    .as_ref()
                    .map(|id| run_ordinals.get(id).copied().unwrap_or(0))
                    .unwrap_or(-1);
                if left_ordinal != right_ordinal {
                    return left_ordinal.cmp(&right_ordinal);
                }
                if left.role == MESSAGE_USER && right.role != MESSAGE_USER {
                    std::cmp::Ordering::Less
                } else if left.role != MESSAGE_USER && right.role == MESSAGE_USER {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            });
        }
        EVENT_TURN_ITEM_UPDATED => {
            let EventPayload::TurnItem(item) = &event.payload else {
                return Err(payload_type_error(event, "TurnItem"));
            };
            base.turn_items = upsert_by_id(&base.turn_items, item.clone(), |row| &row.id);
        }
        EVENT_PLAN_UPDATED => {
            let EventPayload::PlanArtifact(plan) = &event.payload else {
                return Err(payload_type_error(event, "PlanArtifact"));
            };
            base.plans = upsert_by_id(&base.plans, plan.clone(), |row| &row.id);
        }
        EVENT_CHECKPOINT_SCOPE_CREATED => {
            let EventPayload::CheckpointScope(scope) = &event.payload else {
                return Err(payload_type_error(event, "CheckpointScope"));
            };
            base.checkpoint_scopes =
                upsert_by_id(&base.checkpoint_scopes, scope.clone(), |row| &row.id);
        }
        EVENT_CHECKPOINT_CAPTURED => {
            let EventPayload::Checkpoint(checkpoint) = &event.payload else {
                return Err(payload_type_error(event, "Checkpoint"));
            };
            base.checkpoints = upsert_by_id(&base.checkpoints, checkpoint.clone(), |row| &row.id);
        }
        EVENT_CHECKPOINT_ROLLBACK_REQUESTED => {
            let EventPayload::CheckpointRollbackRequest(_) = &event.payload else {
                return Err(payload_type_error(event, "CheckpointRollbackRequest"));
            };
        }
        EVENT_CONTEXT_HANDOFF_UPDATED => {
            let EventPayload::ContextHandoff(handoff) = &event.payload else {
                return Err(payload_type_error(event, "ContextHandoff"));
            };
            base.context_handoffs =
                upsert_by_id(&base.context_handoffs, handoff.clone(), |row| &row.id);
        }
        EVENT_CONTEXT_TRANSFER_CREATED | EVENT_CONTEXT_TRANSFER_UPDATED => {
            let EventPayload::ContextTransfer(transfer) = &event.payload else {
                return Err(payload_type_error(event, "ContextTransfer"));
            };
            base.context_transfers =
                upsert_by_id(&base.context_transfers, transfer.clone(), |row| &row.id);
        }
        _ => {
            // Unknown newer events still advance the cursor, but cannot mutate rows
            // whose semantics this version does not understand.
        }
    }
    Ok(base)
}

/// Replay rebuilds projections from the durable log. Sequence order, rather
/// than provider timestamps or IDs, is authoritative.
pub fn replay(events: &[StoredEvent]) -> Result<HashMap<ThreadID, Projection>, Error> {
    let mut ordered = events.to_vec();
    ordered.sort_by_key(|e| e.sequence);
    let mut projections: HashMap<ThreadID, Projection> = HashMap::new();
    for stored in &ordered {
        let event = &stored.event;
        let current = projections.get(&event.thread_id).cloned().unwrap_or_default();
        let next = apply(&current, event).map_err(|err| {
            Error::with_detail(
                err.kind,
                format!("replay event {} ({}): {}", stored.sequence, event.event_type, err),
            )
        })?;
        projections.insert(event.thread_id.clone(), next);
    }
    Ok(projections)
}

pub(crate) fn latest_run(runs: &[Run]) -> Option<Run> {
    let mut iter = runs.iter();
    let mut latest = iter.next()?.clone();
    for run in iter {
        if run.ordinal > latest.ordinal {
            latest = run.clone();
        }
    }
    Some(latest)
}

pub(crate) fn update_time(value: Option<DateTime<Utc>>, now: DateTime<Utc>) -> DateTime<Utc> {
    value.unwrap_or(now)
}
