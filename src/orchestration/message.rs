use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Duration, Utc};
use serde_json::json;

use crate::orchestration::command::*;
use crate::orchestration::errors::*;
use crate::orchestration::event::*;
use crate::orchestration::ids::*;
use crate::orchestration::orchestrator::*;
use crate::orchestration::projection::*;
use crate::orchestration::queue::*;
use crate::orchestration::types::*;

impl Orchestrator {
    pub(crate) fn plan_message_dispatch(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        let command = plan
            .command
            .payload
            .as_dispatch_message()
            .cloned()
            .unwrap_or_default();
        if plan.projection.thread.archived_at.is_some()
            || plan.projection.thread.deleted_at.is_some()
        {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                "archived or deleted threads cannot receive messages",
            ));
        }
        for message in &plan.projection.messages {
            if message.id == command.message_id {
                return Err(Error::with_detail(
                    ErrorKind::CommandRejected,
                    format!("message {} already exists", command.message_id),
                ));
            }
        }
        let (actor, source) = creation_defaults(&command.created_by, &command.creation_source);

        if plan.projection.thread.last_visited_at.is_none() && actor == ACTOR_USER {
            let mut thread = plan.projection.thread.clone();
            let visited_at = plan.now + Duration::nanoseconds(-1_000_000);
            thread.last_visited_at = Some(visited_at);
            thread.updated_at = plan.now;
            let thread_id = thread.id.clone();
            let instance_id = thread.provider_instance_id.clone();
            plan.emit_and_apply(EventSpec {
                event_type: EVENT_THREAD_METADATA_UPDATED.to_string(),
                thread_id,
                instance_id,
                payload: EventPayload::Thread(thread),
                ..Default::default()
            })?;
        }
        if !command.title_seed.is_empty() && plan.projection.messages.is_empty() {
            let mut thread = plan.projection.thread.clone();
            thread.title = command.title_seed.clone();
            thread.updated_at = plan.now;
            let thread_id = thread.id.clone();
            let instance_id = thread.provider_instance_id.clone();
            plan.emit_and_apply(EventSpec {
                event_type: EVENT_THREAD_METADATA_UPDATED.to_string(),
                thread_id,
                instance_id,
                payload: EventPayload::Thread(thread),
                ..Default::default()
            })?;
        }

        let model = command
            .model_selection
            .clone()
            .unwrap_or_else(|| plan.projection.thread.model_selection.clone());
        let provider = self.resolve_provider(&model.instance_id).map_err(|err| {
            Error::with_detail(
                err.kind,
                format!("resolve provider {}: {}", model.instance_id, err),
            )
        })?;

        if !plan.projection.thread.model_selection.equal(&model) {
            let mut thread = plan.projection.thread.clone();
            let event_type = if thread.provider_instance_id != model.instance_id {
                EVENT_THREAD_PROVIDER_SWITCHED
            } else {
                EVENT_THREAD_MODEL_SELECTION_UPDATED
            };
            thread.model_selection = model.clone();
            thread.provider_instance_id = model.instance_id.clone();
            thread.updated_at = plan.now;
            let thread_id = thread.id.clone();
            plan.emit_and_apply(EventSpec {
                event_type: event_type.to_string(),
                thread_id,
                instance_id: model.instance_id.clone(),
                payload: EventPayload::Thread(thread),
                ..Default::default()
            })?;
        }

        let mut mode = command.dispatch_mode.clone();
        if mode.mode_type.is_empty() {
            mode.mode_type = DISPATCH_START_IMMEDIATELY.to_string();
        }
        if mode.mode_type == DISPATCH_STEER_ACTIVE || mode.mode_type == DISPATCH_RESTART_ACTIVE {
            return Err(Error::with_detail(
                ErrorKind::UnsupportedCapability,
                format!("{} requires the provider steering adapter", mode.mode_type),
            ));
        }

        let has_active_run = plan.projection.active_run().is_some();
        let mut queue_held = queue_is_held(&plan.projection);
        let user_releases_queue =
            actor == ACTOR_USER && (source == CREATION_WEB || source == CREATION_MOBILE);
        if queue_held && user_releases_queue {
            release_queue_holds(plan)?;
            queue_held = false;
        }
        let queued = has_active_run
            || mode.mode_type == DISPATCH_QUEUE_AFTER_ACTIVE
            || (queue_held && actor != ACTOR_USER);
        if queued && !provider.capabilities.turns.supports_queued_messages {
            return Err(Error::with_detail(
                ErrorKind::UnsupportedCapability,
                format!("provider {} does not support queued messages", model.instance_id),
            ));
        }

        let ordinal = plan.projection.next_run_ordinal();
        let run_id = derive_run_id(&plan.command.thread_id, ordinal);
        let (mut provider_thread, mut provider_thread_new) = select_provider_thread(
            &plan.projection,
            &provider.driver,
            &model.instance_id,
            &run_id,
            plan.now,
        );

        let mut handoff: Option<ContextHandoff> = None;
        self.resolve_pending_fork(
            plan,
            &provider,
            &mut provider_thread,
            &run_id,
            &mut handoff,
        )?;
        if handoff.is_none() {
            if let Some(ref active_provider_id) = plan.projection.thread.active_provider_thread_id {
                if let Some(active_thread) = plan.projection.provider_thread(active_provider_id) {
                    if active_thread.provider_instance_id != model.instance_id {
                        handoff = self.plan_provider_handoff(
                            plan,
                            &active_thread,
                            &provider_thread,
                            &run_id,
                        )?;
                    }
                }
            }
        }
        if let Some(ref h) = handoff {
            provider_thread.handoff_ids =
                append_unique_handoff(&provider_thread.handoff_ids, &h.id);
            provider_thread.updated_at = plan.now;
            provider_thread_new = true;
        }

        let mut status = RUN_STARTING.to_string();
        if mode.mode_type == DISPATCH_DEFER_START {
            status = RUN_PREPARING.to_string();
        }
        if queued {
            status = RUN_QUEUED.to_string();
        }
        if status == RUN_STARTING || status == RUN_PREPARING {
            if provider_thread.first_run_ordinal.is_none() {
                provider_thread.first_run_ordinal = Some(ordinal);
            }
            provider_thread.last_run_ordinal = Some(ordinal);
            provider_thread.updated_at = plan.now;
            provider_thread_new = true;
        }
        if provider_thread_new {
            plan.emit_and_apply(EventSpec {
                event_type: EVENT_PROVIDER_THREAD_UPDATED.to_string(),
                thread_id: plan.command.thread_id.clone(),
                driver: provider.driver.clone(),
                instance_id: model.instance_id.clone(),
                payload: EventPayload::ProviderThread(provider_thread.clone()),
                ..Default::default()
            })?;
        }
        if let Some(ref h) = handoff {
            plan.emit_and_apply(EventSpec {
                event_type: EVENT_CONTEXT_HANDOFF_UPDATED.to_string(),
                thread_id: plan.command.thread_id.clone(),
                run_id: Some(run_id.clone()),
                driver: provider.driver.clone(),
                instance_id: model.instance_id.clone(),
                payload: EventPayload::ContextHandoff(h.clone()),
                ..Default::default()
            })?;
        }

        let attempt_id = derive_run_attempt_id(&run_id, 1);
        let root_node_id = derive_root_node_id(&run_id);
        let provider_thread_id = provider_thread.id.clone();

        let mut run = Run {
            id: run_id.clone(),
            thread_id: plan.command.thread_id.clone(),
            ordinal,
            provider_instance_id: model.instance_id.clone(),
            model_selection: model.clone(),
            provider_thread_id: Some(provider_thread_id.clone()),
            user_message_id: command.message_id.clone(),
            root_node_id: Some(root_node_id.clone()),
            active_attempt_id: Some(attempt_id.clone()),
            status: status.clone(),
            queue_position: None,
            queue_held: false,
            context_handoff_id: handoff.as_ref().map(|h| h.id.clone()),
            checkpoint_id: None,
            requested_at: plan.now,
            started_at: None,
            completed_at: None,
            restart_continuation_of_run_id: None,
        };

        if queued {
            let mut position = 1;
            for candidate in &plan.projection.runs {
                if candidate.status != RUN_QUEUED {
                    continue;
                }
                if candidate.queue_held {
                    queue_held = true;
                }
                let candidate_position = candidate.queue_position.unwrap_or(candidate.ordinal);
                if candidate_position >= position {
                    position = candidate_position + 1;
                }
            }
            run.queue_position = Some(position);
            run.queue_held = queue_held;
        }

        plan.emit_and_apply(EventSpec {
            event_type: EVENT_RUN_CREATED.to_string(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run_id.clone()),
            node_id: Some(root_node_id.clone()),
            driver: provider.driver.clone(),
            instance_id: model.instance_id.clone(),
            payload: EventPayload::Run(run.clone()),
            ..Default::default()
        })?;

        let attempt = RunAttempt {
            id: attempt_id.clone(),
            run_id: run_id.clone(),
            attempt_ordinal: 1,
            root_node_id: root_node_id.clone(),
            provider_instance_id: model.instance_id.clone(),
            provider_thread_id: provider_thread.id.clone(),
            provider_turn_id: None,
            reason: "initial".to_string(),
            status: ATTEMPT_PENDING.to_string(),
            started_at: None,
            completed_at: None,
        };
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_RUN_ATTEMPT_CREATED.to_string(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run_id.clone()),
            node_id: Some(root_node_id.clone()),
            driver: provider.driver.clone(),
            instance_id: model.instance_id.clone(),
            payload: EventPayload::RunAttempt(attempt),
            ..Default::default()
        })?;

        let mut root = ExecutionNode {
            id: root_node_id.clone(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run_id.clone()),
            parent_node_id: None,
            root_node_id: root_node_id.clone(),
            kind: NODE_ROOT_TURN.to_string(),
            status: NODE_PENDING.to_string(),
            counts_for_run: true,
            provider_thread_id: Some(provider_thread_id.clone()),
            provider_turn_id: None,
            native_item_ref: None,
            runtime_request_id: None,
            checkpoint_scope_id: None,
            started_at: None,
            completed_at: None,
        };
        if status != RUN_PREPARING {
            if let Some(ref worktree_path) = plan.projection.thread.worktree_path {
                let scope = CheckpointScope {
                    id: derive_checkpoint_scope_id(&run.thread_id, "root"),
                    thread_id: run.thread_id.clone(),
                    run_id: Some(run_id.clone()),
                    node_id: root_node_id.clone(),
                    parent_scope_id: None,
                    provider_thread_id: Some(provider_thread_id.clone()),
                    kind: "root_run".to_string(),
                    ordinal_within_parent: 0,
                    advances_app_run_count: true,
                    cwd: worktree_path.clone(),
                    created_at: plan.now,
                };
                root.checkpoint_scope_id = Some(scope.id.clone());
                plan.emit_and_apply(EventSpec {
                    event_type: EVENT_CHECKPOINT_SCOPE_CREATED.to_string(),
                    thread_id: run.thread_id.clone(),
                    run_id: Some(run_id.clone()),
                    node_id: Some(root_node_id.clone()),
                    driver: provider.driver.clone(),
                    instance_id: model.instance_id.clone(),
                    payload: EventPayload::CheckpointScope(scope),
                    ..Default::default()
                })?;
            }
        }
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_NODE_UPDATED.to_string(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run_id.clone()),
            node_id: Some(root_node_id.clone()),
            driver: provider.driver.clone(),
            instance_id: model.instance_id.clone(),
            payload: EventPayload::ExecutionNode(root),
            ..Default::default()
        })?;

        let message = ConversationMessage {
            created_by: actor.clone(),
            creation_source: source.clone(),
            id: command.message_id.clone(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run_id.clone()),
            node_id: Some(root_node_id.clone()),
            role: MESSAGE_USER.to_string(),
            text: command.text.clone(),
            attachments: command.attachments.clone(),
            streaming: false,
            created_at: plan.now,
            updated_at: plan.now,
        };
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_MESSAGE_UPDATED.to_string(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run_id.clone()),
            node_id: Some(root_node_id.clone()),
            driver: provider.driver.clone(),
            instance_id: model.instance_id.clone(),
            payload: EventPayload::ConversationMessage(message.clone()),
            ..Default::default()
        })?;

        let (item_status, input_intent) = if queued {
            (TURN_ITEM_PENDING, "queued_turn")
        } else {
            (TURN_ITEM_COMPLETED, "turn_start")
        };
        let mut data = BTreeMap::new();
        data.insert("inputIntent".to_string(), json!(input_intent));
        data.insert("attachments".to_string(), json!(message.attachments));

        let mut item = TurnItem {
            created_by: actor,
            creation_source: source,
            id: derive_user_turn_item_id(&message.id),
            thread_id: run.thread_id.clone(),
            run_id: Some(run_id.clone()),
            node_id: Some(root_node_id.clone()),
            provider_thread_id: Some(provider_thread_id),
            provider_turn_id: None,
            native_item_ref: None,
            parent_item_id: None,
            ordinal: next_turn_item_ordinal(&plan.projection),
            status: item_status.to_string(),
            title: String::new(),
            started_at: Some(plan.now),
            completed_at: if item_status == TURN_ITEM_COMPLETED {
                Some(plan.now)
            } else {
                None
            },
            updated_at: plan.now,
            item_type: "user_message".to_string(),
            message_id: message.id,
            text: message.text,
            request_id: String::new(),
            data,
        };
        if item_status == TURN_ITEM_COMPLETED {
            item.completed_at = Some(plan.now);
        }
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_TURN_ITEM_UPDATED.to_string(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run_id),
            node_id: Some(root_node_id),
            driver: provider.driver,
            instance_id: model.instance_id,
            payload: EventPayload::TurnItem(item),
            ..Default::default()
        })?;

        if status == RUN_STARTING {
            return self.append_turn_start_effect(plan, &run);
        }
        Ok(())
    }

    pub(crate) fn plan_provider_handoff(
        &self,
        plan: &mut CommandPlan<'_>,
        from: &ProviderThread,
        to: &ProviderThread,
        run_id: &str,
    ) -> Result<Option<ContextHandoff>, Error> {
        let Some(latest) = latest_run(&plan.projection.runs) else {
            return Ok(None);
        };
        let mut from_ordinal = 1;
        let mut strategy = "full_thread_summary";
        if let Some(last_seen) = to.last_run_ordinal {
            if last_seen < latest.ordinal {
                from_ordinal = last_seen + 1;
                strategy = "delta_since_target_last_seen";
            }
        }
        let transfer_id = self.gen_random_id(
            "context-transfer",
            &[
                "type",
                "provider_handoff",
                "source-thread",
                &plan.command.thread_id,
                "target-thread",
                &plan.command.thread_id,
            ],
        )?;
        let now = plan.now;
        let handoff_id = self.gen_random_id(
            "context-handoff",
            &[
                "thread",
                &plan.command.thread_id,
                "from-provider-instance",
                &from.provider_instance_id,
                "to-provider-instance",
                &to.provider_instance_id,
            ],
        )?;
        let handoff = ContextHandoff {
            id: handoff_id.clone(),
            transfer_id: Some(transfer_id.clone()),
            thread_id: plan.command.thread_id.clone(),
            target_run_id: run_id.to_string(),
            from_provider_thread_ids: vec![from.id.clone()],
            to_provider_thread_id: to.id.clone(),
            covered_run_ordinals: CoveredRunOrdinals {
                from: from_ordinal,
                to: latest.ordinal,
            },
            strategy: strategy.to_string(),
            status: "ready".to_string(),
            summary_message_id: None,
            summary_text: handoff_summary(&plan.projection, from_ordinal, latest.ordinal),
            created_by_provider_instance_id: None,
            created_at: now,
            updated_at: now,
        };
        let resolution_strategy = match strategy {
            "delta_since_target_last_seen" => "delta_context",
            _ => "portable_context",
        };
        let transfer = ContextTransfer {
            id: transfer_id,
            transfer_type: "provider_handoff".to_string(),
            source_thread_id: plan.command.thread_id.clone(),
            target_thread_id: plan.command.thread_id.clone(),
            source_point: ContextSourcePoint {
                thread_id: plan.command.thread_id.clone(),
                run_id: Some(latest.id.clone()),
                ..Default::default()
            },
            base_point: None,
            source_provider_instance_id: Some(from.provider_instance_id.clone()),
            target_provider_instance_id: Some(to.provider_instance_id.clone()),
            target_run_id: Some(run_id.to_string()),
            status: "consumed".to_string(),
            resolution: Some(ContextTransferResolution {
                strategy: resolution_strategy.to_string(),
                context_handoff_id: Some(handoff.id.clone()),
                provider_thread_ref: None,
            }),
            error: None,
            created_by: ACTOR_SYSTEM.to_string(),
            created_at: now,
            updated_at: now,
            consumed_at: Some(now),
        };
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_CONTEXT_TRANSFER_CREATED.to_string(),
            thread_id: plan.command.thread_id.clone(),
            run_id: Some(run_id.to_string()),
            instance_id: to.provider_instance_id.clone(),
            payload: EventPayload::ContextTransfer(transfer),
            ..Default::default()
        })?;
        Ok(Some(handoff))
    }

    pub(crate) fn resolve_pending_fork(
        &self,
        plan: &mut CommandPlan<'_>,
        provider: &ProviderInfo,
        provider_thread: &mut ProviderThread,
        run_id: &str,
        handoff: &mut Option<ContextHandoff>,
    ) -> Result<(), Error> {
        let mut pending = plan
            .projection
            .context_transfers
            .iter()
            .find(|t| t.transfer_type == "fork" && t.status == "pending")
            .cloned();
        let Some(ref mut pending_transfer) = pending else {
            return Ok(());
        };
        let source_snapshot = self
            .store
            .snapshot(&pending_transfer.source_thread_id)
            .map_err(|err| Error::with_detail(err.kind, format!("load fork source: {err}")))?;
        let mut source_run = Run::default();
        if let Some(ref src_run_id) = pending_transfer.source_point.run_id {
            source_run = source_snapshot
                .projection
                .run(src_run_id)
                .ok_or_else(|| {
                    Error::with_detail(
                        ErrorKind::StableSourceNotFound,
                        "fork source run disappeared",
                    )
                })?;
        }
        let source_provider = source_run
            .provider_thread_id
            .as_ref()
            .and_then(|ptid| source_snapshot.projection.provider_thread(ptid))
            .unwrap_or_default();

        let native = pending_transfer.source_provider_instance_id.as_deref()
            == Some(provider_thread.provider_instance_id.as_str())
            && source_provider
                .native_thread_ref
                .as_ref()
                .map(|r| r.strength == NATIVE_REF_STRONG)
                .unwrap_or(false)
            && provider.capabilities.threads.can_fork_thread;

        if native {
            let mut fork_source = ProviderThreadForkSource {
                provider_thread_id: source_provider.id.clone(),
                provider_turn_id: None,
                checkpoint_id: None,
            };
            if let Some(ref active_attempt_id) = source_run.active_attempt_id {
                if let Some(turn) = latest_provider_turn_for_attempt(
                    &source_snapshot.projection.provider_turns,
                    active_attempt_id,
                ) {
                    fork_source.provider_turn_id = Some(turn.id);
                }
            }
            provider_thread.forked_from = Some(fork_source);
            pending_transfer.resolution = Some(ContextTransferResolution {
                strategy: "native_fork".to_string(),
                context_handoff_id: None,
                provider_thread_ref: source_provider.native_thread_ref.clone(),
            });
        } else {
            let handoff_id = self.gen_random_id(
                "context-handoff",
                &[
                    "thread",
                    &plan.command.thread_id,
                    "from-provider-instance",
                    &provider_thread.provider_instance_id,
                    "to-provider-instance",
                    &provider_thread.provider_instance_id,
                ],
            )?;
            let to_ordinal = if !source_run.id.is_empty() {
                source_run.ordinal
            } else {
                0
            };
            let mut created = ContextHandoff {
                id: handoff_id,
                transfer_id: Some(pending_transfer.id.clone()),
                thread_id: plan.command.thread_id.clone(),
                target_run_id: run_id.to_string(),
                from_provider_thread_ids: if !source_provider.id.is_empty() {
                    vec![source_provider.id.clone()]
                } else {
                    Vec::new()
                },
                to_provider_thread_id: provider_thread.id.clone(),
                covered_run_ordinals: CoveredRunOrdinals {
                    from: 1,
                    to: to_ordinal.max(1),
                },
                strategy: "fork_delta_summary".to_string(),
                status: "ready".to_string(),
                summary_message_id: None,
                summary_text: handoff_summary(&source_snapshot.projection, 1, to_ordinal),
                created_by_provider_instance_id: if !source_run.provider_instance_id.is_empty() {
                    Some(source_run.provider_instance_id.clone())
                } else {
                    None
                },
                created_at: plan.now,
                updated_at: plan.now,
            };
            pending_transfer.resolution = Some(ContextTransferResolution {
                strategy: "portable_context".to_string(),
                context_handoff_id: Some(created.id.clone()),
                provider_thread_ref: None,
            });
            *handoff = Some(std::mem::take(&mut created));
        }

        pending_transfer.target_provider_instance_id =
            Some(provider_thread.provider_instance_id.clone());
        pending_transfer.target_run_id = Some(run_id.to_string());
        pending_transfer.status = "consumed".to_string();
        pending_transfer.updated_at = plan.now;
        pending_transfer.consumed_at = Some(plan.now);

        plan.emit_and_apply(EventSpec {
            event_type: EVENT_CONTEXT_TRANSFER_UPDATED.to_string(),
            thread_id: plan.command.thread_id.clone(),
            run_id: Some(run_id.to_string()),
            instance_id: provider_thread.provider_instance_id.clone(),
            payload: EventPayload::ContextTransfer(pending_transfer.clone()),
            ..Default::default()
        })?;
        Ok(())
    }
}

pub(crate) fn select_provider_thread(
    projection: &Projection,
    driver: &str,
    instance_id: &str,
    run_id: &str,
    now_time: DateTime<Utc>,
) -> (ProviderThread, bool) {
    if let Some(ref active_id) = projection.thread.active_provider_thread_id {
        if let Some(active) = projection.provider_thread(active_id) {
            if active.provider_instance_id == instance_id {
                return (active, false);
            }
        }
    }
    let mut candidates: Vec<ProviderThread> = projection
        .provider_threads
        .iter()
        .filter(|candidate| {
            candidate.app_thread_id.as_deref() == Some(projection.thread.id.as_str())
                && candidate.owner_node_id.is_none()
                && candidate.provider_instance_id == instance_id
                && candidate.status != PROVIDER_THREAD_CLOSED
                && candidate.status != PROVIDER_THREAD_ERROR
        })
        .cloned()
        .collect();
    if !candidates.is_empty() {
        candidates.sort_by(|a, b| {
            b.last_run_ordinal
                .unwrap_or(0)
                .cmp(&a.last_run_ordinal.unwrap_or(0))
        });
        return (candidates.remove(0), false);
    }
    let thread_id = derive_provider_thread_id(driver, instance_id, &format!("pending:{run_id}"));
    (
        ProviderThread {
            id: thread_id,
            driver: driver.to_string(),
            provider_instance_id: instance_id.to_string(),
            app_thread_id: Some(projection.thread.id.clone()),
            owner_node_id: None,
            provider_session_id: None,
            native_thread_ref: None,
            native_conversation_head_ref: None,
            forked_from: None,
            handoff_ids: Vec::new(),
            first_run_ordinal: None,
            last_run_ordinal: None,
            status: PROVIDER_THREAD_NOT_LOADED.to_string(),
            created_at: now_time,
            updated_at: now_time,
        },
        true,
    )
}

fn append_unique_handoff(ids: &[ContextHandoffID], id: &str) -> Vec<ContextHandoffID> {
    if ids.iter().any(|existing| existing == id) {
        return ids.to_vec();
    }
    let mut next = ids.to_vec();
    next.push(id.to_string());
    next
}

pub(crate) fn handoff_summary(projection: &Projection, from_ordinal: i64, to_ordinal: i64) -> String {
    let mut run_ordinals: HashMap<RunID, i64> = HashMap::new();
    for run in &projection.runs {
        run_ordinals.insert(run.id.clone(), run.ordinal);
    }
    let mut text = format!("Portable orchestration context for runs {from_ordinal}-{to_ordinal}.\n");
    for message in &projection.messages {
        let Some(ref run_id) = message.run_id else {
            continue;
        };
        let ordinal = run_ordinals.get(run_id).copied().unwrap_or(0);
        if ordinal < from_ordinal || ordinal > to_ordinal || message.streaming {
            continue;
        }
        text.push_str(&format!("\n[{}, run {}]\n{}\n", message.role, ordinal, message.text));
    }
    text
}
