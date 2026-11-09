use std::collections::{BTreeMap, HashMap};

use serde_json::json;

use crate::orchestration::command::*;
use crate::orchestration::errors::*;
use crate::orchestration::event::*;
use crate::orchestration::ids::*;
use crate::orchestration::orchestrator::*;
use crate::orchestration::projection::*;
use crate::orchestration::types::*;

impl Orchestrator {
    pub(crate) fn plan_delegated_task(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        let command = plan
            .command
            .payload
            .as_delegate_task()
            .cloned()
            .unwrap_or_default();
        let Some(parent_run) = plan.projection.run(&command.parent_run_id) else {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                format!("parent run {} is not active", command.parent_run_id),
            ));
        };
        if !run_status_is_active(&parent_run.status) {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                format!("parent run {} is not active", command.parent_run_id),
            ));
        }
        let Some(parent_node) = plan.projection.node(&command.parent_node_id) else {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                format!(
                    "parent node {} is not part of active run {}",
                    command.parent_node_id, parent_run.id
                ),
            ));
        };
        if parent_node.run_id.as_deref() != Some(parent_run.id.as_str())
            || parent_run.root_node_id.is_none()
        {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                format!(
                    "parent node {} is not part of active run {}",
                    command.parent_node_id, parent_run.id
                ),
            ));
        }

        let provider = self.resolve_provider(&command.model_selection.instance_id)?;
        let (actor, source) = creation_defaults(&command.created_by, &command.creation_source);
        let now = plan.now;
        let task_node_id = derive_delegated_task_node_id(&plan.command.command_id);
        let child_thread_id = derive_delegated_task_thread_id(&plan.command.command_id);
        let child_message_id = derive_delegated_task_message_id(&plan.command.command_id);
        let task_turn_item_id = derive_delegated_task_turn_item_id(&plan.command.command_id);

        let mut title = command.title.clone();
        if title.is_empty() {
            title = format!("Task: {}", command.task.trim());
            if title.len() > 80 {
                let mut end = 80;
                while !title.is_char_boundary(end) {
                    end -= 1;
                }
                title.truncate(end);
            }
        }

        let mut child_thread = Thread {
            created_by: actor.clone(),
            creation_source: source.clone(),
            id: child_thread_id.clone(),
            project_id: plan.projection.thread.project_id.clone(),
            title: title.clone(),
            provider_instance_id: command.model_selection.instance_id.clone(),
            model_selection: command.model_selection.clone(),
            runtime_mode: command.runtime_mode.clone(),
            interaction_mode: command.interaction_mode.clone(),
            branch: plan.projection.thread.branch.clone(),
            worktree_path: plan.projection.thread.worktree_path.clone(),
            active_provider_thread_id: None,
            lineage: ThreadLineage {
                parent_thread_id: Some(command.parent_thread_id.clone()),
                relationship_to_parent: Some("subagent".to_string()),
                root_thread_id: plan.projection.thread.lineage.root_thread_id.clone(),
                side_chat: false,
            },
            forked_from: Some(ForkedFrom {
                fork_type: "node".to_string(),
                thread_id: String::new(),
                run_id: String::new(),
                node_id: task_node_id.clone(),
                provider_thread_id: String::new(),
                provider_turn_id: None,
            }),
            created_at: now,
            updated_at: now,
            archived_at: None,
            settled_override: None,
            settled_at: None,
            last_visited_at: None,
            deleted_at: None,
        };
        thread_defaults(&mut child_thread);

        let child_created = plan.emit(EventSpec {
            event_type: EVENT_THREAD_CREATED.to_string(),
            thread_id: child_thread_id.clone(),
            driver: provider.driver.clone(),
            instance_id: command.model_selection.instance_id.clone(),
            payload: EventPayload::Thread(child_thread.clone()),
            ..Default::default()
        })?;

        let task_node = ExecutionNode {
            id: task_node_id.clone(),
            thread_id: command.parent_thread_id.clone(),
            run_id: Some(parent_run.id.clone()),
            parent_node_id: Some(parent_node.id.clone()),
            root_node_id: parent_run.root_node_id.clone().unwrap(),
            kind: NODE_SUBAGENT.to_string(),
            status: NODE_RUNNING.to_string(),
            counts_for_run: false,
            provider_thread_id: None,
            provider_turn_id: None,
            native_item_ref: None,
            runtime_request_id: None,
            checkpoint_scope_id: None,
            started_at: Some(now),
            completed_at: None,
        };
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_NODE_UPDATED.to_string(),
            thread_id: command.parent_thread_id.clone(),
            run_id: Some(parent_run.id.clone()),
            node_id: Some(task_node_id.clone()),
            driver: provider.driver.clone(),
            instance_id: command.model_selection.instance_id.clone(),
            payload: EventPayload::ExecutionNode(task_node),
            ..Default::default()
        })?;

        let task = Subagent {
            id: task_node_id.clone(),
            thread_id: command.parent_thread_id.clone(),
            run_id: Some(parent_run.id.clone()),
            parent_node_id: parent_node.id.clone(),
            origin: "app_owned".to_string(),
            created_by: actor.clone(),
            driver: provider.driver.clone(),
            provider_instance_id: command.model_selection.instance_id.clone(),
            provider_thread_id: None,
            child_thread_id: Some(child_thread_id.clone()),
            native_task_ref: None,
            title: if !command.title.is_empty() {
                Some(command.title.clone())
            } else {
                None
            },
            prompt: command.task.clone(),
            model: Some(command.model_selection.model.clone()),
            model_selection: None,
            completion_wake: String::new(),
            progress: String::new(),
            result: None,
            status: NODE_RUNNING.to_string(),
            started_at: Some(now),
            completed_at: None,
            updated_at: now,
        };
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_SUBAGENT_UPDATED.to_string(),
            thread_id: command.parent_thread_id.clone(),
            run_id: Some(parent_run.id.clone()),
            node_id: Some(task_node_id.clone()),
            driver: provider.driver.clone(),
            instance_id: command.model_selection.instance_id.clone(),
            payload: EventPayload::Subagent(task),
            ..Default::default()
        })?;

        let mut data = BTreeMap::new();
        data.insert("subagentId".to_string(), json!(task_node_id));
        data.insert("origin".to_string(), json!("app_owned"));
        data.insert("driver".to_string(), json!(provider.driver));
        data.insert(
            "providerInstanceId".to_string(),
            json!(command.model_selection.instance_id),
        );
        data.insert("childThreadId".to_string(), json!(child_thread_id));
        data.insert("prompt".to_string(), json!(command.task));

        let item = TurnItem {
            created_by: actor.clone(),
            creation_source: source.clone(),
            id: task_turn_item_id.clone(),
            thread_id: command.parent_thread_id.clone(),
            run_id: Some(parent_run.id.clone()),
            node_id: Some(task_node_id.clone()),
            provider_thread_id: parent_run.provider_thread_id.clone(),
            provider_turn_id: None,
            native_item_ref: None,
            parent_item_id: None,
            ordinal: next_turn_item_ordinal(&plan.projection),
            status: TURN_ITEM_RUNNING.to_string(),
            title,
            started_at: Some(now),
            completed_at: None,
            updated_at: now,
            item_type: "subagent".to_string(),
            message_id: String::new(),
            text: String::new(),
            request_id: String::new(),
            data,
        };
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_TURN_ITEM_UPDATED.to_string(),
            thread_id: command.parent_thread_id.clone(),
            run_id: Some(parent_run.id.clone()),
            node_id: Some(task_node_id),
            driver: provider.driver.clone(),
            instance_id: command.model_selection.instance_id.clone(),
            payload: EventPayload::TurnItem(item),
            ..Default::default()
        })?;

        let mut child_plan = CommandPlan {
            orchestrator: self,
            now,
            projection: empty_projection(&child_created, child_thread),
            events: Vec::new(),
            effects: Vec::new(),
            expected_sequences: HashMap::new(),
            command: Command {
                command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
                command_id: plan.command.command_id.clone(),
                thread_id: child_thread_id.clone(),
                payload: CommandPayload::DispatchMessage(DispatchMessage {
                    message_id: child_message_id,
                    text: command.task,
                    attachments: Vec::new(),
                    model_selection: Some(command.model_selection.clone()),
                    dispatch_mode: DispatchMode {
                        mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                        target_run_id: String::new(),
                    },
                    title_seed: String::new(),
                    created_by: actor.clone(),
                    creation_source: source,
                }),
            },
        };
        self.plan_message_dispatch(&mut child_plan)?;
        plan.events.extend(child_plan.events);
        plan.effects.extend(child_plan.effects);
        plan.expected_sequences.insert(child_thread_id.clone(), 0);
        if child_plan.projection.runs.len() != 1 {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                "delegated child did not create a run",
            ));
        }
        let child_run = &child_plan.projection.runs[0];
        let transfer_id = self.gen_random_id(
            "context-transfer",
            &[
                "type",
                "subagent_spawn",
                "source-thread",
                &command.parent_thread_id,
                "target-thread",
                &child_thread_id,
            ],
        )?;
        let transfer = ContextTransfer {
            id: transfer_id,
            transfer_type: "subagent_spawn".to_string(),
            source_thread_id: command.parent_thread_id.clone(),
            target_thread_id: child_thread_id.clone(),
            source_point: ContextSourcePoint {
                thread_id: command.parent_thread_id,
                run_id: Some(parent_run.id),
                turn_item_id: Some(task_turn_item_id),
                ..Default::default()
            },
            base_point: None,
            source_provider_instance_id: Some(parent_run.provider_instance_id),
            target_provider_instance_id: Some(command.model_selection.instance_id.clone()),
            target_run_id: Some(child_run.id.clone()),
            status: "consumed".to_string(),
            resolution: None,
            error: None,
            created_by: actor,
            created_at: now,
            updated_at: now,
            consumed_at: Some(now),
        };
        plan.emit(EventSpec {
            event_type: EVENT_CONTEXT_TRANSFER_CREATED.to_string(),
            thread_id: child_thread_id,
            run_id: Some(child_run.id.clone()),
            driver: provider.driver,
            instance_id: command.model_selection.instance_id,
            payload: EventPayload::ContextTransfer(transfer),
            ..Default::default()
        })?;
        Ok(())
    }

    pub(crate) fn finalize_delegated_task(
        &self,
        plan: &mut CommandPlan<'_>,
        child_run: &Run,
    ) -> Result<(), Error> {
        let thread = &plan.projection.thread;
        if thread.lineage.relationship_to_parent.as_deref() != Some("subagent")
            || thread.lineage.parent_thread_id.is_none()
            || thread
                .forked_from
                .as_ref()
                .map(|f| f.fork_type != "node")
                .unwrap_or(true)
        {
            return Ok(());
        }
        let parent_thread_id = thread.lineage.parent_thread_id.clone().unwrap();
        let parent_snapshot = self.store.snapshot(&parent_thread_id)?;
        let task_id = thread.forked_from.as_ref().unwrap().node_id.clone();
        let Some(mut task) = find_subagent(&parent_snapshot.projection.subagents, &task_id) else {
            return Ok(());
        };
        if task.completed_at.is_some() {
            return Ok(());
        }

        let mut parent_plan = CommandPlan {
            orchestrator: self,
            command: plan.command.clone(),
            now: plan.now,
            events: Vec::new(),
            effects: Vec::new(),
            projection: parent_snapshot.projection.clone(),
            expected_sequences: HashMap::new(),
        };
        let status = child_run.status.clone();
        let summary = latest_assistant_text(&plan.projection.messages, &child_run.id);
        task.status = status.clone();
        task.result = summary.clone();
        task.completed_at = Some(plan.now);
        task.updated_at = plan.now;
        task.provider_thread_id = child_run.provider_thread_id.clone();
        let raw_event_id = plan.events.last().and_then(|e| e.raw_event_id.clone());

        parent_plan.emit_and_apply(EventSpec {
            event_type: EVENT_SUBAGENT_UPDATED.to_string(),
            thread_id: parent_thread_id.clone(),
            run_id: task.run_id.clone(),
            node_id: Some(task.id.clone()),
            driver: task.driver.clone(),
            instance_id: task.provider_instance_id.clone(),
            raw_event_id: raw_event_id.clone(),
            payload: EventPayload::Subagent(task.clone()),
        })?;

        if let Some(mut node) = parent_plan.projection.node(&task_id) {
            node.status = status.clone();
            node.completed_at = Some(plan.now);
            parent_plan.emit_and_apply(EventSpec {
                event_type: EVENT_NODE_UPDATED.to_string(),
                thread_id: parent_thread_id.clone(),
                run_id: node.run_id.clone(),
                node_id: Some(node.id.clone()),
                driver: task.driver.clone(),
                instance_id: task.provider_instance_id.clone(),
                raw_event_id: raw_event_id.clone(),
                payload: EventPayload::ExecutionNode(node),
            })?;
        }

        let items = parent_plan.projection.turn_items.clone();
        for mut item in items {
            if item.item_type != "subagent" || item.node_id.as_deref() != Some(task_id.as_str()) {
                continue;
            }
            item.status = status.clone();
            item.completed_at = Some(plan.now);
            item.updated_at = plan.now;
            item.data.insert("result".to_string(), json!(summary));
            parent_plan.emit_and_apply(EventSpec {
                event_type: EVENT_TURN_ITEM_UPDATED.to_string(),
                thread_id: parent_thread_id.clone(),
                run_id: item.run_id.clone(),
                node_id: item.node_id.clone(),
                driver: task.driver.clone(),
                instance_id: task.provider_instance_id.clone(),
                raw_event_id: raw_event_id.clone(),
                payload: EventPayload::TurnItem(item),
            })?;
        }

        let transfer_id = self.gen_random_id(
            "context-transfer",
            &[
                "type",
                "subagent_result",
                "source-thread",
                &plan.projection.thread.id,
                "target-thread",
                &parent_thread_id,
            ],
        )?;
        let mut target_provider = parent_snapshot
            .projection
            .thread
            .provider_instance_id
            .clone();
        if let Some(ref task_run_id) = task.run_id {
            if let Some(parent_run) = parent_snapshot.projection.run(task_run_id) {
                target_provider = parent_run.provider_instance_id;
            }
        }
        let transfer = ContextTransfer {
            id: transfer_id,
            transfer_type: "subagent_result".to_string(),
            source_thread_id: plan.projection.thread.id.clone(),
            target_thread_id: parent_thread_id.clone(),
            source_point: ContextSourcePoint {
                thread_id: plan.projection.thread.id.clone(),
                run_id: Some(child_run.id.clone()),
                ..Default::default()
            },
            base_point: None,
            source_provider_instance_id: Some(child_run.provider_instance_id.clone()),
            target_provider_instance_id: Some(target_provider),
            target_run_id: task.run_id.clone(),
            status: "consumed".to_string(),
            resolution: None,
            error: None,
            created_by: ACTOR_SYSTEM.to_string(),
            created_at: plan.now,
            updated_at: plan.now,
            consumed_at: Some(plan.now),
        };
        parent_plan.emit_and_apply(EventSpec {
            event_type: EVENT_CONTEXT_TRANSFER_CREATED.to_string(),
            thread_id: parent_thread_id.clone(),
            run_id: task.run_id.clone(),
            node_id: Some(task.id),
            driver: task.driver,
            instance_id: task.provider_instance_id,
            raw_event_id,
            payload: EventPayload::ContextTransfer(transfer),
        })?;

        plan.events.extend(parent_plan.events);
        plan.expected_sequences
            .insert(parent_thread_id, parent_snapshot.snapshot_sequence);
        Ok(())
    }
}

fn find_subagent(tasks: &[Subagent], id: &str) -> Option<Subagent> {
    tasks.iter().find(|t| t.id == id).cloned()
}

fn latest_assistant_text(messages: &[ConversationMessage], run_id: &str) -> Option<String> {
    let mut result = None;
    for message in messages {
        if message.run_id.as_deref() == Some(run_id) && message.role == MESSAGE_ASSISTANT {
            result = Some(message.text.clone());
        }
    }
    result
}
