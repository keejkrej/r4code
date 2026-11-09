use std::collections::BTreeMap;

use serde_json::json;

use crate::orchestration::command::*;
use crate::orchestration::errors::*;
use crate::orchestration::event::*;
use crate::orchestration::ids::*;
use crate::orchestration::orchestrator::*;
use crate::orchestration::types::*;

impl Orchestrator {
    pub(crate) fn plan_run_interrupt(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        let command = plan
            .command
            .payload
            .as_interrupt_run()
            .cloned()
            .unwrap_or_default();
        let Some(run) = plan.projection.run(&command.run_id) else {
            return Err(Error::with_detail(
                ErrorKind::RunNotInterruptible,
                format!("run {}", command.run_id),
            ));
        };
        if !run_status_is_active(&run.status)
            || run.root_node_id.is_none()
            || run.provider_thread_id.is_none()
        {
            return Err(Error::with_detail(
                ErrorKind::RunNotInterruptible,
                format!("run {}", command.run_id),
            ));
        }
        let ptid = run.provider_thread_id.as_ref().unwrap();
        let provider_thread = plan.projection.provider_thread(ptid).ok_or_else(|| {
            Error::with_detail(
                ErrorKind::RunNotInterruptible,
                format!("run {} provider thread is missing", command.run_id),
            )
        })?;
        let provider_turn = run
            .active_attempt_id
            .as_ref()
            .and_then(|aid| {
                latest_provider_turn_for_attempt(&plan.projection.provider_turns, aid)
            })
            .unwrap_or_default();

        let mut data = BTreeMap::new();
        data.insert("holdQueue".to_string(), json!(command.hold_queue));
        let mut item = TurnItem {
            created_by: ACTOR_SYSTEM.to_string(),
            creation_source: CREATION_SERVER.to_string(),
            id: derive_run_signal_turn_item_id(&run.id, "interrupt-request"),
            thread_id: run.thread_id.clone(),
            run_id: Some(run.id.clone()),
            node_id: run.root_node_id.clone(),
            provider_thread_id: Some(provider_thread.id.clone()),
            provider_turn_id: if !provider_turn.id.is_empty() {
                Some(provider_turn.id.clone())
            } else {
                None
            },
            native_item_ref: None,
            parent_item_id: None,
            ordinal: next_turn_item_ordinal(&plan.projection),
            status: TURN_ITEM_COMPLETED.to_string(),
            title: "Interrupt requested".to_string(),
            started_at: Some(plan.now),
            completed_at: Some(plan.now),
            updated_at: plan.now,
            item_type: "run_interrupt_request".to_string(),
            message_id: String::new(),
            text: command.reason.clone(),
            request_id: String::new(),
            data,
        };
        if !provider_turn.id.is_empty() {
            item.provider_turn_id = Some(provider_turn.id.clone());
        }
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_TURN_ITEM_UPDATED.to_string(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run.id.clone()),
            node_id: run.root_node_id.clone(),
            instance_id: run.provider_instance_id.clone(),
            payload: EventPayload::TurnItem(item),
            ..Default::default()
        })?;

        // Acknowledgement is not termination. The run changes only when the root
        // provider turn reports an interrupted terminal state.
        if !provider_turn.id.is_empty() {
            if let Some(ref session_id) = provider_thread.provider_session_id {
                plan.effect(
                    EFFECT_PROVIDER_TURN_INTERRUPT,
                    format!(
                        "effect:{}:provider-turn.interrupt:{}",
                        plan.command.command_id, provider_turn.id
                    ),
                    EffectPayload::TurnInterrupt(TurnInterruptEffect {
                        provider_session_id: session_id.clone(),
                        provider_thread_id: provider_thread.id,
                        provider_turn_id: provider_turn.id,
                        reason: command.reason,
                    }),
                );
            }
        }

        if command.hold_queue {
            let runs = plan.projection.runs.clone();
            for mut queued in runs {
                if queued.status != RUN_QUEUED {
                    continue;
                }
                queued.queue_held = true;
                plan.emit_and_apply(EventSpec {
                    event_type: EVENT_RUN_UPDATED.to_string(),
                    thread_id: queued.thread_id.clone(),
                    run_id: Some(queued.id.clone()),
                    node_id: queued.root_node_id.clone(),
                    instance_id: queued.provider_instance_id.clone(),
                    payload: EventPayload::Run(queued),
                    ..Default::default()
                })?;
            }
        }
        Ok(())
    }

    pub(crate) fn plan_runtime_request_respond(
        &self,
        plan: &mut CommandPlan<'_>,
    ) -> Result<(), Error> {
        let command = plan
            .command
            .payload
            .as_runtime_request_response()
            .cloned()
            .unwrap_or_default();
        let Some(mut request) = plan.projection.runtime_request(&command.request_id) else {
            return Err(Error::with_detail(
                ErrorKind::RequestNotRespondable,
                format!("request {}", command.request_id),
            ));
        };
        if request.status != REQUEST_PENDING
            || request.response_capability.capability_type != "live"
            || request.response_capability.provider_session_id.is_none()
        {
            return Err(Error::with_detail(
                ErrorKind::RequestNotRespondable,
                format!("request {}", command.request_id),
            ));
        }
        request.status = REQUEST_RESOLVED.to_string();
        request.resolved_at = Some(plan.now);
        request.decision = command.decision.clone();
        request.feedback = command.feedback.clone();
        request.answers = command.answers.clone();

        plan.emit_and_apply(EventSpec {
            event_type: EVENT_RUNTIME_REQUEST_UPDATED.to_string(),
            thread_id: plan.command.thread_id.clone(),
            node_id: Some(request.node_id.clone()),
            payload: EventPayload::RuntimeRequest(request.clone()),
            ..Default::default()
        })?;

        if let Some(mut node) = plan.projection.node(&request.node_id) {
            node.status = NODE_COMPLETED.to_string();
            node.completed_at = Some(plan.now);
            plan.emit_and_apply(EventSpec {
                event_type: EVENT_NODE_UPDATED.to_string(),
                thread_id: node.thread_id.clone(),
                run_id: node.run_id.clone(),
                node_id: Some(node.id.clone()),
                payload: EventPayload::ExecutionNode(node),
                ..Default::default()
            })?;
        }

        let items = plan.projection.turn_items.clone();
        for mut item in items {
            if item.request_id != request.id {
                continue;
            }
            item.status = TURN_ITEM_COMPLETED.to_string();
            if let Some(ref decision) = command.decision {
                if decision == "decline" || decision == "cancel" {
                    item.status = TURN_ITEM_DECLINED.to_string();
                }
            }
            item.completed_at = Some(plan.now);
            item.updated_at = plan.now;
            plan.emit_and_apply(EventSpec {
                event_type: EVENT_TURN_ITEM_UPDATED.to_string(),
                thread_id: item.thread_id.clone(),
                run_id: item.run_id.clone(),
                node_id: item.node_id.clone(),
                payload: EventPayload::TurnItem(item),
                ..Default::default()
            })?;
        }

        plan.effect(
            EFFECT_PROVIDER_RUNTIME_REQUEST_RESPOND,
            format!(
                "effect:{}:provider-runtime-request.respond:{}",
                plan.command.command_id, request.id
            ),
            EffectPayload::RuntimeRequestRespond(RuntimeRequestRespondEffect {
                request,
                response: command,
            }),
        );
        Ok(())
    }
}
