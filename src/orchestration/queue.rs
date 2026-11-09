use serde_json::json;

use crate::orchestration::command::*;
use crate::orchestration::errors::*;
use crate::orchestration::event::*;
use crate::orchestration::orchestrator::*;
use crate::orchestration::types::*;

pub(crate) fn queued_runs_in_delivery_order(projection: &Projection) -> Vec<Run> {
    let mut runs: Vec<Run> = projection
        .runs
        .iter()
        .filter(|r| r.status == RUN_QUEUED)
        .cloned()
        .collect();
    runs.sort_by(|left, right| {
        let left_pos = left.queue_position.unwrap_or(left.ordinal);
        let right_pos = right.queue_position.unwrap_or(right.ordinal);
        if left_pos != right_pos {
            return left_pos.cmp(&right_pos);
        }
        left.ordinal.cmp(&right.ordinal)
    });
    runs
}

pub(crate) fn latest_queue_anchor(projection: &Projection) -> Option<Run> {
    let mut latest: Option<Run> = None;
    for run in &projection.runs {
        if run.status != RUN_QUEUED {
            if latest.as_ref().map(|l| run.ordinal > l.ordinal).unwrap_or(true) {
                latest = Some(run.clone());
            }
        }
    }
    latest
}

pub(crate) fn queue_is_held(projection: &Projection) -> bool {
    for run in &projection.runs {
        if run.status == RUN_QUEUED && run.queue_held {
            return true;
        }
    }
    let Some(latest) = latest_queue_anchor(projection) else {
        return false;
    };
    for item in &projection.turn_items {
        if item.item_type == "run_interrupt_request"
            && item.run_id.as_deref() == Some(latest.id.as_str())
        {
            if item
                .data
                .get("holdQueue")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                return true;
            }
        }
    }
    false
}

pub(crate) fn release_queue_holds(plan: &mut CommandPlan<'_>) -> Result<bool, Error> {
    let mut changed = false;
    let runs = plan.projection.runs.clone();
    for mut run in runs {
        if run.status == RUN_QUEUED && run.queue_held {
            run.queue_held = false;
            plan.emit_and_apply(EventSpec {
                event_type: EVENT_RUN_UPDATED.to_string(),
                thread_id: run.thread_id.clone(),
                run_id: Some(run.id.clone()),
                node_id: run.root_node_id.clone(),
                instance_id: run.provider_instance_id.clone(),
                payload: EventPayload::Run(run),
                ..Default::default()
            })?;
            changed = true;
        }
    }
    let Some(latest) = latest_queue_anchor(&plan.projection) else {
        return Ok(changed);
    };
    let items = plan.projection.turn_items.clone();
    for mut item in items {
        if item.item_type != "run_interrupt_request"
            || item.run_id.as_deref() != Some(latest.id.as_str())
        {
            continue;
        }
        let held = item
            .data
            .get("holdQueue")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !held {
            continue;
        }
        item.data.clear();
        item.data.insert("holdQueue".to_string(), json!(false));
        item.updated_at = plan.now;
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_TURN_ITEM_UPDATED.to_string(),
            thread_id: item.thread_id.clone(),
            run_id: item.run_id.clone(),
            node_id: item.node_id.clone(),
            instance_id: latest.provider_instance_id.clone(),
            payload: EventPayload::TurnItem(item),
            ..Default::default()
        })?;
        changed = true;
    }
    Ok(changed)
}

pub(crate) fn turn_start_effect_id(command_id: &str, run_id: &str) -> String {
    if command_id.is_empty() {
        format!("effect:provider-turn.start:{run_id}")
    } else {
        format!("effect:{command_id}:provider-turn.start:{run_id}")
    }
}

impl Orchestrator {
    pub(crate) fn plan_queue_resume(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        let changed = release_queue_holds(plan)?;
        if !changed {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                "queue is not held",
            ));
        }
        self.start_next_queued_run(plan)
    }

    pub(crate) fn plan_queued_run_cancel(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        let command = plan
            .command
            .payload
            .as_cancel_queued_run()
            .cloned()
            .unwrap_or_default();
        let Some(mut run) = plan.projection.run(&command.run_id) else {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                format!("run {} is not queued", command.run_id),
            ));
        };
        if run.status != RUN_QUEUED {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                format!("run {} is not queued", command.run_id),
            ));
        }
        run.status = RUN_CANCELLED.to_string();
        run.completed_at = Some(plan.now);
        run.queue_held = false;
        plan.emit_and_apply(EventSpec {
            event_type: EVENT_RUN_UPDATED.to_string(),
            thread_id: run.thread_id.clone(),
            run_id: Some(run.id.clone()),
            node_id: run.root_node_id.clone(),
            instance_id: run.provider_instance_id.clone(),
            payload: EventPayload::Run(run.clone()),
            ..Default::default()
        })?;

        let items = plan.projection.turn_items.clone();
        for mut item in items {
            if item.run_id.as_deref() == Some(run.id.as_str()) && item.item_type == "user_message" {
                item.status = TURN_ITEM_CANCELLED.to_string();
                item.completed_at = Some(plan.now);
                item.updated_at = plan.now;
                plan.emit_and_apply(EventSpec {
                    event_type: EVENT_TURN_ITEM_UPDATED.to_string(),
                    thread_id: run.thread_id.clone(),
                    run_id: Some(run.id.clone()),
                    node_id: item.node_id.clone(),
                    instance_id: run.provider_instance_id.clone(),
                    payload: EventPayload::TurnItem(item),
                    ..Default::default()
                })?;
            }
        }
        self.start_next_queued_run(plan)
    }

    pub(crate) fn start_next_queued_run(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        if plan.projection.active_run().is_some() {
            return Ok(());
        }
        for mut queued in queued_runs_in_delivery_order(&plan.projection) {
            if queued.queue_held {
                continue;
            }
            if let Some(ref ptid) = queued.provider_thread_id {
                let mut provider_thread =
                    plan.projection.provider_thread(ptid).ok_or_else(|| {
                        Error::with_detail(
                            ErrorKind::CommandRejected,
                            format!("queued run {} provider thread is missing", queued.id),
                        )
                    })?;
                if provider_thread.first_run_ordinal.is_none() {
                    provider_thread.first_run_ordinal = Some(queued.ordinal);
                }
                provider_thread.last_run_ordinal = Some(queued.ordinal);
                provider_thread.updated_at = plan.now;
                plan.emit_and_apply(EventSpec {
                    event_type: EVENT_PROVIDER_THREAD_UPDATED.to_string(),
                    thread_id: queued.thread_id.clone(),
                    run_id: Some(queued.id.clone()),
                    driver: provider_thread.driver.clone(),
                    instance_id: provider_thread.provider_instance_id.clone(),
                    payload: EventPayload::ProviderThread(provider_thread),
                    ..Default::default()
                })?;
            }
            queued.status = RUN_STARTING.to_string();
            queued.queue_position = None;
            plan.emit_and_apply(EventSpec {
                event_type: EVENT_RUN_UPDATED.to_string(),
                thread_id: queued.thread_id.clone(),
                run_id: Some(queued.id.clone()),
                node_id: queued.root_node_id.clone(),
                instance_id: queued.provider_instance_id.clone(),
                payload: EventPayload::Run(queued.clone()),
                ..Default::default()
            })?;

            let items = plan.projection.turn_items.clone();
            for mut item in items {
                if item.item_type != "user_message"
                    || item.run_id.as_deref() != Some(queued.id.as_str())
                {
                    continue;
                }
                item.status = TURN_ITEM_COMPLETED.to_string();
                item.started_at = Some(plan.now);
                item.completed_at = Some(plan.now);
                item.updated_at = plan.now;
                plan.emit_and_apply(EventSpec {
                    event_type: EVENT_TURN_ITEM_UPDATED.to_string(),
                    thread_id: item.thread_id.clone(),
                    run_id: item.run_id.clone(),
                    node_id: item.node_id.clone(),
                    instance_id: queued.provider_instance_id.clone(),
                    payload: EventPayload::TurnItem(item),
                    ..Default::default()
                })?;
            }
            return self.append_turn_start_effect(plan, &queued);
        }
        Ok(())
    }

    pub(crate) fn append_turn_start_effect(
        &self,
        plan: &mut CommandPlan<'_>,
        run: &Run,
    ) -> Result<(), Error> {
        let (Some(active_attempt_id), Some(provider_thread_id)) =
            (&run.active_attempt_id, &run.provider_thread_id)
        else {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                format!("run {} lacks attempt or provider thread", run.id),
            ));
        };
        let attempt = plan
            .projection
            .attempts
            .iter()
            .find(|c| &c.id == active_attempt_id)
            .cloned();
        let provider_thread = plan.projection.provider_thread(provider_thread_id);
        let message = plan
            .projection
            .messages
            .iter()
            .find(|c| c.id == run.user_message_id)
            .cloned();
        let (Some(attempt), Some(provider_thread), Some(message)) =
            (attempt, provider_thread, message)
        else {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                format!("run {} has incomplete start state", run.id),
            ));
        };
        let handoff = run.context_handoff_id.as_ref().and_then(|hid| {
            plan.projection
                .context_handoffs
                .iter()
                .find(|c| &c.id == hid)
                .cloned()
        });
        plan.effect(
            EFFECT_PROVIDER_TURN_START,
            turn_start_effect_id(&plan.command.command_id, &run.id),
            EffectPayload::TurnStart(Box::new(TurnStartEffect {
                thread: plan.projection.thread.clone(),
                run: run.clone(),
                attempt,
                provider_thread,
                message,
                context_handoff: handoff,
            })),
        );
        Ok(())
    }
}
