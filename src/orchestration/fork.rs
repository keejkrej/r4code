use crate::orchestration::command::*;
use crate::orchestration::errors::*;
use crate::orchestration::event::*;
use crate::orchestration::orchestrator::*;
use crate::orchestration::types::*;

pub(crate) fn forkable_status(status: &str) -> bool {
    matches!(
        status,
        RUN_COMPLETED | RUN_WAITING | RUN_FAILED | RUN_INTERRUPTED | RUN_CANCELLED
    )
}

pub(crate) fn resolve_fork_source(
    source: &Projection,
    point: &ForkSourcePoint,
) -> Result<(Run, ContextSourcePoint), Error> {
    let mut selected = Run::default();
    match point.point_type.as_str() {
        "latest_stable" => {
            for run in &source.runs {
                if forkable_status(&run.status) && run.ordinal > selected.ordinal {
                    selected = run.clone();
                }
            }
        }
        "run" => {
            selected = source.run(&point.run_id).ok_or_else(|| {
                Error::with_detail(
                    ErrorKind::StableSourceNotFound,
                    format!("run {}", point.run_id),
                )
            })?;
        }
        "checkpoint" => {
            let mut found = None;
            for checkpoint in &source.checkpoints {
                if checkpoint.id == point.checkpoint_id && checkpoint.status == "ready" {
                    if let Some(ref run_id) = checkpoint.run_id {
                        found = source.run(run_id);
                        break;
                    }
                }
            }
            selected = found.ok_or_else(|| {
                Error::with_detail(
                    ErrorKind::StableSourceNotFound,
                    format!("checkpoint {}", point.checkpoint_id),
                )
            })?;
        }
        _ => {}
    }
    if selected.id.is_empty() {
        return Err(Error::from(ErrorKind::StableSourceNotFound));
    }
    let mut source_point = ContextSourcePoint {
        thread_id: source.thread.id.clone(),
        run_id: Some(selected.id.clone()),
        turn_item_id: None,
        checkpoint_id: selected.checkpoint_id.clone(),
        provider_thread_ref: None,
        provider_turn_ref: None,
    };
    if !point.checkpoint_id.is_empty() {
        source_point.checkpoint_id = Some(point.checkpoint_id.clone());
    }
    Ok((selected, source_point))
}

impl Orchestrator {
    pub(crate) fn plan_thread_fork(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        let command = plan
            .command
            .payload
            .as_fork_thread()
            .cloned()
            .unwrap_or_default();
        if !plan.projection.thread.id.is_empty() {
            return Err(Error {
                op: "fork thread".to_string(),
                command_id: plan.command.command_id.clone(),
                thread_id: plan.command.thread_id.clone(),
                kind: ErrorKind::ThreadAlreadyExists,
                detail: String::new(),
                cause_kinds: vec![ErrorKind::ThreadAlreadyExists],
                turn_error: None,
            });
        }
        let source_snapshot = self
            .store
            .snapshot(&command.source_thread_id)
            .map_err(|err| {
                Error::with_detail(
                    err.kind,
                    format!("load fork source {}: {}", command.source_thread_id, err),
                )
            })?;
        let source = source_snapshot.projection;
        if source.thread.deleted_at.is_some() {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                "source thread is deleted",
            ));
        }
        let (source_run, mut source_point) = resolve_fork_source(&source, &command.source_point)?;
        if !source_run.id.is_empty() && !forkable_status(&source_run.status) {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                format!(
                    "fork source run {} is {}; active and rolled-back runs cannot be forked",
                    source_run.id, source_run.status
                ),
            ));
        }
        let (actor, creation_source) =
            creation_defaults(&command.created_by, &command.creation_source);
        let mut target = source.thread.clone();
        target.created_by = actor.clone();
        target.creation_source = creation_source;
        target.id = plan.command.thread_id.clone();
        target.title = if command.title.is_empty() {
            format!("{} fork", source.thread.title)
        } else {
            command.title
        };
        if command.side_chat {
            target.runtime_mode = RUNTIME_APPROVAL_REQUIRED.to_string();
        }
        target.active_provider_thread_id = None;
        target.lineage = ThreadLineage {
            parent_thread_id: Some(source.thread.id.clone()),
            relationship_to_parent: Some("fork".to_string()),
            root_thread_id: source.thread.lineage.root_thread_id.clone(),
            side_chat: command.side_chat,
        };
        target.forked_from = if !source_run.id.is_empty() {
            Some(ForkedFrom {
                fork_type: "run".to_string(),
                thread_id: source.thread.id.clone(),
                run_id: source_run.id.clone(),
                node_id: String::new(),
                provider_thread_id: String::new(),
                provider_turn_id: None,
            })
        } else {
            None
        };
        target.created_at = plan.now;
        target.updated_at = plan.now;
        target.archived_at = None;
        target.settled_override = None;
        target.settled_at = None;
        target.last_visited_at = None;
        target.deleted_at = None;

        let source_provider = source_run
            .provider_thread_id
            .as_ref()
            .and_then(|ptid| source.provider_thread(ptid))
            .unwrap_or_default();
        if let Some(ref native_thread_ref) = source_provider.native_thread_ref {
            source_point.provider_thread_ref = Some(native_thread_ref.clone());
            if let Some(ref active_attempt_id) = source_run.active_attempt_id {
                if let Some(turn) =
                    latest_provider_turn_for_attempt(&source.provider_turns, active_attempt_id)
                {
                    source_point.provider_turn_ref = turn.native_turn_ref;
                }
            }
        }

        let transfer_id = self.gen_random_id(
            "context-transfer",
            &[
                "type",
                "fork",
                "source-thread",
                &source.thread.id,
                "target-thread",
                &target.id,
            ],
        )?;
        let provider_instance = if !source_run.provider_instance_id.is_empty() {
            source_run.provider_instance_id.clone()
        } else {
            source.thread.provider_instance_id.clone()
        };

        let mut transfer = ContextTransfer {
            id: transfer_id,
            transfer_type: "fork".to_string(),
            source_thread_id: source.thread.id.clone(),
            target_thread_id: target.id.clone(),
            source_point,
            base_point: None,
            source_provider_instance_id: Some(provider_instance.clone()),
            target_provider_instance_id: None,
            target_run_id: None,
            status: "pending".to_string(),
            resolution: None,
            error: None,
            created_by: actor,
            created_at: plan.now,
            updated_at: plan.now,
            consumed_at: None,
        };

        if !source_provider.id.is_empty()
            && source_provider
                .native_thread_ref
                .as_ref()
                .map(|r| r.strength != NATIVE_REF_STRONG)
                .unwrap_or(true)
        {
            transfer.error = Some(
                "Source provider thread does not expose a strong native thread ref.".to_string(),
            );
        }

        let target_id = target.id.clone();
        let target_instance = target.provider_instance_id.clone();
        plan.emit(EventSpec {
            event_type: EVENT_THREAD_CREATED.to_string(),
            thread_id: target_id.clone(),
            instance_id: target_instance,
            payload: EventPayload::Thread(target),
            ..Default::default()
        })?;
        plan.emit(EventSpec {
            event_type: EVENT_CONTEXT_TRANSFER_CREATED.to_string(),
            thread_id: target_id,
            instance_id: provider_instance,
            payload: EventPayload::ContextTransfer(transfer),
            ..Default::default()
        })?;
        Ok(())
    }
}
