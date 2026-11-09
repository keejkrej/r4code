use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};

use crate::orchestration::command::*;
use crate::orchestration::errors::*;
use crate::orchestration::event::*;
use crate::orchestration::ids::*;
use crate::orchestration::projection::*;
use crate::orchestration::store::*;
use crate::orchestration::types::*;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProviderInfo {
    pub driver: ProviderDriverKind,
    pub capabilities: ProviderCapabilities,
}

pub trait ProviderResolver: Send + Sync {
    fn resolve(&self, instance_id: &str) -> Result<ProviderInfo, Error>;
}

impl<F> ProviderResolver for F
where
    F: Fn(&str) -> Result<ProviderInfo, Error> + Send + Sync,
{
    fn resolve(&self, instance_id: &str) -> Result<ProviderInfo, Error> {
        (self)(instance_id)
    }
}

pub type ClockFn = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;
pub type IdGeneratorFn = Arc<dyn Fn(&str, &[&str]) -> Result<String, Error> + Send + Sync>;

pub struct Orchestrator {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) now: ClockFn,
    pub(crate) random_id_fn: IdGeneratorFn,
    pub(crate) providers: Mutex<Option<Arc<dyn ProviderResolver>>>,
    locks_mu: Mutex<HashMap<ThreadID, Arc<Mutex<()>>>>,
}

impl Orchestrator {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self {
            store,
            now: Arc::new(Utc::now),
            random_id_fn: Arc::new(|prefix, parts| {
                random_id(prefix, parts).map_err(Error::internal)
            }),
            providers: Mutex::new(None),
            locks_mu: Mutex::new(HashMap::new()),
        }
    }

    pub fn new_in_memory() -> Self {
        Self::new(Arc::new(MemoryStore::new()))
    }

    pub fn with_clock<F>(mut self, now: F) -> Self
    where
        F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        self.now = Arc::new(now);
        self
    }

    pub fn with_id_generator<F>(mut self, generator: F) -> Self
    where
        F: Fn(&str, &[&str]) -> Result<String, Error> + Send + Sync + 'static,
    {
        self.random_id_fn = Arc::new(generator);
        self
    }

    pub fn with_provider_resolver(self, resolver: Arc<dyn ProviderResolver>) -> Self {
        self.set_provider_resolver(resolver);
        self
    }

    pub fn set_provider_resolver(&self, resolver: Arc<dyn ProviderResolver>) {
        let mut guard = self.providers.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(resolver);
    }

    pub fn store(&self) -> &Arc<dyn Store> {
        &self.store
    }

    pub(crate) fn lock(&self, thread_id: &str) -> Arc<Mutex<()>> {
        let mut guard = self.locks_mu.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .entry(thread_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(crate) fn resolve_provider(&self, instance_id: &str) -> Result<ProviderInfo, Error> {
        let resolver = self
            .providers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(resolver) = resolver {
            return resolver.resolve(instance_id);
        }
        // Instance IDs are often driver IDs in a single-installation app. A real
        // registry should replace this default before multi-instance support.
        Ok(ProviderInfo {
            driver: instance_id.to_string(),
            capabilities: ProviderCapabilities {
                turns: TurnCapabilities {
                    supports_queued_messages: true,
                    terminal_status_quality: "strong".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
        })
    }

    pub(crate) fn gen_random_id(&self, prefix: &str, parts: &[&str]) -> Result<String, Error> {
        (self.random_id_fn)(prefix, parts)
    }

    pub fn dispatch(&self, command: Command) -> Result<DispatchResult, Error> {
        if let Err(err) = validate_command(&command) {
            return Err(command_error(&command, ErrorKind::InvalidCommand, err.to_string()));
        }
        let thread_lock = self.lock(&command.thread_id);
        let _guard = thread_lock.lock().unwrap_or_else(|e| e.into_inner());

        match self.store.get_receipt(&command.command_id) {
            Err(err) => {
                return Err(command_error_from(&command, err, "read command receipt"));
            }
            Ok(Some(receipt)) => {
                return self.replay_receipt(&command, &receipt);
            }
            Ok(None) => {}
        }

        let snapshot = match self.store.snapshot(&command.thread_id) {
            Ok(snap) => snap,
            Err(load_err) => {
                if is_not_found(&load_err)
                    && (command.command_type == COMMAND_THREAD_CREATE
                        || command.command_type == COMMAND_THREAD_FORK)
                {
                    ThreadSnapshot::default()
                } else {
                    return Err(command_error_from(&command, load_err, "load projection"));
                }
            }
        };

        let mut plan = CommandPlan {
            orchestrator: self,
            command: command.clone(),
            now: (self.now)(),
            events: Vec::new(),
            effects: Vec::new(),
            projection: snapshot.projection,
            expected_sequences: HashMap::new(),
        };

        if let Err(plan_err) = self.plan(&mut plan) {
            return self.reject(&command, snapshot.snapshot_sequence, plan_err);
        }

        if plan.events.is_empty() {
            return Err(command_error(
                &command,
                ErrorKind::CommandRejected,
                "command produced no domain events",
            ));
        }

        let accepted_at = plan.events.last().unwrap().occurred_at;
        let receipt = Receipt {
            command_id: command.command_id.clone(),
            thread_id: command.thread_id.clone(),
            command_type: command.command_type.clone(),
            accepted_at,
            result_sequence: 0,
            status: RECEIPT_ACCEPTED.to_string(),
            error: None,
        };

        let committed = self
            .store
            .commit(CommitInput {
                thread_id: command.thread_id.clone(),
                expected_sequence: snapshot.snapshot_sequence,
                expected_sequences: plan.expected_sequences,
                events: plan.events,
                effects: plan.effects.clone(),
                receipt: Some(receipt),
                command_id: None,
            })
            .map_err(|err| command_error_from(&command, err, "commit command"))?;

        if let Some(ref committed_receipt) = committed.receipt {
            validate_receipt(&command, committed_receipt)?;
        }

        let mut result = DispatchResult {
            sequence: committed.sequence,
            stored_events: committed.stored_events,
            effects: Vec::new(),
            replayed: !committed.committed,
        };
        if committed.committed {
            result.effects = plan.effects;
        }
        Ok(result)
    }

    fn plan(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        match plan.command.command_type.as_str() {
            COMMAND_THREAD_CREATE => self.plan_thread_create(plan),
            COMMAND_MESSAGE_DISPATCH => self.plan_message_dispatch(plan),
            COMMAND_RUN_INTERRUPT => self.plan_run_interrupt(plan),
            COMMAND_RUNTIME_REQUEST_RESPOND => self.plan_runtime_request_respond(plan),
            COMMAND_DELEGATED_TASK_REQUEST => self.plan_delegated_task(plan),
            COMMAND_THREAD_FORK => self.plan_thread_fork(plan),
            COMMAND_QUEUED_RUN_CANCEL => self.plan_queued_run_cancel(plan),
            COMMAND_QUEUE_RESUME => self.plan_queue_resume(plan),
            COMMAND_PROVIDER_SWITCH => self.plan_provider_switch(plan),
            COMMAND_THREAD_RUNTIME_MODE_SET => self.plan_runtime_mode_change(plan),
            COMMAND_THREAD_ARCHIVE | COMMAND_THREAD_UNARCHIVE | COMMAND_THREAD_DELETE => {
                self.plan_thread_lifecycle(plan)
            }
            _ => Err(Error::from(ErrorKind::InvalidCommand)),
        }
    }

    fn replay_receipt(
        &self,
        command: &Command,
        receipt: &Receipt,
    ) -> Result<DispatchResult, Error> {
        validate_receipt(command, receipt)?;
        let events = self
            .store
            .read(&EventFilter {
                command_id: Some(command.command_id.clone()),
                ..Default::default()
            })
            .map_err(|err| command_error_from(command, err, "read receipt events"))?;
        Ok(DispatchResult {
            sequence: receipt.result_sequence,
            stored_events: events,
            effects: Vec::new(),
            replayed: true,
        })
    }

    fn reject(
        &self,
        command: &Command,
        sequence: u64,
        cause: Error,
    ) -> Result<DispatchResult, Error> {
        let detail = cause.to_string();
        let now = (self.now)();
        let receipt = Receipt {
            command_id: command.command_id.clone(),
            thread_id: command.thread_id.clone(),
            command_type: command.command_type.clone(),
            accepted_at: now,
            result_sequence: sequence,
            status: RECEIPT_REJECTED.to_string(),
            error: Some(detail.clone()),
        };
        if let Err(commit_err) = self.store.commit(CommitInput {
            thread_id: command.thread_id.clone(),
            expected_sequence: sequence,
            receipt: Some(receipt),
            ..Default::default()
        }) {
            return Err(command_error_from(
                command,
                commit_err,
                format!("commit rejection after: {detail}"),
            ));
        }
        if !cause.op.is_empty() || !cause.command_id.is_empty() {
            return Err(cause);
        }
        Err(command_error_from_cause(
            command,
            ErrorKind::CommandRejected,
            &cause,
            detail,
        ))
    }

    pub fn snapshot(&self, thread_id: &str) -> Result<ThreadSnapshot, Error> {
        self.store.snapshot(thread_id)
    }

    pub fn list(&self) -> Result<Vec<ThreadSnapshot>, Error> {
        self.store.list()
    }

    pub fn events(&self, filter: &EventFilter) -> Result<Vec<StoredEvent>, Error> {
        self.store.read(filter)
    }

    fn plan_thread_create(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        let command = plan
            .command
            .payload
            .as_create_thread()
            .cloned()
            .unwrap_or_default();
        if !plan.projection.thread.id.is_empty() {
            return Err(Error {
                op: "create thread".to_string(),
                command_id: plan.command.command_id.clone(),
                thread_id: plan.command.thread_id.clone(),
                kind: ErrorKind::ThreadAlreadyExists,
                detail: String::new(),
                cause_kinds: vec![ErrorKind::ThreadAlreadyExists],
                turn_error: None,
            });
        }
        let (actor, source) = creation_defaults(&command.created_by, &command.creation_source);
        let mut thread = Thread {
            created_by: actor,
            creation_source: source,
            id: plan.command.thread_id.clone(),
            project_id: command.project_id,
            title: command.title,
            provider_instance_id: command.model_selection.instance_id.clone(),
            model_selection: command.model_selection,
            runtime_mode: command.runtime_mode,
            interaction_mode: command.interaction_mode,
            branch: command.branch,
            worktree_path: command.worktree_path,
            active_provider_thread_id: None,
            lineage: ThreadLineage {
                root_thread_id: plan.command.thread_id.clone(),
                ..Default::default()
            },
            forked_from: None,
            created_at: plan.now,
            updated_at: plan.now,
            archived_at: None,
            settled_override: None,
            settled_at: None,
            last_visited_at: None,
            deleted_at: None,
        };
        thread_defaults(&mut thread);
        let thread_id = thread.id.clone();
        let instance_id = thread.provider_instance_id.clone();
        plan.emit(EventSpec {
            event_type: EVENT_THREAD_CREATED.to_string(),
            thread_id,
            instance_id,
            payload: EventPayload::Thread(thread),
            ..Default::default()
        })?;
        Ok(())
    }

    fn plan_thread_lifecycle(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        let mut thread = plan.projection.thread.clone();
        let event_type = match plan.command.command_type.as_str() {
            COMMAND_THREAD_ARCHIVE => {
                thread.archived_at = Some(plan.now);
                EVENT_THREAD_ARCHIVED
            }
            COMMAND_THREAD_UNARCHIVE => {
                thread.archived_at = None;
                EVENT_THREAD_UNARCHIVED
            }
            COMMAND_THREAD_DELETE => {
                thread.deleted_at = Some(plan.now);
                EVENT_THREAD_DELETED
            }
            _ => return Err(Error::from(ErrorKind::InvalidCommand)),
        };
        thread.updated_at = plan.now;
        let thread_id = thread.id.clone();
        let instance_id = thread.provider_instance_id.clone();
        plan.emit(EventSpec {
            event_type: event_type.to_string(),
            thread_id,
            instance_id,
            payload: EventPayload::Thread(thread),
            ..Default::default()
        })?;
        Ok(())
    }

    fn plan_runtime_mode_change(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        let command = plan
            .command
            .payload
            .as_runtime_mode_change()
            .cloned()
            .unwrap_or_default();
        let mut thread = plan.projection.thread.clone();
        if thread.runtime_mode == command.runtime_mode {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                "runtime mode is unchanged",
            ));
        }
        thread.runtime_mode = command.runtime_mode;
        thread.updated_at = plan.now;
        let thread_id = thread.id.clone();
        let instance_id = thread.provider_instance_id.clone();
        plan.emit(EventSpec {
            event_type: EVENT_THREAD_RUNTIME_MODE_UPDATED.to_string(),
            thread_id,
            instance_id,
            payload: EventPayload::Thread(thread),
            ..Default::default()
        })?;
        Ok(())
    }

    fn plan_provider_switch(&self, plan: &mut CommandPlan<'_>) -> Result<(), Error> {
        let command = plan
            .command
            .payload
            .as_provider_switch()
            .cloned()
            .unwrap_or_default();
        let mut thread = plan.projection.thread.clone();
        if thread.model_selection.equal(&command.model_selection) {
            return Err(Error::with_detail(
                ErrorKind::CommandRejected,
                "provider selection is unchanged",
            ));
        }
        thread.provider_instance_id = command.model_selection.instance_id.clone();
        thread.model_selection = command.model_selection;
        thread.updated_at = plan.now;
        let thread_id = thread.id.clone();
        let instance_id = thread.provider_instance_id.clone();
        plan.emit(EventSpec {
            event_type: EVENT_THREAD_PROVIDER_SWITCHED.to_string(),
            thread_id,
            instance_id,
            payload: EventPayload::Thread(thread),
            ..Default::default()
        })?;
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct EventSpec {
    pub event_type: EventType,
    pub thread_id: ThreadID,
    pub run_id: Option<RunID>,
    pub node_id: Option<NodeID>,
    pub driver: ProviderDriverKind,
    pub instance_id: ProviderInstanceID,
    pub raw_event_id: Option<RawEventID>,
    pub payload: EventPayload,
}

pub(crate) struct CommandPlan<'a> {
    pub orchestrator: &'a Orchestrator,
    pub command: Command,
    pub now: DateTime<Utc>,
    pub events: Vec<Event>,
    pub effects: Vec<Effect>,
    pub projection: Projection,
    pub expected_sequences: HashMap<ThreadID, u64>,
}

impl<'a> CommandPlan<'a> {
    pub fn emit(&mut self, spec: EventSpec) -> Result<Event, Error> {
        let id = if self.command.command_id.is_empty() {
            self.orchestrator
                .gen_random_id("event", &["thread", &spec.thread_id])?
        } else {
            self.orchestrator.gen_random_id(
                "event",
                &[
                    "thread",
                    &spec.thread_id,
                    "command",
                    &self.command.command_id,
                ],
            )?
        };
        let event = Event {
            id,
            event_type: spec.event_type,
            thread_id: spec.thread_id,
            run_id: spec.run_id,
            node_id: spec.node_id,
            driver: spec.driver,
            provider_instance_id: spec.instance_id,
            raw_event_id: spec.raw_event_id,
            occurred_at: self.now,
            payload: spec.payload,
        };
        self.events.push(event.clone());
        Ok(event)
    }

    pub fn emit_and_apply(&mut self, spec: EventSpec) -> Result<Event, Error> {
        let event = self.emit(spec)?;
        self.projection = apply(&self.projection, &event)?;
        Ok(event)
    }

    pub fn effect(&mut self, kind: &str, id: String, payload: EffectPayload) {
        self.effects.push(Effect {
            id,
            kind: kind.to_string(),
            command_id: self.command.command_id.clone(),
            thread_id: self.command.thread_id.clone(),
            payload,
        });
    }
}

pub(crate) fn next_turn_item_ordinal(projection: &Projection) -> i64 {
    let mut ordinal = 1;
    for item in &projection.turn_items {
        if item.ordinal >= ordinal {
            ordinal = item.ordinal + 1;
        }
    }
    ordinal
}
