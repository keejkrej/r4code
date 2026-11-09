use std::collections::HashMap;
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::orchestration::command::*;
use crate::orchestration::errors::*;
use crate::orchestration::event::*;
use crate::orchestration::ids::*;
use crate::orchestration::projection::*;
use crate::orchestration::types::*;

pub type ReceiptStatus = String;

pub const RECEIPT_ACCEPTED: &str = "accepted";
pub const RECEIPT_REJECTED: &str = "rejected";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Receipt {
    #[serde(default)]
    pub command_id: CommandID,
    #[serde(default)]
    pub thread_id: ThreadID,
    #[serde(default)]
    pub command_type: CommandType,
    #[serde(default = "default_zero_time")]
    pub accepted_at: DateTime<Utc>,
    #[serde(default)]
    pub result_sequence: u64,
    #[serde(default)]
    pub status: ReceiptStatus,
    #[serde(default)]
    pub error: Option<String>,
}

impl Default for Receipt {
    fn default() -> Self {
        Self {
            command_id: String::new(),
            thread_id: String::new(),
            command_type: String::new(),
            accepted_at: default_zero_time(),
            result_sequence: 0,
            status: String::new(),
            error: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CommitInput {
    pub thread_id: ThreadID,
    pub expected_sequence: u64,
    pub expected_sequences: HashMap<ThreadID, u64>,
    pub events: Vec<Event>,
    pub effects: Vec<Effect>,
    pub receipt: Option<Receipt>,
    pub command_id: Option<CommandID>,
}

#[derive(Debug, Clone, Default)]
pub struct CommitResult {
    pub sequence: u64,
    pub stored_events: Vec<StoredEvent>,
    pub receipt: Option<Receipt>,
    pub committed: bool,
}

#[derive(Debug, Clone, Default)]
pub struct EventFilter {
    pub after_sequence: Option<u64>,
    pub through_sequence: Option<u64>,
    pub thread_id: Option<ThreadID>,
    pub event_type: Option<EventType>,
    pub command_id: Option<CommandID>,
    pub raw_event_id: Option<RawEventID>,
    pub limit: usize,
}

/// Store is the persistence seam. Commit must atomically check the thread
/// sequence, append all events, fold their projections, and insert the receipt.
/// An existing command receipt wins and returns Committed=false.
pub trait Store: Send + Sync {
    fn snapshot(&self, thread_id: &str) -> Result<ThreadSnapshot, Error>;
    fn list(&self) -> Result<Vec<ThreadSnapshot>, Error>;
    fn read(&self, filter: &EventFilter) -> Result<Vec<StoredEvent>, Error>;
    fn get_receipt(&self, command_id: &str) -> Result<Option<Receipt>, Error>;
    fn commit(&self, input: CommitInput) -> Result<CommitResult, Error>;
}

struct MemoryStoreInner {
    events: Vec<StoredEvent>,
    projections: HashMap<ThreadID, Projection>,
    thread_sequences: HashMap<ThreadID, u64>,
    receipts: HashMap<CommandID, Receipt>,
    sequence: u64,
}

pub struct MemoryStore {
    inner: RwLock<MemoryStoreInner>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(MemoryStoreInner {
                events: Vec::new(),
                projections: HashMap::new(),
                thread_sequences: HashMap::new(),
                receipts: HashMap::new(),
                sequence: 0,
            }),
        }
    }
}

impl Store for MemoryStore {
    fn snapshot(&self, thread_id: &str) -> Result<ThreadSnapshot, Error> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::internal("memory store lock poisoned"))?;
        let projection = guard
            .projections
            .get(thread_id)
            .ok_or_else(|| Error::from(ErrorKind::ThreadNotFound))?;
        let clone = clone_json(projection).map_err(|e| {
            Error::internal(format!("clone thread projection: {e}"))
        })?;
        let seq = guard.thread_sequences.get(thread_id).copied().unwrap_or(0);
        Ok(ThreadSnapshot {
            projection: clone,
            snapshot_sequence: seq,
        })
    }

    fn list(&self) -> Result<Vec<ThreadSnapshot>, Error> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::internal("memory store lock poisoned"))?;
        let mut ids: Vec<ThreadID> = guard.projections.keys().cloned().collect();
        ids.sort();
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            let proj = &guard.projections[&id];
            let clone = clone_json(proj).map_err(|e| {
                Error::internal(format!("clone thread projection: {e}"))
            })?;
            let seq = guard.thread_sequences.get(&id).copied().unwrap_or(0);
            result.push(ThreadSnapshot {
                projection: clone,
                snapshot_sequence: seq,
            });
        }
        Ok(result)
    }

    fn read(&self, filter: &EventFilter) -> Result<Vec<StoredEvent>, Error> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::internal("memory store lock poisoned"))?;
        let mut result = Vec::new();
        for stored in &guard.events {
            if let Some(after) = filter.after_sequence {
                if stored.sequence <= after {
                    continue;
                }
            }
            if let Some(through) = filter.through_sequence {
                if stored.sequence > through {
                    continue;
                }
            }
            if let Some(ref tid) = filter.thread_id {
                if &stored.event.thread_id != tid {
                    continue;
                }
            }
            if let Some(ref etype) = filter.event_type {
                if &stored.event.event_type != etype {
                    continue;
                }
            }
            if let Some(ref cid) = filter.command_id {
                if stored.command_id.as_ref() != Some(cid) {
                    continue;
                }
            }
            if let Some(ref raw_id) = filter.raw_event_id {
                if stored.event.raw_event_id.as_ref() != Some(raw_id) {
                    continue;
                }
            }
            let clone = clone_json(stored).map_err(|e| {
                Error::internal(format!("clone stored event: {e}"))
            })?;
            result.push(clone);
            if filter.limit > 0 && result.len() == filter.limit {
                break;
            }
        }
        Ok(result)
    }

    fn get_receipt(&self, command_id: &str) -> Result<Option<Receipt>, Error> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::internal("memory store lock poisoned"))?;
        Ok(guard.receipts.get(command_id).cloned())
    }

    fn commit(&self, input: CommitInput) -> Result<CommitResult, Error> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::internal("memory store lock poisoned"))?;

        if let Some(ref receipt) = input.receipt {
            if let Some(existing) = guard.receipts.get(&receipt.command_id).cloned() {
                let mut events = Vec::new();
                for stored in &guard.events {
                    if stored.command_id.as_deref() == Some(receipt.command_id.as_str()) {
                        let clone = clone_json(stored).map_err(|e| {
                            Error::internal(format!("clone receipt event: {e}"))
                        })?;
                        events.push(clone);
                    }
                }
                return Ok(CommitResult {
                    sequence: existing.result_sequence,
                    stored_events: events,
                    receipt: Some(existing),
                    committed: false,
                });
            }
        }

        if input
            .receipt
            .as_ref()
            .map(|r| r.status == RECEIPT_ACCEPTED)
            .unwrap_or(true)
        {
            let mut expected: HashMap<ThreadID, u64> = HashMap::new();
            expected.insert(input.thread_id.clone(), input.expected_sequence);
            for (tid, seq) in &input.expected_sequences {
                expected.insert(tid.clone(), *seq);
            }
            for (tid, seq) in expected {
                let current = guard.thread_sequences.get(&tid).copied().unwrap_or(0);
                if current != seq {
                    return Err(Error::with_detail(
                        ErrorKind::SequenceConflict,
                        format!("for thread {tid}: expected {seq}, actual {current}"),
                    ));
                }
            }
        }

        let mut result_sequence = guard
            .thread_sequences
            .get(&input.thread_id)
            .copied()
            .unwrap_or(0);
        let mut stored_events = Vec::with_capacity(input.events.len());

        if !input.events.is_empty() {
            let mut staged_projections: HashMap<ThreadID, Projection> = HashMap::new();
            let mut staged_thread_sequences: HashMap<ThreadID, u64> = HashMap::new();
            let mut staged_events: Vec<StoredEvent> = Vec::with_capacity(input.events.len());
            let mut next_sequence = guard.sequence;

            for event in &input.events {
                let projection = if let Some(p) = staged_projections.get(&event.thread_id) {
                    p.clone()
                } else if let Some(existing) = guard.projections.get(&event.thread_id) {
                    clone_json(existing).map_err(|e| {
                        Error::internal(format!("clone projection before commit: {e}"))
                    })?
                } else {
                    Projection::default()
                };

                let event_clone = clone_json(event).map_err(|e| {
                    Error::internal(format!("clone event {}: {e}", event.id))
                })?;

                let next_proj = apply(&projection, &event_clone).map_err(|err| {
                    Error::with_detail(err.kind, format!("apply event {}: {}", event.id, err))
                })?;

                staged_projections.insert(event.thread_id.clone(), next_proj);
                next_sequence += 1;
                staged_thread_sequences.insert(event.thread_id.clone(), next_sequence);

                let command_id = if let Some(ref r) = input.receipt {
                    Some(r.command_id.clone())
                } else {
                    input.command_id.clone()
                };

                let stored = StoredEvent {
                    sequence: next_sequence,
                    command_id,
                    event: event_clone,
                };
                let result_event = clone_json(&stored).map_err(|e| {
                    Error::internal(format!("clone committed event {}: {e}", event.id))
                })?;
                staged_events.push(stored);
                stored_events.push(result_event);
                result_sequence = next_sequence;
            }

            // Publish only after every event and aggregate applied successfully.
            guard.sequence = next_sequence;
            guard.events.extend(staged_events);
            for (tid, proj) in staged_projections {
                if let Some(&seq) = staged_thread_sequences.get(&tid) {
                    guard.thread_sequences.insert(tid.clone(), seq);
                }
                guard.projections.insert(tid, proj);
            }
        }

        if let Some(mut receipt) = input.receipt {
            receipt.result_sequence = result_sequence;
            guard
                .receipts
                .insert(receipt.command_id.clone(), receipt.clone());
            return Ok(CommitResult {
                sequence: result_sequence,
                stored_events,
                receipt: Some(receipt),
                committed: true,
            });
        }

        Ok(CommitResult {
            sequence: result_sequence,
            stored_events,
            receipt: None,
            committed: true,
        })
    }
}

pub(crate) fn receipt_error(receipt: &Receipt) -> Error {
    let detail = receipt
        .error
        .clone()
        .unwrap_or_else(|| "previously rejected".to_string());
    Error {
        op: format!("replay {}", receipt.command_type),
        command_id: receipt.command_id.clone(),
        thread_id: receipt.thread_id.clone(),
        kind: ErrorKind::CommandPreviouslyRejected,
        detail,
        cause_kinds: vec![ErrorKind::CommandPreviouslyRejected],
        turn_error: None,
    }
}

pub(crate) fn validate_receipt(command: &Command, receipt: &Receipt) -> Result<(), Error> {
    if receipt.thread_id != command.thread_id || receipt.command_type != command.command_type {
        return Err(command_error(
            command,
            ErrorKind::CommandIDConflict,
            format!(
                "receipt targets {}/{}, command targets {}/{}",
                receipt.thread_id, receipt.command_type, command.thread_id, command.command_type
            ),
        ));
    }
    if receipt.status == RECEIPT_REJECTED {
        return Err(receipt_error(receipt));
    }
    Ok(())
}

pub(crate) fn is_not_found(err: &Error) -> bool {
    err.is(ErrorKind::ThreadNotFound)
}
