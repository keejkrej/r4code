use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use rusqlite::{Connection, OptionalExtension, ToSql, TransactionBehavior, params};

use crate::orchestration::command::*;
use crate::orchestration::errors::*;
use crate::orchestration::event::*;
use crate::orchestration::ids::*;
use crate::orchestration::outbox::*;
use crate::orchestration::projection::*;
use crate::orchestration::store::*;
use crate::orchestration::types::*;

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path_str = path.as_ref().to_string_lossy();
        let conn = if path_str == ":memory:" {
            Connection::open_in_memory().map_err(sql_err)?
        } else {
            if let Some(parent) = path.as_ref().parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        Error::with_detail(
                            ErrorKind::StorageError,
                            format!("create sqlite dir: {e}"),
                        )
                    })?;
                }
            }
            let c = Connection::open(path.as_ref()).map_err(sql_err)?;
            c.execute_batch(
                "PRAGMA busy_timeout = 5000;
                 PRAGMA journal_mode = WAL;
                 PRAGMA foreign_keys = ON;",
            )
            .map_err(sql_err)?;
            c
        };

        let store = Self {
            conn: Mutex::new(conn),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn close(&self) -> Result<(), Error> {
        let guard = self.lock_conn()?;
        let _ = guard.execute_batch("PRAGMA optimize;");
        Ok(())
    }

    fn lock_conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>, Error> {
        self.conn
            .lock()
            .map_err(|_| Error::internal("sqlite connection lock poisoned"))
    }

    fn migrate(&self) -> Result<(), Error> {
        let schema = "
CREATE TABLE IF NOT EXISTS orchestration_events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  thread_id TEXT NOT NULL,
  command_id TEXT,
  event_type TEXT NOT NULL,
  raw_event_id TEXT,
  event_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS orchestration_events_thread ON orchestration_events(thread_id, sequence);
CREATE INDEX IF NOT EXISTS orchestration_events_command ON orchestration_events(command_id);
CREATE INDEX IF NOT EXISTS orchestration_events_raw ON orchestration_events(raw_event_id);
CREATE TABLE IF NOT EXISTS orchestration_receipts (
  command_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  command_type TEXT NOT NULL,
  accepted_at TEXT NOT NULL,
  result_sequence INTEGER NOT NULL,
  status TEXT NOT NULL,
  error TEXT
);
CREATE TABLE IF NOT EXISTS orchestration_effects (
  id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL,
  thread_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  claimed_by TEXT,
  lease_expires_at INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS orchestration_effects_claim ON orchestration_effects(status, lease_expires_at, created_at);
";
        let guard = self.lock_conn()?;
        guard.execute_batch(schema).map_err(sql_err)
    }
}

fn sql_err(err: rusqlite::Error) -> Error {
    Error::with_detail(ErrorKind::StorageError, err.to_string())
}

fn json_err(err: serde_json::Error) -> Error {
    Error::with_detail(ErrorKind::StorageError, err.to_string())
}

fn parse_rfc3339(value: &str) -> Result<DateTime<Utc>, Error> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| Error::with_detail(ErrorKind::StorageError, format!("parse time {value:?}: {e}")))
}

fn read_receipt_conn(conn: &Connection, command_id: &str) -> Result<Option<Receipt>, Error> {
    let mut stmt = conn
        .prepare(
            "SELECT command_id, thread_id, command_type, accepted_at, result_sequence, status, error
             FROM orchestration_receipts WHERE command_id = ?1",
        )
        .map_err(sql_err)?;
    let row = stmt
        .query_row(params![command_id], |row| {
            let command_id: String = row.get(0)?;
            let thread_id: String = row.get(1)?;
            let command_type: String = row.get(2)?;
            let accepted_at: String = row.get(3)?;
            let result_sequence: i64 = row.get(4)?;
            let status: String = row.get(5)?;
            let error: Option<String> = row.get(6)?;
            Ok((
                command_id,
                thread_id,
                command_type,
                accepted_at,
                result_sequence as u64,
                status,
                error,
            ))
        })
        .optional()
        .map_err(sql_err)?;

    let Some((command_id, thread_id, command_type, accepted_at_str, result_sequence, status, error)) =
        row
    else {
        return Ok(None);
    };
    let accepted_at = parse_rfc3339(&accepted_at_str)?;
    Ok(Some(Receipt {
        command_id,
        thread_id,
        command_type,
        accepted_at,
        result_sequence,
        status,
        error,
    }))
}

fn read_thread_events_conn(conn: &Connection, thread_id: &str) -> Result<Vec<StoredEvent>, Error> {
    let mut stmt = conn
        .prepare(
            "SELECT event_json FROM orchestration_events WHERE thread_id = ?1 ORDER BY sequence",
        )
        .map_err(sql_err)?;
    let rows = stmt
        .query_map(params![thread_id], |row| row.get::<_, String>(0))
        .map_err(sql_err)?;
    let mut result = Vec::new();
    for r in rows {
        let data = r.map_err(sql_err)?;
        let event: StoredEvent = serde_json::from_str(&data).map_err(json_err)?;
        result.push(event);
    }
    Ok(result)
}

fn read_command_events_conn(
    conn: &Connection,
    command_id: &str,
) -> Result<Vec<StoredEvent>, Error> {
    let mut stmt = conn
        .prepare(
            "SELECT event_json FROM orchestration_events WHERE command_id = ?1 ORDER BY sequence",
        )
        .map_err(sql_err)?;
    let rows = stmt
        .query_map(params![command_id], |row| row.get::<_, String>(0))
        .map_err(sql_err)?;
    let mut result = Vec::new();
    for r in rows {
        let data = r.map_err(sql_err)?;
        let event: StoredEvent = serde_json::from_str(&data).map_err(json_err)?;
        result.push(event);
    }
    Ok(result)
}

impl Store for SqliteStore {
    fn snapshot(&self, thread_id: &str) -> Result<ThreadSnapshot, Error> {
        let events = self.read(&EventFilter {
            thread_id: Some(thread_id.to_string()),
            ..Default::default()
        })?;
        if events.is_empty() {
            return Err(Error::from(ErrorKind::ThreadNotFound));
        }
        let mut projections = replay(&events)?;
        let projection = projections
            .remove(thread_id)
            .ok_or_else(|| Error::from(ErrorKind::ThreadNotFound))?;
        let snapshot_sequence = events.last().unwrap().sequence;
        Ok(ThreadSnapshot {
            projection,
            snapshot_sequence,
        })
    }

    fn list(&self) -> Result<Vec<ThreadSnapshot>, Error> {
        let ids: Vec<ThreadID> = {
            let guard = self.lock_conn()?;
            let mut stmt = guard
                .prepare("SELECT DISTINCT thread_id FROM orchestration_events ORDER BY thread_id")
                .map_err(sql_err)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(sql_err)?;
            let mut list = Vec::new();
            for r in rows {
                list.push(r.map_err(sql_err)?);
            }
            list.sort();
            list
        };
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            result.push(self.snapshot(&id)?);
        }
        Ok(result)
    }

    fn read(&self, filter: &EventFilter) -> Result<Vec<StoredEvent>, Error> {
        let guard = self.lock_conn()?;
        let mut query = String::from("SELECT event_json FROM orchestration_events");
        let mut conditions: Vec<&str> = Vec::new();
        let mut args: Vec<Box<dyn ToSql>> = Vec::new();

        if let Some(after) = filter.after_sequence {
            conditions.push("sequence > ?");
            args.push(Box::new(after as i64));
        }
        if let Some(through) = filter.through_sequence {
            conditions.push("sequence <= ?");
            args.push(Box::new(through as i64));
        }
        if let Some(ref tid) = filter.thread_id {
            conditions.push("thread_id = ?");
            args.push(Box::new(tid.clone()));
        }
        if let Some(ref etype) = filter.event_type {
            conditions.push("event_type = ?");
            args.push(Box::new(etype.clone()));
        }
        if let Some(ref cid) = filter.command_id {
            conditions.push("command_id = ?");
            args.push(Box::new(cid.clone()));
        }
        if let Some(ref raw_id) = filter.raw_event_id {
            conditions.push("raw_event_id = ?");
            args.push(Box::new(raw_id.clone()));
        }
        if !conditions.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&conditions.join(" AND "));
        }
        query.push_str(" ORDER BY sequence");
        if filter.limit > 0 {
            query.push_str(" LIMIT ?");
            args.push(Box::new(filter.limit as i64));
        }

        let mut stmt = guard.prepare(&query).map_err(sql_err)?;
        let param_refs: Vec<&dyn ToSql> = args.iter().map(|a| a.as_ref()).collect();
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| row.get::<_, String>(0))
            .map_err(sql_err)?;
        let mut result = Vec::new();
        for r in rows {
            let data = r.map_err(sql_err)?;
            let event: StoredEvent = serde_json::from_str(&data).map_err(json_err)?;
            result.push(event);
        }
        Ok(result)
    }

    fn get_receipt(&self, command_id: &str) -> Result<Option<Receipt>, Error> {
        let guard = self.lock_conn()?;
        read_receipt_conn(&guard, command_id)
    }

    fn commit(&self, input: CommitInput) -> Result<CommitResult, Error> {
        let mut guard = self.lock_conn()?;
        let tx = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;

        if let Some(ref receipt) = input.receipt {
            if let Some(existing) = read_receipt_conn(&tx, &receipt.command_id)? {
                let events = read_command_events_conn(&tx, &receipt.command_id)?;
                tx.commit().map_err(sql_err)?;
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
                let current_i64: i64 = tx
                    .query_row(
                        "SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events WHERE thread_id = ?1",
                        params![tid],
                        |row| row.get(0),
                    )
                    .map_err(sql_err)?;
                let current = current_i64 as u64;
                if current != seq {
                    return Err(Error::with_detail(
                        ErrorKind::SequenceConflict,
                        format!("for thread {tid}: expected {seq}, actual {current}"),
                    ));
                }
            }
        }

        let mut result_sequence = input.expected_sequence;
        let mut stored_events = Vec::with_capacity(input.events.len());
        let mut staged_projections: HashMap<ThreadID, Projection> = HashMap::new();

        for event in &input.events {
            let data = serde_json::to_string(event).map_err(json_err)?;
            let projection = if let Some(p) = staged_projections.get(&event.thread_id) {
                p.clone()
            } else {
                let previous = read_thread_events_conn(&tx, &event.thread_id)?;
                if !previous.is_empty() {
                    let mut projections = replay(&previous)?;
                    projections.remove(&event.thread_id).unwrap_or_default()
                } else {
                    Projection::default()
                }
            };

            let updated_projection = apply(&projection, event)?;
            staged_projections.insert(event.thread_id.clone(), updated_projection);

            let command_id: Option<CommandID> = if let Some(ref r) = input.receipt {
                Some(r.command_id.clone())
            } else {
                input.command_id.clone()
            };

            tx.execute(
                "INSERT INTO orchestration_events
                 (event_id, thread_id, command_id, event_type, raw_event_id, event_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    event.id,
                    event.thread_id,
                    command_id,
                    event.event_type,
                    event.raw_event_id,
                    data
                ],
            )
            .map_err(sql_err)?;

            let sequence = tx.last_insert_rowid() as u64;
            let event_roundtrip: Event = serde_json::from_str(&data).map_err(json_err)?;
            let stored = StoredEvent {
                sequence,
                command_id,
                event: event_roundtrip,
            };
            let stored_json = serde_json::to_string(&stored).map_err(json_err)?;
            tx.execute(
                "UPDATE orchestration_events SET event_json = ?1 WHERE sequence = ?2",
                params![stored_json, sequence as i64],
            )
            .map_err(sql_err)?;

            stored_events.push(stored);
            result_sequence = sequence;
        }

        let created_at = input
            .receipt
            .as_ref()
            .map(|r| r.accepted_at)
            .unwrap_or_else(Utc::now);
        let created_at_str = created_at.to_rfc3339();

        for effect in &input.effects {
            let data = serde_json::to_string(effect).map_err(json_err)?;
            tx.execute(
                "INSERT INTO orchestration_effects
                 (id, command_id, thread_id, kind, payload_json, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6, ?7)",
                params![
                    effect.id,
                    effect.command_id,
                    effect.thread_id,
                    effect.kind,
                    data,
                    created_at_str,
                    created_at_str
                ],
            )
            .map_err(sql_err)?;
        }

        let receipt_out = if let Some(mut receipt) = input.receipt {
            receipt.result_sequence = result_sequence;
            tx.execute(
                "INSERT INTO orchestration_receipts
                 (command_id, thread_id, command_type, accepted_at, result_sequence, status, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    receipt.command_id,
                    receipt.thread_id,
                    receipt.command_type,
                    receipt.accepted_at.to_rfc3339(),
                    receipt.result_sequence as i64,
                    receipt.status,
                    receipt.error
                ],
            )
            .map_err(sql_err)?;
            Some(receipt)
        } else {
            None
        };

        tx.commit().map_err(sql_err)?;
        Ok(CommitResult {
            sequence: result_sequence,
            stored_events,
            receipt: receipt_out,
            committed: true,
        })
    }
}

impl OutboxStore for SqliteStore {
    fn claim_effects(
        &self,
        worker_id: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<OutboxEffect>, Error> {
        if worker_id.is_empty() {
            return Err(Error::internal("worker ID is required"));
        }
        let limit = if limit == 0 { 10 } else { limit };
        let lease = if lease.is_zero() {
            Duration::from_secs(60)
        } else {
            lease
        };
        let now = Utc::now();
        let now_ms = now.timestamp_millis();
        let lease_chrono =
            chrono::Duration::from_std(lease).unwrap_or_else(|_| chrono::Duration::seconds(60));
        let lease_expires = now + lease_chrono;
        let lease_expires_ms = lease_expires.timestamp_millis();
        let now_str = now.to_rfc3339();

        let mut guard = self.lock_conn()?;
        let tx = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;

        let effects: Vec<Effect> = {
            let mut stmt = tx
                .prepare(
                    "SELECT payload_json FROM orchestration_effects
                     WHERE status = 'pending' OR (status = 'running' AND lease_expires_at <= ?1)
                     ORDER BY created_at, id LIMIT ?2",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![now_ms, limit as i64], |row| row.get::<_, String>(0))
                .map_err(sql_err)?;
            let mut list = Vec::new();
            for r in rows {
                let data = r.map_err(sql_err)?;
                let eff: Effect = serde_json::from_str(&data).map_err(json_err)?;
                list.push(eff);
            }
            list
        };

        let mut claimed = Vec::with_capacity(effects.len());
        for effect in effects {
            let count = tx
                .execute(
                    "UPDATE orchestration_effects
                     SET status = 'running', attempts = attempts + 1, claimed_by = ?1, lease_expires_at = ?2, updated_at = ?3
                     WHERE id = ?4 AND (status = 'pending' OR (status = 'running' AND lease_expires_at <= ?5))",
                    params![worker_id, lease_expires_ms, now_str, effect.id, now_ms],
                )
                .map_err(sql_err)?;
            if count != 1 {
                continue;
            }
            claimed.push(OutboxEffect {
                effect,
                status: EFFECT_RUNNING.to_string(),
                attempts: 0,
                last_error: None,
                claimed_by: worker_id.to_string(),
                lease_expires_at: Some(lease_expires),
                created_at: default_zero_time(),
                updated_at: now,
            });
        }
        tx.commit().map_err(sql_err)?;

        for item in &mut claimed {
            let attempts: i32 = guard
                .query_row(
                    "SELECT attempts FROM orchestration_effects WHERE id = ?1",
                    params![item.effect.id],
                    |row| row.get(0),
                )
                .map_err(sql_err)?;
            item.attempts = attempts;
        }
        Ok(claimed)
    }

    fn complete_effect(
        &self,
        effect_id: &str,
        worker_id: &str,
        execution_err: Option<&str>,
    ) -> Result<(), Error> {
        let (status, failure) = match execution_err {
            Some(err_msg) => (EFFECT_FAILED, Some(err_msg.to_string())),
            None => (EFFECT_COMPLETED, None),
        };
        let now = Utc::now();
        let guard = self.lock_conn()?;
        let count = guard
            .execute(
                "UPDATE orchestration_effects
                 SET status = ?1, last_error = ?2, claimed_by = NULL, lease_expires_at = NULL, updated_at = ?3
                 WHERE id = ?4 AND claimed_by = ?5 AND status = 'running' AND lease_expires_at > ?6",
                params![
                    status,
                    failure,
                    now.to_rfc3339(),
                    effect_id,
                    worker_id,
                    now.timestamp_millis()
                ],
            )
            .map_err(sql_err)?;
        if count != 1 {
            return Err(Error::internal(format!(
                "effect {effect_id} is no longer claimed by {worker_id}"
            )));
        }
        Ok(())
    }

    fn renew_effect(
        &self,
        effect_id: &str,
        worker_id: &str,
        lease: Duration,
    ) -> Result<bool, Error> {
        let lease = if lease.is_zero() {
            Duration::from_secs(60)
        } else {
            lease
        };
        let now = Utc::now();
        let lease_chrono =
            chrono::Duration::from_std(lease).unwrap_or_else(|_| chrono::Duration::seconds(60));
        let expires_ms = (now + lease_chrono).timestamp_millis();
        let guard = self.lock_conn()?;
        let count = guard
            .execute(
                "UPDATE orchestration_effects
                 SET lease_expires_at = ?1, updated_at = ?2
                 WHERE id = ?3 AND claimed_by = ?4 AND status = 'running' AND lease_expires_at > ?5",
                params![
                    expires_ms,
                    now.to_rfc3339(),
                    effect_id,
                    worker_id,
                    now.timestamp_millis()
                ],
            )
            .map_err(sql_err)?;
        Ok(count == 1)
    }

    fn list_effects(&self) -> Result<Vec<OutboxEffect>, Error> {
        let guard = self.lock_conn()?;
        let mut stmt = guard
            .prepare(
                "SELECT payload_json, status, attempts, last_error, claimed_by,
                        lease_expires_at, created_at, updated_at
                 FROM orchestration_effects ORDER BY created_at, id",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| {
                let data: String = row.get(0)?;
                let status: String = row.get(1)?;
                let attempts: i32 = row.get(2)?;
                let last_error: Option<String> = row.get(3)?;
                let claimed_by: Option<String> = row.get(4)?;
                let lease_expires_at: Option<i64> = row.get(5)?;
                let created_at: String = row.get(6)?;
                let updated_at: String = row.get(7)?;
                Ok((
                    data,
                    status,
                    attempts,
                    last_error,
                    claimed_by,
                    lease_expires_at,
                    created_at,
                    updated_at,
                ))
            })
            .map_err(sql_err)?;

        let mut result = Vec::new();
        for r in rows {
            let (
                data,
                status,
                attempts,
                last_error,
                claimed_by,
                lease_expires_ms,
                created_at_str,
                updated_at_str,
            ) = r.map_err(sql_err)?;
            let effect: Effect = serde_json::from_str(&data).map_err(json_err)?;
            let created_at = parse_rfc3339(&created_at_str)?;
            let updated_at = parse_rfc3339(&updated_at_str)?;
            let lease_expires_at =
                lease_expires_ms.and_then(|ms| Utc.timestamp_millis_opt(ms).single());
            result.push(OutboxEffect {
                effect,
                status,
                attempts,
                last_error,
                claimed_by: claimed_by.unwrap_or_default(),
                lease_expires_at,
                created_at,
                updated_at,
            });
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    use tempfile::TempDir;

    use super::*;
    use crate::orchestration::*;

    fn create_command(command_id: &str, thread_id: &str) -> Command {
        Command {
            command_type: COMMAND_THREAD_CREATE.to_string(),
            command_id: command_id.to_string(),
            thread_id: thread_id.to_string(),
            payload: CommandPayload::CreateThread(CreateThread {
                project_id: "project".to_string(),
                title: "Durable thread".to_string(),
                model_selection: ModelSelection {
                    instance_id: "codex".to_string(),
                    model: "default".to_string(),
                    options: Default::default(),
                },
                created_by: ACTOR_USER.to_string(),
                creation_source: CREATION_WEB.to_string(),
                ..Default::default()
            }),
        }
    }

    fn message_command(command_id: &str, thread_id: &str, message_id: &str) -> Command {
        Command {
            command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
            command_id: command_id.to_string(),
            thread_id: thread_id.to_string(),
            payload: CommandPayload::DispatchMessage(DispatchMessage {
                message_id: message_id.to_string(),
                text: "hello".to_string(),
                dispatch_mode: DispatchMode {
                    mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                    ..Default::default()
                },
                created_by: ACTOR_USER.to_string(),
                creation_source: CREATION_WEB.to_string(),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_durable_events_receipts_and_outbox_across_restart() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.sqlite");

        let store1 = Arc::new(SqliteStore::open(&path).unwrap());
        let o = Orchestrator::new(Arc::clone(&store1) as Arc<dyn Store>);
        let _created = o.dispatch(create_command("create", "thread-1")).unwrap();
        let sent = o
            .dispatch(message_command("send", "thread-1", "message-1"))
            .unwrap();
        assert_eq!(sent.effects.len(), 1);

        let replayed = o
            .dispatch(message_command("send", "thread-1", "message-1"))
            .unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.sequence, sent.sequence);
        assert!(replayed.effects.is_empty());

        let effects = store1.list_effects().unwrap();
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].status, EFFECT_PENDING);
        store1.close().unwrap();
        drop(o);
        drop(store1);

        let store2 = SqliteStore::open(&path).unwrap();
        let snapshot = store2.snapshot("thread-1").unwrap();
        assert_eq!(snapshot.snapshot_sequence, sent.sequence);
        assert_eq!(snapshot.projection.runs.len(), 1);
        assert_eq!(snapshot.projection.messages[0].text, "hello");

        let events = store2.read(&EventFilter::default()).unwrap();
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event.sequence, (index + 1) as u64);
        }

        let claimed = store2
            .claim_effects("worker-1", 10, Duration::from_secs(60))
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].attempts, 1);
        assert!(matches!(
            claimed[0].effect.payload,
            EffectPayload::TurnStart(_)
        ));

        store2
            .complete_effect(&sent.effects[0].id, "worker-1", None)
            .unwrap();
        let effects = store2.list_effects().unwrap();
        assert_eq!(effects[0].status, EFFECT_COMPLETED);

        let reclaimed = store2
            .claim_effects("worker-2", 10, Duration::from_secs(60))
            .unwrap();
        assert!(reclaimed.is_empty());
    }

    #[test]
    fn test_expired_effect_lease_can_be_reclaimed_once() {
        let dir = TempDir::new().unwrap();
        let s = Arc::new(SqliteStore::open(dir.path().join("state.sqlite")).unwrap());
        let o = Orchestrator::new(Arc::clone(&s) as Arc<dyn Store>);
        o.dispatch(create_command("create", "thread-1")).unwrap();
        o.dispatch(message_command("send", "thread-1", "message-1"))
            .unwrap();

        let first = s
            .claim_effects("worker-1", 1, Duration::from_millis(1))
            .unwrap();
        assert_eq!(first.len(), 1);
        thread::sleep(Duration::from_millis(10));

        let second = s
            .claim_effects("worker-2", 1, Duration::from_secs(60))
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].attempts, 2);

        let stale = s.complete_effect(&first[0].effect.id, "worker-1", None);
        assert!(stale.is_err(), "stale worker completed a reclaimed effect");

        s.complete_effect(
            &second[0].effect.id,
            "worker-2",
            Some("provider unavailable"),
        )
        .unwrap();
        let effects = s.list_effects().unwrap();
        assert_eq!(effects[0].status, EFFECT_FAILED);
        assert!(effects[0].last_error.is_some());
    }

    #[test]
    fn test_worker_completes_provider_turn_after_restart() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.sqlite");

        {
            let s1 = Arc::new(SqliteStore::open(&path).unwrap());
            let o1 = Orchestrator::new(Arc::clone(&s1) as Arc<dyn Store>);
            o1.dispatch(create_command("create", "thread-1")).unwrap();
            o1.dispatch(message_command("send", "thread-1", "message-1"))
                .unwrap();
            s1.close().unwrap();
        }

        let s2 = Arc::new(SqliteStore::open(&path).unwrap());
        let o2 = Arc::new(Orchestrator::new(Arc::clone(&s2) as Arc<dyn Store>));
        let o2_exec = Arc::clone(&o2);

        let executor: Arc<dyn EffectExecutor> =
            Arc::new(move |_ctx: &ExecutionContext, effect: &Effect| {
                let EffectPayload::TurnStart(ref start) = effect.payload else {
                    return Err(Error::internal("expected TurnStartEffect"));
                };
                let mut turn = ProviderTurn {
                    id: "turn-restored".to_string(),
                    provider_thread_id: start.run.provider_thread_id.clone().unwrap(),
                    node_id: start.run.root_node_id.clone().unwrap(),
                    run_attempt_id: start.run.active_attempt_id.clone(),
                    ordinal: 1,
                    status: PROVIDER_TURN_RUNNING.to_string(),
                    ..Default::default()
                };
                o2_exec.ingest(IngestInput {
                    thread_id: start.thread.id.clone(),
                    run_id: Some(start.run.id.clone()),
                    provider_instance_id: start.run.provider_instance_id.clone(),
                    event: ProviderEvent {
                        event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                        provider_turn: Some(turn.clone()),
                        ..Default::default()
                    },
                    ..Default::default()
                })?;
                turn.status = PROVIDER_TURN_COMPLETED.to_string();
                o2_exec.ingest(IngestInput {
                    thread_id: start.thread.id.clone(),
                    run_id: Some(start.run.id.clone()),
                    provider_instance_id: start.run.provider_instance_id.clone(),
                    event: ProviderEvent {
                        event_type: PROVIDER_EVENT_TURN_UPDATED.to_string(),
                        provider_turn: Some(turn),
                        ..Default::default()
                    },
                    ..Default::default()
                })?;
                Ok(())
            });

        let mut worker = EffectWorker::new(
            Arc::clone(&s2) as Arc<dyn OutboxStore>,
            executor,
            "worker-restart",
        );
        worker.lease = Duration::from_secs(60);

        let count = worker.run_once().unwrap();
        assert_eq!(count, 1);

        let snapshot = o2.snapshot("thread-1").unwrap();
        assert_eq!(snapshot.projection.runs[0].status, RUN_COMPLETED);
    }

    #[test]
    fn test_worker_renews_long_running_effect_lease() {
        let dir = TempDir::new().unwrap();
        let s = Arc::new(SqliteStore::open(dir.path().join("state.sqlite")).unwrap());
        let o = Orchestrator::new(Arc::clone(&s) as Arc<dyn Store>);
        o.dispatch(create_command("create", "thread-1")).unwrap();
        o.dispatch(message_command("send", "thread-1", "message-1"))
            .unwrap();

        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(Mutex::new(release_rx));

        let executor: Arc<dyn EffectExecutor> =
            Arc::new(move |_ctx: &ExecutionContext, _effect: &Effect| {
                let _ = started_tx.send(());
                let _ = release_rx.lock().unwrap().recv();
                Ok(())
            });

        let mut worker = EffectWorker::new(
            Arc::clone(&s) as Arc<dyn OutboxStore>,
            executor,
            "worker-long",
        );
        worker.lease = Duration::from_millis(30);
        let worker = Arc::new(worker);
        let worker_bg = Arc::clone(&worker);

        let handle = thread::spawn(move || worker_bg.run_once());
        started_rx.recv().unwrap();
        thread::sleep(Duration::from_millis(70));

        let claimed = s
            .claim_effects("worker-other", 1, Duration::from_secs(60))
            .unwrap();
        assert!(
            claimed.is_empty(),
            "running effect was reclaimed despite renewal: {claimed:?}"
        );

        let _ = release_tx.send(());
        handle.join().unwrap().unwrap();

        let effects = s.list_effects().unwrap();
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].status, EFFECT_COMPLETED);
        assert_eq!(effects[0].attempts, 1);
    }

    #[test]
    fn test_multi_thread_delegation_survives_restart() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.sqlite");

        let child_id = {
            let s = Arc::new(SqliteStore::open(&path).unwrap());
            let o = Orchestrator::new(Arc::clone(&s) as Arc<dyn Store>);
            o.dispatch(create_command("create", "parent")).unwrap();
            o.dispatch(message_command("send", "parent", "parent-message"))
                .unwrap();
            let parent = o.snapshot("parent").unwrap();
            let run = &parent.projection.runs[0];
            let result = o
                .dispatch(Command {
                    command_type: COMMAND_DELEGATED_TASK_REQUEST.to_string(),
                    command_id: "delegate".to_string(),
                    thread_id: "parent".to_string(),
                    payload: CommandPayload::DelegateTask(DelegateTask {
                        parent_thread_id: "parent".to_string(),
                        parent_run_id: run.id.clone(),
                        parent_node_id: run.root_node_id.clone().unwrap(),
                        task: "child task".to_string(),
                        model_selection: ModelSelection {
                            instance_id: "codex".to_string(),
                            model: "default".to_string(),
                            options: Default::default(),
                        },
                        created_by: ACTOR_AGENT.to_string(),
                        creation_source: CREATION_MCP.to_string(),
                        ..Default::default()
                    }),
                })
                .unwrap();
            assert_eq!(result.effects.len(), 1);
            s.close().unwrap();
            derive_delegated_task_thread_id("delegate")
        };

        let reopened = SqliteStore::open(&path).unwrap();
        let parent_after = reopened.snapshot("parent").unwrap();
        let child_after = reopened.snapshot(&child_id).unwrap();
        assert_eq!(parent_after.projection.subagents.len(), 1);
        assert_eq!(child_after.projection.runs.len(), 1);
        assert_eq!(child_after.projection.context_transfers.len(), 1);

        let encoded_parent = serde_json::to_string(&parent_after.projection.subagents[0]).unwrap();
        assert!(encoded_parent.contains("app_owned"));
    }
}
