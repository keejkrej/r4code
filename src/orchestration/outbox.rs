use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::orchestration::command::Effect;
use crate::orchestration::errors::Error;
use crate::orchestration::types::default_zero_time;

pub type EffectStatus = String;

pub const EFFECT_PENDING: &str = "pending";
pub const EFFECT_RUNNING: &str = "running";
pub const EFFECT_COMPLETED: &str = "completed";
pub const EFFECT_FAILED: &str = "failed";
pub const EFFECT_CANCELLED: &str = "cancelled";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEffect {
    pub effect: Effect,
    pub status: EffectStatus,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub claimed_by: String,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Default for OutboxEffect {
    fn default() -> Self {
        Self {
            effect: Effect::default(),
            status: String::new(),
            attempts: 0,
            last_error: None,
            claimed_by: String::new(),
            lease_expires_at: None,
            created_at: default_zero_time(),
            updated_at: default_zero_time(),
        }
    }
}

pub trait OutboxStore: Send + Sync {
    fn claim_effects(
        &self,
        worker_id: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<OutboxEffect>, Error>;
    fn complete_effect(
        &self,
        effect_id: &str,
        worker_id: &str,
        execution_err: Option<&str>,
    ) -> Result<(), Error>;
    fn renew_effect(
        &self,
        effect_id: &str,
        worker_id: &str,
        lease: Duration,
    ) -> Result<bool, Error>;
    fn list_effects(&self) -> Result<Vec<OutboxEffect>, Error>;
}

/// ExecutionContext allows an EffectExecutor to observe cancellation when a
/// worker lease expires or is stolen during execution.
#[derive(Clone, Default)]
pub struct ExecutionContext {
    cancelled: Arc<AtomicBool>,
    parent: Option<Arc<AtomicBool>>,
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            parent: None,
        }
    }

    pub fn background() -> Self {
        Self::new()
    }

    pub fn child(&self) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            parent: Some(self.cancelled.clone()),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
            || self
                .parent
                .as_ref()
                .is_some_and(|p| p.load(Ordering::SeqCst))
    }
}

pub trait EffectExecutor: Send + Sync {
    fn execute(&self, ctx: &ExecutionContext, effect: &Effect) -> Result<(), Error>;
}

impl<F> EffectExecutor for F
where
    F: Fn(&ExecutionContext, &Effect) -> Result<(), Error> + Send + Sync,
{
    fn execute(&self, ctx: &ExecutionContext, effect: &Effect) -> Result<(), Error> {
        (self)(ctx, effect)
    }
}

pub struct EffectWorker {
    pub store: Arc<dyn OutboxStore>,
    pub executor: Option<Arc<dyn EffectExecutor>>,
    pub worker_id: String,
    pub limit: usize,
    pub lease: Duration,
    pub poll: Duration,
    run_mu: Mutex<()>,
}

impl EffectWorker {
    pub fn new(
        store: Arc<dyn OutboxStore>,
        executor: Arc<dyn EffectExecutor>,
        worker_id: impl Into<String>,
    ) -> Self {
        Self {
            store,
            executor: Some(executor),
            worker_id: worker_id.into(),
            limit: 0,
            lease: Duration::ZERO,
            poll: Duration::ZERO,
            run_mu: Mutex::new(()),
        }
    }

    pub fn run_once(&self) -> Result<usize, Error> {
        let _guard = self
            .run_mu
            .lock()
            .map_err(|_| Error::internal("worker lock poisoned"))?;
        let executor = self
            .executor
            .as_ref()
            .ok_or_else(|| Error::internal("effect executor is required"))?;
        let claimed = self
            .store
            .claim_effects(&self.worker_id, self.limit, self.lease)?;
        let count = claimed.len();
        for claimed_effect in claimed {
            let execution_err = self.execute_with_lease(executor.as_ref(), &claimed_effect);
            let err_str = execution_err.as_ref().map(|e| e.to_string());
            self.store.complete_effect(
                &claimed_effect.effect.id,
                &self.worker_id,
                err_str.as_deref(),
            )?;
        }
        Ok(count)
    }

    fn execute_with_lease(
        &self,
        executor: &dyn EffectExecutor,
        claimed: &OutboxEffect,
    ) -> Option<Error> {
        let lease = if self.lease.is_zero() {
            Duration::from_secs(60)
        } else {
            self.lease
        };
        let interval = {
            let third = lease / 3;
            if third.is_zero() {
                Duration::from_millis(1)
            } else {
                third
            }
        };

        let ctx = ExecutionContext::new();
        let hb_ctx = ctx.clone();
        let store = Arc::clone(&self.store);
        let effect_id = claimed.effect.id.clone();
        let worker_id = self.worker_id.clone();

        let (stop_tx, stop_rx) = mpsc::channel::<()>();
        let (err_tx, err_rx) = mpsc::channel::<Error>();

        let hb_handle = thread::spawn(move || {
            loop {
                match stop_rx.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if hb_ctx.is_cancelled() {
                            return;
                        }
                        match store.renew_effect(&effect_id, &worker_id, lease) {
                            Ok(true) => {}
                            Ok(false) => {
                                let _ = err_tx.send(Error::internal(format!(
                                    "effect {effect_id} lost its worker lease"
                                )));
                                hb_ctx.cancel();
                                return;
                            }
                            Err(err) => {
                                let _ = err_tx.send(err);
                                hb_ctx.cancel();
                                return;
                            }
                        }
                    }
                }
            }
        });

        let exec_res = executor.execute(&ctx, &claimed.effect);
        ctx.cancel();
        let _ = stop_tx.send(());
        let _ = hb_handle.join();

        match exec_res {
            Err(err) => Some(err),
            Ok(()) => err_rx.try_recv().ok(),
        }
    }

    pub fn run(&self, stop: &ExecutionContext) -> Result<(), Error> {
        let poll = if self.poll.is_zero() {
            Duration::from_millis(50)
        } else {
            self.poll
        };
        loop {
            if stop.is_cancelled() {
                return Ok(());
            }
            let count = self.run_once()?;
            if count > 0 {
                continue;
            }
            if stop.is_cancelled() {
                return Ok(());
            }
            thread::sleep(poll);
        }
    }
}

impl std::fmt::Display for EffectWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "orchestration effect worker {}", self.worker_id)
    }
}
