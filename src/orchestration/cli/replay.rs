use std::sync::{Arc, Mutex};

use crate::orchestration::errors::Error;
use crate::orchestration::outbox::ExecutionContext;
use crate::orchestration::types::{ProviderCapabilities, ProviderDriverKind};

use super::adapter::{Adapter, EmitFn, RunRequest};

pub struct ReplayAdapter {
    pub adapter: Arc<dyn Adapter>,
    pub frames: Vec<Vec<u8>>,
}

impl ReplayAdapter {
    pub fn new<A: Adapter + 'static>(adapter: A, frames: Vec<Vec<u8>>) -> Self {
        Self {
            adapter: Arc::new(adapter),
            frames,
        }
    }
}

impl Adapter for ReplayAdapter {
    fn driver(&self) -> ProviderDriverKind {
        self.adapter.driver()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.adapter.capabilities()
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        emit: &EmitFn,
    ) -> Result<(), Error> {
        for frame in &self.frames {
            self.adapter.handle_frame(ctx, request, frame, emit)?;
        }
        Ok(())
    }

    fn handle_frame(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        frame: &[u8],
        emit: &EmitFn,
    ) -> Result<(), Error> {
        self.adapter.handle_frame(ctx, request, frame, emit)
    }
}

pub struct SequenceAdapter {
    pub adapter: Arc<dyn Adapter>,
    pub turns: Vec<Vec<Vec<u8>>>,
    index: Mutex<usize>,
}

impl SequenceAdapter {
    pub fn new<A: Adapter + 'static>(adapter: A, turns: Vec<Vec<Vec<u8>>>) -> Self {
        Self {
            adapter: Arc::new(adapter),
            turns,
            index: Mutex::new(0),
        }
    }

    pub fn new_arc(adapter: Arc<dyn Adapter>, turns: Vec<Vec<Vec<u8>>>) -> Self {
        Self {
            adapter,
            turns,
            index: Mutex::new(0),
        }
    }
}

impl Adapter for SequenceAdapter {
    fn driver(&self) -> ProviderDriverKind {
        self.adapter.driver()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.adapter.capabilities()
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        emit: &EmitFn,
    ) -> Result<(), Error> {
        let current = {
            let mut idx = self.index.lock().unwrap_or_else(|e| e.into_inner());
            if *idx >= self.turns.len() {
                return Err(Error::provider(format!(
                    "sequence replay has {} turns, requested {}",
                    self.turns.len(),
                    *idx + 1
                )));
            }
            let c = *idx;
            *idx += 1;
            c
        };
        for frame in &self.turns[current] {
            self.adapter.handle_frame(ctx, request, frame, emit)?;
        }
        Ok(())
    }

    fn handle_frame(
        &self,
        ctx: &ExecutionContext,
        request: &RunRequest,
        frame: &[u8],
        emit: &EmitFn,
    ) -> Result<(), Error> {
        self.adapter.handle_frame(ctx, request, frame, emit)
    }
}
