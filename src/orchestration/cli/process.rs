use std::io::Write;
use std::sync::{Arc, Mutex};

use crate::orchestration::errors::Error;

pub const DEFAULT_STDERR_LIMIT: usize = 32 * 1024;

#[derive(Debug, Clone)]
pub struct ProcessError {
    pub binary: String,
    pub args: Vec<String>,
    pub cause: String,
    pub stderr: String,
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.stderr.trim().is_empty() {
            write!(f, "provider process {}: {}", self.binary, self.cause)
        } else {
            write!(
                f,
                "provider process {}: {}: {}",
                self.binary,
                self.cause,
                self.stderr.trim()
            )
        }
    }
}

impl ProcessError {
    pub fn into_error(self) -> Error {
        Error::provider(self.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct BoundedStderr {
    inner: Arc<Mutex<(Vec<u8>, usize)>>,
}

impl BoundedStderr {
    pub fn new(limit: usize) -> Self {
        let cap = if limit == 0 {
            DEFAULT_STDERR_LIMIT
        } else {
            limit
        };
        Self {
            inner: Arc::new(Mutex::new((Vec::new(), cap))),
        }
    }

    pub fn write_bytes(&self, p: &[u8]) {
        if let Ok(mut guard) = self.inner.lock() {
            let (ref mut buf, max) = *guard;
            let max = if max == 0 { DEFAULT_STDERR_LIMIT } else { max };
            let remaining = max.saturating_sub(buf.len());
            if remaining > 0 {
                let take = remaining.min(p.len());
                buf.extend_from_slice(&p[..take]);
            }
        }
    }

    pub fn as_string(&self) -> String {
        if let Ok(guard) = self.inner.lock() {
            String::from_utf8_lossy(&guard.0).to_string()
        } else {
            String::new()
        }
    }
}

impl Write for BoundedStderr {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_bytes(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
