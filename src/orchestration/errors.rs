use std::fmt;

use crate::orchestration::command::Command;
use crate::orchestration::ids::{CommandID, ThreadID};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    ThreadNotFound,
    ThreadAlreadyExists,
    CommandRejected,
    CommandPreviouslyRejected,
    CommandIDConflict,
    SequenceConflict,
    InvalidCommand,
    InvalidProviderEvent,
    RunNotInterruptible,
    RequestNotRespondable,
    StableSourceNotFound,
    UnsupportedCapability,
    StorageError,
    ProviderError,
}

impl ErrorKind {
    pub fn message(self) -> &'static str {
        match self {
            Self::ThreadNotFound => "orchestration thread not found",
            Self::ThreadAlreadyExists => "orchestration thread already exists",
            Self::CommandRejected => "orchestration command rejected",
            Self::CommandPreviouslyRejected => "orchestration command previously rejected",
            Self::CommandIDConflict => "orchestration command ID conflict",
            Self::SequenceConflict => "orchestration sequence conflict",
            Self::InvalidCommand => "invalid orchestration command",
            Self::InvalidProviderEvent => "invalid provider event",
            Self::RunNotInterruptible => "run is not interruptible",
            Self::RequestNotRespondable => "runtime request is not respondable",
            Self::StableSourceNotFound => "no stable fork source found",
            Self::UnsupportedCapability => "provider capability is unavailable",
            Self::StorageError => "orchestration storage error",
            Self::ProviderError => "provider error",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TurnErrorInfo {
    pub class: String,
    pub message: String,
    pub code: String,
    pub reset_at: String,
}

/// Error adds command and aggregate context while preserving an `ErrorKind`
/// sentinel for callers (`err.is(ErrorKind::...)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub op: String,
    pub command_id: CommandID,
    pub thread_id: ThreadID,
    pub kind: ErrorKind,
    pub detail: String,
    pub cause_kinds: Vec<ErrorKind>,
    pub turn_error: Option<TurnErrorInfo>,
}

impl Error {
    pub fn sentinel(kind: ErrorKind) -> Self {
        Self {
            op: String::new(),
            command_id: String::new(),
            thread_id: String::new(),
            kind,
            detail: String::new(),
            cause_kinds: vec![kind],
            turn_error: None,
        }
    }

    pub fn with_detail(kind: ErrorKind, detail: impl Into<String>) -> Self {
        Self {
            op: String::new(),
            command_id: String::new(),
            thread_id: String::new(),
            kind,
            detail: detail.into(),
            cause_kinds: vec![kind],
            turn_error: None,
        }
    }

    pub fn wrap(kind: ErrorKind, cause: &Error, detail: impl Into<String>) -> Self {
        let mut cause_kinds = vec![kind];
        for &k in &cause.cause_kinds {
            if !cause_kinds.contains(&k) {
                cause_kinds.push(k);
            }
        }
        Self {
            op: String::new(),
            command_id: String::new(),
            thread_id: String::new(),
            kind,
            detail: detail.into(),
            cause_kinds,
            turn_error: cause.turn_error.clone(),
        }
    }

    pub fn is(&self, target: ErrorKind) -> bool {
        self.kind == target || self.cause_kinds.contains(&target)
    }

    pub fn is_command_error(&self) -> bool {
        !self.op.is_empty()
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::with_detail(ErrorKind::StorageError, detail)
    }

    pub fn provider(detail: impl Into<String>) -> Self {
        Self::with_detail(ErrorKind::ProviderError, detail)
    }

    pub fn turn(
        class: impl Into<String>,
        message: impl Into<String>,
        code: impl Into<String>,
        reset_at: impl Into<String>,
    ) -> Self {
        let class_str = class.into();
        let message_str = message.into();
        let detail = if message_str.is_empty() {
            "provider turn failed".to_string()
        } else {
            message_str.clone()
        };
        Self {
            op: String::new(),
            command_id: String::new(),
            thread_id: String::new(),
            kind: ErrorKind::ProviderError,
            detail,
            cause_kinds: vec![ErrorKind::ProviderError],
            turn_error: Some(TurnErrorInfo {
                class: class_str,
                message: message_str,
                code: code.into(),
                reset_at: reset_at.into(),
            }),
        }
    }

    pub fn as_turn_error(&self) -> Option<&TurnErrorInfo> {
        self.turn_error.as_ref()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self::sentinel(kind)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.op.is_empty() {
            if self.detail.is_empty() {
                return f.write_str(self.kind.message());
            }
            if self.kind == ErrorKind::ProviderError {
                return f.write_str(&self.detail);
            }
            return write!(f, "{}: {}", self.detail, self.kind.message());
        }
        let mut message = self.op.clone();
        if !self.command_id.is_empty() {
            message.push_str(" command ");
            message.push_str(&self.command_id);
        }
        if !self.thread_id.is_empty() {
            message.push_str(" thread ");
            message.push_str(&self.thread_id);
        }
        if !self.detail.is_empty() {
            message.push_str(": ");
            message.push_str(&self.detail);
        } else {
            message.push_str(": ");
            message.push_str(self.kind.message());
        }
        f.write_str(&message)
    }
}

impl std::error::Error for Error {}

pub fn command_error(command: &Command, kind: ErrorKind, detail: impl Into<String>) -> Error {
    Error {
        op: format!("dispatch {}", command.command_type.as_str()),
        command_id: command.command_id.clone(),
        thread_id: command.thread_id.clone(),
        kind,
        detail: detail.into(),
        cause_kinds: vec![kind],
        turn_error: None,
    }
}

pub fn command_error_from_cause(
    command: &Command,
    kind: ErrorKind,
    cause: &Error,
    detail: impl Into<String>,
) -> Error {
    let mut cause_kinds = vec![kind];
    for &k in &cause.cause_kinds {
        if !cause_kinds.contains(&k) {
            cause_kinds.push(k);
        }
    }
    Error {
        op: format!("dispatch {}", command.command_type.as_str()),
        command_id: command.command_id.clone(),
        thread_id: command.thread_id.clone(),
        kind,
        detail: detail.into(),
        cause_kinds,
        turn_error: cause.turn_error.clone(),
    }
}

pub fn command_error_from(command: &Command, cause: Error, detail: impl Into<String>) -> Error {
    let detail_str = detail.into();
    let full_detail = if cause.detail.is_empty() {
        format!("{detail_str}: {}", cause.kind.message())
    } else {
        format!("{detail_str}: {}", cause)
    };
    command_error_from_cause(command, cause.kind, &cause, full_detail)
}

pub fn require_id(name: &str, value: &str) -> Result<(), Error> {
    if value.is_empty() {
        return Err(Error::with_detail(
            ErrorKind::InvalidCommand,
            format!("{name} is required"),
        ));
    }
    Ok(())
}
