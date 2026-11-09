//! Rust side of the t3code control-plane protocol v1.
//!
//! The normative wire contract lives in the shared control-plane repository
//! (`t3code-control-plane/protocol/PROTOCOL.md`). This module provides:
//!
//! - protocol constants, scopes, and the MCP tool → scope table ([`Scope`], [`scope_for_tool`],
//!   [`scope_for_mcp_method`]);
//! - pairing-code helpers, key fingerprints, the pairing-exchange message, and the Ed25519
//!   device identity ([`crypto`]);
//! - the device-side connect-token verifier ([`verify_connect_token`]) that mirrors the
//!   TypeScript reference verifier step by step;
//! - a JWKS cache with rate-limited refetch on unknown `kid` ([`JwksCache`]);
//! - a thread-safe request authorizer for the device API ([`DeviceAuthorizer`]);
//! - a blocking HTTP client for the device and GUI flows ([`Client`]);
//! - the persisted device state file ([`DeviceState`]).
//!
//! The device daemon (heartbeat loop + HTTP/WebSocket listener) is not wired up yet; these are
//! the building blocks it will use.

pub mod authorizer;
pub mod client;
pub mod crypto;
pub mod jwks;
pub mod state;
pub mod token;
pub mod types;

#[cfg(test)]
mod client_tests;
#[cfg(test)]
mod tests;

pub use authorizer::{AuthError, AuthorizerState, DeviceAuthorizer};
pub use client::{Client, ClientError, Endpoints, PairingStatus, PollStep};
pub use crypto::{
    DeviceIdentity, format_user_code, normalize_user_code, pairing_exchange_message,
    public_key_fingerprint,
};
pub use jwks::{HttpJwksFetcher, JwksCache, JwksFetcher};
pub use state::DeviceState;
pub use token::{
    ConnectClaims, Jwk, Jwks, VerificationContext, VerifyError, VerifyErrorCode,
    verify_connect_token, verify_connect_token_any_scope,
};
pub use types::*;

use serde::{Deserialize, Serialize};
use std::fmt;

/// Protocol version carried in discovery (`protocolVersion`) and connect tokens (`t3cp_ver`).
pub const PROTOCOL_VERSION: i64 = 1;
/// Path of the discovery document relative to the control-plane base URL.
pub const DISCOVERY_PATH: &str = "/.well-known/t3code-control-plane";
/// Domain-separation prefix of the pairing-exchange signature message.
pub const PAIRING_EXCHANGE_CONTEXT: &str = "t3cp-pairing-exchange-v1";
/// Pairing / user code alphabet (no vowels, no 0/O/1/I).
pub const CODE_ALPHABET: &str = "BCDFGHJKLMNPQRSTVWXZ";
/// Number of characters in a normalized pairing / user code.
pub const CODE_LENGTH: usize = 8;
/// Prefix of device credentials (`t3cpd_…`).
pub const DEVICE_CREDENTIAL_PREFIX: &str = "t3cpd_";
/// Prefix of GUI client tokens (`t3cpc_…`).
pub const CLIENT_TOKEN_PREFIX: &str = "t3cpc_";
/// Default connect-token lifetime issued by the control plane.
pub const CONNECT_TOKEN_TTL_SECONDS: i64 = 300;
/// Default heartbeat interval.
pub const HEARTBEAT_INTERVAL_SECONDS: u64 = 60;
/// Default polling interval for pairing exchange / client token polling.
pub const POLL_INTERVAL_SECONDS: u64 = 5;
/// Seconds added to the polling interval on `slow_down`.
pub const SLOW_DOWN_INCREMENT_SECONDS: u64 = 5;
/// Clock leeway applied to `exp` / `nbf` during connect-token verification.
pub const LEEWAY_SECONDS: i64 = 30;
/// Maximum accepted connect-token lifetime (`exp − iat`).
pub const MAX_LIFETIME_SECONDS: i64 = 900;
/// Minimum delay between JWKS refetches triggered by an unknown `kid`.
pub const JWKS_REFETCH_INTERVAL_SECONDS: u64 = 60;

/// Orchestration scopes carried by client tokens and connect tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Scope {
    #[serde(rename = "orchestration:read")]
    Read,
    #[serde(rename = "orchestration:write")]
    Write,
}

impl Scope {
    pub const ALL: [Scope; 2] = [Scope::Read, Scope::Write];

    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Read => "orchestration:read",
            Scope::Write => "orchestration:write",
        }
    }

    pub fn parse(value: &str) -> Option<Scope> {
        match value {
            "orchestration:read" => Some(Scope::Read),
            "orchestration:write" => Some(Scope::Write),
            _ => None,
        }
    }

    /// Whether a token carrying `scopes` may perform an operation requiring `self`
    /// (step 10: read needs read or write; write needs write).
    pub fn is_satisfied_by<S: AsRef<str>>(self, scopes: &[S]) -> bool {
        let has = |s: Scope| scopes.iter().any(|v| v.as_ref() == s.as_str());
        match self {
            Scope::Read => has(Scope::Read) || has(Scope::Write),
            Scope::Write => has(Scope::Write),
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// MCP tools that only need `orchestration:read`.
pub const READ_TOOLS: [&str; 5] = [
    "orchestrator_capabilities",
    "t3_thread_list",
    "t3_thread_read",
    "t3_thread_wait",
    "task_status",
];

/// MCP tools that need `orchestration:write`.
pub const WRITE_TOOLS: [&str; 6] = [
    "create_threads",
    "delegate_task",
    "task_cancel",
    "t3_thread_send",
    "t3_thread_interrupt",
    "t3_pending_request",
];

/// Scope required to call the MCP tool `name`. Unknown tools default to `orchestration:write`.
pub fn scope_for_tool(name: &str) -> Scope {
    if READ_TOOLS.contains(&name) {
        Scope::Read
    } else {
        Scope::Write
    }
}

/// Scope required for an MCP JSON-RPC method.
///
/// Returns `None` when the method needs only a valid token (`initialize`, `tools/list`, `ping`,
/// and client notifications such as `notifications/initialized`, which carry no operation).
/// `tools/call` maps through [`scope_for_tool`] using `tool_name` (missing name → write).
/// Every other method defaults to `orchestration:write`.
pub fn scope_for_mcp_method(method: &str, tool_name: Option<&str>) -> Option<Scope> {
    match method {
        "initialize" | "tools/list" | "ping" => None,
        m if m.starts_with("notifications/") => None,
        "tools/call" => Some(tool_name.map_or(Scope::Write, scope_for_tool)),
        _ => Some(Scope::Write),
    }
}
