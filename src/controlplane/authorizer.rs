//! Thread-safe connect-token authorizer for the device API.

use super::Scope;
use super::jwks::JwksCache;
use super::state::DeviceState;
use super::token::{
    ConnectClaims, VerificationContext, VerifyError, VerifyErrorCode, peek_kid,
    verify_connect_token, verify_connect_token_any_scope,
};
use super::types::HeartbeatResponse;
use std::fmt;
use std::sync::RwLock;

/// Mutable verification state, updated from heartbeat responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizerState {
    pub issuer: String,
    pub device_id: String,
    pub revocation_epoch: i64,
    pub revoked: bool,
}

/// A rejected request: HTTP status plus the protocol error body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthError {
    /// `401` for verification failures (steps 1–9), `403` for `insufficient_scope`.
    pub status: u16,
    pub error: VerifyError,
}

impl AuthError {
    pub fn code(&self) -> VerifyErrorCode {
        self.error.code
    }

    /// `{"error": "<code>", "message": "<reason>"}`.
    pub fn body(&self) -> serde_json::Value {
        serde_json::json!({ "error": self.error.code.as_str(), "message": self.error.reason })
    }

    pub fn body_string(&self) -> String {
        self.body().to_string()
    }
}

impl From<VerifyError> for AuthError {
    fn from(error: VerifyError) -> Self {
        Self {
            status: error.code.http_status(),
            error,
        }
    }
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.status, self.error)
    }
}

impl std::error::Error for AuthError {}

type Clock = Box<dyn Fn() -> i64 + Send + Sync>;

/// Verifies `Authorization: Bearer <JWT>` headers against the device's pairing state.
pub struct DeviceAuthorizer {
    state: RwLock<AuthorizerState>,
    jwks: JwksCache,
    clock: Clock,
}

impl DeviceAuthorizer {
    pub fn new(
        issuer: impl Into<String>,
        device_id: impl Into<String>,
        revocation_epoch: i64,
        jwks: JwksCache,
    ) -> Self {
        Self {
            state: RwLock::new(AuthorizerState {
                issuer: issuer.into(),
                device_id: device_id.into(),
                revocation_epoch,
                revoked: false,
            }),
            jwks,
            clock: Box::new(unix_now),
        }
    }

    pub fn from_device_state(state: &DeviceState, jwks: JwksCache) -> Self {
        Self::new(
            state.issuer.clone(),
            state.device_id.clone(),
            state.revocation_epoch,
            jwks,
        )
    }

    /// Overrides the clock (Unix seconds). Intended for tests.
    pub fn with_clock(mut self, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.clock = Box::new(clock);
        self
    }

    pub fn jwks(&self) -> &JwksCache {
        &self.jwks
    }

    pub fn state(&self) -> AuthorizerState {
        self.state.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Applies a successful heartbeat: keeps `max(stored, returned)` revocation epoch, follows
    /// `jwksUri` changes, and marks the device revoked if `status` is `revoked`. The issuer stays
    /// pinned from pairing and is never updated from a heartbeat.
    pub fn apply_heartbeat(&self, heartbeat: &HeartbeatResponse) {
        {
            let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
            state.revocation_epoch = state.revocation_epoch.max(heartbeat.revocation_epoch);
            if heartbeat.status == "revoked" {
                state.revoked = true;
            }
        }
        self.jwks.set_uri(&heartbeat.jwks_uri);
    }

    /// Call after heartbeat returned `403 device_revoked`: every token is rejected from now on.
    pub fn mark_revoked(&self) {
        self.state
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .revoked = true;
    }

    /// Verifies a raw connect token. `required == None` runs steps 1–9 only.
    pub fn verify_token(
        &self,
        token: &str,
        required: Option<Scope>,
    ) -> Result<ConnectClaims, VerifyError> {
        if let Some(kid) = peek_kid(token) {
            self.jwks.ensure_kid(&kid);
        }
        let jwks = self.jwks.snapshot();
        let state = self.state();
        let ctx = VerificationContext {
            issuer: &state.issuer,
            device_id: &state.device_id,
            revocation_epoch: state.revocation_epoch,
            revoked: state.revoked,
            jwks: &jwks,
            now: (self.clock)(),
        };
        match required {
            Some(scope) => verify_connect_token(token, &ctx, scope),
            None => verify_connect_token_any_scope(token, &ctx),
        }
    }

    /// Authorizes a request from its `Authorization` header value.
    pub fn authorize(
        &self,
        authorization_header: Option<&str>,
        required: Scope,
    ) -> Result<ConnectClaims, AuthError> {
        self.authorize_optional_scope(authorization_header, Some(required))
    }

    /// Like [`authorize`](Self::authorize), where `required == None` means only a valid token is
    /// needed (see [`super::scope_for_mcp_method`]).
    pub fn authorize_optional_scope(
        &self,
        authorization_header: Option<&str>,
        required: Option<Scope>,
    ) -> Result<ConnectClaims, AuthError> {
        let token = bearer_token(authorization_header).ok_or_else(|| {
            AuthError::from(VerifyError::new(
                VerifyErrorCode::InvalidToken,
                "missing bearer token",
            ))
        })?;
        self.verify_token(token, required).map_err(AuthError::from)
    }
}

/// Extracts the token from `Bearer <token>` (scheme is case-insensitive).
pub fn bearer_token(header: Option<&str>) -> Option<&str> {
    let (scheme, token) = header?.trim().split_once(' ')?;
    let token = token.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() && !token.contains(' '))
        .then_some(token)
}

fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}
