//! Device-side connect-token verification (PROTOCOL.md "Device-side verification (normative)").
//!
//! This mirrors `verifyConnectToken` in the control plane's `src/lib/tokens.ts` step by step and
//! deliberately avoids JWT libraries.

use super::crypto::{b64url_decode, is_b64url_byte};
use super::{LEEWAY_SECONDS, MAX_LIFETIME_SECONDS, PROTOCOL_VERSION, Scope};
use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt;

/// One JWKS entry. All fields are optional so that malformed or foreign keys are skipped during
/// lookup instead of failing the whole set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwk {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kty: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crv: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub x: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alg: Option<String>,
    #[serde(rename = "use", default, skip_serializing_if = "Option::is_none")]
    pub use_: Option<String>,
}

/// `GET /.well-known/jwks.json` document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwks {
    #[serde(default)]
    pub keys: Vec<Jwk>,
}

impl Jwks {
    /// Step 2: the `OKP`/`Ed25519` key with this `kid` that has an `x` value.
    pub fn find(&self, kid: &str) -> Option<&Jwk> {
        self.keys
            .iter()
            .find(|k| {
                k.kid.as_deref() == Some(kid)
                    && k.kty.as_deref() == Some("OKP")
                    && k.crv.as_deref() == Some("Ed25519")
            })
            .filter(|k| k.x.as_deref().is_some_and(|x| !x.is_empty()))
    }

    pub fn contains_kid(&self, kid: &str) -> bool {
        self.find(kid).is_some()
    }
}

/// Device-side verification inputs.
#[derive(Debug, Clone, Copy)]
pub struct VerificationContext<'a> {
    /// Issuer stored at pairing (exact string comparison).
    pub issuer: &'a str,
    /// The device's own ID (`dev_…`).
    pub device_id: &'a str,
    /// Stored revocation epoch; tokens with a lower `t3cp_epoch` are rejected.
    pub revocation_epoch: i64,
    /// Set once heartbeat answered `403 device_revoked`.
    pub revoked: bool,
    pub jwks: &'a Jwks,
    /// Verification time in Unix seconds.
    pub now: i64,
}

/// Error codes returned by the device API (PROTOCOL.md "Suggested codes").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VerifyErrorCode {
    InvalidToken,
    TokenExpired,
    WrongAudience,
    StaleEpoch,
    DeviceRevoked,
    InsufficientScope,
}

impl VerifyErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            VerifyErrorCode::InvalidToken => "invalid_token",
            VerifyErrorCode::TokenExpired => "token_expired",
            VerifyErrorCode::WrongAudience => "wrong_audience",
            VerifyErrorCode::StaleEpoch => "stale_epoch",
            VerifyErrorCode::DeviceRevoked => "device_revoked",
            VerifyErrorCode::InsufficientScope => "insufficient_scope",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "invalid_token" => VerifyErrorCode::InvalidToken,
            "token_expired" => VerifyErrorCode::TokenExpired,
            "wrong_audience" => VerifyErrorCode::WrongAudience,
            "stale_epoch" => VerifyErrorCode::StaleEpoch,
            "device_revoked" => VerifyErrorCode::DeviceRevoked,
            "insufficient_scope" => VerifyErrorCode::InsufficientScope,
            _ => return None,
        })
    }

    /// HTTP status for the device API: `403` for step 10, `401` for steps 1–9.
    pub fn http_status(self) -> u16 {
        match self {
            VerifyErrorCode::InsufficientScope => 403,
            _ => 401,
        }
    }
}

impl fmt::Display for VerifyErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyError {
    pub code: VerifyErrorCode,
    pub reason: String,
}

impl VerifyError {
    pub fn new(code: VerifyErrorCode, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.reason)
    }
}

impl std::error::Error for VerifyError {}

/// Claims of a verified connect token.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectClaims {
    pub iss: String,
    /// User ID (`usr_…`). The normative steps don't require it, so it is optional.
    pub sub: Option<String>,
    /// The audience device ID (normalized from string or single-element array).
    pub aud: String,
    pub iat: i64,
    pub nbf: i64,
    pub exp: i64,
    /// Token ID (`ctk_…`), optional for the same reason as `sub`.
    pub jti: Option<String>,
    /// Space-separated `scope` split on single spaces, empty entries dropped, order preserved.
    pub scopes: Vec<String>,
    pub epoch: i64,
    pub version: i64,
    /// The `kid` header the token was verified with.
    pub kid: String,
    /// Full decoded payload, including unknown claims.
    pub raw: Map<String, Value>,
}

impl ConnectClaims {
    pub fn allows(&self, required: Scope) -> bool {
        required.is_satisfied_by(&self.scopes)
    }
}

fn fail<T>(code: VerifyErrorCode, reason: impl Into<String>) -> Result<T, VerifyError> {
    Err(VerifyError::new(code, reason))
}

/// Decodes a base64url JSON-object segment.
pub(crate) fn decode_segment(segment: &str) -> Option<Map<String, Value>> {
    let bytes = b64url_decode(segment)?;
    match serde_json::from_slice::<Value>(&bytes).ok()? {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

/// Returns the `kid` header of a compact JWS without verifying anything (used to decide whether
/// the JWKS cache needs a refetch).
pub fn peek_kid(token: &str) -> Option<String> {
    let header = token.split('.').next()?;
    decode_segment(header)?
        .get("kid")?
        .as_str()
        .map(str::to_string)
}

/// Largest integer exactly representable in an IEEE-754 double (`Number.MAX_SAFE_INTEGER`).
const MAX_SAFE_INTEGER: i64 = (1 << 53) - 1;

/// An integral JSON number within ±(2^53−1). Like `Number.isInteger` in the reference verifier,
/// integral values written with a fraction or exponent (`300.0`, `3e2`) are accepted; strings,
/// non-integral numbers, and out-of-range values are not.
fn int_claim(claims: &Map<String, Value>, name: &str) -> Option<i64> {
    let Value::Number(n) = claims.get(name)? else {
        return None;
    };
    let value = if let Some(i) = n.as_i64() {
        i
    } else if n.is_u64() {
        return None;
    } else {
        let f = n.as_f64()?;
        if !f.is_finite() || f.fract() != 0.0 || f.abs() > MAX_SAFE_INTEGER as f64 {
            return None;
        }
        f as i64
    };
    (value.abs() <= MAX_SAFE_INTEGER).then_some(value)
}

/// Verifies a connect token following the 10 normative steps.
pub fn verify_connect_token(
    token: &str,
    ctx: &VerificationContext<'_>,
    required: Scope,
) -> Result<ConnectClaims, VerifyError> {
    verify(token, ctx, Some(required))
}

/// Runs steps 1–9 only, for operations that need just a valid token (MCP `initialize`,
/// `tools/list`, `ping`).
pub fn verify_connect_token_any_scope(
    token: &str,
    ctx: &VerificationContext<'_>,
) -> Result<ConnectClaims, VerifyError> {
    verify(token, ctx, None)
}

fn verify(
    token: &str,
    ctx: &VerificationContext<'_>,
    required: Option<Scope>,
) -> Result<ConnectClaims, VerifyError> {
    use VerifyErrorCode::*;

    // 1. Structure and header.
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return fail(InvalidToken, "malformed token");
    }
    let (h, p, s) = (parts[0], parts[1], parts[2]);
    let Some(header) = decode_segment(h) else {
        return fail(InvalidToken, "malformed header");
    };
    if header.get("alg").and_then(Value::as_str) != Some("EdDSA") {
        return fail(InvalidToken, "unsupported alg");
    }
    if let Some(typ) = header.get("typ")
        && typ.as_str() != Some("JWT")
    {
        return fail(InvalidToken, "unsupported typ");
    }
    let Some(kid) = header.get("kid").and_then(Value::as_str) else {
        return fail(InvalidToken, "missing kid");
    };

    // 2. Key lookup.
    let Some(x) = ctx.jwks.find(kid).and_then(|k| k.x.as_deref()) else {
        return fail(InvalidToken, "unknown kid");
    };

    // 3. Signature over the ASCII bytes `<header>.<payload>`.
    if s.is_empty() || !s.bytes().all(is_b64url_byte) {
        return fail(InvalidToken, "malformed signature");
    }
    let valid = (|| {
        let sig: [u8; 64] = b64url_decode(s)?.try_into().ok()?;
        let key: [u8; 32] = b64url_decode(x)?.try_into().ok()?;
        let key = VerifyingKey::from_bytes(&key).ok()?;
        let signing_input = format!("{h}.{p}");
        key.verify_strict(signing_input.as_bytes(), &Signature::from_bytes(&sig))
            .ok()
    })()
    .is_some();
    if !valid {
        return fail(InvalidToken, "bad signature");
    }

    let Some(claims) = decode_segment(p) else {
        return fail(InvalidToken, "malformed payload");
    };

    // 4. Issuer.
    let iss = match claims.get("iss").and_then(Value::as_str) {
        Some(iss) if iss == ctx.issuer => iss.to_string(),
        _ => return fail(InvalidToken, "wrong issuer"),
    };

    // 5. Audience.
    let aud_ok = match claims.get("aud") {
        Some(Value::String(aud)) => aud == ctx.device_id,
        Some(Value::Array(items)) => items.len() == 1 && items[0].as_str() == Some(ctx.device_id),
        _ => false,
    };
    if !aud_ok {
        return fail(WrongAudience, "token is for another device");
    }

    // 6. Time.
    let (Some(exp), Some(nbf), Some(iat)) = (
        int_claim(&claims, "exp"),
        int_claim(&claims, "nbf"),
        int_claim(&claims, "iat"),
    ) else {
        return fail(InvalidToken, "missing time claims");
    };
    if i128::from(exp) <= i128::from(ctx.now) - i128::from(LEEWAY_SECONDS) {
        return fail(TokenExpired, "token expired");
    }
    if i128::from(nbf) > i128::from(ctx.now) + i128::from(LEEWAY_SECONDS) {
        return fail(InvalidToken, "token not yet valid");
    }
    if i128::from(exp) - i128::from(iat) > i128::from(MAX_LIFETIME_SECONDS) {
        return fail(InvalidToken, "token lifetime too long");
    }

    // 7. Version.
    let version = int_claim(&claims, "t3cp_ver");
    if version != Some(PROTOCOL_VERSION) {
        return fail(InvalidToken, "unsupported protocol version");
    }

    // 8. Revocation epoch.
    let Some(epoch) = int_claim(&claims, "t3cp_epoch") else {
        return fail(InvalidToken, "missing epoch");
    };
    if epoch < ctx.revocation_epoch {
        return fail(StaleEpoch, "token predates revocation");
    }

    // 9. Device revocation.
    if ctx.revoked {
        return fail(DeviceRevoked, "device revoked");
    }

    // 10. Scope.
    let scopes: Vec<String> = claims
        .get("scope")
        .and_then(Value::as_str)
        .map(|s| {
            s.split(' ')
                .filter(|v| !v.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    if let Some(required) = required
        && !required.is_satisfied_by(&scopes)
    {
        return fail(InsufficientScope, format!("requires {required}"));
    }

    Ok(ConnectClaims {
        iss,
        sub: claims
            .get("sub")
            .and_then(Value::as_str)
            .map(str::to_string),
        aud: ctx.device_id.to_string(),
        iat,
        nbf,
        exp,
        jti: claims
            .get("jti")
            .and_then(Value::as_str)
            .map(str::to_string),
        scopes,
        epoch,
        version: PROTOCOL_VERSION,
        kid: kid.to_string(),
        raw: claims,
    })
}
