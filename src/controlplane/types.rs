//! Wire types for the control-plane HTTP API. Field names follow PROTOCOL.md exactly (camelCase);
//! unknown fields are ignored. Secret-bearing types redact their secrets in `Debug`.

use super::Scope;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Protocol error body `{"error": "<code>", "message": "<text>"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
    #[serde(default)]
    pub message: String,
}

/// `GET /.well-known/t3code-control-plane`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Discovery {
    pub protocol_version: i64,
    pub issuer: String,
    pub jwks_uri: String,
    pub device_pairing_endpoint: String,
    pub client_authorization_endpoint: String,
    pub client_token_endpoint: String,
    pub devices_endpoint: String,
    #[serde(default)]
    pub scopes_supported: Vec<String>,
    #[serde(default)]
    pub connect_token_ttl_seconds: Option<u64>,
    #[serde(default)]
    pub heartbeat_interval_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientInfo {
    pub kind: String,
    pub version: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailscaleInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub magic_dns_name: Option<String>,
}

/// `POST /v1/device-pairings` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartPairingRequest {
    /// base64url raw 32-byte Ed25519 public key.
    pub public_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub hostname: String,
    /// `darwin` or `linux`.
    pub platform: String,
    pub arch: String,
    pub client: ClientInfo,
    pub tailscale: TailscaleInfo,
    pub port: u16,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

/// `POST /v1/device-pairings` response (`201`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartPairingResponse {
    pub pairing_id: String,
    pub code: String,
    pub verification_uri: String,
    pub verification_uri_complete: String,
    pub expires_at: String,
    pub interval: u64,
}

/// `POST /v1/device-pairings/:code/exchange` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangePairingRequest {
    pub timestamp: String,
    pub signature: String,
}

/// `POST /v1/device-pairings/:code/exchange` success body.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairingCredentials {
    pub device_id: String,
    pub device_credential: String,
    pub owner_user_id: String,
    pub issuer: String,
    pub jwks_uri: String,
    pub revocation_epoch: i64,
    pub heartbeat_interval_seconds: u64,
}

impl fmt::Debug for PairingCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PairingCredentials")
            .field("device_id", &self.device_id)
            .field("device_credential", &"<redacted>")
            .field("owner_user_id", &self.owner_user_id)
            .field("issuer", &self.issuer)
            .field("jwks_uri", &self.jwks_uri)
            .field("revocation_epoch", &self.revocation_epoch)
            .field(
                "heartbeat_interval_seconds",
                &self.heartbeat_interval_seconds,
            )
            .finish()
    }
}

/// `POST /v1/devices/:deviceId/heartbeat` request. All fields are optional.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tailscale: Option<TailscaleInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<ClientInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<String>>,
}

/// `POST /v1/devices/:deviceId/heartbeat` response (`200`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    pub device_id: String,
    pub status: String,
    pub revocation_epoch: i64,
    pub issuer: String,
    pub jwks_uri: String,
    pub heartbeat_interval_seconds: u64,
    pub server_time: String,
}

/// `POST /v1/client-authorizations` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartClientAuthorizationRequest {
    pub client_name: String,
    pub client: ClientInfo,
    /// Defaults to both scopes on the server when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<Scope>>,
}

/// `POST /v1/client-authorizations` response (`201`).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartClientAuthorizationResponse {
    pub authorization_id: String,
    /// Secret; keep it out of logs.
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: String,
    pub expires_at: String,
    pub interval: u64,
}

impl fmt::Debug for StartClientAuthorizationResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StartClientAuthorizationResponse")
            .field("authorization_id", &self.authorization_id)
            .field("device_code", &"<redacted>")
            .field("user_code", &self.user_code)
            .field("verification_uri", &self.verification_uri)
            .field("verification_uri_complete", &self.verification_uri_complete)
            .field("expires_at", &self.expires_at)
            .field("interval", &self.interval)
            .finish()
    }
}

/// `POST /v1/client-authorizations/token` request.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientTokenRequest {
    pub device_code: String,
}

/// `POST /v1/client-authorizations/token` success body.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientTokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub scope: String,
    pub user_id: String,
    pub expires_at: String,
}

impl ClientTokenResponse {
    pub fn scopes(&self) -> Vec<Scope> {
        self.scope.split(' ').filter_map(Scope::parse).collect()
    }
}

impl fmt::Debug for ClientTokenResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientTokenResponse")
            .field("access_token", &"<redacted>")
            .field("token_type", &self.token_type)
            .field("scope", &self.scope)
            .field("user_id", &self.user_id)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub avatar_url: Option<String>,
}

/// `GET /v1/me`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeResponse {
    pub user: User,
    #[serde(default)]
    pub scope: String,
}

/// A device as returned by `GET /v1/devices` and `POST /v1/devices/:id/revoke`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub platform: Option<String>,
    #[serde(default)]
    pub arch: Option<String>,
    #[serde(default)]
    pub client: Option<ClientInfo>,
    #[serde(default)]
    pub tailscale: Option<TailscaleInfo>,
    #[serde(default)]
    pub port: Option<u16>,
    /// Absent (or `null`) for revoked devices.
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub public_key_fingerprint: Option<String>,
    pub status: String,
    #[serde(default)]
    pub online: bool,
    #[serde(default)]
    pub revocation_epoch: i64,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub last_seen_at: Option<String>,
}

/// `GET /v1/devices`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceList {
    pub devices: Vec<Device>,
}

/// `POST /v1/devices/:deviceId/connect-tokens` request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectTokenRequest {
    /// Defaults to `["orchestration:read"]` on the server when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<Scope>>,
}

/// `POST /v1/devices/:deviceId/connect-tokens` response (`201`).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectTokenResponse {
    pub token: String,
    pub token_type: String,
    pub scope: String,
    pub expires_in: u64,
    pub expires_at: String,
    pub device_id: String,
    #[serde(default)]
    pub endpoint: Option<String>,
}

impl fmt::Debug for ConnectTokenResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectTokenResponse")
            .field("token", &"<redacted>")
            .field("token_type", &self.token_type)
            .field("scope", &self.scope)
            .field("expires_in", &self.expires_in)
            .field("expires_at", &self.expires_at)
            .field("device_id", &self.device_id)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}
