//! Blocking HTTP client for the control-plane API (device daemon and GUI flows).

use super::crypto::{DeviceIdentity, normalize_user_code, rfc3339_millis_now};
use super::types::*;
use super::{
    DISCOVERY_PATH, POLL_INTERVAL_SECONDS, PROTOCOL_VERSION, SLOW_DOWN_INCREMENT_SECONDS, Scope,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Resolved endpoint URLs. Built from the base URL with the spec's default paths, then replaced by
/// the discovery document when available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    pub base_url: String,
    pub issuer: String,
    pub jwks_uri: String,
    pub device_pairing: String,
    pub client_authorization: String,
    pub client_token: String,
    pub devices: String,
    pub me: String,
    pub client_token_revoke: String,
}

impl Endpoints {
    pub fn from_base(base_url: &str) -> Self {
        let base = base_url.trim_end_matches('/').to_string();
        Self {
            issuer: base.clone(),
            jwks_uri: format!("{base}/.well-known/jwks.json"),
            device_pairing: format!("{base}/v1/device-pairings"),
            client_authorization: format!("{base}/v1/client-authorizations"),
            client_token: format!("{base}/v1/client-authorizations/token"),
            devices: format!("{base}/v1/devices"),
            me: format!("{base}/v1/me"),
            client_token_revoke: format!("{base}/v1/client-tokens/revoke"),
            base_url: base,
        }
    }

    pub fn from_discovery(base_url: &str, discovery: &Discovery) -> Self {
        Self {
            issuer: discovery.issuer.clone(),
            jwks_uri: discovery.jwks_uri.clone(),
            device_pairing: discovery.device_pairing_endpoint.clone(),
            client_authorization: discovery.client_authorization_endpoint.clone(),
            client_token: discovery.client_token_endpoint.clone(),
            devices: discovery.devices_endpoint.clone(),
            ..Self::from_base(base_url)
        }
    }
}

/// One polling attempt of a device-code style flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollStep<T> {
    Ready(T),
    /// `authorization_pending`: keep polling.
    Pending,
    /// `slow_down`: add 5 s to the interval.
    SlowDown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// Network / TLS / I/O failure.
    Transport(String),
    /// Non-2xx response with the protocol `error` code (or `http_<status>` if the body was not a
    /// protocol error).
    Api {
        status: u16,
        error: String,
        message: String,
    },
    /// A 2xx body did not match the expected shape.
    Decode(String),
    /// `access_denied`: the user denied the pairing / authorization. Stop.
    AccessDenied(String),
    /// `expired_token`: the code expired or was already used. Start over.
    ExpiredToken(String),
    /// Heartbeat `403 device_revoked`: stop accepting tokens, delete the credential, re-pair.
    DeviceRevoked(String),
    /// Discovery advertised an unsupported `protocolVersion`.
    UnsupportedProtocol(i64),
    InvalidInput(String),
}

impl ClientError {
    /// The protocol `error` code, if the failure came from the control plane.
    pub fn code(&self) -> Option<&str> {
        match self {
            ClientError::Api { error, .. } => Some(error),
            ClientError::AccessDenied(_) => Some("access_denied"),
            ClientError::ExpiredToken(_) => Some("expired_token"),
            ClientError::DeviceRevoked(_) => Some("device_revoked"),
            _ => None,
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            ClientError::Api { status, .. } => Some(*status),
            ClientError::AccessDenied(_) | ClientError::ExpiredToken(_) => Some(400),
            ClientError::DeviceRevoked(_) => Some(403),
            _ => None,
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Transport(m) => write!(f, "control plane unreachable: {m}"),
            ClientError::Api {
                status,
                error,
                message,
            } => write!(f, "control plane returned {status} {error}: {message}"),
            ClientError::Decode(m) => write!(f, "unexpected control-plane response: {m}"),
            ClientError::AccessDenied(m) => write!(f, "access_denied: {m}"),
            ClientError::ExpiredToken(m) => write!(f, "expired_token: {m}"),
            ClientError::DeviceRevoked(m) => write!(f, "device_revoked: {m}"),
            ClientError::UnsupportedProtocol(v) => {
                write!(f, "unsupported control-plane protocol version {v}")
            }
            ClientError::InvalidInput(m) => write!(f, "invalid input: {m}"),
        }
    }
}

impl std::error::Error for ClientError {}

pub type Result<T> = std::result::Result<T, ClientError>;

type Sleeper = Arc<dyn Fn(Duration) + Send + Sync>;

/// Blocking control-plane client.
#[derive(Clone)]
pub struct Client {
    endpoints: Endpoints,
    agent: ureq::Agent,
    sleeper: Sleeper,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("endpoints", &self.endpoints)
            .finish_non_exhaustive()
    }
}

fn build_agent(timeout: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(timeout))
        .user_agent(concat!("r4code/", env!("CARGO_PKG_VERSION")))
        .build()
        .into()
}

impl Client {
    /// Client for `base_url` using the spec's default endpoint paths. Prefer
    /// [`Client::connect`], which reads the discovery document.
    pub fn new(base_url: &str) -> Self {
        Self {
            endpoints: Endpoints::from_base(base_url),
            agent: build_agent(Duration::from_secs(30)),
            sleeper: Arc::new(std::thread::sleep),
        }
    }

    /// Creates a client and applies the discovery document.
    pub fn connect(base_url: &str) -> Result<Self> {
        let mut client = Self::new(base_url);
        let discovery = client.discover()?;
        client.apply_discovery(&discovery);
        Ok(client)
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.agent = build_agent(timeout);
        self
    }

    /// Replaces the sleep function used between polls (tests inject a recorder).
    pub fn with_sleeper(mut self, sleeper: impl Fn(Duration) + Send + Sync + 'static) -> Self {
        self.sleeper = Arc::new(sleeper);
        self
    }

    pub fn endpoints(&self) -> &Endpoints {
        &self.endpoints
    }

    /// `GET /.well-known/t3code-control-plane`.
    pub fn discover(&self) -> Result<Discovery> {
        let url = format!("{}{DISCOVERY_PATH}", self.endpoints.base_url);
        let (status, body) = self.get(&url, None)?;
        let discovery: Discovery = parse_ok(status, &body)?;
        if discovery.protocol_version != PROTOCOL_VERSION {
            return Err(ClientError::UnsupportedProtocol(discovery.protocol_version));
        }
        Ok(discovery)
    }

    pub fn apply_discovery(&mut self, discovery: &Discovery) {
        self.endpoints = Endpoints::from_discovery(&self.endpoints.base_url, discovery);
    }

    // --- Device pairing -----------------------------------------------------------------------

    /// `POST /v1/device-pairings`.
    pub fn start_pairing(&self, request: &StartPairingRequest) -> Result<StartPairingResponse> {
        let (status, body) = self.post(&self.endpoints.device_pairing, None, Some(request))?;
        parse_ok(status, &body)
    }

    /// `GET /v1/device-pairings/:code` (public status view).
    pub fn pairing_status(&self, code: &str) -> Result<PairingStatus> {
        let code = normalize_code(code)?;
        let url = format!("{}/{code}", self.endpoints.device_pairing);
        let (status, body) = self.get(&url, None)?;
        parse_ok(status, &body)
    }

    /// One `POST /v1/device-pairings/:code/exchange` attempt, signed with the device key.
    pub fn exchange_pairing(
        &self,
        code: &str,
        pairing_id: &str,
        identity: &DeviceIdentity,
    ) -> Result<PollStep<PairingCredentials>> {
        let code = normalize_code(code)?;
        let timestamp = rfc3339_millis_now();
        let signature = identity.sign_pairing_exchange(&code, pairing_id, &timestamp);
        let url = format!("{}/{code}/exchange", self.endpoints.device_pairing);
        let request = ExchangePairingRequest {
            timestamp,
            signature,
        };
        let (status, body) = self.post(&url, None, Some(&request))?;
        poll_result(status, &body)
    }

    /// Polls the exchange every `interval` seconds (adding 5 s on `slow_down`) until the pairing is
    /// approved, denied (`AccessDenied`), or expired (`ExpiredToken`).
    pub fn poll_pairing(
        &self,
        start: &StartPairingResponse,
        identity: &DeviceIdentity,
    ) -> Result<PairingCredentials> {
        self.poll_loop(start.interval, &start.expires_at, || {
            self.exchange_pairing(&start.code, &start.pairing_id, identity)
        })
    }

    // --- Device heartbeat ---------------------------------------------------------------------

    /// `POST /v1/devices/:deviceId/heartbeat`. `403 device_revoked` maps to
    /// [`ClientError::DeviceRevoked`].
    pub fn heartbeat(
        &self,
        device_id: &str,
        device_credential: &str,
        request: &HeartbeatRequest,
    ) -> Result<HeartbeatResponse> {
        let url = format!(
            "{}/{}/heartbeat",
            self.endpoints.devices,
            encode_path_segment(device_id)
        );
        let (status, body) = self.post(&url, Some(device_credential), Some(request))?;
        match parse_ok(status, &body) {
            Err(ClientError::Api {
                status: 403,
                error,
                message,
            }) if error == "device_revoked" => Err(ClientError::DeviceRevoked(message)),
            other => other,
        }
    }

    // --- GUI client authorization -------------------------------------------------------------

    /// `POST /v1/client-authorizations`.
    pub fn start_client_authorization(
        &self,
        request: &StartClientAuthorizationRequest,
    ) -> Result<StartClientAuthorizationResponse> {
        let (status, body) =
            self.post(&self.endpoints.client_authorization, None, Some(request))?;
        parse_ok(status, &body)
    }

    /// One `POST /v1/client-authorizations/token` attempt.
    pub fn poll_client_token_once(
        &self,
        device_code: &str,
    ) -> Result<PollStep<ClientTokenResponse>> {
        let request = ClientTokenRequest {
            device_code: device_code.to_string(),
        };
        let (status, body) = self.post(&self.endpoints.client_token, None, Some(&request))?;
        poll_result(status, &body)
    }

    /// Polls the token endpoint until the user approves or the flow terminates.
    pub fn poll_client_token(
        &self,
        start: &StartClientAuthorizationResponse,
    ) -> Result<ClientTokenResponse> {
        self.poll_loop(start.interval, &start.expires_at, || {
            self.poll_client_token_once(&start.device_code)
        })
    }

    /// `GET /v1/me`.
    pub fn me(&self, client_token: &str) -> Result<MeResponse> {
        let (status, body) = self.get(&self.endpoints.me, Some(client_token))?;
        parse_ok(status, &body)
    }

    /// `GET /v1/devices`.
    pub fn list_devices(&self, client_token: &str) -> Result<Vec<Device>> {
        let (status, body) = self.get(&self.endpoints.devices, Some(client_token))?;
        parse_ok::<DeviceList>(status, &body).map(|list| list.devices)
    }

    /// `POST /v1/devices/:deviceId/connect-tokens`. `scopes == None` uses the server default
    /// (`["orchestration:read"]`).
    pub fn mint_connect_token(
        &self,
        client_token: &str,
        device_id: &str,
        scopes: Option<&[Scope]>,
    ) -> Result<ConnectTokenResponse> {
        let url = format!(
            "{}/{}/connect-tokens",
            self.endpoints.devices,
            encode_path_segment(device_id)
        );
        let request = ConnectTokenRequest {
            scopes: scopes.map(<[Scope]>::to_vec),
        };
        let (status, body) = self.post(&url, Some(client_token), Some(&request))?;
        parse_ok(status, &body)
    }

    /// `POST /v1/devices/:deviceId/revoke`. Returns the updated device.
    pub fn revoke_device(&self, client_token: &str, device_id: &str) -> Result<Device> {
        let url = format!(
            "{}/{}/revoke",
            self.endpoints.devices,
            encode_path_segment(device_id)
        );
        let (status, body) = self.post::<()>(&url, Some(client_token), None)?;
        parse_ok(status, &body)
    }

    /// `POST /v1/client-tokens/revoke` (expects `204`).
    pub fn revoke_client_token(&self, client_token: &str) -> Result<()> {
        let (status, body) = self.post::<()>(
            &self.endpoints.client_token_revoke,
            Some(client_token),
            None,
        )?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(api_error(status, &body))
        }
    }

    // --- Internals ----------------------------------------------------------------------------

    fn poll_loop<T>(
        &self,
        interval: u64,
        expires_at: &str,
        mut step: impl FnMut() -> Result<PollStep<T>>,
    ) -> Result<T> {
        let mut interval = if interval == 0 {
            POLL_INTERVAL_SECONDS
        } else {
            interval
        };
        let deadline = chrono::DateTime::parse_from_rfc3339(expires_at)
            .ok()
            .map(|d| d.with_timezone(&chrono::Utc));
        loop {
            if deadline.is_some_and(|d| chrono::Utc::now() >= d) {
                return Err(ClientError::ExpiredToken("code expired".to_string()));
            }
            (self.sleeper)(Duration::from_secs(interval));
            match step()? {
                PollStep::Ready(value) => return Ok(value),
                PollStep::Pending => {}
                PollStep::SlowDown => interval += SLOW_DOWN_INCREMENT_SECONDS,
            }
        }
    }

    fn get(&self, url: &str, bearer: Option<&str>) -> Result<(u16, String)> {
        let mut request = self.agent.get(url).header("Accept", "application/json");
        if let Some(token) = bearer {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        read_response(request.call())
    }

    fn post<B: Serialize>(
        &self,
        url: &str,
        bearer: Option<&str>,
        body: Option<&B>,
    ) -> Result<(u16, String)> {
        let mut request = self.agent.post(url).header("Accept", "application/json");
        if let Some(token) = bearer {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        let response = match body {
            Some(body) => request.send_json(body),
            None => request.send_empty(),
        };
        read_response(response)
    }
}

/// `GET /v1/device-pairings/:code` response.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairingStatus {
    pub pairing_id: String,
    pub status: String,
    pub expires_at: String,
}

fn normalize_code(code: &str) -> Result<String> {
    normalize_user_code(code)
        .ok_or_else(|| ClientError::InvalidInput(format!("invalid pairing code {code:?}")))
}

fn read_response(
    response: std::result::Result<ureq::http::Response<ureq::Body>, ureq::Error>,
) -> Result<(u16, String)> {
    let mut response = response.map_err(|e| ClientError::Transport(e.to_string()))?;
    let status = response.status().as_u16();
    let body = response
        .body_mut()
        .read_to_string()
        .map_err(|e| ClientError::Transport(e.to_string()))?;
    Ok((status, body))
}

fn parse_ok<T: DeserializeOwned>(status: u16, body: &str) -> Result<T> {
    if !(200..300).contains(&status) {
        return Err(api_error(status, body));
    }
    serde_json::from_str(body).map_err(|e| ClientError::Decode(e.to_string()))
}

fn api_error(status: u16, body: &str) -> ClientError {
    match serde_json::from_str::<ErrorBody>(body) {
        Ok(err) => ClientError::Api {
            status,
            error: err.error,
            message: err.message,
        },
        Err(_) => ClientError::Api {
            status,
            error: format!("http_{status}"),
            message: body.chars().take(200).collect(),
        },
    }
}

/// Maps device-code polling responses (RFC 8628 style errors on `400`).
fn poll_result<T: DeserializeOwned>(status: u16, body: &str) -> Result<PollStep<T>> {
    match parse_ok(status, body) {
        Ok(value) => Ok(PollStep::Ready(value)),
        Err(ClientError::Api {
            status: 400,
            error,
            message,
        }) => match error.as_str() {
            "authorization_pending" => Ok(PollStep::Pending),
            "slow_down" => Ok(PollStep::SlowDown),
            "access_denied" => Err(ClientError::AccessDenied(message)),
            "expired_token" => Err(ClientError::ExpiredToken(message)),
            _ => Err(ClientError::Api {
                status: 400,
                error,
                message,
            }),
        },
        Err(err) => Err(err),
    }
}

/// Percent-encodes everything except RFC 3986 unreserved characters.
fn encode_path_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}
