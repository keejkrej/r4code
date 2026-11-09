//! HTTP client tests against a tiny in-process HTTP/1.1 server.

use super::*;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    authorization: Option<String>,
    body: String,
}

impl Recorded {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or(Value::Null)
    }
}

type Handler = dyn Fn(&Recorded, &str) -> (u16, String) + Send + Sync;

struct TestServer {
    base: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
}

impl TestServer {
    /// Serves each connection with `handler(request, base_url)`.
    fn start(handler: impl Fn(&Recorded, &str) -> (u16, String) + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<Handler> = Arc::new(handler);
        {
            let base = base.clone();
            let requests = Arc::clone(&requests);
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { continue };
                    let Some(request) = read_request(&mut stream) else {
                        continue;
                    };
                    requests.lock().unwrap().push(request.clone());
                    let (status, body) = handler(&request, &base);
                    let response = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
                }
            });
        }
        Self { base, requests }
    }

    fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    fn client(&self, sleeps: &Arc<Mutex<Vec<u64>>>) -> Client {
        let sleeps = Arc::clone(sleeps);
        Client::new(&self.base)
            .with_timeout(Duration::from_secs(5))
            .with_sleeper(move |d| sleeps.lock().unwrap().push(d.as_secs()))
    }
}

fn read_request(stream: &mut std::net::TcpStream) -> Option<Recorded> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let mut content_length = 0usize;
    let mut authorization = None;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).ok()?;
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().ok()?;
            } else if name.eq_ignore_ascii_case("authorization") {
                authorization = Some(value.to_string());
            }
        }
    }
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).ok()?;
    Some(Recorded {
        method,
        path,
        authorization,
        body: String::from_utf8(body).ok()?,
    })
}

fn err(code: &str) -> (u16, String) {
    (400, json!({"error": code, "message": code}).to_string())
}

fn future() -> String {
    (chrono::Utc::now() + chrono::Duration::minutes(10))
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

fn start_response(base: &str) -> StartPairingResponse {
    StartPairingResponse {
        pairing_id: "dpr_test".to_string(),
        code: "BCDF-GHJK".to_string(),
        verification_uri: format!("{base}/pair"),
        verification_uri_complete: format!("{base}/pair?code=BCDF-GHJK"),
        expires_at: future(),
        interval: 5,
    }
}

fn paired_body(base: &str) -> String {
    json!({
        "deviceId": "dev_test",
        "deviceCredential": "t3cpd_secret",
        "ownerUserId": "usr_test",
        "issuer": base,
        "jwksUri": format!("{base}/.well-known/jwks.json"),
        "revocationEpoch": 0,
        "heartbeatIntervalSeconds": 60
    })
    .to_string()
}

fn pairing_request(identity: &DeviceIdentity) -> StartPairingRequest {
    StartPairingRequest {
        public_key: identity.public_key_base64url(),
        name: Some("Test Mac".to_string()),
        hostname: "test-mac".to_string(),
        platform: "darwin".to_string(),
        arch: "arm64".to_string(),
        client: ClientInfo {
            kind: "r4code".to_string(),
            version: "0.1.0".to_string(),
        },
        tailscale: TailscaleInfo {
            ip: Some("100.101.102.103".to_string()),
            magic_dns_name: None,
        },
        port: 7420,
        capabilities: vec!["mcp".to_string(), "orchestration".to_string()],
    }
}

#[test]
fn pairing_start_then_poll_pending_until_approved() {
    let exchanges = Arc::new(Mutex::new(0));
    let server = {
        let exchanges = Arc::clone(&exchanges);
        TestServer::start(
            move |req, base| match (req.method.as_str(), req.path.as_str()) {
                ("POST", "/v1/device-pairings") => {
                    (201, serde_json::to_string(&start_response(base)).unwrap())
                }
                ("POST", "/v1/device-pairings/BCDFGHJK/exchange") => {
                    let mut n = exchanges.lock().unwrap();
                    *n += 1;
                    if *n < 3 {
                        err("authorization_pending")
                    } else {
                        (200, paired_body(base))
                    }
                }
                _ => (
                    404,
                    json!({"error": "not_found", "message": "nope"}).to_string(),
                ),
            },
        )
    };
    let sleeps = Arc::new(Mutex::new(Vec::new()));
    let client = server.client(&sleeps);
    let identity = DeviceIdentity::generate().unwrap();

    let start = client.start_pairing(&pairing_request(&identity)).unwrap();
    assert_eq!(start.code, "BCDF-GHJK");
    let paired = client.poll_pairing(&start, &identity).unwrap();
    assert_eq!(paired.device_id, "dev_test");
    assert_eq!(paired.device_credential, "t3cpd_secret");
    assert_eq!(paired.issuer, server.base);
    assert_eq!(*sleeps.lock().unwrap(), vec![5, 5, 5]);

    let requests = server.requests();
    // Start request uses the exact camelCase wire names.
    let start_body = requests[0].json();
    assert_eq!(start_body["publicKey"], identity.public_key_base64url());
    assert_eq!(start_body["tailscale"]["ip"], "100.101.102.103");
    assert!(start_body["tailscale"].get("magicDnsName").is_none());
    assert_eq!(start_body["client"]["kind"], "r4code");
    assert_eq!(start_body["port"], 7420);

    // Every exchange is signed over the spec message with the timestamp exactly as sent.
    let exchange_requests: Vec<_> = requests
        .iter()
        .filter(|r| r.path.ends_with("/exchange"))
        .collect();
    assert_eq!(exchange_requests.len(), 3);
    let public_key: [u8; 32] = identity.public_key_bytes();
    let key = ed25519_dalek::VerifyingKey::from_bytes(&public_key).unwrap();
    for req in exchange_requests {
        assert!(req.authorization.is_none());
        let body = req.json();
        let timestamp = body["timestamp"].as_str().unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(timestamp).is_ok());
        assert!(
            timestamp.ends_with('Z') && timestamp.len() == 24,
            "{timestamp}"
        );
        let message = pairing_exchange_message("BCDFGHJK", "dpr_test", timestamp);
        let sig: [u8; 64] = crypto::b64url_decode(body["signature"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        key.verify_strict(
            message.as_bytes(),
            &ed25519_dalek::Signature::from_bytes(&sig),
        )
        .unwrap();
    }

    // The result persists as device state.
    let state = DeviceState::from_pairing(&server.base, &paired);
    assert_eq!(state.device_id, "dev_test");
}

#[test]
fn pairing_poll_slow_down_adds_five_seconds() {
    let exchanges = Arc::new(Mutex::new(0));
    let server = {
        let exchanges = Arc::clone(&exchanges);
        TestServer::start(move |_req, base| {
            let mut n = exchanges.lock().unwrap();
            *n += 1;
            match *n {
                1 => err("slow_down"),
                2 => err("authorization_pending"),
                3 => err("slow_down"),
                _ => (200, paired_body(base)),
            }
        })
    };
    let sleeps = Arc::new(Mutex::new(Vec::new()));
    let client = server.client(&sleeps);
    let identity = DeviceIdentity::generate().unwrap();
    let paired = client
        .poll_pairing(&start_response(&server.base), &identity)
        .unwrap();
    assert_eq!(paired.device_id, "dev_test");
    assert_eq!(*sleeps.lock().unwrap(), vec![5, 10, 10, 15]);
}

#[test]
fn pairing_poll_terminal_errors() {
    for (code, check) in [
        (
            "access_denied",
            (|e: &ClientError| matches!(e, ClientError::AccessDenied(_)))
                as fn(&ClientError) -> bool,
        ),
        ("expired_token", |e| {
            matches!(e, ClientError::ExpiredToken(_))
        }),
    ] {
        let server = TestServer::start(move |_req, _base| err(code));
        let sleeps = Arc::new(Mutex::new(Vec::new()));
        let client = server.client(&sleeps);
        let identity = DeviceIdentity::generate().unwrap();
        let error = client
            .poll_pairing(&start_response(&server.base), &identity)
            .unwrap_err();
        assert!(check(&error), "{code}: {error:?}");
        assert_eq!(error.code(), Some(code));
        assert_eq!(sleeps.lock().unwrap().len(), 1);
    }

    // invalid_signature is surfaced with its protocol code and status.
    let server = TestServer::start(|_req, _base| {
        (
            401,
            json!({"error": "invalid_signature", "message": "bad"}).to_string(),
        )
    });
    let client = server.client(&Arc::new(Mutex::new(Vec::new())));
    let identity = DeviceIdentity::generate().unwrap();
    let error = client
        .exchange_pairing("bcdf ghjk", "dpr_test", &identity)
        .unwrap_err();
    assert_eq!(error.code(), Some("invalid_signature"));
    assert_eq!(error.status(), Some(401));
    assert_eq!(
        server.requests()[0].path,
        "/v1/device-pairings/BCDFGHJK/exchange"
    );

    // Invalid codes never hit the network.
    let error = client
        .exchange_pairing("AAAA-AAAA", "dpr_test", &identity)
        .unwrap_err();
    assert!(matches!(error, ClientError::InvalidInput(_)));

    // A locally expired code stops polling without sleeping.
    let sleeps = Arc::new(Mutex::new(Vec::new()));
    let client = server.client(&sleeps);
    let mut start = start_response(&server.base);
    start.expires_at = "2020-01-01T00:00:00.000Z".to_string();
    let error = client.poll_pairing(&start, &identity).unwrap_err();
    assert!(matches!(error, ClientError::ExpiredToken(_)));
    assert!(sleeps.lock().unwrap().is_empty());
}

#[test]
fn heartbeat_success_and_revoked() {
    let revoked = Arc::new(Mutex::new(false));
    let server = {
        let revoked = Arc::clone(&revoked);
        TestServer::start(move |req, base| {
            assert_eq!(req.path, "/v1/devices/dev_test/heartbeat");
            if *revoked.lock().unwrap() {
                return (
                    403,
                    json!({"error": "device_revoked", "message": "revoked"}).to_string(),
                );
            }
            (
                200,
                json!({
                    "deviceId": "dev_test",
                    "status": "active",
                    "revocationEpoch": 4,
                    "issuer": base,
                    "jwksUri": format!("{base}/.well-known/jwks.json"),
                    "heartbeatIntervalSeconds": 60,
                    "serverTime": "2026-11-09T20:02:00.000Z",
                    "somethingNew": 1
                })
                .to_string(),
            )
        })
    };
    let client = server.client(&Arc::new(Mutex::new(Vec::new())));
    let request = HeartbeatRequest {
        port: Some(7421),
        ..Default::default()
    };
    let response = client
        .heartbeat("dev_test", "t3cpd_secret", &request)
        .unwrap();
    assert_eq!(response.revocation_epoch, 4);

    let recorded = &server.requests()[0];
    assert_eq!(
        recorded.authorization.as_deref(),
        Some("Bearer t3cpd_secret")
    );
    assert_eq!(recorded.json(), json!({"port": 7421}));

    // Feed the heartbeat into the authorizer.
    let auth = DeviceAuthorizer::new(
        server.base.clone(),
        "dev_test",
        0,
        JwksCache::new("unused", |_: &str| Ok(Jwks::default())),
    );
    auth.apply_heartbeat(&response);
    assert_eq!(auth.state().revocation_epoch, 4);
    assert_eq!(
        auth.jwks().uri(),
        format!("{}/.well-known/jwks.json", server.base)
    );

    *revoked.lock().unwrap() = true;
    let error = client
        .heartbeat("dev_test", "t3cpd_secret", &request)
        .unwrap_err();
    assert!(matches!(error, ClientError::DeviceRevoked(_)), "{error:?}");
    assert_eq!(error.code(), Some("device_revoked"));
    auth.mark_revoked();
    assert!(auth.state().revoked);
}

#[test]
fn heartbeat_invalid_credential_is_not_revocation() {
    let server = TestServer::start(|_req, _base| {
        (
            401,
            json!({"error": "invalid_credential", "message": "unknown"}).to_string(),
        )
    });
    let client = server.client(&Arc::new(Mutex::new(Vec::new())));
    let error = client
        .heartbeat("dev_test", "t3cpd_bad", &HeartbeatRequest::default())
        .unwrap_err();
    assert_eq!(error.code(), Some("invalid_credential"));
    assert!(!matches!(error, ClientError::DeviceRevoked(_)));
    assert_eq!(server.requests()[0].body, "{}");
}

#[test]
fn gui_flow_discovery_authorization_and_devices() {
    let token_polls = Arc::new(Mutex::new(0));
    let server = {
        let token_polls = Arc::clone(&token_polls);
        TestServer::start(move |req, base| {
            let bearer_ok = req.authorization.as_deref() == Some("Bearer t3cpc_token");
            match (req.method.as_str(), req.path.as_str()) {
                ("GET", "/.well-known/t3code-control-plane") => (
                    200,
                    json!({
                        "protocolVersion": 1,
                        "issuer": base,
                        "jwksUri": format!("{base}/.well-known/jwks.json"),
                        "devicePairingEndpoint": format!("{base}/v1/device-pairings"),
                        "clientAuthorizationEndpoint": format!("{base}/v1/client-authorizations"),
                        "clientTokenEndpoint": format!("{base}/v1/client-authorizations/token"),
                        "devicesEndpoint": format!("{base}/v1/devices"),
                        "scopesSupported": ["orchestration:read", "orchestration:write"],
                        "connectTokenTtlSeconds": 300,
                        "heartbeatIntervalSeconds": 60
                    })
                    .to_string(),
                ),
                ("POST", "/v1/client-authorizations") => (
                    201,
                    json!({
                        "authorizationId": "cau_1",
                        "deviceCode": "secret-device-code",
                        "userCode": "BCDF-GHJK",
                        "verificationUri": format!("{base}/authorize"),
                        "verificationUriComplete": format!("{base}/authorize?code=BCDF-GHJK"),
                        "expiresAt": future(),
                        "interval": 5
                    })
                    .to_string(),
                ),
                ("POST", "/v1/client-authorizations/token") => {
                    assert_eq!(req.json(), json!({"deviceCode": "secret-device-code"}));
                    let mut n = token_polls.lock().unwrap();
                    *n += 1;
                    if *n == 1 {
                        err("authorization_pending")
                    } else {
                        (
                            200,
                            json!({
                                "accessToken": "t3cpc_token",
                                "tokenType": "Bearer",
                                "scope": "orchestration:read orchestration:write",
                                "userId": "usr_1",
                                "expiresAt": future()
                            })
                            .to_string(),
                        )
                    }
                }
                _ if !bearer_ok => (401, json!({"error": "invalid_token", "message": "no"}).to_string()),
                ("GET", "/v1/me") => (
                    200,
                    json!({
                        "user": {"id": "usr_1", "name": "Jack", "username": "jack", "email": null, "avatarUrl": null},
                        "scope": "orchestration:read orchestration:write"
                    })
                    .to_string(),
                ),
                ("GET", "/v1/devices") => (
                    200,
                    json!({"devices": [
                        {
                            "id": "dev_1", "name": "Mac", "hostname": "mac", "platform": "darwin", "arch": "arm64",
                            "client": {"kind": "r4code", "version": "0.1.0"},
                            "tailscale": {"ip": null, "magicDnsName": "mac.tailnet.ts.net"},
                            "port": 7420, "endpoint": "http://mac.tailnet.ts.net:7420",
                            "capabilities": ["mcp"], "publicKeyFingerprint": "SHA256:abc",
                            "status": "active", "online": true, "revocationEpoch": 0,
                            "createdAt": "2026-11-09T20:00:00.000Z", "lastSeenAt": null, "revokedAt": null
                        },
                        {"id": "dev_2", "status": "revoked", "endpoint": null, "revocationEpoch": 1}
                    ]})
                    .to_string(),
                ),
                ("POST", "/v1/devices/dev_1/connect-tokens") => {
                    let scopes = req.json()["scopes"].clone();
                    if scopes == json!(["orchestration:write"]) {
                        (
                            201,
                            json!({
                                "token": "a.b.c", "tokenType": "Bearer",
                                "scope": "orchestration:read orchestration:write",
                                "expiresIn": 300, "expiresAt": future(), "deviceId": "dev_1",
                                "endpoint": "http://mac.tailnet.ts.net:7420"
                            })
                            .to_string(),
                        )
                    } else {
                        assert!(req.json().get("scopes").is_none());
                        (403, json!({"error": "insufficient_scope", "message": "lacks"}).to_string())
                    }
                }
                ("POST", "/v1/devices/dev_2/connect-tokens") => {
                    (409, json!({"error": "device_revoked", "message": "revoked"}).to_string())
                }
                ("POST", "/v1/client-tokens/revoke") => (204, String::new()),
                _ => (404, json!({"error": "not_found", "message": "nope"}).to_string()),
            }
        })
    };
    let sleeps = Arc::new(Mutex::new(Vec::new()));
    let mut client = server.client(&sleeps);
    let discovery = client.discover().unwrap();
    assert_eq!(discovery.issuer, server.base);
    client.apply_discovery(&discovery);
    assert_eq!(client.endpoints().me, format!("{}/v1/me", server.base));

    let start = client
        .start_client_authorization(&StartClientAuthorizationRequest {
            client_name: "r4code desktop".to_string(),
            client: ClientInfo {
                kind: "r4code".to_string(),
                version: "0.1.0".to_string(),
            },
            scopes: Some(vec![Scope::Read, Scope::Write]),
        })
        .unwrap();
    assert!(!format!("{start:?}").contains("secret-device-code"));
    let token = client.poll_client_token(&start).unwrap();
    assert_eq!(token.access_token, "t3cpc_token");
    assert_eq!(token.scopes(), vec![Scope::Read, Scope::Write]);
    assert_eq!(*sleeps.lock().unwrap(), vec![5, 5]);
    let start_body = server
        .requests()
        .into_iter()
        .find(|r| r.path == "/v1/client-authorizations")
        .unwrap()
        .json();
    assert_eq!(
        start_body,
        json!({"clientName": "r4code desktop", "client": {"kind": "r4code", "version": "0.1.0"}, "scopes": ["orchestration:read", "orchestration:write"]})
    );

    let me = client.me(&token.access_token).unwrap();
    assert_eq!(me.user.username.as_deref(), Some("jack"));
    assert_eq!(me.user.email, None);

    let devices = client.list_devices(&token.access_token).unwrap();
    assert_eq!(devices.len(), 2);
    assert_eq!(
        devices[0]
            .tailscale
            .as_ref()
            .unwrap()
            .magic_dns_name
            .as_deref(),
        Some("mac.tailnet.ts.net")
    );
    assert_eq!(devices[1].endpoint, None);

    let connect = client
        .mint_connect_token(&token.access_token, "dev_1", Some(&[Scope::Write]))
        .unwrap();
    assert_eq!(connect.token, "a.b.c");
    assert_eq!(connect.expires_in, 300);
    let error = client
        .mint_connect_token(&token.access_token, "dev_1", None)
        .unwrap_err();
    assert_eq!(
        (error.status(), error.code()),
        (Some(403), Some("insufficient_scope"))
    );
    let error = client
        .mint_connect_token(&token.access_token, "dev_2", Some(&[Scope::Read]))
        .unwrap_err();
    assert_eq!(
        (error.status(), error.code()),
        (Some(409), Some("device_revoked"))
    );

    let error = client.me("t3cpc_wrong").unwrap_err();
    assert_eq!(error.code(), Some("invalid_token"));

    client.revoke_client_token(&token.access_token).unwrap();
}

#[test]
fn discovery_rejects_unknown_protocol_version_and_transport_errors() {
    let server =
        TestServer::start(|_req, base| {
            (200, json!({
            "protocolVersion": 2, "issuer": base, "jwksUri": "", "devicePairingEndpoint": "",
            "clientAuthorizationEndpoint": "", "clientTokenEndpoint": "", "devicesEndpoint": ""
        }).to_string())
        });
    let error = Client::connect(&server.base).unwrap_err();
    assert_eq!(error, ClientError::UnsupportedProtocol(2));

    // Non-protocol error bodies still surface a code.
    let server = TestServer::start(|_req, _base| (502, "<html>bad gateway</html>".to_string()));
    let error = Client::new(&server.base).discover().unwrap_err();
    assert_eq!(error.code(), Some("http_502"));

    // Unreachable host.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let error = Client::new(&format!("http://{addr}"))
        .with_timeout(Duration::from_secs(2))
        .discover()
        .unwrap_err();
    assert!(matches!(error, ClientError::Transport(_)), "{error:?}");
}
