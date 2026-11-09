//! Shared test vectors, verifier, authorizer, JWKS cache, and persistence tests.

use super::crypto::{b64url_decode, b64url_encode};
use super::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Synced from `t3code-control-plane/protocol/test-vectors.json`.
const VECTORS: &str = include_str!("testdata/test-vectors.json");

fn vectors() -> Value {
    serde_json::from_str(VECTORS).expect("test vectors parse")
}

fn s(v: &Value) -> &str {
    v.as_str().expect("string")
}

#[test]
fn vectors_protocol_constants() {
    let v = vectors();
    assert_eq!(v["protocolVersion"].as_i64(), Some(PROTOCOL_VERSION));
    assert_eq!(s(&v["codeAlphabet"]), CODE_ALPHABET);
    assert_eq!(
        v["connectTokens"]["leewaySeconds"].as_i64(),
        Some(LEEWAY_SECONDS)
    );
    assert_eq!(
        v["connectTokens"]["maxLifetimeSeconds"].as_i64(),
        Some(MAX_LIFETIME_SECONDS)
    );
}

#[test]
fn vectors_code_normalization() {
    let v = vectors();
    let cases = v["codeNormalization"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let input = s(&case["input"]);
        let expected = case["normalized"].as_str();
        assert_eq!(
            normalize_user_code(input).as_deref(),
            expected,
            "normalize({input:?})"
        );
    }
    for case in v["codeFormatting"].as_array().unwrap() {
        assert_eq!(
            format_user_code(s(&case["normalized"])),
            s(&case["display"])
        );
    }
}

#[test]
fn vectors_fingerprint() {
    let v = vectors();
    let fp = &v["fingerprint"];
    assert_eq!(
        public_key_fingerprint(s(&fp["publicKey"])).as_deref(),
        Some(s(&fp["fingerprint"]))
    );
}

#[test]
fn vectors_pairing_exchange() {
    let v = vectors();
    let px = &v["pairingExchange"];
    let identity = DeviceIdentity::from_seed_base64url(s(&px["deviceSeed"])).unwrap();
    assert_eq!(identity.seed_base64url(), s(&px["deviceSeed"]));
    assert_eq!(identity.public_key_base64url(), s(&px["publicKey"]));
    assert_eq!(
        identity.fingerprint(),
        s(&v["fingerprint"]["fingerprint"]),
        "fingerprint vector uses the same device key"
    );

    let message =
        pairing_exchange_message(s(&px["code"]), s(&px["pairingId"]), s(&px["timestamp"]));
    assert_eq!(message, s(&px["message"]));
    let signature =
        identity.sign_pairing_exchange(s(&px["code"]), s(&px["pairingId"]), s(&px["timestamp"]));
    assert_eq!(
        signature,
        s(&px["signature"]),
        "deterministic Ed25519 signature"
    );

    // Byte-for-byte, and it verifies strictly against the published public key.
    let sig_bytes: [u8; 64] = b64url_decode(s(&px["signature"]))
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(identity.sign(message.as_bytes()), sig_bytes);
    let pk: [u8; 32] = b64url_decode(s(&px["publicKey"]))
        .unwrap()
        .try_into()
        .unwrap();
    ed25519_dalek::VerifyingKey::from_bytes(&pk)
        .unwrap()
        .verify_strict(
            message.as_bytes(),
            &ed25519_dalek::Signature::from_bytes(&sig_bytes),
        )
        .unwrap();
}

#[test]
fn vectors_signing_key_and_kid_thumbprint() {
    let v = vectors();
    let ct = &v["connectTokens"];
    let signer = DeviceIdentity::from_seed_base64url(s(&ct["signingKey"]["seed"])).unwrap();
    let x = s(&ct["signingKey"]["privateJwk"]["x"]);
    assert_eq!(signer.public_key_base64url(), x);
    // RFC 7638 thumbprint.
    let canonical = format!(r#"{{"crv":"Ed25519","kty":"OKP","x":"{x}"}}"#);
    let kid = b64url_encode(Sha256::digest(canonical.as_bytes()));
    assert_eq!(kid, s(&ct["signingKey"]["kid"]));
    assert_eq!(kid, s(&ct["jwks"]["keys"][0]["kid"]));
}

#[test]
fn vectors_connect_tokens() {
    let v = vectors();
    let ct = &v["connectTokens"];
    let jwks: Jwks = serde_json::from_value(ct["jwks"].clone()).unwrap();
    let context = &ct["context"];
    let cases = ct["cases"].as_array().unwrap();
    assert!(cases.len() >= 30, "expected the full case list");
    for case in cases {
        let name = s(&case["name"]);
        let required = Scope::parse(s(&case["requiredScope"])).expect("scope");
        let ctx = VerificationContext {
            issuer: s(&context["issuer"]),
            device_id: s(&context["deviceId"]),
            revocation_epoch: context["revocationEpoch"].as_i64().unwrap(),
            revoked: case["revoked"].as_bool().unwrap_or(false),
            jwks: &jwks,
            now: context["now"].as_i64().unwrap(),
        };
        let result = verify_connect_token(s(&case["token"]), &ctx, required);
        let expect = &case["expect"];
        if expect["valid"].as_bool().unwrap() {
            let claims = result.unwrap_or_else(|e| panic!("{name}: expected valid, got {e}"));
            let scopes: Vec<String> = serde_json::from_value(expect["scopes"].clone()).unwrap();
            assert_eq!(claims.scopes, scopes, "{name}: scopes");
            assert_eq!(claims.aud, ctx.device_id, "{name}");
            assert_eq!(claims.iss, ctx.issuer, "{name}");
        } else {
            let err = result
                .err()
                .unwrap_or_else(|| panic!("{name}: expected error"));
            assert_eq!(
                err.code.as_str(),
                s(&expect["error"]),
                "{name}: {}",
                err.reason
            );
            assert_eq!(VerifyErrorCode::parse(err.code.as_str()), Some(err.code));
        }
    }
}

#[test]
fn vectors_tool_scopes() {
    let v = vectors();
    let read: Vec<String> = serde_json::from_value(v["toolScopes"]["read"].clone()).unwrap();
    let write: Vec<String> = serde_json::from_value(v["toolScopes"]["write"].clone()).unwrap();
    assert_eq!(read, READ_TOOLS);
    assert_eq!(write, WRITE_TOOLS);
    for tool in &read {
        assert_eq!(scope_for_tool(tool), Scope::Read, "{tool}");
    }
    for tool in &write {
        assert_eq!(scope_for_tool(tool), Scope::Write, "{tool}");
    }
    assert_eq!(scope_for_tool("some_future_tool"), Scope::Write);

    assert_eq!(scope_for_mcp_method("initialize", None), None);
    assert_eq!(scope_for_mcp_method("tools/list", None), None);
    assert_eq!(scope_for_mcp_method("ping", None), None);
    assert_eq!(
        scope_for_mcp_method("notifications/initialized", None),
        None
    );
    assert_eq!(
        scope_for_mcp_method("tools/call", Some("t3_thread_read")),
        Some(Scope::Read)
    );
    assert_eq!(
        scope_for_mcp_method("tools/call", Some("delegate_task")),
        Some(Scope::Write)
    );
    assert_eq!(scope_for_mcp_method("tools/call", None), Some(Scope::Write));
    assert_eq!(
        scope_for_mcp_method("resources/list", None),
        Some(Scope::Write)
    );
}

#[test]
fn scope_serde_and_parsing() {
    assert_eq!(
        serde_json::to_string(&[Scope::Read, Scope::Write]).unwrap(),
        r#"["orchestration:read","orchestration:write"]"#
    );
    assert_eq!(Scope::parse("orchestration:write"), Some(Scope::Write));
    assert_eq!(Scope::parse("orchestration:admin"), None);
    assert!(Scope::Read.is_satisfied_by(&["orchestration:write"]));
    assert!(!Scope::Write.is_satisfied_by(&["orchestration:read"]));
}

// --- Token crafting helpers for authorizer/verifier tests ----------------------------------------

struct Fixture {
    signer: DeviceIdentity,
    kid: String,
    jwks: Jwks,
    issuer: String,
    device_id: String,
    now: i64,
}

fn fixture() -> Fixture {
    let v = vectors();
    let ct = &v["connectTokens"];
    Fixture {
        signer: DeviceIdentity::from_seed_base64url(s(&ct["signingKey"]["seed"])).unwrap(),
        kid: s(&ct["signingKey"]["kid"]).to_string(),
        jwks: serde_json::from_value(ct["jwks"].clone()).unwrap(),
        issuer: s(&ct["context"]["issuer"]).to_string(),
        device_id: s(&ct["context"]["deviceId"]).to_string(),
        now: ct["context"]["now"].as_i64().unwrap(),
    }
}

impl Fixture {
    fn claims(&self, scope: &str) -> Value {
        json!({
            "iss": self.issuer,
            "sub": "usr_test",
            "aud": self.device_id,
            "iat": self.now - 10,
            "nbf": self.now - 10,
            "exp": self.now + 290,
            "jti": "ctk_test",
            "scope": scope,
            "t3cp_epoch": 1,
            "t3cp_ver": 1,
        })
    }

    fn token(&self, claims: &Value) -> String {
        craft_jwt(
            &json!({"alg": "EdDSA", "typ": "JWT", "kid": self.kid}),
            claims,
            &self.signer,
        )
    }

    fn raw_token(&self, payload_json: &str) -> String {
        let h = b64url_encode(
            json!({"alg": "EdDSA", "typ": "JWT", "kid": self.kid})
                .to_string()
                .as_bytes(),
        );
        let p = b64url_encode(payload_json.as_bytes());
        let sig = self.signer.sign_base64url(format!("{h}.{p}").as_bytes());
        format!("{h}.{p}.{sig}")
    }

    fn ctx(&self) -> VerificationContext<'_> {
        VerificationContext {
            issuer: &self.issuer,
            device_id: &self.device_id,
            revocation_epoch: 1,
            revoked: false,
            jwks: &self.jwks,
            now: self.now,
        }
    }

    fn authorizer(&self) -> DeviceAuthorizer {
        let jwks = self.jwks.clone();
        let cache = JwksCache::new(
            "https://control-plane.test/.well-known/jwks.json",
            move |_: &str| Ok(jwks.clone()),
        );
        let now = self.now;
        DeviceAuthorizer::new(self.issuer.clone(), self.device_id.clone(), 1, cache)
            .with_clock(move || now)
    }
}

fn craft_jwt(header: &Value, claims: &Value, signer: &DeviceIdentity) -> String {
    let h = b64url_encode(header.to_string().as_bytes());
    let p = b64url_encode(claims.to_string().as_bytes());
    let sig = signer.sign_base64url(format!("{h}.{p}").as_bytes());
    format!("{h}.{p}.{sig}")
}

#[test]
fn verifier_rejects_non_integer_time_claims() {
    let f = fixture();
    let ctx = f.ctx();
    let ok = f.token(&f.claims("orchestration:read"));
    assert!(verify_connect_token(&ok, &ctx, Scope::Read).is_ok());

    let mut claims = f.claims("orchestration:read");
    claims["exp"] = json!(format!("{}", f.now + 290));
    let err = verify_connect_token(&f.token(&claims), &ctx, Scope::Read).unwrap_err();
    assert_eq!(err.code, VerifyErrorCode::InvalidToken);

    let payload = f.claims("orchestration:read").to_string().replace(
        &format!("\"iat\":{}", f.now - 10),
        &format!("\"iat\":{}.5", f.now - 10),
    );
    assert!(payload.contains(".5"));
    let err = verify_connect_token(&f.raw_token(&payload), &ctx, Scope::Read).unwrap_err();
    assert_eq!(err.code, VerifyErrorCode::InvalidToken);

    // Integral values written as floats are integral JSON numbers (Number.isInteger semantics).
    let payload = f
        .claims("orchestration:read")
        .to_string()
        .replace("\"t3cp_ver\":1", "\"t3cp_ver\":1.0")
        .replace(
            &format!("\"exp\":{}", f.now + 290),
            &format!("\"exp\":{}.0", f.now + 290),
        );
    assert!(payload.contains("1.0"));
    assert!(verify_connect_token(&f.raw_token(&payload), &ctx, Scope::Read).is_ok());

    // Outside ±(2^53−1) is rejected even though it fits in i64.
    let mut claims = f.claims("orchestration:read");
    claims["t3cp_epoch"] = json!(1_i64 << 53);
    let err = verify_connect_token(&f.token(&claims), &ctx, Scope::Read).unwrap_err();
    assert_eq!(err.code, VerifyErrorCode::InvalidToken);
    claims["t3cp_epoch"] = json!((1_i64 << 53) - 1);
    assert!(verify_connect_token(&f.token(&claims), &ctx, Scope::Read).is_ok());
    claims["t3cp_epoch"] = json!(u64::MAX);
    assert_eq!(
        verify_connect_token(&f.token(&claims), &ctx, Scope::Read)
            .unwrap_err()
            .code,
        VerifyErrorCode::InvalidToken
    );
}

#[test]
fn verifier_edge_cases() {
    let f = fixture();
    let ctx = f.ctx();
    let check = |claims: &Value, required: Scope| {
        verify_connect_token(&f.token(claims), &ctx, required).map_err(|e| e.code)
    };

    // nbf exactly at now + 30 is accepted.
    let mut c = f.claims("orchestration:read");
    c["iat"] = json!(f.now + 30);
    c["nbf"] = json!(f.now + 30);
    c["exp"] = json!(f.now + 330);
    assert!(check(&c, Scope::Read).is_ok());

    // exp − iat == 900 is accepted, 901 is not.
    let mut c = f.claims("orchestration:read");
    c["exp"] = json!(f.now - 10 + 900);
    assert!(check(&c, Scope::Read).is_ok());
    c["exp"] = json!(f.now - 10 + 901);
    assert_eq!(check(&c, Scope::Read), Err(VerifyErrorCode::InvalidToken));

    // Non-string audience entries and empty arrays.
    let mut c = f.claims("orchestration:read");
    c["aud"] = json!([]);
    assert_eq!(check(&c, Scope::Read), Err(VerifyErrorCode::WrongAudience));
    c["aud"] = json!([42]);
    assert_eq!(check(&c, Scope::Read), Err(VerifyErrorCode::WrongAudience));
    c.as_object_mut().unwrap().remove("aud");
    assert_eq!(check(&c, Scope::Read), Err(VerifyErrorCode::WrongAudience));

    // Missing issuer, version as a string.
    let mut c = f.claims("orchestration:read");
    c.as_object_mut().unwrap().remove("iss");
    assert_eq!(check(&c, Scope::Read), Err(VerifyErrorCode::InvalidToken));
    let mut c = f.claims("orchestration:read");
    c["t3cp_ver"] = json!("1");
    assert_eq!(check(&c, Scope::Read), Err(VerifyErrorCode::InvalidToken));

    // Scope is split on single spaces only; extra spaces are ignored, unknown scopes kept.
    let c = f.claims(" orchestration:read  orchestration:write extra ");
    let claims = verify_connect_token(&f.token(&c), &ctx, Scope::Write).unwrap();
    assert_eq!(
        claims.scopes,
        vec!["orchestration:read", "orchestration:write", "extra"]
    );
    assert_eq!(claims.sub.as_deref(), Some("usr_test"));
    assert_eq!(claims.jti.as_deref(), Some("ctk_test"));
    assert_eq!(claims.kid, f.kid);

    // typ: null counts as present and wrong.
    let token = craft_jwt(
        &json!({"alg": "EdDSA", "typ": null, "kid": f.kid}),
        &f.claims("orchestration:read"),
        &f.signer,
    );
    assert_eq!(
        verify_connect_token(&token, &ctx, Scope::Read)
            .unwrap_err()
            .code,
        VerifyErrorCode::InvalidToken
    );

    // Padded base64 segments are rejected.
    let token = f.token(&f.claims("orchestration:read"));
    let mut parts: Vec<String> = token.split('.').map(str::to_string).collect();
    parts[2].push('=');
    assert_eq!(
        verify_connect_token(&parts.join("."), &ctx, Scope::Read)
            .unwrap_err()
            .code,
        VerifyErrorCode::InvalidToken
    );

    // Steps 1–9 only.
    let c = f.claims("");
    assert!(verify_connect_token_any_scope(&f.token(&c), &ctx).is_ok());
}

// --- Authorizer ------------------------------------------------------------------------------

#[test]
fn authorizer_status_codes_and_scope_gating() {
    let f = fixture();
    let auth = f.authorizer();
    let read = f.token(&f.claims("orchestration:read"));
    let write = f.token(&f.claims("orchestration:read orchestration:write"));

    // Missing / malformed header → 401 invalid_token.
    for header in [
        None,
        Some(""),
        Some("Basic abc"),
        Some("Bearer"),
        Some("Bearer  "),
    ] {
        let err = auth.authorize(header, Scope::Read).unwrap_err();
        assert_eq!(err.status, 401, "{header:?}");
        assert_eq!(err.code(), VerifyErrorCode::InvalidToken);
        assert_eq!(err.body()["error"], "invalid_token");
    }
    let err = auth
        .authorize(Some("Bearer not-a-token"), Scope::Read)
        .unwrap_err();
    assert_eq!(
        (err.status, err.code()),
        (401, VerifyErrorCode::InvalidToken)
    );

    // Read token: read ok, write → 403 insufficient_scope.
    let header = format!("Bearer {read}");
    let claims = auth.authorize(Some(&header), Scope::Read).unwrap();
    assert_eq!(claims.scopes, vec!["orchestration:read"]);
    let err = auth.authorize(Some(&header), Scope::Write).unwrap_err();
    assert_eq!(err.status, 403);
    assert_eq!(err.code(), VerifyErrorCode::InsufficientScope);
    let body: Value = serde_json::from_str(&err.body_string()).unwrap();
    assert_eq!(body["error"], "insufficient_scope");
    assert_eq!(body["message"], "requires orchestration:write");

    // Scheme is case-insensitive.
    let header_w = format!("bearer {write}");
    assert!(auth.authorize(Some(&header_w), Scope::Write).is_ok());

    // MCP gating: tools/call on a write tool with a read token is 403; initialize passes.
    let tool_scope = scope_for_mcp_method("tools/call", Some("delegate_task"));
    let err = auth
        .authorize_optional_scope(Some(&header), tool_scope)
        .unwrap_err();
    assert_eq!(err.status, 403);
    let no_scope = format!("Bearer {}", f.token(&f.claims("")));
    assert!(
        auth.authorize_optional_scope(Some(&no_scope), scope_for_mcp_method("initialize", None))
            .is_ok()
    );
    assert_eq!(
        auth.authorize(Some(&no_scope), scope_for_tool("t3_thread_list"))
            .unwrap_err()
            .status,
        403
    );

    // Expired token → 401 token_expired.
    let mut c = f.claims("orchestration:read");
    c["iat"] = json!(f.now - 400);
    c["nbf"] = json!(f.now - 400);
    c["exp"] = json!(f.now - 100);
    let err = auth
        .authorize(Some(&format!("Bearer {}", f.token(&c))), Scope::Read)
        .unwrap_err();
    assert_eq!(
        (err.status, err.code()),
        (401, VerifyErrorCode::TokenExpired)
    );
}

#[test]
fn authorizer_heartbeat_updates_and_revocation() {
    let f = fixture();
    let auth = f.authorizer();
    let header = format!("Bearer {}", f.token(&f.claims("orchestration:read")));
    assert!(auth.authorize(Some(&header), Scope::Read).is_ok());

    let heartbeat = |epoch: i64, status: &str| HeartbeatResponse {
        device_id: f.device_id.clone(),
        status: status.to_string(),
        revocation_epoch: epoch,
        issuer: f.issuer.clone(),
        jwks_uri: "https://control-plane.test/.well-known/jwks.json".to_string(),
        heartbeat_interval_seconds: 60,
        server_time: "2026-11-06T20:00:00.000Z".to_string(),
    };

    auth.apply_heartbeat(&heartbeat(2, "active"));
    assert_eq!(auth.state().revocation_epoch, 2);
    let err = auth.authorize(Some(&header), Scope::Read).unwrap_err();
    assert_eq!((err.status, err.code()), (401, VerifyErrorCode::StaleEpoch));

    // The epoch never goes backwards.
    auth.apply_heartbeat(&heartbeat(0, "active"));
    assert_eq!(auth.state().revocation_epoch, 2);

    let mut c = f.claims("orchestration:read");
    c["t3cp_epoch"] = json!(2);
    let fresh = format!("Bearer {}", f.token(&c));
    assert!(auth.authorize(Some(&fresh), Scope::Read).is_ok());

    auth.mark_revoked();
    assert!(auth.state().revoked);
    let err = auth.authorize(Some(&fresh), Scope::Read).unwrap_err();
    assert_eq!(
        (err.status, err.code()),
        (401, VerifyErrorCode::DeviceRevoked)
    );
    assert_eq!(err.body()["error"], "device_revoked");
}

#[test]
fn authorizer_is_shareable_across_threads() {
    let f = fixture();
    let auth = Arc::new(f.authorizer());
    let header = format!("Bearer {}", f.token(&f.claims("orchestration:read")));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let auth = Arc::clone(&auth);
            let header = header.clone();
            std::thread::spawn(move || auth.authorize(Some(&header), Scope::Read).is_ok())
        })
        .collect();
    for h in handles {
        assert!(h.join().unwrap());
    }
}

// --- JWKS cache ------------------------------------------------------------------------------

#[test]
fn jwks_cache_refetches_unknown_kid_at_most_once_per_interval() {
    let f = fixture();
    let fetches = Arc::new(AtomicUsize::new(0));
    let served = Arc::new(std::sync::Mutex::new(Jwks::default()));
    let cache = {
        let fetches = Arc::clone(&fetches);
        let served = Arc::clone(&served);
        JwksCache::new("https://cp.test/.well-known/jwks.json", move |uri: &str| {
            assert_eq!(uri, "https://cp.test/.well-known/jwks.json");
            fetches.fetch_add(1, Ordering::SeqCst);
            Ok(served.lock().unwrap().clone())
        })
    };

    // First miss fetches (empty set), second miss within 60 s does not.
    assert!(!cache.ensure_kid(&f.kid));
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    *served.lock().unwrap() = f.jwks.clone();
    assert!(!cache.ensure_kid(&f.kid));
    assert_eq!(fetches.load(Ordering::SeqCst), 1);

    // A forced refresh picks up the key; subsequent hits don't fetch.
    cache.refresh().unwrap();
    assert_eq!(fetches.load(Ordering::SeqCst), 2);
    assert!(cache.ensure_kid(&f.kid));
    assert!(cache.ensure_kid(&f.kid));
    assert_eq!(fetches.load(Ordering::SeqCst), 2);

    // Unknown kid still rate-limited.
    assert!(!cache.ensure_kid("other"));
    assert_eq!(fetches.load(Ordering::SeqCst), 2);
}

#[test]
fn jwks_cache_refetch_after_interval_and_failures_keep_keys() {
    let f = fixture();
    let fetches = Arc::new(AtomicUsize::new(0));
    let cache = {
        let fetches = Arc::clone(&fetches);
        let jwks = f.jwks.clone();
        JwksCache::new("https://cp.test/jwks", move |_: &str| {
            if fetches.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(jwks.clone())
            } else {
                Err("boom".to_string())
            }
        })
        .with_min_refetch_interval(Duration::ZERO)
    };
    assert!(cache.ensure_kid(&f.kid));
    assert!(!cache.ensure_kid("rotated"));
    assert_eq!(fetches.load(Ordering::SeqCst), 2);
    // Failed fetch kept the old keys.
    assert!(cache.snapshot().contains_kid(&f.kid));

    // Changing the URI resets the rate limit.
    let fetches_b = Arc::new(AtomicUsize::new(0));
    let limited = {
        let fetches_b = Arc::clone(&fetches_b);
        JwksCache::new("a", move |_: &str| {
            fetches_b.fetch_add(1, Ordering::SeqCst);
            Ok(Jwks::default())
        })
    };
    assert!(!limited.ensure_kid("k"));
    assert!(!limited.ensure_kid("k"));
    assert_eq!(fetches_b.load(Ordering::SeqCst), 1);
    limited.set_uri("b");
    assert_eq!(limited.uri(), "b");
    assert!(!limited.ensure_kid("k"));
    assert_eq!(fetches_b.load(Ordering::SeqCst), 2);
}

#[test]
fn authorizer_fetches_jwks_lazily_on_unknown_kid() {
    let f = fixture();
    let fetches = Arc::new(AtomicUsize::new(0));
    let cache = {
        let fetches = Arc::clone(&fetches);
        let jwks = f.jwks.clone();
        JwksCache::new("https://cp.test/jwks", move |_: &str| {
            fetches.fetch_add(1, Ordering::SeqCst);
            Ok(jwks.clone())
        })
    };
    let now = f.now;
    let auth = DeviceAuthorizer::new(f.issuer.clone(), f.device_id.clone(), 1, cache)
        .with_clock(move || now);
    let header = format!("Bearer {}", f.token(&f.claims("orchestration:read")));
    assert!(auth.authorize(Some(&header), Scope::Read).is_ok());
    assert!(auth.authorize(Some(&header), Scope::Read).is_ok());
    assert_eq!(fetches.load(Ordering::SeqCst), 1);

    // A token with a foreign kid does not trigger another fetch within 60 s.
    let foreign = craft_jwt(
        &json!({"alg": "EdDSA", "typ": "JWT", "kid": "foreign"}),
        &f.claims("orchestration:read"),
        &f.signer,
    );
    let err = auth
        .authorize(Some(&format!("Bearer {foreign}")), Scope::Read)
        .unwrap_err();
    assert_eq!(err.code(), VerifyErrorCode::InvalidToken);
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
}

// --- Persistence ------------------------------------------------------------------------------

#[cfg(unix)]
fn mode(path: &std::path::Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

#[test]
fn device_identity_seed_file_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join("device.key");
    let identity = DeviceIdentity::load_or_generate(&path).unwrap();
    #[cfg(unix)]
    assert_eq!(mode(&path), 0o600);
    let loaded = DeviceIdentity::load_or_generate(&path).unwrap();
    assert_eq!(loaded.public_key_bytes(), identity.public_key_bytes());
    assert!(!format!("{identity:?}").contains(&identity.seed_base64url()));

    // Saving over a world-readable file tightens permissions.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        identity.save(&path).unwrap();
        assert_eq!(mode(&path), 0o600);
    }

    std::fs::write(&path, "not base64!").unwrap();
    assert!(DeviceIdentity::load(&path).is_err());
    std::fs::write(&path, b64url_encode([1u8; 16])).unwrap();
    assert!(DeviceIdentity::load(&path).is_err());
    let a = DeviceIdentity::generate().unwrap();
    let b = DeviceIdentity::generate().unwrap();
    assert_ne!(a.public_key_bytes(), b.public_key_bytes());
}

#[test]
fn device_state_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("device.json");
    let paired: PairingCredentials = serde_json::from_value(json!({
        "deviceId": "dev_1",
        "deviceCredential": "t3cpd_secret",
        "ownerUserId": "usr_1",
        "issuer": "https://cp.test",
        "jwksUri": "https://cp.test/.well-known/jwks.json",
        "revocationEpoch": 3,
        "heartbeatIntervalSeconds": 60,
        "futureField": true
    }))
    .unwrap();
    assert!(!format!("{paired:?}").contains("t3cpd_secret"));
    let state = DeviceState::from_pairing("https://cp.test", &paired);
    state.save(&path).unwrap();
    #[cfg(unix)]
    assert_eq!(mode(&path), 0o600);
    let loaded = DeviceState::load(&path).unwrap();
    assert_eq!(loaded, state);
    assert_eq!(loaded.revocation_epoch, 3);
    assert!(!format!("{loaded:?}").contains("t3cpd_secret"));
    let raw: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(raw["deviceCredential"], "t3cpd_secret");
    assert_eq!(raw["controlPlaneUrl"], "https://cp.test");

    let f = fixture();
    let auth = DeviceAuthorizer::from_device_state(
        &loaded,
        JwksCache::new("x", move |_: &str| Ok(f.jwks.clone())),
    );
    assert_eq!(auth.state().revocation_epoch, 3);
    assert_eq!(auth.state().device_id, "dev_1");

    DeviceState::delete(&path).unwrap();
    assert!(!path.exists());
    DeviceState::delete(&path).unwrap();
}
