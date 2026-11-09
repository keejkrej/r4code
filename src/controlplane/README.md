# `controlplane` — t3code control-plane protocol v1 (Rust)

Rust implementation of the device and GUI sides of the shared t3code control plane
(`../t3code-control-plane`, TypeScript/Next.js on Vercel). The normative contract is
`t3code-control-plane/protocol/PROTOCOL.md`; this module mirrors the reference verifier in
`src/lib/tokens.ts` step by step.

> **Status:** library building blocks only. The device daemon (heartbeat loop, HTTP/WebSocket
> device API that calls the authorizer, pairing CLI) is **not wired up yet**, and nothing here has
> been exercised against a deployed control plane — only against the shared test vectors and an
> in-process HTTP test server.

## Contents

| File | What it provides |
| --- | --- |
| `mod.rs` | Protocol constants, `Scope`, `scope_for_tool`, `scope_for_mcp_method` |
| `crypto.rs` | `normalize_user_code`, `format_user_code`, `public_key_fingerprint`, `pairing_exchange_message`, base64url helpers, `DeviceIdentity` (Ed25519 seed file, `0600`) |
| `token.rs` | `verify_connect_token` (10 normative steps), `VerifyError` / `VerifyErrorCode`, `Jwks`, `ConnectClaims` |
| `jwks.rs` | `JwksCache` (refetch on unknown `kid` at most once per 60 s), injectable `JwksFetcher`, `HttpJwksFetcher` |
| `authorizer.rs` | `DeviceAuthorizer`: thread-safe `Authorization: Bearer` check returning `401`/`403` + `{"error","message"}` |
| `client.rs` | Blocking `Client` (ureq): discovery, device pairing, heartbeat, GUI client authorization, devices, connect tokens |
| `types.rs` | Wire types with the spec's camelCase field names; secrets are redacted in `Debug` |
| `state.rs` | `DeviceState` persisted JSON (`0600`), atomic private-file writes |
| `testdata/test-vectors.json` | **Synced copy** of `t3code-control-plane/protocol/test-vectors.json` — do not edit by hand; re-copy when the control plane regenerates it |

## Device side

```rust,no_run
use r4code::controlplane::*;

let client = Client::connect("https://t3code-control-plane.vercel.app")?;
let identity = DeviceIdentity::load_or_generate("/Users/me/.r4code/device.key")?;

// 1. Start pairing and show the code / URL to the user.
let start = client.start_pairing(&StartPairingRequest {
    public_key: identity.public_key_base64url(),
    name: None,
    hostname: "mac-mini".into(),
    platform: "darwin".into(),
    arch: "arm64".into(),
    client: ClientInfo { kind: "r4code".into(), version: env!("CARGO_PKG_VERSION").into() },
    tailscale: TailscaleInfo { ip: None, magic_dns_name: Some("mac-mini.tailnet-abc.ts.net".into()) },
    port: 7420,
    capabilities: vec!["mcp".into(), "orchestration".into()],
})?;
println!("Open {} (code {}, key {})", start.verification_uri_complete, start.code, identity.fingerprint());

// 2. Poll the signed exchange (honors interval, slow_down, access_denied, expired_token).
let paired = client.poll_pairing(&start, &identity)?;
let state = DeviceState::from_pairing("https://t3code-control-plane.vercel.app", &paired);
state.save("/Users/me/.r4code/control-plane.json")?;

// 3. Verify connect tokens on the device API.
let authorizer = DeviceAuthorizer::from_device_state(&state, JwksCache::http(paired.jwks_uri.clone()));
// per request:
// match authorizer.authorize(req.header("authorization"), scope_for_tool("t3_thread_send")) {
//     Ok(claims) => ...,
//     Err(e) => respond(e.status, e.body_string()),   // 401 or 403
// }

// 4. Heartbeat every heartbeatIntervalSeconds.
match client.heartbeat(&state.device_id, &state.device_credential, &HeartbeatRequest::default()) {
    Ok(hb) => authorizer.apply_heartbeat(&hb),          // max(epoch), jwksUri updates
    Err(ClientError::DeviceRevoked(_)) => {
        authorizer.mark_revoked();                      // reject every token from now on
        DeviceState::delete("/Users/me/.r4code/control-plane.json")?;
    }
    Err(e) => eprintln!("heartbeat failed: {e}"),
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

For MCP over the device API, use `scope_for_mcp_method(method, tool_name)` with
`DeviceAuthorizer::authorize_optional_scope`: `initialize`, `tools/list`, `ping`, and
`notifications/*` need only a valid token; `tools/call` follows the tool table; unknown tools and
other methods need `orchestration:write`.

## GUI side

```rust,no_run
use r4code::controlplane::*;

let client = Client::connect("https://t3code-control-plane.vercel.app")?;
let start = client.start_client_authorization(&StartClientAuthorizationRequest {
    client_name: "r4code desktop on mac-mini".into(),
    client: ClientInfo { kind: "r4code".into(), version: "0.1.0".into() },
    scopes: None, // both scopes
})?;
// open start.verification_uri_complete in the browser, then:
let token = client.poll_client_token(&start)?;
for device in client.list_devices(&token.access_token)? {
    println!("{} {} {:?}", device.id, device.status, device.endpoint);
}
let connect = client.mint_connect_token(&token.access_token, "dev_…", Some(&[Scope::Write]))?;
// connect directly to connect.endpoint over Tailscale with `Authorization: Bearer {connect.token}`.
# Ok::<(), Box<dyn std::error::Error>>(())
```

All failures from the control plane surface as `ClientError` with `code()` returning the protocol
`error` string (`invalid_token`, `insufficient_scope`, `device_revoked`, …).

## Verifier behavior notes

- Follows the 10 normative steps in order; the first failing step decides the code.
- `alg` must be exactly `"EdDSA"`; `typ`, if present, must be the string `"JWT"` (`null` is
  rejected); `kid` must be a string.
- Signatures use `ed25519-dalek`'s `verify_strict` (rejects non-canonical `S` and small-order
  keys/`R`).
- base64url must be canonical and unpadded (stricter than Node's `Buffer` decoding, allowed by the
  spec).
- Time, epoch, and version claims must be integral JSON numbers within ±(2^53−1); `300.0` is
  accepted like `Number.isInteger`, strings and fractions are not.
- `sub` and `jti` are surfaced when present but are not required by the normative steps.

## Tests

```sh
cargo test controlplane
```

Every `codeNormalization`, `codeFormatting`, `fingerprint`, `pairingExchange` (public key and
deterministic signature byte-for-byte), and `connectTokens` case from the shared vectors is
checked, plus client tests against a local `TcpListener` server (pairing pending → approved,
`slow_down`, terminal errors, heartbeat revoked, GUI flow) and authorizer tests (401/403, scope
gating, epoch/revocation updates, JWKS refetch rate limiting).
