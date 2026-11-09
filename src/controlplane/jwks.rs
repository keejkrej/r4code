//! JWKS cache with rate-limited refetch on unknown `kid` (step 2).

use super::JWKS_REFETCH_INTERVAL_SECONDS;
use super::token::Jwks;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

/// Fetches the JWKS document from `jwksUri`. Injectable so tests and alternative transports can
/// supply keys without HTTP.
pub trait JwksFetcher: Send + Sync {
    fn fetch_jwks(&self, uri: &str) -> Result<Jwks, String>;
}

impl<F> JwksFetcher for F
where
    F: Fn(&str) -> Result<Jwks, String> + Send + Sync,
{
    fn fetch_jwks(&self, uri: &str) -> Result<Jwks, String> {
        self(uri)
    }
}

/// Default fetcher: blocking HTTPS GET with `ureq`.
pub struct HttpJwksFetcher {
    agent: ureq::Agent,
}

impl HttpJwksFetcher {
    pub fn new(timeout: Duration) -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            .build()
            .into();
        Self { agent }
    }
}

impl Default for HttpJwksFetcher {
    fn default() -> Self {
        Self::new(Duration::from_secs(10))
    }
}

impl JwksFetcher for HttpJwksFetcher {
    fn fetch_jwks(&self, uri: &str) -> Result<Jwks, String> {
        self.agent
            .get(uri)
            .header("Accept", "application/json")
            .call()
            .map_err(|e| format!("fetch {uri}: {e}"))?
            .body_mut()
            .read_json::<Jwks>()
            .map_err(|e| format!("decode {uri}: {e}"))
    }
}

struct CacheState {
    uri: String,
    jwks: Jwks,
    last_fetch: Option<Instant>,
}

/// Thread-safe JWKS cache.
///
/// The first lookup fetches the JWKS. Afterwards, an unknown `kid` triggers a refetch at most
/// once per `min_refetch_interval` (default 60 s); failed fetch attempts count toward the limit and
/// keep the previously cached keys.
pub struct JwksCache {
    state: RwLock<CacheState>,
    fetch_lock: Mutex<()>,
    fetcher: Box<dyn JwksFetcher>,
    min_refetch_interval: Duration,
}

impl JwksCache {
    pub fn new(uri: impl Into<String>, fetcher: impl JwksFetcher + 'static) -> Self {
        Self {
            state: RwLock::new(CacheState {
                uri: uri.into(),
                jwks: Jwks::default(),
                last_fetch: None,
            }),
            fetch_lock: Mutex::new(()),
            fetcher: Box::new(fetcher),
            min_refetch_interval: Duration::from_secs(JWKS_REFETCH_INTERVAL_SECONDS),
        }
    }

    /// Cache backed by [`HttpJwksFetcher`].
    pub fn http(uri: impl Into<String>) -> Self {
        Self::new(uri, HttpJwksFetcher::default())
    }

    pub fn with_min_refetch_interval(mut self, interval: Duration) -> Self {
        self.min_refetch_interval = interval;
        self
    }

    /// Pre-seeds keys (e.g. from a persisted copy) without counting as a fetch.
    pub fn with_keys(self, jwks: Jwks) -> Self {
        self.write().jwks = jwks;
        self
    }

    pub fn uri(&self) -> String {
        self.read().uri.clone()
    }

    /// Updates `jwksUri` (e.g. from a heartbeat response). A changed URI resets the rate limit.
    pub fn set_uri(&self, uri: &str) {
        let mut state = self.write();
        if state.uri != uri {
            state.uri = uri.to_string();
            state.last_fetch = None;
        }
    }

    pub fn snapshot(&self) -> Jwks {
        self.read().jwks.clone()
    }

    /// Forces a fetch now (e.g. at startup), regardless of the rate limit.
    pub fn refresh(&self) -> Result<(), String> {
        let _guard = self.fetch_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.fetch_locked()
    }

    /// Ensures `kid` is cached, refetching if allowed by the rate limit. Returns whether the key is
    /// available afterwards.
    pub fn ensure_kid(&self, kid: &str) -> bool {
        if self.read().jwks.contains_kid(kid) {
            return true;
        }
        let _guard = self.fetch_lock.lock().unwrap_or_else(|e| e.into_inner());
        {
            let state = self.read();
            if state.jwks.contains_kid(kid) {
                return true;
            }
            if let Some(last) = state.last_fetch
                && last.elapsed() < self.min_refetch_interval
            {
                return false;
            }
        }
        let _ = self.fetch_locked();
        self.read().jwks.contains_kid(kid)
    }

    /// Fetches while holding `fetch_lock` (but not the state lock, so verification of known keys
    /// continues during the request).
    fn fetch_locked(&self) -> Result<(), String> {
        let uri = {
            let mut state = self.write();
            state.last_fetch = Some(Instant::now());
            state.uri.clone()
        };
        let jwks = self.fetcher.fetch_jwks(&uri)?;
        let mut state = self.write();
        if state.uri == uri {
            state.jwks = jwks;
        }
        Ok(())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, CacheState> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, CacheState> {
        self.state.write().unwrap_or_else(|e| e.into_inner())
    }
}
