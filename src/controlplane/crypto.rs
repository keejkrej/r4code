//! Pairing codes, key fingerprints, the pairing-exchange message, base64url helpers, and the
//! Ed25519 device identity.

use super::{CODE_ALPHABET, CODE_LENGTH, PAIRING_EXCHANGE_CONTEXT, state::write_private_file};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};
use std::fmt;
use std::io;
use std::path::Path;

/// Encodes bytes as unpadded base64url (RFC 4648 §5).
pub fn b64url_encode(bytes: impl AsRef<[u8]>) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Decodes unpadded base64url. Only `[A-Za-z0-9_-]` is accepted (no padding, no whitespace) and
/// the encoding must be canonical.
pub fn b64url_decode(value: &str) -> Option<Vec<u8>> {
    if value.is_empty() || !value.bytes().all(is_b64url_byte) {
        return None;
    }
    URL_SAFE_NO_PAD.decode(value).ok()
}

pub(crate) fn is_b64url_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'_'
}

/// Normalizes a pairing / user code: uppercases ASCII letters, drops every character that is not
/// ASCII `A`–`Z` or `0`–`9`, and returns the result only if exactly 8 characters remain and all are
/// in [`CODE_ALPHABET`].
pub fn normalize_user_code(input: &str) -> Option<String> {
    let out: String = input
        .chars()
        .map(|c| c.to_ascii_uppercase())
        .filter(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        .collect();
    if out.len() != CODE_LENGTH || !out.chars().all(|c| CODE_ALPHABET.contains(c)) {
        return None;
    }
    Some(out)
}

/// Formats a normalized 8-character code for display as `XXXX-XXXX`.
pub fn format_user_code(normalized: &str) -> String {
    if normalized.len() == CODE_LENGTH && normalized.is_ascii() {
        format!("{}-{}", &normalized[..4], &normalized[4..])
    } else {
        normalized.to_string()
    }
}

/// `SHA256:` + base64url(SHA-256(raw public key bytes)) for a base64url-encoded public key.
/// Returns `None` if `public_key` is not valid base64url.
pub fn public_key_fingerprint(public_key: &str) -> Option<String> {
    b64url_decode(public_key).map(|raw| fingerprint_bytes(&raw))
}

/// Fingerprint of raw public key bytes.
pub fn fingerprint_bytes(raw: &[u8]) -> String {
    format!("SHA256:{}", b64url_encode(Sha256::digest(raw)))
}

/// The UTF-8 message signed during pairing exchange: context, normalized code, pairing ID, and
/// timestamp joined by `\n` with no trailing newline.
pub fn pairing_exchange_message(
    normalized_code: &str,
    pairing_id: &str,
    timestamp: &str,
) -> String {
    [
        PAIRING_EXCHANGE_CONTEXT,
        normalized_code,
        pairing_id,
        timestamp,
    ]
    .join("\n")
}

/// Current time as an RFC 3339 UTC timestamp with millisecond precision.
pub fn rfc3339_millis_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// Ed25519 device key. The private seed never appears in `Debug` output.
#[derive(Clone)]
pub struct DeviceIdentity {
    signing_key: SigningKey,
}

impl DeviceIdentity {
    /// Generates a fresh key from the OS CSPRNG.
    pub fn generate() -> io::Result<Self> {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).map_err(|e| io::Error::other(format!("getrandom: {e}")))?;
        Ok(Self::from_seed(seed))
    }

    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self {
            signing_key: SigningKey::from_bytes(&seed),
        }
    }

    /// Builds the identity from a base64url-encoded 32-byte seed (the format of the seed file and
    /// the test vectors).
    pub fn from_seed_base64url(seed: &str) -> io::Result<Self> {
        let raw = b64url_decode(seed.trim())
            .ok_or_else(|| invalid_data("device seed is not valid base64url"))?;
        let seed: [u8; 32] = raw
            .try_into()
            .map_err(|_| invalid_data("device seed must be 32 bytes"))?;
        Ok(Self::from_seed(seed))
    }

    pub fn seed_base64url(&self) -> String {
        b64url_encode(self.signing_key.to_bytes())
    }

    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// Raw public key as base64url (the `publicKey` field of the start-pairing request).
    pub fn public_key_base64url(&self) -> String {
        b64url_encode(self.public_key_bytes())
    }

    pub fn fingerprint(&self) -> String {
        fingerprint_bytes(&self.public_key_bytes())
    }

    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.signing_key.sign(message).to_bytes()
    }

    pub fn sign_base64url(&self, message: &[u8]) -> String {
        b64url_encode(self.sign(message))
    }

    /// Signs the pairing-exchange message and returns the base64url signature.
    pub fn sign_pairing_exchange(
        &self,
        normalized_code: &str,
        pairing_id: &str,
        timestamp: &str,
    ) -> String {
        let message = pairing_exchange_message(normalized_code, pairing_id, timestamp);
        self.sign_base64url(message.as_bytes())
    }

    /// Loads a seed file (base64url seed, surrounding whitespace ignored).
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Self::from_seed_base64url(&text)
    }

    /// Writes the seed file atomically with `0600` permissions on unix.
    pub fn save(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let mut text = self.seed_base64url();
        text.push('\n');
        write_private_file(path.as_ref(), text.as_bytes())
    }

    /// Loads the seed file, or generates and saves a new identity if it does not exist.
    pub fn load_or_generate(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        match Self::load(path) {
            Ok(identity) => Ok(identity),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let identity = Self::generate()?;
                identity.save(path)?;
                Ok(identity)
            }
            Err(err) => Err(err),
        }
    }
}

impl fmt::Debug for DeviceIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceIdentity")
            .field("public_key", &self.public_key_base64url())
            .finish_non_exhaustive()
    }
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}
