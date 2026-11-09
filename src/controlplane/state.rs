//! Persisted device state and private-file helpers.

use super::types::PairingCredentials;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{self, Write};
use std::path::Path;

/// What a paired device persists next to its private key.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceState {
    /// Control-plane base URL used for discovery.
    pub control_plane_url: String,
    pub device_id: String,
    /// `t3cpd_…` secret used for heartbeats.
    pub device_credential: String,
    /// Token issuer stored at pairing; connect tokens must match it exactly.
    pub issuer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_uri: Option<String>,
    pub revocation_epoch: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<String>,
}

impl DeviceState {
    pub fn from_pairing(control_plane_url: impl Into<String>, paired: &PairingCredentials) -> Self {
        Self {
            control_plane_url: control_plane_url.into(),
            device_id: paired.device_id.clone(),
            device_credential: paired.device_credential.clone(),
            issuer: paired.issuer.clone(),
            jwks_uri: Some(paired.jwks_uri.clone()),
            revocation_epoch: paired.revocation_epoch,
            owner_user_id: Some(paired.owner_user_id.clone()),
        }
    }

    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let bytes = std::fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Writes pretty JSON atomically with `0600` permissions on unix.
    pub fn save(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let mut json = serde_json::to_vec_pretty(self).map_err(io::Error::other)?;
        json.push(b'\n');
        write_private_file(path.as_ref(), &json)
    }

    /// Deletes the state file (after `device_revoked`). Missing files are not an error.
    pub fn delete(path: impl AsRef<Path>) -> io::Result<()> {
        match std::fs::remove_file(path) {
            Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
            _ => Ok(()),
        }
    }
}

impl fmt::Debug for DeviceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceState")
            .field("control_plane_url", &self.control_plane_url)
            .field("device_id", &self.device_id)
            .field("device_credential", &"<redacted>")
            .field("issuer", &self.issuer)
            .field("jwks_uri", &self.jwks_uri)
            .field("revocation_epoch", &self.revocation_epoch)
            .field("owner_user_id", &self.owner_user_id)
            .finish()
    }
}

/// Writes `bytes` to `path` via a sibling temp file + rename. On unix the file is created with
/// mode `0600` and the final permissions are forced to `0600`.
pub fn write_private_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let mut tmp_name = file_name.to_os_string();
    tmp_name.push(format!(".tmp-{}", std::process::id()));
    let tmp = path.with_file_name(tmp_name);

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| {
        let mut file = options.open(&tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}
