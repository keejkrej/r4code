use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::orchestration::errors::Error;

pub fn resolve_provider_binary(binary: &str) -> Result<String, Error> {
    if binary.is_empty() {
        return Err(Error::provider("provider binary is required"));
    }
    if binary.contains(std::path::MAIN_SEPARATOR) || binary.contains('/') {
        if is_executable_file(Path::new(binary)) {
            return Ok(binary.to_string());
        }
        return Err(Error::provider(format!(
            "provider binary {:?} was not found",
            binary
        )));
    }
    if let Some(found) = look_path_in_env(binary, &env::var("PATH").unwrap_or_default()) {
        return Ok(found);
    }
    for dir in known_provider_directories() {
        let candidate = Path::new(&dir).join(binary);
        if is_executable_file(&candidate) {
            return Ok(candidate.to_string_lossy().into_owned());
        }
    }
    Err(Error::provider(format!(
        "provider binary {:?} was not found in PATH or common CLI locations",
        binary
    )))
}

fn look_path_in_env(binary: &str, path_var: &str) -> Option<String> {
    for dir in env::split_paths(path_var) {
        if dir.as_os_str().is_empty() {
            continue;
        }
        let candidate = dir.join(binary);
        if is_executable_file(&candidate) {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }
    None
}

pub fn known_provider_directories() -> Vec<String> {
    let mut dirs = Vec::new();
    let home = env::var("HOME").ok().or_else(|| {
        dirs_home_fallback()
    });
    if let Some(home) = home {
        let home_path = PathBuf::from(&home);
        for suffix in [
            ".local/bin",
            ".bun/bin",
            ".cargo/bin",
            "bin",
            ".npm-global/bin",
            ".volta/bin",
            ".asdf/shims",
            ".mise/shims",
            ".local/share/mise/shims",
        ] {
            dirs.push(home_path.join(suffix).to_string_lossy().into_owned());
        }
    }
    for d in ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin"] {
        dirs.push(d.to_string());
    }
    dirs
}

fn dirs_home_fallback() -> Option<String> {
    env::var("USERPROFILE").ok()
}

pub fn native_environment(extra: &[String]) -> Vec<(String, String)> {
    let mut base: Vec<(String, String)> = env::vars().collect();
    for item in extra {
        if let Some((k, v)) = item.split_once('=') {
            if let Some(existing) = base.iter_mut().find(|(ek, _)| ek == k) {
                existing.1 = v.to_string();
            } else {
                base.push((k.to_string(), v.to_string()));
            }
        }
    }
    let current_path = base
        .iter()
        .find(|(k, _)| k == "PATH")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    let mut parts: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let separator = if cfg!(windows) { ";" } else { ":" };
    for entry in current_path
        .split(separator)
        .map(|s| s.to_string())
        .chain(known_provider_directories())
    {
        if entry.is_empty() || seen.contains(&entry) {
            continue;
        }
        seen.insert(entry.clone());
        parts.push(entry);
    }
    let joined_path = parts.join(separator);
    if let Some(existing) = base.iter_mut().find(|(k, _)| k == "PATH") {
        existing.1 = joined_path;
    } else {
        base.push(("PATH".to_string(), joined_path));
    }
    base
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(meta) = fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}
