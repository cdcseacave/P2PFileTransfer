//! Small CLI helpers shared by send/resume.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Derive a human-readable "base name" from a path, even when the path is
/// `.`, `..`, or ends with a trailing separator. `Path::file_name` returns
/// `None` for those cases — using `.unwrap()` panicked the CLI on entirely
/// reasonable inputs like `p2p-transfer send .` (review finding 3.1).
///
/// Strategy: try `file_name` first; on `None`, canonicalize and try again.
/// As a last resort fall back to the path's own display form. Never panics.
pub fn derive_base_name(path: &Path) -> Result<String> {
    if let Some(name) = path.file_name() {
        return Ok(name.to_string_lossy().to_string());
    }
    let canonical = path.canonicalize().with_context(|| {
        format!(
            "path has no file name and cannot be canonicalised: {}",
            path.display()
        )
    })?;
    if let Some(name) = canonical.file_name() {
        return Ok(name.to_string_lossy().to_string());
    }
    // Filesystem root (e.g. `/` or `C:\`) — no meaningful base name.
    Ok(canonical.display().to_string())
}

/// Build the on-disk path for a resume state file. Honours an explicit
/// `--state-dir` from the caller and falls back to the current working
/// directory (the historical default). When `state_dir` is `Some`, the
/// directory is created on demand so the caller doesn't have to.
///
/// Without `--state-dir`, `p2p-transfer resume <id>` was implicitly
/// scoped to the CWD; users who `cd`-ed between failure and resume saw
/// "State file not found" with no recovery hint (review finding 3.4).
pub fn resolve_state_file(state_dir: Option<&Path>, transfer_id: &str) -> Result<PathBuf> {
    let file_name = format!("transfer_{transfer_id}.json");
    match state_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("failed to create state dir {}", dir.display()))?;
            Ok(dir.join(file_name))
        }
        None => Ok(PathBuf::from(file_name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Finding 3.1: `derive_base_name` must not panic on `.`, `..`, or
    /// trailing separators. The pre-fix code used `path.file_name().unwrap()`
    /// in p2p-cli/src/send.rs and panicked on `p2p-transfer send .`.
    #[test]
    fn derive_base_name_handles_dot_and_dotdot() {
        let name = derive_base_name(Path::new(".")).expect("dot must resolve");
        assert!(
            !name.is_empty(),
            "dot path should resolve to current dir's basename"
        );

        let dotdot = derive_base_name(Path::new(".."));
        assert!(dotdot.is_ok(), "double-dot must not panic, got {dotdot:?}");
    }

    #[test]
    fn derive_base_name_handles_plain_file_name() {
        let name = derive_base_name(Path::new("hello.bin")).unwrap();
        assert_eq!(name, "hello.bin");
    }

    /// Finding 3.4: when `--state-dir` is supplied, the resume state
    /// file lives under that directory regardless of the user's CWD.
    /// The directory is auto-created.
    #[test]
    fn resolve_state_file_honours_explicit_state_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("nested").join("subdir");
        let path = resolve_state_file(Some(&dir), "abc-123").unwrap();
        assert_eq!(path, dir.join("transfer_abc-123.json"));
        assert!(dir.exists(), "state dir must be auto-created");
    }

    #[test]
    fn resolve_state_file_defaults_to_cwd_when_state_dir_absent() {
        let path = resolve_state_file(None, "abc-123").unwrap();
        assert_eq!(path, PathBuf::from("transfer_abc-123.json"));
    }
}
