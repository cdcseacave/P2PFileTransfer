//! Small CLI helpers for the send path: base-name derivation and locating
//! resume state on disk.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result};
use tracing::warn;

use p2p_core::protocol::FileMetadata;
use p2p_core::transfer_folder::{enumerate_files, FolderTransferState};

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

/// Per-user default directory for resume state files, used whenever
/// `--state-dir` is not supplied. Keying off a stable per-user location
/// (not the CWD) lets a `send` re-run from any working directory still
/// find the prior incomplete transfer for the same (peer, source).
///
/// * Windows: `%APPDATA%\p2p-transfer\state`
/// * Linux:   `$XDG_DATA_HOME/p2p-transfer/state` (`~/.local/share/...`)
/// * macOS:   `~/Library/Application Support/p2p-transfer/state`
pub fn default_state_dir() -> PathBuf {
    match directories::BaseDirs::new() {
        Some(base) => base.data_dir().join("p2p-transfer").join("state"),
        // No home directory (extremely unusual — e.g. a stripped service
        // account). Fall back to a relative dir so resume still works
        // within a single working directory.
        None => PathBuf::from("p2p-transfer").join("state"),
    }
}

/// Build the on-disk path for a resume state file. An explicit
/// `--state-dir` wins; otherwise the per-user [`default_state_dir`] is
/// used. The directory is created on demand so the caller can write the
/// state file straight away.
pub fn resolve_state_file(state_dir: Option<&Path>, transfer_id: &str) -> Result<PathBuf> {
    let file_name = format!("transfer_{transfer_id}.json");
    let dir = match state_dir {
        Some(dir) => dir.to_path_buf(),
        None => default_state_dir(),
    };
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create state dir {}", dir.display()))?;
    Ok(dir.join(file_name))
}

/// Find a prior incomplete transfer that a fresh `send` of `source_root`
/// to `peer_fingerprint` should resume, by scanning `state_dir` for a
/// `transfer_*.json` whose stamped peer matches and whose recorded file
/// list matches the source as it exists on disk right now.
///
/// Matching is **strict**: every file must agree on `(relative path, size,
/// modified time)`. Any drift — a resized file, a touched mtime, a file
/// added or removed — is treated as a different transfer and yields `None`
/// (i.e. a fresh transfer). When more than one state file matches, the
/// newest (by file mtime) is chosen and a warning is logged; the stale
/// ones are removed by their own success path on a later run.
pub async fn find_resumable_state(
    state_dir: &Path,
    source_root: &Path,
    peer_fingerprint: [u8; 32],
) -> Result<Option<(PathBuf, FolderTransferState)>> {
    // Enumerate the source exactly as a fresh send would. If it can't be
    // enumerated (empty folder, vanished path) there's nothing to resume;
    // let the main send path surface the real error.
    let Ok(current) = enumerate_files(source_root).await else {
        return Ok(None);
    };
    let current_key = sorted_file_keys(&current);

    let mut entries = match tokio::fs::read_dir(state_dir).await {
        Ok(entries) => entries,
        // State dir doesn't exist yet → no prior transfers.
        Err(_) => return Ok(None),
    };

    let mut best: Option<(PathBuf, SystemTime, FolderTransferState)> = None;
    let mut matches = 0usize;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !is_transfer_state_file(&path) {
            continue;
        }
        // Unparseable files (corrupt, or written by an older format) are
        // ignored rather than migrated — per the no-backcompat rule, an
        // unreadable checkpoint just means "start fresh".
        let Ok(state) = FolderTransferState::load_from_file(&path).await else {
            continue;
        };
        if state.peer_fingerprint != peer_fingerprint {
            continue;
        }
        if sorted_file_keys(&state.files) != current_key {
            continue;
        }
        matches += 1;
        let mtime = entry
            .metadata()
            .await
            .ok()
            .and_then(|m| m.modified().ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let is_newer = match &best {
            Some((_, best_mtime, _)) => mtime > *best_mtime,
            None => true,
        };
        if is_newer {
            best = Some((path, mtime, state));
        }
    }

    if matches > 1 {
        warn!(
            "{} resumable state files match this source and peer; resuming the newest. \
             Stale ones are removed when their transfer next completes.",
            matches
        );
    }

    Ok(best.map(|(path, _, state)| (path, state)))
}

fn is_transfer_state_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with("transfer_") && n.ends_with(".json"))
}

/// `(path, size, modified)` triples sorted by path, so the match in
/// [`find_resumable_state`] is independent of directory-enumeration order
/// (which is not guaranteed stable across runs). The stored `state.files`
/// order is left untouched — only these throwaway comparison keys are
/// sorted — so the resume's file indices stay valid.
fn sorted_file_keys(files: &[FileMetadata]) -> Vec<(String, u64, u64)> {
    let mut keys: Vec<(String, u64, u64)> = files
        .iter()
        .map(|f| (f.path.clone(), f.size, f.modified))
        .collect();
    keys.sort();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use p2p_core::protocol::ConfigMessage;
    use p2p_core::Uuid;

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

    /// `default_state_dir` always ends with `p2p-transfer/state`, wherever
    /// the per-user data dir lands on this platform.
    #[test]
    fn default_state_dir_targets_per_user_p2p_transfer_state() {
        let dir = default_state_dir();
        let mut tail = dir
            .components()
            .rev()
            .map(|c| c.as_os_str().to_string_lossy().to_string());
        assert_eq!(tail.next().as_deref(), Some("state"));
        assert_eq!(tail.next().as_deref(), Some("p2p-transfer"));
    }

    /// Finding 3.4: when `--state-dir` is supplied, the resume state file
    /// lives under that directory regardless of the user's CWD. The
    /// directory is auto-created.
    #[test]
    fn resolve_state_file_honours_explicit_state_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("nested").join("subdir");
        let path = resolve_state_file(Some(&dir), "abc-123").unwrap();
        assert_eq!(path, dir.join("transfer_abc-123.json"));
        assert!(dir.exists(), "state dir must be auto-created");
    }

    // ---- find_resumable_state -------------------------------------------

    /// Create `tmp/src/` populated with the given files and return the
    /// `TempDir` (kept alive by the caller so it isn't reaped).
    async fn make_source(files: &[(&str, &[u8])]) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("src");
        tokio::fs::create_dir_all(&root).await.unwrap();
        for (name, content) in files {
            tokio::fs::write(root.join(name), content).await.unwrap();
        }
        tmp
    }

    /// Persist a state file (stamped with `peer_fp`) describing `files`.
    async fn save_state(state_dir: &Path, files: Vec<FileMetadata>, peer_fp: [u8; 32]) -> PathBuf {
        tokio::fs::create_dir_all(state_dir).await.unwrap();
        let mut state = FolderTransferState::new(
            Uuid::new_v4(),
            "src".into(),
            files,
            &ConfigMessage::default(),
        );
        state.peer_fingerprint = peer_fp;
        let path = state_dir.join(format!("transfer_{}.json", state.transfer_id));
        state.save_to_file(&path).await.unwrap();
        path
    }

    #[tokio::test]
    async fn matches_when_peer_and_files_agree() {
        let tmp = make_source(&[("a.txt", b"hello"), ("b.txt", b"world!!")]).await;
        let src = tmp.path().join("src");
        let state_dir = tmp.path().join("state");
        let fp = [7u8; 32];
        let files = enumerate_files(&src).await.unwrap();
        let saved = save_state(&state_dir, files, fp).await;

        let (path, state) = find_resumable_state(&state_dir, &src, fp)
            .await
            .unwrap()
            .expect("matching peer + file list should resume");
        assert_eq!(path, saved);
        assert_eq!(state.peer_fingerprint, fp);
    }

    #[tokio::test]
    async fn rejects_when_peer_fingerprint_differs() {
        let tmp = make_source(&[("a.txt", b"hello")]).await;
        let src = tmp.path().join("src");
        let state_dir = tmp.path().join("state");
        let files = enumerate_files(&src).await.unwrap();
        save_state(&state_dir, files, [1u8; 32]).await;

        let found = find_resumable_state(&state_dir, &src, [2u8; 32])
            .await
            .unwrap();
        assert!(found.is_none(), "a different peer must not match");
    }

    #[tokio::test]
    async fn rejects_when_a_file_size_differs() {
        let tmp = make_source(&[("a.txt", b"hello")]).await;
        let src = tmp.path().join("src");
        let state_dir = tmp.path().join("state");
        let fp = [3u8; 32];
        let mut files = enumerate_files(&src).await.unwrap();
        files[0].size += 1; // pretend the file used to be a different size
        save_state(&state_dir, files, fp).await;

        let found = find_resumable_state(&state_dir, &src, fp).await.unwrap();
        assert!(found.is_none(), "size drift must not match");
    }

    #[tokio::test]
    async fn rejects_when_a_file_mtime_differs() {
        let tmp = make_source(&[("a.txt", b"hello")]).await;
        let src = tmp.path().join("src");
        let state_dir = tmp.path().join("state");
        let fp = [4u8; 32];
        let mut files = enumerate_files(&src).await.unwrap();
        files[0].modified ^= 0xFFFF; // perturb the recorded mtime
        save_state(&state_dir, files, fp).await;

        let found = find_resumable_state(&state_dir, &src, fp).await.unwrap();
        assert!(found.is_none(), "mtime drift must not match");
    }

    #[tokio::test]
    async fn rejects_when_a_file_is_added_or_removed() {
        let tmp = make_source(&[("a.txt", b"hello"), ("b.txt", b"there")]).await;
        let src = tmp.path().join("src");
        let state_dir = tmp.path().join("state");
        let fp = [5u8; 32];
        let mut files = enumerate_files(&src).await.unwrap();
        files.pop(); // state knows about fewer files than exist now
        save_state(&state_dir, files, fp).await;

        let found = find_resumable_state(&state_dir, &src, fp).await.unwrap();
        assert!(found.is_none(), "file-set drift must not match");
    }

    #[tokio::test]
    async fn returns_none_on_empty_state_dir() {
        let tmp = make_source(&[("a.txt", b"hello")]).await;
        let src = tmp.path().join("src");
        let state_dir = tmp.path().join("state");
        tokio::fs::create_dir_all(&state_dir).await.unwrap();

        let found = find_resumable_state(&state_dir, &src, [9u8; 32])
            .await
            .unwrap();
        assert!(found.is_none(), "no state files → no match");
    }
}
