//! Resume operations.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tokio::signal;
use tracing::{debug, info, warn};

use p2p_core::{
    identity::Identity, protocol::Capabilities, session::P2PSession,
    transfer_folder::FolderTransferState, Uuid,
};

pub async fn handle_resume(
    transfer_id: String,
    to: String,
    peer_fingerprint_hex: String,
    path: PathBuf,
    state_dir: Option<PathBuf>,
    max_reconnect_attempts: u32,
    identity_dir: Option<PathBuf>,
) -> Result<()> {
    info!("Resuming transfer");
    info!("  Transfer ID: {}", transfer_id);
    info!("  Path: {}", path.display());
    info!("  Peer address: {}", to);

    if !path.exists() {
        anyhow::bail!("Path does not exist: {}", path.display());
    }

    let state_path = crate::util::resolve_state_file(state_dir.as_deref(), &transfer_id)?;
    if !state_path.exists() {
        anyhow::bail!(
            "State file not found: {}. (If the original `send` ran with --state-dir, pass the same value here.)",
            state_path.display()
        );
    }

    info!("Loading transfer state...");
    let state = FolderTransferState::load_from_file(&state_path).await?;
    debug!(
        "Progress: {}/{} files ({:.1}%)",
        state.completed_files.len(),
        state.files.len(),
        state.progress_percentage()
    );

    let peer_addr = to.parse::<SocketAddr>()?;

    if peer_fingerprint_hex.len() != 64 {
        anyhow::bail!(
            "--peer-fingerprint must be 64 hex chars, got {}",
            peer_fingerprint_hex.len()
        );
    }
    let mut peer_fp = [0u8; 32];
    peer_fp.copy_from_slice(&hex::decode(&peer_fingerprint_hex)?);

    let identity = Arc::new(Identity::load_or_generate(identity_dir.as_deref())?);
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();
    // Resume the original negotiated config — using ConfigMessage::default
    // here would mis-align the .partial on disk because the receiver and
    // ChunkWriter compute offsets from this chunk_size.
    let config = state.to_config_message();

    info!("Reconnecting to peer...");
    let mut session = P2PSession::connect(
        peer_addr,
        peer_fp,
        identity,
        device_id,
        capabilities,
        config,
    )
    .await?;
    info!("Session established");

    let mut progress = p2p_core::progress::ProgressState::new(state.total_bytes);
    progress.add_bytes(state.transferred_bytes);

    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: max_reconnect_attempts,
        ..Default::default()
    };

    info!("Resuming folder transfer...");
    tokio::select! {
        result = session.send_path(&path, &reconnect_config, Some(&state_path), Some(&mut progress)) => {
            result?;
            let _ = tokio::fs::remove_file(&state_path).await;
            info!("Transfer resumed and completed!");
        }
        _ = signal::ctrl_c() => {
            // The on-disk state is up to date as of the last completed
            // file (sender persists per-file via the FolderTransferSession
            // state callback wired in send_path's error path). Chunks
            // completed mid-file since the last file boundary will be
            // re-sent on the next resume.
            warn!("Transfer interrupted. State persisted up to the most recent file boundary.");
            info!(
                "Use 'p2p-transfer resume {} --to {} --peer-fingerprint {} --path {}{}' to continue",
                transfer_id,
                to,
                peer_fingerprint_hex,
                path.display(),
                state_dir
                    .as_deref()
                    .map(|d| format!(" --state-dir {}", d.display()))
                    .unwrap_or_default(),
            );
            return Ok(());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_nonexistent_path() {
        let tid = Uuid::new_v4().to_string();
        let result = handle_resume(
            tid,
            "127.0.0.1:1".into(),
            "0".repeat(64),
            PathBuf::from("definitely/does/not/exist"),
            None,
            1,
            None,
        )
        .await;
        let err = result.expect_err("nonexistent path must error").to_string();
        assert!(err.contains("does not exist"), "got: {err}");
    }

    /// Finding 3.4: when --state-dir is supplied, handle_resume reads
    /// the state file from there rather than the current working
    /// directory. Without the flag, users who `cd`-ed between failure
    /// and resume saw "State file not found" with no recovery hint.
    #[tokio::test]
    async fn finds_state_file_via_state_dir_flag_from_unrelated_cwd() {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join("state");
        let file_path = tmp.path().join("payload.bin");
        tokio::fs::write(&file_path, b"hi").await.unwrap();

        // The state file just needs to exist for handle_resume to get
        // past the early "State file not found" bail; it will then fail
        // later trying to deserialise — but that's after we've proven
        // the path resolution honours --state-dir.
        let tid = Uuid::new_v4().to_string();
        tokio::fs::create_dir_all(&state_dir).await.unwrap();
        tokio::fs::write(
            state_dir.join(format!("transfer_{tid}.json")),
            b"{}", // empty JSON object — will fail to deserialise later
        )
        .await
        .unwrap();

        let result = handle_resume(
            tid,
            "127.0.0.1:1".into(),
            "0".repeat(64),
            file_path,
            Some(state_dir.clone()),
            1,
            None,
        )
        .await;

        let err = result
            .expect_err("should fail later for unrelated reasons")
            .to_string();
        assert!(
            !err.contains("State file not found"),
            "--state-dir must let resume locate the file; got: {err}"
        );
    }

    #[tokio::test]
    async fn accepts_file_path() {
        let tmp = tempfile::tempdir().unwrap();
        let file_path = tmp.path().join("payload.bin");
        tokio::fs::write(&file_path, b"hello").await.unwrap();

        let tid = Uuid::new_v4().to_string();
        let result = handle_resume(
            tid,
            "127.0.0.1:1".into(),
            "0".repeat(64),
            file_path,
            None,
            1,
            None,
        )
        .await;
        let err = result
            .expect_err("no state file → should error later")
            .to_string();
        assert!(
            !err.contains("not a directory"),
            "resume must accept file paths; got: {err}"
        );
    }
}
