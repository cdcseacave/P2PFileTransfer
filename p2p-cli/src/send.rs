//! Send operations

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use p2p_core::{
    bandwidth::format_bandwidth,
    progress::ProgressState,
    protocol::{Capabilities, ConfigMessage, FileMetadata},
    session::P2PSession,
    transfer_folder::scan_folder_for_parallel,
    Uuid,
};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::signal;

use crate::cli::{SessionParams, TransferParams};
use tracing::warn;

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.cyan} {msg}")
        .unwrap()
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
}

fn make_spinner(msg: &str) -> ProgressBar {
    let bar = ProgressBar::new_spinner();
    bar.set_style(spinner_style());
    bar.set_message(msg.to_string());
    bar.enable_steady_tick(std::time::Duration::from_millis(80));
    bar
}

pub async fn handle_send(
    path: PathBuf,
    session_params: SessionParams,
    transfer_params: TransferParams,
) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("Path does not exist: {}", path.display());
    }

    let config = ConfigMessage {
        compression_enabled: transfer_params.compress,
        compression_level: transfer_params.compress_level,
        adaptive_compression: transfer_params.adaptive,
        chunk_size: transfer_params.chunk_size * 1024,
        window_size: transfer_params.window_size,
        bandwidth_limit: transfer_params.max_speed,
    };

    let parallel = transfer_params.parallel.max(1);

    if parallel > 1 && path.is_dir() {
        handle_parallel_send(path, session_params, config, transfer_params.max_retries, parallel)
            .await
    } else {
        handle_single_send(path, session_params, config, transfer_params.max_retries).await
    }
}

/// Standard single-connection send (original behaviour).
async fn handle_single_send(
    path: PathBuf,
    session_params: SessionParams,
    config: ConfigMessage,
    max_retries: u32,
) -> Result<()> {
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();

    let peer_label = session_params
        .peer
        .as_deref()
        .unwrap_or("(discovery)")
        .to_string();
    let sp = make_spinner(&format!("Connecting to {}...", peer_label));

    let mut session = P2PSession::establish(
        &session_params.get_role("client"),
        session_params.peer.clone(),
        session_params.discover,
        session_params.port,
        device_id,
        capabilities,
        Some(config.clone()),
    )
    .await?;

    sp.finish_and_clear();
    eprintln!("✓ Connected  peer={}", session.peer_device_id());

    let result = tokio::select! {
        result = send_path(&mut session, &path, max_retries) => result,
        _ = signal::ctrl_c() => Err(anyhow::anyhow!("Transfer interrupted by user (Ctrl+C)")),
    };
    result
}

/// Work-stealing batch size: ~512 MB per pull from the queue.
/// Large files (> threshold) are taken one at a time.
/// Small files are grouped until the threshold is reached.
const BATCH_TARGET_BYTES: u64 = 512 * 1024 * 1024;

fn take_batch(queue: &mut VecDeque<FileMetadata>) -> Vec<FileMetadata> {
    let mut batch = Vec::new();
    let mut total = 0u64;

    while let Some(file) = queue.front() {
        // Always take at least 1 file; stop if adding next would exceed target
        if !batch.is_empty() && total + file.size > BATCH_TARGET_BYTES {
            break;
        }
        total += file.size;
        batch.push(queue.pop_front().unwrap());
    }

    batch
}

/// Multi-connection parallel send with work-stealing queue.
///
/// Files are sorted largest-first and put in a shared queue. Each connection
/// pulls batches until the queue is empty, eliminating stragglers caused by
/// static pre-assignment.
async fn handle_parallel_send(
    path: PathBuf,
    session_params: SessionParams,
    config: ConfigMessage,
    _max_retries: u32,
    parallel: usize,
) -> Result<()> {
    let sp = make_spinner(&format!("Scanning {}...", path.display()));
    let (base_path, mut all_files) = scan_folder_for_parallel(&path).await?;
    let total_files = all_files.len();
    let total_bytes: u64 = all_files.iter().map(|f| f.size).sum();
    sp.finish_and_clear();
    eprintln!(
        "✓ Scan complete  {} files  {}  ({} connections)",
        total_files,
        format_bandwidth(total_bytes),
        parallel
    );

    // Sort largest-first: large files distributed immediately, small files fill gaps
    all_files.sort_by(|a, b| b.size.cmp(&a.size));

    // Shared work-stealing queue
    let queue: Arc<tokio::sync::Mutex<VecDeque<FileMetadata>>> =
        Arc::new(tokio::sync::Mutex::new(VecDeque::from(all_files)));

    let multi = MultiProgress::new();

    let overall_bar = multi.add(ProgressBar::new(total_bytes));
    overall_bar.set_style(
        ProgressStyle::with_template(
            "  [Total ] {bar:35.yellow/white} {bytes}/{total_bytes} ({bytes_per_sec}, ETA: {eta})",
        )
        .unwrap()
        .progress_chars("█▉▊▋▌▍▎▏ "),
    );
    overall_bar.enable_steady_tick(std::time::Duration::from_millis(100));

    let peer_addr = session_params.peer.clone();
    let port = session_params.port;
    let role = session_params.get_role("client");
    let discover = session_params.discover;

    let mut handles = Vec::new();
    for idx in 0..parallel {
        let queue_clone = queue.clone();
        let config_clone = config.clone();
        let peer_clone = peer_addr.clone();
        let role_clone = role.clone();
        let base_path_clone = base_path.clone();
        let overall_clone = overall_bar.clone();

        // Per-connection bar: length grows as batches are taken from queue
        let conn_bar = multi.add(ProgressBar::new(0));
        conn_bar.set_style(
            ProgressStyle::with_template(&format!(
                "  [Conn {:>3}] {{bar:32.cyan/blue}} {{bytes}}/{{total_bytes}} ({{bytes_per_sec}}) {{msg}}",
                idx + 1
            ))
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏ "),
        );
        conn_bar.enable_steady_tick(std::time::Duration::from_millis(100));

        let handle = tokio::spawn(async move {
            let device_id = Uuid::new_v4();
            let capabilities = Capabilities::all();

            let mut session = P2PSession::establish(
                &role_clone,
                peer_clone,
                discover,
                port,
                device_id,
                capabilities,
                Some(config_clone),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Connection {}: {}", idx + 1, e))?;

            loop {
                let batch = {
                    let mut q = queue_clone.lock().await;
                    take_batch(&mut q)
                };
                if batch.is_empty() {
                    break;
                }

                // Extend bar length to include this batch
                let batch_bytes: u64 = batch.iter().map(|f| f.size).sum();
                let new_len = conn_bar.length().unwrap_or(0) + batch_bytes;
                conn_bar.set_length(new_len);

                let mut progress =
                    ProgressState::from_bars_persistent(conn_bar.clone(), overall_clone.clone());

                session
                    .send_file_group(&base_path_clone, batch, Some(&mut progress))
                    .await
                    .map_err(|e| anyhow::anyhow!("Connection {} failed: {}", idx + 1, e))?;
            }

            conn_bar.finish_with_message("done");
            Ok::<(), anyhow::Error>(())
        });

        handles.push(handle);
    }

    let mut errors = Vec::new();
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!("Connection {} failed: {}", idx + 1, e);
                errors.push(e);
            }
            Err(e) => {
                warn!("Connection {} panicked: {}", idx + 1, e);
                errors.push(anyhow::anyhow!("Task panic: {}", e));
            }
        }
    }

    overall_bar.finish_with_message("all done");

    if errors.is_empty() {
        eprintln!("✓ All transfers complete");
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "{} connection(s) failed: {}",
            errors.len(),
            errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        ))
    }
}

async fn send_path(session: &mut P2PSession, path: &Path, max_retries: u32) -> Result<()> {
    let base_name = path.file_name().unwrap().to_string_lossy().to_string();

    let config = session.config();
    let mode = if config.window_size == 1 { "sequential" } else { "windowed" };
    let retry_label = match max_retries {
        0 => "unlimited retries".to_string(),
        1 => "no retry".to_string(),
        n => format!("max {} retries", n),
    };

    eprintln!("↑ {}  mode={}  w={}  {}", base_name, mode, config.window_size, retry_label);

    let mut progress = p2p_core::progress::ProgressState::new(0);

    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: max_retries,
        initial_backoff_secs: 3,
        max_backoff_secs: 180,
        exponential: true,
    };

    session
        .send_path(path, &reconnect_config, Some(&mut progress))
        .await
        .map(|_| eprintln!("✓ Transfer complete"))
        .map_err(|e| e.into())
}
