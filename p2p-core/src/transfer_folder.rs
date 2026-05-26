//! Folder-level transfer orchestration.
//!
//! A folder transfer is a sequence of single-file transfers reusing the
//! same QUIC connection. After all files are sent, the sender emits a
//! `Complete` control message; per-file SHA-256s are exchanged via
//! `FileChecksum` control messages so both sides agree on integrity.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::time::{Instant, SystemTime};

use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

/// Reject paths the receiver should never write to: absolute paths,
/// `..` traversal, current-dir tricks, prefix components (Windows
/// drive letters / UNC roots), and anything else but bog-standard
/// `Normal` components. Returns the same path back as a [`PathBuf`]
/// once it has been confirmed safe.
pub fn sanitize_relative_path(p: &Path) -> Result<PathBuf> {
    if p.is_absolute() {
        return Err(Error::Protocol(format!(
            "rejecting absolute path in transfer: {}",
            p.display()
        )));
    }
    let mut clean = PathBuf::new();
    for comp in p.components() {
        match comp {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {
                return Err(Error::Protocol(format!(
                    "rejecting `.` component in transfer path: {}",
                    p.display()
                )))
            }
            Component::ParentDir => {
                return Err(Error::Protocol(format!(
                    "rejecting `..` component in transfer path: {}",
                    p.display()
                )))
            }
            Component::Prefix(_) | Component::RootDir => {
                return Err(Error::Protocol(format!(
                    "rejecting drive/root component in transfer path: {}",
                    p.display()
                )))
            }
        }
    }
    if clean.as_os_str().is_empty() {
        return Err(Error::Protocol("transfer path is empty".to_string()));
    }
    Ok(clean)
}

use crate::bandwidth;
use crate::error::{Error, Result};
use crate::network::quic::QuicConnection;
use crate::progress::ProgressState;
use crate::protocol::{
    CompleteMessage, ConfigMessage, FileChecksumMessage, FileMetadata, Message, ResumePoint,
    TransferInfo,
};
use crate::transfer_file::{chunk_count, validate_file_size, FileTransferSession};

/// Statistics emitted at end of a folder transfer.
#[derive(Debug, Clone)]
pub struct TransferStats {
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
    pub duration_secs: f64,
    pub compression_ratio: f64,
    pub compression_percent: f64,
    pub network_speed_mbps: f64,
    pub felt_speed_mbps: f64,
}

/// What was actually transferred — returned from `receive_folder` / `send`
/// so callers can record accurate history entries instead of placeholders
/// like the output directory path (finding 2.3).
#[derive(Debug, Clone, Default)]
pub struct TransferSummary {
    /// The top-level item name as agreed during TransferInfo (single
    /// filename for a single-file send, folder name for a folder send).
    pub root_name: String,
    /// Relative paths of every file that was transferred (not the .partial
    /// names, not the absolute output paths).
    pub files: Vec<String>,
    /// Total bytes whose transfer was completed in this session (excludes
    /// resumed chunks counted in a prior session — `bytes_transferred`
    /// before this `receive_to` call).
    pub bytes: u64,
}

/// Accept policy for an incoming transfer. The receiver consults this
/// after reading TransferInfo but before sending `Ready`. `Reject` causes
/// the receiver to send `Cancel`; the sender then returns Ok without
/// opening any chunk streams (finding 2.1).
#[derive(Debug, Clone, Copy)]
pub enum AcceptDecision {
    Accept,
    Reject,
}

/// Callback fired after each file completes so the caller can persist state.
pub type StateCallback = std::sync::Arc<dyn Fn(&FolderTransferState) + Send + Sync>;

/// Folder transfer session — orchestrates many single-file transfers over
/// one borrowed [`QuicConnection`].
pub struct FolderTransferSession<'a> {
    connection: &'a mut QuicConnection,
    config: ConfigMessage,
    transfer_id: Uuid,
    state_callback: Option<StateCallback>,
    total_compressed_bytes: u64,
    transfer_start: Option<Instant>,
}

impl<'a> FolderTransferSession<'a> {
    pub fn new(
        connection: &'a mut QuicConnection,
        config: ConfigMessage,
        transfer_id: Uuid,
    ) -> Self {
        Self {
            connection,
            config,
            transfer_id,
            state_callback: None,
            total_compressed_bytes: 0,
            transfer_start: None,
        }
    }

    pub fn set_state_callback(&mut self, callback: StateCallback) {
        self.state_callback = Some(callback);
    }

    fn calc_compression_stats(&self, total_bytes: u64) -> (f64, f64) {
        let ratio = if total_bytes > 0 {
            total_bytes as f64 / self.total_compressed_bytes as f64
        } else {
            1.0
        };
        let percent = if total_bytes >= self.total_compressed_bytes {
            (total_bytes - self.total_compressed_bytes) as f64 / total_bytes as f64 * 100.0
        } else {
            -((self.total_compressed_bytes - total_bytes) as f64 / total_bytes as f64 * 100.0)
        };
        (ratio, percent)
    }

    fn display_transfer_stats(
        &self,
        total_files: usize,
        total_bytes: u64,
        duration_secs: f64,
        is_sender: bool,
    ) {
        info!("Transfer Statistics:");
        let action = if is_sender { "sent" } else { "received" };
        let mb_per_sec = |bytes: u64| {
            if duration_secs > 0.0 {
                bytes as f64 / duration_secs / 1_048_576.0
            } else {
                0.0
            }
        };

        if self.config.compression_enabled && self.total_compressed_bytes > 0 {
            let (ratio, percent) = self.calc_compression_stats(total_bytes);
            let direction = if is_sender { "->" } else { "<-" };
            let (label, abs_percent) = if percent >= 0.0 {
                (
                    format!("{percent:.1}% saved, {ratio:.2}x compression"),
                    percent,
                )
            } else {
                (format!("{:.1}% overhead", -percent), -percent)
            };
            let _ = abs_percent;
            info!(
                "   Data: {} {} {} ({})",
                bandwidth::format_bandwidth(total_bytes),
                direction,
                bandwidth::format_bandwidth(self.total_compressed_bytes),
                label,
            );
            info!(
                "   Speed: {:.2} MB/s network, {:.2} MB/s throughput",
                mb_per_sec(self.total_compressed_bytes),
                mb_per_sec(total_bytes),
            );
        } else if duration_secs > 0.0 {
            info!("   Speed: {:.2} MB/s", mb_per_sec(total_bytes));
        }
        info!(
            "Folder transfer complete: {} files, {} {}",
            total_files,
            bandwidth::format_bandwidth(total_bytes),
            action
        );
    }

    /// Send a file or folder, updating `state` as chunks complete (for resume).
    pub async fn send(
        &mut self,
        path: &Path,
        state: &mut FolderTransferState,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        self.transfer_start = Some(Instant::now());
        self.total_compressed_bytes = 0;

        let resume_point = if !state.files.is_empty() {
            info!(
                "Resuming transfer: {} of {} files done",
                state.completed_files.len(),
                state.files.len()
            );
            if let Some(next_file) = state.next_file() {
                let completed = state.get_completed_chunks(next_file);
                if !completed.is_empty() {
                    Some(ResumePoint {
                        transfer_id: self.transfer_id,
                        file_index: next_file as u32,
                        completed_chunks: completed.to_vec(),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            info!("Starting transfer: {:?}", path);

            let base_name = path
                .file_name()
                .ok_or_else(|| Error::Protocol("Invalid path".to_string()))?
                .to_string_lossy()
                .to_string();

            let files = if path.is_file() {
                let metadata = fs::metadata(path).await?;
                let size = metadata.len();
                let modified = metadata
                    .modified()
                    .unwrap_or(SystemTime::UNIX_EPOCH)
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let file_name = path.file_name().unwrap().to_string_lossy().to_string();
                vec![(
                    PathBuf::from(file_name.clone()),
                    FileMetadata {
                        path: file_name,
                        size,
                        modified,
                        checksum: [0u8; 32],
                    },
                )]
            } else if path.is_dir() {
                let files = self.scan_folder(path).await?;
                if files.is_empty() {
                    return Err(Error::Protocol("Folder is empty".to_string()));
                }
                files
            } else {
                return Err(Error::Protocol(
                    "Path is neither a file nor a directory".to_string(),
                ));
            };

            let file_list: Vec<FileMetadata> = files.iter().map(|(_, m)| m.clone()).collect();
            *state = FolderTransferState::new(self.transfer_id, base_name, file_list, &self.config);
            None
        };

        let total_files = state.files.len();
        let total_bytes = state.total_bytes;
        if let Some(ref mut p) = progress {
            p.set_total_bytes(total_bytes);
        }

        let is_resuming = resume_point.is_some();
        let completed_files: Vec<u32> = state.completed_files.iter().map(|i| *i as u32).collect();
        let transfer_info = TransferInfo {
            transfer_id: self.transfer_id,
            items: state.files.clone(),
            resume_from: resume_point,
            completed_files,
        };
        self.connection
            .send_message(&Message::TransferInfo(Box::new(transfer_info)))
            .await?;

        match self.connection.recv_message().await {
            Ok(Message::Ready) => {}
            Ok(Message::Cancel) => {
                info!("Receiver rejected the transfer; no chunks sent.");
                return Err(Error::Cancelled);
            }
            Ok(msg) => return Err(Error::Protocol(format!("Expected Ready, got {:?}", msg))),
            // The receiver may close the connection immediately after
            // sending Cancel without waiting for our acknowledgement —
            // that surfaces here as Disconnected/Network/Quic instead of
            // a clean Cancel message. Treat as a rejection.
            Err(e) if matches!(&e, Error::Disconnected | Error::Network(_) | Error::Quic(_)) => {
                info!("Receiver disconnected before sending Ready; treating as cancel.");
                return Err(Error::Cancelled);
            }
            Err(e) => return Err(e),
        }
        debug!(
            "Receiver ready, {}",
            if is_resuming { "resuming" } else { "starting" }
        );

        let base_path = if path.is_file() {
            path.parent()
                .ok_or_else(|| Error::Protocol("File has no parent directory".to_string()))?
        } else {
            path.parent().unwrap_or(path)
        };

        for file_index in 0..state.files.len() {
            if state.completed_files.contains(&file_index) {
                continue;
            }
            let file_meta = &state.files[file_index];
            let relative_path = PathBuf::from(&file_meta.path);
            let full_path = base_path.join(&relative_path);
            let completed_chunks = state.get_completed_chunks(file_index).to_vec();

            let chunk_callback = |chunk_index: u64| {
                state.mark_chunk_complete(file_index, chunk_index);
            };

            self.send_single_file(
                &full_path,
                file_index as u32,
                &completed_chunks,
                progress.as_deref_mut(),
                Some(chunk_callback),
            )
            .await?;

            state.mark_file_complete(file_index);
            state.current_file = state.next_file();
            if let Some(cb) = &self.state_callback {
                cb(state);
            }
            trace!("File {} complete", relative_path.display());
        }

        let duration = self.transfer_start.map(|s| s.elapsed()).unwrap_or_default();
        let complete = CompleteMessage {
            transfer_id: self.transfer_id,
            total_bytes,
            duration_ms: duration.as_millis() as u64,
        };
        self.connection
            .send_message(&Message::Complete(complete))
            .await?;

        if let Some(ref mut p) = progress {
            p.finish();
        }

        self.display_transfer_stats(total_files, total_bytes, duration.as_secs_f64(), true);
        Ok(())
    }

    /// Receive a folder from the peer.
    ///
    /// `accept_decision` is invoked after parsing the TransferInfo but
    /// before any data flows — return `Reject` to send `Cancel` and skip
    /// the transfer. `Ok(TransferSummary::default())` is returned on
    /// rejection so the caller can record an "interrupted" history entry
    /// without losing the file list.
    pub async fn receive_folder(
        &mut self,
        output_dir: &Path,
        state_path: Option<&Path>,
        accept_decision: impl FnOnce(&TransferInfo) -> AcceptDecision,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<TransferSummary> {
        let transfer_info = match self.connection.recv_message().await? {
            Message::TransferInfo(info) => *info,
            msg => {
                return Err(Error::Protocol(format!(
                    "Expected TransferInfo, got {:?}",
                    msg
                )))
            }
        };
        if transfer_info.items.is_empty() {
            return Err(Error::Protocol("No files in transfer".to_string()));
        }
        // Reject manifests with absurd per-file sizes before opening any
        // stream — a hostile peer could otherwise pin us in
        // accept_uni() forever by advertising u64::MAX (finding 4.1).
        for f in &transfer_info.items {
            validate_file_size(f.size)?;
        }

        if matches!(accept_decision(&transfer_info), AcceptDecision::Reject) {
            info!("Transfer rejected by accept policy; notifying sender");
            self.connection.send_message(&Message::Cancel).await?;
            return Ok(TransferSummary::default());
        }

        info!("Starting receive to: {:?}", output_dir);
        let is_resume = transfer_info.resume_from.is_some();

        if let Some(state_file) = state_path {
            if state_file.exists() {
                match FolderTransferState::load_from_file(state_file).await {
                    Ok(existing) if existing.transfer_id == transfer_info.transfer_id => {
                        info!(
                            "Detected existing transfer {}, resuming automatically",
                            transfer_info.transfer_id
                        );
                    }
                    Ok(_) => {}
                    Err(e) => warn!("Failed to load existing state: {}", e),
                }
            }
        }

        self.transfer_id = transfer_info.transfer_id;
        self.transfer_start = Some(Instant::now());
        self.total_compressed_bytes = 0;

        let total_bytes: u64 = transfer_info.items.iter().map(|f| f.size).sum();

        let mut already_transferred = 0u64;
        if let Some(ref resume_point) = transfer_info.resume_from {
            let file_index = resume_point.file_index as usize;
            for i in 0..file_index.min(transfer_info.items.len()) {
                already_transferred += transfer_info.items[i].size;
            }
            if file_index < transfer_info.items.len() {
                let current_size = transfer_info.items[file_index].size;
                let total_chunks = chunk_count(current_size, self.config.chunk_size);
                let completed_chunks = resume_point.completed_chunks.len() as u64;
                let added = if completed_chunks < total_chunks {
                    completed_chunks * self.config.chunk_size as u64
                } else {
                    current_size
                };
                already_transferred += added;
            }
            info!(
                "Resume: {} bytes already transferred ({:.1}%)",
                already_transferred,
                (already_transferred as f64 / total_bytes as f64) * 100.0
            );
        }

        if let Some(ref mut p) = progress {
            p.set_total_bytes(total_bytes);
            if already_transferred > 0 {
                p.add_bytes(already_transferred);
            }
        }

        fs::create_dir_all(output_dir).await?;
        self.connection.send_message(&Message::Ready).await?;

        let total_files = transfer_info.items.len();
        let skip: std::collections::HashSet<u32> =
            transfer_info.completed_files.iter().copied().collect();
        for (file_index, file_meta) in transfer_info.items.iter().enumerate() {
            if skip.contains(&(file_index as u32)) {
                debug!(
                    "Skipping file {} (sender marked complete in prior session): {}",
                    file_index, file_meta.path
                );
                continue;
            }
            let relative_path = sanitize_relative_path(Path::new(&file_meta.path))?;
            let full_path = output_dir.join(&relative_path);
            info!(
                "Receiving file {}/{}: {}",
                file_index + 1,
                total_files,
                relative_path.display()
            );
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent).await?;
            }

            let total_chunks = chunk_count(file_meta.size, self.config.chunk_size);
            let already_sent = transfer_info
                .resume_from
                .as_ref()
                .filter(|rp| rp.file_index as usize == file_index)
                .map(|rp| rp.completed_chunks.len() as u64)
                .unwrap_or(0);
            let streams_to_receive = total_chunks.saturating_sub(already_sent);

            self.receive_single_file(
                &full_path,
                file_index as u32,
                total_chunks,
                streams_to_receive,
                progress.as_deref_mut(),
            )
            .await?;
            trace!("File {} complete", relative_path.display());
        }

        match self.connection.recv_message().await? {
            Message::Complete(_) => {}
            msg => warn!("Expected Complete message, got {:?}", msg),
        }

        if let Some(ref mut p) = progress {
            p.finish();
        }
        let duration = self.transfer_start.map(|s| s.elapsed()).unwrap_or_default();
        self.display_transfer_stats(total_files, total_bytes, duration.as_secs_f64(), false);

        let _ = is_resume;

        // Build a summary the CLI can record in history. `files` is the
        // per-file relative-path list as agreed at TransferInfo time —
        // covers both the "single file" send and the "folder of many
        // files" send without distinguishing.
        let summary = TransferSummary {
            root_name: transfer_info
                .items
                .first()
                .map(|f| f.path.clone())
                .unwrap_or_default(),
            files: transfer_info.items.iter().map(|f| f.path.clone()).collect(),
            bytes: total_bytes.saturating_sub(already_transferred),
        };
        Ok(summary)
    }

    async fn send_single_file<F>(
        &mut self,
        path: &Path,
        file_index: u32,
        completed_chunks: &[u64],
        progress: Option<&mut ProgressState>,
        chunk_complete_callback: Option<F>,
    ) -> Result<()>
    where
        F: FnMut(u64),
    {
        let mut file_session = FileTransferSession::new(self.connection, self.config.clone());

        let sender_checksum = file_session
            .send_file(path, completed_chunks, chunk_complete_callback, progress)
            .await?;
        self.total_compressed_bytes += file_session.compressed_bytes_sent;

        let checksum_msg = FileChecksumMessage {
            transfer_id: self.transfer_id,
            file_index,
            checksum: sender_checksum,
        };
        self.connection
            .send_message(&Message::FileChecksum(checksum_msg))
            .await?;

        match self.connection.recv_message().await? {
            Message::FileChecksum(peer_msg) => {
                if peer_msg.checksum != sender_checksum {
                    return Err(Error::Verification(format!(
                        "File checksum mismatch for file {}: sender={:02x?}, receiver={:02x?}",
                        file_index,
                        &sender_checksum[..8],
                        &peer_msg.checksum[..8]
                    )));
                }
                debug!(
                    "File {} checksum verified: {:02x?}",
                    file_index,
                    &sender_checksum[..8]
                );
            }
            msg => {
                return Err(Error::Protocol(format!(
                    "Expected FileChecksum, got {:?}",
                    msg
                )))
            }
        }
        Ok(())
    }

    async fn receive_single_file(
        &mut self,
        path: &Path,
        file_index: u32,
        total_chunks: u64,
        streams_to_receive: u64,
        progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        let mut file_session = FileTransferSession::new(self.connection, self.config.clone());

        let receiver_checksum = file_session
            .receive_file(
                path,
                total_chunks,
                streams_to_receive,
                None::<fn(u64)>,
                progress,
            )
            .await?;

        let our_msg = FileChecksumMessage {
            transfer_id: self.transfer_id,
            file_index,
            checksum: receiver_checksum,
        };
        self.connection
            .send_message(&Message::FileChecksum(our_msg))
            .await?;

        let sender_checksum = match self.connection.recv_message().await? {
            Message::FileChecksum(peer_msg) => {
                if peer_msg.file_index != file_index {
                    return Err(Error::Protocol(format!(
                        "File index mismatch: expected {}, got {}",
                        file_index, peer_msg.file_index
                    )));
                }
                peer_msg.checksum
            }
            msg => {
                return Err(Error::Protocol(format!(
                    "Expected FileChecksum, got {:?}",
                    msg
                )))
            }
        };

        if sender_checksum != receiver_checksum {
            return Err(Error::Verification(format!(
                "File {} checksum mismatch: sender={:02x?}, receiver={:02x?}",
                file_index,
                &sender_checksum[..8],
                &receiver_checksum[..8]
            )));
        }
        Ok(())
    }

    async fn scan_folder(&self, folder_path: &Path) -> Result<Vec<(PathBuf, FileMetadata)>> {
        let mut files = Vec::new();
        let base_path = folder_path.parent().unwrap_or(folder_path);
        let mut stack: std::collections::VecDeque<PathBuf> = std::collections::VecDeque::new();
        stack.push_back(folder_path.to_path_buf());
        while let Some(current) = stack.pop_front() {
            let mut entries = fs::read_dir(&current).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let metadata = entry.metadata().await?;
                if metadata.is_file() {
                    let relative_path = path
                        .strip_prefix(base_path)
                        .map_err(|e| Error::Protocol(format!("Invalid path: {}", e)))?
                        .to_path_buf();
                    let relative_path = sanitize_relative_path(&relative_path)?;
                    let size = metadata.len();
                    let modified = metadata
                        .modified()
                        .unwrap_or(SystemTime::UNIX_EPOCH)
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    files.push((
                        relative_path.clone(),
                        FileMetadata {
                            path: relative_path.to_string_lossy().to_string(),
                            size,
                            modified,
                            checksum: [0u8; 32],
                        },
                    ));
                    trace!("Found file: {} ({} bytes)", path.display(), size);
                } else if metadata.is_dir() {
                    stack.push_back(path);
                }
            }
        }
        Ok(files)
    }
}

/// On-disk state for chunk-level resume. Embeds the negotiated
/// [`ConfigMessage`] verbatim so resume rehydrates the same chunk_size and
/// compression settings the original session used — without this the
/// `.partial` on disk (laid out under the original chunk_size) and the
/// resumed session's offsets disagree, silently corrupting the file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderTransferState {
    pub transfer_id: Uuid,
    pub folder_name: String,
    pub files: Vec<FileMetadata>,
    pub completed_files: Vec<usize>,
    pub current_file: Option<usize>,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub file_chunks: HashMap<usize, Vec<u64>>,
    /// Negotiated config snapshot — must match what the `.partial` on
    /// disk was laid out with. Resume reads `config.chunk_size` directly.
    pub config: ConfigMessage,
}

impl FolderTransferState {
    pub fn new(
        transfer_id: Uuid,
        folder_name: String,
        files: Vec<FileMetadata>,
        config: &ConfigMessage,
    ) -> Self {
        let total_bytes = files.iter().map(|f| f.size).sum();
        Self {
            transfer_id,
            folder_name,
            files,
            completed_files: Vec::new(),
            current_file: None,
            total_bytes,
            transferred_bytes: 0,
            file_chunks: HashMap::new(),
            config: config.clone(),
        }
    }

    pub fn mark_chunk_complete(&mut self, file_index: usize, chunk_index: u64) {
        self.file_chunks
            .entry(file_index)
            .or_default()
            .push(chunk_index);
    }

    pub fn get_completed_chunks(&self, file_index: usize) -> &[u64] {
        self.file_chunks
            .get(&file_index)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn mark_file_complete(&mut self, file_index: usize) {
        if !self.completed_files.contains(&file_index) {
            self.completed_files.push(file_index);
            if file_index < self.files.len() {
                self.transferred_bytes += self.files[file_index].size;
            }
        }
    }

    pub fn next_file(&self) -> Option<usize> {
        (0..self.files.len()).find(|i| !self.completed_files.contains(i))
    }

    pub fn is_complete(&self) -> bool {
        self.completed_files.len() == self.files.len()
    }

    pub fn progress_percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.transferred_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }

    pub async fn save_to_file(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| Error::Protocol(format!("Failed to serialize state: {}", e)))?;
        fs::write(path, json).await?;
        Ok(())
    }

    pub async fn load_from_file(path: &Path) -> Result<Self> {
        let json = fs::read_to_string(path).await?;
        serde_json::from_str(&json)
            .map_err(|e| Error::Protocol(format!("Failed to deserialize state: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cfg(chunk_size: u32) -> ConfigMessage {
        ConfigMessage {
            compression_enabled: false,
            compression_level: 3,
            adaptive_compression: false,
            chunk_size,
            bandwidth_limit: 0,
        }
    }

    #[tokio::test]
    async fn folder_transfer_state_tracks_files() {
        let files = vec![
            FileMetadata {
                path: "a.txt".to_string(),
                size: 100,
                modified: 0,
                checksum: [0u8; 32],
            },
            FileMetadata {
                path: "b.txt".to_string(),
                size: 200,
                modified: 0,
                checksum: [0u8; 32],
            },
        ];
        let mut state =
            FolderTransferState::new(Uuid::new_v4(), "x".to_string(), files, &make_cfg(65536));
        assert_eq!(state.total_bytes, 300);
        assert_eq!(state.next_file(), Some(0));

        state.mark_file_complete(0);
        assert_eq!(state.transferred_bytes, 100);
        assert_eq!(state.next_file(), Some(1));

        state.mark_chunk_complete(1, 7);
        assert_eq!(state.get_completed_chunks(1), &[7u64]);

        state.mark_file_complete(1);
        assert!(state.is_complete());
        assert_eq!(state.progress_percentage(), 100.0);
    }

    #[test]
    fn sanitize_accepts_normal_relative_paths() {
        let ok = sanitize_relative_path(Path::new("dir/sub/file.txt")).unwrap();
        assert_eq!(ok, PathBuf::from("dir/sub/file.txt"));
        let plain = sanitize_relative_path(Path::new("file.txt")).unwrap();
        assert_eq!(plain, PathBuf::from("file.txt"));
    }

    #[test]
    fn sanitize_rejects_parent_dir() {
        let err = sanitize_relative_path(Path::new("../evil")).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
        let err = sanitize_relative_path(Path::new("a/../../evil")).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn sanitize_rejects_current_dir_marker() {
        let err = sanitize_relative_path(Path::new("./evil")).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn sanitize_rejects_absolute_path() {
        #[cfg(windows)]
        let abs = Path::new(r"C:\Windows\System32\evil.dll");
        #[cfg(not(windows))]
        let abs = Path::new("/etc/passwd");
        let err = sanitize_relative_path(abs).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    /// Finding 1.2: `FolderTransferState` must carry the original
    /// negotiated chunk_size (and other compression knobs) so resume can
    /// rehydrate the same `ConfigMessage` instead of falling back to the
    /// default. Otherwise the `.partial` on disk (laid out under the
    /// original chunk_size) and the new session's offsets disagree and
    /// every chunk lands at the wrong file offset.
    #[tokio::test]
    async fn state_remembers_negotiated_chunk_size_across_serde_roundtrip() {
        let files = vec![FileMetadata {
            path: "x.bin".into(),
            size: 4 * 1024 * 1024,
            modified: 0,
            checksum: [0u8; 32],
        }];
        let cfg = make_cfg(1024 * 1024);
        let state = FolderTransferState::new(Uuid::new_v4(), "f".into(), files, &cfg);
        assert_eq!(state.config.chunk_size, 1024 * 1024);

        let json = serde_json::to_string(&state).unwrap();
        let round: FolderTransferState = serde_json::from_str(&json).unwrap();
        assert_eq!(round.config.chunk_size, 1024 * 1024);
        assert_eq!(round.config.compression_enabled, cfg.compression_enabled);
        assert_eq!(round.config.compression_level, cfg.compression_level);
        assert_eq!(round.config.adaptive_compression, cfg.adaptive_compression);
        assert_eq!(round.config.bandwidth_limit, cfg.bandwidth_limit);
    }

    /// Finding 1.1: multi-file folder resume must not deadlock when the
    /// sender skips already-completed files. Before the fix the sender
    /// opened zero streams for files in `state.completed_files` while the
    /// receiver iterated every item in `transfer_info.items` and blocked
    /// on `accept_uni()` forever. With the new `TransferInfo.completed_files`
    /// field the receiver knows which file indices to skip.
    #[tokio::test]
    async fn multi_file_resume_with_completed_files_does_not_deadlock() {
        use crate::identity::Identity;
        use crate::network::quic::QuicEndpoint;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        use std::sync::Arc;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let src_dir = dir.path().join("src");
        let dst_dir = dir.path().join("dst");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        tokio::fs::create_dir_all(&dst_dir).await.unwrap();

        let chunk_size = 64usize;
        // 3 files, each one chunk wide. After resume the sender pretends
        // file index 0 is already complete.
        let names = ["a.bin", "b.bin", "c.bin"];
        let bodies: Vec<Vec<u8>> = (0..names.len())
            .map(|i| vec![i as u8; chunk_size])
            .collect();
        for (n, body) in names.iter().zip(bodies.iter()) {
            tokio::fs::write(src_dir.join(n), body).await.unwrap();
        }

        let cfg = make_cfg(chunk_size as u32);

        let server_id = Arc::new(Identity::generate().unwrap());
        let server_fp = server_id.fingerprint();
        let server_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            server_id.clone(),
        )
        .unwrap();
        let server_addr = server_ep.local_addr().unwrap();

        let dst_recv = dst_dir.clone();
        let cfg_recv = cfg.clone();
        let recv_task = tokio::spawn(async move {
            let mut conn = server_ep.accept().await.unwrap();
            let _ = conn.recv_message().await.unwrap(); // initial Ping to drive accept_bi
            let mut session = FolderTransferSession::new(&mut conn, cfg_recv, Uuid::new_v4());
            session
                .receive_folder(&dst_recv, None, |_| AcceptDecision::Accept, None)
                .await
        });

        let client_id = Arc::new(Identity::generate().unwrap());
        let client_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            client_id,
        )
        .unwrap();
        let mut conn = client_ep.connect(server_addr, server_fp).await.unwrap();
        conn.send_message(&crate::protocol::Message::Ping)
            .await
            .unwrap();

        // Build state.files with the same `src/<name>` relative paths that
        // scan_folder would emit (sender resolves base_path as src_dir's
        // parent and joins these relative paths).
        let files: Vec<FileMetadata> = names
            .iter()
            .zip(bodies.iter())
            .map(|(n, b)| FileMetadata {
                path: format!("src/{n}"),
                size: b.len() as u64,
                modified: 0,
                checksum: [0u8; 32],
            })
            .collect();
        let mut state = FolderTransferState::new(Uuid::new_v4(), "src".into(), files, &cfg);
        state.mark_file_complete(0); // pretend file 0 already shipped

        let mut session = FolderTransferSession::new(&mut conn, cfg, state.transfer_id);
        let send_path = src_dir.clone();
        let send_fut = session.send(&send_path, &mut state, None);

        let result = tokio::time::timeout(Duration::from_secs(5), async {
            send_fut.await.unwrap();
            recv_task.await.unwrap().unwrap();
        })
        .await;

        result.expect("multi-file resume must finish within 5 s — receiver hung waiting for streams the sender skipped");
        // The receiver writes non-skipped files under output_dir/src/<name>.
        let recv_root = dst_dir.join("src");
        for (i, (n, body)) in names.iter().zip(bodies.iter()).enumerate() {
            if i == 0 {
                // Skipped — receiver never wrote it; nothing to assert.
                continue;
            }
            let got = tokio::fs::read(recv_root.join(n)).await.unwrap();
            assert_eq!(got, *body, "file {n} mismatch");
        }
    }

    /// Finding 2.1 / 2.3: the reject path sends Cancel to the sender,
    /// the sender returns Err(Cancelled) without opening any chunk
    /// streams, and the receiver returns an empty TransferSummary so
    /// the CLI can log an interrupted history entry without inventing
    /// a fake "received the output dir" placeholder.
    #[tokio::test]
    async fn receive_folder_reject_sends_cancel_and_returns_empty_summary() {
        use crate::identity::Identity;
        use crate::network::quic::QuicEndpoint;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        use std::sync::Arc;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let src_dir = dir.path().join("src");
        let dst_dir = dir.path().join("dst");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        tokio::fs::write(src_dir.join("only.bin"), vec![7u8; 32])
            .await
            .unwrap();

        let cfg = make_cfg(64);

        let server_id = Arc::new(Identity::generate().unwrap());
        let server_fp = server_id.fingerprint();
        let server_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            server_id.clone(),
        )
        .unwrap();
        let server_addr = server_ep.local_addr().unwrap();

        let dst_recv = dst_dir.clone();
        let cfg_recv = cfg.clone();
        let recv_task = tokio::spawn(async move {
            let mut conn = server_ep.accept().await.unwrap();
            let _ = conn.recv_message().await.unwrap();
            let mut session = FolderTransferSession::new(&mut conn, cfg_recv, Uuid::new_v4());
            session
                .receive_folder(&dst_recv, None, |_info| AcceptDecision::Reject, None)
                .await
        });

        let client_id = Arc::new(Identity::generate().unwrap());
        let client_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            client_id,
        )
        .unwrap();
        let mut conn = client_ep.connect(server_addr, server_fp).await.unwrap();
        conn.send_message(&crate::protocol::Message::Ping)
            .await
            .unwrap();

        let files = vec![FileMetadata {
            path: "src/only.bin".into(),
            size: 32,
            modified: 0,
            checksum: [0u8; 32],
        }];
        let mut state = FolderTransferState::new(Uuid::new_v4(), "src".into(), files, &cfg);
        let mut session = FolderTransferSession::new(&mut conn, cfg, state.transfer_id);
        let send_result = tokio::time::timeout(
            Duration::from_secs(5),
            session.send(&src_dir, &mut state, None),
        )
        .await
        .expect("send must return promptly on receiver reject")
        .unwrap_err();
        assert!(
            matches!(send_result, Error::Cancelled),
            "sender must surface Cancel as Error::Cancelled, got {send_result:?}"
        );

        let recv_summary = tokio::time::timeout(Duration::from_secs(5), recv_task)
            .await
            .expect("receiver must return promptly on reject")
            .unwrap()
            .unwrap();
        assert!(
            recv_summary.files.is_empty(),
            "rejected transfer's summary must be empty so CLI logs an interrupted entry, got {recv_summary:?}"
        );
    }

    /// Finding 2.3: receiver summary on success carries the actual
    /// per-file list from TransferInfo, not the output directory path.
    /// The CLI's history record then names the real files.
    #[tokio::test]
    async fn receive_folder_accept_returns_summary_with_per_file_list() {
        use crate::identity::Identity;
        use crate::network::quic::QuicEndpoint;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        use std::sync::Arc;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let src_dir = dir.path().join("src");
        let dst_dir = dir.path().join("dst");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        tokio::fs::write(src_dir.join("alpha.bin"), vec![1u8; 64])
            .await
            .unwrap();
        tokio::fs::write(src_dir.join("beta.bin"), vec![2u8; 64])
            .await
            .unwrap();

        let cfg = make_cfg(64);

        let server_id = Arc::new(Identity::generate().unwrap());
        let server_fp = server_id.fingerprint();
        let server_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            server_id.clone(),
        )
        .unwrap();
        let server_addr = server_ep.local_addr().unwrap();

        let dst_recv = dst_dir.clone();
        let cfg_recv = cfg.clone();
        let recv_task = tokio::spawn(async move {
            let mut conn = server_ep.accept().await.unwrap();
            let _ = conn.recv_message().await.unwrap();
            let mut session = FolderTransferSession::new(&mut conn, cfg_recv, Uuid::new_v4());
            session
                .receive_folder(&dst_recv, None, |_| AcceptDecision::Accept, None)
                .await
        });

        let client_id = Arc::new(Identity::generate().unwrap());
        let client_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            client_id,
        )
        .unwrap();
        let mut conn = client_ep.connect(server_addr, server_fp).await.unwrap();
        conn.send_message(&crate::protocol::Message::Ping)
            .await
            .unwrap();

        let files = vec![
            FileMetadata {
                path: "src/alpha.bin".into(),
                size: 64,
                modified: 0,
                checksum: [0u8; 32],
            },
            FileMetadata {
                path: "src/beta.bin".into(),
                size: 64,
                modified: 0,
                checksum: [0u8; 32],
            },
        ];
        let mut state = FolderTransferState::new(Uuid::new_v4(), "src".into(), files, &cfg);
        let mut session = FolderTransferSession::new(&mut conn, cfg, state.transfer_id);

        let result = tokio::time::timeout(Duration::from_secs(5), async {
            session.send(&src_dir, &mut state, None).await.unwrap();
            recv_task.await.unwrap().unwrap()
        })
        .await
        .expect("transfer must complete within 5 s");

        assert_eq!(result.files, vec!["src/alpha.bin", "src/beta.bin"]);
        assert_eq!(result.root_name, "src/alpha.bin");
        assert_eq!(result.bytes, 128);
    }

    #[test]
    fn sanitize_rejects_empty_path() {
        let err = sanitize_relative_path(Path::new("")).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }
}
