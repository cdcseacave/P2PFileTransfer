//! History command handler

use anyhow::Result;
use p2p_core::history::{TransferDirection, TransferHistory, TransferStatus};
use tracing::info;

pub async fn handle_history(
    limit: usize,
    direction: Option<String>,
    completed: bool,
    failed: bool,
) -> Result<()> {
    info!("📜 Transfer History\n");

    // Load history
    let history_path = TransferHistory::default_path();
    let history = if history_path.exists() {
        TransferHistory::load_from_file(&history_path).await?
    } else {
        info!("No transfer history found.");
        return Ok(());
    };

    // Filter records
    let mut records: Vec<_> = history.records().iter().collect();

    // Filter by direction
    if let Some(dir) = direction {
        let dir_filter = match dir.to_lowercase().as_str() {
            "send" => TransferDirection::Send,
            "receive" | "recv" => TransferDirection::Receive,
            _ => {
                anyhow::bail!("Invalid direction. Use 'send' or 'receive'");
            }
        };
        records.retain(|r| r.direction == dir_filter);
    }

    // Filter by status
    if completed {
        records.retain(|r| r.status == TransferStatus::Completed);
    } else if failed {
        records.retain(|r| r.status == TransferStatus::Failed);
    }

    // Sort by start time (most recent first)
    records.sort_by(|a, b| b.start_time.cmp(&a.start_time));

    // Limit results
    let records: Vec<_> = records.into_iter().take(limit).collect();

    if records.is_empty() {
        info!("No transfers found matching the filters.");
        return Ok(());
    }

    // Display records
    info!("Found {} transfer(s):\n", records.len());

    for record in records {
        let direction_icon = match record.direction {
            TransferDirection::Send => "📤",
            TransferDirection::Receive => "📥",
        };

        let status_icon = match record.status {
            TransferStatus::Completed => "✅",
            TransferStatus::Interrupted => "⏸️",
            TransferStatus::Failed => "❌",
        };

        // Format timestamp
        let datetime = format_timestamp(record.start_time);

        // Format size
        let size_str = format_bytes(record.bytes_transferred);

        // Format duration
        let duration_str = format_duration(record.duration_secs);

        info!(
            "{} {} Transfer {}",
            direction_icon, status_icon, record.transfer_id
        );
        info!("  Started:   {}", datetime);
        info!("  Peer:      {}", record.peer_address);
        info!("  Files:     {} file(s)", record.files.len());
        info!("  Size:      {}", size_str);
        info!("  Duration:  {}", duration_str);
        info!("  Status:    {:?}", record.status);

        if !record.files.is_empty() && record.files.len() <= 5 {
            info!("  Files:");
            for file in &record.files {
                info!("    - {}", file);
            }
        } else if record.files.len() > 5 {
            info!(
                "  Files: {} files (use details command to see all)",
                record.files.len()
            );
        }

        info!("");
    }

    Ok(())
}

fn format_timestamp(unix_secs: u64) -> String {
    use chrono::{DateTime, Local};

    let datetime = DateTime::from_timestamp(unix_secs as i64, 0)
        .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
    let local: DateTime<Local> = datetime.into();
    local.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];

    if bytes == 0 {
        return "0 B".to_string();
    }

    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_idx])
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m {}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    }
}
