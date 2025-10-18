//! History tab view

use crate::{
    message::Message,
    state::AppState,
    utils::{format_bytes, format_duration},
};
use iced::{
    widget::{column, container, scrollable, text, Space},
    Element, Length,
};
use p2p_core::history::TransferStatus;

pub fn view_history_tab(state: &AppState) -> Element<'_, Message> {
    let history = state.history.lock().unwrap();
    let transfers = history.recent(20);

    let mut content =
        column![text("Transfer History").size(18), Space::with_height(16),].spacing(8);

    if transfers.is_empty() {
        content = content.push(container(text("No transfer history yet").size(14)).padding(20));
    } else {
        for transfer in transfers {
            let status_icon = match transfer.status {
                TransferStatus::Completed => "✓",
                TransferStatus::Failed => "✗",
                TransferStatus::Interrupted => "⏸",
            };

            let status_text = match transfer.status {
                TransferStatus::Completed => "Completed",
                TransferStatus::Failed => "Failed",
                TransferStatus::Interrupted => "Interrupted",
            };

            let direction = match transfer.direction {
                p2p_core::history::TransferDirection::Send => "Sent",
                p2p_core::history::TransferDirection::Receive => "Received",
            };

            let file_display = if transfer.files.is_empty() {
                "Unknown".to_string()
            } else if transfer.files.len() == 1 {
                transfer.files[0].clone()
            } else {
                format!("{} files", transfer.files.len())
            };

            let entry = container(
                column![
                    iced::widget::row![
                        text(status_icon).size(16),
                        Space::with_width(8),
                        text(&file_display).size(14),
                    ]
                    .align_items(iced::Alignment::Center)
                    .spacing(4),
                    Space::with_height(6),
                    text(format!(
                        "{} • {} • {}",
                        direction,
                        status_text,
                        format_bytes(transfer.bytes_transferred)
                    ))
                    .size(12),
                    Space::with_height(4),
                    text(format!(
                        "Peer: {} • Duration: {}",
                        transfer.peer_address,
                        format_duration(transfer.duration_secs)
                    ))
                    .size(12),
                ]
                .spacing(2),
            )
            .padding(12);

            content = content.push(entry).push(Space::with_height(8));
        }
    }

    let card = container(content).padding(20);

    container(scrollable(card))
        .width(Length::Fill)
        .height(Length::Fill)
        .padding(16)
        .into()
}
