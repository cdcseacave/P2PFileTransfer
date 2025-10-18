//! Send tab view

use crate::{
    message::Message,
    state::AppState,
    utils::{format_bytes, format_duration},
};
use iced::{
    widget::{button, column, container, progress_bar, scrollable, text, text_input, Space},
    Element, Length,
};

pub fn view_send_tab(state: &AppState) -> Element<'_, Message> {
    let path_input = text_input("File or folder path", &state.send_state.path_input)
        .on_input(Message::PathInputChanged)
        .padding(8);

    let browse_file_button = button(text("Browse File").size(14))
        .on_press(Message::BrowseFile)
        .padding([8, 16])
        .style(iced::theme::Button::Secondary);

    let browse_folder_button = button(text("Browse Folder").size(14))
        .on_press(Message::BrowseFolder)
        .padding([8, 16])
        .style(iced::theme::Button::Secondary);

    let send_button = if state.transfer_progress.is_some() || state.send_state.path_input.is_empty()
    {
        button(text("Send").size(14))
            .padding([8, 16])
            .style(iced::theme::Button::Primary)
    } else {
        button(text("Send").size(14))
            .on_press(Message::StartSend)
            .padding([8, 16])
            .style(iced::theme::Button::Primary)
    };

    let mut content = column![
        text("Send File/Folder").size(18),
        Space::with_height(12),
        text("Path").size(13),
        path_input,
        Space::with_height(8),
        iced::widget::row![browse_file_button, browse_folder_button].spacing(8),
        Space::with_height(16),
        send_button,
    ]
    .spacing(6);

    if let Some(progress) = &state.transfer_progress {
        if progress.is_sending {
            let progress_value = if progress.total_bytes > 0 {
                progress.transferred_bytes as f32 / progress.total_bytes as f32
            } else {
                0.0
            };

            let progress_bar_widget = progress_bar(0.0..=1.0, progress_value);

            let stats = column![
                text(format!("Progress: {:.1}%", progress_value * 100.0)).size(14),
                Space::with_height(4),
                text(format!(
                    "Transferred: {} / {}",
                    format_bytes(progress.transferred_bytes),
                    format_bytes(progress.total_bytes)
                ))
                .size(13),
                Space::with_height(4),
                text(format!(
                    "Speed: {}/s",
                    format_bytes(progress.speed_bps as u64)
                ))
                .size(13),
                Space::with_height(4),
                text(format!(
                    "Time remaining: {}",
                    if progress.speed_bps > 0.0 {
                        let remaining_bytes = progress
                            .total_bytes
                            .saturating_sub(progress.transferred_bytes);
                        format_duration(
                            (remaining_bytes as f64 / progress.speed_bps.max(1.0)) as u64,
                        )
                    } else {
                        "calculating...".to_string()
                    }
                ))
                .size(13),
            ]
            .spacing(2);

            content = content.push(Space::with_height(20)).push(
                container(
                    column![
                        text("Transfer Progress").size(13),
                        Space::with_height(8),
                        progress_bar_widget,
                        Space::with_height(12),
                        stats,
                    ]
                    .spacing(4),
                )
                .padding(12),
            );
        }
    }

    let card = container(content).padding(20);

    container(scrollable(card))
        .width(Length::Fill)
        .height(Length::Fill)
        .padding(16)
        .into()
}
