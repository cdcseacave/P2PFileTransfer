//! Receive tab view

use crate::{
    message::Message,
    state::AppState,
    utils::{format_bytes, format_duration},
};
use iced::{
    widget::{
        button, checkbox, column, container, progress_bar, scrollable, text, text_input, Space,
    },
    Element, Length,
};

pub fn view_receive_tab(state: &AppState) -> Element<'_, Message> {
    let output_input = text_input("Output directory", &state.receive_state.output_input)
        .on_input(Message::OutputDirChanged)
        .padding(8);

    let browse_button = button(text("Browse").size(14))
        .on_press(Message::BrowseOutputDir)
        .padding([8, 16])
        .style(iced::theme::Button::Secondary);

    let open_button = button(text("📁 Open").size(14))
        .on_press(Message::OpenOutputDir)
        .padding([8, 16])
        .style(iced::theme::Button::Secondary);

    let buttons_row = iced::widget::row![browse_button, Space::with_width(8), open_button]
        .align_items(iced::Alignment::Center);

    let auto_accept_checkbox = checkbox(
        "Auto-accept incoming transfers",
        state.receive_state.auto_accept,
    )
    .on_toggle(Message::AutoAcceptToggled);

    let mut content = column![
        text("Receive File/Folder").size(18),
        Space::with_height(12),
        text("Output Directory").size(13),
        output_input,
        Space::with_height(8),
        buttons_row,
        Space::with_height(16),
        auto_accept_checkbox,
    ]
    .spacing(6);

    if let Some(progress) = &state.transfer_progress {
        if !progress.is_sending {
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
