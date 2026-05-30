//! Settings tab view

use crate::{message::Message, state::AppState};
use iced::{
    widget::{checkbox, column, container, scrollable, slider, text, text_input, Space},
    Element, Length,
};

pub fn view_settings_tab(state: &AppState) -> Element<'_, Message> {
    let compression_checkbox = checkbox("Enable compression", state.settings.compression_enabled)
        .on_toggle(Message::CompressionToggled);

    let compression_level_slider = slider(
        -7..=22,
        state.settings.compression_level,
        Message::CompressionLevelChanged,
    )
    .step(1);

    let chunk_size_input = text_input("Chunk size (KB)", &state.settings.chunk_size_kb.to_string())
        .on_input(|s| {
            s.parse::<u32>()
                .map(Message::ChunkSizeChanged)
                .unwrap_or(Message::ChunkSizeChanged(state.settings.chunk_size_kb))
        })
        .padding(8);

    let bandwidth_input = text_input(
        "Bandwidth limit (MB/s, 0 = unlimited)",
        &state.settings.bandwidth_input,
    )
    .on_input(Message::BandwidthLimitChanged)
    .padding(8);

    let max_retries_input = text_input("Max retries", &state.settings.max_retries.to_string())
        .on_input(|s| {
            s.parse::<u32>()
                .map(Message::MaxRetriesChanged)
                .unwrap_or(Message::MaxRetriesChanged(state.settings.max_retries))
        })
        .padding(8);

    let content = column![
        text("Settings").size(18),
        Space::with_height(20),
        // Compression section
        container(
            column![
                text("Compression").size(15),
                Space::with_height(8),
                compression_checkbox,
                Space::with_height(12),
                text(format!(
                    "Compression Level: {}",
                    state.settings.compression_level
                ))
                .size(13),
                Space::with_height(6),
                compression_level_slider,
                Space::with_height(6),
                text("Range: -7 (fast) to 22 (max compression)").size(12),
            ]
            .spacing(4)
        )
        .padding(12),
        Space::with_height(16),
        // Transfer parameters section
        container(
            column![
                text("Transfer Parameters").size(15),
                Space::with_height(12),
                text("Chunk Size").size(13),
                Space::with_height(4),
                chunk_size_input,
                Space::with_height(12),
                text("Bandwidth Limit").size(13),
                Space::with_height(4),
                bandwidth_input,
                Space::with_height(12),
                text("Max Retries").size(13),
                Space::with_height(4),
                max_retries_input,
            ]
            .spacing(2)
        )
        .padding(12),
    ]
    .spacing(6);

    let card = container(content).padding(20);

    container(scrollable(card))
        .width(Length::Fill)
        .height(Length::Fill)
        .padding(16)
        .into()
}
