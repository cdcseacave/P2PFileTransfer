//! Console view - displays chronological log of all messages

use crate::state::AppState;
use iced::{
    widget::{column, container, text, text_editor, Space},
    Element, Length,
};

pub fn view_console<'a>(state: &'a AppState) -> Element<'a, crate::message::Message> {
    // Current status line - shows only connection status
    let status_text = &state.connection_state.status_message;

    let current_status = container(text(format!("Connection: {}", status_text)).size(12))
        .padding([6, 8])
        .width(Length::Fill)
        .style(iced::theme::Container::Box);

    // Use text_editor for selectable console output with action handler
    let console_editor = text_editor(&state.console_content)
        .height(Length::Fixed(150.0))
        .on_action(crate::message::Message::ConsoleAction);

    container(column![current_status, Space::with_height(2), console_editor].spacing(4))
        .padding(8)
        .width(Length::Fill)
        .into()
}
