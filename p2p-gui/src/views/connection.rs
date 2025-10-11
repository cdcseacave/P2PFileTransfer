//! Connection tab view

use crate::{
    message::Message,
    state::{AppState, ConnectionMode},
};
use iced::{
    widget::{
        button, checkbox, column, container, pick_list, row, scrollable, text, text_input, Space,
    },
    Element, Length,
};

pub fn view_connection_tab(state: &AppState) -> Element<'_, Message> {
    let mode_picker = pick_list(
        ConnectionMode::all(),
        Some(state.connection_state.mode),
        Message::ModeSelected,
    )
    .padding(8);

    let port_input = text_input("Port (e.g., 14567)", &state.connection_state.port)
        .on_input(Message::PortChanged)
        .padding(8)
        .width(Length::Fixed(150.0));

    let mut content = column![
        text("Connection").size(18),
        Space::with_height(12),
        text("Mode").size(13),
        mode_picker,
        Space::with_height(12),
    ]
    .spacing(6);

    if state.connection_state.mode == ConnectionMode::Connect {
        let peer_input = text_input(
            "Peer address (e.g., 192.168.1.100)",
            &state.connection_state.peer_address,
        )
        .on_input(Message::PeerAddressChanged)
        .padding(8);

        // Create side-by-side layout for Port and Peer Address
        let inputs_row = row![
            column![
                text("Peer Address").size(13),
                Space::with_height(6),
                peer_input,
            ]
            .spacing(0),
            Space::with_width(16),
            column![text("Port").size(13), Space::with_height(6), port_input,]
                .spacing(0)
                .width(Length::Fill),
        ]
        .align_items(iced::Alignment::Start);

        let discovery_checkbox =
            checkbox("Use peer discovery", state.connection_state.use_discovery)
                .on_toggle(Message::DiscoveryToggled);

        content = content
            .push(inputs_row)
            .push(Space::with_height(8))
            .push(discovery_checkbox);
    } else {
        // Listen mode - just show Port
        content = content.push(text("Port").size(13)).push(port_input);
    }

    let action_button = if state.connection_state.is_active {
        button(
            text(match state.connection_state.mode {
                ConnectionMode::Listen => "Stop Listening",
                ConnectionMode::Connect => "Disconnect",
            })
            .size(14),
        )
        .on_press(Message::StopConnection)
        .padding([8, 16])
        .style(iced::theme::Button::Destructive)
    } else {
        button(
            text(match state.connection_state.mode {
                ConnectionMode::Listen => "Start Listening",
                ConnectionMode::Connect => "Connect",
            })
            .size(14),
        )
        .on_press(Message::StartConnection)
        .padding([8, 16])
        .style(iced::theme::Button::Primary)
    };

    content = content.push(Space::with_height(16)).push(action_button);

    let card = container(content).padding(20);

    container(scrollable(card))
        .width(Length::Fill)
        .height(Length::Fill)
        .padding(16)
        .into()
}
