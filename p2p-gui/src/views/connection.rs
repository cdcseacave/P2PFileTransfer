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

    match state.connection_state.mode {
        ConnectionMode::Connect => {
            let peer_input = text_input(
                "Peer address (e.g., 192.168.1.100)",
                &state.connection_state.peer_address,
            )
            .on_input(Message::PeerAddressChanged)
            .padding(8);

            let fp_input = text_input(
                "Peer cert fingerprint (64 hex chars)",
                &state.connection_state.peer_fingerprint,
            )
            .on_input(Message::PeerFingerprintChanged)
            .padding(8);

            let inputs_row = row![
                column![
                    text("Peer Address").size(13),
                    Space::with_height(6),
                    peer_input,
                ]
                .spacing(0),
                Space::with_width(16),
                column![text("Port").size(13), Space::with_height(6), port_input]
                    .spacing(0)
                    .width(Length::Fill),
            ]
            .align_items(iced::Alignment::Start);

            let discovery_checkbox =
                checkbox("Use peer discovery (LAN beacons)", state.connection_state.use_discovery)
                    .on_toggle(Message::DiscoveryToggled);

            content = content
                .push(inputs_row)
                .push(Space::with_height(8))
                .push(text("Peer Cert Fingerprint").size(13))
                .push(Space::with_height(6))
                .push(fp_input)
                .push(Space::with_height(4))
                .push(text("Required for direct --peer mode. Auto-filled by discovery.").size(11))
                .push(Space::with_height(8))
                .push(discovery_checkbox);
        }
        ConnectionMode::Rendezvous => {
            let rendezvous_input = text_input(
                "Rendezvous server (host[:port])",
                &state.connection_state.rendezvous_address,
            )
            .on_input(Message::RendezvousAddressChanged)
            .padding(8);

            let code_input = text_input("Pairing code (4-32 chars)", &state.connection_state.code)
                .on_input(Message::CodeChanged)
                .padding(8)
                .width(Length::Fill);

            let generate_button = button(text("Generate").size(13))
                .on_press(Message::GenerateCode)
                .padding([8, 12]);

            let code_row = row![code_input, Space::with_width(8), generate_button]
                .align_items(iced::Alignment::Center);

            content = content
                .push(text("Rendezvous Server").size(13))
                .push(Space::with_height(6))
                .push(rendezvous_input)
                .push(Space::with_height(12))
                .push(text("Shared Pairing Code").size(13))
                .push(Space::with_height(6))
                .push(code_row)
                .push(Space::with_height(4))
                .push(
                    text(
                        "Both peers enter the same code. Pairing waits up to 5 minutes \
                         for the other side to connect.",
                    )
                    .size(11),
                );
        }
        ConnectionMode::Listen => {
            content = content.push(text("Port").size(13)).push(port_input);
        }
    }

    let action_button = if state.connection_state.is_active {
        button(
            text(match state.connection_state.mode {
                ConnectionMode::Listen => "Stop Listening",
                ConnectionMode::Connect => "Disconnect",
                ConnectionMode::Rendezvous => "Cancel pairing",
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
                ConnectionMode::Rendezvous => "Pair with code",
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
