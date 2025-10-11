//! Main application module
//!
//! This module contains the main Iced application implementation.

use crate::{
    message::Message,
    operations,
    state::{AppState, Tab},
    views,
};
use iced::{
    widget::{button, column, container, row, text},
    Application, Command, Element, Length, Settings, Size, Theme,
};

/// Main P2P Transfer Application
pub struct P2PTransferApp {
    state: AppState,
}

impl Application for P2PTransferApp {
    type Executor = iced::executor::Default;
    type Message = Message;
    type Theme = Theme;
    type Flags = ();

    fn new(_flags: Self::Flags) -> (Self, Command<Self::Message>) {
        (
            Self {
                state: AppState::new(),
            },
            Command::none(),
        )
    }

    fn title(&self) -> String {
        String::from("P2P File Transfer")
    }

    fn update(&mut self, message: Self::Message) -> Command<Self::Message> {
        operations::handle_message(&mut self.state, message)
    }

    fn view(&self) -> Element<'_, Self::Message> {
        // Platform-specific emoji font
        #[cfg(target_os = "macos")]
        let emoji_font = iced::Font::with_name("Apple Color Emoji");
        #[cfg(target_os = "windows")]
        let emoji_font = iced::Font::with_name("Segoe UI Emoji");
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        let emoji_font = iced::Font::with_name("Noto Color Emoji");

        let tabs = row(Tab::all()
            .iter()
            .map(|&tab| {
                let is_active = tab == self.state.current_tab;
                // Create separate text elements for emoji (with emoji font) and text (with default font)
                let tab_content = row![
                    text(tab.icon()).font(emoji_font).size(14),
                    text(" "),
                    text(tab.text()).size(14)
                ]
                .align_items(iced::Alignment::Center);

                button(tab_content)
                    .on_press(Message::TabSelected(tab))
                    .padding([8, 16])
                    .style(if is_active {
                        iced::theme::Button::Primary
                    } else {
                        iced::theme::Button::Secondary
                    })
                    .into()
            })
            .collect::<Vec<_>>())
        .spacing(8)
        .padding(12);

        let content = match self.state.current_tab {
            Tab::Connection => views::view_connection_tab(&self.state),
            Tab::Send => views::view_send_tab(&self.state),
            Tab::Receive => views::view_receive_tab(&self.state),
            Tab::Settings => views::view_settings_tab(&self.state),
            Tab::History => views::view_history_tab(&self.state),
        };

        // Add console at the bottom
        let console = views::view_console(&self.state);

        let main_content = column![tabs, content, console].spacing(0);

        container(main_content)
            .width(Length::Fill)
            .height(Length::Fill)
            .into()
    }

    fn theme(&self) -> Self::Theme {
        Theme::Dark
    }

    fn subscription(&self) -> iced::Subscription<Self::Message> {
        // No subscriptions needed - Iced handles window close properly
        // Resources will be cleaned up via Drop when app terminates
        iced::Subscription::none()
    }
}

/// Run the GUI application
pub fn run() -> anyhow::Result<()> {
    let settings = Settings {
        window: iced::window::Settings {
            size: Size::new(900.0, 650.0),
            min_size: Some(Size::new(700.0, 500.0)),
            ..Default::default()
        },
        ..Default::default()
    };

    // Run the app and handle any errors
    let result = P2PTransferApp::run(settings);

    // Give a moment for cleanup to complete
    std::thread::sleep(std::time::Duration::from_millis(100));

    result.map_err(|e| anyhow::anyhow!("GUI error: {}", e))?;
    Ok(())
}
