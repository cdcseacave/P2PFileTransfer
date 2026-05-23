# p2p-gui — Agent Notes

`p2p-gui` is the Iced 0.12 GUI for the P2P transfer tool. It's built on top of `p2p-core` and is reached either directly from the binary (when only `gui` is enabled) or via `p2p-cli` (`run_cli_sync` short-circuits the no-arg case to `run_gui()`). Workspace-wide guidance lives in the root [AGENTS.md](../AGENTS.md).

## Elm-architecture layout

Standard Iced split — touch the right file:

```
src/
├── lib.rs            # public `run_gui()` entry point (called outside any Tokio runtime)
├── app.rs            # P2PTransferApp: Iced Application impl (new/title/update/view/theme)
├── state.rs          # AppState, Tab enum, per-tab state structs, ConsoleIcon
├── message.rs        # the full Message enum — every event/command in the app
├── operations.rs     # handle_message(state, msg) -> Command<Message>; spawns async work
├── styles.rs         # color palette and button/container styles
├── utils.rs          # formatting helpers (sizes, durations, speeds)
└── views/
    ├── mod.rs        # re-exports view_*_tab functions
    ├── connection.rs
    ├── send.rs
    ├── receive.rs
    ├── settings.rs
    ├── history.rs
    └── console.rs    # bottom-of-window console (rendered on every tab)
```

`app.rs::view` composes: tabs row → active tab's `view_*_tab` → console at the bottom.

When adding a feature, the usual edit set is: `state.rs` (field) → `message.rs` (variant) → `views/<tab>.rs` (widget) → `operations.rs` (handler arm).

## Runtime model

- **Do not call `run_gui()` from inside `tokio::runtime::Runtime::block_on`.** Iced 0.12 owns its own Tokio runtime via the `tokio` feature. Nesting panics. `p2p-cli::run_cli_sync` is structured specifically to call `run_gui()` *before* it ever constructs an async runtime.
- `Application::Executor = iced::executor::Default` — async work spawned via `Command::perform` runs on Iced's executor.
- Long-running transfers hold the `P2PSession` in `Arc<Mutex<P2PSession>>` inside `AppState` so both the send and receive tabs can drive the same connection.

## Tabs

`Tab::all()` returns `[Connection, Send, Receive, Settings, History]`. Each tab has its own state struct in `state.rs` (e.g., `ConnectionState`) and a `view_<tab>_tab(state) -> Element<Message>` in `views/`. Adding a tab: extend the `Tab` enum + `all()` + `icon()` + `text()`, add a state struct, add a view function and re-export from `views/mod.rs`, add the match arm in `app.rs::view`.

### Connection tab modes

`ConnectionMode::all()` returns `[Listen, Connect, Rendezvous]`:

- **Listen** — bind on `--port` and accept the next inbound session.
- **Connect** — direct dial of `peer_address` with `peer_fingerprint` pinned at the TLS layer (or pulled from a LAN beacon when `use_discovery` is set).
- **Rendezvous** ("Pair with code (cross-NAT)") — pair through `rendezvous_address` with a shared `code`. The view exposes a Generate button that fills `code` with a fresh 6-character base32 (`p2p_core::traversal::generate_code`). Peer fingerprint comes from the rendezvous match — the user doesn't have to type it.

Session establishment runs **inside `Command::perform`** (off the iced thread): the async future calls `P2PSession::connect`, `accept`, or `from_rendezvous` and returns `Message::ConnectionEstablishedWithSession(Arc<tokio::Mutex<P2PSession>>)`. Only the wrapped session is stored in `AppState` so the message loop never holds the mutex across an await. Don't lock the mutex on the iced thread — go through `Command::perform` for any operation that needs the session.

## Cross-platform emoji font

`app.rs::view` selects an emoji font by target OS — `Apple Color Emoji` (macOS), `Segoe UI Emoji` (Windows), `Noto Color Emoji` (otherwise). Tab labels render the emoji and the text as **separate** `text` elements so the emoji font doesn't bleed into the regular label. Preserve this split when editing the tabs row; mixing them with a single `text` widget breaks rendering on Windows.

## Logging

The GUI uses `tracing` (no separate subscriber here — the CLI's `init_logging` already configured one when launched via `p2p-cli`; when launched directly via `main.rs` no subscriber is set, which is fine for the GUI's needs). Use `info!`, `debug!`, etc. for diagnostics — user-visible messages go through the console view (`AppState::console_messages` with `ConsoleIcon` for severity).

## Theme

`fn theme()` returns `Theme::Dark` hard-coded. If you add a settings toggle for light/dark, route it through the `Settings` tab → `Message::ThemeChanged(Theme)` → store on `AppState` → return from `theme()`. Don't read theme from a global.

## Testing & lint

```bash
cargo test -p p2p-gui
cargo clippy -p p2p-gui --all-targets -- -D warnings
```

The GUI doesn't currently have automated end-to-end tests — manual smoke testing is the norm. When verifying changes, launch with `cargo run --release --features full` and walk the tabs.
