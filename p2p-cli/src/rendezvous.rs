//! Shared helpers for session establishment.
//!
//! All three transfer-related CLI commands (`send`, `receive`, `resume`)
//! reach a session via the same two paths — direct (peer addr or LAN
//! discovery) and rendezvous (code-based pairing through a relay-capable
//! server). [`establish_session`] is the single entry point they all share
//! so the dispatch lives in one place. The lower-level [`establish`]
//! handles the rendezvous-specific work and is also called directly on
//! re-pair after a disconnect.
//!
//! Why a unified entry point rather than duplicating the `if rendezvous {}
//! else {}` block per call site: the receive loop needs to re-pair after a
//! sender disconnect — and the rendezvous half of that branch is where the
//! bug used to live (`reaccept()` only works when this side ended up the
//! QUIC responder, which is non-deterministic post-rendezvous). Funnelling
//! everything through one helper means the re-pair path is identical to
//! the initial pair and the role randomness no longer matters.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tokio::net::lookup_host;
use tracing::info;

use p2p_core::{
    identity::Identity,
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    Uuid,
};

use crate::cli::SessionParams;

/// True iff `--rendezvous` was supplied. Lets callers branch before
/// touching `--peer` / `--discover`.
pub fn is_rendezvous_mode(params: &SessionParams) -> bool {
    params.rendezvous.is_some()
}

/// Establish a session using whichever mode `params` selects.
///
/// * `--rendezvous` set → pair via [`establish`] (rendezvous + code).
/// * otherwise → direct mode via [`P2PSession::establish`] (peer addr or
///   LAN discovery).
///
/// `role_default` is the per-command default (`"client"` for `send` /
/// `resume`, `"server"` for `receive`) used only in direct mode; the
/// rendezvous path is symmetric and ignores it.
pub async fn establish_session(
    params: &SessionParams,
    role_default: &str,
    identity: Arc<Identity>,
    device_id: Uuid,
    capabilities: Capabilities,
    config: Option<ConfigMessage>,
) -> Result<P2PSession> {
    if is_rendezvous_mode(params) {
        establish(
            params,
            identity,
            device_id,
            capabilities,
            config.unwrap_or_default(),
        )
        .await
    } else {
        let role = params.get_role(role_default);
        let peer_fp = params.parsed_fingerprint()?;
        P2PSession::establish(
            &role,
            params.peer.clone(),
            peer_fp,
            params.discover,
            params.port,
            identity,
            device_id,
            capabilities,
            config,
        )
        .await
        .map_err(Into::into)
    }
}

/// Establish a session via rendezvous + code. Validates that `--code`
/// is also present and resolves `--rendezvous` to a `SocketAddr`.
pub async fn establish(
    params: &SessionParams,
    identity: Arc<Identity>,
    device_id: Uuid,
    capabilities: Capabilities,
    config: ConfigMessage,
) -> Result<P2PSession> {
    let rendezvous_host = params
        .rendezvous
        .as_deref()
        .ok_or_else(|| anyhow!("internal: rendezvous mode requested without --rendezvous"))?;
    let code = params
        .code
        .as_deref()
        .ok_or_else(|| anyhow!("--code is required when --rendezvous is set"))?
        .to_string();

    let rendezvous_addr = resolve_first(rendezvous_host)
        .await
        .with_context(|| format!("resolving --rendezvous '{rendezvous_host}'"))?;

    info!(
        "Pairing through rendezvous {rendezvous_addr} with code '{code}' (this may take a moment, relay={})...",
        params.force_relay
    );

    let session = P2PSession::from_rendezvous(
        rendezvous_addr,
        code,
        identity,
        device_id,
        capabilities,
        config,
        params.force_relay,
    )
    .await?;
    Ok(session)
}

async fn resolve_first(host_port: &str) -> Result<SocketAddr> {
    let with_port = p2p_core::with_default_port(host_port, p2p_core::DEFAULT_RENDEZVOUS_PORT);
    let mut iter = lookup_host(&with_port).await?;
    iter.next()
        .ok_or_else(|| anyhow!("could not resolve rendezvous address '{with_port}'"))
}
