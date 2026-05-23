//! Shared helper for `--rendezvous` / `--code` session establishment.

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

    info!("Pairing through rendezvous {rendezvous_addr} with code '{code}' (this may take a moment)...");

    let session = P2PSession::from_rendezvous(
        rendezvous_addr,
        code,
        identity,
        device_id,
        capabilities,
        config,
    )
    .await?;
    Ok(session)
}

async fn resolve_first(host_port: &str) -> Result<SocketAddr> {
    // If the user passed bare "host" with no port, fill in the default.
    let with_port = if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{host_port}:{}", p2p_core::DEFAULT_RENDEZVOUS_PORT)
    };
    let mut iter = lookup_host(&with_port).await?;
    iter.next()
        .ok_or_else(|| anyhow!("could not resolve rendezvous address '{with_port}'"))
}
