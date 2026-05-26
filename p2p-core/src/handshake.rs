//! Connection handshake protocol over a QUIC control stream.
//!
//! By the time we run the handshake, TLS 1.3 has already authenticated the
//! peer's certificate against the pinned fingerprint (client side) or
//! accepted whatever cert the peer presented (server side, Phase 0). This
//! handshake layer is concerned with the *application* protocol: version
//! check, configuration exchange, and an application-level cross-check
//! that the cert fingerprint the peer claims in HELLO matches the one the
//! TLS layer observed.

use crate::error::{Error, Result};
use crate::identity::{Fingerprint, Identity};
use crate::network::quic::QuicConnection;
use crate::protocol::{ConfigMessage, HelloMessage, Message, TransferInfo};
use crate::{MIN_PROTOCOL_VERSION, PROTOCOL_VERSION};
use tracing::{debug, trace};
use uuid::Uuid;

/// Handshake result containing negotiated parameters.
#[derive(Debug, Clone)]
pub struct HandshakeResult {
    pub peer_device_id: Uuid,
    pub peer_fingerprint: Fingerprint,
    pub config: ConfigMessage,
}

/// Cross-check the peer's claimed fingerprint against the cert TLS
/// actually observed. With mutual TLS, both sides see the peer's cert,
/// so `observed` is always `Some` — any mismatch (including a missing
/// observation, which means the peer presented no cert at all and the
/// responder shouldn't have accepted the handshake) is fatal.
fn cross_check_fingerprint(claimed: Fingerprint, observed: Option<Fingerprint>) -> Result<()> {
    match observed {
        Some(actual) if actual == claimed => Ok(()),
        _ => Err(Error::FingerprintMismatch),
    }
}

/// Handshake initiator side.
pub struct HandshakeClient {
    device_id: Uuid,
    fingerprint: Fingerprint,
}

impl HandshakeClient {
    pub fn new(device_id: Uuid, identity: &Identity) -> Self {
        Self {
            device_id,
            fingerprint: identity.fingerprint(),
        }
    }

    pub async fn perform_handshake(
        &self,
        conn: &mut QuicConnection,
        config: ConfigMessage,
    ) -> Result<HandshakeResult> {
        debug!("Starting handshake with {}", conn.peer_addr());

        trace!("Sending HELLO");
        let hello = Message::Hello(HelloMessage {
            protocol_version: PROTOCOL_VERSION,
            min_version: MIN_PROTOCOL_VERSION,
            device_id: self.device_id,
            cert_fingerprint: self.fingerprint,
        });
        conn.send_message(&hello).await?;

        trace!("Waiting for HELLO_ACK");
        let peer_hello = match conn.recv_message().await? {
            Message::HelloAck(h) => h,
            Message::Error(e) => {
                return Err(Error::Protocol(format!("Handshake error: {}", e.message)))
            }
            msg => return Err(Error::Protocol(format!("Expected HelloAck, got {:?}", msg))),
        };

        if peer_hello.protocol_version != PROTOCOL_VERSION {
            return Err(Error::VersionMismatch {
                peer: peer_hello.protocol_version,
                ours: PROTOCOL_VERSION,
            });
        }

        // Cross-check the peer's claimed fingerprint against the cert TLS
        // actually validated. As the initiator we pinned it, so this must
        // succeed unless the responder is sending HELLO data that doesn't
        // match its TLS cert.
        cross_check_fingerprint(peer_hello.cert_fingerprint, conn.peer_fingerprint())?;

        trace!("Sending CONFIG");
        conn.send_message(&Message::Config(config.clone())).await?;

        trace!("Waiting for CONFIG_ACK");
        match conn.recv_message().await? {
            Message::ConfigAck => {}
            Message::Error(e) => {
                return Err(Error::Protocol(format!("Config rejected: {}", e.message)))
            }
            msg => {
                return Err(Error::Protocol(format!(
                    "Expected ConfigAck, got {:?}",
                    msg
                )))
            }
        }

        debug!("Handshake completed");
        Ok(HandshakeResult {
            peer_device_id: peer_hello.device_id,
            peer_fingerprint: peer_hello.cert_fingerprint,
            config,
        })
    }

    pub async fn send_transfer_info(
        &self,
        conn: &mut QuicConnection,
        info: TransferInfo,
    ) -> Result<()> {
        trace!("Sending TRANSFER_INFO");
        conn.send_message(&Message::TransferInfo(Box::new(info)))
            .await?;

        trace!("Waiting for READY");
        match conn.recv_message().await? {
            Message::Ready => Ok(()),
            Message::Error(e) => Err(Error::Protocol(format!("Transfer rejected: {}", e.message))),
            msg => Err(Error::Protocol(format!("Expected Ready, got {:?}", msg))),
        }
    }
}

/// Handshake responder side.
pub struct HandshakeServer {
    device_id: Uuid,
    fingerprint: Fingerprint,
}

impl HandshakeServer {
    pub fn new(device_id: Uuid, identity: &Identity) -> Self {
        Self {
            device_id,
            fingerprint: identity.fingerprint(),
        }
    }

    pub async fn perform_handshake(&self, conn: &mut QuicConnection) -> Result<HandshakeResult> {
        debug!("Starting handshake with {}", conn.peer_addr());

        trace!("Waiting for HELLO");
        let peer_hello = match conn.recv_message().await? {
            Message::Hello(h) => h,
            msg => return Err(Error::Protocol(format!("Expected Hello, got {:?}", msg))),
        };

        if peer_hello.protocol_version != PROTOCOL_VERSION {
            return Err(Error::VersionMismatch {
                peer: peer_hello.protocol_version,
                ours: PROTOCOL_VERSION,
            });
        }

        // Mutual TLS: the client presented its cert during the QUIC
        // handshake, so `peer_fingerprint()` is `Some` and any
        // disagreement with the HELLO claim is fatal.
        cross_check_fingerprint(peer_hello.cert_fingerprint, conn.peer_fingerprint())?;

        trace!("Sending HELLO_ACK");
        let hello_ack = Message::HelloAck(HelloMessage {
            protocol_version: PROTOCOL_VERSION,
            min_version: MIN_PROTOCOL_VERSION,
            device_id: self.device_id,
            cert_fingerprint: self.fingerprint,
        });
        conn.send_message(&hello_ack).await?;

        trace!("Waiting for CONFIG");
        let config = match conn.recv_message().await? {
            Message::Config(c) => c,
            msg => return Err(Error::Protocol(format!("Expected Config, got {:?}", msg))),
        };

        trace!("Sending CONFIG_ACK");
        conn.send_message(&Message::ConfigAck).await?;

        debug!("Handshake completed");
        Ok(HandshakeResult {
            peer_device_id: peer_hello.device_id,
            peer_fingerprint: peer_hello.cert_fingerprint,
            config,
        })
    }

    pub async fn recv_transfer_info(&self, conn: &mut QuicConnection) -> Result<TransferInfo> {
        trace!("Waiting for TRANSFER_INFO");
        let info = match conn.recv_message().await? {
            Message::TransferInfo(i) => *i,
            msg => {
                return Err(Error::Protocol(format!(
                    "Expected TransferInfo, got {:?}",
                    msg
                )))
            }
        };

        trace!("Sending READY");
        conn.send_message(&Message::Ready).await?;

        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::quic::QuicEndpoint;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    #[tokio::test]
    async fn handshake_round_trip_over_quic() {
        let server_identity = Arc::new(Identity::generate().unwrap());
        let server_fp = server_identity.fingerprint();
        let server_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            server_identity.clone(),
        )
        .unwrap();
        let server_addr = server_ep.local_addr().unwrap();

        let server_device_id = Uuid::new_v4();
        let server_id_for_task = server_identity.clone();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            let mut conn = server_ep.accept().await.unwrap();
            let h = HandshakeServer::new(server_device_id, &server_id_for_task);
            let result = h.perform_handshake(&mut conn).await.unwrap();
            // Hold the connection until the test signals the client is done
            // reading the last handshake message. P2PSession does the same in
            // production by keeping `conn` alive for the session's lifetime.
            let _ = done_rx.await;
            result
        });

        let client_identity = Arc::new(Identity::generate().unwrap());
        let client_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            client_identity.clone(),
        )
        .unwrap();
        let mut client_conn = client_ep.connect(server_addr, server_fp).await.unwrap();

        let client = HandshakeClient::new(Uuid::new_v4(), &client_identity);
        let client_result = client
            .perform_handshake(&mut client_conn, ConfigMessage::default())
            .await
            .unwrap();

        done_tx.send(()).ok();
        let server_result = server_task.await.unwrap();
        assert_eq!(client_result.peer_fingerprint, server_fp);
        // Mutual TLS: the responder now also observes the initiator's
        // cert. The HELLO cross-check on the responder side would have
        // failed if the observation didn't match the claim, so this
        // just confirms the value made it out into the result.
        assert_eq!(
            server_result.peer_fingerprint,
            client_identity.fingerprint()
        );
    }

    #[test]
    fn cross_check_fingerprint_rejects_missing_observation() {
        // With mTLS, the responder must always observe a client cert.
        // A `None` observation means the peer never presented one, which
        // is a security failure even if the HELLO claims a valid value.
        let claimed: Fingerprint = [0xAA; 32];
        assert!(matches!(
            cross_check_fingerprint(claimed, None),
            Err(Error::FingerprintMismatch)
        ));
    }

    #[test]
    fn cross_check_fingerprint_rejects_mismatched_observation() {
        let claimed: Fingerprint = [0xAA; 32];
        let observed: Fingerprint = [0xBB; 32];
        assert!(matches!(
            cross_check_fingerprint(claimed, Some(observed)),
            Err(Error::FingerprintMismatch)
        ));
    }

    #[test]
    fn cross_check_fingerprint_accepts_matching_observation() {
        let fp: Fingerprint = [0x42; 32];
        assert!(cross_check_fingerprint(fp, Some(fp)).is_ok());
    }
}
