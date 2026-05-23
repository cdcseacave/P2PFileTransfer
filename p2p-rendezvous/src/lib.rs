//! Pairing-by-code rendezvous for `p2p-transfer`.
//!
//! Two peers connect to the same `rendezvousd` instance, register with a
//! short shared code, and receive each other's public UDP endpoint + TLS
//! cert fingerprint + device id. From there both peers race a
//! [`quinn::Endpoint::connect`] against an [`Endpoint::accept`] — QUIC's
//! `Initial` packets serve as the NAT hole-punch, no separate raw send is
//! needed.
//!
//! Wire transport: MessagePack frames over TCP. Each frame is a 4-byte
//! big-endian length prefix followed by the serialized [`protocol::Message`]
//! payload. The server **never** sees user data; the rendezvous channel
//! is closed as soon as the peer match is delivered.

pub mod client;
pub mod protocol;
pub mod relay;
pub mod server;

pub use client::{register, ClientError, MatchOutcome, PeerInfo, RelayInfo};
pub use protocol::{Message, RegisterRequest, RendezvousProtoError};
pub use relay::{Relay, RelayError, RelayHello, FINGERPRINT_LEN, SESSION_TOKEN_LEN};
pub use server::{Server, ServerError};

/// Default port `rendezvousd` listens on for TCP control-channel
/// connections from `p2p-transfer` peers.
pub const DEFAULT_PORT: u16 = 14570;

/// Length-prefixed framed-message read/write helpers shared by client and
/// server. Kept private to this crate — peers don't speak this wire
/// format anywhere except against the rendezvous.
mod framing {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::protocol::{Message, RendezvousProtoError};

    /// Hard cap on a single rendezvous frame. The protocol only carries
    /// codes + endpoints + fingerprints; nothing legitimate is large.
    const MAX_FRAME_BYTES: u32 = 4096;

    pub(crate) async fn write_message<W>(w: &mut W, msg: &Message) -> Result<(), RendezvousProtoError>
    where
        W: AsyncWriteExt + Unpin,
    {
        let payload = rmp_serde::to_vec(msg).map_err(RendezvousProtoError::Encode)?;
        if payload.len() as u32 > MAX_FRAME_BYTES {
            return Err(RendezvousProtoError::FrameTooLarge {
                size: payload.len() as u32,
                cap: MAX_FRAME_BYTES,
            });
        }
        w.write_all(&(payload.len() as u32).to_be_bytes())
            .await
            .map_err(RendezvousProtoError::Io)?;
        w.write_all(&payload).await.map_err(RendezvousProtoError::Io)?;
        w.flush().await.map_err(RendezvousProtoError::Io)?;
        Ok(())
    }

    pub(crate) async fn read_message<R>(r: &mut R) -> Result<Message, RendezvousProtoError>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut len_buf = [0u8; 4];
        r.read_exact(&mut len_buf).await.map_err(RendezvousProtoError::Io)?;
        let len = u32::from_be_bytes(len_buf);
        if len > MAX_FRAME_BYTES {
            return Err(RendezvousProtoError::FrameTooLarge {
                size: len,
                cap: MAX_FRAME_BYTES,
            });
        }
        let mut payload = vec![0u8; len as usize];
        r.read_exact(&mut payload).await.map_err(RendezvousProtoError::Io)?;
        rmp_serde::from_slice(&payload).map_err(RendezvousProtoError::Decode)
    }
}
