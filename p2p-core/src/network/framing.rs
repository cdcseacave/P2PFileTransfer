//! Message framing for the protocol

use crate::error::{Error, Result};
use crate::protocol::Message;
use crate::PROTOCOL_MAGIC;
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum message size (10 MB)
const MAX_MESSAGE_SIZE: u32 = 10 * 1024 * 1024;

/// Write a message to an async writer
pub async fn write_message<W>(writer: &mut W, message: &Message) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    // Serialize message
    let payload = rmp_serde::to_vec(message)?;

    if payload.len() > MAX_MESSAGE_SIZE as usize {
        return Err(Error::Protocol(format!(
            "Message too large: {} bytes",
            payload.len()
        )));
    }

    // Write frame: Magic (4) + Length (4) + Payload (N)
    let mut frame = BytesMut::with_capacity(8 + payload.len());
    frame.put_slice(&PROTOCOL_MAGIC);
    frame.put_u32(payload.len() as u32);
    frame.put_slice(&payload);

    writer.write_all(&frame).await?;
    writer.flush().await?;

    Ok(())
}

/// Read a message from an async reader. A clean close *between frames*
/// (zero bytes available when the next frame would start) maps to
/// [`Error::Disconnected`]; truncation mid-magic, or anywhere else
/// inside a frame, is [`Error::Protocol`].
pub async fn read_message<R>(reader: &mut R) -> Result<Message>
where
    R: AsyncReadExt + Unpin,
{
    // Probe for the first byte of the magic. 0 bytes back == clean
    // between-frames close. Anything <4 bytes after that is mid-frame
    // truncation, not a graceful disconnect.
    let mut magic = [0u8; 4];
    match reader.read(&mut magic[..1]).await {
        Ok(0) => return Err(Error::Disconnected),
        Ok(_) => {}
        Err(e) => {
            // Between-frames close: a peer that called `close(0, "")` surfaces
            // here as an io::Error with one of these kinds (depending on the
            // quinn version and which path the close took). Treat it as a
            // graceful disconnect so the receive loop can re-accept instead of
            // bubbling up "Error: connection lost" after a successful transfer.
            use std::io::ErrorKind::*;
            if matches!(
                e.kind(),
                ConnectionAborted | ConnectionReset | NotConnected | BrokenPipe | UnexpectedEof
            ) {
                return Err(Error::Disconnected);
            }
            return Err(Error::Network(e));
        }
    }
    reader
        .read_exact(&mut magic[1..])
        .await
        .map_err(|e| Error::Protocol(format!("truncated magic: {e}")))?;

    if magic != PROTOCOL_MAGIC {
        return Err(Error::Protocol(format!("Invalid magic bytes: {:?}", magic)));
    }

    // Reads from here on are inside a frame: any short read is a
    // truncation, not a clean disconnect.
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| Error::Protocol(format!("truncated frame header: {e}")))?;
    let len = u32::from_be_bytes(len_buf);

    if len > MAX_MESSAGE_SIZE {
        return Err(Error::Protocol(format!("Message too large: {} bytes", len)));
    }

    let mut payload = vec![0u8; len as usize];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|e| Error::Protocol(format!("truncated frame payload: {e}")))?;

    let message = rmp_serde::from_slice(&payload)?;

    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::HelloMessage;
    use uuid::Uuid;

    #[tokio::test]
    async fn read_on_empty_returns_disconnected() {
        let empty: Vec<u8> = Vec::new();
        let mut cursor = &empty[..];
        let err = read_message(&mut cursor).await.unwrap_err();
        assert!(
            matches!(err, Error::Disconnected),
            "expected Disconnected, got {err:?}"
        );
    }

    #[tokio::test]
    async fn read_partial_magic_returns_protocol_error() {
        // One byte of magic, then EOF — peer crashed mid-frame, not a
        // clean between-frames close.
        let buf = [PROTOCOL_MAGIC[0]];
        let mut cursor = &buf[..];
        let err = read_message(&mut cursor).await.unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "expected Protocol, got {err:?}"
        );
    }

    #[tokio::test]
    async fn read_truncated_frame_returns_protocol_error() {
        // Magic + a length prefix that promises 100 bytes, but no payload.
        let mut buf = Vec::new();
        buf.extend_from_slice(&PROTOCOL_MAGIC);
        buf.extend_from_slice(&100u32.to_be_bytes());
        let mut cursor = &buf[..];
        let err = read_message(&mut cursor).await.unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "expected Protocol, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_write_read_message() {
        let msg = Message::Hello(HelloMessage {
            protocol_version: crate::PROTOCOL_VERSION,
            min_version: crate::MIN_PROTOCOL_VERSION,
            device_id: Uuid::new_v4(),
            cert_fingerprint: [0u8; 32],
        });

        let mut buffer = Vec::new();
        write_message(&mut buffer, &msg).await.unwrap();

        let mut cursor = &buffer[..];
        let read_msg = read_message(&mut cursor).await.unwrap();

        match (msg, read_msg) {
            (Message::Hello(h1), Message::Hello(h2)) => {
                assert_eq!(h1.protocol_version, h2.protocol_version);
                assert_eq!(h1.device_id, h2.device_id);
            }
            _ => panic!("Message type mismatch"),
        }
    }
}
