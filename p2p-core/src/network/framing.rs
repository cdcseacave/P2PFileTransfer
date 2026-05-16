//! Message framing for the protocol

use crate::error::{Error, Result};
use crate::protocol::Message;
use crate::PROTOCOL_MAGIC;
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum message size (128 MB) — large enough for chunked file-list batches plus data chunks
const MAX_MESSAGE_SIZE: u32 = 128 * 1024 * 1024;

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

/// Read a message from an async reader
pub async fn read_message<R>(reader: &mut R) -> Result<Message>
where
    R: AsyncReadExt + Unpin,
{
    // Read magic bytes
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic).await?;

    if magic != PROTOCOL_MAGIC {
        return Err(Error::Protocol(format!("Invalid magic bytes: {:?}", magic)));
    }

    // Read length
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);

    if len > MAX_MESSAGE_SIZE {
        return Err(Error::Protocol(format!("Message too large: {} bytes", len)));
    }

    // Read payload
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload).await?;

    // Deserialize message
    let message = rmp_serde::from_slice(&payload)?;

    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Capabilities, HelloMessage};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_write_read_message() {
        let msg = Message::Hello(HelloMessage {
            protocol_version: 1,
            min_version: 1,
            device_id: Uuid::new_v4(),
            capabilities: Capabilities::all(),
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
