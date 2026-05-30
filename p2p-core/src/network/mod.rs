//! Network layer abstractions. QUIC is the only transport.

pub mod framing;
pub mod quic;
pub mod udp;

pub use framing::{read_message, write_message};
pub use quic::{QuicConnection, QuicEndpoint};
