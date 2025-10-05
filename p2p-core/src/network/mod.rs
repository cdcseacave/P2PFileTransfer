//! Network layer abstractions

pub mod framing;
pub mod tcp;
pub mod udp;

pub use framing::{read_message, write_message};
