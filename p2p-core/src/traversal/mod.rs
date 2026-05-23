//! NAT traversal orchestrator (Phase 1).
//!
//! Owns the UDP socket lifecycle: bind → STUN probe → rendezvous endpoint
//! exchange → simultaneous QUIC connect/accept hole-punch → hand-off to
//! [`crate::network::quic::QuicConnection`].
//!
//! Phase 0 ships an empty scaffold so the rest of the crate compiles
//! against the eventual public surface; the bulk of the implementation
//! lands together with the `p2p-rendezvous` crate.

pub mod stun;

// `establish_via_rendezvous`, `race_connect_and_accept`, and the
// `RendezvousClient` glue live here once Phase 1 starts.
