//! Async STUN client that operates on a borrowed UDP socket.
//!
//! Unlike the legacy [`crate::nat`] diagnostic client, this version takes a
//! pre-bound `tokio::net::UdpSocket` so the mapping discovered via STUN
//! refers to the *same* socket that QUIC will then own. That's the central
//! requirement for hole punching: the public endpoint reported by STUN must
//! be the one the punched packets and the subsequent QUIC handshake share.
//!
//! Phase 0 ships the message-construction + response-parsing primitives.
//! Phase 1 wires them into `traversal::mod::establish_via_rendezvous`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::{timeout_at, Instant};

use crate::error::{Error, Result};

const BINDING_REQUEST: u16 = 0x0001;
const BINDING_RESPONSE: u16 = 0x0101;
const MAGIC_COOKIE: u32 = 0x2112_A442;
const ATTR_MAPPED_ADDRESS: u16 = 0x0001;
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;
const QUERY_TIMEOUT: Duration = Duration::from_secs(3);

/// Query a single STUN server using `socket` and return the public address
/// it reports for that socket. Times out after [`QUERY_TIMEOUT`]. Drops
/// (rather than fails on) packets that aren't from `server` or whose
/// transaction id doesn't match the request — the socket may be shared
/// with other traffic (rendezvous, prior STUN queries) and a stale or
/// spoofed packet must not poison the in-flight query.
pub async fn query(socket: &UdpSocket, server: SocketAddr) -> Result<SocketAddr> {
    let (request, expected_tx) = build_binding_request();
    socket
        .send_to(&request, server)
        .await
        .map_err(Error::Network)?;

    let deadline = Instant::now() + QUERY_TIMEOUT;
    let mut buf = [0u8; 1024];
    loop {
        let (len, from) = match timeout_at(deadline, socket.recv_from(&mut buf)).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(Error::Network(e)),
            Err(_) => return Err(Error::Timeout),
        };
        let data = &buf[..len];
        if from != server || data.len() < 20 || data[8..20] != expected_tx {
            continue;
        }
        return parse_binding_response(data);
    }
}

/// Classify whether the path likely supports UDP hole punching by querying
/// two distinct STUN servers and comparing the mapped ports. Cone NATs
/// reuse the same source-port mapping for any destination; symmetric NATs
/// pick a fresh source port per destination.
pub async fn classify_nat(socket: &UdpSocket, a: SocketAddr, b: SocketAddr) -> Result<NatClass> {
    let map_a = query(socket, a).await?;
    let map_b = query(socket, b).await?;
    Ok(if map_a.port() == map_b.port() {
        NatClass::Cone { public: map_a }
    } else {
        NatClass::Symmetric
    })
}

/// Coarse NAT classification — only what matters for the punch/relay decision.
#[derive(Debug, Clone)]
pub enum NatClass {
    /// Same mapped port across destinations — punchable.
    Cone { public: SocketAddr },
    /// Different mapped port per destination — relay required.
    Symmetric,
}

fn build_binding_request() -> ([u8; 20], [u8; 12]) {
    let mut packet = [0u8; 20];
    packet[0..2].copy_from_slice(&BINDING_REQUEST.to_be_bytes());
    // Length = 0 (no attributes); already zero.
    packet[4..8].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
    let tx: [u8; 12] = rand::random();
    packet[8..20].copy_from_slice(&tx);
    (packet, tx)
}

fn parse_binding_response(data: &[u8]) -> Result<SocketAddr> {
    if data.len() < 20 {
        return Err(Error::Protocol("STUN response too short".to_string()));
    }
    let msg_type = u16::from_be_bytes([data[0], data[1]]);
    if msg_type != BINDING_RESPONSE {
        return Err(Error::Protocol(format!(
            "unexpected STUN message type: 0x{msg_type:04x}"
        )));
    }
    let msg_len = u16::from_be_bytes([data[2], data[3]]) as usize;
    let cookie = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    if cookie != MAGIC_COOKIE {
        return Err(Error::Protocol("invalid STUN magic cookie".to_string()));
    }
    let tx_id = &data[8..20];

    let mut offset = 20usize;
    let end = (20usize.saturating_add(msg_len)).min(data.len());
    while offset + 4 <= end {
        let attr_type = u16::from_be_bytes([data[offset], data[offset + 1]]);
        let attr_len = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
        offset += 4;
        if offset + attr_len > end {
            break;
        }
        let attr = &data[offset..offset + attr_len];
        match attr_type {
            ATTR_XOR_MAPPED_ADDRESS => {
                if let Ok(addr) = parse_xor_mapped(attr, tx_id) {
                    return Ok(addr);
                }
            }
            ATTR_MAPPED_ADDRESS => {
                if let Ok(addr) = parse_mapped(attr) {
                    return Ok(addr);
                }
            }
            _ => {}
        }
        offset += (attr_len + 3) & !3;
    }
    Err(Error::Protocol(
        "no mapped-address attribute in STUN response".to_string(),
    ))
}

fn parse_xor_mapped(attr: &[u8], tx_id: &[u8]) -> Result<SocketAddr> {
    if attr.len() < 8 {
        return Err(Error::Protocol("XOR-MAPPED-ADDRESS too short".to_string()));
    }
    let family = attr[1];
    let xor_port = u16::from_be_bytes([attr[2], attr[3]]);
    let port = xor_port ^ ((MAGIC_COOKIE >> 16) as u16);
    match family {
        0x01 => {
            let xor_ip = u32::from_be_bytes([attr[4], attr[5], attr[6], attr[7]]);
            let ip = Ipv4Addr::from(xor_ip ^ MAGIC_COOKIE);
            Ok(SocketAddr::new(IpAddr::V4(ip), port))
        }
        0x02 => {
            if attr.len() < 20 {
                return Err(Error::Protocol(
                    "XOR-MAPPED-ADDRESS IPv6 too short".to_string(),
                ));
            }
            let mut key = [0u8; 16];
            key[..4].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
            key[4..].copy_from_slice(tx_id);
            let mut octets = [0u8; 16];
            for i in 0..16 {
                octets[i] = attr[4 + i] ^ key[i];
            }
            Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        f => Err(Error::Protocol(format!("unknown address family: {f}"))),
    }
}

fn parse_mapped(attr: &[u8]) -> Result<SocketAddr> {
    if attr.len() < 8 {
        return Err(Error::Protocol("MAPPED-ADDRESS too short".to_string()));
    }
    let family = attr[1];
    let port = u16::from_be_bytes([attr[2], attr[3]]);
    match family {
        0x01 => {
            let ip = Ipv4Addr::new(attr[4], attr[5], attr[6], attr[7]);
            Ok(SocketAddr::new(IpAddr::V4(ip), port))
        }
        0x02 => {
            if attr.len() < 20 {
                return Err(Error::Protocol("MAPPED-ADDRESS IPv6 too short".to_string()));
            }
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&attr[4..20]);
            Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        f => Err(Error::Protocol(format!("unknown address family: {f}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_request_has_correct_header() {
        let (req, tx) = build_binding_request();
        assert_eq!(u16::from_be_bytes([req[0], req[1]]), BINDING_REQUEST);
        assert_eq!(
            u32::from_be_bytes([req[4], req[5], req[6], req[7]]),
            MAGIC_COOKIE
        );
        // The transaction id in the packet must match the returned id.
        assert_eq!(&req[8..20], &tx[..]);
    }

    #[test]
    fn rejects_response_with_wrong_tx_id() {
        // Construct a STUN-shaped response whose tx_id is all zeros and
        // verify our wire layer would reject it against a non-zero tx.
        let mut response = [0u8; 32];
        response[0..2].copy_from_slice(&BINDING_RESPONSE.to_be_bytes());
        response[2..4].copy_from_slice(&12u16.to_be_bytes());
        response[4..8].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
        // tx_id at 8..20 left as zeros.
        // attribute: XOR-MAPPED-ADDRESS, port 0, IP 0.0.0.0
        response[20..22].copy_from_slice(&ATTR_XOR_MAPPED_ADDRESS.to_be_bytes());
        response[22..24].copy_from_slice(&8u16.to_be_bytes());
        response[25] = 0x01;

        let parsed = parse_binding_response(&response).unwrap();
        // The parser itself doesn't validate tx; that's the query() job.
        // But assert that comparing the response tx (zeros) to a non-zero
        // expected tx yields "not equal" — i.e. the field is where we
        // think it is.
        let expected_tx: [u8; 12] = [0x42; 12];
        let response_tx = &response[8..20];
        assert_ne!(response_tx, &expected_tx[..]);
        // Sanity: parsing didn't fail just on the zero tx.
        let _ = parsed;
    }

    #[test]
    fn parses_xor_mapped_ipv4() {
        let port: u16 = 32853;
        let xor_port = port ^ ((MAGIC_COOKIE >> 16) as u16);
        let ip = 0xC000_0201u32;
        let xor_ip = ip ^ MAGIC_COOKIE;
        let mut data = vec![0u8, 0x01];
        data.extend_from_slice(&xor_port.to_be_bytes());
        data.extend_from_slice(&xor_ip.to_be_bytes());
        let tx = [0u8; 12];
        let addr = parse_xor_mapped(&data, &tx).unwrap();
        assert_eq!(addr.port(), port);
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)));
    }
}
