//! rustls 0.23 configuration for QUIC.
//!
//! QUIC mandates TLS 1.3, so there's no "negotiate or skip encryption"
//! mode — every connection is encrypted. We don't use a CA hierarchy: each
//! device presents a long-lived self-signed cert (see [`crate::identity`])
//! and the peer pins it by SHA-256 fingerprint.
//!
//! Three roles use this module:
//!
//! * The QUIC server endpoint builds a [`rustls::ServerConfig`] with the
//!   local cert/key and signals it accepts any client cert.
//! * The QUIC client endpoint builds a [`rustls::ClientConfig`] with a
//!   [`FingerprintVerifier`] that compares the presented cert's SHA-256
//!   against the expected fingerprint (received out of band — beacon, code,
//!   rendezvous).
//! * Both sides advertise the ALPN protocol `ALPN_PROTOCOL` from `lib.rs`.

use std::sync::Arc;
use std::sync::OnceLock;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};

use crate::error::{Error, Result};
use crate::identity::{fingerprint_of, Fingerprint, Identity};
use crate::ALPN_PROTOCOL;

/// Install rustls's process-wide crypto provider once. Safe to call repeatedly.
pub fn install_default_crypto_provider() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        // Ignore the result: another caller (or a transitive dep) may have
        // installed it first, which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Build a TLS 1.3 server config presenting the local device identity
/// and requiring the client to present its own cert. The cert chain isn't
/// rooted in any CA — the client cert is recorded by the TLS layer and the
/// handshake layer (see [`crate::handshake`]) cross-checks its fingerprint
/// against the value the peer claims in HELLO. Without mutual TLS that
/// cross-check would have nothing to compare against on the responder
/// side (peer_identity would always be `None`), so the HELLO claim would
/// be unverified.
pub fn server_config(identity: &Identity) -> Result<Arc<rustls::ServerConfig>> {
    install_default_crypto_provider();

    let cert_chain = vec![identity.cert_der()];
    let key = identity.private_key_der();

    let mut cfg = rustls::ServerConfig::builder()
        .with_client_cert_verifier(Arc::new(AcceptAnyClientCert::new()))
        .with_single_cert(cert_chain, key)
        .map_err(|e| Error::Tls(format!("server config: {e}")))?;
    cfg.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
    // Required for quinn's `QuicServerConfig::try_from`: enables 0-RTT-sized
    // early data window. Quinn rejects anything other than 0 or u32::MAX.
    cfg.max_early_data_size = u32::MAX;
    Ok(Arc::new(cfg))
}

/// Build a TLS 1.3 client config that pins the server cert's SHA-256 to
/// `expected_fingerprint`. The cert chain itself is not validated against
/// any trust root; pinning is the whole story.
pub fn client_config_pinning(
    expected_fingerprint: Fingerprint,
    identity: &Identity,
) -> Result<Arc<rustls::ClientConfig>> {
    install_default_crypto_provider();

    let verifier = Arc::new(FingerprintVerifier::new(expected_fingerprint));

    // Present our cert so the responder's mutual-TLS verifier sees our
    // SPKI and the application-layer HELLO cross-check has something
    // authoritative to compare against.
    let cert_chain = vec![identity.cert_der()];
    let key = identity.private_key_der();
    let mut cfg = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(cert_chain, key)
        .map_err(|e| Error::Tls(format!("client auth cert: {e}")))?;
    cfg.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
    Ok(Arc::new(cfg))
}

/// rustls verifier that accepts exactly one peer certificate, identified by
/// its SHA-256 fingerprint. Signature verification (proving the peer holds
/// the private key) is delegated to the active crypto provider — we only
/// override identity pinning, not cryptographic checks.
#[derive(Debug)]
pub struct FingerprintVerifier {
    expected: Fingerprint,
    schemes: Vec<SignatureScheme>,
}

impl FingerprintVerifier {
    pub fn new(expected: Fingerprint) -> Self {
        let provider = rustls::crypto::ring::default_provider();
        let schemes = provider
            .signature_verification_algorithms
            .supported_schemes();
        Self { expected, schemes }
    }
}

impl ServerCertVerifier for FingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        let presented = fingerprint_of(end_entity);
        if presented == self.expected {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "peer fingerprint mismatch (expected {}, got {})",
                hex::encode(self.expected),
                hex::encode(presented),
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}

/// Client-cert verifier that accepts any presented certificate — peer
/// identity is authenticated at the application layer by cross-checking
/// the HELLO fingerprint against the cert TLS captured here. The whole
/// reason for requiring the client cert at all is so the responder's
/// `peer_fingerprint()` returns `Some`; the verifier itself doesn't pin.
#[derive(Debug)]
pub struct AcceptAnyClientCert {
    schemes: Vec<SignatureScheme>,
}

impl Default for AcceptAnyClientCert {
    fn default() -> Self {
        Self::new()
    }
}

impl AcceptAnyClientCert {
    pub fn new() -> Self {
        let provider = rustls::crypto::ring::default_provider();
        let schemes = provider
            .signature_verification_algorithms
            .supported_schemes();
        Self { schemes }
    }
}

impl ClientCertVerifier for AcceptAnyClientCert {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> std::result::Result<ClientCertVerified, rustls::Error> {
        // Trust any presented cert at the TLS layer. The handshake layer
        // pins it against the HELLO fingerprint right after.
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_server_and_client_configs() {
        let identity = Identity::generate().unwrap();
        let fp = identity.fingerprint();
        let server = server_config(&identity).unwrap();
        let client = client_config_pinning(fp, &identity).unwrap();
        assert_eq!(server.alpn_protocols, vec![ALPN_PROTOCOL.to_vec()]);
        assert_eq!(client.alpn_protocols, vec![ALPN_PROTOCOL.to_vec()]);
    }

    #[test]
    fn fingerprint_verifier_rejects_other_cert() {
        let target = Identity::generate().unwrap();
        let attacker = Identity::generate().unwrap();
        let verifier = FingerprintVerifier::new(target.fingerprint());

        let cert = attacker.cert_der();
        let res = verifier.verify_server_cert(
            &cert,
            &[],
            &ServerName::try_from("p2p-transfer").unwrap(),
            &[],
            UnixTime::now(),
        );
        assert!(res.is_err());
    }

    #[test]
    fn fingerprint_verifier_accepts_pinned_cert() {
        let identity = Identity::generate().unwrap();
        let verifier = FingerprintVerifier::new(identity.fingerprint());
        let cert = identity.cert_der();
        let res = verifier.verify_server_cert(
            &cert,
            &[],
            &ServerName::try_from("p2p-transfer").unwrap(),
            &[],
            UnixTime::now(),
        );
        assert!(res.is_ok());
    }
}
