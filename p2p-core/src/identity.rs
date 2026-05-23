//! Per-device long-lived identity.
//!
//! On first run we generate an Ed25519 keypair and a self-signed X.509
//! certificate, persist both to the user's config directory, and reuse
//! them for every subsequent run. The certificate's SHA-256 fingerprint
//! is the stable per-device identifier used for TLS pinning and for the
//! `device_id` exposed in discovery beacons and handshakes.
//!
//! Files written:
//!   <config_dir>/p2p-transfer/identity.key   (PEM-encoded PKCS#8 Ed25519)
//!   <config_dir>/p2p-transfer/identity.cert  (PEM-encoded X.509)

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use sha2::{Digest, Sha256};
use tracing::{debug, info};

use crate::error::{Error, Result};

/// SHA-256 fingerprint of a certificate's DER encoding. Used everywhere
/// we need to refer to a peer's identity off the wire.
pub type Fingerprint = [u8; 32];

/// Loaded device identity: keypair + cert + cached fingerprint.
///
/// Cloneable because the underlying PEM strings and DER bytes are cheap;
/// `Arc<Identity>` is fine when many tasks need to read it concurrently.
#[derive(Debug, Clone)]
pub struct Identity {
    cert_der: Arc<CertificateDer<'static>>,
    key_der: Arc<PrivatePkcs8KeyDer<'static>>,
    fingerprint: Fingerprint,
}

impl Identity {
    /// Load the identity from the default location, generating + persisting
    /// a fresh one if none exists.
    pub fn load_or_generate() -> Result<Self> {
        let dir = default_identity_dir()?;
        Self::load_or_generate_in(&dir)
    }

    /// Load the identity from `dir`, generating + persisting a fresh one if
    /// none exists. Exposed for tests that want a temporary directory.
    pub fn load_or_generate_in(dir: &Path) -> Result<Self> {
        let key_path = dir.join("identity.key");
        let cert_path = dir.join("identity.cert");

        if key_path.exists() && cert_path.exists() {
            debug!("Loading device identity from {}", dir.display());
            return Self::load(&key_path, &cert_path);
        }

        info!("Generating new device identity at {}", dir.display());
        std::fs::create_dir_all(dir).map_err(Error::Network)?;
        let identity = Self::generate()?;
        identity.persist(&key_path, &cert_path)?;
        Ok(identity)
    }

    /// Generate a fresh Ed25519 keypair + matching self-signed cert in memory.
    pub fn generate() -> Result<Self> {
        let key_pair = KeyPair::generate_for(&rcgen::PKCS_ED25519)
            .map_err(|e| Error::Tls(format!("keypair generation failed: {e}")))?;
        Self::from_key_pair(key_pair)
    }

    fn from_key_pair(key_pair: KeyPair) -> Result<Self> {
        let mut params = CertificateParams::new(vec!["p2p-transfer".to_string()])
            .map_err(|e| Error::Tls(format!("cert params: {e}")))?;
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "p2p-transfer device");
        params.distinguished_name = dn;

        let cert = params
            .self_signed(&key_pair)
            .map_err(|e| Error::Tls(format!("self-sign: {e}")))?;

        let cert_der: CertificateDer<'static> = cert.der().clone();
        let key_der_bytes = key_pair.serialize_der();
        let key_der: PrivatePkcs8KeyDer<'static> = PrivatePkcs8KeyDer::from(key_der_bytes);

        let fingerprint = fingerprint_of(&cert_der);

        Ok(Self {
            cert_der: Arc::new(cert_der),
            key_der: Arc::new(key_der),
            fingerprint,
        })
    }

    fn load(key_path: &Path, cert_path: &Path) -> Result<Self> {
        let key_pem = std::fs::read_to_string(key_path).map_err(Error::Network)?;
        let cert_pem = std::fs::read_to_string(cert_path).map_err(Error::Network)?;

        let key_der_bytes = pem_to_der(&key_pem, "PRIVATE KEY")?;
        let cert_der_bytes = pem_to_der(&cert_pem, "CERTIFICATE")?;

        let cert_der: CertificateDer<'static> = CertificateDer::from(cert_der_bytes);
        let key_der: PrivatePkcs8KeyDer<'static> = PrivatePkcs8KeyDer::from(key_der_bytes);
        let fingerprint = fingerprint_of(&cert_der);

        Ok(Self {
            cert_der: Arc::new(cert_der),
            key_der: Arc::new(key_der),
            fingerprint,
        })
    }

    fn persist(&self, key_path: &Path, cert_path: &Path) -> Result<()> {
        let key_pem = der_to_pem(self.key_der.secret_pkcs8_der(), "PRIVATE KEY");
        let cert_pem = der_to_pem(self.cert_der.as_ref(), "CERTIFICATE");

        write_restricted(key_path, key_pem.as_bytes())?;
        std::fs::write(cert_path, cert_pem.as_bytes()).map_err(Error::Network)?;
        Ok(())
    }

    /// DER-encoded certificate ready for handing to `rustls`.
    pub fn cert_der(&self) -> CertificateDer<'static> {
        (*self.cert_der).clone()
    }

    /// PKCS#8 DER-encoded private key ready for handing to `rustls`.
    pub fn private_key_der(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            self.key_der.secret_pkcs8_der().to_vec(),
        ))
    }

    /// Stable fingerprint = SHA-256 of the certificate DER. This is the
    /// identifier other peers will pin against when talking to us.
    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    /// Hex-encoded fingerprint, for log messages and short-code display.
    pub fn fingerprint_hex(&self) -> String {
        hex::encode(self.fingerprint)
    }
}

/// Compute the canonical fingerprint for a peer certificate.
pub fn fingerprint_of(cert: &CertificateDer<'_>) -> Fingerprint {
    let mut hasher = Sha256::new();
    hasher.update(cert.as_ref());
    hasher.finalize().into()
}

fn default_identity_dir() -> Result<PathBuf> {
    let base = dirs::config_dir().ok_or_else(|| {
        Error::Tls("no config directory available for identity storage".to_string())
    })?;
    Ok(base.join("p2p-transfer"))
}

fn pem_to_der(pem: &str, label: &str) -> Result<Vec<u8>> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let start = pem
        .find(&begin)
        .ok_or_else(|| Error::Tls(format!("PEM missing {label} header")))?
        + begin.len();
    let stop = pem
        .find(&end)
        .ok_or_else(|| Error::Tls(format!("PEM missing {label} footer")))?;
    let body: String = pem[start..stop]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|e| Error::Tls(format!("PEM base64 decode: {e}")))
}

fn der_to_pem(der: &[u8], label: &str) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).expect("base64 ascii"));
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

#[cfg(unix)]
fn write_restricted(path: &Path, data: &[u8]) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(Error::Network)?;
    use std::io::Write;
    f.write_all(data).map_err(Error::Network)?;
    Ok(())
}

#[cfg(not(unix))]
fn write_restricted(path: &Path, data: &[u8]) -> Result<()> {
    // On Windows the per-user config dir already provides ACL-based isolation;
    // we don't manipulate ACLs here.
    std::fs::write(path, data).map_err(Error::Network)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn generates_and_reloads_stable_fingerprint() {
        let dir = tempdir().unwrap();
        let id1 = Identity::load_or_generate_in(dir.path()).unwrap();
        let id2 = Identity::load_or_generate_in(dir.path()).unwrap();
        assert_eq!(
            id1.fingerprint(),
            id2.fingerprint(),
            "fingerprint must be stable across loads"
        );
        assert!(dir.path().join("identity.key").exists());
        assert!(dir.path().join("identity.cert").exists());
    }

    #[test]
    fn fresh_identities_have_distinct_fingerprints() {
        let a = Identity::generate().unwrap();
        let b = Identity::generate().unwrap();
        assert_ne!(a.fingerprint(), b.fingerprint());
        assert_eq!(a.fingerprint_hex().len(), 64);
    }
}
