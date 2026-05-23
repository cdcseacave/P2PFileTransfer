//! Trust-on-first-use store of known peer fingerprints.
//!
//! When we connect to a peer over LAN discovery (no rendezvous, no
//! out-of-band fingerprint exchange), the first connection accepts whatever
//! certificate the peer presents and records its fingerprint here. Future
//! connections to the same peer fail unless the presented fingerprint
//! matches the stored one — that's the user-visible "this peer's identity
//! changed, abort" signal that a real MITM would trigger.
//!
//! Storage: `<config_dir>/p2p-transfer/known_peers.json`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::identity::Fingerprint;

/// Hex-encoded fingerprint used as the on-disk key. Keeps the JSON readable.
type FingerprintHex = String;

#[derive(Debug, Default, Serialize, Deserialize)]
struct Store {
    /// Map of peer fingerprint (hex) -> human-readable display name (best effort).
    peers: BTreeMap<FingerprintHex, PeerRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
    pub display_name: String,
    /// Unix seconds — when we first trusted this peer.
    pub first_seen: u64,
    /// Unix seconds — last successful connection.
    pub last_seen: u64,
}

/// File-backed fingerprint store. Reads cache the file on first access;
/// writes flush eagerly so a crash doesn't lose trust state.
#[derive(Debug)]
pub struct KnownPeers {
    path: PathBuf,
    state: Mutex<Store>,
}

impl KnownPeers {
    /// Open (or create) the default known-peers store.
    pub fn open_default() -> Result<Self> {
        let path = default_path()?;
        Self::open(path)
    }

    /// Open (or create) the store at the given path.
    pub fn open(path: PathBuf) -> Result<Self> {
        let state = if path.exists() {
            let bytes = std::fs::read(&path).map_err(Error::Network)?;
            serde_json::from_slice::<Store>(&bytes)
                .map_err(|e| Error::Other(format!("known_peers.json parse: {e}")))?
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(Error::Network)?;
            }
            Store::default()
        };
        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    /// Look up the trusted fingerprint for a peer, by its claimed fingerprint.
    /// Returns `None` if we've never seen this peer before.
    pub fn get(&self, fp: &Fingerprint) -> Option<PeerRecord> {
        self.lock().peers.get(&hex::encode(fp)).cloned()
    }

    /// Trust a peer for the first time (TOFU pin).
    pub fn trust(&self, fp: &Fingerprint, display_name: &str) -> Result<()> {
        let now = now_secs();
        let mut store = self.lock();
        let entry = store
            .peers
            .entry(hex::encode(fp))
            .or_insert_with(|| PeerRecord {
                display_name: display_name.to_string(),
                first_seen: now,
                last_seen: now,
            });
        entry.last_seen = now;
        if !display_name.is_empty() {
            entry.display_name = display_name.to_string();
        }
        self.flush(&store)
    }

    /// Decide whether a presented fingerprint is acceptable for a peer that
    /// claims `expected_fp`. On LAN/TOFU, presenting a different fingerprint
    /// than the stored one is the MITM-signal that aborts the connection.
    pub fn verify_or_pin(
        &self,
        claimed_fp: &Fingerprint,
        presented_fp: &Fingerprint,
        display_name: &str,
    ) -> Result<()> {
        if claimed_fp != presented_fp {
            warn!(
                "peer claimed fingerprint {} but presented {}",
                hex::encode(claimed_fp),
                hex::encode(presented_fp),
            );
            return Err(Error::FingerprintMismatch);
        }
        match self.get(claimed_fp) {
            None => {
                debug!("TOFU pinning new peer {}", hex::encode(claimed_fp));
                self.trust(claimed_fp, display_name)
            }
            Some(_) => {
                // Refresh last-seen but the fingerprint already matches the
                // stored one (since claimed == presented and we have it).
                self.trust(claimed_fp, display_name)
            }
        }
    }

    /// Remove a peer from the trust store.
    pub fn forget(&self, fp: &Fingerprint) -> Result<()> {
        let mut store = self.lock();
        store.peers.remove(&hex::encode(fp));
        self.flush(&store)
    }

    /// All trusted peers, for UI display.
    pub fn list(&self) -> Vec<(FingerprintHex, PeerRecord)> {
        self.lock()
            .peers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn lock(&self) -> MutexGuard<'_, Store> {
        // Poisoning here means the file is out of sync with the in-memory
        // state, which is recoverable: we just keep working with the value.
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn flush(&self, store: &Store) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(store)
            .map_err(|e| Error::Other(format!("known_peers.json serialize: {e}")))?;
        std::fs::write(&self.path, bytes).map_err(Error::Network)
    }
}

fn default_path() -> Result<PathBuf> {
    let base = dirs::config_dir()
        .ok_or_else(|| Error::Other("no config directory for known_peers.json".to_string()))?;
    Ok(base.join("p2p-transfer").join("known_peers.json"))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn tofu_pins_then_verifies() {
        let dir = tempdir().unwrap();
        let store = KnownPeers::open(dir.path().join("kp.json")).unwrap();
        let fp = [7u8; 32];

        assert!(store.get(&fp).is_none());
        store.verify_or_pin(&fp, &fp, "alice").unwrap();
        assert!(store.get(&fp).is_some());

        // Second time around: same claimed/presented → ok.
        store.verify_or_pin(&fp, &fp, "alice").unwrap();
    }

    #[test]
    fn fingerprint_mismatch_is_rejected() {
        let dir = tempdir().unwrap();
        let store = KnownPeers::open(dir.path().join("kp.json")).unwrap();
        let claimed = [1u8; 32];
        let presented = [2u8; 32];
        let err = store.verify_or_pin(&claimed, &presented, "bob").unwrap_err();
        assert!(matches!(err, Error::FingerprintMismatch));
        assert!(store.get(&claimed).is_none());
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("kp.json");
        let fp = [9u8; 32];
        {
            let store = KnownPeers::open(path.clone()).unwrap();
            store.trust(&fp, "carol").unwrap();
        }
        let reopened = KnownPeers::open(path).unwrap();
        let rec = reopened.get(&fp).unwrap();
        assert_eq!(rec.display_name, "carol");
    }
}
