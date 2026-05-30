# P2P File Transfer

A peer-to-peer file transfer tool in Rust. Two peers establish an
authenticated **QUIC** connection (TLS 1.3 with **mutual auth**, both
ends cert-pinned by SHA-256) and stream files chunk-by-chunk over
per-chunk unidirectional QUIC streams. Cross-NAT pairing through a
self-hosted rendezvous server, with a UDP relay fallback for symmetric
NATs. Ships with a CLI and an optional Iced GUI.

## Highlights

* **QUIC + mutual TLS 1.3** on a single UDP socket — encryption and
  client-cert authentication are mandatory. Both peers pin each
  other's cert by SHA-256 fingerprint.
* **Per-device identity** — Ed25519 keypair + self-signed cert,
  persisted across runs.
* **LAN auto-discovery** — UDP beacons announce device name + cert
  fingerprint so receivers can pin immediately.
* **Cross-NAT pairing** — `p2p-rendezvous` crate + `rendezvousd`
  binary; peers exchange short codes and the server matches them by
  public endpoint + cert fingerprint. UDP hole-punching uses the QUIC
  Initial packets themselves.
* **Relay fallback** — symmetric NATs that can't be punched directly
  fall through to a UDP forwarder; QUIC TLS still terminates
  end-to-end (the relay sees ciphertext only).
* **Resume** — chunk-level bitmap persisted per transfer; reconnects
  pick up where they left off. Chunk indices are `u64` end-to-end —
  very large files transfer correctly.
* **Integrity** — per-file SHA-256 exchanged both ways; receiver
  mismatch is a hard failure (no silent acceptance).
* **Path safety** — every incoming relative path is sanitized; the
  receiver rejects absolute paths, `..`, `.`, drive letters, and UNC
  roots.
* **Adaptive zstd compression** — auto-disabled when data is
  incompressible.
* **Bandwidth throttling** — token-bucket cap (`--max-speed 10M`).
* **GUI** (optional) — Iced-based tabs for Connection / Send / Receive /
  Settings / History; the Connection tab has a `Pair with code` mode
  for cross-NAT setup.

## Build

```
cargo build --release                                       # CLI only
cargo build --release --features full                       # CLI + GUI
cargo build --release --features gui --no-default-features  # GUI only
```

The default binary is the CLI; passing no subcommand launches the GUI
when built with `--features gui|full`.

## CLI

### Receive

```
p2p-transfer receive --output ./received --port 14567 --auto-accept
```

On first run a long-lived identity is generated at
`<config_dir>/p2p-transfer/identity.{key,cert}`. The startup log prints
this device's fingerprint — share it with the sender.

### Send (direct)

```
p2p-transfer send ./bigfile.bin \
    --peer 192.168.1.42:14567 \
    --peer-fingerprint 94524738f9fd3fc60162f67f62178533d18f352f61df70d5bd47bca9bbbb66cc
```

`--peer-fingerprint` is required and is the 64-hex-char SHA-256 of the
receiver's cert (printed when the receiver starts up).

### Send (LAN auto-discovery)

```
p2p-transfer send ./bigfile.bin --discover
```

Picks the first peer that broadcasts a beacon; pulls its cert
fingerprint straight from the beacon, no flag needed.

### Discover

```
p2p-transfer discover --timeout 10
```

Lists every peer broadcasting beacons during the timeout, with their
addresses, device IDs, and cert fingerprints.

### NAT diagnostic

```
p2p-transfer nat-test
p2p-transfer nat-test --stun-server stun.cloudflare.com:3478
p2p-transfer nat-test --rendezvous rendezvous.example.com:14570
```

* Without `--rendezvous`: queries two STUN servers on the same UDP
  socket and reports `Cone` (UDP hole-punching will work) or
  `Symmetric` (relay required).
* With `--rendezvous`: stands up two local peers, registers both at
  the given rendezvous with a fresh code, and races a QUIC handshake
  between them. Reports `direct` / `relay` / `failed` plus latency —
  the cleanest end-to-end check that your rendezvous + (optional)
  relay setup actually works.

### Cross-NAT pairing through a rendezvous

When the two peers are on different networks and you don't want to (or
can't) port-forward, run a small rendezvous server somewhere reachable
to both sides (a free-tier VPS, a docker-compose stack, your home
router):

```
# On the rendezvous host:
rendezvousd --bind 0.0.0.0:14570
```

Then both peers run:

```
# Sender
p2p-transfer send ./bigfile.bin \
    --rendezvous rendezvous.example.com:14570 \
    --code ABC123

# Receiver
p2p-transfer receive --output ./received \
    --rendezvous rendezvous.example.com:14570 \
    --code ABC123
```

Whichever peer types the same `--code` first waits up to 5 minutes for
the other; once both have arrived they exchange public endpoints + cert
fingerprints and complete the QUIC handshake by UDP hole-punching. The
rendezvous never sees the file data — it only matches peers.

### Relay fallback (symmetric NAT)

Symmetric NATs can't be punched directly. Run `rendezvousd` with a relay
attached so peers can fall back to a forwarder when the punch fails:

```
rendezvousd --bind 0.0.0.0:14570 \
            --relay-bind 0.0.0.0:14571 \
            --max-relay-mbps 50
```

Peers automatically request the relay when STUN spots a symmetric NAT.
You can also force the relay path for debugging by passing
`--force-relay` on `send` / `receive`. The relay just forwards UDP
packets between the two peers — QUIC TLS still terminates end-to-end so
the relay only sees ciphertext.

The rendezvous applies several anti-abuse measures: a per-process
concurrency cap (default 1024 simultaneous handlers, backpressured at
the listener), the registered `public_endpoint` IP is replaced by the
TCP source IP server-side (the user-supplied UDP port is kept — only
the IP is forgeable for traffic reflection), and the relay's slot
binding pins each session's two seats to specific cert fingerprints
upfront so impostors with only the session token can't take a seat.

### Self-hosting `rendezvousd` on a VPS

A scripted, idempotent installer for Ubuntu 24+ lives at `scripts/deploy.py`.
It runs end-to-end from a clean box — apt deps, rust toolchain, repo clone,
release build, systemd unit, dedicated `rendezvous` system user, UFW rules
— and is safe to re-run any time to update.

On a fresh VPS you don't need to clone the repo first — fetch just the
deploy script and it will do the clone itself:

```bash
sudo apt-get install -y python3 curl
curl -fsSL https://raw.githubusercontent.com/cdcseacave/P2PFileTransfer/develop/scripts/deploy.py -o deploy.py
```

Then drive it:

```bash
# First install (clones to /opt/p2p, builds, starts the service) or same command to
# Update later (pulls latest develop, rebuilds, restarts only if changed)
sudo python3 deploy.py install /opt/p2p

# Pin to a different branch
sudo python3 deploy.py install /opt/p2p --branch main

# Reclaim disk after a successful install (deletes target/, keeps the
# /usr/local/bin/rendezvousd binary and the running service)
sudo python3 deploy.py install /opt/p2p --prune-build
sudo python3 deploy.py clean-build /opt/p2p     # standalone form

# Full teardown
sudo python3 deploy.py uninstall                       # keeps repo
sudo python3 deploy.py uninstall --purge-repo /opt/p2p # removes repo too
```

The installer compares the freshly built binary's SHA256 against the
installed copy and only restarts the service when it actually changed, so
no-op re-runs don't interrupt active pairings. A `clean-build` + later
`install` works fine — cargo just rebuilds `target/` from scratch.

### Resume

`resume` accepts the same pairing flags as `send`/`receive` — either
direct addressing or rendezvous-mediated. Pick whichever matches how the
original `send` reached the peer.

```
# Direct (same LAN, or a stable port-forwarded receiver)
p2p-transfer resume <transfer_id> \
    --path ./bigfile.bin \
    --peer 192.168.1.42:14567 \
    --peer-fingerprint <hex>

# Cross-NAT (the receiver is still listening through the same rendezvous + code)
p2p-transfer resume <transfer_id> \
    --path ./bigfile.bin \
    --rendezvous rendezvous.example.com:14570 \
    --code ABC123
```

Reads `transfer_<transfer_id>.json` (written when a transfer is
interrupted) and continues from the chunk bitmap. The state file lives
in the working directory where the transfer started — pass
`--state-dir` if you started the original `send` from somewhere else.
The original `--path` and pairing flags aren't stored, so you have to
supply them again on resume.

### History

```
p2p-transfer history --limit 10
```

## GUI

```
p2p-transfer            # if built with --features gui|full
p2p-transfer gui
```

Tabs: Connection (Listen / Connect / Pair-with-code), Send, Receive,
Settings, History.

The Connection tab's "Pair with code (cross-NAT)" mode takes a
rendezvous server + shared code (with a Generate button) and pairs the
two peers through it; the UI stays responsive during the wait because
session establishment runs off the message loop.

## Performance

Localhost loopback transfer of a 2 MB file completes in ~25 ms over QUIC
(≈80 MB/s, compression on). Real-world LAN throughput is limited
primarily by zstd compression speed and disk I/O.

`benchmark.py` runs an automated sender/receiver harness if you want
numbers on your hardware:

```
python3 benchmark.py --mode sender                    # local
python3 benchmark.py --mode receiver --port 14568     # one machine
python3 benchmark.py --mode sender   --receiver-ip 192.168.1.100 --port 14568
```

## Requirements

* Rust 1.79+
* UDP port 14567 reachable (or whatever you pass to `--port`).
* For LAN discovery, UDP broadcast must not be filtered on the network.
* For cross-NAT pairing, a reachable `rendezvousd` instance (and, if
  any peer is behind a symmetric NAT, the same `rendezvousd` running
  with `--relay-bind` for the UDP forwarder).

## Security model

* TLS 1.3 with **mutual authentication** — both ends present a
  self-signed cert and each side pins the other by SHA-256
  fingerprint at the application layer.
* No CA, no key escrow. The fingerprint is delivered out-of-band: on
  the command line (`--peer-fingerprint`), in the LAN beacon (TOFU
  pinning), or via the rendezvous match.
* The rendezvous server only matches peers — it never sees user data
  and is never trusted to vouch for cert authenticity (the cert is
  cross-checked against the fingerprint at handshake time).
* The relay forwards UDP datagrams verbatim — QUIC TLS terminates
  end-to-end between the two real peers, so the relay only sees
  ciphertext.
* All wire-supplied paths are sanitized before any filesystem write;
  receiver-side SHA-256 mismatch is fatal.

## License

MIT — see `LICENSE`.
