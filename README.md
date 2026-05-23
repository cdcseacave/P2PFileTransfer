# P2P File Transfer

A peer-to-peer file transfer tool in Rust. Two peers establish an
authenticated **QUIC** connection (TLS 1.3, cert-pinned) and stream files
chunk-by-chunk over per-chunk unidirectional QUIC streams. Ships with a
CLI and an optional Iced GUI.

## Highlights

* **QUIC + TLS 1.3** on a single UDP socket — encryption is mandatory.
* **Per-device identity** — Ed25519 keypair + self-signed cert, pinned
  by SHA-256 fingerprint.
* **LAN auto-discovery** — UDP beacons announce device name + cert
  fingerprint so receivers can pin immediately.
* **Resume** — chunk-level bitmap persisted per transfer; reconnects
  pick up where they left off.
* **Adaptive zstd compression** — auto-disabled when data is
  incompressible.
* **Bandwidth throttling** — token-bucket cap (`--max-speed 10M`).
* **GUI** (optional) — Iced-based tabs for Connection / Send / Receive /
  Settings / History.

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
```

Queries two STUN servers on the same UDP socket and reports `Cone` (UDP
hole-punching will work) or `Symmetric` (relay required — Phase 2).

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

Symmetric NATs cannot be punched through and the receiver/sender will
print `Hole punch failed: symmetric NAT detected — enable relay
fallback (Phase 2)`.

### Resume

```
p2p-transfer resume <transfer_id> \
    --to 192.168.1.42:14567 \
    --peer-fingerprint <hex> \
    --path ./bigfile.bin
```

Reads `transfer_<transfer_id>.json` (written when a transfer is
interrupted) and continues from the chunk bitmap.

### History

```
p2p-transfer history --limit 10
```

## GUI

```
p2p-transfer            # if built with --features gui|full
p2p-transfer gui
```

Tabs: Connection (listen or connect), Send, Receive, Settings, History.

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

## License

MIT — see `LICENSE`.
