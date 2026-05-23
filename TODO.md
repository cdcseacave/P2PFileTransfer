# TODO — P2P File Transfer

## Current state

* **Phase 0 — Clean QUIC rewrite** — **done** (2026-05). Single QUIC
  transport with TLS 1.3 + cert pinning; per-chunk uni streams; TCP +
  sliding window + per-chunk CRC + per-chunk ACK + encryption-capability
  bit + `--window-size` / `--max-retries` CLI flags all removed.
  `cargo test --all` and `cargo clippy --all-targets --all-features --
  -D warnings` green.
* **Phase 1 — Rendezvous + UDP hole punching** — **done** (2026-05).
  New `p2p-rendezvous` crate + `rendezvousd` binary; CLI flags
  `--rendezvous` and `--code` on `send` / `receive`;
  `traversal::establish_via_rendezvous` orchestrates STUN +
  registration + race-connect-and-accept punch. Symmetric NAT is
  detected up front by querying two STUN servers and surfaces
  `Error::HolePunchFailed`. `tests/traversal_loopback_test.rs` covers
  the rendezvous + punch primitives end-to-end on localhost (real
  cross-NAT requires a netns harness / two laptops + VPS).

## Active work

### Phase 1.5 — IPv6 + real-world traversal validation

* IPv6: bind a second `quinn::Endpoint` per address family and race the
  punch against both peer endpoints simultaneously (~80 LoC delta in
  `traversal/mod.rs`).
* Linux netns harness in `tests/traversal/`: two namespaces behind
  `iptables -t nat -A POSTROUTING -j MASQUERADE`, rendezvous in a third.
* Real-world: two laptops on different home networks, rendezvous on a
  free-tier VPS, target time-to-pair ≤ 10 s after both sides enter the
  code.

### Phase 2 — QUIC relay fallback

* `rendezvousd --relay-bind <addr>` opens a second `quinn::Endpoint`.
* Both peers `connect` to the relay with a per-session token; the relay
  byte-pipes the two `quinn::Connection`s.
* End-to-end TLS still terminates on the peers because the cert
  fingerprint came from the rendezvous, not the relay (relay sees
  ciphertext only).
* `--max-relay-mbps` rate cap. 1 GB symmetric-NAT transfer as the
  acceptance benchmark.

### Phase 3 — GUI pairing + polish

* GUI Connection tab: "Pair with code" sub-flow. `pairing_mode: {
  Discovery, Direct, Rendezvous }`.
* **Fix the GUI mutex deadlock:** today the establish call runs inside
  the `Arc<tokio::Mutex<P2PSession>>` lock; a 30-second pairing wait
  would freeze the message loop. Build the session outside the lock,
  then assign it.
* `nat-test --rendezvous <URL>` performs a real self-loop punch test
  (not just STUN).
* Refresh `README.md`, `DESIGN.md`, `CHANGELOG.md` with rendezvous +
  relay usage and the docker-compose stanza for self-hosting
  `rendezvousd`.

## Nice-to-have / parking lot

* Connection pooling for many-files transfers (the current design already
  multiplexes chunks over a single connection, so the gain is small).
* Mobile clients (iOS / Android) — out of scope until the core protocol
  is stable.
* Web client via WebTransport (QUIC in the browser).
* `p2p-transfer` packaged into Homebrew / Scoop / .deb.

## Testing & QA

* Linux netns end-to-end traversal test (Phase 1): two namespaces each
  behind `iptables -t nat -A POSTROUTING -j MASQUERADE`, rendezvous in
  a third.
* Real-world two-laptop pairing through a free-tier VPS rendezvous
  (Phase 1 acceptance).
* 1 GB symmetric-NAT transfer through `--relay-bind` (Phase 2
  acceptance).
* GUI smoke: enter code, UI stays responsive during a 30 s wait.

## Cleanup audit (per phase exit)

```
rg "TcpConnection|TcpServer|window\.rs|crc32|ChunkAck|--legacy"
```

should return zero hits.

`cargo machete` (or a manual `Cargo.toml` review) confirms no orphaned
dependencies.
