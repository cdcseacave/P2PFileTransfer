# TODO — P2P File Transfer

## Current state

* **Phase 0 — Clean QUIC rewrite** — **done** (2026-05). Single QUIC
  transport with TLS 1.3 + cert pinning; per-chunk uni streams; TCP +
  sliding window + per-chunk CRC + per-chunk ACK + encryption-capability
  bit + `--window-size` / `--max-retries` CLI flags all removed.
  `cargo test --all` and `cargo clippy --all-targets --all-features --
  -D warnings` green.

## Active work

### Phase 1 — Rendezvous server + UDP hole punching

* New workspace member crate `p2p-rendezvous`: MessagePack-over-TCP
  protocol, `rendezvousd` binary, and a `RendezvousClient` used by
  `p2p-core/src/traversal/`.
* CLI flags `--rendezvous <addr>` + `--code <code>` + `--peer-id <hex>`
  on `send` / `receive`.
* `traversal::establish_via_rendezvous(...)` orchestrates: bind UDP →
  STUN on that socket → register code at rendezvous → wait for peer →
  race `quinn::Endpoint::connect` vs `accept` as the hole punch.
* Symmetric-NAT detection: two STUN servers, compare mapped ports;
  surface `Error::HolePunchFailed` cleanly when relay is needed.
* IPv6 in the same phase if timeline allows (one `quinn::Endpoint` per
  family, race both targets).

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
