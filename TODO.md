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
* **Phase 2 — QUIC relay fallback** — **done** (2026-05).
  `rendezvousd --relay-bind <addr> --max-relay-mbps <n>` runs a tiny
  UDP packet forwarder alongside the rendezvous. Rendezvous matches
  where either side sets `want_relay` (symmetric NAT or
  `--force-relay`) get a `RelayMatch` with a fresh session token.
  Each peer sends a `RelayHello` so the relay records its source
  address, then runs a normal QUIC handshake with the relay's address
  as the apparent peer — packets are forwarded verbatim, so QUIC TLS
  terminates end-to-end between the two real peers (the relay sees
  ciphertext only). `tests/relay_loopback_test.rs` proves the full
  rendezvous-→-relay-→-QUIC-handshake path on localhost.
* **Security & robustness hardening** — **done** (2026-05-23). 16
  code-review findings (4 Critical, 6 High, 6 Medium) landed in one
  pass: drain QUIC streams on finish (last-chunk loss), chunk indices
  widened to `u64`, wire-supplied `chunk_index` bounds-checked,
  receiver SHA-256 mismatch is fatal, path-traversal sanitizer on
  both sides, mutual TLS with fingerprint cross-check on the
  responder, deterministic-staggered punch with address-validated
  accept, STUN tx-id validation, rendezvous concurrency cap +
  TCP-sourced public IP, relay slot pre-binding + larger recv buffer
  + off-hot-path idle eviction, typed disconnect framing. Per the
  no-compat rule, no shims — wire formats and call sites changed in
  place.

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

* **Phase 3 — GUI pairing + polish** — **done** (2026-05).
  Connection tab has a third mode "Pair with code (cross-NAT)" that
  takes a rendezvous server + shared code (with a Generate button);
  Connect mode now exposes the `--peer-fingerprint` field needed for
  direct mode. The session is built off the iced thread (no mutex
  deadlock — connect/from_rendezvous run inside `Command::perform`
  and only the resulting `P2PSession` is wrapped in `Arc<Mutex<...>>`
  via `ConnectionEstablishedWithSession`). `nat-test --rendezvous URL`
  runs a real self-loop punch and reports `direct` / `relay` / `failed`
  with latency. Docs (README/DESIGN/TODO/CHANGELOG) describe rendezvous
  + relay end-to-end.

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
