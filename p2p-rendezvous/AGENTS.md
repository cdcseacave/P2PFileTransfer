# p2p-rendezvous — Agent Notes

`p2p-rendezvous` is the matchmaking + relay crate. It provides a tiny pairing-by-code rendezvous protocol over TCP plus an optional UDP packet relay, and ships the `rendezvousd` binary that operators self-host (no public default URL is baked into `p2p-transfer`). Workspace-wide guidance lives in the root [AGENTS.md](../AGENTS.md); this file covers what's specific to this crate.

## What this crate does — and what it doesn't

The rendezvous **only matches peers**. It receives a `RegisterRequest` containing the peer's public endpoint, cert fingerprint, device id, and `want_relay` bit; pairs it by `code` with another peer who arrives with the same code; and writes back the inverse (`Message::Match` with the other peer's endpoint+fingerprint+device_id, or `Message::RelayMatch` with a relay session token if either side asked for relay mode). The connection is closed as soon as the match is delivered. The rendezvous **never sees user data**, never proxies QUIC, never touches the cert beyond gossiping the fingerprint.

The relay (separate Phase 2 component) forwards UDP datagrams between two paired peers verbatim. Each peer announces itself with a `RelayHello` (magic + token + cert fingerprint); the relay records the source address against the token's pre-bound slot and then forwards every subsequent UDP datagram to the other slot's address. The relay **never looks at the QUIC bytes** — TLS terminates end-to-end between the two real peers, so the relay sees ciphertext only.

## Module map

```
src/
├── lib.rs            crate-level re-exports + private MessagePack framing (4 KiB frame cap)
├── protocol.rs       wire enum (Register / Match / RelayMatch / Expired / Rejected) + types
├── server.rs         TCP listener, pairing state, concurrency cap, public-IP rewrite
├── relay.rs          UDP packet forwarder + session bookkeeping
├── client.rs         `register` / `register_full` — used by p2p-core via traversal::mod
└── bin/rendezvousd.rs   the `rendezvousd` binary entry point
```

`lib.rs` re-exports the things outside callers actually need: `Server`, `Relay`, `RelayHello`, `register`, `MatchOutcome`, `RegisterRequest`, the `Message`/`*Error` types, and constants `DEFAULT_PORT`, `FINGERPRINT_LEN`, `SESSION_TOKEN_LEN`.

## Wire protocol

Transport is TCP. Each frame is a 4-byte big-endian length prefix followed by a MessagePack-encoded `protocol::Message` payload. Frames are capped at 4 KiB (`MAX_FRAME_BYTES`) — nothing legitimate is large, and a small cap is the easiest defense against frame-bomb abuse.

`protocol::Message`:

| Variant | Direction | Meaning |
|---|---|---|
| `Register(RegisterRequest)` | client → server | "I'm here for this code; here's my endpoint+fingerprint+device_id." |
| `Match { peer_endpoint, peer_fingerprint, peer_device_id }` | server → client | Direct hole-punch pairing. |
| `RelayMatch { relay_endpoint, relay_session_token, peer_fingerprint, peer_device_id }` | server → client | Relay-mediated pairing — go through `relay_endpoint`. |
| `Expired` | server → client | Code TTL elapsed before a partner arrived. |
| `Rejected { reason }` | server → client | Malformed request (bad version, bad code, etc.). |

`RegisterRequest.want_relay` is set by the client when STUN spots a symmetric NAT or `--force-relay` is passed. If either peer of a pair sets it and the server has a relay attached, the server hands out `RelayMatch`; otherwise it hands out `Match` (and warns).

`PROTOCOL_VERSION = 1`. Equality check; bump together on server + client when the wire format changes.

## Server (`server.rs`)

### Concurrency cap

`Server::bind_with(addr, ttl, max_concurrent)` configures a `tokio::sync::Semaphore` (default `1024` via `DEFAULT_MAX_CONCURRENT`). The accept loop acquires a permit *before* `listener.accept()` so connections beyond the cap sit in the kernel's backlog rather than piling up as detached spawned tasks. The permit is held by the spawned handler and released on drop. Don't move the `accept().await` outside the `acquire_owned().await` — that defeats the backpressure (you'd accept then queue).

### Public-IP rewrite (anti-reflection)

A `RegisterRequest` carries `public_endpoint: SocketAddr` — the address the peer claims to be reachable at. Without sanitization, a peer could put a third-party's IP in there and the rendezvous would tell its partner to start sending QUIC `Initial` packets to that victim. `handle_connection` therefore rewrites `req.public_endpoint = SocketAddr::new(peer.ip(), req.public_endpoint.port())` where `peer` is the TCP accept's source. The port is kept (because the punch socket is UDP — a different transport from the TCP control channel, so the port number can't be inferred from the TCP peer), but the IP comes from the kernel-observed TCP source and is no longer forgeable. Don't pass the client-supplied IP downstream; always use the post-rewrite value.

### Code validity

`is_valid_code` enforces 4–32 ASCII alphanumeric characters. Codes are matched case-sensitively. The same code can be reused after a successful pair (the slot is removed on match) but two peers racing both as "first" lose the duplicate-registration case with `Message::Rejected`.

### TTL + expiry

`DEFAULT_CODE_TTL = 300s`. Expired waiters are dropped lazily on each lock acquisition; the waiting task also gets `Message::Expired` when its `oneshot::Receiver` times out.

## Relay (`relay.rs`)

### Session lifecycle

1. The rendezvous server calls `relay.reserve_session(token, peer_a_fp, peer_b_fp).await?` when it decides a pair needs the relay. This fails with `RelayError::DuplicateFingerprint` if both peers claim the same cert fingerprint — that would make the slot binding ambiguous.
2. Each peer sends one or more `RelayHello` packets (`P2RZ` magic + version + token + cert fingerprint) to the relay's UDP address. The relay parses the hello, looks up the session by token, and **binds the slot by fingerprint** (Slot A if the hello fingerprint matches `peer_a_expected_fp`, Slot B if it matches `peer_b_expected_fp`, drop otherwise). Once both slots have addresses recorded, the relay forwards.
3. Subsequent UDP packets from a paired source address are forwarded verbatim to the other slot's address (the relay never inspects the QUIC bytes). `bytes_forwarded` is incremented for diagnostics.
4. Idle sessions are evicted by a background `idle_sweep_loop` that runs every `IDLE_SWEEP_INTERVAL` (30 s) — this used to be inline on the per-packet hot path; now the per-packet code only holds the mutex long enough to look up and update one entry.

### Slot pre-binding rule

`reserve_session` rejects `peer_a_fp == peer_b_fp` outright. With distinct fingerprints, the hello → slot lookup is a single equality check per slot; there's no possibility of one peer occupying both slots or stealing the other slot's seat. If you ever need to support shared-fingerprint pairing (someone running both peers on the same machine), introduce an explicit slot id in the hello rather than relaxing this check.

### Buffer sizing

`RECV_BUF_BYTES = 65 KiB` — large enough for the UDP payload ceiling so jumbo frames don't truncate. The forwarder warns when it reads exactly `RECV_BUF_BYTES` so we'd notice if the buffer ever wasn't enough (kernel sets the datagram's full size in the read; truncation would silently drop the tail).

### Rate cap

`bandwidth_cap_bps` is a single token bucket across **all** sessions (the cap is on the relay as a whole, not per session). Burst is 0.5 s of cap. Pass `0` to disable.

## Client (`client.rs`)

`register(server, req) -> Result<PeerInfo, ClientError>` is the direct-only convenience that returns `ClientError::UnexpectedFromServer` if the server hands back a `RelayMatch`. `register_full(server, req) -> Result<MatchOutcome, ClientError>` returns either `MatchOutcome::Direct(PeerInfo)` or `MatchOutcome::Relay(RelayInfo)` — this is what `p2p-core::traversal::mod` uses.

Wait timeout is `REGISTER_WAIT_TIMEOUT = 310s` — a touch beyond the default server TTL so a clean `Expired` is preferred over a client-side hang.

## `rendezvousd` binary (`bin/rendezvousd.rs`)

Flags:

```
rendezvousd --bind 0.0.0.0:14570                           # rendezvous only
            --code-ttl-secs 300                            # default
            --relay-bind 0.0.0.0:14571                     # opt-in Phase 2 relay
            --max-relay-mbps 50                            # 0 = unlimited
            --verbosity info                               # off|error|warn|info|debug|trace
```

When `--relay-bind` is omitted, peers that ask for relay get a direct `Match` and a warn log line — that's the operator's signal to either run the relay or accept that symmetric-NAT pairs will fail.

The binary uses its own `tracing_subscriber` (separate from `p2p-cli`'s init) because `rendezvousd` ships as a standalone binary on the rendezvous host.

## Conventions specific to this crate

- **No `p2p-core` dependency.** Keep this crate self-contained — `p2p-core::traversal` depends on it, not the other way around. `client.rs` returns its own `ClientError`; the orchestrator translates to `p2p_core::Error::Rendezvous(string)`.
- **No public-default URL.** Don't add a constant or env var that points the binary at a default rendezvous host. Operators self-host; that's the whole point.
- **Slot-binding invariants live in `reserve_session`.** If a future feature needs to relax the fingerprint check, change it there explicitly — don't loosen the `forward_loop` lookup.
- **`PROTOCOL_VERSION` is equality-checked.** Bump it together on server + client and fail the build if anything still references the old constant.

## Deploying to a VPS

`scripts/deploy.py` is the supported way to run `rendezvousd` on a real
server (Ubuntu 24+). It's a single-file Python 3 stdlib script with three
subcommands: `install`, `uninstall`, and `clean-build`. Every step is
idempotent — `dpkg -s` checks each apt package, the cargo binary's SHA256
is compared against the installed copy before any restart, the systemd
unit is compared byte-for-byte before re-writing, etc. Safe to re-run.

Key invariants worth knowing if you touch the script:

- **Build identity** is `$SUDO_USER` when invoked via sudo, else root.
  Cargo state lives in that user's `~/.cargo`. Don't switch to a global
  cargo install — keeping per-user state means a `clean-build` only wipes
  `<dest>/target/` and rust itself survives.
- **Restart only on change.** `install_binary` and `install_service_unit`
  each return a "changed?" bool; `systemd_enable_and_start` restarts the
  daemon only when one of them flips. A no-op `install` re-run does not
  drop in-flight pairings.
- **`clean-build` is recoverable.** Removing `<dest>/target/` doesn't
  break the running service (the binary is at `/usr/local/bin/rendezvousd`,
  not under the repo). A later `install` rebuilds the target dir from
  scratch and the SHA256 compare keeps the no-op restart suppression
  working.
- **Service-unit constants** live next to `SERVICE_UNIT` at the top of the
  file. When the binary's CLI surface changes (new flag, renamed flag, new
  default), update the `ExecStart=` line — that's the single source of
  truth the script writes to `/etc/systemd/system/rendezvousd.service`.
- **UFW handling is opt-in.** If `ufw` isn't installed or isn't `active`,
  the firewall step is skipped (logged as a warning) — the script never
  enables a firewall the operator didn't choose to run.

## Tests

```bash
cargo test -p p2p-rendezvous
cargo test -p p2p-rendezvous server::tests::      # one module
cargo test -p p2p-rendezvous -- --nocapture
```

Unit tests are inline (`#[cfg(test)] mod tests`) in each module: hello roundtrip + decode rejection in `relay.rs`; code matching, code rejection, IP rewriting, and concurrency cap in `server.rs`. End-to-end coverage lives in the workspace `tests/`:

- `tests/traversal_loopback_test.rs` — full rendezvous + race-connect-and-accept punch on localhost.
- `tests/relay_loopback_test.rs` — full rendezvous + relay + QUIC-over-relay end-to-end.
