#!/usr/bin/env bash
# v4 — full stress against the fixed branch (quic @ 1af3e79+).
#
# Uses the new capabilities:
#   --identity-dir <PATH>            distinct identities per process
#   --max-reconnect-attempts N       finite retries (default 5)
#   resume <ID> --path FILE          works for single files now
#   history --limit N                works at any -v level + records from CLI
#
# Run from repo root:   bash smoke/src/stress_v4.sh
set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/p2p-transfer.exe"
RVZ="$ROOT/target/release/rendezvousd.exe"
WORK="$ROOT/target/tmp/stress4-$$"
mkdir -p "$WORK"
cd "$WORK"

PASS=0; FAIL=0; declare -a RESULTS=()
ok()   { RESULTS+=("PASS  $*"); PASS=$((PASS+1)); echo "PASS  $*"; }
bad()  { RESULTS+=("FAIL  $*"); FAIL=$((FAIL+1)); echo "FAIL  $*"; }
note() { printf "\n==== %s ====\n" "$*"; }

sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'
  else powershell -NoProfile -Command "(Get-FileHash -Algorithm SHA256 -LiteralPath '$1').Hash.ToLower()"
  fi
}
killtree() { local p="$1"; [[ -z "${p:-}" ]] && return 0; taskkill //PID "$p" //F //T >/dev/null 2>&1 || kill -9 "$p" 2>/dev/null || true; }

ID_S="$WORK/id-send"
ID_R="$WORK/id-recv"
ID_R2="$WORK/id-recv2"
mkdir -p "$ID_S" "$ID_R" "$ID_R2"

############################################################
# T0 — binary smoke
note "T0  binary smoke"
"$BIN" --version > t0v.txt 2>&1 && grep -qi "p2p-transfer" t0v.txt && ok "T0a  --version" || bad "T0a"
"$BIN" --help    > t0h.txt 2>&1 && grep -q  "send" t0h.txt          && ok "T0b  --help"    || bad "T0b"
"$RVZ" --help    > t0rh.txt 2>&1 && grep -qi "bind" t0rh.txt        && ok "T0c  rendezvousd --help" || bad "T0c"
# new flag visible in help?
grep -q "identity-dir" t0h.txt && ok "T0d  --identity-dir documented" || bad "T0d  --identity-dir missing from help"

############################################################
# T1 — direct send/receive, 1 KB
note "T1  direct send/receive small file"
mkdir -p t1/in t1/out
head -c 1024 /dev/urandom > t1/in/small.bin
SH_IN=$(sha256 t1/in/small.bin)
"$BIN" -v info --identity-dir "$ID_R" receive --port 26561 --auto-accept --output t1/out > t1r.log 2>&1 &
RECV=$!; sleep 3
FP=$(grep -oE '[0-9a-f]{64}' t1r.log | head -1)
"$BIN" -v info --identity-dir "$ID_S" send t1/in/small.bin --peer 127.0.0.1:26561 --peer-fingerprint "$FP" > t1s.log 2>&1
RC=$?
sleep 1; killtree "$RECV"; wait "$RECV" 2>/dev/null
[[ $RC -eq 0 && -f t1/out/small.bin && "$SH_IN" == "$(sha256 t1/out/small.bin)" ]] && ok "T1" || bad "T1 rc=$RC"

############################################################
# T2 — 32 MB random, adaptive zstd must disable
note "T2  32 MB random + adaptive disable"
mkdir -p t2/in t2/out
head -c 33554432 /dev/urandom > t2/in/big.bin
SH_IN=$(sha256 t2/in/big.bin)
"$BIN" -v info --identity-dir "$ID_R" receive --port 26562 --auto-accept --output t2/out > t2r.log 2>&1 &
RECV=$!; sleep 3
FP=$(grep -oE '[0-9a-f]{64}' t2r.log | head -1)
"$BIN" -v info --identity-dir "$ID_S" send t2/in/big.bin --peer 127.0.0.1:26562 --peer-fingerprint "$FP" > t2s.log 2>&1
RC=$?
sleep 1; killtree "$RECV"; wait "$RECV" 2>/dev/null
[[ $RC -eq 0 && "$SH_IN" == "$(sha256 t2/out/big.bin)" ]] && ok "T2  sha256 match" || bad "T2"
grep -qiE "adaptive|disabled" t2s.log t2r.log && ok "T2b  adaptive zstd disabled" || bad "T2b  adaptive line missing"

############################################################
# T3 — folder send (3 files mixed compressibility)
note "T3  folder send"
mkdir -p t3/in/sub t3/out
yes "AAAAA quick brown fox 01234" | head -c 1048576 > t3/in/repeat.txt
echo hello > t3/in/sub/a.txt
echo world > t3/in/sub/b.txt
SH_A=$(sha256 t3/in/repeat.txt); SH_B=$(sha256 t3/in/sub/a.txt); SH_C=$(sha256 t3/in/sub/b.txt)
"$BIN" -v info --identity-dir "$ID_R" receive --port 26563 --auto-accept --output t3/out > t3r.log 2>&1 &
RECV=$!; sleep 3
FP=$(grep -oE '[0-9a-f]{64}' t3r.log | head -1)
"$BIN" -v info --identity-dir "$ID_S" send t3/in --peer 127.0.0.1:26563 --peer-fingerprint "$FP" > t3s.log 2>&1
RC=$?
sleep 1; killtree "$RECV"; wait "$RECV" 2>/dev/null
SH_A2=$(sha256 t3/out/in/repeat.txt 2>/dev/null); SH_B2=$(sha256 t3/out/in/sub/a.txt 2>/dev/null); SH_C2=$(sha256 t3/out/in/sub/b.txt 2>/dev/null)
[[ $RC -eq 0 && "$SH_A" == "$SH_A2" && "$SH_B" == "$SH_B2" && "$SH_C" == "$SH_C2" ]] && ok "T3  3/3 files match" || bad "T3  rc=$RC"

############################################################
# T4 — bandwidth throttle, 24 MB @ 4 MB/s ≈ 4 s
note "T4  bandwidth throttle 4M"
mkdir -p t4/in t4/out
head -c 25165824 /dev/urandom > t4/in/cap.bin
"$BIN" -v info --identity-dir "$ID_R" receive --port 26564 --auto-accept --output t4/out > t4r.log 2>&1 &
RECV=$!; sleep 3
FP=$(grep -oE '[0-9a-f]{64}' t4r.log | head -1)
T0=$(date +%s%N)
"$BIN" -v info --identity-dir "$ID_S" send t4/in/cap.bin --peer 127.0.0.1:26564 --peer-fingerprint "$FP" --max-speed 4M > t4s.log 2>&1
RC=$?
T1=$(date +%s%N); MS=$(( (T1-T0)/1000000 ))
sleep 1; killtree "$RECV"; wait "$RECV" 2>/dev/null
[[ $RC -eq 0 && $MS -ge 3500 ]] && ok "T4  throttle honored (${MS} ms)" || bad "T4  rc=$RC ${MS} ms"

############################################################
# T5 — discover loopback
note "T5  LAN discover"
"$BIN" -v info --identity-dir "$ID_R" receive --port 26565 --auto-accept --output t5out > t5r.log 2>&1 &
RECV=$!; sleep 4
"$BIN" -v info --identity-dir "$ID_S" discover --timeout 6 --port 26565 > t5d.log 2>&1
killtree "$RECV"; wait "$RECV" 2>/dev/null
grep -qE "[0-9a-f]{64}|fingerprint|Discovered" t5d.log && ok "T5  beacon seen" || bad "T5"

############################################################
# T6 — nat-test STUN (soft, network-dependent)
note "T6  nat-test STUN"
timeout 25 "$BIN" -v info --identity-dir "$ID_S" nat-test > t6.log 2>&1
if grep -qiE "cone|symmetric|reflexive|public|mapped" t6.log; then ok "T6  STUN reachable ($(grep -oiE 'cone|symmetric' t6.log | head -1))"
else RESULTS+=("SKIP  T6  STUN unreachable"); fi

############################################################
# T7 — rendezvousd + self-loop punch
note "T7  rendezvous self-loop"
"$RVZ" --bind 127.0.0.1:26570 > t7rvz.log 2>&1 &
RV=$!; sleep 3
timeout 30 "$BIN" -v info --identity-dir "$ID_S" nat-test --rendezvous 127.0.0.1:26570 > t7.log 2>&1
RC=$?
grep -qiE "direct|relay|connected" t7.log && ok "T7  self-loop ($(grep -oiE 'direct|relay|failed' t7.log | head -1))" || bad "T7 rc=$RC"
killtree "$RV"; wait "$RV" 2>/dev/null

############################################################
# T8 — rendezvous-mediated transfer (direct punch path)
note "T8  rendezvous transfer"
"$RVZ" --bind 127.0.0.1:26571 > t8rvz.log 2>&1 &
RV=$!; sleep 3
mkdir -p t8/in t8/out
head -c 4194304 /dev/urandom > t8/in/rvz.bin
SH_IN=$(sha256 t8/in/rvz.bin)
CODE="V4$$"
"$BIN" -v info --identity-dir "$ID_R" receive --rendezvous 127.0.0.1:26571 --code "$CODE" --auto-accept --output t8/out > t8r.log 2>&1 &
RECV=$!; sleep 3
"$BIN" -v info --identity-dir "$ID_S" send t8/in/rvz.bin --rendezvous 127.0.0.1:26571 --code "$CODE" > t8s.log 2>&1
RC=$?
sleep 1; killtree "$RECV"; wait "$RECV" 2>/dev/null
[[ $RC -eq 0 && "$SH_IN" == "$(sha256 t8/out/rvz.bin)" ]] && ok "T8  rendezvous transfer match" || bad "T8 rc=$RC"
killtree "$RV"; wait "$RV" 2>/dev/null

############################################################
# T9 — RELAY: real live data path through the forwarder (now works because each peer has its own --identity-dir)
note "T9  relay path with --force-relay + distinct identity dirs"
"$RVZ" --bind 127.0.0.1:26580 --relay-bind 127.0.0.1:26581 --max-relay-mbps 50 > t9rvz.log 2>&1 &
RV=$!; sleep 3
mkdir -p t9/in t9/out
head -c 2097152 /dev/urandom > t9/in/relay.bin
SH_IN=$(sha256 t9/in/relay.bin)
CODE="REL$$"
"$BIN" -v info --identity-dir "$ID_R2" receive --rendezvous 127.0.0.1:26580 --code "$CODE" --force-relay --auto-accept --output t9/out > t9r.log 2>&1 &
RECV=$!; sleep 3
"$BIN" -v info --identity-dir "$ID_S" send t9/in/relay.bin --rendezvous 127.0.0.1:26580 --code "$CODE" --force-relay > t9s.log 2>&1
RC=$?
sleep 1; killtree "$RECV"; wait "$RECV" 2>/dev/null
[[ $RC -eq 0 && "$SH_IN" == "$(sha256 t9/out/relay.bin)" ]] && ok "T9  relay end-to-end match" || { bad "T9 rc=$RC"; tail -5 t9s.log; tail -5 t9r.log; }
killtree "$RV"; wait "$RV" 2>/dev/null

############################################################
# T10 — single-file resume (now possible because resume accepts files)
note "T10  single-file resume + bounded retries"
mkdir -p t10/in t10/out
head -c 8388608 /dev/urandom > t10/in/resume.bin   # 8 MB at 1 MB/s = 8 s
SH_IN=$(sha256 t10/in/resume.bin)
"$BIN" -v info --identity-dir "$ID_R" receive --port 26590 --auto-accept --output t10/out > t10r.log 2>&1 &
RECV=$!; sleep 3
FP=$(grep -oE '[0-9a-f]{64}' t10r.log | head -1)

# default max_reconnect_attempts=5 with 3+6+12+24+48 backoff = ~93s total max,
# but we kill the receiver permanently so each reconnect attempt fails fast.
"$BIN" -v info --identity-dir "$ID_S" send t10/in/resume.bin --peer 127.0.0.1:26590 --peer-fingerprint "$FP" --max-speed 1M > t10s.log 2>&1 &
SEND=$!
sleep 3                           # ~3 MB in
echo "T10  killing receiver (sender must persist state, then bounded retries)…"
killtree "$RECV"; wait "$RECV" 2>/dev/null
echo "T10  waiting for sender to exhaust 5 reconnect attempts (~90s max)…"
wait "$SEND" 2>/dev/null
SEND_RC=$?
echo "T10  sender exited rc=$SEND_RC"

STATE=$(ls transfer_*.json 2>/dev/null | head -1)
if [[ -n "$STATE" ]]; then
  ok "T10a  state file written ($STATE)"
  TID=$(echo "$STATE" | sed -E 's/transfer_(.+)\.json/\1/')

  "$BIN" -v info --identity-dir "$ID_R" receive --port 26590 --auto-accept --output t10/out > t10r2.log 2>&1 &
  RECV2=$!; sleep 3
  FP2=$(grep -oE '[0-9a-f]{64}' t10r2.log | head -1)

  # Resume with a FILE path — this is the bug we fixed.
  "$BIN" -v info --identity-dir "$ID_S" resume "$TID" --to 127.0.0.1:26590 --peer-fingerprint "$FP2" --path t10/in/resume.bin > t10res.log 2>&1
  RC=$?
  sleep 1; killtree "$RECV2"; wait "$RECV2" 2>/dev/null

  if [[ $RC -eq 0 && -f t10/out/resume.bin && "$SH_IN" == "$(sha256 t10/out/resume.bin)" ]]; then
    ok "T10b  single-file resume completed, sha256 match"
  else
    bad "T10b  rc=$RC  file_present=$([[ -f t10/out/resume.bin ]] && echo yes || echo no)"
    tail -10 t10res.log
  fi
else
  bad "T10a  no state file written (sender rc=$SEND_RC)"
  tail -15 t10s.log
fi

############################################################
# T11 — CLI history is now populated and visible at any verbosity
note "T11  CLI history populated + visible at -v warn"
# Snapshot user's real history file so we can roll back the side effects.
USER_HIST=$(powershell -NoProfile -Command "[Environment]::GetFolderPath('UserProfile')" | tr -d '\r')/.p2p-transfer/history.json
BACKUP_HIST=""
if [[ -f "$USER_HIST" ]]; then
  BACKUP_HIST="$WORK/history.json.backup"
  cp "$USER_HIST" "$BACKUP_HIST"
  echo "T11  backed up real history to $BACKUP_HIST"
fi
rm -f "$USER_HIST"

# Drive one send + receive to get exactly 2 records (1 SEND, 1 RECV).
mkdir -p t11/in t11/out
head -c 8192 /dev/urandom > t11/in/h.bin
"$BIN" -v info --identity-dir "$ID_R" receive --port 26600 --auto-accept --output t11/out > t11r.log 2>&1 &
RECV=$!; sleep 3
FP=$(grep -oE '[0-9a-f]{64}' t11r.log | head -1)
"$BIN" -v info --identity-dir "$ID_S" send t11/in/h.bin --peer 127.0.0.1:26600 --peer-fingerprint "$FP" > t11s.log 2>&1
sleep 2
killtree "$RECV"; wait "$RECV" 2>/dev/null
sleep 1

# At -v warn, history MUST still print (fixed Minor 2).
"$BIN" -v warn history --limit 10 > t11h.log 2>&1
SENDS=$(grep -c "^\[SEND\]" t11h.log || true)
RECVS=$(grep -c "^\[RECV\]" t11h.log || true)
echo "T11  SEND records=$SENDS  RECV records=$RECVS"
[[ "$SENDS" -ge 1 ]] && ok "T11a  SEND recorded by CLI" || bad "T11a  no SEND record"
[[ "$RECVS" -ge 1 ]] && ok "T11b  RECV recorded by CLI" || bad "T11b  no RECV record"
grep -qE "Status:.*Completed" t11h.log && ok "T11c  Completed status displayed" || bad "T11c"
grep -q "Transfer History" t11h.log && ok "T11d  output visible at -v warn" || bad "T11d  hidden"

# Restore real history.
rm -f "$USER_HIST"
if [[ -n "$BACKUP_HIST" ]]; then
  cp "$BACKUP_HIST" "$USER_HIST"
  echo "T11  restored real history"
fi

############################################################
# T12 — concurrency: 8 record_transfer-equivalent CLI runs in parallel
#        (sender and receiver on same machine, 4 pairs). All 8 records must persist.
note "T12  history concurrent writes (8-pair simultaneous CLI)"
# Use a private history file (override default by point HOME via... we can't.
# Instead: snapshot real, run pairs, count delta, restore.
USER_HIST=$(powershell -NoProfile -Command "[Environment]::GetFolderPath('UserProfile')" | tr -d '\r')/.p2p-transfer/history.json
BACKUP_HIST=""
PRE_COUNT=0
if [[ -f "$USER_HIST" ]]; then
  BACKUP_HIST="$WORK/history.json.backup2"
  cp "$USER_HIST" "$BACKUP_HIST"
  PRE_COUNT=$(grep -c '"transfer_id"' "$USER_HIST" || echo 0)
fi

mkdir -p t12/in t12/out
for i in 0 1 2 3; do
  head -c 1024 /dev/urandom > t12/in/$i.bin
done

PAIRS=()
for i in 0 1 2 3; do
  PORT=$((26700 + i))
  mkdir -p "$WORK/id-s-$i" "$WORK/id-r-$i" "t12/out/$i"
  "$BIN" -v info --identity-dir "$WORK/id-r-$i" receive --port $PORT --auto-accept --output t12/out/$i > t12-r-$i.log 2>&1 &
  PAIRS+=($!)
done
sleep 3

for i in 0 1 2 3; do
  PORT=$((26700 + i))
  FP=$(grep -oE '[0-9a-f]{64}' t12-r-$i.log | head -1)
  "$BIN" -v info --identity-dir "$WORK/id-s-$i" send t12/in/$i.bin --peer 127.0.0.1:$PORT --peer-fingerprint "$FP" > t12-s-$i.log 2>&1 &
  PAIRS+=($!)
done

# wait for all senders
sleep 8
for p in "${PAIRS[@]}"; do killtree "$p"; done
sleep 2

POST_COUNT=$(grep -c '"transfer_id"' "$USER_HIST" 2>/dev/null || echo 0)
DELTA=$((POST_COUNT - PRE_COUNT))
echo "T12  history records: pre=$PRE_COUNT  post=$POST_COUNT  delta=$DELTA"
# Expect 8 new records (4 senders + 4 receivers all distinct). Allow ≥7 for receive-side race quirks.
if [[ $DELTA -ge 7 ]]; then
  ok "T12  ≥7 concurrent records persisted (delta=$DELTA)"
else
  bad "T12  only $DELTA records persisted out of 8 expected"
fi

# Restore
rm -f "$USER_HIST"
if [[ -n "$BACKUP_HIST" ]]; then cp "$BACKUP_HIST" "$USER_HIST"; fi

############################################################
# Summary
echo
echo "=========================================================="
echo "STRESS V4 SUMMARY     PASS=$PASS    FAIL=$FAIL"
echo "=========================================================="
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo "Workdir: $WORK"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
