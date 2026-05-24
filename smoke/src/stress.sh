#!/usr/bin/env bash
# End-to-end stress / smoke test for every p2p-transfer CLI surface.
# Run from repo root:   bash smoke/src/stress.sh
set -u  # do NOT set -e: we want to keep going past test failures and report a summary

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/p2p-transfer.exe"
RVZ="$ROOT/target/release/rendezvousd.exe"
WORK="$ROOT/target/tmp/stress-$$"
mkdir -p "$WORK"
cd "$WORK"

PASS=0
FAIL=0
declare -a RESULTS=()

note()  { printf "\n==== %s ====\n" "$*"; }
ok()    { RESULTS+=("PASS  $*"); PASS=$((PASS+1)); echo "PASS  $*"; }
bad()   { RESULTS+=("FAIL  $*"); FAIL=$((FAIL+1)); echo "FAIL  $*"; }
have()  { command -v "$1" >/dev/null 2>&1; }

# sha256 wrapper that works under git-bash + powershell
sha256() {
  if have sha256sum; then sha256sum "$1" | awk '{print $1}'
  else powershell -NoProfile -Command "(Get-FileHash -Algorithm SHA256 -LiteralPath '$1').Hash.ToLower()"
  fi
}

# Try several /proc-style ways to kill a child started in background.
killtree() {
  local pid="$1"
  [[ -z "${pid:-}" ]] && return 0
  taskkill //PID "$pid" //F //T >/dev/null 2>&1 || kill -9 "$pid" 2>/dev/null || true
}

# Wait until a TCP/UDP port is bound on localhost. ($1=port $2=timeout)
wait_port() {
  local port="$1" max="$2" i=0
  while ! powershell -NoProfile -Command "Test-NetConnection -ComputerName 127.0.0.1 -Port $port -InformationLevel Quiet -WarningAction SilentlyContinue" 2>/dev/null | grep -qi true; do
    i=$((i+1))
    [[ $i -ge $max ]] && return 1
    sleep 1
  done
  return 0
}

# Sleep helper that prints a dot per second
sleep_d() { for _ in $(seq 1 "$1"); do printf .; sleep 1; done; echo; }

############################################################
# T0 — version + help (basic smoke; catches link/runtime issues)
note "T0  binary smoke"
"$BIN"  --version > t0-cli.txt 2>&1 && grep -qi "p2p-transfer" t0-cli.txt && ok "T0a  p2p-transfer --version" || bad "T0a  p2p-transfer --version"
"$BIN"  --help    > t0-help.txt 2>&1 && grep -q  "send"        t0-help.txt && ok "T0b  p2p-transfer --help"    || bad "T0b  p2p-transfer --help"
"$RVZ"  --help    > t0-rvz.txt  2>&1 && grep -qi "bind"        t0-rvz.txt  && ok "T0c  rendezvousd --help"     || bad "T0c  rendezvousd --help"

############################################################
# T1 — direct send/receive small file (1 KB)
note "T1  direct send/receive small file"
mkdir -p t1/in t1/out
head -c 1024 /dev/urandom > t1/in/small.bin
SH_IN=$(sha256 t1/in/small.bin)

"$BIN" -v warn receive --port 24561 --auto-accept --output t1/out > t1-recv.log 2>&1 &
RECV=$!
sleep 2
"$BIN" -v warn send t1/in/small.bin --peer 127.0.0.1:24561 \
       --peer-fingerprint "$(grep -oE '[0-9a-f]{64}' t1-recv.log | head -1)" \
       > t1-send.log 2>&1
RC=$?
sleep 1
killtree "$RECV"; wait "$RECV" 2>/dev/null
if [[ $RC -eq 0 && -f t1/out/small.bin ]]; then
  SH_OUT=$(sha256 t1/out/small.bin)
  [[ "$SH_IN" == "$SH_OUT" ]] && ok "T1  small.bin sha256 match" || bad "T1  sha256 mismatch  in=$SH_IN  out=$SH_OUT"
else
  bad "T1  send rc=$RC  file_present=$([[ -f t1/out/small.bin ]] && echo yes || echo no)"
fi

############################################################
# T2 — direct send/receive large random file (32 MB, incompressible → adaptive should disable zstd)
note "T2  direct send/receive 32 MB random"
mkdir -p t2/in t2/out
head -c 33554432 /dev/urandom > t2/in/big.bin
SH_IN=$(sha256 t2/in/big.bin)

"$BIN" -v info receive --port 24562 --auto-accept --output t2/out > t2-recv.log 2>&1 &
RECV=$!
sleep 2
FP=$(grep -oE '[0-9a-f]{64}' t2-recv.log | head -1)
"$BIN" -v info send t2/in/big.bin --peer 127.0.0.1:24562 --peer-fingerprint "$FP" > t2-send.log 2>&1
RC=$?
sleep 1
killtree "$RECV"; wait "$RECV" 2>/dev/null
if [[ $RC -eq 0 && -f t2/out/big.bin ]]; then
  SH_OUT=$(sha256 t2/out/big.bin)
  [[ "$SH_IN" == "$SH_OUT" ]] && ok "T2  32MB random sha256 match" || bad "T2  sha256 mismatch"
  # Compression should be skipped for random data — look for the adaptive log line
  if grep -qiE "(adaptive|incompressible|disabling compression|compression disabled)" t2-send.log t2-recv.log; then
    ok "T2b  adaptive zstd disabled for random data"
  else
    bad "T2b  adaptive log message not found (manual check t2-send.log)"
  fi
else
  bad "T2  send rc=$RC"
fi

############################################################
# T3 — direct send/receive a folder with compressible content
note "T3  send/receive folder (compressible)"
mkdir -p t3/in/sub t3/out
yes "AAAAAAAA the quick brown fox jumps over the lazy dog 0123456789" | head -c 1048576 > t3/in/repeat.txt
echo "hello"  > t3/in/sub/a.txt
echo "world"  > t3/in/sub/b.txt
SH_A=$(sha256 t3/in/repeat.txt)
SH_B=$(sha256 t3/in/sub/a.txt)
SH_C=$(sha256 t3/in/sub/b.txt)

"$BIN" -v warn receive --port 24563 --auto-accept --output t3/out > t3-recv.log 2>&1 &
RECV=$!
sleep 2
FP=$(grep -oE '[0-9a-f]{64}' t3-recv.log | head -1)
"$BIN" -v warn send t3/in --peer 127.0.0.1:24563 --peer-fingerprint "$FP" > t3-send.log 2>&1
RC=$?
sleep 1
killtree "$RECV"; wait "$RECV" 2>/dev/null
if [[ $RC -eq 0 ]]; then
  # Folder send delivers under out/<srcname>/...
  SH_A2=$(sha256 t3/out/in/repeat.txt 2>/dev/null)
  SH_B2=$(sha256 t3/out/in/sub/a.txt 2>/dev/null)
  SH_C2=$(sha256 t3/out/in/sub/b.txt 2>/dev/null)
  if [[ "$SH_A" == "$SH_A2" && "$SH_B" == "$SH_B2" && "$SH_C" == "$SH_C2" ]]; then
    ok "T3  folder sha256 match (3/3 files)"
  else
    bad "T3  folder sha256 mismatch  a:$SH_A==$SH_A2  b:$SH_B==$SH_B2  c:$SH_C==$SH_C2"
    ls -R t3/out >> t3-send.log
  fi
else
  bad "T3  send rc=$RC"
fi

############################################################
# T4 — bandwidth cap honored on a 8 MB file with --max-speed 4M (should take ~2s)
note "T4  bandwidth throttle --max-speed 4M"
mkdir -p t4/in t4/out
head -c 8388608 /dev/urandom > t4/in/cap.bin
"$BIN" -v warn receive --port 24564 --auto-accept --output t4/out > t4-recv.log 2>&1 &
RECV=$!
sleep 2
FP=$(grep -oE '[0-9a-f]{64}' t4-recv.log | head -1)
T0=$(date +%s%N)
"$BIN" -v warn send t4/in/cap.bin --peer 127.0.0.1:24564 --peer-fingerprint "$FP" --max-speed 4M > t4-send.log 2>&1
RC=$?
T1=$(date +%s%N)
MS=$(( (T1 - T0) / 1000000 ))
killtree "$RECV"; wait "$RECV" 2>/dev/null
echo "T4  elapsed=${MS} ms (~2000ms expected at 4 MB/s for 8 MB)"
if [[ $RC -eq 0 && $MS -ge 1300 ]]; then
  ok "T4  bandwidth throttle honored (${MS} ms ≥ 1300 ms)"
else
  bad "T4  throttle skipped or too fast (rc=$RC, ${MS} ms)"
fi

############################################################
# T5 — discover sees an advertising receiver
note "T5  discover"
"$BIN" -v warn receive --port 24565 --auto-accept --output t5out > t5-recv.log 2>&1 &
RECV=$!
sleep 3
"$BIN" -v warn discover --timeout 6 --port 24565 > t5-disc.log 2>&1
killtree "$RECV"; wait "$RECV" 2>/dev/null
if grep -qE "(fingerprint|[0-9a-f]{64}|peer)" t5-disc.log; then
  ok "T5  discover saw at least one beacon"
else
  bad "T5  discover output: $(head -5 t5-disc.log | tr '\n' ' | ')"
fi

############################################################
# T6 — nat-test (STUN). Network-dependent — soft-fail.
note "T6  nat-test (STUN, soft)"
timeout 20 "$BIN" -v warn nat-test > t6.log 2>&1
RC=$?
if [[ $RC -eq 0 ]] && grep -qiE "(cone|symmetric|nat type|reflexive|public)" t6.log; then
  ok "T6  nat-test STUN reachable"
else
  echo "T6 (soft) nat-test rc=$RC  $(head -3 t6.log | tr '\n' ' | ')"
  RESULTS+=("SKIP  T6  nat-test STUN — network/STUN unreachable")
fi

############################################################
# T7 — rendezvous daemon + nat-test self-loop punch
note "T7  rendezvousd + nat-test self-loop"
"$RVZ" --bind 127.0.0.1:24570 > t7-rvz.log 2>&1 &
RVPID=$!
sleep 2

timeout 30 "$BIN" -v info nat-test --rendezvous 127.0.0.1:24570 > t7.log 2>&1
RC=$?
if [[ $RC -eq 0 ]] && grep -qiE "(direct|relay|punch|hand[s ]?hake|connected)" t7.log; then
  ok "T7  rendezvous self-loop completed ($(grep -oiE 'direct|relay|failed' t7.log | head -1))"
else
  bad "T7  self-loop rc=$RC  $(head -5 t7.log | tr '\n' ' | ')"
fi

############################################################
# T8 — real send/receive through rendezvous (--code), with a small file
note "T8  send/receive via rendezvous --code"
mkdir -p t8/in t8/out
head -c 4194304 /dev/urandom > t8/in/rvz.bin
SH_IN=$(sha256 t8/in/rvz.bin)
CODE="STRESS$(date +%s)"

"$BIN" -v info receive --rendezvous 127.0.0.1:24570 --code "$CODE" --auto-accept --output t8/out > t8-recv.log 2>&1 &
RECV=$!
sleep 2
"$BIN" -v info send t8/in/rvz.bin --rendezvous 127.0.0.1:24570 --code "$CODE" > t8-send.log 2>&1
RC=$?
sleep 1
killtree "$RECV"; wait "$RECV" 2>/dev/null
if [[ $RC -eq 0 && -f t8/out/rvz.bin ]]; then
  SH_OUT=$(sha256 t8/out/rvz.bin)
  [[ "$SH_IN" == "$SH_OUT" ]] && ok "T8  rendezvous transfer sha256 match" || bad "T8  sha256 mismatch"
else
  bad "T8  send rc=$RC  out_present=$([[ -f t8/out/rvz.bin ]] && echo yes || echo no)"
fi

# Tear down basic rendezvousd before relay variant
killtree "$RVPID"; wait "$RVPID" 2>/dev/null

############################################################
# T9 — rendezvous with relay attached + --force-relay path
note "T9  rendezvousd --relay-bind + --force-relay"
"$RVZ" --bind 127.0.0.1:24580 --relay-bind 127.0.0.1:24581 --max-relay-mbps 50 > t9-rvz.log 2>&1 &
RVPID=$!
sleep 2

mkdir -p t9/in t9/out
head -c 2097152 /dev/urandom > t9/in/relay.bin
SH_IN=$(sha256 t9/in/relay.bin)
CODE="RELAY$(date +%s)"

"$BIN" -v info receive --rendezvous 127.0.0.1:24580 --code "$CODE" --force-relay --auto-accept --output t9/out > t9-recv.log 2>&1 &
RECV=$!
sleep 2
"$BIN" -v info send t9/in/relay.bin --rendezvous 127.0.0.1:24580 --code "$CODE" --force-relay > t9-send.log 2>&1
RC=$?
sleep 1
killtree "$RECV"; wait "$RECV" 2>/dev/null
if [[ $RC -eq 0 && -f t9/out/relay.bin ]]; then
  SH_OUT=$(sha256 t9/out/relay.bin)
  if [[ "$SH_IN" == "$SH_OUT" ]]; then
    ok "T9  relay transfer sha256 match"
  else
    bad "T9  sha256 mismatch"
  fi
  if grep -qiE "relay" t9-send.log t9-recv.log; then
    ok "T9b  relay path advertised in logs"
  else
    bad "T9b  no 'relay' string in logs (sanity)"
  fi
else
  bad "T9  send rc=$RC"
fi

killtree "$RVPID"; wait "$RVPID" 2>/dev/null

############################################################
# T10 — resume: start a slow transfer, interrupt, check state file, resume, verify hash, check history
note "T10  resume + history"
mkdir -p t10/in t10/out
head -c 6291456 /dev/urandom > t10/in/resume.bin  # 6 MB
SH_IN=$(sha256 t10/in/resume.bin)

# slow it down so we have time to interrupt
"$BIN" -v info receive --port 24590 --auto-accept --output t10/out > t10-recv.log 2>&1 &
RECV=$!
sleep 2
FP=$(grep -oE '[0-9a-f]{64}' t10-recv.log | head -1)

"$BIN" -v info send t10/in/resume.bin --peer 127.0.0.1:24590 --peer-fingerprint "$FP" --max-speed 1M > t10-send.log 2>&1 &
SEND=$!
sleep 2
killtree "$SEND"; wait "$SEND" 2>/dev/null
sleep 1
killtree "$RECV"; wait "$RECV" 2>/dev/null

STATE=$(ls transfer_*.json 2>/dev/null | head -1)
if [[ -n "$STATE" ]]; then
  ok "T10a  state file written ($STATE)"
  TID=$(echo "$STATE" | sed -E 's/transfer_(.+)\.json/\1/')
  echo "T10  TID=$TID"

  "$BIN" -v info receive --port 24590 --auto-accept --output t10/out > t10-recv2.log 2>&1 &
  RECV2=$!
  sleep 2
  FP2=$(grep -oE '[0-9a-f]{64}' t10-recv2.log | head -1)
  "$BIN" -v info resume "$TID" --to 127.0.0.1:24590 --peer-fingerprint "$FP2" --path t10/in/resume.bin > t10-resume.log 2>&1
  RC=$?
  sleep 1
  killtree "$RECV2"; wait "$RECV2" 2>/dev/null

  if [[ $RC -eq 0 && -f t10/out/resume.bin ]]; then
    SH_OUT=$(sha256 t10/out/resume.bin)
    [[ "$SH_IN" == "$SH_OUT" ]] && ok "T10b  resume completed, sha256 matches" || bad "T10b  sha256 mismatch after resume"
  else
    bad "T10b  resume rc=$RC  out_present=$([[ -f t10/out/resume.bin ]] && echo yes || echo no)"
  fi
else
  bad "T10a  no transfer_*.json was written (interrupt may have been too late or too early)"
fi

"$BIN" -v warn history --limit 50 > t10-hist.log 2>&1
if [[ -s t10-hist.log ]] && grep -qiE "(send|receive|transfer|history)" t10-hist.log; then
  ok "T10c  history command produced output"
else
  bad "T10c  history empty / unreadable"
fi

############################################################
# Summary
echo
echo "==========================================================="
echo "STRESS SUMMARY    PASS=$PASS    FAIL=$FAIL"
echo "==========================================================="
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo "Workdir: $WORK"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
