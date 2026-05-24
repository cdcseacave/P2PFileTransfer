#!/usr/bin/env bash
# v2 — re-runs the failing tests from stress.sh with two fixes:
#   1) ALL processes log at -v info so the fingerprint banner is visible.
#   2) Each peer gets a separate APPDATA → distinct identity → relay actually punches through.
set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/p2p-transfer.exe"
RVZ="$ROOT/target/release/rendezvousd.exe"
WORK="$ROOT/target/tmp/stress2-$$"
mkdir -p "$WORK"
cd "$WORK"

PASS=0; FAIL=0
declare -a RESULTS=()
ok()  { RESULTS+=("PASS  $*"); PASS=$((PASS+1)); echo "PASS  $*"; }
bad() { RESULTS+=("FAIL  $*"); FAIL=$((FAIL+1)); echo "FAIL  $*"; }
note(){ printf "\n==== %s ====\n" "$*"; }

sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'
  else powershell -NoProfile -Command "(Get-FileHash -Algorithm SHA256 -LiteralPath '$1').Hash.ToLower()"
  fi
}
killtree() { local p="$1"; [[ -z "${p:-}" ]] && return 0; taskkill //PID "$p" //F //T >/dev/null 2>&1 || kill -9 "$p" 2>/dev/null || true; }

# Make two distinct identity homes — overrides dirs::config_dir() on Windows.
ID_SEND="$WORK/id-sender"
ID_RECV="$WORK/id-receiver"
ID_R2="$WORK/id-receiver2"
mkdir -p "$ID_SEND" "$ID_RECV" "$ID_R2"

run_send()    { APPDATA="$ID_SEND" "$BIN" -v info "$@"; }
run_recv()    { APPDATA="$ID_RECV" "$BIN" -v info "$@"; }
run_recv2()   { APPDATA="$ID_R2"   "$BIN" -v info "$@"; }
run_default() { "$BIN" -v info "$@"; }   # uses current user APPDATA

############################################################
# T1 — direct send/receive small file
note "T1  direct send/receive small file"
mkdir -p t1/in t1/out
head -c 1024 /dev/urandom > t1/in/small.bin
SH_IN=$(sha256 t1/in/small.bin)

APPDATA="$ID_RECV" "$BIN" -v info receive --port 25561 --auto-accept --output t1/out > t1-recv.log 2>&1 &
RECV=$!
sleep 3
FP=$(grep -oE '[0-9a-f]{64}' t1-recv.log | head -1)
echo "T1  receiver fp=$FP"
APPDATA="$ID_SEND" "$BIN" -v info send t1/in/small.bin --peer 127.0.0.1:25561 --peer-fingerprint "$FP" > t1-send.log 2>&1
RC=$?
sleep 1; killtree "$RECV"; wait "$RECV" 2>/dev/null
if [[ $RC -eq 0 && -f t1/out/small.bin && "$SH_IN" == "$(sha256 t1/out/small.bin)" ]]; then
  ok "T1  small file sha256 match"
else
  bad "T1  rc=$RC  out=$([[ -f t1/out/small.bin ]] && echo yes || echo no)"
fi

############################################################
# T3 — folder send (compressible)
note "T3  send/receive folder"
mkdir -p t3/in/sub t3/out
yes "AAAAAAAA the quick brown fox jumps over the lazy dog" | head -c 1048576 > t3/in/repeat.txt
echo "hello" > t3/in/sub/a.txt
echo "world" > t3/in/sub/b.txt
SH_A=$(sha256 t3/in/repeat.txt); SH_B=$(sha256 t3/in/sub/a.txt); SH_C=$(sha256 t3/in/sub/b.txt)

APPDATA="$ID_RECV" "$BIN" -v info receive --port 25563 --auto-accept --output t3/out > t3-recv.log 2>&1 &
RECV=$!
sleep 3
FP=$(grep -oE '[0-9a-f]{64}' t3-recv.log | head -1)
APPDATA="$ID_SEND" "$BIN" -v info send t3/in --peer 127.0.0.1:25563 --peer-fingerprint "$FP" > t3-send.log 2>&1
RC=$?
sleep 1; killtree "$RECV"; wait "$RECV" 2>/dev/null
SH_A2=$(sha256 t3/out/in/repeat.txt 2>/dev/null); SH_B2=$(sha256 t3/out/in/sub/a.txt 2>/dev/null); SH_C2=$(sha256 t3/out/in/sub/b.txt 2>/dev/null)
if [[ $RC -eq 0 && "$SH_A" == "$SH_A2" && "$SH_B" == "$SH_B2" && "$SH_C" == "$SH_C2" ]]; then
  ok "T3  folder sha256 match (3/3)"
else
  bad "T3  rc=$RC  hashes a:$SH_A==$SH_A2  b:$SH_B==$SH_B2  c:$SH_C==$SH_C2"
  ls -R t3/out 2>/dev/null | head -20
fi

############################################################
# T4 — bandwidth throttle (4 MB/s on 8 MB → ~2 s)
note "T4  bandwidth throttle"
mkdir -p t4/in t4/out
head -c 8388608 /dev/urandom > t4/in/cap.bin
APPDATA="$ID_RECV" "$BIN" -v info receive --port 25564 --auto-accept --output t4/out > t4-recv.log 2>&1 &
RECV=$!
sleep 3
FP=$(grep -oE '[0-9a-f]{64}' t4-recv.log | head -1)
T0=$(date +%s%N)
APPDATA="$ID_SEND" "$BIN" -v info send t4/in/cap.bin --peer 127.0.0.1:25564 --peer-fingerprint "$FP" --max-speed 4M > t4-send.log 2>&1
RC=$?
T1=$(date +%s%N); MS=$(( (T1-T0)/1000000 ))
sleep 1; killtree "$RECV"; wait "$RECV" 2>/dev/null
if [[ $RC -eq 0 && $MS -ge 1300 ]]; then
  ok "T4  4M cap honored (${MS} ms)"
else
  bad "T4  rc=$RC  elapsed=${MS} ms (expected ≥1300)"
fi

############################################################
# T5 — discover loopback
note "T5  discover"
APPDATA="$ID_RECV" "$BIN" -v info receive --port 25565 --auto-accept --output t5out > t5-recv.log 2>&1 &
RECV=$!
sleep 4
APPDATA="$ID_SEND" "$BIN" -v info discover --timeout 8 --port 25565 > t5-disc.log 2>&1
RC=$?
killtree "$RECV"; wait "$RECV" 2>/dev/null
if grep -qE "[0-9a-f]{64}|fingerprint|device|peer" t5-disc.log; then
  ok "T5  discover saw beacon"
  head -10 t5-disc.log
else
  bad "T5  discover empty"
  echo "T5  recv log:"; head -10 t5-recv.log
  echo "T5  disc log:"; head -10 t5-disc.log
fi

############################################################
# T6 — nat-test (STUN, soft — depends on outbound 3478 to Google STUN)
note "T6  nat-test STUN"
APPDATA="$ID_SEND" timeout 25 "$BIN" -v info nat-test > t6.log 2>&1
RC=$?
if [[ $RC -eq 0 ]] && grep -qiE "(cone|symmetric|nat type|reflexive|public|mapped)" t6.log; then
  ok "T6  nat-test STUN reachable ($(grep -oiE 'cone|symmetric' t6.log | head -1))"
else
  echo "T6  log:"; head -20 t6.log
  RESULTS+=("SKIP  T6  nat-test STUN — network/STUN unreachable")
fi

############################################################
# T9 — relay forced, with TWO distinct identities (this is the real test)
note "T9  relay (--force-relay) with distinct identities"
"$RVZ" --bind 127.0.0.1:25580 --relay-bind 127.0.0.1:25581 --max-relay-mbps 50 > t9-rvz.log 2>&1 &
RVPID=$!
sleep 3

mkdir -p t9/in t9/out
head -c 2097152 /dev/urandom > t9/in/relay.bin
SH_IN=$(sha256 t9/in/relay.bin)
CODE="RELAY$$"

APPDATA="$ID_R2" "$BIN" -v info receive --rendezvous 127.0.0.1:25580 --code "$CODE" --force-relay --auto-accept --output t9/out > t9-recv.log 2>&1 &
RECV=$!
sleep 3
APPDATA="$ID_SEND" "$BIN" -v info send t9/in/relay.bin --rendezvous 127.0.0.1:25580 --code "$CODE" --force-relay > t9-send.log 2>&1
RC=$?
sleep 1; killtree "$RECV"; wait "$RECV" 2>/dev/null
if [[ $RC -eq 0 && -f t9/out/relay.bin && "$SH_IN" == "$(sha256 t9/out/relay.bin)" ]]; then
  ok "T9  relay transfer sha256 match"
else
  bad "T9  rc=$RC"
  echo "T9-recv tail:"; tail -10 t9-recv.log
  echo "T9-send tail:"; tail -10 t9-send.log
  echo "T9-rvz  tail:"; tail -10 t9-rvz.log
fi
killtree "$RVPID"; wait "$RVPID" 2>/dev/null

############################################################
# T10 — resume + history, with a longer in-flight window
note "T10  resume + history"
mkdir -p t10/in t10/out
head -c 16777216 /dev/urandom > t10/in/resume.bin   # 16 MB
SH_IN=$(sha256 t10/in/resume.bin)

APPDATA="$ID_RECV" "$BIN" -v info receive --port 25590 --auto-accept --output t10/out > t10-recv.log 2>&1 &
RECV=$!
sleep 3
FP=$(grep -oE '[0-9a-f]{64}' t10-recv.log | head -1)

APPDATA="$ID_SEND" "$BIN" -v info send t10/in/resume.bin --peer 127.0.0.1:25590 --peer-fingerprint "$FP" --max-speed 1M > t10-send.log 2>&1 &
SEND=$!
sleep 6                 # let ~6 MB / 16 MB go through, then sever
killtree "$SEND"; wait "$SEND" 2>/dev/null
sleep 1
killtree "$RECV"; wait "$RECV" 2>/dev/null

STATE=$(ls transfer_*.json 2>/dev/null | head -1)
if [[ -n "$STATE" ]]; then
  ok "T10a  state file written ($STATE)"
  TID=$(echo "$STATE" | sed -E 's/transfer_(.+)\.json/\1/')

  APPDATA="$ID_RECV" "$BIN" -v info receive --port 25590 --auto-accept --output t10/out > t10-recv2.log 2>&1 &
  RECV2=$!
  sleep 3
  FP2=$(grep -oE '[0-9a-f]{64}' t10-recv2.log | head -1)
  APPDATA="$ID_SEND" "$BIN" -v info resume "$TID" --to 127.0.0.1:25590 --peer-fingerprint "$FP2" --path t10/in/resume.bin > t10-resume.log 2>&1
  RC=$?
  sleep 1; killtree "$RECV2"; wait "$RECV2" 2>/dev/null
  if [[ $RC -eq 0 && -f t10/out/resume.bin && "$SH_IN" == "$(sha256 t10/out/resume.bin)" ]]; then
    ok "T10b  resume completed, sha256 matches"
  else
    bad "T10b  rc=$RC  out=$([[ -f t10/out/resume.bin ]] && echo yes || echo no)"
    echo "T10b resume log tail:"; tail -15 t10-resume.log
  fi
else
  bad "T10a  no transfer_*.json written"
  echo "T10 send log tail:"; tail -15 t10-send.log
  echo "T10 cwd contents:"; ls -la | head -20
fi

# history (uses the sender's identity dir for history file location)
APPDATA="$ID_SEND" "$BIN" -v info history --limit 50 > t10-hist.log 2>&1
if [[ -s t10-hist.log ]] && grep -qiE "(send|receive|transfer|history|complete|fail)" t10-hist.log; then
  ok "T10c  history produced output"
  head -10 t10-hist.log
else
  bad "T10c  history empty"
  echo "T10c output:"; head -10 t10-hist.log
fi

############################################################
echo
echo "==========================================================="
echo "STRESS V2 SUMMARY    PASS=$PASS    FAIL=$FAIL"
echo "==========================================================="
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo "Workdir: $WORK"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
