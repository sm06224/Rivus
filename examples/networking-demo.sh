#!/usr/bin/env bash
# Self-contained networking-execution demo (design §33). Spins up a loopback
# HTTP server and a loopback TCP feed (std-only, via python3), then runs Rivus's
# `net` transports against them — zero external network, nothing to configure.
#
#   examples/networking-demo.sh
#
# Requires: cargo, python3. Everything stays on 127.0.0.1 (the capability
# boundary allows loopback without RIVUS_CAP_NET_HOSTS).
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v python3 >/dev/null; then
  echo "this demo uses python3 to stand up loopback servers; please install it" >&2
  exit 1
fi

echo "── building rivus --features net ─────────────────────────────────────"
cargo build -q -p rivus-cli --features net
RIVUS=target/debug/rivus

# ── 1) HTTP GET source ────────────────────────────────────────────────────
echo
echo "── 1) open \"http://…\"  (bounded GET, CSV over HTTP) ─────────────────"
PORTFILE=$(mktemp)
python3 - "$PORTFILE" <<'PY' &
import http.server, socketserver, sys
CSV=b"name,age,country\nalice,30,JP\nbob,17,US\ncarol,42,JP\ndave,55,FR\n"
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length",str(len(CSV))); self.end_headers()
        self.wfile.write(CSV)
    def log_message(self,*a): pass
with socketserver.TCPServer(("127.0.0.1",0),H) as s:
    open(sys.argv[1],"w").write(str(s.server_address[1]))
    s.handle_request()
PY
sleep 0.4
HPORT=$(cat "$PORTFILE"); rm -f "$PORTFILE"
echo "   serving CSV on http://127.0.0.1:$HPORT/data.csv"
$RIVUS run -c "Adults:
  open \"http://127.0.0.1:$HPORT/data.csv\"
  |? age >= 18, country == \"JP\"
  |> name age country
  print
;"

# ── 2) TCP subscribe source ───────────────────────────────────────────────
echo
echo "── 2) subscribe \"tcp://…\"  (unbounded TCP feed) ─────────────────────"
FPORTFILE=$(mktemp)
python3 - "$FPORTFILE" <<'PY' &
import socket, sys
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(("127.0.0.1",0)); s.listen(1)
open(sys.argv[1],"w").write(str(s.getsockname()[1]))
c,_=s.accept()
c.sendall(b"name,age\nalice,30\nbob,17\ncarol,42\ndave,55\nerin,12\n")
c.close(); s.close()
PY
sleep 0.4
FPORT=$(cat "$FPORTFILE"); rm -f "$FPORTFILE"
echo "   feeding records on tcp://127.0.0.1:$FPORT"
$RIVUS run -c "Live:
  subscribe \"tcp://127.0.0.1:$FPORT\"
  |? age >= 18
  |> name age
  take 100
  print
;"

# ── 3) Protected-channel distributed execution (the headline) ─────────────
echo
echo "── 3) distributed: ship the IR to a remote worker, stream the result ─"
printf 'name,age,country\nalice,30,JP\nbob,17,US\ncarol,42,JP\ndave,55,FR\n' > /tmp/rivus_dist_demo.csv
# Start a protected-channel worker (loopback = the §28.12.5-1 exception; a real
# deployment binds the WireGuard interface via RIVUS_CAP_NET_IFACE).
$RIVUS serve --bind 127.0.0.1:9077 2>/tmp/rivus_serve.log &
SERVE_PID=$!
sleep 0.6
echo "   worker: $(cat /tmp/rivus_serve.log)"
cat > /tmp/rivus_dist_demo.riv <<'RIV'
Adults:
  open /tmp/rivus_dist_demo.csv
  |? age >= 18, country == "JP"
  |> name age
;
RIV
echo "   coordinator: rivus run … --on rivus://127.0.0.1:9077  (IR is the artifact)"
$RIVUS run /tmp/rivus_dist_demo.riv --on rivus://127.0.0.1:9077
kill $SERVE_PID 2>/dev/null
rm -f /tmp/rivus_dist_demo.csv /tmp/rivus_dist_demo.riv /tmp/rivus_serve.log

echo
echo "── done.  primary transport = kernel WireGuard (crypto delegated), loopback for the demo;"
echo "         remote hosts/peers need RIVUS_CAP_NET_HOSTS / RIVUS_CAP_NET_PEERS / RIVUS_CAP_NET_IFACE. ─"
