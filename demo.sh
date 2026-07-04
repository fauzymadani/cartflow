#!/usr/bin/env bash
# End-to-end saga check through the gateway. Run after `tilt up` (gateway on :8080).
# Asserts: in-stock small order -> confirmed; unknown item -> cancelled;
#          in-stock large order -> cancelled (payment declines, stock released).
set -euo pipefail
BASE=${1:-http://localhost:8080}

TOKEN=$(curl -sf -XPOST "$BASE/login" -d '{"user":"demo","pass":"demo"}' \
  | grep -o '"token":"[^"]*"' | cut -d'"' -f4)
[ -n "$TOKEN" ] || { echo "FAIL: no token from /login"; exit 1; }
AUTH=(-H "Authorization: Bearer $TOKEN")

place() { curl -sf "${AUTH[@]}" -XPOST "$BASE/orders" -d "$1" | grep -o '"id":"[^"]*"' | cut -d'"' -f4; }
poll() { # $1=id $2=want
  for _ in $(seq 20); do
    s=$(curl -sf "${AUTH[@]}" "$BASE/orders/$1" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
    [ "$s" = "$2" ] && { echo "  $1 -> $s"; return 0; }
    sleep 0.5
  done
  echo "FAIL: $1 stuck at '$s' (wanted $2)"; exit 1
}

# reject unauthenticated access
code=$(curl -so /dev/null -w '%{http_code}' -XPOST "$BASE/orders" -d '{"item":"widget","qty":1}')
[ "$code" = 401 ] || { echo "FAIL: unauthenticated order returned $code, want 401"; exit 1; }
echo "auth: unauthenticated -> 401 OK"

echo "confirm path:";       poll "$(place '{"item":"widget","qty":2}')" confirmed
echo "out-of-stock path:";  poll "$(place '{"item":"nope","qty":1}')"   cancelled
echo "payment-decline+compensation path:"; poll "$(place '{"item":"widget","qty":5}')" cancelled
echo "OK"
