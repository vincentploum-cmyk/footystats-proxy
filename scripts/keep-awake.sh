#!/usr/bin/env bash
set -euo pipefail

# Keep the Render free-tier instance warm (it spins down after ~15 min idle).
# Pings /cache-status and verifies the app is actually serving — not just that
# the host answered — by asserting the JSON carries the leagueRegistry field.
# Run on a schedule (GitHub Actions cron / external uptime monitor).

URLS=(
  "https://footystats-proxy.onrender.com/cache-status"
)

for url in "${URLS[@]}"; do
  response=$(curl -fsSL --retry 2 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 30 "$url")
  echo "$response" | python3 -c 'import json, sys; data = json.load(sys.stdin); assert "leagueRegistry" in data, "missing leagueRegistry"'
  printf '%s ok %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$url"
done
