#!/bin/zsh
set -euo pipefail

URLS=(
  "https://idea-wheel.onrender.com/api/health"
  "https://ideareels.io/api/health"
)

for url in "${URLS[@]}"; do
  response=$(curl -fsSL --retry 2 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 30 "$url")
  echo "$response" | python3 -c 'import json, sys; data = json.load(sys.stdin); assert data.get("ok") is True; assert data.get("service") == "ideareels"'
  printf '%s ok %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$url"
done
