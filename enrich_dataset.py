"""
Enrich dataset with BTTS 1H/2H combo fields from FootyStats API.

Usage:
  1. Set your API key: export FOOTY_API_KEY=your_key_here
  2. Place dataset_combined_filled.csv in the same directory
  3. Run: python3 enrich_dataset.py
  4. Output: dataset_enriched.csv
"""

import csv
import json
import time
import os
import urllib.request

API_KEY = os.environ.get('FOOTY_API_KEY')
if not API_KEY:
    print("ERROR: Set FOOTY_API_KEY environment variable first")
    print("  export FOOTY_API_KEY=your_key_here")
    exit(1)

BASE = "https://api.football-data-api.com/league-teams"

# Step 1: Read CSV and get unique season IDs
rows = []
season_ids = set()
with open('dataset_combined_filled.csv') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for r in reader:
        rows.append(r)
        season_ids.add(r['competition_id'])

print(f"Loaded {len(rows)} rows, {len(season_ids)} unique seasons")

# Step 2: Fetch team stats for each season
team_stats = {}
failed = []

for i, sid in enumerate(sorted(season_ids)):
    url = f"{BASE}?season_id={sid}&include=stats&key={API_KEY}"
    print(f"  [{i+1}/{len(season_ids)}] Fetching season {sid}...", end=" ", flush=True)
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        if data.get('error'):
            print(f"API ERROR: {data['error']}")
            failed.append(sid)
            time.sleep(1)
            continue

        teams = data.get('data', [])
        team_stats[sid] = {}
        for t in teams:
            tid = str(t.get('id', ''))
            stats = t.get('stats', {})
            team_stats[sid][tid] = stats
        print(f"{len(teams)} teams")
    except Exception as e:
        print(f"FAILED: {e}")
        failed.append(sid)

    time.sleep(1)

print(f"\nFetched {len(team_stats)} seasons, {len(failed)} failed: {failed}")

# Step 3: Enrich each row
new_fields = [
    'h_btts_yy_num', 'h_btts_yn_num', 'h_btts_ny_num', 'h_btts_nn_num', 'h_btts_yy_pct',
    'a_btts_yy_num', 'a_btts_yn_num', 'a_btts_ny_num', 'a_btts_nn_num', 'a_btts_yy_pct',
]

enriched_fieldnames = fieldnames + new_fields
matched = 0
unmatched = 0

for r in rows:
    sid = r['competition_id']
    hid = r['homeID']
    aid = r['awayID']

    h_stats = team_stats.get(sid, {}).get(hid, {})
    a_stats = team_stats.get(sid, {}).get(aid, {})

    def get_stat(stats, key, suffix='_home'):
        v = stats.get(key + suffix)
        if v is None:
            v = stats.get(key + '_overall')
        return v if v is not None else ''

    h_yy = get_stat(h_stats, 'btts_1h2h_yes_yes_num')
    h_yn = get_stat(h_stats, 'btts_1h2h_yes_no_num')
    h_ny = get_stat(h_stats, 'btts_1h2h_no_yes_num')
    h_nn = get_stat(h_stats, 'btts_1h2h_no_no_num')
    h_mp = get_stat(h_stats, 'seasonMatchesPlayed')

    a_yy = get_stat(a_stats, 'btts_1h2h_yes_yes_num', '_away')
    a_yn = get_stat(a_stats, 'btts_1h2h_yes_no_num', '_away')
    a_ny = get_stat(a_stats, 'btts_1h2h_no_yes_num', '_away')
    a_nn = get_stat(a_stats, 'btts_1h2h_no_no_num', '_away')
    a_mp = get_stat(a_stats, 'seasonMatchesPlayed', '_away')

    def pct(num, mp):
        try:
            n = float(num)
            m = float(mp)
            return round(n / m * 100, 1) if m > 0 else ''
        except:
            return ''

    r['h_btts_yy_num'] = h_yy
    r['h_btts_yn_num'] = h_yn
    r['h_btts_ny_num'] = h_ny
    r['h_btts_nn_num'] = h_nn
    r['h_btts_yy_pct'] = pct(h_yy, h_mp)

    r['a_btts_yy_num'] = a_yy
    r['a_btts_yn_num'] = a_yn
    r['a_btts_ny_num'] = a_ny
    r['a_btts_nn_num'] = a_nn
    r['a_btts_yy_pct'] = pct(a_yy, a_mp)

    if h_stats and a_stats:
        matched += 1
    else:
        unmatched += 1

print(f"\nMatched: {matched}, Unmatched: {unmatched}")

# Step 4: Write enriched CSV
with open('dataset_enriched.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=enriched_fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"Written dataset_enriched.csv with {len(enriched_fieldnames)} columns")
print("Done! Upload dataset_enriched.csv to GitHub so we can analyze it.")
