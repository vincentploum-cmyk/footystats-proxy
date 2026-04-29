"""
Backfill Supabase match_results table from FootyStats API for a date range.

Replicates the rank/signal logic from server.js exactly so historical rows
match the runtime model. Skips women's leagues (15020, 16037, 16046, 16563)
per CLAUDE.md. Idempotent — uses Supabase upsert on match_id.

Caveat: uses CURRENT team stats, which include matches inside the range.
Same backtest bias as the original CSV load. Acceptable for recalibration
input but not for evaluating live prediction quality.

Usage:
  export FOOTY_API_KEY=...
  export SUPABASE_URL=https://<project>.supabase.co
  export SUPABASE_ANON_KEY=...

  python3 scripts/backfill_supabase.py --start 2026-04-01 --end 2026-04-29
  python3 scripts/backfill_supabase.py --start 2026-04-01 --end 2026-04-29 --dry-run

Requirements: pip install requests
"""

import argparse
import os
import sys
import time
from datetime import date, timedelta

import requests

API_KEY = os.environ.get("FOOTY_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_ANON_KEY")

if not API_KEY:
    sys.exit("ERROR: set FOOTY_API_KEY env var")
if not SUPABASE_URL or not SUPABASE_KEY:
    sys.exit("ERROR: set SUPABASE_URL and SUPABASE_ANON_KEY env vars")

BASE = "https://api.football-data-api.com"
WOMENS_LEAGUE_IDS = {15020, 16037, 16046, 16563}

PROB25_BY_RANK = {4: 87.5, 3: 60.6, 2: 36.6, 1: 23.1, 0: 11.5}
PROB15_BY_RANK = {4: 96.9, 3: 75.0, 2: 64.2, 1: 56.5, 0: 33.0}
RANK_LABELS    = {4: "Fire", 3: "Prime", 2: "Watch", 1: "Signal", 0: "Low"}


def safe_num(v):
    try:
        f = float(v)
        if f != f or f in (float("inf"), float("-inf")):
            return 0.0
        return f
    except (TypeError, ValueError):
        return 0.0


def extract_stats(team_obj, role):
    s = team_obj.get("stats") or {}
    sfx = "_home" if role == "home" else "_away"
    mp_role = safe_num(s.get("seasonMatchesPlayed" + sfx)) or 1.0

    def pick(role_key, fallback_key):
        rv = s.get(role_key)
        if rv is not None and mp_role >= 3:
            return rv
        fv = s.get(fallback_key)
        return fv if fv is not None else 0

    return {
        "name":      team_obj.get("name") or team_obj.get("cleanName") or "",
        "scored_fh": safe_num(pick("scoredAVGHT"   + sfx, "scoredAVGHT_overall")),
        "conced_fh": safe_num(pick("concededAVGHT" + sfx, "concededAVGHT_overall")),
        "t1_pct":    safe_num(pick("seasonOver25PercentageHT" + sfx, "seasonOver25PercentageHT_overall")),
        "cn010_avg": safe_num((s.get("goals_conceded_min_0_to_10" + sfx) or 0) / mp_role),
        "sot_avg":   0.0,
        "mp":        safe_num(s.get("seasonMatchesPlayed_overall") or 0),
        "mp_role":   mp_role,
    }


def compute_signals(snap):
    h, a = snap["home"], snap["away"]
    ci    = h["scored_fh"] + a["scored_fh"] + h["conced_fh"] + a["conced_fh"]
    def_ci = h["conced_fh"] + a["conced_fh"]
    sig_a = ci    >= 3.2
    sig_b = h["t1_pct"] >= 25 and a["t1_pct"] >= 25
    sig_c = def_ci >= 2.25
    sig_d = a["scored_fh"] >= 1.25
    raw_rank = sum([sig_a, sig_b, sig_c, sig_d])
    rank = 2 if (raw_rank >= 3 and not sig_c) else raw_rank
    return {
        "rank": rank,
        "label": RANK_LABELS.get(rank, "Low"),
        "prob25": PROB25_BY_RANK.get(rank, 10.0),
        "prob15": PROB15_BY_RANK.get(rank, 31.4),
        "ci": round(ci, 2),
        "def_ci": round(def_ci, 2),
        "eligible": rank >= 3,
        "signals": {
            "A": {"met": sig_a, "label": "Combined Intensity",
                  "value": f"{ci:.2f}", "threshold": ">= 3.20"},
            "B": {"met": sig_b, "label": "FH History Both",
                  "value": f"{h['t1_pct']:.0f}%/{a['t1_pct']:.0f}%", "threshold": "both >= 25%"},
            "C": {"met": sig_c, "label": "Leaky Defences",
                  "value": f"{h['conced_fh']:.2f}+{a['conced_fh']:.2f}={def_ci:.2f}", "threshold": ">= 2.25"},
            "D": {"met": sig_d, "label": "Away FH Attack",
                  "value": f"{a['scored_fh']:.2f}", "threshold": ">= 1.25"},
        },
    }


def get_json(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "error" in data and "rate limit" in str(data["error"]).lower():
                print(f"  rate limit hit — sleeping 60s")
                time.sleep(60)
                continue
            return data
        except Exception as e:
            if attempt == retries - 1:
                print(f"  fetch failed: {e}")
                return None
            time.sleep(2 ** attempt)
    return None


def fetch_fixtures(d):
    return get_json(f"{BASE}/todays-matches?date={d}&key={API_KEY}") or {"data": []}


def fetch_team_stats(sid):
    return get_json(f"{BASE}/league-teams?season_id={sid}&include=stats&key={API_KEY}") or {"data": []}


def fetch_league_names():
    data = get_json(f"{BASE}/league-list?key={API_KEY}")
    if not data:
        return {}
    out = {}
    for league in data.get("data", []):
        country = league.get("country") or ""
        name = league.get("league_name") or league.get("name") or ""
        full = f"{country} · {name}" if country else name
        for s in (league.get("season") or []):
            sid = s.get("id")
            if sid:
                out[int(sid)] = full
    return out


def supabase_upsert(rows, dry_run=False):
    if dry_run or not rows:
        return len(rows), None
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    url = f"{SUPABASE_URL}/rest/v1/match_results?on_conflict=match_id"
    r = requests.post(url, json=rows, headers=headers, timeout=30)
    if r.status_code >= 300:
        return 0, f"HTTP {r.status_code}: {r.text[:300]}"
    return len(rows), None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end",   required=True, help="YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--batch-size", type=int, default=500)
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    if end < start:
        sys.exit("end date must be >= start date")

    print(f"Loading league name registry...")
    league_names = fetch_league_names()
    print(f"  {len(league_names)} season IDs mapped")

    dates = []
    d = start
    while d <= end:
        dates.append(d.isoformat())
        d += timedelta(days=1)

    print(f"\nFetching fixtures for {len(dates)} dates...")
    all_fixtures = []
    seen_match_ids = set()
    for ds in dates:
        data = fetch_fixtures(ds)
        fixtures = data.get("data") or []
        new = 0
        for fx in fixtures:
            mid = fx.get("id")
            if mid and mid not in seen_match_ids:
                seen_match_ids.add(mid)
                all_fixtures.append(fx)
                new += 1
        print(f"  {ds}: {len(fixtures)} fixtures ({new} new)")
        time.sleep(0.5)

    completed = [f for f in all_fixtures if f.get("status") == "complete"]
    print(f"\nTotal: {len(all_fixtures)} unique fixtures, {len(completed)} completed")

    season_ids = sorted({f.get("competition_id") for f in completed if f.get("competition_id")})
    season_ids = [s for s in season_ids if s not in WOMENS_LEAGUE_IDS]
    print(f"\nFetching team stats for {len(season_ids)} season(s) (women's leagues skipped)...")
    team_stats_by_season = {}
    for i, sid in enumerate(season_ids):
        data = fetch_team_stats(sid)
        teams = data.get("data") or []
        team_stats_by_season[sid] = {t.get("id"): t for t in teams if t.get("id")}
        print(f"  [{i+1}/{len(season_ids)}] season {sid}: {len(teams)} teams")
        time.sleep(0.5)

    print(f"\nBuilding match_results rows...")
    rows = []
    by_rank = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
    skipped = {"missing_stats": 0, "womens": 0, "no_ids": 0}
    for fx in completed:
        sid = fx.get("competition_id")
        if sid in WOMENS_LEAGUE_IDS:
            skipped["womens"] += 1
            continue
        h_id = fx.get("homeID") or fx.get("home_id")
        a_id = fx.get("awayID") or fx.get("away_id")
        if not h_id or not a_id:
            skipped["no_ids"] += 1
            continue
        teams = team_stats_by_season.get(sid) or {}
        h_team = teams.get(h_id)
        a_team = teams.get(a_id)
        if not h_team or not a_team:
            skipped["missing_stats"] += 1
            continue

        h_stats = extract_stats(h_team, "home")
        a_stats = extract_stats(a_team, "away")
        snap = {"fetched_at": "backfill", "home": h_stats, "away": a_stats}
        result = compute_signals(snap)
        by_rank[result["rank"]] += 1

        fh_h = int(fx.get("ht_goals_team_a") or 0)
        fh_a = int(fx.get("ht_goals_team_b") or 0)
        ft_h = int(fx.get("homeGoalCount")  or 0)
        ft_a = int(fx.get("awayGoalCount")  or 0)
        fh_total = fh_h + fh_a

        rows.append({
            "match_id":       fx.get("id"),
            "competition_id": sid,
            "league_name":    league_names.get(sid),
            "home_id":        h_id,
            "away_id":        a_id,
            "home_name":      h_stats["name"],
            "away_name":      a_stats["name"],
            "date_unix":      fx.get("date_unix"),
            "ht_home":        fh_h,
            "ht_away":        fh_a,
            "ft_home":        ft_h,
            "ft_away":        ft_a,
            "fh_total":       fh_total,
            "hit_15":         fh_total > 1,
            "hit_25":         fh_total > 2,
            "rank":           result["rank"],
            "ci":             result["ci"],
            "def_ci":         result["def_ci"],
            "prob25":         result["prob25"],
            "prob15":         result["prob15"],
            "signals":        result["signals"],
            "snap": {
                "fetchedAt": snap["fetched_at"],
                "home": {"name": h_stats["name"], "scored_fh": h_stats["scored_fh"],
                         "conced_fh": h_stats["conced_fh"], "t1_pct": h_stats["t1_pct"],
                         "cn010_avg": h_stats["cn010_avg"], "sot_avg": 0},
                "away": {"name": a_stats["name"], "scored_fh": a_stats["scored_fh"],
                         "conced_fh": a_stats["conced_fh"], "t1_pct": a_stats["t1_pct"],
                         "cn010_avg": a_stats["cn010_avg"], "sot_avg": 0},
            },
        })

    print(f"  built {len(rows)} rows")
    print(f"  skipped: {skipped}")
    print(f"  by rank: {by_rank}")

    if args.dry_run:
        print(f"\nDRY RUN — no writes. Sample row:")
        if rows:
            import json
            print(json.dumps(rows[0], indent=2, default=str)[:1500])
        return

    print(f"\nUpserting to Supabase in batches of {args.batch_size}...")
    written = 0
    for i in range(0, len(rows), args.batch_size):
        batch = rows[i:i + args.batch_size]
        n, err = supabase_upsert(batch)
        if err:
            print(f"  batch {i // args.batch_size + 1}: ERROR {err}")
            break
        written += n
        print(f"  batch {i // args.batch_size + 1}: {n} rows OK ({written}/{len(rows)})")

    print(f"\nDone: wrote {written}/{len(rows)} rows.")
    if written > 0:
        print("Tip: hit /admin/recalibrate?token=... or wait for the daily auto-recalibration.")


if __name__ == "__main__":
    main()
