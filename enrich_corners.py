"""
Build a first-half corners backtest dataset.

Mirrors enrich_dataset.py: reads dataset_combined_filled.csv, pulls data from
the FootyStats API per season, and writes a focused CSV keyed on match `id`
(joinable back to the goals dataset on `id`).

Outcome: first-half total corners, with over-line flags for 6+ / 7+ / 8+ / 9+ / 10+.

Predictors (no leakage — each computed only from data available before kickoff):
  * Pre-game rolling FH corner rates, per team, from that team's EARLIER
    same-season matches. Venue-specific (home team's home matches, away team's
    away matches), with a fallback to all-venue when the venue sample is thin.
  * Season-level full-match corner averages from /league-teams (kept for
    comparison; these are season aggregates, mildly leaky on a backtest).

Usage:
  export FOOTY_API_KEY=your_key_here
  python3 enrich_corners.py             # writes dataset_corners.csv
  python3 enrich_corners.py --discover  # dump corner-related API fields and exit

Notes:
  * In this dataset competition_id is used directly as the FootyStats season_id
    (same convention as enrich_dataset.py).
  * FH corners are derived from the per-minute corner timing arrays in
    /league-matches. Matches without recorded timings get blank outcome cells
    so they can be filtered out in analysis.
  * Women's leagues are flagged (is_womens), not dropped — exclude them when
    calibrating thresholds (see CLAUDE.md).
"""

import csv
import json
import os
import sys
import time
import urllib.request

API_KEY = os.environ.get("FOOTY_API_KEY")
if not API_KEY:
    print("ERROR: set FOOTY_API_KEY environment variable first")
    print("  export FOOTY_API_KEY=your_key_here")
    sys.exit(1)

BASE = "https://api.football-data-api.com"
INPUT_CSV = "dataset_combined_filled.csv"
OUTPUT_CSV = "dataset_corners.csv"
DISCOVER = "--discover" in sys.argv

WOMENS_LEAGUE_IDS = {15020, 16037, 16046, 16563}

# FH total-corner over-lines to flag (corners >= N).
TARGETS = [6, 7, 8, 9, 10]

# Minimum prior matches before a venue-specific rolling average is trusted;
# below this we fall back to the team's all-venue history.
MIN_VENUE_SAMPLE = 3

# Candidate field names for the per-minute corner timing arrays in a match
# object. FootyStats has historically used team_a_corner_timings /
# team_b_corner_timings; extra names are tried defensively.
TIMING_KEYS_A = ["team_a_corner_timings", "team_a_corner_timings_recorded"]
TIMING_KEYS_B = ["team_b_corner_timings", "team_b_corner_timings_recorded"]


# ─── HTTP ────────────────────────────────────────────────────────────────────

def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "footystats-proxy/enrich_corners"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read())
        except Exception as e:
            if attempt == retries - 1:
                return {"error": str(e)}
            time.sleep(2 ** attempt)
    return {"error": "unreachable"}


def fetch_league_matches(sid):
    """All matches for a season, paginated like server.js fetchLeagueMatches."""
    all_matches = []
    page = 1
    while page <= 5:
        url = (BASE + "/league-matches?season_id=" + sid +
               "&max_per_page=300&page=" + str(page) + "&key=" + API_KEY)
        data = fetch_json(url)
        if not isinstance(data, dict) or data.get("error"):
            err = data.get("error") if isinstance(data, dict) else "fetch failed"
            return (all_matches, None) if all_matches else ([], err)
        batch = data.get("data") or []
        all_matches.extend(batch)
        if len(batch) < 300:
            break
        page += 1
        time.sleep(1)
    return all_matches, None


def fetch_team_stats(sid):
    url = (BASE + "/league-teams?season_id=" + sid +
           "&include=stats&key=" + API_KEY)
    data = fetch_json(url)
    if not isinstance(data, dict) or data.get("error"):
        return {}, data.get("error") if isinstance(data, dict) else "fetch failed"
    out = {}
    for t in data.get("data", []):
        out[str(t.get("id", ""))] = t.get("stats", {}) or {}
    return out, None


# ─── CORNER PARSING ──────────────────────────────────────────────────────────

def parse_minute(v):
    """Timing entries are minute integers; tolerate strings and '45+2' forms."""
    if v is None:
        return None
    digits = ""
    for ch in str(v).strip():
        if ch.isdigit():
            digits += ch
        else:
            break
    return int(digits) if digits else None


def get_timing_array(match, keys):
    for k in keys:
        v = match.get(k)
        if isinstance(v, list):
            return v
    return None


def count_fh(arr):
    n = 0
    for x in (arr or []):
        m = parse_minute(x)
        if m is not None and m <= 45:
            n += 1
    return n


def corner_data(match):
    """Return (ft_home, ft_away, fh_home, fh_away).

    ft_* are full-match corner counts (None if not recorded).
    fh_* are first-half corner counts from timing arrays (None if timings not
    recorded for this match).
    """
    ft_a = match.get("team_a_corners")
    ft_b = match.get("team_b_corners")
    ft_a = ft_a if isinstance(ft_a, (int, float)) and ft_a >= 0 else None
    ft_b = ft_b if isinstance(ft_b, (int, float)) and ft_b >= 0 else None

    recorded_flag = match.get("corner_timings_recorded")
    arr_a = get_timing_array(match, TIMING_KEYS_A)
    arr_b = get_timing_array(match, TIMING_KEYS_B)

    fh_a = fh_b = None
    has_arrays = arr_a is not None or arr_b is not None
    if recorded_flag == 1 or (recorded_flag is None and has_arrays):
        fh_a = count_fh(arr_a)
        fh_b = count_fh(arr_b)
    return ft_a, ft_b, fh_a, fh_b


# ─── DISCOVER MODE ───────────────────────────────────────────────────────────

def run_discover(first_sid):
    print("Discover mode — sampling season " + first_sid + "\n")

    matches, err = fetch_league_matches(first_sid)
    if err:
        print("  league-matches error: " + str(err))
    elif matches:
        m = matches[0]
        print("  /league-matches sample match keys containing 'corner':")
        for k in sorted(m.keys()):
            if "corner" in k.lower():
                v = m[k]
                shown = v if not isinstance(v, list) else (
                    "[" + str(len(v)) + " items] " + str(v[:8]))
                print("    " + k + " = " + str(shown))
        print()

    time.sleep(1)
    stats_map, err = fetch_team_stats(first_sid)
    if err:
        print("  league-teams error: " + str(err))
    elif stats_map:
        sample = next(iter(stats_map.values()))
        print("  /league-teams sample stats keys containing 'corner':")
        for k in sorted(sample.keys()):
            if "corner" in k.lower():
                print("    " + k + " = " + str(sample[k]))
    print("\nVerify the field names above match what the script expects "
          "(TIMING_KEYS_A/B, season_corner_stats), then run without --discover.")


# ─── ROLLING PRE-GAME FEATURES + OUTCOMES ────────────────────────────────────

def avg(pairs, idx):
    vals = [p[idx] for p in pairs]
    return round(sum(vals) / len(vals), 3) if vals else None


def build_season_index(matches):
    """Walk a season chronologically and produce, per match id, a dict with the
    FH corner outcome plus the pre-game rolling features for both teams
    (features use strictly earlier matches — no leakage).
    """
    played = []
    for m in matches:
        if m.get("status") != "complete":
            continue
        if not isinstance(m.get("date_unix"), (int, float)):
            continue
        played.append(m)
    played.sort(key=lambda m: m["date_unix"])

    hist_home = {}   # team_id -> [(fh_for, fh_against)] from HOME matches
    hist_away = {}   # team_id -> [(fh_for, fh_against)] from AWAY matches
    hist_all = {}    # team_id -> [(fh_for, fh_against)] from any venue
    index = {}

    for m in played:
        hid = str(m.get("homeID", ""))
        aid = str(m.get("awayID", ""))
        ft_h, ft_a, fh_h, fh_a = corner_data(m)

        h_venue = hist_home.get(hid, [])
        a_venue = hist_away.get(aid, [])
        h_all = hist_all.get(hid, [])
        a_all = hist_all.get(aid, [])

        def venue_or_all(venue_hist, all_hist, idx):
            if len(venue_hist) >= MIN_VENUE_SAMPLE:
                return avg(venue_hist, idx)
            return avg(all_hist, idx)

        h_for = venue_or_all(h_venue, h_all, 0)
        h_against = venue_or_all(h_venue, h_all, 1)
        a_for = venue_or_all(a_venue, a_all, 0)
        a_against = venue_or_all(a_venue, a_all, 1)

        cc_intensity = cc_defint = None
        if None not in (h_for, h_against, a_for, a_against):
            cc_intensity = round(h_for + h_against + a_for + a_against, 3)
            cc_defint = round(h_against + a_against, 3)

        fh_total = (fh_h + fh_a) if (fh_h is not None and fh_a is not None) else None
        ft_total = (ft_h + ft_a) if (ft_h is not None and ft_a is not None) else None

        index[str(m.get("id", ""))] = {
            "ft_corners_home": ft_h, "ft_corners_away": ft_a,
            "ft_corners_total": ft_total,
            "fh_corners_home": fh_h, "fh_corners_away": fh_a,
            "fh_corners_total": fh_total,
            "fh_recorded": 1 if fh_total is not None else 0,
            "h_fhc_for": h_for, "h_fhc_against": h_against,
            "a_fhc_for": a_for, "a_fhc_against": a_against,
            "cc_intensity": cc_intensity, "cc_defint": cc_defint,
            "h_prior_n": len(h_all), "a_prior_n": len(a_all),
            "h_venue_n": len(h_venue), "a_venue_n": len(a_venue),
        }

        # Update history AFTER recording this match's features (no leakage).
        if fh_h is not None and fh_a is not None:
            hist_home.setdefault(hid, []).append((fh_h, fh_a))
            hist_away.setdefault(aid, []).append((fh_a, fh_h))
            hist_all.setdefault(hid, []).append((fh_h, fh_a))
            hist_all.setdefault(aid, []).append((fh_a, fh_h))

    return index


# ─── TEAM SEASON STATS ───────────────────────────────────────────────────────

def stat(stats, *names):
    for n in names:
        v = stats.get(n)
        if v is not None:
            return v
    return ""


def season_corner_stats(stats_map, team_id, venue):
    s = stats_map.get(str(team_id), {})
    sfx = "_home" if venue == "home" else "_away"
    return {
        "cornersAVG": stat(s, "cornersAVG" + sfx, "cornersAVG_overall",
                           "cornersTotalAVG" + sfx, "cornersTotalAVG_overall"),
        "cornersAgainstAVG": stat(s, "cornersAgainstAVG" + sfx,
                                  "cornersAgainstAVG_overall",
                                  "cornersAgainst_avg" + sfx),
    }


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    with open(INPUT_CSV) as f:
        rows = list(csv.DictReader(f))

    season_ids = sorted({r["competition_id"] for r in rows if r.get("competition_id")})
    print("Loaded " + str(len(rows)) + " rows, " +
          str(len(season_ids)) + " unique seasons")

    if DISCOVER:
        run_discover(season_ids[0])
        return

    match_index = {}          # match_id -> outcome + feature dict
    season_team_stats = {}     # season_id -> stats_map
    failed = []

    for i, sid in enumerate(season_ids):
        print("  [" + str(i + 1) + "/" + str(len(season_ids)) +
              "] season " + sid + " ...", end=" ", flush=True)

        matches, err = fetch_league_matches(sid)
        if err:
            print("matches FAILED: " + str(err))
            failed.append(sid)
            time.sleep(1)
            continue

        match_index.update(build_season_index(matches))

        time.sleep(1)
        stats_map, serr = fetch_team_stats(sid)
        if serr:
            print(str(len(matches)) + " matches, team-stats FAILED: " + str(serr))
        else:
            season_team_stats[sid] = stats_map
            print(str(len(matches)) + " matches, " +
                  str(len(stats_map)) + " teams")

        time.sleep(1)

    out_fields = [
        "id", "competition_id", "date_unix", "status", "is_womens",
        "home_id", "away_id", "home_name", "away_name",
        "ft_corners_home", "ft_corners_away", "ft_corners_total",
        "fh_corners_home", "fh_corners_away", "fh_corners_total", "fh_recorded",
    ]
    out_fields += ["corners_" + str(t) + "plus" for t in TARGETS]
    out_fields += [
        "h_fhc_for", "h_fhc_against", "a_fhc_for", "a_fhc_against",
        "cc_intensity", "cc_defint",
        "h_prior_n", "a_prior_n", "h_venue_n", "a_venue_n",
        "h_cornersAVG", "h_cornersAgainstAVG",
        "a_cornersAVG", "a_cornersAgainstAVG",
    ]

    out_rows = []
    n_recorded = 0
    base_counts = {t: 0 for t in TARGETS}

    def blank(v):
        return "" if v is None else v

    for r in rows:
        mid = r.get("id", "")
        sid = r.get("competition_id", "")
        comp = int(sid) if sid.isdigit() else 0
        idx = match_index.get(mid, {})
        stats_map = season_team_stats.get(sid, {})
        h_ss = season_corner_stats(stats_map, r.get("homeID", ""), "home")
        a_ss = season_corner_stats(stats_map, r.get("awayID", ""), "away")

        fh_total = idx.get("fh_corners_total")
        out = {
            "id": mid,
            "competition_id": sid,
            "date_unix": r.get("date_unix", ""),
            "status": r.get("status", ""),
            "is_womens": 1 if comp in WOMENS_LEAGUE_IDS else 0,
            "home_id": r.get("homeID", ""),
            "away_id": r.get("awayID", ""),
            "home_name": r.get("home_name", ""),
            "away_name": r.get("away_name", ""),
            "ft_corners_home": blank(idx.get("ft_corners_home")),
            "ft_corners_away": blank(idx.get("ft_corners_away")),
            "ft_corners_total": blank(idx.get("ft_corners_total")),
            "fh_corners_home": blank(idx.get("fh_corners_home")),
            "fh_corners_away": blank(idx.get("fh_corners_away")),
            "fh_corners_total": blank(fh_total),
            "fh_recorded": idx.get("fh_recorded", 0),
            "h_fhc_for": blank(idx.get("h_fhc_for")),
            "h_fhc_against": blank(idx.get("h_fhc_against")),
            "a_fhc_for": blank(idx.get("a_fhc_for")),
            "a_fhc_against": blank(idx.get("a_fhc_against")),
            "cc_intensity": blank(idx.get("cc_intensity")),
            "cc_defint": blank(idx.get("cc_defint")),
            "h_prior_n": blank(idx.get("h_prior_n")),
            "a_prior_n": blank(idx.get("a_prior_n")),
            "h_venue_n": blank(idx.get("h_venue_n")),
            "a_venue_n": blank(idx.get("a_venue_n")),
            "h_cornersAVG": h_ss["cornersAVG"],
            "h_cornersAgainstAVG": h_ss["cornersAgainstAVG"],
            "a_cornersAVG": a_ss["cornersAVG"],
            "a_cornersAgainstAVG": a_ss["cornersAgainstAVG"],
        }
        for t in TARGETS:
            if fh_total is None:
                out["corners_" + str(t) + "plus"] = ""
            else:
                hit = 1 if fh_total >= t else 0
                out["corners_" + str(t) + "plus"] = hit
                if hit:
                    base_counts[t] += 1
        if fh_total is not None:
            n_recorded += 1
        out_rows.append(out)

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(out_rows)

    print("\nWrote " + OUTPUT_CSV + " — " + str(len(out_rows)) + " rows")
    print("FH corners recorded: " + str(n_recorded) + "/" + str(len(out_rows)))
    if n_recorded:
        print("Base rates (all rows with recorded FH corners):")
        for t in TARGETS:
            pct = round(base_counts[t] / n_recorded * 100, 1)
            print("  FH corners " + str(t) + "+ : " +
                  str(base_counts[t]) + " (" + str(pct) + "%)")
    if failed:
        print("Failed seasons (" + str(len(failed)) + "): " + ", ".join(failed))
    print("\nNext: run --discover first if any corner column is all-blank, "
          "then analyze dataset_corners.csv for signal lift.")


if __name__ == "__main__":
    main()
