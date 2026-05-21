"""
Calibrate the rank -> probability tables for the recent-form signal engine,
using only LOOK-AHEAD-FREE data.

Why this exists
---------------
The original signals leaned on full-season aggregate stats
(seasonOver25PercentageHT, scoredAVGHT, ...). In dataset_combined_filled.csv
those are end-of-season figures, so any backtest on them is inflated by
look-ahead bias — and when reconstructed honestly they barely separate games.

The rewritten engine uses each team's last-5 first-half form, which IS available
pre-kickoff and DOES produce a monotonic rank gradient. This script rebuilds
each team's rolling last-5 FH form using only matches before each fixture,
recomputes the 4 recent-form signals + rank exactly as server.js computeSignals()
does, and reports the honest PROB25_BY_RANK / PROB15_BY_RANK tables.

Signals (all from last-5 FH form, look-ahead-free):
  A Recent Intensity : (home L5 FH total) + (away L5 FH total) >= 4.0
  B Attack vs Leak   : home L5 FH-scored > 1.0  AND away L5 FH-conceded > 1.0
  C Both Scoring     : home L5 FH-scored > 0.8  AND away L5 FH-scored   > 0.8
  D Both Open        : home L5 FH-total  > 1.5   AND away L5 FH-total    > 1.5

Usage:
  python3 scripts/recalibrate_pregame.py [path/to/dataset_combined_filled.csv]
"""

import csv
import sys
from collections import defaultdict

WOMENS_LEAGUE_IDS = {15020, 16037, 16046, 16563}
MIN_RECENT_GAMES = 3  # need at least this many prior games to form a signal


def fint(s):
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def load_games(path):
    games = []
    with open(path, newline="") as f:
        for x in csv.DictReader(f):
            d = fint(x["date_unix"]); comp = fint(x["competition_id"])
            hid, aid = fint(x["homeID"]), fint(x["awayID"])
            a, b = fint(x["ht_goals_team_a"]), fint(x["ht_goals_team_b"])
            fho, fh1 = fint(x["fh_over25"]), fint(x["fh_over15"])
            if None in (d, comp, hid, aid, a, b, fho, fh1):
                continue
            games.append({"d": d, "comp": comp, "hid": hid, "aid": aid,
                          "a": a, "b": b, "fho": fho, "fh1": fh1})
    games.sort(key=lambda g: g["d"])
    return games


def last5(history):
    """Average FH scored/conceded/total over a team's most recent <=5 games."""
    games = history[-5:]
    if len(games) < MIN_RECENT_GAMES:
        return None
    n = len(games)
    f = sum(g["f"] for g in games) / n
    a = sum(g["a"] for g in games) / n
    return {"f": f, "a": a, "t": f + a}


def reconstruct(games):
    hist = defaultdict(list)
    recs = []
    for g in games:
        h5 = last5(hist[g["hid"]])
        a5 = last5(hist[g["aid"]])
        if h5 and a5 and g["comp"] not in WOMENS_LEAGUE_IDS:
            recs.append({"h5": h5, "a5": a5, "fho": g["fho"], "fh1": g["fh1"]})
        # append THIS match to each team's history after using prior form
        hist[g["hid"]].append({"f": g["a"], "a": g["b"]})
        hist[g["aid"]].append({"f": g["b"], "a": g["a"]})
    return recs


def rank_of(r):
    """Mirrors server.js computeSignals() — pure count, no gate."""
    h5, a5 = r["h5"], r["a5"]
    sig_a = (h5["t"] + a5["t"]) >= 4.0
    sig_b = h5["f"] > 1.0 and a5["a"] > 1.0
    sig_c = h5["f"] > 0.8 and a5["f"] > 0.8
    sig_d = h5["t"] > 1.5 and a5["t"] > 1.5
    return sum([sig_a, sig_b, sig_c, sig_d])


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "dataset_combined_filled.csv"
    recs = reconstruct(load_games(path))
    n = len(recs)
    base25 = sum(r["fho"] for r in recs) / n * 100
    base15 = sum(r["fh1"] for r in recs) / n * 100
    print(f"Pre-game matches (both teams {MIN_RECENT_GAMES}+ prior games): {n:,}")
    print(f"Base rate FH>2.5 = {base25:.1f}%   FH>1.5 = {base15:.1f}%\n")

    buckets = defaultdict(lambda: [0, 0, 0])
    for r in recs:
        b = buckets[rank_of(r)]
        b[0] += 1; b[1] += r["fho"]; b[2] += r["fh1"]

    print(f"{'rank':>4} {'n':>7} {'FH>2.5':>8} {'FH>1.5':>8}")
    p25, p15 = {}, {}
    for rk in range(5):
        cnt, h25, h15 = buckets.get(rk, [0, 0, 0])
        if cnt == 0:
            p25[rk] = round(base25, 1); p15[rk] = round(base15, 1)
            print(f"{rk:>4} {0:>7} {'--':>8} {'--':>8}")
            continue
        v25, v15 = round(h25 / cnt * 100, 1), round(h15 / cnt * 100, 1)
        p25[rk], p15[rk] = v25, v15
        print(f"{rk:>4} {cnt:>7,} {v25:>7.1f}% {v15:>7.1f}%")

    print("\n// Paste into server.js:")
    print("const PROB25_BY_RANK = { " + ", ".join(f"{k}: {p25[k]}" for k in (4, 3, 2, 1, 0)) + " };")
    print("const PROB15_BY_RANK = { " + ", ".join(f"{k}: {p15[k]}" for k in (4, 3, 2, 1, 0)) + " };")


if __name__ == "__main__":
    main()
