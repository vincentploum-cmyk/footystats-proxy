"""
Recalibrate the rank -> probability tables from LOOK-AHEAD-FREE pre-game stats.

Why this exists
---------------
dataset_combined_filled.csv stores full-season aggregate team stats
(scoredAVGHT, seasonOver25PercentageHT, ...). Those reflect games that had
not been played yet at kickoff, so any backtest on them is inflated by
look-ahead bias. This script reconstructs each team's season-to-date,
role-specific stats using ONLY matches played before each fixture, recomputes
the 4 signals + rank exactly as server.js does, and reports the honest
PROB25_BY_RANK / PROB15_BY_RANK tables.

Usage:
  python3 scripts/recalibrate_pregame.py [path/to/dataset_combined_filled.csv]

Output: prints honest probability tables ready to paste into server.js.
"""

import csv
import sys
from collections import defaultdict

WOMENS_LEAGUE_IDS = {15020, 16037, 16046, 16563}
MIN_PRIOR_GAMES = 3  # mirror extractStats: role stats need >=3 games, else fall back


def fnum(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def fint(s):
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def load_games(path):
    games = []
    with open(path, newline="") as f:
        for x in csv.DictReader(f):
            d = fint(x["date_unix"])
            comp = fint(x["competition_id"])
            hid, aid = fint(x["homeID"]), fint(x["awayID"])
            a, b = fint(x["ht_goals_team_a"]), fint(x["ht_goals_team_b"])
            fho, fh1 = fint(x["fh_over25"]), fint(x["fh_over15"])
            if None in (d, comp, hid, aid, a, b, fho, fh1):
                continue
            games.append({"d": d, "comp": comp, "hid": hid, "aid": aid,
                          "a": a, "b": b, "fho": fho, "fh1": fh1})
    games.sort(key=lambda g: g["d"])
    return games


def reconstruct(games):
    # tally[(comp, team, role)] = [n, fh_scored, fh_conceded, over25_count]
    tally = defaultdict(lambda: [0, 0, 0, 0])

    def role_stat(comp, team, role):
        n, sc, cc, ov = tally[(comp, team, role)]
        return (sc / n, cc / n, ov / n * 100, n) if n else None

    def overall_stat(comp, team):
        h, a = tally[(comp, team, "H")], tally[(comp, team, "A")]
        n = h[0] + a[0]
        return ((h[1] + a[1]) / n, (h[2] + a[2]) / n, (h[3] + a[3]) / n * 100, n) if n else None

    def team_stat(comp, team, role):
        rs = role_stat(comp, team, role)
        if rs and rs[3] >= MIN_PRIOR_GAMES:
            return rs
        return overall_stat(comp, team)

    recs = []
    for g in games:
        hs = team_stat(g["comp"], g["hid"], "H")
        as_ = team_stat(g["comp"], g["aid"], "A")
        if (hs and as_ and hs[3] >= MIN_PRIOR_GAMES and as_[3] >= MIN_PRIOR_GAMES
                and g["comp"] not in WOMENS_LEAGUE_IDS):
            recs.append({"h_sc": hs[0], "h_cc": hs[1], "h_t1": hs[2],
                         "a_sc": as_[0], "a_cc": as_[1], "a_t1": as_[2],
                         "fho": g["fho"], "fh1": g["fh1"]})
        # update tallies AFTER using the pre-game state
        over = 1 if (g["a"] + g["b"]) > 2 else 0
        th = tally[(g["comp"], g["hid"], "H")]
        th[0] += 1; th[1] += g["a"]; th[2] += g["b"]; th[3] += over
        ta = tally[(g["comp"], g["aid"], "A")]
        ta[0] += 1; ta[1] += g["b"]; ta[2] += g["a"]; ta[3] += over
    return recs


def rank_of(r):
    # Mirrors server.js computeSignals(), including the "sig C required for
    # rank 3+" gate at line 487.
    sig_a = (r["h_sc"] + r["a_sc"] + r["h_cc"] + r["a_cc"]) >= 3.2
    sig_b = r["h_t1"] >= 25 and r["a_t1"] >= 25
    sig_c = (r["h_cc"] + r["a_cc"]) >= 2.25
    sig_d = r["a_sc"] >= 1.25
    raw = sum([sig_a, sig_b, sig_c, sig_d])
    return 2 if (raw >= 3 and not sig_c) else raw


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "dataset_combined_filled.csv"
    games = load_games(path)
    recs = reconstruct(games)
    n = len(recs)
    base25 = sum(r["fho"] for r in recs) / n * 100
    base15 = sum(r["fh1"] for r in recs) / n * 100
    print(f"Pre-game matches (both teams {MIN_PRIOR_GAMES}+ prior games): {n:,}")
    print(f"Base rate FH>2.5 = {base25:.1f}%   FH>1.5 = {base15:.1f}%\n")

    buckets = defaultdict(lambda: [0, 0, 0])  # rank -> [n, fho, fh1]
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

    print("\n// Honest, look-ahead-free tables (paste into server.js):")
    print("const PROB25_BY_RANK = { " + ", ".join(f"{k}: {p25[k]}" for k in (4, 3, 2, 1, 0)) + " };")
    print("const PROB15_BY_RANK = { " + ", ".join(f"{k}: {p15[k]}" for k in (4, 3, 2, 1, 0)) + " };")


if __name__ == "__main__":
    main()
