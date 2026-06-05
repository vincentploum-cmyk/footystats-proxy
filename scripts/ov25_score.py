#!/usr/bin/env python3
"""OV2.5 1H shortlist scorer — additive score, no internal signals.

Reads a footy-dataset CSV export, adds `score` and `tier` columns, writes a copy
ranked by score (desc), and prints the over-2.5 hit rate per score and per tier.

  score = 0
  +2 if prob25      >= 14.5
  +1 if prob15      >= 43.6
  +1 if pm_o05HT    >= 80
  +1 if pm_o15HT    >= 43
  +1 if a_t1_pct    >= 19
  +1 if a_conced_fh >= 0.83
  +1 if l5_h_t      >= 1.6     (tie-breaker)

  tiers:  0-2 ignore | 3-4 watchlist | 5 shortlist | 6+ strongest

Usage:
  python3 scripts/ov25_score.py <export.csv> [scored_output.csv]
"""
import csv
import sys


def f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def score_row(r):
    s = 0

    def ge(col, thr):
        v = f(r.get(col))
        return v is not None and v >= thr

    if ge("prob25", 14.5):      s += 2
    if ge("prob15", 43.6):      s += 1
    if ge("pm_o05HT", 80):      s += 1
    if ge("pm_o15HT", 43):      s += 1
    if ge("a_t1_pct", 19):      s += 1
    if ge("a_conced_fh", 0.83): s += 1
    if ge("l5_h_t", 1.6):       s += 1
    return s


def tier(s):
    if s >= 6:
        return "strongest"
    if s >= 5:
        return "shortlist"
    if s >= 3:
        return "watchlist"
    return "ignore"


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    src = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else "scored_" + src.split("/")[-1]

    with open(src, newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        print("no rows")
        return

    for r in rows:
        r["score"] = score_row(r)
        r["tier"] = tier(r["score"])
    rows.sort(key=lambda r: r["score"], reverse=True)

    cols = [c for c in rows[0].keys() if c not in ("score", "tier")] + ["score", "tier"]
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    has_hit = "hit_25" in rows[0]

    def rate(group):
        n = len(group)
        h = sum(1 for r in group if str(r.get("hit_25")).strip() in ("1", "1.0")) if has_hit else 0
        return n, h, (100.0 * h / n if (n and has_hit) else 0.0)

    print("Scored %d rows -> %s\n" % (len(rows), out))
    if not has_hit:
        print("(no hit_25 column — wrote ranked file only)")
        return

    print("score    n    hits   OV2.5%")
    for s in range(8, -1, -1):
        g = [r for r in rows if r["score"] == s]
        if not g:
            continue
        n, h, p = rate(g)
        print("  %2d   %4d   %4d   %5.1f" % (s, n, h, p))

    print("\ntier         n    hits   OV2.5%")
    for t in ("strongest", "shortlist", "watchlist", "ignore"):
        g = [r for r in rows if r["tier"] == t]
        if not g:
            continue
        n, h, p = rate(g)
        print("  %-9s %4d   %4d   %5.1f" % (t, n, h, p))


if __name__ == "__main__":
    main()
