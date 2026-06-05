#!/usr/bin/env python3
"""First-half over-goals tools — shortlist score + O1.5 & O2.5 filters (pre-game only).

Reads a footy-dataset CSV export, adds scoring/filter columns, writes a ranked
copy, and prints hit rates per score/tier and for each filter — validated on the
data itself. O1.5 FH = 2+ first-half goals (hit_15); O2.5 FH = 3+ (hit_25).

SHORTLIST SCORE (additive, tuned to O2.5):
  +2 prob25>=14.5  +1 prob15>=43.6  +1 pm_o05HT>=80  +1 pm_o15HT>=43
  +1 a_t1_pct>=19  +1 a_conced_fh>=0.83  +1 l5_h_t>=1.6
  tiers: 0-2 ignore | 3-4 watchlist | 5 shortlist | 6+ strongest

O2.5 FH FILTER (all three -> play25):
  env_fh = h_scored_fh + h_conced_fh + a_scored_fh + a_conced_fh  >= 2.85
  l5_fh  = l5_h_f + l5_a_f                                        >= 1.6
  prob25                                                          >= 14.5

O1.5 FH FILTER (all three -> play15):
  env_fh                                                          >= 2.60
  l5_fh                                                           >= 1.4
  prob15                                                          >= 38

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


def g(r, col):
    """Field as float, missing/blank -> 0.0 (so blanks never clear a threshold)."""
    v = f(r.get(col))
    return v if v is not None else 0.0


# ── shortlist score (O2.5-tuned) ─────────────────────────────────────────────
def score_row(r):
    s = 0
    if g(r, "prob25") >= 14.5:      s += 2
    if g(r, "prob15") >= 43.6:      s += 1
    if g(r, "pm_o05HT") >= 80:      s += 1
    if g(r, "pm_o15HT") >= 43:      s += 1
    if g(r, "a_t1_pct") >= 19:      s += 1
    if g(r, "a_conced_fh") >= 0.83: s += 1
    if g(r, "l5_h_t") >= 1.6:       s += 1
    return s


def tier(s):
    if s >= 6:
        return "strongest"
    if s >= 5:
        return "shortlist"
    if s >= 3:
        return "watchlist"
    return "ignore"


# ── O1.5 & O2.5 filters (shared combined features, different thresholds) ──────
def filter_eval(r):
    # Round the combined sums so binary float error can't drop a match that sits
    # exactly on a threshold (e.g. 0.7+0.6+0.6+0.7 -> 2.5999999 < 2.60).
    env_fh = round(g(r, "h_scored_fh") + g(r, "h_conced_fh") + g(r, "a_scored_fh") + g(r, "a_conced_fh"), 4)
    l5_fh = round(g(r, "l5_h_f") + g(r, "l5_a_f"), 4)
    play25 = (env_fh >= 2.85) and (l5_fh >= 1.6) and (g(r, "prob25") >= 14.5)
    play15 = (env_fh >= 2.60) and (l5_fh >= 1.4) and (g(r, "prob15") >= 38)
    return env_fh, l5_fh, play25, play15


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    src = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else "scored_" + src.split("/")[-1]

    with open(src, newline="") as fh:
        reader = csv.DictReader(fh)
        base_cols = list(reader.fieldnames or [])
        rows = list(reader)
    if not rows:
        print("no rows")
        return

    for r in rows:
        r["score"] = score_row(r)
        r["tier"] = tier(r["score"])
        env_fh, l5_fh, play25, play15 = filter_eval(r)
        r["env_fh"] = round(env_fh, 2)
        r["l5_fh"] = round(l5_fh, 2)
        r["play25"] = 1 if play25 else 0
        r["play15"] = 1 if play15 else 0
    rows.sort(key=lambda r: (r["play25"], r["play15"], r["score"]), reverse=True)

    extra = ["score", "tier", "env_fh", "l5_fh", "play25", "play15"]
    cols = [c for c in base_cols if c not in extra] + extra
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    def rate(group, hitcol):
        n = len(group)
        h = sum(1 for r in group if str(r.get(hitcol)).strip() in ("1", "1.0"))
        return n, h, (100.0 * h / n if n else 0.0)

    print("Scored %d rows -> %s\n" % (len(rows), out))

    def filter_block(title, playcol, hitcol):
        if hitcol not in base_cols:
            return
        bn, bh, bp = rate(rows, hitcol)
        print("%s  (base %.1f%%)" % (title, bp))
        print("            n    hits    rate")
        for label, want in (("PLAY", 1), ("skip", 0)):
            grp = [r for r in rows if r[playcol] == want]
            n, h, p = rate(grp, hitcol)
            lift = (p / bp) if bp else 0.0
            print("  %-6s  %4d   %4d   %5.1f   (lift %.2f)" % (label, n, h, p, lift))
        print("")

    filter_block("O2.5 FH FILTER (env>=2.85 & l5>=1.6 & prob25>=14.5)  target hit_25", "play25", "hit_25")
    filter_block("O1.5 FH FILTER (env>=2.60 & l5>=1.4 & prob15>=38)   target hit_15", "play15", "hit_15")

    if "hit_25" in base_cols:
        print("SHORTLIST SCORE (target hit_25)")
        print("score    n    hits    rate")
        for s in range(8, -1, -1):
            grp = [r for r in rows if r["score"] == s]
            if not grp:
                continue
            n, h, p = rate(grp, "hit_25")
            print("  %2d   %4d   %4d   %5.1f" % (s, n, h, p))
        print("\ntier         n    hits    rate")
        for t in ("strongest", "shortlist", "watchlist", "ignore"):
            grp = [r for r in rows if r["tier"] == t]
            if not grp:
                continue
            n, h, p = rate(grp, "hit_25")
            print("  %-9s %4d   %4d   %5.1f" % (t, n, h, p))


if __name__ == "__main__":
    main()
