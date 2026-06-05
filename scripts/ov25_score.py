#!/usr/bin/env python3
"""OV2.5 first-half tools — shortlist score + 3-rule filter (pre-game fields only).

Reads a footy-dataset CSV export, adds scoring/filter columns, writes a ranked
copy, and prints the over-2.5 (3+ first-half goals) hit rate per score, per tier,
and for the 3-rule filter — so everything validates on the data itself.

SHORTLIST SCORE (additive):
  +2 if prob25 >= 14.5
  +1 if prob15 >= 43.6
  +1 if pm_o05HT >= 80
  +1 if pm_o15HT >= 43
  +1 if a_t1_pct >= 19
  +1 if a_conced_fh >= 0.83
  +1 if l5_h_t >= 1.6
  tiers: 0-2 ignore | 3-4 watchlist | 5 shortlist | 6+ strongest

3-RULE FILTER (all three must hold = play):
  Rule 1  combined historical FH environment:
          h_scored_fh + h_conced_fh + a_scored_fh + a_conced_fh  >= 2.85
  Rule 2  combined last-5 FH scoring:
          l5_h_f + l5_a_f                                        >= 1.6
  Rule 3  model probability:
          prob25                                                >= 14.5

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


# ── shortlist score ──────────────────────────────────────────────────────────
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


# ── 3-rule filter ────────────────────────────────────────────────────────────
def filter_eval(r):
    env_fh = g(r, "h_scored_fh") + g(r, "h_conced_fh") + g(r, "a_scored_fh") + g(r, "a_conced_fh")
    l5_fh = g(r, "l5_h_f") + g(r, "l5_a_f")
    rule1 = env_fh >= 2.85
    rule2 = l5_fh >= 1.6
    rule3 = g(r, "prob25") >= 14.5
    return env_fh, l5_fh, (rule1 and rule2 and rule3)


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
        env_fh, l5_fh, play = filter_eval(r)
        r["env_fh"] = round(env_fh, 2)
        r["l5_fh"] = round(l5_fh, 2)
        r["play"] = 1 if play else 0
    rows.sort(key=lambda r: (r["play"], r["score"]), reverse=True)

    extra = ["score", "tier", "env_fh", "l5_fh", "play"]
    cols = [c for c in base_cols if c not in extra] + extra
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    has_hit = "hit_25" in base_cols

    def rate(group):
        n = len(group)
        h = sum(1 for r in group if str(r.get("hit_25")).strip() in ("1", "1.0")) if has_hit else 0
        return n, h, (100.0 * h / n if (n and has_hit) else 0.0)

    print("Scored %d rows -> %s\n" % (len(rows), out))
    if not has_hit:
        print("(no hit_25 column — wrote ranked file only)")
        return

    bn, bh, bp = rate(rows)
    print("base rate: %d / %d = %.1f%% OV2.5 (3+ first-half goals)\n" % (bh, bn, bp))

    print("3-RULE FILTER (env_fh>=2.85 & l5_fh>=1.6 & prob25>=14.5)")
    print("            n    hits   OV2.5%")
    for label, grp in (("PLAY", [r for r in rows if r["play"] == 1]),
                       ("skip", [r for r in rows if r["play"] == 0])):
        n, h, p = rate(grp)
        lift = (p / bp) if bp else 0.0
        print("  %-6s  %4d   %4d   %5.1f   (lift %.2f)" % (label, n, h, p, lift))

    print("\nSHORTLIST SCORE")
    print("score    n    hits   OV2.5%")
    for s in range(8, -1, -1):
        grp = [r for r in rows if r["score"] == s]
        if not grp:
            continue
        n, h, p = rate(grp)
        print("  %2d   %4d   %4d   %5.1f" % (s, n, h, p))
    print("\ntier         n    hits   OV2.5%")
    for t in ("strongest", "shortlist", "watchlist", "ignore"):
        grp = [r for r in rows if r["tier"] == t]
        if not grp:
            continue
        n, h, p = rate(grp)
        print("  %-9s %4d   %4d   %5.1f" % (t, n, h, p))


if __name__ == "__main__":
    main()
