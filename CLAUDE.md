# CLAUDE.md — footystats-proxy

## Project Overview

A Node.js/Express proxy server that predicts first-half (FH) goals in football matches. It fetches data from the football-data-api.com API, applies a 3-signal pre-game ranking algorithm, and serves predictions via a server-rendered HTML frontend with async JSON loading.

**Live:** https://footystats-proxy.onrender.com | **Hosted on:** Render.com

## Architecture

**Near-single-file monolith:** Almost all logic lives in `server.js`. No build step, no
transpilation, no framework — vanilla JS throughout. The one carve-out is `lib/freeze.js`:
the pre-game freeze invariant, extracted so it has a single source of truth and a test.

```
server.js          — Express server, API proxy, prediction engine, HTML frontend
lib/freeze.js      — Pre-game freeze contract (candidate flags + frozen-vs-live rule)
scripts/           — Offline tools: ov25_score.py (scorer), test_freeze.js (invariant test)
package.json       — Dependencies; `npm start`, `npm test`
.github/workflows/ — Keep-alive cron ping (every 20 min)
```

### Key Subsystems in server.js

| Section | Purpose |
|---------|---------|
| **safeFetch / rate limiting** | Wraps API calls with timeout, caching, and rate-limit detection |
| **In-memory caches** | `FIXTURE_CACHE`, `LEAGUE_MATCHES_CACHE`, `TEAM_STATS_CACHE`, `SERVER_MATCH_CACHE` with TTLs |
| **Prediction engine** | `buildLast5()` → `last5Form()` → `computeSignals()` pipeline; 2 signals — A + B (last-5 FH form). `extractStats()` builds the season-stat `snap` frozen pre-game. |
| **buildHTML()** | Server-side HTML/CSS/JS generation (inline, no templates) |
| **Routes** | `/` (HTML shell), `/preds` (JSON API), `/cache-status`, `/debug`, `/api/*` (passthrough) |

### Data Flow

```
Client loads / → HTML shell renders instantly
  → Client JS fetches /preds?tz=<offset>
    → Server fetches fixtures for 5-day range
    → For each league/season: fetch matches + team stats (cached)
    → extractStats() per team → computeSignals() per match
    → JSON response → Client renders predictions
```

## Development

### Prerequisites

- Node.js (no specific version pinned)
- `FOOTY_API_KEY` environment variable (required — app exits without it)
- `PORT` environment variable (optional, defaults to 3001)

### Commands

```bash
npm install        # Install dependencies
npm start          # Run server (node server.js)
```

There is no test suite, linter, or build step configured.

### Dependencies

- `express` — Web framework
- `cors` — CORS middleware
- `dotenv` — Environment variable loading
- `node-fetch@2` — HTTP client (CommonJS-compatible v2)

## Prediction Algorithm (2-Signal Ranking)

Rank is **derived from the calibrated probability**, not from counting signals, so rank
and probability always move together (a higher rank is always a higher-probability match).
It is tiered on `prob15` (FH-over-1.5, the primary bet): `>=45 → 3`, `>=40 → 2`, `>=30 → 1`,
else `0`. 🔥 Fire (2–3) → 📢 Signal (1) → Low (0). `eligible` = rank ≥ 2. On the A+B table
the max prob15 is 43.3% (combo 11), so the live range is rank 0–2; tier 3 stays defined but
unreachable. The match list sorts by probability (`prob25 → prob15 → ci → rank`).

### Signals

A and B are **last-5 FH form** signals (read from `snap.l5`, pre-kickoff). They are the
**only two production signals.** (Signal C — Team Mismatch — was removed; see below.)

| Signal | Name | Rule | Data source |
|--------|------|------|-------------|
| A | Mutual Instability | `snap.l5.home.t >= 1.6` AND `snap.l5.away.t >= 1.4` | Last-5 FH form (pre-kickoff) |
| B | Away Team Scoring | `snap.l5.away.f >= 0.8` | Last-5 FH form (pre-kickoff) |

`snap.l5.home.t` / `snap.l5.away.t` are each team's last-5 FH **total** (scored + conceded
per game); `snap.l5.away.f` is the away team's last-5 FH **scored** average.

**Both A and B require `snap.l5`** (both teams had ≥ 3 recent games — `last5Form()` returns
null otherwise). On thin-coverage leagues where `l5` is null, neither fires (combo `00`) —
the known blind spot. The O1.5/O2.5 candidate flags (below) are the only pre-game read there.

> **Signal C (Team Mismatch) was removed (2026-06).** On the full live cohort (n≈1022) its
> standalone FH>2.5 lift fell to ~1.0 (no edge), and B+C was *anti-additive* — below base
> rate. A directional split (home-stronger helps, away-stronger fades) showed a marginal
> rank-0-only gain but didn't justify the complexity. A+B carry the real lift (A 1.72, B 1.38)
> and rank monotonically without C. `/signalc-validate` and `/mismatch-holdout` still compute
> C from `snap` (independent of the live model) if you want to revisit it.

`computeSignals(snap, hLast5, aLast5)` returns:
- `rank` = probability tier (0–2; tier 3 unreachable on the A+B table), `label` = Low / Signal / Fire
- `prob15`, `prob25` = global combo-table probabilities (see below)
- `ci` = combined intensity (`homeL5Total + awayL5Total`) — used for display/sorting
- `defCi` = away last-5 FH total (`awayL5Total`)
- `eligible` = rank ≥ 2
- `ov15Candidate` / `ov25Candidate` = FH over-1.5 / over-2.5 **candidate** flags (see below);
  also `envFh` (= both teams' season `scored_fh + conced_fh`) and `l5Fh` (= home+away
  last-5 FH scored) — the two combined inputs they use
- `signals` = `{ A: {…}, B: {…}, O15: {…}, O25: {…} }`

### Over-1.5 / Over-2.5 FH candidate signals

Separate from the A/B combo, two **candidate** flags detect FH over-goal targets via three
pre-game checks that must all clear (rounded to absorb float drift at the boundary):

| Flag | env_fh (`Σ scored_fh+conced_fh`) | l5_fh (`Σ last-5 FH scored`) | model prob |
|------|------|------|------|
| `ov25Candidate` (Over 2.5 FH) | ≥ 2.85 | ≥ 1.6 | `prob25` ≥ 14.5 |
| `ov15Candidate` (Over 1.5 FH) | ≥ 2.60 | ≥ 1.4 | `prob15` ≥ 38 |

They surface as `signals.O15` / `signals.O25` and as **O1.5 / O2.5 badges** on the match card
(`candidatePill()`). They fire **no betPill** and don't affect rank/sort — detection only. The
identical logic runs offline in `scripts/ov25_score.py` over a dataset export. They rely on
season FH stats (always present) + `l5` (null on thin leagues → `l5_fh = 0` → won't fire there).

### Probability Tables

Combo-keyed (`PROB15_BY_COMBO` / `PROB25_BY_COMBO` in `server.js`, key = bit(A)+bit(B)),
recalibrated on the **clean live cohort (n=1075, women excluded)** via `/signalc-validate`:

| Combo (A B) | n | prob15 (FH>1.5) | prob25 (FH>2.5) | rank |
|-------|---|-----------------|-----------------|------|
| 11 — A+B | 97 | **43.3%** | **17.5%** | 2 (Fire) |
| 01 — B only | 330 | 39.1% | 10.9% | 1 (Signal) |
| 10 — A only | 42 | 38.1% | 14.3% | 1 (Signal) |
| 00 — neither | 606 | 29.9% | 9.1% | 0 (Low) |

Key findings:
- **A+B (combo 11) is the strongest FH>2.5/FH>1.5 combo** (17.5% / 43.3%, ~1.9x base). The
  O2.5 candidate pill effectively requires it (only combo 11 clears `prob25 >= 14.5`).
- **Ranking is monotonic** — rank 2 (A+B) > rank 1 (A or B) > rank 0 (neither) on both markets.
  No combo can outrank a stronger one (the old C-driven rank-2-below-rank-1 inversion is gone).
- **A alone is near baseline** (combo 10: O2.5 ≈ base). B carries the FH>1.5 edge; A+B together
  carry FH>2.5.
- betPill is signal-based: 🔥 when A+B fire, 🎯 when B fires.

Recalibrate via `/signalc-validate` (it still reports the full A/B/C breakdown, holdout
stability, and C's marginal lift — useful for re-checking the C-removal decision). Re-run
when the cohort roughly doubles. The live model reads only the 4-combo A+B table above.

### combo string format

`comboFromSignals()` returns a **2-char string**: bit(A) + bit(B).
e.g. "11" = A+B, "01" = B only, "10" = A only, "00" = neither.
`applyLeagueProb()` uses this 2-char key, which **now matches** the Supabase
`compute_league_combo_buckets()` RPC's 2-char key (it predates C) — so per-league combo
overrides line up again (they fell back to global while the model carried the 3-char Signal-C
key). The coarse per-league *rank* override was removed (PR #60), so the fallback chain is
league-combo → global.

### Clean data — what to trust

The Supabase `match_results` table has three source types:

| fetchedAt | Rows | Usable for signal analysis? |
|-----------|------|----------------------------|
| `historical-import` | ~18k | ❌ Season stats are end-of-season (look-ahead bias) |
| `backfill` | ~11k | ❌ Same — season stats fetched after the fact |
| Timestamp (e.g. `2026-05-03 00:11`) | ~800+ | ✅ Snap frozen pre-game, clean |

**Always filter to live rows** for any backtesting or recalibration:
```sql
WHERE snap->>'fetchedAt' NOT IN ('historical-import', 'backfill')
  AND snap->>'fetchedAt' NOT LIKE '%(from history)%'
```

Clean rows grow by ~10–15/day. ~860 completed matches at last calibration.
Run `/signal-backtest` (or `/calibration`) for the authoritative current `cohortSize`.

### snap structure (live-captured rows)

```json
{
  "fetchedAt": "2026-05-03 00:11",
  "home": { "name": "...", "scored_fh": 0.94, "conced_fh": 0.61, "t1_pct": 27.0, "cn010_avg": 0.09, "sot_avg": 0 },
  "away": { "name": "...", "scored_fh": 0.55, "conced_fh": 0.70, "t1_pct": 13.0, "cn010_avg": 0.12, "sot_avg": 0 },
  "l5": { "home": { "f": 0.8, "a": 0.6, "t": 1.4 }, "away": { "f": 0.6, "a": 0.8, "t": 1.4 } },
  "prematch": { "o15HT": 42, "o05HT": 78, "xgHome": 0.61, "xgAway": 0.44, "btts_fhg": 31 }
}
```

`snap.l5` is **self-reconstructed only**: built from `LEAGUE_MATCHES_CACHE` game history,
present when both teams had ≥ 3 recent cached games. For thin/new seasons (USL2, WPSL on
short calendars) the cache is near-empty, so `snap.l5` stays null and **neither signal can
fire** — the known blind spot.

> **FootyStats does NOT expose last-5 HT team stats.** The team-stats endpoint
> (`/league-teams?include=stats`, ~1065 keys) carries only season-aggregate HT fields
> (`scoredAVGHT_overall/_home/_away`) — there is **no `scoredAVGHT_*_5` field**. A native
> last-5 fallback is therefore impossible; verified via `/debug-raw-api` (`verdict` block).
> The fallback path for thin leagues is `snap.prematch` (below), not team stats.

`snap.prematch` (optional): FootyStats' **own** pre-match first-half predictors, read from
the `/league-matches` endpoint and frozen at capture time. These ARE populated on every
match, including thin-coverage leagues where `l5` is null:
- `o15HT` / `o05HT` — FootyStats' FH-over-1.5 / over-0.5 potential scores (0–100).
- `xgHome` / `xgAway` — pre-match expected goals (full match).
- `btts_fhg` — both-teams-to-score-in-FH potential.

Not yet wired into the signals — captured and being calibrated (`/prematch-mine`) as a
candidate **fallback FH predictor when `snap.l5` is null**. Backfill onto historical rows
with `/admin/backfill-prematch` (these fields persist on completed matches, so they can be
read retroactively and correlated with the FH results we already hold).

> **Calibration status (2026-06, n≈689 with prematch):** `o15HT_potential` predicts FH>1.5
> cleanly and monotonically **on the full cohort** (≥50 → 48% vs 36.4% base, lift 1.32;
> ≥55 → 53.5%). **But it does NOT replicate on the `l5`-null blind-spot subset** (the only
> place it would actually be used): there, ≥50 gives 33% FH>1.5 at n=12 — *below* the
> subset's own 36.7% base. Pre-match `xg` is noise on both (full-match xG ≠ FH). `btts_fhg`
> looks strong on the subset (lift ~1.8) but at n=9. **Do NOT wire any prematch field as a
> live fallback** until the blind-spot subset (`/prematch-mine` → `blindspot`) reaches
> n≳100 and a cutoff holds clearly above that subset's own `baseRate15`. The full-cohort
> result is carried by established leagues and must not be trusted for thin leagues.

`snap.home/away` season stats are always present for live rows. Each carries an
optional **`xt`** sub-object (forward captures only, post-2026-06): extended FootyStats
season fields frozen pre-game — `o15ht`/`o05ht`/`bttsfhg`/`leadHT` (HT percentages),
`xgf`/`xga` (season xG per game), `datk` (dangerous attacks/game), `fhsc`/`fhcn` (FH goals
scored/conceded per game, 0–40'). These **cannot be backfilled** — team season stats are
cumulative, so re-fetching later leaks post-match games (the look-ahead trap). They only
populate going forward; `/season-mine` uses them with a train/test date split.

### USL Leagues — Excluded from the Cohort (unreliable half-time data)

USL leagues (USA — Championship / League One / League Two / W / Super League) are
**always excluded from the analysis cohort.** FootyStats has **no reliable half-time
score** for them: matches come back with the FT correct but the HT recorded as 0-0
(e.g. Appalachian FT 5-2, real HT 3-0, stored HT 0-0). Since `hit_15`/`hit_25` are
derived from the HT total, those are **false misses** that can't be recovered from the
API (the re-fetch keeps returning 0-0). Matched by **name** (`/\bUSL\b/i` on
`league_name`), so it catches every USL tier without hardcoding competition_ids.

Unlike the women's exclusion (opt-in via `?exclude_women=true`), this one is
**unconditional** — applied in every cohort route: `/calibration`, `/signal-backtest`,
`/signalc-validate`, `/floor-test`, `/prematch-mine`, `/season-mine`, `/last5-mine`,
`/corner-mine`, `/rank0-overs`, `/rank0-holdout`, `/mismatch-holdout`, and
`/admin/export-dataset` (via the `isExcludedCohortLeague()` helper). USL matches are
still **served live** like any league — only the model-fitting/analysis cohort drops them.

### Women's Leagues — Excluded from Signal Calibration

The following competition IDs are excluded from clean signal analysis:

| competition_id | League |
|---------------|--------|
| 15020 | Liga MX Femenil |
| 16037 | (women's) |
| 16046 | Arsenal Women / WSL |
| 16563 | Women's internationals |

These leagues **are served live** (predicted with the global table, like any league — the
PR #60 `computePreds` skip was reverted). They are excluded **only from recalibration /
model-fitting**, not from serving: the CSV loader, `/admin/backfill`, `/corner-mine`, and
the `exclude_women` calibration queries all drop them so the men's-calibrated thresholds
aren't biased by a different-distribution population.

**This exclusion is a precaution, not a validated result** — it was never backed by measured
women's base rates/lift. To validate (and decide whether they need their own table), run the
women's-only cohort: `/signal-backtest?women_only=true` and `/calibration?women_only=true`
(mirror of `exclude_women`). Women's leagues often have wider talent gaps → more mismatches,
so they're also a natural test case for **Signal C**. Until that cohort is checked, women's
matches are shown but their probabilities use the men's table.

### Look-ahead bias — lessons learned

- `historical-import` and `backfill` rows used season-aggregate stats fetched **after** the match,
  making all their signal features end-of-season figures. Any analysis on those rows is inflated.
- `ci` and `def_ci` in those rows were computed from old season-stat-based signals (not last-5 form)
  and are equally tainted.
- The current A and B signals read only from `snap.l5` (last-5 FH form), which is frozen
  pre-kickoff for live rows — look-ahead-free. They are NOT valid for backfill/historical-import
  rows (those rows often lack `snap.l5` or carry stale form).
- **Never recalibrate on tainted rows.** Always filter to live captures.

### Pre-game freeze contract — THE invariant (do not break)

This is the most important rule in the codebase: **a prediction shown for a completed
match must be exactly what was frozen before kickoff — never recomputed from current
(post-match) data.** Violating it silently inflates the very hit rates we bet on.

Three pure functions in **`lib/freeze.js`** are the single source of truth, used by the
live engine (`computeSignals`), the snapshot restore (`loadFrozenSnapshots`), and the
`/calibration` + `/history` readouts — kept in one place so these paths can't drift
apart (a past drift wiped the candidate pills on restart):

- `selectPregamePrediction(frozen, result, isComplete)` — a completed match uses the
  frozen snapshot **only**; the live `result` is usable **only while still upcoming**.
  Completed + never-frozen → `null` (shown as no-stats, NOT a post-game recompute).
- `computeOverCandidates(snap, prob15, prob25)` — O1.5/O2.5 candidate flags from
  pre-game inputs only (env_fh, l5_fh, prob); the match result is never an input.
- `isResultFinal(status, isPlayed, ftGoals)` — gates when a match counts as final
  (recordable). `complete` → final; `incomplete` → final **only** if played AND it has a
  posted score (`ftGoals > 0`). A 0-0 `incomplete` match is "score not posted yet", so it
  stays pending — never lock in a placeholder 0-0 / false MISS (it can't be overwritten
  later, since `/admin/backfill-results` only touches `hit_25 IS NULL`). `computePreds`'
  `isComplete` flows through this, so it gates both the result display and persistence.
  Already-poisoned 0-0 rows are re-resolved with `/admin/backfill-results?recheckZeros=1`.

**`scripts/test_freeze.js` (`npm test`) locks these invariants** — run it before any
change near the freeze/snapshot/candidate paths; it fails loudly if look-ahead returns.
When editing candidate-flag or freeze logic, change **`lib/freeze.js`**, never re-inline
the formula at a call site.

### Deprecated / removed signals

> Note: signal **letters were reused** across model generations. The current production
> signals are A = Mutual Instability and B = Away Team Scoring (both last-5 form).
> The rows below are *older, dropped* signals — do not confuse the dropped "B (Attack vs
> Leak)" with today's B.

| Signal (old) | Reason dropped |
|--------------|---------------|
| C — Team Mismatch (`\|net-FH gap\| ≥ 0.5`, season stats) | No live edge on full cohort (lift ~1.0); anti-additive with B (B+C below base rate). Directional split was marginal/rank-0-only — not worth the complexity. Removed 2026-06 → model is A+B only. |
| E — Home Profile (`t1_pct≥25 & scored_fh≥0.94`, season stats) | Replaced by last-5 form model (A+B); the A+E setup was the prior generation |
| Old B — Attack vs Leak (`hF>1.0 & aA>1.0`) | n=19 on clean data, too thin |
| Old C — Both Scoring (`hF>0.8 & aF>0.8`) | Below baseline on clean data (36.4% FH>1.5 vs 38.1% base) |
| Old D — Both Open (`hT>1.5 & aT>1.5`) | n=13 on clean data, too thin |
| Old season-stat signals (CI≥3.2, T1 both≥25%, etc.) | Look-ahead bias inflated results |
| Odds signals (O1, FH O2.5) | Not reliably available pre-kickoff |
| CN010 as core signal | Near-zero additive value on clean data |

## Caching

All caching is in-memory (no external cache store):

| Cache | TTL |
|-------|-----|
| Fixtures (by date) | 30 minutes |
| League matches (by season) | 2 hours |
| Team stats (by season) | 6 hours |

Rate limiting is detected from API response metadata and prevents further
requests until the reset time. Cached data is served while rate-limited.

## Data Capture

Pre-game snaps are frozen and completed results recorded by `persistEarlySnapshots()`
+ `persistCompletedPreds()`, which run on **every `/preds` call** AND on a
**traffic-independent timer** (`selfCapture()`, every 20 min — see `SELF_CAPTURE_TTL`).
The timer exists because `computePreds` only covers a 5-day forward window: a match
drops out the day after it completes, so without it a low-traffic day would leave the
result permanently unrecorded (a "pending" row: snap frozen, `hit_25 IS NULL`). The
keep-alive cron keeps the instance warm so the timer fires.

Rows that went pending before self-capture existed can be recovered with
`GET /admin/backfill-results` (reads final HT/FT scores from league-matches and writes
only the result fields — snap/signals/rank untouched). Check `/supabase-status.selfCapture`
for the last timer run.

## Routes

| Route | Response | Purpose |
|-------|----------|---------|
| `GET /` | HTML | Full page with inline CSS/JS, loads predictions async |
| `GET /preds?tz=<offset>` | JSON | Prediction data for 5-day window |
| `GET /cache-status` | JSON | Cache sizes and rate-limit status |
| `GET /debug` | JSON | Fixtures, league registry, cache state |
| `GET /calibration` | JSON | Live predicted-vs-actual by rank/combo (Supabase). `?exclude_women=true` / `?women_only=true` / `?competition_id=N` |
| `GET /signal-backtest` | JSON | Per-signal live lift + `byRank`/`byCombo` (Supabase). `?exclude_women=true` / `?women_only=true` / `?competition_id=N` |
| `GET /prematch-mine` | JSON | Calibrate FootyStats `snap.prematch` predictors (`o15HT` etc.) vs actual FH results (Supabase) |
| `GET /season-mine` | JSON | Mine frozen season stats (`snap.home/away` + `xt.*`) as FH signals with a train/test date holdout (Supabase) |
| `GET /signalc-validate` | JSON | Recalibrate the 3-signal (A+B+C) combo table: 8-combo probs, train/test stability, C's marginal lift within each A+B combo (Supabase) |
| `GET /mismatch-holdout` | JSON | Out-of-sample test for Signal C (team mismatch): direction buckets + rank-0 holdout (Supabase) |
| `GET /rank0-overs` / `GET /rank0-holdout` | JSON | Characterise rank-0 false-negatives / holdout-test the home-attack lead (Supabase) |
| `GET /corner-mine` | JSON | Correlate pre-game last-5 corners with over-2.5 (Supabase) |
| `GET /history?days=N` | JSON | Recent completed matches with results (Supabase) |
| `GET /admin/export-dataset` | CSV/JSON | Flat one-row-per-match export of the clean live cohort (all pre-game features + l5 + prematch + signals/combo/rank + results) for offline analysis — gated; `?format=json` |
| `GET /supabase-status` | JSON | Supabase connection + persistence + self-capture status |
| `GET /debug-raw-api?sid=N` | JSON | Raw FootyStats fields for a season + `verdict` on pre-match predictor availability |
| `GET /admin/backfill-results` | JSON | Resolve pending rows (snap frozen, no result) from league-matches scores — gated; `dryRun=1` / `limit=N`. `recheckZeros=1` re-resolves rows already stored as 0-0 (premature-completion false misses) against current scores — only corrects rows whose real score is now non-zero; also covers no-stats (null-snap) rows so History is correct, and falls back to a direct `/match?match_id=` fetch for rows outside the league-matches window |
| `GET /admin/backfill-prematch` | JSON | Merge `snap.prematch` predictors onto live rows from league-matches — gated; additive, never touches l5/signals; `dryRun=1` / `limit=N` |
| `GET /admin/ht-audit` | JSON | Find leagues with the USL-style half-time gap (HT near-zero while FT scores are healthy) — gated; per-league `avgHt`/`avgFt`/`htFtRatio`, `suspect` flag, `newSuspects` = not yet excluded. `?minN=N` |
| `GET /api/*` | JSON | Passthrough proxy — gated by `LOAD_DATASET_TOKEN` (fails closed if unset) |
| `GET /admin/*` | JSON | Dataset load / backfill / recalibrate — gated by `LOAD_DATASET_TOKEN` (fails closed if unset) |

## Conventions and Patterns

- **No TypeScript, no ESLint, no Prettier** — plain JavaScript with no enforced style
- **HTML is built via string concatenation** in `buildHTML()` — no template engine
- **Section comments** use `// ─── SECTION ─────` divider format
- **Fix comments** labeled `FIX 1` through `FIX 4` document known workarounds
- **Previous-season fallback**: `PREV_SEASON` map provides fallback season IDs
  when current season has < 5 completed matches
- **Backfill is safe**: both `/admin/backfill` and `/admin/load-dataset` use
  `ignoreDuplicates: true` — running them will never overwrite a live-captured snap.
  `/admin/backfill-results` only UPDATEs result fields (`ht_*`/`ft_*`/`fh_total`/`hit_*`)
  on rows where `hit_25 IS NULL`, so it never touches a frozen snap/signals/rank either.

## Dataset

A CSV dataset (`dataset_combined_filled.csv`) was built and maintained separately
for backtesting. **Do not use it for signal calibration** — the season stats are
end-of-season figures (look-ahead bias). It is a research artifact only.

## Planned: Self-Learning System

The goal is to make the server recalibrate probability tables and signal weights
from its own completed match history, without requiring manual dataset updates.

### Phase 1 — Rolling probability recalibration (in-memory)
- On each `/preds` call, scan `SERVER_MATCH_CACHE` for completed matches
- Recompute `PROB25_BY_COMBO` and `PROB15_BY_COMBO` from recent results
- Apply a minimum sample guardrail (n ≥ 30 per rank) before overriding defaults
- Resets on Render restart — useful for session-level adaptation

### Phase 2 — League-specific probability tables (persisted)
- Maintain separate prob tables per league/competition_id
- Persist to Supabase (`league_prob_tables`)
- Update nightly from accumulated match results

### Phase 3 — Weighted signal scoring per league
- Replace integer rank count with a weighted float score
- Weights learned per league from match history
- Matches ranked by score rather than signal count
- Requires Phase 2 persistence infrastructure

**Constraint:** Render free tier has no disk persistence between restarts.
All learning that needs to survive restarts must use Supabase.

## Environment

- **No Docker** — deployed directly as a Node.js app on Render
- **Supabase** — `SUPABASE_URL` and `SUPABASE_ANON_KEY` required for persistence
- **GitHub Actions** — single workflow for keep-alive pings to prevent Render free-tier spin-down
- **Auto-deploy** — Render deploys automatically on push to main branch
- **`LOAD_DATASET_TOKEN`** — optional; gates admin endpoints. Fails closed (503) if not set.

## Important Notes for AI Assistants

- All logic is in `server.js` — there are no other source files
- The frontend is fully inline (HTML/CSS/JS inside `buildHTML()`) — changes
  to UI require editing string literals in server.js
- `node-fetch` v2 is used (CommonJS `require()` compatible) — do not upgrade
  to v3 without converting to ESM
- The `FOOTY_API_KEY` is sensitive — never commit it or log it in full
- The app handles rate limiting gracefully by serving cached data — preserve
  this behavior in all modifications
- `PREV_SEASON` mappings need manual updates when new seasons start
- **Signals A and B use last-5 FH form** read from `snap.l5`. When `snap.l5` is absent
  (thin leagues), neither can fire → combo `00`, rank 0. `computeSignals(snap, hLast5, aLast5)`
  reads `snap.l5` directly. (Signal C, which read season stats and could fire when `l5` was
  null, was removed — see the Signals section and Deprecated.)
- **`snap.l5` is self-reconstructed only** (from `LEAGUE_MATCHES_CACHE`). There is **no
  native FootyStats last-5 HT stat** to fall back on — `scoredAVGHT_*_5` fields do not
  exist in the team-stats API (verified via `/debug-raw-api` verdict). When the cache is
  thin (USL2/WPSL short seasons) `l5` is null and the signals cannot fire.
- **`snap.prematch`** captures FootyStats' own pre-match FH predictors (`o15HT`, `o05HT`,
  `xgHome`, `xgAway`, `btts_fhg`) at freeze time — the ONLY pre-game FH signal available
  for thin leagues. Not yet a live signal; being calibrated via `/prematch-mine` as a
  fallback for when `l5` is null. Backfill historical rows with `/admin/backfill-prematch`
  (additive — merges only a `prematch` sub-object into snap, never touches l5/home/away).
- **Signal C (Team Mismatch) was removed (2026-06)** — no live edge on the full cohort and
  anti-additive with B (see the Signals section). The production model is A + B only. Don't
  re-wire C without fresh out-of-sample validation that the *directional* version (home-stronger
  helps / away-stronger fades) clears base rate on a held-out split; the absolute-gap version
  is dead. `/signalc-validate` and `/mismatch-holdout` still compute it from `snap` for that check.
- **Do NOT reintroduce the old level-based Signal E** (`t1_pct`/`scored_fh` levels), the old
  "Both Scoring"/"Both Open"/"Attack vs Leak" signals, or Signal D — all dropped on clean data
  for thin samples / below-baseline lift. See "Deprecated / removed signals".
- **Do not add odds-based signals** — odds data is not reliably available pre-kickoff
- **Do not reintroduce CN010 as a core signal** — near-zero additive value on clean data
- **Women's leagues (15020, 16037, 16046, 16563) must be excluded** from any
  threshold recalibration or model retraining
- **Match ordering is probability-first**: `prob25 → prob15 → ci → rank` (in `computePreds`
  and the client day / best-bets sorts). The per-combo calibrated probability is the source
  of truth. On the A+B table rank and probability agree (monotonic), so `ci`/`rank` are just
  tiebreakers.
- `computeSignals()` returns `ci` (combined intensity = `homeL5Total + awayL5Total`) and
  `defCi` (away last-5 FH total) — keep them in the return object, they feed display and the
  sort tiebreak
- The `eligible` flag (**rank ≥ 2**) controls star badges on league pills in the UI.
  `applyLeagueProb()` no longer overrides it with a `prob25 >= 40` rule (that was always
  false on the global table, max ~20% — it silently killed the badge). `eligible25` /
  `eligible15` remain as informational prob-tier flags only; nothing gates on them
  (`betPill` is signal-based).
- The 🔥/🎯 badges (`betPill`) are **signal-based** (🔥 = A+B both fire, 🎯 = B fires),
  not prob-based.
- `snap.l5` carries the last-5 inputs signals A and B use — preserve it in the snapshot path.
  `snap.home`/`snap.away` `scored_fh`/`conced_fh` are still captured (they feed the O1.5/O2.5
  candidate `env_fh`), just no longer used by a core signal.
- **combo string is 2 chars** (`bit(A) + bit(B)`) — matches the Supabase combo RPC's 2-char
  key. `comboFromSignals()` and `applyLeagueProb()` use it. (Reverted from the 3-char Signal-C
  key when C was removed — see "combo string format".)
- **To realign stored rows with the current model**, use `/admin/recompute-signals`
  (runs the real `computeSignals()`). Run it after deploying the C-removal so stored combos,
  ranks, and probs match the A+B model (old rows carry 3-char combos / C-based ranks).
- **Backfill guard**: both admin upsert routes use `ignoreDuplicates: true` — do not remove this
