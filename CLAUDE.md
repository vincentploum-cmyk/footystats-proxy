# CLAUDE.md — footystats-proxy

## Project Overview

A Node.js/Express proxy server that predicts first-half (FH) goals in football matches. It fetches data from the football-data-api.com API, applies a 2-signal pre-game ranking algorithm, and serves predictions via a server-rendered HTML frontend with async JSON loading.

**Live:** https://footystats-proxy.onrender.com | **Hosted on:** Render.com

## Architecture

**Single-file monolith:** All application logic lives in `server.js` (~870 lines). No build step, no transpilation, no framework — vanilla JS throughout.

```
server.js          — Express server, API proxy, prediction engine, HTML frontend
package.json       — Dependencies and start script
.github/workflows/ — Keep-alive cron ping (every 20 min)
```

### Key Subsystems in server.js

| Section | Purpose |
|---------|---------|
| **safeFetch / rate limiting** | Wraps API calls with timeout, caching, and rate-limit detection |
| **In-memory caches** | `FIXTURE_CACHE`, `LEAGUE_MATCHES_CACHE`, `TEAM_STATS_CACHE`, `SERVER_MATCH_CACHE` with TTLs |
| **Prediction engine** | `buildLast5()` → `last5Form()` → `computeSignals()` pipeline; 2 signals (A + E). `extractStats()` builds the season-stat `snap` frozen pre-game. |
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

Matches are ranked 0–2 based on how many signals fire. Rank = signals fired (max 2).
🔥 Fire (2) → 📢 Signal (1) → Low (0). `eligible` = rank ≥ 2.

### Signals

| Signal | Name | Rule | Data source |
|--------|------|------|-------------|
| A | Recent Intensity | `hT + aT >= 4.0` | Last-5 FH form (pre-kickoff) |
| E | Home Profile | `snap.home.t1_pct >= 25` AND `snap.home.scored_fh >= 0.94` | Season stats frozen pre-game |

**Signal A** requires ≥ 3 recent games per team (`last5Form()` returns null otherwise → rank 0 for A).
`hT = hF + hA` (home last-5 FH total), `aT = aF + aA` (away last-5 FH total).

**Signal E** uses season-aggregate stats frozen at pre-game snap time:
- `snap.home.t1_pct` = `seasonOver25PercentageHT` (home team's historical % of matches with FH > 2.5)
- `snap.home.scored_fh` = `scoredAVGHT` (home team's season avg FH goals scored)

These are **not** look-ahead biased for live-captured rows — the snap is frozen on first write
(`ignoreDuplicates: true`) before the match kicks off.

`computeSignals(snap, hLast5, aLast5)` returns:
- `ci` = recent intensity (`hT + aT`) — used for display/sorting
- `defCi` = away last-5 FH conceded (`aA`)
- `signals` = `{ A: { met, label, value, threshold }, E: { met, label, value, threshold } }`

### Probability Tables

Calibrated on **601 clean live-captured matches** (look-ahead-free, snap frozen pre-game).
Combo-specific probabilities — A-only and E-only differ significantly at rank 1:

| Combo | Rank | n | FH > 1.5 | FH > 2.5 |
|-------|------|---|----------|----------|
| A + E | 2 🔥 | 26 | 57.7% | 26.9% |
| E only | 1 | 23 | 52.2% | 17.4% |
| A only | 1 | 47 | 44.7% | 10.6% |
| Neither | 0 | 505 | 35.8% | 10.1% |

Global fallback rank table (used when league-specific data is insufficient):

| Rank | prob15 | prob25 |
|------|--------|--------|
| 2 | 57.7% | 26.9% |
| 1 | 47.2% | 12.8% (weighted avg) |
| 0 | 35.8% | 10.1% |

Recalibrate by re-running the threshold analysis SQL on `match_results` filtered to
live-captured rows (`fetchedAt NOT IN ('historical-import', 'backfill')`).

### combo string format

`comboFromSignals()` returns a **2-char string**: bit(A) + bit(E).
e.g. "11" = both fire, "10" = A only, "01" = E only, "00" = neither.
The Supabase `compute_league_combo_buckets()` function has been updated to match.

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

Clean rows grow by ~10–15/day. At ~600 completed matches as of May 2026.

### snap structure (live-captured rows)

```json
{
  "fetchedAt": "2026-05-03 00:11",
  "home": { "name": "...", "scored_fh": 0.94, "conced_fh": 0.61, "t1_pct": 27.0, "cn010_avg": 0.09, "sot_avg": 0 },
  "away": { "name": "...", "scored_fh": 0.55, "conced_fh": 0.70, "t1_pct": 13.0, "cn010_avg": 0.12, "sot_avg": 0 },
  "l5": { "home": { "f": 0.8, "a": 0.6, "t": 1.4 }, "away": { "f": 0.6, "a": 0.8, "t": 1.4 } }
}
```

`snap.l5` is non-null only when both teams had ≥ 3 recent games at capture time.
`snap.home/away` season stats are always present for live rows.

### Women's Leagues — Excluded from Signal Calibration

The following competition IDs are excluded from clean signal analysis:

| competition_id | League |
|---------------|--------|
| 15020 | Liga MX Femenil |
| 16037 | (women's) |
| 16046 | Arsenal Women / WSL |
| 16563 | Women's internationals |

These leagues still appear in the UI but must not be used for threshold recalibration.

### Look-ahead bias — lessons learned

- `historical-import` and `backfill` rows used season-aggregate stats fetched **after** the match,
  making all their signal features end-of-season figures. Any analysis on those rows is inflated.
- `ci` and `def_ci` in those rows were computed from old season-stat-based signals (not last-5 form)
  and are equally tainted.
- Signal E uses `t1_pct` and `scored_fh` — these ARE valid pre-game for live-captured rows because
  the snap is frozen before kickoff. They are NOT valid for backfill/historical-import rows.
- **Never recalibrate on tainted rows.** Always filter to live captures.

### Deprecated / removed signals

| Signal | Reason dropped |
|--------|---------------|
| B — Attack vs Leak (`hF>1.0 & aA>1.0`) | n=19 on clean data, too thin |
| C — Both Scoring (`hF>0.8 & aF>0.8`) | Below baseline on clean data (36.4% FH>1.5 vs 38.1% base) |
| D — Both Open (`hT>1.5 & aT>1.5`) | n=13 on clean data, too thin |
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

## Routes

| Route | Response | Purpose |
|-------|----------|---------|
| `GET /` | HTML | Full page with inline CSS/JS, loads predictions async |
| `GET /preds?tz=<offset>` | JSON | Prediction data for 5-day window |
| `GET /cache-status` | JSON | Cache sizes and rate-limit status |
| `GET /debug` | JSON | Fixtures, league registry, cache state |
| `GET /calibration` | JSON | Live predicted-vs-actual by rank/combo (Supabase) |
| `GET /signal-backtest` | JSON | Per-signal live lift + `byRank`/`byCombo` (Supabase) |
| `GET /history?days=N` | JSON | Recent completed matches with results (Supabase) |
| `GET /supabase-status` | JSON | Supabase connection + persistence status |
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
  `ignoreDuplicates: true` — running them will never overwrite a live-captured snap

## Dataset

A CSV dataset (`dataset_combined_filled.csv`) was built and maintained separately
for backtesting. **Do not use it for signal calibration** — the season stats are
end-of-season figures (look-ahead bias). It is a research artifact only.

## Planned: Self-Learning System

The goal is to make the server recalibrate probability tables and signal weights
from its own completed match history, without requiring manual dataset updates.

### Phase 1 — Rolling probability recalibration (in-memory)
- On each `/preds` call, scan `SERVER_MATCH_CACHE` for completed matches
- Recompute `PROB25_BY_RANK` and `PROB15_BY_RANK` from recent results
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
- **Signal E uses season stats from the snap** (`snap.home.t1_pct` and `snap.home.scored_fh`),
  NOT from last-5 form. This is intentional and look-ahead-free for live rows.
- **Signal A uses last-5 recent form** — `computeSignals` takes `(snap, hLast5, aLast5)`.
  When hLast5/aLast5 are unavailable, Signal A cannot fire (returns false), rank stays 0 for A.
- **Do NOT reintroduce signals B, C, or D** — they were dropped after calibration on 601 clean
  matches showed B/D had n<20 and C was below baseline. See "Deprecated / removed signals" above.
- **Do not add odds-based signals** — odds data is not reliably available pre-kickoff
- **Do not reintroduce CN010 as a core signal** — near-zero additive value on clean data
- **Women's leagues (15020, 16037, 16046, 16563) must be excluded** from any
  threshold recalibration or model retraining
- `computeSignals()` returns `ci` (recent intensity = `hT + aT`) and `defCi`
  (away last-5 FH conceded) — keep them in the return object, they drive display/sorting
- The `eligible` flag (rank ≥ 2) controls star badges on league pills in the UI
- The 🔥/🎯 FILTER badges (`betPill`) are **UI heuristics** (prob25≥20 or prob15≥50),
  not the calibrated rank — don't conflate them
- `snap.l5` carries the last-5 inputs Signal A used — preserve it in the snapshot path
- **combo string is 2 chars** (`bit(A) + bit(E)`) — do not revert to 4-char ABCD format
- **Backfill guard**: both admin upsert routes use `ignoreDuplicates: true` — do not remove this
