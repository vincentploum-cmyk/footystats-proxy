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
| **Prediction engine** | `buildLast5()` → `last5Form()` → `computeSignals()` pipeline; 2 signals (A + B), both last-5 FH form. `extractStats()` builds the season-stat `snap` frozen pre-game. |
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

Both signals are **last-5 FH form** signals, read from `snap.l5` (pre-kickoff). They
are NOT season-stat signals — see "Deprecated / removed signals" for the old A+E model.

| Signal | Name | Rule | Data source |
|--------|------|------|-------------|
| A | Mutual Instability | `snap.l5.home.t >= 1.6` AND `snap.l5.away.t >= 1.4` | Last-5 FH form (pre-kickoff) |
| B | Away Team Scoring | `snap.l5.away.f >= 0.8` | Last-5 FH form (pre-kickoff) |

`snap.l5.home.t` / `snap.l5.away.t` are each team's last-5 FH **total** (scored + conceded
per game); `snap.l5.away.f` is the away team's last-5 FH **scored** average.

**Both signals require `snap.l5`** (i.e. both teams had ≥ 3 recent games at capture time —
`last5Form()` returns null otherwise). When `snap.l5` is absent, `hasL5 = false`, neither
signal fires, and rank stays 0.

`computeSignals(snap, hLast5, aLast5)` returns:
- `rank` = signals fired (0–2), `label` = Low / Signal / Fire
- `prob15`, `prob25` = global rank-table probabilities (see below)
- `ci` = combined intensity (`homeL5Total + awayL5Total`) — used for display/sorting
- `defCi` = away last-5 FH total (`awayL5Total`)
- `eligible` = rank ≥ 2
- `signals` = `{ A: { met, label, value, threshold }, B: { met, label, value, threshold } }`

### Probability Tables

Calibrated on **732 clean resolved matches** (women excluded, look-ahead-free).
Tables are **combo-keyed** (`PROB15_BY_COMBO` / `PROB25_BY_COMBO` in `server.js`):

| Combo | n | prob15 (FH>1.5) | prob25 (FH>2.5) |
|-------|---|-----------------|-----------------|
| 11 🔥 A+B | 72 | 47.2% | **20.8%** (2.2× baseline) |
| 01 — B only | 225 | **44.9%** | 11.6% |
| 10 — A only | 31 | 29.0% | 9.7% |
| 00 — neither | 404 | 32.9% | 9.4% |

Key findings from calibration (732 matches, women excluded):
- **A alone is at/below baseline on both metrics** (9.7% FH>2.5, 29.0% FH>1.5 vs 32.9% baseline) — zero standalone value.
- **B alone is a strong FH>1.5 predictor** (44.9% vs 32.9% baseline, n=225) but barely moves FH>2.5 (11.6%).
- **A+B is the FH>2.5 signal** (20.8% vs 9.4% baseline = 2.2× lift, n=72).
- The betPill already reflects this: 🔥 only when A+B fire; 🎯 whenever B fires.

Recalibrate by re-running the three combo/rank SQL queries against `match_results` filtered to
live-captured rows. The `/signal-backtest` endpoint reports `cohortSize`, per-signal lift, and
per-rank `gap25` (actual − predicted); a persistently negative `gap25` at rank 2 is the trigger.

### combo string format

`comboFromSignals()` returns a **2-char string**: bit(A) + bit(B).
e.g. "11" = both fire, "10" = A only, "01" = B only, "00" = neither.
The Supabase `compute_league_combo_buckets()` RPC and `applyLeagueProb()` lookup both
use this 2-char key.

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
- The current A and B signals read only from `snap.l5` (last-5 FH form), which is frozen
  pre-kickoff for live rows — look-ahead-free. They are NOT valid for backfill/historical-import
  rows (those rows often lack `snap.l5` or carry stale form).
- **Never recalibrate on tainted rows.** Always filter to live captures.

### Deprecated / removed signals

> Note: signal **letters were reused** across model generations. The current production
> signals are A = Mutual Instability and B = Away Team Scoring (both last-5 form).
> The rows below are *older, dropped* signals — do not confuse the dropped "B (Attack vs
> Leak)" with today's B.

| Signal (old) | Reason dropped |
|--------------|---------------|
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
- **Both signals (A and B) use last-5 FH form** read from `snap.l5`, NOT season stats.
  `computeSignals(snap, hLast5, aLast5)` reads `snap.l5` directly; when `snap.l5` is
  absent neither signal fires and rank stays 0. (Season stats like `t1_pct`/`scored_fh`
  are still captured in `snap.home`/`snap.away` but are no longer used by the signals.)
- **Do NOT reintroduce the season-stat Signal E** (`t1_pct`/`scored_fh`) as a core signal —
  it was the prior-generation A+E model, replaced by the last-5 A+B model.
- **Do NOT reintroduce signals C or D, or the old "Attack vs Leak" signal** — dropped on
  clean data for thin samples / below-baseline lift. See "Deprecated / removed signals".
- **Do not add odds-based signals** — odds data is not reliably available pre-kickoff
- **Do not reintroduce CN010 as a core signal** — near-zero additive value on clean data
- **Women's leagues (15020, 16037, 16046, 16563) must be excluded** from any
  threshold recalibration or model retraining
- `computeSignals()` returns `ci` (combined intensity = `homeL5Total + awayL5Total`) and
  `defCi` (away last-5 FH total) — keep them in the return object, they drive display/sorting
- The `eligible` flag (rank ≥ 2) controls star badges on league pills in the UI
- The 🔥/🎯 FILTER badges (`betPill`) are **UI heuristics** (prob25≥20 or prob15≥50),
  not the calibrated rank — don't conflate them
- `snap.l5` carries the last-5 inputs signals A and B use — preserve it in the snapshot path
- **combo string is 2 chars** (`bit(A) + bit(B)`) — do not revert to 3-/4-char formats.
  `comboFromSignals()`, `applyLeagueProb()`, and the Supabase combo RPC all use it.
- **To realign stored rows with the current model**, use `/admin/recompute-signals`
  (runs the real `computeSignals()`). The old `/admin/backfill-signal-d` route was
  removed — it wrote a phantom Signal C and a 3-signal rank.
- **Backfill guard**: both admin upsert routes use `ignoreDuplicates: true` — do not remove this
