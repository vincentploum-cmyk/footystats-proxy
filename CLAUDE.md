# CLAUDE.md — footystats-proxy

## Project Overview

A Node.js/Express proxy server that predicts first-half (FH) goals in football matches. It fetches data from the football-data-api.com API, applies a 4-signal recent-form ranking algorithm (each team's last-5 first-half form), and serves predictions via a server-rendered HTML frontend with async JSON loading.

**Live:** https://footystats-proxy.onrender.com | **Hosted on:** Render.com

## Architecture

**Single-file monolith:** All application logic lives in `server.js` (~860 lines). No build step, no transpilation, no framework — vanilla JS throughout.

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
| **Prediction engine** | `buildLast5()` → `last5Form()` → `computeSignals()` pipeline; 4 recent-form signals. (`extractStats()` still builds the season-stat `snap` for display) |
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

## Prediction Algorithm (4-Signal Ranking)

Matches are ranked 0–4 based on how many signals fire. **All four signals derive
from each team's last-5 first-half form** — the average first-half goals scored
and conceded over their most recent (≤5) played matches. These are reconstructed
at prediction time via `buildLast5()` and consumed by `computeSignals()`. Recent
form is available pre-kickoff, so the signals carry **no season-aggregate
look-ahead bias** (see "Look-ahead bias" below).

Notation: `hF`/`hA` = home team last-5 FH scored/conceded; `aF`/`aA` = away team
last-5 FH scored/conceded; `hT = hF + hA`, `aT = aF + aA` (per-team FH totals).

### Signals

| Signal | Name | Rule |
|--------|------|------|
| A | Recent Intensity | `hT + aT >= 4.0` |
| B | Attack vs Leak | `hF > 1.0` AND `aA > 1.0` (home attacking, away leaking) |
| C | Both Scoring | `hF > 0.8` AND `aF > 0.8` |
| D | Both Open | `hT > 1.5` AND `aT > 1.5` |

A team needs **≥ 3 recent games** for signals to evaluate; otherwise the match is
rank 0 (`last5Form()` returns null). `computeSignals(snap, hLast5, aLast5)` also
returns `ci` = recent intensity (`hT + aT`) for display/sorting, and `defCi` =
away last-5 FH conceded (`aA`).

Rank = count of signals fired (0–4):
🔥 Fire (4) → ⚡ Prime (3) → 👀 Watch (2) → 📢 Signal (1) → Low (0).

### Probability Tables (calibrated on 24,677 look-ahead-free pre-game matches, base rate 11.2% FH > 2.5)

| Rank | n (matches) | FH > 2.5 | FH > 1.5 |
|------|-------------|----------|----------|
| 4    | 53          | 30.2%    | 50.9%    |
| 3    | 337         | 17.5%    | 42.7%    |
| 2    | 679         | 16.8%    | 40.4%    |
| 1    | 1,501       | 13.9%    | 36.4%    |
| 0    | 22,107      | 10.7%    | 29.8%    |

Regenerate with `python3 scripts/recalibrate_pregame.py`. Rank 4 has a small
sample (n=53) — treat with caution. Rank 3+ is "eligible" (star badges in UI).
Note the honest scale: even rank 3 (~17.5%) sits near the break-even for typical
FH-over-2.5 market odds, so probabilities are modest, not the inflated 60%+ the
old season-stat tables claimed.

### Per-combination performance

Rank just counts signals; specific combos diverge at the same rank. On the clean
data, **B alone (rank 1) ≈ 20% beats every rank-3 combo**, while **C is nearly
dead weight** (~12% alone, ~base rate) and tends to drag combos down. Use
`GET /signal-backtest` (`byCombo` field) to monitor this on live data; consider a
weighted/combo score rather than a flat count if the pattern holds.

### Recent-form snapshot (`snap.l5`)

Each frozen snapshot stores the last-5 inputs the signals used:
`snap.l5 = { home: {f, a, t}, away: {f, a, t} }`. This lets the recent-form
signals be re-validated and re-tuned on live data — earlier snapshots stored only
season stats, which made the new signals impossible to backtest live.

### Women's Leagues — Excluded from Signal Calibration

The following competition IDs are excluded from clean signal analysis. They have
different FH goal dynamics and degrade model accuracy:

| competition_id | League |
|---------------|--------|
| 15020 | Liga MX Femenil |
| 16037 | (women's) |
| 16046 | Arsenal Women / WSL |
| 16563 | Women's internationals |

These leagues still appear in the UI but their signal hit rates should not be
used to recalibrate thresholds.

### Look-ahead bias (why the engine was rewritten)

The original signals used **full-season** aggregate stats
(`seasonOver25PercentageHT`, `scoredAVGHT`, ...). In `dataset_combined_filled.csv`
those are end-of-season figures, so the original backtest was inflated — signal B
appeared to be 4.71x lift, signal A 2.77x, etc. Validating against the live
`match_results` (which froze as-of-kickoff stats) and against a look-ahead-free
reconstruction both showed the true lifts were only ~1.2–1.4x, and the rank table
was flat/inverted. The signals were rewritten on **last-5 recent form**, which is
genuinely available pre-kickoff and produces a monotonic gradient. The lesson:
**only trust backtests on as-of-kickoff data** — reconstruct pre-game stats with
`scripts/recalibrate_pregame.py` or validate on live `match_results`, never on the
raw season-aggregate dataset.

### Deprecated / removed

- **Old season-stat signals** (CI≥3.2, T1 both≥25%, DefCI≥2.25, away scored≥1.25)
  — removed; collapsed to ~1.3x lift once look-ahead bias was stripped.
- **Odds signals** (O1, FH O2.5) — not reliably available at prediction time.
- **CN010 (early goals conceded)** — near-zero additive value.
- **"sig C required for rank 3+" gate** — removed; it gated eligibility on the
  weakest signal.

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
| `GET /api/*` | JSON | Passthrough proxy — **gated by `LOAD_DATASET_TOKEN`**, routes through `safeFetch` |
| `GET /admin/*` | JSON | Dataset load / backfill / recalibrate — gated by `LOAD_DATASET_TOKEN` (fails closed) |

## Conventions and Patterns

- **No TypeScript, no ESLint, no Prettier** — plain JavaScript with no enforced style
- **HTML is built via string concatenation** in `buildHTML()` — no template engine
- **Section comments** use `// ─── SECTION ─────` divider format
- **Fix comments** labeled `FIX 1` through `FIX 4` document known workarounds
- **Previous-season fallback**: `PREV_SEASON` map provides fallback season IDs
  when current season has < 5 completed matches
- **Commit messages** have historically been brief ("Update server.js")

## Dataset

A CSV dataset (`dataset_combined_filled.csv`) was built and maintained separately
for backtesting. Key facts:

- 27,443 total rows across 101 competition IDs and multiple seasons
- 24,203 complete matches; 23,593 (97.5%) have signal stats filled
- 610 rows permanently unfillable (comp IDs 15238, 14904 — no team stats available)

⚠️ **Look-ahead caveat:** the per-row team stats columns (`*scoredAVGHT*`,
`*seasonOver25PercentageHT*`, etc.) are **full-season aggregates**, not as-of-
kickoff values. Backtesting signals directly on those columns inflates results.
`scripts/recalibrate_pregame.py` rebuilds each team's pre-game last-5 FH form by
walking matches chronologically (date-sorted, prior games only) — that is the
look-ahead-free view the current signals are calibrated on (~24,677 usable rows).

The dataset is not used at runtime — it is a research artifact only.

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
- Persist to an external store (Supabase free tier recommended)
- Update nightly from accumulated match results
- Env var needed: `SUPABASE_URL`, `SUPABASE_ANON_KEY`

### Phase 3 — Weighted signal scoring per league
- Replace integer rank count with a weighted float score
- Weights learned per league from match history
- Matches ranked by score rather than signal count
- Requires Phase 2 persistence infrastructure

**Constraint:** Render free tier has no disk persistence between restarts.
All learning that needs to survive restarts must use an external DB.
Supabase free tier (500MB, no credit card) is sufficient for this use case.

## Environment

- **No Docker** — deployed directly as a Node.js app on Render
- **No database** — purely API-driven with in-memory state (Supabase planned)
- **GitHub Actions** — single workflow for keep-alive pings to prevent
  Render free-tier spin-down
- **Auto-deploy** — Render deploys automatically on push to main branch

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
- **Signals derive from last-5 recent form, not season stats** — `computeSignals`
  takes `(snap, hLast5, aLast5)`. Do NOT revert to season-aggregate signals
  (`seasonOver25PercentageHT`, `scoredAVGHT`); they only looked strong because of
  look-ahead bias in the dataset. See "Look-ahead bias" above.
- **Never recalibrate or validate signals on the raw `dataset_combined_filled.csv`
  season stats** — they're end-of-season figures (look-ahead bias). Use
  `scripts/recalibrate_pregame.py` (reconstructs pre-game state) or live
  `match_results` via `/signal-backtest`.
- **Do not add odds-based signals** — odds data is not reliably available at
  prediction time from the FootyStats API
- **Do not reintroduce CN010 as a core signal** — near-zero additive value
- **Women's leagues (15020, 16037, 16046, 16563) must be excluded** from any
  threshold recalibration or model retraining — their FH dynamics differ
  significantly from men's football
- `computeSignals()` returns `ci` (recent intensity = `hT + aT`) and `defCi`
  (away last-5 FH conceded); both are persisted and used for display/sorting —
  keep them in the return object.
- The `eligible` flag (rank ≥ 3) controls star badges on league pills in the UI
- Rank 4 probability is ~30.2% FH>2.5 but based on only n=53 matches — always
  note this caveat if surfacing the number to users
- The 🔥/🎯 FILTER badges (`betPill`) are **UI heuristics**, not the calibrated
  engine — don't conflate them with rank/probability
- `snap.l5` carries the last-5 inputs the signals used — preserve it when changing
  the snapshot/persistence path so live re-validation stays possible
