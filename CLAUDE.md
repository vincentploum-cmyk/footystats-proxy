# CLAUDE.md — footystats-proxy

## Project Overview

A Node.js/Express proxy server that predicts first-half (FH) goals in football matches. It fetches data from the football-data-api.com API, applies a 4-signal ranking algorithm, and serves predictions via a server-rendered HTML frontend with async JSON loading.

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
| **Prediction engine** | `extractStats()` → `computeSignals()` pipeline with 4-signal ranking |
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

Matches are ranked 0–4 based on how many signals fire. All signals are computed
from team stats only — **no odds data is used**. Stats come from the
`/league-teams?season_id=X&include=stats` endpoint at prediction time.

### Signals

| Signal | Name | Field(s) | Threshold |
|--------|------|----------|-----------|
| A | Combined Intensity (CI) | `home_scored_fh + away_scored_fh + home_conceded_fh + away_conceded_fh` | ≥ 3.20 |
| B | FH History Both | Both teams `seasonOver25PercentageHT` (role-specific) | both ≥ 25% |
| C | Leaky Defences (DefCI) | `home_conceded_fh + away_conceded_fh` | ≥ 2.25 |
| D | Away FH Attack | Away team `scoredAVGHT_away` | ≥ 1.25 |

Rank = count of signals fired (0–4). Rank determines probability labels:
🔥 Fire (4) → ⚡ Prime (3) → 👀 Watch (2) → 📢 Signal (1) → Low (0).

### Probability Tables (backtested on 22,967 matches, base rate 12.8% FH > 2.5)

| Rank | n (matches) | FH > 2.5 | FH > 1.5 |
|------|-------------|----------|----------|
| 4    | 24          | 87.5%    | 100.0%   |
| 3    | 142         | 62.0%    | 75.4%    |
| 2    | 710         | 40.3%    | 66.8%    |
| 1    | 1,644       | 29.6%    | 61.1%    |
| 0    | 20,447      | 10.0%    | 31.4%    |

Rank 4 has a small sample (n=24) — treat with caution. Rank 3+ is considered
"eligible" and shown with star badges in the UI.

### Signal Field Mapping in extractStats()

`extractStats(teamObj, role)` returns:

| Field | Source stat | Role suffix |
|-------|------------|-------------|
| `scored_fh` | `scoredAVGHT` | `_home` or `_away` |
| `conced_fh` | `concededAVGHT` | `_home` or `_away` |
| `t1_pct` | `seasonOver25PercentageHT` | `_home` or `_away` |
| `cn010_avg` | `goals_conceded_min_0_to_10` / `mpRole` | `_home` or `_away` |
| `sot_avg` | `shotsOnTarget` / `mpRole` | `_home` or `_away` (informational only) |

Role-specific stats are preferred when `mpRole >= 3`, otherwise falls back to
`_overall`. `cn010_avg` is kept for display purposes but is **not a signal** —
it was removed from scoring after analysis showed near-zero additive value.

### Women's Leagues — Excluded from Signal Computation

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

### Signal Research Summary

Signals were selected from a backtested dataset of 27,443 matches across 101
competition IDs. Key findings:

- **T1 (signal B)** is the strongest individual signal: 4.71x lift at ≥25% threshold
- **CI (signal A)** is 2.77x lift — reliable but broad
- **DefCI (signal C)** is independently strong at 2.85x lift — leaky defences
  predict FH goals as powerfully as high-scoring attacks
- **Away scored (signal D)** is 2.43x lift — away teams that score heavily in
  FH force open games
- **Odds signals** (O1, FH O2.5) were explored but excluded — not reliably
  available at prediction time from the FootyStats API
- **CN010 (early goals conceded)** was dropped — near-zero additive value when
  CI + T1 are already present

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
| `GET /api/*` | JSON | Passthrough proxy to football-data-api.com |

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
- Clean analysis set (CI>0, no women's): **22,967 rows**
- Signals were researched and validated against this dataset before being
  implemented in server.js

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
- **Do not add odds-based signals** — odds data is not reliably available at
  prediction time from the FootyStats API
- **Do not reintroduce CN010 as a core signal** — it was deliberately removed
  after analysis showed it adds no value when CI + T1 are present
- **Women's leagues (15020, 16037, 16046, 16563) must be excluded** from any
  threshold recalibration or model retraining — their FH dynamics differ
  significantly from men's football
- When modifying `computeSignals()`, always preserve the `defCi` field in the
  return object — the UI uses it directly for the DefCI display row
- The `eligible` flag (rank ≥ 3) controls star badges on league pills in the UI
- Rank 4 probability of 87.5% FH>2.5 is real but based on only n=24 matches —
  always note this caveat if surfacing the number to users
