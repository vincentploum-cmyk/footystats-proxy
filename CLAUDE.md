# CLAUDE.md — footystats-proxy

## Project Overview

A Node.js/Express proxy server that predicts first-half (FH) goals in football matches. It fetches data from the football-data-api.com API, applies a 4-signal ranking algorithm, and serves predictions via a server-rendered HTML frontend with async JSON loading.

**Live:** https://footystats-proxy.onrender.com | **Hosted on:** Render.com

## Architecture

**Single-file monolith:** All application logic lives in `server.js` (~850 lines). No build step, no transpilation, no framework — vanilla JS throughout.

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

Matches are ranked 0–4 based on how many signals fire:

| Signal | Name | Threshold |
|--------|------|-----------|
| A | Combined Intensity (CI) | `home_scored_fh + away_scored_fh + home_conceded_fh + away_conceded_fh ≥ 3.20` |
| B | Both teams FH history | Both teams ≥ 25% over 2.5 FH |
| C | Leaky Defences (DefCI) | `home_conceded_fh + away_conceded_fh ≥ 2.25` |
| D | Away FH Attack | Away team FH attack avg ≥ 1.25 |

Rank determines probability labels: 🔥 Fire (4) → ⚡ Prime (3) → 👀 Watch (2) → 📢 Signal (1) → Low (0).

Backtested on 22,967 matches (12.8% base rate for FH > 2.5).

## Caching

All caching is in-memory (no external cache store):

| Cache | TTL |
|-------|-----|
| Fixtures (by date) | 30 minutes |
| League matches (by season) | 2 hours |
| Team stats (by season) | 6 hours |

Rate limiting is detected from API response metadata and prevents further requests until the reset time.

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
- **Previous-season fallback**: `PREV_SEASON` map provides fallback season IDs when current season has < 5 matches
- **Commit messages** have historically been brief ("Update server.js")

## Environment

- **No Docker** — deployed directly as a Node.js app on Render
- **No database** — purely API-driven with in-memory state
- **GitHub Actions** — single workflow for keep-alive pings to prevent Render free-tier spin-down

## Important Notes for AI Assistants

- All logic is in `server.js` — there are no other source files
- The frontend is fully inline (HTML/CSS/JS inside `buildHTML()`) — changes to UI require editing string literals in server.js
- `node-fetch` v2 is used (CommonJS `require()` compatible) — do not upgrade to v3 without converting to ESM
- The `FOOTY_API_KEY` is sensitive — never commit it or log it in full
- The app handles rate limiting gracefully by serving cached data — preserve this behavior
- `PREV_SEASON` mappings need manual updates when new seasons start
