# CLAUDE.md — footystats-proxy

## Project Overview

This is a Node.js/Express proxy server for the FootyStats football data API. It caches and serves league, match, and team statistics data with built-in rate limiting and caching layers.

## Tech Stack

- **Runtime:** Node.js with Express
- **Entry point:** `server.js`
- **Dependencies:** express, cors, dotenv, node-fetch (v2)
- **API source:** football-data-api.com
- **Config:** `.env` file with `FOOTY_API_KEY` and optional `PORT`

## Commands

- `npm start` — runs the server (`node server.js`)
- Server defaults to port 3001

## Architecture Notes

- Single-file server (`server.js`) handling all routes and caching
- In-memory caches for fixtures, league matches, team stats, and server matches
- Rate limiting protection against upstream API limits
- League name mapping loaded at startup from the league-list endpoint

## Business Ideation Framework

When asked to brainstorm or develop business ideas, apply these principles:

### Core Philosophy
Think like a solo founder or small team that needs revenue within 30-90 days. Prioritize ideas that are:
1. **Simple to build** — can be prototyped in days, not months
2. **Clear monetization** — the path from user to revenue is obvious and short
3. **Low operational overhead** — minimal ongoing costs, no complex infrastructure
4. **Leveraging existing data/APIs** — build on top of what already exists (like this project does with FootyStats)

### Ideation Process
When generating ideas, follow this structured approach:

1. **Spot the inefficiency** — What manual, repetitive, or painful process exists today that people already pay to solve (or would pay to avoid)?
2. **Find the wedge** — What is the smallest possible version that delivers real value? Not an app, not a platform — maybe just a spreadsheet, a bot, a single API endpoint, or a daily email.
3. **Validate the revenue model** — Pick one: subscription, one-time purchase, usage-based, affiliate, or advertising. If you can't name the model in one word, simplify.
4. **Estimate the math** — How many customers at what price point to hit $1K/month? $10K/month? If the numbers require millions of users, rethink.
5. **Identify the unfair advantage** — What makes this hard to copy? Unique data, niche expertise, network effects, or speed-to-market.

### Idea Evaluation Criteria
Rate each idea on these dimensions (1-5 scale):
- **Time to MVP:** How fast can a working version ship? (5 = weekend, 1 = months)
- **Revenue clarity:** How obvious is the monetization? (5 = people already pay for this, 1 = "we'll figure it out")
- **Market pull:** Are people actively searching for this solution? (5 = clear demand, 1 = needs education)
- **Defensibility:** Can this build a moat over time? (5 = strong network effects/data moat, 1 = trivially cloneable)
- **Solo-friendly:** Can one person run this? (5 = fully automated, 1 = needs a team)

### Sports Data Specific Opportunities
Since this project already works with football/soccer data, consider these angles:
- **Prediction/analytics tools** for bettors, fantasy players, or scouts
- **Automated content generation** (match previews, stat roundups, social media posts)
- **Alert/notification services** (odds movements, lineup changes, stat milestones)
- **Data visualization dashboards** for niche leagues or specific stat categories
- **API reselling** with value-added transformations, caching, or bundling
- **Telegram/Discord bots** serving stats to communities willing to pay for premium tiers

### Output Format for Ideas
When presenting business ideas, structure each one as:
```
**Idea:** [One-line description]
**Problem:** [What pain point does this solve?]
**Solution:** [The simplest version that works]
**Revenue model:** [How it makes money]
**MVP scope:** [What to build first — be brutally minimal]
**Math:** [X customers * $Y/month = $Z MRR]
**First 10 customers:** [Specific channels to find them]
**Risk:** [The #1 reason this could fail]
```

### Innovation Triggers
To generate breakthrough yet practical ideas, combine these lenses:
- **Automation:** What if this task required zero human effort?
- **Unbundling:** What feature of a big product could be its own business?
- **Rebundling:** What 3 free tools could be combined into one paid product?
- **Niche down:** What if this only served [specific small audience] extremely well?
- **Time arbitrage:** What information is valuable in the first 5 minutes but worthless after an hour?
- **Format shift:** What if this data was delivered as [audio/SMS/email/Slack message] instead?
