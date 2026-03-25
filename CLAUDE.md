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
