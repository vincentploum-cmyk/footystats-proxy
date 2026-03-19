require("dotenv").config();

const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;
const KEY = process.env.FOOTY_API_KEY;
const BASE = "https://api.football-data-api.com";

if (!KEY) {
  console.error("Missing FOOTY_API_KEY");
  process.exit(1);
}

let LEAGUE_NAMES = {};
let SERVER_MATCH_CACHE = {};
let TEAM_CACHE = {};
let FROZEN_PREMATCH = {};

let LAST_CACHE_BUILD = 0;

// --------------------
// FETCH LEAGUES
// --------------------
async function fetchLeagueList() {
  const url = `${BASE}/league-list?key=${KEY}`;
  const data = await fetch(url).then(r => r.json());

  const map = {};
  (data.data || []).forEach(l => {
    map[l.id] = normalizeLeagueName(l.name);
  });

  LEAGUE_NAMES = map;

  console.log("League-list:", Object.keys(map).length, "leagues found");
}

// --------------------
// NORMALIZE LEAGUE NAME
// --------------------
function normalizeLeagueName(name = "") {
  return name.replace("Europe · ", "").trim();
}

// --------------------
// CACHE MATCHES (LOW CALL FREQUENCY)
// --------------------
async function rebuildServerMatchCache() {
  const now = Date.now();
  if (now - LAST_CACHE_BUILD < 10 * 60 * 1000) return;

  LAST_CACHE_BUILD = now;

  try {
    const url = `${BASE}/todays-matches?key=${KEY}`;
    const data = await fetch(url).then(r => r.json());

    const cache = {};

    (data.data || []).forEach(match => {
      if (!LEAGUE_NAMES[match.competition_id]) return;

      const key = `${match.home_name}_${match.away_name}`;
      cache[key] = match;
    });

    SERVER_MATCH_CACHE = cache;

    console.log("Match cache:", Object.keys(cache).length);

  } catch (e) {
    console.error("Cache rebuild failed:", e.message);
  }
}

// --------------------
// TEAM CACHE (REDUCE API CALLS)
// --------------------
async function getTeamMatches(teamId) {
  if (TEAM_CACHE[teamId]) return TEAM_CACHE[teamId];

  const url = `${BASE}/team-matches?key=${KEY}&team_id=${teamId}`;
  const data = await fetch(url).then(r => r.json());

  let matches = data.data || [];

  matches = matches.filter(m => LEAGUE_NAMES[m.competition_id]);
  matches = dedupeMatches(matches);

  TEAM_CACHE[teamId] = matches;

  return matches;
}

// --------------------
// DEDUPE MATCHES
// --------------------
function dedupeMatches(matches = []) {
  const seen = new Set();

  return matches.filter(m => {
    const key = `${m.date}_${m.opponent}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

// --------------------
// LAST 5 MATCHES
// --------------------
async function getRecentMatches(teamId, excludeMatchId = null) {
  const matches = await getTeamMatches(teamId);

  return matches
    .filter(m => m.id !== excludeMatchId)
    .slice(0, 5);
}

// --------------------
// H2H (FROM SAME CACHE)
// --------------------
function getH2H(homeId, awayId) {
  const all = Object.values(TEAM_CACHE).flat();

  return dedupeMatches(
    all.filter(m =>
      (m.homeID === homeId && m.awayID === awayId) ||
      (m.homeID === awayId && m.awayID === homeId)
    )
  ).slice(0, 5);
}

// --------------------
// SIMPLE FH SCORE (1–5)
// --------------------
function calculateScore(home, away) {
  let score = 0;

  if (home.fh_scored > 1.2) score++;
  if (away.fh_scored > 1.0) score++;
  if (home.fh_conceded > 0.8) score++;
  if (away.fh_conceded > 0.8) score++;
  if (home.early_goals > 0.2 || away.early_goals > 0.2) score++;

  return Math.min(5, score);
}

// --------------------
// API
// --------------------
app.get("/api/match/:home/:away", async (req, res) => {
  try {
    const { home, away } = req.params;

    const key = `${home}_${away}`;
    const match = SERVER_MATCH_CACHE[key];

    if (!match) {
      return res.json({ error: "Match not found" });
    }

    const matchId = match.id;

    // 🔒 Freeze logic
    if (FROZEN_PREMATCH[matchId]) {
      return res.json(FROZEN_PREMATCH[matchId]);
    }

    const homeMatches = await getTeamMatches(match.homeID);
    const awayMatches = await getTeamMatches(match.awayID);

    const homeRecent = await getRecentMatches(match.homeID, matchId);
    const awayRecent = await getRecentMatches(match.awayID, matchId);

    const h2h = getH2H(match.homeID, match.awayID);

    const score = calculateScore(match.home_stats || {}, match.away_stats || {});

    const result = {
      match,
      score,
      homeRecent,
      awayRecent,
      h2h
    };

    // 🔒 freeze AFTER kickoff
    if (match.status === "complete") {
      FROZEN_PREMATCH[matchId] = result;
    }

    res.json(result);

  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// --------------------
// START SERVER
// --------------------
app.listen(PORT, async () => {
  console.log("Server running on port", PORT);

  await fetchLeagueList();

  if (Object.keys(LEAGUE_NAMES).length > 0) {
    await rebuildServerMatchCache();

    setInterval(rebuildServerMatchCache, 10 * 60 * 1000);
    console.log("Cache running every 10 min");
  }
});
