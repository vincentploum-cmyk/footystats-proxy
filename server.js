require("dotenv").config();

const express = require("express");
const cors    = require("cors");
const fetch   = require("node-fetch");

const app  = express();
app.use(cors());

const KEY  = process.env.FOOTY_API_KEY;
const BASE = "https://api.football-data-api.com";
const PORT = process.env.PORT || 3001;

if (!KEY) { console.error("Missing FOOTY_API_KEY"); process.exit(1); }

// ─── LEAGUE REGISTRY ─────────────────────────────────────────────────────────
let LEAGUE_NAMES = {};

async function fetchLeagueList() {
  try {
    const data = await safeFetch(BASE + "/league-list?key=" + KEY);
    if (!data) { console.warn("fetchLeagueList skipped — rate limited"); return; }
    const list = data.data || [];
    console.log("League-list: " + list.length + " leagues found");
    const map = {};
    for (const league of list) {
      const leagueName = league.league_name || league.name || "";
      const country    = league.country || "";
      const name       = country ? country + " \u00b7 " + leagueName : leagueName;
      if (!name) continue;
      const seasons = league.season || [];
      for (const s of seasons) { if (s.id) map[parseInt(s.id, 10)] = name; }
    }
    LEAGUE_NAMES = map;
    console.log("Mapped " + Object.keys(map).length + " season IDs");
    if (list.length === 0) {
      console.log("Got 0 leagues — retrying in 2 min...");
      setTimeout(fetchLeagueList, 2 * 60 * 1000);
    }
  } catch(e) {
    console.error("Failed to load league list: " + e.message);
    LEAGUE_NAMES = {};
  }
}

// ─── CACHE LAYER ─────────────────────────────────────────────────────────────
// All expensive API calls are cached server-side.
// Only re-fetched when TTL expires — never on every page load.
//
//  FIXTURE_CACHE        : /todays-matches per date     TTL = 10 min
//  LEAGUE_MATCHES_CACHE : /league-matches per sid      TTL = 30 min
//  TEAM_STATS_CACHE     : /league-teams per sid        TTL = 60 min
//  SERVER_MATCH_CACHE   : team → completed matches     built from LEAGUE_MATCHES_CACHE

const FIXTURE_CACHE        = {};   // date → { data, ts }
const LEAGUE_MATCHES_CACHE = {};   // sid  → { data, ts }
const TEAM_STATS_CACHE     = {};   // sid  → { data, ts }
let   SERVER_MATCH_CACHE   = {};   // teamId → [slim matches]
let   RATE_LIMITED_UNTIL   = 0;    // epoch ms — back off when 429 received

const TTL_FIXTURES = 10 * 60 * 1000;   // 10 minutes
const TTL_MATCHES  = 30 * 60 * 1000;   // 30 minutes
const TTL_TEAMS    = 60 * 60 * 1000;   // 60 minutes

// Safe fetch — detects rate limit, returns null if hit
async function safeFetch(url) {
  if (Date.now() < RATE_LIMITED_UNTIL) {
    console.warn("Rate limited — skipping: " + url);
    return null;
  }
  try {
    const data = await fetch(url).then(r => r.json());
    if (data && data.error && String(data.error).toLowerCase().includes("rate limit")) {
      const reset = data.metadata && data.metadata.request_limit_refresh_next
        ? data.metadata.request_limit_refresh_next * 1000
        : Date.now() + 60 * 60 * 1000;
      RATE_LIMITED_UNTIL = reset;
      const mins = Math.ceil((reset - Date.now()) / 60000);
      console.warn("Rate limit hit — backing off for " + mins + " min");
      return null;
    }
    return data;
  } catch(e) {
    console.error("Fetch error: " + e.message);
    return null;
  }
}

// Cached fixture fetch
async function fetchFixtures(date) {
  const now = Date.now();
  if (FIXTURE_CACHE[date] && (now - FIXTURE_CACHE[date].ts) < TTL_FIXTURES) {
    return FIXTURE_CACHE[date].data;
  }
  const data = await safeFetch(BASE + "/todays-matches?date=" + date + "&key=" + KEY);
  if (data) FIXTURE_CACHE[date] = { data, ts: now };
  return data || FIXTURE_CACHE[date]?.data || { data: [] };
}

// Cached league matches fetch
async function fetchLeagueMatches(sid) {
  const now = Date.now();
  if (LEAGUE_MATCHES_CACHE[sid] && (now - LEAGUE_MATCHES_CACHE[sid].ts) < TTL_MATCHES) {
    return LEAGUE_MATCHES_CACHE[sid].data;
  }
  const data = await safeFetch(BASE + "/league-matches?season_id=" + sid + "&max_per_page=150&page=1&sort=date_unix&order=desc&key=" + KEY);
  if (data) LEAGUE_MATCHES_CACHE[sid] = { data, ts: now };
  return data || LEAGUE_MATCHES_CACHE[sid]?.data || { data: [] };
}

// Cached team stats fetch
async function fetchTeamStats(sid) {
  const now = Date.now();
  if (TEAM_STATS_CACHE[sid] && (now - TEAM_STATS_CACHE[sid].ts) < TTL_TEAMS) {
    return TEAM_STATS_CACHE[sid].data;
  }
  const data = await safeFetch(BASE + "/league-teams?season_id=" + sid + "&include=stats&key=" + KEY);
  if (data) TEAM_STATS_CACHE[sid] = { data, ts: now };
  return data || TEAM_STATS_CACHE[sid]?.data || { data: [] };
}

// Rebuild the server match cache from already-cached league matches
function rebuildServerMatchCache() {
  const newCache = {};
  let total = 0;
  for (const [sid, entry] of Object.entries(LEAGUE_MATCHES_CACHE)) {
    const leagueName = LEAGUE_NAMES[parseInt(sid, 10)] || "League " + sid;
    for (const m of (entry.data.data || []).filter(m => m.status === "complete")) {
      const slim = {
        homeID: m.homeID, awayID: m.awayID,
        home_name: m.home_name || "", away_name: m.away_name || "",
        date_unix: m.date_unix || 0,
        ht_goals_team_a: parseInt(m.ht_goals_team_a || 0, 10),
        ht_goals_team_b: parseInt(m.ht_goals_team_b || 0, 10),
        homeGoalCount:   parseInt(m.homeGoalCount   || 0, 10),
        awayGoalCount:   parseInt(m.awayGoalCount   || 0, 10),
        status: m.status, league: leagueName,
      };
      if (m.homeID) { if (!newCache[m.homeID]) newCache[m.homeID] = []; newCache[m.homeID].push(slim); }
      if (m.awayID) { if (!newCache[m.awayID]) newCache[m.awayID] = []; newCache[m.awayID].push(slim); }
      total++;
    }
  }
  SERVER_MATCH_CACHE = newCache;
  console.log("Server match cache rebuilt: " + Object.keys(newCache).length + " teams, " + total + " records");
}

const PREV_SEASON = {
  16504:13973, 16544:11321, 16571:15746,
  16614:14086, 16615:14116, 16036:13703,
};

// ─── HELPERS ─────────────────────────────────────────────────────────────────
const ftch = url => fetch(url).then(r => r.json());
const safe = v   => (isNaN(v) || !isFinite(v)) ? 0 : Number(v);

function getDates(tzOffset) {
  const now   = new Date();
  const local = new Date(now.getTime() + tzOffset * 60 * 1000);
  const fmt   = d => {
    const y   = d.getUTCFullYear();
    const m   = String(d.getUTCMonth() + 1).padStart(2, "0");
    const day = String(d.getUTCDate()).padStart(2, "0");
    return y + "-" + m + "-" + day;
  };
  const dates = [];
  for (let i = 0; i < 5; i++) {
    const d = new Date(local);
    d.setUTCDate(local.getUTCDate() + i);
    dates.push(fmt(d));
  }
  return [...new Set(dates)];
}

function unixToLocalDate(unix, tzOffset) {
  const d   = new Date((unix * 1000) + tzOffset * 60 * 1000);
  const y   = d.getUTCFullYear();
  const m   = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return y + "-" + m + "-" + day;
}

// ─── SIGNAL ENGINE ────────────────────────────────────────────────────────────
// 4 independent signals — each worth 1 point. Rank = total points fired (0–4).
//
//   A  CI >= 3.2
//      Combined FH intensity: h_scored_fh + a_scored_fh + h_conced_fh + a_conced_fh
//      Measures raw goal volume potential for both teams.
//
//   B  T1: both teams seasonOver25PercentageHT >= 20%
//      Both teams have historically produced FH>2.5 in ≥20% of their games.
//      Mutual history signal — must fire for both sides.
//
//   C  CN010: either team concedes ≥0.25 goals/game in 0–10 min
//      At least one team gets hit early, breaking defensive shape fast.
//
//   D  Either team FH scored avg (role) >= 1.5
//      Home team scores ≥1.5 FH goals/game at home, OR
//      Away team scores ≥1.5 FH goals/game away. Elite attacking threat.
//
// Backtested on 24,203 complete matches (base rate 12.6%):
//   Rank 4 (4/4): 73.2% FH>2.5 | 85.4% FH>1.5  (n=82)
//   Rank 3 (3/4): 44.7% FH>2.5 | 69.6% FH>1.5  (n=293)
//   Rank 2 (2/4): 29.6% FH>2.5 | 61.2% FH>1.5  (n=531)
//   Rank 1 (1/4): 12.8% FH>2.5 | 38.2% FH>1.5  (n=1549)
//   Rank 0 (0/4): 11.5% FH>2.5 | 32.9% FH>1.5

// Probability lookup by rank
const PROB25_BY_RANK = { 4: 73.2, 3: 44.7, 2: 29.6, 1: 12.8, 0: 11.5 };
const PROB15_BY_RANK = { 4: 85.4, 3: 69.6, 2: 61.2, 1: 38.2, 0: 32.9 };

const RANK_LABELS = { 4: "Fire", 3: "Prime", 2: "Watch", 1: "Signal", 0: "Low" };

function computeSignals(snap) {
  const h = snap.home;
  const a = snap.away;

  // CI = sum of all 4 role-specific FH scoring averages
  const ci = safe(h.scored_fh + a.scored_fh + h.conced_fh + a.conced_fh);

  const sigA = ci >= 3.2;
  const sigB = h.t1_pct >= 20 && a.t1_pct >= 20;
  const sigC = h.cn010_avg >= 0.25 || a.cn010_avg >= 0.25;
  const sigD = h.scored_fh >= 1.5 || a.scored_fh >= 1.5;

  const rank  = [sigA, sigB, sigC, sigD].filter(Boolean).length;
  const label = RANK_LABELS[rank] || "Low";

  const prob25 = PROB25_BY_RANK[rank] ?? 11.5;
  const prob15 = PROB15_BY_RANK[rank] ?? 32.9;

  return {
    rank, label,
    prob25, prob15,
    ci:      +ci.toFixed(2),
    eligible: rank >= 3,
    signals: {
      A: { met: sigA, label: "Combined Intensity",    value: ci.toFixed(2),                                               threshold: ">= 3.20" },
      B: { met: sigB, label: "Both Teams FH History", value: h.t1_pct.toFixed(0) + "%/" + a.t1_pct.toFixed(0) + "%",    threshold: "both >= 20%" },
      C: { met: sigC, label: "Early Goals",           value: h.cn010_avg.toFixed(2) + "/" + a.cn010_avg.toFixed(2),      threshold: "either >= 0.25" },
      D: { met: sigD, label: "Elite FH Scorer",       value: h.scored_fh.toFixed(2) + "/" + a.scored_fh.toFixed(2),     threshold: "either >= 1.50" },
    },
  };
}

// ─── STAT EXTRACTOR ──────────────────────────────────────────────────────────
function extractStats(teamObj, role) {
  const s   = teamObj.stats || {};
  const sfx = role === "home" ? "_home" : "_away";
  const mpR = safe(s["seasonMatchesPlayed" + sfx]) || 1;

  const pick = (rk, fk) => {
    const rv = s[rk];
    if (rv !== null && rv !== undefined && mpR >= 3) return rv;
    const fv = s[fk];
    if (fv !== null && fv !== undefined) return fv;
    return 0;
  };

  return {
    name:      teamObj.name || teamObj.cleanName || "",
    scored_fh: safe(pick("scoredAVGHT"   + sfx, "scoredAVGHT_overall")),
    conced_fh: safe(pick("concededAVGHT" + sfx, "concededAVGHT_overall")),
    t1_pct:    safe(pick("seasonOver25PercentageHT" + sfx, "seasonOver25PercentageHT_overall")),
    cn010_avg: safe((s["goals_conceded_min_0_to_10" + sfx] || 0) / mpR),
    mp:        safe(s.seasonMatchesPlayed_overall || 0),
    mpRole:    mpR,
  };
}

// ─── BUILD LAST 5 ─────────────────────────────────────────────────────────────
// Only includes matches within the last 35 days (5 weeks) of today.
// This prevents stale form from months-old matches showing when cache is sparse.
const LAST5_WINDOW_SECS = 35 * 24 * 60 * 60; // 35 days in seconds

function buildLast5(teamId, cache) {
  if (!teamId || !cache[teamId]) return [];
  const cutoff = Math.floor(Date.now() / 1000) - LAST5_WINDOW_SECS;
  // Deduplicate: same match appears twice in cache (once as home, once as away).
  const seen = new Set();
  const unique = cache[teamId].filter(m => {
    if ((m.date_unix || 0) < cutoff) return false;          // outside 5-week window
    const key = (m.date_unix || 0) + "_" + (m.homeID || "") + "_" + (m.awayID || "");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  return unique
    .sort((a, b) => (b.date_unix || 0) - (a.date_unix || 0))
    .slice(0, 5)
    .map(m => {
      const isHome = m.homeID === teamId;
      const ftFor  = isHome ? m.homeGoalCount   : m.awayGoalCount;
      const ftAgst = isHome ? m.awayGoalCount   : m.homeGoalCount;
      const fhFor  = isHome ? m.ht_goals_team_a : m.ht_goals_team_b;
      const fhAgst = isHome ? m.ht_goals_team_b : m.ht_goals_team_a;
      const result = ftFor > ftAgst ? "W" : ftFor < ftAgst ? "L" : "D";
      const date   = m.date_unix ? new Date(m.date_unix * 1000).toISOString().slice(0, 10) : "";
      return { date, venue: isHome ? "H" : "A", opp: isHome ? m.away_name : m.home_name,
               competition: m.league || "", fhFor, fhAgst, ftFor, ftAgst, result };
    });
}

// ─── CACHE STATUS + DEBUG ENDPOINTS ─────────────────────────────────────────
app.get("/cache-status", (req, res) => {
  const now = Date.now();
  const fixtureEntries = Object.entries(FIXTURE_CACHE).map(([date, e]) => ({
    date, ageMin: Math.round((now - e.ts) / 60000), expiresInMin: Math.round((TTL_FIXTURES - (now - e.ts)) / 60000),
    matchCount: (e.data.data || []).length,
  }));
  const matchEntries = Object.entries(LEAGUE_MATCHES_CACHE).map(([sid, e]) => ({
    sid, league: LEAGUE_NAMES[parseInt(sid)] || "?",
    ageMin: Math.round((now - e.ts) / 60000), expiresInMin: Math.round((TTL_MATCHES - (now - e.ts)) / 60000),
    matchCount: (e.data.data || []).length,
  }));
  const teamEntries = Object.entries(TEAM_STATS_CACHE).map(([sid, e]) => ({
    sid, league: LEAGUE_NAMES[parseInt(sid)] || "?",
    ageMin: Math.round((now - e.ts) / 60000), expiresInMin: Math.round((TTL_TEAMS - (now - e.ts)) / 60000),
    teamCount: (e.data.data || []).length,
  }));
  res.json({
    rateLimitedUntil: RATE_LIMITED_UNTIL > now ? new Date(RATE_LIMITED_UNTIL).toISOString() : "not limited",
    rateLimitedMinRemaining: RATE_LIMITED_UNTIL > now ? Math.ceil((RATE_LIMITED_UNTIL - now) / 60000) : 0,
    leagueRegistry: Object.keys(LEAGUE_NAMES).length + " seasons mapped",
    serverMatchCache: Object.keys(SERVER_MATCH_CACHE).length + " teams",
    fixtureCacheEntries: fixtureEntries,
    leagueMatchesCacheEntries: matchEntries.length,
    teamStatsCacheEntries: teamEntries.length,
    leagueMatchesCache: matchEntries,
    teamStatsCache: teamEntries,
  });
});

app.get("/debug", async (req, res) => {
  try {
    const tzOffset    = parseInt(req.query.tz || "0", 10);
    const dates       = getDates(tzOffset);
    const leagueCount = Object.keys(LEAGUE_NAMES).length;
    const raw         = await fetchFixtures(dates[0]);
    const fixtures    = raw.data || [];
    const passing     = fixtures.filter(m => {
      const sid = parseInt(m.competition_id, 10);
      return !leagueCount || LEAGUE_NAMES[sid];
    });
    const cids      = [...new Set(fixtures.map(m => m.competition_id))].slice(0, 20);
    const knownSids = Object.keys(LEAGUE_NAMES).slice(0, 20);
    res.json({
      date: dates[0],
      rateLimited: Date.now() < RATE_LIMITED_UNTIL,
      rateLimitedUntil: RATE_LIMITED_UNTIL > Date.now() ? new Date(RATE_LIMITED_UNTIL).toISOString() : null,
      leagueRegistrySize: leagueCount,
      totalFixtures: fixtures.length,
      passingFilter: passing.length,
      sampleCompetitionIds: cids,
      sampleKnownSeasonIds: knownSids,
      cachedTeams: Object.keys(SERVER_MATCH_CACHE).length,
      fixtureCacheAge: FIXTURE_CACHE[dates[0]] ? Math.round((Date.now() - FIXTURE_CACHE[dates[0]].ts) / 60000) + " min" : "not cached",
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── API PASSTHROUGH ─────────────────────────────────────────────────────────
app.get("/api/*", async (req, res) => {
  try {
    const path = req.path.replace("/api", "");
    const qs   = new URLSearchParams({ ...req.query, key: KEY }).toString();
    res.json(await ftch(BASE + path + "?" + qs));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── MAIN ROUTE ──────────────────────────────────────────────────────────────
app.get("/", async (req, res) => {
  try {
    const tzOffset  = parseInt(req.query.tz || "0", 10);
    const dates     = getDates(tzOffset);
    const fetchedAt = new Date().toISOString().slice(0, 16).replace("T", " ");

    // Fetch fixtures for 5 days — cached, max 1 API call per date per 10 min
    const allFixtures = [];
    for (const d of dates) {
      const r = await fetchFixtures(d);
      for (const m of (r.data || [])) {
        allFixtures.push(Object.assign({}, m, { _date: d }));
      }
    }

    // Group by league — only include leagues present in LEAGUE_NAMES.
    // If LEAGUE_NAMES is still empty (startup race), show nothing rather than
    // processing every fixture from every league we have no stats for.
    const leagueFixtures = {};
    for (const m of allFixtures) {
      const sid = parseInt(m.competition_id, 10);
      if (LEAGUE_NAMES[sid]) {
        if (!leagueFixtures[sid]) leagueFixtures[sid] = [];
        leagueFixtures[sid].push(m);
      }
    }

    // Per-request local cache — extends SERVER_MATCH_CACHE with matches
    // fetched this request that may not be in the server cache yet.
    const localExtra    = {};
    const serverCacheKeys = new Set();
    for (const matches of Object.values(SERVER_MATCH_CACHE)) {
      for (const m of matches) {
        serverCacheKeys.add((m.date_unix||0) + "_" + (m.homeID||"") + "_" + (m.awayID||""));
      }
    }

    const slimM = (m, lg) => ({
      homeID: m.homeID, awayID: m.awayID,
      home_name: m.home_name || "", away_name: m.away_name || "",
      date_unix: m.date_unix || 0,
      ht_goals_team_a: parseInt(m.ht_goals_team_a || 0, 10),
      ht_goals_team_b: parseInt(m.ht_goals_team_b || 0, 10),
      homeGoalCount: parseInt(m.homeGoalCount || 0, 10),
      awayGoalCount: parseInt(m.awayGoalCount || 0, 10),
      status: m.status, league: lg || "",
    });

    const nowSecs = Math.floor(Date.now() / 1000);
    const addToLocalExtra = (matches, lg) => {
      for (const m of matches) {
        // Accept 'complete' matches, and also 'incomplete' matches that were
        // clearly played: kick-off was in the past AND FT score is non-zero.
        // Some providers (e.g. FootyStats for Women's Liga MX) never flip the
        // status to 'complete' even after the match has been played.
        const isPlayed = m.status === "complete" ||
          (m.status === "incomplete" &&
           (m.date_unix || 0) < nowSecs &&
           (parseInt(m.homeGoalCount || 0, 10) + parseInt(m.awayGoalCount || 0, 10)) > 0);
        if (!isPlayed) continue;
        const key = (m.date_unix||0) + "_" + (m.homeID||"") + "_" + (m.awayID||"");
        if (serverCacheKeys.has(key)) continue;
        const slim = slimM(m, lg);
        if (m.homeID) { if (!localExtra[m.homeID]) localExtra[m.homeID] = []; localExtra[m.homeID].push(slim); }
        if (m.awayID) { if (!localExtra[m.awayID]) localExtra[m.awayID] = []; localExtra[m.awayID].push(slim); }
        serverCacheKeys.add(key);
      }
    };

    const preds = [];

    for (const sid of Object.keys(leagueFixtures)) {
      const fixtures   = leagueFixtures[sid];
      if (!fixtures.length) continue;
      const leagueName = LEAGUE_NAMES[parseInt(sid, 10)] || "League " + sid;

      // Completed matches — cached, max 1 API call per league per 30 min
      let completed = [];
      try {
        const p1 = await fetchLeagueMatches(sid);
        completed = (p1.data || []).filter(m => m.status === "complete");
        addToLocalExtra(completed, leagueName);
        if (completed.length < 5 && PREV_SEASON[sid]) {
          const prev  = await fetchLeagueMatches(PREV_SEASON[sid]);
          const prevC = (prev.data || []).filter(m => m.status === "complete");
          addToLocalExtra(prevC, leagueName);
          completed = [...completed, ...prevC];
        }
      } catch(e) { console.error("[" + sid + "] match fetch: " + e.message); }

      // Team stats — cached, max 1 API call per league per 60 min
      let teamMap = {};
      try {
        const tr = await fetchTeamStats(sid);
        for (const t of (tr.data || [])) teamMap[t.id] = t;
        if (!Object.keys(teamMap).length && PREV_SEASON[sid]) {
          const tr2 = await fetchTeamStats(PREV_SEASON[sid]);
          for (const t of (tr2.data || [])) teamMap[t.id] = t;
        }
      } catch(e) { console.error("[" + sid + "] team fetch: " + e.message); }

      for (const fix of fixtures) {
        const homeId   = fix.homeID || fix.home_id;
        const awayId   = fix.awayID || fix.away_id;
        const homeTeam = teamMap[homeId];
        const awayTeam = teamMap[awayId];
        const missing  = !homeTeam || !awayTeam;

        const matchDate = fix.date_unix
          ? unixToLocalDate(fix.date_unix, tzOffset)
          : fix._date;

        let snap   = null;
        let result = null;

        if (!missing) {
          const hStats = extractStats(homeTeam, "home");
          const aStats = extractStats(awayTeam, "away");
          snap   = { fetchedAt, home: hStats, away: aStats };
          result = computeSignals(snap);
        }

        // Merge server cache + local extras (no mutation of server cache)
        const mergedCache = (tid) => {
          const base  = SERVER_MATCH_CACHE[tid] || [];
          const extra = localExtra[tid] || [];
          return base.concat(extra);
        };
        const hLast5 = buildLast5(homeId, { [homeId]: mergedCache(homeId), [awayId]: mergedCache(awayId) });
        const aLast5 = buildLast5(awayId, { [homeId]: mergedCache(homeId), [awayId]: mergedCache(awayId) });
        const hAvgFH = hLast5.length ? +(hLast5.reduce((s, g) => s + g.fhFor + g.fhAgst, 0) / hLast5.length).toFixed(2) : null;
        const aAvgFH = aLast5.length ? +(aLast5.reduce((s, g) => s + g.fhFor + g.fhAgst, 0) / aLast5.length).toFixed(2) : null;

        const isComplete = fix.status === "complete";
        const fhH = parseInt(fix.ht_goals_team_a || 0, 10);
        const fhA = parseInt(fix.ht_goals_team_b || 0, 10);
        const ftH = parseInt(fix.homeGoalCount   || 0, 10);
        const ftA = parseInt(fix.awayGoalCount   || 0, 10);

        preds.push({
          id: fix.id, homeId, awayId,
          league:       leagueName,
          leagueSid:    parseInt(sid, 10),
          home:         fix.home_name || "",
          away:         fix.away_name || "",
          dt:           (fix.date_unix || 0) * 1000,
          matchDate,
          status:       fix.status || "upcoming",
          missingStats: missing,
          // snapshot for display
          snap: snap ? {
            fetchedAt: snap.fetchedAt,
            home: {
              name:      snap.home.name,
              scored_fh: snap.home.scored_fh,
              conced_fh: snap.home.conced_fh,
              t1_pct:    snap.home.t1_pct,
              cn010_avg: snap.home.cn010_avg,
            },
            away: {
              name:      snap.away.name,
              scored_fh: snap.away.scored_fh,
              conced_fh: snap.away.conced_fh,
              t1_pct:    snap.away.t1_pct,
              cn010_avg: snap.away.cn010_avg,
            },
          } : null,
          // signal outputs
          rank:     result ? result.rank     : 0,
          label:    result ? result.label    : "Low",
          prob25:   result ? result.prob25   : 11.5,
          prob15:   result ? result.prob15   : 32.9,
          eligible: result ? result.eligible : false,
          ci:       result ? result.ci       : 0,
          signals:  result ? result.signals  : {},
          // form
          hLast5, aLast5, hAvgFH, aAvgFH,
          // result
          matchResult: isComplete ? {
            fhH, fhA, ftH, ftA,
            hit25: (fhH + fhA) > 2,
            hit15: (fhH + fhA) > 1,
          } : null,
        });
      }
    }

    // Rebuild server match cache from newly cached league data
    rebuildServerMatchCache();

    preds.sort((a, b) => b.rank - a.rank || b.prob25 - a.prob25);

    const rateLimited = Date.now() < RATE_LIMITED_UNTIL;
    res.send(buildHTML(preds, dates, rateLimited));
  } catch(e) {
    console.error(e);
    res.status(500).send("<pre>Error: " + e.message + "\n" + e.stack + "</pre>");
  }
});

// ─── HTML BUILDER ─────────────────────────────────────────────────────────────
function buildHTML(preds, dates, rateLimited) {
  const predsJSON = JSON.stringify(preds)
    .replace(/</g, "\\u003c").replace(/>/g, "\\u003e").replace(/&/g, "\\u0026");

  const CSS = `
*{box-sizing:border-box;margin:0;padding:0}
body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;font-size:15px}
.hdr{background:#0f1923;padding:14px 16px;position:sticky;top:0;z-index:10}
.hdr-inner{max-width:860px;margin:0 auto}
.hdr-top{display:flex;align-items:center;justify-content:space-between;gap:8px}
.hdr-sub{font-size:10px;letter-spacing:2px;text-transform:uppercase;color:#ff6b35;font-weight:500}
.hdr-title{font-size:20px;font-weight:700;color:#fff;margin-top:2px}
.tabs{display:flex;gap:6px;margin-top:10px;flex-wrap:wrap}
.tab{padding:5px 11px;border-radius:20px;font-size:12px;font-weight:500;cursor:pointer;border:1px solid rgba(255,255,255,0.15);color:rgba(255,255,255,0.6);background:transparent;white-space:nowrap}
.tab.active{background:#ff6b35;border-color:#ff6b35;color:#fff}
.body{padding:12px 14px;max-width:860px;margin:0 auto}
.league-row{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:13px 15px;display:flex;align-items:center;justify-content:space-between;cursor:pointer;margin-bottom:8px;transition:box-shadow .15s}
.league-row:hover{box-shadow:0 2px 8px rgba(0,0,0,.08)}
.card{background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;margin-bottom:12px}
.card-accent{height:4px}
.card-inner{padding:14px}
.rank-pill{text-align:center;border-radius:10px;padding:8px 10px;min-width:76px;flex-shrink:0;border:1px solid}
.r4{background:#e8f5e9;border-color:#a5d6a7} .r4 .rn,.r4 .rl{color:#1b5e20}
.r3{background:#f1f8e9;border-color:#c5e1a5} .r3 .rn,.r3 .rl{color:#33691e}
.r2{background:#fff8e1;border-color:#ffe082} .r2 .rn,.r2 .rl{color:#e65100}
.r1,.r0{background:#f3f4f6;border-color:#e5e7eb} .r1 .rn,.r1 .rl,.r0 .rn,.r0 .rl{color:#9ca3af}
.rn{font-size:24px;font-weight:700;line-height:1}
.rl{font-size:10px;font-weight:500;margin-top:2px}
.prob-strip{display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap}
.pp{display:flex;align-items:center;gap:5px;padding:4px 9px;border-radius:20px;border:0.5px solid}
.pp15{background:#eff6ff;border-color:#bfdbfe}
.pp25{background:#f0fdf4;border-color:#bbf7d0}
.pp-dot{width:5px;height:5px;border-radius:50%;flex-shrink:0}
.pp15 .pp-dot{background:#2563eb} .pp25 .pp-dot{background:#16a34a}
.pp-lbl{font-size:10px;font-weight:500}
.pp15 .pp-lbl{color:#1e40af} .pp25 .pp-lbl{color:#166534}
.pp-val{font-size:12px;font-weight:700}
.pp15 .pp-val{color:#1d4ed8} .pp25 .pp-val{color:#15803d}
.teams{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:8px;margin-bottom:12px}
.team-box{background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:10px}
.team-role{font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:#9ca3af;margin-bottom:2px}
.team-name{font-size:14px;font-weight:700;color:#111827;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:6px}
.form-strip{display:flex;gap:3px;margin-bottom:7px;align-items:center;flex-wrap:wrap}
.form-lbl{font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.5px;margin-right:1px}
.fw{display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:4px;font-size:9px;font-weight:700;background:#dcfce7;color:#166534}
.fd{display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:4px;font-size:9px;font-weight:700;background:#f3f4f6;color:#6b7280;border:1px solid #e5e7eb}
.fl{display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:4px;font-size:9px;font-weight:700;background:#fee2e2;color:#b91c1c}
.stat-grid{display:grid;grid-template-columns:1fr 1fr;gap:4px}
.chip{background:#fff;border:1px solid #e5e7eb;border-radius:6px;padding:5px 6px}
.chip.on{background:#f0fdf4;border-color:#a5d6a7}
.chip-lbl{font-size:7px;color:#9ca3af;text-transform:uppercase;letter-spacing:.3px;margin-bottom:2px}
.chip.on .chip-lbl{color:#166534}
.chip-val{font-size:13px;font-weight:700;color:#111827}
.chip.on .chip-val{color:#15803d}
.chip-thr{font-size:8px;color:#9ca3af;margin-top:1px}
.chip.on .chip-thr{color:#15803d}
.signals{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:10px}
.sig{display:flex;align-items:center;gap:4px;font-size:10px;padding:3px 8px;border-radius:20px;font-weight:500;border:1px solid}
.sig-y{background:#f0fdf4;color:#15803d;border-color:#a5d6a7}
.sig-n{background:#fef2f2;color:#b91c1c;border-color:#fca5a5}
.sig-dot{width:5px;height:5px;border-radius:50%;flex-shrink:0}
.sig-y .sig-dot{background:#16a34a} .sig-n .sig-dot{background:#dc2626}
.ci-bar{background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:7px 10px;font-size:11px;color:#1e40af;font-family:monospace;margin-bottom:12px}
.result-box{border-radius:10px;overflow:hidden;margin-bottom:12px;display:flex;flex-wrap:wrap;border:1px solid #e5e7eb}
.res-cell{padding:9px 12px;text-align:center;flex:1;min-width:70px}
.toggle-btn{font-size:12px;color:#6b7280;cursor:pointer;padding-top:11px;display:flex;align-items:center;gap:5px;border-top:1px solid #f3f4f6;margin-top:4px}
.details{display:none;padding-top:10px}
.details.open{display:block}
.form-wrap{border-top:1px solid #f3f4f6;padding-top:10px}
.form-team-lbl{font-size:10px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px}
.tbl-scroll{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}
.ftable{width:100%;min-width:460px;border-collapse:collapse;font-size:11px;margin-bottom:10px}
.ftable th{background:#f9fafb;padding:5px;text-align:left;font-size:9px;color:#6b7280;font-weight:600;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid #e5e7eb;white-space:nowrap}
.ftable td{padding:5px;border-bottom:1px solid #f9fafb;white-space:nowrap}
.ftable tr:last-child td{border-bottom:none}
.ftable tfoot td{background:#f9fafb;font-weight:600;font-size:10px;padding:5px;border-top:1px solid #e5e7eb}
.fw2{color:#15803d;font-weight:700} .fl2{color:#b91c1c;font-weight:700} .fd2{color:#6b7280;font-weight:700}
.fh-hot{color:#c2410c;font-weight:600}
.back-btn{background:#f3f4f6;border:1px solid #e5e7eb;padding:6px 12px;border-radius:20px;font-size:12px;cursor:pointer;color:#374151;white-space:nowrap}
@media(max-width:480px){
  .hdr-title{font-size:17px}.rn{font-size:20px}.team-name{font-size:12px}
  .sig{font-size:9px;padding:2px 6px}.ci-bar{font-size:10px}
}
@media(min-width:640px){
  .hdr{padding:14px 20px}.body{padding:16px 20px}.card-inner{padding:16px}
  .hdr-title{font-size:22px}.tab{font-size:13px;padding:6px 14px}
  .sig{font-size:11px;padding:4px 9px}.ftable{font-size:12px}.ftable th{font-size:10px}
}`.trim();

  let J = "";
  J += "var ALL=" + predsJSON + ";";
  J += "var DATES=" + JSON.stringify(dates) + ";";
  J += "var DAY_LABELS=['Today','Tomorrow','Day 3','Day 4','Day 5'];";
  J += "var activeDate=DATES[0]||null;";
  J += "var activeLeague=null;";

  J += "function esc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;');}";
  J += "function fmtDate(d){return new Date(d).toLocaleDateString('en-GB',{weekday:'long',day:'2-digit',month:'short'});}";
  J += "function rankAccent(r){return r===4?'#2e7d32':r===3?'#558b2f':r===2?'#f9a825':r===1?'#ef6c00':'#9e9e9e';}";
  J += "function rankCls(r){return r===4?'r4':r===3?'r3':r===2?'r2':r===1?'r1':'r0';}";
  J += "function lgLabel(r){return r===4?'Fire \ud83d\udd25':r===3?'Prime \u26a1':r===2?'Watch \ud83d\udc40':r===1?'Signal \ud83d\udce1':'Low';}";
  J += "function lgLabelCol(r){return r===4?'#1b5e20':r===3?'#33691e':r===2?'#e65100':'#9ca3af';}";
  J += "function fLetter(r){return r==='W'?'<span class=\"fw\">W</span>':r==='L'?'<span class=\"fl\">L</span>':'<span class=\"fd\">D</span>';}";
  J += "function mkChip(lbl,val,thr,on){return '<div class=\"chip'+(on?' on':'')+'\"><div class=\"chip-lbl\">'+esc(lbl)+'</div><div class=\"chip-val\">'+esc(val)+'</div><div class=\"chip-thr\">'+esc(thr)+'</div></div>';}";

  // renderTabs
  J += "function renderTabs(){";
  J += "  var el=document.getElementById('dayTabs');var h='';";
  J += "  for(var i=0;i<DATES.length;i++){";
  J += "    var d=DATES[i];";
  J += "    var cnt=ALL.filter(function(p){return p.matchDate===d;}).length;";
  J += "    h+='<button class=\"tab'+(d===activeDate?' active':'')+'\" data-di=\"'+i+'\">'+esc(DAY_LABELS[i]||d)+' <span style=\"font-size:10px;opacity:.7\">('+cnt+')</span></button>';";
  J += "  }";
  J += "  el.innerHTML=h;";
  J += "  el.querySelectorAll('[data-di]').forEach(function(btn){btn.addEventListener('click',function(){";
  J += "    var i=Number(btn.getAttribute('data-di'));";
  J += "    activeDate=DATES[i];activeLeague=null;renderTabs();renderLeagueList();";
  J += "    document.getElementById('hdrTitle').textContent=fmtDate(new Date(DATES[i]+'T12:00:00'));";
  J += "  });});";
  J += "}";

  // renderLeagueList
  J += "function renderLeagueList(){";
  J += "  var main=document.getElementById('mainView');";
  J += "  if(!activeDate){main.innerHTML='';return;}";
  J += "  var dp=ALL.filter(function(p){return p.matchDate===activeDate;});";
  J += "  var lmap={};dp.forEach(function(p){if(!lmap[p.league])lmap[p.league]=[];lmap[p.league].push(p);});";
  J += "  var ll=Object.entries(lmap).sort(function(a,b){";
  J += "    return Math.max.apply(null,b[1].map(function(p){return p.rank;}))-Math.max.apply(null,a[1].map(function(p){return p.rank;}));";
  J += "  });";
  J += "  if(!ll.length){main.innerHTML='<p style=\"color:#6b7280;text-align:center;padding:40px\">No matches found.</p>';return;}";
  J += "  var h='<div style=\"font-size:12px;color:#6b7280;margin-bottom:12px\">'+dp.length+' matches across '+ll.length+' leagues</div>';";
  J += "  ll.forEach(function(e){";
  J += "    var lg=e[0],ms=e[1];";
  J += "    var tr=Math.max.apply(null,ms.map(function(p){return p.rank;}));";
  J += "    var en=ms.filter(function(p){return p.eligible;}).length;";
  J += "    var col=lgLabelCol(tr);";
  J += "    h+='<div class=\"league-row\" data-lg=\"'+esc(lg)+'\">'";
  J += "      +'<div style=\"min-width:0;flex:1;margin-right:12px\">'";
  J += "        +'<div style=\"font-size:14px;font-weight:500;color:#111827;white-space:nowrap;overflow:hidden;text-overflow:ellipsis\">'+esc(lg)+'</div>'";
  J += "        +'<div style=\"font-size:11px;color:#6b7280;margin-top:2px\">'+ms.length+' match'+(ms.length>1?'es':'')+(en?' &middot; <span style=\"color:#15803d;font-weight:600\">'+en+' eligible</span>':'')+'</div>'";
  J += "      +'</div>'";
  J += "      +'<div style=\"text-align:right;flex-shrink:0\">'";
  J += "        +'<div style=\"font-size:24px;font-weight:700;color:'+col+'\">'+tr+'/4</div>'";
  J += "        +'<div style=\"font-size:10px;font-weight:600;color:'+col+'\">'+esc(lgLabel(tr))+'</div>'";
  J += "      +'</div></div>';";
  J += "  });";
  J += "  main.innerHTML=h;";
  J += "  main.querySelectorAll('[data-lg]').forEach(function(el){el.addEventListener('click',function(){activeLeague=el.getAttribute('data-lg');renderMatchList();});});";
  J += "}";

  // renderMatchList
  J += "function renderMatchList(){";
  J += "  var ms=ALL.filter(function(p){return p.matchDate===activeDate&&p.league===activeLeague;}).sort(function(a,b){return b.rank-a.rank||b.prob25-a.prob25;});";
  J += "  var h='<div style=\"display:flex;align-items:center;gap:10px;margin-bottom:14px;flex-wrap:wrap\">'";
  J += "    +'<button class=\"back-btn\" id=\"backBtn\">\u2190 Back</button>'";
  J += "    +'<div style=\"font-size:15px;font-weight:700;color:#111827;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">'+esc(activeLeague)+'</div></div>';";
  J += "  ms.forEach(function(m){h+=renderCard(m);});";
  J += "  document.getElementById('mainView').innerHTML=h;";
  J += "  document.getElementById('backBtn').addEventListener('click',function(){activeLeague=null;renderLeagueList();});";
  J += "  document.querySelectorAll('.toggle-btn').forEach(function(btn){btn.addEventListener('click',function(){";
  J += "    var d=btn.nextElementSibling;var o=d.classList.toggle('open');";
  J += "    btn.innerHTML=o?'\u25b2 Hide last 5 games':'\u25bc Show last 5 games';";
  J += "  });});";
  J += "  ms.forEach(function(m){renderForm(m.id,m.homeId,m.home,m.hLast5,m.hAvgFH);renderForm(m.id,m.awayId,m.away,m.aLast5,m.aAvgFH);});";
  J += "}";

  // renderForm — returns empty string (hidden) when no games in window
  J += "function renderForm(mid,tid,tname,games,avgFH){";
  J += "  var el=document.getElementById('form-'+mid+'-'+tid);if(!el)return;";
  J += "  if(!games||!games.length){el.innerHTML='';return;}";
  J += "  var rows=games.map(function(g){";
  J += "    var hot=(g.fhFor+g.fhAgst)>2;";
  J += "    var rc=g.result==='W'?'fw2':g.result==='L'?'fl2':'fd2';";
  J += "    return '<tr><td>'+esc(g.date)+'</td>'";
  J += "      +'<td style=\"overflow:hidden;text-overflow:ellipsis\">'+esc(g.opp)+'</td>'";
  J += "      +'<td style=\"text-align:center;color:#9ca3af\">'+esc(g.venue)+'</td>'";
  J += "      +'<td style=\"text-align:center\" class=\"'+(hot?'fh-hot':'')+'\">'+g.fhFor+'-'+g.fhAgst+'</td>'";
  J += "      +'<td style=\"text-align:center;color:#9ca3af\">'+g.ftFor+'-'+g.ftAgst+'</td>'";
  J += "      +'<td style=\"text-align:center\" class=\"'+rc+'\">'+g.result+'</td></tr>';";
  J += "  }).join('');";
  J += "  var foot='';";
  J += "  if(avgFH!==null&&avgFH!==undefined){foot='<tfoot><tr>'";
  J += "    +'<td colspan=\"3\" style=\"color:#6b7280\">Avg FH goals (last 5)</td>'";
  J += "    +'<td style=\"text-align:center;font-family:monospace;color:#1d4ed8;font-size:12px\">'+avgFH+'</td>'";
  J += "    +'<td colspan=\"2\" style=\"text-align:center;color:#9ca3af\">('+games.length+' games)</td>'";
  J += "    +'</tr></tfoot>';}";
  J += "  el.innerHTML='<div class=\"form-team-lbl\">'+esc(tname)+' \u2014 last 5 (all competitions)</div>'";
  J += "    +'<div class=\"tbl-scroll\"><table class=\"ftable\"><thead><tr>'";
  J += "    +'<th style=\"width:16%\">Date</th><th>Opponent</th>'";
  J += "    +'<th style=\"width:8%;text-align:center\">H/A</th><th style=\"width:10%;text-align:center\">FH</th>'";
  J += "    +'<th style=\"width:10%;text-align:center\">FT</th><th style=\"width:8%;text-align:center\">Res</th>'";
  J += "    +'</tr></thead><tbody>'+rows+'</tbody>'+foot+'</table></div>'";
  J += "    +'<div style=\"font-size:10px;color:#9ca3af;margin-top:4px\">All competitions \u00b7 orange FH = more than 2 first-half goals</div>';";
  J += "}";

  // renderCard
  J += "function renderCard(m){";
  J += "  var accent=rankAccent(m.rank);var rc=rankCls(m.rank);";
  J += "  var dt=m.dt?new Date(m.dt).toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}):m.matchDate;";

  // status badge
  J += "  var sb=m.status==='complete'";
  J += "    ?'<span style=\"background:#f3f4f6;color:#6b7280;border:1px solid #e5e7eb;font-size:10px;padding:2px 8px;border-radius:20px;font-weight:600\">Final</span>'";
  J += "    :m.status==='live'";
  J += "    ?'<span style=\"background:#fef9c3;color:#ca8a04;border:1px solid #fde047;font-size:10px;padding:2px 8px;border-radius:20px;font-weight:600\">\u25cf Live</span>'";
  J += "    :'<span style=\"background:#f0fdf4;color:#15803d;border:1px solid #a5d6a7;font-size:10px;padding:2px 8px;border-radius:20px;font-weight:600\">Upcoming</span>';";

  // prob strip
  J += "  var ps='<div class=\"prob-strip\">'";
  J += "    +'<div class=\"pp pp15\"><div class=\"pp-dot\"></div><span class=\"pp-lbl\">FH over 1.5</span><span class=\"pp-val\">'+m.prob15+'%</span></div>'";
  J += "    +'<div class=\"pp pp25\"><div class=\"pp-dot\"></div><span class=\"pp-lbl\">FH over 2.5</span><span class=\"pp-val\">'+m.prob25+'%</span></div>'";
  J += "    +'</div>';";

  // result box
  J += "  var rb='';";
  J += "  if(m.matchResult){";
  J += "    var r=m.matchResult;";
  J += "    var c25=r.hit25?'#15803d':'#dc2626',bg25=r.hit25?'#f0fdf4':'#fef2f2',bc25=r.hit25?'#a5d6a7':'#fca5a5';";
  J += "    var c15=r.hit15?'#1d4ed8':'#dc2626',bg15=r.hit15?'#eff6ff':'#fef2f2',bc15=r.hit15?'#bfdbfe':'#fca5a5';";
  J += "    rb='<div class=\"result-box\">'";
  J += "      +'<div class=\"res-cell\" style=\"background:#f9fafb;border-right:1px solid #e5e7eb\">'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:2px\">1st Half</div>'";
  J += "        +'<div style=\"font-family:monospace;font-weight:700;font-size:20px;color:#111827;line-height:1\">'+r.fhH+'\u2013'+r.fhA+'</div>'";
  J += "      +'</div>'";
  J += "      +'<div class=\"res-cell\" style=\"background:'+bg15+';border-right:1px solid '+bc15+'\">'";
  J += "        +'<div style=\"font-size:9px;text-transform:uppercase;letter-spacing:.6px;margin-bottom:2px;color:'+c15+'\">FH over 1.5</div>'";
  J += "        +'<div style=\"font-size:12px;font-weight:700;color:'+c15+'\">'+(r.hit15?'\u2713 HIT':'\u2717 MISS')+'</div>'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;margin-top:1px\">'+m.prob15+'% pre</div>'";
  J += "      +'</div>'";
  J += "      +'<div class=\"res-cell\" style=\"background:'+bg25+';border-right:1px solid '+bc25+'\">'";
  J += "        +'<div style=\"font-size:9px;text-transform:uppercase;letter-spacing:.6px;margin-bottom:2px;color:'+c25+'\">FH over 2.5</div>'";
  J += "        +'<div style=\"font-size:12px;font-weight:700;color:'+c25+'\">'+(r.hit25?'\u2713 HIT':'\u2717 MISS')+'</div>'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;margin-top:1px\">'+m.prob25+'% pre</div>'";
  J += "      +'</div>'";
  J += "      +'<div class=\"res-cell\" style=\"background:#f9fafb\">'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:2px\">Full Time</div>'";
  J += "        +'<div style=\"font-family:monospace;font-weight:700;font-size:16px;color:#374151;line-height:1\">'+r.ftH+'\u2013'+r.ftA+'</div>'";
  J += "      +'</div>'";
  J += "    +'</div>';";
  J += "  }";

  // team boxes — 4 chips each showing the 4 signal-relevant stats
  J += "  function teamBox(role,name,tid,last5,sn){";
  J += "    var fs='<div class=\"form-strip\"><span class=\"form-lbl\">Form</span>';";
  J += "    if(last5&&last5.length)last5.forEach(function(g){fs+=fLetter(g.result);});";
  J += "    else fs+='<span style=\"font-size:10px;color:#9ca3af\">...</span>';";
  J += "    fs+='</div>';";
  J += "    var chips='';";
  J += "    if(sn){";
  J += "      var sfx=role==='Home'?'(home)':'(away)';";
  J += "      var scoredThr=role==='Home'?1.5:1.5;";
  J += "      chips+=mkChip('FH Scored '+sfx,sn.scored_fh.toFixed(2),'\u2265 1.50 \u2192 sig D',sn.scored_fh>=1.5);";
  J += "      chips+=mkChip('FH Conceded '+sfx,sn.conced_fh.toFixed(2),'used in CI (A)',false);";
  J += "      chips+=mkChip('FH>2.5 hist '+sfx,sn.t1_pct.toFixed(0)+'%','\u2265 20% \u2192 sig B',sn.t1_pct>=20);";
  J += "      chips+=mkChip('Early conceded',sn.cn010_avg.toFixed(2),'\u2265 0.25 \u2192 sig C',sn.cn010_avg>=0.25);";
  J += "    }";
  J += "    return '<div class=\"team-box\">'";
  J += "      +'<div class=\"team-role\">'+esc(role)+'</div>'";
  J += "      +'<div class=\"team-name\">'+esc(name)+'</div>'";
  J += "      +fs+'<div class=\"stat-grid\">'+chips+'</div>'";
  J += "      +'</div>';";
  J += "  }";

  // signals row — 4 signals A/B/C/D, all equal weight
  J += "  var sigsH='<div class=\"signals\">';";
  J += "  ['A','B','C','D'].forEach(function(k){";
  J += "    var s=m.signals[k];if(!s)return;";
  J += "    var cls=s.met?'sig-y':'sig-n';";
  J += "    sigsH+='<div class=\"sig '+cls+'\">'";
  J += "      +'<div class=\"sig-dot\"></div>'+esc(k)+' \u00b7 '+esc(s.label)";
  J += "      +'<span style=\"opacity:.7;margin-left:4px;font-size:9px\">('+esc(s.value)+')</span>'";
  J += "      +'</div>';";
  J += "  });";
  J += "  sigsH+='</div>';";

  // CI bar — shows the combined intensity value
  J += "  var ciH='';";
  J += "  if(m.snap){";
  J += "    var h=m.snap.home,a=m.snap.away,ciMet=m.ci>=3.2;";
  J += "    ciH='<div class=\"ci-bar\">CI = '";
  J += "      +h.scored_fh.toFixed(2)+' + '+a.scored_fh.toFixed(2)";
  J += "      +' + '+h.conced_fh.toFixed(2)+' + '+a.conced_fh.toFixed(2)";
  J += "      +' = <strong style=\"color:'+(ciMet?'#15803d':'#374151')+'\">'+m.ci+'</strong>'";
  J += "      +' \u00b7 needs \u2265 3.20 '+(ciMet?'\u2713':'\u2717')+'</div>';";
  J += "  }";

  // missing stats warning
  J += "  var mw=m.missingStats?'<span style=\"background:#fef3c7;color:#92400e;font-size:10px;padding:2px 6px;border-radius:4px;font-weight:600;margin-left:6px\">\u26a0 no stats</span>':'';";

  // assemble card
  J += "  return '<div class=\"card\">'";
  J += "    +'<div class=\"card-accent\" style=\"background:'+accent+'\"></div>'";
  J += "    +'<div class=\"card-inner\">'";
  J += "      +'<div style=\"display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin-bottom:12px\">'";
  J += "        +'<div style=\"min-width:0\">'";
  J += "          +'<div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:5px\">'+esc(m.league)+' \u00b7 '+esc(dt)+mw+'</div>'";
  J += "          +sb";
  J += "        +'</div>'";
  J += "        +'<div class=\"rank-pill '+rc+'\">'";
  J += "          +'<div class=\"rn\">'+m.rank+'/4</div>'";
  J += "          +'<div class=\"rl\">'+esc(m.label)+'</div>'";
  J += "        +'</div>'";
  J += "      +'</div>'";
  J += "    +ps+rb";
  J += "    +'<div class=\"teams\">'";
  J += "    +teamBox('Home',m.home,m.homeId,m.hLast5,m.snap?m.snap.home:null)";
  J += "    +teamBox('Away',m.away,m.awayId,m.aLast5,m.snap?m.snap.away:null)";
  J += "    +'</div>'";
  J += "    +sigsH+ciH";
  // Only render toggle + form section when at least one team has recent data
  J += "    +(m.hLast5&&m.hLast5.length||m.aLast5&&m.aLast5.length?";
  J += "      '<div class=\"toggle-btn\">\u25bc Show last 5 games</div>'";
  J += "      +'<div class=\"details\">'";
  J += "        +'<div class=\"form-wrap\">'";
  J += "          +'<div id=\"form-'+m.id+'-'+m.homeId+'\"><div style=\"font-size:11px;color:#9ca3af\">\u21bb Loading '+esc(m.home)+'...</div></div>'";
  J += "          +'<div id=\"form-'+m.id+'-'+m.awayId+'\" style=\"margin-top:10px\"><div style=\"font-size:11px;color:#9ca3af\">\u21bb Loading '+esc(m.away)+'...</div></div>'";
  J += "        +'</div>'";
  J += "      +'</div>'";
  J += "    :'')";
  J += "  +'</div></div>';";
  J += "}";

  J += "if(DATES.length)document.getElementById('hdrTitle').textContent=fmtDate(new Date(DATES[0]+'T12:00:00'));";
  J += "renderTabs();renderLeagueList();";

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>First Half Predictor</title>
<script>(function(){var p=new URLSearchParams(window.location.search);if(!p.has('tz')){p.set('tz',-new Date().getTimezoneOffset());window.location.search=p.toString();}})();<\/script>
<style>${CSS}</style>
</head>
<body>
<div class="hdr">
  <div class="hdr-inner">
    <div class="hdr-top">
      <div>
        <div class="hdr-sub">&#9917; First Half Predictor</div>
        <div class="hdr-title" id="hdrTitle">Loading...</div>
      </div>
      <button onclick="location.reload()" style="background:#ff6b35;color:#fff;border:none;padding:7px 14px;border-radius:20px;font-size:12px;font-weight:600;cursor:pointer;flex-shrink:0;">&#8635; Refresh</button>
    </div>
    <div class="tabs" id="dayTabs"></div>
  </div>
</div>
<div class="body">
  ${rateLimited ? '<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:12px;color:#b91c1c;line-height:1.6"><strong>&#9888; API rate limit reached</strong> — showing cached data. Resets at ' + new Date(RATE_LIMITED_UNTIL).toLocaleTimeString('en-GB') + '. <a href="/cache-status" style="color:#b91c1c">View cache status</a></div>' : ''}
  <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:12px;color:#92400e;line-height:1.6">
    <strong>How it works:</strong> 4 independent signals (A–D), each worth 1 point. Rank = signals fired.
    Backtested on 24,203 matches · base rate 12.6% · Rank 4 = 73.2% FH&gt;2.5 &middot; 85.4% FH&gt;1.5.
  </div>
  <div id="mainView"></div>
</div>
<script>${J}<\/script>
</body>
</html>`;
}

// ─── STARTUP ─────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log("Server on port " + PORT);
  console.log("Memory: " + Math.round(process.memoryUsage().heapUsed/1024/1024) + "MB");
  fetchLeagueList().then(() => {
    if (Object.keys(LEAGUE_NAMES).length > 0) {
      setTimeout(rebuildServerMatchCache, 5 * 60 * 1000);
      console.log("Match cache warming in 5 min...");
    }
  }).catch(e => console.error("League list failed:", e.message));
});

process.on("uncaughtException", e => console.error("Uncaught:", e.message));
