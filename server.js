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

let LEAGUE_NAMES = {};
let LEAGUE_LIST_LOADING = false;

async function fetchLeagueList() {
  if (LEAGUE_LIST_LOADING) return;
  LEAGUE_LIST_LOADING = true;
  try {
    const data = await safeFetch(BASE + "/league-list?key=" + KEY);
    if (!data) { console.warn("fetchLeagueList skipped"); LEAGUE_LIST_LOADING = false; return; }
    const list = data.data || [];
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
    if (list.length === 0) setTimeout(fetchLeagueList, 2 * 60 * 1000);
  } catch(e) {
    console.error("Failed to load league list: " + e.message);
    LEAGUE_NAMES = {};
  } finally {
    LEAGUE_LIST_LOADING = false;
  }
}

const FIXTURE_CACHE        = {};
const LEAGUE_MATCHES_CACHE = {};
const TEAM_STATS_CACHE     = {};
let   SERVER_MATCH_CACHE   = {};
const CI_SNAPSHOT_CACHE    = {};  // matchId → { ci, defCi, rank, label, prob25, prob15, eligible, signals, snap }
let   RATE_LIMITED_UNTIL   = 0;

const TTL_FIXTURES = 30 * 60 * 1000;
const TTL_MATCHES  =  2 * 60 * 60 * 1000;
const TTL_TEAMS    =  6 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS = 8000;

async function safeFetch(url) {
  if (Date.now() < RATE_LIMITED_UNTIL) { console.warn("Rate limited: " + url); return null; }
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    const data = await fetch(url, { signal: controller.signal }).then(r => r.json()).finally(() => clearTimeout(timer));
    if (data && data.error && String(data.error).toLowerCase().includes("rate limit")) {
      const reset = data.metadata && data.metadata.request_limit_refresh_next
        ? data.metadata.request_limit_refresh_next * 1000
        : Date.now() + 60 * 60 * 1000;
      RATE_LIMITED_UNTIL = reset;
      console.warn("Rate limit hit — backing off for " + Math.ceil((reset - Date.now()) / 60000) + " min");
      return null;
    }
    return data;
  } catch(e) { console.error("Fetch error: " + e.message); return null; }
}

async function fetchFixtures(date) {
  const now = Date.now();
  if (FIXTURE_CACHE[date] && (now - FIXTURE_CACHE[date].ts) < TTL_FIXTURES) return FIXTURE_CACHE[date].data;
  const data = await safeFetch(BASE + "/todays-matches?date=" + date + "&key=" + KEY);
  if (data) FIXTURE_CACHE[date] = { data, ts: now };
  return data || FIXTURE_CACHE[date]?.data || { data: [] };
}

async function fetchLeagueMatches(sid) {
  const now = Date.now();
  if (LEAGUE_MATCHES_CACHE[sid] && (now - LEAGUE_MATCHES_CACHE[sid].ts) < TTL_MATCHES) return LEAGUE_MATCHES_CACHE[sid].data;
  const page1 = await safeFetch(BASE + "/league-matches?season_id=" + sid + "&max_per_page=300&page=1&sort=date_unix&order=desc&key=" + KEY);
  if (!page1) return LEAGUE_MATCHES_CACHE[sid]?.data || { data: [] };
  let allMatches = page1.data || [];
  // Fetch page 2 only for large seasons (e.g. La Liga ~380 matches)
  if (allMatches.length >= 300) {
    const page2 = await safeFetch(BASE + "/league-matches?season_id=" + sid + "&max_per_page=300&page=2&sort=date_unix&order=desc&key=" + KEY);
    if (page2 && page2.data && page2.data.length) allMatches = allMatches.concat(page2.data);
  }
  const data = { ...page1, data: allMatches };
  LEAGUE_MATCHES_CACHE[sid] = { data, ts: now };
  return data;
}

async function fetchTeamStats(sid) {
  const now = Date.now();
  if (TEAM_STATS_CACHE[sid] && (now - TEAM_STATS_CACHE[sid].ts) < TTL_TEAMS) return TEAM_STATS_CACHE[sid].data;
  const data = await safeFetch(BASE + "/league-teams?season_id=" + sid + "&include=stats&key=" + KEY);
  if (data) TEAM_STATS_CACHE[sid] = { data, ts: now };
  return data || TEAM_STATS_CACHE[sid]?.data || { data: [] };
}

function rebuildServerMatchCache() {
  const newCache = {};
  let total = 0;
  for (const [sid, entry] of Object.entries(LEAGUE_MATCHES_CACHE)) {
    const leagueName = LEAGUE_NAMES[parseInt(sid, 10)] || "League " + sid;
    const nowSecs = Math.floor(Date.now() / 1000);
    for (const m of (entry.data.data || []).filter(m => isPlayedMatch(m, nowSecs))) {
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

const PREV_SEASON = { 16504:13973, 16544:11321, 16571:15746, 16614:14086, 16615:14116, 16036:13703 };
const ftch = url => fetch(url).then(r => r.json());
const safe = v   => (isNaN(v) || !isFinite(v)) ? 0 : Number(v);

function getDates(tzOffset) {
  const now = new Date();
  const local = new Date(now.getTime() + tzOffset * 60 * 1000);
  const fmt = d => {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
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
  const d = new Date((unix * 1000) + tzOffset * 60 * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return y + "-" + m + "-" + day;
}

const PROB25_BY_RANK = { 4: 87.5, 3: 62.0, 2: 40.3, 1: 29.6, 0: 10.0 };
const PROB15_BY_RANK = { 4: 100.0, 3: 75.4, 2: 66.8, 1: 61.1, 0: 31.4 };
const RANK_LABELS = { 4: "Fire", 3: "Prime", 2: "Watch", 1: "Signal", 0: "Low" };

function computeSignals(snap) {
  const h = snap.home, a = snap.away;
  const ci    = safe(h.scored_fh + a.scored_fh + h.conced_fh + a.conced_fh);
  const defCi = safe(h.conced_fh + a.conced_fh);
  const sigA = ci    >= 3.2;
  const sigB = h.t1_pct >= 25 && a.t1_pct >= 25;
  const sigC = defCi >= 2.25;
  const sigD = a.scored_fh >= 1.25;
  const rank = [sigA, sigB, sigC, sigD].filter(Boolean).length;
  return {
    rank, label: RANK_LABELS[rank] || "Low",
    prob25: PROB25_BY_RANK[rank] ?? 10.0,
    prob15: PROB15_BY_RANK[rank] ?? 31.4,
    ci: +ci.toFixed(2), defCi: +defCi.toFixed(2), eligible: rank >= 3,
    signals: {
      A: { met: sigA, label: "Combined Intensity",  value: ci.toFixed(2),                                                  threshold: ">= 3.20" },
      B: { met: sigB, label: "FH History Both",     value: h.t1_pct.toFixed(0) + "%/" + a.t1_pct.toFixed(0) + "%",        threshold: "both >= 25%" },
      C: { met: sigC, label: "Leaky Defences",      value: h.conced_fh.toFixed(2) + "+" + a.conced_fh.toFixed(2) + "=" + defCi.toFixed(2), threshold: ">= 2.25" },
      D: { met: sigD, label: "Away FH Attack",      value: a.scored_fh.toFixed(2),                                         threshold: ">= 1.25" },
    },
  };
}

function extractStats(teamObj, role) {
  const s = teamObj.stats || {};
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
    // Shots on target per game (role-specific, fallback to overall)
    sot_avg:   safe((s["shotsOnTarget" + sfx] || s["shotsOnTarget_overall"] || 0) / mpR),
    mp:        safe(s.seasonMatchesPlayed_overall || 0),
    mpRole:    mpR,
  };
}

const LAST5_WINDOW_SECS = 35 * 24 * 60 * 60;

// FIX 3: isPlayedMatch — used for both addToLocalExtra AND completed filtering
// so that incomplete-but-played matches (e.g. Liga MX Femenil) are included
// A past match with status "incomplete" is treated as played (covers 0-0 results too)
const isPlayedMatch = (m, nowSecs) =>
  m.status === "complete" ||
  (m.status === "incomplete" &&
   (m.date_unix || 0) < nowSecs - 7200);

function buildLast5(teamId, cache) {
  if (!teamId) return [];
  // Merge passed-in cache with a full scan of LEAGUE_MATCHES_CACHE for this team
  let entries = (cache[teamId] || []).slice();
  const passedKeys = new Set(entries.map(m => (m.date_unix||0)+"_"+(m.homeID||"")+"_"+(m.awayID||"")));
  const nowSecs = Math.floor(Date.now() / 1000);
  for (const entry of Object.values(LEAGUE_MATCHES_CACHE)) {
    for (const m of (entry.data.data || [])) {
      if (!isPlayedMatch(m, nowSecs)) continue;
      if (m.homeID !== teamId && m.awayID !== teamId) continue;
      const key = (m.date_unix||0)+"_"+(m.homeID||"")+"_"+(m.awayID||"");
      if (passedKeys.has(key)) continue;
      passedKeys.add(key);
      entries.push({
        homeID: m.homeID, awayID: m.awayID,
        home_name: m.home_name||"", away_name: m.away_name||"",
        date_unix: m.date_unix||0,
        ht_goals_team_a: parseInt(m.ht_goals_team_a||0,10),
        ht_goals_team_b: parseInt(m.ht_goals_team_b||0,10),
        homeGoalCount: parseInt(m.homeGoalCount||0,10),
        awayGoalCount: parseInt(m.awayGoalCount||0,10),
        status: m.status,
      });
    }
  }
  if (!entries.length) return [];
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - LAST5_WINDOW_SECS;
  const seen = new Set();
  const dedup = (m) => {
    const key = (m.date_unix || 0) + "_" + (m.homeID || "") + "_" + (m.awayID || "");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  };
  // Try 35-day window first
  let unique = entries.filter(m => (m.date_unix || 0) >= cutoff && dedup(m));
  // If fewer than 3, drop the time window entirely — just take the most recent
  if (unique.length < 3) {
    seen.clear();
    unique = entries.filter(m => dedup(m));
  }
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
               fhFor, fhAgst, ftFor, ftAgst, result };
    });
}

app.get("/cache-status", (req, res) => {
  const now = Date.now();
  res.json({
    rateLimitedUntil: RATE_LIMITED_UNTIL > now ? new Date(RATE_LIMITED_UNTIL).toISOString() : "not limited",
    rateLimitedMinRemaining: RATE_LIMITED_UNTIL > now ? Math.ceil((RATE_LIMITED_UNTIL - now) / 60000) : 0,
    leagueRegistry: Object.keys(LEAGUE_NAMES).length + " seasons mapped",
    serverMatchCache: Object.keys(SERVER_MATCH_CACHE).length + " teams",
    fixtureCacheEntries: Object.entries(FIXTURE_CACHE).map(([date, e]) => ({
      date, ageMin: Math.round((now - e.ts) / 60000), matchCount: (e.data.data || []).length,
    })),
    leagueMatchesCacheEntries: Object.keys(LEAGUE_MATCHES_CACHE).length,
    teamStatsCacheEntries: Object.keys(TEAM_STATS_CACHE).length,
    leagueMatchesCache: Object.entries(LEAGUE_MATCHES_CACHE).map(([sid, e]) => ({
      sid, league: LEAGUE_NAMES[parseInt(sid)] || "?",
      ageMin: Math.round((now - e.ts) / 60000), matchCount: (e.data.data || []).length,
    })),
    teamStatsCache: Object.entries(TEAM_STATS_CACHE).map(([sid, e]) => ({
      sid, ageMin: Math.round((now - e.ts) / 60000), teamCount: (e.data.data || []).length,
    })),
  });
});

app.get("/debug", async (req, res) => {
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const dates = getDates(tzOffset);
    const raw = await fetchFixtures(dates[0]);
    const fixtures = raw.data || [];
    res.json({
      date: dates[0], rateLimited: Date.now() < RATE_LIMITED_UNTIL,
      leagueRegistrySize: Object.keys(LEAGUE_NAMES).length,
      totalFixtures: fixtures.length,
      cachedTeams: Object.keys(SERVER_MATCH_CACHE).length,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get("/debug-team/:teamId", (req, res) => {
  const tid = parseInt(req.params.teamId, 10);
  const nowSecs = Math.floor(Date.now() / 1000);
  // Check SERVER_MATCH_CACHE
  const smcEntries = (SERVER_MATCH_CACHE[tid] || []).length;
  // Scan LEAGUE_MATCHES_CACHE
  const lmcMatches = [];
  for (const [sid, entry] of Object.entries(LEAGUE_MATCHES_CACHE)) {
    for (const m of (entry.data.data || [])) {
      if (m.homeID !== tid && m.awayID !== tid) continue;
      lmcMatches.push({
        sid, status: m.status, date_unix: m.date_unix,
        home: m.home_name, away: m.away_name,
        homeID: m.homeID, awayID: m.awayID,
        ht_a: m.ht_goals_team_a, ht_b: m.ht_goals_team_b,
        ftH: m.homeGoalCount, ftA: m.awayGoalCount,
        isPlayed: isPlayedMatch(m, nowSecs),
      });
    }
  }
  res.json({ teamId: tid, serverMatchCache: smcEntries, leagueMatchesCache: lmcMatches.slice(0, 20) });
});

app.get("/api/*", async (req, res) => {
  try {
    const path = req.path.replace("/api", "");
    const qs = new URLSearchParams({ ...req.query, key: KEY }).toString();
    res.json(await ftch(BASE + path + "?" + qs));
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// ─── SHARED PREDS COMPUTATION ────────────────────────────────────────────────
async function computePreds(tzOffset) {
  const fetchedAt = new Date().toISOString().slice(0, 16).replace("T", " ");
  const dates     = getDates(tzOffset);

  // Ensure league registry is loaded — kick off if not running, no blocking wait
  if (Object.keys(LEAGUE_NAMES).length === 0 && !LEAGUE_LIST_LOADING) fetchLeagueList();
  const leagueFilterActive = Object.keys(LEAGUE_NAMES).length > 0;

    const allFixtures = [];
    const dayResults = await Promise.all(dates.map(d => fetchFixtures(d)));
    for (let i = 0; i < dates.length; i++) {
      for (const m of (dayResults[i].data || [])) {
        allFixtures.push(Object.assign({}, m, { _date: dates[i] }));
      }
    }

    // FIX 4: deduplicate by match id before grouping — prevents same match
    // appearing twice when it maps to two season IDs in LEAGUE_NAMES
    const seenFixtureIds = new Set();
    const leagueFixtures = {};
    for (const m of allFixtures) {
      const sid = parseInt(m.competition_id, 10);
      if (leagueFilterActive && !LEAGUE_NAMES[sid]) continue;
      const fid = String(m.id || (m.homeID + "_" + m.awayID + "_" + (m.date_unix || 0)));
      if (seenFixtureIds.has(fid)) continue;
      seenFixtureIds.add(fid);
      if (!leagueFixtures[sid]) leagueFixtures[sid] = [];
      leagueFixtures[sid].push(m);
    }

    const localExtra = {};
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
        if (!isPlayedMatch(m, nowSecs)) continue;
        const key = (m.date_unix||0) + "_" + (m.homeID||"") + "_" + (m.awayID||"");
        if (serverCacheKeys.has(key)) continue;
        const slim = slimM(m, lg);
        if (m.homeID) { if (!localExtra[m.homeID]) localExtra[m.homeID] = []; localExtra[m.homeID].push(slim); }
        if (m.awayID) { if (!localExtra[m.awayID]) localExtra[m.awayID] = []; localExtra[m.awayID].push(slim); }
        serverCacheKeys.add(key);
      }
    };

    const preds = [];
    const leagueSids = Object.keys(leagueFixtures).filter(sid => leagueFixtures[sid].length);

    const leagueData = await Promise.all(leagueSids.map(async (sid) => {
      const leagueName = LEAGUE_NAMES[parseInt(sid, 10)] || "League " + sid;
      const [matchRes, teamRes] = await Promise.all([
        fetchLeagueMatches(sid).catch(e => { console.error("[" + sid + "] match: " + e.message); return { data: [] }; }),
        fetchTeamStats(sid).catch(e => { console.error("[" + sid + "] team: " + e.message); return { data: [] }; }),
      ]);

      // FIX 3: pass ALL matches to addToLocalExtra (not just complete),
      // and use isPlayedMatch for completed filter too
      const allMatches = matchRes.data || [];
      addToLocalExtra(allMatches, leagueName);
      const completed = allMatches.filter(m => isPlayedMatch(m, nowSecs));

      const needsPrevMatches = completed.length < 5 && PREV_SEASON[sid];
      const needsPrevTeams   = (teamRes.data || []).length === 0 && PREV_SEASON[sid];
      if (needsPrevMatches || needsPrevTeams) {
        const prevSid = PREV_SEASON[sid];
        const [prevMatch, prevTeam] = await Promise.all([
          needsPrevMatches ? fetchLeagueMatches(prevSid).catch(() => ({ data: [] })) : Promise.resolve({ data: [] }),
          needsPrevTeams   ? fetchTeamStats(prevSid).catch(() => ({ data: [] }))     : Promise.resolve({ data: [] }),
        ]);
        if (needsPrevMatches) addToLocalExtra(prevMatch.data || [], leagueName);
        if (needsPrevTeams)   teamRes.data = (teamRes.data || []).concat(prevTeam.data || []);
      }

      const teamMap = {};
      for (const t of (teamRes.data || [])) teamMap[t.id] = t;
      return { sid, leagueName, teamMap, fixtures: leagueFixtures[sid] };
    }));

    for (const { sid, leagueName, teamMap, fixtures } of leagueData) {
      for (const fix of fixtures) {
        const homeId   = fix.homeID || fix.home_id;
        const awayId   = fix.awayID || fix.away_id;
        const homeTeam = teamMap[homeId];
        const awayTeam = teamMap[awayId];
        const matchDate = fix.date_unix ? unixToLocalDate(fix.date_unix, tzOffset) : fix._date;

        const mergedCache = (tid) => (SERVER_MATCH_CACHE[tid] || []).concat(localExtra[tid] || []);
        const hLast5 = buildLast5(homeId, { [homeId]: mergedCache(homeId), [awayId]: mergedCache(awayId) });
        const aLast5 = buildLast5(awayId, { [homeId]: mergedCache(homeId), [awayId]: mergedCache(awayId) });
        const hAvgFH = hLast5.length ? +(hLast5.reduce((s, g) => s + g.fhFor + g.fhAgst, 0) / hLast5.length).toFixed(2) : null;
        const aAvgFH = aLast5.length ? +(aLast5.reduce((s, g) => s + g.fhFor + g.fhAgst, 0) / aLast5.length).toFixed(2) : null;

        let snap = null, result = null;
        const missing = !homeTeam || !awayTeam;
        if (!missing) {
          const hStats = extractStats(homeTeam, "home");
          const aStats = extractStats(awayTeam, "away");
          snap   = { fetchedAt, home: hStats, away: aStats };
          result = computeSignals(snap);
        } else if (hLast5.length >= 1 && aLast5.length >= 1) {
          // Synthetic stats from match history when API stats are missing
          const hHome = hLast5.filter(g => g.venue === "H");
          const aAway = aLast5.filter(g => g.venue === "A");
          const hGames = hHome.length >= 2 ? hHome : hLast5;
          const aGames = aAway.length >= 2 ? aAway : aLast5;
          const avg = (arr, fn) => arr.reduce((s, g) => s + fn(g), 0) / arr.length;
          const hScored  = +avg(hGames, g => g.fhFor).toFixed(2);
          const hConced  = +avg(hGames, g => g.fhAgst).toFixed(2);
          const aScored  = +avg(aGames, g => g.fhFor).toFixed(2);
          const aConced  = +avg(aGames, g => g.fhAgst).toFixed(2);
          const hT1pct   = +(hGames.filter(g => g.fhFor + g.fhAgst > 2).length / hGames.length * 100).toFixed(0);
          const aT1pct   = +(aGames.filter(g => g.fhFor + g.fhAgst > 2).length / aGames.length * 100).toFixed(0);
          const hStats = { name: fix.home_name || "", scored_fh: hScored, conced_fh: hConced, t1_pct: hT1pct, cn010_avg: 0, sot_avg: 0, mp: hGames.length, mpRole: hGames.length };
          const aStats = { name: fix.away_name || "", scored_fh: aScored, conced_fh: aConced, t1_pct: aT1pct, cn010_avg: 0, sot_avg: 0, mp: aGames.length, mpRole: aGames.length };
          snap   = { fetchedAt: fetchedAt + " (from history)", home: hStats, away: aStats };
          result = computeSignals(snap);
        }

        const isComplete = fix.status === "complete" || (fix.status === "incomplete" && isPlayedMatch(fix, nowSecs));
        const fhH = parseInt(fix.ht_goals_team_a || 0, 10);
        const fhA = parseInt(fix.ht_goals_team_b || 0, 10);
        const ftH = parseInt(fix.homeGoalCount   || 0, 10);
        const ftA = parseInt(fix.awayGoalCount   || 0, 10);

        // Snapshot pre-match prediction values before match completes
        const matchId = fix.id;
        if (result && !isComplete && !CI_SNAPSHOT_CACHE[matchId]) {
          CI_SNAPSHOT_CACHE[matchId] = {
            ci: result.ci, defCi: result.defCi, rank: result.rank,
            label: result.label, prob25: result.prob25, prob15: result.prob15,
            eligible: result.eligible, signals: result.signals,
            snap: snap ? {
              fetchedAt: snap.fetchedAt,
              home: { name: snap.home.name, scored_fh: snap.home.scored_fh, conced_fh: snap.home.conced_fh, t1_pct: snap.home.t1_pct, cn010_avg: snap.home.cn010_avg, sot_avg: snap.home.sot_avg },
              away: { name: snap.away.name, scored_fh: snap.away.scored_fh, conced_fh: snap.away.conced_fh, t1_pct: snap.away.t1_pct, cn010_avg: snap.away.cn010_avg, sot_avg: snap.away.sot_avg },
            } : null,
          };
        }
        // Use frozen snapshot if match is complete and we have one
        const frozen = isComplete ? CI_SNAPSHOT_CACHE[matchId] : null;

        preds.push({
          id: matchId, homeId, awayId,
          league: leagueName, leagueSid: parseInt(sid, 10),
          home: fix.home_name || "", away: fix.away_name || "",
          dt: (fix.date_unix || 0) * 1000,
          matchDate, status: fix.status || "upcoming", missingStats: missing && !result,
          snap: frozen ? frozen.snap : (snap ? {
            fetchedAt: snap.fetchedAt,
            home: { name: snap.home.name, scored_fh: snap.home.scored_fh, conced_fh: snap.home.conced_fh, t1_pct: snap.home.t1_pct, cn010_avg: snap.home.cn010_avg, sot_avg: snap.home.sot_avg },
            away: { name: snap.away.name, scored_fh: snap.away.scored_fh, conced_fh: snap.away.conced_fh, t1_pct: snap.away.t1_pct, cn010_avg: snap.away.cn010_avg, sot_avg: snap.away.sot_avg },
          } : null),
          rank:     frozen ? frozen.rank     : (result ? result.rank     : 0),
          label:    frozen ? frozen.label    : (result ? result.label    : "Low"),
          prob25:   frozen ? frozen.prob25   : (result ? result.prob25   : 10.0),
          prob15:   frozen ? frozen.prob15   : (result ? result.prob15   : 31.4),
          eligible: frozen ? frozen.eligible : (result ? result.eligible : false),
          ci:       frozen ? frozen.ci       : (result ? result.ci       : 0),
          defCi:    frozen ? frozen.defCi    : (result ? result.defCi    : 0),
          signals:  frozen ? frozen.signals  : (result ? result.signals  : {}),
          hLast5, aLast5, hAvgFH, aAvgFH,
          matchResult: isComplete ? { fhH, fhA, ftH, ftA, hit25: (fhH+fhA)>2, hit15: (fhH+fhA)>1 } : null,
        });
      }
    }

    rebuildServerMatchCache();
    preds.sort((a, b) => b.ci - a.ci || b.rank - a.rank);
  return { preds, dates };
}

// ─── /preds JSON ENDPOINT — kept for potential future use ───────────────────
app.get("/preds", async (req, res) => {
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const { preds } = await computePreds(tzOffset);
    res.json({ ok: true, preds, rateLimited: Date.now() < RATE_LIMITED_UNTIL });
  } catch(e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── MAIN ROUTE — serves shell instantly, /preds fetches data async ──────────
app.get("/", (req, res) => {
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const dates    = getDates(tzOffset);
    // Kick off background league list fetch if needed — no blocking
    if (Object.keys(LEAGUE_NAMES).length === 0 && !LEAGUE_LIST_LOADING) fetchLeagueList();
    res.send(buildHTML([], dates, Date.now() < RATE_LIMITED_UNTIL, tzOffset));
  } catch(e) {
    console.error(e);
    if (!res.headersSent) res.status(500).send("<pre>Error: " + e.message + "</pre>");
  }
});

function buildHTML(preds, dates, rateLimited, tzOffset) {
  const predsJSON = JSON.stringify(preds)
    .replace(/</g,"\\u003c").replace(/>/g,"\\u003e").replace(/&/g,"\\u0026");

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
.pill-bar{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px}
.lpill{display:inline-flex;align-items:center;gap:5px;padding:5px 11px;border-radius:20px;font-size:12px;font-weight:500;cursor:pointer;border:1.5px solid #e5e7eb;background:#fff;color:#374151;white-space:nowrap;transition:all .15s;user-select:none}
.lpill:hover{border-color:#d1d5db;background:#f9fafb}
.lpill.open{background:#0f1923;border-color:#0f1923;color:#fff}
.lpill-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.lpill-badge{font-size:10px;opacity:.7;margin-left:1px}
.league-section{margin-bottom:16px}
.league-section-hdr{font-size:11px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.7px;margin-bottom:8px;padding-bottom:6px;border-bottom:1px solid #e5e7eb}
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
.fw{display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:4px;font-size:9px;font-weight:700;background:#dcfce7;color:#166634}
.fd{display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:4px;font-size:9px;font-weight:700;background:#f3f4f6;color:#6b7280;border:1px solid #e5e7eb}
.fl{display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:4px;font-size:9px;font-weight:700;background:#fee2e2;color:#b91c1c}
.stat-grid{display:grid;grid-template-columns:1fr 1fr;gap:4px}
.chip{background:#fff;border:0.5px solid #e5e7eb;border-radius:6px;padding:5px 6px}
.chip-lbl{font-size:7px;color:#9ca3af;text-transform:uppercase;letter-spacing:.3px;margin-bottom:2px}
.chip-val{font-size:13px;font-weight:700;color:#111827}
.chip-thr{font-size:8px;color:#9ca3af;margin-top:1px}
.chip.g-light{background:#e8f5e9;border-color:#a5d6a7}
.chip.g-light .chip-lbl{color:#388e3c}.chip.g-light .chip-val{color:#2e7d32}.chip.g-light .chip-thr{color:#388e3c}
.chip.g-bright{background:#1b5e20;border-color:#1b5e20}
.chip.g-bright .chip-lbl{color:#a5d6a7}.chip.g-bright .chip-val{color:#a5d6a7}.chip.g-bright .chip-thr{color:#a5d6a7}
.chip.r-light{background:#fef2f2;border-color:#fca5a5}
.chip.r-light .chip-lbl{color:#b91c1c}.chip.r-light .chip-val{color:#991b1b}.chip.r-light .chip-thr{color:#b91c1c}
.chip.r-bright{background:#b71c1c;border-color:#b71c1c}
.chip.r-bright .chip-lbl{color:#ef9a9a}.chip.r-bright .chip-val{color:#fff}.chip.r-bright .chip-thr{color:#ef9a9a}
.signals{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:10px}
.sig{display:flex;align-items:center;gap:4px;font-size:10px;padding:3px 8px;border-radius:20px;font-weight:500;border:1px solid}
.sig-y{background:#f0fdf4;color:#15803d;border-color:#a5d6a7}
.sig-n{background:#fef2f2;color:#b91c1c;border-color:#fca5a5}
.sig-dot{width:5px;height:5px;border-radius:50%;flex-shrink:0}
.sig-y .sig-dot{background:#16a34a} .sig-n .sig-dot{background:#dc2626}
.ci-bar{border-radius:8px;padding:9px 12px;font-size:11px;font-family:monospace;margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px}
.ci-cold{background:#f9fafb;border:0.5px solid #e5e7eb;color:#374151}
.ci-light{background:#e8f5e9;border:1px solid #a5d6a7;color:#2e7d32}
.ci-bright{background:#1b5e20;border:1.5px solid #388e3c;color:#a5d6a7}
.result-box{border-radius:10px;overflow:hidden;margin-bottom:12px;display:flex;flex-wrap:wrap;border:1px solid #e5e7eb}
.res-cell{padding:9px 12px;text-align:center;flex:1;min-width:70px}
.toggle-btn{font-size:12px;color:#6b7280;cursor:pointer;padding-top:11px;display:flex;align-items:center;gap:5px;border-top:1px solid #f3f4f6;margin-top:4px}
.details{display:none;padding-top:10px}
.details.open{display:block}
.form-wrap{border-top:1px solid #f3f4f6;padding-top:10px}
.form-team-lbl{font-size:10px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px}
.tbl-scroll{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}
.ftable{width:100%;border-collapse:collapse;font-size:11px;margin-bottom:6px}
.ftable th{background:#f9fafb;padding:4px 5px;text-align:left;font-size:9px;color:#6b7280;font-weight:600;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid #e5e7eb;white-space:nowrap}
.ftable td{padding:4px 5px;border-bottom:1px solid #f9fafb;white-space:nowrap}
.ftable tr:last-child td{border-bottom:none}
.ftable tfoot td{background:#f9fafb;font-weight:600;font-size:10px;padding:4px 5px;border-top:1px solid #e5e7eb}
.fw2{color:#15803d;font-weight:700} .fl2{color:#b91c1c;font-weight:700} .fd2{color:#6b7280;font-weight:700}
@media(max-width:480px){
  .hdr-title{font-size:17px}.rn{font-size:20px}.team-name{font-size:12px}
  .sig{font-size:9px;padding:2px 6px}.ci-bar{font-size:10px}
  .ftable{font-size:9px;table-layout:fixed;width:100%}.ftable th,.ftable td{padding:2px 2px;overflow:hidden;text-overflow:ellipsis}
}
@media(min-width:640px){
  .hdr{padding:14px 20px}.body{padding:16px 20px}.card-inner{padding:16px}
  .hdr-title{font-size:22px}.tab{font-size:13px;padding:6px 14px}
  .sig{font-size:11px;padding:4px 9px}.ftable{font-size:12px}.ftable th{font-size:10px}
}`.trim();

  let J = "";
  J += "var ALL=[];";
  J += "var DATES=" + JSON.stringify(dates) + ";";
  J += "var TZ=" + JSON.stringify(tzOffset) + ";";
  J += "var DAY_LABELS=['Today','Tomorrow','Day 3','Day 4','Day 5'];";
  J += "var activeDate=DATES[0]||null;";
  J += "var openLeague=null;";

  J += "function esc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;');}";
  J += "function fmtDate(d){return new Date(d).toLocaleDateString('en-GB',{weekday:'long',day:'2-digit',month:'short'});}";
  J += "function rankAccent(r){return r===4?'#2e7d32':r===3?'#558b2f':r===2?'#f9a825':r===1?'#ef6c00':'#9e9e9e';}";
  J += "function rankCls(r){return r===4?'r4':r===3?'r3':r===2?'r2':r===1?'r1':'r0';}";
  J += "function lgLabel(r){return r===4?'Fire \ud83d\udd25':r===3?'Prime \u26a1':r===2?'Watch \ud83d\udc40':r===1?'Signal \ud83d\udce1':'Low';}";
  J += "function fLetter(r){return r==='W'?'<span class=\"fw\">W</span>':r==='L'?'<span class=\"fl\">L</span>':'<span class=\"fd\">D</span>';}";
  J += "function mkChip(lbl,val,thr,state){var cls='chip'+(state?' '+state:'');return '<div class=\"'+cls+'\">'+'<div class=\"chip-lbl\">'+esc(lbl)+'</div><div class=\"chip-val\">'+esc(val)+'</div><div class=\"chip-thr\">'+esc(thr)+'</div></div>';}";

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
  J += "    activeDate=DATES[i];openLeague=null;renderTabs();renderLeagueList();";
  J += "    document.getElementById('hdrTitle').textContent=fmtDate(new Date(DATES[i]+'T12:00:00'));";
  J += "  });});";
  J += "}";

  // renderLeagueList — accordion: only one league open at a time
  J += "function renderLeagueList(){";
  J += "  var main=document.getElementById('mainView');";
  J += "  if(!activeDate){main.innerHTML='';return;}";
  J += "  var dp=ALL.filter(function(p){return p.matchDate===activeDate;});";
  J += "  var lmap={};dp.forEach(function(p){if(!lmap[p.league])lmap[p.league]=[];lmap[p.league].push(p);});";
  J += "  var ll=Object.entries(lmap).sort(function(a,b){return a[0].localeCompare(b[0]);});";
  J += "  if(!ll.length){main.innerHTML='<p style=\"color:#6b7280;text-align:center;padding:40px\">No matches found.</p>';return;}";
  J += "  var pills='<div class=\"pill-bar\">';";
  J += "  ll.forEach(function(e){";
  J += "    var lg=e[0],ms=e[1];";
  J += "    var tr=Math.max.apply(null,ms.map(function(p){return p.rank;}));";
  J += "    var en=ms.filter(function(p){return p.eligible;}).length;";
  J += "    var isOpen=openLeague===lg;";
  J += "    var pillCls='lpill'+(isOpen?' open':'');";
  J += "    var dotBg=isOpen?'#ff6b35':rankAccent(tr);";
  J += "    var lgParts=lg.split(' \u00b7 ');";
  J += "    var lgCountry=lgParts.length>1?lgParts[0]:'';";
  J += "    var lgName=lgParts[lgParts.length-1];";
  J += "    var lgShort=lgCountry?'<span style=\"opacity:.55;font-size:10px;margin-right:2px\">'+esc(lgCountry)+'</span>'+esc(lgName):esc(lgName);";
  J += "    var badge=en?'<span class=\"lpill-badge\">('+en+'\u2605)</span>':'<span class=\"lpill-badge\">'+ms.length+'</span>';";
  J += "    pills+='<button class=\"'+pillCls+'\" data-lg=\"'+esc(lg)+'\"><span class=\"lpill-dot\" style=\"background:'+dotBg+'\"></span>'+lgShort+badge+'</button>';";
  J += "  });";
  J += "  pills+='</div>';";
  J += "  var sections='';";
  J += "  ll.forEach(function(e){";
  J += "    var lg=e[0],ms=e[1];";
  J += "    if(openLeague!==lg)return;";
  J += "    var sorted=ms.slice().sort(function(a,b){return (b.ci-a.ci)||((a.dt||0)-(b.dt||0));});";
  J += "    sections+='<div class=\"league-section\"><div class=\"league-section-hdr\">'+esc(lg)+'</div>';";
  J += "    sorted.forEach(function(m){sections+=renderCard(m);});";
  J += "    sections+='</div>';";
  J += "  });";
  J += "  main.innerHTML=pills+sections;";
  // FIX 2: clicking a pill closes all others, toggles itself
  J += "  main.querySelectorAll('.lpill').forEach(function(btn){btn.addEventListener('click',function(){";
  J += "    var lg=btn.getAttribute('data-lg');";
  J += "    openLeague=(openLeague===lg)?null:lg;";
  J += "    renderLeagueList();";
  J += "  });});";
  J += "  document.querySelectorAll('.toggle-btn').forEach(function(btn){btn.addEventListener('click',function(){";
  J += "    var d=btn.nextElementSibling;var o=d.classList.toggle('open');";
  J += "    btn.innerHTML=o?'\u25b2 Hide last 5 games':'\u25bc Show last 5 games';";
  J += "  });});";
  J += "  if(openLeague){var oms=lmap[openLeague]||[];oms.forEach(function(m){renderForm(m.id,m.homeId,m.home,m.hLast5,m.hAvgFH);renderForm(m.id,m.awayId,m.away,m.aLast5,m.aAvgFH);});}";
  J += "}";

  // renderForm — FIX 1: mobile-friendly, no min-width, FT hidden on small screens
  J += "function renderForm(mid,tid,tname,games,avgFH){";
  J += "  var el=document.getElementById('form-'+mid+'-'+tid);if(!el)return;";
  J += "  if(!games||!games.length){el.innerHTML='';return;}";
  J += "  var rows=games.map(function(g){";
  J += "    var fhTotal=g.fhFor+g.fhAgst;";
  J += "    var fhStyle=fhTotal>2?'color:#15803d;font-weight:600':fhTotal===2?'color:#1d4ed8;font-weight:600':'color:#111827';";
  J += "    var rc=g.result==='W'?'fw2':g.result==='L'?'fl2':'fd2';";
  J += "    var shortDate=g.date?g.date.slice(5):'';";
  J += "    return '<tr>'";
  J += "      +'<td>'+esc(shortDate)+'</td>'";
  J += "      +'<td style=\"overflow:hidden;text-overflow:ellipsis;max-width:100px\">'+esc(g.opp)+'</td>'";
  J += "      +'<td style=\"text-align:center;color:#9ca3af\">'+esc(g.venue)+'</td>'";
  J += "      +'<td style=\"text-align:center;'+fhStyle+'\">'+g.fhFor+'-'+g.fhAgst+'</td>'";
  J += "      +'<td style=\"text-align:center;color:#9ca3af\">'+g.ftFor+'-'+g.ftAgst+'</td>'";
  J += "      +'<td style=\"text-align:center\" class=\"'+rc+'\">'+g.result+'</td>'";
  J += "    +'</tr>';";
  J += "  }).join('');";
  J += "  var foot='';";
  J += "  if(avgFH!==null&&avgFH!==undefined){foot='<tfoot><tr>'";
  J += "    +'<td colspan=\"3\" style=\"color:#6b7280\">Avg FH goals</td>'";
  J += "    +'<td style=\"text-align:center;font-family:monospace;color:#1d4ed8;font-size:12px\">'+avgFH+'</td>'";
  J += "    +'<td colspan=\"2\" style=\"text-align:center;color:#9ca3af\">('+games.length+')</td>'";
  J += "    +'</tr></tfoot>';}";
  J += "  el.innerHTML='<div class=\"form-team-lbl\">'+esc(tname)+' \u2014 last 5</div>'";
  J += "    +'<div class=\"tbl-scroll\"><table class=\"ftable\"><thead><tr>'";
  J += "    +'<th style=\"width:14%\">Date</th><th style=\"width:36%\">Opponent</th>'";
  J += "    +'<th style=\"width:10%;text-align:center\">H/A</th>'";
  J += "    +'<th style=\"width:12%;text-align:center\">FH</th>'";
  J += "    +'<th style=\"width:12%;text-align:center\">FT</th>'";
  J += "    +'<th style=\"width:10%;text-align:center\">Res</th>'";
  J += "    +'</tr></thead><tbody>'+rows+'</tbody>'+foot+'</table></div>';";
  J += "}";

  J += "function renderCard(m){";
  J += "  var accent=rankAccent(m.rank);var rc=rankCls(m.rank);";
  J += "  var dt=m.dt?new Date(m.dt).toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}):m.matchDate;";
  J += "  var sb=m.status==='complete'?'<span style=\"background:#f3f4f6;color:#6b7280;border:1px solid #e5e7eb;font-size:10px;padding:2px 8px;border-radius:20px;font-weight:600\">Final</span>'";
  J += "    :m.status==='live'?'<span style=\"background:#fef9c3;color:#ca8a04;border:1px solid #fde047;font-size:10px;padding:2px 8px;border-radius:20px;font-weight:600\">\u25cf Live</span>'";
  J += "    :'<span style=\"background:#f0fdf4;color:#15803d;border:1px solid #a5d6a7;font-size:10px;padding:2px 8px;border-radius:20px;font-weight:600\">Upcoming</span>';";
  J += "  var ps='<div class=\"prob-strip\">'";
  J += "    +'<div class=\"pp pp15\"><div class=\"pp-dot\"></div><span class=\"pp-lbl\">FH over 1.5</span><span class=\"pp-val\">'+m.prob15+'%</span></div>'";
  J += "    +'<div class=\"pp pp25\"><div class=\"pp-dot\"></div><span class=\"pp-lbl\">FH over 2.5</span><span class=\"pp-val\">'+m.prob25+'%</span></div>'";
  J += "    +'</div>';";
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
  J += "        +'<div style=\"font-size:12px;font-weight:700;color:'+c15+'\">'+( r.hit15?'\u2713 HIT':'\u2717 MISS')+'</div>'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;margin-top:1px\">'+m.prob15+'% pre</div>'";
  J += "      +'</div>'";
  J += "      +'<div class=\"res-cell\" style=\"background:'+bg25+';border-right:1px solid '+bc25+'\">'";
  J += "        +'<div style=\"font-size:9px;text-transform:uppercase;letter-spacing:.6px;margin-bottom:2px;color:'+c25+'\">FH over 2.5</div>'";
  J += "        +'<div style=\"font-size:12px;font-weight:700;color:'+c25+'\">'+( r.hit25?'\u2713 HIT':'\u2717 MISS')+'</div>'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;margin-top:1px\">'+m.prob25+'% pre</div>'";
  J += "      +'</div>'";
  J += "      +'<div class=\"res-cell\" style=\"background:#f9fafb\">'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:2px\">Full Time</div>'";
  J += "        +'<div style=\"font-family:monospace;font-weight:700;font-size:16px;color:#374151;line-height:1\">'+r.ftH+'\u2013'+r.ftA+'</div>'";
  J += "      +'</div>'";
  J += "    +'</div>';";
  J += "  }";
  J += "  function teamBox(role,name,tid,last5,sn){";
  J += "    var fs='<div class=\"form-strip\"><span class=\"form-lbl\">Form</span>';";
  J += "    if(last5&&last5.length)last5.forEach(function(g){fs+=fLetter(g.result);});";
  J += "    else fs+='<span style=\"font-size:10px;color:#9ca3af\">...</span>';";
  J += "    fs+='</div>';";
  J += "    var chips='';";
  J += "    if(sn){";
  J += "      var sfx=role==='Home'?'(home)':'(away)';";
  J += "      var scState=sn.scored_fh>=1.5?'g-bright':sn.scored_fh>=1.0?'g-light':'';" ;
  J += "      chips+=mkChip('FH Scored '+sfx,sn.scored_fh.toFixed(2),'away \u2265 1.25 \u2192 sig D',scState);";
  J += "      var coState=sn.conced_fh>=1.1?'r-bright':sn.conced_fh>=0.7?'r-light':'';" ;
  J += "      chips+=mkChip('FH Conceded '+sfx,sn.conced_fh.toFixed(2),'\u2265 2.25 combined \u2192 sig C',coState);";
  J += "      var t1State=sn.t1_pct>=25?'g-bright':sn.t1_pct>=15?'g-light':'';" ;
  J += "      chips+=mkChip('FH>2.5 hist '+sfx,sn.t1_pct.toFixed(0)+'%','\u2265 25% \u2192 sig B',t1State);";
  J += "      var cnState=sn.cn010_avg>=0.25?'r-light':'';" ;
  J += "      chips+=mkChip('Early conceded',sn.cn010_avg.toFixed(2),'info only',cnState);";
  J += "      if(sn.sot_avg>0){var sotState=sn.sot_avg>=4?'g-bright':sn.sot_avg>=2.5?'g-light':'';chips+=mkChip('Shots on target '+sfx,(sn.sot_avg).toFixed(1)+'/g','per game',sotState);}";
  J += "    }";
  J += "    return '<div class=\"team-box\"><div class=\"team-role\">'+esc(role)+'</div><div class=\"team-name\">'+esc(name)+'</div>'+fs+'<div class=\"stat-grid\">'+chips+'</div></div>';";
  J += "  }";
  J += "  var sigsH='<div class=\"signals\">';";
  J += "  ['A','B','C','D'].forEach(function(k){";
  J += "    var s=m.signals[k];if(!s)return;";
  J += "    sigsH+='<div class=\"sig '+(s.met?'sig-y':'sig-n')+'\">'";
  J += "      +'<div class=\"sig-dot\"></div>'+esc(k)+' \u00b7 '+esc(s.label)";
  J += "      +'<span style=\"opacity:.7;margin-left:4px;font-size:9px\">('+esc(s.value)+')</span>'";
  J += "      +'</div>';";
  J += "  });";
  J += "  sigsH+='</div>';";
  J += "  var ciH='';";
  J += "  if(m.snap){";
  J += "    var h=m.snap.home,a=m.snap.away;";
  J += "    var defCiVal=m.defCi||0;";
  J += "    var ciCls=m.ci>=3.2?'ci-bar ci-bright':m.ci>=2.8?'ci-bar ci-light':'ci-bar ci-cold';";
  J += "    var ciValCol=m.ci>=3.2?'#69f0ae':m.ci>=2.8?'#2e7d32':'#111827';";
  J += "    var ciCheck=m.ci>=3.2?'\u2713':m.ci>=2.8?'\u25d1':'\u2717';";
  J += "    var defCiCol=defCiVal>=2.25?'#69f0ae':defCiVal>=1.5?'#a5d6a7':'inherit';";
  J += "    ciH='<div class=\"'+ciCls+'\">'";
  J += "      +'<span>'+h.scored_fh.toFixed(2)+' + '+a.scored_fh.toFixed(2)+' + '+h.conced_fh.toFixed(2)+' + '+a.conced_fh.toFixed(2)+'</span>'";
  J += "      +'<span style=\"font-size:18px;font-weight:700;color:'+ciValCol+'\">'+m.ci+' '+ciCheck+'</span>'";
  J += "    +'</div>'";
  J += "    +'<div style=\"font-size:10px;font-family:monospace;padding:4px 12px 8px;color:#6b7280\">'";
  J += "      +'DefCI: <span style=\"font-weight:700;color:'+defCiCol+'\">'+defCiVal.toFixed(2)+'</span>'";
  J += "      +' (conceded sum \u2265 2.25 \u2192 sig C) \u00b7 Away scored: <span style=\"font-weight:700;color:'+(a.scored_fh>=1.25?'#2e7d32':'#6b7280')+'\">'+a.scored_fh.toFixed(2)+'</span>'";
  J += "      +' (\u2265 1.25 \u2192 sig D)'";
  J += "    +'</div>';";
  J += "  }";
  J += "  var mw=m.missingStats?'<span style=\"background:#fef3c7;color:#92400e;font-size:10px;padding:2px 6px;border-radius:4px;font-weight:600;margin-left:6px\">\u26a0 no stats</span>':'';";
  J += "  return '<div class=\"card\">'";
  J += "    +'<div class=\"card-accent\" style=\"background:'+accent+'\"></div>'";
  J += "    +'<div class=\"card-inner\">'";
  J += "      +'<div style=\"display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin-bottom:12px\">'";
  J += "        +'<div style=\"min-width:0\">'";
  J += "          +'<div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:5px\">'+esc(m.league)+' \u00b7 '+esc(dt)+mw+'</div>'";
  J += "          +sb";
  J += "        +'</div>'";
  J += "        +'<div class=\"rank-pill '+rc+'\"><div class=\"rn\">'+m.rank+'/4</div><div class=\"rl\">'+esc(m.label)+'</div></div>'";
  J += "      +'</div>'";
  J += "    +ps+rb";
  J += "    +'<div class=\"teams\">'";
  J += "    +teamBox('Home',m.home,m.homeId,m.hLast5,m.snap?m.snap.home:null)";
  J += "    +teamBox('Away',m.away,m.awayId,m.aLast5,m.snap?m.snap.away:null)";
  J += "    +'</div>'";
  J += "    +sigsH+ciH";
  J += "    +(m.hLast5&&m.hLast5.length||m.aLast5&&m.aLast5.length?";
  J += "      '<div class=\"toggle-btn\">\u25bc Show last 5 games</div>'";
  J += "      +'<div class=\"details\">'";
  J += "        +'<div class=\"form-wrap\">'";
  J += "          +'<div id=\"form-'+m.id+'-'+m.homeId+'\"><div style=\"font-size:11px;color:#9ca3af\">\u21bb Loading '+esc(m.home)+'...</div></div>'";
  J += "          +'<div id=\"form-'+m.id+'-'+m.awayId+'\" style=\"margin-top:6px\"><div style=\"font-size:11px;color:#9ca3af\">\u21bb Loading '+esc(m.away)+'...</div></div>'";
  J += "        +'</div>'";
  J += "      +'</div>'";
  J += "    :'')";
  J += "  +'</div></div>';";
  J += "}";

  J += "if(DATES.length)document.getElementById('hdrTitle').textContent=fmtDate(new Date(DATES[0]+'T12:00:00'));";
  J += "renderTabs();renderLeagueList();";
  J += "(function loadPreds(){";
  J += "  var main=document.getElementById('mainView');";
  J += "  if(ALL.length===0&&main)main.innerHTML='<p style=\"color:#6b7280;text-align:center;padding:40px;font-size:13px\">\u23f3 Loading predictions\u2026</p>';";
  J += "  fetch('/preds?tz='+TZ).then(function(r){return r.json();}).then(function(d){";
  J += "    if(d.ok&&d.preds&&d.preds.length){ALL=d.preds;renderTabs();renderLeagueList();}";
  J += "    else setTimeout(loadPreds,3000);";
  J += "  }).catch(function(){setTimeout(loadPreds,4000);});";
  J += "})();";

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
  ${rateLimited ? '<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:12px;color:#b91c1c;line-height:1.6"><strong>&#9888; API rate limit reached</strong> \u2014 showing cached data. Resets at ' + new Date(RATE_LIMITED_UNTIL).toLocaleTimeString('en-GB') + '. <a href="/cache-status" style="color:#b91c1c">View cache status</a></div>' : ''}
  <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:12px;color:#92400e;line-height:1.6">
    <strong>How it works:</strong> 4 stats-only signals (A&ndash;D), each worth 1 point. Rank = signals fired.
    A: CI&ge;3.2 &middot; B: Both teams FH&gt;2.5 history&ge;25% &middot; C: Defence CI&ge;2.25 &middot; D: Away FH scored&ge;1.25.
    Backtested on 22,967 matches &middot; base rate 12.8% &middot; Rank 4 = 87.5% FH&gt;2.5 &middot; Rank 3 = 62.0% FH&gt;2.5.
  </div>
  <div id="mainView"></div>
</div>
<script>${J}<\/script>
</body>
</html>`;
}

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
