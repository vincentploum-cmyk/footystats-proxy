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
  console.error("Missing FOOTY_API_KEY in environment variables.");
  process.exit(1);
}

const LEAGUE_NAMES = {
  16504: "USA MLS",
  15000: "Scotland Premiership",
  14968: "Germany Bundesliga",
  14924: "UEFA Champions League",
  15050: "England Premier League",
  14930: "England Championship",
  14956: "Spain La Liga",
  16558: "Norway Eliteserien",
  14932: "France Ligue 1",
  15068: "Italy Serie A",
  14931: "Germany 2. Bundesliga",
  14923: "Austria Bundesliga",
  16036: "Australia A-League",
  16544: "Brazil Serie A",
  16571: "Argentina Primera Division",
  15047: "Switzerland Super League",
  16242: "Japan J1 League",
  15234: "Mexico Liga MX",
  16614: "Colombia Primera A",
  16615: "Chile Primera Division",
  15055: "Denmark Superliga",
  16714: "Ecuador Serie A",
  16708: "Uruguay Primera Division",
  15002: "UEFA Europa League",
  15238: "England FA Cup",
  10117: "WC Qual Asia",
  12061: "WC Qual Africa",
  11084: "UEFA Euro Championship",
  9128: "UEFA Euro Qualifiers",
  16808: "UEFA Nations League",
  10121: "WC Qual South America",
  1425: "FIFA World Cup 2018",
  15020: "Mexico Liga MX Femenil",
  8994: "Asia Womens Olympic",
  16823: "CONCACAF Champions League",
  16046: "UEFA Womens CL",
  11426: "WC Qual CONCACAF",
  6704: "Womens WC Qual Oceania",
  14904: "UEFA Conference League",
  12980: "CONCACAF Nations League",
  16494: "FIFA World Cup",
  7977: "CONCACAF League",
  12801: "WC Qual Oceania",
  16562: "CONCACAF Gold Cup Qual",
  16037: "Australia A-League Women",
  13861: "UEFA Womens Nations League",
  16563: "Womens WC Qual Europe"
};

// ── Config ────────────────────────────────────────────────────────────────────
const CACHE_TTL_MS = 20 * 60 * 1000;
const FETCH_TIMEOUT_MS = 12000;
const MAX_RETRIES = 2;

// ── Cache + de-dupe ───────────────────────────────────────────────────────────
const responseCache = new Map();
const inflight = new Map();

function maskKey(url) {
  return url.replace(/key=[^&]+/, "key=***");
}

function cacheGet(key) {
  const item = responseCache.get(key);
  if (!item) return null;
  if (Date.now() - item.ts > CACHE_TTL_MS) {
    responseCache.delete(key);
    return null;
  }
  return item.val;
}

function cacheSet(key, val) {
  responseCache.set(key, { ts: Date.now(), val });
  return val;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJsonWithTimeout(url, timeoutMs = FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) throw new Error(`Upstream ${res.status} ${res.statusText}`);
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
}

async function ftch(url) {
  const cached = cacheGet(url);
  if (cached) {
    console.log("CACHE:", maskKey(url));
    return cached;
  }

  if (inflight.has(url)) {
    console.log("WAIT :", maskKey(url));
    return inflight.get(url);
  }

  const promise = (async () => {
    let lastErr;
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        const data = await fetchJsonWithTimeout(url);
        cacheSet(url, data);
        console.log("API  :", maskKey(url));
        return data;
      } catch (err) {
        lastErr = err;
        if (attempt < MAX_RETRIES) {
          await sleep(500 * (attempt + 1));
        }
      }
    }
    throw lastErr;
  })();

  inflight.set(url, promise);

  try {
    return await promise;
  } finally {
    inflight.delete(url);
  }
}

// ── Utilities ─────────────────────────────────────────────────────────────────
const safe = (v) => (v == null || !isFinite(v) || isNaN(v) ? 0 : +v);

function clamp(x, min, max) {
  return Math.max(min, Math.min(max, x));
}

function round2(x) {
  return +safe(x).toFixed(2);
}

function avg(arr) {
  if (!arr.length) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function teamKey(name) {
  return (name || "").toLowerCase().trim();
}

function getDates(tzOffset = 0) {
  const now = new Date();
  const local = new Date(now.getTime() + tzOffset * 60 * 1000);

  const fmt = (d) => {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const day = String(d.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
  };

  const dates = [];
  for (let i = 0; i < 6; i++) {
    const d = new Date(local);
    d.setUTCDate(local.getUTCDate() + i);
    dates.push(fmt(d));
  }

  return [...new Set(dates)];
}

function unixToLocalDate(unix, tzOffset) {
  if (!unix) return null;
  const d = new Date(unix * 1000 + tzOffset * 60 * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function unwrapTeam(t) {
  if (!t) return null;
  if (t.stats && typeof t.stats === "object") return { ...t, ...t.stats };
  return t;
}

function getFixtureId(m) {
  return String(m?.id ?? m?.match_id ?? m?.fixture_id ?? m?.game_id ?? "");
}

function isSameFixture(a, b) {
  const aId = getFixtureId(a);
  const bId = getFixtureId(b);

  if (aId && bId) return aId === bId;

  return (
    teamKey(a.home_name) === teamKey(b.home_name) &&
    teamKey(a.away_name) === teamKey(b.away_name) &&
    safe(a.date_unix) === safe(b.date_unix)
  );
}

// ── Simplified FH model ───────────────────────────────────────────────────────
function calcSeasonEnv(ht, at) {
  return round2(
    safe(ht.scoredAVGHT_home) +
      safe(ht.concededAVGHT_home) +
      safe(at.scoredAVGHT_away) +
      safe(at.concededAVGHT_away)
  );
}

function calcRecentEnv(hLast5, aLast5) {
  const h = avg(hLast5.map((g) => g.htFor + g.htAgainst));
  const a = avg(aLast5.map((g) => g.htFor + g.htAgainst));
  return {
    home: round2(h),
    away: round2(a),
    val: round2((h + a) / 2)
  };
}

function calcVenueEnv(hHome5, aAway5) {
  const h = avg(hHome5.map((g) => g.htFor + g.htAgainst));
  const a = avg(aAway5.map((g) => g.htFor + g.htAgainst));
  return {
    home: round2(h),
    away: round2(a),
    val: round2((h + a) / 2)
  };
}

function calcH2HEnv(h2h) {
  if (!h2h.length) {
    return { val: null, count: 0 };
  }

  const vals = h2h.map((m) => {
    const homeHT = parseInt(m.ht_goals_team_a || 0, 10);
    const awayHT = parseInt(m.ht_goals_team_b || 0, 10);
    return homeHT + awayHT;
  });

  return {
    val: round2(avg(vals)),
    count: h2h.length
  };
}

function computeCompositeScore({ seasonEnv, recentEnv, venueEnv, h2hEnv }) {
  const seasonPart = clamp(seasonEnv / 3.2, 0, 1.2);
  const recentPart = clamp(recentEnv.val / 2.0, 0, 1.2);
  const venuePart = clamp(venueEnv.val / 2.0, 0, 1.2);

  let score;
  if (h2hEnv.val != null) {
    const h2hPart = clamp(h2hEnv.val / 2.0, 0, 1.2);
    score =
      seasonPart * 0.4 +
      recentPart * 0.3 +
      venuePart * 0.2 +
      h2hPart * 0.1;
  } else {
    score = seasonPart * 0.45 + recentPart * 0.35 + venuePart * 0.2;
  }

  return round2(score);
}

function rawToRank(raw) {
  if (raw >= 1.0) return 5;
  if (raw >= 0.86) return 4;
  if (raw >= 0.72) return 3;
  if (raw >= 0.58) return 2;
  return 1;
}

function rankLabel(rank) {
  if (rank === 5) return "Elite";
  if (rank === 4) return "Strong";
  if (rank === 3) return "Borderline";
  if (rank === 2) return "Weak";
  return "Avoid";
}

function isEligible(rank) {
  return rank >= 4;
}

// ── Data loaders ──────────────────────────────────────────────────────────────
async function fetchTodayFixtures(dates) {
  const dayResults = await Promise.all(
    dates.map((d) => ftch(`${BASE}/todays-matches?date=${d}&key=${KEY}`))
  );

  const allFixtures = [];
  for (let i = 0; i < dates.length; i++) {
    for (const m of dayResults[i].data || []) {
      allFixtures.push({ ...m, _date: dates[i] });
    }
  }

  return allFixtures;
}

function groupFixturesByLeague(allFixtures) {
  const leagueFixtures = {};
  const leagueNameOverride = {};

  for (const m of allFixtures) {
    const sid = parseInt(m.competition_id, 10);
    if (!sid) continue;

    if (!leagueFixtures[sid]) leagueFixtures[sid] = [];
    leagueFixtures[sid].push(m);

    if (!leagueNameOverride[sid]) {
      leagueNameOverride[sid] =
        LEAGUE_NAMES[sid] ||
        m.league ||
        m.competition ||
        m.competition_name ||
        `League ${sid}`;
    }
  }

  return { leagueFixtures, leagueNameOverride };
}

async function loadLeagueTeams(sid) {
  const r = await ftch(
    `${BASE}/league-teams?season_id=${sid}&include=stats&key=${KEY}`
  );

  const teamMap = {};
  for (const t of r.data || []) {
    if (t.id != null) {
      teamMap[t.id] = t;
      teamMap[String(t.id)] = t;
    }
    teamMap[`__name__${teamKey(t.name)}`] = t;
    if (t.clean_name) {
      teamMap[`__name__${teamKey(t.clean_name)}`] = t;
    }
  }

  return teamMap;
}

async function loadLeagueHistory(sid) {
  const lmRes = await ftch(
    `${BASE}/league-matches?season_id=${sid}&max_per_page=150&page=1&key=${KEY}`
  );

  const teamHistory = {};
  const completedMatches = (lmRes.data || []).filter((m) => m.status === "complete");
  completedMatches.sort((a, b) => (b.date_unix || 0) - (a.date_unix || 0));

  for (const m of completedMatches) {
    const addGame = (teamName, isHome) => {
      const key = teamKey(teamName);
      if (!key) return;
      if (!teamHistory[key]) teamHistory[key] = [];

      teamHistory[key].push({
        opp: isHome ? m.away_name : m.home_name,
        venue: isHome ? "H" : "A",
        htFor: isHome
          ? parseInt(m.ht_goals_team_a || 0, 10)
          : parseInt(m.ht_goals_team_b || 0, 10),
        htAgainst: isHome
          ? parseInt(m.ht_goals_team_b || 0, 10)
          : parseInt(m.ht_goals_team_a || 0, 10),
        ftFor: isHome
          ? parseInt(m.homeGoalCount || 0, 10)
          : parseInt(m.awayGoalCount || 0, 10),
        ftAgainst: isHome
          ? parseInt(m.awayGoalCount || 0, 10)
          : parseInt(m.homeGoalCount || 0, 10),
        date_unix: safe(m.date_unix),
        date: m.date_unix
          ? new Date(m.date_unix * 1000).toISOString().slice(0, 10)
          : ""
      });
    };

    if (m.home_name) addGame(m.home_name, true);
    if (m.away_name) addGame(m.away_name, false);
  }

  return { teamHistory, completedMatches };
}

// ── Pre-match freeze helpers ──────────────────────────────────────────────────
function filterHistoryBeforeFixture(teamHistory, fixture) {
  const cutoff = safe(fixture.date_unix);
  const filtered = {};

  for (const [team, games] of Object.entries(teamHistory || {})) {
    filtered[team] = games.filter((g) => {
      const gameUnix = safe(g.date_unix);
      return gameUnix && gameUnix < cutoff;
    });
  }

  return filtered;
}

function filterCompletedMatchesBeforeFixture(completedMatches, fixture) {
  const cutoff = safe(fixture.date_unix);

  return (completedMatches || []).filter((m) => {
    const gameUnix = safe(m.date_unix);
    if (!gameUnix || gameUnix >= cutoff) return false;
    if (isSameFixture(m, fixture)) return false;
    return true;
  });
}

function getLast5(teamHistory, teamName) {
  return (teamHistory[teamKey(teamName)] || []).slice(0, 5);
}

function getLastNByVenue(teamHistory, teamName, venue, n = 5) {
  return (teamHistory[teamKey(teamName)] || [])
    .filter((g) => g.venue === venue)
    .slice(0, n);
}

function getH2HMatches(completedMatches, homeName, awayName, limit = 5) {
  const hk = teamKey(homeName);
  const ak = teamKey(awayName);

  return (completedMatches || [])
    .filter((m) => {
      const mh = teamKey(m.home_name);
      const ma = teamKey(m.away_name);
      return (mh === hk && ma === ak) || (mh === ak && ma === hk);
    })
    .slice(0, limit);
}

// ── Match prediction builder ──────────────────────────────────────────────────
function buildPredictionForFixture(
  fixture,
  leagueName,
  teamMap,
  teamHistory,
  completedMatches,
  tzOffset
) {
  const homeId = String(fixture.homeID || fixture.home_id || "");
  const awayId = String(fixture.awayID || fixture.away_id || "");

  const htRaw =
    teamMap[homeId] ||
    teamMap[parseInt(homeId, 10)] ||
    teamMap[`__name__${teamKey(fixture.home_name)}`];

  const atRaw =
    teamMap[awayId] ||
    teamMap[parseInt(awayId, 10)] ||
    teamMap[`__name__${teamKey(fixture.away_name)}`];

  const ht = unwrapTeam(htRaw) || {};
  const at = unwrapTeam(atRaw) || {};

  // Freeze all history inputs to pre-match only.
  const preMatchTeamHistory = filterHistoryBeforeFixture(teamHistory, fixture);
  const preMatchCompletedMatches = filterCompletedMatchesBeforeFixture(
    completedMatches,
    fixture
  );

  const hLast5 = getLast5(preMatchTeamHistory, fixture.home_name || "");
  const aLast5 = getLast5(preMatchTeamHistory, fixture.away_name || "");
  const hHome5 = getLastNByVenue(preMatchTeamHistory, fixture.home_name || "", "H", 5);
  const aAway5 = getLastNByVenue(preMatchTeamHistory, fixture.away_name || "", "A", 5);
  const h2h = getH2HMatches(
    preMatchCompletedMatches,
    fixture.home_name || "",
    fixture.away_name || "",
    5
  );

  const seasonEnv = calcSeasonEnv(ht, at);
  const recentEnv = calcRecentEnv(hLast5, aLast5);
  const venueEnv = calcVenueEnv(hHome5, aAway5);
  const h2hEnv = calcH2HEnv(h2h);

  const rawScore = computeCompositeScore({
    seasonEnv,
    recentEnv,
    venueEnv,
    h2hEnv
  });

  const rank = rawToRank(rawScore);
  const eligible = isEligible(rank);
  const matchDate = unixToLocalDate(fixture.date_unix, tzOffset) || fixture._date;

  return {
    league: leagueName,
    leagueSid: parseInt(fixture.competition_id, 10),
    dt: (fixture.date_unix || 0) * 1000,
    matchDate,
    home: fixture.home_name,
    away: fixture.away_name,
    rank,
    rankLabel: rankLabel(rank),
    eligible,
    rawScore,
    seasonEnv,
    recentEnv,
    venueEnv,
    h2hEnv,
    missingStats: !htRaw || !atRaw,
    status: fixture.status || "incomplete",
    fhH: parseInt(fixture.ht_goals_team_a || 0, 10),
    fhA: parseInt(fixture.ht_goals_team_b || 0, 10),
    ftH: parseInt(fixture.homeGoalCount || 0, 10),
    ftA: parseInt(fixture.awayGoalCount || 0, 10),
    hLast5,
    aLast5,
    h2h,
    hAvgFH: {
      scoredHome: round2(ht.scoredAVGHT_home),
      concededHome: round2(ht.concededAVGHT_home)
    },
    aAvgFH: {
      scoredAway: round2(at.scoredAVGHT_away),
      concededAway: round2(at.concededAVGHT_away)
    }
  };
}

// ── Small endpoints ───────────────────────────────────────────────────────────
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "footystats-proxy-simple-fh",
    uptimeSec: Math.round(process.uptime()),
    cacheEntries: responseCache.size,
    inflightRequests: inflight.size
  });
});

app.get("/cache-status", (req, res) => {
  const entries = [...responseCache.entries()].map(([url, e]) => ({
    url: maskKey(url),
    ageSec: Math.round((Date.now() - e.ts) / 1000),
    expiresInSec: Math.max(
      0,
      Math.round((CACHE_TTL_MS - (Date.now() - e.ts)) / 1000)
    )
  }));

  res.json({
    ok: true,
    ttlSec: CACHE_TTL_MS / 1000,
    count: entries.length,
    inflightRequests: inflight.size,
    entries
  });
});

app.get("/cache-flush", (req, res) => {
  const count = responseCache.size;
  responseCache.clear();
  res.json({ ok: true, flushed: count });
});

app.get("/api/*", async (req, res) => {
  try {
    const path = req.path.replace("/api", "");
    const qs = new URLSearchParams({ ...req.query, key: KEY }).toString();
    const data = await ftch(`${BASE}${path}?${qs}`);
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Main route ────────────────────────────────────────────────────────────────
app.get("/", async (req, res) => {
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const dates = getDates(tzOffset);

    const allFixtures = await fetchTodayFixtures(dates);
    const { leagueFixtures, leagueNameOverride } = groupFixturesByLeague(allFixtures);
    const leagueIds = Object.keys(leagueFixtures);

    const leagueDataEntries = await Promise.all(
      leagueIds.map(async (sid) => {
        try {
          const [teamMap, historyData] = await Promise.all([
            loadLeagueTeams(sid),
            loadLeagueHistory(sid)
          ]);

          return [
            sid,
            {
              teamMap,
              teamHistory: historyData.teamHistory,
              completedMatches: historyData.completedMatches
            }
          ];
        } catch (err) {
          console.log(`League load error sid=${sid}:`, err.message);
          return [
            sid,
            {
              teamMap: {},
              teamHistory: {},
              completedMatches: [],
              error: err.message
            }
          ];
        }
      })
    );

    const leagueData = Object.fromEntries(leagueDataEntries);
    const preds = [];

    for (const sid of leagueIds) {
      const fixtures = leagueFixtures[sid] || [];
      const leagueName = leagueNameOverride[sid];
      const { teamMap, teamHistory, completedMatches } = leagueData[sid] || {
        teamMap: {},
        teamHistory: {},
        completedMatches: []
      };

      for (const fixture of fixtures) {
        preds.push(
          buildPredictionForFixture(
            fixture,
            leagueName,
            teamMap,
            teamHistory,
            completedMatches,
            tzOffset
          )
        );
      }
    }

    preds.sort(
      (a, b) =>
        b.rank - a.rank ||
        b.rawScore - a.rawScore ||
        Number(b.eligible) - Number(a.eligible)
    );

    res.send(buildHTML(preds, dates));
  } catch (e) {
    console.error(e);
    res.status(500).send(`<pre>Error: ${e.message}\n${e.stack}</pre>`);
  }
});

// ── HTML builder ──────────────────────────────────────────────────────────────
function buildHTML(preds, dates) {
  const predsJSON = JSON.stringify(preds).replace(/</g, "\\u003c");
  const datesJSON = JSON.stringify(dates);

  let H = "";
  H += "<!DOCTYPE html><html><head>";
  H += '<meta charset="UTF-8">';
  H += '<meta name="viewport" content="width=device-width,initial-scale=1">';
  H += "<title>FH Over 2.5 Rank</title>";
  H += "<script>(function(){var p=new URLSearchParams(window.location.search);if(!p.has('tz')){p.set('tz',-new Date().getTimezoneOffset());window.location.search=p.toString();}})();<\/script>";
  H += "<style>";
  H += "*{box-sizing:border-box;margin:0;padding:0}";
  H += "body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;font-size:15px}";
  H += "details>summary::-webkit-details-marker{display:none}";
  H += ".tab{padding:8px 14px;border-radius:6px;cursor:pointer;font-size:14px;font-weight:600;border:1px solid #e5e7eb;background:#fff;color:#6b7280;transition:all .15s}";
  H += ".tab.active{background:#111827;color:#fff;border-color:#111827}";
  H += ".league-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:16px 18px;margin-bottom:10px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;box-shadow:0 1px 3px rgba(0,0,0,.05);transition:box-shadow .15s}";
  H += ".league-card:hover{box-shadow:0 3px 8px rgba(0,0,0,.1)}";
  H += ".back-btn{background:#f3f4f6;border:1px solid #e5e7eb;padding:7px 16px;border-radius:6px;cursor:pointer;font-size:15px;font-weight:600;color:#374151}";
  H += ".match-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:14px}";
  H += ".mini-table{width:100%;border-collapse:collapse;font-size:12px}";
  H += ".mini-table th{background:#f9fafb;padding:6px 8px;text-align:left;font-size:10px;color:#6b7280;font-weight:700;text-transform:uppercase;border-bottom:1px solid #e5e7eb}";
  H += ".mini-table td{padding:7px 8px;border-bottom:1px solid #f3f4f6}";
  H += "</style></head><body>";

  H += '<div style="background:#fff;border-bottom:1px solid #e5e7eb;padding:14px 20px;position:sticky;top:0;z-index:10">';
  H += '<div style="max-width:920px;margin:0 auto">';
  H += '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">';
  H += '<div><div style="font-size:11px;color:#6b7280;letter-spacing:1px;text-transform:uppercase">&#9917; First Half Over 2.5</div>';
  H += '<h1 style="font-size:22px;font-weight:800;color:#111827" id="headerTitle">Loading...</h1></div>';
  H += '<button onclick="location.reload()" style="background:#111827;color:#fff;padding:8px 16px;font-size:14px;border:none;border-radius:6px;font-weight:600;cursor:pointer">&#8635; Refresh</button>';
  H += "</div>";
  H += '<div id="dayTabs" style="display:flex;gap:8px;flex-wrap:wrap"></div>';
  H += "</div></div>";

  H += '<div style="padding:16px 20px;max-width:920px;margin:0 auto">';
  H += '<div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#92400e;line-height:1.6">';
  H += "<strong>Simple model:</strong> every match gets a rank from <strong>1 to 5</strong>. ";
  H += "A rank of <strong>4 or 5</strong> is treated as eligible for first-half over 2.5. ";
  H += "The displayed rank is frozen to <strong>pre-match data only</strong> so you can audit it after the match ends.";
  H += "</div>";
  H += '<div id="mainView"></div></div>';

  H += "<script>";
  H += "var ALL_PREDS=" + predsJSON + ";";
  H += "var DATES=" + datesJSON + ";";
  H += 'var DAY_LABELS=["Today","Tomorrow","Day 3","Day 4","Day 5","Day 6"];';
  H += "var activeDate=DATES[0];";
  H += "var activeLeague=null;";
  H += "function fmt(d){return new Date(d).toLocaleDateString('en-GB',{weekday:'long',day:'2-digit',month:'short'});}";
  H += "function shortName(n){return (n||'').split(' ').slice(0,2).join(' ');}";

  H += "function rankColor(rank){if(rank===5)return '#16a34a';if(rank===4)return '#65a30d';if(rank===3)return '#d97706';if(rank===2)return '#f59e0b';return '#6b7280';}";
  H += "function rankBg(rank){if(rank===5)return '#f0fdf4';if(rank===4)return '#f7fee7';if(rank===3)return '#fffbeb';if(rank===2)return '#fffbeb';return '#f9fafb';}";
  H += "function rankBorder(rank){if(rank===5)return '#bbf7d0';if(rank===4)return '#d9f99d';if(rank===3)return '#fde68a';if(rank===2)return '#fcd34d';return '#e5e7eb';}";

  H += "function renderTabs(){";
  H += "var el=document.getElementById('dayTabs'),html='';";
  H += "for(var i=0;i<DATES.length;i++){";
  H += "var d=DATES[i],count=ALL_PREDS.filter(function(p){return p.matchDate===d;}).length;";
  H += "var cls=d===activeDate?'tab active':'tab';";
  H += "html+='<button class=\"'+cls+'\" onclick=\"selectDay('+i+')\">'+(DAY_LABELS[i]||d)+' <span style=\"font-size:12px;opacity:.7\">('+count+')</span></button>';";
  H += "}";
  H += "el.innerHTML=html;";
  H += "}";

  H += "function selectDay(i){";
  H += "activeDate=DATES[i];";
  H += "activeLeague=null;";
  H += "renderTabs();";
  H += "renderLeagueList();";
  H += "document.getElementById('headerTitle').textContent=fmt(new Date(DATES[i]+'T12:00:00'));";
  H += "}";

  H += "function selectLeague(league){activeLeague=league;renderMatchList();}";
  H += "function backToLeagues(){activeLeague=null;renderLeagueList();}";

  H += "function renderLeagueList(){";
  H += "var dayPreds=ALL_PREDS.filter(function(p){return p.matchDate===activeDate;});";
  H += "var leagueMap={};";
  H += "for(var i=0;i<dayPreds.length;i++){";
  H += "var p=dayPreds[i];";
  H += "if(!leagueMap[p.league])leagueMap[p.league]=[];";
  H += "leagueMap[p.league].push(p);";
  H += "}";
  H += "var leagueList=Object.entries(leagueMap).sort(function(a,b){";
  H += "var aTop=Math.max.apply(null,a[1].map(function(p){return p.rank*100+p.rawScore;}));";
  H += "var bTop=Math.max.apply(null,b[1].map(function(p){return p.rank*100+p.rawScore;}));";
  H += "return bTop-aTop;});";
  H += "if(!leagueList.length){";
  H += "document.getElementById('mainView').innerHTML='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280\">No matches found for this day.</div>';";
  H += "return;}";
  H += "var html='<div style=\"font-size:13px;color:#6b7280;margin-bottom:12px\">'+dayPreds.length+' matches across '+leagueList.length+' leagues &middot; sorted by strongest rank</div>';";
  H += "for(var j=0;j<leagueList.length;j++){";
  H += "var league=leagueList[j][0],matches=leagueList[j][1];";
  H += "var topRank=Math.max.apply(null,matches.map(function(p){return p.rank;}));";
  H += "var eligibleCount=matches.filter(function(p){return p.eligible;}).length;";
  H += "var col=rankColor(topRank);";
  H += "var safeLeague=league.replace(/\\\\/g,'\\\\\\\\').replace(/'/g,\"\\\\'\");";
  H += "html+='<div class=\"league-card\" onclick=\"selectLeague(\\''+safeLeague+'\\')\">';";
  H += "html+='<div style=\"flex:1;min-width:0;margin-right:12px\">';";
  H += "html+='<div style=\"font-size:18px;font-weight:700;color:#111827;margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis\">'+league+'</div>';";
  H += "html+='<div style=\"font-size:13px;color:#6b7280\">'+matches.length+' match'+(matches.length>1?'es':'')+' &middot; '+eligibleCount+' eligible</div>';";
  H += "html+='</div><div style=\"text-align:right;flex-shrink:0\">';";
  H += "html+='<div style=\"font-size:28px;font-weight:800;color:'+col+'\">'+topRank+'/5</div>';";
  H += "html+='<div style=\"font-size:11px;color:#9ca3af;margin-top:1px\">top rank</div>';";
  H += "html+='</div></div>';";
  H += "}";
  H += "document.getElementById('mainView').innerHTML=html;";
  H += "}";

  H += "function renderMatchList(){";
  H += "var matches=ALL_PREDS.filter(function(p){return p.matchDate===activeDate&&p.league===activeLeague;}).sort(function(a,b){return b.rank-a.rank||b.rawScore-a.rawScore;});";
  H += "var html='<div style=\"display:flex;align-items:center;gap:12px;margin-bottom:16px\">';";
  H += "html+='<button class=\"back-btn\" onclick=\"backToLeagues()\">&#8592; Back</button>';";
  H += "html+='<div style=\"font-size:19px;font-weight:700;color:#111827\">'+activeLeague+'</div></div>';";
  H += "for(var i=0;i<matches.length;i++)html+=renderMatchCard(matches[i]);";
  H += "document.getElementById('mainView').innerHTML=html;";
  H += "}";

  H += "function gamesTable(games,title){";
  H += "if(!games||!games.length)return '';";
  H += "var rows='';";
  H += "for(var i=0;i<games.length;i++){";
  H += "var g=games[i],htTot=g.htFor+g.htAgainst;";
  H += "rows+='<tr>';";
  H += "rows+='<td>'+g.date+'</td>';";
  H += "rows+='<td>'+g.venue+'</td>';";
  H += "rows+='<td>'+g.opp+'</td>';";
  H += "rows+='<td>'+g.htFor+'-'+g.htAgainst+' ('+htTot+')</td>';";
  H += "rows+='<td>'+g.ftFor+'-'+g.ftAgainst+'</td>';";
  H += "'</tr>';";
  H += "}";
  H += "return '<div style=\"margin-top:12px\"><div style=\"font-size:12px;font-weight:700;color:#374151;margin-bottom:6px\">'+title+'</div><table class=\"mini-table\"><thead><tr><th>Date</th><th>H/A</th><th>Opponent</th><th>HT</th><th>FT</th></tr></thead><tbody>'+rows+'</tbody></table></div>';";
  H += "}";

  H += "function h2hTable(matches){";
  H += "if(!matches||!matches.length)return '<div style=\"margin-top:12px;font-size:12px;color:#6b7280\">No recent H2H found.</div>';";
  H += "var rows='';";
  H += "for(var i=0;i<matches.length;i++){";
  H += "var m=matches[i];";
  H += "var ht=(parseInt(m.ht_goals_team_a||0,10))+'-'+(parseInt(m.ht_goals_team_b||0,10));";
  H += "var ft=(parseInt(m.homeGoalCount||0,10))+'-'+(parseInt(m.awayGoalCount||0,10));";
  H += "var date=m.date_unix?new Date(m.date_unix*1000).toISOString().slice(0,10):'';";
  H += "rows+='<tr><td>'+date+'</td><td>'+m.home_name+'</td><td>'+m.away_name+'</td><td>'+ht+'</td><td>'+ft+'</td></tr>';";
  H += "}";
  H += "return '<div style=\"margin-top:12px\"><div style=\"font-size:12px;font-weight:700;color:#374151;margin-bottom:6px\">H2H</div><table class=\"mini-table\"><thead><tr><th>Date</th><th>Home</th><th>Away</th><th>HT</th><th>FT</th></tr></thead><tbody>'+rows+'</tbody></table></div>';";
  H += "}";

  H += "function renderMatchCard(m){";
  H += "var col=rankColor(m.rank),bg=rankBg(m.rank),br=rankBorder(m.rank);";
  H += "var dt=m.dt?new Date(m.dt).toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}):m.matchDate;";
  H += "var badgeText=m.eligible?'Eligible':'Not eligible';";
  H += "var warnStr=m.missingStats?'<span style=\"background:#fef3c7;color:#92400e;font-size:11px;padding:2px 7px;border-radius:4px;margin-left:8px;font-weight:600\">&#9888; missing stats</span>':'';";
  H += "var html='<div class=\"match-card\" style=\"border-left:4px solid '+col+'\">';";
  H += "html+='<div style=\"padding:16px\">';";
  H += "html+='<div style=\"display:grid;grid-template-columns:1fr auto;gap:12px;align-items:start\">';";
  H += "html+='<div>';";
  H += "html+='<div style=\"font-size:12px;color:#9ca3af;margin-bottom:4px\">'+dt+warnStr+'</div>';";
  H += "html+='<div style=\"font-size:20px;font-weight:800;color:#111827;margin-bottom:10px\">'+m.home+' <span style=\"color:#d1d5db;font-weight:500\">vs</span> '+m.away+'</div>';";
  H += "html+='<div style=\"display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-bottom:10px\">';";
  H += "html+='<div style=\"background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:10px\"><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700;margin-bottom:3px\">Home FH</div><div style=\"font-size:13px;font-weight:700;color:#111827\">Scored '+m.hAvgFH.scoredHome+'</div><div style=\"font-size:13px;font-weight:700;color:#dc2626\">Conceded '+m.hAvgFH.concededHome+'</div></div>';";
  H += "html+='<div style=\"background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:10px\"><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700;margin-bottom:3px\">Away FH</div><div style=\"font-size:13px;font-weight:700;color:#111827\">Scored '+m.aAvgFH.scoredAway+'</div><div style=\"font-size:13px;font-weight:700;color:#dc2626\">Conceded '+m.aAvgFH.concededAway+'</div></div>';";
  H += "html+='<div style=\"background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:10px\"><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700;margin-bottom:3px\">Decision</div><div style=\"font-size:13px;font-weight:800;color:'+(m.eligible?'#15803d':'#6b7280')+'\">'+badgeText+'</div><div style=\"font-size:12px;color:#6b7280;margin-top:2px\">4/5 or 5/5 only</div></div>';";
  H += "</div>";
  H += "html+='<div style=\"display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px\">';";
  H += "html+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px\"><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700\">Season env</div><div style=\"font-size:22px;font-weight:800;color:#111827\">'+m.seasonEnv.toFixed(2)+'</div></div>';";
  H += "html+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px\"><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700\">Recent last 5</div><div style=\"font-size:22px;font-weight:800;color:#111827\">'+m.recentEnv.val.toFixed(2)+'</div><div style=\"font-size:11px;color:#6b7280\">'+m.recentEnv.home.toFixed(2)+' / '+m.recentEnv.away.toFixed(2)+'</div></div>';";
  H += "html+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px\"><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700\">Venue form</div><div style=\"font-size:22px;font-weight:800;color:#111827\">'+m.venueEnv.val.toFixed(2)+'</div><div style=\"font-size:11px;color:#6b7280\">'+m.venueEnv.home.toFixed(2)+' / '+m.venueEnv.away.toFixed(2)+'</div></div>';";
  H += "html+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px\"><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700\">H2H</div><div style=\"font-size:22px;font-weight:800;color:#111827\">'+(m.h2hEnv.val==null?'-':m.h2hEnv.val.toFixed(2))+'</div><div style=\"font-size:11px;color:#6b7280\">'+(m.h2hEnv.count?m.h2hEnv.count+' match(es)':'none')+'</div></div>';";
  H += "</div>";
  H += "</div>";
  H += "html+='<div style=\"text-align:center;min-width:110px;background:'+bg+';border:1px solid '+br+';border-radius:10px;padding:12px 8px\">';";
  H += "html+='<div style=\"font-size:36px;font-weight:900;color:'+col+';line-height:1\">'+m.rank+'/5</div>';";
  H += "html+='<div style=\"font-size:13px;color:'+col+';font-weight:700;margin-top:4px\">'+m.rankLabel+'</div>';";
  H += "html+='<div style=\"font-size:11px;color:#6b7280;margin-top:6px\">Raw '+m.rawScore.toFixed(2)+'</div>';";
  H += "html+='<div style=\"font-size:11px;color:#6b7280;margin-top:4px\">'+(m.eligible?'Eligible':'No')+'</div>';";
  H += "html+='</div>';";
  H += "</div>";
  H += "if(m.status==='complete'){";
  H += "html+='<div style=\"margin-top:10px;padding:10px 12px;border-radius:8px;background:#f9fafb;border:1px solid #e5e7eb;font-size:13px;color:#374151\">Actual result &mdash; FH: <strong>'+m.fhH+'-'+m.fhA+'</strong> &middot; FT: <strong>'+m.ftH+'-'+m.ftA+'</strong></div>';";
  H += "}";
  H += "html+='<details style=\"margin-top:12px\"><summary style=\"font-size:13px;color:#6b7280;padding:4px 0;border-top:1px solid #f3f4f6;padding-top:10px\">&#9660; Match history</summary><div style=\"padding-top:8px\">';";
  H += "html+=gamesTable(m.hLast5||[],'Home team &mdash; last 5 overall');";
  H += "html+=gamesTable(m.aLast5||[],'Away team &mdash; last 5 overall');";
  H += "html+=h2hTable(m.h2h||[]);";
  H += "html+='</div></details>';";
  H += "html+='</div></div>';";
  H += "return html;";
  H += "}";

  H += "document.getElementById('headerTitle').textContent=fmt(new Date(DATES[0]+'T12:00:00'));";
  H += "renderTabs();";
  H += "renderLeagueList();";
  H += "<\/script></body></html>";

  return H;
}

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
