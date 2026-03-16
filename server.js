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

// ── Performance / resilience config ───────────────────────────────────────────
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
    if (!res.ok) {
      throw new Error(`Upstream ${res.status} ${res.statusText}`);
    }
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
const safe = (v) => (isNaN(v) || !isFinite(v) || v == null ? 0 : +v);

function clamp(x, min, max) {
  return Math.max(min, Math.min(max, x));
}

function weightedAvg(arr, getter) {
  const w = [5, 4, 3, 2, 1];
  let num = 0;
  let den = 0;
  for (let i = 0; i < Math.min(arr.length, 5); i++) {
    num += w[i] * getter(arr[i]);
    den += w[i];
  }
  return den ? num / den : 0;
}

function stdDev(vals) {
  if (!vals.length) return 0;
  const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
  const variance =
    vals.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / vals.length;
  return Math.sqrt(variance);
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

// ── Signal helpers ────────────────────────────────────────────────────────────
function calcCI(ht, at) {
  const val =
    safe(ht.scoredAVGHT_home) +
    safe(at.scoredAVGHT_away) +
    safe(ht.concededAVGHT_home) +
    safe(at.concededAVGHT_away);

  return { val: +val.toFixed(3), met: val >= 3.2 };
}

function calcT1(ht, at) {
  const hv = safe(ht.seasonOver25PercentageHT_overall);
  const av = safe(at.seasonOver25PercentageHT_overall);
  return {
    hVal: hv,
    aVal: av,
    lower: Math.min(hv, av),
    met: hv >= 20 && av >= 20
  };
}

function calcFH15(ht, at) {
  const hv = safe(ht.seasonOver15PercentageHT_overall);
  const av = safe(at.seasonOver15PercentageHT_overall);
  return {
    hVal: hv,
    aVal: av,
    lower: Math.min(hv, av),
    met: hv >= 40 && av >= 40
  };
}

function calcCN010(ht, at) {
  const hMP =
    safe(ht.seasonMatchesPlayed_home) ||
    safe(ht.mp_home) ||
    safe(ht.matchesPlayed_home) ||
    1;
  const hCon =
    safe(ht.goals_conceded_min_0_to_10_home) ||
    safe(ht.goals_conceded_min_0_to_10_overall) ||
    0;
  const hRate = hCon / hMP;

  const aMP =
    safe(at.seasonMatchesPlayed_away) ||
    safe(at.mp_away) ||
    safe(at.matchesPlayed_away) ||
    1;
  const aCon =
    safe(at.goals_conceded_min_0_to_10_away) ||
    safe(at.goals_conceded_min_0_to_10_overall) ||
    0;
  const aRate = aCon / aMP;

  return {
    hRate: +hRate.toFixed(3),
    hCon,
    hMP,
    aRate: +aRate.toFixed(3),
    aCon,
    aMP,
    val: +Math.max(hRate, aRate).toFixed(3),
    met: hRate >= 0.25 || aRate >= 0.25
  };
}

function calcCrossFH(ht, at) {
  const homeSide =
    (safe(ht.scoredAVGHT_home) + safe(at.concededAVGHT_away)) / 2;
  const awaySide =
    (safe(at.scoredAVGHT_away) + safe(ht.concededAVGHT_home)) / 2;
  const total = homeSide + awaySide;

  return {
    homeSide: +homeSide.toFixed(3),
    awaySide: +awaySide.toFixed(3),
    val: +total.toFixed(3),
    met: total >= 1.55
  };
}

function calcLast5FHTrend(hLast5, aLast5) {
  const h = weightedAvg(hLast5, (g) => g.htFor + g.htAgainst);
  const a = weightedAvg(aLast5, (g) => g.htFor + g.htAgainst);
  const val = (h + a) / 2;

  return {
    h: +h.toFixed(3),
    a: +a.toFixed(3),
    val: +val.toFixed(3),
    met: val >= 1.6
  };
}

function calcVenueLast5FHTrend(hHome5, aAway5) {
  const h = weightedAvg(hHome5, (g) => g.htFor + g.htAgainst);
  const a = weightedAvg(aAway5, (g) => g.htFor + g.htAgainst);
  const val = (h + a) / 2;

  return {
    h: +h.toFixed(3),
    a: +a.toFixed(3),
    val: +val.toFixed(3),
    met: val >= 1.55
  };
}

function calcZeroZeroPenalty(hLast5, aLast5) {
  const hRate = hLast5.length
    ? hLast5.filter((g) => g.htFor + g.htAgainst === 0).length / hLast5.length
    : 0;
  const aRate = aLast5.length
    ? aLast5.filter((g) => g.htFor + g.htAgainst === 0).length / aLast5.length
    : 0;
  const avgRate = (hRate + aRate) / 2;

  return {
    hRate: +hRate.toFixed(3),
    aRate: +aRate.toFixed(3),
    val: +avgRate.toFixed(3),
    met: avgRate <= 0.25
  };
}

function calcFHVolatility(hLast5, aLast5) {
  const hVals = hLast5.map((g) => g.htFor + g.htAgainst);
  const aVals = aLast5.map((g) => g.htFor + g.htAgainst);
  const h = stdDev(hVals);
  const a = stdDev(aVals);
  const val = (h + a) / 2;

  return {
    h: +h.toFixed(3),
    a: +a.toFixed(3),
    val: +val.toFixed(3),
    met: val >= 0.95
  };
}

function calcOneSidedPressure(ht, at) {
  const homePush = safe(ht.scoredAVGHT_home) * safe(at.concededAVGHT_away);
  const awayPush = safe(at.scoredAVGHT_away) * safe(ht.concededAVGHT_home);
  const best = Math.max(homePush, awayPush);

  return {
    homePush: +homePush.toFixed(3),
    awayPush: +awayPush.toFixed(3),
    val: +best.toFixed(3),
    met: best >= 0.42
  };
}

function calcLast5FHExplosions(hLast5, aLast5) {
  const hRate = hLast5.length
    ? hLast5.filter((g) => g.htFor + g.htAgainst >= 3).length / hLast5.length
    : 0;
  const aRate = aLast5.length
    ? aLast5.filter((g) => g.htFor + g.htAgainst >= 3).length / aLast5.length
    : 0;
  const val = (hRate + aRate) / 2;

  return {
    hRate: +hRate.toFixed(3),
    aRate: +aRate.toFixed(3),
    val: +val.toFixed(3),
    met: val >= 0.2
  };
}

function calcModelScore(inputs) {
  let score = 0;

  score += inputs.ci.val * 0.9;
  score += Math.min(inputs.t1.lower / 100, 0.4) * 1.4;
  score += Math.min(inputs.fh15.lower / 100, 0.7) * 1.1;
  score += Math.min(inputs.cn.val, 0.6) * 0.9;

  score += inputs.crossFH.val * 1.35;
  score += inputs.last5Trend.val * 1.05;
  score += inputs.venueTrend.val * 1.15;
  score += inputs.oneSided.val * 1.1;
  score += inputs.volatility.val * 0.55;
  score += inputs.explosionRate.val * 1.25;

  score -= inputs.zeroZero.val * 1.6;

  return +score.toFixed(4);
}

function scoreToProb(score) {
  const x = -4.25 + score * 1.18;
  const prob = 1 / (1 + Math.exp(-x));
  return Math.round(clamp(prob * 100, 3, 75));
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
    teamMap[`__name__${(t.name || "").toLowerCase().trim()}`] = t;
    if (t.clean_name) {
      teamMap[`__name__${t.clean_name.toLowerCase().trim()}`] = t;
    }
  }

  return teamMap;
}

async function loadLeagueHistory(sid) {
  const lmRes = await ftch(
    `${BASE}/league-matches?season_id=${sid}&max_per_page=150&page=1&key=${KEY}`
  );

  const teamHistory = {};
  const completed = (lmRes.data || []).filter((m) => m.status === "complete");
  completed.sort((a, b) => (b.date_unix || 0) - (a.date_unix || 0));

  for (const m of completed) {
    const addGame = (teamName, isHome) => {
      const key = (teamName || "").toLowerCase().trim();
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
        date: m.date_unix
          ? new Date(m.date_unix * 1000).toISOString().slice(0, 10)
          : ""
      });
    };

    if (m.home_name) addGame(m.home_name, true);
    if (m.away_name) addGame(m.away_name, false);
  }

  return teamHistory;
}

function getLast5(teamHistory, teamName) {
  const games = teamHistory[(teamName || "").toLowerCase().trim()] || [];
  return games.slice(0, 5);
}

function getLastNByVenue(teamHistory, teamName, venue, n = 5) {
  const games = (teamHistory[(teamName || "").toLowerCase().trim()] || []).filter(
    (g) => g.venue === venue
  );
  return games.slice(0, n);
}

function buildPredictionForFixture(fixture, leagueName, teamMap, teamHistory, tzOffset) {
  const homeId = String(fixture.homeID || fixture.home_id || "");
  const awayId = String(fixture.awayID || fixture.away_id || "");

  const htRaw =
    teamMap[homeId] ||
    teamMap[parseInt(homeId, 10)] ||
    teamMap[`__name__${(fixture.home_name || "").toLowerCase().trim()}`];

  const atRaw =
    teamMap[awayId] ||
    teamMap[parseInt(awayId, 10)] ||
    teamMap[`__name__${(fixture.away_name || "").toLowerCase().trim()}`];

  const ht = unwrapTeam(htRaw) || {};
  const at = unwrapTeam(atRaw) || {};

  const hLast5 = getLast5(teamHistory, fixture.home_name || "");
  const aLast5 = getLast5(teamHistory, fixture.away_name || "");
  const hHome5 = getLastNByVenue(teamHistory, fixture.home_name || "", "H", 5);
  const aAway5 = getLastNByVenue(teamHistory, fixture.away_name || "", "A", 5);

  const ci = calcCI(ht, at);
  const t1 = calcT1(ht, at);
  const fh15 = calcFH15(ht, at);
  const cn = calcCN010(ht, at);
  const crossFH = calcCrossFH(ht, at);
  const last5Trend = calcLast5FHTrend(hLast5, aLast5);
  const venueTrend = calcVenueLast5FHTrend(hHome5, aAway5);
  const zeroZero = calcZeroZeroPenalty(hLast5, aLast5);
  const volatility = calcFHVolatility(hLast5, aLast5);
  const oneSided = calcOneSidedPressure(ht, at);
  const explosionRate = calcLast5FHExplosions(hLast5, aLast5);

  const signals = [
    {
      key: "CI",
      label: "HT Intensity Index (CI)",
      desc: "H scored(home) + A scored(away) + H conceded(home) + A conceded(away) ≥ 3.2",
      hVal:
        `${safe(ht.scoredAVGHT_home).toFixed(2)} sc / ${safe(ht.concededAVGHT_home).toFixed(2)} cn`,
      aVal:
        `${safe(at.scoredAVGHT_away).toFixed(2)} sc / ${safe(at.concededAVGHT_away).toFixed(2)} cn`,
      combinedVal: ci.val.toFixed(3),
      threshold: "≥ 3.2",
      met: ci.met,
      lift: "core"
    },
    {
      key: "T1",
      label: "Both Teams FH Over 2.5 Rate",
      desc: "seasonOver25PercentageHT_overall ≥ 20% for both teams",
      hVal: `${t1.hVal.toFixed(1)}%`,
      aVal: `${t1.aVal.toFixed(1)}%`,
      combinedVal: `${t1.lower.toFixed(1)}% (lower)`,
      threshold: "both ≥ 20%",
      met: t1.met,
      lift: "history"
    },
    {
      key: "FH15",
      label: "Both Teams FH Over 1.5 Rate",
      desc: "seasonOver15PercentageHT_overall ≥ 40% for both teams",
      hVal: `${fh15.hVal.toFixed(1)}%`,
      aVal: `${fh15.aVal.toFixed(1)}%`,
      combinedVal: `${fh15.lower.toFixed(1)}% (lower)`,
      threshold: "both ≥ 40%",
      met: fh15.met,
      lift: "history"
    },
    {
      key: "CN010",
      label: "Early Goal Conceded Rate (0-10)",
      desc: "Either team concedes ≥ 0.25 goals/game in minutes 0–10",
      hVal: `${cn.hCon} in ${cn.hMP} (${cn.hRate.toFixed(3)}/g)`,
      aVal: `${cn.aCon} in ${cn.aMP} (${cn.aRate.toFixed(3)}/g)`,
      combinedVal: cn.val.toFixed(3),
      threshold: "either ≥ 0.25",
      met: cn.met,
      lift: "chaos"
    },
    {
      key: "XFH",
      label: "Cross-Matchup FH Expectation",
      desc: "FH scoring blended with opponent FH concession",
      hVal: crossFH.homeSide.toFixed(3),
      aVal: crossFH.awaySide.toFixed(3),
      combinedVal: crossFH.val.toFixed(3),
      threshold: "≥ 1.55",
      met: crossFH.met,
      lift: "matchup"
    },
    {
      key: "L5",
      label: "Last 5 FH Trend",
      desc: "Weighted recent FH total-goal trend",
      hVal: last5Trend.h.toFixed(3),
      aVal: last5Trend.a.toFixed(3),
      combinedVal: last5Trend.val.toFixed(3),
      threshold: "≥ 1.60",
      met: last5Trend.met,
      lift: "recent"
    },
    {
      key: "VENUE_L5",
      label: "Venue-Specific Last 5 FH Trend",
      desc: "Home last 5 home + away last 5 away",
      hVal: venueTrend.h.toFixed(3),
      aVal: venueTrend.a.toFixed(3),
      combinedVal: venueTrend.val.toFixed(3),
      threshold: "≥ 1.55",
      met: venueTrend.met,
      lift: "venue"
    },
    {
      key: "EXP",
      label: "Last 5 FH Explosion Rate",
      desc: "Share of recent matches with 3+ FH goals",
      hVal: `${(explosionRate.hRate * 100).toFixed(0)}%`,
      aVal: `${(explosionRate.aRate * 100).toFixed(0)}%`,
      combinedVal: `${(explosionRate.val * 100).toFixed(0)}%`,
      threshold: "≥ 20%",
      met: explosionRate.met,
      lift: "tail"
    },
    {
      key: "VOL",
      label: "FH Volatility",
      desc: "Higher FH volatility favors FH over 2.5 tails",
      hVal: volatility.h.toFixed(3),
      aVal: volatility.a.toFixed(3),
      combinedVal: volatility.val.toFixed(3),
      threshold: "≥ 0.95",
      met: volatility.met,
      lift: "variance"
    },
    {
      key: "PRESS",
      label: "One-Sided Pressure",
      desc: "One team may drive the FH over largely by itself",
      hVal: oneSided.homePush.toFixed(3),
      aVal: oneSided.awayPush.toFixed(3),
      combinedVal: oneSided.val.toFixed(3),
      threshold: "≥ 0.42",
      met: oneSided.met,
      lift: "mismatch"
    },
    {
      key: "ZZ",
      label: "0-0 FH Resistance",
      desc: "Penalty filter: recent scoreless first halves",
      hVal: `${(zeroZero.hRate * 100).toFixed(0)}%`,
      aVal: `${(zeroZero.aRate * 100).toFixed(0)}%`,
      combinedVal: `${(zeroZero.val * 100).toFixed(0)}%`,
      threshold: "≤ 25%",
      met: zeroZero.met,
      lift: "penalty"
    }
  ];

  const modelScore = calcModelScore({
    ci,
    t1,
    fh15,
    cn,
    crossFH,
    last5Trend,
    venueTrend,
    zeroZero,
    volatility,
    oneSided,
    explosionRate
  });

  const prob = scoreToProb(modelScore);
  const nMet = signals.filter((s) => s.met).length;
  const matchDate = unixToLocalDate(fixture.date_unix, tzOffset) || fixture._date;

  return {
    league: leagueName,
    leagueSid: parseInt(fixture.competition_id, 10),
    dt: (fixture.date_unix || 0) * 1000,
    matchDate,
    home: fixture.home_name,
    away: fixture.away_name,
    prob,
    modelScore,
    nMet,
    signals,
    missingStats: !htRaw || !atRaw,
    status: fixture.status || "incomplete",
    fhH: parseInt(fixture.ht_goals_team_a || 0, 10),
    fhA: parseInt(fixture.ht_goals_team_b || 0, 10),
    ftH: parseInt(fixture.homeGoalCount || 0, 10),
    ftA: parseInt(fixture.awayGoalCount || 0, 10),
    hLast5,
    aLast5,
    hHome5,
    aAway5,
    hAvgFH: {
      scored: safe(ht.scoredAVGHT_overall).toFixed(2),
      scoredHome: safe(ht.scoredAVGHT_home).toFixed(2),
      conceded: safe(ht.concededAVGHT_overall).toFixed(2),
      concededHome: safe(ht.concededAVGHT_home).toFixed(2)
    },
    aAvgFH: {
      scored: safe(at.scoredAVGHT_overall).toFixed(2),
      scoredAway: safe(at.scoredAVGHT_away).toFixed(2),
      conceded: safe(at.concededAVGHT_overall).toFixed(2),
      concededAway: safe(at.concededAVGHT_away).toFixed(2)
    }
  };
}

// ── Small endpoints ───────────────────────────────────────────────────────────
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "footystats-proxy",
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

app.get("/debug-league", async (req, res) => {
  const sid = req.query.sid || "14924";
  const date = req.query.date || new Date().toISOString().slice(0, 10);

  try {
    const [fixtureRes, teamRes] = await Promise.all([
      ftch(`${BASE}/todays-matches?date=${date}&key=${KEY}`),
      ftch(`${BASE}/league-teams?season_id=${sid}&include=stats&key=${KEY}`)
    ]);

    const fixtures = (fixtureRes.data || []).filter(
      (m) => String(m.competition_id) === String(sid)
    );

    const sampleFixture = fixtures[0] || {};
    const sampleTeam = (teamRes.data || [])[0] || {};

    res.json({
      sid,
      date,
      fixtureCount: fixtures.length,
      fixtureIds: fixtures.map((m) => ({
        home: m.home_name,
        away: m.away_name,
        homeID: m.homeID,
        awayID: m.awayID,
        competition_id: m.competition_id
      })),
      teamCount: (teamRes.data || []).length,
      sampleTeamName: sampleTeam.name,
      sampleTeamId: sampleTeam.id,
      sampleTeamTopKeys: Object.keys(sampleTeam),
      sampleTeamStatsKeys: sampleTeam.stats
        ? Object.keys(sampleTeam.stats).slice(0, 40)
        : [],
      sampleFixtureKeys: Object.keys(sampleFixture)
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
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

// ── Main model route ──────────────────────────────────────────────────────────
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
          const [teamMap, teamHistory] = await Promise.all([
            loadLeagueTeams(sid),
            loadLeagueHistory(sid)
          ]);
          return [sid, { teamMap, teamHistory }];
        } catch (err) {
          console.log(`League load error sid=${sid}:`, err.message);
          return [sid, { teamMap: {}, teamHistory: {}, error: err.message }];
        }
      })
    );

    const leagueData = Object.fromEntries(leagueDataEntries);
    const preds = [];

    for (const sid of leagueIds) {
      const fixtures = leagueFixtures[sid] || [];
      const name = leagueNameOverride[sid];
      const { teamMap, teamHistory } = leagueData[sid] || {
        teamMap: {},
        teamHistory: {}
      };

      for (const fixture of fixtures) {
        preds.push(
          buildPredictionForFixture(fixture, name, teamMap, teamHistory, tzOffset)
        );
      }
    }

    preds.sort(
      (a, b) => b.prob - a.prob || b.modelScore - a.modelScore || b.nMet - a.nMet
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
  H += "<title>First Half Score</title>";
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
  H += "summary{cursor:pointer;user-select:none;list-style:none}";
  H += ".sig-table{width:100%;border-collapse:collapse;table-layout:fixed;font-size:13px}";
  H += ".sig-table th{background:#f9fafb;padding:7px 8px;text-align:left;font-size:11px;color:#6b7280;font-weight:600;text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid #e5e7eb}";
  H += ".sig-table td{padding:10px 8px;border-bottom:1px solid #f3f4f6;vertical-align:top}";
  H += ".sig-row-met{background:#f0fdf4}";
  H += ".sig-row-unmet{background:#fff}";
  H += ".pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700}";
  H += ".pill-met{background:#dcfce7;color:#15803d}";
  H += ".pill-unmet{background:#fee2e2;color:#dc2626}";
  H += "</style></head><body>";

  H += '<div id="header" style="background:#fff;border-bottom:1px solid #e5e7eb;padding:14px 20px;position:sticky;top:0;z-index:10">';
  H += '<div style="max-width:860px;margin:0 auto">';
  H += '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">';
  H += '<div><div style="font-size:11px;color:#6b7280;letter-spacing:1px;text-transform:uppercase">&#9917; First Half Score</div>';
  H += '<h1 style="font-size:22px;font-weight:800;color:#111827" id="headerTitle">Loading...</h1></div>';
  H += '<button onclick="location.reload()" style="background:#111827;color:#fff;padding:8px 16px;font-size:14px;border:none;border-radius:6px;font-weight:600;cursor:pointer">&#8635; Refresh</button>';
  H += "</div>";
  H += '<div id="dayTabs" style="display:flex;gap:8px;flex-wrap:wrap"></div>';
  H += "</div></div>";

  H += '<div style="padding:16px 20px;max-width:860px;margin:0 auto">';
  H += '<div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#92400e;line-height:1.6">';
  H += "<strong>How it works:</strong> weighted v2 model using season FH splits, recent last-5 FH trends, venue-specific form, volatility, explosion rate, one-sided pressure, and 0-0 resistance.";
  H += "</div>";
  H += '<div id="mainView"></div></div>';

  H += "<script>";
  H += "var ALL_PREDS=" + predsJSON + ";";
  H += "var DATES=" + datesJSON + ";";
  H += 'var DAY_LABELS=["Today","Tomorrow","Day 3","Day 4","Day 5","Day 6"];';
  H += "var activeDate=DATES[0];var activeLeague=null;";
  H += "function fmt(d){return new Date(d).toLocaleDateString('en-GB',{weekday:'long',day:'2-digit',month:'short'});}";
  H += "function shortName(n){return (n||'').split(' ').slice(0,2).join(' ');}";

  H += "function renderTabs(){";
  H += "var el=document.getElementById('dayTabs'),html='';";
  H += "for(var i=0;i<DATES.length;i++){";
  H += "var d=DATES[i],count=ALL_PREDS.filter(function(p){return p.matchDate===d;}).length;";
  H += "var cls=d===activeDate?'tab active':'tab';";
  H += "html+='<button class=\"'+cls+'\" onclick=\"selectDay('+i+')\">'+(DAY_LABELS[i]||d)+' <span style=\"font-size:12px;opacity:.7\">('+count+')</span></button>';";
  H += "}el.innerHTML=html;}";
  H += "function selectDay(i){activeDate=DATES[i];activeLeague=null;renderTabs();renderLeagueList();document.getElementById('headerTitle').textContent=fmt(new Date(DATES[i]+'T12:00:00'));}";
  H += "function selectLeague(league){activeLeague=league;renderMatchList();}";
  H += "function backToLeagues(){activeLeague=null;renderLeagueList();}";

  H += "function renderLeagueList(){";
  H += "var dayPreds=ALL_PREDS.filter(function(p){return p.matchDate===activeDate;});";
  H += "var leagueMap={};";
  H += "for(var i=0;i<dayPreds.length;i++){var p=dayPreds[i];if(!leagueMap[p.league])leagueMap[p.league]=[];leagueMap[p.league].push(p);}";
  H += "var leagueList=Object.entries(leagueMap).sort(function(a,b){return Math.max.apply(null,b[1].map(function(p){return p.prob;}))-Math.max.apply(null,a[1].map(function(p){return p.prob;}));});";
  H += "if(!leagueList.length){document.getElementById('mainView').innerHTML='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280\">No matches found for this day.</div>';return;}";
  H += "var html='<div style=\"font-size:13px;color:#6b7280;margin-bottom:12px\">'+dayPreds.length+' matches across '+leagueList.length+' leagues &middot; sorted by probability</div>';";
  H += "for(var j=0;j<leagueList.length;j++){";
  H += "var league=leagueList[j][0],matches=leagueList[j][1];";
  H += "var maxProb=Math.max.apply(null,matches.map(function(p){return p.prob;}));";
  H += "var maxScore=Math.max.apply(null,matches.map(function(p){return p.modelScore;}));";
  H += "var hotCount=matches.filter(function(p){return p.prob>=35;}).length;";
  H += "var probCol=maxProb>=45?'#16a34a':maxProb>=25?'#d97706':'#6b7280';";
  H += "var hotStr=hotCount>0?' &middot; <span style=\"color:#15803d;font-weight:600\">'+hotCount+' strong matches</span>':'';";
  H += "var safeLeague=league.replace(/\\\\/g,'\\\\\\\\').replace(/'/g,\"\\\\'\");";
  H += "html+='<div class=\"league-card\" onclick=\"selectLeague(\\''+safeLeague+'\\')\">';";
  H += "html+='<div style=\"flex:1;min-width:0;margin-right:12px\">';";
  H += "html+='<div style=\"font-size:18px;font-weight:700;color:#111827;margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis\">'+league+'</div>';";
  H += "html+='<div style=\"font-size:13px;color:#6b7280\">'+matches.length+' match'+(matches.length>1?'es':'')+hotStr+'</div>';";
  H += "html+='</div><div style=\"text-align:right;flex-shrink:0\">';";
  H += "html+='<div style=\"font-size:26px;font-weight:800;color:'+probCol+'\">'+maxProb+'%</div>';";
  H += "html+='<div style=\"font-size:11px;color:#9ca3af;margin-top:1px\">top score '+maxScore.toFixed(2)+'</div>';";
  H += "html+='</div></div>';}";
  H += "document.getElementById('mainView').innerHTML=html;}";
  H += "function renderMatchList(){";
  H += "var matches=ALL_PREDS.filter(function(p){return p.matchDate===activeDate&&p.league===activeLeague;}).sort(function(a,b){return b.prob-a.prob||b.modelScore-a.modelScore;});";
  H += "var html='<div style=\"display:flex;align-items:center;gap:12px;margin-bottom:16px\">';";
  H += "html+='<button class=\"back-btn\" onclick=\"backToLeagues()\">&#8592; Back</button>';";
  H += "html+='<div style=\"font-size:19px;font-weight:700;color:#111827\">'+activeLeague+'</div></div>';";
  H += "for(var i=0;i<matches.length;i++)html+=renderMatchCard(matches[i]);";
  H += "document.getElementById('mainView').innerHTML=html;}";
  H += "function last5Table(teamName,games,title){";
  H += "if(!games||!games.length)return '';";
  H += "var rows='';";
  H += "for(var i=0;i<games.length;i++){";
  H += "var g=games[i], htTot=g.htFor+g.htAgainst;";
  H += "var htCol=htTot>=2?'#15803d':'#374151';";
  H += "var htBg=htTot>=2?'#f0fdf4':'transparent';";
  H += "var vBg=g.venue==='H'?'#eff6ff':'#fdf4ff';";
  H += "var vCol=g.venue==='H'?'#1d4ed8':'#7e22ce';";
  H += "rows+='<tr style=\"border-bottom:1px solid #f3f4f6\">';";
  H += "rows+='<td style=\"padding:5px 8px;font-size:11px;color:#9ca3af\">'+g.date+'</td>';";
  H += "rows+='<td style=\"padding:5px 6px\"><span style=\"background:'+vBg+';color:'+vCol+';font-size:10px;font-weight:700;padding:1px 5px;border-radius:3px\">'+g.venue+'</span></td>';";
  H += "rows+='<td style=\"padding:5px 8px;font-size:12px;color:#374151;max-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">'+g.opp+'</td>';";
  H += "rows+='<td style=\"padding:5px 8px;font-size:13px;font-weight:700;text-align:center;color:'+htCol+';background:'+htBg+'\">'+g.htFor+'-'+g.htAgainst+'</td>';";
  H += "rows+='<td style=\"padding:5px 8px;font-size:12px;color:#6b7280;text-align:center\">'+g.ftFor+'-'+g.ftAgainst+'</td></tr>';}";
  H += "return '<div style=\"margin-bottom:12px\"><div style=\"font-size:11px;font-weight:700;color:#374151;margin-bottom:4px\">'+teamName+' — '+title+'</div>'";
  H += "+'<table style=\"width:100%;border-collapse:collapse;font-size:12px\"><thead><tr style=\"background:#f9fafb;border-bottom:1px solid #e5e7eb\">'";
  H += "+'<th style=\"padding:4px 8px;font-size:10px;color:#9ca3af;font-weight:600;text-align:left\">Date</th>'";
  H += "+'<th style=\"padding:4px 6px;font-size:10px;color:#9ca3af;font-weight:600\">H/A</th>'";
  H += "+'<th style=\"padding:4px 8px;font-size:10px;color:#9ca3af;font-weight:600;text-align:left\">Opponent</th>'";
  H += "+'<th style=\"padding:4px 8px;font-size:10px;color:#9ca3af;font-weight:600;text-align:center\">HT</th>'";
  H += "+'<th style=\"padding:4px 8px;font-size:10px;color:#9ca3af;font-weight:600;text-align:center\">FT</th>'";
  H += "+'</tr></thead><tbody>'+rows+'</tbody></table></div>';}";
  H += "function renderMatchCard(m){";
  H += "var probCol=m.prob>=45?'#16a34a':m.prob>=25?'#d97706':'#6b7280';";
  H += "var probBg=m.prob>=45?'#f0fdf4':m.prob>=25?'#fffbeb':'#f9fafb';";
  H += "var probBorder=m.prob>=45?'#bbf7d0':m.prob>=25?'#fde68a':'#e5e7eb';";
  H += "var probLabel=m.prob>=45?'🔥 HIGH':m.prob>=25?'⚡ MED':'❄ LOW';";
  H += "var dt=m.dt?new Date(m.dt).toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}):m.matchDate;";
  H += "var dots='';";
  H += "for(var i=0;i<m.signals.length;i++){var s=m.signals[i];dots+='<span title=\"'+s.label+'\" style=\"display:inline-block;width:11px;height:11px;border-radius:50%;background:'+(s.met?'#16a34a':'#e5e7eb')+';margin-right:3px\"></span>';}";
  H += "var hSc=m.hAvgFH?m.hAvgFH.scoredHome:'-';";
  H += "var hCn=m.hAvgFH?m.hAvgFH.concededHome:'-';";
  H += "var aSc=m.aAvgFH?m.aAvgFH.scoredAway:'-';";
  H += "var aCn=m.aAvgFH?m.aAvgFH.concededAway:'-';";
  H += "var statsBar='<div style=\"display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:6px\">';";
  H += "statsBar+='<div style=\"background:#f9fafb;border:1px solid #e5e7eb;border-radius:6px;padding:8px 10px\">';";
  H += "statsBar+='<div style=\"font-size:10px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.4px;margin-bottom:5px\">'+shortName(m.home)+' (home)</div>';";
  H += "statsBar+='<div style=\"display:flex;gap:12px\">';";
  H += "statsBar+='<div><div style=\"font-size:10px;color:#9ca3af\">FH Scored</div><div style=\"font-size:18px;font-weight:800;color:#111827;line-height:1.1\">'+hSc+'</div></div>';";
  H += "statsBar+='<div><div style=\"font-size:10px;color:#9ca3af\">FH Conceded</div><div style=\"font-size:18px;font-weight:800;color:#dc2626;line-height:1.1\">'+hCn+'</div></div>';";
  H += "statsBar+='</div></div>';";
  H += "statsBar+='<div style=\"background:#f9fafb;border:1px solid #e5e7eb;border-radius:6px;padding:8px 10px\">';";
  H += "statsBar+='<div style=\"font-size:10px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.4px;margin-bottom:5px\">'+shortName(m.away)+' (away)</div>';";
  H += "statsBar+='<div style=\"display:flex;gap:12px\">';";
  H += "statsBar+='<div><div style=\"font-size:10px;color:#9ca3af\">FH Scored</div><div style=\"font-size:18px;font-weight:800;color:#111827;line-height:1.1\">'+aSc+'</div></div>';";
  H += "statsBar+='<div><div style=\"font-size:10px;color:#9ca3af\">FH Conceded</div><div style=\"font-size:18px;font-weight:800;color:#dc2626;line-height:1.1\">'+aCn+'</div></div>';";
  H += "statsBar+='</div></div>';";
  H += "statsBar+='</div>';";
  H += "var sigRows='';";
  H += "for(var i=0;i<m.signals.length;i++){var s=m.signals[i];";
  H += "sigRows+='<tr class=\"'+(s.met?'sig-row-met':'sig-row-unmet')+'\">';";
  H += "sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6\"><div style=\"font-weight:600;color:#111827;margin-bottom:2px\">'+s.label+'</div><div style=\"font-size:11px;color:#9ca3af\">'+s.desc+'</div></td>';";
  H += "sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><div style=\"font-weight:700;color:#374151;font-size:13px\">'+s.hVal+'</div></td>';";
  H += "sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><div style=\"font-weight:700;color:#374151;font-size:13px\">'+s.aVal+'</div></td>';";
  H += "sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><div style=\"font-weight:800;font-size:14px;color:'+(s.met?'#15803d':'#dc2626')+'\">'+s.combinedVal+'</div><div style=\"font-size:10px;color:#9ca3af\">'+s.threshold+'</div></td>';";
  H += "sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><span class=\"pill '+(s.met?'pill-met':'pill-unmet')+'\">'+(s.met?'&#10003; MET':'&#10007; MISS')+'</span><div style=\"font-size:10px;color:#9ca3af;margin-top:3px\">'+s.lift+'</div></td></tr>';}";
  H += "var warnStr=m.missingStats?'<span style=\"background:#fef3c7;color:#92400e;font-size:11px;padding:2px 7px;border-radius:4px;margin-left:8px;font-weight:600\">&#9888; missing stats</span>':'';";
  H += "var html='<div class=\"match-card\" style=\"border-left:4px solid '+probCol+'\">';";
  H += "html+='<div style=\"padding:16px\">';";
  H += "html+='<div style=\"display:grid;grid-template-columns:1fr auto;gap:10px;align-items:start\">';";
  H += "html+='<div style=\"min-width:0\">';";
  H += "html+='<div style=\"font-size:12px;color:#9ca3af;margin-bottom:3px\">'+dt+warnStr+'</div>';";
  H += "html+='<div style=\"font-size:17px;font-weight:700;color:#111827;margin-bottom:8px;line-height:1.3\">'+m.home+' <span style=\"color:#d1d5db;font-weight:400;font-size:13px\">vs</span> '+m.away+'</div>';";
  H += "html+=statsBar;";
  H += "html+='<div style=\"display:flex;align-items:center;gap:6px;margin-top:8px\">'+dots+'<span style=\"font-size:12px;color:#6b7280\">'+m.nMet+'/'+m.signals.length+' signals met</span></div>';";
  H += "html+='</div>';";
  H += "if(m.status==='complete'){";
  H += "var fhHit=(m.fhH+m.fhA)>2;";
  H += "var rb=fhHit?'#f0fdf4':'#fef2f2',rbr=fhHit?'#bbf7d0':'#fecaca',rfc=fhHit?'#16a34a':'#dc2626';";
  H += "html+='<div style=\"text-align:center;min-width:92px;background:'+rb+';border:1px solid '+rbr+';border-radius:8px;padding:8px 6px;flex-shrink:0\">';";
  H += "html+='<div style=\"font-size:11px;color:#9ca3af;font-weight:600\">FH</div>';";
  H += "html+='<div style=\"font-size:24px;font-weight:800;color:'+rfc+';line-height:1.1\">'+m.fhH+'-'+m.fhA+'</div>';";
  H += "html+='<div style=\"font-size:10px;color:#9ca3af;margin-top:5px;font-weight:600\">FT</div>';";
  H += "html+='<div style=\"font-size:16px;font-weight:700;color:#374151;line-height:1.1\">'+m.ftH+'-'+m.ftA+'</div>';";
  H += "html+='<div style=\"font-size:11px;color:'+probCol+';margin-top:5px;font-weight:700\">'+m.prob+'% pre</div>';";
  H += "html+='<div style=\"font-size:10px;color:#9ca3af;margin-top:3px\">Score '+m.modelScore.toFixed(2)+'</div>';";
  H += "html+='</div>';";
  H += "}else{";
  H += "html+='<div style=\"text-align:center;min-width:92px;background:'+probBg+';border:1px solid '+probBorder+';border-radius:8px;padding:10px 6px;flex-shrink:0\">';";
  H += "html+='<div style=\"font-size:30px;font-weight:800;color:'+probCol+';line-height:1\">'+m.prob+'%</div>';";
  H += "html+='<div style=\"font-size:12px;color:'+probCol+';margin-top:2px\">'+probLabel+'</div>';";
  H += "html+='<div style=\"font-size:10px;color:#9ca3af;margin-top:2px\">FH OVER 2.5</div>';";
  H += "html+='<div style=\"font-size:10px;color:#9ca3af;margin-top:4px\">Score '+m.modelScore.toFixed(2)+'</div>';";
  H += "html+='</div>';}";
  H += "html+='</div>';";
  H += "html+='<details><summary style=\"font-size:13px;color:#6b7280;padding:5px 0;border-top:1px solid #f3f4f6;margin-top:10px\">&#9660; Signal detail</summary>';";
  H += "html+='<div style=\"padding-top:10px\">';";
  H += "html+='<table class=\"sig-table\" style=\"margin-bottom:14px\"><thead><tr>';";
  H += "html+='<th style=\"width:30%\">Signal</th>';";
  H += "html+='<th style=\"width:16%;text-align:center\">'+shortName(m.home)+'</th>';";
  H += "html+='<th style=\"width:16%;text-align:center\">'+shortName(m.away)+'</th>';";
  H += "html+='<th style=\"width:18%;text-align:center\">Combined</th>';";
  H += "html+='<th style=\"width:20%;text-align:center\">Result</th>';";
  H += "html+='</tr></thead><tbody>'+sigRows+'</tbody></table>';";
  H += "html+=last5Table(m.home,m.hLast5||[],'last 5 overall');";
  H += "html+=last5Table(m.away,m.aLast5||[],'last 5 overall');";
  H += "html+=last5Table(m.home,m.hHome5||[],'last 5 home');";
  H += "html+=last5Table(m.away,m.aAway5||[],'last 5 away');";
  H += "html+='</div></details>';";
  H += "html+='</div></div>';";
  H += "return html;}";
  H += "document.getElementById('headerTitle').textContent=fmt(new Date(DATES[0]+'T12:00:00'));";
  H += "renderTabs();renderLeagueList();";
  H += "<\/script></body></html>";

  return H;
}

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
