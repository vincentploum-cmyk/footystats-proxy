require("dotenv").config();

const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");

const app = express();
app.use(cors());

const KEY = process.env.FOOTY_API_KEY;
const BASE = "https://api.football-data-api.com";
const PORT = process.env.PORT || 3001;

if (!KEY) {
  console.error("Missing FOOTY_API_KEY");
  process.exit(1);
}

// ─── LEAGUE REGISTRY ─────────────────────────────────────────────────────────
let LEAGUE_NAMES = {};

// ─── SERVER-LEVEL MATCH CACHE — built once at startup, refreshed hourly ──────
// Stores last 100 completed matches per team across all subscribed leagues
// so that cross-competition last 5 works without per-request API calls
let SERVER_MATCH_CACHE = {};
let SERVER_CACHE_BUILT_AT = 0;

async function buildServerMatchCache() {
  if (!Object.keys(LEAGUE_NAMES).length) return;
  console.log('Building server match cache for ' + Object.keys(LEAGUE_NAMES).length + ' leagues...');
  const newCache = {};
  let totalMatches = 0;

  // process in batches of 5 to avoid hammering the API
  const sids = Object.keys(LEAGUE_NAMES);
  for (let i = 0; i < sids.length; i += 5) {
    const batch = sids.slice(i, i + 5);
    await Promise.all(batch.map(async (sid) => {
      try {
        const leagueName = LEAGUE_NAMES[parseInt(sid, 10)] || 'League ' + sid;
        const r = await fetch(BASE + '/league-matches?season_id=' + sid + '&max_per_page=100&page=1&key=' + KEY).then(r => r.json());
        const completed = (r.data || []).filter(m => m.status === 'complete');
        for (const m of completed) {
          const slim = {
            homeID: m.homeID, awayID: m.awayID,
            home_name: m.home_name || '', away_name: m.away_name || '',
            date_unix: m.date_unix || 0,
            ht_goals_team_a: parseInt(m.ht_goals_team_a || 0, 10),
            ht_goals_team_b: parseInt(m.ht_goals_team_b || 0, 10),
            homeGoalCount: parseInt(m.homeGoalCount || 0, 10),
            awayGoalCount: parseInt(m.awayGoalCount || 0, 10),
            status: m.status,
            league: leagueName,
          };
          if (m.homeID) { if (!newCache[m.homeID]) newCache[m.homeID] = []; newCache[m.homeID].push(slim); }
          if (m.awayID) { if (!newCache[m.awayID]) newCache[m.awayID] = []; newCache[m.awayID].push(slim); }
          totalMatches++;
        }
      } catch(e) {
        // silently skip failed leagues
      }
    }));
  }

  SERVER_MATCH_CACHE = newCache;
  SERVER_CACHE_BUILT_AT = Date.now();
  console.log('Server match cache built: ' + Object.keys(newCache).length + ' teams, ' + totalMatches + ' match records');
}

// refresh cache every 60 minutes
setInterval(buildServerMatchCache, 60 * 60 * 1000);

async function fetchLeagueList() {
  try {
    const url = BASE + "/league-list?key=" + KEY;
    const data = await fetch(url).then((r) => r.json());
    const list = data.data || [];
    console.log("League-list: " + list.length + " leagues found");
    const map = {};
    for (const league of list) {
      const leagueName = league.league_name || league.name || "";
      const country = league.country || "";
      const name = country ? country + " · " + leagueName : leagueName;
      if (!name) continue;
      const seasons = league.season || [];
      for (const s of seasons) {
        if (s.id) map[parseInt(s.id, 10)] = name;
      }
    }
    LEAGUE_NAMES = map;
    console.log("Mapped " + Object.keys(map).length + " season IDs across " + list.length + " leagues");
    for (const league of list) {
      const seasons = league.season || [];
      const latest = seasons[seasons.length - 1];
      const name = league.league_name || league.name || "?";
      if (latest) console.log("  " + latest.id + " (" + latest.year + "): " + name);
    }
  } catch (e) {
    console.error("Failed to load league list: " + e.message);
    LEAGUE_NAMES = {};
  }
}

const PREV_SEASON = {
  16504: 13973,
  16544: 11321,
  16571: 15746,
  16614: 14086,
  16615: 14116,
  16036: 13703,
};

// ─── HELPERS ─────────────────────────────────────────────────────────────────
const ftch = (url) => fetch(url).then((r) => r.json());
const safe = (v) => (isNaN(v) || !isFinite(v) ? 0 : Number(v));
const safeDiv = (n, d) => (d > 0 ? n / d : 0);

const getDates = (tzOffset = 0) => {
  const now = new Date();
  const local = new Date(now.getTime() + tzOffset * 60 * 1000);
  const fmt = (d) => {
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
};

function unixToLocalDate(unix, tzOffset) {
  const local = new Date(unix * 1000 + tzOffset * 60 * 1000);
  const y = local.getUTCFullYear();
  const m = String(local.getUTCMonth() + 1).padStart(2, "0");
  const d = String(local.getUTCDate()).padStart(2, "0");
  return y + "-" + m + "-" + d;
}

// ─── SIGNAL ENGINE ───────────────────────────────────────────────────────────
function computeRank(snap) {
  const h = snap.home;
  const a = snap.away;

  const expFH = safe(h.scored_fh * a.conced_fh + a.scored_fh * h.conced_fh);
  const S1 = expFH >= 1.2;
  const S3 = h.btts_ht_pct >= 25 && a.btts_ht_pct >= 25;
  const S4 = h.o25ht_pct >= 20 || a.o25ht_pct >= 20;
  const M2 = h.cs_ht_pct <= 30 || a.cs_ht_pct <= 30;
  const M4 = h.cn010_avg >= 0.25 || a.cn010_avg >= 0.25;

  const medMet = [M2, M4].filter(Boolean).length;

  const rawOdds = snap.odds_fh_o15;
  const oddsAvail = rawOdds && rawOdds > 0 && rawOdds < 90;
  const oddsBoost = oddsAvail && rawOdds <= 2.0;
  const oddsStrong = oddsAvail && rawOdds <= 1.8;

  let rank, prob, label;

  if (S1 && S3 && S4) {
    rank = 4; prob = 44; label = "Strong Pick";
  } else if (S1 && S3) {
    rank = 3; prob = 38; label = "Worth Watching";
  } else if (S1 && S4) {
    rank = 3; prob = 38; label = "Worth Watching";
  } else if (S1 && medMet >= 1) {
    rank = 2; prob = 22; label = "Moderate";
  } else if ((S3 || S4) && medMet >= 1) {
    rank = 2; prob = 22; label = "Moderate";
  } else if (S1 || S3 || S4) {
    rank = 2; prob = 22; label = "Moderate";
  } else {
    rank = 1; prob = 13; label = "Low Signal";
  }

  if (oddsStrong && rank >= 4) {
    rank = 5; prob = 53; label = "Prime Pick";
  } else if (oddsBoost && rank === 4) {
    rank = 5; prob = 53; label = "Prime Pick";
  } else if (oddsBoost && rank === 3) {
    rank = 4; prob = 44; label = "Strong Pick";
  } else if (oddsStrong && rank === 2) {
    rank = 3; prob = 38; label = "Worth Watching";
  }

  const strongMet = [S1, S3, S4].filter(Boolean).length;

  return {
    rank, prob, label,
    eligible: rank >= 4,
    oddsAvail,
    expFH: +expFH.toFixed(3),
    strongMet,
    medMet,
    signals: {
      S1: { met: S1, noData: false, label: "Exp FH Goals >= 1.20", value: expFH.toFixed(2), threshold: ">= 1.20", tier: "strong" },
      S3: { met: S3, noData: false, label: "Both BTTS FH >= 25%", value: h.btts_ht_pct + "%/" + a.btts_ht_pct + "%", threshold: "both>=25%", tier: "strong" },
      S4: { met: S4, noData: false, label: "FH Over 2.5 >= 20%", value: h.o25ht_pct + "%/" + a.o25ht_pct + "%", threshold: "either>=20%", tier: "strong" },
      M2: { met: M2, noData: false, label: "FH Clean Sheet <= 30%", value: h.cs_ht_pct + "%/" + a.cs_ht_pct + "%", threshold: "either<=30%", tier: "medium" },
      M4: { met: M4, noData: false, label: "Early Goals >= 0.25/gm", value: h.cn010_avg.toFixed(2) + "/" + a.cn010_avg.toFixed(2), threshold: "either>=0.25", tier: "medium" },
    },
  };
}

// ─── EXTRACTORS ──────────────────────────────────────────────────────────────
function extractSnapshotStats(teamObj, role) {
  const s = teamObj.stats || {};
  const sfx = role === "home" ? "_home" : "_away";
  const mpR = s["seasonMatchesPlayed" + sfx] || 1;

  // pick role-specific field first; only fall back to overall if role field is null/undefined
  // critically: do NOT use || because 0 is a valid value and || would skip it
  // also fall back to overall if fewer than 3 role-specific games (small sample = unreliable)
  const pick = (roleKey, fallbackKey) => {
    const rv = s[roleKey];
    if (rv !== null && rv !== undefined && mpR >= 3) return rv;
    const fv = s[fallbackKey];
    if (fv !== null && fv !== undefined) return fv;
    return 0;
  };

  return {
    name: teamObj.name || teamObj.cleanName || "",
    scored_fh: safe(pick("scoredAVGHT" + sfx, "scoredAVGHT_overall")),
    conced_fh: safe(pick("concededAVGHT" + sfx, "concededAVGHT_overall")),
    btts_ht_pct: safe(pick("seasonBTTSPercentageHT" + sfx, "seasonBTTSPercentageHT_overall")),
    cs_ht_pct: safe(pick("seasonCSPercentageHT" + sfx, "seasonCSPercentageHT_overall")),
    o25ht_pct: safe(pick("seasonOver25PercentageHT" + sfx, "seasonOver25PercentageHT_overall")),
    o15ht_pct: safe(pick("seasonOver15PercentageHT" + sfx, "seasonOver15PercentageHT_overall")),
    fts_ht_pct: safe(pick("seasonFTSPercentageHT" + sfx, "seasonFTSPercentageHT_overall")),
    cn010_avg: safe(safeDiv(s["goals_conceded_min_0_to_10" + sfx] || 0, mpR)),
    scored_ft: safe(s.seasonScoredAVG_overall || 0),
    conced_ft: safe(s.seasonConcededAVG_overall || 0),
    o25ft_pct: safe(s.seasonOver25Percentage_overall || 0),
    ppg: safe(pick("seasonPPG" + sfx, "seasonPPG_overall")),
    mp: s.seasonMatchesPlayed_overall || 0,
    mpRole: mpR,
  };
}

function buildH2H(homeId, awayId, completedMatches) {
  return completedMatches
    .filter((m) => (m.homeID === homeId && m.awayID === awayId) || (m.homeID === awayId && m.awayID === homeId))
    .sort((a, b) => (b.date_unix || 0) - (a.date_unix || 0))
    .slice(0, 6)
    .map((m) => ({
      date_unix: m.date_unix,
      home_name: m.home_name || "",
      away_name: m.away_name || "",
      ht_goals_team_a: parseInt(m.ht_goals_team_a || 0, 10),
      ht_goals_team_b: parseInt(m.ht_goals_team_b || 0, 10),
      homeGoalCount: parseInt(m.homeGoalCount || 0, 10),
      awayGoalCount: parseInt(m.awayGoalCount || 0, 10),
    }));
}

// ─── DEBUG ENDPOINT — dump raw stats for a team ──────────────────────────
app.get("/debug/team/:teamId", async (req, res) => {
  try {
    const teamId = req.params.teamId;
    const url = BASE + "/team?team_id=" + teamId + "&include=stats&key=" + KEY;
    const data = await ftch(url);
    const team = (data.data && data.data[0]) || data.data || null;
    if (!team) return res.json({ error: "team not found", raw: data });
    const stats = team.stats || {};
    // filter to only FH-related keys so the response is readable
    const fhKeys = Object.keys(stats).filter(k =>
      k.match(/HT|ht|half|btts|BTTS|cs_|CS|clean|Clean|cn010|goals_conceded_min/i)
    );
    const fhStats = {};
    fhKeys.forEach(k => fhStats[k] = stats[k]);
    res.json({ teamId, name: team.name || team.cleanName, fhStats, allKeys: Object.keys(stats) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── FORM ENDPOINT — cross-competition last 5 ─────────────────────────────
app.get("/form/:teamId", async (req, res) => {
  try {
    const teamId = parseInt(req.params.teamId, 10);
    if (!teamId) return res.status(400).json({ error: "invalid teamId" });

    res.setHeader("Cache-Control", "no-store");
    const url = BASE + "/team-matches?team_id=" + teamId + "&max_per_page=20&page=1&key=" + KEY;
    const data = await ftch(url);
    const matches = (data.data || [])
      .filter((m) => m.status === "complete")
      .sort((a, b) => (b.date_unix || 0) - (a.date_unix || 0))
      .slice(0, 5)
      .map((m) => {
        const isHome = parseInt(m.homeID, 10) === teamId;
        const ftFor  = isHome ? parseInt(m.homeGoalCount || 0, 10) : parseInt(m.awayGoalCount || 0, 10);
        const ftAgst = isHome ? parseInt(m.awayGoalCount || 0, 10) : parseInt(m.homeGoalCount || 0, 10);
        const fhFor  = isHome ? parseInt(m.ht_goals_team_a || 0, 10) : parseInt(m.ht_goals_team_b || 0, 10);
        const fhAgst = isHome ? parseInt(m.ht_goals_team_b || 0, 10) : parseInt(m.ht_goals_team_a || 0, 10);
        const result = ftFor > ftAgst ? "W" : ftFor < ftAgst ? "L" : "D";
        const date = m.date_unix ? new Date(m.date_unix * 1000).toISOString().slice(0, 10) : "";
        const competition = m.competition || m.league_name || m.competition_id || "";
        const opp = isHome ? (m.away_name || "") : (m.home_name || "");
        return { date, venue: isHome ? "H" : "A", opp, competition, fhFor, fhAgst, ftFor, ftAgst, result };
      });

    // compute avg FH goals (total FH goals in each game / number of games)
    const avgFH = matches.length
      ? (matches.reduce((s, g) => s + g.fhFor + g.fhAgst, 0) / matches.length).toFixed(2)
      : null;

    res.json({ teamId, matches, avgFH });
  } catch (e) {
    console.error("/form/" + req.params.teamId + ": " + e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── API PASSTHROUGH ─────────────────────────────────────────────────────────
app.get("/api/*", async (req, res) => {
  try {
    const path = req.path.replace("/api", "");
    const qs = new URLSearchParams({ ...req.query, key: KEY }).toString();
    const data = await ftch(BASE + path + "?" + qs);
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── MAIN ROUTE ──────────────────────────────────────────────────────────────
app.get("/", async (req, res) => {
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const dates = getDates(tzOffset);
    const fetchedAt = new Date().toISOString().slice(0, 16).replace("T", " ");

    const dayResults = await Promise.all(
      dates.map((d) => ftch(BASE + "/todays-matches?date=" + d + "&key=" + KEY))
    );

    const allFixtures = [];
    for (let i = 0; i < dates.length; i++) {
      for (const m of dayResults[i].data || []) {
        allFixtures.push(Object.assign({}, m, { _date: dates[i] }));
      }
    }

    const leagueFixtures = {};
    const hasFilter = Object.keys(LEAGUE_NAMES).length > 0;
    for (const m of allFixtures) {
      const sid = parseInt(m.competition_id, 10);
      if (!hasFilter || LEAGUE_NAMES[sid]) {
        if (!leagueFixtures[sid]) leagueFixtures[sid] = [];
        leagueFixtures[sid].push(m);
      }
    }

    const preds = [];

    // global cross-league match cache: teamId -> array of slim matches
    const globalMatchCache = {};

    // merge server-level match cache into per-request global cache
    for (const [teamId, matches] of Object.entries(SERVER_MATCH_CACHE)) {
      globalMatchCache[teamId] = [...(globalMatchCache[teamId] || []), ...matches];
    }

    const slimMatch = (m, leagueName) => ({
      homeID: m.homeID, awayID: m.awayID,
      home_name: m.home_name || "", away_name: m.away_name || "",
      date_unix: m.date_unix || 0,
      ht_goals_team_a: parseInt(m.ht_goals_team_a || 0, 10),
      ht_goals_team_b: parseInt(m.ht_goals_team_b || 0, 10),
      homeGoalCount: parseInt(m.homeGoalCount || 0, 10),
      awayGoalCount: parseInt(m.awayGoalCount || 0, 10),
      status: m.status,
      league: leagueName || "",
    });

    const addToCache = (matches, leagueName) => {
      for (const m of matches) {
        if (m.status !== "complete") continue;
        const slim = slimMatch(m, leagueName);
        if (m.homeID) {
          if (!globalMatchCache[m.homeID]) globalMatchCache[m.homeID] = [];
          globalMatchCache[m.homeID].push(slim);
        }
        if (m.awayID) {
          if (!globalMatchCache[m.awayID]) globalMatchCache[m.awayID] = [];
          globalMatchCache[m.awayID].push(slim);
        }
      }
    };

    for (const sid of Object.keys(leagueFixtures)) {
      const fixtures = leagueFixtures[sid];
      if (!fixtures.length) continue;
      const leagueName = LEAGUE_NAMES[parseInt(sid, 10)] || "League " + sid;

      let completedMatches = [];
      try {
        const p1 = await ftch(BASE + "/league-matches?season_id=" + sid + "&max_per_page=100&page=1&key=" + KEY);
        completedMatches = (p1.data || []).filter((m) => m.status === "complete");
        addToCache(completedMatches, leagueName);

        if (completedMatches.length < 5 && PREV_SEASON[sid]) {
          const prev = await ftch(BASE + "/league-matches?season_id=" + PREV_SEASON[sid] + "&max_per_page=100&page=1&key=" + KEY);
          const prevCompleted = (prev.data || []).filter((m) => m.status === "complete");
          addToCache(prevCompleted, leagueName);
          completedMatches = [...completedMatches, ...prevCompleted];
        }
      } catch (e) {
        console.error("[" + sid + "] match fetch: " + e.message);
      }

      let teamMap = {};
      try {
        const tr = await ftch(BASE + "/league-teams?season_id=" + sid + "&include=stats&key=" + KEY);
        for (const t of tr.data || []) teamMap[t.id] = t;
        if (!Object.keys(teamMap).length && PREV_SEASON[sid]) {
          const tr2 = await ftch(BASE + "/league-teams?season_id=" + PREV_SEASON[sid] + "&include=stats&key=" + KEY);
          for (const t of tr2.data || []) teamMap[t.id] = t;
        }
      } catch (e) {
        console.error("[" + sid + "] team fetch: " + e.message);
      }

      for (const fixture of fixtures) {
        const homeId = fixture.homeID || fixture.home_id;
        const awayId = fixture.awayID || fixture.away_id;
        const homeTeam = teamMap[homeId];
        const awayTeam = teamMap[awayId];
        const missing = !homeTeam || !awayTeam;

        const matchDate = fixture.date_unix
          ? unixToLocalDate(fixture.date_unix, tzOffset)
          : fixture._date;

        let snapshot = null;
        let rankResult = null;

        if (!missing) {
          const hStats = extractSnapshotStats(homeTeam, "home");
          const aStats = extractSnapshotStats(awayTeam, "away");
          const odds_fh_o15 = parseFloat(fixture.odds_1st_half_over15 || 0) || null;
          const odds_ft_o25 = parseFloat(fixture.odds_ft_over25 || 0) || null;

          snapshot = {
            fetchedAt,
            home: hStats,
            away: aStats,
            odds_fh_o15: odds_fh_o15 || 99,
            odds_ft_o25: odds_ft_o25 || null,
          };
          rankResult = computeRank(snapshot);
        }

        // build last5 from global cross-league cache
        const buildLast5FromCache = (teamId) => {
          if (!teamId || !globalMatchCache[teamId]) return [];
          return globalMatchCache[teamId]
            .sort((a, b) => (b.date_unix || 0) - (a.date_unix || 0))
            .slice(0, 5)
            .map((m) => {
              const isHome = m.homeID === teamId;
              const ftFor  = isHome ? m.homeGoalCount : m.awayGoalCount;
              const ftAgst = isHome ? m.awayGoalCount : m.homeGoalCount;
              const fhFor  = isHome ? m.ht_goals_team_a : m.ht_goals_team_b;
              const fhAgst = isHome ? m.ht_goals_team_b : m.ht_goals_team_a;
              const result = ftFor > ftAgst ? 'W' : ftFor < ftAgst ? 'L' : 'D';
              const date   = m.date_unix ? new Date(m.date_unix * 1000).toISOString().slice(0, 10) : '';
              const opp    = isHome ? m.away_name : m.home_name;
              return { date, venue: isHome ? 'H' : 'A', opp, competition: m.league || '', fhFor, fhAgst, ftFor, ftAgst, result };
            });
        };

        const hLast5 = buildLast5FromCache(homeId);
        const aLast5 = buildLast5FromCache(awayId);
        const hAvgFH = hLast5.length ? (hLast5.reduce((s, g) => s + g.fhFor + g.fhAgst, 0) / hLast5.length).toFixed(2) : null;
        const aAvgFH = aLast5.length ? (aLast5.reduce((s, g) => s + g.fhFor + g.fhAgst, 0) / aLast5.length).toFixed(2) : null;

        const h2h = homeId && awayId ? buildH2H(homeId, awayId, completedMatches) : [];
        const isComplete = fixture.status === "complete";

        preds.push({
          id: fixture.id,
          homeId,
          awayId,
          league: LEAGUE_NAMES[parseInt(sid, 10)] || "League " + sid,
          leagueSid: parseInt(sid, 10),
          home: fixture.home_name || "",
          away: fixture.away_name || "",
          dt: (fixture.date_unix || 0) * 1000,
          matchDate,
          status: fixture.status || "upcoming",
          missingStats: missing,
          snapshot: snapshot ? {
            fetchedAt: snapshot.fetchedAt,
            odds_fh_o15: snapshot.odds_fh_o15,
            odds_ft_o25: snapshot.odds_ft_o25,
            home: {
              name: snapshot.home.name,
              scored_fh: snapshot.home.scored_fh,
              conced_fh: snapshot.home.conced_fh,
              btts_ht_pct: snapshot.home.btts_ht_pct,
              cs_ht_pct: snapshot.home.cs_ht_pct,
              o25ht_pct: snapshot.home.o25ht_pct,
              cn010_avg: snapshot.home.cn010_avg,
            },
            away: {
              name: snapshot.away.name,
              scored_fh: snapshot.away.scored_fh,
              conced_fh: snapshot.away.conced_fh,
              btts_ht_pct: snapshot.away.btts_ht_pct,
              cs_ht_pct: snapshot.away.cs_ht_pct,
              o25ht_pct: snapshot.away.o25ht_pct,
              cn010_avg: snapshot.away.cn010_avg,
            },
          } : null,
          rank: rankResult ? rankResult.rank : 0,
          prob: rankResult ? rankResult.prob : 0,
          label: rankResult ? rankResult.label : "No data",
          eligible: rankResult ? rankResult.eligible : false,
          oddsAvail: rankResult ? rankResult.oddsAvail : false,
          expFH: rankResult ? rankResult.expFH : 0,
          strongMet: rankResult ? rankResult.strongMet : 0,
          medMet: rankResult ? rankResult.medMet : 0,
          signals: rankResult ? rankResult.signals : {},
          hLast5,
          aLast5,
          hAvgFH,
          aAvgFH,
          h2h,
          result: isComplete ? {
            fhH: parseInt(fixture.ht_goals_team_a || 0, 10),
            fhA: parseInt(fixture.ht_goals_team_b || 0, 10),
            ftH: parseInt(fixture.homeGoalCount || 0, 10),
            ftA: parseInt(fixture.awayGoalCount || 0, 10),
            hit: parseInt(fixture.ht_goals_team_a || 0, 10) + parseInt(fixture.ht_goals_team_b || 0, 10) > 2,
          } : null,
        });
      }
    }

    preds.sort((a, b) => b.rank - a.rank || b.prob - a.prob);
    res.send(buildHTML(preds, dates));
  } catch (e) {
    console.error(e);
    res.status(500).send("<pre>Error: " + e.message + "\n" + e.stack + "</pre>");
  }
});

// ─── HTML BUILDER ────────────────────────────────────────────────────────────
function buildHTML(preds, dates) {
  const predsJSON = JSON.stringify(preds)
    .replace(/</g, "\\u003c")
    .replace(/>/g, "\\u003e")
    .replace(/&/g, "\\u0026");
  const datesJSON = JSON.stringify(dates);

  var J = "";
  J += "var ALL_PREDS=" + predsJSON + ";";
  J += "var DATES=" + datesJSON + ";";
  J += "var DAY_LABELS=['Today','Tomorrow','Day 3','Day 4','Day 5'];";
  J += "var activeDate=DATES[0]||null;";
  J += "var activeLeague=null;";

  J += "function fmtDate(d){return new Date(d).toLocaleDateString('en-GB',{weekday:'long',day:'2-digit',month:'short'});}";
  J += "function esc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\\\"/g,'&quot;');}";
  J += "function rankColor(r){return r===5?'#15803d':r===4?'#16a34a':r===3?'#d97706':r===2?'#9ca3af':'#d1d5db';}";
  J += "function rankBg(r){return r>=4?'#f0fdf4':r===3?'#fffbeb':'#f9fafb';}";
  J += "function rankBorder(r){return r>=4?'#bbf7d0':r===3?'#fde68a':'#e5e7eb';}";
  J += "function rankLeft(r){return r===5?'#15803d':r===4?'#16a34a':r===3?'#d97706':r===2?'#9ca3af':'#e5e7eb';}";
  J += "function emptyMsg(t){return '<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280\">'+t+'</div>';}";

  // ── stat box coloring ──
  // scored_fh: green if >=0.8, uncolored otherwise
  // conced_fh: red if >=0.8 (threshold MET = bad), uncolored otherwise
  // btts_ht_pct: green if >=25, uncolored otherwise
  // cs_ht_pct: green if <=30, uncolored otherwise
  // o25ht_pct: green if >=20, uncolored otherwise
  // cn010_avg: green if >=0.25, uncolored otherwise
  // All non-FH-conceded: if threshold NOT met → uncolored (no red)
  J += "function statBoxColor(key,val){";
  J += "  if(key==='scored_fh') return val>=0.8?{bg:'#f0fdf4',border:'#bbf7d0',col:'#16a34a'}:{bg:'',border:'#e5e7eb',col:'#111827'};";
  J += "  if(key==='conced_fh') return val>=0.8?{bg:'#fef2f2',border:'#fecaca',col:'#dc2626'}:{bg:'',border:'#e5e7eb',col:'#111827'};";
  J += "  if(key==='btts_ht_pct') return val>=25?{bg:'#f0fdf4',border:'#bbf7d0',col:'#16a34a'}:{bg:'',border:'#e5e7eb',col:'#111827'};";
  J += "  if(key==='cs_ht_pct') return val<=30?{bg:'#f0fdf4',border:'#bbf7d0',col:'#16a34a'}:{bg:'',border:'#e5e7eb',col:'#111827'};";
  J += "  if(key==='o25ht_pct') return val>=20?{bg:'#f0fdf4',border:'#bbf7d0',col:'#16a34a'}:{bg:'',border:'#e5e7eb',col:'#111827'};";
  J += "  if(key==='cn010_avg') return val>=0.25?{bg:'#f0fdf4',border:'#bbf7d0',col:'#16a34a'}:{bg:'',border:'#e5e7eb',col:'#111827'};";
  J += "  return {bg:'',border:'#e5e7eb',col:'#111827'};";
  J += "}";

  J += "function statBox(label,key,val,sigNum){";
  J += "  var c=statBoxColor(key,val);";
  J += "  var bg=c.bg||'#f9fafb';";
  J += "  return '<div style=\"background:'+bg+';border:1px solid '+c.border+';border-radius:7px;padding:8px 10px;text-align:center\">'";
  J += "    +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:3px\">'+esc(label)+'</div>'";
  J += "    +'<div style=\"font-family:monospace;font-weight:700;font-size:16px;color:'+c.col+'\">'+esc(val)+'</div>'";
  J += "    +'<div style=\"font-size:9px;color:#9ca3af;margin-top:2px\">&rarr; sig '+sigNum+'</div>'";
  J += "    +'</div>';";
  J += "}";

  J += "function formLetter(r){";
  J += "  var col=r==='W'?'#16a34a':r==='L'?'#dc2626':'#6b7280';";
  J += "  return '<span style=\"font-size:12px;font-weight:700;color:'+col+';margin-right:3px\">'+r+'</span>';";
  J += "}";

  J += "function renderTabs(){";
  J += "  var el=document.getElementById('dayTabs');var html='';";
  J += "  for(var i=0;i<DATES.length;i++){";
  J += "    var d=DATES[i];";
  J += "    var cnt=ALL_PREDS.filter(function(p){return p.matchDate===d;}).length;";
  J += "    var cls=d===activeDate?'tab active':'tab';";
  J += "    var lbl=DAY_LABELS[i]||d;";
  J += "    html+='<button class=\"'+cls+'\" data-di=\"'+i+'\">'+lbl+' <span style=\"font-size:11px;opacity:.7\">('+cnt+')</span></button>';";
  J += "  }";
  J += "  el.innerHTML=html;";
  J += "  el.querySelectorAll('[data-di]').forEach(function(btn){";
  J += "    btn.addEventListener('click',function(){";
  J += "      var i=Number(btn.getAttribute('data-di'));";
  J += "      activeDate=DATES[i];activeLeague=null;";
  J += "      renderTabs();renderLeagueList();";
  J += "      document.getElementById('headerTitle').textContent=fmtDate(new Date(DATES[i]+'T12:00:00'));";
  J += "    });";
  J += "  });";
  J += "}";

  J += "function renderLeagueList(){";
  J += "  var main=document.getElementById('mainView');";
  J += "  if(!activeDate){main.innerHTML=emptyMsg('No dates available.');return;}";
  J += "  var dayPreds=ALL_PREDS.filter(function(p){return p.matchDate===activeDate;});";
  J += "  var lmap={};";
  J += "  dayPreds.forEach(function(p){if(!lmap[p.league])lmap[p.league]=[];lmap[p.league].push(p);});";
  J += "  var llist=Object.entries(lmap).sort(function(a,b){";
  J += "    var aT=Math.max.apply(null,a[1].map(function(p){return p.rank;}));";
  J += "    var bT=Math.max.apply(null,b[1].map(function(p){return p.rank;}));";
  J += "    return bT-aT;});";
  J += "  if(!llist.length){main.innerHTML=emptyMsg('No matches found for this day.');return;}";
  J += "  var html='<div style=\"font-size:13px;color:#6b7280;margin-bottom:12px\">'+dayPreds.length+' matches across '+llist.length+' leagues &middot; sorted by highest rank</div>';";
  J += "  llist.forEach(function(e){";
  J += "    var league=e[0],matches=e[1];";
  J += "    var topRank=Math.max.apply(null,matches.map(function(p){return p.rank;}));";
  J += "    var eligN=matches.filter(function(p){return p.eligible;}).length;";
  J += "    var col=rankColor(topRank);";
  J += "    html+='<div class=\"league-card\" data-lg=\"'+esc(league)+'\">'";
  J += "      +'<div style=\"flex:1;min-width:0;margin-right:12px\">'";
  J += "      +'<div style=\"font-size:18px;font-weight:700;color:#111827;margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis\">'+esc(league)+'</div>'";
  J += "      +'<div style=\"font-size:13px;color:#6b7280\">'+matches.length+' match'+(matches.length>1?'es':'')";
  J += "      +(eligN?' &middot; <span style=\"color:#15803d;font-weight:700\">'+eligN+' eligible</span>':'')+'</div>'";
  J += "      +'</div>'";
  J += "      +'<div style=\"text-align:right;flex-shrink:0\">'";
  J += "      +'<div style=\"font-size:30px;font-weight:900;color:'+col+'\">'+topRank+'/5</div>'";
  J += "      +'<div style=\"font-size:11px;color:#9ca3af\">top rank</div>'";
  J += "      +'</div></div>';";
  J += "  });";
  J += "  main.innerHTML=html;";
  J += "  main.querySelectorAll('[data-lg]').forEach(function(el){";
  J += "    el.addEventListener('click',function(){activeLeague=el.getAttribute('data-lg');renderMatchList();});";
  J += "  });";
  J += "}";

  J += "function renderMatchList(){";
  J += "  var matches=ALL_PREDS.filter(function(p){return p.matchDate===activeDate&&p.league===activeLeague;}).sort(function(a,b){return b.rank-a.rank||b.prob-a.prob;});";
  J += "  var html='<div style=\"display:flex;align-items:center;gap:12px;margin-bottom:16px\">'";
  J += "    +'<button class=\"back-btn\" id=\"backBtn\">&#8592; Back</button>'";
  J += "    +'<div style=\"font-size:19px;font-weight:700;color:#111827\">'+esc(activeLeague)+'</div></div>';";
  J += "  matches.forEach(function(m){html+=renderMatchCard(m);});";
  J += "  document.getElementById('mainView').innerHTML=html;";
  J += "  document.getElementById('backBtn').addEventListener('click',function(){activeLeague=null;renderLeagueList();});";
  // trigger background form fetches for all visible cards
  J += "  matches.forEach(function(m){renderFormTable(m.id,m.homeId,m.home,{matches:m.hLast5||[],avgFH:m.hAvgFH});renderFormTable(m.id,m.awayId,m.away,{matches:m.aLast5||[],avgFH:m.aAvgFH});})";
  J += "}";

  // ── background form fetch ──
  // fetchForm removed — data now pre-computed server-side

  J += "function renderFormTable(matchId,teamId,teamName,d){";
  J += "  var el=document.getElementById('form-'+matchId+'-'+teamId);";
  J += "  if(!el)return;";
  J += "  var matches=d.matches||[];";
  J += "  if(!matches.length){el.innerHTML='<p style=\"font-size:12px;color:#9ca3af\">No recent games found.</p>';return;}";
  // update form strip
  J += "  var stripEl=document.getElementById('strip-'+matchId+'-'+teamId);";
  J += "  if(stripEl){";
  J += "    stripEl.innerHTML=matches.map(function(g){return formLetter(g.result);}).join('');";
  J += "  }";
  // build table
  J += "  var rows=matches.map(function(g){";
  J += "    var tot=g.fhFor+g.fhAgst,fire=tot>2;";
  J += "    var rc=g.result==='W'?'#16a34a':g.result==='L'?'#dc2626':'#6b7280';";
  J += "    return '<tr style=\"background:'+(fire?'#fff7ed':'')+'\"><td style=\"padding:5px 6px;font-size:11px;color:#6b7280\">'+esc(g.date)+'</td>'";
  J += "      +'<td style=\"padding:5px 6px;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">'+esc(g.competition)+'</td>'";
  J += "      +'<td style=\"padding:5px 6px;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">'+esc(g.opp)+'</td>'";
  J += "      +'<td style=\"text-align:center;padding:5px 6px;font-size:10px;color:#6b7280\">'+esc(g.venue)+'</td>'";
  J += "      +'<td style=\"text-align:center;padding:5px 6px;font-size:11px;font-weight:700;color:'+(fire?'#ea580c':'#374151')+'\">'+g.fhFor+'-'+g.fhAgst+'</td>'";
  J += "      +'<td style=\"text-align:center;padding:5px 6px;font-size:11px;color:#9ca3af\">'+g.ftFor+'-'+g.ftAgst+'</td>'";
  J += "      +'<td style=\"text-align:center;padding:5px 6px;font-size:12px;font-weight:700;color:'+rc+'\">'+esc(g.result)+'</td></tr>';";
  J += "  }).join('');";
  J += "  var avgRow='';";
  J += "  if(d.avgFH!==null){";
  J += "    avgRow='<tfoot><tr style=\"background:#f9fafb;border-top:1px solid #e5e7eb\">'";
  J += "      +'<td colspan=\"4\" style=\"padding:6px;font-size:10px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.5px\">Avg FH goals (last 5)</td>'";
  J += "      +'<td style=\"text-align:center;padding:6px;font-family:monospace;font-size:13px;font-weight:700;color:#2563eb\">'+d.avgFH+'</td>'";
  J += "      +'<td colspan=\"2\" style=\"padding:6px;font-size:10px;color:#9ca3af;text-align:center\">('+matches.length+' games)</td>'";
  J += "      +'</tr></tfoot>';";
  J += "  }";
  J += "  el.innerHTML='<div style=\"font-size:11px;font-weight:700;color:#374151;margin-bottom:6px\">'+esc(teamName)+' &mdash; last 5 (all competitions)</div>'";
  J += "    +'<table class=\"mini-table\" style=\"table-layout:fixed\"><thead><tr>'";
  J += "    +'<th style=\"width:20%\">Date</th><th style=\"width:22%\">Competition</th><th>Opponent</th>'";
  J += "    +'<th style=\"width:8%;text-align:center\">H/A</th><th style=\"width:11%;text-align:center\">FH</th>'";
  J += "    +'<th style=\"width:11%;text-align:center\">FT</th><th style=\"width:8%;text-align:center\">Res</th>'";
  J += "    +'</tr></thead><tbody>'+rows+'</tbody>'+avgRow+'</table>'";
  J += "    +'<div style=\"font-size:10px;color:#9ca3af;margin-top:4px\">All competitions &middot; W/D/L = full-time result</div>';";
  J += "}";

  J += "function sigRow(num,s){";
  J += "  if(!s)return '';";
  J += "  var icon,icol,rbg,lc,labelCol,valueCol;";
  J += "  if(s.noData){";
  J += "    icon='&#8212;';icol='#9ca3af';rbg='#f9fafb';lc='#e5e7eb';labelCol='#9ca3af';valueCol='#9ca3af';";
  J += "  } else if(s.met){";
  J += "    icon='&#10003;';icol='#15803d';rbg='#f0fdf4';lc='#16a34a';labelCol='#111827';valueCol='#15803d';";
  J += "  } else {";
  J += "    icon='&#10007;';icol='#dc2626';rbg='#fef2f2';lc='#dc2626';labelCol='#111827';valueCol='#dc2626';";
  J += "  }";
  J += "  var badgeBg=s.noData?'#9ca3af':s.tier==='strong'?'#15803d':'#ca8a04';";
  J += "  var tierBadge=s.tier==='strong'";
  J += "    ?'<span style=\"background:'+badgeBg+';color:#fff;font-size:9px;padding:1px 5px;border-radius:3px;margin-right:4px;font-weight:700\">STRONG</span>'";
  J += "    :'<span style=\"background:'+badgeBg+';color:#fff;font-size:9px;padding:1px 5px;border-radius:3px;margin-right:4px;font-weight:700\">MED</span>';";
  J += "  return '<div style=\"display:flex;align-items:flex-start;gap:10px;padding:10px 14px;background:'+rbg+';border-radius:7px;border-left:4px solid '+lc+';margin-bottom:6px;'+(s.noData?'opacity:0.55':'')+'\">'";
  J += "    +'<span style=\"font-size:16px;color:'+icol+';font-weight:700;min-width:20px;margin-top:1px\">'+icon+'</span>'";
  J += "    +'<div style=\"flex:1\">'";
  J += "    +'<div style=\"display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:4px\">'";
  J += "    +'<span style=\"font-weight:600;font-size:12px;color:'+labelCol+'\">'";
  J += "    +'<span style=\"background:#6b7280;color:#fff;font-size:9px;padding:1px 5px;border-radius:3px;margin-right:4px;font-weight:700\">'+num+'</span>'";
  J += "    +tierBadge+esc(s.label)+'</span>'";
  J += "    +'<div style=\"display:flex;align-items:center;gap:8px\">'";
  J += "    +'<span style=\"font-family:monospace;font-weight:700;font-size:13px;color:'+valueCol+'\">'+esc(s.value)+'</span>'";
  J += "    +'<span style=\"font-size:10px;color:#d1d5db\">('+esc(s.threshold)+')</span>'";
  J += "    +'</div></div></div></div>';";
  J += "}";

  J += "function probRefHTML(){";
  J += "  var rows=[{r:5,p:53,l:'S1+S3+S4 (market confirms)'},{r:4,p:44,l:'S1+S3+S4'},{r:3,p:38,l:'S1+S3, or S1+S4'},{r:2,p:22,l:'One weaker qualifying setup'},{r:1,p:13,l:'Base rate — no signals'}];";
  J += "  return rows.map(function(row){";
  J += "    var c=rankColor(row.r);";
  J += "    return '<div style=\"display:flex;align-items:center;gap:8px;margin-bottom:7px\">'";
  J += "      +'<div style=\"width:7px;height:7px;border-radius:50%;background:'+c+';flex-shrink:0\"></div>'";
  J += "      +'<div style=\"flex:1;font-size:11px;color:#374151\">'+esc(row.l)+'</div>'";
  J += "      +'<div style=\"font-family:monospace;font-weight:700;color:'+c+';font-size:13px;min-width:34px;text-align:right\">'+row.p+'%</div>'";
  J += "      +'<div style=\"width:80px;background:#e5e7eb;border-radius:3px;height:4px\">'";
  J += "      +'<div style=\"width:'+(row.p*2)+'%;background:'+c+';height:4px;border-radius:3px\"></div></div></div>';";
  J += "  }).join('');";
  J += "}";

  J += "function h2hTable(matches){";
  J += "  if(!matches||!matches.length)return '<p style=\"font-size:12px;color:#9ca3af;margin-top:8px\">No H2H in dataset.</p>';";
  J += "  var rows=matches.map(function(g){";
  J += "    var tot=parseInt(g.ht_goals_team_a||0)+parseInt(g.ht_goals_team_b||0),fire=tot>2;";
  J += "    var date=g.date_unix?new Date(g.date_unix*1000).toISOString().slice(0,10):'';";
  J += "    return '<tr style=\"background:'+(fire?'#fff7ed':'')+'\"><td>'+esc(date)+'</td><td>'+esc(g.home_name)+'</td><td>'+esc(g.away_name)+'</td>'";
  J += "      +'<td style=\"text-align:center;font-weight:700;color:'+(fire?'#ea580c':'#374151')+'\">'+parseInt(g.ht_goals_team_a||0)+'-'+parseInt(g.ht_goals_team_b||0)+'</td>'";
  J += "      +'<td style=\"text-align:center;color:#9ca3af\">'+parseInt(g.homeGoalCount||0)+'-'+parseInt(g.awayGoalCount||0)+'</td></tr>';";
  J += "  }).join('');";
  J += "  return '<div style=\"margin-top:12px\"><div style=\"font-size:11px;font-weight:700;color:#374151;margin-bottom:6px\">Head to Head</div>'";
  J += "    +'<table class=\"mini-table\"><thead><tr><th>Date</th><th>Home</th><th>Away</th><th>FH</th><th>FT</th></tr></thead><tbody>'+rows+'</tbody></table>'";
  J += "    +'<div style=\"font-size:10px;color:#9ca3af;margin-top:6px\">H2H is contextual only — not used in signal scoring</div></div>';";
  J += "}";

  J += "function renderMatchCard(m){";
  J += "  var col=rankColor(m.rank),bg=rankBg(m.rank),br=rankBorder(m.rank),left=rankLeft(m.rank);";
  J += "  var dt=m.dt?new Date(m.dt).toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}):m.matchDate;";
  J += "  var sc=m.status==='complete'?{bg:'#f3f4f6',border:'#e5e7eb',color:'#6b7280',txt:'Final'}:m.status==='live'?{bg:'#fef9c3',border:'#fde047',color:'#ca8a04',txt:'&#9679; Live'}:{bg:'#eff6ff',border:'#bfdbfe',color:'#2563eb',txt:'Upcoming'};";
  J += "  var statusBadge='<span style=\"padding:2px 8px;border-radius:20px;font-size:10px;font-weight:700;background:'+sc.bg+';border:1px solid '+sc.border+';color:'+sc.color+'\">'+sc.txt+'</span>';";
  J += "  var frozen=m.snapshot?'<span style=\"padding:2px 8px;border-radius:20px;font-size:10px;font-weight:600;background:#fffbeb;border:1px solid #fde68a;color:#92400e;margin-left:6px\">&#128274; '+esc((m.snapshot||{}).fetchedAt||'')+'</span>':'';";
  J += "  var missWarn=m.missingStats?'<span style=\"background:#fef3c7;color:#92400e;font-size:11px;padding:2px 7px;border-radius:4px;font-weight:600;margin-left:6px\">&#9888; missing stats</span>':'';";

  // ── team cards (home left / away right) with form strip + FH stats ──
  J += "  var homeCardBg=m.snapshot?'':'#f9fafb';";
  J += "  var fhScoredH=m.snapshot?m.snapshot.home.scored_fh:null;";
  J += "  var fhConcedH=m.snapshot?m.snapshot.home.conced_fh:null;";
  J += "  var fhScoredA=m.snapshot?m.snapshot.away.scored_fh:null;";
  J += "  var fhConcedA=m.snapshot?m.snapshot.away.conced_fh:null;";

  J += "  function fhStatMini(key,val){";
  J += "    if(val===null)return '';";
  J += "    var c=statBoxColor(key,val);";
  J += "    var bg=c.bg||'#fff';";
  J += "    return '<div style=\"flex:1;text-align:center;background:'+bg+';border:1px solid '+c.border+';border-radius:6px;padding:5px 4px\">'";
  J += "      +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.4px;margin-bottom:1px\">'+(key==='scored_fh'?'FH Scored':'FH Conceded')+'</div>'";
  J += "      +'<div style=\"font-size:15px;font-weight:700;color:'+c.col+'\">'+val.toFixed(2)+'</div>'";
  J += "      +'</div>';";
  J += "  }";

  J += "  var teamGrid='<div style=\"display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:10px;margin-bottom:12px\">';";
  // home card
  J += "  teamGrid+='<div style=\"background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:10px 12px\">';";
  J += "  teamGrid+='<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:3px\">Home</div>';";
  J += "  teamGrid+='<div style=\"font-size:17px;font-weight:800;color:#111827;margin-bottom:5px\">'+esc(m.home)+'</div>';";
  J += "  teamGrid+='<div id=\"strip-'+m.id+'-'+m.homeId+'\" style=\"margin-bottom:8px;min-height:18px\"><span style=\"font-size:11px;color:#9ca3af\">loading form...</span></div>';";
  J += "  if(fhScoredH!==null){teamGrid+='<div style=\"display:flex;gap:6px\">'+fhStatMini('scored_fh',fhScoredH)+fhStatMini('conced_fh',fhConcedH)+'</div>';}";
  J += "  teamGrid+='</div>';";
  // away card
  J += "  teamGrid+='<div style=\"background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:10px 12px\">';";
  J += "  teamGrid+='<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:3px\">Away</div>';";
  J += "  teamGrid+='<div style=\"font-size:17px;font-weight:800;color:#111827;margin-bottom:5px\">'+esc(m.away)+'</div>';";
  J += "  teamGrid+='<div id=\"strip-'+m.id+'-'+m.awayId+'\" style=\"margin-bottom:8px;min-height:18px\"><span style=\"font-size:11px;color:#9ca3af\">loading form...</span></div>';";
  J += "  if(fhScoredA!==null){teamGrid+='<div style=\"display:flex;gap:6px\">'+fhStatMini('scored_fh',fhScoredA)+fhStatMini('conced_fh',fhConcedA)+'</div>';}";
  J += "  teamGrid+='</div>';";
  J += "  teamGrid+='</div>';";

  // result block
  J += "  var resultHTML='';";
  J += "  if(m.result){";
  J += "    var rb=m.result.hit?'#f0fdf4':'#fef2f2',rbr=m.result.hit?'#bbf7d0':'#fecaca',rfc=m.result.hit?'#15803d':'#dc2626';";
  J += "    resultHTML='<div style=\"display:flex;align-items:stretch;gap:0;border:1px solid '+rbr+';border-radius:10px;overflow:hidden;margin-top:14px\">'";
  J += "      +'<div style=\"padding:10px 16px;background:'+rb+';text-align:center;border-right:1px solid '+rbr+'\">'";
  J += "      +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:2px\">1st Half</div>'";
  J += "      +'<div style=\"font-family:monospace;font-weight:800;font-size:22px;color:'+rfc+';line-height:1\">'+m.result.fhH+'&ndash;'+m.result.fhA+'</div>'";
  J += "      +'<div style=\"font-size:10px;font-weight:700;color:'+rfc+';margin-top:3px\">'+(m.result.hit?'&#10003; HIT':'&#10007; MISS')+'</div></div>'";
  J += "      +'<div style=\"padding:10px 14px;background:#f9fafb;text-align:center\">'";
  J += "      +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:2px\">Full Time</div>'";
  J += "      +'<div style=\"font-family:monospace;font-weight:700;font-size:16px;color:#374151;line-height:1\">'+m.result.ftH+'&ndash;'+m.result.ftA+'</div>'";
  J += "      +'<div style=\"font-size:9px;color:#9ca3af;margin-top:3px\">'+m.prob+'% pre-game</div></div></div>';";
  J += "  }";

  // full stats grid (expanded)
  J += "  var statsHTML='';";
  J += "  if(!m.missingStats&&m.snapshot){";
  J += "    var h=m.snapshot.home,a=m.snapshot.away;";
  J += "    statsHTML='<div style=\"display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:16px;margin-bottom:14px\">';";
  J += "    statsHTML+='<div><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px\">Home &mdash; '+esc(h.name)+'</div>'";
  J += "      +'<div style=\"display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:6px\">'";
  J += "      +statBox('Avg Scored FH','scored_fh',h.scored_fh.toFixed(2),1)";
  J += "      +statBox('Avg Conceded FH','conced_fh',h.conced_fh.toFixed(2),1)";
  J += "      +statBox('BTTS FH %','btts_ht_pct',h.btts_ht_pct+'%',2)";
  J += "      +statBox('FH Clean Sheet %','cs_ht_pct',h.cs_ht_pct+'%',4)";
  J += "      +statBox('FH Over 2.5 %','o25ht_pct',h.o25ht_pct+'%',3)";
  J += "      +statBox('Early Goals /gm','cn010_avg',h.cn010_avg.toFixed(2),5)";
  J += "      +'</div></div>';";
  J += "    statsHTML+='<div><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px\">Away &mdash; '+esc(a.name)+'</div>'";
  J += "      +'<div style=\"display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:6px\">'";
  J += "      +statBox('Avg Scored FH','scored_fh',a.scored_fh.toFixed(2),1)";
  J += "      +statBox('Avg Conceded FH','conced_fh',a.conced_fh.toFixed(2),1)";
  J += "      +statBox('BTTS FH %','btts_ht_pct',a.btts_ht_pct+'%',2)";
  J += "      +statBox('FH Clean Sheet %','cs_ht_pct',a.cs_ht_pct+'%',4)";
  J += "      +statBox('FH Over 2.5 %','o25ht_pct',a.o25ht_pct+'%',3)";
  J += "      +statBox('Early Goals /gm','cn010_avg',a.cn010_avg.toFixed(2),5)";
  J += "      +'</div></div>';";
  J += "    statsHTML+='</div>';";
  J += "    statsHTML+='<div style=\"background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:10px 14px\">'";
  J += "      +'<div style=\"font-size:10px;color:#1d4ed8;font-weight:700;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px\">Expected FH Goals</div>'";
  J += "      +'<div style=\"font-family:monospace;font-size:12px;color:#374151;line-height:2\">'";
  J += "      +'('+h.scored_fh.toFixed(2)+' &times; '+a.conced_fh.toFixed(2)+')'";
  J += "      +' + ('+a.scored_fh.toFixed(2)+' &times; '+h.conced_fh.toFixed(2)+')'";
  J += "      +' = <strong style=\"font-size:16px;color:'+(m.expFH>=1.2?'#dc2626':'#374151')+'\">'+m.expFH+'</strong>'";
  J += "      +' <span style=\"font-size:11px;color:'+(m.expFH>=1.2?'#15803d':'#9ca3af')+'\">'+(m.expFH>=1.2?'&#10003; &ge; 1.20':'&#10007; &lt; 1.20')+'</span>'";
  J += "      +'</div></div>';";
  J += "  }";

  J += "  var noStatsMsg=m.missingStats?'<div style=\"color:#9ca3af;font-size:13px;padding:12px 0\">No team stats available.</div>':'';";

  J += "  return '<div class=\"match-card\" style=\"border-left:4px solid '+left+'\">'";
  J += "    +'<div style=\"padding:18px 20px\">'";
  // header
  J += "    +'<div style=\"display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:12px\">'";
  J += "    +'<div>'";
  J += "    +'<div style=\"font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:4px\">'+esc(m.league)+' &middot; '+esc(dt)+missWarn+'</div>'";
  J += "    +'<div style=\"display:flex;align-items:center;gap:6px;flex-wrap:wrap\">'+statusBadge+frozen+'</div>'";
  J += "    +'</div>'";
  J += "    +'<div style=\"text-align:center;min-width:90px;background:'+bg+';border:1px solid '+br+';border-radius:10px;padding:10px 8px;flex-shrink:0\">'";
  J += "    +'<div style=\"font-size:32px;font-weight:900;color:'+col+';line-height:1\">'+m.rank+'/5</div>'";
  J += "    +'<div style=\"font-size:11px;color:'+col+';font-weight:700;margin-top:3px\">'+esc(m.label)+'</div>'";
  J += "    +'<div style=\"font-size:10px;color:#9ca3af;margin-top:4px\">'+m.prob+'% FH&gt;2.5</div>'";
  J += "    +'</div></div>'";
  // team grid
  J += "    +teamGrid";
  // signal pills
  J += "    +'<div style=\"display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap\">'";
  J += "    +'<div style=\"padding:3px 10px;border-radius:20px;background:'+(m.strongMet>0?'#f0fdf4':'#f3f4f6')+';border:1px solid '+(m.strongMet>0?'#86efac':'#e5e7eb')+'\">'";
  J += "    +'<span style=\"font-family:monospace;font-weight:700;font-size:11px;color:'+(m.strongMet>0?'#15803d':'#9ca3af')+'\">'+m.strongMet+'/3</span>'";
  J += "    +'<span style=\"font-size:10px;color:#9ca3af;margin-left:4px\">strong</span></div>'";
  J += "    +'<div style=\"padding:3px 10px;border-radius:20px;background:'+(m.medMet>0?'#fffbeb':'#f3f4f6')+';border:1px solid '+(m.medMet>0?'#fde68a':'#e5e7eb')+'\">'";
  J += "    +'<span style=\"font-family:monospace;font-weight:700;font-size:11px;color:'+(m.medMet>0?'#ca8a04':'#9ca3af')+'\">'+m.medMet+'/2</span>'";
  J += "    +'<span style=\"font-size:10px;color:#9ca3af;margin-left:4px\">medium</span></div>'";
  J += "    +'<div style=\"padding:3px 10px;border-radius:20px;background:#eff6ff;border:1px solid #bfdbfe\">'";
  J += "    +'<span style=\"font-family:monospace;font-weight:700;font-size:11px;color:#2563eb\">'+m.expFH+'</span>'";
  J += "    +'<span style=\"font-size:10px;color:#9ca3af;margin-left:4px\">exp FH goals</span></div>'";
  J += "    +'</div>'";
  J += "    +resultHTML";
  // expandable detail
  J += "    +'<details style=\"margin-top:14px\">'";
  J += "    +'<summary style=\"font-size:13px;color:#6b7280;cursor:pointer;padding-top:10px;border-top:1px solid #f3f4f6\">&#9660; Show full detail</summary>'";
  J += "    +'<div style=\"padding-top:14px\">'";
  // strong signals
  J += "    +'<div style=\"font-size:10px;font-weight:700;color:#15803d;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:8px\">&#9679; Strong Signals</div>'";
  J += "    +noStatsMsg";
  J += "    +(m.missingStats?'':sigRow(1,m.signals.S1)+sigRow(2,m.signals.S3)+sigRow(3,m.signals.S4))";
  J += "    +(m.missingStats?'':'<div style=\"font-size:10px;font-weight:700;color:#ca8a04;letter-spacing:1.5px;text-transform:uppercase;margin:14px 0 8px\">&#9670; Medium Signals</div>')";
  J += "    +(m.missingStats?'':sigRow(4,m.signals.M2)+sigRow(5,m.signals.M4))";
  // prob ref
  J += "    +'<div style=\"background:#f9fafb;border:1px solid #e5e7eb;border-radius:9px;padding:14px 16px;margin-top:14px\">'";
  J += "    +'<div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:10px;font-weight:600\">Probability Reference &middot; 5,699 matches</div>'";
  J += "    +probRefHTML()+'</div>'";
  // team stats
  J += "    +'<div style=\"font-size:11px;font-weight:700;color:#374151;margin-top:16px;margin-bottom:10px;text-transform:uppercase;letter-spacing:.8px\">Team Stats</div>'";
  J += "    +statsHTML";
  // form tables (populated by background fetch)
  J += "    +'<div id=\"form-'+m.id+'-'+m.homeId+'\" style=\"margin-top:14px\"><div style=\"font-size:12px;color:#9ca3af\">&#8635; Loading '+esc(m.home)+' recent games...</div></div>'";
  J += "    +'<div id=\"form-'+m.id+'-'+m.awayId+'\" style=\"margin-top:14px\"><div style=\"font-size:12px;color:#9ca3af\">&#8635; Loading '+esc(m.away)+' recent games...</div></div>'";
  J += "    +h2hTable(m.h2h)";
  J += "    +'</div></details></div></div>';";
  J += "}";

  J += "if(DATES.length){document.getElementById('headerTitle').textContent=fmtDate(new Date(DATES[0]+'T12:00:00'));}";
  J += "renderTabs();renderLeagueList();";

  return (
    "<!DOCTYPE html><html><head>" +
    "<meta charset=\"UTF-8\"/>" +
    "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/>" +
    "<title>FH Over 2.5 Predictor</title>" +
    "<script>(function(){var p=new URLSearchParams(window.location.search);if(!p.has('tz')){p.set('tz',-new Date().getTimezoneOffset());window.location.search=p.toString();}})();<\/script>" +
    "<style>" +
    "*{box-sizing:border-box;margin:0;padding:0}" +
    "body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;font-size:15px}" +
    "details>summary::-webkit-details-marker{display:none}" +
    ".tab{padding:8px 14px;border-radius:6px;cursor:pointer;font-size:14px;font-weight:600;border:1px solid #e5e7eb;background:#fff;color:#6b7280;transition:all .15s}" +
    ".tab.active{background:#111827;color:#fff;border-color:#111827}" +
    ".league-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:16px 18px;margin-bottom:10px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;box-shadow:0 1px 3px rgba(0,0,0,.05);transition:box-shadow .15s}" +
    ".league-card:hover{box-shadow:0 3px 8px rgba(0,0,0,.1)}" +
    ".back-btn{background:#f3f4f6;border:1px solid #e5e7eb;padding:7px 16px;border-radius:6px;cursor:pointer;font-size:15px;font-weight:600;color:#374151}" +
    ".match-card{background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:14px}" +
    ".mini-table{width:100%;border-collapse:collapse;font-size:12px}" +
    ".mini-table th{background:#f9fafb;padding:6px 8px;text-align:left;font-size:10px;color:#6b7280;font-weight:700;text-transform:uppercase;border-bottom:1px solid #e5e7eb}" +
    ".mini-table td{padding:6px 8px;border-bottom:1px solid #f3f4f6}" +
    "details summary{cursor:pointer;user-select:none;list-style:none}" +
    "</style></head><body>" +
    "<div style=\"background:#fff;border-bottom:1px solid #e5e7eb;padding:14px 20px;position:sticky;top:0;z-index:10\">" +
    "<div style=\"max-width:860px;margin:0 auto\">" +
    "<div style=\"display:flex;align-items:center;justify-content:space-between;margin-bottom:12px\">" +
    "<div><div style=\"font-size:11px;color:#6b7280;letter-spacing:1px;text-transform:uppercase\">&#9917; First Half Over 2.5</div>" +
    "<h1 style=\"font-size:22px;font-weight:800;color:#111827\" id=\"headerTitle\">Loading...</h1></div>" +
    "<button onclick=\"location.reload()\" style=\"background:#111827;color:#fff;padding:8px 16px;font-size:14px;border:none;border-radius:6px;font-weight:600;cursor:pointer\">&#8635; Refresh</button>" +
    "</div><div id=\"dayTabs\" style=\"display:flex;gap:8px;flex-wrap:wrap\"></div></div></div>" +
    "<div style=\"padding:16px 20px;max-width:860px;margin:0 auto\">" +
    "<div style=\"background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#92400e;line-height:1.6\">" +
    "<strong>How it works:</strong> 3 stat-based strong signals + 2 medium signals, backtested on 5,699 matches (base rate 13.2%). " +
    "Rank&nbsp;5&nbsp;=&nbsp;53% &middot; Rank&nbsp;4&nbsp;=&nbsp;44% &middot; Rank&nbsp;3&nbsp;=&nbsp;38% &middot; Rank&nbsp;2&nbsp;=&nbsp;22% &middot; Rank&nbsp;1&nbsp;=&nbsp;13%. " +
    "Odds used internally as a silent booster only.</div>" +
    "<div id=\"mainView\"></div></div>" +
    "<script>" + J + "<\/script>" +
    "</body></html>"
  );
}

fetchLeagueList().then(() => {
  buildServerMatchCache(); // fire and forget — cache builds in background
  app.listen(PORT, () => {
    console.log("Server running on port " + PORT);
    console.log("Memory at start: " + Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + "MB");
  });
});

process.on("uncaughtException", (e) => console.error("Uncaught:", e.message));
