const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");
const app = express();
app.use(cors());

const KEY  = "437fa5361a693ad65c0c97d75f55042da3529532df53b57d34fe28f89789c0e7";
const BASE = "https://api.football-data-api.com";

// ─── LEAGUE REGISTRY — loaded from your FootyStats subscription on startup ────
// Populated by fetchLeagueList() below. Falls back to empty object which means
// only leagues with a known ID will filter through (all of yours will).
let LEAGUE_NAMES = {};

async function fetchLeagueList() {
  try {
    const url  = BASE + "/league-list?key=" + KEY;
    const data = await fetch(url).then(r => r.json());
    const list = data.data || [];
    const map  = {};
    for (const league of list) {
      // Each league object has: id, name, country, season, season_id
      // We key by season_id so it matches competition_id in match data
      if (league.season_id && league.name) {
        map[parseInt(league.season_id)] = league.name;
      }
      // Also key by id in case some endpoints use that
      if (league.id && league.name) {
        map[parseInt(league.id)] = league.name;
      }
    }
    LEAGUE_NAMES = map;
    console.log("Loaded " + Object.keys(map).length + " leagues from FootyStats subscription:");
    list.forEach(l => console.log("  " + l.season_id + " / " + l.id + ": " + l.name));
  } catch (e) {
    console.error("Failed to load league list: " + e.message);
    // Keep LEAGUE_NAMES as empty — matches will still show but without filtering
    LEAGUE_NAMES = {};
  }
}

const PREV_SEASON = {
  16504:13973, 16544:11321, 16571:15746,
  16614:14086, 16615:14116, 16036:13703,
};

// ─── HELPERS ─────────────────────────────────────────────────────────────────
const ftch    = url => fetch(url).then(r => r.json());
const safe    = v   => (isNaN(v) || !isFinite(v)) ? 0 : v;
const safeDiv = (n, d) => d > 0 ? n / d : 0;

const getDates = (tzOffset = 0) => {
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
};

function unixToLocalDate(unix, tzOffset) {
  const local = new Date((unix * 1000) + tzOffset * 60 * 1000);
  const y = local.getUTCFullYear();
  const m = String(local.getUTCMonth() + 1).padStart(2, "0");
  const d = String(local.getUTCDate()).padStart(2, "0");
  return y + "-" + m + "-" + d;
}

// ─── SIGNAL ENGINE ───────────────────────────────────────────────────────────
// Backtested on 5,699 matches. Base rate 13.2%.
//
// STRONG (use role-specific FH stats):
//   S1  ExpFH = (hScored x aConceded) + (aScored x hConceded) >= 1.20  → 3.20x lift
//   S2  FH odds over 1.5 <= 1.80                                        → 3.61x lift
//   S3  BOTH teams BTTS FH % >= 25%                                     → 4.06x lift
//
// MEDIUM:
//   M1  FH odds over 1.5 <= 2.00                                        → 2.97x lift
//   M2  Either team FH clean sheet % <= 30%                             → 3.30x lift
//   M3  Either team FH over 2.5 % >= 20%                                → 3.46x lift
//   M4  Either team early goals (0-10 min) per game >= 0.25             → 3.30x lift
//
// RANK:
//   5 = S1 + S2 + S3         → 48%
//   4 = S1 + (S2 or S3)      → 42%
//   3 = S1 + M1 + medMet>=2  → 35%
//   2 = M1 + medMet>=2       → 25%
//   1 = everything else      → 13%

function computeRank(snap) {
  const h    = snap.home;
  const a    = snap.away;

  // Odds: null/0 means genuinely unavailable — NOT a miss, just unknown
  const rawOdds   = snap.odds_fh_o15;
  const oddsAvail = rawOdds && rawOdds > 0 && rawOdds < 90;
  const odds      = oddsAvail ? rawOdds : null;

  const expFH = safe((h.scored_fh * a.conced_fh) + (a.scored_fh * h.conced_fh));

  // Stat-only signals — always computable
  const S1 = expFH >= 1.20;
  const S3 = h.btts_ht_pct >= 25 && a.btts_ht_pct >= 25;
  const M2 = h.cs_ht_pct  <= 30 || a.cs_ht_pct  <= 30;
  const M3 = h.o25ht_pct  >= 20 || a.o25ht_pct  >= 20;
  const M4 = h.cn010_avg  >= 0.25 || a.cn010_avg >= 0.25;

  // Single merged odds signal — three tiers:
  //   "strong"  → odds <= 1.80  (was S2)
  //   "medium"  → odds <= 2.00  (was M1)
  //   "miss"    → odds >  2.00
  //   "noData"  → no odds from API
  const oddsStrong = oddsAvail && odds <= 1.80;
  const oddsMedium = oddsAvail && odds <= 2.00;  // includes strong
  const oddsTier   = !oddsAvail ? "noData"
                   : odds <= 1.80 ? "strong"
                   : odds <= 2.00 ? "medium"
                   : "miss";

  // Count signals for rank computation
  const strongMet = [S1, oddsStrong, S3].filter(Boolean).length;
  const medMet    = [oddsMedium, M2, M3, M4].filter(Boolean).length;

  // Rank logic — odds signals absent when unavailable, stat signals always score
  const suffix = oddsAvail ? "" : "*";

  let rank, prob, label;
  if      (S1 && oddsStrong && S3)               { rank=5; prob=48; label="Prime Pick";           }
  else if (S1 && S3)                             { rank=4; prob=42; label="Strong Pick"+suffix;   }
  else if (S1 && oddsStrong)                     { rank=4; prob=42; label="Strong Pick";          }
  else if (S1 && oddsMedium && medMet >= 2)      { rank=3; prob=35; label="Worth Watching";       }
  else if (S1 && !oddsAvail && (M2||M3||M4))     { rank=3; prob=35; label="Worth Watching"+suffix;}
  else if (oddsMedium && medMet >= 2)            { rank=2; prob=25; label="Moderate";             }
  else if (!oddsAvail && (M2||M3||M4))           { rank=2; prob=25; label="Moderate"+suffix;      }
  else                                           { rank=1; prob=13; label="Low Signal";           }

  // Build odds signal display — single row, colour driven by tier
  const oddsSignal = {
    label:    "FH Odds Over 1.5",
    value:    oddsAvail ? odds.toFixed(2) : "no data",
    noData:   !oddsAvail,
    oddsTier,                          // "strong" | "medium" | "miss" | "noData"
    // met/tier for display logic
    metStrong: oddsStrong,
    metMedium: oddsMedium && !oddsStrong,
  };

  return {
    rank, prob, label,
    eligible:  rank >= 4,
    oddsAvail,
    expFH:     +expFH.toFixed(3),
    strongMet, medMet,
    signals: {
      S1: { met:S1,  noData:false, label:"Exp FH Goals >= 1.20",  value:expFH.toFixed(2),                              threshold:">= 1.20",    tier:"strong" },
      OD: oddsSignal,
      S3: { met:S3,  noData:false, label:"Both BTTS FH >= 25%",   value:h.btts_ht_pct+"%/"+a.btts_ht_pct+"%",         threshold:"both>=25%",  tier:"strong" },
      M2: { met:M2,  noData:false, label:"FH Clean Sheet <= 30%", value:h.cs_ht_pct+"%/"+a.cs_ht_pct+"%",             threshold:"either<=30%", tier:"medium" },
      M3: { met:M3,  noData:false, label:"FH Over 2.5 >= 20%",    value:h.o25ht_pct+"%/"+a.o25ht_pct+"%",             threshold:"either>=20%", tier:"medium" },
      M4: { met:M4,  noData:false, label:"Early Goals >= 0.25/gm",value:h.cn010_avg.toFixed(2)+"/"+a.cn010_avg.toFixed(2), threshold:"either>=0.25", tier:"medium" },
    },
  };
}

// ─── EXTRACTORS ──────────────────────────────────────────────────────────────
function extractSnapshotStats(teamObj, role) {
  const s   = teamObj.stats || {};
  const sfx = role === "home" ? "_home" : "_away";
  const mpR = s["seasonMatchesPlayed"+sfx] || 1;
  return {
    name:        teamObj.name || teamObj.cleanName || "",
    scored_fh:   safe(s["scoredAVGHT"+sfx]           || s.scoredAVGHT_overall    || 0),
    conced_fh:   safe(s["concededAVGHT"+sfx]         || s.concededAVGHT_overall  || 0),
    btts_ht_pct: safe(s["seasonBTTSPercentageHT"+sfx]|| s.seasonBTTSPercentageHT_overall || 0),
    cs_ht_pct:   safe(s["seasonCSPercentageHT"+sfx]  || s.seasonCSPercentageHT_overall   || 0),
    o25ht_pct:   safe(s.seasonOver25PercentageHT_overall || 0),
    o15ht_pct:   safe(s.seasonOver15PercentageHT_overall || 0),
    fts_ht_pct:  safe(s["seasonFTSPercentageHT"+sfx] || s.seasonFTSPercentageHT_overall  || 0),
    cn010_avg:   safe(safeDiv(s["goals_conceded_min_0_to_10"+sfx] || 0, mpR)),
    scored_ft:   safe(s.seasonScoredAVG_overall   || 0),
    conced_ft:   safe(s.seasonConcededAVG_overall || 0),
    o25ft_pct:   safe(s.seasonOver25Percentage_overall || 0),
    ppg:         safe(s["seasonPPG"+sfx] || s.seasonPPG_overall || 0),
    mp:          s.seasonMatchesPlayed_overall || 0,
    mpRole:      mpR,
  };
}

function buildLast5(teamId, role, completedMatches) {
  return completedMatches
    .filter(m => (role==="home" ? m.homeID : m.awayID) === teamId)
    .sort((a,b) => (b.date_unix||0)-(a.date_unix||0))
    .slice(0,5)
    .map(m => {
      const isHome = m.homeID === teamId;
      return {
        date:    m.date_unix ? new Date(m.date_unix*1000).toISOString().slice(0,10) : "",
        venue:   isHome ? "H" : "A",
        opp:     isHome ? (m.away_name||"") : (m.home_name||""),
        fhFor:   isHome ? parseInt(m.ht_goals_team_a||0) : parseInt(m.ht_goals_team_b||0),
        fhAgst:  isHome ? parseInt(m.ht_goals_team_b||0) : parseInt(m.ht_goals_team_a||0),
        ftFor:   isHome ? parseInt(m.homeGoalCount||0) : parseInt(m.awayGoalCount||0),
        ftAgst:  isHome ? parseInt(m.awayGoalCount||0) : parseInt(m.homeGoalCount||0),
      };
    });
}

function buildH2H(homeId, awayId, completedMatches) {
  return completedMatches
    .filter(m =>
      (m.homeID===homeId && m.awayID===awayId) ||
      (m.homeID===awayId && m.awayID===homeId)
    )
    .sort((a,b) => (b.date_unix||0)-(a.date_unix||0))
    .slice(0,6)
    .map(m => ({
      date_unix:        m.date_unix,
      home_name:        m.home_name || "",
      away_name:        m.away_name || "",
      ht_goals_team_a:  parseInt(m.ht_goals_team_a||0),
      ht_goals_team_b:  parseInt(m.ht_goals_team_b||0),
      homeGoalCount:    parseInt(m.homeGoalCount||0),
      awayGoalCount:    parseInt(m.awayGoalCount||0),
    }));
}

// ─── API PASSTHROUGH ─────────────────────────────────────────────────────────
app.get("/api/*", async (req,res) => {
  try {
    const path = req.path.replace("/api","");
    const qs   = new URLSearchParams({...req.query, key:KEY}).toString();
    const data = await ftch(BASE+path+"?"+qs);
    res.json(data);
  } catch(e) { res.status(500).json({error:e.message}); }
});

// ─── MAIN ROUTE ──────────────────────────────────────────────────────────────
app.get("/", async (req,res) => {
  try {
    const tzOffset  = parseInt(req.query.tz || "0");
    const dates     = getDates(tzOffset);
    const fetchedAt = new Date().toISOString().slice(0,16).replace("T"," ");

    // Fetch fixtures for all days
    const dayResults = await Promise.all(
      dates.map(d => ftch(BASE+"/todays-matches?date="+d+"&key="+KEY))
    );
    const allFixtures = [];
    for (let i=0; i<dates.length; i++) {
      for (const m of (dayResults[i].data||[])) {
        allFixtures.push(Object.assign({},m,{_date:dates[i]}));
      }
    }

    // Group by league — if LEAGUE_NAMES is empty (fetch failed), include all
    const leagueFixtures = {};
    const hasFilter = Object.keys(LEAGUE_NAMES).length > 0;
    for (const m of allFixtures) {
      const sid = parseInt(m.competition_id);
      if (!hasFilter || LEAGUE_NAMES[sid]) {
        if (!leagueFixtures[sid]) leagueFixtures[sid] = [];
        leagueFixtures[sid].push(m);
      }
    }

    const preds = [];

    // Per-league processing
    for (const sid of Object.keys(leagueFixtures)) {
      const fixtures = leagueFixtures[sid];
      if (!fixtures.length) continue;

      // Slim mapper — keep only what we need for H2H + last5
      // Discard full match payload immediately to save memory
      const slimMatch = m => ({
        homeID:          m.homeID,
        awayID:          m.awayID,
        home_name:       m.home_name||"",
        away_name:       m.away_name||"",
        date_unix:       m.date_unix||0,
        ht_goals_team_a: parseInt(m.ht_goals_team_a||0),
        ht_goals_team_b: parseInt(m.ht_goals_team_b||0),
        homeGoalCount:   parseInt(m.homeGoalCount||0),
        awayGoalCount:   parseInt(m.awayGoalCount||0),
        status:          m.status,
      });

      // Fetch completed matches — page 1 only, max 60 most recent
      let completedMatches = [];
      try {
        const p1 = await ftch(BASE+"/league-matches?season_id="+sid+"&max_per_page=100&page=1&key="+KEY);
        completedMatches = (p1.data||[])
          .filter(m => m.status==="complete")
          .map(slimMatch)
          .slice(-60);

        // Fallback to previous season if too few
        if (completedMatches.length < 5 && PREV_SEASON[sid]) {
          const prev = await ftch(BASE+"/league-matches?season_id="+PREV_SEASON[sid]+"&max_per_page=100&page=1&key="+KEY);
          completedMatches = (prev.data||[])
            .filter(m => m.status==="complete")
            .map(slimMatch)
            .slice(-60);
        }
      } catch(e) { console.error("["+sid+"] match fetch: "+e.message); }

      // Fetch team stats
      let teamMap = {};
      try {
        const tr = await ftch(BASE+"/league-teams?season_id="+sid+"&include=stats&key="+KEY);
        for (const t of (tr.data||[])) teamMap[t.id] = t;

        if (!Object.keys(teamMap).length && PREV_SEASON[sid]) {
          const tr2 = await ftch(BASE+"/league-teams?season_id="+PREV_SEASON[sid]+"&include=stats&key="+KEY);
          for (const t of (tr2.data||[])) teamMap[t.id] = t;
        }
      } catch(e) { console.error("["+sid+"] team fetch: "+e.message); }

      // Build prediction per fixture
      for (const fixture of fixtures) {
        const homeId   = fixture.homeID || fixture.home_id;
        const awayId   = fixture.awayID || fixture.away_id;
        const homeTeam = teamMap[homeId];
        const awayTeam = teamMap[awayId];
        const missing  = !homeTeam || !awayTeam;

        const matchDate = fixture.date_unix
          ? unixToLocalDate(fixture.date_unix, tzOffset)
          : fixture._date;

        let snapshot   = null;
        let rankResult = null;

        if (!missing) {
          const hStats       = extractSnapshotStats(homeTeam, "home");
          const aStats       = extractSnapshotStats(awayTeam, "away");
          const odds_fh_o15  = parseFloat(fixture.odds_1st_half_over15||0) || null;
          const odds_ft_o25  = parseFloat(fixture.odds_ft_over25||0)       || null;

          snapshot = {
            fetchedAt,
            home:        hStats,
            away:        aStats,
            odds_fh_o15: odds_fh_o15 || 99,
            odds_ft_o25: odds_ft_o25 || null,
          };
          rankResult = computeRank(snapshot);
        }

        const hLast5    = homeId ? buildLast5(homeId,"home",completedMatches) : [];
        const aLast5    = awayId ? buildLast5(awayId,"away",completedMatches) : [];
        const h2h       = (homeId&&awayId) ? buildH2H(homeId,awayId,completedMatches) : [];
        const isComplete= fixture.status==="complete";

        preds.push({
          id:          fixture.id,
          league:      LEAGUE_NAMES[parseInt(sid)] || "League " + sid,
          leagueSid:   parseInt(sid),
          home:        fixture.home_name||"",
          away:        fixture.away_name||"",
          dt:          (fixture.date_unix||0)*1000,
          matchDate,
          status:      fixture.status||"upcoming",
          missingStats: missing,
          // Snapshot: only the display fields the client needs
          snapshot: snapshot ? {
            fetchedAt:   snapshot.fetchedAt,
            odds_fh_o15: snapshot.odds_fh_o15,
            odds_ft_o25: snapshot.odds_ft_o25,
            home: {
              name:        snapshot.home.name,
              scored_fh:   snapshot.home.scored_fh,
              conced_fh:   snapshot.home.conced_fh,
              btts_ht_pct: snapshot.home.btts_ht_pct,
              cs_ht_pct:   snapshot.home.cs_ht_pct,
              o25ht_pct:   snapshot.home.o25ht_pct,
              cn010_avg:   snapshot.home.cn010_avg,
            },
            away: {
              name:        snapshot.away.name,
              scored_fh:   snapshot.away.scored_fh,
              conced_fh:   snapshot.away.conced_fh,
              btts_ht_pct: snapshot.away.btts_ht_pct,
              cs_ht_pct:   snapshot.away.cs_ht_pct,
              o25ht_pct:   snapshot.away.o25ht_pct,
              cn010_avg:   snapshot.away.cn010_avg,
            },
          } : null,
          rank:        rankResult ? rankResult.rank      : 0,
          prob:        rankResult ? rankResult.prob      : 0,
          label:       rankResult ? rankResult.label     : "No data",
          eligible:    rankResult ? rankResult.eligible  : false,
          oddsAvail:   rankResult ? rankResult.oddsAvail : false,
          expFH:       rankResult ? rankResult.expFH     : 0,
          strongMet:   rankResult ? rankResult.strongMet : 0,
          medMet:      rankResult ? rankResult.medMet    : 0,
          signals:     rankResult ? rankResult.signals   : {},
          hLast5, aLast5, h2h,
          result: isComplete ? {
            fhH: parseInt(fixture.ht_goals_team_a||0),
            fhA: parseInt(fixture.ht_goals_team_b||0),
            ftH: parseInt(fixture.homeGoalCount||0),
            ftA: parseInt(fixture.awayGoalCount||0),
            hit: (parseInt(fixture.ht_goals_team_a||0)+parseInt(fixture.ht_goals_team_b||0)) > 2,
          } : null,
        });
      }
    }

    preds.sort((a,b) => b.rank-a.rank || b.prob-a.prob);
    res.send(buildHTML(preds, dates));

  } catch(e) {
    console.error(e);
    res.status(500).send("<pre>Error: "+e.message+"\n"+e.stack+"</pre>");
  }
});

// ─── HTML BUILDER ────────────────────────────────────────────────────────────
function buildHTML(preds, dates) {
  const predsJSON = JSON.stringify(preds)
    .replace(/</g,"\\u003c").replace(/>/g,"\\u003e").replace(/&/g,"\\u0026");
  const datesJSON = JSON.stringify(dates);

  // ── All client JS built with string concatenation — zero template literal nesting ──
  var J = "";
  J += "var ALL_PREDS="+predsJSON+";";
  J += "var DATES="+datesJSON+";";
  J += "var DAY_LABELS=['Today','Tomorrow','Day 3','Day 4','Day 5'];";
  J += "var activeDate=DATES[0]||null;";
  J += "var activeLeague=null;";

  J += "function fmtDate(d){return new Date(d).toLocaleDateString('en-GB',{weekday:'long',day:'2-digit',month:'short'});}";
  J += "function esc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;');}";
  J += "function rankColor(r){return r===5?'#15803d':r===4?'#16a34a':r===3?'#d97706':r===2?'#9ca3af':'#d1d5db';}";
  J += "function rankBg(r){return r>=4?'#f0fdf4':r===3?'#fffbeb':'#f9fafb';}";
  J += "function rankBorder(r){return r>=4?'#bbf7d0':r===3?'#fde68a':'#e5e7eb';}";
  J += "function rankLeft(r){return r===5?'#15803d':r===4?'#16a34a':r===3?'#d97706':r===2?'#9ca3af':'#e5e7eb';}";
  J += "function emptyMsg(t){return '<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280\">'+t+'</div>';}";

  // renderTabs
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

  // renderLeagueList
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

  // renderMatchList
  J += "function renderMatchList(){";
  J += "  var matches=ALL_PREDS.filter(function(p){return p.matchDate===activeDate&&p.league===activeLeague;}).sort(function(a,b){return b.rank-a.rank||b.prob-a.prob;});";
  J += "  var html='<div style=\"display:flex;align-items:center;gap:12px;margin-bottom:16px\">'";
  J += "    +'<button class=\"back-btn\" id=\"backBtn\">&#8592; Back</button>'";
  J += "    +'<div style=\"font-size:19px;font-weight:700;color:#111827\">'+esc(activeLeague)+'</div></div>';";
  J += "  matches.forEach(function(m){html+=renderMatchCard(m);});";
  J += "  document.getElementById('mainView').innerHTML=html;";
  J += "  document.getElementById('backBtn').addEventListener('click',function(){activeLeague=null;renderLeagueList();});";
  J += "}";

  // sigRow — renders a standard signal row (met/missed/noData)
  // oddsRow — renders the merged odds signal with 3 colour tiers
  J += "function sigRow(s){";
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
  J += "  var badge=s.tier==='strong'";
  J += "    ?'<span style=\"background:'+badgeBg+';color:#fff;font-size:9px;padding:1px 5px;border-radius:3px;margin-right:6px;font-weight:700\">STRONG</span>'";
  J += "    :'<span style=\"background:'+badgeBg+';color:#fff;font-size:9px;padding:1px 5px;border-radius:3px;margin-right:6px;font-weight:700\">MED</span>';";
  J += "  var noDataNote=s.noData?'<div style=\"font-size:10px;color:#9ca3af;margin-top:3px;font-style:italic\">No odds from API \u2014 check your sportsbook for the line</div>':'';";
  J += "  return '<div style=\"display:flex;align-items:flex-start;gap:10px;padding:10px 14px;background:'+rbg+';border-radius:7px;border-left:4px solid '+lc+';margin-bottom:6px;'+(s.noData?'opacity:0.55':'')+'\">'";
  J += "    +'<span style=\"font-size:16px;color:'+icol+';font-weight:700;min-width:20px;margin-top:1px\">'+icon+'</span>'";
  J += "    +'<div style=\"flex:1\">'";
  J += "    +'<div style=\"display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:4px\">'";
  J += "    +'<span style=\"font-weight:600;font-size:12px;color:'+labelCol+'\">'+badge+esc(s.label)+'</span>'";
  J += "    +'<div style=\"display:flex;align-items:center;gap:8px\">'";
  J += "    +'<span style=\"font-family:monospace;font-weight:700;font-size:13px;color:'+valueCol+'\">'+esc(s.value)+'</span>'";
  J += "    +'<span style=\"font-size:10px;color:#d1d5db\">('+esc(s.threshold)+')</span>'";
  J += "    +'</div></div>'";
  J += "    +noDataNote";
  J += "    +'</div></div>';";
  J += "}";

  // oddsRow — merged odds signal: green if <=1.80 (strong), amber if <=2.00 (medium), red if >2.00
  J += "function oddsRow(od){";
  J += "  if(!od)return '';";
  J += "  var icon,icol,rbg,lc,valueCol,sublabel,badgeBg,badgeTxt;";
  J += "  if(od.noData){";
  J += "    icon='&#8212;';icol='#9ca3af';rbg='#f9fafb';lc='#e5e7eb';valueCol='#9ca3af';";
  J += "    badgeBg='#9ca3af';badgeTxt='ODDS';sublabel='';";
  J += "  } else if(od.oddsTier==='strong'){";
  // strong: odds <= 1.80 — green, STRONG badge
  J += "    icon='&#10003;';icol='#15803d';rbg='#f0fdf4';lc='#16a34a';valueCol='#15803d';";
  J += "    badgeBg='#15803d';badgeTxt='STRONG';sublabel='<span style=\"font-size:10px;color:#15803d;margin-left:8px\">&le; 1.80 &#10003;</span>';";
  J += "  } else if(od.oddsTier==='medium'){";
  // medium: odds > 1.80 but <= 2.00 — amber, MED badge
  J += "    icon='&#10003;';icol='#ca8a04';rbg='#fffbeb';lc='#eab308';valueCol='#ca8a04';";
  J += "    badgeBg='#ca8a04';badgeTxt='MED';sublabel='<span style=\"font-size:10px;color:#ca8a04;margin-left:8px\">&le; 2.00 &#10003;</span>';";
  J += "  } else {";
  // miss: odds > 2.00 — red
  J += "    icon='&#10007;';icol='#dc2626';rbg='#fef2f2';lc='#dc2626';valueCol='#dc2626';";
  J += "    badgeBg='#9ca3af';badgeTxt='ODDS';sublabel='<span style=\"font-size:10px;color:#9ca3af;margin-left:8px\">&gt; 2.00 &#10007;</span>';";
  J += "  }";
  J += "  var badge='<span style=\"background:'+badgeBg+';color:#fff;font-size:9px;padding:1px 5px;border-radius:3px;margin-right:6px;font-weight:700\">'+badgeTxt+'</span>';";
  J += "  var noDataNote=od.noData?'<div style=\"font-size:10px;color:#9ca3af;margin-top:3px;font-style:italic\">No odds from API \u2014 check your sportsbook for the line</div>':'';";
  J += "  var thresholds=od.noData?'':' <span style=\"font-size:10px;color:#d1d5db\">(&le;1.80 strong &middot; &le;2.00 medium)</span>';";
  J += "  return '<div style=\"display:flex;align-items:flex-start;gap:10px;padding:10px 14px;background:'+rbg+';border-radius:7px;border-left:4px solid '+lc+';margin-bottom:6px;'+(od.noData?'opacity:0.55':'')+'\">'";
  J += "    +'<span style=\"font-size:16px;color:'+icol+';font-weight:700;min-width:20px;margin-top:1px\">'+icon+'</span>'";
  J += "    +'<div style=\"flex:1\">'";
  J += "    +'<div style=\"display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:4px\">'";
  J += "    +'<span style=\"font-weight:600;font-size:12px;color:#111827\">'+badge+esc(od.label)+sublabel+'</span>'";
  J += "    +'<span style=\"font-family:monospace;font-weight:700;font-size:13px;color:'+valueCol+'\">'+esc(od.value)+thresholds+'</span>'";
  J += "    +'</div>'";
  J += "    +noDataNote";
  J += "    +'</div></div>';";
  J += "}";

  // statBox helper
  J += "function statBox(label,val,hi){";
  J += "  return '<div style=\"background:'+(hi?'#fef2f2':'#f9fafb')+';border:1px solid '+(hi?'#fecaca':'#e5e7eb')+';border-radius:7px;padding:8px 10px;text-align:center\">'";
  J += "    +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:3px\">'+esc(label)+'</div>'";
  J += "    +'<div style=\"font-family:monospace;font-weight:700;font-size:16px;color:'+(hi?'#dc2626':'#111827')+'\">'+esc(val)+'</div>'";
  J += "    +'</div>';";
  J += "}";

  // last5Table helper
  J += "function last5Table(games,title){";
  J += "  if(!games||!games.length)return '<p style=\"font-size:12px;color:#9ca3af\">No games found.</p>';";
  J += "  var rows=games.map(function(g){";
  J += "    var tot=g.fhFor+g.fhAgst,fire=tot>2;";
  J += "    return '<tr style=\"background:'+(fire?'#fff7ed':'')+'\"><td>'+esc(g.date)+'</td><td>'+esc(g.venue)+'</td><td>'+esc(g.opp)+'</td>'";
  J += "      +'<td style=\"text-align:center;font-weight:700;color:'+(fire?'#ea580c':'#374151')+'\">'+g.fhFor+'-'+g.fhAgst+'</td>'";
  J += "      +'<td style=\"text-align:center;color:#9ca3af\">'+g.ftFor+'-'+g.ftAgst+'</td>'";
  J += "      +'<td style=\"text-align:center\">'+(fire?'&#128293;':'')+'</td></tr>';";
  J += "  }).join('');";
  J += "  return '<div style=\"margin-top:12px\"><div style=\"font-size:11px;font-weight:700;color:#374151;margin-bottom:6px\">'+esc(title)+'</div>'";
  J += "    +'<table class=\"mini-table\"><thead><tr><th>Date</th><th>H/A</th><th>Opp</th><th>FH</th><th>FT</th><th></th></tr></thead><tbody>'+rows+'</tbody></table></div>';";
  J += "}";

  // h2hTable helper
  J += "function h2hTable(matches){";
  J += "  if(!matches||!matches.length)return '<p style=\"font-size:12px;color:#9ca3af;margin-top:8px\">No H2H in dataset.</p>';";
  J += "  var rows=matches.map(function(g){";
  J += "    var tot=parseInt(g.ht_goals_team_a||0)+parseInt(g.ht_goals_team_b||0),fire=tot>2;";
  J += "    var date=g.date_unix?new Date(g.date_unix*1000).toISOString().slice(0,10):'';";
  J += "    return '<tr style=\"background:'+(fire?'#fff7ed':'')+'\"><td>'+esc(date)+'</td><td>'+esc(g.home_name)+'</td><td>'+esc(g.away_name)+'</td>'";
  J += "      +'<td style=\"text-align:center;font-weight:700;color:'+(fire?'#ea580c':'#374151')+'\">'+parseInt(g.ht_goals_team_a||0)+'-'+parseInt(g.ht_goals_team_b||0)+'</td>'";
  J += "      +'<td style=\"text-align:center;color:#9ca3af\">'+parseInt(g.homeGoalCount||0)+'-'+parseInt(g.awayGoalCount||0)+'</td>'";
  J += "      +'<td style=\"text-align:center\">'+(fire?'&#128293;':'')+'</td></tr>';";
  J += "  }).join('');";
  J += "  return '<div style=\"margin-top:12px\"><div style=\"font-size:11px;font-weight:700;color:#374151;margin-bottom:6px\">Head to Head</div>'";
  J += "    +'<table class=\"mini-table\"><thead><tr><th>Date</th><th>Home</th><th>Away</th><th>FH</th><th>FT</th><th></th></tr></thead><tbody>'+rows+'</tbody></table>'";
  J += "    +'<div style=\"font-size:10px;color:#9ca3af;margin-top:6px\">H2H is contextual only \u2014 not used in signal scoring</div></div>';";
  J += "}";

  // probRef helper
  J += "function probRefHTML(){";
  J += "  var rows=[{r:5,p:48,l:'All 3 strong signals met'},{r:4,p:42,l:'S1 + S2 or S3'},{r:3,p:35,l:'S1 + M1 + 1 more medium'},{r:2,p:25,l:'M1 + 1 more medium'},{r:1,p:13,l:'Base rate \u2014 no signals'}];";
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

  // renderMatchCard
  J += "function renderMatchCard(m){";
  J += "  var col=rankColor(m.rank),bg=rankBg(m.rank),br=rankBorder(m.rank),left=rankLeft(m.rank);";
  J += "  var dt=m.dt?new Date(m.dt).toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}):m.matchDate;";
  J += "  var sc=m.status==='complete'?{bg:'#f3f4f6',border:'#e5e7eb',color:'#6b7280',txt:'Final'}:m.status==='live'?{bg:'#fef9c3',border:'#fde047',color:'#ca8a04',txt:'&#9679; Live'}:{bg:'#eff6ff',border:'#bfdbfe',color:'#2563eb',txt:'Upcoming'};";
  J += "  var statusBadge='<span style=\"padding:2px 8px;border-radius:20px;font-size:10px;font-weight:700;background:'+sc.bg+';border:1px solid '+sc.border+';color:'+sc.color+'\">'+sc.txt+'</span>';";
  J += "  var frozen=m.snapshot?'<span style=\"padding:2px 8px;border-radius:20px;font-size:10px;font-weight:600;background:#fffbeb;border:1px solid #fde68a;color:#92400e;margin-left:6px\">&#128274; '+esc((m.snapshot||{}).fetchedAt||'')+'</span>':'';";
  J += "  var missWarn=m.missingStats?'<span style=\"background:#fef3c7;color:#92400e;font-size:11px;padding:2px 7px;border-radius:4px;font-weight:600;margin-left:6px\">&#9888; missing stats</span>':'';";
  J += "  var noOddsWarn=(!m.missingStats&&!m.oddsAvail)?'<span style=\"background:#f3f4f6;color:#6b7280;font-size:11px;padding:2px 7px;border-radius:4px;font-weight:600;margin-left:6px\">&#9711; No odds available \u2014 check your sportsbook</span>':'';";
  J += "  var noStatsMsg=m.missingStats?'<div style=\"color:#9ca3af;font-size:13px;padding:12px 0\">No team stats available.</div>':'';";

  // stats block
  J += "  var statsHTML='';";
  J += "  if(!m.missingStats&&m.snapshot){";
  J += "    var h=m.snapshot.home,a=m.snapshot.away;";
  J += "    statsHTML='<div style=\"display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:14px\">';";
  J += "    statsHTML+='<div><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px\">Home \u2014 '+esc(h.name)+'</div>'";
  J += "      +'<div style=\"display:grid;grid-template-columns:1fr 1fr;gap:6px\">'";
  J += "      +statBox('Avg Scored FH',h.scored_fh.toFixed(2),h.scored_fh>=0.8)";
  J += "      +statBox('Avg Conceded FH',h.conced_fh.toFixed(2),h.conced_fh>=0.8)";
  J += "      +statBox('BTTS FH %',h.btts_ht_pct+'%',h.btts_ht_pct>=25)";
  J += "      +statBox('FH Clean Sheet %',h.cs_ht_pct+'%',false)";
  J += "      +statBox('FH Over 2.5 %',h.o25ht_pct+'%',h.o25ht_pct>=20)";
  J += "      +statBox('Early Goals /gm',h.cn010_avg.toFixed(2),h.cn010_avg>=0.25)";
  J += "      +'</div></div>';";
  J += "    statsHTML+='<div><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px\">Away \u2014 '+esc(a.name)+'</div>'";
  J += "      +'<div style=\"display:grid;grid-template-columns:1fr 1fr;gap:6px\">'";
  J += "      +statBox('Avg Scored FH',a.scored_fh.toFixed(2),a.scored_fh>=0.8)";
  J += "      +statBox('Avg Conceded FH',a.conced_fh.toFixed(2),a.conced_fh>=0.8)";
  J += "      +statBox('BTTS FH %',a.btts_ht_pct+'%',a.btts_ht_pct>=25)";
  J += "      +statBox('FH Clean Sheet %',a.cs_ht_pct+'%',false)";
  J += "      +statBox('FH Over 2.5 %',a.o25ht_pct+'%',a.o25ht_pct>=20)";
  J += "      +statBox('Early Goals /gm',a.cn010_avg.toFixed(2),a.cn010_avg>=0.25)";
  J += "      +'</div></div>';";
  J += "    statsHTML+='</div>';";
  J += "    var oddsColor=m.oddsAvail&&m.snapshot.odds_fh_o15<=1.80?'#dc2626':'#374151';";
  J += "    var oddsDisplay=m.oddsAvail?m.snapshot.odds_fh_o15.toFixed(2):'no data';";
  J += "    var oddsColorFinal=m.oddsAvail?oddsColor:'#9ca3af';";
  J += "    statsHTML+='<div style=\"background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:10px 14px\">'";
  J += "      +'<div style=\"font-size:10px;color:#1d4ed8;font-weight:700;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px\">Expected FH Goals</div>'";
  J += "      +'<div style=\"font-family:monospace;font-size:12px;color:#374151;line-height:2\">'";
  J += "      +'('+h.scored_fh.toFixed(2)+' &times; '+a.conced_fh.toFixed(2)+')'";
  J += "      +' + ('+a.scored_fh.toFixed(2)+' &times; '+h.conced_fh.toFixed(2)+')'";
  J += "      +' = <strong style=\"font-size:16px;color:'+(m.expFH>=1.2?'#dc2626':'#374151')+'\">'+m.expFH+'</strong>'";
  J += "      +' <span style=\"font-size:11px;color:'+(m.expFH>=1.2?'#15803d':'#9ca3af')+'\">'+( m.expFH>=1.2?'&#10003; &ge; 1.20':'&#10007; &lt; 1.20')+'</span>'";
  J += "      +'</div>'";
  J += "      +'<div style=\"margin-top:8px;display:flex;gap:16px\">'";
  J += "      +'<div><span style=\"font-size:10px;color:#9ca3af\">FH o1.5 Odds</span> <span style=\"font-family:monospace;font-weight:700;color:'+oddsColorFinal+'\">'+oddsDisplay+'</span></div>'";
  J += "      +(m.snapshot.odds_ft_o25?'<div><span style=\"font-size:10px;color:#9ca3af\">FT o2.5 Odds</span> <span style=\"font-family:monospace;font-weight:700;color:#374151\">'+m.snapshot.odds_ft_o25.toFixed(2)+'</span></div>':'')";
  J += "      +'</div></div>';";
  J += "  }";

  // result block
  J += "  var resultHTML='';";
  J += "  if(m.result){";
  J += "    var rb=m.result.hit?'#f0fdf4':'#fef2f2',rbr=m.result.hit?'#bbf7d0':'#fecaca',rfc=m.result.hit?'#15803d':'#dc2626';";
  J += "    resultHTML='<div style=\"display:flex;align-items:stretch;gap:0;border:1px solid '+rbr+';border-radius:10px;overflow:hidden;margin-top:14px\">'";
  J += "      +'<div style=\"padding:10px 16px;background:'+rb+';text-align:center;border-right:1px solid '+rbr+'\">'";
  J += "      +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:2px\">1st Half</div>'";
  J += "      +'<div style=\"font-family:monospace;font-weight:800;font-size:22px;color:'+rfc+';line-height:1\">'+m.result.fhH+'&ndash;'+m.result.fhA+'</div>'";
  J += "      +'<div style=\"font-size:10px;font-weight:700;color:'+rfc+';margin-top:3px\">'+(m.result.hit?'&#10003; HIT':'&#10007; MISS')+'</div>'";
  J += "      +'</div>'";
  J += "      +'<div style=\"padding:10px 14px;background:#f9fafb;text-align:center\">'";
  J += "      +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:2px\">Full Time</div>'";
  J += "      +'<div style=\"font-family:monospace;font-weight:700;font-size:16px;color:#374151;line-height:1\">'+m.result.ftH+'&ndash;'+m.result.ftA+'</div>'";
  J += "      +'<div style=\"font-size:9px;color:#9ca3af;margin-top:3px\">'+m.prob+'% pre-game</div>'";
  J += "      +'</div></div>';";
  J += "  }";

  // assemble card
  J += "  return '<div class=\"match-card\" style=\"border-left:4px solid '+left+'\">'";
  J += "    +'<div style=\"padding:18px 20px\">'";
  J += "    +'<div style=\"display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:14px\">'";
  J += "    +'<div>'";
  J += "    +'<div style=\"font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:4px\">'+esc(m.league)+' &middot; '+esc(dt)+missWarn+noOddsWarn+'</div>'";
  J += "    +'<div style=\"display:flex;align-items:center;gap:6px;flex-wrap:wrap\">'+statusBadge+frozen+'</div>'";
  J += "    +'</div>'";
  J += "    +'<div style=\"text-align:center;min-width:90px;background:'+bg+';border:1px solid '+br+';border-radius:10px;padding:10px 8px;flex-shrink:0\">'";
  J += "    +'<div style=\"font-size:32px;font-weight:900;color:'+col+';line-height:1\">'+m.rank+'/5</div>'";
  J += "    +'<div style=\"font-size:11px;color:'+col+';font-weight:700;margin-top:3px\">'+esc(m.label)+'</div>'";
  J += "    +'<div style=\"font-size:10px;color:#9ca3af;margin-top:4px\">'+m.prob+'% FH&gt;2.5</div>'";
  J += "    +'</div></div>'";
  J += "    +'<div style=\"font-size:22px;font-weight:800;color:#111827;margin-bottom:8px\">'+esc(m.home)+' <span style=\"color:#d1d5db;font-weight:400;font-size:14px\">vs</span> '+esc(m.away)+'</div>'";
  J += "    +'<div style=\"display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap\">'";
  J += "    +'<div style=\"padding:3px 10px;border-radius:20px;background:'+(m.strongMet>0?'#f0fdf4':'#f3f4f6')+';border:1px solid '+(m.strongMet>0?'#86efac':'#e5e7eb')+'\">'";
  J += "    +'<span style=\"font-family:monospace;font-weight:700;font-size:11px;color:'+(m.strongMet>0?'#15803d':'#9ca3af')+'\">'+m.strongMet+'/3</span>'";
  J += "    +'<span style=\"font-size:10px;color:#9ca3af;margin-left:4px\">strong</span></div>'";
  J += "    +'<div style=\"padding:3px 10px;border-radius:20px;background:'+(m.medMet>0?'#fffbeb':'#f3f4f6')+';border:1px solid '+(m.medMet>0?'#fde68a':'#e5e7eb')+'\">'";
  J += "    +'<span style=\"font-family:monospace;font-weight:700;font-size:11px;color:'+(m.medMet>0?'#ca8a04':'#9ca3af')+'\">'+m.medMet+'/4</span>'";
  J += "    +'<span style=\"font-size:10px;color:#9ca3af;margin-left:4px\">medium</span></div>'";
  J += "    +'<div style=\"padding:3px 10px;border-radius:20px;background:#eff6ff;border:1px solid #bfdbfe\">'";
  J += "    +'<span style=\"font-family:monospace;font-weight:700;font-size:11px;color:#2563eb\">'+m.expFH+'</span>'";
  J += "    +'<span style=\"font-size:10px;color:#9ca3af;margin-left:4px\">exp FH goals</span></div>'";
  J += "    +'</div>'";
  J += "    +resultHTML";
  J += "    +'<details style=\"margin-top:14px\">'";
  J += "    +'<summary style=\"font-size:13px;color:#6b7280;cursor:pointer;padding-top:10px;border-top:1px solid #f3f4f6\">&#9660; Show full detail</summary>'";
  J += "    +'<div style=\"padding-top:14px\">'";
  J += "    +'<div style=\"font-size:10px;font-weight:700;color:#15803d;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:8px\">&#9679; Strong Signals</div>'";
  J += "    +noStatsMsg";
  J += "    +(m.missingStats?'':sigRow(m.signals.S1)+oddsRow(m.signals.OD)+sigRow(m.signals.S3))";
  J += "    +(m.missingStats?'':'<div style=\"font-size:10px;font-weight:700;color:#ca8a04;letter-spacing:1.5px;text-transform:uppercase;margin:14px 0 8px\">&#9670; Medium Signals</div>')";
  J += "    +(m.missingStats?'':sigRow(m.signals.M2)+sigRow(m.signals.M3)+sigRow(m.signals.M4))";
  J += "    +'<div style=\"background:#f9fafb;border:1px solid #e5e7eb;border-radius:9px;padding:14px 16px;margin-top:14px\">'";
  J += "    +'<div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px;margin-bottom:10px;font-weight:600\">Probability Reference &middot; 5,699 matches</div>'";
  J += "    +probRefHTML()";
  J += "    +'</div>'";
  J += "    +'<div style=\"font-size:11px;font-weight:700;color:#374151;margin-top:16px;margin-bottom:10px;text-transform:uppercase;letter-spacing:.8px\">Team Stats</div>'";
  J += "    +statsHTML";
  J += "    +last5Table(m.hLast5,'Home \u2014 last 5')";
  J += "    +last5Table(m.aLast5,'Away \u2014 last 5')";
  J += "    +h2hTable(m.h2h)";
  J += "    +'</div></details></div></div>';";
  J += "}";

  // init
  J += "if(DATES.length){document.getElementById('headerTitle').textContent=fmtDate(new Date(DATES[0]+'T12:00:00'));}";
  J += "renderTabs();renderLeagueList();";

  // ── HTML shell ──
  return "<!DOCTYPE html><html><head>"
    + "<meta charset=\"UTF-8\"/>"
    + "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/>"
    + "<title>FH Over 2.5 Predictor</title>"
    + "<script>(function(){var p=new URLSearchParams(window.location.search);if(!p.has('tz')){p.set('tz',-new Date().getTimezoneOffset());window.location.search=p.toString();}})();<\/script>"
    + "<style>"
    + "*{box-sizing:border-box;margin:0;padding:0}"
    + "body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;font-size:15px}"
    + "details>summary::-webkit-details-marker{display:none}"
    + ".tab{padding:8px 14px;border-radius:6px;cursor:pointer;font-size:14px;font-weight:600;border:1px solid #e5e7eb;background:#fff;color:#6b7280;transition:all .15s}"
    + ".tab.active{background:#111827;color:#fff;border-color:#111827}"
    + ".league-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:16px 18px;margin-bottom:10px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;box-shadow:0 1px 3px rgba(0,0,0,.05);transition:box-shadow .15s}"
    + ".league-card:hover{box-shadow:0 3px 8px rgba(0,0,0,.1)}"
    + ".back-btn{background:#f3f4f6;border:1px solid #e5e7eb;padding:7px 16px;border-radius:6px;cursor:pointer;font-size:15px;font-weight:600;color:#374151}"
    + ".match-card{background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:14px}"
    + ".mini-table{width:100%;border-collapse:collapse;font-size:12px}"
    + ".mini-table th{background:#f9fafb;padding:6px 8px;text-align:left;font-size:10px;color:#6b7280;font-weight:700;text-transform:uppercase;border-bottom:1px solid #e5e7eb}"
    + ".mini-table td{padding:6px 8px;border-bottom:1px solid #f3f4f6}"
    + "details summary{cursor:pointer;user-select:none;list-style:none}"
    + "</style></head><body>"
    + "<div style=\"background:#fff;border-bottom:1px solid #e5e7eb;padding:14px 20px;position:sticky;top:0;z-index:10\">"
    + "<div style=\"max-width:860px;margin:0 auto\">"
    + "<div style=\"display:flex;align-items:center;justify-content:space-between;margin-bottom:12px\">"
    + "<div><div style=\"font-size:11px;color:#6b7280;letter-spacing:1px;text-transform:uppercase\">&#9917; First Half Over 2.5</div>"
    + "<h1 style=\"font-size:22px;font-weight:800;color:#111827\" id=\"headerTitle\">Loading...</h1></div>"
    + "<button onclick=\"location.reload()\" style=\"background:#111827;color:#fff;padding:8px 16px;font-size:14px;border:none;border-radius:6px;font-weight:600;cursor:pointer\">&#8635; Refresh</button>"
    + "</div><div id=\"dayTabs\" style=\"display:flex;gap:8px;flex-wrap:wrap\"></div></div></div>"
    + "<div style=\"padding:16px 20px;max-width:860px;margin:0 auto\">"
    + "<div style=\"background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#92400e;line-height:1.6\">"
    + "<strong>How it works:</strong> 3 strong + 4 medium signals, backtested on 5,699 matches (base rate 13.2%). "
    + "Rank&nbsp;5&nbsp;=&nbsp;48% &middot; Rank&nbsp;4&nbsp;=&nbsp;42% &middot; Rank&nbsp;3&nbsp;=&nbsp;35% &middot; Rank&nbsp;2&nbsp;=&nbsp;25% &middot; Rank&nbsp;1&nbsp;=&nbsp;13%. "
    + "All signals frozen at fetch time.</div>"
    + "<div id=\"mainView\"></div></div>"
    + "<script>" + J + "<\/script>"
    + "</body></html>";
}

const PORT = process.env.PORT || 3001;

// Fetch league list first, then start listening
fetchLeagueList().then(() => {
  app.listen(PORT, () => {
    console.log("Server running on port " + PORT);
    console.log("Memory at start: " + Math.round(process.memoryUsage().heapUsed/1024/1024) + "MB");
  });
});

process.on("uncaughtException", e => console.error("Uncaught:", e.message));
