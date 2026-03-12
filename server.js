const express=require("express"),cors=require("cors"),fetch=require("node-fetch"),app=express();
app.use(cors());
const KEY="437fa5361a693ad65c0c97d75f55042da3529532df53b57d34fe28f89789c0e7",BASE="https://api.football-data-api.com";

const LEAGUE_NAMES={
  16504:"USA MLS",15000:"Scotland Premiership",14968:"Germany Bundesliga",
  14924:"UEFA Champions League",15050:"England Premier League",14930:"England Championship",
  14956:"Spain La Liga",16558:"Norway Eliteserien",14932:"France Ligue 1",
  15068:"Italy Serie A",14931:"Germany 2. Bundesliga",14923:"Austria Bundesliga",
  16036:"Australia A-League",16544:"Brazil Serie A",16571:"Argentina Primera Division",
  15047:"Switzerland Super League",16242:"Japan J1 League",15234:"Mexico Liga MX",
  16614:"Colombia Primera A",16615:"Chile Primera Division",15055:"Denmark Superliga",
  16714:"Ecuador Serie A",16708:"Uruguay Primera Division",15002:"UEFA Europa League",
  15238:"England FA Cup",10117:"WC Qual Asia",12061:"WC Qual Africa",
  11084:"UEFA Euro Championship",9128:"UEFA Euro Qualifiers",16808:"UEFA Nations League",
  10121:"WC Qual South America",1425:"FIFA World Cup 2018",15020:"Mexico Liga MX Femenil",
  8994:"Asia Womens Olympic",16823:"CONCACAF Champions League",16046:"UEFA Womens CL",
  11426:"WC Qual CONCACAF",6704:"Womens WC Qual Oceania",14904:"UEFA Conference League",
  12980:"CONCACAF Nations League",16494:"FIFA World Cup",7977:"CONCACAF League",
  12801:"WC Qual Oceania",16562:"CONCACAF Gold Cup Qual",16037:"Australia A-League Women",
  13861:"UEFA Womens Nations League",16563:"Womens WC Qual Europe"
};

// Previous season IDs for H2H fallback when current season has limited data
const PREV_SEASON={
  16504:13973,16544:11321,16571:15746,16614:14086,16615:14116,
  16036:13703,15050:13792,14968:13880,14956:13885,15068:13900,
  14932:13895,14924:12325,15002:15003,14904:15004,14930:13793,
  14931:13881,15000:13882,14923:13886,15047:13905,16242:13920,
  15234:13935,15055:13895,16558:13953,16808:16809,16823:16824
};

// Probability table indexed by number of signals met (0-4)
const PROB_TABLE=[10,20,25,35,45];

// ── Cache (20-min TTL matching FootyStats' own update frequency) ─────────────
// Reduces ~25-30 API calls per page load down to near-zero after first hit
const CACHE_TTL=20*60*1000;
const _cache=new Map();
function cacheGet(key){
  const e=_cache.get(key);
  if(!e) return null;
  if(Date.now()-e.ts>CACHE_TTL){_cache.delete(key);return null;}
  return e.val;
}
function cacheSet(key,val){_cache.set(key,{val,ts:Date.now()});return val;}

const _rawFetch=url=>fetch(url).then(r=>r.json());
const ftch=async url=>{
  const hit=cacheGet(url);
  if(hit){console.log("CACHE: "+url.replace(/key=[^&]+/,"key=***"));return hit;}
  const data=await _rawFetch(url);
  cacheSet(url,data);
  console.log("API:   "+url.replace(/key=[^&]+/,"key=***"));
  return data;
};

const safe=v=>(isNaN(v)||!isFinite(v)||v==null)?0:+v;

const getDates=(tzOffset=0)=>{
  const now=new Date();
  const local=new Date(now.getTime()+tzOffset*60*1000);
  const fmt=d=>{
    const y=d.getUTCFullYear();
    const m=String(d.getUTCMonth()+1).padStart(2,"0");
    const day=String(d.getUTCDate()).padStart(2,"0");
    return y+"-"+m+"-"+day;
  };
  const dates=[];
  for(let i=0;i<6;i++){
    const d=new Date(local);
    d.setUTCDate(local.getUTCDate()+i);
    dates.push(fmt(d));
  }
  return[...new Set(dates)];
};

// Debug endpoint: shows raw team data for a league + today's fixture IDs
// Usage: /debug-league?sid=14924
app.get("/debug-league",async(req,res)=>{
  const sid=req.query.sid||"14924";
  const date=req.query.date||new Date().toISOString().slice(0,10);
  try{
    const [fixtureRes,teamRes]=await Promise.all([
      fetch(BASE+"/todays-matches?date="+date+"&key="+KEY).then(r=>r.json()),
      fetch(BASE+"/league-teams?season_id="+sid+"&include=stats&key="+KEY).then(r=>r.json())
    ]);
    const fixtures=(fixtureRes.data||[]).filter(m=>String(m.competition_id)===String(sid));
    const teams=(teamRes.data||[]).slice(0,2); // first 2 teams as sample
    // Show all odds-related keys from a sample fixture
    const sampleFixture=fixtures[0]||{};
    const oddsKeys=Object.keys(sampleFixture).filter(k=>k.toLowerCase().includes('odd'));
    const oddsValues={};
    oddsKeys.forEach(k=>oddsValues[k]=sampleFixture[k]);

    const fixtureIds=fixtures.map(m=>({
      home:m.home_name,away:m.away_name,
      homeID:m.homeID,home_id:m.home_id,
      awayID:m.awayID,away_id:m.away_id,
      competition_id:m.competition_id
    }));
    const sampleTeam=teams[0]||{};
    const topLevelHTKeys=Object.keys(sampleTeam).filter(k=>k.toLowerCase().includes('ht')||k.toLowerCase().includes('half'));
    const statsKeys=sampleTeam.stats?Object.keys(sampleTeam.stats).filter(k=>k.toLowerCase().includes('ht')||k.toLowerCase().includes('half')):[];
    res.json({
      sid,date,
      fixtureCount:fixtures.length,
      fixtureIds,
      teamCount:(teamRes.data||[]).length,
      sampleTeamId:sampleTeam.id,
      sampleTeamName:sampleTeam.name,
      topLevelHTKeys,
      statsSubObjectHTKeys:statsKeys,
      sampleTeamAllKeys:Object.keys(sampleTeam).slice(0,30),
      sampleTeamStatsKeys:sampleTeam.stats?Object.keys(sampleTeam.stats).slice(0,30):[],
      sampleFixtureOddsKeys:oddsKeys,
      sampleFixtureOddsValues:oddsValues,
      sampleFixtureAllKeys:Object.keys(sampleFixture),
      sampleFixtureOddsObject:sampleFixture.odds||sampleFixture.unified_odds||null,
      // Show full fixture for first match to see every field
      sampleFixtureFull:sampleFixture
    });
  }catch(e){res.status(500).json({error:e.message});}
});

// Cache status / manual flush endpoint
app.get("/cache-status",(req,res)=>{
  const entries=[..._cache.entries()].map(([url,e])=>({
    url:url.replace(/key=[^&]+/,"key=***"),
    age:Math.round((Date.now()-e.ts)/1000)+"s",
    expiresIn:Math.max(0,Math.round((CACHE_TTL-(Date.now()-e.ts))/1000))+"s"
  }));
  res.json({count:entries.length,ttl:CACHE_TTL/1000+"s",entries});
});
app.get("/cache-flush",(req,res)=>{
  const n=_cache.size;
  _cache.clear();
  res.json({flushed:n});
});

// Pass-through proxy for raw API access (bypasses cache intentionally)
app.get("/api/*",async(req,res)=>{
  try{
    const path=req.path.replace("/api","");
    const qs=new URLSearchParams({...req.query,key:KEY}).toString();
    const data=await ftch(BASE+path+"?"+qs);
    res.json(data);
  }catch(e){res.status(500).json({error:e.message})}
});

// ── Signal helpers ──────────────────────────────────────────────────────────

// CI: combined HT intensity index
// = scoredAVGHT_home + scoredAVGHT_away + concededAVGHT_home + concededAVGHT_away >= 3.2
function calcCI(ht, at){
  // ht = home team stats obj, at = away team stats obj
  const val = safe(ht.scoredAVGHT_home)
             + safe(at.scoredAVGHT_away)
             + safe(ht.concededAVGHT_home)
             + safe(at.concededAVGHT_away);
  return { val: +val.toFixed(3), met: val >= 3.2 };
}

// T1: both teams seasonOver25PercentageHT_overall >= 20
function calcT1(ht, at){
  const hv = safe(ht.seasonOver25PercentageHT_overall);
  const av = safe(at.seasonOver25PercentageHT_overall);
  return { hVal: hv, aVal: av, met: hv >= 20 && av >= 20 };
}

// O1: both teams seasonOver15PercentageHT_overall >= 35%
// Replaces odds-based signal (odds not available in this API plan)
// Uses confirmed field from stats sub-object
function calcO1(ht, at){
  const hv = safe(ht.seasonOver15PercentageHT_overall);
  const av = safe(at.seasonOver15PercentageHT_overall);
  return { hVal: hv, aVal: av, met: hv >= 35 && av >= 35 };
}

// AV: combined avg total FH goals (AVGHT_home + AVGHT_away) >= 1.6
// Uses confirmed AVGHT_overall field from stats sub-object
function calcAV(ht, at){
  const hv = safe(ht.AVGHT_home || ht.AVGHT_overall);
  const av = safe(at.AVGHT_away || at.AVGHT_overall);
  const combined = hv + av;
  return { hVal: +hv.toFixed(2), aVal: +av.toFixed(2), combined: +combined.toFixed(3), met: combined >= 1.6 };
}

// ── Main route ───────────────────────────────────────────────────────────────

app.get("/",async(req,res)=>{
  try{
    const tzOffset=parseInt(req.query.tz||"0");
    const dates=getDates(tzOffset);

    // 1. Fetch fixtures for the date window
    const dayResults=await Promise.all(
      dates.map(d=>ftch(BASE+"/todays-matches?date="+d+"&key="+KEY))
    );
    const allFixtures=[];
    for(let i=0;i<dates.length;i++){
      for(const m of (dayResults[i].data||[])){
        allFixtures.push(Object.assign({},m,{_date:dates[i]}));
      }
    }

    // 2. Group fixtures by season_id — accept ALL competitions
    const leagueFixtures={};
    const leagueNameOverride={};
    for(const m of allFixtures){
      const sid=parseInt(m.competition_id);
      if(!sid) continue;
      if(!leagueFixtures[sid]) leagueFixtures[sid]=[];
      leagueFixtures[sid].push(m);
      if(!leagueNameOverride[sid]){
        leagueNameOverride[sid]=LEAGUE_NAMES[sid]||m.league||m.competition||m.competition_name||('League '+sid);
      }
    }

    // 3. Fetch all league-team stats in parallel, then process fixtures
    const preds=[];
    const allSids=Object.keys(leagueFixtures);
    const teamMapsArr=await Promise.all(allSids.map(async sid=>{
      try{
        const r=await ftch(BASE+"/league-teams?season_id="+sid+"&include=stats&key="+KEY);
        const m={};
        for(const t of (r.data||[])){
          if(t.id!=null){ m[t.id]=t; m[String(t.id)]=t; }
          m["__name__"+(t.name||"").toLowerCase().trim()]=t;
          if(t.clean_name) m["__name__"+t.clean_name.toLowerCase().trim()]=t;
        }
        return m;
      }catch(e){ console.log("league-teams error sid="+sid,e.message); return {}; }
    }));
    const teamMapBySid={};
    allSids.forEach((sid,i)=>teamMapBySid[sid]=teamMapsArr[i]);

    for(const sid of allSids){
      const teamMap=teamMapBySid[sid];
      if(!Object.keys(teamMap).length){
        console.log("No team data for sid="+sid);
        continue;
      }

      const fixtures=leagueFixtures[sid]||[];
      for(const fixture of fixtures){
        // Resolve home/away team stats
        // Normalise IDs to string to avoid int/string mismatch
        const homeId=String(fixture.homeID||fixture.home_id||"");
        const awayId=String(fixture.awayID||fixture.away_id||"");
        let htRaw=teamMap[homeId]||teamMap[parseInt(homeId)]||teamMap["__name__"+(fixture.home_name||"").toLowerCase().trim()];
        let atRaw=teamMap[awayId]||teamMap[parseInt(awayId)]||teamMap["__name__"+(fixture.away_name||"").toLowerCase().trim()];

        // Unwrap .stats sub-object if the API nests fields there
        const unwrap=t=>{
          if(!t) return null;
          if(t.stats && typeof t.stats==='object') return Object.assign({},t,t.stats);
          return t;
        };
        let ht=unwrap(htRaw);
        let at=unwrap(atRaw);

        if(!ht||!at){
          console.log("Missing team stats: "+fixture.home_name+"(id="+homeId+") vs "+fixture.away_name+"(id="+awayId+") sid="+sid);
          ht=ht||{};
          at=at||{};
        }

        // ── Compute signals ──
        const ci  = calcCI(ht, at);
        const t1  = calcT1(ht, at);
        const o1  = calcO1(ht, at);
        const av  = calcAV(ht, at);

        // Count met signals (O1 only counts if odds available)
        const signals=[
          {
            key:"CI",
            label:"HT Intensity Index (CI)",
            desc:"H scored(H) + A scored(A) + H conceded(H) + A conceded(A) ≥ 3.2 — 2.76x lift",
            hVal: safe(ht.scoredAVGHT_home).toFixed(2)+" sc / "+safe(ht.concededAVGHT_home).toFixed(2)+" cn",
            aVal: safe(at.scoredAVGHT_away).toFixed(2)+" sc / "+safe(at.concededAVGHT_away).toFixed(2)+" cn",
            combinedVal: ci.val.toFixed(3),
            threshold:"≥ 3.2",
            met: ci.met,
            lift:"2.76x lift"
          },
          {
            key:"T1",
            label:"Both Teams FH Over 25%",
            desc:"seasonOver25PercentageHT_overall ≥ 20% for both — 2.65x lift",
            hVal: safe(ht.seasonOver25PercentageHT_overall).toFixed(1)+"%",
            aVal: safe(at.seasonOver25PercentageHT_overall).toFixed(1)+"%",
            combinedVal: Math.min(safe(ht.seasonOver25PercentageHT_overall),safe(at.seasonOver25PercentageHT_overall)).toFixed(1)+"% (lower)",
            threshold:"both ≥ 20%",
            met: t1.met,
            lift:"2.65x lift"
          },
          {
            key:"O1",
            label:"Both Teams FH Over 1.5 Rate",
            desc:"seasonOver15PercentageHT_overall ≥ 35% for both teams — 2.08x lift",
            hVal: safe(ht.seasonOver15PercentageHT_overall).toFixed(1)+"%",
            aVal: safe(at.seasonOver15PercentageHT_overall).toFixed(1)+"%",
            combinedVal: Math.min(safe(ht.seasonOver15PercentageHT_overall),safe(at.seasonOver15PercentageHT_overall)).toFixed(1)+"% (lower)",
            threshold:"both ≥ 35%",
            met: o1.met,
            lift:"2.08x lift"
          },
          {
            key:"AV",
            label:"Combined FH Goals Avg",
            desc:"H avg FH goals (home) + A avg FH goals (away) ≥ 1.6",
            hVal: av.hVal.toFixed(2)+" avg",
            aVal: av.aVal.toFixed(2)+" avg",
            combinedVal: av.combined.toFixed(3),
            threshold:"sum ≥ 1.6",
            met: av.met,
            lift:"volume signal"
          }
        ];

        // For prob table: O1 if unavailable contributes 0 (neither met nor penalised)
        const nMet=signals.filter(s=>s.met).length;
        const prob=PROB_TABLE[Math.min(nMet,4)];

        // Form: formRun_ht_overall is a string like "WWDLL" (W=FH win, D=draw, L=loss)
        // last 5 chars = last 5 matches
        const hForm = String(ht.formRun_ht_overall||ht.formRun_overall||"").slice(-5);
        const aForm = String(at.formRun_ht_overall||at.formRun_overall||"").slice(-5);

        // FH averages (role-specific where possible)
        const hFHSc = safe(ht.scoredAVGHT_home||ht.scoredAVGHT_overall);
        const hFHCn = safe(ht.concededAVGHT_home||ht.concededAVGHT_overall);
        const aFHSc = safe(at.scoredAVGHT_away||at.scoredAVGHT_overall);
        const aFHCn = safe(at.concededAVGHT_away||at.concededAVGHT_overall);

        // Recent FH results from league match history (last 5 per team)
        // Build from completed matches already in teamMap context
        const hRecent = (ht.recent_ht||[]).slice(0,5);
        const aRecent = (at.recent_ht||[]).slice(0,5);

        preds.push({
          league:leagueNameOverride[sid],
          leagueSid:parseInt(sid),
          dt:(fixture.date_unix||0)*1000,
          matchDate:fixture._date,
          home:fixture.home_name,
          away:fixture.away_name,
          prob,nMet,signals,
          missingStats:!htRaw||!atRaw,
          status:fixture.status||"incomplete",
          fhH:parseInt(fixture.ht_goals_team_a||0),
          fhA:parseInt(fixture.ht_goals_team_b||0),
          ftH:parseInt(fixture.homeGoalCount||0),
          ftA:parseInt(fixture.awayGoalCount||0),
          // Form & stats
          hForm, aForm,
          hFHSc:+hFHSc.toFixed(2), hFHCn:+hFHCn.toFixed(2),
          aFHSc:+aFHSc.toFixed(2), aFHCn:+aFHCn.toFixed(2),
          // H2H will be populated after all fixtures processed (same league matches)
          h2h:[]
        });
      }
    }

    // Populate H2H — only for TODAY's leagues to limit API calls.
    // Use cache aggressively; cap at 12 unique league fetches per request.
    const leagueMatches={};
    const fetchLeagueMatches=async(sid)=>{
      if(leagueMatches[sid]) return leagueMatches[sid];
      try{
        const r=await ftch(BASE+"/league-matches?season_id="+sid+"&max_per_page=300&page=1&key="+KEY);
        leagueMatches[sid]=(r.data||[]).filter(m=>m.status==="complete");
      }catch(e){ leagueMatches[sid]=[]; }
      return leagueMatches[sid];
    };

    // Only fetch H2H for TODAY's matches (not all 6 days) to limit call count
    const todayDate=dates[0];
    const todayPreds=preds.filter(p=>p.matchDate===todayDate);
    const uniqueSidsToday=[...new Set(todayPreds.map(p=>p.leagueSid))].slice(0,12);

    // Pre-fetch all needed league-matches in parallel (all cached after first hit)
    await Promise.all(uniqueSidsToday.map(sid=>fetchLeagueMatches(sid)));
    // Also pre-fetch prev seasons for leagues that need it
    await Promise.all(uniqueSidsToday
      .filter(sid=>PREV_SEASON[sid])
      .map(sid=>fetchLeagueMatches(PREV_SEASON[sid]))
    );

    for(const p of preds){
      const sid=p.leagueSid;
      // Only populate H2H if we fetched this league's matches
      if(!uniqueSidsToday.includes(sid)){ p.h2h=[]; continue; }
      const all=leagueMatches[sid]||[];
      const matchH2H=arr=>arr.filter(m=>
        (m.home_name===p.home&&m.away_name===p.away)||
        (m.home_name===p.away&&m.away_name===p.home)
      );
      let h2hMatches=matchH2H(all);
      if(h2hMatches.length<2&&PREV_SEASON[sid]){
        const prevAll=leagueMatches[PREV_SEASON[sid]]||[];
        const prevH2H=matchH2H(prevAll);
        if(prevH2H.length>h2hMatches.length) h2hMatches=prevH2H;
      }
      p.h2h=h2hMatches.slice(-5).map(m=>({
        home:m.home_name, away:m.away_name,
        fhH:parseInt(m.ht_goals_team_a||0), fhA:parseInt(m.ht_goals_team_b||0),
        ftH:parseInt(m.homeGoalCount||0),   ftA:parseInt(m.awayGoalCount||0)
      }));
    }

    preds.sort((a,b)=>b.prob-a.prob||b.nMet-a.nMet);
    res.send(buildHTML(preds,dates));
  }catch(e){
    console.error(e);
    res.status(500).send("<pre>Error: "+e.message+"\n"+e.stack+"</pre>");
  }
});


// ── HTML builder ─────────────────────────────────────────────────────────────

function buildHTML(preds,dates){
  const predsJSON=JSON.stringify(preds).replace(/</g,"\\u003c");
  const datesJSON=JSON.stringify(dates);

  const css=`
    *{box-sizing:border-box;margin:0;padding:0}
    html,body{height:100%;overflow:hidden}
    body{background:#0d0f14;color:#e2e8f0;font-family:'Barlow',sans-serif;font-size:14px;display:flex;flex-direction:column}
    ::-webkit-scrollbar{width:5px;height:5px}
    ::-webkit-scrollbar-track{background:#161920}
    ::-webkit-scrollbar-thumb{background:#2d3448;border-radius:3px}

    /* ── Top bar ── */
    #topbar{background:#161920;border-bottom:1px solid #1e2333;padding:0 20px;display:flex;align-items:center;gap:0;flex-shrink:0;height:52px}
    #topbar .logo{font-family:'Barlow Condensed',sans-serif;font-size:20px;font-weight:800;letter-spacing:1px;color:#fff;text-transform:uppercase;margin-right:24px;white-space:nowrap}
    #topbar .logo span{color:#f59e0b}
    #daytabs{display:flex;gap:2px;align-items:center;flex:1;overflow-x:auto}
    #daytabs::-webkit-scrollbar{height:0}
    .dtab{padding:6px 14px;border-radius:4px;cursor:pointer;font-family:'Barlow Condensed',sans-serif;font-size:13px;font-weight:700;letter-spacing:.5px;text-transform:uppercase;color:#64748b;background:transparent;border:none;white-space:nowrap;transition:all .15s}
    .dtab:hover{color:#94a3b8;background:#1e2333}
    .dtab.active{color:#f59e0b;background:#1e2333;border-bottom:2px solid #f59e0b}
    .dtab .cnt{font-size:11px;opacity:.7;margin-left:4px}
    #refreshbtn{margin-left:auto;background:#f59e0b;color:#0d0f14;border:none;padding:6px 14px;border-radius:4px;font-family:'Barlow Condensed',sans-serif;font-size:13px;font-weight:800;letter-spacing:.5px;text-transform:uppercase;cursor:pointer;flex-shrink:0;transition:opacity .15s}
    #refreshbtn:hover{opacity:.85}

    /* ── Layout ── */
    #layout{display:flex;flex:1;overflow:hidden}

    /* ── Left panel: league list ── */
    #sidebar{width:220px;flex-shrink:0;background:#111318;border-right:1px solid #1e2333;overflow-y:auto;padding:8px 0}
    .sidebar-section{padding:6px 14px 2px;font-family:'Barlow Condensed',sans-serif;font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#334155}
    .league-item{display:flex;align-items:center;justify-content:space-between;padding:7px 14px;cursor:pointer;border-left:3px solid transparent;transition:all .12s;gap:6px}
    .league-item:hover{background:#161920;color:#fff}
    .league-item.active{background:#1a1f2e;border-left-color:#f59e0b;color:#fff}
    .league-item .lname{font-size:13px;font-weight:600;color:inherit;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1;color:#94a3b8}
    .league-item.active .lname{color:#fff}
    .league-item:hover .lname{color:#fff}
    .league-item .lbadge{display:flex;align-items:center;gap:4px;flex-shrink:0}
    .league-item .lcount{font-family:'DM Mono',monospace;font-size:11px;color:#475569}
    .league-item .lprob{font-family:'Barlow Condensed',sans-serif;font-size:12px;font-weight:700;padding:1px 5px;border-radius:3px}
    .prob-high{background:#064e2b;color:#4ade80}
    .prob-med{background:#431407;color:#fb923c}
    .prob-low{background:#1e2333;color:#475569}

    /* ── Right panel: match list ── */
    #main{flex:1;overflow-y:auto;padding:16px 20px}
    #main-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}
    #main-header h2{font-family:'Barlow Condensed',sans-serif;font-size:22px;font-weight:800;text-transform:uppercase;letter-spacing:.5px;color:#fff}
    #main-header .sub{font-size:12px;color:#475569}
    #no-league{display:flex;flex-direction:column;align-items:center;justify-content:center;height:60%;color:#334155;gap:8px}
    #no-league .icon{font-size:48px}
    #no-league p{font-family:'Barlow Condensed',sans-serif;font-size:16px;letter-spacing:1px;text-transform:uppercase}

    /* ── Match card ── */
    .match-card{background:#161920;border:1px solid #1e2333;border-radius:8px;margin-bottom:12px;overflow:hidden;border-left:3px solid #1e2333;transition:border-color .15s}
    .match-card.prob-h{border-left-color:#22c55e}
    .match-card.prob-m{border-left-color:#f97316}
    .match-card.prob-l{border-left-color:#334155}

    .mc-top{display:grid;grid-template-columns:1fr auto;gap:12px;padding:14px 16px;align-items:start}
    .mc-meta{font-family:'Barlow Condensed',sans-serif;font-size:11px;color:#475569;letter-spacing:.5px;text-transform:uppercase;margin-bottom:4px}
    .mc-teams{font-family:'Barlow Condensed',sans-serif;font-size:20px;font-weight:700;color:#fff;line-height:1.2;margin-bottom:8px}
    .mc-teams .vs{color:#334155;font-weight:400;font-size:14px;margin:0 6px}
    .mc-signals{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
    .sig-dot{width:10px;height:10px;border-radius:50%;display:inline-block}
    .sig-dot.met{background:#22c55e}
    .sig-dot.miss{background:#1e2333;border:1px solid #2d3448}
    .sig-label{font-size:11px;color:#64748b}

    /* Prob badge */
    .prob-badge{text-align:center;flex-shrink:0;min-width:68px}
    .prob-num{font-family:'Barlow Condensed',sans-serif;font-size:32px;font-weight:800;line-height:1}
    .prob-badge.prob-h .prob-num{color:#22c55e}
    .prob-badge.prob-m .prob-num{color:#f97316}
    .prob-badge.prob-l .prob-num{color:#475569}
    .prob-lbl{font-family:'Barlow Condensed',sans-serif;font-size:10px;letter-spacing:1px;text-transform:uppercase;margin-top:2px;color:#475569}
    .result-badge{background:#0d0f14;border:1px solid #1e2333;border-radius:6px;padding:8px 10px;text-align:center;min-width:68px}
    .result-badge .fh{font-family:'Barlow Condensed',sans-serif;font-size:10px;font-weight:700;letter-spacing:1px;color:#475569;text-transform:uppercase}
    .result-badge .score{font-family:'Barlow Condensed',sans-serif;font-size:26px;font-weight:800;line-height:1.1}
    .result-badge.hit .score{color:#22c55e}
    .result-badge.miss .score{color:#ef4444}
    .result-badge .ft{font-size:12px;color:#475569;margin-top:2px}

    /* Stats row */
    .mc-stats{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#1e2333;border-top:1px solid #1e2333}
    .mc-team-stats{background:#161920;padding:12px 16px}
    .mc-team-stats .tname{font-family:'Barlow Condensed',sans-serif;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#64748b;margin-bottom:8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .stat-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
    .stat-row .slabel{font-size:11px;color:#475569}
    .stat-row .sval{font-family:'DM Mono',monospace;font-size:12px;color:#94a3b8;font-weight:500}
    .stat-row .sval.good{color:#22c55e}

    /* Form badges */
    .form-row{display:flex;gap:3px;margin-bottom:6px}
    .fb{width:26px;height:26px;border-radius:4px;display:flex;align-items:center;justify-content:center;font-family:'Barlow Condensed',sans-serif;font-size:11px;font-weight:800}
    .fb.W{background:#064e2b;color:#4ade80}
    .fb.D{background:#1e2333;color:#94a3b8}
    .fb.L{background:#450a0a;color:#f87171}
    .fb.empty{background:#0d0f14;border:1px solid #1e2333;color:#334155}

    /* H2H section */
    .mc-h2h{background:#111318;border-top:1px solid #1e2333;padding:10px 16px}
    .mc-h2h .h2h-title{font-family:'Barlow Condensed',sans-serif;font-size:11px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:#475569;margin-bottom:8px}
    .h2h-row{display:flex;align-items:center;gap:8px;margin-bottom:5px;font-size:12px}
    .h2h-teams{flex:1;color:#64748b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .h2h-fh{font-family:'DM Mono',monospace;font-size:12px;font-weight:500;padding:2px 6px;border-radius:3px;flex-shrink:0}
    .h2h-fh.hit{background:#064e2b;color:#4ade80}
    .h2h-fh.miss{background:#0d0f14;color:#475569;border:1px solid #1e2333}
    .h2h-ft{font-family:'DM Mono',monospace;font-size:11px;color:#334155;flex-shrink:0}
    .no-h2h{font-size:11px;color:#334155;font-style:italic}

    /* Signal detail (collapsible) */
    .mc-detail summary{padding:8px 16px;font-size:11px;color:#475569;cursor:pointer;display:flex;align-items:center;gap:6px;border-top:1px solid #1e2333;list-style:none;font-family:'Barlow Condensed',sans-serif;letter-spacing:.5px;text-transform:uppercase;font-weight:700;transition:color .15s}
    .mc-detail summary:hover{color:#94a3b8}
    .mc-detail summary::-webkit-details-marker{display:none}
    .sig-table{width:100%;border-collapse:collapse;font-size:12px;margin:0 0 0}
    .sig-table th{background:#0d0f14;padding:6px 16px;text-align:left;font-size:10px;color:#334155;font-weight:700;text-transform:uppercase;letter-spacing:.5px;font-family:'Barlow Condensed',sans-serif}
    .sig-table td{padding:8px 16px;border-bottom:1px solid #111318;color:#64748b;vertical-align:top}
    .sig-table tr.met td{background:#0b1a11}
    .sig-table tr.miss td{background:#161920}
    .sig-table td .sname{font-weight:600;color:#94a3b8;margin-bottom:1px}
    .sig-table td .sdesc{font-size:10px;color:#334155}
    .pill{font-family:'Barlow Condensed',sans-serif;font-size:11px;font-weight:700;padding:2px 7px;border-radius:3px;letter-spacing:.5px}
    .pill.met{background:#064e2b;color:#4ade80}
    .pill.miss{background:#450a0a;color:#f87171}
    .mono{font-family:'DM Mono',monospace}

    /* Empty / loading states */
    .empty-state{text-align:center;padding:60px 20px;color:#334155}
    .empty-state p{font-family:'Barlow Condensed',sans-serif;font-size:16px;letter-spacing:1px;text-transform:uppercase;margin-top:8px}


    /* Best bets item */
    .best-item .lname{color:#f59e0b!important}
    .best-item.active{border-left-color:#f59e0b;background:#1a1a0a}
    .prob-high{background:#064e2b;color:#4ade80}
    .sidebar-divider{height:1px;background:#1e2333;margin:4px 0}
    .league-item-inner{display:flex;flex-direction:column;flex:1;min-width:0;margin-right:6px}
    .lmeta{font-size:10px;color:#334155;margin-top:1px;font-family:'DM Mono',monospace}

    /* Card layout */
    .mc-body{display:grid;grid-template-columns:1fr 1fr;border-top:1px solid #1e2333}
    .mc-stats{display:contents}
    .mc-team-col{background:#161920;padding:12px 16px;border-right:1px solid #1e2333}
    .mc-team-col:last-child{border-right:none}
    .mc-h2h{background:#111318;padding:12px 16px;border-top:1px solid #1e2333;grid-column:1/-1}
    .mc-top-left{flex:1;min-width:0}

    /* Form */
    .form-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px}
    .form-label{font-size:10px;color:#334155;text-transform:uppercase;letter-spacing:.5px;font-family:'Barlow Condensed',sans-serif;font-weight:700}
    .form-row{display:flex;gap:3px}

    /* Stats */
    .stat-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:3px}
    .slabel{font-size:11px;color:#475569}
    .sval{font-family:'DM Mono',monospace;font-size:12px;color:#94a3b8}
    .sval.hi{color:#22c55e}

    /* H2H */
    .h2h-title{font-family:'Barlow Condensed',sans-serif;font-size:11px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:#475569;margin-bottom:6px}
    .h2h-summary{font-size:11px;color:#64748b;margin-bottom:6px;font-family:'DM Mono',monospace}
    .h2h-row{display:flex;align-items:center;gap:8px;margin-bottom:4px}
    .h2h-teams{flex:1;font-size:12px;color:#475569;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .h2h-fh{font-family:'DM Mono',monospace;font-size:12px;font-weight:500;padding:1px 6px;border-radius:3px;flex-shrink:0}
    .h2h-fh.hit{background:#064e2b;color:#4ade80}
    .h2h-fh.miss{background:#0d0f14;color:#475569;border:1px solid #1e2333}
    .h2h-ft{font-family:'DM Mono',monospace;font-size:11px;color:#334155;flex-shrink:0}
    .no-h2h{font-size:11px;color:#334155;font-style:italic}

    /* Result badge */
    .rb-fh{font-size:10px;color:#475569;font-weight:700;font-family:'Barlow Condensed',sans-serif;letter-spacing:1px}
    .rb-score{font-family:'Barlow Condensed',sans-serif;font-size:26px;font-weight:800;line-height:1.1}
    .rb-ft{font-size:11px;color:#475569;margin-top:2px}
    .rb-pre{font-size:10px;margin-top:4px}
    .result-badge{background:#0d0f14;border:1px solid #1e2333;border-radius:6px;padding:8px 10px;text-align:center;min-width:68px;flex-shrink:0}
    .result-badge.hit .rb-score{color:#22c55e}
    .result-badge.hit .rb-pre{color:#22c55e}
    .result-badge.miss .rb-score{color:#ef4444}
    .result-badge.miss .rb-pre{color:#ef4444}
    .prob-sub{font-size:10px;color:#475569;margin-top:2px}
    .card-league{font-size:10px;color:#f59e0b;font-weight:700}
    .warn-badge{background:#431407;color:#fb923c;font-size:10px;padding:1px 5px;border-radius:3px;margin-left:6px}
    .thresh{font-size:10px;color:#334155}

    /* Signal table */
    .sig-table{width:100%;border-collapse:collapse;font-size:12px}
    .sig-table th{background:#0d0f14;padding:6px 16px;text-align:left;font-size:10px;color:#334155;font-weight:700;text-transform:uppercase;letter-spacing:.5px;font-family:'Barlow Condensed',sans-serif}
    .sig-table td{padding:8px 16px;border-bottom:1px solid #111318;color:#64748b;vertical-align:top}
    .sig-table tr.met td{background:#0b1a11}
    .sig-table tr.miss td{background:#161920}
    .sname{font-weight:600;color:#94a3b8;margin-bottom:1px}
    .sdesc{font-size:10px;color:#334155}
    .pill{font-family:'Barlow Condensed',sans-serif;font-size:11px;font-weight:700;padding:2px 7px;border-radius:3px;letter-spacing:.5px}
    .pill.met{background:#064e2b;color:#4ade80}
    .pill.miss{background:#450a0a;color:#f87171}

    @media(max-width:640px){
      #sidebar{width:160px}
      .mc-body{grid-template-columns:1fr}
      .mc-team-col:first-child{border-right:none;border-bottom:1px solid #1e2333}
      .mc-h2h{grid-column:1}
    }
  `;

  const js=`
    var ALL=ALL_PREDS;
    var DATES=${datesJSON};
    var DAY_LABELS=["Today","Tomorrow","Day 3","Day 4","Day 5","Day 6"];
    var activeDate=DATES[0];
    var activeLeague=null;

    function localDate(ts){var d=new Date(ts);return d.getFullYear()+'-'+pad(d.getMonth()+1)+'-'+pad(d.getDate());}
    function pad(n){return String(n).padStart(2,'0');}
    function fmtDate(s){return new Date(s+'T12:00:00').toLocaleDateString('en-GB',{weekday:'long',day:'2-digit',month:'short'});}
    function fmtTime(ts){return new Date(ts).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'});}
    function shortName(n){return (n||'').split(' ').slice(0,2).join(' ');}
    function probClass(p){return p>=35?'prob-h':p>=20?'prob-m':'prob-l';}
    function probLabel(p){return p>=35?'🔥 HIGH':p>=20?'⚡ MED':'❄ LOW';}

    ALL.forEach(function(p){p.matchDate=localDate(p.dt);});

    // ── Day tabs ──────────────────────────────────────────────────────────────
    function renderDayTabs(){
      var el=document.getElementById('daytabs'),html='';
      for(var i=0;i<DATES.length;i++){
        var d=DATES[i];
        var cnt=ALL.filter(function(p){return p.matchDate===d;}).length;
        var cls='dtab'+(d===activeDate?' active':'');
        html+='<button class="'+cls+'" onclick="selectDay('+i+')">'+(DAY_LABELS[i]||d)+'<span class="cnt">('+cnt+')</span></button>';
      }
      document.getElementById('daytabs').innerHTML=html;
    }
    function selectDay(i){activeDate=DATES[i];activeLeague=null;renderDayTabs();renderSidebar();renderMain();}

    // ── Sidebar ───────────────────────────────────────────────────────────────
    function renderSidebar(){
      var dayPreds=ALL.filter(function(p){return p.matchDate===activeDate;});
      var leagueMap={};
      dayPreds.forEach(function(p){
        if(!leagueMap[p.league])leagueMap[p.league]={matches:[],nextKO:Infinity};
        leagueMap[p.league].matches.push(p);
        if(p.dt<leagueMap[p.league].nextKO) leagueMap[p.league].nextKO=p.dt;
      });
      var leagues=Object.entries(leagueMap).sort(function(a,b){
        return Math.max.apply(null,b[1].matches.map(function(p){return p.prob;}))-Math.max.apply(null,a[1].matches.map(function(p){return p.prob;}));
      });
      var html='<div class="sidebar-day">'+fmtDate(activeDate)+'</div>';
      // Best bets shortcut
      var allDay=dayPreds.slice().sort(function(a,b){return b.prob-a.prob;});
      var top3=allDay.slice(0,3).filter(function(p){return p.prob>=20;});
      if(top3.length){
        var active=activeLeague==='__best__'?' active':'';
        html+='<div class="league-item best-item'+active+'" onclick="selectLeague(\'__best__\')">';
        html+='<span class="lname">⭐ Best Bets</span>';
        html+='<span class="lbadge"><span class="lcount">'+top3.length+'</span><span class="lprob prob-high">TOP</span></span>';
        html+='</div>';
        html+='<div class="sidebar-divider"></div>';
      }
      html+='<div class="sidebar-section">All Leagues</div>';
      leagues.forEach(function(entry){
        var lname=entry[0],data=entry[1];
        var matches=data.matches;
        var maxP=Math.max.apply(null,matches.map(function(p){return p.prob;}));
        var pc=probClass(maxP);
        var active=lname===activeLeague?' active':'';
        var nextTime=fmtTime(data.nextKO);
        var hotCount=matches.filter(function(p){return p.nMet>=3;}).length;
        var safeL=lname.replace(/\\/g,'\\\\').replace(/'/g,"\\'");
        html+='<div class="league-item'+active+'" onclick="selectLeague(\''+safeL+'\')">'+
          '<div class="league-item-inner">'+
            '<span class="lname">'+lname+'</span>'+
            '<span class="lmeta">'+nextTime+(hotCount?' · <span style="color:#4ade80">'+hotCount+'🔥</span>':'')+'</span>'+
          '</div>'+
          '<span class="lbadge">'+
            '<span class="lcount">'+matches.length+'</span>'+
            '<span class="lprob '+pc+'">'+maxP+'%</span>'+
          '</span>'+
        '</div>';
      });
      if(!leagues.length) html+='<div style="padding:20px 14px;font-size:12px;color:#334155">No matches</div>';
      document.getElementById('sidebar').innerHTML=html;
    }

    // ── Main panel ────────────────────────────────────────────────────────────
    function selectLeague(l){activeLeague=l;renderSidebar();renderMain();}

    function renderMain(){
      var el=document.getElementById('main');
      if(!activeLeague){
        el.innerHTML='<div id="no-league"><div class="icon">⚽</div><p>Select a league from the left</p></div>';
        return;
      }
      var matches;
      if(activeLeague==='__best__'){
        matches=ALL.filter(function(p){return p.matchDate===activeDate&&p.prob>=20;}).sort(function(a,b){return b.prob-a.prob;}).slice(0,10);
      } else {
        matches=ALL.filter(function(p){return p.matchDate===activeDate&&p.league===activeLeague;}).sort(function(a,b){return b.prob-a.prob;});
      }
      var title=activeLeague==='__best__'?'⭐ Best Bets Today':activeLeague;
      var html='<div id="main-header"><h2>'+title+'</h2><span class="sub">'+matches.length+' match'+(matches.length!==1?'es':'')+'</span></div>';
      matches.forEach(function(m){html+=renderCard(m);});
      el.innerHTML=html;
    }

    // ── Form badges ───────────────────────────────────────────────────────────
    function renderForm(formStr,label){
      var html='<div class="form-header"><span class="form-label">'+label+'</span><div class="form-row">';
      var chars=formStr?formStr.toUpperCase().split(''):[];
      while(chars.length<5)chars.unshift('');
      chars.slice(-5).forEach(function(c){
        var cls=c==='W'?'W':c==='D'?'D':c==='L'?'L':'empty';
        html+='<div class="fb '+cls+'" title="'+({'W':'Win','D':'Draw','L':'Loss','':'No data'}[c]||c)+'">'+( c||'·')+'</div>';
      });
      html+='</div></div>';
      return html;
    }

    // ── Match card ────────────────────────────────────────────────────────────
    function renderCard(m){
      var pc=probClass(m.prob);
      var dt=fmtTime(m.dt);
      var leagueLine=activeLeague==='__best__'?'<span class="card-league">'+m.league+'</span> · ':'';
      var dots='';
      m.signals.forEach(function(s){
        dots+='<span class="sig-dot '+(s.met?'met':'miss')+'" title="'+s.label+'"></span>';
      });

      // Prob / result badge
      var badge='';
      if(m.status==='complete'){
        var hit=(m.fhH+m.fhA)>2;
        badge='<div class="result-badge '+(hit?'hit':'miss')+'">'+
          '<div class="rb-fh">FH</div>'+
          '<div class="rb-score">'+m.fhH+'-'+m.fhA+'</div>'+
          '<div class="rb-ft">'+m.ftH+'-'+m.ftA+'</div>'+
          '<div class="rb-pre">'+m.prob+'% pre</div>'+
        '</div>';
      } else {
        badge='<div class="prob-badge '+pc+'">'+
          '<div class="prob-num">'+m.prob+'%</div>'+
          '<div class="prob-lbl">'+probLabel(m.prob)+'</div>'+
          '<div class="prob-sub">FH &gt;2.5</div>'+
        '</div>';
      }

      // Signal rows
      var sigRows='';
      m.signals.forEach(function(s){
        sigRows+='<tr class="'+(s.met?'met':'miss')+'">'+
          '<td><div class="sname">'+s.label+'</div><div class="sdesc">'+s.desc+'</div></td>'+
          '<td class="mono">'+s.hVal+'</td>'+
          '<td class="mono">'+s.aVal+'</td>'+
          '<td class="mono">'+s.combinedVal+'<br><span class="thresh">'+s.threshold+'</span></td>'+
          '<td><span class="pill '+(s.met?'met':'miss')+'">'+(s.met?'✓ MET':'✗ MISS')+'</span><br><span class="thresh">'+s.lift+'</span></td>'+
        '</tr>';
      });

      // H2H
      var h2hHtml='';
      if(m.h2h&&m.h2h.length){
        var hits=m.h2h.filter(function(g){return(g.fhH+g.fhA)>2;}).length;
        h2hHtml='<div class="h2h-summary">'+hits+'/'+m.h2h.length+' meetings with FH &gt;2.5</div>';
        m.h2h.forEach(function(g){
          var ht=g.fhH+g.fhA;var hit=ht>2;
          h2hHtml+='<div class="h2h-row">'+
            '<span class="h2h-teams">'+g.home+' v '+g.away+'</span>'+
            '<span class="h2h-fh '+(hit?'hit':'miss')+'">FH '+g.fhH+'-'+g.fhA+'</span>'+
            '<span class="h2h-ft">'+g.ftH+'-'+g.ftA+' FT</span>'+
          '</div>';
        });
      } else {
        h2hHtml='<div class="no-h2h">No H2H data available</div>';
      }

      var warn=m.missingStats?'<span class="warn-badge">⚠ stats missing</span>':'';

      return '<div class="match-card '+pc+'">'+
        '<div class="mc-top">'+
          '<div class="mc-top-left">'+
            '<div class="mc-meta">'+leagueLine+dt+warn+'</div>'+
            '<div class="mc-teams">'+m.home+'<span class="vs">vs</span>'+m.away+'</div>'+
            '<div class="mc-signals">'+dots+'<span class="sig-label">'+m.nMet+'/4 signals met</span></div>'+
          '</div>'+
          badge+
        '</div>'+
        '<div class="mc-body">'+
          '<div class="mc-stats">'+
            '<div class="mc-team-col">'+
              renderForm(m.hForm,'HT Form')+
              '<div class="stat-row"><span class="slabel">FH Scored avg</span><span class="sval hi">'+m.hFHSc+'</span></div>'+
              '<div class="stat-row"><span class="slabel">FH Conceded avg</span><span class="sval">'+m.hFHCn+'</span></div>'+
            '</div>'+
            '<div class="mc-team-col">'+
              renderForm(m.aForm,'HT Form')+
              '<div class="stat-row"><span class="slabel">FH Scored avg</span><span class="sval hi">'+m.aFHSc+'</span></div>'+
              '<div class="stat-row"><span class="slabel">FH Conceded avg</span><span class="sval">'+m.aFHCn+'</span></div>'+
            '</div>'+
          '</div>'+
          '<div class="mc-h2h">'+
            '<div class="h2h-title">H2H</div>'+
            h2hHtml+
          '</div>'+
        '</div>'+
        '<details class="mc-detail">'+
          '<summary>▾ Signal breakdown</summary>'+
          '<table class="sig-table">'+
            '<thead><tr><th style="width:30%">Signal</th><th style="width:17%">'+shortName(m.home)+'</th>'+
            '<th style="width:17%">'+shortName(m.away)+'</th><th style="width:20%">Combined</th><th>Result</th></tr></thead>'+
            '<tbody>'+sigRows+'</tbody>'+
          '</table>'+
        '</details>'+
      '</div>';
    }

    // ── Init ──────────────────────────────────────────────────────────────────
    renderDayTabs();
    renderSidebar();
    // Auto-select Best Bets if any high-prob matches today, else first league
    (function(){
      var dayP=ALL.filter(function(p){return p.matchDate===activeDate;});
      var hasBest=dayP.some(function(p){return p.prob>=20;});
      if(hasBest){ selectLeague('__best__'); }
      else if(dayP.length){
        var best=dayP.reduce(function(a,b){return b.prob>a.prob?b:a;},dayP[0]);
        if(best) selectLeague(best.league);
      } else { renderMain(); }
    })();
  `;

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>First Half Score</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap" rel="stylesheet">
<script>(function(){var p=new URLSearchParams(window.location.search);if(!p.has('tz')){p.set('tz',-new Date().getTimezoneOffset());window.location.search=p.toString();}})()</script>
<style>${css}</style>
</head>
<body>
<div id="topbar">
  <div class="logo">⚽ First <span>Half</span></div>
  <div id="daytabs"></div>
  <button id="refreshbtn" onclick="location.reload()">↺ Refresh</button>
</div>
<div id="layout">
  <div id="sidebar"></div>
  <div id="main"></div>
</div>
<script>
var ALL_PREDS=${predsJSON};
${js}
</script>
</body>
</html>`;
}

const PORT=process.env.PORT||3001;
app.listen(PORT,()=>{
  console.log("Server running on port "+PORT);
  // Pre-warm cache on startup: fetch today's fixtures so first user request is fast
  const today=new Date().toISOString().slice(0,10);
  fetch("http://localhost:"+PORT+"/?tz=0").catch(()=>{});
});
