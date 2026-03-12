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

const PROB_TABLE=[10,20,25,35,45];

// ── Cache (20-min TTL) ────────────────────────────────────────────────────────
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

// ── Debug / utility endpoints ─────────────────────────────────────────────────
app.get("/debug-league",async(req,res)=>{
  const sid=req.query.sid||"14924";
  const date=req.query.date||new Date().toISOString().slice(0,10);
  try{
    const [fixtureRes,teamRes]=await Promise.all([
      fetch(BASE+"/todays-matches?date="+date+"&key="+KEY).then(r=>r.json()),
      fetch(BASE+"/league-teams?season_id="+sid+"&include=stats&key="+KEY).then(r=>r.json())
    ]);
    const fixtures=(fixtureRes.data||[]).filter(m=>String(m.competition_id)===String(sid));
    const sampleFixture=fixtures[0]||{};
    const sampleTeam=(teamRes.data||[])[0]||{};
    res.json({
      sid,date,
      fixtureCount:fixtures.length,
      fixtureIds:fixtures.map(m=>({home:m.home_name,away:m.away_name,homeID:m.homeID,awayID:m.awayID,competition_id:m.competition_id})),
      teamCount:(teamRes.data||[]).length,
      sampleTeamName:sampleTeam.name,
      sampleTeamId:sampleTeam.id,
      sampleTeamTopKeys:Object.keys(sampleTeam),
      sampleTeamStatsKeys:sampleTeam.stats?Object.keys(sampleTeam.stats).slice(0,40):[],
      sampleFixtureKeys:Object.keys(sampleFixture),
      sampleFixtureOddsFields:Object.fromEntries(Object.entries(sampleFixture).filter(([k])=>k.toLowerCase().includes('odd')))
    });
  }catch(e){res.status(500).json({error:e.message});}
});

app.get("/cache-status",(req,res)=>{
  const entries=[..._cache.entries()].map(([url,e])=>({
    url:url.replace(/key=[^&]+/,"key=***"),
    age:Math.round((Date.now()-e.ts)/1000)+"s",
    expiresIn:Math.max(0,Math.round((CACHE_TTL-(Date.now()-e.ts))/1000))+"s"
  }));
  res.json({count:entries.length,ttl:CACHE_TTL/1000+"s",entries});
});
app.get("/cache-flush",(req,res)=>{const n=_cache.size;_cache.clear();res.json({flushed:n});});

app.get("/api/*",async(req,res)=>{
  try{
    const path=req.path.replace("/api","");
    const qs=new URLSearchParams({...req.query,key:KEY}).toString();
    const data=await ftch(BASE+path+"?"+qs);
    res.json(data);
  }catch(e){res.status(500).json({error:e.message})}
});

// ── Signal helpers ─────────────────────────────────────────────────────────────

// CI: H scored(home) + A scored(away) + H conceded(home) + A conceded(away) >= 3.2
function calcCI(ht,at){
  const val=safe(ht.scoredAVGHT_home)+safe(at.scoredAVGHT_away)
           +safe(ht.concededAVGHT_home)+safe(at.concededAVGHT_away);
  return{val:+val.toFixed(3),met:val>=3.2};
}

// T1: both teams seasonOver25PercentageHT_overall >= 20%
function calcT1(ht,at){
  const hv=safe(ht.seasonOver25PercentageHT_overall);
  const av=safe(at.seasonOver25PercentageHT_overall);
  return{hVal:hv,aVal:av,met:hv>=20&&av>=20};
}

// FH15: replaces odds signal — both teams seasonOver15PercentageHT_overall >= 40%
// Confirmed available in stats sub-object from debug-league endpoint
function calcFH15(ht,at){
  const hv=safe(ht.seasonOver15PercentageHT_overall);
  const av=safe(at.seasonOver15PercentageHT_overall);
  return{hVal:hv,aVal:av,met:hv>=40&&av>=40};
}

// CN010: either team concedes >= 0.25 goals/game in minutes 0-10
// Fields from /league-teams?include=stats:
//   goals_conceded_min_0_to_10_home/away = total goals conceded in 0-10min in that role
//   seasonMatchesPlayed_home/away        = matches played in that role (correct denominator)
function calcCN010(ht,at){
  // Home team defending at home
  const hMP=safe(ht.seasonMatchesPlayed_home)||safe(ht.mp_home)||safe(ht.matchesPlayed_home)||1;
  const hCon=safe(ht.goals_conceded_min_0_to_10_home)||safe(ht.goals_conceded_min_0_to_10_overall)||0;
  const hRate=hCon/hMP;
  // Away team defending away
  const aMP=safe(at.seasonMatchesPlayed_away)||safe(at.mp_away)||safe(at.matchesPlayed_away)||1;
  const aCon=safe(at.goals_conceded_min_0_to_10_away)||safe(at.goals_conceded_min_0_to_10_overall)||0;
  const aRate=aCon/aMP;
  return{
    hRate:+hRate.toFixed(3),hCon,hMP,
    aRate:+aRate.toFixed(3),aCon,aMP,
    met:hRate>=0.25||aRate>=0.25
  };
}

// ── Main route ────────────────────────────────────────────────────────────────
app.get("/",async(req,res)=>{
  try{
    const tzOffset=parseInt(req.query.tz||"0");
    const dates=getDates(tzOffset);

    const dayResults=await Promise.all(
      dates.map(d=>ftch(BASE+"/todays-matches?date="+d+"&key="+KEY))
    );
    const allFixtures=[];
    for(let i=0;i<dates.length;i++){
      for(const m of (dayResults[i].data||[])){
        allFixtures.push(Object.assign({},m,{_date:dates[i]}));
      }
    }

    const leagueFixtures={};
    const leagueNameOverride={};
    for(const m of allFixtures){
      const sid=parseInt(m.competition_id);
      if(!sid) continue;
      if(!leagueFixtures[sid]) leagueFixtures[sid]=[];
      leagueFixtures[sid].push(m);
      if(!leagueNameOverride[sid]){
        leagueNameOverride[sid]=LEAGUE_NAMES[sid]||m.league||m.competition||m.competition_name||("League "+sid);
      }
    }

    const preds=[];
    for(const sid of Object.keys(leagueFixtures)){
      let teamMap={};
      try{
        const r=await ftch(BASE+"/league-teams?season_id="+sid+"&include=stats&key="+KEY);
        for(const t of (r.data||[])){
          if(t.id!=null){teamMap[t.id]=t;teamMap[String(t.id)]=t;}
          teamMap["__name__"+(t.name||"").toLowerCase().trim()]=t;
          if(t.clean_name) teamMap["__name__"+t.clean_name.toLowerCase().trim()]=t;
        }
      }catch(e){console.log("league-teams error sid="+sid,e.message);}

      if(!Object.keys(teamMap).length){console.log("No team data sid="+sid);continue;}

      const unwrap=t=>{
        if(!t) return null;
        if(t.stats&&typeof t.stats==="object") return Object.assign({},t,t.stats);
        return t;
      };

      // Build teamHistory: last 5 completed matches per team from league-matches
      // Uses the already-cached league-matches endpoint — zero extra API calls
      const teamHistory={};
      try{
        const lmRes=await ftch(BASE+"/league-matches?season_id="+sid+"&max_per_page=150&page=1&key="+KEY);
        const completed=(lmRes.data||[]).filter(m=>m.status==="complete");
        // Sort newest first
        completed.sort((a,b)=>(b.date_unix||0)-(a.date_unix||0));
        for(const m of completed){
          const addGame=(teamName,isHome)=>{
            const key=teamName.toLowerCase().trim();
            if(!teamHistory[key]) teamHistory[key]=[];
            if(teamHistory[key].length>=5) return;
            teamHistory[key].push({
              opp:isHome?m.away_name:m.home_name,
              venue:isHome?"H":"A",
              htFor:isHome?parseInt(m.ht_goals_team_a||0):parseInt(m.ht_goals_team_b||0),
              htAgainst:isHome?parseInt(m.ht_goals_team_b||0):parseInt(m.ht_goals_team_a||0),
              ftFor:isHome?parseInt(m.homeGoalCount||0):parseInt(m.awayGoalCount||0),
              ftAgainst:isHome?parseInt(m.awayGoalCount||0):parseInt(m.homeGoalCount||0),
              date:m.date_unix?new Date(m.date_unix*1000).toISOString().slice(0,10):""
            });
          };
          if(m.home_name) addGame(m.home_name,true);
          if(m.away_name) addGame(m.away_name,false);
        }
      }catch(e){console.log("teamHistory build error sid="+sid,e.message);}

      for(const fixture of (leagueFixtures[sid]||[])){
        const homeId=String(fixture.homeID||fixture.home_id||"");
        const awayId=String(fixture.awayID||fixture.away_id||"");
        const htRaw=teamMap[homeId]||teamMap[parseInt(homeId)]||teamMap["__name__"+(fixture.home_name||"").toLowerCase().trim()];
        const atRaw=teamMap[awayId]||teamMap[parseInt(awayId)]||teamMap["__name__"+(fixture.away_name||"").toLowerCase().trim()];

        const ht=unwrap(htRaw)||{};
        const at=unwrap(atRaw)||{};

        if(!htRaw||!atRaw){
          console.log("Missing stats: "+fixture.home_name+" vs "+fixture.away_name+" sid="+sid);
        }

        const ci   = calcCI(ht,at);
        const t1   = calcT1(ht,at);
        const fh15 = calcFH15(ht,at);
        const cn   = calcCN010(ht,at);

        // Last 5 from teamHistory (built from league-matches already fetched — zero extra API calls)
        const getLast5=(teamName)=>{
          const games=(teamHistory[teamName.toLowerCase().trim()]||[]);
          return games.slice(0,5);
        };
        const hLast5=getLast5(fixture.home_name||"");
        const aLast5=getLast5(fixture.away_name||"");

        const signals=[
          {
            key:"CI",
            label:"HT Intensity Index (CI)",
            desc:"H scored(home) + A scored(away) + H conceded(home) + A conceded(away) ≥ 3.2 — 2.76x lift",
            hVal:safe(ht.scoredAVGHT_home).toFixed(2)+" sc / "+safe(ht.concededAVGHT_home).toFixed(2)+" cn",
            aVal:safe(at.scoredAVGHT_away).toFixed(2)+" sc / "+safe(at.concededAVGHT_away).toFixed(2)+" cn",
            combinedVal:ci.val.toFixed(3),
            threshold:"≥ 3.2",
            met:ci.met,
            lift:"2.76x lift"
          },
          {
            key:"T1",
            label:"Both Teams FH Over 2.5 Rate",
            desc:"seasonOver25PercentageHT_overall ≥ 20% for both teams — 2.65x lift",
            hVal:safe(ht.seasonOver25PercentageHT_overall).toFixed(1)+"%",
            aVal:safe(at.seasonOver25PercentageHT_overall).toFixed(1)+"%",
            combinedVal:Math.min(safe(ht.seasonOver25PercentageHT_overall),safe(at.seasonOver25PercentageHT_overall)).toFixed(1)+"% (lower)",
            threshold:"both ≥ 20%",
            met:t1.met,
            lift:"2.65x lift"
          },
          {
            key:"FH15",
            label:"Both Teams FH Over 1.5 Rate",
            desc:"seasonOver15PercentageHT_overall ≥ 40% for both teams — 2.08x lift",
            hVal:safe(ht.seasonOver15PercentageHT_overall).toFixed(1)+"%",
            aVal:safe(at.seasonOver15PercentageHT_overall).toFixed(1)+"%",
            combinedVal:Math.min(safe(ht.seasonOver15PercentageHT_overall),safe(at.seasonOver15PercentageHT_overall)).toFixed(1)+"% (lower)",
            threshold:"both ≥ 40%",
            met:fh15.met,
            lift:"2.08x lift"
          },
          {
            key:"CN010",
            label:"Early Goal Conceded Rate (0-10 min)",
            desc:"Either team concedes ≥ 0.25 goals/game in minutes 0–10 — chaos signal",
            hVal:cn.hCon+" in "+cn.hMP+" ("+cn.hRate.toFixed(3)+"/g home)",
            aVal:cn.aCon+" in "+cn.aMP+" ("+cn.aRate.toFixed(3)+"/g away)",
            combinedVal:Math.max(cn.hRate,cn.aRate).toFixed(3)+" (higher)",
            threshold:"either ≥ 0.25",
            met:cn.met,
            lift:"chaos signal"
          }
        ];

        const nMet=signals.filter(s=>s.met).length;
        const prob=PROB_TABLE[Math.min(nMet,4)];

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
          hLast5,aLast5,
          hAvgFH:{
            scored:safe(ht.scoredAVGHT_overall).toFixed(2),
            scoredHome:safe(ht.scoredAVGHT_home).toFixed(2),
            conceded:safe(ht.concededAVGHT_overall).toFixed(2),
            concededHome:safe(ht.concededAVGHT_home).toFixed(2)
          },
          aAvgFH:{
            scored:safe(at.scoredAVGHT_overall).toFixed(2),
            scoredAway:safe(at.scoredAVGHT_away).toFixed(2),
            conceded:safe(at.concededAVGHT_overall).toFixed(2),
            concededAway:safe(at.concededAVGHT_away).toFixed(2)
          }
        });
      }
    }

    preds.sort((a,b)=>b.prob-a.prob||b.nMet-a.nMet);
    res.send(buildHTML(preds,dates));
  }catch(e){
    console.error(e);
    res.status(500).send("<pre>Error: "+e.message+"\n"+e.stack+"</pre>");
  }
});

// ── HTML builder ──────────────────────────────────────────────────────────────
function buildHTML(preds,dates){
  const predsJSON=JSON.stringify(preds).replace(/</g,"\\u003c");
  const datesJSON=JSON.stringify(dates);

  var H="";
  H+="<!DOCTYPE html><html><head>";
  H+="<meta charset=\"UTF-8\">";
  H+="<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">";
  H+="<title>First Half Score</title>";
  H+="<script>(function(){var p=new URLSearchParams(window.location.search);if(!p.has('tz')){p.set('tz',-new Date().getTimezoneOffset());window.location.search=p.toString();}})();<\/script>";
  H+="<style>";
  H+="*{box-sizing:border-box;margin:0;padding:0}";
  H+="body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;font-size:15px}";
  H+="details>summary::-webkit-details-marker{display:none}";
  H+=".tab{padding:8px 14px;border-radius:6px;cursor:pointer;font-size:14px;font-weight:600;border:1px solid #e5e7eb;background:#fff;color:#6b7280;transition:all .15s}";
  H+=".tab.active{background:#111827;color:#fff;border-color:#111827}";
  H+=".league-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:16px 18px;margin-bottom:10px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;box-shadow:0 1px 3px rgba(0,0,0,.05);transition:box-shadow .15s}";
  H+=".league-card:hover{box-shadow:0 3px 8px rgba(0,0,0,.1)}";
  H+=".back-btn{background:#f3f4f6;border:1px solid #e5e7eb;padding:7px 16px;border-radius:6px;cursor:pointer;font-size:15px;font-weight:600;color:#374151}";
  H+=".match-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:14px}";
  H+="summary{cursor:pointer;user-select:none;list-style:none}";
  H+=".sig-table{width:100%;border-collapse:collapse;table-layout:fixed;font-size:13px}";
  H+=".sig-table th{background:#f9fafb;padding:7px 8px;text-align:left;font-size:11px;color:#6b7280;font-weight:600;text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid #e5e7eb}";
  H+=".sig-table td{padding:10px 8px;border-bottom:1px solid #f3f4f6;vertical-align:top}";
  H+=".sig-row-met{background:#f0fdf4}";
  H+=".sig-row-unmet{background:#fff}";
  H+=".pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700}";
  H+=".pill-met{background:#dcfce7;color:#15803d}";
  H+=".pill-unmet{background:#fee2e2;color:#dc2626}";
  H+="</style></head><body>";

  H+="<div id=\"header\" style=\"background:#fff;border-bottom:1px solid #e5e7eb;padding:14px 20px;position:sticky;top:0;z-index:10\">";
  H+="<div style=\"max-width:780px;margin:0 auto\">";
  H+="<div style=\"display:flex;align-items:center;justify-content:space-between;margin-bottom:12px\">";
  H+="<div><div style=\"font-size:11px;color:#6b7280;letter-spacing:1px;text-transform:uppercase\">&#9917; First Half Score</div>";
  H+="<h1 style=\"font-size:22px;font-weight:800;color:#111827\" id=\"headerTitle\">Loading...</h1></div>";
  H+="<button onclick=\"location.reload()\" style=\"background:#111827;color:#fff;padding:8px 16px;font-size:14px;border:none;border-radius:6px;font-weight:600;cursor:pointer\">&#8635; Refresh</button>";
  H+="</div>";
  H+="<div id=\"dayTabs\" style=\"display:flex;gap:8px;flex-wrap:wrap\"></div>";
  H+="</div></div>";

  H+="<div style=\"padding:16px 20px;max-width:780px;margin:0 auto\">";
  H+="<div style=\"background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#92400e;line-height:1.6\">";
  H+="<strong>How it works:</strong> 4 data-driven signals from team season stats. ";
  H+="0 signals = 10% &nbsp;|&nbsp; 1 = 20% &nbsp;|&nbsp; 2 = 25% &nbsp;|&nbsp; 3 = 35% &nbsp;|&nbsp; 4 = 45% chance of FH &gt; 2.5 goals.";
  H+="</div>";
  H+="<div id=\"mainView\"></div></div>";

  H+="<script>";
  H+="var ALL_PREDS="+predsJSON+";";
  H+="var DATES="+datesJSON+";";
  H+="var DAY_LABELS=[\"Today\",\"Tomorrow\",\"Day 3\",\"Day 4\",\"Day 5\",\"Day 6\"];";
  H+="var activeDate=DATES[0];var activeLeague=null;";
  H+="function localDateStr(ts){var d=new Date(ts);return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');}";
  H+="ALL_PREDS.forEach(function(p){p.matchDate=localDateStr(p.dt);});";
  H+="function fmt(d){return new Date(d).toLocaleDateString('en-GB',{weekday:'long',day:'2-digit',month:'short'});}";

  H+="function renderTabs(){";
  H+="var el=document.getElementById('dayTabs'),html='';";
  H+="for(var i=0;i<DATES.length;i++){";
  H+="var d=DATES[i],count=ALL_PREDS.filter(function(p){return p.matchDate===d;}).length;";
  H+="var cls=d===activeDate?'tab active':'tab';";
  H+="html+='<button class=\"'+cls+'\" onclick=\"selectDay('+i+')\">'+( DAY_LABELS[i]||d)+' <span style=\"font-size:12px;opacity:.7\">('+count+')</span></button>';";
  H+="}el.innerHTML=html;}";

  H+="function selectDay(i){activeDate=DATES[i];activeLeague=null;renderTabs();renderLeagueList();document.getElementById('headerTitle').textContent=fmt(new Date(DATES[i]+'T12:00:00'));}";

  H+="function renderLeagueList(){";
  H+="var dayPreds=ALL_PREDS.filter(function(p){return p.matchDate===activeDate;});";
  H+="var leagueMap={};";
  H+="for(var i=0;i<dayPreds.length;i++){var p=dayPreds[i];if(!leagueMap[p.league])leagueMap[p.league]=[];leagueMap[p.league].push(p);}";
  H+="var leagueList=Object.entries(leagueMap).sort(function(a,b){return Math.max.apply(null,b[1].map(function(p){return p.prob;}))-Math.max.apply(null,a[1].map(function(p){return p.prob;}));});";
  H+="if(!leagueList.length){document.getElementById('mainView').innerHTML='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280\">No matches found for this day.</div>';return;}";
  H+="var html='<div style=\"font-size:13px;color:#6b7280;margin-bottom:12px\">'+dayPreds.length+' matches across '+leagueList.length+' leagues &middot; sorted by probability</div>';";
  H+="for(var j=0;j<leagueList.length;j++){";
  H+="var league=leagueList[j][0],matches=leagueList[j][1];";
  H+="var maxProb=Math.max.apply(null,matches.map(function(p){return p.prob;}));";
  H+="var maxN=Math.max.apply(null,matches.map(function(p){return p.nMet;}));";
  H+="var probCol=maxProb>=35?'#16a34a':maxProb>=20?'#d97706':'#6b7280';";
  H+="var hotCount=matches.filter(function(p){return p.nMet>=3;}).length;";
  H+="var hotStr=hotCount>0?' &middot; <span style=\"color:#15803d;font-weight:600\">'+hotCount+' with 3+ signals</span>':'';";
  H+="var safeLeague=league.replace(/\\\\/g,'\\\\\\\\').replace(/'/g,\"\\\\'\");";
  H+="html+='<div class=\"league-card\" onclick=\"selectLeague(\\''+safeLeague+'\\')\">';";
  H+="html+='<div style=\"flex:1;min-width:0;margin-right:12px\">';";
  H+="html+='<div style=\"font-size:18px;font-weight:700;color:#111827;margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis\">'+league+'</div>';";
  H+="html+='<div style=\"font-size:13px;color:#6b7280\">'+matches.length+' match'+(matches.length>1?'es':'')+hotStr+'</div>';";
  H+="html+='</div><div style=\"text-align:right;flex-shrink:0\">';";
  H+="html+='<div style=\"font-size:26px;font-weight:800;color:'+probCol+'\">'+maxProb+'%</div>';";
  H+="html+='<div style=\"font-size:11px;color:#9ca3af;margin-top:1px\">'+maxN+'/4 signals</div>';";
  H+="html+='</div></div>';}";
  H+="document.getElementById('mainView').innerHTML=html;}";

  H+="function selectLeague(league){activeLeague=league;renderMatchList();}";
  H+="function backToLeagues(){activeLeague=null;renderLeagueList();}";

  H+="function renderMatchList(){";
  H+="var matches=ALL_PREDS.filter(function(p){return p.matchDate===activeDate&&p.league===activeLeague;}).sort(function(a,b){return b.prob-a.prob;});";
  H+="var html='<div style=\"display:flex;align-items:center;gap:12px;margin-bottom:16px\">';";
  H+="html+='<button class=\"back-btn\" onclick=\"backToLeagues()\">&#8592; Back</button>';";
  H+="html+='<div style=\"font-size:19px;font-weight:700;color:#111827\">'+activeLeague+'</div></div>';";
  H+="for(var i=0;i<matches.length;i++)html+=renderMatchCard(matches[i]);";
  H+="document.getElementById('mainView').innerHTML=html;}";

  H+="function shortName(n){return (n||'').split(' ').slice(0,2).join(' ');}";

  H+="function renderMatchCard(m){";
  H+="var probCol=m.prob>=35?'#16a34a':m.prob>=20?'#d97706':'#6b7280';";
  H+="var probBg=m.prob>=35?'#f0fdf4':m.prob>=20?'#fffbeb':'#f9fafb';";
  H+="var probBorder=m.prob>=35?'#bbf7d0':m.prob>=20?'#fde68a':'#e5e7eb';";
  H+="var probLabel=m.prob>=35?'&#128293; HIGH':m.prob>=20?'&#9889; MED':'&#10052; LOW';";
  H+="var dt=new Date(m.dt).toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'});";
  H+="var dots='';";
  H+="for(var i=0;i<m.signals.length;i++){var s=m.signals[i];dots+='<span title=\"'+s.label+'\" style=\"display:inline-block;width:12px;height:12px;border-radius:50%;background:'+(s.met?'#16a34a':'#e5e7eb')+';margin-right:3px\"></span>';}";
  H+="var sigRows='';";
  H+="for(var i=0;i<m.signals.length;i++){var s=m.signals[i];";
  H+="sigRows+='<tr class=\"'+(s.met?'sig-row-met':'sig-row-unmet')+'\">';";
  H+="sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6\"><div style=\"font-weight:600;color:#111827;margin-bottom:2px\">'+s.label+'</div><div style=\"font-size:11px;color:#9ca3af\">'+s.desc+'</div></td>';";
  H+="sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><div style=\"font-weight:700;color:#374151;font-size:13px\">'+s.hVal+'</div></td>';";
  H+="sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><div style=\"font-weight:700;color:#374151;font-size:13px\">'+s.aVal+'</div></td>';";
  H+="sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><div style=\"font-weight:800;font-size:14px;color:'+(s.met?'#15803d':'#dc2626')+'\">'+s.combinedVal+'</div><div style=\"font-size:10px;color:#9ca3af\">'+s.threshold+'</div></td>';";
  H+="sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><span class=\"pill '+(s.met?'pill-met':'pill-unmet')+'\">'+(s.met?'&#10003; MET':'&#10007; MISS')+'</span><div style=\"font-size:10px;color:#9ca3af;margin-top:3px\">'+s.lift+'</div></td></tr>';}";
  H+="var warnStr=m.missingStats?'<span style=\"background:#fef3c7;color:#92400e;font-size:11px;padding:2px 7px;border-radius:4px;margin-left:8px;font-weight:600\">&#9888; missing stats</span>':'';";
  H+="var html='<div class=\"match-card\" style=\"border-left:4px solid '+probCol+'\">';";
  H+="html+='<div style=\"padding:16px\">';";
  H+="html+='<div style=\"font-size:11px;color:#9ca3af;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px\">'+m.league+'</div>';";
  H+="html+='<div style=\"display:grid;grid-template-columns:1fr auto;gap:10px;align-items:start;margin-bottom:12px\">';";
  H+="html+='<div style=\"min-width:0\">';";
  H+="html+='<div style=\"font-size:12px;color:#9ca3af;margin-bottom:3px\">'+dt+warnStr+'</div>';";
  H+="html+='<div style=\"font-size:17px;font-weight:700;color:#111827;margin-bottom:5px;line-height:1.3\">'+m.home+' <span style=\"color:#d1d5db;font-weight:400;font-size:13px\">vs</span> '+m.away+'</div>';";
  H+="html+='<div style=\"display:flex;align-items:center;gap:8px\">'+dots+'<span style=\"font-size:12px;color:#6b7280\">'+m.nMet+'/4 signals met</span></div>';";
  H+="html+='</div>';";
  H+="if(m.status==='complete'){";
  H+="var fhHit=(m.fhH+m.fhA)>2;";
  H+="var rb=fhHit?'#f0fdf4':'#fef2f2',rbr=fhHit?'#bbf7d0':'#fecaca',rfc=fhHit?'#16a34a':'#dc2626';";
  H+="html+='<div style=\"text-align:center;min-width:76px;background:'+rb+';border:1px solid '+rbr+';border-radius:8px;padding:8px 6px;flex-shrink:0\">';";
  H+="html+='<div style=\"font-size:11px;color:#9ca3af;font-weight:600\">FH</div>';";
  H+="html+='<div style=\"font-size:24px;font-weight:800;color:'+rfc+';line-height:1.1\">'+m.fhH+'-'+m.fhA+'</div>';";
  H+="html+='<div style=\"font-size:10px;color:#9ca3af;margin-top:5px;font-weight:600\">FT</div>';";
  H+="html+='<div style=\"font-size:16px;font-weight:700;color:#374151;line-height:1.1\">'+m.ftH+'-'+m.ftA+'</div>';";
  H+="html+='<div style=\"font-size:11px;color:'+probCol+';margin-top:5px;font-weight:700\">'+m.prob+'% pre</div></div>';";
  H+="}else{";
  H+="html+='<div style=\"text-align:center;min-width:76px;background:'+probBg+';border:1px solid '+probBorder+';border-radius:8px;padding:10px 6px;flex-shrink:0\">';";
  H+="html+='<div style=\"font-size:30px;font-weight:800;color:'+probCol+';line-height:1\">'+m.prob+'%</div>';";
  H+="html+='<div style=\"font-size:12px;color:'+probCol+';margin-top:2px\">'+probLabel+'</div>';";
  H+="html+='<div style=\"font-size:10px;color:#9ca3af;margin-top:2px\">FH OVER 2.5</div></div>';}";
  H+="html+='</div>';";
  H+="html+='<details><summary style=\"font-size:13px;color:#6b7280;padding:5px 0;border-top:1px solid #f3f4f6\">&#9660; Show signal detail</summary>';";
  H+="html+='<div style=\"padding-top:10px\">';";
  H+="html+='<table class=\"sig-table\" style=\"margin-bottom:14px\"><thead><tr>';";
  H+="html+='<th style=\"width:30%\">Signal</th>';";
  H+="html+='<th style=\"width:16%;text-align:center\">'+shortName(m.home)+'</th>';";
  H+="html+='<th style=\"width:16%;text-align:center\">'+shortName(m.away)+'</th>';";
  H+="html+='<th style=\"width:18%;text-align:center\">Combined</th>';";
  H+="html+='<th style=\"width:20%;text-align:center\">Result</th>';";
  H+="html+='</tr></thead><tbody>'+sigRows+'</tbody></table>';";

  // ── Avg FH stats (from FootyStats team stats) ──
  H+="html+='<div style=\"display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px\">';";
  H+="function mkAvgBlock(name,s,roleLabel){";
  H+="  var b='<div style=\"background:#f9fafb;border:1px solid #e5e7eb;border-radius:6px;padding:10px 12px\">';";
  H+="  b+='<div style=\"font-size:11px;font-weight:700;color:#374151;margin-bottom:5px\">'+name+' \u2014 FH avg</div>';";
  H+="  b+='<div style=\"font-size:12px;color:#6b7280\">Scored <strong style=\"color:#111827\">'+s.scored+'</strong> &middot; Conceded <strong style=\"color:#111827\">'+s.conceded+'</strong> (overall)</div>';";
  H+="  b+='<div style=\"font-size:11px;color:#9ca3af;margin-top:2px\">'+roleLabel+': <strong>'+s.roleScored+'</strong> sc / <strong>'+s.roleConceded+'</strong> cn</div>';";
  H+="  b+='</div>';";
  H+="  return b;";
  H+="}";
  H+="var hFHStats={scored:m.hAvgFH?m.hAvgFH.scored:'-',conceded:m.hAvgFH?m.hAvgFH.conceded:'-',roleScored:m.hAvgFH?m.hAvgFH.scoredHome:'-',roleConceded:m.hAvgFH?m.hAvgFH.concededHome:'-'};";
  H+="var aFHStats={scored:m.aAvgFH?m.aAvgFH.scored:'-',conceded:m.aAvgFH?m.aAvgFH.conceded:'-',roleScored:m.aAvgFH?m.aAvgFH.scoredAway:'-',roleConceded:m.aAvgFH?m.aAvgFH.concededAway:'-'};";
  H+="html+=mkAvgBlock(shortName(m.home),hFHStats,'Home');";
  H+="html+=mkAvgBlock(shortName(m.away),aFHStats,'Away');";
  H+="html+='</div>';";

  // ── Last 5 matches per team ──
  H+="function last5Table(teamName,games){";
  H+="  if(!games||!games.length)return '';";
  H+="  var rows='';";
  H+="  for(var i=0;i<games.length;i++){";
  H+="    var g=games[i];";
  H+="    var htTot=g.htFor+g.htAgainst;";
  H+="    var htCol=htTot>=2?'#15803d':'#374151';";
  H+="    var htBg=htTot>=2?'#f0fdf4':'transparent';";
  H+="    var vBg=g.venue==='H'?'#eff6ff':'#fdf4ff';";
  H+="    var vCol=g.venue==='H'?'#1d4ed8':'#7e22ce';";
  H+="    rows+='<tr style=\"border-bottom:1px solid #f3f4f6\">';";
  H+="    rows+='<td style=\"padding:5px 8px;font-size:11px;color:#9ca3af\">'+g.date+'</td>';";
  H+="    rows+='<td style=\"padding:5px 6px\"><span style=\"background:'+vBg+';color:'+vCol+';font-size:10px;font-weight:700;padding:1px 5px;border-radius:3px\">'+g.venue+'</span></td>';";
  H+="    rows+='<td style=\"padding:5px 8px;font-size:12px;color:#374151;max-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">'+g.opp+'</td>';";
  H+="    rows+='<td style=\"padding:5px 8px;font-size:13px;font-weight:700;text-align:center;color:'+htCol+';background:'+htBg+'\">'+g.htFor+'-'+g.htAgainst+'</td>';";
  H+="    rows+='<td style=\"padding:5px 8px;font-size:12px;color:#6b7280;text-align:center\">'+g.ftFor+'-'+g.ftAgainst+'</td>';";
  H+="    rows+='</tr>';";
  H+="  }";
  H+="  var t='<div style=\"margin-bottom:12px\">';";
  H+="  t+='<div style=\"font-size:11px;font-weight:700;color:#374151;margin-bottom:4px\">'+teamName+' \u2014 last 5 games</div>';";
  H+="  t+='<table style=\"width:100%;border-collapse:collapse;font-size:12px\">';";
  H+="  t+='<thead><tr style=\"background:#f9fafb;border-bottom:1px solid #e5e7eb\">';";
  H+="  t+='<th style=\"padding:4px 8px;font-size:10px;color:#9ca3af;font-weight:600;text-align:left\">Date</th>';";
  H+="  t+='<th style=\"padding:4px 6px;font-size:10px;color:#9ca3af;font-weight:600\">H/A</th>';";
  H+="  t+='<th style=\"padding:4px 8px;font-size:10px;color:#9ca3af;font-weight:600;text-align:left\">Opponent</th>';";
  H+="  t+='<th style=\"padding:4px 8px;font-size:10px;color:#9ca3af;font-weight:600;text-align:center\">HT</th>';";
  H+="  t+='<th style=\"padding:4px 8px;font-size:10px;color:#9ca3af;font-weight:600;text-align:center\">FT</th>';";
  H+="  t+='</tr></thead><tbody>'+rows+'</tbody></table></div>';";
  H+="  return t;";
  H+="}";
  H+="html+=last5Table(m.home,m.hLast5||[]);";
  H+="html+=last5Table(m.away,m.aLast5||[]);";

  H+="html+='</div></details></div></div>';";
  H+="return html;}";

  H+="document.getElementById('headerTitle').textContent=fmt(new Date());";
  H+="renderTabs();renderLeagueList();";
  H+="<\/script></body></html>";
  return H;
}

const PORT=process.env.PORT||3001;
app.listen(PORT,()=>console.log("Server running on port "+PORT));
