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

// Probability based on strong/medium signal counts (backtested on 5,699 matches)
// Strong: S1=expFH>=1.20, S2=odds_fh_o15<=1.80, S3=both teams BTTS FH>=25%
// Medium: M1=odds<=2.00, M2=either CS_ht<=30%, M3=either o25ht>=20%, M4=either cn010>=0.25
function calcProb(strongMet,mediumMet,odds_fh_o15){
  if(strongMet===3)return{prob:48,label:"Prime Pick"};
  if(strongMet===2&&odds_fh_o15<=2.00)return{prob:42,label:"Strong Pick"};
  if(strongMet>=1&&mediumMet>=2)return{prob:35,label:"Worth Watching"};
  if(mediumMet>=2)return{prob:25,label:"Moderate"};
  return{prob:13,label:"Low Signal"};
}

const ftch=url=>fetch(url).then(r=>r.json());
const avgF=(arr,fn)=>arr.length?arr.reduce((s,x)=>s+fn(x),0)/arr.length:0;
const pctF=(arr,fn)=>arr.length?arr.filter(fn).length/arr.length:0;
const pm=t=>{const m=String(t).match(/^(\d+)/);return m?parseInt(m[1]):null};
const safe=v=>isNaN(v)||!isFinite(v)?0:v;

// In-memory cache — TTL 55 min (stays under the 1800/hour rate limit)
const CACHE={};
const CACHE_TTL=55*60*1000;
function cacheGet(key){const e=CACHE[key];if(e&&Date.now()-e.ts<CACHE_TTL)return e.val;return null;}
function cacheSet(key,val){CACHE[key]={val,ts:Date.now()};return val;}
async function ftchCached(url,key){const hit=cacheGet(key);if(hit)return hit;const val=await ftch(url);if(val&&!val.error)cacheSet(key,val);return val;}

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
  for(let i=0;i<4;i++){
    const d=new Date(local);
    d.setUTCDate(local.getUTCDate()+i);
    dates.push(fmt(d));
  }
  return[...new Set(dates)];
};

app.get("/api/*",async(req,res)=>{
  try{
    const path=req.path.replace("/api","");
    const qs=new URLSearchParams({...req.query,key:KEY}).toString();
    const data=await ftch(BASE+path+"?"+qs);
    res.json(data);
  }catch(e){res.status(500).json({error:e.message})}
});

app.get("/fetch-all-data",async(req,res)=>{
  const LEAGUE_IDS=[
    16504,15000,14968,14924,15050,14930,14956,16558,14932,15068,
    14931,14923,16036,16544,16571,15047,16242,15234,16614,16615,
    15055,16714,16708,15002,15238,14904,12980,16823,16046,16037,
    16808,16563,16562,13861,9128,10117,10121,12061,11426,
    7977,12801,16494,15020,8994,6704
  ];
  const sleep=ms=>new Promise(r=>setTimeout(r,ms));
  const all=[];
  for(const sid of LEAGUE_IDS){
    try{
      const url=BASE+"/league-matches?season_id="+sid+"&max_per_page=300&page=1&key="+KEY;
      const data=await ftch(url);
      const matches=(data.data||[])
        .filter(m=>m.status==="complete")
        .filter(m=>new Date((m.date_unix||0)*1000)>=new Date("2025-09-01"))
        .map(m=>({
          home_name:m.home_name,away_name:m.away_name,
          ht_goals_team_a:m.ht_goals_team_a,ht_goals_team_b:m.ht_goals_team_b,
          homeGoalCount:m.homeGoalCount,awayGoalCount:m.awayGoalCount,
          homeGoals_timings:m.homeGoals_timings,awayGoals_timings:m.awayGoals_timings,
          date_unix:m.date_unix,competition_id:sid
        }));
      all.push(...matches);
      console.log(sid+": "+matches.length+" matches");
    }catch(e){console.log(sid+": error "+e.message);}
    await sleep(300);
  }
  res.setHeader("Content-Disposition","attachment; filename=all_leagues.json");
  res.json({data:all});
});

app.get("/",async(req,res)=>{
  try{
    const tzOffset=parseInt(req.query.tz||"0");
    const dates=getDates(tzOffset);
    const dayResults=await Promise.all(dates.map(d=>ftchCached(BASE+"/todays-matches?date="+d+"&key="+KEY,"today:"+d)));
    const allFixtures=[];
    for(let i=0;i<dates.length;i++){
      for(const m of (dayResults[i].data||[])){allFixtures.push(Object.assign({},m,{_date:dates[i]}));}
    }
    const leagueFixtures={};
    for(const m of allFixtures){
      const sid=parseInt(m.competition_id);
      if(LEAGUE_NAMES[sid]){
        if(!leagueFixtures[sid])leagueFixtures[sid]=[];
        leagueFixtures[sid].push(m);
      }
    }
    const activeLeagues=Object.keys(leagueFixtures);
    const preds=[];


    for(const sid of activeLeagues){
      const fixtures=leagueFixtures[sid]||[];
      if(!fixtures.length) continue;

      // Fetch per-team stats from /league-teams (includes FH stats)
      let teamMap={};
      try{
        const teamsData=await ftchCached(BASE+"/league-teams?season_id="+sid+"&include=stats&key="+KEY,"teams:"+sid);
        for(const t of (teamsData.data||[])){
          // scored/conceded FH averages
          const mp=t.stats?t.stats.matches_played||t.stats.MP||1:1;
          // scoredAVGHT and concededAVGHT come directly from API
          const scored_fh=safe(parseFloat(t.stats&&(t.stats.scoredAVGHT_overall!=null?t.stats.scoredAVGHT_overall:t.stats.scoredAVGHT)||0));
          const conced_fh=safe(parseFloat(t.stats&&(t.stats.concededAVGHT_overall!=null?t.stats.concededAVGHT_overall:t.stats.concededAVGHT)||0));
          // seasonOver25PercentageHT_overall: % of games with FH > 2.5 goals (team-level)
          const o25ht_pct=safe(parseFloat(t.stats&&(t.stats.seasonOver25PercentageHT_overall!=null?t.stats.seasonOver25PercentageHT_overall:0)||0));
          // BTTS HT %
          const btts_ht_pct=safe(parseFloat(t.stats&&(t.stats.btts_ht_percentage_overall!=null?t.stats.btts_ht_percentage_overall:t.stats.bttsPercentageHT_overall)||0));
          // FH clean sheet %
          const cs_ht_pct=safe(parseFloat(t.stats&&(t.stats.clean_sheet_half_time_percentage_overall!=null?t.stats.clean_sheet_half_time_percentage_overall:t.stats.csPercentageHT_overall)||0));
          // Early goals (0-10 min) per game
          const g010=safe(parseFloat(t.stats&&(t.stats.goals_conceded_min_0_to_10!=null?t.stats.goals_conceded_min_0_to_10:0)||0));
          const mpNum=safe(parseFloat(t.stats&&(t.stats.matches_played_overall!=null?t.stats.matches_played_overall:mp)||1));
          const cn010_avg=mpNum>0?safe(g010/mpNum):0;
          teamMap[t.name]={scored_fh,conced_fh,o25ht_pct,btts_ht_pct,cs_ht_pct,cn010_avg};
        }
      }catch(e){console.log("league-teams error sid="+sid+": "+e.message);}

      // Also build h2h from completed matches (keep for display)
      const h2hMap={};
      let totalFH=0,totalGames=0;
      try{
        const mapM=m=>({home_name:m.home_name,away_name:m.away_name,
          ht_goals_team_a:m.ht_goals_team_a,ht_goals_team_b:m.ht_goals_team_b,
          homeGoalCount:m.homeGoalCount,awayGoalCount:m.awayGoalCount,
          date_unix:m.date_unix});
        const p1=await ftchCached(BASE+"/league-matches?season_id="+sid+"&max_per_page=150&page=1&key="+KEY,"matches:"+sid);
        const completed=(p1.data||[]).filter(m=>m.status==="complete").map(mapM);
        for(const m of completed){
          const ha=parseInt(m.ht_goals_team_a||0),hb=parseInt(m.ht_goals_team_b||0);
          const fa=parseInt(m.homeGoalCount||0),fb=parseInt(m.awayGoalCount||0);
          totalFH+=ha+hb; totalGames++;
          const k=[m.home_name,m.away_name].sort().join("|");
          if(!h2hMap[k])h2hMap[k]=[];
          h2hMap[k].push({home:m.home_name,away:m.away_name,htH:ha,htA:hb,ftH:fa,ftA:fb});
        }
      }catch(e){}

      const leagueHalfAvg=totalGames>0?Math.max((totalFH/totalGames)/2,0.5):0.5;

      for(const fixture of fixtures){
        const h=fixture.home_name,a=fixture.away_name;
        const hT=teamMap[h]||null;
        const aT=teamMap[a]||null;
        if(!hT||!aT) continue;

        // --- STRONG SIGNALS ---
        // S1: Expected FH Goals >= 1.20
        const expFH=safe((hT.scored_fh*aT.conced_fh)+(aT.scored_fh*hT.conced_fh));
        const S1=expFH>=1.20;

        // S2: FH Over 1.5 odds <= 1.80 (from fixture data if available, else skip)
        const odds_fh_o15=safe(parseFloat(fixture.odds_ft_over15||fixture.o15_ht||0));
        const hasOdds=odds_fh_o15>1.0;
        const S2=hasOdds&&odds_fh_o15<=1.80;

        // S3: Both teams BTTS FH >= 25%
        const S3=hT.btts_ht_pct>=25&&aT.btts_ht_pct>=25;

        // --- MEDIUM SIGNALS ---
        // M1: FH Over 1.5 odds <= 2.00
        const M1=hasOdds&&odds_fh_o15<=2.00;

        // M2: Either team FH clean sheet <= 30%
        const M2=hT.cs_ht_pct<=30||aT.cs_ht_pct<=30;

        // M3: Either team FH Over 2.5 >= 20%
        const M3=hT.o25ht_pct>=20||aT.o25ht_pct>=20;

        // M4: Early goals (0-10 min) >= 0.25 per game (either team)
        const M4=hT.cn010_avg>=0.25||aT.cn010_avg>=0.25;

        const strongMet=[S1,S2,S3].filter(Boolean).length;
        const mediumMet=[M1,M2,M3,M4].filter(Boolean).length;
        const {prob,label:probLabel}=calcProb(strongMet,mediumMet,odds_fh_o15);

        const h2hKey=[h,a].sort().join("|");
        const h2h=(h2hMap[h2hKey]||[]).slice(0,6);

        // Build signals array for display (strong first, then medium)
        const signals=[
          // STRONG
          {label:"S1: Exp FH Goals",desc:"(H scored × A conceded) + (A scored × H conceded)",
           hVal:hT.scored_fh.toFixed(2)+" sc / "+hT.conced_fh.toFixed(2)+" cn",
           aVal:aT.scored_fh.toFixed(2)+" sc / "+aT.conced_fh.toFixed(2)+" cn",
           combinedVal:expFH.toFixed(3),threshold:">= 1.20",met:S1,tier:"STRONG"},
          {label:"S2: FH Over 1.5 Odds",desc:"Market pricing strong FH activity",
           hVal:"—",aVal:"—",
           combinedVal:hasOdds?odds_fh_o15.toFixed(2):"n/a",threshold:"<= 1.80",met:S2,tier:"STRONG"},
          {label:"S3: BTTS First Half",desc:"Both teams BTTS FH % (both must be >= 25%)",
           hVal:hT.btts_ht_pct.toFixed(0)+"%",aVal:aT.btts_ht_pct.toFixed(0)+"%",
           combinedVal:hT.btts_ht_pct.toFixed(0)+"% / "+aT.btts_ht_pct.toFixed(0)+"%",threshold:"both >= 25%",met:S3,tier:"STRONG"},
          // MEDIUM
          {label:"M1: FH Over 1.5 Odds",desc:"Market expects active first half",
           hVal:"—",aVal:"—",
           combinedVal:hasOdds?odds_fh_o15.toFixed(2):"n/a",threshold:"<= 2.00",met:M1,tier:"MEDIUM"},
          {label:"M2: FH Clean Sheet",desc:"Either team FH clean sheet % <= 30% (leaky defence)",
           hVal:hT.cs_ht_pct.toFixed(0)+"%",aVal:aT.cs_ht_pct.toFixed(0)+"%",
           combinedVal:Math.min(hT.cs_ht_pct,aT.cs_ht_pct).toFixed(0)+"%",threshold:"either <= 30%",met:M2,tier:"MEDIUM"},
          {label:"M3: FH Over 2.5 %",desc:"Either team has >= 20% of games with FH > 2.5",
           hVal:hT.o25ht_pct.toFixed(0)+"%",aVal:aT.o25ht_pct.toFixed(0)+"%",
           combinedVal:Math.max(hT.o25ht_pct,aT.o25ht_pct).toFixed(0)+"%",threshold:"either >= 20%",met:M3,tier:"MEDIUM"},
          {label:"M4: Early Goals 0-10",desc:"Either team concedes >= 0.25 goals/game in first 10 min",
           hVal:hT.cn010_avg.toFixed(2),aVal:aT.cn010_avg.toFixed(2),
           combinedVal:Math.max(hT.cn010_avg,aT.cn010_avg).toFixed(2),threshold:"either >= 0.25",met:M4,tier:"MEDIUM"}
        ];

        preds.push({
          league:LEAGUE_NAMES[sid],leagueSid:parseInt(sid),
          dt:(fixture.date_unix||0)*1000,matchDate:fixture._date,
          home:h,away:a,
          expFH:+expFH.toFixed(3),leagueAvg:+leagueHalfAvg.toFixed(2),
          prob,probLabel,strongMet,mediumMet,
          signals,h2h:h2h.slice(0,6),
          smallSample:false,
          status:fixture.status||"incomplete",
          fhH:parseInt(fixture.ht_goals_team_a||0),
          fhA:parseInt(fixture.ht_goals_team_b||0),
          ftH:parseInt(fixture.homeGoalCount||0),
          ftA:parseInt(fixture.awayGoalCount||0)
        });
      }
    }

    preds.sort((a,b)=>b.prob-a.prob||b.strongMet-a.strongMet||b.mediumMet-a.mediumMet);
    res.send(buildHTML(preds,dates));
  }catch(e){console.error(e);res.status(500).send("<pre>Error: "+e.message+"</pre>");}
});

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
  H+="html,body{height:100%}";
  H+="body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;font-size:15px;display:flex;flex-direction:column;min-height:100vh}";
  H+="details>summary::-webkit-details-marker{display:none}";
  H+=".tab{padding:7px 12px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600;border:1px solid #e5e7eb;background:#fff;color:#6b7280;transition:all .15s;white-space:nowrap}";
  H+=".tab.active{background:#111827;color:#fff;border-color:#111827}";
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
  H+=".chip{display:inline-block;padding:3px 8px;font-size:12px;border-radius:3px;margin:2px}";
  // Sidebar league item styles
  H+=".sidebar-league{display:flex;align-items:center;justify-content:space-between;padding:9px 12px;border-radius:6px;cursor:pointer;transition:background .12s;gap:8px;border:1px solid transparent}";
  H+=".sidebar-league:hover{background:#f3f4f6}";
  H+=".sidebar-league.active{background:#111827;border-color:#111827}";
  H+=".sidebar-league.active .slg-name{color:#fff}";
  H+=".sidebar-league.active .slg-sub{color:#9ca3af}";
  H+=".sidebar-league.active .slg-prob{color:#fff}";
  H+=".slg-name{font-size:13px;font-weight:600;color:#111827;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}";
  H+=".slg-sub{font-size:11px;color:#9ca3af;margin-top:1px;white-space:nowrap}";
  H+=".slg-prob{font-size:14px;font-weight:800;flex-shrink:0}";
  // Layout
  H+="#appShell{display:flex;flex:1;min-height:0}";
  H+="#sidebar{width:220px;flex-shrink:0;background:#fff;border-right:1px solid #e5e7eb;display:flex;flex-direction:column;position:sticky;top:57px;height:calc(100vh - 57px);overflow-y:auto}";
  H+="#mainPane{flex:1;min-width:0;overflow-y:auto;padding:16px 20px}";
  // Mobile: sidebar collapses
  H+="@media(max-width:640px){#appShell{flex-direction:column}#sidebar{width:100%;height:auto;position:static;border-right:none;border-bottom:1px solid #e5e7eb}}";
  H+="</style></head><body>";

  // Top header bar (sticky)
  H+="<div id=\"header\" style=\"background:#fff;border-bottom:1px solid #e5e7eb;padding:10px 16px;position:sticky;top:0;z-index:20;height:57px\">";
  H+="<div style=\"display:flex;align-items:center;justify-content:space-between;gap:16px\">";
  H+="<div style=\"display:flex;align-items:center;gap:16px;min-width:0\">";
  H+="<div style=\"flex-shrink:0\">";
  H+="<div style=\"font-size:10px;color:#6b7280;letter-spacing:1px;text-transform:uppercase\">&#9917; First Half Score</div>";
  H+="<h1 style=\"font-size:17px;font-weight:800;color:#111827;white-space:nowrap\" id=\"headerTitle\">Loading...</h1>";
  H+="</div>";
  H+="<div id=\"dayTabs\" style=\"display:flex;gap:6px;flex-wrap:nowrap;overflow-x:auto\"></div>";
  H+="</div>";
  H+="<button onclick=\"location.reload()\" style=\"background:#111827;color:#fff;padding:7px 14px;font-size:13px;border:none;border-radius:6px;font-weight:600;cursor:pointer;flex-shrink:0\">&#8635; Refresh</button>";
  H+="</div></div>";

  // App shell: sidebar + main
  H+="<div id=\"appShell\">";

  // LEFT SIDEBAR
  H+="<div id=\"sidebar\">";
  H+="<div style=\"padding:10px 12px 6px;font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:.8px\">Leagues</div>";
  H+="<div id=\"sidebarLeagues\" style=\"padding:0 8px 12px\"></div>";
  H+="</div>";

  // MAIN PANE
  H+="<div id=\"mainPane\">";
  H+="<div style=\"background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:12px;color:#92400e;line-height:1.6\">";
  H+="<strong>How it works:</strong> 3 strong signals + 4 medium signals from /league-teams API stats. ";
  H+="3 strong = 48% &nbsp;|&nbsp; 2 strong + odds &le;2.00 = 42% &nbsp;|&nbsp; 1-2 strong + 2 medium = 35% &nbsp;|&nbsp; 2+ medium = 25% &nbsp;|&nbsp; base = 13%.";
  H+="</div>";
  H+="<div id=\"mainView\"></div>";
  H+="</div>";
  H+="</div>";// end appShell

  H+="<script>";
  H+="var ALL_PREDS="+predsJSON+";";
  H+="var DATES="+datesJSON+";";
  H+="var DAY_LABELS=[\"Today\",\"Tomorrow\",\"Day 3\",\"Day 4\",\"Day 5\"];";
  H+="var activeDate=DATES[0];";
  H+="var activeLeague=null;";

  H+="function localDateStr(ts){var d=new Date(ts);return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');}";
  H+="ALL_PREDS.forEach(function(p){p.matchDate=localDateStr(p.dt);});";
  H+="function fmt(d){return new Date(d).toLocaleDateString('en-GB',{weekday:'long',day:'2-digit',month:'short'});}";

  // Render day tabs
  H+="function renderTabs(){";
  H+="  var el=document.getElementById('dayTabs');";
  H+="  var html='';";
  H+="  for(var i=0;i<DATES.length;i++){";
  H+="    var d=DATES[i];";
  H+="    var count=ALL_PREDS.filter(function(p){return p.matchDate===d;}).length;";
  H+="    var cls=d===activeDate?'tab active':'tab';";
  H+="    var lbl=DAY_LABELS[i]||d;";
  H+="    html+='<button class=\"'+cls+'\" onclick=\"selectDay('+i+')\">'+lbl+' <span style=\"font-size:11px;opacity:.7\">('+count+')</span></button>';";
  H+="  }";
  H+="  el.innerHTML=html;";
  H+="}";

  // Build sorted league list for active day
  H+="function getLeagueList(){";
  H+="  var dayPreds=ALL_PREDS.filter(function(p){return p.matchDate===activeDate;});";
  H+="  var leagueMap={};";
  H+="  for(var i=0;i<dayPreds.length;i++){var p=dayPreds[i];if(!leagueMap[p.league])leagueMap[p.league]=[];leagueMap[p.league].push(p);}";
  H+="  return Object.entries(leagueMap).sort(function(a,b){return Math.max.apply(null,b[1].map(function(p){return p.prob;}))-Math.max.apply(null,a[1].map(function(p){return p.prob;}));});";
  H+="}";

  // Render sidebar league list
  H+="function renderSidebar(){";
  H+="  var leagueList=getLeagueList();";
  H+="  var html='';";
  H+="  for(var j=0;j<leagueList.length;j++){";
  H+="    var league=leagueList[j][0];";
  H+="    var matches=leagueList[j][1];";
  H+="    var maxProb=Math.max.apply(null,matches.map(function(p){return p.prob;}));";
  H+="    var maxN=Math.max.apply(null,matches.map(function(p){return p.strongMet;}));";
  H+="    var probCol=maxProb>=42?'#dc2626':maxProb>=35?'#ea580c':maxProb>=25?'#ca8a04':'#9ca3af';";
  H+="    var isActive=league===activeLeague;";
  H+="    var cls=isActive?'sidebar-league active':'sidebar-league';";
  H+="    var safeLeague=league.replace(/\\\\/g,'\\\\\\\\').replace(/'/g,\"\\\\'\");";
  H+="    html+='<div class=\"'+cls+'\" onclick=\"selectLeague(\\''+safeLeague+'\\')\">'+";
  H+="      '<div style=\"min-width:0\">'+";
  H+="        '<div class=\"slg-name\">'+league+'</div>'+";
  H+="        '<div class=\"slg-sub\">'+matches.length+' match'+(matches.length>1?'es':'')+' &middot; '+maxN+'/3 strong</div>'+";
  H+="      '</div>'+";
  H+="      '<div class=\"slg-prob\" style=\"color:'+(isActive?'#fff':probCol)+'\">'+maxProb+'%</div>'+";
  H+="    '</div>';";
  H+="  }";
  H+="  document.getElementById('sidebarLeagues').innerHTML=html||'<div style=\"padding:8px 4px;font-size:12px;color:#9ca3af\">No matches</div>';";
  H+="}";

  // Select day
  H+="function selectDay(i){";
  H+="  activeDate=DATES[i];";
  H+="  var leagues=getLeagueList();";
  H+="  activeLeague=leagues.length?leagues[0][0]:null;";
  H+="  renderTabs();";
  H+="  renderSidebar();";
  H+="  renderMatchList();";
  H+="  document.getElementById('headerTitle').textContent=fmt(new Date(DATES[i]+'T12:00:00'));";
  H+="}";

  // Select league from sidebar
  H+="function selectLeague(league){";
  H+="  activeLeague=league;";
  H+="  renderSidebar();";
  H+="  renderMatchList();";
  // Scroll main pane to top
  H+="  var mp=document.getElementById('mainPane');if(mp)mp.scrollTop=0;";
  H+="}";

  // Render match list in main pane
  H+="function renderMatchList(){";
  H+="  var el=document.getElementById('mainView');";
  H+="  if(!activeLeague){el.innerHTML='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280\">Select a league</div>';return;}";
  H+="  var matches=ALL_PREDS.filter(function(p){return p.matchDate===activeDate&&p.league===activeLeague;}).sort(function(a,b){return b.prob-a.prob;});";
  H+="  var html='<div style=\"font-size:17px;font-weight:700;color:#111827;margin-bottom:14px\">'+activeLeague+'</div>';";
  H+="  if(!matches.length){html+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280\">No matches</div>';}";
  H+="  for(var i=0;i<matches.length;i++)html+=renderMatchCard(matches[i]);";
  H+="  el.innerHTML=html;";
  H+="}";

  H+="function shortName(n){return n.split(' ').slice(0,2).join(' ');}";

  // renderMatchCard — identical logic, no changes needed
  H+="function renderMatchCard(m){";
  H+="  var probCol=m.prob>=42?'#dc2626':m.prob>=35?'#ea580c':m.prob>=25?'#ca8a04':'#9ca3af';";
  H+="  var probBg=m.prob>=42?'#fef2f2':m.prob>=35?'#fff7ed':m.prob>=25?'#fffbeb':'#f9fafb';";
  H+="  var probBorder=m.prob>=42?'#fca5a5':m.prob>=35?'#fed7aa':m.prob>=25?'#fde68a':'#e5e7eb';";
  H+="  var probLabel=m.prob>=35?'&#128293; HIGH':m.prob>=20?'&#9889; MED':'&#10052; LOW';";
  H+="  var dt=new Date(m.dt).toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'});";
  H+="  var dots='';";
  H+="  for(var i=0;i<m.signals.length;i++){";
  H+="    var sc=m.signals[i].met?'#16a34a':'#e5e7eb';";
  H+="    dots+='<span title=\"'+m.signals[i].label+'\" style=\"display:inline-block;width:12px;height:12px;border-radius:50%;background:'+sc+';margin-right:3px\"></span>';";
  H+="  }";
  H+="  var sigRows='';";
  H+="  for(var i=0;i<m.signals.length;i++){";
  H+="    var s=m.signals[i];";
  H+="    var valCol=s.met?'#15803d':'#dc2626';";
  H+="    var pillCls=s.met?'pill pill-met':'pill pill-unmet';";
  H+="    var pillTxt=s.met?'&#10003; MET':'&#10007; MISS';";
  H+="    var rowCls=s.met?'sig-row-met':'sig-row-unmet';";
  H+="    sigRows+='<tr class=\"'+rowCls+'\"><td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6\"><div style=\"font-weight:600;color:#111827;margin-bottom:2px\">'+s.label+'</div><div style=\"font-size:11px;color:#9ca3af\">'+s.desc+'</div></td>';";
  H+="    sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><div style=\"font-weight:700;color:#374151;font-size:13px\">'+s.hVal+'</div></td>';";
  H+="    sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><div style=\"font-weight:700;color:#374151;font-size:13px\">'+s.aVal+'</div></td>';";
  H+="    sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><div style=\"font-weight:800;font-size:14px;color:'+valCol+'\">'+s.combinedVal+'</div><div style=\"font-size:10px;color:#9ca3af\">'+s.threshold+'</div></td>';";
  H+="    sigRows+='<td style=\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\"><span class=\"'+pillCls+'\">'+pillTxt+'</span><div style=\"font-size:10px;color:#9ca3af;margin-top:3px\">'+s.tier+'</div></td></tr>';";
  H+="  }";
  H+="  var h2hRows='';var h2hHits=0;var h2hArr=m.h2h||[];";
  H+="  for(var i=0;i<h2hArr.length;i++){";
  H+="    var g=h2hArr[i];var ht=g.htH+g.htA;if(ht>=2)h2hHits++;";
  H+="    var htCol=ht>=2?'#16a34a':'#374151';";
  H+="    h2hRows+='<tr style=\"border-bottom:1px solid #f3f4f6\"><td style=\"padding:5px 8px;font-size:12px;color:#6b7280\">'+g.home+' vs '+g.away+'</td><td style=\"padding:5px 8px;font-size:13px;text-align:center;font-weight:700;color:'+htCol+'\">HT '+g.htH+'-'+g.htA+'</td><td style=\"padding:5px 8px;font-size:12px;text-align:center;color:#6b7280\">FT '+g.ftH+'-'+g.ftA+'</td></tr>';";
  H+="  }";
  H+="  var html='<div class=\"match-card\" style=\"border-left:4px solid '+probCol+'\">';";
  H+="  html+='<div style=\"padding:16px\">';";
  H+="  html+='<div style=\"display:grid;grid-template-columns:1fr auto;gap:10px;align-items:start;margin-bottom:12px\">';";
  H+="  html+='<div style=\"min-width:0\">';";
  H+="  html+='<div style=\"font-size:12px;color:#9ca3af;margin-bottom:3px\">'+dt+'</div>';";
  H+="  html+='<div style=\"font-size:17px;font-weight:700;color:#111827;margin-bottom:5px;line-height:1.3\">'+m.home+' <span style=\"color:#d1d5db;font-weight:400;font-size:13px\">vs</span> '+m.away+'</div>';";
  H+="  html+='<div style=\"display:flex;align-items:center;gap:8px\">'+dots+'<span style=\"font-size:12px;color:#6b7280\">'+m.strongMet+'/3 strong &middot; '+m.mediumMet+'/4 medium</span></div>';";
  H+="  html+='<div style=\"font-size:12px;color:#6b7280;margin-top:4px\">Exp FH: <strong style=\"color:#374151\">'+m.expFH+'</strong> &middot; League avg: '+m.leagueAvg+'</div>';";
  H+="  html+='</div>';";
  H+="  if(m.status==='complete'){var fhHit=(m.fhH+m.fhA)>2;var rb=fhHit?'#f0fdf4':'#fef2f2';var rbr=fhHit?'#bbf7d0':'#fecaca';var rfc=fhHit?'#16a34a':'#dc2626';";
  H+="    html+='<div style=\"text-align:center;min-width:76px;background:'+rb+';border:1px solid '+rbr+';border-radius:8px;padding:8px 6px;flex-shrink:0\">';";
  H+="    html+='<div style=\"font-size:11px;color:#9ca3af;font-weight:600\">FH</div>';";
  H+="    html+='<div style=\"font-size:24px;font-weight:800;color:'+rfc+';line-height:1.1\">'+m.fhH+'-'+m.fhA+'</div>';";
  H+="    html+='<div style=\"font-size:10px;color:#9ca3af;margin-top:5px;font-weight:600\">FT</div>';";
  H+="    html+='<div style=\"font-size:16px;font-weight:700;color:#374151;line-height:1.1\">'+m.ftH+'-'+m.ftA+'</div>';";
  H+="    html+='<div style=\"font-size:11px;color:'+probCol+';margin-top:5px;font-weight:700\">'+m.prob+'% pre</div></div>';";
  H+="  }else{";
  H+="    html+='<div style=\"text-align:center;min-width:76px;background:'+probBg+';border:1px solid '+probBorder+';border-radius:8px;padding:10px 6px;flex-shrink:0\">';";
  H+="    html+='<div style=\"font-size:30px;font-weight:800;color:'+probCol+';line-height:1\">'+m.prob+'%</div>';";
  H+="    html+='<div style=\"font-size:12px;color:'+probCol+';margin-top:2px\">'+m.probLabel+'</div>';";
  H+="    html+='<div style=\"font-size:10px;color:#9ca3af;margin-top:2px\">FH OVER 2.5</div></div>';";
  H+="  }";
  H+="  html+='</div>';";
  H+="  html+='<details><summary style=\"font-size:13px;color:#6b7280;padding:5px 0;border-top:1px solid #f3f4f6\">&#9660; Show signal detail</summary>';";
  H+="  html+='<div style=\"padding-top:10px\">';";
  H+="  html+='<table class=\"sig-table\" style=\"margin-bottom:14px\"><thead><tr>';";
  H+="  html+='<th style=\"width:30%\">Signal</th>';";
  H+="  html+='<th style=\"width:16%;text-align:center\">'+shortName(m.home)+'</th>';";
  H+="  html+='<th style=\"width:16%;text-align:center\">'+shortName(m.away)+'</th>';";
  H+="  html+='<th style=\"width:18%;text-align:center\">Combined</th>';";
  H+="  html+='<th style=\"width:20%;text-align:center\">Result</th>';";
  H+="  html+='</tr></thead><tbody>'+sigRows+'</tbody></table>';";
  H+="  if(h2hRows){html+='<div style=\"font-size:12px;font-weight:600;color:#374151;margin-bottom:5px\">H2H &mdash; '+h2hHits+'/'+h2hArr.length+' meetings with FH &ge;2 goals</div>';html+='<table style=\"width:100%;border-collapse:collapse\">'+h2hRows+'</table>';}";
  H+="  html+='</div></details></div></div>';";
  H+="  return html;";
  H+="}";

  // Init
  H+="document.getElementById('headerTitle').textContent=fmt(new Date());";
  H+="renderTabs();";
  H+="var initLeagues=getLeagueList();";
  H+="if(initLeagues.length)activeLeague=initLeagues[0][0];";
  H+="renderSidebar();";
  H+="renderMatchList();";
  H+="<\/script></body></html>";

  return H;
}

const PORT=process.env.PORT||3001;
app.listen(PORT,()=>console.log("Server running on port "+PORT));
