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

const PROB_TABLE=[5,10,20,35,45];

const ftch=url=>fetch(url).then(r=>r.json());
const avgF=(arr,fn)=>arr.length?arr.reduce((s,x)=>s+fn(x),0)/arr.length:0;
const pctF=(arr,fn)=>arr.length?arr.filter(fn).length/arr.length:0;
const pm=t=>{const m=String(t).match(/^(\d+)/);return m?parseInt(m[1]):null};
const safe=v=>isNaN(v)||!isFinite(v)?0:v;

const getDates=()=>{
  const today=new Date();
  const fmt=d=>{
    const y=d.getFullYear();
    const m=String(d.getMonth()+1).padStart(2,"0");
    const day=String(d.getDate()).padStart(2,"0");
    return y+"-"+m+"-"+day;
  };
  const dates=[];
  for(let i=0;i<4;i++){
    const d=new Date(today);
    d.setDate(today.getDate()+i);
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
    const dates=getDates();
    const dayResults=await Promise.all(dates.map(d=>ftch(BASE+"/todays-matches?date="+d+"&key="+KEY)));
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

    for(const sid of activeLeagues.slice(0,20)){
      let completed=[];
      try{
        const p1=await ftch(BASE+"/league-matches?season_id="+sid+"&max_per_page=150&page=1&key="+KEY);
        completed=(p1.data||[]).filter(m=>m.status==="complete").map(m=>({
          home_name:m.home_name,away_name:m.away_name,
          ht_goals_team_a:m.ht_goals_team_a,ht_goals_team_b:m.ht_goals_team_b,
          homeGoalCount:m.homeGoalCount,awayGoalCount:m.awayGoalCount,
          homeGoals_timings:m.homeGoals_timings,awayGoals_timings:m.awayGoals_timings,
          date_unix:m.date_unix
        }));
      }catch(e){}

      const fixtures=leagueFixtures[sid]||[];
      if(completed.length<5){continue;}

      const team={};
      const en=n=>{if(!team[n])team[n]={h:[],a:[]};};
      let totalFH=0,totalGames=0;
      const h2hMap={};

      for(const m of completed){
        const ha=parseInt(m.ht_goals_team_a||0),hb=parseInt(m.ht_goals_team_b||0);
        const fa=parseInt(m.homeGoalCount||0),fb=parseInt(m.awayGoalCount||0);
        en(m.home_name);en(m.away_name);
        totalFH+=ha+hb;totalGames++;
        const hts=(m.homeGoals_timings||[]).map(pm).filter(t=>t!==null);
        const ats=(m.awayGoals_timings||[]).map(pm).filter(t=>t!==null);
        const fhTot=ha+hb;
        const shTot=(fa-ha)+(fb-hb);
        const rec=(fh_sc,fh_cn)=>({
          fh_sc,fh_cn,
          fh_btts:ha>0&&hb>0,
          fh_most:fhTot>shTot,
          fh_total:ha+hb,ft_total:fa+fb,
          date:m.date_unix||0
        });
        team[m.home_name].h.push(rec(ha,hb));
        team[m.away_name].a.push(rec(hb,ha));
        const k=[m.home_name,m.away_name].sort().join("|");
        if(!h2hMap[k])h2hMap[k]=[];
        h2hMap[k].push({home:m.home_name,away:m.away_name,htH:ha,htA:hb,ftH:fa,ftA:fb});
      }

      for(const t of Object.values(team)){
        t.h.sort((a,b)=>b.date-a.date);
        t.a.sort((a,b)=>b.date-a.date);
      }

      const leagueHalfAvg=totalGames>0?Math.max((totalFH/totalGames)/2,0.5):0.5;

      for(const fixture of fixtures){
        const h=fixture.home_name,a=fixture.away_name;
        const ht=team[h]||{h:[],a:[]};
        const at=team[a]||{h:[],a:[]};
        const hGames=ht.h.length>=3?ht.h.slice(0,12):([].concat(ht.h,ht.a).sort((x,y)=>y.date-x.date).slice(0,12));
        const aGames=at.a.length>=3?at.a.slice(0,12):([].concat(at.h,at.a).sort((x,y)=>y.date-x.date).slice(0,12));
        if(hGames.length<2||aGames.length<2)continue;

        const hFHSc=safe(avgF(hGames,g=>g.fh_sc));
        const hFHCn=safe(avgF(hGames,g=>g.fh_cn));
        const aFHSc=safe(avgF(aGames,g=>g.fh_sc));
        const aFHCn=safe(avgF(aGames,g=>g.fh_cn));
        const expFH=safe((hFHSc*aFHCn)+(aFHSc*hFHCn));
        const s1=expFH>=1.083;

        const hBtts=safe(pctF(hGames,g=>g.fh_btts));
        const aBtts=safe(pctF(aGames,g=>g.fh_btts));
        const bttsAvg=(hBtts+aBtts)/2;
        const s2=bttsAvg>=0.261;

        const fhCnSum=hFHCn+aFHCn;
        const s3=fhCnSum>=1.542;

        const hFHMost=safe(pctF(hGames,g=>g.fh_most));
        const aFHMost=safe(pctF(aGames,g=>g.fh_most));
        const fhMostAvg=(hFHMost+aFHMost)/2;
        const s4=fhMostAvg>=0.394;

        const nMet=+s1+ +s2+ +s3+ +s4;
        const prob=PROB_TABLE[nMet];

        const h2hKey=[h,a].sort().join("|");
        const h2h=(h2hMap[h2hKey]||[]).slice(0,6);

        const signals=[
          {label:"Exp FH goals",desc:"(H scored x A conceded) + (A scored x H conceded)",
           hVal:hFHSc.toFixed(2)+" sc / "+hFHCn.toFixed(2)+" cn",
           aVal:aFHSc.toFixed(2)+" sc / "+aFHCn.toFixed(2)+" cn",
           combinedVal:expFH.toFixed(3),threshold:">= 1.083",met:s1,lift:"2.29x lift"},
          {label:"BTTS First Half",desc:"Both teams score in 1H - avg of both teams",
           hVal:(hBtts*100).toFixed(0)+"%",aVal:(aBtts*100).toFixed(0)+"%",
           combinedVal:(bttsAvg*100).toFixed(0)+"%",threshold:"avg >= 26%",met:s2,lift:"2.00x lift"},
          {label:"FH Conceded Sum",desc:"Total FH goals conceded per game - both teams combined",
           hVal:hFHCn.toFixed(2)+"/g",aVal:aFHCn.toFixed(2)+"/g",
           combinedVal:fhCnSum.toFixed(3),threshold:"sum >= 1.542",met:s3,lift:"1.96x lift"},
          {label:"FH = Best Half",desc:"% of games where 1st half has more goals than 2nd half",
           hVal:(hFHMost*100).toFixed(0)+"%",aVal:(aFHMost*100).toFixed(0)+"%",
           combinedVal:(fhMostAvg*100).toFixed(0)+"%",threshold:"avg >= 39%",met:s4,lift:"1.77x lift"}
        ];

        preds.push({
          league:LEAGUE_NAMES[sid],leagueSid:parseInt(sid),
          dt:(fixture.date_unix||0)*1000,matchDate:fixture._date,
          home:h,away:a,
          expFH:+expFH.toFixed(3),leagueAvg:+leagueHalfAvg.toFixed(2),
          prob,nMet,signals,h2h:h2h.slice(0,6),
          hChips:[].concat(ht.h,ht.a).sort((x,y)=>y.date-x.date).slice(0,6).map(g=>({fhTotal:g.fh_total,ftTotal:g.ft_total})),
          aChips:[].concat(at.h,at.a).sort((x,y)=>y.date-x.date).slice(0,6).map(g=>({fhTotal:g.fh_total,ftTotal:g.ft_total})),
          smallSample:hGames.length<6||aGames.length<6
        });
      }
    }

    preds.sort((a,b)=>b.prob-a.prob||b.nMet-a.nMet);
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
  H+=".chip{display:inline-block;padding:3px 8px;font-size:12px;border-radius:3px;margin:2px}";
  H+="</style></head><body>";
  H+="<div id=\"header\" style=\"background:#fff;border-bottom:1px solid #e5e7eb;padding:14px 20px;position:sticky;top:0;z-index:10\">";
  H+="<div style=\"max-width:780px;margin:0 auto\">";
  H+="<div style=\"display:flex;align-items:center;justify-content:space-between;margin-bottom:12px\">";
  H+="<div>";
  H+="<div style=\"font-size:11px;color:#6b7280;letter-spacing:1px;text-transform:uppercase\">&#9917; First Half Score</div>";
  H+="<h1 style=\"font-size:22px;font-weight:800;color:#111827\" id=\"headerTitle\">Loading...</h1>";
  H+="</div>";
  H+="<button onclick=\"location.reload()\" style=\"background:#111827;color:#fff;padding:8px 16px;font-size:14px;border:none;border-radius:6px;font-weight:600;cursor:pointer\">&#8635; Refresh</button>";
  H+="</div>";
  H+="<div id=\"dayTabs\" style=\"display:flex;gap:8px;flex-wrap:wrap\"></div>";
  H+="</div></div>";
  H+="<div style=\"padding:16px 20px;max-width:780px;margin:0 auto\">";
  H+="<div style=\"background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#92400e;line-height:1.6\">";
  H+="<strong>How it works:</strong> 4 data-driven signals, each backed by 3,460 matches. ";
  H+="0 signals = 5% chance &nbsp;|&nbsp; 2 signals = 20% &nbsp;|&nbsp; 3 signals = 35% &nbsp;|&nbsp; 4 signals = 45% ";
  H+="&nbsp;&#8212; Even at 4/4, 57% of matches do NOT hit FH&gt;2.5 (but 66% of those end FH=2, one goal short).";
  H+="</div>";
  H+="<div id=\"mainView\"></div>";
  H+="</div>";
  H+="<script>";
  H+="var ALL_PREDS="+predsJSON+";";
  H+="var DATES="+datesJSON+";";
  H+="var DAY_LABELS=[\"Today\",\"Tomorrow\",\"Day 3\",\"Day 4\",\"Day 5\"];";
  H+="var activeDate=DATES[0];";
  H+="var activeLeague=null;";
  H+="function fmt(d){return new Date(d).toLocaleDateString(\"en-GB\",{weekday:\"long\",day:\"2-digit\",month:\"short\"});}";
  H+="function renderTabs(){";
  H+="  var el=document.getElementById(\"dayTabs\");";
  H+="  var html=\"\";";
  H+="  for(var i=0;i<DATES.length;i++){";
  H+="    var d=DATES[i];";
  H+="    var count=ALL_PREDS.filter(function(p){return p.matchDate===d;}).length;";
  H+="    var cls=d===activeDate?\"tab active\":\"tab\";";
  H+="    var lbl=DAY_LABELS[i]||d;";
  H+="    html+=\"<button class=\\\"\"+cls+\"\\\" onclick=\\\"selectDay('\"+d+\"')\\\">\"+lbl+\" <span style=\\\"font-size:12px;opacity:.7\\\">(\"+ count +\")</span></button>\";";
  H+="  }";
  H+="  el.innerHTML=html;";
  H+="}";
  H+="function selectDay(d){";
  H+="  activeDate=d;";
  H+="  activeLeague=null;";
  H+="  renderTabs();";
  H+="  renderLeagueList();";
  H+="  document.getElementById(\"headerTitle\").textContent=fmt(new Date(d+\"T12:00:00\"));";
  H+="}";
  H+="function renderLeagueList(){";
  H+="  var dayPreds=ALL_PREDS.filter(function(p){return p.matchDate===activeDate;});";
  H+="  var leagueMap={};";
  H+="  for(var i=0;i<dayPreds.length;i++){";
  H+="    var p=dayPreds[i];";
  H+="    if(!leagueMap[p.league])leagueMap[p.league]=[];";
  H+="    leagueMap[p.league].push(p);";
  H+="  }";
  H+="  var leagueList=Object.entries(leagueMap).sort(function(a,b){";
  H+="    return Math.max.apply(null,b[1].map(function(p){return p.prob;}))-Math.max.apply(null,a[1].map(function(p){return p.prob;}));";
  H+="  });";
  H+="  if(!leagueList.length){";
  H+="    document.getElementById(\"mainView\").innerHTML=\"<div style=\\\"background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280\\\">No matches found for this day.</div>\";";
  H+="    return;";
  H+="  }";
  H+="  var html=\"<div style=\\\"font-size:13px;color:#6b7280;margin-bottom:12px\\\">\"+dayPreds.length+\" matches across \"+leagueList.length+\" leagues &middot; sorted by probability</div>\";";
  H+="  for(var j=0;j<leagueList.length;j++){";
  H+="    var league=leagueList[j][0];";
  H+="    var matches=leagueList[j][1];";
  H+="    var maxProb=Math.max.apply(null,matches.map(function(p){return p.prob;}));";
  H+="    var maxN=Math.max.apply(null,matches.map(function(p){return p.nMet;}));";
  H+="    var probCol=maxProb>=35?\"#16a34a\":maxProb>=20?\"#d97706\":\"#6b7280\";";
  H+="    var hotCount=matches.filter(function(p){return p.nMet>=3;}).length;";
  H+="    var hotStr=hotCount>0?\" &middot; <span style=\\\"color:#15803d;font-weight:600\\\">\"+hotCount+\" with 3+ signals</span>\":\"\";";
  H+="    var safeLeague=league.replace(/\\\\/g,\"\\\\\\\\\").replace(/'/g,\"\\\\'\");";
  H+="    html+=\"<div class=\\\"league-card\\\" onclick=\\\"selectLeague('\"+safeLeague+\"')\\\">\";";
  H+="    html+=\"<div style=\\\"flex:1;min-width:0;margin-right:12px\\\">\";";
  H+="    html+=\"<div style=\\\"font-size:18px;font-weight:700;color:#111827;margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis\\\">\"+league+\"</div>\";";
  H+="    html+=\"<div style=\\\"font-size:13px;color:#6b7280\\\">\"+matches.length+\" match\"+(matches.length>1?\"es\":\"\")+hotStr+\"</div>\";";
  H+="    html+=\"</div>\";";
  H+="    html+=\"<div style=\\\"text-align:right;flex-shrink:0\\\">\";";
  H+="    html+=\"<div style=\\\"font-size:26px;font-weight:800;color:\"+probCol+\"\\\">\"+maxProb+\"%</div>\";";
  H+="    html+=\"<div style=\\\"font-size:11px;color:#9ca3af;margin-top:1px\\\">\"+maxN+\"/4 signals</div>\";";
  H+="    html+=\"</div></div>\";";
  H+="  }";
  H+="  document.getElementById(\"mainView\").innerHTML=html;";
  H+="}";
  H+="function selectLeague(league){activeLeague=league;renderMatchList();}";
  H+="function backToLeagues(){activeLeague=null;renderLeagueList();}";
  H+="function renderMatchList(){";
  H+="  var matches=ALL_PREDS.filter(function(p){return p.matchDate===activeDate&&p.league===activeLeague;}).sort(function(a,b){return b.prob-a.prob;});";
  H+="  var html=\"<div style=\\\"display:flex;align-items:center;gap:12px;margin-bottom:16px\\\">\";";
  H+="  html+=\"<button class=\\\"back-btn\\\" onclick=\\\"backToLeagues()\\\">&#8592; Back</button>\";";
  H+="  html+=\"<div style=\\\"font-size:19px;font-weight:700;color:#111827\\\">\"+activeLeague+\"</div></div>\";";
  H+="  for(var i=0;i<matches.length;i++)html+=renderMatchCard(matches[i]);";
  H+="  document.getElementById(\"mainView\").innerHTML=html;";
  H+="}";
  H+="function shortName(n){return n.split(\" \").slice(0,2).join(\" \");}";
  H+="function renderMatchCard(m){";
  H+="  var probCol=m.prob>=35?\"#16a34a\":m.prob>=20?\"#d97706\":\"#6b7280\";";
  H+="  var probBg=m.prob>=35?\"#f0fdf4\":m.prob>=20?\"#fffbeb\":\"#f9fafb\";";
  H+="  var probBorder=m.prob>=35?\"#bbf7d0\":m.prob>=20?\"#fde68a\":\"#e5e7eb\";";
  H+="  var probLabel=m.prob>=35?\"&#128293; HIGH\":m.prob>=20?\"&#9889; MED\":\"&#10052; LOW\";";
  H+="  var dt=new Date(m.dt).toLocaleString(\"en-GB\",{weekday:\"short\",day:\"2-digit\",month:\"short\",hour:\"2-digit\",minute:\"2-digit\"});";
  H+="  var dots=\"\";";
  H+="  for(var i=0;i<m.signals.length;i++){";
  H+="    var sc=m.signals[i].met?\"#16a34a\":\"#e5e7eb\";";
  H+="    dots+=\"<span title=\\\"\"+m.signals[i].label+\"\\\" style=\\\"display:inline-block;width:12px;height:12px;border-radius:50%;background:\"+sc+\";margin-right:3px\\\"></span>\";";
  H+="  }";
  H+="  var sigRows=\"\";";
  H+="  for(var i=0;i<m.signals.length;i++){";
  H+="    var s=m.signals[i];";
  H+="    var valCol=s.met?\"#15803d\":\"#dc2626\";";
  H+="    var pillCls=s.met?\"pill pill-met\":\"pill pill-unmet\";";
  H+="    var pillTxt=s.met?\"&#10003; MET\":\"&#10007; MISS\";";
  H+="    var rowCls=s.met?\"sig-row-met\":\"sig-row-unmet\";";
  H+="    sigRows+=\"<tr class=\\\"\"+rowCls+\"\\\">\";";
  H+="    sigRows+=\"<td style=\\\"padding:10px 8px;border-bottom:1px solid #f3f4f6\\\">\";";
  H+="    sigRows+=\"<div style=\\\"font-weight:600;color:#111827;margin-bottom:2px\\\">\"+s.label+\"</div>\";";
  H+="    sigRows+=\"<div style=\\\"font-size:11px;color:#9ca3af\\\">\"+s.desc+\"</div></td>\";";
  H+="    sigRows+=\"<td style=\\\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\\\"><div style=\\\"font-weight:700;color:#374151;font-size:13px\\\">\"+s.hVal+\"</div></td>\";";
  H+="    sigRows+=\"<td style=\\\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\\\"><div style=\\\"font-weight:700;color:#374151;font-size:13px\\\">\"+s.aVal+\"</div></td>\";";
  H+="    sigRows+=\"<td style=\\\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\\\">\";";
  H+="    sigRows+=\"<div style=\\\"font-weight:800;font-size:14px;color:\"+valCol+\"\\\">\"+s.combinedVal+\"</div>\";";
  H+="    sigRows+=\"<div style=\\\"font-size:10px;color:#9ca3af\\\">\"+s.threshold+\"</div></td>\";";
  H+="    sigRows+=\"<td style=\\\"padding:10px 8px;border-bottom:1px solid #f3f4f6;text-align:center\\\">\";";
  H+="    sigRows+=\"<span class=\\\"\"+pillCls+\"\\\">\"+pillTxt+\"</span>\";";
  H+="    sigRows+=\"<div style=\\\"font-size:10px;color:#9ca3af;margin-top:3px\\\">\"+s.lift+\"</div></td></tr>\";";
  H+="  }";
  H+="  var h2hRows=\"\";";
  H+="  var h2hHits=0;";
  H+="  var h2hArr=m.h2h||[];";
  H+="  for(var i=0;i<h2hArr.length;i++){";
  H+="    var g=h2hArr[i];";
  H+="    var ht=g.htH+g.htA;";
  H+="    if(ht>=2)h2hHits++;";
  H+="    var htCol=ht>=2?\"#16a34a\":\"#374151\";";
  H+="    h2hRows+=\"<tr style=\\\"border-bottom:1px solid #f3f4f6\\\">\";";
  H+="    h2hRows+=\"<td style=\\\"padding:5px 8px;font-size:12px;color:#6b7280\\\">\"+g.home+\" vs \"+g.away+\"</td>\";";
  H+="    h2hRows+=\"<td style=\\\"padding:5px 8px;font-size:13px;text-align:center;font-weight:700;color:\"+htCol+\"\\\">HT \"+g.htH+\"-\"+g.htA+\"</td>\";";
  H+="    h2hRows+=\"<td style=\\\"padding:5px 8px;font-size:12px;text-align:center;color:#6b7280\\\">FT \"+g.ftH+\"-\"+g.ftA+\"</td></tr>\";";
  H+="  }";
  H+="  function mkChip(g){";
  H+="    var bc=g.fhTotal>1?\"#bbf7d0\":\"#e5e7eb\";";
  H+="    var bg=g.fhTotal>1?\"#f0fdf4\":\"#f9fafb\";";
  H+="    var fc=g.fhTotal>1?\"#16a34a\":\"#6b7280\";";
  H+="    return \"<span class=\\\"chip\\\" style=\\\"border:1px solid \"+bc+\";background:\"+bg+\";color:\"+fc+\"\\\">FH \"+g.fhTotal+\"&middot;FT \"+g.ftTotal+\"</span>\";";
  H+="  }";
  H+="  var hChipStr=\"\",aChipStr=\"\";";
  H+="  for(var i=0;i<(m.hChips||[]).length;i++)hChipStr+=mkChip(m.hChips[i]);";
  H+="  for(var i=0;i<(m.aChips||[]).length;i++)aChipStr+=mkChip(m.aChips[i]);";
  H+="  var smallWarn=m.smallSample?\"<span style=\\\"background:#fef3c7;color:#92400e;font-size:11px;padding:2px 7px;border-radius:4px;margin-left:8px;font-weight:600\\\">&#9888; small sample</span>\":\"\";";
  H+="  var html=\"\";";
  H+="  html+=\"<div class=\\\"match-card\\\" style=\\\"border-left:4px solid \"+probCol+\"\\\">\";";
  H+="  html+=\"<div style=\\\"padding:16px\\\">\";";
  H+="  html+=\"<div style=\\\"font-size:11px;color:#9ca3af;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px\\\">\"+m.league+\"</div>\";";
  H+="  html+=\"<div style=\\\"display:grid;grid-template-columns:1fr auto;gap:10px;align-items:start;margin-bottom:12px\\\">\";";
  H+="  html+=\"<div style=\\\"min-width:0\\\">\";";
  H+="  html+=\"<div style=\\\"font-size:12px;color:#9ca3af;margin-bottom:3px\\\">\"+dt+smallWarn+\"</div>\";";
  H+="  html+=\"<div style=\\\"font-size:17px;font-weight:700;color:#111827;margin-bottom:5px;line-height:1.3\\\">\"+m.home+\" <span style=\\\"color:#d1d5db;font-weight:400;font-size:13px\\\">vs</span> \"+m.away+\"</div>\";";
  H+="  html+=\"<div style=\\\"display:flex;align-items:center;gap:8px\\\">\"+dots+\"<span style=\\\"font-size:12px;color:#6b7280\\\">\"+m.nMet+\"/4 signals met</span></div>\";";
  H+="  html+=\"<div style=\\\"font-size:12px;color:#6b7280;margin-top:4px\\\">Exp FH: <strong style=\\\"color:#374151\\\">\"+m.expFH+\"</strong> &middot; League avg: \"+m.leagueAvg+\"</div>\";";
  H+="  html+=\"</div>\";";
  H+="  html+=\"<div style=\\\"text-align:center;min-width:76px;background:\"+probBg+\";border:1px solid \"+probBorder+\";border-radius:8px;padding:10px 6px;flex-shrink:0\\\">\";";
  H+="  html+=\"<div style=\\\"font-size:30px;font-weight:800;color:\"+probCol+\";line-height:1\\\">\"+m.prob+\"%</div>\";";
  H+="  html+=\"<div style=\\\"font-size:12px;color:\"+probCol+\";margin-top:2px\\\">\"+probLabel+\"</div>\";";
  H+="  html+=\"<div style=\\\"font-size:10px;color:#9ca3af;margin-top:2px\\\">FH OVER 2.5</div></div></div>\";";
  H+="  html+=\"<details>\";";
  H+="  html+=\"<summary style=\\\"font-size:13px;color:#6b7280;padding:5px 0;border-top:1px solid #f3f4f6\\\">&#9660; Show signal detail</summary>\";";
  H+="  html+=\"<div style=\\\"padding-top:10px\\\">\";";
  H+="  html+=\"<table class=\\\"sig-table\\\" style=\\\"margin-bottom:14px\\\">\";";
  H+="  html+=\"<thead><tr>\";";
  H+="  html+=\"<th style=\\\"width:30%\\\">Signal</th>\";";
  H+="  html+=\"<th style=\\\"width:16%;text-align:center\\\">\"+shortName(m.home)+\"</th>\";";
  H+="  html+=\"<th style=\\\"width:16%;text-align:center\\\">\"+shortName(m.away)+\"</th>\";";
  H+="  html+=\"<th style=\\\"width:18%;text-align:center\\\">Combined</th>\";";
  H+="  html+=\"<th style=\\\"width:20%;text-align:center\\\">Result</th>\";";
  H+="  html+=\"</tr></thead><tbody>\"+sigRows+\"</tbody></table>\";";
  H+="  html+=\"<div style=\\\"display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px\\\">\";";
  H+="  html+=\"<div><div style=\\\"font-size:12px;font-weight:600;color:#374151;margin-bottom:4px\\\">\"+shortName(m.home)+\"</div>\"+hChipStr+\"</div>\";";
  H+="  html+=\"<div><div style=\\\"font-size:12px;font-weight:600;color:#374151;margin-bottom:4px\\\">\"+shortName(m.away)+\"</div>\"+aChipStr+\"</div>\";";
  H+="  html+=\"</div>\";";
  H+="  if(h2hRows){";
  H+="    html+=\"<div style=\\\"font-size:12px;font-weight:600;color:#374151;margin-bottom:5px\\\">H2H &mdash; \"+h2hHits+\"/\"+h2hArr.length+\" meetings with FH &ge;2 goals</div>\";";
  H+="    html+=\"<table style=\\\"width:100%;border-collapse:collapse\\\">\"+h2hRows+\"</table>\";";
  H+="  }";
  H+="  html+=\"</div></details></div></div>\";";
  H+="  return html;";
  H+="}";
  H+="document.getElementById(\"headerTitle\").textContent=fmt(new Date(DATES[0]+\"T12:00:00\"));";
  H+="renderTabs();";
  H+="renderLeagueList();";
  H+="<\/script></body></html>";

  return H;
}

const PORT=process.env.PORT||3001;
app.listen(PORT,()=>console.log("Server running on port "+PORT));
