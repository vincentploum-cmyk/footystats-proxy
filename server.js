const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");
const app = express();

app.use(cors());

const KEY = "437fa5361a693ad65c0c97d75f55042da3529532df53b57d34fe28f89789c0e7";
const BASE = "https://api.football-data-api.com";
const BUNDESLIGA_ID = 14968;

app.get("/api/*", async (req, res) => {
  try {
    const path = req.path.replace("/api", "");
    const qs = new URLSearchParams({ ...req.query, key: KEY }).toString();
    const url = `${BASE}${path}?${qs}`;
    const r = await fetch(url);
    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/", (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Bundesliga FH Predictor</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#060c14;font-family:'Courier New',monospace;color:#b8ccd8;min-height:100vh}
header{padding:16px 20px;border-bottom:1px solid #00e87a18;background:linear-gradient(180deg,#0b1726,#070e18);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px}
.tag{font-size:9px;letter-spacing:4px;color:#00e87a;margin-bottom:4px}
h1{font-size:18px;font-weight:700;color:#fff}
main{padding:16px 20px;max-width:720px;margin:0 auto}
#loader{text-align:center;padding:60px 20px;color:#00e87a}
.spin{font-size:32px;display:inline-block;animation:spin 2s linear infinite;margin-bottom:14px}
@keyframes spin{to{transform:rotate(360deg)}}
#status{font-size:10px;letter-spacing:3px}
.hint{font-size:9px;color:#1e3e58;margin-top:6px}
#error{background:rgba(255,60,60,.07);border:1px solid rgba(255,60,60,.2);padding:12px 16px;margin-bottom:16px;color:#ff6868;font-size:11px;line-height:1.8;display:none}
.filters{display:flex;gap:5px;margin-bottom:16px;flex-wrap:wrap;align-items:center;justify-content:space-between}
.filter-label{font-size:9px;color:#1e3e58;letter-spacing:2px}
.fbtn{background:transparent;border:1px solid rgba(255,255,255,.08);color:#1e3e58;padding:5px 12px;cursor:pointer;font-size:9px;letter-spacing:2px;text-transform:uppercase;font-family:inherit}
.fbtn.active{background:rgba(0,232,122,.1);border-color:#00e87a;color:#00e87a}
.card{margin-bottom:8px;animation:fade .3s ease both}
@keyframes fade{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
.card-main{background:rgba(255,255,255,.02);padding:14px 16px}
.card-date{font-size:10px;color:#1e3e58;letter-spacing:1px;margin-bottom:5px}
.card-teams{font-size:14px;font-weight:bold;color:#ddeeff;margin-bottom:8px}
.vs{color:#1a3a52;font-weight:normal;font-size:10px;margin:0 6px}
.card-stats{display:flex;gap:14px;font-size:9px;color:#1e3e58;flex-wrap:wrap}
.sv{color:#4a7a9a}
.card-right{text-align:right;min-width:70px}
.pct{font-size:28px;font-weight:bold;line-height:1}
.rating{font-size:9px;margin-top:3px;letter-spacing:1px}
.over-lbl{font-size:8px;color:#1a3a52;margin-top:2px}
.expand-btn{margin-top:10px;background:rgba(255,255,255,.02);border:1px solid rgba(255,255,255,.06);color:#3a5a78;padding:5px 12px;cursor:pointer;font-size:9px;letter-spacing:2px;width:100%;font-family:inherit}
.details{background:rgba(255,255,255,.015);border:1px solid rgba(255,255,255,.05);border-top:none;padding:14px 16px;font-size:10px;line-height:1.9;display:none}
.details.open{display:block}
.detail-title{color:#00e87a;letter-spacing:2px;font-size:9px;margin-bottom:6px;margin-top:10px}
.detail-title:first-child{margin-top:0}
.stat-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:4px;font-size:9px}
.stat-header{color:#3a5a78}
.stat-team{color:#8ab0c8}
.stat-val{color:#ddeeff;text-align:center}
.form-chips{display:flex;gap:4px;flex-wrap:wrap;margin-top:4px;margin-bottom:8px}
.chip{padding:2px 7px;font-size:9px;border:1px solid}
.chip.hot{background:rgba(0,232,122,.15);border-color:#00e87a44;color:#00e87a}
.chip.cold{background:rgba(255,255,255,.04);border-color:rgba(255,255,255,.08);color:#3a5a78}
.h2h-row{display:flex;gap:10px;align-items:center;padding:3px 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:9px}
.h2h-check{min-width:16px;font-weight:bold}
.h2h-teams{color:#8ab0c8;flex:1}
.h2h-ht{color:#ddeeff}
.h2h-ft{color:#3a5a78}
.model-note{margin-top:20px;padding:10px 14px;background:rgba(255,255,255,.012);border:1px solid rgba(255,255,255,.05);font-size:8px;color:#1a3a52;letter-spacing:1px;line-height:2}
.refresh-btn{background:transparent;border:1px solid #00e87a33;color:#00e87a;padding:6px 16px;cursor:pointer;font-size:10px;letter-spacing:2px;font-family:inherit}
</style>
</head>
<body>
<header>
  <div><div class="tag">⚽ BUNDESLIGA PREDICTOR</div><h1>First Half Over 2.5 Goals</h1></div>
  <button class="refresh-btn" onclick="loadAll()">↺ REFRESH</button>
</header>
<main>
  <div id="error"></div>
  <div id="loader"><div class="spin">◌</div><div id="status">Loading...</div><div class="hint">First load may take ~50s</div></div>
  <div id="app" style="display:none"></div>
</main>
<script>
const BUNDESLIGA_ID = ${BUNDESLIGA_ID};
function poissonOver25(lam){let p=0,t=Math.exp(-lam);for(let k=0;k<3;k++){p+=t;t*=lam/(k+1)}return(1-p)*100}
function badge(p){return p>=35?{label:"HIGH",ico:"🔥",col:"#00e87a"}:p>=20?{label:"MEDIUM",ico:"⚡",col:"#f5c518"}:{label:"LOW",ico:"❄️",col:"#3a5a78"}}
function setStatus(s){document.getElementById("status").textContent=s}
function showError(msg){const el=document.getElementById("error");el.textContent=msg;el.style.display=msg?"block":"none"}
let allMatches=[],currentFilter="all";
async function api(path){const res=await fetch(path);if(!res.ok)throw new Error("HTTP "+res.status);return res.json()}

async function loadAll(){
  document.getElementById("loader").style.display="block";
  document.getElementById("app").style.display="none";
  showError("");
  try{
    setStatus("Loading season history…");
    const[p1,p2]=await Promise.all([api("/api/league-matches?season_id="+BUNDESLIGA_ID+"&max_per_page=300&page=1"),api("/api/league-matches?season_id="+BUNDESLIGA_ID+"&max_per_page=300&page=2")]);
    const completed=[...(p1.data||[]),...(p2.data||[])].filter(m=>m.status==="complete");
    const team={};
    function ensure(n){if(!team[n])team[n]={hp:0,hs:0,hc:0,ap:0,as:0,ac:0,recent:[]}}
    let totalFH=0;
    for(const m of completed){
      const ha=parseInt(m.ht_goals_team_a||0),hb=parseInt(m.ht_goals_team_b||0);
      const h=m.home_name,a=m.away_name;
      ensure(h);ensure(a);
      team[h].hp++;team[h].hs+=ha;team[h].hc+=hb;
      team[a].ap++;team[a].as+=hb;team[a].ac+=ha;
      totalFH+=ha+hb;
      team[h].recent.push({opp:a,scored:ha,conceded:hb,total:ha+hb,date:m.date_unix});
      team[a].recent.push({opp:h,scored:hb,conceded:ha,total:ha+hb,date:m.date_unix});
    }
    for(const t of Object.values(team)){t.recent.sort((a,b)=>b.date-a.date);t.recent=t.recent.slice(0,6)}
    const halfAvg=(totalFH/completed.length)/2;
    const h2hMap={};
    for(const m of completed){const key=[m.home_name,m.away_name].sort().join("|");if(!h2hMap[key])h2hMap[key]=[];h2hMap[key].push(m)}
    setStatus("Loading weekend fixtures…");
    const today=new Date();
    const daysToSat=(6-today.getDay()+7)%7||7;
    const sat=new Date(today);sat.setDate(today.getDate()+daysToSat);
    const sun=new Date(sat);sun.setDate(sat.getDate()+1);
    const fmt=d=>d.toISOString().slice(0,10);
    const[satData,sunData]=await Promise.all([api("/api/todays-matches?date="+fmt(sat)),api("/api/todays-matches?date="+fmt(sun))]);
    const weekend=[...(satData.data||[]),...(sunData.data||[])].filter(m=>m.competition_id===BUNDESLIGA_ID);
    if(!weekend.length)throw new Error("No Bundesliga fixtures found this weekend.");
    setStatus("Calculating predictions…");
    allMatches=weekend.map(m=>{
      const h=m.home_name,a=m.away_name;
      const hs=team[h]||{hp:1,hs:1,hc:1,ap:1,as:1,ac:1,recent:[]};
      const as_=team[a]||{hp:1,hs:1,hc:1,ap:1,as:1,ac:1,recent:[]};
      const hAtt=(hs.hs/Math.max(hs.hp,1))/halfAvg,hDef=(hs.hc/Math.max(hs.hp,1))/halfAvg;
      const aAtt=(as_.as/Math.max(as_.ap,1))/halfAvg,aDef=(as_.ac/Math.max(as_.ap,1))/halfAvg;
      const expH=hAtt*aDef*halfAvg,expA=aAtt*hDef*halfAvg,expTotal=expH+expA;
      let prob=poissonOver25(expTotal);
      const hR=hs.recent.slice(0,6),aR=as_.recent.slice(0,6);
      const hFR=hR.filter(r=>r.total>2).length/Math.max(hR.length,1);
      const aFR=aR.filter(r=>r.total>2).length/Math.max(aR.length,1);
      prob=Math.min(prob+((hFR+aFR)/2)*15,95);
      const key=[h,a].sort().join("|");
      const h2h=(h2hMap[key]||[]).slice(0,5).map(g=>({home:g.home_name,away:g.away_name,htH:parseInt(g.ht_goals_team_a||0),htA:parseInt(g.ht_goals_team_b||0),ftH:g.homeGoalCount,ftA:g.awayGoalCount}));
      if(h2h.length>0){const h2hRate=h2h.filter(g=>g.htH+g.htA>2).length/h2h.length;prob=prob*0.75+(h2hRate*100)*0.25}
      return{dt:new Date(m.date_unix*1000),home:h,away:a,expH:expH.toFixed(2),expA:expA.toFixed(2),expTotal:expTotal.toFixed(2),prob:Math.round(prob),hStats:{sc:(hs.hs/Math.max(hs.hp,1)).toFixed(2),cn:(hs.hc/Math.max(hs.hp,1)).toFixed(2)},aStats:{sc:(as_.as/Math.max(as_.ap,1)).toFixed(2),cn:(as_.ac/Math.max(as_.ap,1)).toFixed(2)},hRecent:hR,aRecent:aR,h2h,hFR:Math.round(hFR*100),aFR:Math.round(aFR*100)};
    }).sort((a,b)=>b.prob-a.prob);
    document.getElementById("loader").style.display="none";
    document.getElementById("app").style.display="block";
    renderApp();
  }catch(e){showError("⚠ "+e.message);document.getElementById("loader").style.display="none"}
}

function setFilter(f){currentFilter=f;document.querySelectorAll(".fbtn").forEach(b=>b.classList.remove("active"));document.getElementById("filter-"+f).classList.add("active");renderMatches()}
function toggleDetails(i){const el=document.getElementById("d"+i),btn=document.getElementById("e"+i),open=el.classList.contains("open");el.classList.toggle("open",!open);btn.textContent=open?"▼ SHOW DETAILS":"▲ HIDE DETAILS"}

function renderApp(){
  document.getElementById("app").innerHTML='<div class="filters"><span class="filter-label">WEEKEND · '+allMatches.length+' MATCHES</span><div style="display:flex;gap:5px"><button id="filter-all" class="fbtn active" onclick="setFilter(\'all\')">All</button><button id="filter-high" class="fbtn" onclick="setFilter(\'high\')">🔥 High</button><button id="filter-medium" class="fbtn" onclick="setFilter(\'medium\')">⚡ Med</button></div></div><div id="match-list"></div><div class="model-note">MODEL · Poisson + form + H2H · <span style="color:#00e87a">🔥 HIGH ≥35%</span> · <span style="color:#f5c518">⚡ MED ≥20%</span> · <span style="color:#3a5a78">❄️ LOW</span></div>';
  renderMatches();
}

function renderMatches(){
  const shown=currentFilter==="high"?allMatches.filter(m=>m.prob>=35):currentFilter==="medium"?allMatches.filter(m=>m.prob>=20&&m.prob<35):allMatches;
  document.getElementById("match-list").innerHTML=shown.map((m,i)=>{
    const b=badge(m.prob);
    const ds=m.dt.toLocaleString("en-GB",{weekday:"short",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"});
    const chips=(arr,lbl,rate)=>'<div style="margin-bottom:4px;color:#5a7a9a">'+lbl+' — FH &gt;2.5 in '+rate+'% of last '+arr.length+' games</div><div class="form-chips">'+arr.map(r=>'<span class="chip '+(r.total>2?'hot':'cold')+'">'+r.scored+'-'+r.conceded+' vs '+r.opp.split(' ')[0]+'</span>').join('')+'</div>';
    const h2hRows=m.h2h.map(g=>{const ht=g.htH+g.htA;return'<div class="h2h-row"><span class="h2h-check" style="color:'+(ht>2?'#00e87a':'#1e3e58')+'">'+(ht>2?'✓':'✗')+'</span><span class="h2h-teams">'+g.home+' vs '+g.away+'</span><span class="h2h-ht">HT '+g.htH+'-'+g.htA+'</span><span class="h2h-ft">FT '+g.ftH+'-'+g.ftA+'</span></div>'}).join('');
    const h2hOver=m.h2h.filter(g=>g.htH+g.htA>2).length;
    return '<div class="card"><div class="card-main" style="border-left:3px solid '+b.col+';border:1px solid '+b.col+'18"><div style="display:grid;grid-template-columns:1fr auto;gap:12px;align-items:center"><div><div class="card-date">'+ds+'</div><div class="card-teams">'+m.home+'<span class="vs">vs</span>'+m.away+'</div><div class="card-stats"><span>EXP FH <span class="sv">'+m.expTotal+'</span></span><span>'+m.home.split(' ')[0]+' <span class="sv">'+m.expH+'</span></span><span>'+m.away.split(' ')[0]+' <span class="sv">'+m.expA+'</span></span></div></div><div class="card-right"><div class="pct" style="color:'+b.col+'">'+m.prob+'%</div><div class="rating" style="color:'+b.col+'">'+b.ico+' '+b.label+'</div><div class="over-lbl">FH OVER 2.5</div></div></div><button id="e'+i+'" class="expand-btn" onclick="toggleDetails('+i+')">▼ SHOW DETAILS</button></div><div id="d'+i+'" class="details"><div class="detail-title">SEASON FH STATS</div><div class="stat-grid"><span class="stat-header">TEAM</span><span class="stat-header" style="text-align:center">SCORED/G</span><span class="stat-header" style="text-align:center">CONCEDED/G</span><span class="stat-team">'+m.home+' (H)</span><span class="stat-val">'+m.hStats.sc+'</span><span class="stat-val">'+m.hStats.cn+'</span><span class="stat-team">'+m.away+' (A)</span><span class="stat-val">'+m.aStats.sc+'</span><span class="stat-val">'+m.aStats.cn+'</span></div><div class="detail-title">RECENT FORM</div>'+chips(m.hRecent,m.home,m.hFR)+chips(m.aRecent,m.away,m.aFR)+(m.h2h.length?'<div class="detail-title">H2H — FH &gt;2.5 in '+h2hOver+'/'+m.h2h.length+' meetings</div>'+h2hRows:'')+'</div></div>';
  }).join('');
}
loadAll();
</script>
</body>
</html>`);
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log("Server running on port " + PORT));
