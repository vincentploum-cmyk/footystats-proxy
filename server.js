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

const pois=lam=>{let p=0,t=Math.exp(-lam);for(let k=0;k<3;k++){p+=t;t*=lam/(k+1)}return(1-p)*100};
const ftch=url=>fetch(url).then(r=>r.json());
const avgF=(arr,fn)=>arr.length?arr.reduce((s,x)=>s+fn(x),0)/arr.length:0;
const pctF=(arr,fn)=>arr.length?arr.filter(fn).length/arr.length:0;
const pm=t=>{const m=String(t).match(/^(\d+)/);return m?parseInt(m[1]):null};
const win=(ts,a,b)=>ts.filter(t=>t>=a&&t<=b).length;
const safe=v=>isNaN(v)||!isFinite(v)?0:v;

const getDates=()=>{
  const today=new Date();
  const fmt=d=>{
    const y=d.getFullYear();
    const m=String(d.getMonth()+1).padStart(2,'0');
    const day=String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${day}`;
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
    const data=await ftch(`${BASE}${path}?${qs}`);
    res.json(data);
  }catch(e){res.status(500).json({error:e.message})}
});

app.get("/",async(req,res)=>{
  try{
    const dates=getDates();
    console.log("Fetching dates:",dates);
    const dayResults=await Promise.all(dates.map(d=>ftch(`${BASE}/todays-matches?date=${d}&key=${KEY}`)));
    const allFixtures=[];
    for(let i=0;i<dates.length;i++){
      for(const m of (dayResults[i].data||[])){
        allFixtures.push({...m,_date:dates[i]});
      }
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
    console.log(`${allFixtures.length} fixtures across ${activeLeagues.length} leagues`);

    const preds=[];

    for(const sid of activeLeagues.slice(0,20)){
      let completed=[];
      try{
        const p1=await ftch(`${BASE}/league-matches?season_id=${sid}&max_per_page=150&page=1&key=${KEY}`);
        completed=(p1.data||[])
          .filter(m=>m.status==="complete")
          .map(m=>({
            home_name:m.home_name,away_name:m.away_name,
            ht_goals_team_a:m.ht_goals_team_a,ht_goals_team_b:m.ht_goals_team_b,
            homeGoalCount:m.homeGoalCount,awayGoalCount:m.awayGoalCount,
            homeGoals_timings:m.homeGoals_timings,awayGoals_timings:m.awayGoals_timings,
            date_unix:m.date_unix
          }));
      }catch(e){}

      const fixtures=leagueFixtures[sid]||[];
      if(completed.length<5){completed=null;continue;}

      const team={};
      const en=n=>{if(!team[n])team[n]={h:[],a:[]}};
      let totalFH=0,totalGames=0;
      const h2hMap={};

      for(const m of completed){
        const ha=parseInt(m.ht_goals_team_a||0),hb=parseInt(m.ht_goals_team_b||0);
        const fa=parseInt(m.homeGoalCount||0),fb=parseInt(m.awayGoalCount||0);
        en(m.home_name);en(m.away_name);
        totalFH+=ha+hb;totalGames++;
        const hts=(m.homeGoals_timings||[]).map(pm).filter(t=>t!==null);
        const ats=(m.awayGoals_timings||[]).map(pm).filter(t=>t!==null);
        const fh_hts=hts.filter(t=>t<=45);
        const fh_ats=ats.filter(t=>t<=45);
        const rec=(fh_sc,fh_cn,sc,cn)=>({
          fh_sc,fh_cn,
          sc_0_10:win(sc,0,10),cn_11_20:win(cn,11,20),
          fh_total:ha+hb,ft_total:fa+fb,
          fh_btts:ha>0&&hb>0,ft_o25:fa+fb>2,
          fh_o15:ha+hb>1,date:m.date_unix||0
        });
        team[m.home_name].h.push(rec(ha,hb,fh_hts,fh_ats));
        team[m.away_name].a.push(rec(hb,ha,fh_ats,fh_hts));
        // Build H2H
        const k=[m.home_name,m.away_name].sort().join("|");
        if(!h2hMap[k])h2hMap[k]=[];
        h2hMap[k].push({
          home:m.home_name,away:m.away_name,
          htH:ha,htA:hb,
          ftH:fa,ftA:fb
        });
      }

      completed=null;

      for(const t of Object.values(team)){
        t.h.sort((a,b)=>b.date-a.date);
        t.a.sort((a,b)=>b.date-a.date);
      }

      const safeHalf=totalGames>0?Math.max((totalFH/totalGames)/2,0.5):0.5;

      for(const fixture of fixtures){
        const h=fixture.home_name,a=fixture.away_name;
        const ht=team[h]||{h:[],a:[]};
        const at=team[a]||{h:[],a:[]};
        const hGames=ht.h.length>=3?ht.h.slice(0,12):([...ht.h,...ht.a].sort((x,y)=>y.date-x.date).slice(0,12));
        const aGames=at.a.length>=3?at.a.slice(0,12):([...at.h,...at.a].sort((x,y)=>y.date-x.date).slice(0,12));
        const hAll=[...ht.h,...ht.a].sort((x,y)=>y.date-x.date).slice(0,8);
        const aAll=[...at.h,...at.a].sort((x,y)=>y.date-x.date).slice(0,8);
        if(hGames.length<2||aGames.length<2)continue;

        const hFHSc=safe(avgF(hGames,g=>g.fh_sc));
        const hFHCn=safe(avgF(hGames,g=>g.fh_cn));
        const aFHSc=safe(avgF(aGames,g=>g.fh_sc));
        const aFHCn=safe(avgF(aGames,g=>g.fh_cn));

        const expH=Math.max((hFHSc/safeHalf)*(aFHCn/safeHalf)*safeHalf,0.05);
        const expA=Math.max((aFHSc/safeHalf)*(hFHCn/safeHalf)*safeHalf,0.05);
        let prob=safe(pois(expH+expA));

        // Recalibrated weights — ~40% reduction across board
        const hBtts=safe(pctF(hGames,g=>g.fh_btts));
        const aBtts=safe(pctF(aGames,g=>g.fh_btts));
        prob+=(hBtts-0.15)*30+(aBtts-0.15)*30;

        const hCn1120=safe(avgF(hGames,g=>g.cn_11_20));
        const aCn1120=safe(avgF(aGames,g=>g.cn_11_20));
        prob+=(hCn1120-0.105)*35+(aCn1120-0.105)*35;

        const hSc010=safe(avgF(hGames,g=>g.sc_0_10));
        const aSc010=safe(avgF(aGames,g=>g.sc_0_10));
        prob+=(hSc010-0.098)*45+(aSc010-0.098)*45;

        prob+=((hFHCn-safeHalf)/safeHalf)*5+((aFHCn-safeHalf)/safeHalf)*5;
        prob+=((hFHSc-safeHalf)/safeHalf)*4+((aFHSc-safeHalf)/safeHalf)*4;

        const hFTo25=safe(pctF(hGames,g=>g.ft_o25));
        const aFTo25=safe(pctF(aGames,g=>g.ft_o25));
        prob+=(hFTo25-0.517)*12+(aFTo25-0.517)*12;

        const hFHo15=safe(pctF(hGames,g=>g.fh_o15));
        const aFHo15=safe(pctF(aGames,g=>g.fh_o15));
        prob+=((hFHo15+aFHo15)/2-0.30)*30;

        const hR=safe(pctF(hAll.slice(0,5),g=>g.fh_o15));
        const aR=safe(pctF(aAll.slice(0,5),g=>g.fh_o15));
        prob+=((hR+aR)/2-0.30)*12;

        // H2H signal
        const h2hKey=[h,a].sort().join("|");
        const h2h=(h2hMap[h2hKey]||[]).slice(0,6);
        const h2hRate=h2h.length?h2h.filter(g=>g.htH+g.htA>=2).length/h2h.length:0;
        prob+=h2hRate*6;

        // Cap at 65% — model was overconfident above this
        prob=Math.max(2,Math.min(65,Math.round(safe(prob))));

        // Badges — only when BOTH teams qualify
        const keySignals=[];
        if(hBtts>=0.20&&aBtts>=0.20)
          keySignals.push({label:"BTTS FH",impact:"critical",val:`${(hBtts*100).toFixed(0)}%/${(aBtts*100).toFixed(0)}%`});
        if(hCn1120>=0.10&&aCn1120>=0.10)
          keySignals.push({label:"Cn 11-20",impact:"critical",val:`${hCn1120.toFixed(2)}/${aCn1120.toFixed(2)}`});
        if(hSc010>=0.10&&aSc010>=0.10)
          keySignals.push({label:"Sc 0-10",impact:"strong",val:`${hSc010.toFixed(2)}/${aSc010.toFixed(2)}`});
        if(hFHCn>=safeHalf&&aFHCn>=safeHalf)
          keySignals.push({label:"FH Conceded",impact:"strong",val:`${hFHCn.toFixed(2)}/${aFHCn.toFixed(2)}`});
        if(hFTo25>=0.60&&aFTo25>=0.60)
          keySignals.push({label:"FT Chaos",impact:"strong",val:`${(hFTo25*100).toFixed(0)}%/${(aFTo25*100).toFixed(0)}%`});
        if(h2hRate>=0.50&&h2h.length>=2)
          keySignals.push({label:"H2H FH",impact:"moderate",val:`${h2h.filter(g=>g.htH+g.htA>=2).length}/${h2h.length}`});

        const signals=[
          {label:"FH BTTS rate",hVal:(hBtts*100).toFixed(0)+"%",aVal:(aBtts*100).toFixed(0)+"%",threshold:"20%",hGood:hBtts>=0.20,aGood:aBtts>=0.20,impact:"critical",note:"2.0x lift"},
          {label:"Conceded 11-20/g",hVal:hCn1120.toFixed(3),aVal:aCn1120.toFixed(3),threshold:"0.100",hGood:hCn1120>=0.10,aGood:aCn1120>=0.10,impact:"critical",note:"combo 2.7x"},
          {label:"Scored 0-10/g",hVal:hSc010.toFixed(3),aVal:aSc010.toFixed(3),threshold:"0.100",hGood:hSc010>=0.10,aGood:aSc010>=0.10,impact:"strong",note:"1.6x lift"},
          {label:"FH conceded/g",hVal:hFHCn.toFixed(2),aVal:aFHCn.toFixed(2),threshold:safeHalf.toFixed(2),hGood:hFHCn>=safeHalf,aGood:aFHCn>=safeHalf,impact:"strong",note:"2.2x lift"},
          {label:"FH scored/g",hVal:hFHSc.toFixed(2),aVal:aFHSc.toFixed(2),threshold:safeHalf.toFixed(2),hGood:hFHSc>=safeHalf,aGood:aFHSc>=safeHalf,impact:"moderate",note:"supporting"},
          {label:"FT >2.5 rate",hVal:(hFTo25*100).toFixed(0)+"%",aVal:(aFTo25*100).toFixed(0)+"%",threshold:"52%",hGood:hFTo25>=0.52,aGood:aFTo25>=0.52,impact:"moderate",note:"1.6x lift"},
          {label:"FH >1.5 rate",hVal:(hFHo15*100).toFixed(0)+"%",aVal:(aFHo15*100).toFixed(0)+"%",threshold:"30%",hGood:hFHo15>=0.30,aGood:aFHo15>=0.30,impact:"moderate",note:"supporting"},
          {label:"Recent FH >1.5",hVal:(hR*100).toFixed(0)+"%",aVal:(aR*100).toFixed(0)+"%",threshold:"30%",hGood:hR>=0.30,aGood:aR>=0.30,impact:"moderate",note:"last 5 games"},
          {label:"H2H FH ≥2 goals",hVal:`${h2h.filter(g=>g.htH+g.htA>=2).length}/${h2h.length}`,aVal:"",threshold:"50%",hGood:h2hRate>=0.50,aGood:true,impact:"moderate",note:"matchup pattern"},
        ];

        preds.push({
          league:LEAGUE_NAMES[sid],leagueSid:parseInt(sid),
          dt:(fixture.date_unix||0)*1000,matchDate:fixture._date,
          home:h,away:a,
          expH:+expH.toFixed(2),expA:+expA.toFixed(2),expTotal:+(expH+expA).toFixed(2),
          prob,signals,keySignals,h2h:h2h.slice(0,6),
          hChips:[...ht.h,...ht.a].sort((x,y)=>y.date-x.date).slice(0,6).map(g=>({fhTotal:g.fh_total,ftTotal:g.ft_total})),
          aChips:[...at.h,...at.a].sort((x,y)=>y.date-x.date).slice(0,6).map(g=>({fhTotal:g.fh_total,ftTotal:g.ft_total})),
          halfAvg:+safeHalf.toFixed(2)
        });
      }
    }

    preds.sort((a,b)=>b.prob-a.prob);
    res.send(buildHTML(preds,dates));
  }catch(e){console.error(e);res.status(500).send("<pre>Error: "+e.message+"</pre>")}
});

function buildHTML(preds,dates){
  const predsJSON=JSON.stringify(preds).replace(/</g,"\\u003c");
  const datesJSON=JSON.stringify(dates);

  return`<!DOCTYPE html><html><head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <title>First Half Score</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;font-size:16px}
    details>summary::-webkit-details-marker{display:none}
    .tab{padding:8px 14px;border-radius:6px;cursor:pointer;font-size:14px;font-weight:600;border:1px solid #e5e7eb;background:#fff;color:#6b7280;transition:all .15s}
    .tab.active{background:#111827;color:#fff;border-color:#111827}
    .league-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:16px 18px;margin-bottom:10px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;box-shadow:0 1px 3px rgba(0,0,0,.05);transition:box-shadow .15s}
    .league-card:hover{box-shadow:0 3px 8px rgba(0,0,0,.1)}
    .back-btn{background:#f3f4f6;border:1px solid #e5e7eb;padding:7px 16px;border-radius:6px;cursor:pointer;font-size:15px;font-weight:600;color:#374151}
    .match-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:14px}
    summary{cursor:pointer;user-select:none;list-style:none}
    .sig-table{width:100%;border-collapse:collapse;border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;table-layout:fixed}
    .sig-table td,.sig-table th{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .col-signal{width:42%}.col-val{width:19%}.col-thresh{width:20%}
  </style>
  </head><body>

  <div id="header" style="background:#fff;border-bottom:1px solid #e5e7eb;padding:14px 20px;position:sticky;top:0;z-index:10">
    <div style="max-width:760px;margin:0 auto">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
        <div>
          <div style="font-size:12px;color:#6b7280;letter-spacing:1px;text-transform:uppercase">⚽ First Half Score</div>
          <h1 style="font-size:23px;font-weight:800;color:#111827" id="headerTitle">Loading...</h1>
        </div>
        <button onclick="location.reload()" style="background:#111827;color:#fff;padding:8px 18px;font-size:15px;border:none;border-radius:6px;font-weight:600;cursor:pointer">↺ Refresh</button>
      </div>
      <div id="dayTabs" style="display:flex;gap:8px;flex-wrap:wrap"></div>
    </div>
  </div>

  <div style="padding:16px 20px;max-width:760px;margin:0 auto">
    <div style="font-size:14px;color:#9ca3af;margin-bottom:16px;line-height:1.8">
      <span style="background:#14532d;color:#fff;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600">critical</span> &nbsp;
      <span style="background:#bbf7d0;color:#166534;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600">strong</span> &nbsp;
      <span style="background:#fef9c3;color:#854d0e;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600">moderate</span>
    </div>
    <div id="mainView"></div>
  </div>

  <script>
  const ALL_PREDS=${predsJSON};
  const DATES=${datesJSON};
  const DAY_LABELS=["Today","Tomorrow","Saturday","Sunday","Monday"];
  let activeDate=DATES[0];
  let activeLeague=null;

  function fmt(d){return new Date(d).toLocaleDateString("en-GB",{weekday:"long",day:"2-digit",month:"short"})}

  function renderTabs(){
    const el=document.getElementById("dayTabs");
    el.innerHTML=DATES.map((d,i)=>{
      const count=ALL_PREDS.filter(p=>p.matchDate===d).length;
      return \`<button class="tab \${d===activeDate?"active":""}" onclick="selectDay('\${d}')">\${DAY_LABELS[i]||d} <span style="font-size:13px;opacity:.7">(\${count})</span></button>\`;
    }).join("");
  }

  function selectDay(d){
    activeDate=d;activeLeague=null;
    renderTabs();renderLeagueList();
    document.getElementById("headerTitle").textContent=fmt(new Date(d+"T12:00:00"));
  }

  function renderLeagueList(){
    const dayPreds=ALL_PREDS.filter(p=>p.matchDate===activeDate);
    const leagues={};
    for(const p of dayPreds){if(!leagues[p.league])leagues[p.league]=[];leagues[p.league].push(p);}
    const leagueList=Object.entries(leagues).sort((a,b)=>Math.max(...b[1].map(p=>p.prob))-Math.max(...a[1].map(p=>p.prob)));
    if(leagueList.length===0){
      document.getElementById("mainView").innerHTML=\`<div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280;font-size:16px">No matches found for this day.</div>\`;
      return;
    }
    document.getElementById("mainView").innerHTML=\`
      <div style="font-size:14px;color:#6b7280;margin-bottom:14px">\${dayPreds.length} matches across \${leagueList.length} leagues · sorted by top probability</div>
      \${leagueList.map(([league,matches])=>{
        const maxProb=Math.max(...matches.map(p=>p.prob));
        const probCol=maxProb>=35?"#16a34a":maxProb>=20?"#d97706":"#6b7280";
        const critCount=matches.filter(p=>p.keySignals.some(s=>s.impact==="critical")).length;
        return \`<div class="league-card" onclick="selectLeague('\${league.replace(/\\\\/g,'\\\\\\\\').replace(/'/g,"\\\\'")}')">
          <div style="flex:1;min-width:0;margin-right:12px">
            <div style="font-size:19px;font-weight:700;color:#111827;margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">\${league}</div>
            <div style="font-size:15px;color:#6b7280">\${matches.length} match\${matches.length>1?"es":""}\${critCount>0?\` · <span style="color:#15803d;font-weight:600">\${critCount} with critical signals</span>\`:""}</div>
          </div>
          <div style="text-align:right;flex-shrink:0">
            <div style="font-size:28px;font-weight:800;color:\${probCol}">\${maxProb}%</div>
            <div style="font-size:13px;color:#9ca3af">top match</div>
          </div>
        </div>\`;
      }).join("")}\`;
  }

  function selectLeague(league){activeLeague=league;renderMatchList();}
  function backToLeagues(){activeLeague=null;renderLeagueList();}

  function renderMatchList(){
    const matches=ALL_PREDS.filter(p=>p.matchDate===activeDate&&p.league===activeLeague).sort((a,b)=>b.prob-a.prob);
    document.getElementById("mainView").innerHTML=\`
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
        <button class="back-btn" onclick="backToLeagues()">← Back</button>
        <div style="font-size:20px;font-weight:700;color:#111827">\${activeLeague}</div>
      </div>
      \${matches.map(renderMatchCard).join("")}\`;
  }

  function renderMatchCard(m){
    const probCol=m.prob>=35?"#16a34a":m.prob>=20?"#d97706":"#6b7280";
    const probBg=m.prob>=35?"#f0fdf4":m.prob>=20?"#fffbeb":"#f9fafb";
    const probBorder=m.prob>=35?"#bbf7d0":m.prob>=20?"#fde68a":"#e5e7eb";
    const probLabel=m.prob>=35?"🔥 HIGH":m.prob>=20?"⚡ MED":"❄️ LOW";
    const dt=new Date(m.dt).toLocaleString("en-GB",{weekday:"short",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"});
    const shortName=n=>n.split(" ").slice(0,2).join(" ");
    const keyBadges=m.keySignals.map(s=>{
      const bg=s.impact==="critical"?"#14532d":s.impact==="strong"?"#bbf7d0":"#fef9c3";
      const col=s.impact==="critical"?"#fff":s.impact==="strong"?"#166534":"#854d0e";
      return \`<span style="display:inline-flex;align-items:center;gap:4px;padding:4px 10px;font-size:13px;border-radius:12px;background:\${bg};color:\${col};margin:2px;font-weight:600">\${s.label} \${s.val}</span>\`;
    }).join("");
    const sigRows=m.signals.map((s,i)=>{
      const impBg=s.impact==="critical"?"#14532d":s.impact==="strong"?"#bbf7d0":"#fef9c3";
      const impCol=s.impact==="critical"?"#fff":s.impact==="strong"?"#166534":"#854d0e";
      const hCol=s.hGood?"#15803d":"#dc2626";
      const aCol=s.aVal===""?"#9ca3af":s.aGood?"#15803d":"#dc2626";
      return \`<tr style="background:\${i%2===0?"#fff":"#f9fafb"}">
        <td class="col-signal" style="padding:6px 8px;font-size:13px;color:#374151">
          <span style="display:inline-block;padding:1px 5px;border-radius:3px;font-size:11px;background:\${impBg};color:\${impCol};margin-right:4px;font-weight:600">\${s.impact[0].toUpperCase()}</span>\${s.label}
        </td>
        <td class="col-val" style="padding:6px 6px;text-align:center;font-size:14px;font-weight:700;color:\${hCol}">\${s.hVal}</td>
        <td class="col-thresh" style="padding:6px 6px;text-align:center;font-size:12px;color:#9ca3af">\${s.threshold}</td>
        <td class="col-val" style="padding:6px 6px;text-align:center;font-size:14px;font-weight:700;color:\${aCol}">\${s.aVal}</td>
      </tr>\`;
    }).join("");
    const chip=g=>\`<span style="display:inline-block;padding:3px 8px;font-size:13px;border-radius:3px;margin:2px;border:1px solid \${g.fhTotal>1?"#bbf7d0":"#e5e7eb"};background:\${g.fhTotal>1?"#f0fdf4":"#f9fafb"};color:\${g.fhTotal>1?"#16a34a":"#6b7280"}">FH \${g.fhTotal}·FT \${g.ftTotal}</span>\`;
    const h2hRows=(m.h2h||[]).map(g=>{
      const ht=g.htH+g.htA;
      return \`<tr style="border-bottom:1px solid #f3f4f6">
        <td style="padding:5px 8px;font-size:13px;color:#6b7280">\${g.home} vs \${g.away}</td>
        <td style="padding:5px 8px;font-size:13px;text-align:center;font-weight:700;color:\${ht>=2?"#16a34a":"#374151"}">HT \${g.htH}-\${g.htA}</td>
        <td style="padding:5px 8px;font-size:13px;text-align:center;color:#6b7280">FT \${g.ftH}-\${g.ftA}</td>
      </tr>\`;
    }).join("");

    return \`<div class="match-card" style="border-left:4px solid \${probCol}">
      <div style="padding:16px">
        <div style="font-size:12px;color:#9ca3af;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">\${m.league}</div>
        <div style="display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center">
          <div style="min-width:0">
            <div style="font-size:13px;color:#9ca3af;margin-bottom:4px">\${dt}</div>
            <div style="font-size:18px;font-weight:700;color:#111827;margin-bottom:6px;line-height:1.3">\${m.home} <span style="color:#d1d5db;font-weight:400;font-size:14px">vs</span> \${m.away}</div>
            <div style="font-size:13px;color:#6b7280">Exp FH: <strong style="color:#374151">\${m.expTotal}</strong> · League avg: \${m.halfAvg}</div>
          </div>
          <div style="text-align:center;min-width:80px;background:\${probBg};border:1px solid \${probBorder};border-radius:8px;padding:10px 6px;flex-shrink:0">
            <div style="font-size:33px;font-weight:800;color:\${probCol};line-height:1">\${m.prob}%</div>
            <div style="font-size:13px;color:\${probCol};margin-top:2px">\${probLabel}</div>
            <div style="font-size:11px;color:#9ca3af;margin-top:2px">FH OVER 2.5</div>
          </div>
        </div>
        \${keyBadges?\`<div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:2px">\${keyBadges}</div>\`:""}
        <details style="margin-top:12px">
          <summary style="font-size:14px;color:#6b7280;padding:5px 0">▼ Show all signals</summary>
          <div style="padding-top:10px;overflow-x:hidden">
            <table class="sig-table" style="margin-bottom:14px">
              <thead><tr style="background:#f3f4f6">
                <th class="col-signal" style="padding:6px 8px;text-align:left;font-size:12px;color:#6b7280;font-weight:600">Signal</th>
                <th class="col-val" style="padding:6px 6px;text-align:center;font-size:12px;color:#6b7280;font-weight:600">\${shortName(m.home)}</th>
                <th class="col-thresh" style="padding:6px 6px;text-align:center;font-size:12px;color:#6b7280;font-weight:600">Thr.</th>
                <th class="col-val" style="padding:6px 6px;text-align:center;font-size:12px;color:#6b7280;font-weight:600">\${shortName(m.away)}</th>
              </tr></thead>
              <tbody>\${sigRows}</tbody>
            </table>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
              <div>
                <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:4px">\${shortName(m.home)}</div>
                <div>\${m.hChips.map(chip).join("")}</div>
              </div>
              <div>
                <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:4px">\${shortName(m.away)}</div>
                <div>\${m.aChips.map(chip).join("")}</div>
              </div>
            </div>
            \${h2hRows?\`<div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:6px">
              H2H — \${(m.h2h||[]).filter(g=>g.htH+g.htA>=2).length}/\${(m.h2h||[]).length} meetings FH ≥2 goals
            </div>
            <table style="width:100%;border-collapse:collapse">\${h2hRows}</table>\`:""}
          </div>
        </details>
      </div>
    </div>\`;
  }

  document.getElementById("headerTitle").textContent=fmt(new Date(DATES[0]+"T12:00:00"));
  renderTabs();
  renderLeagueList();
  </script>
  </body></html>`;
}

const PORT=process.env.PORT||3001;
app.get("/fetch-all-data", async (req, res) => {
  const LEAGUE_IDS = [
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
    }catch(e){console.log(`${sid}: error`);}
    await sleep(300);
  }
  res.setHeader("Content-Disposition","attachment; filename=all_leagues.json");
  res.json({data:all});
});
```

Save → commit → wait for Render to deploy → then visit:
```
https://footystats-proxy.onrender.com/fetch-all-data
app.listen(PORT,()=>console.log("Server running on port "+PORT));
