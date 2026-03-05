const express=require("express"),cors=require("cors"),fetch=require("node-fetch"),app=express();
app.use(cors());
const KEY="437fa5361a693ad65c0c97d75f55042da3529532df53b57d34fe28f89789c0e7",BASE="https://api.football-data-api.com";

// All subscribed leagues - used to look up names
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

const getWeekendDates=()=>{
  const today=new Date();
  const daysToSat=(6-today.getDay()+7)%7||7;
  const sat=new Date(today);sat.setDate(today.getDate()+daysToSat);
  const sun=new Date(sat);sun.setDate(sat.getDate()+1);
  return[sat,sun].map(d=>d.toISOString().slice(0,10));
};

app.get("/api/*",async(req,res)=>{
  try{const path=req.path.replace("/api",""),qs=new URLSearchParams({...req.query,key:KEY}).toString(),data=await ftch(`${BASE}${path}?${qs}`);res.json(data)}
  catch(e){res.status(500).json({error:e.message})}
});

app.get("/",async(req,res)=>{
  try{
    // Step 1: fetch weekend fixtures (just 2 requests)
    const[sat,sun]=getWeekendDates();
    console.log(`Fetching weekend: ${sat} and ${sun}`);
    const[satData,sunData]=await Promise.all([
      ftch(`${BASE}/todays-matches?date=${sat}&key=${KEY}`),
      ftch(`${BASE}/todays-matches?date=${sun}&key=${KEY}`)
    ]);
    const allFixtures=[...(satData.data||[]),...(sunData.data||[])];
    
    // Step 2: find which leagues have games and are in our subscribed list
    const leagueFixtures={};
    for(const m of allFixtures){
      const sid=m.competition_id;
      if(LEAGUE_NAMES[sid]){
        if(!leagueFixtures[sid])leagueFixtures[sid]=[];
        leagueFixtures[sid].push(m);
      }
    }
    const activeLeagues=Object.keys(leagueFixtures);
    console.log(`Found ${allFixtures.length} fixtures across ${activeLeagues.length} subscribed leagues`);

    // Step 3: fetch history only for active leagues (2 requests each)
    const leagueHistory={};
    await Promise.all(activeLeagues.map(async sid=>{
      try{
        const[p1,p2]=await Promise.all([
          ftch(`${BASE}/league-matches?season_id=${sid}&max_per_page=300&page=1&key=${KEY}`),
          ftch(`${BASE}/league-matches?season_id=${sid}&max_per_page=300&page=2&key=${KEY}`)
        ]);
        leagueHistory[sid]=[...(p1.data||[]),...(p2.data||[])].filter(m=>m.status==="complete");
      }catch(e){console.error("History error",sid,e.message);leagueHistory[sid]=[]}
    }));

    // Step 4: compute predictions
    const preds=[];
    for(const sid of activeLeagues){
      const completed=leagueHistory[sid]||[];
      const fixtures=leagueFixtures[sid]||[];
      if(completed.length<5)continue;

      const team={};
      const en=n=>{if(!team[n])team[n]={hp:0,hs:0,hc:0,ap:0,as:0,ac:0,recent:[]}};
      let totalFH=0;
      for(const m of completed){
        const ha=parseInt(m.ht_goals_team_a||0),hb=parseInt(m.ht_goals_team_b||0);
        en(m.home_name);en(m.away_name);
        team[m.home_name].hp++;team[m.home_name].hs+=ha;team[m.home_name].hc+=hb;
        team[m.away_name].ap++;team[m.away_name].as+=hb;team[m.away_name].ac+=ha;
        totalFH+=ha+hb;
        team[m.home_name].recent.push({opp:m.away_name,scored:ha,conceded:hb,total:ha+hb,date:m.date_unix});
        team[m.away_name].recent.push({opp:m.home_name,scored:hb,conceded:ha,total:ha+hb,date:m.date_unix});
      }
      for(const t of Object.values(team)){t.recent.sort((a,b)=>b.date-a.date);t.recent=t.recent.slice(0,6)}
      const halfAvg=(totalFH/completed.length)/2||0.5;
      const h2hMap={};
      for(const m of completed){const k=[m.home_name,m.away_name].sort().join("|");if(!h2hMap[k])h2hMap[k]=[];h2hMap[k].push(m)}

      for(const m of fixtures){
        const h=m.home_name,a=m.away_name;
        const hs=team[h]||{hp:1,hs:0,hc:0,ap:1,as:0,ac:0,recent:[]};
        const as_=team[a]||{hp:1,hs:0,hc:0,ap:1,as:0,ac:0,recent:[]};
        const hAtt=(hs.hs/Math.max(hs.hp,1))/halfAvg,hDef=(hs.hc/Math.max(hs.hp,1))/halfAvg;
        const aAtt=(as_.as/Math.max(as_.ap,1))/halfAvg,aDef=(as_.ac/Math.max(as_.ap,1))/halfAvg;
        const expH=hAtt*aDef*halfAvg,expA=aAtt*hDef*halfAvg,expTotal=expH+expA;
        let prob=pois(expTotal);
        const hR=hs.recent,aR=as_.recent;
        const hFR=hR.filter(r=>r.total>2).length/Math.max(hR.length,1);
        const aFR=aR.filter(r=>r.total>2).length/Math.max(aR.length,1);
        prob=Math.min(prob+((hFR+aFR)/2)*15,95);
        const key=[h,a].sort().join("|");
        const h2h=(h2hMap[key]||[]).slice(0,5).map(g=>({home:g.home_name,away:g.away_name,htH:parseInt(g.ht_goals_team_a||0),htA:parseInt(g.ht_goals_team_b||0),ftH:g.homeGoalCount,ftA:g.awayGoalCount}));
        if(h2h.length){const r=h2h.filter(g=>g.htH+g.htA>2).length/h2h.length;prob=prob*0.75+r*100*0.25}
        preds.push({league:LEAGUE_NAMES[sid],dt:m.date_unix*1000,home:h,away:a,expH:+expH.toFixed(2),expA:+expA.toFixed(2),expTotal:+expTotal.toFixed(2),prob:Math.round(prob),hSc:+(hs.hs/Math.max(hs.hp,1)).toFixed(2),hCn:+(hs.hc/Math.max(hs.hp,1)).toFixed(2),aSc:+(as_.as/Math.max(as_.ap,1)).toFixed(2),aCn:+(as_.ac/Math.max(as_.ap,1)).toFixed(2),hR,aR,h2h,hFR:Math.round(hFR*100),aFR:Math.round(aFR*100)});
      }
    }
    preds.sort((a,b)=>b.prob-a.prob);
    console.log(`Returning ${preds.length} predictions`);
    res.send(buildHTML(preds,sat,sun));
  }catch(e){console.error(e);res.status(500).send("<pre>Error: "+e.message+"</pre>")}
});

function buildHTML(preds,sat,sun){
  const badge=p=>p>=35?["HIGH","🔥","#00e87a"]:p>=20?["MEDIUM","⚡","#f5c518"]:["LOW","❄️","#3a5a78"];
  const chip=r=>`<span style="padding:2px 7px;font-size:9px;border:1px solid ${r.total>2?"#00e87a44":"rgba(255,255,255,.08)"};background:${r.total>2?"rgba(0,232,122,.15)":"rgba(255,255,255,.04)"};color:${r.total>2?"#00e87a":"#3a5a78"}">${r.scored}-${r.conceded} vs ${r.opp.split(" ")[0]}</span>`;
  const cards=preds.map(m=>{
    const[label,ico,col]=badge(m.prob);
    const dt=new Date(m.dt).toLocaleString("en-GB",{weekday:"short",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"});
    const h2hOver=m.h2h.filter(g=>g.htH+g.htA>2).length;
    const h2hRows=m.h2h.map(g=>{const ht=g.htH+g.htA;return`<div style="display:flex;gap:10px;padding:3px 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:9px"><span style="min-width:16px;color:${ht>2?"#00e87a":"#1e3e58"}">${ht>2?"✓":"✗"}</span><span style="flex:1;color:#8ab0c8">${g.home} vs ${g.away}</span><span style="color:#ddeeff">HT ${g.htH}-${g.htA}</span><span style="color:#3a5a78;margin-left:8px">FT ${g.ftH}-${g.ftA}</span></div>`}).join("");
    return`<div style="margin-bottom:8px">
      <div style="background:rgba(255,255,255,.02);border-left:3px solid ${col};border:1px solid ${col}18;padding:14px 16px">
        <div style="font-size:8px;color:#1e3e58;letter-spacing:2px;margin-bottom:4px">${m.league.toUpperCase()}</div>
        <div style="display:grid;grid-template-columns:1fr auto;gap:12px;align-items:center">
          <div>
            <div style="font-size:10px;color:#1e3e58;margin-bottom:5px">${dt}</div>
            <div style="font-size:14px;font-weight:bold;color:#ddeeff;margin-bottom:8px">${m.home} <span style="color:#1a3a52;font-weight:normal;font-size:10px;margin:0 6px">vs</span> ${m.away}</div>
            <div style="display:flex;gap:14px;font-size:9px;color:#1e3e58;flex-wrap:wrap">
              <span>EXP FH <span style="color:#4a7a9a">${m.expTotal}</span></span>
              <span>${m.home.split(" ")[0]} <span style="color:#4a7a9a">${m.expH}</span></span>
              <span>${m.away.split(" ")[0]} <span style="color:#4a7a9a">${m.expA}</span></span>
            </div>
          </div>
          <div style="text-align:right;min-width:70px">
            <div style="font-size:28px;font-weight:bold;color:${col};line-height:1">${m.prob}%</div>
            <div style="font-size:9px;color:${col};margin-top:3px">${ico} ${label}</div>
            <div style="font-size:8px;color:#1a3a52;margin-top:2px">FH OVER 2.5</div>
          </div>
        </div>
        <details style="margin-top:10px">
          <summary style="cursor:pointer;font-size:9px;color:#3a5a78;letter-spacing:2px;padding:5px 0;list-style:none">▼ SHOW DETAILS</summary>
          <div style="padding-top:10px;font-size:10px;line-height:1.9">
            <div style="color:#00e87a;font-size:9px;letter-spacing:2px;margin-bottom:6px">SEASON FH STATS</div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:4px;font-size:9px;margin-bottom:10px">
              <span style="color:#3a5a78">TEAM</span><span style="color:#3a5a78;text-align:center">SCORED/G</span><span style="color:#3a5a78;text-align:center">CONCEDED/G</span>
              <span style="color:#8ab0c8">${m.home} (H)</span><span style="color:#ddeeff;text-align:center">${m.hSc}</span><span style="color:#ddeeff;text-align:center">${m.hCn}</span>
              <span style="color:#8ab0c8">${m.away} (A)</span><span style="color:#ddeeff;text-align:center">${m.aSc}</span><span style="color:#ddeeff;text-align:center">${m.aCn}</span>
            </div>
            <div style="color:#00e87a;font-size:9px;letter-spacing:2px;margin-bottom:6px">RECENT FORM (FH)</div>
            <div style="margin-bottom:4px;color:#5a7a9a">${m.home} — FH &gt;2.5 in ${m.hFR}% of last ${m.hR.length} games</div>
            <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:8px">${m.hR.map(chip).join("")}</div>
            <div style="margin-bottom:4px;color:#5a7a9a">${m.away} — FH &gt;2.5 in ${m.aFR}% of last ${m.aR.length} games</div>
            <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:10px">${m.aR.map(chip).join("")}</div>
            ${m.h2h.length?`<div style="color:#00e87a;font-size:9px;letter-spacing:2px;margin-bottom:6px">H2H — FH &gt;2.5 in ${h2hOver}/${m.h2h.length} meetings</div>${h2hRows}`:""}
          </div>
        </details>
      </div>
    </div>`;
  }).join("");
  return`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>FH Over 2.5 Predictor</title>
  <style>*{box-sizing:border-box;margin:0;padding:0}body{background:#060c14;font-family:'Courier New',monospace;color:#b8ccd8;min-height:100vh}details>summary::-webkit-details-marker{display:none}</style>
  </head><body>
  <div style="padding:16px 20px;border-bottom:1px solid #00e87a18;background:linear-gradient(180deg,#0b1726,#070e18);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px">
    <div><div style="font-size:9px;letter-spacing:4px;color:#00e87a;margin-bottom:4px">⚽ FH OVER 2.5 PREDICTOR</div><h1 style="font-size:18px;font-weight:700;color:#fff">${sat} / ${sun}</h1></div>
    <a href="/" style="border:1px solid #00e87a33;color:#00e87a;padding:6px 16px;font-size:10px;letter-spacing:2px;text-decoration:none">↺ REFRESH</a>
  </div>
  <div style="padding:16px 20px;max-width:720px;margin:0 auto">
    <div style="font-size:9px;color:#1e3e58;letter-spacing:2px;margin-bottom:16px">${preds.length} MATCHES · SORTED BY PROBABILITY</div>
    ${preds.length===0?'<div style="text-align:center;padding:50px;color:#1e3e58">No fixtures found this weekend.</div>':cards}
    <div style="margin-top:20px;padding:10px 14px;background:rgba(255,255,255,.012);border:1px solid rgba(255,255,255,.05);font-size:8px;color:#1a3a52;letter-spacing:1px;line-height:2">
      MODEL · Poisson + form (last 6) + H2H · <span style="color:#00e87a">🔥 HIGH ≥35%</span> · <span style="color:#f5c518">⚡ MED ≥20%</span> · <span style="color:#3a5a78">❄️ LOW</span>
    </div>
  </div></body></html>`;
}

const PORT=process.env.PORT||3001;
app.listen(PORT,()=>console.log("Server running on port "+PORT));
