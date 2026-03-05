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
    const[sat,sun]=getWeekendDates();
    console.log(`Fetching weekend: ${sat} and ${sun}`);
    const[satData,sunData]=await Promise.all([
      ftch(`${BASE}/todays-matches?date=${sat}&key=${KEY}`),
      ftch(`${BASE}/todays-matches?date=${sun}&key=${KEY}`)
    ]);
    const allFixtures=[...(satData.data||[]),...(sunData.data||[])];
    const leagueFixtures={};
    for(const m of allFixtures){
      const sid=m.competition_id;
      if(LEAGUE_NAMES[sid]){if(!leagueFixtures[sid])leagueFixtures[sid]=[];leagueFixtures[sid].push(m)}
    }
    const activeLeagues=Object.keys(leagueFixtures);
    console.log(`${allFixtures.length} fixtures across ${activeLeagues.length} leagues`);

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

    const preds=[];
    for(const sid of activeLeagues){
      const completed=leagueHistory[sid]||[];
      const fixtures=leagueFixtures[sid]||[];
      if(completed.length<5)continue;

      const team={};
      const en=n=>{if(!team[n])team[n]={hp:0,hs:0,hc:0,ap:0,as:0,ac:0,recentFH:[],recentFT:[]}};
      let totalFH=0;
      for(const m of completed){
        const ha=parseInt(m.ht_goals_team_a||0),hb=parseInt(m.ht_goals_team_b||0);
        const fa=parseInt(m.homeGoalCount||0),fb=parseInt(m.awayGoalCount||0);
        en(m.home_name);en(m.away_name);
        team[m.home_name].hp++;team[m.home_name].hs+=ha;team[m.home_name].hc+=hb;
        team[m.away_name].ap++;team[m.away_name].as+=hb;team[m.away_name].ac+=ha;
        totalFH+=ha+hb;
        team[m.home_name].recentFH.push({opp:m.away_name,scored:ha,conceded:hb,total:ha+hb,date:m.date_unix});
        team[m.away_name].recentFH.push({opp:m.home_name,scored:hb,conceded:ha,total:ha+hb,date:m.date_unix});
        team[m.home_name].recentFT.push({total:fa+fb,date:m.date_unix});
        team[m.away_name].recentFT.push({total:fa+fb,date:m.date_unix});
      }
      for(const t of Object.values(team)){
        t.recentFH.sort((a,b)=>b.date-a.date);t.recentFH=t.recentFH.slice(0,6);
        t.recentFT.sort((a,b)=>b.date-a.date);t.recentFT=t.recentFT.slice(0,4);
      }
      const halfAvg=(totalFH/completed.length)/2||0.5;
      const h2hMap={};
      for(const m of completed){
        const k=[m.home_name,m.away_name].sort().join("|");
        if(!h2hMap[k])h2hMap[k]=[];
        h2hMap[k].push(m);
      }

      for(const m of fixtures){
        const h=m.home_name,a=m.away_name;
        const hs=team[h]||{hp:1,hs:0,hc:0,ap:1,as:0,ac:0,recentFH:[],recentFT:[]};
        const as_=team[a]||{hp:1,hs:0,hc:0,ap:1,as:0,ac:0,recentFH:[],recentFT:[]};

        // Averages
        const hFHSc=hs.hs/Math.max(hs.hp,1);
        const hFHCn=hs.hc/Math.max(hs.hp,1);
        const aFHSc=as_.as/Math.max(as_.ap,1);
        const aFHCn=as_.ac/Math.max(as_.ap,1);

        // RULE 1: both teams avg FH scored > 1.0
        if(hFHSc<=1.0||aFHSc<=1.0)continue;
        // RULE 2: both teams avg FH conceded > 1.0
        if(hFHCn<=1.0||aFHCn<=1.0)continue;

        // H2H
        const key=[h,a].sort().join("|");
        const h2h=(h2hMap[key]||[]).slice(0,5).map(g=>({
          home:g.home_name,away:g.away_name,
          htH:parseInt(g.ht_goals_team_a||0),htA:parseInt(g.ht_goals_team_b||0),
          ftH:parseInt(g.homeGoalCount||0),ftA:parseInt(g.awayGoalCount||0)
        }));

        // RULE 3: at least 1 H2H with FH >= 2 goals (over 1.5)
        const h2hOver15=h2h.filter(g=>g.htH+g.htA>=2).length;
        if(h2h.length>0&&h2hOver15===0)continue;

        // RULE 4: both teams last 4 FT fixtures, at least 3 had 3+ goals
        const hFTChaos=hs.recentFT.filter(r=>r.total>=3).length;
        const aFTChaos=as_.recentFT.filter(r=>r.total>=3).length;
        if(hFTChaos<3||aFTChaos<3)continue;

        // Passed all rules — compute probability
        const hAtt=hFHSc/halfAvg,hDef=hFHCn/halfAvg;
        const aAtt=aFHSc/halfAvg,aDef=aFHCn/halfAvg;
        const expH=hAtt*aDef*halfAvg,expA=aAtt*hDef*halfAvg,expTotal=expH+expA;
        let prob=pois(expTotal);
        const hFR=hs.recentFH.filter(r=>r.total>2).length/Math.max(hs.recentFH.length,1);
        const aFR=as_.recentFH.filter(r=>r.total>2).length/Math.max(as_.recentFH.length,1);
        prob=Math.min(prob+((hFR+aFR)/2)*15,95);
        if(h2h.length){const r=h2hOver15/h2h.length;prob=prob*0.75+r*100*0.25}

        // Rule pass indicators for display
        const rules=[
          {ok:true,label:`FH Scored: ${hFHSc.toFixed(2)} / ${aFHSc.toFixed(2)}`},
          {ok:true,label:`FH Conceded: ${hFHCn.toFixed(2)} / ${aFHCn.toFixed(2)}`},
          {ok:true,label:`H2H FH≥2: ${h2hOver15}/${h2h.length} meetings`},
          {ok:true,label:`FT Chaos: ${h} ${hFTChaos}/4, ${a.split(" ")[0]} ${aFTChaos}/4`},
        ];

        preds.push({
          league:LEAGUE_NAMES[sid],dt:m.date_unix*1000,home:h,away:a,
          expH:+expH.toFixed(2),expA:+expA.toFixed(2),expTotal:+expTotal.toFixed(2),
          prob:Math.round(prob),
          hFHSc:+hFHSc.toFixed(2),hFHCn:+hFHCn.toFixed(2),
          aFHSc:+aFHSc.toFixed(2),aFHCn:+aFHCn.toFixed(2),
          hR:hs.recentFH,aR:as_.recentFH,
          hFT:hs.recentFT,aFT:as_.recentFT,
          h2h,rules,
          hFR:Math.round(hFR*100),aFR:Math.round(aFR*100),
          h2hOver15,hFTChaos,aFTChaos
        });
      }
    }
    preds.sort((a,b)=>b.prob-a.prob);
    console.log(`${preds.length} matches passed all 4 rules`);
    res.send(buildHTML(preds,sat,sun));
  }catch(e){console.error(e);res.status(500).send("<pre>Error: "+e.message+"</pre>")}
});

function buildHTML(preds,sat,sun){
  const badge=p=>p>=35?["HIGH","🔥","#16a34a"]:p>=20?["MEDIUM","⚡","#d97706"]:["LOW","❄️","#6b7280"];
  const chip=(r,fh)=>`<span style="display:inline-block;padding:2px 8px;font-size:11px;border-radius:3px;border:1px solid ${fh?(r.total>2?"#16a34a44":"#e5e7eb"):(r.total>=3?"#16a34a44":"#e5e7eb")};background:${fh?(r.total>2?"#f0fdf4":"#f9fafb"):(r.total>=3?"#f0fdf4":"#f9fafb")};color:${fh?(r.total>2?"#16a34a":"#9ca3af"):(r.total>=3?"#16a34a":"#9ca3af")}">${fh?`${r.scored}-${r.conceded} vs ${r.opp.split(" ")[0]}`:`${r.total} goals`}</span>`;

  const cards=preds.map(m=>{
    const[label,ico,col]=badge(m.prob);
    const dt=new Date(m.dt).toLocaleString("en-GB",{weekday:"short",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"});
    const h2hRows=m.h2h.map(g=>{const ht=g.htH+g.htA;return`<tr style="border-bottom:1px solid #f3f4f6"><td style="padding:4px 8px;color:#6b7280">${g.home} vs ${g.away}</td><td style="padding:4px 8px;text-align:center;font-weight:600;color:${ht>=2?"#16a34a":"#374151"}">HT ${g.htH}-${g.htA}</td><td style="padding:4px 8px;text-align:center;color:#6b7280">FT ${g.ftH}-${g.ftA}</td></tr>`}).join("");

    return`<div style="margin-bottom:12px;background:#fff;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.06)">
      <div style="padding:14px 16px;border-left:4px solid ${col}">
        <div style="font-size:10px;color:#9ca3af;letter-spacing:1px;margin-bottom:4px;text-transform:uppercase">${m.league}</div>
        <div style="display:grid;grid-template-columns:1fr auto;gap:12px;align-items:center">
          <div>
            <div style="font-size:11px;color:#9ca3af;margin-bottom:4px">${dt}</div>
            <div style="font-size:16px;font-weight:700;color:#111827;margin-bottom:8px">${m.home} <span style="color:#d1d5db;font-weight:400;font-size:12px">vs</span> ${m.away}</div>
            <div style="display:flex;gap:16px;font-size:11px;color:#6b7280;flex-wrap:wrap">
              <span>Exp FH total: <strong style="color:#374151">${m.expTotal}</strong></span>
              <span>${m.home.split(" ")[0]}: <strong style="color:#374151">${m.expH}</strong></span>
              <span>${m.away.split(" ")[0]}: <strong style="color:#374151">${m.expA}</strong></span>
            </div>
          </div>
          <div style="text-align:center;min-width:80px;background:#f9fafb;border-radius:8px;padding:10px">
            <div style="font-size:30px;font-weight:800;color:${col};line-height:1">${m.prob}%</div>
            <div style="font-size:11px;color:${col};margin-top:2px">${ico} ${label}</div>
            <div style="font-size:9px;color:#9ca3af;margin-top:2px">FH OVER 2.5</div>
          </div>
        </div>

        <!-- Rule badges -->
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:10px">
          <span style="font-size:10px;padding:2px 8px;border-radius:12px;background:#f0fdf4;color:#16a34a;border:1px solid #bbf7d0">✓ FH Scored ${m.hFHSc}/${m.aFHSc}</span>
          <span style="font-size:10px;padding:2px 8px;border-radius:12px;background:#f0fdf4;color:#16a34a;border:1px solid #bbf7d0">✓ FH Conceded ${m.hFHCn}/${m.aFHCn}</span>
          <span style="font-size:10px;padding:2px 8px;border-radius:12px;background:#f0fdf4;color:#16a34a;border:1px solid #bbf7d0">✓ H2H FH≥2: ${m.h2hOver15}/${m.h2h.length}</span>
          <span style="font-size:10px;padding:2px 8px;border-radius:12px;background:#f0fdf4;color:#16a34a;border:1px solid #bbf7d0">✓ FT Chaos ${m.hFTChaos}/4 · ${m.aFTChaos}/4</span>
        </div>

        <details style="margin-top:10px">
          <summary style="cursor:pointer;font-size:11px;color:#6b7280;padding:4px 0;list-style:none">▼ Show details</summary>
          <div style="padding-top:10px">

            <div style="font-size:11px;font-weight:600;color:#374151;margin-bottom:6px">Season FH Stats</div>
            <table style="width:100%;font-size:11px;border-collapse:collapse;margin-bottom:12px">
              <tr style="background:#f9fafb"><th style="padding:4px 8px;text-align:left;color:#6b7280;font-weight:500">Team</th><th style="padding:4px 8px;text-align:center;color:#6b7280;font-weight:500">FH Scored/g</th><th style="padding:4px 8px;text-align:center;color:#6b7280;font-weight:500">FH Conceded/g</th></tr>
              <tr style="border-bottom:1px solid #f3f4f6"><td style="padding:4px 8px;color:#374151">${m.home} (H)</td><td style="padding:4px 8px;text-align:center;font-weight:600;color:#16a34a">${m.hFHSc}</td><td style="padding:4px 8px;text-align:center;font-weight:600;color:#16a34a">${m.hFHCn}</td></tr>
              <tr><td style="padding:4px 8px;color:#374151">${m.away} (A)</td><td style="padding:4px 8px;text-align:center;font-weight:600;color:#16a34a">${m.aFHSc}</td><td style="padding:4px 8px;text-align:center;font-weight:600;color:#16a34a">${m.aFHCn}</td></tr>
            </table>

            <div style="font-size:11px;font-weight:600;color:#374151;margin-bottom:6px">Recent Form — FH Goals (last 6)</div>
            <div style="margin-bottom:4px;font-size:11px;color:#6b7280">${m.home} — FH &gt;2.5 in ${m.hFR}% of games</div>
            <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:8px">${m.hR.map(r=>chip(r,true)).join("")}</div>
            <div style="margin-bottom:4px;font-size:11px;color:#6b7280">${m.away} — FH &gt;2.5 in ${m.aFR}% of games</div>
            <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:12px">${m.aR.map(r=>chip(r,true)).join("")}</div>

            <div style="font-size:11px;font-weight:600;color:#374151;margin-bottom:6px">FT Chaos — Last 4 Full Time Results</div>
            <div style="margin-bottom:4px;font-size:11px;color:#6b7280">${m.home} (${m.hFTChaos}/4 with 3+ goals)</div>
            <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:8px">${m.hFT.map(r=>chip(r,false)).join("")}</div>
            <div style="margin-bottom:4px;font-size:11px;color:#6b7280">${m.away} (${m.aFTChaos}/4 with 3+ goals)</div>
            <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:12px">${m.aFT.map(r=>chip(r,false)).join("")}</div>

            ${m.h2h.length?`<div style="font-size:11px;font-weight:600;color:#374151;margin-bottom:6px">H2H History (${m.h2hOver15}/${m.h2h.length} with FH ≥2 goals)</div>
            <table style="width:100%;font-size:11px;border-collapse:collapse">${h2hRows}</table>`:""}
          </div>
        </details>
      </div>
    </div>`;
  }).join("");

  return`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>FH Over 2.5 Predictor</title>
  <style>*{box-sizing:border-box;margin:0;padding:0}body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;min-height:100vh}details>summary::-webkit-details-marker{display:none}</style>
  </head><body>
  <div style="background:#fff;border-bottom:1px solid #e5e7eb;padding:16px 20px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px">
    <div>
      <div style="font-size:11px;color:#6b7280;letter-spacing:1px;text-transform:uppercase;margin-bottom:2px">⚽ FH Over 2.5 Predictor</div>
      <h1 style="font-size:20px;font-weight:800;color:#111827">${sat} &amp; ${sun}</h1>
    </div>
    <a href="/" style="background:#111827;color:#fff;padding:8px 18px;font-size:12px;text-decoration:none;border-radius:6px;font-weight:600">↺ Refresh</a>
  </div>
  <div style="padding:16px 20px;max-width:740px;margin:0 auto">
    <div style="font-size:11px;color:#6b7280;margin-bottom:14px">${preds.length} matches passed all 4 rules · sorted by probability</div>
    ${preds.length===0?`<div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280">No matches passed all 4 rules this weekend.</div>`:cards}
    <div style="margin-top:16px;padding:12px 16px;background:#fff;border:1px solid #e5e7eb;border-radius:8px;font-size:11px;color:#9ca3af;line-height:1.8">
      <strong style="color:#374151">Rules applied:</strong> (1) Both teams FH scored &gt;1.0 · (2) Both teams FH conceded &gt;1.0 · (3) ≥1 H2H match with FH ≥2 goals · (4) Both teams ≥3/4 recent FT matches with 3+ goals · Model: Poisson + form + H2H blend
    </div>
  </div>
  </body></html>`;
}

const PORT=process.env.PORT||3001;
app.listen(PORT,()=>console.log("Server running on port "+PORT));
