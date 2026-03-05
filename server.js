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

// Returns [today, saturday, sunday] — deduplicated
const getDates=()=>{
  const today=new Date();
  const fmt=d=>d.toISOString().slice(0,10);
  const dates=[];
  // Always fetch next 4 days (today through Sunday)
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

    // Fetch all dates in parallel (just 2-3 requests)
    const dayResults=await Promise.all(
      dates.map(d=>ftch(`${BASE}/todays-matches?date=${d}&key=${KEY}`))
    );

    // Map each fixture to its date
    const allFixtures=[];
    for(let i=0;i<dates.length;i++){
      for(const m of (dayResults[i].data||[])){
        allFixtures.push({...m,_date:dates[i]});
      }
    }

    // Group by league
    const leagueFixtures={};
    for(const m of allFixtures){
      const sid=m.competition_id;
      if(LEAGUE_NAMES[sid]){
        if(!leagueFixtures[sid])leagueFixtures[sid]=[];
        leagueFixtures[sid].push(m);
      }
    }
    const activeLeagues=Object.keys(leagueFixtures);
    console.log(`${allFixtures.length} fixtures across ${activeLeagues.length} leagues`);

    // Fetch history sequentially (memory safe)
    const leagueHistory={};
    for(const sid of activeLeagues){
      try{
        const p1=await ftch(`${BASE}/league-matches?season_id=${sid}&max_per_page=300&page=1&key=${KEY}`);
        leagueHistory[sid]=(p1.data||[])
          .filter(m=>m.status==="complete")
          .map(m=>({
            home_name:m.home_name,away_name:m.away_name,
            ht_goals_team_a:m.ht_goals_team_a,ht_goals_team_b:m.ht_goals_team_b,
            homeGoalCount:m.homeGoalCount,awayGoalCount:m.awayGoalCount,
            homeGoals_timings:m.homeGoals_timings,awayGoals_timings:m.awayGoals_timings,
            date_unix:m.date_unix,competition_id:m.competition_id
          }));
      }catch(e){leagueHistory[sid]=[]}
    }

    const preds=[];

    for(const sid of activeLeagues){
      const completed=leagueHistory[sid]||[];
      const fixtures=leagueFixtures[sid]||[];
      if(completed.length<5)continue;

      const team={};
      const en=n=>{if(!team[n])team[n]={h:[],a:[]}};
      let totalFH=0,totalGames=0;

      for(const m of completed){
        const ha=parseInt(m.ht_goals_team_a||0),hb=parseInt(m.ht_goals_team_b||0);
        const fa=parseInt(m.homeGoalCount||0),fb=parseInt(m.awayGoalCount||0);
        en(m.home_name);en(m.away_name);
        totalFH+=ha+hb;totalGames++;
        const hts=(m.homeGoals_timings||[]).map(pm).filter(t=>t!==null);
        const ats=(m.awayGoals_timings||[]).map(pm).filter(t=>t!==null);
        const fh_hts=hts.filter(t=>t<=45);
        const fh_ats=ats.filter(t=>t<=45);
        const rec=(fh_sc,fh_cn,sc,cn,ft_sc,ft_cn)=>({
          fh_sc,fh_cn,
          sc_0_10:win(sc,0,10),sc_11_20:win(sc,11,20),
          sc_21_30:win(sc,21,30),sc_31_45:win(sc,31,45),
          cn_0_10:win(cn,0,10),cn_11_20:win(cn,11,20),
          cn_21_30:win(cn,21,30),cn_31_45:win(cn,31,45),
          fh_total:ha+hb,ft_total:fa+fb,
          fh_btts:ha>0&&hb>0,ft_o25:fa+fb>2,ft_o35:fa+fb>3,
          fh_o15:ha+hb>1,fh_o25:ha+hb>2,
          date:m.date_unix
        });
        team[m.home_name].h.push(rec(ha,hb,fh_hts,fh_ats,fa,fb));
        team[m.away_name].a.push(rec(hb,ha,fh_ats,fh_hts,fb,fa));
      }
      for(const t of Object.values(team)){
        t.h.sort((a,b)=>b.date-a.date);
        t.a.sort((a,b)=>b.date-a.date);
      }

      const leagueHalfAvg=(totalFH/totalGames)/2||0.5;
      const h2hMap={};
      for(const m of completed){
        const k=[m.home_name,m.away_name].sort().join("|");
        if(!h2hMap[k])h2hMap[k]=[];
        h2hMap[k].push(m);
      }

      for(const fixture of fixtures){
        const h=fixture.home_name,a=fixture.away_name;
        const ht=team[h]||{h:[],a:[]};
        const at=team[a]||{h:[],a:[]};
        const hGames=ht.h.length>=3?ht.h.slice(0,12):([...ht.h,...ht.a].sort((x,y)=>y.date-x.date).slice(0,12));
        const aGames=at.a.length>=3?at.a.slice(0,12):([...at.h,...at.a].sort((x,y)=>y.date-x.date).slice(0,12));
        const hAll=[...ht.h,...ht.a].sort((x,y)=>y.date-x.date).slice(0,8);
        const aAll=[...at.h,...at.a].sort((x,y)=>y.date-x.date).slice(0,8);
        if(hGames.length<2||aGames.length<2)continue;

        const hFHSc=avgF(hGames,g=>g.fh_sc);
        const hFHCn=avgF(hGames,g=>g.fh_cn);
        const aFHSc=avgF(aGames,g=>g.fh_sc);
        const aFHCn=avgF(aGames,g=>g.fh_cn);
        const halfAvg=leagueHalfAvg;
        const expH=Math.max((hFHSc/halfAvg)*(aFHCn/halfAvg)*halfAvg,0.05);
        const expA=Math.max((aFHSc/halfAvg)*(hFHCn/halfAvg)*halfAvg,0.05);
        const expTotal=expH+expA;
        let prob=pois(expTotal);

        // Signal 1: FH BTTS rate — 4.2x lift (CRITICAL)
        const hBtts=pctF(hGames,g=>g.fh_btts);
        const aBtts=pctF(aGames,g=>g.fh_btts);
        const combinedBtts=(hBtts+aBtts)/2;
        prob+=(combinedBtts-0.20)*90;

        // Signal 2: Conceded 11-20 min — 4.2x lift (CRITICAL)
        const hCn1120=avgF(hGames,g=>g.cn_11_20);
        const aCn1120=avgF(aGames,g=>g.cn_11_20);
        prob+=(hCn1120-0.105)*60+(aCn1120-0.105)*60;

        // Signal 3: Scored 0-10 min — 2.5x lift (STRONG)
        const hSc010=avgF(hGames,g=>g.sc_0_10);
        const aSc010=avgF(aGames,g=>g.sc_0_10);
        prob+=(hSc010-0.098)*80+(aSc010-0.098)*80;

        // Signal 4: FH conceded/g — 2.2x lift (STRONG)
        prob+=((hFHCn-halfAvg)/halfAvg)*8+((aFHCn-halfAvg)/halfAvg)*8;

        // Signal 5: FH scored/g — 3.1x lift at high end
        prob+=((hFHSc-halfAvg)/halfAvg)*6+((aFHSc-halfAvg)/halfAvg)*6;

        // Signal 6: FT >2.5 rate — 4.7x at high threshold
        const hFTo25=pctF(hGames,g=>g.ft_o25);
        const aFTo25=pctF(aGames,g=>g.ft_o25);
        prob+=((hFTo25+aFTo25)/2-0.517)*40;

        // Signal 7: FH >1.5 rate — 3.0x lift
        const hFHo15=pctF(hGames,g=>g.fh_o15);
        const aFHo15=pctF(aGames,g=>g.fh_o15);
        prob+=((hFHo15+aFHo15)/2-0.30)*50;

        // Signal 8: H2H
        const key=[h,a].sort().join("|");
        const h2h=(h2hMap[key]||[]).slice(0,6).map(g=>({
          home:g.home_name,away:g.away_name,
          htH:parseInt(g.ht_goals_team_a||0),htA:parseInt(g.ht_goals_team_b||0),
          ftH:parseInt(g.homeGoalCount||0),ftA:parseInt(g.awayGoalCount||0)
        }));
        const h2hFHo15Rate=h2h.length?h2h.filter(g=>g.htH+g.htA>=2).length/h2h.length:0;
        prob+=h2hFHo15Rate*8;

        // Signal 9: Recent FH form
        const hRecentFHo15=pctF(hAll.slice(0,5),g=>g.fh_o15);
        const aRecentFHo15=pctF(aAll.slice(0,5),g=>g.fh_o15);
        prob+=((hRecentFHo15+aRecentFHo15)/2-0.30)*20;

        prob=Math.max(2,Math.min(92,Math.round(prob)));

        // High-impact signal badges (shown on card face)
        const keySignals=[];
        if(combinedBtts>=0.20) keySignals.push({label:"BTTS FH",impact:"critical",val:`${(hBtts*100).toFixed(0)}%/${(aBtts*100).toFixed(0)}%`});
        if(hCn1120>=0.15&&aCn1120>=0.15) keySignals.push({label:"Cn 11-20",impact:"critical",val:`${hCn1120.toFixed(2)}/${aCn1120.toFixed(2)}`});
        if(hSc010>=0.10&&aSc010>=0.10) keySignals.push({label:"Sc 0-10",impact:"strong",val:`${hSc010.toFixed(2)}/${aSc010.toFixed(2)}`});
        if(hFHCn>=halfAvg&&aFHCn>=halfAvg) keySignals.push({label:"FH Conceded",impact:"strong",val:`${hFHCn.toFixed(2)}/${aFHCn.toFixed(2)}`});
        if((hFTo25+aFTo25)/2>=0.60) keySignals.push({label:"FT Chaos",impact:"strong",val:`${(hFTo25*100).toFixed(0)}%/${(aFTo25*100).toFixed(0)}%`});
        if(h2hFHo15Rate>=0.50) keySignals.push({label:"H2H FH",impact:"strong",val:`${h2h.filter(g=>g.htH+g.htA>=2).length}/${h2h.length}`});

        const signals=[
          {label:"FH BTTS rate",hVal:(hBtts*100).toFixed(0)+"%",aVal:(aBtts*100).toFixed(0)+"%",threshold:"20%",hGood:hBtts>=0.20,aGood:aBtts>=0.20,impact:"critical",note:"4.2x lift"},
          {label:"Conceded 11-20/g",hVal:hCn1120.toFixed(3),aVal:aCn1120.toFixed(3),threshold:"0.105",hGood:hCn1120>=0.15,aGood:aCn1120>=0.15,impact:"critical",note:"4.2x lift"},
          {label:"Scored 0-10/g",hVal:hSc010.toFixed(3),aVal:aSc010.toFixed(3),threshold:"0.098",hGood:hSc010>=0.10,aGood:aSc010>=0.10,impact:"strong",note:"2.5x lift"},
          {label:"FH conceded/g",hVal:hFHCn.toFixed(2),aVal:aFHCn.toFixed(2),threshold:halfAvg.toFixed(2),hGood:hFHCn>=halfAvg,aGood:aFHCn>=halfAvg,impact:"strong",note:"2.2x lift"},
          {label:"FH scored/g",hVal:hFHSc.toFixed(2),aVal:aFHSc.toFixed(2),threshold:halfAvg.toFixed(2),hGood:hFHSc>=halfAvg,aGood:aFHSc>=halfAvg,impact:"moderate",note:"3.1x at high end"},
          {label:"FT >2.5 rate",hVal:(hFTo25*100).toFixed(0)+"%",aVal:(aFTo25*100).toFixed(0)+"%",threshold:"52%",hGood:hFTo25>=0.52,aGood:aFTo25>=0.52,impact:"moderate",note:"4.7x at >70%"},
          {label:"FH >1.5 rate",hVal:(hFHo15*100).toFixed(0)+"%",aVal:(aFHo15*100).toFixed(0)+"%",threshold:"30%",hGood:hFHo15>=0.30,aGood:aFHo15>=0.30,impact:"moderate",note:"3.0x lift"},
          {label:"H2H FH ≥2 goals",hVal:`${h2h.filter(g=>g.htH+g.htA>=2).length}/${h2h.length}`,aVal:"",threshold:"",hGood:h2hFHo15Rate>=0.33,aGood:true,impact:"moderate",note:"Matchup pattern"},
          {label:"Recent FH >1.5",hVal:(hRecentFHo15*100).toFixed(0)+"%",aVal:(aRecentFHo15*100).toFixed(0)+"%",threshold:"30%",hGood:hRecentFHo15>=0.30,aGood:aRecentFHo15>=0.30,impact:"moderate",note:"Last 5 games"},
        ];

        const mkChips=games=>games.slice(0,6).map(g=>({fhTotal:g.fh_total,ftTotal:g.ft_total,date:g.date}));

        preds.push({
          league:LEAGUE_NAMES[sid],dt:fixture.date_unix*1000,
          matchDate:fixture._date,
          home:h,away:a,
          expH:+expH.toFixed(2),expA:+expA.toFixed(2),
          expTotal:+expTotal.toFixed(2),
          prob,signals,h2h,keySignals,
          hChips:mkChips([...ht.h,...ht.a].sort((x,y)=>y.date-x.date)),
          aChips:mkChips([...at.h,...at.a].sort((x,y)=>y.date-x.date)),
          halfAvg:+halfAvg.toFixed(2)
        });
      }
    }

    preds.sort((a,b)=>b.prob-a.prob);
    const dates2=getDates();
    res.send(buildHTML(preds,dates2));
  }catch(e){
    console.error(e);
    res.status(500).send("<pre>Error: "+e.message+"</pre>");
  }
});

function buildHTML(preds,dates){
  const badge=p=>p>=35?["HIGH","🔥","#16a34a","#f0fdf4","#bbf7d0"]:
                  p>=20?["MED","⚡","#d97706","#fffbeb","#fde68a"]:
                        ["LOW","❄️","#6b7280","#f9fafb","#e5e7eb"];

  const impactDot=i=>i==="critical"?"🔴":i==="strong"?"🟡":"🔵";

  const keyBadge=s=>`<span style="display:inline-flex;align-items:center;gap:3px;padding:3px 8px;font-size:11px;border-radius:12px;background:${s.impact==="critical"?"#fee2e2":"#fef9c3"};color:${s.impact==="critical"?"#dc2626":"#92400e"};border:1px solid ${s.impact==="critical"?"#fca5a5":"#fde68a"};margin:2px">
    ${impactDot(s.impact)} ${s.label} <strong>${s.val}</strong>
  </span>`;

  const chip=g=>`<span style="display:inline-block;padding:2px 8px;font-size:11px;border-radius:3px;margin:2px;
    border:1px solid ${g.fhTotal>1?"#bbf7d0":"#e5e7eb"};
    background:${g.fhTotal>1?"#f0fdf4":"#f9fafb"};
    color:${g.fhTotal>1?"#16a34a":"#6b7280"}">FH ${g.fhTotal} · FT ${g.ftTotal}</span>`;

  const sigRow=(s,i)=>`<tr style="background:${i%2===0?"#fff":"#f9fafb"}">
    <td style="padding:5px 8px;font-size:13px;color:#374151">
      ${impactDot(s.impact)} ${s.label}
      <span style="font-size:10px;color:#9ca3af;margin-left:4px">${s.note}</span>
    </td>
    <td style="padding:5px 8px;text-align:center;font-size:13px;font-weight:700;color:${s.hGood?"#16a34a":"#dc2626"}">${s.hVal}</td>
    <td style="padding:5px 8px;text-align:center;font-size:11px;color:#9ca3af">${s.threshold}</td>
    <td style="padding:5px 8px;text-align:center;font-size:13px;font-weight:700;color:${s.aVal===""?"#9ca3af":s.aGood?"#16a34a":"#dc2626"}">${s.aVal}</td>
  </tr>`;

  const h2hRow=g=>{const ht=g.htH+g.htA;return`<tr style="border-bottom:1px solid #f3f4f6">
    <td style="padding:4px 8px;font-size:12px;color:#6b7280">${g.home} vs ${g.away}</td>
    <td style="padding:4px 8px;font-size:12px;text-align:center;font-weight:700;color:${ht>=2?"#16a34a":"#374151"}">HT ${g.htH}-${g.htA}</td>
    <td style="padding:4px 8px;font-size:12px;text-align:center;color:#6b7280">FT ${g.ftH}-${g.ftA}</td>
  </tr>`};

  const cards=preds.map(m=>{
    const[label,ico,col,bg,border]=badge(m.prob);
    const dt=new Date(m.dt).toLocaleString("en-GB",{weekday:"short",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"});

    return`<div style="margin-bottom:14px;background:#fff;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05)">
      <div style="padding:14px 16px;border-left:4px solid ${col}">
        <div style="font-size:11px;color:#9ca3af;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">${m.league}</div>
        <div style="display:grid;grid-template-columns:1fr auto;gap:12px;align-items:center">
          <div>
            <div style="font-size:12px;color:#9ca3af;margin-bottom:4px">${dt}</div>
            <div style="font-size:17px;font-weight:700;color:#111827;margin-bottom:6px">
              ${m.home} <span style="color:#d1d5db;font-weight:400;font-size:13px">vs</span> ${m.away}
            </div>
            <div style="font-size:12px;color:#6b7280">
              Poisson exp FH: <strong style="color:#374151">${m.expTotal}</strong>
              (${m.home.split(" ")[0]} ${m.expH} + ${m.away.split(" ")[0]} ${m.expA})
              · League avg: ${m.halfAvg}
            </div>
          </div>
          <div style="text-align:center;min-width:82px;background:${bg};border:1px solid ${border};border-radius:8px;padding:10px 8px">
            <div style="font-size:32px;font-weight:800;color:${col};line-height:1">${m.prob}%</div>
            <div style="font-size:12px;color:${col};margin-top:2px">${ico} ${label}</div>
            <div style="font-size:10px;color:#9ca3af;margin-top:2px">FH OVER 2.5</div>
          </div>
        </div>

        <!-- Key signal badges -->
        ${m.keySignals.length?`<div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:2px">
          ${m.keySignals.map(keyBadge).join("")}
        </div>`:""}

        <details style="margin-top:10px">
          <summary style="cursor:pointer;font-size:12px;color:#6b7280;padding:5px 0;list-style:none;user-select:none">▼ Show all signals</summary>
          <div style="padding-top:10px">
            <table style="width:100%;border-collapse:collapse;margin-bottom:14px;border:1px solid #e5e7eb;border-radius:6px;overflow:hidden">
              <thead><tr style="background:#f3f4f6">
                <th style="padding:5px 8px;text-align:left;font-size:11px;color:#6b7280;font-weight:600">Signal</th>
                <th style="padding:5px 8px;text-align:center;font-size:11px;color:#6b7280;font-weight:600">${m.home.split(" ")[0]}</th>
                <th style="padding:5px 8px;text-align:center;font-size:11px;color:#6b7280;font-weight:600">Threshold</th>
                <th style="padding:5px 8px;text-align:center;font-size:11px;color:#6b7280;font-weight:600">${m.away.split(" ")[0]}</th>
              </tr></thead>
              <tbody>${m.signals.map(sigRow).join("")}</tbody>
            </table>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
              <div>
                <div style="font-size:12px;font-weight:600;color:#374151;margin-bottom:4px">${m.home} recent</div>
                <div>${m.hChips.map(chip).join("")}</div>
              </div>
              <div>
                <div style="font-size:12px;font-weight:600;color:#374151;margin-bottom:4px">${m.away} recent</div>
                <div>${m.aChips.map(chip).join("")}</div>
              </div>
            </div>
            ${m.h2h.length?`<div style="font-size:12px;font-weight:600;color:#374151;margin-bottom:6px">
              H2H — ${m.h2h.filter(g=>g.htH+g.htA>=2).length}/${m.h2h.length} meetings had FH ≥2 goals
            </div>
            <table style="width:100%;border-collapse:collapse">${m.h2h.map(h2hRow).join("")}</table>`:""}
          </div>
        </details>
      </div>
    </div>`;
  }).join("");

  return`<!DOCTYPE html><html><head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <title>FH Over 2.5 Predictor</title>
  <style>*{box-sizing:border-box;margin:0;padding:0}body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827}details>summary::-webkit-details-marker{display:none}</style>
  </head><body>
  <div style="background:#fff;border-bottom:1px solid #e5e7eb;padding:16px 20px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;position:sticky;top:0;z-index:10">
    <div>
      <div style="font-size:11px;color:#6b7280;letter-spacing:1px;text-transform:uppercase;margin-bottom:2px">⚽ FH Over 2.5 Predictor</div>
      <h1 style="font-size:20px;font-weight:800;color:#111827">${dates.join(" · ")}</h1>
    </div>
    <a href="/" style="background:#111827;color:#fff;padding:8px 18px;font-size:13px;text-decoration:none;border-radius:6px;font-weight:600">↺ Refresh</a>
  </div>
  <div style="padding:16px 20px;max-width:760px;margin:0 auto">
    <div style="font-size:11px;color:#6b7280;margin-bottom:6px">${preds.length} matches · sorted by probability</div>
    <div style="font-size:11px;color:#9ca3af;margin-bottom:16px;line-height:1.8">
      🔴 Critical signal (4.2x lift) · 🟡 Strong signal (2.2-2.5x) · 🔵 Moderate signal
    </div>
    ${preds.length===0
      ?`<div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280">No matches found.</div>`
      :cards}
  </div>
  </body></html>`;
}

const PORT=process.env.PORT||3001;
app.listen(PORT,()=>console.log("Server running on port "+PORT));
