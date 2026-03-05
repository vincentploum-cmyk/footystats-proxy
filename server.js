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

// Poisson: P(X >= 3) i.e. FH over 2.5
const pois=lam=>{let p=0,t=Math.exp(-lam);for(let k=0;k<3;k++){p+=t;t*=lam/(k+1)}return(1-p)*100};
const ftch=url=>fetch(url).then(r=>r.json());
const avg=(arr,fn)=>arr.length?arr.reduce((s,x)=>s+fn(x),0)/arr.length:0;
const pct=(arr,fn)=>arr.length?arr.filter(fn).length/arr.length:0;
const pm=t=>{const m=String(t).match(/^(\d+)/);return m?parseInt(m[1]):null};
const win=(ts,a,b)=>ts.filter(t=>t>=a&&t<=b).length;

const getWeekendDates=()=>{
  const today=new Date();
  const daysToSat=(6-today.getDay()+7)%7||7;
  const sat=new Date(today);sat.setDate(today.getDate()+daysToSat);
  const sun=new Date(sat);sun.setDate(sat.getDate()+1);
  return[sat,sun].map(d=>d.toISOString().slice(0,10));
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
    const[sat,sun]=getWeekendDates();
    const[satData,sunData]=await Promise.all([
      ftch(`${BASE}/todays-matches?date=${sat}&key=${KEY}`),
      ftch(`${BASE}/todays-matches?date=${sun}&key=${KEY}`)
    ]);
    const allFixtures=[...(satData.data||[]),...(sunData.data||[])];
    const leagueFixtures={};
    for(const m of allFixtures){
      const sid=m.competition_id;
      if(LEAGUE_NAMES[sid]){
        if(!leagueFixtures[sid])leagueFixtures[sid]=[];
        leagueFixtures[sid].push(m);
      }
    }
    const activeLeagues=Object.keys(leagueFixtures);

    // Fetch history only for leagues with weekend fixtures
    const leagueHistory={};
for(const sid of activeLeagues){
  try{
    const[p1,p2]=await Promise.all([
      ftch(`${BASE}/league-matches?season_id=${sid}&max_per_page=300&page=1&key=${KEY}`),
      ftch(`${BASE}/league-matches?season_id=${sid}&max_per_page=300&page=2&key=${KEY}`)
    ]);
    leagueHistory[sid]=[...(p1.data||[]),...(p2.data||[])].filter(m=>m.status==="complete");
  }catch(e){leagueHistory[sid]=[]}
}

    const preds=[];

    for(const sid of activeLeagues){
      const completed=leagueHistory[sid]||[];
      const fixtures=leagueFixtures[sid]||[];
      if(completed.length<5)continue;

      // Build rich per-team stats including timing windows
      const team={};
      const en=n=>{if(!team[n])team[n]={h:[],a:[]}};
      let totalFH=0,totalGames=0;

      for(const m of completed){
        const ha=parseInt(m.ht_goals_team_a||0),hb=parseInt(m.ht_goals_team_b||0);
        const fa=parseInt(m.homeGoalCount||0),fb=parseInt(m.awayGoalCount||0);
        en(m.home_name);en(m.away_name);
        totalFH+=ha+hb;totalGames++;

        // Parse goal timings
        const hts=[...(m.homeGoals_timings||[])].map(pm).filter(t=>t!==null);
        const ats=[...(m.awayGoals_timings||[])].map(pm).filter(t=>t!==null);
        const fh_hts=hts.filter(t=>t<=45);
        const fh_ats=ats.filter(t=>t<=45);

        const homeRec={
          fh_sc:ha, fh_cn:hb,
          sc_0_10:win(fh_hts,0,10), sc_11_20:win(fh_hts,11,20),
          sc_21_30:win(fh_hts,21,30), sc_31_45:win(fh_hts,31,45),
          cn_0_10:win(fh_ats,0,10), cn_11_20:win(fh_ats,11,20),
          cn_21_30:win(fh_ats,21,30), cn_31_45:win(fh_ats,31,45),
          fh_total:ha+hb, ft_total:fa+fb,
          fh_btts:ha>0&&hb>0, ft_o25:fa+fb>2, ft_o35:fa+fb>3,
          fh_o15:ha+hb>1, fh_o25:ha+hb>2,
          date:m.date_unix
        };
        const awayRec={
          fh_sc:hb, fh_cn:ha,
          sc_0_10:win(fh_ats,0,10), sc_11_20:win(fh_ats,11,20),
          sc_21_30:win(fh_ats,21,30), sc_31_45:win(fh_ats,31,45),
          cn_0_10:win(fh_hts,0,10), cn_11_20:win(fh_hts,11,20),
          cn_21_30:win(fh_hts,21,30), cn_31_45:win(fh_hts,31,45),
          fh_total:ha+hb, ft_total:fa+fb,
          fh_btts:ha>0&&hb>0, ft_o25:fa+fb>2, ft_o35:fa+fb>3,
          fh_o15:ha+hb>1, fh_o25:ha+hb>2,
          date:m.date_unix
        };
        team[m.home_name].h.push(homeRec);
        team[m.away_name].a.push(awayRec);
      }

      // Sort recent
      for(const t of Object.values(team)){
        t.h.sort((a,b)=>b.date-a.date);
        t.a.sort((a,b)=>b.date-a.date);
      }

      const leagueHalfAvg=(totalFH/totalGames)/2||0.5;

      // H2H map
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

        // Use home-specific stats for home team, away-specific for away team
        const hGames=ht.h.slice(0,12);
        const aGames=at.a.slice(0,12);
        // Also get recent all-venue for form
        const hAll=[...ht.h,...ht.a].sort((x,y)=>y.date-x.date).slice(0,8);
        const aAll=[...at.h,...at.a].sort((x,y)=>y.date-x.date).slice(0,8);

        if(hGames.length<3||aGames.length<3)continue;

        // ── CORE FH STATS (home/away specific) ──
        const hFHSc=avg(hGames,g=>g.fh_sc);
        const hFHCn=avg(hGames,g=>g.fh_cn);
        const aFHSc=avg(aGames,g=>g.fh_sc);
        const aFHCn=avg(aGames,g=>g.fh_cn);

        // ── POISSON BASE (Dixon-Coles style) ──
        const halfAvg=leagueHalfAvg;
        const expH=Math.max((hFHSc/halfAvg)*(aFHCn/halfAvg)*halfAvg,0.05);
        const expA=Math.max((aFHSc/halfAvg)*(hFHCn/halfAvg)*halfAvg,0.05);
        const expTotal=expH+expA;
        let prob=pois(expTotal); // base probability from Poisson

        // ── SIGNAL 1: FH BTTS rate (4.2x lift at >30%) ──
        // Data showed: combined BTTS >30% → 50% hit rate (4.2x base)
        const hBtts=pct(hGames,g=>g.fh_btts);
        const aBtts=pct(aGames,g=>g.fh_btts);
        const combinedBtts=(hBtts+aBtts)/2;
        // Scale: 0%=−8, 20%=0, 30%=+10, 40%+=+18
        prob+=(combinedBtts-0.20)*90; // at 0.30 adds +9, at 0.40 adds +18

        // ── SIGNAL 2: cn_11_20 rate (4.2x lift at >0.20/g both) ──
        // Hidden gem from data: both teams conceding in 11-20 window is very predictive
        const hCn1120=avg(hGames,g=>g.cn_11_20);
        const aCn1120=avg(aGames,g=>g.cn_11_20);
        // At 0.20/g both → +15 boost; baseline 0.105/g
        prob+=(hCn1120-0.105)*60+(aCn1120-0.105)*60;

        // ── SIGNAL 3: sc_0_10 rate (2.5x lift at >0.15/g both) ──
        // Teams that score in first 10 mins historically → open games
        const hSc010=avg(hGames,g=>g.sc_0_10);
        const aSc010=avg(aGames,g=>g.sc_0_10);
        // Baseline 0.098/g; at 0.15 → +8, at 0.20 → +14
        prob+=(hSc010-0.098)*80+(aSc010-0.098)*80;

        // ── SIGNAL 4: FH conceded/game (2.2x lift when both >0.7) ──
        // Leaky defenses in FH → more goals for both
        const hCnBoost=(hFHCn-halfAvg)/halfAvg;
        const aCnBoost=(aFHCn-halfAvg)/halfAvg;
        prob+=hCnBoost*8+aCnBoost*8;

        // ── SIGNAL 5: FH scored/game (3.1x lift when both >0.8) ──
        const hScBoost=(hFHSc-halfAvg)/halfAvg;
        const aScBoost=(aFHSc-halfAvg)/halfAvg;
        prob+=hScBoost*6+aScBoost*6;

        // ── SIGNAL 6: FT >2.5 rate (4.7x lift at >70% combined) ──
        // Matches in chaos-prone fixtures overall
        const hFTo25=pct(hGames,g=>g.ft_o25);
        const aFTo25=pct(aGames,g=>g.ft_o25);
        const combinedFTo25=(hFTo25+aFTo25)/2;
        // Baseline 0.517; at 0.70 → +8
        prob+=(combinedFTo25-0.517)*40;

        // ── SIGNAL 7: FH >1.5 historical rate (3.0x lift at >50%) ──
        const hFHo15=pct(hGames,g=>g.fh_o15);
        const aFHo15=pct(aGames,g=>g.fh_o15);
        const combinedFHo15=(hFHo15+aFHo15)/2;
        // Baseline 0.30; at 0.50 → +10
        prob+=(combinedFHo15-0.30)*50;

        // ── SIGNAL 8: sc_31_45 (late FH burst — important for FH>3.5) ──
        // The 31-45 window has most FH goals; teams scoring here late
        const hSc3145=avg(hGames,g=>g.sc_31_45);
        const aSc3145=avg(aGames,g=>g.sc_31_45);
        const baselineSc3145=0.262;
        prob+=(hSc3145-baselineSc3145)*15+(aSc3145-baselineSc3145)*15;

        // ── SIGNAL 9: H2H FH history ──
        const key=[h,a].sort().join("|");
        const h2h=(h2hMap[key]||[]).slice(0,6).map(g=>({
          home:g.home_name,away:g.away_name,
          htH:parseInt(g.ht_goals_team_a||0),htA:parseInt(g.ht_goals_team_b||0),
          ftH:parseInt(g.homeGoalCount||0),ftA:parseInt(g.awayGoalCount||0)
        }));
        const h2hFHo15Rate=h2h.length?h2h.filter(g=>g.htH+g.htA>=2).length/h2h.length:0;
        // H2H adds up to +8 if all meetings had FH >=2
        prob+=h2hFHo15Rate*8;

        // ── SIGNAL 10: Recent FH form (last 5 all venues) ──
        const hRecentFHo15=pct(hAll.slice(0,5),g=>g.fh_o15);
        const aRecentFHo15=pct(aAll.slice(0,5),g=>g.fh_o15);
        prob+=((hRecentFHo15+aRecentFHo15)/2-0.30)*20;

        // Clamp
        prob=Math.max(2,Math.min(92,Math.round(prob)));

        // Build signal display
        const signals=[
          {
            label:"FH BTTS rate",
            hVal:(hBtts*100).toFixed(0)+"%",
            aVal:(aBtts*100).toFixed(0)+"%",
            threshold:"20%",
            hGood:hBtts>=0.20,aGood:aBtts>=0.20,
            note:"4.2x lift at >30% combined"
          },
          {
            label:"Conceded 11-20 min/g",
            hVal:hCn1120.toFixed(3),
            aVal:aCn1120.toFixed(3),
            threshold:"0.105",
            hGood:hCn1120>=0.15,aGood:aCn1120>=0.15,
            note:"4.2x lift when both >0.20 — hidden gem"
          },
          {
            label:"Scored 0-10 min/g",
            hVal:hSc010.toFixed(3),
            aVal:aSc010.toFixed(3),
            threshold:"0.098",
            hGood:hSc010>=0.10,aGood:aSc010>=0.10,
            note:"2.5x lift when both >0.15"
          },
          {
            label:"FH conceded/g",
            hVal:hFHCn.toFixed(2),
            aVal:aFHCn.toFixed(2),
            threshold:halfAvg.toFixed(2)+" (lg avg)",
            hGood:hFHCn>=halfAvg,aGood:aFHCn>=halfAvg,
            note:"2.2x lift when both >0.7"
          },
          {
            label:"FH scored/g",
            hVal:hFHSc.toFixed(2),
            aVal:aFHSc.toFixed(2),
            threshold:halfAvg.toFixed(2)+" (lg avg)",
            hGood:hFHSc>=halfAvg,aGood:aFHSc>=halfAvg,
            note:"3.1x lift when both >0.8"
          },
          {
            label:"FT >2.5 rate",
            hVal:(hFTo25*100).toFixed(0)+"%",
            aVal:(aFTo25*100).toFixed(0)+"%",
            threshold:"52%",
            hGood:hFTo25>=0.52,aGood:aFTo25>=0.52,
            note:"4.7x lift at >70% combined"
          },
          {
            label:"FH >1.5 rate",
            hVal:(hFHo15*100).toFixed(0)+"%",
            aVal:(aFHo15*100).toFixed(0)+"%",
            threshold:"30%",
            hGood:hFHo15>=0.30,aGood:aFHo15>=0.30,
            note:"3.0x lift at >50%"
          },
          {
            label:"Scored 31-45 min/g",
            hVal:hSc3145.toFixed(3),
            aVal:aSc3145.toFixed(3),
            threshold:"0.262",
            hGood:hSc3145>=0.262,aGood:aSc3145>=0.262,
            note:"Late FH burst — important for >3.5"
          },
          {
            label:"H2H FH ≥2 goals",
            hVal:`${h2h.filter(g=>g.htH+g.htA>=2).length}/${h2h.length}`,
            aVal:"",
            threshold:"",
            hGood:h2hFHo15Rate>=0.33,aGood:true,
            note:"Historical matchup pattern"
          },
          {
            label:"Recent FH >1.5 (last 5)",
            hVal:(hRecentFHo15*100).toFixed(0)+"%",
            aVal:(aRecentFHo15*100).toFixed(0)+"%",
            threshold:"30%",
            hGood:hRecentFHo15>=0.30,aGood:aRecentFHo15>=0.30,
            note:"Current form trend"
          },
        ];

        // Recent chips for display
        const mkChips=games=>games.slice(0,6).map(g=>({
          fhTotal:g.fh_total,ftTotal:g.ft_total,
          sc010:g.sc_0_10,cn1120:g.cn_11_20,date:g.date
        }));

        preds.push({
          league:LEAGUE_NAMES[sid],dt:fixture.date_unix*1000,
          home:h,away:a,
          expH:+expH.toFixed(2),expA:+expA.toFixed(2),
          expTotal:+expTotal.toFixed(2),
          prob,signals,h2h,
          hChips:mkChips([...ht.h,...ht.a].sort((x,y)=>y.date-x.date)),
          aChips:mkChips([...at.h,...at.a].sort((x,y)=>y.date-x.date)),
          halfAvg:+halfAvg.toFixed(2)
        });
      }
    }

    preds.sort((a,b)=>b.prob-a.prob);
    res.send(buildHTML(preds,sat,sun));
  }catch(e){
    console.error(e);
    res.status(500).send("<pre>Error: "+e.message+"</pre>");
  }
});

function buildHTML(preds,sat,sun){
  const badge=p=>p>=35?["HIGH","🔥","#16a34a","#f0fdf4","#bbf7d0"]:
                  p>=20?["MED","⚡","#d97706","#fffbeb","#fde68a"]:
                        ["LOW","❄️","#6b7280","#f9fafb","#e5e7eb"];

  const chip=g=>{
    const fhGood=g.fhTotal>1;
    const ftGood=g.ftTotal>2;
    return`<span title="FT: ${g.ftTotal} goals | 0-10 scored: ${g.sc010} | 11-20 conceded: ${g.cn1120}"
      style="display:inline-block;padding:2px 8px;font-size:12px;border-radius:3px;margin:2px;
      border:1px solid ${fhGood?"#bbf7d0":"#e5e7eb"};
      background:${fhGood?"#f0fdf4":"#f9fafb"};
      color:${fhGood?"#16a34a":"#6b7280"}">FH ${g.fhTotal} · FT ${g.ftTotal}</span>`;
  };

  const sigRow=(s,i)=>{
    const bg=i%2===0?"#fff":"#f9fafb";
    const hColor=s.hGood?"#16a34a":"#dc2626";
    const aColor=s.aVal===""?"":s.aGood?"#16a34a":"#dc2626";
    return`<tr style="background:${bg}">
      <td style="padding:5px 8px;font-size:13px;color:#374151">${s.label}</td>
      <td style="padding:5px 8px;text-align:center;font-size:13px;font-weight:700;color:${hColor}">${s.hVal}</td>
      <td style="padding:5px 8px;text-align:center;font-size:12px;color:#9ca3af">${s.threshold}</td>
      <td style="padding:5px 8px;text-align:center;font-size:13px;font-weight:700;color:${aColor}">${s.aVal}</td>
      <td style="padding:5px 8px;font-size:12px;color:#9ca3af;display:none" class="note">${s.note}</td>
    </tr>`;
  };

  const h2hRow=g=>{
    const ht=g.htH+g.htA;
    return`<tr style="border-bottom:1px solid #f3f4f6">
      <td style="padding:4px 8px;font-size:13px;color:#6b7280">${g.home} vs ${g.away}</td>
      <td style="padding:4px 8px;font-size:13px;text-align:center;font-weight:700;color:${ht>=2?"#16a34a":"#374151"}">HT ${g.htH}-${g.htA}</td>
      <td style="padding:4px 8px;font-size:13px;text-align:center;color:#6b7280">FT ${g.ftH}-${g.ftA}</td>
    </tr>`;
  };

  const cards=preds.map(m=>{
    const[label,ico,col,bg,border]=badge(m.prob);
    const dt=new Date(m.dt).toLocaleString("en-GB",{weekday:"short",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"});
    const goodSignals=m.signals.filter(s=>s.hGood&&(s.aVal===""||s.aGood)).length;
    const totalSignals=m.signals.length;

    return`<div style="margin-bottom:12px;background:#fff;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05)">
      <div style="padding:14px 16px;border-left:4px solid ${col}">

        <!-- Header -->
        <div style="font-size:12px;color:#9ca3af;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">${m.league}</div>
        <div style="display:grid;grid-template-columns:1fr auto;gap:12px;align-items:center">
          <div>
            <div style="font-size:13px;color:#9ca3af;margin-bottom:4px">${dt}</div>
            <div style="font-size:16px;font-weight:700;color:#111827;margin-bottom:6px">
              ${m.home} <span style="color:#d1d5db;font-weight:400;font-size:12px">vs</span> ${m.away}
            </div>
            <div style="font-size:13px;color:#6b7280">
              Poisson exp FH: <strong style="color:#374151">${m.expTotal}</strong>
              (${m.home.split(" ")[0]} ${m.expH} + ${m.away.split(" ")[0]} ${m.expA})
              · League avg FH/team: ${m.halfAvg}
            </div>
            <div style="margin-top:6px;font-size:12px;color:#6b7280">
              Signals positive: <strong style="color:${goodSignals>=7?"#16a34a":goodSignals>=5?"#d97706":"#dc2626"}">${goodSignals}/${totalSignals}</strong>
            </div>
          </div>
          <div style="text-align:center;min-width:82px;background:${bg};border:1px solid ${border};border-radius:8px;padding:10px 8px">
            <div style="font-size:30px;font-weight:800;color:${col};line-height:1">${m.prob}%</div>
            <div style="font-size:13px;color:${col};margin-top:2px">${ico} ${label}</div>
            <div style="font-size:11px;color:#9ca3af;margin-top:2px">FH OVER 2.5</div>
          </div>
        </div>

        <!-- Expand -->
        <details style="margin-top:10px">
          <summary style="cursor:pointer;font-size:13px;color:#6b7280;padding:5px 0;list-style:none;user-select:none">
            ▼ Show all 10 signals
          </summary>
          <div style="padding-top:10px">

            <!-- Signal table -->
            <table style="width:100%;border-collapse:collapse;margin-bottom:14px;border:1px solid #e5e7eb;border-radius:6px;overflow:hidden">
              <thead>
                <tr style="background:#f3f4f6">
                  <th style="padding:5px 8px;text-align:left;font-size:12px;color:#6b7280;font-weight:600">Signal</th>
                  <th style="padding:5px 8px;text-align:center;font-size:12px;color:#6b7280;font-weight:600">${m.home.split(" ")[0]}</th>
                  <th style="padding:5px 8px;text-align:center;font-size:12px;color:#6b7280;font-weight:600">Threshold</th>
                  <th style="padding:5px 8px;text-align:center;font-size:12px;color:#6b7280;font-weight:600">${m.away.split(" ")[0]}</th>
                </tr>
              </thead>
              <tbody>${m.signals.map(sigRow).join("")}</tbody>
            </table>

            <!-- Recent form -->
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
              <div>
                <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:4px">${m.home} recent</div>
                <div>${m.hChips.map(chip).join("")}</div>
              </div>
              <div>
                <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:4px">${m.away} recent</div>
                <div>${m.aChips.map(chip).join("")}</div>
              </div>
            </div>

            <!-- H2H -->
            ${m.h2h.length?`
            <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:6px">
              H2H — ${m.h2h.filter(g=>g.htH+g.htA>=2).length}/${m.h2h.length} meetings had FH ≥2 goals
            </div>
            <table style="width:100%;border-collapse:collapse">
              ${m.h2h.map(h2hRow).join("")}
            </table>`:""}

          </div>
        </details>
      </div>
    </div>`;
  }).join("");

  return`<!DOCTYPE html><html><head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>FH Over 2.5 Predictor</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827}
    details>summary::-webkit-details-marker{display:none}
  </style>
  </head><body>
  <div style="background:#fff;border-bottom:1px solid #e5e7eb;padding:16px 20px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;position:sticky;top:0;z-index:10">
    <div>
      <div style="font-size:12px;color:#6b7280;letter-spacing:1px;text-transform:uppercase;margin-bottom:2px">⚽ FH Over 2.5 Predictor · Data-Driven Model</div>
      <h1 style="font-size:20px;font-weight:800;color:#111827">${sat} &amp; ${sun}</h1>
    </div>
    <a href="/" style="background:#111827;color:#fff;padding:8px 18px;font-size:12px;text-decoration:none;border-radius:6px;font-weight:600">↺ Refresh</a>
  </div>
  <div style="padding:16px 20px;max-width:760px;margin:0 auto">
    <div style="font-size:13px;color:#6b7280;margin-bottom:6px">${preds.length} matches · sorted by probability</div>
    <div style="font-size:12px;color:#9ca3af;margin-bottom:16px;line-height:1.7">
      Model trained on 659 matches across EPL, La Liga, Ligue 1, MLS.
      Top signals: <strong style="color:#374151">FH BTTS rate</strong> (4.2x lift) ·
      <strong style="color:#374151">Conceded 11-20 min</strong> (4.2x) ·
      <strong style="color:#374151">Scored 0-10 min</strong> (2.5x) ·
      <strong style="color:#374151">FT >2.5 rate</strong> (4.7x at high threshold) ·
      Dropped: xG, dangerous attacks, corners (all &lt;1.1x lift in data)
    </div>
    ${preds.length===0
      ?`<div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280">No matches found this weekend.</div>`
      :cards}
  </div>
  </body></html>`;
}

const PORT=process.env.PORT||3001;
app.listen(PORT,()=>console.log("Server running on port "+PORT));
