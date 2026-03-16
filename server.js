function buildHTML(preds, dates) {
  const predsJSON = JSON.stringify(preds)
    .replace(/</g, "\\u003c")
    .replace(/>/g, "\\u003e");
  const datesJSON = JSON.stringify(dates)
    .replace(/</g, "\\u003c")
    .replace(/>/g, "\\u003e");

  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>FH Over 2.5 Rank</title>
  <script>
    (function () {
      var p = new URLSearchParams(window.location.search);
      if (!p.has("tz")) {
        p.set("tz", -new Date().getTimezoneOffset());
        window.location.search = p.toString();
      }
    })();
  </script>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;font-size:15px}
    details>summary::-webkit-details-marker{display:none}
    .tab{padding:8px 14px;border-radius:6px;cursor:pointer;font-size:14px;font-weight:600;border:1px solid #e5e7eb;background:#fff;color:#6b7280;transition:all .15s}
    .tab.active{background:#111827;color:#fff;border-color:#111827}
    .league-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:16px 18px;margin-bottom:10px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;box-shadow:0 1px 3px rgba(0,0,0,.05);transition:box-shadow .15s}
    .league-card:hover{box-shadow:0 3px 8px rgba(0,0,0,.1)}
    .back-btn{background:#f3f4f6;border:1px solid #e5e7eb;padding:7px 16px;border-radius:6px;cursor:pointer;font-size:15px;font-weight:600;color:#374151}
    .match-card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:14px}
    .mini-table{width:100%;border-collapse:collapse;font-size:12px}
    .mini-table th{background:#f9fafb;padding:6px 8px;text-align:left;font-size:10px;color:#6b7280;font-weight:700;text-transform:uppercase;border-bottom:1px solid #e5e7eb}
    .mini-table td{padding:7px 8px;border-bottom:1px solid #f3f4f6}
  </style>
</head>
<body>
  <div style="background:#fff;border-bottom:1px solid #e5e7eb;padding:14px 20px;position:sticky;top:0;z-index:10">
    <div style="max-width:920px;margin:0 auto">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
        <div>
          <div style="font-size:11px;color:#6b7280;letter-spacing:1px;text-transform:uppercase">&#9917; First Half Over 2.5</div>
          <h1 style="font-size:22px;font-weight:800;color:#111827" id="headerTitle">Loading...</h1>
        </div>
        <button onclick="location.reload()" style="background:#111827;color:#fff;padding:8px 16px;font-size:14px;border:none;border-radius:6px;font-weight:600;cursor:pointer">&#8635; Refresh</button>
      </div>
      <div id="dayTabs" style="display:flex;gap:8px;flex-wrap:wrap"></div>
    </div>
  </div>

  <div style="padding:16px 20px;max-width:920px;margin:0 auto">
    <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#92400e;line-height:1.6">
      <strong>Simple model:</strong> every match gets a rank from <strong>1 to 5</strong>.
      A rank of <strong>4 or 5</strong> is treated as eligible for first-half over 2.5.
      The displayed rank is frozen to <strong>pre-match data only</strong> so you can audit it after the match ends.
    </div>
    <div id="mainView"></div>
  </div>

  <script>
    const ALL_PREDS = ${predsJSON};
    const DATES = ${datesJSON};
    const DAY_LABELS = ["Today", "Tomorrow", "Day 3", "Day 4", "Day 5", "Day 6"];

    let activeDate = DATES[0] || null;
    let activeLeague = null;

    function fmt(d) {
      return new Date(d).toLocaleDateString("en-GB", {
        weekday: "long",
        day: "2-digit",
        month: "short"
      });
    }

    function esc(s) {
      return String(s ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function rankColor(rank) {
      if (rank === 5) return "#16a34a";
      if (rank === 4) return "#65a30d";
      if (rank === 3) return "#d97706";
      if (rank === 2) return "#f59e0b";
      return "#6b7280";
    }

    function rankBg(rank) {
      if (rank === 5) return "#f0fdf4";
      if (rank === 4) return "#f7fee7";
      if (rank === 3) return "#fffbeb";
      if (rank === 2) return "#fffbeb";
      return "#f9fafb";
    }

    function rankBorder(rank) {
      if (rank === 5) return "#bbf7d0";
      if (rank === 4) return "#d9f99d";
      if (rank === 3) return "#fde68a";
      if (rank === 2) return "#fcd34d";
      return "#e5e7eb";
    }

    function renderTabs() {
      const el = document.getElementById("dayTabs");
      let html = "";

      for (let i = 0; i < DATES.length; i++) {
        const d = DATES[i];
        const count = ALL_PREDS.filter((p) => p.matchDate === d).length;
        const cls = d === activeDate ? "tab active" : "tab";

        html += \`<button class="\${cls}" data-day-index="\${i}">
          \${DAY_LABELS[i] || d}
          <span style="font-size:12px;opacity:.7">(\${count})</span>
        </button>\`;
      }

      el.innerHTML = html;

      el.querySelectorAll("[data-day-index]").forEach((btn) => {
        btn.addEventListener("click", () => {
          const i = Number(btn.getAttribute("data-day-index"));
          activeDate = DATES[i];
          activeLeague = null;
          renderTabs();
          renderLeagueList();
          document.getElementById("headerTitle").textContent = fmt(new Date(DATES[i] + "T12:00:00"));
        });
      });
    }

    function renderLeagueList() {
      const main = document.getElementById("mainView");

      if (!activeDate) {
        main.innerHTML = '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280">No matches found.</div>';
        return;
      }

      const dayPreds = ALL_PREDS.filter((p) => p.matchDate === activeDate);
      const leagueMap = {};

      for (const p of dayPreds) {
        if (!leagueMap[p.league]) leagueMap[p.league] = [];
        leagueMap[p.league].push(p);
      }

      const leagueList = Object.entries(leagueMap).sort((a, b) => {
        const aTop = Math.max(...a[1].map((p) => p.rank * 100 + p.rawScore));
        const bTop = Math.max(...b[1].map((p) => p.rank * 100 + p.rawScore));
        return bTop - aTop;
      });

      if (!leagueList.length) {
        main.innerHTML = '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:40px;text-align:center;color:#6b7280">No matches found for this day.</div>';
        return;
      }

      let html = \`<div style="font-size:13px;color:#6b7280;margin-bottom:12px">
        \${dayPreds.length} matches across \${leagueList.length} leagues &middot; sorted by strongest rank
      </div>\`;

      for (const [league, matches] of leagueList) {
        const topRank = Math.max(...matches.map((p) => p.rank));
        const eligibleCount = matches.filter((p) => p.eligible).length;
        const col = rankColor(topRank);

        html += \`<div class="league-card" data-league="\${esc(league)}">
          <div style="flex:1;min-width:0;margin-right:12px">
            <div style="font-size:18px;font-weight:700;color:#111827;margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">\${esc(league)}</div>
            <div style="font-size:13px;color:#6b7280">\${matches.length} match\${matches.length > 1 ? "es" : ""} &middot; \${eligibleCount} eligible</div>
          </div>
          <div style="text-align:right;flex-shrink:0">
            <div style="font-size:28px;font-weight:800;color:\${col}">\${topRank}/5</div>
            <div style="font-size:11px;color:#9ca3af;margin-top:1px">top rank</div>
          </div>
        </div>\`;
      }

      main.innerHTML = html;

      main.querySelectorAll("[data-league]").forEach((el) => {
        el.addEventListener("click", () => {
          activeLeague = el.getAttribute("data-league");
          renderMatchList();
        });
      });
    }

    function gamesTable(games, title) {
      if (!games || !games.length) return "";

      let rows = "";
      for (const g of games) {
        const htTot = (g.htFor || 0) + (g.htAgainst || 0);
        rows += \`<tr>
          <td>\${esc(g.date)}</td>
          <td>\${esc(g.venue)}</td>
          <td>\${esc(g.opp)}</td>
          <td>\${g.htFor}-\${g.htAgainst} (\${htTot})</td>
          <td>\${g.ftFor}-\${g.ftAgainst}</td>
        </tr>\`;
      }

      return \`<div style="margin-top:12px">
        <div style="font-size:12px;font-weight:700;color:#374151;margin-bottom:6px">\${title}</div>
        <table class="mini-table">
          <thead>
            <tr><th>Date</th><th>H/A</th><th>Opponent</th><th>HT</th><th>FT</th></tr>
          </thead>
          <tbody>\${rows}</tbody>
        </table>
      </div>\`;
    }

    function h2hTable(matches) {
      if (!matches || !matches.length) {
        return '<div style="margin-top:12px;font-size:12px;color:#6b7280">No recent H2H found.</div>';
      }

      let rows = "";
      for (const m of matches) {
        const ht = \`\${parseInt(m.ht_goals_team_a || 0, 10)}-\${parseInt(m.ht_goals_team_b || 0, 10)}\`;
        const ft = \`\${parseInt(m.homeGoalCount || 0, 10)}-\${parseInt(m.awayGoalCount || 0, 10)}\`;
        const date = m.date_unix ? new Date(m.date_unix * 1000).toISOString().slice(0, 10) : "";

        rows += \`<tr>
          <td>\${esc(date)}</td>
          <td>\${esc(m.home_name)}</td>
          <td>\${esc(m.away_name)}</td>
          <td>\${ht}</td>
          <td>\${ft}</td>
        </tr>\`;
      }

      return \`<div style="margin-top:12px">
        <div style="font-size:12px;font-weight:700;color:#374151;margin-bottom:6px">H2H</div>
        <table class="mini-table">
          <thead>
            <tr><th>Date</th><th>Home</th><th>Away</th><th>HT</th><th>FT</th></tr>
          </thead>
          <tbody>\${rows}</tbody>
        </table>
      </div>\`;
    }

    function renderMatchCard(m) {
      const col = rankColor(m.rank);
      const bg = rankBg(m.rank);
      const br = rankBorder(m.rank);
      const dt = m.dt
        ? new Date(m.dt).toLocaleString("en-GB", {
            weekday: "short",
            day: "2-digit",
            month: "short",
            hour: "2-digit",
            minute: "2-digit"
          })
        : m.matchDate;

      const badgeText = m.eligible ? "Eligible" : "Not eligible";
      const warnStr = m.missingStats
        ? '<span style="background:#fef3c7;color:#92400e;font-size:11px;padding:2px 7px;border-radius:4px;margin-left:8px;font-weight:600">&#9888; missing stats</span>'
        : "";

      let html = \`<div class="match-card" style="border-left:4px solid \${col}">
        <div style="padding:16px">
          <div style="display:grid;grid-template-columns:1fr auto;gap:12px;align-items:start">
            <div>
              <div style="font-size:12px;color:#9ca3af;margin-bottom:4px">\${esc(dt)}\${warnStr}</div>
              <div style="font-size:20px;font-weight:800;color:#111827;margin-bottom:10px">\${esc(m.home)} <span style="color:#d1d5db;font-weight:500">vs</span> \${esc(m.away)}</div>

              <div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-bottom:10px">
                <div style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:10px">
                  <div style="font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700;margin-bottom:3px">Home FH</div>
                  <div style="font-size:13px;font-weight:700;color:#111827">Scored \${m.hAvgFH.scoredHome}</div>
                  <div style="font-size:13px;font-weight:700;color:#dc2626">Conceded \${m.hAvgFH.concededHome}</div>
                </div>

                <div style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:10px">
                  <div style="font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700;margin-bottom:3px">Away FH</div>
                  <div style="font-size:13px;font-weight:700;color:#111827">Scored \${m.aAvgFH.scoredAway}</div>
                  <div style="font-size:13px;font-weight:700;color:#dc2626">Conceded \${m.aAvgFH.concededAway}</div>
                </div>

                <div style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:10px">
                  <div style="font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700;margin-bottom:3px">Decision</div>
                  <div style="font-size:13px;font-weight:800;color:\${m.eligible ? "#15803d" : "#6b7280"}">\${badgeText}</div>
                  <div style="font-size:12px;color:#6b7280;margin-top:2px">4/5 or 5/5 only</div>
                </div>
              </div>

              <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px">
                <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px">
                  <div style="font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700">Season env</div>
                  <div style="font-size:22px;font-weight:800;color:#111827">\${m.seasonEnv.toFixed(2)}</div>
                </div>

                <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px">
                  <div style="font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700">Recent last 5</div>
                  <div style="font-size:22px;font-weight:800;color:#111827">\${m.recentEnv.val.toFixed(2)}</div>
                  <div style="font-size:11px;color:#6b7280">\${m.recentEnv.home.toFixed(2)} / \${m.recentEnv.away.toFixed(2)}</div>
                </div>

                <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px">
                  <div style="font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700">Venue form</div>
                  <div style="font-size:22px;font-weight:800;color:#111827">\${m.venueEnv.val.toFixed(2)}</div>
                  <div style="font-size:11px;color:#6b7280">\${m.venueEnv.home.toFixed(2)} / \${m.venueEnv.away.toFixed(2)}</div>
                </div>

                <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px">
                  <div style="font-size:10px;color:#9ca3af;text-transform:uppercase;font-weight:700">H2H</div>
                  <div style="font-size:22px;font-weight:800;color:#111827">\${m.h2hEnv.val == null ? "-" : m.h2hEnv.val.toFixed(2)}</div>
                  <div style="font-size:11px;color:#6b7280">\${m.h2hEnv.count ? m.h2hEnv.count + " match(es)" : "none"}</div>
                </div>
              </div>
            </div>

            <div style="text-align:center;min-width:110px;background:\${bg};border:1px solid \${br};border-radius:10px;padding:12px 8px">
              <div style="font-size:36px;font-weight:900;color:\${col};line-height:1">\${m.rank}/5</div>
              <div style="font-size:13px;color:\${col};font-weight:700;margin-top:4px">\${esc(m.rankLabel)}</div>
              <div style="font-size:11px;color:#6b7280;margin-top:6px">Raw \${m.rawScore.toFixed(2)}</div>
              <div style="font-size:11px;color:#6b7280;margin-top:4px">\${m.eligible ? "Eligible" : "No"}</div>
            </div>
          </div>\`;

      if (m.status === "complete") {
        html += \`<div style="margin-top:10px;padding:10px 12px;border-radius:8px;background:#f9fafb;border:1px solid #e5e7eb;font-size:13px;color:#374151">
          Actual result &mdash; FH: <strong>\${m.fhH}-\${m.fhA}</strong> &middot; FT: <strong>\${m.ftH}-\${m.ftA}</strong>
        </div>\`;
      }

      html += \`<details style="margin-top:12px">
        <summary style="font-size:13px;color:#6b7280;padding:4px 0;border-top:1px solid #f3f4f6;padding-top:10px">&#9660; Match history</summary>
        <div style="padding-top:8px">
          \${gamesTable(m.hLast5 || [], "Home team &mdash; last 5 overall")}
          \${gamesTable(m.aLast5 || [], "Away team &mdash; last 5 overall")}
          \${h2hTable(m.h2h || [])}
        </div>
      </details>
    </div></div>\`;

      return html;
    }

    function renderMatchList() {
      const matches = ALL_PREDS
        .filter((p) => p.matchDate === activeDate && p.league === activeLeague)
        .sort((a, b) => b.rank - a.rank || b.rawScore - a.rawScore);

      let html = '<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">';
      html += '<button class="back-btn" id="backBtn">&#8592; Back</button>';
      html += '<div style="font-size:19px;font-weight:700;color:#111827">' + esc(activeLeague) + "</div></div>";

      for (const m of matches) html += renderMatchCard(m);

      document.getElementById("mainView").innerHTML = html;

      const backBtn = document.getElementById("backBtn");
      if (backBtn) {
        backBtn.addEventListener("click", () => {
          activeLeague = null;
          renderLeagueList();
        });
      }
    }

    if (DATES.length) {
      document.getElementById("headerTitle").textContent = fmt(new Date(DATES[0] + "T12:00:00"));
    } else {
      document.getElementById("headerTitle").textContent = "No matches found";
    }

    renderTabs();
    renderLeagueList();
  </script>
</body>
</html>`;
}
