require("dotenv").config();

const express = require("express");
const cors    = require("cors");
const fetch   = require("node-fetch");

const app  = express();
app.use(cors());

const KEY  = process.env.FOOTY_API_KEY;
const BASE = "https://api.football-data-api.com";
const PORT = process.env.PORT || 3001;

if (!KEY) { console.error("Missing FOOTY_API_KEY"); process.exit(1); }

// ─── SUPABASE CLIENT ─────────────────────────────────────────────────────────
// Optional. When SUPABASE_URL + SUPABASE_ANON_KEY (or SUPABASE_SERVICE_ROLE_KEY)
// are set, completed matches are persisted to the match_results table on each
// /preds call. Apply supabase/schema.sql in the Supabase SQL editor first.
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY || "";
let supabase = null;
if (SUPABASE_URL && SUPABASE_KEY) {
  try {
    const { createClient } = require("@supabase/supabase-js");
    supabase = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });
    console.log("Supabase enabled (" + SUPABASE_URL.replace(/^https?:\/\//, "").split(".")[0] + ")");
  } catch (e) {
    console.error("Supabase init failed: " + e.message);
    supabase = null;
  }
} else {
  console.log("Supabase disabled (set SUPABASE_URL + SUPABASE_ANON_KEY to enable)");
}
const SUPABASE_PERSISTED_IDS = new Set();
const SUPABASE_EARLY_PERSISTED_IDS = new Set();
let SUPABASE_LAST_PERSIST = { at: 0, attempted: 0, written: 0, error: null };
let SUPABASE_LAST_EARLY  = { at: 0, attempted: 0, written: 0, error: null };

function buildSnapRow(p) {
  return {
    match_id: p.id,
    competition_id: p.leagueSid || null,
    league_name: p.league || null,
    home_id: p.homeId || null,
    away_id: p.awayId || null,
    home_name: p.home || null,
    away_name: p.away || null,
    date_unix: p.dt ? Math.floor(p.dt / 1000) : null,
    rank: p.rank || 0,
    ci: p.ci || 0,
    def_ci: p.defCi || 0,
    prob25: p.prob25 || 0,
    prob15: p.prob15 || 0,
    signals: p.signals || {},
    snap: p.snap || null,
  };
}

// Persist pre-game snapshots immediately so they survive Render restarts.
// Uses upsert with ignoreDuplicates so existing rows are NOT overwritten —
// the FIRST observation's snap is preserved (this is the freeze).
async function persistEarlySnapshots(preds) {
  if (!supabase || !Array.isArray(preds) || preds.length === 0) return;
  const rows = [];
  for (const p of preds) {
    if (!p || !p.id) continue;
    if (p.matchResult) continue;            // completed → other path
    if (!p.snap) continue;                  // no stats → nothing to freeze
    if (SUPABASE_EARLY_PERSISTED_IDS.has(p.id)) continue;
    if (SUPABASE_PERSISTED_IDS.has(p.id))    continue;
    rows.push(buildSnapRow(p));
  }
  if (rows.length === 0) return;
  try {
    const { error } = await supabase
      .from("match_results")
      .upsert(rows, { onConflict: "match_id", ignoreDuplicates: true });
    if (error) {
      SUPABASE_LAST_EARLY = { at: Date.now(), attempted: rows.length, written: 0, error: error.message };
      console.error("Early snapshot persist failed: " + error.message);
      return;
    }
    for (const r of rows) SUPABASE_EARLY_PERSISTED_IDS.add(r.match_id);
    SUPABASE_LAST_EARLY = { at: Date.now(), attempted: rows.length, written: rows.length, error: null };
    console.log("Supabase: froze " + rows.length + " pre-game snapshot" + (rows.length === 1 ? "" : "s"));
  } catch (e) {
    SUPABASE_LAST_EARLY = { at: Date.now(), attempted: rows.length, written: 0, error: e.message };
    console.error("Early snapshot exception: " + e.message);
  }
}

async function persistCompletedPreds(preds) {
  if (!supabase || !Array.isArray(preds) || preds.length === 0) return;
  const completed = [];
  for (const p of preds) {
    if (!p || !p.id || !p.matchResult) continue;
    if (SUPABASE_PERSISTED_IDS.has(p.id)) continue;
    completed.push(p);
  }
  if (completed.length === 0) return;
  try {
    // Step 1: insert full row for matches that have NO existing row (ignoreDuplicates).
    // Skips matches that were already persisted early — preserving their pre-game snap.
    const insertRows = completed.map(p => ({
      ...buildSnapRow(p),
      ht_home: p.matchResult.fhH,
      ht_away: p.matchResult.fhA,
      ft_home: p.matchResult.ftH,
      ft_away: p.matchResult.ftA,
      fh_total: (p.matchResult.fhH || 0) + (p.matchResult.fhA || 0),
      hit_15: !!p.matchResult.hit15,
      hit_25: !!p.matchResult.hit25,
    }));
    const insErr = (await supabase.from("match_results").upsert(insertRows, { onConflict: "match_id", ignoreDuplicates: true })).error;
    if (insErr) console.error("Completed insert error: " + insErr.message);
    // Step 2: update result fields on every row (touches both new + existing rows).
    // Crucially does NOT touch snap/signals/rank — preserving the early-frozen values.
    let written = 0;
    let lastErr = null;
    await Promise.all(completed.map(async (p) => {
      const { error } = await supabase.from("match_results").update({
        ht_home: p.matchResult.fhH,
        ht_away: p.matchResult.fhA,
        ft_home: p.matchResult.ftH,
        ft_away: p.matchResult.ftA,
        fh_total: (p.matchResult.fhH || 0) + (p.matchResult.fhA || 0),
        hit_15: !!p.matchResult.hit15,
        hit_25: !!p.matchResult.hit25,
      }).eq("match_id", p.id);
      if (error) { lastErr = error.message; return; }
      written++;
      SUPABASE_PERSISTED_IDS.add(p.id);
    }));
    SUPABASE_LAST_PERSIST = { at: Date.now(), attempted: completed.length, written, error: lastErr };
    if (written) console.log("Supabase: completed " + written + " match" + (written === 1 ? "" : "es"));
    if (lastErr) console.error("Completed update error: " + lastErr);
  } catch (e) {
    SUPABASE_LAST_PERSIST = { at: Date.now(), attempted: completed.length, written: 0, error: e.message };
    console.error("Completed persist exception: " + e.message);
  }
}

// Rehydrate CI_SNAPSHOT_CACHE from Supabase on startup. Without this, a Render
// restart loses every in-memory frozen pre-match snapshot, so completed matches
// would be recomputed from current stats instead of showing their frozen view.
async function loadFrozenSnapshots() {
  if (!supabase) return;
  try {
    const cutoff = Math.floor(Date.now() / 1000) - 30 * 24 * 60 * 60;
    const { data, error } = await supabase
      .from("match_results")
      .select("match_id, rank, ci, def_ci, prob25, prob15, signals, snap")
      .not("snap", "is", null)
      .gte("date_unix", cutoff);
    if (error) throw error;
    let n = 0;
    for (const r of (data || [])) {
      if (!r.match_id || CI_SNAPSHOT_CACHE[r.match_id]) continue;
      const rank = r.rank || 0;
      CI_SNAPSHOT_CACHE[r.match_id] = {
        ci: Number(r.ci) || 0, defCi: Number(r.def_ci) || 0,
        rank, label: RANK_LABELS[rank] || "Low",
        prob25: Number(r.prob25) || 0, prob15: Number(r.prob15) || 0,
        probSource: "frozen", probSampleN: 0, probCombo: null,
        eligible: rank >= 2, signals: r.signals || {}, snap: r.snap || null,
      };
      n++;
    }
    console.log("Rehydrated " + n + " frozen snapshot" + (n === 1 ? "" : "s") + " from Supabase");
  } catch (e) {
    console.error("loadFrozenSnapshots: " + e.message);
  }
}

// ─── PHASE 2: PER-LEAGUE PROBABILITY TABLES ──────────────────────────────────
// Cache shape: { "<compId>:<rank>": { n, prob25, prob15 } }
// Populated from league_prob_tables on startup + every CACHE_TTL_MS.
// Recalibration: calls the compute_league_prob_buckets() RPC, upserts results.
const LEAGUE_PROB_MIN_N      = 30;                  // min sample size for league override
const LEAGUE_PROB_CACHE_TTL  = 6 * 60 * 60 * 1000;  // 6h
const LEAGUE_PROB_RECAL_TTL  = 24 * 60 * 60 * 1000; // 24h
let   LEAGUE_PROB_CACHE      = {};
let   LEAGUE_PROB_LAST_LOAD  = { at: 0, rows: 0, error: null };
let   LEAGUE_PROB_LAST_RECAL = { at: 0, buckets: 0, written: 0, error: null };

async function loadLeagueProbCache() {
  if (!supabase) return;
  try {
    const next = {};
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("league_prob_tables")
        .select("competition_id, rank, n, prob25, prob15")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      for (const r of data) {
        next[r.competition_id + ":" + r.rank] = { n: r.n, prob25: Number(r.prob25), prob15: Number(r.prob15) };
      }
      if (data.length < PAGE) break;
    }
    LEAGUE_PROB_CACHE = next;
    LEAGUE_PROB_LAST_LOAD = { at: Date.now(), rows: Object.keys(next).length, error: null };
    console.log("League prob cache loaded: " + LEAGUE_PROB_LAST_LOAD.rows + " rows");
  } catch (e) {
    LEAGUE_PROB_LAST_LOAD = { at: Date.now(), rows: Object.keys(LEAGUE_PROB_CACHE).length, error: e.message };
    console.error("League prob cache load failed: " + e.message);
  }
}

async function recalibrateLeagueProbs() {
  if (!supabase) return { ok: false, error: "supabase not enabled" };
  const t0 = Date.now();
  try {
    const { data: buckets, error: rpcErr } = await supabase.rpc("compute_league_prob_buckets");
    if (rpcErr) throw rpcErr;
    if (!buckets || buckets.length === 0) {
      LEAGUE_PROB_LAST_RECAL = { at: Date.now(), buckets: 0, written: 0, error: null };
      return { ok: true, buckets: 0, written: 0 };
    }
    const rows = buckets.map(b => ({
      competition_id: b.competition_id,
      rank: b.rank,
      n: b.n,
      prob25: b.prob25,
      prob15: b.prob15,
      updated_at: new Date().toISOString(),
    }));
    const BATCH = 500;
    let written = 0;
    for (let i = 0; i < rows.length; i += BATCH) {
      const batch = rows.slice(i, i + BATCH);
      const { error } = await supabase
        .from("league_prob_tables")
        .upsert(batch, { onConflict: "competition_id,rank" });
      if (error) throw error;
      written += batch.length;
    }
    await loadLeagueProbCache();
    LEAGUE_PROB_LAST_RECAL = { at: Date.now(), buckets: buckets.length, written, error: null };
    console.log("Recalibrated league prob tables: " + written + " buckets (" + (Date.now() - t0) + "ms)");
    return { ok: true, buckets: buckets.length, written, elapsedMs: Date.now() - t0 };
  } catch (e) {
    LEAGUE_PROB_LAST_RECAL = { at: Date.now(), buckets: 0, written: 0, error: e.message };
    console.error("Recalibration failed: " + e.message);
    return { ok: false, error: e.message };
  }
}

// Phase 3: per-league signal-combination probabilities
// Cache shape: { "<compId>:<combo4chars>": { n, prob25, prob15 } }
const LEAGUE_COMBO_MIN_N    = 20;
let   LEAGUE_COMBO_CACHE    = {};
let   LEAGUE_COMBO_LAST_LOAD  = { at: 0, rows: 0, error: null };
let   LEAGUE_COMBO_LAST_RECAL = { at: 0, buckets: 0, written: 0, error: null };

function comboFromSignals(sigs) {
  if (!sigs) return "000";
  const bit = (k) => (sigs[k] && sigs[k].met) ? "1" : "0";
  return bit("A") + bit("B") + bit("C");
}

async function loadLeagueComboCache() {
  if (!supabase) return;
  try {
    const next = {};
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("league_combo_probs")
        .select("competition_id, sig_combo, n, prob25, prob15")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      for (const r of data) {
        next[r.competition_id + ":" + r.sig_combo] = { n: r.n, prob25: Number(r.prob25), prob15: Number(r.prob15) };
      }
      if (data.length < PAGE) break;
    }
    LEAGUE_COMBO_CACHE = next;
    LEAGUE_COMBO_LAST_LOAD = { at: Date.now(), rows: Object.keys(next).length, error: null };
    console.log("League combo cache loaded: " + LEAGUE_COMBO_LAST_LOAD.rows + " rows");
  } catch (e) {
    LEAGUE_COMBO_LAST_LOAD = { at: Date.now(), rows: Object.keys(LEAGUE_COMBO_CACHE).length, error: e.message };
    console.error("League combo cache load failed: " + e.message);
  }
}

async function recalibrateLeagueComboProbs() {
  if (!supabase) return { ok: false, error: "supabase not enabled" };
  const t0 = Date.now();
  try {
    const { data: buckets, error: rpcErr } = await supabase.rpc("compute_league_combo_buckets");
    if (rpcErr) throw rpcErr;
    if (!buckets || buckets.length === 0) {
      LEAGUE_COMBO_LAST_RECAL = { at: Date.now(), buckets: 0, written: 0, error: null };
      return { ok: true, buckets: 0, written: 0 };
    }
    const rows = buckets.map(b => ({
      competition_id: b.competition_id,
      sig_combo: b.sig_combo,
      n: b.n, prob25: b.prob25, prob15: b.prob15,
      updated_at: new Date().toISOString(),
    }));
    const BATCH = 500;
    let written = 0;
    for (let i = 0; i < rows.length; i += BATCH) {
      const batch = rows.slice(i, i + BATCH);
      const { error } = await supabase
        .from("league_combo_probs")
        .upsert(batch, { onConflict: "competition_id,sig_combo" });
      if (error) throw error;
      written += batch.length;
    }
    await loadLeagueComboCache();
    LEAGUE_COMBO_LAST_RECAL = { at: Date.now(), buckets: buckets.length, written, error: null };
    console.log("Recalibrated league combo probs: " + written + " buckets (" + (Date.now() - t0) + "ms)");
    return { ok: true, buckets: buckets.length, written, elapsedMs: Date.now() - t0 };
  } catch (e) {
    LEAGUE_COMBO_LAST_RECAL = { at: Date.now(), buckets: 0, written: 0, error: e.message };
    console.error("Combo recalibration failed: " + e.message);
    return { ok: false, error: e.message };
  }
}

// Override hierarchy: combo (n>=20) → league rank (n>=30) → global
function applyLeagueProb(result, compId) {
  if (!result || !compId) return;
  // 1. Try combo-level (Phase 3)
  const combo = comboFromSignals(result.signals);
  const comboEntry = LEAGUE_COMBO_CACHE[compId + ":" + combo];
  if (comboEntry && comboEntry.n >= LEAGUE_COMBO_MIN_N) {
    result.prob25 = comboEntry.prob25;
    result.prob15 = comboEntry.prob15;
    result.probSource  = "league_combo";
    result.probSampleN = comboEntry.n;
    result.probCombo   = combo;
    // Recalculate eligibility based on league-specific probs
    result.eligible25 = result.prob25 >= 40.0;
    result.eligible15 = result.prob15 >= 50.0;
    result.eligible = result.eligible25;  // Primary eligible
    return;
  }
  // 2. Fall back to per-league rank prob (Phase 2)
  const entry = LEAGUE_PROB_CACHE[compId + ":" + result.rank];
  if (entry && entry.n >= LEAGUE_PROB_MIN_N) {
    result.prob25 = entry.prob25;
    result.prob15 = entry.prob15;
    result.probSource  = "league_rank";
    result.probSampleN = entry.n;
    result.probCombo   = combo;
    // Recalculate eligibility based on league-specific probs
    result.eligible25 = result.prob25 >= 40.0;
    result.eligible15 = result.prob15 >= 50.0;
    result.eligible = result.eligible25;  // Primary eligible
    return;
  }
  // 3. Global default already in result.prob25/prob15
  result.probSource  = "global";
  result.probSampleN = entry ? entry.n : 0;
  result.probCombo   = combo;
  // Ensure eligibility flags are set for global fallback too
  result.eligible25 = result.prob25 >= 40.0;
  result.eligible15 = result.prob15 >= 50.0;
  result.eligible = result.eligible25;  // Primary eligible
}

let LEAGUE_NAMES = {};
let LEAGUE_LIST_LOADING = false;

async function fetchLeagueList() {
  if (LEAGUE_LIST_LOADING) return;
  LEAGUE_LIST_LOADING = true;
  try {
    const data = await safeFetch(BASE + "/league-list?key=" + KEY);
    if (!data) { console.warn("fetchLeagueList skipped"); LEAGUE_LIST_LOADING = false; return; }
    const list = data.data || [];
    const map = {};
    for (const league of list) {
      const leagueName = league.league_name || league.name || "";
      const country    = league.country || "";
      const name       = country ? country + " \u00b7 " + leagueName : leagueName;
      if (!name) continue;
      const seasons = league.season || [];
      for (const s of seasons) { if (s.id) map[parseInt(s.id, 10)] = name; }
    }
    LEAGUE_NAMES = map;
    console.log("Mapped " + Object.keys(map).length + " season IDs");
    if (list.length === 0) setTimeout(fetchLeagueList, 2 * 60 * 1000);
  } catch(e) {
    console.error("Failed to load league list: " + e.message);
    LEAGUE_NAMES = {};
  } finally {
    LEAGUE_LIST_LOADING = false;
  }
}

const FIXTURE_CACHE        = {};
const LEAGUE_MATCHES_CACHE = {};
const TEAM_STATS_CACHE     = {};
let   SERVER_MATCH_CACHE   = {};
const CI_SNAPSHOT_CACHE    = {};  // matchId → { ci, defCi, rank, label, prob25, prob15, eligible, signals, snap }
let   RATE_LIMITED_UNTIL   = 0;

const TTL_FIXTURES = 30 * 60 * 1000;
const TTL_MATCHES  =  2 * 60 * 60 * 1000;
const TTL_TEAMS    =  6 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS = 8000;

async function safeFetch(url) {
  if (Date.now() < RATE_LIMITED_UNTIL) { console.warn("Rate limited: " + url); return null; }
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    const data = await fetch(url, { signal: controller.signal }).then(r => r.json()).finally(() => clearTimeout(timer));
    if (data && data.error && String(data.error).toLowerCase().includes("rate limit")) {
      const reset = data.metadata && data.metadata.request_limit_refresh_next
        ? data.metadata.request_limit_refresh_next * 1000
        : Date.now() + 60 * 60 * 1000;
      RATE_LIMITED_UNTIL = reset;
      console.warn("Rate limit hit — backing off for " + Math.ceil((reset - Date.now()) / 60000) + " min");
      return null;
    }
    return data;
  } catch(e) { console.error("Fetch error: " + e.message); return null; }
}

async function fetchFixtures(date) {
  const now = Date.now();
  if (FIXTURE_CACHE[date] && (now - FIXTURE_CACHE[date].ts) < TTL_FIXTURES) return FIXTURE_CACHE[date].data;
  const data = await safeFetch(BASE + "/todays-matches?date=" + date + "&key=" + KEY);
  if (data) FIXTURE_CACHE[date] = { data, ts: now };
  return data || FIXTURE_CACHE[date]?.data || { data: [] };
}

async function fetchLeagueMatches(sid) {
  const now = Date.now();
  if (LEAGUE_MATCHES_CACHE[sid] && (now - LEAGUE_MATCHES_CACHE[sid].ts) < TTL_MATCHES) return LEAGUE_MATCHES_CACHE[sid].data;
  const page1 = await safeFetch(BASE + "/league-matches?season_id=" + sid + "&max_per_page=300&page=1&sort=date_unix&order=desc&key=" + KEY);
  if (!page1) return LEAGUE_MATCHES_CACHE[sid]?.data || { data: [] };
  let allMatches = page1.data || [];
  // Fetch page 2 only for large seasons (e.g. La Liga ~380 matches)
  if (allMatches.length >= 300) {
    const page2 = await safeFetch(BASE + "/league-matches?season_id=" + sid + "&max_per_page=300&page=2&sort=date_unix&order=desc&key=" + KEY);
    if (page2 && page2.data && page2.data.length) allMatches = allMatches.concat(page2.data);
  }
  const data = { ...page1, data: allMatches };
  LEAGUE_MATCHES_CACHE[sid] = { data, ts: now };
  return data;
}

async function fetchTeamStats(sid) {
  const now = Date.now();
  if (TEAM_STATS_CACHE[sid] && (now - TEAM_STATS_CACHE[sid].ts) < TTL_TEAMS) return TEAM_STATS_CACHE[sid].data;
  const data = await safeFetch(BASE + "/league-teams?season_id=" + sid + "&include=stats&key=" + KEY);
  if (data) TEAM_STATS_CACHE[sid] = { data, ts: now };
  return data || TEAM_STATS_CACHE[sid]?.data || { data: [] };
}

function rebuildServerMatchCache() {
  const newCache = {};
  let total = 0;
  for (const [sid, entry] of Object.entries(LEAGUE_MATCHES_CACHE)) {
    const leagueName = LEAGUE_NAMES[parseInt(sid, 10)] || "League " + sid;
    const nowSecs = Math.floor(Date.now() / 1000);
    for (const m of (entry.data.data || []).filter(m => isPlayedMatch(m, nowSecs))) {
      const slim = {
        homeID: m.homeID, awayID: m.awayID,
        home_name: m.home_name || "", away_name: m.away_name || "",
        date_unix: m.date_unix || 0,
        ht_goals_team_a: parseInt(m.ht_goals_team_a || 0, 10),
        ht_goals_team_b: parseInt(m.ht_goals_team_b || 0, 10),
        homeGoalCount:   parseInt(m.homeGoalCount   || 0, 10),
        awayGoalCount:   parseInt(m.awayGoalCount   || 0, 10),
        status: m.status, league: leagueName,
      };
      if (m.homeID) { if (!newCache[m.homeID]) newCache[m.homeID] = []; newCache[m.homeID].push(slim); }
      if (m.awayID) { if (!newCache[m.awayID]) newCache[m.awayID] = []; newCache[m.awayID].push(slim); }
      total++;
    }
  }
  SERVER_MATCH_CACHE = newCache;
  console.log("Server match cache rebuilt: " + Object.keys(newCache).length + " teams, " + total + " records");
}

const PREV_SEASON = { 16504:13973, 16544:11321, 16571:15746, 16614:14086, 16615:14116, 16036:13703 };
const ftch = url => fetch(url).then(r => r.json());
const safe = v   => (isNaN(v) || !isFinite(v)) ? 0 : Number(v);

function getDates(tzOffset) {
  const now = new Date();
  const local = new Date(now.getTime() + tzOffset * 60 * 1000);
  const fmt = d => {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const day = String(d.getUTCDate()).padStart(2, "0");
    return y + "-" + m + "-" + day;
  };
  const dates = [];
  for (let i = 0; i < 5; i++) {
    const d = new Date(local);
    d.setUTCDate(local.getUTCDate() + i);
    dates.push(fmt(d));
  }
  return [...new Set(dates)];
}

function unixToLocalDate(unix, tzOffset) {
  const d = new Date((unix * 1000) + tzOffset * 60 * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return y + "-" + m + "-" + day;
}

// Multi-signal probabilities — validated on live-captured matches.
// Signal A: Mutual Instability (home L5 total >= 1.8 AND away L5 total >= 1.6) — FH>2.5 at 35.5%
// Signal B: Away Team Scoring (away L5 FH >= 0.8) — FH>1.5 at 41.5%
const PROB15_BY_RANK = { 0: 37.7, 1: 41.5, 2: 47.0 };
const PROB25_BY_RANK = { 0: 11.4, 1: 23.0, 2: 35.5 };
const RANK_LABELS = { 0: "Low", 1: "Signal", 2: "Fire" };

// Average a team's last-5 first-half form. Goals are team-relative (fhFor =
// scored, fhAgst = conceded). Returns null if fewer than 3 recent games.
function last5Form(arr) {
  if (!Array.isArray(arr) || arr.length < 3) return null;
  const games = arr.slice(0, 5);
  const n = games.length;
  let f = 0, a = 0;
  for (const g of games) { f += (g.fhFor || 0); a += (g.fhAgst || 0); }
  return { f: f / n, a: a / n, t: (f + a) / n };
}

// Multi-signal engine — validated on live-captured matches.
//   Signal A: Mutual Instability (home L5 total >= 1.8 AND away L5 total >= 1.6) — FH>2.5 at 35.5%
//   Signal B: Away Team Scoring (away L5 FH >= 0.8) — FH>1.5 at 41.5%
function computeSignals(snap, hLast5, aLast5) {
  const f2 = (v) => v.toFixed(2);

  // Extract L5 metrics from snap
  const homeL5Total = snap && snap.l5 && snap.l5.home ? (snap.l5.home.t || 0) : 0;
  const awayL5Scored = snap && snap.l5 && snap.l5.away ? (snap.l5.away.f || 0) : 0;
  const awayL5Total = snap && snap.l5 && snap.l5.away ? (snap.l5.away.t || 0) : 0;

  // Check if we have L5 data
  const hasL5 = !!(snap && snap.l5 && snap.l5.home && snap.l5.away);

  // Signal A: Mutual Instability (both teams in upper-half of range)
  const sigA = hasL5 && homeL5Total >= 1.8 && awayL5Total >= 1.6;

  // Signal B: Away Team Scoring (away team moderately active)
  const sigB = hasL5 && awayL5Scored >= 0.8;

  // Compute rank (0-2) based on how many signals fire
  const signalCount = (sigA ? 1 : 0) + (sigB ? 1 : 0);
  const rank = Math.min(signalCount, 2);

  // Get probabilities from rank tables
  const prob15 = PROB15_BY_RANK[rank] || 35.8;
  const prob25 = PROB25_BY_RANK[rank] || 11.4;

  return {
    rank,
    label: RANK_LABELS[rank] || "Low",
    prob15,
    prob25,
    ci: homeL5Total + awayL5Total,  // combined intensity
    defCi: awayL5Total,              // away activity
    eligible: rank >= 2,
    signals: {
      A: { met: sigA, label: "Mutual Instability", value: f2(homeL5Total) + " / " + f2(awayL5Total), threshold: "home L5 total >= 1.8 & away L5 total >= 1.6" },
      B: { met: sigB, label: "Away Team Scoring", value: f2(awayL5Scored), threshold: "away L5 FH >= 0.8" },
    },
  };
}

function extractStats(teamObj, role) {
  const s = teamObj.stats || {};
  const sfx = role === "home" ? "_home" : "_away";
  const mpR = safe(s["seasonMatchesPlayed" + sfx]) || 1;
  const pick = (rk, fk) => {
    const rv = s[rk];
    if (rv !== null && rv !== undefined && mpR >= 3) return rv;
    const fv = s[fk];
    if (fv !== null && fv !== undefined) return fv;
    return 0;
  };
  return {
    name:      teamObj.name || teamObj.cleanName || "",
    scored_fh: safe(pick("scoredAVGHT"   + sfx, "scoredAVGHT_overall")),
    conced_fh: safe(pick("concededAVGHT" + sfx, "concededAVGHT_overall")),
    t1_pct:    safe(pick("seasonOver25PercentageHT" + sfx, "seasonOver25PercentageHT_overall")),
    cn010_avg: safe((s["goals_conceded_min_0_to_10" + sfx] || 0) / mpR),
    // Shots on target per game (role-specific, fallback to overall)
    sot_avg:   safe((s["shotsOnTarget" + sfx] || s["shotsOnTarget_overall"] || 0) / mpR),
    mp:        safe(s.seasonMatchesPlayed_overall || 0),
    mpRole:    mpR,
  };
}

const LAST5_WINDOW_SECS = 35 * 24 * 60 * 60;

// FIX 3: isPlayedMatch — used for both addToLocalExtra AND completed filtering
// so that incomplete-but-played matches (e.g. Liga MX Femenil) are included
// A past match with status "incomplete" is treated as played (covers 0-0 results too)
const isPlayedMatch = (m, nowSecs) =>
  m.status === "complete" ||
  (m.status === "incomplete" &&
   (m.date_unix || 0) < nowSecs - 7200);

function buildLast5(teamId, cache) {
  if (!teamId) return [];
  // Merge passed-in cache with a full scan of LEAGUE_MATCHES_CACHE for this team
  let entries = (cache[teamId] || []).slice();
  const passedKeys = new Set(entries.map(m => (m.date_unix||0)+"_"+(m.homeID||"")+"_"+(m.awayID||"")));
  const nowSecs = Math.floor(Date.now() / 1000);
  for (const entry of Object.values(LEAGUE_MATCHES_CACHE)) {
    for (const m of (entry.data.data || [])) {
      if (!isPlayedMatch(m, nowSecs)) continue;
      if (m.homeID !== teamId && m.awayID !== teamId) continue;
      const key = (m.date_unix||0)+"_"+(m.homeID||"")+"_"+(m.awayID||"");
      if (passedKeys.has(key)) continue;
      passedKeys.add(key);
      entries.push({
        homeID: m.homeID, awayID: m.awayID,
        home_name: m.home_name||"", away_name: m.away_name||"",
        date_unix: m.date_unix||0,
        ht_goals_team_a: parseInt(m.ht_goals_team_a||0,10),
        ht_goals_team_b: parseInt(m.ht_goals_team_b||0,10),
        homeGoalCount: parseInt(m.homeGoalCount||0,10),
        awayGoalCount: parseInt(m.awayGoalCount||0,10),
        status: m.status,
      });
    }
  }
  if (!entries.length) return [];
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - LAST5_WINDOW_SECS;
  const seen = new Set();
  const dedup = (m) => {
    const key = (m.date_unix || 0) + "_" + (m.homeID || "") + "_" + (m.awayID || "");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  };
  // Try 35-day window first
  let unique = entries.filter(m => (m.date_unix || 0) >= cutoff && dedup(m));
  // If fewer than 3, drop the time window entirely — just take the most recent
  if (unique.length < 3) {
    seen.clear();
    unique = entries.filter(m => dedup(m));
  }
  return unique
    .sort((a, b) => (b.date_unix || 0) - (a.date_unix || 0))
    .slice(0, 5)
    .map(m => {
      const isHome = m.homeID === teamId;
      const teamFt = isHome ? m.homeGoalCount : m.awayGoalCount;
      const oppFt  = isHome ? m.awayGoalCount : m.homeGoalCount;
      const result = teamFt > oppFt ? "W" : teamFt < oppFt ? "L" : "D";
      const date   = m.date_unix ? new Date(m.date_unix * 1000).toISOString().slice(0, 10) : "";
      // Goals are team-relative (for/against from THIS team's perspective), not home-away.
      return { date, venue: isHome ? "H" : "A", opp: isHome ? m.away_name : m.home_name,
               fhFor:  isHome ? m.ht_goals_team_a : m.ht_goals_team_b,
               fhAgst: isHome ? m.ht_goals_team_b : m.ht_goals_team_a,
               ftFor:  isHome ? m.homeGoalCount : m.awayGoalCount,
               ftAgst: isHome ? m.awayGoalCount : m.homeGoalCount, result };
    });
}

app.get("/supabase-status", (req, res) => {
  res.json({
    enabled: !!supabase,
    url: SUPABASE_URL ? SUPABASE_URL.replace(/^(https?:\/\/[^.]+).*/, "$1...") : null,
    keyType: process.env.SUPABASE_SERVICE_ROLE_KEY ? "service_role" : (process.env.SUPABASE_ANON_KEY ? "anon" : null),
    persistedThisSession: SUPABASE_PERSISTED_IDS.size,
    lastPersist: SUPABASE_LAST_PERSIST.at
      ? { at: new Date(SUPABASE_LAST_PERSIST.at).toISOString(), attempted: SUPABASE_LAST_PERSIST.attempted, written: SUPABASE_LAST_PERSIST.written, error: SUPABASE_LAST_PERSIST.error }
      : null,
  });
});

// ─── ONE-SHOT HISTORICAL DATASET LOADER ──────────────────────────────────────
// Loads dataset_combined_filled.csv into match_results. Gated by LOAD_DATASET_TOKEN.
// Usage: GET /admin/load-dataset?token=<LOAD_DATASET_TOKEN>
// Skips: women's leagues, incomplete matches, rows missing IDs or HT data.
// Recomputes rank using current 4-signal logic so historical data matches runtime.
const WOMENS_LEAGUE_IDS = new Set([15020, 16037, 16046, 16563]);

function parseCsvDataset(buf) {
  const text = buf.toString("utf8");
  // Character-level RFC-4180 parse: handles quoted commas, escaped quotes (""),
  // and newlines inside quoted fields — split(",") corrupts all three.
  const records = [];
  let field = "", record = [], inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else inQuotes = false;
      } else field += c;
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === ",") {
      record.push(field); field = "";
    } else if (c === "\n") {
      record.push(field); records.push(record); field = ""; record = [];
    } else if (c !== "\r") {
      field += c;
    }
  }
  if (field.length > 0 || record.length > 0) { record.push(field); records.push(record); }
  if (records.length < 2) return { header: [], idx: {}, rows: [] };
  const header = records[0];
  const idx = {};
  header.forEach((h, i) => idx[h] = i);
  const rows = records.slice(1).filter(r => !(r.length === 1 && r[0] === ""));
  return { header, idx, rows };
}

function csvRowToMatchResult(row, idx) {
  const get = (k) => row[idx[k]];
  const num = (k) => { const v = parseFloat(get(k)); return isNaN(v) ? 0 : v; };
  const intg = (k) => { const v = parseInt(get(k), 10); return isNaN(v) ? 0 : v; };

  const matchId = intg("id");
  const compId  = intg("competition_id");
  if (!matchId || !compId) return null;
  if (WOMENS_LEAGUE_IDS.has(compId)) return null;
  if (get("status") !== "complete") return null;

  const homeId = intg("homeID");
  const awayId = intg("awayID");
  if (!homeId || !awayId) return null;

  const hStats = {
    name: get("home_name") || "",
    scored_fh: num("h_scoredAVGHT_home"),
    conced_fh: num("h_concededAVGHT_home"),
    t1_pct:    num("h_seasonOver25PercentageHT_home"),
    cn010_avg: num("h_cn010_home"),
    sot_avg:   0,
    mp:        intg("h_mp_home"),
    mpRole:    intg("h_mp_home"),
  };
  const aStats = {
    name: get("away_name") || "",
    scored_fh: num("a_scoredAVGHT_away"),
    conced_fh: num("a_concededAVGHT_away"),
    t1_pct:    num("a_seasonOver25PercentageHT_away"),
    cn010_avg: num("a_cn010_away"),
    sot_avg:   0,
    mp:        intg("a_mp_away"),
    mpRole:    intg("a_mp_away"),
  };
  const snap = { fetchedAt: "historical-import", home: hStats, away: aStats };
  const result = computeSignals(snap);

  const fhH = intg("ht_goals_team_a");
  const fhA = intg("ht_goals_team_b");
  const ftH = intg("homeGoalCount");
  const ftA = intg("awayGoalCount");
  const fhTotal = fhH + fhA;

  return {
    match_id: matchId,
    competition_id: compId,
    league_name: LEAGUE_NAMES[compId] || null,
    home_id: homeId,
    away_id: awayId,
    home_name: hStats.name,
    away_name: aStats.name,
    date_unix: intg("date_unix") || null,
    ht_home: fhH,
    ht_away: fhA,
    ft_home: ftH,
    ft_away: ftA,
    fh_total: fhTotal,
    hit_15: fhTotal > 1,
    hit_25: fhTotal > 2,
    rank: result.rank,
    ci: result.ci,
    def_ci: result.defCi,
    prob25: result.prob25,
    prob15: result.prob15,
    signals: result.signals,
    snap: { fetchedAt: snap.fetchedAt,
            home: { name: hStats.name, scored_fh: hStats.scored_fh, conced_fh: hStats.conced_fh, t1_pct: hStats.t1_pct, cn010_avg: hStats.cn010_avg, sot_avg: 0 },
            away: { name: aStats.name, scored_fh: aStats.scored_fh, conced_fh: aStats.conced_fh, t1_pct: aStats.t1_pct, cn010_avg: aStats.cn010_avg, sot_avg: 0 } },
  };
}

app.get("/admin/load-dataset", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });

  const fs = require("fs");
  const path = require("path");
  const csvPath = path.join(__dirname, "dataset_combined_filled.csv");
  if (!fs.existsSync(csvPath)) return res.status(500).json({ ok: false, error: "dataset_combined_filled.csv not found" });

  const t0 = Date.now();
  let parsed;
  try {
    const buf = fs.readFileSync(csvPath);
    parsed = parseCsvDataset(buf);
  } catch (e) {
    return res.status(500).json({ ok: false, error: "csv read/parse failed: " + e.message });
  }

  const { idx, rows } = parsed;
  if (!idx || !rows) return res.status(500).json({ ok: false, error: "csv had no rows" });

  const validRows = [];
  let skipped = 0;
  for (const r of rows) {
    const mr = csvRowToMatchResult(r, idx);
    if (mr) validRows.push(mr); else skipped++;
  }

  const dryRun = req.query.dryRun === "1";
  if (dryRun) {
    const byRank = { 0: 0, 1: 0, 2: 0 };
    const byComp = {};
    for (const r of validRows) {
      byRank[r.rank] = (byRank[r.rank] || 0) + 1;
      byComp[r.competition_id] = (byComp[r.competition_id] || 0) + 1;
    }
    return res.json({
      ok: true, dryRun: true,
      totalRows: rows.length, valid: validRows.length, skipped,
      byRank, distinctCompetitions: Object.keys(byComp).length,
      sampleRow: validRows[0] || null,
      elapsedMs: Date.now() - t0,
    });
  }

  const BATCH_SIZE = 500;
  let written = 0;
  const errors = [];
  for (let i = 0; i < validRows.length; i += BATCH_SIZE) {
    const batch = validRows.slice(i, i + BATCH_SIZE);
    const { error } = await supabase.from("match_results").upsert(batch, { onConflict: "match_id", ignoreDuplicates: true });
    if (error) {
      errors.push({ batchStart: i, message: error.message });
      if (errors.length >= 5) break;
    } else {
      written += batch.length;
    }
  }

  res.json({
    ok: errors.length === 0,
    totalRows: rows.length,
    valid: validRows.length,
    skipped,
    written,
    batches: Math.ceil(validRows.length / BATCH_SIZE),
    errors,
    elapsedMs: Date.now() - t0,
  });
});

app.get("/admin/backfill", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  const start = req.query.start, end = req.query.end;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(start || "") || !/^\d{4}-\d{2}-\d{2}$/.test(end || "")) {
    return res.status(400).json({ ok: false, error: "start/end required as YYYY-MM-DD" });
  }
  // Cap range so we don't hammer FootyStats
  const startMs = Date.parse(start), endMs = Date.parse(end);
  if (isNaN(startMs) || isNaN(endMs) || endMs < startMs) return res.status(400).json({ ok: false, error: "invalid range" });
  const dayCount = Math.round((endMs - startMs) / 86400000) + 1;
  if (dayCount > 800) return res.status(400).json({ ok: false, error: "range too large (max ~2 years)" });

  const t0 = Date.now();
  const dates = [];
  for (let d = new Date(startMs); d <= endMs; d.setUTCDate(d.getUTCDate() + 1)) dates.push(d.toISOString().slice(0, 10));

  const seen = new Set();
  const fixtures = [];
  for (const d of dates) {
    const f = await fetchFixtures(d);
    for (const m of (f && f.data) || []) {
      const id = m.id;
      if (!id || seen.has(id)) continue;
      seen.add(id);
      if (m.status !== "complete") continue;
      const sid = parseInt(m.competition_id, 10);
      if (!sid || WOMENS_LEAGUE_IDS.has(sid)) continue;
      fixtures.push(m);
    }
  }

  const sids = Array.from(new Set(fixtures.map(f => parseInt(f.competition_id, 10))));
  const teamMaps = {};
  for (const sid of sids) {
    const tr = await fetchTeamStats(sid);
    const tmap = {};
    for (const t of (tr && tr.data) || []) if (t && t.id) tmap[t.id] = t;
    teamMaps[sid] = tmap;
  }

  const rows = [];
  let skipped = 0;
  for (const fix of fixtures) {
    const sid = parseInt(fix.competition_id, 10);
    const teamMap = teamMaps[sid] || {};
    const homeId = fix.homeID || fix.home_id;
    const awayId = fix.awayID || fix.away_id;
    const ht = teamMap[homeId], at = teamMap[awayId];
    if (!homeId || !awayId || !ht || !at) { skipped++; continue; }
    const hStats = extractStats(ht, "home");
    const aStats = extractStats(at, "away");
    const snap = { fetchedAt: "backfill", home: hStats, away: aStats };
    const result = computeSignals(snap);
    const fhH = parseInt(fix.ht_goals_team_a || 0, 10);
    const fhA = parseInt(fix.ht_goals_team_b || 0, 10);
    const ftH = parseInt(fix.homeGoalCount || 0, 10);
    const ftA = parseInt(fix.awayGoalCount || 0, 10);
    rows.push({
      match_id: fix.id,
      competition_id: sid,
      league_name: LEAGUE_NAMES[sid] || null,
      home_id: homeId, away_id: awayId,
      home_name: hStats.name, away_name: aStats.name,
      date_unix: fix.date_unix || null,
      ht_home: fhH, ht_away: fhA, ft_home: ftH, ft_away: ftA,
      fh_total: fhH + fhA,
      hit_15: (fhH + fhA) > 1, hit_25: (fhH + fhA) > 2,
      rank: result.rank, ci: result.ci, def_ci: result.defCi,
      prob25: result.prob25, prob15: result.prob15,
      signals: result.signals,
      snap: { fetchedAt: snap.fetchedAt,
              home: { name: hStats.name, scored_fh: hStats.scored_fh, conced_fh: hStats.conced_fh, t1_pct: hStats.t1_pct, cn010_avg: hStats.cn010_avg, sot_avg: 0 },
              away: { name: aStats.name, scored_fh: aStats.scored_fh, conced_fh: aStats.conced_fh, t1_pct: aStats.t1_pct, cn010_avg: aStats.cn010_avg, sot_avg: 0 } },
    });
  }

  if (req.query.dryRun === "1") {
    return res.json({ ok: true, dryRun: true, dates: dates.length, fixtures: fixtures.length, valid: rows.length, skipped, sampleRow: rows[0] || null, elapsedMs: Date.now() - t0 });
  }

  const BATCH = 500;
  let written = 0;
  const errors = [];
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const { error } = await supabase.from("match_results").upsert(batch, { onConflict: "match_id", ignoreDuplicates: true });
    if (error) {
      errors.push({ batchStart: i, message: error.message });
      if (errors.length >= 5) break;
    } else {
      written += batch.length;
    }
  }
  res.json({ ok: errors.length === 0, dates: dates.length, fixtures: fixtures.length, valid: rows.length, skipped, written, errors, elapsedMs: Date.now() - t0 });
});

// Backfill snap.l5 (per-team last-5 FH form) on live-captured rows that were
// persisted before commit 9499687 added the field. For each row, fetches the
// match's competition_id season matches (cached), then reconstructs the
// home/away team's last 5 *as of that match's kickoff* by filtering games to
// date_unix < row.date_unix. Skips rows where fewer than 3 prior games exist
// for either team (insufficient sample, same null-rule as last5Form).
//
//   GET /admin/backfill-l5?token=<TOKEN>&dryRun=1     -> report counts only
//   GET /admin/backfill-l5?token=<TOKEN>&limit=200    -> process up to 200 rows
//
// Idempotent: only touches rows where snap.l5 is null/missing. Uses regular
// UPDATE (not the ignoreDuplicates upsert), so it can patch existing rows.
app.get("/admin/backfill-l5", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });

  const dryRun = req.query.dryRun === "1";
  const limit = Math.max(1, Math.min(1000, parseInt(req.query.limit || "200", 10)));
  const t0 = Date.now();

  try {
    const allLive = [];
    const PAGE = 300;
    // Live captures are all recent; bound by date_unix so the planner can use
    // the date index and skip the bulk of historical-import/backfill rows
    // before evaluating the JSON-path NOT-IN filters.
    const cutoffSec = Math.floor(Date.now() / 1000) - 365 * 86400;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("match_id, competition_id, home_id, away_id, date_unix, snap")
        .gte("date_unix", cutoffSec)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      allLive.push(...data);
      if (data.length < PAGE) break;
    }
    const missing = allLive.filter(r => r.snap && !r.snap.l5 && r.home_id && r.away_id && r.competition_id && r.date_unix);

    if (dryRun) {
      const byComp = {};
      for (const r of missing) byComp[r.competition_id] = (byComp[r.competition_id] || 0) + 1;
      return res.json({
        ok: true, dryRun: true,
        totalLive: allLive.length, missingL5: missing.length,
        skippedNoIds: allLive.filter(r => r.snap && !r.snap.l5).length - missing.length,
        distinctCompetitions: Object.keys(byComp).length,
        topComps: Object.entries(byComp).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([c, n]) => ({ competition_id: +c, n })),
      });
    }

    const work = missing.slice(0, limit);
    // Group by competition_id so each league season is fetched once
    const byComp = {};
    for (const r of work) {
      if (!byComp[r.competition_id]) byComp[r.competition_id] = [];
      byComp[r.competition_id].push(r);
    }

    const teamL5 = (matches, tid, beforeUnix) => {
      const games = matches
        .filter(m => (m.homeID === tid || m.awayID === tid) && (m.date_unix || 0) < beforeUnix && (m.status === "complete" || m.status === "incomplete"))
        .sort((a, b) => (b.date_unix || 0) - (a.date_unix || 0))
        .slice(0, 5);
      if (games.length < 3) return null;
      let f = 0, a = 0;
      for (const m of games) {
        const isHome = m.homeID === tid;
        f += parseInt((isHome ? m.ht_goals_team_a : m.ht_goals_team_b) || 0, 10);
        a += parseInt((isHome ? m.ht_goals_team_b : m.ht_goals_team_a) || 0, 10);
      }
      const n = games.length;
      return { f: +(f / n).toFixed(2), a: +(a / n).toFixed(2), t: +((f + a) / n).toFixed(2) };
    };

    let processed = 0, written = 0, skippedThin = 0, errs = 0;
    const errors = [];

    for (const [compId, rs] of Object.entries(byComp)) {
      if (Date.now() < RATE_LIMITED_UNTIL) {
        errors.push({ compId, error: "rate-limited; aborting batch" });
        break;
      }
      let matches;
      try {
        const matchRes = await fetchLeagueMatches(compId);
        matches = matchRes.data || [];
        // Also pull previous season if mapped — last-5 near season boundary
        const prevSid = PREV_SEASON[compId];
        if (prevSid) {
          try {
            const prev = await fetchLeagueMatches(prevSid);
            if (prev && prev.data) matches = matches.concat(prev.data);
          } catch (_) { /* best-effort */ }
        }
      } catch (e) {
        errors.push({ compId, error: "fetch failed: " + e.message });
        continue;
      }
      for (const r of rs) {
        processed++;
        const hL5 = teamL5(matches, r.home_id, r.date_unix);
        const aL5 = teamL5(matches, r.away_id, r.date_unix);
        if (!hL5 || !aL5) { skippedThin++; continue; }
        const newSnap = Object.assign({}, r.snap, { l5: { home: hL5, away: aL5 } });
        const { error } = await supabase.from("match_results").update({ snap: newSnap }).eq("match_id", r.match_id);
        if (error) {
          errs++;
          if (errors.length < 10) errors.push({ match_id: r.match_id, error: error.message });
        } else {
          written++;
        }
      }
    }

    res.json({
      ok: errs === 0,
      totalLive: allLive.length,
      missingL5: missing.length,
      processed, written, skippedThin, errs,
      remaining: missing.length - processed,
      elapsedMs: Date.now() - t0,
      errors,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/admin/backfill-signal-d", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });

  const t0 = Date.now();
  try {
    // Fetch all live-captured matches with snap.l5
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("match_id, signals, snap")
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }

    const withL5 = all.filter(m => m.snap && m.snap.l5 && m.snap.l5.home && m.snap.l5.away);
    const toUpdate = [];

    for (const m of withL5) {
      const snap = m.snap;
      const sigs = m.signals || {};

      // Recompute Signal C
      const sigC = (snap.l5.away.f || 0) >= 1.0 && (snap.l5.home.a || 0) >= 0.8;
      const sigA = sigs.A && sigs.A.met;
      const sigB = sigs.B && sigs.B.met;

      // Build new signals object with updated C
      const newSignals = Object.assign({}, sigs, {
        C: { met: sigC, label: "Away Attack + Home Leak", value: (snap.l5.away.f || 0).toFixed(2) + " / " + (snap.l5.home.a || 0).toFixed(2), threshold: "away L5 FH >= 1.0 & home leak >= 0.8" }
      });

      // Recompute combo and probabilities
      const combo = (sigA ? "1" : "0") + (sigB ? "1" : "0") + (sigC ? "1" : "0");
      const prob25 = PROB25_BY_COMBO[combo] !== undefined ? PROB25_BY_COMBO[combo] : 11.5;
      const prob15 = PROB15_BY_COMBO[combo] !== undefined ? PROB15_BY_COMBO[combo] : 35.8;
      const eligible25 = prob25 >= 40.0;
      const eligible15 = prob15 >= 50.0;

      // Recompute rank
      const rank = (sigA ? 1 : 0) + (sigB ? 1 : 0) + (sigC ? 1 : 0);
      const label = rank >= 2 ? "Fire" : rank === 1 ? "Signal" : "Low";

      toUpdate.push({
        match_id: m.match_id,
        signals: newSignals,
        rank, label,
        prob25, prob15,
        eligible25, eligible15
      });
    }

    // Batch update
    if (toUpdate.length > 0) {
      const BATCH = 500;
      let written = 0;
      for (let i = 0; i < toUpdate.length; i += BATCH) {
        const batch = toUpdate.slice(i, i + BATCH);
        const { error } = await supabase
          .from("match_results")
          .upsert(batch.map(u => ({
            match_id: u.match_id,
            signals: u.signals,
            rank: u.rank,
            label: u.label,
            prob25: u.prob25,
            prob15: u.prob15,
            eligible25: u.eligible25,
            eligible15: u.eligible15
          })), { onConflict: "match_id" });
        if (error) throw error;
        written += batch.length;
      }
    }

    res.json({
      ok: true,
      totalLive: all.length,
      withL5: withL5.length,
      updated: toUpdate.length,
      elapsedMs: Date.now() - t0,
      note: "Backfilled Signal C on all live-captured matches with snap.l5. Recomputed rank, label, prob25, prob15, eligible25, eligible15."
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/admin/recalibrate", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  const rank  = await recalibrateLeagueProbs();
  const combo = await recalibrateLeagueComboProbs();
  res.json({ rank, combo });
});

app.get("/league-combos", (req, res) => {
  const compId = req.query.compId ? parseInt(req.query.compId, 10) : null;
  const out = [];
  for (const [k, v] of Object.entries(LEAGUE_COMBO_CACHE)) {
    const [cid, combo] = k.split(":");
    const cidN = parseInt(cid, 10);
    if (compId && cidN !== compId) continue;
    out.push({ competition_id: cidN, league: LEAGUE_NAMES[cidN] || null, sig_combo: combo, n: v.n, prob25: v.prob25, prob15: v.prob15, usedForOverride: v.n >= LEAGUE_COMBO_MIN_N });
  }
  out.sort((a, b) => a.competition_id - b.competition_id || a.sig_combo.localeCompare(b.sig_combo));
  res.json({
    ok: true,
    minSampleForOverride: LEAGUE_COMBO_MIN_N,
    cacheRows: Object.keys(LEAGUE_COMBO_CACHE).length,
    lastLoad:  LEAGUE_COMBO_LAST_LOAD.at  ? { at: new Date(LEAGUE_COMBO_LAST_LOAD.at).toISOString(),  rows:    LEAGUE_COMBO_LAST_LOAD.rows,    error: LEAGUE_COMBO_LAST_LOAD.error }    : null,
    lastRecal: LEAGUE_COMBO_LAST_RECAL.at ? { at: new Date(LEAGUE_COMBO_LAST_RECAL.at).toISOString(), buckets: LEAGUE_COMBO_LAST_RECAL.buckets, written: LEAGUE_COMBO_LAST_RECAL.written, error: LEAGUE_COMBO_LAST_RECAL.error } : null,
    rows: out,
  });
});

app.get("/league-probs", (req, res) => {
  const compId = req.query.compId ? parseInt(req.query.compId, 10) : null;
  const out = [];
  for (const [k, v] of Object.entries(LEAGUE_PROB_CACHE)) {
    const [cid, rank] = k.split(":").map(Number);
    if (compId && cid !== compId) continue;
    out.push({ competition_id: cid, league: LEAGUE_NAMES[cid] || null, rank, n: v.n, prob25: v.prob25, prob15: v.prob15, usedForOverride: v.n >= LEAGUE_PROB_MIN_N });
  }
  out.sort((a, b) => a.competition_id - b.competition_id || a.rank - b.rank);
  res.json({
    ok: true,
    minSampleForOverride: LEAGUE_PROB_MIN_N,
    cacheRows: Object.keys(LEAGUE_PROB_CACHE).length,
    lastLoad:  LEAGUE_PROB_LAST_LOAD.at  ? { at: new Date(LEAGUE_PROB_LAST_LOAD.at).toISOString(),  rows:    LEAGUE_PROB_LAST_LOAD.rows,    error: LEAGUE_PROB_LAST_LOAD.error }    : null,
    lastRecal: LEAGUE_PROB_LAST_RECAL.at ? { at: new Date(LEAGUE_PROB_LAST_RECAL.at).toISOString(), buckets: LEAGUE_PROB_LAST_RECAL.buckets, written: LEAGUE_PROB_LAST_RECAL.written, error: LEAGUE_PROB_LAST_RECAL.error } : null,
    rows: out,
  });
});

// Clean-cohort calibration: matches with TRUE pre-game freeze (excludes
// historical-import and backfill rows). Used to measure real forward accuracy.
app.get("/calibration", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const all = [];
    const PAGE = 1000;
    const compId = req.query.competition_id ? parseInt(req.query.competition_id, 10) : null;
    const exclude = req.query.exclude_women === "true";
    const WOMEN_LEAGUES = [15020, 16037, 16046, 16563];
    for (let off = 0; ; off += PAGE) {
      let q = supabase
        .from("match_results")
        .select("rank, prob25, prob15, hit_25, hit_15, signals, snap, competition_id")
        .not("hit_25", "is", null)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill");
      if (compId) q = q.eq("competition_id", compId);
      if (exclude) q = q.not("competition_id", "in", `(${WOMEN_LEAGUES.join(",")})`);
      const { data, error } = await q.range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }
    const byRank = {};
    const byCombo = {};
    for (let r = 0; r <= 3; r++) byRank[r] = { n: 0, hit25: 0, hit15: 0, sumP25: 0, sumP15: 0 };
    let total = { n: 0, hit25: 0, hit15: 0, sumP25: 0, sumP15: 0 };
    for (const m of all) {
      const r = m.rank;
      if (byRank[r] !== undefined) {
        byRank[r].n++;
        if (m.hit_25) byRank[r].hit25++;
        if (m.hit_15) byRank[r].hit15++;
        byRank[r].sumP25 += Number(m.prob25 || 0);
        byRank[r].sumP15 += Number(m.prob15 || 0);
      }
      // Per-combo bucket (3-bit: ABD)
      const sigs = m.signals || {};
      const bit = (k) => (sigs[k] && sigs[k].met) ? "1" : "0";
      const combo = bit("A") + bit("B") + bit("C");
      if (!byCombo[combo]) byCombo[combo] = { n: 0, hit25: 0, hit15: 0, sumP25: 0, sumP15: 0 };
      byCombo[combo].n++;
      if (m.hit_25) byCombo[combo].hit25++;
      if (m.hit_15) byCombo[combo].hit15++;
      byCombo[combo].sumP25 += Number(m.prob25 || 0);
      byCombo[combo].sumP15 += Number(m.prob15 || 0);
      total.n++;
      if (m.hit_25) total.hit25++;
      if (m.hit_15) total.hit15++;
      total.sumP25 += Number(m.prob25 || 0);
      total.sumP15 += Number(m.prob15 || 0);
    }
    function finalize(b) {
      b.predicted25 = b.n ? +(b.sumP25 / b.n).toFixed(1) : 0;
      b.predicted15 = b.n ? +(b.sumP15 / b.n).toFixed(1) : 0;
      b.actual25 = b.n ? +(b.hit25 / b.n * 100).toFixed(1) : 0;
      b.actual15 = b.n ? +(b.hit15 / b.n * 100).toFixed(1) : 0;
      delete b.sumP25; delete b.sumP15;
    }
    for (const k of Object.keys(byRank)) finalize(byRank[k]);
    for (const k of Object.keys(byCombo)) finalize(byCombo[k]);
    const WOMEN_LEAGUE_NAMES = { 15020: "Liga MX Femenil", 16037: "Women's", 16046: "Arsenal Women / WSL", 16563: "Women's Internationals" };
    res.json({
      ok: true,
      cohortSize: total.n,
      filter: {
        league: compId ? `competition_id=${compId}` : (exclude ? "excluding women's leagues" : "all leagues"),
        women_excluded: exclude || false,
      },
      summary: {
        n: total.n,
        hit25: total.hit25, hit15: total.hit15,
        actual25: total.n ? +(total.hit25 / total.n * 100).toFixed(1) : 0,
        actual15: total.n ? +(total.hit15 / total.n * 100).toFixed(1) : 0,
        predicted25: total.n ? +(total.sumP25 / total.n).toFixed(1) : 0,
        predicted15: total.n ? +(total.sumP15 / total.n).toFixed(1) : 0,
      },
      byRank,
      byCombo,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── SIGNAL BACKTEST (live data, no look-ahead bias) ─────────────────────────
// Tests each signal A-D against real recorded predictions. Excludes
// historical-import and backfill rows, whose seasonOver25PercentageHT carries
// full-season look-ahead bias. lift25 ~1.0 means the signal has no live
// predictive value; a negative gap25 means the probability table overpredicts.
app.get("/signal-backtest", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const all = [];
    const PAGE = 1000;
    const compId = req.query.competition_id ? parseInt(req.query.competition_id, 10) : null;
    const exclude = req.query.exclude_women === "true";
    const WOMEN_LEAGUES = [15020, 16037, 16046, 16563];
    for (let off = 0; ; off += PAGE) {
      let q = supabase
        .from("match_results")
        .select("rank, prob25, prob15, hit_25, hit_15, signals, snap, competition_id")
        .not("hit_25", "is", null)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill");
      if (compId) q = q.eq("competition_id", compId);
      if (exclude) q = q.not("competition_id", "in", `(${WOMEN_LEAGUES.join(",")})`);
      const { data, error } = await q.range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }
    const n = all.length;
    const base25 = n ? all.filter(m => m.hit_25).length / n : 0;
    const base15 = n ? all.filter(m => m.hit_15).length / n : 0;
    const met = (m, k) => !!(m.signals && m.signals[k] && m.signals[k].met);

    const perSignal = {};
    for (const k of ["A", "B"]) {
      const fired = all.filter(m => met(m, k));
      const quiet = all.filter(m => !met(m, k));
      const fHit = fired.length ? fired.filter(m => m.hit_25).length / fired.length : 0;
      const qHit = quiet.length ? quiet.filter(m => m.hit_25).length / quiet.length : 0;
      const labelRow = fired.find(m => m.signals[k] && m.signals[k].label);
      perSignal[k] = {
        label: labelRow ? labelRow.signals[k].label : k,
        fires: fired.length,
        hit25WhenFire: +(fHit * 100).toFixed(1),
        hit25WhenQuiet: +(qHit * 100).toFixed(1),
        lift25: base25 ? +(fHit / base25).toFixed(2) : 0,
      };
    }

    const byRank = {};
    for (let r = 0; r <= 2; r++) byRank[r] = { n: 0, hit25: 0, hit15: 0, sumP25: 0, sumP15: 0 };
    for (const m of all) {
      const b = byRank[m.rank];
      if (!b) continue;
      b.n++;
      if (m.hit_25) b.hit25++;
      if (m.hit_15) b.hit15++;
      b.sumP25 += Number(m.prob25 || 0);
      b.sumP15 += Number(m.prob15 || 0);
    }
    for (const r of Object.keys(byRank)) {
      const b = byRank[r];
      b.actual25 = b.n ? +(b.hit25 / b.n * 100).toFixed(1) : 0;
      b.actual15 = b.n ? +(b.hit15 / b.n * 100).toFixed(1) : 0;
      b.predicted25 = b.n ? +(b.sumP25 / b.n).toFixed(1) : 0;
      b.predicted15 = b.n ? +(b.sumP15 / b.n).toFixed(1) : 0;
      b.gap25 = +(b.actual25 - b.predicted25).toFixed(1);
      delete b.sumP25; delete b.sumP15;
    }

    // Per-combination buckets — rank counts signals, but specific combos can
    // diverge a lot at the same rank (e.g. B alone vs C alone). This surfaces
    // which combinations actually carry the predictive weight.
    const byCombo = {};
    for (const m of all) {
      const c = ["A", "E"].filter(k => met(m, k)).join("") || "(none)";
      if (!byCombo[c]) byCombo[c] = { n: 0, hit25: 0, hit15: 0 };
      byCombo[c].n++;
      if (m.hit_25) byCombo[c].hit25++;
      if (m.hit_15) byCombo[c].hit15++;
    }
    for (const c of Object.keys(byCombo)) {
      const b = byCombo[c];
      b.rank = c === "(none)" ? 0 : c.length;
      b.actual25 = b.n ? +(b.hit25 / b.n * 100).toFixed(1) : 0;
      b.actual15 = b.n ? +(b.hit15 / b.n * 100).toFixed(1) : 0;
      b.lift25 = base25 ? +(b.hit25 / b.n / base25).toFixed(2) : 0;
    }

    res.json({
      ok: true,
      cohortSize: n,
      filter: {
        league: compId ? `competition_id=${compId}` : (exclude ? "excluding women's leagues" : "all leagues"),
        women_excluded: exclude || false,
      },
      baseRate25: +(base25 * 100).toFixed(1),
      baseRate15: +(base15 * 100).toFixed(1),
      perSignal,
      byRank,
      byCombo,
      note: "Excludes historical-import/backfill. lift25~1.0 = no live edge; gap25<0 = table overpredicts; byCombo.lift25 < same-rank peers = that signal is dead weight.",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── ANALYZE LAST-5 FORM PATTERNS (no fabrication) ─────────────────────────
app.get("/analyze-l5-patterns", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("snap, signals, hit_25, hit_15")
        .not("hit_25", "is", null)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }

    const stats = {
      total: all.length,
      with_l5: 0,
      home_last5_f: [], home_last5_a: [], home_last5_t: [],
      away_last5_f: [], away_last5_a: [], away_last5_t: [],
      combined_total: [],
      signal_A_threshold: 4.0,
      signal_B_threshold: 0.81,
      signal_C_away_f_threshold: 1.0,
      signal_C_home_a_threshold: 0.8,
    };

    for (const m of all) {
      const snap = m.snap || {};
      if (!snap.l5) continue;
      stats.with_l5++;

      const h5 = snap.l5.home || {};
      const a5 = snap.l5.away || {};

      if (h5.f !== undefined) stats.home_last5_f.push(h5.f);
      if (h5.a !== undefined) stats.home_last5_a.push(h5.a);
      if (h5.t !== undefined) stats.home_last5_t.push(h5.t);

      if (a5.f !== undefined) stats.away_last5_f.push(a5.f);
      if (a5.a !== undefined) stats.away_last5_a.push(a5.a);
      if (a5.t !== undefined) stats.away_last5_t.push(a5.t);

      const combined = (h5.t || 0) + (a5.t || 0);
      stats.combined_total.push(combined);
    }

    const summarize = (arr, name) => {
      if (arr.length === 0) return { name, n: 0 };
      arr.sort((a, b) => a - b);
      const sum = arr.reduce((a, b) => a + b, 0);
      const mean = sum / arr.length;
      const median = arr[Math.floor(arr.length / 2)];
      const min = arr[0];
      const max = arr[arr.length - 1];
      const pct25 = arr[Math.floor(arr.length * 0.25)];
      const pct75 = arr[Math.floor(arr.length * 0.75)];
      return { name, n: arr.length, min, pct25, median, mean: +mean.toFixed(2), pct75, max };
    };

    res.json({
      ok: true,
      total_matches: stats.total,
      matches_with_l5: stats.with_l5,
      last5_distributions: {
        home_scored_avg: summarize(stats.home_last5_f, "home avg FH scored"),
        home_conceded_avg: summarize(stats.home_last5_a, "home avg FH conceded"),
        home_combined_avg: summarize(stats.home_last5_t, "home combined avg"),
        away_scored_avg: summarize(stats.away_last5_f, "away avg FH scored"),
        away_conceded_avg: summarize(stats.away_last5_a, "away avg FH conceded"),
        away_combined_avg: summarize(stats.away_last5_t, "away combined avg"),
        both_teams_combined: summarize(stats.combined_total, "both teams combined FH total"),
      },
      signal_thresholds: {
        A: { threshold: 4.0, description: "hT + aT >= 4.0 (combined last-5 avg)" },
        B: { threshold: 0.81, description: "both hF >= 0.81 AND aF >= 0.81" },
        C: { threshold: "aF >= 1.0 AND hA >= 0.8", description: "away attack AND home leak" },
      },
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/history", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  const days = Math.min(7, Math.max(1, parseInt(req.query.days || "7", 10)));
  const cutoffSec = Math.floor(Date.now() / 1000) - days * 86400;
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("match_id, competition_id, league_name, home_name, away_name, date_unix, ht_home, ht_away, ft_home, ft_away, fh_total, hit_15, hit_25, rank, ci, def_ci, prob25, prob15, signals, snap")
        .gte("date_unix", cutoffSec)
        .not("hit_25", "is", null)
        .order("date_unix", { ascending: false })
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }
    const byRank = {};
    for (let r = 0; r <= 2; r++) byRank[r] = { n: 0, hit25: 0, hit15: 0, expSum25: 0, expSum15: 0 };
    let total = { n: 0, hit25: 0, hit15: 0 };
    for (const m of all) {
      const r = m.rank;
      if (byRank[r]) {
        byRank[r].n++;
        if (m.hit_25) byRank[r].hit25++;
        if (m.hit_15) byRank[r].hit15++;
        byRank[r].expSum25 += Number(m.prob25 || 0);
        byRank[r].expSum15 += Number(m.prob15 || 0);
      }
      total.n++;
      if (m.hit_25) total.hit25++;
      if (m.hit_15) total.hit15++;
    }
    for (const r of Object.keys(byRank)) {
      const b = byRank[r];
      b.actual25 = b.n ? +(b.hit25 / b.n * 100).toFixed(1) : 0;
      b.actual15 = b.n ? +(b.hit15 / b.n * 100).toFixed(1) : 0;
      b.expected25 = b.n ? +(b.expSum25 / b.n).toFixed(1) : 0;
      b.expected15 = b.n ? +(b.expSum15 / b.n).toFixed(1) : 0;
      delete b.expSum25; delete b.expSum15;
    }
    res.json({
      ok: true,
      days,
      summary: {
        total: total.n,
        hit25: total.hit25, hit15: total.hit15,
        actual25: total.n ? +(total.hit25 / total.n * 100).toFixed(1) : 0,
        actual15: total.n ? +(total.hit15 / total.n * 100).toFixed(1) : 0,
      },
      byRank,
      matches: all,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/h2h", async (req, res) => {
  if (!supabase) return res.json({ ok: true, matches: [] });
  const h = parseInt(req.query.h, 10);
  const a = parseInt(req.query.a, 10);
  const limit = Math.min(20, Math.max(1, parseInt(req.query.limit || "5", 10)));
  if (!h || !a) return res.status(400).json({ ok: false, error: "h and a required" });
  try {
    const { data, error } = await supabase
      .from("match_results")
      .select("match_id, date_unix, home_id, away_id, home_name, away_name, ht_home, ht_away, ft_home, ft_away, league_name")
      .or("and(home_id.eq." + h + ",away_id.eq." + a + "),and(home_id.eq." + a + ",away_id.eq." + h + ")")
      .not("hit_25", "is", null)
      .order("date_unix", { ascending: false })
      .limit(limit);
    if (error) throw error;
    res.json({ ok: true, matches: data || [] });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/cache-status", (req, res) => {
  const now = Date.now();
  res.json({
    rateLimitedUntil: RATE_LIMITED_UNTIL > now ? new Date(RATE_LIMITED_UNTIL).toISOString() : "not limited",
    rateLimitedMinRemaining: RATE_LIMITED_UNTIL > now ? Math.ceil((RATE_LIMITED_UNTIL - now) / 60000) : 0,
    leagueRegistry: Object.keys(LEAGUE_NAMES).length + " seasons mapped",
    serverMatchCache: Object.keys(SERVER_MATCH_CACHE).length + " teams",
    fixtureCacheEntries: Object.entries(FIXTURE_CACHE).map(([date, e]) => ({
      date, ageMin: Math.round((now - e.ts) / 60000), matchCount: (e.data.data || []).length,
    })),
    leagueMatchesCacheEntries: Object.keys(LEAGUE_MATCHES_CACHE).length,
    teamStatsCacheEntries: Object.keys(TEAM_STATS_CACHE).length,
    leagueMatchesCache: Object.entries(LEAGUE_MATCHES_CACHE).map(([sid, e]) => ({
      sid, league: LEAGUE_NAMES[parseInt(sid)] || "?",
      ageMin: Math.round((now - e.ts) / 60000), matchCount: (e.data.data || []).length,
    })),
    teamStatsCache: Object.entries(TEAM_STATS_CACHE).map(([sid, e]) => ({
      sid, ageMin: Math.round((now - e.ts) / 60000), teamCount: (e.data.data || []).length,
    })),
  });
});

app.get("/debug", async (req, res) => {
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const dates = getDates(tzOffset);
    const raw = await fetchFixtures(dates[0]);
    const fixtures = raw.data || [];
    res.json({
      date: dates[0], rateLimited: Date.now() < RATE_LIMITED_UNTIL,
      leagueRegistrySize: Object.keys(LEAGUE_NAMES).length,
      totalFixtures: fixtures.length,
      cachedTeams: Object.keys(SERVER_MATCH_CACHE).length,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get("/debug-team/:teamId", (req, res) => {
  const tid = parseInt(req.params.teamId, 10);
  const nowSecs = Math.floor(Date.now() / 1000);
  // Check SERVER_MATCH_CACHE
  const smcEntries = (SERVER_MATCH_CACHE[tid] || []).length;
  // Scan LEAGUE_MATCHES_CACHE
  const lmcMatches = [];
  for (const [sid, entry] of Object.entries(LEAGUE_MATCHES_CACHE)) {
    for (const m of (entry.data.data || [])) {
      if (m.homeID !== tid && m.awayID !== tid) continue;
      lmcMatches.push({
        sid, status: m.status, date_unix: m.date_unix,
        home: m.home_name, away: m.away_name,
        homeID: m.homeID, awayID: m.awayID,
        ht_a: m.ht_goals_team_a, ht_b: m.ht_goals_team_b,
        ftH: m.homeGoalCount, ftA: m.awayGoalCount,
        isPlayed: isPlayedMatch(m, nowSecs),
      });
    }
  }
  res.json({ teamId: tid, serverMatchCache: smcEntries, leagueMatchesCache: lmcMatches.slice(0, 20) });
});

app.get("/api/*", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  try {
    const path = req.path.replace("/api", "");
    const params = { ...req.query };
    delete params.token;
    const qs = new URLSearchParams({ ...params, key: KEY }).toString();
    const data = await safeFetch(BASE + path + "?" + qs);
    if (!data) return res.status(503).json({ error: "upstream unavailable or rate limited" });
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// ─── SHARED PREDS COMPUTATION ────────────────────────────────────────────────
async function computePreds(tzOffset) {
  const fetchedAt = new Date().toISOString().slice(0, 16).replace("T", " ");
  const dates     = getDates(tzOffset);

  // Ensure league registry is loaded — kick off if not running, no blocking wait
  if (Object.keys(LEAGUE_NAMES).length === 0 && !LEAGUE_LIST_LOADING) fetchLeagueList();
  const leagueFilterActive = Object.keys(LEAGUE_NAMES).length > 0;

    const allFixtures = [];
    const dayResults = await Promise.all(dates.map(d => fetchFixtures(d)));
    for (let i = 0; i < dates.length; i++) {
      for (const m of (dayResults[i].data || [])) {
        allFixtures.push(Object.assign({}, m, { _date: dates[i] }));
      }
    }

    // FIX 4: deduplicate by match id before grouping — prevents same match
    // appearing twice when it maps to two season IDs in LEAGUE_NAMES
    const seenFixtureIds = new Set();
    const leagueFixtures = {};
    for (const m of allFixtures) {
      const sid = parseInt(m.competition_id, 10);
      if (leagueFilterActive && !LEAGUE_NAMES[sid]) continue;
      const fid = String(m.id || (m.homeID + "_" + m.awayID + "_" + (m.date_unix || 0)));
      if (seenFixtureIds.has(fid)) continue;
      seenFixtureIds.add(fid);
      if (!leagueFixtures[sid]) leagueFixtures[sid] = [];
      leagueFixtures[sid].push(m);
    }

    const localExtra = {};
    const serverCacheKeys = new Set();
    for (const matches of Object.values(SERVER_MATCH_CACHE)) {
      for (const m of matches) {
        serverCacheKeys.add((m.date_unix||0) + "_" + (m.homeID||"") + "_" + (m.awayID||""));
      }
    }

    const slimM = (m, lg) => ({
      homeID: m.homeID, awayID: m.awayID,
      home_name: m.home_name || "", away_name: m.away_name || "",
      date_unix: m.date_unix || 0,
      ht_goals_team_a: parseInt(m.ht_goals_team_a || 0, 10),
      ht_goals_team_b: parseInt(m.ht_goals_team_b || 0, 10),
      homeGoalCount: parseInt(m.homeGoalCount || 0, 10),
      awayGoalCount: parseInt(m.awayGoalCount || 0, 10),
      status: m.status, league: lg || "",
    });

    const nowSecs = Math.floor(Date.now() / 1000);

    const addToLocalExtra = (matches, lg) => {
      for (const m of matches) {
        if (!isPlayedMatch(m, nowSecs)) continue;
        const key = (m.date_unix||0) + "_" + (m.homeID||"") + "_" + (m.awayID||"");
        if (serverCacheKeys.has(key)) continue;
        const slim = slimM(m, lg);
        if (m.homeID) { if (!localExtra[m.homeID]) localExtra[m.homeID] = []; localExtra[m.homeID].push(slim); }
        if (m.awayID) { if (!localExtra[m.awayID]) localExtra[m.awayID] = []; localExtra[m.awayID].push(slim); }
        serverCacheKeys.add(key);
      }
    };

    const preds = [];
    const leagueSids = Object.keys(leagueFixtures).filter(sid => leagueFixtures[sid].length);

    const leagueData = await Promise.all(leagueSids.map(async (sid) => {
      const leagueName = LEAGUE_NAMES[parseInt(sid, 10)] || "League " + sid;
      const [matchRes, teamRes] = await Promise.all([
        fetchLeagueMatches(sid).catch(e => { console.error("[" + sid + "] match: " + e.message); return { data: [] }; }),
        fetchTeamStats(sid).catch(e => { console.error("[" + sid + "] team: " + e.message); return { data: [] }; }),
      ]);

      // FIX 3: pass ALL matches to addToLocalExtra (not just complete),
      // and use isPlayedMatch for completed filter too
      const allMatches = matchRes.data || [];
      addToLocalExtra(allMatches, leagueName);
      const completed = allMatches.filter(m => isPlayedMatch(m, nowSecs));

      const needsPrevMatches = completed.length < 5 && PREV_SEASON[sid];
      const needsPrevTeams   = (teamRes.data || []).length === 0 && PREV_SEASON[sid];
      if (needsPrevMatches || needsPrevTeams) {
        const prevSid = PREV_SEASON[sid];
        const [prevMatch, prevTeam] = await Promise.all([
          needsPrevMatches ? fetchLeagueMatches(prevSid).catch(() => ({ data: [] })) : Promise.resolve({ data: [] }),
          needsPrevTeams   ? fetchTeamStats(prevSid).catch(() => ({ data: [] }))     : Promise.resolve({ data: [] }),
        ]);
        if (needsPrevMatches) addToLocalExtra(prevMatch.data || [], leagueName);
        if (needsPrevTeams)   teamRes.data = (teamRes.data || []).concat(prevTeam.data || []);
      }

      const teamMap = {};
      for (const t of (teamRes.data || [])) teamMap[t.id] = t;
      return { sid, leagueName, teamMap, fixtures: leagueFixtures[sid] };
    }));

    for (const { sid, leagueName, teamMap, fixtures } of leagueData) {
      for (const fix of fixtures) {
        const homeId   = fix.homeID || fix.home_id;
        const awayId   = fix.awayID || fix.away_id;
        const homeTeam = teamMap[homeId];
        const awayTeam = teamMap[awayId];
        const matchDate = fix.date_unix ? unixToLocalDate(fix.date_unix, tzOffset) : fix._date;

        const mergedCache = (tid) => (SERVER_MATCH_CACHE[tid] || []).concat(localExtra[tid] || []);
        const hLast5 = buildLast5(homeId, { [homeId]: mergedCache(homeId), [awayId]: mergedCache(awayId) });
        const aLast5 = buildLast5(awayId, { [homeId]: mergedCache(homeId), [awayId]: mergedCache(awayId) });
        const hAvgFH = hLast5.length ? +(hLast5.reduce((s, g) => s + g.fhFor + g.fhAgst, 0) / hLast5.length).toFixed(2) : null;
        const aAvgFH = aLast5.length ? +(aLast5.reduce((s, g) => s + g.fhFor + g.fhAgst, 0) / aLast5.length).toFixed(2) : null;

        let snap = null, result = null;
        const missing = !homeTeam || !awayTeam;

        // Build L5 aggregates from last5 arrays
        const hL5 = last5Form(hLast5);
        const aL5 = last5Form(aLast5);
        const l5Data = (hL5 && aL5) ? { home: hL5, away: aL5 } : null;

        if (!missing) {
          const hStats = extractStats(homeTeam, "home");
          const aStats = extractStats(awayTeam, "away");
          snap   = { fetchedAt, home: hStats, away: aStats };
          if (l5Data) snap.l5 = l5Data;
          result = computeSignals(snap, hLast5, aLast5);
        } else if (hLast5.length >= 1 && aLast5.length >= 1) {
          // Synthetic stats from match history when API stats are missing
          const hHome = hLast5.filter(g => g.venue === "H");
          const aAway = aLast5.filter(g => g.venue === "A");
          const hGames = hHome.length >= 2 ? hHome : hLast5;
          const aGames = aAway.length >= 2 ? aAway : aLast5;
          const avg = (arr, fn) => arr.reduce((s, g) => s + fn(g), 0) / arr.length;
          const hScored  = +avg(hGames, g => g.fhFor).toFixed(2);
          const hConced  = +avg(hGames, g => g.fhAgst).toFixed(2);
          const aScored  = +avg(aGames, g => g.fhFor).toFixed(2);
          const aConced  = +avg(aGames, g => g.fhAgst).toFixed(2);
          const hT1pct   = +(hGames.filter(g => g.fhFor + g.fhAgst > 2).length / hGames.length * 100).toFixed(0);
          const aT1pct   = +(aGames.filter(g => g.fhFor + g.fhAgst > 2).length / aGames.length * 100).toFixed(0);
          const hStats = { name: fix.home_name || "", scored_fh: hScored, conced_fh: hConced, t1_pct: hT1pct, cn010_avg: 0, sot_avg: 0, mp: hGames.length, mpRole: hGames.length };
          const aStats = { name: fix.away_name || "", scored_fh: aScored, conced_fh: aConced, t1_pct: aT1pct, cn010_avg: 0, sot_avg: 0, mp: aGames.length, mpRole: aGames.length };
          snap   = { fetchedAt: fetchedAt + " (from history)", home: hStats, away: aStats };
          if (l5Data) snap.l5 = l5Data;
          result = computeSignals(snap, hLast5, aLast5);
        }

        // Phase 2: override prob25/prob15 with league-specific values when n>=30
        if (result) applyLeagueProb(result, parseInt(sid, 10));

        const isComplete = fix.status === "complete" || (fix.status === "incomplete" && isPlayedMatch(fix, nowSecs));
        const fhH = parseInt(fix.ht_goals_team_a || 0, 10);
        const fhA = parseInt(fix.ht_goals_team_b || 0, 10);
        const ftH = parseInt(fix.homeGoalCount   || 0, 10);
        const ftA = parseInt(fix.awayGoalCount   || 0, 10);

        // Freeze the last-5 FH form the signals used, so the recent-form inputs
        // can be re-validated on live data later (snapshots previously kept only
        // season stats, leaving the new signals impossible to backtest live).
        const h5snap = last5Form(hLast5), a5snap = last5Form(aLast5);
        const l5snap = (h5snap && a5snap) ? {
          home: { f: +h5snap.f.toFixed(2), a: +h5snap.a.toFixed(2), t: +h5snap.t.toFixed(2) },
          away: { f: +a5snap.f.toFixed(2), a: +a5snap.a.toFixed(2), t: +a5snap.t.toFixed(2) },
        } : null;

        // Snapshot pre-match prediction values before match completes
        const matchId = fix.id;
        if (result && !isComplete && !CI_SNAPSHOT_CACHE[matchId]) {
          CI_SNAPSHOT_CACHE[matchId] = {
            ci: result.ci, defCi: result.defCi, rank: result.rank,
            label: result.label, prob25: result.prob25, prob15: result.prob15,
            probSource: result.probSource, probSampleN: result.probSampleN, probCombo: result.probCombo,
            eligible: result.eligible, eligible25: result.eligible25, eligible15: result.eligible15, signals: result.signals,
            snap: snap ? {
              fetchedAt: snap.fetchedAt,
              home: { name: snap.home.name, scored_fh: snap.home.scored_fh, conced_fh: snap.home.conced_fh, t1_pct: snap.home.t1_pct, cn010_avg: snap.home.cn010_avg, sot_avg: snap.home.sot_avg },
              away: { name: snap.away.name, scored_fh: snap.away.scored_fh, conced_fh: snap.away.conced_fh, t1_pct: snap.away.t1_pct, cn010_avg: snap.away.cn010_avg, sot_avg: snap.away.sot_avg },
              l5: l5snap,
            } : null,
          };
        }
        // Use frozen snapshot if match is complete and we have one
        const frozen = isComplete ? CI_SNAPSHOT_CACHE[matchId] : null;

        preds.push({
          id: matchId, homeId, awayId,
          league: leagueName, leagueSid: parseInt(sid, 10),
          home: fix.home_name || "", away: fix.away_name || "",
          dt: (fix.date_unix || 0) * 1000,
          matchDate, status: fix.status || "upcoming", missingStats: missing && !result,
          snap: frozen ? frozen.snap : (snap ? {
            fetchedAt: snap.fetchedAt,
            home: { name: snap.home.name, scored_fh: snap.home.scored_fh, conced_fh: snap.home.conced_fh, t1_pct: snap.home.t1_pct, cn010_avg: snap.home.cn010_avg, sot_avg: snap.home.sot_avg },
            away: { name: snap.away.name, scored_fh: snap.away.scored_fh, conced_fh: snap.away.conced_fh, t1_pct: snap.away.t1_pct, cn010_avg: snap.away.cn010_avg, sot_avg: snap.away.sot_avg },
            l5: l5snap,
          } : null),
          rank:     frozen ? frozen.rank     : (result ? result.rank     : 0),
          label:    frozen ? frozen.label    : (result ? result.label    : "Low"),
          prob25:   frozen ? frozen.prob25   : (result ? result.prob25   : 10.0),
          prob15:   frozen ? frozen.prob15   : (result ? result.prob15   : 31.4),
          probSource:  frozen ? frozen.probSource  : (result ? result.probSource  : "global"),
          probSampleN: frozen ? frozen.probSampleN : (result ? result.probSampleN : 0),
          probCombo:   frozen ? frozen.probCombo   : (result ? result.probCombo   : null),
          eligible: frozen ? frozen.eligible : (result ? result.eligible : false),
          eligible25: frozen ? frozen.eligible25 : (result ? result.eligible25 : false),
          eligible15: frozen ? frozen.eligible15 : (result ? result.eligible15 : false),
          ci:       frozen ? frozen.ci       : (result ? result.ci       : 0),
          defCi:    frozen ? frozen.defCi    : (result ? result.defCi    : 0),
          signals:  frozen ? frozen.signals  : (result ? result.signals  : {}),
          hLast5, aLast5, hAvgFH, aAvgFH,
          matchResult: isComplete ? { fhH, fhA, ftH, ftA, hit25: (fhH+fhA)>2, hit15: (fhH+fhA)>1 } : null,
        });
      }
    }

    rebuildServerMatchCache();
    preds.sort((a, b) => b.ci - a.ci || b.rank - a.rank);
  return { preds, dates };
}

// ─── /preds JSON ENDPOINT — kept for potential future use ───────────────────
app.get("/preds", async (req, res) => {
  res.set("Cache-Control", "no-store");
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const { preds } = await computePreds(tzOffset);
    // Fire-and-forget persistence of completed matches to Supabase (no-op if disabled)
    persistEarlySnapshots(preds).catch(e => console.error("early persist error:", e.message));
    persistCompletedPreds(preds).catch(e => console.error("persist error:", e.message));
    res.json({ ok: true, preds, rateLimited: Date.now() < RATE_LIMITED_UNTIL });
  } catch(e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Mine candidate last-5 signals against the clean cohort. Reads snap.l5
// (home/away L5 FH scored/conceded/total averages) directly, so it's
// independent of the stored rank/signals fields and won't be polluted by
// old captures that predate the A+E migration. Reports lift for a grid
// of threshold combinations so we can see whether a stronger or different
// last-5 pattern exists in data we already have.
app.get("/last5-mine", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("hit_25, hit_15, snap")
        .not("hit_25", "is", null)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }
    const cohortAll = all.length;
    const cohort = all.filter(m => m.snap && m.snap.l5 && m.snap.l5.home && m.snap.l5.away);
    const n = cohort.length;
    const base25 = n ? cohort.filter(m => m.hit_25).length / n : 0;
    const base15 = n ? cohort.filter(m => m.hit_15).length / n : 0;

    const feats = (m) => ({
      hT: m.snap.l5.home.t || 0, aT: m.snap.l5.away.t || 0,
      hF: m.snap.l5.home.f || 0, hA: m.snap.l5.home.a || 0,
      aF: m.snap.l5.away.f || 0, aA: m.snap.l5.away.a || 0,
    });

    const test = (label, pred) => {
      const fired = cohort.filter(m => pred(feats(m)));
      const fN = fired.length;
      if (!fN) return { label, n: 0, actual25: 0, actual15: 0, lift25: 0, lift15: 0 };
      const h25 = fired.filter(m => m.hit_25).length;
      const h15 = fired.filter(m => m.hit_15).length;
      return {
        label, n: fN,
        actual25: +(h25 / fN * 100).toFixed(1),
        actual15: +(h15 / fN * 100).toFixed(1),
        lift25: base25 ? +(h25 / fN / base25).toFixed(2) : 0,
        lift15: base15 ? +(h15 / fN / base15).toFixed(2) : 0,
      };
    };

    const results = [];
    results.push(test("A: Recent Intensity (hT+aT>=4.0)", x => x.hT + x.aT >= 4.0));
    results.push(test("B: Both Attack (hF>=0.81 & aF>=0.81)", x => x.hF >= 0.81 && x.aF >= 0.81));
    results.push(test("C: away attack + home leak (aF>=1 & hA>=0.8)", x => x.aF >= 1.0 && x.hA >= 0.8));
    results.push(test("A+B: FH>2.5 (intensity + both attack)", x => x.hT + x.aT >= 4.0 && x.hF >= 0.81 && x.aF >= 0.81));
    results.push(test("A+C: intensity + away attack", x => x.hT + x.aT >= 4.0 && x.aF >= 1.0 && x.hA >= 0.8));
    results.push(test("B+C: both attack + away strong", x => x.hF >= 0.81 && x.aF >= 0.81 && x.aF >= 1.0 && x.hA >= 0.8));
    results.push(test("A+B+C: all three", x => x.hT + x.aT >= 4.0 && x.hF >= 0.81 && x.aF >= 0.81 && x.aF >= 1.0 && x.hA >= 0.8));
    for (const t of [3.0, 3.5, 4.5, 5.0, 5.5]) results.push(test("hT+aT>=" + t, x => x.hT + x.aT >= t));
    for (const t of [1.5, 2.0, 2.5, 3.0]) {
      results.push(test("home.t>=" + t, x => x.hT >= t));
      results.push(test("away.t>=" + t, x => x.aT >= t));
    }
    for (const af of [0.8, 1.0, 1.2]) {
      for (const da of [0.8, 1.0, 1.2]) {
        results.push(test("hF>=" + af + " & aA>=" + da + " (home atk vs away leak)", x => x.hF >= af && x.aA >= da));
        results.push(test("aF>=" + af + " & hA>=" + da + " (away atk vs home leak)", x => x.aF >= af && x.hA >= da));
      }
    }
    for (const t of [0.6, 0.8, 0.81, 0.82, 0.83, 0.84, 0.85, 0.9, 1.0]) {
      results.push(test("hF>=" + t + " & aF>=" + t + " (both attack)", x => x.hF >= t && x.aF >= t));
      results.push(test("hA>=" + t + " & aA>=" + t + " (both leaky)", x => x.hA >= t && x.aA >= t));
    }
    for (const t of [1.0, 1.2, 1.4]) {
      results.push(test("hF>=" + t + " (home attack)", x => x.hF >= t));
      results.push(test("aF>=" + t + " (away attack)", x => x.aF >= t));
      results.push(test("hA>=" + t + " (home leak)", x => x.hA >= t));
      results.push(test("aA>=" + t + " (away leak)", x => x.aA >= t));
    }

    const actionable = results.filter(r => r.n >= 20).sort((a, b) => b.lift25 - a.lift25);
    const tight = results.filter(r => r.n >= 30).sort((a, b) => b.lift25 - a.lift25);

    res.json({
      ok: true,
      cohortTotal: cohortAll,
      cohortWithL5: n,
      withoutL5: cohortAll - n,
      baseRate25: +(base25 * 100).toFixed(1),
      baseRate15: +(base15 * 100).toFixed(1),
      topByLift25_n20: actionable.slice(0, 15),
      topByLift15_n20: actionable.slice().sort((a, b) => b.lift15 - a.lift15).slice(0, 15),
      topByLift25_n30: tight.slice(0, 10),
      allResults: results,
      note: "Cohort = live-captured matches with snap.l5 present. Lift > 1.5 with n>=30 is a candidate signal. Compare to current A (hT+aT>=4.0) to see if a stronger or different last-5 pattern exists.",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── RANK 0 MINING: find hidden signals within no-signal matches ────────────────
// Tests patterns ONLY on rank 0 matches (neither A nor B fire) to find
// asymmetric or other patterns that could extract value from the "no signal" group.
app.get("/last5-rank0", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("hit_25, hit_15, snap")
        .not("hit_25", "is", null)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }
    const cohortAll = all.length;
    const withL5 = all.filter(m => m.snap && m.snap.l5 && m.snap.l5.home && m.snap.l5.away);

    const feats = (m) => ({
      hT: m.snap.l5.home.t || 0, aT: m.snap.l5.away.t || 0,
      hF: m.snap.l5.home.f || 0, hA: m.snap.l5.home.a || 0,
      aF: m.snap.l5.away.f || 0, aA: m.snap.l5.away.a || 0,
    });

    // Filter to rank 0: neither A (hT+aT>=4.0) nor B (hF>=0.81 & aF>=0.81)
    const rank0 = withL5.filter(m => {
      const f = feats(m);
      const sigA = f.hT + f.aT >= 4.0;
      const sigB = f.hF >= 0.81 && f.aF >= 0.81;
      return !sigA && !sigB;
    });

    const n = rank0.length;
    const base25 = n ? rank0.filter(m => m.hit_25).length / n : 0;
    const base15 = n ? rank0.filter(m => m.hit_15).length / n : 0;

    const test = (label, pred) => {
      const fired = rank0.filter(m => pred(feats(m)));
      const fN = fired.length;
      if (!fN) return { label, n: 0, actual25: 0, actual15: 0, lift25: 0, lift15: 0 };
      const h25 = fired.filter(m => m.hit_25).length;
      const h15 = fired.filter(m => m.hit_15).length;
      return {
        label, n: fN,
        actual25: +(h25 / fN * 100).toFixed(1),
        actual15: +(h15 / fN * 100).toFixed(1),
        lift25: base25 ? +(h25 / fN / base25).toFixed(2) : 0,
        lift15: base15 ? +(h15 / fN / base15).toFixed(2) : 0,
      };
    };

    const results = [];
    // Asymmetric patterns: one team strong, opponent weak
    for (const hThresh of [1.5, 2.0, 2.5]) results.push(test("home.t>=" + hThresh, x => x.hT >= hThresh));
    for (const aThresh of [1.5, 2.0, 2.5]) results.push(test("away.t>=" + aThresh, x => x.aT >= aThresh));

    // One team attacking, other leaking
    for (const atk of [0.8, 1.0, 1.2]) {
      for (const leak of [0.8, 1.0, 1.2]) {
        results.push(test("hF>=" + atk + " & aA>=" + leak, x => x.hF >= atk && x.aA >= leak));
        results.push(test("aF>=" + atk + " & hA>=" + leak, x => x.aF >= atk && x.hA >= leak));
      }
    }

    // Single-side attacks
    for (const t of [1.0, 1.2, 1.4]) {
      results.push(test("hF>=" + t + " (home attack only)", x => x.hF >= t));
      results.push(test("aF>=" + t + " (away attack only)", x => x.aF >= t));
    }

    // Single-side leaks
    for (const t of [1.0, 1.2]) {
      results.push(test("hA>=" + t + " (home leaks)", x => x.hA >= t));
      results.push(test("aA>=" + t + " (away leaks)", x => x.aA >= t));
    }

    const actionable = results.filter(r => r.n >= 20).sort((a, b) => b.lift25 - a.lift25);

    res.json({
      ok: true,
      rank0Total: n,
      rank0Pct: withL5.length ? +(n / withL5.length * 100).toFixed(1) : 0,
      baseRate25: +(base25 * 100).toFixed(1),
      baseRate15: +(base15 * 100).toFixed(1),
      topByLift25_n20: actionable.slice(0, 15),
      topByLift15_n20: actionable.slice().sort((a, b) => b.lift15 - a.lift15).slice(0, 15),
      allResults: results,
      note: "Rank 0 cohort = matches where neither A (hT+aT>=4.0) nor B (hF>=0.81 & aF>=0.81) fire. This is where we look for NEW signals to capture value left on the table.",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Diagnostic: how often A and E fire on CURRENT upcoming matches (live signal
// code, not the persisted snapshots that /signal-backtest reads). Use this to
// tell whether Signal E is genuinely dormant or whether stored snapshots are
// just stale from before the A+E migration.
app.get("/signal-fires", async (req, res) => {
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const { preds } = await computePreds(tzOffset);
    const upcoming = preds.filter(p => p.status !== "complete" && !p.matchResult && p.snap);
    let aFires = 0, eFires = 0, aeFires = 0;
    const eExamples = [];
    for (const p of upcoming) {
      const aMet = !!(p.signals && p.signals.A && p.signals.A.met);
      const eMet = !!(p.signals && p.signals.E && p.signals.E.met);
      if (aMet) aFires++;
      if (eMet) eFires++;
      if (aMet && eMet) aeFires++;
      if (eMet && eExamples.length < 10) {
        eExamples.push({
          match: p.home + " vs " + p.away, league: p.league,
          t1_pct: p.snap.home && p.snap.home.t1_pct,
          scored_fh: p.snap.home && p.snap.home.scored_fh,
        });
      }
    }
    const topHomeByT1 = upcoming
      .filter(p => p.snap.home && p.snap.home.t1_pct != null)
      .sort((a, b) => (b.snap.home.t1_pct || 0) - (a.snap.home.t1_pct || 0))
      .slice(0, 15)
      .map(p => ({
        match: p.home + " vs " + p.away, league: p.league,
        t1_pct: p.snap.home.t1_pct, scored_fh: p.snap.home.scored_fh,
        eMet: !!(p.signals && p.signals.E && p.signals.E.met),
      }));
    res.json({
      ok: true,
      upcomingTotal: upcoming.length,
      aFires, eFires, aeFires,
      aFireRate: upcoming.length ? +(aFires / upcoming.length * 100).toFixed(1) : 0,
      eFireRate: upcoming.length ? +(eFires / upcoming.length * 100).toFixed(1) : 0,
      eExamples,
      topHomeByT1,
      note: "Live signals on upcoming matches. If eFires=0 but topHomeByT1 has rows above 25/0.94, signals.E is being computed but no match clears the bar. If t1_pct is generally <25, the E threshold is unreachable for this league set.",
    });
  } catch(e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── PATTERN CANDIDATE TESTING ─────────────────────────────────────────────
// Test Tier 1 pattern candidates against live FH>2.5 matches
app.get("/test-pattern-candidates", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const { data: matches, error } = await supabase
      .from("match_results")
      .select("match_id, home_name, away_name, ht_home, ht_away, snap")
      .not("snap", "is", null)
      .not("snap->>fetchedAt", "eq", "historical-import")
      .not("snap->>fetchedAt", "eq", "backfill");
    if (error) throw error;

    const fh25 = (matches || [])
      .filter(m => m.snap && m.snap.l5 && m.snap.l5.home && m.snap.l5.away && (parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10)) > 2.5);

    const patterns = {
      "Passive-Passive Exclusion (both < 1.0)": m => {
        const ht = m.snap.l5.home.t || 0;
        const at = m.snap.l5.away.t || 0;
        return !(ht < 1.0 && at < 1.0);
      },
      "Combined Conceding >= 1.4": m => {
        const hc = m.snap.l5.home.a || 0;
        const ac = m.snap.l5.away.a || 0;
        return (hc + ac) >= 1.4;
      },
      "Combined Conceding >= 1.6": m => {
        const hc = m.snap.l5.home.a || 0;
        const ac = m.snap.l5.away.a || 0;
        return (hc + ac) >= 1.6;
      },
      "Combined Conceding >= 1.8": m => {
        const hc = m.snap.l5.home.a || 0;
        const ac = m.snap.l5.away.a || 0;
        return (hc + ac) >= 1.8;
      },
      "Explosion (combined >= 4.0)": m => {
        const ht = m.snap.l5.home.t || 0;
        const at = m.snap.l5.away.t || 0;
        return (ht + at) >= 4.0;
      },
      "Explosion (strict: home >= 2.0 AND away >= 1.8)": m => {
        const ht = m.snap.l5.home.t || 0;
        const at = m.snap.l5.away.t || 0;
        return ht >= 2.0 && at >= 1.8;
      },
      "Dominant-Side Imbalance (max >= 2.0 AND min <= 1.2)": m => {
        const ht = m.snap.l5.home.t || 0;
        const at = m.snap.l5.away.t || 0;
        const max_t = Math.max(ht, at);
        const min_t = Math.min(ht, at);
        return max_t >= 2.0 && min_t <= 1.2;
      },
    };

    const results = {};
    for (const [name, test] of Object.entries(patterns)) {
      const hits = fh25.filter(m => test(m));
      results[name] = {
        count: hits.length,
        total_fh25: fh25.length,
        hit_rate: fh25.length ? +(hits.length / fh25.length * 100).toFixed(1) : 0,
        lift: fh25.length ? +(hits.length / fh25.length / 1.0).toFixed(2) : 0,
        examples: hits.slice(0, 3).map(m => m.home_name + " vs " + m.away_name),
      };
    }

    res.json({
      ok: true,
      fh25_total: fh25.length,
      patterns: results,
      note: "Testing Tier 1 pattern candidates against live FH>2.5 matches. Hit rate = % of FH>2.5 matches this pattern catches. Lift = hit_rate vs baseline.",
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── MAIN ROUTE — serves shell instantly, /preds fetches data async ──────────
app.get("/", (req, res) => {
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const dates    = getDates(tzOffset);
    // Kick off background league list fetch if needed — no blocking
    if (Object.keys(LEAGUE_NAMES).length === 0 && !LEAGUE_LIST_LOADING) fetchLeagueList();
    res.send(buildHTML([], dates, Date.now() < RATE_LIMITED_UNTIL, tzOffset));
  } catch(e) {
    console.error(e);
    if (!res.headersSent) res.status(500).send("<pre>Error: " + e.message + "</pre>");
  }
});

function buildHTML(preds, dates, rateLimited, tzOffset) {
  const predsJSON = JSON.stringify(preds)
    .replace(/</g,"\\u003c").replace(/>/g,"\\u003e").replace(/&/g,"\\u0026");

  const CSS = `
*{box-sizing:border-box;margin:0;padding:0}
body{background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;font-size:15px}
.hdr{background:#0f1923;padding:14px 16px;position:sticky;top:0;z-index:10}
.hdr-inner{max-width:860px;margin:0 auto}
.hdr-top{display:flex;align-items:center;justify-content:space-between;gap:8px}
.hdr-sub{font-size:10px;letter-spacing:2px;text-transform:uppercase;color:#ff6b35;font-weight:500}
.hdr-title{font-size:20px;font-weight:700;color:#fff;margin-top:2px}
.tabs{display:flex;gap:6px;margin-top:10px;flex-wrap:wrap}
.tab{padding:5px 11px;border-radius:20px;font-size:12px;font-weight:500;cursor:pointer;border:1px solid rgba(255,255,255,0.15);color:rgba(255,255,255,0.6);background:transparent;white-space:nowrap}
.tab.active{background:#ff6b35;border-color:#ff6b35;color:#fff}
.body{padding:12px 14px;max-width:860px;margin:0 auto}
.pill-bar{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px}
.lpill{display:inline-flex;align-items:center;gap:5px;padding:5px 11px;border-radius:20px;font-size:12px;font-weight:500;cursor:pointer;border:1.5px solid #e5e7eb;background:#fff;color:#374151;white-space:nowrap;transition:all .15s;user-select:none}
.lpill:hover{border-color:#d1d5db;background:#f9fafb}
.lpill.open{background:#0f1923;border-color:#0f1923;color:#fff}
.lpill-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.lpill-badge{font-size:10px;opacity:.7;margin-left:1px}
.league-section{margin-bottom:16px}
.league-section-hdr{font-size:11px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.7px;margin-bottom:8px;padding-bottom:6px;border-bottom:1px solid #e5e7eb}
.card{background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;margin-bottom:12px}
.card-accent{height:4px}
.card-inner{padding:14px}
.rank-pill{text-align:center;border-radius:10px;padding:8px 10px;min-width:76px;flex-shrink:0;border:1px solid}
.r2{background:#e8f5e9;border-color:#a5d6a7} .r2 .rn,.r2 .rl{color:#1b5e20}
.r1{background:#fff8e1;border-color:#ffe082} .r1 .rn,.r1 .rl{color:#e65100}
.r0{background:#f3f4f6;border-color:#e5e7eb} .r0 .rn,.r0 .rl{color:#9ca3af}
.rn{font-size:24px;font-weight:700;line-height:1}
.rl{font-size:10px;font-weight:500;margin-top:2px}
.prob-strip{display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap}
.pp{display:flex;align-items:center;gap:5px;padding:4px 9px;border-radius:20px;border:0.5px solid}
.pp15{background:#eff6ff;border-color:#bfdbfe}
.pp25{background:#f0fdf4;border-color:#bbf7d0}
.pp-dot{width:5px;height:5px;border-radius:50%;flex-shrink:0}
.pp15 .pp-dot{background:#2563eb} .pp25 .pp-dot{background:#16a34a}
.pp-lbl{font-size:10px;font-weight:500}
.pp15 .pp-lbl{color:#1e40af} .pp25 .pp-lbl{color:#166534}
.pp-val{font-size:12px;font-weight:700}
.pp15 .pp-val{color:#1d4ed8} .pp25 .pp-val{color:#15803d}
.teams{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:8px;margin-bottom:12px}
.team-box{background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:10px}
.team-role{font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:#9ca3af;margin-bottom:2px}
.team-name{font-size:14px;font-weight:700;color:#111827;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:6px}
.form-strip{display:flex;gap:3px;margin-bottom:7px;align-items:center;flex-wrap:wrap}
.form-lbl{font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.5px;margin-right:1px}
.fw{display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:4px;font-size:9px;font-weight:700;background:#dcfce7;color:#166634}
.fd{display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:4px;font-size:9px;font-weight:700;background:#f3f4f6;color:#6b7280;border:1px solid #e5e7eb}
.fl{display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:4px;font-size:9px;font-weight:700;background:#fee2e2;color:#b91c1c}
.stat-grid{display:grid;grid-template-columns:1fr 1fr;gap:4px}
.chip{background:#fff;border:0.5px solid #e5e7eb;border-radius:6px;padding:5px 6px}
.chip-lbl{font-size:7px;color:#9ca3af;text-transform:uppercase;letter-spacing:.3px;margin-bottom:2px}
.chip-val{font-size:13px;font-weight:700;color:#111827}
.chip-thr{font-size:8px;color:#9ca3af;margin-top:1px}
.chip.g-light{background:#e8f5e9;border-color:#a5d6a7}
.chip.g-light .chip-lbl{color:#388e3c}.chip.g-light .chip-val{color:#2e7d32}.chip.g-light .chip-thr{color:#388e3c}
.chip.g-bright{background:#1b5e20;border-color:#1b5e20}
.chip.g-bright .chip-lbl{color:#a5d6a7}.chip.g-bright .chip-val{color:#a5d6a7}.chip.g-bright .chip-thr{color:#a5d6a7}
.chip.r-light{background:#fef2f2;border-color:#fca5a5}
.chip.r-light .chip-lbl{color:#b91c1c}.chip.r-light .chip-val{color:#991b1b}.chip.r-light .chip-thr{color:#b91c1c}
.chip.r-bright{background:#b71c1c;border-color:#b71c1c}
.chip.r-bright .chip-lbl{color:#ef9a9a}.chip.r-bright .chip-val{color:#fff}.chip.r-bright .chip-thr{color:#ef9a9a}
.signals{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:10px}
.sig{display:flex;align-items:center;gap:4px;font-size:10px;padding:3px 8px;border-radius:20px;font-weight:500;border:1px solid}
.sig-y{background:#f0fdf4;color:#15803d;border-color:#a5d6a7}
.sig-n{background:#fef2f2;color:#b91c1c;border-color:#fca5a5}
.sig-dot{width:5px;height:5px;border-radius:50%;flex-shrink:0}
.sig-y .sig-dot{background:#16a34a} .sig-n .sig-dot{background:#dc2626}
.ci-bar{border-radius:8px;padding:9px 12px;font-size:11px;font-family:monospace;margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px}
.ci-cold{background:#f9fafb;border:0.5px solid #e5e7eb;color:#374151}
.ci-light{background:#e8f5e9;border:1px solid #a5d6a7;color:#2e7d32}
.ci-bright{background:#1b5e20;border:1.5px solid #388e3c;color:#a5d6a7}
.result-box{border-radius:10px;overflow:hidden;margin-bottom:12px;display:flex;flex-wrap:wrap;border:1px solid #e5e7eb}
.res-cell{padding:9px 12px;text-align:center;flex:1;min-width:70px}
.toggle-btn{font-size:12px;color:#6b7280;cursor:pointer;padding-top:11px;display:flex;align-items:center;gap:5px;border-top:1px solid #f3f4f6;margin-top:4px}
.details{display:none;padding-top:10px}
.details.open{display:block}
.form-wrap{border-top:1px solid #f3f4f6;padding-top:10px}
.form-team-lbl{font-size:10px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px}
.tbl-scroll{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}
.ftable{width:100%;border-collapse:collapse;font-size:11px;margin-bottom:6px}
.ftable th{background:#f9fafb;padding:4px 5px;text-align:left;font-size:9px;color:#6b7280;font-weight:600;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid #e5e7eb;white-space:nowrap}
.ftable td{padding:4px 5px;border-bottom:1px solid #f9fafb;white-space:nowrap}
.ftable tr:last-child td{border-bottom:none}
.ftable tfoot td{background:#f9fafb;font-weight:600;font-size:10px;padding:4px 5px;border-top:1px solid #e5e7eb}
.fw2{color:#15803d;font-weight:700} .fl2{color:#b91c1c;font-weight:700} .fd2{color:#6b7280;font-weight:700}
@media(max-width:480px){
  .hdr-title{font-size:17px}.rn{font-size:20px}.team-name{font-size:12px}
  .sig{font-size:9px;padding:2px 6px}.ci-bar{font-size:10px}
  .ftable{font-size:9px;table-layout:fixed;width:100%}.ftable th,.ftable td{padding:2px 2px;overflow:hidden;text-overflow:ellipsis}
}
@media(min-width:640px){
  .hdr{padding:14px 20px}.body{padding:16px 20px}.card-inner{padding:16px}
  .hdr-title{font-size:22px}.tab{font-size:13px;padding:6px 14px}
  .sig{font-size:11px;padding:4px 9px}.ftable{font-size:12px}.ftable th{font-size:10px}
}`.trim();

  let J = "";
  J += "var ALL=[];";
  J += "var DATES=" + JSON.stringify(dates) + ";";
  J += "var TZ=" + JSON.stringify(tzOffset) + ";";
  J += "var DAY_LABELS=['Today','Tomorrow','Day 3','Day 4','Day 5'];";
  J += "var activeDate=DATES[0]||null;";
  J += "var activeView='days';";
  J += "var openLeague=null;";

  J += "function esc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;');}";
  J += "function fmtDate(d){return new Date(d).toLocaleDateString('en-GB',{weekday:'long',day:'2-digit',month:'short'});}";
  J += "function rankAccent(r){return r>=2?'#2e7d32':r===1?'#e65100':'#9ca3af';}";
  J += "function rankCls(r){return r>=2?'r2':r===1?'r1':'r0';}";
  J += "function lgLabel(r){return r>=2?'Fire \ud83d\udd25':r===1?'Signal \ud83d\udce1':'Low';}";
  J += "function fLetter(r){return r==='W'?'<span class=\"fw\">W</span>':r==='L'?'<span class=\"fl\">L</span>':'<span class=\"fd\">D</span>';}";
  J += "function mkChip(lbl,val,thr,state){var cls='chip'+(state?' '+state:'');return '<div class=\"'+cls+'\">'+'<div class=\"chip-lbl\">'+esc(lbl)+'</div><div class=\"chip-val\">'+esc(val)+'</div><div class=\"chip-thr\">'+esc(thr)+'</div></div>';}";
  // betPill: Fire (🔥) and Dart (🎯) badges based on calibrated eligibility flags
  // Fire (🔥): eligible25 = true (FH>2.5 probability ≥ 40%, primarily B+C or A+B+C combos)
  // Dart (🎯): eligible15 = true (FH>1.5 probability ≥ 50%, primarily C, A+B, or A+B+C combos)
  J += "function betPill(m){if(!m)return '';";
  J += "  var h='';";
  J += "  if(m.eligible25)h+='<div style=\"display:inline-block;background:#dc2626;color:#fff;font-size:11px;font-weight:700;padding:3px 9px;border-radius:6px;margin-right:4px\" title=\"FH>2.5 candidate (prob ≥ 40%)\">🔥</div>';";
  J += "  if(m.eligible15)h+='<div style=\"display:inline-block;background:#15803d;color:#fff;font-size:11px;font-weight:700;padding:3px 9px;border-radius:6px;letter-spacing:.5px;margin-right:6px\" title=\"FH>1.5 candidate (prob ≥ 50%)\">🎯</div>';";
  J += "  return h;}";

  J += "function renderTabs(){";
  J += "  var el=document.getElementById('dayTabs');var h='';";
  J += "  for(var i=0;i<DATES.length;i++){";
  J += "    var d=DATES[i];";
  J += "    var cnt=ALL.filter(function(p){return p.matchDate===d;}).length;";
  J += "    h+='<button class=\"tab'+(activeView==='days'&&d===activeDate?' active':'')+'\" data-di=\"'+i+'\">'+esc(DAY_LABELS[i]||d)+' <span style=\"font-size:10px;opacity:.7\">('+cnt+')</span></button>';";
  J += "  }";
  J += "  h+='<button class=\"tab'+(activeView==='bestbets'?' active':'')+'\" data-view=\"bestbets\" style=\"background:'+(activeView==='bestbets'?'#ff6b35':'#1b5e20')+';color:#fff;font-weight:700\">\ud83c\udfaf Best Bets</button>';";
  J += "  h+='<button class=\"tab'+(activeView==='history'?' active':'')+'\" data-view=\"history\" style=\"background:'+(activeView==='history'?'#ff6b35':'#374151')+';color:#fff;font-weight:700\">\ud83d\udcca History</button>';";
  J += "  h+='<button class=\"tab'+(activeView==='calibration'?' active':'')+'\" data-view=\"calibration\" style=\"background:'+(activeView==='calibration'?'#ff6b35':'#4f46e5')+';color:#fff;font-weight:700\">\ud83d\udccf Calibration</button>';";
  J += "  el.innerHTML=h;";
  J += "  el.querySelectorAll('[data-di]').forEach(function(btn){btn.addEventListener('click',function(){";
  J += "    var i=Number(btn.getAttribute('data-di'));";
  J += "    activeView='days';activeDate=DATES[i];openLeague=null;renderTabs();renderLeagueList();";
  J += "    document.getElementById('hdrTitle').textContent=fmtDate(new Date(DATES[i]+'T12:00:00'));";
  J += "  });});";
  J += "  var bbBtn=el.querySelector('[data-view=\"bestbets\"]');";
  J += "  if(bbBtn)bbBtn.addEventListener('click',function(){";
  J += "    activeView='bestbets';activeDate=null;renderTabs();renderBestBets();";
  J += "    document.getElementById('hdrTitle').textContent='Best Bets \u2014 Parlays';";
  J += "  });";
  J += "  var hBtn=el.querySelector('[data-view=\"history\"]');";
  J += "  if(hBtn)hBtn.addEventListener('click',function(){";
  J += "    activeView='history';activeDate=null;renderTabs();renderHistory();";
  J += "    document.getElementById('hdrTitle').textContent='History \u2014 Last 7 Days';";
  J += "  });";
  J += "  var cBtn=el.querySelector('[data-view=\"calibration\"]');";
  J += "  if(cBtn)cBtn.addEventListener('click',function(){";
  J += "    activeView='calibration';activeDate=null;renderTabs();renderCalibration();";
  J += "    document.getElementById('hdrTitle').textContent='Calibration \u2014 Clean Cohort';";
  J += "  });";
  J += "}";

  // renderCalibration: lazy-load /calibration, show clean-cohort summary + per-rank table
  J += "var CALIB=null;var CALIB_LOADING=false;";
  J += "function renderCalibration(){";
  J += "  var main=document.getElementById('mainView');";
  J += "  if(CALIB_LOADING){main.innerHTML='<p style=\"color:#6b7280;text-align:center;padding:40px;font-size:13px\">\u23f3 Loading calibration\u2026</p>';return;}";
  J += "  if(!CALIB){";
  J += "    CALIB_LOADING=true;main.innerHTML='<p style=\"color:#6b7280;text-align:center;padding:40px;font-size:13px\">\u23f3 Loading calibration\u2026</p>';";
  J += "    fetch('/calibration').then(function(r){return r.json();}).then(function(d){";
  J += "      CALIB_LOADING=false;";
  J += "      if(!d.ok){main.innerHTML='<p style=\"color:#b91c1c;text-align:center;padding:40px\">'+esc(d.error||'Failed to load')+'</p>';return;}";
  J += "      CALIB=d;renderCalibration();";
  J += "    }).catch(function(e){CALIB_LOADING=false;main.innerHTML='<p style=\"color:#b91c1c;text-align:center;padding:40px\">Failed: '+esc(e.message)+'</p>';});";
  J += "    return;";
  J += "  }";
  J += "  var d=CALIB,s=d.summary||{};";
  J += "  var h='<div style=\"max-width:920px;margin:0 auto\">';";
  J += "  h+='<div style=\"background:#eef2ff;border:1px solid #c7d2fe;border-radius:10px;padding:14px;margin-bottom:14px;font-size:12px;color:#4338ca;line-height:1.5\">';";
  J += "  h+='<strong>Clean cohort:</strong> '+d.cohortSize+' matches captured pre-game (no look-ahead bias). ';";
  J += "  h+='This excludes the 22k historical CSV + backfilled rows, so it\\'s the ONLY honest accuracy measure. ';";
  J += "  h+='Bigger cohort = more reliable. Aim for \u2265200 before trusting any specific rank\\'s row.';";
  J += "  h+='</div>';";
  J += "  if(d.cohortSize===0){h+='<p style=\"color:#6b7280;text-align:center;padding:40px\">No clean-cohort matches yet. The cohort grows daily as live-captured matches complete.</p></div>';main.innerHTML=h;return;}";
  J += "  h+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:16px;margin-bottom:14px\">';";
  J += "  h+='<div style=\"font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px\">Overall \u2014 '+d.cohortSize+' matches</div>';";
  J += "  h+='<div style=\"display:flex;gap:24px;flex-wrap:wrap\">';";
  // Helper for one stat block (predicted vs actual)
  J += "  function blk(lbl,pred,act){var col=(act>=pred-2)?'#0f766e':(act>=pred-5)?'#a16207':'#b91c1c';";
  J += "    return '<div><div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.5px\">'+lbl+'</div>'";
  J += "      +'<div style=\"display:flex;align-items:baseline;gap:6px\"><span style=\"font-size:24px;font-weight:700;color:'+col+'\">'+act+'%</span>'";
  J += "      +'<span style=\"font-size:11px;color:#9ca3af\">actual / '+pred+'% pred</span></div></div>';}";
  J += "  h+=blk('FH > 1.5',s.predicted15,s.actual15);";
  J += "  h+=blk('FH > 2.5',s.predicted25,s.actual25);";
  J += "  h+='</div></div>';";
  J += "  h+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:14px;overflow-x:auto\">';";
  J += "  h+='<div style=\"font-size:13px;font-weight:600;margin-bottom:10px\">By rank \u2014 predicted vs actual</div>';";
  J += "  h+='<table style=\"width:100%;font-size:12px;border-collapse:collapse\"><thead><tr style=\"text-align:left;color:#6b7280;border-bottom:1px solid #e5e7eb\">'";
  J += "    +'<th style=\"padding:6px 8px\">Rank</th><th style=\"padding:6px 8px\">N</th>'";
  J += "    +'<th style=\"padding:6px 8px\">Pred FH&gt;2.5</th><th style=\"padding:6px 8px\">Actual</th>'";
  J += "    +'<th style=\"padding:6px 8px\">Pred FH&gt;1.5</th><th style=\"padding:6px 8px\">Actual</th></tr></thead><tbody>';";
  J += "  for(var r=2;r>=0;r--){var b=d.byRank[r]||{n:0,predicted25:0,actual25:0,predicted15:0,actual15:0};";
  J += "    var lbl=['Low','Signal','Fire'][r];";
  J += "    var c25=(b.n<10)?'#9ca3af':(b.actual25>=b.predicted25-2)?'#0f766e':'#b91c1c';";
  J += "    var c15=(b.n<10)?'#9ca3af':(b.actual15>=b.predicted15-2)?'#0f766e':'#b91c1c';";
  J += "    h+='<tr style=\"border-bottom:1px solid #f3f4f6\">'";
  J += "      +'<td style=\"padding:6px 8px;font-weight:600\">'+r+' '+lbl+'</td>'";
  J += "      +'<td style=\"padding:6px 8px\">'+b.n+(b.n<10?'<span style=\"color:#9ca3af;font-size:10px\"> (low n)</span>':'')+'</td>'";
  J += "      +'<td style=\"padding:6px 8px;color:#6b7280\">'+b.predicted25+'%</td>'";
  J += "      +'<td style=\"padding:6px 8px;font-weight:600;color:'+c25+'\">'+b.actual25+'%</td>'";
  J += "      +'<td style=\"padding:6px 8px;color:#6b7280\">'+b.predicted15+'%</td>'";
  J += "      +'<td style=\"padding:6px 8px;font-weight:600;color:'+c15+'\">'+b.actual15+'%</td>'";
  J += "    +'</tr>';";
  J += "  }";
  J += "  h+='</tbody></table></div>';";
  // By-combo table — same level of granularity the model actually predicts at
  J += "  var comboMeaning={'000':'none','001':'C only','010':'B only','011':'B+C','100':'A only','101':'A+C','110':'A+B','111':'A+B+C'};";
  J += "  var comboKeys=Object.keys(d.byCombo||{}).sort(function(a,b){return d.byCombo[b].n-d.byCombo[a].n;});";
  J += "  if(comboKeys.length){";
  J += "    h+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:14px;margin-top:14px;overflow-x:auto\">';";
  J += "    h+='<div style=\"font-size:13px;font-weight:600;margin-bottom:10px\">By signal combo — predicted vs actual</div>';";
  J += "    h+='<table style=\"width:100%;font-size:12px;border-collapse:collapse\"><thead><tr style=\"text-align:left;color:#6b7280;border-bottom:1px solid #e5e7eb\">'";
  J += "      +'<th style=\"padding:6px 8px\">Combo</th><th style=\"padding:6px 8px\">N</th>'";
  J += "      +'<th style=\"padding:6px 8px\">Pred FH&gt;2.5</th><th style=\"padding:6px 8px\">Actual</th>'";
  J += "      +'<th style=\"padding:6px 8px\">Pred FH&gt;1.5</th><th style=\"padding:6px 8px\">Actual</th></tr></thead><tbody>';";
  J += "    comboKeys.forEach(function(c){var b=d.byCombo[c];";
  J += "      var lbl=comboMeaning[c]||c;";
  J += "      var c25=(b.n<10)?'#9ca3af':(b.actual25>=b.predicted25-2)?'#0f766e':'#b91c1c';";
  J += "      var c15=(b.n<10)?'#9ca3af':(b.actual15>=b.predicted15-2)?'#0f766e':'#b91c1c';";
  J += "      h+='<tr style=\"border-bottom:1px solid #f3f4f6\">'";
  J += "        +'<td style=\"padding:6px 8px;font-family:ui-monospace,monospace;font-weight:600\">'+c+' <span style=\"color:#6b7280;font-weight:400;font-family:system-ui\">'+lbl+'</span></td>'";
  J += "        +'<td style=\"padding:6px 8px\">'+b.n+(b.n<10?'<span style=\"color:#9ca3af;font-size:10px\"> (low n)</span>':'')+'</td>'";
  J += "        +'<td style=\"padding:6px 8px;color:#6b7280\">'+b.predicted25+'%</td>'";
  J += "        +'<td style=\"padding:6px 8px;font-weight:600;color:'+c25+'\">'+b.actual25+'%</td>'";
  J += "        +'<td style=\"padding:6px 8px;color:#6b7280\">'+b.predicted15+'%</td>'";
  J += "        +'<td style=\"padding:6px 8px;font-weight:600;color:'+c15+'\">'+b.actual15+'%</td>'";
  J += "      +'</tr>';";
  J += "    });";
  J += "    h+='</tbody></table></div>';";
  J += "  }";
  J += "  h+='</div>';";
  J += "  main.innerHTML=h;";
  J += "}";

  // renderHistory \u2014 fetches /history and shows summary + per-rank calibration + match list
  J += "var HISTORY=null;var HISTORY_LOADING=false;";
  J += "function renderHistory(){";
  J += "  var main=document.getElementById('mainView');";
  J += "  if(HISTORY_LOADING){main.innerHTML='<p style=\"color:#6b7280;text-align:center;padding:40px;font-size:13px\">\u23f3 Loading history\u2026</p>';return;}";
  J += "  if(!HISTORY){";
  J += "    HISTORY_LOADING=true;main.innerHTML='<p style=\"color:#6b7280;text-align:center;padding:40px;font-size:13px\">\u23f3 Loading history\u2026</p>';";
  J += "    fetch('/history?days=7').then(function(r){return r.json();}).then(function(d){";
  J += "      HISTORY_LOADING=false;";
  J += "      if(!d.ok){main.innerHTML='<p style=\"color:#b91c1c;text-align:center;padding:40px\">'+esc(d.error||'Failed to load history')+'</p>';return;}";
  J += "      HISTORY=d;renderHistory();";
  J += "    }).catch(function(e){HISTORY_LOADING=false;main.innerHTML='<p style=\"color:#b91c1c;text-align:center;padding:40px\">Failed to load: '+esc(e.message)+'</p>';});";
  J += "    return;";
  J += "  }";
  J += "  var d=HISTORY;";
  J += "  var h='<div style=\"max-width:920px;margin:0 auto\">';";
  J += "  if(!d.matches||!d.matches.length){h+='<p style=\"color:#6b7280;text-align:center;padding:40px\">No completed matches in the last '+d.days+' days yet.</p>';h+='</div>';main.innerHTML=h;return;}";
  J += "  h+='<div style=\"font-size:12px;color:#6b7280;margin-bottom:8px\">'+d.matches.length+' matches \u2014 last '+d.days+' days, newest first. Click a row to expand.</div>';";
  J += "  d.matches.forEach(function(m){";
  J += "    var dStr=m.date_unix?new Date(m.date_unix*1000).toISOString().slice(0,10):'';";
  J += "    var rankColor=['#6b7280','#0891b2','#dc2626','#dc2626'][m.rank]||'#6b7280';";
  J += "    var sigs=m.signals||{};";
  J += "    var sigStr=['A','E'].map(function(k){return sigs[k]&&sigs[k].met?k:'\u00b7';}).join('');";
  J += "    h+='<div class=\"hist-row\" data-mid=\"'+m.match_id+'\" style=\"cursor:pointer;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px 12px;display:flex;gap:10px;align-items:center;font-size:12px;margin-bottom:6px\">';";
  J += "    h+='<div style=\"width:22px;height:22px;border-radius:50%;background:'+rankColor+';color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:12px;flex-shrink:0\">'+m.rank+'</div>';";
  J += "    h+=betPill({prob25:Number(m.prob25)||0,prob15:Number(m.prob15)||0,eligible25:!!m.eligible25,eligible15:!!m.eligible15,snap:m.snap});";
  J += "    h+='<div style=\"flex:1;min-width:0\"><div style=\"font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis\">'+esc(m.home_name||'')+' \u2013 '+esc(m.away_name||'')+'</div>';";
  J += "    h+='<div style=\"color:#6b7280;font-size:11px\">'+esc(m.league_name||'\u2014')+' \u00b7 '+dStr+'</div></div>';";
  J += "    h+='<div style=\"text-align:right;flex-shrink:0;font-family:ui-monospace,monospace;color:#374151\">'+sigStr+' \u00b7 CI '+(m.ci||0)+' \u00b7 '+(m.prob25||0)+'%</div>';";
  J += "    h+='<div style=\"text-align:right;flex-shrink:0\"><div style=\"font-weight:700\">HT '+(m.ht_home||0)+'\u2013'+(m.ht_away||0)+'</div><div style=\"font-size:11px;color:#6b7280\">FT '+(m.ft_home||0)+'\u2013'+(m.ft_away||0)+'</div></div>';";
  J += "    h+='<div class=\"hist-chev\" style=\"margin-left:6px;color:#9ca3af;font-size:13px;flex-shrink:0\">\u25bc</div>';";
  J += "    h+='</div>';";
  J += "    h+='<div id=\"histdetails-'+m.match_id+'\" style=\"display:none;margin-bottom:10px\"></div>';";
  J += "  });";
  J += "  h+='</div>';";
  J += "  main.innerHTML=h;";
  // Click-to-expand: lazy-render full match card on first open
  J += "  document.querySelectorAll('.hist-row').forEach(function(row){row.addEventListener('click',function(){";
  J += "    var mid=row.dataset.mid;var det=document.getElementById('histdetails-'+mid);var chev=row.querySelector('.hist-chev');";
  J += "    if(det.style.display==='none'){";
  J += "      det.style.display='block';chev.textContent='\u25b2';";
  J += "      if(!det.dataset.loaded){";
  J += "        var m=HISTORY.matches.find(function(x){return String(x.match_id)===mid;});";
  J += "        if(m){";
  J += "          var lbl=['Low','Signal','Fire'][m.rank]||'Low';";
  J += "          var card={id:m.match_id,homeId:m.home_id,awayId:m.away_id,league:m.league_name||'\u2014',leagueSid:m.competition_id,home:m.home_name||'',away:m.away_name||'',dt:(m.date_unix||0)*1000,matchDate:m.date_unix?new Date(m.date_unix*1000).toISOString().slice(0,10):'',status:'complete',missingStats:!m.snap,rank:m.rank||0,label:lbl,prob25:Number(m.prob25)||0,prob15:Number(m.prob15)||0,eligible:(m.rank||0)>=2,eligible25:!!m.eligible25,eligible15:!!m.eligible15,ci:Number(m.ci)||0,defCi:Number(m.def_ci)||0,signals:m.signals||{},snap:m.snap||null,hLast5:[],aLast5:[],hAvgFH:null,aAvgFH:null,matchResult:{fhH:m.ht_home||0,fhA:m.ht_away||0,ftH:m.ft_home||0,ftA:m.ft_away||0,hit25:!!m.hit_25,hit15:!!m.hit_15}};";
  J += "          det.innerHTML=renderCard(card);det.dataset.loaded='1';";
  J += "          det.querySelectorAll('.toggle-btn').forEach(function(btn){btn.addEventListener('click',function(ev){ev.stopPropagation();var dEl=btn.nextElementSibling;var o=dEl.classList.toggle('open');btn.innerHTML=o?'\u25b2 Hide last 5 games & H2H':'\u25bc Show last 5 games & H2H';if(o){var h2h=dEl.querySelector('[id^=\"h2h-\"]');if(h2h&&!h2h.dataset.loaded){loadH2H(h2h);}}});});";
  J += "        }";
  J += "      }";
  J += "    }else{det.style.display='none';chev.textContent='\u25bc';}";
  J += "  });});";
  J += "}";

  // renderLeagueList — accordion: only one league open at a time
  J += "function renderLeagueList(){";
  J += "  var main=document.getElementById('mainView');";
  J += "  if(!activeDate){main.innerHTML='';return;}";
  J += "  var dp=ALL.filter(function(p){return p.matchDate===activeDate;});";
  J += "  var lmap={};dp.forEach(function(p){if(!lmap[p.league])lmap[p.league]=[];lmap[p.league].push(p);});";
  J += "  var ll=Object.entries(lmap).sort(function(a,b){return a[0].localeCompare(b[0]);});";
  J += "  if(!ll.length){main.innerHTML='<p style=\"color:#6b7280;text-align:center;padding:40px\">No matches found.</p>';return;}";
  J += "  var pills='<div class=\"pill-bar\">';";
  J += "  ll.forEach(function(e){";
  J += "    var lg=e[0],ms=e[1];";
  J += "    var tr=Math.max.apply(null,ms.map(function(p){return p.rank;}));";
  J += "    var en=ms.filter(function(p){return p.eligible;}).length;";
  J += "    var isOpen=openLeague===lg;";
  J += "    var pillCls='lpill'+(isOpen?' open':'');";
  J += "    var dotBg=isOpen?'#ff6b35':rankAccent(tr);";
  J += "    var lgParts=lg.split(' \u00b7 ');";
  J += "    var lgCountry=lgParts.length>1?lgParts[0]:'';";
  J += "    var lgName=lgParts[lgParts.length-1];";
  J += "    var lgShort=lgCountry?'<span style=\"opacity:.55;font-size:10px;margin-right:2px\">'+esc(lgCountry)+'</span>'+esc(lgName):esc(lgName);";
  J += "    var badge=en?'<span class=\"lpill-badge\">('+en+'\u2605)</span>':'<span class=\"lpill-badge\">'+ms.length+'</span>';";
  J += "    pills+='<button class=\"'+pillCls+'\" data-lg=\"'+esc(lg)+'\"><span class=\"lpill-dot\" style=\"background:'+dotBg+'\"></span>'+lgShort+badge+'</button>';";
  J += "  });";
  J += "  pills+='</div>';";
  J += "  var sections='';";
  J += "  ll.forEach(function(e){";
  J += "    var lg=e[0],ms=e[1];";
  J += "    if(openLeague!==lg)return;";
  J += "    var sorted=ms.slice().sort(function(a,b){return (b.rank-a.rank)||(b.ci-a.ci)||((a.dt||0)-(b.dt||0));});";
  J += "    sections+='<div class=\"league-section\"><div class=\"league-section-hdr\">'+esc(lg)+'</div>';";
  J += "    sorted.forEach(function(m){sections+=renderCard(m);});";
  J += "    sections+='</div>';";
  J += "  });";
  J += "  main.innerHTML=pills+sections;";
  // FIX 2: clicking a pill closes all others, toggles itself
  J += "  main.querySelectorAll('.lpill').forEach(function(btn){btn.addEventListener('click',function(){";
  J += "    var lg=btn.getAttribute('data-lg');";
  J += "    openLeague=(openLeague===lg)?null:lg;";
  J += "    renderLeagueList();";
  J += "  });});";
  J += "  document.querySelectorAll('.toggle-btn').forEach(function(btn){btn.addEventListener('click',function(){";
  J += "    var d=btn.nextElementSibling;var o=d.classList.toggle('open');";
  J += "    btn.innerHTML=o?'\u25b2 Hide last 5 games & H2H':'\u25bc Show last 5 games & H2H';";
  J += "    if(o){var h2h=d.querySelector('[id^=\"h2h-\"]');if(h2h&&!h2h.dataset.loaded){loadH2H(h2h);}}";
  J += "  });});";
  J += "  if(openLeague){var oms=lmap[openLeague]||[];oms.forEach(function(m){renderForm(m.id,m.homeId,m.home,m.hLast5,m.hAvgFH);renderForm(m.id,m.awayId,m.away,m.aLast5,m.aAvgFH);});}";
  J += "}";

  // renderForm — FIX 1: mobile-friendly, no min-width, FT hidden on small screens
  J += "function renderForm(mid,tid,tname,games,avgFH){";
  J += "  var el=document.getElementById('form-'+mid+'-'+tid);if(!el)return;";
  J += "  if(!games||!games.length){el.innerHTML='';return;}";
  J += "  var rows=games.map(function(g){";
  J += "    var fhTotal=g.fhFor+g.fhAgst;";
  J += "    var fhStyle=fhTotal>2?'color:#15803d;font-weight:600':fhTotal===2?'color:#1d4ed8;font-weight:600':'color:#111827';";
  J += "    var rc=g.result==='W'?'fw2':g.result==='L'?'fl2':'fd2';";
  J += "    var shortDate=g.date?g.date.slice(5):'';";
  J += "    return '<tr>'";
  J += "      +'<td>'+esc(shortDate)+'</td>'";
  J += "      +'<td style=\"overflow:hidden;text-overflow:ellipsis;max-width:100px\">'+esc(g.opp)+'</td>'";
  J += "      +'<td style=\"text-align:center;color:#9ca3af\">'+esc(g.venue)+'</td>'";
  J += "      +'<td style=\"text-align:center;'+fhStyle+'\">'+g.fhFor+'-'+g.fhAgst+'</td>'";
  J += "      +'<td style=\"text-align:center;color:#9ca3af\">'+g.ftFor+'-'+g.ftAgst+'</td>'";
  J += "      +'<td style=\"text-align:center\" class=\"'+rc+'\">'+g.result+'</td>'";
  J += "    +'</tr>';";
  J += "  }).join('');";
  J += "  var foot='';";
  J += "  if(avgFH!==null&&avgFH!==undefined){foot='<tfoot><tr>'";
  J += "    +'<td colspan=\"3\" style=\"color:#6b7280\">Avg FH goals</td>'";
  J += "    +'<td style=\"text-align:center;font-family:monospace;color:#1d4ed8;font-size:12px\">'+avgFH+'</td>'";
  J += "    +'<td colspan=\"2\" style=\"text-align:center;color:#9ca3af\">('+games.length+')</td>'";
  J += "    +'</tr></tfoot>';}";
  J += "  el.innerHTML='<div class=\"form-team-lbl\">'+esc(tname)+' \u2014 last 5</div>'";
  J += "    +'<div class=\"tbl-scroll\"><table class=\"ftable\"><thead><tr>'";
  J += "    +'<th style=\"width:14%\">Date</th><th style=\"width:36%\">Opponent</th>'";
  J += "    +'<th style=\"width:10%;text-align:center\">H/A</th>'";
  J += "    +'<th style=\"width:12%;text-align:center\">FH</th>'";
  J += "    +'<th style=\"width:12%;text-align:center\">FT</th>'";
  J += "    +'<th style=\"width:10%;text-align:center\">Res</th>'";
  J += "    +'</tr></thead><tbody>'+rows+'</tbody>'+foot+'</table></div>';";
  J += "}";

  // loadH2H — lazy-fetches /h2h on first toggle expansion
  J += "function loadH2H(el){";
  J += "  el.dataset.loaded='1';";
  J += "  var h=el.dataset.h,a=el.dataset.a,hn=el.dataset.hn||'';";
  J += "  el.innerHTML='<div style=\"font-size:11px;color:#9ca3af\">↻ Loading H2H...</div>';";
  J += "  fetch('/h2h?h='+h+'&a='+a+'&limit=10').then(function(r){return r.json();}).then(function(d){";
  J += "    if(!d.ok||!d.matches||!d.matches.length){el.innerHTML='<div style=\"font-size:11px;color:#9ca3af\">No H2H history found.</div>';return;}";
  J += "    var rows=d.matches.map(function(m){";
  J += "      var ds=m.date_unix?new Date(m.date_unix*1000).toISOString().slice(0,10):'';";
  J += "      var fhT=(m.ht_home||0)+(m.ht_away||0);";
  J += "      var ftT=(m.ft_home||0)+(m.ft_away||0);";
  J += "      var ov15=fhT>1?'<span style=\"color:#15803d\">✓1.5</span>':'<span style=\"color:#9ca3af\">·1.5</span>';";
  J += "      var ov25=fhT>2?'<span style=\"color:#dc2626\">✓2.5</span>':'<span style=\"color:#9ca3af\">·2.5</span>';";
  J += "      return '<tr><td style=\"padding:3px 6px;color:#6b7280;font-size:10px;white-space:nowrap\">'+ds+'</td>'";
  J += "        +'<td style=\"padding:3px 6px;font-size:11px;line-height:1.3\">'+esc(m.home_name||'')+' <span style=\"color:#9ca3af\">v</span> '+esc(m.away_name||'')+'</td>'";
  J += "        +'<td style=\"padding:3px 6px;font-weight:700;font-size:12px;text-align:center;white-space:nowrap\">'+(m.ht_home||0)+'-'+(m.ht_away||0)+'</td>'";
  J += "        +'<td style=\"padding:3px 6px;color:#6b7280;font-size:11px;text-align:center;white-space:nowrap\">'+(m.ft_home||0)+'-'+(m.ft_away||0)+'</td>'";
  J += "        +'<td style=\"padding:3px 6px;font-size:10px;text-align:right;white-space:nowrap\">'+ov15+' '+ov25+'</td></tr>';";
  J += "    }).join('');";
  J += "    var nMatches=d.matches.length;";
  J += "    var hits15=d.matches.filter(function(m){return ((m.ht_home||0)+(m.ht_away||0))>1;}).length;";
  J += "    var hits25=d.matches.filter(function(m){return ((m.ht_home||0)+(m.ht_away||0))>2;}).length;";
  J += "    el.innerHTML='<div style=\"font-size:11px;font-weight:600;color:#374151;margin-bottom:4px\">H2H — last '+nMatches+' meetings · FH&gt;1.5 '+hits15+'/'+nMatches+' · FH&gt;2.5 '+hits25+'/'+nMatches+'</div>'";
  J += "      +'<table style=\"width:100%;border-collapse:collapse;table-layout:auto\"><thead><tr style=\"color:#9ca3af;font-size:10px;text-align:left\"><th style=\"padding:3px 6px;white-space:nowrap\">Date</th><th style=\"padding:3px 6px\">Match</th><th style=\"padding:3px 6px;text-align:center;white-space:nowrap\">HT</th><th style=\"padding:3px 6px;text-align:center;white-space:nowrap\">FT</th><th style=\"padding:3px 6px;text-align:right;white-space:nowrap\">FH O/U</th></tr></thead><tbody>'+rows+'</tbody></table>';";
  J += "  }).catch(function(e){el.innerHTML='<div style=\"font-size:11px;color:#b91c1c\">H2H load failed</div>';});";
  J += "}";

  J += "function renderCard(m){";
  J += "  var accent=rankAccent(m.rank);var rc=rankCls(m.rank);";
  J += "  var dt=m.dt?new Date(m.dt).toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}):m.matchDate;";
  J += "  var sb=m.status==='complete'?'<span style=\"background:#f3f4f6;color:#6b7280;border:1px solid #e5e7eb;font-size:10px;padding:2px 8px;border-radius:20px;font-weight:600\">Final</span>'";
  J += "    :m.status==='live'?'<span style=\"background:#fef9c3;color:#ca8a04;border:1px solid #fde047;font-size:10px;padding:2px 8px;border-radius:20px;font-weight:600\">\u25cf Live</span>'";
  J += "    :'<span style=\"background:#f0fdf4;color:#15803d;border:1px solid #a5d6a7;font-size:10px;padding:2px 8px;border-radius:20px;font-weight:600\">Upcoming</span>';";
  J += "  var ps='<div class=\"prob-strip\">'";
  J += "    +'<div class=\"pp pp15\"><div class=\"pp-dot\"></div><span class=\"pp-lbl\">FH over 1.5</span><span class=\"pp-val\">'+m.prob15+'%</span></div>'";
  J += "    +'<div class=\"pp pp25\"><div class=\"pp-dot\"></div><span class=\"pp-lbl\">FH over 2.5</span><span class=\"pp-val\">'+m.prob25+'%</span></div>'";
  J += "    +'</div>';";
  J += "  var rb='';";
  J += "  if(m.matchResult){";
  J += "    var r=m.matchResult;";
  J += "    var c25=r.hit25?'#15803d':'#dc2626',bg25=r.hit25?'#f0fdf4':'#fef2f2',bc25=r.hit25?'#a5d6a7':'#fca5a5';";
  J += "    var c15=r.hit15?'#1d4ed8':'#dc2626',bg15=r.hit15?'#eff6ff':'#fef2f2',bc15=r.hit15?'#bfdbfe':'#fca5a5';";
  J += "    rb='<div class=\"result-box\">'";
  J += "      +'<div class=\"res-cell\" style=\"background:#f9fafb;border-right:1px solid #e5e7eb\">'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:2px\">1st Half</div>'";
  J += "        +'<div style=\"font-family:monospace;font-weight:700;font-size:20px;color:#111827;line-height:1\">'+r.fhH+'\u2013'+r.fhA+'</div>'";
  J += "      +'</div>'";
  J += "      +'<div class=\"res-cell\" style=\"background:'+bg15+';border-right:1px solid '+bc15+'\">'";
  J += "        +'<div style=\"font-size:9px;text-transform:uppercase;letter-spacing:.6px;margin-bottom:2px;color:'+c15+'\">FH over 1.5</div>'";
  J += "        +'<div style=\"font-size:12px;font-weight:700;color:'+c15+'\">'+( r.hit15?'\u2713 HIT':'\u2717 MISS')+'</div>'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;margin-top:1px\">'+m.prob15+'% pre</div>'";
  J += "      +'</div>'";
  J += "      +'<div class=\"res-cell\" style=\"background:'+bg25+';border-right:1px solid '+bc25+'\">'";
  J += "        +'<div style=\"font-size:9px;text-transform:uppercase;letter-spacing:.6px;margin-bottom:2px;color:'+c25+'\">FH over 2.5</div>'";
  J += "        +'<div style=\"font-size:12px;font-weight:700;color:'+c25+'\">'+( r.hit25?'\u2713 HIT':'\u2717 MISS')+'</div>'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;margin-top:1px\">'+m.prob25+'% pre</div>'";
  J += "      +'</div>'";
  J += "      +'<div class=\"res-cell\" style=\"background:#f9fafb\">'";
  J += "        +'<div style=\"font-size:9px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:2px\">Full Time</div>'";
  J += "        +'<div style=\"font-family:monospace;font-weight:700;font-size:16px;color:#374151;line-height:1\">'+r.ftH+'\u2013'+r.ftA+'</div>'";
  J += "      +'</div>'";
  J += "    +'</div>';";
  J += "  }";
  J += "  function teamBox(role,name,tid,last5,sn){";
  J += "    var fs='<div class=\"form-strip\"><span class=\"form-lbl\">Form</span>';";
  J += "    if(last5&&last5.length)last5.forEach(function(g){fs+=fLetter(g.result);});";
  J += "    else fs+='<span style=\"font-size:10px;color:#9ca3af\">...</span>';";
  J += "    fs+='</div>';";
  // FH engagement bar: 5 cells, oldest→newest, height + color show FH goals (fhFor + fhAgst)
  J += "    var fhBar='';";
  J += "    if(last5&&last5.length){";
  J += "      var totals=last5.map(function(g){return (g.fhFor||0)+(g.fhAgst||0);});";
  J += "      var sum=totals.reduce(function(a,b){return a+b;},0);";
  J += "      var avg=(sum/totals.length).toFixed(1);";
  J += "      var colors=['#f3f4f6','#fef3c7','#fde68a','#86efac','#34d399','#15803d','#166534'];";
  J += "      fhBar='<div style=\"display:flex;align-items:flex-end;gap:3px;margin-top:5px\" title=\"FH goals (scored+conceded) per match — oldest to newest\">';";
  J += "      fhBar+='<span style=\"font-size:9px;color:#9ca3af;width:18px;flex-shrink:0\">FH</span>';";
  J += "      totals.forEach(function(t){";
  J += "        var idx=Math.min(t,colors.length-1);";
  J += "        var bg=colors[idx];";
  J += "        var fg=t>=2?'#fff':'#111827';";
  J += "        var hPct=Math.min(100,(t/2)*100);if(t===0)hPct=14;";
  J += "        fhBar+='<div style=\"flex:1;display:flex;flex-direction:column;align-items:stretch;height:24px;justify-content:flex-end\">'";
  J += "          +'<div style=\"background:'+bg+';height:'+hPct+'%;border-radius:2px;display:flex;align-items:center;justify-content:center;color:'+fg+';font-size:10px;font-weight:700;min-height:14px\">'+t+'</div>'";
  J += "        +'</div>';";
  J += "      });";
  J += "      fhBar+='<span style=\"font-size:10px;color:#374151;margin-left:6px;font-weight:600\">avg '+avg+'</span></div>';";
  J += "    }";
  J += "    var chips='';";
  J += "    if(sn){";
  J += "      var sfx=role==='Home'?'(home)':'(away)';";
  J += "      var scState=sn.scored_fh>=1.5?'g-bright':sn.scored_fh>=1.0?'g-light':'';" ;
  J += "      chips+=mkChip('FH Scored '+sfx,sn.scored_fh.toFixed(2),'away \u2265 1.25 \u2192 sig D',scState);";
  J += "      var coState=sn.conced_fh>=1.1?'r-bright':sn.conced_fh>=0.7?'r-light':'';" ;
  J += "      chips+=mkChip('FH Conceded '+sfx,sn.conced_fh.toFixed(2),'\u2265 2.25 combined \u2192 sig C',coState);";
  J += "      var t1State=sn.t1_pct>=25?'g-bright':sn.t1_pct>=15?'g-light':'';" ;
  J += "      chips+=mkChip('FH>2.5 hist '+sfx,sn.t1_pct.toFixed(0)+'%','\u2265 25% \u2192 sig B',t1State);";
  J += "      var cnState=sn.cn010_avg>=0.25?'r-light':'';" ;
  J += "      chips+=mkChip('Early conceded',sn.cn010_avg.toFixed(2),'info only',cnState);";
  J += "      if(sn.sot_avg>0){var sotState=sn.sot_avg>=4?'g-bright':sn.sot_avg>=2.5?'g-light':'';chips+=mkChip('Shots on target '+sfx,(sn.sot_avg).toFixed(1)+'/g','per game',sotState);}";
  J += "    }";
  J += "    return '<div class=\"team-box\"><div class=\"team-role\">'+esc(role)+'</div><div class=\"team-name\">'+esc(name)+'</div>'+fs+fhBar+'<div class=\"stat-grid\">'+chips+'</div></div>';";
  J += "  }";
  J += "  var sigsH='<div class=\"signals\">';";
  J += "  ['A','E'].forEach(function(k){";
  J += "    var s=m.signals[k];if(!s)return;";
  J += "    sigsH+='<div class=\"sig '+(s.met?'sig-y':'sig-n')+'\">'";
  J += "      +'<div class=\"sig-dot\"></div>'+esc(k)+' \u00b7 '+esc(s.label)";
  J += "      +'<span style=\"opacity:.7;margin-left:4px;font-size:9px\">('+esc(s.value)+')</span>'";
  J += "      +'</div>';";
  J += "  });";
  J += "  sigsH+='</div>';";
  J += "  var ciVal=m.ci||0;";
  J += "  var ciCls=ciVal>=4.0?'ci-bar ci-bright':ciVal>=3.0?'ci-bar ci-light':'ci-bar ci-cold';";
  J += "  var ciValCol=ciVal>=4.0?'#69f0ae':ciVal>=3.0?'#2e7d32':'#111827';";
  J += "  var ciCheck=ciVal>=4.0?'\u2713':ciVal>=3.0?'\u25d1':'\u2717';";
  J += "  var ciH='<div class=\"'+ciCls+'\">'";
  J += "    +'<span>Recent FH intensity \u00b7 both teams\u2019 last 5</span>'";
  J += "    +'<span style=\"font-size:18px;font-weight:700;color:'+ciValCol+'\">'+ciVal.toFixed(2)+' '+ciCheck+'</span>'";
  J += "  +'</div>';";
  J += "  var mw=m.missingStats?'<span style=\"background:#fef3c7;color:#92400e;font-size:10px;padding:2px 6px;border-radius:4px;font-weight:600;margin-left:6px\">\u26a0 no stats</span>':'';";
  J += "  return '<div class=\"card\">'";
  J += "    +'<div class=\"card-accent\" style=\"background:'+accent+'\"></div>'";
  J += "    +'<div class=\"card-inner\">'";
  J += "      +'<div style=\"display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin-bottom:12px\">'";
  J += "        +'<div style=\"min-width:0\">'";
  J += "          +'<div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:5px\">'+esc(m.league)+' \u00b7 '+esc(dt)+mw+'</div>'";
  J += "          +sb";
  J += "        +'</div>'";
  J += "        +'<div style=\"display:flex;align-items:center;gap:6px\">'+betPill(m)+'<div class=\"rank-pill '+rc+'\"><div class=\"rn\">'+m.rank+'/2</div><div class=\"rl\">'+esc(m.label)+'</div></div></div>'";
  J += "      +'</div>'";
  J += "    +ps+rb";
  J += "    +'<div class=\"teams\">'";
  J += "    +teamBox('Home',m.home,m.homeId,m.hLast5,m.snap?m.snap.home:null)";
  J += "    +teamBox('Away',m.away,m.awayId,m.aLast5,m.snap?m.snap.away:null)";
  J += "    +'</div>'";
  J += "    +sigsH+ciH";
  J += "    +(m.hLast5&&m.hLast5.length||m.aLast5&&m.aLast5.length?";
  J += "      '<div class=\"toggle-btn\">\u25bc Show last 5 games & H2H</div>'";
  J += "      +'<div class=\"details\">'";
  J += "        +'<div class=\"form-wrap\">'";
  J += "          +'<div id=\"form-'+m.id+'-'+m.homeId+'\"><div style=\"font-size:11px;color:#9ca3af\">\u21bb Loading '+esc(m.home)+'...</div></div>'";
  J += "          +'<div id=\"form-'+m.id+'-'+m.awayId+'\" style=\"margin-top:6px\"><div style=\"font-size:11px;color:#9ca3af\">\u21bb Loading '+esc(m.away)+'...</div></div>'";
  J += "        +'</div>'";
  J += "        +'<div id=\"h2h-'+m.id+'\" data-h=\"'+m.homeId+'\" data-a=\"'+m.awayId+'\" data-hn=\"'+esc(m.home||'')+'\" style=\"margin-top:10px;padding-top:10px;border-top:1px solid #f3f4f6\"></div>'";
  J += "      +'</div>'";
  J += "    :'')";
  J += "  +'</div></div>';";
  J += "}";

  // ─── renderBestBets ─────────────────────────────────────────────────────────
  J += "function parlayProb(legs,probKey){";
  J += "  var p=1;legs.forEach(function(l){p*=l[probKey]/100;});";
  J += "  return +(p*100).toFixed(1);";
  J += "}";

  // Render a compact match row for leaderboards
  J += "function renderBBRow(m,probKey,goalLabel,idx){";
  J += "  var rc=rankCls(m.rank);";
  J += "  var dt=m.dt?new Date(m.dt).toLocaleDateString('en-GB',{weekday:'short',day:'2-digit',month:'short'}):'';";
  J += "  var sigs='';if(m.signals){['A','E'].forEach(function(k){if(m.signals[k]&&m.signals[k].met)sigs+=k+' ';});}";
  J += "  var h='<div style=\"display:flex;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid #f3f4f6\">';";
  J += "  h+='<div style=\"font-size:18px;font-weight:800;color:#d1d5db;width:24px;text-align:center\">'+(idx+1)+'</div>';";
  J += "  h+='<div style=\"flex:1;min-width:0\">';";
  J += "  h+='<div style=\"font-weight:600;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis\">'+esc(m.home)+' vs '+esc(m.away)+'</div>';";
  J += "  h+='<div style=\"font-size:10px;color:#9ca3af\">'+esc(m.league)+' \u00b7 '+esc(dt)+'</div>';";
  J += "  h+='</div>';";
  J += "  h+='<div style=\"display:flex;gap:4px;align-items:center;flex-shrink:0\">';";
  J += "  if(sigs)h+='<span style=\"background:#fef9c3;color:#92400e;padding:1px 6px;border-radius:10px;font-size:9px;font-weight:600\">'+sigs.trim()+'</span>';";
  J += "  h+='<span style=\"background:#eff6ff;color:#1d4ed8;padding:1px 6px;border-radius:10px;font-size:9px\">CI '+m.ci+'</span>';";
  J += "  h+='<span class=\"rn '+rc+'\" style=\"font-size:12px;padding:2px 6px\">'+m.rank+'/2</span>';";
  J += "  h+='<span style=\"background:#1b5e20;color:#fff;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700\">'+m[probKey]+'%</span>';";
  J += "  h+='</div></div>';";
  J += "  return h;";
  J += "}";

  // Render a parlay card
  J += "function renderParlayCard(title,legs,probKey,goalLabel){";
  J += "  var combo=parlayProb(legs,probKey);";
  J += "  var h='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:16px;margin-bottom:16px\">';";
  J += "  h+='<div style=\"display:flex;justify-content:space-between;align-items:center;margin-bottom:12px\">';";
  J += "  h+='<div style=\"font-weight:700;font-size:15px;color:#111827\">'+esc(title)+'</div>';";
  J += "  h+='<div style=\"background:#1b5e20;color:#fff;padding:4px 12px;border-radius:20px;font-size:12px;font-weight:700\">'+combo+'% combo</div>';";
  J += "  h+='</div>';";
  J += "  legs.forEach(function(m,i){h+=renderBBRow(m,probKey,goalLabel,i);});";
  J += "  h+='</div>';";
  J += "  return h;";
  J += "}";

  // Render a section card (top 7, value picks, best per day)
  J += "function renderBBSection(title,color,borderColor,matches,probKey,goalLabel){";
  J += "  var h='<div style=\"margin-bottom:24px\">';";
  J += "  h+='<div style=\"font-size:16px;font-weight:700;color:'+color+';margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid '+borderColor+'\">'+title+'</div>';";
  J += "  h+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden\">';";
  J += "  matches.forEach(function(m,i){h+=renderBBRow(m,probKey,goalLabel,i);});";
  J += "  h+='</div></div>';";
  J += "  return h;";
  J += "}";

  J += "function renderBestBets(){";
  J += "  var main=document.getElementById('mainView');";
  J += "  var now=Date.now();";
  J += "  var bbDates=DATES.slice(0,4);";
  J += "  var upcoming=ALL.filter(function(p){return p.status!=='complete'&&!p.missingStats&&p.dt>now&&bbDates.indexOf(p.matchDate)>=0;}).slice();";
  J += "  if(!upcoming.length){main.innerHTML='<p style=\"color:#6b7280;text-align:center;padding:40px\">No qualifying matches found.</p>';return;}";
  J += "  var h='';";

  // Top 7 Over 1.5
  J += "  var s15=upcoming.slice().sort(function(a,b){return b.prob15-a.prob15||b.ci-a.ci;});";
  J += "  h+=renderBBSection('\u26bd Top 7 \u2014 FH Over 1.5 Goals','#1d4ed8','#bfdbfe',s15.slice(0,7),'prob15','> 1.5');";

  // Top 7 Over 2.5
  J += "  var s25=upcoming.slice().sort(function(a,b){return b.prob25-a.prob25||b.ci-a.ci;});";
  J += "  h+=renderBBSection('\ud83d\udd25 Top 7 \u2014 FH Over 2.5 Goals','#15803d','#a5d6a7',s25.slice(0,7),'prob25','> 2.5');";

  // Top 5 Value Picks — high CI but low rank (close to more signals firing)
  J += "  var value=upcoming.filter(function(p){return p.rank===1&&p.ci>=3.0;}).sort(function(a,b){return b.ci-a.ci;});";
  J += "  if(value.length){";
  J += "    h+='<div style=\"margin-bottom:24px\">';";
  J += "    h+='<div style=\"font-size:16px;font-weight:700;color:#92400e;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #fde68a\">\ud83d\udca1 Top 5 Value Picks \u2014 High CI, Signals Developing</div>';";
  J += "    h+='<div style=\"background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:8px 12px;margin-bottom:10px;font-size:11px;color:#92400e\">These matches have high recent FH intensity (\u2265 3.0) but only 1\u20132 signals fired. Both teams\u2019 recent first halves suggest goal potential the full signal set hasn\\'t captured yet.</div>';";
  J += "    h+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden\">';";
  J += "    value.slice(0,5).forEach(function(m,i){h+=renderBBRow(m,'prob15','value',i);});";
  J += "    h+='</div></div>';";
  J += "  }";

  // Best match per day
  J += "  h+='<div style=\"margin-bottom:24px\">';";
  J += "  h+='<div style=\"font-size:16px;font-weight:700;color:#7c3aed;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #c4b5fd\">\ud83c\udfc6 Best Match Per Day</div>';";
  J += "  DATES.slice(0,4).forEach(function(d,di){";
  J += "    var dayMatches=upcoming.filter(function(p){return p.matchDate===d;}).sort(function(a,b){return b.rank-a.rank||b.ci-a.ci;});";
  J += "    if(!dayMatches.length)return;";
  J += "    var best=dayMatches[0];var rc=rankCls(best.rank);";
  J += "    var dt=best.dt?new Date(best.dt).toLocaleDateString('en-GB',{weekday:'short',day:'2-digit',month:'short'}):'';";
  J += "    var sigs='';if(best.signals){['A','E'].forEach(function(k){if(best.signals[k]&&best.signals[k].met)sigs+=k+' ';});}";
  J += "    h+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:12px 14px;margin-bottom:10px\">';";
  J += "    h+='<div style=\"font-size:11px;color:#7c3aed;font-weight:700;text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px\">'+(DAY_LABELS[di]||d)+' \u2014 '+esc(dt)+'</div>';";
  J += "    h+='<div style=\"display:flex;justify-content:space-between;align-items:center;margin-bottom:4px\">';";
  J += "    h+='<div style=\"font-weight:700;font-size:14px\">'+esc(best.home)+' vs '+esc(best.away)+'</div>';";
  J += "    h+='<span class=\"rn '+rc+'\" style=\"font-size:14px;padding:2px 8px\">'+best.rank+'/2</span>';";
  J += "    h+='</div>';";
  J += "    h+='<div style=\"font-size:11px;color:#6b7280;margin-bottom:6px\">'+esc(best.league)+'</div>';";
  J += "    h+='<div style=\"display:flex;gap:6px;flex-wrap:wrap;font-size:11px\">';";
  J += "    h+='<span style=\"background:#eff6ff;color:#1d4ed8;padding:2px 8px;border-radius:12px\">CI: '+best.ci+'</span>';";
  J += "    h+='<span style=\"background:#f0fdf4;color:#15803d;padding:2px 8px;border-radius:12px\">FH>1.5: '+best.prob15+'%</span>';";
  J += "    h+='<span style=\"background:#f0fdf4;color:#15803d;padding:2px 8px;border-radius:12px\">FH>2.5: '+best.prob25+'%</span>';";
  J += "    if(sigs)h+='<span style=\"background:#fef9c3;color:#92400e;padding:2px 8px;border-radius:12px\">Signals: '+sigs.trim()+'</span>';";
  J += "    h+='</div></div>';";
  J += "  });";
  J += "  h+='</div>';";

  // Parlays — Over 1.5
  J += "  var p15=upcoming.filter(function(p){return p.rank>=1;}).sort(function(a,b){return b.prob15-a.prob15;});";
  J += "  h+='<div style=\"margin-bottom:24px\"><div style=\"font-size:16px;font-weight:700;color:#1d4ed8;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #bfdbfe\">\u26bd Parlays \u2014 FH Over 1.5 Goals</div>';";
  J += "  if(p15.length>=2)h+=renderParlayCard('2-Leg Parlay',p15.slice(0,2),'prob15','> 1.5');";
  J += "  if(p15.length>=3)h+=renderParlayCard('3-Leg Parlay',p15.slice(0,3),'prob15','> 1.5');";
  J += "  if(p15.length>=4)h+=renderParlayCard('4-Leg Parlay',p15.slice(0,4),'prob15','> 1.5');";
  J += "  h+='</div>';";

  // Parlays — Over 2.5
  J += "  var p25=upcoming.filter(function(p){return p.rank>=1;}).sort(function(a,b){return b.prob25-a.prob25;});";
  J += "  h+='<div><div style=\"font-size:16px;font-weight:700;color:#15803d;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #a5d6a7\">\ud83d\udd25 Parlays \u2014 FH Over 2.5 Goals</div>';";
  J += "  if(p25.length>=2)h+=renderParlayCard('2-Leg Parlay',p25.slice(0,2),'prob25','> 2.5');";
  J += "  if(p25.length>=3)h+=renderParlayCard('3-Leg Parlay',p25.slice(0,3),'prob25','> 2.5');";
  J += "  if(p25.length>=4)h+=renderParlayCard('4-Leg Parlay',p25.slice(0,4),'prob25','> 2.5');";
  J += "  h+='</div>';";

  J += "  main.innerHTML=h;";
  J += "}";

  J += "if(DATES.length)document.getElementById('hdrTitle').textContent=fmtDate(new Date(DATES[0]+'T12:00:00'));";
  J += "renderTabs();renderLeagueList();";
  J += "(function loadPreds(){";
  J += "  var main=document.getElementById('mainView');";
  J += "  if(ALL.length===0&&main)main.innerHTML='<p style=\"color:#6b7280;text-align:center;padding:40px;font-size:13px\">\u23f3 Loading predictions\u2026</p>';";
  J += "  fetch('/preds?tz='+TZ+'&_='+Date.now()).then(function(r){return r.json();}).then(function(d){";
  J += "    if(d.ok&&d.preds&&d.preds.length){ALL=d.preds;renderTabs();renderLeagueList();}";
  J += "    else setTimeout(loadPreds,3000);";
  J += "  }).catch(function(){setTimeout(loadPreds,4000);});";
  J += "})();";

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>First Half Predictor</title>
<script>(function(){var p=new URLSearchParams(window.location.search);if(!p.has('tz')){p.set('tz',-new Date().getTimezoneOffset());window.location.search=p.toString();}})();<\/script>
<style>${CSS}</style>
</head>
<body>
<div class="hdr">
  <div class="hdr-inner">
    <div class="hdr-top">
      <div>
        <div class="hdr-sub">&#9917; First Half Predictor</div>
        <div class="hdr-title" id="hdrTitle">Loading...</div>
      </div>
      <button onclick="location.reload()" style="background:#ff6b35;color:#fff;border:none;padding:7px 14px;border-radius:20px;font-size:12px;font-weight:600;cursor:pointer;flex-shrink:0;">&#8635; Refresh</button>
    </div>
    <div class="tabs" id="dayTabs"></div>
  </div>
</div>
<div class="body">
  ${rateLimited ? '<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:12px;color:#b91c1c;line-height:1.6"><strong>&#9888; API rate limit reached</strong> \u2014 showing cached data. Resets at ' + new Date(RATE_LIMITED_UNTIL).toLocaleTimeString('en-GB') + '. <a href="/cache-status" style="color:#b91c1c">View cache status</a></div>' : ''}
  <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:12px;color:#92400e;line-height:1.6">
    <strong>How it works:</strong> 2 pre-game signals (A &amp; E) &mdash; no look-ahead bias. Signal A: recent last-5 FH intensity. Signal E: home team historical FH profile + current season scoring. Each worth 1 point; rank = signals fired (max 2).
    A: both teams' L5 FH total &ge; 4.0 &middot; B: home L5 scored &gt;1.0 &amp; away L5 conceded &gt;1.0 &middot; C: both L5 FH scored &gt;0.8 &middot; D: both L5 FH total &gt;1.5.
    Calibrated on 24,677 look-ahead-free pre-game matches &middot; base rate 11.2% FH&gt;2.5 &middot; rank 0&ndash;4: 10.7 / 13.9 / 16.8 / 17.5 / 30.2% &mdash; see scripts/recalibrate_pregame.py.
  </div>
  <div id="mainView"></div>
</div>
<script>${J}<\/script>
</body>
</html>`;
}

app.listen(PORT, () => {
  console.log("Server on port " + PORT);
  console.log("Memory: " + Math.round(process.memoryUsage().heapUsed/1024/1024) + "MB");
  fetchLeagueList().then(() => {
    if (Object.keys(LEAGUE_NAMES).length > 0) {
      setTimeout(rebuildServerMatchCache, 5 * 60 * 1000);
      console.log("Match cache warming in 5 min...");
    }
  }).catch(e => console.error("League list failed:", e.message));

  if (supabase) {
    // Restore frozen pre-match snapshots so completed matches survive restarts.
    loadFrozenSnapshots().catch(e => console.error("loadFrozenSnapshots:", e.message));
    // Initial load. If empty, trigger one recalibration so we don't wait 24h.
    setTimeout(async () => {
      try {
        await loadLeagueProbCache();
        await loadLeagueComboCache();
        if (Object.keys(LEAGUE_PROB_CACHE).length === 0) {
          console.log("league_prob_tables empty — running initial rank recalibration");
          await recalibrateLeagueProbs();
        }
        if (Object.keys(LEAGUE_COMBO_CACHE).length === 0) {
          console.log("league_combo_probs empty — running initial combo recalibration");
          await recalibrateLeagueComboProbs();
        }
      } catch (e) { console.error("initial league prob load:", e.message); }
    }, 30 * 1000);
    // Periodic cache refresh
    setInterval(() => { loadLeagueProbCache().catch(e => console.error("loadLeagueProbCache:", e.message)); }, LEAGUE_PROB_CACHE_TTL);
    setInterval(() => { loadLeagueComboCache().catch(e => console.error("loadLeagueComboCache:", e.message)); }, LEAGUE_PROB_CACHE_TTL);
    // Daily recalibration (both rank + combo)
    setInterval(() => { recalibrateLeagueProbs().catch(e => console.error("recalibrateLeagueProbs:", e.message)); }, LEAGUE_PROB_RECAL_TTL);
    setInterval(() => { recalibrateLeagueComboProbs().catch(e => console.error("recalibrateLeagueComboProbs:", e.message)); }, LEAGUE_PROB_RECAL_TTL);
  }
});

process.on("uncaughtException", e => console.error("Uncaught:", e.message));
