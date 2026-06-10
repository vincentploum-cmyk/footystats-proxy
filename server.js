require("dotenv").config();

const express = require("express");
const cors    = require("cors");
const fetch   = require("node-fetch");
// Pre-game freeze contract (the integrity invariant) — single source of truth.
const { computeOverCandidates, selectPregamePrediction } = require("./lib/freeze");

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

// Self-capture: drive computePreds on a timer so freezing pre-game snapshots and
// recording completed results no longer depend on inbound page traffic. Without
// it, a match whose pre-game window OR post-completion day saw no visitors never
// gets a frozen snap / recorded result — that is the pending-row leak. The
// keep-alive cron keeps the instance warm so this interval actually fires.
const SELF_CAPTURE_TTL = 20 * 60 * 1000;  // 20 min
let SELF_CAPTURE_RUNNING = false;
let SELF_CAPTURE_LAST = { at: 0, preds: 0, error: null };
async function selfCapture() {
  if (!supabase || SELF_CAPTURE_RUNNING) return;
  SELF_CAPTURE_RUNNING = true;
  try {
    const { preds } = await computePreds(0);
    await persistEarlySnapshots(preds);
    await persistCompletedPreds(preds);
    SELF_CAPTURE_LAST = { at: Date.now(), preds: preds.length, error: null };
    console.log("selfCapture: scanned " + preds.length + " preds");
  } catch (e) {
    SELF_CAPTURE_LAST = { at: Date.now(), preds: 0, error: e.message };
    console.error("selfCapture: " + e.message);
  } finally {
    SELF_CAPTURE_RUNNING = false;
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
      // Recompute candidate flags from the frozen PRE-GAME snap (single source of
      // truth) so they survive a restart with no look-ahead.
      const oc = computeOverCandidates(r.snap, r.prob15, r.prob25);
      CI_SNAPSHOT_CACHE[r.match_id] = {
        ci: Number(r.ci) || 0, defCi: Number(r.def_ci) || 0,
        rank, label: RANK_LABELS[rank] || "Low",
        prob25: Number(r.prob25) || 0, prob15: Number(r.prob15) || 0,
        probSource: "frozen", probSampleN: 0, probCombo: null,
        eligible: rank >= 2, signals: r.signals || {}, snap: r.snap || null,
        ov15Candidate: oc.ov15Candidate, ov25Candidate: oc.ov25Candidate,
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
// Cache shape: { "<compId>:<combo2chars>": { n, prob25, prob15 } }
// combo is bit(A) + bit(B) — the two live signals (see computeSignals). This now
// matches the Supabase compute_league_combo_buckets() RPC's 2-char key, so per-league
// overrides line up again (they fell back to global while the model carried Signal C).
const LEAGUE_COMBO_MIN_N    = 20;
let   LEAGUE_COMBO_CACHE    = {};
let   LEAGUE_COMBO_LAST_LOAD  = { at: 0, rows: 0, error: null };
let   LEAGUE_COMBO_LAST_RECAL = { at: 0, buckets: 0, written: 0, error: null };

function comboFromSignals(sigs) {
  if (!sigs) return "00";
  const bit = (k) => (sigs[k] && sigs[k].met) ? "1" : "0";
  return bit("A") + bit("B");
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
    // eligible = rank >= 2 (documented star-badge rule). eligible25/eligible15 are
    // informational prob-tier flags only — nothing gates on them (betPill is signal-based).
    result.eligible25 = result.prob25 >= 40.0;
    result.eligible15 = result.prob15 >= 50.0;
    result.eligible = result.rank >= 2;
    return;
  }
  // 2. League-rank is too coarse to override the calibrated combo-keyed tables —
  //    keep the global combo probabilities when the league combo sample is too thin.
  //    Surface league-rank as debug metadata only (for /calibration etc.).
  const rankEntry = LEAGUE_PROB_CACHE[compId + ":" + result.rank];
  result.probSource  = "global";
  result.probSampleN = 0;
  result.probCombo   = combo;
  if (rankEntry) {
    result.leagueRankDebug = { n: rankEntry.n, prob25: rankEntry.prob25, prob15: rankEntry.prob15 };
  }
  // eligible = rank >= 2 (documented star-badge rule). eligible25/eligible15 are
  // informational prob-tier flags only — nothing gates on them (betPill is signal-based).
  result.eligible25 = result.prob25 >= 40.0;
  result.eligible15 = result.prob15 >= 50.0;
  result.eligible = result.rank >= 2;
}

let LEAGUE_NAMES = {};
// Season IDs of current international competitions (country === "International"):
// friendlies, Nations Leagues, WC/continental qualifiers, etc. Used to assemble a
// cross-competition last-5 for national teams, whose games are split across these.
let INTERNATIONAL_SIDS = new Set();
let LEAGUE_LIST_LOADING = false;

async function fetchLeagueList() {
  if (LEAGUE_LIST_LOADING) return;
  LEAGUE_LIST_LOADING = true;
  try {
    const data = await safeFetch(BASE + "/league-list?key=" + KEY);
    if (!data) { console.warn("fetchLeagueList skipped"); LEAGUE_LIST_LOADING = false; return; }
    const list = data.data || [];
    const map = {};
    const intl = new Set();
    const CUR_YEAR = new Date().getFullYear();
    for (const league of list) {
      const leagueName = league.league_name || league.name || "";
      const country    = league.country || "";
      const name       = country ? country + " \u00b7 " + leagueName : leagueName;
      if (!name) continue;
      const isIntl = country.toLowerCase() === "international";
      const seasons = league.season || [];
      for (const s of seasons) {
        if (!s.id) continue;
        map[parseInt(s.id, 10)] = name;
        // Collect only current/previous-year international seasons \u2014 these hold the
        // recent matches needed for a last-5, and the year filter keeps the set
        // small (skips finished tournaments like past Euros/Copas).
        if (isIntl) {
          const yr = parseInt(String(s.year || "").slice(0, 4), 10) || 0;
          if (yr >= CUR_YEAR - 1) intl.add(parseInt(s.id, 10));
        }
      }
    }
    if (Object.keys(map).length) {
      LEAGUE_NAMES = map;
      INTERNATIONAL_SIDS = intl;
      console.log("Mapped " + Object.keys(map).length + " season IDs (" + intl.size + " current international)");
    }
    if (list.length === 0) setTimeout(fetchLeagueList, 2 * 60 * 1000);
  } catch(e) {
    console.error("Failed to load league list: " + e.message);
    // Keep the existing registry on a refresh error — don't wipe a good map.
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
  const url = (p) => BASE + "/league-teams?season_id=" + sid + "&include=stats&page=" + p + "&key=" + KEY;
  const page1 = await safeFetch(url(1));
  if (!page1) return TEAM_STATS_CACHE[sid]?.data || { data: [] };
  let teams = page1.data || [];
  // /league-teams paginates (~50 teams/page). A normal club league fits on one page,
  // but big rosters — International Friendlies has hundreds of national teams — span
  // many pages. Without this loop, teams past page 1 (e.g. Portugal/Chile) are missing
  // from teamMap, so their fixtures get flagged missingStats and hidden. Page through
  // all pages (capped) and concat. max_page=1 leaves every other league unchanged.
  const maxPage = Math.min(page1.pager && page1.pager.max_page ? page1.pager.max_page : 1, 15);
  for (let p = 2; p <= maxPage; p++) {
    const pg = await safeFetch(url(p));
    if (pg && pg.data && pg.data.length) teams = teams.concat(pg.data);
    else break;
  }
  const data = { ...page1, data: teams };
  TEAM_STATS_CACHE[sid] = { data, ts: now };
  return data;
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

// Multi-signal probabilities — combo-keyed: bit(A)+bit(B)+bit(C).
// Combo-keyed probabilities — key = bit(A)+bit(B) (2-char). Recalibrated on the
// clean live cohort (n=1075, women excluded) via /signalc-validate.
// Signal C (team mismatch) was REMOVED: on the full cohort its standalone lift fell
// to ~1.0 (no edge) and B+C was anti-additive (below base rate), and the directional
// split that would have rescued it added complexity for a marginal rank-0-only gain.
// A+B carry the real lift (A 1.72, B 1.38) and ranking is monotonic without C.
// n by combo: 00=606, 01=330, 10=42, 11=97 (women excluded).
const PROB15_BY_COMBO = { "00": 29.9, "01": 39.1, "10": 38.1, "11": 43.3 };
const PROB25_BY_COMBO = { "00":  9.1, "01": 10.9, "10": 14.3, "11": 17.5 };
const RANK_LABELS = { 0: "Low", 1: "Signal", 2: "Fire", 3: "Fire" };

function numOrNull(v) {
  return (v != null && !isNaN(Number(v))) ? Number(v) : null;
}

function pushUnique(arr, label) {
  if (label && !arr.includes(label)) arr.push(label);
}

// Pattern helper: lightweight secondary context only. The calibrated combo probabilities
// remain the source of truth; these tags just improve tiebreaking and explainability.
function derivePatternContext(pred) {
  const snap = (pred && pred.snap) || {};
  const pm = snap.prematch || {};
  const home = snap.home || {};
  const away = snap.away || {};
  const l5 = snap.l5 || {};
  const hL5 = l5.home || {};
  const aL5 = l5.away || {};
  const sigA = !!(pred && pred.signals && pred.signals.A && pred.signals.A.met);

  const pmO05 = numOrNull(pm.o05HT);
  const pmO15 = numOrNull(pm.o15HT);
  const pmBtts = numOrNull(pm.btts_fhg);
  const pmXgHome = numOrNull(pm.xgHome);
  const pmXgAway = numOrNull(pm.xgAway);
  const hScored = numOrNull(home.scored_fh);
  const hT1 = numOrNull(home.t1_pct);
  const aConced = numOrNull(away.conced_fh);
  const hCn010 = numOrNull(home.cn010_avg);
  const hL5F = numOrNull(hL5.f);
  const hL5T = numOrNull(hL5.t);
  const aL5F = numOrNull(aL5.f);

  let score15 = 0;
  let score25 = 0;
  const reasons15 = [];
  const reasons25 = [];
  const cautions = [];

  if (pmO05 != null && pmO05 >= 86) { score15 += 2; score25 += 1; pushUnique(reasons15, "elite prematch FH goal pace"); pushUnique(reasons25, "elite prematch FH pace"); }
  if (pmBtts != null && pmBtts >= 34) { score15 += 2; score25 += 1; pushUnique(reasons15, "both teams projected live early"); }
  if (pmO15 != null && pmO15 >= 50) { score15 += 2; score25 += 1; pushUnique(reasons15, "strong prematch over 1.5 signal"); }
  if (hL5T != null && hL5T >= 2.0) { score15 += 2; score25 += 2; pushUnique(reasons15, "home last-5 FH totals are hot"); pushUnique(reasons25, "home last-5 FH chaos is high"); }
  if (hScored != null && hScored >= 1.14) { score15 += 1; score25 += 1; pushUnique(reasons15, "home scores early often"); }
  if (numOrNull(pred && pred.ci) != null && Number(pred.ci) >= 3.55) { score15 += 1; score25 += 2; pushUnique(reasons15, "combined FH intensity is high"); pushUnique(reasons25, "combined FH intensity is high"); }
  if (aL5F != null && aL5F >= 1.2) { score15 += 1; score25 += 1; pushUnique(reasons15, "away side brings recent FH scoring"); }
  if (aConced != null && aConced >= 0.88) { score15 += 1; score25 += 1; pushUnique(reasons15, "away side leaks early goals"); }

  if (aConced != null && aConced >= 0.88 && pmXgHome != null && pmXgHome >= 1.59) {
    score15 += 3;
    pushUnique(reasons15, "strong home pressure vs soft away defence");
  }
  if (hL5F != null && hL5F >= 1.0 && aL5F != null && aL5F >= 1.0) {
    score15 += 2;
    score25 += 3;
    pushUnique(reasons15, "both teams scored 1.0+ FH in L5");
    pushUnique(reasons25, "both teams scored 1.0+ FH in L5");
  }
  if (pmO15 != null && pmO15 >= 45 && pmBtts != null && pmBtts >= 28.4) {
    score15 += 2;
    score25 += 1;
    pushUnique(reasons15, "prematch FH goal signals agree");
  }

  if (Number(pred && pred.prob25) >= 19.6) { score25 += 2; pushUnique(reasons25, "top calibrated FH over 2.5 bucket"); }
  if (sigA) { score25 += 1; pushUnique(reasons25, "mutual instability is live"); }
  if (hL5F != null && hL5F >= 1.0 && pmBtts != null && pmBtts >= 28.4) {
    score25 += 2;
    pushUnique(reasons25, "home pressure plus live-both-teams setup");
  }
  if (hT1 != null && hT1 >= 21 && hL5F != null && hL5F >= 1.0) {
    score25 += 2;
    pushUnique(reasons25, "home side converts strong FH volume");
  }
  if (hL5T != null && hL5T >= 1.6 && pmO15 != null && pmO15 >= 45) {
    score25 += 2;
    pushUnique(reasons25, "recent chaos and prematch model agree");
  }
  if (aConced != null && aConced >= 0.88 && pmXgAway != null && pmXgAway >= 1.49) {
    score25 += 2;
    pushUnique(reasons25, "away xG stays live in a soft early defence game");
  }
  if (hCn010 != null && hCn010 >= 0.167 && pmO15 != null && pmO15 >= 45) {
    score25 += 1;
    pushUnique(reasons25, "early-goal timing risk plus strong prematch pace");
  }

  if (pmO05 != null && pmO05 <= 54) { score15 -= 2; score25 -= 4; pushUnique(cautions, "cold prematch FH pace"); }
  if (aConced != null && aConced <= 0.33) { score15 -= 1; score25 -= 2; pushUnique(cautions, "away defence rarely concedes early"); }
  if (hScored != null && hScored <= 0.33) { score15 -= 2; score25 -= 2; pushUnique(cautions, "home side rarely scores early"); }
  if (pmBtts != null && pmBtts <= 12) { score15 -= 1; score25 -= 2; pushUnique(cautions, "low FH BTTS pressure"); }
  if (Number(pred && pred.prob25) <= 5.4) { score25 -= 2; pushUnique(cautions, "weak calibrated 2.5 bucket"); }
  if (pmO15 != null && pmO15 <= 27) { score15 -= 1; score25 -= 1; pushUnique(cautions, "cold prematch over 1.5 signal"); }

  return {
    score15,
    score25,
    tag15: score15 >= 5 ? "Strong 1.5 setup" : (score15 >= 3 ? "Live 1.5 setup" : ""),
    tag25: score25 >= 6 ? "Strong 2.5 setup" : (score25 >= 4 ? "Live 2.5 setup" : ""),
    cautionTag: cautions.length && (score15 <= 0 || score25 <= 0) ? "Caution trap" : "",
    reasons15: reasons15.slice(0, 3),
    reasons25: reasons25.slice(0, 3),
    cautions: cautions.slice(0, 3),
  };
}

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

// Multi-signal engine — production thresholds from grid-search calibration.
//   Signal A: Mutual Instability (home L5 total >= 1.6 AND away L5 total >= 1.4)
//   Signal B: Away Team Scoring (away L5 FH >= 0.8)
// A+B is the FH>2.5 engine (20.8% vs 9.4% baseline = 2.2x lift, n=72).
// B alone drives FH>1.5 (44.9% vs 32.9% baseline). A alone is at/below baseline on both.
function computeSignals(snap, hLast5, aLast5) {
  const f2 = (v) => v.toFixed(2);

  // Extract L5 metrics from snap
  const homeL5Total = snap && snap.l5 && snap.l5.home ? (snap.l5.home.t || 0) : 0;
  const awayL5Scored = snap && snap.l5 && snap.l5.away ? (snap.l5.away.f || 0) : 0;
  const awayL5Total = snap && snap.l5 && snap.l5.away ? (snap.l5.away.t || 0) : 0;

  // Check if we have L5 data
  const hasL5 = !!(snap && snap.l5 && snap.l5.home && snap.l5.away);

  // Signal A: Mutual Instability (looser thresholds for sample stability)
  const sigA = hasL5 && homeL5Total >= 1.6 && awayL5Total >= 1.4;

  // Signal B: Away Team Scoring (away team moderately active)
  const sigB = hasL5 && awayL5Scored >= 0.8;

  // Combo-keyed probabilities (bit(A)+bit(B)). Signal C (team mismatch) was removed —
  // it had no live edge and B+C was anti-additive; A+B alone rank monotonically.
  const combo = (sigA ? "1" : "0") + (sigB ? "1" : "0");
  const prob15 = PROB15_BY_COMBO[combo];
  const prob25 = PROB25_BY_COMBO[combo];

  // Rank is derived from the calibrated probability — NOT from counting signals — so
  // rank and probability always move together (a higher rank is always a higher-
  // probability match). Tiered on FH-over-1.5, the primary bet: 3 & 2 = Fire, 1 =
  // Signal, 0 = Low.
  const rank = prob15 >= 45 ? 3 : prob15 >= 40 ? 2 : prob15 >= 30 ? 1 : 0;

  // ── Over-1.5 / Over-2.5 first-half CANDIDATE signals ──────────────────────
  // Detect FH over-goal candidates from pre-game inputs only (see lib/freeze.js,
  // the single source of truth shared with the restore + calibration paths).
  const { envFh, l5Fh, ov15Candidate, ov25Candidate } = computeOverCandidates(snap, prob15, prob25);

  return {
    rank,
    label: RANK_LABELS[rank] || "Low",
    prob15,
    prob25,
    ci: homeL5Total + awayL5Total,  // combined intensity
    defCi: awayL5Total,              // away activity
    eligible: rank >= 2,
    ov15Candidate, ov25Candidate, envFh, l5Fh,
    signals: {
      A: { met: sigA, label: "Mutual Instability", value: f2(homeL5Total) + " / " + f2(awayL5Total), threshold: "home L5 total >= 1.6 & away L5 total >= 1.4" },
      B: { met: sigB, label: "Away Team Scoring", value: f2(awayL5Scored), threshold: "away L5 FH >= 0.8" },
      O15: { met: ov15Candidate, label: "Over 1.5 FH candidate", value: "env " + f2(envFh) + " · L5 " + f2(l5Fh) + " · p15 " + prob15, threshold: "env-FH >= 2.60 & L5-FH >= 1.4 & prob15 >= 38" },
      O25: { met: ov25Candidate, label: "Over 2.5 FH candidate", value: "env " + f2(envFh) + " · L5 " + f2(l5Fh) + " · p25 " + prob25, threshold: "env-FH >= 2.85 & L5-FH >= 1.6 & prob25 >= 14.5" },
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
  // Role-specific season field with overall fallback (percentages / per-game avgs
  // FootyStats already computes — no division needed).
  const sv = (base) => { const r = s[base + sfx]; return safe(r != null ? r : s[base + "_overall"]); };
  // First-half goal-timing: per-game scored/conceded in the 0–40 min window
  // (role-specific buckets), normalised by matches played. This is FootyStats'
  // own goal-timing data — when teams actually score in the first half.
  const fhBuckets = (kind) =>
    safe(((s["goals_" + kind + "_min_0_to_10" + sfx]  || 0) +
          (s["goals_" + kind + "_min_11_to_20" + sfx] || 0) +
          (s["goals_" + kind + "_min_21_to_30" + sfx] || 0) +
          (s["goals_" + kind + "_min_31_to_40" + sfx] || 0)) / mpR);
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
    // Extended FootyStats season fields, frozen pre-game for forward mining.
    // These cannot be backfilled cleanly (season stats are cumulative → re-fetching
    // later leaks post-match games), so they only populate on live forward captures.
    xt: {
      o15ht:  sv("seasonOver15PercentageHT"),   // % of games over 1.5 HT goals
      o05ht:  sv("seasonOver05PercentageHT"),   // % over 0.5 HT goals
      bttsfhg: sv("btts_fhg_percentage"),        // % both teams scored in FH
      leadHT: sv("leadingAtHTPercentage"),       // % leading at HT
      xgf:    sv("xg_for_avg"),                  // pre-game season xG for (per game)
      xga:    sv("xg_against_avg"),              // season xG against
      datk:   sv("dangerous_attacks_avg"),       // dangerous attacks per game
      fhsc:   fhBuckets("scored"),               // FH goals scored / game (0–40')
      fhcn:   fhBuckets("conceded"),             // FH goals conceded / game (0–40')
    },
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
  // If thin, expand to ONE bounded older window (70 days). If still < 3, return [] so
  // last5Form yields null and neither signal fires — better than firing on stale form.
  if (unique.length < 3) {
    seen.clear();
    const olderCutoff = now - 2 * LAST5_WINDOW_SECS;
    unique = entries.filter(m => (m.date_unix || 0) >= olderCutoff && dedup(m));
    if (unique.length < 3) return [];
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
    selfCapture: SELF_CAPTURE_LAST.at
      ? { at: new Date(SELF_CAPTURE_LAST.at).toISOString(), preds: SELF_CAPTURE_LAST.preds, running: SELF_CAPTURE_RUNNING, error: SELF_CAPTURE_LAST.error }
      : { at: null, running: SELF_CAPTURE_RUNNING },
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
    const byRank = { 0: 0, 1: 0, 2: 0, 3: 0 };
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

// Recompute stored rank/signals/prob25/prob15 on existing rows using current
// computeSignals logic. Run after threshold/model changes to align historical
// rows with the live model. snap.l5 must already be present.
app.get("/admin/recompute-signals", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });

  const t0 = Date.now();
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("match_id, snap")
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }

    const updates = [];
    let skippedNoL5 = 0;
    for (const m of all) {
      if (!m.snap || !m.snap.l5 || !m.snap.l5.home || !m.snap.l5.away) {
        skippedNoL5++;
        continue;
      }
      const result = computeSignals(m.snap, [], []);
      updates.push({
        match_id: m.match_id,
        rank: result.rank,
        ci: result.ci,
        def_ci: result.defCi,
        prob25: result.prob25,
        prob15: result.prob15,
        signals: result.signals,
      });
    }

    let written = 0, errs = 0;
    const errors = [];
    const BATCH = 500;
    for (let i = 0; i < updates.length; i += BATCH) {
      const batch = updates.slice(i, i + BATCH);
      const { error } = await supabase.from("match_results").upsert(batch, { onConflict: "match_id" });
      if (error) {
        errs += batch.length;
        if (errors.length < 10) errors.push({ batchStart: i, error: error.message });
      } else {
        written += batch.length;
      }
    }

    res.json({
      ok: errs === 0,
      totalLive: all.length,
      skippedNoL5,
      processed: updates.length,
      written, errs,
      elapsedMs: Date.now() - t0,
      errors,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
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

// Backfill snap.prematch (FootyStats' own pre-match first-half predictors:
// o15HT/o05HT potentials, pre-match xG, btts_fhg) onto live-captured rows that
// predate the prematch-capture path. These match-level fields persist on completed
// matches in /league-matches, so they can be read retroactively and correlated with
// the FH results we already hold — enabling immediate calibration of o15HT_potential
// as a fallback FH signal (FootyStats does NOT expose last-5 HT team stats, so this
// is the only pre-game FH predictor available for thin-coverage leagues).
//
//   GET /admin/backfill-prematch?token=<TOKEN>&dryRun=1   -> report counts only
//   GET /admin/backfill-prematch?token=<TOKEN>&limit=300  -> fill up to 300 rows
//
// Only merges a `prematch` sub-object into snap via UPDATE — never touches
// home/away/l5/signals/rank, so the pre-game freeze is preserved. Idempotent:
// considers only rows where snap.prematch is missing.
app.get("/admin/backfill-prematch", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });

  const dryRun = req.query.dryRun === "1";
  const limit = Math.max(1, Math.min(1000, parseInt(req.query.limit || "300", 10)));
  const t0 = Date.now();

  // Read the same pre-match predictor fields the live capture path freezes.
  const extractPrematch = (m) => {
    const pm = {};
    if (m.o15HT_potential    != null) pm.o15HT    = m.o15HT_potential;
    if (m.o05HT_potential    != null) pm.o05HT    = m.o05HT_potential;
    if (m.team_a_xg_prematch != null) pm.xgHome   = m.team_a_xg_prematch;
    if (m.team_b_xg_prematch != null) pm.xgAway   = m.team_b_xg_prematch;
    if (m.btts_fhg_potential != null) pm.btts_fhg = m.btts_fhg_potential;
    return Object.keys(pm).length ? pm : null;
  };

  try {
    const allLive = [];
    const PAGE = 300;
    const cutoffSec = Math.floor(Date.now() / 1000) - 365 * 86400;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("match_id, competition_id, snap")
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
    const missing = allLive.filter(r => r.snap && !r.snap.prematch && r.competition_id);

    if (dryRun) {
      const byComp = {};
      for (const r of missing) byComp[r.competition_id] = (byComp[r.competition_id] || 0) + 1;
      return res.json({
        ok: true, dryRun: true,
        totalLive: allLive.length, missingPrematch: missing.length,
        distinctCompetitions: Object.keys(byComp).length,
        topComps: Object.entries(byComp).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([c, n]) => ({ competition_id: +c, n })),
      });
    }

    const work = missing.slice(0, limit);
    const byComp = {};
    for (const r of work) {
      if (!byComp[r.competition_id]) byComp[r.competition_id] = [];
      byComp[r.competition_id].push(r);
    }

    let processed = 0, written = 0, skippedNoData = 0, errs = 0;
    const errors = [];

    for (const [compId, rs] of Object.entries(byComp)) {
      if (Date.now() < RATE_LIMITED_UNTIL) {
        errors.push({ compId, error: "rate-limited; aborting batch" });
        break;
      }
      let matchById;
      try {
        const matchRes = await fetchLeagueMatches(compId);
        matchById = new Map();
        for (const m of (matchRes.data || [])) if (m && m.id) matchById.set(m.id, m);
      } catch (e) {
        errors.push({ compId, error: "fetch failed: " + e.message });
        continue;
      }
      for (const r of rs) {
        processed++;
        const m = matchById.get(r.match_id);
        const pm = m ? extractPrematch(m) : null;
        if (!pm) { skippedNoData++; continue; }
        const newSnap = Object.assign({}, r.snap, { prematch: pm });
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
      missingPrematch: missing.length,
      processed, written, skippedNoData, errs,
      remaining: missing.length - processed,
      elapsedMs: Date.now() - t0,
      errors,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Backfill match results (ht/ft goals, hit_15, hit_25) on clean live-captured
// rows whose outcome was never recorded — matches whose snap was frozen pre-game
// but which dropped out of the 5-day /preds window before any request filled in
// their result (hit_25 IS NULL, the pending-row leak). For each pending row,
// fetches its league season matches (cached) and reads the final HT/FT score by
// match id.
//
//   GET /admin/backfill-results?token=<TOKEN>&dryRun=1    -> report counts only
//   GET /admin/backfill-results?token=<TOKEN>&limit=300   -> resolve up to 300 rows
//
// Only UPDATEs result fields — never touches snap/signals/rank, so the pre-game
// freeze is preserved. Idempotent: considers only rows where hit_25 is null, and
// writes only matches that are genuinely played with valid first-half data.
app.get("/admin/backfill-results", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });

  const dryRun = req.query.dryRun === "1";
  const limit = Math.max(1, Math.min(1000, parseInt(req.query.limit || "300", 10)));
  const t0 = Date.now();

  try {
    const pending = [];
    const PAGE = 300;
    const cutoffSec = Math.floor(Date.now() / 1000) - 365 * 86400;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("match_id, competition_id, date_unix")
        .is("hit_25", null)
        .gte("date_unix", cutoffSec)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      pending.push(...data);
      if (data.length < PAGE) break;
    }
    // Only rows we can locate (have competition_id) and whose kickoff has passed
    // by > 2h (same played-threshold as isPlayedMatch) — future matches stay pending.
    const nowSecs = Math.floor(Date.now() / 1000);
    const resolvable = pending.filter(r => r.competition_id && r.date_unix && r.date_unix < nowSecs - 7200);

    if (dryRun) {
      const byComp = {};
      for (const r of resolvable) byComp[r.competition_id] = (byComp[r.competition_id] || 0) + 1;
      return res.json({
        ok: true, dryRun: true,
        pendingTotal: pending.length,
        resolvableCandidates: resolvable.length,
        notYetPlayedOrNoIds: pending.length - resolvable.length,
        distinctCompetitions: Object.keys(byComp).length,
        topComps: Object.entries(byComp).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([c, n]) => ({ competition_id: +c, n })),
      });
    }

    const work = resolvable.slice(0, limit);
    const byComp = {};
    for (const r of work) {
      if (!byComp[r.competition_id]) byComp[r.competition_id] = [];
      byComp[r.competition_id].push(r);
    }

    let processed = 0, written = 0, notPlayed = 0, notFound = 0, noFhData = 0, errs = 0;
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
        const prevSid = PREV_SEASON[compId];
        if (prevSid) {
          try { const prev = await fetchLeagueMatches(prevSid); if (prev && prev.data) matches = matches.concat(prev.data); } catch (_) { /* best-effort */ }
        }
      } catch (e) {
        errors.push({ compId, error: "fetch failed: " + e.message });
        continue;
      }
      const byId = {};
      for (const m of matches) byId[String(m.id)] = m;

      for (const r of rs) {
        processed++;
        const m = byId[String(r.match_id)];
        if (!m) { notFound++; continue; }
        if (!isPlayedMatch(m, nowSecs)) { notPlayed++; continue; }
        const fhH = parseInt(m.ht_goals_team_a, 10);
        const fhA = parseInt(m.ht_goals_team_b, 10);
        // API returns -1 for unavailable HT data — can't compute a hit, skip.
        if (!(fhH >= 0) || !(fhA >= 0)) { noFhData++; continue; }
        const ftH = parseInt(m.homeGoalCount || 0, 10);
        const ftA = parseInt(m.awayGoalCount || 0, 10);
        const fhTotal = fhH + fhA;
        const { error } = await supabase.from("match_results").update({
          ht_home: fhH, ht_away: fhA, ft_home: ftH, ft_away: ftA,
          fh_total: fhTotal,
          hit_15: fhTotal > 1,
          hit_25: fhTotal > 2,
        }).eq("match_id", r.match_id);
        if (error) {
          errs++;
          if (errors.length < 10) errors.push({ match_id: r.match_id, error: error.message });
        } else {
          written++;
          SUPABASE_PERSISTED_IDS.add(r.match_id);
        }
      }
    }

    res.json({
      ok: errs === 0,
      pendingTotal: pending.length,
      resolvableCandidates: resolvable.length,
      processed, written, notPlayed, notFound, noFhData, errs,
      remaining: resolvable.length - processed,
      elapsedMs: Date.now() - t0,
      errors,
      note: "Resolved pending rows from league-matches final scores. Only result fields written; snap/signals/rank preserved. Re-run until remaining hits 0.",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// NOTE: the former /admin/backfill-signal-d route was removed. It recomputed a
// phantom Signal C and a 3-signal rank (0–3) against tables that never existed
// (PROB*_BY_COMBO), contradicting the live 2-signal A+B engine. To realign
// stored rows with the current model, use /admin/recompute-signals, which runs
// the real computeSignals() over every live-captured snap.

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

// ─── FLOOR-FILTER TEST — does a combined pre-game "floor" profile have lift? ──
// Tests a candidate filter against the FULL clean cohort and — critically — against
// the control group. Reports the hit rate among matches that PASS vs the base rate
// (= lift), plus each sub-condition's own lift. The whole point: a floor that 90% of
// WINNERS clear is only an edge if the LOSERS clear it much less often. Thresholds
// are query params (defaults = the proposed "perfect storm" profile).
app.get("/floor-test", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const q = req.query;
    const num = (k, d) => (q[k] != null && q[k] !== "") ? Number(q[k]) : d;
    const T = {
      l5_h_f:    num("l5_h_f", 0.40),
      h_scored:  num("h_scored", 0.50),
      a_scored:  num("a_scored", 0.22),
      a_conced:  num("a_conced", 0.50),
      ci_min:    num("ci_min", 1.60),
      ci_max:    num("ci_max", 3.40),
      defci_min: num("defci_min", 0.60),
      defci_max: num("defci_max", 2.00),
      p15_min:   num("p15_min", 36.0),
      p15_max:   num("p15_max", 43.6),
    };
    const exclude = q.exclude_women === "true";
    const WOMEN = [15020, 16037, 16046, 16563];
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      let qq = supabase.from("match_results")
        .select("competition_id, prob15, ci, def_ci, hit_25, snap")
        .not("hit_25", "is", null).not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill");
      if (exclude) qq = qq.not("competition_id", "in", `(${WOMEN.join(",")})`);
      const { data, error } = await qq.range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || !data.length) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }
    const rows = all.filter(m => {
      const fa = m.snap && m.snap.fetchedAt ? String(m.snap.fetchedAt) : "";
      return !fa.includes("(from history)");
    });
    const toPct = (v) => { const n = Number(v || 0); return n > 0 && n < 1 ? n * 100 : n; };
    const feat = (m) => {
      const s = m.snap || {}, h = s.home || {}, a = s.away || {}, l5 = s.l5 || {}, l5h = l5.home || {};
      return {
        l5_h_f: l5h.f != null ? Number(l5h.f) : null,
        h_scored: h.scored_fh != null ? Number(h.scored_fh) : null,
        a_scored: a.scored_fh != null ? Number(a.scored_fh) : null,
        a_conced: a.conced_fh != null ? Number(a.conced_fh) : null,
        ci: Number(m.ci || 0),
        def_ci: Number(m.def_ci || 0),
        p15: toPct(m.prob15),
        hit: m.hit_25 ? 1 : 0,
      };
    };
    const conds = {
      "l5_h_f>=min":      (f) => f.l5_h_f != null && f.l5_h_f >= T.l5_h_f,
      "h_scored_fh>=min": (f) => f.h_scored != null && f.h_scored >= T.h_scored,
      "a_scored_fh>=min": (f) => f.a_scored != null && f.a_scored >= T.a_scored,
      "a_conced_fh>=min": (f) => f.a_conced != null && f.a_conced >= T.a_conced,
      "ci_in_range":      (f) => f.ci >= T.ci_min && f.ci <= T.ci_max,
      "def_ci_in_range":  (f) => f.def_ci >= T.defci_min && f.def_ci <= T.defci_max,
      "prob15_in_band":   (f) => f.p15 >= T.p15_min && f.p15 <= T.p15_max,
    };
    const feats = rows.map(feat);
    const N = feats.length;
    const totalHits = feats.reduce((s, f) => s + f.hit, 0);
    const base = N ? totalHits / N : 0;
    // Per-condition: hit rate among matches that PASS this one condition, plus how
    // many of ALL hits vs ALL misses clear it (the control-group split).
    const perCond = {};
    for (const [name, fn] of Object.entries(conds)) {
      let pass = 0, passHit = 0, hitsPass = 0, missPass = 0;
      for (const f of feats) {
        const ok = fn(f);
        if (ok) { pass++; if (f.hit) passHit++; }
        if (f.hit && ok) hitsPass++;
        if (!f.hit && ok) missPass++;
      }
      perCond[name] = {
        hitRateIfPass: pass ? +(passHit / pass * 100).toFixed(1) : 0,
        lift: (pass && base) ? +((passHit / pass) / base).toFixed(2) : 0,
        pctOfHitsClearing: totalHits ? +(hitsPass / totalHits * 100).toFixed(1) : 0,
        pctOfMissesClearing: (N - totalHits) ? +(missPass / (N - totalHits) * 100).toFixed(1) : 0,
      };
    }
    // Combined filter (ALL conditions).
    let pass = 0, passHit = 0, fail = 0, failHit = 0;
    for (const f of feats) {
      const ok = Object.values(conds).every(fn => fn(f));
      if (ok) { pass++; if (f.hit) passHit++; } else { fail++; if (f.hit) failHit++; }
    }
    res.json({
      ok: true,
      cohortN: N, totalHits, baseRate25: +(base * 100).toFixed(1),
      filter: exclude ? "excluding women's leagues" : "all leagues",
      thresholds: T,
      perCondition: perCond,
      combinedFilter: {
        passN: pass,
        hitRateIfPass: pass ? +(passHit / pass * 100).toFixed(1) : 0,
        lift: (pass && base) ? +((passHit / pass) / base).toFixed(2) : 0,
        coverageOfHits: totalHits ? +(passHit / totalHits * 100).toFixed(1) : 0,
        hitRateIfFail: fail ? +(failHit / fail * 100).toFixed(1) : 0,
      },
      note: "VERDICT KEYS: combinedFilter.hitRateIfPass vs baseRate25 — if it clearly beats base (and the ~19.6% best combo) the floor is a real edge; if it ≈ baseRate25 the floors just describe normal matches. perCondition.pctOfMissesClearing is the control: a condition only discriminates if pctOfHitsClearing >> pctOfMissesClearing.",
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
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
    const womenOnly = req.query.women_only === "true";  // women's-only cohort (validation)
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
      if (womenOnly) q = q.in("competition_id", WOMEN_LEAGUES);
      else if (exclude) q = q.not("competition_id", "in", `(${WOMEN_LEAGUES.join(",")})`);
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
    // Normalize prob to percentage: old rows stored as 11.5, new rows as 0.115
    const toPct = (v) => { const n = Number(v || 0); return n > 0 && n < 1 ? n * 100 : n; };
    // O1.5 / O2.5 candidate split via lib/freeze.js (single source of truth),
    // recomputed from each row's snap so it covers the full history.
    const cand = { ov15: { play: { n: 0, hit: 0 }, skip: { n: 0, hit: 0 } },
                   ov25: { play: { n: 0, hit: 0 }, skip: { n: 0, hit: 0 } } };
    for (const m of all) {
      const r = m.rank;
      const p25 = toPct(m.prob25);
      const p15 = toPct(m.prob15);
      if (byRank[r] !== undefined) {
        byRank[r].n++;
        if (m.hit_25) byRank[r].hit25++;
        if (m.hit_15) byRank[r].hit15++;
        byRank[r].sumP25 += p25;
        byRank[r].sumP15 += p15;
      }
      // Per-combo bucket (2-bit: A+B). Signal C removed from the model.
      const sigs = m.signals || {};
      const bit = (k) => (sigs[k] && sigs[k].met) ? "1" : "0";
      const combo = bit("A") + bit("B");
      if (!byCombo[combo]) byCombo[combo] = { n: 0, hit25: 0, hit15: 0, sumP25: 0, sumP15: 0 };
      byCombo[combo].n++;
      if (m.hit_25) byCombo[combo].hit25++;
      if (m.hit_15) byCombo[combo].hit15++;
      byCombo[combo].sumP25 += p25;
      byCombo[combo].sumP15 += p15;
      // O1.5 / O2.5 candidate split — single source of truth (lib/freeze.js).
      const oc = computeOverCandidates(m.snap, m.prob15, m.prob25);
      const ov15 = oc.ov15Candidate, ov25 = oc.ov25Candidate;
      (ov15 ? cand.ov15.play : cand.ov15.skip).n++;
      if (m.hit_15) (ov15 ? cand.ov15.play : cand.ov15.skip).hit++;
      (ov25 ? cand.ov25.play : cand.ov25.skip).n++;
      if (m.hit_25) (ov25 ? cand.ov25.play : cand.ov25.skip).hit++;
      total.n++;
      if (m.hit_25) total.hit25++;
      if (m.hit_15) total.hit15++;
      total.sumP25 += p25;
      total.sumP15 += p15;
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
    // O1.5 / O2.5 candidate calibration (PLAY = flag fires) vs base rate.
    const pct = (h, n) => (n ? +(h / n * 100).toFixed(1) : 0);
    const base15 = total.n ? total.hit15 / total.n : 0;
    const base25 = total.n ? total.hit25 / total.n : 0;
    const candBlock = (c, base) => ({
      play: { n: c.play.n, hits: c.play.hit, actual: pct(c.play.hit, c.play.n),
              lift: (base && c.play.n) ? +((c.play.hit / c.play.n) / base).toFixed(2) : 0 },
      skip: { n: c.skip.n, hits: c.skip.hit, actual: pct(c.skip.hit, c.skip.n) },
    });
    const candidates = {
      base15: +(base15 * 100).toFixed(1),
      base25: +(base25 * 100).toFixed(1),
      ov15: candBlock(cand.ov15, base15),  // actual = FH-over-1.5 hit rate
      ov25: candBlock(cand.ov25, base25),  // actual = FH-over-2.5 hit rate
      note: "Candidate flags recomputed inline from snap+prob (covers full history). ov15 actual=hit_15, ov25 actual=hit_25. PLAY = flag fires; compare play.actual vs base & play.lift.",
    };
    const WOMEN_LEAGUE_NAMES = { 15020: "Liga MX Femenil", 16037: "Women's", 16046: "Arsenal Women / WSL", 16563: "Women's Internationals" };
    res.json({
      ok: true,
      cohortSize: total.n,
      filter: {
        league: compId ? `competition_id=${compId}` : (womenOnly ? "women's leagues only" : exclude ? "excluding women's leagues" : "all leagues"),
        women_excluded: exclude || false,
        women_only: womenOnly || false,
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
      candidates,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── SIGNAL BACKTEST (live data, no look-ahead bias) ─────────────────────────
// Tests signals A and B against real recorded predictions. Excludes
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
    const womenOnly = req.query.women_only === "true";  // women's-only cohort (validation)
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
      if (womenOnly) q = q.in("competition_id", WOMEN_LEAGUES);
      else if (exclude) q = q.not("competition_id", "in", `(${WOMEN_LEAGUES.join(",")})`);
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
    for (let r = 0; r <= 3; r++) byRank[r] = { n: 0, hit25: 0, hit15: 0, sumP25: 0, sumP15: 0 };
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
    // diverge a lot at the same rank (e.g. A alone vs B alone). This surfaces
    // which combinations actually carry the predictive weight.
    const byCombo = {};
    for (const m of all) {
      const c = ["A", "B"].filter(k => met(m, k)).join("") || "(none)";
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
        league: compId ? `competition_id=${compId}` : (womenOnly ? "women's leagues only" : exclude ? "excluding women's leagues" : "all leagues"),
        women_excluded: exclude || false,
        women_only: womenOnly || false,
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
    for (let r = 0; r <= 3; r++) byRank[r] = { n: 0, hit25: 0, hit15: 0, expSum25: 0, expSum15: 0 };
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
    // Attach candidate flags server-side (single source of truth) so the client
    // doesn't re-derive them — recomputed from each row's frozen pre-game snap.
    for (const m of all) {
      const oc = computeOverCandidates(m.snap, m.prob15, m.prob25);
      m.ov15Candidate = oc.ov15Candidate;
      m.ov25Candidate = oc.ov25Candidate;
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

// ─── DATASET EXPORT — flat CSV/JSON of the clean live cohort for offline analysis ───
// Gated by LOAD_DATASET_TOKEN. One row per completed match with EVERY frozen pre-game
// feature (season stats + xt, last-5 FH form, FootyStats prematch predictors), the fired
// signals/combo/rank, the frozen probabilities, and the final results + hit targets.
// Clean cohort only — historical-import/backfill/(from history) rows are dropped (those
// carry look-ahead bias). Usage: GET /admin/export-dataset?token=<TOKEN>[&format=json]
app.get("/admin/export-dataset", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("match_id, competition_id, league_name, home_name, away_name, date_unix, ht_home, ht_away, ft_home, ft_away, fh_total, hit_15, hit_25, rank, ci, def_ci, prob25, prob15, signals, snap")
        .not("hit_25", "is", null)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .order("date_unix", { ascending: true })
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }
    const rows = all.filter(m => {
      const fa = m.snap && m.snap.fetchedAt ? String(m.snap.fetchedAt) : "";
      return !fa.includes("(from history)");
    });

    const num = (v) => (v == null || v === "" || isNaN(Number(v))) ? "" : Number(v);
    const bit = (s, k) => (s && s[k] && s[k].met) ? 1 : 0;
    const flat = rows.map(m => {
      const s = m.snap || {};
      const h = s.home || {}, a = s.away || {};
      const hx = h.xt || {}, ax = a.xt || {};
      const l5 = s.l5 || {}, l5h = l5.home || {}, l5a = l5.away || {};
      const pm = s.prematch || {};
      const sg = m.signals || {};
      return {
        match_id: m.match_id, competition_id: m.competition_id, league_name: m.league_name || "",
        is_women: WOMENS_LEAGUE_IDS.has(m.competition_id) ? 1 : 0,
        date_unix: m.date_unix || "", date_iso: m.date_unix ? new Date(m.date_unix * 1000).toISOString().slice(0, 10) : "",
        home_name: m.home_name || "", away_name: m.away_name || "", fetchedAt: s.fetchedAt || "", has_l5: s.l5 ? 1 : 0,
        // fired signals / rank / combo (2-char bit(A)bit(B); Signal C removed from model —
        // raw season stats below still let you study the mismatch offline)
        sigA: bit(sg, "A"), sigB: bit(sg, "B"),
        combo: "" + bit(sg, "A") + bit(sg, "B"), rank: m.rank == null ? "" : m.rank,
        prob15: num(m.prob15), prob25: num(m.prob25), ci: num(m.ci), def_ci: num(m.def_ci),
        // home season stats
        h_scored_fh: num(h.scored_fh), h_conced_fh: num(h.conced_fh), h_t1_pct: num(h.t1_pct), h_cn010: num(h.cn010_avg), h_sot: num(h.sot_avg),
        // away season stats
        a_scored_fh: num(a.scored_fh), a_conced_fh: num(a.conced_fh), a_t1_pct: num(a.t1_pct), a_cn010: num(a.cn010_avg), a_sot: num(a.sot_avg),
        // home extended season fields (xt — forward captures only, may be blank on older rows)
        h_o15ht: num(hx.o15ht), h_o05ht: num(hx.o05ht), h_bttsfhg: num(hx.bttsfhg), h_leadHT: num(hx.leadHT), h_xgf: num(hx.xgf), h_xga: num(hx.xga), h_datk: num(hx.datk), h_fhsc: num(hx.fhsc), h_fhcn: num(hx.fhcn),
        // away extended season fields (xt)
        a_o15ht: num(ax.o15ht), a_o05ht: num(ax.o05ht), a_bttsfhg: num(ax.bttsfhg), a_leadHT: num(ax.leadHT), a_xgf: num(ax.xgf), a_xga: num(ax.xga), a_datk: num(ax.datk), a_fhsc: num(ax.fhsc), a_fhcn: num(ax.fhcn),
        // last-5 first-half form (f=scored, a=conceded, t=total per game)
        l5_h_f: num(l5h.f), l5_h_a: num(l5h.a), l5_h_t: num(l5h.t), l5_a_f: num(l5a.f), l5_a_a: num(l5a.a), l5_a_t: num(l5a.t),
        // FootyStats own prematch predictors
        pm_o15HT: num(pm.o15HT), pm_o05HT: num(pm.o05HT), pm_xgHome: num(pm.xgHome), pm_xgAway: num(pm.xgAway), pm_btts_fhg: num(pm.btts_fhg),
        // results + targets
        ht_home: num(m.ht_home), ht_away: num(m.ht_away), ft_home: num(m.ft_home), ft_away: num(m.ft_away), fh_total: num(m.fh_total),
        hit_15: m.hit_15 ? 1 : 0, hit_25: m.hit_25 ? 1 : 0,
      };
    });

    if (req.query.format === "json") return res.json({ ok: true, n: flat.length, rows: flat });
    const cols = flat.length ? Object.keys(flat[0]) : [];
    const esc = (v) => { const sv = v == null ? "" : String(v); return /[",\n]/.test(sv) ? '"' + sv.replace(/"/g, '""') + '"' : sv; };
    const lines = [cols.join(",")];
    for (const r of flat) lines.push(cols.map(c => esc(r[c])).join(","));
    res.set("Content-Type", "text/csv; charset=utf-8");
    res.set("Content-Disposition", `attachment; filename="footy_dataset_${new Date().toISOString().slice(0, 10)}.csv"`);
    res.send(lines.join("\n"));
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
      // Women's leagues ARE served live (using the global table) — they're only kept out
      // of the recalibration cohort (see /admin/backfill, /signalc-validate). Validate
      // their fit with `?women_only=true` on /signal-backtest and /calibration.
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
      // Index full match objects by ID so the inner loop can read pre-match fields
      // (o15HT_potential, xg, etc.) that exist on /league-matches but not /fixtures.
      const matchMap = new Map();
      for (const m of allMatches) if (m.id) matchMap.set(m.id, m);
      return { sid, leagueName, teamMap, fixtures: leagueFixtures[sid], matchMap };
    }));

    // ── International cross-competition last-5 ──────────────────────────────────
    // National teams split their games across many competitions (friendlies,
    // Nations League, qualifiers) — so a fixture's own competition rarely holds ≥3
    // of their recent games and buildLast5 returns null. When the window contains
    // international fixtures, pull recent matches from every current international
    // competition into localExtra so last-5 can be assembled across competitions.
    // Runs AFTER the fixtured leagues are fetched, so normal predictions are never
    // starved; fetchLeagueMatches is 2h-cached and safeFetch backs off gracefully
    // if it hits the rate limit (those comps just stay absent, no crash).
    if (INTERNATIONAL_SIDS.size && leagueSids.some(s => INTERNATIONAL_SIDS.has(parseInt(s, 10)))) {
      const already = new Set(leagueSids.map(s => parseInt(s, 10)));
      const intlToFetch = [...INTERNATIONAL_SIDS].filter(s => !already.has(s));
      const CHUNK = 5;
      for (let i = 0; i < intlToFetch.length; i += CHUNK) {
        const chunk = intlToFetch.slice(i, i + CHUNK);
        const res = await Promise.all(chunk.map(s => fetchLeagueMatches(s).catch(() => ({ data: [] }))));
        res.forEach((r, j) => addToLocalExtra(r.data || [], LEAGUE_NAMES[chunk[j]] || ""));
      }
    }

    for (const { sid, leagueName, teamMap, fixtures, matchMap } of leagueData) {
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
          snap = { fetchedAt, home: hStats, away: aStats };
          if (l5Data) snap.l5 = l5Data;
          // Capture pre-match fixture-level predictors from /league-matches.
          // FootyStats does NOT expose last-5 HT team stats (no scoredAVGHT_*_5
          // fields exist), so when self-reconstruction yields null these match-level
          // predictors are the only pre-game first-half signal available for thin
          // leagues. Captured here; used as a fallback once calibrated.
          const fullFix = matchMap.get(fix.id);
          if (fullFix) {
            const pm = {};
            if (fullFix.o15HT_potential    != null) pm.o15HT   = fullFix.o15HT_potential;
            if (fullFix.o05HT_potential    != null) pm.o05HT   = fullFix.o05HT_potential;
            if (fullFix.team_a_xg_prematch != null) pm.xgHome  = fullFix.team_a_xg_prematch;
            if (fullFix.team_b_xg_prematch != null) pm.xgAway  = fullFix.team_b_xg_prematch;
            if (fullFix.btts_fhg_potential != null) pm.btts_fhg = fullFix.btts_fhg_potential;
            if (Object.keys(pm).length) snap.prematch = pm;
          }
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

        // l5snap = whatever snap.l5 was set to: self-reconstructed or native API fallback.
        const l5snap = snap ? snap.l5 : null;

        // Snapshot pre-match prediction values before match completes
        const matchId = fix.id;
        if (result && !isComplete && !CI_SNAPSHOT_CACHE[matchId]) {
          CI_SNAPSHOT_CACHE[matchId] = {
            ci: result.ci, defCi: result.defCi, rank: result.rank,
            label: result.label, prob25: result.prob25, prob15: result.prob15,
            probSource: result.probSource, probSampleN: result.probSampleN, probCombo: result.probCombo,
            eligible: result.eligible, eligible25: result.eligible25, eligible15: result.eligible15, signals: result.signals,
            ov15Candidate: result.ov15Candidate, ov25Candidate: result.ov25Candidate,
            snap: snap ? {
              fetchedAt: snap.fetchedAt,
              home: { name: snap.home.name, scored_fh: snap.home.scored_fh, conced_fh: snap.home.conced_fh, t1_pct: snap.home.t1_pct, cn010_avg: snap.home.cn010_avg, sot_avg: snap.home.sot_avg, ...(snap.home.xt ? { xt: snap.home.xt } : {}) },
              away: { name: snap.away.name, scored_fh: snap.away.scored_fh, conced_fh: snap.away.conced_fh, t1_pct: snap.away.t1_pct, cn010_avg: snap.away.cn010_avg, sot_avg: snap.away.sot_avg, ...(snap.away.xt ? { xt: snap.away.xt } : {}) },
              l5: l5snap,
              ...(snap.prematch ? { prematch: snap.prematch } : {}),
            } : null,
          };
        }
        // Use frozen snapshot if match is complete and we have one
        const frozen = isComplete ? CI_SNAPSHOT_CACHE[matchId] : null;
        // A completed match must ONLY ever show a prediction that was frozen before
        // kickoff. If it was never frozen (e.g. internationals whose stats only became
        // computable after the window's earlier games were cached), do NOT fall back to
        // the live `result` — that's recomputed from current, post-match data, i.e.
        // look-ahead. Use the live result only while the match is still upcoming.
        const pre = selectPregamePrediction(frozen, result, isComplete);
        const liveSnap = snap ? {
          fetchedAt: snap.fetchedAt,
          home: { name: snap.home.name, scored_fh: snap.home.scored_fh, conced_fh: snap.home.conced_fh, t1_pct: snap.home.t1_pct, cn010_avg: snap.home.cn010_avg, sot_avg: snap.home.sot_avg, ...(snap.home.xt ? { xt: snap.home.xt } : {}) },
          away: { name: snap.away.name, scored_fh: snap.away.scored_fh, conced_fh: snap.away.conced_fh, t1_pct: snap.away.t1_pct, cn010_avg: snap.away.cn010_avg, sot_avg: snap.away.sot_avg, ...(snap.away.xt ? { xt: snap.away.xt } : {}) },
          l5: l5snap,
          ...(snap.prematch ? { prematch: snap.prematch } : {}),
        } : null;

        const pred = {
          id: matchId, homeId, awayId,
          league: leagueName, leagueSid: parseInt(sid, 10),
          home: fix.home_name || "", away: fix.away_name || "",
          dt: (fix.date_unix || 0) * 1000,
          matchDate, status: fix.status || "upcoming",
          missingStats: (missing && !result) || (isComplete && !frozen),
          snap: frozen ? frozen.snap : (!isComplete ? liveSnap : null),
          rank:     pre ? pre.rank     : 0,
          label:    pre ? pre.label    : "Low",
          prob25:   pre ? pre.prob25   : 10.0,
          prob15:   pre ? pre.prob15   : 31.4,
          probSource:  pre ? pre.probSource  : "global",
          probSampleN: pre ? pre.probSampleN : 0,
          probCombo:   pre ? pre.probCombo   : null,
          eligible:   pre ? pre.eligible   : false,
          eligible25: pre ? pre.eligible25 : false,
          eligible15: pre ? pre.eligible15 : false,
          ci:       pre ? pre.ci       : 0,
          defCi:    pre ? pre.defCi    : 0,
          signals:  pre ? pre.signals  : {},
          ov15Candidate: pre ? pre.ov15Candidate : false,
          ov25Candidate: pre ? pre.ov25Candidate : false,
          hLast5, aLast5, hAvgFH, aAvgFH,
          matchResult: isComplete ? { fhH, fhA, ftH, ftA, hit25: (fhH+fhA)>2, hit15: (fhH+fhA)>1 } : null,
        };
        pred.pattern = derivePatternContext(pred);
        preds.push(pred);
      }
    }

    rebuildServerMatchCache();
    // Probability-first ordering: the per-combo calibrated probability is the source of
    // truth (Signal C is anti-additive, so rank count can disagree). Pattern scores are
    // secondary context only, then ci/rank break ties.
    preds.sort((a, b) =>
      (b.rank || 0) - (a.rank || 0) ||
      (b.prob25 || 0) - (a.prob25 || 0) ||
      (((b.pattern || {}).score25) || 0) - (((a.pattern || {}).score25) || 0) ||
      (b.prob15 || 0) - (a.prob15 || 0) ||
      (((b.pattern || {}).score15) || 0) - (((a.pattern || {}).score15) || 0) ||
      (b.ci || 0) - (a.ci || 0));
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
// old captures that predate the current A+B last-5 model. Reports lift for a grid
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

// Calibrate FootyStats' own pre-match first-half predictors (snap.prematch:
// o15HT/o05HT potentials, pre-match xG, btts_fhg) against actual FH results.
// This is the only pre-game FH signal available for thin-coverage leagues where
// our self-reconstructed l5 is null. Reports hit rates / lift by threshold so we
// can pick a cutoff for o15HT_potential before wiring it in as a fallback.
// Run /admin/backfill-prematch first to populate snap.prematch on existing rows.
app.get("/prematch-mine", async (req, res) => {
  res.set("Cache-Control", "no-store");
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
    const cohort = all.filter(m => m.snap && m.snap.prematch);
    const hasL5 = (m) => !!(m.snap.l5 && m.snap.l5.home && m.snap.l5.away);
    // The blind-spot subset: prematch present but l5 null — exactly where o15HT
    // would actually be used as a fallback. This is the population that matters.
    const blindspot = cohort.filter(m => !hasL5(m));

    const pm = (m) => m.snap.prematch || {};

    // Run the full threshold sweep over an arbitrary subset, with that subset's
    // own base rates, so the lift is honest within-population (not vs the global base).
    const mine = (rows) => {
      const N = rows.length;
      const b25 = N ? rows.filter(m => m.hit_25).length / N : 0;
      const b15 = N ? rows.filter(m => m.hit_15).length / N : 0;
      const test = (label, pred) => {
        const fired = rows.filter(m => pred(pm(m)));
        const fN = fired.length;
        if (!fN) return { label, n: 0, actual25: 0, actual15: 0, lift25: 0, lift15: 0 };
        const h25 = fired.filter(m => m.hit_25).length;
        const h15 = fired.filter(m => m.hit_15).length;
        return {
          label, n: fN,
          actual25: +(h25 / fN * 100).toFixed(1),
          actual15: +(h15 / fN * 100).toFixed(1),
          lift25: b25 ? +(h25 / fN / b25).toFixed(2) : 0,
          lift15: b15 ? +(h15 / fN / b15).toFixed(2) : 0,
        };
      };
      const results = [];
      // o15HT_potential is FootyStats' own FH-over-1.5 likelihood (0–100). Sweep cutoffs.
      for (const t of [40, 45, 50, 55, 60, 65, 70]) results.push(test("o15HT>=" + t, x => x.o15HT != null && x.o15HT >= t));
      for (const t of [60, 65, 70, 75, 80, 85]) results.push(test("o05HT>=" + t, x => x.o05HT != null && x.o05HT >= t));
      for (const t of [2.0, 2.5, 3.0, 3.5]) results.push(test("xgTotal>=" + t, x => (x.xgHome != null && x.xgAway != null) && (x.xgHome + x.xgAway) >= t));
      for (const t of [20, 25, 30, 35]) results.push(test("btts_fhg>=" + t, x => x.btts_fhg != null && x.btts_fhg >= t));
      results.push(test("o15HT>=50 & xgTotal>=2.5", x => x.o15HT >= 50 && (x.xgHome + x.xgAway) >= 2.5));
      results.push(test("o15HT>=55 & xgTotal>=3.0", x => x.o15HT >= 55 && (x.xgHome + x.xgAway) >= 3.0));
      return {
        n: N,
        baseRate25: +(b25 * 100).toFixed(1),
        baseRate15: +(b15 * 100).toFixed(1),
        results,
      };
    };

    const full = mine(cohort);
    const bs = mine(blindspot);
    const actionable = full.results.filter(r => r.n >= 20);

    res.json({
      ok: true,
      cohortTotal: cohortAll,
      cohortWithPrematch: full.n,
      withoutPrematch: cohortAll - full.n,
      // ── Full cohort (mostly l5-present) — the broad validation ──
      baseRate25: full.baseRate25,
      baseRate15: full.baseRate15,
      topByLift15: actionable.slice().sort((a, b) => b.lift15 - a.lift15).slice(0, 15),
      topByLift25: actionable.slice().sort((a, b) => b.lift25 - a.lift25).slice(0, 15),
      allResults: full.results,
      // ── Blind-spot subset (l5 null) — the population we'd actually use this on ──
      blindspot: {
        n: bs.n,
        baseRate25: bs.baseRate25,
        baseRate15: bs.baseRate15,
        results: bs.results,
        note: "Same sweep restricted to prematch-present rows whose l5 is null — i.e. the thin-league matches our A+B signals can't touch. Grows as forward capture adds these rows. Want o15HT cutoffs here to hold actual15 well above this subset's own baseRate15 before wiring in.",
      },
      note: "Full cohort lift is vs the full-cohort base. o15HT_potential is FootyStats' own FH>1.5 score. Decision gate: does o15HT predict FH>1.5 within the blindspot subset, not just overall?",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Mine FootyStats SEASON stats (frozen pre-game in snap.home/away) as FH-goal
// signals, with an HONEST train/test date split so we don't repeat the o15HT
// overfit. Older matches = train (threshold selection), newer = test (reported).
// A candidate is only credible if its TEST lift holds near its train lift.
//
// Always-present features (every live row): scored_fh, conced_fh, t1_pct.
// Extended features (xt.*: o15ht, xgf, fhsc, …) populate only on forward captures
// after this deploy, so their n grows over time — the same row may report them as
// null until then. Look-ahead-free because snap freezes these at kickoff.
app.get("/season-mine", async (req, res) => {
  res.set("Cache-Control", "no-store");
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("hit_25, hit_15, date_unix, snap")
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
    const cohort = all.filter(m => m.snap && m.snap.home && m.snap.away && m.date_unix);
    cohort.sort((a, b) => (a.date_unix || 0) - (b.date_unix || 0));

    // 70/30 chronological split — train on the past, test on the most recent.
    const cut = Math.floor(cohort.length * 0.7);
    const train = cohort.slice(0, cut);
    const test = cohort.slice(cut);

    // Flatten the snap into a feature map (home/away season stats + extended xt).
    const feat = (m) => {
      const h = m.snap.home || {}, a = m.snap.away || {};
      const hx = h.xt || {}, ax = a.xt || {};
      return {
        hScored: h.scored_fh, aScored: a.scored_fh,
        hConced: h.conced_fh, aConced: a.conced_fh,
        hT1: h.t1_pct, aT1: a.t1_pct,
        // extended (may be undefined on pre-deploy rows)
        hO15: hx.o15ht, aO15: ax.o15ht,
        hXgf: hx.xgf, aXgf: ax.xgf, hXga: hx.xga, aXga: ax.xga,
        hFhsc: hx.fhsc, aFhsc: ax.fhsc, hFhcn: hx.fhcn, aFhcn: ax.fhcn,
        hDatk: hx.datk, aDatk: ax.datk,
      };
    };

    const rate = (rows, line) => rows.length ? rows.filter(m => m[line]).length / rows.length : 0;
    const baseTrain15 = rate(train, "hit_15"), baseTest15 = rate(test, "hit_15");
    const baseTrain25 = rate(train, "hit_25"), baseTest25 = rate(test, "hit_25");

    // Evaluate a predicate on a row set, requiring all referenced features present.
    const evalOn = (rows, pred) => {
      const fired = rows.filter(m => { const f = feat(m); return pred(f); });
      const n = fired.length;
      if (!n) return { n: 0, hit15: 0, hit25: 0 };
      return {
        n,
        hit15: +(fired.filter(m => m.hit_15).length / n * 100).toFixed(1),
        hit25: +(fired.filter(m => m.hit_25).length / n * 100).toFixed(1),
      };
    };

    // Candidate single/twin-feature thresholds. `req` lists features that must be
    // non-null for a row to count (keeps extended-feature candidates honest).
    const C = [];
    const add = (label, need, pred) => C.push({ label, need, pred });
    const has = (f, keys) => keys.every(k => f[k] != null);
    // Always-present season HT stats (mineable on the full 888 today):
    for (const t of [0.7, 0.8, 0.9, 1.0]) add("hScored>=" + t, ["hScored"], f => f.hScored >= t);
    for (const t of [0.6, 0.7, 0.8, 0.9]) add("aScored>=" + t, ["aScored"], f => f.aScored >= t);
    for (const t of [0.7, 0.8, 0.9]) add("hConced>=" + t, ["hConced"], f => f.hConced >= t);
    for (const t of [0.7, 0.8, 0.9]) add("aConced>=" + t, ["aConced"], f => f.aConced >= t);
    for (const t of [20, 25, 30]) add("hT1>=" + t, ["hT1"], f => f.hT1 >= t);
    for (const t of [20, 25, 30]) add("aT1>=" + t, ["aT1"], f => f.aT1 >= t);
    add("hScored+aScored>=1.6", ["hScored", "aScored"], f => f.hScored + f.aScored >= 1.6);
    add("hScored+aConced>=1.7", ["hScored", "aConced"], f => f.hScored + f.aConced >= 1.7);
    add("both T1>=25", ["hT1", "aT1"], f => f.hT1 >= 25 && f.aT1 >= 25);
    add("hScored>=0.9 & aScored>=0.7", ["hScored", "aScored"], f => f.hScored >= 0.9 && f.aScored >= 0.7);
    // Extended features (populate after deploy):
    for (const t of [40, 45, 50]) add("xt:o15ht both>=" + t, ["hO15", "aO15"], f => f.hO15 >= t && f.aO15 >= t);
    for (const t of [2.0, 2.5]) add("xt:xgFor sum>=" + t, ["hXgf", "aXgf"], f => f.hXgf + f.aXgf >= t);
    add("xt:fhScore sum>=1.4", ["hFhsc", "aFhsc"], f => f.hFhsc + f.aFhsc >= 1.4);

    // For each candidate: train hit/lift (selection) AND test hit/lift (verdict).
    const results = C.map(c => {
      const tr = evalOn(train, m => has(feat(m), c.need) && c.pred(feat(m)));
      const te = evalOn(test, m => has(feat(m), c.need) && c.pred(feat(m)));
      return {
        label: c.label,
        train: { n: tr.n, hit15: tr.hit15, lift15: baseTrain15 ? +(tr.hit15 / 100 / baseTrain15).toFixed(2) : 0, hit25: tr.hit25 },
        test:  { n: te.n, hit15: te.hit15, lift15: baseTest15 ? +(te.hit15 / 100 / baseTest15).toFixed(2) : 0, hit25: te.hit25 },
      };
    });

    // Credible = selected on train (n>=30, lift>1.15) AND holds on test (n>=15, lift>1.1).
    const credible = results.filter(r =>
      r.train.n >= 30 && r.train.lift15 > 1.15 && r.test.n >= 15 && r.test.lift15 > 1.1
    ).sort((a, b) => b.test.lift15 - a.test.lift15);

    res.json({
      ok: true,
      cohort: cohort.length,
      trainN: train.length, testN: test.length,
      baseRate15: { train: +(baseTrain15 * 100).toFixed(1), test: +(baseTest15 * 100).toFixed(1) },
      baseRate25: { train: +(baseTrain25 * 100).toFixed(1), test: +(baseTest25 * 100).toFixed(1) },
      currentModelRef: "A+B combo 11 ≈ 44% FH>1.5; B-only ≈ 42%. Beat that out-of-sample to matter.",
      credible,
      allResults: results.sort((a, b) => b.test.lift15 - a.test.lift15),
      note: "TRAIN selects, TEST judges. Trust only candidates whose test.lift15 stays near train.lift15 at reasonable n — that's an out-of-sample win, not an overfit. xt:* candidates show n=0 until forward capture accumulates post-deploy rows.",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Correlate pre-game last-5 CORNER production with the over-2.5 outcome.
// For each resolved match, reconstruct each team's last-5 corner averages from
// match history (games strictly BEFORE the match date — look-ahead-free, same
// discipline as L5), then correlate the combined attacking-corner volume with the
// goals-over-2.5 result. Reports against BOTH lines (FH>2.5 and FT>2.5) and BOTH
// corner types (full-match corners and first-half corners), since "over 2.5" is
// ambiguous. Corner fields are ~50% covered on thin leagues, so usable n < cohort.
app.get("/corner-mine", async (req, res) => {
  res.set("Cache-Control", "no-store");
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  const t0 = Date.now();
  try {
    const all = [];
    const PAGE = 1000;
    const cutoffSec = Math.floor(Date.now() / 1000) - 400 * 86400;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("match_id, competition_id, home_id, away_id, date_unix, ht_home, ht_away, ft_home, ft_away")
        .not("hit_25", "is", null)
        .gte("date_unix", cutoffSec)
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || !data.length) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }
    const rows = all.filter(r =>
      r.competition_id && r.home_id && r.away_id && r.date_unix &&
      r.ht_home != null && r.ht_away != null && r.ft_home != null && r.ft_away != null &&
      !WOMENS_LEAGUE_IDS.has(r.competition_id));

    const byComp = {};
    for (const r of rows) { (byComp[r.competition_id] = byComp[r.competition_id] || []).push(r); }

    const pres = (v) => v != null && v !== -1 && v !== "" && !isNaN(Number(v));
    const feats = [];
    let competitionsFetched = 0, skippedThin = 0, rateLimited = false;

    for (const [cid, rs] of Object.entries(byComp)) {
      if (Date.now() < RATE_LIMITED_UNTIL) { rateLimited = true; break; }
      let matches;
      try { const mr = await fetchLeagueMatches(cid); matches = (mr && mr.data) || []; }
      catch (_) { continue; }
      competitionsFetched++;

      // team's last-5 corners-FOR average (full-match and first-half), as-of a date.
      const teamL5 = (tid, before) => {
        const games = matches
          .filter(m => (m.homeID === tid || m.awayID === tid) && (m.date_unix || 0) < before && m.status === "complete")
          .sort((a, b) => (b.date_unix || 0) - (a.date_unix || 0));
        const out = {};
        const full = games.filter(m => pres(m.team_a_corners) && pres(m.team_b_corners)).slice(0, 5);
        if (full.length >= 3) out.forFull = full.reduce((s, m) => s + Number(m.homeID === tid ? m.team_a_corners : m.team_b_corners), 0) / full.length;
        const fh = games.filter(m => pres(m.team_a_fh_corners) && pres(m.team_b_fh_corners)).slice(0, 5);
        if (fh.length >= 3) out.forFH = fh.reduce((s, m) => s + Number(m.homeID === tid ? m.team_a_fh_corners : m.team_b_fh_corners), 0) / fh.length;
        return out;
      };

      for (const r of rs) {
        const h = teamL5(r.home_id, r.date_unix);
        const a = teamL5(r.away_id, r.date_unix);
        const rec = {
          o25fh: (Number(r.ht_home) + Number(r.ht_away)) > 2 ? 1 : 0,
          o25ft: (Number(r.ft_home) + Number(r.ft_away)) > 2 ? 1 : 0,
        };
        if (h.forFull != null && a.forFull != null) rec.cornFull = +(h.forFull + a.forFull).toFixed(2);
        if (h.forFH != null && a.forFH != null) rec.cornFH = +(h.forFH + a.forFH).toFixed(2);
        if (rec.cornFull != null || rec.cornFH != null) feats.push(rec); else skippedThin++;
      }
    }

    const pearson = (xs, ys) => {
      const n = xs.length;
      if (n < 3) return null;
      let sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
      for (let i = 0; i < n; i++) { sx += xs[i]; sy += ys[i]; sxx += xs[i] * xs[i]; syy += ys[i] * ys[i]; sxy += xs[i] * ys[i]; }
      const cov = sxy - sx * sy / n, vx = sxx - sx * sx / n, vy = syy - sy * sy / n;
      if (vx <= 0 || vy <= 0) return null;
      return cov / Math.sqrt(vx * vy);
    };

    const analyze = (featKey, outKey) => {
      const sub = feats.filter(f => f[featKey] != null);
      if (sub.length < 10) return { n: sub.length, note: "too few rows" };
      const xs = sub.map(f => f[featKey]), ys = sub.map(f => f[outKey]);
      const r = pearson(xs, ys);
      const sorted = [...xs].sort((a, b) => a - b);
      const q = (p) => sorted[Math.max(0, Math.floor(p * (sorted.length - 1)))];
      const cuts = [q(0.25), q(0.5), q(0.75)];
      const idxOf = (x) => x <= cuts[0] ? 0 : x <= cuts[1] ? 1 : x <= cuts[2] ? 2 : 3;
      const agg = [0, 1, 2, 3].map(() => ({ n: 0, hit: 0, xsum: 0 }));
      for (const f of sub) { const b = idxOf(f[featKey]); agg[b].n++; agg[b].hit += f[outKey]; agg[b].xsum += f[featKey]; }
      const labels = ["Q1 (fewest corners)", "Q2", "Q3", "Q4 (most corners)"];
      return {
        n: sub.length,
        baseRate: +(ys.reduce((s, y) => s + y, 0) / ys.length * 100).toFixed(1),
        correlation: r != null ? +r.toFixed(3) : null,
        quartileCuts: cuts.map(c => +c.toFixed(2)),
        buckets: agg.map((g, i) => ({
          bucket: labels[i], n: g.n,
          avgCorners: g.n ? +(g.xsum / g.n).toFixed(2) : 0,
          over25Rate: g.n ? +(g.hit / g.n * 100).toFixed(1) : 0,
        })),
      };
    };

    res.json({
      ok: true,
      cohortRows: rows.length,
      usableWithCorners: feats.length,
      competitionsFetched,
      skippedThinNoCorners: skippedThin,
      rateLimitedEarly: rateLimited,
      results: {
        "fullCorners_vs_FH_over2.5": analyze("cornFull", "o25fh"),
        "fullCorners_vs_FT_over2.5": analyze("cornFull", "o25ft"),
        "fhCorners_vs_FH_over2.5": analyze("cornFH", "o25fh"),
        "fhCorners_vs_FT_over2.5": analyze("cornFH", "o25ft"),
      },
      note: "correlation = Pearson r between combined pre-game last-5 corners-for and the binary over-2.5 outcome (point-biserial). |r|<0.1 ~ no relationship; 0.1-0.3 weak; >0.3 moderate. Compare Q4 over25Rate vs Q1/baseRate to see if more recent corners → more goals. Corners reconstructed from games before each match (look-ahead-free).",
      elapsedMs: Date.now() - t0,
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

// Characterise the FALSE NEGATIVES: matches the CURRENT model rates rank 0
// (neither A: hT>=1.6 & aT>=1.4, nor B: aF>=0.8) that still went over in the
// first half. For each pre-game feature, contrast its mean among rank-0 games
// that DID go over vs those that didn't — a feature that separates them is a
// candidate signal we're currently missing. Then sweep thresholds for lift.
// In-sample/exploratory: confirm any hit with a holdout before wiring (o15HT lesson).
app.get("/rank0-overs", async (req, res) => {
  res.set("Cache-Control", "no-store");
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
    const withL5 = all.filter(m => m.snap && m.snap.l5 && m.snap.l5.home && m.snap.l5.away);

    // CURRENT production signal definitions (must match computeSignals).
    const isRank0 = (m) => {
      const hT = m.snap.l5.home.t || 0, aT = m.snap.l5.away.t || 0, aF = m.snap.l5.away.f || 0;
      const sigA = hT >= 1.6 && aT >= 1.4;
      const sigB = aF >= 0.8;
      return !sigA && !sigB;
    };
    const rank0 = withL5.filter(isRank0);

    const F = (m) => {
      const l5 = m.snap.l5, h = m.snap.home || {}, a = m.snap.away || {}, pm = m.snap.prematch || {};
      return {
        hT: l5.home.t, aT: l5.away.t, ci: (l5.home.t || 0) + (l5.away.t || 0),
        hF: l5.home.f, hA: l5.home.a, aF: l5.away.f, aA: l5.away.a,
        h_scored_fh: h.scored_fh, a_scored_fh: a.scored_fh,
        h_conced_fh: h.conced_fh, a_conced_fh: a.conced_fh,
        h_t1: h.t1_pct, a_t1: a.t1_pct,
        o15HT: pm.o15HT, o05HT: pm.o05HT,
      };
    };

    const n = rank0.length;
    const overs15 = rank0.filter(m => m.hit_15);
    const overs25 = rank0.filter(m => m.hit_25);
    const base15 = n ? overs15.length / n : 0;
    const base25 = n ? overs25.length / n : 0;

    const r2 = (v) => v == null ? null : +v.toFixed(2);
    const mean = (arr) => arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : null;

    // Feature contrast: mean of each feature among rank-0 OVERS vs rank-0 NON-overs.
    const featureKeys = ["hT", "aT", "ci", "hF", "hA", "aF", "aA",
      "h_scored_fh", "a_scored_fh", "h_conced_fh", "a_conced_fh", "h_t1", "a_t1", "o15HT", "o05HT"];
    const contrast = {};
    for (const k of featureKeys) {
      const present = rank0.filter(m => F(m)[k] != null);
      const o15 = present.filter(m => m.hit_15).map(m => F(m)[k]);
      const u15 = present.filter(m => !m.hit_15).map(m => F(m)[k]);
      const o25 = present.filter(m => m.hit_25).map(m => F(m)[k]);
      const u25 = present.filter(m => !m.hit_25).map(m => F(m)[k]);
      const m_o15 = mean(o15), m_u15 = mean(u15), m_o25 = mean(o25), m_u25 = mean(u25);
      contrast[k] = {
        nPresent: present.length,
        over15_mean: r2(m_o15), under15_mean: r2(m_u15),
        delta15: (m_o15 != null && m_u15 != null) ? r2(m_o15 - m_u15) : null,
        over25_mean: r2(m_o25), under25_mean: r2(m_u25),
        delta25: (m_o25 != null && m_u25 != null) ? r2(m_o25 - m_u25) : null,
      };
    }
    // Rank features by how strongly they separate the FH>1.5 overs (|delta15|).
    const topSeparators15 = Object.entries(contrast)
      .filter(([, v]) => v.delta15 != null && v.nPresent >= 30)
      .sort((a, b) => Math.abs(b[1].delta15) - Math.abs(a[1].delta15))
      .map(([k, v]) => ({ feature: k, delta15: v.delta15, over15_mean: v.over15_mean, under15_mean: v.under15_mean, n: v.nPresent }));
    const topSeparators25 = Object.entries(contrast)
      .filter(([, v]) => v.delta25 != null && v.nPresent >= 30)
      .sort((a, b) => Math.abs(b[1].delta25) - Math.abs(a[1].delta25))
      .map(([k, v]) => ({ feature: k, delta25: v.delta25, over25_mean: v.over25_mean, under25_mean: v.under25_mean, n: v.nPresent }));

    // Threshold lift sweep within rank-0 (asymmetric / season / prematch patterns).
    const test = (label, pred) => {
      const sub = rank0.filter(m => { try { return pred(F(m)); } catch (_) { return false; } });
      const fN = sub.length;
      if (!fN) return { label, n: 0 };
      const h25 = sub.filter(m => m.hit_25).length, h15 = sub.filter(m => m.hit_15).length;
      return {
        label, n: fN,
        actual15: +(h15 / fN * 100).toFixed(1), lift15: base15 ? +(h15 / fN / base15).toFixed(2) : 0,
        actual25: +(h25 / fN * 100).toFixed(1), lift25: base25 ? +(h25 / fN / base25).toFixed(2) : 0,
      };
    };
    const ge = (v, t) => v != null && v >= t;
    const results = [];
    for (const t of [1.0, 1.2, 1.4]) results.push(test("hF>=" + t + " (home L5 scored)", x => ge(x.hF, t)));
    for (const t of [2.0, 2.5, 3.0]) results.push(test("hT>=" + t + " (home L5 total)", x => ge(x.hT, t)));
    for (const t of [1.5, 2.0]) results.push(test("aT>=" + t + " (away L5 total)", x => ge(x.aT, t)));
    for (const t of [1.0, 1.2]) { results.push(test("hA>=" + t + " (home L5 leak)", x => ge(x.hA, t))); results.push(test("aA>=" + t + " (away L5 leak)", x => ge(x.aA, t))); }
    results.push(test("hF>=1.0 & aA>=1.0 (home atk vs away leak)", x => ge(x.hF, 1.0) && ge(x.aA, 1.0)));
    results.push(test("home scored_fh>=0.9 (season)", x => ge(x.h_scored_fh, 0.9)));
    results.push(test("both t1_pct>=25 (season)", x => ge(x.h_t1, 25) && ge(x.a_t1, 25)));
    for (const t of [45, 50, 55]) results.push(test("o15HT>=" + t + " (prematch)", x => ge(x.o15HT, t)));

    const actionable = results.filter(r => r.n >= 20);

    res.json({
      ok: true,
      cohortWithL5: withL5.length,
      rank0Total: n,
      rank0Pct: withL5.length ? +(n / withL5.length * 100).toFixed(1) : 0,
      rank0_overs15: overs15.length,
      rank0_overs25: overs25.length,
      baseRate15: +(base15 * 100).toFixed(1),
      baseRate25: +(base25 * 100).toFixed(1),
      topSeparators15,
      topSeparators25,
      featureContrast: contrast,
      thresholdSweep_byLift15: actionable.slice().sort((a, b) => b.lift15 - a.lift15).slice(0, 12),
      thresholdSweep_byLift25: actionable.slice().sort((a, b) => b.lift25 - a.lift25).slice(0, 12),
      note: "rank0 = neither current signal fires. topSeparators ranks features by how differently they read on the overs vs non-overs (a real pattern shows a large delta at decent n). thresholdSweep lift>1.3 at n>=20 is a candidate — but in-sample; confirm with a date holdout before wiring (o15HT looked great in-sample then failed on the blind spot).",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// HOLDOUT test for the "home attack dominance" lead surfaced by /rank0-overs:
// among rank-0 matches (neither current signal fires), does a strong home attack
// predict FH goals OUT OF SAMPLE? Chronological 70/30 split of the rank-0 cohort —
// older = train (picks the threshold), newer = test (judges it). A candidate is
// only credible if its TEST lift holds; this is the check we skipped before o15HT.
app.get("/rank0-holdout", async (req, res) => {
  res.set("Cache-Control", "no-store");
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("hit_25, hit_15, date_unix, snap")
        .not("hit_25", "is", null)
        .not("date_unix", "is", null)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }
    const withL5 = all.filter(m => m.snap && m.snap.l5 && m.snap.l5.home && m.snap.l5.away && m.date_unix);

    // rank-0 under the CURRENT production signals.
    const rank0 = withL5.filter(m => {
      const hT = m.snap.l5.home.t || 0, aT = m.snap.l5.away.t || 0, aF = m.snap.l5.away.f || 0;
      return !(hT >= 1.6 && aT >= 1.4) && !(aF >= 0.8);
    }).sort((a, b) => (a.date_unix || 0) - (b.date_unix || 0));

    const F = (m) => ({
      hF: m.snap.l5.home.f != null ? m.snap.l5.home.f : null,
      hT: m.snap.l5.home.t != null ? m.snap.l5.home.t : null,
      h_scored_fh: m.snap.home ? m.snap.home.scored_fh : null,
    });

    const cut = Math.floor(rank0.length * 0.7);
    const train = rank0.slice(0, cut);
    const test = rank0.slice(cut);

    const rate = (rows, key) => rows.length ? rows.filter(m => m[key]).length / rows.length : 0;
    const tBase15 = rate(train, "hit_15"), tBase25 = rate(train, "hit_25");
    const eBase15 = rate(test, "hit_15"), eBase25 = rate(test, "hit_25");

    const evalOn = (rows, pred, base15, base25) => {
      const sub = rows.filter(m => { const f = F(m); return pred(f); });
      const n = sub.length;
      if (!n) return { n: 0 };
      const h15 = sub.filter(m => m.hit_15).length, h25 = sub.filter(m => m.hit_25).length;
      return {
        n,
        hit15: +(h15 / n * 100).toFixed(1), lift15: base15 ? +(h15 / n / base15).toFixed(2) : 0,
        hit25: +(h25 / n * 100).toFixed(1), lift25: base25 ? +(h25 / n / base25).toFixed(2) : 0,
      };
    };
    const ge = (v, t) => v != null && v >= t;

    // Pre-specified candidates (the home-attack lead, from L5 and season angles).
    const defs = [
      { label: "hF>=1.2 (home L5 scored)", pred: f => ge(f.hF, 1.2) },
      { label: "hF>=1.0 (home L5 scored)", pred: f => ge(f.hF, 1.0) },
      { label: "hT>=2.0 (home L5 total)", pred: f => ge(f.hT, 2.0) },
      { label: "home scored_fh>=0.9 (season)", pred: f => ge(f.h_scored_fh, 0.9) },
      { label: "hF>=1.0 OR scored_fh>=0.9 (home dominance)", pred: f => ge(f.hF, 1.0) || ge(f.h_scored_fh, 0.9) },
    ];
    const verdictFor = (te) => {
      if (!te || te.n < 12) return "INCONCLUSIVE: test n too small";
      if (te.lift15 >= 1.2) return "HOLDS: test FH>1.5 lift >= 1.2 out of sample";
      if (te.lift15 >= 1.05) return "MARGINAL: small out-of-sample edge";
      return "FAILS: no out-of-sample edge (like o15HT)";
    };
    const candidates = defs.map(d => {
      const tr = evalOn(train, d.pred, tBase15, tBase25);
      const te = evalOn(test, d.pred, eBase15, eBase25);
      return { label: d.label, train: tr, test: te, verdict: verdictFor(te) };
    });

    // Overfit check: let TRAIN pick the best hF threshold, then judge on TEST.
    let trainSelected = null;
    let best = null;
    for (const t of [0.8, 0.9, 1.0, 1.1, 1.2, 1.3]) {
      const tr = evalOn(train, f => ge(f.hF, t), tBase15, tBase25);
      if (tr.n >= 20 && (!best || tr.lift15 > best.tr.lift15)) best = { t, tr };
    }
    if (best) {
      const te = evalOn(test, f => ge(f.hF, best.t), eBase15, eBase25);
      trainSelected = { chosen: "hF>=" + best.t, train: best.tr, test: te, verdict: verdictFor(te) };
    }

    res.json({
      ok: true,
      rank0Total: rank0.length,
      split: {
        trainN: train.length, testN: test.length,
        trainBase15: +(tBase15 * 100).toFixed(1), trainBase25: +(tBase25 * 100).toFixed(1),
        testBase15: +(eBase15 * 100).toFixed(1), testBase25: +(eBase25 * 100).toFixed(1),
      },
      candidates,
      trainSelected,
      note: "Train = older 70% of rank-0 games, Test = newer 30%. Trust a candidate ONLY if its test.lift15 holds >= ~1.2 at test.n >= ~12. lift is vs the same split's rank-0 base rate. Small test n means even a HOLDS verdict is tentative until more data accrues — but a FAILS verdict is decisive (that's how o15HT was caught).",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Tests "Reason 2": does a team-quality MISMATCH (one team much better than the
// other) predict FH goals? Two parts: (1) DIRECTION — quartile buckets on the
// full live cohort, to see whether a bigger gap means MORE goals or a controlled
// 1-0; (2) HOLDOUT — chronological 70/30 split of the rank-0 cohort (where
// mismatches hide, since A+B need mutual activity), train picks the threshold,
// test judges it. Same discipline as /rank0-holdout. Symmetric gap, both directions.
app.get("/mismatch-holdout", async (req, res) => {
  res.set("Cache-Control", "no-store");
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("hit_25, hit_15, date_unix, snap")
        .not("hit_25", "is", null)
        .not("date_unix", "is", null)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }
    const live = all.filter(m => m.snap && m.snap.home && m.snap.away && m.date_unix);

    const F = (m) => {
      const h = m.snap.home || {}, a = m.snap.away || {}, l5 = m.snap.l5;
      const num = (v) => (v != null && !isNaN(Number(v))) ? Number(v) : null;
      const hSF = num(h.scored_fh), hCF = num(h.conced_fh), aSF = num(a.scored_fh), aCF = num(a.conced_fh);
      const seasonGap = (hSF != null && hCF != null && aSF != null && aCF != null)
        ? Math.abs((hSF - hCF) - (aSF - aCF)) : null;
      const scoredGap = (hSF != null && aSF != null) ? Math.abs(hSF - aSF) : null;
      const t1Gap = (num(h.t1_pct) != null && num(a.t1_pct) != null) ? Math.abs(num(h.t1_pct) - num(a.t1_pct)) : null;
      let l5Gap = null;
      if (l5 && l5.home && l5.away) l5Gap = Math.abs(((l5.home.f || 0) - (l5.home.a || 0)) - ((l5.away.f || 0) - (l5.away.a || 0)));
      return { seasonGap, scoredGap, t1Gap, l5Gap };
    };

    // ── Part 1: DIRECTION on full cohort (quartile buckets + correlation) ──
    const pearson = (xs, ys) => {
      const n = xs.length;
      if (n < 3) return null;
      let sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
      for (let i = 0; i < n; i++) { sx += xs[i]; sy += ys[i]; sxx += xs[i] * xs[i]; syy += ys[i] * ys[i]; sxy += xs[i] * ys[i]; }
      const cov = sxy - sx * sy / n, vx = sxx - sx * sx / n, vy = syy - sy * sy / n;
      if (vx <= 0 || vy <= 0) return null;
      return +(cov / Math.sqrt(vx * vy)).toFixed(3);
    };
    const direction = (featKey) => {
      const sub = live.filter(m => F(m)[featKey] != null);
      if (sub.length < 40) return { feature: featKey, n: sub.length, note: "too few" };
      const xs = sub.map(m => F(m)[featKey]);
      const sorted = [...xs].sort((a, b) => a - b);
      const q = (p) => sorted[Math.max(0, Math.floor(p * (sorted.length - 1)))];
      const cuts = [q(0.25), q(0.5), q(0.75)];
      const idx = (x) => x <= cuts[0] ? 0 : x <= cuts[1] ? 1 : x <= cuts[2] ? 2 : 3;
      const agg = [0, 1, 2, 3].map(() => ({ n: 0, h15: 0, h25: 0, xs: 0 }));
      for (const m of sub) { const b = idx(F(m)[featKey]); agg[b].n++; agg[b].h15 += m.hit_15 ? 1 : 0; agg[b].h25 += m.hit_25 ? 1 : 0; agg[b].xs += F(m)[featKey]; }
      const labels = ["Q1 (closest match)", "Q2", "Q3", "Q4 (biggest mismatch)"];
      return {
        feature: featKey, n: sub.length,
        corr15: pearson(xs, sub.map(m => m.hit_15 ? 1 : 0)),
        corr25: pearson(xs, sub.map(m => m.hit_25 ? 1 : 0)),
        quartiles: agg.map((g, i) => ({
          bucket: labels[i], n: g.n, avgGap: g.n ? +(g.xs / g.n).toFixed(2) : 0,
          over15Rate: g.n ? +(g.h15 / g.n * 100).toFixed(1) : 0,
          over25Rate: g.n ? +(g.h25 / g.n * 100).toFixed(1) : 0,
        })),
      };
    };

    // ── Part 2: HOLDOUT on rank-0 cohort ──
    const rank0 = live.filter(m => m.snap.l5 && m.snap.l5.home && m.snap.l5.away).filter(m => {
      const hT = m.snap.l5.home.t || 0, aT = m.snap.l5.away.t || 0, aF = m.snap.l5.away.f || 0;
      return !(hT >= 1.6 && aT >= 1.4) && !(aF >= 0.8);
    }).sort((a, b) => (a.date_unix || 0) - (b.date_unix || 0));
    const cut = Math.floor(rank0.length * 0.7);
    const train = rank0.slice(0, cut), test = rank0.slice(cut);
    const rate = (rows, key) => rows.length ? rows.filter(m => m[key]).length / rows.length : 0;
    const tB15 = rate(train, "hit_15"), tB25 = rate(train, "hit_25"), eB15 = rate(test, "hit_15"), eB25 = rate(test, "hit_25");
    const evalOn = (rows, pred, b15, b25) => {
      const sub = rows.filter(m => { try { return pred(F(m)); } catch (_) { return false; } });
      const n = sub.length;
      if (!n) return { n: 0 };
      const h15 = sub.filter(m => m.hit_15).length, h25 = sub.filter(m => m.hit_25).length;
      return { n, hit15: +(h15 / n * 100).toFixed(1), lift15: b15 ? +(h15 / n / b15).toFixed(2) : 0, hit25: +(h25 / n * 100).toFixed(1), lift25: b25 ? +(h25 / n / b25).toFixed(2) : 0 };
    };
    const ge = (v, t) => v != null && v >= t;
    const verdictFor = (te) => {
      if (!te || te.n < 12) return "INCONCLUSIVE: test n too small";
      if (te.lift15 >= 1.2) return "HOLDS: test FH>1.5 lift >= 1.2 out of sample";
      if (te.lift15 >= 1.05) return "MARGINAL: small out-of-sample edge";
      return "FAILS: no out-of-sample edge";
    };
    const defs = [
      { label: "seasonGap>=0.5 (net FH rating gap)", pred: f => ge(f.seasonGap, 0.5) },
      { label: "seasonGap>=0.7", pred: f => ge(f.seasonGap, 0.7) },
      { label: "seasonGap>=1.0", pred: f => ge(f.seasonGap, 1.0) },
      { label: "l5Gap>=0.9 (net FH form gap)", pred: f => ge(f.l5Gap, 0.9) },
      { label: "scoredGap>=0.5 (FH scored gap)", pred: f => ge(f.scoredGap, 0.5) },
      { label: "t1Gap>=15 (FH-scoring %% gap)", pred: f => ge(f.t1Gap, 15) },
    ];
    const candidates = defs.map(d => ({
      label: d.label,
      train: evalOn(train, d.pred, tB15, tB25),
      test: evalOn(test, d.pred, eB15, eB25),
      verdict: verdictFor(evalOn(test, d.pred, eB15, eB25)),
    }));

    res.json({
      ok: true,
      cohort: { liveRows: live.length, rank0Total: rank0.length },
      directionFullCohort: {
        seasonGap: direction("seasonGap"),
        l5Gap: direction("l5Gap"),
      },
      rank0Holdout: {
        split: { trainN: train.length, testN: test.length, trainBase15: +(tB15 * 100).toFixed(1), testBase15: +(eB15 * 100).toFixed(1), trainBase25: +(tB25 * 100).toFixed(1), testBase25: +(eB25 * 100).toFixed(1) },
        candidates,
      },
      note: "Part 1 (direction): if Q4 (biggest mismatch) over15Rate is ABOVE Q1, big gaps mean more goals (reason 2 real); if FLAT or BELOW, a mismatch tends to a controlled low-scoring game. corr near 0 = no relationship. Part 2 (holdout): trust a candidate only if test.lift15 >= ~1.2 at test.n >= 12. A FAILS verdict here means reason 2 doesn't convert to FH goals in our data.",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// VALIDATION for wiring Signal C (team mismatch). Computes the full 3-signal
// model (A+B+C, C = season net-FH-rating gap >= 0.5) on the whole clean cohort:
//  (1) the recalibrated 8-combo probability table (the numbers we'd wire),
//  (2) a chronological train/test split of that table to confirm it's stable
//      out of sample (not just rank-0),
//  (3) C's marginal lift INSIDE each existing A+B combo — tells us whether C is
//      a global signal or only a rank-0 patch.
const SIGNAL_C_GAP = 0.5;
app.get("/signalc-validate", async (req, res) => {
  res.set("Cache-Control", "no-store");
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("hit_25, hit_15, date_unix, snap")
        .not("hit_25", "is", null)
        .not("date_unix", "is", null)
        .not("snap", "is", null)
        .not("snap->>fetchedAt", "eq", "historical-import")
        .not("snap->>fetchedAt", "eq", "backfill")
        .range(off, off + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < PAGE) break;
    }

    const seasonGap = (m) => {
      const h = m.snap.home || {}, a = m.snap.away || {};
      const num = (v) => (v != null && !isNaN(Number(v))) ? Number(v) : null;
      const hSF = num(h.scored_fh), hCF = num(h.conced_fh), aSF = num(a.scored_fh), aCF = num(a.conced_fh);
      return (hSF != null && hCF != null && aSF != null && aCF != null) ? Math.abs((hSF - hCF) - (aSF - aCF)) : null;
    };

    // Need l5 (for A,B) AND a computable gap (for C).
    const rows = all.filter(m => m.snap && m.snap.l5 && m.snap.l5.home && m.snap.l5.away && seasonGap(m) != null)
      .map(m => {
        const l5 = m.snap.l5;
        const A = (l5.home.t || 0) >= 1.6 && (l5.away.t || 0) >= 1.4;
        const B = (l5.away.f || 0) >= 0.8;
        const C = seasonGap(m) >= SIGNAL_C_GAP;
        return {
          date: m.date_unix || 0, hit15: !!m.hit_15, hit25: !!m.hit_25,
          A, B, C, combo2: (A ? 1 : 0) + "" + (B ? 1 : 0),
          combo3: (A ? 1 : 0) + "" + (B ? 1 : 0) + "" + (C ? 1 : 0),
        };
      }).sort((a, b) => a.date - b.date);

    const summarize = (rs, keyFn) => {
      const g = {};
      for (const r of rs) { const k = keyFn(r); (g[k] = g[k] || { n: 0, h15: 0, h25: 0 }); g[k].n++; g[k].h15 += r.hit15 ? 1 : 0; g[k].h25 += r.hit25 ? 1 : 0; }
      const out = {};
      for (const k of Object.keys(g).sort()) {
        const x = g[k];
        out[k] = { n: x.n, prob15: +(x.h15 / x.n * 100).toFixed(1), prob25: +(x.h25 / x.n * 100).toFixed(1) };
      }
      return out;
    };

    // (1) Full-cohort recalibrated 3-signal combo table (key = A B C).
    const comboTable3 = summarize(rows, r => r.combo3);
    const comboTable2_current = summarize(rows, r => r.combo2);

    // (2) Holdout stability of the 3-combo table.
    const cut = Math.floor(rows.length * 0.7);
    const trainT = summarize(rows.slice(0, cut), r => r.combo3);
    const testT = summarize(rows.slice(cut), r => r.combo3);
    const stability = {};
    for (const k of Object.keys(comboTable3)) {
      const tr = trainT[k], te = testT[k];
      stability[k] = {
        train: tr || { n: 0 }, test: te || { n: 0 },
        prob15_drift: (tr && te) ? +(te.prob15 - tr.prob15).toFixed(1) : null,
        prob25_drift: (tr && te) ? +(te.prob25 - tr.prob25).toFixed(1) : null,
      };
    }

    // (3) C's marginal effect INSIDE each existing A+B combo.
    const withinCombo = {};
    for (const c2 of ["00", "01", "10", "11"]) {
      const base = rows.filter(r => r.combo2 === c2);
      const withC = base.filter(r => r.C), noC = base.filter(r => !r.C);
      const stat = (rs) => rs.length ? { n: rs.length, prob15: +(rs.filter(r => r.hit15).length / rs.length * 100).toFixed(1), prob25: +(rs.filter(r => r.hit25).length / rs.length * 100).toFixed(1) } : { n: 0 };
      const sWith = stat(withC), sNo = stat(noC);
      withinCombo[c2] = {
        all: stat(base), withC: sWith, withoutC: sNo,
        lift15: (sWith.n && sNo.n && sNo.prob15) ? +(sWith.prob15 / sNo.prob15).toFixed(2) : null,
        lift25: (sWith.n && sNo.n && sNo.prob25) ? +(sWith.prob25 / sNo.prob25).toFixed(2) : null,
      };
    }

    // Overall + headline rates for context.
    const n = rows.length;
    const overall = { n, prob15: +(rows.filter(r => r.hit15).length / n * 100).toFixed(1), prob25: +(rows.filter(r => r.hit25).length / n * 100).toFixed(1) };
    const cFires = rows.filter(r => r.C).length;

    res.json({
      ok: true,
      signalC: "C = |(home.scored_fh - home.conced_fh) - (away.scored_fh - away.conced_fh)| >= " + SIGNAL_C_GAP + " (season, frozen pre-game)",
      cohortN: n,
      overall,
      cFireRate: +(cFires / n * 100).toFixed(1),
      comboTable3_recalibrated: comboTable3,
      comboTable2_current,
      holdoutStability: stability,
      cMarginalWithinCombo: withinCombo,
      note: "comboTable3 keys are A B C bits (e.g. '011' = B+C, no A). This is the table we'd wire. holdoutStability: small prob drift (train vs test) at decent n = trustworthy bucket. cMarginalWithinCombo is the key test: if withC.prob15 > withoutC.prob15 across ALL four combos, C adds value everywhere (true global signal); if it only helps in '00' (rank-0), C is a rank-0 rescue and should be wired narrowly. Buckets with n<30 are noisy — judge accordingly.",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Diagnostic: how often A and B fire on CURRENT upcoming matches (live signal
// code, not the persisted snapshots that /signal-backtest reads). Use this to
// see whether either signal is dormant or saturated for the current league set.
app.get("/signal-fires", async (req, res) => {
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const { preds } = await computePreds(tzOffset);
    const upcoming = preds.filter(p => p.status !== "complete" && !p.matchResult && p.snap);
    let aFires = 0, bFires = 0, abFires = 0;
    const abExamples = [];
    for (const p of upcoming) {
      const aMet = !!(p.signals && p.signals.A && p.signals.A.met);
      const bMet = !!(p.signals && p.signals.B && p.signals.B.met);
      if (aMet) aFires++;
      if (bMet) bFires++;
      if (aMet && bMet) abFires++;
      if (aMet && bMet && abExamples.length < 10) {
        abExamples.push({
          match: p.home + " vs " + p.away, league: p.league,
          aValue: p.signals.A && p.signals.A.value,
          bValue: p.signals.B && p.signals.B.value,
        });
      }
    }
    const topByCi = upcoming
      .filter(p => typeof p.ci === "number")
      .sort((a, b) => (b.ci || 0) - (a.ci || 0))
      .slice(0, 15)
      .map(p => ({
        match: p.home + " vs " + p.away, league: p.league,
        ci: p.ci,
        aMet: !!(p.signals && p.signals.A && p.signals.A.met),
        bMet: !!(p.signals && p.signals.B && p.signals.B.met),
      }));
    res.json({
      ok: true,
      upcomingTotal: upcoming.length,
      aFires, bFires, abFires,
      aFireRate: upcoming.length ? +(aFires / upcoming.length * 100).toFixed(1) : 0,
      bFireRate: upcoming.length ? +(bFires / upcoming.length * 100).toFixed(1) : 0,
      abFireRate: upcoming.length ? +(abFires / upcoming.length * 100).toFixed(1) : 0,
      abExamples,
      topByCi,
      note: "Live A (Mutual Instability) + B (Away Scoring) signals on upcoming matches. abFires = rank-2 (both fire). If a signal's fire rate is ~0% or ~100%, its threshold may be mis-set for the current league set.",
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

// ─── CALIBRATION TESTING ───────────────────────────────────────────────────
// Validates model structure and probability calibration on live FH>2.5 matches
app.get("/calibration-test", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const { data: allMatches, error } = await supabase
      .from("match_results")
      .select("match_id, home_name, away_name, ht_home, ht_away, snap, rank, prob25")
      .not("snap", "is", null)
      .not("snap->>fetchedAt", "eq", "historical-import")
      .not("snap->>fetchedAt", "eq", "backfill");
    if (error) throw error;

    // Filter to viable matches (snap + L5 data), but keep ALL outcomes
    const viable = (allMatches || []).filter(m => m.snap && m.snap.l5);

    // Mark outcomes
    const fh25Total = viable.filter(m => (parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10)) > 2.5).length;
    const fh15Total = viable.filter(m => (parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10)) > 1.5).length;

    // 1. Hit rate by rank (CORRECT: evaluate ALL matches grouped by rank)
    const byRank = { 0: { n: 0, hits25: 0, hits15: 0 }, 1: { n: 0, hits25: 0, hits15: 0 }, 2: { n: 0, hits25: 0, hits15: 0 } };
    for (const m of viable) {
      const r = m.rank || 0;
      const fh = parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10);
      if (byRank[r]) {
        byRank[r].n++;
        if (fh > 2.5) byRank[r].hits25++;
        if (fh > 1.5) byRank[r].hits15++;
      }
    }
    for (const r of Object.keys(byRank)) {
      const b = byRank[r];
      b.hit_rate_fh25 = b.n ? +(b.hits25 / b.n * 100).toFixed(1) : 0;
      b.hit_rate_fh15 = b.n ? +(b.hits15 / b.n * 100).toFixed(1) : 0;
    }

    // 2. Hit rate by prob25 bucket (CORRECT: ALL matches, not just FH>2.5)
    const byProb = {};
    const buckets = ["0-10", "10-20", "20-30", "30-40", "40-50", "50+"];
    for (const bucket of buckets) byProb[bucket] = { n: 0, hits25: 0, hits15: 0 };
    for (const m of viable) {
      const pRaw = Number(m.prob25 || 0);
      const p = pRaw > 0 && pRaw < 1 ? pRaw * 100 : pRaw;  // normalize decimal/percentage
      const fh = parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10);
      let bucket;
      if (p < 10) bucket = "0-10";
      else if (p < 20) bucket = "10-20";
      else if (p < 30) bucket = "20-30";
      else if (p < 40) bucket = "30-40";
      else if (p < 50) bucket = "40-50";
      else bucket = "50+";
      byProb[bucket].n++;
      if (fh > 2.5) byProb[bucket].hits25++;
      if (fh > 1.5) byProb[bucket].hits15++;
    }
    for (const bucket of Object.keys(byProb)) {
      const b = byProb[bucket];
      b.hit_rate_fh25 = b.n ? +(b.hits25 / b.n * 100).toFixed(1) : 0;
      b.hit_rate_fh15 = b.n ? +(b.hits15 / b.n * 100).toFixed(1) : 0;
    }

    // 3. Conceding escalator quality (CORRECT: ALL matches)
    const withEscalator = viable.filter(m => {
      const hc = (m.snap.l5?.home?.a || 0);
      const ac = (m.snap.l5?.away?.a || 0);
      return (hc + ac) >= 1.6;
    });
    const withoutEscalator = viable.filter(m => {
      const hc = (m.snap.l5?.home?.a || 0);
      const ac = (m.snap.l5?.away?.a || 0);
      return (hc + ac) < 1.6;
    });
    const escalatorMetrics = {
      with: {
        n: withEscalator.length,
        hits25: withEscalator.filter(m => (parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10)) > 2.5).length,
        hit_rate_fh25: 0,
      },
      without: {
        n: withoutEscalator.length,
        hits25: withoutEscalator.filter(m => (parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10)) > 2.5).length,
        hit_rate_fh25: 0,
      },
    };
    escalatorMetrics.with.hit_rate_fh25 = escalatorMetrics.with.n ? +(escalatorMetrics.with.hits25 / escalatorMetrics.with.n * 100).toFixed(1) : 0;
    escalatorMetrics.without.hit_rate_fh25 = escalatorMetrics.without.n ? +(escalatorMetrics.without.hits25 / escalatorMetrics.without.n * 100).toFixed(1) : 0;

    // 4. Passive-passive filter precision (on viable matches)
    const bothPassive = viable.filter(m => {
      const ht = (m.snap?.l5?.home?.t || 0);
      const at = (m.snap?.l5?.away?.t || 0);
      return ht < 1.0 && at < 1.0;
    });
    const viableAndNotPassive = viable.filter(m => {
      const ht = (m.snap?.l5?.home?.t || 0);
      const at = (m.snap?.l5?.away?.t || 0);
      return !(ht < 1.0 && at < 1.0);
    });
    const bothPassiveFh25 = bothPassive.filter(m => (parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10)) > 2.5).length;
    const filterMetrics = {
      total_viable: viable.length,
      both_passive_count: bothPassive.length,
      both_passive_pct: viable.length ? +(bothPassive.length / viable.length * 100).toFixed(1) : 0,
      both_passive_fh25_hits: bothPassiveFh25,
      passed_filter_count: viableAndNotPassive.length,
      fh25_in_passed: viableAndNotPassive.filter(m => (parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10)) > 2.5).length,
    };

    // 5. Distribution stability (sample sizes, time range)
    const now = Date.now();
    const oldestMatch = viable.reduce((min, m) => Math.min(min, m.date_unix || now / 1000), now / 1000);
    const daysOfData = (now / 1000 - oldestMatch) / (24 * 3600);

    res.json({
      ok: true,
      summary: {
        total_matches: (allMatches || []).length,
        viable_matches: viable.length,
        fh25_total: fh25Total,
        fh15_total: fh15Total,
        days_of_data: +(daysOfData || 0).toFixed(1),
      },
      by_rank: byRank,
      by_prob25_bucket: byProb,
      conceding_escalator: escalatorMetrics,
      passive_passive_filter: filterMetrics,
      note: "Calibration validation metrics. Hit rate by rank tests structure. Hit rate by prob25 tests calibration accuracy. Escalator metrics check for false positives. Filter metrics validate hard filter. Distribution drift checks robustness over time.",
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── SIGNAL CONTRIBUTION TEST ──────────────────────────────────────────────
// List all matches from the last N days — useful when team-name search fails
app.get("/list-recent-matches", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  const days = Math.min(7, Math.max(1, parseInt(req.query.days || "2", 10)));
  const cutoffSec = Math.floor(Date.now() / 1000) - days * 86400;
  try {
    const { data, error } = await supabase
      .from("match_results")
      .select("match_id, home_name, away_name, date_unix, ht_home, ht_away, hit_25, hit_15, rank, signals, snap, league_name")
      .gte("date_unix", cutoffSec)
      .order("date_unix", { ascending: false })
      .limit(200);
    if (error) throw error;
    const rows = (data || []).map(m => ({
      match_id: m.match_id,
      date: m.date_unix ? new Date(m.date_unix * 1000).toISOString().slice(0, 16).replace("T", " ") : null,
      teams: `${m.home_name} vs ${m.away_name}`,
      league: m.league_name,
      ht: m.ht_home !== null ? `${m.ht_home}-${m.ht_away}` : "pending",
      fh_total: m.ht_home !== null ? (m.ht_home + m.ht_away) : null,
      hit_15: m.hit_15,
      hit_25: m.hit_25,
      rank: m.rank,
      sigA: !!(m.signals && m.signals.A && m.signals.A.met),
      sigB: !!(m.signals && m.signals.B && m.signals.B.met),
      has_l5: !!(m.snap && m.snap.l5),
    }));
    res.json({ ok: true, count: rows.length, days, matches: rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Inspect a single match by team names or match_id — shows snap, signals, outcome
app.get("/inspect-match", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  const q = (req.query.q || "").trim();
  if (!q) return res.status(400).json({ ok: false, error: "pass ?q=TeamName or ?q=match_id" });
  try {
    let query = supabase
      .from("match_results")
      .select("match_id, home_name, away_name, date_unix, ht_home, ht_away, ft_home, ft_away, hit_25, hit_15, rank, prob25, prob15, ci, signals, snap, league_name");
    if (/^\d+$/.test(q)) {
      query = query.eq("match_id", parseInt(q, 10));
    } else {
      query = query.or(`home_name.ilike.%${q}%,away_name.ilike.%${q}%`);
    }
    const { data, error } = await query.order("date_unix", { ascending: false }).limit(20);
    if (error) throw error;
    if (!data || data.length === 0) return res.json({ ok: true, matches: [], note: "no matches found" });

    const matches = data.map(m => {
      const sigA = m.signals && m.signals.A && m.signals.A.met;
      const sigB = m.signals && m.signals.B && m.signals.B.met;
      const l5h = m.snap && m.snap.l5 && m.snap.l5.home;
      const l5a = m.snap && m.snap.l5 && m.snap.l5.away;
      // Recompute what the signals SHOULD have fired based on stored L5
      const shouldA = l5h && l5a && (l5h.t || 0) >= 1.6 && (l5a.t || 0) >= 1.4;
      const shouldB = l5a && (l5a.f || 0) >= 0.8;
      return {
        match_id: m.match_id,
        date: m.date_unix ? new Date(m.date_unix * 1000).toISOString() : null,
        league: m.league_name,
        teams: `${m.home_name} vs ${m.away_name}`,
        ht_score: `${m.ht_home || 0}-${m.ht_away || 0}`,
        ft_score: `${m.ft_home || 0}-${m.ft_away || 0}`,
        fh_total: (m.ht_home || 0) + (m.ht_away || 0),
        hit_15: m.hit_15,
        hit_25: m.hit_25,
        stored_rank: m.rank,
        stored_prob25: m.prob25,
        stored_prob15: m.prob15,
        stored_ci: m.ci,
        stored_signals: {
          A: { met: !!sigA, value: m.signals?.A?.value, threshold: m.signals?.A?.threshold },
          B: { met: !!sigB, value: m.signals?.B?.value, threshold: m.signals?.B?.threshold },
        },
        l5_data: {
          home: l5h ? { scored: l5h.f, conceded: l5h.a, total: l5h.t } : null,
          away: l5a ? { scored: l5a.f, conceded: l5a.a, total: l5a.t } : null,
        },
        recomputed_signals: {
          A_should_fire: shouldA,
          B_should_fire: shouldB,
          A_matches_stored: shouldA === !!sigA,
          B_matches_stored: shouldB === !!sigB,
        },
        fetched_at: m.snap?.fetchedAt,
      };
    });

    res.json({ ok: true, count: matches.length, matches });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Deep debug: pick a fixture, trace through the whole signal computation
app.get("/trace-fixture", async (req, res) => {
  const cid = req.query.cid ? parseInt(req.query.cid, 10) : null;
  if (!cid) return res.status(400).json({ ok: false, error: "pass ?cid=<competition_id>" });
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const dates = getDates(tzOffset);
    // Find a fixture in the requested league
    let fix = null;
    for (const d of dates) {
      const raw = await fetchFixtures(d);
      const found = (raw.data || []).find(f => f.competition_id === cid);
      if (found) { fix = found; break; }
    }
    if (!fix) return res.json({ ok: true, found: false, note: "no upcoming fixtures for that competition_id" });

    const homeId = fix.homeID || fix.home_id;
    const awayId = fix.awayID || fix.away_id;

    // Fetch team stats + matches for this league
    const teamRes = await fetchTeamStats(cid);
    const matchRes = await fetchLeagueMatches(cid);
    const teamMap = {};
    for (const t of (teamRes.data || [])) teamMap[t.id] = t;

    const homeTeam = teamMap[homeId];
    const awayTeam = teamMap[awayId];

    // Try building L5
    const mergedCache = (tid) => (SERVER_MATCH_CACHE[tid] || []);
    const hLast5 = buildLast5(homeId, { [homeId]: mergedCache(homeId), [awayId]: mergedCache(awayId) });
    const aLast5 = buildLast5(awayId, { [homeId]: mergedCache(homeId), [awayId]: mergedCache(awayId) });

    res.json({
      ok: true,
      found: true,
      fixture: {
        id: fix.id, status: fix.status,
        home_name: fix.home_name, away_name: fix.away_name,
        homeID_raw: fix.homeID, home_id_raw: fix.home_id,
        awayID_raw: fix.awayID, away_id_raw: fix.away_id,
        homeID_type: typeof (fix.homeID || fix.home_id),
        resolved_homeId: homeId, resolved_awayId: awayId,
      },
      teamMap: {
        total_teams: Object.keys(teamMap).length,
        sample_team_ids: Object.keys(teamMap).slice(0, 5),
        sample_team_id_types: Object.keys(teamMap).slice(0, 3).map(k => typeof k),
        home_lookup_hit: !!homeTeam,
        away_lookup_hit: !!awayTeam,
        home_team_name: homeTeam ? homeTeam.name : null,
        away_team_name: awayTeam ? awayTeam.name : null,
      },
      l5: {
        home_l5_count: hLast5.length,
        away_l5_count: aLast5.length,
        home_l5_first: hLast5[0] || null,
        away_l5_first: aLast5[0] || null,
      },
      league_matches_cached: !!LEAGUE_MATCHES_CACHE[cid],
      league_matches_count: LEAGUE_MATCHES_CACHE[cid] ? (LEAGUE_MATCHES_CACHE[cid].data.data || []).length : 0,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// List distinct leagues/competition IDs from the next 5 days of fixtures
app.get("/current-leagues", async (req, res) => {
  try {
    const tzOffset = parseInt(req.query.tz || "0", 10);
    const dates = getDates(tzOffset);
    const seen = {};
    for (const d of dates) {
      const raw = await fetchFixtures(d);
      const fixtures = raw.data || [];
      for (const f of fixtures) {
        const cid = f.competition_id;
        if (!cid) continue;
        if (!seen[cid]) {
          seen[cid] = {
            competition_id: cid,
            league_name: LEAGUE_NAMES[cid] || "(unknown)",
            fixture_count: 0,
            sample_match: f.home_name + " vs " + f.away_name,
          };
        }
        seen[cid].fixture_count++;
      }
    }
    const leagues = Object.values(seen).sort((a, b) => b.fixture_count - a.fixture_count);
    res.json({ ok: true, dates, league_count: leagues.length, leagues });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Debug what FootyStats actually returns for a given season_id
app.get("/debug-raw-api", async (req, res) => {
  const sid = req.query.sid;
  if (!sid) return res.status(400).json({ ok: false, error: "pass ?sid=<season_id>" });
  try {
    const matches = await safeFetch(BASE + "/league-matches?season_id=" + sid + "&max_per_page=50&page=1&sort=date_unix&order=desc&key=" + KEY);
    const teams = await safeFetch(BASE + "/league-teams?season_id=" + sid + "&include=stats&key=" + KEY);
    const all = (matches && matches.data) ? matches.data : [];
    // Prefer an upcoming/incomplete match — its *_potential/_prematch/odds fields are
    // the live pre-game predictors we'd actually capture.
    const sampleMatch = all.find(m => m.status === "incomplete") || all[0] || null;
    const sampleTeam = teams && teams.data && teams.data[0] ? teams.data[0] : null;

    // Pull the pre-match first-half predictor VALUES FootyStats already gives us.
    const preMatchFH = sampleMatch ? {
      o05HT_potential:     sampleMatch.o05HT_potential,
      o15HT_potential:     sampleMatch.o15HT_potential,
      btts_fhg_potential:  sampleMatch.btts_fhg_potential,
      team_a_xg_prematch:  sampleMatch.team_a_xg_prematch,
      team_b_xg_prematch:  sampleMatch.team_b_xg_prematch,
      total_xg_prematch:   sampleMatch.total_xg_prematch,
      pre_match_home_ppg:  sampleMatch.pre_match_home_ppg,
      pre_match_away_ppg:  sampleMatch.pre_match_away_ppg,
      odds_1st_half_over05: sampleMatch.odds_1st_half_over05,
      odds_1st_half_over15: sampleMatch.odds_1st_half_over15,
      odds_1st_half_over25: sampleMatch.odds_1st_half_over25,
      avg_potential:       sampleMatch.avg_potential,
    } : null;

    // Surface every team-stats key that looks HT/first-half/form/last-x related,
    // with values, so we can see what recent-form data the API actually carries.
    const st = sampleTeam && sampleTeam.stats ? sampleTeam.stats : null;
    const formish = st ? Object.fromEntries(
      Object.keys(st)
        .filter(k => /ht|fh|first_half|last|form|recent|_5|_6|_10/i.test(k))
        .map(k => [k, st[k]])
    ) : {};

    // VERDICT: report whether FootyStats' match-level pre-match FH predictors are
    // populated for this league. (Native last-5 HT team stats do NOT exist in the
    // team API — no scoredAVGHT_*_5 fields — so these match-level potentials are the
    // only pre-game FH signal available for thin-coverage leagues like USL2/WPSL.)
    let verdict = null;
    if (sampleMatch) {
      const o15 = sampleMatch.o15HT_potential;
      const o05 = sampleMatch.o05HT_potential;
      const txg = sampleMatch.total_xg_prematch;
      const have = (v) => v != null && v !== -1 && v !== "" && !(typeof v === "number" && isNaN(v));
      const usable = have(o15) || have(o05) || have(txg);
      verdict = {
        match: sampleMatch.home_name + " vs " + sampleMatch.away_name,
        o15HT_potential: o15, o05HT_potential: o05, total_xg_prematch: txg,
        prematch_available: usable,
        conclusion: usable
          ? "OK: match-level pre-match FH predictors populated — usable as fallback signal"
          : "FAIL: pre-match predictors empty for this league too",
      };
    }

    // RECENT-FORM RECON: to build last-5 versions of richer features we need those
    // stats to exist PER COMPLETED MATCH (not just as season aggregates). Probe a
    // completed match for actual (post-game) xG / shots / corners / possession /
    // attacks, with values, so we know what's aggregatable over a team's last 5.
    const completed = all.find(m => m.status === "complete") || null;
    let perMatchRecon = null;
    if (completed) {
      const candidateFields = [
        "team_a_xg", "team_b_xg", "total_xg",
        "team_a_xg_prematch", "team_b_xg_prematch",
        "team_a_shots", "team_b_shots",
        "team_a_shotsOnTarget", "team_b_shotsOnTarget",
        "team_a_corners", "team_b_corners",
        "team_a_possession", "team_b_possession",
        "team_a_dangerous_attacks", "team_b_dangerous_attacks",
        "team_a_fh_shots", "team_b_fh_shots",
        "ht_goals_team_a", "ht_goals_team_b",
        "homeGoalCount", "awayGoalCount",
      ];
      const present = {}, missing = [];
      for (const f of candidateFields) {
        const v = completed[f];
        if (v === undefined) missing.push(f);
        else present[f] = v;
      }
      perMatchRecon = {
        match: completed.home_name + " vs " + completed.away_name,
        status: completed.status,
        present_fields: present,            // exist on the match record (with values)
        missing_fields: missing,            // not on the record at all
        all_match_keys: Object.keys(completed).sort(),
        note: "present_fields with real (non -1) values can be aggregated into last-5 form. -1 / missing means FootyStats doesn't carry it per-match for this league, so a recent-form version is NOT buildable.",
      };
    }

    // FILL-RATE SCAN across ALL completed matches in this season's sample, so we
    // can see which per-match fields are actually populated (not just present as a
    // key). Focus on first-half-specific fields — those are the only recent-form
    // candidates worth building, since full-match xG/shots don't target FH goals.
    const completedAll = all.filter(m => m.status === "complete");
    const numFields = [
      "ht_goals_team_a", "ht_goals_team_b",          // baseline (what L5 uses)
      "team_a_fh_corners", "team_b_fh_corners",      // FH corners (FH-specific)
      "team_a_0_10_min_goals", "team_b_0_10_min_goals", // early goals (FH-specific)
      "team_a_corners", "team_b_corners",            // full-match corners
      "team_a_shotsOnTarget", "team_b_shotsOnTarget", // full-match SoT
      "team_a_xg", "team_b_xg",                       // full-match xG
      "team_a_dangerous_attacks", "team_b_dangerous_attacks",
    ];
    const fillRates = {};
    for (const f of numFields) {
      let present = 0, nonzero = 0;
      const samples = [];
      for (const m of completedAll) {
        const v = m[f];
        if (v !== undefined && v !== null && v !== -1) {
          present++;
          if (Number(v) > 0) nonzero++;
          if (samples.length < 5) samples.push(v);
        }
      }
      fillRates[f] = {
        present: present, nonzero: nonzero, of: completedAll.length,
        pctPresent: completedAll.length ? +(present / completedAll.length * 100).toFixed(0) : 0,
        samples,
      };
    }
    // Goal-timing arrays: are per-match goal minutes recorded? (would let us derive
    // FH goal counts precisely and any minute-window feature).
    const timingScan = { homeGoals_timings_nonEmpty: 0, awayGoals_timings_nonEmpty: 0, of: completedAll.length, sampleHome: null };
    for (const m of completedAll) {
      const ht = m.homeGoals_timings, at = m.awayGoals_timings;
      if (Array.isArray(ht) && ht.length) { timingScan.homeGoals_timings_nonEmpty++; if (!timingScan.sampleHome) timingScan.sampleHome = ht; }
      if (Array.isArray(at) && at.length) timingScan.awayGoals_timings_nonEmpty++;
    }
    const recentFormFeasibility = {
      completedMatchesScanned: completedAll.length,
      fillRates,
      timingScan,
      note: "Build last-5 versions ONLY of fields with high pctPresent AND nonzero spread. FH-specific fields (fh_corners, 0_10_min_goals, goal timings) are the ones worth testing vs L5; full-match xG/shots are not FH-targeted.",
    };

    // Default response is lean (pasteable). Add ?full=1 for the giant key dumps.
    const full = req.query.full === "1";
    if (perMatchRecon && !full) delete perMatchRecon.all_match_keys;
    res.json({
      ok: true,
      season_id: sid,
      verdict,
      recentFormFeasibility,
      perMatchRecon,
      sample_match: sampleMatch ? {
        id: sampleMatch.id,
        date_unix: sampleMatch.date_unix,
        home: sampleMatch.home_name,
        away: sampleMatch.away_name,
        status: sampleMatch.status,
        preMatchFH,
      } : null,
      sample_team: sampleTeam ? {
        id: sampleTeam.id,
        name: sampleTeam.name,
        has_stats: !!sampleTeam.stats,
        total_stats_keys: st ? Object.keys(st).length : 0,
        ht_form_fields: full ? formish : undefined,
        all_stats_keys: full && st ? Object.keys(st) : undefined,
      } : null,
      match_count: all.length,
      team_count: teams && teams.data ? teams.data.length : 0,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Clear all in-memory caches (use after changing FootyStats league selection)
app.get("/admin/flush-caches", async (req, res) => {
  const expected = process.env.LOAD_DATASET_TOKEN;
  if (!expected) return res.status(503).json({ ok: false, error: "admin token not configured" });
  if (req.query.token !== expected) return res.status(403).json({ ok: false, error: "invalid token" });
  const before = {
    fixtures: Object.keys(FIXTURE_CACHE).length,
    leagueMatches: Object.keys(LEAGUE_MATCHES_CACHE).length,
    teamStats: Object.keys(TEAM_STATS_CACHE).length,
    leagueNames: Object.keys(LEAGUE_NAMES).length,
  };
  for (const k of Object.keys(FIXTURE_CACHE)) delete FIXTURE_CACHE[k];
  for (const k of Object.keys(LEAGUE_MATCHES_CACHE)) delete LEAGUE_MATCHES_CACHE[k];
  for (const k of Object.keys(TEAM_STATS_CACHE)) delete TEAM_STATS_CACHE[k];
  LEAGUE_NAMES = {};
  // Re-fetch the league list (async, non-blocking)
  fetchLeagueList();
  res.json({ ok: true, cleared: before, note: "league list re-fetching in background" });
});

// Day-by-day calibration over recent N days — diagnoses drift vs random variance
app.get("/daily-drift", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  const days = Math.min(30, Math.max(1, parseInt(req.query.days || "10", 10)));
  const cutoffSec = Math.floor(Date.now() / 1000) - days * 86400;
  try {
    const all = [];
    const PAGE = 1000;
    for (let off = 0; ; off += PAGE) {
      const { data, error } = await supabase
        .from("match_results")
        .select("match_id, date_unix, ht_home, ht_away, hit_25, hit_15, rank, signals, snap")
        .gte("date_unix", cutoffSec)
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

    const byDay = {};
    for (const m of all) {
      const day = new Date(m.date_unix * 1000).toISOString().slice(0, 10);
      if (!byDay[day]) byDay[day] = { total: 0, fh25: 0, fh15: 0, ab: 0, ab_hits25: 0, ab_hits15: 0, single: 0, single_hits25: 0, neither: 0, neither_hits25: 0 };
      const d = byDay[day];
      d.total++;
      if (m.hit_25) d.fh25++;
      if (m.hit_15) d.fh15++;
      const sigA = m.signals && m.signals.A && m.signals.A.met;
      const sigB = m.signals && m.signals.B && m.signals.B.met;
      if (sigA && sigB) {
        d.ab++;
        if (m.hit_25) d.ab_hits25++;
        if (m.hit_15) d.ab_hits15++;
      } else if (sigA || sigB) {
        d.single++;
        if (m.hit_25) d.single_hits25++;
      } else {
        d.neither++;
        if (m.hit_25) d.neither_hits25++;
      }
    }

    const dayList = Object.keys(byDay).sort().reverse();
    const rows = dayList.map(day => {
      const d = byDay[day];
      return {
        date: day,
        total: d.total,
        fh25_rate: d.total ? +(d.fh25 / d.total * 100).toFixed(1) : 0,
        ab_fires: d.ab,
        ab_hits25: d.ab_hits25,
        ab_hit_rate: d.ab ? +(d.ab_hits25 / d.ab * 100).toFixed(1) : null,
        single_fires: d.single,
        single_hit_rate: d.single ? +(d.single_hits25 / d.single * 100).toFixed(1) : null,
        neither_fires: d.neither,
        neither_hit_rate: d.neither ? +(d.neither_hits25 / d.neither * 100).toFixed(1) : null,
      };
    });

    // Rolling totals across all days
    const total = { matches: 0, ab: 0, ab_hits: 0, single: 0, single_hits: 0, neither: 0, neither_hits: 0 };
    for (const d of Object.values(byDay)) {
      total.matches += d.total;
      total.ab += d.ab;
      total.ab_hits += d.ab_hits25;
      total.single += d.single;
      total.single_hits += d.single_hits25;
      total.neither += d.neither;
      total.neither_hits += d.neither_hits25;
    }

    res.json({
      ok: true,
      days_requested: days,
      summary: {
        matches: total.matches,
        ab_fires: total.ab,
        ab_hit_rate_pct: total.ab ? +(total.ab_hits / total.ab * 100).toFixed(1) : 0,
        single_fires: total.single,
        single_hit_rate_pct: total.single ? +(total.single_hits / total.single * 100).toFixed(1) : 0,
        neither_fires: total.neither,
        neither_hit_rate_pct: total.neither ? +(total.neither_hits / total.neither * 100).toFixed(1) : 0,
      },
      expected: { ab: 20.8, bOnly: 11.6, aOnly: 9.7, neither: 9.4 },
      by_day: rows,
      note: "Use 5+ days to distinguish drift from noise. A+B fires rarely (~5-8% of matches).",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Measures independent contribution of Signal A and Signal B
app.get("/signal-contribution-test", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const { data: allMatches, error } = await supabase
      .from("match_results")
      .select("match_id, home_name, away_name, ht_home, ht_away, snap")
      .not("snap", "is", null)
      .not("snap->>fetchedAt", "eq", "historical-import")
      .not("snap->>fetchedAt", "eq", "backfill");
    if (error) throw error;

    // Filter to viable (snap + L5 data)
    const viable = (allMatches || []).filter(m => m.snap && m.snap.l5);

    // Compute signals for each match
    const withSignals = viable.map(m => {
      const ht = m.snap.l5.home?.t || 0;
      const at = m.snap.l5.away?.t || 0;
      const af = m.snap.l5.away?.f || 0;
      const fh = parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10);

      const sigA = ht >= 1.8 && at >= 1.6;  // Mutual Instability
      const sigB = af >= 0.8;                // Away Scoring

      return {
        match_id: m.match_id,
        fh_total: fh,
        is_fh25: fh > 2.5,
        sigA, sigB,
      };
    });

    // Group by signal state
    const groups = {
      "A only": { n: 0, hits: 0 },
      "B only": { n: 0, hits: 0 },
      "A + B": { n: 0, hits: 0 },
      "Neither": { n: 0, hits: 0 },
    };

    for (const m of withSignals) {
      let group;
      if (m.sigA && m.sigB) group = "A + B";
      else if (m.sigA) group = "A only";
      else if (m.sigB) group = "B only";
      else group = "Neither";

      groups[group].n++;
      if (m.is_fh25) groups[group].hits++;
    }

    // Calculate metrics
    const results = {};
    for (const [name, data] of Object.entries(groups)) {
      results[name] = {
        matches: data.n,
        fh25_hits: data.hits,
        hit_rate_pct: data.n ? +(data.hits / data.n * 100).toFixed(1) : 0,
      };
    }

    // Calculate baseline for reference
    const baselineFh25 = withSignals.filter(m => m.is_fh25).length;
    const baselineRate = withSignals.length ? +(baselineFh25 / withSignals.length * 100).toFixed(1) : 0;

    res.json({
      ok: true,
      baseline: {
        total_matches: withSignals.length,
        fh25_matches: baselineFh25,
        baseline_rate_pct: baselineRate,
      },
      signal_states: results,
      interpretation: {
        note: "Signal A alone should show highest hit rate if it's the core engine. Signal B degrading A+B suggests overconstraining.",
        "A > A+B": "Signal B may be hurting calibration",
        "A ≈ baseline": "Signal A may need threshold adjustment",
        "B alone strong": "Signal B might work better as primary",
      },
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── SIGNAL A THRESHOLD GRID SEARCH ────────────────────────────────────────
// Tests Signal A threshold variations to see if A-alone and A+B rates can be improved
app.get("/signal-a-grid-search", async (req, res) => {
  if (!supabase) return res.status(400).json({ ok: false, error: "Supabase not enabled" });
  try {
    const { data: allMatches, error } = await supabase
      .from("match_results")
      .select("match_id, ht_home, ht_away, snap")
      .not("snap", "is", null)
      .not("snap->>fetchedAt", "eq", "historical-import")
      .not("snap->>fetchedAt", "eq", "backfill");
    if (error) throw error;

    const viable = (allMatches || []).filter(m => m.snap && m.snap.l5);

    // Grid of Signal A thresholds to test
    const thresholds = [
      { home: 1.6, away: 1.4, label: "1.6/1.4" },
      { home: 1.6, away: 1.5, label: "1.6/1.5" },
      { home: 1.7, away: 1.5, label: "1.7/1.5" },
      { home: 1.8, away: 1.6, label: "1.8/1.6 (current)" },
      { home: 1.9, away: 1.7, label: "1.9/1.7" },
      { home: 2.0, away: 1.8, label: "2.0/1.8" },
    ];

    const results = {};

    for (const threshold of thresholds) {
      const states = {
        "A only": { n: 0, hits: 0 },
        "A + B": { n: 0, hits: 0 },
      };

      for (const m of viable) {
        const ht = m.snap.l5.home?.t || 0;
        const at = m.snap.l5.away?.t || 0;
        const af = m.snap.l5.away?.f || 0;
        const fh = parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10);

        const sigA = ht >= threshold.home && at >= threshold.away;
        const sigB = af >= 0.8;
        const isFh25 = fh > 2.5;

        if (sigA && !sigB) {
          states["A only"].n++;
          if (isFh25) states["A only"].hits++;
        }
        if (sigA && sigB) {
          states["A + B"].n++;
          if (isFh25) states["A + B"].hits++;
        }
      }

      results[threshold.label] = {
        threshold: `home >= ${threshold.home}, away >= ${threshold.away}`,
        "A only": {
          matches: states["A only"].n,
          fh25_hits: states["A only"].hits,
          hit_rate_pct: states["A only"].n ? +(states["A only"].hits / states["A only"].n * 100).toFixed(1) : 0,
        },
        "A + B": {
          matches: states["A + B"].n,
          fh25_hits: states["A + B"].hits,
          hit_rate_pct: states["A + B"].n ? +(states["A + B"].hits / states["A + B"].n * 100).toFixed(1) : 0,
        },
      };
    }

    res.json({
      ok: true,
      note: "Grid search for Signal A thresholds. Goal: maximize both A-only and A+B hit rates.",
      baseline_fh25: viable.filter(m => (parseInt(m.ht_home || 0, 10) + parseInt(m.ht_away || 0, 10)) > 2.5).length,
      baseline_rate_pct: 9.4,
      results,
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
  // betPill: Fire (🔥) for Signal A+B, Dart (🎯) for Signal B
  // 🔥 Fire: only when BOTH A and B fire (A alone is noise)
  // 🎯 Dart: whenever B fires (alone or with A)
  J += "function betPill(m){if(!m)return '';";
  J += "  var h='';";
  J += "  var sigA=m.signals&&m.signals.A&&m.signals.A.met;";
  J += "  var sigB=m.signals&&m.signals.B&&m.signals.B.met;";
  J += "  if(sigA&&sigB)h+='<div style=\"display:inline-block;background:#dc2626;color:#fff;font-size:11px;font-weight:700;padding:3px 9px;border-radius:6px;margin-right:4px\" title=\"Signal A+B: Mutual Instability + Away Scoring\">🔥</div>';";
  J += "  if(sigB)h+='<div style=\"display:inline-block;background:#15803d;color:#fff;font-size:11px;font-weight:700;padding:3px 9px;border-radius:6px;letter-spacing:.5px;margin-right:6px\" title=\"Signal B: Away Team Scoring\">🎯</div>';";
  J += "  return h;}";
  // Over-1.5 / Over-2.5 FH candidate badges (the 3-rule filters: env-FH + recent FH + model prob).
  J += "function candidatePill(m){if(!m)return '';var h='';";
  J += "  var o15=m.ov15Candidate!=null?m.ov15Candidate:!!(m.signals&&m.signals.O15&&m.signals.O15.met);";
  J += "  var o25=m.ov25Candidate!=null?m.ov25Candidate:!!(m.signals&&m.signals.O25&&m.signals.O25.met);";
  J += "  if(o25)h+='<div style=\"display:inline-block;background:#b91c1c;color:#fff;font-size:10px;font-weight:800;padding:3px 8px;border-radius:6px;margin-right:4px\" title=\"Over 2.5 FH candidate — env-FH&ge;2.85, L5-FH&ge;1.6, prob25&ge;14.5\">O2.5</div>';";
  J += "  if(o15)h+='<div style=\"display:inline-block;background:#1d4ed8;color:#fff;font-size:10px;font-weight:800;padding:3px 8px;border-radius:6px;margin-right:4px\" title=\"Over 1.5 FH candidate — env-FH&ge;2.60, L5-FH&ge;1.4, prob15&ge;38\">O1.5</div>';";
  J += "  return h;}";
  J += "function patternScore(m,key){var p=m&&m.pattern?m.pattern:null;if(!p)return 0;return key==='prob25'?(Number(p.score25)||0):(Number(p.score15)||0);}";
  J += "function sortFor15(a,b){return ((b.prob15||0)-(a.prob15||0))|| (patternScore(b,'prob15')-patternScore(a,'prob15')) || ((b.prob25||0)-(a.prob25||0)) || ((b.ci||0)-(a.ci||0)) || ((b.rank||0)-(a.rank||0));}";
  J += "function sortFor25(a,b){return ((b.prob25||0)-(a.prob25||0))|| (patternScore(b,'prob25')-patternScore(a,'prob25')) || ((b.prob15||0)-(a.prob15||0)) || ((b.ci||0)-(a.ci||0)) || ((b.rank||0)-(a.rank||0));}";
  J += "function sortDefault(a,b){return ((b.rank||0)-(a.rank||0))||sortFor25(a,b)||sortFor15(a,b)||((a.dt||0)-(b.dt||0));}";
  J += "function patternBadges(m){if(!m||!m.pattern)return '';var p=m.pattern,h='';var t15=(p.reasons15||[]).join(' • ');var t25=(p.reasons25||[]).join(' • ');var tc=(p.cautions||[]).join(' • ');if(p.tag15)h+='<span title=\"'+esc(t15||p.tag15)+'\" style=\"background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:700\">'+esc(p.tag15)+'</span>';if(p.tag25)h+='<span title=\"'+esc(t25||p.tag25)+'\" style=\"background:#f0fdf4;color:#15803d;border:1px solid #a7f3d0;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:700\">'+esc(p.tag25)+'</span>';if(p.cautionTag)h+='<span title=\"'+esc(tc||p.cautionTag)+'\" style=\"background:#fff7ed;color:#c2410c;border:1px solid #fdba74;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:700\">'+esc(p.cautionTag)+'</span>';return h?'<div style=\"display:flex;gap:6px;flex-wrap:wrap;margin-top:10px\">'+h+'</div>':'';}";
  J += "function patternBadgeForRow(m,probKey){if(!m||!m.pattern)return '';var p=m.pattern;var lbl=probKey==='prob25'?(p.tag25||''):(p.tag15||'');var tips=probKey==='prob25'?(p.reasons25||[]):(p.reasons15||[]);if(lbl)return '<span title=\"'+esc(tips.join(' • ')||lbl)+'\" style=\"background:'+(probKey==='prob25'?'#f0fdf4':'#eff6ff')+';color:'+(probKey==='prob25'?'#15803d':'#1d4ed8')+';padding:1px 6px;border-radius:10px;font-size:9px;font-weight:700\">'+esc(lbl)+'</span>';if(p.cautionTag)return '<span title=\"'+esc((p.cautions||[]).join(' • ')||p.cautionTag)+'\" style=\"background:#fff7ed;color:#c2410c;padding:1px 6px;border-radius:10px;font-size:9px;font-weight:700\">'+esc(p.cautionTag)+'</span>';return '';}";

  J += "function renderTabs(){";
  J += "  var el=document.getElementById('dayTabs');var h='';";
  J += "  for(var i=0;i<DATES.length;i++){";
  J += "    var d=DATES[i];";
  J += "    var cnt=ALL.filter(function(p){return p.matchDate===d && !p.missingStats;}).length;";
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
  // Candidate signals (O1.5 / O2.5) \u2014 hit rate vs base + lift (what you bet on).
  J += "  var cand=d.candidates;";
  J += "  if(cand){";
  J += "    h+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:14px;overflow-x:auto\">';";
  J += "    h+='<div style=\"font-size:13px;font-weight:600;margin-bottom:4px\">Candidate signals \u2014 hit rate vs base</div>';";
  J += "    h+='<div style=\"font-size:11px;color:#6b7280;margin-bottom:10px\">PLAY = the O1.5 / O2.5 badge fires. Lift = hit rate \u00f7 base. Skip = same market when the badge is off.</div>';";
  J += "    h+='<table style=\"width:100%;font-size:12px;border-collapse:collapse\"><thead><tr style=\"text-align:left;color:#6b7280;border-bottom:1px solid #e5e7eb\">'";
  J += "      +'<th style=\"padding:6px 8px\">Signal</th><th style=\"padding:6px 8px\">PLAY n</th>'";
  J += "      +'<th style=\"padding:6px 8px\">Hit rate</th><th style=\"padding:6px 8px\">Base</th>'";
  J += "      +'<th style=\"padding:6px 8px\">Lift</th><th style=\"padding:6px 8px\">Skip rate</th></tr></thead><tbody>';";
  J += "    function candRow(name,c,base){if(!c||!c.play)return '';var p=c.play,sk=c.skip||{};";
  J += "      var col=(p.n<20)?'#9ca3af':(p.actual>=base)?'#0f766e':'#b91c1c';";
  J += "      return '<tr style=\"border-bottom:1px solid #f3f4f6\">'";
  J += "        +'<td style=\"padding:6px 8px;font-weight:600\">'+name+'</td>'";
  J += "        +'<td style=\"padding:6px 8px\">'+(p.n||0)+(p.n<20?'<span style=\"color:#9ca3af;font-size:10px\"> (low n)</span>':'')+'</td>'";
  J += "        +'<td style=\"padding:6px 8px;font-weight:700;color:'+col+'\">'+(p.actual||0)+'%</td>'";
  J += "        +'<td style=\"padding:6px 8px;color:#6b7280\">'+base+'%</td>'";
  J += "        +'<td style=\"padding:6px 8px;font-weight:600\">'+(p.lift||0)+'\u00d7</td>'";
  J += "        +'<td style=\"padding:6px 8px;color:#9ca3af\">'+(sk.actual||0)+'%</td>'";
  J += "      +'</tr>';}";
  J += "    h+=candRow('O1.5 candidate',cand.ov15,cand.base15);";
  J += "    h+=candRow('O2.5 candidate',cand.ov25,cand.base25);";
  J += "    h+='</tbody></table></div>';";
  J += "  }";
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
  J += "    var sigs=m.signals||{};";
  J += "    var sigStr=['A','B'].map(function(k){return sigs[k]&&sigs[k].met?k:'\u00b7';}).join('');";
  J += "    var ov15=!!m.ov15Candidate,ov25=!!m.ov25Candidate;";  // server-computed (lib/freeze.js)
  J += "    var cb=function(lbl,hit){return '<span style=\"display:inline-block;background:'+(hit?'#15803d':'#b91c1c')+';color:#fff;font-size:10px;font-weight:700;padding:2px 6px;border-radius:5px\">'+lbl+(hit?' \u2713':' \u2717')+'</span>';};";
  J += "    var leftH=(ov25?cb('O2.5',!!m.hit_25):'')+(ov15?cb('O1.5',!!m.hit_15):'')+betPill({prob25:Number(m.prob25)||0,prob15:Number(m.prob15)||0,eligible25:!!m.eligible25,eligible15:!!m.eligible15,snap:m.snap,signals:m.signals});";
  J += "    h+='<div class=\"hist-row\" data-mid=\"'+m.match_id+'\" style=\"cursor:pointer;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px 12px;display:flex;gap:10px;align-items:center;font-size:12px;margin-bottom:6px\">';";
  J += "    if(leftH)h+='<div style=\"display:flex;align-items:center;gap:3px;flex-shrink:0\">'+leftH+'</div>';";
  J += "    h+='<div style=\"flex:1;min-width:0\"><div style=\"font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis\">'+esc(m.home_name||'')+' \u2013 '+esc(m.away_name||'')+'</div>';";
  J += "    h+='<div style=\"color:#6b7280;font-size:11px\">'+esc(m.league_name||'\u2014')+' \u00b7 '+dStr+'</div></div>';";
  J += "    h+='<div style=\"text-align:right;flex-shrink:0;font-family:ui-monospace,monospace;color:#374151\">'+sigStr+' \u00b7 CI '+(Number(m.ci)||0).toFixed(1)+' \u00b7 '+(Number(m.prob25)||0).toFixed(1)+'%</div>';";
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
  J += "          var lbl=['Low','Signal','Fire','Fire'][m.rank]||'Low';";
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
  J += "    var sorted=ms.slice().sort(function(a,b){return ((a.missingStats?1:0)-(b.missingStats?1:0))||sortDefault(a,b);});";
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
  J += "  var pv15=m.missingStats?'—':(m.prob15+'%'),pv25=m.missingStats?'—':(m.prob25+'%');";
  J += "  var ps='<div class=\"prob-strip\">'";
  J += "    +'<div class=\"pp pp15\"><div class=\"pp-dot\"></div><span class=\"pp-lbl\">FH over 1.5</span><span class=\"pp-val\">'+pv15+'</span></div>'";
  J += "    +'<div class=\"pp pp25\"><div class=\"pp-dot\"></div><span class=\"pp-lbl\">FH over 2.5</span><span class=\"pp-val\">'+pv25+'</span></div>'";
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
  J += "      var l5=m.snap&&m.snap.l5?(role==='Home'?m.snap.l5.home:m.snap.l5.away):null;";
  J += "      if(l5){";
  J += "        var tThr=role==='Home'?1.6:1.4;";
  J += "        var tState=l5.t>=tThr?'g-bright':l5.t>=tThr*0.85?'g-light':'';";
  J += "        chips+=mkChip('L5 FH total '+sfx,l5.t.toFixed(2),(role==='Home'?'\u2265 1.6':'\u2265 1.4')+' \u2192 sig A',tState);";
  J += "        var fState=l5.f>=0.8?'g-bright':l5.f>=0.5?'g-light':'';";
  J += "        if(role==='Away'){chips+=mkChip('L5 FH scored (away)',l5.f.toFixed(2),'\u2265 0.8 \u2192 sig B',fState);}";
  J += "        else{chips+=mkChip('L5 FH scored (home)',l5.f.toFixed(2),'info only',fState);}";
  J += "        var l5coState=l5.a>=1.0?'r-bright':l5.a>=0.6?'r-light':'r-light';";
  J += "        chips+=mkChip('L5 FH conceded '+sfx,l5.a.toFixed(2),'info only',l5coState);";
  J += "      }";
  J += "      var scState=sn.scored_fh>=1.5?'g-bright':sn.scored_fh>=1.0?'g-light':'';" ;
  J += "      chips+=mkChip('Season FH scored '+sfx,sn.scored_fh.toFixed(2),'season avg',scState);";
  J += "      var coState=sn.conced_fh>=1.1?'r-bright':sn.conced_fh>=0.7?'r-light':'r-light';";
  J += "      chips+=mkChip('Season FH conceded '+sfx,sn.conced_fh.toFixed(2),'season avg',coState);";
  J += "    }";
  J += "    return '<div class=\"team-box\"><div class=\"team-role\">'+esc(role)+'</div><div class=\"team-name\">'+esc(name)+'</div>'+fs+fhBar+'<div class=\"stat-grid\">'+chips+'</div></div>';";
  J += "  }";
  J += "  var sigsH='<div class=\"signals\">';";
  J += "  ['A','B'].forEach(function(k){";
  J += "    var s=m.signals[k];if(!s)return;";
  J += "    sigsH+='<div class=\"sig '+(s.met?'sig-y':'sig-n')+'\">'";
  J += "      +'<div class=\"sig-dot\"></div>'+esc(k)+' \u00b7 '+esc(s.label)";
  J += "      +'<span style=\"opacity:.7;margin-left:4px;font-size:9px\">('+esc(s.value)+')</span>'";
  J += "      +'</div>';";
  J += "  });";
  J += "  sigsH+='</div>';";
  J += "  var ciVal=m.ci||0;";
  J += "  var ciCls=ciVal>=3.0?'ci-bar ci-bright':ciVal>=2.0?'ci-bar ci-light':'ci-bar ci-cold';";
  J += "  var ciValCol=ciVal>=3.0?'#69f0ae':ciVal>=2.0?'#2e7d32':'#111827';";
  J += "  var ciCheck=ciVal>=3.0?'\u2713':ciVal>=2.0?'\u25d1':'\u2717';";
  J += "  var ciH='<div class=\"'+ciCls+'\">'";
  J += "    +'<span>Recent FH intensity \u00b7 both teams\u2019 last 5</span>'";
  J += "    +'<span style=\"font-size:18px;font-weight:700;color:'+ciValCol+'\">'+ciVal.toFixed(2)+' '+ciCheck+'</span>'";
  J += "  +'</div>';";
  J += "  var patternH=patternBadges(m);";
  J += "  var mw=m.missingStats?'<span style=\"background:#fef3c7;color:#92400e;font-size:10px;padding:2px 6px;border-radius:4px;font-weight:600;margin-left:6px\">\u26a0 no stats</span>':'';";
  J += "  return '<div class=\"card\"'+(m.missingStats?' style=\"opacity:.55\"':'')+'>'";
  J += "    +'<div class=\"card-accent\" style=\"background:'+accent+'\"></div>'";
  J += "    +'<div class=\"card-inner\">'";
  J += "      +'<div style=\"display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin-bottom:12px\">'";
  J += "        +'<div style=\"min-width:0\">'";
  J += "          +'<div style=\"font-size:10px;color:#9ca3af;text-transform:uppercase;letter-spacing:.7px;margin-bottom:5px\">'+esc(m.league)+' \u00b7 '+esc(dt)+mw+'</div>'";
  J += "          +sb";
  J += "        +'</div>'";
  J += "        +'<div style=\"display:flex;align-items:center;gap:6px;flex-wrap:wrap;justify-content:flex-end\">'+candidatePill(m)+betPill(m)+'</div>'";
  J += "      +'</div>'";
  J += "    +ps+rb";
  J += "    +patternH";
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
  J += "  var sigs='';if(m.signals){['A','B'].forEach(function(k){if(m.signals[k]&&m.signals[k].met)sigs+=k+' ';});}";
  J += "  var ptag=patternBadgeForRow(m,probKey);";
  J += "  var h='<div style=\"display:flex;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid #f3f4f6\">';";
  J += "  h+='<div style=\"font-size:18px;font-weight:800;color:#d1d5db;width:24px;text-align:center\">'+(idx+1)+'</div>';";
  J += "  h+='<div style=\"flex:1;min-width:0\">';";
  J += "  h+='<div style=\"font-weight:600;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis\">'+esc(m.home)+' vs '+esc(m.away)+'</div>';";
  J += "  h+='<div style=\"font-size:10px;color:#9ca3af\">'+esc(m.league)+' \u00b7 '+esc(dt)+'</div>';";
  J += "  if(ptag)h+='<div style=\"margin-top:4px\">'+ptag+'</div>';";
  J += "  h+='</div>';";
  J += "  h+='<div style=\"display:flex;gap:4px;align-items:center;flex-shrink:0\">';";
  J += "  if(sigs)h+='<span style=\"background:#fef9c3;color:#92400e;padding:1px 6px;border-radius:10px;font-size:9px;font-weight:600\">'+sigs.trim()+'</span>';";
  J += "  h+='<span style=\"background:#eff6ff;color:#1d4ed8;padding:1px 6px;border-radius:10px;font-size:9px\">CI '+(Number(m.ci)||0).toFixed(1)+'</span>';";
  J += "  h+='<span class=\"rn '+rc+'\" style=\"font-size:12px;padding:2px 6px\">'+m.rank+'/3</span>';";
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
  J += "  var s15=upcoming.slice().sort(sortFor15);";
  J += "  h+=renderBBSection('\u26bd Top 7 \u2014 FH Over 1.5 Goals','#1d4ed8','#bfdbfe',s15.slice(0,7),'prob15','> 1.5');";

  // Top 7 Over 2.5
  J += "  var s25=upcoming.slice().sort(sortFor25);";
  J += "  h+=renderBBSection('\ud83d\udd25 Top 7 \u2014 FH Over 2.5 Goals','#15803d','#a5d6a7',s25.slice(0,7),'prob25','> 2.5');";

  // Top 5 Value Picks — high CI but low rank (close to more signals firing)
  J += "  var value=upcoming.filter(function(p){return p.rank<=1&&((p.pattern&&((p.pattern.score15||0)>=4||(p.pattern.score25||0)>=4))||p.ci>=3.2);}).sort(function(a,b){return Math.max(patternScore(b,'prob15'),patternScore(b,'prob25'))-Math.max(patternScore(a,'prob15'),patternScore(a,'prob25'))||b.ci-a.ci||b.prob15-a.prob15;});";
  J += "  if(value.length){";
  J += "    h+='<div style=\"margin-bottom:24px\">';";
  J += "    h+='<div style=\"font-size:16px;font-weight:700;color:#92400e;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #fde68a\">\ud83d\udca1 Top 5 Value Picks \u2014 High CI, Signals Developing</div>';";
  J += "    h+='<div style=\"background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:8px 12px;margin-bottom:10px;font-size:11px;color:#92400e\">These are secondary-pattern matches: the calibrated probability is decent, but the newer pattern tags suggest extra FH pressure that raw rank alone misses.</div>';";
  J += "    h+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden\">';";
  J += "    value.slice(0,5).forEach(function(m,i){h+=renderBBRow(m,'prob15','value',i);});";
  J += "    h+='</div></div>';";
  J += "  }";

  // Best match per day
  J += "  h+='<div style=\"margin-bottom:24px\">';";
  J += "  h+='<div style=\"font-size:16px;font-weight:700;color:#7c3aed;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #c4b5fd\">\ud83c\udfc6 Best Match Per Day</div>';";
  J += "  DATES.slice(0,4).forEach(function(d,di){";
  J += "    var dayMatches=upcoming.filter(function(p){return p.matchDate===d;}).sort(sortDefault);";
  J += "    if(!dayMatches.length)return;";
  J += "    var best=dayMatches[0];var rc=rankCls(best.rank);";
  J += "    var dt=best.dt?new Date(best.dt).toLocaleDateString('en-GB',{weekday:'short',day:'2-digit',month:'short'}):'';";
  J += "    var sigs='';if(best.signals){['A','B'].forEach(function(k){if(best.signals[k]&&best.signals[k].met)sigs+=k+' ';});}";
  J += "    h+='<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:12px 14px;margin-bottom:10px\">';";
  J += "    h+='<div style=\"font-size:11px;color:#7c3aed;font-weight:700;text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px\">'+(DAY_LABELS[di]||d)+' \u2014 '+esc(dt)+'</div>';";
  J += "    h+='<div style=\"display:flex;justify-content:space-between;align-items:center;margin-bottom:4px\">';";
  J += "    h+='<div style=\"font-weight:700;font-size:14px\">'+esc(best.home)+' vs '+esc(best.away)+'</div>';";
  J += "    h+='<span class=\"rn '+rc+'\" style=\"font-size:14px;padding:2px 8px\">'+best.rank+'/3</span>';";
  J += "    h+='</div>';";
  J += "    h+='<div style=\"font-size:11px;color:#6b7280;margin-bottom:6px\">'+esc(best.league)+'</div>';";
  J += "    h+='<div style=\"display:flex;gap:6px;flex-wrap:wrap;font-size:11px\">';";
  J += "    h+='<span style=\"background:#eff6ff;color:#1d4ed8;padding:2px 8px;border-radius:12px\">CI: '+best.ci+'</span>';";
  J += "    h+='<span style=\"background:#f0fdf4;color:#15803d;padding:2px 8px;border-radius:12px\">FH>1.5: '+best.prob15+'%</span>';";
  J += "    h+='<span style=\"background:#f0fdf4;color:#15803d;padding:2px 8px;border-radius:12px\">FH>2.5: '+best.prob25+'%</span>';";
  J += "    if(best.pattern&&best.pattern.tag15)h+='<span style=\"background:#eff6ff;color:#1d4ed8;padding:2px 8px;border-radius:12px\">'+esc(best.pattern.tag15)+'</span>';";
  J += "    if(best.pattern&&best.pattern.tag25)h+='<span style=\"background:#f0fdf4;color:#15803d;padding:2px 8px;border-radius:12px\">'+esc(best.pattern.tag25)+'</span>';";
  J += "    if(best.pattern&&best.pattern.cautionTag)h+='<span style=\"background:#fff7ed;color:#c2410c;padding:2px 8px;border-radius:12px\">'+esc(best.pattern.cautionTag)+'</span>';";
  J += "    if(sigs)h+='<span style=\"background:#fef9c3;color:#92400e;padding:2px 8px;border-radius:12px\">Signals: '+sigs.trim()+'</span>';";
  J += "    h+='</div></div>';";
  J += "  });";
  J += "  h+='</div>';";

  // Parlays — Over 1.5
  J += "  var p15=upcoming.filter(function(p){return p.rank>=1;}).sort(sortFor15);";
  J += "  h+='<div style=\"margin-bottom:24px\"><div style=\"font-size:16px;font-weight:700;color:#1d4ed8;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #bfdbfe\">\u26bd Parlays \u2014 FH Over 1.5 Goals</div>';";
  J += "  if(p15.length>=2)h+=renderParlayCard('2-Leg Parlay',p15.slice(0,2),'prob15','> 1.5');";
  J += "  if(p15.length>=3)h+=renderParlayCard('3-Leg Parlay',p15.slice(0,3),'prob15','> 1.5');";
  J += "  if(p15.length>=4)h+=renderParlayCard('4-Leg Parlay',p15.slice(0,4),'prob15','> 1.5');";
  J += "  h+='</div>';";

  // Parlays — Over 2.5
  J += "  var p25=upcoming.filter(function(p){return p.rank>=1;}).sort(sortFor25);";
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
    <strong>How it works:</strong> 3 pre-game signals (A, B &amp; C) &mdash; no look-ahead bias. Signal A &#128293;: Mutual Instability (home L5 FH total &ge; 1.6 AND away L5 FH total &ge; 1.4). Signal B &#127919;: Away Team Scoring (away L5 FH scored &ge; 0.8). Signal C: Team Mismatch (season net-FH rating gap &ge; 0.5) &mdash; adds value mainly on no-signal games. Rank = signals fired (max 3); probability is per-combo, so a higher rank never overrides a lower calibrated probability. &#128293; shown when both A+B fire; &#127919; shown when B fires.
    Calibrated on 888 clean matches (women excluded): A+B = 19.8% FH&gt;2.5 (2.0&times; the no-signal rate 9.9%) &middot; B only = 11.3% &middot; A only = 9.4% (at baseline) &middot; neither = 9.9%. FH&gt;1.5: 44.2% (A+B) &middot; 42.1% (B only) &middot; 31.3% (A only) &middot; 32.7% (neither).
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
  // Re-fetch the league registry every 12h so newly-added leagues appear without a restart.
  setInterval(() => { fetchLeagueList(); }, 12 * 60 * 60 * 1000);

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
    // Traffic-independent capture: freeze snaps + record results on a timer so
    // pending rows stop accumulating on low-traffic days. Kick once after caches
    // warm, then every 20 min (well under the fixture cache 30-min TTL).
    setTimeout(() => { selfCapture(); }, 6 * 60 * 1000);
    setInterval(() => { selfCapture(); }, SELF_CAPTURE_TTL);
  }
});

process.on("uncaughtException", e => console.error("Uncaught:", e.message));
