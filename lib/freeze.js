"use strict";
// ─── Pre-game freeze contract — the model's integrity invariant ──────────────
// These two pure functions encode the rules that must NEVER be violated, kept in
// ONE place so the live engine, the snapshot restore, and the calibration/history
// readouts can't drift apart (that drift once wiped the pills on restart).
// No I/O, no API key, no DB — unit-tested by scripts/test_freeze.js.

// O1.5 / O2.5 candidate flags from PRE-GAME inputs ONLY:
//   env_fh = both teams' season FH scored+conceded   (frozen pre-game)
//   l5_fh  = home+away last-5 FH scored               (frozen pre-game)
//   prob15 / prob25                                   (model probability)
// The match result is NEVER an input. Tolerates probs stored as 0-1 or 0-100.
function computeOverCandidates(snap, prob15, prob25) {
  const n0 = (v) => { const x = Number(v); return Number.isFinite(x) ? x : 0; };
  const r4 = (v) => Math.round(v * 1e4) / 1e4;  // absorb float drift at the boundary
  const h = (snap && snap.home) || {}, a = (snap && snap.away) || {};
  const l5 = (snap && snap.l5) || {}, l5h = l5.home || {}, l5a = l5.away || {};
  const envFh = r4(n0(h.scored_fh) + n0(h.conced_fh) + n0(a.scored_fh) + n0(a.conced_fh));
  const l5Fh = r4(n0(l5h.f) + n0(l5a.f));
  let p15 = Number(prob15) || 0, p25 = Number(prob25) || 0;
  if (p15 > 0 && p15 < 1) p15 *= 100;
  if (p25 > 0 && p25 < 1) p25 *= 100;
  return {
    envFh, l5Fh,
    ov15Candidate: envFh >= 2.60 && l5Fh >= 1.4 && p15 >= 38,
    ov25Candidate: envFh >= 2.85 && l5Fh >= 1.6 && p25 >= 14.5,
  };
}

// THE freeze rule: a completed match may only ever show a prediction that was
// frozen BEFORE kickoff. The live `result` (recomputed from current, post-match
// stats) is usable ONLY while the match is still upcoming. Returns the prediction
// source to display, or null if there's no legitimate pre-game prediction.
function selectPregamePrediction(frozen, result, isComplete) {
  if (frozen) return frozen;          // frozen pre-game snapshot always wins
  if (isComplete) return null;        // completed + never frozen → NO look-ahead recompute
  return result || null;              // still upcoming → live result is fine
}

module.exports = { computeOverCandidates, selectPregamePrediction };
