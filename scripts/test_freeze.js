"use strict";
// Locks the pre-game freeze contract (lib/freeze.js). If any change re-introduces
// look-ahead, this fails. Run: `node scripts/test_freeze.js` (or `npm test`).
const assert = require("assert");
const { computeOverCandidates, selectPregamePrediction, isResultFinal } = require("../lib/freeze");

let failures = 0;
function check(name, fn) {
  try { fn(); console.log("  ok   " + name); }
  catch (e) { failures++; console.error("FAIL   " + name + " — " + e.message); }
}

// ── selectPregamePrediction — THE freeze rule ───────────────────────────────
const frozen = { rank: 2, ov25Candidate: true };
const live = { rank: 3, ov25Candidate: true };
check("completed + frozen -> frozen", () =>
  assert.strictEqual(selectPregamePrediction(frozen, live, true), frozen));
check("completed + NO frozen -> null (the look-ahead guard)", () =>
  assert.strictEqual(selectPregamePrediction(null, live, true), null));
check("completed + no frozen + no result -> null", () =>
  assert.strictEqual(selectPregamePrediction(null, null, true), null));
check("upcoming + result -> result", () =>
  assert.strictEqual(selectPregamePrediction(null, live, false), live));
check("upcoming + frozen -> frozen wins", () =>
  assert.strictEqual(selectPregamePrediction(frozen, live, false), frozen));

// ── computeOverCandidates — pre-game inputs only ────────────────────────────
const hot = { home: { scored_fh: 1, conced_fh: 1 }, away: { scored_fh: 1, conced_fh: 1 }, l5: { home: { f: 1 }, away: { f: 1 } } };
const cold = { home: { scored_fh: 0.3, conced_fh: 0.3 }, away: { scored_fh: 0.3, conced_fh: 0.3 }, l5: { home: { f: 0.2 }, away: { f: 0.2 } } };

check("clears thresholds -> both candidates true", () => {
  const oc = computeOverCandidates(hot, 50, 20);
  assert.strictEqual(oc.envFh, 4);
  assert.strictEqual(oc.l5Fh, 2);
  assert.ok(oc.ov15Candidate && oc.ov25Candidate);
});
check("below thresholds -> both false", () => {
  const oc = computeOverCandidates(cold, 26, 7);
  assert.ok(!oc.ov15Candidate && !oc.ov25Candidate);
});
check("O2.5 exact boundary (env 2.85, l5 1.6, p25 14.5) -> true", () => {
  const s = { home: { scored_fh: 1.85, conced_fh: 1 }, away: { scored_fh: 0, conced_fh: 0 }, l5: { home: { f: 1.6 }, away: { f: 0 } } };
  assert.ok(computeOverCandidates(s, 43.6, 14.5).ov25Candidate);
});
check("float boundary 0.7+0.6+0.6+0.7 rounds to 2.60 -> O1.5 env clears", () => {
  const s = { home: { scored_fh: 0.7, conced_fh: 0.6 }, away: { scored_fh: 0.6, conced_fh: 0.7 }, l5: { home: { f: 0.7 }, away: { f: 0.7 } } };
  const oc = computeOverCandidates(s, 38, 0);
  assert.strictEqual(oc.envFh, 2.6);
  assert.ok(oc.ov15Candidate);
});
check("probs stored as 0-1 are normalized", () => {
  const oc = computeOverCandidates(hot, 0.50, 0.196);
  assert.ok(oc.ov15Candidate && oc.ov25Candidate);
});
check("NEVER reads the match result (no look-ahead)", () => {
  const base = computeOverCandidates(hot, 50, 20);
  const tainted = JSON.parse(JSON.stringify(hot));
  tainted.hit_25 = 1; tainted.ft_home = 9;
  tainted.home.ft_goals = 9; tainted.away.ft_goals = 9;
  assert.deepStrictEqual(computeOverCandidates(tainted, 50, 20), base);
});
check("missing snap -> no candidates, no throw", () => {
  const oc = computeOverCandidates(null, 50, 20);
  assert.ok(!oc.ov15Candidate && !oc.ov25Candidate);
});

// ── isResultFinal — don't record a placeholder 0-0 ──────────────────────────
check("complete status is always final (even 0-0)", () =>
  assert.strictEqual(isResultFinal("complete", false, 0), true));
check("incomplete + played + posted score -> final", () =>
  assert.strictEqual(isResultFinal("incomplete", true, 2), true));
check("incomplete + played + 0-0 -> NOT final (score not posted yet)", () =>
  assert.strictEqual(isResultFinal("incomplete", true, 0), false));
check("incomplete + NOT played (upcoming) -> NOT final, even with a score", () =>
  assert.strictEqual(isResultFinal("incomplete", false, 3), false));
check("unknown/suspended status -> NOT final", () =>
  assert.strictEqual(isResultFinal("suspended", true, 4), false));

if (failures) { console.error("\n" + failures + " freeze-invariant test(s) FAILED"); process.exit(1); }
console.log("\nAll freeze-invariant tests passed.");
