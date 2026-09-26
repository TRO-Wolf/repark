# GUIDED-EXECUTOR-BANDS-1 — executor work-order shape follows the engine's Terminal-Bench 4.0 band

**Filed:** 2026-09-26 by the orchestrating session under owner ruling.
**Status:** ACTIVE (owner, 2026-09-26: "a solid plan for right now"). The bands and the checklist live
in `scripts/coordinator/bands.md`; the handbook's EXECUTOR BANDS rule points at them; the Muse addendum
carries the engine line. The weekly score re-pull is the orchestrating session's duty; a change to the
band table in the repo is a docs PR the owner sees.

## 1. What was observed

- The Muse Spark round on U11 (edge resolution, 2026-09-25) halted on rulings the work order had left
  open and produced nothing mergeable in six hours; the salvage was an Opus triage round on top.
- Every Opus 5.5 round on the same unit produced merged commits.
- The cost claim first attached to that comparison ("roughly ten times") was not a measurement and was
  withdrawn. The measured picture: Muse is a flat subscription (zero marginal Claude quota); Opus rounds
  spend the weekly Claude quota (49 Opus sub-agent rounds since the 2026-09-25 morning read 746 M cached
  tokens; a fixer round 10 to 60 M, a verifier 3 to 16 M, a full PR loop about 110 M).
- The pattern is not Muse-specific: executors below a capability line do the code well when the answer
  is pinned and wander or halt when the order leaves a decision open.

## 2. The ruling

Three bands on Terminal-Bench 4.0 (Artificial Analysis as the reference source), threshold 50, floor 25:

| Band | Score | Shape |
|---|---|---|
| self-directed | 50 and above | the ordinary work order |
| guided | 25 to 49 | the guided-execution checklist (rulings pre-made, one cell per round with the recorded Spark answer pasted in, a design sketch for multi-crate work, the file-ceiling escape pre-named, halt within the first hour on a question, smaller step budget, pattern carried in the engine addendum) |
| clerk | below 25 | mechanical work only |

Consequences on 2026-09-26: Muse Spark 1.3 (33.3), Devin SWE-2 (27.3), Grok 4.7, GPT-5.6 Sol and
GLM-5.3 run guided; GPT-5.6 Terra (21.5), Grok 4.6 and GPT-5.6 Luna drop to clerk; the Opus family,
Fable and GPT-6 Astra stay self-directed. Sonnet and Haiku stay banned whatever they score. The Opus
5.5 medium verifier and fixer rule on every lane is unchanged.

## 3. Standing duties

1. Re-pull the scores at least weekly; the pull date heads the table; an unscored engine runs guided;
   a score is never invented.
2. Report band changes in the morning report; a changed band table in the repo is a docs PR.
3. Apply a guided engine's lessons at the next run's start through its addendum, never mid-run
   (`scripts/coordinator/lessons.md` holds the evidence).

## 4. Deferred

- A per-engine step budget in the launchers (today the work order states it).
- A mechanical check that a guided engine's work order pastes a Spark answer (today the orchestrating
  session's read of the order is the gate).
